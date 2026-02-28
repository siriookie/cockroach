# CockroachDB RangeFeed 客户端实现深度解析（上篇）

> **目标读者**：有扎实 Go / 分布式系统基础、但尚未阅读该代码的工程师。
> **分析版本**：`pkg/kv/kvclient/rangefeed/rangefeed.go` + 相关依赖文件。
> **方法论**：BFS 梳理控制流 → DFS 深入关键函数 → 具体运行示例 → 补充知识。

---

## 一、背景：RangeFeed 是什么？它解决了什么问题？

### 1.1 核心问题

分布式数据库中，一个普遍需求是 **变更数据捕获（CDC, Change Data Capture）**——监听某个 key range 内的所有写入并实时推送。在 CockroachDB 中，这一能力由 **RangeFeed** 机制实现。

**用途清单（工程视角）**：
| 使用方 | 场景 |
|---|---|
| `changefeed` (CDC) | 将行级变更推送到 Kafka / Webhook |
| `sqlliveness` | 监听 session 心跳 key，判断 session 是否存活 |
| `spanconfigkvsubscriber` | 监听 span config 表的变更以触发重配置 |
| `settingswatcher` | 监听集群 settings 变更 |
| `rangefeedcache` | 提供通用的"全量+增量"缓存语义 |

### 1.2 在分层架构中的位置

```
┌─────────────────────────────────────────────────────┐
│  SQL / CDC / sqlliveness / settingswatcher 等        │  使用方（上层）
├─────────────────────────────────────────────────────┤
│  pkg/kv/kvclient/rangefeed/  ← 本文分析              │  客户端封装层
│    ├── Factory / RangeFeed (rangefeed.go)            │
│    ├── config.go  (Option 模式)                      │
│    ├── scanner.go (初始扫描)                          │
│    └── db_adapter.go (DB 接口实现)                   │
├─────────────────────────────────────────────────────┤
│  pkg/kv/kvclient/kvcoord/ (DistSender)               │  KV 协调层
│    → 路由 RangeFeed RPC 到各 Range Leader            │
├─────────────────────────────────────────────────────┤
│  pkg/kv/kvserver/ (RangeFeed processor)              │  存储层（服务端）
│    → 将 Raft log 转化为 RangeFeedEvent               │
└─────────────────────────────────────────────────────┘
```

**本文只分析客户端封装层**，但会追踪到 `DistSender` 的边界以说明接口语义。

### 1.3 核心论文背景

RangeFeed 的设计思路来源于：
- **Google Spanner** 的 **TrueTime + Commit Timestamps**：CockroachDB 使用 HLC (Hybrid Logical Clock) 仿制 TrueTime 语义，使得每个写入有精确的时间戳，订阅者可以从任意时间点开始。
- **Kafka / Dataflow** 的 **watermark 语义**：CockroachDB 的 `Checkpoint` 事件等价于 watermark，表明该时间点之前的变更已全部发出。`Frontier` 数据结构跟踪所有 span 中最慢的 watermark（即"短板"），这直接对应 Dataflow 论文中的 **frontier/watermark** 概念。

---

## 二、第一轮 BFS：控制流与组件协作（How it flows）

### 2.1 核心数据结构一览

```
Factory
├── stopper  *stop.Stopper        // 生命周期管理
├── client   DB                   // 向下层对接的接口（可 mock）
└── knobs    *TestingKnobs        // 测试注入点

RangeFeed
├── config (内嵌)                  // 所有 Option 配置
│   ├── retryOptions              // 指数退避参数
│   ├── withInitialScan           // 是否开启初始扫描
│   ├── onValue     OnValue       // 单条事件回调
│   ├── onValues    OnValues      // 批量事件回调（初始扫描优化路径）
│   ├── onCheckpoint              // checkpoint 回调
│   ├── onFrontierAdvance         // frontier 推进回调
│   ├── frontierVisitor           // frontier 精细检视回调
│   ├── onSSTable                 // SST 注入事件回调
│   ├── onDeleteRange             // MVCC 范围删除事件回调
│   └── frontierQuantize          // frontier 时间戳量化（减少碎片）
├── initialTimestamp hlc.Timestamp // 订阅起始时间（exclusive）
├── spans []roachpb.Span           // 监听范围
├── cancel context.CancelFunc      // 停止信号
├── running sync.WaitGroup         // 等待后台 goroutine 退出
└── started int32 (atomic)         // 一次性启动保护
```

**DB 接口**（db_adapter.go 中的 `dbAdapter` 实现，测试中可被 mock）：

```go
// rangefeed.go:44-78
type DB interface {
    RangeFeed(ctx, spans, startFrom, eventC, opts...) error
    RangeFeedFromFrontier(ctx, frontier, eventC, opts...) error
    Scan(ctx, spans, asOf, rowFn, rowsFn, cfg) error
}
```

> **设计意图**：将 `DB` 抽象为接口，使得整个 `RangeFeed` 客户端可以在不依赖真实 KV 层的情况下被测试（mock）。这是 CockroachDB 代码库中普遍的依赖注入模式。

---

### 2.2 执行路径概览（主状态机）

整个 RangeFeed 的生命周期是一个清晰的状态机：

```
[调用方]
    │
    ▼
Factory.RangeFeed()  ──→  Factory.New()  ──→  r.Start()
                                                    │
                                            ┌───────┴────────┐
                                            │  r.start()     │
                                            │  (原子保护,     │
                                            │   启动 stopper  │
                                            │   异步任务)     │
                                            └───────┬────────┘
                                                    │ goroutine
                                                    ▼
                                            ┌───────────────┐
                                            │   r.run()     │  ← 重试主循环
                                            │               │
                                            │  ┌─────────┐  │
                                            │  │InitScan │  │  仅首次
                                            │  └────┬────┘  │
                                            │       │        │
                                            │  ┌────▼─────┐ │
                              eventCh ←────│  │RangeFeed │ │  goroutine A
                                            │  │ Task     │ │
                                            │  └──────────┘ │
                                            │  ┌──────────┐ │
                              eventCh ────→│  │process   │ │  goroutine B
                                            │  │Events    │ │
                                            │  └──────────┘ │
                                            │  (GoAndWait)  │
                                            └───────┬───────┘
                                                    │ 失败/重试
                                                    ▼
                                            [指数退避 r.Next()]
                                                    │
                                            [ctx 取消 / 不可恢复错误]
                                                    │
                                                    ▼
                                            [running.Done()]  ← Close() 可返回
```

---

### 2.3 触发方式分析

| 维度 | 说明 |
|---|---|
| **启动** | **主动调用**（`Factory.RangeFeed()` 或 `r.Start()`），非被动/惰性触发 |
| **事件消费** | **被动回调**：由 `processEvents` goroutine 阻塞在 `eventCh` 上等待推送 |
| **重试** | **自愈循环**：`run()` 内的 `retry.StartWithCtx` 实现指数退避，非定时器触发 |
| **初始扫描** | **主动、阻塞**：`runInitialScan` 在开始 RangeFeed 流之前同步完成（在同一 goroutine 中） |
| **Frontier 推进** | **事件驱动**：每收到 `Checkpoint` 事件时更新，非单独定时器 |
| **关闭** | **信号驱动**：`Close()` 调用 `cancel()`，ctx 取消会级联关闭所有子任务 |

---

### 2.4 关键共享状态与模块交互

```
RangeFeed.run()
    │
    ├── frontier (span.Frontier)
    │     ├── 初始扫描结束后更新（scanner.go 中 Forward）
    │     └── 每收到 Checkpoint 事件时更新（processEvent 中 Forward）
    │         → 共享于 rangeFeedTask 和 processEventsTask 之间
    │         → 注意：二者通过 ctxgroup.GoAndWait 并发，
    │           但 frontier 的写入只在 processEventsTask 中，
    │           rangeFeedTask 只读 frontier.Frontier() 来决定起始 ts，
    │           这两个动作在时间上不重叠（GoAndWait 的 restart 语义保证）
    │
    ├── eventCh (chan kvcoord.RangeFeedMessage)
    │     ├── 写入方：DB.RangeFeed → DistSender → 各 Range processor
    │     └── 读取方：processEvents()
    │
    └── f.cancel (context.CancelFunc)
          └── Close() 或 stopper quiesce 触发，取消 ctx 通知所有子任务退出
```

> **不变量（Invariant）**：`frontier` 只在 `processEventsTask` goroutine 中被写入；下一轮循环开始时 `rangeFeedTask` 读取 `frontier.Frontier()`，此时 `processEventsTask` 已经退出（`GoAndWait` 等待两者都完成才继续）。因此 **`frontier` 的访问在整个 `run()` 循环中是单线程的**，无需额外加锁。

---

## 三、DFS 深入：关键函数与核心逻辑

### 3.1 `Factory.New()` + `initConfig()` — 构造阶段

**文件**：[rangefeed.go:153-168](pkg/kv/kvclient/rangefeed/rangefeed.go#L153)，[config.go:299-308](pkg/kv/kvclient/rangefeed/config.go#L299)

```go
// rangefeed.go:154-168
func (f *Factory) New(
    name string, initialTimestamp hlc.Timestamp, onValue OnValue, options ...Option,
) *RangeFeed {
    r := RangeFeed{
        client:           f.client,
        stopper:          f.stopper,
        knobs:            f.knobs,
        initialTimestamp: initialTimestamp,
        name:             name,
        onValue:          onValue,
    }
    initConfig(&r.config, options)
    return &r
}
```

```go
// config.go:299-308
func initConfig(c *config, options []Option) {
    *c = config{} // the default config is its zero value
    for _, o := range options {
        o.set(c)
    }
    if c.targetScanBytes == 0 {
        c.targetScanBytes = 1 << 19 // 512 KiB
    }
}
```

**设计模式**：**Functional Options Pattern**（函数式选项模式）。

每个 `Option` 是一个 `optionFunc`：
```go
// config.go:78-80
type optionFunc func(*config)
func (o optionFunc) set(c *config) { o(c) }
```

这一模式的优势：
1. 构造函数签名稳定，新增选项不破坏 API。
2. 每个选项自描述，调用方代码即文档。
3. `initConfig` 先 zero-value 整个 config，再逐一 apply，避免遗留脏状态。

**初始化后的关键默认值**：
- `targetScanBytes = 512 KiB`：控制初始扫描的每个 RPC 请求返回多少字节。
- `retryOptions`：零值对应 `retry.Options` 默认值（初始等待 ~500ms，最大等待 ~10s，指数增长）。

---

### 3.2 `start()` — 并发启动核心

**文件**：[rangefeed.go:224-285](pkg/kv/kvclient/rangefeed/rangefeed.go#L224)

```go
func (f *RangeFeed) start(
    ctx context.Context, frontier span.Frontier, ownsFrontier bool, resumeFromFrontier bool,
) error {
    if !atomic.CompareAndSwapInt32(&f.started, 0, 1) {
        return errors.AssertionFailedf("rangefeed already started")
    }
    // ...
    ctx, f.cancel = f.stopper.WithCancelOnQuiesce(ctx)
    f.running.Add(1)
    if err := f.stopper.RunAsyncTask(ctx, "rangefeed", runWithFrontier); err != nil {
        f.cancel()
        f.running.Done()
        return err
    }
    return nil
}
```

**逐段解析**：

**① CAS 保护（第 227 行）**

```go
if !atomic.CompareAndSwapInt32(&f.started, 0, 1) {
    return errors.AssertionFailedf("rangefeed already started")
}
```

- `started` 是 `int32`，用 atomic CAS 保证只有一个调用者能从 0 改为 1。
- 为什么不用 `sync.Once`？因为 `sync.Once` 不会返回错误；而这里需要通知调用方"你调用错了"。

**② Frontier 所有权管理（第 237-240 行）**

```go
runWithFrontier := func(ctx context.Context) {
    if ownsFrontier {
        defer frontier.Release()
    }
    // ...
}
```

- `Start()` 路径：`ownsFrontier=true`，RangeFeed 自己创建 frontier，用完自己释放（将 B-Tree 节点归还对象池）。
- `StartFromFrontier()` 路径：`ownsFrontier=false`，调用方拥有 frontier，负责在 `Close()` 返回后自行 `Release()`。
- 这是 **所有权转移（ownership transfer）** 语义的典型实现：谁创建谁负责，通过参数明确约定。

**③ pprof 标签（第 243-247 行）**

```go
ctx, reset := pprofutil.SetProfilerLabels(
    ctx, append(f.extraPProfLabels, "rangefeed", f.name)...,
)
defer reset()
```

- 在所有由这个 RangeFeed 产生的 goroutine 调用栈上打上 `rangefeed=<name>` 标签。
- 在 `go tool pprof` 中可以用这些标签过滤，精确定位哪个订阅者在消耗 CPU。

**④ Stopper 集成（第 276-284 行）**

```go
ctx, f.cancel = f.stopper.WithCancelOnQuiesce(ctx)
f.running.Add(1)
if err := f.stopper.RunAsyncTask(ctx, "rangefeed", runWithFrontier); err != nil {
    f.cancel()
    f.running.Done()
    return err
}
```

- `stopper.WithCancelOnQuiesce`：服务器 quiesce（优雅关闭）时自动取消 ctx。
- `stopper.RunAsyncTask`：在 stopper 管理的 goroutine 池中运行，确保服务器关闭时能等待所有 task 完成。
- `f.running.Add(1)` 和 defer `f.running.Done()` 配合 `Close()` 的 `f.running.Wait()`，实现调用方等待后台 goroutine 完全退出的语义。
- **错误路径**：若 `RunAsyncTask` 失败（stopper 已停止），必须立即 `f.cancel()` 和 `f.running.Done()`，否则 `Close()` 会永远卡在 `Wait()` 上。

---

### 3.3 `run()` — 自愈重试主循环（最重要的函数）

**文件**：[rangefeed.go:307-429](pkg/kv/kvclient/rangefeed/rangefeed.go#L307)

这是整个模块最核心的函数，理解它等于理解了整个 RangeFeed 的自愈机制。

```go
func (f *RangeFeed) run(ctx context.Context, frontier span.Frontier, resumeWithFrontier bool) {
    defer f.running.Done()
    r := retry.StartWithCtx(ctx, f.retryOptions)
    restartLogEvery := log.Every(10 * time.Second)

    // ─── 阶段 1：初始扫描（仅首次，阻塞完成） ───
    if f.withInitialScan {
        if failed := f.runInitialScan(ctx, &restartLogEvery, &r, frontier); failed {
            return
        }
    } else if !resumeWithFrontier {
        // 没有初始扫描，手动将 frontier 推进到 initialTimestamp
        for _, sp := range f.spans {
            if _, err := frontier.Forward(sp, f.initialTimestamp); err != nil { ... }
        }
    }

    // ─── 阶段 2：构建 RangeFeed 选项 ───
    eventCh := make(chan kvcoord.RangeFeedMessage)
    rangefeedOpts := []kvcoord.RangeFeedOption{kvcoord.WithBulkDelivery()}
    if f.withDiff    { rangefeedOpts = append(rangefeedOpts, kvcoord.WithDiff()) }
    if f.withFiltering { rangefeedOpts = append(rangefeedOpts, kvcoord.WithFiltering()) }
    // ... 其他选项

    // ─── 阶段 3：重试主循环 ───
    for i := 0; r.Next(); i++ {
        ts := frontier.Frontier()

        rangeFeedTask := func(ctx context.Context) error {
            return f.client.RangeFeed(ctx, f.spans, ts, eventCh, rangefeedOpts...)
        }
        processEventsTask := func(ctx context.Context) error {
            return f.processEvents(ctx, frontier, eventCh)
        }

        err := ctxgroup.GoAndWait(ctx, rangeFeedTask, processEventsTask)

        // ─── 阶段 4：错误分类处理 ───
        if errors.HasType(err, &kvpb.BatchTimestampBeforeGCError{}) ||
           errors.HasType(err, &kvpb.MVCCHistoryMutationError{}) {
            // 不可恢复错误，直接退出
            if errCallback := f.onUnrecoverableError; errCallback != nil {
                errCallback(ctx, err)
            }
            return
        }
        // ... 日志和 ctx 检查

        // ─── 阶段 5：退避重置判断 ───
        ranFor := start.Elapsed()
        if ranFor > resetThreshold { // 30秒
            i = 1
            r.Reset()
        }
    }
}
```

#### 3.3.1 阶段 1 详解：Frontier 初始化的两条路径

```
withInitialScan=true:
    runInitialScan() 完成后，frontier 中每个 span 的 ts = initialTimestamp
    → 下一次 RangeFeed 从 initialTimestamp+1 开始（catchup scan 区间 = 空集）

withInitialScan=false, resumeWithFrontier=false:
    手动 frontier.Forward(sp, initialTimestamp)
    → 和上面等价，但跳过了数据扫描

resumeWithFrontier=true (StartFromFrontier 路径):
    frontier 已由调用方设置好每个 span 的时间戳
    → RangeFeed 从各 span 自己的时间戳开始，支持分 span 进度恢复
```

这三条路径统一了一个不变量：**进入重试循环之前，`frontier` 中每个 span 必须有合法的起始时间戳**。

#### 3.3.2 阶段 2 详解：`WithBulkDelivery` 的含义

```go
// rangefeed.go:341
rangefeedOpts = append(rangefeedOpts, kvcoord.WithBulkDelivery())
```

- 告诉服务端：可以将多个事件打包为一个 `BulkEvents` 消息发送，减少 channel 写入次数和反序列化开销。
- 客户端的 `processEvent` 中有对应的展开逻辑：先尝试快速路径（全是 Val 事件），回退到逐个处理。
- 这是一种**批处理优化**，对 catchup scan 期间的大量事件尤为显著。

#### 3.3.3 阶段 3 详解：`ctxgroup.GoAndWait` 的并发语义

```go
err := ctxgroup.GoAndWait(ctx, rangeFeedTask, processEventsTask)
```

`ctxgroup.GoAndWait` 做的事情（对应 `pkg/util/ctxgroup`）：
1. 将两个 task 在同一个子 ctx 下并发运行。
2. 任意一个 task 返回**非 nil 错误**时，取消子 ctx，等待另一个 task 也退出。
3. 返回**第一个**出现的非 nil 错误（或若都成功则返回 nil）。

这解决了一个典型的"goroutine 泄漏"问题：若 `rangeFeedTask` 报错（如网络断开），需要取消 `processEventsTask`（它阻塞在 `eventCh` 上）；反之亦然（若 event handler panic，也需要取消 RPC 连接）。

```
rangeFeedTask:    ────────────────✗ (error)
                                   ↓ cancel subCtx
processEventsTask: ───────────────────ctx.Done() ──✗

GoAndWait returns: first error
```

#### 3.3.4 阶段 4 详解：错误分类

```go
// rangefeed.go:397-405
if errors.HasType(err, &kvpb.BatchTimestampBeforeGCError{}) ||
   errors.HasType(err, &kvpb.MVCCHistoryMutationError{}) {
    if errCallback := f.onUnrecoverableError; errCallback != nil {
        errCallback(ctx, err)
    }
    return  // ← 直接退出，不再重试
}
```

两类**不可恢复错误**的含义：

| 错误类型 | 触发条件 | 为何不可重试 |
|---|---|---|
| `BatchTimestampBeforeGCError` | `frontier` 时间戳落后于 GC 阈值 | MVCC 历史已被 GC 删除，无法从该时间点重建事件流 |
| `MVCCHistoryMutationError` | 有 `ClearRange` 等操作破坏了 MVCC 历史 | 历史被物理破坏，订阅者无法获得正确的事件序列 |

对于其他错误（节点宕机、网络分区等），`run()` 通过 `r.Next()` 的指数退避进行重试。

#### 3.3.5 阶段 5 详解：退避重置

```go
// rangefeed.go:425-428
if ranFor > resetThreshold { // 30秒
    i = 1
    r.Reset()
}
```

**为什么需要重置退避？**

假设 RangeFeed 稳定运行了 2 小时，然后因为一次短暂的节点重启失败。如果不重置，下一次重试的等待时间会是 `maxBackoff`（可能 10 秒），合理；但实际上此时可能只需要等 1-2 秒节点就恢复了。

重置逻辑：若本次 RangeFeed 运行超过 30 秒，说明系统是健康的，下次失败应该重新从最小退避开始，避免在集群稳定后因为历史的退避状态导致恢复时间过长。

`i = 1` 而不是 `i = 0` 的细节：第 0 次不打日志，第 1 次才开始打"第 N 次重试"的日志。重置到 1 保留了日志语义的连续性。

---

### 3.4 `runInitialScan()` — 初始全量扫描

**文件**：[scanner.go:28-127](pkg/kv/kvclient/rangefeed/scanner.go#L28)

初始扫描解决的问题：RangeFeed 的 catchup scan 只能提供**订阅时间点之后**的变更。但很多场景需要在订阅前先获取当前数据快照（如 `settingswatcher` 启动时需要全部当前设置值）。

```go
func (f *RangeFeed) runInitialScan(
    ctx context.Context, n *log.EveryN, r *retry.Retry, frontier span.Frontier,
) (canceled bool) {
    // 将 KV 行转换为 RangeFeedValue 事件
    onValue := func(kv roachpb.KeyValue) {
        v := kvpb.RangeFeedValue{Key: kv.Key, Value: kv.Value}
        if !f.useRowTimestampInInitialScan {
            v.Value.Timestamp = f.initialTimestamp  // 统一时间戳
        }
        if f.withDiff {
            v.PrevValue = v.Value  // 初始扫描无"上一个值"，用自身填充
            v.PrevValue.Timestamp = hlc.Timestamp{}
        }
        f.onValue(ctx, &v)
    }
```

**关键设计决策 1：时间戳处理**

- 默认（`useRowTimestampInInitialScan=false`）：所有扫描结果的时间戳统一设为 `initialTimestamp`。
  - 优点：下游处理简单，所有初始数据"看起来"发生在同一时刻。
  - 缺点：丢失了真实的行时间戳信息。

- 设置后（`useRowTimestampInInitialScan=true`）：保留行的真实 MVCC 时间戳。
  - 使用场景：需要精确知道每行最后更新时间的场景（如某些 CDC 应用）。

**关键设计决策 2：PrevValue 填充**

初始扫描的行不是"新写入"，但消费方（如 CDC）可能会检查 `PrevValue` 来判断是否是更新还是插入。用 `v.PrevValue = v.Value` 表示"这一行在订阅开始时就存在，不是新增"，配合空时间戳表示"没有历史"。这个语义约定需要消费方配合理解。

```go
    // 关键：将 frontier 包装为线程安全版本（支持并发扫描）
    frontier = span.MakeConcurrentFrontier(frontier)

    // 包装 OnSpanDone：扫描完一个 span 后推进 frontier
    userSpanDoneCallback := f.scanConfig.OnSpanDone
    f.scanConfig.OnSpanDone = func(ctx context.Context, sp roachpb.Span) error {
        if userSpanDoneCallback != nil {
            if err := userSpanDoneCallback(ctx, sp); err != nil {
                return err
            }
        }
        advanced, err := frontier.Forward(sp, f.initialTimestamp)
        // ...
        return nil
    }
```

**关键设计决策 3：ConcurrentFrontier 包装**

```go
frontier = span.MakeConcurrentFrontier(frontier)
```

`MakeFrontier` 返回的 `btreeFrontier` **不是线程安全的**。初始扫描支持并发（`WithInitialScanParallelismFn`），多个 goroutine 可能同时调用 `OnSpanDone`，进而并发调用 `frontier.Forward()`。`MakeConcurrentFrontier` 包装了一个 `syncutil.Mutex`，使得并发 Forward 安全。

扫描完成后，这个并发版 frontier 被丢弃，`run()` 中后续的访问仍使用原始的非并发 frontier（因为 processEvents 是单线程的）。

```go
    // 重试循环
    r.Reset()
    for r.Next() {
        toScan = toScan[:0]
        for sp, ts := range frontier.Entries() {
            if ts.IsEmpty() || ts.Less(f.initialTimestamp) {
                toScan = append(toScan, sp)  // 只扫描未完成的 span
            }
        }
        if len(toScan) > 0 {
            if err := f.client.Scan(ctx, toScan, f.initialTimestamp, onValue, onValues, f.scanConfig); err != nil {
                // ... 错误处理和重试
                continue
            }
        }
        // 全部完成
        if f.onInitialScanDone != nil { f.onInitialScanDone(ctx) }
        return false
    }
    return true  // ctx 被取消
}
```

**重试幂等性**：`runInitialScan` 中使用 frontier 记录哪些 span 已扫描完成。若扫描中途失败，下次重试只重扫 frontier 中还未推进到 `initialTimestamp` 的 span。这是一种"断点续传"机制，使得初始扫描的重试是幂等的（但非原子的：已扫描的 span 的行可能被再次传给 `onValue`，因此 **文档明确说明"rows may be observed multiple times"**）。

---

### 3.5 `processEvents()` / `processEvent()` — 事件分发中枢

**文件**：[rangefeed.go:432-533](pkg/kv/kvclient/rangefeed/rangefeed.go#L432)

```go
func (f *RangeFeed) processEvents(
    ctx context.Context, frontier span.Frontier, eventCh <-chan kvcoord.RangeFeedMessage,
) error {
    for {
        select {
        case ev := <-eventCh:
            if err := f.processEvent(ctx, frontier, ev.RangeFeedEvent, ev.RegisteredSpan); err != nil {
                return err
            }
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}
```

这是一个标准的**事件循环**（Event Loop）模式。注意：
- `select` 没有 `default` 分支，即**无忙等待**，CPU 友好。
- `ctx.Done()` 保证在 `rangeFeedTask` 出错（进而取消 ctx）时能立即退出，不会卡在 `eventCh` 上。

---

### 3.6 `processEvent()` — 事件分类路由（Switch 核心）

**文件**：[rangefeed.go:448-533](pkg/kv/kvclient/rangefeed/rangefeed.go#L448)

```go
func (f *RangeFeed) processEvent(
    ctx context.Context, frontier span.Frontier,
    ev *kvpb.RangeFeedEvent, registeredSpan roachpb.Span,
) error {
    switch {
    case ev.Val != nil:       // 普通值变更
    case ev.Checkpoint != nil:  // watermark 推进
    case ev.SST != nil:       // SST 文件注入
    case ev.DeleteRange != nil: // MVCC 范围删除
    case ev.Metadata != nil:  // 元数据事件
    case ev.Error != nil:     // 错误（静默，由 RangeFeed RPC 返回值携带）
    case ev.BulkEvents != nil: // 批量事件（优化路径）
    }
}
```

**逐事件类型分析**：

#### ① Val 事件（最常见）

```go
case ev.Val != nil:
    f.onValue(ctx, ev.Val)
```

直接调用用户回调，无任何过滤或变换。注意 `onValue` 是在 `processEvents` goroutine 中调用的，即**回调是同步的**。用户回调不能阻塞太久，否则会积压 `eventCh`，进而背压到服务端（channel 满了生产者会 block）。

#### ② Checkpoint 事件（watermark 推进）

```go
case ev.Checkpoint != nil:
    ts := ev.Checkpoint.ResolvedTS
    if f.frontierQuantize != 0 {
        ts.Logical = 0
        ts.WallTime -= ts.WallTime % int64(f.frontierQuantize)
    }
    advanced, err := frontier.Forward(ev.Checkpoint.Span, ts)
    if err != nil { return err }
    if f.onCheckpoint != nil { f.onCheckpoint(ctx, ev.Checkpoint) }
    if advanced && f.onFrontierAdvance != nil {
        f.onFrontierAdvance(ctx, frontier.Frontier())
    }
    if f.frontierVisitor != nil {
        f.frontierVisitor(ctx, advanced, frontier)
    }
```

这是最复杂的事件类型。逐步解析：

**步骤 1：时间戳量化（可选）**
```go
if f.frontierQuantize != 0 {
    ts.Logical = 0
    ts.WallTime -= ts.WallTime % int64(f.frontierQuantize)
}
```
将时间戳截断到最近的量化边界（如 1 秒）。目的：减少 B-Tree frontier 中的 span 碎片。当多个 span 的时间戳量化到同一值时，它们可以合并为一个节点，减少内存和查找开销。

**步骤 2：推进 Frontier**
```go
advanced, err := frontier.Forward(ev.Checkpoint.Span, ts)
```
`frontier.Forward()` 内部调用 `btreeFrontier.forward()`，会执行：
- 在 B-Tree 中找到与 `span` 重叠的所有 `btreeFrontierEntry`。
- 若某个 entry 的 ts 小于新 ts，更新它（也需要更新 minHeap 中的位置）。
- 若 span 边界不对齐（如传入 span 比某个 entry 大），需要分裂 entry。
- 相邻 entry 时间戳相同时合并（减少碎片）。

`advanced=true` 当且仅当全局最小时间戳（`frontier.Frontier()` = minHeap 堆顶）也随之提升了。

**步骤 3：三层回调**
```
onCheckpoint:         每次收到 checkpoint 都调用（可能不 advance frontier 整体）
onFrontierAdvance:    只在 frontier 整体推进时调用（下游关心"最慢的 span 到哪了"）
frontierVisitor:      每次 checkpoint 后都调用，传入完整的 frontier 快照
```

这三层回调满足不同细粒度的需求，调用方按需选择。

#### ③ BulkEvents — 批量事件优化路径

```go
case ev.BulkEvents != nil:
    if f.onValues != nil {
        allValues := true
        buf := make([]kv.KeyValue, len(ev.BulkEvents.Events))
        for i := range ev.BulkEvents.Events {
            if ev.BulkEvents.Events[i].Val != nil {
                buf[i] = kv.KeyValue{
                    Key:   ev.BulkEvents.Events[i].Val.Key,
                    Value: &ev.BulkEvents.Events[i].Val.Value,
                }
            } else {
                allValues = false
                break
            }
        }
        if allValues {
            f.onValues(ctx, buf)
            return nil
        }
    }
    // 回退到逐个处理
    for _, e := range ev.BulkEvents.Events {
        if err := f.processEvent(ctx, frontier, e, registeredSpan); err != nil {
            return err
        }
    }
```

**两条路径**：
- **快速路径**（全是 Val 事件 + 用户注册了 `onValues`）：一次性构建 `[]kv.KeyValue`，调用批量回调，避免 N 次函数调用和 N 次 interface dispatch 开销。
- **回退路径**：遇到非 Val 事件（如 range tombstone），或用户没注册 `onValues`，逐个递归调用 `processEvent`。

注意：批量路径绕过了 `useRowTimestampInInitialScan` 和 `withDiff` 的处理，因为这些处理是逐行的。注释明确说明这是 `onValues` 回调的使用限制（`config.go:186-196`）。

#### ④ Error 事件（静默处理）

```go
case ev.Error != nil:
    // Intentionally do nothing, we'll get an error returned from the
    // call to RangeFeed.
```

服务端会在关闭流之前发送一个 Error 事件，然后关闭 gRPC stream，使得 `DB.RangeFeed()` 的调用返回非 nil 错误。在客户端这里收到 Error 事件时什么都不做，等待 RPC 调用自然返回。这避免了重复处理错误的问题。

---

### 3.7 `dbAdapter` — 向下层对接

**文件**：[db_adapter.go](pkg/kv/kvclient/rangefeed/db_adapter.go)

#### `dbAdapter.RangeFeed()` — 转发到 DistSender

```go
// db_adapter.go:67-83
func (dbc *dbAdapter) RangeFeed(
    ctx context.Context, spans []roachpb.Span, startFrom hlc.Timestamp,
    eventC chan<- kvcoord.RangeFeedMessage, opts ...kvcoord.RangeFeedOption,
) error {
    timedSpans := make([]kvcoord.SpanTimePair, 0, len(spans))
    for _, sp := range spans {
        timedSpans = append(timedSpans, kvcoord.SpanTimePair{
            Span:       sp,
            StartAfter: startFrom,  // 注意：exclusive
        })
    }
    return dbc.distSender.RangeFeed(ctx, timedSpans, eventC, opts...)
}
```

**关键语义**：`StartAfter` 是 exclusive（不包含该时间戳），即服务端只发送 `ts > startFrom` 的事件。这与 `runInitialScan` 在 `initialTimestamp` 处扫描数据形成互补：扫描获取 `ts <= initialTimestamp` 的快照，RangeFeed 获取 `ts > initialTimestamp` 的增量。

#### `dbAdapter.RangeFeedFromFrontier()` — 按 Span 粒度恢复

```go
// db_adapter.go:85-101
func (dbc *dbAdapter) RangeFeedFromFrontier(...) error {
    timedSpans := make([]kvcoord.SpanTimePair, 0, frontier.Len())
    for sp, ts := range frontier.Entries() {
        timedSpans = append(timedSpans, kvcoord.SpanTimePair{
            Span:       sp.Clone(),  // 注意：必须 Clone！
            StartAfter: ts,
        })
    }
    return dbc.distSender.RangeFeed(ctx, timedSpans, eventC, opts...)
}
```

**为什么必须 Clone span？**

注释说明："the rangefeed progress tracker will manipulate the original frontier"。DistSender 内部会操作传入的 span（如截断、分割）。若不 clone，会直接修改 frontier 中的 span 底层字节，导致 frontier 数据结构损坏（B-Tree 的 Key 被改变，但树的排序没有更新）。

#### `dbAdapter.Scan()` — 并行初始扫描

```go
// db_adapter.go:104-168
func (dbc *dbAdapter) Scan(...) error {
    // 无并行：串行扫描每个 span
    if cfg.scanParallelism == nil {
        for _, sp := range spans {
            if err := dbc.scanSpan(...); err != nil { return err }
        }
        return nil
    }

    // 有并行：用 divideAndSendScanRequests 按 Range 边界分割并并发扫描
    g := ctxgroup.WithContext(ctx)
    err := dbc.divideAndSendScanRequests(ctx, &g, spans, ...)
    return errors.CombineErrors(err, g.Wait())
}
```

#### `dbAdapter.scanSpan()` — 单 Span 分页扫描

```go
// db_adapter.go:171-234
func (dbc *dbAdapter) scanSpan(...) error {
    return dbc.db.TxnWithAdmissionControl(ctx,
        kvpb.AdmissionHeader_ROOT_KV,
        admissionPri,  // BulkNormalPri 或 NormalPri
        kv.SteppingDisabled,
        func(ctx context.Context, txn *kv.Txn) error {
            txn.SetFixedTimestamp(ctx, asOf)  // as-of 查询
            sp := span
            var b kv.Batch
            for {
                b.Header.TargetBytes = targetScanBytes  // 512 KiB 分页
                b.Scan(sp.Key, sp.EndKey)
                txn.Run(ctx, &b)
                res := b.Results[0]
                // ... 回调每行
                if res.ResumeSpan == nil { break }  // 扫描完成
                sp = res.ResumeSpanAsValue()  // 继续下一页
                b = kv.Batch{}
            }
        })
}
```

**核心机制：分页扫描（Pagination）**

`TargetBytes = 512 KiB` 告诉服务端每次 RPC 最多返回 512 KiB 数据。若数据超过这个大小，服务端返回 `ResumeSpan` 指向下一页的起始 key。客户端更新 `sp = res.ResumeSpanAsValue()` 后继续下一轮 Scan，直到 `ResumeSpan == nil`。

这避免了一次扫描可能拉取 GB 级数据导致 OOM 的问题。配合 `WithMemoryMonitor` 还可以控制内存预算。

**admission control 优先级**：
- 系统表（`overSystemTable=true`）：使用 `NormalPri`，不受 bulk 流量降级影响。
- 普通订阅：使用 `BulkNormalPri`，在负载高时会被 AC 降级，避免影响前台流量。

#### `divideAndSendScanRequests()` — 按 Range 边界并行扫描

```go
// db_adapter.go:239-300
func (dbc *dbAdapter) divideAndSendScanRequests(...) error {
    ri := kvcoord.MakeRangeIterator(dbc.distSender)
    exportLim := limit.MakeConcurrentRequestLimiter("rangefeedScanLimiter", parallelismFn())

    for _, sp := range sg.Slice() {
        nextRS, _ := keys.SpanAddr(sp)
        for ri.Seek(ctx, nextRS.Key, kvcoord.Ascending); ri.Valid(); ri.Next(ctx) {
            desc := ri.Desc()
            partialRS, _ := nextRS.Intersect(desc.RSpan())
            nextRS.Key = partialRS.EndKey

            limAlloc, _ := exportLim.Begin(ctx)  // 获取并发令牌
            sp := partialRS.AsRawSpanWithNoLocals()
            workGroup.GoCtx(func(ctx context.Context) error {
                defer limAlloc.Release()
                return dbc.scanSpan(ctx, sp, ...)
            })
        }
    }
    return nil
}
```

**设计**：
1. 使用 `RangeIterator` 遍历目标 span 覆盖的所有 Range（每个 Range 是 Raft 复制单元，约 512 MB）。
2. 将每个 Range-内子 span 作为独立的扫描任务提交给 `workGroup`（`ctxgroup`）。
3. `ConcurrentRequestLimiter` 控制最大并发度，默认最大 64（`kv.rangefeed.max_scan_parallelism`）。
4. 动态调整：`parallelismFn()` 在每次提交前调用，允许在扫描过程中动态调整并发度。
