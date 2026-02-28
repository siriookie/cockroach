# CockroachDB RangeFeed 客户端实现深度解析（下篇）

> 本篇继续上篇内容，包含具体运行示例、Frontier 内部机制、并发模型与补充知识。
> 上篇：`rangefeed_deep_dive_part1.md`

---

## 四、具体运行示例

### 4.1 正常场景：`settingswatcher` 订阅系统设置变更

**背景**：CockroachDB 的 `settingswatcher` 订阅 `settings` 系统表（Span: `[/Table/6/, /Table/7/)`），在集群启动时获取所有当前设置值，并实时监听变化。

#### 时间线

```
T=0   调用方: factory.RangeFeed(ctx, "settingswatcher",
              spans=[/Table/6/, /Table/7/)],
              initialTs=@10.0,   // HLC walltime=10s
              onValue=updateSetting,
              WithInitialScan(onDone),
              WithSystemTablePriority(),
              WithOnFrontierAdvance(logProgress))

T=0   start() 执行:
      - CAS: started 0→1 ✓
      - MakeFrontier([/Table/6/, /Table/7/)) → frontier = {[/Table/6/,/Table/7/): ts=0}
      - stopper.RunAsyncTask() → 启动 goroutine G1

T=0.1 G1: run() 开始
      withInitialScan=true → 进入 runInitialScan()

T=0.1 runInitialScan():
      frontier = MakeConcurrentFrontier(frontier)
      toScan = [/Table/6/, /Table/7/)   // frontier ts=0 < initialTs=10

      client.Scan(ctx, toScan, @10.0, onValue, nil, cfg)
        → scanSpan: TxnWithAdmissionControl(NormalPri)
          → txn.SetFixedTimestamp(@10.0)
          → Scan(/Table/6/, /Table/7/, TargetBytes=512KiB)
          → 服务端返回: rows=[("cluster.version","22.2"), ("sql.defaults.vectorize","on"), ...]
          → 对每行调用 onValue():
              v.Key = "cluster.version", v.Value.Timestamp = @10.0
              updateSetting(ctx, &v)  // 更新本地 settings map
              ...

          假设数据量 < 512KiB → res.ResumeSpan == nil → 扫描完成

      OnSpanDone(/Table/6/, /Table/7/) 被调用:
        frontier.Forward(/Table/6/,/Table/7/), @10.0)
        → frontier = {[/Table/6/,/Table/7/): ts=@10.0}  ← 推进了

      toScan 下次检查: ts=@10.0 不低于 initialTs=@10.0 → toScan=[]
      → 扫描完成
      onDone(ctx)  // 通知调用方初始扫描完成
      return false (未取消)

T=0.8 runInitialScan 返回 false，进入重试主循环

T=0.8 run() 阶段 2: 构建 RangeFeed 选项
      rangefeedOpts = [WithBulkDelivery(), WithSystemTablePriority()]
      eventCh = make(chan RangeFeedMessage)

T=0.8 run() 第一次循环 (i=0):
      ts = frontier.Frontier() = @10.0

      GoAndWait(ctx,
          rangeFeedTask:      client.RangeFeed(ctx, spans, @10.0, eventCh, opts...)
                              → DistSender.RangeFeed: 向 Range Leader 发起 gRPC stream
                              → 服务端开始 catchup scan (ts > @10.0) + 实时推送
          processEventsTask:  processEvents(ctx, frontier, eventCh)
      )

T=1.0 服务端发来 Checkpoint 事件:
      ev.Checkpoint = {Span:/Table/6/,/Table/7/), ResolvedTS:@10.5}
      processEvent():
        frontier.Forward([/Table/6/,/Table/7/), @10.5) → advanced=true
        onCheckpoint(ctx, ev.Checkpoint)
        onFrontierAdvance(ctx, @10.5)  // logProgress 被调用

T=2.0 用户执行: SET CLUSTER SETTING sql.defaults.vectorize = 'experimental_always'
      → 写入 settings 表, MVCC ts = @20.0

T=2.0 服务端推送 Val 事件:
      ev.Val = {Key:"sql.defaults.vectorize", Value:"experimental_always", Value.Timestamp:@20.0}
      processEvent():
        onValue(ctx, ev.Val)  → updateSetting() 更新本地设置

T=2.1 服务端推送 Checkpoint 事件:
      ev.Checkpoint = {Span:[/Table/6/,/Table/7/), ResolvedTS:@20.0}
      → frontier 推进到 @20.0

T=∞   正常运行，周期性收到 Checkpoint，偶尔收到 Val...
```

**状态追踪表**：

| 时刻 | frontier 状态 | 事件 | 系统行为 |
|---|---|---|---|
| T=0 | {[/Table/6/,7/): @0} | 启动 | 初始扫描开始 |
| T=0.8 | {[/Table/6/,7/): @10.0} | 扫描完成 | RangeFeed RPC 建立 |
| T=1.0 | {[/Table/6/,7/): @10.5} | Checkpoint | frontier 推进，logProgress 触发 |
| T=2.0 | {[/Table/6/,7/): @10.5} | Val 事件 | 设置更新到内存 |
| T=2.1 | {[/Table/6/,7/): @20.0} | Checkpoint | frontier 追上写入时间戳 |

---

### 4.2 边界场景：节点故障 + 超 GC 阈值

**场景**：RangeFeed 稳定运行中，订阅的 Range Leader 所在节点崩溃；同时集群 GC TTL 设置为 2 小时，若 frontier 落后超过 2 小时则触发不可恢复错误。

#### 时间线

```
T=0     RangeFeed 正常运行，frontier = @100.0 (wall time = 100s)
        rangeFeedTask 阻塞在 gRPC stream 接收
        processEventsTask 阻塞在 eventCh select

T=50    节点 N3 崩溃（Range1 的 Leader 在 N3 上）

T=50.1  gRPC 连接断开
        → DistSender.RangeFeed 返回错误: "grpc: connection broken"
        → rangeFeedTask 返回 err

T=50.1  GoAndWait:
        rangeFeedTask 返回 err → 取消子 ctx
        processEventsTask: ctx.Done() 触发 → 返回 ctx.Err()
        GoAndWait 返回 err = "grpc: connection broken"

T=50.1  run() 错误分类:
        - 不是 BatchTimestampBeforeGCError ✓
        - 不是 MVCCHistoryMutationError ✓
        → 可恢复错误，进入退避

        ctx.Err() == nil (用户未关闭)
        restartLogEvery.ShouldLog() → true → 打印警告日志

        ranFor = 50s > resetThreshold(30s)
        → i=1, r.Reset()  ← 重置退避到最小值

T=50.1  r.Next() → 等待最小退避时间 (~500ms)

T=50.7  Raft 重新选举完成，N4 成为新 Leader

T=50.7  run() 第二轮循环:
        ts = frontier.Frontier() = @100.0  ← 保持了上次的 frontier！

        新的 rangeFeedTask:
          client.RangeFeed(ctx, spans, @100.0, eventCh, opts...)
          → DistSender 路由到 N4 (新 Leader)
          → 服务端从 @100.0 开始 catchup scan
          → 服务端重放 T=50 期间错过的写入事件
          → 恢复正常推送

        processEventsTask 重新启动

T=50.8  收到 catchup 事件，系统恢复正常
```

**极端情况：超 GC 阈值**

```
[假设] 网络分区导致 RangeFeed 断开，分区持续 3 小时（> GC TTL 2小时）

T=0     frontier = @1000.0 (wall time = 1000s)
T=10800 分区恢复，RangeFeed 尝试重连
        ts = frontier.Frontier() = @1000.0

        client.RangeFeed(ctx, spans, @1000.0, eventCh, opts...)
        → 服务端检查: now = @11800.0, GC threshold = @5400.0
        → startFrom (@1000.0) < GC threshold (@5400.0) !!
        → 服务端返回: BatchTimestampBeforeGCError{Timestamp:@1000.0, Threshold:@5400.0}

T=10800 GoAndWait 返回 err = BatchTimestampBeforeGCError

T=10800 run() 错误分类:
        errors.HasType(err, &kvpb.BatchTimestampBeforeGCError{}) → true!

        if errCallback := f.onUnrecoverableError; errCallback != nil {
            errCallback(ctx, err)
            // 调用方在此可以:
            // 1. 关闭当前 RangeFeed
            // 2. 创建新的 RangeFeed，重新做初始扫描
        }
        return  ← 永久退出，不再重试

[调用方处理]  // 如 changefeed 的处理方式
        收到 unrecoverable error →
        f.Close() → 等待 run goroutine 退出
        → 创建新 RangeFeed with WithInitialScan → 从头开始
```

**退避状态追踪**（节点故障场景）：

```
第 1 次失败 (T=50.7):  ranFor=50s > 30s → Reset → 等待 ~500ms
第 2 次失败 (T=52.0):  ranFor=1.3s < 30s → 不重置 → 等待 ~1s
第 3 次失败 (T=55.0):  ranFor=3s < 30s → 不重置 → 等待 ~2s
第 4 次失败 (T=62.0):  ranFor=7s < 30s → 不重置 → 等待 ~4s
第 5 次失败 (T=75.0):  ranFor=13s < 30s → 不重置 → 等待 ~8s
...
一旦成功运行 > 30s：下次失败重置回 500ms
```

这个退避策略在"节点频繁抖动"时能防止 thundering herd（大量 RangeFeed 同时重连），同时在"一次短暂故障"后能快速恢复。

---

## 五、补充知识

### 5.1 Frontier 内部机制：B-Tree + MinHeap 双结构

**文件**：[pkg/util/span/frontier.go:121-394](pkg/util/span/frontier.go)

`btreeFrontier` 同时维护两个数据结构，这是理解 Frontier 性能保证的关键。

```
btreeFrontier
├── tree   (btree)          按 span 的 start key 排序的 B-Tree
│   存放 *btreeFrontierEntry
│   用途: 快速找到与某 span 重叠的 entry（范围查询）
│
└── minHeap (frontierHeap)  最小堆，按 ts 排序
    存放 *btreeFrontierEntry（同一批对象！）
    用途: O(1) 查询全局最小时间戳（frontier.Frontier()）
```

每个 `btreeFrontierEntry` 在两个结构中都有引用，修改时需要同步更新两者。

**Forward 操作的时间复杂度**：
- B-Tree 范围查询：O(k log n)，k = 与传入 span 重叠的 entry 数
- Heap 更新（sift up/down）：O(k log n)
- 合并相邻 entry：O(m)，m = 被合并的 entry 数
- 总体：O(k log n)

**minHeap 的关键设计**：堆顶 = 时间戳最小的 entry = 全局 frontier。每次调用 `frontier.Frontier()` 都是 O(1) 的：

```go
// frontier.go:231-236
func (f *btreeFrontier) Frontier() hlc.Timestamp {
    if f.minHeap.Len() == 0 {
        return hlc.Timestamp{}
    }
    return f.minHeap[0].ts  // 堆顶，O(1)
}
```

**为什么需要 B-Tree + Heap 两个结构？**

只用 B-Tree：`Forward` 更新快，但 `Frontier()` 需要遍历所有 entry 找最小 ts，O(n)。
只用 Heap：`Frontier()` 快，但 `Forward` 需要找到与 span 重叠的 entry，按 ts 排序的堆无法高效做 span 范围查询，O(n)。
两者结合：`Forward` O(k log n)，`Frontier()` O(1)，以内存和维护代价换取两个操作都高效。

**量化优化（`frontierQuantize`）对 B-Tree 的影响**：

```
未量化时（设置 ts 精确到纳秒）:
  frontier = {[a,b):@10.001s, [b,c):@10.002s, [c,d):@10.003s}  ← 3 个 entry

量化到 1s 后:
  Checkpoint([a,b), @10.001s) → ts = @10.0s
  Checkpoint([b,c), @10.002s) → ts = @10.0s
  mergeEntries: [a,b) 和 [b,c) 相邻且 ts 相同 → 合并
  frontier = {[a,c):@10.0s}  ← 2 个 entry

  再收到 Checkpoint([c,d), @10.003s) → ts = @10.0s
  mergeEntries: [a,c) 和 [c,d) 相邻且 ts 相同 → 合并
  frontier = {[a,d):@10.0s}  ← 1 个 entry
```

量化将 n 个精细时间戳的 entry 压缩为少量 entry，显著减少 B-Tree 大小，提升 `Forward` 效率。对于订阅大量 Range 的 RangeFeed（如全表 CDC），这个优化非常显著。

---

### 5.2 `ctxgroup.GoAndWait` 并发模型详解

`ctxgroup` 是 CockroachDB 内部对 `errgroup` 的扩展，位于 `pkg/util/ctxgroup`。

`GoAndWait` 的语义等价于：

```go
// 伪代码说明语义
func GoAndWait(ctx context.Context, fns ...func(context.Context) error) error {
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    g := errgroup.Group{}
    for _, fn := range fns {
        fn := fn
        g.Go(func() error {
            err := fn(ctx)
            if err != nil {
                cancel()  // 任意一个出错，取消其他
            }
            return err
        })
    }
    return g.Wait()  // 等待所有完成
}
```

在 `run()` 中的应用：

```
rangeFeedTask:                                 processEventsTask:
  blockingRPC(...)                              for { select { case <-eventCh } }
                                                        ↑
                                                   阻塞等待
  ─────── 网络断开 ─────→ 返回 err

  cancel() 被触发
                                                ctx.Done() ←─── 取消信号
                                                返回 ctx.Err()

GoAndWait 返回第一个 err
```

**为什么不用 `sync.WaitGroup` + `chan error`？**

因为需要"任一失败就取消其他"的语义，而 `sync.WaitGroup` 没有取消机制。`ctxgroup` 将 context 取消和错误收集集成在一起，代码更简洁，也更不容易出错。

---

### 5.3 HLC 时间戳与 Exclusive 语义

CockroachDB 使用 **HLC（Hybrid Logical Clock）** 作为事务时间戳，定义在 `pkg/util/hlc`。

```go
// HLC 时间戳结构
type Timestamp struct {
    WallTime int64   // 纳秒级 wall clock
    Logical  int32   // 逻辑计数器（同一 wall time 内的排序）
}
```

**RangeFeed 中的 Exclusive 起始语义**（来自 `rangefeed.go:134-138` 的注释）：

```
// NB: for the rangefeed itself, initialTimestamp is exclusive, i.e. the first
// possible event emitted by the server (including the catchup scan) is at
// initialTimestamp.Next(). This follows from the gRPC API semantics. However,
// the initial scan (if any) is run at initialTimestamp.
```

这意味着：
- **初始扫描** at `@T`：读取 `ts <= T` 的数据（as-of 语义，inclusive）。
- **RangeFeed 流** from `@T`：只接收 `ts > T` 的事件（exclusive）。

二者的衔接是完美的：时间轴上的任意写入，要么出现在初始扫描的快照中（`ts <= T`），要么出现在 RangeFeed 流中（`ts > T`），不会有遗漏或重复（在无故障的正常情况下）。

**`.Next()` 方法的作用**：

```go
// ts.Next() 返回 ts 的直接后继（最小的 ts' > ts）
func (t Timestamp) Next() Timestamp {
    if t.Logical == math.MaxInt32 {
        return Timestamp{WallTime: t.WallTime + 1}
    }
    return Timestamp{WallTime: t.WallTime, Logical: t.Logical + 1}
}
```

服务端内部将 `startFrom` 转化为 `startFrom.Next()`，以实现 exclusive 语义。

---

### 5.4 与 Raft + ClosedTimestamp 的关系

理解 RangeFeed 的 Checkpoint 事件，需要了解服务端的 **ClosedTimestamp** 机制。

```
Raft 日志
────────────────────────────────────────────────────→ 时间
写入A@T1  写入B@T2  写入C@T3  ClosedTs@T3  写入D@T4
                                ↑
                         此点之前的写入已全部 commit
                         不会有新的 ts ≤ T3 的写入

ClosedTimestamp 推进 → 服务端可以安全地发送 Checkpoint(span, T3)
                      表明 "我保证不会再有 ts ≤ T3 的事件了"
```

**ClosedTimestamp** 由 `pkg/kv/kvserver/closedts` 模块维护。每个 Range 定期（默认 200ms）推进 closed timestamp，一旦推进，服务端的 RangeFeed processor 就可以向所有订阅者发送 Checkpoint 事件。

这就是为什么 Checkpoint（watermark）是**周期性**的而不是每次写入后立即发送：它们由 closed timestamp 的推进触发，而非写入事件本身。

**对客户端 frontier 的影响**：

```
normal frontier 推进模式:
  写入 @T1 → Val 事件 (无 frontier 推进)
  写入 @T2 → Val 事件 (无 frontier 推进)
  ClosedTs@T2 → Checkpoint → frontier 推进到 T2
  写入 @T3 → Val 事件 (无 frontier 推进)
  ClosedTs@T3 → Checkpoint → frontier 推进到 T3

frontier 的语义: "我已收到所有 ts ≤ frontier 的事件"
```

这意味着在两次 Checkpoint 之间，`frontier.Frontier()` 保持不变，即使有 Val 事件进来。这是正确的：Val 事件本身不能证明"我已收到所有更早的事件"，只有 Checkpoint 才有这个语义保证。

---

### 5.5 admission control 与背压机制

CockroachDB 的 AC（Admission Control）系统位于 `pkg/util/admission`，对 RangeFeed 有两个影响点：

**① 初始扫描的 admission priority**

```go
// db_adapter.go:189-191
admissionPri := admissionpb.BulkNormalPri
if overSystemTable {
    admissionPri = admissionpb.NormalPri
}
```

- `BulkNormalPri`：批量操作优先级，在 IO 负载高时会被 AC 降速甚至暂停，以保护前台查询。
- `NormalPri`：正常优先级，系统内部功能（sqlliveness、settingswatcher 等）使用此优先级，确保即使在负载高时也能正常工作。

**② RangeFeed 流的背压**

RangeFeed 的 `eventCh` 是**无缓冲** channel：

```go
// rangefeed.go:334
eventCh := make(chan kvcoord.RangeFeedMessage)
```

当 `processEventsTask` 的 `onValue` 回调处理慢时：
- `processEvents` 阻塞在 `eventCh` 读取上（等待）。
- 实际上此时 channel 不是读取阻塞，而是写入阻塞：服务端 DistSender 向 channel 写入时会阻塞。
- 这形成了**自然的背压链**：

```
服务端 Range processor
    → gRPC stream
    → DistSender (写入 eventCh)
    ← 背压 ← eventCh (无缓冲，满了阻塞写入)
    ← 背压 ← processEventsTask (onValue 慢)
```

> **注意**：代码注释（rangefeed.go:332-333）提到了考虑增加 event buffering，但当前实现是无缓冲的。这意味着慢的 event handler 会直接背压到服务端，可能导致服务端的 RangeFeed processor 内存积压。生产中应确保 `onValue` 回调不阻塞。

---

### 5.6 Invoker 模式 — 可观测性的扩展点

```go
// config.go:37
invoker func(func() error) error
```

`invoker` 是一个"装饰器"：

```go
// rangefeed.go:249-256
if f.invoker != nil {
    _ = f.invoker(func() error {
        f.run(ctx, frontier, resumeFromFrontier)
        return nil
    })
    return
}
f.run(ctx, frontier, resumeFromFrontier)
```

使用场景（如在 tracing 中包装）：

```go
// 调用方可以这样注入追踪逻辑
WithInvoker(func(fn func() error) error {
    sp := openTraceSpan("rangefeed-work")
    defer sp.Finish()
    return fn()
})
```

所有 `rangeFeedTask` 和 `processEventsTask` 都在这个 invoker 的 goroutine 调用栈中运行，使得 pprof 的 goroutine 追踪、OpenTracing span 等能看到调用方的 context。

---

### 5.7 并发安全性总结

| 结构/方法 | 并发安全性 | 原因 |
|---|---|---|
| `RangeFeed.started` | ✅ 安全 | `atomic.CompareAndSwapInt32` |
| `RangeFeed.cancel` | ✅ 安全 | 只在 `start()` 中写入一次 |
| `RangeFeed.running` | ✅ 安全 | `sync.WaitGroup` 本身线程安全 |
| `frontier`（run循环中）| ✅ 安全 | GoAndWait 保证同时只有 processEventsTask 写 |
| `frontier`（初始扫描）| ✅ 安全 | `MakeConcurrentFrontier` 包装了 mutex |
| `config` 字段 | ✅ 安全 | start 后只读，不再修改 |
| `eventCh` | ✅ 安全 | Go channel 本身线程安全 |
| `onValue` 回调 | ⚠️ 调用方保证 | processEvents 单线程调用，但回调内部需调用方保证安全 |

**关键不变量（重申）**：

1. `frontier` 的写入（`Forward`）在整个 `run()` 生命周期内是串行的：要么在 `runInitialScan` 中（单线程），要么在 `processEventsTask` 中（单线程），二者不重叠（由 `GoAndWait` 保证）。

2. `started` 字段保证 `start()` 只能被调用一次，防止多次启动导致 `running.Add(1)` 和 `running.Done()` 失衡。

3. `close()` 的正确性依赖于：`f.cancel()` → ctx 取消 → `GoAndWait` 两个 task 都退出 → `run()` 的 `r.Next()` 返回 false → `run()` 返回 → `running.Done()` → `Close()` 的 `running.Wait()` 返回。

---

### 5.8 测试架构 — TestingKnobs 与 Mock

**TestingKnobs**（rangefeed.go:88-98）：

```go
type TestingKnobs struct {
    OnRangefeedRestart func()           // 每次重启时调用
    IgnoreOnDeleteRangeError bool       // 忽略 DeleteRange 事件无 handler 的错误
}
```

**mockgen 生成的 DB mock**（rangefeed.go:35）：

```go
//go:generate mockgen -destination=mocks_generated_test.go --package=rangefeed . DB
```

通过 mock DB 接口，测试可以：
- 精确控制 `RangeFeed()` 返回特定错误，验证重试逻辑。
- 注入特定事件序列，验证 `processEvent` 的行为。
- 模拟不可恢复错误，验证 `onUnrecoverableError` 回调。

---

## 六、设计模式总结

| 模式 | 在代码中的体现 | 优势 |
|---|---|---|
| **Functional Options** | `config.go` 中的 `Option` / `optionFunc` | 稳定的 API，自描述的配置 |
| **Dependency Injection** | `DB` 接口 + `dbAdapter` | 可测试性，可替换底层实现 |
| **Ownership Transfer** | `ownsFrontier` 参数 | 明确内存责任，避免泄漏 |
| **Event Loop** | `processEvents` 的 select 循环 | 无忙等待，CPU 友好 |
| **Decorator / Wrapper** | `invoker` 函数，`MakeConcurrentFrontier` | 无侵入式地添加横切关注点 |
| **Circuit Breaker (简化版)** | 不可恢复错误直接 return | 防止在无效状态下无限重试 |
| **Exponential Backoff with Reset** | `retry.StartWithCtx` + `resetThreshold` | 平衡快速恢复和 thundering herd 防止 |

---

## 七、完整调用链一览

```
Factory.RangeFeed()
└── Factory.New()         构造 RangeFeed，initConfig
└── RangeFeed.Start()     创建 frontier，调用 start()
    └── RangeFeed.start()  CAS 保护，stopper.RunAsyncTask 启动 goroutine
        └── RangeFeed.run()  主循环
            ├── runInitialScan()          (若 withInitialScan)
            │   ├── dbAdapter.Scan()
            │   │   ├── dbAdapter.scanSpan()    串行模式
            │   │   └── divideAndSendScanRequests()  并行模式
            │   │       └── dbAdapter.scanSpan()    × N goroutines
            │   └── frontier.Forward()        每个 span 完成后推进
            │
            └── ctxgroup.GoAndWait()          每次重试启动两个 goroutine
                ├── rangeFeedTask
                │   └── dbAdapter.RangeFeed()
                │       └── DistSender.RangeFeed()  → gRPC stream to Range servers
                │
                └── processEventsTask
                    └── RangeFeed.processEvents()
                        └── RangeFeed.processEvent()
                            ├── Val         → onValue()
                            ├── Checkpoint  → frontier.Forward() + onCheckpoint() + onFrontierAdvance()
                            ├── SST         → onSSTable()
                            ├── DeleteRange → onDeleteRange()
                            ├── Metadata    → onMetadata()
                            ├── Error       → (忽略)
                            └── BulkEvents  → onValues() 或展开后逐个 processEvent()
```

---

*文档生成时间：2026-02-28*
*分析文件版本：CockroachDB main branch（commit cc6689469ff 附近）*
*作者：Claude Code（基于源代码静态分析）*
