# KVSubscriber 模块深度源码分析（上）

## 目录

1. [系统定位与架构概览](#1-系统定位与架构概览)
2. [第一轮 BFS：控制流与组件协作](#2-第一轮-bfs控制流与组件协作)
3. [DFS 深入：KVSubscriber 构造与初始化](#3-dfs-深入kvsubscriber-构造与初始化)
4. [DFS 深入：rangefeedcache.Watcher 机制](#4-dfs-深入rangefeedcachewatcher-机制)
5. [DFS 深入：SpanConfigDecoder 事件翻译](#5-dfs-深入spanconfigdecoder-事件翻译)
6. [DFS 深入：Start 与指标监控](#6-dfs-深入start-与指标监控)

---

## 1. 系统定位与架构概览

### 1.1 什么是 Span Configuration

CockroachDB 将所有数据组织为有序的 key-value 对，按 range 分片（默认 512MB）。每个 range 可以拥有独立的配置（`roachpb.SpanConfig`），包括：

| 配置项 | 含义 |
|--------|------|
| `NumReplicas` | 副本数 |
| `NumVoters` | 投票者数 |
| `GCPolicy.TTLSeconds` | MVCC GC 保留时间 |
| `GCPolicy.ProtectionPolicies` | 受保护时间戳策略 |
| `Constraints` | 副本放置约束 |
| `ExcludeDataFromBackup` | 是否排除备份 |

这些配置存储在系统表 `system.span_configurations` 中，由 SQL 层（`SQLTranslator` + `Reconciler`）写入。KV 层需要实时读取这些配置来决定 range 的行为（分裂、GC、副本放置等）。

### 1.2 KVSubscriber 的角色

`KVSubscriber` 是 **KV 层读取 Span Configuration 的唯一入口**。它的核心职责是：

1. **实时同步**：通过 Rangefeed 监听 `system.span_configurations` 表的变更
2. **内存缓存**：维护一个内存中的 `spanconfig.Store`，提供高效的 key → config 查询
3. **变更通知**：通知已注册的 handler（如 split queue、GC queue）哪些 span 的配置发生了变化
4. **受保护时间戳查询**：提供 `GetProtectionTimestamps` 接口，供 GC 决策使用

### 1.3 架构总览

```
┌──────────────────────────────────────────────────────────────────────┐
│                        SQL Layer                                     │
│  ALTER TABLE t CONFIGURE ZONE USING num_replicas=5;                  │
│                                                                      │
│  SQLTranslator → Reconciler → KVAccessor.UpdateSpanConfigRecords()  │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ KV Write (system.span_configurations)
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  system.span_configurations 表                       │
│  Row: [start_key, end_key, span_config_proto]                        │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ Rangefeed (CDC)
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  rangefeedcache.Watcher                               │
│  ┌─────────────┐  ┌──────────────────┐  ┌────────────────────────┐  │
│  │ RangeFeed   │→ │ Buffer (bounded) │→ │ Frontier (resolved TS) │  │
│  │ onValue()   │  │ BufferEvent[]    │  │ → Flush() → Update     │  │
│  └─────────────┘  └──────────────────┘  └────────────┬───────────┘  │
└──────────────────────────────────────────────────────┼───────────────┘
                               │                       │
                    TranslateEvent()            handleUpdate()
                    (SpanConfigDecoder)                 │
                                               ┌───────▼───────┐
                                               │ KVSubscriber   │
                                               │                │
                                               │ mu.internal    │← spanconfig.Store
                                               │ mu.handlers    │← []handler
                                               │ mu.lastUpdated │← hlc.Timestamp
                                               └───────┬───────┘
                                                       │
                                    ┌──────────────────┼──────────────────┐
                                    │                  │                  │
                             ┌──────▼──────┐  ┌───────▼───────┐ ┌───────▼──────┐
                             │ Split Queue │  │  GC Queue     │ │ Replica      │
                             │ NeedsSplit()│  │ GetProtection │ │ GetSpanConfig│
                             │ ComputeSplit│  │ Timestamps()  │ │ ForKey()     │
                             └─────────────┘  └───────────────┘ └──────────────┘
```

**核心组件清单**：

| 文件 | 职责 |
|------|------|
| `pkg/spanconfig/spanconfig.go` | 接口定义：`KVSubscriber`, `StoreReader`, `ProtectedTSReader`, `Store` |
| `pkg/spanconfig/spanconfigkvsubscriber/kvsubscriber.go` | KVSubscriber 核心实现：初始化、订阅、更新处理 |
| `pkg/spanconfig/spanconfigkvsubscriber/spanconfigdecoder.go` | 将 KV 行事件解码为 `spanconfig.Update` |
| `pkg/kv/kvclient/rangefeed/rangefeedcache/watcher.go` | 通用 rangefeed 缓存框架：初始扫描 + 增量更新 |
| `pkg/spanconfig/spanconfigstore/store.go` | 内存 span config 存储：区间 B-tree + 查询 |

---

## 2. 第一轮 BFS：控制流与组件协作

### 2.1 接口层次

KVSubscriber 实现了两个组合接口：

```go
// pkg/spanconfig/spanconfig.go:102
type KVSubscriber interface {
    StoreReader          // NeedsSplit, ComputeSplitKey, GetSpanConfigForKey
    ProtectedTSReader    // GetProtectionTimestamps
    LastUpdated() hlc.Timestamp
    Subscribe(func(ctx context.Context, updated roachpb.Span))
}
```

其中：

```go
// StoreReader (spanconfig.go:279)
type StoreReader interface {
    NeedsSplit(ctx context.Context, start, end roachpb.RKey) (bool, error)
    ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) (roachpb.RKey, error)
    GetSpanConfigForKey(ctx context.Context, key roachpb.RKey) (roachpb.SpanConfig, roachpb.Span, error)
}

// ProtectedTSReader (spanconfig.go:508)
type ProtectedTSReader interface {
    GetProtectionTimestamps(ctx context.Context, sp roachpb.Span) (
        protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, _ error,
    )
}
```

**设计意图**：KVSubscriber 同时扮演 **配置查询者** 和 **受保护时间戳查询者** 两个角色。调用者无需关心这些数据来自 rangefeed 还是其他来源——它只需要通过接口获取一致性快照。

### 2.2 数据流时序图

```
 Time ──────────────────────────────────────────────────────────────►

 SQL Layer:  ALTER TABLE t CONFIGURE ZONE ...
             │
             ▼
 KV Write:   system.span_configurations row updated (txn commit @ ts=T1)
             │
             ▼
 RangeFeed:  RangeFeedValue{Key, Value, PrevValue, Timestamp=T1}
             │
             ▼ (每个事件立即到达)
 Watcher:    TranslateEvent() → BufferEvent{Update, ts=T1}
             │                   加入 Buffer
             │
             ▼ (等待 closed timestamp 推进, ~3s 延迟)
 Watcher:    Frontier 推进到 T2 (T2 >= T1)
             │
             ▼ buffer.Flush(T2)
 Watcher:    handleUpdate() → IncrementalUpdate{Events, Timestamp=T2}
             │
             ▼
 KVSubscriber: handlePartialUpdate()
               │
               ├── sort events (deletions before additions)
               ├── mu.Lock()
               ├── for each event: mu.internal.Apply(ev.Update)
               ├── mu.lastUpdated = T2
               ├── mu.Unlock()
               │
               └── for each handler: handler.invoke(ctx, target.KeyspaceTargeted())
                   │
                   ├── Split Queue: 检查是否需要分裂
                   ├── GC Queue: 检查 GC 策略变更
                   └── ...
```

**关键延迟**：从 SQL 提交到 KV 层感知，延迟约 `kv.closed_timestamp.target_duration`（默认 3s）+ `kv.rangefeed.closed_timestamp_refresh_interval`（默认 3s）。这不是 bug，而是 rangefeed 的一致性保证所要求的。

### 2.3 两种更新模式

KVSubscriber 处理两种截然不同的更新：

| 更新类型 | 触发时机 | 处理方式 | 对读者的影响 |
|----------|----------|----------|-------------|
| `CompleteUpdate` | 初始扫描完成 / rangefeed 重建 | 创建全新 Store 并原子替换 | 读者看到完整一致的快照 |
| `IncrementalUpdate` | Frontier 推进 | 在现有 Store 上增量 Apply | 读者看到逐步更新的视图 |

**CompleteUpdate 的 Store 替换策略**是 KVSubscriber 最核心的设计决策（详见 DFS 部分）。

### 2.4 模块间交互总结

| 交互对象 | 交互方式 | 方向 | 频率 |
|----------|----------|------|------|
| `rangefeedcache.Watcher` | 回调 `handleUpdate` | ← Watcher 推送 | 每 ~3s（frontier bump） |
| `spanconfigstore.Store` | `Apply()`, `NeedsSplit()`, etc. | → Store 查询/写入 | 随事件/读请求 |
| `SpanConfigDecoder` | `TranslateEvent()` | ← Watcher 调用 | 每个 rangefeed 事件 |
| Split Queue / GC Queue | `Subscribe()` 注册的 handler | → 通知 | 每次配置变更 |
| `metric.Registry` | gauge 更新 | → 指标上报 | 每 5s (metrics poller) |
| `cluster.Settings` | 读取 `metricsPollerInterval` | ← 配置读取 | 每个指标刷新周期 |

---

## 3. DFS 深入：KVSubscriber 构造与初始化

### 3.1 KVSubscriber 结构体

**文件**：`pkg/spanconfig/spanconfigkvsubscriber/kvsubscriber.go:96-127`

```go
type KVSubscriber struct {
    fallback roachpb.SpanConfig       // 找不到配置时的默认值
    knobs    *spanconfig.TestingKnobs  // 测试注入点
    settings *cluster.Settings         // 集群设置访问

    rfc *rangefeedcache.Watcher[*BufferEvent]  // 底层 rangefeed 缓存

    mu struct {
        syncutil.RWMutex
        lastUpdated hlc.Timestamp      // 最后同步时间戳
        internal    spanconfig.Store   // 内存配置存储（可整体替换）
        handlers    []handler          // 已注册的更新通知回调
    }

    clock        *hlc.Clock
    metrics      *Metrics
    boundsReader spanconfigstore.BoundsReader  // 租户配置边界
}
```

**并发模型**：

- `mu.RWMutex` 保护所有可变状态：`internal`, `handlers`, `lastUpdated`
- **读路径**（`GetSpanConfigForKey`, `NeedsSplit` 等）使用 `RLock` → 多读者并发安全
- **写路径**（`handleCompleteUpdate`, `handlePartialUpdate`）使用 `Lock` → 排他写入
- `rfc`（Watcher）是只写一次的（`New` 时创建），之后不可变 → 无需锁保护

**关键设计**：`mu.internal` 是一个 `spanconfig.Store` **接口**，而非具体类型。这使得 `handleCompleteUpdate` 可以创建一个全新的 `spanconfigstore.Store` 实例，在锁外填充数据，然后在锁内原子替换指针。

### 3.2 New() 函数

**文件**：`kvsubscriber.go:167-218`

```go
func New(
    clock *hlc.Clock,
    rangeFeedFactory *rangefeed.Factory,
    spanConfigurationsTableID uint32,
    bufferMemLimit int64,
    fallback roachpb.SpanConfig,
    settings *cluster.Settings,
    boundsReader spanconfigstore.BoundsReader,
    knobs *spanconfig.TestingKnobs,
    registry *metric.Registry,
) *KVSubscriber
```

**初始化流程**：

```
New()
│
├── [1] 空值防护：knobs == nil → 创建默认 TestingKnobs
│
├── [2] 计算监听范围
│   ├── spanConfigTableStart = SystemSQLCodec.IndexPrefix(tableID, primaryKeyIndexID)
│   └── spanConfigTableSpan = [spanConfigTableStart, spanConfigTableStart.PrefixEnd())
│
├── [3] 创建初始 spanConfigStore
│   └── spanconfigstore.New(fallback, settings, boundsReader, knobs)
│
├── [4] 构造 KVSubscriber 实例
│   └── s = &KVSubscriber{fallback, knobs, settings, clock, boundsReader}
│
├── [5] 创建 rangefeedcache.Watcher
│   └── s.rfc = rangefeedcache.NewWatcher(
│           "spanconfig-subscriber",
│           clock, rangeFeedFactory,
│           bufferSize = bufferMemLimit / spanConfigurationsTableRowSize,
│           spans = [spanConfigTableSpan],
│           withPrevValue = true,        // 需要 PrevValue 来解码删除事件
│           withRowTSInInitialScan = true, // 初始扫描也需要行时间戳
│           translateEvent = NewSpanConfigDecoder().TranslateEvent,
│           onUpdate = s.handleUpdate,
│           knobs = rfCacheKnobs,
│       )
│
├── [6] 设置初始 Store
│   └── s.mu.internal = spanConfigStore
│
├── [7] 创建并注册指标
│   ├── s.metrics = makeKVSubscriberMetrics()
│   └── registry.AddMetricStruct(s.metrics)  // 如果 registry != nil
│
└── return s
```

#### 3.2.1 Buffer 容量计算

```go
const spanConfigurationsTableRowSize = 5 << 10 // 5 KB

// bufferSize = bufferMemLimit / 5KB
int(bufferMemLimit / spanConfigurationsTableRowSize)
```

**具体数值示例**：
- 假设 `bufferMemLimit = 64MB`（典型值）
- `bufferSize = 64 * 1024 * 1024 / 5120 = 13,107` 个事件
- 这意味着在两次 frontier bump 之间（~3s），buffer 最多容纳 13,107 个 span config 变更事件
- 注释明确说明 `spanConfigurationsTableRowSize` 是一个粗略估计值，仅用于限制 buffer 大小

**一旦 buffer 溢出**：rangefeedcache.Watcher 返回错误，触发 retry loop 重建整个 rangefeed（包括初始扫描）。

#### 3.2.2 监听范围计算

```go
spanConfigTableStart := keys.SystemSQLCodec.IndexPrefix(
    spanConfigurationsTableID,
    keys.SpanConfigurationsTablePrimaryKeyIndexID,
)
spanConfigTableSpan := roachpb.Span{
    Key:    spanConfigTableStart,
    EndKey: spanConfigTableStart.PrefixEnd(),
}
```

这构造了 `system.span_configurations` 表的**主键索引**的完整 key 范围。`PrefixEnd()` 返回前缀的字典序下一个值，确保范围覆盖该表的所有行。

#### 3.2.3 withPrevValue = true 的原因

Span Configuration 表的行包含 `[start_key (PK), end_key, config]`。当一行被删除时，rangefeed 事件中的 `Value` 为空（tombstone）。但仅凭 `Key`（即 `start_key`）无法还原被删除的 span，因为 `end_key` 存储在 value 中而非 key 中。

设置 `withPrevValue = true` 使 rangefeed 在删除事件中携带 `PrevValue`（删除前的完整行内容），从而允许 `SpanConfigDecoder` 解码出完整的被删除 span。

---

## 4. DFS 深入：rangefeedcache.Watcher 机制

**文件**：`pkg/kv/kvclient/rangefeed/rangefeedcache/watcher.go`

Watcher 是一个**泛型框架**，为任何需要基于 rangefeed 维护一致性缓存的场景提供了标准化的实现。KVSubscriber 是它最重要的使用者之一。

### 4.1 Watcher 结构体

```go
type Watcher[E rangefeedbuffer.Event] struct {
    name                   redact.SafeString    // "spanconfig-subscriber"
    clock                  *hlc.Clock
    rangefeedFactory       *rangefeed.Factory
    spans                  []roachpb.Span       // 监听的 KV 范围
    bufferSize             int                  // 增量更新阶段的 buffer 上限
    withPrevValue          bool                 // 是否携带删除前的值
    withRowTSInInitialScan bool                 // 初始扫描是否携带行时间戳

    started int32 // 原子变量，防止并发启动

    translateEvent TranslateEventFunc[E]  // 事件翻译函数
    onUpdate       OnUpdateFunc[E]        // 更新回调函数

    lastFrontierTS hlc.Timestamp  // 跨 rangefeed 尝试的 frontier 单调性断言
    restartErrCh   chan error     // 测试用：注入重启错误
    knobs          TestingKnobs
}
```

### 4.2 Start()：带退避的重试循环

**文件**：`watcher.go:192-221`

```go
func Start[E rangefeedbuffer.Event](
    ctx context.Context, stopper *stop.Stopper, c *Watcher[E], onError func(error),
) error {
    return stopper.RunAsyncTask(ctx, string(c.name), func(ctx context.Context) {
        ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
        defer cancel()

        const aWhile = 5 * time.Minute
        for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
            started := crtime.NowMono()
            if err := c.Run(ctx); err != nil {
                if errors.Is(err, context.Canceled) {
                    return
                }
                if onError != nil {
                    onError(err)
                }
                if started.Elapsed() > aWhile {
                    r.Reset()  // 运行超过 5 分钟视为"成功过"，重置退避
                }
                log.Dev.Warningf(ctx, "%s: failed with %v, retrying...", c.name, err)
                continue
            }
            return
        }
    })
}
```

**退避策略**：

| 参数 | 值 | 含义 |
|------|----|------|
| `InitialBackoff` | 50ms | 首次重试等待 |
| `MaxBackoff` | 2s | 最大重试等待 |
| `Multiplier` | 2 | 指数退避因子 |
| `aWhile` | 5min | 运行超过此时间则重置退避 |

**重置退避的设计理由**：如果 rangefeed 已经稳定运行了 5 分钟再崩溃，说明问题是暂时性的（如网络抖动），应该立即重试而非继续退避。如果连续快速失败，退避保护系统免受重试风暴。

### 4.3 Run()：核心运行逻辑

**文件**：`watcher.go:231-368`

这是 Watcher 最复杂的函数。它可以分为三个阶段：

#### 阶段一：初始化

```go
func (s *Watcher[E]) Run(ctx context.Context) error {
    // [1] CAS 防止并发启动
    if !atomic.CompareAndSwapInt32(&s.started, 0, 1) {
        log.Dev.Fatal(ctx, "currently started: only allowed once at any point in time")
    }
    defer func() { atomic.StoreInt32(&s.started, 0) }()

    // [2] 创建无限制 buffer（初始扫描阶段不设上限）
    buffer := rangefeedbuffer.New[E](math.MaxInt)

    // [3] 创建三个协调通道
    frontierBumpedCh := make(chan struct{})   // frontier 推进通知
    initialScanDoneCh := make(chan struct{})  // 初始扫描完成通知
    errCh := make(chan error)                 // 错误传播
```

**为什么初始扫描阶段 buffer 无限制**：

初始扫描需要接收 `system.span_configurations` 表中的**全部**数据。如果设置了 buffer limit，buffer 溢出会导致 Run 返回错误 → retry → 再次初始扫描 → 再次溢出：**无限循环**。因此初始扫描必须使用无限 buffer。增量阶段再切换为有限 buffer（见阶段二）。

#### 阶段一续：构建 RangeFeed

```go
    // [4] 事件翻译回调
    onValue := func(ctx context.Context, ev *kvpb.RangeFeedValue) {
        bEv, ok := s.translateEvent(ctx, ev)  // SpanConfigDecoder.TranslateEvent
        if !ok {
            return  // 跳过无关事件（如 tombstone-on-tombstone）
        }
        if err := buffer.Add(bEv); err != nil {
            select {
            case <-ctx.Done():
            case errCh <- err:  // buffer 溢出，报告错误
            }
        }
    }

    // [5] 确定初始扫描起始时间戳
    initialScanTS := s.clock.Now()
    if initialScanTS.Less(s.lastFrontierTS) {
        log.Dev.Fatalf(...)  // 时间戳回退 → fatal（不可恢复）
    }

    // [6] 构造并启动 RangeFeed
    rangeFeed := s.rangefeedFactory.New(string(s.name), initialScanTS,
        onValue,
        rangefeed.WithInitialScan(func(ctx context.Context) {
            initialScanDoneCh <- struct{}{}  // 初始扫描完成信号
        }),
        rangefeed.WithOnFrontierAdvance(func(ctx context.Context, frontierTS hlc.Timestamp) {
            mu.Lock()
            mu.frontierTS = frontierTS  // 更新 frontier 时间戳
            mu.Unlock()
            frontierBumpedCh <- struct{}{}  // frontier 推进信号
        }),
        rangefeed.WithSystemTablePriority(),    // 高优先级准入
        rangefeed.WithDiff(s.withPrevValue),     // 携带 PrevValue
        rangefeed.WithRowTimestampInInitialScan(s.withRowTSInInitialScan),
        rangefeed.WithOnInitialScanError(...),   // 认证错误 → 永久失败
    )
    rangeFeed.Start(ctx, s.spans)
    defer rangeFeed.Close()
```

#### 阶段二：事件循环

```go
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()

        case <-frontierBumpedCh:
            // Frontier 推进 → 刷新 buffer → 发送 IncrementalUpdate
            mu.Lock()
            frontierTS := mu.frontierTS
            mu.Unlock()
            s.handleUpdate(ctx, buffer, frontierTS, IncrementalUpdate)

        case <-initialScanDoneCh:
            // 初始扫描完成 → 刷新 buffer → 发送 CompleteUpdate
            s.handleUpdate(ctx, buffer, initialScanTS, CompleteUpdate)
            // ★ 关键：此后设置 buffer 上限
            buffer.SetLimit(s.bufferSize)

        case err := <-errCh:
            return err  // buffer 溢出或认证错误 → 退出 Run

        case err := <-s.restartErrCh:
            return err  // 测试注入的重启错误
        }
    }
```

### 4.4 handleUpdate：Flush 与回调

```go
func (s *Watcher[E]) handleUpdate(
    ctx context.Context, buffer *rangefeedbuffer.Buffer[E],
    ts hlc.Timestamp, updateType UpdateType,
) {
    s.onUpdate(ctx, Update[E]{
        Type:      updateType,
        Timestamp: ts,
        Events:    buffer.Flush(ctx, ts),  // 取出所有 ts <= 给定时间戳的事件
    })
}
```

`buffer.Flush(ctx, ts)` 返回所有时间戳 `<= ts` 的已缓冲事件。Flush 后这些事件从 buffer 中移除。

**时序保证**：
- `CompleteUpdate` 的 `ts` 是 `initialScanTS`（`s.clock.Now()` 在 rangefeed 创建时）
- `IncrementalUpdate` 的 `ts` 是 `frontierTS`（所有 span 的 resolved timestamp 的最小值）
- Frontier 保证：所有 `ts <= frontierTS` 的事件都已被 buffer 接收，不会再有遗漏

### 4.5 完整生命周期图

```
Start()
│
└── retry loop
    │
    └── Run()
        │
        ├── buffer = New(MAX_INT)  // 无限 buffer
        ├── rangeFeed.Start()
        │
        ├── ◄── initialScanDoneCh ──  [initial scan events arrive via onValue]
        │   │
        │   ├── buffer.Flush(initialScanTS) → CompleteUpdate → handleCompleteUpdate
        │   └── buffer.SetLimit(bufferSize)  // 切换为有限 buffer
        │
        ├── ◄── frontierBumpedCh ──  [incremental events arrive via onValue]
        │   │
        │   └── buffer.Flush(frontierTS) → IncrementalUpdate → handlePartialUpdate
        │
        ├── ◄── errCh ──  [buffer overflow or auth error]
        │   │
        │   └── return err → retry loop → Run() again (new initial scan)
        │
        └── ◄── ctx.Done ──  [stopper quiescing]
            │
            └── return ctx.Err() → Start exits
```

---

## 5. DFS 深入：SpanConfigDecoder 事件翻译

**文件**：`pkg/spanconfig/spanconfigkvsubscriber/spanconfigdecoder.go`

### 5.1 SpanConfigDecoder 结构体

```go
type SpanConfigDecoder struct {
    alloc   tree.DatumAlloc      // 内存分配器（减少 GC 压力）
    columns []catalog.Column     // system.span_configurations 表的列定义
    decoder valueside.Decoder    // KV value → datum 解码器
}
```

`SpanConfigDecoder` **不是线程安全的**（注释明确说明 "not safe for concurrent use"）。这没问题，因为 rangefeed 的 `onValue` 回调在 rangefeed 的事件处理协程中串行调用。

### 5.2 TranslateEvent 核心逻辑

```go
func (sd *SpanConfigDecoder) TranslateEvent(
    ctx context.Context, ev *kvpb.RangeFeedValue,
) (*BufferEvent, bool) {
    deleted := !ev.Value.IsPresent()  // Value 为空 = 行被删除
    var value roachpb.Value

    if deleted {
        if !ev.PrevValue.IsPresent() {
            return nil, false  // tombstone-on-tombstone，忽略
        }
        value = ev.PrevValue  // ★ 使用 PrevValue 解码被删除的行
    } else {
        value = ev.Value      // 正常行：使用当前 Value
    }

    record, err := sd.decode(roachpb.KeyValue{Key: ev.Key, Value: value})
    if err != nil {
        log.Dev.Fatalf(ctx, "failed to decode row: %v", err)  // 不可重试 → fatal
    }

    var update spanconfig.Update
    if deleted {
        update, err = spanconfig.Deletion(record.GetTarget())
    } else {
        update = spanconfig.Update(record)
    }

    return &BufferEvent{update, ev.Value.Timestamp}, true
}
```

**事件类型分类**：

| 场景 | `ev.Value` | `ev.PrevValue` | 处理 |
|------|-----------|----------------|------|
| 新增配置 | Present | N/A | `spanconfig.Update(record)` |
| 修改配置 | Present | Present | `spanconfig.Update(record)` (新值) |
| 删除配置 | Empty (tombstone) | Present | `spanconfig.Deletion(target)` |
| tombstone-on-tombstone | Empty | Empty | `return nil, false` (忽略) |

### 5.3 decode 函数：行解码

```go
func (sd *SpanConfigDecoder) decode(kv roachpb.KeyValue) (spanconfig.Record, error) {
    // [1] 从主键解码 start_key
    types := []*types.T{sd.columns[0].GetType()}
    startKeyRow := make([]rowenc.EncDatum, 1)
    rowenc.DecodeIndexKey(keys.SystemSQLCodec, startKeyRow, nil, kv.Key)
    startKeyRow[0].EnsureDecoded(types[0], &sd.alloc)
    rawSp.Key = []byte(tree.MustBeDBytes(startKeyRow[0].Datum))

    // [2] 从 value (column family) 解码 end_key 和 config
    bytes, _ := kv.Value.GetTuple()
    datums, _ := sd.decoder.Decode(&sd.alloc, bytes)

    if endKey := datums[1]; endKey != tree.DNull {
        rawSp.EndKey = []byte(tree.MustBeDBytes(endKey))
    }
    if config := datums[2]; config != tree.DNull {
        protoutil.Unmarshal([]byte(tree.MustBeDBytes(config)), &conf)
    }

    return spanconfig.MakeRecord(spanconfig.DecodeTarget(rawSp), conf)
}
```

**表结构映射**：

| 列序号 | 列名 | 存储位置 | 解码方式 |
|--------|------|----------|----------|
| 0 | `start_key` | 主键 (KV key) | `DecodeIndexKey` |
| 1 | `end_key` | Value (column family) | `valueside.Decoder` |
| 2 | `config` | Value (column family) | `protoutil.Unmarshal` |

**为什么 `start_key` 在 key 中而 `end_key` 在 value 中**：`start_key` 是主键列，CockroachDB 按照主键编码 KV key。`end_key` 和 `config` 是非主键列，按列族 (column family) 编码在 value 中。

### 5.4 BufferEvent 结构

```go
type BufferEvent struct {
    spanconfig.Update          // 嵌入：包含 Target 和 SpanConfig
    ts hlc.Timestamp           // 事件的 MVCC 时间戳
}

func (w *BufferEvent) Timestamp() hlc.Timestamp {
    return w.ts
}
```

`BufferEvent` 实现了 `rangefeedbuffer.Event` 接口（仅需 `Timestamp()` 方法）。这个时间戳用于 buffer 的 `Flush(ts)` 过滤和排序。

---

## 6. DFS 深入：Start 与指标监控

### 6.1 Start() 方法

**文件**：`kvsubscriber.go:246-295`

`Start()` 做两件事：启动指标刷新协程 + 启动 rangefeed 订阅。

```go
func (s *KVSubscriber) Start(ctx context.Context, stopper *stop.Stopper) error {
    // [1] 指标刷新协程
    if err := stopper.RunAsyncTask(ctx, "kvsubscriber-metrics", func(ctx context.Context) {
        settingChangeCh := make(chan struct{}, 1)
        metricsPollerInterval.SetOnChange(&s.settings.SV, func(ctx context.Context) {
            select {
            case settingChangeCh <- struct{}{}:
            default:  // 非阻塞写入，避免 setting 变更回调阻塞
            }
        })

        var timer timeutil.Timer
        defer timer.Stop()

        for {
            interval := metricsPollerInterval.Get(&s.settings.SV)
            if interval > 0 {
                timer.Reset(interval)
            } else {
                timer.Stop()  // interval=0 → 禁用指标刷新
            }
            select {
            case <-timer.C:
                s.updateMetrics(ctx)
            case <-settingChangeCh:
                continue  // 设置变更 → 重新读取 interval
            case <-stopper.ShouldQuiesce():
                return
            }
        }
    }); err != nil {
        return err
    }

    // [2] 启动 rangefeed 订阅
    return rangefeedcache.Start(ctx, stopper, s.rfc, nil)
}
```

**指标刷新循环的设计模式**：

这是一个 **setting-aware timer loop**，CockroachDB 中的标准模式：

1. `SetOnChange` 注册 setting 变更回调
2. 回调向 buffered channel (容量=1) 发送信号
3. 主循环 `select` 同时监听 timer 和 setting 变更
4. Setting 变更时立即进入下一轮循环，使用新的 interval 重置 timer

`default` 分支确保回调不会阻塞：如果 channel 已满（说明已有一个未处理的变更通知），新通知被丢弃。因为下一轮循环会读取最新的 setting 值，丢弃中间通知没有影响。

### 6.2 updateMetrics()

**文件**：`kvsubscriber.go:297-319`

```go
func (s *KVSubscriber) updateMetrics(ctx context.Context) {
    protectedTimestamps, lastUpdated, err := s.GetProtectionTimestamps(ctx, keys.EverythingSpan)
    if err != nil {
        log.Dev.Errorf(ctx, "while refreshing kvsubscriber metrics: %v", err)
        return  // 非 fatal，下一个 tick 重试
    }

    earliestTS := hlc.Timestamp{}
    for _, protectedTimestamp := range protectedTimestamps {
        if earliestTS.IsEmpty() || protectedTimestamp.Less(earliestTS) {
            earliestTS = protectedTimestamp
        }
    }

    now := s.clock.PhysicalTime()
    s.metrics.ProtectedRecordCount.Update(int64(len(protectedTimestamps)))
    s.metrics.UpdateBehindNanos.Update(now.Sub(lastUpdated.GoTime()).Nanoseconds())
    if earliestTS.IsEmpty() {
        s.metrics.OldestProtectedRecordNanos.Update(0)
    } else {
        s.metrics.OldestProtectedRecordNanos.Update(now.Sub(earliestTS.GoTime()).Nanoseconds())
    }
}
```

**指标语义**：

| 指标 | 含义 | 告警场景 |
|------|------|----------|
| `UpdateBehindNanos` | `now - lastUpdated` | 持续增长 → rangefeed 断开，配置不再更新 |
| `ProtectedRecordCount` | 全局受保护时间戳数 | 过多 → 可能阻止 GC |
| `OldestProtectedRecordNanos` | `now - 最老的受保护时间戳` | 持续增长 → 某个备份/CDC 任务挂起 |

**运行时数值示例**：
- 正常集群：`UpdateBehindNanos ≈ 5-8s`（metrics poller 间隔 + rangefeed closed TS 延迟）
- 一个正在运行的备份：`ProtectedRecordCount = 1`，`OldestProtectedRecordNanos ≈ 备份持续时间`
- Rangefeed 断开 10 分钟后：`UpdateBehindNanos ≈ 600,000,000,000`（600s in nanos）

### 6.3 Metrics 结构体

```go
type Metrics struct {
    UpdateBehindNanos          *metric.Gauge  // 滞后纳秒数
    ProtectedRecordCount       *metric.Gauge  // 受保护记录数
    OldestProtectedRecordNanos *metric.Gauge  // 最老受保护记录的年龄（纳秒）
}
```

这三个 Gauge 类型指标通过 `metric.Registry` 注册，可在 DB Console 和 Prometheus endpoint (`/_status/vars`) 上查看。

---

**以上是上半部分**。涵盖了系统架构、BFS 全局控制流、KVSubscriber 构造初始化、rangefeedcache.Watcher 的完整机制（初始扫描/增量更新/buffer/retry）、SpanConfigDecoder 的事件翻译、以及 Start 和指标监控。

下半部分将覆盖：**CompleteUpdate 的 Store 原子替换、IncrementalUpdate 的排序与应用、Handler 通知模式、StoreReader 代理方法、GetProtectionTimestamps 实现、具体运行时示例、以及设计模式总结**。
