# StoreGossip深度剖析——基于容量变化检测的自适应Store状态广播机制

## 一、第一轮 BFS：职责边界与设计动机（Why）

### 1.1 系统性问题背景

在 CockroachDB 的分布式架构中，每个 Store 都需要向集群中的其他节点**广播自己的状态信息**，包括：
- **容量信息**：剩余磁盘空间、已用空间、可用空间
- **负载信息**：QPS、写入速率、CPU 使用率
- **副本信息**：Range 数量、Lease 数量
- **健康状态**：IO 过载状态、磁盘健康状态

这些信息对于集群的**副本放置决策**（Allocator）至关重要。如果没有准确、及时的 Store 状态信息，会导致：

**问题 1：副本分布不均**
```
场景：Store A 快满了，但 Allocator 不知道
结果：继续向 Store A 分配新副本
后果：Store A 磁盘满，导致写入失败
```

**问题 2：负载不均衡**
```
场景：Store B 的 QPS 暴涨 10 倍，但其他节点不知道
结果：继续向 Store B 转移 lease
后果：Store B 过载，查询延迟飙升
```

**问题 3：频繁无效的 Gossip**
```
场景：每次 Range 操作都立即 gossip
结果：Gossip 网络被大量重复信息淹没
后果：带宽浪费，Gossip 延迟增加
```

**问题 4：Gossip 不及时**
```
场景：Store 容量从 50% 变为 99%，但没有 gossip
结果：Allocator 基于过时信息做决策
后果：新副本被分配到即将满的 Store
```

### 1.2 StoreGossip 解决的核心问题

`StoreGossip` 是一个**智能的、自适应的 Store 状态广播管理器**，它解决了以下核心问题：

1. **何时 Gossip**：
   - 周期性 gossip（定期更新）
   - 容量显著变化时立即 gossip（紧急更新）
   - 避免过于频繁的 gossip（速率限制）

2. **Gossip 什么**：
   - 缓存的容量信息（避免重复计算）
   - 增量更新的统计信息（Range/Lease 计数）
   - 实时负载指标（QPS/WPS/CPU）

3. **如何避免 Gossip 风暴**：
   - 阈值检测（容量变化超过 10% 才 gossip）
   - 频率限制（最快 2 秒 gossip 一次）
   - 防止递归触发（gossipOngoing 标志）

### 1.3 在系统中的位置

```
                        Gossip Network
                             |
                             v
        +---------------------------------------------------+
        |            StoreGossip (单例，Store 级别)          |
        +---------------------------------------------------+
                             |
                +------------+------------+
                |                         |
        主动触发 Gossip            被动触发 Gossip
                |                         |
    +----------------------+    +----------------------+
    | Store.startGossip()  |    | 容量变化事件          |
    | (周期性，10s/次)      |    | - Range 增删          |
    +----------------------+    | - Lease 变更          |
                                | - QPS/WPS 变化        |
                                | - IO 过载              |
                                +----------------------+
                                         |
                                         v
                        +--------------------------------+
                        | shouldGossipOnCapacityDelta() |
                        | (智能决策引擎)                 |
                        +--------------------------------+
                                         |
                                         v
                        +--------------------------------+
                        |    Gossip.AddInfoProto()      |
                        | (广播到集群)                   |
                        +--------------------------------+
```

**上游调用者**：
- `Store.NewStore()`：创建 `StoreGossip` 单例
- `Store.startGossip()`：周期性触发 gossip
- `Store.MaybeGossipOnCapacityChange()`：容量变化时触发
- `Store.computeMetrics()`：负载指标更新时触发

**下游依赖**：
- `gossip.Gossip`：实际的 Gossip 网络
- `Store.Descriptor()`：获取 Store 的完整描述符
- `timeutil.TimeSource`：时间源（用于速率限制）

### 1.4 核心抽象与生命周期

#### 核心对象：StoreGossip（单例，Store 级别）

```go
type StoreGossip struct {
    Ident            roachpb.StoreIdent       // Store 标识（NodeID + StoreID）
    stopper          *stop.Stopper            // 用于优雅关闭
    knobs            StoreGossipTestingKnobs  // 测试钩子
    cachedCapacity   *cachedCapacity          // 缓存的容量信息
    gossipOngoing    atomic.Bool              // 防止递归触发
    gossiper         InfoGossiper             // Gossip 网络接口
    descriptorGetter StoreDescriptorProvider  // Store 描述符提供者
    sv               *settings.Values         // 集群配置
    clock            timeutil.TimeSource      // 时间源
}
```

**生命周期**：
- **创建**：`Store.NewStore()` 时调用 `NewStoreGossip()`
- **初始化**：`Store.Start()` 后，`Ident` 字段被填充
- **运行**：持续接收容量变化事件并决策是否 gossip
- **销毁**：`Store.Stop()` 时停止

#### 核心状态：cachedCapacity

```go
type cachedCapacity struct {
    syncutil.Mutex
    cached           roachpb.StoreCapacity // 当前缓存的容量
    lastGossiped     roachpb.StoreCapacity // 上次 gossip 的容量
    lastGossipedTime time.Time             // 上次 gossip 的时间
}
```

**职责**：
- 缓存当前容量（避免重复计算 `Store.Descriptor()`）
- 记录上次 gossip 的容量（用于计算变化量）
- 记录上次 gossip 的时间（用于频率限制）

**并发安全**：通过 `syncutil.Mutex` 保护所有字段。

---

## 二、第二轮 BFS：控制流与组件协作（How it flows）

### 2.1 主要执行路径：Store 状态的 Gossip 生命周期

#### 阶段 0：初始化（Store 启动时）

```go
// NewStore() 中
if s.cfg.Gossip != nil {
    s.storeGossip = NewStoreGossip(
        cfg.Gossip,                       // Gossip 网络
        s,                                // Store 作为描述符提供者
        cfg.TestingKnobs.GossipTestingKnobs,
        &cfg.Settings.SV,
        timeutil.DefaultTimeSource{},
    )
}
```

**执行步骤**：

```go
// store_gossip.go:277-292
func NewStoreGossip(
    gossiper InfoGossiper,
    descGetter StoreDescriptorProvider,
    testingKnobs StoreGossipTestingKnobs,
    sv *settings.Values,
    clock timeutil.TimeSource,
) *StoreGossip {
    return &StoreGossip{
        cachedCapacity:   &cachedCapacity{},  // 初始化空缓存
        gossiper:         gossiper,
        descriptorGetter: descGetter,
        knobs:            testingKnobs,
        sv:               sv,
        clock:            clock,
    }
}
```

**状态变化**：
```
StoreGossip 对象被创建
cachedCapacity = {
    cached:           StoreCapacity{} (空)
    lastGossiped:     StoreCapacity{} (空)
    lastGossipedTime: time.Time{}     (零值)
}
gossipOngoing = false
```

**关键点**：
- **注意 `Ident` 字段未初始化**，它会在 `Store.Start()` 后被填充
- **Gossip 尚未开始**，需要等待 `Store.startGossip()` 启动周期性 gossip

---

#### 阶段 1：周期性 Gossip（Store 启动后）

```go
// Store.Start() 中
s.startGossip()
```

**执行流程**（简化版）：

```go
// store_gossip.go:101-179 (简化)
func (s *Store) startGossip() {
    // 每 10 秒触发一次 gossip
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            // 周期性 gossip Store 描述符
            s.storeGossip.GossipStore(ctx, false /* useCached */)
        case <-s.stopper.ShouldQuiesce():
            return
        }
    }
}
```

**状态变化**（第一次 Gossip）：

```
T0: ticker 触发
    ↓
T1: 调用 GossipStore(ctx, useCached=false)
    ↓
T2: gossipOngoing = true (防止递归)
    ↓
T3: 调用 Store.Descriptor(ctx, useCached=false)
    → 重新计算容量（昂贵操作）
    → 返回 StoreDescriptor{
        StoreID: 1,
        Capacity: {
            RangeCount: 100,
            LeaseCount: 50,
            Available:  500GB,
            Used:       300GB,
            QPS:        1000,
            ...
        }
    }
    ↓
T4: 更新 cachedCapacity
    cachedCapacity.lastGossiped = Capacity
    cachedCapacity.lastGossipedTime = now
    ↓
T5: 调用 gossiper.AddInfoProto(key, storeDesc, TTL)
    → 广播到 Gossip 网络
    ↓
T6: gossipOngoing = false
```

**关键点**：
- **第一次 Gossip 使用 `useCached=false`**，确保获取最新的容量信息
- **后续周期性 Gossip 也使用 `useCached=false`**，因为周期已经是 10 秒，不需要缓存

---

#### 阶段 2：容量变化触发 Gossip（运行时）

**触发方式 1：Range 增删**

```go
// Replica 被添加到 Store 时
s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeAddEvent)

// Replica 被移除时
s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeRemoveEvent)
```

**执行流程**：

```go
// store_gossip.go:425-445
func (s *StoreGossip) MaybeGossipOnCapacityChange(ctx context.Context, cce CapacityChangeEvent) {
    // Step 1: 增量更新缓存的统计信息
    s.cachedCapacity.Lock()
    switch cce {
    case RangeAddEvent:
        s.cachedCapacity.cached.RangeCount++
    case RangeRemoveEvent:
        s.cachedCapacity.cached.RangeCount--
    case LeaseAddEvent:
        s.cachedCapacity.cached.LeaseCount++
    case LeaseRemoveEvent:
        s.cachedCapacity.cached.LeaseCount--
    }
    s.cachedCapacity.Unlock()

    // Step 2: 检查是否需要 gossip
    if shouldGossip, reason := s.shouldGossipOnCapacityDelta(); shouldGossip {
        s.asyncGossipStore(context.TODO(), reason, true /* useCached */)
    }
}
```

**状态变化**（假设 Range 增加 6 个）：

```
T0: 初始状态
    cached.RangeCount = 100
    lastGossiped.RangeCount = 100

T1: RangeAddEvent × 6
    cached.RangeCount = 106

T2: shouldGossipOnCapacityDelta() 检查
    deltaRangeCount = 106 - 100 = 6
    GossipWhenRangeCountDeltaExceeds = 5
    6 > 5 → shouldGossip = true

T3: asyncGossipStore(reason="range-count(6.0) change", useCached=true)
    ↓
T4: GossipStore(ctx, useCached=true)
    → 使用缓存的 Descriptor（不重新计算）
    ↓
T5: 更新 lastGossiped
    lastGossiped.RangeCount = 106
    lastGossipedTime = now
```

**关键点**：
- **增量更新**：不需要重新计算 `Store.Descriptor()`，只更新 `cached.RangeCount`
- **阈值检测**：只有变化超过 5 个 Range 时才 gossip
- **使用缓存**：`useCached=true` 避免昂贵的重新计算

---

**触发方式 2：负载变化**

```go
// 每秒更新一次负载指标
s.storeGossip.RecordNewPerSecondStats(newQPS, newWPS, newWBPS, newCPUS)
```

**执行流程**：

```go
// store_gossip.go:450-465
func (s *StoreGossip) RecordNewPerSecondStats(newQPS, newWPS, newWBPS, newCPUS float64) {
    // Step 1: 更新缓存
    s.cachedCapacity.Lock()
    s.cachedCapacity.cached.QueriesPerSecond = newQPS
    s.cachedCapacity.cached.WritesPerSecond = newWPS
    s.cachedCapacity.cached.WriteBytesPerSecond = newWBPS
    s.cachedCapacity.cached.CPUPerSecond = newCPUS
    s.cachedCapacity.Unlock()

    // Step 2: 检查是否需要 gossip
    if shouldGossip, reason := s.shouldGossipOnCapacityDelta(); shouldGossip {
        s.asyncGossipStore(context.TODO(), reason, false /* useCached */)
    }
}
```

**状态变化**（假设 QPS 翻倍）：

```
T0: 初始状态
    cached.QPS = 1000
    lastGossiped.QPS = 1000

T1: 负载突增
    newQPS = 2000

T2: RecordNewPerSecondStats(2000, ...)
    cached.QPS = 2000

T3: shouldGossipOnCapacityDelta() 检查
    deltaQPS = 2000 - 1000 = 1000
    deltaFraction = 1000 / 1000 = 1.0 (100%)
    gossipWhenLoadDeltaExceedsFraction = 0.5 (50%)
    1.0 > 0.5 → shouldGossip = true

T4: asyncGossipStore(reason="queries-per-second(1000.0) change", useCached=false)
    → 注意：useCached=false，因为负载变化可能伴随其他状态变化
```

**关键点**：
- **负载指标更新不使用缓存**：因为负载变化通常意味着系统状态发生了显著变化，需要重新计算完整的 Descriptor
- **阈值是相对变化**：必须超过 50% 的相对变化且绝对变化超过 100

---

**触发方式 3：IO 过载状态变化**

```go
// IO 阈值更新时
s.storeGossip.RecordNewIOThreshold(threshold, thresholdMax)
```

**执行流程**：

```go
// store_gossip.go:470-479
func (s *StoreGossip) RecordNewIOThreshold(threshold, thresholdMax admissionpb.IOThreshold) {
    s.cachedCapacity.Lock()
    s.cachedCapacity.cached.IOThreshold = threshold
    s.cachedCapacity.cached.IOThresholdMax = thresholdMax
    s.cachedCapacity.Unlock()

    if shouldGossip, reason := s.shouldGossipOnCapacityDelta(); shouldGossip {
        s.asyncGossipStore(context.TODO(), reason, true /* useCached */)
    }
}
```

**状态变化**（假设 IO 过载分数从 0.1 增加到 0.4）：

```
T0: 初始状态
    cached.IOThresholdMax.Score = 0.1
    lastGossiped.IOThresholdMax.Score = 0.1

T1: IO 过载增加
    newIOThresholdMax.Score = 0.4

T2: RecordNewIOThreshold(threshold, thresholdMax)
    cached.IOThresholdMax.Score = 0.4

T3: shouldGossipOnCapacityDelta() 检查
    cachedMaxIOScore = 0.4
    lastGossipMaxIOScore = 0.1
    gossipMinMaxIOOverloadScore = 0.3
    0.4 >= 0.3 && 0.4 > 0.1 → shouldGossip = true

T4: asyncGossipStore(reason="io-overload(0.4) change", useCached=true)
```

**关键点**：
- **IO 过载是关键信号**：一旦超过 0.3（`LeaseIOOverloadThreshold`），立即 gossip
- **用于 lease 转移决策**：其他节点会避免向 IO 过载的 Store 转移 lease

---

### 2.2 核心决策引擎：shouldGossipOnCapacityDelta()

这是 `StoreGossip` 的**智能大脑**，决定何时触发 gossip。

**决策流程**：

```go
// store_gossip.go:486-563 (简化)
func (s *StoreGossip) shouldGossipOnCapacityDelta() (should bool, reason string) {
    // Step 1: 检查是否正在 gossip 或 gossip 太频繁
    if s.gossipOngoing.Load() || !s.canEagerlyGossipNow() {
        return false, ""
    }

    // Step 2: 获取配置阈值
    gossipWhenCapacityDeltaExceedsFraction := 0.10  // 10% 变化
    gossipMinMaxIOOverloadScore := 0.3              // IO 过载阈值

    // Step 3: 检查各项指标的变化
    s.cachedCapacity.Lock()
    defer s.cachedCapacity.Unlock()

    updateForQPS, deltaQPS := deltaExceedsThreshold(
        s.cachedCapacity.lastGossiped.QueriesPerSecond,
        s.cachedCapacity.cached.QueriesPerSecond,
        gossipMinAbsoluteDelta,                    // 100
        gossipWhenLoadDeltaExceedsFraction)        // 0.5 (50%)

    updateForRangeCount, deltaRangeCount := deltaExceedsThreshold(
        float64(s.cachedCapacity.lastGossiped.RangeCount),
        float64(s.cachedCapacity.cached.RangeCount),
        GossipWhenRangeCountDeltaExceeds,          // 5
        gossipWhenCapacityDeltaExceedsFraction)    // 0.1 (10%)

    updateForLeaseCount, deltaLeaseCount := deltaExceedsThreshold(
        float64(s.cachedCapacity.lastGossiped.LeaseCount),
        float64(s.cachedCapacity.cached.LeaseCount),
        gossipWhenLeaseCountDeltaExceeds,          // 5
        gossipWhenCapacityDeltaExceedsFraction)    // 0.1 (10%)

    cachedMaxIOScore, _ := s.cachedCapacity.cached.IOThresholdMax.Score()
    lastGossipMaxIOScore, _ := s.cachedCapacity.lastGossiped.IOThresholdMax.Score()
    updateForMaxIOOverloadScore := cachedMaxIOScore >= gossipMinMaxIOOverloadScore &&
        cachedMaxIOScore > lastGossipMaxIOScore

    // Step 4: 构造 reason 字符串
    if updateForQPS {
        reason += fmt.Sprintf("queries-per-second(%.1f) ", deltaQPS)
    }
    if updateForRangeCount {
        reason += fmt.Sprintf("range-count(%.1f) ", deltaRangeCount)
    }
    if updateForLeaseCount {
        reason += fmt.Sprintf("lease-count(%.1f) ", deltaLeaseCount)
    }
    if updateForMaxIOOverloadScore {
        reason += fmt.Sprintf("io-overload(%.1f) ", cachedMaxIOScore)
    }
    if reason != "" {
        should = true
        reason += "change"
    }

    return should, reason
}
```

**阈值检测逻辑**：

```go
// store_gossip.go:567-581
func deltaExceedsThreshold(
    old, cur, requiredMinDelta, requiredDeltaFraction float64,
) (exceeds bool, delta float64) {
    delta = cur - old
    deltaAbsolute := math.Abs(cur - old)
    deltaFraction := 10e9  // 非常大的默认值

    if old != 0 {
        deltaFraction = deltaAbsolute / old
    }

    // 必须同时满足绝对变化和相对变化
    exceeds = deltaAbsolute >= requiredMinDelta && deltaFraction >= requiredDeltaFraction
    return exceeds, delta
}
```

**决策表**：

| 指标          | 绝对阈值 | 相对阈值 | 示例                         |
|--------------|---------|---------|------------------------------|
| QPS          | 100     | 50%     | 1000 → 1600 (超过)           |
| WPS          | 100     | 50%     | 500 → 800 (超过)             |
| RangeCount   | 5       | 10%     | 100 → 106 (超过)             |
| LeaseCount   | 5       | 10%     | 50 → 56 (超过)               |
| IOOverload   | N/A     | > 0.3   | 0.2 → 0.4 (超过)             |

**为什么需要双重阈值**？

```
示例 1：小基数的相对变化
old = 10, cur = 20
deltaFraction = 1.0 (100%)  ← 看起来很大
deltaAbsolute = 10          ← 实际很小
→ 不触发 gossip（避免频繁 gossip）

示例 2：大基数的绝对变化
old = 10000, cur = 10099
deltaFraction = 0.0099 (0.99%) ← 看起来很小
deltaAbsolute = 99             ← 接近阈值
→ 不触发 gossip（误差范围内）

示例 3：满足双重阈值
old = 1000, cur = 1600
deltaFraction = 0.6 (60%)    ← 超过 50%
deltaAbsolute = 600          ← 超过 100
→ 触发 gossip
```

---

### 2.3 频率限制机制

**问题**：如果容量频繁变化，会导致 gossip 风暴。

**解决方案**：`canEagerlyGossipNow()` 检查距离上次 gossip 是否已过足够时间。

```go
// store_gossip.go:332-341
func (s *StoreGossip) canEagerlyGossipNow() (canGossip bool) {
    now := s.clock.Now()
    s.cachedCapacity.Lock()
    defer s.cachedCapacity.Unlock()

    nextValidGossipTime := s.cachedCapacity.lastGossipedTime.Add(
        MaxStoreGossipFrequency.Get(s.sv))  // 默认 2 秒

    return nextValidGossipTime.Before(now)
}
```

**时间线示例**：

```
T0 (00:00:00): 第一次 gossip
    lastGossipedTime = 00:00:00
    nextValidGossipTime = 00:00:02

T1 (00:00:01): 容量变化触发 gossip
    canEagerlyGossipNow() → false (距离上次才 1 秒)
    → 跳过 gossip

T2 (00:00:02): 容量再次变化
    canEagerlyGossipNow() → true (已过 2 秒)
    → 执行 gossip
    lastGossipedTime = 00:00:02
    nextValidGossipTime = 00:00:04

T3 (00:00:03): 容量再次变化
    canEagerlyGossipNow() → false
    → 跳过 gossip
```

**配置参数**：

```go
// store_gossip.go:43-48
var MaxStoreGossipFrequency = settings.RegisterDurationSetting(
    settings.SystemOnly,
    "kv.store_gossip.max_frequency",
    "the maximum frequency at which a store will gossip its store descriptor",
    defaultMaxStoreGossipFrequency,  // 2 * time.Second
)
```

**关键点**：
- **周期性 gossip 不受此限制**：周期性 gossip（10 秒/次）总是会执行
- **只限制"紧急 gossip"**：容量变化触发的 gossip 受此限制

---

### 2.4 异步 Gossip 机制

**问题**：Gossip 操作可能涉及磁盘 IO 和网络 RPC，不应阻塞调用者。

**解决方案**：`asyncGossipStore()` 在后台 goroutine 中执行 gossip。

```go
// store_gossip.go:346-366
func (s *StoreGossip) asyncGossipStore(ctx context.Context, reason string, useCached bool) {
    gossipFn := func(ctx context.Context) {
        log.VEventf(ctx, 2, "gossiping on %s", reason)
        if err := s.GossipStore(ctx, useCached); err != nil {
            log.KvDistribution.Warningf(ctx, "error gossiping on %s: %+v", reason, err)
        }
    }

    // 如果是测试环境，同步执行
    if s.knobs.AsyncDisabled {
        gossipFn(ctx)
        return
    }

    // 异步执行
    if err := s.stopper.RunAsyncTask(
        ctx, fmt.Sprintf("storage.Store: gossip on %s", reason), gossipFn,
    ); err != nil {
        log.KvDistribution.Warningf(ctx, "unable to gossip on %s: %+v", reason, err)
    }
}
```

**执行模型**：

```
主线程（调用 MaybeGossipOnCapacityChange）
    |
    | 检查是否需要 gossip
    v
shouldGossipOnCapacityDelta() → true
    |
    | 启动异步任务
    v
asyncGossipStore(...)
    |
    | 创建 goroutine
    v
+----------------------------+
| Gossip Goroutine           |
|----------------------------|
| GossipStore()              |
|   → Store.Descriptor()     |  ← 可能涉及磁盘 IO
|   → gossiper.AddInfoProto()|  ← 可能涉及网络 RPC
+----------------------------+
    |
    | 返回
    v
主线程继续执行（不等待）
```

**关键点**：
- **非阻塞**：调用者不需要等待 gossip 完成
- **错误处理**：Gossip 失败只记录日志，不影响正常操作
- **测试支持**：`AsyncDisabled` 允许测试同步验证 gossip 行为

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 NewStoreGossip：构造函数

```go
// store_gossip.go:277-292
func NewStoreGossip(
    gossiper InfoGossiper,
    descGetter StoreDescriptorProvider,
    testingKnobs StoreGossipTestingKnobs,
    sv *settings.Values,
    clock timeutil.TimeSource,
) *StoreGossip {
    return &StoreGossip{
        cachedCapacity:   &cachedCapacity{},
        gossiper:         gossiper,
        descriptorGetter: descGetter,
        knobs:            testingKnobs,
        sv:               sv,
        clock:            clock,
    }
}
```

**输入参数解析**：

| 参数           | 类型                      | 实际传入值                     | 作用                      |
|---------------|--------------------------|------------------------------|--------------------------|
| gossiper      | InfoGossiper             | `cfg.Gossip` (Gossip 网络)   | 广播 Store 描述符         |
| descGetter    | StoreDescriptorProvider  | `s` (Store 自身)             | 获取 Store 描述符         |
| testingKnobs  | StoreGossipTestingKnobs  | `cfg.TestingKnobs.GossipTestingKnobs` | 测试钩子 |
| sv            | *settings.Values         | `&cfg.Settings.SV`           | 集群配置                 |
| clock         | timeutil.TimeSource      | `timeutil.DefaultTimeSource{}` | 时间源               |

**为什么 `descGetter` 是 `Store` 自身**？

```go
// Store 实现了 StoreDescriptorProvider 接口
type StoreDescriptorProvider interface {
    Descriptor(ctx context.Context, cached bool) (*roachpb.StoreDescriptor, error)
}

var _ StoreDescriptorProvider = &Store{}

// Store.Descriptor() 实现
func (s *Store) Descriptor(ctx context.Context, cached bool) (*roachpb.StoreDescriptor, error) {
    // 如果 cached=true，使用缓存的容量信息
    if cached {
        capacity := s.storeGossip.CachedCapacity()
        return &roachpb.StoreDescriptor{
            StoreID:  s.Ident.StoreID,
            Capacity: capacity,
            ...
        }, nil
    }

    // 如果 cached=false，重新计算容量（昂贵操作）
    capacity := s.computeCapacity()  // 调用 Pebble 的磁盘统计
    return &roachpb.StoreDescriptor{
        StoreID:  s.Ident.StoreID,
        Capacity: capacity,
        ...
    }, nil
}
```

**不变量**：
- 返回的 `StoreGossip` 对象的 `Ident` 字段为空，需要在 `Store.Start()` 后填充
- `cachedCapacity` 初始化为空，第一次 gossip 时会被填充

**并发安全分析**：
- `NewStoreGossip` 本身不涉及并发，只是简单的对象构造
- 并发安全由 `cachedCapacity.Mutex` 保证

---

### 3.2 GossipStore：执行 Gossip 的核心逻辑

```go
// store_gossip.go:369-408
func (s *StoreGossip) GossipStore(ctx context.Context, useCached bool) error {
    // Step 1: 设置 gossipOngoing 标志（防止递归）
    s.gossipOngoing.Store(true)
    defer s.gossipOngoing.Store(false)

    // Step 2: 获取 Store 描述符
    storeDesc, err := s.descriptorGetter.Descriptor(ctx, useCached)
    if err != nil {
        return errors.Wrapf(err, "problem getting store descriptor for store %+v", s.Ident)
    }

    // Step 3: 更新缓存和时间戳
    now := s.clock.Now()
    s.cachedCapacity.Lock()
    s.cachedCapacity.lastGossiped = storeDesc.Capacity
    s.cachedCapacity.lastGossipedTime = now
    s.cachedCapacity.Unlock()

    // Step 4: 构造 Gossip key
    gossipStoreKey := gossip.MakeStoreDescKey(storeDesc.StoreID)
    // 例如："store-descriptor-1"（Store ID 为 1）

    // Step 5: 测试钩子（可选）
    if fn := s.knobs.StoreGossipIntercept; fn != nil {
        fn(storeDesc)
    }

    // Step 6: 广播到 Gossip 网络
    return s.gossiper.AddInfoProto(gossipStoreKey, storeDesc, gossip.StoreTTL)
}
```

**输入**：
- `useCached bool`：是否使用缓存的描述符

**输出**：
- `error`：Gossip 失败时返回错误

**关键点 1：gossipOngoing 标志的作用**

```go
// ❌ 没有 gossipOngoing 标志的问题
GossipStore()
    → descriptorGetter.Descriptor(ctx, false)
        → Store.Descriptor()
            → s.computeCapacity()
                → 触发某些回调
                    → 可能调用 MaybeGossipOnCapacityChange()
                        → 再次调用 GossipStore()
                            → 无限递归！
```

**解决方案**：

```go
// store_gossip.go:486-493
func (s *StoreGossip) shouldGossipOnCapacityDelta() (should bool, reason string) {
    // 如果正在 gossip，跳过
    if s.gossipOngoing.Load() || !s.canEagerlyGossipNow() {
        return false, ""
    }
    ...
}
```

**关键点 2：useCached 参数的语义**

| useCached | 何时使用                  | 原因                              |
|-----------|-------------------------|----------------------------------|
| `false`   | 周期性 gossip            | 确保获取最新的磁盘容量信息         |
| `false`   | 负载变化触发              | 负载变化可能伴随其他状态变化       |
| `true`    | Range/Lease 变化触发     | 只需要更新计数，无需重新计算容量   |
| `true`    | IO 过载触发              | IO 阈值已经在缓存中更新           |

**关键点 3：lastGossiped 的更新时机**

```go
// 在 Gossip 成功前更新
s.cachedCapacity.lastGossiped = storeDesc.Capacity
s.cachedCapacity.lastGossipedTime = now

// 然后才调用 AddInfoProto
return s.gossiper.AddInfoProto(...)
```

**为什么在 Gossip 前更新**？

- **避免重复触发**：如果在 Gossip 后更新，中间可能有新的容量变化事件触发，导致重复 gossip
- **Gossip 失败的处理**：即使 Gossip 失败，也更新 `lastGossiped`，避免频繁重试

**不变量**：
- 在 `GossipStore()` 返回后，`lastGossiped` 必须等于 `storeDesc.Capacity`
- `gossipOngoing` 在函数返回时必须为 `false`（通过 `defer` 保证）

---

### 3.3 shouldGossipOnCapacityDelta：智能决策引擎

这是 `StoreGossip` 最复杂的函数，包含多个决策分支。

```go
// store_gossip.go:486-563 (完整版)
func (s *StoreGossip) shouldGossipOnCapacityDelta() (should bool, reason string) {
    // ==================== Step 1: 前置条件检查 ====================
    if s.gossipOngoing.Load() || !s.canEagerlyGossipNow() {
        return false, ""
    }

    // ==================== Step 2: 获取配置 ====================
    gossipWhenCapacityDeltaExceedsFraction := GossipWhenCapacityDeltaExceedsFraction.Get(s.sv)
    if overrideCapacityDeltaFraction := s.knobs.OverrideGossipWhenCapacityDeltaExceedsFraction; overrideCapacityDeltaFraction > 0 {
        gossipWhenCapacityDeltaExceedsFraction = overrideCapacityDeltaFraction
    }

    gossipMinMaxIOOverloadScore := allocatorimpl.LeaseIOOverloadThreshold.Get(s.sv)

    // ==================== Step 3: 检查各项指标 ====================
    s.cachedCapacity.Lock()

    // 3.1 检查 QPS
    updateForQPS, deltaQPS := deltaExceedsThreshold(
        s.cachedCapacity.lastGossiped.QueriesPerSecond,
        s.cachedCapacity.cached.QueriesPerSecond,
        gossipMinAbsoluteDelta,                    // 100
        gossipWhenLoadDeltaExceedsFraction)        // 0.5

    // 3.2 检查 WPS
    updateForWPS, deltaWPS := deltaExceedsThreshold(
        s.cachedCapacity.lastGossiped.WritesPerSecond,
        s.cachedCapacity.cached.WritesPerSecond,
        gossipMinAbsoluteDelta,                    // 100
        gossipWhenLoadDeltaExceedsFraction)        // 0.5

    // 3.3 检查 WBPS (Write Bytes Per Second)
    updateForWBPS, deltaWBPS := deltaExceedsThreshold(
        s.cachedCapacity.lastGossiped.WriteBytesPerSecond,
        s.cachedCapacity.cached.WriteBytesPerSecond,
        gossipMinAbsoluteDelta,                    // 100
        gossipWhenLoadDeltaExceedsFraction)        // 0.5

    // 3.4 检查 CPU
    updateForCPUS, deltaCPUS := deltaExceedsThreshold(
        s.cachedCapacity.lastGossiped.CPUPerSecond,
        s.cachedCapacity.cached.CPUPerSecond,
        gossipMinAbsoluteDelta,                    // 100
        gossipWhenLoadDeltaExceedsFraction)        // 0.5

    // 3.5 检查 Range Count
    updateForRangeCount, deltaRangeCount := deltaExceedsThreshold(
        float64(s.cachedCapacity.lastGossiped.RangeCount),
        float64(s.cachedCapacity.cached.RangeCount),
        GossipWhenRangeCountDeltaExceeds,          // 5
        gossipWhenCapacityDeltaExceedsFraction)    // 0.1

    // 3.6 检查 Lease Count
    updateForLeaseCount, deltaLeaseCount := deltaExceedsThreshold(
        float64(s.cachedCapacity.lastGossiped.LeaseCount),
        float64(s.cachedCapacity.cached.LeaseCount),
        gossipWhenLeaseCountDeltaExceeds,          // 5
        gossipWhenCapacityDeltaExceedsFraction)    // 0.1

    // 3.7 检查 IO Overload Score
    cachedMaxIOScore, _ := s.cachedCapacity.cached.IOThresholdMax.Score()
    lastGossipMaxIOScore, _ := s.cachedCapacity.lastGossiped.IOThresholdMax.Score()
    updateForMaxIOOverloadScore := cachedMaxIOScore >= gossipMinMaxIOOverloadScore &&
        cachedMaxIOScore > lastGossipMaxIOScore

    // 3.8 检查磁盘健康状态变化
    diskUnhealthy := s.cachedCapacity.cached.IOThreshold.DiskUnhealthy
    updateForChangeInDiskUnhealth :=
        s.cachedCapacity.lastGossiped.IOThreshold.DiskUnhealthy != diskUnhealthy

    s.cachedCapacity.Unlock()

    // ==================== Step 4: 测试钩子 ====================
    if s.knobs.DisableLeaseCapacityGossip {
        updateForLeaseCount = false
    }

    // ==================== Step 5: 构造 reason 字符串 ====================
    if updateForQPS {
        reason += fmt.Sprintf("queries-per-second(%.1f) ", deltaQPS)
    }
    if updateForWPS {
        reason += fmt.Sprintf("writes-per-second(%.1f) ", deltaWPS)
    }
    if updateForWBPS {
        reason += fmt.Sprintf("write-bytes-per-second(%.1f) ", deltaWBPS)
    }
    if updateForCPUS {
        reason += fmt.Sprintf("cpu-nanos-per-second(%.1f) ", deltaCPUS)
    }
    if updateForRangeCount {
        reason += fmt.Sprintf("range-count(%.1f) ", deltaRangeCount)
    }
    if updateForLeaseCount {
        reason += fmt.Sprintf("lease-count(%.1f) ", deltaLeaseCount)
    }
    if updateForMaxIOOverloadScore {
        reason += fmt.Sprintf("io-overload(%.1f) ", cachedMaxIOScore)
    }
    if updateForChangeInDiskUnhealth {
        reason += fmt.Sprintf("disk-unhealthy(%t) ", diskUnhealthy)
    }
    if reason != "" {
        should = true
        reason += "change"
    }

    return should, reason
}
```

**输入**：无（使用对象内部状态）

**输出**：
- `should bool`：是否应该 gossip
- `reason string`：触发 gossip 的原因（用于日志）

**关键点 1：为什么检查这么多指标**？

| 指标        | 影响的决策                     | 示例                          |
|------------|-------------------------------|------------------------------|
| QPS        | Lease 放置（避免热点）         | QPS 暴涨 → 转移 lease         |
| WPS        | 副本放置（写入负载均衡）       | WPS 过高 → 避免新副本         |
| RangeCount | 副本放置（负载均衡）           | Range 数量失衡 → 再平衡       |
| LeaseCount | Lease 放置（负载均衡）         | Lease 数量失衡 → 转移 lease   |
| IOOverload | Lease 转移（避免 IO 过载）     | IO 过载 → 紧急转移 lease      |
| DiskHealth | 副本移除（磁盘故障保护）       | 磁盘不健康 → 移除所有副本     |

**关键点 2：为什么 IO Overload 的检查逻辑不同**？

```go
// ❌ IO Overload 不使用 deltaExceedsThreshold
updateForMaxIOOverloadScore := cachedMaxIOScore >= gossipMinMaxIOOverloadScore &&
    cachedMaxIOScore > lastGossipMaxIOScore

// ✅ 其他指标使用 deltaExceedsThreshold
updateForQPS, deltaQPS := deltaExceedsThreshold(...)
```

**原因**：
- **IO Overload 是绝对指标**：一旦超过 0.3，就认为 Store 过载
- **其他指标是相对指标**：需要考虑变化量的大小

**示例**：

```
场景 1：IO Overload 从 0.1 增加到 0.4
cachedMaxIOScore = 0.4
lastGossipMaxIOScore = 0.1
gossipMinMaxIOOverloadScore = 0.3

0.4 >= 0.3 && 0.4 > 0.1 → true
→ 触发 gossip（因为超过了过载阈值）

场景 2：IO Overload 从 0.25 增加到 0.28
cachedMaxIOScore = 0.28
lastGossipMaxIOScore = 0.25
gossipMinMaxIOOverloadScore = 0.3

0.28 >= 0.3 → false
→ 不触发 gossip（虽然增加了，但未超过阈值）

场景 3：IO Overload 从 0.35 增加到 0.36
cachedMaxIOScore = 0.36
lastGossipMaxIOScore = 0.35
gossipMinMaxIOOverloadScore = 0.3

0.36 >= 0.3 && 0.36 > 0.35 → true
→ 触发 gossip（已过载且在恶化）
```

**关键点 3：reason 字符串的作用**

```go
reason += fmt.Sprintf("queries-per-second(%.1f) ", deltaQPS)
```

**示例输出**：

```
"queries-per-second(1000.0) range-count(6.0) change"
```

**用途**：
- **日志记录**：方便调试和监控
- **指标统计**：可以统计哪些类型的变化最常触发 gossip

**不变量**：
- 如果 `should == true`，`reason` 必须非空
- 如果 `should == false`，`reason` 必须为空字符串

---

### 3.4 MaybeGossipOnCapacityChange：容量变化的入口

```go
// store_gossip.go:425-445
func (s *StoreGossip) MaybeGossipOnCapacityChange(ctx context.Context, cce CapacityChangeEvent) {
    // Step 1: 增量更新缓存
    s.cachedCapacity.Lock()
    switch cce {
    case RangeAddEvent:
        s.cachedCapacity.cached.RangeCount++
    case RangeRemoveEvent:
        s.cachedCapacity.cached.RangeCount--
    case LeaseAddEvent:
        s.cachedCapacity.cached.LeaseCount++
    case LeaseRemoveEvent:
        s.cachedCapacity.cached.LeaseCount--
    }
    s.cachedCapacity.Unlock()

    // Step 2: 检查是否需要 gossip
    if shouldGossip, reason := s.shouldGossipOnCapacityDelta(); shouldGossip {
        s.asyncGossipStore(context.TODO(), reason, true /* useCached */)
    }
}
```

**输入**：
- `cce CapacityChangeEvent`：容量变化事件类型

**输出**：无

**关键点 1：为什么使用 `context.TODO()`**？

```go
s.asyncGossipStore(context.TODO(), reason, true /* useCached */)
```

**原因**：
- Gossip 是**最终一致性**操作，不需要立即完成
- 调用者的 `ctx` 可能很快被取消（例如请求处理完成）
- 使用 `context.TODO()` 确保 gossip 任务不会被过早取消

**替代方案**：

```go
// ❌ 错误做法：使用调用者的 ctx
s.asyncGossipStore(ctx, reason, true)

// 问题：如果 ctx 被取消，gossip 任务会被中断
```

**关键点 2：为什么只更新计数，不重新计算容量**？

```go
// ✅ 只更新计数
s.cachedCapacity.cached.RangeCount++

// ❌ 不重新计算完整容量
capacity := s.descriptorGetter.Descriptor(ctx, false)
s.cachedCapacity.cached = capacity
```

**原因**：
- **性能**：`Descriptor()` 调用 Pebble 的磁盘统计，非常昂贵（几十毫秒）
- **频率**：Range 操作非常频繁（每秒可能数百次）
- **增量更新足够准确**：只要周期性 gossip 会重新计算完整容量

**不变量**：
- `cached.RangeCount` 和 `cached.LeaseCount` 必须与实际值保持同步
- 增量更新可能有微小误差，但会在下次周期性 gossip 时修正

---

## 四、运行时行为与系统反馈（Runtime Behavior）

### 4.1 系统如何感知运行时信号

#### 信号 1：容量变化（Range/Lease）

**感知方式**：通过事件回调

```go
// Replica 被添加到 Store 时
func (s *Store) addReplicaToRangeMap(repl *Replica) {
    ...
    s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeAddEvent)
}

// Replica 被移除时
func (s *Store) removeReplicaFromRangeMap(repl *Replica) {
    ...
    s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeRemoveEvent)
}

// Lease 获取时
func (r *Replica) leaseAcquired() {
    ...
    r.store.storeGossip.MaybeGossipOnCapacityChange(ctx, LeaseAddEvent)
}

// Lease 释放时
func (r *Replica) leaseReleased() {
    ...
    r.store.storeGossip.MaybeGossipOnCapacityChange(ctx, LeaseRemoveEvent)
}
```

**反馈路径**：

```
Range/Lease 操作
    ↓
MaybeGossipOnCapacityChange(event)
    ↓
cached.RangeCount++ (或 --)
    ↓
shouldGossipOnCapacityDelta()
    ↓
[如果变化 >= 5] → asyncGossipStore()
    ↓
Gossip 到集群
    ↓
Allocator 感知到变化
    ↓
调整副本/lease 放置策略
```

**关键点**：
- **即时感知**：事件发生时立即更新缓存
- **延迟决策**：不是每次都 gossip，只有累积变化超过阈值时才 gossip
- **最终一致性**：即使某次 gossip 失败，下次周期性 gossip 也会修正

#### 信号 2：负载变化（QPS/WPS/CPU）

**感知方式**：通过周期性采样

```go
// Store.computeMetrics() 每 10 秒调用一次
func (s *Store) computeMetrics(ctx context.Context) {
    // 计算过去 10 秒的平均 QPS/WPS
    newQPS := s.replicaStats.getQueryRate()
    newWPS := s.replicaStats.getWriteRate()
    newWBPS := s.replicaStats.getWriteBytesRate()
    newCPUS := s.replicaStats.getCPURate()

    // 更新 StoreGossip
    s.storeGossip.RecordNewPerSecondStats(newQPS, newWPS, newWBPS, newCPUS)
}
```

**反馈路径**：

```
每 10 秒
    ↓
computeMetrics()
    ↓
RecordNewPerSecondStats(newQPS, ...)
    ↓
cached.QPS = newQPS
    ↓
shouldGossipOnCapacityDelta()
    ↓
[如果变化 >= 50%] → asyncGossipStore()
    ↓
Gossip 到集群
    ↓
Allocator 感知到负载变化
    ↓
转移 lease 或副本
```

**关键点**：
- **采样延迟**：负载变化不会立即被感知，最多延迟 10 秒
- **平滑处理**：使用滑动窗口平均，避免短暂的尖刺触发 gossip
- **阈值高**：负载需要变化 50% 以上才触发 gossip

#### 信号 3：IO 过载（IO Threshold）

**感知方式**：通过 IO 负载监听器

```go
// ioLoadListener 每秒更新 IO 阈值
func (iol *ioLoadListener) updateIOThreshold() {
    threshold := iol.calculateThreshold()
    thresholdMax := iol.getMaxThresholdInWindow()

    // 更新 Store 的 IO 阈值
    iol.store.storeGossip.RecordNewIOThreshold(threshold, thresholdMax)
}
```

**反馈路径**：

```
每秒
    ↓
ioLoadListener.updateIOThreshold()
    ↓
RecordNewIOThreshold(threshold, thresholdMax)
    ↓
cached.IOThresholdMax = thresholdMax
    ↓
shouldGossipOnCapacityDelta()
    ↓
[如果 score >= 0.3 且增加] → asyncGossipStore()
    ↓
Gossip 到集群
    ↓
Allocator 感知到 IO 过载
    ↓
避免向该 Store 转移 lease
```

**关键点**：
- **高优先级**：IO 过载是紧急信号，一旦超过 0.3 立即 gossip
- **单向触发**：只在 IO 过载增加时 gossip，减少时不主动 gossip（等周期性 gossip）

---

### 4.2 这些信号如何影响决策

#### 决策 1：是否立即 Gossip

**决策点**：`shouldGossipOnCapacityDelta()` 中

```go
if shouldGossip, reason := s.shouldGossipOnCapacityDelta(); shouldGossip {
    s.asyncGossipStore(...)
}
```

**影响因素**：
- 容量变化量（绝对 + 相对）
- 距离上次 gossip 的时间
- 是否正在 gossip

**即时 vs 滞后**：**即时**（在检测到变化时立即决策）

**局部 vs 全局**：**局部**（每个 Store 独立决策）

#### 决策 2：是否使用缓存的描述符

**决策点**：`asyncGossipStore()` 的 `useCached` 参数

```go
// 容量变化触发
s.asyncGossipStore(context.TODO(), reason, true /* useCached */)

// 负载变化触发
s.asyncGossipStore(context.TODO(), reason, false /* useCached */)
```

**影响因素**：
- 触发类型（容量 vs 负载）
- 是否需要完整的磁盘统计

**即时 vs 滞后**：**即时**（在触发 gossip 时决策）

#### 决策 3：是否启动异步任务

**决策点**：`asyncGossipStore()` 中

```go
if s.knobs.AsyncDisabled {
    gossipFn(ctx)  // 同步执行
    return
}

// 异步执行
s.stopper.RunAsyncTask(ctx, ..., gossipFn)
```

**影响因素**：
- 是否在测试环境
- Stopper 是否已停止

**即时 vs 滞后**：**滞后**（异步任务可能延迟执行）

---

### 4.3 为什么采用当前策略

#### 策略 1：阈值触发（而不是每次都 Gossip）

**原因**：
- **带宽限制**：Gossip 网络带宽有限，不能每次都 gossip
- **CPU 开销**：计算 `Store.Descriptor()` 需要调用 Pebble 的统计接口
- **最终一致性**：Allocator 不需要实时的容量信息

**替代方案**：**每次变化都 Gossip**

| 方案        | 优点                  | 缺点                    |
|-------------|----------------------|------------------------|
| 阈值触发    | 减少 Gossip 次数       | 容量信息可能滞后        |
| 每次都 Gossip | 容量信息最准确        | Gossip 风暴，CPU 爆炸   |

**为什么选择阈值触发**：
- Allocator 的决策不需要精确到个位数的 Range 数量
- 10% 的误差在可接受范围内

#### 策略 2：双重阈值（绝对 + 相对）

**原因**：
- **小基数问题**：相对变化大但绝对值小（不重要）
- **大基数问题**：绝对变化大但相对变化小（可能是噪声）

**替代方案**：**只用相对阈值**

| 方案        | 优点                  | 缺点                    |
|-------------|----------------------|------------------------|
| 双重阈值    | 平衡灵敏度和稳定性     | 配置复杂                |
| 只用相对    | 配置简单               | 小基数时过于敏感        |
| 只用绝对    | 配置简单               | 大基数时不够灵敏        |

**为什么选择双重阈值**：
- CockroachDB 的 Store 容量范围很大（从 10 个 Range 到 10 万个 Range）
- 双重阈值在各种规模下都能正常工作

#### 策略 3：异步 Gossip（而不是同步）

**原因**：
- **非阻塞**：不希望 Range 操作被 Gossip 阻塞
- **解耦**：Gossip 失败不应影响 Range 操作

**替代方案**：**同步 Gossip**

| 方案        | 优点                  | 缺点                    |
|-------------|----------------------|------------------------|
| 异步 Gossip | 不阻塞调用者           | 可能延迟（但通常很快）  |
| 同步 Gossip | 确定性强               | 阻塞 Range 操作         |

**为什么选择异步 Gossip**：
- Range 操作的延迟比 Gossip 延迟更重要
- Gossip 是**最终一致性**操作，延迟几百毫秒可以接受

---

### 4.4 设计如何在目标间取得平衡

#### 目标 1：及时性（Timeliness）

**机制**：
- 容量显著变化时立即 gossip（阈值触发）
- IO 过载时紧急 gossip（高优先级）

**代价**：
- 需要额外的决策逻辑（`shouldGossipOnCapacityDelta`）
- 可能出现 gossip 风暴（通过频率限制缓解）

#### 目标 2：准确性（Accuracy）

**机制**：
- 周期性 gossip 使用完整的 `Store.Descriptor()`（非缓存）
- 增量更新确保计数准确

**代价**：
- 周期性 gossip 有 CPU 开销（调用 Pebble 统计）
- 缓存可能与实际略有偏差

#### 目标 3：效率（Efficiency）

**机制**：
- 缓存容量信息（避免重复计算）
- 异步 gossip（不阻塞主路径）
- 阈值过滤（减少不必要的 gossip）

**代价**：
- 容量信息可能滞后
- 异步任务有额外的调度开销

#### 目标 4：稳定性（Stability）

**机制**：
- 频率限制（最快 2 秒一次）
- 双重阈值（避免噪声触发）
- 防止递归触发（`gossipOngoing` 标志）

**代价**：
- 可能错过某些容量变化
- 在高负载时 gossip 可能被延迟

**总体权衡**：

| 目标        | 权重 | 实现手段                        |
|-------------|------|--------------------------------|
| 及时性      | 高   | 阈值触发 + IO 过载优先           |
| 准确性      | 中   | 周期性重新计算 + 增量更新        |
| 效率        | 高   | 缓存 + 异步 + 阈值过滤           |
| 稳定性      | 高   | 频率限制 + 双重阈值 + 防递归     |

---

## 五、设计模式分析

### 5.1 模式 1：Observer Pattern（观察者模式）变种

#### 定义

**Subject**（被观察者）定义一个添加和通知观察者的接口，**Observer**（观察者）在被通知时更新自己的状态。

#### 在代码中的体现

```go
// Subject: Store 的容量状态
type Store struct {
    storeGossip *StoreGossip  // Observer
    ...
}

// Observer: StoreGossip
type StoreGossip struct {
    cachedCapacity *cachedCapacity  // 观察到的状态
    ...
}

// 事件通知
func (s *Store) addReplicaToRangeMap(repl *Replica) {
    ...
    // 通知观察者
    s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeAddEvent)
}
```

#### 为什么是"变种"

**经典 Observer Pattern**：
```go
// 经典模式
type Observer interface {
    Update(subject Subject)
}

type Subject struct {
    observers []Observer
}

func (s *Subject) Notify() {
    for _, obs := range s.observers {
        obs.Update(s)
    }
}
```

**CockroachDB 的变种**：
```go
// 变种：直接引用 + 事件类型
s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeAddEvent)
```

**为什么不用经典模式**？

| 经典模式                  | CockroachDB 变种             |
|--------------------------|------------------------------|
| 支持多个观察者            | 只有一个观察者（StoreGossip） |
| 观察者需要实现接口        | 直接调用方法                 |
| 观察者需要主动拉取状态    | 通过事件类型传递信息         |

**选择变种的原因**：
- **单一职责**：`StoreGossip` 是 `Store` 唯一的 gossip 管理器
- **性能**：避免接口调用和 for 循环的开销
- **简单性**：不需要注册/注销观察者的复杂逻辑

#### 是否属于事实标准

是的，Observer Pattern 在分布式系统中非常常见：

- **Kubernetes**：Controller 监听资源变化
- **Kafka**：Consumer 监听 Topic 变化
- **Prometheus**：Exporter 监听指标变化

---

### 5.2 模式 2：Cache-Aside Pattern（旁路缓存模式）

#### 定义

应用程序首先检查缓存，如果缓存未命中，则从数据源加载数据并更新缓存。

#### 在代码中的体现

```go
// GossipStore() 中
func (s *StoreGossip) GossipStore(ctx context.Context, useCached bool) error {
    var storeDesc *roachpb.StoreDescriptor

    if useCached {
        // Cache Hit: 使用缓存
        capacity := s.CachedCapacity()
        storeDesc = &roachpb.StoreDescriptor{
            Capacity: capacity,
            ...
        }
    } else {
        // Cache Miss: 从数据源加载
        storeDesc, err = s.descriptorGetter.Descriptor(ctx, false)
        if err != nil {
            return err
        }

        // 更新缓存
        s.cachedCapacity.Lock()
        s.cachedCapacity.cached = storeDesc.Capacity
        s.cachedCapacity.Unlock()
    }

    // 使用数据
    return s.gossiper.AddInfoProto(...)
}
```

#### 为什么选择这种模式

**问题**：`Store.Descriptor()` 调用 Pebble 的统计接口，非常昂贵

**解决方案**：
- 缓存容量信息（`cachedCapacity`）
- 只有在必要时才重新计算（`useCached=false`）

**类比**：

```
Cache-Aside Pattern          StoreGossip
       |                          |
   Application                GossipStore
       |                          |
   Check Cache             Check useCached
       |                          |
   Cache Hit?                 true?
   /        \                /        \
 Yes        No             Yes        No
  |          |              |          |
Return    Query DB     Return      Query
Cache   → Update      Cached    Descriptor
        Cache                  → Update
                               Cache
```

#### 是否属于事实标准

是的，在数据库和存储系统中非常常见：

- **Redis**：Cache-Aside 是最常见的缓存策略
- **Memcached**：应用程序手动管理缓存
- **CDN**：边缘节点缓存内容

---

### 5.3 模式 3：Throttling Pattern（限流模式）

#### 定义

限制某个操作的执行频率，避免资源耗尽或过载。

#### 在代码中的体现

```go
// 频率限制
func (s *StoreGossip) canEagerlyGossipNow() (canGossip bool) {
    now := s.clock.Now()
    s.cachedCapacity.Lock()
    defer s.cachedCapacity.Unlock()

    nextValidGossipTime := s.cachedCapacity.lastGossipedTime.Add(
        MaxStoreGossipFrequency.Get(s.sv))  // 2 秒

    return nextValidGossipTime.Before(now)
}

// 阈值限制
func deltaExceedsThreshold(old, cur, requiredMinDelta, requiredDeltaFraction float64) (exceeds bool, delta float64) {
    deltaAbsolute := math.Abs(cur - old)
    deltaFraction := deltaAbsolute / old

    // 必须同时满足绝对阈值和相对阈值
    exceeds = deltaAbsolute >= requiredMinDelta && deltaFraction >= requiredDeltaFraction
    return exceeds, delta
}
```

#### 为什么选择这种模式

**问题**：如果不限流，可能出现 gossip 风暴

```
场景：大量 Range 快速迁移
T0: RangeAddEvent × 100
    → 每次都触发 gossip
    → 100 次 gossip（每次几十 KB）
    → 带宽爆炸
```

**解决方案**：
- **频率限制**：最快 2 秒 gossip 一次
- **阈值限制**：变化必须超过 10% 才 gossip

**两层限流**：

```
                请求
                  |
                  v
         +-------------------+
         | 阈值限制           |  ← 过滤小变化
         | (10% 变化)        |
         +-------------------+
                  |
                  v
         +-------------------+
         | 频率限制           |  ← 过滤高频请求
         | (2 秒/次)         |
         +-------------------+
                  |
                  v
              Gossip
```

#### 是否属于事实标准

是的，在分布式系统和 API 设计中是标准模式：

- **API 网关**：限制每秒请求数（QPS）
- **Kubernetes**：限制 API 调用频率
- **云服务**：限制 IOPS 和带宽

---

### 5.4 模式 4：Reentrancy Guard Pattern（防重入保护模式）

#### 定义

使用标志位防止函数被递归调用。

#### 在代码中的体现

```go
// StoreGossip 中的防重入标志
type StoreGossip struct {
    gossipOngoing atomic.Bool  // 防止递归触发
    ...
}

func (s *StoreGossip) GossipStore(ctx context.Context, useCached bool) error {
    // 设置标志
    s.gossipOngoing.Store(true)
    defer s.gossipOngoing.Store(false)

    // 执行 Gossip
    storeDesc, err := s.descriptorGetter.Descriptor(ctx, useCached)
    ...
}

func (s *StoreGossip) shouldGossipOnCapacityDelta() (should bool, reason string) {
    // 检查标志
    if s.gossipOngoing.Load() || !s.canEagerlyGossipNow() {
        return false, ""
    }
    ...
}
```

#### 为什么选择这种模式

**问题**：Gossip 可能触发递归

```
GossipStore()
    → Descriptor()
        → computeCapacity()
            → 某些回调
                → MaybeGossipOnCapacityChange()
                    → GossipStore()  ← 递归！
```

**解决方案**：
- 在 `GossipStore()` 开始时设置 `gossipOngoing = true`
- 在 `shouldGossipOnCapacityDelta()` 中检查标志
- 如果标志为 `true`，跳过 gossip

**为什么用 `atomic.Bool` 而不是 `Mutex`**？

| 方案           | 优点                  | 缺点                    |
|---------------|----------------------|------------------------|
| atomic.Bool   | 无锁，性能高           | 只能表示简单状态        |
| Mutex         | 可以保护复杂状态       | 有锁开销                |

**选择 `atomic.Bool` 的原因**：
- 只需要一个布尔标志
- `shouldGossipOnCapacityDelta()` 在热路径上，需要高性能

#### 是否属于事实标准

是的，在并发编程中非常常见：

- **Linux 内核**：`in_interrupt()` 检查是否在中断上下文
- **JavaScript**：防抖（debounce）和节流（throttle）
- **GUI 编程**：防止事件处理递归

---

### 5.5 模式 5：Delta Encoding Pattern（增量编码模式）

#### 定义

只传输或处理变化的部分，而不是完整的状态。

#### 在代码中的体现

```go
// 增量更新
func (s *StoreGossip) MaybeGossipOnCapacityChange(ctx context.Context, cce CapacityChangeEvent) {
    s.cachedCapacity.Lock()
    switch cce {
    case RangeAddEvent:
        s.cachedCapacity.cached.RangeCount++  // 只更新变化的部分
    case RangeRemoveEvent:
        s.cachedCapacity.cached.RangeCount--
    }
    s.cachedCapacity.Unlock()

    // 只有变化超过阈值才 gossip
    if shouldGossip, reason := s.shouldGossipOnCapacityDelta(); shouldGossip {
        s.asyncGossipStore(context.TODO(), reason, true /* useCached */)
    }
}
```

#### 为什么选择这种模式

**问题**：每次 Range 操作都重新计算完整容量太昂贵

**解决方案**：
- **增量更新缓存**：`RangeCount++` 而不是重新计算
- **检查变化量**：只有累积变化超过阈值才 gossip

**类比**：

```
Full Encoding              Delta Encoding
(完整编码)                 (增量编码)
    |                          |
每次计算完整状态           只计算变化量
    |                          |
Store.Descriptor()         cached.RangeCount++
    ↓                          ↓
几十毫秒                   几纳秒
```

#### 是否属于事实标准

是的，在数据传输和存储中非常常见：

- **Git**：只存储文件的 diff
- **数据库 WAL**：只记录变化，不记录完整快照
- **视频编码**：只编码关键帧之间的差异

---

### 5.6 模式总结

| 模式                | 解决的问题                | 是否标准 | 相关代码                          |
|---------------------|--------------------------|---------|----------------------------------|
| Observer (变种)     | 容量变化通知              | ✅      | `MaybeGossipOnCapacityChange`    |
| Cache-Aside         | 避免昂贵的容量计算        | ✅      | `cachedCapacity`                 |
| Throttling          | 限制 Gossip 频率          | ✅      | `canEagerlyGossipNow`            |
| Reentrancy Guard    | 防止 Gossip 递归触发      | ✅      | `gossipOngoing`                  |
| Delta Encoding      | 增量更新容量              | ✅      | `RangeCount++`                   |

**核心理念**：
- **效率优先**：缓存 + 增量更新
- **稳定性优先**：限流 + 防递归
- **解耦**：观察者模式

---

## 六、具体运行示例

### 6.1 正常场景：周期性 Gossip

**初始状态**：
```
Store 已启动，包含 100 个 Range，50 个 Lease
cachedCapacity = {
    cached:           RangeCount=100, LeaseCount=50, QPS=1000
    lastGossiped:     RangeCount=100, LeaseCount=50, QPS=1000
    lastGossipedTime: T0 (00:00:00)
}
```

---

**步骤 1：10 秒后，周期性 Gossip 触发（T1 = 00:00:10）**

```go
ticker := time.NewTicker(10 * time.Second)
<-ticker.C

// 触发 Gossip
s.storeGossip.GossipStore(ctx, useCached=false)
```

**执行流程**：
```
T1 (00:00:10): GossipStore(useCached=false)
    ↓
Step 1: gossipOngoing = true
    ↓
Step 2: 调用 Store.Descriptor(ctx, cached=false)
    → 重新计算容量（昂贵操作，耗时 20ms）
    → 返回 StoreDescriptor{
        StoreID: 1,
        Capacity: {
            RangeCount: 102,  ← 实际值（可能有微小偏差）
            LeaseCount: 51,
            Available:  480GB,
            Used:       320GB,
            QPS:        1050,
            ...
        }
    }
    ↓
Step 3: 更新缓存
    cachedCapacity.cached = Capacity{RangeCount=102, LeaseCount=51, QPS=1050}
    cachedCapacity.lastGossiped = Capacity{RangeCount=102, LeaseCount=51, QPS=1050}
    cachedCapacity.lastGossipedTime = T1 (00:00:10)
    ↓
Step 4: Gossip 到网络
    gossiper.AddInfoProto("store-descriptor-1", storeDesc, 60s)
    ↓
Step 5: gossipOngoing = false
```

**状态变化**：
```
T0 → T1
lastGossiped.RangeCount: 100 → 102
lastGossiped.LeaseCount: 50 → 51
lastGossiped.QPS: 1000 → 1050
lastGossipedTime: 00:00:00 → 00:00:10
```

**关键点**：
- **重新计算**：周期性 Gossip 总是使用 `useCached=false`
- **修正偏差**：增量更新的微小误差被修正

---

### 6.2 边界场景 1：Range 数量快速增加

**初始状态**（T2 = 00:00:12）：
```
cachedCapacity = {
    cached:           RangeCount=102, LeaseCount=51, QPS=1050
    lastGossiped:     RangeCount=102, LeaseCount=51, QPS=1050
    lastGossipedTime: T1 (00:00:10)
}
```

---

**步骤 1：Range 分裂，增加 3 个 Range（T2 = 00:00:12）**

```go
// 3 个 Range 分裂（每次分裂产生 2 个 RangeAddEvent）
for i := 0; i < 3; i++ {
    s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeAddEvent)
    s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeAddEvent)
}
```

**状态变化**：
```
T2 (00:00:12): RangeAddEvent × 6
    ↓
cached.RangeCount: 102 → 103 → 104 → 105 → 106 → 107 → 108
    ↓
shouldGossipOnCapacityDelta() 检查
    deltaRangeCount = 108 - 102 = 6
    requiredMinDelta = 5
    6 > 5 → shouldGossip = true
    reason = "range-count(6.0) change"
    ↓
asyncGossipStore(reason, useCached=true)
```

**但是**：频率限制生效！

```
canEagerlyGossipNow() 检查
    lastGossipedTime = T1 (00:00:10)
    now = T2 (00:00:12)
    nextValidGossipTime = T1 + 2s = 00:00:12
    00:00:12 <= 00:00:12 → canGossip = true（刚好到时间）

→ 允许 Gossip
```

**执行 Gossip**：
```
T2 (00:00:12): GossipStore(useCached=true)
    ↓
Step 1: gossipOngoing = true
    ↓
Step 2: 使用缓存（不重新计算）
    storeDesc = StoreDescriptor{
        Capacity: cached{RangeCount=108, LeaseCount=51, QPS=1050}
    }
    ↓
Step 3: 更新 lastGossiped
    lastGossiped.RangeCount = 108
    lastGossipedTime = T2 (00:00:12)
    ↓
Step 4: Gossip 到网络
    ↓
Step 5: gossipOngoing = false
```

**状态变化**：
```
T1 → T2
cached.RangeCount: 102 → 108
lastGossiped.RangeCount: 102 → 108
lastGossipedTime: 00:00:10 → 00:00:12
```

**关键点**：
- **阈值触发**：变化达到 6 个 Range，超过阈值 5
- **使用缓存**：避免昂贵的重新计算
- **频率限制**：刚好到达 2 秒间隔

---

### 6.3 边界场景 2：QPS 翻倍

**初始状态**（T3 = 00:00:20）：
```
cachedCapacity = {
    cached:           RangeCount=108, LeaseCount=51, QPS=1050
    lastGossiped:     RangeCount=108, LeaseCount=51, QPS=1050
    lastGossipedTime: T2 (00:00:12)
}
```

---

**步骤 1：负载突增（T3 = 00:00:20）**

```go
// Store.computeMetrics() 每 10 秒调用一次
newQPS := 2100.0  // QPS 翻倍

s.storeGossip.RecordNewPerSecondStats(newQPS, ...)
```

**执行流程**：
```
T3 (00:00:20): RecordNewPerSecondStats(2100, ...)
    ↓
Step 1: 更新缓存
    cached.QPS = 2100
    ↓
Step 2: shouldGossipOnCapacityDelta() 检查
    deltaQPS = 2100 - 1050 = 1050
    deltaAbsolute = 1050 >= 100 ✓
    deltaFraction = 1050 / 1050 = 1.0 (100%)
    requiredDeltaFraction = 0.5 (50%)
    1.0 > 0.5 ✓
    → shouldGossip = true
    reason = "queries-per-second(1050.0) change"
    ↓
Step 3: canEagerlyGossipNow() 检查
    lastGossipedTime = T2 (00:00:12)
    now = T3 (00:00:20)
    nextValidGossipTime = T2 + 2s = 00:00:14
    00:00:20 > 00:00:14 ✓
    → canGossip = true
    ↓
Step 4: asyncGossipStore(reason, useCached=false)
    → 注意：useCached=false，需要重新计算
```

**执行 Gossip**：
```
T3 (00:00:20): GossipStore(useCached=false)
    ↓
Step 1: gossipOngoing = true
    ↓
Step 2: 重新计算容量
    storeDesc = Store.Descriptor(ctx, cached=false)
    → Capacity{RangeCount=108, LeaseCount=51, QPS=2100, ...}
    ↓
Step 3: 更新 lastGossiped
    lastGossiped = Capacity{RangeCount=108, LeaseCount=51, QPS=2100}
    lastGossipedTime = T3 (00:00:20)
    ↓
Step 4: Gossip 到网络
    ↓
Step 5: gossipOngoing = false
```

**状态变化**：
```
T2 → T3
cached.QPS: 1050 → 2100
lastGossiped.QPS: 1050 → 2100
lastGossipedTime: 00:00:12 → 00:00:20
```

**关键点**：
- **QPS 阈值高**：需要 50% 的相对变化
- **不使用缓存**：负载变化可能伴随其他状态变化，需要完整更新

---

### 6.4 异常场景：频率限制阻止 Gossip

**初始状态**（T4 = 00:00:21）：
```
cachedCapacity = {
    cached:           RangeCount=108, LeaseCount=51, QPS=2100
    lastGossiped:     RangeCount=108, LeaseCount=51, QPS=2100
    lastGossipedTime: T3 (00:00:20)
}
```

---

**步骤 1：1 秒后，Range 增加 6 个（T4 = 00:00:21）**

```go
// Range 快速增加
for i := 0; i < 6; i++ {
    s.storeGossip.MaybeGossipOnCapacityChange(ctx, RangeAddEvent)
}
```

**执行流程**：
```
T4 (00:00:21): RangeAddEvent × 6
    ↓
Step 1: 更新缓存
    cached.RangeCount: 108 → 114
    ↓
Step 2: shouldGossipOnCapacityDelta() 检查
    deltaRangeCount = 114 - 108 = 6
    6 > 5 ✓
    → shouldGossip = true (假设)
    ↓
Step 3: canEagerlyGossipNow() 检查
    lastGossipedTime = T3 (00:00:20)
    now = T4 (00:00:21)
    nextValidGossipTime = T3 + 2s = 00:00:22
    00:00:21 < 00:00:22 ✗
    → canGossip = false
    ↓
Step 4: shouldGossipOnCapacityDelta() 返回 false
    → 跳过 Gossip
```

**状态变化**：
```
T3 → T4
cached.RangeCount: 108 → 114
lastGossiped.RangeCount: 108 (未变化)
lastGossipedTime: 00:00:20 (未变化)
```

**结果**：
- Gossip 被**频率限制**阻止
- 容量信息暂时不一致（cached ≠ lastGossiped）
- 下次周期性 Gossip（T5 = 00:00:30）会修正

---

**步骤 2：下次周期性 Gossip（T5 = 00:00:30）**

```go
T5 (00:00:30): 周期性 Gossip 触发
    ↓
GossipStore(useCached=false)
    → 重新计算容量
    → Capacity{RangeCount=114, ...}
    ↓
更新 lastGossiped
    lastGossiped.RangeCount = 114
    lastGossipedTime = 00:00:30
    ↓
Gossip 到网络
```

**状态变化**：
```
T4 → T5
cached.RangeCount: 114 (不变)
lastGossiped.RangeCount: 108 → 114
lastGossipedTime: 00:00:20 → 00:00:30
```

**关键点**：
- **最终一致性**：即使某次 Gossip 被阻止，周期性 Gossip 也会修正
- **频率限制的重要性**：避免 Gossip 风暴

---

### 6.5 边界场景 3：IO 过载触发紧急 Gossip

**初始状态**（T6 = 00:00:35）：
```
cachedCapacity = {
    cached:           RangeCount=114, IOThresholdMax.Score=0.2
    lastGossiped:     RangeCount=114, IOThresholdMax.Score=0.2
    lastGossipedTime: T5 (00:00:30)
}
```

---

**步骤 1：IO 过载突增（T6 = 00:00:35）**

```go
// ioLoadListener 每秒更新
threshold := admissionpb.IOThreshold{...}
thresholdMax := admissionpb.IOThreshold{Score: 0.4}  // 过载！

s.storeGossip.RecordNewIOThreshold(threshold, thresholdMax)
```

**执行流程**：
```
T6 (00:00:35): RecordNewIOThreshold(thresholdMax.Score=0.4)
    ↓
Step 1: 更新缓存
    cached.IOThresholdMax.Score = 0.4
    ↓
Step 2: shouldGossipOnCapacityDelta() 检查
    cachedMaxIOScore = 0.4
    lastGossipMaxIOScore = 0.2
    gossipMinMaxIOOverloadScore = 0.3

    0.4 >= 0.3 ✓ (已过载)
    0.4 > 0.2 ✓ (在恶化)
    → updateForMaxIOOverloadScore = true
    reason = "io-overload(0.4) change"
    ↓
Step 3: canEagerlyGossipNow() 检查
    lastGossipedTime = T5 (00:00:30)
    now = T6 (00:00:35)
    nextValidGossipTime = T5 + 2s = 00:00:32
    00:00:35 > 00:00:32 ✓
    → canGossip = true
    ↓
Step 4: asyncGossipStore(reason="io-overload(0.4) change", useCached=true)
```

**执行 Gossip**：
```
T6 (00:00:35): GossipStore(useCached=true)
    ↓
使用缓存（IO 阈值已在缓存中更新）
    storeDesc = StoreDescriptor{
        Capacity: {IOThresholdMax.Score=0.4, ...}
    }
    ↓
Gossip 到网络
    → 其他节点感知到 IO 过载
    → Allocator 避免向该 Store 转移 lease
```

**状态变化**：
```
T5 → T6
cached.IOThresholdMax.Score: 0.2 → 0.4
lastGossiped.IOThresholdMax.Score: 0.2 → 0.4
lastGossipedTime: 00:00:30 → 00:00:35
```

**关键点**：
- **紧急信号**：IO 过载是高优先级信号，立即 gossip
- **保护机制**：避免向 IO 过载的 Store 转移更多负载

---

## 七、设计取舍与替代方案（Trade-offs）

### 7.1 当前方案 vs 每次变化都 Gossip

#### 当前方案：阈值触发 + 频率限制

**优点**：
- 减少 Gossip 次数（10x-100x）
- 减少带宽消耗
- 减少 CPU 开销（`Store.Descriptor()` 调用）

**缺点**：
- 容量信息可能滞后（最多 10 秒）
- 配置复杂（多个阈值参数）

#### 替代方案：每次变化都 Gossip

**优点**：
- 容量信息最准确
- 配置简单（无需阈值）

**缺点**：
- Gossip 风暴（每秒可能数百次）
- 带宽爆炸（每次 Gossip 几十 KB）
- CPU 爆炸（`Store.Descriptor()` 每秒数百次）

**权衡分析**：

| 指标            | 当前方案    | 每次都 Gossip |
|----------------|-----------|--------------|
| Gossip 次数     | ~10次/分钟 | ~1000次/分钟 |
| 带宽消耗        | ~100 KB/分钟 | ~10 MB/分钟 |
| CPU 开销        | 低        | 极高          |
| 容量信息滞后    | 最多 10 秒 | 0 秒          |

**为什么选择当前方案**：
- Allocator 的决策不需要秒级的准确性
- 带宽和 CPU 是宝贵资源
- 10 秒的滞后在可接受范围内

---

### 7.2 当前方案 vs 基于订阅的推送

#### 当前方案：Gossip 广播

**优点**：
- 简单（无需管理订阅关系）
- 最终一致性（Gossip 网络保证）

**缺点**：
- 广播到所有节点（即使某些节点不需要）
- 无法针对特定节点优化

#### 替代方案：基于订阅的推送

```go
// ❌ 假设的替代方案
type StoreCapacitySubscriber interface {
    OnCapacityChange(storeID roachpb.StoreID, capacity roachpb.StoreCapacity)
}

// Allocator 订阅容量变化
allocator.SubscribeToCapacityChanges(storeID, callback)
```

**优点**：
- 只推送给需要的节点
- 可以针对不同订阅者定制信息

**缺点**：
- 需要管理订阅关系（复杂）
- 订阅者故障会导致信息丢失
- 需要额外的 RPC 通道

**为什么选择当前方案**：
- Gossip 网络已经存在，无需额外基础设施
- Store 状态信息对所有节点都有用（Allocator、监控等）
- 订阅管理的复杂度不值得

---

### 7.3 当前方案 vs 集中式容量管理

#### 当前方案：分布式 Gossip

**优点**：
- 去中心化（无单点故障）
- 扩展性好（Gossip 网络自然扩展）

**缺点**：
- 最终一致性（可能滞后）
- 无法精确控制传播顺序

#### 替代方案：集中式容量管理

```go
// ❌ 假设的替代方案
type CentralCapacityManager struct {
    storeCapacities map[roachpb.StoreID]roachpb.StoreCapacity
}

// 每个 Store 向中心管理器报告
manager.ReportCapacity(storeID, capacity)

// Allocator 从中心管理器查询
capacity := manager.GetCapacity(storeID)
```

**优点**：
- 强一致性（中心管理器是单一真相来源）
- 可以精确控制

**缺点**：
- 单点故障（中心管理器故障导致全局不可用）
- 扩展性差（所有请求都到中心管理器）
- 延迟高（需要 RPC 到中心管理器）

**为什么选择当前方案**：
- CockroachDB 是去中心化系统，不应该有单点
- Gossip 网络的最终一致性足够好
- 中心管理器的扩展性问题难以解决

---

### 7.4 当前方案 vs 拉模式（Pull Model）

#### 当前方案：推模式（Push Model）

**优点**：
- 低延迟（容量变化立即推送）
- 无需轮询

**缺点**：
- 可能 Gossip 过多

#### 替代方案：拉模式（Pull Model）

```go
// ❌ 假设的替代方案

// Allocator 需要时主动查询
func (a *Allocator) GetStoreCapacity(storeID roachpb.StoreID) roachpb.StoreCapacity {
    return a.storePool.GetStore(storeID).Capacity
}

// StorePool 定期从 Store 拉取
func (sp *StorePool) pullCapacities() {
    for _, store := range sp.stores {
        capacity := store.GetCapacity()  // RPC 调用
        sp.updateCapacity(store.StoreID, capacity)
    }
}
```

**优点**：
- 按需获取（只有需要时才拉取）
- 无 Gossip 风暴

**缺点**：
- 延迟高（需要 RPC 往返）
- 轮询开销（需要定期拉取）
- 扩展性差（拉取者需要知道所有 Store）

**为什么选择当前方案**：
- Allocator 需要实时的容量信息（拉模式延迟太高）
- Gossip 网络已经存在，推模式无额外开销
- 阈值和频率限制解决了 Gossip 过多的问题

---

### 7.5 总结：当前方案的权衡

| 维度          | 当前方案得分 | 说明                                      |
|---------------|-------------|-------------------------------------------|
| 准确性        | ⭐⭐⭐⭐    | 最多 10 秒滞后，对 Allocator 足够         |
| 及时性        | ⭐⭐⭐⭐⭐  | 容量显著变化时立即 Gossip                 |
| 效率          | ⭐⭐⭐⭐⭐  | 缓存 + 阈值过滤 + 频率限制                |
| 扩展性        | ⭐⭐⭐⭐⭐  | Gossip 网络自然扩展                       |
| 复杂度        | ⭐⭐⭐      | 多个阈值参数，逻辑较复杂                  |
| 稳定性        | ⭐⭐⭐⭐⭐  | 防递归 + 频率限制 + 最终一致性             |

**核心权衡**：
- **优先保证效率和稳定性**，而不是秒级的准确性
- **优先去中心化**，而不是强一致性
- **优先简单的 Gossip**，而不是复杂的订阅机制

---

## 八、总结与心智模型（Mental Model）

### 8.1 核心思想总结

`StoreGossip` 是一个**智能的、自适应的、基于阈值的 Store 状态广播管理器**，它通过以下机制实现高效的容量信息传播：

1. **缓存机制**：避免昂贵的容量重新计算
2. **增量更新**：只更新变化的部分（Range/Lease 计数）
3. **阈值触发**：只有显著变化时才 Gossip（10% 或 50%）
4. **频率限制**：避免 Gossip 风暴（最快 2 秒/次）
5. **防递归**：通过 `gossipOngoing` 标志避免递归触发
6. **异步执行**：不阻塞主路径

### 8.2 可复用的心智模型

**你可以把 `StoreGossip` 理解为：**

> **一个带有"智能过滤器"的容量变化广播器**
>
> - Store 的容量像一个**实时变化的信号源**
> - `StoreGossip` 是一个**信号处理器**，对信号进行：
>   - **采样**（周期性 Gossip）
>   - **滤波**（阈值过滤小变化）
>   - **限流**（频率限制）
>   - **放大**（显著变化立即广播）
> - Gossip 网络是**广播信道**，将处理后的信号发送到所有节点
> - 其他节点（Allocator）是**信号接收器**，根据接收到的信号调整决策

**类比**：

| StoreGossip        | 类比                            |
|--------------------|---------------------------------|
| Store 容量变化     | 温度传感器的读数                |
| cachedCapacity     | 温度缓冲区                      |
| shouldGossipOnCapacityDelta | 温度变化检测器（> 1°C 才报告） |
| canEagerlyGossipNow | 报告频率限制器（最快 2 秒/次）  |
| GossipStore        | 温度广播器                      |
| Gossip 网络        | 无线通信网络                    |

### 8.3 设计哲学

1. **效率优于准确性**：
   - 容量信息不需要秒级准确
   - 阈值过滤 + 频率限制减少 Gossip 次数

2. **最终一致性**：
   - 容量信息可能短暂不一致
   - 周期性 Gossip 确保最终一致

3. **防御性编程**：
   - 防止递归触发（`gossipOngoing`）
   - 防止 Gossip 风暴（频率限制）
   - 防止过时数据（周期性重新计算）

4. **自适应**：
   - 根据负载自动调整 Gossip 频率
   - IO 过载时紧急 Gossip

### 8.4 使用建议

**对于使用者（例如 Store 或其他组件）：**

```go
// ✅ 正确用法

// 1. 在 Store 初始化时创建 StoreGossip
storeGossip := NewStoreGossip(gossiper, store, knobs, sv, clock)

// 2. 在容量变化时通知
storeGossip.MaybeGossipOnCapacityChange(ctx, RangeAddEvent)

// 3. 在负载指标更新时通知
storeGossip.RecordNewPerSecondStats(qps, wps, wbps, cpus)

// 4. 在 IO 阈值更新时通知
storeGossip.RecordNewIOThreshold(threshold, thresholdMax)

// 5. 周期性 Gossip（由 Store.startGossip() 管理）
```

**常见错误**：

```go
// ❌ 错误 1：手动调用 GossipStore 太频繁
for _, repl := range replicas {
    storeGossip.GossipStore(ctx, false)  // 每个副本都 gossip
}
// 应该：让 MaybeGossipOnCapacityChange 决定

// ❌ 错误 2：忘记更新缓存
// 某处修改了 Range 数量，但没有调用 MaybeGossipOnCapacityChange
// 应该：所有容量变化都通过 StoreGossip

// ❌ 错误 3：在测试中假设 Gossip 立即完成
storeGossip.MaybeGossipOnCapacityChange(ctx, RangeAddEvent)
// 立即检查 Gossip 结果 ← 可能还在异步执行
// 应该：使用 knobs.AsyncDisabled 在测试中同步执行
```

### 8.5 扩展性考虑

**如果未来需要支持以下场景，当前设计的适应性：**

| 场景                        | 适应性 | 说明                                      |
|-----------------------------|--------|-------------------------------------------|
| 更多的 Store（1000+ 节点）   | ⭐⭐⭐⭐⭐ | Gossip 网络自然扩展                       |
| 更频繁的容量变化（10倍）     | ⭐⭐⭐⭐   | 阈值和频率限制可以调整                     |
| 不同优先级的 Store           | ⭐⭐⭐    | 需要修改 Gossip TTL 或添加优先级标签      |
| 跨数据中心的 Store           | ⭐⭐⭐⭐   | Gossip 网络支持跨 DC                      |
| 实时容量预测                 | ⭐⭐     | 需要大幅改造，添加预测模型                 |

### 8.6 最终心智模型（伪代码）

```go
// 高度抽象的伪代码
type StoreGossip = {
    cache: {
        current:         StoreCapacity,  // 当前缓存的容量
        lastBroadcasted: StoreCapacity,  // 上次广播的容量
        lastBroadcastTime: Time,         // 上次广播时间
    },

    OnCapacityChange(event) {
        // 1. 增量更新缓存
        cache.current.Update(event)

        // 2. 检查是否需要广播
        if ShouldBroadcast(cache) {
            AsyncBroadcast(cache.current)
        }
    },

    ShouldBroadcast(cache) -> bool {
        // 防止递归
        if broadcasting { return false }

        // 频率限制
        if Now() - cache.lastBroadcastTime < 2s { return false }

        // 阈值检测
        delta = cache.current - cache.lastBroadcasted
        if delta >= Threshold {
            return true
        }

        return false
    },

    AsyncBroadcast(capacity) {
        RunInBackground(() => {
            broadcasting = true
            Gossip.Broadcast(capacity)
            cache.lastBroadcasted = capacity
            cache.lastBroadcastTime = Now()
            broadcasting = false
        })
    },

    PeriodicBroadcast() {
        Every(10s, () => {
            // 重新计算完整容量（修正累积误差）
            fullCapacity = Store.ComputeCapacity()
            cache.current = fullCapacity
            Broadcast(fullCapacity)
        })
    },
}
```

---

**结束语**：

`StoreGossip` 是一个**教科书式的自适应信息传播系统**，它通过多层过滤（阈值 + 频率限制）和智能决策（增量更新 + 周期性修正）实现了高效的 Store 状态广播。理解这个模块的关键是理解**为什么需要这些过滤机制**，而不仅仅是**这些机制如何实现**。

这种"先过滤、再传播"的设计思想在分布式系统中非常常见，适用于各种需要高效状态同步的场景。
