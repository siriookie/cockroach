# ReplicaScanner深度剖析——基于自适应节奏控制的Store级Replica后台扫描调度器

## 一、第一轮 BFS：职责边界与设计动机（Why）

### 1.1 系统性问题背景

在 CockroachDB 的 Store 中，每个 Store 可能包含**数千到数万个 Replica**（每个 Replica 对应一个 Range）。这些 Replica 需要定期进行**后台维护操作**，包括：

**维护操作清单**：
1. **MVCC GC**：清理过期的 MVCC 版本
2. **Raft Log GC**：清理已应用的 Raft 日志
3. **Range Split**：检查是否需要分裂（Range 太大）
4. **Range Merge**：检查是否需要合并（Range 太小）
5. **Replica Rebalance**：检查副本分布是否均衡
6. **Replica GC**：清理已移除的副本
7. **Consistency Check**：一致性检查
8. **Raft Snapshot**：检查是否需要发送快照
9. **Lease Transfer**：检查是否需要转移 lease

这些维护操作面临的核心问题：

**问题 1：如何高效地遍历所有 Replica**？
```
场景：Store 有 10,000 个 Replica
挑战：如何在合理的时间内扫描所有 Replica？
问题：
  - 如果扫描太快，会占用过多 CPU
  - 如果扫描太慢，维护操作会滞后
```

**问题 2：如何控制扫描节奏**？
```
场景：Store 的 Replica 数量动态变化
挑战：如何确保扫描周期稳定？
问题：
  - Replica 数量增加时，不能让扫描周期过长
  - Replica 数量减少时，不能让扫描过于频繁
```

**问题 3：如何避免 CPU 峰值**？
```
场景：扫描过程中会调用队列的 MaybeAddAsync
挑战：如何平滑 CPU 使用？
问题：
  - 如果不控制节奏，扫描会在短时间内完成，造成 CPU 峰值
  - 如果控制不当，可能导致扫描过慢
```

**问题 4：如何支持动态启用/禁用**？
```
场景：某些场景下需要暂停后台扫描
挑战：如何优雅地暂停和恢复？
问题：
  - 禁用时，正在扫描的 Replica 如何处理？
  - 禁用期间，Replica 移除事件如何处理？
```

**问题 5：如何支持多个队列**？
```
场景：每个 Replica 需要检查是否加入多个队列
挑战：如何高效地将 Replica 分发到多个队列？
问题：
  - 是否需要为每个队列单独扫描？（浪费）
  - 如何确保所有队列都能及时处理？
```

### 1.2 ReplicaScanner 解决的核心问题

`ReplicaScanner` 是一个**自适应节奏控制的后台扫描调度器**，它解决了以下核心问题：

1. **统一扫描入口**：
   - 所有队列共享同一个扫描循环
   - 每个 Replica 只被扫描一次，然后分发到所有队列

2. **自适应节奏控制**（Pacing）：
   - 根据目标扫描周期（如 10 分钟）和 Replica 数量动态计算等待时间
   - 确保扫描周期稳定，不受 Replica 数量变化影响

3. **CPU 平滑**：
   - 在扫描每个 Replica 之间插入等待时间
   - 避免短时间内的 CPU 峰值

4. **动态控制**：
   - 支持运行时启用/禁用扫描
   - 禁用时仍然处理 Replica 移除事件

5. **最小/最大空闲时间**：
   - 在小 Store 中，避免扫描过慢（最大空闲时间）
   - 在大 Store 中，避免扫描过快（最小空闲时间）

### 1.3 在系统中的位置

```
                    Store 启动
                         |
                         v
            +----------------------------+
            |     Store.Start()          |
            +----------------------------+
                         |
                         v
            +----------------------------+
            |  newReplicaScanner()       |
            |  - 创建 scanner 对象        |
            |  - 配置扫描周期              |
            +----------------------------+
                         |
                         v
            +----------------------------+
            |  scanner.AddQueues()       |
            |  - mvccGCQueue             |
            |  - mergeQueue              |
            |  - splitQueue              |
            |  - replicateQueue          |
            |  - replicaGCQueue          |
            |  - raftLogQueue            |
            |  - raftSnapshotQueue       |
            |  - consistencyQueue        |
            |  - leaseQueue              |
            +----------------------------+
                         |
                         v
            +----------------------------+
            |  scanner.Start()           |
            |  - 启动队列                 |
            |  - 启动扫描循环              |
            +----------------------------+
                         |
                         v
            +----------------------------+
            |     scanLoop()             |  ← 无限循环
            |  (后台 goroutine)          |
            +----------------------------+
                         |
                         v
        +--------------------------------+
        |   遍历所有 Replica              |
        |   (通过 replicaSet.Visit)      |
        +--------------------------------+
                         |
                         v
        +--------------------------------+
        |   对每个 Replica：              |
        |   1. paceInterval() 计算等待    |
        |   2. 等待指定时间               |
        |   3. 分发到所有队列              |
        |      queue.MaybeAddAsync()     |
        +--------------------------------+
```

**上游调用者**：
- `Store.Start()`：创建 `replicaScanner` 并启动

**下游依赖**：
- `replicaSet`（实际是 `storeReplicaVisitor`）：提供 Replica 遍历接口
- 多个 `replicaQueue`：接收扫描到的 Replica

**并行组件**：
- 各个队列的后台处理 goroutine（独立运行）

### 1.4 核心抽象与生命周期

#### 核心对象 1：replicaScanner（单例，Store 级别）

```go
type replicaScanner struct {
    log.AmbientContext
    clock   *hlc.Clock
    stopper *stop.Stopper

    // 扫描配置
    targetInterval time.Duration  // 目标扫描周期（如 10 分钟）
    minIdleTime    time.Duration  // 最小等待时间（如 10ms）
    maxIdleTime    time.Duration  // 最大等待时间（如 1s）

    waitTimer      timeutil.Timer // 共享定时器（避免重复分配）
    replicas       replicaSet     // Replica 集合（实际是 storeReplicaVisitor）
    queues         []replicaQueue // 管理的队列列表
    removed        chan *Replica  // Replica 移除通知通道

    // 统计信息（加锁保护）
    mu struct {
        syncutil.Mutex
        scanCount        int64         // 扫描轮数
        waitEnabledCount int64         // 等待启用的次数
        total            time.Duration // 总扫描时间
        disabled         bool          // 是否禁用
    }

    setDisabledCh chan struct{} // 禁用状态变化通知
}
```

**生命周期**：
- **创建**：`Store.Start()` 时调用 `newReplicaScanner()`
- **初始化**：调用 `AddQueues()` 添加队列
- **启动**：调用 `Start()` 启动扫描循环
- **运行**：持续扫描所有 Replica，直到 Store 停止
- **销毁**：`Store.Stop()` 时通过 `stopper` 停止

#### 核心接口 1：replicaSet

```go
type replicaSet interface {
    // Visit 遍历所有 Replica，直到 visitor 返回 false
    Visit(func(*Replica) bool)

    // EstimatedCount 返回剩余 Replica 的估计数量
    EstimatedCount() int
}
```

**实际实现**：`storeReplicaVisitor`（在 `store.go` 中）

```go
type storeReplicaVisitor struct {
    store   *Store
    repls   []*Replica // 快照：扫描开始时的所有 Replica
    visited int        // 已访问的数量
    order   storeReplicaVisitorOrder // 遍历顺序
}
```

**关键点**：
- `Visit()` 每次调用都会重新创建 Replica 快照（通过 `store.mu.replicasByRangeID.Range`）
- 快照机制避免了长时间持有锁
- 默认使用**随机顺序**遍历（避免多个 Store 同时扫描相同的 Replica）

#### 核心接口 2：replicaQueue

```go
type replicaQueue interface {
    Start(*stop.Stopper)
    MaybeAddAsync(context.Context, replicaInQueue, hlc.ClockTimestamp)
    AddAsync(context.Context, replicaInQueue, float64)
    MaybeRemove(roachpb.RangeID)
    Name() string
    NeedsLease() bool
    SetDisabled(disabled bool)
}
```

**实际实现**：
- `mvccGCQueue`
- `splitQueue`
- `mergeQueue`
- `replicateQueue`
- `replicaGCQueue`
- `raftLogQueue`
- `raftSnapshotQueue`
- `consistencyQueue`
- `leaseQueue`

### 1.5 设计动机总结

**核心理念**：**基于自适应节奏控制的统一扫描调度器**

1. **统一扫描**：
   - 所有队列共享同一个扫描循环
   - 避免每个队列独立扫描（浪费 CPU）

2. **自适应节奏**（Adaptive Pacing）：
   - 根据剩余时间和剩余 Replica 数量动态计算等待时间
   - 确保扫描周期稳定

3. **CPU 平滑**：
   - 在每个 Replica 之间插入等待
   - 避免短时间内的 CPU 峰值

4. **灵活控制**：
   - 支持动态禁用/启用
   - 支持最小/最大空闲时间约束

---

## 二、第二轮 BFS：控制流与组件协作（How it flows）

### 2.1 主要执行路径：扫描循环的完整生命周期

#### 阶段 0：创建与初始化（Store 启动时）

```go
// Store.Start() 中
s.scanner = newReplicaScanner(
    s.cfg.AmbientCtx,
    s.cfg.Clock,
    cfg.ScanInterval,       // 10 分钟（默认）
    cfg.ScanMinIdleTime,    // 可选
    cfg.ScanMaxIdleTime,    // 可选
    newStoreReplicaVisitor(s),
)
```

**执行步骤**：

```go
// scanner.go:92-115
func newReplicaScanner(
    ambient log.AmbientContext,
    clock *hlc.Clock,
    targetInterval, minIdleTime, maxIdleTime time.Duration,
    replicas replicaSet,
) *replicaScanner {
    if targetInterval < 0 {
        panic("scanner interval must be greater than or equal to zero")
    }

    rs := &replicaScanner{
        AmbientContext: ambient,
        clock:          clock,
        targetInterval: targetInterval,  // 10 分钟
        minIdleTime:    minIdleTime,
        maxIdleTime:    maxIdleTime,
        replicas:       replicas,        // storeReplicaVisitor
        removed:        make(chan *Replica),
        setDisabledCh:  make(chan struct{}, 1),
    }

    // 特殊情况：targetInterval == 0 表示禁用
    if targetInterval == 0 {
        rs.SetDisabled(true)
    }

    return rs
}
```

**状态变化**：
```
replicaScanner 对象被创建
targetInterval = 10 分钟
minIdleTime = 0（或配置值）
maxIdleTime = 0（或配置值）
replicas = storeReplicaVisitor{store: s}
disabled = false（除非 targetInterval == 0）
```

**关键点**：
- 如果 `targetInterval == 0`，scanner 被标记为禁用
- `removed` 通道用于接收 Replica 移除通知（无缓冲）
- `setDisabledCh` 通道用于通知禁用状态变化（有 1 个缓冲）

---

**添加队列**：

```go
// Store.Start() 中
s.scanner.AddQueues(
    s.mvccGCQueue,
    s.mergeQueue,
    s.splitQueue,
    s.replicateQueue,
    s.replicaGCQueue,
    s.raftLogQueue,
    s.raftSnapshotQueue,
    s.consistencyQueue,
    s.leaseQueue,
)
```

**执行步骤**：

```go
// scanner.go:119-121
func (rs *replicaScanner) AddQueues(queues ...replicaQueue) {
    rs.queues = append(rs.queues, queues...)
}
```

**状态变化**：
```
queues = [
    mvccGCQueue,
    mergeQueue,
    splitQueue,
    replicateQueue,
    replicaGCQueue,
    raftLogQueue,
    raftSnapshotQueue,
    consistencyQueue,
    leaseQueue,
]
```

---

#### 阶段 1：启动扫描循环（Store 启动后）

```go
// Store.Start() 中
s.scanner.Start()
```

**执行步骤**：

```go
// scanner.go:124-129
func (rs *replicaScanner) Start() {
    // Step 1: 启动所有队列
    for _, queue := range rs.queues {
        queue.Start(rs.stopper)
    }

    // Step 2: 启动扫描循环
    rs.scanLoop()
}
```

**scanLoop 执行流程**：

```go
// scanner.go:261-309
func (rs *replicaScanner) scanLoop() {
    ctx := rs.AnnotateCtx(context.Background())
    _ = rs.stopper.RunAsyncTask(ctx, "scan-loop", func(ctx context.Context) {
        start := timeutil.Now()  // 记录扫描开始时间
        defer rs.waitTimer.Stop()

        for {
            // Step 1: 检查是否禁用
            if rs.GetDisabled() {
                if done := rs.waitEnabled(); done {
                    return  // stopper 停止
                }
                continue
            }

            // Step 2: 遍历所有 Replica
            var shouldStop bool
            count := 0
            rs.replicas.Visit(func(repl *Replica) bool {
                count++
                shouldStop = rs.waitAndProcess(ctx, start, repl)
                return !shouldStop  // 如果需要停止，返回 false
            })

            // Step 3: 如果没有 Replica，等待一段时间
            if count == 0 {
                shouldStop = rs.waitAndProcess(ctx, start, nil)
            }

            // Step 4: 检查是否需要停止
            if shouldStop {
                return
            }

            // Step 5: 更新统计信息
            func() {
                rs.mu.Lock()
                defer rs.mu.Unlock()
                rs.mu.scanCount++
                rs.mu.total += timeutil.Since(start)
            }()

            // Step 6: 重置扫描开始时间
            start = timeutil.Now()
        }
    })
}
```

**状态变化**（第一轮扫描）：

```
T0 (00:00:00): 扫描开始
    start = T0
    ↓
T1: 开始遍历 Replica
    count = 0
    ↓
T2: 遍历到第 1 个 Replica (RangeID=1)
    count = 1
    → waitAndProcess(start, repl1)
        → paceInterval() 计算等待时间
        → 等待 60ms（假设 10000 个 Replica，10 分钟扫描周期）
        → 分发到所有队列
    ↓
T3: 遍历到第 2 个 Replica (RangeID=2)
    count = 2
    → waitAndProcess(start, repl2)
        → 等待 60ms
        → 分发到所有队列
    ↓
...
    ↓
T10000: 遍历完所有 Replica
    count = 10000
    ↓
T10001: 更新统计
    scanCount = 1
    total = 10 分钟（约）
    ↓
T10002: 重置 start
    start = T10001
    ↓
T10003: 开始第二轮扫描
```

**关键点**：
- 扫描循环在**后台 goroutine** 中运行
- 每轮扫描后，`start` 被重置，开始新一轮
- `count == 0` 的情况：Store 中没有 Replica，仍然需要等待（避免忙循环）

---

#### 阶段 2：节奏控制（Pacing）

**核心函数**：`paceInterval()`

```go
// scanner.go:186-206
func (rs *replicaScanner) paceInterval(start, now time.Time) time.Duration {
    // Step 1: 计算已经过的时间
    elapsed := now.Sub(start)

    // Step 2: 计算剩余时间
    remainingNanos := rs.targetInterval.Nanoseconds() - elapsed.Nanoseconds()
    if remainingNanos < 0 {
        remainingNanos = 0  // 已经超时，不等待
    }

    // Step 3: 获取剩余 Replica 数量
    count := rs.replicas.EstimatedCount()
    if count < 1 {
        count = 1
    }

    // Step 4: 计算平均等待时间
    interval := time.Duration(remainingNanos / int64(count))

    // Step 5: 应用最小空闲时间约束
    if rs.minIdleTime > 0 && interval < rs.minIdleTime {
        interval = rs.minIdleTime
    }

    // Step 6: 应用最大空闲时间约束
    if rs.maxIdleTime > 0 && interval > rs.maxIdleTime {
        interval = rs.maxIdleTime
    }

    return interval
}
```

**示例计算**（假设 10 分钟扫描周期，10000 个 Replica）：

```
初始状态：
  targetInterval = 10 分钟 = 600 秒 = 600,000,000,000 纳秒
  start = T0
  now = T0
  elapsed = 0
  remainingNanos = 600,000,000,000

第 1 次调用：
  count = EstimatedCount() = 10000
  interval = 600,000,000,000 / 10000 = 60,000,000 纳秒 = 60ms

第 2 次调用（60ms 后）：
  now = T0 + 60ms
  elapsed = 60ms = 60,000,000 纳秒
  remainingNanos = 600,000,000,000 - 60,000,000 = 599,940,000,000
  count = EstimatedCount() = 9999
  interval = 599,940,000,000 / 9999 ≈ 60,000,000 纳秒 = 60ms

第 5000 次调用（5 分钟后）：
  now = T0 + 5 分钟
  elapsed = 5 分钟 = 300,000,000,000 纳秒
  remainingNanos = 600,000,000,000 - 300,000,000,000 = 300,000,000,000
  count = EstimatedCount() = 5000
  interval = 300,000,000,000 / 5000 = 60,000,000 纳秒 = 60ms

第 10000 次调用（接近 10 分钟）：
  now = T0 + 9 分 59 秒
  elapsed = 599,000,000,000 纳秒
  remainingNanos = 600,000,000,000 - 599,000,000,000 = 1,000,000,000
  count = EstimatedCount() = 1
  interval = 1,000,000,000 / 1 = 1,000,000,000 纳秒 = 1 秒

第 10001 次调用（超时后）：
  now = T0 + 10 分 1 秒
  elapsed = 601,000,000,000 纳秒
  remainingNanos = 600,000,000,000 - 601,000,000,000 = -1,000,000,000 → 0
  count = EstimatedCount() = 0 → 1
  interval = 0 / 1 = 0 纳秒 = 0（立即继续）
```

**关键点**：
- **自适应计算**：等待时间根据剩余时间和剩余 Replica 数量动态调整
- **超时处理**：如果已经超时，`interval = 0`，立即继续
- **最小/最大约束**：确保等待时间在合理范围内

---

#### 阶段 3：等待并处理 Replica

**核心函数**：`waitAndProcess()`

```go
// scanner.go:212-243
func (rs *replicaScanner) waitAndProcess(ctx context.Context, start time.Time, repl *Replica) bool {
    // Step 1: 计算等待时间
    waitInterval := rs.paceInterval(start, timeutil.Now())
    rs.waitTimer.Reset(waitInterval)

    if log.V(6) {
        log.KvDistribution.Infof(ctx, "wait timer interval set to %s", waitInterval)
    }

    for {
        select {
        case <-rs.waitTimer.C:
            // 定时器触发
            if log.V(6) {
                log.KvDistribution.Infof(ctx, "wait timer fired")
            }

            if repl == nil {
                return false  // 没有 Replica 要处理
            }

            // Step 2: 将 Replica 分发到所有队列
            if log.V(2) {
                log.KvDistribution.Infof(ctx, "replica scanner processing %s", repl)
            }
            for _, q := range rs.queues {
                q.MaybeAddAsync(ctx, repl, rs.clock.NowAsClockTimestamp())
            }
            return false  // 处理完成

        case repl := <-rs.removed:
            // Replica 移除通知
            rs.removeReplica(repl)
            // 继续等待

        case <-rs.stopper.ShouldQuiesce():
            // Store 停止
            return true
        }
    }
}
```

**执行流程**：

```
T0: waitAndProcess(start, repl1) 调用
    ↓
T1: paceInterval() 返回 60ms
    waitTimer.Reset(60ms)
    ↓
T2: select 阻塞，等待 3 个事件之一
    ↓
T3 (60ms 后): waitTimer.C 触发
    ↓
T4: 遍历所有队列
    mvccGCQueue.MaybeAddAsync(repl1, now)
    mergeQueue.MaybeAddAsync(repl1, now)
    splitQueue.MaybeAddAsync(repl1, now)
    ...
    ↓
T5: 返回 false（继续扫描）
```

**Replica 移除的处理**：

```
T0: waitAndProcess(start, repl1) 调用
    waitTimer.Reset(60ms)
    ↓
T1 (30ms 后): 另一个线程调用 RemoveReplica(repl99)
    removed <- repl99
    ↓
T2: select 的 case <-rs.removed 触发
    removeReplica(repl99)
    → 从所有队列移除 repl99
    ↓
T3: 继续 for 循环，回到 select 阻塞
    ↓
T4 (再过 30ms): waitTimer.C 触发
    ↓
T5: 处理 repl1
```

**关键点**：
- **多路复用**：同时等待定时器、Replica 移除、Store 停止
- **即使在等待期间，也能处理 Replica 移除事件**
- **`repl == nil` 的情况**：没有 Replica 要处理（Store 为空），只是等待

---

#### 阶段 4：Replica 移除处理

```go
// scanner.go:245-256
func (rs *replicaScanner) removeReplica(repl *Replica) {
    // 从所有队列移除
    rangeID := repl.RangeID
    for _, q := range rs.queues {
        q.MaybeRemove(rangeID)
    }

    if log.V(6) {
        ctx := rs.AnnotateCtx(context.TODO())
        log.KvDistribution.Infof(ctx, "removed replica %s", repl)
    }
}
```

**调用路径**：

```
Store.RemoveReplica(repl)
    ↓
scanner.RemoveReplica(repl)
    ↓
removed <- repl  (非阻塞发送)
    ↓
scanLoop 中的 select
    case repl := <-rs.removed:
        removeReplica(repl)
```

**并发安全性**：
- `RemoveReplica()` 可能在任何 goroutine 中调用
- 通过通道传递 Replica，避免竞态
- 通道无缓冲，如果 scanner 忙，`RemoveReplica()` 会阻塞

---

#### 阶段 5：禁用/启用处理

**禁用**：

```go
// scanner.go:149-158
func (rs *replicaScanner) SetDisabled(disabled bool) {
    rs.mu.Lock()
    defer rs.mu.Unlock()
    rs.mu.disabled = disabled

    // 非阻塞通知
    select {
    case rs.setDisabledCh <- struct{}{}:
    default:
    }
}
```

**启用等待**：

```go
// scanner.go:313-332
func (rs *replicaScanner) waitEnabled() bool {
    rs.mu.Lock()
    rs.mu.waitEnabledCount++
    rs.mu.Unlock()

    for {
        if !rs.GetDisabled() {
            return false  // 已启用，继续扫描
        }

        select {
        case <-rs.setDisabledCh:
            // 禁用状态可能变化，重新检查
            continue

        case repl := <-rs.removed:
            // 即使禁用，仍然处理移除
            rs.removeReplica(repl)

        case <-rs.stopper.ShouldQuiesce():
            return true  // Store 停止
        }
    }
}
```

**执行流程**：

```
T0: scanner 正在扫描
    ↓
T1: 用户调用 SetDisabled(true)
    disabled = true
    setDisabledCh <- struct{}{}
    ↓
T2: scanLoop 下次循环检查 GetDisabled()
    → true
    ↓
T3: 调用 waitEnabled()
    waitEnabledCount++
    进入 for 循环
    ↓
T4: select 阻塞
    - 等待 setDisabledCh
    - 等待 removed
    - 等待 stopper
    ↓
T5: 用户调用 SetDisabled(false)
    disabled = false
    setDisabledCh <- struct{}{}
    ↓
T6: waitEnabled() 的 select 触发
    case <-rs.setDisabledCh:
        continue
    ↓
T7: 重新检查 GetDisabled()
    → false
    ↓
T8: waitEnabled() 返回 false
    ↓
T9: scanLoop 继续扫描
```

**关键点**：
- **禁用期间，仍然处理 Replica 移除**
- **非阻塞通知**：`setDisabledCh` 有 1 个缓冲，防止阻塞调用者
- **幂等性**：多次调用 `SetDisabled(true)` 是安全的

---

### 2.2 触发方式总结

| 操作                  | 触发方式           | 频率           |
|-----------------------|-------------------|---------------|
| 创建 scanner          | Store 启动时       | 一次          |
| 启动扫描循环           | Store 启动时       | 一次          |
| 扫描 Replica          | 定时（自适应节奏）   | 每轮扫描一次   |
| 移除 Replica          | 被动（通道通知）    | 按需          |
| 禁用 scanner          | 主动调用           | 按需          |
| 启用 scanner          | 主动调用           | 按需          |

---

### 2.3 与其他模块的交互

#### 交互 1：与 storeReplicaVisitor 的交互

```
scanner.scanLoop()
    ↓
replicas.Visit(func(repl *Replica) bool {
    ...
})
    ↓
storeReplicaVisitor.Visit()
    ↓
创建 Replica 快照（遍历 store.mu.replicasByRangeID）
    ↓
随机打乱顺序
    ↓
遍历快照中的每个 Replica
    ↓
调用 visitor(repl)
```

**关键点**：
- `Visit()` 每次调用都创建新快照
- 快照避免长时间持有锁
- 随机顺序避免多个 Store 同时扫描相同的 Replica

#### 交互 2：与队列的交互

```
scanner.waitAndProcess()
    ↓
for _, q := range rs.queues {
    q.MaybeAddAsync(ctx, repl, now)
}
    ↓
每个队列独立决定是否接受 Replica
    ↓
队列的后台 goroutine 处理 Replica
```

**关键点**：
- **异步分发**：`MaybeAddAsync` 不阻塞
- **独立决策**：每个队列独立判断是否接受
- **解耦**：scanner 不关心队列的处理结果

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 newReplicaScanner：构造函数

```go
// scanner.go:92-115
func newReplicaScanner(
    ambient log.AmbientContext,
    clock *hlc.Clock,
    targetInterval, minIdleTime, maxIdleTime time.Duration,
    replicas replicaSet,
) *replicaScanner {
    if targetInterval < 0 {
        panic("scanner interval must be greater than or equal to zero")
    }

    rs := &replicaScanner{
        AmbientContext: ambient,
        clock:          clock,
        targetInterval: targetInterval,
        minIdleTime:    minIdleTime,
        maxIdleTime:    maxIdleTime,
        replicas:       replicas,
        removed:        make(chan *Replica),
        setDisabledCh:  make(chan struct{}, 1),
    }

    if targetInterval == 0 {
        rs.SetDisabled(true)
    }

    return rs
}
```

**输入参数解析**：

| 参数           | 类型                | 实际传入值（Store 中）      | 作用                      |
|---------------|---------------------|---------------------------|--------------------------|
| ambient       | log.AmbientContext  | `s.cfg.AmbientCtx`        | 日志上下文                |
| clock         | *hlc.Clock          | `s.cfg.Clock`             | 混合逻辑时钟              |
| targetInterval | time.Duration      | `cfg.ScanInterval`        | 目标扫描周期（10 分钟）   |
| minIdleTime   | time.Duration       | `cfg.ScanMinIdleTime`     | 最小等待时间（可选）      |
| maxIdleTime   | time.Duration       | `cfg.ScanMaxIdleTime`     | 最大等待时间（可选）      |
| replicas      | replicaSet          | `newStoreReplicaVisitor(s)` | Replica 遍历接口       |

**为什么 `targetInterval < 0` 会 panic**？

```go
if targetInterval < 0 {
    panic("scanner interval must be greater than or equal to zero")
}
```

**原因**：
- 负数没有实际意义
- 如果需要禁用，应该传 `0`
- Panic 是防御性编程，捕获配置错误

**为什么 `targetInterval == 0` 会自动禁用**？

```go
if targetInterval == 0 {
    rs.SetDisabled(true)
}
```

**原因**：
- `0` 表示"不扫描"
- 自动禁用避免调用者忘记调用 `SetDisabled()`

**为什么 `removed` 通道无缓冲，`setDisabledCh` 有 1 个缓冲**？

| 通道              | 缓冲大小 | 原因                                      |
|------------------|---------|------------------------------------------|
| `removed`        | 0       | 确保 Replica 移除被及时处理，反压调用者     |
| `setDisabledCh`  | 1       | 避免 `SetDisabled()` 阻塞，非阻塞通知      |

**不变量**：
- 如果 `targetInterval == 0`，`disabled == true`
- `removed` 通道永远不会被关闭（直到 Store 停止）

---

### 3.2 paceInterval：自适应节奏计算

```go
// scanner.go:186-206
func (rs *replicaScanner) paceInterval(start, now time.Time) time.Duration {
    // Step 1: 计算已经过的时间
    elapsed := now.Sub(start)

    // Step 2: 计算剩余时间
    remainingNanos := rs.targetInterval.Nanoseconds() - elapsed.Nanoseconds()
    if remainingNanos < 0 {
        remainingNanos = 0
    }

    // Step 3: 获取剩余 Replica 数量
    count := rs.replicas.EstimatedCount()
    if count < 1 {
        count = 1
    }

    // Step 4: 计算平均等待时间
    interval := time.Duration(remainingNanos / int64(count))

    // Step 5: 应用最小空闲时间约束
    if rs.minIdleTime > 0 && interval < rs.minIdleTime {
        interval = rs.minIdleTime
    }

    // Step 6: 应用最大空闲时间约束
    if rs.maxIdleTime > 0 && interval > rs.maxIdleTime {
        interval = rs.maxIdleTime
    }

    return interval
}
```

**输入**：
- `start time.Time`：本轮扫描的开始时间
- `now time.Time`：当前时间

**输出**：
- `time.Duration`：应该等待的时间

**核心算法**：

```
interval = (目标周期 - 已过时间) / 剩余 Replica 数量
```

**关键点 1：为什么 `count < 1` 时设为 1**？

```go
if count < 1 {
    count = 1
}
```

**原因**：
- 避免除以 0
- 如果没有 Replica，等待时间应该是完整的 `targetInterval`

**关键点 2：为什么需要最小/最大空闲时间约束**？

| 场景                    | 问题                        | 解决方案           |
|------------------------|----------------------------|-------------------|
| 小 Store（10 个 Replica） | `interval` 可能过大（60 秒）  | `maxIdleTime` 限制 |
| 大 Store（100000 个 Replica）| `interval` 可能过小（6ms）   | `minIdleTime` 限制 |

**示例**：

```
场景 1：小 Store
  targetInterval = 10 分钟 = 600 秒
  count = 10
  interval = 600 / 10 = 60 秒
  maxIdleTime = 1 秒
  → 最终 interval = 1 秒（被限制）

场景 2：大 Store
  targetInterval = 10 分钟 = 600 秒
  count = 100000
  interval = 600 / 100000 = 0.006 秒 = 6ms
  minIdleTime = 10ms
  → 最终 interval = 10ms（被限制）
```

**关键点 3：为什么 `remainingNanos < 0` 时设为 0**？

```go
if remainingNanos < 0 {
    remainingNanos = 0
}
```

**原因**：
- 扫描已经超时（超过目标周期）
- 不等待，立即继续扫描
- 尽快完成本轮扫描

**不变量**：
- `interval >= 0`
- 如果设置了 `minIdleTime`，`interval >= minIdleTime`
- 如果设置了 `maxIdleTime`，`interval <= maxIdleTime`

**并发安全分析**：
- `paceInterval()` 是纯函数，不修改状态
- `rs.replicas.EstimatedCount()` 可能涉及锁（在 `storeReplicaVisitor` 中）

---

### 3.3 waitAndProcess：等待并处理

```go
// scanner.go:212-243
func (rs *replicaScanner) waitAndProcess(ctx context.Context, start time.Time, repl *Replica) bool {
    waitInterval := rs.paceInterval(start, timeutil.Now())
    rs.waitTimer.Reset(waitInterval)

    if log.V(6) {
        log.KvDistribution.Infof(ctx, "wait timer interval set to %s", waitInterval)
    }

    for {
        select {
        case <-rs.waitTimer.C:
            if log.V(6) {
                log.KvDistribution.Infof(ctx, "wait timer fired")
            }

            if repl == nil {
                return false
            }

            if log.V(2) {
                log.KvDistribution.Infof(ctx, "replica scanner processing %s", repl)
            }

            // 分发到所有队列
            for _, q := range rs.queues {
                q.MaybeAddAsync(ctx, repl, rs.clock.NowAsClockTimestamp())
            }
            return false

        case repl := <-rs.removed:
            rs.removeReplica(repl)

        case <-rs.stopper.ShouldQuiesce():
            return true
        }
    }
}
```

**输入**：
- `start time.Time`：本轮扫描的开始时间
- `repl *Replica`：要处理的 Replica（可能为 `nil`）

**输出**：
- `bool`：是否应该停止扫描（`true` = 停止）

**关键点 1：为什么使用 `for` 循环而不是单次 `select`**？

```go
for {
    select {
        case <-rs.waitTimer.C:
            ...
            return false

        case repl := <-rs.removed:
            rs.removeReplica(repl)
            // 继续循环

        case <-rs.stopper.ShouldQuiesce():
            return true
    }
}
```

**原因**：
- 等待期间可能有多个 Replica 移除事件
- 处理完移除事件后，需要继续等待定时器
- `for` 循环确保定时器到期前，所有移除事件都被处理

**关键点 2：为什么 `repl == nil` 时返回 `false`**？

```go
if repl == nil {
    return false
}
```

**原因**：
- `repl == nil` 表示没有 Replica 要处理（Store 为空）
- 返回 `false` 表示继续扫描（进入下一轮）
- 如果返回 `true`，扫描循环会停止

**关键点 3：为什么分发到所有队列**？

```go
for _, q := range rs.queues {
    q.MaybeAddAsync(ctx, repl, rs.clock.NowAsClockTimestamp())
}
```

**原因**：
- 每个 Replica 可能需要被多个队列处理
- 例如：同时需要 GC、分裂检查、一致性检查
- 统一扫描避免每个队列独立扫描

**不变量**：
- 如果返回 `true`，调用者应该停止扫描
- 如果返回 `false`，调用者应该继续扫描
- `waitTimer` 在函数返回前必须被停止或重置

**并发安全分析**：
- `waitTimer` 是 scanner 独占的，无竞争
- `removed` 通道可能被多个 goroutine 发送（但通道是并发安全的）
- `MaybeAddAsync()` 是异步的，队列内部处理并发安全

---

### 3.4 scanLoop：主扫描循环

```go
// scanner.go:261-309
func (rs *replicaScanner) scanLoop() {
    ctx := rs.AnnotateCtx(context.Background())
    _ = rs.stopper.RunAsyncTask(ctx, "scan-loop", func(ctx context.Context) {
        start := timeutil.Now()
        defer rs.waitTimer.Stop()

        for {
            // Step 1: 检查是否禁用
            if rs.GetDisabled() {
                if done := rs.waitEnabled(); done {
                    return
                }
                continue
            }

            // Step 2: 遍历所有 Replica
            var shouldStop bool
            count := 0
            rs.replicas.Visit(func(repl *Replica) bool {
                count++
                shouldStop = rs.waitAndProcess(ctx, start, repl)
                return !shouldStop
            })

            // Step 3: 如果没有 Replica，等待一段时间
            if count == 0 {
                shouldStop = rs.waitAndProcess(ctx, start, nil)
            }

            // Step 4: 检查是否需要停止
            if shouldStop {
                return
            }

            // Step 5: 更新统计信息
            func() {
                rs.mu.Lock()
                defer rs.mu.Unlock()
                rs.mu.scanCount++
                rs.mu.total += timeutil.Since(start)
            }()

            if log.V(6) {
                log.KvDistribution.Infof(ctx, "reset replica scan iteration")
            }

            // Step 6: 重置扫描开始时间
            start = timeutil.Now()
        }
    })
}
```

**输入**：无

**输出**：无（goroutine 持续运行直到停止）

**关键点 1：为什么使用 `RunAsyncTask`**？

```go
rs.stopper.RunAsyncTask(ctx, "scan-loop", func(ctx context.Context) {
    ...
})
```

**原因**：
- 扫描循环需要在后台运行
- `RunAsyncTask` 确保在 stopper 停止时，goroutine 会被通知
- 任务名称 `"scan-loop"` 用于调试和监控

**关键点 2：为什么 `defer rs.waitTimer.Stop()`**？

```go
defer rs.waitTimer.Stop()
```

**原因**：
- 确保 goroutine 退出时，定时器被停止
- 避免定时器泄漏（goroutine 退出但定时器仍在运行）

**关键点 3：为什么 `count == 0` 时调用 `waitAndProcess(ctx, start, nil)`**？

```go
if count == 0 {
    shouldStop = rs.waitAndProcess(ctx, start, nil)
}
```

**原因**：
- Store 中没有 Replica
- 仍然需要等待一段时间，避免忙循环
- `waitAndProcess()` 会等待目标周期后返回

**关键点 4：为什么更新统计信息需要加锁**？

```go
func() {
    rs.mu.Lock()
    defer rs.mu.Unlock()
    rs.mu.scanCount++
    rs.mu.total += timeutil.Since(start)
}()
```

**原因**：
- `scanCount` 和 `total` 可能被其他 goroutine 读取（如 `avgScan()`）
- 锁保证读写一致性

**关键点 5：为什么重置 `start` 而不是累加**？

```go
start = timeutil.Now()
```

**原因**：
- 每轮扫描是独立的
- 如果累加，误差会累积
- 重置确保每轮扫描的目标周期是准确的

**不变量**：
- `scanLoop()` 在单个 goroutine 中运行
- 每轮扫描后，`scanCount` 递增 1
- 每轮扫描后，`start` 被重置

---

### 3.5 SetDisabled：动态控制

```go
// scanner.go:149-158
func (rs *replicaScanner) SetDisabled(disabled bool) {
    rs.mu.Lock()
    defer rs.mu.Unlock()
    rs.mu.disabled = disabled

    // 非阻塞通知
    select {
    case rs.setDisabledCh <- struct{}{}:
    default:
    }
}
```

**输入**：
- `disabled bool`：是否禁用

**输出**：无

**关键点 1：为什么使用非阻塞 `select`**？

```go
select {
case rs.setDisabledCh <- struct{}{}:
default:
}
```

**原因**：
- 避免阻塞调用者
- 如果通道已满（缓冲区为 1），说明已经有通知在等待，无需重复通知

**关键点 2：为什么通道缓冲区为 1**？

```go
setDisabledCh: make(chan struct{}, 1),
```

**原因**：
- 缓冲区为 1 确保至少有一个通知不会阻塞
- 即使 scanner 正在处理其他事件，通知也能被缓存
- 缓冲区不需要更大，因为只需要通知"状态可能变化"，不需要记录每次变化

**不变量**：
- `SetDisabled()` 不会阻塞调用者
- `disabled` 的更新是原子的（通过 `mu` 保护）

---

### 3.6 waitEnabled：等待启用

```go
// scanner.go:313-332
func (rs *replicaScanner) waitEnabled() bool {
    rs.mu.Lock()
    rs.mu.waitEnabledCount++
    rs.mu.Unlock()

    for {
        if !rs.GetDisabled() {
            return false
        }

        select {
        case <-rs.setDisabledCh:
            continue

        case repl := <-rs.removed:
            rs.removeReplica(repl)

        case <-rs.stopper.ShouldQuiesce():
            return true
        }
    }
}
```

**输入**：无

**输出**：
- `bool`：是否应该停止（`true` = 停止）

**关键点 1：为什么在 `for` 循环开始时增加 `waitEnabledCount`**？

```go
rs.mu.Lock()
rs.mu.waitEnabledCount++
rs.mu.Unlock()
```

**原因**：
- 统计 scanner 进入等待启用状态的次数
- 用于测试和监控

**关键点 2：为什么每次循环都检查 `GetDisabled()`**？

```go
for {
    if !rs.GetDisabled() {
        return false
    }
    ...
}
```

**原因**：
- `setDisabledCh` 通知的是"状态可能变化"，不是"已启用"
- 需要重新检查实际状态

**关键点 3：为什么禁用期间仍然处理 Replica 移除**？

```go
case repl := <-rs.removed:
    rs.removeReplica(repl)
```

**原因**：
- Replica 移除是关键操作，不能延迟
- 即使不扫描，也需要从队列中移除已删除的 Replica
- 避免队列中积累已删除的 Replica

**不变量**：
- 如果返回 `false`，scanner 已启用
- 如果返回 `true`，stopper 已停止
- 禁用期间，所有 Replica 移除事件都被处理

---

## 四、运行时行为与系统反馈（Runtime Behavior）

### 4.1 系统如何感知运行时信号

#### 信号 1：Replica 数量变化

**感知方式**：通过 `EstimatedCount()`

```go
count := rs.replicas.EstimatedCount()
```

**实现**（在 `storeReplicaVisitor` 中）：

```go
func (rs *storeReplicaVisitor) EstimatedCount() int {
    rs.store.mu.RLock()
    defer rs.store.mu.RUnlock()

    count := rs.store.mu.replicasByRangeID.Len() - rs.visited
    if count < 1 {
        count = 1
    }
    return count
}
```

**反馈路径**：

```
Replica 数量变化
    ↓
EstimatedCount() 返回新值
    ↓
paceInterval() 使用新值计算等待时间
    ↓
等待时间自动调整
```

**关键点**：
- **即时感知**：每次调用 `paceInterval()` 都获取最新数量
- **自适应调整**：等待时间随 Replica 数量变化

#### 信号 2：扫描耗时（是否超时）

**感知方式**：通过 `elapsed` 计算

```go
elapsed := now.Sub(start)
remainingNanos := rs.targetInterval.Nanoseconds() - elapsed.Nanoseconds()
if remainingNanos < 0 {
    remainingNanos = 0
}
```

**反馈路径**：

```
扫描耗时过长（超过目标周期）
    ↓
remainingNanos < 0 → 设为 0
    ↓
paceInterval() 返回 0
    ↓
不等待，立即继续扫描
```

**关键点**：
- **滞后感知**：只有在扫描过程中才能发现超时
- **即时调整**：一旦超时，立即加速扫描

#### 信号 3：Replica 移除

**感知方式**：通过 `removed` 通道

```go
case repl := <-rs.removed:
    rs.removeReplica(repl)
```

**反馈路径**：

```
Store.RemoveReplica(repl)
    ↓
scanner.RemoveReplica(repl)
    ↓
removed <- repl
    ↓
scanLoop 接收通知
    ↓
从所有队列移除
```

**关键点**：
- **即时感知**：通道通知是即时的
- **优先处理**：即使在等待期间，也会立即处理

#### 信号 4：禁用/启用

**感知方式**：通过 `setDisabledCh` 通道

```go
case <-rs.setDisabledCh:
    continue
```

**反馈路径**：

```
用户调用 SetDisabled(true/false)
    ↓
disabled = true/false
    ↓
setDisabledCh <- struct{}{}
    ↓
scanLoop 或 waitEnabled 接收通知
    ↓
重新检查 disabled 状态
```

**关键点**：
- **即时通知**：通道通知是即时的
- **延迟生效**：下次循环检查时才生效

---

### 4.2 这些信号如何影响决策

#### 决策 1：是否继续扫描

**决策点**：`scanLoop()` 中

```go
if rs.GetDisabled() {
    if done := rs.waitEnabled(); done {
        return
    }
    continue
}
```

**影响因素**：
- `disabled` 状态
- stopper 是否停止

**即时 vs 滞后**：**滞后**（下次循环检查时生效）

**局部 vs 全局**：**局部**（只影响当前 scanner）

#### 决策 2：等待多久

**决策点**：`paceInterval()` 中

```go
interval := time.Duration(remainingNanos / int64(count))
```

**影响因素**：
- 剩余时间（`remainingNanos`）
- 剩余 Replica 数量（`count`）
- 最小/最大空闲时间约束

**即时 vs 滞后**：**即时**（每次调用都重新计算）

**局部 vs 全局**：**局部**（基于本地状态）

#### 决策 3：是否处理 Replica 移除

**决策点**：`waitAndProcess()` 和 `waitEnabled()` 中

```go
case repl := <-rs.removed:
    rs.removeReplica(repl)
```

**影响因素**：
- Replica 移除事件

**即时 vs 滞后**：**即时**（通道通知立即处理）

**局部 vs 全局**：**局部**（只影响当前 scanner 和队列）

---

### 4.3 为什么采用当前策略

#### 策略 1：自适应节奏控制（而不是固定间隔）

**原因**：
- Replica 数量动态变化
- 固定间隔无法保证扫描周期稳定

**替代方案**：**固定间隔**

```go
// ❌ 假设的替代方案
waitInterval := targetInterval / estimatedReplicaCount

// 问题：
// - 如果 Replica 数量增加，扫描周期会变长
// - 如果 Replica 数量减少，扫描周期会变短
```

**为什么选择自适应**：
- 确保扫描周期稳定在目标值附近
- 自动适应 Replica 数量变化

#### 策略 2：统一扫描（而不是每个队列独立扫描）

**原因**：
- 避免重复遍历 Replica
- 减少锁竞争（获取 Replica 列表需要锁）

**替代方案**：**每个队列独立扫描**

```go
// ❌ 假设的替代方案
for _, queue := range queues {
    go func(q replicaQueue) {
        for {
            store.mu.RLock()
            replicas := store.getAllReplicas()
            store.mu.RUnlock()

            for _, repl := range replicas {
                q.MaybeAddAsync(repl)
            }
        }
    }(queue)
}

// 问题：
// - 每个队列都遍历一次，浪费 CPU
// - 多个 goroutine 竞争 store.mu 锁
```

**为什么选择统一扫描**：
- 性能更好（只遍历一次）
- 锁竞争更少

#### 策略 3：通道传递 Replica 移除（而不是直接调用）

**原因**：
- 解耦：调用者不需要知道 scanner 的内部状态
- 并发安全：通道是并发安全的

**替代方案**：**直接调用**

```go
// ❌ 假设的替代方案
func (rs *replicaScanner) RemoveReplica(repl *Replica) {
    rs.mu.Lock()
    defer rs.mu.Unlock()

    for _, q := range rs.queues {
        q.MaybeRemove(repl.RangeID)
    }
}

// 问题：
// - 需要持有锁，可能阻塞调用者
// - 如果 scanner 正在等待，无法及时处理
```

**为什么选择通道**：
- 异步处理，不阻塞调用者
- 即使在等待期间，也能及时处理

#### 策略 4：最小/最大空闲时间约束（而不是无约束）

**原因**：
- 小 Store：避免扫描过慢
- 大 Store：避免扫描过快（CPU 峰值）

**替代方案**：**无约束**

```go
// ❌ 假设的替代方案
interval := remainingNanos / count

// 问题：
// - 小 Store（10 个 Replica）：interval = 60 秒（太慢）
// - 大 Store（100000 个 Replica）：interval = 6ms（太快）
```

**为什么选择约束**：
- 平衡扫描周期和 CPU 使用
- 适应不同规模的 Store

---

### 4.4 设计如何在目标间取得平衡

#### 目标 1：扫描周期稳定性

**机制**：
- 自适应节奏控制（`paceInterval()`）
- 动态计算等待时间

**代价**：
- 需要频繁计算（每个 Replica 都计算）
- 依赖 `EstimatedCount()` 的准确性

#### 目标 2：CPU 平滑

**机制**：
- 在每个 Replica 之间插入等待
- 最小/最大空闲时间约束

**代价**：
- 扫描周期可能略长（等待时间累积）
- 复杂的节奏控制逻辑

#### 目标 3：及时响应

**机制**：
- 通道传递 Replica 移除
- 即使在等待期间也处理移除

**代价**：
- 需要多路复用（`select` 三个通道）
- 代码复杂度增加

#### 目标 4：资源效率

**机制**：
- 统一扫描（所有队列共享）
- 共享定时器（避免重复分配）

**代价**：
- 队列之间不能独立调整扫描周期
- 需要协调队列的禁用/启用

**总体权衡**：

| 目标        | 权重 | 实现手段                        |
|-------------|------|--------------------------------|
| 扫描周期稳定 | 高   | 自适应节奏控制                  |
| CPU 平滑    | 高   | 等待时间插入 + 约束              |
| 及时响应    | 中   | 通道 + 多路复用                 |
| 资源效率    | 高   | 统一扫描 + 共享定时器           |

---

## 五、设计模式分析

### 5.1 模式 1：Adaptive Pacing Pattern（自适应节奏模式）

#### 定义

根据剩余时间和剩余工作量动态调整处理速度，确保在目标时间内完成所有工作。

#### 在代码中的体现

```go
func (rs *replicaScanner) paceInterval(start, now time.Time) time.Duration {
    elapsed := now.Sub(start)
    remainingNanos := rs.targetInterval.Nanoseconds() - elapsed.Nanoseconds()
    if remainingNanos < 0 {
        remainingNanos = 0
    }

    count := rs.replicas.EstimatedCount()
    if count < 1 {
        count = 1
    }

    interval := time.Duration(remainingNanos / int64(count))

    // 应用约束
    if rs.minIdleTime > 0 && interval < rs.minIdleTime {
        interval = rs.minIdleTime
    }
    if rs.maxIdleTime > 0 && interval > rs.maxIdleTime {
        interval = rs.maxIdleTime
    }

    return interval
}
```

#### 为什么选择这种模式

**问题**：如何确保扫描周期稳定？

```
场景：Store 有 10000 个 Replica，目标周期 10 分钟
挑战：
  - 前半段扫描可能很快（CPU 空闲）
  - 后半段扫描可能很慢（CPU 繁忙）
  - 如果不调整，扫描周期会不稳定
```

**解决方案**：
- 动态计算每个 Replica 的等待时间
- 根据剩余时间和剩余 Replica 数量调整
- 确保扫描周期接近目标值

**类比**：

```
Adaptive Pacing          导航中的"预计到达时间"
       |                          |
计算剩余时间            剩余距离 / 剩余时间 = 所需速度
       |                          |
计算剩余工作量          动态调整速度建议
       |                          |
动态调整速度            确保按时到达
```

#### 是否属于事实标准

是的，在系统调度和资源管理中非常常见：

- **Kubernetes**：Pod QoS 的动态调整
- **TCP 拥塞控制**：根据网络状况调整发送速率
- **垃圾回收**：根据堆大小和 GC 目标动态调整 GC 频率

---

### 5.2 模式 2：Visitor Pattern（访问者模式）

#### 定义

将数据结构的遍历逻辑与操作逻辑分离，允许在不修改数据结构的情况下定义新操作。

#### 在代码中的体现

```go
// 数据结构接口
type replicaSet interface {
    Visit(func(*Replica) bool)
    EstimatedCount() int
}

// 访问者
rs.replicas.Visit(func(repl *Replica) bool {
    count++
    shouldStop = rs.waitAndProcess(ctx, start, repl)
    return !shouldStop
})
```

#### 为什么选择这种模式

**问题**：如何遍历 Store 中的所有 Replica？

```
挑战：
  - Replica 存储在 Store 的 map 中
  - 遍历需要持有锁
  - 不能长时间持有锁（阻塞其他操作）
```

**解决方案**：
- `replicaSet` 接口抽象了遍历逻辑
- `Visit()` 方法内部创建快照，避免长时间持锁
- scanner 只需要提供处理逻辑（visitor 函数）

**类比**：

```
Visitor Pattern          文件遍历
       |                      |
replicaSet.Visit()      os.Walk()
       |                      |
visitor(repl)           visitor(file)
```

#### 是否属于事实标准

是的，在遍历和操作分离的场景中非常常见：

- **编译器**：AST 遍历和代码生成
- **文件系统**：`filepath.Walk()` 遍历文件树
- **容器**：`list.Range()` 遍历链表

---

### 5.3 模式 3：Fan-out Pattern（扇出模式）

#### 定义

将一个输入分发到多个处理器，每个处理器独立处理。

#### 在代码中的体现

```go
// 一个 Replica 分发到多个队列
for _, q := range rs.queues {
    q.MaybeAddAsync(ctx, repl, rs.clock.NowAsClockTimestamp())
}
```

**流程**：

```
           scanner
               |
         [Replica R1]
               |
   +-----------+-----------+----------+
   |           |           |          |
mvccGCQueue splitQueue mergeQueue  ...

每个队列独立决定是否接受 R1
```

#### 为什么选择这种模式

**问题**：每个 Replica 需要被多个队列检查

```
场景：
  - mvccGCQueue：检查是否需要 GC
  - splitQueue：检查是否需要分裂
  - mergeQueue：检查是否需要合并
  - ...
```

**解决方案**：
- scanner 统一扫描一次
- 将每个 Replica 分发到所有队列
- 每个队列独立决策

**替代方案**：**每个队列独立扫描**（已在 4.3 节分析）

#### 是否属于事实标准

是的，在消息分发和任务调度中非常常见：

- **消息队列**：Pub/Sub 模式
- **Kubernetes**：事件分发到多个 Controller
- **日志系统**：日志分发到多个输出

---

### 5.4 模式 4：Graceful Degradation Pattern（优雅降级模式）

#### 定义

在资源不足或异常情况下，系统能够降低服务质量但继续运行。

#### 在代码中的体现

**情况 1：扫描超时**

```go
remainingNanos := rs.targetInterval.Nanoseconds() - elapsed.Nanoseconds()
if remainingNanos < 0 {
    remainingNanos = 0  // 不等待，立即继续
}
```

**情况 2：禁用期间仍处理移除**

```go
func (rs *replicaScanner) waitEnabled() bool {
    for {
        ...
        select {
        case repl := <-rs.removed:
            rs.removeReplica(repl)  // 即使禁用，仍处理移除
        ...
        }
    }
}
```

**情况 3：最小/最大空闲时间约束**

```go
if rs.minIdleTime > 0 && interval < rs.minIdleTime {
    interval = rs.minIdleTime  // 避免过快
}
if rs.maxIdleTime > 0 && interval > rs.maxIdleTime {
    interval = rs.maxIdleTime  // 避免过慢
}
```

#### 为什么选择这种模式

**问题**：如何处理异常情况？

```
场景 1：扫描超时
  → 不等待，尽快完成本轮

场景 2：禁用 scanner
  → 停止扫描，但仍处理移除

场景 3：极端 Store 规模
  → 应用约束，避免极端行为
```

**解决方案**：
- 检测异常情况
- 降级但不完全停止
- 确保核心功能（如移除）仍然可用

#### 是否属于事实标准

是的，在分布式系统和服务设计中非常常见：

- **限流降级**：超过阈值时降低服务质量
- **熔断器**：部分失败时降级
- **超时重试**：失败后以降低的速率重试

---

### 5.5 模式 5：Channel-based Decoupling Pattern（基于通道的解耦模式）

#### 定义

使用通道在组件之间传递消息，实现异步和解耦。

#### 在代码中的体现

```go
// 定义
removed chan *Replica
setDisabledCh chan struct{}

// 发送
func (rs *replicaScanner) RemoveReplica(repl *Replica) {
    select {
    case rs.removed <- repl:
    case <-rs.stopper.ShouldQuiesce():
    }
}

func (rs *replicaScanner) SetDisabled(disabled bool) {
    select {
    case rs.setDisabledCh <- struct{}{}:
    default:
    }
}

// 接收
select {
case repl := <-rs.removed:
    rs.removeReplica(repl)
case <-rs.setDisabledCh:
    continue
}
```

#### 为什么选择这种模式

**问题**：如何实现异步通知？

```
场景 1：Replica 移除
  → Store 删除 Replica 后，需要通知 scanner
  → scanner 可能在等待（不能阻塞 Store）

场景 2：禁用状态变化
  → 用户调用 SetDisabled()
  → scanner 可能在扫描或等待
```

**解决方案**：
- 使用通道传递消息
- 发送者不需要知道接收者的状态
- 接收者通过 `select` 多路复用

**类比**：

```
Channel Decoupling       邮箱系统
       |                      |
removed <- repl         投递信件
       |                      |
select case <-removed    定期检查邮箱
```

#### 是否属于事实标准

是的，在 Go 并发编程中是标准模式：

- **Go 并发模式**："Don't communicate by sharing memory; share memory by communicating"
- **Actor 模型**：消息传递
- **事件驱动架构**：事件队列

---

### 5.6 模式总结

| 模式                | 解决的问题                | 是否标准 | 相关代码                          |
|---------------------|--------------------------|---------|----------------------------------|
| Adaptive Pacing     | 扫描周期稳定性            | ✅      | `paceInterval()`                 |
| Visitor             | 遍历与操作分离            | ✅      | `replicaSet.Visit()`             |
| Fan-out             | 一对多分发                | ✅      | `for _, q := range rs.queues`    |
| Graceful Degradation | 异常情况处理              | ✅      | 超时、禁用、约束                  |
| Channel Decoupling  | 异步通知                  | ✅      | `removed`、`setDisabledCh`        |

**核心理念**：
- **自适应**：根据运行时状态动态调整
- **解耦**：组件之间通过接口和通道通信
- **健壮**：优雅处理异常情况

---

## 六、具体运行示例

### 6.1 正常场景：完整的扫描循环

**初始状态**：
```
Store 启动完成
Replica 数量 = 10000
targetInterval = 10 分钟 = 600 秒
minIdleTime = 0
maxIdleTime = 0
scanCount = 0
```

---

**步骤 1：启动 scanner（T0 = 00:00:00）**

```go
scanner.Start()
```

**执行**：
```
T0 (00:00:00): scanLoop() 启动
    start = T0
    ↓
Step 1: GetDisabled() → false
    ↓
Step 2: 开始遍历 Replica
    replicas.Visit(func(repl *Replica) bool {
        count++
        shouldStop = waitAndProcess(ctx, start, repl)
        return !shouldStop
    })
```

---

**步骤 2：处理第 1 个 Replica（T1 = 00:00:00）**

```
T1: visitor 被调用，repl = Range 1
    count = 1
    ↓
waitAndProcess(start=T0, repl=Range 1)
    ↓
paceInterval(start=T0, now=T0)
    elapsed = 0
    remainingNanos = 600 * 10^9
    count = EstimatedCount() = 10000
    interval = 600 * 10^9 / 10000 = 60,000,000 ns = 60ms
    ↓
waitTimer.Reset(60ms)
    ↓
select 阻塞
    ↓
[60ms 后]
    ↓
T2 (00:00:00.060): waitTimer.C 触发
    ↓
分发到所有队列
    mvccGCQueue.MaybeAddAsync(Range 1, now)
    splitQueue.MaybeAddAsync(Range 1, now)
    ...
    ↓
waitAndProcess() 返回 false
```

---

**步骤 3：处理第 2 个 Replica（T3 = 00:00:00.060）**

```
T3: visitor 被调用，repl = Range 2
    count = 2
    ↓
waitAndProcess(start=T0, repl=Range 2)
    ↓
paceInterval(start=T0, now=T3)
    elapsed = 60ms = 60,000,000 ns
    remainingNanos = 600 * 10^9 - 60,000,000 = 599,940,000,000 ns
    count = EstimatedCount() = 9999
    interval = 599,940,000,000 / 9999 ≈ 60,000,000 ns = 60ms
    ↓
waitTimer.Reset(60ms)
    ↓
[60ms 后]
    ↓
T4 (00:00:00.120): 处理 Range 2
```

---

**步骤 4：中间省略...扫描到第 5000 个 Replica（T5000 = 00:05:00）**

```
T5000 (00:05:00): visitor 被调用，repl = Range 5000
    count = 5000
    ↓
paceInterval(start=T0, now=T5000)
    elapsed = 5 分钟 = 300,000,000,000 ns
    remainingNanos = 600 * 10^9 - 300 * 10^9 = 300,000,000,000 ns
    count = EstimatedCount() = 5000
    interval = 300,000,000,000 / 5000 = 60,000,000 ns = 60ms
    ↓
[等待 60ms 并处理]
```

**关键观察**：
- 等待时间始终是 60ms
- 扫描周期保持稳定

---

**步骤 5：扫描到最后一个 Replica（T10000 = 00:09:59）**

```
T10000 (00:09:59): visitor 被调用，repl = Range 10000
    count = 10000
    ↓
paceInterval(start=T0, now=T10000)
    elapsed = 599,000,000,000 ns
    remainingNanos = 600 * 10^9 - 599 * 10^9 = 1,000,000,000 ns = 1 秒
    count = EstimatedCount() = 1
    interval = 1,000,000,000 / 1 = 1,000,000,000 ns = 1 秒
    ↓
waitTimer.Reset(1秒)
    ↓
[1 秒后]
    ↓
T10001 (00:10:00): 处理 Range 10000
```

**关键观察**：
- 最后一个 Replica 等待 1 秒
- 确保扫描周期接近 10 分钟

---

**步骤 6：完成第一轮扫描（T10002 = 00:10:00）**

```
T10002 (00:10:00): Visit() 返回
    count = 10000
    shouldStop = false
    ↓
更新统计
    scanCount++ → 1
    total += (T10002 - T0) = 10 分钟
    ↓
重置 start
    start = T10002
    ↓
继续下一轮扫描
```

---

**状态变化总结**：

| 时间点      | count | 剩余 Replica | elapsed | remainingNanos | interval |
|------------|-------|-------------|---------|----------------|----------|
| T0         | 0     | 10000       | 0       | 600s           | 60ms     |
| T1 (60ms)  | 1     | 9999        | 60ms    | 599.94s        | 60ms     |
| T5000 (5m) | 5000  | 5000        | 5m      | 5m             | 60ms     |
| T10000 (9m59s) | 10000 | 1       | 599s    | 1s             | 1s       |

**关键点**：
- 扫描周期接近 10 分钟
- 每个 Replica 的等待时间自适应调整
- 扫描过程平滑，无 CPU 峰值

---

### 6.2 边界场景 1：Replica 数量动态增加

**初始状态**（T0 = 00:00:00）：
```
Replica 数量 = 5000
targetInterval = 10 分钟
正在进行第一轮扫描
已扫描 2500 个 Replica（耗时 5 分钟）
```

---

**步骤 1：Range 分裂，Replica 数量增加（T1 = 00:05:00）**

```
T1 (00:05:00): 大量 Range 分裂
    Replica 数量：5000 → 7500（增加 2500 个）
```

---

**步骤 2：下次调用 paceInterval（T2 = 00:05:00.030）**

```
T2: 处理第 2501 个 Replica
    ↓
paceInterval(start=T0, now=T2)
    elapsed = 5 分钟 = 300 秒
    remainingNanos = 600 - 300 = 300 秒
    count = EstimatedCount() = 7500 - 2500 = 5000（剩余）
    interval = 300s / 5000 = 60ms
    ↓
等待 60ms
```

**关键点**：
- **自动适应**：等待时间自动调整
- **扫描周期仍然稳定**：因为等待时间变短了

---

**步骤 3：继续扫描新增的 Replica**

```
T3 → T7500: 继续扫描剩余 5000 个 Replica
    每个 Replica 等待约 60ms
    ↓
T7500 (00:10:00): 完成第一轮扫描
    扫描了 7500 个 Replica
    耗时约 10 分钟
```

**关键点**：
- 即使 Replica 数量增加，扫描周期仍然接近 10 分钟
- 自适应节奏控制确保稳定性

---

### 6.3 边界场景 2：扫描超时

**初始状态**（T0 = 00:00:00）：
```
Replica 数量 = 10000
targetInterval = 10 分钟
正在进行第一轮扫描
```

---

**步骤 1：前 5000 个 Replica 扫描很慢（T1 = 00:12:00）**

```
T1 (00:12:00): 已扫描 5000 个 Replica
    耗时 12 分钟（超过目标 10 分钟）
    ↓
原因：
  - CPU 繁忙（其他任务占用 CPU）
  - 队列处理慢（阻塞了 waitAndProcess）
```

---

**步骤 2：下次调用 paceInterval（T2 = 00:12:00）**

```
T2: 处理第 5001 个 Replica
    ↓
paceInterval(start=T0, now=T2)
    elapsed = 12 分钟 = 720 秒
    remainingNanos = 600 - 720 = -120 秒 → 0
    count = EstimatedCount() = 5000
    interval = 0 / 5000 = 0
    ↓
不等待，立即继续
```

**关键点**：
- **超时检测**：`remainingNanos < 0` 被检测到
- **加速扫描**：不等待，尽快完成

---

**步骤 3：剩余 Replica 快速扫描**

```
T3 → T5000: 快速扫描剩余 5000 个 Replica
    每个 Replica 等待 0ms（立即继续）
    ↓
T5000 (00:12:05): 完成第一轮扫描
    扫描了 10000 个 Replica
    耗时 12 分 5 秒
```

**关键点**：
- 虽然超时，但仍然完成了扫描
- 不会无限延迟（优雅降级）

---

### 6.4 边界场景 3：禁用期间 Replica 移除

**初始状态**（T0 = 00:00:00）：
```
scanner 正在扫描
Replica 数量 = 10000
```

---

**步骤 1：用户禁用 scanner（T1 = 00:05:00）**

```
T1 (00:05:00): 用户调用 SetDisabled(true)
    disabled = true
    setDisabledCh <- struct{}{}
```

---

**步骤 2：scanner 检测到禁用（T2 = 00:05:00.001）**

```
T2: scanLoop 下次循环
    ↓
GetDisabled() → true
    ↓
调用 waitEnabled()
    waitEnabledCount++
    ↓
进入 for 循环
    select {
        case <-setDisabledCh:
        case repl := <-removed:
        case <-stopper.ShouldQuiesce():
    }
```

---

**步骤 3：Replica 被移除（T3 = 00:05:30）**

```
T3 (00:05:30): Store 删除 Range 999
    ↓
Store.RemoveReplica(Range 999)
    ↓
scanner.RemoveReplica(Range 999)
    removed <- Range 999
    ↓
waitEnabled() 的 select 触发
    case repl := <-removed:
        removeReplica(Range 999)
        → mvccGCQueue.MaybeRemove(999)
        → splitQueue.MaybeRemove(999)
        → ...
    ↓
继续 for 循环（仍在等待启用）
```

**关键点**：
- **即使禁用，仍然处理移除**
- 避免队列中积累已删除的 Replica

---

**步骤 4：用户启用 scanner（T4 = 00:06:00）**

```
T4 (00:06:00): 用户调用 SetDisabled(false)
    disabled = false
    setDisabledCh <- struct{}{}
    ↓
waitEnabled() 的 select 触发
    case <-setDisabledCh:
        continue
    ↓
重新检查 GetDisabled() → false
    ↓
waitEnabled() 返回 false
    ↓
scanLoop 继续扫描
```

**状态变化**：

| 时间点      | 事件                  | disabled | 动作              |
|------------|----------------------|----------|------------------|
| T0         | scanner 正常扫描      | false    | 扫描中           |
| T1         | 用户禁用             | true     | 停止扫描         |
| T2         | scanner 检测到禁用    | true     | 进入 waitEnabled |
| T3         | Replica 移除         | true     | 处理移除         |
| T4         | 用户启用             | false    | 继续扫描         |

---

### 6.5 边界场景 4：最小/最大空闲时间约束

**场景 1：小 Store（10 个 Replica）**

```
初始状态：
  Replica 数量 = 10
  targetInterval = 10 分钟 = 600 秒
  maxIdleTime = 1 秒

计算：
  count = 10
  interval = 600s / 10 = 60 秒
  ↓
应用约束：
  maxIdleTime = 1 秒
  interval > maxIdleTime → interval = 1 秒
  ↓
实际等待 1 秒

结果：
  扫描周期 = 10 × 1秒 = 10 秒（而不是 10 分钟）
  提前完成扫描
```

**场景 2：大 Store（100000 个 Replica）**

```
初始状态：
  Replica 数量 = 100000
  targetInterval = 10 分钟 = 600 秒
  minIdleTime = 10ms

计算：
  count = 100000
  interval = 600s / 100000 = 6ms
  ↓
应用约束：
  minIdleTime = 10ms
  interval < minIdleTime → interval = 10ms
  ↓
实际等待 10ms

结果：
  扫描周期 = 100000 × 10ms = 1000 秒 = 16.7 分钟（而不是 10 分钟）
  延长了扫描周期
```

**权衡**：
- 小 Store：提前完成，避免过慢
- 大 Store：延长周期，避免 CPU 峰值

---

## 七、设计取舍与替代方案（Trade-offs）

### 7.1 当前方案 vs 固定间隔扫描

#### 当前方案：自适应节奏控制

**优点**：
- 扫描周期稳定（接近目标值）
- 自动适应 Replica 数量变化

**缺点**：
- 需要频繁计算等待时间
- 依赖 `EstimatedCount()` 的准确性

#### 替代方案：固定间隔扫描

```go
// ❌ 假设的替代方案
interval := targetInterval / estimatedReplicaCount

for {
    for _, repl := range replicas {
        time.Sleep(interval)
        processReplica(repl)
    }
}
```

**优点**：
- 实现简单
- 计算开销小

**缺点**：
- 扫描周期不稳定（受 Replica 数量变化影响）
- 如果 Replica 数量增加，扫描周期会变长

**权衡分析**：

| 指标            | 当前方案    | 固定间隔     |
|----------------|-----------|------------|
| 扫描周期稳定性   | ⭐⭐⭐⭐⭐  | ⭐⭐        |
| 计算开销        | ⭐⭐⭐      | ⭐⭐⭐⭐⭐   |
| 适应性          | ⭐⭐⭐⭐⭐  | ⭐⭐        |

**为什么选择当前方案**：
- 扫描周期稳定性是核心需求
- 计算开销可接受（只是简单的除法）

---

### 7.2 当前方案 vs 每个队列独立扫描

#### 当前方案：统一扫描

**优点**：
- 性能好（只遍历一次）
- 锁竞争少（只获取一次 Replica 列表）

**缺点**：
- 队列之间不能独立调整扫描周期
- 需要协调队列的禁用/启用

#### 替代方案：每个队列独立扫描

```go
// ❌ 假设的替代方案
for _, queue := range queues {
    go func(q replicaQueue) {
        for {
            for _, repl := range getAllReplicas() {
                q.MaybeAddAsync(repl)
                time.Sleep(interval)
            }
        }
    }(queue)
}
```

**优点**：
- 队列独立（可以独立调整扫描周期）
- 无需协调

**缺点**：
- 性能差（每个队列都遍历一次）
- 锁竞争多（每个队列都获取 Replica 列表）

**权衡分析**：

| 指标            | 当前方案    | 独立扫描     |
|----------------|-----------|------------|
| 性能            | ⭐⭐⭐⭐⭐  | ⭐⭐        |
| 灵活性          | ⭐⭐⭐      | ⭐⭐⭐⭐⭐   |
| 锁竞争          | ⭐⭐⭐⭐⭐  | ⭐⭐        |

**为什么选择当前方案**：
- 性能优先（遍历 10000 个 Replica 是昂贵的）
- 队列的扫描周期通常不需要独立调整

---

### 7.3 当前方案 vs 基于优先级的扫描

#### 当前方案：顺序扫描

**优点**：
- 实现简单
- 公平（所有 Replica 都被扫描）

**缺点**：
- 无法优先处理重要的 Replica
- 扫描顺序随机（可能延迟关键 Replica）

#### 替代方案：基于优先级的扫描

```go
// ❌ 假设的替代方案
type prioritizedReplica struct {
    repl     *Replica
    priority int
}

replicas := getPrioritizedReplicas()
sort.Slice(replicas, func(i, j int) bool {
    return replicas[i].priority > replicas[j].priority
})

for _, pr := range replicas {
    processReplica(pr.repl)
}
```

**优点**：
- 重要的 Replica 被优先处理
- 可以根据负载动态调整优先级

**缺点**：
- 实现复杂（需要定义优先级）
- 排序开销（10000 个 Replica 的排序）
- 公平性差（低优先级 Replica 可能被饿死）

**权衡分析**：

| 指标            | 当前方案    | 优先级扫描   |
|----------------|-----------|------------|
| 实现复杂度      | ⭐⭐⭐⭐⭐  | ⭐⭐        |
| 公平性          | ⭐⭐⭐⭐⭐  | ⭐⭐⭐      |
| 响应性          | ⭐⭐⭐      | ⭐⭐⭐⭐⭐   |

**为什么选择当前方案**：
- 后台扫描不需要极高的响应性
- 公平性和简单性更重要

---

### 7.4 当前方案 vs 事件驱动扫描

#### 当前方案：定期扫描

**优点**：
- 简单（无需监听事件）
- 可靠（不会漏掉 Replica）

**缺点**：
- 可能滞后（最多延迟一个扫描周期）
- 浪费（扫描不需要处理的 Replica）

#### 替代方案：事件驱动扫描

```go
// ❌ 假设的替代方案
type ReplicaEvent struct {
    repl  *Replica
    event string  // "created", "modified", "deleted"
}

events := make(chan ReplicaEvent)

// 监听事件
for event := range events {
    for _, queue := range queues {
        queue.MaybeAddAsync(event.repl)
    }
}
```

**优点**：
- 响应快（事件发生时立即处理）
- 效率高（只处理变化的 Replica）

**缺点**：
- 实现复杂（需要事件系统）
- 可能漏事件（如果事件系统有 bug）
- 无法处理"周期性检查"（如一致性检查）

**权衡分析**：

| 指标            | 当前方案    | 事件驱动     |
|----------------|-----------|------------|
| 响应性          | ⭐⭐⭐      | ⭐⭐⭐⭐⭐   |
| 可靠性          | ⭐⭐⭐⭐⭐  | ⭐⭐⭐      |
| 实现复杂度      | ⭐⭐⭐⭐⭐  | ⭐⭐        |

**为什么选择当前方案**：
- 后台维护不需要极高的响应性
- 可靠性和简单性更重要
- 某些队列需要周期性检查（如一致性检查）

---

### 7.5 总结：当前方案的权衡

| 维度          | 当前方案得分 | 说明                                      |
|---------------|-------------|-------------------------------------------|
| 扫描周期稳定性 | ⭐⭐⭐⭐⭐  | 自适应节奏控制确保稳定                     |
| CPU 平滑      | ⭐⭐⭐⭐⭐  | 等待时间插入 + 约束                        |
| 性能          | ⭐⭐⭐⭐⭐  | 统一扫描，只遍历一次                       |
| 公平性        | ⭐⭐⭐⭐⭐  | 所有 Replica 都被扫描                      |
| 灵活性        | ⭐⭐⭐      | 队列之间不能独立调整                       |
| 响应性        | ⭐⭐⭐      | 最多延迟一个扫描周期                       |

**核心权衡**：
- **优先保证稳定性和性能**，而不是响应性
- **优先保证公平性**，而不是优先级
- **优先简单性**，而不是复杂的优化

---

## 八、总结与心智模型（Mental Model）

### 8.1 核心思想总结

`replicaScanner` 是一个**基于自适应节奏控制的统一后台扫描调度器**，它通过以下机制实现高效、稳定的 Replica 后台维护：

1. **自适应节奏控制**：根据剩余时间和剩余 Replica 数量动态计算等待时间
2. **统一扫描**：所有队列共享同一个扫描循环，避免重复遍历
3. **CPU 平滑**：在每个 Replica 之间插入等待时间，避免 CPU 峰值
4. **优雅降级**：超时、禁用、极端规模等异常情况下仍能正常工作
5. **异步解耦**：通过通道传递消息，实现组件之间的异步通信

### 8.2 可复用的心智模型

**你可以把 `replicaScanner` 理解为：**

> **一个带有"自动驾驶"功能的后台清洁工**
>
> - Store 中的 Replica 像**需要定期清洁的房间**
> - scanner 是**清洁工**，需要在 10 分钟内清洁所有房间
> - **自动驾驶**：
>   - 根据剩余时间和剩余房间数量，自动调整清洁速度
>   - 如果时间不够，加速清洁（不休息）
>   - 如果时间充裕，放慢清洁（休息时间长）
> - **一次清洁，多重检查**：
>   - 清洁工进入一个房间
>   - 同时检查多个项目（垃圾、灰尘、整理等）
>   - 将需要处理的项目分发到不同的队列（垃圾队列、灰尘队列等）
> - **即使休息，也能响应紧急通知**：
>   - 休息期间，如果有房间被拆除，立即从清洁列表中移除

**类比**：

| replicaScanner        | 清洁工                            |
|-----------------------|----------------------------------|
| Replica               | 房间                             |
| 扫描周期（10 分钟）    | 清洁时间限制                      |
| paceInterval()        | 自动驾驶（根据进度调整速度）       |
| 队列                  | 不同的维护任务（垃圾、灰尘等）     |
| 通道                  | 紧急通知（房间拆除）               |

### 8.3 设计哲学

1. **自适应优于固定**：
   - 根据运行时状态动态调整
   - 适应 Replica 数量变化

2. **统一优于分散**：
   - 所有队列共享扫描循环
   - 避免重复工作

3. **平滑优于突发**：
   - 等待时间插入
   - CPU 使用平滑

4. **健壮优于脆弱**：
   - 优雅降级（超时、禁用、极端规模）
   - 异常情况下仍能工作

### 8.4 使用建议

**对于使用者（Store 或测试）：**

```go
// ✅ 正确用法

// 1. 创建 scanner
scanner := newReplicaScanner(
    ambientCtx,
    clock,
    10 * time.Minute,  // 目标周期
    10 * time.Millisecond,  // 最小空闲时间（可选）
    1 * time.Second,  // 最大空闲时间（可选）
    newStoreReplicaVisitor(store),
)

// 2. 添加队列
scanner.AddQueues(mvccGCQueue, splitQueue, mergeQueue, ...)

// 3. 启动
scanner.Start()

// 4. 移除 Replica 时通知
scanner.RemoveReplica(repl)

// 5. 动态控制
scanner.SetDisabled(true)   // 禁用
scanner.SetDisabled(false)  // 启用
```

**常见错误**：

```go
// ❌ 错误 1：忘记调用 AddQueues
scanner.Start()  // 没有队列，扫描无效

// ❌ 错误 2：忘记通知 Replica 移除
store.removeReplica(repl)
// 没有调用 scanner.RemoveReplica(repl)
// 队列中会积累已删除的 Replica

// ❌ 错误 3：targetInterval 设置为负数
scanner := newReplicaScanner(..., -10 * time.Minute, ...)
// panic!
```

### 8.5 扩展性考虑

**如果未来需要支持以下场景，当前设计的适应性：**

| 场景                        | 适应性 | 说明                                      |
|-----------------------------|--------|-------------------------------------------|
| 更多的 Replica（100k+）      | ⭐⭐⭐⭐⭐ | 自适应节奏控制确保扫描周期稳定             |
| 更多的队列（20+）            | ⭐⭐⭐⭐⭐ | 统一扫描，队列数量不影响性能               |
| 不同优先级的 Replica         | ⭐⭐⭐    | 需要修改为优先级队列                       |
| 事件驱动的扫描               | ⭐⭐     | 需要大幅改造，添加事件系统                 |
| 多个扫描周期                 | ⭐⭐⭐    | 需要多个 scanner 实例                      |

### 8.6 最终心智模型（伪代码）

```go
// 高度抽象的伪代码
type ReplicaScanner = {
    targetInterval: Duration,   // 目标周期（如 10 分钟）
    replicas: ReplicaSet,        // Replica 集合
    queues: []Queue,             // 队列列表

    ScanLoop() {
        while true {
            start := Now()

            for repl in replicas {
                // 自适应计算等待时间
                interval := (targetInterval - Elapsed(start)) / RemainingCount()
                Wait(interval)

                // 分发到所有队列
                for queue in queues {
                    queue.MaybeAdd(repl)
                }
            }

            // 统计
            scanCount++
        }
    },

    AdaptivePacing(start, now) -> Duration {
        elapsed := now - start
        remaining := targetInterval - elapsed
        if remaining < 0 {
            return 0  // 超时，不等待
        }

        count := replicas.EstimatedCount()
        interval := remaining / count

        // 应用约束
        if interval < minIdleTime {
            return minIdleTime
        }
        if interval > maxIdleTime {
            return maxIdleTime
        }

        return interval
    },
}
```

---

**结束语**：

`replicaScanner` 是一个**教科书式的自适应调度系统**，它通过自适应节奏控制、统一扫描、优雅降级等机制，实现了高效、稳定、健壮的后台 Replica 维护。理解这个模块的关键是理解**为什么需要自适应**，而不仅仅是**如何实现自适应**。

这种"根据剩余时间和剩余工作量动态调整速度"的设计思想在分布式系统、操作系统、网络协议中都有广泛应用，是系统编程的核心能力之一。
