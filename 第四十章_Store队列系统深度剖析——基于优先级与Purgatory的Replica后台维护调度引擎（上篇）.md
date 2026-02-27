# 第四十章_Store队列系统深度剖析——基于优先级与Purgatory的Replica后台维护调度引擎（上篇）

## 一、第一轮 BFS：职责边界与设计动机（Why）

### 1.1 系统性问题背景

在 CockroachDB 的 Store 层，每个 Store 可能包含 **数千到数万个 Replica**。这些 Replica 需要持续进行 **多种不同类型的后台维护操作**，每种操作有不同的优先级、约束条件和执行频率要求。

**核心挑战**：如何设计一个统一的调度框架来管理这些异构的后台任务？

#### 1.1.1 问题矩阵

```
维护操作分类：

维护类型 | 触发频率 | 需要 Lease? | 可能失败? | 影响范围
---------|---------|------------|-----------|----------
Lease Transfer | 按需 + 周期 | ✅ 必须 | ✅ 高 | Range 级
MVCC GC | 周期 (每 2 小时) | ❌ 不需要 | ❌ 低 | Replica 级
Range Split | 按需 + 周期 | ✅ 必须 | ✅ 高 | Range 级
Range Merge | 周期 | ✅ 必须 | ✅ 高 | Range 级
Replica Add/Remove | 按需 + 周期 | ✅ 必须 | ✅ 极高 | Range 级
Replica GC | 周期 | ❌ 不需要 | ❌ 低 | Replica 级
Raft Log Truncate | 周期 | ❌ 不需要 | ❌ 低 | Replica 级
Raft Snapshot | 按需 | ❌ 不需要 | ✅ 中 | Replica 级
Consistency Check | 周期 (每 24 小时) | ❌ 不需要 | ❌ 低 | Replica 级
```

**问题 1：如何避免为每种操作单独实现调度器？**
```
场景：10 种维护操作 × 10,000 个 Replica = 100,000 个待调度任务
挑战：
  - 如果每种操作独立调度，会有 10 个独立的调度循环
  - 每个调度器独立扫描所有 Replica，CPU 开销极大
  - 无法统一控制并发度和优先级
```

**问题 2：如何处理维护操作的失败与重试？**
```
场景：Replica 正在 rebalancing，暂时无法 Split
挑战：
  - 不能无限重试（浪费 CPU）
  - 不能永久放弃（可能之后条件满足）
  - 需要等待特定事件再重试（如 rebalancing 完成）
```

**问题 3：如何确保关键操作优先执行？**
```
场景：某个 Range 严重 under-replicated（只剩 1 个副本）
挑战：
  - 该 Replica 应该立即被 replicateQueue 处理
  - 但其他低优先级任务（如 GC）可能已经在队列中
  - 需要支持优先级插队
```

**问题 4：如何避免重复工作？**
```
场景：Scanner 每 10 分钟扫描一次，但 Replica 可能在扫描间隙主动入队
挑战：
  - 同一个 Replica 可能被多次加入队列
  - 需要去重机制
  - 但优先级可能变化，需要支持更新
```

#### 1.1.2 如果没有统一的队列系统

假设每个维护操作独立实现：

```go
// 反例：每种操作独立调度

// Store 结构体会包含 10 个独立的协程
type Store struct {
    // 每个协程独立扫描所有 Replica
    leaseTransferLoop    *goroutine
    mvccGCLoop           *goroutine
    splitLoop            *goroutine
    mergeLoop            *goroutine
    replicateLoop        *goroutine
    replicaGCLoop        *goroutine
    raftLogTruncateLoop  *goroutine
    raftSnapshotLoop     *goroutine
    consistencyCheckLoop *goroutine
    // ...
}

// 每个协程独立实现
func (s *Store) leaseTransferLoop() {
    for {
        s.mu.Lock()
        replicas := s.getAllReplicas() // 扫描所有 Replica
        s.mu.Unlock()

        for _, repl := range replicas {
            if shouldTransferLease(repl) {
                transferLease(repl) // 无优先级控制
            }
        }
        time.Sleep(10 * time.Minute) // 固定间隔
    }
}

// 问题：
// 1. 10 个协程同时扫描 10,000 个 Replica = 100,000 次检查/周期
// 2. 无法处理失败重试（每次都重新检查）
// 3. 无法支持按需触发（只能等下一个周期）
// 4. 无法统一控制并发度
```

**实际后果**：
- **CPU 峰值**：10 个协程同时扫描，CPU 使用率飙升
- **响应延迟**：紧急操作（如 under-replicated）可能等待数分钟
- **重复工作**：Scanner 和事件触发可能同时处理同一个 Replica
- **难以测试**：10 个独立的状态机，组合爆炸

### 1.2 队列系统的核心抽象

CockroachDB 采用 **统一队列架构 + 具体队列实现分离** 的设计：

```
┌─────────────────────────────────────────────────────────────────┐
│                         replicaScanner                          │
│  职责：统一扫描所有 Replica，调用所有队列的 shouldQueue()      │
│  频率：10 分钟一轮（自适应节奏控制）                            │
└───────────────────┬─────────────────────────────────────────────┘
                    │ MaybeAdd(replica)
                    ↓
        ┌───────────────────────────────────┐
        │ 10 个队列共享同一个 baseQueue     │
        │ 基础设施：                        │
        │  - 优先级堆（priorityQueue）      │
        │  - Purgatory（失败重试机制）      │
        │  - 异步处理（processSem 限流）    │
        │  - 前置条件检查（needsLease 等）  │
        └───────────────────────────────────┘
                    │
        ┌───────────┴───────────┬───────────────┬──────────────┐
        ↓                       ↓               ↓              ↓
   leaseQueue            mvccGCQueue      splitQueue    replicateQueue
   (具体实现)            (具体实现)       (具体实现)    (具体实现)
   shouldQueue()         shouldQueue()    shouldQueue() shouldQueue()
   process()             process()        process()     process()
```

**核心思想**：
1. **扫描职责分离**：`replicaScanner` 统一扫描，避免重复遍历
2. **队列基础设施复用**：所有队列共享 `baseQueue` 的优先级、去重、限流逻辑
3. **具体策略可插拔**：每个队列实现 `queueImpl` 接口，定义自己的 `shouldQueue` 和 `process`

### 1.3 10 个队列的职责边界

#### 1.3.1 Lease 管理队列

**leaseQueue**：
- **职责**：管理 Range Lease 的转移，确保 Lease 按负载均衡策略分布
- **触发条件**：
  - Lease 在非首选 Leaseholder 上（违反 `lease_preferences`）
  - 节点负载不均衡（QPS 或 CPU 差异过大）
  - 节点进入 draining 状态（需要将 Lease 转移走）
  - IO overload 导致需要 shed leases
- **约束**：
  - `needsLease=true`：只有 Leaseholder 才能发起 Lease 转移
  - `needsSpanConfigs=true`：需要知道 `lease_preferences` 配置
  - `acceptsUnsplitRanges=false`：未分裂的 Range 可能跨多个 zone，无法确定首选 Leaseholder

#### 1.3.2 数据清理队列

**mvccGCQueue**：
- **职责**：清理过期的 MVCC 版本和已解决的 Intent
- **触发条件**：
  - `GCScore` 超过阈值（基于垃圾数据量和时间）
  - Intent 过多且超过一定时长
  - 通过 range tombstone 删除的数据（最高优先级 `deleteRangePriority`）
- **约束**：
  - `needsLease=false`：每个 Replica 可以独立 GC
  - `acceptsUnsplitRanges=true`：GC 与 Split 无关
  - `processDestroyedReplicas=true`：可以处理 `destroyReasonMergePending` 的 Replica

**replicaGCQueue**：
- **职责**：清理已从 Range Descriptor 中移除的本地 Replica 数据
- **触发条件**：
  - Replica 不在最新的 Range Descriptor 中
  - Replica 长时间未收到 Raft 消息（12 小时）
  - 被怀疑已移除（`replicaIsSuspect`）
- **约束**：
  - `needsLease=false`：清理本地数据不需要 Lease
  - `processDestroyedReplicas=true`：专门处理已销毁的 Replica

**raftLogQueue**：
- **职责**：截断已应用到状态机的 Raft 日志
- **触发条件**：
  - Raft 日志大小超过阈值
  - 所有 Follower 已应用到某个 Index
  - Replica 进入静默状态（quiescent）时，可以截断整个日志
- **约束**：
  - `needsLease=false`：日志截断是 Raft 层操作，不需要 Lease
  - `acceptsUnsplitRanges=true`：与 Split 无关

#### 1.3.3 Range 分裂与合并队列

**splitQueue**：
- **职责**：将过大的 Range 分裂成两个小 Range
- **触发条件**：
  - Range 大小超过 `range_max_bytes`
  - Range 负载（QPS）超过 `load_based_splitting` 阈值
  - Span Config 要求在特定 Key 分裂
- **约束**：
  - `needsLease=true`：只有 Leaseholder 才能发起 Split
  - `acceptsUnsplitRanges=true`：Split Queue 的职责就是分裂，当然要接受未分裂的 Range
  - `maxConcurrency=4`：Split 涉及昂贵的 RocksDB 扫描，限制并发

**mergeQueue**：
- **职责**：将过小的 Range 与右邻居合并
- **触发条件**：
  - Range 大小低于 `range_min_bytes`
  - 合并后的大小不会超过 `range_max_bytes`
  - 两个 Range 的 Zone Config 兼容
- **约束**：
  - `needsLease=true`：只有 Leaseholder 才能发起 Merge
  - `acceptsUnsplitRanges=false`：必须先确保 Range 已正确分裂
  - `maxConcurrency=1`：Merge 需要 Rebalance 右侧 Range 的副本，非常昂贵

#### 1.3.4 副本复制队列

**replicateQueue**：
- **职责**：确保每个 Range 的副本数量、位置、类型符合配置要求
- **触发条件**：
  - Under-replicated（副本数 < `num_replicas`）
  - Over-replicated（副本数 > `num_replicas`）
  - 违反 `constraints` 或 `lease_preferences`
  - 存在 Learner Replica（需要提升为 Voter）
  - 节点进入 decommissioning 状态
  - 负载不均衡（某些 Store 副本过多/过少）
- **约束**：
  - `needsLease=true`：只有 Leaseholder 才能发起 Replica 变更
  - `acceptsUnsplitRanges=false`：必须先知道 Zone Config
  - `maxConcurrency=1`：副本变更可能涉及 Snapshot，串行处理避免流量峰值

#### 1.3.5 快照与一致性队列

**raftSnapshotQueue**：
- **职责**：检测是否需要向 Follower 发送 Raft Snapshot
- **触发条件**：
  - Follower 落后太多，需要的日志已被截断
  - 新增 Replica 需要初始化
- **约束**：
  - `needsLease=false`：Follower 也需要接收 Snapshot
  - `needsSpanConfigs=false`：Snapshot 可能是为了让 Span Config Range 可用，不能依赖它
  - `acceptsUnsplitRanges=true`：Snapshot 与 Split 无关

**consistencyQueue**：
- **职责**：定期检查 Range 副本间的数据一致性
- **触发条件**：
  - 上次检查时间超过 `consistency_check.interval`（默认 24 小时）
- **约束**：
  - `needsLease=false`：每个 Replica 独立计算 Checksum
  - `acceptsUnsplitRanges=true`：一致性检查与 Split 无关
  - 速率限制：`consistency_check.max_rate` 控制扫描速度

#### 1.3.6 MMA Store Rebalancer

**mmaStoreRebalancer**：
- **职责**：基于 MMA（Multi-region Multi-tenant Allocator）策略进行 Store 级别的负载均衡
- **触发条件**：
  - Store 的副本数与集群平均值差异过大
  - Store 的磁盘使用率不均衡
- **说明**：这不是传统的队列，而是一个独立的 Rebalancer，但在初始化流程中与队列一起创建

### 1.4 在系统中的位置

```
Store 启动流程：
  └─ NewStore(...)
      ├─ 创建 replicaScanner（扫描器）
      ├─ 创建 10 个队列：
      │   ├─ leaseQueue        ← 依赖 allocator（决策引擎）
      │   ├─ mvccGCQueue       ← 独立（本地 GC）
      │   ├─ mergeQueue        ← 依赖 db（发送 AdminMerge）
      │   ├─ splitQueue        ← 依赖 db（发送 AdminSplit）
      │   ├─ replicateQueue    ← 依赖 allocator（副本决策）
      │   ├─ replicaGCQueue    ← 依赖 db（移除 Replica）
      │   ├─ mmaStoreRebalancer ← 依赖 MMAllocator
      │   ├─ raftLogQueue      ← 独立（日志截断）
      │   ├─ raftSnapshotQueue ← 独立（快照检测）
      │   └─ consistencyQueue  ← 独立（一致性检查）
      └─ scanner.AddQueues(所有队列)  ← 注册到扫描器

运行时协作：
  replicaScanner（每 10 分钟一轮）
      ├─ 遍历所有 Replica
      ├─ 对每个 Replica 调用所有队列的 shouldQueue()
      └─ 根据优先级将 Replica 加入对应队列

  baseQueue（每个队列独立处理）
      ├─ 从 priorityQueue 弹出最高优先级 Replica
      ├─ 获取 Lease（如果 needsLease=true）
      ├─ 调用具体队列的 process()
      └─ 如果失败 → 放入 purgatory（等待特定事件）
```

**上游依赖**：
- **replicaScanner**：统一扫描触发 `shouldQueue`
- **Gossip**：获取集群状态（如节点 liveness）
- **SpanConfig**：获取 Zone Config（如副本数、约束条件）
- **Allocator**：决策引擎（Lease 转移目标、副本添加/移除位置）

**下游消费**：
- **Raft**：通过 `AdminSplit`、`AdminMerge`、`AdminChangeReplicas` 等命令修改 Range
- **Storage Engine**：执行 MVCC GC、日志截断
- **Gossip**：广播 Store 状态变化

### 1.5 核心状态与生命周期

#### 1.5.1 replicaItem 状态机

```
┌──────────────┐
│  Not Exists  │ (Replica 未入队)
└──────┬───────┘
       │ addInternal()
       ↓
┌──────────────┐
│   Queued     │ (在 priorityQ 中等待)
└──────┬───────┘
       │ processReplica()
       ↓
┌──────────────┐
│  Processing  │ (正在执行 process())
└──────┬───────┘
       │
       ├─ 成功 → 移除
       │
       ├─ 失败（PurgatoryError）
       │   ↓
       │ ┌──────────────┐
       │ │  Purgatory   │ (等待特定事件)
       │ └──────┬───────┘
       │        │ purgatoryChan 触发
       │        ↓
       └────→ 重新入队
```

**关键状态字段**：
```go
type replicaItem struct {
    rangeID   roachpb.RangeID
    replicaID roachpb.ReplicaID
    seq       int       // FIFO 顺序（优先级相同时）
    priority  float64   // 优先级（越大越先处理）
    index     int       // 在堆中的位置
    processing bool     // 是否正在处理
    requeue    bool     // 处理完后是否重新入队
    callbacks  []processCallback // 回调函数
}
```

#### 1.5.2 baseQueue 长期状态

```go
type baseQueue struct {
    mu struct {
        replicas  map[roachpb.RangeID]*replicaItem // 所有在队列中的 Replica
        priorityQ priorityQueue                      // 优先级堆
        purgatory map[roachpb.RangeID]PurgatoryError // 失败重试区
        disabled  bool                                // 队列是否禁用
        stopped   bool                                // 队列是否停止
    }

    processSem chan struct{}  // 控制并发度（maxConcurrency）
    addOrMaybeAddSem *quotapool.IntPool // 控制入队并发
    incoming   chan struct{}  // 新 Replica 入队信号
}
```

**生命周期**：
1. **初始化**：`NewStore` 中创建所有队列
2. **启动**：`scanner.Start()` 启动扫描循环和队列处理循环
3. **运行时**：
   - Scanner 每 10 分钟扫描一次
   - 每个队列独立处理，从 priorityQ 弹出最高优先级 Replica
   - 失败的 Replica 进入 purgatory
4. **停止**：`stopper.Stop()` 停止所有队列

### 1.6 为什么需要 10 个不同的队列？

**核心答案**：不同维护操作的 **约束条件、失败模式、优先级策略** 完全不同，无法用一个队列实现。

**对比表**：

| 特性 | leaseQueue | mvccGCQueue | replicateQueue | splitQueue |
|------|-----------|------------|----------------|-----------|
| **需要 Lease?** | ✅ 必须 | ❌ 不需要 | ✅ 必须 | ✅ 必须 |
| **需要 SpanConfig?** | ✅ 需要 | ❌ 不需要 | ✅ 需要 | ✅ 需要 |
| **接受未分裂 Range?** | ❌ 拒绝 | ✅ 接受 | ❌ 拒绝 | ✅ 接受 |
| **处理已销毁 Replica?** | ❌ 拒绝 | ✅ 接受 | ❌ 拒绝 | ❌ 拒绝 |
| **最大并发度** | 1 | 1 | 1 | 4 |
| **失败重试策略** | Purgatory (10s) | 无 | Purgatory (1min) | Purgatory (1min) |
| **定时器间隔** | 0（贪婪处理）| 1s | 0（贪婪处理）| 0（贪婪处理）|

**为什么配置不同？**

1. **leaseQueue `needsLease=true`**：
   - 只有 Leaseholder 才有权限发起 Lease 转移
   - 如果允许 Follower 操作，会导致 Lease 冲突

2. **splitQueue `acceptsUnsplitRanges=true`**：
   - Split Queue 的职责就是分裂，当然要接受未分裂的 Range
   - 如果拒绝，Split 永远不会发生

3. **mvccGCQueue `needsLease=false`**：
   - 每个 Replica 可以独立 GC 自己的本地数据
   - 不需要协调，提高并发度

4. **replicateQueue `maxConcurrency=1`**：
   - 副本变更可能涉及发送 Snapshot（几十 MB 到几 GB）
   - 串行处理避免网络流量峰值

### 1.7 总结：为什么需要队列系统

**核心答案**：
1. **避免重复扫描**：10 个队列共享一个 Scanner，将 100,000 次检查降低到 10,000 次
2. **统一优先级控制**：紧急操作（under-replicated）可以抢占低优先级任务
3. **统一失败重试**：Purgatory 机制避免无效重试，等待特定事件再触发
4. **统一并发控制**：`processSem` 和 `addOrMaybeAddSem` 限制资源消耗
5. **策略可插拔**：每个队列只需实现 `shouldQueue` 和 `process`，基础设施全部复用

**如果没有队列系统**：
- 10 个独立的扫描循环 → **10 倍 CPU 开销**
- 无优先级控制 → **紧急操作延迟数分钟**
- 无去重机制 → **同一个 Replica 被重复处理**
- 无失败重试 → **无效重试导致 CPU 浪费**

---

## 二、第二轮 BFS：控制流与组件协作（How it flows）

### 2.1 整体执行路径

```
时间线视角：

T0: Store 启动
  └─ NewStore(...)
      ├─ 创建 replicaScanner
      ├─ 创建 10 个队列（每个队列调用 newBaseQueue）
      └─ scanner.AddQueues(所有队列)

T1: Store.Start() 调用 scanner.Start()
  ├─ 启动 scanner.scanLoop()  (goroutine 1)
  └─ 启动 10 个 queue.Start() (goroutine 2-11)

T2: Scanner 开始第一轮扫描
  └─ scanLoop() {
      for _, repl := range allReplicas {
          for _, queue := range queues {
              queue.MaybeAdd(repl) // 并发调用
          }
          time.Sleep(paceInterval) // 自适应节奏
      }
  }

T3: leaseQueue 收到第一个 Replica
  └─ MaybeAdd(repl)
      ├─ replicaCanBeProcessed(repl, acquireLeaseIfNeeded=false)
      │   ├─ 检查 Replica 是否初始化
      │   ├─ 检查是否需要 SpanConfig（如果 needsSpanConfigs=true）
      │   └─ 检查是否需要 Split（如果 acceptsUnsplitRanges=false）
      ├─ shouldQueue(repl) → (should=true, priority=1.5)
      └─ addInternal(repl, priority=1.5)
          ├─ 检查 mu.purgatory（如果在 purgatory，拒绝）
          ├─ 检查 mu.replicas（如果已存在，更新优先级）
          └─ heap.Push(mu.priorityQ, replicaItem)

T4: leaseQueue 的 processLoop 从堆中取出 Replica
  └─ processLoop() {
      select {
      case <-incoming:
          item := heap.Pop(mu.priorityQ)
          processSem <- struct{}{} // 获取并发槽
          go processReplica(item)
      case <-timer:
          // 定期检查
      case <-purgatoryChan:
          // 处理 purgatory 中的所有 Replica
      }
  }

T5: 异步处理 Replica
  └─ processReplica(item)
      ├─ getReplica(item.rangeID) → 重新获取最新的 Replica 对象
      ├─ replicaCanBeProcessed(repl, acquireLeaseIfNeeded=true)
      │   ├─ 如果 needsLease=true，尝试获取 Lease
      │   └─ 如果获取失败，返回错误
      ├─ process(repl) → 调用具体队列的实现
      │   └─ leaseQueue.process(repl)
      │       ├─ 调用 allocator.PlanOneChange() 计算目标节点
      │       ├─ repl.AdminTransferLease(target)
      │       └─ return (processed=true, err=nil)
      ├─ 处理结果：
      │   ├─ 如果成功 → removeLocked(item)
      │   ├─ 如果失败且是 PurgatoryError → addToPurgatory(item)
      │   └─ 如果 item.requeue=true → 重新入队
      └─ <-processSem // 释放并发槽
```

### 2.2 触发方式矩阵

#### 2.2.1 周期性扫描触发（Periodic Scanner）

**触发者**：`replicaScanner.scanLoop()`

**流程**：
```go
func (rs *replicaScanner) scanLoop() {
    for {
        start := timeutil.Now()
        count := 0

        // 遍历所有 Replica
        rs.replicas.Visit(func(repl *Replica) bool {
            now := rs.clock.NowAsClockTimestamp()

            // 对每个 Replica 调用所有队列的 MaybeAdd
            for _, queue := range rs.queues {
                queue.MaybeAdd(ctx, repl, now)
            }

            count++
            interval := rs.paceInterval(start, timeutil.Now())
            time.Sleep(interval) // 自适应节奏控制
            return true // 继续遍历
        })

        // 一轮扫描完成
        rs.mu.Lock()
        rs.mu.scanCount++
        rs.mu.Unlock()
    }
}
```

**特点**：
- **全量扫描**：每轮扫描所有 Replica
- **自适应节奏**：根据 `targetInterval`（默认 10 分钟）动态调整扫描间隔
- **并发调用**：每个 Replica 依次调用所有队列的 `MaybeAdd`
- **非阻塞**：`MaybeAdd` 内部使用 `addOrMaybeAddSem` 限制并发，超过限制时静默丢弃

#### 2.2.2 事件驱动触发（Event-Driven）

**触发场景**：

1. **Raft 日志增长**：
```go
// 在 Replica.handleRaftReadyRaftMuLocked 中
if raftLogSize > threshold {
    s.raftLogQueue.AddAsync(ctx, repl, priority)
}
```

2. **MVCC Stats 变化**：
```go
// 在 Replica.updateRangeInfo 中
if gcScore > threshold {
    s.mvccGCQueue.AddAsync(ctx, repl, priority)
}
```

3. **Span Config 更新**：
```go
// 在 Store.onSpanConfigUpdate 中
affectedReplicas := findAffectedReplicas(update)
for _, repl := range affectedReplicas {
    s.replicateQueue.AddAsync(ctx, repl, priority)
}
```

4. **Replica 被移除（Rebalancing）**：
```go
// 在 Replica.changeReplicasImpl 中
if isRemoved(repl) {
    s.replicaGCQueue.AddAsync(ctx, repl, replicaGCPriorityRemoved)
}
```

**特点**：
- **按需触发**：只处理发生变化的 Replica
- **高优先级**：事件驱动的优先级通常高于扫描发现的
- **可能阻塞**：`AddAsync` 会等待 `addOrMaybeAddSem`，而 `MaybeAddAsync` 不会

#### 2.2.3 Purgatory 触发（Retry on Event）

**触发场景**：某个 Replica 处理失败，返回 `PurgatoryError`

**流程**：
```go
// 在 processReplica 中
err := bq.impl.process(ctx, repl, confReader, priority)
if errors.HasType(err, (*PurgatoryError)(nil)) {
    bq.addToPurgatory(item, err)
}

// Purgatory 触发
func (bq *baseQueue) processLoop(stopper *stop.Stopper) {
    purgatoryChan := bq.impl.purgatoryChan()

    for {
        select {
        case <-purgatoryChan: // 定时器或外部事件
            bq.mu.Lock()
            purgatoryReplicas := bq.mu.purgatory
            bq.mu.purgatory = make(map[roachpb.RangeID]PurgatoryError)
            bq.mu.Unlock()

            // 将所有 purgatory 中的 Replica 重新入队
            for rangeID := range purgatoryReplicas {
                bq.AddAsync(ctx, rangeID, priority)
            }
        }
    }
}
```

**Purgatory 定时器配置**：

| 队列 | Purgatory 间隔 | 触发事件 |
|------|---------------|---------|
| leaseQueue | 10 秒 | Ticker |
| replicateQueue | 1 分钟 | Ticker + Gossip 更新 |
| splitQueue | 1 分钟 | Ticker |
| mergeQueue | 1 分钟 | Ticker |

**示例：replicateQueue 的 Purgatory**
```go
// Replica 无法添加副本（目标节点不可用）
err := repl.ChangeReplicas(ctx, addReplica(store5))
// 返回 PurgatoryError: "store 5 not available"

// 进入 purgatory
bq.mu.purgatory[rangeID] = err

// 1 分钟后或 Gossip 更新时重试
select {
case <-time.NewTicker(1 * time.Minute).C:
    retryAllPurgatoryReplicas()
case <-gossipUpdateChan:
    retryAllPurgatoryReplicas()
}
```

### 2.3 与其他模块的交互

#### 2.3.1 与 replicaScanner 的协作

**Scanner 的职责**：
- 统一遍历所有 Replica
- 调用所有队列的 `MaybeAdd`
- 控制扫描节奏（避免 CPU 峰值）

**队列的职责**：
- 判断是否需要处理（`shouldQueue`）
- 维护优先级队列
- 异步处理 Replica

**关键接口**：
```go
type replicaQueue interface {
    MaybeAdd(ctx context.Context, repl replicaInQueue, now hlc.ClockTimestamp)
    Start(stopper *stop.Stopper)
    Name() string
    NeedsLease() bool
}
```

**时序图**：
```
replicaScanner                baseQueue                  queueImpl
     |                             |                          |
     |--MaybeAdd(repl, now)------->|                          |
     |                             |--shouldQueue(repl)------>|
     |                             |<-----(should, priority)--|
     |                             |                          |
     |                             |--addInternal(repl)       |
     |                             |  (加入 priorityQ)        |
     |<-----------------------------|                          |
     |                             |                          |
     | (继续扫描下一个 Replica)     |                          |
     |                             |                          |
     |                             |--processLoop()           |
     |                             |  (异步处理)              |
     |                             |--process(repl)---------->|
     |                             |<-----(processed, err)----|
```

#### 2.3.2 与 Allocator 的协作

**Allocator 的职责**：
- 计算 Replica 应该添加/移除到哪个 Store
- 计算 Lease 应该转移到哪个 Store
- 考虑约束条件（constraints）、负载均衡、节点 liveness

**队列的职责**：
- 检测需要变更（`shouldQueue`）
- 执行 Allocator 的决策（`process`）

**关键流程（以 leaseQueue 为例）**：
```go
func (lq *leaseQueue) process(ctx context.Context, repl *Replica, ...) (bool, error) {
    // 1. 调用 Allocator 计算目标
    change, err := lq.planner.PlanOneChange(ctx, repl, desc, conf, options)
    if err != nil {
        return false, err
    }

    // 2. 执行决策
    if transferOp, ok := change.Op.(plan.AllocationTransferLeaseOp); ok {
        err = repl.AdminTransferLease(ctx, transferOp.Target.StoreID, false)
        return err == nil, err
    }

    return false, nil
}
```

**Allocator 决策示例**：
```
输入：
  - Range r1234，当前 Leaseholder=Store1
  - conf.LeasePreferences=[{Constraints: [region=us-west]}]
  - Store1 在 region=us-east, Store2 在 region=us-west

Allocator 决策：
  - 应该将 Lease 转移到 Store2（满足 LeasePreferences）
  - 优先级=1.0

leaseQueue 执行：
  - repl.AdminTransferLease(Store2)
```

#### 2.3.3 与 Raft 的协作

**Raft 的职责**：
- 执行 Admin 命令（Split、Merge、ChangeReplicas）
- 复制日志到 Follower
- 发送 Snapshot

**队列的职责**：
- 检测需要执行 Admin 命令
- 通过 `db.AdminSplit` 等接口发起操作

**关键流程（以 splitQueue 为例）**：
```go
func (sq *splitQueue) process(ctx context.Context, repl *Replica, ...) (bool, error) {
    // 1. 查找分裂点
    splitKey := findSplitKey(repl)
    if splitKey == nil {
        return false, nil
    }

    // 2. 发起 AdminSplit 请求
    err := sq.db.AdminSplit(ctx, splitKey, hlc.MaxTimestamp)
    return err == nil, err
}

// AdminSplit 最终会调用 Raft
func (db *DB) AdminSplit(...) error {
    // 构造 AdminSplitRequest
    req := &kvpb.AdminSplitRequest{
        RequestHeader: kvpb.RequestHeader{Key: splitKey},
        SplitKey:      splitKey,
    }

    // 通过 Raft 复制
    return db.sendAdminRequest(ctx, req)
}
```

#### 2.3.4 共享状态管理

**问题**：多个队列可能同时操作同一个 Replica，如何避免冲突？

**解决方案 1：Allocator Token**
```go
// 在 leaseQueue.process 中
if tokenErr := repl.allocatorToken.TryAcquire(ctx, lq.name); tokenErr != nil {
    return false, tokenErr
}
defer repl.allocatorToken.Release(ctx)
```

**作用**：
- 确保同一时刻只有一个队列在修改 Replica 的副本配置
- 避免 leaseQueue 和 replicateQueue 同时操作

**解决方案 2：Lease 要求**
```go
// 在 replicaCanBeProcessed 中
if cfg.needsLease {
    status, pErr := repl.redirectOnOrAcquireLease(ctx)
    if pErr != nil || !status.Lease.OwnedBy(repl.StoreID()) {
        return nil, errors.New("not leaseholder")
    }
}
```

**作用**：
- 确保只有 Leaseholder 发起 Range 级操作
- Follower 的队列会因为 `needsLease=true` 而跳过

**解决方案 3：去重**
```go
// 在 addInternal 中
item, ok := bq.mu.replicas[desc.RangeID]
if ok {
    // Replica 已在队列中
    if item.processing {
        item.requeue = true // 标记为需要重新入队
        return false, nil
    }
    // 更新优先级
    if priority > item.priority {
        bq.mu.priorityQ.update(item, priority)
    }
    return false, nil
}
```

**作用**：
- 同一个 Replica 在队列中只有一个副本
- 如果正在处理，标记为 `requeue`，处理完后重新入队

### 2.4 状态流转图

#### 2.4.1 正常流程（Happy Path）

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Scanner 发现 Replica 需要处理                                 │
│    shouldQueue(repl) → (should=true, priority=1.5)              │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. 加入优先级队列                                                │
│    heap.Push(priorityQ, replicaItem{priority=1.5})             │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. processLoop 从堆中取出                                        │
│    item := heap.Pop(priorityQ)  (最高优先级)                    │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. 获取并发槽                                                    │
│    processSem <- struct{}  (如果槽已满，阻塞)                   │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. 重新检查前置条件                                              │
│    replicaCanBeProcessed(repl, acquireLeaseIfNeeded=true)      │
│    - 检查 Replica 是否仍然存在                                   │
│    - 获取 Lease（如果 needsLease=true）                         │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 6. 调用具体队列的 process                                        │
│    processed, err := impl.process(ctx, repl, confReader)       │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 7. 成功处理                                                      │
│    removeLocked(item)                                           │
│    <-processSem  (释放并发槽)                                   │
└─────────────────────────────────────────────────────────────────┘
```

#### 2.4.2 失败流程（Purgatory Path）

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. process 返回 PurgatoryError                                  │
│    err = "target store 5 not available"                         │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. 加入 purgatory                                                │
│    mu.purgatory[rangeID] = err                                  │
│    removeLocked(item)  (从 priorityQ 移除)                      │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. 等待触发事件                                                  │
│    - 定时器 (如 1 分钟)                                          │
│    - Gossip 更新 (节点状态变化)                                  │
│    - Span Config 更新                                           │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. purgatoryChan 触发                                            │
│    将所有 purgatory 中的 Replica 重新入队                        │
│    for rangeID := range purgatory {                             │
│        AddAsync(rangeID, priority)                              │
│    }                                                            │
└─────────────────────────────────────────────────────────────────┘
```

#### 2.4.3 重新入队流程（Requeue Path）

```
场景：Replica 正在处理时，Scanner 又发现需要处理

┌─────────────────────────────────────────────────────────────────┐
│ 1. Scanner 调用 MaybeAdd                                         │
│    addInternal(repl, priority=2.0)                              │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. 检查 mu.replicas                                              │
│    item, ok := mu.replicas[rangeID]                             │
│    if ok && item.processing {                                   │
│        item.requeue = true  // 标记为需要重新入队                │
│        return               // 不重复加入                        │
│    }                                                            │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. processReplica 完成后检查 requeue 标志                        │
│    if item.requeue {                                            │
│        heap.Push(priorityQ, item)  // 重新入队                  │
│    } else {                                                     │
│        removeLocked(item)          // 移除                      │
│    }                                                            │
└─────────────────────────────────────────────────────────────────┘
```

### 2.5 并发控制机制

#### 2.5.1 processSem（处理并发度控制）

```go
// 在 newBaseQueue 中
bq.processSem = make(chan struct{}, cfg.maxConcurrency)

// 在 processLoop 中
select {
case <-incoming:
    item := heap.Pop(mu.priorityQ)
    bq.processSem <- struct{}{} // 获取槽位（阻塞）
    go func() {
        defer func() { <-bq.processSem }() // 释放槽位
        bq.processReplica(item)
    }()
}
```

**并发度配置**：

| 队列 | maxConcurrency | 原因 |
|------|---------------|------|
| leaseQueue | 1 | Lease 转移需要协调，串行避免冲突 |
| replicateQueue | 1 | Snapshot 可能很大，串行避免流量峰值 |
| splitQueue | 4 | Split 主要是 CPU 密集（RocksDB 扫描），允许并发 |
| mergeQueue | 1 | Merge 涉及复杂的副本 Rebalance，串行处理 |
| mvccGCQueue | 1 | GC 涉及大量 IO，串行避免磁盘压力 |

#### 2.5.2 addOrMaybeAddSem（入队并发度控制）

```go
// 在 newBaseQueue 中
bq.addOrMaybeAddSem = quotapool.NewIntPool("queue-add", uint64(cfg.addOrMaybeAddSemSize))

// 在 MaybeAdd 中
alloc, err := bq.addOrMaybeAddSem.TryAcquire(ctx, 1)
if err != nil {
    // 超过并发限制，静默丢弃
    if bq.addLogN.ShouldLog() {
        log.Warningf(ctx, "unable to acquire add semaphore: %v", err)
    }
    return
}
defer alloc.Release()

// 执行 shouldQueue 和 addInternal
```

**作用**：
- 限制同时执行 `shouldQueue` 的并发度（默认 20）
- `shouldQueue` 可能需要读取 Span Config、计算 GC Score 等，避免 CPU 峰值
- `MaybeAdd` 使用 `TryAcquire`（非阻塞），超过限制时静默丢弃
- `AddAsync` 使用 `Acquire`（阻塞），确保一定入队

#### 2.5.3 mu.replicas 去重

```go
// 在 addInternal 中
bq.mu.Lock()
defer bq.mu.Unlock()

item, ok := bq.mu.replicas[desc.RangeID]
if ok {
    // Replica 已在队列中，只更新优先级
    if priority > item.priority {
        bq.mu.priorityQ.update(item, priority)
    }
    return false, nil
}

// 创建新 item
item = &replicaItem{rangeID: desc.RangeID, priority: priority}
bq.mu.replicas[desc.RangeID] = item
heap.Push(&bq.mu.priorityQ, item)
```

**作用**：
- 确保每个 Replica 在队列中只有一个副本
- 避免重复处理
- 支持优先级更新（如发现更紧急的情况）

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 baseQueue.MaybeAdd() - 入队决策函数

**函数签名**：
```go
func (bq *baseQueue) maybeAdd(
    ctx context.Context,
    repl replicaInQueue,
    now hlc.ClockTimestamp,
)
```

**职责**：
- 判断 Replica 是否应该加入队列
- 如果应该，计算优先级并加入

**执行流程**：

```go
func (bq *baseQueue) maybeAdd(ctx context.Context, repl replicaInQueue, now hlc.ClockTimestamp) {
    // ────────────────────────────────────────────────────────────────
    // 步骤 1: 限流检查
    // ────────────────────────────────────────────────────────────────
    // 使用 TryAcquire（非阻塞），超过并发限制时静默丢弃
    alloc, err := bq.addOrMaybeAddSem.TryAcquire(ctx, 1)
    if err != nil {
        // 并发度已满，丢弃该 Replica
        // 注意：这不是错误，而是流控机制
        // Scanner 下一轮还会再次尝试
        if bq.addLogN.ShouldLog() {
            log.Warningf(ctx, "unable to acquire add semaphore for %s", repl)
        }
        return
    }
    defer alloc.Release()

    // ────────────────────────────────────────────────────────────────
    // 步骤 2: 前置条件检查
    // ────────────────────────────────────────────────────────────────
    // 关键参数：acquireLeaseIfNeeded=false
    // 为什么是 false？
    //   - 入队时不应该获取 Lease，否则：
    //     1. 可能导致无谓的 Lease 竞争
    //     2. 扫描速度会变慢（等待 Lease 获取）
    //     3. 最终可能不需要处理该 Replica
    //   - 只检查是否已经持有 Lease，不主动获取
    confReader, err := bq.replicaCanBeProcessed(ctx, repl, false /* acquireLeaseIfNeeded */)
    if err != nil {
        // 不满足前置条件，不入队
        // 常见原因：
        //   - Replica 未初始化
        //   - Span Config 不可用
        //   - 需要 Lease 但当前不是 Leaseholder
        //   - Range 需要 Split（如果 acceptsUnsplitRanges=false）
        bq.updateMetricsOnEnqueueFailedPrecondition()
        return
    }

    // ────────────────────────────────────────────────────────────────
    // 步骤 3: 调用具体队列的 shouldQueue
    // ────────────────────────────────────────────────────────────────
    realRepl, _ := repl.(*Replica)
    should, priority := bq.impl.shouldQueue(ctx, now, realRepl, confReader)
    if !should {
        // 该队列不需要处理该 Replica
        bq.updateMetricsOnEnqueueNoAction()
        return
    }

    // ────────────────────────────────────────────────────────────────
    // 步骤 4: 检查是否有外部文件（External SSTs）
    // ────────────────────────────────────────────────────────────────
    // 某些队列（如 mergeQueue）配置了 skipIfReplicaHasExternalFilesConfig
    // 原因：
    //   - External SSTs 是通过 BACKUP/RESTORE 导入的大文件
    //   - Merge 这些 Range 可能导致性能问题
    //   - 等待 Compaction 完成后再 Merge
    extConf := bq.skipIfReplicaHasExternalFilesConfig
    if extConf != nil && extConf.Get(&bq.store.cfg.Settings.SV) {
        hasExternal, err := realRepl.HasExternalBytes()
        if err != nil {
            bq.updateMetricsOnEnqueueUnexpectedError()
            log.Warningf(ctx, "could not determine if %s has external bytes: %s", realRepl, err)
            return
        }
        if hasExternal {
            log.VInfof(ctx, 1, "skipping %s for %s because it has external bytes", bq.name, realRepl)
            return
        }
    }

    // ────────────────────────────────────────────────────────────────
    // 步骤 5: 加入队列
    // ────────────────────────────────────────────────────────────────
    _, err = bq.addInternal(ctx, repl.Desc(), repl.ReplicaID(), priority, noopProcessCallback)
    if !isExpectedQueueError(err) {
        log.Errorf(ctx, "unable to add %s to %s: %+v", repl, bq.name, err)
    }
}
```

**不变量（Invariants）**：
1. **入队时不获取 Lease**：`acquireLeaseIfNeeded=false`，避免扫描期间的 Lease 竞争
2. **幂等性**：同一个 Replica 多次调用 `MaybeAdd` 只会更新优先级，不会重复入队
3. **非阻塞**：使用 `TryAcquire`，超过并发限制时静默丢弃（下一轮扫描会重试）
4. **优先级单调性**：如果新优先级更高，更新堆中的位置

**为什么这么设计？**

1. **为什么入队时不获取 Lease？**
   - Scanner 遍历所有 Replica，如果每个都尝试获取 Lease：
     - 非 Leaseholder 会发起 `RequestLease` RPC（昂贵）
     - 可能触发不必要的 Lease 转移
     - 扫描速度变慢（需要等待 RPC）
   - 正确策略：
     - 入队时：只检查是否已持有 Lease
     - 处理时：如果需要 Lease 且未持有，再尝试获取

2. **为什么使用 TryAcquire 而不是 Acquire？**
   - `TryAcquire`（非阻塞）：
     - Scanner 每 10 分钟扫描一次，偶尔丢弃一个 Replica 可接受
     - 避免扫描协程被阻塞
   - `AddAsync`（阻塞）：
     - 事件驱动触发（如 under-replicated），必须入队
     - 使用 `Acquire`，即使需要等待

### 3.2 baseQueue.replicaCanBeProcessed() - 前置条件检查

**函数签名**：
```go
func (bq *baseQueue) replicaCanBeProcessed(
    ctx context.Context,
    repl replicaInQueue,
    acquireLeaseIfNeeded bool,
) (spanconfig.StoreReader, error)
```

**职责**：
- 检查 Replica 是否满足队列的处理要求
- 如果需要 Lease，可选地获取 Lease

**执行流程**：

```go
func (bq *baseQueue) replicaCanBeProcessed(
    ctx context.Context,
    repl replicaInQueue,
    acquireLeaseIfNeeded bool,
) (spanconfig.StoreReader, error) {
    // ────────────────────────────────────────────────────────────────
    // 检查 1: Replica 是否初始化
    // ────────────────────────────────────────────────────────────────
    if !repl.IsInitialized() {
        // 未初始化的 Replica 不能处理
        // 原因：
        //   - Range Descriptor 未就绪
        //   - 可能正在接收 Snapshot
        return nil, errReplicaNotInitialized
    }

    // ────────────────────────────────────────────────────────────────
    // 检查 2: Replica 是否已销毁
    // ────────────────────────────────────────────────────────────────
    if destroyReason, err := repl.IsDestroyed(); err != nil {
        // 该 Replica 已被销毁
        if !bq.processDestroyedReplicas {
            // 大多数队列不处理已销毁的 Replica
            return nil, err
        }
        // replicaGCQueue 和 mvccGCQueue 可以处理
        // 例如：clearRangeData 需要在 Replica 销毁后执行
    }

    // ────────────────────────────────────────────────────────────────
    // 检查 3: 获取 Span Config
    // ────────────────────────────────────────────────────────────────
    var confReader spanconfig.StoreReader
    if bq.needsSpanConfigs {
        confReader = bq.store.GetStoreConfig().SpanConfigSubscriber
        if confReader == nil {
            // Span Config 系统未就绪
            // 原因：
            //   - 节点刚启动
            //   - Span Config Range 不可用
            return nil, errors.New("span config not available")
        }
    }

    // ────────────────────────────────────────────────────────────────
    // 检查 4: 检查是否需要 Split
    // ────────────────────────────────────────────────────────────────
    if bq.needsSpanConfigs && !bq.acceptsUnsplitRanges {
        // 检查 Range 是否跨越多个 Span Config
        needsSplit, err := repl.NeedsSplit(ctx, confReader)
        if err != nil {
            return nil, err
        }
        if needsSplit {
            // 该 Range 需要先 Split
            // 例如：跨越 Table boundary
            //       /Table/51/1 -> /Table/52/1
            //       应该先分裂为：
            //       /Table/51/1 -> /Table/52
            //       /Table/52 -> /Table/52/1
            return nil, errors.Newf("range needs split")
        }
    }

    // ────────────────────────────────────────────────────────────────
    // 检查 5: 检查或获取 Lease
    // ────────────────────────────────────────────────────────────────
    if bq.needsLease {
        if acquireLeaseIfNeeded {
            // 处理阶段：如果需要 Lease 且未持有，尝试获取
            status, pErr := repl.redirectOnOrAcquireLease(ctx)
            if pErr != nil {
                // 无法获取 Lease
                // 常见原因：
                //   - 该 Replica 正在被移除
                //   - 其他节点持有 Lease 且不愿转移
                //   - 网络分区
                return nil, pErr.GoError()
            }
            if !status.Lease.OwnedBy(repl.StoreID()) {
                // Lease 在其他节点上
                return nil, errors.Newf("not leaseholder")
            }
        } else {
            // 入队阶段：只检查是否已持有 Lease
            status := repl.CurrentLeaseStatus(ctx)
            if !status.Lease.OwnedBy(repl.StoreID()) {
                // 当前不是 Leaseholder，不入队
                // 注意：不尝试获取 Lease（避免扫描期间的 Lease 竞争）
                return nil, errors.Newf("not leaseholder")
            }
        }
    }

    return confReader, nil
}
```

**不变量**：
1. **双阶段检查**：
   - 入队时（`acquireLeaseIfNeeded=false`）：只检查，不获取
   - 处理时（`acquireLeaseIfNeeded=true`）：如果需要，尝试获取
2. **严格顺序**：初始化 → 未销毁 → Span Config → Split → Lease
3. **快速失败**：任一条件不满足，立即返回错误

**为什么分两个阶段？**

- **入队阶段**：
  - 目标：快速筛选，避免阻塞 Scanner
  - 策略：只检查当前状态，不发起 RPC
  - 结果：可能入队后发现不满足条件（Lease 已转移）

- **处理阶段**：
  - 目标：确保满足所有条件
  - 策略：主动获取 Lease（如果需要）
  - 结果：可能因获取 Lease 失败而进入 purgatory

### 3.3 baseQueue.addInternal() - 入队核心逻辑

**函数签名**：
```go
func (bq *baseQueue) addInternal(
    ctx context.Context,
    desc *roachpb.RangeDescriptor,
    replicaID roachpb.ReplicaID,
    priority float64,
    cb processCallback,
) (added bool, err error)
```

**职责**：
- 将 Replica 加入优先级队列
- 处理去重、优先级更新、purgatory 检查

**执行流程**：

```go
func (bq *baseQueue) addInternal(
    ctx context.Context,
    desc *roachpb.RangeDescriptor,
    replicaID roachpb.ReplicaID,
    priority float64,
    cb processCallback,
) (added bool, err error) {
    // ────────────────────────────────────────────────────────────────
    // 步骤 1: 检查 Replica 是否初始化
    // ────────────────────────────────────────────────────────────────
    if !desc.IsInitialized() {
        cb.onEnqueueResult(-1, errReplicaNotInitialized)
        return false, errReplicaNotInitialized
    }

    bq.mu.Lock()
    defer bq.mu.Unlock()

    // ────────────────────────────────────────────────────────────────
    // 步骤 2: 检查队列是否停止或禁用
    // ────────────────────────────────────────────────────────────────
    if bq.mu.stopped {
        cb.onEnqueueResult(-1, errQueueStopped)
        return false, errQueueStopped
    }
    if bq.mu.disabled {
        // 测试时可以通过 TestingKnobs 绕过
        bypassDisabled := bq.store.TestingKnobs().BaseQueueDisabledBypassFilter
        if bypassDisabled == nil || !bypassDisabled(desc.RangeID) {
            cb.onEnqueueResult(-1, errQueueDisabled)
            return false, errQueueDisabled
        }
    }

    // ────────────────────────────────────────────────────────────────
    // 步骤 3: 检查是否在 purgatory 中
    // ────────────────────────────────────────────────────────────────
    if _, ok := bq.mu.purgatory[desc.RangeID]; ok {
        // 该 Replica 之前处理失败，正在 purgatory 中等待
        // 不重新入队，等待 purgatoryChan 触发
        cb.onEnqueueResult(-1, errReplicaAlreadyInPurgatory)
        return false, nil // 注意：返回 nil 而不是错误
    }

    // ────────────────────────────────────────────────────────────────
    // 步骤 4: 检查是否已在队列中
    // ────────────────────────────────────────────────────────────────
    item, ok := bq.mu.replicas[desc.RangeID]
    if ok {
        // 该 Replica 已在队列中
        if item.processing {
            // 正在处理，标记为需要重新入队
            wasRequeued := item.requeue
            item.requeue = true
            if !wasRequeued {
                // 首次标记为 requeue，调用回调
                cb.onEnqueueResult(-1, errReplicaAlreadyProcessing)
            }
            return false, nil
        }

        // 更新优先级
        if priority > item.priority {
            // 新优先级更高，更新堆
            bq.mu.priorityQ.update(item, priority)
            cb.onEnqueueResult(item.index, nil)
            return false, nil
        }
        // 优先级未变化，不更新
        return false, nil
    }

    // ────────────────────────────────────────────────────────────────
    // 步骤 5: 检查队列是否已满
    // ────────────────────────────────────────────────────────────────
    if int64(bq.mu.priorityQ.Len()) >= bq.mu.maxSize {
        // 队列已满（默认 10000）
        // 策略：如果新 Replica 优先级高于堆顶（最低优先级），则替换
        lastItem := bq.mu.priorityQ.sl[bq.mu.priorityQ.Len()-1]
        if priority <= lastItem.priority {
            // 新 Replica 优先级不够高，丢弃
            bq.updateMetricsOnDroppedDueToFullQueue()
            cb.onEnqueueResult(-1, errDroppedDueToFullQueueSize)
            return false, nil
        }
        // 移除最低优先级的 Replica
        bq.removeLocked(lastItem)
        // 通知被移除的 Replica 的回调
        for _, oldCb := range lastItem.callbacks {
            oldCb.onEnqueueResult(-1, errDroppedDueToFullQueueSize)
        }
    }

    // ────────────────────────────────────────────────────────────────
    // 步骤 6: 创建新 replicaItem 并加入堆
    // ────────────────────────────────────────────────────────────────
    item = &replicaItem{
        rangeID:   desc.RangeID,
        replicaID: replicaID,
        priority:  priority,
        callbacks: []processCallback{cb},
    }
    bq.mu.replicas[desc.RangeID] = item
    heap.Push(&bq.mu.priorityQ, item)

    // 通知 processLoop 有新 Replica
    select {
    case bq.incoming <- struct{}{}:
    default:
        // incoming channel 已有信号，不重复发送
    }

    // 调用回调
    cb.onEnqueueResult(item.index, nil)
    bq.updateMetricsOnEnqueueAdd()

    return true, nil
}
```

**不变量**：
1. **唯一性**：`mu.replicas[rangeID]` 确保每个 Replica 最多一个 item
2. **优先级一致性**：堆的性质 `parent.priority >= child.priority`
3. **容量限制**：`priorityQ.Len() <= maxSize`（队列满时移除最低优先级）
4. **互斥性**：Replica 不能同时在 `priorityQ` 和 `purgatory` 中

**关键设计决策**：

1. **为什么队列满时替换最低优先级？**
   - 确保高优先级任务不会因队列满而被丢弃
   - 例如：严重 under-replicated 的 Range 应该抢占低优先级的 GC

2. **为什么 purgatory 中的 Replica 不能重新入队？**
   - 避免无效重试（条件未满足时重复失败）
   - 等待特定事件（如 Gossip 更新）后统一重试

3. **为什么正在处理的 Replica 标记 requeue 而不是入队？**
   - 避免优先级队列中有重复 item
   - 处理完成后检查 `requeue` 标志，如果为 true 则重新入队

### 3.4 baseQueue.processReplica() - 处理核心逻辑

**函数签名**：
```go
func (bq *baseQueue) processReplica(
    ctx context.Context,
    item *replicaItem,
) (processed bool, err error)
```

**职责**：
- 重新获取最新的 Replica 对象
- 检查前置条件
- 调用具体队列的 `process`
- 处理失败重试（purgatory）

**执行流程**：

```go
func (bq *baseQueue) processReplica(
    ctx context.Context,
    item *replicaItem,
) (processed bool, err error) {
    // ────────────────────────────────────────────────────────────────
    // 步骤 1: 重新获取 Replica 对象
    // ────────────────────────────────────────────────────────────────
    // 为什么重新获取？
    //   - 从入队到处理可能经过几分钟
    //   - Replica 可能已被移除并重新创建（新 ReplicaID）
    //   - Replica 的状态可能已变化
    repl, err := bq.getReplica(item.rangeID)
    if err != nil {
        // Replica 不存在或已销毁
        bq.finishProcessingReplica(ctx, item, false, err)
        return false, err
    }

    // 检查 ReplicaID 是否匹配
    if repl.ReplicaID() != item.replicaID {
        // Replica 已被重新创建（新 ReplicaID）
        // 场景：
        //   1. 入队时 ReplicaID=5
        //   2. Replica 被移除（Rebalance）
        //   3. Replica 又被添加回来（ReplicaID=6）
        //   4. item.replicaID=5，但当前 Replica 是 6
        err := errors.Newf("replica ID mismatch: expected %d, got %d", item.replicaID, repl.ReplicaID())
        bq.finishProcessingReplica(ctx, item, false, err)
        return false, err
    }

    // ────────────────────────────────────────────────────────────────
    // 步骤 2: 再次检查前置条件（acquireLeaseIfNeeded=true）
    // ────────────────────────────────────────────────────────────────
    // 为什么再次检查？
    //   - 入队时 acquireLeaseIfNeeded=false（只检查）
    //   - 处理时 acquireLeaseIfNeeded=true（主动获取）
    confReader, err := bq.replicaCanBeProcessed(ctx, repl, true /* acquireLeaseIfNeeded */)
    if err != nil {
        // 无法满足前置条件
        // 常见原因：
        //   - Lease 已转移到其他节点
        //   - Span Config 不可用
        //   - Replica 需要 Split
        bq.finishProcessingReplica(ctx, item, false, err)
        return false, err
    }

    // ────────────────────────────────────────────────────────────────
    // 步骤 3: 设置处理超时
    // ────────────────────────────────────────────────────────────────
    timeout := bq.processTimeoutFunc(bq.store.cfg.Settings, repl)
    processCtx, cancel := context.WithTimeout(ctx, timeout)
    defer cancel()

    // ────────────────────────────────────────────────────────────────
    // 步骤 4: 调用具体队列的 process
    // ────────────────────────────────────────────────────────────────
    start := timeutil.Now()
    processed, err = bq.impl.process(processCtx, repl.(*Replica), confReader, item.priority)
    duration := timeutil.Since(start)

    // 更新统计
    atomic.AddInt64(&bq.processDur, int64(duration))

    // ────────────────────────────────────────────────────────────────
    // 步骤 5: 处理结果
    // ────────────────────────────────────────────────────────────────
    if err != nil {
        // 处理失败
        if errors.HasType(err, (*PurgatoryError)(nil)) {
            // 该错误类型表示可以放入 purgatory
            bq.addToPurgatory(ctx, item, err)
            return false, err
        }
        // 其他错误，直接失败
        bq.updateMetricsOnFailure()
        bq.finishProcessingReplica(ctx, item, false, err)
        return false, err
    }

    // 处理成功
    bq.updateMetricsOnSuccess()
    bq.finishProcessingReplica(ctx, item, processed, nil)
    return processed, nil
}
```

**并发语义**：
- `processReplica` 在独立的 goroutine 中执行
- 通过 `processSem` 控制并发度（最多 `maxConcurrency` 个同时执行）
- 每个 Replica 同一时刻最多被一个 goroutine 处理（通过 `item.processing` 标志）

**锁策略**：
- `getReplica`：需要 `store.mu.RLock()`（读锁）
- `replicaCanBeProcessed`：可能需要 `repl.raftMu.Lock()`（获取 Lease 时）
- `impl.process`：具体队列自行决定（通常需要 `repl.mu.Lock()`）
- `finishProcessingReplica`：需要 `bq.mu.Lock()`（更新队列状态）

**为什么这么设计？**

1. **为什么重新获取 Replica 对象？**
   - 从入队到处理可能经过几分钟，Replica 状态可能已变化
   - Replica 可能已被移除并重新创建（新 ReplicaID）
   - 确保处理的是最新状态

2. **为什么设置超时？**
   - 避免处理卡住（如 Raft 不可达）
   - 超时后自动失败，释放并发槽
   - 不同队列超时不同：
     - MVCC GC：10 分钟（需要扫描大量数据）
     - Lease Transfer：1 分钟（RPC 超时）

3. **为什么区分 processed 和 err？**
   - `processed=true, err=nil`：成功处理
   - `processed=false, err=nil`：无需处理（如已满足条件）
   - `processed=false, err!=nil`：处理失败
   - 用于指标统计（区分 "无需处理" 和 "处理失败"）

---

*由于内容较长，将在下篇继续讲解：*
- *3.5 具体队列的 shouldQueue 和 process 实现*
- *第四章：运行时行为与系统反馈*
- *第五章：设计模式分析*
- *第六章：具体运行示例*
- *第七章：设计取舍与替代方案*
- *第八章：总结与心智模型*
