# 第四十章_Store队列系统深度剖析——基于优先级与Purgatory的Replica后台维护调度引擎（续篇）

*接续下篇*

## 六、具体运行示例（Concrete Examples）

### 6.1 完整生命周期示例：Replica 从 Under-replicated 到正常

**初始状态**：

```
集群状态：
  - Store 1: r1000 (Voter), 健康
  - Store 2: r1000 (Voter), 健康
  - Store 3: 不存在
  - Range r1000 配置：num_replicas=3

问题：Range r1000 只有 2 个副本，under-replicated
```

**时间线（使用具体时间戳和状态变化）**：

```
T0: 00:00:00 - Store.Start() 启动
  └─ scanner.Start()
      ├─ 启动 scanLoop (goroutine 1)
      └─ 启动 10 个 queue.processLoop (goroutine 2-11)

T1: 00:00:10 - Scanner 第一轮扫描开始
  └─ scanLoop 遍历所有 Replica
      └─ repl=r1000, 调用所有队列的 MaybeAdd

T2: 00:00:10.001 - replicateQueue.MaybeAdd(r1000)
  ├─ replicaCanBeProcessed(r1000, acquireLeaseIfNeeded=false)
  │   ├─ 检查：Replica 已初始化 ✓
  │   ├─ 检查：SpanConfig 可用 ✓
  │   ├─ 检查：当前是 Leaseholder ✓ (Store 1)
  │   └─ 返回 confReader
  ├─ shouldQueue(r1000)
  │   ├─ desc.Replicas = [Store1, Store2]
  │   ├─ conf.NumReplicas = 3
  │   ├─ deficit = 3 - 2 = 1
  │   └─ 返回 (should=true, priority=10.0)
  └─ addInternal(r1000, priority=10.0)
      ├─ 检查 purgatory: 不存在 ✓
      ├─ 检查 mu.replicas: 不存在 ✓
      ├─ 创建 replicaItem{rangeID=1000, priority=10.0, seq=1}
      ├─ heap.Push(priorityQ, item)
      └─ incoming <- struct{}{} (通知 processLoop)

T3: 00:00:10.002 - replicateQueue.processLoop 收到信号
  ├─ select case <-incoming:
  ├─ item := heap.Pop(priorityQ) // item = r1000
  ├─ processSem <- struct{}{} // 获取槽位
  └─ go processReplica(item)

T4: 00:00:10.003 - processReplica(r1000) 开始
  ├─ getReplica(1000) → repl (最新对象)
  ├─ replicaCanBeProcessed(r1000, acquireLeaseIfNeeded=true)
  │   └─ 尝试获取 Lease → 已持有 ✓
  ├─ timeout := 1 * time.Minute
  └─ process(r1000)

T5: 00:00:10.005 - replicateQueue.process(r1000)
  ├─ repl.allocatorToken.TryAcquire() ✓
  ├─ planner.PlanOneChange()
  │   ├─ 当前副本：[Store1, Store2]
  │   ├─ 需要添加 1 个 Voter
  │   ├─ storePool.GetStoreList()
  │   │   └─ [Store1, Store2, Store3, Store4, Store5]
  │   ├─ 计算每个 Store 的适合度：
  │   │   Store3: diversity=high, capacity=50% → score=0.9
  │   │   Store4: diversity=medium, capacity=80% → score=0.6
  │   │   Store5: diversity=low, capacity=30% → score=0.5
  │   └─ 选择 Store3 (最高分)
  └─ 返回 AllocationAddOp{Target=Store3}

T6: 00:00:10.010 - 执行副本添加
  ├─ repl.ChangeReplicas(ctx, addReplica(Store3))
  │   ├─ 构造 ChangeReplicasRequest
  │   ├─ 通过 Raft 提交 (ConfChange)
  │   └─ 等待 Raft Apply
  ├─ Apply 成功后：
  │   ├─ desc.Replicas = [Store1, Store2, Store3(LEARNER)]
  │   └─ 发送 Snapshot 到 Store3
  └─ Learner 提升为 Voter：
      └─ desc.Replicas = [Store1, Store2, Store3(VOTER)]

T7: 00:00:10.150 - process 完成
  ├─ 耗时：145ms
  ├─ 返回 (processed=true, err=nil)
  └─ finishProcessingReplica(r1000)
      ├─ removeLocked(item) // 从 mu.replicas 移除
      ├─ updateMetricsOnSuccess()
      └─ <-processSem // 释放槽位

T8: 00:00:10.151 - Scanner 继续扫描下一个 Replica
  └─ repl=r1001, 调用所有队列的 MaybeAdd...

状态变化总结：
  T0-T1:   Replica 处于 under-replicated 状态
  T1-T2:   Scanner 发现问题，加入队列
  T2-T3:   在优先级队列中等待（10.0 优先级）
  T3-T7:   异步处理，添加副本到 Store3
  T7后:    Replica 恢复正常（3 个副本）
```

**关键观察点**：

1. **延迟构成**：
   - 扫描发现：10ms
   - 入队+调度：1ms
   - Allocator 决策：5ms
   - Raft Apply + Snapshot：140ms（主要耗时）

2. **为什么 priority=10.0？**
   - `deficit * 10.0 = 1 * 10.0 = 10.0`
   - 如果 deficit=2（只剩 1 个副本），priority=20.0
   - 如果 numVoters=1，priority=100.0（紧急）

3. **如果失败会怎样？**
   - 假设 Store3 不可用：
     ```
     err := repl.ChangeReplicas(ctx, addReplica(Store3))
     // 返回：&replicatePurgatoryError{msg: "store 3 unavailable"}

     addToPurgatory(r1000, err)
     // r1000 进入 purgatory，等待 Gossip 更新或 1 分钟后重试
     ```

### 6.2 压力场景示例：队列满时的行为

**初始状态**：

```
队列状态：
  - replicateQueue.maxSize = 10000
  - 当前队列大小 = 9999
  - 新 Replica r5000 需要入队，priority=5.0
  - 队列中最低优先级 = 1.0 (r9999)
```

**执行流程**：

```
T0: Scanner 调用 replicateQueue.MaybeAdd(r5000)
  └─ shouldQueue(r5000) → (true, 5.0)

T1: addInternal(r5000, priority=5.0)
  ├─ bq.mu.Lock()
  ├─ 检查队列大小：len(priorityQ) = 9999 >= maxSize(10000)
  │   └─ 队列已满！
  ├─ 查找最低优先级 Replica：
  │   lastItem := priorityQ.sl[9998] // 最后一个元素
  │   lastItem.rangeID = 9999
  │   lastItem.priority = 1.0
  ├─ 比较优先级：
  │   新 Replica priority=5.0 > lastItem.priority=1.0
  │   → 新 Replica 优先级更高，可以替换
  ├─ 移除最低优先级 Replica：
  │   removeLocked(lastItem)
  │   for _, cb := range lastItem.callbacks {
  │       cb.onEnqueueResult(-1, errDroppedDueToFullQueueSize)
  │   }
  │   updateMetricsOnDroppedDueToFullQueue()
  ├─ 加入新 Replica：
  │   item := &replicaItem{rangeID=5000, priority=5.0}
  │   heap.Push(priorityQ, item)
  └─ bq.mu.Unlock()

最终状态：
  - 队列大小：9999 (不变)
  - r9999 被移除（牺牲低优先级任务）
  - r5000 成功入队
```

**关键观察点**：

1. **为什么不扩容？**
   - 队列大小限制是保护机制
   - 避免内存无限增长
   - 强制优先级排序（高优先级抢占低优先级）

2. **被移除的 r9999 会怎样？**
   - Scanner 下一轮会再次尝试加入
   - 如果优先级仍然很低，可能再次被移除
   - 如果条件改善（如变为 under-replicated），优先级提升

3. **如果新 Replica 优先级也很低（如 0.5）？**
   ```
   if priority <= lastItem.priority {
       // 新 Replica 优先级不够高，直接丢弃
       updateMetricsOnDroppedDueToFullQueue()
       cb.onEnqueueResult(-1, errDroppedDueToFullQueueSize)
       return false, nil
   }
   ```

### 6.3 边界场景示例：Purgatory 重试

**场景**：Replica 需要添加副本到 Store5，但 Store5 暂时不可用

**时间线**：

```
T0: 00:10:00 - replicateQueue.process(r2000)
  ├─ planner.PlanOneChange() → AllocationAddOp{Target=Store5}
  ├─ repl.ChangeReplicas(ctx, addReplica(Store5))
  │   └─ 返回错误："store 5 unavailable"
  └─ errors.HasType(err, (*PurgatoryError)(nil)) → true
      └─ addToPurgatory(r2000, err)
          ├─ mu.purgatory[2000] = err
          ├─ removeLocked(item) // 从 priorityQ 移除
          └─ updateMetricsOnPurgatory()

T1: 00:10:00 - 等待状态
  队列状态：
    priorityQ: []          (r2000 已移除)
    purgatory: [r2000]     (等待重试)

  processLoop 状态：
    select {
      case <-purgatoryChan:  // time.NewTicker(1 * time.Minute).C
        // 每分钟触发一次
      case <-updateChan:      // Gossip 更新
        // 等待节点状态变化
    }

T2: 00:10:30 - Gossip 更新：Store5 恢复
  └─ replicateQueue.updateChan 收到信号
      └─ processPurgatory()
          ├─ mu.Lock()
          ├─ purgatoryReplicas := mu.purgatory
          │   └─ [r2000]
          ├─ mu.purgatory = make(map) // 清空
          ├─ mu.Unlock()
          └─ for rangeID := range purgatoryReplicas {
                AddAsync(ctx, r2000, priority)
            }

T3: 00:10:30.001 - r2000 重新入队
  ├─ shouldQueue(r2000) → (true, 10.0)
  └─ addInternal(r2000, priority=10.0)
      └─ heap.Push(priorityQ, item)

T4: 00:10:30.005 - processReplica(r2000) 第二次尝试
  ├─ process(r2000)
  │   ├─ planner.PlanOneChange() → AllocationAddOp{Target=Store5}
  │   └─ repl.ChangeReplicas(ctx, addReplica(Store5))
  │       └─ 成功！Store5 已恢复
  └─ 返回 (processed=true, err=nil)

总耗时：
  第一次尝试（T0）：失败，耗时 100ms
  等待时间（T0-T2）：30秒（等待 Gossip 更新）
  第二次尝试（T3-T4）：成功，耗时 150ms
  总计：30.25秒
```

**关键观察点**：

1. **为什么不立即重试？**
   - 立即重试会浪费 CPU（Store5 仍然不可用）
   - 等待 Gossip 更新确保有意义的重试

2. **如果 Gossip 更新很慢怎么办？**
   - purgatoryChan 每 1 分钟触发一次（兜底）
   - 即使 Gossip 不更新，1 分钟后也会重试

3. **如果第二次仍然失败？**
   - 再次进入 purgatory
   - 循环直到成功或条件改变

### 6.4 并发场景示例：多个队列同时操作同一个 Replica

**场景**：Range r3000 同时触发多个维护操作

**初始状态**：

```
Range r3000 状态：
  - 大小：550MB (超过 512MB 阈值)
  - GCBytesAge：高（大量垃圾数据）
  - 副本数：2/3 (under-replicated)
  - 当前 Leaseholder：Store1
```

**并发时间线**：

```
T0: 00:20:00 - Scanner 扫描到 r3000

T1: 00:20:00.001 - 并发调用所有队列的 MaybeAdd
  ├─ splitQueue.MaybeAdd(r3000) (goroutine A)
  │   └─ shouldQueue → (true, priority=0.074)
  │       // (550MB - 512MB) / 512MB = 0.074
  │
  ├─ mvccGCQueue.MaybeAdd(r3000) (goroutine B)
  │   └─ shouldQueue → (true, priority=2.5)
  │
  └─ replicateQueue.MaybeAdd(r3000) (goroutine C)
      └─ shouldQueue → (true, priority=10.0)
          // deficit=1, priority=1*10=10.0

T2: 00:20:00.010 - 三个队列的状态
  splitQueue.priorityQ:     [r3000(priority=0.074)]
  mvccGCQueue.priorityQ:    [r3000(priority=2.5)]
  replicateQueue.priorityQ: [r3000(priority=10.0)]

T3: 00:20:00.015 - replicateQueue 先处理（优先级最高）
  └─ replicateQueue.processReplica(r3000)
      ├─ repl.allocatorToken.TryAcquire() ✓
      ├─ process(r3000)
      │   └─ ChangeReplicas(addReplica(Store3))
      │       └─ 成功添加副本
      └─ allocatorToken.Release()

T4: 00:20:00.200 - splitQueue 尝试处理
  └─ splitQueue.processReplica(r3000)
      ├─ repl.allocatorToken.TryAcquire()
      │   └─ 失败！replicateQueue 刚释放，但锁被其他队列持有
      └─ 返回 tokenErr

T5: 00:20:00.201 - splitQueue 重新入队
  └─ finishProcessingReplica(r3000, processed=false, err=tokenErr)
      ├─ item.requeue = true
      └─ heap.Push(priorityQ, item) // 重新入队

T6: 00:20:00.500 - mvccGCQueue 尝试处理
  └─ mvccGCQueue.processReplica(r3000)
      ├─ repl.allocatorToken.TryAcquire() ✓
      │   // MVCC GC 不需要 allocatorToken（因为不修改副本配置）
      ├─ process(r3000)
      │   └─ runGC(r3000)
      │       └─ 成功清理垃圾数据
      └─ 返回 (processed=true, err=nil)

T7: 00:20:01.000 - splitQueue 第二次尝试
  └─ splitQueue.processReplica(r3000)
      ├─ repl.allocatorToken.TryAcquire() ✓
      ├─ process(r3000)
      │   └─ AdminSplit(splitKey)
      │       └─ 成功分裂为 r3000 和 r3001
      └─ 返回 (processed=true, err=nil)

最终结果：
  - r3000: 副本数 3/3 ✓，大小 275MB ✓，GC 完成 ✓
  - r3001: 新 Range，大小 275MB
```

**关键观察点**：

1. **allocatorToken 如何避免冲突？**
   - replicateQueue 和 splitQueue 都需要 allocatorToken
   - mvccGCQueue 不需要（不修改副本配置）
   - Token 确保同一时刻只有一个队列修改副本配置

2. **为什么 splitQueue 失败后重新入队？**
   - `item.requeue = true` 标记需要重新处理
   - 下次 processLoop 会再次取出并处理

3. **执行顺序为什么是 replicate → GC → split？**
   - 基于优先级：10.0 > 2.5 > 0.074
   - replicate 最紧急（影响可用性）
   - GC 其次（性能影响）
   - split 最低（只是大小优化）

---

## 七、设计取舍与替代方案（Trade-offs）

### 7.1 当前方案的优势与劣势

#### 7.1.1 优势

**1. 统一的调度框架**

**优势**：
- 代码复用：baseQueue 提供通用逻辑
- 一致性：所有队列的行为一致（前置条件检查、Purgatory、限流）
- 易于扩展：新增队列只需实现 `queueImpl` 接口

**具体收益**：
```
代码行数估算：
  - baseQueue: ~1500 行
  - 每个具体队列: ~300 行
  - 10 个队列总计: ~4500 行

如果没有 baseQueue：
  - 每个队列独立实现: ~800 行/个
  - 10 个队列总计: ~8000 行

节省：~3500 行代码（43%）
```

**2. 优先级保证**

**优势**：
- 紧急操作优先：under-replicated Replica 总是先处理
- 避免饿死：相同优先级时 FIFO

**替代方案对比**：

| 方案 | 优先级保证 | 公平性 | 实现复杂度 |
|------|----------|--------|-----------|
| **优先级队列（当前）** | ✅ 严格保证 | ✅ FIFO | 中 |
| FIFO 队列 | ❌ 无保证 | ✅ 完全公平 | 低 |
| 多级队列 | ✅ 弱保证 | ⚠️ 可能饿死 | 高 |
| 轮询 | ❌ 无保证 | ✅ 完全公平 | 低 |

**3. Purgatory 避免无效重试**

**优势**：
- CPU 节省：避免重复失败
- 事件驱动：条件满足时立即重试

**量化收益**：
```
假设场景：100 个 Replica 需要添加副本到不可用的 Store5

无 Purgatory（立即重试）：
  - 重试次数：100 * 60 次/分钟 * 5 分钟 = 30,000 次
  - 每次耗时：100ms
  - 总 CPU 时间：3000 秒 = 50 分钟

有 Purgatory（等待 Gossip 更新）：
  - 重试次数：100 * 1 次 = 100 次
  - 每次耗时：100ms
  - 总 CPU 时间：10 秒

CPU 节省：99.7%
```

#### 7.1.2 劣势

**1. 扫描延迟**

**问题**：
- Scanner 每 10 分钟扫描一次
- 新问题可能需要等待 10 分钟才被发现

**影响**：
```
场景：00:00:00 Store3 宕机，导致 r5000 变为 under-replicated

最坏情况时间线：
  00:00:00 - Store3 宕机
  00:09:59 - Scanner 刚完成上一轮
  00:19:59 - Scanner 发现 r5000 under-replicated
  00:20:01 - replicateQueue 开始处理
  00:20:10 - 添加新副本完成

总延迟：20.1 分钟
```

**缓解措施**：
- 事件驱动触发：Gossip 更新时立即加入队列
- 减少扫描间隔：但会增加 CPU 开销

**2. 队列容量限制**

**问题**：
- 队列最大 10000 个 Replica
- 超过限制时，低优先级任务被丢弃

**影响**：
```
场景：集群有 50,000 个 Replica，其中 15,000 个需要 MVCC GC

队列状态：
  - 10,000 个高优先级 Replica 入队（under-replicated, decommissioning）
  - 5,000 个低优先级 GC 任务被丢弃

结果：
  - 这 5,000 个 Replica 的 GC 延迟
  - 下一轮扫描会再次尝试（10 分钟后）
```

**缓解措施**：
- 增大队列容量：但会增加内存占用
- 分离紧急队列和非紧急队列

**3. 并发度限制**

**问题**：
- 大多数队列 `maxConcurrency=1`
- 处理速度受限

**影响**：
```
场景：replicateQueue 有 1000 个 Replica 需要添加副本

串行处理（maxConcurrency=1）：
  - 每个 Replica 耗时：150ms
  - 总耗时：1000 * 150ms = 150 秒 = 2.5 分钟

如果 maxConcurrency=10：
  - 总耗时：150 秒 / 10 = 15 秒

差距：10 倍
```

**不能随意提高并发的原因**：
- Snapshot 传输会耗尽网络带宽
- Lease 转移需要协调（并发会导致冲突）

### 7.2 替代方案对比

#### 7.2.1 方案 A：每个队列独立扫描

**设计**：

```go
// 每个队列独立的 Scanner 和 processLoop
type leaseQueue struct {
    scanner *replicaScanner // 独立扫描器
}

func (lq *leaseQueue) Start() {
    // 每 10 分钟扫描一次
    go lq.scanner.scanLoop(10 * time.Minute, func(repl *Replica) {
        if should, priority := lq.shouldQueue(repl); should {
            lq.add(repl, priority)
        }
    })

    // 处理循环
    go lq.processLoop()
}
```

**对比**：

| 维度 | 当前方案（共享扫描） | 方案 A（独立扫描） |
|------|-------------------|-------------------|
| **CPU 开销** | 10,000 个 Replica * 1 次扫描 = 10,000 次检查 | 10,000 * 10 次 = 100,000 次检查 |
| **内存占用** | 1 个 Scanner | 10 个 Scanner |
| **扫描延迟** | 10 分钟/轮 | 10 分钟/轮 |
| **实现复杂度** | 中 | 低 |
| **扩展性** | 新增队列无额外开销 | 新增队列增加 10% CPU |

**结论**：当前方案 CPU 效率高 10 倍，代价是实现稍复杂。

#### 7.2.2 方案 B：事件驱动架构

**设计**：

```go
// 完全基于事件，不定期扫描
type EventBus struct {
    subscribers map[EventType][]chan Event
}

// Replica 状态变化时发布事件
func (r *Replica) updateMVCCStats(stats enginepb.MVCCStats) {
    r.stats = stats

    // 发布事件
    eventBus.Publish(EventMVCCStatsChanged, Event{
        RangeID: r.RangeID,
        Stats:   stats,
    })
}

// mvccGCQueue 订阅事件
func (mgcq *mvccGCQueue) Start() {
    eventChan := eventBus.Subscribe(EventMVCCStatsChanged)

    go func() {
        for event := range eventChan {
            if mgcq.shouldQueue(event.RangeID) {
                mgcq.add(event.RangeID)
            }
        }
    }()
}
```

**对比**：

| 维度 | 当前方案（扫描+事件） | 方案 B（纯事件） |
|------|-------------------|-----------------|
| **响应延迟** | 最坏 10 分钟 | 立即（ms 级） |
| **CPU 开销** | 定期扫描（周期性峰值）| 事件触发（平滑）|
| **遗漏风险** | 无（扫描兜底） | 有（事件丢失） |
| **实现复杂度** | 中 | 高 |
| **边缘情况** | 扫描兜底 | 需要手动处理 |

**方案 B 的问题**：

1. **事件风暴**：
   ```
   场景：Store 重启后，所有 Replica 状态变化

   事件数量：
     - 10,000 个 Replica
     - 每个 Replica 触发 5 个事件（Stats, Lease, Descriptor, ...）
     - 总计：50,000 个事件

   EventBus 可能过载
   ```

2. **事件丢失**：
   ```
   场景：Channel 满时，事件可能被丢弃

   后果：
     - 某些 Replica 永远不会被处理
     - 需要定期扫描兜底（回到当前方案）
   ```

**结论**：纯事件驱动在分布式系统中不可靠，需要扫描兜底。

#### 7.2.3 方案 C：Per-Replica 队列

**设计**：

```go
// 每个 Replica 维护自己的任务队列
type Replica struct {
    taskQueue chan Task
}

type Task interface {
    Execute(ctx context.Context, repl *Replica) error
}

// Replica 处理自己的任务
func (r *Replica) processTaskLoop() {
    for task := range r.taskQueue {
        task.Execute(ctx, r)
    }
}

// 队列只需生成任务
func (lq *leaseQueue) shouldQueue(repl *Replica) {
    if needsLeaseTransfer(repl) {
        repl.taskQueue <- &LeaseTransferTask{target: ...}
    }
}
```

**对比**：

| 维度 | 当前方案（全局队列） | 方案 C（Per-Replica 队列） |
|------|-------------------|--------------------------|
| **优先级保证** | ✅ 全局优先级 | ❌ 无法跨 Replica 比较 |
| **并发控制** | ✅ 全局限流 | ⚠️ 难以控制全局并发 |
| **内存占用** | O(N) (N=队列大小) | O(R) (R=Replica 数量) |
| **实现复杂度** | 中 | 低 |

**方案 C 的问题**：

1. **无法保证全局优先级**：
   ```
   场景：
     - r1000: under-replicated (1/3), 紧急
     - r2000: 负载均衡 Lease 转移, 低优先级

   方案 C：
     - 两个 Replica 独立处理
     - 可能 r2000 先完成（因为 Replica 处理顺序随机）

   当前方案：
     - r1000 priority=100, r2000 priority=1
     - 全局队列确保 r1000 先处理
   ```

2. **难以控制全局并发**：
   ```
   目标：全局最多 1 个 replicateQueue 任务并发

   方案 C：
     - 每个 Replica 独立处理
     - 需要全局信号量（回到当前方案的 processSem）
   ```

**结论**：方案 C 适合独立任务，不适合需要全局优先级和并发控制的场景。

### 7.3 当前方案适用场景与不适用场景

#### 7.3.1 适用场景

**1. 后台维护任务**

特征：
- 非实时（可以延迟秒/分钟）
- 需要优先级排序
- 可能失败需要重试

示例：
- MVCC GC
- Raft Log 截断
- 一致性检查

**2. 需要全局协调的操作**

特征：
- 同一时刻只能有一个任务执行
- 需要获取 Lease 或其他全局资源

示例：
- Lease 转移
- Replica 添加/移除
- Range 分裂/合并

**3. 大规模集群**

特征：
- 数千到数万个 Replica
- 需要高效的扫描和调度

当前方案优势：
- 共享扫描减少 CPU 开销
- 优先级队列确保紧急任务优先

#### 7.3.2 不适用场景

**1. 实时操作**

特征：
- 需要 ms 级响应
- 不能容忍扫描延迟

反例：
- 读写请求处理（不应该通过队列）
- Raft 消息处理（直接处理）

**2. 纯本地操作**

特征：
- 不需要全局优先级
- 不需要协调

反例：
- Replica 级别的统计更新（直接更新）
- 本地缓存刷新（直接刷新）

**3. 需要严格顺序保证的操作**

特征：
- 操作 A 必须在操作 B 之前完成
- 不能乱序

当前方案问题：
- 优先级队列可能打乱顺序
- 需要额外的依赖管理机制

---

## 八、总结与心智模型（Mental Model）

### 8.1 核心思想一句话总结

**CockroachDB 的队列系统是一个基于优先级堆和 Purgatory 机制的统一调度框架，通过共享扫描器和模板方法模式，为 10 种不同的后台维护操作提供一致的入队、调度、重试和限流能力，在全局优先级保证、CPU 效率和代码复用之间取得平衡。**

### 8.2 心智模型

**将队列系统理解为一个 "三层过滤 + 一层执行" 的流水线**：

```
┌──────────────────────────────────────────────────────────────┐
│ 第一层：生产者（replicaScanner）                              │
│ ────────────────────────────────────────────────────────────│
│ 职责：定期扫描所有 Replica，调用所有队列的 shouldQueue       │
│ 输出：候选 Replica 集合（每个队列独立）                       │
│ 心智模型：广撒网，初步筛选                                    │
└────────────────────┬─────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────────────────────┐
│ 第二层：优先级队列（priorityQueue）                           │
│ ────────────────────────────────────────────────────────────│
│ 职责：维护全局优先级，确保紧急任务先处理                      │
│ 输出：按优先级排序的 Replica 队列                            │
│ 心智模型：紧急事件优先，FIFO 保证公平                        │
└────────────────────┬─────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────────────────────┐
│ 第三层：并发控制（processSem）                                │
│ ────────────────────────────────────────────────────────────│
│ 职责：限制同时执行的任务数量，避免资源耗尽                    │
│ 输出：最多 maxConcurrency 个并发任务                         │
│ 心智模型：流量控制阀门，防止过载                             │
└────────────────────┬─────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────────────────────┐
│ 第四层：执行与重试（process + Purgatory）                     │
│ ────────────────────────────────────────────────────────────│
│ 职责：执行具体操作，失败时智能重试                            │
│ 输出：成功完成或进入 Purgatory 等待                          │
│ 心智模型：乐观执行，失败进炼狱，条件满足时复活                │
└──────────────────────────────────────────────────────────────┘
```

**类比**：将队列系统类比为 **机场安检流程**

| 队列组件 | 机场类比 | 相似点 |
|---------|---------|--------|
| **replicaScanner** | 售票窗口 | 筛选需要安检的乘客（有票才能进） |
| **shouldQueue** | 初步检查 | 检查是否需要安检（VIP 通道、普通通道） |
| **priorityQueue** | 排队区 | 按优先级排队（头等舱优先、紧急航班优先） |
| **processSem** | 安检通道数量 | 限制同时安检的人数（只有 5 个通道） |
| **process** | 安检过程 | 执行具体检查 |
| **Purgatory** | 二次安检区 | 被拦下的乘客等待重新检查 |

**关键洞察**：

1. **广撒网，精筛选**：
   - Scanner 扫描所有 Replica（广撒网）
   - shouldQueue 精确判断（精筛选）
   - 避免无效入队（CPU 浪费）

2. **全局优先级，局部执行**：
   - priorityQueue 确保全局最紧急的先处理（全局）
   - 每个队列独立执行（局部）
   - 通过 allocatorToken 避免冲突

3. **乐观执行，智能重试**：
   - 先尝试执行（乐观）
   - 失败后分析原因（智能）
     - 永久失败：放弃
     - 暂时失败：Purgatory
   - 条件满足时自动重试

4. **模板方法，策略可插拔**：
   - baseQueue 提供骨架（模板）
   - 具体队列实现策略（可插拔）
   - 新增队列只需实现 `queueImpl` 接口

### 8.3 关键不变量（Invariants）

**理解这些不变量有助于推理系统行为**：

1. **唯一性**：`∀ rangeID, mu.replicas[rangeID]` 在队列中最多一个
   - 推论：同一个 Replica 不会被重复处理
   - 推论：优先级更新会覆盖旧优先级

2. **互斥性**：`priorityQ ∩ purgatory = ∅`
   - 推论：Replica 不能同时在队列和 purgatory 中
   - 推论：从 purgatory 恢复时，必须从 priorityQ 移除

3. **优先级单调性**：`∀ i < j, priorityQ[i].priority ≥ priorityQ[j].priority`
   - 推论：Pop 总是返回最高优先级 Replica
   - 推论：相同优先级时，seq 小的先处理（FIFO）

4. **并发度约束**：`正在处理的任务数 ≤ maxConcurrency`
   - 推论：processSem 的容量决定最大并发
   - 推论：处理完成后必须释放 sem

5. **前置条件一致性**：`入队时的前置条件 ⊆ 处理时的前置条件`
   - 推论：入队时不获取 Lease，处理时获取
   - 推论：状态可能变化，需要重新检查

### 8.4 调试心智模型

**遇到问题时的推理框架**：

```
问题：Replica r1000 没有被处理

推理流程：
1. 是否被 Scanner 发现？
   → 检查 Scanner 是否遍历到 r1000
   → 检查 shouldQueue 是否返回 true

2. 是否成功入队？
   → 检查是否在 purgatory（等待重试）
   → 检查是否在 priorityQ（等待处理）
   → 检查是否被队列满拒绝

3. 是否在处理中？
   → 检查 item.processing 标志
   → 检查是否获取到 processSem

4. 是否处理失败？
   → 检查是否返回 PurgatoryError（进炼狱）
   → 检查是否返回其他错误（直接失败）

5. 是否处理完成？
   → 检查 metrics.successes
   → 检查 Replica 状态是否改变
```

### 8.5 扩展心智模型

**如何新增一个队列？**

```
步骤 1：定义队列结构体
  type myQueue struct {
      *baseQueue
      // 队列特有字段
  }

步骤 2：实现 queueImpl 接口
  func (mq *myQueue) shouldQueue(...) (bool, float64) {
      // 判断是否需要处理
  }

  func (mq *myQueue) process(...) (bool, error) {
      // 执行具体操作
  }

  func (mq *myQueue) timer(...) time.Duration {
      // 返回处理间隔
  }

  func (mq *myQueue) purgatoryChan() <-chan time.Time {
      // 返回 purgatory 触发 channel
  }

  func (mq *myQueue) updateChan() <-chan time.Time {
      // 返回外部事件 channel
  }

步骤 3：配置 queueConfig
  queueConfig{
      maxSize:              10000,
      needsLease:           true/false,
      needsSpanConfigs:     true/false,
      acceptsUnsplitRanges: true/false,
      maxConcurrency:       1,
      // ...
  }

步骤 4：注册到 Scanner
  scanner.AddQueues(myQueue)

完成！baseQueue 自动提供：
  - 优先级队列
  - Purgatory 机制
  - 并发控制
  - 前置条件检查
  - 指标统计
```

### 8.6 最终心智模型图

```
可以把整个队列系统理解为一个 "智能分诊系统"：

                    ┌─────────────────┐
                    │   全科医生       │
                    │ (replicaScanner) │
                    │  初步诊断        │
                    └────────┬────────┘
                             │
          ┌──────────────────┼──────────────────┐
          ↓                  ↓                  ↓
    ┌─────────┐        ┌─────────┐        ┌─────────┐
    │ 急诊室   │        │ 普通门诊 │        │ 体检中心 │
    │(replicate)│       │(lease)   │        │(mvccGC) │
    │priority=100│      │priority=10│       │priority=1│
    └─────┬─────┘        └─────┬─────┘      └─────┬─────┘
          │                    │                  │
          └──────────┬─────────┴──────────────────┘
                     ↓
            ┌────────────────┐
            │ 统一分诊台      │
            │ (priorityQueue)│
            │ 按紧急程度排队  │
            └────────┬───────┘
                     ↓
            ┌────────────────┐
            │ 诊室数量限制    │
            │ (processSem)   │
            │ 只有 N 个诊室   │
            └────────┬───────┘
                     ↓
            ┌────────────────┐
            │ 诊断与治疗      │
            │ (process)      │
            │ 执行具体操作    │
            └────────┬───────┘
                     │
          ┌──────────┴──────────┐
          ↓ 成功                 ↓ 失败
    ┌─────────┐          ┌─────────────┐
    │ 治愈离开 │          │ 观察室       │
    │(success) │          │ (purgatory)  │
    └─────────┘          │ 等待复诊     │
                         └──────────────┘
                               ↑  ↓
                               └──┘
                           条件满足后重新诊断
```

**记住这个心智模型**：
- Scanner = 全科医生（初步诊断）
- priorityQueue = 分诊台（按紧急程度排队）
- processSem = 诊室数量（资源限制）
- process = 治疗（执行操作）
- Purgatory = 观察室（失败后等待条件满足）

---

## 结语

CockroachDB 的队列系统是一个精心设计的调度引擎，它在以下目标之间取得了优雅的平衡：

1. **效率**：共享扫描器减少 90% CPU 开销
2. **优先级**：全局优先级队列确保紧急任务优先
3. **可靠性**：Purgatory 机制避免无效重试
4. **可扩展性**：模板方法模式使新增队列只需 300 行代码
5. **一致性**：统一的前置条件检查和错误处理

**关键设计原则**：

- **分离关注点**：扫描 ≠ 决策 ≠ 执行
- **统一抽象**：baseQueue 提供通用逻辑
- **策略可插拔**：queueImpl 接口定义扩展点
- **智能重试**：Purgatory 等待条件满足
- **优先级保证**：堆 + FIFO 确保公平性

**适用范围**：

- ✅ 后台维护任务（MVCC GC, Raft Log 截断）
- ✅ 需要全局协调的操作（Lease 转移, Replica 变更）
- ✅ 大规模集群（数万个 Replica）
- ❌ 实时操作（读写请求）
- ❌ 纯本地操作（统计更新）
- ❌ 严格顺序依赖（需要额外依赖管理）

**未来演进方向**：

1. **动态优先级**：根据负载自动调整
2. **自适应并发**：根据资源使用情况动态调整 maxConcurrency
3. **预测性调度**：基于历史数据预测何时需要维护
4. **更细粒度的限流**：按操作类型独立限流

---

**延伸阅读**：

- [Raft 论文](https://raft.github.io/raft.pdf)：理解为什么需要 Snapshot Queue
- [CockroachDB Rebalancing](https://www.cockroachlabs.com/docs/stable/architecture/replication-layer.html)：理解 replicateQueue 的决策逻辑
- [MVCC GC](https://www.cockroachlabs.com/blog/living-without-atomic-clocks/)：理解 mvccGCQueue 的 GC Score 计算

**相关章节**：

- [第 38 章：StoreReplicaBTree 深度剖析](./第三十八章_StoreReplicaBTree深度剖析——基于B树的Range索引与高效路由系统.md)：理解 Scanner 如何遍历 Replica
- [第 39 章：ReplicaScanner 深度剖析](./ReplicaScanner深度剖析——基于自适应节奏控制的Store级Replica后台扫描调度器.md)：理解 Scanner 的自适应节奏控制
- [第 31 章：Allocator 决策引擎](./第三十一章_Allocator决策引擎——集群级副本放置的约束求解器.md)：理解 leaseQueue 和 replicateQueue 的决策逻辑

---

*全文完*
