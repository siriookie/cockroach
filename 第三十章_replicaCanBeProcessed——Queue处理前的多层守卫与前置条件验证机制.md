# 第三十章：replicaCanBeProcessed——Queue 处理前的多层守卫与前置条件验证机制

## 一、BFS 概览：为什么需要严格的前置条件检查？(Why)

### 1.1 核心问题：Queue 处理的不确定性与时效性

在分布式系统中,**从 Replica 入队到实际处理之间存在大量不确定性**:

**问题场景**:

```
T0: Replica Scanner 扫描到 Range-100,调用 shouldQueue()
  → 返回 true,priority=0.8
  → 加入 Replicate Queue

T0 + 5秒: Range-100 在队列中等待(前面有更高优先级的 Range)

T0 + 10秒: Queue 从堆中 pop Range-100,准备处理

问题: 这 10 秒内发生了什么?
  - Range-100 可能被分裂成 Range-100 和 Range-101
  - Range-100 可能被合并到 Range-99
  - Range-100 可能丢失了 lease (转移到其他 Store)
  - Range-100 的 Replica 可能被移除 (rebalancing)
  - Range-100 的 span config 可能变化,需要先 split
  - Store 可能正在关闭,span config 系统不可用
```

**如果没有前置条件检查**:

```
场景 1: Replicate Queue 尝试 up-replicate 已被移除的 Replica

processReplica()
  → allocator.AllocateVoter()
  → 访问 Replica.mu.state.Desc → panic (已销毁)
  → 整个 Queue goroutine 崩溃

场景 2: Split Queue 尝试处理已被分裂的 Range

processReplica()
  → AdminSplit(key="b") → 返回 "range already split"
  → 但已经消耗了处理时间,浪费资源

场景 3: Lease Queue 尝试转移不持有的 lease

processReplica()
  → TransferLease(target=Store-2)
  → 返回 NotLeaseHolderError
  → 重复失败,占用队列槽位
```

### 1.2 解决方案：分层守卫机制

`replicaCanBeProcessed` 提供了**处理前的最后一道防线**:

```
入队时检查 (shouldQueue):
  - 基本可行性判断 (是否需要处理)
  - 计算优先级
    ↓
队列中等待 (可能数秒到数分钟)
    ↓
处理前检查 (replicaCanBeProcessed): ← 我们在这里!
  - Replica 是否仍然存在?
  - Replica 是否已初始化?
  - Replica 是否已被销毁?
  - Span Config 是否可用?
  - Range 是否需要先分裂?
  - 是否持有 lease (如果需要)?
    ↓
实际处理 (process)
```

**关键设计理念**:

1. **双重检查(Double-Checking)**: 入队时检查 + 处理前检查
2. **惰性获取 Lease**: 只在需要时才尝试获取,避免无谓竞争
3. **快速失败(Fail-Fast)**: 不满足条件立即返回,不浪费处理资源
4. **条件分离**: 不同 Queue 有不同需求,配置化控制

### 1.3 在整个系统中的位置

**层次结构**:

```
┌─────────────────────────────────────────────────────────────┐
│ Store 级别                                                    │
└─────────────────────────────────────────────────────────────┘
    Replica Scanner (定期扫描所有 Replica)
              ↓
    调用所有 Queue 的 shouldQueue()
              ↓
    MaybeAddAsync() → addInternal()

┌─────────────────────────────────────────────────────────────┐
│ Queue 级别 (baseQueue)                                        │
└─────────────────────────────────────────────────────────────┘
    priorityQueue (优先级堆)
              ↓
    processLoop() → pop()
              ↓
    processReplica() → replicaCanBeProcessed() ← 本章主角
              ↓
    queueImpl.process() (具体队列的处理逻辑)

┌─────────────────────────────────────────────────────────────┐
│ Replica 级别                                                  │
└─────────────────────────────────────────────────────────────┘
    Replica 状态验证
    Lease 获取/验证
    Span Config 获取
```

**与前述章节的关系**:

| 章节 | 机制 | 与本章的关系 |
|------|------|------------|
| 第二十五章 | Store.Start() | 在启动时创建各种 Queue,每个 Queue 都会使用本函数 |
| 第二十八章 | makeIOOverloadCapacityChangeFn | 触发 lease 转移时,将 Replica 加入 leaseQueue,最终会调用本函数 |
| 第二十九章 | RebalanceObjectiveManager | Queue 处理时需要 span config,本函数负责获取 |

**协作对象**:

```go
// pkg/kv/kvserver/queue.go
type baseQueue struct {
    // ...
    queueConfig  // 配置 (needsLease, needsSpanConfigs, acceptsUnsplitRanges)
    impl queueImpl  // 具体队列实现
    store *Store    // 所属 Store
}

// 每个具体 Queue 都有自己的配置
type queueConfig struct {
    needsLease           bool  // 是否需要 lease
    needsSpanConfigs     bool  // 是否需要 span config
    acceptsUnsplitRanges bool  // 是否接受未分裂的 Range
    processDestroyedReplicas bool  // 是否处理已销毁的 Replica
}
```

**不同 Queue 的配置差异**:

| Queue | needsLease | needsSpanConfigs | acceptsUnsplitRanges | processDestroyedReplicas |
|-------|-----------|-----------------|---------------------|------------------------|
| Replicate Queue | ✅ | ✅ | ❌ | ❌ |
| Split Queue | ✅ | ✅ | ✅ | ❌ |
| Merge Queue | ✅ | ✅ | ❌ | ❌ |
| GC Queue | ❌ | ❌ | ✅ | ✅ |
| Raft Snapshot Queue | ❌ | ❌ | ✅ | ❌ |
| Consistency Checker | ❌ | ❌ | ✅ | ❌ |

**为什么配置不同?**

```
Replicate Queue:
  - needsLease=true: 只有 leaseholder 才能发起 replica 变更
  - acceptsUnsplitRanges=false: 必须先 split,否则不知道该用哪个 zone config

Split Queue:
  - needsLease=true: 只有 leaseholder 才能发起 split
  - acceptsUnsplitRanges=true: Split Queue 的职责就是分裂,当然要接受未分裂的 Range

GC Queue:
  - needsLease=false: 每个 Replica 都可以独立 GC 自己的 Raft 日志
  - processDestroyedReplicas=true: GC Queue 的职责之一就是清理 destroyReasonMergePending 的 Replica

Raft Snapshot Queue:
  - needsLease=false: follower 也需要接收 snapshot
  - needsSpanConfigs=false: snapshot 可能是为了让 span config range 可用,不能依赖它
```

---

## 二、BFS 控制流：检查何时触发?如何传播？(How it flows)

### 2.1 调用路径 1: 入队时的检查 (maybeAdd)

**位置**: `pkg/kv/kvserver/queue.go:844-910`

```go
func (bq *baseQueue) maybeAdd(ctx context.Context, repl replicaInQueue, now hlc.ClockTimestamp) {
    // ... 检查 stopped, disabled

    // 第一次调用: acquireLeaseIfNeeded=false
    confReader, err := bq.replicaCanBeProcessed(ctx, repl, false /* acquireLeaseIfNeeded */)
    if err != nil {
        bq.updateMetricsOnEnqueueFailedPrecondition()
        return  // 不满足前置条件,不入队
    }

    // 调用具体队列的 shouldQueue
    should, priority := bq.impl.shouldQueue(ctx, now, realRepl, confReader)
    if !should {
        bq.updateMetricsOnEnqueueNoAction()
        return
    }

    // 加入队列
    _, err = bq.addInternal(ctx, repl.Desc(), repl.ReplicaID(), priority, noopProcessCallback)
}
```

**调用时机**: Replica Scanner 扫描时,或外部主动调用 `MaybeAddAsync`

**参数**: `acquireLeaseIfNeeded=false`
- **为什么是 false?** 入队时不应该获取 lease,否则会导致:
  1. 无谓的 lease 竞争 (可能最终不需要处理)
  2. 扫描速度变慢 (每个 Replica 都可能等待 lease 获取)

**检查内容**:
1. Replica 是否初始化
2. Replica 是否销毁
3. Span Config 是否可用 (如果需要)
4. Range 是否需要分裂 (如果不接受未分裂)
5. **不获取 lease**,只检查当前是否持有

**时间线示例**:

```
T0: Replica Scanner 扫描到 Range-100
T1: maybeAdd() 调用
T2: replicaCanBeProcessed(acquireLeaseIfNeeded=false)
  - IsInitialized(): true
  - IsDestroyed(): nil
  - needsSpanConfigs=true → GetConfReader(): success
  - acceptsUnsplitRanges=false → NeedsSplit(): false
  - needsLease=true → CurrentLeaseStatus():
      Lease { Replica: (n1,s1):1, Epoch: 5, ... }
      OwnedBy(s1): true ← 持有 lease,通过
T3: 返回 confReader, nil
T4: shouldQueue() 返回 true, priority=0.8
T5: addInternal() 加入队列
T6: 总耗时 ~2ms
```

### 2.2 调用路径 2: 处理前的检查 (processReplica)

**位置**: `pkg/kv/kvserver/queue.go:1226-1262`

```go
func (bq *baseQueue) processReplica(
    ctx context.Context, repl replicaInQueue, priorityAtEnqueue float64,
) error {
    ctx, span := tracing.EnsureChildSpan(ctx, bq.Tracer, bq.processOpName())
    defer span.Finish()

    // 第二次调用: acquireLeaseIfNeeded=true
    conf, err := bq.replicaCanBeProcessed(ctx, repl, true /* acquireLeaseIfNeeded */)
    if err != nil {
        if errors.Is(err, errMarkNotAcquirableLease) {
            return nil  // 无法获取 lease,但不是错误
        }
        log.VErrEventf(ctx, 2, "replica can not be processed now: %s", err)
        return err
    }

    // 带超时的实际处理
    return timeutil.RunWithTimeout(ctx, ..., processTimeoutFunc(repl), func(ctx) error {
        processed, err := bq.impl.process(ctx, realRepl, conf, priorityAtEnqueue)
        // ...
    })
}
```

**调用时机**: Queue 的 processLoop 从优先级堆 pop 出 Replica 后

**参数**: `acquireLeaseIfNeeded=true`
- **为什么是 true?** 处理前必须确保持有 lease,否则:
  1. 无法执行需要 lease 的操作 (如 AdminSplit, ChangeReplicas)
  2. 会导致处理失败,浪费资源

**检查内容** (与 maybeAdd 相比):
1. 所有 maybeAdd 的检查 (Replica 状态、Span Config 等)
2. **主动获取 lease** (如果需要且当前不持有)
3. 切换 lease 类型 (如果需要)

**时间线示例**:

```
T0: processLoop 从堆中 pop Range-100
T1: processReplica() 调用
T2: replicaCanBeProcessed(acquireLeaseIfNeeded=true)
  - IsInitialized(): true
  - IsDestroyed(): nil
  - GetConfReader(): success
  - NeedsSplit(): false
  - needsLease=true → redirectOnOrAcquireLease()
    ↓
    当前 lease 状态: NotLeaseHolderError (lease 已转移)
    ↓
    尝试获取 lease:
      - 发送 RequestLease Raft 提议
      - 等待 Raft 日志应用
      - 耗时 ~50ms
    ↓
    获取成功: Lease { Replica: (n1,s1):1, Epoch: 6, ... }
  - maybeSwitchLeaseType(): 检查是否需要从 Expiration 切换到 Epoch
T3: 返回 confReader, nil
T4: 调用 impl.process()
T5: 总耗时 ~55ms (主要是 lease 获取)
```

### 2.3 两次调用的差异

**核心差异**: `acquireLeaseIfNeeded` 参数

| 调用点 | acquireLeaseIfNeeded | 原因 |
|--------|---------------------|------|
| maybeAdd | false | 入队时不应阻塞,避免扫描变慢 |
| processReplica | true | 处理前必须持有 lease,确保能成功 |

**具体行为差异**:

```
场景: Replica 当前不持有 lease

maybeAdd (acquireLeaseIfNeeded=false):
  → CurrentLeaseStatus(): Lease { Replica: (n2,s2):2, ... }
  → OwnedBy(s1): false
  → 返回 benignerror("needs lease, not adding")
  → Replica 不入队

processReplica (acquireLeaseIfNeeded=true):
  → redirectOnOrAcquireLease()
  → 发送 RequestLease Raft 提议
  → 等待获取 lease
  → 如果成功 → 继续处理
  → 如果失败 (NotLeaseHolderError) → 返回 errMarkNotAcquirableLease
```

**为什么需要两次检查?**

```
时间差导致状态变化:

T0: maybeAdd() 时
  - Replica 持有 lease ✅
  - 加入队列

T0 + 10秒: processReplica() 时
  - Lease 已被转移到 Store-2 ❌
  - 需要重新获取 lease

如果没有 processReplica 的检查:
  - 直接调用 impl.process()
  - AdminSplit() 返回 NotLeaseHolderError
  - 处理失败,浪费时间
```

### 2.4 完整的处理流程图

```
┌─────────────────────────────────────────────────────────────┐
│ 阶段 1: Replica Scanner 扫描                                  │
└─────────────────────────────────────────────────────────────┘
    for each Replica in Store:
      MaybeAddAsync(repl, now)
        ↓
      maybeAdd()
        ↓
      replicaCanBeProcessed(acquireLeaseIfNeeded=false)
        - 检查基本状态
        - 不获取 lease
        ↓
      shouldQueue() → true, priority=0.8
        ↓
      addInternal() → 加入优先级堆

┌─────────────────────────────────────────────────────────────┐
│ 阶段 2: 队列中等待 (可能数秒到数分钟)                          │
└─────────────────────────────────────────────────────────────┘
    priorityQueue 维护最大堆
    按优先级排序

┌─────────────────────────────────────────────────────────────┐
│ 阶段 3: processLoop 处理                                      │
└─────────────────────────────────────────────────────────────┘
    pop() → 从堆中取出最高优先级 Replica
      ↓
    processReplica()
      ↓
    replicaCanBeProcessed(acquireLeaseIfNeeded=true) ← 第二次检查
      - 重新检查所有状态
      - 主动获取 lease (如果需要)
      ↓
    impl.process() → 实际处理逻辑
      ↓
    finishProcessingReplica()
```

**并发场景分析**:

```
场景: 两个 goroutine 同时操作同一个 Replica

Goroutine 1 (Replica Scanner):
  T0: maybeAdd(Range-100)
  T1: replicaCanBeProcessed(false)
  T2: 检查通过,加入队列

Goroutine 2 (Remove Replica):
  T1.5: RemoveReplica(Range-100)
    → Replica.shMu.destroyStatus.Set(destroyReasonRemoved)
    → MaybeRemove(Range-100) → 从队列移除

Goroutine 3 (processLoop):
  T3: pop() → 可能已经被移除
  T4: getReplica(Range-100) → 返回 nil
  T5: removeFromReplicaSetLocked() → 清理
  T6: 重新 pop

关键保护: pop() 中会检查 replicaID 是否匹配
```

---

## 三、DFS 深入：6 个守卫检查的实现细节 (How it works)

### 3.1 函数签名与返回值

**完整签名** (queue.go:1275-1277):

```go
func (bq *baseQueue) replicaCanBeProcessed(
    ctx context.Context,
    repl replicaInQueue,
    acquireLeaseIfNeeded bool,
) (spanconfig.StoreReader, error)
```

**输入**:
- `ctx`: 上下文,用于取消、超时、日志
- `repl`: 要检查的 Replica (接口类型,隐藏具体实现)
- `acquireLeaseIfNeeded`: 是否主动获取 lease

**输出**:
- `spanconfig.StoreReader`: Span Config 读取器 (如果队列需要)
- `error`: 错误信息 (如果不满足前置条件)

**返回值语义**:

```go
返回 (confReader, nil):
  - Replica 满足所有前置条件
  - 可以继续处理
  - confReader 可能是 nil (如果 needsSpanConfigs=false)

返回 (nil, err):
  - Replica 不满足前置条件
  - 不应该处理
  - err 描述具体原因
```

**特殊错误**: `errMarkNotAcquirableLease`

```go
// queue.go:1264-1266
var errMarkNotAcquirableLease = errors.New("lease can't be acquired")
```

**用途**: 区分"无法获取 lease"和其他错误

```go
// processReplica 中的处理 (queue.go:1238-1240)
if errors.Is(err, errMarkNotAcquirableLease) {
    return nil  // 不是真正的错误,跳过即可
}
```

### 3.2 检查 1: Replica 初始化状态

**代码** (queue.go:1278-1285):

```go
if !repl.IsInitialized() {
    // We checked this when adding the replica, but we need to check it again
    // in case this is a different replica with the same range ID (see #14193).
    // This is possible in the case where the replica was enqueued while not
    // having a replica ID, perhaps due to a pre-emptive snapshot, and has
    // since been removed and re-added at a different replica ID.
    return nil, errors.New("cannot process uninitialized replica")
}
```

**`IsInitialized()` 的含义**:

```go
// pkg/kv/kvserver/replica.go
func (r *Replica) IsInitialized() bool {
    r.mu.RLock()
    defer r.mu.RUnlock()
    return r.isInitializedRLocked()
}

func (r *Replica) isInitializedRLocked() bool {
    return r.mu.state.Desc.IsInitialized()
}

// pkg/roachpb/metadata.go
func (r *RangeDescriptor) IsInitialized() bool {
    return len(r.InternalReplicas) > 0
}
```

**初始化的含义**: Replica 有完整的 RangeDescriptor,包含 replica 列表

**未初始化的场景**:

**场景 1: Preemptive Snapshot**

```
Node-1 创建新 Range-100:
  T0: 发送 preemptive snapshot 到 Node-2
  T1: Node-2 创建 Replica 对象,rangeID=100, replicaID=0 (未初始化)
  T2: 接收 snapshot 数据
  T3: 应用 RangeDescriptor → replicaID=2 (已初始化)

如果 T2 时 Replica Scanner 扫描:
  - IsInitialized()=false
  - 不应该入队 (还没有完整信息)
```

**场景 2: Replica 被移除后重新添加**

```
原 Replica:
  rangeID=100, replicaID=1 → 已初始化,已入队

Rebalancing 移除:
  T0: ChangeReplicas([Remove (n1,s1):1])
  T1: Replica.IsDestroyed()=destroyReasonRemoved
  T2: Replica 从 Store 移除

重新添加:
  T3: ChangeReplicas([Add (n1,s1):3])
  T4: 创建新 Replica,rangeID=100, replicaID=3
  T5: 接收 snapshot → 初始化

问题: 队列中可能还有 rangeID=100 的旧 item
解决: pop() 时检查 replicaID 是否匹配
```

**注释引用的 issue #14193**:

```
问题描述: Replica 被移除并重新添加后,队列中的 item 仍然指向旧的 replicaID

复现步骤:
  1. Replica (rangeID=100, replicaID=1) 入队
  2. Replica 被移除 (rebalancing)
  3. Replica 重新添加 (replicaID=3)
  4. Queue 处理旧 item (rangeID=100)
  5. getReplica(100) 返回新 Replica (replicaID=3)
  6. replicaID 不匹配 → 移除 item

修复: pop() 中增加 replicaID 匹配检查
```

### 3.3 检查 2: Replica 销毁状态

**代码** (queue.go:1287-1294):

```go
// The replica GC queue can process destroyed replicas if it is stuck in
// destroyReasonMergePending for too long.
if reason, err := repl.IsDestroyed(); err != nil {
    if !bq.queueConfig.processDestroyedReplicas || reason == destroyReasonRemoved {
        log.VEventf(ctx, 3, "replica destroyed (%s); skipping", err)
        return nil, errors.Wrap(err, "cannot process destroyed replica")
    }
}
```

**`IsDestroyed()` 的实现**:

```go
// pkg/kv/kvserver/replica.go
func (r *Replica) IsDestroyed() (DestroyReason, error) {
    r.shMu.RLock()
    defer r.shMu.RUnlock()
    return r.shMu.destroyStatus.Get()
}

// pkg/kv/kvserver/replica_destroy.go
type destroyStatus struct {
    reason DestroyReason
    err    error
}

type DestroyReason int

const (
    destroyReasonRemoved DestroyReason = iota
    destroyReasonMergePending
)
```

**两种销毁原因**:

**1. `destroyReasonRemoved`**: Replica 已从 Raft group 移除

```
触发场景:
  - Rebalancing: ChangeReplicas([Remove (n1,s1):1])
  - Range 合并: 右半部分被合并到左半部分
  - Replica 损坏: 被标记为 corrupted

状态设置:
  r.shMu.destroyStatus.Set(
      errors.New("removed from raft group"),
      destroyReasonRemoved,
  )

后续处理:
  - 所有 Queue 都应该跳过 (除非 processDestroyedReplicas=true)
  - Replica 等待 GC 清理
```

**2. `destroyReasonMergePending`**: Range 合并待定

```
触发场景:
  - Range-99 和 Range-100 合并
  - Range-100 (右侧) 被标记为 destroyReasonMergePending
  - 等待 Range-99 (左侧) 完成 subsumption

状态设置:
  r.shMu.destroyStatus.Set(
      errors.New("merge pending"),
      destroyReasonMergePending,
  )

问题: 如果 Range-99 的 leaseholder 故障,subsumption 可能一直不完成
解决: GC Queue 可以处理 destroyReasonMergePending (如果超时)
```

**`processDestroyedReplicas` 配置**:

```go
// 只有 GC Queue 设置为 true
gcQueue := newGCQueue(store, cfg)
gcQueue.queueConfig.processDestroyedReplicas = true

// 其他 Queue 都是 false
replicateQueue := newReplicateQueue(store, cfg)
replicateQueue.queueConfig.processDestroyedReplicas = false
```

**逻辑分支**:

```
if reason, err := repl.IsDestroyed(); err != nil:
  ↓
  reason = destroyReasonRemoved:
    - 所有 Queue 都返回错误 (包括 GC Queue)
    - Replica 已彻底移除,不应该处理

  reason = destroyReasonMergePending:
    - GC Queue: 继续处理 (可能需要清理)
    - 其他 Queue: 返回错误 (合并待定,不应该处理)
```

**具体示例**:

```
场景: Range-100 正在被合并到 Range-99

T0: Range-99 发起合并
  → Range-100.shMu.destroyStatus.Set(destroyReasonMergePending)

T1: Replicate Queue 尝试处理 Range-100
  → IsDestroyed() → (destroyReasonMergePending, error)
  → processDestroyedReplicas=false
  → 返回 "cannot process destroyed replica"
  → 跳过处理

T2: GC Queue 尝试处理 Range-100
  → IsDestroyed() → (destroyReasonMergePending, error)
  → processDestroyedReplicas=true
  → 继续执行 (检查是否需要清理)
```

### 3.4 检查 3: Span Config 可用性

**代码** (queue.go:1296-1307):

```go
// The conf is only populated if the queue requires a span config. Otherwise
// nil is always returned.
var confReader spanconfig.StoreReader
if bq.needsSpanConfigs {
    var err error
    confReader, err = bq.store.GetConfReader(ctx)
    if err != nil {
        if log.V(1) || !errors.Is(err, errSpanConfigsUnavailable) {
            log.KvDistribution.Warningf(ctx, "unable to retrieve conf reader, skipping: %v", err)
        }
        return nil, err
    }
    // ... 后续检查
}
```

**`GetConfReader` 的实现**:

```go
// pkg/kv/kvserver/store.go
func (s *Store) GetConfReader(ctx context.Context) (spanconfig.StoreReader, error) {
    if s.cfg.SpanConfigSubscriber == nil {
        return nil, errSpanConfigsUnavailable
    }
    return s.cfg.SpanConfigSubscriber.GetReader(), nil
}

var errSpanConfigsUnavailable = errors.New("span configs unavailable")
```

**Span Config 系统**:

```
Span Config 是 CockroachDB 的配置系统,替代了旧的 Zone Config

核心组件:
  - SpanConfigSubscriber: 订阅 span config 变更
  - StoreReader: 读取 span config
  - SpanConfig: 配置本身 (副本数、约束、GC TTL 等)

数据流:
  System Range (存储 span config)
    ↓ Rangefeed
  SpanConfigSubscriber (每个 Store 一个)
    ↓
  StoreReader (内存缓存)
    ↓
  Queue 读取配置
```

**何时不可用?**

```
场景 1: Store 启动早期
  - Store.Start() 正在执行
  - SpanConfigSubscriber 尚未初始化
  - GetConfReader() → errSpanConfigsUnavailable

场景 2: System Range 不可用
  - Span config range 没有 quorum
  - Rangefeed 断开
  - SpanConfigSubscriber 数据过期

场景 3: 测试环境
  - 某些测试不需要 span config
  - cfg.SpanConfigSubscriber = nil
```

**为什么需要 Span Config?**

```
Replicate Queue:
  - 需要知道 ReplicationFactor (副本数)
  - 需要知道 Constraints (副本放置约束)
  - 例如: "至少 3 个副本,分布在不同 AZ"

Split Queue:
  - 需要知道 RangeMaxBytes (Range 最大大小)
  - 需要知道是否有 split key
  - 例如: "Range 超过 512MB 自动分裂"

Merge Queue:
  - 需要知道 RangeMinBytes (Range 最小大小)
  - 需要知道相邻 Range 的配置是否兼容
  - 例如: "Range 小于 16MB 且配置相同时合并"
```

**错误日志处理**:

```go
if log.V(1) || !errors.Is(err, errSpanConfigsUnavailable) {
    log.KvDistribution.Warningf(ctx, "unable to retrieve conf reader, skipping: %v", err)
}
```

**逻辑**: 只在以下情况记录 Warning:
- `log.V(1)=true` (详细日志级别)
- 或者错误**不是** `errSpanConfigsUnavailable`

**为什么特殊处理 `errSpanConfigsUnavailable`?**

```
errSpanConfigsUnavailable 在启动早期很常见:

T0: Store 启动
T1~T5: Span config 系统初始化 (5 秒)
T1~T5 期间: 每次 maybeAdd() 都会返回 errSpanConfigsUnavailable

如果每次都记录 Warning:
  - 日志会被大量重复消息淹没
  - 实际上这是正常现象

解决: 只在 V(1) 级别记录 (通常不开启)
```

### 3.5 检查 4: Range 是否需要分裂

**代码** (queue.go:1309-1322):

```go
if !bq.acceptsUnsplitRanges {
    // Queue does not accept unsplit ranges. Check to see if the range needs to
    // be spilt because of a span config.
    needsSplit, err := confReader.NeedsSplit(ctx, repl.Desc().StartKey, repl.Desc().EndKey)
    if err != nil {
        log.KvDistribution.Warningf(ctx, "unable to compute NeedsSplit, skipping: %v", err)
        return nil, err
    }
    if needsSplit {
        log.VEventf(ctx, 3, "split needed; skipping")
        return nil, errors.New("split needed; skipping")
    }
}
```

**`NeedsSplit` 的判断逻辑**:

```go
// pkg/spanconfig/spanconfig.go
func (r *StoreReader) NeedsSplit(
    ctx context.Context, start, end roachpb.RKey,
) (bool, error) {
    // 检查 Range [start, end) 是否跨越多个 span config
    configs := r.GetConfigsForKeyRange(start, end)

    if len(configs) > 1 {
        return true, nil  // 跨越多个配置,需要分裂
    }

    // 检查是否有显式的 split key
    if r.HasSplitKey(start, end) {
        return true, nil
    }

    return false, nil
}
```

**为什么需要这个检查?**

**场景**: Replicate Queue 尝试处理跨越多个 zone 的 Range

```
Range-100: [a, z)
  - [a, m): zone=us-east (3 副本)
  - [m, z): zone=us-west (5 副本)

问题: 应该有多少副本?
  - 如果按 [a, m) 的配置 → 3 个副本 → [m, z) 副本不足
  - 如果按 [m, z) 的配置 → 5 个副本 → [a, m) 副本过多

解决: 拒绝处理,等待 Split Queue 先分裂
```

**`acceptsUnsplitRanges` 的含义**:

```
acceptsUnsplitRanges=true:
  - Split Queue: 职责就是分裂,当然要接受未分裂的 Range
  - GC Queue: 清理 Raft 日志,与 split 无关
  - Consistency Checker: 检查一致性,与 split 无关

acceptsUnsplitRanges=false:
  - Replicate Queue: 需要明确的副本配置
  - Merge Queue: 需要确保配置兼容
  - Lease Queue: 需要明确的 lease 偏好设置
```

**具体示例**:

```
T0: 用户创建 zone config
  ALTER TABLE users CONFIGURE ZONE USING num_replicas = 5

T1: Span config 系统传播配置
  - [/Table/100, /Table/100/users): 3 副本 (默认)
  - [/Table/100/users, /Table/101): 5 副本 (新配置)

T2: Replicate Queue 扫描到 Range-100
  - Range-100: [/Table/100, /Table/101)
  - NeedsSplit([/Table/100, /Table/101)) → true
  - acceptsUnsplitRanges=false
  - 返回 "split needed; skipping"
  - Replicate Queue 跳过

T3: Split Queue 扫描到 Range-100
  - NeedsSplit() → true
  - acceptsUnsplitRanges=true
  - 继续处理 → AdminSplit(/Table/100/users)

T4: 分裂后
  - Range-100: [/Table/100, /Table/100/users) → 3 副本
  - Range-101: [/Table/100/users, /Table/101) → 5 副本

T5: Replicate Queue 重新扫描
  - Range-100: NeedsSplit() → false → 处理
  - Range-101: NeedsSplit() → false → 处理
```

### 3.6 检查 5: Lease 获取 (acquireLeaseIfNeeded=true)

**代码** (queue.go:1324-1356):

```go
// If the queue requires a replica to have the range lease in
// order to be processed, check whether this replica has range lease
// and renew or acquire if necessary.
if bq.needsLease {
    if acquireLeaseIfNeeded {
        _, pErr := repl.redirectOnOrAcquireLease(ctx)
        if pErr != nil {
            switch v := pErr.GetDetail().(type) {
            case *kvpb.NotLeaseHolderError, *kvpb.RangeNotFoundError:
                log.VEventf(ctx, 3, "%s; skipping", v)
                return nil, errMarkNotAcquirableLease
            }
            log.VErrEventf(ctx, 2, "could not obtain lease: %s", pErr)
            return nil, errors.Wrapf(pErr.GoError(), "%s: could not obtain lease", repl)
        }

        // TODO(baptist): Should this be added to replicaInQueue?
        realRepl, _ := repl.(*Replica)
        pErr = realRepl.maybeSwitchLeaseType(ctx)
        if pErr != nil {
            return nil, pErr.GoError()
        }
    } else {
        // Don't process if we don't own the lease.
        st := repl.CurrentLeaseStatus(ctx)
        if st.IsValid() && !st.OwnedBy(repl.StoreID()) {
            log.VEventf(ctx, 1, "needs lease; not adding: %v", st.Lease)
            // NB: this is an expected error, so make sure it doesn't get
            // logged loudly.
            return nil, benignerror.New(errors.Newf("needs lease, not adding: %v", st.Lease))
        }
    }
}
```

**两个分支**: `acquireLeaseIfNeeded` 为 true 或 false

#### 3.6.1 分支 1: acquireLeaseIfNeeded=false (入队时)

**代码** (queue.go:1346-1355):

```go
} else {
    // Don't process if we don't own the lease.
    st := repl.CurrentLeaseStatus(ctx)
    if st.IsValid() && !st.OwnedBy(repl.StoreID()) {
        log.VEventf(ctx, 1, "needs lease; not adding: %v", st.Lease)
        return nil, benignerror.New(errors.Newf("needs lease, not adding: %v", st.Lease))
    }
}
```

**`CurrentLeaseStatus` 的实现**:

```go
// pkg/kv/kvserver/replica.go
func (r *Replica) CurrentLeaseStatus(ctx context.Context) kvserverpb.LeaseStatus {
    r.mu.RLock()
    defer r.mu.RUnlock()
    return r.leaseStatusAtRLocked(ctx, r.store.Clock().NowAsClockTimestamp())
}

func (r *Replica) leaseStatusAtRLocked(
    ctx context.Context, now hlc.ClockTimestamp,
) kvserverpb.LeaseStatus {
    lease := r.mu.state.Lease
    if lease.Empty() {
        return kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED}
    }

    // 检查 lease 是否过期
    if lease.Type() == roachpb.LeaseExpiration {
        if lease.Expiration.Less(now.ToTimestamp()) {
            return kvserverpb.LeaseStatus{
                Lease: *lease,
                State: kvserverpb.LeaseState_EXPIRED,
            }
        }
    } else if lease.Type() == roachpb.LeaseEpoch {
        // 检查 liveness epoch
        liveness, ok := r.store.cfg.NodeLiveness.GetLiveness(lease.Replica.NodeID)
        if !ok || liveness.Epoch < lease.Epoch {
            return kvserverpb.LeaseStatus{
                Lease: *lease,
                State: kvserverpb.LeaseState_EXPIRED,
            }
        }
    }

    return kvserverpb.LeaseStatus{
        Lease: *lease,
        State: kvserverpb.LeaseState_VALID,
        Now:   now,
    }
}
```

**检查逻辑**:

```
st := repl.CurrentLeaseStatus(ctx)
  ↓
st.IsValid(): 检查 lease 是否有效 (未过期)
  ↓
st.OwnedBy(repl.StoreID()): 检查 lease 是否属于本 Store

如果不满足:
  - 返回 benignerror (不会触发 metric.failures)
  - Replica 不入队
```

**为什么使用 `benignerror`?**

```go
// pkg/kv/kvserver/benignerror/benignerror.go
type benignError struct {
    error
}

func IsBenign(err error) bool {
    var bErr *benignError
    return errors.As(err, &bErr)
}
```

**用途**: 区分"预期的失败"和"真正的错误"

```
场景 1: 不持有 lease (benign error)
  - 这是正常现象,不应该入队
  - 不应该记录到 metric.failures
  - 不应该打印 Warning 日志

场景 2: 获取 lease 失败 (真正错误)
  - 应该记录到 metric.failures
  - 应该打印 Warning 日志
  - 可能需要调查
```

**具体示例**:

```
T0: Replica Scanner 扫描到 Range-100
T1: maybeAdd() 调用
T2: replicaCanBeProcessed(acquireLeaseIfNeeded=false)
T3: needsLease=true → CurrentLeaseStatus()
  - 当前 lease: Lease { Replica: (n2,s2):2, Epoch: 5 }
  - st.IsValid(): true (未过期)
  - st.OwnedBy(s1): false (属于 Store-2)
T4: 返回 benignerror("needs lease, not adding")
T5: maybeAdd() 返回,不入队
T6: metric.enqueueFailedPrecondition++
```

#### 3.6.2 分支 2: acquireLeaseIfNeeded=true (处理前)

**代码** (queue.go:1328-1345):

```go
if acquireLeaseIfNeeded {
    _, pErr := repl.redirectOnOrAcquireLease(ctx)
    if pErr != nil {
        switch v := pErr.GetDetail().(type) {
        case *kvpb.NotLeaseHolderError, *kvpb.RangeNotFoundError:
            log.VEventf(ctx, 3, "%s; skipping", v)
            return nil, errMarkNotAcquirableLease
        }
        log.VErrEventf(ctx, 2, "could not obtain lease: %s", pErr)
        return nil, errors.Wrapf(pErr.GoError(), "%s: could not obtain lease", repl)
    }

    realRepl, _ := repl.(*Replica)
    pErr = realRepl.maybeSwitchLeaseType(ctx)
    if pErr != nil {
        return nil, pErr.GoError()
    }
}
```

**`redirectOnOrAcquireLease` 的实现**:

```go
// pkg/kv/kvserver/replica.go
func (r *Replica) redirectOnOrAcquireLease(
    ctx context.Context,
) (kvserverpb.LeaseStatus, *kvpb.Error) {
    // 检查当前 lease 状态
    st := r.CurrentLeaseStatus(ctx)

    if st.IsValid() && st.OwnedBy(r.StoreID()) {
        return st, nil  // 已经持有 lease,直接返回
    }

    // 如果 lease 属于其他 Store,返回 NotLeaseHolderError
    if st.IsValid() {
        return st, kvpb.NewError(&kvpb.NotLeaseHolderError{
            Lease:       st.Lease,
            LeaseHolder: st.Lease.Replica,
        })
    }

    // Lease 已过期,尝试获取
    return r.requestLease(ctx)
}

func (r *Replica) requestLease(ctx context.Context) (kvserverpb.LeaseStatus, *kvpb.Error) {
    // 发送 RequestLease Raft 提议
    ba := &kvpb.BatchRequest{}
    ba.Add(&kvpb.RequestLeaseRequest{
        Lease: roachpb.Lease{
            Replica:   r.Desc().Replicas().VoterDescriptor(r.StoreID()),
            Start:     r.Clock().NowAsClockTimestamp(),
            // ...
        },
    })

    _, pErr := r.Send(ctx, ba)
    if pErr != nil {
        return kvserverpb.LeaseStatus{}, pErr
    }

    // 等待 lease 应用
    return r.CurrentLeaseStatus(ctx), nil
}
```

**获取流程**:

```
1. 检查当前 lease 状态:
   - 如果已持有 → 返回
   - 如果属于其他 Store → 返回 NotLeaseHolderError
   - 如果过期 → 继续

2. 发送 RequestLease Raft 提议:
   - 构造 Lease 对象
   - 提交到 Raft
   - 等待应用

3. 可能的结果:
   - 成功: 获得 lease
   - 失败: NotLeaseHolderError (其他 Store 抢先)
   - 失败: RangeNotFoundError (Range 已不存在)
```

**错误处理**:

```go
switch v := pErr.GetDetail().(type) {
case *kvpb.NotLeaseHolderError, *kvpb.RangeNotFoundError:
    return nil, errMarkNotAcquirableLease  // 无法获取,但不是错误
}
```

**为什么特殊处理这两种错误?**

```
NotLeaseHolderError:
  - 其他 Store 持有 lease,且不愿意转移
  - 或者在获取过程中被其他 Store 抢先
  - 这是正常的竞争结果,不应该记录为 failure

RangeNotFoundError:
  - Range 已被删除或合并
  - 不应该继续处理
  - 也不应该记录为 failure
```

**`maybeSwitchLeaseType` 的作用**:

```go
// pkg/kv/kvserver/replica.go
func (r *Replica) maybeSwitchLeaseType(ctx context.Context) *kvpb.Error {
    // 检查是否需要从 Expiration lease 切换到 Epoch lease

    st := r.CurrentLeaseStatus(ctx)
    if st.Lease.Type() == roachpb.LeaseExpiration {
        // 检查 node liveness 是否可用
        if r.store.cfg.NodeLiveness != nil {
            // 发送 TransferLease 到自己,但使用 Epoch 类型
            return r.TransferLease(ctx, r.StoreID(), true /* transferEpochLease */)
        }
    }
    return nil
}
```

**为什么需要切换 lease 类型?**

```
Expiration Lease (旧类型):
  - 基于时间戳过期
  - 需要时钟同步
  - 可能因为时钟偏移导致问题

Epoch Lease (新类型):
  - 基于 node liveness epoch
  - 不依赖时钟同步
  - 更可靠

迁移策略:
  - 新集群: 默认使用 Epoch lease
  - 旧集群: 逐步从 Expiration 切换到 Epoch
  - 切换时机: 获取 lease 后,检查是否需要转换
```

**完整的处理前 lease 获取流程**:

```
T0: processReplica() 调用
T1: replicaCanBeProcessed(acquireLeaseIfNeeded=true)
T2: redirectOnOrAcquireLease()
  ↓
T3: CurrentLeaseStatus()
  - Lease { Replica: (n2,s2):2, Epoch: 5 }
  - OwnedBy(s1): false
  ↓
T4: 返回 NotLeaseHolderError
  ↓
T5: 检查错误类型 → NotLeaseHolderError
T6: 返回 errMarkNotAcquirableLease
T7: processReplica() 返回 nil (不是真正的错误)
T8: finishProcessingReplica() 不记录 failure

或者:

T3: CurrentLeaseStatus()
  - Lease { Replica: (n1,s1):1, Epoch: 4 }
  - State: EXPIRED (liveness epoch 已更新)
  ↓
T4: requestLease()
  - 发送 RequestLease Raft 提议
  - Epoch: 5
  ↓
T5: 等待 Raft 日志应用 (~50ms)
T6: CurrentLeaseStatus()
  - Lease { Replica: (n1,s1):1, Epoch: 5 }
  - OwnedBy(s1): true
  ↓
T7: maybeSwitchLeaseType() → nil (已经是 Epoch lease)
T8: 返回 confReader, nil
T9: 继续处理
```

---

## 四、运行时行为：前置条件如何影响处理决策？(Runtime Behavior)

### 4.1 入队阶段的快速过滤

**场景**: Replica Scanner 每 10 分钟扫描一次所有 Replica

```
Store 有 2000 个 Replica:
  - 1500 个正常 Replica
  - 300 个不持有 lease
  - 100 个未初始化 (正在接收 snapshot)
  - 50 个已销毁 (等待 GC)
  - 50 个需要先 split

Replicate Queue 扫描:
  T0: 开始扫描
  T0~T5: 遍历 2000 个 Replica,调用 maybeAdd()
    - 每个 maybeAdd() 调用 replicaCanBeProcessed(false)

  结果:
    - 1500 个: 通过前置条件检查
      → 调用 shouldQueue() → 其中 500 个需要 replicate
      → 加入队列
    - 300 个: needsLease 检查失败 (metric.enqueueFailedPrecondition++)
    - 100 个: IsInitialized() 失败 (metric.enqueueFailedPrecondition++)
    - 50 个: IsDestroyed() 失败 (metric.enqueueFailedPrecondition++)
    - 50 个: NeedsSplit() 失败 (metric.enqueueFailedPrecondition++)

  T5: 扫描完成
  队列中: 500 个 Replica 等待处理
```

**性能优化**: 快速失败

```
前置条件检查耗时:
  - IsInitialized(): RLock + 读取状态 → ~100ns
  - IsDestroyed(): RLock + 读取状态 → ~100ns
  - CurrentLeaseStatus(): RLock + 时钟比较 → ~500ns
  - 总计: ~1μs

shouldQueue 耗时 (如果调用):
  - 需要计算优先级
  - 可能需要读取 Raft 状态
  - 可能需要查询 allocator
  - 总计: ~100μs

通过前置条件快速过滤:
  - 节省了 500 个不必要的 shouldQueue() 调用
  - 节省: 500 × 100μs = 50ms
```

### 4.2 处理阶段的 Lease 获取

**场景**: Replicate Queue 处理 500 个 Replica

```
T0: processLoop 开始
T1: pop() → Range-100 (priority=0.9)
T2: processReplica()
  ↓
T3: replicaCanBeProcessed(acquireLeaseIfNeeded=true)
  ↓
T4: redirectOnOrAcquireLease()

情况 1: 已持有 lease (最常见)
  - CurrentLeaseStatus() → VALID, OwnedBy=true
  - 直接返回 (~1ms)
  - 总耗时: ~2ms

情况 2: Lease 过期,需要获取 (偶尔)
  - requestLease()
  - Raft 提议 + 等待应用
  - 耗时: ~50ms
  - 总耗时: ~52ms

情况 3: 其他 Store 持有 lease (偶尔)
  - 返回 NotLeaseHolderError
  - 耗时: ~1ms
  - 跳过处理,继续下一个

统计 (500 个 Replica):
  - 450 个: 情况 1 (已持有 lease) → 450 × 2ms = 900ms
  - 30 个: 情况 2 (需要获取 lease) → 30 × 52ms = 1560ms
  - 20 个: 情况 3 (无法获取 lease) → 20 × 1ms = 20ms

总耗时: 900ms + 1560ms + 20ms = 2480ms ≈ 2.5 秒
实际处理: 480 个 (450 + 30)
```

### 4.3 Span Config 不可用的影响

**场景**: Store 启动早期,Span Config 系统尚未就绪

```
T0: Store.Start() 开始
T1: 创建各种 Queue
T2: Replica Scanner 开始扫描

T2~T7 (前 5 秒): SpanConfigSubscriber 正在初始化
  - GetConfReader() → errSpanConfigsUnavailable
  - 所有需要 span config 的 Queue 都无法入队:
    - Replicate Queue
    - Split Queue
    - Merge Queue
  - 只有不需要 span config 的 Queue 可以运行:
    - GC Queue
    - Raft Snapshot Queue

T7: SpanConfigSubscriber 初始化完成
  - GetConfReader() → 返回 StoreReader
  - Replicate Queue 开始正常工作
  - Split Queue 开始正常工作

影响:
  - 启动后前 5 秒: 无法执行 rebalancing 和 split
  - 但 GC 和 snapshot 可以正常进行
  - 不影响数据可用性 (已有的 Replica 仍然可以服务请求)
```

### 4.4 Range 分裂的级联效应

**场景**: 用户创建新 zone config,导致大量 Range 需要分裂

```
T0: 用户执行
  ALTER TABLE users CONFIGURE ZONE USING num_replicas = 5

T1: Span config 系统传播配置
  - 影响 100 个 Range

T2: Replicate Queue 扫描
  - 遇到这 100 个 Range
  - replicaCanBeProcessed() → NeedsSplit() → true
  - 全部跳过,返回 "split needed; skipping"

T3: Split Queue 扫描
  - 遇到这 100 个 Range
  - acceptsUnsplitRanges=true → 通过检查
  - shouldQueue() → true
  - 全部加入 Split Queue

T4: Split Queue 处理
  - 逐个处理,AdminSplit()
  - 每个 split 耗时 ~100ms
  - 100 个 split × 100ms = 10 秒

T5: Split 完成后
  - 原来 100 个 Range → 现在 200 个 Range
  - Replicate Queue 重新扫描
  - replicaCanBeProcessed() → NeedsSplit() → false
  - shouldQueue() → true (需要调整副本数)
  - 200 个 Range 加入 Replicate Queue

T6: Replicate Queue 处理
  - 将副本数从 3 增加到 5
  - 每个 Range 添加 2 个副本
  - 耗时: 200 × 2 × 500ms = 200 秒 ≈ 3.3 分钟

总耗时: 10 秒 (split) + 200 秒 (replicate) = 210 秒 ≈ 3.5 分钟
```

**关键设计**: 强制分裂优先

```
如果允许 Replicate Queue 处理未分裂的 Range:

问题:
  - Range [a, z) 跨越两个 zone
  - [a, m): 3 副本
  - [m, z): 5 副本
  - Replicate Queue 不知道该使用哪个配置
  - 可能做出错误决策

解决: 拒绝处理,等待 Split Queue
```

---

## 五、具体案例：完整的前置条件检查过程 (Concrete Example)

### 5.1 场景设置

**集群配置**:
- 3 个节点,每个节点 1 个 Store
- Store-1 上有 2000 个 Replica
- Replicate Queue 正在运行

**Range-100 的状态**:
```
RangeID: 100
ReplicaID: 1
Key Range: [/Table/users/1, /Table/users/1000)
Replicas: [(n1,s1):1, (n2,s2):2, (n3,s3):3]
Leaseholder: (n1,s1):1
Span Config: { NumReplicas: 3, Constraints: [...] }
```

### 5.2 案例 1: 正常入队和处理

#### 阶段 1: 入队时的检查

```
T0 = 10:00:00.000: Replica Scanner 扫描到 Range-100
T1 = 10:00:00.001: 调用 maybeAdd(Range-100, now)

T2 = 10:00:00.002: replicaCanBeProcessed(ctx, Range-100, false)
  → 参数: acquireLeaseIfNeeded=false

  检查 1: IsInitialized()
    → Range-100.mu.state.Desc.InternalReplicas
      = [(n1,s1):1, (n2,s2):2, (n3,s3):3]
    → len(InternalReplicas) = 3 > 0
    → 返回 true ✅

  检查 2: IsDestroyed()
    → Range-100.shMu.destroyStatus.Get()
    → 返回 (0, nil) ✅ (未销毁)

  检查 3: needsSpanConfigs=true
    → GetConfReader()
    → SpanConfigSubscriber.GetReader()
    → 返回 StoreReader ✅

  检查 4: acceptsUnsplitRanges=false
    → NeedsSplit([/Table/users/1, /Table/users/1000))
    → 查询 span config 边界
    → 只有一个 span config 覆盖整个 Range
    → 返回 false ✅

  检查 5: needsLease=true, acquireLeaseIfNeeded=false
    → CurrentLeaseStatus()
    → Lease { Replica: (n1,s1):1, Epoch: 5 }
    → OwnedBy(s1): true ✅

T3 = 10:00:00.003: 返回 (confReader, nil)

T4 = 10:00:00.004: shouldQueue(Range-100, confReader)
  → 检查副本分布
  → 所有 3 个副本都在不同节点
  → 返回 false, 0 (不需要处理)

T5 = 10:00:00.005: updateMetricsOnEnqueueNoAction()
  → metric.enqueueNoAction++

T6: maybeAdd() 返回,Range-100 未入队
```

**耗时**: ~5ms
**结果**: Range-100 状态正常,不需要 replicate

#### 阶段 2: 假设需要处理

**修改场景**: Store-2 故障,Range-100 需要 up-replicate

```
T0 = 10:01:00.000: Store-2 故障
T1 = 10:01:00.100: Liveness 检测到 Store-2 down
T2 = 10:01:10.000: Replica Scanner 再次扫描到 Range-100

T3 = 10:01:10.001: maybeAdd(Range-100, now)
T4 = 10:01:10.002: replicaCanBeProcessed(false)
  → 所有检查通过 (同上)
  → 返回 (confReader, nil)

T5 = 10:01:10.003: shouldQueue(Range-100, confReader)
  → 检查副本分布
  → Replica (n2,s2):2 失联
  → 只有 2 个有效副本 < 3
  → 返回 true, priority=1.0 (高优先级)

T6 = 10:01:10.004: addInternal()
  → 创建 replicaItem { rangeID: 100, priority: 1.0 }
  → heap.Push(&priorityQ, item)
  → metric.enqueueAdd++

T7 = 10:01:10.005: Range-100 成功入队
```

#### 阶段 3: 队列中等待

```
T8 = 10:01:10.005 ~ 10:01:15.000: 队列中等待
  - 前面有更高优先级的 Range (priority > 1.0)
  - 或者 processSem 已满 (并发处理数达到上限)
```

#### 阶段 4: 处理前的检查

```
T9 = 10:01:15.000: processLoop pop() Range-100
T10 = 10:01:15.001: processReplica(Range-100, priority=1.0)

T11 = 10:01:15.002: replicaCanBeProcessed(ctx, Range-100, true)
  → 参数: acquireLeaseIfNeeded=true

  检查 1: IsInitialized() → true ✅
  检查 2: IsDestroyed() → nil ✅
  检查 3: GetConfReader() → StoreReader ✅
  检查 4: NeedsSplit() → false ✅

  检查 5: needsLease=true, acquireLeaseIfNeeded=true
    → redirectOnOrAcquireLease()

    T11.1: CurrentLeaseStatus()
      → Lease { Replica: (n1,s1):1, Epoch: 5 }
      → OwnedBy(s1): true ✅

    T11.2: 已持有 lease,直接返回

    T11.3: maybeSwitchLeaseType()
      → Lease.Type() = LeaseEpoch (已经是 Epoch)
      → 返回 nil ✅

T12 = 10:01:15.003: 返回 (confReader, nil)

T13 = 10:01:15.004: impl.process(Range-100, confReader, priority=1.0)
  → allocator.AllocateVoter()
  → 选择 Store-4 作为新副本
  → AdminChangeReplicas([Add (n4,s4):4])
  → 发送 Raft 提议
  → 等待 Raft 日志应用
  → 等待 snapshot 传输完成
  → 耗时: ~500ms

T14 = 10:01:15.504: process() 返回 (processed=true, err=nil)
T15 = 10:01:15.505: finishProcessingReplica()
  → metric.successes++
  → removeFromReplicaSetLocked(Range-100)
```

**总耗时**:
- 入队检查: ~5ms
- 队列等待: ~5 秒
- 处理前检查: ~3ms
- 实际处理: ~500ms
- **总计**: ~5.5 秒

### 5.3 案例 2: Lease 丢失的处理

**场景**: Range-100 在入队和处理之间丢失了 lease

```
T0 = 10:02:00.000: Replica Scanner 扫描到 Range-100
T1 = 10:02:00.001: maybeAdd(Range-100, now)

T2 = 10:02:00.002: replicaCanBeProcessed(false)
  → CurrentLeaseStatus()
  → Lease { Replica: (n1,s1):1, Epoch: 5 }
  → OwnedBy(s1): true ✅
  → 返回 (confReader, nil)

T3 = 10:02:00.003: shouldQueue() → true, priority=0.5
T4 = 10:02:00.004: addInternal() → 成功入队

T5 = 10:02:05.000: Lease 被转移到 Store-2
  → 新 Lease: { Replica: (n2,s2):2, Epoch: 6 }
  → 原因: Load-based lease 转移

T6 = 10:02:10.000: processLoop pop() Range-100
T7 = 10:02:10.001: processReplica(Range-100, priority=0.5)

T8 = 10:02:10.002: replicaCanBeProcessed(true)
  → redirectOnOrAcquireLease()

  T8.1: CurrentLeaseStatus()
    → Lease { Replica: (n2,s2):2, Epoch: 6 }
    → OwnedBy(s1): false ❌

  T8.2: Lease 属于其他 Store
    → 返回 NotLeaseHolderError

  T8.3: 检查错误类型
    → case *kvpb.NotLeaseHolderError:
    → 返回 errMarkNotAcquirableLease

T9 = 10:02:10.003: processReplica() 收到 err
  → errors.Is(err, errMarkNotAcquirableLease) → true
  → 返回 nil (不是真正的错误)

T10 = 10:02:10.004: finishProcessingReplica(err=nil)
  → 不记录 metric.failures
  → 不记录错误日志
  → removeFromReplicaSetLocked(Range-100)
```

**结果**: Range-100 未处理,但不记录为 failure

**为什么不尝试获取 lease?**

```
场景分析:

如果尝试获取 lease:
  - 发送 RequestLease Raft 提议
  - 但 Store-2 已经持有 lease
  - 需要等待 lease 过期才能获取
  - 可能导致 lease 在两个 Store 之间来回转移

当前策略: 不竞争 lease
  - 如果其他 Store 持有 lease,跳过处理
  - 让其他 Store 的 Queue 来处理
  - 避免无谓的 lease 竞争
```

### 5.4 案例 3: Lease 过期后重新获取

**场景**: Range-100 的 lease 过期,需要重新获取

```
T0 = 10:03:00.000: Replica Scanner 扫描到 Range-100
T1 = 10:03:00.001: maybeAdd(Range-100, now)

T2 = 10:03:00.002: replicaCanBeProcessed(false)
  → CurrentLeaseStatus()
  → Lease { Replica: (n1,s1):1, Epoch: 5 }
  → 检查 liveness epoch
  → Store-1 的 liveness: { Epoch: 6, ... } (已更新)
  → Lease.Epoch (5) < Liveness.Epoch (6)
  → 返回 EXPIRED ❌

  → OwnedBy(s1): 虽然 Replica 是 (n1,s1):1,但 lease 已过期
  → 返回 benignerror("needs lease, not adding")

T3 = 10:03:00.003: maybeAdd() 返回,未入队
```

**如果 lease 已过期,如何入队?**

```
方案 1: Replica Scanner 触发 lease 续约
  - 每次扫描时,如果 lease 过期,主动获取
  - 问题: 扫描会变慢,影响其他 Queue

方案 2: 等待下次扫描
  - 10 分钟后再次扫描
  - 此时可能已经有请求触发了 lease 获取
  - Lease 已经续约,可以正常入队

实际: 使用方案 2
  - Replica Scanner 不获取 lease
  - 但客户端请求会触发 lease 获取
  - 下次扫描时 lease 通常已经有效
```

**如果通过 AddAsync 强制入队?**

```
某些场景下,外部代码主动调用 AddAsync:

T0: 检测到 Store-2 故障
T1: 遍历所有 Range,找到受影响的 Range-100
T2: AddAsync(Range-100, priority=2.0)
  → 直接调用 addInternal(),不调用 maybeAdd()
  → 绕过 replicaCanBeProcessed(false)
  → 直接加入队列

T3: processLoop pop() Range-100
T4: processReplica(Range-100)
T5: replicaCanBeProcessed(true)
  → redirectOnOrAcquireLease()
  → Lease 已过期 → requestLease()
  → 发送 RequestLease Raft 提议
  → 等待 ~50ms
  → 获取成功
  → 继续处理 ✅
```

---

## 六、设计权衡：为什么这样设计？(Trade-offs)

### 6.1 双重检查 vs 单次检查

**当前设计**: 入队时检查 + 处理前检查

```
优点:
  1. 减少队列占用: 不满足条件的 Replica 不入队
  2. 快速失败: 扫描时快速过滤,不浪费队列空间
  3. 处理前再确认: 确保状态仍然满足条件

缺点:
  1. 重复检查: 某些 Replica 被检查两次
  2. 代码重复: 两个调用点需要维护一致性
```

**替代方案 1**: 只在处理前检查

```go
func (bq *baseQueue) maybeAdd(...) {
    // 不调用 replicaCanBeProcessed
    should, priority := bq.impl.shouldQueue(...)
    if should {
        bq.addInternal(..., priority)
    }
}

func (bq *baseQueue) processReplica(...) {
    conf, err := bq.replicaCanBeProcessed(true)
    if err != nil {
        return err
    }
    // 继续处理
}
```

**问题**:

```
场景: Store 有 2000 个 Replica,其中 500 个不持有 lease

不检查直接入队:
  - shouldQueue() 调用 2000 次
  - 其中 500 个不持有 lease,但仍然调用 shouldQueue()
  - shouldQueue() 可能很复杂(如 Replicate Queue 需要查询 allocator)
  - 浪费: 500 × 100μs = 50ms

处理时才发现不持有 lease:
  - 队列中有 500 个不持有 lease 的 Replica
  - 占用队列空间,挤掉真正需要处理的 Replica
  - 处理时发现错误,又要移除
  - 浪费队列槽位
```

**替代方案 2**: 只在入队时检查

```go
func (bq *baseQueue) maybeAdd(...) {
    conf, err := bq.replicaCanBeProcessed(false)
    if err != nil {
        return
    }
    should, priority := bq.impl.shouldQueue(...)
    if should {
        bq.addInternal(..., priority)
    }
}

func (bq *baseQueue) processReplica(...) {
    // 不调用 replicaCanBeProcessed
    processed, err := bq.impl.process(...)
    return err
}
```

**问题**:

```
场景: Replica 入队后,lease 被转移

T0: maybeAdd() 时持有 lease → 入队
T0 + 10秒: Lease 转移到其他 Store
T0 + 15秒: processReplica()
  → 直接调用 impl.process()
  → AdminChangeReplicas() 返回 NotLeaseHolderError
  → 处理失败,记录 metric.failures

问题:
  - 无法区分"预期的失败"和"真正的错误"
  - 所有 NotLeaseHolderError 都记录为 failures
  - 日志噪音,误导监控
```

**为什么选择双重检查?**

```
平衡考虑:
  1. 入队检查 (acquireLeaseIfNeeded=false):
     - 快速过滤,减少队列占用
     - 不阻塞扫描,保持扫描速度

  2. 处理前检查 (acquireLeaseIfNeeded=true):
     - 再次确认,处理时间差导致的状态变化
     - 主动获取 lease,确保能成功处理
     - 区分预期失败和真正错误

成本:
  - 重复检查: 对于最终处理的 Replica,检查两次
  - 但检查很快 (~1ms),相比处理时间 (~500ms) 可忽略
```

### 6.2 主动获取 Lease vs 被动检查

**当前设计**: 处理前主动获取 lease

```
acquireLeaseIfNeeded=true:
  - redirectOnOrAcquireLease()
  - 如果 lease 过期,主动发送 RequestLease
  - 等待 lease 获取成功后再处理
```

**替代方案**: 被动检查,不获取

```go
func (bq *baseQueue) replicaCanBeProcessed(...) {
    if bq.needsLease {
        st := repl.CurrentLeaseStatus(ctx)
        if !st.IsValid() || !st.OwnedBy(repl.StoreID()) {
            return nil, errors.New("needs lease")
        }
    }
    return confReader, nil
}
```

**对比**:

| 方案 | 优点 | 缺点 |
|------|------|------|
| 主动获取 | 处理成功率高,一次获取多次使用 | 可能导致 lease 竞争 |
| 被动检查 | 不竞争 lease,简单 | 处理失败率高,浪费队列槽位 |

**主动获取的场景**:

```
T0: Replica Scanner 扫描,发现需要处理
T1: maybeAdd() → lease 有效 → 入队
T2 (10 秒后): lease 过期
T3 (15 秒后): processReplica() → 主动获取 lease
T4: 获取成功 → 处理

如果被动检查:
  T3: processReplica() → lease 无效 → 返回错误
  T4: Replica 从队列移除
  T5 (10 分钟后): 再次扫描 → 再次入队
  T6: 如果 lease 仍然无效 → 又失败

问题: 反复入队失败,浪费资源
```

**主动获取的问题**: Lease 竞争

```
场景: 两个 Store 同时尝试获取 lease

Store-1:
  T0: processReplica() → requestLease()
  T1: 发送 RequestLease (Epoch=6)

Store-2:
  T0.5: processReplica() → requestLease()
  T1.5: 发送 RequestLease (Epoch=6)

Raft 裁决:
  T2: Store-1 的提议先到达 leader
  T3: Raft 应用 Store-1 的 lease
  T4: Store-2 的提议被拒绝 (已有更新的 lease)

结果:
  - Store-1: 获取成功,继续处理 ✅
  - Store-2: 获取失败,返回 NotLeaseHolderError ❌
    → 但被标记为 errMarkNotAcquirableLease
    → 不记录 failure ✅
```

**为什么选择主动获取?**

```
权衡:
  1. 处理成功率: 主动获取提高成功率
  2. Lease 竞争: 虽然可能竞争,但 Raft 会裁决
  3. 错误处理: NotLeaseHolderError 被特殊处理,不记录 failure

实际观察:
  - 大部分情况下,Replica 在处理前仍持有 lease (快速路径)
  - 少数情况需要获取 lease (~5%)
  - 极少数情况会竞争 (<1%)

结论: 主动获取的收益大于成本
```

### 6.3 配置化条件 vs 硬编码

**当前设计**: 通过 `queueConfig` 配置条件

```go
type queueConfig struct {
    needsLease           bool
    needsSpanConfigs     bool
    acceptsUnsplitRanges bool
    processDestroyedReplicas bool
}
```

**替代方案 1**: 每个 Queue 实现自己的检查

```go
// Replicate Queue
func (rq *replicateQueue) canProcess(repl *Replica) error {
    if !repl.IsInitialized() {
        return errors.New("not initialized")
    }
    if !repl.OwnsLease() {
        return errors.New("needs lease")
    }
    // ... 其他检查
}

// GC Queue
func (gcq *gcQueue) canProcess(repl *Replica) error {
    if !repl.IsInitialized() {
        return errors.New("not initialized")
    }
    // 不检查 lease
    // ... 其他检查
}
```

**问题**:

```
代码重复:
  - IsInitialized() 检查在每个 Queue 中重复
  - IsDestroyed() 检查在每个 Queue 中重复
  - 维护成本高

不一致风险:
  - 某个 Queue 忘记检查 IsDestroyed()
  - 导致 bug,难以发现

测试困难:
  - 需要为每个 Queue 编写测试
  - 测试覆盖率难以保证
```

**替代方案 2**: 全局硬编码

```go
func (bq *baseQueue) replicaCanBeProcessed(...) {
    // 所有 Queue 都检查所有条件
    if !repl.IsInitialized() { ... }
    if reason, err := repl.IsDestroyed(); err != nil { ... }
    if _, err := bq.store.GetConfReader(); err != nil { ... }
    if needsSplit { ... }
    if !repl.OwnsLease() { ... }
}
```

**问题**:

```
过度约束:
  - GC Queue 不需要 lease,但被强制检查
  - Raft Snapshot Queue 不需要 span config,但被强制检查
  - 导致某些 Queue 无法正常工作

启动依赖:
  - 如果 span config 系统初始化慢
  - 所有 Queue 都被阻塞
  - 包括不需要 span config 的 GC Queue
```

**为什么选择配置化?**

```
灵活性:
  - 每个 Queue 根据需要配置条件
  - GC Queue: needsLease=false, needsSpanConfigs=false
  - Replicate Queue: needsLease=true, needsSpanConfigs=true

复用性:
  - baseQueue 实现通用检查逻辑
  - 所有 Queue 共享,避免重复

可测试性:
  - 测试 baseQueue 一次
  - 所有 Queue 自动受益

可扩展性:
  - 新增 Queue 只需要配置 queueConfig
  - 不需要实现检查逻辑
```

### 6.4 同步检查 vs 异步检查

**当前设计**: 同步检查

```go
func (bq *baseQueue) replicaCanBeProcessed(...) {
    // 所有检查都是同步的
    if !repl.IsInitialized() { ... }
    if reason, err := repl.IsDestroyed(); err != nil { ... }
    // ...
    return confReader, nil
}
```

**替代方案**: 异步检查

```go
func (bq *baseQueue) replicaCanBeProcessedAsync(...) <-chan error {
    errCh := make(chan error, 1)
    go func() {
        // 异步执行所有检查
        if !repl.IsInitialized() {
            errCh <- errors.New("not initialized")
            return
        }
        // ...
        errCh <- nil
    }()
    return errCh
}
```

**对比**:

| 方案 | 优点 | 缺点 |
|------|------|------|
| 同步 | 简单,快速 (1μs),易调试 | 阻塞调用者 |
| 异步 | 不阻塞调用者 | 复杂,goroutine 开销 (~2μs),难调试 |

**为什么选择同步?**

```
性能分析:

同步检查耗时:
  - IsInitialized(): ~100ns
  - IsDestroyed(): ~100ns
  - CurrentLeaseStatus(): ~500ns
  - GetConfReader(): ~200ns
  - NeedsSplit(): ~100ns (缓存命中)
  - 总计: ~1μs

异步检查开销:
  - 创建 goroutine: ~2μs
  - 通道通信: ~1μs
  - 总计: ~3μs + 1μs (实际检查) = 4μs

结论: 异步检查反而更慢!

复杂性:
  - 异步需要处理 goroutine 泄漏
  - 需要处理超时
  - 需要处理取消
  - 错误处理更复杂

简单性胜出: 同步检查足够快,更简单
```

---

## 七、核心总结：replicaCanBeProcessed 的本质 (Summary)

### 7.1 一句话总结

**`replicaCanBeProcessed` 是 Queue 处理流程的守门员(Gatekeeper),它通过 6 层前置条件检查,确保 Replica 在入队和处理时都满足必要条件,同时根据 `acquireLeaseIfNeeded` 参数区分"快速过滤"和"确保成功"两种模式,在不同阶段提供不同程度的保障,避免浪费队列资源和处理时间。**

### 7.2 核心机制

**6 层守卫检查**:

```
第 1 层: Replica 初始化状态
  - IsInitialized(): 是否有完整的 RangeDescriptor
  - 防止: 处理未完成 snapshot 的 Replica

第 2 层: Replica 销毁状态
  - IsDestroyed(): 是否已被标记为销毁
  - 特殊处理: GC Queue 可以处理 destroyReasonMergePending

第 3 层: Span Config 可用性
  - GetConfReader(): span config 系统是否就绪
  - 条件化: 只有 needsSpanConfigs=true 的 Queue 才检查

第 4 层: Range 分裂需求
  - NeedsSplit(): 是否跨越多个 span config 边界
  - 条件化: 只有 acceptsUnsplitRanges=false 的 Queue 才检查

第 5层: Lease 持有/获取
  - acquireLeaseIfNeeded=false: CurrentLeaseStatus() (被动检查)
  - acquireLeaseIfNeeded=true: redirectOnOrAcquireLease() (主动获取)

第 6 层: Lease 类型切换
  - maybeSwitchLeaseType(): 从 Expiration 切换到 Epoch (如果需要)
```

**两种模式**:

```
模式 1: 入队时 (acquireLeaseIfNeeded=false)
  目标: 快速过滤,不浪费队列空间
  行为: 只检查当前状态,不主动获取 lease
  耗时: ~1μs
  失败: 返回 benignerror,不记录 failure

模式 2: 处理前 (acquireLeaseIfNeeded=true)
  目标: 确保成功,值得花费时间
  行为: 重新检查所有状态,主动获取 lease
  耗时: ~1μs (快速路径) 或 ~50ms (需要获取 lease)
  失败: 区分 errMarkNotAcquirableLease 和真正错误
```

### 7.3 心智模型

**比喻: 机场安检的两道检查**

```
第一道检查 (值机柜台 = maybeAdd):
  - 检查护照、签证 (replicaCanBeProcessed(false))
  - 目的: 不让不合格的人进入候机区
  - 不通过: 不发登机牌,不占用座位
  - 通过: 发登机牌,进入候机区 (入队)

第二道检查 (登机口 = processReplica):
  - 再次检查护照、签证 (replicaCanBeProcessed(true))
  - 目的: 确保仍然合格 (可能签证过期了)
  - 如果签证过期: 主动续签 (获取 lease)
  - 不通过: 不登机,但不算"违规"
  - 通过: 登机 (处理)
```

**核心类比**:

| 机场流程 | Queue 流程 |
|---------|-----------|
| 护照 | IsInitialized() |
| 签证有效期 | Lease 状态 |
| 值机柜台 | maybeAdd() |
| 候机区 | priorityQueue |
| 登机口 | processReplica() |
| 续签 | requestLease() |
| 不发登机牌 | 不入队 |
| 拒绝登机 | 返回 errMarkNotAcquirableLease |

### 7.4 关键代码位置

| 功能 | 文件 | 行号 |
|------|------|------|
| replicaCanBeProcessed | pkg/kv/kvserver/queue.go | 1275-1358 |
| 入队调用 (false) | pkg/kv/kvserver/queue.go | 876 |
| 处理前调用 (true) | pkg/kv/kvserver/queue.go | 1236 |
| queueConfig 定义 | pkg/kv/kvserver/queue.go | 366-441 |
| IsInitialized | pkg/roachpb/metadata.go | - |
| IsDestroyed | pkg/kv/kvserver/replica.go | - |
| redirectOnOrAcquireLease | pkg/kv/kvserver/replica.go | - |

### 7.5 设计亮点

1. **双重检查模式**: 入队时快速过滤,处理前再确认,平衡效率和准确性

2. **条件化检查**: 通过 `queueConfig` 配置,每个 Queue 只检查必要的条件

3. **Lease 获取策略**: 入队时被动检查,处理前主动获取,避免无谓竞争

4. **错误分类**: `errMarkNotAcquirableLease` 和 `benignerror` 区分预期失败和真正错误

5. **Span Config 容错**: 启动早期 span config 不可用时,只阻塞需要它的 Queue

6. **销毁状态特殊处理**: destroyReasonMergePending 允许 GC Queue 处理,其他 Queue 拒绝

### 7.6 局限性

1. **时间差窗口**: 入队和处理之间的时间差,可能导致状态变化

2. **Lease 竞争**: 多个 Store 同时尝试获取 lease,虽然 Raft 会裁决,但仍有开销

3. **重复检查**: 最终处理的 Replica 被检查两次,虽然检查很快,但仍有冗余

4. **硬编码顺序**: 6 个检查的顺序是硬编码的,无法根据具体 Queue 优化

**缓解措施**:
- 时间差: 处理前再次检查,确保状态仍然有效
- Lease 竞争: NotLeaseHolderError 被特殊处理,不记录 failure
- 重复检查: 检查耗时 ~1μs,相比处理耗时可忽略
- 硬编码顺序: 当前顺序是经过优化的(快速检查在前)

---

**本章完**

通过本章分析,我们深入理解了 CockroachDB Queue 系统的前置条件验证机制:`replicaCanBeProcessed` 通过 6 层守卫检查,在入队和处理两个阶段提供不同程度的保障。入队时使用"快速过滤"模式,避免不合格的 Replica 占用队列空间;处理前使用"确保成功"模式,主动获取 lease 并再次确认所有条件,最大化处理成功率。这种双重检查 + 条件化配置的设计,在灵活性、性能和可靠性之间取得了良好的平衡,是 CockroachDB 分布式 Queue 系统的核心保障机制之一。
