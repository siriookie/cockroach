# Raft Log Truncator 深度剖析——基于持久化感知的松耦合 Raft 日志截断机制（下篇）

> **接上篇**: 本文继续深入分析 **Raft Log Truncator** 的设计与实现，重点讲解截断执行逻辑、队列管理、运行时行为、设计模式及工程权衡。

---

## 三、DFS 深入：关键函数与核心逻辑（续）

### 3.5 核心方法：`tryEnactTruncations()`

这是执行实际截断的核心方法，逻辑复杂且关键。

```go
func (t *raftLogTruncator) tryEnactTruncations(
    ctx context.Context, rangeID roachpb.RangeID, reader storage.Reader,
) {
    // 1. 获取 Replica（加锁 raftMu）
    r := t.store.acquireReplicaForTruncator(rangeID)
    if r == nil {
        // Replica 已被销毁或不存在
        return
    }
    defer t.store.releaseReplicaForTruncator(r)

    // 2. 获取当前截断状态
    truncState := r.getTruncatedState()
    pendingTruncs := r.getPendingTruncs()

    // 3. 移除 noop 截断（已被 snapshot 超越）
    pendingTruncs.mu.Lock()
    for !pendingTruncs.isEmptyLocked() {
        pendingTrunc := pendingTruncs.frontLocked()
        if pendingTrunc.Index <= truncState.Index {
            pendingTruncs.popLocked()
        } else {
            break
        }
    }
    pendingTruncs.mu.Unlock()

    if pendingTruncs.isEmptyLocked() {
        return
    }

    // 4. 读取持久化的 RaftAppliedIndex
    stateLoader := r.getStateLoader()
    as, err := stateLoader.LoadRangeAppliedState(ctx, reader)
    if err != nil {
        log.KvExec.Errorf(ctx, "error loading RangeAppliedState: %s", err)
        pendingTruncs.reset()
        return
    }

    // 5. 找到可以执行的截断
    enactIndex := -1
    pendingTruncs.iterateLocked(func(index int, trunc pendingTruncation) {
        if trunc.Index > as.RaftAppliedIndex {
            return  // 尚未持久化，停止
        }
        enactIndex = index
    })

    if enactIndex < 0 {
        // 无法执行任何截断，重新入队等待下次
        t.enqueueRange(rangeID)
        return
    }

    // 6. 执行截断
    batch := t.store.getEngine().NewWriteBatch()
    defer batch.Close()

    if err := handleTruncatedStateBelowRaftPreApply(ctx, truncState,
        pendingTruncs.mu.truncs[enactIndex].RaftTruncatedState,
        stateLoader.StateLoader, batch,
    ); err != nil {
        log.KvExec.Errorf(ctx, "while attempting to truncate raft log: %+v", err)
        pendingTruncs.reset()
        return
    }

    // 7. 更新 Replica 状态（两阶段）
    pendingTruncs.iterateLocked(func(index int, trunc pendingTruncation) {
        if index <= enactIndex {
            r.stagePendingTruncation(ctx, trunc)
        }
    })

    // 8. 提交 WriteBatch
    sync := pendingTruncs.mu.truncs[enactIndex].hasSideloaded
    if err := batch.Commit(sync); err != nil {
        log.KvExec.Fatalf(ctx, "while committing batch to truncate raft log: %+v", err)
        return
    }

    // 9. 完成截断（释放 sideloaded 文件等）
    r.finalizeTruncation(ctx)

    // 10. 从队列中移除已执行的截断
    pendingTruncs.mu.Lock()
    for i := 0; i <= enactIndex; i++ {
        pendingTruncs.popLocked()
    }
    pendingTruncs.mu.Unlock()

    // 11. 如果还有未执行的截断，重新入队
    if !pendingTruncs.isEmptyLocked() {
        t.enqueueRange(rangeID)
    }
}
```

**逐段深入分析**：

#### Part 1: 获取 Replica（带锁保护）

```go
r := t.store.acquireReplicaForTruncator(rangeID)
if r == nil {
    return
}
defer t.store.releaseReplicaForTruncator(r)
```

**acquireReplicaForTruncator 的实现** (`store.go:4412-4431`):

```go
func (s *storeForTruncatorImpl) acquireReplicaForTruncator(
    rangeID roachpb.RangeID,
) replicaForTruncator {
    r, err := (*Store)(s).GetReplica(rangeID)
    if err != nil || r == nil {
        return nil
    }

    r.raftMu.Lock()  // 关键：锁定 raftMu

    // 检查 Replica 是否存活
    if isAlive := func() bool {
        r.mu.Lock()
        defer r.mu.Unlock()
        return r.shMu.destroyStatus.IsAlive()
    }(); !isAlive {
        r.raftMu.Unlock()
        return nil
    }

    return (*raftTruncatorReplica)(r)
}
```

**关键设计点**：

1. **为什么锁定 raftMu**？
   ```go
   // raftMu 保护的状态：
   r.raftMu.Lock()
       ├─ Raft log 相关状态（truncated state, pending truncations）
       ├─ 防止并发的 Raft 消息处理
       └─ 防止 Replica 在截断过程中被销毁
   ```

2. **双重检查模式**：
   ```go
   GetReplica()  // 第一次检查
   →  raftMu.Lock()
   →  检查 destroyStatus  // 第二次检查
   ```
   - **TOCTOU 问题**（Time-of-Check-Time-of-Use）：
     ```
     时间线：
     T0: GetReplica() 成功
     T1: 另一个 goroutine 开始销毁 Replica
     T2: raftMu.Lock()  → 阻塞在这里
     T3: 销毁完成
     T4: raftMu.Lock() 获取锁
     T5: 检查 destroyStatus → 发现已销毁
     ```
   - **解决方案**：锁内再次检查状态

3. **类型转换的巧妙之处**：
   ```go
   return (*raftTruncatorReplica)(r)
   ```
   - `raftTruncatorReplica` 是 `Replica` 的新类型定义
   - 为 Replica 添加 truncator 专用方法集
   - **Adapter Pattern（适配器模式）**

#### Part 2: 移除 Noop 截断

```go
pendingTruncs.mu.Lock()
for !pendingTruncs.isEmptyLocked() {
    pendingTrunc := pendingTruncs.frontLocked()
    if pendingTrunc.Index <= truncState.Index {
        pendingTruncs.popLocked()
    } else {
        break
    }
}
pendingTruncs.mu.Unlock()
```

**场景**：
```
初始状态：
    truncState.Index = 1000
    pendingTruncs[0] = {Index: 800}   // Noop（已被超越）
    pendingTruncs[1] = {Index: 1200}

为什么会出现这种情况？
    T0: 提议截断到 800，加入队列
    T1: 等待持久化...
    T2: 收到 Snapshot，直接截断到 1000
        └─ truncState.Index = 1000
    T3: durabilityAdvanced() 被调用
        └─ 发现 pendingTruncs[0].Index (800) <= truncState.Index (1000)
        └─ 这是 Noop，直接移除

移除后：
    pendingTruncs[0] = {Index: 1200}
```

**不变量**：
```
INV-1: 移除后，队列中所有截断的 Index > truncState.Index
       → 保证不会重复截断

INV-2: 移除操作是幂等的
       → 多次调用结果相同
```

#### Part 3: 读取持久化状态（关键安全检查）

```go
stateLoader := r.getStateLoader()
as, err := stateLoader.LoadRangeAppliedState(ctx, reader)
if err != nil {
    log.KvExec.Errorf(ctx, "error loading RangeAppliedState: %s", err)
    pendingTruncs.reset()  // 出错时清空所有待截断
    return
}
```

**LoadRangeAppliedState 的语义**：
```go
type RangeAppliedState struct {
    RaftAppliedIndex     uint64  // 已应用到状态机的最大 index
    LeaseAppliedIndex    uint64
    RaftClosedTimestamp  hlc.Timestamp
    // ...
}
```

**为什么使用传入的 Reader**？
```go
// reader 是 GuaranteedDurability Reader
as, err := stateLoader.LoadRangeAppliedState(ctx, reader)
```

**对比**：
```go
// 如果使用普通 Reader：
normalReader := engine.NewReader(storage.StandardDurability)
as, _ := stateLoader.LoadRangeAppliedState(ctx, normalReader)
// 可能读到 memtable 中的新值（尚未持久化）

// 使用 GuaranteedDurability Reader：
durableReader := engine.NewReader(storage.GuaranteedDurability)
as, _ := stateLoader.LoadRangeAppliedState(ctx, durableReader)
// 只读取 SST 中的值（已持久化）
```

**安全性证明**：
```
定理：如果 as.RaftAppliedIndex = N（通过 GuaranteedDurability Reader 读取），
      则 [1, N] 的所有日志条目应用已持久化。

证明：
1. RaftAppliedIndex 存储在 StateEngine 中
2. GuaranteedDurability Reader 只读取已 flush 的数据
3. 如果 RaftAppliedIndex = N 已 flush，说明所有 ≤ N 的应用已持久化
4. 因此截断 [1, M] (M ≤ N) 是安全的  ∎
```

#### Part 4: 找到可执行的截断

```go
enactIndex := -1
pendingTruncs.iterateLocked(func(index int, trunc pendingTruncation) {
    if trunc.Index > as.RaftAppliedIndex {
        return  // 尚未持久化
    }
    enactIndex = index
})

if enactIndex < 0 {
    t.enqueueRange(rangeID)  // 重新入队
    return
}
```

**决策逻辑**：
```
场景 1：所有截断都可执行
    as.RaftAppliedIndex = 5500 (持久化)
    pendingTruncs[0] = {Index: 5000}
    pendingTruncs[1] = {Index: 5200}

    决策：
    enactIndex = 1  → 执行到 pendingTruncs[1]

场景 2：部分可执行
    as.RaftAppliedIndex = 5100 (持久化)
    pendingTruncs[0] = {Index: 5000}
    pendingTruncs[1] = {Index: 5200}

    决策：
    enactIndex = 0  → 只执行 pendingTruncs[0]
                    → pendingTruncs[1] 留待下次

场景 3：都无法执行
    as.RaftAppliedIndex = 4900 (持久化)
    pendingTruncs[0] = {Index: 5000}

    决策：
    enactIndex = -1  → 重新入队，等待下次持久化
```

**为什么重新入队而不是丢弃**？
```go
if enactIndex < 0 {
    t.enqueueRange(rangeID)  // 保留在队列中
    return
}
```
- ✅ **持久化是连续的**：下次 flush 时 AppliedIndex 会前进
- ✅ **避免丢失截断**：如果丢弃，日志会永久增长
- ✅ **自动重试**：下次 `durabilityAdvanced()` 会再次尝试

#### Part 5: 执行截断（两阶段提交）

**Phase 1: 准备 WriteBatch**

```go
batch := t.store.getEngine().NewWriteBatch()
defer batch.Close()

if err := handleTruncatedStateBelowRaftPreApply(ctx, truncState,
    pendingTruncs.mu.truncs[enactIndex].RaftTruncatedState,
    stateLoader.StateLoader, batch,
); err != nil {
    log.KvExec.Errorf(ctx, "while attempting to truncate raft log: %+v", err)
    pendingTruncs.reset()
    return
}
```

**handleTruncatedStateBelowRaftPreApply 的作用**：
```go
// 简化逻辑：
func handleTruncatedStateBelowRaftPreApply(
    ctx context.Context,
    oldTruncState kvserverpb.RaftTruncatedState,
    newTruncState kvserverpb.RaftTruncatedState,
    stateLoader kvstorage.StateLoader,
    batch storage.WriteBatch,
) error {
    // 1. 删除 [oldIndex+1, newIndex] 的 Raft log entries
    for i := oldTruncState.Index + 1; i <= newTruncState.Index; i++ {
        batch.ClearUnversioned(keys.RaftLogKey(rangeID, i))
    }

    // 2. 更新 RaftTruncatedState 元数据
    batch.PutUnversioned(keys.RaftTruncatedStateKey(rangeID), newTruncState)

    return nil
}
```

**Phase 2: 更新 Replica 状态**

```go
pendingTruncs.iterateLocked(func(index int, trunc pendingTruncation) {
    if index <= enactIndex {
        r.stagePendingTruncation(ctx, trunc)
    }
})
```

**stagePendingTruncation 的作用** (`raft_truncator_replica.go:32-34`):
```go
func (r *raftTruncatorReplica) stagePendingTruncation(_ context.Context, pt pendingTruncation) {
    (*Replica)(r).stagePendingTruncationRaftMuLocked(pt)
}

// 实际实现（在 Replica 中）：
func (r *Replica) stagePendingTruncationRaftMuLocked(pt pendingTruncation) {
    // 更新 Raft log 大小估算
    r.mu.Lock()
    defer r.mu.Unlock()

    r.mu.raftLogSize += pt.logDeltaBytes  // 通常是负数（减少）
    if !pt.isDeltaTrusted {
        r.mu.raftLogSizeTrusted = false
    }
}
```

**为什么是两阶段**？
```
Phase 1: stagePendingTruncation()
    └─ 更新内存状态（raftLogSize 等）
    └─ 此时 WriteBatch 尚未提交

Phase 2: finalizeTruncation() (在 Commit 之后)
    └─ 释放 sideloaded 文件
    └─ 更新截断状态
    └─ 此时 WriteBatch 已提交

如果只有一阶段：
    风险：Commit 失败后，内存状态已修改
    → 状态不一致

两阶段的好处：
    ✓ Commit 前只做可逆的修改
    ✓ Commit 后做不可逆的操作（删除文件）
    ✓ 保证一致性
```

**Phase 3: 提交 WriteBatch**

```go
sync := pendingTruncs.mu.truncs[enactIndex].hasSideloaded
if err := batch.Commit(sync); err != nil {
    log.KvExec.Fatalf(ctx, "while committing batch to truncate raft log: %+v", err)
    return
}
```

**sync 参数的关键决策**：
```go
sync := pendingTruncs.mu.truncs[enactIndex].hasSideloaded
```

**为什么需要 sync**？
```
场景：截断的日志包含 sideloaded entries（例如 Snapshot）

T0: batch.Commit(sync=false)
    └─ WriteBatch 写入 memtable
    └─ 但尚未 fsync

T1: finalizeTruncation()
    └─ 删除 sideloaded 文件（不可逆）

T2: 系统崩溃

T3: 重启
    └─ WriteBatch 丢失（memtable 未刷盘）
    └─ 截断元数据丢失
    └─ 但 sideloaded 文件已删除
    └─ 致命错误：Raft 认为 log 存在，但文件缺失

解决方案：
    如果 hasSideloaded → sync=true (强制 fsync)
    └─ 保证 WriteBatch 持久化后再删除文件
```

**性能权衡**：
```
无 sideloaded (常见情况):
    sync = false
    └─ 无额外 fsync 开销（松耦合截断的核心优势）

有 sideloaded (罕见情况):
    sync = true
    └─ 需要 fsync（1-10ms 延迟）
    └─ 但这是必需的，无法避免
```

**Phase 4: 完成截断**

```go
r.finalizeTruncation(ctx)
```

**finalizeTruncation 的作用** (`raft_truncator_replica.go:36-38`):
```go
func (r *raftTruncatorReplica) finalizeTruncation(ctx context.Context) {
    (*Replica)(r).finalizeTruncationRaftMuLocked(ctx)
}

// 实际实现（简化）：
func (r *Replica) finalizeTruncationRaftMuLocked(ctx context.Context) {
    // 1. 删除 sideloaded 文件
    if hasSideloaded {
        r.logStorage.ls.Sideload.TruncateTo(ctx, newTruncIndex)
    }

    // 2. 更新截断状态
    r.mu.Lock()
    r.mu.state.TruncatedState = newTruncState
    r.mu.Unlock()

    // 3. 通知 Raft
    r.mu.internalRaftGroup.AdvanceSubsumedIndex(newTruncIndex)
}
```

#### Part 6: 清理队列并重新入队

```go
pendingTruncs.mu.Lock()
for i := 0; i <= enactIndex; i++ {
    pendingTruncs.popLocked()
}
pendingTruncs.mu.Unlock()

if !pendingTruncs.isEmptyLocked() {
    t.enqueueRange(rangeID)
}
```

**队列操作**：
```
执行前：
    pendingTruncs[0] = {Index: 5000}  ← enactIndex = 1
    pendingTruncs[1] = {Index: 5200}  ← 已执行

执行 popLocked() 两次：
    第一次 pop: pendingTruncs[0] = pendingTruncs[1] = {Index: 5200}
                pendingTruncs[1] = {}
    第二次 pop: pendingTruncs[0] = {}

执行后：
    pendingTruncs = 空队列
```

**为什么重新入队**？
```go
if !pendingTruncs.isEmptyLocked() {
    t.enqueueRange(rangeID)
}
```
- **场景**：只执行了 `pendingTruncs[0]`，`pendingTruncs[1]` 仍待处理
- **策略**：重新加入 `addRanges`，等待下次持久化
- **保证**：所有截断最终都会被执行

### 3.6 队列管理：`pendingLogTruncations`

**核心结构** (`raft_log_truncator.go:42-70`):

```go
type pendingLogTruncations struct {
    mu struct {
        syncutil.Mutex
        // 固定大小：最多 2 个条目
        truncs [2]pendingTruncation
    }
}
```

**为什么固定为 2**？

这是一个精妙的设计，值得深入分析：

#### 设计意图

```
问题：如果队列无限大会怎样？

场景：
    T0: 提议截断到 1000 (RaftAppliedIndex = 800)
    T1: 提议截断到 2000 (RaftAppliedIndex = 800)
    T2: 提议截断到 3000 (RaftAppliedIndex = 800)
    ...
    T100: 提议截断到 100000 (RaftAppliedIndex 仍是 800)

队列状态：
    truncs[0] = 1000
    truncs[1] = 2000
    ...
    truncs[99] = 100000

问题：
    ✗ truncs[0] 永远无法执行（RaftAppliedIndex 卡在 800）
    ✗ 后续所有截断都被阻塞
    ✗ 日志永久增长
```

#### 解决方案：最多 2 个条目 + 合并策略

```go
type pendingLogTruncations struct {
    mu struct {
        truncs [2]pendingTruncation
        // truncs[0] = 最老的截断（等待持久化）
        // truncs[1] = 所有后续截断的合并
    }
}
```

**合并逻辑** (`raft_log_truncator.go:270-351`):

```go
func (t *raftLogTruncator) addPendingTruncation(
    ctx context.Context,
    r replicaForTruncator,
    trunc kvserverpb.RaftTruncatedState,
    raftExpectedFirstIndex kvpb.RaftIndex,
    raftLogDelta int64,
) {
    pendingTrunc := pendingTruncation{
        RaftTruncatedState: trunc,
        expectedFirstIndex: raftExpectedFirstIndex,
        logDeltaBytes:      raftLogDelta,
        isDeltaTrusted:     true,
    }

    pendingTruncs := r.getPendingTruncs()

    // 计算当前已截断的 index
    alreadyTruncIndex := r.getTruncatedState().Index
    pendingTruncs.iterateLocked(func(index int, trunc pendingTruncation) {
        if trunc.Index > alreadyTruncIndex {
            alreadyTruncIndex = trunc.Index
        }
    })

    if alreadyTruncIndex >= pendingTrunc.Index {
        // Noop 截断，直接丢弃
        return
    }

    // 决定加入位置
    i := -1
    pendingTruncs.iterateLocked(func(index int, trunc pendingTruncation) {
        i = index
    })
    pos := i + 1  // 队列末尾

    mergeWithPending := false
    if pos == pendingTruncs.capacity() {  // capacity() = 2
        // 队列已满，需要合并
        pos--
        mergeWithPending = true
    }

    // 计算 sideloaded entries 大小
    if entries, size, err := r.sideloadedStats(ctx, kvpb.RaftSpan{
        After: alreadyTruncIndex, Last: pendingTrunc.Index,
    }); err != nil {
        log.KvExec.Errorf(ctx, "while computing sideloaded files: %+v", err)
        pendingTrunc.isDeltaTrusted = false
    } else if entries != 0 {
        pendingTrunc.logDeltaBytes -= size
        pendingTrunc.hasSideloaded = true
    }

    if mergeWithPending {
        // 合并：将现有条目合并到新条目中
        pendingTrunc = pendingTrunc.merge(pendingTruncs.mu.truncs[pos])
    }

    pendingTruncs.mu.Lock()
    pendingTruncs.mu.truncs[pos] = pendingTrunc
    pendingTruncs.mu.Unlock()

    if pos == 0 {
        // 队列从空变为非空，加入全局待处理集合
        t.enqueueRange(r.getRangeID())
    }
}
```

**合并示例**：

```
场景：连续收到 4 个截断请求

T0: 初始状态
    truncs[0] = {}
    truncs[1] = {}

T1: 添加截断到 1000
    truncs[0] = {Index: 1000, delta: -50MB}
    truncs[1] = {}
    └─ pos = 0 → enqueueRange()

T2: 添加截断到 2000
    truncs[0] = {Index: 1000, delta: -50MB}
    truncs[1] = {Index: 2000, delta: -60MB}
    └─ pos = 1

T3: 添加截断到 3000
    truncs[0] = {Index: 1000, delta: -50MB}
    truncs[1] = merge({Index: 2000}, {Index: 3000})
             = {Index: 3000, delta: -130MB, isDeltaTrusted: true}
    └─ pos = 1 (capacity reached, merge)

T4: 添加截断到 4000
    truncs[0] = {Index: 1000, delta: -50MB}
    truncs[1] = merge({Index: 3000}, {Index: 4000})
             = {Index: 4000, delta: -190MB, isDeltaTrusted: ?}
    └─ pos = 1 (继续合并)
```

**合并函数** (`raft_log_truncator.go:172-181`):

```go
func (pt pendingTruncation) merge(prev pendingTruncation) pendingTruncation {
    res := prev
    res.RaftTruncatedState = pt.RaftTruncatedState  // 使用新的截断点
    res.logDeltaBytes += pt.logDeltaBytes           // 累加 delta
    res.hasSideloaded = prev.hasSideloaded || pt.hasSideloaded

    // 检查是否可信
    if !pt.isDeltaTrusted || prev.Index+1 != pt.expectedFirstIndex {
        res.isDeltaTrusted = false
    }
    return res
}
```

**关键检查**：
```go
if prev.Index+1 != pt.expectedFirstIndex {
    res.isDeltaTrusted = false
}
```

**为什么需要这个检查**？
```
场景：日志不连续

假设：
    prev.Index = 1000
    pt.expectedFirstIndex = 1005  (期望从 1005 开始删除)

问题：
    [1001, 1004] 的日志大小未计入 delta
    → logDeltaBytes 不准确

标记：
    isDeltaTrusted = false
    → 后续需要重新计算 raftLogSize
```

#### 队列操作的线程安全

**读取（无需加锁）**：
```go
// replicaForTruncator 保证 raftMu 已持有
pendingTruncs.iterateLocked(func(index int, trunc pendingTruncation) {
    // 无需 pendingTruncs.mu.Lock()
})
```

**写入（需要加锁）**：
```go
pendingTruncs.mu.Lock()
pendingTruncs.mu.truncs[pos] = pendingTrunc
pendingTruncs.mu.Unlock()
```

**设计契约** (`raft_log_truncator.go:37-41`):
```
注释：
"We require Replica.raftMu to be additionally held while modifying
 the pending truncations. Hence, either one of those mutexes is
 sufficient for reading."

翻译：
- 修改时：必须持有 raftMu + pendingTruncs.mu
- 读取时：持有 raftMu OR pendingTruncs.mu 即可

好处：
✓ 读取不阻塞（raftLogQueue 只需持有 raftMu）
✓ 写入互斥（truncator 持有两个锁）
```

---

## 四、运行时行为与系统反馈（Runtime Behavior）

### 4.1 系统如何感知运行时信号

#### 信号 1：Pebble Flush 事件

```
触发条件：
├─ Memtable 达到大小阈值（默认 64 MB）
├─ Write stall（写入过快，强制 flush）
├─ 定时 flush（WAL 大小限制）
└─ 显式 flush（用户触发或 Checkpoint）

感知机制：
    Pebble EventListener → FlushEnd → flushCompletedCallback()

响应速度：
    立即（回调在 Pebble 的 goroutine 中执行）

影响范围：
    全局（所有 Range 都可能受益于此次持久化）
```

**频率估算**：
```
典型工作负载：
    写入速率：100 MB/s
    Memtable 大小：64 MB
    Flush 间隔：640 ms

高负载：
    写入速率：500 MB/s
    Memtable 大小：64 MB
    Flush 间隔：128 ms

低负载：
    写入速率：10 MB/s
    Memtable 大小：64 MB
    Flush 间隔：6.4 秒
```

#### 信号 2：日志大小超限

```
检测机制（Raft Log Queue）：
    每分钟扫描所有 Replica
    └─ 检查 raftLogSize > 64 MB
    └─ 提议截断

触发条件：
    raftLogSize = sum(所有 Raft log entries 的大小)
    threshold = RaftLogTruncationThreshold (默认 64 MB)

影响决策：
    if raftLogSize > threshold * 2:
        急迫截断（保留更少的日志）
    elif raftLogSize > threshold:
        正常截断（保留 2048 条日志）
```

#### 信号 3：持久化进度

```
度量：
    RaftAppliedIndex (已持久化)
    vs
    pendingTrunc.Index (待截断点)

决策逻辑：
    if RaftAppliedIndex >= pendingTrunc.Index:
        可以安全截断
    else:
        等待下次 flush
```

### 4.2 信号如何影响决策

#### 即时 vs 滞后

**即时响应**：
```
Pebble Flush 完成
    ↓ (< 1 ms)
durabilityAdvancedCallback()
    ↓ (启动 goroutine)
durabilityAdvanced()
    ↓ (立即处理)
tryEnactTruncations()
```

**滞后响应**：
```
日志大小检测
    ↓ (最多 1 分钟延迟)
Raft Log Queue 扫描
    ↓ (Raft 共识延迟 ~ 100ms)
addPendingTruncation()
    ↓ (等待持久化，可能数秒)
实际截断
```

**设计哲学**：
- **截断执行**：即时（依赖 Pebble 事件）
- **截断提议**：滞后（定期扫描，不紧急）
- **好处**：解耦提议和执行，降低系统耦合度

#### 局部 vs 全局

**局部决策**：
```
tryEnactTruncations(rangeID):
    只关心单个 Range
    └─ 读取该 Range 的 RaftAppliedIndex
    └─ 检查该 Range 的 pendingTruncs
    └─ 独立决策是否截断
```

**全局协调**：
```
durabilityAdvanced():
    处理所有待截断的 Range
    └─ 共享一个 GuaranteedDurability Reader
    └─ 批量执行（减少系统调用）
    └─ 但每个 Range 独立决策
```

**好处**：
- ✅ **无全局锁**：Range 之间不互相阻塞
- ✅ **并行友好**：未来可以并行处理多个 Range
- ✅ **故障隔离**：一个 Range 失败不影响其他

### 4.3 为什么采用当前策略

#### 惰性 vs 主动

**当前策略：惰性等待持久化**

```
惰性（当前）：
    等待 Pebble 自然 flush
    └─ 无额外 fsync 开销
    └─ 延迟数秒不影响正确性

主动（传统）：
    每次截断都 fsync
    └─ 延迟 1-10ms
    └─ 吞吐量下降 10-100 倍
```

**权衡**：
```
优势：
✓ 零额外开销（复用 Pebble flush）
✓ 批量优化（一次 flush 处理所有 Range）
✓ 吞吐量高

劣势：
✗ 延迟不可控（依赖 Pebble flush 频率）
✗ 极低负载时可能延迟较长
```

#### 本地自治 vs 集中控制

**当前策略：本地自治**

```
本地自治（当前）：
    每个 Store 独立管理截断
    └─ 独立的 raftLogTruncator
    └─ 独立的 pendingTruncs 队列
    └─ 无跨 Store 通信

集中控制（替代方案）：
    单个全局 truncator 管理所有 Store
    └─ 需要跨 Store 通信
    └─ 单点瓶颈
```

**为什么选择本地自治**？
- ✅ **无单点**：每个 Store 独立运作
- ✅ **可扩展**：随 Store 数量线性扩展
- ✅ **低延迟**：无网络通信开销

### 4.4 平衡多个目标

| 目标 | 实现机制 | 权衡 |
|------|---------|------|
| **稳定性** | 单 goroutine 执行 | 无并发冲突 |
| **吞吐量** | 批量处理 + 零 fsync | 可能延迟截断 |
| **公平性** | 遍历所有 Range | 无优先级区分 |
| **资源利用率** | 复用 Pebble flush | 依赖写入负载 |

**稳定性保证**：
```
单 goroutine 执行：
    runningTruncation = true  (互斥标志)
    └─ 最多 1 个 goroutine 执行截断
    └─ 无竞态条件
```

**吞吐量优化**：
```
批量处理：
    一次 durabilityAdvanced() 处理所有 Range
    └─ 共享 Reader
    └─ 减少系统调用

零 fsync：
    仅在 hasSideloaded 时 fsync
    └─ 99% 的截断无需 fsync
```

**公平性考虑**：
```
顺序遍历：
    for _, rangeID := range ranges {
        tryEnactTruncations(rangeID)
    }

问题：
    前面的 Range 可能耗时较长
    后面的 Range 被延迟

缓解：
    截断操作通常很快（< 1ms）
    即使有 1000 个 Range，总耗时 < 1 秒
```

---

## 五、设计模式分析（Design Patterns）

### 5.1 事件驱动架构（Event-Driven Architecture）

**模式识别**：

```go
// 注册监听器
s.StateEngine().RegisterFlushCompletedCallback(func() {
    truncator.durabilityAdvancedCallback()
})

// 事件源
Pebble FlushEnd Event

// 事件处理器
durabilityAdvancedCallback()
    └─ 启动异步任务
    └─ durabilityAdvanced()
```

**标准事件驱动模式的三要素**：
1. **事件源（Event Source）**：Pebble Flush
2. **事件监听器（Event Listener）**：`flushCompletedCallback`
3. **事件处理器（Event Handler）**：`durabilityAdvanced()`

**为什么选择这种模式**？
- ✅ **解耦**：truncator 不依赖 Pebble 的内部实现
- ✅ **非侵入**：Pebble 不需要知道 truncator 的存在
- ✅ **可扩展**：可以注册多个监听器（虽然当前只有一个）

**事实标准地位**：
- **Node.js EventEmitter**：相同模式
- **Java Listener Pattern**：相同模式
- **Kubernetes Informer**：类似模式（watch events）

### 5.2 双缓冲模式（Double Buffering Pattern）

**模式识别**：

```go
type raftLogTruncator struct {
    mu struct {
        addRanges, drainRanges map[roachpb.RangeID]struct{}
    }
}

func (t *raftLogTruncator) durabilityAdvanced(ctx context.Context) {
    t.mu.Lock()
    t.mu.addRanges, t.mu.drainRanges = t.mu.drainRanges, t.mu.addRanges
    t.mu.Unlock()
    // 现在可以处理 drainRanges，不阻塞新的入队
}
```

**经典双缓冲模式**：
```
图形渲染中的双缓冲：
    frontBuffer (显示给用户)
    backBuffer (后台渲染)

    每帧结束：swap(frontBuffer, backBuffer)
```

**在 truncator 中的应用**：
```
生产者：
    向 addRanges 添加新 Range
    └─ 持锁时间短（O(1)）

消费者：
    交换 addRanges 和 drainRanges
    └─ 处理 drainRanges（不持锁）
    └─ 持锁时间短（O(1)）
```

**为什么选择这种模式**？
- ✅ **低延迟入队**：生产者不受消费速度影响
- ✅ **无阻塞**：消费者不持锁处理
- ✅ **简单高效**：无需复杂的队列数据结构

**与经典模式的差异**：
- **经典双缓冲**：两个缓冲区轮流使用
- **这里的实现**：交换指针，复用 map

### 5.3 有界队列与合并策略（Bounded Queue with Merging）

**模式识别**：

```go
type pendingLogTruncations struct {
    mu struct {
        truncs [2]pendingTruncation  // 固定大小队列
    }
}

func addPendingTruncation(...) {
    if pos == capacity() {
        // 合并而非拒绝
        pendingTrunc = pendingTrunc.merge(pendingTruncs.mu.truncs[pos])
    }
}
```

**这不是标准的有界队列**：
```
标准有界队列（如 Go channel）：
    capacity = 100
    if len(queue) >= 100:
        block() 或 reject()

truncator 的队列：
    capacity = 2
    if len(queue) >= 2:
        merge(new, old)  // 合并，不拒绝
```

**合并策略的创新**：
```
优势：
✓ 永不丢失截断请求
✓ 永不阻塞生产者
✓ 自动压缩（多个截断合并为一个）

代价：
✗ 合并后的 delta 可能不准确
   → isDeltaTrusted = false
   → 需要重新计算 raftLogSize
```

**为什么不用无界队列**？
```
无界队列的问题：
    如果 truncs[0] 永远无法执行
    → 队列无限增长
    → 内存泄漏

有界 + 合并的好处：
    最多 2 个条目
    → 内存可控（O(1)）
    → 保证 truncs[1] 最终可执行
```

### 5.4 两阶段提交（Two-Phase Commit）

**模式识别**：

```go
// Phase 1: 准备（可逆）
r.stagePendingTruncation(ctx, trunc)
    └─ 更新内存状态

// Phase 2: 提交（不可逆）
batch.Commit(sync)
    └─ 持久化到磁盘

// Phase 3: 完成（不可逆）
r.finalizeTruncation(ctx)
    └─ 删除 sideloaded 文件
```

**标准两阶段提交**：
```
分布式事务（2PC）：
    Phase 1: Prepare (所有参与者投票)
    Phase 2: Commit (协调者决定提交或中止)
```

**在 truncator 中的变体**：
```
Phase 1: stagePendingTruncation()
    └─ 可逆的内存修改
    └─ 如果后续失败，可以回滚

Phase 2: batch.Commit()
    └─ 持久化（不可逆）

Phase 3: finalizeTruncation()
    └─ 删除文件（不可逆）
    └─ 只有在 Commit 成功后执行
```

**为什么需要分阶段**？
```
场景：Commit 失败

如果只有一阶段：
    deleteSideloadedFiles()  // 已执行
    batch.Commit()           // 失败
    → 文件已删除，但元数据未更新
    → 不一致

两阶段的好处：
    batch.Commit()           // 失败
    → finalizeTruncation() 不执行
    → 文件保留
    → 一致
```

### 5.5 适配器模式（Adapter Pattern）

**模式识别**：

```go
// Store 的适配器
type storeForTruncatorImpl Store

var _ storeForTruncator = &storeForTruncatorImpl{}

// Replica 的适配器
type raftTruncatorReplica Replica

var _ replicaForTruncator = &raftTruncatorReplica{}
```

**标准适配器模式**：
```
目标：
    使现有类（Store/Replica）适配新接口
    （storeForTruncator/replicaForTruncator）

方法：
    创建新类型（type alias）
    实现接口方法
```

**为什么使用适配器**？
```
问题：
    truncator 需要的接口与 Store/Replica 不匹配

传统解决方案：
    修改 Store/Replica，添加 truncator 专用方法
    → 破坏封装
    → Store/Replica 变得臃肿

适配器方案（当前）：
    创建专用适配器类型
    → Store/Replica 保持原样
    → truncator 获得所需接口
    → 职责分离
```

**事实标准地位**：
- **GoF 设计模式**：Adapter Pattern
- **Java 的 Adapter 类**：相同模式
- **gRPC 的 ServiceRegistrar**：类似模式

### 5.6 观察者模式的简化版（Simplified Observer）

**模式识别**：

```go
// 只有一个观察者
RegisterFlushCompletedCallback(func() {
    truncator.durabilityAdvancedCallback()
})
```

**标准观察者模式**：
```
subject.RegisterObserver(observer1)
subject.RegisterObserver(observer2)
subject.RegisterObserver(observer3)

subject.Notify():
    for observer in observers:
        observer.Update()
```

**简化版（当前）**：
```
subject.RegisterCallback(callback)
    └─ 只允许一个回调

subject.Notify():
    if callback != nil:
        callback()
```

**为什么简化**？
- ✅ **单一职责**：Pebble flush 只需通知 truncator
- ✅ **简单高效**：无需维护观察者列表
- ✅ **避免过度设计**：当前不需要多个观察者

**如果未来需要多个观察者**？
```go
// 可能的扩展：
type flushObservers []func()

func (p *Pebble) RegisterFlushObserver(cb func()) {
    p.mu.flushObservers = append(p.mu.flushObservers, cb)
}

func (p *Pebble) notifyFlush() {
    for _, cb := range p.mu.flushObservers {
        cb()
    }
}
```

### 5.7 刻意避免的模式：生产者-消费者队列

**为什么不用 channel**？

```go
// 可能的实现（未采用）：
type raftLogTruncator struct {
    rangeChan chan roachpb.RangeID  // 容量 1000
}

func enqueueRange(rangeID) {
    select {
    case rangeChan <- rangeID:
    default:
        // 队列满，丢弃或阻塞
    }
}

func durabilityAdvanced() {
    for rangeID := range rangeChan {
        tryEnactTruncations(rangeID)
    }
}
```

**为什么不这样设计**？
```
问题 1：重复入队
    Range R1 在 T0 入队
    Range R1 在 T1 再次入队
    → channel 中有两个 R1
    → 重复处理（浪费）

    map 的好处：
    addRanges[R1] = struct{}{}  // 幂等

问题 2：无法批量交换
    channel 是流式的
    无法"交换"生产和消费缓冲

    map + 指针交换：
    O(1) 时间复杂度

问题 3：难以检查是否为空
    len(channel) 不可靠（竞态）

    map：
    len(addRanges)  // 准确
```

---

## 六、具体运行示例（Concrete Example）

### 6.1 场景设置

**系统配置**：
- 1 个 Store，管理 3 个 Range
- Pebble memtable 大小：64 MB
- Raft log 截断阈值：64 MB
- 写入 QPS：10000/s，平均 entry 大小：1 KB

**Range 初始状态**：
```
Range 1:
    RaftAppliedIndex: 10000
    Truncated Index: 5000
    Raft Log Size: 70 MB (需要截断)

Range 2:
    RaftAppliedIndex: 8000
    Truncated Index: 3000
    Raft Log Size: 65 MB (需要截断)

Range 3:
    RaftAppliedIndex: 12000
    Truncated Index: 10000
    Raft Log Size: 30 MB (无需截断)
```

### 6.2 正常情况的完整时间线

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
时间线：正常截断流程
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

T0 (00:00.000): Raft Log Queue 扫描
    ├─ 检测到 Range 1 日志大小 70 MB > 64 MB
    ├─ 计算截断点：newIndex = 10000 - 2048 = 7952
    ├─ 提议 TruncateLogRequest
    │   RaftTruncatedState{Index: 7952, Term: 42}
    │   expectedFirstIndex: 5001
    │   raftLogDelta: -58 MB (估算)
    └─ 通过 Raft 共识复制到所有 Replica

T0+50ms (00:00.050): Raft 提议达成共识
    └─ Range 1 的所有 Replica 应用该命令

T0+55ms (00:00.055): Replica 应用 TruncateLogRequest
    ├─ 调用 raftLogTruncator.addPendingTruncation()
    │
    ├─ 计算 sideloaded entries 大小
    │   sideloadedStats([5001, 7952]) → 0 entries
    │
    ├─ 创建 pendingTruncation:
    │   {
    │       Index: 7952,
    │       expectedFirstIndex: 5001,
    │       logDeltaBytes: -58 MB,
    │       isDeltaTrusted: true,
    │       hasSideloaded: false,
    │   }
    │
    ├─ 加入队列：
    │   Range1.pendingTruncs[0] = pendingTruncation
    │
    └─ enqueueRange(Range1)
        └─ truncator.mu.addRanges[Range1] = struct{}{}

T0+100ms (00:00.100): 同样处理 Range 2
    └─ Range2.pendingTruncs[0] = {Index: 5952, delta: -55 MB}
    └─ truncator.mu.addRanges[Range2] = struct{}{}

T0+100ms: 等待持久化...
    ├─ RaftAppliedIndex = 10000 (在 memtable 中)
    ├─ 尚未 flush 到 SST
    └─ 无法安全截断

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

T1 (00:00.640): Pebble 触发 memtable flush
    ├─ 原因：memtable 达到 64 MB
    ├─ 将 memtable 写入 L0 SST
    ├─ 包含 RangeAppliedState{RaftAppliedIndex: 10000}
    └─ Flush 完成，触发 FlushEnd 事件

T1+1ms (00:00.641): Pebble 调用回调
    └─ p.mu.flushCompletedCallback()
        └─ truncator.durabilityAdvancedCallback()

T1+2ms (00:00.642): durabilityAdvancedCallback() 逻辑
    ├─ 检查 runningTruncation = false
    ├─ len(addRanges) = 2 (Range1, Range2)
    ├─ 设置 runningTruncation = true
    └─ 启动 goroutine

T1+3ms (00:00.643): 异步 goroutine 开始执行
    └─ durabilityAdvanced(ctx)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

T1+4ms (00:00.644): durabilityAdvanced() 执行
    ├─ 交换队列：
    │   addRanges = {} (empty)
    │   drainRanges = {Range1, Range2}
    │
    ├─ 创建 GuaranteedDurability Reader
    │   └─ 只读取 SST 中的数据
    │
    └─ 遍历 drainRanges

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

T1+5ms (00:00.645): 处理 Range 1
    ├─ tryEnactTruncations(Range1, reader)
    │
    ├─ 获取 Replica：
    │   r = acquireReplicaForTruncator(Range1)
    │   └─ raftMu.Lock()  (锁定)
    │
    ├─ 读取当前截断状态：
    │   truncState.Index = 5000
    │
    ├─ 检查待截断队列：
    │   pendingTruncs[0] = {Index: 7952}
    │   (无 noop，队列有效)
    │
    ├─ 读取持久化的 RaftAppliedIndex：
    │   as = stateLoader.LoadRangeAppliedState(ctx, reader)
    │   as.RaftAppliedIndex = 10000  (已持久化)
    │
    ├─ 检查是否可执行：
    │   pendingTruncs[0].Index = 7952
    │   7952 <= 10000  → 可以执行！
    │   enactIndex = 0
    │
    ├─ 创建 WriteBatch
    │
    ├─ 删除日志 [5001, 7952]：
    │   for i := 5001; i <= 7952; i++ {
    │       batch.ClearUnversioned(keys.RaftLogKey(Range1, i))
    │   }
    │   删除 2952 条日志
    │
    ├─ 更新元数据：
    │   batch.PutUnversioned(
    │       keys.RaftTruncatedStateKey(Range1),
    │       RaftTruncatedState{Index: 7952, Term: 42}
    │   )
    │
    ├─ 更新 Replica 状态：
    │   r.stagePendingTruncation(ctx, pendingTruncs[0])
    │   └─ raftLogSize = 70 MB + (-58 MB) = 12 MB
    │
    ├─ 提交 WriteBatch：
    │   sync = false  (无 sideloaded)
    │   batch.Commit(false)
    │   └─ 写入 memtable（不 fsync）
    │
    ├─ 完成截断：
    │   r.finalizeTruncation(ctx)
    │   └─ truncState.Index = 7952
    │
    ├─ 清理队列：
    │   pendingTruncs.popLocked()
    │   └─ pendingTruncs[0] = {} (empty)
    │
    └─ 释放 Replica：
        └─ raftMu.Unlock()

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

T1+6ms (00:00.646): 处理 Range 2
    ├─ tryEnactTruncations(Range2, reader)
    ├─ (类似 Range 1 的流程)
    └─ 成功截断到 Index 5952

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

T1+7ms (00:00.647): durabilityAdvanced() 完成
    ├─ 检查 queuedDurabilityCB = false
    ├─ 设置 runningTruncation = false
    └─ goroutine 退出

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

最终状态：
Range 1:
    RaftAppliedIndex: 10000
    Truncated Index: 7952  (从 5000 前进)
    Raft Log Size: 12 MB  (从 70 MB 减少)
    磁盘空间释放: 58 MB

Range 2:
    RaftAppliedIndex: 8000
    Truncated Index: 5952  (从 3000 前进)
    Raft Log Size: 10 MB  (从 65 MB 减少)
    磁盘空间释放: 55 MB

总耗时: 7 ms
零额外 fsync！
```

### 6.3 边界场景：持久化进度不足

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
时间线：持久化进度不足的情况
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

T0 (00:00.000): 提议截断到 Index 7952
    └─ Range1.pendingTruncs[0] = {Index: 7952}
    └─ enqueueRange(Range1)

T1 (00:00.640): Pebble Flush 触发
    ├─ 持久化的 RaftAppliedIndex = 7500  (< 7952)
    └─ durabilityAdvancedCallback()

T1+5ms (00:00.645): tryEnactTruncations(Range1)
    ├─ pendingTruncs[0].Index = 7952
    ├─ as.RaftAppliedIndex = 7500
    ├─ 7952 > 7500  → 无法安全截断
    ├─ enactIndex = -1
    │
    ├─ 重新入队：
    │   t.enqueueRange(Range1)
    │   └─ addRanges[Range1] = struct{}{}
    │
    └─ 返回（不执行截断）

T2 (00:01.280): 下一次 Pebble Flush
    ├─ 持久化的 RaftAppliedIndex = 8000  (>= 7952)
    └─ durabilityAdvancedCallback()

T2+5ms (00:01.285): tryEnactTruncations(Range1)
    ├─ as.RaftAppliedIndex = 8000
    ├─ 7952 <= 8000  → 可以执行
    └─ 成功截断到 7952
```

**关键点**：
- **自动重试**：无需手动干预
- **安全优先**：宁可延迟，不冒险
- **最终一致**：持久化进度最终会前进

---

（由于内容较长，我将在这里暂停，继续撰写剩余部分。请回复"继续"以查看完整的下篇文档。）

**文档版本**: v1.0（下篇-Part1）
**作者**: 基于 CockroachDB 源码分析
**最后更新**: 2026-02-04
