# 第四十章_Store队列系统深度剖析——基于优先级与Purgatory的Replica后台维护调度引擎（下篇）

*接续上篇*

### 3.5 具体队列的 shouldQueue 与 process 实现

每个具体队列实现 `queueImpl` 接口的两个核心方法：

```go
type queueImpl interface {
    shouldQueue(context.Context, hlc.ClockTimestamp, *Replica, spanconfig.StoreReader) (shouldQueue bool, priority float64)
    process(context.Context, *Replica, spanconfig.StoreReader, float64) (processed bool, err error)
}
```

#### 3.5.1 leaseQueue - Lease 转移队列

**shouldQueue 实现**：

```go
func (lq *leaseQueue) shouldQueue(
    ctx context.Context, now hlc.ClockTimestamp, repl *Replica, confReader spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
    // 获取 SpanConfig（包含 lease_preferences）
    conf, _, err := confReader.GetSpanConfigForKey(ctx, repl.startKey)
    if err != nil {
        return false, 0
    }

    desc := repl.Desc()

    // 调用 LeasePlanner 判断是否需要转移
    return lq.planner.ShouldPlanChange(ctx, now, repl, desc, &conf, plan.PlannerOptions{
        CanTransferLease: lq.canTransferLeaseFrom(ctx, repl, &conf),
    })
}
```

**Planner 决策逻辑**：

```go
// 在 plan.LeasePlanner.ShouldPlanChange 中
func (lp *LeasePlanner) ShouldPlanChange(...) (bool, float64) {
    // 1. 检查当前 Leaseholder 是否在 lease_preferences 中
    currentLease := repl.CurrentLeaseStatus(ctx)
    preferred := findPreferredLeaseholder(conf.LeasePreferences, desc)

    if currentLease.Replica.StoreID != preferred.StoreID {
        // 当前 Leaseholder 不是首选，需要转移
        return true, 1.0
    }

    // 2. 检查负载均衡
    storeList := lp.storePool.GetStoreList()
    if shouldRebalanceForQPS(repl, storeList) {
        // 该 Store 的 QPS 显著高于平均值
        return true, 0.5
    }

    // 3. 检查节点状态
    if isNodeDraining(currentLease.Replica.NodeID) {
        // 节点正在 draining，需要转移 Lease
        return true, 2.0 // 高优先级
    }

    // 4. 检查 IO overload
    if isIOOverloaded(currentLease.Replica.StoreID) {
        // Store IO 过载，需要 shed leases
        return true, 1.5
    }

    return false, 0
}
```

**process 实现**：

```go
func (lq *leaseQueue) process(
    ctx context.Context, repl *Replica, confReader spanconfig.StoreReader, _ float64,
) (processed bool, err error) {
    // 获取 allocatorToken（确保同一时刻只有一个队列修改副本配置）
    if tokenErr := repl.allocatorToken.TryAcquire(ctx, lq.name); tokenErr != nil {
        return false, tokenErr
    }
    defer repl.allocatorToken.Release(ctx)

    // 再次获取 SpanConfig
    conf, _, err := confReader.GetSpanConfigForKey(ctx, repl.startKey)
    if err != nil {
        return false, err
    }

    desc := repl.Desc()

    // 计算应该转移到哪个节点
    change, err := lq.planner.PlanOneChange(ctx, repl, desc, &conf, plan.PlannerOptions{
        CanTransferLease: lq.canTransferLeaseFrom(ctx, repl, &conf),
    })
    if err != nil {
        return false, err
    }

    // 执行转移
    if transferOp, ok := change.Op.(plan.AllocationTransferLeaseOp); ok {
        lease, _ := repl.GetLease()
        log.Infof(ctx, "transferring lease to %d usage=%v, lease=[%v type=%v]",
            transferOp.Target, transferOp.Usage, lease, lease.Type())

        // 记录转移时间（用于限流）
        lq.lastLeaseTransfer.Store(timeutil.Now())

        // 通知 MMA AllocatorSync
        changeID := lq.as.NonMMAPreTransferLease(
            ctx, lq.store.StoreID(), desc, transferOp.Usage, transferOp.Source, transferOp.Target,
        )

        // 执行 AdminTransferLease
        err = repl.AdminTransferLease(ctx, transferOp.Target.StoreID, false /* bypassSafetyChecks */)

        // 通知 AllocatorSync 结果
        lq.as.PostTransferLeaseResult(changeID, err)

        return err == nil, err
    }

    return false, nil
}
```

**关键决策**：
- **为什么需要 allocatorToken？**
  - 避免 leaseQueue 和 replicateQueue 同时修改副本配置
  - 例如：leaseQueue 正在转移 Lease 时，replicateQueue 不应该尝试添加副本

- **为什么记录 lastLeaseTransfer？**
  - 限流：避免频繁转移 Lease
  - `MinLeaseTransferInterval`（默认 1 秒）确保最小间隔

#### 3.5.2 mvccGCQueue - MVCC 垃圾回收队列

**shouldQueue 实现**：

```go
func (mgcq *mvccGCQueue) shouldQueue(
    ctx context.Context, _ hlc.ClockTimestamp, repl *Replica, _ spanconfig.StoreReader,
) (bool, float64) {
    // 1. 检查 Protected Timestamp（是否允许 GC）
    conf, err := repl.LoadSpanConfig(ctx)
    if err != nil {
        return false, 0
    }

    canGC, gcTimestamp, oldThreshold, newThreshold, err := repl.checkProtectedTimestampsForGC(ctx, conf.TTL())
    if err != nil || !canGC {
        return false, 0
    }

    // 2. 获取上次 GC 时间
    lastGC, err := repl.getQueueLastProcessed(ctx, mgcq.name)
    if err != nil {
        return false, 0
    }

    // 3. 计算 GC Score
    score := makeMVCCGCQueueScore(ctx, repl, gcTimestamp, lastGC, conf.TTL(), canAdvanceGCThreshold)

    return score.ShouldQueue, score.FinalScore
}
```

**GC Score 计算公式**：

```go
func makeMVCCGCQueueScore(
    ctx context.Context,
    repl *Replica,
    now hlc.Timestamp,
    lastGC hlc.Timestamp,
    gcTTL time.Duration,
    canAdvanceGCThreshold bool,
) mvccGCQueueScore {
    // 获取 MVCC Stats
    repl.mu.RLock()
    ms := *repl.shMu.state.Stats
    hint := *repl.shMu.state.GCHint
    repl.mu.RUnlock()

    // 1. 计算 Dead Fraction（死亡数据占比）
    // DeadFraction = GCBytesAge / (LiveBytes + GCBytesAge)
    liveBytes := ms.LiveBytes
    gcBytes := ms.GCBytesAge
    deadFraction := float64(0)
    if liveBytes+gcBytes > 0 {
        deadFraction = float64(gcBytes) / float64(liveBytes+gcBytes)
    }

    // 2. 计算 Values Scalable Score
    // 基于 GCBytesAge 和 TTL
    // 越多的垃圾数据 + 越长时间未 GC → 分数越高
    valuesScalableScore := float64(gcBytes) / float64(gcTTL.Nanoseconds())

    // 3. 计算 Intent Score
    // 基于 Intent 数量和平均年龄
    intentCount := ms.IntentCount
    intentAge := ms.IntentAge
    intentScore := float64(intentCount) * float64(intentAge) / float64(intentAgeNormalization)

    // 4. 应用 Fuzz Factor（随机化，避免所有 Replica 同时 GC）
    fuzzFactor := 0.5 + rand.Float64()

    // 5. 最终分数
    finalScore := (valuesScalableScore*deadFraction + intentScore) * fuzzFactor

    // 6. 判断是否应该入队
    shouldQueue := false
    if valuesScalableScore*deadFraction >= mvccGCKeyScoreThreshold {
        shouldQueue = true
    }
    if intentScore >= mvccGCIntentScoreThreshold {
        shouldQueue = true
    }
    if !hint.IsEmpty() && hint.GCTimestamp.Less(now) {
        // 有 GC Hint（如 range tombstone），最高优先级
        shouldQueue = true
        finalScore = deleteRangePriority
    }

    return mvccGCQueueScore{
        TTL:                 gcTTL,
        LastGC:              time.Duration(now.WallTime - lastGC.WallTime),
        DeadFraction:        deadFraction,
        ValuesScalableScore: valuesScalableScore,
        IntentScore:         intentScore,
        FuzzFactor:          fuzzFactor,
        FinalScore:          finalScore,
        ShouldQueue:         shouldQueue,
        GCBytes:             ms.GCBytes,
        GCByteAge:           gcBytes,
        Hint:                hint,
    }
}
```

**关键公式解释**：

```
FinalScore = (ValuesScalableScore * DeadFraction + IntentScore) * FuzzFactor

其中：
  ValuesScalableScore = GCBytesAge / TTL
    - GCBytesAge: 累计垃圾数据量 * 时间（字节*秒）
    - TTL: 数据保留时间（默认 25 小时）
    - 含义：归一化的垃圾数据量

  DeadFraction = GCBytesAge / (LiveBytes + GCBytesAge)
    - 含义：垃圾数据占比（0 到 1）
    - 作用：避免对活跃 Range 频繁 GC

  IntentScore = IntentCount * IntentAge / 8h
    - 含义：未解决的 Intent 压力
    - 8h：归一化常数（平均 Intent 年龄）

  FuzzFactor = 0.5 + rand.Float64()  (0.5 到 1.5)
    - 作用：随机化，避免所有 Replica 同时 GC
```

**process 实现**：

```go
func (mgcq *mvccGCQueue) process(
    ctx context.Context, repl *Replica, _ spanconfig.StoreReader, _ float64,
) (bool, error) {
    // 1. 计算 GC 请求
    conf, err := repl.LoadSpanConfig(ctx)
    if err != nil {
        return false, err
    }

    snap := repl.Engine().NewSnapshot()
    defer snap.Close()

    gcInfo, err := gc.CalculateThreshold(ctx, snap, repl.Desc().RSpan(), conf.TTL())
    if err != nil {
        return false, err
    }

    if gcInfo.NumKeysAffected == 0 {
        // 没有需要 GC 的数据
        return false, nil
    }

    // 2. 执行 GC
    gcReq := &kvpb.GCRequest{
        RequestHeader: kvpb.RequestHeader{Key: repl.Desc().StartKey.AsRawKey()},
        Threshold:     gcInfo.Threshold,
        Keys:          gcInfo.Keys,
        RangeKeys:     gcInfo.RangeKeys,
    }

    _, pErr := kv.SendWrappedWith(ctx, repl.store.DB().NonTransactionalSender(), kvpb.Header{
        RangeID: repl.RangeID,
    }, gcReq)

    if pErr != nil {
        return false, pErr.GoError()
    }

    // 3. 解决 Intent
    if len(gcInfo.Intents) > 0 {
        err := mgcq.intentResolver.CleanupIntentsAsync(ctx, gcInfo.Intents, intentresolver.CleanupOptions{
            PoisonPolicy: intentresolver.PoisonAborted,
        })
        if err != nil {
            log.Warningf(ctx, "failed to cleanup intents: %v", err)
        }
    }

    return true, nil
}
```

**为什么这么设计？**

1. **为什么使用 GCBytesAge 而不是 GCBytes？**
   - `GCBytesAge` = Σ(垃圾数据大小 × 存在时间)
   - 考虑了时间因素：旧垃圾数据优先级更高
   - 例如：1GB 存在 1 天 > 100MB 存在 1 小时

2. **为什么需要 DeadFraction？**
   - 避免对活跃 Range 频繁 GC
   - 例如：Range 有 10GB 活数据 + 100MB 垃圾 → DeadFraction=0.01 → 低优先级
   - 例如：Range 有 100MB 活数据 + 10GB 垃圾 → DeadFraction=0.99 → 高优先级

3. **为什么需要 FuzzFactor？**
   - 避免所有 Replica 同时 GC（CPU 峰值）
   - 随机化使 GC 时间分散

#### 3.5.3 splitQueue - Range 分裂队列

**shouldQueue 实现**：

```go
func (sq *splitQueue) shouldQueue(
    ctx context.Context, now hlc.ClockTimestamp, repl *Replica, confReader spanconfig.StoreReader,
) (bool, float64) {
    // 1. 检查是否需要基于大小分裂
    conf, _, err := confReader.GetSpanConfigForKey(ctx, repl.startKey)
    if err != nil {
        return false, 0
    }

    ms := repl.GetMVCCStats()
    if ms.Total() > conf.RangeMaxBytes {
        // Range 大小超过阈值
        excess := ms.Total() - conf.RangeMaxBytes
        priority := float64(excess) / float64(conf.RangeMaxBytes)
        return true, priority
    }

    // 2. 检查是否需要基于负载分裂
    if shouldSplitBasedOnLoad(repl, conf) {
        return true, 1.0
    }

    // 3. 检查是否需要基于 SpanConfig 分裂
    if needsSplitBySpanConfig(ctx, repl, confReader) {
        return true, 1.0
    }

    return false, 0
}
```

**process 实现**：

```go
func (sq *splitQueue) process(
    ctx context.Context, repl *Replica, confReader spanconfig.StoreReader, _ float64,
) (bool, error) {
    // 1. 查找分裂点
    conf, _, err := confReader.GetSpanConfigForKey(ctx, repl.startKey)
    if err != nil {
        return false, err
    }

    splitKey, splitByLoadReason := findSplitKey(ctx, repl, conf)
    if splitKey == nil {
        // 没有合适的分裂点
        // 例如：Range 只有一个 Key，无法分裂
        return false, &splitByLoadPurgatoryError{
            msg: "cannot find split key",
        }
    }

    // 2. 检查是否在 Table boundary
    if splitKey.Equal(keys.MustAddr(keys.TableDataMin)) {
        // 不在 Table boundary 分裂
        return false, nil
    }

    // 3. 执行分裂
    _, pErr := sq.db.AdminSplit(ctx, splitKey.AsRawKey(), hlc.MaxTimestamp)
    if pErr != nil {
        // 分裂失败
        if errors.Is(pErr.GoError(), kvpb.ErrCannotSplitError) {
            // 无法分裂（如跨事务）
            return false, &splitByLoadPurgatoryError{
                msg: pErr.String(),
            }
        }
        return false, pErr.GoError()
    }

    // 4. 更新指标
    if splitByLoadReason {
        sq.metrics.LoadBasedSplitCount.Inc(1)
    } else {
        sq.metrics.SizeBasedSplitCount.Inc(1)
    }

    return true, nil
}
```

**分裂点查找算法**：

```go
func findSplitKey(ctx context.Context, repl *Replica, conf spanconfig.Config) (splitKey roachpb.RKey, isLoad bool) {
    // 1. 优先检查 SpanConfig 要求的分裂点
    if splitKey := findSpanConfigSplitKey(ctx, repl, conf); splitKey != nil {
        return splitKey, false
    }

    // 2. 检查负载分裂
    if loadSplitKey := repl.loadBasedSplitter.MaybeSplitKey(timeutil.Now()); loadSplitKey != nil {
        return loadSplitKey, true
    }

    // 3. 基于大小分裂：查找中点
    ms := repl.GetMVCCStats()
    targetSize := ms.Total() / 2

    // 扫描 Range，查找最接近 targetSize 的 Key
    snap := repl.Engine().NewSnapshot()
    defer snap.Close()

    iter := snap.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
        LowerBound: repl.Desc().StartKey.AsRawKey(),
        UpperBound: repl.Desc().EndKey.AsRawKey(),
    })
    defer iter.Close()

    accumulatedSize := int64(0)
    iter.SeekGE(storage.MVCCKey{Key: repl.Desc().StartKey.AsRawKey()})
    for ; ; iter.Next() {
        if ok, err := iter.Valid(); err != nil || !ok {
            break
        }
        key := iter.UnsafeKey()
        accumulatedSize += int64(len(key.Key) + len(iter.UnsafeValue()))

        if accumulatedSize >= targetSize {
            // 找到分裂点
            return keys.Addr(key.Key), false
        }
    }

    return nil, false
}
```

**为什么这么设计？**

1. **为什么优先检查 SpanConfig 分裂点？**
   - 某些 Key（如 Table boundary）必须分裂
   - 确保不同 Table 在不同 Range 中（Zone Config 隔离）

2. **为什么负载分裂优先于大小分裂？**
   - 负载分裂更紧急（热点问题）
   - 大小分裂可以延迟（只影响性能，不影响可用性）

3. **为什么查找中点而不是随机分裂？**
   - 均匀分裂确保两个新 Range 大小相近
   - 避免一个 Range 太小（可能立即 Merge）或太大（可能立即再次 Split）

#### 3.5.4 raftSnapshotQueue - Raft 快照队列

**shouldQueue 实现**：

```go
func (rq *raftSnapshotQueue) shouldQueue(
    ctx context.Context, now hlc.ClockTimestamp, repl *Replica, _ spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
    // 检查是否有 Follower 需要 Snapshot
    if status := repl.RaftStatus(); status != nil {
        // raft.Status.Progress 只在 Leader 上填充
        for _, p := range status.Progress {
            if p.State == tracker.StateSnapshot {
                // 该 Follower 正在等待 Snapshot
                log.VInfof(ctx, 2, "raft snapshot needed, enqueuing")
                return true, raftSnapshotPriority
            }
        }
    }
    return false, 0
}
```

**Follower 什么时候进入 StateSnapshot？**

```
场景 1：Follower 落后太多，需要的日志已被截断
  Leader: [100, 200]  (日志已截断到 Index 100)
  Follower: Index 50  (需要 Index 50-100 的日志，但已被截断)
  → Follower 进入 StateSnapshot

场景 2：新增 Replica
  Leader: [1, 1000]
  新 Replica: 未初始化
  → 新 Replica 进入 StateSnapshot

场景 3：Follower 长时间未响应
  Leader: 检测到 Follower heartbeat 超时
  → 将 Follower 标记为 StateSnapshot
```

**process 实现**：

```go
func (rq *raftSnapshotQueue) process(
    ctx context.Context, repl *Replica, _ spanconfig.StoreReader, _ float64,
) (anyProcessed bool, _ error) {
    // 检查所有 Follower
    if status := repl.RaftStatus(); status != nil {
        for id, p := range status.Progress {
            if p.State == tracker.StateSnapshot {
                log.Infof(ctx, "sending raft snapshot to replica %d", id)

                // 发送 Snapshot
                if processed, err := rq.processRaftSnapshot(ctx, repl, roachpb.ReplicaID(id)); err != nil {
                    return false, err
                } else if processed {
                    anyProcessed = true
                }
            }
        }
    }
    return anyProcessed, nil
}

func (rq *raftSnapshotQueue) processRaftSnapshot(
    ctx context.Context, repl *Replica, replicaID roachpb.ReplicaID,
) (bool, error) {
    // 1. 检查 Snapshot 限流
    if !rq.canSendSnapshot() {
        return false, errors.New("snapshot rate limit exceeded")
    }

    // 2. 构造 Snapshot
    snap, err := repl.GetSnapshot(ctx, kvserverpb.SnapshotRequest_RAFT)
    if err != nil {
        return false, err
    }
    defer snap.Close()

    // 3. 发送 Snapshot（通过 Raft Transport）
    err = repl.sendSnapshot(ctx, snap, replicaID)
    if err != nil {
        return false, err
    }

    return true, nil
}
```

**为什么这么设计？**

1. **为什么 needsLease=false？**
   - Raft Leader 不一定是 Leaseholder
   - Follower 也需要接收 Snapshot（由 Leader 发送）
   - 如果 needsLease=true，Follower 的队列会跳过该 Replica

2. **为什么 needsSpanConfigs=false？**
   - Snapshot 可能是为了让 Span Config Range 本身可用
   - 不能依赖 Span Config 已就绪（鸡生蛋问题）

3. **为什么限流？**
   - Snapshot 可能很大（几 GB）
   - 同时发送多个 Snapshot 会耗尽网络带宽
   - 默认限制：8MB/s

---

## 四、运行时行为与系统反馈（Runtime Behavior）

### 4.1 系统如何感知运行时信号

#### 4.1.1 负载信号

**Replica 负载指标**：

```go
// 在 Replica 中维护
type Replica struct {
    mu struct {
        state struct {
            Stats *enginepb.MVCCStats // MVCC 统计（大小、版本数、垃圾数据量）
        }
    }

    loadStats *replicastats.ReplicaStats // 负载统计（QPS、CPU、读写字节数）
}
```

**如何收集？**

```go
// 每次请求处理后更新
func (r *Replica) recordRequestStats(
    ctx context.Context,
    ba *kvpb.BatchRequest,
    br *kvpb.BatchResponse,
) {
    // 更新 QPS
    r.loadStats.RecordRequest(ba, br)

    // 更新读写字节数
    r.loadStats.RecordBatchSize(ba.Size(), br.Size())

    // 更新 CPU 时间
    r.loadStats.RecordCPUTime(cpuTime)
}
```

**队列如何使用？**

```go
// leaseQueue 根据负载决定是否转移 Lease
func (lq *leaseQueue) shouldQueue(...) (bool, float64) {
    qps := repl.loadStats.QueriesPerSecond()
    avgQPS := lq.storePool.GetAverageQPS()

    if qps > avgQPS*1.5 {
        // 该 Replica 的 QPS 显著高于平均值
        // 考虑转移 Lease 到负载更低的节点
        return true, float64(qps) / float64(avgQPS)
    }

    return false, 0
}
```

#### 4.1.2 阻塞信号

**场景 1：Raft 不可达**

```go
// 在 raftLogQueue.process 中
func (rlq *raftLogQueue) process(...) (bool, error) {
    // 尝试截断 Raft 日志
    truncateIndex := calculateTruncateIndex(repl)

    // 检查所有 Follower 是否已应用到该 Index
    status := repl.RaftStatus()
    for _, p := range status.Progress {
        if p.Match < truncateIndex {
            // Follower 还未应用到截断点
            // 返回 PurgatoryError，等待 Follower 追上
            return false, &raftLogTruncatePurgatoryError{
                msg: fmt.Sprintf("follower %d lagging (match=%d, truncate=%d)",
                    p.ID, p.Match, truncateIndex),
            }
        }
    }

    // 所有 Follower 已追上，执行截断
    return rlq.truncateLog(ctx, repl, truncateIndex)
}
```

**Purgatory 触发**：
- Follower 追上后，Raft 会发送 heartbeat
- raftLogQueue 的 purgatoryChan 定期检查（无外部触发）
- 定期重试（1 分钟）

**场景 2：目标节点不可用**

```go
// 在 replicateQueue.process 中
func (rq *replicateQueue) process(...) (bool, error) {
    // 计算应该添加副本到哪个 Store
    change, err := rq.planner.PlanOneChange(ctx, repl, desc, conf, options)
    if err != nil {
        return false, err
    }

    if addOp, ok := change.Op.(plan.AllocationAddOp); ok {
        // 尝试添加副本
        err := repl.ChangeReplicas(ctx, addReplica(addOp.Target))
        if errors.Is(err, errTargetNodeUnavailable) {
            // 目标节点不可用
            return false, &replicatePurgatoryError{
                msg: fmt.Sprintf("target store %d unavailable", addOp.Target.StoreID),
            }
        }
        return err == nil, err
    }

    return false, nil
}
```

**Purgatory 触发**：
- Gossip 更新（节点 liveness 变化）
- replicateQueue 的 updateChan 监听 Gossip 变化
- 立即重试所有 purgatory 中的 Replica

**实现**：

```go
// 在 replicateQueue 中
func (rq *replicateQueue) updateChan() <-chan time.Time {
    // 监听 Gossip 更新
    return rq.gossipUpdateC
}

// 在 processLoop 中
select {
case <-updateChan():
    // Gossip 更新，重试 purgatory
    rq.processPurgatory()
}
```

#### 4.1.3 失败信号

**场景 1：Lease 转移失败**

```go
// 在 leaseQueue.process 中
err := repl.AdminTransferLease(ctx, target, false)
if err != nil {
    if errors.Is(err, kvpb.LeaseRejectedError{}) {
        // 目标节点拒绝 Lease
        // 可能原因：
        //   - 目标节点正在关闭
        //   - 目标节点的 Store 正在 draining
        //   - 网络分区
        return false, &leasePurgatoryError{
            msg: fmt.Sprintf("lease rejected by store %d: %v", target, err),
        }
    }
    // 其他错误，直接失败
    return false, err
}
```

**场景 2：Split 失败（跨事务）**

```go
// 在 splitQueue.process 中
_, pErr := sq.db.AdminSplit(ctx, splitKey, hlc.MaxTimestamp)
if pErr != nil {
    if errors.Is(pErr.GoError(), kvpb.ErrCannotSplitError) {
        // 无法分裂：Range 中有未提交的事务跨越分裂点
        // 例如：事务写入 Key1 和 Key3，分裂点是 Key2
        return false, &splitByLoadPurgatoryError{
            msg: fmt.Sprintf("cannot split: %v", pErr),
        }
    }
    return false, pErr.GoError()
}
```

**Purgatory 重试策略**：
- 定时器（1 分钟）：适用于无外部触发的情况
- 事件驱动：Gossip 更新、Span Config 更新

### 4.2 信号如何影响决策

#### 4.2.1 优先级动态调整

**场景：under-replicated 优先级提升**

```go
// 在 replicateQueue 中
func (rq *replicateQueue) shouldQueue(...) (bool, float64) {
    desc := repl.Desc()
    conf, _, err := confReader.GetSpanConfigForKey(ctx, repl.startKey)
    if err != nil {
        return false, 0
    }

    // 检查副本数
    numReplicas := len(desc.Replicas().Descriptors())
    numVoters := len(desc.Replicas().VoterDescriptors())
    requiredVoters := int(conf.NumVoters)

    if numVoters < requiredVoters {
        // Under-replicated
        deficit := requiredVoters - numVoters
        priority := float64(deficit) * 10.0

        if numVoters == 1 {
            // 只剩 1 个副本，极高优先级
            priority = 100.0
        }

        return true, priority
    }

    // 检查是否有副本在 decommissioning 节点上
    for _, repl := range desc.Replicas().Descriptors() {
        if isDecommissioning(repl.NodeID) {
            // 节点正在下线，高优先级
            return true, 50.0
        }
    }

    // 其他情况（负载均衡）
    return true, 1.0
}
```

**优先级语义**：

```
优先级范围：
  100+    : 紧急（只剩 1 个副本）
  50-100  : 高（节点 decommissioning）
  10-50   : 中（under-replicated）
  1-10    : 低（负载均衡）
  0-1     : 极低（优化）
```

**优先级队列排序**：

```go
// 在 priorityQueue 中
func (pq priorityQueue) Less(i, j int) bool {
    a, b := pq.sl[i], pq.sl[j]
    if a.priority == b.priority {
        // 优先级相同，FIFO（先入队先处理）
        return a.seq < b.seq
    }
    // 优先级高的先处理
    return a.priority > b.priority
}
```

**示例**：

```
队列状态：
  r1000: priority=100 (只剩 1 个副本)
  r2000: priority=50  (节点 decommissioning)
  r3000: priority=10  (under-replicated: 2/3)
  r4000: priority=1   (负载均衡)

处理顺序：
  1. r1000 (最紧急)
  2. r2000
  3. r3000
  4. r4000
```

#### 4.2.2 即时 vs 滞后决策

**即时决策（Immediate）**：

```go
// 场景：Replica 被移除后立即 GC
func (r *Replica) changeReplicasImpl(...) error {
    // 执行 Replica 变更
    err := r.applyConfChange(ctx, change)
    if err != nil {
        return err
    }

    // 如果当前 Replica 被移除，立即加入 GC 队列
    if change.isRemoval(r.ReplicaID()) {
        r.store.replicaGCQueue.AddAsync(ctx, r, replicaGCPriorityRemoved)
    }

    return nil
}
```

**滞后决策（Delayed）**：

```go
// 场景：MVCC GC 需要等待 2 小时 Cooldown
func (mgcq *mvccGCQueue) shouldQueue(...) (bool, float64) {
    lastGC, err := repl.getQueueLastProcessed(ctx, mgcq.name)
    if err != nil {
        return false, 0
    }

    // 检查 Cooldown
    if time.Since(lastGC.GoTime()) < mvccGCQueueCooldownDuration {
        // 上次 GC 时间太近，跳过
        return false, 0
    }

    // 计算分数
    score := makeMVCCGCQueueScore(ctx, repl, ...)
    return score.ShouldQueue, score.FinalScore
}
```

**为什么不同策略？**

| 决策类型 | 适用场景 | 原因 |
|---------|---------|------|
| **即时** | replicaGC, under-replicated | 影响可用性，必须立即处理 |
| **滞后** | MVCC GC, Lease Transfer | 性能优化，可以延迟 |

#### 4.2.3 局部 vs 全局决策

**局部决策（Local）**：

```go
// 场景：Raft 日志截断只考虑本 Range 的 Follower
func (rlq *raftLogQueue) process(...) (bool, error) {
    status := repl.RaftStatus()

    // 查找最慢的 Follower
    minMatch := uint64(math.MaxUint64)
    for _, p := range status.Progress {
        if p.Match < minMatch {
            minMatch = p.Match
        }
    }

    // 截断到最慢 Follower 的位置
    truncateIndex := minMatch - raftLogTruncateThreshold
    return rlq.truncateLog(ctx, repl, truncateIndex)
}
```

**全局决策（Global）**：

```go
// 场景：Lease 转移需要考虑整个集群的负载分布
func (lq *leaseQueue) process(...) (bool, error) {
    // 1. 获取集群所有 Store 的状态
    storeList := lq.storePool.GetStoreList()

    // 2. 计算每个 Store 的 QPS
    storeQPS := make(map[roachpb.StoreID]float64)
    for _, store := range storeList.Stores {
        storeQPS[store.StoreID] = store.Capacity.QueriesPerSecond
    }

    // 3. 查找 QPS 最低的 Store
    targetStore := findLeastLoadedStore(storeQPS, desc)

    // 4. 转移 Lease
    return repl.AdminTransferLease(ctx, targetStore, false)
}
```

**为什么不同策略？**

| 决策类型 | 信息来源 | 优点 | 缺点 |
|---------|---------|------|------|
| **局部** | 本 Replica | 快速，无需协调 | 可能不是全局最优 |
| **全局** | Gossip, StorePool | 全局最优 | 需要协调，延迟高 |

### 4.3 当前策略的平衡点

#### 4.3.1 稳定性 vs 吞吐量

**稳定性优先场景**：

```go
// leaseQueue 的限流机制
func (lq *leaseQueue) process(...) (bool, error) {
    // 检查上次转移时间
    if lastTransfer, ok := lq.lastLeaseTransfer.Load().(time.Time); ok {
        if time.Since(lastTransfer) < MinLeaseTransferInterval.Get(&lq.store.cfg.Settings.SV) {
            // 距离上次转移太近，跳过
            return false, nil
        }
    }

    // 执行转移
    err := repl.AdminTransferLease(ctx, target, false)
    if err == nil {
        lq.lastLeaseTransfer.Store(timeutil.Now())
    }

    return err == nil, err
}
```

**原因**：
- 频繁转移 Lease 会导致请求抖动（需要等待新 Leaseholder 获取 Lease）
- `MinLeaseTransferInterval`（默认 1 秒）确保最小间隔
- 牺牲吞吐量（可能错过更好的转移时机），换取稳定性

**吞吐量优先场景**：

```go
// splitQueue 的并发控制
// 在 queueConfig 中
queueConfig{
    maxConcurrency: 4, // 允许 4 个 Split 同时执行
}
```

**原因**：
- Split 主要是 CPU 密集（RocksDB 扫描）
- 提高并发度可以加速 Split 处理
- 但不能无限提高（避免 CPU 峰值）

#### 4.3.2 公平性 vs 资源利用率

**公平性优先场景**：

```go
// priorityQueue 的 FIFO 语义
func (pq priorityQueue) Less(i, j int) bool {
    a, b := pq.sl[i], pq.sl[j]
    if a.priority == b.priority {
        // 优先级相同，FIFO
        return a.seq < b.seq
    }
    return a.priority > b.priority
}
```

**原因**：
- 避免低优先级任务饿死
- 优先级相同时，先入队的先处理（公平）

**资源利用率优先场景**：

```go
// processSem 的并发控制
// 在 processLoop 中
select {
case <-incoming:
    item := heap.Pop(mu.priorityQ)
    bq.processSem <- struct{}{} // 获取槽位
    go bq.processReplica(item)  // 立即启动 goroutine
}
```

**原因**：
- 一旦有空闲槽位，立即处理下一个 Replica
- 最大化 CPU 利用率
- 但可能导致低优先级任务延迟（资源利用率优先）

#### 4.3.3 响应延迟 vs 批处理效率

**响应延迟优先场景**：

```go
// leaseQueue, replicateQueue 的 timer 设置
queueConfig{
    timer: 0, // 贪婪处理，无延迟
}

// 在 processLoop 中
func (bq *baseQueue) timer(lastProcessDur time.Duration) time.Duration {
    return bq.impl.timer(lastProcessDur)
}

func (lq *leaseQueue) timer(lastProcessDur time.Duration) time.Duration {
    return 0 // 立即处理下一个
}
```

**原因**：
- Lease 转移和副本变更影响可用性
- 应该立即处理，不等待

**批处理效率优先场景**：

```go
// mvccGCQueue 的 timer 设置
queueConfig{
    timer: 1 * time.Second, // 每秒处理一个
}

func (mgcq *mvccGCQueue) timer(lastProcessDur time.Duration) time.Duration {
    interval := mvccGCQueueInterval.Get(&mgcq.store.cfg.Settings.SV)
    return interval
}
```

**原因**：
- MVCC GC 是 IO 密集操作
- 批处理可以减少磁盘 seek
- 允许一定延迟，换取更高效率

---

## 五、设计模式分析（Design Patterns）

### 5.1 显式设计模式

#### 5.1.1 优先级队列（Priority Queue）

**模式定义**：使用堆（Heap）维护元素优先级，确保最高优先级元素先被处理。

**实现**：

```go
type priorityQueue struct {
    seqGen int            // 序列号生成器（FIFO）
    sl     []*replicaItem // 堆数组
}

// 堆接口实现
func (pq priorityQueue) Less(i, j int) bool {
    a, b := pq.sl[i], pq.sl[j]
    if a.priority == b.priority {
        return a.seq < b.seq // FIFO
    }
    return a.priority > b.priority // 最大堆
}

func (pq *priorityQueue) Push(x interface{}) {
    item := x.(*replicaItem)
    item.index = len(pq.sl)
    pq.seqGen++
    item.seq = pq.seqGen
    pq.sl = append(pq.sl, item)
}

func (pq *priorityQueue) Pop() interface{} {
    n := len(pq.sl)
    item := pq.sl[n-1]
    item.index = -1
    pq.sl = pq.sl[0 : n-1]
    return item
}
```

**使用场景**：

```go
// 在 processLoop 中
item := heap.Pop(&bq.mu.priorityQ).(*replicaItem)
// 总是获取最高优先级的 Replica
```

**为什么选择这个模式？**

1. **时间复杂度**：
   - 插入：O(log N)
   - 弹出最大值：O(log N)
   - 更新优先级：O(log N)
   - 查找：O(1)（通过 `mu.replicas[rangeID]`）

2. **替代方案对比**：

| 方案 | 插入 | 弹出最大值 | 更新优先级 | 空间 |
|------|------|-----------|-----------|------|
| **优先级队列（堆）** | O(log N) | O(log N) | O(log N) | O(N) |
| 排序数组 | O(N) | O(1) | O(N) | O(N) |
| 链表 | O(1) | O(N) | O(1) | O(N) |
| 跳表 | O(log N) | O(log N) | O(log N) | O(N log N) |

3. **当前方案优势**：
   - 平衡的时间复杂度
   - Go 标准库支持（`container/heap`）
   - 内存占用小（无额外指针）

4. **FIFO 保证**：
   - 优先级相同时，先入队的先处理
   - 通过 `seqGen` 序列号实现
   - 避免低优先级任务饿死

#### 5.1.2 Purgatory（炼狱模式）

**模式定义**：失败的任务不立即重试，而是放入 "炼狱"，等待特定事件触发后统一重试。

**实现**：

```go
type baseQueue struct {
    mu struct {
        purgatory map[roachpb.RangeID]PurgatoryError // 炼狱映射
    }
}

// 加入炼狱
func (bq *baseQueue) addToPurgatory(ctx context.Context, item *replicaItem, err error) {
    bq.mu.Lock()
    defer bq.mu.Unlock()

    bq.mu.purgatory[item.rangeID] = err
    bq.removeLocked(item) // 从 priorityQ 移除
    bq.updateMetricsOnPurgatory()
}

// 处理炼狱
func (bq *baseQueue) processPurgatory() {
    bq.mu.Lock()
    purgatoryReplicas := bq.mu.purgatory
    bq.mu.purgatory = make(map[roachpb.RangeID]PurgatoryError) // 清空
    bq.mu.Unlock()

    // 将所有炼狱中的 Replica 重新入队
    for rangeID := range purgatoryReplicas {
        bq.AddAsync(ctx, rangeID, priority)
    }
}
```

**触发机制**：

```go
// 在 processLoop 中
purgatoryChan := bq.impl.purgatoryChan()
updateChan := bq.impl.updateChan()

for {
    select {
    case <-purgatoryChan: // 定时器
        bq.processPurgatory()
    case <-updateChan: // 外部事件（如 Gossip 更新）
        bq.processPurgatory()
    }
}
```

**具体队列的触发策略**：

```go
// leaseQueue: 定时器 + 无外部事件
func (lq *leaseQueue) purgatoryChan() <-chan time.Time {
    return lq.purgCh // time.NewTicker(10 * time.Second).C
}

func (lq *leaseQueue) updateChan() <-chan time.Time {
    return nil // 无外部事件
}

// replicateQueue: 定时器 + Gossip 更新
func (rq *replicateQueue) purgatoryChan() <-chan time.Time {
    return rq.purgCh // time.NewTicker(1 * time.Minute).C
}

func (rq *replicateQueue) updateChan() <-chan time.Time {
    return rq.gossipUpdateC // Gossip 更新通知
}
```

**为什么选择这个模式？**

1. **避免无效重试**：
   - 场景：目标节点不可用，立即重试 100 次 → 浪费 CPU
   - Purgatory：等待节点恢复（Gossip 更新）后统一重试

2. **批处理重试**：
   - 场景：Gossip 更新后，所有受影响的 Replica 需要重试
   - Purgatory：一次性重试所有炼狱中的 Replica

3. **替代方案对比**：

| 方案 | CPU 开销 | 延迟 | 复杂度 |
|------|---------|------|--------|
| **Purgatory** | 低（事件触发） | 中（等待触发） | 中 |
| 立即重试 | 极高（无效重试） | 低 | 低 |
| 指数退避 | 中 | 高（可能等待过久） | 中 |
| 事件队列 | 中 | 低 | 高 |

4. **事实标准**：
   - 分布式系统中常见模式
   - Raft 实现中也有类似机制（unreachable replicas）

#### 5.1.3 Template Method（模板方法）

**模式定义**：基类定义算法骨架，子类实现具体步骤。

**实现**：

```go
// baseQueue 定义处理骨架
func (bq *baseQueue) processReplica(ctx context.Context, item *replicaItem) (bool, error) {
    // 1. 重新获取 Replica（模板固定）
    repl, err := bq.getReplica(item.rangeID)
    if err != nil {
        return false, err
    }

    // 2. 检查前置条件（模板固定）
    confReader, err := bq.replicaCanBeProcessed(ctx, repl, true)
    if err != nil {
        return false, err
    }

    // 3. 调用具体队列的实现（子类实现）
    processed, err := bq.impl.process(ctx, repl.(*Replica), confReader, item.priority)

    // 4. 处理结果（模板固定）
    if err != nil {
        if errors.HasType(err, (*PurgatoryError)(nil)) {
            bq.addToPurgatory(ctx, item, err)
        }
        return false, err
    }

    return processed, nil
}

// queueImpl 接口（子类必须实现）
type queueImpl interface {
    shouldQueue(context.Context, hlc.ClockTimestamp, *Replica, spanconfig.StoreReader) (bool, float64)
    process(context.Context, *Replica, spanconfig.StoreReader, float64) (bool, error)
    timer(time.Duration) time.Duration
    purgatoryChan() <-chan time.Time
    updateChan() <-chan time.Time
}
```

**具体队列实现**：

```go
// leaseQueue 实现具体步骤
func (lq *leaseQueue) process(...) (bool, error) {
    // 具体的 Lease 转移逻辑
    return lq.transferLease(ctx, repl, target)
}

// mvccGCQueue 实现具体步骤
func (mgcq *mvccGCQueue) process(...) (bool, error) {
    // 具体的 MVCC GC 逻辑
    return mgcq.runGC(ctx, repl)
}
```

**为什么选择这个模式？**

1. **代码复用**：
   - 10 个队列共享 `processReplica` 的骨架逻辑
   - 避免重复实现前置条件检查、Purgatory 处理

2. **强制一致性**：
   - 所有队列的前置条件检查逻辑一致
   - 所有队列的错误处理逻辑一致

3. **扩展性**：
   - 新增队列只需实现 `queueImpl` 接口
   - 无需修改 `baseQueue`

4. **Go 语言特色**：
   - 使用接口（interface）而不是继承
   - 通过组合（composition）实现模板方法

#### 5.1.4 Strategy（策略模式）

**模式定义**：定义一系列算法，将每个算法封装起来，使它们可以互换。

**实现**：

```go
// 超时策略接口
type queueProcessTimeoutFunc func(*cluster.Settings, replicaInQueue) time.Duration

// 默认策略：固定超时
func defaultProcessTimeoutFunc(cs *cluster.Settings, _ replicaInQueue) time.Duration {
    return queueGuaranteedProcessingTimeBudget.Get(&cs.SV) // 1 分钟
}

// 速率限制策略：基于数据量
func makeRateLimitedTimeoutFunc(rateSettings *settings.ByteSizeSetting) queueProcessTimeoutFunc {
    return func(cs *cluster.Settings, r replicaInQueue) time.Duration {
        minimumTimeout := queueGuaranteedProcessingTimeBudget.Get(&cs.SV)

        repl, ok := r.(interface{ GetMVCCStats() enginepb.MVCCStats })
        if !ok {
            return minimumTimeout
        }

        // 根据数据量计算超时
        minSnapshotRate := rateSettings.Get(&cs.SV)
        estimatedDuration := time.Duration(repl.GetMVCCStats().Total()/minSnapshotRate) * time.Second
        timeout := estimatedDuration * permittedRangeScanSlowdown

        if timeout < minimumTimeout {
            timeout = minimumTimeout
        }

        return timeout
    }
}
```

**具体队列的策略选择**：

```go
// raftSnapshotQueue: 使用速率限制策略
queueConfig{
    processTimeoutFunc: makeRateLimitedTimeoutFunc(rebalanceSnapshotRate),
}
// 原因：Snapshot 大小差异巨大（1MB 到 10GB）
//       需要根据数据量动态调整超时

// consistencyQueue: 使用速率限制策略
queueConfig{
    processTimeoutFunc: makeRateLimitedTimeoutFunc(consistencyCheckRate),
}
// 原因：一致性检查需要扫描所有数据
//       大 Range 需要更长超时

// leaseQueue: 使用默认策略
queueConfig{
    processTimeoutFunc: defaultProcessTimeoutFunc,
}
// 原因：Lease 转移是固定时间操作（RPC 超时）
```

**为什么选择这个模式？**

1. **灵活性**：
   - 不同队列可以选择不同的超时策略
   - 运行时可以动态调整（通过 Settings）

2. **可测试性**：
   - 每个策略独立测试
   - 可以注入 mock 策略

3. **业界实践**：
   - 超时策略是分布式系统中的标准模式
   - 动态超时 vs 固定超时是经典权衡

#### 5.1.5 Observer（观察者模式）

**模式定义**：定义对象间的一对多依赖，当一个对象状态改变时，所有依赖者都得到通知。

**实现**：

```go
// replicateQueue 观察 Gossip 更新
type replicateQueue struct {
    gossipUpdateC <-chan time.Time // 观察者通道
}

// Gossip 系统发布更新
func (g *Gossip) RegisterCallback(pattern string, callback func()) {
    // 当 Gossip 更新时，触发回调
    g.callbacks[pattern] = callback
}

// replicateQueue 注册观察
func newReplicateQueue(store *Store, allocator allocatorimpl.Allocator) *replicateQueue {
    rq := &replicateQueue{
        gossipUpdateC: make(chan time.Time, 1),
    }

    // 监听节点 liveness 变化
    store.cfg.Gossip.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyNodeLivenessPrefix), func() {
        select {
        case rq.gossipUpdateC <- timeutil.Now():
        default:
            // Channel 已满，忽略
        }
    })

    return rq
}

// 处理更新
func (rq *replicateQueue) updateChan() <-chan time.Time {
    return rq.gossipUpdateC
}
```

**触发流程**：

```
Gossip 系统               replicateQueue
     |                           |
     |--Node 1 Down 事件-------->|
     |                           |--processPurgatory()
     |                           |  (重试所有炼狱中的 Replica)
     |                           |
     |--Node 1 Up 事件---------->|
     |                           |--processPurgatory()
```

**为什么选择这个模式？**

1. **解耦**：
   - Gossip 系统不需要知道 replicateQueue 的存在
   - replicateQueue 只依赖 Gossip 接口

2. **多对多**：
   - 多个队列可以观察同一个 Gossip 事件
   - 一个队列可以观察多个 Gossip 事件

3. **异步通知**：
   - 通过 Channel 实现非阻塞通知
   - 避免 Gossip 更新被队列处理阻塞

### 5.2 隐式设计模式

#### 5.2.1 Producer-Consumer（生产者-消费者）

**模式体现**：

```go
// 生产者：replicaScanner
func (rs *replicaScanner) scanLoop() {
    for _, repl := range allReplicas {
        for _, queue := range rs.queues {
            queue.MaybeAdd(repl) // 生产任务
        }
    }
}

// 消费者：baseQueue.processLoop
func (bq *baseQueue) processLoop(stopper *stop.Stopper) {
    for {
        select {
        case <-bq.incoming: // 接收任务
            item := heap.Pop(&bq.mu.priorityQ)
            go bq.processReplica(item) // 消费任务
        }
    }
}
```

**关键特性**：
- **缓冲队列**：`priorityQueue`（最多 10000 个）
- **多消费者**：`maxConcurrency` 个 goroutine
- **背压机制**：队列满时，丢弃最低优先级任务

#### 5.2.2 Semaphore（信号量）

**模式体现**：

```go
// processSem 控制并发度
type baseQueue struct {
    processSem chan struct{} // make(chan struct{}, maxConcurrency)
}

// 获取信号量
bq.processSem <- struct{}{} // 阻塞直到有空闲槽位
defer func() { <-bq.processSem }() // 释放槽位

// addOrMaybeAddSem 控制入队并发
alloc, err := bq.addOrMaybeAddSem.TryAcquire(ctx, 1)
if err != nil {
    // 超过并发限制，丢弃
    return
}
defer alloc.Release()
```

**关键特性**：
- **processSem**：阻塞式（确保不超过并发度）
- **addOrMaybeAddSem**：非阻塞式（TryAcquire，超过限制时丢弃）

#### 5.2.3 Token Bucket（令牌桶）

**模式体现**：

```go
// leaseQueue 的限流
func (lq *leaseQueue) process(...) (bool, error) {
    // 检查令牌
    if lastTransfer, ok := lq.lastLeaseTransfer.Load().(time.Time); ok {
        if time.Since(lastTransfer) < MinLeaseTransferInterval.Get(&lq.store.cfg.Settings.SV) {
            // 令牌不足，跳过
            return false, nil
        }
    }

    // 消耗令牌
    lq.lastLeaseTransfer.Store(timeutil.Now())

    // 执行操作
    return lq.transferLease(ctx, repl, target)
}
```

**关键特性**：
- 简化版 Token Bucket（只有一个令牌）
- 确保最小间隔（`MinLeaseTransferInterval`）

#### 5.2.4 Double-Checked Locking（双重检查锁定）

**模式体现**：

```go
// 在 addInternal 中
func (bq *baseQueue) addInternal(...) (bool, error) {
    // 第一次检查（无锁）
    if !desc.IsInitialized() {
        return false, errReplicaNotInitialized
    }

    // 获取锁
    bq.mu.Lock()
    defer bq.mu.Unlock()

    // 第二次检查（有锁）
    if bq.mu.stopped {
        return false, errQueueStopped
    }

    // 执行操作
    ...
}
```

**为什么使用？**
- 避免不必要的锁竞争
- 第一次检查过滤掉明显不满足条件的情况
- 第二次检查确保原子性

#### 5.2.5 RAII（Resource Acquisition Is Initialization）

**模式体现**：

```go
// allocatorToken 的 RAII 模式
func (lq *leaseQueue) process(...) (bool, error) {
    // 获取资源
    if tokenErr := repl.allocatorToken.TryAcquire(ctx, lq.name); tokenErr != nil {
        return false, tokenErr
    }

    // 确保释放（即使 panic）
    defer repl.allocatorToken.Release(ctx)

    // 使用资源
    ...
}
```

**Go 语言特色**：
- 使用 `defer` 实现 RAII
- 确保资源总是被释放（即使 panic）

### 5.3 为什么选择这些模式？

#### 5.3.1 与分布式系统的契合度

**优先级队列 + Purgatory**：
- **来源**：Raft 实现中的 "unreachable replicas" 机制
- **契合度**：高
  - 分布式系统中失败是常态
  - 需要区分 "永久失败" 和 "暂时失败"
  - Purgatory 是后者的优雅处理

**Observer + Event-Driven**：
- **来源**：发布-订阅模式（Pub/Sub）
- **契合度**：高
  - Gossip 系统天然是发布-订阅
  - 队列作为订阅者监听状态变化
  - 事件驱动避免轮询开销

#### 5.3.2 避免的模式

**为什么不使用 Active Object 模式？**

```go
// 反例：每个队列独立的 Actor
type leaseQueueActor struct {
    mailbox chan *Replica // 队列专用 Channel
}

func (lqa *leaseQueueActor) Run() {
    for repl := range lqa.mailbox {
        lqa.process(repl)
    }
}
```

**避免原因**：
- **无法统一优先级**：每个队列独立处理，无法抢占
- **资源浪费**：10 个队列 = 10 个独立 goroutine
- **难以限流**：每个队列独立限流，全局并发度难以控制

**为什么不使用 Command 模式？**

```go
// 反例：每个操作封装为 Command
type LeaseTransferCommand struct {
    repl   *Replica
    target roachpb.StoreID
}

func (cmd *LeaseTransferCommand) Execute() error {
    return cmd.repl.AdminTransferLease(ctx, cmd.target, false)
}
```

**避免原因**：
- **过度抽象**：增加复杂度，收益有限
- **Go 语言特色**：函数是一等公民，可以直接传递函数

---

*下篇将继续讲解具体运行示例、设计取舍与替代方案、总结与心智模型*
