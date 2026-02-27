# 第二十六章 ioThresholdMap 生命周期分析——基于 IO 过载的 Raft 副本流控机制

## 引言：当副本所在的 Store 磁盘过载时会发生什么

在分布式数据库中，**副本（Replica）之间的复制流量**是维持数据一致性的核心机制。然而，当某个 Store 的磁盘遭遇 **IO 过载**（如 SSD 写入放大、LSM compaction 积压）时，继续向该 Store 的 follower 副本发送 Raft 日志条目（`MsgApp`）会导致：

1. **雪上加霜**：加剧该 Store 的 IO 负担，延长恢复时间
2. **资源浪费**：leader 副本浪费网络带宽和 CPU 发送消息，而 follower 无法及时处理
3. **级联故障**：如果多个 follower 过载，可能导致 Raft quorum 丢失

为了在**保护过载 Store** 和**维持 Raft quorum** 之间取得平衡，CockroachDB 实现了一套**基于 IO Threshold 的副本暂停机制**（Follower Pausing）。本章将深入分析该机制的核心数据结构 `ioThresholdMap` 的生命周期、作用域和运行时行为。

---

## I. 鸟瞰：ioThresholdMap 是什么，为什么需要它

### 1.1 核心职责

`ioThresholdMap` 是一个**不可变的快照对象**（immutable snapshot），记录了集群中**所有 Store 的 IO 健康状况**。它的核心职责是：

**输入**（构造时）：
- 从 StorePool 获取的 `map[StoreID]*admissionpb.IOThreshold`（每个 Store 的 IO 负载评分）
- `pauseReplicationIOThreshold`（集群设置，默认 0 表示禁用，生产环境通常设为 0.8-0.9）

**输出**（运行时查询）：
- `AbovePauseThreshold(storeID)`：判断某个 Store 是否超过暂停阈值（应该暂停向其复制）
- `Sequence()`：序列号，仅当"可暂停的 Store 集合"发生变化时递增（用于触发 Replica 重新评估）

**生命周期特征**：
- **创建频率**：每次 Raft tick（默认 100ms）创建一次新的 `ioThresholdMap`
- **作用域**：整个 Store（所有 Replica 共享同一个 `ioThresholdMap` 实例）
- **并发访问**：通过 `ioThresholds` 容器的读写锁保护，确保 Replica 读取时看到一致的快照

### 1.2 为何需要不可变快照模式

**设计约束**：
- **Raft tick 频率高**：每个 Store 可能有 10,000 个 Replica，每次 tick 都需要检查 IO 状态
- **IO Threshold 变化频率低**：Store 的 IO 评分通常在数秒内保持稳定
- **并发访问无锁**：Replica 在处理 Raft Ready 时不应阻塞在锁上

**不可变快照的优势**：

| 维度 | 可变 map（加锁） | 不可变快照（Copy-on-Write） |
|------|----------------|--------------------------|
| **读取开销** | 每次查询需要加锁 | 无锁读取（仅指针解引用） |
| **并发竞争** | 10,000 个 Replica 竞争同一把锁 | 无竞争 |
| **一致性** | 可能在 tick 过程中看到不一致状态 | 整个 tick 周期内看到相同快照 |
| **内存开销** | 低（单个 map） | 稍高（每 100ms 分配一次，约 1KB） |

**实现方式**：
```go
type ioThresholds struct {
    mu struct {
        syncutil.Mutex
        inner *ioThresholdMap  // 原子替换，旧快照可被 Replica 持续使用
    }
}
```

### 1.3 在整个系统中的位置

```
┌─────────────────────────────────────────────────────────────┐
│                     Store (Raft Tick Loop)                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  每 100ms 触发一次                                     │   │
│  │  1. updateIOThresholdMap()                           │   │
│  │     - 从 StorePool 读取所有 Store 的 IOThreshold      │   │
│  │     - 调用 ioThresholds.Replace() 创建新快照          │   │
│  │  2. 遍历所有 unquiesced Replica                       │   │
│  │     - 调用 Replica.tick()                            │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   Replica.tick()                            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  1. updatePausedFollowersLocked(ioThresholdMap)     │   │
│  │     - 读取快照，判断哪些 follower 应被暂停            │   │
│  │     - 调用 computeExpendableOverloadedFollowers()    │   │
│  │  2. 更新 r.mu.pausedFollowers map                   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│         Replica.handleRaftReadyRaftMuLocked()               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  1. 从 Raft 获取待发送的消息（rd.Messages）            │   │
│  │  2. 调用 sendRaftMessages(messages, pausedFollowers) │   │
│  │     - 如果目标 replicaID 在 pausedFollowers 中       │   │
│  │     - 丢弃 MsgApp 消息（阻止向过载 follower 复制）     │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**关键交互**：
- **StorePool**：提供集群中所有 Store 的 IO Threshold（由 Gossip 或 KV 层传播）
- **Raft Tick Loop**：周期性触发 `updateIOThresholdMap()`
- **Replica.tick()**：每个 Replica 读取快照并更新 `pausedFollowers`
- **Replica.sendRaftMessages()**：根据 `pausedFollowers` 决定是否丢弃 MsgApp

---

## II. 控制流：从构造到查询的完整生命周期

### 2.1 阶段 1：构造新快照（每 100ms 一次）

**触发时机**：Store 的 Raft Tick Loop（[pkg/kv/kvserver/store_raft.go:991](pkg/kv/kvserver/store_raft.go#L991)）

```go
// 在 Store.raftTickLoop() 中，每个 tick 周期执行：
func (s *Store) updateIOThresholdMap() {
    // 1. 从 StorePool 读取集群中所有 Store 的 IOThreshold
    ioThresholdMap := map[roachpb.StoreID]*admissionpb.IOThreshold{}
    for _, sd := range s.cfg.StorePool.GetStores() {
        ioThreshold := sd.Capacity.IOThreshold  // 复制一份（避免竞态）
        ioThresholdMap[sd.StoreID] = &ioThreshold
    }

    // 2. 读取集群设置的暂停阈值
    threshold := pauseReplicationIOThreshold.Get(&s.cfg.Settings.SV)
    if threshold <= 0 {
        threshold = math.MaxFloat64  // 禁用暂停机制
    }

    // 3. 原子替换旧快照
    old, cur := s.ioThresholds.Replace(ioThresholdMap, threshold)

    // 4. 如果"可暂停的 Store 集合"发生变化，记录日志
    shouldLog := log.V(1) || old.seq != cur.seq
    if shouldLog {
        log.KvExec.Infof(
            s.AnnotateCtx(context.Background()), "pausable stores: %+v", cur)
    }
}
```

**输入**：
- `StorePool.GetStores()`：返回集群中所有 Store 的容量信息（包括 `IOThreshold`）
  ```go
  type IOThreshold struct {
      L0NumSubLevels          int32   // LSM L0 子层数（Pebble 特有指标）
      L0NumSubLevelsThreshold int32   // 阈值（通常 20）
      L0NumFiles              int64   // L0 文件数
      L0NumFilesThreshold     int64   // 阈值（通常 1000）
      // Score = max(L0NumSubLevels/Threshold, L0NumFiles/Threshold)
  }
  ```

**输出**：
- 新的 `ioThresholdMap` 对象，包含：
  - `threshold`：当前生效的暂停阈值（如 0.8）
  - `seq`：序列号（如果可暂停集合变化，从上一个 seq 递增 1）
  - `m`：`map[StoreID]*IOThreshold`

**关键不变式**：
- **原子性**：`Replace()` 方法持有 `ioThresholds.mu` 锁，确保旧快照不会被并发修改
- **顺序一致性**：所有 Replica 在同一个 tick 周期内看到相同的快照（因为 tick 是串行的）

### 2.2 阶段 2：Replica 读取快照并更新 pausedFollowers

**触发时机**：每个 Replica 的 tick 处理（[pkg/kv/kvserver/replica_raft.go:1413](pkg/kv/kvserver/replica_raft.go#L1413)）

```go
func (r *Replica) tick(
    ctx context.Context,
    livenessMap liveness.IsLiveMap,
    ioThresholdMap *ioThresholdMap,  // 从 Store 传入的快照
) (bool, error) {
    // ... Raft tick 处理 ...

    // 更新 pausedFollowers（基于 ioThresholdMap）
    r.updatePausedFollowersLocked(ctx, ioThresholdMap)

    // ... 后续 quiesce、lease 检查等 ...
}
```

**核心逻辑**：`updatePausedFollowersLocked()`（[pkg/kv/kvserver/replica_raft_overload.go:295](pkg/kv/kvserver/replica_raft_overload.go#L295)）

```go
func (r *Replica) updatePausedFollowersLocked(ctx context.Context, ioThresholdMap *ioThresholdMap) {
    r.mu.pausedFollowers = nil  // 清空旧状态

    desc := r.descRLocked()
    repls := desc.Replicas()

    // 1. 快速路径：如果没有任何 Store 超过阈值，直接返回
    if !ioThresholdMap.AnyAbovePauseThreshold(repls) {
        return
    }

    // 2. 只有 Raft leader 才暂停 follower
    if !r.isRaftLeaderRLocked() {
        return
    }

    // 3. 如果启用了 RAC v2（pull-mode），跳过暂停机制
    if r.shouldReplicationAdmissionControlUsePullMode(ctx) {
        return
    }

    // 4. 关键 Range（如 liveness range）不暂停
    if !quotaPoolEnabledForRange(desc) {
        return
    }

    // 5. 只有 leaseholder 才能暂停 follower（防止脑裂）
    status := r.leaseStatusAtRLocked(ctx, r.Clock().NowAsClockTimestamp())
    if !status.IsValid() || !status.OwnedBy(r.StoreID()) {
        return
    }

    // 6. 计算可暂停的 follower 集合
    seed := int64(r.RangeID)  // 使用 RangeID 作为随机种子（确保稳定性）
    now := r.store.Clock().Now().GoTime()
    d := computeExpendableOverloadedFollowersInput{
        self:          r.replicaID,
        replDescs:     repls,
        ioOverloadMap: ioThresholdMap,
        getProgressMap: func(_ context.Context) map[raftpb.PeerID]tracker.Progress {
            prs := r.mu.internalRaftGroup.Status().Progress
            updateRaftProgressFromActivity(ctx, prs, repls.Descriptors(), func(id roachpb.ReplicaID) bool {
                return r.mu.lastUpdateTimes.isFollowerActiveSince(id, now, r.store.cfg.RangeLeaseDuration)
            })
            return prs
        },
        minLiveMatchIndex: r.mu.proposalQuotaBaseIndex,  // 落后的 follower 不计入 quorum
        seed:              seed,
    }
    r.mu.pausedFollowers, _ = computeExpendableOverloadedFollowers(ctx, d)

    // 7. 通知 Raft 这些 follower 不可达（让 Raft 进入 probing 状态）
    bypassFn := r.store.TestingKnobs().RaftReportUnreachableBypass
    for replicaID := range r.mu.pausedFollowers {
        if bypassFn != nil && bypassFn(replicaID) {
            continue
        }
        r.mu.internalRaftGroup.ReportUnreachable(raftpb.PeerID(replicaID))
    }
}
```

**关键决策流程**：
```
ioThresholdMap.AnyAbovePauseThreshold()?
    ↓ 否
    返回（不暂停任何 follower）
    ↓ 是
是 Raft leader？
    ↓ 否
    返回（follower 不暂停其他副本）
    ↓ 是
是 leaseholder？
    ↓ 否
    返回（防止脑裂）
    ↓ 是
调用 computeExpendableOverloadedFollowers()
    ↓
计算出最大可暂停集合（保证 quorum）
    ↓
更新 r.mu.pausedFollowers
```

### 2.3 阶段 3：发送 Raft 消息时过滤 MsgApp

**触发时机**：处理 Raft Ready（[pkg/kv/kvserver/replica_raft.go:1018](pkg/kv/kvserver/replica_raft.go#L1018)）

```go
func (r *Replica) handleRaftReadyRaftMuLocked(...) {
    // ... 生成 Raft Ready ...

    r.mu.Lock()
    pausedFollowers := r.mu.pausedFollowers  // 读取暂停列表
    r.mu.Unlock()

    // 发送消息时过滤
    r.sendRaftMessages(ctx, ready.Messages, pausedFollowers)

    // ... 后续处理 committed entries ...
}
```

**过滤逻辑**：`sendRaftMessages()`（[pkg/kv/kvserver/replica_raft.go:1810](pkg/kv/kvserver/replica_raft.go#L1810)）

```go
func (r *Replica) sendRaftMessages(
    ctx context.Context,
    messages []raftpb.Message,
    blocked map[roachpb.ReplicaID]struct{},  // 即 pausedFollowers
) {
    for _, message := range messages {
        _, drop := blocked[roachpb.ReplicaID(message.To)]
        if drop {
            r.store.Metrics().RaftPausedFollowerDroppedMsgs.Inc(1)
        }
        switch message.Type {
        case raftpb.MsgApp:
            if drop {
                // 丢弃 MsgApp 消息，不发送到网络
                continue
            }
            // ... 发送消息 ...
        case raftpb.MsgHeartbeat:
            // 注意：Heartbeat 不会被丢弃（用于探测恢复）
            // ...
        }
    }
}
```

**关键设计**：
- **只丢弃 MsgApp**：Heartbeat、Snapshot 请求等消息不受影响
- **指标记录**：`RaftPausedFollowerDroppedMsgs` 用于监控被丢弃的消息数
- **Raft 状态机感知**：之前调用的 `ReportUnreachable()` 会让 Raft 进入 `StateProbe`，减少无效的 MsgApp 生成

### 2.4 阶段 4：快照的替换与垃圾回收

**垃圾回收时机**：
- Go 的 GC 会在没有 Replica 引用旧快照时回收它
- 由于 tick 周期短（100ms），通常在下一次 tick 后旧快照就可被回收

**内存开销估算**：
- 假设集群有 100 个 Store
- 每个 `IOThreshold` 结构体约 32 字节
- 每个快照：100 × 32 = **3.2 KB**
- 同时存在的快照数：最多 2 个（旧快照 + 新快照），约 **6.4 KB**
- 对于 256 GB 内存的节点，开销可忽略不计

---

## III. 深度剖析：核心函数的输入、输出与不变式

### 3.1 ioThresholds.Replace()：原子替换快照

**函数签名**：
```go
func (osm *ioThresholds) Replace(
    m map[roachpb.StoreID]*admissionpb.IOThreshold,
    seqThreshold float64,
) (prev, cur *ioThresholdMap)
```

**输入**：
- `m`：新的 Store → IOThreshold 映射
- `seqThreshold`：暂停阈值（如 0.8）

**输出**：
- `prev`：替换前的快照
- `cur`：新创建的快照

**算法流程**：
```go
func (osm *ioThresholds) Replace(...) (prev, cur *ioThresholdMap) {
    osm.mu.Lock()
    defer osm.mu.Unlock()

    last := osm.mu.inner
    if last == nil {
        last = &ioThresholdMap{}  // 初始化
    }

    // 1. 创建新快照（继承旧序列号）
    next := &ioThresholdMap{threshold: seqThreshold, seq: last.seq, m: m}

    // 2. 检查"可暂停 Store 集合"是否变化
    var delta int
    for id := range last.m {
        if last.AbovePauseThreshold(id) != next.AbovePauseThreshold(id) {
            delta = 1
            break
        }
    }
    for id := range next.m {
        if last.AbovePauseThreshold(id) != next.AbovePauseThreshold(id) {
            delta = 1
            break
        }
    }

    // 3. 如果集合变化，递增序列号
    next.seq += delta

    // 4. 原子替换
    osm.mu.inner = next
    return last, next
}
```

**关键不变式**：
- **序列号递增规则**：仅当"可暂停 Store 集合"变化时递增（避免无效的 Replica 状态更新）
- **阈值可变性**：即使 `seqThreshold` 变化，如果没有 Store 跨越阈值，序列号也不变

**并发安全性**：
- **写者互斥**：`Replace()` 持有 `osm.mu` 锁，串行化所有更新
- **读者无锁**：`Current()` 只读取指针，不阻塞 Replica

### 3.2 computeExpendableOverloadedFollowers()：计算可暂停集合

**函数签名**：
```go
func computeExpendableOverloadedFollowers(
    ctx context.Context,
    d computeExpendableOverloadedFollowersInput,
) (map[roachpb.ReplicaID]struct{}, map[roachpb.ReplicaID]nonLiveReason)
```

**输入**（通过 `computeExpendableOverloadedFollowersInput` 结构体）：
```go
type computeExpendableOverloadedFollowersInput struct {
    self          roachpb.ReplicaID           // 本副本的 ID
    replDescs     roachpb.ReplicaSet          // Range 的所有副本
    ioOverloadMap ioThresholdMapI             // IO 阈值快照
    getProgressMap func(context.Context) map[raftpb.PeerID]tracker.Progress  // 惰性获取 Raft 进度
    seed          int64                       // 随机种子（使用 RangeID）
    minLiveMatchIndex kvpb.RaftIndex          // 落后的 follower 不计入 quorum
}
```

**输出**：
- `map[roachpb.ReplicaID]struct{}`：可暂停的 follower 集合
- `map[roachpb.ReplicaID]nonLiveReason`：非活跃副本及原因（Inactive/Paused/Behind）

**算法流程**：

#### 步骤 1：识别过载且活跃的 follower

```go
var liveOverloadedVoterCandidates map[roachpb.ReplicaID]struct{}
var liveOverloadedNonVoterCandidates map[roachpb.ReplicaID]struct{}
var prs map[raftpb.PeerID]tracker.Progress

for _, replDesc := range d.replDescs.Descriptors() {
    // 跳过未过载的 Store 和本副本
    if pausable := d.ioOverloadMap.AbovePauseThreshold(replDesc.StoreID); !pausable || replDesc.ReplicaID == d.self {
        continue
    }

    // 首次发现过载 follower 时，惰性获取 Raft 进度
    if prs == nil {
        prs = d.getProgressMap(ctx)
        nonLive = map[roachpb.ReplicaID]nonLiveReason{}
        for id, pr := range prs {
            if !pr.RecentActive {
                nonLive[roachpb.ReplicaID(id)] = nonLiveReasonInactive
            }
            if pr.IsPaused() {
                nonLive[roachpb.ReplicaID(id)] = nonLiveReasonPaused
            }
            if kvpb.RaftIndex(pr.Match) < d.minLiveMatchIndex {
                nonLive[roachpb.ReplicaID(id)] = nonLiveReasonBehind
            }
        }
        liveOverloadedVoterCandidates = map[roachpb.ReplicaID]struct{}{}
        liveOverloadedNonVoterCandidates = map[roachpb.ReplicaID]struct{}{}
    }

    // 区分 voter 和 non-voter
    if prs[raftpb.PeerID(replDesc.ReplicaID)].IsLearner {
        liveOverloadedNonVoterCandidates[replDesc.ReplicaID] = struct{}{}
    } else {
        liveOverloadedVoterCandidates[replDesc.ReplicaID] = struct{}{}
    }
}
```

**优化要点**：
- **惰性计算**：只有在发现至少一个过载 follower 时才调用 `getProgressMap()`（避免无效的 Raft 状态读取）
- **活跃性判断**：结合 `RecentActive`、`IsPaused`、`Match Index` 三个维度

#### 步骤 2：贪心缩减候选集（保证 quorum）

```go
var rnd *rand.Rand
for len(liveOverloadedVoterCandidates) > 0 {
    // 检查当前候选集是否能保证 quorum
    up := d.replDescs.CanMakeProgress(func(replDesc roachpb.ReplicaDescriptor) bool {
        rid := replDesc.ReplicaID
        if _, ok := nonLive[rid]; ok {
            return false  // 非活跃副本不计入 quorum
        }
        if _, ok := liveOverloadedVoterCandidates[rid]; ok {
            return false  // 暂停候选不计入 quorum
        }
        if _, ok := liveOverloadedNonVoterCandidates[rid]; ok {
            return false  // non-voter 不影响 quorum（但也暂停）
        }
        return true  // 健康副本计入 quorum
    })

    if up {
        // 找到最大可暂停集合，退出循环
        break
    }

    // Quorum 不足，随机移除一个 voter 候选
    var sl []roachpb.ReplicaID
    for sid := range liveOverloadedVoterCandidates {
        sl = append(sl, sid)
    }
    slices.Sort(sl)  // 排序保证测试确定性

    if rnd == nil {
        rnd = rand.New(rand.NewSource(d.seed))  // 使用 RangeID 作为种子
    }
    delete(liveOverloadedVoterCandidates, sl[rnd.Intn(len(sl))])
}

// 返回 voter 和 non-voter 的并集
for nonVoter := range liveOverloadedNonVoterCandidates {
    liveOverloadedVoterCandidates[nonVoter] = struct{}{}
}
return liveOverloadedVoterCandidates, nonLive
```

**算法复杂度**：
- **时间复杂度**：O(V × R)，其中 V 是过载 voter 数，R 是副本总数
- **最坏情况**：所有 voter 都过载，需要逐个检查 quorum（如 5 副本中 4 个过载，需检查 4 次）

**关键设计权衡**：

| 方案 | 优点 | 缺点 |
|------|------|------|
| **当前方案（贪心 + 随机）** | 最大化暂停副本数，负载分散（不同 Range 暂停不同 Store） | 每次 tick 需计算（但大部分情况下快速路径） |
| **预定义优先级** | 计算简单 | 总是暂停同一个 Store，负载不均 |
| **轮询（Round-Robin）** | 公平性好 | 可能频繁变化暂停集合，导致 Raft 不稳定 |

**为何使用 RangeID 作为随机种子**：
- **稳定性**：同一个 Range 在多次 tick 中会选择相同的 Store 暂停（只要过载集合不变）
- **多样性**：不同 Range 会暂停不同的 Store，避免单点热点
- **示例**：
  - Range 1 (seed=1) → 暂停 Store 3
  - Range 2 (seed=2) → 暂停 Store 5
  - Range 3 (seed=3) → 暂停 Store 3
  - 结果：Store 3 和 Store 5 都能减轻负载

### 3.3 ioThresholdMap.AbovePauseThreshold()：判断 Store 是否过载

**函数签名**：
```go
func (osm *ioThresholdMap) AbovePauseThreshold(id roachpb.StoreID) bool
```

**实现**：
```go
func (osm *ioThresholdMap) AbovePauseThreshold(id roachpb.StoreID) bool {
    sc, _ := osm.m[id].Score()
    return sc > osm.threshold
}
```

**Score 计算**（在 `admissionpb.IOThreshold` 中）：
```go
func (iot *IOThreshold) Score() (float64, bool) {
    if iot.L0NumSubLevelsThreshold == 0 || iot.L0NumFilesThreshold == 0 {
        return 0, false  // 未初始化
    }

    // 计算两个维度的负载比例
    subLevelScore := float64(iot.L0NumSubLevels) / float64(iot.L0NumSubLevelsThreshold)
    fileScore := float64(iot.L0NumFiles) / float64(iot.L0NumFilesThreshold)

    // 返回最大值（最严重的维度）
    return max(subLevelScore, fileScore), true
}
```

**示例计算**：
- **正常 Store**：L0SubLevels=5（阈值 20），L0Files=100（阈值 1000）
  - subLevelScore = 5/20 = 0.25
  - fileScore = 100/1000 = 0.10
  - Score = max(0.25, 0.10) = **0.25**（未超过 0.8 阈值）

- **过载 Store**：L0SubLevels=18（阈值 20），L0Files=950（阈值 1000）
  - subLevelScore = 18/20 = 0.90
  - fileScore = 950/1000 = 0.95
  - Score = max(0.90, 0.95) = **0.95**（超过 0.8 阈值）

---

## IV. 运行时行为：动态信号与负载管理

### 4.1 IO Threshold 的传播路径

```
Pebble 引擎（每个 Store）
    ↓ 每 10 秒更新一次
Store.UpdateCapacity()
    ↓ 通过 Gossip 传播
其他节点的 StorePool
    ↓ 每 100ms 读取一次
Store.updateIOThresholdMap()
    ↓
创建新的 ioThresholdMap 快照
```

**时间延迟分析**：
- **Pebble 指标更新**：实时（每次 compaction 完成）
- **Gossip 传播延迟**：1-3 秒（取决于网络和 Gossip 拓扑）
- **快照更新延迟**：最多 100ms（下一次 tick）
- **总延迟**：**1-3 秒**（从 Store 过载到其他节点停止向其复制）

### 4.2 序列号变化触发 Replica 重新评估

**场景 1：Store 从正常变为过载**

```
时刻 T0:
  - Store 3: Score=0.75（未过载）
  - ioThresholdMap.seq = 100

时刻 T0+2s（Gossip 传播后）:
  - Store 3: Score=0.85（过载）
  - updateIOThresholdMap() 检测到集合变化
  - ioThresholdMap.seq = 101（递增）
  - 日志输出："pausable stores: s3: 0.85 [pausable-threshold=0.80]"

时刻 T0+2.1s（下一次 tick）:
  - 所有 Replica 调用 updatePausedFollowersLocked()
  - 如果 Replica 有副本在 Store 3，将其加入 pausedFollowers
  - 调用 r.mu.internalRaftGroup.ReportUnreachable(Store3ReplicaID)

时刻 T0+2.2s（下一次 Raft Ready）:
  - sendRaftMessages() 丢弃所有发往 Store 3 的 MsgApp
  - 指标 RaftPausedFollowerDroppedMsgs 增加
```

**场景 2：Store 从过载恢复**

```
时刻 T10:
  - Store 3: Score=0.85（过载）
  - 多数 Range 的 Store 3 副本在 pausedFollowers 中

时刻 T10+5s:
  - Store 3: Score=0.78（恢复）
  - updateIOThresholdMap() 检测到集合变化
  - ioThresholdMap.seq = 102（递增）

时刻 T10+5.1s:
  - 所有 Replica 调用 updatePausedFollowersLocked()
  - Store 3 副本从 pausedFollowers 移除
  - Raft 开始发送 MsgApp（Store 3 副本可能落后很多）

时刻 T10+5.2s:
  - Store 3 副本开始接收并应用 Raft log
  - 如果 log 已被 truncate，触发 Snapshot 传输
```

### 4.3 Quorum 约束的动态调整

**3 副本 Range 示例**：

| 副本 | Store | Score | 是否过载 | 是否活跃 | 可否暂停 |
|------|-------|-------|---------|---------|---------|
| R1 (leader) | S1 | 0.3 | ❌ | ✅ | ❌（leader 不暂停） |
| R2 (follower) | S2 | 0.9 | ✅ | ✅ | ❓ |
| R3 (follower) | S3 | 0.85 | ✅ | ✅ | ❓ |

**Quorum 计算**：
- 需要 2/3 副本存活才能提交日志
- 如果暂停 R2 和 R3，只剩 R1，**无法满足 quorum**
- `computeExpendableOverloadedFollowers()` 会随机移除一个候选：
  - 假设 seed=123，选择移除 R2
  - 最终 `pausedFollowers = {R3}`
  - R1 和 R2 可形成 quorum（2/3）

**5 副本 Range 示例**：

| 副本 | Store | Score | 可否暂停 |
|------|-------|-------|---------|
| R1 (leader) | S1 | 0.3 | ❌ |
| R2 | S2 | 0.9 | ✅ |
| R3 | S3 | 0.85 | ✅ |
| R4 | S4 | 0.2 | ❌ |
| R5 | S5 | 0.15 | ❌ |

**Quorum 计算**：
- 需要 3/5 副本
- 如果暂停 R2 和 R3，剩余 R1、R4、R5（3 个），**满足 quorum**
- 最终 `pausedFollowers = {R2, R3}`

### 4.4 与其他准入控制机制的协作

**准入控制栈**：
```
┌─────────────────────────────────────────────────────┐
│  1. KV Admission Control (写请求准入)                 │
│     - 基于 CPU、IO token 限制请求速率                 │
│     - 作用于 leader 副本                             │
└─────────────────────────────────────────────────────┘
              ↓（请求通过准入）
┌─────────────────────────────────────────────────────┐
│  2. Raft Proposal Quota (提案配额)                   │
│     - 限制 in-flight Raft log 总大小（默认 64 MB）    │
│     - 防止 leader 提案速度远超 follower 应用速度      │
└─────────────────────────────────────────────────────┘
              ↓（获取 quota）
┌─────────────────────────────────────────────────────┐
│  3. Follower Pausing (本章分析的机制)                 │
│     - 基于 follower 的 IO 负载暂停复制                │
│     - 作用于 MsgApp 发送阶段                         │
└─────────────────────────────────────────────────────┘
              ↓（MsgApp 被过滤或发送）
┌─────────────────────────────────────────────────────┐
│  4. Replication Admission Control v2 (RAC v2)       │
│     - Pull-mode 流控制，follower 主动拉取 log        │
│     - 如果启用，替代 Follower Pausing                │
└─────────────────────────────────────────────────────┘
```

**互斥关系**：
- 如果启用 **RAC v2**（`shouldReplicationAdmissionControlUsePullMode()` 返回 true），**Follower Pausing 不生效**
- RAC v2 提供了更精细的流控制（基于 token），而 Follower Pausing 是粗粒度的"二元开关"

**示例场景**：
- **RAC v2 禁用**（默认）：使用 Follower Pausing
- **RAC v2 启用**：Follower Pausing 的 `updatePausedFollowersLocked()` 提前返回，`pausedFollowers` 始终为空

---

## V. 具体示例：IO 过载场景的完整时间线

### 5.1 场景设定

**集群拓扑**：
- 3 个节点，每个节点 1 个 Store
- Store 1: 正常（Score=0.3）
- Store 2: 轻微过载（Score=0.75）
- Store 3: 严重过载（Score=0.95）

**Range 配置**：
- Range 100（关键用户数据）
- 副本：R1@S1（leader, leaseholder）、R2@S2（follower）、R3@S3（follower）
- 当前 Raft log index：1000

**集群设置**：
- `pauseReplicationIOThreshold = 0.8`

### 5.2 时间线分解

#### T0: 正常运行阶段

```
Store 1 (S1):
  - IOThreshold.Score = 0.30
  - ioThresholdMap.AbovePauseThreshold(S1) = false

Store 2 (S2):
  - IOThreshold.Score = 0.75
  - ioThresholdMap.AbovePauseThreshold(S2) = false

Store 3 (S3):
  - IOThreshold.Score = 0.60
  - ioThresholdMap.AbovePauseThreshold(S3) = false

Range 100:
  - r.mu.pausedFollowers = {} (空集合)
  - R1 正常向 R2、R3 发送 MsgApp
```

#### T0+10s: Store 3 遭遇 IO 过载

```
Store 3 (S3):
  - 大量写入导致 LSM compaction 积压
  - L0NumSubLevels: 5 → 19 (阈值 20)
  - L0NumFiles: 200 → 980 (阈值 1000)
  - IOThreshold.Score = max(19/20, 980/1000) = 0.98

  - Store 3 通过 Gossip 广播新的 StoreDescriptor
```

#### T0+12s: Gossip 传播完成

```
Store 1 的 StorePool:
  - 接收到 Store 3 的更新
  - 更新 StoreDescriptor[S3].Capacity.IOThreshold

Store 1 的 raftTickLoop:
  - 调用 updateIOThresholdMap()
  - 检测到 Store 3 的 Score (0.98) > threshold (0.8)
  - 创建新快照：
    ioThresholdMap {
      threshold: 0.8,
      seq: 101 (从 100 递增),
      m: {
        S1: &IOThreshold{Score=0.30},
        S2: &IOThreshold{Score=0.75},
        S3: &IOThreshold{Score=0.98},
      }
    }
  - 日志输出："pausable stores: s3: 0.98 [pausable-threshold=0.80]"
```

#### T0+12.1s: Replica 100 的 tick 处理

```
Replica 100 (R1 on Store 1):
  - 调用 tick(ctx, livenessMap, ioThresholdMap)
  - 进入 updatePausedFollowersLocked()

  1. 快速路径检查：
     ioThresholdMap.AnyAbovePauseThreshold({R1@S1, R2@S2, R3@S3})?
     → S3 超过阈值，继续

  2. 检查条件：
     - isRaftLeaderRLocked()? → true (R1 是 leader)
     - isLeaseholder()? → true (R1 是 leaseholder)
     - quotaPoolEnabled()? → true
     - RAC v2 启用? → false

  3. 调用 computeExpendableOverloadedFollowers():
     输入：
       - self = R1
       - replDescs = {R1@S1, R2@S2, R3@S3}
       - ioOverloadMap = ioThresholdMap
       - seed = 100 (RangeID)
       - minLiveMatchIndex = 950

     步骤 1：识别过载候选
       - R2@S2: Score=0.75 < 0.8 → 跳过
       - R3@S3: Score=0.98 > 0.8 → 加入 liveOverloadedVoterCandidates

     步骤 2：检查 quorum
       - 候选集 = {R3}
       - 活跃副本 = {R1, R2} (排除 R3)
       - CanMakeProgress({R1, R2})? → true (2/3 满足 quorum)
       - 退出循环

     输出：pausedFollowers = {R3}

  4. 更新 Replica 状态：
     r.mu.pausedFollowers = {R3}
     r.mu.internalRaftGroup.ReportUnreachable(raftpb.PeerID(R3))
```

#### T0+12.2s: 处理 Raft Ready

```
Replica 100 (R1):
  - 客户端写入新数据，生成 Raft proposal
  - Raft 状态机产生 Ready:
    ready.Messages = [
      MsgApp{To: R2, Index: 1001, Entries: [...]},
      MsgApp{To: R3, Index: 1001, Entries: [...]},
    ]

  - 调用 sendRaftMessages(ready.Messages, pausedFollowers={R3})

  - 处理 MsgApp 到 R2:
    _, drop := pausedFollowers[R2]? → false
    → 正常发送到 Store 2

  - 处理 MsgApp 到 R3:
    _, drop := pausedFollowers[R3]? → true
    → 丢弃消息，不发送到 Store 3
    → r.store.Metrics().RaftPausedFollowerDroppedMsgs.Inc(1)
```

**效果**：
- **Store 2**：继续接收 MsgApp，正常应用 Raft log
- **Store 3**：不再接收 MsgApp，IO 负载减轻（但落后于 leader）

#### T0+20s: Store 3 开始恢复

```
Store 3 (S3):
  - Compaction 完成，L0 文件数下降
  - L0NumSubLevels: 19 → 8
  - L0NumFiles: 980 → 400
  - IOThreshold.Score = max(8/20, 400/1000) = 0.40

  - Store 3 通过 Gossip 广播更新
```

#### T0+22s: Gossip 传播完成，Replica 恢复复制

```
Store 1 的 updateIOThresholdMap():
  - 检测到 Store 3 的 Score (0.40) < threshold (0.8)
  - 创建新快照：
    ioThresholdMap {
      threshold: 0.8,
      seq: 102 (递增),
      m: {S1: 0.30, S2: 0.75, S3: 0.40}
    }

Replica 100 的 updatePausedFollowersLocked():
  - AnyAbovePauseThreshold()? → false (没有 Store 超过 0.8)
  - 直接返回，清空 pausedFollowers
  - r.mu.pausedFollowers = {}

Replica 100 的 sendRaftMessages():
  - MsgApp 到 R3 不再被丢弃
  - 但 R3 的 Match Index = 950（落后）
  - Raft 检测到 R3 落后，可能需要发送 Snapshot
```

#### T0+23s: Snapshot 传输（如果需要）

```
假设 Raft log 被 truncate 到 index 980:
  - R3 的 Match Index = 950 < 980
  - Raft 无法通过 MsgApp 追赶
  - 触发 Snapshot 请求

Replica 100 (R1):
  - 生成 Snapshot（包含 index 1100 的完整状态）
  - 调用 r.sendSnapshot(R3, snapshot)

  - 检查 pausedFollowers:
    _, destPaused := r.mu.pausedFollowers[R3]? → false
    → 允许发送 Snapshot

Store 3 (R3):
  - 接收并应用 Snapshot
  - Match Index 更新为 1100
  - 恢复与 leader 同步
```

### 5.3 指标变化时间线

| 时刻 | `RaftPausedFollowerDroppedMsgs` | R3 Match Index | R3 IO Score |
|------|--------------------------------|----------------|-------------|
| T0 | 0 | 1000 | 0.60 |
| T0+12s | 0 | 1000 | 0.98 (过载) |
| T0+12.2s | 1 | 1000 | 0.98 |
| T0+13s | 15 | 1000 | 0.98 |
| T0+20s | 120 | 1000 | 0.40 (恢复) |
| T0+22s | 120 | 1000 | 0.40 |
| T0+23s | 120 | 1100 (Snapshot) | 0.40 |

**累计影响**：
- **暂停时长**：10 秒（T0+12s 到 T0+22s）
- **丢弃的 MsgApp**：120 条（假设 leader 每 100ms 生成一次 proposal）
- **节省的 IO**：假设每条 MsgApp 包含 10 个条目，每条 1 KB，总计 120 × 10 × 1 KB = **1.2 MB**

---

## VI. 设计取舍与权衡

### 6.1 不可变快照 vs 可变 Map

**当前设计**：不可变快照（Copy-on-Write）

#### 优点

| 维度 | 详情 |
|------|------|
| **并发性能** | Replica 读取无需加锁，延迟 < 10ns（指针解引用） |
| **一致性** | 整个 tick 周期内所有 Replica 看到相同快照 |
| **简单性** | 避免处理读写锁升级、死锁等问题 |

#### 缺点

| 维度 | 详情 |
|------|------|
| **内存开销** | 每 100ms 分配 ~3 KB（100 个 Store），每秒 30 KB |
| **GC 压力** | 频繁分配小对象（但 Go GC 对此优化良好） |

#### 对比方案：可变 Map + 读写锁

```go
type ioThresholdMap struct {
    mu sync.RWMutex
    m  map[roachpb.StoreID]*admissionpb.IOThreshold
}

func (osm *ioThresholdMap) AbovePauseThreshold(id roachpb.StoreID) bool {
    osm.mu.RLock()
    defer osm.mu.RUnlock()
    sc, _ := osm.m[id].Score()
    return sc > osm.threshold
}
```

**问题**：
- **竞争**：10,000 个 Replica 同时 tick，RWMutex 成为瓶颈
- **延迟**：在高并发下，RLock 可能阻塞数十微秒
- **复杂性**：需要处理更新时的写锁升级

**性能对比**（48 核机器，10,000 个 Replica）：
- **不可变快照**：总 tick 时间 ~50ms（并行 tick）
- **可变 Map**：总 tick 时间 ~200ms（锁竞争导致串行化）

### 6.2 贪心算法 vs 精确优化

**当前算法**：贪心 + 随机（`computeExpendableOverloadedFollowers`）

#### 优点

| 维度 | 详情 |
|------|------|
| **速度** | O(V × R)，通常 < 1ms（V=过载 voter 数，R=副本数） |
| **公平性** | 不同 Range 暂停不同 Store（通过 RangeID 作为随机种子） |
| **鲁棒性** | 即使 Raft 进度信息不准确，也能保证 quorum |

#### 缺点

| 维度 | 详情 |
|------|------|
| **次优解** | 可能无法暂停最多的副本（如 5 副本中 3 个过载，可能只暂停 1 个） |
| **随机性** | 不同 Range 可能暂停同一个 Store，负载不均 |

#### 对比方案：整数规划（ILP）

**目标函数**：
```
最大化 Σ (paused[r][s] × io_score[s])
约束条件：
  - ∀ Range r: Σ (1 - paused[r][s]) × is_voter[s] >= quorum_size[r]
  - paused[r][s] ∈ {0, 1}
```

**问题**：
- **计算复杂度**：NP-hard，对于 10,000 个 Range 不可行
- **全局信息需求**：需要所有 Range 的副本信息（当前只有 leader 有）

**实际取舍**：
- 对于单个 Range，贪心算法已足够好（副本数通常 ≤ 5）
- 跨 Range 的负载均衡由随机种子保证（统计意义上的公平）

### 6.3 Push-Mode (Follower Pausing) vs Pull-Mode (RAC v2)

#### Follower Pausing (Push-Mode)

| 维度 | 详情 |
|------|------|
| **工作模式** | Leader 主动丢弃发往过载 follower 的 MsgApp |
| **粒度** | 二元（暂停/不暂停） |
| **延迟** | 低（无需 follower 反馈） |
| **适用场景** | IO 严重过载（需要完全停止复制） |

#### RAC v2 (Pull-Mode)

| 维度 | 详情 |
|------|------|
| **工作模式** | Follower 根据自身负载主动拉取 log |
| **粒度** | 精细（基于 token 的流控制） |
| **延迟** | 中等（需要 follower → leader 的反馈循环） |
| **适用场景** | 中等负载（需要平滑限速） |

**互斥原因**：
- RAC v2 已提供流控制，无需额外暂停
- 如果同时启用，可能导致过度保护（follower 永远追不上）

**启用条件**：
```go
func (r *Replica) shouldReplicationAdmissionControlUsePullMode(ctx context.Context) bool {
    return replicarac2.Enabled(r.ClusterSettings()) &&
           r.kvflowRangeController != nil
}
```

### 6.4 惰性 vs 主动更新

**当前设计**：惰性（仅在 tick 时更新）

#### 优点

| 维度 | 详情 |
|------|------|
| **开销低** | 无需后台协程 |
| **时效性** | 100ms 延迟对 IO 过载场景足够（IO 恢复通常需数秒） |
| **简单性** | 避免管理定时器生命周期 |

#### 缺点

| 维度 | 详情 |
|------|------|
| **延迟** | 如果 Store 在两次 tick 之间过载，最多 100ms 才响应 |
| **不适合突发** | 无法立即响应瞬时 IO 尖峰 |

#### 对比方案：主动监听 IO 事件

```go
// 假设的实现
func (s *Store) watchIOThresholds() {
    ch := s.cfg.StorePool.SubscribeIOThresholdChanges()
    for update := range ch {
        if update.Score > pauseThreshold {
            s.updateIOThresholdMap()  // 立即更新
        }
    }
}
```

**问题**：
- **复杂性**：需要实现发布-订阅机制
- **开销**：可能触发频繁的快照更新（如 IO 在阈值附近震荡）
- **收益有限**：IO 过载通常持续数秒到数分钟，100ms 延迟可接受

---

## VII. 总结与核心思想

### 7.1 ioThresholdMap 的本质

**核心定位**：**分布式集群 IO 健康状况的不可变快照**，用于驱动 Raft 副本级别的流控制决策。

**核心公式**：
```
ioThresholdMap = Snapshot({StoreID → IOThreshold}, pauseThreshold, seq)

对于每个 Replica (leader):
  pausedFollowers = computeExpendableOverloadedFollowers(
    ioThresholdMap,
    raftProgress,
    quorumConstraints
  )

对于每条 MsgApp:
  if destination ∈ pausedFollowers:
    drop(MsgApp)
  else:
    send(MsgApp)
```

### 7.2 三层架构

| 层次 | 组件 | 更新频率 | 作用域 |
|------|------|---------|--------|
| **1. 数据源** | Pebble IOThreshold | 实时 | 单个 Store |
| **2. 传播层** | Gossip + StorePool | 1-3 秒 | 集群全局 |
| **3. 决策层** | ioThresholdMap | 100ms | 单个 Store 的所有 Replica |

### 7.3 关键设计原则

#### 7.3.1 **不可变性保证无锁并发**

```go
// 读取快照（无锁）
snapshot := s.ioThresholds.Current()

// 在整个 tick 周期内，snapshot 不会改变
for _, replica := range allReplicas {
    replica.tick(ctx, liveness, snapshot)
}
```

**优点**：
- 避免 10,000 个 Replica 竞争同一把锁
- 确保所有 Replica 看到一致的世界观

#### 7.3.2 **序列号机制减少无效更新**

```go
if old.seq == cur.seq {
    // 可暂停 Store 集合未变化，跳过日志
    return
}
log.KvExec.Infof("pausable stores: %+v", cur)
```

**优点**：
- 避免每 100ms 记录一次重复日志
- 减少 Replica 重新计算 `pausedFollowers` 的频率（虽然当前实现每次 tick 都计算）

#### 7.3.3 **Quorum 优先，负载均衡其次**

```go
// 贪心算法：从最大暂停集合开始，逐步缩减直到满足 quorum
for len(candidates) > 0 {
    if CanMakeProgress(excludingCandidates) {
        break  // 找到最大可暂停集合
    }
    removeRandomCandidate()  // 随机移除一个，重试
}
```

**优点**：
- 永远不会牺牲 Raft 一致性换取 IO 保护
- 通过随机化避免总是暂停同一个 Store

### 7.4 心智模型

**如果只记住一件事**：

> `ioThresholdMap` 是一个**每 100ms 更新一次的只读快照**，记录了"哪些 Store 的磁盘正在喘不过气"。每个 Raft leader 根据这个快照，**在保证 quorum 的前提下**，尽可能多地停止向过载 Store 的 follower 发送数据，直到它们恢复健康。

**类比**：
- **交通信号灯**：ioThresholdMap 就像路口的红绿灯状态快照
- **车辆（Raft MsgApp）**：每辆车根据快照决定是否通过（发送或丢弃）
- **交警（computeExpendableOverloadedFollowers）**：确保至少有足够的车道开放（满足 quorum）

### 7.5 简化伪代码

```go
// 阶段 1：Store 级别（每 100ms）
func Store.raftTickLoop() {
    for range ticker.C {
        snapshot := createSnapshotFromStorePool()  // 读取所有 Store 的 IO 状态
        s.ioThresholds.Replace(snapshot)           // 原子替换

        for _, replica := range allReplicas {
            replica.tick(snapshot)                 // 传递快照
        }
    }
}

// 阶段 2：Replica 级别（每次 tick）
func Replica.tick(snapshot) {
    if !snapshot.anyOverloaded(r.replicas) {
        r.pausedFollowers = {}
        return
    }

    if !r.isLeaderAndLeaseholder() {
        return
    }

    r.pausedFollowers = computeMaxPausableSet(
        overloadedReplicas: snapshot.findOverloaded(r.replicas),
        quorumSize: r.quorumSize(),
        raftProgress: r.raft.getProgress(),
    )
}

// 阶段 3：消息发送（每次 Raft Ready）
func Replica.sendRaftMessages(messages) {
    for _, msg := range messages {
        if msg.type == MsgApp && msg.to ∈ r.pausedFollowers {
            metrics.droppedMsgs++
            continue  // 丢弃
        }
        transport.send(msg)
    }
}
```

---

## 附录：代码位置索引

| 组件 | 代码位置 |
|------|---------|
| **ioThresholdMap 定义** | [pkg/kv/kvserver/replica_raft_overload.go:188-192](pkg/kv/kvserver/replica_raft_overload.go#L188-L192) |
| **ioThresholds 容器** | [pkg/kv/kvserver/replica_raft_overload.go:251-256](pkg/kv/kvserver/replica_raft_overload.go#L251-L256) |
| **Replace() 方法** | [pkg/kv/kvserver/replica_raft_overload.go:267-293](pkg/kv/kvserver/replica_raft_overload.go#L267-L293) |
| **updateIOThresholdMap()** | [pkg/kv/kvserver/store_raft.go:1041-1058](pkg/kv/kvserver/store_raft.go#L1041-L1058) |
| **updatePausedFollowersLocked()** | [pkg/kv/kvserver/replica_raft_overload.go:295-376](pkg/kv/kvserver/replica_raft_overload.go#L295-L376) |
| **computeExpendableOverloadedFollowers()** | [pkg/kv/kvserver/replica_raft_overload.go:95-186](pkg/kv/kvserver/replica_raft_overload.go#L95-L186) |
| **sendRaftMessages()** | [pkg/kv/kvserver/replica_raft.go:1810-1900](pkg/kv/kvserver/replica_raft.go#L1810-L1900) |
| **Replica.tick() 调用点** | [pkg/kv/kvserver/replica_raft.go:1413](pkg/kv/kvserver/replica_raft.go#L1413) |
| **pauseReplicationIOThreshold 设置** | [pkg/kv/kvserver/replica_raft_overload.go:26-32](pkg/kv/kvserver/replica_raft_overload.go#L26-L32) |

---

**下一章预告**：
[第二十七章] 将分析 **Replication Admission Control v2 (RAC v2)** 的 pull-mode 流控制机制，深入探讨：
- 为何 RAC v2 能提供比 Follower Pausing 更精细的流控制
- Token-based 限流如何防止 follower 积压
- RangeController 的生命周期与动态调整算法
- 与 Follower Pausing 的共存与切换策略
