# MMA Store Rebalancer 深度剖析——基于多维度指标与集中式决策的新一代 Store 级负载均衡器

> **核心文件**：[pkg/kv/kvserver/mma_store_rebalancer.go](pkg/kv/kvserver/mma_store_rebalancer.go)
> **上下文**：[pkg/kv/kvserver/allocator/mmaprototype](pkg/kv/kvserver/allocator/mmaprototype), [pkg/kv/kvserver/mmaintegration](pkg/kv/kvserver/mmaintegration)
> **作者**：Claude Sonnet 4.5
> **日期**：2026-02-09

---

## 一、职责边界与设计动机（Why）

### 1.1 MMA Store Rebalancer 的核心使命

**MMA Store Rebalancer** 是 CockroachDB 中新一代的 **Store 级负载均衡器**（Multi-Metric Allocator Store Rebalancer），负责在整个集群范围内实现 **多维度负载均衡**。它的核心职责包括：

1. **监控 Store 级负载状态**：收集每个 Store 的 CPU、写带宽、存储容量等多维度指标
2. **计算负载均衡方案**：调用 MMA Allocator 的集中式算法，生成 Lease 转移和 Replica 变更方案
3. **应用负载均衡方案**：执行 Lease 转移（`AdminTransferLease`）和 Replica 变更（`changeReplicasImpl`）
4. **与 Allocator Sync 协调**：避免与 Replicate Queue、Lease Queue 等其他组件产生冲突

### 1.2 系统性问题：旧 Store Rebalancer 的局限性

在 MMA Store Rebalancer 出现之前，CockroachDB 使用传统的 **Store Rebalancer**（[store_rebalancer.go:104-128](pkg/kv/kvserver/store_rebalancer.go#L104-L128)），其存在以下关键限制：

**问题 1：单一维度决策（QPS 或 CPU）**

```go
// 旧 StoreRebalancer（store_rebalancer.go:114）
type StoreRebalancer struct {
	// ...
	objectiveProvider RebalanceObjectiveProvider  // 只支持 QPS 或 CPU 单一目标
}
```

- **缺陷**：只能根据单一维度（`LoadBasedRebalancingMode` 设置为 `qps` 或 `cpu`）进行负载均衡
- **后果**：当集群同时存在 CPU 热点和写带宽热点时，无法同时优化两个维度

**问题 2：局部贪心决策**

旧 Store Rebalancer 的工作模式：

```
1. 扫描本地 Store 的所有 Replica
2. 逐个 Replica 评估是否需要转移 Lease 或迁移副本
3. 使用 Allocator.TransferLeaseTarget() 或 Allocator.RebalanceTarget() 单独决策
```

- **缺陷**：每个决策都是局部的，不考虑其他并发决策的影响
- **后果**：可能出现"乒乓效应"（Replica 在多个 Store 之间反复迁移）

**问题 3：与 Queue 系统的竞争**

CockroachDB 有多个 Queue 并发运行（Replicate Queue、Lease Queue、GC Queue 等），它们各自独立地做出副本放置决策，导致：

- **决策冲突**：Store Rebalancer 转移 Lease 到 Store A，同时 Lease Queue 转移 Lease 到 Store B
- **资源浪费**：多个组件重复扫描和评估相同的 Replica
- **抖动**（Thrashing）：多个组件交替修改同一个 Range 的副本配置

### 1.3 MMA Store Rebalancer 的设计目标

MMA Store Rebalancer 是为了解决上述问题而设计的新一代架构，其核心目标：

**目标 1：多维度综合优化**

```go
// mmaprototype/load.go - 多维度负载向量
type LoadVector [nLoadDimensions]LoadValue

const (
	CPURate        LoadDimension = iota  // CPU 使用率
	WriteBandwidth                       // 写带宽
	ByteSize                             // 存储容量
	nLoadDimensions
)
```

- **实现**：同时考虑 CPU、写带宽、存储容量三个维度
- **优势**：可以在一次优化中平衡多个资源约束

**目标 2：集中式全局决策**

MMA Allocator 维护**整个集群的负载状态**，而不是仅仅本地 Store 的状态：

```go
// mmaprototype/allocator.go:30-40
type Allocator interface {
	// ProcessStoreLoadMsg 收集所有 Store 的负载信息
	ProcessStoreLoadMsg(ctx context.Context, msg *StoreLoadMsg)

	// ComputeChanges 基于全局状态计算变更
	ComputeChanges(ctx context.Context, msg *StoreLeaseholderMsg, opts ChangeOptions) []ExternalRangeChange
}
```

- **实现**：通过 Gossip 协议定期收集所有 Store 的负载信息
- **优势**：可以识别集群级的负载不平衡模式，做出更优的全局决策

**目标 3：与其他组件的协调**

引入 **AllocatorSync**（[mmaintegration/allocator_sync.go:76-92](pkg/kv/kvserver/mmaintegration/allocator_sync.go#L76-L92)）组件：

```go
type AllocatorSync struct {
	knobs        *TestingKnobs
	sp           storePool           // 更新 StorePool 状态
	st           *cluster.Settings
	mmaAllocator mmaState            // 通知 MMA Allocator
	mu           struct {
		syncutil.Mutex
		changeSeqGen   SyncChangeID
		trackedChanges map[SyncChangeID]trackedAllocatorChange
	}
}
```

**核心职责**：

1. **Pre-Apply 注册**：Replicate Queue、Lease Queue 在执行变更前，先通过 `AllocatorSync.NonMMAPreTransferLease()` 注册意图
2. **冲突检测**：MMA Allocator 可以通过 `BuildMMARebalanceAdvisor()` 检测候选 Store 是否与 MMA 的计划冲突
3. **Post-Apply 通知**：变更完成后，通过 `PostApply()` 同步更新 StorePool 和 MMA Allocator 的状态

**优势**：避免 MMA Store Rebalancer 与其他 Queue 产生冲突和抖动

### 1.4 在整个系统中的位置

```
                        ┌─────────────────────────────────────┐
                        │         CockroachDB Cluster         │
                        │  ┌───────────┐  ┌───────────┐      │
                        │  │  Node 1   │  │  Node 2   │ ...  │
                        │  └─────┬─────┘  └─────┬─────┘      │
                        └────────┼────────────┼──────────────┘
                                 │            │
                      ┌──────────▼────────────▼────────┐
                      │       Gossip Network           │ ← Store Load Messages
                      └──────────┬─────────────────────┘
                                 │
                      ┌──────────▼────────────────────┐
                      │      MMA Allocator            │ ← 集中式决策引擎
                      │  - 维护全局负载状态            │
                      │  - 计算负载均衡方案            │
                      └──────────┬────────────────────┘
                                 │ ExternalRangeChange[]
                      ┌──────────▼────────────────────┐
                      │   MMA Store Rebalancer        │ ← **本模块**
                      │  - 获取 Leaseholder 信息       │
                      │  - 调用 ComputeChanges()       │
                      │  - 应用 Lease/Replica 变更     │
                      └──────────┬────────────────────┘
                                 │
                    ┌────────────┴──────────────┐
                    │                           │
         ┌──────────▼──────────┐   ┌───────────▼──────────┐
         │  AdminTransferLease │   │  changeReplicasImpl  │
         │  (Lease Transfer)   │   │  (Replica Migration) │
         └─────────────────────┘   └──────────────────────┘
                    │                           │
                    └────────────┬──────────────┘
                                 │
                      ┌──────────▼────────────────────┐
                      │      AllocatorSync            │ ← 协调中心
                      │  - 注册变更                   │
                      │  - 更新 StorePool             │
                      │  - 通知 MMA Allocator         │
                      └───────────────────────────────┘
```

**上游组件**：
- **Gossip Network**：提供所有 Store 的负载信息（通过 `ProcessStoreLoadMsg`）
- **MMA Allocator**：提供集中式决策算法

**下游组件**：
- **Replica**：执行 Lease 转移和副本变更
- **AllocatorSync**：协调与其他 Queue 的关系

---

## 二、控制流与组件协作（How it flows）

### 2.1 主执行路径：定期重平衡循环

MMA Store Rebalancer 的核心是 `run()` 方法中的**定期触发 + 连续执行**模式：

```go
// mma_store_rebalancer.go:69-100
func (m *mmaStoreRebalancer) run(ctx context.Context, stopper *stop.Stopper) {
	// Line 70: 创建定时器，默认间隔 60 秒
	timer := time.NewTicker(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&m.st.SV)))
	defer timer.Stop()
	log.KvDistribution.Infof(ctx, "starting multi-metric store rebalancer with mode=%v", kvserverbase.LoadBasedRebalancingMode.Get(&m.st.SV))

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopper.ShouldQuiesce():
			return
		case <-timer.C:
			// Line 81-83: 等待第一个 tick 以便累积统计信息
			timer.Reset(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&m.st.SV)))
			// Line 84-86: 检查是否启用 MMA 模式
			if !kvserverbase.LoadBasedRebalancingModeIsMMA(&m.st.SV) {
				continue
			}

			// Line 88-97: *** 核心逻辑 *** 连续重平衡直到没有变更
			periodicCall := true
			for {
				attemptedChanges := m.rebalance(ctx, periodicCall)
				if !attemptedChanges {
					break  // 没有变更，退出内循环
				}
				periodicCall = false  // 后续调用标记为非周期性
			}
		}
	}
}
```

**执行流程时间线**：

```
时间轴:  T0        T1(+60s)      T2         T3         T4         T5(+60s)
        │          │             │          │          │          │
事件:   启动        第一次tick     rebalance  rebalance  rebalance  第二次tick
        │          │             │          │          │          │
        │          ├─────────────▶ 计算变更  │          │          │
        │          │             ├──────────▶ 应用变更  │          │
        │          │             │          ├──────────▶ 计算变更  │
        │          │             │          │          ├──────────▶ 无变更，等待
        │          │             │          │          │          │
        └──────────┴─────────────┴──────────┴──────────┴──────────┴──────────▶
                   等待累积统计    连续执行直到收敛                  下一轮周期
```

**关键设计点**：

1. **定期触发**（Line 70）：使用 `jitteredInterval()` 添加随机抖动（±25%），避免所有节点同时重平衡
2. **连续执行**（Line 91-97）：第一次调用 `rebalance()` 后，如果有变更，立即再次调用，直到没有变更为止
3. **条件检查**（Line 84-86）：每次 tick 都检查集群设置，支持运行时动态开关 MMA 模式

### 2.2 单次重平衡流程：rebalance()

```go
// mma_store_rebalancer.go:130-151
func (m *mmaStoreRebalancer) rebalance(ctx context.Context, periodicCall bool) bool {
	// **步骤 1**：获取本地 Store 持有的所有 Leaseholder 信息
	knownStoresByMMA := m.mma.KnownStores()
	storeLeaseholderMsg, numIgnoredRanges := m.store.MakeStoreLeaseholderMsg(ctx, knownStoresByMMA)
	if numIgnoredRanges > 0 {
		log.KvDistribution.Infof(ctx, "mma rebalancer: ignored %d ranges since the allocator does not know all stores",
			numIgnoredRanges)
	}

	// **步骤 2**：调用 MMA Allocator 计算变更方案
	changes := m.mma.ComputeChanges(ctx, &storeLeaseholderMsg, mmaprototype.ChangeOptions{
		LocalStoreID: m.store.StoreID(),
		PeriodicCall: periodicCall,
	})

	// **步骤 3**：逐个应用变更（Lease Transfer 或 Replica Changes）
	for _, change := range changes {
		if err := m.applyChange(ctx, change); err != nil {
			log.KvDistribution.VInfof(ctx, 1, "failed to apply change for range %d: %v", change.RangeID, err)
		}
	}

	// **步骤 4**：返回是否有变更（作为连续执行的信号）
	return len(changes) > 0
}
```

**步骤详解**：

**步骤 1：收集 Leaseholder 信息**（Line 132-136）

`MakeStoreLeaseholderMsg()` 方法扫描本地 Store，构造 `StoreLeaseholderMsg`：

```go
// StoreLeaseholderMsg 的结构（mmaprototype/messages.go:37-42）
type StoreLeaseholderMsg struct {
	roachpb.StoreID
	Ranges []RangeMsg  // 本 Store 作为 Leaseholder 的所有 Range
}

// 每个 RangeMsg 包含（messages.go:56-62）
type RangeMsg struct {
	roachpb.RangeID
	Replicas                 []StoreIDAndReplicaState  // 所有副本的位置和状态
	RangeLoad                RangeLoad                 // CPU、写带宽、容量等负载
	MaybeSpanConfIsPopulated bool
	MaybeSpanConf            roachpb.SpanConfig
}
```

**关键**：只包含**本 Store 作为 Leaseholder** 的 Range，因为只有 Leaseholder 才有权限执行 Lease 转移和副本变更。

`knownStoresByMMA` 参数的作用：如果 MMA Allocator 尚未通过 Gossip 得知某个 Store 的存在，则忽略涉及该 Store 的 Range，避免做出错误决策。

**步骤 2：计算变更方案**（Line 138-141）

调用 `m.mma.ComputeChanges()`，该方法是 MMA Allocator 的核心决策引擎：

```go
// mmaprototype/allocator.go:108-109
ComputeChanges(ctx context.Context, msg *StoreLeaseholderMsg, opts ChangeOptions) []ExternalRangeChange
```

**决策过程**（简化描述，详细算法在 `mmaprototype/` 包中）：

1. **识别过载 Store**：计算每个 Store 的负载利用率（Load / Capacity），识别超过阈值的 Store
2. **选择要迁移的 Range**：在过载 Store 上，选择负载贡献最大的 Range
3. **选择目标 Store**：在负载较低且满足约束条件的 Store 中，选择最优目标
4. **生成变更方案**：
   - **Lease Transfer**：如果目标 Store 上已有副本，仅转移 Lease
   - **Replica Changes**：如果目标 Store 上无副本，需要先添加副本，再转移 Lease

**`ChangeOptions.PeriodicCall` 的作用**（Line 140）：

- `true`：表示这是定期触发的第一次调用，MMA Allocator 会更新内部指标（Gauge）并记录日志
- `false`：表示这是连续执行中的后续调用，跳过日志和指标更新，避免日志爆炸

**步骤 3：应用变更**（Line 144-148）

逐个调用 `applyChange()`，将 MMA 的决策应用到实际的 Replica 上。

**步骤 4：返回是否有变更**（Line 150）

- 如果 `len(changes) > 0`，返回 `true`，触发 `run()` 中的内循环再次调用 `rebalance()`
- 如果 `len(changes) == 0`，返回 `false`，退出内循环，等待下一次 tick

### 2.3 应用单个变更：applyChange()

```go
// mma_store_rebalancer.go:155-177
func (m *mmaStoreRebalancer) applyChange(
	ctx context.Context, change mmaprototype.ExternalRangeChange,
) error {
	// **步骤 1**：获取 Replica 对象
	repl := m.store.GetReplicaIfExists(change.RangeID)
	if repl == nil {
		m.as.MarkChangeAsFailed(ctx, change)
		return errors.Errorf("replica not found for range %d", change.RangeID)
	}

	// **步骤 2**：通过 AllocatorSync 注册变更（Pre-Apply）
	changeID := m.as.MMAPreApply(ctx, repl.RangeUsageInfo(), change)

	var err error
	switch {
	case change.IsPureTransferLease():
		// **步骤 3a**：应用 Lease Transfer
		err = m.applyLeaseTransfer(ctx, repl, change)
	case change.IsChangeReplicas():
		// **步骤 3b**：应用 Replica Changes
		err = m.applyReplicaChanges(ctx, repl, change)
	default:
		return errors.Errorf("unknown change type for range %d", change.RangeID)
	}

	// **步骤 4**：通过 AllocatorSync 通知结果（Post-Apply）
	m.as.PostApply(ctx, changeID, err == nil /*success*/)
	return err
}
```

**步骤详解**：

**步骤 1：获取 Replica**（Line 158-162）

- `GetReplicaIfExists()` 可能返回 `nil`，原因：
  - Range 已被合并（Merge）或删除
  - Lease 已转移到其他 Store
- 如果 Replica 不存在，调用 `as.MarkChangeAsFailed()`，通知 MMA Allocator 该变更失败

**步骤 2：Pre-Apply 注册**（Line 163）

`AllocatorSync.MMAPreApply()` 的作用：

```go
// mmaintegration/allocator_sync.go:217-245
func (as *AllocatorSync) MMAPreApply(
	ctx context.Context,
	usage allocator.RangeUsageInfo,
	pendingChange mmaprototype.ExternalRangeChange,
) SyncChangeID {
	trackedChange := trackedAllocatorChange{
		isMMARegistered: true,
		mmaChange:       pendingChange,
		usage:           usage,  // 记录 Range 的负载信息
	}
	// 根据变更类型设置 leaseTransferOp 或 changeReplicasOp
	// ...
	return as.addTrackedChange(trackedChange)  // 分配唯一的 SyncChangeID
}
```

**关键作用**：

1. **追踪变更**：为每个变更分配唯一的 `SyncChangeID`
2. **记录负载信息**：保存 `RangeUsageInfo`（CPU、写带宽、容量等），用于后续更新 StorePool

**步骤 3a：Lease Transfer**（Line 167, 180-188）

```go
func (m *mmaStoreRebalancer) applyLeaseTransfer(
	ctx context.Context, repl replicaToApplyChanges, change mmaprototype.ExternalRangeChange,
) error {
	return repl.AdminTransferLease(
		ctx,
		change.LeaseTransferTarget(),
		false, /* bypassSafetyChecks */
	)
}
```

- `AdminTransferLease()` 是 Replica 的方法，执行 Raft 协议的 Lease Transfer
- `bypassSafetyChecks=false`：需要检查目标 Store 是否满足 Lease Transfer 的前置条件（如副本是否存在、是否 up-to-date 等）

**步骤 3b：Replica Changes**（Line 169, 191-208）

```go
func (m *mmaStoreRebalancer) applyReplicaChanges(
	ctx context.Context, repl replicaToApplyChanges, change mmaprototype.ExternalRangeChange,
) error {
	_, err := repl.changeReplicasImpl(
		ctx,
		repl.Desc(),
		kvserverpb.SnapshotRequest_REPLICATE_QUEUE,  // 标记为 Replicate Queue 发起的变更
		0,  // senderQueuePriority
		kvserverpb.ReasonRebalance,  // 原因：负载均衡
		"todo: this is the rebalance detail for the range log",
		change.ReplicationChanges(),  // 转换为 kvpb.ReplicationChanges
	)
	return err
}
```

**关键点**：

- `changeReplicasImpl()` 是 Replica 的核心方法，负责执行副本配置变更
- `change.ReplicationChanges()` 将 MMA 的 `ExternalRangeChange` 转换为 CockroachDB 内部的 `kvpb.ReplicationChanges` 格式
- **TODO 注释**（Line 197-198）：指出应该设置超时（类似 `replicateQueue.processTimeoutFunc`），避免长时间阻塞

**步骤 4：Post-Apply 通知**（Line 175）

`AllocatorSync.PostApply()` 的作用：

```go
// mmaintegration/allocator_sync.go:259-278
func (as *AllocatorSync) PostApply(ctx context.Context, syncChangeID SyncChangeID, success bool) {
	trackedChange := as.getTrackedChange(syncChangeID)  // 根据 ID 获取变更信息

	// **4a**：通知 MMA Allocator 变更结果
	if trackedChange.isMMARegistered {
		as.mmaAllocator.AdjustPendingChangeDisposition(ctx, trackedChange.mmaChange, success)
	}

	if !success {
		return  // 失败，不更新 StorePool
	}

	// **4b**：更新 StorePool（仅成功时）
	switch {
	case trackedChange.leaseTransferOp != nil:
		as.sp.UpdateLocalStoresAfterLeaseTransfer(...)
	case trackedChange.changeReplicasOp != nil:
		for _, chg := range trackedChange.changeReplicasOp.chgs {
			as.sp.UpdateLocalStoreAfterRebalance(...)
		}
	}
}
```

**关键作用**：

1. **通知 MMA Allocator**：
   - 成功：MMA 内部状态已反映该变更（Load 已迁移）
   - 失败：MMA 回滚 Pending Change，允许重新计算
2. **更新 StorePool**：
   - StorePool 维护每个 Store 的负载统计
   - 更新后，其他组件（如 Replicate Queue）可以基于最新状态做决策

### 2.4 组件间的消息流

```
时间轴:  T0                T1                  T2                  T3
        │                 │                   │                   │
        │                 │                   │                   │
    ┌───▼────┐        ┌───▼────┐          ┌──▼──┐            ┌───▼────┐
    │ Gossip │        │  MMA   │          │Repl │            │AllocSync│
    │Network │        │Allocator│         │     │            │         │
    └───┬────┘        └───┬────┘          └──┬──┘            └───┬────┘
        │                 │                   │                   │
        │ StoreLoadMsg    │                   │                   │
        ├─────────────────▶                   │                   │
        │                 │                   │                   │
        │                 │StoreLeaseholderMsg│                   │
        │                 ◀───────────────────┤                   │
        │                 │                   │                   │
        │                 │ ComputeChanges()  │                   │
        │                 │                   │                   │
        │                 │ExternalRangeChange[]                  │
        │                 ├───────────────────▶                   │
        │                 │                   │                   │
        │                 │                   │  MMAPreApply()    │
        │                 │                   ├───────────────────▶
        │                 │                   │  SyncChangeID     │
        │                 │                   ◀───────────────────┤
        │                 │                   │                   │
        │                 │                   │AdminTransferLease │
        │                 │                   │   或               │
        │                 │                   │changeReplicasImpl │
        │                 │                   │                   │
        │                 │                   │  PostApply()      │
        │                 │                   ├───────────────────▶
        │                 │                   │                   │
        │                 │AdjustPendingChangeDisposition(success)│
        │                 ◀───────────────────┴───────────────────┤
        │                 │                                       │
        │                 │UpdateLocalStoresAfterLeaseTransfer()  │
        │                 │              or                       │
        │                 │UpdateLocalStoreAfterRebalance()       │
        │                 │                   ┌───────────────────▶
        │                 │                   │   StorePool
        │                 │                   │
```

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 newMMAStoreRebalancer：构造函数与依赖注入

```go
// mma_store_rebalancer.go:55-65
func newMMAStoreRebalancer(
	s *Store, mma mmaprototype.Allocator, st *cluster.Settings, sp *storepool.StorePool,
) *mmaStoreRebalancer {
	return &mmaStoreRebalancer{
		store: (*mmaStore)(s),              // 类型转换：*Store -> *mmaStore
		mma:   mma,                          // MMA Allocator 实例
		st:    st,                           // 集群设置
		sp:    sp,                           // StorePool（用于查询其他 Store 的状态）
		as:    s.cfg.AllocatorSync,         // AllocatorSync 实例
	}
}
```

**关键设计点**：

**1. 类型转换：`(*mmaStore)(s)`**（Line 59）

```go
// mma_store_rebalancer.go:26-39
type replicaToApplyChanges interface {
	RangeUsageInfo() allocator.RangeUsageInfo
	AdminTransferLease(ctx context.Context, target roachpb.StoreID, bypassSafetyChecks bool) error
	changeReplicasImpl(...) (updatedDesc *roachpb.RangeDescriptor, _ error)
	Desc() *roachpb.RangeDescriptor
}
```

- `mmaStore` 是 `Store` 的类型别名，提供 `GetReplicaIfExists()` 等方法
- 通过接口 `replicaToApplyChanges` 解耦，便于测试（可以 Mock Replica）

**2. 依赖注入**

所有依赖通过构造函数参数注入，而不是在方法内部创建，遵循 **Dependency Injection** 原则：

- **好处**：便于单元测试（可以注入 Mock 对象）
- **示例**：在测试中，可以注入一个 Mock `mmaprototype.Allocator`，控制 `ComputeChanges()` 的返回值

### 3.2 run：主循环与状态控制

**不变量**（Invariants）：

1. **定时器间隔动态调整**：每次 tick 后，根据集群设置重新设置间隔（Line 83）
   ```go
   timer.Reset(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&m.st.SV)))
   ```

2. **MMA 模式检查**：每次 tick 都检查 `LoadBasedRebalancingMode` 设置（Line 84-86）
   ```go
   if !kvserverbase.LoadBasedRebalancingModeIsMMA(&m.st.SV) {
       continue  // 跳过本次 tick，但仍然重置定时器
   }
   ```
   - **原因**：支持运行时动态切换模式（`off` ↔ `qps` ↔ `cpu` ↔ `mma`）

3. **连续执行直到收敛**：内循环不断调用 `rebalance()`，直到返回 `false`（Line 91-97）
   - **收敛条件**：`ComputeChanges()` 返回空列表
   - **防止无限循环**：MMA Allocator 内部记录 Pending Changes，避免重复提议相同的变更

**并发语义**：

- `run()` 方法在单独的 Goroutine 中运行（由 `start()` 启动）
- **无锁设计**：`run()` 本身不使用锁，依赖以下机制保证线程安全：
  1. **Timer Channel**：`timer.C` 是线程安全的
  2. **Context Done**：`ctx.Done()` 和 `stopper.ShouldQuiesce()` 是线程安全的
  3. **MMA Allocator 内部锁**：`mma.ComputeChanges()` 内部使用锁保护共享状态
  4. **AllocatorSync 内部锁**：`as.MMAPreApply()` 和 `as.PostApply()` 使用 `mu` 保护 `trackedChanges` map

**分支逻辑分析**：

**分支 1：`ctx.Done()` 或 `stopper.ShouldQuiesce()`**（Line 76-79）

- **触发条件**：Store 关闭或节点 Draining
- **行为**：立即退出循环，不执行 `defer timer.Stop()`（在函数开头 defer）

**分支 2：`timer.C`**（Line 80）

- **触发条件**：定时器到期
- **行为**：重置定时器，执行重平衡逻辑

**分支 3：MMA 模式未启用**（Line 84-86）

- **触发条件**：`LoadBasedRebalancingMode != "mma"`
- **行为**：跳过重平衡，继续等待下一次 tick
- **注意**：仍然重置定时器，保证下次 tick 的间隔正确

### 3.3 rebalance：核心决策应用流程

**输入**：
- `periodicCall bool`：是否为定期触发的第一次调用

**输出**：
- `bool`：是否尝试了变更（无论成功或失败）

**核心分支**：

**分支 1：Replica 不存在**（Line 159-162）

```go
repl := m.store.GetReplicaIfExists(change.RangeID)
if repl == nil {
	m.as.MarkChangeAsFailed(ctx, change)
	return errors.Errorf("replica not found for range %d", change.RangeID)
}
```

- **原因**：
  - Range 已被 Merge 或 Split
  - Lease 已转移到其他 Store（MMA 的信息滞后）
- **处理**：
  - 调用 `MarkChangeAsFailed()`，通知 MMA Allocator 该变更失败
  - MMA Allocator 会调用 `AdjustPendingChangeDisposition(change, success=false)`，回滚内部状态

**分支 2：Lease Transfer vs Replica Changes**（Line 165-172）

```go
switch {
case change.IsPureTransferLease():
	err = m.applyLeaseTransfer(ctx, repl, change)
case change.IsChangeReplicas():
	err = m.applyReplicaChanges(ctx, repl, change)
default:
	return errors.Errorf("unknown change type for range %d", change.RangeID)
}
```

**判断逻辑**（[range_change.go:129-154](pkg/kv/kvserver/allocator/mmaprototype/range_change.go#L129-L154)）：

```go
// IsPureTransferLease 返回 true 当且仅当：
// 1. len(Changes) == 2（恰好两个变更）
// 2. 一个是 AddLease，一个是 RemoveLease
func (rc *ExternalRangeChange) IsPureTransferLease() bool {
	if len(rc.Changes) != 2 {
		return false
	}
	var addLease, removeLease int
	for _, c := range rc.Changes {
		switch c.ChangeType {
		case AddLease:
			addLease++
		case RemoveLease:
			removeLease++
		default:
			return false  // 包含其他类型的变更，不是纯 Lease Transfer
		}
	}
	if addLease != 1 || removeLease != 1 {
		panic(...)  // 违反不变量
	}
	return true
}
```

**示例**：

- **Lease Transfer**：
  ```
  Changes: [
      {Target: Store2, ChangeType: AddLease},     // 在 Store2 上添加 Lease
      {Target: Store1, ChangeType: RemoveLease},  // 在 Store1 上移除 Lease
  ]
  ```

- **Replica Changes + Lease Transfer**：
  ```
  Changes: [
      {Target: Store3, ChangeType: AddReplica},   // 在 Store3 上添加副本
      {Target: Store3, ChangeType: AddLease},     // 在 Store3 上添加 Lease
      {Target: Store1, ChangeType: RemoveLease},  // 在 Store1 上移除 Lease
  ]
  ```
  - **不是** Pure Lease Transfer（`len(Changes) != 2`）
  - 调用 `applyReplicaChanges()`

**不变量验证**：

在 `applyReplicaChanges()` 中，`change.ReplicationChanges()` 会验证以下不变量（[range_change.go:170-232](pkg/kv/kvserver/allocator/mmaprototype/range_change.go#L170-L232)）：

1. **只包含 ChangeReplica/AddReplica/RemoveReplica 类型**（Line 177-182）
2. **新 Leaseholder 必须放在第一个位置**（Line 227-230）：
   ```go
   if newLeaseholderIndex >= 0 {
       // 将新 Leaseholder 移到索引 0
       chgs[0], chgs[newLeaseholderIndex] = chgs[newLeaseholderIndex], chgs[0]
   }
   ```
   - **原因**：CockroachDB 的约定（参见 `Replica.maybeTransferLeaseDuringLeaveJoint`）

### 3.4 AllocatorSync 的协调机制

**核心数据结构**：

```go
// mmaintegration/allocator_sync.go:76-92
type AllocatorSync struct {
	knobs        *TestingKnobs
	sp           storePool
	st           *cluster.Settings
	mmaAllocator mmaState
	mu           struct {
		syncutil.Mutex
		changeSeqGen   SyncChangeID         // 单调递增的序列号
		trackedChanges map[SyncChangeID]trackedAllocatorChange
	}
}

type trackedAllocatorChange struct {
	isMMARegistered bool                            // 是否已向 MMA 注册
	mmaChange       mmaprototype.ExternalRangeChange
	usage           allocator.RangeUsageInfo        // Range 的负载信息
	leaseTransferOp *leaseTransferOp                // 仅 Lease Transfer 时非空
	changeReplicasOp *changeReplicasOp              // 仅 Replica Changes 时非空
}
```

**工作流程**：

**1. Pre-Apply：注册变更**

```go
// Line 217-245
func (as *AllocatorSync) MMAPreApply(...) SyncChangeID {
	trackedChange := trackedAllocatorChange{
		isMMARegistered: true,
		mmaChange:       pendingChange,
		usage:           usage,
	}
	// 根据变更类型设置 leaseTransferOp 或 changeReplicasOp
	switch {
	case pendingChange.IsPureTransferLease():
		trackedChange.leaseTransferOp = &leaseTransferOp{
			transferFrom: pendingChange.LeaseTransferFrom(),
			transferTo:   pendingChange.LeaseTransferTarget(),
		}
	case pendingChange.IsChangeReplicas():
		trackedChange.changeReplicasOp = &changeReplicasOp{
			chgs: pendingChange.ReplicationChanges(),
		}
	}
	return as.addTrackedChange(trackedChange)  // 加锁，插入 map，返回 SyncChangeID
}
```

**2. Post-Apply：应用结果**

```go
// Line 259-278
func (as *AllocatorSync) PostApply(ctx context.Context, syncChangeID SyncChangeID, success bool) {
	trackedChange := as.getTrackedChange(syncChangeID)  // 加锁，从 map 中删除

	// 通知 MMA Allocator
	if trackedChange.isMMARegistered {
		as.mmaAllocator.AdjustPendingChangeDisposition(ctx, trackedChange.mmaChange, success)
	}

	if !success {
		return  // 失败，不更新 StorePool
	}

	// 更新 StorePool
	switch {
	case trackedChange.leaseTransferOp != nil:
		as.sp.UpdateLocalStoresAfterLeaseTransfer(
			trackedChange.leaseTransferOp.transferFrom,
			trackedChange.leaseTransferOp.transferTo,
			trackedChange.usage)
	case trackedChange.changeReplicasOp != nil:
		for _, chg := range trackedChange.changeReplicasOp.chgs {
			as.sp.UpdateLocalStoreAfterRebalance(
				chg.Target.StoreID, trackedChange.usage, chg.ChangeType)
		}
	}
}
```

**关键作用**：

1. **状态同步**：
   - **MMA Allocator**：更新 Pending Changes，避免重复提议
   - **StorePool**：更新负载统计，影响其他组件的决策

2. **错误处理**：
   - 如果 `success=false`，MMA Allocator 会回滚 Pending Change，但**不回滚 StorePool**
   - **原因**：StorePool 的更新是幂等的（后续的 Gossip 消息会纠正状态）

3. **并发安全**：
   - `trackedChanges` map 使用 `mu` 保护
   - `SyncChangeID` 是单调递增的，避免冲突

---

## 四、运行时行为与系统反馈（Runtime Behavior）

### 4.1 系统如何感知负载信号

**信号源 1：Store Load Messages（通过 Gossip）**

每个 Store 定期（默认 10 秒）通过 Gossip 网络广播自己的负载状态：

```go
// mmaprototype/messages.go:16-30
type StoreLoadMsg struct {
	roachpb.NodeID
	roachpb.StoreID

	Load LoadVector                  // 当前负载：CPU、写带宽、容量
	Capacity LoadVector               // 容量：最大 CPU、最大写带宽、最大容量
	SecondaryLoad SecondaryLoadVector // 辅助指标：读 QPS、写 QPS 等

	LoadTime time.Time                // 采样时间戳
}
```

**负载向量的三个维度**：

```go
// mmaprototype/load.go
type LoadVector [nLoadDimensions]LoadValue

const (
	CPURate        LoadDimension = iota  // CPU 纳秒/秒
	WriteBandwidth                       // 写字节/秒
	ByteSize                             // 存储字节数
	nLoadDimensions
)
```

**容量计算**（针对 CPU）：

- **CPU 容量** = `(节点总 CPU 使用率 / 节点上的 Store 数量)`
- **示例**：节点有 16 核，2 个 Store，节点 CPU 使用率为 800%（8 核满负载）
  - 每个 Store 的 CPU 容量 = 800% / 2 = 400%

**信号源 2：Store Leaseholder Messages（本地生成）**

每次 `rebalance()` 调用时，本地 Store 生成 `StoreLeaseholderMsg`：

```go
// mmaprototype/messages.go:37-42
type StoreLeaseholderMsg struct {
	roachpb.StoreID
	Ranges []RangeMsg  // 本 Store 作为 Leaseholder 的所有 Range
}
```

每个 `RangeMsg` 包含：

```go
type RangeMsg struct {
	roachpb.RangeID
	Replicas []StoreIDAndReplicaState  // 所有副本的位置和状态
	RangeLoad RangeLoad                 // 该 Range 的负载
	// ...
}

type RangeLoad struct {
	Load     LoadVector        // CPU、写带宽、容量
	RaftCPU  LoadValue         // Raft 协议的 CPU 开销
}
```

**负载采样**：

- **CPU**：`RangeUsageInfo.RequestCPUNanosPerSecond + RaftCPUNanosPerSecond`
- **WriteBandwidth**：`RangeUsageInfo.WriteBytesPerSecond`
- **ByteSize**：`RangeUsageInfo.LogicalBytes`（MVCC 总字节数）

### 4.2 决策信号如何影响行为

**决策流程**（MMA Allocator 内部）：

```
1. 计算每个 Store 的利用率：
   utilization = Load / Capacity

2. 识别过载 Store：
   isOverloaded = utilization > threshold（如 0.8）

3. 对于每个过载 Store：
   a. 选择负载贡献最大的 Range（Top-K 算法）
   b. 在欠载 Store 中选择最优目标
   c. 生成变更方案

4. 返回变更列表给 MMA Store Rebalancer
```

**阈值配置**（示例值，实际在 MMA Allocator 内部）：

- **过载阈值**：80%
- **欠载阈值**：60%
- **目标利用率**：70%

**即时 vs 滞后**：

- **即时信号**：`StoreLeaseholderMsg` 是实时生成的，反映当前 Leaseholder 状态
- **滞后信号**：
  - `StoreLoadMsg` 通过 Gossip 传播，有 10-30 秒延迟
  - MMA Allocator 的内部状态更新依赖 Gossip，因此决策基于"稍旧"的集群状态

**局部 vs 全局**：

- **局部信号**：`rebalance()` 只能操作**本 Store 作为 Leaseholder** 的 Range
- **全局决策**：MMA Allocator 基于**所有 Store** 的负载状态计算全局最优方案
- **结果**：每个 Store 的 MMA Store Rebalancer 独立运行，但它们共享同一个 MMA Allocator 实例（单例模式），因此决策是全局协调的

### 4.3 当前策略的设计理由

**为什么采用定期触发（而不是事件驱动）？**

**旧 Store Rebalancer 的问题**：

- 事件驱动：Replica 负载变化 → 触发重平衡
- **缺陷**：负载波动频繁，导致过度重平衡

**MMA Store Rebalancer 的改进**：

- 定期触发（默认 60 秒）
- **优势**：
  1. **平滑负载**：只响应持续的负载不平衡，过滤短暂波动
  2. **批量决策**：一次性考虑多个 Range，生成更优的全局方案
  3. **减少抖动**：避免频繁的 Lease Transfer 和 Replica 迁移

**为什么连续执行直到收敛？**

**示例场景**：

```
初始状态：
Store1: 90% 负载（过载）
Store2: 50% 负载
Store3: 50% 负载

第一轮 rebalance()：
- 转移 Range A 从 Store1 到 Store2
- 转移 Range B 从 Store1 到 Store3

第二轮 rebalance()（立即触发）：
- 转移 Range C 从 Store1 到 Store2
- ...

收敛后：
Store1: 70% 负载
Store2: 65% 负载
Store3: 65% 负载
```

**如果不连续执行**：

- 第一轮转移 2 个 Range 后，等待 60 秒
- **问题**：Store1 仍然过载（85%），但需要等待下一个周期
- **后果**：客户端请求延迟增加，用户体验下降

**连续执行的好处**：

1. **快速收敛**：在几秒内完成多次迭代，快速达到目标状态
2. **减少总迁移次数**：MMA Allocator 会记录 Pending Changes，避免重复迁移同一个 Range

**为什么不使用主动触发（Proactive Rebalancing）？**

**主动触发**：预测未来负载，提前迁移 Range

**MMA Store Rebalancer 采用惰性触发（Reactive Rebalancing）**：

- **原因**：
  1. **负载预测困难**：工作负载高度动态，预测准确性低
  2. **避免误判**：主动迁移可能在负载恢复正常后仍然执行，造成不必要的开销
- **权衡**：牺牲一定的响应速度，换取更高的决策准确性

### 4.4 设计在目标间的平衡

**目标 1：稳定性（Stability）**

- **机制**：定期触发 + 抖动（Jitter）
- **效果**：避免多个节点同时重平衡，减少 Raft 日志和快照的网络拥塞

**目标 2：吞吐量（Throughput）**

- **机制**：连续执行直到收敛
- **效果**：快速迁移负载，减少过载 Store 对整体吞吐量的影响

**目标 3：公平性（Fairness）**

- **机制**：MMA Allocator 的全局优化算法
- **效果**：
  - 所有 Store 的利用率接近（如 65%-75%）
  - 避免某些 Store 长期过载或闲置

**目标 4：资源利用率（Resource Utilization）**

- **机制**：多维度负载均衡（CPU + WriteBandwidth + ByteSize）
- **效果**：
  - CPU 热点和存储热点同时优化
  - 避免单一维度优化导致其他维度失衡

**冲突与权衡**：

| 冲突 | 当前选择 | 权衡理由 |
|------|---------|---------|
| 稳定性 vs 吞吐量 | 连续执行 | 在一个周期内快速收敛，但周期间隔较长（60秒） |
| 公平性 vs 资源利用率 | 多维度优化 | 可能牺牲某个维度的最优解，以实现整体平衡 |
| 局部自治 vs 集中控制 | 集中式决策 + 分布式执行 | MMA Allocator 集中决策，各 Store 独立执行，通过 AllocatorSync 协调 |

---

（未完待续，将在下篇继续讲解设计模式分析、具体运行示例和设计取舍）
