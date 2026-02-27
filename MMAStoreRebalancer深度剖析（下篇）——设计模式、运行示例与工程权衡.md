# MMA Store Rebalancer 深度剖析（下篇）——设计模式、运行示例与工程权衡

> **接续上篇**：本文是《MMA Store Rebalancer 深度剖析》的下篇，继续分析设计模式、具体运行示例和设计取舍。

---

## 五、设计模式分析

### 5.1 中介者模式（Mediator Pattern）

**定义**：用一个中介对象来封装一系列对象之间的交互，使各对象不需要显式地相互引用。

**在 MMA Store Rebalancer 中的体现**：

**AllocatorSync 作为中介者**

在旧架构中，多个组件直接交互，导致复杂的依赖关系：

```
旧架构（无中介者）：
Store Rebalancer ──▶ StorePool
       │
       └─────────────▶ MMA Allocator
Replicate Queue ───▶ StorePool
       │
       └─────────────▶ MMA Allocator
Lease Queue ────────▶ StorePool
```

**问题**：每个组件需要知道如何更新 StorePool 和 MMA Allocator，逻辑重复且容易出错。

**新架构（中介者模式）**：

```go
// mmaintegration/allocator_sync.go:76-92
type AllocatorSync struct {  // ← 中介者
	sp           storePool     // 被协调的对象 1
	mmaAllocator mmaState      // 被协调的对象 2
	mu           struct {
		syncutil.Mutex
		trackedChanges map[SyncChangeID]trackedAllocatorChange
	}
}
```

```
新架构（中介者模式）：
MMA Store Rebalancer ──▶ AllocatorSync ──▶ StorePool
                                  │
                                  └────────▶ MMA Allocator
Replicate Queue ───────▶ AllocatorSync ──▶ StorePool
                                  │
                                  └────────▶ MMA Allocator
Lease Queue ────────────▶ AllocatorSync ──▶ StorePool
```

**中介者的职责**（[allocator_sync.go:217-278](pkg/kv/kvserver/mmaintegration/allocator_sync.go#L217-L278)）：

1. **Pre-Apply 注册**：
   ```go
   func (as *AllocatorSync) MMAPreApply(...) SyncChangeID {
       trackedChange := trackedAllocatorChange{...}
       return as.addTrackedChange(trackedChange)
   }
   ```

2. **Post-Apply 分发**：
   ```go
   func (as *AllocatorSync) PostApply(ctx context.Context, syncChangeID SyncChangeID, success bool) {
       trackedChange := as.getTrackedChange(syncChangeID)

       // 通知 MMA Allocator
       if trackedChange.isMMARegistered {
           as.mmaAllocator.AdjustPendingChangeDisposition(...)
       }

       // 更新 StorePool
       if success {
           switch {
           case trackedChange.leaseTransferOp != nil:
               as.sp.UpdateLocalStoresAfterLeaseTransfer(...)
           case trackedChange.changeReplicasOp != nil:
               as.sp.UpdateLocalStoreAfterRebalance(...)
           }
       }
   }
   ```

**优势**：

- **解耦**：各组件只需与 AllocatorSync 交互，不需要知道 StorePool 和 MMA Allocator 的存在
- **集中控制**：变更追踪和状态同步逻辑集中在 AllocatorSync 中
- **易于扩展**：未来添加新的 Queue（如 Split Queue）时，只需调用 AllocatorSync 的接口

**与传统中介者模式的差异**：

- **传统中介者**：所有对象都通过中介者通信，对象间完全解耦
- **本实现**：AllocatorSync 只负责协调 StorePool 和 MMA Allocator，各组件仍然直接调用 Replica 的方法（如 `AdminTransferLease()`）

### 5.2 策略模式（Strategy Pattern）的缺失

**观察**：MMA Store Rebalancer 与旧 Store Rebalancer 是**平行实现**，而不是通过策略模式切换算法。

**旧 Store Rebalancer 的策略模式**（[store_rebalancer.go:114-128](pkg/kv/kvserver/store_rebalancer.go#L114-L128)）：

```go
type StoreRebalancer struct {
	// ...
	objectiveProvider RebalanceObjectiveProvider  // ← 策略接口
}

// 可以在 QPS 和 CPU 两种策略之间切换
type RebalanceObjectiveProvider interface {
	Objective() allocator.RangeRebalanceObjective
}
```

**MMA Store Rebalancer 的实现**：

```go
// mma_store_rebalancer.go:47-53
type mmaStoreRebalancer struct {
	store *mmaStore
	mma   mmaprototype.Allocator  // ← 直接依赖具体实现，无策略接口
	st    *cluster.Settings
	sp    *storepool.StorePool
	as    *mmaintegration.AllocatorSync
}
```

**为什么不使用策略模式？**

1. **算法复杂度**：MMA Allocator 的算法远比 QPS/CPU 单维度优化复杂，不适合与旧算法共享接口
2. **状态管理**：MMA Allocator 维护全局集群状态，而旧 Allocator 是无状态的
3. **阶段性替代**：MMA 是旧 Store Rebalancer 的替代品，而非变体，最终会完全移除旧实现

**设计决策**：通过集群设置 `LoadBasedRebalancingMode` 控制是否启用 MMA：

```go
// mma_store_rebalancer.go:84-86
if !kvserverbase.LoadBasedRebalancingModeIsMMA(&m.st.SV) {
	continue  // 禁用 MMA，等待下一次 tick
}

// store_rebalancer.go:172
disabled: func() bool {
	mode := kvserverbase.LoadBasedRebalancingMode.Get(&st.SV)
	return mode == kvserverbase.LBRebalancingOff || kvserverbase.LoadBasedRebalancingModeIsMMA(&st.SV) ||
		rq.store.cfg.TestingKnobs.DisableStoreRebalancer
}
```

**优势**：

- **简单**：避免复杂的策略切换逻辑
- **安全**：两个实现完全独立，互不干扰
- **清晰**：代码路径一目了然，易于调试

### 5.3 观察者模式（Observer Pattern）的隐式应用

**定义**：定义对象间的一对多依赖关系，当一个对象状态改变时，所有依赖它的对象都会得到通知。

**在 Gossip 网络中的体现**：

```go
// server.go（简化）
func (s *Server) startStoreRebalancers() {
	// 注册 Gossip 回调
	s.gossip.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix),
		func(_ string, content roachpb.Value) {
			var storeDesc roachpb.StoreDescriptor
			if err := content.GetProto(&storeDesc); err != nil {
				return
			}

			// 通知 MMA Allocator（观察者）
			msg := convertToStoreLoadMsg(storeDesc)
			s.mmaAllocator.ProcessStoreLoadMsg(ctx, &msg)
		})
}
```

**观察者模式结构**：

- **Subject（主题）**：Gossip Network
- **Observer（观察者）**：MMA Allocator
- **通知机制**：回调函数

**隐式的原因**：

- 没有显式的 `Observer` 接口
- 回调函数直接调用 `ProcessStoreLoadMsg()`

**优势**：

- **解耦**：Gossip 不需要知道 MMA Allocator 的存在
- **扩展性**：可以注册多个回调，处理不同的 Gossip 消息

**与传统观察者模式的差异**：

- **传统观察者**：Subject 维护 Observer 列表，调用 `Update()` 方法
- **本实现**：Gossip 维护回调函数列表，调用回调函数

### 5.4 资源池模式（Object Pool Pattern）的应用

**定义**：维护一个对象池，避免频繁创建和销毁对象。

**在 AllocatorSync 中的体现**：

```go
// mmaintegration/allocator_sync.go:81-91
type AllocatorSync struct {
	mu struct {
		syncutil.Mutex
		changeSeqGen   SyncChangeID
		trackedChanges map[SyncChangeID]trackedAllocatorChange  // ← 对象池
	}
}
```

**对象池的生命周期**：

```
1. Pre-Apply：创建 trackedAllocatorChange，插入 map
   ├─ MMAPreApply()
   └─ addTrackedChange() ──▶ map[SyncChangeID]trackedAllocatorChange

2. 执行变更：AdminTransferLease() 或 changeReplicasImpl()

3. Post-Apply：从 map 中取出并删除
   ├─ PostApply()
   └─ getTrackedChange() ──▶ delete(map, SyncChangeID)
```

**为什么是"池"？**

- **临时对象**：`trackedAllocatorChange` 的生命周期很短（几秒到几分钟）
- **频繁创建**：每次重平衡可能创建几十个对象
- **避免垃圾回收**：通过 map 管理，避免频繁的 GC

**与传统对象池的差异**：

- **传统对象池**：对象创建后放入池中，使用完毕后归还池中复用
- **本实现**：对象创建后放入 map，使用完毕后删除（不复用）

**为什么不复用对象？**

- **内存占用小**：`trackedAllocatorChange` 结构体很小（几十字节）
- **逻辑简单**：不复用避免状态重置的复杂性
- **GC 友好**：Go 的 GC 对短生命周期小对象优化较好

### 5.5 分阶段提交模式（Two-Phase Commit Pattern）的变体

**定义**：将操作分为准备阶段和提交阶段，确保原子性。

**在 MMA Store Rebalancer 中的体现**：

**Phase 1：Pre-Apply（准备阶段）**

```go
// mma_store_rebalancer.go:163
changeID := m.as.MMAPreApply(ctx, repl.RangeUsageInfo(), change)
```

- **目标**：注册变更意图，预留资源
- **操作**：
  1. 分配 `SyncChangeID`
  2. 记录 Range 的负载信息
  3. 插入 `trackedChanges` map

**Phase 2：Execution（执行阶段）**

```go
// mma_store_rebalancer.go:165-172
switch {
case change.IsPureTransferLease():
	err = m.applyLeaseTransfer(ctx, repl, change)
case change.IsChangeReplicas():
	err = m.applyReplicaChanges(ctx, repl, change)
}
```

- **目标**：执行实际的 Lease Transfer 或 Replica Changes
- **操作**：
  1. 调用 Raft 协议执行变更
  2. 等待变更完成或失败

**Phase 3：Post-Apply（提交阶段）**

```go
// mma_store_rebalancer.go:175
m.as.PostApply(ctx, changeID, err == nil /*success*/)
```

- **目标**：确认变更结果，更新全局状态
- **操作**：
  1. 根据 `SyncChangeID` 获取 `trackedAllocatorChange`
  2. 通知 MMA Allocator（`AdjustPendingChangeDisposition`）
  3. 更新 StorePool

**与传统 2PC 的差异**：

| 维度 | 传统 2PC | MMA Store Rebalancer |
|------|---------|----------------------|
| **原子性** | 所有参与者要么全部提交，要么全部回滚 | 单个 Range 的变更是原子的，但多个 Range 之间无原子性保证 |
| **协调者** | 存在中心化的协调者 | AllocatorSync 扮演协调者角色，但不强制原子性 |
| **失败处理** | Phase 1 失败 → 回滚；Phase 2 失败 → 根据日志恢复 | 失败后通知 MMA Allocator，由其重新计算 |
| **阻塞** | 传统 2PC 阻塞直到所有参与者响应 | 本实现非阻塞，变更失败不影响其他 Range |

**为什么不使用严格的 2PC？**

1. **性能**：严格的 2PC 会阻塞所有 Range，影响吞吐量
2. **可用性**：分布式系统中，某些节点可能长时间不可达，严格的 2PC 会导致整体不可用
3. **最终一致性**：CockroachDB 的负载均衡追求最终一致性，而非强一致性

### 5.6 有限状态机模式（State Machine Pattern）的缺失

**观察**：MMA Store Rebalancer 没有显式的状态机。

**为什么不需要状态机？**

**原因 1：无状态设计**

```go
// mma_store_rebalancer.go:47-53
type mmaStoreRebalancer struct {
	store *mmaStore
	mma   mmaprototype.Allocator
	st    *cluster.Settings
	sp    *storepool.StorePool
	as    *mmaintegration.AllocatorSync
}
```

- **无状态**：`mmaStoreRebalancer` 本身不维护任何状态变量
- **依赖外部状态**：所有状态都在 MMA Allocator 和 AllocatorSync 中

**原因 2：简单的控制流**

```go
// mma_store_rebalancer.go:69-100
func (m *mmaStoreRebalancer) run(ctx context.Context, stopper *stop.Stopper) {
	timer := time.NewTicker(...)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			for {
				attemptedChanges := m.rebalance(ctx, periodicCall)
				if !attemptedChanges {
					break
				}
			}
		}
	}
}
```

- **控制流单一**：只有"等待 tick → 重平衡 → 等待 tick"循环
- **无复杂状态转换**：不需要管理"空闲 → 重平衡中 → 等待确认"等状态

**对比**：如果使用状态机，可能的设计：

```go
type State int
const (
	Idle State = iota
	Rebalancing
	WaitingForConfirmation
)

type mmaStoreRebalancer struct {
	// ...
	mu    sync.Mutex
	state State
}
```

**缺点**：

- **增加复杂性**：需要管理状态转换的正确性
- **并发难度**：需要锁保护状态变量
- **不必要**：当前设计已足够清晰

### 5.7 模板方法模式（Template Method Pattern）

**定义**：定义算法的骨架，将某些步骤延迟到子类实现。

**在 `applyChange()` 中的体现**：

```go
// mma_store_rebalancer.go:155-177
func (m *mmaStoreRebalancer) applyChange(...) error {
	// **步骤 1**：获取 Replica（固定步骤）
	repl := m.store.GetReplicaIfExists(change.RangeID)
	if repl == nil {
		m.as.MarkChangeAsFailed(ctx, change)
		return errors.Errorf("replica not found for range %d", change.RangeID)
	}

	// **步骤 2**：Pre-Apply（固定步骤）
	changeID := m.as.MMAPreApply(ctx, repl.RangeUsageInfo(), change)

	var err error
	// **步骤 3**：执行变更（变化步骤，根据变更类型调用不同方法）
	switch {
	case change.IsPureTransferLease():
		err = m.applyLeaseTransfer(ctx, repl, change)
	case change.IsChangeReplicas():
		err = m.applyReplicaChanges(ctx, repl, change)
	default:
		return errors.Errorf("unknown change type for range %d", change.RangeID)
	}

	// **步骤 4**：Post-Apply（固定步骤）
	m.as.PostApply(ctx, changeID, err == nil /*success*/)
	return err
}
```

**模板方法结构**：

- **固定步骤**：步骤 1、2、4（获取 Replica、Pre-Apply、Post-Apply）
- **变化步骤**：步骤 3（根据变更类型调用 `applyLeaseTransfer()` 或 `applyReplicaChanges()`）

**优势**：

- **代码复用**：固定步骤的逻辑集中在 `applyChange()` 中
- **扩展性**：未来添加新类型的变更（如 Scatter）时，只需添加新的分支
- **清晰性**：算法骨架一目了然

**与传统模板方法的差异**：

- **传统模板方法**：基类定义算法骨架，子类重写某些步骤
- **本实现**：使用 switch 语句分发，而非继承

**为什么不使用继承？**

- **Go 语言特性**：Go 推崇组合而非继承
- **简单性**：只有两种变更类型，switch 语句足够清晰
- **类型安全**：编译时检查 `change.IsPureTransferLease()` 和 `change.IsChangeReplicas()` 的互斥性

---

## 六、具体运行示例

### 6.1 场景设定

**集群配置**：

- **节点数量**：5 个节点（Node1-Node5）
- **每个节点**：1 个 Store（Store1-Store5）
- **副本因子**：3（每个 Range 有 3 个副本）

**初始状态**（T0 时刻）：

| Store | CPU 负载 | CPU 容量 | CPU 利用率 | Leaseholder 数量 | 总 Range 数量 |
|-------|----------|----------|------------|------------------|---------------|
| Store1 | 800% | 1000% | 80% | 1000 | 1500 |
| Store2 | 600% | 1000% | 60% | 600 | 1500 |
| Store3 | 500% | 1000% | 50% | 500 | 1500 |
| Store4 | 450% | 1000% | 45% | 450 | 1500 |
| Store5 | 400% | 1000% | 40% | 400 | 1500 |

**负载不平衡识别**：

- **过载 Store**：Store1（80% > 阈值 70%）
- **欠载 Store**：Store4（45%）、Store5（40%）

**触发条件**：T1 时刻（+60秒），Store1 的 MMA Store Rebalancer 定时器触发

### 6.2 第一轮重平衡（T1 时刻）

**步骤 1：收集 Leaseholder 信息**

```go
// mma_store_rebalancer.go:132-136
knownStoresByMMA := m.mma.KnownStores()  // {Store1, Store2, Store3, Store4, Store5}
storeLeaseholderMsg, numIgnoredRanges := m.store.MakeStoreLeaseholderMsg(ctx, knownStoresByMMA)
```

**`storeLeaseholderMsg` 的内容**（简化）：

```go
StoreLeaseholderMsg{
	StoreID: Store1,
	Ranges: [
		{RangeID: 42, Replicas: [Store1, Store2, Store3], RangeLoad: {CPURate: 2.5%, ...}},
		{RangeID: 100, Replicas: [Store1, Store4, Store5], RangeLoad: {CPURate: 3.8%, ...}},
		// ...共 1000 个 Range
	],
}
```

**步骤 2：调用 MMA Allocator 计算变更**

```go
// mma_store_rebalancer.go:138-141
changes := m.mma.ComputeChanges(ctx, &storeLeaseholderMsg, mmaprototype.ChangeOptions{
	LocalStoreID: Store1,
	PeriodicCall: true,  // 第一次调用
})
```

**MMA Allocator 的决策过程**（简化）：

1. **计算 Store1 的过载程度**：
   ```
   overload = (800% - 700%) / 700% = 14.3%
   ```

2. **选择要迁移的 Range**：
   - 使用 Top-K 算法，选择 Store1 上 CPU 负载最高的 Range
   - 假设选择了 3 个 Range：
     - Range 42：CPU 2.5%（Replicas: Store1, Store2, Store3）
     - Range 100：CPU 3.8%（Replicas: Store1, Store4, Store5）
     - Range 201：CPU 3.2%（Replicas: Store1, Store2, Store4）

3. **为每个 Range 选择目标 Store**：
   - **Range 42**：
     - 当前 Leaseholder：Store1
     - 候选目标：Store2（60%）、Store3（50%）
     - 选择：Store3（利用率更低）
     - **决策**：Lease Transfer from Store1 to Store3

   - **Range 100**：
     - 当前 Leaseholder：Store1
     - 候选目标：Store4（45%）、Store5（40%）
     - 选择：Store5（利用率更低）
     - **决策**：Lease Transfer from Store1 to Store5

   - **Range 201**：
     - 当前 Leaseholder：Store1
     - 候选目标：Store2（60%）、Store4（45%）
     - 选择：Store4（利用率更低）
     - **决策**：Lease Transfer from Store1 to Store4

**返回的变更列表**（`changes`）：

```go
[]ExternalRangeChange{
	{RangeID: 42, Changes: [
		{Target: Store3, ChangeType: AddLease},
		{Target: Store1, ChangeType: RemoveLease},
	]},
	{RangeID: 100, Changes: [
		{Target: Store5, ChangeType: AddLease},
		{Target: Store1, ChangeType: RemoveLease},
	]},
	{RangeID: 201, Changes: [
		{Target: Store4, ChangeType: AddLease},
		{Target: Store1, ChangeType: RemoveLease},
	]},
}
```

**步骤 3：应用变更**

**应用 Range 42 的变更**：

```go
// mma_store_rebalancer.go:144-148
for _, change := range changes {
	if err := m.applyChange(ctx, change); err != nil {
		log.KvDistribution.VInfof(ctx, 1, "failed to apply change for range %d: %v", change.RangeID, err)
	}
}
```

**`applyChange(change)` 的执行**（Range 42）：

```go
// 步骤 1：获取 Replica
repl := m.store.GetReplicaIfExists(42)  // 返回 Replica 对象

// 步骤 2：Pre-Apply
changeID := m.as.MMAPreApply(ctx, repl.RangeUsageInfo(), change)
// 返回 SyncChangeID = 1

// 步骤 3：执行 Lease Transfer
err = repl.AdminTransferLease(ctx, Store3, false)
// Raft 协议执行，Lease 从 Store1 转移到 Store3
// 假设成功，err = nil

// 步骤 4：Post-Apply
m.as.PostApply(ctx, changeID, true /*success*/)
```

**AllocatorSync 的 Post-Apply 处理**：

```go
// mmaintegration/allocator_sync.go:259-278
func (as *AllocatorSync) PostApply(ctx context.Context, syncChangeID=1, success=true) {
	trackedChange := as.getTrackedChange(1)
	// trackedChange = {
	//     isMMARegistered: true,
	//     mmaChange: {RangeID: 42, Changes: [AddLease to Store3, RemoveLease from Store1]},
	//     usage: {CPURate: 2.5%, ...},
	//     leaseTransferOp: {transferFrom: Store1, transferTo: Store3},
	// }

	// 通知 MMA Allocator
	as.mmaAllocator.AdjustPendingChangeDisposition(ctx, trackedChange.mmaChange, true)
	// MMA Allocator 内部状态更新：
	// - Store1 的负载减少 2.5%
	// - Store3 的负载增加 2.5%

	// 更新 StorePool
	as.sp.UpdateLocalStoresAfterLeaseTransfer(Store1, Store3, trackedChange.usage)
	// StorePool 内部状态更新：
	// - Store1.LeaseCount -= 1
	// - Store3.LeaseCount += 1
}
```

**同样的流程应用于 Range 100 和 Range 201**

**第一轮重平衡结果**（T1+5秒）：

| Store | CPU 负载 | CPU 利用率 | Leaseholder 数量 | 变化 |
|-------|----------|------------|------------------|------|
| Store1 | 790% | 79% | 997 | -3 Leases |
| Store2 | 600% | 60% | 600 | - |
| Store3 | 502.5% | 50.25% | 501 | +1 Lease (Range 42) |
| Store4 | 453.2% | 45.32% | 451 | +1 Lease (Range 201) |
| Store5 | 403.8% | 40.38% | 401 | +1 Lease (Range 100) |

**步骤 4：检查是否继续重平衡**

```go
// mma_store_rebalancer.go:150
return len(changes) > 0  // 返回 true
```

- 由于 `len(changes) == 3 > 0`，`run()` 中的内循环会立即再次调用 `rebalance()`

### 6.3 第二轮重平衡（T1+5秒）

**步骤 1：收集 Leaseholder 信息**（同上）

**步骤 2：调用 MMA Allocator 计算变更**

```go
changes := m.mma.ComputeChanges(ctx, &storeLeaseholderMsg, mmaprototype.ChangeOptions{
	LocalStoreID: Store1,
	PeriodicCall: false,  // 非周期性调用
})
```

**MMA Allocator 的决策过程**：

1. **重新计算 Store1 的过载程度**：
   ```
   overload = (790% - 700%) / 700% = 12.9%
   ```
   - 仍然过载，但程度减轻

2. **选择要迁移的 Range**：
   - 再次选择 CPU 负载最高的 Range
   - 假设选择了 2 个 Range：
     - Range 305：CPU 2.9%（Replicas: Store1, Store2, Store5）
     - Range 450：CPU 2.7%（Replicas: Store1, Store3, Store4）

3. **为每个 Range 选择目标 Store**：
   - **Range 305**：Lease Transfer from Store1 to Store5
   - **Range 450**：Lease Transfer from Store1 to Store4

**第二轮重平衡结果**（T1+10秒）：

| Store | CPU 负载 | CPU 利用率 | Leaseholder 数量 |
|-------|----------|------------|------------------|
| Store1 | 784.4% | 78.44% | 995 |
| Store2 | 600% | 60% | 600 |
| Store3 | 502.5% | 50.25% | 501 |
| Store4 | 455.9% | 45.59% | 452 |
| Store5 | 406.7% | 40.67% | 402 |

### 6.4 第三轮重平衡（T1+10秒）

**MMA Allocator 的决策过程**：

1. **重新计算 Store1 的过载程度**：
   ```
   overload = (784.4% - 700%) / 700% = 12.1%
   ```
   - 仍然过载

2. **选择要迁移的 Range**：
   - 继续选择 CPU 负载最高的 Range
   - 假设选择了 2 个 Range，执行 Lease Transfer

**第三轮重平衡结果**（T1+15秒）：

| Store | CPU 负载 | CPU 利用率 | Leaseholder 数量 |
|-------|----------|------------|------------------|
| Store1 | 778.8% | 77.88% | 993 |
| Store2 | 600% | 60% | 600 |
| Store3 | 502.5% | 50.25% | 501 |
| Store4 | 458.6% | 45.86% | 453 |
| Store5 | 409.6% | 40.96% | 403 |

### 6.5 第四轮重平衡（T1+15秒）

**MMA Allocator 的决策过程**：

1. **重新计算 Store1 的过载程度**：
   ```
   overload = (778.8% - 700%) / 700% = 11.3%
   ```
   - 仍然过载

2. **选择要迁移的 Range**：
   - 继续选择 CPU 负载最高的 Range
   - 假设选择了 2 个 Range，执行 Lease Transfer

**第四轮重平衡结果**（T1+20秒）：

| Store | CPU 负载 | CPU 利用率 | Leaseholder 数量 |
|-------|----------|------------|------------------|
| Store1 | 773.2% | 77.32% | 991 |
| Store2 | 600% | 60% | 600 |
| Store3 | 502.5% | 50.25% | 501 |
| Store4 | 461.3% | 46.13% | 454 |
| Store5 | 412.5% | 41.25% | 404 |

### 6.6 第五轮重平衡（T1+20秒）

**MMA Allocator 的决策过程**：

1. **重新计算 Store1 的过载程度**：
   ```
   overload = (773.2% - 700%) / 700% = 10.5%
   ```
   - 仍然过载

2. **选择要迁移的 Range**：
   - 继续选择 CPU 负载最高的 Range
   - 但此时发现：
     - 所有候选 Range 的 CPU 负载都很低（< 1%）
     - 迁移这些 Range 对整体负载均衡的改善不大
   - **决策**：不生成任何变更

**返回的变更列表**：

```go
changes := []ExternalRangeChange{}  // 空列表
```

**步骤 4：检查是否继续重平衡**

```go
// mma_store_rebalancer.go:150
return len(changes) > 0  // 返回 false
```

- 由于 `len(changes) == 0`，`run()` 中的内循环退出
- 等待下一次 tick（T2 时刻，+60秒）

**最终状态**（T1+20秒）：

| Store | CPU 负载 | CPU 利用率 | Leaseholder 数量 | 总变更 |
|-------|----------|------------|------------------|--------|
| Store1 | 773.2% | 77.32% | 991 | -9 Leases |
| Store2 | 600% | 60% | 600 | - |
| Store3 | 502.5% | 50.25% | 501 | +1 Lease |
| Store4 | 461.3% | 46.13% | 454 | +4 Leases |
| Store5 | 412.5% | 41.25% | 404 | +4 Leases |

**分析**：

- **收敛时间**：约 20 秒（4 轮迭代）
- **负载改善**：Store1 的 CPU 利用率从 80% 降至 77.32%（下降 2.68%）
- **未完全收敛**：Store1 仍然高于目标（70%），但剩余的 Range 负载都很低，继续迁移效果不明显
- **下一次周期**：等待 60 秒后，如果 Store1 的负载仍然过高，会继续重平衡

### 6.7 边界场景：Replica 变更（T2 时刻）

假设在 T2 时刻（T1+60秒），MMA Allocator 决定对 Range 500 进行副本变更（添加副本到 Store5，并转移 Lease）。

**初始状态**：

- **Range 500**：
  - Replicas: [Store1, Store2, Store3]
  - Leaseholder: Store1
  - CPU 负载：4.5%

**MMA Allocator 的决策**：

- **目标**：将 Range 500 的 Lease 转移到 Store5
- **问题**：Store5 上没有 Range 500 的副本
- **解决方案**：先添加副本到 Store5，再转移 Lease

**返回的变更**：

```go
ExternalRangeChange{
	RangeID: 500,
	Changes: [
		{Target: Store5, ChangeType: AddReplica, Next: {ReplicaType: VOTER_FULL, IsLeaseholder: true}},
		{Target: Store5, ChangeType: AddLease},
		{Target: Store1, ChangeType: RemoveLease},
	],
}
```

**应用变更**：

```go
// mma_store_rebalancer.go:169
err = m.applyReplicaChanges(ctx, repl, change)
```

**`applyReplicaChanges()` 的执行**：

```go
// mma_store_rebalancer.go:198-207
_, err := repl.changeReplicasImpl(
	ctx,
	repl.Desc(),
	kvserverpb.SnapshotRequest_REPLICATE_QUEUE,
	0,
	kvserverpb.ReasonRebalance,
	"todo: this is the rebalance detail for the range log",
	change.ReplicationChanges(),  // 转换为 kvpb.ReplicationChanges
)
```

**`change.ReplicationChanges()` 的转换**（[range_change.go:170-232](pkg/kv/kvserver/allocator/mmaprototype/range_change.go#L170-L232)）：

```go
kvpb.ReplicationChanges{
	{ChangeType: ADD_VOTER, Target: Store5},  // 添加副本到 Store5（放在第一位，因为是新 Leaseholder）
	{ChangeType: REMOVE_VOTER, Target: Store3},  // 移除 Store3 的副本（为了保持副本因子为 3）
}
```

**注意**：Lease Transfer 会在副本添加完成后自动发生（参见 `Replica.maybeTransferLeaseDuringLeaveJoint`）

**执行过程**：

1. **添加副本到 Store5**：
   - Raft Leader（Store1）向 Store5 发送快照
   - Store5 应用快照，成为 VOTER_INCOMING
   - Raft 进入 Joint Consensus 状态

2. **移除 Store3 的副本**：
   - Raft Leader（Store1）向 Store3 发送 RemoveReplica 命令
   - Store3 标记为 VOTER_OUTGOING
   - Raft 退出 Joint Consensus，Store5 变为 VOTER_FULL

3. **自动 Lease Transfer**：
   - `maybeTransferLeaseDuringLeaveJoint()` 检测到 Store5 是新的 VOTER_FULL 且被标记为新 Leaseholder
   - 自动将 Lease 从 Store1 转移到 Store5

**最终结果**：

- **Range 500**：
  - Replicas: [Store1, Store2, Store5]
  - Leaseholder: Store5
  - CPU 负载：4.5%

---

## 七、设计取舍与替代方案（Trade-offs）

### 7.1 当前设计的核心权衡

#### 权衡 1：集中式决策 vs 分布式决策

**当前设计**：集中式决策（MMA Allocator）+ 分布式执行（各 Store 的 MMA Store Rebalancer）

**优势**：

- **全局最优**：MMA Allocator 基于整个集群的负载状态计算全局最优方案
- **避免冲突**：所有 Store 共享同一个 MMA Allocator 实例，避免决策冲突

**劣势**：

- **单点瓶颈**：MMA Allocator 是单例，所有请求都需要经过它
- **内存占用**：MMA Allocator 需要维护所有 Store 和 Range 的状态（可能几 GB）
- **扩展性限制**：当集群规模超过 10000 个 Store 时，MMA Allocator 可能成为性能瓶颈

**替代方案 1：完全分布式决策**

```
每个 Store 独立决策，基于本地状态和 Gossip 得知的其他 Store 状态
```

**优势**：

- **无单点**：每个 Store 独立运行，无单点故障
- **低延迟**：决策完全在本地完成，无需等待集中式协调

**劣势**：

- **局部最优**：每个 Store 只能做出局部最优决策，无法保证全局最优
- **冲突频繁**：多个 Store 可能同时尝试迁移同一个 Range
- **抖动严重**：缺乏全局协调，容易出现 Lease 在多个 Store 之间反复转移

**替代方案 2：分层决策**

```
- 区域级 Allocator：负责区域内的负载均衡
- 集群级 Allocator：负责跨区域的负载均衡
```

**优势**：

- **减少单点压力**：将决策分散到多个区域级 Allocator
- **局部性**：大部分决策在区域内完成，减少跨区域通信

**劣势**：

- **复杂性**：需要协调多个 Allocator，避免区域间冲突
- **一致性**：区域级和集群级 Allocator 的状态可能不一致

**为什么选择当前设计？**

1. **CockroachDB 的典型规模**：大部分集群在 100-1000 个 Store 之间，单个 MMA Allocator 足以应对
2. **全局最优优先**：CockroachDB 优先考虑负载均衡的质量，而非决策延迟
3. **简单性**：集中式设计更容易理解和调试

#### 权衡 2：定期触发 vs 事件驱动

**当前设计**：定期触发（默认 60 秒）

**优势**：

- **稳定性**：只响应持续的负载不平衡，过滤短暂波动
- **批量决策**：一次性考虑多个 Range，生成更优的方案
- **减少抖动**：避免频繁的 Lease Transfer 和 Replica 迁移

**劣势**：

- **响应延迟**：最坏情况下需要等待 60 秒才能响应负载变化
- **浪费资源**：即使负载稳定，仍然定期执行 `rebalance()`（虽然会快速退出）

**替代方案 1：事件驱动**

```
负载变化 → 触发重平衡
例如：Store 的 CPU 利用率超过阈值时，立即触发
```

**优势**：

- **低延迟**：立即响应负载变化
- **节省资源**：负载稳定时不执行 `rebalance()`

**劣势**：

- **频繁触发**：负载波动频繁时，可能每秒触发多次
- **抖动**：短暂的负载峰值会导致不必要的迁移
- **复杂性**：需要定义清晰的触发条件和去抖逻辑

**替代方案 2：自适应间隔**

```
根据集群负载动态调整触发间隔：
- 负载稳定时：60 秒
- 负载波动时：30 秒
- 严重不平衡时：10 秒
```

**优势**：

- **平衡**：在稳定性和响应性之间取得平衡

**劣势**：

- **复杂性**：需要额外的逻辑判断何时调整间隔
- **参数调优**：需要仔细调优阈值和间隔值

**为什么选择当前设计？**

1. **工作负载特征**：CockroachDB 的工作负载通常是持续的，而非突发的
2. **简单性**：固定间隔的逻辑简单，易于理解和调试
3. **足够好**：60 秒的延迟在大多数场景下是可接受的

#### 权衡 3：Lease Transfer vs Replica Migration

**当前设计**：优先 Lease Transfer，必要时 Replica Migration

**Lease Transfer**：

- **延迟**：~100ms（单次 Raft 消息往返）
- **成本**：低（无数据迁移）
- **限制**：目标 Store 必须已有副本

**Replica Migration**：

- **延迟**：~10 秒到几分钟（取决于 Range 大小和网络速度）
- **成本**：高（需要传输完整快照）
- **限制**：无（可以迁移到任意 Store）

**决策逻辑**（在 MMA Allocator 中）：

```
1. 如果目标 Store 已有副本 → Lease Transfer
2. 否则 → Replica Migration（添加副本 + Lease Transfer + 可选的移除旧副本）
```

**优势**：

- **快速响应**：大部分情况下使用 Lease Transfer，快速缓解负载不平衡
- **成本低**：避免不必要的数据迁移

**劣势**：

- **受限于副本配置**：如果目标 Store 没有副本，必须进行 Replica Migration
- **延迟高**：Replica Migration 可能需要几分钟，无法快速响应负载变化

**替代方案 1：纯 Lease Transfer**

```
只执行 Lease Transfer，不执行 Replica Migration
```

**优势**：

- **低成本**：无数据迁移
- **快速**：所有操作都在 100ms 内完成

**劣势**：

- **受限**：只能在现有副本间转移 Lease
- **无法优化副本配置**：无法将副本迁移到更优的 Store

**替代方案 2：积极 Replica Migration**

```
优先使用 Replica Migration，以优化副本配置
```

**优势**：

- **最优配置**：可以将副本放置在最优的 Store 上

**劣势**：

- **成本高**：大量的数据迁移，消耗网络和磁盘带宽
- **延迟高**：响应负载变化的延迟增加

**为什么选择当前设计？**

1. **80/20 原则**：大部分情况下（80%），目标 Store 已有副本，Lease Transfer 足够
2. **成本敏感**：数据迁移的成本远高于 Lease Transfer
3. **渐进优化**：先快速响应（Lease Transfer），再慢慢优化副本配置（Replica Migration）

#### 权衡 4：同步应用 vs 异步应用

**当前设计**：同步应用（阻塞直到变更完成或失败）

```go
// mma_store_rebalancer.go:144-148
for _, change := range changes {
	if err := m.applyChange(ctx, change); err != nil {
		log.KvDistribution.VInfof(ctx, 1, "failed to apply change for range %d: %v", change.RangeID, err)
	}
}
```

**优势**：

- **简单**：逻辑直观，易于理解
- **快速失败**：立即知道变更是否成功，可以快速重试

**劣势**：

- **串行执行**：一次只能应用一个变更，无法并行
- **阻塞**：如果某个变更耗时很长（如 Replica Migration），会阻塞后续变更

**替代方案：异步应用**

```go
for _, change := range changes {
	change := change
	go func() {
		if err := m.applyChange(ctx, change); err != nil {
			log.KvDistribution.VInfof(ctx, 1, "failed to apply change for range %d: %v", change.RangeID, err)
		}
	}()
}
```

**优势**：

- **并行执行**：多个变更可以同时执行，提高吞吐量
- **非阻塞**：慢速变更不会阻塞快速变更

**劣势**：

- **复杂性**：需要处理并发错误、资源限制（如最大并发数）
- **难以调试**：多个 Goroutine 并发执行，日志交错，难以追踪
- **资源竞争**：多个 Lease Transfer 可能竞争相同的网络带宽

**为什么选择当前设计？**

1. **简单优先**：同步执行的逻辑简单，易于理解和调试
2. **足够快**：大部分 Lease Transfer 在 100ms 内完成，串行执行的延迟可接受
3. **避免竞争**：串行执行避免了多个变更竞争资源

**未来改进方向**：

- 可以引入**有限并发**（如最多 5 个并发变更），平衡吞吐量和复杂性

### 7.2 复杂度分析

**时间复杂度**：

- **单次 `rebalance()`**：
  ```
  O(N_ranges_on_store) + O(MMA_Allocator.ComputeChanges) + O(N_changes)
  其中：
  - N_ranges_on_store：本 Store 作为 Leaseholder 的 Range 数量（~1000）
  - MMA_Allocator.ComputeChanges：O(N_stores * N_ranges)（集中式算法）
  - N_changes：返回的变更数量（~10）
  ```

- **单次 `applyChange()`**：
  ```
  O(1) + O(Lease_Transfer) 或 O(Replica_Migration)
  其中：
  - O(Lease_Transfer)：~100ms
  - O(Replica_Migration)：~10s 到几分钟
  ```

**空间复杂度**：

- **MMA Store Rebalancer**：O(1)（无状态）
- **MMA Allocator**：O(N_stores + N_ranges)（维护所有 Store 和 Range 的状态）
- **AllocatorSync**：O(N_pending_changes)（通常 < 100）

**扩展性分析**：

| 集群规模 | Store 数量 | Range 数量 | MMA Allocator 内存 | 单次 ComputeChanges 延迟 |
|----------|------------|------------|-------------------|--------------------------|
| 小型 | 10 | 1000 | ~10 MB | ~10 ms |
| 中型 | 100 | 10000 | ~100 MB | ~100 ms |
| 大型 | 1000 | 100000 | ~1 GB | ~1 s |
| 超大型 | 10000 | 1000000 | ~10 GB | ~10 s |

**瓶颈分析**：

- **内存**：当集群规模超过 10000 个 Store 时，MMA Allocator 的内存占用可能成为问题
- **CPU**：`ComputeChanges()` 的 CPU 开销随集群规模线性增长
- **解决方案**：
  1. **分片**：将 MMA Allocator 分片到多个实例
  2. **采样**：只维护活跃 Range 的状态，忽略负载很低的 Range
  3. **增量计算**：缓存中间结果，避免每次重新计算

### 7.3 可维护性分析

**代码结构**：

- **模块化**：MMA Store Rebalancer、MMA Allocator、AllocatorSync 职责清晰，低耦合
- **接口抽象**：`Allocator` 接口、`replicaToApplyChanges` 接口便于测试和模拟
- **注释齐全**：关键逻辑都有 TODO 和详细注释

**测试性**：

- **单元测试**：可以 Mock `mmaprototype.Allocator` 和 `replicaToApplyChanges` 接口
- **集成测试**：可以在单个节点上运行 MMA Store Rebalancer，模拟多 Store 场景
- **仿真测试**：可以使用 `asim`（Allocator Simulator）模拟大规模集群

**调试性**：

- **日志丰富**：关键决策和变更都有日志记录
- **指标完善**：MMA Allocator 提供详细的指标（如 CPU 利用率、Lease 转移次数等）
- **观测性**：可以通过 `LoadSummaryForAllStores()` 查看所有 Store 的负载状态

**工程风险**：

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| MMA Allocator 内存泄漏 | 高（可能导致节点 OOM） | 定期审查内存使用，添加内存限制 |
| Lease Transfer 风暴 | 中（可能导致 Raft 消息拥塞） | 限制并发 Lease Transfer 数量 |
| AllocatorSync 死锁 | 低（使用简单的 Mutex） | 代码审查，避免嵌套锁 |
| MMA Allocator 计算错误 | 中（可能导致负载更不平衡） | 广泛的单元测试和仿真测试 |

### 7.4 未来演进方向

**方向 1：支持更多维度的负载均衡**

当前支持：CPU、WriteBandwidth、ByteSize

未来可能添加：
- **读 QPS**：针对读密集型工作负载
- **网络带宽**：针对跨地域复制场景
- **磁盘 IOPS**：针对 IO 密集型工作负载

**方向 2：机器学习辅助决策**

- **负载预测**：基于历史数据预测未来负载，提前迁移 Range
- **最优策略搜索**：使用强化学习搜索最优的负载均衡策略

**方向 3：更细粒度的控制**

- **Per-Tenant 负载均衡**：为不同租户设置不同的负载均衡策略
- **Zone-Aware 负载均衡**：考虑地理位置，优先在同一区域内迁移

**方向 4：与 Replicate Queue 深度集成**

当前：MMA Store Rebalancer 与 Replicate Queue 并行运行，通过 AllocatorSync 协调

未来：可能统一为单一的副本管理器，同时负责：
- Up-replication（增加副本）
- Down-replication（减少副本）
- Rebalancing（负载均衡）
- Constraint satisfaction（约束满足）

---

## 八、总结

### 8.1 核心要点回顾

**MMA Store Rebalancer 的核心创新**：

1. **多维度优化**：同时考虑 CPU、写带宽、存储容量三个维度，避免单一维度优化导致其他维度失衡
2. **集中式决策**：MMA Allocator 基于全局状态计算最优方案，避免局部贪心决策
3. **中介者模式**：AllocatorSync 协调 MMA Store Rebalancer、Replicate Queue、Lease Queue 等组件，避免冲突和抖动
4. **连续执行**：定期触发后，连续执行直到收敛，快速达到目标状态

**与旧 Store Rebalancer 的对比**：

| 维度 | 旧 Store Rebalancer | MMA Store Rebalancer |
|------|---------------------|----------------------|
| **决策维度** | 单一（QPS 或 CPU） | 多维度（CPU + WriteBandwidth + ByteSize） |
| **决策范围** | 局部（本 Store） | 全局（所有 Store） |
| **协调机制** | 无 | AllocatorSync |
| **触发方式** | 定期 + 事件驱动 | 定期 |
| **收敛速度** | 慢（单次迭代） | 快（连续迭代） |

### 8.2 适用场景

**最适合的场景**：

1. **多维度负载不平衡**：集群同时存在 CPU 热点、写带宽热点、存储容量不平衡
2. **大规模集群**：100-1000 个 Store，10000+ Range
3. **持续工作负载**：工作负载相对稳定，无频繁的突发流量

**不适合的场景**：

1. **超大规模集群**：10000+ Store，MMA Allocator 的内存和 CPU 开销可能成为瓶颈
2. **高度动态工作负载**：负载波动剧烈，定期触发（60秒）无法快速响应
3. **低延迟要求**：需要毫秒级响应的场景，MMA 的集中式决策可能延迟过高

### 8.3 延伸阅读

- **旧 Store Rebalancer**：[store_rebalancer.go](pkg/kv/kvserver/store_rebalancer.go) - 理解旧架构的局限性
- **MMA Allocator**：[allocator.go](pkg/kv/kvserver/allocator/mmaprototype/allocator.go) - 理解集中式决策算法
- **AllocatorSync**：[allocator_sync.go](pkg/kv/kvserver/mmaintegration/allocator_sync.go) - 理解协调机制
- **Replica 变更**：[replica_command.go](pkg/kv/kvserver/replica_command.go) - 理解 `changeReplicasImpl` 的实现

---

**作者**：Claude Sonnet 4.5
**日期**：2026-02-09
**版本**：v1.0（完整版）
