# Replica 初始化模块深度剖析——基于双重锁定与状态机转换的分布式副本构建机制（上篇）

## 一、职责边界与设计动机（Why）

### 1.1 系统性问题背景

在分布式 KV 存储系统中，存在一个核心的**副本生命周期管理（Replica Lifecycle Management）**难题：**如何在内存中正确、高效、线程安全地构建一个可以参与 Raft 共识的副本对象？**

如果没有 Replica 初始化机制，系统会面临以下具体困难：

#### 1.1.1 无法从磁盘数据恢复到可用状态

CockroachDB 使用 Raft 协议进行数据复制。每个 Replica 是一个 Raft 状态机的本地实例，维护着：

- **Raft 状态**：Term、Vote、Commit index、Log entries
- **状态机状态**：Applied index、RangeDescriptor、Lease 信息
- **元数据**：RangeID、ReplicaID、TenantID
- **运行时组件**：并发管理器、提案缓冲区、日志存储

在节点重启或接收快照后，必须将磁盘上的**静态数据**转换为内存中的**活跃对象**。这个过程需要：

1. **加载持久化状态**：从 Engine 读取 RangeDescriptor、HardState、TruncatedState 等
2. **重建 Raft 状态机**：创建 `raft.RawNode`，恢复 Raft 的内部状态
3. **初始化运行时组件**：创建并发管理器、提案缓冲区、流控组件等
4. **建立不变量**：确保所有状态满足一致性约束

**如果没有统一的初始化机制**：

- 不同场景（重启、快照、分裂）可能使用不同的初始化逻辑，导致状态不一致
- 可能遗漏某些组件的初始化，导致运行时 panic
- 可能破坏不变量，导致数据损坏或 Raft 协议违规

#### 1.1.2 无法处理未初始化到已初始化的状态转换

Replica 有两种状态：

**未初始化（Uninitialized）**：
- 只有 RangeID 和 ReplicaID
- 没有 RangeDescriptor（不知道 key range 和副本集成员）
- 无法处理读写请求
- 只能接收快照（Snapshot）或参与选举

**已初始化（Initialized）**：
- 有完整的 RangeDescriptor
- 知道自己管理的 key range
- 可以处理读写请求
- 可以完全参与 Raft 协议

**状态转换场景**：

```
场景 1：新副本接收初始快照
  Uninitialized → Initialized
  时机：follower 第一次从 leader 接收快照

场景 2：Range 分裂
  Parent Replica (Initialized) → Child Replica (Initialized)
  时机：Range 分裂后创建新的 RHS Replica

场景 3：Store 重启
  Disk State → Initialized Replica
  时机：LoadAndReconcileReplicas 后重建内存对象
```

**如果没有清晰的状态转换机制**：

- 可能在未初始化状态下处理请求，导致数据路由错误
- 可能重复初始化，导致状态覆盖或泄漏
- 可能在转换过程中被并发请求观察到中间状态

#### 1.1.3 无法保证并发安全性

Replica 是一个**高并发对象**，可能被以下 goroutines 同时访问：

1. **Raft goroutine**：处理 Raft Ready 事件，应用日志条目
2. **Proposal goroutine**：提交写请求到 Raft
3. **Lease request goroutine**：处理租约请求
4. **Read goroutine**：处理一致性读请求
5. **GC goroutine**：清理旧版本数据
6. **Replication queue goroutine**：执行副本重新平衡

**初始化过程必须保证**：

- **原子性**：其他 goroutines 要么看到完全未初始化的 Replica，要么看到完全已初始化的 Replica，不能看到中间状态
- **可见性**：初始化完成后，所有字段的修改对其他 goroutines 可见（内存屏障）
- **顺序性**：初始化步骤必须按特定顺序执行（如先设置 startKey 再设置 Desc）

**如果没有正确的并发控制**：

- 可能出现数据竞争（data race），导致未定义行为
- 可能观察到不一致的状态（如 Desc 已设置但 startKey 未设置）
- 可能违反不变量（如 ReplicaID 与 Descriptor 中的 ReplicaID 不匹配）

### 1.2 Replica 模块在系统中的位置

```
┌─────────────────────────────────────────────────────────────────┐
│                    CockroachDB Store 架构                        │
├─────────────────────────────────────────────────────────────────┤
│  Store 启动流程:                                                │
│    Step 1: LoadAndReconcileReplicas                             │
│      ↓ 返回 []kvstorage.Replica (磁盘侧元数据)                  │
│    Step 2: 为每个 kvstorage.Replica 创建内存对象                │
│      ↓                                                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  newInitializedReplica (本文主角)                        │  │
│  │  输入：kvstorage.LoadedReplicaState                      │  │
│  │  输出：*Replica (完全初始化，可参与 Raft)                │  │
│  └───────────────────┬──────────────────────────────────────┘  │
│                      │                                          │
│  创建的 Replica 对象包含:                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  核心组件:                                                │  │
│  │  - raft.RawNode: Raft 状态机                             │  │
│  │  - concurrency.Manager: 并发管理器（锁表）               │  │
│  │  - ProposalBuf: 提案缓冲区                                │  │
│  │  - replica_rac2.Processor: RACv2 流控处理器              │  │
│  │  - logstore.LogStore: Raft 日志存储                      │  │
│  │  - abortspan.AbortSpan: 中止事务跟踪                     │  │
│  │                                                           │  │
│  │  状态:                                                    │  │
│  │  - shMu.state: 共享状态 (Desc, Lease, Applied index)     │  │
│  │  - mu.proposals: 待处理的提案                             │  │
│  │  - startKey: Range 的起始 key                            │  │
│  └──────────────────────────────────────────────────────────┘  │
│                      │                                          │
│  Step 3: 注册到 Store.replicas map                             │
│    store.replicas[rangeID] = replica                           │
│    ↓                                                            │
│  Step 4: 启动 Raft goroutine                                   │
│    go replica.raftScheduler.processReady()                     │
│    ↓                                                            │
│  Step 5: Replica 进入正常运行状态                               │
│    - 可以处理读写请求                                           │
│    - 可以参与 Raft 共识                                         │
│    - 可以执行副本重新平衡                                       │
└─────────────────────────────────────────────────────────────────┘
```

**上游**：
- `Store.Start()`：Store 启动流程，调用 `newInitializedReplica` 重建所有 Replicas
- `Store.getOrCreateReplica()`：收到 Raft 消息时惰性创建 Replica
- `Replica.splitTrigger()`：Range 分裂时创建子 Replica

**下游**：
- `Replica.processRaftCommand()`：处理已提交的 Raft 命令
- `Replica.Send()`：处理客户端读写请求
- `Replica.tryGetOrAcquireLease()`：处理租约请求
- `replicationQueue`：将 Replica 加入复制队列进行重新平衡

**关键依赖**：
- `kvstorage.LoadedReplicaState`：从磁盘加载的状态
- `raft.RawNode`：Raft 协议的核心实现
- `concurrency.Manager`：MVCC 并发控制
- `storage.Engine`：Pebble 存储引擎

### 1.3 核心抽象与生命周期

#### 核心数据结构：Replica

```go
// 位置：replica.go (未在提供的文件中，但在 replica_init.go 中被使用)
type Replica struct {
	// 不可变字段 (在创建后不再改变)
	AmbientContext log.AmbientContext
	RangeID        roachpb.RangeID
	replicaID      roachpb.ReplicaID
	creationTime   time.Time
	store          *Store

	// raftMu 保护的字段 (Raft 相关)
	raftMu struct {
		syncutil.Mutex
		// ... Raft 相关字段
	}

	// mu 保护的字段 (状态机相关)
	mu struct {
		syncutil.RWMutex
		internalRaftGroup *raft.RawNode
		proposals         map[kvserverbase.CmdIDKey]*ProposalData
		// ... 更多字段
	}

	// shMu 保护的字段 (共享状态)
	shMu struct {
		ReplicaMutex
		state kvstorage.ReplicaState  // Desc, Lease, Applied index 等
	}

	// 并发管理器
	concMgr *concurrency.Manager

	// 其他组件
	flowControlV2 replica_rac2.Processor
	logStorage    *replicaLogStorage
	abortSpan     *abortspan.AbortSpan
	// ... 更多组件
}
```

**关键字段解释**：

| 字段 | 类型 | 用途 | 并发控制 |
|------|------|------|---------|
| `RangeID` | `roachpb.RangeID` | Range 的全局唯一标识符 | 不可变 |
| `replicaID` | `roachpb.ReplicaID` | 本地副本的 ID（每次重新添加递增） | 不可变 |
| `startKey` | `roachpb.RKey` | Range 的起始 key（用于快速路由） | 只写一次（在初始化时） |
| `mu.internalRaftGroup` | `*raft.RawNode` | Raft 状态机 | `mu` 保护 |
| `shMu.state.Desc` | `*roachpb.RangeDescriptor` | Range 的完整描述符 | `shMu` + `raftMu` |
| `concMgr` | `*concurrency.Manager` | 并发管理器（锁表） | 内部有锁 |
| `flowControlV2` | `replica_rac2.Processor` | RACv2 流控处理器 | 内部有锁 |

#### 核心状态：Initialized vs Uninitialized

**判断标准**：

```go
// 位置：replica_init.go:412-414
func (r *Replica) IsInitialized() bool {
	return r.isInitialized.Load()
}
```

**状态标志**：
- `r.isInitialized`：原子布尔值，指示 Replica 是否已初始化
- `r.shMu.state.Desc.IsInitialized()`：RangeDescriptor 是否有有效的 key range

**不变量**：
```
r.isInitialized.Load() == true
  ⇒ r.shMu.state.Desc.IsInitialized() == true
  ⇒ r.startKey != nil
  ⇒ r.mu.tenantID != (roachpb.TenantID{})
```

#### 生命周期

```
创建 (Creation)
  ↓ newUninitializedReplicaWithoutRaftGroup()
未初始化 (Uninitialized)
  - RangeID, ReplicaID 已设置
  - Desc 未设置或为空
  - 只能接收快照或参与选举
  ↓ initRaftMuLockedReplicaMuLocked(loaded) 或 initFromSnapshotLockedRaftMuLocked(desc)
已初始化 (Initialized)
  - Desc 已设置
  - startKey 已设置
  - 可以处理读写请求
  ↓ 正常运行，直到被移除
销毁 (Destroyed)
  ↓ removeReplicaImpl()
已移除 (Removed)
  - 数据已删除
  - 对象不再被 Store 引用
```

**状态转换触发条件**：

| 转换 | 触发条件 | 调用路径 |
|------|---------|---------|
| Creation → Uninitialized | 收到 Raft 消息但本地无 Replica | `Store.getOrCreateReplica()` |
| Uninitialized → Initialized (重启) | Store 启动加载磁盘数据 | `LoadAndReconcileReplicas() → newInitializedReplica()` |
| Uninitialized → Initialized (快照) | 收到初始快照 | `Replica.applySnapshot() → initFromSnapshotLockedRaftMuLocked()` |
| Initialized → Removed | 配置变更移除副本 | `Replica.changeReplicasImpl() → removeReplicaImpl()` |

---

## 二、控制流与组件协作（How it flows）

### 2.1 主要执行路径

#### 路径 1：Store 重启时初始化所有 Replicas（正常路径）

```
时间线：Store 正常重启，从磁盘恢复所有 Replicas

Step 1: Store.Start() 被调用
  ↓
Step 2: LoadAndReconcileReplicas(ctx, eng)
  返回：[]kvstorage.Replica
  内容：每个 Replica 包含 {RangeID, ReplicaID, Desc, hardState, tombstone}
  ↓
Step 3: 遍历每个 kvstorage.Replica
  for _, diskRepl := range diskReplicas {
    ↓
    Step 3.1: 调用 diskRepl.Load(ctx, eng, storeID)
      返回：kvstorage.LoadedReplicaState
      包含：
        - ReplicaID
        - LastEntryID (最后一个日志条目的 ID)
        - ReplState (包含 Desc, Lease, Applied index 等)
        - TruncState (日志截断状态)
        - hardState (Raft 的 HardState)
      ↓
    Step 3.2: 调用 newInitializedReplica(store, loaded, waitForPrevLeaseToExpire)
      位置：replica_init.go:81-95
      ═════════════════════════════════════════════════════════
      阶段 1：创建未初始化的框架
      ═════════════════════════════════════════════════════════
      r := newUninitializedReplicaWithoutRaftGroup(store, loaded.FullReplicaID())
        ↓
        创建 Replica 对象，初始化所有组件：
        - abortSpan
        - concMgr (并发管理器)
        - flowControlV2 (RACv2 处理器)
        - logStorage (日志存储)
        - proposalBuf (提案缓冲区)
        - ... 更多组件
        ↓
        此时 Replica 仍是未初始化状态：
        - r.shMu.state.Desc = uninitState.Desc (空描述符)
        - r.isInitialized = false
        - r.startKey = nil

      ═════════════════════════════════════════════════════════
      阶段 2：持有双重锁
      ═════════════════════════════════════════════════════════
      r.raftMu.Lock()
      r.mu.Lock()
      defer r.raftMu.Unlock()
      defer r.mu.Unlock()

      为什么要持有双重锁？
      - raftMu：保护 Raft 状态机的创建和配置
      - mu：保护 Replica 状态的修改（如 proposals map）
      - 双重锁定确保初始化过程的原子性

      ═════════════════════════════════════════════════════════
      阶段 3：调用 initRaftMuLockedReplicaMuLocked(loaded, waitForPrevLeaseToExpire)
      ═════════════════════════════════════════════════════════
      位置：replica_init.go:300-366
        ↓
        Step 3.3.1: 验证输入
          if desc.RangeID != r.RangeID || loaded.ReplicaID != r.replicaID {
            return AssertionFailedf(...)  // 确保状态匹配
          }
          if !desc.IsInitialized() {
            return AssertionFailedf(...)  // 确保 Desc 有效
          }
          if r.IsInitialized() {
            return AssertionFailedf(...)  // 确保不重复初始化
          }
        ↓
        Step 3.3.2: 设置 startKey
          r.setStartKeyLocked(desc.StartKey)
          位置：replica_init.go:287-296
          作用：设置 Range 的起始 key（只能设置一次）
          不变量：startKey != nil ⇒ Replica 已初始化
        ↓
        Step 3.3.3: 加载状态到 shMu.state
          r.shMu.state = loaded.ReplState
          包含：
            - Desc: RangeDescriptor
            - Lease: 当前租约
            - RaftAppliedIndex: 已应用的 Raft 索引
            - ... 更多状态
        ↓
        Step 3.3.4: 初始化日志存储状态
          ls := r.asLogStorage()
          ls.shMu.trunc = loaded.TruncState
          ls.shMu.last = loaded.LastEntryID
        ↓
        Step 3.3.5: 初始化 Raft 组
          r.initRaftGroupRaftMuLockedReplicaMuLocked()
          位置：replica_init.go:370-392
            ↓
            创建 raft.RawNode:
            rg, err := raft.NewRawNode(newRaftConfig(
              ctx,
              (*replicaRaftStorage)(r),  // Storage 接口实现
              raftpb.PeerID(r.replicaID),
              r.shMu.state.RaftAppliedIndex,
              r.store.cfg,
              r.shMu.currentRACv2Mode == rac2.MsgAppPull,  // 是否使用 pull 模式
              &raftLogger{ctx: ctx},
              (*replicaRLockedStoreLiveness)(r),
              r.store.raftMetrics,
              r.store.TestingKnobs().RaftTestingKnobs,
            ))
            ↓
            r.mu.internalRaftGroup = rg
            r.mu.raftTracer = *rafttrace.NewRaftTracer(...)
            ↓
            初始化流控处理器:
            r.flowControlV2.InitRaftLocked(ctx, replica_rac2.NewRaftNode(rg, ...), rg.LogMark())
        ↓
        Step 3.3.6: 设置描述符并触发回调
          r.setDescLockedRaftMuLocked(ctx, desc)
          位置：replica_init.go:436-516
            ↓
            更新内部状态：
            - r.shMu.state.Desc = desc
            - r.isInitialized.Store(true)  ← 关键：原子地标记为已初始化
            - r.mu.tenantID = extractTenantID(desc.StartKey)
            - r.rangeStr.store(r.replicaID, desc)
            ↓
            通知其他组件：
            - r.concMgr.OnRangeDescUpdated(desc)
            - r.flowControlV2.OnDescChangedLocked(ctx, desc, r.mu.tenantID)
            ↓
            处理特殊 Range（如 Liveness、Meta）：
            - 将其加入高优先级调度
        ↓
        Step 3.3.7: 处理租约过期（可选）
          if r.shMu.state.Lease.Sequence > 0 && waitForPrevLeaseToExpire {
            r.waitForPreviousLeaseToExpire(r.store)
            位置：replica_init.go:523-535
            作用：等待旧租约过期，避免时间戳缓存丢失导致的一致性问题
          }
          r.mu.minLeaseProposedTS = r.Clock().NowAsClockTimestamp()

      ═════════════════════════════════════════════════════════
      阶段 4：返回已初始化的 Replica
      ═════════════════════════════════════════════════════════
      return r, nil
    ↓
    Step 3.3: 将 Replica 注册到 Store
      store.mu.Lock()
      store.mu.replicas.Insert(r)
      store.mu.Unlock()
  }
  ↓
Step 4: 启动所有 Replicas 的 Raft goroutines
  for _, r := range store.mu.replicas.Values() {
    r.startRaftProcessor()
  }

状态变化总结：
  BEFORE: 磁盘数据（kvstorage.LoadedReplicaState）
  AFTER:  内存对象（*Replica，已初始化，可处理请求）
```

**关键时间点**：

| 时间点 | 操作 | 关键状态变化 | 可见性 |
|-------|------|-------------|--------|
| T0 | newUninitializedReplicaWithoutRaftGroup | `isInitialized = false` | 仅创建线程可见 |
| T1 | setStartKeyLocked | `startKey = desc.StartKey` | 持有 mu |
| T2 | r.shMu.state = loaded.ReplState | `state.Desc = desc` | 持有 mu |
| T3 | initRaftGroupRaftMuLockedReplicaMuLocked | `mu.internalRaftGroup = rg` | 持有 mu |
| T4 | setDescLockedRaftMuLocked | `isInitialized.Store(true)` | **原子发布，对所有线程可见** |
| T5 | 释放锁 | - | Replica 完全可用 |

#### 路径 2：接收快照时初始化 Replica（快照路径）

```
时间线：Follower 从 Leader 接收初始快照

Step 1: Replica.applySnapshot(ctx, inSnap, snap)
  前提：当前 Replica 是未初始化状态
  ↓
Step 2: 持有双重锁
  r.raftMu.Lock()
  r.mu.Lock()
  ↓
Step 3: 调用 initFromSnapshotLockedRaftMuLocked(ctx, &snap.Desc)
  位置：replica_init.go:396-406
  ═════════════════════════════════════════════════════════
  简化的初始化流程（相比 initRaftMuLockedReplicaMuLocked）
  ═════════════════════════════════════════════════════════
    ↓
    Step 3.1: 验证 Desc
      if !desc.IsInitialized() {
        return AssertionFailedf("initializing replica with uninitialized desc")
      }
    ↓
    Step 3.2: 设置描述符和 startKey
      r.setDescLockedRaftMuLocked(ctx, desc)
      r.setStartKeyLocked(desc.StartKey)
    ↓
    返回 nil

状态变化：
  BEFORE: Uninitialized Replica (只有 RangeID + ReplicaID)
  AFTER:  Initialized Replica (有 Desc + startKey)
```

**为什么快照初始化更简单？**

- **不需要加载 Raft 状态**：快照已经包含了完整的状态机状态
- **不需要创建 Raft 组**：Raft 组在应用快照后会被重置
- **不需要处理租约过期**：快照应用时租约会被重新获取

### 2.2 触发方式

**主动触发（Eager）**：

1. **Store 重启**：
   - 触发时机：`Store.Start()` 启动流程
   - 调用方：`Store.Start() → newInitializedReplica()`
   - 频率：每次 Store 启动时，对所有 Replicas 执行一次

2. **Range 分裂**：
   - 触发时机：Parent Range 执行分裂操作
   - 调用方：`Replica.splitTrigger() → newInitializedReplica()`
   - 频率：Range 达到分裂阈值时

**被动触发（Lazy）**：

3. **接收 Raft 消息**：
   - 触发时机：收到目标 Replica 不存在的 Raft 消息
   - 调用方：`Store.getOrCreateReplica() → newUninitializedReplica()`
   - 频率：惰性创建，按需触发
   - 注意：创建的是**未初始化** Replica，后续通过快照初始化

**触发路径对比**：

| 场景 | 初始状态 | 触发函数 | 最终状态 | 数据来源 |
|------|---------|---------|---------|---------|
| Store 重启 | 无对象 | `newInitializedReplica` | Initialized | 磁盘 |
| Range 分裂 | 无对象 | `newInitializedReplica` | Initialized | Parent Replica |
| 收到消息 | 无对象 | `newUninitializedReplica` | Uninitialized | - |
| 接收快照 | Uninitialized | `initFromSnapshotLockedRaftMuLocked` | Initialized | Snapshot |

### 2.3 与其他模块的交互

#### 交互方式 1：读取磁盘状态（Pull Model）

```
newInitializedReplica
  ↓ 依赖
kvstorage.LoadedReplicaState
  ↓ 来源
kvstorage.Replica.Load(ctx, eng, storeID)
  ↓ 读取
storage.Engine (Pebble)
  ↓ 包含的 keys
/Local/RangeID/<rangeID>/RaftReplicaID
/Local/RangeID/<rangeID>/RaftHardState
/Local/RangeID/<rangeID>/RaftTruncatedState
/Local/Range/<startKey>/RangeDescriptor
/Local/Range/<startKey>/RangeLease
... (还有十几个其他 keys)
```

**为什么使用 Pull Model？**

- **控制权在调用方**：初始化流程可以决定何时、如何加载数据
- **容易实现重试**：如果加载失败，可以重新调用
- **无需注册回调**：不需要向 Engine 注册监听器

#### 交互方式 2：初始化 Raft 状态机（Creation）

```go
// 位置：replica_init.go:372-386
rg, err := raft.NewRawNode(newRaftConfig(
	ctx,
	(*replicaRaftStorage)(r),  // 实现 raft.Storage 接口
	raftpb.PeerID(r.replicaID),
	r.shMu.state.RaftAppliedIndex,  // 告诉 Raft 从哪里开始
	r.store.cfg,
	r.shMu.currentRACv2Mode == rac2.MsgAppPull,
	&raftLogger{ctx: ctx},
	(*replicaRLockedStoreLiveness)(r),
	r.store.raftMetrics,
	r.store.TestingKnobs().RaftTestingKnobs,
))
```

**replicaRaftStorage 接口实现**：

```go
// replicaRaftStorage 实现 raft.Storage 接口
type replicaRaftStorage Replica

func (r *replicaRaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error)
func (r *replicaRaftStorage) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error)
func (r *replicaRaftStorage) Term(i uint64) (uint64, error)
func (r *replicaRaftStorage) LastIndex() (uint64, error)
func (r *replicaRaftStorage) FirstIndex() (uint64, error)
func (r *replicaRaftStorage) Snapshot() (raftpb.Snapshot, error)
```

**为什么需要这个适配器？**

- **解耦**：raft 库不依赖 CockroachDB 的存储层
- **灵活性**：可以用不同的存储后端（Pebble、内存等）
- **测试性**：可以 mock Storage 接口进行单测

#### 交互方式 3：通知其他组件（Observer Pattern）

```go
// 位置：replica_init.go:501-503
r.concMgr.OnRangeDescUpdated(desc)
r.flowControlV2.OnDescChangedLocked(ctx, desc, r.mu.tenantID)
```

**为什么需要通知？**

- **concMgr**：需要更新锁表的 key range（只锁该 Range 的 keys）
- **flowControlV2**：需要创建 RangeController（管理该 Range 的流控）

**通知顺序重要吗？**

是的。必须**先设置 Desc，再通知组件**，确保组件看到一致的状态。

#### 交互方式 4：双重锁定（Locking Protocol）

```go
// 位置：replica_init.go:85-88
r.raftMu.Lock()
defer r.raftMu.Unlock()
r.mu.Lock()
defer r.mu.Unlock()
```

**为什么需要双重锁？**

CockroachDB 使用**分层锁定策略**：

1. **raftMu**：保护 Raft 相关的状态
   - 持有者：Raft goroutine、Proposal goroutine
   - 保护字段：Raft 消息队列、Ready 处理状态

2. **mu**：保护 Replica 状态机状态
   - 持有者：Read goroutine、Proposal goroutine、GC goroutine
   - 保护字段：proposals map、internalRaftGroup、minLeaseProposedTS

3. **shMu**：保护共享状态（可被 raftMu 或 mu 保护）
   - 持有者：任何持有 raftMu 或 mu 的线程
   - 保护字段：Desc、Lease、Applied index

**初始化为什么要同时持有两把锁？**

- **原子性**：确保初始化过程不被其他操作打断
- **可见性**：确保所有字段的修改一起可见（内存屏障）
- **不变量**：确保不破坏 Raft 状态机与状态机状态的一致性

**锁顺序**：

```
正确顺序: raftMu → mu
错误顺序: mu → raftMu (会导致死锁)
```

**示例死锁场景**（如果顺序错误）：

```
Goroutine A (初始化):         Goroutine B (处理 Raft Ready):
  mu.Lock()                     raftMu.Lock()
  // 等待 raftMu              // 等待 mu
  raftMu.Lock() ← 阻塞          mu.Lock() ← 阻塞
  ❌ 死锁！
```

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 newUninitializedReplicaWithoutRaftGroup 函数解析

#### 函数签名与职责

```go
// 位置：replica_init.go:121-282
func newUninitializedReplicaWithoutRaftGroup(store *Store, id roachpb.FullReplicaID) *Replica
```

**输入**：
- `store *Store`：所属的 Store 对象
- `id roachpb.FullReplicaID`：完整的副本 ID（RangeID + ReplicaID）

**输出**：
- `*Replica`：未初始化的 Replica 对象，**不包含 Raft 组**

**为什么函数名强调 "WithoutRaftGroup"？**

这是一个**优化**：避免创建 Raft 组两次。

**场景对比**：

| 场景 | 是否需要 Raft 组 | 创建时机 |
|------|----------------|---------|
| `newUninitializedReplica` | 需要 | 立即创建（用于接收消息） |
| `newInitializedReplica` | 需要 | 延迟创建（在 `initRaftMuLockedReplicaMuLocked` 中创建） |

如果 `newInitializedReplica` 调用 `newUninitializedReplica`（包含 Raft 组创建），会导致：

```
newUninitializedReplica
  ↓ 创建 Raft 组 #1 (基于 uninitState)
initRaftMuLockedReplicaMuLocked
  ↓ 销毁 Raft 组 #1
  ↓ 创建 Raft 组 #2 (基于 loaded state)
```

**优化后的流程**：

```
newUninitializedReplicaWithoutRaftGroup
  ↓ 不创建 Raft 组
initRaftMuLockedReplicaMuLocked
  ↓ 创建 Raft 组 #1 (基于 loaded state)
```

#### 核心逻辑：组件初始化

```go
// 位置：replica_init.go:122-282
func newUninitializedReplicaWithoutRaftGroup(store *Store, id roachpb.FullReplicaID) *Replica {
	// ═════════════════════════════════════════════════════════
	// 阶段 1：创建未初始化状态
	// ═════════════════════════════════════════════════════════
	uninitState := kvstorage.UninitializedReplicaState(id.RangeID)
	// uninitState.Desc = 空描述符（只有 RangeID）

	// ═════════════════════════════════════════════════════════
	// 阶段 2：创建 Replica 结构体
	// ═════════════════════════════════════════════════════════
	r := &Replica{
		AmbientContext: store.cfg.AmbientCtx,
		RangeID:        id.RangeID,
		replicaID:      id.ReplicaID,
		creationTime:   timeutil.Now(),
		store:          store,
		abortSpan:      abortspan.New(id.RangeID),  // 事务中止跟踪
		// ... (省略部分字段)
	}

	// ═════════════════════════════════════════════════════════
	// 阶段 3：初始化并发管理器
	// ═════════════════════════════════════════════════════════
	r.concMgr = concurrency.NewManager(concurrency.Config{
		NodeDesc:           store.nodeDesc,
		RangeDesc:          uninitState.Desc,  // 未初始化的 Desc
		Settings:           store.ClusterSettings(),
		DB:                 store.DB(),
		Clock:              store.Clock(),
		Stopper:            store.Stopper(),
		IntentResolver:     store.intentResolver,
		TxnWaitMetrics:     store.txnWaitMetrics,
		SlowLatchGauge:     store.metrics.SlowLatchRequests,
		// ... 更多配置
	})

	// 为什么用 uninitState.Desc？
	// - 并发管理器需要一个初始的 Desc（即使是空的）
	// - 后续通过 OnRangeDescUpdated() 更新

	// ═════════════════════════════════════════════════════════
	// 阶段 4：初始化提案缓冲区
	// ═════════════════════════════════════════════════════════
	r.mu.proposals = map[kvserverbase.CmdIDKey]*ProposalData{}
	r.mu.proposalBuf.Init(
		(*replicaProposer)(r),
		tracker.NewLockfreeTracker(),
		r.Clock(),
		r.ClusterSettings(),
	)

	// 为什么初始化为空 map？
	// - 未初始化的 Replica 不会处理提案
	// - 初始化后才会接收提案

	// ═════════════════════════════════════════════════════════
	// 阶段 5：初始化日志存储
	// ═════════════════════════════════════════════════════════
	sideloaded := logstore.NewDiskSideloadStorage(
		store.cfg.Settings,
		id.RangeID,
		store.StateEngine().GetAuxiliaryDir(),  // SST 文件存储位置
		store.limiters.BulkIOWriteRate,
		store.StateEngine().Env(),
	)

	r.logStorage = &replicaLogStorage{
		ctx:                r.raftCtx,
		raftEntriesMonitor: store.cfg.RaftEntriesMonitor,
		cache:              store.raftEntryCache,
		onSync:             (*replicaSyncCallback)(r),
		metrics:            store.metrics,
	}
	r.logStorage.ls = &logstore.LogStore{
		RangeID:     id.RangeID,
		Engine:      store.LogEngine(),
		Sideload:    sideloaded,
		StateLoader: r.raftMu.stateLoader.StateLoader,
		SyncWaiter:  store.syncWaiters[int(id.RangeID)%len(store.syncWaiters)],
		Settings:    store.cfg.Settings,
		// ... 更多配置
	}

	// 为什么使用 RangeID % len(syncWaiters)？
	// - 负载均衡：将 Ranges 分散到多个 SyncWaiter 循环
	// - 避免单个 SyncWaiter 成为瓶颈

	// ═════════════════════════════════════════════════════════
	// 阶段 6：初始化流控处理器（RACv2）
	// ═════════════════════════════════════════════════════════
	r.flowControlV2 = replica_rac2.NewProcessor(replica_rac2.ProcessorOptions{
		NodeID:                   store.NodeID(),
		StoreID:                  r.StoreID(),
		RangeID:                  r.RangeID,
		ReplicaID:                r.replicaID,
		ReplicaForTesting:        (*replicaForRACv2)(r),
		ReplicaMutexAsserter:     rac2.MakeReplicaMutexAsserter(&r.raftMu.Mutex, (*syncutil.RWMutex)(&r.mu.ReplicaMutex)),
		RaftScheduler:            r.store.scheduler,
		AdmittedPiggybacker:      r.store.cfg.KVFlowAdmittedPiggybacker,
		ACWorkQueue:              r.store.cfg.KVAdmissionController,
		MsgAppSender:             r,
		EvalWaitMetrics:          r.store.cfg.KVFlowEvalWaitMetrics,
		RangeControllerFactory:   r.store.kvflowRangeControllerFactory,
		Knobs:                    r.store.TestingKnobs().FlowControlTestingKnobs,
	})

	// 为什么在未初始化时就创建 flowControlV2？
	// - 处理器需要监听 Raft 事件（包括快照应用）
	// - 初始化后通过 InitRaftLocked() 激活

	// ═════════════════════════════════════════════════════════
	// 阶段 7：设置初始状态
	// ═════════════════════════════════════════════════════════
	r.shMu.state = uninitState

	// ═════════════════════════════════════════════════════════
	// 阶段 8：设置日志标签
	// ═════════════════════════════════════════════════════════
	r.rangeStr.store(id.ReplicaID, uninitState.Desc)
	r.AmbientContext.AddLogTag("r", &r.rangeStr)
	r.raftCtx = logtags.AddTag(r.AnnotateCtx(context.Background()), "raft", nil)

	// 为什么添加日志标签？
	// - 便于在日志中区分不同的 Replica
	// - 格式：[r<rangeID>/<replicaID>]

	return r
}
```

#### 关键设计决策

**Q1：为什么在创建时就初始化所有组件（如 concMgr、flowControlV2），而不是延迟到 Replica 初始化？**

A：**避免 nil 检查**。

如果延迟初始化：

```go
// 每次访问都需要检查
if r.concMgr != nil {
	r.concMgr.OnRangeDescUpdated(desc)
}
```

**当前设计**：

```go
// 无需检查，直接调用
r.concMgr.OnRangeDescUpdated(desc)
```

**代价**：

- **内存开销**：未初始化的 Replica 也会占用内存
- **初始化开销**：创建组件需要时间

**权衡**：

CockroachDB 选择**简化代码逻辑**，避免 nil 检查导致的复杂性。

**Q2：为什么 `logStorage.ls.SyncWaiter` 使用取模分配？**

A：**负载均衡**。

```go
// 位置：replica_init.go:239
SyncWaiter: store.syncWaiters[int(id.RangeID)%len(store.syncWaiters)],
```

**SyncWaiter 的作用**：

- 处理 Raft 日志的 fsync 回调
- 每个 SyncWaiter 是一个独立的 goroutine

**为什么需要多个 SyncWaiter？**

- **并行处理**：多个 Ranges 的 fsync 回调可以并行处理
- **避免阻塞**：某个 Range 的慢回调不会阻塞其他 Ranges

**为什么用 RangeID % len(syncWaiters)？**

- **确定性**：同一个 Range 总是映射到同一个 SyncWaiter
- **均匀分布**：假设 RangeID 是连续的，可以均匀分布到所有 SyncWaiters

**配置**：

```go
// 默认值：8 个 SyncWaiters
len(store.syncWaiters) = 8
```

### 3.2 initRaftMuLockedReplicaMuLocked 函数解析

#### 函数签名与不变量

```go
// 位置：replica_init.go:300-366
func (r *Replica) initRaftMuLockedReplicaMuLocked(
	s kvstorage.LoadedReplicaState, waitForPrevLeaseToExpire bool,
) error
```

**输入**：
- `s kvstorage.LoadedReplicaState`：从磁盘加载的状态
- `waitForPrevLeaseToExpire bool`：是否等待旧租约过期

**输出**：
- `error`：初始化失败时返回错误

**前置条件（Preconditions）**：
- `r.raftMu` 已持有
- `r.mu` 已持有
- `r.IsInitialized() == false`（不能重复初始化）
- `s.ReplState.Desc.IsInitialized() == true`（加载的状态必须有效）

**后置条件（Postconditions）**：
- `r.IsInitialized() == true`
- `r.startKey != nil`
- `r.mu.internalRaftGroup != nil`
- `r.shMu.state.Desc == s.ReplState.Desc`

**不变量（Invariants）**：

```go
// 位置：replica_init.go:305-314
// INVARIANT 1: RangeID 和 ReplicaID 必须匹配
desc.RangeID == r.RangeID && s.ReplicaID == r.replicaID

// INVARIANT 2: 描述符必须已初始化
desc.IsInitialized() == true

// INVARIANT 3: 不能重复初始化
r.IsInitialized() == false
```

#### 核心逻辑：状态转换

```go
func (r *Replica) initRaftMuLockedReplicaMuLocked(
	s kvstorage.LoadedReplicaState, waitForPrevLeaseToExpire bool,
) error {
	desc := s.ReplState.Desc

	// ═════════════════════════════════════════════════════════
	// 阶段 1：验证不变量
	// ═════════════════════════════════════════════════════════
	if desc.RangeID != r.RangeID || s.ReplicaID != r.replicaID {
		return errors.AssertionFailedf(
			"%s: trying to init with other replica's state r%d/%d", r, desc.RangeID, s.ReplicaID)
	}
	if !desc.IsInitialized() {
		return errors.AssertionFailedf("%s: cannot init replica with uninitialized descriptor", r)
	}
	if r.IsInitialized() {
		return errors.AssertionFailedf("%s: cannot reinitialize an initialized replica", r)
	}

	// 为什么这些检查很重要？
	// - 防止使用错误的数据初始化（如其他 Replica 的状态）
	// - 防止重复初始化导致的状态覆盖
	// - 防止使用无效的描述符

	// ═════════════════════════════════════════════════════════
	// 阶段 2：设置 startKey
	// ═════════════════════════════════════════════════════════
	r.setStartKeyLocked(desc.StartKey)
	// 位置：replica_init.go:287-296

	// 为什么 startKey 很重要？
	// - 用于快速路由：Store.LookupReplica(key) → 二分查找 startKey
	// - 只能设置一次：防止 Range 的 key range 变化
	// - 不变量：startKey == desc.StartKey（在 Replica 的整个生命周期内）

	// ═════════════════════════════════════════════════════════
	// 阶段 3：加载状态机状态
	// ═════════════════════════════════════════════════════════
	r.shMu.state = s.ReplState
	// 包含：
	// - Desc: RangeDescriptor
	// - Lease: 当前租约
	// - RaftAppliedIndex: 已应用到状态机的最大索引
	// - GCThreshold: GC 阈值
	// - ... 更多状态

	// 为什么不验证 Lease 的有效性？
	// - Lease 可能已过期（节点重启后）
	// - 会在后续的租约请求中重新获取

	// ═════════════════════════════════════════════════════════
	// 阶段 4：处理 ForceFlushIndex（可选）
	// ═════════════════════════════════════════════════════════
	if r.shMu.state.ForceFlushIndex != (roachpb.ForceFlushIndex{}) {
		r.flowControlV2.ForceFlushIndexChangedLocked(context.TODO(), r.shMu.state.ForceFlushIndex.Index)
	}

	// ForceFlushIndex 是什么？
	// - 指示某个索引之前的所有 entries 必须被 force-flushed（在 pull 模式下）
	// - 用于确保关键操作（如租约转移）的日志条目被及时复制

	// ═════════════════════════════════════════════════════════
	// 阶段 5：初始化日志存储状态
	// ═════════════════════════════════════════════════════════
	ls := r.asLogStorage()
	ls.shMu.trunc = s.TruncState
	ls.shMu.last = s.LastEntryID

	// TruncState: 日志截断状态
	// - Index: 第一个有效日志条目的索引
	// - Term: 该索引处的 Term
	// 作用：Raft 不需要保留所有历史日志，可以通过快照 + 截断优化

	// LastEntryID: 最后一个日志条目的 ID
	// - Index: 最后一个条目的索引
	// - Term: 最后一个条目的 Term
	// 作用：快速获取 LastIndex()，无需扫描日志

	// ═════════════════════════════════════════════════════════
	// 阶段 6：初始化 Raft 组
	// ═════════════════════════════════════════════════════════
	if err := r.initRaftGroupRaftMuLockedReplicaMuLocked(); err != nil {
		return err
	}

	// 为什么在设置 Desc 之前初始化 Raft 组？
	// 注释说明：
	// "We do this before the call to setDescLockedRaftMuLocked(), since it flips
	//  isInitialized and we'd like the Raft group to be in place before then."
	// - 确保 Raft 组在 Replica 标记为已初始化之前就绪
	// - 避免其他 goroutines 在 isInitialized = true 后访问 nil 的 Raft 组

	// ═════════════════════════════════════════════════════════
	// 阶段 7：设置描述符（关键步骤）
	// ═════════════════════════════════════════════════════════
	r.setDescLockedRaftMuLocked(r.AnnotateCtx(context.TODO()), desc)

	// 这一步会：
	// 1. 设置 r.shMu.state.Desc = desc
	// 2. 原子地设置 r.isInitialized.Store(true) ← 关键
	// 3. 通知其他组件（concMgr, flowControlV2）
	// 4. 提取并设置 tenantID

	// ═════════════════════════════════════════════════════════
	// 阶段 8：处理租约过期（可选）
	// ═════════════════════════════════════════════════════════
	if r.shMu.state.Lease.Sequence > 0 {
		if waitForPrevLeaseToExpire {
			if err := r.waitForPreviousLeaseToExpire(r.store); err != nil {
				return err
			}
		}
		r.mu.minLeaseProposedTS = r.Clock().NowAsClockTimestamp()
	}

	// 为什么要等待旧租约过期？
	// 注释说明：
	// "Wait for the previous lease to expire. This is important because if the
	//  node was restarted, we don't want to reacquire the lease with a start
	//  time that overlaps the previous lease."
	// - 避免时间戳缓存丢失导致的一致性问题
	// - 场景：节点重启后，内存中的时间戳缓存丢失，但旧租约可能仍在其他节点生效

	return nil
}
```

#### 租约过期等待机制详解

**问题场景**：

```
时刻 T1: 节点 A 持有 Lease，ExpirationTime = T10
时刻 T2: 节点 A 崩溃并重启
时刻 T3: 节点 A 恢复，时间戳缓存丢失
时刻 T4: 节点 A 重新获取 Lease，StartTime = T4

问题：
  - 如果 T4 < T10，新租约与旧租约重叠
  - 其他节点可能仍认为旧租约有效
  - 可能出现两个节点同时认为自己持有租约
```

**解决方案**：

```go
// 位置：replica_init.go:523-535
func (r *Replica) waitForPreviousLeaseToExpire(store *Store) error {
	st := r.leaseStatusAtRLocked(r.AnnotateCtx(context.TODO()), r.Clock().NowAsClockTimestamp())
	if st.OwnedBy(store.StoreID()) {
		// 只有我们是旧租约的持有者时才等待
		if err := r.Clock().SleepUntil(
			r.AnnotateCtx(context.TODO()),
			st.Expiration().Next(),  // 等到过期时间的下一个时刻
		); err != nil {
			return err
		}
	}
	return nil
}
```

**为什么只在 `OwnedBy(store.StoreID())` 时等待？**

- **避免不必要的等待**：如果旧租约是其他节点持有的，等待无意义（我们没有时间戳缓存需要保护）
- **优化启动时间**：大多数情况下，Replica 不持有租约，无需等待

**为什么使用 `SleepUntil` 而非 `Forward minLeaseProposedTS`？**

注释解释：

```
"Note that we need to sleep (instead of just forwarding the
 minLeaseProposedTS) because we will run into assertions where the
 lease proposed time is in the future compared to r.Clock().Now(), and
 we don't allow acquiring a lease that starts in the future."
```

**原因**：

- Raft 提案的时间戳必须 ≤ 当前时间
- 如果只 forward `minLeaseProposedTS`，可能违反这个不变量
- Sleep 确保当前时间真正推进到租约过期后

---

（未完待续，请在下篇继续阅读 initRaftGroupRaftMuLockedReplicaMuLocked、setDescLockedRaftMuLocked 的详细分析，以及运行时行为、设计模式、具体示例和权衡分析）
