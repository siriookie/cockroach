# 第二十二章 Node.Start 流程——节点核心启动与分布式服务激活

## 一、第一轮 BFS：整体职责与设计动机（Why）

### 1.1 背景与要解决的问题

在 CockroachDB 的启动序列中，`Server.PreStart()` 已经完成了所有组件的**构造**（construction）工作：
- 存储引擎（Pebble）已被打开但未激活
- KV 层组件（DistSender、RangeDescriptorCache）已被创建但未连接到网络
- 准入控制（Admission Control）、SQL 执行器已被初始化但未处理请求

此时系统面临的核心问题是：**如何将一个"构造完成但处于冷启动状态"的节点激活成为集群中的活跃成员？**

具体包括：
1. **服务发现**：如何让集群中的其他节点知道本节点的存在？
2. **状态同步**：如何获取集群的全局元数据（如 Range 分布、节点拓扑）？
3. **数据激活**：如何加载磁盘上的 Replica 并启动 Raft 共识？
4. **网络注册**：如何开始接收和处理分布式 RPC 请求？

`Node.start()` 正是解决这些问题的**激活层**，它将已构造的组件从"静态配置"转变为"动态运行"。

### 1.2 在系统中的位置

```
Server.Start()  [pkg/server/server.go]
  ├─ PreStart()                    // 构造阶段：创建所有对象
  │   ├─ 打开 Pebble 引擎
  │   ├─ 初始化 Gossip（未连接）
  │   ├─ 创建 DistSender
  │   └─ 构造 Admission Control
  │
  ├─ Node.start()                  // ← 本章重点：激活阶段
  │   ├─ 注册节点描述符到 Gossip
  │   ├─ 启动 Store（加载 Replica）
  │   ├─ 连接 Gossip 网络
  │   └─ 启动后台任务
  │
  └─ AcceptClients()               // 对外服务阶段
      ├─ 监听 SQL 端口
      └─ 监听 RPC 端口
```

**协作模块**：

| 模块 | 交互方式 | 目的 |
|------|---------|------|
| **Gossip** | `Gossip.SetNodeDescriptor()`<br>`Gossip.Start()` | 将节点信息广播到集群，建立 P2P 网络 |
| **Store** | `Store.Start(ctx, stopper)` | 加载磁盘上的 Replica，启动 Raft |
| **KV Client** | 通过 `storeCfg.DB` | Store 启动后才能处理 KV 请求 |
| **Admission Control** | `SetTenantWeightProvider()` | Store 启动后才能获取租户权重 |
| **NodeLiveness** | 心跳机制 | 启动后持续向集群证明节点存活 |

### 1.3 核心对象与关键状态

#### 长期存在的结构体

```go
type Node struct {
    stopper      *stop.Stopper           // 生命周期管理
    clusterID    *base.ClusterIDContainer // 集群 UUID
    Descriptor   roachpb.NodeDescriptor   // 节点元数据（ID、地址、属性）
    storeCfg     kvserver.StoreConfig     // Store 配置（共享给所有 Store）
    stores       *kvserver.Stores         // 节点上的所有 Store（通常 1 个）

    // 状态字段
    startedAt    int64                    // HLC 墙钟时间戳
    lastUp       int64                    // 上次启动时间（从 Store 持久化读取）
    initialStart bool                     // 是否首次启动（vs 重启）

    // 异步初始化通道
    additionalStoreInitCh chan struct{}   // 新 Store 初始化完成信号
}
```

#### 核心状态

```go
type initState struct {
    nodeID               roachpb.NodeID       // 节点 ID（持久化在 Store）
    clusterID            uuid.UUID            // 集群 ID
    initializedEngines   []storage.Engine     // 已有数据的引擎（立即启动）
    uninitializedEngines []storage.Engine     // 新添加的引擎（异步初始化）
    clusterVersion       clusterversion.ClusterVersion
    initialSettingsKVs   []roachpb.KeyValue   // 初始集群设置
}
```

**关键不变量**：
- `nodeID` 必须在所有 Store 中保持一致（由 `validateStores()` 检查）
- `initializedEngines` 至少包含一个引擎（否则无法启动）
- `Descriptor` 必须先注册到 Gossip，才能启动 Store（避免 Store 发送请求时目标节点不可达）

---

## 二、第二轮 BFS：控制流与交互关系（How it flows）

### 2.1 主要执行路径

`Node.start()` 的执行是**严格串行的**，但内部操作通过 goroutine 实现**并发**。整体流程可以分为七个阶段：

#### 阶段 0：初始化节点描述符（L655-L674）

```go
n.initialStart = initialStart
n.startedAt = n.storeCfg.Clock.Now().WallTime  // 记录启动时间（HLC）

n.Descriptor = roachpb.NodeDescriptor{
    NodeID:          state.nodeID,              // 从 initState 获取
    Address:         util.MakeUnresolvedAddr(...),  // RPC 地址
    SQLAddress:      util.MakeUnresolvedAddr(...),  // SQL 地址
    HTTPAddress:     util.MakeUnresolvedAddr(...),  // HTTP 管理界面地址
    Attrs:           attrs,                     // 节点属性（如 SSD）
    Locality:        locality,                  // 地理位置（如 region=us-east）
    ClusterName:     clusterName,
    ServerVersion:   n.storeCfg.Settings.Version.LatestVersion(),
    BuildTag:        build.GetInfo().Tag,
    StartedAt:       n.startedAt,
}
```

**设计要点**：
- `NodeDescriptor` 是节点在集群中的"身份证"，包含所有路由信息
- `Address` vs `SQLAddress`：支持 RPC 和 SQL 流量分离（不同网络接口）
- `Locality` 用于跨数据中心的 Replica 放置决策

#### 阶段 1：注册到 Gossip 网络（L676-L680）

```go
n.storeCfg.Gossip.NodeID.Set(ctx, n.Descriptor.NodeID)
if err := n.storeCfg.Gossip.SetNodeDescriptor(&n.Descriptor); err != nil {
    return errors.Wrapf(err, "couldn't gossip descriptor for node %d", n.Descriptor.NodeID)
}
```

**关键点**：
- 此时 Gossip **尚未连接**到其他节点（`startGossiping()` 在后面）
- 但可以先将节点描述符写入本地 Gossip 存储
- 一旦 Gossip 连接建立，会自动将此信息传播出去

**Why this order？**
- Store 启动后会立即尝试与其他节点通信（如 Raft heartbeat）
- 如果目标节点在 Gossip 中找不到本节点的 `NodeDescriptor`，RPC 会失败
- 因此必须先注册，再启动 Store

#### 阶段 2：并发启动已初始化的 Store（L682-L725）

这是 `Node.start()` 中**最重的操作**，涉及加载磁盘上的所有 Replica。

```go
var sem *quotapool.IntPool
if !startStoresAsync {
    sem = quotapool.NewIntPool("store start concurrency", 1)  // 串行启动（测试用）
}

engineErrC := make(chan error, len(state.initializedEngines))  // 缓冲通道，避免 goroutine 阻塞

for i := range state.initializedEngines {
    engine := state.initializedEngines[i]
    err := n.stopper.RunAsyncTaskEx(ctx,
        stop.TaskOpts{TaskName: "initialize-stores", Sem: sem, WaitForSem: true},
        func(ctx context.Context) {
            start := timeutil.Now()
            s := kvserver.NewStore(ctx, n.storeCfg, engine, &n.Descriptor)
            if err := s.Start(workersCtx, n.stopper); err != nil {
                engineErrC <- errors.Wrap(err, "failed to start store")
                return
            }
            n.addStore(ctx, s)
            log.Dev.Infof(ctx, "initialized store s%s in %s (%d replicas)",
                s.StoreID(), timeutil.Since(start), s.ReplicaCount())
            engineErrC <- nil
        })
    if err != nil {
        return err
    }
}

// 等待所有 Store 启动完成（或第一个错误）
for range state.initializedEngines {
    select {
    case <-n.stopper.ShouldQuiesce():
        return errors.New("shutting down")
    case <-ctx.Done():
        return ctx.Err()
    case err := <-engineErrC:
        if err != nil {
            return err  // Fail-fast：任何 Store 失败都会导致节点启动失败
        }
    }
}
```

**并发控制**：
- **生产环境**：多个 Store 并行启动（`startStoresAsync=true`）
- **测试环境**：通过 `sem` 信号量强制串行（便于调试）

**错误处理**：
- 使用 **buffered channel**（容量 = Store 数量）避免 goroutine 泄漏
- **Fail-fast 策略**：任何一个 Store 失败，整个节点启动失败

#### 阶段 3：验证 Store 一致性（L727-L731）

```go
if err := n.validateStores(ctx); err != nil {
    return err
}
```

**不变量检查**：
```go
func (n *Node) validateStores(ctx context.Context) error {
    return n.stores.VisitStores(func(s *kvserver.Store) error {
        if s.Ident.ClusterID != n.clusterID.Get() {
            return errors.Errorf("store %s cluster ID mismatch", s.StoreID())
        }
        if s.Ident.NodeID != n.Descriptor.NodeID {
            return errors.Errorf("store %s node ID mismatch", s.StoreID())
        }
        return nil
    })
}
```

**Why needed？**
- 防止误将其他集群的磁盘挂载到当前节点
- 防止多个节点共享同一磁盘（会导致数据损坏）

#### 阶段 4：恢复节点的上次启动时间（L733-L748）

```go
var mostRecentTimestamp hlc.Timestamp
if err := n.stores.VisitStores(func(s *kvserver.Store) error {
    timestamp, err := s.ReadLastUpTimestamp(ctx)
    if err != nil {
        return err
    }
    if mostRecentTimestamp.Less(timestamp) {
        mostRecentTimestamp = timestamp
    }
    return nil
}); err != nil {
    return errors.Wrapf(err, "failed to read last up timestamp from stores")
}
n.lastUp = mostRecentTimestamp.WallTime
```

**用途**：
- 计算节点的**停机时长**（用于诊断）
- 影响 Replica 的 **lease 恢复策略**（长时间停机后可能放弃旧 lease）

#### 阶段 5：将 Store 注册为 Gossip 持久化存储（L750-L755）

```go
if err := n.storeCfg.Gossip.SetStorage(n.stores); err != nil {
    return errors.Wrap(err, "failed to initialize the gossip interface")
}
```

**关键机制**：
- Gossip 需要持久化已知节点的地址（用于下次启动时的 bootstrap）
- 通过 `n.stores` 实现 `gossip.Storage` 接口，将地址写入 Store 的系统键

**Why after Store.Start()？**
- Store 启动前，`stores` 集合为空，无法提供存储能力

#### 阶段 6：异步初始化新添加的 Store（L757-L785）

```go
if len(state.uninitializedEngines) > 0 {
    n.additionalStoreInitCh = make(chan struct{})
    if err := n.stopper.RunAsyncTask(workersCtx, "initialize-additional-stores",
        func(ctx context.Context) {
            if err := n.initializeAdditionalStores(ctx, state.uninitializedEngines); err != nil {
                log.Dev.Fatalf(ctx, "while initializing additional stores: %v", err)
            }
            close(n.additionalStoreInitCh)
        }); err != nil {
        close(n.additionalStoreInitCh)
        return err
    }
}
```

**Why 异步？**（关键设计决策）

注释中明确说明了原因（L759-L774）：

> 考虑存储 Store ID 分配器的 Range。当我们重启持有该 Range quorum 的节点集合时，
> 特别是当我们使用辅助 Store 重启时，这些 Store 在初始化期间需要 Store ID。
> 但如果我们将节点启动（特别是打开 RPC 大门）阻塞在所有 Store 完全初始化上，
> 我们将在尝试分配 Store ID 时陷入死锁。

**死锁场景**：
1. 节点 A、B、C 持有 Store ID 分配器的 Range
2. 全部重启，且都有新 Store 需要分配 ID
3. 如果都等待新 Store 初始化才启动 RPC → 无法处理分配请求 → 死锁

**解决方案**：
- 已有数据的 Store 同步启动（能够立即处理请求）
- 新 Store 异步初始化（不阻塞节点启动）
- 调用者通过 `waitForAdditionalStoreInit()` 等待完整初始化（如需要）

#### 阶段 7：启动后台任务（L787-L816）

```go
// 1. 启动定期指标计算
n.startComputePeriodicMetrics(n.stopper, base.DefaultMetricsSampleInterval)

// 2. 将节点注册为 Admission Control 的租户权重提供者
n.storeCfg.KVAdmissionController.SetTenantWeightProvider(n, n.stopper)

// 3. 启动 Gossip 网络（开始连接其他节点）
n.startGossiping(workersCtx, n.stopper)

// 4. 启动 Key Visualizer 的统计收集器（如果启用）
if keyvissettings.Enabled.Get(&n.storeCfg.Settings.SV) {
    terminateCollector = n.enableSpanStatsCollector(ctx)
}

// 5. 启动 Liveness Range 的定期 Compaction
n.startPeriodicLivenessCompaction(n.stopper, livenessRangeCompactInterval)
```

**关键顺序约束**（L791-L796）：

> **警告**：小心将此行移到启动 Store 之前；Store 升级依赖于集群版本尚未通过 Gossip 更新的事实。
> 我们有升级过程希望在服务器以给定集群版本启动时运行，但不希望在服务器以较低版本启动后立即升级时运行。
> 如果 Gossip 更早启动，这将是可能的。

**Why？**
- Store 启动时会执行**版本迁移逻辑**（如数据格式升级）
- 迁移代码依赖"启动时的集群版本"来决定是否执行
- 如果 Gossip 先启动 → 可能立即收到更高的集群版本 → 跳过必要的迁移 → 数据损坏

### 2.2 触发时机

`Node.start()` 是**同步调用**，在 `Server.Start()` 的主线程中执行：

```go
// pkg/server/server.go
func (s *Server) Start(ctx context.Context) error {
    // ... PreStart() ...

    state, err := s.listStoresAndGetInitState(ctx)  // 扫描磁盘，识别已初始化/未初始化的引擎

    if err := s.node.start(
        ctx, workersCtx,
        advAddrU, advSQLAddrU, advHTTPAddrU,
        *state,
        initialStart,
        s.cfg.ClusterName,
        s.cfg.NodeAttributes,
        s.cfg.Locality,
        s.cfg.LocalityAddresses,
    ); err != nil {
        return err
    }

    // ... AcceptClients() ...
}
```

**前置条件**：
- `PreStart()` 已完成（所有组件已构造）
- `initState` 已准备好（已扫描磁盘，识别 Store）

**后置保证**：
- 所有已初始化的 Store 已启动且可用
- 节点已在 Gossip 中可见
- 可以开始接受外部 RPC 和 SQL 连接

### 2.3 与其他组件的交互

#### 与 Gossip 的交互

```
Node.start()
    ├─ SetNodeDescriptor()  // 本地写入（L678）
    │   └─ gossip.mu.Lock()
    │       └─ gossip.nodeDescs[nodeID] = descriptor
    │
    ├─ Store.Start()        // Store 也会使用 Gossip
    │   └─ Gossip.NodeID.Set()
    │
    ├─ SetStorage()         // 注册持久化存储（L753）
    │   └─ gossip.storage = n.stores
    │
    └─ startGossiping()     // 启动 P2P 连接（L797）
        └─ Gossip.Start()
            ├─ 连接到 --join 指定的节点
            └─ 开始定期广播节点/Store 信息
```

**Gossip 的作用**：
1. **服务发现**：其他节点通过 Gossip 获取本节点的 RPC 地址
2. **元数据同步**：获取集群的 Range 分布、版本信息
3. **健康检查**：通过 Gossip 心跳判断节点存活性

#### 与 Store 的交互

`Store.Start()` 是节点启动中最复杂的操作，涉及：

**Store.Start() 的内部流程**（简化版）：

```
Store.Start()
    ├─ 1. 读取 Store 标识（L2127）
    │   └─ ident = kvstorage.ReadStoreIdent(engine)
    │
    ├─ 2. 启动 Rangefeed 调度器（L2145-L2157）
    │   └─ rangefeedScheduler.Start()
    │
    ├─ 3. 创建 ID 分配器（L2184-L2193）
    │   └─ idAlloc = idalloc.NewAllocator(RangeIDGenerator)
    │
    ├─ 4. 创建 Intent Resolver（L2202-L2212）
    │   └─ 用于清理未提交的事务意图
    │
    ├─ 5. 启动 Store Liveness（L2230-L2243）
    │   └─ storeLiveness.Start()  // 类似 NodeLiveness，但针对 Store
    │
    ├─ 6. 加载所有 Replica（L2300-L2364）
    │   └─ repls = kvstorage.LoadAndReconcileReplicas(engine)
    │   └─ for each repl:
    │       ├─ newInitializedReplica()  // 创建 Replica 对象
    │       ├─ addToReplicasByRangeIDLocked()
    │       ├─ addToReplicasByKeyLocked()
    │       └─ maybeUnquiesce()  // 唤醒使用 leader lease 的 Replica
    │
    ├─ 7. 注册 NodeLiveness 回调（L2370）
    │   └─ NodeLiveness.RegisterCallback(nodeIsLiveCallback)
    │
    ├─ 8. 启动 Gossip（L2402）
    │   └─ startGossip()  // 定期广播 Store 的容量信息
    │
    ├─ 9. 启动 Raft 处理循环（L2429）
    │   └─ processRaft()  // 核心：处理 Raft 消息和心跳
    │
    ├─ 10. 启动 Rangefeed 更新器（L2434）
    │   └─ startRangefeedUpdater()
    │
    └─ 11. 启动 Store Rebalancer（L2441-L2447）
        └─ storeRebalancer.Start()  // 定期执行 Replica 再平衡
```

**关键点**：
- 加载 Replica 时会调用 `maybeUnquiesce()`（L2361）
- 这会唤醒使用 **leader lease** 或 **expiration lease** 的 Replica
- 目的是快速重新建立 Raft leadership 和 lease

#### 与 Admission Control 的交互

```go
// L789
n.storeCfg.KVAdmissionController.SetTenantWeightProvider(n, n.stopper)
```

**Why 在 Store 启动后？**
- `TenantWeightProvider` 需要从 Store 读取租户的资源使用情况
- Store 未启动时，无法访问磁盘上的租户统计信息

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 Store.Start() 的 Replica 加载流程

这是整个节点启动中**最耗时的操作**，需要逐个加载磁盘上的所有 Replica。

#### 3.1.1 LoadAndReconcileReplicas()

```go
// pkg/kv/kvserver/kvstorage/replica_load.go
repls, err := kvstorage.LoadAndReconcileReplicas(ctx, s.TODOEngine())
```

**输入**：Pebble 引擎实例
**输出**：`[]LoadedReplicaDescriptor`（Replica 描述符列表）

**核心逻辑**：

1. **扫描 Range ID 局部键**（系统键前缀为 `\x01k{storeID}repl{rangeID}`）：
   ```go
   iter := engine.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
       LowerBound: keys.LocalRangeIDPrefix,
       UpperBound: keys.LocalRangeIDPrefixEnd,
   })
   for ; ; iter.Next() {
       rangeID := extractRangeID(iter.Key())
       // 读取 RangeDescriptor
       desc := readRangeDescriptor(engine, rangeID)
       repls = append(repls, LoadedReplicaDescriptor{RangeID: rangeID, Desc: desc})
   }
   ```

2. **协调（Reconcile）不一致状态**：
   - 检测 **split 或 merge 中途崩溃**的 Replica
   - 清理 **tombstone**（已删除但未 GC 的 Replica）
   - 验证 **Raft log 的连续性**

**性能考虑**：
- 使用 `log.Every(10 * time.Second)` 限制日志频率（L2304）
- 避免在日志输出上花费过多时间（可能有数万个 Replica）

#### 3.1.2 newInitializedReplica()

```go
rep, err := newInitializedReplica(s, state, true /* waitForPrevLeaseToExpire */)
```

**输入**：
- `s *Store`：Store 实例
- `state ReplicaState`：从磁盘加载的 Replica 状态
- `waitForPrevLeaseToExpire bool`：是否等待旧 lease 过期

**输出**：`*Replica`（完全初始化的 Replica 对象）

**核心步骤**：

1. **创建 Replica 对象**：
   ```go
   r := &Replica{
       AmbientContext: s.cfg.AmbientCtx,
       RangeID:        state.Desc.RangeID,
       store:          s,
       abortSpan:      abortspan.New(state.Desc.RangeID),
   }
   ```

2. **初始化 Raft**：
   ```go
   r.mu.Lock()
   r.mu.state = state
   r.mu.lastIndex = state.RaftAppliedIndex
   r.mu.raftLogSize = state.RaftLogSize
   r.mu.Unlock()

   // 创建 Raft 实例
   r.mu.internalRaftGroup = raft.NewRawNode(raftConfig)
   ```

3. **恢复 lease 状态**：
   ```go
   if state.Lease.Replica.StoreID == s.StoreID() {
       // 本节点持有 lease
       if state.Lease.Expiration.Less(now) {
           // lease 已过期，放弃
           r.mu.proposalBuf.FlushLeaseRequest()
       } else if waitForPrevLeaseToExpire {
           // 等待旧 lease 过期（防止脑裂）
           r.mu.minLeaseProposedTS = state.Lease.Expiration.Next()
       }
   }
   ```

**并发安全**：
- `r.mu` 必须在设置状态前加锁
- 但**不能**在持有 `r.mu` 时持有 `s.mu`（避免死锁）

**Why waitForPrevLeaseToExpire？**
- 防止"双 lease"问题：
  1. 节点 A 持有 lease，然后崩溃
  2. 节点 A 重启后，内存中仍认为自己持有 lease
  3. 但节点 B 可能已经获取了新 lease
  4. 如果 A 立即使用旧 lease → 违反了 lease 的唯一性保证

#### 3.1.3 maybeUnquiesce()

```go
if l, _ := rep.GetLease(); !l.SupportsQuiescence() && l.Sequence > 0 {
    rep.maybeUnquiesce(ctx, true /* wakeLeader */, true /* mayCampaign */)
}
```

**触发条件**：
- Lease 类型为 **leader lease** 或 **expiration lease**（不支持 quiescence）
- Lease 序号 > 0（已经分配过 lease）

**Why？**
- **Epoch-based lease**（基于 NodeLiveness）支持 quiescence，可以安全地让 Raft 进入睡眠
- **Expiration-based lease** 必须持续发送心跳来续约 → 不能 quiesce

**Unquiesce 的作用**：
1. 唤醒 Raft：发送 Raft tick 消息
2. 如果是 follower：向 leader 发送消息（触发 leader 发送心跳）
3. 如果没有 leader：允许发起选举（`mayCampaign=true`）

**Pre-vote 机制**：
- 使用 Raft **pre-vote**（L2353 注释）
- 避免扰乱现有的 Raft leader（如果已经存在）

### 3.2 startGossiping() 的定期广播机制

```go
func (n *Node) startGossiping(ctx context.Context, stopper *stop.Stopper) {
    _ = stopper.RunAsyncTask(ctx, "start-gossip", func(ctx context.Context) {
        // 验证节点描述符已经 gossip（L1055）
        if _, err := n.storeCfg.Gossip.GetNodeDescriptor(n.Descriptor.NodeID); err != nil {
            panic(err)
        }

        statusTicker := time.NewTicker(gossipStatusInterval)     // 默认 10s
        storesTicker := time.NewTicker(gossip.StoresInterval)    // 默认 10s
        nodeTicker := time.NewTicker(gossip.NodeDescriptorInterval)  // 默认 1h
        defer func() {
            nodeTicker.Stop()
            storesTicker.Stop()
            statusTicker.Stop()
        }()

        n.gossipStores(ctx)  // 启动时立即广播一次
        for {
            select {
            case <-statusTicker.C:
                n.storeCfg.Gossip.LogStatus()  // 记录 Gossip 连接状态

            case <-storesTicker.C:
                n.gossipStores(ctx)  // 广播 Store 容量信息

            case <-nodeTicker.C:
                if err := n.storeCfg.Gossip.SetNodeDescriptor(&n.Descriptor); err != nil {
                    log.Dev.Warningf(ctx, "couldn't gossip descriptor: %s", err)
                }

            case <-stopper.ShouldQuiesce():
                return
            }
        }
    })
}
```

#### 3.2.1 gossipStores() 的内容

```go
func (n *Node) gossipStores(ctx context.Context) {
    if err := n.stores.VisitStores(func(s *kvserver.Store) error {
        return s.GossipStore(ctx, false /* useCached */)
    }); err != nil {
        log.Dev.Warningf(ctx, "%v", err)
    }
}
```

**GossipStore() 广播的内容**：
1. **Store 容量信息**：
   - 总磁盘容量
   - 已用容量
   - 可用容量
   - Range 数量
   - Replica 数量
   - Lease 数量

2. **Dead Replica 信息**：
   - 哪些 Replica 已被删除（tombstone）
   - 用于其他节点清理过时的路由信息

**广播频率**：
- 每 10 秒一次（`gossip.StoresInterval`）
- 用于**负载均衡决策**：
  - StoreRebalancer 根据容量信息决定 Replica 迁移
  - Lease transfer 优先选择低负载的 Store

### 3.3 异步初始化新 Store 的防死锁机制

#### 3.3.1 死锁场景详解

假设有以下场景：
- 集群有 3 个节点：N1, N2, N3
- **Store ID 分配器的 Range**（称为 `R_alloc`）的 Replica 分布在 N1, N2, N3
- 每个节点添加了一个新磁盘（需要分配 Store ID）

**串行初始化的死锁路径**：

```
时刻 T0: 所有节点重启，进入 Node.start()

时刻 T1:
  N1: 等待新 Store 初始化（需要分配 Store ID）
  N2: 等待新 Store 初始化
  N3: 等待新 Store 初始化

时刻 T2:
  N1 的新 Store 发起 RPC: AllocateStoreID()
    → 请求路由到 R_alloc 的 leaseholder（假设在 N2）
    → N2 尚未启动 RPC 服务（因为在等待 Store 初始化）
    → 超时

时刻 T3:
  N1, N2, N3 全部超时
  → 节点启动失败
  → 集群无法启动
```

#### 3.3.2 异步初始化的解决方案

```go
if len(state.uninitializedEngines) > 0 {
    n.additionalStoreInitCh = make(chan struct{})

    // 异步启动，不阻塞 Node.start() 返回
    if err := n.stopper.RunAsyncTask(workersCtx, "initialize-additional-stores",
        func(ctx context.Context) {
            if err := n.initializeAdditionalStores(ctx, state.uninitializedEngines); err != nil {
                log.Dev.Fatalf(ctx, "while initializing additional stores: %v", err)
            }
            close(n.additionalStoreInitCh)  // 通知完成
        }); err != nil {
        close(n.additionalStoreInitCh)
        return err
    }
}
```

**关键改进**：
1. **已有 Store 同步启动** → 立即可以处理 RPC 请求
2. **新 Store 异步启动** → 不阻塞节点启动
3. **RPC 服务立即可用** → 可以响应 `AllocateStoreID()` 请求

**时间线**：

```
T0: Node.start() 开始
T1: 已有 Store 启动完成
T2: Node.start() 返回
T3: AcceptClients() → RPC 端口开始监听
T4: 新 Store 开始初始化（后台 goroutine）
  └─ 发起 AllocateStoreID() RPC
      → N2 已经可以处理请求（因为 T3 已完成）
      → 成功获取 Store ID
T5: 新 Store 初始化完成，close(additionalStoreInitCh)
```

#### 3.3.3 等待完整初始化

调用者可以通过以下方式等待所有 Store 就绪：

```go
// pkg/server/server.go
func (s *Server) Start(ctx context.Context) error {
    // ... node.start() 已返回 ...

    // 等待新 Store 初始化完成（如果需要）
    s.node.waitForAdditionalStoreInit()

    // 现在所有 Store 都已就绪
}
```

```go
func (n *Node) waitForAdditionalStoreInit() {
    if n.additionalStoreInitCh != nil {
        <-n.additionalStoreInitCh  // 阻塞直到 channel 关闭
    }
}
```

**使用场景**：
- 测试代码：需要确保所有 Store 都已启动
- 调试工具：需要完整的 Store 列表
- **生产环境不需要等待**：新 Store 启动不影响现有服务

---

## 四、动态行为分析（Runtime 行为）

### 4.1 Gossip 网络的建立过程

#### 4.1.1 Bootstrap 阶段

节点启动时，Gossip 网络尚未连接，需要通过以下方式建立连接：

**方式 1：使用 --join 参数**（最常见）

```bash
cockroach start --join=node1:26257,node2:26257,node3:26257
```

1. **读取持久化的已知节点列表**：
   ```go
   // Node.start() L753
   if err := n.storeCfg.Gossip.SetStorage(n.stores); err != nil {
       return errors.Wrap(err, "failed to initialize the gossip interface")
   }
   ```
   - Gossip 从 Store 读取上次保存的节点地址（系统键：`gossip-bootstrap-info`）
   - 如果是首次启动，列表为空

2. **连接到 --join 指定的节点**：
   ```go
   // pkg/gossip/gossip.go: Start()
   for _, addr := range g.bootstrapAddrs {
       go g.startClient(addr)  // 尝试连接
   }
   ```

3. **握手并交换节点列表**：
   - 连接成功后，发送 `GossipRequest{NodeID, Infos}`
   - 对方回复 `GossipResponse{Infos}`，包含其他节点的地址
   - 本节点将新地址添加到已知节点列表

4. **持久化新地址**：
   ```go
   // pkg/gossip/storage.go
   func (g *Gossip) updateBootstrapInfo() {
       addresses := g.getBootstrapAddresses()
       for _, addr := range addresses {
           g.storage.WriteBootstrapInfo(&gossip.BootstrapInfo{
               Addresses: addresses,
           })
       }
   }
   ```

**方式 2：单节点集群**（无 --join）

- Gossip 只包含自己，不尝试连接其他节点
- 适用于测试或单节点部署

#### 4.1.2 运行时维护

Gossip 网络建立后，通过以下机制保持连接：

**连接管理**：
```go
// pkg/gossip/client.go
func (c *client) start() {
    for {
        select {
        case <-c.closer:
            return
        case <-time.After(gossipInterval):  // 默认 1s
            c.sendGossip()  // 发送增量更新
        }
    }
}
```

**节点发现**：
- 每 10 秒广播一次 Store 信息（L1064 `storesTicker`）
- 每 1 小时广播一次节点描述符（L1065 `nodeTicker`）

**连接淘汰**：
- 如果节点超过 `gossipThreshold`（默认 30s）未响应 → 标记为"可疑"
- 超过 `livenessThreshold`（默认 5min）→ 关闭连接

### 4.2 Replica 的 Raft 状态恢复

#### 4.2.1 Raft Log 的完整性检查

加载 Replica 时，会验证 Raft log 的连续性：

```go
// pkg/kv/kvserver/kvstorage/replica_load.go
func (r *LoadedReplicaDescriptor) Load(ctx context.Context, engine storage.Engine, storeID roachpb.StoreID) (ReplicaState, error) {
    state := ReplicaState{}

    // 读取 HardState（包含 Term、Vote、Commit）
    hs, err := loadHardState(ctx, engine, r.RangeID)
    if err != nil {
        return state, err
    }
    state.RaftHardState = hs

    // 读取 Raft log 边界
    state.RaftLogSize = computeRaftLogSize(engine, r.RangeID)

    // 检查 log 连续性
    firstIndex, err := loadRaftLogFirstIndex(engine, r.RangeID)
    lastIndex, err := loadRaftLogLastIndex(engine, r.RangeID)
    if lastIndex < firstIndex {
        return state, errors.Errorf("invalid raft log: last < first")
    }

    state.RaftAppliedIndex = loadAppliedIndex(engine, r.RangeID)
    if state.RaftAppliedIndex > lastIndex {
        return state, errors.Errorf("applied index > last index")
    }

    return state, nil
}
```

**不变量**：
- `firstIndex <= appliedIndex <= lastIndex`
- `commitIndex <= lastIndex`
- `appliedIndex <= commitIndex`（由 Raft 保证）

#### 4.2.2 Raft Leadership 的重新建立

Store 启动后，Raft 会自动选举新的 leader：

**场景 1：所有节点同时重启**

1. **初始状态**：所有 Replica 启动时都是 follower
   ```go
   // pkg/kv/kvserver/replica_raft.go
   func (r *Replica) loadRaftState(state ReplicaState) {
       cfg := raft.Config{
           ID:              uint64(r.replicaID),
           ElectionTick:    int(r.cfg.RaftElectionTimeoutTicks),
           HeartbeatTick:   1,
           Storage:         (*replicaRaftStorage)(r),
           ...
       }
       r.mu.internalRaftGroup = raft.NewRawNode(cfg)
   }
   ```

2. **Election timeout 触发选举**：
   - 每个 follower 的 election timeout 随机化（防止同时选举）
   - 第一个超时的 Replica 发起 pre-vote
   - 如果 pre-vote 成功 → 发起正式选举（RequestVote RPC）
   - 获得多数票 → 成为 leader

3. **Leader 发送心跳**：
   ```go
   // pkg/kv/kvserver/store_raft.go
   func (s *Store) processRaft(ctx context.Context) {
       ticker := time.NewTicker(s.cfg.RaftTickInterval)  // 默认 200ms
       for {
           select {
           case <-ticker.C:
               s.visitReplicasLocked(func(r *Replica) {
                   r.tick()  // 触发 Raft tick
               })
           }
       }
   }
   ```

**场景 2：单个节点重启**

- 重启节点上的 Replica 启动后，立即收到现有 leader 的 heartbeat
- 不会发起选举（因为 leader 仍在运行）
- 快速恢复到 follower 状态

#### 4.2.3 Lease 的恢复策略

**Expiration-based lease**（旧机制）：
```go
// 启动时检查 lease 是否仍有效
if state.Lease.Expiration.Less(now) {
    // Lease 已过期，放弃
    r.mu.proposalBuf.FlushLeaseRequest()
    log.VEventf(ctx, 2, "lease expired, will re-acquire")
} else {
    // Lease 仍有效，但需要等待旧 lease 过期（防止双 lease）
    r.mu.minLeaseProposedTS = state.Lease.Expiration.Next()
}
```

**Epoch-based lease**（当前机制）：
- 基于 **NodeLiveness** 的 epoch（单调递增的版本号）
- 节点重启后，NodeLiveness epoch 会递增
- 旧 lease 自动失效（因为 epoch 不匹配）
- 重新获取 lease，使用新 epoch

### 4.3 Store 启动的性能特征

#### 4.3.1 加载时间分析

**影响因素**：

1. **Replica 数量**：
   - 需要逐个加载每个 Replica 的状态
   - 典型节点：10,000 - 50,000 个 Replica
   - 加载时间：O(Replica 数量)

2. **Raft log 大小**：
   - 需要读取 Raft log 的首尾位置
   - 如果 log 未被 truncate → 扫描时间长

3. **Intent 数量**：
   - 需要扫描所有未提交的 write intent
   - Intent resolver 启动时会检查这些 intent

**优化措施**：

1. **并发加载**（如果配置允许）：
   ```go
   if startStoresAsync {
       // 多个 Store 并行启动
       for i := range engines {
           go startStore(engines[i])
       }
   }
   ```

2. **惰性初始化**：
   - Replica 加载后不会立即激活所有功能
   - 例如：rangefeed 只在第一个请求到达时启动

3. **定期日志**（避免用户误以为卡死）：
   ```go
   logEvery := log.Every(10 * time.Second)
   for i, repl := range repls {
       if logEvery.ShouldLog() && i > 0 {
           log.Infof(ctx, "initialized %d/%d replicas", i, len(repls))
       }
       // ... 加载 Replica ...
   }
   ```

#### 4.3.2 内存使用峰值

**加载阶段**：
- 每个 Replica 需要分配内存结构：
  - `Replica` 对象：~10KB
  - `raft.RawNode`：~5KB
  - `proposalBuf`：~2KB
  - 总计：~17KB/Replica

- 50,000 个 Replica：
  - 17KB × 50,000 = 850 MB

**运行阶段**：
- Raft log cache：默认 128 MB/Store
- Rangefeed buffers：取决于活跃 rangefeed 数量
- Proposal buffers：取决于写入负载

---

## 五、具体示例（必须有）

### 5.1 完整的节点启动时间线

假设一个 **3 节点集群**，每个节点有 1 个 Store，每个 Store 有 10,000 个 Replica。

#### 时刻 T0：调用 Node.start()

**输入**：
```go
state = initState{
    nodeID:             1,
    clusterID:          "8a3d8e7c-1234-5678-9abc-def012345678",
    initializedEngines: []storage.Engine{engine1},
    uninitializedEngines: []storage.Engine{},  // 无新 Store
}
```

#### 时刻 T0+10ms：构造节点描述符

```go
n.Descriptor = roachpb.NodeDescriptor{
    NodeID:        1,
    Address:       util.MakeUnresolvedAddr("tcp", "node1:26257"),
    SQLAddress:    util.MakeUnresolvedAddr("tcp", "node1:26257"),
    HTTPAddress:   util.MakeUnresolvedAddr("tcp", "node1:8080"),
    Attrs:         roachpb.Attributes{Attrs: []string{"ssd"}},
    Locality:      roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east"}}},
    ServerVersion: roachpb.Version{Major: 24, Minor: 1},
    StartedAt:     1705435200000000000,  // 2024-01-16 12:00:00 UTC
}
```

#### 时刻 T0+15ms：注册到 Gossip

```go
n.storeCfg.Gossip.NodeID.Set(ctx, 1)
n.storeCfg.Gossip.SetNodeDescriptor(&n.Descriptor)
```

**Gossip 内部状态**：
```
gossip.nodeDescs[1] = {
    NodeID:  1,
    Address: "node1:26257",
    ...
}
```

此时 Gossip **尚未连接**到其他节点，但已在本地缓存。

#### 时刻 T0+20ms：启动 Store（异步）

```go
n.stopper.RunAsyncTaskEx(ctx, ..., func(ctx context.Context) {
    s := kvserver.NewStore(ctx, n.storeCfg, engine1, &n.Descriptor)
    s.Start(workersCtx, n.stopper)  // 这里会阻塞 ~5-10 秒
    n.addStore(ctx, s)
})
```

**Store.Start() 内部时间线**：

- **T0+20ms → T0+50ms**：读取 Store 标识
  ```
  ident = kvstorage.ReadStoreIdent(engine1)
  → StoreID: 1, NodeID: 1, ClusterID: "8a3d..."
  ```

- **T0+50ms → T0+100ms**：创建 ID 分配器、Intent Resolver 等

- **T0+100ms → T0+8s**：**加载 Replica**（最耗时）
  ```
  repls = kvstorage.LoadAndReconcileReplicas(engine1)
  → 10,000 个 Replica

  每隔 10 秒打印日志：
    T0+100ms: "initialized 0/10000 replicas"
    T0+1s:    "initialized 1200/10000 replicas"
    T0+2s:    "initialized 2400/10000 replicas"
    ...
    T0+8s:    "initialized 10000/10000 replicas"
  ```

- **T0+8s → T0+8.2s**：启动 Raft 处理循环
  ```go
  s.processRaft(ctx)  // 启动 goroutine，持续处理 Raft 消息
  ```

- **T0+8.2s → T0+8.5s**：启动其他后台任务
  - Rangefeed updater
  - Store rebalancer
  - Gossip 广播器

#### 时刻 T0+8.5s：Store 启动完成

```
engineErrC <- nil
log.Dev.Infof("initialized store s1 in 8.48s (10000 replicas)")
```

#### 时刻 T0+8.5s：Node.start() 继续执行

**验证 Store**：
```go
n.validateStores(ctx)
→ 检查 Store 1 的 clusterID 和 nodeID → 通过
```

**读取 lastUp 时间**：
```go
timestamp = s.ReadLastUpTimestamp(ctx)
→ 1705348800000000000  // 上次停机时间：2024-01-15 12:00:00 UTC
n.lastUp = 1705348800000000000

停机时长 = T0 - lastUp = 24 小时
```

**注册 Gossip 存储**：
```go
n.storeCfg.Gossip.SetStorage(n.stores)
→ Gossip 现在可以从 Store 读取/写入持久化数据
```

#### 时刻 T0+8.6s：启动后台任务

**1. 定期指标计算**：
```go
n.startComputePeriodicMetrics(n.stopper, 10*time.Second)
→ 每 10 秒计算一次节点级别的指标（CPU、内存、磁盘 I/O）
```

**2. 设置租户权重提供者**：
```go
n.storeCfg.KVAdmissionController.SetTenantWeightProvider(n, n.stopper)
→ Admission Control 现在可以从 Store 读取租户统计信息
```

**3. 启动 Gossip 网络**：
```go
n.startGossiping(workersCtx, n.stopper)
→ 启动 3 个 ticker：
   - statusTicker: 10s（记录 Gossip 状态）
   - storesTicker: 10s（广播 Store 容量）
   - nodeTicker: 1h（广播节点描述符）
```

**立即执行一次 gossipStores()**：
```go
n.gossipStores(ctx)
→ s.GossipStore(ctx, false)
    → 广播 Store 容量信息：
        {
            StoreID: 1,
            Capacity: {
                Available: 900 GB,
                Used: 100 GB,
                RangeCount: 10000,
                LeaseCount: 3300,
            }
        }
```

此时 Gossip 尝试连接到 `--join` 指定的节点（假设是 `node2:26257, node3:26257`）。

#### 时刻 T0+8.7s：Gossip 连接建立

**Gossip 内部日志**：
```
[gossip] connecting to node2:26257...
[gossip] connected to node2:26257 (nodeID=2)
[gossip] received 1500 infos from node2
[gossip] connecting to node3:26257...
[gossip] connected to node3:26257 (nodeID=3)
[gossip] received 1500 infos from node3
```

**收到的信息**：
- 节点 2 和节点 3 的 `NodeDescriptor`
- 节点 2 和节点 3 的 Store 容量
- 集群的 Range 分布信息（部分）
- 集群版本信息

#### 时刻 T0+8.8s：Node.start() 返回

```go
return nil
```

**此时节点状态**：
- ✅ 所有 Store 已启动
- ✅ Gossip 已连接到集群
- ✅ Raft 处理循环正在运行
- ✅ 节点对集群可见

#### 时刻 T0+9s：Raft 开始选举

**假设 Range 1 在节点 1 上的 Replica 是 follower**：

- 启动后，Raft tick 开始递增（每 200ms 一次）
- 收到节点 2 上的 leader 发来的 heartbeat：
  ```
  MsgHeartbeat{
      From:   2,
      To:     1,
      Term:   42,
      Commit: 1000000,
  }
  ```
- 回复 heartbeat response：
  ```
  MsgHeartbeatResp{
      From: 1,
      To:   2,
      Term: 42,
  }
  ```

**假设 Range 2 在节点 1 上的 Replica 是 leader**（重启前持有 leadership）：

- 启动后，election timeout 触发（随机 3-6 秒）
- 发起 pre-vote：
  ```
  MsgPreVote{
      From: 1,
      To:   2,
      Term: 43,
      LogTerm: 42,
      Index: 2000000,
  }
  ```
- 如果收到多数节点的同意 → 发起正式选举 → 成为 leader
- 开始发送 heartbeat 给 follower

#### 时刻 T0+10s：第一次 Gossip 广播

```go
case <-storesTicker.C:  // 10 秒触发
    n.gossipStores(ctx)
```

**广播内容更新**：
```
{
    StoreID: 1,
    Capacity: {
        Available: 899.8 GB,  // 略微下降（写入了一些数据）
        Used: 100.2 GB,
        RangeCount: 10000,
        LeaseCount: 3305,     // 获取了 5 个新 lease
        QueriesPerSecond: 120,
    }
}
```

#### 时刻 T0+15s：节点完全稳定

- 所有 Raft Range 都已选举出 leader
- Lease 分布趋于均衡
- 节点开始处理客户端请求

### 5.2 异步 Store 初始化的死锁避免示例

#### 场景设置

- 集群：N1, N2, N3
- **Store ID 分配器的 Range**（RangeID=2）的 leaseholder 在 N2
- 每个节点添加了一个新磁盘：
  - N1: `/dev/sdb`（未初始化）
  - N2: `/dev/sdb`（未初始化）
  - N3: `/dev/sdb`（未初始化）

#### 旧设计（同步初始化）的死锁

**时刻 T0**：所有节点重启

```
N1:
  ├─ 启动已有 Store 1（/dev/sda）
  └─ 等待新 Store 2（/dev/sdb）初始化 ← 阻塞在这里

N2:
  ├─ 启动已有 Store 1（/dev/sda）
  └─ 等待新 Store 2（/dev/sdb）初始化 ← 阻塞在这里

N3:
  ├─ 启动已有 Store 1（/dev/sda）
  └─ 等待新 Store 2（/dev/sdb）初始化 ← 阻塞在这里
```

**时刻 T1**：N1 的新 Store 尝试分配 ID

```
N1 的新 Store 初始化器:
  → 调用 AllocateStoreID() RPC
  → 路由到 RangeID=2 的 leaseholder（N2）
  → N2 的 RPC 服务器尚未启动（因为 Node.start() 未返回）
  → 超时（默认 5 分钟）
```

**结果**：死锁，所有节点无法启动。

#### 新设计（异步初始化）的解决

**时刻 T0**：所有节点重启

```
N1:
  ├─ 启动已有 Store 1（/dev/sda）→ 完成（8 秒）
  ├─ Node.start() 返回（不等待新 Store）
  ├─ AcceptClients() → RPC 服务器启动
  └─ 后台 goroutine: 初始化新 Store 2

N2:
  ├─ 启动已有 Store 1（/dev/sda）→ 完成（8 秒）
  ├─ Node.start() 返回
  ├─ AcceptClients() → RPC 服务器启动
  └─ 后台 goroutine: 初始化新 Store 2

N3:
  ├─ 启动已有 Store 1（/dev/sda）→ 完成（8 秒）
  ├─ Node.start() 返回
  ├─ AcceptClients() → RPC 服务器启动
  └─ 后台 goroutine: 初始化新 Store 2
```

**时刻 T0+10s**：N1 的后台任务开始初始化新 Store

```go
func (n *Node) initializeAdditionalStores(ctx context.Context, engines []storage.Engine) error {
    for _, engine := range engines {
        // 分配 Store ID
        storeID, err := allocateStoreID(ctx, n.storeCfg.DB)
        if err != nil {
            return err
        }

        // 写入 Store 标识
        ident := roachpb.StoreIdent{
            ClusterID: n.clusterID.Get(),
            NodeID:    n.Descriptor.NodeID,
            StoreID:   storeID,
        }
        if err := kvstorage.WriteStoreIdent(ctx, engine, ident); err != nil {
            return err
        }

        // 创建并启动 Store
        s := kvserver.NewStore(ctx, n.storeCfg, engine, &n.Descriptor)
        if err := s.Start(ctx, n.stopper); err != nil {
            return err
        }
        n.addStore(ctx, s)
    }
    return nil
}
```

**allocateStoreID() 的 RPC 路径**：

```
N1 后台任务:
  → n.storeCfg.DB.Inc(ctx, keys.StoreIDGenerator, 1)
      → DistSender.Send()
          → 查询 Range 2 的 leaseholder（通过 RangeDescriptorCache）
          → 发现 leaseholder = N2, StoreID=1
          → 发送 RPC 到 N2:26257
          → N2 的 RPC 服务器已经在运行（T0+9s 启动）
          → N2 处理请求，返回新的 Store ID = 4
  → 成功获取 Store ID
```

**时刻 T0+12s**：N1 的新 Store 初始化完成

```
N1:
  ├─ Store 1（StoreID=1）→ 运行中
  └─ Store 2（StoreID=4）→ 运行中 ✅

close(n.additionalStoreInitCh)  // 通知等待者
```

**时刻 T0+15s**：所有节点的新 Store 都已初始化

```
N1: Store 1, Store 4
N2: Store 2, Store 5
N3: Store 3, Store 6
```

---

## 六、设计取舍与权衡（Trade-offs）

### 6.1 同步 vs 异步 Store 初始化

| 维度 | 同步初始化 | 异步初始化（当前设计） |
|------|----------|---------------------|
| **简单性** | ✅ 逻辑简单，易于理解 | ⚠️ 需要额外的同步机制（channel） |
| **启动速度** | ❌ 慢（需等待所有 Store） | ✅ 快（只等待已有 Store） |
| **死锁风险** | ❌ 高（新 Store 需要 RPC） | ✅ 无死锁风险 |
| **资源利用** | ❌ 阻塞主线程 | ✅ 并发利用 CPU |
| **错误处理** | ✅ 错误立即暴露 | ⚠️ 后台错误需特殊处理（Fatal） |

**当前设计的权衡**：
- 牺牲了一定的简单性（需要 `additionalStoreInitCh`）
- 换取了**零死锁**和**更快的启动时间**
- 适合生产环境（新 Store 是罕见操作）

### 6.2 串行 vs 并发 Store 启动

| 维度 | 串行启动（`startStoresAsync=false`） | 并发启动（`startStoresAsync=true`） |
|------|--------------------------------|--------------------------------|
| **启动速度** | ❌ 慢（N 个 Store × 8 秒 = N×8 秒） | ✅ 快（max(8 秒)） |
| **CPU 使用** | ✅ 低（单线程） | ⚠️ 高（多线程竞争） |
| **内存峰值** | ✅ 低（逐个加载） | ⚠️ 高（同时加载） |
| **可调试性** | ✅ 易于调试（单线程） | ❌ 难以调试（并发竞争） |

**生产环境默认**：
- `startStoresAsync=true`（并发启动）
- 因为节点通常只有 1-2 个 Store，并发收益有限
- 但避免了串行的额外延迟

**测试环境**：
- `startStoresAsync=false`（串行启动）
- 便于复现 bug（消除并发竞争）

### 6.3 Gossip 启动时机的权衡

**当前顺序**：

```
1. Store.Start()        // 加载 Replica
2. startGossiping()     // 启动 Gossip 网络
```

**如果反过来**：

```
1. startGossiping()     // ← 先启动 Gossip
2. Store.Start()        // ← 后加载 Replica
```

**问题**（L791-L796 的注释）：

> Store 升级依赖于集群版本尚未通过 Gossip 更新的事实。

**具体场景**：

假设集群版本从 `v23.2` 升级到 `v24.1`，并且 `v24.1` 需要执行以下迁移：

```go
// pkg/kv/kvserver/store.go
func (s *Store) Start(ctx context.Context, stopper *stop.Stopper) error {
    clusterVersion := s.cfg.Settings.Version.ActiveVersion(ctx)

    if clusterVersion.Major == 24 && clusterVersion.Minor == 1 {
        // 执行 v24.1 的数据迁移
        if err := s.migrateToV24_1(ctx); err != nil {
            return err
        }
    }

    // ... 加载 Replica ...
}
```

**如果 Gossip 先启动**：

1. 节点以 `v23.2` 启动
2. Gossip 立即连接到其他节点，收到集群版本 = `v24.1`
3. `s.cfg.Settings.Version` 被 Gossip 更新为 `v24.1`
4. `Store.Start()` 执行时，发现版本已经是 `v24.1`
5. **误认为迁移已完成**，跳过迁移逻辑
6. → 数据损坏

**当前设计的保证**：
- Store 启动时，Gossip 尚未连接 → 版本信息来自本地持久化
- 迁移逻辑基于"启动时的本地版本"执行
- Gossip 启动后，版本更新不会影响已执行的迁移

### 6.4 Fail-fast vs Graceful Degradation

**当前设计**：Fail-fast

```go
case err := <-engineErrC:
    if err != nil {
        return err  // 任何 Store 失败 → 节点启动失败
    }
```

**Alternative：部分启动**

```go
case err := <-engineErrC:
    if err != nil {
        log.Errorf("failed to start store: %v", err)
        continue  // 跳过失败的 Store，继续启动其他 Store
    }
```

**权衡**：

| 策略 | 优点 | 缺点 |
|------|-----|-----|
| **Fail-fast**（当前） | ✅ 错误明确，不会部分运行<br>✅ 避免数据不一致 | ❌ 单个 Store 失败导致整个节点不可用 |
| **Graceful Degradation** | ✅ 部分服务可用<br>✅ 提高可用性 | ❌ 可能导致数据丢失（如果失败的 Store 持有唯一副本）<br>❌ 运维复杂（需要监控部分失败） |

**CockroachDB 的选择**：
- **Fail-fast** 更适合分布式数据库：
  - 数据正确性 > 可用性（在单节点层面）
  - 集群层面通过副本保证可用性（单节点失败不影响集群）

---

## 七、总结与心智模型

### 7.1 核心思想

`Node.start()` 是 CockroachDB 节点从"构造完成"到"对外服务"的**关键过渡层**，它解决了以下问题：

1. **服务发现**：通过 Gossip 让节点在集群中可见
2. **状态恢复**：从磁盘加载 Replica，恢复 Raft 状态
3. **网络激活**：启动 RPC 处理循环，开始接收请求
4. **死锁避免**：通过异步初始化新 Store，避免循环依赖

**关键设计原则**：

- **严格的顺序约束**：Gossip 注册 → Store 启动 → Gossip 连接 → 后台任务
- **Fail-fast 策略**：任何已有 Store 失败都会导致节点启动失败
- **异步非阻塞**：新 Store 的初始化不阻塞节点启动

### 7.2 心智模型

**如果只记住一件事，那就是**：

> `Node.start()` 将节点从"孤立的本地状态"转变为"集群的活跃成员"，
> 其核心是**先让已有的 Store 对外可见（通过 Gossip），
> 再启动需要网络的 Store 初始化（避免死锁）**。

**简化流程**：

```
Node.start()
  ├─ 阶段 1：身份注册
  │   └─ "我是节点 X，地址是 Y"（Gossip）
  │
  ├─ 阶段 2：状态恢复
  │   └─ "加载磁盘上的 Replica"（Store.Start）
  │
  ├─ 阶段 3：网络激活
  │   └─ "开始监听 Raft 消息"（processRaft）
  │
  └─ 阶段 4：后台服务
      └─ "定期广播状态"（startGossiping）
```

### 7.3 简化伪代码

```go
func (n *Node) start(ctx context.Context, state initState) error {
    // 0. 构造身份
    n.Descriptor = makeNodeDescriptor(state.nodeID, addresses)

    // 1. 注册到 Gossip（本地）
    gossip.SetNodeDescriptor(n.Descriptor)

    // 2. 启动已有 Store（同步，Fail-fast）
    for engine := range state.initializedEngines {
        store := NewStore(engine)
        if err := store.Start(); err != nil {
            return err  // 致命错误
        }
        n.stores.Add(store)
    }

    // 3. 验证一致性
    if err := validateStores(); err != nil {
        return err
    }

    // 4. 连接 Gossip 到其他节点
    gossip.SetStorage(n.stores)
    startGossiping()  // 异步，立即返回

    // 5. 启动新 Store（异步，非阻塞）
    if len(state.uninitializedEngines) > 0 {
        go func() {
            for engine := range state.uninitializedEngines {
                storeID := allocateStoreID()  // 需要 RPC
                store := NewStore(engine, storeID)
                store.Start()
                n.stores.Add(store)
            }
        }()
    }

    // 6. 启动后台任务
    startMetrics()
    startLivenessCompaction()

    return nil
}
```

### 7.4 与准入控制的联系

虽然 `Node.start()` 本身不直接涉及准入控制的**调整逻辑**，但它在以下方面影响准入控制：

1. **TenantWeightProvider 的注册**（L789）：
   - Store 启动后，Admission Control 才能获取租户权重
   - 影响 KVSlotAdjuster 的 slot 分配（第十五章）

2. **StoreGrantCoordinators 的激活**（在 `PreStart` 中创建，但依赖 Store）：
   - Store 启动后，`StoreGrantCoordinators` 才能开始监听 LSM 状态
   - 影响 IO 准入控制的 token 计算（第十七章）

3. **NodeLiveness 的回调注册**：
   - Store 启动后才能注册 `nodeIsLiveCallback`
   - 影响 Replica 的唤醒和 lease 转移（间接影响负载）

---

## 附录：关键代码位置索引

| 功能 | 文件 | 行号 |
|------|-----|-----|
| `Node.start()` 主函数 | `pkg/server/node.go` | L645-L818 |
| `Store.Start()` 主函数 | `pkg/kv/kvserver/store.go` | L2122-L2453 |
| Replica 加载循环 | `pkg/kv/kvserver/store.go` | L2300-L2365 |
| `startGossiping()` | `pkg/server/node.go` | L1046-L1088 |
| `initializeAdditionalStores()` | `pkg/server/node.go` | 未完整展示（需查看源码） |
| Gossip 持久化存储 | `pkg/gossip/storage.go` | 需查看源码 |
| `LoadAndReconcileReplicas()` | `pkg/kv/kvserver/kvstorage/replica_load.go` | 需查看源码 |

---

## 参考资料

- [CockroachDB 架构文档](https://www.cockroachlabs.com/docs/stable/architecture/overview.html)
- [Raft 协议详解](https://raft.github.io/)
- [Gossip 协议论文](https://www.cs.cornell.edu/home/rvr/papers/gossip.pdf)
- [CockroachDB 启动流程 RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/)（假设有相关 RFC）

---

**本章完**。下一章将分析 **Gossip 网络的内部机制**，深入解释节点发现、连接维护和元数据传播的实现细节。
