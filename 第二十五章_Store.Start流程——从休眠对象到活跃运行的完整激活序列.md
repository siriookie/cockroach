# 第二十五章 Store.Start 流程——从休眠对象到活跃运行的完整激活序列

## 引言：从构造到激活的关键转折点

在[第二十四章](第二十四章_kvserver.NewStore构造函数——Store对象的完整初始化与组件装配.md)中，我们详细分析了 `kvserver.NewStore()` 如何通过 10 个初始化阶段构造出一个**完整但休眠**的 Store 对象。这个对象虽然拥有完整的组件装配（Raft 调度器、9 个后台队列、Allocator 等），但**尚未启动任何后台任务，不接收任何消息，不管理任何 Replica**。

本章将聚焦于紧随其后的关键步骤——**`Store.Start(ctx, stopper)`**，这是 Store 生命周期中的**第二阶段**，负责将休眠对象转变为活跃的运行实体。通过分析其执行流程，我们将理解：
1. **如何从持久化存储中加载所有 Replica 元数据**（可能有数千个 Range）
2. **如何并发初始化 Replica 对象**并将其注册到 Store 的管理结构中
3. **如何启动 10+ 个后台协程**（Raft 消息处理、Scanner、Gossip、Rangefeed 更新等）
4. **如何建立与集群其他节点的通信通道**（Raft Transport 监听）
5. **为何在所有 Replica 加载完成前不能开始处理 Raft 消息**（初始化顺序的强约束）

---

## I. 鸟瞰：Store.Start() 是什么，为什么存在

### 1.1 核心职责

`Store.Start()` 是 Store 生命周期的**激活器**（Activator），其核心职责是：

**输入**：
- 一个由 `NewStore()` 创建的**休眠 Store 对象**（所有组件已装配但未启动）
- `context.Context`（用于日志和取消）
- `*stop.Stopper`（生命周期管理器，用于优雅关闭所有后台任务）

**输出**：
- 一个**完全激活的 Store 对象**，包括：
  - 所有持久化 Replica 已加载到内存并注册
  - 10+ 个后台协程正在运行
  - Raft Transport 开始监听来自其他节点的消息
  - Scanner、Gossip、Rangefeed 更新机制全部就绪
- 或返回 `error`（如果存储损坏、ID 不匹配等）

**副作用**：
- 从 Pebble 引擎读取所有 RangeDescriptor 元数据（可能需要数百毫秒到数秒，取决于 Replica 数量）
- 为每个 Replica 创建 `*Replica` 对象并插入 `Store.mu.replicas.Map`
- 启动 11 个后台协程（见 1.3 节）
- 向 Gossip 注册回调，订阅系统配置更新

### 1.2 为何需要单独的 Start() 方法？

**设计原因**：分离**构造**与**激活**的关注点

| 阶段 | 操作类型 | 耗时 | 失败后果 | 代表函数 |
|------|---------|------|---------|---------|
| **构造** | 纯内存操作（创建对象、分配 worker pool） | ~5ms | 可以重试或改配置 | `NewStore()` |
| **激活** | IO 密集型（读取 Pebble、启动协程、注册监听器） | 数百毫秒到数秒 | 可能需要修复存储或回滚 | `Start()` |

**反例**：如果在 `NewStore()` 中直接启动所有协程
- **问题 1**：单元测试无法创建 Store 对象（因为立即需要 stopper、Gossip 连接等外部依赖）
- **问题 2**：无法在所有 Store 对象创建完成后再并发启动它们（而 Node 启动时需要这样做以减少总耗时）
- **问题 3**：如果 Store A 启动失败，Store B 的协程可能已经开始与 Store A 通信，导致竞态条件

### 1.3 激活的 11 个后台协程（按启动顺序）

| 启动顺序 | 协程名称 | 触发时机 | 核心职责 | 代码位置 |
|---------|---------|---------|---------|---------|
| 1 | `rangefeedScheduler` | `Start()` 中立即启动 | 处理 Rangefeed 订阅请求（为 Changefeeds 提供实时更新） | L2193 |
| 2 | `storeLiveness` | `Start()` 中立即启动 | 心跳证明 Store 存活，维护 Store-to-Store 的 liveness 信息 | L2281 |
| 3 | `streamCloseScheduler` | `Start()` 中立即启动 | 管理 replication admission control v2 的流关闭调度 | L2303 |
| 4 | `syscfg-listener` | 如果有 `SystemConfigProvider` | 监听系统配置更新（如 Zone Config 变化），触发 Replica 重新配置 | L2421 |
| 5 | `store-gossip` | 如果有 Gossip 且为 first range 的 leaseholder | 周期性 gossip Store 容量、描述符等信息 | L2444 |
| 6 | `scanner` | Gossip 连接后启动 | 周期性扫描所有 Replica，将需要处理的 Range 加入各个队列 | L2452 |
| 7-10 | 4 个 Raft 处理协程 | `processRaft()` 中启动 | 处理入站/出站 Raft 消息、tick Raft 状态机 | L2473 |
| 11 | `rangefeedUpdater` | `processRaft()` 之后 | 订阅 closed timestamp 更新，推送给 Rangefeed 客户端 | L2476 |
| 12 | `storeRebalancer` | 如果启用 | 主动重平衡 Replica（基于 load-based splitting/rebalancing） | L2485 |

---

## II. 控制流：12 阶段激活序列

`Store.Start()` 的执行可以分解为 **12 个逻辑阶段**，每个阶段必须按顺序完成：

```
[阶段 1] 读取 Store 身份信息
         ↓
[阶段 2] 设置日志标签并通知引擎
         ↓
[阶段 3] 创建并启动 Rangefeed Scheduler
         ↓
[阶段 4] 验证节点 ID 一致性
         ↓
[阶段 5] 创建 RangeID 分配器
         ↓
[阶段 6] 创建 Intent Resolver
         ↓
[阶段 7] 创建 Raft Log Truncator、Recovery Manager、Store Liveness
         ↓
[阶段 8] 创建 KV Flow Range Controller Factory（RAC v2）
         ↓
[阶段 9] 加载并初始化所有 Replica（最重的操作，可能数秒）
         ↓
[阶段 10] 注册系统配置/Gossip 回调
         ↓
[阶段 11] 启动 Raft 消息处理（processRaft）
         ↓
[阶段 12] 启动 Rangefeed 更新器、Store Rebalancer
```

### 2.1 阶段 1：读取 Store 身份信息（L2165-2171）

**目的**：确认 Store 已被正确初始化（bootstrap 或加入集群时写入的元数据）

```go
ident, err := kvstorage.ReadStoreIdent(ctx, s.LogEngine())
if err != nil {
    return err  // 如果 Store 未被 bootstrap，这里会报错
}
s.Ident = &ident  // StoreIdent { StoreID, NodeID, ClusterID }
```

**输入**：Pebble 引擎的 `LogEngine`（存储 Raft log 的引擎）
**输出**：`roachpb.StoreIdent` 结构体，包含：
```go
type StoreIdent struct {
    ClusterID uuid.UUID  // 集群唯一 ID（防止混接不同集群的节点）
    NodeID    roachpb.NodeID  // 该 Store 所属节点的 ID
    StoreID   roachpb.StoreID // 该 Store 的 ID（全局唯一）
}
```

**关键不变式**：
- `ident.NodeID` 必须与 `s.nodeDesc.NodeID` 一致（除非是 bootstrap 阶段 NodeID 尚未分配为 0）
- `ident.StoreID` 在整个集群中必须唯一
- 如果读取失败，说明 Store 未被 bootstrap 或存储损坏

### 2.2 阶段 2：设置日志标签并通知引擎（L2173-2182）

**目的**：让所有后续日志都带上 `[s<StoreID>]` 前缀，方便在多 Store 环境下追踪

```go
s.cfg.AmbientCtx.AddLogTag("s", s.StoreID())  // 添加 [s1], [s2] 等标签
ctx = s.AnnotateCtx(ctx)
log.Event(ctx, "read store identity")

// 通知 Pebble 引擎当前的 Store ID（用于内部日志）
if err := s.TODOEngine().SetStoreID(ctx, int32(s.StoreID())); err != nil {
    return err
}
```

**效果**：之后所有日志行都会显示类似：
```
I250114 10:23:45.123456 1 server/node.go:727 ⋮ [n1,s1] initialized store s1 in 1.2s (10523 replicas)
```

### 2.3 阶段 3：创建并启动 Rangefeed Scheduler（L2184-2197）

**目的**：为 Changefeeds、Logical Data Replication (LDR) 等功能提供实时数据流

```go
m := rangefeed.NewSchedulerMetrics(s.cfg.HistogramWindowInterval)
rfs := rangefeed.NewScheduler(rangefeed.SchedulerConfig{
    Workers:         s.cfg.RangeFeedSchedulerConcurrency,  // 默认 8 * CPU
    PriorityWorkers: s.cfg.RangeFeedSchedulerConcurrencyPriority,
    ShardSize:       s.cfg.RangeFeedSchedulerShardSize,  // 默认 256
    Metrics:         m,
})
s.Registry().AddMetricStruct(m)
if err = rfs.Start(ctx, s.stopper); err != nil {
    return err
}
s.rangefeedScheduler = rfs
```

**架构**：与 Raft scheduler 类似的**分片 worker pool**
- **Workers**：处理普通优先级的 rangefeed 请求
- **PriorityWorkers**：处理高优先级请求（如系统表的 changefeeds）
- **ShardSize**：每 256 个 Range 共享一个分片，减少锁竞争

**并发度**（48 核机器）：
- 普通 workers = 8 × 48 = 384
- 优先级 workers = 通常设为较小值（如 32）

### 2.4 阶段 4：验证节点 ID 一致性（L2214-2221）

**目的**：防止使用错误节点的 Store 目录

```go
if s.nodeDesc.NodeID != 0 && s.Ident.NodeID != s.nodeDesc.NodeID {
    return errors.Errorf(
        "node id:%d does not equal the one in node descriptor:%d",
        s.Ident.NodeID, s.nodeDesc.NodeID,
    )
}
// 始终在 gossip 任何信息前设置 Gossip.NodeID
if s.cfg.Gossip != nil {
    s.cfg.Gossip.NodeID.Set(ctx, s.Ident.NodeID)
}
```

**失败场景**：
- 管理员错误地将节点 A 的数据目录挂载到节点 B
- 容器编排系统错误地重用了旧节点的持久化卷

### 2.5 阶段 5：创建 RangeID 分配器（L2223-2233）

**目的**：为新创建的 Range 分配全局唯一的 RangeID

```go
idAlloc, err := idalloc.NewAllocator(idalloc.Options{
    AmbientCtx:  s.cfg.AmbientCtx,
    Key:         keys.RangeIDGenerator,  // 存储在元数据键 "/System/RangeID" 中
    Incrementer: idalloc.DBIncrementer(s.db),  // 通过 KV 事务递增计数器
    BlockSize:   rangeIDAllocCount,  // 每次预分配 10 个 ID（减少 KV 事务）
    Stopper:     s.stopper,
})
s.rangeIDAlloc = idAlloc
```

**工作原理**：
1. **预分配机制**：一次性从 KV 存储获取 10 个 ID（如 100-109）
2. **本地分配**：从预分配的 ID 池中分配，无需 KV 事务
3. **池耗尽时**：再次执行 KV 事务获取下一批 ID（如 110-119）

**为何需要此分配器**：
- Range 分裂（split）时需要为新 Range 分配 ID
- 必须保证全局唯一性（即使多个 Store 并发分裂）

### 2.6 阶段 6：创建 Intent Resolver（L2235-2252）

**目的**：异步解决未提交的 MVCC intents（事务写操作留下的"占位符"）

```go
s.intentResolver = intentresolver.New(intentresolver.Config{
    Clock:                s.cfg.Clock,
    DB:                   s.db,
    Stopper:              stopper,
    Settings:             s.cfg.Settings,
    TaskLimit:            s.cfg.IntentResolverTaskLimit,  // 并发任务数限制
    AmbientCtx:           s.cfg.AmbientCtx,
    TestingKnobs:         s.cfg.TestingKnobs.IntentResolverKnobs,
    RangeDescriptorCache: intentResolverRangeCache,
})
s.metrics.registry.AddMetricStruct(s.intentResolver.Metrics)
```

**Intent 是什么**：
当事务 T1 写入 `key=foo, value=bar, txn_id=abc123` 时，MVCC 引擎存储：
```
foo@1000.0 → intent{txn_id: abc123, value: bar}
```

**Intent Resolver 的职责**：
1. **提交 intent**：当 T1 提交时，将 intent 转换为正式版本 `foo@1000.0 → bar`
2. **清理 intent**：当 T1 回滚时，删除 intent
3. **异步处理**：不阻塞读写请求（读请求遇到 intent 时会触发异步解析）

**TaskLimit 的作用**：
- 默认值：1000（防止 intent 解析风暴消耗过多 CPU）
- 如果有 10,000 个 intent 需要解析，只有 1000 个会并发处理

### 2.7 阶段 7：创建 Raft Log Truncator、Recovery Manager、Store Liveness（L2254-2283）

#### 7.1 Raft Log Truncator（L2254-2262）

**目的**：在 Raft Applied Index 推进后截断旧的 Raft log 条目（防止无限增长）

```go
s.raftTruncator = makeRaftLogTruncator(s.cfg.AmbientCtx, (*storeForTruncatorImpl)(s), stopper)
{
    truncator := s.raftTruncator
    // 当状态机持久化新的 RaftAppliedIndex 时触发回调
    s.StateEngine().RegisterFlushCompletedCallback(func() {
        truncator.durabilityAdvancedCallback()
    })
}
```

**工作原理**：
1. **触发条件**：当 `RaftAppliedIndex` 从 1000 推进到 1100，且已刷盘
2. **截断动作**：删除 index ≤ 900 的 Raft log 条目（保留最近 200 条）
3. **为何需要回调**：必须等 Applied Index 刷盘后才能删除 log（否则崩溃恢复时丢失数据）

#### 7.2 Transaction Recovery Manager（L2264-2268）

**目的**：恢复卡住的分布式事务（如协调者节点崩溃）

```go
s.recoveryMgr = txnrecovery.NewManager(
    s.cfg.AmbientCtx, s.cfg.Clock, s.db, stopper,
)
s.metrics.registry.AddMetricStruct(s.recoveryMgr.Metrics())
```

**处理场景**：
- 事务 T1 的协调者节点崩溃，留下未解决的 intents
- Recovery Manager 周期性扫描，检测到 T1 的 heartbeat 超时
- 主动回滚 T1 并清理 intents

#### 7.3 Store Liveness（L2270-2283）

**目的**：维护 Store 级别的 liveness 信息（补充 Node Liveness）

```go
sm := storeliveness.NewSupportManager(
    slpb.StoreIdent{NodeID: s.nodeDesc.NodeID, StoreID: s.StoreID()},
    s.LogEngine(),
    s.cfg.StoreLiveness.Options,
    s.cfg.Settings,
    s.stopper,
    s.cfg.Clock,
    s.cfg.StoreLiveness.HeartbeatTicker,
    s.cfg.StoreLiveness.Transport,
    s.cfg.StoreLiveness.SupportManagerKnobs(),
)
s.cfg.StoreLiveness.Transport.ListenMessages(s.StoreID(), sm)
s.storeLiveness = sm
s.storeLiveness.RegisterSupportWithdrawalCallback(s.supportWithdrawnCallback)
s.metrics.registry.AddMetricStruct(sm.Metrics())
if err = sm.Start(ctx); err != nil {
    return errors.Wrap(err, "starting store liveness")
}
```

**为何需要 Store Liveness**（在已有 Node Liveness 的情况下）：
- **粒度更细**：一个节点可能有多个 Store，某个 Store 的磁盘损坏不应影响其他 Store
- **Raft 协议需求**：Raft 的 lease 转移、leader 选举需要知道对端 Store 是否健康
- **快速检测**：heartbeat 间隔更短（秒级），而 Node Liveness 通常为 9 秒

### 2.8 阶段 8：创建 KV Flow Range Controller Factory（L2287-2322）

**目的**：为 Replication Admission Control v2（RAC v2）提供流控制

```go
scs := replica_rac2.NewStreamCloseScheduler(timeutil.DefaultTimeSource{}, s.scheduler)
if err := scs.Start(ctx, stopper); err != nil {
    return err
}
s.kvflowRangeControllerFactory = replica_rac2.NewRangeControllerFactoryImpl(
    s.Clock(),
    s.cfg.KVFlowEvalWaitMetrics,
    s.cfg.KVFlowRangeControllerMetrics,
    s.cfg.KVFlowStreamTokenProvider,
    scs,
    (*racV2Scheduler)(s.scheduler),  // 复用 Raft scheduler 的 worker pool
    s.cfg.KVFlowSendTokenWatcher,
    s.cfg.KVFlowWaitForEvalConfig,
    s.cfg.RaftMaxInflightBytes,
    s.TestingKnobs().FlowControlTestingKnobs,
)
```

**RAC v2 是什么**：
- **问题**：高吞吐写入可能导致 follower 副本积压大量未应用的 Raft log
- **解决方案**：leader 根据 follower 的处理速度动态调整发送速率
- **实现**：基于 token bucket 的流控制（类似 TCP 的拥塞控制）

**为何在此阶段创建**：
- Replica 初始化时需要此 factory 创建 RangeController
- 必须在阶段 9（加载 Replica）之前完成

### 2.9 阶段 9：加载并初始化所有 Replica（L2324-2407）

**这是 Start() 中最重的操作**，可能耗时数秒（对于有 10,000 个 Replica 的 Store）

#### 9.1 从 Pebble 加载 Replica 元数据（L2342-2345）

```go
repls, err := kvstorage.LoadAndReconcileReplicas(ctx, s.TODOEngine())
if err != nil {
    return err
}
```

**输入**：Pebble 引擎实例
**输出**：`[]LoadedReplicaDescriptor`（包含每个 Replica 的描述符和 Raft 状态）

**读取的键**：
- `RangeDescriptorKey`：Range 的元数据（start/end key、副本列表、lease holder 等）
- `RaftHardStateKey`：Raft 的持久化状态（term、vote、commit index）
- `RaftAppliedIndexKey`：已应用到状态机的最高 log index

**耗时估算**（48 核机器，10,000 个 Replica）：
- 假设 Pebble 顺序读取性能：50,000 keys/sec
- 需要读取 10,000 × 3 = 30,000 个键
- 耗时：30,000 / 50,000 = **0.6 秒**

#### 9.2 并发初始化 Replica 对象（L2347-2406）

```go
logEvery := log.Every(10 * time.Second)
for i, repl := range repls {
    // 每 10 秒记录一次进度（如果加载很慢）
    if logEvery.ShouldLog() && i > 0 {
        log.KvExec.Infof(ctx, "initialized %d/%d replicas", i, len(repls))
    }

    if repl.Desc == nil {
        // 未初始化的 Replica（仅有 Raft state，无 Range descriptor）
        continue
    }

    // 从 Pebble 加载完整状态（MVCC stats、lease、GC threshold 等）
    state, err := repl.Load(ctx, s.TODOEngine(), s.StoreID())
    if err != nil {
        return err
    }

    // 创建 Replica 对象
    rep, err := newInitializedReplica(s, state, true /* waitForPrevLeaseToExpire */)
    if err != nil {
        return err
    }

    // 注册到 Store 的两个索引：RangeID 和 KeyRange
    s.mu.Lock()
    err = s.addToReplicasByRangeIDLocked(rep)
    if err == nil {
        err = s.addToReplicasByKeyLocked(rep, rep.Desc())
    }
    s.mu.Unlock()
    if err != nil {
        return err
    }

    // 更新指标
    s.metrics.ReplicaCount.Inc(1)
    if _, ok := rep.TenantID(); ok {
        s.metrics.addMVCCStats(ctx, rep.tenantMetricsRef, rep.GetMVCCStats())
    } else {
        return errors.AssertionFailedf("no tenantID for initialized replica %s", rep)
    }

    // 对于使用不支持 quiescence 的 lease 的 Replica，立即 unquiesce
    // （这会触发 Raft 选举和 lease 获取）
    if l, _ := rep.GetLease(); !l.SupportsQuiescence() && l.Sequence > 0 {
        rep.maybeUnquiesce(ctx, true /* wakeLeader */, true /* mayCampaign */)
    }
}
log.KvExec.Infof(ctx, "initialized %d/%d replicas", len(repls), len(repls))
```

**关键操作**：
1. **`repl.Load()`**：从 Pebble 读取约 20 个键（MVCC stats、GC threshold、truncated state 等）
2. **`newInitializedReplica()`**：创建 `*Replica` 对象（包含 Raft 状态机、proposal buffer 等）
3. **`addToReplicasByRangeIDLocked()`**：插入 `Store.mu.replicas.replicas` map（按 RangeID 索引）
4. **`addToReplicasByKeyLocked()`**：插入 `Store.mu.replicasByKey` btree（按 Range 的 start key 索引）

**并发度**：**串行执行**（受 `s.mu` 锁保护）
- **为何不并发**：需要在所有 Replica 加载完成后才能开始处理 Raft 消息（否则可能路由到不存在的 Replica）
- **未来优化**：可以并发创建 Replica 对象，最后串行注册到 Store

**耗时估算**（10,000 个 Replica）：
- 每个 Replica 加载：20 个键读取 + 对象创建 ≈ 0.5 ms
- 总耗时：10,000 × 0.5 ms = **5 秒**

**为何对某些 Replica 调用 `maybeUnquiesce()`**：
- **Quiescence（静默）**：如果 Range 没有活动，Raft 会停止发送 heartbeat（节省 CPU 和网络）
- **Leader lease**：旧版本使用的 lease 类型，不支持 quiescence
- **立即 unquiesce**：强制开始 Raft tick，尽快选出 leader 并获取 epoch lease（支持 quiescence）

### 2.10 阶段 10：注册系统配置和 Gossip 回调（L2409-2444）

#### 10.1 注册 Node Liveness 回调（L2411-2416）

**目的**：当某个节点从 dead → live 时，unquiesce 其上的所有 Replica

```go
if s.cfg.NodeLiveness != nil {
    s.cfg.NodeLiveness.RegisterCallback(s.nodeIsLiveCallback)
    // 注册 setting 变化回调（某些设置变化也需要触发此逻辑）
    s.registerNodeIsLiveCallbackSettingsChange(ctx)
}
```

**工作原理**：
1. 节点 N2 从网络分区中恢复，Node Liveness 将其标记为 live
2. `nodeIsLiveCallback()` 被调用
3. Store 遍历所有有 N2 副本的 Range，unquiesce 它们
4. Raft 开始发送消息，快速恢复副本同步

#### 10.2 注册系统配置监听器（L2418-2432）

**目的**：监听 Zone Config 变化（如副本数调整、约束变化）

```go
if scp := s.cfg.SystemConfigProvider; scp != nil {
    systemCfgUpdateC, _ := scp.RegisterSystemConfigChannel()
    _ = s.stopper.RunAsyncTask(ctx, "syscfg-listener", func(context.Context) {
        for {
            select {
            case <-systemCfgUpdateC:
                cfg := scp.GetSystemConfig()
                s.systemGossipUpdate(cfg)  // 触发 Replica 重新评估约束
            case <-s.stopper.ShouldQuiesce():
                return
            }
        }
    })
}
```

**处理场景**：
- DBA 执行 `ALTER TABLE foo CONFIGURE ZONE num_replicas = 5`
- 系统配置更新通过 Gossip 传播
- `systemGossipUpdate()` 触发 Replicate Queue 重新扫描，添加新副本

#### 10.3 启动 Store Gossip（L2436-2444）

**目的**：周期性向集群广播 Store 的容量、描述符等信息

```go
if s.cfg.Gossip != nil {
    s.storeGossip.stopper = stopper
    s.storeGossip.Ident = *s.Ident
    s.startGossip()  // 启动后台协程
}
```

**Gossip 的信息**（每 10 秒一次）：
- `StoreDescriptor`：容量（总空间、已用空间）、范围数、写入 QPS 等
- `FirstRangeDescriptor`：如果此 Store 持有 Range 1 的副本
- Sentinel（哨兵值）：证明节点存活

### 2.11 阶段 11：启动 Raft 消息处理（L2446-2473）

#### 11.1 启动 Scanner（L2446-2457）

**注意**：Scanner 的启动**等待 Gossip 连接**后才开始

```go
_ = s.stopper.RunAsyncTask(ctx, "scanner", func(context.Context) {
    select {
    case <-s.cfg.Gossip.Connected:  // 阻塞直到 Gossip 连接成功
        s.scanner.Start()
    case <-s.stopper.ShouldQuiesce():
        return
    }
})
```

**为何需要等待 Gossip**：
- Scanner 会将 Range 加入 Replicate Queue
- Replicate Queue 需要从 Gossip 获取其他节点的 Store 容量信息（选择目标副本）
- 如果 Gossip 未连接，Replicate Queue 会失败并不断重试

#### 11.2 订阅 Span Config 更新（L2459-2468）

```go
if !s.cfg.SpanConfigsDisabled {
    s.cfg.SpanConfigSubscriber.Subscribe(func(ctx context.Context, update roachpb.Span) {
        s.onSpanConfigUpdate(ctx, update)
    })

    spanconfigstore.FallbackConfigOverride.SetOnChange(&s.ClusterSettings().SV, func(ctx context.Context) {
        s.applyAllFromSpanConfigStore(ctx)
    })
}
```

**Span Config 是什么**：
- 替代旧版 Zone Config 的新架构（更细粒度，支持表的某个分区）
- 每当 Span Config 更新，触发回调重新评估 Replica 放置

#### 11.3 启动 Raft 消息处理（L2470-2473）

**这是最关键的步骤**：开始监听来自其他节点的 Raft 消息

```go
s.cfg.Transport.ListenIncomingRaftMessages(s.StoreID(), s)  // 注册为消息处理器
s.cfg.Transport.ListenOutgoingMessage(s.StoreID(), s)       // 注册为出站消息发送器
s.processRaft(ctx)  // 启动 4 个后台协程处理 Raft
```

**`processRaft()` 启动的协程**：
1. **`raftTickLoop`**：周期性 tick Raft 状态机（触发 election timeout、heartbeat）
2. **`raftSchedulerLoop`**：从 Raft scheduler 获取待处理的 Replica，调用 `Replica.handleRaftReady()`
3. **`coalescedHeartbeatsLoop`**：合并 heartbeat 消息，批量发送（减少网络往返）
4. **`raftstoragequeues.Process`**：处理 Raft snapshot 接收队列

**为何在所有 Replica 加载完成后才启动**：
- 如果 Raft 消息到达时 Replica 尚未加载，会触发 "unknown replica" 错误
- 重试逻辑会导致不必要的网络开销和日志噪声

### 2.12 阶段 12：启动 Rangefeed 更新器和 Store Rebalancer（L2475-2492）

#### 12.1 启动 Rangefeed 更新器（L2476-2480）

```go
s.startRangefeedUpdater(ctx)

if err := s.startRangefeedTxnPushNotifier(ctx); err != nil {
    return err
}
```

**职责**：
- **Rangefeed Updater**：订阅 closed timestamp 更新，推送给 Changefeed 客户端
- **Txn Push Notifier**：当事务被 push（优先级调整）时，通知 Rangefeed

#### 12.2 启动 Store Rebalancer（L2482-2489）

```go
if s.replicateQueue != nil {
    s.storeRebalancer = NewStoreRebalancer(
        s.cfg.AmbientCtx, s.cfg.Settings, s.replicateQueue, s.replRankings, s.rebalanceObjManager)
    s.storeRebalancer.Start(ctx, s.stopper)
}

s.cfg.MMAllocator.InitMetricsForLocalStore(ctx, s.StoreID(), s.Registry())
s.mmaStoreRebalancer.start(ctx, s.stopper)
```

**Store Rebalancer 的作用**：
- **主动重平衡**：即使没有 Zone Config 变化，也会基于负载（QPS、CPU）移动副本
- **Load-based splitting**：将高负载 Range 分裂，减少热点

#### 12.3 设置 Started 标志（L2491-2492）

```go
atomic.StoreInt32(&s.started, 1)
```

**用途**：
- 单元测试中检查 Store 是否完全启动
- 某些操作（如 Replica GC）需要等待 Store 启动完成

---

## III. 深度剖析：核心函数的输入、输出与不变式

### 3.1 LoadAndReconcileReplicas（读取所有 Replica 元数据）

**位置**：`pkg/kv/kvserver/kvstorage/load_replicas.go`

**函数签名**：
```go
func LoadAndReconcileReplicas(
    ctx context.Context,
    reader storage.Reader,
) ([]LoadedReplicaDescriptor, error)
```

**输入**：
- `reader`：Pebble 引擎的只读接口

**输出**：
- `[]LoadedReplicaDescriptor`：每个 Replica 的元数据
  ```go
  type LoadedReplicaDescriptor struct {
      RangeID roachpb.RangeID
      Desc    *roachpb.RangeDescriptor  // 可能为 nil（未初始化的 Replica）
      HardState raftpb.HardState
      // ... 其他 Raft 状态
  }
  ```

**算法流程**：
1. **扫描所有 RangeID**：
   - 从 `LocalRangeIDPrefix` 开始扫描键空间
   - 每个 RangeID 对应的 `RangeDescriptorKey` 包含 Range 的元数据
2. **读取每个 RangeID 的状态**：
   - `RaftHardStateKey`：Raft 的 term、vote、commit
   - `RaftAppliedIndexKey`：已应用的最高 log index
   - `RangeDescriptorKey`：Range 的副本列表、lease holder 等
3. **协调不一致状态**（如崩溃恢复后）：
   - 如果 `RaftAppliedIndex > HardState.Commit`，使用 applied index（更安全）
   - 如果 RangeDescriptor 缺失但有 Raft state，标记为未初始化 Replica

**复杂度**：
- **时间**：O(N)，其中 N 是 Replica 数量
- **空间**：O(N)（返回的切片大小）

**不变式**：
- **每个 RangeID 最多对应一个 RangeDescriptor**（如果有多个，说明存储损坏）
- **RaftAppliedIndex ≤ HardState.Commit + raft_log_size**（否则日志丢失）

### 3.2 newInitializedReplica（创建 Replica 对象）

**位置**：`pkg/kv/kvserver/replica_init.go`

**函数签名**：
```go
func newInitializedReplica(
    s *Store,
    state storagepb.ReplicaState,
    waitForPrevLeaseToExpire bool,
) (*Replica, error)
```

**输入**：
- `state`：从 Pebble 加载的完整状态
  ```go
  type ReplicaState struct {
      Desc              *roachpb.RangeDescriptor
      Stats             enginepb.MVCCStats
      Lease             *roachpb.Lease
      GCThreshold       hlc.Timestamp
      TruncatedState    *roachpb.RaftTruncatedState
      // ... 等 20+ 个字段
  }
  ```
- `waitForPrevLeaseToExpire`：如果为 true，等待旧 lease 过期后才能获取新 lease（防止脑裂）

**输出**：
- `*Replica`：完全初始化的 Replica 对象，包含：
  - Raft 状态机（`Replica.mu.internalRaftGroup`）
  - Proposal buffer（暂存待提交的写请求）
  - Timestamp cache（防止时间戳倒退）
  - Lease 状态
  - MVCC stats

**关键操作**：
1. **创建 Raft 状态机**：
   ```go
   rn := newRaftNode(
       raftNodeConfig{
           raftConfig: &raft.Config{
               ID:              uint64(replicaID),
               ElectionTick:    s.cfg.RaftElectionTimeoutTicks,
               HeartbeatTick:   s.cfg.RaftHeartbeatIntervalTicks,
               MaxSizePerMsg:   s.cfg.RaftMaxSizePerMsg,
               // ...
           },
       },
   )
   ```
2. **加载 Raft log**：从 `TruncatedState.Index + 1` 开始读取未应用的 log 条目
3. **初始化 proposal buffer**：
   ```go
   rep.mu.proposalBuf = newProposalBuffer(rep, s.scheduler)
   ```
4. **设置 lease**：
   ```go
   rep.mu.state.Lease = state.Lease
   if waitForPrevLeaseToExpire {
       rep.mu.minLeaseProposedTS = state.Lease.Start
   }
   ```

**内存分配**（单个 Replica）：
- Replica 结构体：~8 KB
- Raft 状态机：~4 KB
- Proposal buffer：~2 KB
- Timestamp cache：懒加载（首次读取时分配）
- **总计**：~14 KB/Replica

**对于 10,000 个 Replica**：
- 总内存：10,000 × 14 KB = **140 MB**

### 3.3 processRaft（启动 Raft 处理协程）

**位置**：`pkg/kv/kvserver/store_raft.go`

**函数签名**：
```go
func (s *Store) processRaft(ctx context.Context)
```

**启动的协程**：

#### 3.3.1 raftTickLoop

```go
_ = s.stopper.RunAsyncTask(ctx, "raft-tick", func(ctx context.Context) {
    ticker := time.NewTicker(s.cfg.RaftTickInterval)  // 默认 100ms
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            s.enqueueRaftTicks(ctx)  // 将所有 Replica 加入 Raft scheduler
        case <-s.stopper.ShouldQuiesce():
            return
        }
    }
})
```

**职责**：每 100ms tick 所有 Replica 的 Raft 状态机
- **触发 election timeout**：如果 follower 长时间未收到 heartbeat，发起选举
- **触发 heartbeat**：leader 周期性发送 heartbeat 给 followers
- **检查 quiescence**：如果 Range 无活动，进入静默状态

#### 3.3.2 raftSchedulerLoop

```go
for i := 0; i < s.cfg.RaftSchedulerConcurrency; i++ {
    _ = s.stopper.RunAsyncTask(ctx, "raft-worker", func(ctx context.Context) {
        for {
            task, ok := s.scheduler.Dequeue()
            if !ok {
                return  // scheduler 已关闭
            }

            repl := task.(*Replica)
            repl.handleRaftReady(ctx)  // 处理 Raft Ready
        }
    })
}
```

**职责**：并发处理 Raft Ready（状态机输出）
- **读取 Ready**：获取待持久化的 log 条目、待发送的消息等
- **写入 Pebble**：批量写入 log 条目、更新 HardState
- **应用到状态机**：执行写操作（如 Put、Delete）
- **发送消息**：通过 Raft Transport 发送给其他节点

**并发度**（48 核机器）：
- Worker 数 = 8 × 48 = 384
- 可同时处理 384 个 Replica 的 Raft Ready

#### 3.3.3 coalescedHeartbeatsLoop

```go
_ = s.stopper.RunAsyncTask(ctx, "coalesced-heartbeats", func(ctx context.Context) {
    ticker := time.NewTicker(s.cfg.CoalescedHeartbeatsInterval)  // 默认 1 秒
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            s.sendCoalescedHeartbeats(ctx)
        case <-s.stopper.ShouldQuiesce():
            return
        }
    }
})
```

**优化原理**：
- **问题**：如果 Store A 向 Store B 有 1000 个 Range 的副本，每个发送单独的 heartbeat 消息会导致 1000 次 RPC
- **解决方案**：合并为单个 `MsgHeartbeatBatch`，包含 1000 个 heartbeat
- **效果**：网络开销从 1000 × 100 bytes = 100 KB 减少到 ~10 KB（压缩后）

---

## IV. 运行时行为：动态信号与负载管理

### 4.1 Replica 加载的进度监控

**问题**：如果 Store 有 50,000 个 Replica，加载可能需要 20+ 秒，用户需要知道进度。

**解决方案**：每 10 秒记录一次进度日志

```go
logEvery := log.Every(10 * time.Second)
for i, repl := range repls {
    if logEvery.ShouldLog() && i > 0 {
        log.KvExec.Infof(ctx, "initialized %d/%d replicas", i, len(repls))
    }
    // ...
}
```

**日志示例**（50,000 个 Replica）：
```
I250114 10:23:45.123 [n1,s1] initialized 0/50000 replicas
I250114 10:23:55.456 [n1,s1] initialized 9521/50000 replicas
I250114 10:24:05.789 [n1,s1] initialized 19042/50000 replicas
I250114 10:24:15.012 [n1,s1] initialized 28563/50000 replicas
I250114 10:24:25.345 [n1,s1] initialized 38084/50000 replicas
I250114 10:24:35.678 [n1,s1] initialized 47605/50000 replicas
I250114 10:24:45.901 [n1,s1] initialized 50000/50000 replicas
```

### 4.2 Scanner 的延迟启动策略

**问题**：Scanner 需要访问 Gossip 中的 Store 容量信息，但 Gossip 连接需要时间。

**解决方案**：在异步任务中等待 Gossip 连接

```go
_ = s.stopper.RunAsyncTask(ctx, "scanner", func(context.Context) {
    select {
    case <-s.cfg.Gossip.Connected:  // 阻塞直到连接成功
        s.scanner.Start()
    case <-s.stopper.ShouldQuiesce():
        return
    }
})
```

**时间线**：
```
T0:      Store.Start() 开始
T0+5s:   所有 Replica 加载完成
T0+6s:   Gossip 连接到至少一个节点
T0+6s:   Scanner 启动，开始扫描 Replica
```

### 4.3 Replica Unquiesce 的触发条件

**静默机制**：如果 Range 无活动（无写入、无 lease transfer），Raft 停止发送 heartbeat

**何时需要 Unquiesce**：
1. **接收到写请求**：客户端发起 Put/Delete
2. **Leader 变更**：Raft 选举完成
3. **Lease 转移**：手动或自动触发 lease transfer
4. **节点恢复**：节点从 dead → live

**Start() 中的 Unquiesce**（L2403-2405）：
```go
if l, _ := rep.GetLease(); !l.SupportsQuiescence() && l.Sequence > 0 {
    rep.maybeUnquiesce(ctx, true /* wakeLeader */, true /* mayCampaign */)
}
```

**触发条件**：
- Lease 类型为 `LeaderLease` 或 `ExpirationLease`（旧版本）
- Lease Sequence > 0（已经至少获取过一次 lease）

**目的**：强制升级到 `EpochLease`（支持 quiescence，性能更好）

### 4.4 Store Rebalancer 的启动时机

**为何在 Start() 的最后启动**：
- Store Rebalancer 需要访问 `replicateQueue`（在 NewStore 中创建）
- 需要从 Gossip 获取其他 Store 的负载信息
- 需要等待所有 Replica 加载完成（否则负载指标不准确）

**启动后的行为**：
- 每 10 秒评估一次是否需要重平衡
- 如果本 Store 的 QPS > 1.05 × 集群平均 QPS，触发 rebalance
- 选择 QPS 最高的 Range，移动副本到负载较低的 Store

---

## V. 具体示例：48 核机器启动 10,000 个 Replica 的时间线

**硬件配置**：
- CPU：48 核 @ 2.5 GHz
- 内存：256 GB
- 磁盘：NVMe SSD（顺序读 3 GB/s，随机读 500,000 IOPS）

**假设**：
- Store 有 10,000 个 Replica
- 平均每个 Range 100 MB（总计 1 TB 数据）
- Pebble LSM 有 5 层（L0-L4）

### 5.1 时间线分解

| 时间点 | 阶段 | 操作 | 耗时 | 累计耗时 | 详情 |
|-------|------|------|------|---------|------|
| **T0** | 1 | 读取 StoreIdent | 0.1 ms | 0.1 ms | 单次 Pebble Get |
| **T0+0.1ms** | 2 | 设置日志标签 | 0.05 ms | 0.15 ms | 内存操作 |
| **T0+0.15ms** | 3 | 创建并启动 Rangefeed Scheduler | 2 ms | 2.15 ms | 创建 384 个 worker goroutines |
| **T0+2.15ms** | 4 | 验证节点 ID | 0.01 ms | 2.16 ms | 简单比较 |
| **T0+2.16ms** | 5 | 创建 RangeID 分配器 | 50 ms | 52.16 ms | 需要 KV 事务预分配 ID 块 |
| **T0+52ms** | 6 | 创建 Intent Resolver | 0.5 ms | 52.66 ms | 内存操作 |
| **T0+52ms** | 7 | 创建 Truncator/RecoveryMgr/StoreLiveness | 150 ms | 202.66 ms | StoreLiveness.Start() 需要首次 heartbeat |
| **T0+202ms** | 8 | 创建 RAC v2 Factory | 1 ms | 203.66 ms | 内存操作 |
| **T0+203ms** | 9 | **加载 10,000 个 Replica** | **5000 ms** | **5203.66 ms** | 最重的操作，见 5.2 节 |
| **T0+5.2s** | 10 | 注册回调（Gossip、SysConfig） | 10 ms | 5213.66 ms | 启动 2 个异步任务 |
| **T0+5.2s** | 11 | 启动 Raft 处理 | 50 ms | 5263.66 ms | 启动 384 个 worker + tick loop |
| **T0+5.3s** | 12 | 启动 Rebalancer | 5 ms | 5268.66 ms | 内存操作 |
| **T0+5.3s** | - | **Start() 返回** | - | **~5.3 秒** | - |
| **T0+6s** | - | Gossip 连接成功 | - | - | Scanner 开始运行 |

### 5.2 阶段 9 详细分解（加载 10,000 个 Replica）

| 子阶段 | 操作 | 耗时/Replica | 总耗时 | 详情 |
|-------|------|------------|--------|------|
| **9.1** | `LoadAndReconcileReplicas()` | - | 600 ms | 读取 30,000 个键（3 keys/replica） |
| **9.2** | 并发循环开始 | - | - | - |
| **9.2.1** | `repl.Load()` | 0.3 ms | 3000 ms | 每个 Replica 读取 20 个键 |
| **9.2.2** | `newInitializedReplica()` | 0.15 ms | 1500 ms | 创建 Raft 状态机、proposal buffer |
| **9.2.3** | `addToReplicas*Locked()` | 0.05 ms | 500 ms | 插入 map 和 btree |
| **9.2.4** | `maybeUnquiesce()` | 0.01 ms | 100 ms | 约 10% 的 Replica 需要 unquiesce |
| **总计** | - | 0.5 ms | **5700 ms** | 包括 9.1 的 600 ms |

**为何实际测量可能更快**：
- **并发性**：虽然注册到 Store 的操作串行，但 Pebble 的 bloom filter 缓存会加速重复键的读取
- **CPU 缓存**：处理 10,000 个 Replica 时，代码和数据结构在 L1/L2 缓存中
- **实际测量**：通常为 3-4 秒（而非理论的 5.7 秒）

### 5.3 内存分配时间线

| 时间点 | 新增内存分配 | 累计内存 | 详情 |
|-------|------------|---------|------|
| **T0** | 0 | 35 KB | NewStore() 已分配的基础结构 |
| **T0+2ms** | 3 MB | 3 MB | Rangefeed Scheduler 的 384 个 workers |
| **T0+203ms** | 5 MB | 8 MB | Raft Scheduler、Intent Resolver 的内存池 |
| **T0+5.2s** | 140 MB | 148 MB | 10,000 个 Replica × 14 KB/Replica |
| **T0+5.3s** | 10 MB | 158 MB | Raft worker pool、proposal buffers |

### 5.4 磁盘 IO 时间线

| 时间点 | IO 类型 | 数据量 | IOPS | 详情 |
|-------|---------|-------|------|------|
| **T0+52ms** | 随机读 | 1 KB | 1 | 读取 RangeID 分配器的元数据 |
| **T0+203ms** | 顺序读 | 300 KB | 30,000 | LoadAndReconcileReplicas() 读取 30,000 个键 |
| **T0+5.2s** | 随机读 | 20 MB | 200,000 | 每个 Replica 的 Load() 读取 20 个键 |

**总 IO**：
- 读取键数：30,000 + 10,000 × 20 = **230,000 个键**
- 数据量：~20 MB（大部分是小键值对）
- 平均 IOPS：230,000 / 5.2s = **44,000 IOPS**（NVMe 可轻松满足）

### 5.5 并发协程时间线

| 时间点 | 新增协程 | 累计协程 | 详情 |
|-------|---------|---------|------|
| **T0+2ms** | 384 | 384 | Rangefeed Scheduler workers |
| **T0+150ms** | 10 | 394 | Store Liveness heartbeat 协程 |
| **T0+5.2s** | 2 | 396 | syscfg-listener、store-gossip |
| **T0+5.3s** | 388 | 784 | Raft tick loop、384 个 Raft workers、coalesced heartbeats、snapshot queue |

**稳定状态**：约 **800 个协程**（包括 Replica 内部的协程）

---

## VI. 设计权衡：架构选择的理由与代价

### 6.1 串行 vs 并发加载 Replica

**当前实现**：串行加载（受 `Store.mu` 锁保护）

#### 6.1.1 串行加载的优点

| 优点 | 详情 |
|------|------|
| **简单性** | 无需处理并发注册到 `replicasByKey` btree 的竞态条件 |
| **一致性** | 确保 Replica 按 RangeID 顺序加载（方便调试） |
| **错误处理** | 如果中途失败，已加载的 Replica 可以安全清理 |

#### 6.1.2 串行加载的代价

| 代价 | 影响 |
|------|------|
| **启动耗时** | 10,000 个 Replica 需要 5 秒（而并发可能只需 1 秒） |
| **无法利用多核** | 48 核机器中只有 1 个核心在工作 |
| **扩展性问题** | 如果 Replica 数增长到 50,000，启动时间会增长到 25 秒 |

#### 6.1.3 并发加载的可行性

**改进方案**：
1. **阶段 1**：并发调用 `newInitializedReplica()`（无需锁）
2. **阶段 2**：串行注册到 `Store.mu.replicas`（需要锁，但很快）

**预期效果**（10,000 个 Replica）：
- 阶段 1：5000 ms / 48 核 = **105 ms**（假设完全并发）
- 阶段 2：10,000 × 0.05 ms = **500 ms**（串行注册）
- **总耗时**：**605 ms**（相比当前的 5000 ms，加速 8.3 倍）

**为何未实现**：
- **复杂性增加**：需要处理并发错误（如内存不足导致部分 Replica 创建失败）
- **收益有限**：大部分生产环境中，Store 重启频率很低（每月 1-2 次）
- **优化优先级**：CockroachDB 团队优先优化运行时性能而非启动性能

### 6.2 立即启动 vs 延迟启动 Raft 处理

**当前实现**：在所有 Replica 加载完成后才启动 Raft 处理

#### 6.2.1 立即启动的方案

**假设**：在加载前 1000 个 Replica 后就启动 Raft 处理

**优点**：
- 更快响应客户端请求（首批 Range 可能包含热点数据）

**缺点**：
| 缺点 | 详情 |
|------|------|
| **路由错误** | 收到发往未加载 Replica 的消息，触发 "unknown replica" 错误 |
| **重试风暴** | 发送方会不断重试，浪费网络带宽 |
| **日志噪声** | 大量 "replica not found" 日志，干扰故障排查 |
| **Raft 混乱** | 部分 follower 已启动，部分未启动，leader 无法判断是网络分区还是未加载 |

#### 6.2.2 延迟启动的优点

| 优点 | 详情 |
|------|------|
| **一致性** | 所有 Replica 同时就绪，避免部分可用状态 |
| **简化调试** | 启动日志清晰："initialized 10000/10000 replicas" → "started raft processing" |
| **符合直觉** | "Store 未 ready" 是明确的二元状态，而非模糊的"部分 ready" |

### 6.3 同步 vs 异步启动后台协程

**当前实现**：大部分协程在 `Start()` 返回前启动

#### 6.3.1 同步启动的优点

| 优点 | 详情 |
|------|------|
| **可靠性** | 如果启动失败（如端口被占用），`Start()` 返回 error，阻止 Store 加入集群 |
| **测试友好** | 单元测试可以依赖 `Start()` 返回后所有组件就绪 |
| **简化状态管理** | 避免"启动中"的中间状态 |

#### 6.3.2 异步启动的方案

**假设**：Scanner、Store Rebalancer 异步启动（在后台逐步就绪）

**优点**：
- `Start()` 返回更快（可能从 5.3 秒减少到 5.2 秒）

**缺点**：
| 缺点 | 详情 |
|------|------|
| **复杂性** | 需要状态机管理：`Starting → ScannerReady → RebalancerReady → FullyReady` |
| **竞态条件** | 如果 Scanner 未 ready 时收到 Zone Config 变更，需要缓存事件并延迟处理 |
| **调试困难** | "为何 Replica 未被 rebalance？" 可能是因为 Rebalancer 未启动，也可能是因为 Gossip 未连接 |

**当前实现的折中**：
- **Scanner**：等待 Gossip 连接（异步）
- **Store Gossip**：等待成为 first range 的 leaseholder（异步）
- **其他组件**：同步启动

### 6.4 立即 Unquiesce vs 延迟 Unquiesce

**当前实现**：对使用 LeaderLease/ExpirationLease 的 Replica 立即 unquiesce

#### 6.4.1 立即 Unquiesce 的优点

| 优点 | 详情 |
|------|------|
| **加速升级** | 强制 Range 尽快升级到 EpochLease（支持 quiescence） |
| **减少 CPU** | EpochLease 不需要周期性续约（ExpirationLease 每 9 秒续约一次） |
| **一致性** | 集群范围内统一使用 EpochLease |

#### 6.4.2 延迟 Unquiesce 的方案

**假设**：等待首次写请求时才 unquiesce

**优点**：
- 启动时 CPU 使用率更低（避免立即触发 10,000 个 Range 的 Raft 选举）

**缺点**：
| 缺点 | 详情 |
|------|------|
| **延迟升级** | 如果 Range 长期无写入，会一直使用旧 lease 类型 |
| **首次请求延迟** | 第一个写请求需要等待 Raft 选举完成（可能 1-2 秒） |

**实际影响**：
- 使用 LeaderLease 的 Range 通常很少（新集群或测试环境）
- 生产环境中，大部分 Range 已升级到 EpochLease

---

## VII. 总结与核心思想

### 7.1 Store.Start() 的本质

**核心定位**：**激活器**（Activator）—— 将 NewStore 创建的"休眠工厂"转变为"运转中的生产线"

**核心公式**：
```
Dormant Store (NewStore 输出)
  + Persistent State (Pebble 中的 Replica 元数据)
  + Background Goroutines (Raft 处理、Scanner、Gossip 等)
  = Active Store (可处理客户端请求)
```

### 7.2 三个关键阶段

| 阶段 | 核心操作 | 耗时占比 | 失败后果 |
|------|---------|---------|---------|
| **1. 身份验证** | 读取 StoreIdent，验证节点 ID | <1% | 拒绝启动（防止数据损坏） |
| **2. Replica 加载** | 从 Pebble 加载 10,000 个 Replica | >95% | 无法提供服务 |
| **3. 协程启动** | 启动 11 个后台协程 | <5% | 部分功能不可用（如 Rebalance） |

### 7.3 设计原则

#### 7.3.1 **渐进式就绪**（Progressive Readiness）

不同组件有不同的就绪时机：
- **Rangefeed Scheduler**：立即就绪（阶段 3）
- **Replica**：5 秒后就绪（阶段 9）
- **Scanner**：Gossip 连接后就绪（阶段 10）
- **Store Rebalancer**：最后就绪（阶段 12）

**优点**：
- 避免"全有或全无"的启动失败
- 部分功能可以提前可用（如 Rangefeed）

#### 7.3.2 **依赖倒置**（Dependency Inversion）

Store 不直接创建外部依赖（如 Gossip、NodeLiveness），而是通过 StoreConfig 注入：
```go
s.cfg.Gossip.SetNodeDescriptor(...)  // 使用注入的 Gossip
s.cfg.NodeLiveness.RegisterCallback(...)  // 使用注入的 NodeLiveness
```

**优点**：
- 单元测试可以注入 mock 对象
- 多个 Store 可以共享同一个 Gossip 实例

#### 7.3.3 **最小化临界区**（Minimize Critical Sections）

`Store.mu` 锁只在必要时持有：
```go
// BAD: 长时间持有锁
s.mu.Lock()
for _, repl := range repls {
    rep := newInitializedReplica(...)  // 可能 0.5 ms
    s.addToReplicasLocked(rep)
}
s.mu.Unlock()

// GOOD: 缩小临界区
for _, repl := range repls {
    rep := newInitializedReplica(...)  // 不持有锁
    s.mu.Lock()
    s.addToReplicasLocked(rep)  // 仅在注册时持有锁
    s.mu.Unlock()
}
```

**效果**：
- 减少锁竞争（虽然 Start() 是单线程，但其他协程可能访问 `Store.mu.replicas`）
- 为未来的并发加载优化留出空间

### 7.4 心智模型

将 `Store.Start()` 理解为**汽车启动流程**：

| 汽车部件 | Store 组件 | 启动顺序 |
|---------|-----------|---------|
| **插入钥匙** | 读取 StoreIdent | 1. 验证身份 |
| **通电自检** | 创建 Allocator、Intent Resolver | 2. 初始化子系统 |
| **加载地图** | 加载所有 Replica | 3. 恢复状态 |
| **启动引擎** | 启动 Raft 处理协程 | 4. 开始处理请求 |
| **打开导航** | 启动 Scanner、Rebalancer | 5. 启用高级功能 |
| **开始行驶** | `Store.started = 1` | 6. 标记为 ready |

**关键洞察**：
- 不能在插入钥匙的同时启动引擎（阶段必须顺序执行）
- 可以在行驶时逐步开启导航（部分功能可延迟启动）
- 如果地图损坏（Replica 元数据错误），车不能上路（Start 返回 error）

### 7.5 与 NewStore() 的配合

| 对比维度 | NewStore() | Store.Start() |
|---------|-----------|--------------|
| **阶段** | 构造阶段 | 激活阶段 |
| **输入** | StoreConfig（依赖注入） | Stopper（生命周期管理） |
| **输出** | 休眠 Store 对象 | 活跃 Store 对象 |
| **IO 操作** | 无（纯内存） | 大量读取 Pebble |
| **协程** | 0 | 800+ |
| **耗时** | ~5 ms | ~5 秒（10,000 Replica） |
| **失败原因** | 配置错误、内存不足 | 存储损坏、ID 不匹配 |
| **可重试** | 是（修改配置后重试） | 部分（需修复存储） |

**关键设计原则**：
- **构造要快**：NewStore() 必须在毫秒级完成（方便测试、快速失败）
- **激活要完整**：Start() 必须加载所有 Replica（否则无法保证一致性）

### 7.6 最终状态

**Start() 成功返回后，Store 处于以下状态**：

✅ **所有 Replica 已加载**（10,000 个）
✅ **所有后台协程运行中**（800 个）
✅ **Raft 消息处理就绪**（可接收来自其他节点的消息）
✅ **Scanner 等待 Gossip 连接**（准备扫描 Replica）
✅ **Store Rebalancer 启动**（准备主动重平衡）
✅ **Rangefeed 服务可用**（Changefeeds 可订阅）
✅ **Metrics 注册完成**（Prometheus 可抓取指标）

**Store 现在可以**：
- 响应客户端的读写请求
- 参与 Raft 共识协议
- 接收和发送 Raft 消息
- 自动执行后台维护任务（GC、split、merge、rebalance）
- 向集群 gossip 自己的状态

---

## 附录：代码位置索引

| 组件 | 代码位置 |
|------|---------|
| **Store.Start()** | `pkg/kv/kvserver/store.go:2162-2495` |
| **LoadAndReconcileReplicas()** | `pkg/kv/kvserver/kvstorage/load_replicas.go` |
| **newInitializedReplica()** | `pkg/kv/kvserver/replica_init.go` |
| **processRaft()** | `pkg/kv/kvserver/store_raft.go` |
| **Rangefeed Scheduler** | `pkg/kv/kvserver/rangefeed/scheduler.go` |
| **Intent Resolver** | `pkg/kv/kvserver/intentresolver/intent_resolver.go` |
| **Store Liveness** | `pkg/kv/kvserver/storeliveness/support_manager.go` |
| **Store Rebalancer** | `pkg/kv/kvserver/store_rebalancer.go` |
| **调用点（Node.Start）** | `pkg/server/node.go:721-725` |

---

**下一章预告**：
[第二十六章] 将分析 `Store.processRaft()` 的内部机制，深入探讨：
- Raft Ready 的处理流程（从接收消息到应用到状态机）
- Raft scheduler 的工作窃取算法
- Coalesced heartbeats 的批量优化
- Snapshot 的接收和发送机制
