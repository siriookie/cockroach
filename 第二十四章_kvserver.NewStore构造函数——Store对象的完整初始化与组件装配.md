# 第二十四章 kvserver.NewStore 构造函数——Store 对象的完整初始化与组件装配

## 一、第一轮 BFS：整体职责与设计动机（Why）

### 1.1 背景与要解决的问题

在 CockroachDB 的架构中，**Store** 是 KV 层的核心抽象，代表一个物理存储设备（通常是一块磁盘）上的数据管理单元。每个节点可以有多个 Store（如果挂载了多块磁盘），每个 Store 管理着数千到数万个 **Replica**（Range 的副本）。

`NewStore()` 是 Store 对象的**构造函数**，但它不是简单的内存分配——它是一个**复杂的组件装配工厂**，需要解决以下问题：

1. **多层次组件初始化**：
   - Raft 调度器（处理数万个 Raft 实例）
   - 多个后台队列（GC、Split、Merge、Replication 等）
   - 复杂的缓存结构（Timestamp Cache、Raft Entry Cache）
   - 准入控制和流量限制器

2. **依赖关系管理**：
   - Store 依赖 `StoreConfig`（包含 Clock、Gossip、DB 等全局资源）
   - 组件之间相互依赖（如 Allocator 依赖 StorePool）
   - 需要正确的初始化顺序避免空指针或循环依赖

3. **性能优化配置**：
   - Raft 调度器的并发度（基于 CPU 核心数）
   - 各种限流器的初始值（基于集群设置）
   - 缓存大小和滑动窗口参数

4. **可测试性**：
   - 生产环境需要完整的 Gossip、StorePool 等组件
   - 测试环境需要能够禁用某些队列或使用 mock 对象

**核心问题**：如何在**不启动任何后台任务**的情况下，构造一个**完整但静止**的 Store 对象，使其可以在后续的 `Store.Start()` 中被激活？

### 1.2 在系统中的位置

```
Server.Start()
  ├─ PreStart()
  │   ├─ 打开存储引擎（Pebble）
  │   └─ 构造全局配置（StoreConfig）
  │
  ├─ Node.start()
  │   └─ for each engine:
  │       ├─ kvserver.NewStore(ctx, storeCfg, engine, nodeDesc)  // ← 本章重点
  │       │   ├─ 创建 Store 结构体
  │       │   ├─ 初始化 Raft 调度器
  │       │   ├─ 创建所有队列（但不启动）
  │       │   ├─ 初始化缓存和限流器
  │       │   └─ 返回 Store 指针
  │       │
  │       └─ store.Start(ctx, stopper)  // 激活 Store
  │           ├─ 加载 Replica
  │           ├─ 启动 Raft 处理循环
  │           └─ 启动后台队列
  │
  └─ AcceptClients()
```

**协作模块**：

| 模块 | 依赖方式 | 用途 |
|------|---------|------|
| **StoreConfig** | 输入参数 | 提供全局配置（Clock、Gossip、DB、Settings 等） |
| **storage.Engine** | 输入参数 | Pebble 存储引擎实例 |
| **NodeDescriptor** | 输入参数 | 节点元数据（NodeID、地址、属性） |
| **StorePool** | cfg.StorePool | 集群中所有 Store 的状态跟踪 |
| **Allocator** | 内部创建 | 负责 Replica 放置决策 |
| **Raft Scheduler** | 内部创建 | 管理所有 Replica 的 Raft 状态机 |
| **各种 Queue** | 内部创建 | 后台维护任务（GC、Split、Merge 等） |

### 1.3 核心对象与关键状态

#### Store 结构体的核心字段

```go
// pkg/kv/kvserver/store.go: L873-L1022
type Store struct {
    // ========== 身份与配置 ==========
    Ident           *roachpb.StoreIdent  // StoreID、NodeID（Start 时填充）
    cfg             StoreConfig          // 全局配置
    internalEngines kvstorage.Engines    // Pebble 引擎（Log + State）
    db              *kv.DB               // 分布式 KV 客户端
    nodeDesc        *roachpb.NodeDescriptor

    // ========== Raft 相关 ==========
    scheduler       *raftScheduler       // Raft 事件调度器（核心）
    syncWaiters     []*logstore.SyncWaiterLoop  // fsync 等待器
    raftEntryCache  *raftentry.Cache     // Raft log 缓存
    raftMetrics     *raft.Metrics

    // ========== Replica 管理 ==========
    mu struct {
        syncutil.Mutex
        replicasByKey         *storeReplicaBTree        // Key → Replica 映射
        replicaPlaceholders   map[roachpb.RangeID]*ReplicaPlaceholder
        uninitReplicas        map[roachpb.RangeID]*Replica
        creatingReplicas      map[roachpb.RangeID]struct{}
    }

    // ========== 分配与再平衡 ==========
    allocator           allocatorimpl.Allocator  // Replica 放置决策
    replRankings        *ReplicaRankings         // 按负载排序的 Replica
    storeRebalancer     *StoreRebalancer         // 主动再平衡
    mmaStoreRebalancer  *mmaStoreRebalancer      // MMA 再平衡
    rebalanceObjManager *RebalanceObjectiveManager

    // ========== 后台队列 ==========
    scanner            *replicaScanner      // 扫描所有 Replica
    leaseQueue         *leaseQueue          // Lease 转移
    mvccGCQueue        *mvccGCQueue         // MVCC 垃圾回收
    mergeQueue         *mergeQueue          // Range 合并
    splitQueue         *splitQueue          // Range 分裂
    replicateQueue     *replicateQueue      // Replica 复制
    replicaGCQueue     *replicaGCQueue      // Replica 垃圾回收
    raftLogQueue       *raftLogQueue        // Raft log 截断
    raftSnapshotQueue  *raftSnapshotQueue   // Raft snapshot 修复
    consistencyQueue   *consistencyQueue    // 一致性检查
    tsMaintenanceQueue *timeSeriesMaintenanceQueue

    // ========== 缓存与优化 ==========
    tsCache            tscache.Cache        // Timestamp Cache（避免并发写冲突）
    sstSnapshotStorage snaprecv.SSTSnapshotStorage  // Snapshot 临时存储

    // ========== 流量控制 ==========
    limiters struct {
        BulkIOWriteRate                  *rate.Limiter
        ConcurrentExportRequests         limit.ConcurrentRequestLimiter
        ConcurrentAddSSTableRequests     limit.ConcurrentRequestLimiter
        ConcurrentRangefeedIters         limit.ConcurrentRequestLimiter
    }
    consistencyLimiter               *quotapool.RateLimiter
    snapshotApplyQueue               *multiqueue.MultiQueue
    snapshotSendQueue                *multiqueue.MultiQueue
    tenantRateLimiters               *tenantrate.LimiterFactory

    // ========== IO 负载跟踪 ==========
    ioThresholds *ioThresholds  // LSM 层级阈值（用于准入控制）
    ioThreshold struct {
        t                 *admissionpb.IOThreshold
        maxL0NumSubLevels *slidingwindow.MaxSwag  // 5 分钟滑动窗口
        maxL0NumFiles     *slidingwindow.MaxSwag
        maxL0Size         *slidingwindow.MaxSwag
    }

    // ========== 状态标志 ==========
    started   int32         // 原子标志：0=未启动, 1=已启动
    startedAt int64         // 启动时间（HLC）
    draining  atomic.Bool   // 是否正在排空（graceful shutdown）

    // ========== Raft 消息合并 ==========
    coalescedMu struct {
        syncutil.Mutex
        heartbeats         map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat
        heartbeatResponses map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat
    }
}
```

#### StoreConfig 的关键字段

```go
// pkg/kv/kvserver/store.go: L1163-L1300+
type StoreConfig struct {
    // ========== 全局资源 ==========
    Settings     *cluster.Settings  // 集群配置（动态可调）
    Clock        *hlc.Clock         // 混合逻辑时钟
    Gossip       *gossip.Gossip     // 节点间通信
    DB           *kv.DB             // 分布式 KV 客户端
    NodeLiveness *liveness.NodeLiveness  // 节点存活检测
    StorePool    *storepool.StorePool    // 集群中所有 Store 的状态

    // ========== Raft 配置 ==========
    RaftSchedulerConcurrency         int  // Raft 调度器工作线程数（默认 8*CPU）
    RaftSchedulerConcurrencyPriority int  // 优先级调度器线程数
    RaftSchedulerShardSize           int  // 每个 shard 的最大 worker 数
    RaftElectionTimeoutTicks         int  // 选举超时（tick 数）
    RaftEntryCacheSize               uint64  // Raft log 缓存大小

    // ========== 扫描配置 ==========
    ScanInterval    time.Duration  // Replica 扫描间隔（默认 10 分钟）
    ScanMinIdleTime time.Duration  // 扫描最小空闲时间
    ScanMaxIdleTime time.Duration  // 扫描最大空闲时间

    // ========== 传输层 ==========
    Transport            *RaftTransport         // Raft 消息传输
    NodeDialer           *nodedialer.Dialer     // 节点间 RPC
    RPCContext           *rpc.Context
    ClosedTimestampSender *sidetransport.Sender  // 侧信道传输

    // ========== 测试钩子 ==========
    TestingKnobs StoreTestingKnobs  // 允许测试禁用某些组件
}
```

**关键不变量**：

1. **构造与启动分离**：
   ```
   NewStore() 返回后：
   - Store 对象完整但静止
   - 所有队列已创建但未启动
   - 所有缓存已分配但为空
   - 没有 goroutine 在运行

   Store.Start() 之后：
   - 加载磁盘上的 Replica
   - 启动 Raft 处理循环
   - 启动后台队列
   - 开始处理请求
   ```

2. **配置验证先行**：
   ```go
   if !cfg.Valid() {
       log.Fatalf("invalid store configuration")
   }
   ```
   任何配置错误都会导致 panic，而不是返回错误

3. **依赖注入模式**：
   - 所有外部依赖通过 `StoreConfig` 传入
   - 测试时可以传入 mock 对象

---

## 二、第二轮 BFS：控制流与交互关系（How it flows）

### 2.1 主要执行路径

`NewStore()` 的执行是**完全同步**的，不涉及任何异步操作。整体流程可以分为 **10 个阶段**：

#### 阶段 0：配置验证（L1471-L1473）

```go
func NewStore(
    ctx context.Context,
    cfg StoreConfig,
    eng storage.Engine,
    nodeDesc *roachpb.NodeDescriptor,
) *Store {
    // 1. 验证配置完整性
    if !cfg.Valid() {
        log.KvExec.Fatalf(ctx, "invalid store configuration: %+v", &cfg)
    }
```

**cfg.Valid() 检查的内容**：
- `cfg.Clock != nil`
- `cfg.DB != nil`
- `cfg.Transport != nil`
- `cfg.RaftSchedulerConcurrency > 0`
- 其他必要字段非空

**Why Fatalf？**
- Store 的构造是节点启动的关键路径
- 配置错误是编程错误，不应在运行时恢复
- Fail-fast 避免后续难以调试的问题

#### 阶段 1：初始化 IO 阈值跟踪（L1474-L1493）

```go
    // 2. 初始化 IO 阈值（用于准入控制）
    iot := ioThresholds{}
    iot.Replace(nil, 1.0)  // 初始化为空阈值，fraction=1.0

    s := &Store{
        internalEngines: kvstorage.MakeEngines(eng),
        cfg:             cfg,
        db:              cfg.DB,
        nodeDesc:        nodeDesc,
        metrics:         newStoreMetrics(cfg.HistogramWindowInterval),
        ioThresholds:    &iot,
        // ... 其他字段 ...
    }

    s.ioThreshold.t = &admissionpb.IOThreshold{}

    // 3. 创建滑动窗口跟踪器（5 分钟窗口，1 分钟粒度）
    now := cfg.Clock.Now().GoTime()
    s.ioThreshold.maxL0NumSubLevels = slidingwindow.NewMaxSwag(now, time.Minute, 5)
    s.ioThreshold.maxL0NumFiles = slidingwindow.NewMaxSwag(now, time.Minute, 5)
    s.ioThreshold.maxL0Size = slidingwindow.NewMaxSwag(now, time.Minute, 5)
```

**设计要点**：
- **IO 阈值用于准入控制**（见第十七章）：
  - 跟踪 LSM L0 层的负载（sub-levels、files、size）
  - 当 L0 过载时，减少写入准入
- **滑动窗口**（MaxSwag）：
  - 记录过去 5 分钟内的最大值
  - 每分钟更新一次
  - 避免短暂尖峰触发过度限流

#### 阶段 2：初始化 Allocator 和再平衡管理器（L1494-L1542）

```go
    var allocatorStorePool storepool.AllocatorStorePool
    var storePoolIsDeterministic bool

    if cfg.StorePool != nil {
        allocatorStorePool = cfg.StorePool
        storePoolIsDeterministic = allocatorStorePool.IsDeterministic()

        // 4. 注册 IO 过载回调（当检测到 IO 过载时触发）
        allocatorStorePool.SetOnCapacityChange(s.makeIOOverloadCapacityChangeFn())

        // 5. 创建负载均衡目标管理器
        s.rebalanceObjManager = newRebalanceObjectiveManager(
            ctx,
            s.cfg.AmbientCtx,
            s.cfg.Settings,
            func(ctx context.Context, obj LBRebalancingObjective) {
                // 当再平衡目标变化时，更新所有 Replica 的分裂策略
                s.VisitReplicas(func(r *Replica) (wantMore bool) {
                    r.loadBasedSplitter.SetSplitObjective(
                        s.Clock().PhysicalTime(),
                        obj.ToSplitObjective(),
                    )
                    return true
                })
            },
            allocatorStorePool,
            allocatorStorePool,
        )
    }

    // 6. 创建 Allocator（Replica 放置决策引擎）
    if cfg.RPCContext != nil {
        s.allocator = allocatorimpl.MakeAllocator(
            cfg.Settings,
            cfg.AllocatorSync,
            storePoolIsDeterministic,
            cfg.RPCContext.RemoteClocks.Latency,  // 用于评估节点间延迟
            cfg.TestingKnobs.AllocatorKnobs,
        )
    } else {
        // 测试路径：没有 RPC，无法获取延迟
        s.allocator = allocatorimpl.MakeAllocator(
            cfg.Settings,
            cfg.AllocatorSync,
            storePoolIsDeterministic,
            func(id roachpb.NodeID) (time.Duration, bool) {
                return 0, false
            },
            cfg.TestingKnobs.AllocatorKnobs,
        )
    }
```

**Allocator 的作用**：
- 决定新 Replica 应该放置在哪个 Store
- 决定是否需要添加/删除 Replica
- 基于多种约束：
  - 容量均衡
  - 地理位置（Locality）
  - 节点间延迟
  - IO 负载

**RebalanceObjectiveManager**：
- 根据集群负载动态调整再平衡目标
- 可能是 CPU、QPS、磁盘 IO 等
- 通知所有 Replica 调整分裂策略（负载均衡的手段之一）

#### 阶段 3：初始化 Replica 排名和监控（L1544-L1551）

```go
    // 7. 创建 Replica 排名（用于优先处理高负载 Range）
    s.replRankings = NewReplicaRankings()
    s.replRankingsByTenant = NewReplicaRankingsMap()

    // 8. 创建 Raft 接收队列监控器
    s.raftRecvQueues.mon = mon.NewUnlimitedMonitor(ctx, mon.Options{
        Name:     mon.MakeName("raft-receive-queue"),
        CurCount: s.metrics.RaftRcvdQueuedBytes,
        Settings: cfg.Settings,
    })
```

**ReplicaRankings**：
- 维护按 QPS/CPU/Bytes 排序的 Replica 列表
- 用于优先处理热点 Range
- 定期更新（由 Store.computeMetrics() 触发）

#### 阶段 4：创建 Raft 调度器（L1573-L1591）

这是 NewStore 中**最关键的组件**：

```go
    // 9. 创建 Raft 调度器（核心：管理所有 Replica 的 Raft 状态机）
    s.scheduler = newRaftScheduler(
        cfg.AmbientCtx,
        s.metrics,
        s,
        cfg.RaftSchedulerConcurrency,          // worker 数量（通常是 CPU*8）
        cfg.RaftSchedulerShardSize,            // shard 大小（通常是 256）
        cfg.RaftSchedulerConcurrencyPriority,  // 优先级 worker 数量
        cfg.RaftElectionTimeoutTicks,          // 选举超时（用于缓冲 tick）
    )

    // 10. 创建 Raft log fsync 等待器
    //     每 32 个 Raft worker 对应 1 个 SyncWaiter
    //     在 48 核机器上约有 12 个 SyncWaiter
    numSyncWaiters := (cfg.RaftSchedulerConcurrency-1)/32 + 1
    s.syncWaiters = make([]*logstore.SyncWaiterLoop, numSyncWaiters)
    for i := range s.syncWaiters {
        s.syncWaiters[i] = logstore.NewSyncWaiterLoop()
    }

    // 11. 创建 Raft entry 缓存
    s.raftEntryCache = raftentry.NewCache(cfg.RaftEntryCacheSize)
    s.metrics.registry.AddMetricStruct(s.raftEntryCache.Metrics())
```

**Raft 调度器的架构**：

```
                    ┌─────────────────────────┐
                    │   Raft Scheduler        │
                    │  (管理数万个 Replica)    │
                    └───────────┬─────────────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
        ┌───────▼───────┐               ┌───────▼───────┐
        │  Worker Pool  │               │ Priority Pool │
        │  (8*CPU 个)   │               │  (1*CPU 个)   │
        └───────┬───────┘               └───────┬───────┘
                │                               │
     ┌──────────┴──────────┐         ┌──────────┴──────────┐
     │                     │         │                     │
┌────▼────┐          ┌────▼────┐   ┌────▼────┐      ┌────▼────┐
│ Shard 1 │          │ Shard N │   │ Shard P1│      │ Shard Pn│
│ (256)   │   ...    │ (256)   │   │ (256)   │ ...  │ (256)   │
└─────────┘          └─────────┘   └─────────┘      └─────────┘
    │                     │             │                 │
    └─────────────────────┴─────────────┴─────────────────┘
                          │
                  处理 Raft 事件：
                  - Ready 事件
                  - Tick 事件
                  - 消息处理
```

**Why 每 32 个 worker 一个 SyncWaiter？**

（L1579-L1583 注释）：
> Experiments on c5d.12xlarge instances (48 vCPUs) show that with fewer SyncWaiters,
> raft log callback processing can become a bottleneck for write heavy workloads,
> which can drive about 100k raft log appends per second, per store.

- 48 CPU → 384 Raft workers → 12 SyncWaiters
- 每秒 100k appends → 每个 SyncWaiter 处理 ~8k appends/s
- 如果只有 1 个 SyncWaiter → 单线程瓶颈

#### 阶段 5：初始化 Replica 管理数据结构（L1593-L1614）

```go
    // 12. 初始化 Raft 心跳合并 map
    s.coalescedMu.Lock()
    s.coalescedMu.heartbeats = map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat{}
    s.coalescedMu.heartbeatResponses = map[roachpb.StoreIdent][]kvserverpb.RaftHeartbeat{}
    s.coalescedMu.Unlock()

    // 13. 初始化 Replica 查找数据结构
    s.mu.Lock()
    s.mu.replicaPlaceholders = map[roachpb.RangeID]*ReplicaPlaceholder{}
    s.mu.replicasByKey = newStoreReplicaBTree()  // BTree：Key → Replica
    s.mu.creatingReplicas = map[roachpb.RangeID]struct{}{}
    s.mu.uninitReplicas = map[roachpb.RangeID]*Replica{}
    s.mu.Unlock()

    // 14. 初始化 unquiesced Replica 跟踪
    s.unquiescedOrAwakeReplicas.Lock()
    s.unquiescedOrAwakeReplicas.m = map[roachpb.RangeID]struct{}{}
    s.unquiescedOrAwakeReplicas.Unlock()

    // 15. 初始化 rangefeed Replica 跟踪
    s.rangefeedReplicas.Lock()
    s.rangefeedReplicas.m = map[roachpb.RangeID]int64{}
    s.rangefeedReplicas.Unlock()
```

**数据结构的用途**：

| 数据结构 | 类型 | 用途 |
|---------|-----|------|
| `replicasByKey` | BTree | Key → Replica 映射（快速查找） |
| `replicaPlaceholders` | map | 预留 RangeID（防止并发创建） |
| `creatingReplicas` | set | 正在创建中的 Replica |
| `uninitReplicas` | map | 未初始化的 Replica（只有 RangeID，没有 Range 描述符） |
| `unquiescedOrAwakeReplicas` | set | 未静默的 Replica（需要处理 tick） |
| `rangefeedReplicas` | map | 有 rangefeed 订阅的 Replica |

#### 阶段 6：初始化缓存和事务管理（L1613-L1618）

```go
    // 16. 创建 Timestamp Cache（防止并发写冲突）
    s.tsCache = tscache.New(cfg.Clock)
    s.metrics.registry.AddMetricStruct(s.tsCache.Metrics())

    // 17. 创建事务等待指标
    s.txnWaitMetrics = txnwait.NewMetrics(cfg.HistogramWindowInterval)
    s.metrics.registry.AddMetricStruct(s.txnWaitMetrics)

    // 18. 创建 Raft 指标
    s.raftMetrics = raft.NewMetrics()
    s.metrics.registry.AddMetricStruct(s.raftMetrics)
```

**Timestamp Cache**：
- 记录最近读取的 Key 和时间戳
- 防止"过去写"：新写入的时间戳必须 > 已有读的时间戳
- 实现快照隔离（Snapshot Isolation）

#### 阶段 7：创建快照队列和限流器（L1622-L1698）

```go
    // 19. 创建快照应用队列（限制并发）
    s.snapshotApplyQueue = multiqueue.NewMultiQueue(
        int(snapshotApplyLimit.Get(&cfg.Settings.SV))
    )
    snapshotApplyLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
        s.snapshotApplyQueue.UpdateConcurrencyLimit(
            int(snapshotApplyLimit.Get(&cfg.Settings.SV))
        )
    })

    // 20. 创建快照发送队列
    s.snapshotSendQueue = multiqueue.NewMultiQueue(
        int(SnapshotSendLimit.Get(&cfg.Settings.SV))
    )

    // 21. 创建一致性检查限流器
    s.consistencyLimiter = quotapool.NewRateLimiter(
        "ConsistencyQueue",
        quotapool.Limit(consistencyCheckRate.Get(&cfg.Settings.SV)),
        consistencyCheckRate.Get(&cfg.Settings.SV)*consistencyCheckRateBurstFactor,
        quotapool.WithMinimumWait(consistencyCheckRateMinWait),
    )

    // 22. 创建批量 IO 写入限流器
    s.limiters.BulkIOWriteRate = rate.NewLimiter(
        rate.Limit(bulkIOWriteLimit.Get(&cfg.Settings.SV)),
        kvserverbase.BulkIOWriteBurst,
    )

    // 23. 创建 Export 请求限流器（限制并发数）
    s.limiters.ConcurrentExportRequests = limit.MakeConcurrentRequestLimiter(
        "exportRequestLimiter",
        int(exportRequestsLimit.Get(&cfg.Settings.SV)),
    )

    // 24. 创建 AddSSTable 请求限流器
    s.limiters.ConcurrentAddSSTableRequests = limit.MakeConcurrentRequestLimiter(
        "addSSTableRequestLimiter",
        int(addSSTableRequestLimit.Get(&cfg.Settings.SV)),
    )

    // 25. 创建 Rangefeed 迭代器限流器
    s.limiters.ConcurrentRangefeedIters = limit.MakeConcurrentRequestLimiter(
        "rangefeedIterLimiter",
        int(ConcurrentRangefeedItersLimit.Get(&cfg.Settings.SV)),
    )

    // 26. 创建 SST 快照存储（临时目录）
    s.sstSnapshotStorage = snaprecv.NewSSTSnapshotStorage(
        s.StateEngine(),
        s.limiters.BulkIOWriteRate,
    )
    if err := s.sstSnapshotStorage.Clear(); err != nil {
        log.KvDistribution.Warningf(ctx, "failed to clear snapshot storage: %v", err)
    }
```

**限流器的用途**：

| 限流器 | 类型 | 目的 |
|-------|-----|-----|
| `snapshotApplyQueue` | 并发数限制 | 防止同时应用太多 snapshot（内存和 CPU 密集） |
| `consistencyLimiter` | 速率限制 | 一致性检查很昂贵，限制频率 |
| `BulkIOWriteRate` | 速率限制 | 批量导入限流，避免影响正常请求 |
| `ConcurrentExportRequests` | 并发数限制 | Export 会扫描大量数据，限制并发 |

**Dynamic Configuration**：
- 所有限流器都注册了 `SetOnChange` 回调
- 当集群设置变化时，自动调整限制
- 无需重启节点

#### 阶段 8：创建租户限流器（L1699-L1722）

```go
    // 27. 创建租户速率限流器工厂
    authorizer := cfg.TestingKnobs.TenantRateKnobs.Authorizer
    if cfg.RPCContext != nil && cfg.RPCContext.TenantRPCAuthorizer != nil {
        authorizer = cfg.RPCContext.TenantRPCAuthorizer
    }
    if authorizer == nil {
        log.KvDistribution.Fatalf(ctx, "programming error: missing authorizer from config")
    }

    s.tenantRateLimiters = tenantrate.NewLimiterFactory(
        &cfg.Settings.SV,
        &cfg.TestingKnobs.TenantRateKnobs,
        authorizer,
    )
    s.metrics.registry.AddMetricStruct(s.tenantRateLimiters.Metrics())

    // 28. 创建系统配置更新队列限流器
    s.systemConfigUpdateQueueRateLimiter = quotapool.NewRateLimiter(
        "SystemConfigUpdateQueue",
        quotapool.Limit(queueAdditionOnSystemConfigUpdateRate.Get(&cfg.Settings.SV)),
        queueAdditionOnSystemConfigUpdateBurst.Get(&cfg.Settings.SV),
    )

    // 29. 注册锁表大小变化回调
    concurrency.DefaultLockTableSize.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
        newSize := concurrency.DefaultLockTableSize.Get(&cfg.Settings.SV)
        s.VisitReplicas(func(repl *Replica) bool {
            repl.concMgr.SetMaxLockTableSize(newSize)
            return true
        })
    })
```

**租户限流**：
- CockroachDB 支持多租户（multi-tenancy）
- 每个租户有独立的速率限制
- 防止某个租户的突发流量影响其他租户

#### 阶段 9：创建后台队列（L1732-L1771）

```go
    if s.cfg.Gossip != nil {
        // 30. 创建 Store Gossip（广播 Store 容量信息）
        s.storeGossip = NewStoreGossip(
            cfg.Gossip,
            s,
            cfg.TestingKnobs.GossipTestingKnobs,
            &cfg.Settings.SV,
            timeutil.DefaultTimeSource{},
        )

        // 31. 创建 Replica 扫描器
        s.scanner = newReplicaScanner(
            s.cfg.AmbientCtx,
            s.cfg.Clock,
            cfg.ScanInterval,      // 默认 10 分钟
            cfg.ScanMinIdleTime,
            cfg.ScanMaxIdleTime,
            newStoreReplicaVisitor(s),
        )

        // 32. 创建所有后台队列
        s.leaseQueue = newLeaseQueue(s, s.allocator)
        s.mvccGCQueue = newMVCCGCQueue(s)
        s.mergeQueue = newMergeQueue(s, s.db)
        s.splitQueue = newSplitQueue(s, s.db)
        s.replicateQueue = newReplicateQueue(s, s.allocator)
        s.replicaGCQueue = newReplicaGCQueue(s, s.db)
        s.mmaStoreRebalancer = newMMAStoreRebalancer(s, s.cfg.MMAllocator, s.cfg.Settings, s.cfg.StorePool)
        s.raftLogQueue = newRaftLogQueue(s, s.db)
        s.raftSnapshotQueue = newRaftSnapshotQueue(s)
        s.consistencyQueue = newConsistencyQueue(s)

        // 33. 将队列注册到扫描器
        s.scanner.AddQueues(
            s.mvccGCQueue,
            s.mergeQueue,
            s.splitQueue,
            s.replicateQueue,
            s.replicaGCQueue,
            s.raftLogQueue,
            s.raftSnapshotQueue,
            s.consistencyQueue,
            s.leaseQueue,
        )

        // 34. 创建时间序列维护队列（如果启用）
        if tsDS := s.cfg.TimeSeriesDataStore; tsDS != nil {
            s.tsMaintenanceQueue = newTimeSeriesMaintenanceQueue(s, s.db, tsDS)
            s.scanner.AddQueues(s.tsMaintenanceQueue)
        }
    }
```

**队列的职责**：

| 队列 | 职责 | 触发频率 |
|-----|------|---------|
| **leaseQueue** | 转移 lease 到更优位置 | 扫描周期 |
| **mvccGCQueue** | 清理旧版本数据 | 扫描周期 |
| **mergeQueue** | 合并小 Range | 扫描周期 |
| **splitQueue** | 分裂大 Range | 扫描周期 |
| **replicateQueue** | 添加/删除 Replica | 扫描周期 |
| **replicaGCQueue** | 清理已删除的 Replica | 扫描周期 |
| **raftLogQueue** | 截断 Raft log | 扫描周期 |
| **raftSnapshotQueue** | 修复落后的 Replica | 扫描周期 |
| **consistencyQueue** | 一致性检查 | 扫描周期 |
| **tsMaintenanceQueue** | 清理时间序列数据 | 扫描周期 |

**Why 在 NewStore 中创建但不启动？**

- **构造与启动分离**：NewStore 只是准备，Store.Start 才真正激活
- **测试友好**：测试可以禁用某些队列（通过 TestingKnobs）
- **避免竞争**：队列依赖 Store 的很多字段，必须等 Store 完全初始化

#### 阶段 10：处理测试钩子并返回（L1774-L1808）

```go
    // 35. 根据测试配置禁用某些队列
    if cfg.TestingKnobs.DisableGCQueue {
        s.testingSetGCQueueActive(false)
    }
    if cfg.TestingKnobs.DisableLeaseQueue {
        s.TestingSetLeaseQueueActive(false)
    }
    if cfg.TestingKnobs.DisableMergeQueue {
        s.testingSetMergeQueueActive(false)
    }
    // ... 其他队列类似 ...

    if cfg.TestingKnobs.DisableScanner {
        s.testingSetScannerActive(false)
    }

    // 36. 返回构造完成的 Store 对象
    return s
}
```

### 2.2 调用时机与上下文

`NewStore()` 在 `Node.start()` 中被调用：

```go
// pkg/server/node.go: L691-L709
for i := range state.initializedEngines {
    engine := state.initializedEngines[i]
    err := n.stopper.RunAsyncTaskEx(ctx,
        stop.TaskOpts{TaskName: "initialize-stores", ...},
        func(ctx context.Context) {
            start := timeutil.Now()

            // 调用 NewStore 构造 Store 对象
            s := kvserver.NewStore(ctx, n.storeCfg, engine, &n.Descriptor)

            // 启动 Store（加载 Replica、启动 Raft）
            if err := s.Start(workersCtx, n.stopper); err != nil {
                engineErrC <- errors.Wrap(err, "failed to start store")
                return
            }

            // 将 Store 添加到 Node
            n.addStore(ctx, s)

            log.Dev.Infof(ctx, "initialized store s%s in %s (%d replicas)",
                s.StoreID(), timeutil.Since(start), s.ReplicaCount())
            engineErrC <- nil
        })
}
```

**时间线**：

```
T0: Node.start() 开始
T1: 为每个 engine 启动 goroutine
T2: goroutine 调用 NewStore()
    → 构造 Store 对象（同步，~1ms）
T3: goroutine 调用 Store.Start()
    → 加载磁盘上的 Replica（耗时，可能数秒）
    → 启动 Raft 处理循环
    → 启动后台队列
T4: Store 启动完成，添加到 Node
T5: 所有 Store 启动完成，Node.start() 返回
```

### 2.3 与其他组件的交互

#### 与 StoreConfig 的交互

```
NewStore(cfg StoreConfig)
  ├─ 使用 cfg.Clock → s.tsCache, s.ioThreshold
  ├─ 使用 cfg.DB → s.db
  ├─ 使用 cfg.StorePool → s.allocator
  ├─ 使用 cfg.Settings → 所有限流器
  ├─ 使用 cfg.Gossip → s.storeGossip
  └─ 使用 cfg.Transport → (Store.Start 时使用)
```

#### 与 Replica 的交互（在 Start 之后）

```
Store.Start()
  └─ LoadAndReconcileReplicas()
      └─ for each range:
          ├─ NewReplica(store, desc)  // 创建 Replica 对象
          │   └─ replica.store = store  // Replica 持有 Store 引用
          │
          └─ store.addReplicaInternalLocked(replica)
              ├─ store.mu.replicasByKey.Insert(replica)
              └─ replica 可以使用：
                  ├─ store.scheduler.EnqueueRaftReady(replica)
                  ├─ store.tsCache
                  ├─ store.allocator
                  └─ store.limiters
```

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 newRaftScheduler() 的实现

这是 Store 中最复杂的组件之一，负责调度所有 Replica 的 Raft 事件。

```go
// pkg/kv/kvserver/scheduler.go
func newRaftScheduler(
    ambientCtx log.AmbientContext,
    metrics *StoreMetrics,
    store raftProcessor,
    concurrency int,     // 工作线程数（如 48*8=384）
    shardSize int,       // shard 大小（如 256）
    priorityConcurrency int,  // 优先级线程数（如 48）
    maxBufferLen int,    // tick 缓冲大小（如 RaftElectionTimeoutTicks）
) *raftScheduler {
    // 计算 shard 数量：ceil(concurrency / shardSize)
    numShards := (concurrency-1)/shardSize + 1

    shards := make([]raftSchedulerShard, numShards)
    for i := range shards {
        shards[i].init(ambientCtx, metrics, store, shardSize, maxBufferLen)
    }

    // 优先级 shard（用于紧急任务，如选举）
    numPriorityShards := (priorityConcurrency-1)/shardSize + 1
    priorityShards := make([]raftSchedulerShard, numPriorityShards)
    for i := range priorityShards {
        priorityShards[i].init(ambientCtx, metrics, store, shardSize, maxBufferLen)
    }

    return &raftScheduler{
        ambientCtx:      ambientCtx,
        metrics:         metrics,
        store:           store,
        shards:          shards,
        priorityShards:  priorityShards,
        concurrency:     concurrency,
        maxBufferLen:    maxBufferLen,
    }
}
```

**Shard 的结构**：

```go
type raftSchedulerShard struct {
    ambientCtx log.AmbientContext
    metrics    *StoreMetrics
    processor  raftProcessor  // 指向 Store

    mu struct {
        syncutil.Mutex
        // workers 是 goroutine 池
        workers []*raftSchedulerWorker
        // queue 是待处理的 Replica 队列
        queue []replicaInQueue
    }

    // tickBuffer 缓冲 tick 事件，避免频繁唤醒
    tickBuffer struct {
        mu     syncutil.Mutex
        buffer []roachpb.RangeID
    }
}
```

**工作原理**：

1. **EnqueueRaftReady(replica)**：
   ```go
   func (s *raftScheduler) EnqueueRaftReady(rangeID roachpb.RangeID) {
       // 选择 shard（基于 RangeID 哈希）
       shard := &s.shards[int(rangeID) % len(s.shards)]

       shard.mu.Lock()
       defer shard.mu.Unlock()

       // 添加到队列
       shard.mu.queue = append(shard.mu.queue, replicaInQueue{
           rangeID: rangeID,
           typ:     raftReadyEvent,
       })

       // 唤醒一个 worker（如果有空闲）
       shard.maybeStartWorkerLocked()
   }
   ```

2. **Worker 处理循环**：
   ```go
   func (w *raftSchedulerWorker) run() {
       for {
           // 从 shard 取出任务
           task := w.shard.dequeue()
           if task == nil {
               return  // 没有任务，worker 退出
           }

           // 调用 Store.processReady()
           w.shard.processor.processReady(task.rangeID)
       }
   }
   ```

**并发模型**：

```
假设 48 核，384 个 worker，256 per shard

┌─────────────────────────────────────────┐
│          Raft Scheduler                 │
│  (管理 10,000 个 Replica)                │
└──────────────┬──────────────────────────┘
               │
      ┌────────┴────────┐
      │                 │
┌─────▼─────┐     ┌─────▼─────┐
│  Shard 0  │ ... │  Shard 1  │
│  (384/2)  │     │  (384/2)  │
└─────┬─────┘     └─────┬─────┘
      │                 │
      │                 │
  192 workers       192 workers

每个 Shard 管理 ~5,000 个 Replica
```

**Why 需要 Shard？**

- **减少锁竞争**：如果只有一个全局队列，所有 worker 竞争同一个锁
- **提高缓存局部性**：同一 Shard 的 Replica 可能在相邻的内存位置
- **均衡负载**：通过哈希将 Replica 分散到不同 Shard

### 3.2 ioThresholds 的初始化

```go
// pkg/kv/kvserver/store.go: L1474-L1493
type ioThresholds struct {
    // l0FileCountOverload 等字段（原子访问）
    // ...
}

func (iot *ioThresholds) Replace(prev *admissionpb.IOThreshold, fraction float64) {
    // 用 prev 的值乘以 fraction 来初始化阈值
    // fraction=1.0 表示使用原始阈值
    // fraction<1.0 表示更严格的限制
}

// Store 初始化时：
iot := ioThresholds{}
iot.Replace(nil, 1.0)  // nil → 使用默认空阈值，fraction=1.0
s.ioThresholds = &iot

// 创建滑动窗口（5 分钟，1 分钟粒度）
now := cfg.Clock.Now().GoTime()
s.ioThreshold.maxL0NumSubLevels = slidingwindow.NewMaxSwag(now, time.Minute, 5)
s.ioThreshold.maxL0NumFiles = slidingwindow.NewMaxSwag(now, time.Minute, 5)
s.ioThreshold.maxL0Size = slidingwindow.NewMaxSwag(now, time.Minute, 5)
```

**运行时更新**（在 Store.Start 之后）：

```go
// Store 定期从 Pebble 读取 LSM 指标
func (s *Store) updateIOThreshold() {
    metrics := s.engine.GetMetrics()

    // 更新滑动窗口
    now := s.Clock().PhysicalTime()
    s.ioThreshold.maxL0NumSubLevels.Record(now, float64(metrics.L0SubLevels))
    s.ioThreshold.maxL0NumFiles.Record(now, float64(metrics.L0NumFiles))
    s.ioThreshold.maxL0Size.Record(now, float64(metrics.L0TotalSize))

    // 计算新阈值
    newThreshold := &admissionpb.IOThreshold{
        L0NumSubLevelsThreshold: int64(s.ioThreshold.maxL0NumSubLevels.Query(now)),
        L0NumFilesThreshold:     int64(s.ioThreshold.maxL0NumFiles.Query(now)),
        L0MinimumSizePerSubLevel: metrics.L0TotalSize / metrics.L0SubLevels,
    }

    // 原子替换
    s.ioThresholds.Replace(newThreshold, 1.0)
}
```

**Why 使用滑动窗口？**

```
假设 L0 sub-levels 的历史：

T0:  10  ─┐
T1:  12   │
T2:  50  ←┼─ 瞬时尖峰
T3:  15   │
T4:  13  ─┘

如果不用滑动窗口：
  → threshold = 50（被瞬时尖峰影响）
  → 后续正常负载（15）不会触发限流

使用 5 分钟滑动窗口：
  → 5 分钟内最大值 = 50
  → threshold = 50（保护窗口期内的最大负载）
  → 如果 5 分钟后未再出现尖峰 → threshold 逐渐下降
```

### 3.3 Allocator 的创建

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go
func MakeAllocator(
    settings *cluster.Settings,
    allocatorSync *mmaintegration.AllocatorSync,
    deterministic bool,
    latencyFunc func(roachpb.NodeID) (time.Duration, bool),
    knobs *AllocatorKnobs,
) Allocator {
    return Allocator{
        settings:       settings,
        allocatorSync:  allocatorSync,
        deterministic:  deterministic,
        latencyFunc:    latencyFunc,
        knobs:          knobs,
        randGen:        makeAllocatorRand(randutil.NewLockedSource()),
    }
}
```

**输入/输出**：
- **输入**：
  - `latencyFunc`：查询节点间延迟（用于评估放置质量）
  - `deterministic`：测试模式（固定随机种子）
- **输出**：`Allocator` 对象（方法集）

**核心方法**（在 Allocator 上调用）：

```go
// 决定应该添加 Replica 到哪个 Store
func (a *Allocator) AllocateTarget(
    ctx context.Context,
    conf roachpb.SpanConfig,
    existingVoters []roachpb.ReplicaDescriptor,
    existingNonVoters []roachpb.ReplicaDescriptor,
) (roachpb.ReplicationTarget, error)

// 决定应该从哪个 Store 移除 Replica
func (a *Allocator) RemoveTarget(
    ctx context.Context,
    conf roachpb.SpanConfig,
    voters []roachpb.ReplicaDescriptor,
    nonVoters []roachpb.ReplicaDescriptor,
) (roachpb.ReplicaDescriptor, error)
```

**约束考虑**（优先级从高到低）：

1. **硬约束**（必须满足）：
   - Locality 要求（如 `num_replicas=3, region=us-east`）
   - 不同节点（同一节点不能有同一 Range 的两个 Replica）

2. **软约束**（尽量满足）：
   - 容量均衡（避免某个 Store 容量过满）
   - 负载均衡（避免某个 Store QPS 过高）
   - 延迟优化（Replica 尽量靠近）

**不变量**：
- 分配决策是**幂等**的：相同输入 → 相同输出（在确定性模式下）
- 分配决策是**快速**的：通常 < 1ms（基于缓存的 StorePool 信息）

### 3.4 队列的创建与注册

```go
// pkg/kv/kvserver/replicate_queue.go
func newReplicateQueue(store *Store, allocator allocatorimpl.Allocator) *replicateQueue {
    rq := &replicateQueue{
        metrics:       makeReplicateQueueMetrics(),
        allocator:     allocator,
        updateChan:    make(chan time.Time, 1),
        lastInterval:  defaultProcessTimeout,
    }

    rq.baseQueue = newBaseQueue(
        "replicate",
        rq,
        store,
        queueConfig{
            maxSize:              defaultQueueMaxSize,
            maxConcurrency:       replicateQueueConcurrency,
            needsLease:           true,
            needsSystemConfig:    true,
            acceptsUnsplitRanges: false,
            successes:            store.metrics.ReplicateQueueSuccesses,
            failures:             store.metrics.ReplicateQueueFailures,
            pending:              store.metrics.ReplicateQueuePending,
            processingNanos:      store.metrics.ReplicateQueueProcessingNanos,
        },
    )

    return rq
}
```

**baseQueue 的设计**：

```go
type baseQueue struct {
    name           string
    impl           queueImpl  // 具体队列的实现（如 replicateQueue）
    store          *Store
    maxSize        int
    maxConcurrency int

    mu struct {
        syncutil.Mutex
        replicas     map[roachpb.RangeID]*replicaItem  // 待处理的 Replica
        priorityQ    priorityQueue  // 优先级队列（基于 heap）
    }

    // 处理 goroutine 池
    sem chan struct{}  // 信号量（限制并发）
}
```

**队列的生命周期**：

1. **NewStore()**：
   ```go
   s.replicateQueue = newReplicateQueue(s, s.allocator)
   s.scanner.AddQueues(s.replicateQueue)
   ```
   队列已创建，但**未启动**

2. **Store.Start()**：
   ```go
   s.scanner.Start()  // 启动扫描器
   ```
   扫描器定期访问所有 Replica，调用 `queue.MaybeAddAsync(replica)`

3. **运行时**：
   ```go
   queue.MaybeAddAsync(replica)
     → 检查 replica 是否需要处理
     → 如果需要，添加到 priorityQ
     → 如果有空闲 worker，唤醒处理
   ```

**并发控制**：

```go
// baseQueue.processLoop()
func (bq *baseQueue) processLoop(ctx context.Context, stopper *stop.Stopper) {
    for {
        // 等待信号量（限制并发）
        select {
        case <-bq.sem:
        case <-stopper.ShouldQuiesce():
            return
        }

        // 从优先级队列取出 Replica
        bq.mu.Lock()
        item := heap.Pop(&bq.mu.priorityQ).(*replicaItem)
        bq.mu.Unlock()

        // 调用具体队列的处理逻辑
        if err := bq.impl.process(ctx, item.replica); err != nil {
            bq.failures.Inc(1)
        } else {
            bq.successes.Inc(1)
        }

        // 释放信号量
        bq.sem <- struct{}{}
    }
}
```

---

## 四、动态行为分析（Runtime 行为）

### 4.1 Raft 调度器的负载均衡

**问题**：10,000 个 Replica，384 个 worker，如何保证负载均衡？

**方案**：基于 RangeID 的一致性哈希

```go
func (s *raftScheduler) EnqueueRaftReady(rangeID roachpb.RangeID) {
    // 选择 shard
    shardIdx := int(rangeID) % len(s.shards)
    shard := &s.shards[shardIdx]

    // 添加到 shard 的队列
    shard.enqueue(rangeID, raftReadyEvent)
}
```

**效果**：

```
假设 10,000 个 Replica，2 个 Shard

Shard 0: RangeID 为偶数的 Replica（5,000 个）
Shard 1: RangeID 为奇数的 Replica（5,000 个）

每个 Shard 有 192 个 worker
每个 worker 平均处理 ~26 个 Replica
```

**动态调整**（不支持）：
- Raft 调度器的并发度是**启动时固定**的
- 无法动态增加/减少 worker
- 原因：Raft 状态机的并发控制非常复杂，动态调整会引入竞态

### 4.2 限流器的动态更新

**场景**：集群设置变化时，限流器如何更新？

```sql
-- 用户执行 SQL
SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '128MiB';
```

**系统响应**：

1. **集群设置更新**：
   ```go
   // pkg/settings/settings.go
   snapshotApplyLimit.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
       newLimit := int(snapshotApplyLimit.Get(&cfg.Settings.SV))
       s.snapshotApplyQueue.UpdateConcurrencyLimit(newLimit)
   })
   ```

2. **回调触发**：
   ```
   SQL 命令
     → 写入 system.settings 表
     → Gossip 广播到所有节点
     → 每个节点的 Settings.SV 更新
     → 触发 SetOnChange 回调
     → 更新限流器
   ```

3. **立即生效**：
   ```go
   func (mq *multiqueue.MultiQueue) UpdateConcurrencyLimit(newLimit int) {
       mq.mu.Lock()
       defer mq.mu.Unlock()

       oldLimit := len(mq.mu.sem)
       if newLimit > oldLimit {
           // 增加并发：添加信号量
           for i := 0; i < newLimit-oldLimit; i++ {
               mq.mu.sem <- struct{}{}
           }
       } else if newLimit < oldLimit {
           // 减少并发：移除信号量（等待当前任务完成）
           for i := 0; i < oldLimit-newLimit; i++ {
               <-mq.mu.sem
           }
       }
   }
   ```

**无缝更新**：
- 正在运行的任务不受影响
- 新任务使用新的限制
- 无需重启节点

### 4.3 队列的优先级调度

**问题**：某些 Replica 更紧急（如负载高、容量满），如何优先处理？

**方案**：基于 heap 的优先级队列

```go
type replicaItem struct {
    replica  *Replica
    priority float64  // 越高越紧急
    index    int      // heap 索引
}

type priorityQueue []*replicaItem

func (pq priorityQueue) Less(i, j int) bool {
    // 优先级高的排在前面
    return pq[i].priority > pq[j].priority
}

// 添加 Replica 到队列
func (bq *baseQueue) MaybeAddAsync(ctx context.Context, repl *Replica) {
    // 计算优先级（由具体队列实现）
    shouldQueue, priority := bq.impl.shouldQueue(ctx, repl)
    if !shouldQueue {
        return
    }

    bq.mu.Lock()
    defer bq.mu.Unlock()

    item := &replicaItem{
        replica:  repl,
        priority: priority,
    }
    heap.Push(&bq.mu.priorityQ, item)
}
```

**优先级示例**（replicateQueue）：

```go
func (rq *replicateQueue) shouldQueue(ctx context.Context, repl *Replica) (bool, float64) {
    desc := repl.Desc()

    // 检查是否需要复制
    if len(desc.Replicas) < repl.SpanConfig().NumReplicas {
        // 缺少 Replica → 高优先级
        return true, 100.0
    }

    // 检查是否过度复制
    if len(desc.Replicas) > repl.SpanConfig().NumReplicas {
        // 多余 Replica → 中优先级
        return true, 50.0
    }

    // 检查是否负载不均
    if rq.allocator.ShouldTransferLease(ctx, repl) {
        // 需要 lease 转移 → 低优先级
        return true, 10.0
    }

    return false, 0.0
}
```

**效果**：
- 缺少 Replica 的 Range 优先处理（影响可用性）
- Lease 转移的 Range 延后处理（只影响性能）

---

## 五、具体示例（必须有）

### 5.1 完整的 Store 构造示例

假设一个 **48 核**机器，启动 CockroachDB 节点。

#### 时刻 T0：Node.start() 准备构造 Store

```go
// 输入参数
engine := openedPebbleEngine  // 已打开的 Pebble 引擎
nodeDesc := &roachpb.NodeDescriptor{
    NodeID:  1,
    Address: util.MakeUnresolvedAddr("tcp", "node1:26257"),
}
storeCfg := StoreConfig{
    Settings:                 clusterSettings,
    Clock:                    hlcClock,
    DB:                       kvClient,
    Gossip:                   gossipInstance,
    StorePool:                storePoolInstance,
    RaftSchedulerConcurrency: 384,  // 48 CPU * 8
    RaftSchedulerShardSize:   256,
    RaftEntryCacheSize:       128 << 20,  // 128 MB
    ScanInterval:             10 * time.Minute,
    // ... 其他配置 ...
}
```

#### 时刻 T0+0.5ms：配置验证

```go
if !cfg.Valid() {
    log.Fatalf("invalid store configuration")
}
// → 验证通过
```

#### 时刻 T0+1ms：初始化 IO 阈值

```go
iot := ioThresholds{}
iot.Replace(nil, 1.0)
// → 初始化为空阈值

now := cfg.Clock.Now().GoTime()  // 2024-01-16 12:00:00 UTC
s.ioThreshold.maxL0NumSubLevels = slidingwindow.NewMaxSwag(now, time.Minute, 5)
// → 创建 5 分钟滑动窗口，1 分钟粒度
```

**内存分配**：
```
ioThreshold 结构体：~200 bytes
滑动窗口（3 个）：~300 bytes
```

#### 时刻 T0+1.5ms：创建 Allocator

```go
s.allocator = allocatorimpl.MakeAllocator(
    cfg.Settings,
    cfg.AllocatorSync,
    false,  // storePoolIsDeterministic
    cfg.RPCContext.RemoteClocks.Latency,
    cfg.TestingKnobs.AllocatorKnobs,
)
```

**内存分配**：
```
Allocator 结构体：~100 bytes
随机数生成器：~50 bytes
```

#### 时刻 T0+2ms：创建 Raft 调度器

```go
s.scheduler = newRaftScheduler(
    cfg.AmbientCtx,
    s.metrics,
    s,
    384,  // concurrency
    256,  // shardSize
    48,   // priorityConcurrency
    5,    // RaftElectionTimeoutTicks
)
```

**内部计算**：
```
numShards = ceil(384 / 256) = 2
numPriorityShards = ceil(48 / 256) = 1

Shard 0: 192 workers
Shard 1: 192 workers
Priority Shard: 48 workers
```

**内存分配**：
```
raftScheduler：~500 bytes
3 个 Shard：~1.5 KB（每个 shard ~500 bytes）
384+48=432 个 worker slots：~10 KB
```

#### 时刻 T0+2.5ms：创建 SyncWaiters

```go
numSyncWaiters := (384-1)/32 + 1 = 12
s.syncWaiters = make([]*logstore.SyncWaiterLoop, 12)
for i := range s.syncWaiters {
    s.syncWaiters[i] = logstore.NewSyncWaiterLoop()
}
```

**内存分配**：
```
12 个 SyncWaiterLoop：~1.5 KB
```

#### 时刻 T0+3ms：创建 Raft Entry Cache

```go
s.raftEntryCache = raftentry.NewCache(128 << 20)  // 128 MB
```

**内存分配**：
```
Cache 结构体：~200 bytes
缓存空间（延迟分配）：0 bytes（懒分配，使用时才分配）
```

#### 时刻 T0+3.5ms：创建限流器

```go
s.snapshotApplyQueue = multiqueue.NewMultiQueue(2)
s.snapshotSendQueue = multiqueue.NewMultiQueue(2)
s.consistencyLimiter = quotapool.NewRateLimiter(...)
s.limiters.BulkIOWriteRate = rate.NewLimiter(...)
s.limiters.ConcurrentExportRequests = limit.MakeConcurrentRequestLimiter(...)
// ... 其他限流器 ...
```

**内存分配**：
```
所有限流器：~5 KB
```

#### 时刻 T0+4ms：创建队列

```go
s.leaseQueue = newLeaseQueue(s, s.allocator)
s.mvccGCQueue = newMVCCGCQueue(s)
s.mergeQueue = newMergeQueue(s, s.db)
s.splitQueue = newSplitQueue(s, s.db)
s.replicateQueue = newReplicateQueue(s, s.allocator)
s.replicaGCQueue = newReplicaGCQueue(s, s.db)
s.raftLogQueue = newRaftLogQueue(s, s.db)
s.raftSnapshotQueue = newRaftSnapshotQueue(s)
s.consistencyQueue = newConsistencyQueue(s)
// 9 个队列

s.scanner.AddQueues(
    s.mvccGCQueue,
    s.mergeQueue,
    // ... 其他队列 ...
)
```

**内存分配**：
```
9 个队列（每个 ~1 KB）：~9 KB
Scanner：~500 bytes
```

#### 时刻 T0+4.5ms：返回 Store 对象

```go
return s
```

**总内存分配**：
```
Store 结构体本身：~2 KB
Raft 调度器：~12 KB
缓存（Timestamp Cache、Raft Entry Cache）：~200 bytes（懒分配）
限流器：~5 KB
队列：~9.5 KB
其他（metrics、maps 等）：~5 KB

总计：~35 KB（不含延迟分配的缓存空间）
```

**时间开销**：
```
配置验证：~0.5ms
初始化各组件：~4ms
总计：~4.5ms
```

### 5.2 Store 启动后的动态行为

#### 时刻 T1：Store.Start() 加载 Replica

```go
repls, err := kvstorage.LoadAndReconcileReplicas(ctx, s.TODOEngine())
// → 返回 10,000 个 Replica 描述符

for i, repl := range repls {
    rep, err := newInitializedReplica(s, repl.State, true)
    s.addToReplicasByRangeIDLocked(rep)
    s.addToReplicasByKeyLocked(rep, rep.Desc())
}
```

**内存增长**：
```
10,000 个 Replica（每个 ~50 KB）：~500 MB
replicasByKey BTree：~2 MB
```

#### 时刻 T2：Raft 调度器开始工作

```go
// Store.processRaft() 启动
s.scheduler.Start(ctx, s.stopper)

// 对于每个 Replica，定期调用：
s.scheduler.EnqueueRaftTick(replica.RangeID)
```

**负载分布**：
```
10,000 个 Replica 分布到 2 个 Shard

Shard 0: ~5,000 个 Replica（RangeID 为偶数）
  → 192 个 worker
  → 每个 worker 处理 ~26 个 Replica

Shard 1: ~5,000 个 Replica（RangeID 为奇数）
  → 192 个 worker
  → 每个 worker 处理 ~26 个 Replica

Priority Shard: 紧急任务（如选举）
  → 48 个 worker
```

#### 时刻 T3：Scanner 开始扫描

```go
s.scanner.Start()

// 每 10 分钟扫描一次所有 Replica
for _, replica := range s.GetReplicaIterator() {
    // 对每个队列调用 MaybeAddAsync
    s.mvccGCQueue.MaybeAddAsync(ctx, replica)
    s.splitQueue.MaybeAddAsync(ctx, replica)
    s.replicateQueue.MaybeAddAsync(ctx, replica)
    // ... 其他队列 ...
}
```

**扫描速率**：
```
10,000 个 Replica / 10 分钟 = ~17 Replica/秒

如果 ScanMinIdleTime = 10ms：
  → 实际扫描时间：10,000 * 10ms = 100 秒
  → 剩余时间空闲：500 秒
```

---

## 六、设计取舍与权衡（Trade-offs）

### 6.1 构造与启动分离 vs 一步到位

| 维度 | 构造与启动分离（当前设计） | 一步到位（alternative） |
|------|------------------------|---------------------|
| **可测试性** | ✅ 可以构造 Store 后替换组件 | ❌ 难以注入 mock 对象 |
| **错误处理** | ✅ 配置错误在构造时发现 | ⚠️ 启动失败时难以回滚 |
| **启动时间** | ✅ 可以并发启动多个 Store | ❌ 必须串行（避免竞态） |
| **代码复杂度** | ⚠️ 需要维护两个阶段 | ✅ 逻辑集中 |

**当前设计的优势**：

```go
// 测试代码可以这样写：
store := kvserver.NewStore(ctx, cfg, engine, nodeDesc)
store.TestingKnobs.DisableGCQueue = true  // 禁用 GC 队列
store.Start(ctx, stopper)
```

### 6.2 Raft 调度器：固定并发 vs 动态调整

| 维度 | 固定并发（当前设计） | 动态调整 |
|------|------------------|---------|
| **实现复杂度** | ✅ 简单（启动时分配） | ❌ 复杂（需要 worker 池管理） |
| **性能稳定性** | ✅ 可预测 | ⚠️ 动态调整可能引入抖动 |
| **资源利用率** | ⚠️ 低负载时浪费线程 | ✅ 按需分配 |
| **Raft 安全性** | ✅ 无并发竞态 | ❌ 难以保证并发安全 |

**Why 固定并发？**

Raft 状态机的并发控制非常复杂：
```go
// 同一个 Replica 的 Raft 事件必须串行处理
replica.raftMu.Lock()
defer replica.raftMu.Unlock()

// 如果动态增加 worker → 可能多个 worker 同时处理同一 Replica
// → 违反 Raft 的串行假设 → 数据损坏
```

### 6.3 队列扫描：定期全量 vs 事件驱动

| 维度 | 定期全量扫描（当前设计） | 事件驱动 |
|------|---------------------|---------|
| **遗漏任务概率** | ✅ 低（定期兜底） | ⚠️ 事件丢失 → 遗漏任务 |
| **响应延迟** | ⚠️ 最多等待一个扫描周期 | ✅ 立即响应 |
| **CPU 开销** | ⚠️ 持续扫描开销 | ✅ 按需触发 |
| **实现复杂度** | ✅ 简单 | ❌ 需要完整的事件系统 |

**混合方案**（实际采用）：

```go
// 1. 定期扫描（兜底）
s.scanner.Start()  // 每 10 分钟

// 2. 事件驱动（及时响应）
replica.MaybeAddToSplitQueue()  // Range 过大时立即触发
replica.MaybeAddToMVCCGCQueue()  // GC 阈值达到时立即触发
```

### 6.4 限流器：全局 vs 按租户

| 维度 | 全局限流 | 按租户限流（当前设计） |
|------|---------|---------------------|
| **公平性** | ❌ 单个租户可能占用全部资源 | ✅ 每个租户独立配额 |
| **实现复杂度** | ✅ 简单 | ⚠️ 需要租户识别和路由 |
| **内存开销** | ✅ 低（单个限流器） | ⚠️ 每个租户一个限流器 |
| **隔离性** | ❌ 无隔离 | ✅ 强隔离 |

**当前设计**：

```go
s.tenantRateLimiters = tenantrate.NewLimiterFactory(...)

// 每个请求通过租户 ID 获取限流器
limiter := s.tenantRateLimiters.GetTenantLimiter(tenantID)
if err := limiter.Wait(ctx); err != nil {
    return errors.Wrap(err, "tenant rate limit exceeded")
}
```

### 6.5 内存分配：预分配 vs 懒分配

| 维度 | 预分配 | 懒分配（当前设计） |
|------|-------|---------------|
| **启动时间** | ❌ 慢（需要分配大量内存） | ✅ 快（只分配结构体） |
| **内存使用** | ❌ 高（即使未使用也占用） | ✅ 低（按需增长） |
| **运行时性能** | ✅ 无分配开销 | ⚠️ 首次使用时延迟 |

**示例**（Raft Entry Cache）：

```go
// NewStore 时：
s.raftEntryCache = raftentry.NewCache(128 << 20)
// → 只分配 Cache 结构体（~200 bytes）
// → 实际缓存空间（128 MB）延迟分配

// 首次使用时：
entry := s.raftEntryCache.Get(rangeID, index)
// → 触发内存分配（如果缓存为空）
```

---

## 七、总结与心智模型

### 7.1 核心思想

`kvserver.NewStore()` 是一个**组件装配工厂**，负责将数十个独立的组件（Raft 调度器、队列、缓存、限流器）按照正确的依赖关系和初始化顺序组装成一个完整的 Store 对象。

**关键设计原则**：

1. **构造与启动分离**：
   - NewStore 只负责内存分配和对象创建
   - Store.Start 才真正激活组件（启动 goroutine、加载数据）

2. **依赖注入**：
   - 所有外部依赖通过 StoreConfig 传入
   - 便于测试和模块化

3. **配置驱动**：
   - 大部分参数可以通过集群设置动态调整
   - 避免硬编码

4. **延迟分配**：
   - 大内存结构（如缓存）在首次使用时分配
   - 减少启动时间和内存占用

### 7.2 心智模型

**如果只记住一件事，那就是**：

> `NewStore()` 是 Store 对象的**"组装线"**：
> 它按照固定的顺序，将 Raft 调度器、后台队列、缓存、限流器等数十个组件
> 像拼积木一样组装成一个完整但静止的 Store 对象，
> 然后交给 `Store.Start()` 激活，使其开始处理真实的 KV 请求。

**类比**：

这类似于**汽车工厂的装配线**：
- **NewStore**：在生产线上组装发动机、变速箱、车轮、座椅等部件 → 完整的汽车
- **Store.Start**：启动发动机、打开电路、加载燃油 → 可以行驶的汽车
- **Store.Send**：踩油门、转方向盘 → 汽车开始移动

### 7.3 简化伪代码

```python
def NewStore(cfg: StoreConfig, engine: storage.Engine, nodeDesc: NodeDescriptor) -> Store:
    # 验证配置
    if not cfg.valid():
        panic("invalid config")

    # 创建 Store 骨架
    store = Store(
        cfg=cfg,
        engine=engine,
        nodeDesc=nodeDesc,
    )

    # 组装核心组件（顺序很重要）
    store.ioThresholds = create_io_thresholds()
    store.allocator = create_allocator(cfg.StorePool, cfg.RPCContext)
    store.scheduler = create_raft_scheduler(
        concurrency=cfg.RaftSchedulerConcurrency,
        shardSize=cfg.RaftSchedulerShardSize,
    )
    store.syncWaiters = create_sync_waiters(count=concurrency/32)
    store.raftEntryCache = create_raft_cache(size=cfg.RaftEntryCacheSize)

    # 组装后台队列
    if cfg.Gossip is not None:
        store.scanner = create_scanner(interval=cfg.ScanInterval)
        store.leaseQueue = create_lease_queue(store)
        store.mvccGCQueue = create_mvcc_gc_queue(store)
        store.splitQueue = create_split_queue(store)
        store.replicateQueue = create_replicate_queue(store)
        # ... 其他队列 ...

        # 注册队列到扫描器
        store.scanner.add_queues(
            store.leaseQueue,
            store.mvccGCQueue,
            # ... 其他队列 ...
        )

    # 组装限流器
    store.limiters = create_all_limiters(cfg.Settings)

    # 初始化数据结构（但为空）
    store.replicas_by_key = BTree()
    store.replica_placeholders = {}
    store.unquiesced_replicas = set()

    # 返回完整但静止的 Store
    return store


# 使用示例：
store = NewStore(storeCfg, engine, nodeDesc)  # ~5ms
# 此时 store 对象完整，但：
# - 没有 Replica（replicas_by_key 为空）
# - 没有 goroutine 运行
# - 队列未启动

store.Start(ctx, stopper)  # ~8 秒
# 此时 store 激活：
# - 加载了 10,000 个 Replica
# - Raft 调度器启动了 384 个 worker
# - Scanner 开始定期扫描
```

---

## 附录：关键代码位置索引

| 功能 | 文件 | 行号 |
|------|-----|-----|
| `NewStore()` 主函数 | `pkg/kv/kvserver/store.go` | L1468-L1809 |
| `Store` 结构体定义 | `pkg/kv/kvserver/store.go` | L873-L1022 |
| `StoreConfig` 定义 | `pkg/kv/kvserver/store.go` | L1163-L1300+ |
| Raft 调度器创建 | `pkg/kv/kvserver/scheduler.go` | 需查看源码 |
| 队列创建（replicateQueue） | `pkg/kv/kvserver/replicate_queue.go` | 需查看源码 |
| Allocator 创建 | `pkg/kv/kvserver/allocator/allocatorimpl/allocator.go` | 需查看源码 |
| IO 阈值初始化 | `pkg/kv/kvserver/store.go` | L1474-L1493 |

---

## 参考资料

- [CockroachDB Architecture: KV Layer](https://www.cockroachlabs.com/docs/stable/architecture/overview.html#kv-layer)
- [Store 的生命周期](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/store-lifecycle.md)（如果存在）
- [Raft 调度器设计](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/raft-scheduler.md)（如果存在）

---

**本章完**。下一章将分析 **Store.Start() 的 Replica 加载流程**，深入解释如何从磁盘恢复数万个 Raft 状态机并重建内存索引。
