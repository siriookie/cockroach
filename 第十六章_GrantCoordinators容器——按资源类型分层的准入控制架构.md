# 第十六章 GrantCoordinators 容器——按资源类型分层的准入控制架构

---

## 一、第一轮 BFS：整体职责与设计动机（Why）

### 1.1 背景与要解决的问题

在分布式数据库中，不同类型的工作负载对资源的需求和特性截然不同，单一的准入控制策略无法有效管理：

**问题 1：资源类型的异质性**

```
CPU 资源：
    ├─ 特征：可抢占，调度粒度细（微秒级）
    ├─ 过载表现：goroutine 调度延迟增加，吞吐下降
    └─ 控制手段：slot-based（KVWork）、token-based（SQL work）

IO 资源（磁盘）：
    ├─ 特征：不可抢占，操作粒度粗（毫秒级）
    ├─ 过载表现：磁盘队列堆积，L0 compaction 落后
    └─ 控制手段：token-based，基于 Pebble metrics

内存资源：
    ├─ 特征：累积性，OOM 风险高
    ├─ 过载表现：GC 频繁，最终进程被杀
    └─ 控制手段：（当前版本未实现，预留扩展点）
```

**如果使用单一协调器管理所有资源**：

```
反例：统一的 GrantCoordinator 管理 CPU + IO

时间 T0：CPU 过载（runnable goroutines 高）
    ↓
决策：减少所有 slots/tokens
    ↓
后果：
    ├─ CPU-bound 工作被正确限流 ✓
    └─ IO-bound 工作也被限流 ✗
        └─ 但此时磁盘 IO 正常（未饱和）
            └─ 浪费 IO 资源！

时间 T1：某个 Store 的磁盘过载
    ↓
决策：减少该 Store 的 IO tokens
    ↓
后果：
    ├─ 该 Store 的写入被限流 ✓
    └─ 其他 Store 的写入不受影响 ✓
        （需要独立管理）

结论：不同资源类型需要独立的控制平面
```

**问题 2：工作优先级的多样性**

```
Regular 工作（用户前台请求）：
    ├─ 特征：延迟敏感，吞吐优先
    ├─ SLO：P99 < 100ms
    └─ 不能被后台任务影响

Elastic 工作（后台任务）：
    ├─ 特征：延迟容忍，可被抢占
    ├─ 示例：Compaction、Backup、统计信息收集
    └─ 应该在 CPU 空闲时运行

如果共享准入控制：
    后台 Compaction 大量消耗 CPU
        ↓
    用户请求无法获得 CPU slots
        ↓
    P99 延迟飙升 ✗

需要隔离机制：
    RegularCPU GrantCoordinator（优先）
    ElasticCPU GrantCoordinator（次要）
```

**GrantCoordinators 的使命**：

作为顶层容器，按照**资源类型**（CPU vs IO）和**工作优先级**（Regular vs Elastic）维度，管理三个独立的协调器，实现分层准入控制。

### 1.2 在系统中的位置

**所属子系统**：Admission Control（准入控制）

**组织架构**：

```
┌─────────────────────────────────────────────────────────────┐
│                    CockroachDB Node                          │
└─────────────────────────────────────────────────────────────┘
    │
    ├─► Server 初始化（pkg/server/node.go）
    │   └─► NewGrantCoordinators()
    │       └─► 创建 GrantCoordinators 容器
    │
    ↓
┌─────────────────────────────────────────────────────────────┐
│              GrantCoordinators（容器结构体）                  │
│  源代码位置: grant_coordinator.go:82-86                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  type GrantCoordinators struct {                             │
│      RegularCPU *GrantCoordinator           // CPU 层        │
│      ElasticCPU *ElasticCPUGrantCoordinator // CPU 层（低优先）│
│      Stores     *StoreGrantCoordinators     // IO 层（每 Store）│
│  }                                                            │
└─────────────────────────────────────────────────────────────┘
    │
    ├──────────────────┬──────────────────┬──────────────────
    │                  │                  │
    ↓                  ↓                  ↓
RegularCPU      ElasticCPU         Stores
GrantCoordinator GrantCoordinator   StoreGrantCoordinators
    │                  │                  │
    │                  │                  └─► Store 1, 2, ..., N
    │                  │                      (每个 Store 独立)
    ↓                  ↓
管理 3 种 WorkKind:  管理弹性 CPU work
- KVWork            - 低优先级后台任务
- SQLKVResponseWork - 可被 RegularCPU 抢占
- SQLSQLResponseWork
```

**协作关系**：

```
外部输入：
    ├─ goschedstats: 提供 CPU 负载（runnable goroutines）
    │   └─► 每 1ms 调用 RegularCPU.CPULoad()
    │
    ├─ PebbleMetricsProvider: 提供磁盘 IO metrics
    │   └─► 每 1s 调用 Stores.pebbleMetricsTick()
    │
    └─ SchedulerLatencyListener: 提供调度器延迟
        └─► 周期性调用 ElasticCPU.SchedulerLatencyListener

内部协作：
    RegularCPU 和 ElasticCPU 之间：
        └─ 无直接交互，隔离运行
            （ElasticCPU 根据系统负载主动减少授权）

    RegularCPU 和 Stores 之间：
        └─ 写请求需要两层准入：
            1. Stores[storeID].Admit() (IO 层)
            2. RegularCPU.GetWorkQueue(KVWork).Admit() (CPU 层)
```

### 1.3 核心对象与关键状态

#### 1.3.1 GrantCoordinators（容器）

**源代码位置**：`grant_coordinator.go:82-86`

```go
type GrantCoordinators struct {
    RegularCPU *GrantCoordinator
    ElasticCPU *ElasticCPUGrantCoordinator
    Stores     *StoreGrantCoordinators
}
```

**职责**：
- **仅作为容器**：不包含任何控制逻辑
- **生命周期管理**：统一创建和关闭三个协调器
- **外部接口**：提供统一的访问入口

**关键状态**：无（纯容器，状态在子协调器中）

#### 1.3.2 RegularCPU GrantCoordinator

**源代码位置**：`grant_coordinator.go:102-167`

```go
type GrantCoordinator struct {
    ambientCtx log.AmbientContext
    settings   *cluster.Settings

    mu struct {
        syncutil.Mutex

        // Grant Chain 状态
        grantChainActive bool
        grantChainID     grantChainID
        grantChainIndex  WorkKind

        // CPU 负载监听
        cpuOverloadIndicator cpuOverloadIndicator // kvSlotAdjuster
        cpuLoadListener      CPULoadListener      // kvSlotAdjuster
        numProcs             int                  // GOMAXPROCS
    }

    // 3 个 WorkKind 的 granter-requester 对
    granters [numWorkKinds]granterWithLockedCalls
    queues   [numWorkKinds]requesterClose

    useGrantChains bool // 是否启用 Grant Chain
}
```

**职责**：
- 管理 Regular 工作的 CPU 准入
- 3 种 WorkKind：KVWork、SQLKVResponseWork、SQLSQLResponseWork
- 实现 Grant Chain 批量授权机制
- 动态调整 KVWork 的 slots（通过 kvSlotAdjuster）

**关键状态**：
- `granters[3]`：每个 WorkKind 一个 granter（slotGranter 或 tokenGranter）
- `queues[3]`：每个 WorkKind 一个 WorkQueue
- `mu.cpuLoadListener`：kvSlotAdjuster，监听 CPU 负载并调整 slots

#### 1.3.3 ElasticCPU GrantCoordinator

**源代码位置**：`elastic_cpu_grant_coordinator.go:61-65`

```go
type ElasticCPUGrantCoordinator struct {
    SchedulerLatencyListener SchedulerLatencyListener
    ElasticCPUWorkQueue      *ElasticCPUWorkQueue
    elasticCPUGranter        *elasticCPUGranter
}
```

**职责**：
- 管理 Elastic 工作的 CPU 准入
- 根据调度器延迟（scheduler latency）动态调整 token 分配
- 当系统负载高时主动减少授权（让路给 Regular 工作）

**关键状态**：
- `elasticCPUGranter`：token-based granter
- `SchedulerLatencyListener`：监听 p99 调度延迟，>1ms 时减少 tokens

#### 1.3.4 StoreGrantCoordinators

**源代码位置**：`store_grant_coordinator.go:53-102`

```go
type StoreGrantCoordinators struct {
    ambientCtx             log.AmbientContext
    settings               *cluster.Settings
    makeStoreRequesterFunc makeStoreRequesterFunc

    // 每个 StoreID 映射到一个 storeGrantCoordinator
    gcMap syncutil.Map[roachpb.StoreID, storeGrantCoordinator]

    numStores                      int
    setPebbleMetricsProviderCalled bool
    onLogEntryAdmitted             OnLogEntryAdmitted
    closeCh                        chan struct{}
}

// 每个 Store 的独立协调器
type storeGrantCoordinator struct {
    granter        *kvStoreTokenGranter  // IO token 管理
    storeReq       storeRequester        // StoreWorkQueue
    snapshotReq    snapshotRequester     // SnapshotQueue
    ioLoadListener *ioLoadListener       // IO 负载监听
}
```

**职责**：
- 管理所有 Store 的 IO 准入
- **每个 Store 独立管理**（因为每个 Store 对应独立的磁盘）
- 根据 Pebble metrics 动态调整 IO tokens
- 三层 token 检查：ioTokens、l0Tokens、diskBWTokens

**关键状态**：
- `gcMap`：StoreID → storeGrantCoordinator 映射
- 每个 `storeGrantCoordinator` 包含：
  - `kvStoreTokenGranter`：管理该 Store 的 IO tokens
  - `ioLoadListener`：监听该 Store 的 Pebble metrics

**为什么每个 Store 需要独立的协调器？**

```
原因 1: 磁盘独立性
    Node 有 3 个 Store:
        Store 1 → Disk A (SSD, 500MB/s)
        Store 2 → Disk B (SSD, 500MB/s)
        Store 3 → Disk C (HDD, 100MB/s)

    Disk C 过载 → 仅限流 Store 3
    Disk A/B 正常 → Store 1/2 继续工作

原因 2: L0 压力独立性
    Store 1: L0 sublevel count = 18 (接近上限 20)
    Store 2: L0 sublevel count = 3 (正常)

    Store 1 需要限流 → l0Tokens = 0
    Store 2 正常运行 → l0Tokens = 充足

原因 3: 避免连带影响
    如果共享 IO tokens:
        Store 1 磁盘故障 → 所有 Store 被限流 ✗

    独立管理:
        Store 1 故障 → 仅 Store 1 限流 ✓
        Store 2/3 正常 → 继续服务 ✓
```

### 1.4 三个协调器的对比总结

| 协调器            | 管理的资源       | 管理的工作类型           | Granter 类型          | Grant Chain | 动态调整机制              |
|------------------|----------------|-------------------------|----------------------|-------------|-------------------------|
| **RegularCPU**   | CPU (Regular)  | KVWork<br>SQLKVResponseWork<br>SQLSQLResponseWork | slotGranter<br>tokenGranter | ✓ 启用      | kvSlotAdjuster<br>(基于 CPU 负载) |
| **ElasticCPU**   | CPU (Elastic)  | ElasticWork             | elasticCPUGranter<br>(token-based) | ✗ 禁用      | SchedulerLatencyListener<br>(基于调度延迟) |
| **Stores[i]**    | IO (Store i)   | RegularWorkClass<br>ElasticWorkClass | kvStoreTokenGranter  | ✗ 禁用      | ioLoadListener<br>(基于 Pebble metrics) |

**设计理念总结**：

```
维度 1: 资源类型隔离
    CPU 资源 → RegularCPU + ElasticCPU
    IO 资源  → Stores (每个 Store 独立)

维度 2: 工作优先级隔离
    Regular 工作 → RegularCPU (高优先)
    Elastic 工作 → ElasticCPU (低优先)

维度 3: 资源实例隔离
    每个磁盘独立 → Stores[storeID] (互不影响)
```

---

## 二、第二轮 BFS：控制流与交互关系（How it flows）

### 2.1 初始化流程

**源代码位置**：`grant_coordinator.go:285-305`

```go
// NewGrantCoordinators 在节点启动时被调用
func NewGrantCoordinators(
    ambientCtx log.AmbientContext,
    st *cluster.Settings,
    opts Options,
    registry *metric.Registry,
    onLogEntryAdmitted OnLogEntryAdmitted,
    knobs *TestingKnobs,
) GrantCoordinators {
    metrics := makeGrantCoordinatorMetrics()
    registry.AddMetricStruct(metrics)

    if knobs == nil {
        knobs = &TestingKnobs{}
    }

    return GrantCoordinators{
        // 创建三个独立的协调器
        Stores:     makeStoresGrantCoordinators(ambientCtx, opts, st, onLogEntryAdmitted, knobs),
        RegularCPU: makeRegularGrantCoordinator(ambientCtx, opts, st, metrics, registry, knobs),
        ElasticCPU: makeElasticCPUGrantCoordinator(ambientCtx, st, registry),
    }
}
```

**初始化时间线**：

```
T=0: Server 启动
    ↓
pkg/server/node.go: Node.Start()
    ↓
创建 GrantCoordinators
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 步骤 1: 创建 StoreGrantCoordinators                          │
└─────────────────────────────────────────────────────────────┘
    ↓
makeStoresGrantCoordinators()
    ├─ 创建空的 gcMap (稍后动态添加 Store)
    └─ 返回 *StoreGrantCoordinators

┌─────────────────────────────────────────────────────────────┐
│ 步骤 2: 创建 RegularCPU GrantCoordinator                     │
└─────────────────────────────────────────────────────────────┘
    ↓
makeRegularGrantCoordinator()
    ├─ 创建 kvSlotAdjuster (CPU 负载监听器)
    │   ├─ minCPUSlots = 1
    │   ├─ maxCPUSlots = 100,000
    │   └─ threshold = 32 (每核可运行 goroutine 阈值)
    │
    ├─ 创建 GrantCoordinator
    │   ├─ useGrantChains = true
    │   ├─ mu.cpuLoadListener = kvSlotAdjuster
    │   └─ mu.cpuOverloadIndicator = kvSlotAdjuster
    │
    ├─ 为 3 个 WorkKind 创建 granter-requester 对:
    │   ├─ KVWork:
    │   │   ├─ granter = slotGranter (slot-based)
    │   │   └─ requester = WorkQueue
    │   │
    │   ├─ SQLKVResponseWork:
    │   │   ├─ granter = tokenGranter (token-based)
    │   │   └─ requester = WorkQueue
    │   │
    │   └─ SQLSQLResponseWork:
    │       ├─ granter = tokenGranter (token-based)
    │       └─ requester = WorkQueue
    │
    └─ 返回 *GrantCoordinator

┌─────────────────────────────────────────────────────────────┐
│ 步骤 3: 创建 ElasticCPU GrantCoordinator                     │
└─────────────────────────────────────────────────────────────┘
    ↓
makeElasticCPUGrantCoordinator()
    ├─ 创建 elasticCPUGranter (token-based)
    ├─ 创建 schedulerLatencyListener
    │   └─ 监听 p99 调度延迟
    │
    ├─ 创建 ElasticCPUWorkQueue
    └─ 返回 *ElasticCPUGrantCoordinator

T=100ms: goschedstats 开始工作
    ↓
每 1ms 调用 RegularCPU.CPULoad(runnable, procs, 1ms)
    ├─ 传递给 kvSlotAdjuster
    └─ 动态调整 KVWork 的 totalSlots

T=1s: Stores 开始接收 Pebble metrics
    ↓
SetPebbleMetricsProvider(pebbleMetricsProvider)
    ↓
为每个 Store 创建 storeGrantCoordinator
    ├─ Store 1 → gcMap[1] = storeGrantCoordinator{...}
    ├─ Store 2 → gcMap[2] = storeGrantCoordinator{...}
    └─ Store N → gcMap[N] = storeGrantCoordinator{...}
    ↓
启动后台 goroutine，每 1s:
    ├─ 调用 pebbleMetricsTick() (更新 L0 metrics)
    └─ 调用 allocateIOTokensTick() (补充 IO tokens)
```

### 2.2 请求路径的触发时机

#### 2.2.1 RegularCPU：请求路径触发

**KVWork 读请求（仅 CPU 层）**：

```
T=0: 用户请求到达
    ↓
KV 层处理
    ↓
调用 RegularCPU.GetWorkQueue(KVWork).Admit(ctx, info)
    ↓
┌─────────────────────────────────────────────────────────────┐
│ WorkQueue.Admit() 内部流程                                    │
└─────────────────────────────────────────────────────────────┘
    ↓
检查队列是否为空?
    ├─► 是 (快速路径)
    │   ↓
    │   granter.tryGet(1)
    │   ├─ slotGranter.tryGet()
    │   ├─ GrantCoordinator.tryGet(KVWork, 1)
    │   └─ slotGranter.tryGetLocked(1)
    │       ├─ 检查: usedSlots < totalSlots?
    │       └─ 成功 → return (true, nil)
    │   ↓
    │   立即执行请求 (无等待)
    │
    └─► 否 (慢速路径)
        ↓
        创建 waitingWork，加入队列
        ↓
        阻塞在 channel，等待授权通知
```

**KVWork 写请求（双重准入：IO + CPU）**：

```
T=0: 用户写入请求到达
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 第一层：Store 层 IO 准入                                      │
└─────────────────────────────────────────────────────────────┘
    ↓
确定写入的 StoreID (例如: storeID=5)
    ↓
调用 Stores.TryGetQueueForStore(5).Admit(ctx, storeInfo)
    ↓
StoreWorkQueue.Admit()
    ├─ workClass = RegularWorkClass
    ├─ granter = kvStoreTokenChildGranter
    └─ kvStoreTokenGranter.tryGet()
        ├─ 检查 1: ioTokens >= requestedBytes?
        ├─ 检查 2: l0Tokens > 0? (L0 压力)
        └─ 检查 3: diskBWTokens > 0? (磁盘带宽)
    ↓
成功 → 获得 IO 准入
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 第二层：CPU 层准入                                            │
└─────────────────────────────────────────────────────────────┘
    ↓
调用 RegularCPU.GetWorkQueue(KVWork).Admit(ctx, info)
    ↓
WorkQueue.Admit()
    ├─ granter = slotGranter
    └─ tryGet(1)
        └─ 检查: usedSlots < totalSlots?
    ↓
成功 → 获得 CPU 准入
    ↓
执行 Raft 提议
    ↓
写入完成
    ↓
归还资源：
    ├─ StoreWorkQueue.StoreWriteDone() → 归还 IO tokens
    └─ WorkQueue.AdmittedWorkDone() → 归还 CPU slot
```

**关键观察**：
- **顺序很重要**：先 IO 层，再 CPU 层
- **原因**：避免占用 CPU slot 后发现 IO 不足，导致 CPU slot 被浪费地阻塞在 IO 等待上

#### 2.2.2 ElasticCPU：后台任务触发

```
T=0: 后台任务（如 Compaction）需要执行
    ↓
调用 ElasticCPU.ElasticCPUWorkQueue.Admit(ctx, info)
    ↓
ElasticCPUWorkQueue.Admit()
    ├─ granter = elasticCPUGranter
    └─ tryGet(tokens)
        ├─ 检查 1: elasticTokens > 0?
        ├─ 检查 2: 调度延迟 < 阈值? (间接检查)
        └─ 成功 → 返回 true
    ↓
执行后台任务
    ↓
完成后归还 tokens
```

**与 RegularCPU 的隔离**：

```
场景: Regular 工作负载高

RegularCPU:
    ├─ CPU 负载高 (runnable=300)
    ├─ kvSlotAdjuster 减少 slots
    └─ 用户请求获得优先保障 ✓

ElasticCPU:
    ├─ schedulerLatencyListener 检测到 p99 > 1ms
    ├─ 主动减少 elasticTokens
    └─ 后台任务被限流 ✓
    └─ 为 Regular 工作让路

结果: 两个协调器独立决策，但共同保护 CPU
```

#### 2.2.3 Stores：定时触发 + 请求触发

**定时触发（后台 goroutine）**：

```
StoreGrantCoordinators.SetPebbleMetricsProvider() 后启动:

每 1 秒循环:
    ↓
pebbleMetricsProvider.GetPebbleMetrics()
    ├─ 返回所有 Store 的 metrics
    └─ [Store1Metrics, Store2Metrics, ...]
    ↓
for each Store:
    ↓
    storeGrantCoordinator.pebbleMetricsTick(metrics)
        ├─ 更新 L0 sublevel count
        ├─ 更新磁盘 write bytes
        ├─ 计算 L0 tokens
        └─ 计算 disk bandwidth tokens
    ↓
    storeGrantCoordinator.allocateIOTokensTick(ticks)
        ├─ 补充 ioTokens
        ├─ ioTokens += (bandwidth * interval)
        └─ 触发 tryGrant() (如果有等待请求)
```

**请求触发（写入路径）**：

```
T=0: 写入请求到达
    ↓
StoreWorkQueue.Admit(ctx, storeInfo)
    ↓
tryGet() → 消耗 IO tokens
    ↓
完成后:
    ↓
StoreWorkQueue.StoreWriteDone(doneInfo)
    ↓
returnGrant() → 归还 IO tokens
    ↓
tryGrant() → 尝试授权等待的请求
```

### 2.3 三个协调器的交互矩阵

| 交互方向                      | 是否存在? | 交互方式                              | 目的                    |
|------------------------------|----------|--------------------------------------|------------------------|
| RegularCPU ← goschedstats    | ✓        | CPULoad(runnable, procs, period)    | 动态调整 CPU slots      |
| ElasticCPU ← schedulerLatency| ✓        | SchedulerLatencyListener.report()   | 根据延迟调整 tokens     |
| Stores ← PebbleMetrics       | ✓        | pebbleMetricsTick(metrics)          | 动态调整 IO tokens      |
| RegularCPU ↔ ElasticCPU      | ✗        | 无直接交互                            | 隔离运行               |
| RegularCPU → Stores          | ✓        | 写请求先 Store 层，再 CPU 层          | 双重准入               |
| ElasticCPU → Stores          | ✓        | 弹性写请求也需要 IO 准入              | 双重准入               |
| Stores[i] ↔ Stores[j]        | ✗        | 每个 Store 独立                       | 资源隔离               |

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 NewGrantCoordinators()：容器构造函数

**源代码位置**：`grant_coordinator.go:285-305`

**函数签名**：

```go
func NewGrantCoordinators(
    ambientCtx log.AmbientContext,
    st *cluster.Settings,
    opts Options,                    // 配置参数
    registry *metric.Registry,       // metrics 注册器
    onLogEntryAdmitted OnLogEntryAdmitted,  // Raft 日志准入回调
    knobs *TestingKnobs,             // 测试 knobs
) GrantCoordinators
```

**输入**：
- `opts Options`：包含初始配置（MinCPUSlots、MaxCPUSlots、burst tokens 等）
- `registry *metric.Registry`：用于注册 Prometheus metrics
- `onLogEntryAdmitted`：当 Raft 日志被准入时的回调

**输出**：
- `GrantCoordinators` 结构体，包含三个初始化完成的协调器

**不变量（Invariants）**：
```
Invariant 1: 三个协调器必须全部成功创建
    → 如果任何一个失败，整个函数应该 panic（当前实现中未显式检查）

Invariant 2: metrics 必须正确注册
    → registry.AddMetricStruct(metrics) 必须在协调器创建前调用

Invariant 3: knobs 不能为 nil
    → if knobs == nil { knobs = &TestingKnobs{} }
```

**完整实现分析**：

```go
func NewGrantCoordinators(...) GrantCoordinators {
    // ============================================================
    // 阶段 1: 创建共享 metrics
    // ============================================================

    metrics := makeGrantCoordinatorMetrics()
    // 创建 GrantCoordinatorMetrics 结构体，包含:
    //   - KVTotalSlots (Gauge)
    //   - KVUsedSlots (Gauge)
    //   - KVSlotsExhaustedDuration (Counter)
    //   - KVCPULoadShortPeriodDuration (Counter)
    //   - ... (共约 20 个 metrics)

    registry.AddMetricStruct(metrics)
    // 注册到 Prometheus，外部可以通过 HTTP /metrics 端点访问

    // ============================================================
    // 阶段 2: 处理 testing knobs
    // ============================================================

    if knobs == nil {
        knobs = &TestingKnobs{}
    }
    // 确保 knobs 不为 nil，避免后续 nil pointer dereference

    // ============================================================
    // 阶段 3: 创建三个协调器（顺序无关紧要）
    // ============================================================

    return GrantCoordinators{
        // 3.1 创建 StoreGrantCoordinators
        Stores: makeStoresGrantCoordinators(
            ambientCtx, opts, st, onLogEntryAdmitted, knobs),
        // 返回 *StoreGrantCoordinators
        // 此时 gcMap 为空，稍后通过 SetPebbleMetricsProvider 添加 Store

        // 3.2 创建 RegularCPU GrantCoordinator
        RegularCPU: makeRegularGrantCoordinator(
            ambientCtx, opts, st, metrics, registry, knobs),
        // 返回 *GrantCoordinator
        // 包含 3 个 WorkKind 的 granter-requester 对
        // kvSlotAdjuster 已绑定

        // 3.3 创建 ElasticCPU GrantCoordinator
        ElasticCPU: makeElasticCPUGrantCoordinator(
            ambientCtx, st, registry),
        // 返回 *ElasticCPUGrantCoordinator
        // 包含 elasticCPUGranter 和 schedulerLatencyListener
    }
}
```

**为什么这个顺序？**

```
问题: 为什么先创建 Stores，再创建 RegularCPU？

答案: 顺序实际上不重要，因为三个协调器完全独立

但从逻辑上:
    Stores 是最底层（硬件资源）
        ↓
    RegularCPU 是中间层（计算资源）
        ↓
    ElasticCPU 是最上层（可选资源）

这样的顺序更符合资源层次的心智模型
```

### 3.2 makeRegularGrantCoordinator()：RegularCPU 构造

**源代码位置**：`grant_coordinator.go:307-387`

**函数签名**：

```go
func makeRegularGrantCoordinator(
    ambientCtx log.AmbientContext,
    opts Options,
    st *cluster.Settings,
    metrics GrantCoordinatorMetrics,
    registry *metric.Registry,
    knobs *TestingKnobs,
) *GrantCoordinator
```

**核心逻辑剖析**：

```go
func makeRegularGrantCoordinator(...) *GrantCoordinator {
    // ============================================================
    // 步骤 1: 创建 kvSlotAdjuster (CPU 负载监听器)
    // ============================================================

    kvSlotAdjuster := &kvSlotAdjuster{
        settings:    st,
        minCPUSlots: opts.MinCPUSlots,  // 默认 1
        maxCPUSlots: opts.MaxCPUSlots,  // 默认 100,000

        // 绑定 metrics
        totalSlotsMetric:                 metrics.KVTotalSlots,
        cpuLoadShortPeriodDurationMetric: metrics.KVCPULoadShortPeriodDuration,
        cpuLoadLongPeriodDurationMetric:  metrics.KVCPULoadLongPeriodDuration,
        slotAdjusterIncrementsMetric:     metrics.KVSlotAdjusterIncrements,
        slotAdjusterDecrementsMetric:     metrics.KVSlotAdjusterDecrements,
    }
    // kvSlotAdjuster 实现两个接口:
    //   1. CPULoadListener: 接收 CPULoad(runnable, procs, period) 调用
    //   2. cpuOverloadIndicator: 提供 isOverloaded() 方法

    // ============================================================
    // 步骤 2: 创建 GrantCoordinator 框架
    // ============================================================

    coord := &GrantCoordinator{
        ambientCtx:     ambientCtx,
        settings:       st,
        useGrantChains: true,  // ← 关键！启用 Grant Chain
        testingDisableSkipEnforcement: opts.TestingDisableSkipEnforcement,
        knobs:          knobs,
    }

    coord.mu.grantChainID = 1  // 初始 chain ID
    coord.mu.cpuOverloadIndicator = kvSlotAdjuster  // 绑定 CPU 过载检测器
    coord.mu.cpuLoadListener = kvSlotAdjuster        // 绑定 CPU 负载监听器
    coord.mu.numProcs = 1  // 初始值，稍后由 CPULoad 更新

    // ============================================================
    // 步骤 3: 为 KVWork 创建 slotGranter + WorkQueue
    // ============================================================

    kvg := &slotGranter{
        coord:      coord,
        workKind:   KVWork,
        totalSlots: opts.MinCPUSlots,  // 初始值（动态调整）
        skipSlotEnforcement: !goschedstats.Supported,
        // 如果平台不支持 goschedstats，跳过 slot 强制执行

        usedSlotsMetric:              metrics.KVUsedSlots,
        slotsExhaustedDurationMetric: metrics.KVSlotsExhaustedDuration,
    }

    kvSlotAdjuster.granter = kvg  // 双向绑定
    // kvSlotAdjuster 需要通过 granter 调整 totalSlots

    wqMetrics := makeWorkQueueMetrics(
        KVWork.String(), registry,
        admissionpb.NormalPri, admissionpb.LockingNormalPri)

    req := makeRequester(
        ambientCtx, KVWork, kvg, st, wqMetrics,
        makeWorkQueueOptions(KVWork))
    // makeRequester 实际上是 makeWorkQueue，返回 *WorkQueue

    coord.queues[KVWork] = req       // 存储 requester
    kvg.requester = req              // 双向绑定
    coord.granters[KVWork] = kvg     // 存储 granter

    // ============================================================
    // 步骤 4: 为 SQLKVResponseWork 创建 tokenGranter + WorkQueue
    // ============================================================

    tg := &tokenGranter{
        coord:                coord,
        workKind:             SQLKVResponseWork,
        availableBurstTokens: opts.SQLKVResponseBurstTokens,  // 默认 100,000
        maxBurstTokens:       opts.SQLKVResponseBurstTokens,
        cpuOverload:          kvSlotAdjuster,  // 复用 kvSlotAdjuster
    }
    // tokenGranter 与 slotGranter 的区别:
    //   - tokenGranter: 批量分配 tokens（count 可以 > 1）
    //   - slotGranter: 单个分配 slots（count 总是 = 1）

    wqMetrics = makeWorkQueueMetrics(
        SQLKVResponseWork.String(), registry,
        admissionpb.NormalPri, admissionpb.LockingNormalPri)

    req = makeRequester(
        ambientCtx, SQLKVResponseWork, tg, st, wqMetrics,
        makeWorkQueueOptions(SQLKVResponseWork))

    coord.queues[SQLKVResponseWork] = req
    tg.requester = req
    coord.granters[SQLKVResponseWork] = tg

    // ============================================================
    // 步骤 5: 为 SQLSQLResponseWork 创建 tokenGranter + WorkQueue
    // ============================================================

    tg = &tokenGranter{
        coord:                coord,
        workKind:             SQLSQLResponseWork,
        availableBurstTokens: opts.SQLSQLResponseBurstTokens,  // 默认 100,000
        maxBurstTokens:       opts.SQLSQLResponseBurstTokens,
        cpuOverload:          kvSlotAdjuster,  // 复用 kvSlotAdjuster
    }

    wqMetrics = makeWorkQueueMetrics(
        SQLSQLResponseWork.String(), registry,
        admissionpb.NormalPri, admissionpb.LockingNormalPri)

    req = makeRequester(
        ambientCtx, SQLSQLResponseWork, tg, st, wqMetrics,
        makeWorkQueueOptions(SQLSQLResponseWork))

    coord.queues[SQLSQLResponseWork] = req
    tg.requester = req
    coord.granters[SQLSQLResponseWork] = tg

    // ============================================================
    // 返回完整的 GrantCoordinator
    // ============================================================

    return coord
}
```

**关键设计决策**：

**决策 1：为什么 KVWork 使用 slotGranter，SQL work 使用 tokenGranter？**

```
KVWork 特征:
    ├─ 每个请求消耗固定的 CPU 时间（相对均匀）
    ├─ 并发度可控（goroutine 数量 = slot 数量）
    └─ 适合 slot-based 控制

SQL work 特征:
    ├─ 每个请求消耗的 CPU 时间差异巨大
    │   └─ 简单查询: 1ms，复杂查询: 1s
    ├─ 并发度难以预测
    └─ 适合 token-based 控制（按实际 CPU 时间计费）

设计原理:
    slot-based: 粗粒度，快速，适合均匀负载
    token-based: 细粒度，公平，适合异构负载
```

**决策 2：为什么三个 WorkKind 共享一个 kvSlotAdjuster？**

```
原因: CPU 是共享资源

如果独立:
    KVWork 的 kvSlotAdjuster 说: CPU 过载，减少 slots
    SQL work 的 adjuster 说: CPU 正常，增加 tokens
    → 冲突！

共享设计:
    kvSlotAdjuster 监听全局 CPU 负载
        ↓
    所有 WorkKind 共同响应:
        ├─ KVWork: 减少 slots
        ├─ SQLKVResponseWork: 减少 tokens (通过 cpuOverload.isOverloaded())
        └─ SQLSQLResponseWork: 减少 tokens
    → 一致性 ✓
```

### 3.3 makeStoresGrantCoordinators()：Stores 容器构造

**源代码位置**：`store_grant_coordinator.go:29-48`

```go
func makeStoresGrantCoordinators(
    ambientCtx log.AmbientContext,
    opts Options,
    st *cluster.Settings,
    onLogEntryAdmitted OnLogEntryAdmitted,
    knobs *TestingKnobs,
) *StoreGrantCoordinators {
    makeStoreRequester := makeStoreWorkQueue
    if opts.makeStoreRequesterFunc != nil {
        makeStoreRequester = opts.makeStoreRequesterFunc  // 测试注入
    }

    storeCoordinators := &StoreGrantCoordinators{
        ambientCtx:             ambientCtx,
        settings:               st,
        makeStoreRequesterFunc: makeStoreRequester,
        onLogEntryAdmitted:     onLogEntryAdmitted,
        knobs:                  knobs,
        // gcMap: 空的 Map，稍后动态添加
    }
    return storeCoordinators
}
```

**关键观察**：
- **延迟初始化**：此时 `gcMap` 为空
- **原因**：Store 的数量和 ID 在节点启动时未知
- **实际初始化时机**：`SetPebbleMetricsProvider()` 被调用时

**SetPebbleMetricsProvider() 流程**：

**源代码位置**：`store_grant_coordinator.go:122-227`

```go
func (sgc *StoreGrantCoordinators) SetPebbleMetricsProvider(
    startupCtx context.Context,
    pmp PebbleMetricsProvider,
    mrp MetricsRegistryProvider,
    iotc IOThresholdConsumer,
) {
    // ============================================================
    // 阶段 1: 防止重复调用
    // ============================================================

    if sgc.setPebbleMetricsProviderCalled {
        panic(errors.AssertionFailedf(
            "SetPebbleMetricsProvider called more than once"))
    }
    sgc.setPebbleMetricsProviderCalled = true

    // ============================================================
    // 阶段 2: 初始化每个 Store 的协调器
    // ============================================================

    pebbleMetricsProvider := pmp
    sgc.closeCh = make(chan struct{})

    metrics := pebbleMetricsProvider.GetPebbleMetrics()
    // 返回 []StoreMetrics，例如:
    // [
    //   {StoreID: 1, L0NumSublevels: 3, ...},
    //   {StoreID: 2, L0NumSublevels: 5, ...},
    // ]

    for _, m := range metrics {
        // 为每个 Store 创建独立的 storeGrantCoordinator
        gc := sgc.initGrantCoordinator(
            m.StoreID, mrp.GetMetricsRegistry(m.StoreID))
        // initGrantCoordinator 内部:
        //   1. 创建 kvStoreTokenGranter
        //   2. 创建 StoreWorkQueue (Regular + Elastic)
        //   3. 创建 SnapshotQueue
        //   4. 创建 ioLoadListener

        // 存储到 gcMap (线程安全的 Map)
        _, loaded := sgc.gcMap.LoadOrStore(m.StoreID, gc)
        if !loaded {
            sgc.numStores++
        }

        // 初始调用，设置初始状态
        gc.pebbleMetricsTick(startupCtx, m)
        gc.allocateIOTokensTick(
            unloadedDuration.ticksInAdjustmentInterval())
    }

    if sgc.disableTickerForTesting {
        return  // 测试模式，不启动后台 goroutine
    }

    // ============================================================
    // 阶段 3: 启动后台 goroutine（定时更新）
    // ============================================================

    go func() {
        // 创建 ticker，每 1 秒 tick 一次
        t := newTicker(sgc.settings, sgc.closeCh)
        systemLoaded := false
        remainingTicks := t.remainingTicks()

        done := false
        for !done {
            select {
            case <-t.ch:
                // ========== 每 1 秒执行一次 ==========

                remainingTicks--
                if remainingTicks == 0 {
                    // 调整周期结束，开始新周期

                    // 1. 获取最新的 Pebble metrics
                    metrics := pebbleMetricsProvider.GetPebbleMetrics()

                    // 2. 更新每个 Store 的 IO 状态
                    for _, m := range metrics {
                        if gc, ok := sgc.gcMap.Load(m.StoreID); ok {
                            gc.pebbleMetricsTick(ctx, m)
                            // 更新:
                            //   - L0 sublevel count
                            //   - Write bytes
                            //   - Compacted bytes
                            //   → 计算 l0Tokens, diskBWTokens

                            // 通知 IOThresholdConsumer
                            iotc.UpdateIOThreshold(
                                m.StoreID, gc.ioLoadListener.ioThreshold)
                        }
                    }

                    // 开始新的调整周期
                    t.adjustmentStart(systemLoaded)
                    remainingTicks = t.remainingTicks()
                }

                // 3. 为每个 Store 分配 IO tokens
                sgc.gcMap.Range(func(_ roachpb.StoreID,
                                     gc *storeGrantCoordinator) bool {
                    gc.allocateIOTokensTick(int64(remainingTicks))
                    // 补充 ioTokens:
                    //   ioTokens += (bandwidth * tickInterval)

                    // 尝试授权等待的请求
                    gc.granter.tryGrant()

                    return true  // 继续遍历下一个 Store
                })

            case <-sgc.closeCh:
                // 收到关闭信号，退出 goroutine
                done = true
                pebbleMetricsProvider.Close()
            }
        }
        t.stop()
    }()
}
```

**为什么需要后台 goroutine？**

```
问题: RegularCPU 由 goschedstats 驱动（被动），为什么 Stores 需要主动 goroutine？

答案: IO 资源的补充模型不同

RegularCPU (slot-based):
    slots 不需要"补充"
    ├─ usedSlots-- 时，slot 立即可用
    └─ 由请求完成驱动（被动）

Stores (token-based):
    tokens 需要周期性"补充"
    ├─ tokens 代表带宽额度
    ├─ 需要根据实际磁盘性能定期补充
    └─ 不能由单个请求驱动（会导致突发）

示例:
    磁盘带宽 = 100MB/s
    每 1 秒补充 100MB 的 tokens

    如果由请求驱动:
        T=0: 100 个请求同时完成，补充 100MB tokens
        T=0.1: 100 个请求同时到达，瞬间消耗 100MB
        → 磁盘瞬时负载 1000MB/s ✗ (超过物理带宽)

    定期补充:
        每 1 秒稳定补充 100MB
        → 平滑控制磁盘带宽 ✓
```

### 3.4 三个协调器的 Close() 流程

**源代码位置**：`grant_coordinator.go:88-93`

```go
func (gcs GrantCoordinators) Close() {
    gcs.Stores.close()
    gcs.RegularCPU.Close()
    gcs.ElasticCPU.close()
}
```

**关闭顺序的重要性**：

```
顺序: Stores → RegularCPU → ElasticCPU

原因:
    Stores:
        ├─ 需要先停止后台 goroutine (closeCh <- struct{}{})
        └─ 避免在 RegularCPU 关闭后仍尝试授权

    RegularCPU:
        ├─ 关闭 3 个 WorkQueue
        └─ 停止接收新请求

    ElasticCPU:
        └─ 最后关闭（优先级最低，影响最小）

如果顺序错误:
    先关闭 RegularCPU → Stores 的后台 goroutine 仍运行
        ↓
    Stores.tryGrant() 尝试通知 WorkQueue
        ↓
    WorkQueue 已关闭 → panic ✗
```

---

## 四、动态行为分析（Runtime 行为）

### 4.1 RegularCPU 的 CPU 负载响应

**信号来源**：goschedstats，每 1ms 调用 `RegularCPU.CPULoad()`

**调用链**：

```
goschedstats.Collector.tick()
    ↓
coord.CPULoad(runnable, procs, samplePeriod)
    ↓
coord.mu.cpuLoadListener.CPULoad(runnable, procs, samplePeriod)
    ↓
kvSlotAdjuster.CPULoad(runnable, procs, samplePeriod)
    ↓
决策: 增加/保持/减少 totalSlots
    ↓
slotGranter.setTotalSlotsLocked(newTotal)
```

**决策逻辑（详见第十五章）**：

```
threshold = 32 (每核允许的可运行 goroutine 数)
procs = 8
overloadThreshold = 32 * 8 = 256
underloadThreshold = 32 * 8 / 2 = 128

if runnable >= 256:
    → CPU 过载 → totalSlots--

else if runnable <= 128:
    → CPU 空闲 → totalSlots++

else:
    → 正常负载 → 保持不变
```

**对三个 WorkKind 的影响**：

```
KVWork:
    ├─ 直接影响: totalSlots 变化
    └─ slotGranter.tryGetLocked() 使用最新的 totalSlots

SQLKVResponseWork:
    ├─ 间接影响: tokenGranter.cpuOverload.isOverloaded()
    └─ 如果 CPU 过载 → 减少 token 授权频率

SQLSQLResponseWork:
    ├─ 间接影响: 同 SQLKVResponseWork
    └─ 最低优先级，受影响最大
```

### 4.2 ElasticCPU 的调度延迟响应

**信号来源**：schedulerLatencyListener，监听 p99 goroutine 调度延迟

**响应机制**：

```
schedulerLatencyListener.report(p99Latency)
    ↓
if p99Latency > 1ms:
    → 系统负载高
    ↓
    elasticCPUGranter.reduceTokens()
    ├─ availableTokens *= 0.9  (指数衰减)
    └─ 后台任务被限流

else if p99Latency < 500μs:
    → 系统空闲
    ↓
    elasticCPUGranter.increaseTokens()
    ├─ availableTokens *= 1.1  (指数增长)
    └─ 后台任务加速
```

**为什么使用调度延迟而非 CPU 负载？**

```
原因: Elastic 工作的目标不同

RegularCPU:
    目标: 防止 CPU 过载
    指标: runnable goroutines (直接反映 CPU 压力)

ElasticCPU:
    目标: 不影响 Regular 工作的响应时间
    指标: 调度延迟 (直接反映用户感知的延迟)

示例:
    runnable = 100 (不算高)
    但 p99 调度延迟 = 5ms (很高)

    原因: Elastic 工作虽然 goroutine 数量不多，
         但每个 goroutine 执行时间很长（如 Compaction）
         → 阻塞了 Regular 工作的调度

    响应: ElasticCPU 主动减少 tokens
         → 让出 CPU 给 Regular 工作
```

### 4.3 Stores 的 IO 负载响应

**信号来源 1：pebbleMetricsTick（每 1 秒）**

```
pebbleMetricsTick(metrics)
    ↓
ioLoadListener.pebbleMetricsTick(metrics)
    ↓
更新三个关键指标:
    1. L0 sublevel count
       ├─ 正常: < 10
       ├─ 警告: 10-20
       └─ 危险: > 20

    2. Disk write bandwidth
       ├─ 计算: (currentWriteBytes - prevWriteBytes) / interval
       └─ 平滑: 使用 EWMA (指数加权移动平均)

    3. Compaction debt
       └─ debt = (累积写入) - (累积 compaction)
    ↓
计算 tokens:
    l0Tokens = f(L0 sublevel count)
        ├─ 正常 (< 10): l0Tokens = 充足
        ├─ 警告 (10-20): l0Tokens = 逐渐减少
        └─ 危险 (> 20): l0Tokens = 0 (停止准入)

    diskBWTokens = f(disk bandwidth utilization)
        ├─ 低 (< 50%): diskBWTokens = 充足
        ├─ 中 (50-80%): diskBWTokens = 逐渐减少
        └─ 高 (> 80%): diskBWTokens = 严格限制
```

**信号来源 2：allocateIOTokensTick（每 1 秒，分多次 tick）**

```
allocateIOTokensTick(remainingTicks)
    ↓
kvStoreTokenGranter.allocateIOTokensTick(ticks)
    ↓
补充 ioTokens:
    bandwidth = estimatedDiskBandwidth  // 从 Pebble metrics 估算
    tickInterval = adjustmentInterval / totalTicks  // 1s / 100 = 10ms

    tokensToAdd = bandwidth * tickInterval / totalTicks
    ioTokens += tokensToAdd
    ↓
触发授权:
    if ioTokens > 0 && hasWaitingRequests():
        tryGrant()
```

**三层 Token 检查机制**：

**源代码位置**：`granter.go:428-565` (kvStoreTokenGranter.tryGet)

```go
func (sg *kvStoreTokenGranter) tryGet(
    wc admissionpb.WorkClass,
    info StoreWorkInfo,
) bool {
    requested := info.WriteBytes

    if wc == admissionpb.RegularWorkClass {
        // ========== Regular 工作：单层检查 ==========

        if sg.ioTokens >= requested {
            sg.ioTokens -= requested
            return true  // ✓ 授权
        }
        return false  // ✗ IO tokens 不足

    } else {
        // ========== Elastic 工作：三层检查 ==========

        // 检查 1: L0 压力
        if sg.l0Tokens <= 0 {
            return false  // ✗ L0 压力过大，拒绝 Elastic 工作
        }

        // 检查 2: 磁盘带宽
        if sg.diskBWTokens <= 0 {
            return false  // ✗ 磁盘带宽饱和，拒绝 Elastic 工作
        }

        // 检查 3: IO tokens
        if sg.ioTokens >= requested {
            // 同时消耗三种 tokens
            sg.ioTokens -= requested
            sg.l0Tokens -= requested
            sg.diskBWTokens -= requested
            return true  // ✓ 授权
        }

        return false  // ✗ IO tokens 不足
    }
}
```

**为什么 Regular 和 Elastic 的检查不同？**

```
设计原则: 保护 Regular 工作，牺牲 Elastic 工作

Regular 工作 (用户请求):
    ├─ 仅检查 ioTokens
    ├─ 允许在 L0 压力高时继续执行
    │   └─ 短期 L0 压力可以容忍
    └─ 优先保证用户体验

Elastic 工作 (后台任务):
    ├─ 检查 ioTokens、l0Tokens、diskBWTokens
    ├─ L0 压力高时拒绝
    │   └─ 避免进一步恶化 LSM 树健康
    └─ 可以延迟执行

示例场景:
    L0 sublevel count = 18 (接近上限)

    Regular write:
        ioTokens = 1MB > requestedBytes ✓
        → 授权成功
        → 允许用户写入（即使 L0 高）

    Elastic write (Compaction):
        l0Tokens = 0 ✗
        → 拒绝
        → 暂停 Compaction，等待 L0 恢复
```

### 4.4 三个协调器的协同行为

**场景 1：CPU 过载 + IO 正常**

```
T=0: 系统状态
    CPU: runnable=300 (过载)
    Store 1 IO: 正常
    Store 2 IO: 正常

T=1ms: RegularCPU 响应
    kvSlotAdjuster.CPULoad(runnable=300, procs=8)
        ↓
    runnable >= 256 → 过载
        ↓
    totalSlots: 50 → 49
        ↓
    影响:
        ├─ KVWork: 新请求被限流（排队等待）
        ├─ SQLKVResponseWork: cpuOverload.isOverloaded() = true
        └─ SQLSQLResponseWork: cpuOverload.isOverloaded() = true

T=1ms: ElasticCPU 响应
    schedulerLatencyListener.report(p99=2ms)
        ↓
    p99 > 1ms → 负载高
        ↓
    elasticTokens: 10000 → 9000 (减少 10%)
        ↓
    影响:
        └─ 后台 Compaction 被限流

T=1s: Stores 不受影响
    pebbleMetricsTick()
        ↓
    L0 sublevel count = 3 (正常)
    Disk bandwidth = 30% (正常)
        ↓
    ioTokens 正常补充
        ↓
    影响:
        └─ Store 层 IO 准入不受影响

结果:
    ├─ CPU 层被保护（Regular + Elastic 都减少）✓
    └─ IO 层不受影响（磁盘资源未浪费）✓
```

**场景 2：IO 过载 + CPU 正常**

```
T=0: 系统状态
    CPU: runnable=100 (正常)
    Store 1 IO: L0 sublevel=19 (过载)
    Store 2 IO: 正常

T=1ms: RegularCPU 不受影响
    runnable=100 < 128 → 空闲
        ↓
    totalSlots: 40 → 41 (增加)
        ↓
    影响:
        └─ CPU 层准入更宽松

T=1s: Store 1 响应
    pebbleMetricsTick(storeID=1, L0Sublevels=19)
        ↓
    L0 sublevel=19 > 18 → 严重
        ↓
    l0Tokens = 0 (停止 Elastic 工作)
    ioTokens 大幅减少
        ↓
    影响:
        ├─ Regular write: 仅检查 ioTokens (仍可通过，但限流)
        └─ Elastic write: l0Tokens=0 → 完全拒绝

T=1s: Store 2 不受影响
    L0 sublevel=3 (正常)
        ↓
    ioTokens 正常补充
        ↓
    影响:
        └─ Store 2 的写入不受影响

结果:
    ├─ CPU 层不受影响（无需限流）✓
    ├─ Store 1 被保护（IO 限流）✓
    └─ Store 2 不受影响（独立管理）✓
```

**场景 3：全面过载**

```
T=0: 系统状态
    CPU: runnable=350 (严重过载)
    Store 1 IO: L0 sublevel=20 (过载)
    Store 2 IO: Disk bandwidth=90% (过载)

T=1ms: RegularCPU 响应
    totalSlots: 50 → 49 → 48 → ... (持续减少)

T=1ms: ElasticCPU 响应
    elasticTokens: 10000 → 9000 → 8100 → ... (指数衰减)

T=1s: Store 1 响应
    l0Tokens = 0
    ioTokens 严格限制

T=1s: Store 2 响应
    diskBWTokens 严格限制
    ioTokens 严格限制

结果:
    三层全面限流，保护系统稳定
    ├─ CPU 层: Regular 工作受限，Elastic 几乎停止
    ├─ Store 1: Elastic 完全停止，Regular 严格限流
    └─ Store 2: Elastic 完全停止，Regular 严格限流
```

---

## 五、具体示例（必须有）

### 5.1 示例 1：写请求的完整双重准入流程

**场景设定**：
- 用户写入 128KB 数据到 Store 3
- 系统状态：CPU 正常，Store 3 IO 正常

**完整时间线**：

```
T=0: 用户请求到达
    ↓
KV 层确定目标 Store: storeID=3
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 第一层：Store 层 IO 准入                                      │
└─────────────────────────────────────────────────────────────┘
    ↓
T=1μs: 调用 Stores.TryGetQueueForStore(3)
    ├─ gcMap.Load(3) → storeGrantCoordinator
    └─ 返回 *StoreWorkQueue

T=2μs: StoreWorkQueue.Admit(ctx, storeInfo)
    storeInfo = {
        TenantID: 2,
        Priority: NormalPri,
        WorkClass: RegularWorkClass,
        WriteBytes: 128KB,
        ReplicatedWorkInfo: {...},
    }

T=3μs: 队列检查（快速路径）
    len(queue) == 0? 是 ✓
        ↓
    调用 kvStoreTokenChildGranter.tryGet()
        ↓
    kvStoreTokenGranter.tryGet(RegularWorkClass, storeInfo)

T=4μs: IO Token 检查（Regular 工作）
    requested = 128KB = 131072 bytes

    检查: ioTokens >= requested?
    ├─ ioTokens = 500KB = 512000 bytes
    └─ 512000 >= 131072 ✓

    消耗 tokens:
    ioTokens = 512000 - 131072 = 380928 bytes

    返回: true

T=5μs: Store 层准入成功
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 第二层：CPU 层准入                                            │
└─────────────────────────────────────────────────────────────┘
    ↓
T=6μs: 调用 RegularCPU.GetWorkQueue(KVWork).Admit(ctx, info)
    info = {
        TenantID: 2,
        Priority: NormalPri,
        RequestedCount: 1,  // KVWork 总是 1 slot
        ReplicatedWorkInfo: {...},
    }

T=7μs: 队列检查（快速路径）
    len(queue) == 0? 是 ✓
        ↓
    调用 slotGranter.tryGet(1)
        ↓
    GrantCoordinator.tryGet(KVWork, 1)
        ↓
    slotGranter.tryGetLocked(1)

T=8μs: Slot 检查
    usedSlots = 45
    totalSlots = 50

    检查: usedSlots < totalSlots?
    ├─ 45 < 50 ✓

    消耗 slot:
    usedSlots = 45 + 1 = 46

    更新 metric:
    KVUsedSlots.Update(46)

    返回: grantSuccess

T=9μs: CPU 层准入成功
    WorkQueue.Admit() 返回: (true, nil)
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 执行阶段                                                      │
└─────────────────────────────────────────────────────────────┘
    ↓
T=10μs: 开始执行 Raft 提议
    ↓
T=5ms: Raft 提议完成，写入 Pebble
    ↓
T=10ms: 写入完成
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 资源释放阶段                                                  │
└─────────────────────────────────────────────────────────────┘
    ↓
T=10ms+1μs: 归还 Store 层资源
    StoreWorkQueue.StoreWriteDone(originalTokens=131072, doneInfo)
        ↓
    kvStoreTokenGranter.returnGrant(RegularWorkClass, 131072)
        ↓
    ioTokens = 380928 + 131072 = 512000 bytes
        ↓
    检查是否有等待请求:
        hasWaitingRequests()? 否
        → 不触发 tryGrant()

T=10ms+2μs: 归还 CPU 层资源
    WorkQueue.AdmittedWorkDone(tenantID=2, cpuTime=0)
        ↓
    slotGranter.returnGrant(1)
        ↓
    GrantCoordinator.returnGrant(KVWork, 1)
        ↓
    slotGranter.returnGrantLocked(1)
        ↓
    usedSlots = 46 - 1 = 45
        ↓
    更新 metric:
    KVUsedSlots.Update(45)
        ↓
    检查 Grant Chain:
        grantChainActive? 否
        → 调用 tryGrantLocked()
            ↓
        检查是否有等待请求:
            hasWaitingRequests()? 否
            → 不授权

T=10ms+3μs: 完成
```

**资源状态变化总结**：

| 时间点       | Store 3 ioTokens | RegularCPU usedSlots | 说明                    |
|-------------|------------------|---------------------|------------------------|
| T=0         | 512000           | 45                  | 初始状态                |
| T=4μs       | 380928           | 45                  | IO 层准入成功，消耗 tokens |
| T=8μs       | 380928           | 46                  | CPU 层准入成功，消耗 slot  |
| T=10ms+1μs  | 512000           | 46                  | 归还 IO tokens          |
| T=10ms+2μs  | 512000           | 45                  | 归还 CPU slot           |

### 5.2 示例 2：Store 过载时的限流效果

**场景设定**：
- Store 1: L0 sublevel=19 (过载)
- Store 2: L0 sublevel=3 (正常)
- 两个 Elastic write 请求同时到达

**时间线**：

```
T=0: 两个请求同时到达
    ├─ Request A: 写入 Store 1, 256KB, Elastic
    └─ Request B: 写入 Store 2, 256KB, Elastic

T=1s: Stores 后台 goroutine 更新 metrics
    ↓
Store 1:
    pebbleMetricsTick(storeID=1, L0Sublevels=19)
        ↓
    L0 压力计算:
        L0Sublevels=19 > 18 (阈值)
            ↓
        l0Tokens = 0  // ← 关键！
        diskBWTokens = 正常
        ioTokens = 正常

Store 2:
    pebbleMetricsTick(storeID=2, L0Sublevels=3)
        ↓
    L0 压力计算:
        L0Sublevels=3 < 10 (正常)
            ↓
        l0Tokens = 充足
        diskBWTokens = 正常
        ioTokens = 正常

T=1s+10ms: Request A 尝试准入
    ↓
Stores.TryGetQueueForStore(1).Admit(ctx, storeInfo)
    storeInfo.WorkClass = ElasticWorkClass
    storeInfo.WriteBytes = 256KB
    ↓
kvStoreTokenGranter.tryGet(ElasticWorkClass, storeInfo)
    ↓
检查 1: l0Tokens > 0?
    l0Tokens = 0 ✗
    ↓
返回: false
    ↓
Request A 进入队列等待
    ├─ 创建 waitingWork
    ├─ 加入 StoreWorkQueue 的 waitingWorkHeap
    └─ 阻塞在 channel

T=1s+11ms: Request B 尝试准入
    ↓
Stores.TryGetQueueForStore(2).Admit(ctx, storeInfo)
    storeInfo.WorkClass = ElasticWorkClass
    storeInfo.WriteBytes = 256KB
    ↓
kvStoreTokenGranter.tryGet(ElasticWorkClass, storeInfo)
    ↓
检查 1: l0Tokens > 0?
    l0Tokens = 100MB ✓
    ↓
检查 2: diskBWTokens > 0?
    diskBWTokens = 50MB ✓
    ↓
检查 3: ioTokens >= 256KB?
    ioTokens = 5MB ✓
    ↓
消耗 tokens:
    ioTokens -= 256KB
    l0Tokens -= 256KB
    diskBWTokens -= 256KB
    ↓
返回: true
    ↓
Request B 立即执行 ✓

T=2s: 下一轮 pebbleMetricsTick
    ↓
Store 1:
    L0Sublevels=18 (改善)
        ↓
    l0Tokens = 1MB (恢复部分)
        ↓
    tryGrant() 被调用
        ↓
    Request A 获得授权
        ↓
    Request A 开始执行 ✓

结果对比:
    Request A (Store 1 过载):
        ├─ 等待时间: 1 秒
        └─ 保护了 L0 健康

    Request B (Store 2 正常):
        ├─ 等待时间: 0 秒
        └─ 未受 Store 1 影响 ✓
```

**关键观察**：
- **独立管理**：Store 1 的过载不影响 Store 2
- **优先级保护**：Elastic 工作在 L0 压力高时被完全阻止
- **自动恢复**：L0 改善后自动恢复准入

---

## 六、设计取舍与权衡（Trade-offs）

### 6.1 三个协调器 vs 单一协调器

**当前设计：三个独立协调器**

```
优点:
    1. 资源隔离
       ├─ CPU 过载不影响 IO 准入
       └─ IO 过载不影响 CPU 准入

    2. 独立调优
       ├─ CPU: 基于 runnable goroutines
       ├─ IO: 基于 Pebble metrics
       └─ Elastic: 基于调度延迟

    3. 故障隔离
       └─ 一个协调器失效不影响其他

缺点:
    1. 复杂度高
       ├─ 三套代码路径
       └─ 调试困难

    2. 内存开销
       └─ 每个协调器独立的数据结构

    3. 双重准入开销
       └─ 写请求需要两次 Admit() 调用
```

**替代方案：单一协调器**

```
设计:
    GrantCoordinator 管理所有资源
    ├─ CPU slots
    ├─ IO tokens (所有 Store)
    └─ Elastic tokens

优点:
    1. 实现简单
    2. 内存开销低
    3. 单次 Admit() 调用

缺点:
    1. 资源耦合
       └─ CPU 过载 → 错误地限流 IO

    2. 难以扩展
       └─ 添加新资源类型需要修改核心逻辑

    3. 锁竞争严重
       └─ 所有资源竞争同一个 mutex

结论: 当前设计牺牲简单性，换取隔离性和扩展性 ✓
```

### 6.2 每个 Store 独立协调器 vs 全局 Store 协调器

**当前设计：`Stores.gcMap[storeID]` (每个 Store 独立)**

```
优点:
    1. 资源独立性
       └─ Store 1 磁盘故障不影响 Store 2

    2. 精确控制
       └─ 根据每个磁盘的实际性能调整

    3. 水平扩展
       └─ 添加新 Store 不影响现有 Store

缺点:
    1. 内存开销
       └─ N 个 Store × 每个协调器的内存

    2. 管理复杂度
       └─ 需要动态管理 gcMap

    3. 不能跨 Store 共享 IO
       └─ Store 1 空闲时不能借给 Store 2
```

**替代方案：全局 Store 协调器**

```
设计:
    单一 GrantCoordinator 管理所有 Store 的 IO
    ├─ ioTokensTotal = Σ(每个 Store 的带宽)
    └─ 请求可以使用任何 Store 的 tokens

优点:
    1. 资源共享
       └─ Store 1 空闲 → Store 2 可以借用

    2. 内存节省
       └─ 单个协调器

缺点:
    1. 连带影响
       └─ Store 1 过载 → 所有 Store 被限流 ✗

    2. 不公平
       └─ Store 2 的请求可能消耗 Store 1 的 quota

    3. 违背物理隔离
       └─ 磁盘是物理独立的，软件不应该打破这种隔离

结论: 当前设计符合物理现实，虽然牺牲了灵活性 ✓
```

### 6.3 RegularCPU 和 ElasticCPU 分离的价值

**当前设计：两个独立的 CPU 协调器**

```
隔离机制:
    RegularCPU:
        ├─ 管理 KVWork, SQLKVResponseWork, SQLSQLResponseWork
        ├─ 优先级高
        └─ 动态调整 slots

    ElasticCPU:
        ├─ 管理 ElasticWork (后台任务)
        ├─ 优先级低
        └─ 根据调度延迟主动让路

效果:
    Regular 工作负载高 → Elastic 自动减少
    Regular 工作空闲   → Elastic 自动增加
```

**如果合并成一个协调器**：

```
问题:
    所有 WorkKind 共享同一个 slot/token pool
        ↓
    Compaction 消耗大量 tokens
        ↓
    用户请求无法获得 tokens
        ↓
    P99 延迟飙升 ✗

当前设计解决方案:
    ElasticCPU 独立 token pool
        ↓
    schedulerLatencyListener 检测到延迟增加
        ↓
    主动减少 elasticTokens
        ↓
    Compaction 被限流
        ↓
    用户请求不受影响 ✓
```

### 6.4 锁竞争与性能开销

**锁的层次结构**：

```
Level 1: GrantCoordinators (无锁)
    └─ 纯容器，无状态

Level 2: 每个协调器的锁
    ├─ RegularCPU.mu (频繁)
    │   └─ 每个请求的 tryGet/returnGrant
    │
    ├─ ElasticCPU.mu (较少)
    │   └─ 后台任务的频率低
    │
    └─ Stores.gcMap (读多写少)
        └─ sync.Map，无锁读

Level 3: 每个 granter 的锁
    └─ 已被 GrantCoordinator.mu 覆盖

Level 4: 每个 WorkQueue 的锁
    ├─ WorkQueue.mu
    └─ 与 GrantCoordinator.mu 顺序严格
```

**锁顺序约束**：

**源代码注释**：`grant_coordinator.go:107`

```go
// mu is ordered before any mutex acquired in a requester implementation.
```

**含义**：
```
锁获取顺序:
    GrantCoordinator.mu → WorkQueue.mu

禁止:
    持有 WorkQueue.mu 时获取 GrantCoordinator.mu
    → 会导致死锁

示例正确顺序:
    coord.mu.Lock()
    defer coord.mu.Unlock()

    coord.tryGrantLocked()
        ↓
    workQueue.granted(grantChainID)
        ↓
    workQueue.mu.Lock()  // ← 安全，顺序正确
    defer workQueue.mu.Unlock()
```

**性能开销分析**：

| 操作                     | 频率          | 锁竞争程度 | 开销估算      |
|-------------------------|--------------|----------|--------------|
| RegularCPU.tryGet()     | 每个请求      | 中-高     | ~1-5μs       |
| RegularCPU.returnGrant()| 每个请求完成   | 中-高     | ~1-5μs       |
| RegularCPU.CPULoad()    | 每 1ms       | 低       | ~10-50μs     |
| Stores.tryGet()         | 每个写请求    | 低-中     | ~1-3μs       |
| Stores.pebbleMetricsTick() | 每 1s     | 低       | ~100-500μs   |
| ElasticCPU 操作         | 低频         | 低       | 可忽略       |

**总体评估**：

```
10,000 req/s 的负载:
    ├─ tryGet: 10,000/s × 5μs = 50ms/s CPU
    ├─ returnGrant: 10,000/s × 5μs = 50ms/s CPU
    └─ 总计: 100ms/s CPU ≈ 10% 单核 (8 核系统 = 1.25%)

结论: 开销可接受 ✓
```

### 6.5 双重准入的成本

**写请求的双重准入流程**：

```
成本分析:

单次写请求:
    ├─ Store 层 Admit(): ~5μs
    ├─ CPU 层 Admit(): ~5μs
    └─ 总计: ~10μs

如果单层准入:
    └─ 单次 Admit(): ~5μs

额外成本: 5μs per write (100% 开销)

但收益:
    1. 资源隔离
       └─ IO 过载不影响 CPU 准入决策

    2. 精确控制
       └─ 可以独立调整 IO 和 CPU 的限流策略

    3. 可观测性
       └─ 可以分别观察 IO 和 CPU 的瓶颈

权衡:
    5μs vs 典型写入延迟 5ms
    → 额外成本 = 0.1%
    → 可以接受 ✓
```

---

## 七、总结与心智模型

### 7.1 核心思想总结

**GrantCoordinators 实现了一个按资源类型分层、按工作优先级隔离的准入控制架构**。它不包含任何控制逻辑，仅作为容器管理三个独立的协调器，每个协调器专注于一种资源类型（CPU-Regular、CPU-Elastic、IO）或资源实例（每个 Store）的准入控制。

**三个协调器完全独立运行，通过各自的监听器（kvSlotAdjuster、schedulerLatencyListener、ioLoadListener）接收不同的信号源，根据不同的决策逻辑动态调整准入策略，实现多维度的过载保护和资源利用率优化。**

**关键设计理念**：
1. **资源类型隔离**：CPU 和 IO 的特性不同，需要独立控制
2. **工作优先级隔离**：Regular 和 Elastic 的 SLO 不同，需要独立 quota
3. **资源实例隔离**：每个磁盘独立，需要独立管理
4. **双重准入保护**：写请求需要同时通过 IO 和 CPU 两层检查

### 7.2 心智模型

**如果只记住一件事，那就是：**

```
"GrantCoordinators 是一个三维隔离矩阵：
  - 维度 1：资源类型（CPU vs IO）
  - 维度 2：工作优先级（Regular vs Elastic）
  - 维度 3：资源实例（Store 1, 2, ..., N）

每个维度独立决策，共同保护系统。"
```

**可视化心智模型**：

```
         资源类型
            ↓
    ┌───────┬───────┐
    │  CPU  │  IO   │
    ├───────┼───────┤
    │ Reg   │Store1 │ ← Regular 工作
    │ Elast │Store2 │ ← Elastic 工作
    │       │StoreN │ ← 独立实例
    └───────┴───────┘
         ↑
    工作优先级

每个格子独立决策:
    RegularCPU: 根据 CPU 负载调整 slots
    ElasticCPU: 根据调度延迟调整 tokens
    Store[i]: 根据磁盘 metrics 调整 IO tokens
```

### 7.3 简化伪代码

```python
class GrantCoordinators:
    def __init__(self):
        # 三个独立的协调器
        self.RegularCPU = GrantCoordinator(
            workKinds=[KVWork, SQLKVResponseWork, SQLSQLResponseWork],
            cpuLoadListener=kvSlotAdjuster,
            useGrantChains=True
        )

        self.ElasticCPU = ElasticCPUGrantCoordinator(
            schedulerLatencyListener=SchedulerLatencyListener,
            tokenBased=True
        )

        self.Stores = StoreGrantCoordinators(
            stores={},  # 动态添加
            pebbleMetricsListener=ioLoadListener
        )

    def admit_read_request(self, request):
        # 读请求：仅 CPU 层
        return self.RegularCPU.GetWorkQueue(KVWork).Admit(request)

    def admit_write_request(self, request):
        # 写请求：双重准入（顺序重要！）

        # 第一层：IO 层
        store_admitted = self.Stores.TryGetQueueForStore(
            request.storeID).Admit(request)
        if not store_admitted:
            return False  # IO 层拒绝

        # 第二层：CPU 层
        cpu_admitted = self.RegularCPU.GetWorkQueue(KVWork).Admit(request)
        if not cpu_admitted:
            # CPU 层拒绝，需要归还 IO tokens
            self.Stores.TryGetQueueForStore(
                request.storeID).StoreWriteDone(...)
            return False

        return True  # 两层都通过

    def admit_elastic_request(self, request):
        # 弹性请求
        return self.ElasticCPU.ElasticCPUWorkQueue.Admit(request)

    def close(self):
        # 按顺序关闭（Stores → RegularCPU → ElasticCPU）
        self.Stores.close()
        self.RegularCPU.Close()
        self.ElasticCPU.close()


# 信号源驱动
def main_loop():
    coords = GrantCoordinators()

    # CPU 信号（每 1ms）
    goschedstats.register_callback(
        lambda: coords.RegularCPU.CPULoad(runnable, procs, period)
    )

    # 调度延迟信号（每 100ms）
    scheduler_latency.register_callback(
        lambda: coords.ElasticCPU.SchedulerLatencyListener.report(p99)
    )

    # IO 信号（每 1s）
    pebble_metrics.register_callback(
        lambda: coords.Stores.pebbleMetricsTick(metrics)
    )
```

**全文完**

GrantCoordinators 的设计体现了"分而治之"的工程哲学：将复杂的多维资源管理问题分解为三个独立的子问题，每个子问题由专门的协调器解决，最终通过容器统一管理，实现了高内聚、低耦合的准入控制架构。
