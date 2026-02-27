# 第十七章：StoreGrantCoordinators——按 Store 分层的 IO 准入控制架构

## 一、第一轮 BFS：整体职责与设计动机（Why）

### 1.1 存在背景与要解决的问题

在 CockroachDB 的准入控制体系中，我们已经看到了 `GrantCoordinator` 如何管理 CPU 资源（通过 slots 和 tokens）。但对于 **写入密集型工作负载**，仅控制 CPU 是不够的——磁盘 IO 才是真正的瓶颈。

**核心问题**：

1. **L0 层过载**：写入请求最终会刷新到 Pebble（RocksDB 变种）的 L0 层。如果 L0 的 sub-level 数量过多（例如超过 20 个），会导致读放大和写停顿（write stall）
2. **磁盘带宽饱和**：高速写入会占满磁盘带宽，导致读延迟飙升
3. **多 Store 独立性**：一个节点可能有多个 Store（对应不同磁盘），它们的健康状态是独立的

**为什么不能复用 GrantCoordinator**？

- `GrantCoordinator` 管理的是 **节点级别的 CPU 资源**，所有 Store 共享
- 但每个 Store 有 **独立的磁盘**、**独立的 Pebble 实例**、**独立的 L0 状态**
- 需要 **per-store 粒度的准入控制**

### 1.2 在系统中的位置

```
┌─────────────────────────────────────────────────────┐
│              CockroachDB Node                        │
├─────────────────────────────────────────────────────┤
│                                                       │
│  ┌────────────────────────────────────────────┐     │
│  │     GrantCoordinators (顶层容器)            │     │
│  ├────────────────────────────────────────────┤     │
│  │  • RegularCPU (GrantCoordinator)           │     │
│  │    → 管理 CPU 层面的 KVWork, SQLKVResp...  │     │
│  │                                             │     │
│  │  • ElasticCPU (ElasticCPUGrantCoordinator) │     │
│  │    → 管理弹性 CPU 工作                      │     │
│  │                                             │     │
│  │  • Stores (StoreGrantCoordinators) ◄─ 本章  │     │
│  │    → 管理每个 Store 的 IO 准入              │     │
│  └────────────────────────────────────────────┘     │
│                       │                              │
│                       ↓                              │
│  ┌─────────────────────────────────────────────┐    │
│  │   StoreGrantCoordinators                    │    │
│  │   gcMap: IntMap[StoreID → storeGrantCoord]  │    │
│  ├─────────────────────────────────────────────┤    │
│  │                                              │    │
│  │  Store 1:                                    │    │
│  │  ├─ kvStoreTokenGranter                     │    │
│  │  ├─ StoreWorkQueue (regular)                │    │
│  │  ├─ StoreWorkQueue (elastic)                │    │
│  │  ├─ SnapshotQueue                           │    │
│  │  └─ ioLoadListener                          │    │
│  │     ├─ L0 metrics 监听                       │    │
│  │     ├─ 磁盘带宽监听                          │    │
│  │     └─ Token 动态调整                        │    │
│  │                                              │    │
│  │  Store 2: (相同结构)                         │    │
│  │  Store N: (相同结构)                         │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

**协作模块**：

1. **上游**：`GrantCoordinator.RegularCPU` / `ElasticCPU`（CPU 层准入）
2. **平级**：多个 `storeGrantCoordinator`（每个 Store 一个实例）
3. **下游**：
   - `Pebble`：提供 L0 metrics（sub-level count, file count, flush throughput）
   - `StoreWorkQueue`：持有等待写入的请求队列
   - `SnapshotQueue`：处理 Raft snapshot 的接收

### 1.3 核心对象与关键状态

#### 核心结构体

```go
// pkg/util/admission/store_grant_coordinator.go:50-102

type StoreGrantCoordinators struct {
    ambientCtx                log.AmbientContext
    settings                  *cluster.Settings
    makeStoreRequesterFunc    makeStoreRequesterFunc

    // ===== 核心：每个 Store 的独立协调器 =====
    gcMap syncutil.Map[roachpb.StoreID, storeGrantCoordinator]

    numStores                      int
    setPebbleMetricsProviderCalled bool
    onLogEntryAdmitted             OnLogEntryAdmitted
    closeCh                        chan struct{}

    knobs *TestingKnobs
}
```

**为什么需要 `gcMap`？**

```
原因 1：IO 资源隔离
  ├─► 每个 Store 对应一个磁盘
  ├─► 磁盘之间的 IO 容量独立
  └─► 需要独立的 IO token 管理

原因 2：L0 容量独立
  ├─► 每个 Store 有独立的 Pebble 实例
  ├─► L0 sub-level 数独立
  └─► 需要独立的 L0 token 管理

原因 3：磁盘健康状态独立
  ├─► 一个 Store 的磁盘可能过载
  ├─► 其他 Store 的磁盘正常
  └─► 需要独立的准入决策

示例：双 Store 节点

Store 1：
├─► L0 sub-levels = 15（接近上限 20）
├─► 磁盘带宽 = 80%（接近饱和）
└─► IO tokens = 0（停止准入）

Store 2：
├─► L0 sub-levels = 3（正常）
├─► 磁盘带宽 = 30%（正常）
└─► IO tokens = 10000（正常准入）

结果：
- Store 1 的写入请求被阻塞 ✓
- Store 2 的写入请求正常处理 ✓
- 避免 Store 2 被 Store 1 拖累 ✓
```

#### 单个 Store 的协调器

```go
// pkg/util/admission/store_grant_coordinator.go:388-398

type storeGrantCoordinator struct {
    granter        *kvStoreTokenGranter  // IO token 管理
    storeReq       storeRequester        // StoreWorkQueue（2个work class）
    snapshotReq    requesterClose        // SnapshotQueue
    ioLoadListener *ioLoadListener       // IO 负载监听与token调整
}
```

**四大组件的分工**：

1. **`kvStoreTokenGranter`**：管理三种 token
   - Regular IO tokens（常规工作）
   - Elastic IO tokens（弹性工作）
   - Disk bandwidth tokens（磁盘带宽）

2. **`StoreWorkQueue`**：按 work class 排队
   - Regular work class：高优先级写入（用户事务）
   - Elastic work class：低优先级写入（后台任务、bulk import）

3. **`SnapshotQueue`**：Raft snapshot 接收的独立队列

4. **`ioLoadListener`**：负载监听与动态调整
   - 每 15 秒从 Pebble 读取 metrics
   - 每 1ms 或 250ms 分配 tokens
   - 根据 L0 状态和磁盘带宽动态调整 token 速率

---

## 二、第二轮 BFS：控制流与交互关系（How it flows）

### 2.1 初始化时间线

```
T=0: Server 启动
 │
 ├─► pkg/server/node.go: Node.Start()
 │    └─► NewGrantCoordinators(...)
 │         └─► makeStoresGrantCoordinators(...)
 │              ├─ 创建空的 gcMap
 │              └─ 返回 *StoreGrantCoordinators
 │
T=100ms: Stores 初始化完成
 │
 ├─► SetPebbleMetricsProvider(pmp, mrp, iotc)
 │    ├─ metrics := pmp.GetPebbleMetrics()
 │    │   → [{StoreID: 1, L0: {...}}, {StoreID: 2, L0: {...}}, ...]
 │    │
 │    ├─ 为每个 Store 初始化 storeGrantCoordinator:
 │    │   ├─ gc := sgc.initGrantCoordinator(storeID, metricsRegistry)
 │    │   │   ├─ 创建 kvStoreTokenGranter
 │    │   │   ├─ 创建 StoreWorkQueue (regular + elastic)
 │    │   │   ├─ 创建 SnapshotQueue
 │    │   │   └─ 创建 ioLoadListener
 │    │   │
 │    │   └─ sgc.gcMap.LoadOrStore(storeID, gc)
 │    │
 │    └─ 启动后台 goroutine（定时调度）:
 │         ├─ tokenAllocationTicker: 周期性分配 token
 │         └─ 每 15s 调用 pebbleMetricsTick()
 │              每 1ms 调用 allocateIOTokensTick()
 │
T=15s: 第一次 adjustment interval 完成
 │
 ├─► pebbleMetricsTick(ctx, storeMetrics)
 │    ├─ ioLoadListener.pebbleMetricsTick(...)
 │    │   ├─ 计算 L0 增长量
 │    │   ├─ 评估 flush/compaction 速率
 │    │   └─ 决定下一个 15s 的 token 总量
 │    │
 │    └─ iotc.UpdateIOThreshold(storeID, ioThreshold)
 │
T=15s + 1ms, 15s + 2ms, ..., 30s: 每 1ms 分配一次 token
 │
 └─► allocateIOTokensTick(remainingTicks)
      ├─ ioLoadListener.allocateTokensTick(...)
      │   └─ 将 15s 的 token 分摊到每次 tick
      │
      └─ granter.tryGrant()
           └─ 如果有等待请求，尝试授权
```

### 2.2 写入请求的完整生命周期

#### 场景：用户发起写入请求

```
时刻 T0: 请求到达 KV 层
┌─────────────────────────────────────────────────┐
│ 1. Store 层准入（IO 资源）                       │
├─────────────────────────────────────────────────┤
│ StoreWorkQueue.Admit(ctx, WorkInfo{...})       │
│  ├─ workClass := 确定是 regular 还是 elastic     │
│  ├─ estimatedTokens := 估算需要的 L0 bytes       │
│  │                                              │
│  ├─ 快速路径: granter.tryGet(estimatedTokens)  │
│  │   ├─ kvStoreTokenGranter.tryGetLocked()    │
│  │   │   ├─ 检查 availableIOTokens[workClass] │
│  │   │   ├─ 检查 diskTokensAvailable           │
│  │   │   └─ 如果都 > 0: 扣除 token, return true│
│  │   │                                          │
│  │   └─ 如果 token 不足: return false          │
│  │                                              │
│  └─ 慢路径: 入队等待                             │
│      ├─ queue.enqueue(request)                 │
│      └─ 等待 granter 调用 granted(...)         │
└─────────────────────────────────────────────────┘
         │
         ↓ (获得 IO 准入)

时刻 T1: 通过 Store 层准入
┌─────────────────────────────────────────────────┐
│ 2. CPU 层准入（如果需要 CPU 处理）               │
├─────────────────────────────────────────────────┤
│ GrantCoordinator.RegularCPU.GetWorkQueue(KVWork)│
│  ├─ WorkQueue.Admit(...)                        │
│  │   ├─ 检查 slotGranter.usedSlots < totalSlots│
│  │   └─ 获得 CPU slot                           │
│  └─ 执行写入逻辑                                 │
└─────────────────────────────────────────────────┘
         │
         ↓ (执行写入)

时刻 T2: 写入完成
┌─────────────────────────────────────────────────┐
│ 3. 通知完成与 Token 调整                         │
├─────────────────────────────────────────────────┤
│ StoreWorkQueue.AdmittedWorkDone(StoreWorkDoneInfo)│
│  ├─ actualL0Bytes := 实际写入的字节数             │
│  ├─ actualIngestedBytes := 实际 ingest 的字节     │
│  │                                              │
│  ├─ granter.storeWriteDone(estimatedTokens, doneInfo)│
│  │   ├─ delta := actualBytes - estimatedBytes  │
│  │   ├─ 如果 delta > 0: 补扣 token              │
│  │   ├─ 如果 delta < 0: 归还 token              │
│  │   └─ 更新 linear model（用于下次估算）        │
│  │                                              │
│  └─ granter.tryGrant()                          │
│      └─ 如果有等待请求且 token 充足，继续授权    │
└─────────────────────────────────────────────────┘
```

### 2.3 Token 动态调整的触发机制

**问题**：如何决定给多少 tokens？

**答案**：基于 Pebble metrics 的反馈循环（15 秒周期）

```
Adjustment Interval (15s 周期):

T=0s: pebbleMetricsTick() 被调用
 │
 ├─ 读取 Pebble metrics:
 │   ├─ L0 sub-level count
 │   ├─ L0 file count
 │   ├─ L0 total bytes
 │   ├─ cumL0AddedBytes (累计写入 L0 的字节数)
 │   ├─ cumFlushWriteThroughput (flush 速率)
 │   └─ cumCompactionStats (compaction out of L0 的速率)
 │
 ├─ 计算 L0 的健康分数:
 │   score = max(
 │       subLevelCount / 20,
 │       fileCount / 4000
 │   )
 │
 │   如果 score > 1.0: Store 过载
 │   如果 score < 0.5: Store 健康
 │
 ├─ 根据 compaction 速率计算 token:
 │   compactionTokens = (过去15s compact出L0的字节数)
 │
 │   如果 score > 1.0:
 │       → 减少 token（抑制写入）
 │   如果 score < 0.5:
 │       → 增加 token（允许更多写入）
 │
 ├─ 根据 flush 速率计算 token:
 │   flushTokens = flushRate * targetUtilization
 │
 │   如果最近有 write stall:
 │       → targetUtilization 降低（更保守）
 │   如果没有 write stall:
 │       → targetUtilization 提高（更激进）
 │
 └─ 最终 token = min(compactionTokens, flushTokens)
     → 传递给 kvStoreTokenGranter.setAvailableTokens()

T=0s → T=15s: 每 1ms 分配一次 token
 │
 ├─ allocateIOTokensTick(remainingTicks)
 │   ├─ tokensThisTick = totalTokens / remainingTicks
 │   ├─ availableIOTokens[Regular] += tokensThisTick
 │   ├─ availableIOTokens[Elastic] += tokensThisTick
 │   └─ granter.tryGrant()
 │
 └─ 重复 15000 次（每 1ms 一次）

T=15s: 下一个 adjustment interval 开始
 └─ 重复上述过程
```

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 初始化：`initGrantCoordinator()`

**位置**：`pkg/util/admission/store_grant_coordinator.go:229-312`

**职责**：为单个 Store 创建完整的准入控制组件

```go
func (sgc *StoreGrantCoordinators) initGrantCoordinator(
    storeID roachpb.StoreID,
    metricsRegistry *metric.Registry,
) *storeGrantCoordinator {
    // ===== 步骤 1: 创建 metrics =====
    sgcMetrics := makeStoreGrantCoordinatorMetrics(metricsRegistry)
    // 为 regular 和 elastic 各创建一套 WorkQueue metrics
    regularStoreWorkQueueMetrics := makeWorkQueueMetrics(
        fmt.Sprintf("%s-stores", KVWork),
        metricsRegistry,
        admissionpb.NormalPri,
        admissionpb.LockingNormalPri,
    )
    elasticStoreWorkQueueMetrics := makeWorkQueueMetrics(
        fmt.Sprintf("%s-stores", admissionpb.ElasticWorkClass),
        metricsRegistry,
        admissionpb.BulkLowPri,
        admissionpb.BulkNormalPri,
    )

    // ===== 步骤 2: 创建 kvStoreTokenGranter =====
    kvg := &kvStoreTokenGranter{
        knobs: sgc.knobs,
        ioTokensExhaustedDurationMetric: sgcMetrics.KVIOTokensExhaustedDuration,
        availableTokensMetric:           sgcMetrics.KVIOTokensAvailable,
        // ...
    }

    // 初始化为 unlimited（防御性编程）
    // 真正的值会在 pebbleMetricsTick 和 allocateIOTokensTick 中设置
    kvg.mu.startingIOTokens = unlimitedTokens / unloadedDuration.ticksInAdjustmentInterval()
    kvg.mu.availableIOTokens[admissionpb.RegularWorkClass] = unlimitedTokens / unloadedDuration.ticksInAdjustmentInterval()
    kvg.mu.availableIOTokens[admissionpb.ElasticWorkClass] = kvg.mu.availableIOTokens[admissionpb.RegularWorkClass]

    // ===== 步骤 3: 创建 child granters（委托模式）=====
    storeGranters := [admissionpb.NumWorkClasses]granterWithStoreReplicatedWorkAdmitted{
        &kvStoreTokenChildGranter{
            workType: admissionpb.RegularStoreWorkType,
            parent:   kvg,  // 委托给 parent
        },
        &kvStoreTokenChildGranter{
            workType: admissionpb.ElasticStoreWorkType,
            parent:   kvg,
        },
    }
    snapshotGranter := &kvStoreTokenChildGranter{
        workType: admissionpb.SnapshotIngestStoreWorkType,
        parent:   kvg,
    }

    // ===== 步骤 4: 创建 StoreWorkQueue =====
    opts := makeWorkQueueOptions(KVWork)
    opts.usesTokens = true  // 关键：使用 token 而不是 slot

    storeReq := sgc.makeStoreRequesterFunc(
        sgc.ambientCtx,
        storeID,
        storeGranters,
        sgc.settings,
        storeWorkQMetrics,
        opts,
        sgc.knobs,
        sgc.onLogEntryAdmitted,
        sgcMetrics.KVIOTokensBypassed,
        &kvg.mu.Mutex,  // 共享锁！
    )

    requesters := storeReq.getRequesters()
    kvg.regularRequester = requesters[admissionpb.RegularWorkClass]
    kvg.elasticRequester = requesters[admissionpb.ElasticWorkClass]

    // ===== 步骤 5: 创建 SnapshotQueue =====
    snapshotReq := makeSnapshotQueue(snapshotGranter, snapshotQMetrics)
    kvg.snapshotRequester = snapshotReq

    // ===== 步骤 6: 创建 ioLoadListener =====
    ioll := &ioLoadListener{
        storeID:               storeID,
        settings:              sgc.settings,
        kvRequester:           storeReq,
        perWorkTokenEstimator: makeStorePerWorkTokenEstimator(),
        diskBandwidthLimiter:  newDiskBandwidthLimiter(),
        kvGranter:             kvg,
        l0CompactedBytes:      sgcMetrics.L0CompactedBytes,
        l0TokensProduced:      sgcMetrics.L0TokensProduced,
    }

    // ===== 步骤 7: 组装 coordinator =====
    coord := &storeGrantCoordinator{
        granter:        kvg,
        storeReq:       storeReq,
        snapshotReq:    snapshotReq,
        ioLoadListener: ioll,
    }
    return coord
}
```

**关键设计点**：

1. **委托模式**：
   - `kvStoreTokenChildGranter` 是轻量级的 wrapper
   - 实际逻辑在 `kvStoreTokenGranter`（parent）中
   - 好处：避免代码重复，统一管理 token

2. **共享锁**：
   - `StoreWorkQueue` 和 `kvStoreTokenGranter` 共享同一个 mutex
   - 原因：token 的扣除和归还需要原子性
   - 锁顺序：`kvg.mu` 在 `WorkQueue.mu` 之前

3. **初始值为 unlimited**：
   - 防御性编程，避免启动时拒绝请求
   - 真实值会在第一次 `pebbleMetricsTick` 后设置

### 3.2 Token 获取：`tryGetLocked()`

**位置**：`pkg/util/admission/granter.go:476-579`

**职责**：检查 token 是否充足，决定是否授权

```go
func (sg *kvStoreTokenGranter) tryGetLocked(
    count int64,
    wt admissionpb.StoreWorkType,
) bool {
    // ===== 计算磁盘写 token（应用写放大模型）=====
    diskWriteTokens := count
    if wt != admissionpb.SnapshotIngestStoreWorkType {
        // 写放大模型：实际磁盘写入 = 逻辑写入 * 放大系数
        // 例如：count=100, writeAmpLM=10x+1 → diskWriteTokens=1001
        diskWriteTokens = sg.mu.writeAmpLM.applyLinearModel(count)
    }

    // ===== 根据 work type 检查不同的 token =====
    switch wt {
    case admissionpb.RegularStoreWorkType:
        // 常规工作：只需要 Regular IO tokens
        if sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0 {
            sg.subtractIOTokensLocked(count, count, false)
            sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
            sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
            sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
            return true
        }

    case admissionpb.ElasticStoreWorkType:
        // 弹性工作：需要三种 token 都充足
        if sg.mu.diskTokensAvailable.writeByteTokens > 0 &&
            sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0 &&
            sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] > 0 {

            sg.subtractIOTokensLocked(count, count, false)
            sg.mu.elasticIOTokensUsedByElastic += count
            sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
            sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
            sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
            return true
        }

    case admissionpb.SnapshotIngestStoreWorkType:
        // Snapshot ingest：只需要磁盘写 token（不进 L0）
        if sg.mu.diskTokensAvailable.writeByteTokens > 0 {
            sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
            sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
            sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
            return true
        }
    }

    return false
}
```

**三种 work type 的 token 需求矩阵**：

| Work Type | Regular IO Tokens | Elastic IO Tokens | Disk Write Tokens |
|-----------|-------------------|-------------------|-------------------|
| Regular   | ✓                | ✗                | ✓                |
| Elastic   | ✓                | ✓                | ✓                |
| Snapshot  | ✗                | ✗                | ✓                |

**设计理由**：

1. **Regular 优先级最高**：只要 Regular IO tokens 充足就能执行
2. **Elastic 受三重限制**：
   - 必须有 Regular IO tokens（避免饿死 Regular）
   - 必须有 Elastic IO tokens（专属配额）
   - 必须有 Disk Write tokens（磁盘带宽）
3. **Snapshot 特殊处理**：不进 L0，只消耗磁盘带宽

### 3.3 负载监听：`pebbleMetricsTick()`

**位置**：`pkg/util/admission/io_load_listener.go:500-700`（简化版）

**职责**：每 15 秒评估 L0 健康状况，决定下个周期的 token 总量

```go
func (ioll *ioLoadListener) pebbleMetricsTick(
    ctx context.Context,
    m StoreMetrics,
) bool {
    // ===== 步骤 1: 计算增量 =====
    intervalL0AddedBytes := m.L0Metrics.CumL0AddedBytes - ioll.cumL0AddedBytes
    intervalCompactedBytes := computeL0CompactionBytes(m) - ioll.smoothedIntL0CompactedBytes

    // ===== 步骤 2: 计算 L0 健康分数 =====
    ioThreshold := &admissionpb.IOThreshold{}
    ioThreshold.L0NumFiles = m.L0Metrics.NumFiles
    ioThreshold.L0NumSubLevels = m.L0Metrics.NumSubLevels
    ioThreshold.L0Size = m.L0Metrics.TotalBytes

    // 分数计算（越高越不健康）：
    // score = max(
    //     subLevels / 20.0,
    //     fileCount / 4000.0
    // )
    l0Overload := ioThreshold.Score() > 1.0

    // ===== 步骤 3: 根据 compaction 速率计算 token =====
    // 指导思想：如果 compaction 能跟上写入，就给更多 token
    compactionTokens := intervalCompactedBytes

    if l0Overload {
        // 过载：减少 token（抑制写入）
        compactionTokens = int64(float64(compactionTokens) * 0.5)
    }

    // ===== 步骤 4: 根据 flush 速率计算 token =====
    flushRate := m.FlushMetrics.ThroughputMetric.Bytes() / adjustmentInterval.Seconds()

    // 动态调整目标利用率（根据是否有 write stall）
    if m.WriteStallCount > ioll.cumWriteStallCount {
        // 最近有 write stall → 降低目标利用率
        ioll.flushUtilTargetFraction = max(
            ioll.flushUtilTargetFraction * 0.95,
            MinFlushUtilizationFraction.Get(&ioll.settings.SV),
        )
    } else {
        // 没有 write stall → 提高目标利用率
        ioll.flushUtilTargetFraction = min(
            ioll.flushUtilTargetFraction * 1.05,
            maxFlushUtilTargetFraction,
        )
    }

    flushTokens := int64(flushRate * ioll.flushUtilTargetFraction)

    // ===== 步骤 5: 取最小值（最保守的限制）=====
    totalTokens := min(compactionTokens, flushTokens)

    // ===== 步骤 6: 计算 elastic tokens =====
    elasticTokens := totalTokens
    if l0Overload {
        // 过载时，elastic 只能用 20% 的 token
        elasticTokens = int64(float64(totalTokens) * 0.2)
    }

    // ===== 步骤 7: 传递给 granter =====
    ioll.kvGranter.setAvailableTokens(
        totalTokens,              // regular IO tokens
        elasticTokens,            // elastic IO tokens
        diskWriteTokens,          // elastic disk write tokens
        diskReadTokens,           // elastic disk read tokens
        totalTokens,              // regular capacity
        elasticTokens,            // elastic capacity
        diskWriteTokensCapacity,  // disk write capacity
        true,                     // lastTick
    )

    // ===== 步骤 8: 更新状态 =====
    ioll.cumL0AddedBytes = m.L0Metrics.CumL0AddedBytes
    ioll.smoothedIntL0CompactedBytes = intervalCompactedBytes
    ioll.cumWriteStallCount = m.WriteStallCount

    // 返回 true 表示系统有负载
    return intervalL0AddedBytes > 0
}
```

**关键算法**：

1. **L0 健康分数**：
   ```
   score = max(
       subLevelCount / l0SubLevelCountOverloadThreshold,
       fileCount / l0FileCountOverloadThreshold
   )

   score > 1.0: 过载（红色）
   score ∈ [0.5, 1.0]: 预警（黄色）
   score < 0.5: 健康（绿色）
   ```

2. **Flush 利用率的自适应调整**：
   ```
   有 write stall: targetFraction *= 0.95（更保守）
   无 write stall: targetFraction *= 1.05（更激进）

   最小值：MinFlushUtilizationFraction（默认 0.5）
   最大值：maxFlushUtilTargetFraction（默认 0.9）
   ```

3. **Token 分配策略**：
   ```
   健康状态（score < 0.5）:
   ├─ Regular: 100% of compaction/flush capacity
   └─ Elastic: 80% of Regular

   过载状态（score > 1.0）:
   ├─ Regular: 50% of compaction/flush capacity
   └─ Elastic: 20% of Regular
   ```

### 3.4 Token 分配：`allocateTokensTick()`

**位置**：`pkg/util/admission/io_load_listener.go:750-850`（简化版）

**职责**：将 15 秒的 token 分摊到每个 tick（1ms 或 250ms）

```go
func (ioll *ioLoadListener) allocateTokensTick(remainingTicks int64) {
    // ===== 计算本次 tick 应分配的 token =====
    // 例如：totalTokens = 15,000,000 bytes, remainingTicks = 15000
    // → tokensThisTick = 1000 bytes
    tokensThisTick := ioll.totalNumByteTokens / remainingTicks

    // 处理整数除法的余数
    remainder := ioll.totalNumByteTokens % remainingTicks
    if remainder > 0 {
        tokensThisTick++
        ioll.totalNumByteTokens--
    }

    // ===== 同样处理 elastic tokens =====
    elasticTokensThisTick := ioll.totalNumElasticByteTokens / remainingTicks
    // ...

    // ===== 同样处理 disk write tokens =====
    diskWriteTokensThisTick := ioll.diskWriteTokens / remainingTicks
    // ...

    // ===== 累计已分配的 token =====
    ioll.byteTokensAllocated += tokensThisTick
    ioll.elasticByteTokensAllocated += elasticTokensThisTick
    ioll.diskWriteTokensAllocated += diskWriteTokensThisTick

    // ===== 传递给 granter =====
    lastTick := (remainingTicks == 1)
    tokensUsed, tokensUsedByElastic := ioll.kvGranter.setAvailableTokens(
        tokensThisTick,
        elasticTokensThisTick,
        diskWriteTokensThisTick,
        diskReadTokensThisTick,
        tokensThisTick,           // capacity
        elasticTokensThisTick,    // elastic capacity
        diskWriteTokensThisTick,  // disk write capacity
        lastTick,
    )

    // ===== 累计已使用的 token（用于下次计算）=====
    ioll.byteTokensUsed += tokensUsed
    ioll.byteTokensUsedByElasticWork += tokensUsedByElastic
}
```

**为什么要分摊？**

```
问题：如果每 15s 一次性给 15,000,000 tokens

风险 1：Burst 过大
├─ 前 1 秒可能消耗完所有 token
├─ 后 14 秒所有请求被阻塞
└─ 导致延迟抖动

风险 2：无法快速响应过载
├─ L0 突然升高（例如大量 flush）
├─ 但还有 10s 的 token 未消耗
└─ 系统无法及时限流

解决方案：每 1ms 分配一次
├─ 每次分配: 15,000,000 / 15000 = 1000 tokens
├─ 平滑写入速率
└─ 快速响应负载变化
```

### 3.5 工作完成时的 Token 调整：`storeReplicatedWorkAdmittedLocked()`

**位置**：`pkg/util/admission/granter.go:800-950`（简化版）

**职责**：根据实际字节数调整 token，更新估算模型

```go
func (sg *kvStoreTokenGranter) storeReplicatedWorkAdmittedLocked(
    workType admissionpb.StoreWorkType,
    originalTokens int64,
    admittedInfo storeReplicatedWorkAdmittedInfo,
    canGrantAnother bool,
) (additionalTokens int64) {
    // ===== 步骤 1: 计算实际消耗 =====
    actualL0Bytes := admittedInfo.WriteBytes
    actualIngestedBytes := admittedInfo.IngestedBytes

    // ===== 步骤 2: 应用 linear models =====
    // L0 write model: actualL0Tokens = l0WriteLM(actualL0Bytes)
    actualL0Tokens := sg.mu.l0WriteLM.applyLinearModel(actualL0Bytes)

    // Ingest model: actualIngestTokens = ingestLM(actualIngestedBytes)
    actualIngestTokens := sg.mu.ingestLM.applyLinearModel(actualIngestedBytes)

    totalActualTokens := actualL0Tokens + actualIngestTokens

    // ===== 步骤 3: 计算差额 =====
    additionalTokens = totalActualTokens - originalTokens

    // ===== 步骤 4: 调整 availableIOTokens =====
    if additionalTokens > 0 {
        // 低估了：补扣 token
        sg.subtractIOTokensLocked(additionalTokens, additionalTokens, false)
    } else if additionalTokens < 0 {
        // 高估了：归还 token
        sg.subtractIOTokensLocked(additionalTokens, additionalTokens, false)
    }

    // ===== 步骤 5: 调整磁盘写 token =====
    actualDiskWriteTokens := sg.mu.writeAmpLM.applyLinearModel(actualL0Bytes + actualIngestedBytes)
    estimatedDiskWriteTokens := sg.mu.writeAmpLM.applyLinearModel(originalTokens)
    diskWriteDelta := actualDiskWriteTokens - estimatedDiskWriteTokens

    sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteDelta
    sg.mu.diskTokensUsed[workType].writeByteTokens += actualDiskWriteTokens

    // ===== 步骤 6: 更新 metrics =====
    sg.tokensReturnedMetric.Inc(-additionalTokens)

    // ===== 步骤 7: 尝试授权等待请求 =====
    if canGrantAnother {
        wasExhausted := (sg.mu.availableIOTokens[admissionpb.RegularWorkClass] <= 0)
        nowNotExhausted := (sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0)

        if wasExhausted && nowNotExhausted {
            // 从耗尽状态恢复，尝试授权
            sg.tryGrantLocked()
        }
    }

    return additionalTokens
}
```

**Linear Model 的作用**：

```
问题：为什么需要 model？

原因 1：估算不准确
├─ 准入时只知道 logical bytes（例如 100KB）
├─ 实际写入可能更多（压缩、metadata）
└─ 需要在完成时校正

原因 2：写放大
├─ 写入 L0 的 100KB 可能触发 compaction
├─ 实际磁盘写入 = 100KB * write_amp（例如 10）
└─ 需要扣除更多 token

Linear Model 示例：
actualTokens = slope * logicalBytes + intercept

例如：l0WriteLM = 1.2x + 100
├─ logicalBytes = 1000
├─ actualTokens = 1.2 * 1000 + 100 = 1300
└─ 需要补扣 300 tokens

模型更新（每 15s）：
├─ 收集过去 15s 的 (logicalBytes, actualBytes) 样本
├─ 线性回归拟合新的 slope 和 intercept
└─ 用于下个 15s 的估算
```

---

## 四、动态行为分析（Runtime 行为）

### 4.1 L0 负载的感知机制

**信号源**：Pebble metrics（每 15s 采样）

```go
// pkg/util/admission/io_load_listener.go:240-296

type ioLoadListenerState struct {
    // ===== L0 状态（Gauge）=====
    curL0Bytes          int64   // 当前 L0 总字节数
    l0NumFiles          int64   // 当前 L0 文件数
    l0NumSubLevels      int32   // 当前 L0 sub-level 数

    // ===== L0 变化（Cumulative）=====
    cumL0AddedBytes     uint64  // 累计写入 L0 的字节数

    // ===== 指标（Smoothed）=====
    smoothedIntL0CompactedBytes int64  // 平滑后的 L0 compact 出去的字节数
    smoothedCompactionByteTokens float64  // 平滑后的 compaction tokens
    smoothedNumFlushTokens      float64   // 平滑后的 flush tokens

    // ===== 自适应参数 =====
    flushUtilTargetFraction float64  // Flush 利用率目标（动态调整）
}
```

**感知流程**：

```
T=0s: 采集 metrics
├─ m := pmp.GetPebbleMetrics()[storeID]
├─ L0Metrics.NumSubLevels = 18
├─ L0Metrics.NumFiles = 3500
├─ L0Metrics.TotalBytes = 2.5GB
└─ L0Metrics.CumL0AddedBytes = 50GB

T=0s: 计算健康分数
├─ score := max(
│       18 / 20.0,         // sub-level score = 0.9
│       3500 / 4000.0      // file count score = 0.875
│   ) = 0.9
│
├─ score < 1.0 → 未过载（但接近阈值）
└─ 决策：维持当前 token 水平

T=15s: 再次采集
├─ L0Metrics.NumSubLevels = 22  ← 增加了 4 个 sub-levels
├─ L0Metrics.NumFiles = 4100
├─ L0Metrics.CumL0AddedBytes = 52GB  ← 增长 2GB
└─ score := max(22/20, 4100/4000) = 1.1

T=15s: 过载响应
├─ score > 1.0 → 过载！
├─ 计算 compaction 速率:
│   intervalCompacted = (L0CompactedBytes_now - L0CompactedBytes_prev)
│   = 1.8GB（compaction 跟不上写入）
│
├─ 原本 token = 1.8GB / 15s = 120MB/s
├─ 过载惩罚：token *= 0.5
└─ 新 token = 60MB/s（写入速率减半）

T=30s: 观察效果
├─ 由于 token 减半，写入速率下降
├─ L0Metrics.NumSubLevels = 20  ← 恢复到阈值
└─ score = 1.0 → 边界状态
```

### 4.2 Token 调整的时机与策略

**调整周期**：15 秒（`adjustmentInterval`）

**为什么是 15 秒？**

```
太短（例如 1s）:
├─ Compaction 可能未完成
├─ Metrics 采样不稳定
└─ 导致 token 抖动

太长（例如 60s）:
├─ 响应过载太慢
├─ L0 可能快速堆积
└─ 错过干预时机

15s 是平衡点：
├─ 大多数 L0 compaction < 10s
├─ 足够观察趋势
└─ 足够快速响应
```

**调整策略矩阵**：

| 当前状态 | L0 Score | Write Stall | 动作 |
|---------|----------|-------------|------|
| 健康 | < 0.5 | 无 | 增加 token（利用率 +5%） |
| 预警 | 0.5 - 1.0 | 无 | 维持 token |
| 预警 | 0.5 - 1.0 | 有 | 减少 token（利用率 -5%） |
| 过载 | > 1.0 | - | 大幅减少 token（-50%） |

**示例：自适应调整的完整周期**

```
初始状态：
├─ flushUtilTargetFraction = 0.75
├─ flushRate = 100MB/s
└─ flushTokens = 100MB/s * 0.75 = 75MB/s

T=15s: 无 write stall
├─ 系统健康，可以更激进
├─ flushUtilTargetFraction *= 1.05 = 0.7875
└─ flushTokens = 100MB/s * 0.7875 = 78.75MB/s

T=30s: 无 write stall
├─ flushUtilTargetFraction *= 1.05 = 0.826875
└─ flushTokens = 82.69MB/s

T=45s: 出现 write stall！
├─ flushUtilTargetFraction *= 0.95 = 0.785531
└─ flushTokens = 78.55MB/s（降低）

T=60s: 无 write stall
├─ 恢复正常
├─ flushUtilTargetFraction *= 1.05 = 0.824808
└─ flushTokens = 82.48MB/s

收敛到平衡点：
└─ flushUtilTargetFraction 在 0.75 - 0.85 之间震荡
```

### 4.3 Regular vs Elastic 的隔离机制

**Token 层面的隔离**：

```go
// Regular work 的准入条件：
availableIOTokens[RegularWorkClass] > 0

// Elastic work 的准入条件：
availableIOTokens[RegularWorkClass] > 0 &&
availableIOTokens[ElasticWorkClass] > 0 &&
diskTokensAvailable.writeByteTokens > 0
```

**隔离效果**：

```
场景 1：健康状态（score < 0.5）
├─ regularTokens = 100MB/s
├─ elasticTokens = 80MB/s
└─ 两者都能充分利用资源

场景 2：预警状态（score = 0.8）
├─ regularTokens = 100MB/s（维持）
├─ elasticTokens = 60MB/s（减少）
└─ Regular 不受影响，Elastic 开始限流

场景 3：过载状态（score = 1.2）
├─ regularTokens = 50MB/s（减半）
├─ elasticTokens = 10MB/s（Regular 的 20%）
└─ Elastic 几乎停止，Regular 也被限制

关键设计：
├─ Regular 总是有优先权（条件更宽松）
├─ Elastic 受多重约束（三种 token）
└─ 过载时牺牲 Elastic 保护 Regular
```

**示例：Elastic 被限流的完整过程**

```
T=0s: 系统正常
├─ L0 score = 0.3
├─ Regular tokens: 10000 available
├─ Elastic tokens: 8000 available
├─ Regular work: 吞吐量 80MB/s
└─ Elastic work: 吞吐量 60MB/s

T=60s: 开始写入高峰
├─ 大量 bulk import（elastic work）
├─ L0 写入速率飙升到 200MB/s
├─ L0 score = 0.6
└─ Compaction 开始跟不上

T=75s: 第一次调整
├─ L0 score = 0.7
├─ pebbleMetricsTick():
│   ├─ Regular tokens: 维持 10000
│   └─ Elastic tokens: 减少到 5000
├─ Elastic work 开始排队
└─ Elastic 吞吐量: 60MB/s → 40MB/s

T=90s: 继续恶化
├─ L0 score = 0.95
├─ pebbleMetricsTick():
│   ├─ Regular tokens: 减少到 8000
│   └─ Elastic tokens: 减少到 2000
└─ Elastic 吞吐量: 40MB/s → 15MB/s

T=105s: 触发过载
├─ L0 score = 1.1
├─ pebbleMetricsTick():
│   ├─ Regular tokens: 减少到 4000（-50%）
│   └─ Elastic tokens: 减少到 800（Regular 的 20%）
├─ Elastic 几乎停止
└─ Elastic 吞吐量: 15MB/s → 5MB/s

T=120s: 开始恢复
├─ 由于写入减少，compaction 追上来
├─ L0 score = 0.8
├─ pebbleMetricsTick():
│   ├─ Regular tokens: 恢复到 6000
│   └─ Elastic tokens: 恢复到 3000
└─ Elastic 吞吐量: 5MB/s → 20MB/s

T=180s: 完全恢复
├─ L0 score = 0.4
├─ Regular tokens: 10000
├─ Elastic tokens: 8000
└─ 恢复正常吞吐量
```

---

## 五、具体示例（必须有）

### 5.1 示例 1：Store 过载时的完整响应链路

**初始状态**：

```
Store 1:
├─ L0 sub-levels: 8
├─ L0 files: 1500
├─ L0 score: 0.4（健康）
├─ availableIOTokens[Regular]: 8000
├─ availableIOTokens[Elastic]: 6400
└─ 当前吞吐量: 120MB/s
```

**时间线**：

```
T=0s: 突发写入高峰
────────────────────────────────────────────────
事件：10 个并发 bulk import 任务开始执行
├─ 每个任务请求 1000 tokens
├─ StoreWorkQueue.Admit():
│   ├─ granter.tryGet(1000)
│   ├─ availableIOTokens[Elastic] = 6400
│   ├─ 6400 ≥ 1000 → 前 6 个请求通过
│   └─ 后 4 个请求入队等待
│
└─ 写入速率瞬间达到 180MB/s

T=1s: L0 开始堆积
────────────────────────────────────────────────
├─ L0 sub-levels: 8 → 10
├─ Compaction 速率: 100MB/s（跟不上写入）
├─ Token 消耗:
│   ├─ 已分配: 1000 tokens/ms * 1000ms = 1,000,000 tokens
│   └─ 已使用: ~1,200,000 tokens（超出预期）
└─ availableIOTokens[Elastic]: 接近 0

T=5s: Elastic 完全限流
────────────────────────────────────────────────
├─ availableIOTokens[Elastic] = 0
├─ 所有新的 elastic 请求被阻塞
├─ 队列长度: 15 个等待请求
├─ L0 sub-levels: 10 → 13
└─ L0 score: 0.65

T=15s: 第一次 adjustment interval 完成
────────────────────────────────────────────────
pebbleMetricsTick():

1. 读取 metrics:
   ├─ L0 sub-levels: 16
   ├─ L0 files: 2800
   ├─ L0 score: max(16/20, 2800/4000) = 0.8
   └─ cumL0AddedBytes: +3GB（过去 15s）

2. 计算 compaction tokens:
   ├─ intervalCompacted: 1.5GB（compaction 速率）
   ├─ score = 0.8 < 1.0（未过载）
   └─ compactionTokens = 1.5GB / 15s = 100MB/s

3. 计算 flush tokens:
   ├─ flushRate: 120MB/s
   ├─ flushUtilTargetFraction: 0.75
   └─ flushTokens = 120MB/s * 0.75 = 90MB/s

4. 决策:
   ├─ totalTokens = min(100MB/s, 90MB/s) = 90MB/s
   ├─ regularTokens = 90MB/s
   └─ elasticTokens = 90MB/s * 0.6 = 54MB/s（减少）

5. 传递给 granter:
   setAvailableTokens(
       90 * 1024 * 1024 * 15,  // 1.35GB for 15s
       54 * 1024 * 1024 * 15,  // 810MB for 15s
       ...
   )

T=16s: Token 开始恢复分配
────────────────────────────────────────────────
allocateTokensTick(remainingTicks=14999):
├─ tokensThisTick = 1,350,000,000 / 14999 ≈ 90,000
├─ elasticTokensThisTick = 810,000,000 / 14999 ≈ 54,000
├─ availableIOTokens[Regular] += 90,000
├─ availableIOTokens[Elastic] += 54,000
└─ granter.tryGrant() → 授权 2 个等待的 elastic 请求

T=20s: 观察效果
────────────────────────────────────────────────
├─ Elastic 吞吐量: 180MB/s → 54MB/s
├─ L0 写入速率下降
├─ L0 sub-levels: 16 → 15（开始下降）
└─ Compaction 开始追上来

T=30s: 第二次 adjustment interval
────────────────────────────────────────────────
pebbleMetricsTick():

1. L0 score: max(15/20, 2400/4000) = 0.75
2. 趋势：正在恢复
3. 决策：
   ├─ regularTokens = 95MB/s（微增）
   └─ elasticTokens = 60MB/s（恢复中）

T=60s: 恢复正常
────────────────────────────────────────────────
├─ L0 sub-levels: 9
├─ L0 score: 0.45
├─ regularTokens = 120MB/s
├─ elasticTokens = 96MB/s
└─ 系统回到健康状态
```

### 5.2 示例 2：多 Store 的独立控制

**场景**：双 Store 节点，Store 1 过载，Store 2 正常

```
初始状态：
────────────────────────────────────────────────
Store 1:
├─ 磁盘: SATA HDD（慢速磁盘）
├─ L0 sub-levels: 18
├─ L0 score: 0.9
└─ availableIOTokens[Regular]: 2000

Store 2:
├─ 磁盘: NVMe SSD（快速磁盘）
├─ L0 sub-levels: 5
├─ L0 score: 0.25
└─ availableIOTokens[Regular]: 10000

请求分布：
├─ Range R1 → Store 1（leader）
├─ Range R2 → Store 1（follower）
├─ Range R3 → Store 2（leader）
└─ Range R4 → Store 2（follower）
```

**T=0s: 同时写入 4 个 Range**

```
写入 R1（Store 1 leader）:
├─ StoreWorkQueue[Store1].Admit():
│   ├─ availableIOTokens[Regular] = 2000
│   ├─ 请求 tokens = 1000
│   ├─ 2000 > 1000 → 通过
│   └─ availableIOTokens[Regular] = 1000
└─ 写入成功

写入 R2（Store 1 follower）:
├─ StoreWorkQueue[Store1].Admit():
│   ├─ availableIOTokens[Regular] = 1000
│   ├─ 请求 tokens = 1000
│   ├─ 1000 ≥ 1000 → 通过
│   └─ availableIOTokens[Regular] = 0
└─ 写入成功

写入 R3（Store 2 leader）:
├─ StoreWorkQueue[Store2].Admit():
│   ├─ availableIOTokens[Regular] = 10000
│   ├─ 请求 tokens = 1000
│   ├─ 10000 > 1000 → 通过
│   └─ availableIOTokens[Regular] = 9000
└─ 写入成功

写入 R4（Store 2 follower）:
├─ StoreWorkQueue[Store2].Admit():
│   ├─ availableIOTokens[Regular] = 9000
│   ├─ 请求 tokens = 1000
│   ├─ 9000 > 1000 → 通过
│   └─ availableIOTokens[Regular] = 8000
└─ 写入成功
```

**T=1ms: 再次写入**

```
写入 R1（Store 1）:
├─ StoreWorkQueue[Store1].Admit():
│   ├─ availableIOTokens[Regular] = 0 + 90（新分配）
│   ├─ 请求 tokens = 1000
│   ├─ 90 < 1000 → 失败
│   └─ 请求入队等待
└─ 写入被阻塞 ❌

写入 R3（Store 2）:
├─ StoreWorkQueue[Store2].Admit():
│   ├─ availableIOTokens[Regular] = 8000 + 1000（新分配）
│   ├─ 请求 tokens = 1000
│   ├─ 9000 > 1000 → 通过
│   └─ availableIOTokens[Regular] = 8000
└─ 写入成功 ✓
```

**关键观察**：

```
Store 1 的过载不影响 Store 2:
├─ Store 1 的 token 耗尽 → R1, R2 写入被阻塞
├─ Store 2 的 token 充足 → R3, R4 正常写入
└─ 实现了 per-store 的隔离

如果使用全局 token（错误设计）:
├─ Store 1 的过载会消耗全局 token
├─ Store 2 也会受到影响
└─ 性能隔离失败 ❌
```

---

## 六、设计取舍与权衡（Trade-offs）

### 6.1 Per-Store 独立控制 vs 全局集中控制

**当前设计：Per-Store 独立控制**

```
优点：
├─ 资源隔离：一个 Store 过载不影响其他 Store
├─ 精确控制：每个 Store 的 L0 状态独立评估
├─ 扩展性：可以支持数十个 Store（多磁盘节点）
└─ 灵活性：不同磁盘类型（HDD vs SSD）可以有不同策略

缺点：
├─ 内存开销：每个 Store 独立的 coordinator 和 metrics
├─ 复杂度：需要维护多个 goroutine 和 ticker
└─ 调试困难：需要分别查看每个 Store 的状态
```

**替代方案 1：全局集中控制**

```
设计：一个全局 IO token pool，所有 Store 共享

优点：
├─ 简单：只需一个 granter 和一个 ticker
├─ 内存占用少：共享状态
└─ 易于调试：单一控制点

缺点（致命）：
├─ 无法隔离：一个 Store 过载影响所有 Store
├─ 不公平：快速磁盘的性能被慢速磁盘拖累
└─ 无法适配异构硬件：HDD 和 SSD 需要不同策略

例子：
Store 1（HDD）过载 → L0 score = 1.5
Store 2（SSD）健康 → L0 score = 0.3

全局控制：
├─ 全局 token 被 Store 1 的过载状态主导
├─ Store 2 也被限流
└─ 浪费 SSD 的性能 ❌

Per-Store 控制：
├─ Store 1 限流（token 减少）
├─ Store 2 正常（token 充足）
└─ 充分利用硬件 ✓
```

**结论**：Per-Store 控制是必需的，代价（复杂度和内存）是可接受的。

### 6.2 Token-Based vs Slot-Based

**当前设计：Token-Based**

```
为什么 IO 准入使用 token 而不是 slot？

原因 1：资源消耗的延迟性
├─ 写入完成（归还 slot）≠ 资源释放
├─ 实际工作在 flush 和 compaction（异步）
└─ Slot 无法准确反映 IO 压力

原因 2：工作大小差异大
├─ 小写入: 1KB
├─ 大写入: 10MB
├─ 如果用 slot: 两者占用相同资源（不公平）
└─ Token 可以按字节数收费（公平）

原因 3：无明确完成时机
├─ 写入 WAL 后就返回
├─ 但 LSM 的工作才刚开始
└─ 无法准确知道何时"完成"
```

**如果使用 Slot（错误设计）**：

```
问题 1：大小请求不公平
├─ Request A: 写 100 bytes → 占用 1 slot
├─ Request B: 写 10MB → 占用 1 slot
├─ Request B 对 L0 的压力是 A 的 10 万倍
└─ 但占用相同资源（不公平）❌

问题 2：无法精确控制吞吐量
├─ 假设 slot limit = 100
├─ 如果都是小请求: 吞吐量 = 10KB/s（过于保守）
├─ 如果都是大请求: 吞吐量 = 1GB/s（过于激进）
└─ 无法稳定控制写入速率 ❌

问题 3：无法反映 L0 压力
├─ Slot 只知道"有多少请求在执行"
├─ 不知道"这些请求对 L0 的影响"
└─ 无法基于 L0 状态动态调整 ❌
```

**Token 的优势**：

```
优势 1：按字节计费
├─ 100 bytes → 100 tokens
├─ 10MB → 10,000,000 tokens
└─ 公平反映资源消耗 ✓

优势 2：精确控制吞吐量
├─ Token rate = 100MB/s
├─ 无论请求大小，总吞吐量稳定在 100MB/s
└─ 可预测的 L0 写入速率 ✓

优势 3：可以基于 L0 状态调整
├─ L0 sub-levels 高 → 减少 token rate
├─ L0 sub-levels 低 → 增加 token rate
└─ 精确的反馈控制 ✓
```

### 6.3 15 秒 Adjustment Interval 的权衡

**当前设计：15 秒**

```
选择 15s 的理由：

理由 1：Compaction 时间尺度
├─ 大多数 L0 compaction: 5-15 秒
├─ 需要等待 compaction 完成才能观察效果
└─ < 15s: metrics 不稳定

理由 2：避免过度反应
├─ 如果每 1s 调整: 可能对瞬时抖动过度反应
├─ 15s: 平滑短期波动
└─ 稳定的控制策略

理由 3：Metrics 采集成本
├─ Pebble metrics 采集有开销（mutex）
├─ 每 15s: 对性能影响可忽略
└─ 每 1s: 可能影响写入性能
```

**如果太短（例如 1s）**：

```
问题 1：Metrics 不准确
T=0s: 启动 compaction
T=1s: Compaction 进行中
     ├─ L0CompactedBytes = 0（未完成）
     ├─ 误判为"compaction 速率 = 0"
     └─ 错误决策：大幅减少 token ❌

T=10s: Compaction 完成
     ├─ L0CompactedBytes = 500MB
     ├─ 实际速率 = 50MB/s
     └─ 但已经错误限流了 10 秒 ❌

问题 2：过度反应瞬时波动
T=0s: 正常
T=1s: 瞬时 burst（5 个大请求同时到达）
     ├─ L0AddedBytes 瞬间增加
     ├─ 减少 token
     └─ 但 burst 已经结束（过度反应）❌

问题 3：Token 抖动
├─ 每 1s 大幅调整 token
├─ 吞吐量剧烈波动
└─ 延迟不稳定（p99 延迟高）❌
```

**如果太长（例如 60s）**：

```
问题 1：响应过载太慢
T=0s: 正常（L0 score = 0.5）
T=10s: 突发写入高峰
     ├─ L0 sub-levels: 10 → 25
     ├─ L0 score: 1.25（严重过载）
     └─ 但还要等 50s 才能调整 ❌

T=60s: 第一次调整
     ├─ 减少 token
     ├─ 但 L0 已经堆积到 50 sub-levels
     └─ 可能触发 write stall ❌

问题 2：浪费资源
T=0s: 过载（L0 score = 1.5）
T=10s: 写入高峰结束
     ├─ L0 score: 1.5 → 0.8（compaction 追上来）
     ├─ 但 token 还维持在低水平
     └─ 浪费 50s 的写入能力 ❌
```

**结论**：15s 是经验值，在以下三者之间取得平衡：
- Metrics 稳定性
- 响应速度
- 控制稳定性

### 6.4 Token 分摊（1ms tick）的权衡

**当前设计：每 1ms 分配一次 token**

```
为什么分摊到 1ms？

原因 1：平滑 burst
├─ 如果一次性给 15s 的 token（1.5GB）
├─ 前 1s 可能消耗完所有 token
├─ 后 14s 所有请求阻塞
└─ 延迟抖动大 ❌

分摊后：
├─ 每 1ms 给 100KB token
├─ 写入速率平滑
└─ 延迟稳定 ✓

原因 2：快速响应队列
├─ 请求在队列中等待
├─ 每 1ms 有新 token → 可以立即授权
└─ 平均等待时间 = 0.5ms（可接受）

原因 3：与 CPU scheduler 同步
├─ goschedstats 采样周期 = 1ms
├─ Token 分配与 CPU load 同步
└─ 协调 IO 和 CPU 准入 ✓
```

**如果太慢（例如 100ms）**：

```
问题：队列等待时间长
├─ 请求到达 → 入队
├─ 等待下一次 tick（最多 100ms）
├─ p99 延迟增加 100ms
└─ 用户体验差 ❌
```

**如果太快（例如 0.1ms）**：

```
问题：CPU 开销高
├─ Ticker goroutine 每 0.1ms 唤醒
├─ 每 15s 调用 150,000 次 allocateTokensTick()
├─ 锁竞争增加
└─ CPU 占用增加 ❌
```

**结论**：1ms 是最优选择：
- 延迟影响小（< 1ms）
- CPU 开销可接受
- 与 CPU scheduler 同步

---

## 七、总结与心智模型

### 7.1 核心思想总结

`StoreGrantCoordinators` 实现了一个 **基于 Pebble metrics 反馈的自适应 IO 准入控制系统**，核心思想是：

> **将磁盘 IO 抽象为 token，根据 L0 层的健康状况动态调整 token 发放速率，从而在保护磁盘过载和最大化资源利用率之间取得平衡。**

关键设计原则：

1. **Per-Store 隔离**：每个 Store 独立评估和控制，避免相互干扰
2. **Token 而非 Slot**：按字节计费，公平反映资源消耗
3. **反馈控制**：L0 metrics → 评估健康分数 → 调整 token rate → 影响写入速率 → 再次评估
4. **分层隔离**：Regular 和 Elastic 分别管理，过载时优先保护 Regular

### 7.2 心智模型

**如果只记住一件事，那就是**：

> `StoreGrantCoordinators` 是一个 **反馈控制器**（feedback controller），把 L0 sub-level count 当作"温度计"，把 token rate 当作"空调开关"，目标是让"温度"（L0 健康分数）稳定在 0.5 附近。

**类比**：

```
StoreGrantCoordinators ≈ 空调的温控系统

传感器（Sensor）：
├─ Pebble metrics（L0 sub-levels, file count）
└─ 每 15s 采样一次"温度"

控制器（Controller）：
├─ pebbleMetricsTick()
├─ 计算偏差：score - target（例如 0.8 - 0.5 = 0.3）
└─ 决定动作：增加/减少 token

执行器（Actuator）：
├─ allocateTokensTick()
├─ 每 1ms 分配 token
└─ 影响写入速率

反馈环路（Feedback Loop）：
├─ 写入速率 ↓ → L0 增长慢 ↓ → score ↓
└─ Score ↓ → token rate ↑ → 写入速率 ↑

目标（Setpoint）：
└─ L0 score ≈ 0.5（健康状态）
```

### 7.3 简化伪代码

```python
# 简化的 StoreGrantCoordinators 逻辑

class StoreGrantCoordinator:
    def __init__(self, store_id):
        self.store_id = store_id
        self.available_tokens = 10000
        self.total_tokens_for_15s = 10000
        self.target_score = 0.5

    # ===== 每 15s 调用一次 =====
    def pebble_metrics_tick(self, metrics):
        # 1. 计算健康分数
        score = max(
            metrics.l0_sub_levels / 20.0,
            metrics.l0_file_count / 4000.0
        )

        # 2. 计算偏差
        error = score - self.target_score

        # 3. PID 控制（简化为 P 控制）
        if error > 0.5:  # 严重过载
            adjustment_factor = 0.5
        elif error > 0:  # 轻微过载
            adjustment_factor = 0.8
        elif error < -0.2:  # 资源浪费
            adjustment_factor = 1.2
        else:  # 健康
            adjustment_factor = 1.0

        # 4. 计算 token
        compaction_rate = metrics.l0_compacted_bytes / 15  # bytes/s
        flush_rate = metrics.flush_throughput

        base_tokens = min(compaction_rate, flush_rate) * 15
        self.total_tokens_for_15s = base_tokens * adjustment_factor

        # 5. 重置分配计数
        self.ticks_remaining = 15000

    # ===== 每 1ms 调用一次 =====
    def allocate_tokens_tick(self):
        # 平滑分配
        tokens_this_tick = self.total_tokens_for_15s / self.ticks_remaining
        self.available_tokens += tokens_this_tick
        self.ticks_remaining -= 1

        # 尝试授权等待请求
        self.try_grant()

    # ===== 请求准入 =====
    def admit(self, request):
        estimated_tokens = request.estimated_bytes

        # 快速路径
        if self.available_tokens >= estimated_tokens:
            self.available_tokens -= estimated_tokens
            return GRANTED

        # 慢路径：入队等待
        queue.enqueue(request)
        return QUEUED

    # ===== 工作完成 =====
    def work_done(self, request):
        actual_tokens = request.actual_bytes
        estimated_tokens = request.estimated_tokens

        # 调整 token（可能为负，表示归还）
        delta = actual_tokens - estimated_tokens
        self.available_tokens -= delta

        # 尝试授权等待请求
        self.try_grant()

# ===== 多 Store 管理 =====
class StoreGrantCoordinators:
    def __init__(self):
        self.coordinators = {}  # store_id -> StoreGrantCoordinator

    def init_store(self, store_id):
        self.coordinators[store_id] = StoreGrantCoordinator(store_id)

    def get_coordinator(self, store_id):
        return self.coordinators[store_id]

    # 后台 goroutine
    def background_ticker(self):
        while True:
            # 每 15s 调整 token 总量
            if time_elapsed % 15 == 0:
                metrics = pebble.get_metrics()
                for store_id, m in metrics:
                    coord = self.coordinators[store_id]
                    coord.pebble_metrics_tick(m)

            # 每 1ms 分配 token
            for coord in self.coordinators.values():
                coord.allocate_tokens_tick()

            sleep(1ms)
```

### 7.4 关键要点速查表

| 维度 | 设计决策 | 理由 |
|------|---------|------|
| 粒度 | Per-Store 独立控制 | 资源隔离，避免相互干扰 |
| 资源抽象 | Token（按字节） | 公平反映工作量，精确控制吞吐量 |
| 调整周期 | 15 秒 | 平衡 metrics 稳定性和响应速度 |
| 分配频率 | 1ms | 平滑 burst，减少延迟抖动 |
| 隔离机制 | Regular + Elastic 分层 | 过载时优先保护高优先级工作 |
| 反馈信号 | L0 sub-levels + file count | 准确反映写入压力 |
| 目标状态 | L0 score ≈ 0.5 | 留出安全余量，避免 write stall |
| 过载策略 | Token rate *= 0.5 | 快速降低写入速率，保护磁盘 |
| 恢复策略 | Token rate *= 1.2 | 逐步恢复，避免振荡 |

---

## 附录：代码位置索引

| 组件 | 文件位置 | 行号 |
|------|---------|------|
| `StoreGrantCoordinators` | `pkg/util/admission/store_grant_coordinator.go` | 50-102 |
| `storeGrantCoordinator` | `pkg/util/admission/store_grant_coordinator.go` | 388-398 |
| `initGrantCoordinator()` | `pkg/util/admission/store_grant_coordinator.go` | 229-312 |
| `SetPebbleMetricsProvider()` | `pkg/util/admission/store_grant_coordinator.go` | 122-227 |
| `kvStoreTokenGranter` | `pkg/util/admission/granter.go` | 328-410 |
| `tryGetLocked()` | `pkg/util/admission/granter.go` | 476-579 |
| `ioLoadListener` | `pkg/util/admission/io_load_listener.go` | 226-240 |
| `pebbleMetricsTick()` | `pkg/util/admission/io_load_listener.go` | 500-700 |
| `allocateTokensTick()` | `pkg/util/admission/io_load_listener.go` | 750-850 |

---

**本章完**。下一章将深入分析 `ioLoadListener` 的 token 计算算法和 linear model 的动态更新机制。
