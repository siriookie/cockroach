# 第十九章：ioLoadListener——基于 LSM 状态的自适应 Token 计算与 Linear Model 动态更新

## 一、第一轮 BFS：整体职责与设计动机（Why）

### 1.1 存在背景与要解决的问题

在第十七章中，我们看到 `StoreGrantCoordinators` 通过 `kvStoreTokenGranter` 管理 IO tokens。但有一个核心问题尚未回答：

**这些 tokens 是从哪里来的？数量是如何确定的？**

这就是 `ioLoadListener` 的核心职责：**根据 LSM（Log-Structured Merge-tree）的实时状态，动态计算并分配 IO tokens**。

**问题 1：IO 资源的不可预测性**

与 CPU 不同，IO 资源的消耗具有 **延迟性** 和 **放大效应**：

```
用户写入 1MB 数据：
├─ 步骤 1：写入 WAL（瞬间完成）
├─ 步骤 2：写入 MemTable（瞬间完成）
│   └─ Admit() 调用在这里返回 ✓
│
├─ 步骤 3：MemTable 满，触发 Flush（异步，10s 后）
│   └─ 写入 L0：1MB × 1.5（压缩）= 1.5MB
│
├─ 步骤 4：L0 触发 Compaction（异步，30s 后）
│   └─ 写入 L1：1.5MB × 5（写放大）= 7.5MB
│
└─ 总 IO 开销：1MB + 1.5MB + 7.5MB = 10MB（10x 放大！）
```

**传统 slot-based 模型失效**：
- Slot 在 "步骤 2" 结束后就归还了
- 但真正的 IO 工作（步骤 3, 4）才刚开始
- 无法用 slot 模型表达这种 "延迟 + 放大" 的资源消耗

**问题 2：LSM 状态的动态变化**

LSM 的健康状态会影响系统吞吐能力：

| L0 Sub-Level 数量 | 系统状态 | 可接受的写入速率 |
|------------------|----------|---------------|
| < 5 | 健康 | 无限制 |
| 5 - 10 | 预警 | 降低 20% |
| 10 - 20 | 过载 | 降低 50% |
| > 20 | 严重过载 | 降低 90% |

**问题 3：估算的不准确性**

准入时，系统并不知道请求会产生多少实际 IO：

```
用户请求：写入 Key-Value pair

准入时已知信息：
├─ Key 长度：10 bytes
├─ Value 长度：1000 bytes
└─ 预估 L0 字节数：1010 bytes

实际 IO 开销：
├─ Raft log overhead：50 bytes
├─ State machine 应用：2020 bytes（写放大 2x）
├─ 压缩后写入 L0：1500 bytes（压缩率 0.75）
├─ 后续 compaction：7500 bytes（写放大 5x）
└─ 总计：11070 bytes（11x 于预估！）
```

**CockroachDB 的解决方案**：

`ioLoadListener` 通过以下机制解决上述问题：

1. **Token-based 模型**：token 代表字节数，可以精确表达 IO 开销
2. **动态计算**：每 15s 根据 Pebble metrics 重新计算 token 分配
3. **Linear Model**：通过机器学习方法，从历史数据中学习 "预估字节 → 实际 IO" 的映射关系
4. **分摊分配**：将 15s 的 tokens 分摊到每 1ms，避免 burst

### 1.2 在系统中的位置

```
┌────────────────────────────────────────────────────┐
│           CockroachDB Store                         │
├────────────────────────────────────────────────────┤
│                                                     │
│  ┌───────────────────────────────────────────┐    │
│  │  StoreGrantCoordinator                    │    │
│  ├───────────────────────────────────────────┤    │
│  │                                            │    │
│  │  ┌────────────────────────────────┐       │    │
│  │  │  kvStoreTokenGranter           │       │    │
│  │  │  ├─ availableIOTokens[Regular] │       │    │
│  │  │  ├─ availableIOTokens[Elastic] │       │    │
│  │  │  └─ diskTokensAvailable        │       │    │
│  │  └────────────────────────────────┘       │    │
│  │          ▲                                 │    │
│  │          │ setAvailableTokens(...)         │    │
│  │          │                                 │    │
│  │  ┌───────┴────────────────────────┐       │    │
│  │  │  ioLoadListener ◄── 本章重点   │       │    │
│  │  ├────────────────────────────────┤       │    │
│  │  │  每 15s 触发：                  │       │    │
│  │  │  • pebbleMetricsTick()         │       │    │
│  │  │    ├─ 读取 Pebble metrics      │       │    │
│  │  │    ├─ 计算 L0 健康分数         │       │    │
│  │  │    ├─ 计算 compaction tokens   │       │    │
│  │  │    ├─ 计算 flush tokens        │       │    │
│  │  │    └─ 更新 Linear Models       │       │    │
│  │  │                                 │       │    │
│  │  │  每 1ms 触发：                  │       │    │
│  │  │  • allocateTokensTick()        │       │    │
│  │  │    └─ 分摊分配 tokens          │       │    │
│  │  └────────────────────────────────┘       │    │
│  │          │                                 │    │
│  │          │ metrics                         │    │
│  │          ▼                                 │    │
│  │  ┌────────────────────────────────┐       │    │
│  │  │  Pebble LSM                    │       │    │
│  │  │  ├─ L0 sub-levels: 12          │       │    │
│  │  │  ├─ L0 files: 2500             │       │    │
│  │  │  ├─ L0 bytes: 3.5GB            │       │    │
│  │  │  ├─ Flush throughput: 80MB/s   │       │    │
│  │  │  └─ Compaction stats: {...}    │       │    │
│  │  └────────────────────────────────┘       │    │
│  └────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────┘
```

**协作模块**：

1. **Pebble**：LSM 存储引擎，提供 metrics
2. **kvStoreTokenGranter**：执行 token 分配决策
3. **storePerWorkTokenEstimator**：管理 Linear Models
4. **diskBandwidthLimiter**：计算磁盘带宽 tokens

### 1.3 核心对象与关键状态

#### 核心结构体

```go
// pkg/util/admission/io_load_listener.go:226-240

type ioLoadListener struct {
    storeID     roachpb.StoreID
    settings    *cluster.Settings
    kvRequester storeRequester       // StoreWorkQueue
    kvGranter   granterWithIOTokens  // kvStoreTokenGranter

    // ===== 状态管理 =====
    statsInitialized bool
    adjustTokensResult                      // 包含 ioLoadListenerState
    perWorkTokenEstimator storePerWorkTokenEstimator  // Linear Models
    diskBandwidthLimiter  *diskBandwidthLimiter

    // ===== Metrics =====
    l0CompactedBytes *metric.Counter
    l0TokensProduced *metric.Counter
}
```

#### 状态快照

```go
// pkg/util/admission/io_load_listener.go:244-299

type ioLoadListenerState struct {
    // ===== 累计值（Cumulative）=====
    cumL0AddedBytes uint64  // 累计写入 L0 的字节数
    cumWriteStallCount int64  // 累计 write stall 次数
    cumFlushWriteThroughput pebble.ThroughputMetric  // 累计 flush 吞吐量
    cumCompactionStats cumStoreCompactionStats  // 累计 compaction 统计

    // ===== 瞬时值（Gauge）=====
    curL0Bytes int64  // 当前 L0 总字节数

    // ===== 平滑值（Smoothed）=====
    smoothedIntL0CompactedBytes int64  // 平滑后的 L0 compact 速率
    smoothedCompactionByteTokens float64  // 平滑后的 compaction tokens
    smoothedNumFlushTokens float64  // 平滑后的 flush tokens
    flushUtilTargetFraction float64  // Flush 利用率目标（自适应）

    // ===== Token 分配状态 =====
    totalNumByteTokens  int64  // 本周期总 tokens（15s）
    byteTokensAllocated int64  // 已分配的 tokens
    byteTokensUsed      int64  // 已使用的 tokens

    totalNumElasticByteTokens  int64  // Elastic 专用 tokens
    elasticByteTokensAllocated int64

    diskWriteTokens          int64  // 磁盘写 tokens
    diskWriteTokensAllocated int64
    diskReadTokens           int64  // 磁盘读 tokens
    diskReadTokensAllocated  int64
}
```

**关键设计点**：

1. **双时间尺度**：
   - **15s adjustment interval**：重新计算 token 总量
   - **1ms allocation tick**：分摊分配 tokens

2. **三种 token 维度**：
   - **IO tokens (L0-based)**：基于 L0 compaction/flush 容量
   - **Elastic IO tokens**：Elastic 工作专用，更严格限制
   - **Disk bandwidth tokens**：基于磁盘带宽

3. **指数平滑**：避免瞬时抖动
   ```
   smoothedValue = α * currentValue + (1 - α) * prevSmoothedValue
   其中 α = 0.5（默认）
   ```

---

## 二、第二轮 BFS：控制流与交互关系（How it flows）

### 2.1 主要执行路径

#### 路径 1：初始化（节点启动）

```
T=0s: 节点启动
────────────────────────────────────────────────
├─ StoreGrantCoordinators.SetPebbleMetricsProvider()
│   └─ initGrantCoordinator(storeID)
│       └─ ioLoadListener := &ioLoadListener{
│           perWorkTokenEstimator: makeStorePerWorkTokenEstimator(),
│           diskBandwidthLimiter:  newDiskBandwidthLimiter(),
│           statsInitialized: false,  // 关键：未初始化
│       }
│
├─ 启动后台 goroutine:
│   func run() {
│       ticker := tokenAllocationTicker{}
│       for {
│           // ===== 每 15s =====
│           loaded := pebbleMetricsTick(metrics)
│           ticker.adjustmentStart(loaded)
│
│           // ===== 每 1ms 或 250ms =====
│           for ticker.remainingTicks() > 0 {
│               <-ticker.ticker.C
│               allocateTokensTick(remainingTicks)
│           }
│       }
│   }
└─ 返回
```

#### 路径 2：每 15s 的 Token 重新计算

```
T=15s, T=30s, T=45s, ... : pebbleMetricsTick()
────────────────────────────────────────────────
输入：Pebble metrics (从 LSM 读取)

步骤 1: 检查初始化状态
├─ if !io.statsInitialized:
│   ├─ 第一次调用，仅初始化累计值
│   ├─ totalNumByteTokens = unlimitedTokens
│   ├─ statsInitialized = true
│   └─ return false（系统未加载）
│
└─ else: 继续执行

步骤 2: 调用 adjustTokens()
├─ adjustTokensInner(...)
│   ├─ 计算 interval 增量:
│   │   ├─ intL0AddedBytes = cumL0AddedBytes - prev.cumL0AddedBytes
│   │   ├─ intL0CompactedBytes = prev.curL0Bytes + intL0AddedBytes - curL0Bytes
│   │   └─ intWriteStalls = cumWriteStallCount - prev.cumWriteStallCount
│   │
│   ├─ 计算 L0 健康分数:
│   │   score = max(
│   │       L0NumSubLevels / 20,
│   │       L0NumFiles / 4000
│   │   ) * 2
│   │
│   ├─ 计算 Compaction Tokens:
│   │   ├─ 平滑 L0 compacted bytes:
│   │   │   smoothedIntL0CompactedBytes =
│   │   │       0.5 * intL0CompactedBytes +
│   │   │       0.5 * prev.smoothedIntL0CompactedBytes
│   │   │
│   │   └─ 根据 score 分段计算:
│   │       if score < 0.5:  // 健康
│   │           totalNumByteTokens = unlimitedTokens
│   │       elif score < 1.0:  // 低负载
│   │           totalNumByteTokens =
│   │               -score * 2 * smoothedBytes + 3 * smoothedBytes
│   │       elif score < 2.0:  // 中负载
│   │           totalNumByteTokens =
│   │               -score * smoothedBytes/2 + 3 * smoothedBytes/2
│   │       else:  // 过载
│   │           totalNumByteTokens = smoothedBytes / 2
│   │
│   ├─ 计算 Flush Tokens:
│   │   ├─ intFlushTokens =
│   │   │       flushWriteThroughput.PeakRate() * 15s
│   │   │
│   │   ├─ 自适应调整 flushUtilTargetFraction:
│   │   │   if intWriteStalls > 0:
│   │   │       flushUtilTargetFraction -= 0.025 * numSteps
│   │   │   elif highTokenUsage && no stalls:
│   │   │       flushUtilTargetFraction += 0.025
│   │   │
│   │   ├─ 平滑:
│   │   │   smoothedNumFlushTokens =
│   │   │       0.5 * intFlushTokens +
│   │   │       0.5 * prev.smoothedNumFlushTokens
│   │   │
│   │   └─ numFlushTokens =
│   │       flushUtilTargetFraction * smoothedNumFlushTokens
│   │
│   ├─ 取最小值（最保守的限制）:
│   │   if totalNumByteTokens > numFlushTokens:
│   │       totalNumByteTokens = numFlushTokens
│   │       tokenKind = flushTokenKind
│   │   else:
│   │       tokenKind = compactionTokenKind
│   │
│   └─ 计算 Elastic Tokens:
│       if score >= 0.2:
│           totalNumElasticByteTokens =
│               smoothedBytes * (1.25 - 1.25 * (score - 0.2))
│       else:
│           totalNumElasticByteTokens = unlimitedTokens
│
├─ 更新 Linear Models:
│   perWorkTokenEstimator.updateEstimates(...)
│   ├─ 计算 interval 统计:
│   │   ├─ intL0WriteAccountedBytes
│   │   ├─ intIngestedAccountedBytes
│   │   └─ adjustedIntL0WriteBytes
│   │
│   └─ 更新 4 个模型:
│       ├─ l0WriteLM.updateModelUsingIntervalStats()
│       ├─ l0IngestLM.updateModelUsingIntervalStats()
│       ├─ ingestLM.updateModelUsingIntervalStats()
│       └─ writeAmpLM.updateModelUsingIntervalStats()
│
├─ 计算 Disk Bandwidth Tokens:
│   diskBandwidthLimiter.computeElasticTokens(...)
│
└─ 返回 adjustTokensResult

步骤 3: 更新状态
├─ io.adjustTokensResult = res
├─ io.cumFlushWriteThroughput = metrics.Flush.WriteThroughput
└─ return (totalNumByteTokens < unlimitedTokens)  // 是否加载
```

#### 路径 3：每 1ms 的 Token 分配

```
T=15s + 1ms, 15s + 2ms, ..., 30s: allocateTokensTick()
────────────────────────────────────────────────
输入：remainingTicks (剩余 tick 次数)

步骤 1: 计算本次分配量
├─ allocateFunc := func(total, allocated, remainingTicks) {
│       remainingTokens := total - allocated
│       // 向上取整，避免最后 tick 一次性释放
│       toAllocate = (remainingTokens + remainingTicks - 1) / remainingTicks
│       return toAllocate
│   }
│
├─ toAllocateByteTokens = allocateFunc(
│       io.totalNumByteTokens,
│       io.byteTokensAllocated,
│       remainingTicks)
│
├─ toAllocateElasticByteTokens = allocateFunc(
│       io.totalNumElasticByteTokens,
│       io.elasticByteTokensAllocated,
│       remainingTicks)
│
└─ toAllocateDiskWriteTokens = allocateFunc(
        io.diskWriteTokens,
        io.diskWriteTokensAllocated,
        remainingTicks)

步骤 2: 累计已分配量
├─ io.byteTokensAllocated += toAllocateByteTokens
├─ io.elasticByteTokensAllocated += toAllocateElasticByteTokens
└─ io.diskWriteTokensAllocated += toAllocateDiskWriteTokens

步骤 3: 传递给 granter
├─ tokensUsed, tokensUsedByElastic :=
│       io.kvGranter.setAvailableTokens(
│           toAllocateByteTokens,
│           toAllocateElasticByteTokens,
│           toAllocateDiskWriteTokens,
│           toAllocateDiskReadTokens,
│           tokensMaxCapacity,  // 用于 burst 控制
│           elasticTokensMaxCapacity,
│           diskWriteTokenMaxCapacity,
│           remainingTicks == 1,  // lastTick
│       )
│
└─ io.byteTokensUsed += tokensUsed  // 累计使用量
```

### 2.2 状态转换图

```
状态 1: 未初始化
┌────────────────────────────┐
│ statsInitialized = false   │
│ totalNumByteTokens = 0     │
│ cumL0AddedBytes = 0        │
└────────────────────────────┘
         │
         │ pebbleMetricsTick() 第一次调用
         ▼
状态 2: 初始化完成（无限制）
┌────────────────────────────┐
│ statsInitialized = true    │
│ totalNumByteTokens =       │
│     unlimitedTokens        │
│ cumL0AddedBytes = 初始值   │
└────────────────────────────┘
         │
         │ pebbleMetricsTick() 第二次调用
         │ L0 score = 0.3 (健康)
         ▼
状态 3: 健康状态（无限制）
┌────────────────────────────┐
│ totalNumByteTokens =       │
│     unlimitedTokens        │
│ smoothedIntL0Compacted =   │
│     500 MB                 │
└────────────────────────────┘
         │
         │ 写入高峰
         │ L0 score = 0.8 (预警)
         ▼
状态 4: 预警状态（轻微限制）
┌────────────────────────────┐
│ score = 0.8                │
│ totalNumByteTokens =       │
│     -0.8*1000MB + 1500MB   │
│     = 700 MB/15s           │
│ ≈ 46.7 MB/s                │
└────────────────────────────┘
         │
         │ 继续恶化
         │ L0 score = 1.5 (过载)
         ▼
状态 5: 过载状态（严格限制）
┌────────────────────────────┐
│ score = 1.5                │
│ totalNumByteTokens =       │
│     -1.5*250MB + 750MB     │
│     = 375 MB/15s           │
│ ≈ 25 MB/s                  │
└────────────────────────────┘
         │
         │ Compaction 追上来
         │ L0 score = 0.4 (健康)
         ▼
回到状态 3: 健康状态
```

### 2.3 时间线示例

```
完整的 30s 时间线：

T=0s: 节点启动
├─ pebbleMetricsTick() → statsInitialized = true
├─ totalNumByteTokens = unlimitedTokens
└─ 开始 adjustmentInterval #1

T=0.001s: 第 1 次 allocateTokensTick()
├─ remainingTicks = 15000
├─ toAllocate = unlimitedTokens / 15000 ≈ 6.1e14
└─ 分配给 granter

T=0.002s: 第 2 次 allocateTokensTick()
├─ remainingTicks = 14999
└─ 继续分配...

... (14998 次 tick)

T=15s: adjustmentInterval #1 结束
├─ pebbleMetricsTick()
│   ├─ L0 metrics:
│   │   ├─ L0NumSubLevels = 8
│   │   ├─ L0NumFiles = 1600
│   │   └─ L0 score = 0.8 / 2 = 0.4 (健康)
│   │
│   ├─ Interval stats:
│   │   ├─ intL0AddedBytes = 1.2 GB
│   │   ├─ intL0CompactedBytes = 900 MB
│   │   └─ smoothedIntL0CompactedBytes =
│   │       0.5 * 900MB + 0.5 * 0 = 450 MB
│   │
│   ├─ 计算 tokens:
│   │   ├─ score < 0.5 → unlimitedTokens
│   │   └─ totalNumByteTokens = unlimitedTokens
│   │
│   └─ Linear Models 更新:
│       ├─ intL0WriteAccountedBytes = 800 MB
│       ├─ adjustedIntL0WriteBytes = 900 MB
│       └─ l0WriteLM:
│           multiplier = 900 / 800 = 1.125
│           constant = 1
│
└─ 开始 adjustmentInterval #2

T=15.001s: adjustmentInterval #2, tick #1
├─ remainingTicks = 15000
└─ 分配 tokens...

... (继续)

T=30s: adjustmentInterval #2 结束
├─ pebbleMetricsTick()
│   ├─ L0 metrics:
│   │   ├─ L0NumSubLevels = 12  ← 增加了！
│   │   ├─ L0NumFiles = 2400
│   │   └─ L0 score = 1.2 / 2 = 0.6
│   │
│   ├─ Interval stats:
│   │   ├─ intL0AddedBytes = 2.5 GB  ← 写入增加！
│   │   ├─ intL0CompactedBytes = 1.2 GB
│   │   └─ smoothedIntL0CompactedBytes =
│   │       0.5 * 1200MB + 0.5 * 450MB = 825 MB
│   │
│   ├─ 计算 tokens:
│   │   ├─ score = 0.6 ∈ [0.5, 1.0) → 低负载
│   │   ├─ totalNumByteTokens =
│   │   │   -0.6 * 2 * 825MB + 3 * 825MB
│   │   │   = -990MB + 2475MB = 1485 MB / 15s
│   │   └─ ≈ 99 MB/s (开始限流！)
│   │
│   └─ Linear Models 更新:
│       ├─ intL0WriteAccountedBytes = 1.8 GB
│       ├─ adjustedIntL0WriteBytes = 1.5 GB
│       └─ l0WriteLM:
│           multiplier = 0.5 * (1500/1800) +
│                       0.5 * 1.125
│                     = 0.417 + 0.563 = 0.98
│           constant = 平滑更新
│
└─ 开始 adjustmentInterval #3
```

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 核心函数 1：`adjustTokensInner()` —— Token 计算的心脏

**位置**：`pkg/util/admission/io_load_listener.go:837-1311`

**职责**：基于 L0 状态和 flush/compaction 速率，计算下一个 15s 的 token 分配

**输入**：
- `prev ioLoadListenerState`：上一个周期的状态
- `l0Metrics`：Pebble L0 层的 metrics
- `cumWriteStallCount`：累计 write stall 次数
- `cumCompactionStats`：累计 compaction 统计
- `flushWriteThroughput`：Flush 吞吐量 metrics

**输出**：
- `adjustTokensResult`：包含新的 token 分配和状态

**核心逻辑**：

#### 步骤 1：计算 Interval 增量

```go
// pkg/util/admission/io_load_listener.go:868-884

// ===== L0 增长量 =====
cumL0AddedBytes := l0Metrics.TablesFlushed.Bytes +
                   l0Metrics.BlobBytesFlushed +
                   l0Metrics.TablesIngested.Bytes
intL0AddedBytes := int64(cumL0AddedBytes) - int64(prev.cumL0AddedBytes)

// ===== L0 减少量（通过 compaction）=====
// 公式：prev.curL0Bytes + 增加 - 当前 = compact 出去的量
intL0CompactedBytes := prev.curL0Bytes + intL0AddedBytes - curL0Bytes

// ===== Write Stall 增量 =====
intWriteStalls := cumWriteStallCount - prev.cumWriteStallCount
```

**不变量**：
- `intL0AddedBytes >= 0`（累计值单调递增）
- `intL0CompactedBytes >= 0`（防御性检查，忽略负值）

#### 步骤 2：平滑 L0 Compacted Bytes

```go
// pkg/util/admission/io_load_listener.go:890-936

const alpha = 0.5

var smoothedIntL0CompactedBytes int64
if intWALFailover && intL0CompactedBytes < prev.smoothedIntL0CompactedBytes {
    // WAL failover 期间，compaction 可能变慢
    // 复用上次的平滑值，避免错误地降低 token
    smoothedIntL0CompactedBytes = prev.smoothedIntL0CompactedBytes
} else {
    // 正常情况：指数平滑
    smoothedIntL0CompactedBytes = int64(
        alpha * float64(intL0CompactedBytes) +
        (1-alpha) * float64(prev.smoothedIntL0CompactedBytes))
}
```

**为什么需要平滑？**

```
原始数据（每 15s）：
T=0:   intL0CompactedBytes = 500 MB
T=15:  intL0CompactedBytes = 1200 MB  ← 突变！
T=30:  intL0CompactedBytes = 400 MB   ← 又突变！
T=45:  intL0CompactedBytes = 900 MB

问题：
├─ Compaction 调度是不均匀的
├─ 可能连续几个大 compaction，然后暂停
└─ 直接使用会导致 token 剧烈波动

平滑后：
T=0:   smoothed = 500 MB
T=15:  smoothed = 0.5 * 1200 + 0.5 * 500 = 850 MB
T=30:  smoothed = 0.5 * 400 + 0.5 * 850 = 625 MB
T=45:  smoothed = 0.5 * 900 + 0.5 * 625 = 762.5 MB

效果：
└─ 平滑后的值变化更平缓，token 分配更稳定
```

#### 步骤 3：计算 Flush Tokens

```go
// pkg/util/admission/io_load_listener.go:1033-1121

// ===== 100% 利用率的 flush tokens =====
intFlushTokens := float64(flushWriteThroughput.PeakRate()) * adjustmentInterval

// ===== 自适应调整目标利用率 =====
const flushUtilTargetFractionIncrement = 0.025

// 检查 token 使用率
highTokenUsage :=
    float64(prev.byteTokensUsed) >= 0.9 * smoothedNumFlushTokens * flushUtilTargetFraction

if intWriteStalls > 0 {
    // 有 write stall → 降低目标利用率
    numDecreaseSteps := 1
    if intWriteStalls >= 5 {
        numDecreaseSteps = 3
    } else if intWriteStalls >= 2 {
        numDecreaseSteps = 2
    }

    for i := 0; i < numDecreaseSteps; i++ {
        if flushUtilTargetFraction >= minFlushUtilTargetFraction + flushUtilTargetFractionIncrement {
            flushUtilTargetFraction -= flushUtilTargetFractionIncrement
        }
    }
} else if flushUtilTargetFraction < maxFlushUtilTargetFraction - flushUtilTargetFractionIncrement &&
          intWriteStalls == 0 && highTokenUsage {
    // 无 write stall 且 token 使用率高 → 提高目标利用率
    flushUtilTargetFraction += flushUtilTargetFractionIncrement
}

// ===== 平滑 flush tokens =====
if smoothedNumFlushTokens == 0 {
    smoothedNumFlushTokens = intFlushTokens
} else {
    smoothedNumFlushTokens = alpha * intFlushTokens +
                             (1-alpha) * prev.smoothedNumFlushTokens
}

// ===== 最终 flush tokens =====
flushTokensFloat := flushUtilTargetFraction * smoothedNumFlushTokens
numFlushTokens = int64(flushTokensFloat)
```

**Flush 利用率自适应的逻辑**：

```
目标：在 "避免 write stall" 和 "最大化吞吐量" 之间取得平衡

初始状态：
├─ flushUtilTargetFraction = 1.0
├─ smoothedNumFlushTokens = 1000 MB/15s
└─ numFlushTokens = 1.0 * 1000 = 1000 MB/15s

T=15s: 发生 3 次 write stall
├─ intWriteStalls = 3 → numDecreaseSteps = 2
├─ flushUtilTargetFraction -= 0.025 * 2 = 0.95
└─ numFlushTokens = 0.95 * 1000 = 950 MB/15s
    （减少 5% 的 token，降低 flush 压力）

T=30s: 无 write stall，token 使用率 95%
├─ highTokenUsage = true
├─ flushUtilTargetFraction += 0.025 = 0.975
└─ numFlushTokens = 0.975 * 1000 = 975 MB/15s
    （小心增加 token，探测边界）

T=45s: 无 write stall，token 使用率 96%
├─ flushUtilTargetFraction += 0.025 = 1.0
└─ numFlushTokens = 1.0 * 1000 = 1000 MB/15s
    （恢复到峰值）

T=60s: 再次 write stall
└─ 重复降低过程...

结果：
└─ flushUtilTargetFraction 在 [0.5, 1.5] 范围内自适应调整
```

#### 步骤 4：计算 Compaction Tokens（分段函数）

```go
// pkg/util/admission/io_load_listener.go:1129-1205

score, _ := ioThreshold.Score()
score *= 2  // 便于计算

if score < 0.5 {
    // ===== Underload：Score < 0.5 =====
    // 例如：sub-levels < 5
    //
    // 策略：无限制 tokens
    totalNumByteTokens = unlimitedTokens

    // 但仍然维护平滑值，用于后续状态切换
    numTokens := intL0CompactedBytes
    smoothedCompactionByteTokens =
        alpha * float64(numTokens) +
        (1-alpha) * prev.smoothedCompactionByteTokens

} else if score >= 0.5 && score < 1 {
    // ===== Low Load：Score ∈ [0.5, 1.0) =====
    // 例如：sub-levels ∈ [5, 10)
    //
    // 策略：线性插值
    // - score = 0.5 时：2 * smoothedBytes
    // - score = 1.0 时：smoothedBytes
    fTotalNumByteTokens =
        -score * (2 * float64(smoothedIntL0CompactedBytes)) +
        3 * float64(smoothedIntL0CompactedBytes)

    // 应用下界（处理 L0 未被 compact 的情况）
    if fTotalNumByteTokens < float64(l0CompactionTokensLowerBound) {
        fTotalNumByteTokens = float64(l0CompactionTokensLowerBound)
        usedCompactionTokensLowerBound = true
    }

    smoothedCompactionByteTokens =
        alpha * fTotalNumByteTokens +
        (1-alpha) * prev.smoothedCompactionByteTokens

} else if score >= 1 && score < 2 {
    // ===== Medium Load：Score ∈ [1.0, 2.0) =====
    // 例如：sub-levels ∈ [10, 20)
    //
    // 策略：线性插值
    // - score = 1.0 时：smoothedBytes
    // - score = 2.0 时：smoothedBytes / 2
    halfSmoothedBytes := float64(smoothedIntL0CompactedBytes / 2.0)
    fTotalNumByteTokens =
        -score * halfSmoothedBytes +
        3 * halfSmoothedBytes

    smoothedCompactionByteTokens =
        alpha * fTotalNumByteTokens +
        (1-alpha) * prev.smoothedCompactionByteTokens

} else {
    // ===== Overload：Score >= 2.0 =====
    // 例如：sub-levels >= 20
    //
    // 策略：严格限制，仅允许 compaction 速率的一半
    fTotalNumByteTokens = float64(smoothedIntL0CompactedBytes / 2.0)

    smoothedCompactionByteTokens =
        alpha * fTotalNumByteTokens +
        (1-alpha) * prev.smoothedCompactionByteTokens
}

totalNumByteTokens = int64(smoothedCompactionByteTokens)
```

**分段函数的几何意义**：

```
Token 分配 vs L0 Score

Tokens (MB/s)
│
│  Unlimited ──────────────────────────────────
│                                             │
│                                            │
│                                           │
│                                          │
│  2 * smoothed ─┐                        │ Underload
│                │ \                      │
│                │   \                   │
│  smoothed ─────┼─────\───────┐        │
│                │       \      │\      │
│                │         \    │  \   │ Low Load
│                │           \  │    │
│  smoothed/2 ───┼─────────────┴────\─────
│                │                   │  \   Medium Load
│                │                   │    \
│  0 ────────────┴───────────────────┴──────\── Overload
│                │                   │      │ \
└────────────────┴───────────────────┴──────┴───> L0 Score
               0.5                 1.0    2.0

设计理由：
1. Score < 0.5: 系统健康，无需限制
2. Score ∈ [0.5, 1.0): 开始限制，但仍给予缓冲
3. Score ∈ [1.0, 2.0): 中度限制，逐步降低
4. Score >= 2.0: 严重过载，大幅限流
```

#### 步骤 5：计算 Elastic Tokens

```go
// pkg/util/admission/io_load_listener.go:1207-1232

if score >= 0.2 {
    if intWALFailover {
        // WAL failover 期间，几乎停止 elastic work
        totalNumElasticByteTokens = 1
    } else {
        // 线性函数：
        // - score = 0.2: 1.25 * smoothedBytes
        // - score = 0.6: 0.75 * smoothedBytes
        // - score = 1.2: 0 tokens
        totalNumElasticByteTokens = int64(
            float64(smoothedIntL0CompactedBytes) *
            (1.25 - 1.25 * (score - 0.2)))
    }
    totalNumElasticByteTokens = max(totalNumElasticByteTokens, 1)
} else {
    totalNumElasticByteTokens = unlimitedTokens
}

// 进一步限制：如果有大量未 flush 的 memtable
if recentUnflushedMemTableTooLarge {
    if totalNumElasticByteTokens > intL0CompactedBytes {
        totalNumElasticByteTokens = intL0CompactedBytes
    }
}
```

**Elastic Tokens 的设计哲学**：

```
Regular vs Elastic 的隔离策略：

Regular Work:
├─ 高优先级（用户事务）
├─ 仅在 score >= 0.5 时限流
└─ 即使过载，仍给予 smoothedBytes/2 的 tokens

Elastic Work:
├─ 低优先级（后台任务）
├─ 在 score >= 0.2 时就开始限流（更早！）
├─ 在 score >= 1.2 时完全停止
└─ 牺牲 elastic 来保护 regular

示例（smoothedBytes = 1000 MB）:
├─ score = 0.2:
│   ├─ regularTokens = unlimited
│   └─ elasticTokens = 1.25 * 1000 = 1250 MB
│
├─ score = 0.6:
│   ├─ regularTokens = 1800 MB
│   └─ elasticTokens = 0.75 * 1000 = 750 MB
│
├─ score = 1.0:
│   ├─ regularTokens = 1000 MB
│   └─ elasticTokens = 0.25 * 1000 = 250 MB
│
└─ score = 1.2:
    ├─ regularTokens = 900 MB
    └─ elasticTokens = 0 MB（完全停止）
```

#### 步骤 6：取 Compaction 和 Flush Tokens 的最小值

```go
// pkg/util/admission/io_load_listener.go:1252-1267

tokenKind := compactionTokenKind
if totalNumByteTokens > numFlushTokens {
    // Flush 是瓶颈
    totalNumByteTokens = numFlushTokens

    // Elastic 也需要相应减少
    numElasticFlushTokens := int64(0.8 * float64(numFlushTokens))
    if numElasticFlushTokens < totalNumElasticByteTokens {
        totalNumElasticByteTokens = numElasticFlushTokens
    }

    tokenKind = flushTokenKind
}

// Elastic 不能超过 Regular
if totalNumElasticByteTokens > totalNumByteTokens {
    totalNumElasticByteTokens = totalNumByteTokens
}
```

**为什么取最小值？**

```
原因：两种资源都是瓶颈

场景 1：Compaction 是瓶颈
├─ compactionTokens = 500 MB/15s
├─ flushTokens = 1000 MB/15s
├─ 最终：totalNumByteTokens = 500 MB/15s
└─ 限制因素：L0 compaction 跟不上

场景 2：Flush 是瓶颈
├─ compactionTokens = 1000 MB/15s
├─ flushTokens = 500 MB/15s
├─ 最终：totalNumByteTokens = 500 MB/15s
└─ 限制因素：MemTable flush 跟不上

如果不取最小值（错误）：
├─ 假设只看 compaction: totalTokens = 1000 MB/15s
├─ 但 flush 只能处理 500 MB/15s
├─ 导致 MemTable 堆积 → write stall ❌
└─ 正确做法：取 min，双重保护 ✓
```

### 3.2 核心函数 2：`updateModelUsingIntervalStats()` —— Linear Model 的更新

**位置**：`pkg/util/admission/tokens_linear_model.go:85-157`

**职责**：基于 interval 统计，更新 linear model（y = multiplier * x + constant）

**输入**：
- `accountedBytes`：本周期内，请求声称的字节数
- `actualBytes`：本周期内，实际观察到的 LSM 字节数
- `workCount`：本周期内的请求数量

**输出**：
- 更新 `smoothedLinearModel`

**核心算法**：

```go
// pkg/util/admission/tokens_linear_model.go:85-157

func (f *tokensLinearModelFitter) updateModelUsingIntervalStats(
    accountedBytes int64, actualBytes int64, workCount int64,
) {
    // ===== 步骤 0：边界检查 =====
    if workCount <= 1 || (actualBytes <= 0 && ...) {
        // 数据不足，无法拟合
        // 但为避免 constant 过大持续惩罚，将其减半
        f.smoothedLinearModel.constant = max(1, f.smoothedLinearModel.constant / 2)
        return
    }

    // ===== 步骤 1：防御性处理 =====
    if actualBytes < 0 {
        actualBytes = 0
    }
    if accountedBytes <= 0 {
        // 异常情况：有实际字节但无声称字节
        // 假设未来会有正常的 accountedBytes
        accountedBytes = workCount * max(1, f.smoothedPerWorkAccountedBytes)
    }

    // ===== 步骤 2：拟合 constant（从最小值 1 开始）=====
    constant := int64(1)

    // ===== 步骤 3：拟合 multiplier =====
    // 目标：minimizer.x + constant.count = actual
    // 求解：multiplier = (actual - constant.count) / x
    multiplier := float64(max(0, actualBytes - workCount * constant)) /
                  float64(accountedBytes)

    // ===== 步骤 4：将 multiplier 限制在 [min, max] 范围内 =====
    if multiplier > f.multiplierMax {
        multiplier = f.multiplierMax
    } else if multiplier < f.multiplierMin {
        multiplier = f.multiplierMin
    }

    // ===== 步骤 5：检查模型是否能完全解释 actualBytes =====
    modelBytes := int64(multiplier * float64(accountedBytes)) +
                  (constant * workCount)

    if modelBytes < actualBytes {
        // 模型低估了，需要增加 constant 来弥补
        constantAdjust := (actualBytes - modelBytes) / workCount
        if constantAdjust + constant > 0 {
            constant += constantAdjust
        }
    }

    // ===== 步骤 6：记录本周期的精确模型 =====
    f.intLinearModel = tokensLinearModel{
        multiplier: multiplier,
        constant:   constant,
    }

    // ===== 步骤 7：指数平滑 =====
    const alpha = 0.5
    f.smoothedLinearModel.multiplier =
        alpha * multiplier +
        (1-alpha) * f.smoothedLinearModel.multiplier

    f.smoothedLinearModel.constant = int64(
        alpha * float64(constant) +
        (1-alpha) * float64(f.smoothedLinearModel.constant))
}
```

**拟合算法的数学推导**：

```
目标：拟合 y = a*x + b*n，其中
├─ y = actualBytes（实际观察到的 LSM 字节数）
├─ x = accountedBytes（请求声称的字节数）
├─ n = workCount（请求数量）
├─ a = multiplier（要求解）
└─ b = constant（要求解）

约束：
├─ a ∈ [multiplierMin, multiplierMax]
├─ b >= 1
└─ 优先使用 multiplier，minimizing constant

步骤 1：先设 b = 1（最小值）

步骤 2：求解 a
├─ a*x + 1*n = y
├─ a*x = y - n
└─ a = (y - n) / x

步骤 3：如果 a 超出范围，clip 到边界
├─ if a > max: a = max
└─ if a < min: a = min

步骤 4：检查是否能完全解释 y
├─ modelBytes = a*x + b*n
└─ if modelBytes < y:
    ├─ 模型低估，需要增加 b
    └─ b += (y - modelBytes) / n

示例：
假设
├─ accountedBytes = 1000 MB
├─ actualBytes = 1800 MB
├─ workCount = 100
├─ [min, max] = [0.5, 3.0]

步骤 1: b = 1

步骤 2: a = (1800 - 100*1) / 1000 = 1.7

步骤 3: 1.7 ∈ [0.5, 3.0] ✓

步骤 4: modelBytes = 1.7*1000 + 1*100 = 1800
        1800 == 1800 ✓ 无需调整

最终模型：y = 1.7*x + 1
```

**平滑的效果**：

```
假设历史模型：
├─ smoothedMultiplier = 1.5
└─ smoothedConstant = 100

本周期拟合的模型：
├─ intMultiplier = 1.7
└─ intConstant = 1

平滑后（α = 0.5）：
├─ smoothedMultiplier = 0.5 * 1.7 + 0.5 * 1.5 = 1.6
└─ smoothedConstant = 0.5 * 1 + 0.5 * 100 = 50.5 ≈ 50

效果：
├─ 模型逐步调整，而非剧烈变化
├─ 避免单次异常数据的过度影响
└─ 保持模型稳定性
```

### 3.3 核心函数 3：`allocateTokensTick()` —— 平滑分配

**位置**：`pkg/util/admission/io_load_listener.go:597-696`

**职责**：将 15s 的 token 总量分摊到每个 tick（1ms 或 250ms）

**关键设计**：

```go
// pkg/util/admission/io_load_listener.go:598-623

allocateFunc := func(total int64, allocated int64, remainingTicks int64) (toAllocate int64) {
    remainingTokens := total - allocated

    if remainingTokens >= unlimitedTokens - (remainingTicks - 1) {
        // 处理 unlimitedTokens 的溢出
        toAllocate = remainingTokens / remainingTicks
    } else {
        // ===== 向上取整 =====
        // 目的：避免最后一个 tick 突然释放大量 tokens
        toAllocate = (remainingTokens + remainingTicks - 1) / remainingTicks

        // 防御性检查
        if toAllocate + allocated > total {
            toAllocate = total - allocated
        }
    }
    return toAllocate
}
```

**为什么要向上取整？**

```
场景：totalTokens = 150,001, ticks = 15,000 (1ms 间隔)

方案 A：向下取整（错误）
├─ tokensPerTick = 150,001 / 15,000 = 10
├─ 前 15,000 ticks: 10 * 15,000 = 150,000
├─ 最后一个 tick: 150,001 - 150,000 = 1
└─ 问题：分配不均 ❌

方案 B：向上取整（正确）
├─ tokensPerTick = (150,001 + 14,999) / 15,000 = 11
├─ 前 13,636 ticks: 11 * 13,636 = 150,001
├─ 后续 ticks: 0
└─ 效果：更均匀 ✓

数学原理：
ceil(a/b) = (a + b - 1) / b  （整数除法）

实际影响：
├─ totalTokens = 150,000,000 (150 MB)
├─ ticks = 15,000
├─ tokensPerTick = 10,000
└─ 分配完成时间：150,000,000 / 10,000 = 15,000 ticks
    ≈ 15s（完美分配）
```

---

## 四、动态行为分析（Runtime 行为）

### 4.1 LSM 状态如何影响 Token 分配

**信号源**：Pebble Metrics（每 15s 采样一次）

```go
// 关键 Metrics：

1. L0NumSubLevels：L0 层的 sub-level 数量
2. L0NumFiles：L0 层的文件数量
3. L0Bytes：L0 层的总字节数
4. FlushThroughput：MemTable flush 的吞吐量
5. CompactionStats：各层的 compaction 统计
```

**Score 计算**：

```go
// pkg/util/admission/admissionpb/io_threshold.go (简化)

func (iot *IOThreshold) Score() float64 {
    subLevelScore := float64(iot.L0NumSubLevels) /
                     float64(iot.L0NumSubLevelsThreshold)
    fileCountScore := float64(iot.L0NumFiles) /
                      float64(iot.L0NumFilesThreshold)

    score := max(subLevelScore, fileCountScore)

    // 修正：小 L0 的 sub-level 可能被高估
    if iot.L0MinimumSizePerSubLevel > 0 {
        adjustedSubLevels := iot.L0Size / iot.L0MinimumSizePerSubLevel
        adjustedScore := float64(adjustedSubLevels) /
                         float64(iot.L0NumSubLevelsThreshold)
        score = max(score, adjustedScore)
    }

    return score
}
```

**示例：不同 LSM 状态下的 Token 分配**

```
场景 1：健康状态
────────────────────────────────────────────────
L0 Metrics:
├─ Sub-levels: 4
├─ Files: 800
├─ L0 Bytes: 500 MB
└─ Score: max(4/20, 800/4000) = 0.2

Compaction Stats:
├─ intL0CompactedBytes: 600 MB
└─ smoothedIntL0CompactedBytes: 500 MB

计算过程:
├─ score * 2 = 0.4 < 0.5 → Underload
├─ totalNumByteTokens = unlimitedTokens
├─ totalNumElasticByteTokens = unlimitedTokens
└─ tokenKind: 不限流

结果：
├─ Regular: 无限制
└─ Elastic: 无限制

场景 2：预警状态（Low Load）
────────────────────────────────────────────────
L0 Metrics:
├─ Sub-levels: 8
├─ Files: 1600
├─ L0 Bytes: 1.2 GB
└─ Score: max(8/20, 1600/4000) = 0.4

Compaction Stats:
├─ intL0CompactedBytes: 900 MB
└─ smoothedIntL0CompactedBytes: 800 MB

计算过程:
├─ score * 2 = 0.8 ∈ [0.5, 1.0) → Low Load
├─ totalNumByteTokens =
│   -0.8 * 2 * 800MB + 3 * 800MB
│   = -1280MB + 2400MB = 1120 MB / 15s
│   ≈ 74.7 MB/s
│
├─ totalNumElasticByteTokens =
│   800MB * (1.25 - 1.25 * (0.8 - 0.2))
│   = 800MB * (1.25 - 0.75) = 400 MB / 15s
│   ≈ 26.7 MB/s
│
└─ tokenKind: compactionTokenKind

结果：
├─ Regular: 74.7 MB/s（轻微限流）
└─ Elastic: 26.7 MB/s（较严格限流）

场景 3：中负载状态（Medium Load）
────────────────────────────────────────────────
L0 Metrics:
├─ Sub-levels: 14
├─ Files: 2800
├─ L0 Bytes: 2.5 GB
└─ Score: max(14/20, 2800/4000) = 0.7

Compaction Stats:
├─ intL0CompactedBytes: 1000 MB
└─ smoothedIntL0CompactedBytes: 900 MB

计算过程:
├─ score * 2 = 1.4 ∈ [1.0, 2.0) → Medium Load
├─ halfSmoothedBytes = 900MB / 2 = 450MB
├─ totalNumByteTokens =
│   -1.4 * 450MB + 3 * 450MB
│   = -630MB + 1350MB = 720 MB / 15s
│   ≈ 48 MB/s
│
├─ totalNumElasticByteTokens =
│   900MB * (1.25 - 1.25 * (1.4 - 0.2))
│   = 900MB * (1.25 - 1.5) = -225 MB
│   → max(-225, 1) = 1 MB / 15s（几乎停止）
│
└─ tokenKind: compactionTokenKind

结果：
├─ Regular: 48 MB/s（中度限流）
└─ Elastic: ~0 MB/s（几乎停止）

场景 4：过载状态（Overload）
────────────────────────────────────────────────
L0 Metrics:
├─ Sub-levels: 22
├─ Files: 4400
├─ L0 Bytes: 4.0 GB
└─ Score: max(22/20, 4400/4000) = 1.1

Compaction Stats:
├─ intL0CompactedBytes: 1100 MB
└─ smoothedIntL0CompactedBytes: 1000 MB

计算过程:
├─ score * 2 = 2.2 >= 2.0 → Overload
├─ totalNumByteTokens =
│   smoothedIntL0CompactedBytes / 2
│   = 1000MB / 2 = 500 MB / 15s
│   ≈ 33.3 MB/s
│
├─ totalNumElasticByteTokens = 1 MB / 15s
│
└─ tokenKind: compactionTokenKind

结果：
├─ Regular: 33.3 MB/s（严格限流）
└─ Elastic: ~0 MB/s（完全停止）
```

### 4.2 Flush 瓶颈的动态检测

**Flush 吞吐量的测量**：

```
Pebble 提供的 Flush Metrics:
├─ WorkDuration: flush 工作时间
├─ IdleDuration: flush 空闲时间
├─ Bytes: flush 的字节数
└─ PeakRate(): 峰值速率（bytes / work_duration）

示例：
├─ WorkDuration = 12s（15s 周期内）
├─ Bytes = 1.2 GB
└─ PeakRate = 1.2GB / 12s = 100 MB/s
```

**自适应 Flush 利用率**：

```
初始状态：
├─ flushUtilTargetFraction = 1.0
├─ smoothedNumFlushTokens = PeakRate * 15s = 1.5 GB
└─ numFlushTokens = 1.0 * 1.5GB = 1.5 GB / 15s

T=15s: 发生 2 次 write stall
────────────────────────────────────────────────
intWriteStalls = 2
├─ numDecreaseSteps = 2
├─ flushUtilTargetFraction -= 0.025 * 2 = 0.95
└─ numFlushTokens = 0.95 * 1.5GB = 1.425 GB / 15s
    （降低 5%）

原因分析：
├─ Write stall 表明 flush 速度不够快
├─ 尽管 PeakRate 是 100MB/s，但实际可持续的速率更低
└─ 降低 targetFraction，减少准入

T=30s: 无 write stall，token 使用率 92%
────────────────────────────────────────────────
highTokenUsage = (prev.byteTokensUsed >= 0.9 * smoothedTokens * fraction)
              = (1.31GB >= 0.9 * 1.5GB * 0.95)
              = (1.31GB >= 1.28GB) = true

├─ flushUtilTargetFraction += 0.025 = 0.975
└─ numFlushTokens = 0.975 * 1.5GB = 1.4625 GB / 15s
    （小心增加 2.5%）

T=45s: 无 write stall，token 使用率 96%
────────────────────────────────────────────────
├─ flushUtilTargetFraction += 0.025 = 1.0
└─ numFlushTokens = 1.0 * 1.5GB = 1.5 GB / 15s
    （恢复到峰值）

T=60s: 发生 6 次 write stall（严重！）
────────────────────────────────────────────────
intWriteStalls = 6
├─ numDecreaseSteps = 3
├─ flushUtilTargetFraction -= 0.025 * 3 = 0.925
└─ numFlushTokens = 0.925 * 1.5GB = 1.3875 GB / 15s
    （大幅降低 7.5%）

长期行为：
├─ flushUtilTargetFraction 在 [0.5, 1.5] 之间振荡
├─ 收敛到一个"安全"的值（例如 0.85）
└─ 在该值下，write stall 很少发生，但吞吐量接近峰值
```

### 4.3 Linear Model 的收敛过程

**场景：系统从零开始运行**

```
T=0s: 初始化
────────────────────────────────────────────────
l0WriteLM:
├─ multiplier = (min + max) / 2 = (0.5 + 3.0) / 2 = 1.75
├─ constant = 1
└─ smoothedPerWorkAccountedBytes = 1

T=15s: 第一个 adjustment interval
────────────────────────────────────────────────
Interval stats:
├─ intL0WriteAccountedBytes = 800 MB
├─ adjustedIntL0WriteBytes = 1000 MB
└─ workCount = 10000

拟合过程:
├─ constant = 1
├─ multiplier = (1000MB - 10000*1) / 800MB
│            = (1000MB - 0.01MB) / 800MB
│            ≈ 1.25
│
├─ 1.25 ∈ [0.5, 3.0] ✓
│
├─ modelBytes = 1.25 * 800MB + 1 * 10000
│             = 1000MB + 0.01MB ≈ 1000MB ✓
│
└─ intLinearModel = {multiplier: 1.25, constant: 1}

平滑:
├─ smoothedMultiplier = 0.5 * 1.25 + 0.5 * 1.75 = 1.5
└─ smoothedConstant = 0.5 * 1 + 0.5 * 1 = 1

T=30s: 第二个 interval
────────────────────────────────────────────────
Interval stats:
├─ intL0WriteAccountedBytes = 900 MB
├─ adjustedIntL0WriteBytes = 1620 MB  ← 写放大增加！
└─ workCount = 9000

拟合过程:
├─ constant = 1
├─ multiplier = (1620MB - 9000*1) / 900MB
│            = (1620MB - 0.009MB) / 900MB
│            ≈ 1.8
│
├─ 1.8 ∈ [0.5, 3.0] ✓
│
└─ intLinearModel = {multiplier: 1.8, constant: 1}

平滑:
├─ smoothedMultiplier = 0.5 * 1.8 + 0.5 * 1.5 = 1.65
└─ smoothedConstant = 1

T=45s: 第三个 interval
────────────────────────────────────────────────
Interval stats:
├─ intL0WriteAccountedBytes = 950 MB
├─ adjustedIntL0WriteBytes = 1710 MB
└─ workCount = 9500

拟合过程:
├─ multiplier ≈ 1.8
└─ intLinearModel = {multiplier: 1.8, constant: 1}

平滑:
├─ smoothedMultiplier = 0.5 * 1.8 + 0.5 * 1.65 = 1.725
└─ smoothedConstant = 1

收敛结果：
├─ multiplier 收敛到 1.8 附近
├─ 这反映了实际的写放大（2x Raft log + state machine）
└─ 未来的 token 估算将使用这个模型
```

**Model 应用示例**：

```
请求到达时：
├─ 用户声称写入 100 MB
├─ 使用 smoothedLinearModel 估算实际 L0 字节数:
│   estimatedL0Bytes = 1.725 * 100MB + 1
│                   ≈ 172.5 MB
│
└─ 从 granter 获取 172.5 MB tokens

请求完成时：
├─ 实际 L0 字节数（从 Pebble）: 180 MB
├─ delta = 180MB - 172.5MB = 7.5 MB
├─ 补扣 7.5 MB tokens (via tookWithoutPermission)
└─ 下次 updateEstimates 时，模型会微调
```

---

## 五、具体示例（必须有）

### 5.1 示例 1：写入高峰导致 L0 过载的完整响应

**初始状态**：

```
L0 状态：
├─ Sub-levels: 5
├─ Files: 1000
├─ L0 Bytes: 800 MB
└─ Score: 0.25（健康）

Token 状态：
├─ totalNumByteTokens = unlimitedTokens
├─ smoothedIntL0CompactedBytes = 500 MB
└─ flushUtilTargetFraction = 1.0
```

**时间线**：

```
T=0s: 突发写入高峰（例如：bulk import 开始）
────────────────────────────────────────────────
写入速率：从 50 MB/s 飙升到 200 MB/s

L0 变化：
├─ intL0AddedBytes = 3000 MB（15s 内）
├─ intL0CompactedBytes = 600 MB
└─ L0 净增长 = 3000 - 600 = 2400 MB

新状态：
├─ Sub-levels: 5 → 9
├─ Files: 1000 → 1800
├─ L0 Bytes: 800MB → 3200MB
└─ Score: max(9/20, 1800/4000) = 0.45

T=15s: pebbleMetricsTick() 第一次检测
────────────────────────────────────────────────
计算过程:
├─ score * 2 = 0.9 ∈ [0.5, 1.0) → Low Load
│
├─ smoothedIntL0CompactedBytes =
│   0.5 * 600MB + 0.5 * 500MB = 550 MB
│
├─ totalNumByteTokens =
│   -0.9 * 2 * 550MB + 3 * 550MB
│   = -990MB + 1650MB = 660 MB / 15s
│   ≈ 44 MB/s
│
└─ totalNumElasticByteTokens =
    550MB * (1.25 - 1.25 * (0.9 - 0.2))
    = 550MB * 0.375 = 206 MB / 15s
    ≈ 13.7 MB/s

Token 分配：
├─ 每 1ms 分配: 44 MB/s / 1000 = 44 KB
├─ Regular: 开始限流
└─ Elastic: 严格限流

实际效果：
├─ Regular 写入: 200 MB/s → 44 MB/s（降低 78%）
└─ Elastic 写入: 100 MB/s → 13.7 MB/s（降低 86%）

T=30s: 观察效果
────────────────────────────────────────────────
L0 变化：
├─ intL0AddedBytes = 660 MB（受 token 限制）
├─ intL0CompactedBytes = 700 MB（compaction 追上来）
└─ L0 净减少 = 700 - 660 = 40 MB

新状态：
├─ Sub-levels: 9 → 8
├─ Files: 1800 → 1750
├─ L0 Bytes: 3200MB → 3160MB
└─ Score: max(8/20, 1750/4000) = 0.4375

pebbleMetricsTick():
├─ score * 2 = 0.875 ∈ [0.5, 1.0) → Low Load
│
├─ smoothedIntL0CompactedBytes =
│   0.5 * 700MB + 0.5 * 550MB = 625 MB
│
├─ totalNumByteTokens =
│   -0.875 * 2 * 625MB + 3 * 625MB
│   = -1093.75MB + 1875MB = 781.25 MB / 15s
│   ≈ 52 MB/s
│
└─ Regular: 44 MB/s → 52 MB/s（微增 18%）

结论：系统开始恢复

T=45s: 继续恢复
────────────────────────────────────────────────
L0 变化：
├─ intL0AddedBytes = 780 MB
├─ intL0CompactedBytes = 750 MB
└─ L0 净减少 = 750 - 780 = -30 MB

新状态：
├─ Sub-levels: 8 → 7
├─ Files: 1750 → 1650
├─ L0 Bytes: 3160MB → 3130MB
└─ Score: max(7/20, 1650/4000) = 0.4125

pebbleMetricsTick():
├─ score * 2 = 0.825 < 0.9 → 趋势良好
├─ smoothedIntL0CompactedBytes = 687.5 MB
├─ totalNumByteTokens ≈ 867 MB / 15s ≈ 57.8 MB/s
└─ Regular: 继续恢复

T=60s: 恢复到安全水平
────────────────────────────────────────────────
新状态：
├─ Sub-levels: 7 → 5
├─ Files: 1650 → 1200
├─ L0 Bytes: 3130MB → 1500MB
└─ Score: max(5/20, 1200/4000) = 0.3

pebbleMetricsTick():
├─ score * 2 = 0.6 ∈ [0.5, 1.0) → Low Load
├─ smoothedIntL0CompactedBytes ≈ 750 MB
├─ totalNumByteTokens ≈ 1050 MB / 15s ≈ 70 MB/s
└─ Regular: 逐步放开限制

T=90s: 完全恢复
────────────────────────────────────────────────
新状态：
├─ Sub-levels: 5 → 4
├─ Files: 1200 → 900
├─ L0 Bytes: 1500MB → 700MB
└─ Score: max(4/20, 900/4000) = 0.225

pebbleMetricsTick():
├─ score * 2 = 0.45 < 0.5 → Underload
├─ totalNumByteTokens = unlimitedTokens
└─ 恢复正常吞吐量
```

**关键观察**：

1. **15s 响应延迟**：从过载发生到限流生效，有 15s 延迟
2. **渐进式恢复**：token 逐步增加，而非一次性恢复
3. **compaction 追上来**：限流给了 compaction 时间追上写入速率

### 5.2 示例 2：Flush 瓶颈的检测与应对

**初始状态**：

```
Flush Metrics:
├─ PeakRate: 100 MB/s
├─ smoothedNumFlushTokens: 1500 MB/15s
├─ flushUtilTargetFraction: 1.0
└─ numFlushTokens: 1500 MB/15s

Compaction Metrics:
├─ smoothedIntL0CompactedBytes: 2000 MB
└─ totalNumByteTokens: 2000 MB/15s (compaction-based)

最终 tokens:
└─ min(2000, 1500) = 1500 MB/15s (flush-limited)
```

**时间线**：

```
T=0s: 正常运行
────────────────────────────────────────────────
tokenKind: flushTokenKind
intWriteStalls: 0

T=15s: 第一次 write stall
────────────────────────────────────────────────
intWriteStalls: 1

pebbleMetricsTick():
├─ numDecreaseSteps = 1
├─ flushUtilTargetFraction: 1.0 → 0.975
├─ numFlushTokens: 1500 MB → 1462.5 MB
└─ Regular: 100 MB/s → 97.5 MB/s

原因分析：
├─ 虽然 PeakRate 是 100MB/s
├─ 但实际可持续速率更低（可能是 97-98MB/s）
└─ 降低 targetFraction 以避免 stall

T=30s: 无 write stall，token 使用率 92%
────────────────────────────────────────────────
intWriteStalls: 0
highTokenUsage: true（byteTokensUsed >= 0.9 * tokens）

pebbleMetricsTick():
├─ flushUtilTargetFraction: 0.975 → 1.0
├─ numFlushTokens: 1462.5 MB → 1500 MB
└─ 尝试增加

T=45s: 再次 write stall（2 次）
────────────────────────────────────────────────
intWriteStalls: 2

pebbleMetricsTick():
├─ numDecreaseSteps = 2
├─ flushUtilTargetFraction: 1.0 → 0.95
├─ numFlushTokens: 1500 MB → 1425 MB
└─ Regular: 100 MB/s → 95 MB/s

T=60s: 无 write stall
T=75s: 无 write stall，高使用率
├─ flushUtilTargetFraction: 0.95 → 0.975
└─ 小心增加

... (继续振荡)

T=300s: 收敛到稳定值
────────────────────────────────────────────────
flushUtilTargetFraction: 稳定在 0.96
numFlushTokens: 1440 MB/15s ≈ 96 MB/s

结论：
├─ 系统学习到实际可持续的 flush 速率是 96 MB/s
├─ 在该速率下，write stall 很少发生
└─ 达到了"安全吞吐量"的平衡点
```

### 5.3 示例 3：Linear Model 的自适应调整

**场景**：工作负载从纯写入切换到写入+摄取（ingestion）

```
阶段 1：纯写入（T=0s - T=60s）
────────────────────────────────────────────────
Interval stats:
├─ intL0WriteAccountedBytes = 1000 MB
├─ intIngestedAccountedBytes = 0 MB
├─ adjustedIntL0WriteBytes = 2000 MB（2x 写放大）
└─ adjustedIntL0IngestedBytes = 0 MB

Linear Model（l0WriteLM）:
├─ multiplier = 2000 / 1000 = 2.0
├─ constant = 1
└─ 解释：每声称 1 MB 写入，实际产生 2 MB L0 字节

平滑后:
├─ smoothedMultiplier ≈ 2.0
└─ smoothedConstant ≈ 1

应用：
├─ 请求声称写入 100 MB
├─ 估算 L0 字节: 2.0 * 100 + 1 = 201 MB
└─ 获取 201 MB tokens

阶段 2：开始摄取（T=60s - T=120s）
────────────────────────────────────────────────
工作负载变化：
├─ 添加 bulk import（摄取 sstables 到 L0）
├─ 写入: 500 MB/15s
└─ 摄取: 500 MB/15s

Interval stats（T=75s）:
├─ intL0WriteAccountedBytes = 500 MB
├─ intIngestedAccountedBytes = 500 MB
├─ adjustedIntL0WriteBytes = 1000 MB（写入部分）
└─ adjustedIntL0IngestedBytes = 300 MB（摄取到 L0 的部分，60%）

Linear Model（l0WriteLM）:
├─ multiplier = 1000 / 500 = 2.0
├─ constant = 1
└─ 写入模型保持不变 ✓

Linear Model（l0IngestLM）:
├─ multiplier = 300 / 500 = 0.6
├─ constant = 1
└─ 新模型：60% 的摄取字节进入 L0

平滑后:
├─ l0WriteLM: {multiplier: 2.0, constant: 1}
├─ l0IngestLM:
│   ├─ smoothedMultiplier = 0.5 * 0.6 + 0.5 * 0.75 = 0.675
│   └─ smoothedConstant = 1
└─ （假设初始值是 0.75）

应用（T=90s）:
请求 1：写入 100 MB
├─ 使用 l0WriteLM: 2.0 * 100 + 1 = 201 MB
└─ 获取 201 MB tokens

请求 2：摄取 100 MB
├─ 使用 l0IngestLM: 0.675 * 100 + 1 = 68.5 MB
└─ 获取 68.5 MB tokens（摄取消耗更少 token）

阶段 3：摄取比例增加（T=120s - T=180s）
────────────────────────────────────────────────
工作负载变化：
├─ 写入: 200 MB/15s
└─ 摄取: 800 MB/15s

Interval stats（T=135s）:
├─ intL0WriteAccountedBytes = 200 MB
├─ intIngestedAccountedBytes = 800 MB
├─ adjustedIntL0WriteBytes = 400 MB
└─ adjustedIntL0IngestedBytes = 480 MB（摄取到 L0 的部分，60%）

Linear Model（l0IngestLM）:
├─ multiplier = 480 / 800 = 0.6
├─ constant = 1
└─ 模型稳定在 0.6

平滑后:
├─ smoothedMultiplier = 0.5 * 0.6 + 0.5 * 0.675 = 0.6375
└─ 逐步收敛到 0.6

应用：
摄取 100 MB
├─ 估算: 0.6375 * 100 + 1 = 64.75 MB
└─ 更准确的 token 估算

完成时（实际 L0 字节：62 MB）:
├─ delta = 62 - 64.75 = -2.75 MB
├─ 归还 2.75 MB tokens
└─ 模型略微过估，下次会调整
```

---

## 六、设计取舍与权衡（Trade-offs）

### 6.1 15 秒 Adjustment Interval vs 其他周期

**当前设计：15 秒**

```
优点：
├─ 足够长：大多数 L0 compaction 能在 15s 内完成
├─ Metrics 稳定：15s 的聚合数据更可靠
├─ CPU 开销低：每 15s 一次计算，影响可忽略
└─ 与 Pebble 的 compaction 周期匹配

缺点：
├─ 响应延迟：从过载到限流，最多 15s 延迟
├─ 可能错过短期抖动
└─ 无法应对极端突发
```

**替代方案 1：5 秒 interval（更快响应）**

```
优点：
├─ 更快检测过载
└─ 更快调整 token

缺点（致命）：
├─ Compaction 可能未完成，metrics 不准确
├─ 示例：
│   T=0: 启动 1GB compaction（预计 10s）
│   T=5: 检查 metrics
│       ├─ intL0CompactedBytes = 0（未完成）
│       ├─ 误判为"compaction 慢"
│       └─ 错误地大幅降低 token ❌
│   T=10: Compaction 完成
│       ├─ intL0CompactedBytes = 1GB
│       └─ 但已经错误限流 5 秒
│
├─ 平滑值不稳定（α=0.5，5s 周期波动大）
└─ CPU 开销增加 3x
```

**替代方案 2：60 秒 interval（更稳定）**

```
优点：
├─ 更稳定的 metrics
└─ CPU 开销更低

缺点（致命）：
├─ 响应太慢
├─ 示例：
│   T=0: 正常（L0 score = 0.3）
│   T=10: 突发写入高峰
│       ├─ L0 sub-levels: 5 → 30
│       ├─ 但要等 50s 才能限流
│       └─ L0 可能已经到 50+ sub-levels ❌
│   T=60: 第一次调整
│       ├─ 可能已经触发 write stall
│       └─ 损害用户体验
│
└─ 浪费资源：过载结束后，仍维持低 token 很久
```

**结论**：15 秒是经验最优值，平衡了：
- Compaction 完成时间（10s 左右）
- 响应速度
- Metrics 稳定性

### 6.2 Token-based vs Slot-based（为什么 IO 必须用 Token）

**当前设计：Token-based**

```
IO 资源的特殊性：

特性 1：延迟性
├─ Slot 在 Admit() 返回时就归还
├─ 但 IO 工作才刚开始（flush, compaction）
└─ Slot 无法表达这种延迟

特性 2：放大效应
├─ 用户写入 1 MB
├─ 实际磁盘写入可能是 10 MB（写放大）
└─ Slot 无法表达这种放大

特性 3：大小差异大
├─ 小写入：1 KB
├─ 大写入：100 MB
├─ 如果都占 1 slot，不公平
└─ Token 按字节计费，公平
```

**如果使用 Slot（错误设计）**：

```
问题 1：无法精确控制吞吐量
├─ Slot limit = 100
├─ 场景 A：100 个 1KB 写入
│   ├─ 吞吐量 = 100 KB
│   └─ L0 增长 ≈ 200 KB
├─ 场景 B：100 个 10MB 写入
│   ├─ 吞吐量 = 1000 MB
│   └─ L0 增长 ≈ 2000 MB（10000x 于场景 A！）
└─ 相同 slot 限制，完全不同的 IO 压力 ❌

问题 2：无法表达写放大
├─ Request A: 写入 1 MB, 写放大 2x → 实际 IO = 2 MB
├─ Request B: 摄取 1 MB 到 L6 → 实际 IO = 1 MB
├─ 如果都占 1 slot，不公平（A 的 IO 是 B 的 2 倍）
└─ Token 可以精确扣除（A: 2 tokens, B: 1 token）✓

问题 3：无法动态调整
├─ L0 健康：应该给更多吞吐量
├─ L0 过载：应该降低吞吐量
├─ 但 slot 限制是固定的请求数
└─ 无法直接映射到字节吞吐量 ❌
```

**Token 的优势**：

```
优势 1：精确表达 IO 开销
├─ 1 token = 1 byte L0 写入
├─ 100 MB tokens → 100 MB L0 增长
└─ 直接对应 LSM 状态

优势 2：公平性
├─ 小请求：少 tokens
├─ 大请求：多 tokens
└─ 按实际 IO 消耗收费

优势 3：可调整吞吐量
├─ 根据 L0 状态计算 tokens/sec
├─ 直接控制 L0 增长速率
└─ 与 compaction 速率匹配
```

### 6.3 Linear Model vs 固定估算

**当前设计：Linear Model（动态学习）**

```
优点：
├─ 自适应：学习工作负载特征
├─ 准确：考虑写放大、压缩率等因素
├─ 工作负载无关：同一模型适用于不同 workload
└─ 持续优化：随时间改进

缺点：
├─ 复杂：需要 fitter 和平滑逻辑
├─ 冷启动：初期估算不准
└─ 计算开销：每 15s 更新模型
```

**替代方案 1：固定倍数（例如 2x）**

```
estimatedL0Bytes = accountedBytes * 2

优点：
├─ 简单
└─ 无计算开销

缺点（致命）：
├─ 不适应工作负载变化
├─ 示例 A：纯写入
│   ├─ accountedBytes = 100 MB
│   ├─ actualL0Bytes = 200 MB（2x Raft + state machine）
│   └─ estimatedBytes = 200 MB ✓
│
├─ 示例 B：纯摄取到 L6
│   ├─ accountedBytes = 100 MB
│   ├─ actualL0Bytes = 0 MB（不进 L0）
│   └─ estimatedBytes = 200 MB ❌（200x 过估！）
│
└─ 示例 C：写入 + 高压缩率数据
    ├─ accountedBytes = 100 MB
    ├─ actualL0Bytes = 50 MB（压缩后）
    └─ estimatedBytes = 200 MB ❌（4x 过估）

结果：
├─ 场景 B, C 被严重限流
└─ 吞吐量远低于实际容量
```

**替代方案 2：每请求固定 tokens（例如 1 MB）**

```
每个请求扣 1 MB tokens（无论大小）

缺点（致命）：
├─ 对小请求不公平
├─ 示例：
│   ├─ Request A: 写入 100 bytes → 扣 1 MB tokens
│   ├─ Request B: 写入 10 MB → 扣 1 MB tokens
│   └─ B 的 IO 是 A 的 100,000x，但消耗相同 tokens ❌
│
└─ 无法控制吞吐量
    ├─ Token limit = 1000 MB/s
    ├─ 如果都是小请求：实际吞吐量 << 1000 MB/s
    └─ 如果都是大请求：实际吞吐量 >> 1000 MB/s
```

**结论**：Linear Model 是必需的，成本（复杂度）可接受。

### 6.4 指数平滑（α=0.5）vs 其他平滑方法

**当前设计：指数平滑，α=0.5**

```
smoothedValue = 0.5 * currentValue + 0.5 * prevSmoothedValue

优点：
├─ 简单：单一参数
├─ 有记忆：保留历史信息
├─ 响应快：α=0.5 平衡响应和稳定性
└─ 无需存储历史数据

缺点：
├─ α 选择影响性能
├─ 单次异常值仍有影响
└─ 无法适应不同时间尺度
```

**α 值的影响**：

```
α = 0.9（快速响应）
────────────────────────────────────────────────
History: [100, 100, 100, 100, 500]

计算:
├─ T0: smoothed = 100
├─ T1: smoothed = 0.9*100 + 0.1*100 = 100
├─ T2: smoothed = 100
├─ T3: smoothed = 100
└─ T4: smoothed = 0.9*500 + 0.1*100 = 460

效果：
├─ 快速响应突变（460 接近 500）
└─ 但容易受噪声影响

α = 0.5（平衡）
────────────────────────────────────────────────
同样数据：

计算:
├─ T0: smoothed = 100
├─ T1-T3: smoothed = 100
└─ T4: smoothed = 0.5*500 + 0.5*100 = 300

效果：
├─ 响应适中（300 在 100 和 500 之间）
└─ 平衡了响应和稳定性 ✓

α = 0.1（稳定）
────────────────────────────────────────────────
同样数据：

计算:
└─ T4: smoothed = 0.1*500 + 0.9*100 = 140

效果：
├─ 非常稳定（140 接近 100）
├─ 但响应太慢（需要多个周期才能收敛）
└─ 可能错过重要信号
```

**替代方案 1：移动平均（MA）**

```
smoothedValue = (v[t-3] + v[t-2] + v[t-1] + v[t]) / 4

优点：
├─ 直观
└─ 对所有样本权重相同

缺点（相比指数平滑）：
├─ 需要存储历史数据
├─ 内存开销：O(窗口大小)
├─ 旧数据突然"掉出"窗口，导致跳变
└─ 无法给近期数据更高权重
```

**替代方案 2：自适应 α（根据方差）**

```
if variance(recent_samples) > threshold:
    α = 0.9  // 快速响应
else:
    α = 0.3  // 稳定

优点：
├─ 适应不同场景
└─ 波动大时响应快，波动小时更稳定

缺点：
├─ 复杂度高
├─ 需要计算方差（额外开销）
├─ 参数更多（threshold, α_high, α_low）
└─ 边界情况处理复杂
```

**结论**：α=0.5 的指数平滑是最优选择：
- 简单高效
- 平衡响应和稳定性
- 无需额外存储

---

## 七、总结与心智模型

### 7.1 核心思想总结

`ioLoadListener` 实现了一个 **基于 LSM 状态反馈的自适应 Token 分配系统**，通过以下机制确保 IO 资源不被过度使用：

> **每 15 秒从 Pebble 采集 L0 metrics，计算健康分数，根据 compaction/flush 速率和当前负载，使用分段函数计算下一个周期的 token 总量，然后每 1ms 平滑分配这些 tokens 给 granter。同时，通过 Linear Models 持续学习"请求声称字节 → 实际 LSM 字节"的映射关系，提高 token 估算准确性。**

**核心设计原则**：

1. **反馈控制**：LSM 状态 → Token 调整 → 写入速率 → LSM 状态（闭环）
2. **分段策略**：根据 L0 score 使用不同的 token 计算策略
3. **双重保护**：compaction 和 flush 两个维度的 tokens，取最小值
4. **自适应学习**：Linear Models 动态拟合工作负载特征
5. **平滑分配**：避免 burst，每 1ms 分配一次

### 7.2 心智模型

**如果只记住一件事，那就是**：

> `ioLoadListener` 是一个 **PID 控制器**的简化版，目标是让 L0 健康分数稳定在 0.5 左右。它通过动态调整 "token 水龙头的开度"（token rate），控制 "水位"（L0 size），同时用 Linear Models 精确测量每个"用水请求"的实际消耗。

**类比**：

```
ioLoadListener ≈ 水库的智能调度系统

水库状态（L0）:
├─ 水位（L0 Bytes）
├─ 水质（Sub-level count）
└─ 目标：维持在安全水位

流入（写入请求）:
├─ 每个请求像"蓄水"
├─ Token 控制流入速率
└─ Linear Model 预测实际蓄水量

流出（Compaction）:
├─ Compaction 像"泄洪"
├─ smoothedIntL0CompactedBytes 是泄洪速率
└─ 目标：流入 ≤ 流出

控制策略：
├─ 水位低（score < 0.5）：无限制流入
├─ 水位正常（score ∈ [0.5, 1.0)）：限制流入，但留余量
├─ 水位高（score ∈ [1.0, 2.0)）：严格限制流入
└─ 水位危险（score >= 2.0）：极度限制（仅允许泄洪速率的一半）

自适应学习（Linear Models）:
├─ 观察：声称蓄水 1m³，实际蓄水 2m³（写放大）
├─ 学习：建立 "声称量 → 实际量" 的模型
└─ 应用：未来预估更准确
```

### 7.3 简化伪代码

```python
class IOLoadListener:
    def __init__(self):
        self.smoothedIntL0CompactedBytes = 0
        self.smoothedNumFlushTokens = 0
        self.flushUtilTargetFraction = 1.0
        self.l0WriteLM = LinearModel(multiplier=1.75, constant=1)

    # ===== 每 15s 调用 =====
    def pebbleMetricsTick(self, metrics):
        # 1. 计算增量
        intL0AddedBytes = metrics.L0Added - self.prevL0Added
        intL0CompactedBytes = (self.prevL0Bytes + intL0AddedBytes -
                               metrics.curL0Bytes)

        # 2. 平滑 compaction bytes
        self.smoothedIntL0CompactedBytes = (
            0.5 * intL0CompactedBytes +
            0.5 * self.smoothedIntL0CompactedBytes)

        # 3. 计算 L0 健康分数
        score = max(
            metrics.L0SubLevels / 20,
            metrics.L0Files / 4000
        ) * 2

        # 4. 计算 compaction tokens（分段函数）
        if score < 0.5:
            compactionTokens = UNLIMITED
        elif score < 1.0:
            compactionTokens = (
                -score * 2 * self.smoothedIntL0CompactedBytes +
                3 * self.smoothedIntL0CompactedBytes)
        elif score < 2.0:
            compactionTokens = (
                -score * self.smoothedIntL0CompactedBytes / 2 +
                3 * self.smoothedIntL0CompactedBytes / 2)
        else:
            compactionTokens = self.smoothedIntL0CompactedBytes / 2

        # 5. 计算 flush tokens
        intFlushTokens = metrics.FlushPeakRate * 15
        self.smoothedNumFlushTokens = (
            0.5 * intFlushTokens +
            0.5 * self.smoothedNumFlushTokens)

        # 自适应调整
        if metrics.WriteStallCount > 0:
            self.flushUtilTargetFraction -= 0.025
        elif highTokenUsage:
            self.flushUtilTargetFraction += 0.025

        flushTokens = (self.flushUtilTargetFraction *
                       self.smoothedNumFlushTokens)

        # 6. 取最小值
        totalTokens = min(compactionTokens, flushTokens)

        # 7. 计算 elastic tokens
        if score >= 0.2:
            elasticTokens = (self.smoothedIntL0CompactedBytes *
                             (1.25 - 1.25 * (score - 0.2)))
        else:
            elasticTokens = UNLIMITED

        # 8. 更新 Linear Models
        self.l0WriteLM.update(
            accountedBytes=metrics.WriteAccountedBytes,
            actualBytes=metrics.ActualL0WriteBytes,
            workCount=metrics.WorkCount)

        # 9. 返回结果
        return {
            'totalTokens': totalTokens,
            'elasticTokens': elasticTokens,
        }

    # ===== 每 1ms 调用 =====
    def allocateTokensTick(self, totalTokens, remainingTicks):
        # 向上取整，平滑分配
        tokensThisTick = (
            (totalTokens + remainingTicks - 1) / remainingTicks)

        # 传递给 granter
        self.granter.setAvailableTokens(tokensThisTick)

# ===== Linear Model =====
class LinearModel:
    def __init__(self, multiplier, constant):
        self.smoothedMultiplier = multiplier
        self.smoothedConstant = constant

    def update(self, accountedBytes, actualBytes, workCount):
        # 拟合 y = a*x + b*n
        constant = 1  # 最小值
        multiplier = max(0, actualBytes - constant * workCount) / accountedBytes

        # Clip to bounds
        multiplier = clip(multiplier, self.minMult, self.maxMult)

        # 检查是否能完全解释 actualBytes
        modelBytes = multiplier * accountedBytes + constant * workCount
        if modelBytes < actualBytes:
            constant += (actualBytes - modelBytes) / workCount

        # 指数平滑
        self.smoothedMultiplier = (
            0.5 * multiplier + 0.5 * self.smoothedMultiplier)
        self.smoothedConstant = (
            0.5 * constant + 0.5 * self.smoothedConstant)

    def estimate(self, accountedBytes):
        return (self.smoothedMultiplier * accountedBytes +
                self.smoothedConstant)
```

### 7.4 关键要点速查表

| 维度 | 设计决策 | 理由 |
|------|---------|------|
| 调整周期 | 15 秒 | 匹配 compaction 时间尺度，平衡响应和稳定性 |
| 分配频率 | 1ms | 平滑 burst，降低延迟抖动 |
| Token 单位 | Bytes | 精确表达 IO 开销，支持公平分配 |
| Compaction Tokens | 分段函数（基于 score） | 不同负载下不同策略，渐进式限流 |
| Flush Tokens | 自适应利用率 | 动态学习安全吞吐量，避免 write stall |
| 最终 Tokens | min(compaction, flush) | 双重保护，避免任一瓶颈 |
| Elastic 隔离 | 更早限流（score >= 0.2） | 牺牲 elastic 保护 regular |
| Linear Model | 指数平滑拟合 | 自适应学习工作负载，提高估算准确性 |
| 平滑方法 | α=0.5 指数平滑 | 平衡响应速度和稳定性 |

---

## 附录：代码位置索引

| 组件 | 文件位置 | 行号 |
|------|---------|------|
| `ioLoadListener` | `pkg/util/admission/io_load_listener.go` | 226-240 |
| `ioLoadListenerState` | `pkg/util/admission/io_load_listener.go` | 244-299 |
| `pebbleMetricsTick()` | `pkg/util/admission/io_load_listener.go` | 539-592 |
| `adjustTokensInner()` | `pkg/util/admission/io_load_listener.go` | 837-1311 |
| `allocateTokensTick()` | `pkg/util/admission/io_load_listener.go` | 597-696 |
| `storePerWorkTokenEstimator` | `pkg/util/admission/store_token_estimation.go` | 115-141 |
| `updateEstimates()` | `pkg/util/admission/store_token_estimation.go` | 195-321 |
| `tokensLinearModelFitter` | `pkg/util/admission/tokens_linear_model.go` | 31-44 |
| `updateModelUsingIntervalStats()` | `pkg/util/admission/tokens_linear_model.go` | 85-157 |
| `computeL0CompactionTokensLowerBound()` | `pkg/util/admission/io_load_listener.go` | 332-349 |

---

**本章完**。下一章将深入分析 `diskBandwidthLimiter` 的磁盘带宽建模和 Elastic 工作的带宽限制机制。
