# 第十五章 kvSlotAdjuster 动态槽位调整机制——基于 CPU 负载的自适应准入控制

---

## 一、第一轮 BFS：整体职责与设计动机（Why）

### 1.1 背景与要解决的问题

在分布式数据库中，CPU 资源是最宝贵且最难预测的资源之一。固定的准入控制策略面临以下困境：

**问题 1：工作负载的动态性**
```
时间 T0: 简单点查，CPU 利用率 20%
    ↓
    固定 slots = 50
    结果：资源浪费，throughput 受限

时间 T1: 复杂 JOIN + 聚合，CPU 利用率 95%
    ↓
    固定 slots = 50
    结果：过载，请求延迟飙升，系统不稳定
```

**问题 2：硬件异构性**
```
节点 A: 4 核 CPU
节点 B: 64 核 CPU

固定 slots = 50
    ├─► 节点 A: 严重过载（50 / 4 = 12.5 slots/core）
    └─► 节点 B: 严重浪费（50 / 64 = 0.78 slots/core）
```

**问题 3：混合工作负载**
```
场景: CPU-bound + IO-bound 混合
    ↓
CPU-bound 阶段: 需要严格的 slot 控制
IO-bound 阶段: 可以放松 slot 限制（goroutine 大量阻塞在 IO）
    ↓
固定 slots 无法适应这种切换
```

**kvSlotAdjuster 的使命**：
根据实时 CPU 负载动态调整 KVWork 的 slots 数量，在过载保护（backpressure）和资源利用率（utilization）之间取得动态平衡。

### 1.2 在系统中的位置

**所属子系统**：Admission Control（准入控制）

**协作关系图**：
```
┌─────────────────────────────────────────────────────────────┐
│                   goschedstats / grunning                    │
│           (Go runtime scheduler stats collector)             │
│         周期性采样 goroutine 可运行队列长度                   │
└────────────────────────┬────────────────────────────────────┘
                         │ runnable, procs, samplePeriod
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                    GrantCoordinator                          │
│                 (准入控制总协调器)                            │
│         ┌──────────────────────────────────────┐             │
│         │  CPULoad(runnable, procs, period)    │             │
│         └──────────┬───────────────────────────┘             │
│                    │                                          │
│                    ↓                                          │
│         ┌──────────────────────────────────────┐             │
│         │  coord.mu.cpuLoadListener.CPULoad()  │             │
│         └──────────┬───────────────────────────┘             │
└────────────────────┼────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────┐
│                   kvSlotAdjuster                             │
│           (CPU 负载监听器 & 过载指示器)                       │
│                                                               │
│  实现接口:                                                    │
│  - CPULoadListener: 接收 CPU 负载信号                        │
│  - cpuOverloadIndicator: 提供过载判断                        │
│                                                               │
│  核心职责:                                                    │
│  1. 分析 runnable goroutines 数量                            │
│  2. 决策: 增加 / 保持 / 减少 slots                           │
│  3. 调用 slotGranter.setTotalSlotsLocked()                   │
└────────────┬────────────────────────────────────────────────┘
             │
             ↓
┌─────────────────────────────────────────────────────────────┐
│                      slotGranter                             │
│                 (KVWork 的 slot 管理器)                       │
│                                                               │
│  状态:                                                        │
│  - totalSlots: 总 slot 数 (由 kvSlotAdjuster 动态调整)       │
│  - usedSlots: 已使用 slot 数                                 │
│                                                               │
│  操作:                                                        │
│  - tryGetLocked(): 尝试获取 slot                             │
│  - returnGrantLocked(): 归还 slot                            │
│  - setTotalSlotsLocked(): 更新总 slot 数                     │
└────────────┬────────────────────────────────────────────────┘
             │
             ↓
┌─────────────────────────────────────────────────────────────┐
│                      WorkQueue                               │
│                  (KVWork 请求队列)                            │
│                                                               │
│  影响:                                                        │
│  - slots 增加 → 更多请求获得准入                             │
│  - slots 减少 → 更多请求排队等待                             │
└─────────────────────────────────────────────────────────────┘
```

**关键依赖**：
1. **goschedstats**：Go runtime 的 scheduler stats 采集
   - 提供 `runnable` (可运行 goroutine 数量)
   - 提供 `procs` (GOMAXPROCS)
   - 在不支持的平台上返回 `goschedstats.Supported = false`

2. **slotGranter**：slot 分配器
   - 被 kvSlotAdjuster 控制 `totalSlots`
   - 提供 `usedSlots` 给 kvSlotAdjuster 决策

### 1.3 核心对象与关键状态

**源代码位置**：`pkg/util/admission/kv_slot_adjuster.go:28-40`

```go
type kvSlotAdjuster struct {
    settings *cluster.Settings  // 集群配置
    granter  *slotGranter       // 关联的 slotGranter（KVWork）

    // ========== 边界约束 ==========
    minCPUSlots int  // 最小 slots（默认 1）
    maxCPUSlots int  // 最大 slots（默认 100,000）

    // ========== Metrics（可观测性）==========
    totalSlotsMetric                 *metric.Gauge    // 当前 totalSlots
    cpuLoadShortPeriodDurationMetric *metric.Counter  // 短周期采样时长统计
    cpuLoadLongPeriodDurationMetric  *metric.Counter  // 长周期采样时长统计
    slotAdjusterIncrementsMetric     *metric.Counter  // slot 增加次数
    slotAdjusterDecrementsMetric     *metric.Counter  // slot 减少次数
}
```

**接口实现**：
```go
// 源代码位置: kv_slot_adjuster.go:42-43
var _ cpuOverloadIndicator = &kvSlotAdjuster{}
var _ CPULoadListener = &kvSlotAdjuster{}
```

**cpuOverloadIndicator 接口**：
```go
// 判断 CPU 是否过载
func (kvsa *kvSlotAdjuster) isOverloaded() bool {
    return kvsa.granter.usedSlots >= kvsa.granter.totalSlots &&
           !kvsa.granter.skipSlotEnforcement
}
```
- 被 `GrantCoordinator.tryGet()` 调用
- 过载时停止授权，返回 `grantFailDueToSharedResource`

**CPULoadListener 接口**：
```go
// 接收 CPU 负载信号
func (kvsa *kvSlotAdjuster) CPULoad(
    runnable int,        // 可运行 goroutine 数量
    procs int,           // GOMAXPROCS
    samplePeriod time.Duration,  // 采样周期
)
```
- 被 `GrantCoordinator.CPULoad()` 调用
- 周期性接收 scheduler stats，执行 slot 调整逻辑

**关键不变量**：
```
Invariant 1: minCPUSlots ≤ totalSlots ≤ maxCPUSlots
Invariant 2: 调整是渐进式的（additive increase, additive decrease）
Invariant 3: 仅在 usedSlots ≤ totalSlots 时才减少 slots（避免过度反应）
```

### 1.4 为什么需要 kvSlotAdjuster？

**设计理念对比**：

| 方案              | 优点                   | 缺点                          | 适用场景           |
|------------------|----------------------|------------------------------|------------------|
| **固定 slots**    | 简单，可预测           | 无法适应负载变化，资源利用率低    | 负载稳定的系统     |
| **基于 CPU 利用率** | 直观                   | 滞后性强，无法反映调度压力       | 粗粒度控制        |
| **基于队列长度**   | 反应快速               | 间接信号，可能振荡              | 简单场景          |
| **kvSlotAdjuster** | 直接感知调度压力，动态适应 | 依赖 goschedstats，实现复杂     | 生产级分布式数据库 |

**kvSlotAdjuster 的核心优势**：
1. **直接信号**：使用 `runnable goroutines / procs` 作为 CPU 压力指标
   - 比 CPU 利用率更准确（反映调度队列压力）
   - 比队列长度更本质（直接反映系统能力）

2. **自适应**：无需人工调参
   - IO-bound: slots 自动增长到数千（goroutine 大量阻塞）
   - CPU-bound: slots 快速收缩到数十（goroutine 竞争激烈）

3. **渐进式调整**：避免剧烈波动
   - 每次 ±1 slot（additive increase/decrease）
   - 采样频率 1ms，可以每秒调整 1000 次

---

## 二、第二轮 BFS：控制流与交互关系（How it flows）

### 2.1 触发时机

**kvSlotAdjuster 的激活是被动的**，由外部周期性调用驱动，**不是主动的定时任务**。

**调用链全景**：
```
┌─────────────────────────────────────────────────────────────┐
│ 步骤 0: 系统启动时注册 CPU 负载回调                           │
└─────────────────────────────────────────────────────────────┘
    ↓
    pkg/server/node.go (或类似入口)
    ├─► 创建 GrantCoordinators
    │   └─► 创建 GrantCoordinator (RegularCPU)
    │       └─► 创建 kvSlotAdjuster
    │           └─► 注册到 coord.mu.cpuLoadListener
    │
    └─► 启动 goschedstats / grunning 采集器
        └─► 注册回调: coord.CPULoad()

┌─────────────────────────────────────────────────────────────┐
│ 步骤 1: goschedstats 周期性采样（每 1ms）                     │
└─────────────────────────────────────────────────────────────┘
    ↓
    goschedstats.Collector.tick()
    ├─► 读取 Go runtime scheduler stats
    │   ├─► sched.runqsize (全局队列)
    │   ├─► p.runqhead/tail (per-P 本地队列)
    │   └─► 计算 runnable = 全部可运行 goroutine 数
    │
    └─► 触发回调: coord.CPULoad(runnable, procs, 1ms)

┌─────────────────────────────────────────────────────────────┐
│ 步骤 2: GrantCoordinator 转发信号                            │
└─────────────────────────────────────────────────────────────┘
    ↓
    // 源代码位置: grant_coordinator.go:405-446
    func (coord *GrantCoordinator) CPULoad(runnable, procs, period) {
        coord.mu.Lock()
        defer coord.mu.Unlock()

        // 更新 GOMAXPROCS
        coord.mu.numProcs = procs

        // ▼ 关键！转发给 kvSlotAdjuster
        coord.mu.cpuLoadListener.CPULoad(runnable, procs, period)

        // 处理 skipEnforcement 逻辑
        skipEnforcement := period > 1ms || !goschedstats.Supported
        kvg := coord.granters[KVWork].(*slotGranter)
        kvg.skipSlotEnforcement = skipEnforcement

        // 可能触发授权
        coord.tryGrantLocked()
    }

┌─────────────────────────────────────────────────────────────┐
│ 步骤 3: kvSlotAdjuster 执行调整逻辑                          │
└─────────────────────────────────────────────────────────────┘
    ↓
    // 源代码位置: kv_slot_adjuster.go:45-110
    func (kvsa *kvSlotAdjuster) CPULoad(runnable, procs, period) {
        // 读取配置的过载阈值
        threshold := KVSlotAdjusterOverloadThreshold.Get(&settings.SV)
        // 默认值: 32

        // 读取当前使用量
        usedSlots := kvsa.granter.usedSlots

        // ▼ 核心决策逻辑（见下一节详述）
        if runnable >= threshold * procs {
            // CPU 过载 → 减少 slots
            kvsa.granter.setTotalSlotsLocked(tryDecreaseSlots(...))
        } else if runnable <= threshold * procs / 2 {
            // CPU 空闲 → 增加 slots
            kvsa.granter.setTotalSlotsLocked(tryIncreaseSlots(...))
        }
        // 否则：保持不变

        // 更新 metrics
        kvsa.totalSlotsMetric.Update(kvsa.granter.totalSlots)
    }

┌─────────────────────────────────────────────────────────────┐
│ 步骤 4: slotGranter 应用新的 totalSlots                      │
└─────────────────────────────────────────────────────────────┘
    ↓
    // 源代码位置: granter.go:160-177
    func (sg *slotGranter) setTotalSlotsLockedInternal(totalSlots int) {
        if totalSlots > sg.totalSlots {
            // slots 增加
            if sg.totalSlots <= sg.usedSlots && totalSlots > sg.usedSlots {
                // 从耗尽状态恢复
                exhaustedDuration := now.Sub(sg.exhaustedStart)
                sg.slotsExhaustedDurationMetric.Inc(exhaustedDuration)
            }
        } else if totalSlots < sg.totalSlots {
            // slots 减少
            if sg.totalSlots > sg.usedSlots && totalSlots <= sg.usedSlots {
                // 进入耗尽状态
                sg.exhaustedStart = now
            }
        }

        sg.totalSlots = totalSlots  // ◄ 实际更新
    }

┌─────────────────────────────────────────────────────────────┐
│ 步骤 5: 影响后续准入决策                                      │
└─────────────────────────────────────────────────────────────┘
    ↓
    新的 KVWork 请求到达
    ↓
    WorkQueue.Admit() → granter.tryGet() → slotGranter.tryGetLocked()
    ↓
    // 源代码位置: granter.go:57-78
    func (sg *slotGranter) tryGetLocked(count int64, _) grantResult {
        // ▼ 使用最新的 totalSlots 判断
        if sg.usedSlots < sg.totalSlots || sg.skipSlotEnforcement {
            sg.usedSlots++
            return grantSuccess
        }
        return grantFailDueToSharedResource
    }
```

**时间维度分析**：

```
时间轴（以 1ms 为单位）

T=0ms   : goschedstats.tick() → runnable=50, procs=8
          → CPULoad() → kvSlotAdjuster 决策
          → totalSlots: 40 → 41 (增加)

T=1ms   : goschedstats.tick() → runnable=55, procs=8
          → CPULoad() → kvSlotAdjuster 决策
          → totalSlots: 41 → 42 (继续增加)

T=2ms   : goschedstats.tick() → runnable=300, procs=8
          → CPULoad() → kvSlotAdjuster 决策
          → runnable >= 32*8=256 ✓ (过载)
          → totalSlots: 42 → 41 (减少)

T=3ms   : goschedstats.tick() → runnable=320, procs=8
          → totalSlots: 41 → 40 (继续减少)

...

T=100ms : goschedstats.tick() → runnable=100, procs=8
          → runnable < 256, runnable > 128 → 保持不变
          → totalSlots: 30 (稳定)
```

### 2.2 状态变化过程

**状态机视图**：
```
┌─────────────────────────────────────────────────────────────┐
│                    kvSlotAdjuster 状态空间                   │
└─────────────────────────────────────────────────────────────┘

State 1: CPU 空闲 (Underloaded)
    条件: runnable ≤ threshold * procs / 2
    例如: runnable ≤ 32 * 8 / 2 = 128

    决策: 增加 slots

    行为:
    ├─► 检查 usedSlots >= totalSlots (slots 被用满)
    ├─► 检查 totalSlots < maxCPUSlots (未达上限)
    └─► 满足条件 → totalSlots++ (additive increase)

    目的: 提高 throughput，充分利用 CPU

State 2: CPU 正常 (Normal)
    条件: threshold * procs / 2 < runnable < threshold * procs
    例如: 128 < runnable < 256

    决策: 保持不变

    行为:
    └─► 不调用 setTotalSlotsLocked()

    目的: 避免频繁抖动

State 3: CPU 过载 (Overloaded)
    条件: runnable >= threshold * procs
    例如: runnable >= 256

    决策: 减少 slots

    行为:
    ├─► 检查 usedSlots > 0 (有 slot 在使用)
    ├─► 检查 totalSlots > minCPUSlots (未达下限)
    ├─► 检查 usedSlots ≤ totalSlots (调整未生效前不继续减少)
    └─► 满足条件 → totalSlots-- (additive decrease)

    目的: 背压（backpressure），保护 CPU 不被打爆
```

**关键条件检查（减少 slots）**：
```go
// 源代码位置: kv_slot_adjuster.go:58-78
tryDecreaseSlots := func(total int, adjustMetric bool) int {
    // 条件 1: usedSlots > 0
    // 原因: 如果没有 slot 在用，说明没有负载，不需要减少
    if usedSlots > 0 &&

       // 条件 2: total > kvsa.minCPUSlots
       // 原因: 保证至少有 minCPUSlots（默认 1）个 slot
       total > kvsa.minCPUSlots &&

       // 条件 3: usedSlots ≤ total (关键！)
       // 原因: 如果 usedSlots > total，说明之前的减少还未生效
       //      （有请求 bypass 或正在处理中）
       //      此时继续减少会导致过度反应
       usedSlots <= total {

        total--  // 减少 1 个 slot

        if adjustMetric {
            kvsa.slotAdjusterDecrementsMetric.Inc(1)
        }
    }
    return total
}
```

**实例：为什么需要 `usedSlots ≤ total` 检查？**
```
场景: 快速过载

T=0ms:
    totalSlots = 100, usedSlots = 100
    runnable = 300 (过载)
    → 减少 slots → totalSlots = 99

T=1ms:
    totalSlots = 99, usedSlots = 100 (仍有 1 个未释放)
    runnable = 310 (继续过载)

    如果没有 usedSlots ≤ total 检查:
    → 继续减少 → totalSlots = 98
    → 但 usedSlots 还是 100!
    → 继续减少 → totalSlots = 97
    → ...
    → totalSlots 可能快速跌到 1，但 usedSlots 仍然高

    有了 usedSlots ≤ total 检查:
    → 检测到 usedSlots=100 > totalSlots=99
    → 跳过减少，等待 usedSlots 下降
    → 给系统时间让之前的调整生效
```

**增加 slots 的条件**：
```go
// 源代码位置: kv_slot_adjuster.go:79-97
tryIncreaseSlots := func(total int, adjustMetric bool) int {
    // 条件 1: usedSlots >= total (slots 被用满或接近用满)
    // 原因: 只有需求饱和时才增加供给
    if usedSlots >= total &&

       // 条件 2: total < kvsa.maxCPUSlots
       // 原因: 不超过上限（默认 100,000）
       total < kvsa.maxCPUSlots {

        total++  // 增加 1 个 slot

        if adjustMetric {
            kvsa.slotAdjusterIncrementsMetric.Inc(1)
        }
    }
    return total
}
```

**注释中的重要说明**：
```go
// NB: If the workload is IO bound, the slot count here will keep
// incrementing until these slots are no longer the bottleneck for
// admission. So it is not unreasonable to see this slot count go into
// the 1000s. If the workload switches to being CPU bound, we can
// decrease by 1000 slots every second (because the CPULoad ticks are at
// 1ms intervals, and we do additive decrease).
```

**翻译**：
- IO-bound 负载：slots 会持续增长到数千
  - 因为 goroutine 大量阻塞在 IO，usedSlots 总是满的
  - 但 runnable goroutines 很少（大部分在等待 IO）
  - kvSlotAdjuster 会持续增加 slots
- CPU-bound 负载：可以每秒减少 1000 个 slots
  - 1ms 间隔 × 1000 次/秒 = 1000 slots/秒
  - 快速响应负载切换

### 2.3 与其他组件的交互

**交互图（数据流）**：
```
┌─────────────────────────────────────────────────────────────┐
│                      INPUT SIGNALS                           │
└─────────────────────────────────────────────────────────────┘
    │
    ├─► runnable (int)
    │   └─ 来源: goschedstats
    │      语义: 可运行但未运行的 goroutine 数量
    │      采样: 每 1ms
    │
    ├─► procs (int)
    │   └─ 来源: runtime.GOMAXPROCS()
    │      语义: 逻辑 CPU 核心数
    │      变化: 很少变化（仅在配置更新时）
    │
    └─► samplePeriod (time.Duration)
        └─ 来源: goschedstats
           语义: 采样周期
           正常: 1ms
           异常: >1ms (调度器压力过大，采样被延迟)

                    ↓

┌─────────────────────────────────────────────────────────────┐
│                   kvSlotAdjuster.CPULoad()                   │
│                      (核心决策逻辑)                           │
└─────────────────────────────────────────────────────────────┘
    │
    ├─► 读取 threshold (cluster setting)
    │   默认: 32 (可通过 SQL 调整)
    │   语义: 每个 CPU 核允许的可运行 goroutine 数
    │
    ├─► 读取 usedSlots (from slotGranter)
    │   语义: 当前正在使用的 slot 数量
    │   来源: slotGranter.usedSlots
    │
    └─► 计算过载阈值
        overloadThreshold = threshold * procs
        underloadThreshold = threshold * procs / 2

                    ↓

┌─────────────────────────────────────────────────────────────┐
│                      OUTPUT ACTIONS                          │
└─────────────────────────────────────────────────────────────┘
    │
    ├─► setTotalSlotsLocked(newTotal)
    │   └─ 目标: slotGranter
    │      效果: 更新 totalSlots
    │      频率: 每次 CPULoad 调用时可能执行
    │
    ├─► Metrics 更新
    │   ├─ totalSlotsMetric.Update()
    │   ├─ slotAdjusterIncrementsMetric.Inc()
    │   └─ slotAdjusterDecrementsMetric.Inc()
    │
    └─► 间接影响: tryGrantLocked()
        └─ 在 GrantCoordinator.CPULoad() 最后调用
           如果 slots 增加，可能授权等待的请求
```

**skipEnforcement 机制**：
```go
// 源代码位置: grant_coordinator.go:431-440
skipEnforcement := samplePeriod > time.Millisecond || !goschedstats.Supported

// 应用到 KVWork granter
kvg := coord.granters[KVWork].(*slotGranter)
kvg.skipSlotEnforcement = skipEnforcement
```

**为什么需要 skipEnforcement？**
```
问题: 采样周期过长（>1ms）时的困境

假设 samplePeriod = 10ms:
    ↓
kvSlotAdjuster 调整频率: 每 10ms 一次
    ↓
突然的负载增长:
T=0ms:  totalSlots=100, 负载正常
T=5ms:  大量请求涌入，usedSlots→100
T=10ms: kvSlotAdjuster 才发现过载，减少 slots
T=10ms: 但已经有很多请求在排队等待

结果: 10ms 内无法调整，系统可能已经过载

解决方案:
    当 samplePeriod > 1ms 时:
    └─► skipSlotEnforcement = true
        └─► slotGranter.tryGetLocked() 忽略 totalSlots 限制
            └─► 请求可以直接通过（但仍计入 usedSlots）
                └─► 避免人为瓶颈
                    └─► 让系统自然负载均衡（依赖 Go scheduler）
```

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 CPULoad() 函数剖析

**函数签名**：
```go
// 源代码位置: kv_slot_adjuster.go:45
func (kvsa *kvSlotAdjuster) CPULoad(
    runnable int,              // 可运行 goroutine 数
    procs int,                 // GOMAXPROCS
    samplePeriod time.Duration,  // 采样周期
)
```

**输入**：
- `runnable`：Go scheduler 的全局可运行队列 + 所有 P 的本地可运行队列总和
- `procs`：`runtime.GOMAXPROCS(0)` 返回值
- `samplePeriod`：两次采样之间的时间间隔

**输出**：
- 无显式返回值
- 副作用：调用 `slotGranter.setTotalSlotsLocked()` 更新 `totalSlots`

**不变量（Invariants）**：
```
Invariant 1: 调用前后 minCPUSlots ≤ totalSlots ≤ maxCPUSlots
Invariant 2: 每次调用最多改变 totalSlots ±1
Invariant 3: 仅在 usedSlots ≤ totalSlots 时才减少 slots
Invariant 4: 仅在 usedSlots >= totalSlots 时才增加 slots
```

**完整代码注释版**：
```go
func (kvsa *kvSlotAdjuster) CPULoad(runnable int, procs int, samplePeriod time.Duration) {
    // ============================================================
    // 阶段 1: 读取配置与统计
    // ============================================================

    // 读取过载阈值（cluster setting）
    threshold := int(KVSlotAdjusterOverloadThreshold.Get(&kvsa.settings.SV))
    // 默认值: 32
    // 语义: 每个 CPU 核允许的可运行 goroutine 数量
    // 可通过 SQL 动态调整:
    // SET CLUSTER SETTING admission.kv_slot_adjuster.overload_threshold = 16;

    // 记录采样周期 metrics
    periodDurationMicros := samplePeriod.Microseconds()
    if samplePeriod > time.Millisecond {
        // 长周期 (异常)
        kvsa.cpuLoadLongPeriodDurationMetric.Inc(periodDurationMicros)
    } else {
        // 短周期 (正常)
        kvsa.cpuLoadShortPeriodDurationMetric.Inc(periodDurationMicros)
    }

    // ============================================================
    // 阶段 2: 读取当前状态
    // ============================================================

    // 当前已使用的 slots
    usedSlots := kvsa.granter.usedSlots

    // ============================================================
    // 阶段 3: 定义辅助函数 - tryDecreaseSlots
    // ============================================================

    tryDecreaseSlots := func(total int, adjustMetric bool) int {
        // CPU 过载 → 减少 slots

        // 条件检查（三个 AND 条件）
        if usedSlots > 0 &&                 // 有 slot 在使用
           total > kvsa.minCPUSlots &&      // 未触底
           usedSlots <= total {              // 之前的减少已生效

            total--  // additive decrease

            if adjustMetric {
                kvsa.slotAdjusterDecrementsMetric.Inc(1)
            }
        }
        return total
    }

    // ============================================================
    // 阶段 4: 定义辅助函数 - tryIncreaseSlots
    // ============================================================

    tryIncreaseSlots := func(total int, adjustMetric bool) int {
        // CPU 空闲 → 增加 slots

        // 条件检查（两个 AND 条件）
        if usedSlots >= total &&            // slots 已满或接近满
           total < kvsa.maxCPUSlots {       // 未触顶

            // 注释原文:
            // If the workload is IO bound, the slot count here will keep
            // incrementing until these slots are no longer the bottleneck for
            // admission. So it is not unreasonable to see this slot count go into
            // the 1000s. If the workload switches to being CPU bound, we can
            // decrease by 1000 slots every second (because the CPULoad ticks are at
            // 1ms intervals, and we do additive decrease).

            total++  // additive increase

            if adjustMetric {
                kvsa.slotAdjusterIncrementsMetric.Inc(1)
            }
        }
        return total
    }

    // ============================================================
    // 阶段 5: 核心决策逻辑
    // ============================================================

    // 计算过载阈值
    overloadThreshold := threshold * procs  // 例如: 32 * 8 = 256

    if runnable >= overloadThreshold {
        // ========== 分支 1: CPU 过载 ==========
        // 例如: runnable = 300 >= 256

        kvsa.granter.setTotalSlotsLocked(
            tryDecreaseSlots(kvsa.granter.totalSlots, true))

    } else if float64(runnable) <= float64(overloadThreshold/2) {
        // ========== 分支 2: CPU 空闲 ==========
        // 例如: runnable = 100 <= 128
        //
        // 注意: 使用 float64 避免整数除法截断
        // threshold*procs/2 而不是 (threshold/2)*procs

        kvsa.granter.setTotalSlotsLocked(
            tryIncreaseSlots(kvsa.granter.totalSlots, true))

    }
    // else: 128 < runnable < 256 → 不调整 (Normal 状态)

    // ============================================================
    // 阶段 6: 更新 Metrics
    // ============================================================

    kvsa.totalSlotsMetric.Update(int64(kvsa.granter.totalSlots))
}
```

**为什么使用三个阈值区间？**
```
为什么不是简单的 if-else (过载 vs 不过载)？

答案: 避免振荡（oscillation）

反例: 二阈值设计
    threshold = 256

T=0ms: runnable=250 < 256 → 增加 slots → totalSlots=51
T=1ms: runnable=257 > 256 → 减少 slots → totalSlots=50
T=2ms: runnable=254 < 256 → 增加 slots → totalSlots=51
T=3ms: runnable=258 > 256 → 减少 slots → totalSlots=50
...
结果: 疯狂抖动

正确: 三阈值设计（带迟滞）
    overloadThreshold = 256
    underloadThreshold = 128

    [0, 128)        → 增加 slots
    [128, 256)      → 保持不变 (deadband / hysteresis)
    [256, ∞)        → 减少 slots

T=0ms: runnable=250 → 保持不变
T=1ms: runnable=257 → 减少 slots
T=2ms: runnable=254 → 保持不变 (不会立即增加)
T=3ms: runnable=120 → 增加 slots

结果: 稳定，只在明确的信号下调整
```

### 3.2 setTotalSlotsLocked() 函数剖析

**函数签名**：
```go
// 源代码位置: granter.go:152-177
func (sg *slotGranter) setTotalSlotsLocked(totalSlots int)
```

**输入**：
- `totalSlots`：新的总 slot 数

**输出**：
- 无返回值
- 副作用：更新 `sg.totalSlots`，可能更新 `sg.exhaustedStart`

**并发控制**：
- 调用前提：已持有 `GrantCoordinator.mu` 锁
- 函数名后缀 `Locked` 表明此前提

**性能优化（mid-stack inlining）**：
```go
//gcassert:inline
func (sg *slotGranter) setTotalSlotsLocked(totalSlots int) {
    // Fast path: 值未变化，直接返回
    if totalSlots == sg.totalSlots {
        return
    }
    // Slow path: 调用实际更新逻辑
    sg.setTotalSlotsLockedInternal(totalSlots)
}
```
- `//gcassert:inline` 指示编译器内联此函数
- 快速路径仅一个比较，适合内联
- 避免函数调用开销

**核心逻辑**：
```go
func (sg *slotGranter) setTotalSlotsLockedInternal(totalSlots int) {
    // ============================================================
    // 分支 1: slots 增加
    // ============================================================
    if totalSlots > sg.totalSlots {
        // 检查: 是否从耗尽状态恢复？
        //
        // 条件:
        // - 旧值: totalSlots ≤ usedSlots (耗尽)
        // - 新值: totalSlots > usedSlots (有余量)
        if sg.totalSlots <= sg.usedSlots && totalSlots > sg.usedSlots {
            // 计算耗尽持续时间
            now := timeutil.Now()
            exhaustedMicros := now.Sub(sg.exhaustedStart).Microseconds()

            // 累计到 metric
            sg.slotsExhaustedDurationMetric.Inc(exhaustedMicros)

            // 示例:
            // T=0: totalSlots=50, usedSlots=50, exhaustedStart=T0
            // T=10ms: totalSlots→51, usedSlots=50
            // → exhaustedMicros = 10,000μs
            // → slotsExhaustedDurationMetric += 10,000
        }

    // ============================================================
    // 分支 2: slots 减少
    // ============================================================
    } else if totalSlots < sg.totalSlots {
        // 检查: 是否进入耗尽状态？
        //
        // 条件:
        // - 旧值: totalSlots > usedSlots (有余量)
        // - 新值: totalSlots ≤ usedSlots (耗尽)
        if sg.totalSlots > sg.usedSlots && totalSlots <= sg.usedSlots {
            // 记录耗尽开始时间
            sg.exhaustedStart = timeutil.Now()

            // 示例:
            // T=0: totalSlots=51, usedSlots=50
            // T=1ms: totalSlots→50, usedSlots=50
            // → 进入耗尽状态
            // → exhaustedStart = T1
        }
    }

    // ============================================================
    // 最终: 更新 totalSlots
    // ============================================================
    sg.totalSlots = totalSlots
}
```

**slotsExhaustedDuration Metric 的意义**：
```
这个 metric 回答了一个关键问题:
"系统有多少时间处于 slot 耗尽状态？"

用途:
1. 容量规划: exhaustedDuration 高 → 需要增加节点
2. 调优: exhaustedDuration 低 → 可以降低 maxCPUSlots
3. 告警: exhaustedDuration 突增 → 可能有异常负载

示例:
    1 小时 = 3,600,000,000μs
    slotsExhaustedDuration = 360,000,000μs
    → 耗尽比例 = 10%
    → 系统 10% 的时间处于 slot 饱和状态
```

### 3.3 isOverloaded() 函数剖析

**函数签名**：
```go
// 源代码位置: kv_slot_adjuster.go:112-114
func (kvsa *kvSlotAdjuster) isOverloaded() bool {
    return kvsa.granter.usedSlots >= kvsa.granter.totalSlots &&
           !kvsa.granter.skipSlotEnforcement
}
```

**调用者**：
```go
// grant_coordinator.go:472
res := coord.granters[workKind].tryGetLocked(count, demuxHandle)
    ↓
// granter.go:57-78 (slotGranter.tryGetLocked)
if sg.usedSlots < sg.totalSlots || sg.skipSlotEnforcement {
    return grantSuccess
}
if sg.workKind == KVWork {
    return grantFailDueToSharedResource  // ← KVWork 失败意味着 CPU 过载
}
return grantFailLocal
    ↓
// grant_coordinator.go:480-488
case grantFailDueToSharedResource:
    // 检查是否需要终止 Grant Chain
    if coord.mu.grantChainActive && coord.mu.grantChainIndex >= workKind {
        coord.tryTerminateGrantChain()  // ← 终止当前 Grant Chain
    }
    return false
```

**为什么需要 skipSlotEnforcement 检查？**
```go
// 两个条件 AND:
// 1. usedSlots >= totalSlots (slots 耗尽)
// 2. !skipSlotEnforcement (强制执行 slot 限制)

// skipSlotEnforcement = true 的情况:
// - samplePeriod > 1ms (采样周期过长)
// - !goschedstats.Supported (平台不支持 scheduler stats)

// 当 skipSlotEnforcement = true:
// → isOverloaded() 返回 false
// → tryGetLocked() 总是成功
// → 系统退化为"无限 slots"模式
// → 依赖 Go scheduler 和操作系统的调度
```

**设计权衡**：
```
为什么不在 skipSlotEnforcement=true 时也返回 true？

反例: 总是报告过载
    skipSlotEnforcement = true
    isOverloaded() 返回 true
    → GrantCoordinator 停止授权
    → 所有请求排队
    → 但 tryGetLocked() 会成功（因为 skipSlotEnforcement）
    → 矛盾！

正确: skipSlotEnforcement 时返回 false
    skipSlotEnforcement = true
    isOverloaded() 返回 false
    → GrantCoordinator 认为资源充足
    → 继续授权
    → tryGetLocked() 成功
    → 一致性 ✓
```

---

## 四、动态行为分析（Runtime 行为）

### 4.1 CPU 负载感知机制

**runnable goroutines 的含义**：
```
Go scheduler 架构:

┌─────────────────────────────────────────────────────────────┐
│ Global Run Queue                                             │
│ └─► sched.runqhead...runqtail (全局可运行 goroutine 队列)    │
└─────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────┐
│ Per-P Local Run Queues (每个逻辑 CPU 一个)                   │
│                                                               │
│  P0: [g1, g2, g3, ...]  ← p.runqhead...runqtail              │
│  P1: [g10, g11, ...]                                         │
│  P2: [g20, g21, g22, g23, ...]                               │
│  ...                                                         │
│  P7: [g70, ...]                                              │
└─────────────────────────────────────────────────────────────┘

runnable = len(GlobalQueue) + Σ len(P[i].LocalQueue)

示例 (8 核):
    GlobalQueue: 10 个 goroutine
    P0: 5 个
    P1: 3 个
    P2: 8 个
    P3-P7: 各 2 个

    runnable = 10 + 5 + 3 + 8 + 2*5 = 36
```

**为什么 runnable 是好的 CPU 压力指标？**
```
对比其他指标:

指标 1: CPU 利用率 (CPU usage %)
    ├─ 优点: 直观
    └─ 缺点:
        ├─ 滞后性: 采样间隔长（通常数秒）
        ├─ 粗粒度: 无法区分用户态 CPU 和系统态 CPU
        └─ 间接: CPU 100% 可能是一个 goroutine 死循环，不代表负载高

指标 2: 请求队列长度 (queue depth)
    ├─ 优点: 反应快
    └─ 缺点:
        ├─ 间接: 队列长不等于 CPU 压力（可能是 IO bound）
        └─ 局部: 仅反映准入控制层面的压力

指标 3: runnable goroutines / procs
    ├─ 优点:
    │   ├─ 直接: 反映 scheduler 的竞争程度
    │   ├─ 快速: 1ms 采样
    │   ├─ 准确: 区分 CPU-bound (runnable 高) vs IO-bound (runnable 低)
    │   └─ 归一化: 除以 procs 得到每核压力
    └─ 缺点:
        └─ 平台依赖: 需要 goschedstats 支持
```

**runnable/procs 的物理意义**：
```
runnable / procs = 平均每个 P 的可运行 goroutine 数

threshold = 32 (默认)

场景 1: 8 核机器
    runnable = 300
    runnable / procs = 300 / 8 = 37.5

    判断: 37.5 > 32 → 过载

    含义: 每个 P 平均有 37.5 个 goroutine 在等待调度
         → CPU 调度队列拥挤
         → goroutine 竞争激烈
         → 需要背压

场景 2: 64 核机器
    runnable = 300
    runnable / procs = 300 / 64 = 4.7

    判断: 4.7 < 32 → 正常

    含义: 每个 P 平均只有 4.7 个 goroutine 等待
         → CPU 有余量
         → 可以增加 slots

结论: 阈值 32 的含义是"每个 P 允许 32 个排队 goroutine"
```

### 4.2 Slot 调整决策逻辑

**决策树（完整版）**：
```
┌─────────────────────────────────────────────────────────────┐
│ CPULoad(runnable, procs, samplePeriod) 被调用                │
└─────────────────────────────────────────────────────────────┘
    ↓
读取 threshold (默认 32)
读取 usedSlots (from slotGranter)
计算 overloadThreshold = threshold * procs
计算 underloadThreshold = threshold * procs / 2
    ↓
┌─────────────────────────────────────────────────────────────┐
│ 决策分支 1: 判断负载等级                                      │
└─────────────────────────────────────────────────────────────┘
    │
    ├─► runnable >= overloadThreshold?
    │   ├─ Yes → CPU 过载 → 尝试减少 slots
    │   │   ↓
    │   │   usedSlots > 0?
    │   │   ├─ No → 跳过 (没有负载，不需要减少)
    │   │   └─ Yes
    │   │       ↓
    │   │       totalSlots > minCPUSlots?
    │   │       ├─ No → 跳过 (已达下限)
    │   │       └─ Yes
    │   │           ↓
    │   │           usedSlots ≤ totalSlots?
    │   │           ├─ No → 跳过 (之前的减少未生效)
    │   │           └─ Yes → totalSlots-- ✓
    │   │
    │   └─ No → 继续检查
    │       ↓
    │       runnable ≤ underloadThreshold?
    │       ├─ Yes → CPU 空闲 → 尝试增加 slots
    │       │   ↓
    │       │   usedSlots >= totalSlots?
    │       │   ├─ No → 跳过 (有余量，不需要增加)
    │       │   └─ Yes
    │       │       ↓
    │       │       totalSlots < maxCPUSlots?
    │       │       ├─ No → 跳过 (已达上限)
    │       │       └─ Yes → totalSlots++ ✓
    │       │
    │       └─ No → 正常负载 → 保持不变
    │
    ↓
更新 totalSlotsMetric
```

**实例：从空闲到过载的完整周期**：
```
初始状态:
    procs = 8
    threshold = 32
    overloadThreshold = 256
    underloadThreshold = 128
    totalSlots = 50
    usedSlots = 40
    minCPUSlots = 1
    maxCPUSlots = 100000

T=0ms: runnable=100 (空闲)
    ├─ runnable=100 ≤ underloadThreshold=128 ✓
    ├─ usedSlots=40 < totalSlots=50 ✗ (slots 未满)
    └─ 决策: 保持不变 (虽然 CPU 空闲，但 slots 有余量)

T=1ms: usedSlots 增长到 50
    runnable=110 (空闲)
    ├─ runnable=110 ≤ underloadThreshold=128 ✓
    ├─ usedSlots=50 ≥ totalSlots=50 ✓
    ├─ totalSlots=50 < maxCPUSlots=100000 ✓
    └─ 决策: totalSlots→51 (增加 1 个 slot)

T=2ms: runnable=115
    ├─ usedSlots=51 ≥ totalSlots=51 ✓
    └─ 决策: totalSlots→52

T=3ms-T=10ms: 持续增长
    runnable 保持在 110-120 之间
    usedSlots 总是等于 totalSlots (需求饱和)
    totalSlots: 52→53→...→59

T=11ms: 负载突然增加
    runnable=280 (过载！)
    ├─ runnable=280 ≥ overloadThreshold=256 ✓
    ├─ usedSlots=59 > 0 ✓
    ├─ totalSlots=59 > minCPUSlots=1 ✓
    ├─ usedSlots=59 ≤ totalSlots=59 ✓
    └─ 决策: totalSlots→58 (减少 1 个 slot)

T=12ms: runnable=290
    ├─ usedSlots=59 > totalSlots=58 ✗ (减少未生效)
    └─ 决策: 保持不变 (等待 usedSlots 下降)

T=13ms: usedSlots 下降到 58
    runnable=285
    ├─ usedSlots=58 ≤ totalSlots=58 ✓
    └─ 决策: totalSlots→57

T=14ms-T=30ms: 持续减少
    runnable 保持在 280-300 之间
    totalSlots: 57→56→...→40

T=31ms: runnable=250 (回落到正常区间)
    ├─ 128 < runnable=250 < 256
    └─ 决策: 保持不变 (Normal 状态)

T=100ms: 负载恢复正常
    runnable=100
    ├─ runnable=100 ≤ underloadThreshold=128 ✓
    ├─ usedSlots=40 ≥ totalSlots=40 ✓
    └─ 决策: totalSlots→41 (开始恢复)
```

### 4.3 为什么是惰性调整而非定时任务？

**设计对比**：

| 方案              | 触发方式         | 优点                      | 缺点                      |
|------------------|-----------------|--------------------------|--------------------------|
| **惰性调整 (现状)** | 被动，由 CPULoad 驱动 | 1. 零延迟（采样即调整）<br>2. 无额外线程<br>3. 数据新鲜 | 依赖外部调用频率 |
| **定时任务**       | 主动，独立 goroutine | 独立性强              | 1. 调整滞后<br>2. 额外线程开销<br>3. 可能读到旧数据 |

**惰性调整的实现**：
```
调用链:
    goschedstats.tick() (每 1ms)
        ↓
    coord.CPULoad(runnable, procs, 1ms)
        ↓
    kvsa.CPULoad(runnable, procs, 1ms)
        ↓
    决策 + 调整 slots
        ↓
    返回 (同一个 call stack)

特点:
1. 采样和调整在同一个时间点
2. 无需维护独立的 goroutine
3. 无需同步采样数据和调整逻辑
4. 采样频率 = 调整频率 (1ms)
```

**如果使用定时任务**：
```
设计:
    goroutine 1: goschedstats.tick() (每 1ms)
        ↓ 写入
    共享变量: latestRunnable, latestProcs
        ↓ 读取
    goroutine 2: kvSlotAdjuster.adjustLoop() (每 10ms)
        ↓
    决策 + 调整 slots

问题:
1. 数据滞后:
    T=0ms: runnable=100 (采样)
    T=5ms: runnable 飙升到 300 (未采样)
    T=10ms: adjustLoop 读到 runnable=100 (旧数据)
    → 错误决策: 增加 slots!

2. 锁竞争:
    goschedstats 写 latestRunnable 需要锁
    adjustLoop 读 latestRunnable 需要锁
    → 增加 coord.mu 的竞争

3. 额外成本:
    多一个常驻 goroutine
    多一个定时器
```

**惰性调整的优势总结**：
```
1. 零延迟: 采样 → 调整 在 1ms 内完成
2. 数据一致性: 调整基于最新的采样数据
3. 资源高效: 无额外 goroutine 和定时器
4. 简单性: 代码路径简单，易于理解和调试
```

### 4.4 Backpressure vs Utilization 平衡

**两个相互矛盾的目标**：
```
目标 1: Backpressure (背压，过载保护)
    ├─ 含义: 当系统过载时，拒绝新请求
    ├─ 好处: 保护系统稳定性，避免雪崩
    └─ 代价: 降低 throughput，增加延迟

目标 2: Utilization (资源利用率)
    ├─ 含义: 充分利用 CPU 资源
    ├─ 好处: 最大化 throughput
    └─ 代价: 可能过载

矛盾: 过于激进 → 过载
      过于保守 → 浪费

kvSlotAdjuster 的平衡策略:
```

**策略 1：渐进式调整**
```
Additive Increase / Additive Decrease (AIAD)

每次调整: ±1 slot
调整频率: 最快 1ms

好处:
├─ 避免剧烈波动
├─ 给系统时间响应
└─ 可以稳定在一个平衡点

示例:
    最优 slots = 50

    totalSlots=40 → 41 → 42 → ... → 50 (逐步增长)
    totalSlots=60 → 59 → 58 → ... → 50 (逐步下降)

    最终稳定在 50 附近 (±1)
```

**策略 2：三阈值设计（Hysteresis）**
```
过载阈值 = 256 (threshold * procs)
正常阈值 = 128-256 (deadband)
空闲阈值 = 128 (threshold * procs / 2)

作用: 避免频繁切换

反例 (二阈值):
    threshold = 200
    runnable 在 195-205 之间波动
    → 频繁增加/减少 slots
    → 系统不稳定

正例 (三阈值):
    runnable 在 195-205 之间波动
    → 在 [128, 256] 正常区间
    → 保持不变
    → 稳定 ✓
```

**策略 3：条件保护**
```
减少 slots 的条件:
    usedSlots ≤ totalSlots

意义: 确保之前的调整已生效

示例:
    T=0: totalSlots=100, usedSlots=100, runnable=300
    → 决策: totalSlots→99

    T=1ms: totalSlots=99, usedSlots=100 (仍有 1 个未释放)
    → runnable=310 (仍过载)
    → usedSlots > totalSlots ✗
    → 决策: 保持不变 (等待)

    T=5ms: usedSlots→99 (终于释放)
    → runnable=280
    → usedSlots ≤ totalSlots ✓
    → 决策: totalSlots→98

好处: 避免过度反应 (overreaction)
```

**策略 4：IO-bound 自动扩展**
```
场景: IO-bound 负载
    ├─ goroutine 大量阻塞在 disk IO, network IO
    ├─ usedSlots = totalSlots (slots 总是满的)
    └─ runnable < threshold * procs (很少 goroutine 可运行)

kvSlotAdjuster 的响应:
    runnable < 128 → 空闲 → 增加 slots
    usedSlots ≥ totalSlots → 满 → 继续增加

    totalSlots: 100 → 101 → ... → 1000 → ... → 10000

    直到: slots 不再是瓶颈 (usedSlots < totalSlots)

结果:
    CPU-bound: slots 在 数十 (受限于 CPU)
    IO-bound: slots 在 数千 (不受 CPU 限制)
    混合: 自动在两者之间切换

设计哲学:
    "Let the bottleneck dictate the limit"
    让真正的瓶颈决定 slots 数量
```

**平衡示意图**：
```
      Throughput
          ↑
          │           ┌─── 理想曲线 (无过载)
          │          ╱
          │         ╱
          │        ╱
    Max  ├───────╱─────┐ ← kvSlotAdjuster 的目标区域
          │      ╱       │   (高 utilization, 低 backpressure)
          │     ╱        │
          │    ╱         │
          │   ╱          ↓ 实际曲线 (有过载)
          │  ╱      ╱────┘
          │ ╱      ╱
          │╱______╱
          └────────────────────────────────► CPU Load
          0    Optimal    Overload

kvSlotAdjuster 维持在 "Optimal" 点附近:
    ├─ runnable 接近但不超过 threshold * procs
    ├─ usedSlots ≈ totalSlots (高利用率)
    └─ queue depth 较低 (低延迟)
```

---

## 五、具体示例（必须有）

### 5.1 示例 1：CPU 负载较低时的 Slot 扩张

**初始状态**：
```
时间: T=0
系统配置:
    procs = 8
    threshold = 32
    minCPUSlots = 1
    maxCPUSlots = 100000

当前状态:
    totalSlots = 20
    usedSlots = 20
    runnable = 80

计算:
    overloadThreshold = 32 * 8 = 256
    underloadThreshold = 32 * 8 / 2 = 128

    runnable=80 < underloadThreshold=128 ✓ (空闲)
```

**时间线（每 1ms 采样一次）**：

```
T=0ms: CPULoad(runnable=80, procs=8, period=1ms)
    ├─ 判断: 80 ≤ 128 → 空闲
    ├─ usedSlots=20 ≥ totalSlots=20 ✓ (slots 满)
    ├─ totalSlots=20 < maxCPUSlots=100000 ✓
    └─ 决策: totalSlots = 20 + 1 = 21

    影响:
        新的请求到达 → tryGetLocked()
        → usedSlots=20 < totalSlots=21 ✓
        → grantSuccess (原本会失败的请求现在成功了)

    Metrics:
        slotAdjusterIncrementsMetric += 1
        totalSlotsMetric = 21

T=1ms: CPULoad(runnable=85, procs=8, period=1ms)
    ├─ 判断: 85 ≤ 128 → 空闲
    ├─ usedSlots=21 ≥ totalSlots=21 ✓
    └─ 决策: totalSlots = 21 + 1 = 22

    观察: runnable 略微上升 (80→85)
          原因: 更多请求获得准入，更多 goroutine 变为可运行

T=2ms: CPULoad(runnable=88, procs=8, period=1ms)
    └─ 决策: totalSlots = 22 + 1 = 23

T=3ms: CPULoad(runnable=90, procs=8, period=1ms)
    └─ 决策: totalSlots = 23 + 1 = 24

...持续增长...

T=20ms: CPULoad(runnable=120, procs=8, period=1ms)
    ├─ totalSlots = 40
    ├─ 判断: 120 ≤ 128 → 仍然空闲
    └─ 决策: totalSlots = 40 + 1 = 41

T=21ms: CPULoad(runnable=125, procs=8, period=1ms)
    └─ 决策: totalSlots = 41 + 1 = 42

T=22ms: CPULoad(runnable=130, procs=8, period=1ms)
    ├─ 判断: 130 > 128, 130 < 256 → 正常
    └─ 决策: 保持不变 (totalSlots = 42)

    观察: 进入稳定状态
          runnable 在 128-135 之间波动
          totalSlots 稳定在 42

T=23ms-T=100ms: 稳定阶段
    runnable: 127-135 (在 Normal 区间波动)
    totalSlots: 42 (不变)
    usedSlots: 40-42 (接近满)
```

**数值变化总结**：

| 时间 (ms) | runnable | totalSlots | usedSlots | 决策      | 理由             |
|----------|----------|-----------|-----------|----------|------------------|
| 0        | 80       | 20        | 20        | +1 → 21   | 空闲，且 slots 满 |
| 1        | 85       | 21        | 21        | +1 → 22   | 持续空闲         |
| 5        | 95       | 25        | 25        | +1 → 26   | 持续空闲         |
| 10       | 105      | 30        | 30        | +1 → 31   | 持续空闲         |
| 15       | 115      | 35        | 35        | +1 → 36   | 持续空闲         |
| 20       | 120      | 40        | 40        | +1 → 41   | 仍空闲           |
| 22       | 130      | 42        | 42        | 保持      | 进入正常区间     |
| 30       | 132      | 42        | 41        | 保持      | 正常             |
| 50       | 128      | 42        | 40        | 保持      | 正常             |
| 100      | 131      | 42        | 42        | 保持      | 稳定             |

**对 KVWork 请求准入的影响**：

```
T=0ms (totalSlots=20):
    请求 A: tryGetLocked() → usedSlots=20 ≥ totalSlots=20
            → grantFailDueToSharedResource
            → 进入队列等待

T=1ms (totalSlots=21):
    请求 B: tryGetLocked() → usedSlots=20 < totalSlots=21 ✓
            → grantSuccess
            → 立即执行 (不用等待)

T=22ms (totalSlots=42, 稳定):
    请求到达率 = 42 个/ms
    队列: 空 (所有请求都能立即获得准入)
    延迟: P99 < 1ms (快速路径)
    Throughput: 42,000 req/sec
```

**关键观察**：
1. **渐进增长**：从 20 → 42 用了 22ms，每次 +1
2. **自动停止**：runnable 达到 128-135 时停止增长
3. **高效利用**：usedSlots ≈ totalSlots (利用率 > 95%)
4. **低延迟**：队列几乎为空

### 5.2 示例 2：CPU 负载升高时的 Slot 收缩

**初始状态**：
```
时间: T=0
系统配置: 同示例 1

当前状态:
    totalSlots = 100
    usedSlots = 100
    runnable = 150 (正常)

负载变化: T=10ms 时突然涌入大量复杂查询
```

**时间线**：

```
T=0ms-T=9ms: 稳定阶段
    runnable = 145-155 (正常区间)
    totalSlots = 100 (不变)
    usedSlots = 98-100

T=10ms: 负载突增
    ├─ 10 个复杂 JOIN 查询到达
    ├─ 每个查询启动 20 个 goroutine (总计 200 个)
    └─ runnable: 150 → 350 (暴涨!)

    CPULoad(runnable=350, procs=8, period=1ms)
    ├─ 判断: 350 ≥ 256 → 过载!
    ├─ usedSlots=100 > 0 ✓
    ├─ totalSlots=100 > 1 ✓
    ├─ usedSlots=100 ≤ totalSlots=100 ✓
    └─ 决策: totalSlots = 100 - 1 = 99

    Metrics:
        slotAdjusterDecrementsMetric += 1
        totalSlotsMetric = 99

T=11ms: CPULoad(runnable=360, procs=8, period=1ms)
    ├─ 判断: 360 ≥ 256 → 过载
    ├─ usedSlots=100 > totalSlots=99 ✗ (减少未生效!)
    └─ 决策: 保持不变 (totalSlots = 99)

    观察: usedSlots 还没下降，可能有请求正在执行中
          等待这些请求完成

T=12ms: 一个请求完成，调用 AdmittedWorkDone()
    ├─ usedSlots: 100 → 99
    └─ CPULoad(runnable=355, procs=8, period=1ms)
        ├─ usedSlots=99 ≤ totalSlots=99 ✓
        └─ 决策: totalSlots = 99 - 1 = 98

T=13ms: 又一个请求完成
    ├─ usedSlots: 99 → 98
    └─ CPULoad(runnable=350, procs=8, period=1ms)
        └─ 决策: totalSlots = 98 - 1 = 97

T=14ms-T=30ms: 持续减少
    每 1ms 减少 1 个 slot (如果 usedSlots 允许)
    totalSlots: 97 → 96 → ... → 80

T=31ms: runnable 开始下降 (复杂查询逐渐完成)
    CPULoad(runnable=280, procs=8, period=1ms)
    ├─ 判断: 280 ≥ 256 → 仍过载
    ├─ usedSlots=80 ≤ totalSlots=80 ✓
    └─ 决策: totalSlots = 80 - 1 = 79

T=40ms: runnable 继续下降
    CPULoad(runnable=240, procs=8, period=1ms)
    ├─ 判断: 128 < 240 < 256 → 正常
    └─ 决策: 保持不变 (totalSlots = 75)

    观察: 进入稳定状态

T=50ms: 负载完全恢复
    runnable = 180
    totalSlots = 75
    usedSlots = 75

    决策: 保持不变 (在正常区间)

T=100ms: 开始恢复扩张
    runnable = 120 (进入空闲区间)

    CPULoad(runnable=120, procs=8, period=1ms)
    ├─ 判断: 120 ≤ 128 → 空闲
    ├─ usedSlots=75 ≥ totalSlots=75 ✓
    └─ 决策: totalSlots = 75 + 1 = 76

    观察: 开始逐步恢复到之前的水平
```

**数值变化总结**：

| 时间 (ms) | runnable | totalSlots | usedSlots | 决策      | 理由                 |
|----------|----------|-----------|-----------|----------|---------------------|
| 0        | 150      | 100       | 100       | 保持      | 正常                 |
| 10       | 350      | 100       | 100       | -1 → 99   | 过载 (350 ≥ 256)     |
| 11       | 360      | 99        | 100       | 保持      | used > total，等待   |
| 12       | 355      | 99        | 99        | -1 → 98   | 持续过载             |
| 15       | 345      | 96        | 96        | -1 → 95   | 持续过载             |
| 20       | 320      | 91        | 91        | -1 → 90   | 持续过载             |
| 30       | 280      | 81        | 81        | -1 → 80   | 持续过载             |
| 35       | 250      | 76        | 76        | -1 → 75   | 接近阈值             |
| 40       | 240      | 75        | 75        | 保持      | 进入正常区间         |
| 50       | 180      | 75        | 75        | 保持      | 正常                 |
| 100      | 120      | 75        | 75        | +1 → 76   | 空闲，开始恢复       |

**对 KVWork 请求准入的影响**：

```
T=0ms (稳定期):
    请求到达率: 100/ms
    准入率: 100/ms
    队列长度: 0
    延迟: P99 < 1ms

T=10ms (过载开始):
    请求到达率: 150/ms (增加 50%)
    准入率: 99/ms (减少 1%)
    队列长度: 0 → 51 (瞬间增长)
    延迟: P99 < 1ms → P99 ≈ 5ms

T=20ms (深度过载):
    请求到达率: 150/ms
    准入率: 91/ms
    队列长度: 51 + (150-91)*10 = 641
    延迟: P99 ≈ 50ms (队列等待时间)

    背压效果: 拒绝了 59/ms 的请求，保护 CPU 不被打爆

T=40ms (恢复中):
    请求到达率: 100/ms (恢复正常)
    准入率: 75/ms
    队列长度: 641 → 逐步消化
    延迟: P99 ≈ 30ms

T=100ms (完全恢复):
    队列长度: 0
    延迟: P99 < 1ms
    totalSlots 开始逐步增长
```

**关键观察**：
1. **快速响应**：10ms 检测到过载，立即减少 slots
2. **渐进调整**：30ms 内减少了 25 个 slots (100→75)
3. **保护机制**：usedSlots > totalSlots 时暂停减少
4. **自动恢复**：负载下降后自动增加 slots

**CPU 保护效果**：
```
假设没有 kvSlotAdjuster:
    T=10ms: 350 个 goroutine 可运行
    → CPU 调度器压力巨大
    → 每个 goroutine 获得的 CPU 时间片: 1/350
    → 上下文切换开销 >> 实际计算时间
    → 系统崩溃风险

有了 kvSlotAdjuster:
    T=20ms: totalSlots=91
    → 最多 91 个 KVWork goroutine 运行
    → 其余 59 个在队列等待
    → CPU 调度器压力可控
    → 每个 goroutine 获得充足的 CPU 时间
    → 系统稳定 ✓
```

---

## 六、设计取舍与权衡（Trade-offs）

### 6.1 与固定 Slot 数的对比

| 维度           | 固定 Slots              | 动态 Slots (kvSlotAdjuster) |
|---------------|------------------------|----------------------------|
| **实现复杂度**  | 简单 (一行配置)         | 复杂 (需要 CPU 采样、调整逻辑) |
| **适应性**      | 差 (无法应对负载变化)    | 强 (自动适应 CPU/IO bound)  |
| **资源利用率**  | 低-中 (保守配置时浪费)   | 高 (动态扩张到最优值)        |
| **过载保护**    | 中 (取决于固定值)       | 强 (实时响应 CPU 压力)       |
| **硬件异构性**  | 差 (需要每台机器单独配置) | 优 (自动适应核心数)          |
| **运维成本**    | 低 (设置后忘记)         | 中 (需要监控 metrics)       |
| **调试难度**    | 低 (行为可预测)         | 中-高 (动态行为需要时序分析) |

**示例对比**：
```
场景: 混合负载 (白天 CPU-bound, 夜间 IO-bound)

固定 slots = 50:
    白天 (CPU-bound):
        runnable = 300
        → 50 个 goroutine 运行, 250 个等待
        → CPU 可能过载 (50 可能太多)
        → 或资源浪费 (50 可能太少)

    夜间 (IO-bound):
        runnable = 20
        → 20 个 goroutine 运行, 30 个 slot 空闲
        → 资源浪费 (可以支持更多请求)

动态 slots (kvSlotAdjuster):
    白天 (CPU-bound):
        runnable = 300 → 过载
        → totalSlots 自动减少到 30
        → 30 个 goroutine 运行, 270 个等待
        → CPU 保护 ✓

    夜间 (IO-bound):
        runnable = 20 → 空闲
        usedSlots = totalSlots (IO 阻塞)
        → totalSlots 自动增长到 500+
        → 500+ goroutine 可以并发 (大部分在等 IO)
        → 高吞吐 ✓
```

### 6.2 与定时后台调整的对比

| 维度           | 定时后台调整                 | 惰性调整 (现状)            |
|---------------|----------------------------|---------------------------|
| **调整延迟**   | 中-高 (取决于定时间隔)       | 极低 (采样即调整, <1ms)    |
| **数据新鲜度** | 中 (可能读到旧的 runnable)   | 高 (调整基于最新采样)      |
| **并发复杂度** | 高 (需要同步共享状态)        | 低 (在锁保护下调用)        |
| **资源开销**   | 中 (额外 goroutine + 定时器) | 低 (无额外线程)            |
| **独立性**     | 高 (不依赖外部调用)          | 低 (依赖 goschedstats)     |

**定时调整的问题**：
```go
// 假设的定时调整实现
func (kvsa *kvSlotAdjuster) adjustLoop() {
    ticker := time.NewTicker(10 * time.Millisecond)
    defer ticker.Stop()

    for range ticker.C {
        // 读取最新的 runnable (需要锁)
        kvsa.mu.Lock()
        runnable := kvsa.latestRunnable
        procs := kvsa.latestProcs
        kvsa.mu.Unlock()

        // 问题 1: 数据可能是 10ms 前的
        // 问题 2: 需要额外的锁 (kvsa.mu)
        // 问题 3: 与 GrantCoordinator.mu 的锁顺序？

        // 调整逻辑 (需要另一个锁)
        coord.mu.Lock()
        // ... 调整 slots
        coord.mu.Unlock()

        // 问题 4: 两个锁的交互 (死锁风险)
    }
}
```

**惰性调整的优势**：
```go
// 实际的惰性调整
func (coord *GrantCoordinator) CPULoad(runnable, procs, period) {
    coord.mu.Lock()  // ← 单一锁
    defer coord.mu.Unlock()

    // 优势 1: 数据是最新的 (刚采样)
    coord.mu.cpuLoadListener.CPULoad(runnable, procs, period)

    // 优势 2: 在同一个锁下完成所有操作
    // 优势 3: 无需额外 goroutine
    // 优势 4: 调用栈简单，易于调试

    // 可能触发授权 (仍在同一个锁下)
    coord.tryGrantLocked()
}
```

### 6.3 全局集中控制 vs 按 WorkKind 独立控制

**CockroachDB 的选择**：为每个 WorkKind 独立控制

```
架构:
    KVWork:
        └─► slotGranter + kvSlotAdjuster (动态)

    SQLKVResponseWork:
        └─► tokenGranter + 固定 burst tokens

    SQLSQLResponseWork:
        └─► tokenGranter + 固定 burst tokens
```

**对比方案：全局集中控制**：
```
假设: 所有 WorkKind 共享一个 slot pool

优点:
    ├─ 更简单的实现
    └─ 更灵活的资源分配 (work kind 之间可以借用)

缺点:
    ├─ 优先级控制困难
    │   └─ 低优先级 work 可能耗尽所有 slots
    │       └─ 高优先级 work 无法获得资源
    │
    ├─ 公平性问题
    │   └─ 某个 work kind 的突发流量影响其他
    │
    └─ 调优困难
        └─ 一个参数影响所有 work kind
```

**独立控制的优势**：
```
KVWork (最高优先级):
    ├─ 专用 slots (不与其他 work kind 共享)
    ├─ 动态调整 (kvSlotAdjuster)
    └─ 优先保障 (KV 层是基础)

SQLKVResponseWork:
    ├─ token-based (更灵活)
    ├─ 固定 burst limit (可预测)
    └─ CPU 过载时自动减少 (通过 kvSlotAdjuster)

隔离性:
    KVWork 的调整不影响 SQL work
    SQL work 的突发流量不抢占 KV slots
```

**实例**：
```
场景: KVWork 过载 + SQL work 正常

全局控制:
    totalSlots = 100 (共享)
    KVWork 占用: 60
    SQL work 占用: 40
    → KVWork 过载时减少 totalSlots
    → SQL work 也受影响 (被迫减少)
    → 连带伤害 ✗

独立控制:
    KVWork slots: 60 → 减少到 40
    SQL tokens: 不变
    → KVWork 被保护
    → SQL work 不受影响 ✓
```

### 6.4 锁竞争与开销

**kvSlotAdjuster 的锁策略**：
```
所有操作都在 GrantCoordinator.mu 保护下:

CPULoad() 调用链:
    coord.mu.Lock()  ← 获取锁
        ├─► kvsa.CPULoad(...)
        │   └─► granter.setTotalSlotsLocked(...)
        │       └─► 修改 totalSlots
        │
        ├─► 刷新 tokens
        └─► tryGrantLocked()
            └─► 可能授权请求
    coord.mu.Unlock()  ← 释放锁
```

**锁竞争分析**：
```
竞争来源:
1. CPULoad() (每 1ms)
2. tryGet() (每个请求)
3. returnGrant() (请求完成时)
4. continueGrantChain() (Grant Chain 继续时)

频率估算:
    CPULoad: 1000 次/秒
    tryGet + returnGrant: 假设 10,000 req/s → 20,000 次/秒

    总竞争: ~21,000 次/秒 (每个操作需要锁)
```

**开销评估**：
```
单次 CPULoad 的成本:
    ├─ 获取/释放锁: ~100ns
    ├─ 读取 usedSlots: ~10ns
    ├─ 比较和分支: ~20ns
    ├─ 可能调用 setTotalSlotsLocked: ~50ns
    └─ 更新 metrics: ~30ns

    总计: ~210ns per call

    每秒开销: 1000 calls/s * 210ns = 210μs
              = 0.021% CPU (单核)
              = 0.0026% CPU (8核)

结论: 开销极小，可以忽略
```

**与请求处理时间对比**：
```
典型 KVWork 请求:
    ├─ 准入控制开销: ~1μs (包括 CPULoad)
    ├─ Raft 提议: ~1-10ms
    └─ 总延迟: ~1-10ms

    准入控制占比: 1μs / 1ms = 0.1%

结论: kvSlotAdjuster 的开销在噪声范围内
```

### 6.5 惰性更新的成本

**好处**：
1. **零延迟**：采样 → 决策 → 调整 在同一个 call stack
2. **数据一致性**：调整基于最新数据
3. **简单性**：无需维护额外状态

**代价**：
1. **依赖外部调用**：
   ```
   如果 goschedstats 停止调用 CPULoad():
       → kvSlotAdjuster 停止调整
       → totalSlots 保持在最后的值
       → 系统退化为固定 slots
   ```

2. **调用路径耦合**：
   ```
   CPULoad 必须在 GrantCoordinator.mu 锁下调用:
       → 增加了 goschedstats 的限制
       → 如果 CPULoad 很慢，会阻塞授权
   ```

3. **测试复杂度**：
   ```
   需要模拟 CPULoad 调用:
       → 单元测试需要手动调用 CPULoad
       → 难以测试异步行为
   ```

**权衡总结**：
```
CockroachDB 的选择: 惰性更新

原因:
    ├─ 性能优先 (零延迟 > 独立性)
    ├─ goschedstats 是可靠的 (每 1ms 必然调用)
    ├─ 锁已经存在 (GrantCoordinator.mu 本来就需要)
    └─ 测试成本可接受 (有 datadriven 测试框架)

如果是其他系统:
    可能选择定时调整 (如果没有高频 CPU 采样)
```

### 6.6 公平性与吞吐量的取舍

**kvSlotAdjuster 对公平性的影响**：
```
场景: 两个租户竞争 KVWork slots

租户 A: 高优先级，大量请求
租户 B: 正常优先级，少量请求

totalSlots = 50:
    租户 A: 占用 45 slots
    租户 B: 占用 5 slots

    → 租户 B 被"挤压"
    → 但 totalSlots 是根据 CPU 负载调整的
    → 不是根据租户公平性调整的

问题: kvSlotAdjuster 不考虑租户公平性
解决: 由 WorkQueue 的 tenantHeap 负责公平性
```

**分层设计**：
```
┌─────────────────────────────────────────────────────────────┐
│ kvSlotAdjuster: 负责总资源量 (totalSlots)                    │
│ - 目标: CPU 不过载                                            │
│ - 机制: 监听 runnable goroutines                             │
│ - 输出: totalSlots (全局配额)                                 │
└─────────────────────────────────────────────────────────────┘
    ↓ (决定资源总量)
┌─────────────────────────────────────────────────────────────┐
│ WorkQueue + tenantHeap: 负责资源分配 (公平性)                │
│ - 目标: 租户间公平                                            │
│ - 机制: used/weight 比例的最小堆                              │
│ - 输出: 哪个租户的请求被授权                                  │
└─────────────────────────────────────────────────────────────┘
```

**示例**：
```
totalSlots = 100 (由 kvSlotAdjuster 决定)

租户 A: weight=10, used=500
租户 B: weight=1, used=60

tenantHeap 排序:
    租户 A: used/weight = 500/10 = 50
    租户 B: used/weight = 60/1 = 60

    → 租户 A 在堆顶 (ratio 更小)
    → 下一个 slot 授权给租户 A
    → 租户 A: used=501 → ratio=50.1
    → 可能仍在堆顶，继续授权

    → 直到租户 A 的 ratio 超过租户 B
    → 切换到租户 B

结果:
    虽然 totalSlots 不考虑公平性
    但 slots 的分配考虑公平性
    → 整体系统兼顾效率和公平 ✓
```

---

## 七、总结与心智模型

### 7.1 核心思想总结

**kvSlotAdjuster 实现了一个基于 CPU 调度压力的自适应准入控制机制**，其核心思想是：

1. **直接感知调度压力**：使用 `runnable goroutines / procs` 作为 CPU 压力的直接指标，比 CPU 利用率更准确、更及时。

2. **渐进式调整（AIAD）**：每次调整 ±1 slot，频率最高 1ms，既能快速响应（每秒最多调整 1000 个 slots），又能避免剧烈振荡。

3. **三阈值设计（Hysteresis）**：过载阈值（256）、正常区间（128-256）、空闲阈值（128）三个区间，避免频繁切换状态。

4. **自动适应负载类型**：
   - **CPU-bound**：slots 收缩到数十（受 CPU 调度压力限制）
   - **IO-bound**：slots 扩张到数千（goroutine 阻塞在 IO，不占用 CPU）

5. **惰性调整架构**：由 goschedstats 驱动，采样即调整，零延迟，无需额外线程。

### 7.2 简洁心智模型

**如果只记住一件事，那就是：**

```
kvSlotAdjuster 是一个"CPU 压力敏感的恒温器"

类比恒温器:
    目标温度: threshold * procs = 256 (8 核)
    当前温度: runnable goroutines

    太热 (runnable ≥ 256):
        → 关小"阀门" (totalSlots--)
        → 减少 goroutine 进入系统
        → 降温 (runnable 下降)

    太冷 (runnable ≤ 128):
        → 开大"阀门" (totalSlots++)
        → 更多 goroutine 进入系统
        → 升温 (runnable 上升)

    适中 (128 < runnable < 256):
        → 保持不变
        → 稳定运行

关键差异:
    恒温器: 控制温度
    kvSlotAdjuster: 控制 CPU 调度压力
```

### 7.3 简化伪代码

```python
class kvSlotAdjuster:
    def __init__(self):
        self.threshold = 32  # 每核可运行 goroutine 阈值
        self.minSlots = 1
        self.maxSlots = 100000

    def CPULoad(self, runnable, procs, samplePeriod):
        """每 1ms 被调用一次"""

        # 计算阈值
        overload = self.threshold * procs     # 256 (8核)
        underload = overload / 2              # 128

        # 读取当前状态
        used = self.granter.usedSlots
        total = self.granter.totalSlots

        # 决策逻辑
        if runnable >= overload:
            # CPU 过载 → 减少 slots
            if used > 0 and total > self.minSlots and used <= total:
                self.granter.totalSlots = total - 1

        elif runnable <= underload:
            # CPU 空闲 → 增加 slots
            if used >= total and total < self.maxSlots:
                self.granter.totalSlots = total + 1

        # else: 正常负载 → 保持不变

    def isOverloaded(self):
        """判断 CPU 是否过载"""
        return (self.granter.usedSlots >= self.granter.totalSlots and
                not self.granter.skipSlotEnforcement)
```

### 7.4 关键设计决策回顾

| 设计选择               | 原因                                      | 代价                    |
|-----------------------|-------------------------------------------|------------------------|
| **runnable/procs**     | 直接反映调度压力，准确且快速               | 依赖 goschedstats       |
| **AIAD (±1)**          | 稳定，避免振荡                            | 响应速度受限 (最快 1ms) |
| **三阈值**             | 避免频繁切换，稳定在最优点附近             | 参数调优复杂            |
| **惰性调整**           | 零延迟，数据一致性                        | 依赖外部调用            |
| **独立 WorkKind**      | 隔离性，优先级保障                        | 实现复杂                |
| **usedSlots≤total 检查** | 避免过度反应                           | 响应可能延迟 1-2ms      |

### 7.5 调优建议

**可调参数**：
```sql
-- 调整过载阈值（每核允许的可运行 goroutine 数）
SET CLUSTER SETTING admission.kv_slot_adjuster.overload_threshold = 16;
-- 默认: 32
-- 更小 → 更保守（更早减少 slots）
-- 更大 → 更激进（允许更高的调度压力）

-- 调整 slot 边界
-- (需要代码修改，通常不改)
minCPUSlots = 1
maxCPUSlots = 100000
```

**监控指标**：
```
关键 Metrics:
1. admission.granter.total_slots{work=kv}
   → 观察 totalSlots 的变化趋势

2. admission.granter.used_slots{work=kv}
   → 观察 slot 利用率

3. admission.granter.slots_exhausted_duration{work=kv}
   → 观察过载程度（累计耗尽时间）

4. admission.scheduler.latency_us.p99
   → 观察调度延迟（间接反映 CPU 压力）

告警阈值示例:
    slots_exhausted_duration > 10% → 容量不足
    total_slots < 10 → 系统严重过载
    total_slots > 10000 → 可能是 IO-bound (正常)
```

**常见问题诊断**：
```
问题 1: totalSlots 持续下降到 minCPUSlots
    原因: CPU 严重过载
    解决:
        ├─ 检查是否有慢查询
        ├─ 检查是否有死循环 goroutine
        └─ 考虑增加节点

问题 2: totalSlots 增长到数万
    原因: IO-bound 负载
    判断: runnable < 128 但 usedSlots = totalSlots
    解决: 正常现象，不需要干预

问题 3: totalSlots 频繁波动
    原因: runnable 在阈值附近振荡
    解决:
        └─ 调整 threshold (增大 deadband)

问题 4: slotsExhaustedDuration 持续增长
    原因: 容量不足或过载
    解决:
        ├─ 增加节点
        └─ 优化查询
```

---

**全文完**

这套机制的优雅之处在于：它不需要人工调参，不需要预测负载模式，仅通过一个简单的反馈循环（runnable → 决策 → totalSlots → usedSlots → runnable）就能自动适应从 CPU-bound 到 IO-bound 的各种负载，在过载保护和资源利用率之间找到动态平衡。
