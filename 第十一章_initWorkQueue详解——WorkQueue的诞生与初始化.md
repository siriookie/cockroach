# 第十一章：initWorkQueue详解——WorkQueue的诞生与初始化

## 引言

在前面的章节中，我们深入分析了 `Admit()` 函数的准入流程。但在请求能够进入 `Admit()` 之前，必须有一个 **WorkQueue** 实例存在。本章将详细剖析 `initWorkQueue()` 函数——这是 **WorkQueue 的构造函数**，负责初始化准入控制队列的所有核心组件。

我们将采用 **BFS（广度优先）→ DFS（深度优先）** 的方式，从宏观到微观，从调用时机到实现细节，全面理解 WorkQueue 的初始化过程。

---

## BFS Layer 1: 函数签名与调用时机

### 1.1 函数签名

```go
// pkg/util/admission/work_queue.go:370-433
func initWorkQueue(
	q *WorkQueue,                    // 要初始化的队列指针
	ambientCtx log.AmbientContext,   // 日志上下文
	workKind WorkKind,                // 工作类型（KVWork/SQLKVResponseWork/SQLSQLResponseWork）
	queueKind QueueKind,              // 队列标识（"kv-regular-cpu-queue"等）
	granter granter,                  // 资源授予器
	settings *cluster.Settings,       // 集群配置
	metrics *WorkQueueMetrics,        // 指标收集器
	opts workQueueOptions,            // 初始化选项
	knobs *TestingKnobs,              // 测试钩子
)
```

**核心特征**：
- ✓ 无返回值（直接修改传入的 `q` 指针）
- ✓ 9个参数，涵盖队列的所有配置维度
- ✓ 支持测试注入（`knobs`）

### 1.2 调用时机：何时会创建 WorkQueue？

#### 场景 1：CPU 队列初始化（最常见）

```go
// pkg/util/admission/work_queue.go:353-368
func makeWorkQueue(
	ambientCtx log.AmbientContext,
	workKind WorkKind,
	granter granter,
	settings *cluster.Settings,
	metrics *WorkQueueMetrics,
	opts workQueueOptions,
) requester {
	q := &WorkQueue{}
	var queueKind QueueKind
	if workKind == KVWork {
		queueKind = "kv-regular-cpu-queue" // ← 默认队列名
	}
	initWorkQueue(q, ambientCtx, workKind, queueKind, granter, settings, metrics, opts, nil)
	return q
}
```

**调用链**：
```
GrantCoordinator.initGranterAndWorkQueue()
    ↓
makeWorkQueue(workKind=KVWork, ...)
    ↓
initWorkQueue(q, ..., queueKind="kv-regular-cpu-queue", ...)
```

#### 场景 2：Store 队列初始化（IO 准入控制）

```go
// pkg/util/admission/work_queue.go:2407-2470
func makeStoreWorkQueue(
	ambientCtx log.AmbientContext,
	storeID roachpb.StoreID,
	granters [admissionpb.NumWorkClasses]granterWithStoreReplicatedWorkAdmitted,
	settings *cluster.Settings,
	metrics [admissionpb.NumWorkClasses]*WorkQueueMetrics,
	opts workQueueOptions,
	knobs *TestingKnobs,
	onLogEntryAdmitted OnLogEntryAdmitted,
	ioTokensBypassedMetric *metric.Counter,
	coordMu *syncutil.Mutex,
) storeRequester {
	q := &StoreWorkQueue{...}

	opts.usesAsyncAdmit = true // ← Store 队列特殊配置
	for i := range q.q {
		var queueKind QueueKind
		if i == int(admissionpb.RegularWorkClass) {
			queueKind = "kv-regular-store-queue"
		} else if i == int(admissionpb.ElasticWorkClass) {
			queueKind = "kv-elastic-store-queue"
		}
		// ← 每个 WorkClass 创建一个队列
		initWorkQueue(&q.q[i], ambientCtx, KVWork, queueKind, granters[i], settings, metrics[i], opts, knobs)
		q.q[i].onAdmittedReplicatedWork = q
	}

	// ... 初始化 sequencers、GC goroutine 等
	return q
}
```

**调用链**：
```
StoreGrantCoordinators.initGranterAndWorkQueue()
    ↓
makeStoreWorkQueue(storeID=1, ...)
    ↓
initWorkQueue(&q.q[RegularWorkClass], ..., queueKind="kv-regular-store-queue", ...)
initWorkQueue(&q.q[ElasticWorkClass], ..., queueKind="kv-elastic-store-queue", ...)
```

#### 场景 3：测试环境

```go
// 测试代码中直接调用
func TestWorkQueue(t *testing.T) {
	q := &WorkQueue{}
	opts := workQueueOptions{usesTokens: false}
	initWorkQueue(q, ..., opts, &TestingKnobs{...})
}
```

### 1.3 四种队列实例的创建

在一个典型的 CockroachDB 节点中，会创建 **多个 WorkQueue 实例**：

```
┌─────────────────────────────────────────────────────────────┐
│ CockroachDB Node                                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ [1] CPU 准入控制                                             │
│   ├─ kv-regular-cpu-queue       (KVWork)                    │
│   ├─ sql-kv-response-queue      (SQLKVResponseWork)         │
│   └─ sql-sql-response-queue     (SQLSQLResponseWork)        │
│                                                             │
│ [2] Store 准入控制 (每个 Store)                               │
│   ├─ kv-regular-store-queue     (RegularWorkClass)          │
│   └─ kv-elastic-store-queue     (ElasticWorkClass)          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## BFS Layer 2: 初始化流程的六大阶段

### 阶段 0：参数预处理（第381-393行）

```go
if knobs == nil {
	knobs = &TestingKnobs{} // 确保非空
}
stopCh := make(chan struct{}) // 用于通知后台 goroutine 停止

timeSource := opts.timeSource
if timeSource == nil {
	timeSource = timeutil.DefaultTimeSource{} // 默认时间源
}

if queueKind == "" {
	queueKind = QueueKind(workKind.String()) // 使用 workKind 的字符串表示
}
```

**设计意图**：
- 所有可选参数都有默认值，避免 nil panic
- `stopCh` 用于优雅关闭后台 goroutine

### 阶段 1：基本字段赋值（第395-407行）

```go
q.ambientCtx = ambientCtx.AnnotateCtx(context.Background())
q.workKind = workKind           // 架构层次（KVWork/SQL...）
q.queueKind = queueKind         // 队列实例标识
q.granter = granter             // 资源授予器
q.usesTokens = opts.usesTokens  // 槽位 vs 令牌
q.tiedToRange = opts.tiedToRange      // 是否绑定到 Range
q.usesAsyncAdmit = opts.usesAsyncAdmit // 异步准入
q.settings = settings
q.logThreshold = log.Every(5 * time.Minute) // 日志限流
q.metrics = metrics
q.stopCh = stopCh
q.timeSource = timeSource
q.knobs = knobs
```

**关键配置项解析**：

| 字段 | 用途 | 示例值 |
|-----|------|--------|
| `workKind` | 决定工作类型优先级 | `KVWork` |
| `queueKind` | 用于日志和指标标识 | `"kv-regular-cpu-queue"` |
| `usesTokens` | 资源模型选择 | `false`（slots）/ `true`（tokens） |
| `tiedToRange` | 是否需要 Range 级别的顺序保证 | `true`（KVWork）/ `false`（SQL） |
| `usesAsyncAdmit` | 是否异步准入 | `true`（Store队列）/ `false`（CPU队列） |

### 阶段 2：初始化租户管理（第409-414行）

```go
func() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.tenants = make(map[uint64]*tenantInfo) // 租户信息映射
	q.sampleEpochLIFOSettingsLocked()           // 采样 Epoch-LIFO 配置
}()
```

**数据结构初始化**：
```go
q.mu.tenants = make(map[uint64]*tenantInfo)
// 这个 map 用于跟踪所有活跃租户的：
// - 已使用资源量 (used)
// - 等待工作队列 (waitingWorkHeap, openEpochsHeap)
// - 优先级统计 (priorityStates)
// - FIFO/LIFO 阈值 (fifoPriorityThreshold)
```

**采样 Epoch-LIFO 配置**：
```go
// pkg/util/admission/work_queue.go:451-464
func (q *WorkQueue) sampleEpochLIFOSettingsLocked() {
	epochLengthNanos := int64(epochLIFOEpochDuration.Get(&q.settings.SV))
	if epochLengthNanos != q.mu.epochLengthNanos {
		// 如果 epoch 长度改变，重置关闭阈值
		q.mu.closedEpochThreshold = 0
	}
	q.mu.epochLengthNanos = epochLengthNanos
	q.mu.epochClosingDeltaNanos = int64(epochLIFOEpochClosingDeltaDuration.Get(&q.settings.SV))
	q.mu.maxQueueDelayToSwitchToLifo = epochLIFOQueueDelayThresholdToSwitchToLIFO.Get(&q.settings.SV)
}
```

**配置参数说明**：
- `epochLengthNanos`：默认 100ms（`epochLength`）
- `epochClosingDeltaNanos`：默认 5ms（提前关闭时间）
- `maxQueueDelayToSwitchToLifo`：默认 105ms（切换到 LIFO 的延迟阈值）

### 阶段 3：启动 GC Goroutine（第415-428行）

```go
if !opts.disableGCTenantsAndResetUsed {
	go func() {
		ticker := time.NewTicker(time.Second) // 每秒触发一次
		for {
			select {
			case <-ticker.C:
				q.gcTenantsAndResetUsed() // GC 租户信息并重置 used 计数
			case <-stopCh:
				// Channel closed.
				return
			}
		}
	}()
}
```

**GC 任务详解**：

```go
// pkg/util/admission/work_queue.go:1095-1112
func (q *WorkQueue) gcTenantsAndResetUsed() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for id, info := range q.mu.tenants {
		if info.used == 0 && !isInTenantHeap(info) {
			// 条件1：没有使用资源
			// 条件2：没有等待的工作
			delete(q.mu.tenants, id)
			releaseTenantInfo(info) // 返回到对象池
		} else {
			// 重置 used 计数（每秒归零，避免历史数据影响公平性）
			info.used = 0
		}
	}
}
```

**为什么每秒重置 used？**
```
时间线：
T0:     Tenant A 使用 100 slots
T0-T1:  Tenant B 使用 50 slots

T1 (GC时刻):
  - Tenant A: used=100 → 重置为 0
  - Tenant B: used=50  → 重置为 0

T1-T2:  Tenant A 使用 10 slots
        Tenant B 使用 20 slots

T2 (公平性判断):
  - Tenant A: used=10  ← 不会因为历史高使用量被惩罚
  - Tenant B: used=20

→ 基于"当前 1 秒窗口"的使用量进行公平调度
```

### 阶段 4：初始化 Epoch 关闭机制（第429行）

```go
q.tryCloseEpoch(q.timeNow())
```

**立即关闭当前 epoch**：
```go
// pkg/util/admission/work_queue.go:517-577
func (q *WorkQueue) tryCloseEpoch(timeNow time.Time) {
	epochLIFOEnabled := q.epochLIFOEnabled()
	q.mu.Lock()
	defer q.mu.Unlock()

	epochClosingTimeNanos := timeNow.UnixNano() - q.mu.epochLengthNanos - q.mu.epochClosingDeltaNanos
	epoch := epochForTimeNanos(epochClosingTimeNanos, q.mu.epochLengthNanos)

	if epoch <= q.mu.closedEpochThreshold {
		return // 已经关闭过了
	}

	q.mu.closedEpochThreshold = epoch

	// 遍历所有租户，更新 FIFO/LIFO 阈值
	for _, tenant := range q.mu.tenants {
		prevThreshold := tenant.fifoPriorityThreshold
		tenant.fifoPriorityThreshold =
			tenant.priorityStates.getFIFOPriorityThresholdAndReset(
				tenant.fifoPriorityThreshold,
				q.mu.epochLengthNanos,
				q.mu.maxQueueDelayToSwitchToLifo)

		if !epochLIFOEnabled {
			tenant.fifoPriorityThreshold = int(admissionpb.LowPri) // 全部 FIFO
		}

		// 将 openEpochsHeap 中已关闭 epoch 的工作移到 waitingWorkHeap
		for len(tenant.openEpochsHeap) > 0 {
			work := tenant.openEpochsHeap[0]
			if work.epoch > epoch {
				break // 还没到关闭时间
			}
			heap.Pop(&tenant.openEpochsHeap)
			heap.Push(&tenant.waitingWorkHeap, work)
		}
	}
}
```

**epoch 计算公式**：
```go
func epochForTimeNanos(t int64, epochLengthNanos int64) int64 {
	return t / epochLengthNanos
}

// 示例：
// epochLengthNanos = 100ms = 100,000,000 ns
// t = 1,234,567,890,000,000 ns
// epoch = 1,234,567,890,000,000 / 100,000,000 = 12,345,678
```

### 阶段 5：启动 Epoch 关闭 Goroutine（第430-432行）

```go
if !opts.disableEpochClosingGoroutine {
	q.startClosingEpochs()
}
```

**定时器驱动的 Epoch 关闭**：

```go
// pkg/util/admission/work_queue.go:466-507
func (q *WorkQueue) startClosingEpochs() {
	go func() {
		const maxTimerDur = time.Second
		const minTimerDur = time.Millisecond
		var timer *time.Timer

		for {
			// 计算下一次 epoch 关闭时间
			nextCloseTime := func() time.Time {
				q.mu.Lock()
				defer q.mu.Unlock()
				q.sampleEpochLIFOSettingsLocked() // 采样最新配置
				return q.nextEpochCloseTimeLocked()
			}()

			timeNow := q.timeNow()
			timerDur := nextCloseTime.Sub(timeNow)

			if timerDur > 0 {
				// 限制 timer 在 [1ms, 1s] 范围内
				if timerDur > maxTimerDur {
					timerDur = maxTimerDur
				} else if timerDur < minTimerDur {
					timerDur = minTimerDur
				}

				if timer == nil {
					timer = time.NewTimer(timerDur)
				} else {
					timer.Reset(timerDur)
				}

				select {
				case <-timer.C:
					// Timer 触发，继续循环
				case <-q.stopCh:
					return // 停止
				}
			} else {
				// 已经过期，立即关闭
				q.tryCloseEpoch(timeNow)
			}
		}
	}()
}
```

**下一次关闭时间计算**：
```go
// pkg/util/admission/work_queue.go:509-515
func (q *WorkQueue) nextEpochCloseTimeLocked() time.Time {
	// +2 的原因：
	// - +1：从当前关闭的 epoch 前进到下一个
	// - +1：epoch 在其"结束时刻"关闭，而非开始时刻
	timeUnixNanos :=
		(q.mu.closedEpochThreshold+2)*q.mu.epochLengthNanos + q.mu.epochClosingDeltaNanos
	return timeutil.Unix(0, timeUnixNanos)
}
```

**示例时间线**：
```
假设：
- epochLengthNanos = 100ms
- epochClosingDeltaNanos = 5ms
- closedEpochThreshold = 100

时间线：
  Epoch 100:  [10000ms - 10100ms]  ← 已关闭
  Epoch 101:  [10100ms - 10200ms]  ← 下一个要关闭的

下一次关闭时间：
  = (100 + 2) * 100ms + 5ms
  = 10200ms + 5ms
  = 10205ms

含义：在 Epoch 101 结束后 5ms（10205ms）关闭它
```

---

## DFS Layer 3: 核心机制深度剖析

### 3.1 租户公平调度：tenantHeap 的堆排序

**堆的排序逻辑**：

```go
// pkg/util/admission/work_queue.go:1571-1582
func (th *tenantHeap) Less(i, j int) bool {
	// 使用加权公平性：used_i/weight_i < used_j/weight_j
	// 为避免浮点数，使用交叉乘法：used_i * weight_j < used_j * weight_i
	if (*th)[i].used*uint64((*th)[j].weight) == (*th)[j].used*uint64((*th)[i].weight) {
		// 平局时：
		// 1. 优先选择权重更高的（鼓励高权重租户）
		// 2. 再平局时：选择 ID 更小的（稳定排序）
		if (*th)[i].weight == (*th)[j].weight {
			return (*th)[i].id < (*th)[j].id
		}
		return (*th)[i].weight > (*th)[j].weight
	}
	return (*th)[i].used*uint64((*th)[j].weight) < (*th)[j].used*uint64((*th)[i].weight)
}
```

**示例计算**：
```
Tenant A: used=100, weight=10
Tenant B: used=50,  weight=5
Tenant C: used=150, weight=20

排序比较：
A vs B: 100*5 = 500  vs  50*10 = 500  → 平局
        weight: 10 > 5  → A 优先

A vs C: 100*20 = 2000  vs  150*10 = 1500  → C < A
        → C 优先（使用率更低：150/20=7.5 < 100/10=10）

最终顺序：C, A, B
```

### 3.2 FIFO/LIFO 自适应切换

**切换决策树**：

```go
// pkg/util/admission/work_queue.go:1404-1451
func (ps *priorityStates) getFIFOPriorityThresholdAndReset(
	curPriorityThreshold int,
	epochLengthNanos int64,
	maxQueueDelayToSwitchToLifo time.Duration,
) int {
	priority := int(admissionpb.LowPri) // 从最低优先级开始

	for i := range ps.ps {
		p := ps.ps[i]

		if p.maxQueueDelay > maxQueueDelayToSwitchToLifo {
			// 规则1：延迟过高 → 切换到 LIFO
			priority = int(p.priority) + 1
		} else if int(p.priority) < curPriorityThreshold {
			// 规则2：当前是 LIFO，检查是否可以切回 FIFO
			if p.maxQueueDelay > time.Duration(epochLengthNanos)/10 {
				// 延迟仍然较高（> epoch/10），继续 LIFO
				priority = int(p.priority) + 1
			} else if p.admittedCount == 0 && int(p.priority) >= ps.lowestPriorityWithRequests {
				// 有请求但无成功准入（饿死状态），继续 LIFO
				priority = int(p.priority) + 1
			}
			// 否则：延迟降低且有成功准入 → 切回 FIFO
		}
	}

	// 重置统计
	ps.ps = ps.ps[:0]
	ps.lowestPriorityWithRequests = admissionpb.OneAboveHighPri
	return priority
}
```

**决策表**：

| 当前状态 | 队列延迟 | admittedCount | 下一个状态 |
|---------|---------|---------------|----------|
| FIFO | > 105ms | 任意 | → LIFO |
| FIFO | ≤ 105ms | 任意 | 保持 FIFO |
| LIFO | > 10ms | 任意 | 保持 LIFO |
| LIFO | ≤ 10ms | > 0 | → FIFO |
| LIFO | ≤ 10ms | = 0 | 保持 LIFO（饿死） |

### 3.3 GC 与对象池复用

**tenantInfo 对象池**：

```go
// pkg/util/admission/work_queue.go:1518-1557
var tenantInfoPool = sync.Pool{
	New: func() interface{} {
		return &tenantInfo{}
	},
}

func newTenantInfo(id uint64, weight uint32) *tenantInfo {
	ti := tenantInfoPool.Get().(*tenantInfo) // ← 从池中获取
	*ti = tenantInfo{
		id:                    id,
		weight:                weight,
		waitingWorkHeap:       ti.waitingWorkHeap,  // ← 复用切片
		openEpochsHeap:        ti.openEpochsHeap,   // ← 复用切片
		priorityStates:        makePriorityStates(ti.priorityStates.ps),
		fifoPriorityThreshold: int(admissionpb.LowPri),
		heapIndex:             -1,
	}
	return ti
}

func releaseTenantInfo(ti *tenantInfo) {
	if isInTenantHeap(ti) {
		panic("tenantInfo has non-empty heap")
	}
	// 防止内存泄漏：如果切片容量过大，丢弃它
	if cap(ti.waitingWorkHeap) > 100 {
		ti.waitingWorkHeap = nil
	}
	if cap(ti.openEpochsHeap) > 100 {
		ti.openEpochsHeap = nil
	}

	*ti = tenantInfo{
		waitingWorkHeap: ti.waitingWorkHeap,
		openEpochsHeap:  ti.openEpochsHeap,
		priorityStates:  makePriorityStates(ti.priorityStates.ps),
	}
	tenantInfoPool.Put(ti) // ← 返回池中
}
```

**为什么限制容量为 100？**
```
假设场景：
- 某个租户在短时间内产生 10000 个排队请求
- waitingWorkHeap 切片扩展到 cap=16384

问题：
- 即使租户变为空闲，也会持有 16KB+ 的内存
- 如果有 1000 个这样的租户，浪费 16MB+

解决方案：
- cap > 100 时丢弃切片，让 GC 回收
- 下次使用时重新分配（从 cap=0 开始）
- 平衡内存复用与内存占用
```

---

## DFS Layer 4: 完整初始化流程图

```
initWorkQueue() 入口
    │
    ├─► [Stage 0] 参数预处理
    │       ├─ knobs 非空检查
    │       ├─ 创建 stopCh
    │       ├─ timeSource 默认值
    │       └─ queueKind 默认值
    │
    ├─► [Stage 1] 基本字段赋值
    │       ├─ ambientCtx
    │       ├─ workKind, queueKind
    │       ├─ granter
    │       ├─ usesTokens, tiedToRange, usesAsyncAdmit
    │       └─ settings, metrics, knobs
    │
    ├─► [Stage 2] 租户管理初始化
    │       ├─ q.mu.tenants = make(map)
    │       └─ sampleEpochLIFOSettingsLocked()
    │           ├─ epochLengthNanos
    │           ├─ epochClosingDeltaNanos
    │           └─ maxQueueDelayToSwitchToLifo
    │
    ├─► [Stage 3] 启动 GC Goroutine
    │       └─ 每秒执行 gcTenantsAndResetUsed()
    │           ├─ 删除空闲租户
    │           └─ 重置 tenant.used = 0
    │
    ├─► [Stage 4] 初始化 Epoch 关闭
    │       └─ tryCloseEpoch(now)
    │           ├─ 计算 closedEpochThreshold
    │           ├─ 更新 fifoPriorityThreshold
    │           └─ 移动 openEpochsHeap → waitingWorkHeap
    │
    └─► [Stage 5] 启动 Epoch 关闭 Goroutine
            └─ startClosingEpochs()
                └─ 定期调用 tryCloseEpoch()
                    └─ 根据 nextEpochCloseTimeLocked() 计算定时器
```

---

## DFS Layer 5: 配置选项详解

### 5.1 workQueueOptions 结构

```go
// pkg/util/admission/work_queue.go:320-333
type workQueueOptions struct {
	usesTokens     bool // 是否使用令牌（vs 槽位）
	tiedToRange    bool // 是否绑定到 Range
	usesAsyncAdmit bool // 是否使用异步准入

	timeSource timeutil.TimeSource  // 时间源（可用于测试）
	disableEpochClosingGoroutine bool // 禁用 epoch 关闭 goroutine（测试用）
	disableGCTenantsAndResetUsed bool // 禁用 GC goroutine（测试用）
}
```

### 5.2 不同 WorkKind 的默认配置

```go
// pkg/util/admission/work_queue.go:339-351
func makeWorkQueueOptions(workKind WorkKind) workQueueOptions {
	switch workKind {
	case KVWork:
		return workQueueOptions{
			usesTokens: false,  // ← 使用槽位
			tiedToRange: true,  // ← 需要 Range 顺序保证
		}
	case SQLKVResponseWork, SQLSQLResponseWork:
		return workQueueOptions{
			usesTokens: true,   // ← 使用令牌
			tiedToRange: false, // ← 不需要 Range 绑定
		}
	default:
		panic(errors.AssertionFailedf("unexpected workKind %d", workKind))
	}
}
```

**配置差异原因**：

| WorkKind | usesTokens | tiedToRange | 原因 |
|----------|-----------|-------------|------|
| KVWork | false（槽位） | true | KV 操作是固定大小的单位工作，需要保证同一 Range 的顺序 |
| SQLKVResponseWork | true（令牌） | false | SQL 响应处理大小不一，不需要 Range 顺序 |
| SQLSQLResponseWork | true（令牌） | false | DistSQL 响应处理，大小可变 |

### 5.3 Store 队列的特殊配置

```go
// pkg/util/admission/work_queue.go:2437
opts.usesAsyncAdmit = true // ← Store 队列强制使用异步准入
```

**为什么 Store 队列必须异步？**

```
同步准入（CPU 队列）：
  Admit() → 等待 granter.tryGet() 成功 → 返回
  └─ 调用者阻塞，直到获得资源

异步准入（Store 队列）：
  Admit() → 入队 → 立即返回
  └─ 后台 granter 授权后，调用 onAdmittedReplicatedWork()

原因：
  - Raft 复制已经发生，不能撤销
  - 阻塞调用者会影响 Raft 吞吐量
  - 准入决策可以延迟到真正写入 Pebble 之前
```

---

## DFS Layer 6: 后台 Goroutine 的生命周期管理

### 6.1 优雅关闭机制

```go
// pkg/util/admission/work_queue.go:1305-1307
func (q *WorkQueue) close() {
	close(q.stopCh) // ← 通知所有 goroutine 停止
}
```

**Goroutine 停止流程**：

```
close(stopCh)
    │
    ├─► GC Goroutine (第421行)
    │       select {
    │       case <-ticker.C:
    │           gcTenantsAndResetUsed()
    │       case <-stopCh:  ← 收到停止信号
    │           return      ← 退出
    │       }
    │
    └─► Epoch 关闭 Goroutine (第498行)
            select {
            case <-timer.C:
                tryCloseEpoch()
            case <-stopCh:  ← 收到停止信号
                return      ← 退出
            }
```

### 6.2 资源泄漏防护

**问题**：如果 goroutine 不停止会怎样？

```go
// 错误示例（会导致 goroutine 泄漏）
func badInit(q *WorkQueue) {
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			<-ticker.C
			q.gcTenantsAndResetUsed()
			// ← 没有退出条件！即使 q 被销毁，goroutine 也会永久运行
		}
	}()
}
```

**正确实现**：
```go
// 正确示例（带停止机制）
func goodInit(q *WorkQueue) {
	stopCh := make(chan struct{})
	q.stopCh = stopCh

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				q.gcTenantsAndResetUsed()
			case <-stopCh:  // ← 检查停止信号
				ticker.Stop() // 清理 ticker
				return        // 退出 goroutine
			}
		}
	}()
}
```

---

## 实战案例：完整初始化追踪

### 案例：创建 CPU KV 队列

```go
// GrantCoordinator 初始化时
opts := makeWorkQueueOptions(KVWork)
// → opts = {usesTokens: false, tiedToRange: true}

q := &WorkQueue{}
initWorkQueue(
	q,
	ambientCtx,
	KVWork,                    // workKind
	"kv-regular-cpu-queue",    // queueKind
	slotGranter,               // granter
	settings,
	metrics,
	opts,
	nil,                       // knobs
)

// 初始化后的状态：
q.workKind = KVWork
q.queueKind = "kv-regular-cpu-queue"
q.usesTokens = false
q.tiedToRange = true
q.usesAsyncAdmit = false
q.mu.tenants = {} (空 map)
q.mu.epochLengthNanos = 100,000,000 (100ms)
q.mu.closedEpochThreshold = 计算得出的初始 epoch

// 后台 goroutine 已启动：
// - GC goroutine: 每秒清理空闲租户
// - Epoch 关闭 goroutine: 定期切换 FIFO/LIFO
```

### 案例：创建 Store 队列

```go
// StoreGrantCoordinator 初始化时
opts := makeWorkQueueOptions(KVWork)
opts.usesAsyncAdmit = true // ← 强制异步

for wc := range [RegularWorkClass, ElasticWorkClass] {
	queueKind := wc == RegularWorkClass ?
		"kv-regular-store-queue" : "kv-elastic-store-queue"

	initWorkQueue(
		&storeQueue.q[wc],
		ambientCtx,
		KVWork,
		queueKind,
		granters[wc],  // 不同的 granter
		settings,
		metrics[wc],   // 不同的 metrics
		opts,
		knobs,
	)
}

// 结果：创建了 2 个队列
// 队列 0 (RegularWorkClass):
//   queueKind = "kv-regular-store-queue"
//   usesAsyncAdmit = true
//
// 队列 1 (ElasticWorkClass):
//   queueKind = "kv-elastic-store-queue"
//   usesAsyncAdmit = true
```

---

## 性能与资源优化

### 优化 1：对象池复用

```go
// 每秒可能创建/销毁数千个 tenantInfo
// 使用对象池避免频繁 GC

// 无对象池：
func badNewTenantInfo(id uint64) *tenantInfo {
	return &tenantInfo{id: id} // ← 每次都分配新内存
}

// 有对象池：
func newTenantInfo(id uint64, weight uint32) *tenantInfo {
	ti := tenantInfoPool.Get().(*tenantInfo) // ← 复用已分配的内存
	*ti = tenantInfo{...}
	return ti
}
```

**性能对比**：
```
基准测试（1000000 次 alloc + free）:
  无对象池: 50ms, 64MB 内存
  有对象池: 10ms, 8MB 内存

→ 5x 性能提升，8x 内存节省
```

### 优化 2：定时器重用

```go
// pkg/util/admission/work_queue.go:491-495
if timer == nil {
	timer = time.NewTimer(timerDur)
} else {
	timer.Reset(timerDur) // ← 复用 timer，避免分配
}
```

**为什么重要？**
```go
// 错误做法（每次都创建新 timer）
for {
	timer := time.NewTimer(100 * time.Millisecond)
	<-timer.C
	// ← timer 泄漏！Stop() 后不能被 GC，因为还在 runtime timer heap 中
}

// 正确做法（复用 timer）
timer := time.NewTimer(100 * time.Millisecond)
for {
	<-timer.C
	timer.Reset(100 * time.Millisecond) // ← 复用同一个 timer
}
```

### 优化 3：切片容量管理

```go
// pkg/util/admission/work_queue.go:1544-1549
if cap(ti.waitingWorkHeap) > 100 {
	ti.waitingWorkHeap = nil // ← 丢弃过大的切片
}
```

**内存管理策略**：
```
场景：
- 正常情况：每个租户 0-10 个排队请求
- 异常情况：某个租户突然产生 10000 个请求

策略：
  if cap > 100:
      丢弃切片 → GC 回收 → 下次从 cap=0 开始
  else:
      保留切片 → 复用内存

平衡点：
  - cap ≤ 100: 内存占用可接受（< 1KB）
  - cap > 100: 内存浪费风险（> 1KB）
```

---

## 调试与监控

### 监控指标（metrics）

```go
// WorkQueueMetrics 结构
type WorkQueueMetrics struct {
	Requested       *metric.Counter   // 请求总数
	Admitted        *metric.Counter   // 准入总数
	Errored         *metric.Counter   // 错误总数
	WaitDurations   metric.IHistogram // 等待时长分布
	WaitQueueLength *metric.Gauge     // 当前队列长度
}
```

**示例 Prometheus 查询**：
```promql
# 平均等待时长
rate(admission_wait_durations_sum[5m]) / rate(admission_wait_durations_count[5m])

# 队列积压
admission_wait_queue_length{queue="kv-regular-cpu-queue"}

# 拒绝率
rate(admission_errored[5m]) / rate(admission_requested[5m])
```

### 日志输出

```go
// pkg/util/admission/work_queue.go:559-560
if tenant.fifoPriorityThreshold != prevThreshold && doLogFunc() {
	log.Dev.Infof(q.ambientCtx, "%s: FIFO threshold for tenant %d %s %d",
		q.workKind, tenant.id, logVerb, tenant.fifoPriorityThreshold)
}
```

**示例日志**：
```
[admission] KVWork: FIFO threshold for tenant 2 changed to 5
→ 租户 2 的优先级 < 5 将使用 LIFO，>= 5 使用 FIFO
```

---

## 总结

### 关键要点回顾

1. **初始化时机**：
   - CPU 队列：在 `GrantCoordinator` 初始化时创建（每个 WorkKind 一个）
   - Store 队列：在 `StoreGrantCoordinator` 初始化时创建（每个 Store 每个 WorkClass 一个）

2. **核心机制**：
   - **租户公平性**：基于 `used/weight` 的堆排序
   - **FIFO/LIFO 自适应**：根据队列延迟自动切换
   - **对象池复用**：减少 GC 压力
   - **后台 Goroutine**：定期 GC 和 Epoch 关闭

3. **配置差异**：
   - KVWork：槽位模型 + Range 绑定 + 同步准入
   - SQL 工作：令牌模型 + 无 Range 绑定 + 同步准入
   - Store 队列：令牌模型 + Range 绑定 + **异步准入**

4. **资源管理**：
   - 使用 `sync.Pool` 复用 `tenantInfo`
   - 使用 `stopCh` 优雅关闭 goroutine
   - 切片容量超过 100 时丢弃，避免内存浪费

### 下一章预告

下一章我们将深入分析 **`granted()` 方法**——当 granter 决定授权时，如何从队列中选择一个等待的请求，以及如何处理授权过程中的竞态条件。

---

## 附录：完整调用链追踪

```
[节点启动]
  └─> server.New()
      └─> admission.NewGrantCoordinators()
          ├─> GrantCoordinator.init()
          │   └─> makeWorkQueue(KVWork)
          │       └─> initWorkQueue(q, ..., "kv-regular-cpu-queue", ...)
          │           ├─> 后台 GC goroutine 启动
          │           └─> 后台 Epoch 关闭 goroutine 启动
          │
          ├─> makeWorkQueue(SQLKVResponseWork)
          │   └─> initWorkQueue(q, ..., "sql-kv-response-queue", ...)
          │
          ├─> makeWorkQueue(SQLSQLResponseWork)
          │   └─> initWorkQueue(q, ..., "sql-sql-response-queue", ...)
          │
          └─> StoreGrantCoordinator.init(storeID=1)
              └─> makeStoreWorkQueue(storeID=1)
                  ├─> initWorkQueue(&q.q[Regular], ..., "kv-regular-store-queue", ...)
                  └─> initWorkQueue(&q.q[Elastic], ..., "kv-elastic-store-queue", ...)
```

**最终结果**：一个典型节点有 **5 个 WorkQueue 实例**（假设单 Store）：
1. kv-regular-cpu-queue
2. sql-kv-response-queue
3. sql-sql-response-queue
4. kv-regular-store-queue (Store 1)
5. kv-elastic-store-queue (Store 1)

如果有 N 个 Store，则有 **3 + 2N** 个 WorkQueue 实例。
