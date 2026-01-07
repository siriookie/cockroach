# 第十章：Admit详解——准入控制的完整流程剖析

## 引言

`Admit(ctx context.Context, info WorkInfo)` 是CockroachDB准入控制系统的核心入口函数。每一个需要受控的工作请求都必须通过这个函数来请求准入。本章将以BFS（广度优先）的方式先整体把握流程，再以DFS（深度优先）的方式深入每个细节，完整剖析一个请求从到达到被准入（或拒绝）的全过程。

理解Admit函数对于掌握CockroachDB的准入控制至关重要，因为它体现了：
- 如何判断是否启用准入控制
- 快速路径（fast path）vs 慢路径（slow path）的选择
- 租户公平调度的实现
- 优先级和FIFO/LIFO切换机制
- 异步准入vs同步准入
- 超时和取消处理

## 第一层：BFS - 函数签名和整体流程

### 1.1 函数签名

```go
func (q *WorkQueue) Admit(ctx context.Context, info WorkInfo) (enabled bool, err error)
```

**位置**：[pkg/util/admission/work_queue.go:571](pkg/util/admission/work_queue.go#L571)

**参数**：
- `ctx context.Context`：上下文，用于超时和取消控制
- `info WorkInfo`：工作信息，包含租户ID、优先级、创建时间等

**返回值**：
- `enabled bool`：准入控制是否启用
  - `false`：准入控制未启用，直接执行工作
  - `true`：准入控制已启用，需要等待准入
- `err error`：错误信息
  - `nil`：成功准入
  - 非nil：准入失败（超时、取消等）

**重要约定**：
> 只有当 `enabled=true && err==nil` 时，工作被成功准入，调用者必须在工作完成后调用 `AdmittedWorkDone()`

### 1.2 WorkInfo结构体

```go
type WorkInfo struct {
	// 租户ID
	TenantID roachpb.TenantID

	// 优先级（在租户内部使用）
	Priority admissionpb.WorkPriority

	// 创建时间（用于FIFO排序）
	CreateTime int64

	// 是否绕过准入控制
	BypassAdmission bool

	// 请求的令牌/槽位数量
	RequestedCount int64

	// 复制工作信息（用于below-raft异步准入）
	ReplicatedWorkInfo ReplicatedWorkInfo
}
```

**位置**：[pkg/util/admission/work_queue.go:162-190](pkg/util/admission/work_queue.go#L162-L190)

### 1.3 整体流程图（BFS第一层）

```
Admit() 入口
    │
    ├─► [阶段1] 准入控制启用检查
    │       ├─► 未启用 → 返回 (enabled=false, err=nil)
    │       └─► 已启用 → 继续
    │
    ├─► [阶段2] 参数验证与初始化
    │       ├─► 检查 RequestedCount
    │       ├─► 获取租户信息
    │       └─► 检查是否需要旁路
    │
    ├─► [阶段3] 旁路处理（仅KVWork）
    │       ├─► BypassAdmission=true → 直接占用资源
    │       └─► BypassAdmission=false → 继续
    │
    ├─► [阶段4] 快速路径尝试（Fast Path）
    │       ├─► 队列为空 → 尝试 tryGet()
    │       │   ├─► 成功 → 返回 (enabled=true, err=nil)
    │       │   └─► 失败 → 进入慢路径
    │       └─► 队列非空 → 直接进入慢路径
    │
    ├─► [阶段5] 慢路径入队（Slow Path）
    │       ├─► 取消检查
    │       ├─► 创建 waitingWork
    │       ├─► 选择排序方式（FIFO/LIFO）
    │       └─► 入队到租户堆
    │
    └─► [阶段6] 等待准入
            ├─► 异步准入 → 立即返回 (enabled=false, err=nil)
            └─► 同步准入 → 阻塞等待
                ├─► 超时/取消 → 返回 (enabled=true, err!=nil)
                └─► 获得授权 → 返回 (enabled=true, err=nil)
```

## 第二层：BFS - 六大阶段详解

### 2.1 阶段1：准入控制启用检查

**代码位置**：[work_queue.go:572-582](pkg/util/admission/work_queue.go#L572-L582)

```go
if !info.ReplicatedWorkInfo.Enabled {
	enabledSetting := admissionControlEnabledSettings[q.workKind]
	if enabledSetting != nil && !enabledSetting.Get(&q.settings.SV) {
		q.metrics.recordBypassedAdmission(info.Priority)
		return false, nil
	}
}
```

**判断逻辑**：

1. **非复制工作**（`ReplicatedWorkInfo.Enabled == false`）：
   - 检查对应的集群设置
   - `KVWork` → `admission.kv.enabled`
   - `SQLKVResponseWork` → `admission.sql_kv_response.enabled`
   - `SQLSQLResponseWork` → `admission.sql_sql_response.enabled`

2. **复制工作**（`ReplicatedWorkInfo.Enabled == true`）：
   - 总是启用准入控制
   - 使用below-raft异步准入机制

**流程图**：

```
┌─────────────────────────────┐
│ info.ReplicatedWorkInfo     │
│ .Enabled?                   │
└────────┬───────────┬────────┘
         │           │
    false│           │true
         ↓           │
    ┌────────────┐  │
    │检查集群设置 │  │
    │是否启用AC? │  │
    └──┬────┬────┘  │
       │    │       │
     是│    │否     │
       │    ↓       │
       │  返回      │
       │ (false,    │
       │  nil)      │
       │            │
       └────────────┴───► 继续执行
                         (准入控制启用)
```

**关键点**：
- 这是最早的退出点，如果准入控制未启用，直接返回`(false, nil)`
- 调用者看到`enabled=false`，知道无需调用`AdmittedWorkDone()`

### 2.2 阶段2：参数验证与初始化

**代码位置**：[work_queue.go:589-608](pkg/util/admission/work_queue.go#L589-L608)

```go
if info.RequestedCount == 0 {
	// 默认请求1个单位
	info.RequestedCount = 1
}
if !q.usesTokens && info.RequestedCount != 1 {
	panic(errors.AssertionFailedf("unexpected RequestedCount: %d", info.RequestedCount))
}
q.metrics.incRequested(info.Priority)
tenantID := info.TenantID.ToUint64()

q.mu.Lock()
tenant, ok := q.mu.tenants[tenantID]
if !ok {
	tenant = newTenantInfo(tenantID, q.getTenantWeightLocked(tenantID))
	q.mu.tenants[tenantID] = tenant
}
```

**步骤分解**：

1. **RequestedCount处理**：
   - 如果未设置（`== 0`），默认为1
   - 对于槽位型队列（`usesTokens=false`），必须等于1
   - 对于令牌型队列（`usesTokens=true`），可以 > 1

2. **指标记录**：
   - `incRequested(info.Priority)`：统计请求数

3. **租户信息获取**：
   - 查找或创建租户信息
   - 租户信息包含：
     - `used`：已使用的资源量
     - `weight`：租户权重
     - `waitingWorkHeap`：等待队列
     - `priorityStates`：优先级状态

**tenantInfo结构**（重要！）：

```go
type tenantInfo struct {
	id     uint64
	weight uint32
	used   uint64  // 已使用的slots/tokens

	// 等待队列
	waitingWorkHeap workHeap
	openEpochsHeap  workHeap

	// 优先级状态（用于FIFO/LIFO切换）
	priorityStates *priorityStates

	// FIFO阈值（priority < threshold的使用LIFO）
	fifoPriorityThreshold int

	heapIndex int  // 在tenantHeap中的索引
}
```

### 2.3 阶段3：旁路处理（Bypass Admission）

**代码位置**：[work_queue.go:609-637](pkg/util/admission/work_queue.go#L609-L637)

```go
if info.ReplicatedWorkInfo.Enabled {
	if info.BypassAdmission {
		panic("unexpected BypassAdmission bit set for below raft admission")
	}
	if !q.usesTokens {
		panic("unexpected ReplicatedWrite.Enabled on slot-based queue")
	}
}

if info.BypassAdmission && q.workKind == KVWork {
	tenant.used += uint64(info.RequestedCount)
	if isInTenantHeap(tenant) {
		q.mu.tenantHeap.fix(tenant)
	}
	q.mu.Unlock()
	q.granter.tookWithoutPermission(info.RequestedCount)
	q.metrics.incAdmitted(info.Priority)
	q.metrics.recordBypassedAdmission(info.Priority)
	return true, nil
}
```

**旁路条件**：
1. `info.BypassAdmission == true`
2. `q.workKind == KVWork`
3. `info.ReplicatedWorkInfo.Enabled == false`

**为什么只有KVWork可以旁路**：

| 工作类型 | 是否允许旁路 | 原因 |
|---------|-------------|------|
| KVWork | ✓ | 节点心跳、意图解析等内部操作可能导致死锁 |
| SQLKVResponseWork | × | 纯SQL层工作，不会造成死锁 |
| SQLSQLResponseWork | × | 纯SQL层工作，不会造成死锁 |

**旁路流程**：

```
BypassAdmission=true
    ↓
更新 tenant.used
    ↓
调整 tenantHeap
    ↓
granter.tookWithoutPermission()  ← 通知granter强制占用
    ↓
记录指标
    ↓
返回 (true, nil)  ← 立即成功
```

**关键点**：
- 旁路不等于"不消耗资源"
- 资源消耗会记录在`tenant.used`中
- `granter.tookWithoutPermission()`让granter知道资源被强制占用

### 2.4 阶段4：快速路径尝试（Fast Path）

**代码位置**：[work_queue.go:645-718](pkg/util/admission/work_queue.go#L645-L718)

```go
tenant.priorityStates.requestAtPriority(info.Priority)

if len(q.mu.tenantHeap) == 0 && !q.knobs.DisableWorkQueueFastPath {
	// Fast-path. Try to grab token/slot.
	tenant.used += uint64(info.RequestedCount)
	q.mu.Unlock()

	if q.granter.tryGet(canBurst, info.RequestedCount) {
		q.metrics.incAdmitted(info.Priority)
		// ... 处理复制工作的特殊逻辑 ...
		q.metrics.recordFastPathAdmission(info.Priority)
		return true, nil
	}

	// Did not get token/slot, need to queue
	q.mu.Lock()
	tenant, ok = q.mu.tenants[tenantID]
	if !ok {
		tenant = newTenantInfo(tenantID, q.getTenantWeightLocked(tenantID))
		q.mu.tenants[tenantID] = tenant
	}
	// 回退 tenant.used
	if tenant.used >= uint64(info.RequestedCount) {
		tenant.used -= uint64(info.RequestedCount)
	} else {
		tenant.used = 0
	}
}
```

**快速路径条件**：

```
条件1: len(q.mu.tenantHeap) == 0  (队列为空)
  AND
条件2: !q.knobs.DisableWorkQueueFastPath  (未禁用快速路径)
```

**快速路径流程图**：

```
队列为空?
    │
    ├─ Yes → 快速路径
    │         │
    │         ↓
    │    乐观更新 tenant.used
    │         │
    │         ↓
    │    释放锁 q.mu.Unlock()
    │         │
    │         ↓
    │    granter.tryGet()
    │         │
    │    ┌────┴────┐
    │    │         │
    │  成功       失败
    │    │         │
    │    ↓         ↓
    │  返回     重新加锁
    │ (true,    回退used
    │  nil)     进入慢路径
    │
    └─ No → 直接进入慢路径
```

**为什么快速路径释放锁**：

释放锁允许：
1. **并发性提高**：其他goroutine可以同时尝试快速路径
2. **避免长时间持锁**：`tryGet()`可能需要获取`GrantCoordinator.mu`

代价：
1. **竞态条件**：释放锁后，`tenant`可能被GC删除
2. **需要重新获取**：失败时需要重新加锁和查找tenant
3. **不公平**：并发请求可能"插队"

**重要的竞态说明**（代码注释[work_queue.go:689-702](pkg/util/admission/work_queue.go#L689-L702)）：

```go
// There is a race here: before q.mu is acquired, the granter could
// experience a reduction in load and call
// WorkQueue.hasWaitingRequests to see if it should grant, but since
// there is nothing in the queue that method will return false. Then the
// work here queues up even though granter has spare capacity.
```

**竞态场景**：

```
时间线：

T0: 请求A 释放q.mu，调用tryGet()失败
T1: granter负载降低，调用hasWaitingRequests()
T2: hasWaitingRequests()返回false（队列还是空的）
T3: 请求A 重新获取q.mu，准备入队
T4: 请求A 入队完成

结果：granter有空闲容量，但请求A在队列中等待
```

**如何缓解**：
- GrantCoordinator定期（高频率）检查队列状态
- 即使有短暂延迟，最终会被处理

### 2.5 阶段5：慢路径入队（Slow Path）

**代码位置**：[work_queue.go:720-758](pkg/util/admission/work_queue.go#L720-L758)

```go
// Check for cancellation.
startTime := q.timeNow()
if ctx.Err() != nil {
	// Already canceled
	q.mu.Unlock()
	q.metrics.incErrored(info.Priority)
	return true, errors.Wrapf(ctx.Err(), "...")
}

// Push onto heap(s).
ordering := fifoWorkOrdering
if int(info.Priority) < tenant.fifoPriorityThreshold {
	ordering = lifoWorkOrdering
}
work := newWaitingWork(info.Priority, ordering, info.CreateTime,
                       info.RequestedCount, startTime, q.mu.epochLengthNanos)
work.replicated = info.ReplicatedWorkInfo

inTenantHeap := isInTenantHeap(tenant)
if work.epoch <= q.mu.closedEpochThreshold || ordering == fifoWorkOrdering {
	heap.Push(&tenant.waitingWorkHeap, work)
} else {
	heap.Push(&tenant.openEpochsHeap, work)
}
if !inTenantHeap {
	heap.Push(&q.mu.tenantHeap, tenant)
}

q.mu.Unlock()
```

**步骤详解**：

**步骤1：取消检查**

```go
if ctx.Err() != nil {
	// 上下文已取消，直接返回错误
}
```

**步骤2：选择排序方式（FIFO vs LIFO）**

```go
ordering := fifoWorkOrdering  // 默认FIFO
if int(info.Priority) < tenant.fifoPriorityThreshold {
	ordering = lifoWorkOrdering  // 低优先级使用LIFO
}
```

**FIFO vs LIFO 决策**：

```
优先级 vs 阈值
    │
    ├─ priority >= threshold → FIFO（先进先出）
    │                          适合：正常负载，公平处理
    │
    └─ priority < threshold  → LIFO（后进先出）
                               适合：过载情况，优先完成新请求
```

**为什么使用LIFO**：

在严重过载时：
- FIFO会导致所有请求都超时（老请求一直等待）
- LIFO让新请求优先完成（牺牲老请求）
- 至少有部分请求能成功

**步骤3：创建waitingWork**

```go
work := newWaitingWork(
	info.Priority,              // 优先级
	ordering,                   // FIFO/LIFO
	info.CreateTime,            // 创建时间
	info.RequestedCount,        // 请求数量
	startTime,                  // 入队时间
	q.mu.epochLengthNanos,      // epoch长度
)
work.replicated = info.ReplicatedWorkInfo
```

**waitingWork结构**：

```go
type waitingWork struct {
	priority      admissionpb.WorkPriority
	createTime    int64
	requestedCount int64
	enqueueingTime time.Time
	epoch         int64  // LIFO使用的epoch

	replicated ReplicatedWorkInfo  // 复制工作信息
	ch chan grantChainID           // 准入通知channel

	heapIndex         int   // 在heap中的索引
	inWaitingWorkHeap bool  // 是否在waitingWorkHeap中
}
```

**步骤4：选择入队的堆**

```go
if work.epoch <= q.mu.closedEpochThreshold || ordering == fifoWorkOrdering {
	heap.Push(&tenant.waitingWorkHeap, work)
} else {
	heap.Push(&tenant.openEpochsHeap, work)
}
```

**两个堆的用途**：

| 堆名称 | 用途 | 排序规则 |
|--------|------|---------|
| `waitingWorkHeap` | 存放FIFO work或closed epoch的LIFO work | 按priority、createTime排序 |
| `openEpochsHeap` | 存放open epoch的LIFO work | 按epoch倒序（LIFO） |

**步骤5：更新tenantHeap**

```go
if !inTenantHeap {
	heap.Push(&q.mu.tenantHeap, tenant)
}
```

**tenantHeap排序规则**：

```go
// 按 used / weight 排序（公平调度）
func (th tenantHeap) Less(i, j int) bool {
	usedByWeightI := float64(th[i].used) / float64(th[i].weight)
	usedByWeightJ := float64(th[j].used) / float64(th[j].weight)
	return usedByWeightI < usedByWeightJ
}
```

**多层堆结构**：

```
┌─────────────────┐
│  tenantHeap     │  ← 最外层：租户间公平
│  (按used/weight)│
└────┬────────────┘
     │
     ├─► Tenant1 ───┬─► waitingWorkHeap ← 内层：优先级+时间
     │              └─► openEpochsHeap   ← 内层：LIFO epoch
     │
     ├─► Tenant2 ───┬─► waitingWorkHeap
     │              └─► openEpochsHeap
     │
     └─► ...
```

### 2.6 阶段6：等待准入

**代码位置**：[work_queue.go:760-846](pkg/util/admission/work_queue.go#L760-L846)

**分支1：异步准入（复制工作）**

```go
if info.ReplicatedWorkInfo.Enabled {
	if log.V(1) {
		log.Dev.Infof(ctx, "async-path: len(waiting-work)=%d: enqueued...", queueLen)
	}
	return false, nil  // 立即返回，不等待
}
```

**分支2：同步准入（常规工作）**

```go
var span *tracing.Span
ctx, span = tracing.ChildSpan(ctx, "admissionWorkQueueWait")
defer span.Finish()
defer releaseWaitingWork(work)

select {
case <-ctx.Done():
	// 超时或取消
	waitDur := q.timeNow().Sub(startTime)
	// ... 处理取消逻辑 ...
	return true, errors.Newf("deadline expired...")

case chainID, ok := <-work.ch:
	// 获得准入
	q.metrics.incAdmitted(info.Priority)
	waitDur := q.timeNow().Sub(startTime)
	q.metrics.recordFinishWait(info.Priority, waitDur)
	recordAdmissionWorkQueueStats(span, waitDur, q.queueKind, info.Priority, false)
	q.granter.continueGrantChain(chainID)
	return true, nil
}
```

**select两个分支详解**：

**分支A：ctx.Done() - 超时/取消处理**

```
<-ctx.Done() 触发
    ↓
计算等待时间
    ↓
q.mu.Lock()
    ↓
检查 work.heapIndex
    │
    ├─ heapIndex == -1  (已从heap移除，授权已发生)
    │     ↓
    │  不回退used (避免竞态)
    │     ↓
    │  granter.returnGrant()  ← 归还授权
    │     ↓
    │  <-work.ch  ← 接收chainID
    │     ↓
    │  continueGrantChain()  ← 继续授权链
    │
    └─ heapIndex != -1  (仍在heap中)
          ↓
       从heap移除
          ↓
       从tenantHeap移除(如果需要)
          ↓
    返回错误
```

**关键竞态处理**：

```go
if work.heapIndex == -1 {
	// 已从heap移除，说明granter已经granted
	// 不能回退tenant.used，因为：
	// 1. tenant可能已被GC并返回sync.Pool
	// 2. used可能已被重置为0
	q.mu.Unlock()
	q.granter.returnGrant(info.RequestedCount)
	chainID := <-work.ch  // 必须接收，否则授权链会卡住
	q.granter.continueGrantChain(chainID)
}
```

**分支B：<-work.ch - 成功准入**

```
<-work.ch 收到 chainID
    ↓
检查 work.heapIndex
    │
    └─ 必须 == -1 (否则panic)
       ↓
记录指标
    ↓
continueGrantChain(chainID)  ← 重要！继续授权链
    ↓
返回 (true, nil)
```

**授权链机制**：

```
请求1 等待
    ↓
granter.tryGrantLocked()
    ↓
requester.granted(chainID1)
    ↓
work1.ch <- chainID1
    ↓
请求1 收到，执行工作
    ↓
请求1 的goroutine运行
    ↓
continueGrantChain(chainID1)  ← 触发下一次授权
    ↓
granter 尝试授权请求2
    ↓
...
```

**为什么需要授权链**：

1. **自然节流**：只有当被授权的goroutine真正运行时，才触发下一次授权
2. **考虑调度器能力**：避免授权超过调度器能处理的数量
3. **减少突发**：实验显示减少了5倍的授权突发

## 第三层：DFS - 深入关键函数

### 3.1 tenant公平调度的实现

**tenantHeap的排序**：

```go
type tenantHeap []*tenantInfo

func (th tenantHeap) Less(i, j int) bool {
	// 按 used/weight 升序排序
	usedByWeightI := float64(th[i].used) / float64(th[i].weight)
	usedByWeightJ := float64(th[j].used) / float64(th[j].weight)
	return usedByWeightI < usedByWeightJ
}
```

**公平性证明**：

假设：
- Tenant A: weight=1, used=100
- Tenant B: weight=2, used=100

计算：
- A的比值：100/1 = 100
- B的比值：100/2 = 50

结果：B < A，所以B排在前面，优先获得资源

**动态调整**：

```go
// 授权后更新used
tenant.used += uint64(grantedCount)
if isInTenantHeap(tenant) {
	q.mu.tenantHeap.fix(tenant)  // O(log n) 调整堆
}
```

### 3.2 FIFO/LIFO自适应切换

**切换阈值计算**：

```go
func (ps *priorityStates) requestAtPriority(priority admissionpb.WorkPriority) {
	ps.requestCounts[priority]++
}

func (ps *priorityStates) updateDelayLocked(
	priority admissionpb.WorkPriority, delay time.Duration, canceled bool) {

	if delay > ps.maxQueueDelayToSwitchToLifo {
		// 延迟过长，考虑切换到LIFO
		ps.adjustFIFOThreshold(priority)
	}
}

func adjustFIFOThreshold(priority int) {
	if delay > threshold {
		// 降低阈值，让更多优先级使用LIFO
		tenant.fifoPriorityThreshold = max(0, tenant.fifoPriorityThreshold - 1)
	} else {
		// 恢复阈值
		tenant.fifoPriorityThreshold = min(127, tenant.fifoPriorityThreshold + 1)
	}
}
```

**切换时机**：

```
正常负载
    ↓
某优先级延迟增加
    ↓
超过阈值 (默认100ms)
    ↓
fifoPriorityThreshold 降低
    ↓
更多优先级使用LIFO
    ↓
新请求优先处理
    ↓
延迟降低
    ↓
fifoPriorityThreshold 恢复
    ↓
回到FIFO
```

### 3.3 epoch机制（LIFO实现）

**epoch计算**：

```go
func newWaitingWork(... startTime time.Time, epochLengthNanos int64) *waitingWork {
	work := ...
	work.epoch = startTime.UnixNano() / epochLengthNanos
	return work
}
```

**epoch关闭**：

```go
func (q *WorkQueue) closeEpoch() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mu.closedEpochThreshold = (q.timeNow().UnixNano() / q.mu.epochLengthNanos) - 1
}
```

**LIFO出队顺序**：

```
Epoch:     5      4      3      2
         [new]  [new]  [old]  [old]
                  ↑
                  │
        closedEpochThreshold = 3

openEpochsHeap:   [Epoch 5, Epoch 4]  ← 按epoch倒序
waitingWorkHeap:  [Epoch 3, Epoch 2]  ← 按优先级+时间

出队顺序：
1. openEpochsHeap.Pop() → Epoch 5 (最新)
2. openEpochsHeap.Pop() → Epoch 4
3. waitingWorkHeap.Pop() → Epoch 3 中最高优先级
4. ...
```

**为什么使用epoch**：

1. **时间粒度**：避免完全的LIFO（过于激进）
2. **批处理**：同一epoch的请求一起处理
3. **公平性**：closed epoch仍按优先级处理

### 3.4 granter.tryGet() 详解

**调用链**：

```
WorkQueue.Admit()
    ↓
q.granter.tryGet(canBurst, info.RequestedCount)
    ↓
(具体granter实现，如slotGranter或tokenGranter)
    ↓
coord.tryGet(workKind, count, demuxHandle)
    ↓
coord.mu.Lock()
    ↓
granter.tryGetLocked(count, demuxHandle)
    ↓
(检查资源、CPU过载等)
    ↓
返回 granted bool
```

**slotGranter的tryGetLocked**：

```go
func (sg *slotGranter) tryGetLocked(count int64, _ int8) grantResult {
	if count != 1 {
		panic("slots must be 1")
	}
	if sg.usedSlots < sg.totalSlots || sg.skipSlotEnforcement {
		sg.usedSlots++
		return grantSuccess
	}
	if sg.workKind == KVWork {
		return grantFailDueToSharedResource
	}
	return grantFailLocal
}
```

**tokenGranter的tryGetLocked**：

```go
func (tg *tokenGranter) tryGetLocked(count int64, _ int8) grantResult {
	if tg.cpuOverload.isOverloaded() {
		return grantFailDueToSharedResource
	}
	if tg.availableBurstTokens > 0 || tg.skipTokenEnforcement {
		tg.availableBurstTokens -= count  // 可以变负数！
		return grantSuccess
	}
	return grantFailLocal
}
```

### 3.5 租户GC机制

**何时GC租户**：

```go
func (q *WorkQueue) gcTenantsAndResetUsed() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for tenantID, tenant := range q.mu.tenants {
		if tenant.used == 0 &&
		   tenant.waitingWorkHeap.Len() == 0 &&
		   tenant.openEpochsHeap.Len() == 0 {
			// 无资源使用，无等待工作
			delete(q.mu.tenants, tenantID)
			releaseTenantInfo(tenant)  // 返回sync.Pool
		} else {
			// 重置used（每秒一次）
			tenant.used = 0
		}
	}
}
```

**为什么需要GC**：

1. **内存节省**：避免无限增长的tenant map
2. **公平性重置**：每秒重置`used`，避免长期累积
3. **对象复用**：使用sync.Pool减少GC压力

## 第四层：DFS - 完整执行路径示例

### 4.1 示例1：KVWork快速路径成功

```go
// 输入
info := WorkInfo{
	TenantID:       roachpb.MustMakeTenantID(2),
	Priority:       admissionpb.NormalPri,
	CreateTime:     time.Now().UnixNano(),
	RequestedCount: 1,
}

// 执行流程
```

**详细trace**：

```
T0: Admit()调用
    ↓
T1: 检查准入控制 - 已启用 ✓
    ↓
T2: RequestedCount=1 (默认) ✓
    ↓
T3: incRequested(NormalPri)
    ↓
T4: q.mu.Lock()
    ↓
T5: 查找/创建 tenant(ID=2)
    ↓
T6: len(tenantHeap) == 0  ✓ (队列为空)
    ↓
T7: tenant.used += 1  (乐观更新)
    ↓
T8: q.mu.Unlock()  ← 释放锁
    ↓
T9: granter.tryGet(canBurst, 1)
    ↓
    ├─► coord.tryGet(KVWork, 1, 0)
    │       ↓
    │   coord.mu.Lock()
    │       ↓
    │   slotGranter.tryGetLocked(1, 0)
    │       ↓
    │   usedSlots < totalSlots?  ✓ (假设有空闲)
    │       ↓
    │   usedSlots++
    │       ↓
    │   return grantSuccess
    │       ↓
    │   coord.mu.Unlock()
    │       ↓
    └─► return true
    ↓
T10: incAdmitted(NormalPri)
    ↓
T11: recordFastPathAdmission(NormalPri)
    ↓
T12: return (enabled=true, err=nil)
```

**时间消耗**：
- 快速路径：~1-10 微秒
- 无队列等待
- 两次mutex操作（q.mu和coord.mu）

### 4.2 示例2：SQLKVResponseWork慢路径等待

```go
info := WorkInfo{
	TenantID:       roachpb.MustMakeTenantID(3),
	Priority:       admissionpb.LowPri,
	CreateTime:     time.Now().UnixNano(),
	RequestedCount: 100,  // 请求100个tokens
}
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
```

**详细trace**：

```
T0: Admit()调用
    ↓
T1-T5: (同上)
    ↓
T6: len(tenantHeap) > 0  ✗ (队列非空)
    ↓
T7: tenant.priorityStates.requestAtPriority(LowPri)
    ↓
T8: 跳过快速路径
    ↓
T9: startTime = now()
    ↓
T10: ctx.Err() == nil  ✓
    ↓
T11: 选择排序方式
    │   LowPri (3) < fifoPriorityThreshold (64)?  ✓
    │   → ordering = lifoWorkOrdering
    ↓
T12: work = newWaitingWork(...)
    │   epoch = startTime.UnixNano() / epochLength
    │   epoch = 假设为123
    ↓
T13: work.epoch (123) > closedEpochThreshold (120)?  ✓
    │   → heap.Push(&tenant.openEpochsHeap, work)
    ↓
T14: tenant不在tenantHeap中
    │   → heap.Push(&q.mu.tenantHeap, tenant)
    ↓
T15: q.mu.Unlock()
    ↓
T16: recordStartWait(LowPri)
    ↓
T17: 进入 select {}
    │
    │   ... 等待 ...
    │
    ├── (时间流逝，假设1.2秒后)
    │
T18: granter周期性检查队列
    │   ↓
    │ GrantCoordinator.tryGrant()
    │   ↓
    │ granter.tryGrantLocked(chainID)
    │   ↓
    │ tokenGranter.tryGetLocked(1, 0)
    │   ↓
    │ cpuOverload.isOverloaded() == false  ✓
    │   ↓
    │ availableBurstTokens > 0  ✓
    │   ↓
    │ availableBurstTokens -= 1
    │   ↓
    │ return grantSuccess
    │   ↓
    │ requester.granted(chainID)
    │   ↓
    │ WorkQueue.granted(chainID)
    │   ↓
    │ q.mu.Lock()
    │   ↓
    │ tenant = tenantHeap[0]  (公平调度选中)
    │   ↓
    │ tenant.id == 3?  ✓ (可能，取决于used/weight)
    │   ↓
    │ work = heap.Pop(&tenant.openEpochsHeap)  ← 取最新epoch
    │   ↓
    │ waitDur = now - work.enqueueingTime
    │   ↓
    │ tenant.used += 100
    │   ↓
    │ tenantHeap.fix(tenant)
    │   ↓
    │ requestedCount = work.requestedCount  (100)
    │   ↓
    │ q.mu.Unlock()
    │   ↓
    │ work.ch <- chainID  ← 发送通知！
    │   ↓
    └ return 100  (表示接受了100 tokens)
    ↓
T19: <-work.ch 收到 chainID
    ↓
T20: incAdmitted(LowPri)
    ↓
T21: waitDur = now - startTime  (1.2秒)
    ↓
T22: recordFinishWait(LowPri, 1.2秒)
    ↓
T23: recordAdmissionWorkQueueStats(...)
    ↓
T24: continueGrantChain(chainID)  ← 触发下一次授权！
    ↓
T25: return (enabled=true, err=nil)
```

**时间消耗**：
- 总耗时：1.2秒
- 入队：~10 微秒
- 等待：1.2秒
- 出队：~5 微秒

### 4.3 示例3：复制工作异步准入

```go
info := WorkInfo{
	TenantID:       roachpb.MustMakeTenantID(4),
	Priority:       admissionpb.NormalPri,
	CreateTime:     time.Now().UnixNano(),
	RequestedCount: 1024,  // 1KB write
	ReplicatedWorkInfo: ReplicatedWorkInfo{
		Enabled:     true,
		RangeID:     roachpb.RangeID(42),
		LogPosition: LogPosition{Term: 5, Index: 100},
		Ingested:    false,
	},
}
```

**详细trace**：

```
T0: Admit()调用
    ↓
T1: ReplicatedWorkInfo.Enabled = true  ✓
    │   → 跳过集群设置检查
    ↓
T2-T6: (参数验证、租户查找等)
    ↓
T7: ReplicatedWorkInfo.Enabled && BypassAdmission?
    │   → panic("不允许旁路")
    ↓
T8: !usesTokens?
    │   → panic("复制工作必须用tokens")
    ↓
T9: (快速路径或慢路径，假设慢路径)
    ↓
T10: work = newWaitingWork(...)
    │   work.replicated = info.ReplicatedWorkInfo  ← 设置
    ↓
T11: heap.Push(...)
    ↓
T12: q.mu.Unlock()
    ↓
T13: recordStartWait(NormalPri)
    ↓
T14: ReplicatedWorkInfo.Enabled?  ✓
    │   ↓
    │ log.Infof("async-path: enqueued r42 log-position=5/100")
    │   ↓
    └ return (enabled=false, err=nil)  ← 立即返回！
```

**关键点**：

1. **不等待**：立即返回`(false, nil)`
2. **enabled=false**：调用者知道无需调用`AdmittedWorkDone()`
3. **后台处理**：granter稍后会调用`granted()`，触发`onAdmittedReplicatedWork.admittedReplicatedWork()`
4. **流控返还**：通过`onAdmittedReplicatedWork`接口返还flow tokens

### 4.4 示例4：超时取消处理

```go
ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
defer cancel()

info := WorkInfo{
	TenantID:       roachpb.MustMakeTenantID(5),
	Priority:       admissionpb.NormalPri,
	CreateTime:     time.Now().UnixNano(),
	RequestedCount: 1,
}
```

**假设队列拥塞，100ms内未获得授权**：

```
T0: Admit()调用
    ↓
T1-T10: 入队到waitingWorkHeap
    ↓
T11: select {
    │   case <-ctx.Done():  ← 100ms后触发
    │   case <-work.ch:
    │ }
    ↓
T12: <-ctx.Done() 分支
    ↓
T13: waitDur = now - startTime  (100ms)
    ↓
T14: q.mu.Lock()
    ↓
T15: tenant.priorityStates.updateDelayLocked(NormalPri, 100ms, true)
    │   → 可能触发FIFO→LIFO切换
    ↓
T16: work.heapIndex?
    │
    ├─► == -1  (已授权，竞态)
    │       ↓
    │   q.mu.Unlock()
    │       ↓
    │   granter.returnGrant(1)
    │       ↓
    │   chainID := <-work.ch  ← 必须接收
    │       ↓
    │   continueGrantChain(chainID)
    │
    └─► != -1  (仍在heap中)
            ↓
        tenant.waitingWorkHeap.remove(work)
            ↓
        if !isInTenantHeap(tenant) {
            tenantHeap.remove(tenant)
        }
            ↓
        q.mu.Unlock()
    ↓
T17: incErrored(NormalPri)
    ↓
T18: recordFinishWait(NormalPri, 100ms)
    ↓
T19: recordAdmissionWorkQueueStats(..., deadlineExceeded=true)
    ↓
T20: return (enabled=true, err=deadline expired)
```

**重要竞态处理**：

即使进入`ctx.Done()`分支，仍可能与授权竞态：

```
Goroutine A (Admit):           Goroutine B (granter):
    │
T0  │ select {
    │   case <-ctx.Done():  ─────┐
T1  │   ...                     │
    │                            │
    │                          T2│ granter.granted()
    │                            │ q.mu.Lock()
    │                            │ work = heap.Pop(...)
    │                            │ work.ch <- chainID
    │                            │ q.mu.Unlock()
    │                            │
T3  │ q.mu.Lock()               │
T4  │ work.heapIndex?           │
    │   → == -1 (已移除)  ←──────┘
T5  │ 必须处理已授权的情况
    │ granter.returnGrant(1)
    │ <-work.ch  ← 接收chainID
    │ continueGrantChain(chainID)
```

## 第五层：完整调用链可视化

### 5.1 快速路径完整调用链

```
[用户代码]
    │
    ↓
kvQueue.Admit(ctx, info)  ────────────────────┐
    │                                          │
    ↓                                          │
检查准入控制启用 admission.kv.enabled          │
    │                                          │
    ↓                                          │
q.mu.Lock()                                   │
    │                                          │
    ↓                                          │
获取/创建 tenant                                │  WorkQueue
    │                                          │
    ↓                                          │
len(tenantHeap) == 0?  ✓                      │
    │                                          │
    ↓                                          │
tenant.used += count                           │
    │                                          │
    ↓                                          │
q.mu.Unlock()  ←───────────────────────────────┘
    │
    ↓
q.granter.tryGet(canBurst, count) ────────────┐
    │                                          │
    ↓                                          │  granter (slotGranter)
coord.tryGet(workKind, count, 0)              │
    │                                          │
    ↓                                          │
coord.mu.Lock()                               │
    │                                          │
    ↓                                          │
slotGranter.tryGetLocked(count, 0)            │
    │                                          │
    ↓                                          │
usedSlots < totalSlots?  ✓                    │
    │                                          │
    ↓                                          │
usedSlots++                                   │
    │                                          │
    ↓                                          │
return grantSuccess                            │
    │                                          │
    ↓                                          │
coord.mu.Unlock()  ←───────────────────────────┘
    │
    ↓
return true
    │
    ↓
q.metrics.incAdmitted(priority)
    │
    ↓
return (enabled=true, err=nil) ←─ 回到用户代码
```

### 5.2 慢路径完整调用链

```
[用户代码]
    │
    ↓
kvQueue.Admit(ctx, info)
    │
    ↓
... (同快速路径开始部分) ...
    │
    ↓
len(tenantHeap) > 0  ✗ (队列非空)
    │
    ↓
跳过快速路径
    │
    ↓
tenant.priorityStates.requestAtPriority(priority)
    │
    ↓
ctx.Err() == nil?  ✓
    │
    ↓
选择 FIFO/LIFO
    │
    ↓
work = newWaitingWork(...)  ──────────────────┐
    │                                          │
    ↓                                          │
heap.Push(&tenant.waitingWorkHeap, work)      │  入队
    │                                          │
    ↓                                          │
heap.Push(&q.mu.tenantHeap, tenant)           │
    │                                          │
    ↓                                          │
q.mu.Unlock()  ←───────────────────────────────┘
    │
    ↓
recordStartWait(priority)
    │
    ↓
select {
    case <-ctx.Done():  ────────────► 超时分支
    case <-work.ch:  ──┐
}                      │
    │                  │
    ↓                  │
[等待中...]            │
    │                  │
    │                  │  [后台granter线程]
    │                  │      │
    │                  │      ↓
    │                  │  coord.tryGrant()
    │                  │      │
    │                  │      ↓
    │                  │  coord.mu.Lock()
    │                  │      │
    │                  │      ↓
    │                  │  for each workKind (按优先级)
    │                  │      │
    │                  │      ↓
    │                  │  granter.tryGrantLocked(chainID)
    │                  │      │
    │                  │      ↓
    │                  │  检查资源
    │                  │      │
    │                  │      ↓
    │                  │  requester.granted(chainID)
    │                  │      │
    │                  │      ↓
    │                  │  q.mu.Lock()
    │                  │      │
    │                  │      ↓
    │                  │  tenant = tenantHeap[0]  ← 公平调度
    │                  │      │
    │                  │      ↓
    │                  │  work = heap.Pop(...)
    │                  │      │
    │                  │      ↓
    │                  │  tenant.used += count
    │                  │      │
    │                  │      ↓
    │                  │  tenantHeap.fix(tenant)
    │                  │      │
    │                  │      ↓
    │                  │  q.mu.Unlock()
    │                  │      │
    │                  │      ↓
    │                  └──── work.ch <- chainID  ← 发送通知！
    │                         │
    ↓                         │
<-work.ch 收到 chainID  ←─────┘
    │
    ↓
q.metrics.incAdmitted(priority)
    │
    ↓
recordFinishWait(priority, waitDur)
    │
    ↓
recordAdmissionWorkQueueStats(...)
    │
    ↓
continueGrantChain(chainID) ──────────────────┐
    │                                          │
    ↓                                          │
coord.continueGrantChain(workKind, chainID)   │  触发下一次授权
    │                                          │
    ↓                                          │
coord.mu.Lock()                               │
    │                                          │
    ↓                                          │
coord.tryGrantLocked(chainID) ← 递归调用       │
    │                                          │
    ↓                                          │
coord.mu.Unlock()  ←───────────────────────────┘
    │
    ↓
return (enabled=true, err=nil) ←─ 回到用户代码
```

## 第六层：性能与优化考虑

### 6.1 锁粒度分析

**锁的层次结构**：

```
Level 1: GrantCoordinator.mu  (最外层)
    │
    └─► Level 2: WorkQueue.mu  (中层)
            │
            └─► Level 3: tenantWeights.mu  (内层，较少使用)
```

**锁顺序规则**：
> 必须按 Level 1 → Level 2 → Level 3 的顺序获取，避免死锁

**临界区大小**：

| 操作 | 持锁时间 | 优化措施 |
|------|---------|---------|
| 快速路径成功 | ~1μs | 提前释放锁再调用granter |
| 入队操作 | ~5μs | heap操作是O(log n) |
| 出队操作 | ~5μs | 提前构造返回值再释放锁 |
| GC操作 | ~100μs | 每秒一次，影响小 |

### 6.2 内存分配优化

**sync.Pool使用**：

```go
var waitingWorkPool = sync.Pool{
	New: func() interface{} {
		return &waitingWork{
			ch: make(chan grantChainID, 1),
		}
	},
}

func newWaitingWork(...) *waitingWork {
	work := waitingWorkPool.Get().(*waitingWork)
	// 初始化字段...
	return work
}

func releaseWaitingWork(work *waitingWork) {
	*work = waitingWork{}  // 清零
	waitingWorkPool.Put(work)
}
```

**减少分配的设计**：

1. **waitingWork复用**：使用sync.Pool
2. **channel复用**：channel随waitingWork一起复用
3. **tenantInfo复用**：GC时返回sync.Pool

### 6.3 并发性能

**快速路径的并发优势**：

```
线程1    线程2    线程3
  │        │        │
  ├─ Lock  │        │
  │        │        │
  ├─Unlock │        │     ← 快速释放
  │        │        │
  ├─tryGet │Lock    │
  │        │        │
  │        │ Unlock │
  │        │        Lock
  │        │ tryGet │
  │        │        │Unlock
  │        │        │tryGet
```

如果持锁调用tryGet：

```
线程1    线程2    线程3
  │        │        │
  ├─ Lock  │        │
  │        ├─Wait   │
  │        │        ├─Wait
  ├─tryGet │        │
  │        │        │
  ├─Unlock │        │
  │        ├─Lock   │
  │        │        ├─Wait
  │        ├─tryGet │
  │        │        │
  │        ├─Unlock │
  │        │        ├─Lock
  │        │        │tryGet
  │        │        │Unlock
```

**并发度提升**：快速路径允许多个线程并发调用tryGet

### 6.4 CPU缓存友好性

**数据结构布局**：

```go
type WorkQueue struct {
	// 热路径字段放在前面
	workKind   WorkKind
	queueKind  QueueKind
	granter    granter
	usesTokens bool

	// 冷路径字段
	settings *cluster.Settings
	metrics  *WorkQueueMetrics
	knobs    *TestingKnobs

	// 互斥锁保护的字段单独成组
	mu struct {
		syncutil.Mutex
		tenantHeap   tenantHeap
		tenants      map[uint64]*tenantInfo
		// ...
	}
}
```

**缓存行优化**：
- 热字段集中在前64字节（一个缓存行）
- mu字段独占缓存行（避免false sharing）

## 总结

### 核心要点回顾

1. **六大阶段**：
   - 启用检查 → 参数验证 → 旁路处理 → 快速路径 → 慢路径 → 等待准入

2. **两条主路径**：
   - 快速路径：队列为空时直接tryGet，延迟~1-10μs
   - 慢路径：入队等待，延迟取决于负载

3. **三层堆结构**：
   - tenantHeap：租户间公平（按used/weight）
   - waitingWorkHeap：租户内FIFO（按priority+createTime）
   - openEpochsHeap：租户内LIFO（按epoch倒序）

4. **自适应机制**：
   - FIFO/LIFO动态切换（基于延迟）
   - epoch批处理（时间粒度控制）
   - 租户公平调度（权重分配）

5. **异步准入**：
   - 复制工作立即返回
   - 后台granter处理
   - 通过回调返还flow tokens

### 设计精华

1. **公平与效率平衡**：
   - 租户间公平：used/weight排序
   - 租户内公平：优先级+时间排序
   - 快速路径：牺牲部分公平换取低延迟

2. **过载自适应**：
   - FIFO→LIFO切换：过载时优先完成新请求
   - epoch机制：避免完全LIFO的激进性
   - 授权链：自然节流，考虑调度器能力

3. **竞态处理**：
   - 快速路径的锁竞态：容忍短暂不公平
   - 取消竞态：必须处理已授权的情况
   - 租户GC竞态：避免修改已GC的tenant

### 实践建议

1. **调用Admit时**：
   - 总是检查返回的`enabled`和`err`
   - `enabled=true && err==nil`才调用AdmittedWorkDone()
   - 设置合理的context deadline

2. **性能优化**：
   - 尽量减少排队：合理配置slots/tokens
   - 避免长时间持有资源：及时调用AdmittedWorkDone()
   - 监控指标：等待时间、队列长度、旁路率

3. **故障诊断**：
   - 检查准入控制是否启用
   - 查看queueKind定位具体队列
   - 分析等待时间分布识别瓶颈

---

**参考源代码位置**：
- Admit函数：[pkg/util/admission/work_queue.go:571-847](pkg/util/admission/work_queue.go#L571-L847)
- WorkInfo定义：[pkg/util/admission/work_queue.go:162-190](pkg/util/admission/work_queue.go#L162-L190)
- tenantInfo结构：[pkg/util/admission/work_queue.go:967-1023](pkg/util/admission/work_queue.go#L967-L1023)
- granted函数：[pkg/util/admission/work_queue.go:883-960](pkg/util/admission/work_queue.go#L883-L960)

**本章完**
