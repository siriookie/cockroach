# 第七章：WorkQueue 详解——多租户公平调度的艺术

## 引言

在分布式数据库的准入控制系统中,最复杂也最关键的组件之一就是 **WorkQueue**(工作队列)。它不仅要管理成千上万个等待执行的请求,还要在多个租户之间实现公平调度,同时支持优先级排序、防止饥饿、动态切换调度策略(FIFO/LIFO)等高级功能。

本章将深入剖析 CockroachDB 的 WorkQueue 实现,看看它如何在复杂的多租户环境中实现既公平又高效的资源调度。

## 一、WorkQueue 的设计目标

### 1.1 多租户公平性

在 SaaS 模式下,一个 CockroachDB 集群可能服务数百个租户:

```
问题场景:
租户 A (大客户): 每秒 10000 个请求
租户 B (小客户): 每秒 10 个请求
租户 C (中等客户): 每秒 100 个请求

如果简单的 FIFO 队列:
  → 租户 A 的请求占据 99% 的资源
  → 租户 B 和 C 被"饿死"(starved)
  → 违反了 SLA (Service Level Agreement)
```

**WorkQueue 的解决方案**:
```
使用租户堆 (Tenant Heap):
  - 按 used/weight 排序
  - used = 已使用的资源(slots/tokens)
  - weight = 租户权重(可配置)
  - 最少使用的租户优先获得服务

效果:
租户 A: used=1000, weight=1 → ratio=1000
租户 B: used=10, weight=1 → ratio=10   ← 优先服务!
租户 C: used=100, weight=1 → ratio=100

即使租户 A 提交了更多请求,
队列仍然保证租户 B 和 C 获得公平的资源份额
```

### 1.2 优先级调度

在同一个租户内部,不同请求有不同的重要性:

```
优先级层次:
  0 (Admissionpb.NormalPri)    - 用户交互查询
  1 (Admissionpb.BulkNormalPri) - 后台批处理
  2 (Admissionpb.LowPri)       - 索引重建、Schema 变更

要求:
  - 高优先级请求先执行
  - 但低优先级请求不能完全饿死
```

**WorkQueue 的方案**:
- 每个租户内部维护一个**优先级堆**(Priority Heap)
- 结合 **Epoch-LIFO** 机制防止低优先级请求饥饿

### 1.3 动态调度策略切换

根据系统负载自动切换 FIFO 和 LIFO:

```
FIFO (First-In-First-Out):
  正常负载下使用
  ┌─────┬─────┬─────┬─────┐
  │ Req1│ Req2│ Req3│ Req4│ → 先进先出
  └─────┴─────┴─────┴─────┘

LIFO (Last-In-First-Out):
  过载时使用,快速失败(fail-fast)
  ┌─────┬─────┬─────┬─────┐
  │ Req1│ Req2│ Req3│ Req4│ ← 后进先出
  └─────┴─────┴─────┴─────┘
  Req4 先执行,Req1-3 超时失败

为什么过载时用 LIFO?
  - 老请求可能已经超时,执行也是浪费资源
  - 新请求更可能在 deadline 内完成
  - 提高"有效吞吐量"(不浪费资源在注定失败的请求上)
```

## 二、WorkQueue 的核心数据结构

### 2.1 整体架构

```
WorkQueue
├─ mu.tenants: map[uint64]*tenantInfo  (所有租户)
│
├─ mu.tenantHeap: []*tenantInfo       (有等待请求的租户,按 used/weight 排序)
│     │
│     ├─ tenantInfo(租户1)
│     │    ├─ waitingWorkHeap        (FIFO 队列,按优先级+CreateTime排序)
│     │    ├─ openEpochsHeap         (LIFO 队列,按 epoch 倒序排序)
│     │    ├─ used: 1000             (已使用资源)
│     │    ├─ weight: 1              (租户权重)
│     │    └─ fifoPriorityThreshold  (FIFO/LIFO 切换阈值)
│     │
│     ├─ tenantInfo(租户2)
│     │    └─ ... (同上)
│     │
│     └─ tenantInfo(租户3)
│          └─ ... (同上)
│
└─ granter: elasticCPUGranter / slotGranter (资源授权器)
```

### 2.2 tenantInfo 结构详解

```go
// pkg/util/admission/work_queue.go:1312

type tenantInfo struct {
    id     uint64  // 租户 ID (系统租户 = 1)
    weight uint32  // 租户权重,用于加权公平调度 (默认为 1)

    // used: 已使用的资源量
    // - 对于 slots: used = CPU 时间(纳秒)
    // - 对于 tokens: used = token 数量
    //
    // 每秒重置一次,确保公平性不会被历史使用量"污染"
    used uint64

    // 两个堆:一个 FIFO,一个 LIFO
    waitingWorkHeap waitingWorkHeap  // 优先级堆(最小堆,priority 越小越优先)
    openEpochsHeap  openEpochsHeap   // Epoch 堆(LIFO,用于过载时)

    // 优先级状态跟踪
    priorityStates priorityStates

    // FIFO/LIFO 切换阈值
    // priority >= fifoPriorityThreshold → FIFO
    // priority <  fifoPriorityThreshold → LIFO
    fifoPriorityThreshold int

    heapIndex int  // 在 tenantHeap 中的索引 (-1 = 不在堆中)
}
```

**关键设计点**:

1. **`used` 字段的动态重置**:
   ```go
   // 每秒执行一次 (后台 goroutine)
   func (q *WorkQueue) gcTenantsAndResetUsed() {
       q.mu.Lock()
       defer q.mu.Unlock()

       for tenantID, tenant := range q.mu.tenants {
           tenant.used = 0  // 重置使用量

           if tenant.used == 0 && !isInTenantHeap(tenant) {
               // 既没有等待请求,也没有使用资源 → GC
               delete(q.mu.tenants, tenantID)
               releaseTenantInfo(tenant)  // 返回对象池
           }
       }
   }
   ```

   **为什么要重置?**
   - 避免"长期记忆":如果不重置,租户 A 上午使用了很多资源,下午即使空闲也会被"惩罚"
   - 1 秒的周期:既保证短期公平性,又不会频繁波动

2. **双堆结构**:
   ```
   情况 A: 系统负载正常
     waitingWorkHeap 使用中 (FIFO)
     openEpochsHeap  空

   情况 B: 系统过载 (检测到队列延迟 > threshold)
     低优先级请求 → openEpochsHeap (LIFO)
     高优先级请求 → waitingWorkHeap (仍然 FIFO)
   ```

### 2.3 租户堆的排序规则

```go
// pkg/util/admission/work_queue.go:1400

func (th tenantHeap) Less(i, j int) bool {
    // 计算 ratio = used / weight
    iRatio := float64(th[i].used) / float64(th[i].weight)
    jRatio := float64(th[j].used) / float64(th[j].weight)

    // ratio 越小越优先 (最小堆)
    if iRatio != jRatio {
        return iRatio < jRatio
    }

    // ratio 相同时,按租户 ID 排序 (确保稳定性)
    return th[i].id < th[j].id
}
```

**实际例子**:

```
初始状态:
┌─────────┬──────┬────────┬────────────┬──────────┐
│ 租户 ID │ used │ weight │   ratio    │ 堆索引   │
├─────────┼──────┼────────┼────────────┼──────────┤
│    1    │ 1000 │   1    │  1000.0    │    2     │
│    2    │  10  │   1    │   10.0     │    0  ←  │ (堆顶,最优先)
│    3    │  100 │   2    │   50.0     │    1     │
└─────────┴──────┴────────┴────────────┴──────────┘

授权一个请求给租户 2 (消耗 50 tokens):
租户 2: used = 10 + 50 = 60
        ratio = 60 / 1 = 60.0

重新排序:
┌─────────┬──────┬────────┬────────────┬──────────┐
│ 租户 ID │ used │ weight │   ratio    │ 堆索引   │
├─────────┼──────┼────────┼────────────┼──────────┤
│    3    │  100 │   2    │   50.0     │    0  ←  │ (新的堆顶)
│    2    │  60  │   1    │   60.0     │    1     │
│    1    │ 1000 │   1    │  1000.0    │    2     │
└─────────┴──────┴────────┴────────────┴──────────┘

下一个请求授权给租户 3
```

**加权公平性示例**:

```
场景: 不同级别的付费用户
租户 A (企业客户): weight = 10
租户 B (标准客户): weight = 1
租户 C (免费用户): weight = 1

假设三者各消耗了 1000 tokens:
  租户 A: ratio = 1000 / 10 = 100
  租户 B: ratio = 1000 / 1  = 1000
  租户 C: ratio = 1000 / 1  = 1000

结果:
  - 租户 A 优先 (企业客户享有 10倍权重)
  - 租户 B 和 C 平等对待
```

## 三、请求入队流程 (Admit)

### 3.1 快速路径 (Fast Path)

```go
// pkg/util/admission/work_queue.go:634

if len(q.mu.tenantHeap) == 0 && !q.knobs.DisableWorkQueueFastPath {
    // 队列为空,尝试快速路径
    tenant.used += uint64(info.RequestedCount)
    q.mu.Unlock()

    if q.granter.tryGet(canBurst, info.RequestedCount) {
        // 成功获取 token/slot,立即返回
        q.metrics.incAdmitted(info.Priority)
        return true, nil
    }

    // 失败,进入慢速路径(需要排队)
    q.mu.Lock()
    ...
}
```

**快速路径的意义**:
```
正常情况 (90% 的时间):
  队列为空 → 直接尝试获取资源 → 立即返回
  ✅ 延迟: ~1μs (无需排队,无需上下文切换)

过载情况 (10% 的时间):
  队列非空 → 进入慢速路径 → 加入队列等待
  ⏱ 延迟: 几毫秒到几秒
```

**注意事项**:
```go
// 快速路径有一个微小的竞态条件:
//
// 时刻 t0:
//   Goroutine A 检查 len(tenantHeap)==0 ✅
//   Goroutine A 释放锁
//
// 时刻 t1:
//   Goroutine B 也检查 len(tenantHeap)==0 ✅
//   Goroutine B 也释放锁
//
// 时刻 t2:
//   Goroutine A 调用 tryGet() → 成功
//   Goroutine B 调用 tryGet() → 成功
//
// 结果:可能超发资源 (over-admission)

// 为什么可以接受?
//   1. 概率极低 (需要精确的时序)
//   2. 影响可控 (最多多授权几个请求)
//   3. GrantCoordinator 会定期检查并调整
```

### 3.2 慢速路径 (Slow Path)

```go
// pkg/util/admission/work_queue.go:727

// 步骤 1: 确定调度策略 (FIFO vs LIFO)
ordering := fifoWorkOrdering
if int(info.Priority) < tenant.fifoPriorityThreshold {
    ordering = lifoWorkOrdering
}

// 步骤 2: 创建等待请求对象
work := newWaitingWork(
    info.Priority,
    ordering,
    info.CreateTime,     // 用于 FIFO 排序
    info.RequestedCount,
    startTime,           // 入队时间,用于计算等待时长
    q.mu.epochLengthNanos,
)
work.replicated = info.ReplicatedWorkInfo

// 步骤 3: 加入相应的堆
inTenantHeap := isInTenantHeap(tenant)
if work.epoch <= q.mu.closedEpochThreshold || ordering == fifoWorkOrdering {
    heap.Push(&tenant.waitingWorkHeap, work)  // FIFO 堆
} else {
    heap.Push(&tenant.openEpochsHeap, work)   // LIFO 堆
}

// 步骤 4: 如果租户不在租户堆中,加入租户堆
if !inTenantHeap {
    heap.Push(&q.mu.tenantHeap, tenant)
}

q.mu.Unlock()
```

**数据流动图**:

```
请求到达
    ↓
检查队列是否为空
    ├─ 是 → 快速路径 → tryGet() → 成功 → 返回
    └─ 否 → 慢速路径
              ↓
        确定调度策略 (FIFO/LIFO)
              ↓
        创建 waitingWork 对象
              ↓
        ┌────────────────────────────────┐
        │ 加入租户的等待堆:               │
        │  - FIFO → waitingWorkHeap      │
        │  - LIFO → openEpochsHeap       │
        └────────────────────────────────┘
              ↓
        租户不在 tenantHeap?
              ├─ 是 → Push 到 tenantHeap
              └─ 否 → (已在堆中,无需操作)
              ↓
        阻塞等待 (select on work.ch)
```

### 3.3 等待与唤醒

```go
// pkg/util/admission/work_queue.go:767

defer releaseWaitingWork(work)  // 确保释放对象回池

select {
case <-ctx.Done():
    // 情况 1: 上下文取消 (超时或主动取消)
    waitDur := q.timeNow().Sub(startTime)
    q.mu.Lock()

    if work.heapIndex == -1 {
        // 与授权竞态,已经被授权了
        // 需要归还 token/slot
        q.mu.Unlock()
        q.granter.returnGrant(info.RequestedCount)
        chainID := <-work.ch
        q.granter.continueGrantChain(chainID)
    } else {
        // 仍在堆中,手动移除
        if work.inWaitingWorkHeap {
            tenant.waitingWorkHeap.remove(work)
        } else {
            tenant.openEpochsHeap.remove(work)
        }
        if !isInTenantHeap(tenant) {
            q.mu.tenantHeap.remove(tenant)
        }
        q.mu.Unlock()
    }

    q.metrics.incErrored(info.Priority)
    return true, errors.Newf("deadline expired while waiting in queue...")

case chainID, ok := <-work.ch:
    // 情况 2: 被 granted() 唤醒
    if !ok {
        panic("channel should not be closed")
    }
    q.metrics.incAdmitted(info.Priority)
    waitDur := q.timeNow().Sub(startTime)
    q.metrics.recordFinishWait(info.Priority, waitDur)

    if work.heapIndex != -1 {
        panic("grantee should be removed from heap")
    }

    q.granter.continueGrantChain(chainID)
    return true, nil
}
```

**超时处理的竞态条件**:

```
时间线:
t=0:    请求入队,开始等待
        select { case <-ctx.Done(): ... case <-work.ch: ... }

t=100ms: 后台 tryGrant() 触发
         granted() 调用 → work.ch <- chainID
         但此时 select 尚未执行到 case <-work.ch

t=101ms: ctx 超时触发
         select 进入 case <-ctx.Done() 分支

问题:
  - granted() 已经授权,但 Admit() 以为超时了
  - 如果不处理,会导致资源泄漏

解决方案:
  检查 work.heapIndex:
    - heapIndex == -1: 已被 granted() 移除
    - heapIndex != -1: 仍在堆中,真的超时了
```

## 四、请求出队流程 (granted)

### 4.1 核心逻辑

```go
// pkg/util/admission/work_queue.go:883

func (q *WorkQueue) granted(grantChainID grantChainID) int64 {
    now := q.timeNow()
    q.mu.Lock()

    // 步骤 1: 检查队列是否为空
    if len(q.mu.tenantHeap) == 0 {
        q.mu.Unlock()
        return 0  // 无等待请求,拒绝授权
    }

    // 步骤 2: 从租户堆顶部取出 used/weight 最小的租户
    tenant := q.mu.tenantHeap[0]

    // 步骤 3: 从该租户的等待堆中 Pop 出最高优先级的请求
    var item *waitingWork
    if len(tenant.waitingWorkHeap) > 0 {
        item = heap.Pop(&tenant.waitingWorkHeap).(*waitingWork)
    } else {
        item = heap.Pop(&tenant.openEpochsHeap).(*waitingWork)
    }

    // 步骤 4: 更新统计信息
    waitDur := now.Sub(item.enqueueingTime)
    tenant.priorityStates.updateDelayLocked(item.priority, waitDur, false)
    tenant.used += uint64(item.requestedCount)

    // 步骤 5: 重新平衡租户堆 (因为 used 增加了)
    if isInTenantHeap(tenant) {
        q.mu.tenantHeap.fix(tenant)
    } else {
        q.mu.tenantHeap.remove(tenant)
    }

    requestedCount := item.requestedCount
    q.mu.Unlock()

    // 步骤 6: 唤醒等待的 goroutine (在锁外发送,避免死锁)
    item.ch <- grantChainID

    // 步骤 7: 返回实际消耗的 token 数量
    return requestedCount
}
```

### 4.2 授权流程图

```
tryGrant() 调用
    ↓
WorkQueue.granted(grantChainID)
    ↓
┌─────────────────────────────────────────────────────┐
│ 1. 检查 tenantHeap 是否为空                          │
│    Empty → return 0                                 │
└─────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────┐
│ 2. 取出堆顶租户 (used/weight 最小)                   │
│    tenant := tenantHeap[0]                          │
│                                                     │
│    假设: 租户 2 (ratio=10)                          │
└─────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────┐
│ 3. 从该租户的等待堆中 Pop 请求                        │
│    优先级: waitingWorkHeap (FIFO)                    │
│    备选: openEpochsHeap (LIFO)                      │
│                                                     │
│    假设 Pop: Request_A (priority=0, reqCount=100)   │
└─────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────┐
│ 4. 更新租户使用量                                     │
│    tenant.used += 100                               │
│    (10 → 110)                                       │
└─────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────┐
│ 5. 重新平衡 tenantHeap                              │
│    fix(tenant) 调整堆                                │
│                                                     │
│    之前: [租户2, 租户3, 租户1]                       │
│    之后: [租户3, 租户2, 租户1]                       │
│           (因为租户2的ratio从10变成110)              │
└─────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────┐
│ 6. 唤醒 Request_A 的 goroutine                       │
│    item.ch <- grantChainID                          │
│                                                     │
│    Admit() 中的 select 收到信号,停止阻塞             │
└─────────────────────────────────────────────────────┘
    ↓
return 100 (requestedCount)
```

### 4.3 多级调度示例

```
初始状态:
┌──────────┬────────┬───────────────────────────────────┐
│ 租户堆   │ ratio  │ 等待请求堆                         │
├──────────┼────────┼───────────────────────────────────┤
│ 租户 A   │  50    │ [Req_A1(pri=0), Req_A2(pri=1)]    │ ← 堆顶
│ 租户 B   │  100   │ [Req_B1(pri=0)]                   │
│ 租户 C   │  500   │ [Req_C1(pri=2), Req_C2(pri=0)]    │
└──────────┴────────┴───────────────────────────────────┘

第 1 次 granted() 调用:
  ① 选择租户 A (ratio 最小)
  ② Pop Req_A1 (priority=0,最高优先级)
  ③ 租户 A.used += 50 → ratio = 100
  ④ 重新排序 tenantHeap

授权后状态:
┌──────────┬────────┬───────────────────────────────────┐
│ 租户堆   │ ratio  │ 等待请求堆                         │
├──────────┼────────┼───────────────────────────────────┤
│ 租户 A   │  100   │ [Req_A2(pri=1)]                   │ ← 堆顶(与B并列)
│ 租户 B   │  100   │ [Req_B1(pri=0)]                   │
│ 租户 C   │  500   │ [Req_C1(pri=2), Req_C2(pri=0)]    │
└──────────┴────────┴───────────────────────────────────┘

第 2 次 granted() 调用:
  ① 租户 A 和 B 的 ratio 相同,按 ID 排序
  ② 选择 ID 较小的租户 (假设 A < B)
  ③ Pop Req_A2 (priority=1)
  ④ 租户 A.used += 50 → ratio = 150

授权后状态:
┌──────────┬────────┬───────────────────────────────────┐
│ 租户堆   │ ratio  │ 等待请求堆                         │
├──────────┼────────┼───────────────────────────────────┤
│ 租户 B   │  100   │ [Req_B1(pri=0)]                   │ ← 堆顶
│ 租户 A   │  150   │ []                                │
│ 租户 C   │  500   │ [Req_C1(pri=2), Req_C2(pri=0)]    │
└──────────┴────────┴───────────────────────────────────┘

第 3 次 granted() 调用:
  ① 选择租户 B (ratio 最小)
  ② Pop Req_B1
  ③ 租户 B 的等待堆变空 → 从 tenantHeap 移除
  ④ 租户 B.used += 50 → ratio = 150

授权后状态:
┌──────────┬────────┬───────────────────────────────────┐
│ 租户堆   │ ratio  │ 等待请求堆                         │
├──────────┼────────┼───────────────────────────────────┤
│ 租户 C   │  500   │ [Req_C1(pri=2), Req_C2(pri=0)]    │ ← 堆顶
└──────────┴────────┴───────────────────────────────────┘
(租户 A 和 B 不在堆中,因为没有等待请求)
```

**关键观察**:
- 租户 A 的两个请求连续被授权 (因为 ratio 最小)
- 即使租户 C 有高优先级请求 (Req_C2, pri=0),也要等待公平轮次
- 这确保了租户间的公平性

## 五、Epoch-LIFO 机制详解

### 5.1 什么是 Epoch-LIFO?

```
Epoch (时代):
  - 时间被划分为固定长度的"时代"(如 100ms)
  - Epoch 0: [0ms, 100ms)
  - Epoch 1: [100ms, 200ms)
  - Epoch 2: [200ms, 300ms)
  - ...

LIFO 策略:
  当系统过载时,低优先级请求采用 LIFO 排序:
  - 最新到达的请求先执行
  - 老请求可能超时失败
  - 目标:提高"有效吞吐量"
```

**为什么不直接用 LIFO?**
```
问题:纯 LIFO 会导致"饥饿"
  - Epoch 0 的请求永远排在后面
  - 如果系统持续过载,这些请求永远不会被执行
  - 违反了公平性原则

解决方案:Epoch-LIFO
  - 请求按 Epoch 分组
  - 同一 Epoch 内采用 LIFO
  - 但旧 Epoch 的请求最终会被"关闭"并移到 FIFO 堆
```

### 5.2 Epoch 关闭机制

```go
// pkg/util/admission/work_queue.go:500

func (q *WorkQueue) tryCloseEpoch(timeNow time.Time) {
    q.mu.Lock()
    defer q.mu.Unlock()

    // 计算应该关闭的 Epoch
    // 例如:当前时间 = 350ms, epochLength = 100ms, delta = 10ms
    // epochClosingTime = 350 - 100 - 10 = 240ms
    // epoch = 240 / 100 = 2
    epochClosingTimeNanos := timeNow.UnixNano() -
        q.mu.epochLengthNanos - q.mu.epochClosingDeltaNanos
    epoch := epochForTimeNanos(epochClosingTimeNanos, q.mu.epochLengthNanos)

    if epoch <= q.mu.closedEpochThreshold {
        return  // 已经关闭过了
    }

    q.mu.closedEpochThreshold = epoch

    // 遍历所有租户,将关闭的 Epoch 中的请求移到 FIFO 堆
    for _, tenant := range q.mu.tenants {
        for len(tenant.openEpochsHeap) > 0 {
            work := tenant.openEpochsHeap[0]
            if work.epoch > epoch {
                break  // 还没到关闭时间
            }
            heap.Pop(&tenant.openEpochsHeap)
            heap.Push(&tenant.waitingWorkHeap, work)  // 移到 FIFO 堆
        }
    }
}
```

**时间线示例**:

```
t=0ms:   Req1 到达, epoch=0 → openEpochsHeap
         [Req1(epoch=0)]

t=50ms:  Req2 到达, epoch=0 → openEpochsHeap
         [Req2(epoch=0), Req1(epoch=0)]  (LIFO 顺序)

t=100ms: Req3 到达, epoch=1 → openEpochsHeap
         [Req3(epoch=1), Req2(epoch=0), Req1(epoch=0)]

t=150ms: Req4 到达, epoch=1 → openEpochsHeap
         [Req4(epoch=1), Req3(epoch=1), Req2(epoch=0), Req1(epoch=0)]

t=210ms: tryCloseEpoch() 触发
         closedEpochThreshold = 0
         → Req1, Req2 移到 waitingWorkHeap (FIFO)

         openEpochsHeap:  [Req4(epoch=1), Req3(epoch=1)]
         waitingWorkHeap: [Req1, Req2]

t=310ms: tryCloseEpoch() 触发
         closedEpochThreshold = 1
         → Req3, Req4 移到 waitingWorkHeap

         openEpochsHeap:  []
         waitingWorkHeap: [Req1, Req2, Req3, Req4]
```

**防止饥饿**:
- 即使系统持续过载,老请求也会在 1-2 个 Epoch 后移到 FIFO 堆
- 确保最终被执行(或至少有机会超时失败)

### 5.3 动态切换 FIFO/LIFO

```go
// pkg/util/admission/work_queue.go:728

ordering := fifoWorkOrdering
if int(info.Priority) < tenant.fifoPriorityThreshold {
    ordering = lifoWorkOrdering
}
```

**fifoPriorityThreshold 的动态调整**:

```go
// 每次 Epoch 关闭时重新计算
tenant.fifoPriorityThreshold =
    tenant.priorityStates.getFIFOPriorityThresholdAndReset(
        tenant.fifoPriorityThreshold,
        q.mu.epochLengthNanos,
        q.mu.maxQueueDelayToSwitchToLifo,
    )
```

**逻辑**:
```
对于每个优先级:
  if 平均等待时间 > maxQueueDelayToSwitchToLifo (默认 100ms):
    → 该优先级切换到 LIFO
  else:
    → 保持 FIFO

例如:
  Priority 0 (NormalPri):   平均延迟 = 50ms  → FIFO
  Priority 1 (BulkNormalPri): 平均延迟 = 150ms → LIFO
  Priority 2 (LowPri):      平均延迟 = 500ms → LIFO

  → fifoPriorityThreshold = 1
  → Priority >= 1 的请求使用 FIFO
  → Priority < 1 的请求使用 LIFO (实际上没有,因为 0 是最高的)
```

**自适应行为**:
```
场景 A: 系统负载正常
  所有优先级的延迟都 < 100ms
  → fifoPriorityThreshold = int(LowPri) (所有请求都FIFO)

场景 B: 系统轻度过载
  Priority 2 的延迟 > 100ms
  → fifoPriorityThreshold = 2 (只有 LowPri 使用 LIFO)

场景 C: 系统严重过载
  Priority 1, 2 的延迟都 > 100ms
  → fifoPriorityThreshold = 1 (BulkNormalPri 和 LowPri 使用 LIFO)
```

## 六、性能优化技巧

### 6.1 对象池 (sync.Pool)

```go
var tenantInfoPool = sync.Pool{
    New: func() interface{} {
        return &tenantInfo{}
    },
}

var waitingWorkPool = sync.Pool{
    New: func() interface{} {
        return &waitingWork{ch: make(chan grantChainID, 1)}
    },
}
```

**好处**:
- 减少 GC 压力:高频创建/销毁的对象重用
- 降低延迟:避免内存分配的开销

**使用模式**:
```go
// 获取
tenant := tenantInfoPool.Get().(*tenantInfo)
*tenant = tenantInfo{id: 123, ...}  // 重置字段

// 归还
releaseTenantInfo(tenant)  // → tenantInfoPool.Put(tenant)
```

### 6.2 锁的精细控制

```go
// 反模式:持有锁发送 channel
q.mu.Lock()
item.ch <- grantChainID  // ❌ 可能阻塞,持有锁时间过长
q.mu.Unlock()

// 正确模式:释放锁后发送
q.mu.Unlock()
item.ch <- grantChainID  // ✅ 不持有锁
```

**原因**:
- channel 发送可能阻塞(如果接收方还没准备好)
- 持有锁时阻塞会导致其他 goroutine 饥饿

### 6.3 堆操作的批量化

```go
// 关闭 Epoch 时,批量移动请求
for len(tenant.openEpochsHeap) > 0 {
    work := tenant.openEpochsHeap[0]
    if work.epoch > epoch {
        break
    }
    heap.Pop(&tenant.openEpochsHeap)
    heap.Push(&tenant.waitingWorkHeap, work)
}
```

**优化点**:
- 一次性移动多个请求,减少锁获取次数
- 避免频繁的堆重建

## 七、监控与调试

### 7.1 关键指标

```promql
# 等待队列的长度
admission.work_queue.length

# 等待时间的 P99
admission.work_queue.wait_durations.p99

# 快速路径的命中率
admission.work_queue.fast_path_admitted / admission.work_queue.admitted

# 租户公平性指标
admission.work_queue.tenant_used_ratio
```

**健康状态**:
```
queue.length < 100           ✅ 队列短
wait_durations.p99 < 10ms    ✅ 延迟低
fast_path_ratio > 90%        ✅ 大部分请求无需排队
```

**异常状态**:
```
queue.length > 1000          ❌ 队列积压
wait_durations.p99 > 1s      ❌ 延迟高
fast_path_ratio < 50%        ❌ 过载
→ 可能需要增加资源或调整配额
```

### 7.2 日志调试

启用详细日志:

```bash
cockroach start --vmodule=work_queue=1 ...
```

日志示例:

```
I250105 12:34:56.789 admission/work_queue.go:542]
  kv-regular-cpu-queue: FIFO threshold for tenant 1 changed to 2

I250105 12:34:56.790 admission/work_queue.go:657]
  fast-path: admitting t1 pri=NormalPri

I250105 12:34:56.791 admission/work_queue.go:756]
  async-path: len(waiting-work)=15: enqueued t2 pri=BulkNormalPri
```

### 7.3 火焰图分析

使用 pprof 分析热点:

```bash
go tool pprof http://localhost:8080/debug/pprof/profile
(pprof) top 10

# 期望看到:
# WorkQueue.Admit       5%
# WorkQueue.granted     3%
# heap.Push/Pop         2%
```

如果 WorkQueue 的 CPU 占比 > 10%,可能的原因:
- 队列过长 (堆操作成本高)
- 锁竞争严重
- 需要优化堆数据结构

## 八、总结

### WorkQueue 的核心价值

1. **多租户公平性**: 使用 used/weight 堆确保资源公平分配
2. **优先级调度**: 每个租户内部按优先级排序
3. **自适应策略**: FIFO/LIFO 动态切换,过载时快速失败
4. **防止饥饿**: Epoch-LIFO 确保老请求最终被执行
5. **高性能**: 快速路径、对象池、精细锁控制

### 设计模式总结

```
┌─────────────────────────────────────────────────────────┐
│              WorkQueue 的多层堆结构                      │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  第 1 层: 租户堆 (Tenant Heap)                           │
│    按 used/weight 排序,实现跨租户公平性                   │
│                                                          │
│  第 2 层: 优先级堆 (Priority Heap)                        │
│    每个租户内部按优先级排序,实现租户内公平性               │
│                                                          │
│  第 3 层: FIFO/LIFO 堆 (Ordering Heap)                   │
│    根据负载自适应选择调度策略                             │
│                                                          │
│  第 4 层: Epoch 机制 (Epoch Heap)                        │
│    防止 LIFO 导致的饥饿问题                               │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### 实战建议

1. **租户权重配置**:
   ```sql
   -- 企业客户享有 10 倍权重
   ALTER TENANT enterprise SET weight = 10;
   ```

2. **Epoch-LIFO 参数调优**:
   ```sql
   -- 增加 Epoch 长度,减少切换频率
   SET CLUSTER SETTING admission.epoch_lifo.epoch_duration = '200ms';

   -- 降低 LIFO 切换阈值,更早启用 LIFO
   SET CLUSTER SETTING admission.epoch_lifo.queue_delay_threshold_to_switch_to_lifo = '50ms';
   ```

3. **监控队列健康度**:
   ```sql
   SELECT * FROM crdb_internal.node_metrics
   WHERE name LIKE 'admission.work_queue%';
   ```

通过理解 WorkQueue 的内部机制,我们可以更好地诊断准入控制相关的性能问题,优化多租户环境下的资源调度,确保系统在高负载下仍能公平、高效地服务所有租户!
