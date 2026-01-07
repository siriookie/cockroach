# 第六章补充：tryGrant() 机制详解——准入控制的心跳触发器

## 引言

在第六章的 `SchedulerLatency()` 函数分析中,我们看到最后有一段看似简单但实际非常关键的代码:

```go
if e.coord != nil { // only nil in tests
    // TODO(irfansharif): Right now this is the only ticking mechanism for
    // elastic CPU grants; consider some form of explicit ticking instead.
    // We have this need for fine-granularity explicit ticking for the IO
    // tokens too, where the 250ms granularity is too coarse. Ideally a 1ms
    // granularity would be good. We've had problems with that in unloaded
    // systems, see the samplePeriod{Short,Long} logic goschedstats, so
    // maybe we can generalize that period switching into a struct where the
    // coarser period is used only when some func indicates that the
    // relevant "resource" is underloaded -- for goschedstats this resource
    // is the CPU, and for these token buckets it will be based on how many
    // tokens are still available.
    e.coord.tryGrant()
}
```

这段代码的背后隐藏着 CockroachDB 准入控制系统的一个核心机制:**事件驱动的准入放行**。本章将深入剖析这个"心跳触发器"的工作原理。

## 一、为什么需要 tryGrant()?

### 1.1 准入控制的困境

想象一个场景:

```
时刻 t0:
  - 调度器延迟 P99 = 80ms (超过目标 50ms)
  - Elastic CPU 配额 = 50%
  - 状态机决定: 降低配额到 48% ❌

时刻 t0 + 1μs:
  - 新配额已设置: 48%
  - WorkQueue 中有 100 个等待的 elastic 请求
  - 问题: 谁来通知这些请求"现在有新配额了,快来领取"?
```

如果没有主动的"通知"机制,这些等待的请求会:
- ❌ 无限期地等待,直到有新的请求到来触发检查
- ❌ 配额虽然降低了,但没有被实际使用(资源浪费)
- ❌ 延迟指标持续恶化,形成"死锁"

**tryGrant() 的作用**:
> 每次调度器延迟回调触发时,**主动尝试**将等待队列中的请求授权运行,充当"心跳触发器"的角色。

### 1.2 事件驱动 vs 轮询

CockroachDB 的准入控制采用**事件驱动**设计,而非定时轮询:

```
❌ 轮询方式 (效率低):
while true:
    sleep(1ms)  ← 浪费 CPU,延迟高
    if hasWaitingRequests() && hasAvailableTokens():
        grant()

✅ 事件驱动 (高效):
当以下事件发生时,调用 tryGrant():
  1. 调度器延迟回调触发 ← 我们正在讨论的
  2. 请求完成归还 token
  3. 配额动态调整
  4. 新请求入队
```

**优势**:
- ⚡ 零延迟响应:事件发生的瞬间就尝试授权
- 💰 零 CPU 开销:没有事件时不消耗 CPU
- 🎯 精准触发:只在有意义的时刻检查

## 二、tryGrant() 的调用链路

### 2.1 完整的函数调用栈

```
┌────────────────────────────────────────────────────────────────┐
│  pkg/util/schedulerlatency/sampler.go                         │
│  StartSampler() 后台 goroutine                                 │
│    ↓ 每 100ms 采样一次调度器延迟                                │
│    └─→ callback.SchedulerLatency(p99, period)                 │
└────────────────────────────────────────────────────────────────┘
                           ↓
┌────────────────────────────────────────────────────────────────┐
│  pkg/util/admission/scheduler_latency_listener.go             │
│  schedulerLatencyListener.SchedulerLatency(p99, period)       │
│    ↓                                                           │
│    ├─ 1. 计算新的 CPU 配额 (基于状态机)                          │
│    ├─ 2. e.elasticCPULimiter.setUtilizationLimit(newLimit)   │
│    ├─ 3. e.elasticCPULimiter.computeUtilizationMetric()      │
│    └─ 4. e.coord.tryGrant() ← 触发准入控制                     │
└────────────────────────────────────────────────────────────────┘
                           ↓
┌────────────────────────────────────────────────────────────────┐
│  pkg/util/admission/elastic_cpu_grant_coordinator.go          │
│  ElasticCPUGrantCoordinator.tryGrant()                        │
│    ↓                                                           │
│    └─→ e.elasticCPUGranter.tryGrant()                         │
└────────────────────────────────────────────────────────────────┘
                           ↓
┌────────────────────────────────────────────────────────────────┐
│  pkg/util/admission/elastic_cpu_granter.go                    │
│  elasticCPUGranter.tryGrant()                                 │
│    ↓                                                           │
│    for e.hasWaitingRequests() && e.tryGet(canBurst, 1) {     │
│        tokens := e.requester.granted(noGrantChain)            │
│        if tokens == 0 { return } // requester 拒绝             │
│        if tokens > 1 { e.tookWithoutPermission(tokens-1) }   │
│    }                                                           │
└────────────────────────────────────────────────────────────────┘
                           ↓
┌────────────────────────────────────────────────────────────────┐
│  pkg/util/admission/work_queue.go                             │
│  WorkQueue.granted(grantChainID)                              │
│    ↓                                                           │
│    ├─ 1. 从优先级堆中 Pop 出最高优先级的等待请求                   │
│    ├─ 2. 更新租户使用量统计                                      │
│    ├─ 3. item.ch <- grantChainID (唤醒等待的 goroutine)        │
│    └─ 4. 返回请求所需的 token 数量                              │
└────────────────────────────────────────────────────────────────┘
                           ↓
┌────────────────────────────────────────────────────────────────┐
│  用户 goroutine (被阻塞在 Admit() 调用中)                       │
│    ↓                                                           │
│    select {                                                    │
│        case <-item.ch:  ← 收到信号,停止阻塞                     │
│            // 获得了运行许可,开始执行                            │
│            return nil                                          │
│    }                                                           │
└────────────────────────────────────────────────────────────────┘
```

### 2.2 核心代码详解

#### 步骤 1: ElasticCPUGrantCoordinator.tryGrant()

```go
// pkg/util/admission/elastic_cpu_grant_coordinator.go:83

func (e *ElasticCPUGrantCoordinator) tryGrant() {
    e.elasticCPUGranter.tryGrant()  // 简单的委托调用
}
```

这是一个简单的转发函数,真正的逻辑在 `elasticCPUGranter` 中。

#### 步骤 2: elasticCPUGranter.tryGrant()

```go
// pkg/util/admission/elastic_cpu_granter.go:180

func (e *elasticCPUGranter) tryGrant() {
    // 循环尝试授权,直到:
    //   1. 没有等待的请求,或
    //   2. CPU token 配额耗尽
    for e.hasWaitingRequests() && e.tryGet(canBurst /*arbitrary*/, 1) {
        // ① 尝试授权 1 个 token 给等待队列
        tokens := e.requester.granted(noGrantChain)

        if tokens == 0 {
            // ② requester 拒绝了授权 (例如队列为空)
            e.returnGrantWithoutGrantingElsewhere(1)
            return // 退出循环
        } else if tokens > 1 {
            // ③ requester 实际使用了 > 1 个 token
            //   (某些请求需要多个 token,如大批量操作)
            e.tookWithoutPermission(tokens - 1) // 补充扣除
        }
        // ④ 继续下一次循环,尝试授权下一个请求
    }
}
```

**循环逻辑详解**:

```
迭代 1:
  hasWaitingRequests() = true  ✅
  tryGet(1) = true (配额充足)   ✅
  → 调用 requester.granted()
  → tokens = 100000 (1ms = 100μs × 1000ns)
  → tookWithoutPermission(99999) (补充扣除)
  → 继续循环

迭代 2:
  hasWaitingRequests() = true  ✅
  tryGet(1) = true (还有配额)   ✅
  → 调用 requester.granted()
  → tokens = 50000 (0.5ms)
  → tookWithoutPermission(49999)
  → 继续循环

迭代 3:
  hasWaitingRequests() = true  ✅
  tryGet(1) = false (配额耗尽) ❌
  → 退出循环

结果:
  - 授权了 2 个请求
  - 消耗了 150000 纳秒的 CPU token
  - 剩余 50 个请求在队列中等待下次机会
```

#### 步骤 3: WorkQueue.granted()

这是**最复杂**的部分,涉及多租户公平性、优先级调度等:

```go
// pkg/util/admission/work_queue.go:883

func (q *WorkQueue) granted(grantChainID grantChainID) int64 {
    now := q.timeNow()
    q.mu.Lock()
    defer q.mu.Unlock()

    // ① 检查是否有等待的请求
    if len(q.mu.tenantHeap) == 0 {
        return 0  // 队列空,拒绝授权
    }

    // ② 从租户堆中选择下一个要服务的租户
    //   (基于公平性算法,已使用量最少的租户优先)
    tenant := q.mu.tenantHeap[0]

    // ③ 从该租户的请求堆中 Pop 出最高优先级的请求
    var item *waitingWork
    if len(tenant.waitingWorkHeap) > 0 {
        // 有普通等待请求
        item = heap.Pop(&tenant.waitingWorkHeap).(*waitingWork)
    } else {
        // 只有 epoch-LIFO 请求
        item = heap.Pop(&tenant.openEpochsHeap).(*waitingWork)
    }

    // ④ 更新统计信息
    waitDur := now.Sub(item.enqueueingTime)  // 计算等待时长
    tenant.priorityStates.updateDelayLocked(item.priority, waitDur, false)
    tenant.used += uint64(item.requestedCount)  // 增加租户使用量

    // ⑤ 重新平衡租户堆 (因为 used 增加了)
    if isInTenantHeap(tenant) {
        q.mu.tenantHeap.fix(tenant)
    } else {
        q.mu.tenantHeap.remove(tenant)
    }

    requestedCount := item.requestedCount
    q.mu.Unlock()

    // ⑥ 唤醒等待的 goroutine (在锁外发送,避免死锁)
    item.ch <- grantChainID

    // ⑦ 返回实际消耗的 token 数量
    return requestedCount
}
```

**关键数据结构**:

```
WorkQueue 的内部结构:

mu.tenantHeap:  (按 used/weight 排序的最小堆)
┌──────────────────────────────────────────────────┐
│ Tenant1 (used=100, weight=2) → ratio=50          │ ← 优先服务
│ Tenant2 (used=300, weight=2) → ratio=150         │
│ Tenant3 (used=500, weight=1) → ratio=500         │
└──────────────────────────────────────────────────┘

Tenant1.waitingWorkHeap: (按优先级排序)
┌──────────────────────────────────────────────────┐
│ Request_A (priority=0, reqCount=100000)          │ ← Pop 这个
│ Request_B (priority=1, reqCount=50000)           │
│ Request_C (priority=2, reqCount=25000)           │
└──────────────────────────────────────────────────┘

授权后:
  - Request_A 的 goroutine 被唤醒
  - Tenant1.used 增加 100000
  - Tenant1 在堆中下沉 (ratio 增大)
  - 下次 tryGrant() 可能服务 Tenant2
```

## 三、时序图:从调度器延迟到请求唤醒

```
时间线:     Sampler          Listener           Coordinator         Granter          WorkQueue         用户 Goroutine
           (后台)            (回调)              (协调器)            (授权器)         (队列)             (阻塞中)
             |                 |                    |                  |                |                    |
t=0ms        ├─ 采样          |                    |                  |                |                    |
             |  P99=80ms      |                    |                  |                |                    |
             ↓                 |                    |                  |                |                    |
t=0.1ms      └────────────────→SchedulerLatency()  |                  |                |                    |
                               ↓                    |                  |                |                    |
t=0.2ms                        ├─ 计算新配额        |                  |                |                    |
                               |  oldLimit=50%     |                  |                |                    |
                               |  newLimit=48%     |                  |                |                    |
                               ↓                    |                  |                |                    |
t=0.3ms                        ├─ setUtilizationLimit(48%)           |                |                    |
                               |                    |                  |                |                    |
                               ↓                    |                  |                |                    |
t=0.4ms                        └────────────────────→tryGrant()       |                |                    |
                                                    ↓                  |                |                    |
t=0.5ms                                             └─────────────────→tryGrant()      |                    |
                                                                       ↓                |                    |
t=0.6ms                                                                ├─ hasWaitingRequests() = true       |
                                                                       ├─ tryGet(1) = true                  |
                                                                       ↓                |                    |
t=0.7ms                                                                └────────────────→granted()           |
                                                                                        ↓                    |
t=0.8ms                                                                                 ├─ Pop Request_A     |
                                                                                        ├─ Update Tenant1    |
                                                                                        ↓                    |
t=0.9ms                                                                                 └────────────────────→唤醒!
                                                                                                             ↓
t=1.0ms                                                                                                      ├─ 开始执行
                                                                                                             |  业务逻辑
                                                                                                             ↓

总延迟: 1.0ms (从采样到唤醒)
```

## 四、为什么这个机制如此关键?

### 4.1 解决"配额更新后的停滞"问题

**场景 A: 没有 tryGrant() 的世界**:

```
t=0:   配额从 50% 降到 48%
       ↓
       100 个请求在队列中等待
       ↓
t=1s:  等待... (没有新请求到来触发检查)
       ↓
t=2s:  等待... (配额空闲,但没人使用)
       ↓
t=3s:  新请求到来 → 触发 tryGrant() → 前面 100 个请求终于被授权
       ❌ 平均延迟 = 2 秒!
```

**场景 B: 有 tryGrant() 的世界**:

```
t=0:   配额从 50% 降到 48%
       ↓
       setUtilizationLimit(48%)
       ↓
       tryGrant() 立即触发
       ↓
       100 个请求在 1ms 内被授权
       ✅ 平均延迟 = 1ms
```

### 4.2 实现"响应式准入控制"

```
传统准入控制:
  请求 → 检查配额 → 通过/阻塞
  问题: 配额变化时,已阻塞的请求无法感知

响应式准入控制 (CockroachDB):
  请求 → 检查配额 → 阻塞
  配额增加 → tryGrant() → 主动唤醒阻塞的请求
  ✅ 零延迟响应配额变化
```

### 4.3 充当"粘合剂"连接控制回路

```
┌──────────────────────────────────────────────────────────┐
│                    完整的控制回路                          │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  ① 监测层: Scheduler Latency Sampler                      │
│      ↓ (每 100ms 采样 P99)                                │
│                                                           │
│  ② 决策层: Scheduler Latency Listener                     │
│      ↓ (状态机计算新配额)                                  │
│                                                           │
│  ③ 执行层: ElasticCPUGranter.setUtilizationLimit()       │
│      ↓ (更新 Token Bucket 速率)                           │
│                                                           │
│  ④ 触发层: tryGrant() ← 这里!                            │
│      ↓ (主动将配额变化传递给等待请求)                       │
│                                                           │
│  ⑤ 响应层: WorkQueue.granted()                           │
│      ↓ (唤醒用户 goroutine)                               │
│                                                           │
│  ⑥ 反馈层: 用户请求执行,消耗 CPU,影响调度器延迟            │
│      ↓ (形成闭环)                                         │
│      └─────────────────→ 回到 ①                          │
│                                                           │
└──────────────────────────────────────────────────────────┘
```

**如果缺少 tryGrant()**:
- ③ 和 ⑤ 之间的连接断开
- 配额更新无法及时传递给等待请求
- 控制回路的响应速度变慢 (从毫秒级降到秒级)

## 五、TODO 注释的深层含义

让我们重新审视代码中的 TODO 注释:

```go
// TODO(irfansharif): Right now this is the only ticking mechanism for
// elastic CPU grants; consider some form of explicit ticking instead.
```

**现状**:
- tryGrant() 依赖于 SchedulerLatency 回调触发
- 触发频率 = 调度器延迟采样频率 (100ms)
- 对于 CPU 来说,100ms 粒度**刚好合适**

**问题**:
```go
// We have this need for fine-granularity explicit ticking for the IO
// tokens too, where the 250ms granularity is too coarse. Ideally a 1ms
// granularity would be good.
```

- **IO tokens** 也需要类似的 tryGrant() 机制
- 但 IO 操作的延迟敏感度更高,需要 **1ms 粒度**
- 当前的 250ms 粒度太粗糙:
  ```
  场景: IO token 配额增加
  t=0:    配额从 1000 → 5000 IOPS
  t=250ms: tryGrant() 才触发
  ❌ 250ms 的延迟对 IO 密集型任务太长
  ```

**提议的解决方案**:
```go
// maybe we can generalize that period switching into a struct where the
// coarser period is used only when some func indicates that the
// relevant "resource" is underloaded
```

自适应 ticking 频率:

```go
type AdaptiveTicker struct {
    shortPeriod time.Duration  // 1ms (高负载)
    longPeriod  time.Duration  // 250ms (低负载)

    isUnderloaded func() bool  // 检查资源是否空闲
}

func (t *AdaptiveTicker) Tick() {
    if t.isUnderloaded() {
        time.Sleep(t.longPeriod)  // 省电模式
    } else {
        time.Sleep(t.shortPeriod) // 响应模式
    }
    tryGrant()
}
```

**好处**:
- 💰 低负载时省 CPU (长周期)
- ⚡ 高负载时响应快 (短周期)
- 🎯 根据实际需求自动调整

**实际例子**:

```
场景 1: 夜间低负载
  - 等待队列为空
  - isUnderloaded() = true
  - Tick 间隔 = 250ms
  - CPU 开销: 4 次/秒

场景 2: 白天高峰
  - 等待队列有 1000 个请求
  - isUnderloaded() = false
  - Tick 间隔 = 1ms
  - CPU 开销: 1000 次/秒
  - ✅ 但请求延迟从 250ms 降到 1ms (250倍改进!)
```

## 六、实战调试技巧

### 6.1 如何验证 tryGrant() 被调用?

启用详细日志:

```bash
# 设置日志级别
cockroach start --vmodule=scheduler_latency_listener=1,elastic_cpu_granter=1 ...
```

日志输出示例:

```
I250105 12:34:56.789 admission/scheduler_latency_listener.go:274]
  SchedulerLatency: p99=80ms, newLimit=48%

I250105 12:34:56.790 admission/elastic_cpu_granter.go:180]
  tryGrant: hasWaiting=true, tokens=100000 (100μs)

I250105 12:34:56.791 admission/work_queue.go:918]
  granted: tenant=1, priority=0, waitDur=250ms, requestedCount=100000
```

### 6.2 监控 tryGrant() 的效率

查看 Prometheus 指标:

```promql
# 每秒授权的请求数
rate(admission.elastic_cpu.acquired_nanos[1m])

# 等待队列的平均延迟
admission.elastic_cpu.wait_durations.p99

# Token bucket 的可用余额
admission.elastic_cpu.available_nanos
```

**健康状态**:
```
available_nanos > 0        ✅ 配额充足
wait_durations.p99 < 10ms  ✅ 队列延迟低
```

**异常状态**:
```
available_nanos = 0        ❌ 配额耗尽
wait_durations.p99 > 1s    ❌ 队列严重积压
→ 可能需要增加 CPU 资源或调整配额上限
```

### 6.3 性能分析

使用 Go pprof 分析 tryGrant() 的 CPU 开销:

```bash
# 启动 pprof
cockroach debug pprof http://localhost:8080/debug/pprof/profile

# 查看热点函数
(pprof) top 10

# 期望看到:
# elasticCPUGranter.tryGrant  < 1% CPU (非常低)
# WorkQueue.granted           < 2% CPU
```

如果 tryGrant() 的 CPU 占比 > 5%,说明:
- 请求频率过高
- 队列操作效率低
- 需要优化堆操作或减少锁竞争

## 七、总结

### tryGrant() 的三大核心价值

1. **零延迟响应**: 配额变化的瞬间就尝试唤醒等待请求
2. **事件驱动**: 避免定时轮询的 CPU 浪费和固定延迟
3. **闭环粘合**: 连接控制回路的决策层和执行层

### 设计模式总结

```
观察者模式:
  - 配额变化 = 事件
  - tryGrant() = 观察者
  - WorkQueue = 被通知的订阅者

生产者-消费者模式:
  - Token Bucket = 生产者 (生产配额)
  - tryGrant() = 分发器
  - WorkQueue = 消费者队列

控制理论:
  - SchedulerLatency 采样 = 传感器
  - 状态机 = 控制器
  - setUtilizationLimit = 执行器
  - tryGrant() = 反馈触发器 ← 关键环节!
```

### 与第六章的联系

回到第六章的 `SchedulerLatency()` 函数,我们现在可以完整地理解整个流程:

```go
func (e *schedulerLatencyListener) SchedulerLatency(p99, period time.Duration) {
    // 步骤 1-3: 计算并应用新的 CPU 配额
    params := e.getParams(period)
    // ... 状态机逻辑 ...
    e.elasticCPULimiter.setUtilizationLimit(newUtilizationLimit)
    e.elasticCPULimiter.computeUtilizationMetric()

    // 步骤 4: 关键的"粘合剂" ← 我们深入解析的部分
    if e.coord != nil {
        e.coord.tryGrant()  // 主动唤醒等待队列中的请求
        //                     确保配额变化立即生效
        //                     形成闭环反馈控制
    }
}
```

**完整的因果链**:

```
调度器延迟升高
    ↓
SchedulerLatency() 回调触发
    ↓
状态机计算: 降低 CPU 配额 (48%)
    ↓
setUtilizationLimit(48%) 更新 Token Bucket
    ↓
tryGrant() 主动唤醒等待请求 ← 本章重点
    ↓
部分请求获得授权 (在新的 48% 配额下)
    ↓
系统 CPU 负载降低
    ↓
调度器延迟下降
    ↓
(循环回到起点,形成负反馈控制回路)
```

这就是 CockroachDB 准入控制系统中**响应式、事件驱动、零延迟**的核心机制——`tryGrant()`!
