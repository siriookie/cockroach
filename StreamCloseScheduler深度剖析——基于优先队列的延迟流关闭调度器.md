# StreamCloseScheduler 深度剖析——基于优先队列的延迟流关闭调度器

## 一、职责边界与设计动机（Why）

### 1.1 背景：为什么需要 StreamCloseScheduler？

在 CockroachDB 的 RACv2 (Replication Admission Control V2) 流控系统中，每个复制流（Replication Stream）都有其生命周期。当检测到某个流可能已失效时，系统不会立即关闭它，而是采用"探测-延迟-关闭"（Probe-Delay-Close）策略：

**问题场景：**
1. **网络短暂抖动**：如果立即关闭流，网络恢复后需要重新建立，开销很大
2. **误判成本**：过早关闭健康的流会导致不必要的流重建
3. **资源泄漏**：流长期不关闭会占用内存和文件描述符
4. **Raft 主线程压力**：如果在 Raft 主循环中轮询检查超时，会增加延迟

**如果没有 StreamCloseScheduler，系统会遇到：**
- **轮询开销**：需要在 Raft tick 中遍历所有流检查超时，O(n) 复杂度
- **定时精度差**：Raft tick 间隔（通常 200ms）导致关闭延迟不精确
- **代码耦合**：超时逻辑散布在 Raft 主循环中，难以维护
- **测试困难**：依赖真实时间，单元测试无法快速验证

### 1.2 StreamCloseScheduler 的核心价值

StreamCloseScheduler 提供了**异步的、精确的、可测试的延迟事件调度机制**：

```
┌────────────────────────────────────────────────┐
│  Raft Handler                                  │
│  └─> 检测到流可能失效                          │
│      └─> ScheduleSendStreamCloseRaftMuLocked  │ ← 只需一次调用
│          (延迟 N 秒后检查)                      │
└────────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────────┐
│  StreamCloseScheduler                          │
│  └─> 维护优先队列 (按时间排序)                 │
│      └─> 后台 goroutine 等待                   │
│          └─> 时间到达 → EnqueueRaftReady       │
└────────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────────┐
│  Raft Scheduler                                │
│  └─> 触发 Replica 的 Raft Ready 处理           │
│      └─> 在 Raft context 中真正关闭流          │
└────────────────────────────────────────────────┘
```

**核心语义：**
- **延迟调度**：在未来某个时间点触发 Raft 处理
- **精确触发**：使用 Timer 而非轮询，精度达到毫秒级
- **异步解耦**：调度与执行分离，不阻塞 Raft 主线程

### 1.3 在系统中的位置

StreamCloseScheduler 位于 **KV 层的流控子系统** 中：

```
┌─────────────────────────────────────────────┐
│              SQL Layer                      │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│          Transaction Layer (KV)             │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Store (kvserver)                           │
│  ├── Replica (Raft State Machine)          │
│  │   └── RangeController (RACv2)           │
│  │       └── Stream (per-replica流)         │
│  │           └── 检测失效 → 调度关闭         │
│  ├── RaftScheduler (Raft事件调度)           │
│  └── StreamCloseScheduler ← 我们在这里      │
│      └── 延迟触发 EnqueueRaftReady          │
└─────────────────────────────────────────────┘
```

**上游依赖：**
- **RangeController**：检测流失效，调用 `ScheduleSendStreamCloseRaftMuLocked`
- **Raft Handler**：在 Raft context 中发现流问题，请求延迟关闭
- **TimeSource**：时钟抽象，用于计算延迟和测试

**下游依赖：**
- **RaftScheduler**：通过 `EnqueueRaftReady` 触发 Replica 的 Raft 处理
- **Stopper**：管理后台 goroutine 的生命周期

### 1.4 核心抽象与对象

StreamCloseScheduler 的设计围绕以下核心抽象：

#### 1.4.1 长期存在的核心对象

```go
type streamCloseScheduler struct {
    clock     timeutil.TimeSource     // 时钟抽象（便于测试）
    scheduler RaftScheduler           // Raft 调度器接口
    nonEmptyCh chan struct{}          // 信号通道（通知有新事件）

    mu struct {
        syncutil.Mutex
        scheduled scheduledQueue      // 优先队列（最小堆）
    }
}
```

#### 1.4.2 核心状态：事件队列

```go
type scheduledCloseEvent struct {
    rangeID roachpb.RangeID   // 需要处理的 Range
    at      time.Time          // 触发时间
}

// scheduledQueue 实现 heap.Interface
type scheduledQueue struct {
    items []scheduledCloseEvent
}
```

**关键设计决策：**
- **最小堆**：事件按 `at` 时间排序，堆顶是最早的事件
- **无去重**：同一个 RangeID 可以有多个事件（允许重复调度）
- **信号驱动**：`nonEmptyCh` 使用缓冲为 1 的通道，避免信号丢失

#### 1.4.3 生命周期阶段

```
1. 创建阶段 (NewStreamCloseScheduler)
   - 初始化时钟和调度器引用
   - nonEmptyCh 尚未创建

2. 启动阶段 (Start)
   - 创建 nonEmptyCh = make(chan struct{}, 1)
   - 启动后台 goroutine: run()
   - 初始化 Timer

3. 运行阶段 (run)
   - 等待三种事件：
     a. stopper.ShouldQuiesce() - 停止信号
     b. nonEmptyCh - 新事件添加
     c. timer.Ch() - 时间到达
   - 处理到期事件：EnqueueRaftReady

4. 调度阶段 (ScheduleSendStreamCloseRaftMuLocked)
   - 添加事件到堆
   - 如果是最早事件，发送信号到 nonEmptyCh

5. 关闭阶段
   - stopper.Stop() 触发退出
   - Timer 停止，goroutine 结束
```

### 1.5 接口与契约

StreamCloseScheduler 实现了 `rac2.ProbeToCloseTimerScheduler` 接口：

```go
type ProbeToCloseTimerScheduler interface {
    ScheduleSendStreamCloseRaftMuLocked(
        ctx context.Context,
        rangeID roachpb.RangeID,
        delay time.Duration,
    )
}
```

**接口契约：**
1. **必须持有 raftMu**：调用者必须持有 Replica 的 raftMu 锁
2. **异步执行**：方法立即返回，实际关闭在未来发生
3. **至少一次语义**：事件可能重复触发（无害，Raft 层会检查）
4. **无取消机制**：一旦调度，无法取消（设计简化）

---

## 二、控制流与组件协作（How it flows）

### 2.1 主要执行路径

StreamCloseScheduler 的核心是一个**单线程事件循环**，配合优先队列实现精确的延迟调度：

```
Start() → run() 事件循环
├── timer.Ch()        → 时间到达，处理到期事件
├── nonEmptyCh        → 新事件添加，重新计算延迟
└── stopper.ShouldQuiesce() → 停止信号，退出
```

### 2.2 核心执行路径详解

#### 路径 1：调度新事件

```
时间线：
T0: Raft Handler 检测到流可能失效
    └─> 调用 ScheduleSendStreamCloseRaftMuLocked(rangeID=1, delay=5s)

T0+0us: ScheduleSendStreamCloseRaftMuLocked 执行
    ├─> now = clock.Now() = T0
    ├─> event = {rangeID: 1, at: T0 + 5s}
    ├─> mu.Lock()
    ├─> curLen = mu.scheduled.Len()  // 假设当前队列为空
    ├─> recalcDelay = (curLen == 0) // true
    ├─> heap.Push(&mu.scheduled, event)
    │   └─> 堆结构：[{r1, T0+5s}]
    ├─> 因为 recalcDelay == true，发送信号：
    │   select {
    │   case nonEmptyCh <- struct{}{}: // 发送成功
    │   default:                       // 如果通道已满，忽略
    │   }
    └─> mu.Unlock()
    └─> 立即返回（不阻塞）

T0+1ms: run() 循环收到 nonEmptyCh 信号
    ├─> now = clock.Now() = T0+1ms
    ├─> readyEvents(now) 检查到期事件
    │   └─> 堆顶事件：{r1, T0+5s}
    │   └─> T0+5s > T0+1ms，未到期
    │   └─> 返回空切片 []
    ├─> 无事件触发
    ├─> nextDelay(now) 计算下次延迟
    │   ├─> mu.Lock()
    │   ├─> next = heap.Pop(&mu.scheduled)  // {r1, T0+5s}
    │   ├─> delay = (T0+5s) - (T0+1ms) = 4999ms
    │   ├─> heap.Push(&mu.scheduled, next) // 放回
    │   └─> mu.Unlock()
    │   └─> 返回 4999ms
    └─> timer.Reset(4999ms)
```

#### 路径 2：事件触发

```
时间线：
T0+5s: Timer 触发
    └─> timer.Ch() 可读

T0+5s: run() 循环收到 timer 事件
    ├─> now = clock.Now() = T0+5s
    ├─> readyEvents(now) 检查到期事件
    │   ├─> mu.Lock()
    │   ├─> for mu.scheduled.Len() > 0:
    │   │   ├─> next = mu.scheduled.items[0]  // 堆顶
    │   │   ├─> if next.at.After(now):        // T0+5s > T0+5s? false
    │   │   │   └─> break
    │   │   ├─> event = heap.Pop(&mu.scheduled)
    │   │   └─> events.append(event)
    │   ├─> mu.Unlock()
    │   └─> 返回 [{r1, T0+5s}]
    ├─> for _, event := range events:
    │   └─> scheduler.EnqueueRaftReady(rangeID=1)
    │       └─> 将 Range 1 加入 Raft 调度队列
    ├─> nextDelay(now) 计算下次延迟
    │   ├─> mu.Lock()
    │   ├─> 堆为空，返回 maxStreamCloserDelay (24h)
    │   └─> mu.Unlock()
    └─> timer.Reset(24h)

T0+5s+10ms: Raft Scheduler 处理 Range 1
    └─> Replica.handleRaftReadyRaftMuLocked()
        └─> RangeController 检查流状态
            └─> 如果确认失效，真正关闭流
            └─> 如果已恢复，忽略此次触发
```

#### 路径 3：多事件调度

```
时间线：
T0: 调度三个事件
    ├─> ScheduleSendStreamCloseRaftMuLocked(r1, 10s) → 事件 A: {r1, T0+10s}
    ├─> ScheduleSendStreamCloseRaftMuLocked(r2, 5s)  → 事件 B: {r2, T0+5s}
    └─> ScheduleSendStreamCloseRaftMuLocked(r3, 8s)  → 事件 C: {r3, T0+8s}

T0+0ms: 添加事件 A
    ├─> 堆为空，recalcDelay = true
    ├─> 堆：[{r1, T0+10s}]
    └─> 发送信号到 nonEmptyCh

T0+1ms: run() 处理信号
    └─> timer.Reset(9999ms)

T0+10ms: 添加事件 B
    ├─> 堆：[{r1, T0+10s}]
    ├─> 新事件：{r2, T0+5s}
    ├─> 比较：T0+5s < T0+10s，recalcDelay = true
    ├─> heap.Push() 后堆：[{r2, T0+5s}, {r1, T0+10s}]
    └─> 发送信号到 nonEmptyCh

T0+11ms: run() 处理信号
    ├─> 堆顶：{r2, T0+5s}
    └─> timer.Reset(4989ms)

T0+20ms: 添加事件 C
    ├─> 堆：[{r2, T0+5s}, {r1, T0+10s}]
    ├─> 新事件：{r3, T0+8s}
    ├─> 比较：T0+8s > T0+5s，recalcDelay = false
    ├─> heap.Push() 后堆：[{r2, T0+5s}, {r3, T0+8s}, {r1, T0+10s}]
    └─> 不发送信号（Timer 已经正确设置）

T0+5s: Timer 触发，处理事件 B
    ├─> readyEvents() 返回 [{r2, T0+5s}]
    ├─> EnqueueRaftReady(r2)
    ├─> 堆：[{r3, T0+8s}, {r1, T0+10s}]
    └─> timer.Reset(3s)

T0+8s: Timer 触发，处理事件 C
    ├─> readyEvents() 返回 [{r3, T0+8s}]
    ├─> EnqueueRaftReady(r3)
    ├─> 堆：[{r1, T0+10s}]
    └─> timer.Reset(2s)

T0+10s: Timer 触发，处理事件 A
    ├─> readyEvents() 返回 [{r1, T0+10s}]
    ├─> EnqueueRaftReady(r1)
    ├─> 堆：[]
    └─> timer.Reset(24h)
```

### 2.3 触发方式分析

| 操作 | 触发类型 | 频率 | 是否阻塞 |
|------|---------|------|---------|
| `ScheduleSendStreamCloseRaftMuLocked()` | 被动调用 | 按流失效检测频率 | 否（立即返回） |
| `run()` 事件循环 | 主动轮询 | 持续运行 | 是（阻塞在 select） |
| Timer 触发 | 定时驱动 | 按事件到期时间 | 否（通道通知） |
| `nonEmptyCh` 信号 | 信号驱动 | 按新事件添加频率 | 否（异步通知） |

**关键观察：**

1. **非阻塞调度**：
   - `ScheduleSendStreamCloseRaftMuLocked` 只做入队操作
   - 持有 raftMu 的时间极短（微秒级）
   - 不等待事件触发

2. **精确延迟**：
   - 使用 `timeutil.Timer` 而非轮询
   - 延迟精度取决于 Go runtime 的 timer 精度（毫秒级）
   - 避免了 Raft tick 间隔的量化误差

3. **信号优化**：
   - `nonEmptyCh` 缓冲为 1，避免重复信号
   - 只在需要重新计算延迟时发送信号
   - 如果新事件晚于堆顶，不发送信号

### 2.4 与其他模块的交互

#### 2.4.1 与 RaftScheduler 的交互

```go
// RaftScheduler 接口定义
type RaftScheduler interface {
    EnqueueRaftReady(rangeID roachpb.RangeID)
}
```

**交互流程：**
```
StreamCloseScheduler.run()
    └─> 事件到期
        └─> scheduler.EnqueueRaftReady(rangeID)
            └─> RaftScheduler.enqueueRaftReady()
                └─> 将 Range 加入 ready queue
                └─> 通知 worker goroutines
                    └─> Replica.handleRaftReadyRaftMuLocked()
                        └─> RangeController.CloseStream()
```

**关键点：**
- StreamCloseScheduler 不直接操作 Replica
- 通过 RaftScheduler 解耦，保持单一职责
- RaftScheduler 会处理队列满、去重等问题

#### 2.4.2 与 TimeSource 的交互

```go
// 生产环境
timeSource := timeutil.DefaultTimeSource{}

// 测试环境
timeSource := timeutil.NewManualTime(timeutil.UnixEpoch)
timeSource.Advance(5 * time.Second)  // 模拟时间推进
```

**为什么需要时钟抽象？**
- **可测试性**：单元测试可以快速推进时间
- **确定性**：测试结果可重复
- **隔离性**：不依赖真实时间流逝

#### 2.4.3 与 Stopper 的交互

```go
func (s *streamCloseScheduler) Start(ctx context.Context, stopper *stop.Stopper) error {
    return stopper.RunAsyncTask(ctx, "flow-control-stream-close-scheduler",
        func(ctx context.Context) { s.run(ctx, stopper) })
}

func (s *streamCloseScheduler) run(_ context.Context, stopper *stop.Stopper) {
    for {
        select {
        case <-stopper.ShouldQuiesce():  // 停止信号
            return
        case <-s.nonEmptyCh:
        case <-timer.Ch():
        }
        // ...
    }
}
```

**生命周期管理：**
- Stopper 统一管理所有异步任务
- `ShouldQuiesce()` 优先级最高
- 保证 graceful shutdown

#### 2.4.4 共享状态模型

StreamCloseScheduler 的状态通过以下机制共享：

```
┌─────────────────────────────────────────┐
│          StreamCloseScheduler           │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │  mu.scheduled (scheduledQueue)    │ │
│  │  (写锁保护)                        │ │
│  │  ├── 生产者：                      │ │ ← ScheduleSendStreamCloseRaftMuLocked (多 goroutine)
│  │  │   └── ScheduleSendStreamClose   │ │
│  │  │       └── heap.Push()           │ │
│  │  └── 消费者：                      │ │ ← run() goroutine (单线程)
│  │      └── nextDelay() / readyEvents() │
│  │          └── heap.Pop()            │ │
│  └───────────────────────────────────┘ │
│                                         │
│  nonEmptyCh (chan struct{}, 缓冲=1)    │ ← 单向通信：生产者 → 消费者
└─────────────────────────────────────────┘
```

**并发模型：**

1. **读写路径分离**：
   - 写路径（ScheduleSendStreamClose）：多 goroutine 并发调用
   - 读路径（run）：单一 goroutine 消费

2. **锁粒度最小化**：
   - 只在操作堆时持锁
   - Timer 操作不持锁

3. **无饥饿风险**：
   - 使用 `syncutil.Mutex` 保证公平性
   - 信号通道避免忙等

---

## 三、关键函数与核心逻辑（How it works）

### 3.1 NewStreamCloseScheduler：构造函数

```go
func NewStreamCloseScheduler(
    clock timeutil.TimeSource,
    scheduler RaftScheduler,
) *streamCloseScheduler {
    return &streamCloseScheduler{
        scheduler: scheduler,
        clock: clock,
    }
}
```

**输入：**
- `clock`：时钟抽象，便于测试
- `scheduler`：Raft 调度器接口

**关键设计：**
1. **延迟初始化**：`nonEmptyCh` 在 `Start()` 中创建
   - 避免构造函数中创建 goroutine
   - 允许在 Start 前完成其他配置

2. **依赖注入**：
   - 不直接依赖具体实现
   - 提高可测试性

### 3.2 Start：启动后台 goroutine

```go
func (s *streamCloseScheduler) Start(ctx context.Context, stopper *stop.Stopper) error {
    s.nonEmptyCh = make(chan struct{}, 1)  // 缓冲为 1
    return stopper.RunAsyncTask(ctx, "flow-control-stream-close-scheduler",
        func(ctx context.Context) { s.run(ctx, stopper) })
}
```

**为什么缓冲为 1？**

这是一个关键设计决策：

```go
// 错误设计：缓冲为 0
s.nonEmptyCh = make(chan struct{})

// 问题：发送方可能阻塞
select {
case s.nonEmptyCh <- struct{}{}:  // 如果接收方未准备好，阻塞！
default:                          // 永远不会执行
}
```

正确设计：
```go
// 缓冲为 1
s.nonEmptyCh = make(chan struct{}, 1)

// 优势：发送方不阻塞
select {
case s.nonEmptyCh <- struct{}{}:  // 通常立即成功
default:                          // 如果通道已满（已有信号），忽略
}
```

**为什么可以忽略重复信号？**
- 信号的语义是"有新事件，请重新计算延迟"
- 如果通道已满，说明已经有信号在等待
- run() 会处理所有到期事件，不会遗漏

### 3.3 ScheduleSendStreamCloseRaftMuLocked：调度核心

```go
func (s *streamCloseScheduler) ScheduleSendStreamCloseRaftMuLocked(
    ctx context.Context, rangeID roachpb.RangeID, delay time.Duration,
) {
    now := s.clock.Now()
    event := scheduledCloseEvent{
        rangeID: rangeID,
        at:      now.Add(delay),
    }
    s.mu.Lock()
    defer s.mu.Unlock()

    curLen := s.mu.scheduled.Len()
    recalcDelay := (curLen > 0 && s.mu.scheduled.items[0].at.After(event.at)) || curLen == 0
    heap.Push(&s.mu.scheduled, event)
    if recalcDelay {
        select {
        case s.nonEmptyCh <- struct{}{}:
        default:
        }
    }
}
```

**核心逻辑分析：**

**（1）为什么需要 recalcDelay？**

```go
recalcDelay := (curLen > 0 && s.mu.scheduled.items[0].at.After(event.at)) || curLen == 0
```

这个条件判断是性能优化的关键：

```
场景 1：队列为空 (curLen == 0)
  - Timer 当前设置为 maxStreamCloserDelay (24h)
  - 新事件需要立即调整 Timer
  - recalcDelay = true

场景 2：新事件比堆顶早 (items[0].at.After(event.at))
  - 当前 Timer 等待堆顶事件
  - 新事件更早，需要重新设置 Timer
  - recalcDelay = true

场景 3：新事件比堆顶晚
  - 当前 Timer 已经正确设置
  - 无需重新计算
  - recalcDelay = false，避免不必要的通道操作
```

**（2）为什么先检查 `curLen` 再访问 `items[0]`？**

```go
// 错误写法：可能 panic
if s.mu.scheduled.items[0].at.After(event.at) || curLen == 0 {
    // 如果 curLen == 0，items[0] 会 panic: index out of range
}

// 正确写法：短路求值
if curLen > 0 && s.mu.scheduled.items[0].at.After(event.at) {
    // 只有 curLen > 0 时才访问 items[0]
}
```

**（3）为什么在 heap.Push 之后才发送信号？**

```go
heap.Push(&s.mu.scheduled, event)  // 先入队
if recalcDelay {
    select {
    case s.nonEmptyCh <- struct{}{}:  // 后通知
    default:
    }
}
```

原因：
- 确保信号到达时，事件已在队列中
- 避免竞态条件：run() 收到信号但堆为空

### 3.4 run：事件循环的核心

```go
func (s *streamCloseScheduler) run(_ context.Context, stopper *stop.Stopper) {
    timer := s.clock.NewTimer()
    timer.Reset(s.nextDelay(s.clock.Now()))
    defer timer.Stop()

    for {
        select {
        case <-stopper.ShouldQuiesce():
            return
        case <-s.nonEmptyCh:
        case <-timer.Ch():
        }

        now := s.clock.Now()
        for _, event := range s.readyEvents(now) {
            s.scheduler.EnqueueRaftReady(event.rangeID)
        }
        now = s.clock.Now()
        nextDelay := s.nextDelay(now)
        timer.Reset(nextDelay)
    }
}
```

**关键设计决策：**

**（1）为什么不使用 `time.After`？**

```go
// 错误设计：每次循环创建新 Timer
for {
    select {
    case <-time.After(delay):  // 创建新 Timer
    }
}
```

问题：
- 每次迭代创建新 Timer，导致内存分配
- 无法复用 Timer 对象
- Go 1.23+ 优化了 `time.After`，但 CockroachDB 需要兼容旧版本

正确设计：
```go
timer := s.clock.NewTimer()
defer timer.Stop()  // 保证资源释放

for {
    select {
    case <-timer.Ch():
    }
    timer.Reset(nextDelay)  // 复用 Timer
}
```

**（2）为什么两次调用 `clock.Now()`？**

```go
now := s.clock.Now()                  // 第一次
for _, event := range s.readyEvents(now) {
    s.scheduler.EnqueueRaftReady(event.rangeID)
}
now = s.clock.Now()                   // 第二次
nextDelay := s.nextDelay(now)
```

原因：
- `EnqueueRaftReady` 可能耗时（涉及锁和队列操作）
- 使用过期的 `now` 会导致延迟计算不准确
- 重新获取时间，保证下次 Timer 精确

**（3）为什么 select 的三个分支没有优先级？**

```go
select {
case <-stopper.ShouldQuiesce():  // 分支 1
case <-s.nonEmptyCh:              // 分支 2
case <-timer.Ch():                // 分支 3
}
```

Go 的 `select` 语义：
- 如果多个 case 同时就绪，**随机选择**
- 不保证优先级

但这里是安全的：
- `ShouldQuiesce()` 一旦就绪，会一直就绪，下次循环必然退出
- `nonEmptyCh` 和 `timer.Ch()` 都会导致事件处理，顺序无关紧要

### 3.5 nextDelay：计算下次延迟

```go
func (s *streamCloseScheduler) nextDelay(now time.Time) (delay time.Duration) {
    s.mu.Lock()
    defer s.mu.Unlock()

    delay = maxStreamCloserDelay
    if s.mu.scheduled.Len() > 0 {
        next := heap.Pop(&s.mu.scheduled).(scheduledCloseEvent)
        if delay = next.at.Sub(now); delay == 0 {
            delay = time.Nanosecond
        }
        heap.Push(&s.mu.scheduled, next)
    }

    return delay
}
```

**为什么使用 Pop + Push 而非 Peek？**

这是一个有趣的设计选择：

```go
// Go 标准库的 heap 没有 Peek 方法
// 方案 1：直接访问 items[0]
next := s.mu.scheduled.items[0]  // 不安全，绕过 heap 封装

// 方案 2：Pop + Push
next := heap.Pop(&s.mu.scheduled).(scheduledCloseEvent)  // 出队
delay = next.at.Sub(now)
heap.Push(&s.mu.scheduled, next)  // 入队
```

选择方案 2 的原因：
- **封装性**：使用 heap 接口，不直接访问 items
- **一致性**：所有堆操作都通过 heap 包
- **性能可接受**：Pop + Push 是 O(log n)，但 n 通常很小（< 100）

**为什么 delay == 0 时返回 time.Nanosecond？**

```go
if delay = next.at.Sub(now); delay == 0 {
    delay = time.Nanosecond
}
```

原因：
```go
// timer.Reset(0) 会立即触发，但可能引起边界问题
timer.Reset(0)

// timer.Reset(负数) 会 panic
timer.Reset(-1 * time.Second)  // panic: negative duration

// 使用 1ns 保证：
// - 立即触发（1ns 对系统来说可忽略）
// - 避免 panic
timer.Reset(time.Nanosecond)  // 安全且语义清晰
```

### 3.6 readyEvents：获取到期事件

```go
func (s *streamCloseScheduler) readyEvents(now time.Time) []scheduledCloseEvent {
    s.mu.Lock()
    defer s.mu.Unlock()

    var events []scheduledCloseEvent
    for s.mu.scheduled.Len() > 0 {
        next := s.mu.scheduled.items[0]
        if next.at.After(now) {
            break
        }
        events = append(events, heap.Pop(&s.mu.scheduled).(scheduledCloseEvent))
    }

    return events
}
```

**关键逻辑：**

**（1）为什么使用 `items[0]` 而非 `heap.Pop()`？**

```go
next := s.mu.scheduled.items[0]  // 查看堆顶
if next.at.After(now) {
    break  // 未到期，停止
}
heap.Pop(&s.mu.scheduled)  // 确认到期后才 Pop
```

原因：
- Peek 操作：只看不动
- 避免不必要的堆调整
- 一旦发现未到期，立即停止（后续事件必然未到期）

**（2）为什么不限制返回数量？**

```go
// 可能的设计：限制批量大小
const maxBatchSize = 100
for len(events) < maxBatchSize && s.mu.scheduled.Len() > 0 {
    // ...
}
```

当前设计不限制，原因：
- **正确性优先**：必须处理所有到期事件
- **实际负载低**：同一时刻到期的事件通常很少
- **下游限流**：RaftScheduler 会控制并发

**（3）为什么允许返回空切片？**

```go
var events []scheduledCloseEvent  // 初始为 nil
// 如果无到期事件
return events  // 返回 nil
```

这是安全的：
```go
for _, event := range nil {  // 不会 panic，循环 0 次
    // ...
}
```

---

## 四、运行时行为与系统反馈（Runtime Behavior）

### 4.1 系统如何感知运行时信号

StreamCloseScheduler 的设计精髓在于它如何**感知和响应**时间和事件信号：

#### 4.1.1 主动感知：Timer 驱动

```go
const maxStreamCloserDelay = 24 * time.Hour

timer := s.clock.NewTimer()
timer.Reset(nextDelay)
```

**Timer 延迟的选择：**
- **有事件**：精确计算到下一个事件的延迟
- **无事件**：使用 `maxStreamCloserDelay` (24h)

**为什么是 24 小时？**

```go
// 选择 24h 的原因：
// 1. 避免 Timer 永久阻塞（某些 Timer 实现不支持无限延迟）
// 2. 足够长，不会导致无意义的唤醒
// 3. 系统通常会在 24h 内至少有一次事件
// 4. 重启/停止时，Stopper 会强制退出，不依赖 Timer
```

实际上，24h 几乎等价于"无限"：
- 正常情况下，会有新事件添加，触发 `nonEmptyCh`
- 系统重启/停止时，`ShouldQuiesce()` 会触发

#### 4.1.2 被动感知：nonEmptyCh 信号

```go
case s.nonEmptyCh <- struct{}{}:  // 发送方
case <-s.nonEmptyCh:              // 接收方
```

**信号的语义：**
- "有新事件添加，可能需要重新计算延迟"
- 不保证堆中确实有新事件（信号可能重复）
- 接收方需要重新调用 `nextDelay` 获取准确延迟

**为什么不传递事件内容？**

```go
// 错误设计：传递事件
type signal struct {
    event scheduledCloseEvent
}
s.nonEmptyCh <- signal{event}

// 问题：
// 1. 通道需要更大缓冲，增加内存
// 2. 接收方仍需访问堆获取所有事件
// 3. 信号可能丢失（缓冲满），导致事件遗漏
```

正确设计：
```go
// 信号只是通知，不携带数据
s.nonEmptyCh <- struct{}{}

// 接收方自行从堆中获取
events := s.readyEvents(now)
```

### 4.2 信号如何影响决策

#### 4.2.1 延迟重新计算的时机

```
场景 A：新事件比当前等待的事件早
────────────────────────────────────
T0: Timer 设置为 10s (等待事件 A)
T5: 添加事件 B，延迟 2s (T5+2s = T7)
    └─> T7 < T10，需要提前唤醒
    └─> 发送信号到 nonEmptyCh
T5+1ms: run() 收到信号
    └─> 重新计算：nextDelay = 1.999s
    └─> timer.Reset(1.999s)
T7: Timer 触发，处理事件 B
T10: Timer 触发，处理事件 A
```

```
场景 B：新事件比当前等待的事件晚
────────────────────────────────────
T0: Timer 设置为 10s (等待事件 A)
T5: 添加事件 C，延迟 20s (T5+20s = T25)
    └─> T25 > T10，无需提前唤醒
    └─> 不发送信号（recalcDelay = false）
T10: Timer 触发，处理事件 A
    └─> 重新计算：nextDelay = 15s (T25 - T10)
    └─> timer.Reset(15s)
T25: Timer 触发，处理事件 C
```

#### 4.2.2 批量处理的决策

```go
for _, event := range s.readyEvents(now) {
    s.scheduler.EnqueueRaftReady(event.rangeID)
}
```

**为什么不逐个处理？**

```go
// 错误设计：逐个处理
for {
    event := s.getNextReadyEvent(now)
    if event == nil {
        break
    }
    s.scheduler.EnqueueRaftReady(event.rangeID)
    // 问题：每次循环都要加锁，开销大
}
```

正确设计：
```go
// 一次性获取所有到期事件
events := s.readyEvents(now)  // 一次加锁
for _, event := range events {
    s.scheduler.EnqueueRaftReady(event.rangeID)  // 无锁
}
```

**批量处理的优势：**
- **减少锁竞争**：只加一次锁
- **缓存友好**：连续访问堆元素
- **原子性**：同一时刻到期的事件一起处理

### 4.3 为什么采用当前策略

#### 4.3.1 惰性 vs 主动

StreamCloseScheduler 采用**惰性策略**：

```go
// 惰性策略：Timer 到期时才检查
case <-timer.Ch():
    events := s.readyEvents(now)  // 只检查到期事件
```

**为什么不主动轮询？**

```go
// 主动轮询（不推荐）
ticker := time.NewTicker(100 * time.Millisecond)
for {
    case <-ticker.C:
        s.checkAllEvents()  // 每 100ms 检查一次
}
```

主动轮询的问题：
- **浪费 CPU**：大部分时间无事件到期
- **延迟量化**：100ms 间隔导致最多 100ms 延迟误差
- **唤醒开销**：频繁唤醒 goroutine

惰性策略的优势：
- **精确触发**：Timer 精确到毫秒
- **零开销**：无事件时 goroutine 休眠
- **低延迟**：事件到期后立即处理

#### 4.3.2 本地自治 vs 集中控制

StreamCloseScheduler 采用**本地自治**：

```
每个 Store 有自己的 StreamCloseScheduler
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  Store 1        │  │  Store 2        │  │  Store 3        │
│  └─ Scheduler 1 │  │  └─ Scheduler 2 │  │  └─ Scheduler 3 │
└─────────────────┘  └─────────────────┘  └─────────────────┘
     独立运行             独立运行             独立运行
```

**为什么不集中调度？**

```go
// 集中调度（不推荐）
type CentralScheduler struct {
    events map[roachpb.StoreID][]scheduledCloseEvent
}
```

集中调度的问题：
- **单点瓶颈**：所有 Store 竞争同一把锁
- **扩展性差**：Store 数量增加，竞争加剧
- **故障传播**：一个 Store 的问题影响所有 Store

本地自治的优势：
- **无竞争**：每个 Store 独立运行
- **可扩展**：线性扩展到任意 Store 数量
- **故障隔离**：一个 Store 崩溃不影响其他 Store

### 4.4 设计如何在目标间取得平衡

| 目标 | 实现机制 | 权衡 |
|------|---------|------|
| **稳定性** | • 单一 goroutine 消费<br>• Timer 而非轮询<br>• Stopper 管理生命周期 | 牺牲部分并行性<br>（单线程处理） |
| **吞吐量** | • 批量处理到期事件<br>• 最小堆 O(log n)<br>• 锁粒度最小化 | 增加延迟<br>（批量积累时间） |
| **公平性** | • FIFO 顺序处理<br>• 无优先级区分 | 无法处理紧急事件<br>（所有事件平等） |
| **资源利用率** | • 惰性触发（无事件时休眠）<br>• 复用 Timer 对象<br>• 最小堆避免全量扫描 | 需要额外的堆内存<br>（O(n) 空间） |
| **实时性** | • 精确 Timer（毫秒级）<br>• 信号立即通知<br>• 无轮询延迟 | 依赖 Go runtime Timer<br>（不是硬实时） |

**核心权衡：简单性 vs 功能性**

StreamCloseScheduler 的设计清晰地选择了**简单性优先**：

```go
// 简单设计：无取消、无优先级、无去重
func (s *streamCloseScheduler) ScheduleSendStreamCloseRaftMuLocked(
    ctx context.Context, rangeID roachpb.RangeID, delay time.Duration,
) {
    // 只做入队，不做任何复杂逻辑
    event := scheduledCloseEvent{rangeID: rangeID, at: now.Add(delay)}
    heap.Push(&s.mu.scheduled, event)
}
```

**为什么选择简单？**
- StreamCloseScheduler 是底层基础设施
- 复杂性应该在上层（RangeController）
- 简单设计更容易验证正确性

**如果需要复杂功能怎么办？**
- **取消**：可以在 Raft Handler 中忽略过期事件
- **优先级**：可以使用多个 StreamCloseScheduler
- **去重**：可以在 RaftScheduler 中去重

---

## 五、设计模式分析（重点）

### 5.1 Priority Queue（优先队列）模式

**模式定义：**
使用堆（Heap）数据结构管理按优先级排序的元素，支持高效的插入和删除最小元素操作。

**在 StreamCloseScheduler 中的应用：**

```go
type scheduledQueue struct {
    items []scheduledCloseEvent
}

// 实现 heap.Interface
func (pq *scheduledQueue) Len() int { ... }
func (pq *scheduledQueue) Less(i, j int) bool {
    return pq.items[i].Less(pq.items[j])
}
func (pq *scheduledQueue) Swap(i, j int) { ... }
func (pq *scheduledQueue) Push(x interface{}) { ... }
func (pq *scheduledQueue) Pop() interface{} { ... }

func (s scheduledCloseEvent) Less(other scheduledCloseEvent) bool {
    if s.at.Equal(other.at) {
        return s.rangeID < other.rangeID  // 时间相同，按 RangeID 排序
    }
    return s.at.Before(other.at)  // 时间优先
}
```

**为什么选择这个模式？**

1. **时间复杂度优势**：
   ```
   插入事件：O(log n)
   获取最早事件：O(1)
   删除最早事件：O(log n)

   vs. 线性扫描：
   插入事件：O(1)
   获取最早事件：O(n)  ← 每次需要遍历所有事件
   ```

2. **内存效率**：
   - 堆使用连续数组，缓存友好
   - 无需额外指针（vs. 树结构）

3. **标准库支持**：
   - Go 的 `container/heap` 提供成熟实现
   - 无需自己实现堆维护逻辑

**这种模式在分布式系统中是否属于事实标准？**

是的，优先队列在延迟调度场景中是事实标准：
- **Linux 内核**：定时器使用红黑树（类似优先队列）
- **Java ScheduledExecutorService**：内部使用 DelayQueue（基于优先队列）
- **Go time.Timer**：runtime 使用最小堆管理所有 Timer

### 5.2 Event Loop（事件循环）模式

**模式定义：**
单线程循环等待和处理多个事件源，通过 I/O 多路复用避免阻塞。

**在 StreamCloseScheduler 中的应用：**

```go
func (s *streamCloseScheduler) run(_ context.Context, stopper *stop.Stopper) {
    timer := s.clock.NewTimer()
    defer timer.Stop()

    for {
        // I/O 多路复用：等待多个事件源
        select {
        case <-stopper.ShouldQuiesce():  // 事件源 1：停止信号
            return
        case <-s.nonEmptyCh:              // 事件源 2：新事件添加
        case <-timer.Ch():                // 事件源 3：Timer 到期
        }

        // 处理事件
        now := s.clock.Now()
        for _, event := range s.readyEvents(now) {
            s.scheduler.EnqueueRaftReady(event.rangeID)
        }

        // 重置等待
        timer.Reset(s.nextDelay(s.clock.Now()))
    }
}
```

**为什么选择这个模式？**

1. **避免线程爆炸**：
   ```go
   // 错误设计：每个事件一个 goroutine
   for _, event := range events {
       go func(e scheduledCloseEvent) {
           time.Sleep(e.delay)
           handleEvent(e)
       }(event)
   }
   // 问题：1000 个事件 = 1000 个 goroutine
   ```

   Event Loop：
   ```go
   // 单一 goroutine 处理所有事件
   for {
       select { ... }
   }
   // 优势：固定 1 个 goroutine，无论多少事件
   ```

2. **简化并发**：
   - 所有消费逻辑在单线程执行
   - 无需复杂的同步机制
   - 避免死锁和竞态条件

3. **可预测性**：
   - 事件按到达顺序处理
   - 易于调试和推理

**这种模式在分布式系统中是否属于事实标准？**

是的，Event Loop 在异步系统中是事实标准：
- **Node.js**：JavaScript 事件循环
- **Nginx**：事件驱动架构
- **Redis**：单线程事件循环
- **Go runtime**：netpoller 事件循环

### 5.3 Producer-Consumer（生产者-消费者）模式

**模式定义：**
生产者生成数据放入缓冲区，消费者从缓冲区取出数据处理，通过缓冲区解耦生产和消费。

**在 StreamCloseScheduler 中的应用：**

```
生产者 (多个 goroutine)              缓冲区                消费者 (单个 goroutine)
─────────────────────────          ─────────             ─────────────────────
Raft Handler 1
  └─> Schedule(r1, 5s) ─────┐                           run() goroutine
                             │                              ↓
Raft Handler 2               ├──> scheduledQueue     readyEvents()
  └─> Schedule(r2, 3s) ──────┤    (优先队列)              ↓
                             │                         EnqueueRaftReady()
Raft Handler N               │
  └─> Schedule(rN, 10s) ─────┘
```

**关键实现：**

```go
// 生产者：添加事件
func (s *streamCloseScheduler) ScheduleSendStreamCloseRaftMuLocked(...) {
    s.mu.Lock()
    heap.Push(&s.mu.scheduled, event)  // 写入缓冲区
    s.mu.Unlock()

    s.nonEmptyCh <- struct{}{}  // 通知消费者
}

// 消费者：处理事件
func (s *streamCloseScheduler) run(...) {
    for {
        select {
        case <-s.nonEmptyCh:  // 等待通知
        }
        events := s.readyEvents(now)  // 从缓冲区读取
        for _, event := range events {
            s.scheduler.EnqueueRaftReady(event.rangeID)  // 处理
        }
    }
}
```

**为什么选择这个模式？**

1. **解耦生产和消费**：
   - 生产者不等待消费者处理
   - 消费者按自己的节奏处理

2. **缓冲削峰**：
   - 突发流量时，队列暂存事件
   - 消费者稳定处理，不丢失事件

3. **多生产者，单消费者**：
   - 多个 Raft Handler 并发调度
   - 单一 goroutine 串行处理（简化逻辑）

**与经典模式的区别：**

经典模式：
```go
// 经典：有界缓冲区 + 阻塞
buffer := make(chan Event, 100)
// 生产者阻塞
buffer <- event  // 队列满时阻塞
// 消费者阻塞
event := <-buffer  // 队列空时阻塞
```

StreamCloseScheduler：
```go
// 改进：无界缓冲区 + 信号通知
mu.Lock()
heap.Push(&scheduled, event)  // 无界，不阻塞
mu.Unlock()

select {
case nonEmptyCh <- struct{}{}:  // 非阻塞通知
default:
}
```

优势：
- 生产者永不阻塞
- 消费者使用 Timer 代替轮询

### 5.4 Dependency Injection（依赖注入）模式

**模式定义：**
将依赖从外部注入，而非内部创建，提高可测试性和灵活性。

**在 StreamCloseScheduler 中的应用：**

```go
// 接口定义
type RaftScheduler interface {
    EnqueueRaftReady(rangeID roachpb.RangeID)
}

// 构造函数注入
func NewStreamCloseScheduler(
    clock timeutil.TimeSource,  // 注入时钟
    scheduler RaftScheduler,     // 注入调度器
) *streamCloseScheduler {
    return &streamCloseScheduler{
        scheduler: scheduler,
        clock: clock,
    }
}
```

**为什么选择这个模式？**

1. **可测试性**：
   ```go
   // 生产代码
   realClock := timeutil.DefaultTimeSource{}
   realScheduler := &raftScheduler{...}
   scs := NewStreamCloseScheduler(realClock, realScheduler)

   // 测试代码
   fakeClock := timeutil.NewManualTime(timeutil.UnixEpoch)
   fakeScheduler := &testingRaftScheduler{history: []}
   scs := NewStreamCloseScheduler(fakeClock, fakeScheduler)

   // 测试中可以手动推进时钟
   fakeClock.Advance(5 * time.Second)
   ```

2. **隔离性**：
   - StreamCloseScheduler 不依赖具体实现
   - 可以替换不同的调度器

3. **单一职责**：
   - StreamCloseScheduler 只负责调度
   - 不负责创建依赖对象

**测试代码中的应用：**

```go
// 测试用的假调度器
type testingRaftScheduler struct {
    clock   timeutil.TimeSource
    history []scheduledCloseEvent
}

func (t *testingRaftScheduler) EnqueueRaftReady(id roachpb.RangeID) {
    t.history = append(t.history, scheduledCloseEvent{
        rangeID: id,
        at: t.clock.Now(),  // 记录触发时间
    })
}
```

这使得测试可以：
- 验证事件是否在正确时间触发
- 检查调用顺序和频率
- 不依赖真实的 Raft 系统

### 5.5 Timer Coalescing（定时器合并）模式

**模式定义：**
多个定时事件共享同一个 Timer，通过动态调整 Timer 延迟，避免创建大量 Timer 对象。

**在 StreamCloseScheduler 中的应用：**

```go
// 单一 Timer 管理所有事件
timer := s.clock.NewTimer()
defer timer.Stop()

for {
    select {
    case <-timer.Ch():
        // 处理所有到期事件
        for _, event := range s.readyEvents(now) {
            s.scheduler.EnqueueRaftReady(event.rangeID)
        }
        // 重新设置 Timer 到下一个事件
        timer.Reset(s.nextDelay(s.clock.Now()))
    }
}
```

**为什么选择这个模式？**

对比：每事件一 Timer vs. 单一 Timer

```go
// 方案 A：每事件一 Timer（不推荐）
for _, event := range events {
    timer := time.AfterFunc(event.delay, func() {
        handleEvent(event)
    })
}
// 问题：
// - 1000 个事件 = 1000 个 Timer 对象
// - Go runtime 需要维护 1000 个定时器
// - 内存开销：~100 字节/Timer * 1000 = ~100KB

// 方案 B：单一 Timer + 优先队列（当前设计）
timer := time.NewTimer(nextDelay)
for {
    <-timer.C
    handleReadyEvents()
    timer.Reset(nextDelay)
}
// 优势：
// - 固定 1 个 Timer 对象
// - Go runtime 只维护 1 个定时器
// - 内存开销：~100 字节（固定）
```

**这种模式在分布式系统中是否属于事实标准？**

是的，Timer Coalescing 在高性能系统中是常见优化：
- **Linux 内核**：Hashed and Hierarchical Timing Wheels
- **Nginx**：Rbtree-based timer
- **Netty**：HashedWheelTimer

### 5.6 刻意避免的模式

#### 5.6.1 避免：Command Pattern（命令模式）

**Command Pattern 是什么？**
将请求封装为对象，支持撤销、重做、队列等操作。

**为什么不用？**

```go
// Command Pattern（不推荐）
type CloseCommand struct {
    rangeID roachpb.RangeID
    delay   time.Duration
    executed bool
}

func (c *CloseCommand) Execute() { ... }
func (c *CloseCommand) Undo() { ... }  // 撤销关闭

// 问题：
// 1. 过度设计：流关闭无需撤销
// 2. 增加复杂性：需要维护命令历史
// 3. 内存开销：每个命令是对象
```

当前设计：
```go
// 简单结构体
type scheduledCloseEvent struct {
    rangeID roachpb.RangeID
    at      time.Time
}
// 优势：
// - 无方法，零开销
// - 不支持撤销（不需要）
// - 直接入堆，高效
```

#### 5.6.2 避免：Observer Pattern（观察者模式）

**Observer Pattern 是什么？**
主题维护观察者列表，状态变化时通知所有观察者。

**为什么不用？**

```go
// Observer Pattern（不推荐）
type EventObserver interface {
    OnEventReady(event scheduledCloseEvent)
}

type streamCloseScheduler struct {
    observers []EventObserver
}

func (s *streamCloseScheduler) NotifyObservers(event scheduledCloseEvent) {
    for _, obs := range s.observers {
        obs.OnEventReady(event)  // 逐个通知
    }
}

// 问题：
// 1. 过度解耦：只有一个消费者（RaftScheduler）
// 2. 性能开销：遍历观察者列表
// 3. 同步问题：观察者阻塞会影响其他观察者
```

当前设计：
```go
// 直接调用
s.scheduler.EnqueueRaftReady(event.rangeID)

// 优势：
// - 单一消费者，无需列表
// - 直接调用，零开销
// - 清晰的调用链
```

---

## 六、具体运行示例（必须有）

### 6.1 正常场景：多事件调度与触发

**初始状态：**
- 时钟：T=0s
- 堆：空
- Timer：设置为 24h

**时间线：**

```
T=0s
─────────────────────────────────────────────────────────────
[操作] Raft Handler 调度 3 个事件
  └─> ScheduleSendStreamCloseRaftMuLocked(rangeID=1, delay=10s)
  └─> ScheduleSendStreamCloseRaftMuLocked(rangeID=2, delay=5s)
  └─> ScheduleSendStreamCloseRaftMuLocked(rangeID=3, delay=8s)

[状态] 堆：[(r2, T=5s), (r3, T=8s), (r1, T=10s)]
       ↑ 最小堆，按时间排序

[内部] 第一次调度 (r1, 10s)
  ├─> mu.Lock()
  ├─> curLen = 0
  ├─> recalcDelay = true  // 堆为空
  ├─> heap.Push({r1, T=10s})
  ├─> mu.Unlock()
  └─> nonEmptyCh <- struct{}{}  // 发送信号

[内部] run() 收到信号
  ├─> readyEvents(T=0s) → []  // 无到期事件
  ├─> nextDelay(T=0s) → 10s
  └─> timer.Reset(10s)

[内部] 第二次调度 (r2, 5s)
  ├─> mu.Lock()
  ├─> curLen = 1, items[0] = {r1, T=10s}
  ├─> 新事件: {r2, T=5s}
  ├─> recalcDelay = (T=10s > T=5s) = true
  ├─> heap.Push({r2, T=5s})
  │   └─> 堆调整: [{r2, T=5s}, {r1, T=10s}]
  ├─> mu.Unlock()
  └─> nonEmptyCh <- struct{}{}

[内部] run() 收到信号
  ├─> readyEvents(T=0s) → []
  ├─> nextDelay(T=0s) → 5s
  └─> timer.Reset(5s)  // 提前唤醒

[内部] 第三次调度 (r3, 8s)
  ├─> mu.Lock()
  ├─> curLen = 2, items[0] = {r2, T=5s}
  ├─> 新事件: {r3, T=8s}
  ├─> recalcDelay = (T=5s > T=8s) = false  // 不需要提前
  ├─> heap.Push({r3, T=8s})
  │   └─> 堆调整: [{r2, T=5s}, {r3, T=8s}, {r1, T=10s}]
  ├─> mu.Unlock()
  └─> 不发送信号  // Timer 已正确设置
```

```
T=5s
─────────────────────────────────────────────────────────────
[触发] Timer 到期

[内部] run() 收到 timer.Ch() 信号
  ├─> now = T=5s
  ├─> readyEvents(T=5s):
  │   ├─> mu.Lock()
  │   ├─> items[0] = {r2, T=5s}
  │   ├─> T=5s <= T=5s，到期
  │   ├─> events.append(heap.Pop())  → {r2, T=5s}
  │   ├─> items[0] = {r3, T=8s}
  │   ├─> T=8s > T=5s，未到期，break
  │   ├─> mu.Unlock()
  │   └─> 返回 [{r2, T=5s}]
  ├─> for _, event := range [{r2, T=5s}]:
  │   └─> scheduler.EnqueueRaftReady(rangeID=2)
  │       └─> RaftScheduler 将 Range 2 加入 ready 队列
  ├─> nextDelay(T=5s):
  │   ├─> items[0] = {r3, T=8s}
  │   ├─> delay = T=8s - T=5s = 3s
  │   └─> 返回 3s
  └─> timer.Reset(3s)

[状态] 堆：[(r3, T=8s), (r1, T=10s)]
[输出] Range 2 被触发关闭流
```

```
T=8s
─────────────────────────────────────────────────────────────
[触发] Timer 到期

[内部] run() 处理
  ├─> readyEvents(T=8s) → [{r3, T=8s}]
  ├─> scheduler.EnqueueRaftReady(rangeID=3)
  ├─> nextDelay(T=8s) → 2s
  └─> timer.Reset(2s)

[状态] 堆：[(r1, T=10s)]
[输出] Range 3 被触发关闭流
```

```
T=10s
─────────────────────────────────────────────────────────────
[触发] Timer 到期

[内部] run() 处理
  ├─> readyEvents(T=10s) → [{r1, T=10s}]
  ├─> scheduler.EnqueueRaftReady(rangeID=1)
  ├─> nextDelay(T=10s) → 24h  // 堆为空
  └─> timer.Reset(24h)

[状态] 堆：[]
[输出] Range 1 被触发关闭流
```

### 6.2 边界场景：同时到期的多个事件

**初始状态：**
- 时钟：T=100s
- 堆：空

**时间线：**

```
T=100s
─────────────────────────────────────────────────────────────
[操作] 调度 3 个事件，都在 T=105s 到期
  └─> Schedule(r10, delay=5s) → {r10, T=105s}
  └─> Schedule(r20, delay=5s) → {r20, T=105s}
  └─> Schedule(r5, delay=5s)  → {r5, T=105s}

[状态] 堆：[(r5, T=105s), (r10, T=105s), (r20, T=105s)]
       ↑ 时间相同，按 RangeID 排序

[内部] 堆排序规则
  func (s scheduledCloseEvent) Less(other scheduledCloseEvent) bool {
      if s.at.Equal(other.at) {
          return s.rangeID < other.rangeID  ← 时间相同，比较 ID
      }
      return s.at.Before(other.at)
  }

[结果] 堆顶：{r5, T=105s}（最小 RangeID）
```

```
T=105s
─────────────────────────────────────────────────────────────
[触发] Timer 到期

[内部] readyEvents(T=105s):
  ├─> mu.Lock()
  ├─> for mu.scheduled.Len() > 0:
  │   ├─> items[0] = {r5, T=105s}
  │   ├─> T=105s <= T=105s，到期
  │   ├─> events.append(heap.Pop())  → {r5, T=105s}
  │   │
  │   ├─> items[0] = {r10, T=105s}
  │   ├─> T=105s <= T=105s，到期
  │   ├─> events.append(heap.Pop())  → {r10, T=105s}
  │   │
  │   ├─> items[0] = {r20, T=105s}
  │   ├─> T=105s <= T=105s，到期
  │   ├─> events.append(heap.Pop())  → {r20, T=105s}
  │   │
  │   └─> mu.scheduled.Len() == 0, break
  ├─> mu.Unlock()
  └─> 返回 [{r5, T=105s}, {r10, T=105s}, {r20, T=105s}]

[内部] 批量触发
  for _, event := range events:
    └─> scheduler.EnqueueRaftReady(rangeID=5)
    └─> scheduler.EnqueueRaftReady(rangeID=10)
    └─> scheduler.EnqueueRaftReady(rangeID=20)

[输出] 3 个 Range 同时被触发
```

**关键观察：**
- 同时到期的事件**批量处理**，一次性触发
- 按 RangeID 排序，保证确定性（便于测试）
- 只加一次锁，高效

### 6.3 压力场景：高频调度

**场景：**
- 1000 个 Range，每秒调度一次
- 延迟范围：0-10s

**时间线：**

```
T=0s
─────────────────────────────────────────────────────────────
[压力] 1 秒内调度 1000 个事件
  for i := 1; i <= 1000; i++:
      delay := rand.Intn(10) * time.Second
      Schedule(rangeID=i, delay=delay)

[分析] 堆操作复杂度
  ├─> 每次 heap.Push: O(log n)
  ├─> 1000 次插入: 1000 * O(log 1000) ≈ 1000 * 10 = 10,000 次比较
  ├─> 实际耗时: 约 1-2ms（现代 CPU）

[分析] 锁竞争
  ├─> 每次调度持锁时间: ~1μs
  ├─> 1000 次调度总持锁时间: ~1ms
  ├─> 锁竞争概率低（持锁时间短）

[分析] 信号发送
  ├─> 只有堆顶变化时才发送信号
  ├─> 约 10-20 次信号（因为延迟随机）
  ├─> 大部分调度不触发信号（优化有效）

[状态] 堆大小：1000 个事件
       内存占用：约 32KB (32 字节/事件 * 1000)
```

```
T=0s - T=10s
─────────────────────────────────────────────────────────────
[处理] Timer 不断触发

T=0.5s: readyEvents() → 100 个事件（延迟 0s 的）
  └─> 批量触发 100 个 Range
  └─> 堆大小: 900

T=1.5s: readyEvents() → 100 个事件（延迟 1s 的）
  └─> 批量触发 100 个 Range
  └─> 堆大小: 800

...

T=10s: readyEvents() → 最后 100 个事件
  └─> 批量触发
  └─> 堆大小: 0

[性能] 总处理时间: ~10s（与延迟一致）
       平均每事件开销: ~10μs
       峰值堆大小: 1000 events
       峰值内存: ~32KB
```

**关键观察：**
- 堆操作 O(log n) 在 n=1000 时仍然高效
- 批量处理减少了锁开销
- 信号优化避免了大量不必要的唤醒
- 内存占用可控（线性增长）

---

## 七、设计权衡与替代方案（Design Tradeoffs and Alternatives）

### 7.1 核心权衡决策

StreamCloseScheduler 的设计过程中面临多个权衡决策，每个决策都在不同的目标之间寻找平衡点：

#### 7.1.1 权衡 1：单一 Timer vs. 多 Timer

**当前设计：单一 Timer**

```go
// 单一 Timer 管理所有事件
timer := s.clock.NewTimer()
for {
    select {
    case <-timer.Ch():
        // 处理所有到期事件
        for _, event := range s.readyEvents(now) {
            s.scheduler.EnqueueRaftReady(event.rangeID)
        }
        timer.Reset(s.nextDelay(s.clock.Now()))
    }
}
```

**替代方案 A：每事件一个 Timer**

```go
// 为每个事件创建独立 Timer
func (s *streamCloseScheduler) ScheduleSendStreamCloseRaftMuLocked(
    ctx context.Context, rangeID roachpb.RangeID, delay time.Duration,
) {
    time.AfterFunc(delay, func() {
        s.scheduler.EnqueueRaftReady(rangeID)
    })
}
```

**对比分析：**

| 维度 | 单一 Timer | 多 Timer |
|------|-----------|---------|
| **内存占用** | O(1) - 固定一个 Timer 对象<br>(约 100 字节) | O(n) - n 个事件 = n 个 Timer<br>(约 100n 字节) |
| **CPU 开销** | 每次重置 Timer：O(log n)<br>(堆操作) | 每个事件创建 Timer：O(1) |
| **Go Runtime 压力** | Runtime 维护 1 个定时器 | Runtime 维护 n 个定时器<br>Hash table 查找开销增加 |
| **取消机制** | 需要维护事件 ID 映射 | 可以直接 timer.Stop() |
| **代码复杂度** | 需要优先队列和事件循环 | 实现简单，无状态维护 |

**选择单一 Timer 的原因：**

1. **内存效率**：典型负载下可能有数百个待处理事件，单一 Timer 节省约 10-100KB 内存
2. **可预测性**：所有事件在单线程处理，避免并发问题
3. **批量优化**：同时到期的事件可以批量处理
4. **无取消需求**：流关闭场景允许重复触发（Raft 层会检查）

**何时应该选择多 Timer？**

如果场景满足以下条件，多 Timer 更合适：
- 事件数量少（< 10）
- 每个事件有独立的取消需求
- 无需批量处理
- 优先代码简单性

#### 7.1.2 权衡 2：主动轮询 vs. 惰性触发

**当前设计：惰性触发**

```go
// Timer 到期时才检查
select {
case <-timer.Ch():  // 精确到期时触发
    events := s.readyEvents(now)
}
```

**替代方案 B：主动轮询**

```go
// 定期检查所有事件
ticker := time.NewTicker(100 * time.Millisecond)
for {
    case <-ticker.C:
        // 每 100ms 检查一次
        for i := 0; i < len(events); i++ {
            if events[i].at.Before(now) {
                handleEvent(events[i])
            }
        }
}
```

**对比分析：**

| 维度 | 惰性触发 | 主动轮询 |
|------|---------|---------|
| **CPU 利用率** | 无事件时 goroutine 休眠<br>CPU 使用率 ~0% | 每秒 10 次唤醒<br>CPU 使用率 ~0.1-1% |
| **触发精度** | 精确到毫秒级<br>误差 < 1ms | 受轮询间隔限制<br>误差 0-100ms |
| **延迟抖动** | 低抖动，取决于 Timer | 高抖动，量化误差 |
| **实现复杂度** | 需要精确计算下次延迟 | 简单循环检查 |

**选择惰性触发的原因：**

1. **低功耗**：Store 可能长时间无流关闭事件，轮询浪费 CPU
2. **精确触发**：流关闭延迟要求精确（5s 就是 5s，不应该是 5.1s）
3. **扩展性**：节点数增加时，轮询开销线性增长

**何时应该选择主动轮询？**

- 实时性要求不高（秒级延迟可接受）
- 事件频繁（轮询开销可摊薄）
- 需要周期性健康检查

#### 7.1.3 权衡 3：去重 vs. 允许重复

**当前设计：允许重复**

```go
func (s *streamCloseScheduler) ScheduleSendStreamCloseRaftMuLocked(...) {
    // 直接入队，无去重
    heap.Push(&s.mu.scheduled, event)
}
```

**替代方案 C：去重机制**

```go
type streamCloseScheduler struct {
    mu struct {
        syncutil.Mutex
        scheduled     scheduledQueue
        rangeIDToEvent map[roachpb.RangeID]*scheduledCloseEvent  // 去重索引
    }
}

func (s *streamCloseScheduler) ScheduleSendStreamCloseRaftMuLocked(...) {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 检查是否已存在
    if existing, ok := s.mu.rangeIDToEvent[rangeID]; ok {
        // 更新时间或取消旧事件
        if event.at.Before(existing.at) {
            // 需要从堆中删除旧事件，非常复杂！
        }
    }
    heap.Push(&s.mu.scheduled, event)
    s.mu.rangeIDToEvent[rangeID] = &event
}
```

**对比分析：**

| 维度 | 允许重复 | 去重 |
|------|---------|------|
| **堆操作复杂度** | Push：O(log n)<br>Pop：O(log n) | Push：O(log n)<br>Delete：O(n) ← 需要线性扫描<br>Update：O(n) |
| **内存占用** | 堆：O(n) | 堆：O(m)<br>Map：O(m)<br>总：O(2m)<br>m = 去重后数量 |
| **正确性保证** | 简单，无竞态 | 复杂，需要处理：<br>- 更新 vs. 取消<br>- 堆与 map 一致性 |
| **实际影响** | RaftScheduler 会去重<br>触发多次无害 | 提前去重，减少下游压力 |

**选择允许重复的原因：**

1. **简单性**：避免复杂的堆删除操作（Go 标准库 heap 不支持高效删除）
2. **正确性**：减少并发 bug 风险
3. **上层去重**：RaftScheduler 已有去重逻辑，无需在此层做
4. **幂等性**：Raft Handler 中检查流状态是幂等的

**什么情况下必须去重？**

- 下游操作非幂等（如转账）
- 重复触发成本高（如网络请求）
- 事件量巨大（内存受限）

#### 7.1.4 权衡 4：优先级 vs. FIFO

**当前设计：时间优先，无业务优先级**

```go
func (s scheduledCloseEvent) Less(other scheduledCloseEvent) bool {
    if s.at.Equal(other.at) {
        return s.rangeID < other.rangeID  // 只按 ID 排序
    }
    return s.at.Before(other.at)  // 时间优先
}
```

**替代方案 D：支持优先级**

```go
type scheduledCloseEvent struct {
    rangeID  roachpb.RangeID
    at       time.Time
    priority int  // 新增优先级字段
}

func (s scheduledCloseEvent) Less(other scheduledCloseEvent) bool {
    if s.priority != other.priority {
        return s.priority > other.priority  // 高优先级优先
    }
    return s.at.Before(other.at)
}
```

**对比分析：**

| 维度 | 无优先级 | 有优先级 |
|------|---------|---------|
| **公平性** | 所有事件平等 | 低优先级可能饥饿 |
| **复杂性** | 简单 | 需要定义优先级策略 |
| **适用场景** | 流关闭都同等重要 | 某些流更重要（如系统表） |

**选择无优先级的原因：**

1. **无差异化需求**：所有流关闭都同样重要
2. **避免饥饿**：优先级可能导致低优先级事件永不触发
3. **简化设计**：无需定义和维护优先级策略

### 7.2 未选择的替代方案

#### 7.2.1 方案 E：时间轮（Timing Wheel）

**设计：**

```go
type TimingWheel struct {
    slots     [][]scheduledCloseEvent
    currentSlot int
    slotDuration time.Duration  // 如 100ms
}

// 每 100ms tick 一次
func (tw *TimingWheel) tick() {
    tw.currentSlot = (tw.currentSlot + 1) % len(tw.slots)
    for _, event := range tw.slots[tw.currentSlot] {
        handleEvent(event)
    }
    tw.slots[tw.currentSlot] = nil
}
```

**时间轮的优势：**

- 插入 O(1)，删除 O(1)
- 适合高频事件

**为什么没选择？**

1. **精度限制**：精度受 slot 大小限制（如 100ms）
2. **内存浪费**：需要预分配大量 slot（如 3600 个 slot 覆盖 1 小时）
3. **复杂性**：需要处理跨轮（hierarchical timing wheel）
4. **不适合稀疏事件**：StreamCloseScheduler 事件稀疏（每秒 < 10 个）

**适用场景：**

- 高频定时器（每秒数千个）
- 固定精度要求（如 TCP 超时重传）
- 内存充足

#### 7.2.2 方案 F：基于 context.WithTimeout 的方案

**设计：**

```go
func (s *streamCloseScheduler) ScheduleSendStreamCloseRaftMuLocked(
    ctx context.Context, rangeID roachpb.RangeID, delay time.Duration,
) {
    ctx, cancel := context.WithTimeout(ctx, delay)
    defer cancel()

    go func() {
        <-ctx.Done()
        if ctx.Err() == context.DeadlineExceeded {
            s.scheduler.EnqueueRaftReady(rangeID)
        }
    }()
}
```

**为什么没选择？**

1. **Goroutine 爆炸**：每个事件一个 goroutine
2. **无法取消**：context 超时后无法撤销
3. **资源泄漏风险**：goroutine 可能泄漏
4. **无批量优化**：每个事件独立处理

#### 7.2.3 方案 G：集中式调度器

**设计：**

```go
// 全局单例调度器
var globalScheduler *CentralStreamCloseScheduler

type CentralStreamCloseScheduler struct {
    mu struct {
        syncutil.Mutex
        events map[roachpb.StoreID][]scheduledCloseEvent
    }
}
```

**为什么没选择？**

1. **单点瓶颈**：所有 Store 竞争同一把锁
2. **不符合 CockroachDB 架构**：Store 应该是独立的
3. **故障传播**：一个 Store 的问题影响全局
4. **难以测试**：单例增加测试复杂度

### 7.3 设计决策总结

| 决策点 | 选择 | 放弃方案 | 核心原因 |
|--------|------|---------|---------|
| Timer 策略 | 单一 Timer | 多 Timer | 内存效率，批量处理 |
| 触发机制 | 惰性触发 | 主动轮询 | CPU 效率，精确触发 |
| 去重策略 | 允许重复 | 去重 | 简单性，上层去重 |
| 优先级 | 无优先级 | 有优先级 | 公平性，简化设计 |
| 数据结构 | 最小堆 | 时间轮 | 精度要求，稀疏事件 |
| 并发模型 | 单 goroutine | 每事件 goroutine | 资源控制，批量优化 |
| 部署架构 | 本地自治 | 集中调度 | 故障隔离，扩展性 |

**设计哲学：**

StreamCloseScheduler 的设计哲学是**简单性优先，正确性保证，性能适当**：

1. **简单性优先**：
   - 无取消机制
   - 无优先级
   - 无去重
   - 单线程消费

2. **正确性保证**：
   - 至少一次触发语义
   - 依赖注入提高可测试性
   - 清晰的生命周期管理

3. **性能适当**：
   - O(log n) 堆操作
   - 批量处理
   - Timer 复用
   - 惰性触发

**这种设计适合的场景：**

✅ 事件频率：低到中等（每秒 < 100）
✅ 延迟范围：秒到分钟级
✅ 精度要求：毫秒级
✅ 幂等性：可重复触发
✅ 资源约束：内存敏感

**不适合的场景：**

❌ 高频事件（每秒 > 1000）
❌ 微秒级精度要求
❌ 严格一次语义
❌ 需要取消和更新

### 7.4 工程权衡的深层思考

#### 7.4.1 为什么不像 Linux 内核那样使用红黑树？

Linux 内核的定时器使用红黑树：

```c
// Linux kernel timer implementation
struct hrtimer {
    struct rb_node node;  // Red-Black Tree node
    ktime_t expires;
};
```

**红黑树 vs. 最小堆：**

| 特性 | 红黑树 | 最小堆 |
|------|--------|--------|
| 插入 | O(log n) | O(log n) |
| 删除最小 | O(log n) | O(log n) |
| 随机删除 | O(log n) ✓ | O(n) ✗ |
| 内存布局 | 指针跳跃 | 连续数组 |
| 实现复杂度 | 高（需要平衡） | 低（Go 标准库） |

**为什么选择堆？**

1. **无随机删除需求**：StreamCloseScheduler 不需要取消事件
2. **缓存友好**：堆使用连续数组，CPU 缓存命中率高
3. **标准库支持**：Go 提供 `container/heap`，无需自己实现
4. **简单可靠**：红黑树实现复杂，容易引入 bug

#### 7.4.2 为什么不像 Nginx 那样使用红黑树？

Nginx 使用红黑树管理定时器：

```c
// Nginx timer implementation
typedef struct {
    ngx_rbtree_node_t   timer;
    ngx_msec_t          timer_expires;
} ngx_event_t;
```

**Nginx 的场景：**

- 大量并发连接（数万到数十万）
- 需要快速查找和删除特定连接的定时器
- C 语言，手动内存管理

**CockroachDB 的场景：**

- 中等数量事件（数百到数千）
- 不需要随机删除
- Go 语言，GC 管理内存

**结论：**红黑树适合 Nginx，但对 StreamCloseScheduler 是过度设计。

#### 7.4.3 为什么不像 Java ScheduledExecutorService 那样设计？

Java 的 `ScheduledThreadPoolExecutor` 使用 `DelayQueue`：

```java
public class ScheduledThreadPoolExecutor {
    private final DelayQueue<ScheduledFutureTask> queue;
    private final ThreadPoolExecutor executor;
}
```

**Java 方案的特点：**

- 线程池执行任务
- 支持任务取消（`Future.cancel()`）
- 支持周期性任务

**为什么不照搬？**

1. **无线程池需求**：Go 的 goroutine 轻量，无需线程池
2. **无取消需求**：流关闭可以重复触发
3. **无周期性需求**：流关闭是一次性事件

**适应 Go 生态：**

Go 的设计哲学是"简单的事情应该简单"：
- 使用 goroutine 而非线程池
- 使用 channel 而非 Future
- 使用 Timer 而非复杂的调度器

---

## 八、总结与心智模型（Summary and Mental Model）

### 8.1 核心概念总结

StreamCloseScheduler 是一个**延迟事件调度器**，它的本质可以用一句话概括：

> **用单一 Timer + 优先队列实现精确的延迟回调，通过事件循环串行处理所有到期事件。**

**三个核心抽象：**

1. **scheduledQueue（优先队列）**
   - 数据结构：最小堆
   - 职责：按时间排序所有待处理事件
   - 不变量：堆顶永远是最早到期的事件

2. **run() 事件循环**
   - 执行模型：单线程，持续运行
   - 职责：等待事件到期，批量触发
   - 不变量：同一时刻只处理一批事件

3. **nonEmptyCh 信号通道**
   - 通信模型：生产者-消费者
   - 职责：通知 run() 重新计算延迟
   - 不变量：缓冲大小为 1，非阻塞发送

### 8.2 心智模型：餐厅点餐系统类比

想象一个餐厅的点餐系统，可以帮助理解 StreamCloseScheduler：

```
┌─────────────────────────────────────────────────────────┐
│  餐厅点餐系统 (Restaurant Order System)                  │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  顾客 (Raft Handler)                                    │
│    ↓                                                    │
│  点餐："10 分钟后上菜"                                    │
│    ↓                                                    │
│  订单队列 (scheduledQueue)                              │
│    - 按上菜时间排序                                      │
│    - 最早的订单在最前                                    │
│    ↓                                                    │
│  厨房计时器 (Timer)                                      │
│    - 设置为下一个订单的时间                              │
│    - 时间到了响铃                                        │
│    ↓                                                    │
│  厨师 (run() goroutine)                                 │
│    - 听到响铃，检查所有到期订单                           │
│    - 批量出餐 (EnqueueRaftReady)                        │
│    - 重新设置计时器到下一个订单                          │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**类比映射：**

| 餐厅概念 | StreamCloseScheduler | 说明 |
|---------|---------------------|------|
| 顾客点餐 | `ScheduleSendStreamCloseRaftMuLocked` | 提交延迟任务 |
| 订单队列 | `scheduledQueue`（堆） | 按时间排序 |
| 厨房计时器 | `Timer` | 精确计时 |
| 厨师 | `run()` goroutine | 单线程处理 |
| 响铃 | `timer.Ch()` | 事件到期通知 |
| 批量出餐 | 批量调用 `EnqueueRaftReady` | 性能优化 |
| 顾客催单 | `nonEmptyCh` | 紧急订单通知 |

**关键洞察：**

1. **单一厨师**：只有一个厨师（goroutine）处理所有订单，避免混乱
2. **智能计时器**：计时器只设置到下一个订单，不浪费响铃
3. **批量出餐**：同时到期的订单一起处理，提高效率
4. **催单机制**：紧急订单（更早到期）会触发重新设置计时器

### 8.3 关键不变量（Invariants）

理解这些不变量可以快速推理系统行为：

#### 不变量 1：堆顶 = 最早事件

```go
// 任何时刻，堆顶都是最早到期的事件
s.mu.scheduled.items[0].at <= s.mu.scheduled.items[i].at  // ∀ i > 0
```

**推论：**
- 只需检查堆顶就能知道下次触发时间
- 新事件只有比堆顶早才需要重新设置 Timer

#### 不变量 2：Timer 延迟 = 堆顶到期时间 - 当前时间

```go
// Timer 总是设置为堆顶事件的延迟
timer.delay == s.mu.scheduled.items[0].at.Sub(s.clock.Now())
```

**推论：**
- Timer 触发时，至少堆顶事件已到期
- 可能有多个事件同时到期（批量处理）

#### 不变量 3：单一消费者

```go
// 只有 run() goroutine 会调用 readyEvents() 和 nextDelay()
// 保证：无并发修改冲突
```

**推论：**
- 无需复杂的同步机制
- 事件处理顺序可预测

#### 不变量 4：至少一次触发

```go
// 调度的事件最终会触发至少一次
// 但可能触发多次（允许重复）
```

**推论：**
- 下游必须是幂等的
- 可以简化设计（无需去重）

### 8.4 故障模式与容错

#### 故障模式 1：Timer 失效

**现象：**Timer 永不触发

**原因：**
- Go runtime bug（极罕见）
- 时钟回拨

**缓解：**
- `maxStreamCloserDelay`（24h）确保定期唤醒
- Stopper 保证 graceful shutdown

#### 故障模式 2：堆损坏

**现象：**`panic: heap invariant violated`

**原因：**
- 并发修改堆（违反不变量 3）
- Less() 函数不满足传递性

**缓解：**
- 所有堆操作都在 `mu` 保护下
- Less() 实现经过充分测试

#### 故障模式 3：Goroutine 泄漏

**现象：**run() goroutine 未退出

**原因：**
- Stopper 未正确调用
- select 死锁

**缓解：**
- `defer timer.Stop()` 保证资源释放
- `ShouldQuiesce()` 优先级最高

### 8.5 快速诊断检查清单

当 StreamCloseScheduler 行为异常时，按此清单排查：

**1. 事件未触发？**
```go
// 检查点 1：事件是否成功入队？
// 在 ScheduleSendStreamCloseRaftMuLocked 加日志
log.Infof("Scheduled event: rangeID=%d, at=%s", rangeID, event.at)

// 检查点 2：run() 是否在运行？
// 检查 goroutine stack trace
runtime.Stack(buf, true)

// 检查点 3：Timer 是否正确设置？
// 在 nextDelay() 加日志
log.Infof("Next delay: %s", delay)
```

**2. 事件触发过早或过晚？**
```go
// 检查点 1：时钟是否准确？
// 比较 s.clock.Now() 和 time.Now()
realNow := time.Now()
clockNow := s.clock.Now()
if realNow.Sub(clockNow) > time.Second {
    log.Warningf("Clock drift detected: %s", realNow.Sub(clockNow))
}

// 检查点 2：堆是否正确排序？
// 验证堆不变量
for i := 1; i < len(s.mu.scheduled.items); i++ {
    parent := (i - 1) / 2
    if s.mu.scheduled.items[parent].at.After(s.mu.scheduled.items[i].at) {
        panic("heap invariant violated")
    }
}
```

**3. 内存占用过高？**
```go
// 检查点 1：堆大小是否异常？
heapSize := s.mu.scheduled.Len()
if heapSize > 10000 {
    log.Warningf("Large heap size: %d events", heapSize)
}

// 检查点 2：是否有事件泄漏？
// 检查最老事件的时间
if heapSize > 0 {
    oldest := s.mu.scheduled.items[0]
    age := s.clock.Now().Sub(oldest.at)
    if age > 24 * time.Hour {
        log.Warningf("Old event detected: rangeID=%d, age=%s", oldest.rangeID, age)
    }
}
```

### 8.6 与其他系统的类比

理解 StreamCloseScheduler 在更广泛的系统中的位置：

**1. 与 Go Runtime Timer 的关系**

```
┌──────────────────────────────────────┐
│  StreamCloseScheduler                │
│  (应用层定时器)                       │
│  ├─ 管理业务事件 (流关闭)             │
│  └─ 使用 timeutil.Timer               │
└──────────────────────────────────────┘
                ↓
┌──────────────────────────────────────┐
│  Go Runtime Timer                    │
│  (语言层定时器)                       │
│  ├─ 管理所有 time.Timer               │
│  ├─ 使用最小堆 (timer heap)           │
│  └─ 由 runtime 调度                   │
└──────────────────────────────────────┘
                ↓
┌──────────────────────────────────────┐
│  OS Timer                            │
│  (操作系统层定时器)                   │
│  ├─ epoll/kqueue 超时                │
│  └─ 硬件定时器中断                    │
└──────────────────────────────────────┘
```

**层次关系：**
- StreamCloseScheduler 是应用层抽象
- 依赖 Go Runtime 的 Timer 实现
- 最终由 OS 提供精确计时

**2. 在 CockroachDB 中的位置**

```
┌─────────────────────────────────────────────────────┐
│  SQL Layer                                          │
│  (用户查询)                                          │
└─────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────┐
│  KV Layer                                           │
│  ├─ RangeController (流控逻辑)                       │
│  │   └─ 检测流失效，调用 ScheduleSendStreamClose     │
│  ├─ StreamCloseScheduler (延迟调度)  ← 我们在这里    │
│  │   └─ 延迟触发 EnqueueRaftReady                   │
│  └─ RaftScheduler (Raft 事件)                       │
│      └─ 触发 Replica 处理                            │
└─────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────┐
│  Storage Layer                                      │
│  (Raft 日志，RocksDB)                                 │
└─────────────────────────────────────────────────────┘
```

**职责分离：**
- RangeController：决策（何时关闭流）
- StreamCloseScheduler：调度（何时触发检查）
- RaftScheduler：执行（触发 Raft 处理）

### 8.7 一句话总结

如果要用一句话向新人解释 StreamCloseScheduler：

> **StreamCloseScheduler 就像一个智能闹钟管家，它维护一个按时间排序的任务清单（优先队列），用单一闹钟（Timer）管理所有任务，闹钟响时批量处理到期任务，然后设置下一个最早任务的闹钟。**

**关键词：**
- 智能：自动调整 Timer 延迟
- 单一：只用一个 Timer 对象
- 批量：同时到期的事件一起处理
- 精确：精确到毫秒级延迟

### 8.8 进阶学习路径

想要深入理解 StreamCloseScheduler，建议按以下顺序学习：

**Level 1：基础概念**
1. Go 的 `container/heap` 包
2. 优先队列（最小堆）数据结构
3. Go 的 `time.Timer` 用法

**Level 2：设计模式**
1. Event Loop 模式
2. Producer-Consumer 模式
3. Dependency Injection 模式

**Level 3：系统设计**
1. 延迟调度系统设计
2. Timer Coalescing 优化技术
3. Go runtime 的 timer 实现

**Level 4：对比学习**
1. Linux 内核的 hrtimer
2. Nginx 的 rbtree timer
3. Java 的 ScheduledExecutorService

**Level 5：实践应用**
1. 阅读测试代码：[close_scheduler_test.go](pkg/kv/kvserver/kvflowcontrol/replica_rac2/close_scheduler_test.go)
2. 修改代码添加 metrics
3. 尝试实现替代方案（如时间轮）

### 8.9 常见误解澄清

**误解 1："用了堆，所以很慢"**
❌ 堆操作是 O(log n)，对于 n < 1000，log n ≈ 10，非常快

**误解 2："单线程处理，所以吞吐量低"**
❌ 批量处理 + 异步触发，吞吐量取决于下游（RaftScheduler）

**误解 3："允许重复触发，所以会有 bug"**
❌ 下游是幂等的，重复触发是安全的设计选择

**误解 4："没有取消机制，所以不够强大"**
❌ 简单性是一种强大，取消可以在上层实现

**误解 5："24 小时的 maxStreamCloserDelay 太长了"**
❌ 这是兜底值，实际上新事件会重新设置 Timer

### 8.10 最后的建议

**阅读代码时：**
- 先理解不变量，再看实现
- 关注数据流向，而非具体语法
- 思考"为什么不这样设计？"

**调试问题时：**
- 先验证不变量是否成立
- 使用日志追踪事件流
- 对比测试代码的预期行为

**改进设计时：**
- 先理解当前设计的权衡
- 考虑是否真的需要改进
- 评估改进的成本和收益

---

**文档完结**

本文档全面剖析了 StreamCloseScheduler 的设计与实现，涵盖了从设计动机到工程权衡的方方面面。希望读者能够：

1. ✅ 理解为什么需要 StreamCloseScheduler
2. ✅ 掌握其核心设计模式和实现细节
3. ✅ 能够独立调试和优化相关代码
4. ✅ 在设计类似系统时做出正确的权衡决策

如有疑问，建议结合测试代码 [close_scheduler_test.go](pkg/kv/kvserver/kvflowcontrol/replica_rac2/close_scheduler_test.go) 进行实验验证。
