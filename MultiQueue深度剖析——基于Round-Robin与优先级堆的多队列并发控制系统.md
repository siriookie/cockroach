# MultiQueue深度剖析——基于Round-Robin与优先级堆的多队列并发控制系统

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 一、第一轮 BFS：职责边界与设计动机（Why）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 1.1 系统性问题与存在背景

在分布式数据库系统（如 CockroachDB）中，存在**多类工作负载需要竞争有限的系统资源**：

**核心困境**：
- **有限的并发槽位（Concurrency Slots）**：系统只能同时处理有限数量的并发任务（如 IO 操作、Raft 处理、Replica 操作等），超过这个限制会导致资源耗尽（内存、文件描述符、CPU 上下文切换开销）。
- **多种异构任务类型**：不同类型的任务（例如：Raft 快照传输、Replica GC、Split 操作、Merge 操作）具有不同的优先级和资源需求。
- **公平性与饥饿问题**：如果只用单一优先级队列，低优先级或少量任务的队列可能永远得不到执行（饥饿）；如果只用简单 FIFO，则无法体现紧急任务的优先级。
- **动态并发限制调整**：系统可能需要根据负载动态调整并发限制（例如在过载时降低并发度以保护系统）。

**没有 MultiQueue 的后果**：
1. **资源过载**：没有并发控制，任务无限制启动导致 OOM 或 CPU 饱和。
2. **优先级倒置**：紧急任务被大量低优先级任务阻塞。
3. **队列间饥饿**：某类任务持续涌入，其他类型任务永远得不到调度。
4. **缺乏隔离性**：一个失控的任务类型会影响其他所有任务。

### 1.2 系统中的位置与上下游关系

**所属子系统**：
- **资源调度与准入控制层（Resource Scheduling & Admission Control）**
- 位于 `pkg/kv/kvserver/multiqueue/` 包中

**在 CockroachDB 架构中的位置**：
```
┌─────────────────────────────────────────┐
│   KVServer Replica Operations           │  ← 上游：各种 Replica 操作请求
│   (Split/Merge/GC/Snapshot/Raft...)    │
└─────────────────┬───────────────────────┘
                  │
                  ↓
         ┌────────────────────┐
         │   MultiQueue       │  ← 本模块：并发控制与调度
         │  (准入控制网关)     │
         └────────┬───────────┘
                  │
                  ↓
┌─────────────────────────────────────────┐
│  实际任务执行层                          │  ← 下游：获得 Permit 后执行
│  (Storage/Raft/Network IO)              │
└─────────────────────────────────────────┘
```

**上游依赖者**：
- KVServer 中的各类 Queue（如 `replicaQueue`、`raftSnapshotQueue` 等）
- 需要并发控制的 Replica 操作（Split、Merge、GC、Snapshot 传输等）

**下游被调用者**：
- MultiQueue 本身不直接执行任务，它只负责**颁发许可（Permit）**
- 任务获得 Permit 后才能访问底层资源（如 Pebble 存储、Raft 网络）

### 1.3 核心抽象与生命周期

#### 核心对象

**1. `MultiQueue` 结构体**（长期存在，跨多个任务）
```go
type MultiQueue struct {
    mu               syncutil.Mutex     // 保护所有内部状态
    concurrencyLimit int                // 最大并发数（配置）
    remainingRuns    int                // 当前剩余可用槽位（动态）
    mapping          map[int]int        // queueType -> outstanding数组索引
    lastQueueIndex   int                // Round-Robin 轮询的上次位置
    outstanding      []notifyHeap       // 每个队列类型对应一个优先级堆
}
```

**2. `Task` 结构体**（短暂存在，代表一个待执行任务）
```go
type Task struct {
    priority  float64         // 优先级（越大越高）
    queueType int             // 队列类型标识
    heapIdx   int             // 在堆中的索引（-1表示已移除）
    permitC   chan *Permit    // 用于通知任务可以运行的通道（缓冲大小为1）
}
```

**3. `Permit` 结构体**（令牌，表示执行权限）
```go
type Permit struct {
    valid bool  // 防止重复释放
}
```

**4. `notifyHeap` 类型**（优先级堆，实现 Go 标准库 `heap.Interface`）
```go
type notifyHeap []*Task
```

#### 生命周期

```
[Task 生命周期]

1. Add() 创建 Task
   ↓
2. 插入对应 queueType 的堆中
   ↓
3. 等待调度（阻塞在 permitC 上）
   ↓
4a. 正常路径：tryRunNextLocked() 选中并发送 Permit
    ↓
    任务执行
    ↓
    Release(permit) 归还槽位

4b. 取消路径：Cancel(task) 移除并关闭 permitC
```

```
[MultiQueue 生命周期]

NewMultiQueue(n) 创建
   ↓
持续运行（接受 Add/Cancel/Release 调用）
   ↓
可选：UpdateConcurrencyLimit() 动态调整
   ↓
（无显式销毁，由 GC 回收）
```

### 1.4 核心设计意图总结

MultiQueue 的设计目标是：

1. **多队列隔离**：不同类型的任务放入不同队列，避免相互干扰。
2. **优先级调度**：每个队列内部按优先级执行（高优先级先执行）。
3. **公平性保证**：不同队列之间使用 Round-Robin 轮询，避免饥饿。
4. **全局并发控制**：所有队列共享一个并发限制，统一管理系统资源。
5. **动态调整能力**：支持运行时修改并发限制以应对负载变化。
6. **异步非阻塞**：任务通过 channel 异步等待，不占用调用线程。

**关键洞察**：MultiQueue 是一个**准入控制器（Admission Controller）**，它不执行任务本身，而是控制**谁可以执行、什么时候执行**。这是典型的**流量整形（Traffic Shaping）与背压（Backpressure）机制**。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 二、第二轮 BFS：控制流与组件协作（How it flows）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 2.1 主要执行路径与状态流转

#### 路径 1：任务添加与立即执行（Fast Path）

```
[初始状态] remainingRuns = 3, outstanding 所有队列均为空

Step 1: Add(queueType=1, priority=5.0)
   ├─> 检查 remainingRuns > 0 (true)
   ├─> 创建 Task，插入堆
   ├─> tryRunNextLocked()
   │    └─> Pop 堆顶任务
   │    └─> 发送 Permit 到 task.permitC
   │    └─> remainingRuns--  (3 → 2)
   └─> 任务调用者从 GetWaitChan() 立即收到 Permit（无阻塞）

[终态] remainingRuns = 2, 任务在执行中
```

#### 路径 2：任务排队与延迟执行（Slow Path）

```
[初始状态] remainingRuns = 0（所有槽位已满）

Step 1: Add(queueType=2, priority=8.0)
   ├─> 检查 remainingRuns = 0
   ├─> 检查队列长度是否超限（如果 maxQueueLength >= 0）
   │    └─> 如果超限：返回错误 "queue is too long"
   ├─> 创建 Task，插入堆
   ├─> tryRunNextLocked() 执行但无法运行（remainingRuns = 0）
   └─> 任务调用者阻塞在 GetWaitChan() 上

[中间态] Task 在堆中等待，remainingRuns = 0

Step 2: 某个正在执行的任务调用 Release(permit)
   ├─> releaseLocked()
   │    ├─> permit.valid = false（防止重复释放）
   │    ├─> remainingRuns++  (0 → 1)
   │    └─> tryRunNextLocked()
   │         ├─> Round-Robin 选择下一个队列
   │         ├─> Pop 堆顶任务（queueType=2, priority=8.0）
   │         ├─> 发送 Permit 到 task.permitC
   │         └─> remainingRuns--  (1 → 0)
   └─> 被阻塞的任务调用者收到 Permit，开始执行

[终态] remainingRuns = 0, 新任务获得执行权
```

#### 路径 3：任务取消（Cancel Path）

```
[初始状态] Task 在堆中等待，heapIdx = 3

Step 1: Cancel(task)
   ├─> 尝试从堆中移除（tryRemove）
   │    ├─> 检查 heapIdx >= 0 (true)
   │    └─> heap.Remove() 成功移除
   ├─> close(task.permitC)  ← 唤醒所有等待者
   └─> 等待者收到 nil（channel 关闭信号），知道任务已取消

[终态] Task 被移除，等待者退出
```

#### 路径 4：取消正在执行的任务（Race Condition）

```
[初始状态] Task 刚从堆中 Pop 出来，permitC 中已有 Permit

Step 1: Cancel(task) 与任务启动并发
   ├─> tryRemove() 返回 false（heapIdx = -1，已不在堆中）
   ├─> 尝试从 permitC 中抢 Permit
   │    └─> select {
   │         case p := <-task.permitC:  ← 抢到 Permit
   │              close(task.permitC)
   │              releaseLocked(p)      ← 归还槽位
   │         default:                   ← Permit 已被任务拿走
   │              // 不做任何事，任务负责调用 Release()
   │        }
   └─> 保证恰好一方（Cancel 或任务）释放 Permit

[终态] 槽位被正确归还，无泄漏
```

### 2.2 触发方式

| 操作 | 触发方式 | 调用者 | 阻塞性 |
|-----|---------|-------|-------|
| `Add()` | **主动请求驱动** | 上游任务提交者 | 立即返回 Task，任务可能阻塞在 `GetWaitChan()` |
| `tryRunNextLocked()` | **被动回调** | Add/Release/UpdateConcurrencyLimit 内部调用 | 非阻塞 |
| `Cancel()` | **主动请求驱动** | 上游任务取消者 | 非阻塞（可能竞争） |
| `Release()` | **主动请求驱动** | 任务执行完成者 | 非阻塞 |
| `UpdateConcurrencyLimit()` | **主动配置驱动** | 管理/监控组件 | 非阻塞 |

**关键设计**：
- **惰性调度**：`tryRunNextLocked()` 不是定时执行，而是在**有槽位释放或新任务到达**时才触发。
- **异步解耦**：任务提交者通过 channel 等待，不直接阻塞 MultiQueue 的锁。

### 2.3 与其他模块的交互方式

#### 共享状态管理

**互斥锁保护**：
```go
m.mu.Lock()
defer m.mu.Unlock()
```
所有状态修改（`remainingRuns`、`outstanding`、`mapping`）都在锁保护下，保证线性化（Linearizability）。

**无锁读取**：
- `GetWaitChan()` 不加锁（只返回 channel）
- `MaxConcurrency()` 不加锁（读取不可变字段）

#### 信号驱动机制

**Channel 作为同步原语**：
```go
permitC chan *Permit  // 缓冲大小为 1
```

**三种信号**：
1. **正常信号**：`permitC <- &Permit{valid: true}`  → 任务可以运行
2. **取消信号**：`close(permitC)`  → 任务被取消
3. **超时信号**：任务可以 `select` 自己的 timeout channel

**示例交互代码**：
```go
// 任务提交者侧
task, _ := mq.Add(1, 5.0, -1)
select {
case permit := <-task.GetWaitChan():
    if permit != nil {
        defer mq.Release(permit)
        // 执行任务
    }
case <-ctx.Done():
    mq.Cancel(task)
}
```

### 2.4 时间线与状态变化示例

```
时刻 T0: MultiQueue 初始化
    concurrencyLimit = 2
    remainingRuns = 2
    outstanding = []

T1: Task A (type=1, pri=3.0) Add()
    ├─> outstanding[0] = [A]
    ├─> tryRunNextLocked() → 发送 Permit 给 A
    └─> remainingRuns = 1

T2: Task B (type=2, pri=5.0) Add()
    ├─> outstanding[1] = [B]
    ├─> tryRunNextLocked() → 发送 Permit 给 B
    └─> remainingRuns = 0

T3: Task C (type=1, pri=4.0) Add()
    ├─> outstanding[0] = [C]  (堆中)
    ├─> tryRunNextLocked() → 无剩余槽位，不执行
    └─> remainingRuns = 0
    C 阻塞在 permitC 上

T4: Task D (type=2, pri=6.0) Add()
    ├─> outstanding[1] = [D]  (堆中)
    └─> D 阻塞在 permitC 上

T5: Task A Release()
    ├─> remainingRuns = 1
    ├─> Round-Robin 从 lastQueueIndex=0 开始轮询
    ├─> 跳到 index=1，Pop D (优先级更高)
    └─> D 获得 Permit，remainingRuns = 0

T6: Task B Release()
    ├─> remainingRuns = 1
    ├─> Round-Robin 从 lastQueueIndex=1 开始轮询
    ├─> 跳到 index=0（环形），Pop C
    └─> C 获得 Permit，remainingRuns = 0
```

**关键观察**：
- Round-Robin 保证了不同队列的**交替执行**，即使 type=2 的任务持续高优先级，type=1 也能得到调度机会。
- 同一队列内，**严格按优先级执行**（D 优先于 B，C 是唯一任务）。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 三、DFS 深入：关键函数与核心逻辑（How it works）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 3.1 `NewMultiQueue(maxConcurrency int) *MultiQueue`

**函数签名与职责**：
```go
func NewMultiQueue(maxConcurrency int) *MultiQueue
```

**输入**：
- `maxConcurrency`：最大并发数（必须 > 0，否则所有任务将永久阻塞）

**输出**：
- 初始化好的 `MultiQueue` 指针

**实现细节**：
```go
func NewMultiQueue(maxConcurrency int) *MultiQueue {
    queue := MultiQueue{
        remainingRuns:    maxConcurrency,  // 初始所有槽位可用
        concurrencyLimit: maxConcurrency,
        mapping:          make(map[int]int),
    }
    queue.lastQueueIndex = -1  // 初始为 -1，第一次轮询从 0 开始
    return &queue
}
```

**关键不变量**：
1. `remainingRuns <= concurrencyLimit`：剩余槽位不能超过限制
2. `remainingRuns >= 0`：不能为负（`max()` 函数保证）
3. `lastQueueIndex` 范围：`-1 <= lastQueueIndex < len(outstanding)`

**为什么初始 `lastQueueIndex = -1`**？
- 第一次调度时，`(lastQueueIndex + 0 + 1) % len(outstanding)` 从 index=0 开始
- 如果初始为 0，则会从 index=1 开始，导致 index=0 的队列被跳过

### 3.2 `Add(queueType int, priority float64, maxQueueLength int64) (*Task, error)`

**函数签名与职责**：
```go
func (m *MultiQueue) Add(queueType int, priority float64, maxQueueLength int64) (*Task, error)
```

**输入**：
- `queueType`：队列类型标识（可以是任意 int，动态创建）
- `priority`：任务优先级（越大越高）
- `maxQueueLength`：最大队列长度（-1 表示无限制）

**输出**：
- `*Task`：成功时返回任务对象
- `error`：队列过长时返回错误

**核心逻辑分段解析**：

#### 阶段 1：队列长度检查（背压机制）

```go
if m.remainingRuns == 0 && maxQueueLength >= 0 {
    currentLen := int64(m.queueLenLocked())
    if currentLen > maxQueueLength {
        return nil, errors.Newf("queue is too long %d > %d", currentLen, maxQueueLength)
    }
}
```

**为什么只在 `remainingRuns == 0` 时检查**？
- 如果 `remainingRuns > 0`，任务可以立即执行，不会进入队列，无需检查长度。
- 这是一种**快速路径优化**，避免无意义的长度计算。

**`queueLenLocked()` 的语义**：
```go
func (m *MultiQueue) queueLenLocked() int {
    if m.remainingRuns > 0 {
        return 0  // 可以立即运行，不算排队
    }
    count := 1  // 当前任务会排队，所以从 1 开始
    for i := 0; i < len(m.outstanding); i++ {
        count += len(m.outstanding[i])
    }
    return count
}
```
**注意**：这是"假设当前任务加入后的队列长度"，不是当前实际长度。

#### 阶段 2：动态队列创建与任务插入

```go
pos, ok := m.mapping[queueType]
if !ok {
    // 首次遇到该 queueType，创建新堆
    pos = len(m.outstanding)
    m.mapping[queueType] = pos
    m.outstanding = append(m.outstanding, notifyHeap{})
}
```

**设计选择**：
- **懒惰创建（Lazy Initialization）**：不预先分配队列，根据实际使用动态创建。
- **稳定映射**：`queueType` 到 `outstanding` 的映射**永不删除**，队列索引单调增长。

**任务创建与堆插入**：
```go
newTask := Task{
    priority:  priority,
    permitC:   make(chan *Permit, 1),  // 缓冲大小为 1
    heapIdx:   -1,                      // 初始未在堆中
    queueType: queueType,
}
heap.Push(&m.outstanding[pos], &newTask)
```

**为什么 `permitC` 缓冲大小为 1**？
- **避免死锁**：如果缓冲为 0，`tryRunNextLocked()` 发送 Permit 时可能阻塞（接收者还未到达）。
- **语义保证**：最多发送一个 Permit，缓冲为 1 足够。

#### 阶段 3：尝试立即调度

```go
m.tryRunNextLocked()
return &newTask, nil
```

**关键点**：
- 即使 `remainingRuns = 0`，也会调用 `tryRunNextLocked()`（但不会执行任何操作）。
- 这保证了逻辑一致性，代码路径统一。

### 3.3 `tryRunNextLocked()` - 调度核心

**函数签名与职责**：
```go
func (m *MultiQueue) tryRunNextLocked()
```

**前置条件**：
- `m.mu` 已被锁定

**核心逻辑**：
```go
func (m *MultiQueue) tryRunNextLocked() {
    if m.remainingRuns <= 0 {
        return  // 快速退出，无可用槽位
    }

    for i := 0; i < len(m.outstanding); i++ {
        // Round-Robin 轮询：从上次位置的下一个开始
        index := (m.lastQueueIndex + i + 1) % len(m.outstanding)

        if m.outstanding[index].Len() > 0 {
            task := heap.Pop(&m.outstanding[index]).(*Task)
            task.permitC <- &Permit{valid: true}  // 非阻塞发送（缓冲为1）
            m.remainingRuns--
            m.lastQueueIndex = index  // 更新轮询位置
            return
        }
    }
}
```

**Round-Robin 算法细节**：

```
假设 outstanding 有 3 个队列 [A, B, C]
lastQueueIndex = 1（上次从 B 调度）

本次轮询顺序：
i=0: index = (1 + 0 + 1) % 3 = 2  → 检查 C
i=1: index = (1 + 1 + 1) % 3 = 0  → 检查 A
i=2: index = (1 + 2 + 1) % 3 = 1  → 检查 B
```

**为什么使用 `(lastQueueIndex + i + 1)` 而不是 `(lastQueueIndex + i)`**？
- 确保**跳过上次调度的队列**，从下一个队列开始。
- 避免连续从同一队列调度多个任务（在 `remainingRuns > 1` 时）。

**并发安全性**：
- `task.permitC <- ...` 是在持有锁时发送，但因为 channel 有缓冲，不会阻塞。
- 接收者（任务等待者）不持有 MultiQueue 的锁，无死锁风险。

### 3.4 `Cancel(task *Task)` - 竞态处理

**函数签名与职责**：
```go
func (m *MultiQueue) Cancel(task *Task)
```

**输入**：
- `task`：要取消的任务

**复杂性来源**：
- 任务可能处于**三种状态**之一：
  1. 在堆中等待（未被调度）
  2. 已从堆中 Pop，但 Permit 还在 channel 中（刚被调度，未被接收）
  3. Permit 已被接收，任务正在执行

**实现逻辑**：

```go
func (m *MultiQueue) Cancel(task *Task) {
    m.mu.Lock()
    defer m.mu.Unlock()

    // 尝试从堆中移除
    queueIdx := m.mapping[task.queueType]
    ok := m.outstanding[queueIdx].tryRemove(task)

    if ok {
        // 情况 1：成功从堆中移除（任务还未调度）
        close(task.permitC)  // 通知等待者任务已取消
        return
    }

    // 情况 2 或 3：任务已被调度，尝试抢回 Permit
    select {
    case p, ok := <-task.permitC:
        if ok {  // 情况 2：抢到了 Permit
            close(task.permitC)
            m.releaseLocked(p)  // 归还槽位
        }
        // 如果 !ok，说明 channel 已关闭（不应发生）
    default:
        // 情况 3：Permit 已被任务接收者拿走
        // 由接收者负责调用 Release()
    }
}
```

**`tryRemove()` 实现**：
```go
func (h *notifyHeap) tryRemove(task *Task) bool {
    if task.heapIdx < 0 {
        return false  // 已不在堆中
    }
    heap.Remove(h, task.heapIdx)
    return true
}
```

**关键不变量维护**：
- `heap.Remove()` 内部会调用 `Swap()` 和 `Pop()`，确保 `heapIdx` 被更新为 -1。
- `heapIdx < 0` 是"任务不在堆中"的可靠标志。

**竞态窗口分析**：

```
时间线：
T1: tryRunNextLocked() 调用 heap.Pop(task)
    └─> task.heapIdx = -1
T2: tryRunNextLocked() 发送 Permit 到 permitC
T3: Cancel(task) 被调用
    ├─> tryRemove() 返回 false（heapIdx = -1）
    └─> select 尝试从 permitC 接收

可能结果：
- 如果 T3 < T2：Cancel 抢到 Permit（情况 2）
- 如果 T3 > T2：任务接收者已拿走 Permit（情况 3）
```

**为什么这个设计是正确的**？
- **恰好一次释放**：Permit 要么被 Cancel 释放，要么被任务执行者释放，不会重复或遗漏。
- **无锁等待**：Cancel 不阻塞等待任务完成，通过 `select default` 快速返回。

### 3.5 `Release(permit *Permit)` - 槽位归还

**函数签名与职责**：
```go
func (m *MultiQueue) Release(permit *Permit)
```

**输入**：
- `permit`：之前从 Task 接收的 Permit

**实现**：
```go
func (m *MultiQueue) releaseLocked(permit *Permit) {
    if !permit.valid {
        panic("double release of permit")  // 防御性编程
    }
    permit.valid = false

    // 防止 remainingRuns 超过 concurrencyLimit
    if m.remainingRuns < m.concurrencyLimit {
        m.remainingRuns++
    }

    m.tryRunNextLocked()  // 尝试调度下一个任务
}
```

**为什么需要 `if m.remainingRuns < m.concurrencyLimit` 检查**？

**场景**：并发限制动态收缩
```
初始：concurrencyLimit = 5, remainingRuns = 0（5 个任务在执行）
T1: UpdateConcurrencyLimit(3) 被调用
    └─> concurrencyLimit = 3, remainingRuns = 0（没有增加）
T2: 一个任务调用 Release()
    └─> 如果无条件 remainingRuns++，会变成 1
    └─> 但实际上还有 4 个任务在执行（超过限制）
```

**正确行为**：
- 只有当 `remainingRuns < concurrencyLimit` 时才增加，确保不超过新的限制。
- 允许超限任务继续执行，但不允许新任务启动，直到降到限制以下。

**Permit 重复释放检测**：
```go
if !permit.valid {
    panic("double release of permit")
}
```
这是一种**fail-fast 机制**，暴露上游使用错误（例如 Release 后忘记丢弃 Permit）。

### 3.6 `UpdateConcurrencyLimit(newLimit int)` - 动态调整

**函数签名与职责**：
```go
func (m *MultiQueue) UpdateConcurrencyLimit(newLimit int)
```

**输入**：
- `newLimit`：新的并发限制

**实现**：
```go
func (m *MultiQueue) updateConcurrencyLimitLocked(newLimit int) {
    diff := newLimit - m.concurrencyLimit
    m.remainingRuns = max(m.remainingRuns + diff, 0)  // 不允许为负
    m.concurrencyLimit = newLimit
    m.tryRunNextLocked()  // 如果增加了槽位，尝试调度
}
```

**三种场景**：

1. **增加并发限制**（扩容）
```
旧限制：5, remainingRuns = 2
新限制：8
    diff = 3
    remainingRuns = 2 + 3 = 5
    立即调度最多 5 个等待任务
```

2. **减少并发限制（但仍有剩余槽位）**
```
旧限制：5, remainingRuns = 3
新限制：4
    diff = -1
    remainingRuns = 3 - 1 = 2
    正在执行的任务不受影响
```

3. **大幅减少并发限制（已无剩余槽位）**
```
旧限制：10, remainingRuns = 0（10 个任务在执行）
新限制：5
    diff = -5
    remainingRuns = max(0 - 5, 0) = 0
    不允许新任务启动，直到 5 个任务完成
```

**关键设计决策**：
- **不强制终止任务**：已在执行的任务继续运行，即使超过新限制。
- **逐步收敛**：通过拒绝新任务启动，系统自然收敛到新限制。
- **立即生效**：增加限制时，立即调用 `tryRunNextLocked()` 激活等待任务。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 四、运行时行为与系统反馈（Runtime Behavior）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 4.1 系统如何感知运行时信号

MultiQueue **自身不主动感知外部信号**，而是通过**上游调用模式**间接反映系统状态：

#### 信号类型与来源

| 信号类型 | 表现形式 | MultiQueue 的响应 |
|---------|---------|------------------|
| **负载增加** | 高频 Add() 调用 | remainingRuns 快速降为 0，队列长度增长 |
| **负载减少** | Add() 调用频率下降 | remainingRuns 增加，队列清空 |
| **任务执行延迟** | Release() 调用延迟 | 任务长时间占用槽位，新任务等待时间增加 |
| **外部过载** | UpdateConcurrencyLimit(较小值) | 主动降低并发，减少系统压力 |
| **任务取消增加** | 高频 Cancel() 调用 | 槽位快速回收，但可能表示上游超时/失败 |

**被动式设计**：
- MultiQueue 不监控 CPU、内存、IO 等系统指标。
- 它只提供**机制（Mechanism）**，**策略（Policy）** 由上游决定（例如通过 admission controller 动态调整 concurrencyLimit）。

### 4.2 信号如何影响决策

#### 即时响应 vs 滞后响应

**即时响应**：
- `Add()` → `tryRunNextLocked()`：如果有槽位，**立即**调度（O(N_queues) 时间复杂度）。
- `Release()` → `tryRunNextLocked()`：槽位归还后**立即**尝试调度下一个任务。
- `UpdateConcurrencyLimit()` → `tryRunNextLocked()`：限制增加后**立即**激活等待任务。

**无滞后调度**：
- 不使用定时器或后台 goroutine 定期扫描队列。
- 完全由事件驱动（Add/Release/UpdateConcurrencyLimit 触发）。

**优势**：
- **低延迟**：任务等待时间 = 槽位释放时间（无额外调度延迟）。
- **零开销**：空闲时无 CPU 消耗。

**劣势**：
- **无主动平衡**：如果某个队列持续高优先级任务涌入，虽然 Round-Robin 保证其他队列有机会，但**不会主动限流**。

#### 局部 vs 全局决策

**全局视角**：
- 所有队列共享 `remainingRuns`，这是**全局资源**。
- Round-Robin 算法确保全局公平性。

**局部优化**：
- 每个队列内部独立维护优先级堆，**局部最优调度**。
- 不同队列之间无优先级比较（type=1 的高优先级任务不会抢占 type=2 的低优先级任务的槽位）。

**设计权衡**：
- 如果实现**全局优先级队列**（跨队列比较），会失去队列隔离性，导致饥饿。
- 当前设计**牺牲全局最优，换取公平性与隔离性**。

### 4.3 为什么采用当前策略

#### 惰性调度 vs 主动调度

**当前策略**：惰性（事件驱动）

**替代方案**：主动调度（后台 goroutine 定期扫描）
```go
// 假设的主动调度实现
go func() {
    ticker := time.NewTicker(10 * time.Millisecond)
    for range ticker.C {
        m.mu.Lock()
        m.tryRunNextLocked()
        m.mu.Unlock()
    }
}()
```

**为什么不采用**？
1. **无意义的 CPU 消耗**：大部分时间 `remainingRuns = 0`，扫描无效。
2. **增加锁竞争**：定期加锁会与 Add/Release 竞争。
3. **延迟无改善**：事件驱动已经足够及时。

#### 本地自治 vs 集中控制

**当前设计倾向于本地自治**：
- 每个 MultiQueue 实例独立管理自己的并发限制。
- 无跨实例协调（例如不同 Store 的 MultiQueue 不通信）。

**集中控制的场景**：
- 如果需要**全局并发限制**（例如整个节点最多 100 个并发操作），需要在 MultiQueue 之上构建协调层。
- CockroachDB 中通常通过**admission control** 层实现全局策略。

### 4.4 设计如何平衡多个目标

| 目标 | 实现机制 | 效果 |
|-----|---------|-----|
| **稳定性** | 严格的并发限制 + 队列长度检查 | 防止资源耗尽 |
| **吞吐量** | 立即调度（Fast Path）+ 无锁等待（channel） | 最小化延迟，最大化并发利用率 |
| **公平性** | Round-Robin 轮询 + 优先级堆 | 队列间公平，队列内按优先级 |
| **资源利用率** | 动态并发限制调整 + 惰性调度 | 根据负载伸缩，空闲时零开销 |

**不可能三角**：
- **公平性 vs 吞吐量**：Round-Robin 可能导致高优先级任务等待低优先级队列执行，牺牲部分吞吐。
- **稳定性 vs 吞吐量**：严格限制并发数会限制峰值吞吐量。
- **动态性 vs 复杂性**：支持动态调整增加了 UpdateConcurrencyLimit 的复杂性（需要处理 remainingRuns < 0 的情况）。

**CockroachDB 的选择**：
- **优先稳定性和公平性**：避免雪崩和饥饿比极致吞吐更重要。
- **通过分层设计平衡**：MultiQueue 提供机制，上层 admission controller 提供策略。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 五、设计模式分析（Design Patterns）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MultiQueue 融合了多种经典与演化后的设计模式，形成了一个**高度组合的并发控制系统**。

### 5.1 令牌桶模式（Token Bucket Pattern）- 核心

**经典定义**：
- 系统维护一个"桶"，初始包含 N 个令牌。
- 每个请求需要获取一个令牌才能执行。
- 执行完成后归还令牌。
- 当桶为空时，新请求阻塞或拒绝。

**MultiQueue 中的体现**：
```go
type MultiQueue struct {
    concurrencyLimit int  // 桶的容量
    remainingRuns    int  // 当前令牌数
}

type Permit struct {
    valid bool  // 令牌对象
}
```

**操作映射**：
| 令牌桶操作 | MultiQueue 实现 |
|-----------|----------------|
| 获取令牌 | `Add()` → `tryRunNextLocked()` → 发送 Permit |
| 归还令牌 | `Release(permit)` → `remainingRuns++` |
| 拒绝请求 | `Add()` 返回错误（队列过长） |
| 调整容量 | `UpdateConcurrencyLimit()` |

**演化点**：
- **经典令牌桶**：通常按固定速率补充令牌。
- **MultiQueue**：令牌由**任务完成驱动补充**（归还），无固定速率，**负反馈机制**更强。

**为什么选择这个模式**？
- **流量整形**：防止并发过载，保护下游资源。
- **背压传递**：当系统过载时，自然阻塞上游请求。
- **弹性伸缩**：通过动态调整 concurrencyLimit 适应负载变化。

### 5.2 优先级队列模式（Priority Queue Pattern）

**经典定义**：
- 使用堆（Heap）数据结构维护优先级顺序。
- 高优先级元素先出队。

**MultiQueue 中的体现**：
```go
type notifyHeap []*Task  // 每个队列类型一个堆

func (h notifyHeap) Less(i, j int) bool {
    return h[j].priority < h[i].priority  // 大顶堆
}
```

**实现细节**：
- 使用 Go 标准库 `container/heap`。
- **大顶堆**：`priority` 越大越高，Pop 出来的是最大值。
- **O(log N)** 插入/删除复杂度。

**为什么选择这个模式**？
- **紧急任务优先**：例如 Raft 心跳比普通 GC 更紧急。
- **避免低优先级阻塞高优先级**：堆保证高优先级始终先调度。

**局限性**：
- 堆内元素无法高效**查找**（需要 O(N) 遍历）。
- `Cancel()` 操作依赖 `heapIdx` 字段加速，但仍需 O(log N) 调整堆。

### 5.3 调度器模式（Scheduler Pattern）- Round-Robin 变体

**经典定义**：
- 调度器负责在多个任务/队列中选择下一个执行对象。
- Round-Robin 是一种**公平调度算法**。

**MultiQueue 中的体现**：
```go
func (m *MultiQueue) tryRunNextLocked() {
    for i := 0; i < len(m.outstanding); i++ {
        index := (m.lastQueueIndex + i + 1) % len(m.outstanding)
        if m.outstanding[index].Len() > 0 {
            // 从这个队列调度一个任务
            m.lastQueueIndex = index
            return
        }
    }
}
```

**调度策略**：
- **两级调度**：
  1. **队列间**：Round-Robin（公平性）
  2. **队列内**：Priority Queue（效率）

**为什么选择这个模式**？
- **避免饥饿**：如果使用全局优先级队列，低优先级队列可能永远得不到调度。
- **隔离性**：不同类型任务互不干扰（例如快照传输不会阻塞 GC）。

**与 CPU 调度器的类比**：
| CPU 调度器 | MultiQueue |
|-----------|-----------|
| 进程（Process） | 队列类型（queueType） |
| 线程（Thread） | 任务（Task） |
| 时间片轮转 | Round-Robin 队列选择 |
| 优先级调度 | 堆内优先级调度 |

### 5.4 观察者模式（Observer Pattern）- Channel 变体

**经典定义**：
- 主题（Subject）维护观察者（Observer）列表。
- 状态变化时通知所有观察者。

**MultiQueue 中的体现**：
```go
type Task struct {
    permitC chan *Permit  // 观察者的"通知通道"
}

// 主题：MultiQueue
// 事件：槽位可用
// 通知：permitC <- &Permit{...}
```

**演化点**：
- **经典观察者**：使用回调函数或接口。
- **MultiQueue**：使用**channel** 作为通知机制（更 Go 惯用）。

**优势**：
- **解耦**：任务等待者不需要主动轮询，被动接收通知。
- **并发安全**：channel 自带同步语义，无需额外加锁。
- **可取消**：通过 `close(permitC)` 统一通知取消。

**为什么不使用条件变量（sync.Cond）**？
- `sync.Cond` 需要在持有锁时调用 `Wait()`，容易死锁。
- Channel 更符合 Go 的"通过通信共享内存"哲学。

### 5.5 对象池模式（Object Pool Pattern）- 槽位池

**经典定义**：
- 预分配固定数量的对象。
- 使用时借出，完成后归还。
- 避免频繁创建/销毁对象。

**MultiQueue 中的体现**：
```go
concurrencyLimit  // 池大小
remainingRuns     // 可用对象数
Permit            // 池中对象
```

**映射关系**：
| 对象池操作 | MultiQueue 实现 |
|-----------|----------------|
| 借出对象 | 发送 Permit |
| 归还对象 | Release(permit) |
| 池满 | remainingRuns = 0 → 阻塞 |
| 调整池大小 | UpdateConcurrencyLimit() |

**与经典对象池的差异**：
- **经典对象池**：管理具体对象（如数据库连接）。
- **MultiQueue**：管理**抽象槽位**（Permit 是轻量级标记）。

### 5.6 策略模式（Strategy Pattern）- 隐式

**经典定义**：
- 定义算法族，封装每个算法，使它们可互换。

**MultiQueue 中的隐式体现**：
- **调度策略**：Round-Robin + Priority Queue（当前实现）。
- **可替换性**：理论上可以替换为其他策略（如 Weighted Round-Robin、Least Connections）。

**为什么是隐式的**？
- 调度逻辑硬编码在 `tryRunNextLocked()` 中，未抽象为接口。
- 这是工程权衡：**简单性 > 扩展性**（实际中调度策略很少变化）。

**如果需要策略模式，可能的重构**：
```go
type ScheduleStrategy interface {
    SelectNext(outstanding []notifyHeap, lastIndex int) (*Task, int)
}

type RoundRobinStrategy struct{}
func (s *RoundRobinStrategy) SelectNext(...) {...}

type WeightedStrategy struct{}
func (s *WeightedStrategy) SelectNext(...) {...}
```

但当前设计**刻意避免过度抽象**，遵循 YAGNI 原则（You Aren't Gonna Need It）。

### 5.7 防护性编程模式（Defensive Programming）

**体现点 1：重复释放检测**
```go
func (m *MultiQueue) releaseLocked(permit *Permit) {
    if !permit.valid {
        panic("double release of permit")  // Fail-fast
    }
    permit.valid = false
}
```

**体现点 2：堆索引一致性**
```go
func (h *notifyHeap) tryRemove(task *Task) bool {
    if task.heapIdx < 0 {
        return false  // 防止对已移除任务操作
    }
    heap.Remove(h, task.heapIdx)
    return true
}
```

**体现点 3：并发限制下界保护**
```go
m.remainingRuns = max(m.remainingRuns + diff, 0)  // 不允许为负
```

**为什么这些检查是必要的**？
- **分布式系统的复杂性**：并发调用、网络延迟、超时重试等导致状态难以预测。
- **快速暴露 Bug**：`panic` 比静默错误更容易调试。
- **不变量保证**：显式检查确保核心假设不被违反。

### 5.8 事实标准（De Facto Standards）

**模式来源**：
1. **令牌桶**：来自 TCP 拥塞控制、API 限流（如 AWS API Gateway）。
2. **优先级队列**：操作系统进程调度（如 Linux CFS）。
3. **Round-Robin**：网络负载均衡（如 Nginx）、CPU 调度器。

**在分布式数据库中的应用**：
- **TiKV**：使用类似的 RateLimiter + Scheduler 组合。
- **Cassandra**：多个 Compaction 队列 + 优先级调度。
- **ScyllaDB**：基于 Seastar 框架的 Reactor + 调度器模式。

**MultiQueue 的创新点**：
- **动态队列创建**：不预先定义队列类型，根据实际使用懒惰创建。
- **两级调度**：队列间公平 + 队列内优先级，兼顾公平性与效率。
- **取消语义**：处理任务取消的复杂竞态（很多实现忽略这一点）。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 六、具体运行示例（Concrete Examples）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 6.1 正常情况：多队列并发执行

**场景设定**：
- `concurrencyLimit = 2`
- 三种任务类型：
  - Type 1：Raft 快照（高优先级）
  - Type 2：Replica GC（中优先级）
  - Type 3：日志压缩（低优先级）

**时间线**：

```
═══════════════════════════════════════════════════════════════════
T0: 初始化
═══════════════════════════════════════════════════════════════════
MultiQueue{
    concurrencyLimit: 2,
    remainingRuns: 2,
    outstanding: [],
    mapping: {},
    lastQueueIndex: -1
}

═══════════════════════════════════════════════════════════════════
T1: Add(type=1, priority=9.0, maxLen=-1)  // Raft 快照任务 A
═══════════════════════════════════════════════════════════════════
操作：
1. mapping[1] = 0, outstanding = [heap0]
2. Task A 插入 heap0
3. tryRunNextLocked():
   - remainingRuns = 2 > 0
   - index = (-1 + 0 + 1) % 1 = 0
   - Pop A from heap0
   - A.permitC <- Permit{valid: true}
   - remainingRuns = 1
   - lastQueueIndex = 0

状态：
    remainingRuns: 1
    outstanding: [heap0(empty)]
    正在执行：A
    等待队列：无

调用者侧：
    permit := <-taskA.GetWaitChan()  // 立即收到
    // 开始执行快照传输

═══════════════════════════════════════════════════════════════════
T2: Add(type=2, priority=5.0, maxLen=-1)  // GC 任务 B
═══════════════════════════════════════════════════════════════════
操作：
1. mapping[2] = 1, outstanding = [heap0, heap1]
2. Task B 插入 heap1
3. tryRunNextLocked():
   - remainingRuns = 1 > 0
   - index = (0 + 0 + 1) % 2 = 1
   - Pop B from heap1
   - B.permitC <- Permit{valid: true}
   - remainingRuns = 0
   - lastQueueIndex = 1

状态：
    remainingRuns: 0
    outstanding: [heap0(empty), heap1(empty)]
    正在执行：A, B
    等待队列：无

═══════════════════════════════════════════════════════════════════
T3: Add(type=1, priority=8.0, maxLen=-1)  // 第二个快照任务 C
═══════════════════════════════════════════════════════════════════
操作：
1. Task C 插入 heap0
2. tryRunNextLocked():
   - remainingRuns = 0 ≤ 0
   - 直接返回，不调度

状态：
    remainingRuns: 0
    outstanding: [heap0(C), heap1(empty)]
    正在执行：A, B
    等待队列：C

调用者侧：
    select {
    case permit := <-taskC.GetWaitChan():
        // 阻塞在这里，等待 Permit
    }

═══════════════════════════════════════════════════════════════════
T4: Add(type=3, priority=3.0, maxLen=-1)  // 日志压缩任务 D
═══════════════════════════════════════════════════════════════════
操作：
1. mapping[3] = 2, outstanding = [heap0(C), heap1(empty), heap2]
2. Task D 插入 heap2
3. tryRunNextLocked(): 无操作（remainingRuns = 0）

状态：
    remainingRuns: 0
    outstanding: [heap0(C), heap1(empty), heap2(D)]
    正在执行：A, B
    等待队列：C, D

═══════════════════════════════════════════════════════════════════
T5: Add(type=2, priority=6.0, maxLen=-1)  // 第二个 GC 任务 E
═══════════════════════════════════════════════════════════════════
操作：
1. Task E 插入 heap1（优先级 6.0 > C 的 8.0 吗？不，heap1 单独维护）
2. tryRunNextLocked(): 无操作

状态：
    remainingRuns: 0
    outstanding: [heap0(C), heap1(E), heap2(D)]
    正在执行：A, B
    等待队列：C(pri=8.0), E(pri=6.0), D(pri=3.0)

═══════════════════════════════════════════════════════════════════
T6: 任务 A 完成，调用 Release(permitA)
═══════════════════════════════════════════════════════════════════
操作：
1. releaseLocked(permitA):
   - permitA.valid = false
   - remainingRuns = 0 + 1 = 1
2. tryRunNextLocked():
   - remainingRuns = 1 > 0
   - lastQueueIndex = 1（上次从 heap1 调度了 B）
   - 轮询顺序：
     i=0: index = (1 + 0 + 1) % 3 = 2 (heap2)
          heap2.Len() = 1 > 0 → 调度 D
   - Pop D from heap2
   - D.permitC <- Permit{valid: true}
   - remainingRuns = 0
   - lastQueueIndex = 2

状态：
    remainingRuns: 0
    outstanding: [heap0(C), heap1(E), heap2(empty)]
    正在执行：B, D
    等待队列：C, E

调用者侧（任务 D）：
    permit := <-taskD.GetWaitChan()  // 收到 Permit
    // 开始日志压缩

═══════════════════════════════════════════════════════════════════
T7: 任务 B 完成，调用 Release(permitB)
═══════════════════════════════════════════════════════════════════
操作：
1. remainingRuns = 1
2. tryRunNextLocked():
   - lastQueueIndex = 2
   - 轮询顺序：
     i=0: index = (2 + 0 + 1) % 3 = 0 (heap0)
          heap0.Len() = 1 > 0 → 调度 C
   - Pop C from heap0
   - C.permitC <- Permit{valid: true}
   - remainingRuns = 0
   - lastQueueIndex = 0

状态：
    remainingRuns: 0
    outstanding: [heap0(empty), heap1(E), heap2(empty)]
    正在执行：C, D
    等待队列：E

调用者侧（任务 C）：
    permit := <-taskC.GetWaitChan()  // 收到 Permit
    // 开始第二个快照

═══════════════════════════════════════════════════════════════════
T8: 任务 D 完成，调用 Release(permitD)
═══════════════════════════════════════════════════════════════════
操作：
1. remainingRuns = 1
2. tryRunNextLocked():
   - lastQueueIndex = 0
   - 轮询顺序：
     i=0: index = (0 + 0 + 1) % 3 = 1 (heap1)
          heap1.Len() = 1 > 0 → 调度 E
   - Pop E from heap1
   - E.permitC <- Permit{valid: true}
   - remainingRuns = 0
   - lastQueueIndex = 1

状态：
    remainingRuns: 0
    outstanding: [heap0(empty), heap1(empty), heap2(empty)]
    正在执行：C, E
    等待队列：无

═══════════════════════════════════════════════════════════════════
最终：任务 C 和 E 完成后，系统回到空闲状态
═══════════════════════════════════════════════════════════════════
    remainingRuns: 2
    所有队列为空
```

**关键观察**：
1. **Round-Robin 生效**：虽然 Type 1 的任务优先级最高（8.0 vs 6.0 vs 3.0），但 Type 3 的 D 任务在 T6 时被调度（而不是等 C 执行）。
2. **队列内优先级**：在 heap1 中，如果有多个任务，会先调度高优先级的。
3. **无死锁**：所有任务最终都得到执行，无饥饿。

### 6.2 边界场景：队列过长拒绝

**场景设定**：
- `concurrencyLimit = 1`
- `maxQueueLength = 2`（最多允许 2 个任务排队）

**时间线**：

```
═══════════════════════════════════════════════════════════════════
T0: 初始化
═══════════════════════════════════════════════════════════════════
MultiQueue{
    concurrencyLimit: 1,
    remainingRuns: 1,
    outstanding: [],
    mapping: {}
}

═══════════════════════════════════════════════════════════════════
T1: Add(type=1, priority=5.0, maxLen=-1)  // 任务 A（无限制）
═══════════════════════════════════════════════════════════════════
操作：
1. remainingRuns = 1 > 0，跳过长度检查
2. 立即调度 A
3. remainingRuns = 0

状态：
    remainingRuns: 0
    正在执行：A
    等待队列：无

═══════════════════════════════════════════════════════════════════
T2: Add(type=1, priority=4.0, maxLen=2)  // 任务 B
═══════════════════════════════════════════════════════════════════
操作：
1. remainingRuns = 0，进入长度检查
2. queueLenLocked():
   - remainingRuns = 0 → 不是快速路径
   - count = 1（假设当前任务会加入）
   - heap0.Len() = 0
   - count = 1 + 0 = 1
3. currentLen = 1 ≤ maxQueueLength = 2 → 通过
4. Task B 插入 heap0

状态：
    remainingRuns: 0
    正在执行：A
    等待队列：B

═══════════════════════════════════════════════════════════════════
T3: Add(type=1, priority=3.0, maxLen=2)  // 任务 C
═══════════════════════════════════════════════════════════════════
操作：
1. queueLenLocked():
   - count = 1 + heap0.Len() = 1 + 1 = 2
2. currentLen = 2 ≤ maxQueueLength = 2 → 通过
3. Task C 插入 heap0

状态：
    remainingRuns: 0
    正在执行：A
    等待队列：B(pri=4.0), C(pri=3.0)

═══════════════════════════════════════════════════════════════════
T4: Add(type=1, priority=2.0, maxLen=2)  // 任务 D（会被拒绝）
═══════════════════════════════════════════════════════════════════
操作：
1. queueLenLocked():
   - count = 1 + heap0.Len() = 1 + 2 = 3
2. currentLen = 3 > maxQueueLength = 2 → 失败
3. 返回错误：errors.Newf("queue is too long 3 > 2")

状态：
    remainingRuns: 0
    正在执行：A
    等待队列：B, C
    D 被拒绝

调用者侧：
    task, err := mq.Add(1, 2.0, 2)
    if err != nil {
        // 处理队列过长错误
        // 可能：重试、记录日志、返回客户端错误
    }

═══════════════════════════════════════════════════════════════════
T5: 任务 A 完成，Release(permitA)
═══════════════════════════════════════════════════════════════════
操作：
1. remainingRuns = 1
2. tryRunNextLocked():
   - Pop B from heap0（优先级 4.0 > 3.0）
   - B.permitC <- Permit
   - remainingRuns = 0

状态：
    remainingRuns: 0
    正在执行：B
    等待队列：C

═══════════════════════════════════════════════════════════════════
T6: 此时再次 Add(type=1, priority=6.0, maxLen=2)  // 任务 E
═══════════════════════════════════════════════════════════════════
操作：
1. queueLenLocked():
   - count = 1 + heap0.Len() = 1 + 1 = 2
2. currentLen = 2 ≤ maxQueueLength = 2 → 通过
3. Task E 插入 heap0

状态：
    remainingRuns: 0
    正在执行：B
    等待队列：E(pri=6.0), C(pri=3.0)
```

**关键观察**：
1. **背压生效**：任务 D 被拒绝，避免队列无限增长。
2. **队列长度是动态的**：T5 后队列长度降低，T6 时新任务可以加入。
3. **优先级影响出队顺序**：虽然 C 先加入，但 E 优先级更高，会先执行。

### 6.3 压力场景：并发限制动态收缩

**场景设定**：
- 初始 `concurrencyLimit = 5`
- 系统检测到 IO 过载，调用 `UpdateConcurrencyLimit(2)`

**时间线**：

```
═══════════════════════════════════════════════════════════════════
T0: 初始状态
═══════════════════════════════════════════════════════════════════
MultiQueue{
    concurrencyLimit: 5,
    remainingRuns: 0,  // 5 个任务正在执行
    outstanding: [heap0(T6, T7), heap1(T8, T9)]  // 4 个任务排队
}

正在执行：T1, T2, T3, T4, T5
等待队列：T6, T7, T8, T9

═══════════════════════════════════════════════════════════════════
T1: UpdateConcurrencyLimit(2)  // 降低到 2
═══════════════════════════════════════════════════════════════════
操作：
1. diff = 2 - 5 = -3
2. remainingRuns = max(0 + (-3), 0) = 0
3. concurrencyLimit = 2
4. tryRunNextLocked(): 无操作（remainingRuns = 0）

状态：
    concurrencyLimit: 2
    remainingRuns: 0
    正在执行：T1, T2, T3, T4, T5（5 个任务，超过新限制）
    等待队列：T6, T7, T8, T9

**关键点**：已在执行的任务不受影响，继续运行。

═══════════════════════════════════════════════════════════════════
T2: 任务 T1 完成，Release(permit1)
═══════════════════════════════════════════════════════════════════
操作：
1. releaseLocked(permit1):
   - permitremainingRuns = 0 < concurrencyLimit = 2
   - remainingRuns = 0 + 1 = 1  ← 可以增加
2. tryRunNextLocked():
   - 调度 T6（假设 Round-Robin 选中 heap0）
   - remainingRuns = 0

状态：
    concurrencyLimit: 2
    remainingRuns: 0
    正在执行：T2, T3, T4, T5, T6（5 个，仍超限）
    等待队列：T7, T8, T9

═══════════════════════════════════════════════════════════════════
T3: 任务 T2 和 T3 快速完成，连续 Release
═══════════════════════════════════════════════════════════════════
操作（T2）：
1. remainingRuns = 1
2. 调度 T7
3. remainingRuns = 0

操作（T3）：
1. remainingRuns = 1
2. 调度 T8
3. remainingRuns = 0

状态：
    concurrencyLimit: 2
    remainingRuns: 0
    正在执行：T4, T5, T6, T7, T8（5 个）
    等待队列：T9

═══════════════════════════════════════════════════════════════════
T4: 任务 T4, T5, T6 陆续完成
═══════════════════════════════════════════════════════════════════
操作（T4）：
1. remainingRuns = 1
2. 调度 T9
3. remainingRuns = 0

操作（T5）：
1. remainingRuns = 1
2. tryRunNextLocked(): 所有队列为空，无操作
3. remainingRuns = 1（保留槽位）

操作（T6）：
1. remainingRuns = 1 + 1 = 2
2. tryRunNextLocked(): 无操作
3. remainingRuns = 2

状态：
    concurrencyLimit: 2
    remainingRuns: 2  ← 系统收敛到新限制
    正在执行：T7, T8, T9（3 个，逐步降低）
    等待队列：无

═══════════════════════════════════════════════════════════════════
T5: 最终稳定状态（T7, T8, T9 陆续完成后）
═══════════════════════════════════════════════════════════════════
状态：
    concurrencyLimit: 2
    remainingRuns: 2
    正在执行：无
    等待队列：无

系统已完全收敛到新的并发限制。
```

**关键观察**：
1. **非强制终止**：降低限制不会中断正在执行的任务。
2. **逐步收敛**：通过拒绝新任务启动，系统自然降低到新限制。
3. **Release 的限制检查**：`if m.remainingRuns < m.concurrencyLimit` 确保 `remainingRuns` 不超过新限制。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 七、设计取舍与替代方案（Trade-offs & Alternatives）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 7.1 当前设计的核心权衡

#### 权衡 1：Round-Robin vs 全局优先级

**当前选择**：队列间 Round-Robin + 队列内优先级

**替代方案**：全局优先级堆（跨队列比较）
```go
// 假设的全局优先级实现
type GlobalPriorityQueue struct {
    heap notifyHeap  // 所有任务在一个堆中
}
```

**对比分析**：

| 维度 | Round-Robin（当前） | 全局优先级 |
|-----|---------------------|-----------|
| **公平性** | 高：每个队列类型保证得到调度机会 | 低：低优先级队列可能饥饿 |
| **优先级语义** | 局部最优：队列内严格优先级 | 全局最优：所有任务统一排序 |
| **隔离性** | 强：不同类型任务互不干扰 | 弱：高优先级任务可霸占所有槽位 |
| **实现复杂度** | 中等：需要管理多个堆 | 简单：单一堆 |
| **调度开销** | O(N_queues)：需要轮询找非空队列 | O(1)：直接 Pop |

**当前设计胜在**：
- **避免饥饿**：即使某个队列持续高优先级任务，其他队列也能得到调度。
- **类型隔离**：例如 Snapshot 队列不会被 GC 队列的大量任务阻塞。

**当前设计输在**：
- **优先级延迟**：紧急任务（如 Raft 心跳）可能需要等待其他队列的低优先级任务完成。
- **调度效率**：Round-Robin 需要遍历队列数组（虽然通常队列数量很小）。

**适用场景分析**：
- **当前设计适合**：任务类型多样、需要隔离、公平性优先（数据库内核）。
- **全局优先级适合**：任务类型单一、紧急任务优先、可以容忍饥饿（如实时系统）。

#### 权衡 2：令牌归还驱动 vs 固定速率补充

**当前选择**：任务完成时归还令牌（`Release()` 触发）

**替代方案**：固定速率补充令牌（如每秒 N 个）
```go
// 假设的固定速率实现
go func() {
    ticker := time.NewTicker(time.Second / rate)
    for range ticker.C {
        m.mu.Lock()
        if m.remainingRuns < m.concurrencyLimit {
            m.remainingRuns++
            m.tryRunNextLocked()
        }
        m.mu.Unlock()
    }
}()
```

**对比分析**：

| 维度 | 归还驱动（当前） | 固定速率补充 |
|-----|-----------------|-------------|
| **吞吐上限** | 无：取决于任务完成速度 | 有：受补充速率限制 |
| **背压机制** | 强：任务慢则槽位不归还 | 弱：令牌持续补充，可能积压 |
| **CPU 开销** | 零（事件驱动） | 持续（定时器） |
| **速率控制** | 不支持 | 支持（例如限制每秒请求数） |
| **用例** | 并发限制 | 速率限制 |

**当前设计胜在**：
- **零开销**：无后台 goroutine，无定时器。
- **强背压**：任务堆积会自然阻塞新请求，保护系统。
- **弹性**：任务快速完成时，吞吐量自然提升。

**当前设计输在**：
- **无速率控制**：不能限制"每秒最多 N 个请求"。
- **突发流量**：如果槽位充足，所有请求会同时启动（可能造成峰值负载）。

**场景选择**：
- **并发限制**：用当前设计（保护资源，如文件描述符、内存）。
- **速率限制**：用固定速率（保护外部 API，如限制每秒调用次数）。

#### 权衡 3：动态队列创建 vs 预定义队列

**当前选择**：Lazy Initialization（根据 `queueType` 动态创建）

**替代方案**：预定义队列
```go
// 假设的预定义实现
func NewMultiQueue(queueTypes []int, maxConcurrency int) *MultiQueue {
    outstanding := make([]notifyHeap, len(queueTypes))
    mapping := make(map[int]int)
    for i, qt := range queueTypes {
        mapping[qt] = i
        outstanding[i] = notifyHeap{}
    }
    return &MultiQueue{...}
}
```

**对比分析**：

| 维度 | 动态创建（当前） | 预定义队列 |
|-----|-----------------|----------|
| **灵活性** | 高：任意 queueType 可用 | 低：必须预先知道类型 |
| **内存占用** | 按需分配 | 预先分配（可能浪费） |
| **类型安全** | 弱：int 作为 key，易误用 | 强：可用枚举类型 |
| **Round-Robin 稳定性** | 差：队列数量动态变化 | 好：队列数量固定 |

**当前设计胜在**：
- **扩展性**：新增队列类型无需修改 MultiQueue 代码。
- **内存效率**：只为实际使用的队列类型分配内存。

**当前设计输在**：
- **调试困难**：`queueType` 是 int，不直观（应该用常量或枚举）。
- **Round-Robin 不稳定**：新队列加入会改变轮询顺序。

**改进建议**：
```go
// 在上层定义常量
const (
    QueueTypeRaftSnapshot = 1
    QueueTypeReplicaGC    = 2
    QueueTypeSplit        = 3
)
```

#### 权衡 4：堆索引缓存 vs 线性查找

**当前选择**：每个 Task 存储 `heapIdx`（堆中的索引）

**替代方案**：取消时线性查找
```go
func (h *notifyHeap) tryRemove(task *Task) bool {
    for i, t := range *h {
        if t == task {
            heap.Remove(h, i)
            return true
        }
    }
    return false
}
```

**对比分析**：

| 维度 | 缓存索引（当前） | 线性查找 |
|-----|-----------------|---------|
| **取消复杂度** | O(log N)（直接 Remove） | O(N)（查找）+ O(log N)（Remove） |
| **内存占用** | +8 bytes per Task | 无额外开销 |
| **一致性维护** | 需要在 Swap/Push/Pop 时更新 | 无需维护 |
| **适用场景** | 取消频繁 | 取消罕见 |

**当前设计胜在**：
- **高效取消**：O(log N) vs O(N)，在队列长时差异显著。
- **适配 CockroachDB**：Replica 操作可能因超时或 context 取消频繁被取消。

**当前设计输在**：
- **内存开销**：每个 Task 增加 8 字节。
- **一致性负担**：堆操作需要额外维护 `heapIdx`。

### 7.2 替代架构分析

#### 替代方案 1：基于 Channel 的信号量

**实现思路**：
```go
type Semaphore struct {
    permits chan struct{}
}

func NewSemaphore(n int) *Semaphore {
    permits := make(chan struct{}, n)
    for i := 0; i < n; i++ {
        permits <- struct{}{}
    }
    return &Semaphore{permits}
}

func (s *Semaphore) Acquire() {
    <-s.permits
}

func (s *Semaphore) Release() {
    s.permits <- struct{}{}
}
```

**与 MultiQueue 对比**：

| 特性 | MultiQueue | Channel 信号量 |
|-----|-----------|---------------|
| **优先级** | 支持（堆） | 不支持（FIFO） |
| **队列隔离** | 支持（Round-Robin） | 不支持 |
| **队列长度限制** | 支持（maxQueueLength） | 不支持（channel 无限缓冲） |
| **取消语义** | 复杂（竞态处理） | 简单（不占用 permit） |

**结论**：Channel 信号量适合简单并发控制，MultiQueue 适合复杂调度需求。

#### 替代方案 2：分层限流器

**实现思路**：
```
┌──────────────────────────┐
│  Global Rate Limiter     │  ← 全局速率限制
└────────┬─────────────────┘
         │
    ┌────┴────┬────────┐
    ↓         ↓        ↓
┌────────┐ ┌────────┐ ┌────────┐
│Queue 1 │ │Queue 2 │ │Queue 3 │  ← 每个队列独立限流
└────────┘ └────────┘ └────────┘
```

**优势**：
- **更细粒度控制**：全局 + 局部双重限制。
- **防止单一队列霸占**：每个队列有自己的配额。

**劣势**：
- **复杂度高**：需要管理多层限流器。
- **配置困难**：需要调优每个队列的限制。

**适用场景**：
- 多租户系统（每个租户一个队列）。
- 需要严格 QoS 保证的系统。

### 7.3 性能权衡分析

#### 时间复杂度对比

| 操作 | 当前实现 | 最优理论 | 差距原因 |
|-----|---------|---------|---------|
| Add | O(log N_tasks) | O(log N_tasks) | 堆插入 |
| Cancel | O(log N_tasks) | O(log N_tasks) | 堆删除 |
| Release | O(N_queues) | O(1) | Round-Robin 遍历 |
| tryRunNextLocked | O(N_queues) | O(1) | 查找非空队列 |

**瓶颈分析**：
- **Release 的 O(N_queues)**：如果队列数量很多（>100），遍历开销显著。
- **改进方案**：维护非空队列的链表。

```go
type MultiQueue struct {
    // ... 现有字段
    nonEmptyQueues []int  // 非空队列的索引列表
}
```

**改进后复杂度**：O(1)，但增加维护开销（Add/Release 时更新列表）。

#### 内存占用对比

**当前实现**：
```
MultiQueue: 48 + 24*N_queues bytes
  - mu: 8 bytes
  - concurrencyLimit: 8 bytes
  - remainingRuns: 8 bytes
  - mapping: 8 + 8*N_queues bytes (map overhead)
  - lastQueueIndex: 8 bytes
  - outstanding: 24*N_queues bytes (slice header + data)

每个 Task: 48 bytes
  - priority: 8 bytes
  - queueType: 8 bytes
  - heapIdx: 8 bytes
  - permitC: 16 bytes (channel)
```

**如果去掉 heapIdx**：每个 Task 节省 8 bytes，但取消操作变慢。

**如果使用全局堆**：节省 `24*N_queues` bytes，但失去隔离性。

### 7.4 并发性能权衡

#### 锁竞争分析

**锁持有时间**：
- `Add()`：O(log N_tasks) + O(N_queues)
- `Cancel()`：O(log N_tasks) + O(1)
- `Release()`：O(1) + O(N_queues)

**高竞争场景**：
- 大量并发 `Add()` 调用。
- 频繁 `Release()` 触发调度。

**改进方案**：分片锁（Sharded Lock）
```go
type ShardedMultiQueue struct {
    shards []*MultiQueue  // 每个分片独立加锁
}
```

**权衡**：
- **优势**：降低锁竞争，提升并发性能。
- **劣势**：失去全局公平性，实现复杂。

### 7.5 工程实践权衡

#### 当前设计的优势

1. **代码简洁**：~300 行代码，易于理解和维护。
2. **测试覆盖**：测试用例覆盖多种场景（正常、取消、压力）。
3. **零依赖**：只依赖 Go 标准库（`container/heap`）。
4. **类型安全**：使用 Go 泛型前的合理抽象（`interface{}`）。

#### 当前设计的不足

1. **调试困难**：`queueType` 是 int，日志中不直观。
2. **监控缺失**：无内置指标（队列长度、等待时间、取消率）。
3. **配置僵化**：`concurrencyLimit` 是全局的，无法按队列类型差异化。
4. **错误处理**：只有 `panic`（重复释放）和 `error`（队列过长），无细粒度错误码。

#### 可能的改进方向

**1. 增加可观测性**
```go
type MultiQueue struct {
    // ... 现有字段
    metrics struct {
        totalAdded    int64
        totalCanceled int64
        totalReleased int64
        maxQueueLen   int64
    }
}
```

**2. 支持队列级配额**
```go
type QueueConfig struct {
    QueueType  int
    MaxTasks   int  // 该队列最多任务数
    Priority   int  // 队列优先级（影响 Round-Robin 权重）
}
```

**3. 增加超时机制**
```go
func (m *MultiQueue) AddWithTimeout(queueType int, priority float64, timeout time.Duration) (*Task, error) {
    // 内部使用 timer + context
}
```

### 7.6 总结：当前设计的适用边界

**最适合的场景**：
- 任务类型多样（3-10 种）
- 需要队列间公平性
- 并发度适中（1-100）
- 取消操作频繁
- 对延迟敏感（需要立即调度）

**不适合的场景**：
- 队列类型极多（>100）：Round-Robin 遍历开销大
- 需要严格全局优先级：当前设计是局部最优
- 需要速率限制：当前设计是并发限制
- 需要分布式调度：当前设计是单机

**扩展方向**：
- **水平扩展**：在 MultiQueue 之上构建分布式调度器（如基于 Raft 的全局队列）。
- **垂直扩展**：增加分片、监控、配额等高级特性。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 八、总结与心智模型（Mental Model）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 8.1 核心思想总结

**一句话概括**：
MultiQueue 是一个**公平的多队列并发控制器**，通过 **Round-Robin 队列选择 + 优先级堆调度 + 令牌桶限流**，在多种异构任务之间实现了**隔离性、公平性与效率的平衡**。

**设计哲学**：
1. **机制与策略分离**：MultiQueue 提供调度机制，上层（如 admission controller）提供策略。
2. **事件驱动，零开销**：无后台线程，完全由 Add/Release/UpdateConcurrencyLimit 触发。
3. **公平优先于最优**：牺牲全局最优调度，换取队列间公平与类型隔离。
4. **防御性设计**：显式检查不变量，panic 暴露使用错误，保证系统鲁棒性。

### 8.2 可复用的心智模型

**模型 1：令牌餐厅（Token Restaurant）**

想象一个餐厅：
- **餐桌数量**：`concurrencyLimit`（固定）
- **可用餐桌**：`remainingRuns`（动态）
- **顾客队列**：`outstanding[]`（按类型分组：家庭、情侣、商务）
- **叫号规则**：Round-Robin 轮流从每个队列叫一桌，队列内按 VIP 等级（优先级）排序
- **取消预订**：顾客可以离开（Cancel），餐桌归还
- **用餐完毕**：顾客离开（Release），餐桌归还，叫下一桌

**关键点**：
- 即使"商务队列"全是 VIP，也不能连续占用所有餐桌，必须给"家庭队列"机会。
- 餐桌数量可以动态调整（扩建或缩减），但不会赶走正在用餐的顾客。

**模型 2：机场登机口（Airport Gate）**

想象一个机场的登机口调度：
- **登机口**：并发槽位
- **航班队列**：不同队列类型（国内、国际、廉航）
- **乘客优先级**：头等舱、经济舱
- **调度规则**：登机口空闲时，轮流为每个航班类型分配（Round-Robin），每个航班内头等舱先登机（优先级）
- **航班取消**：已排队的乘客可以离开，登机口重新分配

**模型 3：操作系统进程调度（OS Scheduler Analogy）**

| 概念 | 操作系统 | MultiQueue |
|-----|---------|-----------|
| 进程 | 不同程序 | 不同队列类型（queueType） |
| 线程 | 进程内的执行单元 | 队列内的任务（Task） |
| CPU 核心 | 物理资源 | 并发槽位（concurrencyLimit） |
| 调度器 | 内核调度器 | tryRunNextLocked() |
| 时间片轮转 | Round-Robin | 队列间轮询 |
| 优先级 | Nice 值 | Task.priority |
| 阻塞/唤醒 | 信号量 | permitC channel |

### 8.3 设计原则提炼

从 MultiQueue 中可以提炼的通用设计原则：

**1. 两级调度模式**
```
全局调度（公平性）+ 局部调度（效率）
      ↓                    ↓
  Round-Robin        Priority Queue
```
适用场景：多租户系统、混合工作负载调度。

**2. 事件驱动优于轮询**
```
❌ 定时器轮询：for { timer.Sleep(); checkQueue() }
✅ 事件触发：  Add/Release 时调用 tryRunNext()
```
降低 CPU 开销，减少延迟。

**3. 令牌归还式限流**
```
❌ 固定速率：每秒补充 N 个令牌
✅ 归还驱动：任务完成时归还令牌
```
更强的背压机制，自适应负载。

**4. 惰性初始化**
```
❌ 预分配：NewQueue(types []int)
✅ 懒惰创建：遇到新 queueType 时创建
```
节省内存，提升灵活性。

**5. 防御性编程**
```
- 不变量检查：remainingRuns >= 0
- 重复释放检测：permit.valid
- Fail-fast：panic 而不是静默错误
```
早期暴露 bug，易于调试。

### 8.4 高度抽象的伪代码

```pseudo
function MultiQueue:
    state:
        concurrencyLimit: int              // 最大并发数
        remainingRuns: int                 // 当前可用槽位
        queues: Map<Type, PriorityHeap>   // 类型 -> 优先级堆
        lastQueue: Type                    // Round-Robin 指针

    function Add(type, priority):
        if remainingRuns == 0 and QueueTooLong():
            return Error

        task = NewTask(type, priority)
        queues[type].Push(task)

        TryScheduleNext()
        return task

    function TryScheduleNext():
        if remainingRuns == 0:
            return

        // Round-Robin: 从上次位置的下一个开始
        for each queue starting from lastQueue+1:
            if queue.NotEmpty():
                task = queue.PopMax()       // 堆顶（最高优先级）
                SendPermit(task)
                remainingRuns -= 1
                lastQueue = queue
                return

    function Release(permit):
        assert permit.valid
        permit.valid = false

        if remainingRuns < concurrencyLimit:
            remainingRuns += 1

        TryScheduleNext()

    function Cancel(task):
        if queue.Remove(task):
            Close(task.permitChannel)      // 通知等待者
        else:
            // 任务已被调度，尝试抢回 Permit
            if TryRecv(task.permitChannel):
                Release(permit)
```

### 8.5 关键要点速查

**核心数据结构**：
- `MultiQueue`：全局协调器，管理并发限制与队列
- `Task`：任务句柄，携带优先级与通知 channel
- `notifyHeap`：优先级堆，按 priority 排序
- `Permit`：令牌，代表执行权限

**核心操作流程**：
```
Add → 检查队列长度 → 插入堆 → 尝试调度
Release → 归还槽位 → 尝试调度
Cancel → 从堆移除 → 关闭 channel（或抢回 Permit）
```

**核心不变量**：
1. `0 ≤ remainingRuns ≤ concurrencyLimit`
2. `heapIdx < 0` ⇔ 任务不在堆中
3. `permit.valid = true` ⇔ Permit 未被释放

**核心模式**：
- 令牌桶（并发控制）
- 优先级队列（堆）
- 调度器（Round-Robin）
- 观察者（Channel 通知）
- 对象池（槽位管理）

### 8.6 实战应用指导

**何时使用 MultiQueue**：
- 需要在多种任务类型间保证公平性
- 需要并发限制（而非速率限制）
- 任务可以被取消
- 任务类型数量适中（<50）

**何时不使用 MultiQueue**：
- 只有单一任务类型（用简单的信号量）
- 需要严格全局优先级（用单一优先级队列）
- 需要速率限制（用 Token Bucket 或 Rate Limiter）
- 需要分布式调度（需要更复杂的系统）

**集成建议**：
```go
// 1. 创建 MultiQueue
mq := NewMultiQueue(10)

// 2. 定义队列类型常量
const (
    QueueRaftSnapshot = 1
    QueueReplicaGC    = 2
)

// 3. 提交任务
task, err := mq.Add(QueueRaftSnapshot, 9.0, 100)
if err != nil {
    // 队列过长，执行背压策略
    return err
}

// 4. 等待并执行
select {
case permit := <-task.GetWaitChan():
    if permit == nil {
        // 任务被取消
        return
    }
    defer mq.Release(permit)

    // 执行实际工作
    doWork()

case <-ctx.Done():
    mq.Cancel(task)
    return ctx.Err()
}
```

### 8.7 进一步学习路径

**相关技术**：
1. **Go 并发原语**：`sync.Mutex`、`chan`、`context`
2. **数据结构**：堆（Heap）、优先级队列
3. **调度算法**：Round-Robin、CFS、DRF
4. **流量控制**：Token Bucket、Leaky Bucket、BBR

**推荐阅读**：
1. Go 标准库 `container/heap` 源码
2. Kubernetes 调度器源码（多级队列调度）
3. TCP 拥塞控制算法（令牌桶应用）
4. CockroachDB admission control 层（MultiQueue 的上层策略）

**扩展实验**：
1. 修改 Round-Robin 为 Weighted Round-Robin（队列权重）
2. 增加监控指标（队列长度、等待时间分布）
3. 实现分片 MultiQueue（降低锁竞争）
4. 对比不同调度策略的性能（压测）

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 附录：代码索引
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

**核心函数位置**：
- [NewMultiQueue](pkg/kv/kvserver/multiqueue/multi_queue.go#L104-L113)
- [Add](pkg/kv/kvserver/multiqueue/multi_queue.go#L163-L198)
- [tryRunNextLocked](pkg/kv/kvserver/multiqueue/multi_queue.go#L123-L141)
- [Cancel](pkg/kv/kvserver/multiqueue/multi_queue.go#L202-L229)
- [Release](pkg/kv/kvserver/multiqueue/multi_queue.go#L234-L238)
- [UpdateConcurrencyLimit](pkg/kv/kvserver/multiqueue/multi_queue.go#L146-L158)

**测试用例参考**：
- [TestMultiQueueTwoQueues](pkg/kv/kvserver/multiqueue/multi_queue_test.go#L50-L66)：Round-Robin 验证
- [TestMultiQueueComplex](pkg/kv/kvserver/multiqueue/multi_queue_test.go#L72-L90)：复杂调度场景
- [TestMultiQueueCancelInProgress](pkg/kv/kvserver/multiqueue/multi_queue_test.go#L127-L211)：取消竞态处理
- [TestMultiQueueStress](pkg/kv/kvserver/multiqueue/multi_queue_test.go#L216-L261)：并发压力测试

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**END**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

