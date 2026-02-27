# Raft Log Truncator 深度剖析——基于持久化感知的松耦合 Raft 日志截断机制（上篇）

> **背景**: 本文深入分析 CockroachDB 中 **Raft Log Truncator** 的设计与实现，重点讲解 Store 初始化时创建截断器并注册持久化回调的机制，以及整个松耦合截断架构的设计哲学。
>
> **目标读者**: 具备扎实后端与系统基础、理解 Raft 协议基本概念，但尚未深入阅读该代码的工程师。

---

## 一、第一轮 BFS：职责边界与设计动机（Why）

### 1.1 问题背景：Raft Log 的"垃圾回收困境"

在基于 Raft 的分布式系统中，**Raft log** 是共识协议的核心数据结构：

```
Raft Log（逻辑视图）：
┌─────────────────────────────────────────────────────────┐
│ Index: 1    2    3    4    5    6    7    8    9    10  │
│ Entry: [A] [B] [C] [D] [E] [F] [G] [H] [I] [J]         │
│                                                          │
│ Applied Index: 7  ← 状态机已应用到这里                  │
│ Committed Index: 9 ← Raft 已提交到这里                  │
└─────────────────────────────────────────────────────────┘
```

**核心矛盾**：
1. **无限增长的日志**：
   - 每个写操作都追加一条 Raft entry
   - 高 QPS 场景下，日志每秒可能增长数万条
   - 磁盘空间和内存会被耗尽

2. **不能随意删除**：
   - 落后的 Follower 需要通过日志追赶进度
   - 新加入的 Replica 需要通过日志初始化
   - 过早删除会导致必须发送昂贵的 Snapshot

3. **经典的"截断安全性"问题**：
   ```
   时间线：
   T0: Applied Index = 100, 截断到 index 95
       └─ 删除 [1, 95] 的日志

   T1: 系统崩溃，状态机丢失部分未刷盘的修改
       └─ 重启后 Applied Index 回退到 92

   T2: 致命错误！
       └─ 无法重放 [92, 95] 的日志（已被删除）
       └─ 状态机与 Raft log 永久不一致
   ```

### 1.2 传统方案的局限性

**方案 A：同步截断（Tightly Coupled Truncation）**

```go
// 传统方式（伪代码）
func HandleTruncateLog(index RaftIndex) {
    // 1. 立即删除日志
    deleteLogEntries(1, index)

    // 2. 同步刷盘（强制 fsync）
    syncToDisk()

    // 3. 更新元数据
    updateTruncatedState(index)
}
```

**问题**：
- ❌ **性能瓶颈**：每次截断都需要 fsync，延迟 1-10ms
- ❌ **阻塞前台**：影响正常的 Raft 日志写入
- ❌ **可扩展性差**：单个 Store 可能有数千个 Range，无法并发处理

**方案 B：批量截断（Batched Truncation）**

```go
func batchTruncate() {
    batch := collectTruncations() // 收集所有待截断的请求
    deleteLogEntries(batch)
    syncToDisk() // 批量 fsync
}
```

**问题**：
- ✅ 减少了 fsync 次数
- ❌ 仍然需要主动触发 fsync
- ❌ 难以确定何时是"安全"的截断时机

### 1.3 CockroachDB 的"松耦合截断"创新

CockroachDB 从 **v22.2** 开始引入 **Loosely Coupled Raft Log Truncation**，核心思想：

> **"不主动 fsync，而是等待系统自然的持久化事件"**

**设计哲学**：
```
传统方案：
    截断请求 → 立即 fsync → 删除日志
    (主动触发，阻塞式)

CockroachDB 方案：
    截断请求 → 加入待处理队列 → 异步等待
                                      ↓
    Pebble Flush → 触发回调 → 检查并执行截断
    (被动触发，事件驱动)
```

**关键洞察**：
1. **Pebble 的 Flush 本身就是持久化**：
   - 正常的写入操作会触发 memtable flush
   - Flush 后，数据已经持久化到 SST
   - **不需要额外的 fsync**

2. **RaftAppliedIndex 的持久化语义**：
   - 存储在 `RangeAppliedState` 中
   - 当 flush 完成，已提交的 RaftAppliedIndex 变为持久化
   - 此时截断到该 index 是安全的

3. **延迟截断的可接受性**：
   - 延迟几秒钟截断不影响正确性
   - 只是暂时多占用一些磁盘空间
   - 换取的是**零额外 fsync 开销**

### 1.4 Raft Log Truncator 的核心职责

`raftLogTruncator` 是实现这一机制的核心组件：

| 职责维度 | 具体内容 |
|---------|---------|
| **接收截断提议** | 从 Raft log queue 接收截断请求，加入待处理队列 |
| **监听持久化事件** | 注册回调到 Pebble 的 FlushEnd 事件 |
| **批量执行截断** | 当 flush 完成时，检查所有 Range 并执行安全的截断 |
| **并发控制** | 保证同一 Range 不会并发截断 |
| **资源优化** | 避免 Raft log 无限增长，释放磁盘空间 |

**如果没有 raftLogTruncator**：
- ❌ 每次截断都需要显式 fsync，性能损失 10-100 倍
- ❌ 无法批量处理多个 Range 的截断
- ❌ 需要复杂的定时器机制来触发截断
- ❌ 难以保证"仅在持久化后截断"的安全性

### 1.5 在系统中的位置

```
┌────────────────────────────────────────────────────────────┐
│                     SQL Layer                              │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│                    KV Layer                                │
│  TxnCoordSender → DistSender → RangeCache                 │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│                   Store Layer                              │
│  ┌────────────────────────────────────────────────────┐   │
│  │                   Store                            │   │
│  │  ┌──────────────────────────────────────────────┐ │   │
│  │  │   raftLogTruncator (本文焦点)                │ │   │
│  │  │  • 监听 Pebble flush 事件                    │ │   │
│  │  │  • 管理所有 Range 的待截断队列               │ │   │
│  │  │  • 异步批量执行安全截断                      │ │   │
│  │  └──────────────────────────────────────────────┘ │   │
│  │                                                    │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌──────────┐ │   │
│  │  │  Replicas   │  │ Raft Queue  │  │ Scanner  │ │   │
│  │  │ (每个有待   │  │ (提议截断)  │  │          │ │   │
│  │  │  截断队列)  │  │             │  │          │ │   │
│  │  └─────────────┘  └─────────────┘  └──────────┘ │   │
│  └────────────────────────────────────────────────────┘   │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│            Storage Engine (Pebble)                         │
│  ┌──────────────────────────────────────────────────┐     │
│  │  FlushEnd Event                                  │     │
│  │  └─> 触发 raftLogTruncator.durabilityAdvanced() │     │
│  └──────────────────────────────────────────────────┘     │
│  MVCC keys + Raft log entries                             │
└────────────────────────────────────────────────────────────┘
```

**上游调用者**：
- **Raft Log Queue**：定期提议截断，调用 `addPendingTruncation()`
- **Replica Apply**：应用 `TruncateLogRequest` 时注册待截断

**下游依赖**：
- **Pebble Engine**：注册 `FlushEnd` 回调
- **Replica**：通过 `replicaForTruncator` 接口操作 Replica 状态
- **Stopper**：生命周期管理

### 1.6 核心抽象与生命周期

**核心结构体** (`raft_log_truncator.go:188-203`):

```go
type raftLogTruncator struct {
    ambientCtx context.Context
    store      storeForTruncator  // Store 接口（获取 Replica 和 Engine）
    stopper    *stop.Stopper

    mu struct {
        syncutil.Mutex
        // 待处理的 Range 集合（批量交换模式）
        addRanges, drainRanges map[roachpb.RangeID]struct{}

        // 后台任务状态
        runningTruncation  bool  // 是否有 goroutine 正在执行截断
        queuedDurabilityCB bool  // 是否有排队的持久化回调
    }
}
```

**每个 Replica 的待截断队列** (`raft_log_truncator.go:42-70`):

```go
type pendingLogTruncations struct {
    mu struct {
        syncutil.Mutex
        // 固定大小的队列（最多 2 个条目）
        // truncs[0] = 最老的待截断
        // truncs[1] = 所有后续截断的合并
        truncs [2]pendingTruncation
    }
}
```

**生命周期**：
```
T0: Store.NewStore()
    └─ 创建 raftLogTruncator (makeRaftLogTruncator)

T1: Store.Start()
    └─ 注册回调: s.StateEngine().RegisterFlushCompletedCallback(
                     truncator.durabilityAdvancedCallback
                 )

T2: 运行时（持续触发）
    ├─ Pebble Flush → durabilityAdvancedCallback()
    ├─ 启动异步 goroutine → durabilityAdvanced()
    ├─ 遍历所有待处理 Range → tryEnactTruncations()
    └─ 执行安全的截断

T3: Store.Stop()
    └─ Stopper 关闭，所有异步任务优雅退出
```

---

## 二、第二轮 BFS：控制流与组件协作（How it flows）

### 2.1 主要执行路径

#### 路径 A：截断提议的入队（由 Raft Log Queue 触发）

```
时间线（截断请求流程）：

T0: Raft Log Queue 扫描到 Range R123
    │  发现日志大小超过阈值（64 MB）
    │
T1: 计算截断点 newIndex = 5000
    │  (保留最近 2048 条日志用于追赶)
    │
T2: 提议 TruncateLogRequest
    │  通过 Raft 共识复制到所有 Replica
    │
T3: Replica 应用截断命令
    │  调用 raftLogTruncator.addPendingTruncation()
    │
    ├─ 计算 sideloaded entries 大小（如 snapshots）
    │  logDeltaBytes = -58 MB  (删除的字节数)
    │
    ├─ 决定队列位置
    │  如果 truncs[0] 为空 → 加入 truncs[0]
    │  如果 truncs[0] 已满 → 合并到 truncs[1]
    │
    └─ 如果是第一个待截断
       └─ enqueueRange(R123)  // 加入全局待处理集合
```

**关键决策点**：
- **为什么最多 2 个条目**？
  ```
  场景：连续收到 3 个截断请求
  Request 1: index=1000
  Request 2: index=2000
  Request 3: index=3000

  队列状态：
  truncs[0] = Request 1 (等待持久化到 index 1000)
  truncs[1] = merge(Request 2, Request 3) = index 3000

  假设 truncs[0] 永远无法执行（极端情况）：
  - truncs[1] 最终会被执行（因为 index 3000 会持久化）
  - 虽然丢失了 Request 2，但最终截断到 3000 仍然是安全的

  如果只有 1 个条目：
  - 可能永远卡在 index 1000
  - 后续截断永远无法执行
  ```

#### 路径 B：持久化事件触发的批量截断（被动回调）

```
时间线（Flush 触发流程）：

T0: Pebble 内部触发 memtable flush
    │  原因：memtable 达到 64 MB 或定时触发
    │
    ├─ 将内存数据写入 L0 SST
    ├─ 持久化到磁盘
    └─ 触发 FlushEnd 事件

T1: Pebble 调用注册的回调
    └─ p.mu.flushCompletedCallback()
        └─ truncator.durabilityAdvancedCallback()

T2: durabilityAdvancedCallback() 逻辑
    ├─ 检查是否已有 goroutine 在运行
    │  if runningTruncation → 设置 queuedDurabilityCB = true
    │  else → 启动新 goroutine
    │
    └─ stopper.RunAsyncTask("raft-log-truncation", ...)

    ┌──────────────────────────────────────────────┐
    │  异步 Goroutine（与 Pebble 回调分离）      │
    ├──────────────────────────────────────────────┤
    │                                              │
T3: │  durabilityAdvanced()                       │
    │  ├─ 交换队列：addRanges ⇄ drainRanges      │
    │  │  (避免长时间持锁)                         │
    │  │                                           │
    │  ├─ 创建 GuaranteedDurability Reader        │
    │  │  (只读取已持久化的数据)                   │
    │  │                                           │
    │  └─ 遍历 drainRanges 中的每个 RangeID      │
    │      │                                       │
T4: │      └─ tryEnactTruncations(R123, reader)  │
    │          │                                   │
    │          ├─ 获取 Replica (加锁 raftMu)      │
    │          │                                   │
    │          ├─ 读取持久化的 RaftAppliedIndex   │
    │          │  appliedIndex = 5500 (持久化)    │
    │          │                                   │
    │          ├─ 检查待截断队列                  │
    │          │  truncs[0].Index = 5000          │
    │          │  5000 <= 5500 → 可以执行！       │
    │          │                                   │
    │          ├─ 执行截断                         │
    │          │  handleTruncatedStateBelowRaft() │
    │          │  └─ 删除 [oldIndex, 5000] 的日志 │
    │          │                                   │
    │          ├─ 更新 Replica 状态               │
    │          │  stagePendingTruncation()        │
    │          │  finalizeTruncation()            │
    │          │                                   │
    │          └─ 提交 WriteBatch                 │
    │              sync = hasSideloaded           │
    │              (如果有 sideloaded，需要 fsync)│
    │                                              │
T5: │  检查是否有排队的回调                       │
    │  if queuedDurabilityCB → 再次执行          │
    │  else → runningTruncation = false          │
    │                                              │
    └──────────────────────────────────────────────┘
```

**批量优化的体现**：
```
单次 Flush 可能影响：
├─ Range 1: Applied Index 100 → 150  (可截断到 148)
├─ Range 2: Applied Index 200 → 250  (可截断到 248)
├─ Range 3: Applied Index 300 → 350  (可截断到 348)
└─ Range N: ...

一次 durabilityAdvanced() 调用：
└─ 遍历所有 Range，批量执行截断
   └─ 共享一个 GuaranteedDurability Reader
   └─ 减少系统调用开销
```

### 2.2 触发方式分类

| 触发源 | 触发方式 | 同步/异步 | 关键方法 |
|--------|---------|-----------|---------|
| **Raft Log Queue** | 定时扫描（默认 1 分钟） | 异步提议 | `addPendingTruncation()` |
| **Replica Apply** | 应用 TruncateLogRequest | 异步入队 | `addPendingTruncation()` |
| **Pebble Flush** | FlushEnd 事件 | 异步回调 | `durabilityAdvancedCallback()` |
| **Stopper Quiesce** | 优雅关闭 | 中断执行 | 检查 `ShouldQuiesce()` |

**关键特性**：
1. **完全异步**：
   - 截断提议不等待执行
   - 持久化回调不阻塞 Pebble
   - 执行截断在独立 goroutine

2. **事件驱动**：
   - 不依赖定时器
   - 不主动轮询
   - 完全依赖 Pebble 的自然 flush

3. **单线程执行**：
   - 同一时刻最多 1 个 goroutine 执行截断
   - 避免并发冲突和资源竞争

### 2.3 与其他模块交互

**关键交互点**：

1. **与 Pebble 协作**（核心）：
   ```go
   // Store.Start() 中
   s.StateEngine().RegisterFlushCompletedCallback(func() {
       truncator.durabilityAdvancedCallback()
   })
   ```
   - **触发时机**：Pebble 完成 memtable flush 后
   - **回调语义**：此时所有之前的写入已持久化
   - **性能关键**：回调必须快速返回，不能阻塞 Pebble

2. **与 Replica 协作**：
   ```go
   type replicaForTruncator interface {
       getTruncatedState() RaftTruncatedState
       stagePendingTruncation(pendingTruncation)
       finalizeTruncation(context.Context)
       getPendingTruncs() *pendingLogTruncations
       sideloadedStats(RaftSpan) (entries, size, error)
   }
   ```
   - **锁定策略**：`raftMu` 必须在整个截断过程持有
   - **状态更新**：两阶段（stage + finalize）
   - **并发保证**：同一 Replica 不会并发截断

3. **与 Stopper 协作**：
   ```go
   stopper.RunAsyncTask(ctx, "raft-log-truncation", func(ctx) {
       for {
           durabilityAdvanced(ctx)
           // 检查是否有排队的回调
           if !queuedDurabilityCB {
               break
           }
       }
   })
   ```
   - **优雅关闭**：检查 `ShouldQuiesce()`
   - **任务管理**：通过 Stopper 统一管理生命周期

### 2.4 状态流转图

```
┌──────────────────────────────────────────────────────────────┐
│         Raft Log 截断生命周期（单个 Range）                  │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  [日志增长]                                                  │
│      │  Raft log size > 64 MB                               │
│      ▼                                                       │
│  ┌─────────────────┐                                        │
│  │ Queue 提议截断  │                                        │
│  └─────────────────┘                                        │
│      │  TruncateLogRequest                                  │
│      ▼                                                       │
│  ┌─────────────────┐                                        │
│  │ Raft 共识复制   │                                        │
│  └─────────────────┘                                        │
│      │  复制到所有 Replica                                  │
│      ▼                                                       │
│  ┌─────────────────────────────────┐                        │
│  │ Apply: addPendingTruncation     │                        │
│  └─────────────────────────────────┘                        │
│      │                                                       │
│      ├─ pendingTruncs[0] = {index: 5000, delta: -58MB}     │
│      └─ enqueueRange(R123)                                  │
│      │                                                       │
│      ▼                                                       │
│  ┌──────────────────────────────┐                           │
│  │ 等待持久化                   │◄────────────┐             │
│  │ (Pending 状态)               │             │             │
│  └──────────────────────────────┘             │             │
│      │                                         │             │
│      │ (可能等待数秒到数分钟)                  │             │
│      │                                         │             │
│      ▼                                         │             │
│  ┌──────────────────────────────┐             │             │
│  │ Pebble Flush Event           │             │             │
│  │ RaftAppliedIndex 持久化      │             │             │
│  └──────────────────────────────┘             │             │
│      │                                         │             │
│      ├─ durabilityAdvancedCallback()          │             │
│      └─ 启动异步 goroutine                     │             │
│      │                                         │             │
│      ▼                                         │             │
│  ┌──────────────────────────────┐             │             │
│  │ tryEnactTruncations()        │             │             │
│  │ 读取持久化 AppliedIndex      │             │             │
│  └──────────────────────────────┘             │             │
│      │                                         │             │
│      ├─ if appliedIndex < truncIndex ─────────┘             │
│      │   (尚未持久化，重新入队)              (循环)          │
│      │                                                       │
│      ├─ if appliedIndex >= truncIndex                       │
│      │   (可以安全截断)                                      │
│      │                                                       │
│      ▼                                                       │
│  ┌──────────────────────────────┐                           │
│  │ 执行截断                     │                           │
│  │ handleTruncatedState...()    │                           │
│  └──────────────────────────────┘                           │
│      │                                                       │
│      ├─ 删除 [oldIndex, newIndex] 的日志                    │
│      ├─ 更新 Replica 状态                                   │
│      ├─ 释放 sideloaded 文件                                │
│      └─ Commit WriteBatch (可选 sync)                       │
│      │                                                       │
│      ▼                                                       │
│  ┌──────────────────────────────┐                           │
│  │ 截断完成                     │                           │
│  │ 磁盘空间释放                 │                           │
│  └──────────────────────────────┘                           │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 Store 初始化中的创建与注册

让我们从您提供的代码片段开始深入分析（`store.go` 中）：

```go
// Create the raft log truncator and register the callback.
s.raftTruncator = makeRaftLogTruncator(s.cfg.AmbientCtx, (*storeForTruncatorImpl)(s), stopper)
{
    truncator := s.raftTruncator
    // When state machine has persisted new RaftAppliedIndex, fire callback.
    s.StateEngine().RegisterFlushCompletedCallback(func() {
        truncator.durabilityAdvancedCallback()
    })
}
```

**逐行分析**：

#### 步骤 1：创建 truncator

```go
s.raftTruncator = makeRaftLogTruncator(
    s.cfg.AmbientCtx,
    (*storeForTruncatorImpl)(s),  // 类型转换：Store → storeForTruncator 接口
    stopper,
)
```

**关键点**：
- **类型转换的意义**：
  ```go
  type storeForTruncatorImpl Store  // 新类型定义

  var _ storeForTruncator = &storeForTruncatorImpl{}  // 接口实现
  ```
  - 不是简单的接口断言，而是创建了一个**新类型**
  - 为 `Store` 添加了专门的方法集（`acquireReplicaForTruncator` 等）
  - **设计模式**：**Adapter Pattern（适配器模式）**
    - Store 本身不应该暴露 truncator 专用接口
    - `storeForTruncatorImpl` 作为适配器，仅暴露必要的方法

- **为什么需要 `storeForTruncator` 接口**？
  ```go
  type storeForTruncator interface {
      acquireReplicaForTruncator(rangeID) replicaForTruncator
      releaseReplicaForTruncator(r replicaForTruncator)
      getEngine() storage.Engine
  }
  ```
  - **解耦**：truncator 不依赖整个 Store 结构
  - **测试友好**：可以 mock 这个接口
  - **接口隔离原则**：只暴露 truncator 需要的方法

#### 步骤 2：注册持久化回调

```go
s.StateEngine().RegisterFlushCompletedCallback(func() {
    truncator.durabilityAdvancedCallback()
})
```

**深入分析**：

1. **StateEngine() 的语义**：
   ```go
   func (s *Store) StateEngine() storage.Engine {
       return s.internalEngines.StateEngine
   }
   ```
   - 在分离的 Raft log 架构中，有两个引擎：
     - **LogEngine**: 存储 Raft log entries
     - **StateEngine**: 存储状态机数据（包括 `RangeAppliedState`）
   - 这里注册到 StateEngine，因为 **RaftAppliedIndex 存储在状态机中**

2. **RegisterFlushCompletedCallback 的实现**（`pebble.go:2433-2437`）：
   ```go
   func (p *Pebble) RegisterFlushCompletedCallback(cb func()) {
       p.mu.Lock()
       p.mu.flushCompletedCallback = cb
       p.mu.Unlock()
   }
   ```
   - **单例模式**：只能注册一个回调
   - **后注册覆盖前者**：Store 是唯一的注册者

3. **回调触发时机**（`pebble.go:1440-1449`）：
   ```go
   FlushEnd: func(info pebble.FlushInfo) {
       if info.Err != nil {
           return  // Flush 失败，不触发回调
       }
       p.mu.Lock()
       cb := p.mu.flushCompletedCallback
       p.mu.Unlock()
       if cb != nil {
           cb()  // 在 Pebble 的回调 goroutine 中执行
       }
   },
   ```
   - **触发条件**：Pebble 成功完成 memtable flush
   - **执行环境**：在 Pebble 的内部 goroutine 中
   - **性能要求**：回调必须快速返回，不能阻塞 Pebble

**为什么这样设计**？

```
问题：如何知道 RaftAppliedIndex 已持久化？

方案 A（不可行）：主动轮询
    while True:
        if isAppliedIndexDurable():
            doTruncate()
        sleep(100ms)

    问题：
    ✗ 浪费 CPU 资源
    ✗ 延迟不可控
    ✗ 难以确定何时检查

方案 B（不可行）：显式 fsync
    updateAppliedIndex()
    fsync()  // 强制刷盘
    doTruncate()

    问题：
    ✗ 性能开销巨大（1-10ms 延迟）
    ✗ 破坏批量优化

方案 C（当前方案）：监听 Flush 事件
    Pebble Flush → 回调 → 检查并截断

    优势：
    ✓ 零额外开销（复用 Pebble 的 flush）
    ✓ 自动批量化（一次 flush 触发所有 Range）
    ✓ 事件驱动，无需轮询
```

#### 步骤 3：闭包捕获的妙用

```go
{
    truncator := s.raftTruncator
    s.StateEngine().RegisterFlushCompletedCallback(func() {
        truncator.durabilityAdvancedCallback()
    })
}
```

**为什么不直接写**：
```go
s.StateEngine().RegisterFlushCompletedCallback(func() {
    s.raftTruncator.durabilityAdvancedCallback()
})
```

**原因**：
1. **避免闭包捕获整个 Store**：
   ```go
   // 当前方案：
   truncator := s.raftTruncator  // 只捕获 truncator 指针（8 字节）

   // 如果直接引用 s：
   // 闭包会捕获整个 Store（可能数 KB）
   // 虽然实际是指针，但语义不清晰
   ```

2. **明确依赖关系**：
   - 回调只依赖 `raftTruncator`
   - 不依赖 Store 的其他状态

3. **防止循环引用**（虽然 Go 有 GC，但这是好习惯）：
   ```
   Store → raftTruncator
          ↑                ↓
          └── 回调闭包 ←──┘

   如果闭包捕获 s，则：
   Store → raftTruncator → 回调 → Store (循环)
   ```

### 3.2 核心方法：`makeRaftLogTruncator()`

```go
func makeRaftLogTruncator(
    ambientCtx log.AmbientContext,
    store storeForTruncator,
    stopper *stop.Stopper,
) *raftLogTruncator {
    t := &raftLogTruncator{
        ambientCtx: ambientCtx.AnnotateCtx(context.Background()),
        store:      store,
        stopper:    stopper,
    }
    t.mu.addRanges = make(map[roachpb.RangeID]struct{})
    t.mu.drainRanges = make(map[roachpb.RangeID]struct{})
    return t
}
```

**关键设计点**：

1. **双队列模式**：
   ```go
   addRanges, drainRanges map[roachpb.RangeID]struct{}
   ```
   - **目的**：避免长时间持锁
   - **机制**：
     ```go
     // 入队操作（快速，持锁短）
     func enqueueRange(rangeID) {
         t.mu.Lock()
         t.mu.addRanges[rangeID] = struct{}{}
         t.mu.Unlock()
     }

     // 批量消费（不持锁）
     func durabilityAdvanced() {
         t.mu.Lock()
         t.mu.addRanges, t.mu.drainRanges = t.mu.drainRanges, t.mu.addRanges
         t.mu.Unlock()

         // 现在可以慢慢处理 drainRanges，不阻塞新的入队
         for rangeID := range drainRanges {
             tryEnactTruncations(rangeID)
         }
     }
     ```
   - **好处**：
     - ✅ 入队操作不受处理速度影响
     - ✅ 处理过程中不阻塞新请求
     - ✅ 类似**双缓冲**（Double Buffering）模式

2. **使用 `struct{}` 作为 map 值**：
   ```go
   map[roachpb.RangeID]struct{}
   ```
   - **空结构体不占内存**：`sizeof(struct{}) == 0`
   - **仅用作集合**：只关心 key 是否存在
   - **等价于 set**：Go 标准库缺少 set，用 map 模拟

3. **AnnotateCtx 的作用**：
   ```go
   ambientCtx.AnnotateCtx(context.Background())
   ```
   - 为 truncator 的所有日志添加统一的前缀
   - 例如：`[n1,s1,truncator]` 包含 node ID 和 store ID
   - **可观测性**：方便调试和问题定位

### 3.3 核心方法：`durabilityAdvancedCallback()`

```go
func (t *raftLogTruncator) durabilityAdvancedCallback() {
    runTruncation := false
    t.mu.Lock()
    if !t.mu.runningTruncation && len(t.mu.addRanges) > 0 {
        runTruncation = true
        t.mu.runningTruncation = true
    }
    if !runTruncation && len(t.mu.addRanges) > 0 {
        t.mu.queuedDurabilityCB = true
    }
    t.mu.Unlock()

    if !runTruncation {
        return
    }

    if err := t.stopper.RunAsyncTask(t.ambientCtx, "raft-log-truncation",
        func(ctx context.Context) {
            for {
                t.durabilityAdvanced(ctx)
                shouldReturn := false
                t.mu.Lock()
                queued := t.mu.queuedDurabilityCB
                t.mu.queuedDurabilityCB = false
                if !queued {
                    t.mu.runningTruncation = false
                    shouldReturn = true
                }
                t.mu.Unlock()
                if shouldReturn {
                    return
                }
            }
        }); err != nil {
        // Task did not run because stopper is stopped.
        func() {
            t.mu.Lock()
            defer t.mu.Unlock()
            if !t.mu.runningTruncation {
                panic("expected runningTruncation")
            }
            t.mu.runningTruncation = false
        }()
    }
}
```

**逐段分析**：

#### Part 1: 决定是否启动新任务

```go
t.mu.Lock()
if !t.mu.runningTruncation && len(t.mu.addRanges) > 0 {
    runTruncation = true
    t.mu.runningTruncation = true
}
if !runTruncation && len(t.mu.addRanges) > 0 {
    t.mu.queuedDurabilityCB = true
}
t.mu.Unlock()
```

**状态机**：
```
场景 1：首次回调
    runningTruncation = false
    addRanges = {R1, R2, R3}

    决策：
    → runTruncation = true
    → runningTruncation = true
    → 启动 goroutine

场景 2：回调时已有 goroutine 在运行
    runningTruncation = true
    addRanges = {R4, R5}

    决策：
    → runTruncation = false
    → queuedDurabilityCB = true (标记有待处理)
    → 不启动新 goroutine

场景 3：回调时无待处理 Range
    runningTruncation = false/true
    addRanges = {}

    决策：
    → runTruncation = false
    → 直接返回
```

**不变量**：
```
INV-1: 同一时刻最多 1 个 goroutine 执行 durabilityAdvanced()
       → runningTruncation 保证

INV-2: 如果有待处理 Range 且无 goroutine，必定会启动 goroutine
       → 第一个 if 保证

INV-3: 如果回调时有 goroutine 在运行，必定设置 queuedDurabilityCB
       → 第二个 if 保证，goroutine 会循环处理
```

#### Part 2: 异步任务的循环逻辑

```go
t.stopper.RunAsyncTask(t.ambientCtx, "raft-log-truncation",
    func(ctx context.Context) {
        for {
            t.durabilityAdvanced(ctx)  // 执行实际截断

            shouldReturn := false
            t.mu.Lock()
            queued := t.mu.queuedDurabilityCB
            t.mu.queuedDurabilityCB = false
            if !queued {
                t.mu.runningTruncation = false
                shouldReturn = true
            }
            t.mu.Unlock()

            if shouldReturn {
                return
            }
        }
    })
```

**循环的必要性**：
```
时间线：

T0: durabilityAdvanced() 开始执行
    └─ 处理 addRanges = {R1, R2, R3}

T1: 执行过程中，又触发了 flush
    └─ durabilityAdvancedCallback() 被调用
    └─ queuedDurabilityCB = true

T2: durabilityAdvanced() 完成
    └─ 检查 queuedDurabilityCB
    └─ 发现为 true → 再次执行 durabilityAdvanced()

T3: 第二次 durabilityAdvanced() 完成
    └─ queuedDurabilityCB = false
    └─ 退出循环
```

**为什么不是递归**？
```go
// 如果用递归（不推荐）：
func durabilityAdvancedCallback() {
    if !runningTruncation {
        go func() {
            durabilityAdvanced()
            if queuedDurabilityCB {
                durabilityAdvancedCallback()  // 递归
            }
        }()
    }
}
```
- ❌ 增加 goroutine 数量（每次递归创建新 goroutine）
- ❌ 栈空间浪费
- ✅ 当前的循环方案：单个 goroutine，无栈浪费

#### Part 3: 错误处理

```go
if err := t.stopper.RunAsyncTask(...); err != nil {
    // Task did not run because stopper is stopped.
    func() {
        t.mu.Lock()
        defer t.mu.Unlock()
        if !t.mu.runningTruncation {
            panic("expected runningTruncation")
        }
        t.mu.runningTruncation = false
    }()
}
```

**关键点**：
- **唯一错误原因**：Stopper 已停止（系统正在关闭）
- **状态清理**：必须重置 `runningTruncation`，否则状态机永久卡住
- **panic 的作用**：
  ```go
  if !t.mu.runningTruncation {
      panic("expected runningTruncation")
  }
  ```
  - 断言：如果 RunAsyncTask 失败，`runningTruncation` 必定为 true
  - 这是 **防御性编程**：捕获逻辑错误

### 3.4 核心方法：`durabilityAdvanced()`

```go
func (t *raftLogTruncator) durabilityAdvanced(ctx context.Context) {
    t.mu.Lock()
    // 交换队列
    t.mu.addRanges, t.mu.drainRanges = t.mu.drainRanges, t.mu.addRanges
    drainRanges := t.mu.drainRanges
    t.mu.Unlock()

    if len(drainRanges) == 0 {
        return
    }

    ranges := make([]roachpb.RangeID, 0, len(drainRanges))
    for k := range drainRanges {
        ranges = append(ranges, k)
        delete(drainRanges, k)
    }

    // 排序：确定性测试输出
    sort.Sort(rangesByRangeID(ranges))

    // 创建持久化 Reader
    reader := t.store.getEngine().NewReader(storage.GuaranteedDurability)
    defer reader.Close()

    shouldQuiesce := t.stopper.ShouldQuiesce()
    quiesced := false
    for _, rangeID := range ranges {
        t.tryEnactTruncations(ctx, rangeID, reader)

        select {
        case <-shouldQuiesce:
            quiesced = true
        default:
        }
        if quiesced {
            break
        }
    }
}
```

**逐段分析**：

#### Part 1: 队列交换

```go
t.mu.Lock()
t.mu.addRanges, t.mu.drainRanges = t.mu.drainRanges, t.mu.addRanges
drainRanges := t.mu.drainRanges
t.mu.Unlock()
```

**为什么交换而不是复制**？
```go
// 方案 A（低效）：复制
t.mu.Lock()
drainRanges := make(map[RangeID]struct{})
for k := range t.mu.addRanges {
    drainRanges[k] = struct{}{}
    delete(t.mu.addRanges, k)
}
t.mu.Unlock()

// 方案 B（当前）：交换指针
t.mu.Lock()
t.mu.addRanges, t.mu.drainRanges = t.mu.drainRanges, t.mu.addRanges
drainRanges := t.mu.drainRanges  // 引用，不是复制
t.mu.Unlock()
```
- ✅ **O(1) 时间复杂度**（交换指针）
- ✅ **零分配**（复用 map）
- ✅ **持锁时间极短**

#### Part 2: 排序的必要性

```go
sort.Sort(rangesByRangeID(ranges))
```

**为什么需要排序**？
```go
type rangesByRangeID []roachpb.RangeID

func (r rangesByRangeID) Less(i, j int) bool {
    return r[i] < r[j]
}
```

- **非功能性原因**：确定性测试输出
  ```go
  // 无排序：
  日志: truncating R5, R2, R7, R3 (随机顺序)

  // 有排序：
  日志: truncating R2, R3, R5, R7 (确定顺序)
  ```
- **测试友好**：日志输出可重现，易于调试
- **性能影响**：可忽略（排序开销 << 截断开销）

#### Part 3: GuaranteedDurability Reader

```go
reader := t.store.getEngine().NewReader(storage.GuaranteedDurability)
defer reader.Close()
```

**GuaranteedDurability 的语义**：
```go
// Pebble 实现（简化）
func (p *Pebble) NewReader(durability DurabilityRequirement) Reader {
    if durability == GuaranteedDurability {
        // 只读取已 flush 到 SST 的数据
        // 忽略 memtable 中的数据
        return &guaranteedDurableReader{...}
    }
    // 否则读取所有数据（包括 memtable）
    return &reader{...}
}
```

**为什么需要这个语义**？
```
场景：检查 RaftAppliedIndex 是否持久化

T0: RaftAppliedIndex = 100 (在 memtable 中)
T1: Flush 完成（RaftAppliedIndex = 100 已持久化）
T2: durabilityAdvanced() 被调用
T3: 读取 RaftAppliedIndex

如果使用普通 Reader：
├─ 可能读到 memtable 中的新值（例如 105）
├─ 但 105 可能尚未持久化
└─ 截断到 105 是不安全的

使用 GuaranteedDurability Reader：
├─ 只读取 SST 中的值（100）
├─ 保证读到的值已持久化
└─ 截断到 100 是安全的
```

**共享 Reader 的优化**：
```go
reader := ... // 创建一次
for _, rangeID := range ranges {
    tryEnactTruncations(ctx, rangeID, reader)  // 复用
}
```
- ✅ **减少系统调用**：只打开一次文件
- ✅ **共享缓冲区**：减少内存分配
- ✅ **批量优化**：Pebble 内部可能有更好的缓存

#### Part 4: 优雅关闭检查

```go
shouldQuiesce := t.stopper.ShouldQuiesce()
quiesced := false
for _, rangeID := range ranges {
    t.tryEnactTruncations(ctx, rangeID, reader)

    select {
    case <-shouldQuiesce:
        quiesced = true
    default:
    }
    if quiesced {
        break
    }
}
```

**为什么需要检查**？
```
场景：系统正在关闭，但有 10000 个 Range 待截断

如果不检查：
├─ 遍历所有 10000 个 Range
├─ 每个 Range 截断耗时 1ms
└─ 总耗时 10 秒（延迟关闭）

检查后：
├─ 检测到 ShouldQuiesce()
├─ 立即 break
└─ 快速退出（< 1 秒）
```

**设计权衡**：
- **截断是非关键操作**：延迟执行不影响正确性
- **优雅关闭更重要**：快速响应关闭信号
- **下次启动会继续**：未完成的截断会在重启后重新尝试

---

## 总结（上篇）

在上篇中，我们完成了：

1. **职责边界与设计动机**：
   - 理解了传统截断方案的局限性
   - 揭示了松耦合截断的核心思想：**等待自然持久化，而非主动 fsync**

2. **控制流与组件协作**：
   - 梳理了截断提议的入队流程
   - 分析了持久化回调触发的批量截断流程

3. **关键函数与核心逻辑**：
   - 深入分析了 Store 初始化中的创建与注册
   - 剖析了 `durabilityAdvancedCallback()` 的状态机设计
   - 理解了 `durabilityAdvanced()` 的批量优化策略

**下篇预告**：

我们将继续深入分析：
- **tryEnactTruncations()** 的完整逻辑
- **pendingLogTruncations** 的队列管理
- **运行时行为与系统反馈**
- **设计模式分析**
- **具体运行示例**
- **设计取舍与替代方案**
- **总结与心智模型**

---

**文档版本**: v1.0（上篇）
**作者**: 基于 CockroachDB 源码分析
**最后更新**: 2026-02-04
