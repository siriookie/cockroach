# RangeControllerFactory 深度剖析——基于依赖注入与工厂模式的 Per-Store 级别复制流控管理器

## 一、职责边界与设计动机（Why）

### 1.1 系统性问题背景

在 CockroachDB 的 Raft 复制架构中，存在一个核心的资源管理难题：**如何在 leader replica 向 follower replicas 复制 Raft log entries 时，进行有效的准入控制（admission control）和流量控制（flow control）？**

如果没有这套机制，系统会面临以下具体困难：

#### 1.1.1 无节制的复制流量导致资源耗尽

在传统 Raft 实现（包括 CockroachDB 的 RACv1）中，leader 会无差别地向所有 followers 推送日志条目。这会导致：

- **接收端过载**：follower 的磁盘 I/O、CPU、内存都可能被打满
- **网络拥塞**：大量 MsgApp 同时发送，导致网络带宽饱和
- **级联失败**：某个 slow follower 不会减缓 leader 的发送速度，反而会导致该 follower 的队列堆积

#### 1.1.2 缺乏 per-range 的资源隔离

一个 store 上可能承载数百上千个 ranges。如果某些 "hot ranges" 消耗了所有的流控 token，其他 ranges 的复制会被完全阻塞，导致：

- **优先级反转**：低优先级的大批量写入阻塞了高优先级的小事务
- **tail latency 失控**：P99/P999 延迟无法控制

#### 1.1.3 工厂模式缺失导致的代码重复

每个 range 在成为 leader 时，都需要创建一个 `RangeController` 来管理该 range 的复制流控。如果没有工厂抽象：

- **依赖传递混乱**：每次创建 RangeController 都要传递 10+ 个参数
- **测试困难**：无法在单测中 mock 掉 RangeController
- **参数配置不一致**：不同 ranges 可能使用不同的配置

### 1.2 RangeControllerFactory 在系统中的位置

```
┌─────────────────────────────────────────────────────────────────┐
│                         KVServer / Store                        │
├─────────────────────────────────────────────────────────────────┤
│  Per-Store Shared Components (单例，全局共享):                 │
│  - Clock (HLC)                                                  │
│  - StreamTokenCounterProvider (管理所有 store-level tokens)    │
│  - Scheduler (raftScheduler，调度 Raft Ready 处理)             │
│  - SendTokenWatcher (监控 token 使用情况)                      │
│  - Metrics (evalWaitMetrics, rangeControllerMetrics)           │
│  - WaitForEvalConfig (配置项，控制 WaitForEval 行为)           │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │       RangeControllerFactoryImpl (本文主角)              │  │
│  │  职责：聚合 store 级依赖，为每个 range 创建独立的       │  │
│  │        RangeController 实例                               │  │
│  └─────────────────┬────────────────────────────────────────┘  │
│                    │ New(ctx, rangeControllerInitState)        │
│                    ↓                                            │
├─────────────────────────────────────────────────────────────────┤
│  Per-Range Components (每个 range 一个实例):                   │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Replica (Range R1) - processorImpl                      │  │
│  │    - leaderID, term, desc.replicas                       │  │
│  │    - raftInterface, msgAppSender                         │  │
│  │    - 当成为 leader 时调用 factory.New() 创建:            │  │
│  │                                                          │  │
│  │    ┌────────────────────────────────────────────────┐   │  │
│  │    │  RangeController (for R1)                      │   │  │
│  │    │  - 管理 R1 的所有 follower send streams        │   │  │
│  │    │  - 决定何时发送 MsgApp、如何分配 tokens        │   │  │
│  │    │  - WaitForEval: 评估请求的准入控制             │   │  │
│  │    └────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Replica (Range R2) - processorImpl                      │  │
│  │    ... (类似结构，独立的 RangeController)                 │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**上游**：
- `Store.Start()` 阶段创建 `RangeControllerFactoryImpl` 单例
- `Replica.processorImpl.createLeaderStateRaftMuLocked()` 调用 `factory.New()`

**下游**：
- 创建 `rac2.RangeController` 实例（实际类型是 `*rangeController`）
- `RangeController` 进一步创建 `replicaSendStream` 管理每个 follower 的复制流

### 1.3 核心抽象与生命周期

#### 核心对象

**1. RangeControllerFactoryImpl（本文主角）**

```go
// 位置：processor.go:1168-1179
type RangeControllerFactoryImpl struct {
	clock                      *hlc.Clock
	evalWaitMetrics            *rac2.EvalWaitMetrics
	rangeControllerMetrics     *rac2.RangeControllerMetrics
	streamTokenCounterProvider *rac2.StreamTokenCounterProvider
	closeTimerScheduler        rac2.ProbeToCloseTimerScheduler
	scheduler                  rac2.Scheduler
	sendTokenWatcher           *rac2.SendTokenWatcher
	waitForEvalConfig          *rac2.WaitForEvalConfig
	raftMaxInflightBytes       uint64
	knobs                      *kvflowcontrol.TestingKnobs
}
```

- **生命周期**：与 Store 相同，Store 启动时创建，Store 关闭时销毁
- **作用域**：per-store 单例
- **并发语义**：所有字段均为不可变（immutable），可被多个 goroutines 并发访问

**2. rangeControllerInitState（传递 range 特定信息的载体）**

```go
// 位置：processor.go:115-130
type rangeControllerInitState struct {
	term            uint64
	replicaSet      rac2.ReplicaSet
	leaseholder     roachpb.ReplicaID
	nextRaftIndex   uint64
	forceFlushIndex uint64
	// Range 特定的配置和接口
	rangeID        roachpb.RangeID
	tenantID       roachpb.TenantID
	localReplicaID roachpb.ReplicaID
	raftInterface  rac2.RaftInterface
	msgAppSender   rac2.MsgAppSender
	muAsserter     rac2.ReplicaMutexAsserter
}
```

- **生命周期**：瞬态，仅在 `factory.New()` 调用期间存在
- **作用域**：函数参数，用于传递 range 特定的初始化信息
- **并发语义**：只读，调用方在构造时填充

---

## 二、控制流与组件协作（How it flows）

### 2.1 主要执行路径

#### 路径 1：Store 启动时创建 Factory（正常路径）

```
时间线：Store 启动阶段

Step 1: Store.Start()
  ↓
Step 2: 初始化 store-level 共享组件
  ├─ 创建 hlc.Clock
  ├─ 创建 StreamTokenCounterProvider
  ├─ 创建 Scheduler (raftScheduler)
  ├─ 创建 SendTokenWatcher
  ├─ 创建 Metrics (evalWaitMetrics, rangeControllerMetrics)
  └─ 创建 WaitForEvalConfig
  ↓
Step 3: 调用 NewRangeControllerFactoryImpl
  输入：上述所有依赖
  输出：RangeControllerFactoryImpl 实例
  ↓
Step 4: 将 factory 传递给每个 Replica 的 ProcessorOptions
  ↓
Step 5: Replica 初始化时，将 factory 保存到 processorImpl.opts.RangeControllerFactory
```

**关键不变量**：
- Factory 在 Store 启动后立即创建，且只创建一次
- Factory 的所有字段在创建后不再改变（immutable）

#### 路径 2：Replica 成为 Leader 时创建 RangeController（关键路径）

```
时间线：Raft term 变更 + 该 Replica 成为新 leader

Step 1: HandleRaftReadyRaftMuLocked(state RaftNodeBasicState)
  state.IsLeader == true && (termChanged || !p.isLeader)
  触发条件：becameLeader = true
  ↓
Step 2: makeStateConsistentRaftMuLocked(ctx, state)
  检测到 becameLeader == true
  ↓
Step 3: createLeaderStateRaftMuLocked(ctx, term, nextUnstableIndex)
  位置：processor.go:722-753
  ├─ 构造 rangeControllerInitState (aggregating range-specific info)
  │  ├─ term: 当前 Raft term
  │  ├─ replicaSet: p.desc.replicas (所有副本)
  │  ├─ leaseholder: p.leaseholderID
  │  ├─ nextRaftIndex: state.NextUnstableIndex
  │  ├─ forceFlushIndex: p.forceFlushIndex
  │  ├─ rangeID, tenantID, localReplicaID: 从 processorImpl 获取
  │  ├─ raftInterface: p.raftInterface (用于发送 MsgApp)
  │  ├─ msgAppSender: p.opts.MsgAppSender
  │  └─ muAsserter: p.opts.ReplicaMutexAsserter
  │
  ├─ 调用 factory.New(ctx, rangeControllerInitState)
  │  ↓
  │  Step 3.1: RangeControllerFactoryImpl.New()
  │    位置：processor.go:1208-1239
  │    ├─ 构造 rac2.RangeControllerOptions (合并 store-level + range-level 依赖)
  │    └─ 调用 rac2.NewRangeController(ctx, options, initState)
  │       返回：*rangeController 实例
  │
  ├─ 初始化 pendingAdmittedMu.updates = map[...]
  ├─ 初始化 scratch = map[...]
  └─ 原子更新：p.leader.rc = rc (持有 rcReferenceUpdateMu)

状态变化：
  BEFORE: p.leader.rc == nil
  AFTER:  p.leader.rc != nil (指向新创建的 RangeController)
```

**并发控制要点**：
- `createLeaderStateRaftMuLocked` 持有 `Replica.raftMu`
- 更新 `p.leader.rc` 时额外持有 `rcReferenceUpdateMu` (写锁)
- 其他 goroutines 可以通过 `rcReferenceUpdateMu` (读锁) 安全访问 `p.leader.rc`

#### 路径 3：Replica 失去 Leadership 时销毁 RangeController（清理路径）

```
时间线：Raft term 变更 + 该 Replica 不再是 leader

Step 1: HandleRaftReadyRaftMuLocked(state RaftNodeBasicState)
  leftLeader = p.isLeader && (termChanged || !state.IsLeader)
  ↓
Step 2: closeLeaderStateRaftMuLocked(ctx)
  位置：processor.go:704-720
  ├─ 调用 p.leader.rc.CloseRaftMuLocked(ctx)
  │  ├─ 释放所有持有的 send tokens
  │  ├─ 关闭所有 replicaSendStreams
  │  └─ 清理内部状态
  │
  ├─ 清空 pendingAdmittedMu.updates = nil (持有 pendingAdmittedMu)
  ├─ 清空 scratch = nil
  └─ 原子更新：p.leader.rc = nil (持有 rcReferenceUpdateMu 写锁)

状态变化：
  BEFORE: p.leader.rc != nil
  AFTER:  p.leader.rc == nil
```

### 2.2 触发方式

**Factory 创建**：
- **触发时机**：Store 启动时，主动创建（eager initialization）
- **触发频率**：每个 Store 一次
- **调用栈深度**：`Store.Start() → NewRangeControllerFactoryImpl`

**RangeController 创建**：
- **触发时机**：Replica 接收 Raft Ready 事件，检测到 term 变化且 `becameLeader == true`
- **触发频率**：每次成为 leader 时触发（可能频繁，如 leader lease 切换）
- **调用栈深度**：
  ```
  Replica.handleRaftReadyRaftMuLocked
    → processorImpl.HandleRaftReadyRaftMuLocked
      → makeStateConsistentRaftMuLocked
        → createLeaderStateRaftMuLocked
          → factory.New()
  ```

### 2.3 与其他模块的交互

#### 交互方式 1：依赖聚合（Dependency Aggregation）

Factory 聚合了 10 个 store-level 依赖，避免了在每次创建 RangeController 时重复传递。这是典型的 **依赖注入（Dependency Injection）** 模式。

```go
// Factory 持有的依赖（不可变）
├─ clock: *hlc.Clock
│  作用：提供逻辑时钟，用于 token 过期、metrics 时间戳
│  共享级别：store-wide
│
├─ streamTokenCounterProvider: *rac2.StreamTokenCounterProvider
│  作用：管理所有 streams 的 token 分配和回收
│  共享级别：store-wide (跨所有 ranges)
│
├─ scheduler: rac2.Scheduler
│  作用：调度 RangeController 内部事件（如发送 MsgApp）
│  共享级别：store-wide (raftScheduler 单例)
│
├─ sendTokenWatcher: *rac2.SendTokenWatcher
│  作用：监控 token 使用情况，触发告警或限流
│  共享级别：store-wide
│
├─ closeTimerScheduler: rac2.ProbeToCloseTimerScheduler
│  作用：延迟关闭 replicaSendStream，等待 probe 消息
│  共享级别：store-wide
│
├─ evalWaitMetrics / rangeControllerMetrics: *rac2.Metrics
│  作用：记录 WaitForEval 延迟、token 使用等指标
│  共享级别：store-wide
│
├─ waitForEvalConfig: *rac2.WaitForEvalConfig
│  作用：配置 WaitForEval 的行为（如是否启用）
│  共享级别：store-wide
│
├─ raftMaxInflightBytes: uint64
│  作用：Raft 的 max_inflight_bytes 配置，限制单个 follower 的 inflight entries
│  共享级别：store-wide (来自 raft.Config)
│
└─ knobs: *kvflowcontrol.TestingKnobs
   作用：测试钩子，允许单测注入行为
   共享级别：store-wide
```

#### 交互方式 2：接口抽象（Interface Abstraction）

Factory 实现了 `RangeControllerFactory` 接口，使得测试代码可以 mock 掉整个 RangeController 的创建逻辑。

```go
// 位置：processor.go:132-136
type RangeControllerFactory interface {
	New(context.Context, rangeControllerInitState) rac2.RangeController
}
```

在单测中可以这样 mock：

```go
type fakeRangeControllerFactory struct {
	createCalls []rangeControllerInitState
}

func (f *fakeRangeControllerFactory) New(
	ctx context.Context, state rangeControllerInitState,
) rac2.RangeController {
	f.createCalls = append(f.createCalls, state)
	return &fakeRangeController{/* ... */}
}
```

#### 交互方式 3：状态隔离（State Isolation）

每个 RangeController 独立管理其 range 的流控状态，但共享 store-level 的 token pool（通过 `streamTokenCounterProvider`）。这实现了：

- **局部自治**：每个 range 独立决定何时发送 MsgApp
- **全局限流**：所有 ranges 共享同一个 token pool，避免 store-level 过载

```
┌───────────────────────────────────────────────────────┐
│      StreamTokenCounterProvider (store-wide)         │
│  Manages two token pools:                            │
│  - Regular priority tokens: 16MB per store           │
│  - Elastic priority tokens: 8MB per store            │
└──────────────┬────────────────────────────────────────┘
               │ Deduct / Return tokens
               ├──────────┬──────────┬──────────┐
               ↓          ↓          ↓          ↓
     ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
     │ RC (Range1) │ │ RC (Range2) │ │ RC (Range3) │
     │ - Stream A  │ │ - Stream B  │ │ - Stream C  │
     │ - Stream D  │ │ - Stream E  │ │ - Stream F  │
     └─────────────┘ └─────────────┘ └─────────────┘
```

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 NewRangeControllerFactoryImpl 函数解析

#### 函数签名与输入输出

```go
// 位置：processor.go:1181-1205
func NewRangeControllerFactoryImpl(
	clock *hlc.Clock,
	evalWaitMetrics *rac2.EvalWaitMetrics,
	rangeControllerMetrics *rac2.RangeControllerMetrics,
	streamTokenCounterProvider *rac2.StreamTokenCounterProvider,
	closeTimerScheduler rac2.ProbeToCloseTimerScheduler,
	scheduler rac2.Scheduler,
	sendTokenWatcher *rac2.SendTokenWatcher,
	waitForEvalConfig *rac2.WaitForEvalConfig,
	raftMaxInflightBytes uint64,
	knobs *kvflowcontrol.TestingKnobs,
) RangeControllerFactoryImpl {
	return RangeControllerFactoryImpl{
		clock:                      clock,
		evalWaitMetrics:            evalWaitMetrics,
		rangeControllerMetrics:     rangeControllerMetrics,
		streamTokenCounterProvider: streamTokenCounterProvider,
		closeTimerScheduler:        closeTimerScheduler,
		scheduler:                  scheduler,
		sendTokenWatcher:           sendTokenWatcher,
		waitForEvalConfig:          waitForEvalConfig,
		raftMaxInflightBytes:       raftMaxInflightBytes,
		knobs:                      knobs,
	}
}
```

**输入参数详解**：

| 参数名 | 类型 | 来源 | 职责 |
|-------|------|------|------|
| `clock` | `*hlc.Clock` | `Store.cfg.Clock` | 提供混合逻辑时钟（HLC），用于 token 过期、metrics 时间戳、send-queue 超时检测等 |
| `evalWaitMetrics` | `*rac2.EvalWaitMetrics` | Store 启动时创建 | 记录 `WaitForEval` 的等待时间、bypass 次数等指标 |
| `rangeControllerMetrics` | `*rac2.RangeControllerMetrics` | Store 启动时创建 | 记录 RangeController 的 send-queue 长度、token 使用等指标 |
| `streamTokenCounterProvider` | `*rac2.StreamTokenCounterProvider` | Store 启动时创建 | **核心依赖**：管理 store-level 的 token pool，决定每个 stream 能使用多少 tokens |
| `closeTimerScheduler` | `rac2.ProbeToCloseTimerScheduler` | Store 启动时创建 | 延迟关闭 `replicaSendStream`，避免频繁 probe 消息 |
| `scheduler` | `rac2.Scheduler` | `Store.raftScheduler` | 调度 RangeController 内部事件（如 `ScheduleControllerEvent`），触发 MsgApp 发送 |
| `sendTokenWatcher` | `*rac2.SendTokenWatcher` | Store 启动时创建 | 监控 token 使用情况，当某些 streams 长期无 token 时触发告警 |
| `waitForEvalConfig` | `*rac2.WaitForEvalConfig` | 基于 cluster settings 构造 | 控制 `WaitForEval` 的行为（如是否启用、超时时间等） |
| `raftMaxInflightBytes` | `uint64` | `raft.Config.MaxInflightBytes` | Raft 配置，限制单个 follower 的 inflight bytes |
| `knobs` | `*kvflowcontrol.TestingKnobs` | 测试代码注入 | 允许单测修改行为（如强制进入 pull mode） |

**输出**：
- `RangeControllerFactoryImpl` 结构体实例（值语义，非指针）
- 所有字段都是传入参数的副本（对于指针类型，复制的是指针本身，不是指向的对象）

#### 核心逻辑：纯函数式聚合

该函数的实现极其简洁，**只做一件事：将所有依赖聚合到一个结构体中**。这是典型的 **Builder Pattern 的简化版本**：

```go
return RangeControllerFactoryImpl{
	clock:                      clock,  // 直接赋值，无任何逻辑
	evalWaitMetrics:            evalWaitMetrics,
	rangeControllerMetrics:     rangeControllerMetrics,
	streamTokenCounterProvider: streamTokenCounterProvider,
	closeTimerScheduler:        closeTimerScheduler,
	scheduler:                  scheduler,
	sendTokenWatcher:           sendTokenWatcher,
	waitForEvalConfig:          waitForEvalConfig,
	raftMaxInflightBytes:       raftMaxInflightBytes,
	knobs:                      knobs,
}
```

**为什么只能这么做？**

1. **不可变性（Immutability）**：Factory 一旦创建，所有字段不再改变。这使得 Factory 可以被多个 goroutines 并发访问而无需加锁。

2. **零副作用（Zero Side Effects）**：函数不修改任何全局状态，不启动任何 goroutine，不分配任何动态资源。这使得函数可以在任何上下文中安全调用（包括单测）。

3. **依赖反转（Dependency Inversion）**：Factory 不依赖具体实现，只依赖接口（如 `Scheduler`、`ProbeToCloseTimerScheduler`）。这使得测试代码可以注入 mock 实现。

#### 并发语义与加锁策略

**无锁设计**：
- `NewRangeControllerFactoryImpl` 不涉及任何共享状态，因此不需要加锁。
- 返回的 `RangeControllerFactoryImpl` 结构体是值语义（非指针），调用方可以自由复制（虽然实际上总是通过指针传递）。

**并发安全性**：
- Factory 的所有字段都是指针或接口，指向的对象本身是并发安全的（如 `hlc.Clock` 内部有锁）。
- `raftMaxInflightBytes` 是原始类型 `uint64`，读取是原子的（在 64-bit 平台上）。

### 3.2 RangeControllerFactoryImpl.New 方法解析

#### 函数签名与核心流程

```go
// 位置：processor.go:1208-1239
func (f RangeControllerFactoryImpl) New(
	ctx context.Context, state rangeControllerInitState,
) rac2.RangeController {
	return rac2.NewRangeController(
		ctx,
		rac2.RangeControllerOptions{
			RangeID:                state.rangeID,
			TenantID:               state.tenantID,
			LocalReplicaID:         state.localReplicaID,
			SSTokenCounter:         f.streamTokenCounterProvider,
			RaftInterface:          state.raftInterface,
			MsgAppSender:           state.msgAppSender,
			Clock:                  f.clock,
			CloseTimerScheduler:    f.closeTimerScheduler,
			Scheduler:              f.scheduler,
			SendTokenWatcher:       f.sendTokenWatcher,
			EvalWaitMetrics:        f.evalWaitMetrics,
			RangeControllerMetrics: f.rangeControllerMetrics,
			WaitForEvalConfig:      f.waitForEvalConfig,
			RaftMaxInflightBytes:   f.raftMaxInflightBytes,
			ReplicaMutexAsserter:   state.muAsserter,
			Knobs:                  f.knobs,
		},
		rac2.RangeControllerInitState{
			Term:            state.term,
			ReplicaSet:      state.replicaSet,
			Leaseholder:     state.leaseholder,
			NextRaftIndex:   state.nextRaftIndex,
			ForceFlushIndex: state.forceFlushIndex,
		},
	)
}
```

#### 输入输出与不变量

**输入**：
- `ctx context.Context`：用于日志和 tracing，不涉及取消（RangeController 创建是同步且快速的）
- `state rangeControllerInitState`：包含 range 特定的初始化信息（见 2.1 节）

**输出**：
- `rac2.RangeController` 接口（实际返回 `*rangeController`，内部类型）

**不变量（Invariants）**：

1. **Term 单调性**：`state.term` 必须 ≥ 该 range 上一次创建 RangeController 时的 term（由调用方保证）

2. **ReplicaSet 完整性**：`state.replicaSet` 必须包含 `state.localReplicaID`（但代码对此场景有容错，见 processor.go:665-673）

3. **NextRaftIndex 连续性**：`state.nextRaftIndex` 是 RangeController 将要处理的第一个 entry 的索引，必须与 Raft log 一致

4. **依赖有效性**：`state.raftInterface` 和 `state.msgAppSender` 不能为 `nil`（否则 RangeController 无法发送 MsgApp）

#### 核心逻辑：依赖合并与转发

该方法的核心逻辑是 **将 factory-level 依赖和 range-level 依赖合并**，然后转发给 `rac2.NewRangeController` 构造函数。

**依赖来源分类**：

```go
// Store-level dependencies (来自 factory)
Clock:                  f.clock,
SSTokenCounter:         f.streamTokenCounterProvider,
CloseTimerScheduler:    f.closeTimerScheduler,
Scheduler:              f.scheduler,
SendTokenWatcher:       f.sendTokenWatcher,
EvalWaitMetrics:        f.evalWaitMetrics,
RangeControllerMetrics: f.rangeControllerMetrics,
WaitForEvalConfig:      f.waitForEvalConfig,
RaftMaxInflightBytes:   f.raftMaxInflightBytes,
Knobs:                  f.knobs,

// Range-level dependencies (来自 state 参数)
RangeID:        state.rangeID,
TenantID:       state.tenantID,
LocalReplicaID: state.localReplicaID,
RaftInterface:  state.raftInterface,
MsgAppSender:   state.msgAppSender,
ReplicaMutexAsserter: state.muAsserter,

// Initial state (传递给 RangeController 的初始状态)
Term:            state.term,
ReplicaSet:      state.replicaSet,
Leaseholder:     state.leaseholder,
NextRaftIndex:   state.nextRaftIndex,
ForceFlushIndex: state.forceFlushIndex,
```

**为什么要分成 Options 和 InitState 两个参数？**

- `RangeControllerOptions`：**配置和依赖**，这些在 RangeController 的整个生命周期内不会改变
- `RangeControllerInitState`：**初始状态**，这些可能在后续通过 `SetReplicasRaftMuLocked` 等方法更新

这是典型的 **配置与状态分离（Configuration-State Separation）** 模式。

#### 调用链深度与创建开销

```
factory.New()
  ↓
rac2.NewRangeController()
  ├─ 初始化 rangeController 结构体
  ├─ 创建 replicaSendStreams (一个 map[roachpb.ReplicaID]*replicaSendStream)
  │  对于 replicaSet 中的每个 replica：
  │    - 如果是 localReplicaID，跳过（自己不需要 stream）
  │    - 创建 replicaSendStream
  │      ├─ 从 streamTokenCounterProvider 获取 token counter
  │      ├─ 初始化 sendQueue (可能为空，如果 replica 在 StateProbe)
  │      └─ 注册到 closeTimerScheduler
  ├─ 初始化 evalWait (用于 WaitForEval)
  └─ 返回 *rangeController

总开销：
  - 内存分配：O(|replicaSet|)，每个 replica 一个 replicaSendStream
  - CPU 开销：O(|replicaSet|)，遍历一次 replicaSet
  - 无 I/O、无网络、无磁盘操作
  - 无阻塞操作（channel 创建是非阻塞的）
```

**关键优化点**：
- RangeController 创建是**同步且快速**的（通常 < 1ms），不会阻塞 Raft Ready 处理
- 所有 token 分配都是惰性的（不会在创建时预分配）

### 3.3 processorImpl.createLeaderStateRaftMuLocked 调用链解析

#### 函数实现详解

```go
// 位置：processor.go:722-753
func (p *processorImpl) createLeaderStateRaftMuLocked(
	ctx context.Context, term uint64, nextUnstableIndex uint64,
) {
	// ═════════════════════════════════════════════════════════
	// 阶段 1：前置检查
	// ═════════════════════════════════════════════════════════
	if p.leader.rc != nil {
		panic("RangeController already exists")
	}
	// 更新 term（虽然 makeStateConsistentRaftMuLocked 已经更新过，这里是保险）
	p.term = term

	// ═════════════════════════════════════════════════════════
	// 阶段 2：构造 rangeControllerInitState
	// ═════════════════════════════════════════════════════════
	rc := p.opts.RangeControllerFactory.New(ctx, rangeControllerInitState{
		// Raft 状态
		term:            term,
		replicaSet:      p.desc.replicas,
		leaseholder:     p.leaseholderID,
		nextRaftIndex:   nextUnstableIndex,
		forceFlushIndex: p.forceFlushIndex,

		// Range 元信息
		rangeID:        p.opts.RangeID,
		tenantID:       p.desc.tenantID,
		localReplicaID: p.opts.ReplicaID,

		// 接口依赖
		raftInterface: p.raftInterface,
		msgAppSender:  p.opts.MsgAppSender,
		muAsserter:    p.opts.ReplicaMutexAsserter,
	})

	// ═════════════════════════════════════════════════════════
	// 阶段 3：初始化 pendingAdmittedMu 相关状态
	// ═════════════════════════════════════════════════════════
	func() {
		p.leader.pendingAdmittedMu.Lock()
		defer p.leader.pendingAdmittedMu.Unlock()
		// 创建空 map，用于接收 follower 发来的 admitted vectors
		p.leader.pendingAdmittedMu.updates = map[roachpb.ReplicaID]rac2.AdmittedVector{}
	}()
	// 创建 scratch map，用于临时存储 updates（优化内存分配）
	p.leader.scratch = map[roachpb.ReplicaID]rac2.AdmittedVector{}

	// ═════════════════════════════════════════════════════════
	// 阶段 4：原子更新 p.leader.rc 引用
	// ═════════════════════════════════════════════════════════
	p.leader.rcReferenceUpdateMu.Lock()
	defer p.leader.rcReferenceUpdateMu.Unlock()
	p.leader.rc = rc
}
```

#### 关键设计决策解释

**Q1：为什么要在持有 raftMu 的情况下创建 RangeController？**

A：**原子性保证**。从检测到 `becameLeader` 到创建 RangeController，整个过程必须是原子的，不能被其他 Raft 事件打断。如果不持有 raftMu，可能出现：

```
Goroutine A: 检测到 becameLeader
Goroutine B: 处理新的 Raft Ready，发现 term 已变化，触发 leftLeader
Goroutine A: 创建 RangeController（但此时已经不是 leader 了！）
```

**Q2：为什么要用 rcReferenceUpdateMu 额外保护 p.leader.rc？**

A：**读写分离的并发控制**。

- **写操作**（创建/销毁 RangeController）：需要持有 `raftMu` + `rcReferenceUpdateMu` 写锁
- **读操作**（如 `AdmitForEval`）：只需持有 `rcReferenceUpdateMu` 读锁

这样可以让 `AdmitForEval` 在不持有 `raftMu` 的情况下安全读取 `p.leader.rc`，避免长时间持有 `raftMu` 导致 Raft Ready 处理被阻塞。

参考代码 processor.go:1098-1118：

```go
func (p *processorImpl) AdmitForEval(
	ctx context.Context, pri admissionpb.WorkPriority, ct time.Time,
) (admitted bool, err error) {
	var rc rac2.RangeController
	func() {
		p.leader.rcReferenceUpdateMu.RLock()  // 只持有读锁
		defer p.leader.rcReferenceUpdateMu.RUnlock()
		rc = p.leader.rc
	}()
	if rc == nil {
		// ... bypass logic
		return false, nil
	}
	return rc.WaitForEval(ctx, pri)  // 这里可能会阻塞很久
}
```

**Q3：为什么 pendingAdmittedMu.updates 初始化为空 map 而非 nil？**

A：**哨兵值（Sentinel Value）模式**。

- `updates == nil`：表示当前不是 leader（或正在 stepping down）
- `updates != nil`：表示当前是 leader，可以接收 admitted vectors

参考代码 processor.go:977-995：

```go
func (p *processorImpl) EnqueuePiggybackedAdmittedAtLeader(
	from roachpb.ReplicaID, state kvflowcontrolpb.AdmittedState,
) {
	// ...
	p.leader.pendingAdmittedMu.Lock()
	defer p.leader.pendingAdmittedMu.Unlock()
	// Invariant: if updates == nil, we are not the leader or already stepping down
	if p.leader.pendingAdmittedMu.updates == nil {
		return  // Silently drop the update
	}
	// Merge in the received admitted vector
	p.leader.pendingAdmittedMu.updates[from] =
		p.leader.pendingAdmittedMu.updates[from].Merge(
			rac2.AdmittedVector{Term: state.Term, Admitted: admitted})
}
```

这个设计避免了在 stepping down 过程中接收到的 admitted vectors 被错误地应用到新 leader 的 RangeController。

---

## 四、运行时行为与系统反馈（Runtime Behavior）

### 4.1 系统如何感知运行时信号

#### 信号 1：Token 耗尽（Backpressure）

**检测路径**：
```
replicaSendStream 尝试发送 entries
  ↓
tokenCounter.TryDeduct(tokens) 返回 false
  ↓
RangeController 将 entries 加入 send-queue
  ↓
metrics: rangeControllerMetrics.SendQueueBytes.Inc(...)
  ↓
sendTokenWatcher 定期检查（每 5s）
  ↓
发现某些 streams 长期无 token
  ↓
log.KvDistribution.Warningf("stream X has been blocked for Ys")
```

**影响决策**：
- **局部**：该 replicaSendStream 进入 "send-queue mode"，不再立即发送 MsgApp
- **全局**：如果所有 ranges 的 send-queues 都很长，说明 store-level token pool 不足，需要：
  - 触发 admission control 限制新写入
  - 触发 lease 转移（避免 hot leader）

#### 信号 2：Follower 落后（Admitted Vector 滞后）

**检测路径**：
```
Follower 发送 piggybacked admitted vector
  ↓
processorImpl.EnqueuePiggybackedAdmittedAtLeader(replicaID, state)
  ↓
RangeController.AdmitRaftMuLocked(replicaID, admittedVector)
  ↓
replicaSendStream 发现 admittedVector.Admitted[pri] << sentMark[pri]
  ↓
判断：该 follower 在某个优先级上有大量未 admitted entries
  ↓
RangeController.MaybeSendPingsRaftMuLocked()
  ↓
发送 MsgApp ping（空消息，仅触发 follower 回复 admitted vector）
```

**影响决策**：
- **即时**：如果 follower 的 admitted vector 长时间不更新，leader 不会释放 tokens
- **滞后**：只有当 follower 回复新的 admitted vector 后，tokens 才会被释放

**为什么采用这种策略**：
- **避免假释放**：如果 follower 实际上没有 admit entries，过早释放 tokens 会导致 leader 发送更多 entries，加剧 follower 的过载
- **容错性**：允许 follower 短暂落后（如 GC pause），但不会立即判定为慢节点

#### 信号 3：Raft StateProbe/StateSnapshot 转换

**检测路径**：
```
HandleRaftEventRaftMuLocked(ctx, state, event)
  ↓
event.ReplicasStateInfo[replicaID].State == tracker.StateProbe
  ↓
RangeController 检测到 replica 状态变化
  ↓
replicaSendStream.handleReadyState()
  ↓
关闭 send-queue（因为 StateProbe 时 Raft 会自己发送 probe MsgApp）
  ↓
释放该 stream 持有的所有 tokens
  ↓
metrics: rangeControllerMetrics.SendQueueBytes.Dec(...)
```

**影响决策**：
- **局部**：该 replicaSendStream 停止参与 token 竞争
- **全局**：释放的 tokens 可以被其他 ranges 使用

### 4.2 本地自治 vs 全局控制的权衡

#### 本地自治（Local Autonomy）

**RangeController 独立决策**：
- 何时从 send-queue 弹出 entries
- 何时发送 MsgApp ping
- 何时切换 push/pull mode

**优点**：
- **低延迟**：不需要与中心协调者通信
- **高吞吐**：每个 RangeController 可以并行工作
- **容错性**：某个 range 的决策错误不会影响其他 ranges

#### 全局控制（Global Control）

**streamTokenCounterProvider 全局限流**：
- 所有 ranges 共享同一个 token pool
- Token 分配遵循 FIFO 顺序（谁先请求谁先得到）

**优点**：
- **公平性**：避免某个 hot range 占用所有 tokens
- **防过载**：即使某个 range 的 send-queue 很长，也不会无限发送
- **资源隔离**：regular priority 和 elastic priority 有独立的 token pools

#### 混合策略

CockroachDB 采用了 **本地自治 + 全局限流** 的混合策略：

```
┌─────────────────────────────────────────────────────────────┐
│               Local Decision (per RangeController)          │
│  - 决定何时从 send-queue 弹出 entries                        │
│  - 决定每次发送多少 entries（受 raftMaxInflightBytes 限制） │
│  - 决定是否需要 force-flush                                  │
└───────────────────────┬─────────────────────────────────────┘
                        │ Request tokens
                        ↓
┌─────────────────────────────────────────────────────────────┐
│         Global Token Pool (streamTokenCounterProvider)      │
│  - 限制总 token 数量（16MB regular + 8MB elastic）          │
│  - 按 FIFO 顺序分配 tokens                                  │
│  - 跨所有 ranges 强制公平性                                  │
└─────────────────────────────────────────────────────────────┘
```

这种设计在 **吞吐量**（local autonomy）和 **公平性**（global control）之间取得了良好的平衡。

---

## 五、设计模式分析（重点）

### 5.1 Abstract Factory Pattern（抽象工厂模式）

#### 模式识别

**经典定义**：提供一个接口，用于创建一系列相关或相互依赖的对象，而无需指定它们的具体类。

**在本代码中的体现**：

```go
// 抽象工厂接口
type RangeControllerFactory interface {
	New(context.Context, rangeControllerInitState) rac2.RangeController
}

// 具体工厂实现
type RangeControllerFactoryImpl struct { /* ... */ }
func (f RangeControllerFactoryImpl) New(...) rac2.RangeController { /* ... */ }

// 产品接口
type RangeController interface {
	WaitForEval(...)
	HandleRaftEventRaftMuLocked(...)
	// ... 20+ methods
}

// 具体产品实现（内部类型，未导出）
type rangeController struct { /* ... */ }
```

**为什么选择这种模式**：

1. **测试性（Testability）**：单测可以注入 mock 实现：
   ```go
   type fakeFactory struct{}
   func (f fakeFactory) New(...) rac2.RangeController {
       return &fakeRangeController{}
   }

   processor := NewProcessor(ProcessorOptions{
       RangeControllerFactory: fakeFactory{},
   })
   ```

2. **依赖解耦（Decoupling）**：`processorImpl` 只依赖接口，不依赖具体实现。这使得 RangeController 的实现可以在不修改 `processorImpl` 的情况下演进。

3. **运行时多态（Runtime Polymorphism）**：虽然目前只有一个实现，但未来可能有多个（如 RACv3）。

#### 模式的工程化改造

**与经典模式的差异**：

| 经典 Abstract Factory | 本代码实现 |
|----------------------|-----------|
| 通常返回多个产品（如 `CreateButton()`, `CreateTextBox()`） | 只返回一个产品（`RangeController`） |
| Factory 通常是接口 | Factory 既有接口定义，也有值类型实现（非指针） |
| Factory 的创建通常需要参数 | Factory 创建时聚合所有依赖，后续创建产品时只需传递少量参数 |

**工程优化点**：

```go
// 传统方式：每次创建都要传递大量参数
rc := rac2.NewRangeController(
	ctx,
	clock,             // store-level
	metrics,           // store-level
	tokenProvider,     // store-level
	scheduler,         // store-level
	// ... 10+ more store-level dependencies
	rangeID,           // range-level
	tenantID,          // range-level
	// ...
)

// 工厂模式：依赖只传递一次
factory := NewRangeControllerFactoryImpl(
	clock, metrics, tokenProvider, scheduler, /* ... */
)
// 后续创建只需传递 range-level 参数
rc := factory.New(ctx, rangeControllerInitState{
	rangeID: ...,
	tenantID: ...,
	// ...
})
```

### 5.2 Dependency Injection Pattern（依赖注入模式）

#### 模式识别

**经典定义**：通过外部注入依赖，而非在类内部创建依赖。

**在本代码中的体现**：

```go
// Bad: 内部创建依赖（硬编码）
func NewBadFactory() RangeControllerFactory {
	return RangeControllerFactoryImpl{
		clock: hlc.NewClock(...),  // 硬编码创建
		scheduler: newRaftScheduler(...),  // 硬编码创建
		// 测试时无法替换
	}
}

// Good: 依赖注入
func NewRangeControllerFactoryImpl(
	clock *hlc.Clock,              // 外部注入
	scheduler rac2.Scheduler,      // 外部注入（接口）
	// ...
) RangeControllerFactoryImpl {
	return RangeControllerFactoryImpl{
		clock:     clock,
		scheduler: scheduler,
		// ...
	}
}
```

**为什么这里选择构造函数注入（Constructor Injection）而非 Setter 注入？**

1. **不可变性（Immutability）**：Factory 一旦创建，依赖不可更改，避免运行时状态变化导致的 bug。

2. **完整性保证（Completeness Guarantee）**：如果缺少必需的依赖，编译器会报错（缺少参数），而不是运行时 panic。

3. **线程安全（Thread Safety）**：没有 setter 方法，就不存在并发修改的风险。

#### 依赖生命周期管理

```go
// 依赖的生命周期层次
Store.Start()
  ├─ 创建 store-level 依赖（生命周期：与 Store 相同）
  │  ├─ hlc.Clock
  │  ├─ streamTokenCounterProvider
  │  └─ raftScheduler
  │
  ├─ 创建 Factory（生命周期：与 Store 相同）
  │  └─ 持有对 store-level 依赖的引用（非所有权）
  │
  └─ 传递 Factory 给每个 Replica（生命周期：与 Replica 相同）
      └─ 创建 RangeController（生命周期：与 leader term 相同）
          └─ 持有对 Factory 依赖的引用（非所有权）

生命周期关系：
  Store >= Factory >= Replica >= RangeController
  Store >= store-level dependencies
```

**关键设计原则**：
- **所有权清晰（Clear Ownership）**：Store 拥有 store-level 依赖，Factory 只是持有引用
- **生命周期兼容（Lifetime Compatibility）**：短生命周期对象（RangeController）可以安全引用长生命周期对象（store-level 依赖）

### 5.3 Builder Pattern 的简化版本

#### 模式识别

传统 Builder Pattern 通常有链式调用：

```go
// 传统 Builder
builder := NewRangeControllerBuilder()
rc := builder.
	WithClock(clock).
	WithScheduler(scheduler).
	WithTokenProvider(tokenProvider).
	// ... 10+ methods
	Build()
```

**本代码的简化**：

```go
// 简化版 Builder：所有参数一次性传递
factory := NewRangeControllerFactoryImpl(
	clock,
	evalWaitMetrics,
	rangeControllerMetrics,
	// ... all dependencies
)
```

**为什么简化**：

1. **参数数量固定**：不需要可选参数（所有参数都是必需的）
2. **避免部分构造**：传统 Builder 允许部分设置参数然后 `Build()`，可能导致缺少必需参数的运行时错误
3. **编译时检查**：函数签名强制要求所有参数，编译器会检查

#### 与 Functional Options Pattern 的对比

在 Go 中，另一种常见的模式是 Functional Options：

```go
// Functional Options Pattern
type Option func(*RangeControllerFactoryImpl)

func WithClock(c *hlc.Clock) Option {
	return func(f *RangeControllerFactoryImpl) {
		f.clock = c
	}
}

factory := NewRangeControllerFactoryImpl(
	WithClock(clock),
	WithScheduler(scheduler),
	// ...
)
```

**为什么本代码没有采用 Functional Options**：

1. **所有参数都是必需的**：Functional Options 适合有大量可选参数的场景
2. **类型安全性**：直接传参数可以让编译器检查类型，而 `Option` 模式会推迟到运行时
3. **性能考虑**：Functional Options 会创建大量闭包，增加 GC 压力

### 5.4 Immutable Object Pattern（不可变对象模式）

#### 模式识别

`RangeControllerFactoryImpl` 的所有字段都是只读的：

```go
type RangeControllerFactoryImpl struct {
	clock                      *hlc.Clock             // immutable reference
	evalWaitMetrics            *rac2.EvalWaitMetrics  // immutable reference
	// ... all fields are set once and never modified
}

// NO setter methods
// NO exported fields
// NO mutex (because no shared mutable state)
```

**为什么选择不可变性**：

1. **线程安全（Thread Safety）**：不可变对象天然线程安全，无需加锁
2. **推理简单（Ease of Reasoning）**：对象状态不会变化，减少心智负担
3. **缓存友好（Cache-Friendly）**：CPU 可以安全地缓存对象字段，无需担心缓存失效

#### 部分可变性的权衡

虽然 Factory 本身是不可变的，但它引用的对象（如 `streamTokenCounterProvider`）是可变的。这是**浅不可变性（Shallow Immutability）**：

```go
factory.clock                // immutable reference
factory.clock.Now()          // clock 内部状态会变化（当前时间）

factory.streamTokenCounterProvider  // immutable reference
factory.streamTokenCounterProvider.Deduct(...)  // 会修改 token pool 状态
```

**为什么允许这种部分可变性**：

1. **性能考虑**：如果 token pool 也是不可变的，每次 deduct 都要复制整个对象，开销巨大
2. **语义正确性**：token pool 本质上是共享可变状态，强制不可变会导致代码复杂化
3. **并发安全性**：这些可变对象内部有自己的并发控制（如 `syncutil.Mutex`）

### 5.5 Interface Segregation Principle（接口隔离原则）

#### 模式识别

RangeController 接口有 20+ 方法，但 Factory 只需要一个 `New` 方法：

```go
// processorImpl 只关心创建能力
type RangeControllerFactory interface {
	New(context.Context, rangeControllerInitState) rac2.RangeController
}

// 而不是暴露整个 RangeControllerFactoryImpl
```

**为什么这样设计**：

1. **最小权限原则（Principle of Least Privilege）**：`processorImpl` 只需要创建 RangeController 的能力，不需要访问 Factory 的内部字段
2. **依赖倒置（Dependency Inversion）**：`processorImpl` 依赖接口，而非具体实现
3. **测试友好性**：测试时可以提供最小化的 mock 实现

#### ISP 在整个 RACv2 架构中的应用

```go
// 调度能力：只需要一个方法
type Scheduler interface {
	ScheduleControllerEvent(rangeID roachpb.RangeID)
}

// MsgApp 发送能力：只需要一个方法
type MsgAppSender interface {
	SendMsgApp(ctx context.Context, msg raftpb.Message, lowPriorityOverride bool)
}

// Token 管理能力：只暴露必要的方法
type StreamTokenCounterProvider interface {
	// 不暴露内部的 token pool 实现细节
}
```

这种设计使得每个组件只依赖它真正需要的接口，降低了耦合度。

---

## 六、具体运行示例

### 6.1 正常场景：Replica 成为 Leader 并创建 RangeController

#### 初始状态

```
时间: T0
Range: R123
Replicas: {1: n1s1, 2: n2s2, 3: n3s3}
本地 Replica: 2 (在 n2s2 上)
Raft State:
  - Term: 5
  - Leader: unknown
  - IsLeader: false
processorImpl State:
  - p.term: 5
  - p.isLeader: false
  - p.leader.rc: nil
```

#### 时间线

**T1: 收到 Raft Ready，检测到成为 Leader**

```go
// Raft 层通知
state := RaftNodeBasicState{
	Term:              6,  // term 增加
	IsLeader:          true,  // 成为 leader
	Leader:            2,  // 自己
	NextUnstableIndex: 100,  // 下一个要写入的 entry index
	Leaseholder:       2,
}

// 调用链
HandleRaftReadyRaftMuLocked(ctx, state, event)
  ↓
makeStateConsistentRaftMuLocked(ctx, state)
  // 检测到 becameLeader = true
  ↓
createLeaderStateRaftMuLocked(ctx, 6, 100)
```

**T2: 构造 rangeControllerInitState**

```go
initState := rangeControllerInitState{
	term:            6,
	replicaSet:      map[1: {NodeID:1, StoreID:1},
	                     2: {NodeID:2, StoreID:2},
	                     3: {NodeID:3, StoreID:3}},
	leaseholder:     2,
	nextRaftIndex:   100,
	forceFlushIndex: 0,  // 假设没有 force flush
	rangeID:         123,
	tenantID:        roachpb.SystemTenantID,
	localReplicaID:  2,
	raftInterface:   p.raftInterface,
	msgAppSender:    p.opts.MsgAppSender,
	muAsserter:      p.opts.ReplicaMutexAsserter,
}
```

**T3: 调用 Factory.New**

```go
factory.New(ctx, initState)
  ↓
rac2.NewRangeController(ctx, options, initState)
  ├─ 创建 rangeController 实例
  ├─ 为 replica 1 创建 replicaSendStream
  │  └─ 从 streamTokenCounterProvider 获取 token counter
  ├─ 为 replica 3 创建 replicaSendStream
  │  └─ 从 streamTokenCounterProvider 获取 token counter
  └─ 初始化 evalWait
```

**T4: 更新 processorImpl 状态**

```go
p.leader.pendingAdmittedMu.updates = map[]  // 空 map
p.leader.scratch = map[]  // 空 map
p.leader.rc = rc  // 指向新创建的 RangeController

状态变化：
  p.term: 5 → 6
  p.isLeader: false → true
  p.leaderID: 0 → 2
  p.leader.rc: nil → *rangeController
```

**T5: 开始处理写请求**

```go
// 客户端发起写请求
客户端 → leaseholder (replica 2) → AdmitForEval(ctx, NormalPri)
  ↓
processorImpl.AdmitForEval()
  ├─ 读取 p.leader.rc (持有 rcReferenceUpdateMu.RLock)
  ├─ rc != nil
  └─ 调用 rc.WaitForEval(ctx, NormalPri)
      ├─ 检查当前 tokens > 0？
      ├─ 如果是，立即返回 (true, nil)
      └─ 否则，阻塞等待 tokens 可用

// 假设 tokens 充足，立即返回
returned: (waited=true, err=nil)
  ↓
请求被批准，进入 evaluation 阶段
```

**T6: 处理 Raft entries 并发送 MsgApp**

```go
// Raft Ready 包含新 entries
event := RaftEvent{
	Term:    6,
	Entries: [{Index: 100, Term: 6, Data: ...}],
	ReplicasStateInfo: map[
		1: {State: StateReplicate, Match: 99, Next: 100},
		3: {State: StateReplicate, Match: 99, Next: 100},
	],
}

HandleRaftEventRaftMuLocked(ctx, state, event)
  ↓
rc.HandleRaftEventRaftMuLocked(ctx, event)
  ├─ 检测到 entry 100 需要复制
  ├─ 为 replica 1 的 replicaSendStream 分配 tokens
  │  └─ streamTokenCounterProvider.Deduct(stream1, 1024 bytes)
  ├─ 为 replica 3 的 replicaSendStream 分配 tokens
  │  └─ streamTokenCounterProvider.Deduct(stream3, 1024 bytes)
  └─ 发送 MsgApp 到 replica 1 和 3

状态变化：
  stream1.sentMark: {Index: 99} → {Index: 100}
  stream3.sentMark: {Index: 99} → {Index: 100}
  store token pool: 16MB → 16MB - 2KB
```

### 6.2 边界场景：Token 耗尽时的 Send-Queue 行为

#### 初始状态

```
时间: T10
Range: R123 (同上)
RangeController State:
  - replica 1: send-queue 为空，tokens 充足
  - replica 3: send-queue 有 100 个 entries，等待 tokens
Store Token Pool:
  - Regular priority: 0 tokens (耗尽！)
  - Elastic priority: 8MB tokens (充足)
```

#### 时间线

**T10: 新的 Regular Priority Entry 到达**

```go
event := RaftEvent{
	Entries: [{Index: 200, Term: 6, Pri: RegularPri, Data: [10KB]}],
	ReplicasStateInfo: map[
		1: {State: StateReplicate, Next: 200},
		3: {State: StateReplicate, Next: 200},
	],
}

rc.HandleRaftEventRaftMuLocked(ctx, event)
  ↓
尝试为 replica 1 分配 tokens
  streamTokenCounterProvider.TryDeduct(stream1, 10KB, RegularPri)
    ↓
    tokenCounter.tokens[RegularPri] = 0  // 耗尽
    返回: (false, 0)
  ↓
  无法立即发送，加入 send-queue
    stream1.sendQueue.Push(entry 200)
    metrics.SendQueueBytes.Inc(10KB)

同样的逻辑应用到 replica 3
```

**状态变化**：
```
stream1.sendQueue: [] → [entry 200]
stream3.sendQueue: [entries 101-200] → [entries 101-201]
metrics.SendQueueBytes: 1MB → 1MB + 20KB
```

**T11: Follower Admits Entries，释放 Tokens**

```go
// replica 1 回复 admitted vector
EnqueuePiggybackedAdmittedAtLeader(1, AdmittedState{
	Term: 6,
	Admitted: [150, 100],  // [RegularPri: 150, ElasticPri: 100]
})
  ↓
ProcessPiggybackedAdmittedAtLeaderRaftMuLocked(ctx)
  ↓
rc.AdmitRaftMuLocked(ctx, 1, {Term: 6, Admitted: [150, 100]})
  ↓
stream1.replicaSendStream.admit({Term: 6, Admitted: [150, 100]})
  ├─ 计算释放的 tokens: entries [100-150] ≈ 50KB
  └─ streamTokenCounterProvider.Return(stream1, 50KB, RegularPri)
      ↓
      tokenCounter.tokens[RegularPri] = 0 + 50KB = 50KB
      tokenCounter.signal()  // 唤醒等待的 send-queues
```

**T12: RangeController 调度事件，发送 Send-Queue 中的 Entries**

```go
// 由 Scheduler 触发（通过 ScheduleControllerEvent）
ProcessSchedulerEventRaftMuLocked(ctx, MsgAppPull, logSnapshot)
  ↓
rc.HandleSchedulerEventRaftMuLocked(ctx, MsgAppPull, logSnapshot)
  ├─ 遍历所有 replicaSendStreams
  ├─ 检查 stream1.sendQueue 不为空
  ├─ 尝试分配 tokens: TryDeduct(stream1, 10KB, RegularPri)
  │    ↓
  │    tokenCounter.tokens[RegularPri] = 50KB - 10KB = 40KB
  │    返回: (true, 10KB)
  ├─ 从 sendQueue 弹出 entry 200
  ├─ 调用 raftInterface.SendMsgAppRaftMuLocked(1, [entry 200])
  └─ 更新 stream1.sentMark: 199 → 200

状态变化：
  stream1.sendQueue: [entry 200] → []
  store token pool[RegularPri]: 50KB → 40KB
  metrics.SendQueueBytes: 1MB + 20KB → 1MB + 10KB
```

**关键观察**：
1. **Token 耗尽不会阻塞 Raft log 写入**：entries 仍然被写入本地 log，只是不立即发送
2. **Send-queue 是 FIFO 的**：先入队的 entries 先被发送
3. **Token 释放是异步的**：依赖 follower 回复 admitted vector

---

## 七、设计取舍与替代方案（Trade-offs）

### 7.1 当前方案：Per-Store Factory + Per-Range Controller

#### 优势

**1. 资源隔离与共享的平衡**

- ✅ **Store-level 依赖共享**：避免重复创建（如 hlc.Clock 在整个 store 只有一个实例）
- ✅ **Range-level 状态隔离**：每个 range 的流控状态独立，避免互相干扰
- ✅ **Token Pool 全局限流**：所有 ranges 共享同一个 token pool，防止 store-level 过载

**2. 测试性**

- ✅ **易于 Mock**：接口抽象使得单测可以注入 fake 实现
- ✅ **依赖解耦**：可以独立测试 RangeController 而不启动整个 Store

**3. 并发性能**

- ✅ **无锁 Factory**：Factory 是不可变的，多个 goroutines 可以并发调用 `New`
- ✅ **细粒度锁**：每个 RangeController 有独立的锁，不会全局阻塞

**4. 内存开销**

- ✅ **低内存占用**：Factory 只持有引用，不复制大对象
- ✅ **按需创建**：RangeController 只在成为 leader 时创建

#### 劣势

**1. 参数传递复杂度**

- ❌ **Factory 创建需要 10 个参数**：虽然比每次创建 RangeController 时传递要好，但仍然繁琐
- ❌ **参数顺序依赖**：必须严格按照函数签名顺序传递参数

**2. 间接性（Indirection）**

- ❌ **多层抽象**：processorImpl → Factory → RangeController → replicaSendStream
- ❌ **性能开销**：虽然微小，但每次调用 `New` 都要经过一层函数调用

**3. 生命周期管理**

- ❌ **依赖生命周期需要手动保证**：如果 Store 提前销毁了某个依赖（如 streamTokenCounterProvider），但 RangeController 仍在使用，会导致 panic

### 7.2 替代方案 1：全局单例 RangeControllerManager

#### 设计思路

```go
// 全局单例，管理所有 ranges 的 flow control
type RangeControllerManager struct {
	mu       syncutil.RWMutex
	controllers map[roachpb.RangeID]*rangeController
	// ... store-level dependencies
}

func (m *RangeControllerManager) GetOrCreate(
	rangeID roachpb.RangeID, initState rangeControllerInitState,
) *rangeController {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rc, ok := m.controllers[rangeID]; ok {
		return rc
	}
	rc := m.createLocked(rangeID, initState)
	m.controllers[rangeID] = rc
	return rc
}
```

#### 对比分析

| 维度 | 当前方案（Factory） | 替代方案（Manager） |
|------|-------------------|-------------------|
| **并发性能** | ✅ Factory 无锁，可并发创建 | ❌ 全局锁，创建时阻塞所有 ranges |
| **内存开销** | ✅ RangeController 在失去 leadership 时立即销毁 | ❌ Manager 需要显式删除，容易泄漏 |
| **测试性** | ✅ 每个 test case 可以有独立的 Factory | ❌ 全局单例难以在单测间隔离 |
| **代码复杂度** | ✅ 简单（无需管理 map） | ❌ 需要处理并发访问、生命周期管理 |

**结论**：Manager 方案在并发性能和测试性上不如当前方案。

### 7.3 替代方案 2：在 Replica 内部直接创建 RangeController

#### 设计思路

```go
func (p *processorImpl) createLeaderStateRaftMuLocked(...) {
	// 不使用 Factory，直接调用构造函数
	rc := rac2.NewRangeController(
		ctx,
		rac2.RangeControllerOptions{
			RangeID:   p.opts.RangeID,
			Clock:     p.opts.Clock,  // 需要在 ProcessorOptions 中传递
			Scheduler: p.opts.Scheduler,
			// ... 20+ fields
		},
		// ...
	)
	p.leader.rc = rc
}
```

#### 对比分析

| 维度 | 当前方案（Factory） | 替代方案（无 Factory） |
|------|-------------------|---------------------|
| **参数传递** | ✅ Factory 创建时传递一次，后续只传递 range-specific 参数 | ❌ 每次创建都要传递 20+ 个参数 |
| **测试性** | ✅ 可以 mock Factory | ❌ 难以 mock（需要 mock 所有依赖） |
| **依赖管理** | ✅ Factory 聚合依赖，职责清晰 | ❌ processorImpl 需要持有所有 store-level 依赖 |
| **代码重复** | ✅ 创建逻辑集中在 Factory.New | ❌ 如果有多个地方创建 RangeController，代码重复 |

**结论**：无 Factory 方案会导致代码重复和测试困难。

### 7.4 替代方案 3：使用 Context 传递依赖

#### 设计思路

```go
type storeContextKey struct{}

// 在 Store 启动时将依赖放入 context
ctx := context.WithValue(ctx, storeContextKey{}, storeDependencies{
	clock:         clock,
	tokenProvider: tokenProvider,
	// ...
})

// 创建 RangeController 时从 context 获取
func (p *processorImpl) createLeaderStateRaftMuLocked(ctx context.Context, ...) {
	deps := ctx.Value(storeContextKey{}).(storeDependencies)
	rc := rac2.NewRangeController(ctx, deps, ...)
}
```

#### 对比分析

| 维度 | 当前方案（Factory） | 替代方案（Context） |
|------|-------------------|-------------------|
| **类型安全** | ✅ 编译时检查 | ❌ 运行时类型断言，容易 panic |
| **显式依赖** | ✅ 依赖关系清晰 | ❌ 依赖隐藏在 context 中，难以追踪 |
| **性能** | ✅ 直接字段访问 | ❌ context.Value 需要遍历 context 链 |
| **测试性** | ✅ 可以注入 mock Factory | ❌ 需要构造完整的 context |

**结论**：Context 方案在类型安全和显式性上不如当前方案。Go 社区普遍不推荐用 `context.Value` 传递依赖。

### 7.5 适用场景分析

**当前方案适用于**：

1. **中等规模的依赖数量**（10-20 个）：太少不需要 Factory，太多应该考虑分层
2. **依赖生命周期稳定**：store-level 依赖在 Store 启动后不会改变
3. **需要高并发性能**：Factory 无锁设计适合高频创建场景
4. **测试友好性重要**：需要频繁编写单测

**当前方案不适用于**：

1. **依赖关系极其复杂**：如果有 50+ 个依赖，应该考虑更高层次的抽象（如 Dependency Graph）
2. **依赖需要运行时切换**：如果需要在运行时替换某个依赖（如 A/B testing），当前的不可变设计不适用
3. **极致的性能优化**：如果 Factory 的一层间接调用都不能接受，应该考虑更激进的优化（如代码生成）

---

## 八、总结与关键洞察

### 8.1 核心价值

`RangeControllerFactoryImpl` 不仅仅是一个简单的工厂类，它在 CockroachDB 的 RACv2 架构中扮演了**依赖聚合中心（Dependency Aggregation Hub）**的角色：

1. **解耦**：隔离 store-level 和 range-level 关注点
2. **复用**：避免每次创建 RangeController 时重复传递依赖
3. **测试**：提供接口抽象，便于单测
4. **性能**：无锁设计，支持高并发创建

### 8.2 设计模式总结

| 模式 | 应用 | 目的 |
|------|-----|------|
| Abstract Factory | `RangeControllerFactory` 接口 | 解耦具体实现，便于测试 |
| Dependency Injection | 构造函数注入所有依赖 | 外部控制依赖，提高可测试性 |
| Immutable Object | Factory 所有字段只读 | 线程安全，无需加锁 |
| Interface Segregation | 只暴露 `New` 方法 | 最小权限原则 |
| Builder (简化版) | 一次性传递所有参数 | 编译时检查参数完整性 |

### 8.3 工程权衡

在 **简洁性**（Simple Constructor）和 **扩展性**（Complex Builder）之间，CockroachDB 选择了**中间路线**：

- 使用 Factory 避免参数重复传递（比简单构造函数好）
- 不引入复杂的 Builder 或 Options 模式（比过度设计好）
- 通过接口抽象保留未来演进空间（如支持 RACv3）

这种权衡体现了 **YAGNI 原则（You Aren't Gonna Need It）** 和 **KISS 原则（Keep It Simple, Stupid）** 的平衡。

### 8.4 可复用的理解模型

理解 `RangeControllerFactoryImpl` 的关键在于理解三个层次的职责分离：

```
┌─────────────────────────────────────────────────────────┐
│  Layer 1: Store-level Singletons                       │
│  职责：管理全局共享资源（token pool, metrics, clock）   │
└────────────────────┬────────────────────────────────────┘
                     │ Aggregated by
┌─────────────────────────────────────────────────────────┐
│  Layer 2: RangeControllerFactory                        │
│  职责：聚合 store-level 依赖，提供创建能力              │
└────────────────────┬────────────────────────────────────┘
                     │ Creates
┌─────────────────────────────────────────────────────────┐
│  Layer 3: RangeController                               │
│  职责：管理单个 range 的复制流控逻辑                     │
└─────────────────────────────────────────────────────────┘
```

这种分层在分布式系统中极为常见：
- **Kubernetes**：Cluster-level (kube-apiserver) → Factory (ControllerManager) → Per-Resource Controllers
- **Envoy**：Server-level (ThreadLocalStore) → Factory (ClusterManager) → Per-Cluster HTTP Conn Pools

掌握这种模式，可以帮助你设计任何需要在**全局单例**和**多实例对象**之间传递依赖的系统。

---

## 九、参考文献与扩展阅读

1. **CockroachDB RACv2 Design Doc**（内部文档，需要权限）
2. **Go Concurrency Patterns**：https://go.dev/blog/pipelines
3. **Dependency Injection in Go**：https://blog.drewolson.org/dependency-injection-in-go
4. **Immutable Object Pattern**：https://en.wikipedia.org/wiki/Immutable_object
5. **Raft 论文**：*In Search of an Understandable Consensus Algorithm* (Diego Ongaro, 2014)

---

**文档版本**：v1.0
**最后更新**：2026-02-09
**作者**：Claude Code (基于 CockroachDB processor.go 源码分析)
