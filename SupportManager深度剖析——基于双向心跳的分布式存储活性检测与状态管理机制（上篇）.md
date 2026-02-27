# SupportManager 深度剖析——基于双向心跳的分布式存储活性检测与状态管理机制（上篇）

## 一、职责边界与设计动机（Why）

### 1.1 背景：为什么需要 Store Liveness？

在 CockroachDB 的分布式架构中，每个节点（Node）可以包含一个或多个存储实例（Store）。传统的节点级别活性检测（Node Liveness）只能判断整个节点是否存活，但在实际生产环境中，我们会遇到更细粒度的故障场景：

**问题场景：**
1. **部分磁盘故障**：一个节点有 3 块磁盘，其中 1 块出现 IO stall，导致该 Store 无法正常服务，但节点本身和其他 Store 仍然健康
2. **资源隔离失效**：某个 Store 因为特定工作负载耗尽 IO 资源，进入过载状态，但不应影响同节点其他 Store 的可用性判断
3. **Raft 租约管理精度**：Raft 层需要知道某个特定 Store 上的副本（Replica）是否真正可用，而不是简单地依赖节点级别的活性

**如果没有 Store Liveness，系统会遇到：**
- **误判健康状态**：Node Liveness 说节点活着，但实际上某些 Store 已经因磁盘故障无法服务
- **租约安全性降低**：Raft 无法精确判断某个副本是否真正可达，可能导致租约转移延迟或错误
- **资源利用率下降**：健康的 Store 可能因为同节点有故障 Store 而被整体标记为不可用

### 1.2 Store Liveness 的核心价值

Store Liveness 提供了**细粒度的、Store 级别的活性保证**，它通过**双向心跳机制**（bidirectional heartbeat）实现：

```
Store A ──────────────> Store B
         MsgHeartbeat
         (我需要你的支持)

Store A <────────────── Store B
         MsgHeartbeatResp
         (我支持你到时间 T)
```

**核心语义：**
- **Support From**：A 向 B 请求支持，并维护 B 对 A 的支持状态
- **Support For**：B 接收 A 的心跳，并承诺在一定时间内支持 A

这种设计使得：
1. **精确性**：每个 Store 独立维护与其他 Store 的活性关系
2. **局部性**：磁盘故障只影响特定 Store，不会波及整个节点
3. **可追溯性**：支持状态有明确的过期时间（expiration），便于调试和分析

### 1.3 在系统中的位置

SupportManager 位于 CockroachDB 架构的 **KV 层存储子系统** 中：

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
│  ├── SupportManager ← 我们在这里            │
│  ├── StorePool (负载与容量)                 │
│  └── Queues (后台维护)                      │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│    Storage Engine (Pebble/RocksDB)         │
└─────────────────────────────────────────────┘
```

**上游依赖：**
- **Raft 层**：查询某个副本所在 Store 是否存活（`SupportFrom`）
- **Store Liveness Fabric**：Raft 和其他组件通过 `Fabric` 接口访问 SupportManager
- **Storage Engine**：需要持久化心跳状态和元数据

**下游依赖：**
- **Transport**：用于发送和接收 Store Liveness 消息
- **Clock (HLC)**：提供混合逻辑时钟，用于时间戳管理
- **Stopper**：控制异步任务的生命周期

### 1.4 核心抽象与对象

SupportManager 的设计围绕以下核心抽象：

#### 1.4.1 长期存在的核心对象

```go
type SupportManager struct {
    storeID               slpb.StoreIdent          // 本地 Store 身份
    engine                storage.Engine           // 持久化引擎
    clock                 *hlc.Clock              // 混合逻辑时钟
    sender                MessageSender           // 消息发送器

    // === 双向状态 ===
    requesterStateHandler *requesterStateHandler  // "我请求别人支持我"
    supporterStateHandler *supporterStateHandler  // "我支持别人"

    // === 异步通信 ===
    receiveQueue          receiveQueue            // 接收消息队列
    storesToAdd           storesToAdd             // 待添加的 Store

    // === 生命周期控制 ===
    heartbeatTicker       *timeutil.BroadcastTicker // 心跳定时器
    stopper               *stop.Stopper            // 停止控制器

    // === 回调与配置 ===
    withdrawalCallback    func(map[roachpb.StoreID]struct{}) // 撤回支持时的回调
    options               Options                   // 配置选项
    metrics               *SupportManagerMetrics   // 监控指标
}
```

#### 1.4.2 核心状态与生命周期

SupportManager 维护两组独立的状态机：

**Requester State（请求者状态）：**
- 跟踪本 Store 向其他 Store 请求支持的状态
- 每个远程 Store 对应一个 `SupportFromState`
- 包含：Epoch（时代号）、Expiration（过期时间）

**Supporter State（支持者状态）：**
- 跟踪本 Store 对其他 Store 提供支持的状态
- 每个被支持的 Store 对应一个 `SupportForState`
- 包含：Epoch、Expiration、是否已撤回

**生命周期阶段：**

```
1. 创建阶段 (NewSupportManager)
   - 初始化双向状态处理器
   - 创建消息队列
   - 注册指标

2. 启动阶段 (Start)
   - onRestart: 从磁盘加载持久化状态
   - 调整时钟到 MaxWithdrawn
   - 等待到 MaxRequested
   - 增加 Epoch

3. 运行阶段 (startLoop)
   - 定期发送心跳 (heartbeatTicker)
   - 处理接收的消息 (receiveQueue)
   - 撤回过期支持 (supportExpiryTicker)
   - 标记空闲 Store (idleSupportFromTicker)

4. 关闭阶段
   - stopper.ShouldQuiesce() 触发退出
```

### 1.5 为什么需要双向分离的状态

这是一个关键设计决策：**为什么要有 requesterStateHandler 和 supporterStateHandler 两个独立的状态管理器？**

**答案：职责分离与并发安全**

1. **requesterStateHandler**：
   - 负责"我向谁请求支持"
   - 驱动心跳发送
   - 处理心跳响应
   - 维护 `supportFrom` 映射

2. **supporterStateHandler**：
   - 负责"谁向我请求支持"
   - 处理接收到的心跳
   - 生成心跳响应
   - 维护 `supportFor` 映射

**为什么分离？**

- **读写分离**：`SupportFrom` 的查询不需要锁定 `SupportFor` 的状态
- **持久化独立**：两组状态可以独立写盘，减少锁竞争
- **故障隔离**：如果写盘失败，不会同时破坏两组状态
- **语义清晰**：请求支持和提供支持是两个完全不同的责任

---

## 二、控制流与组件协作（How it flows）

### 2.1 主要执行路径

SupportManager 的核心是一个**单线程事件循环**（event loop），所有状态变更都在 `startLoop` 中串行执行：

```
startLoop() 主循环
├── heartbeatTicker.C        → sendHeartbeats()
├── supportExpiryTicker.C    → withdrawSupport()
├── idleSupportFromTicker.C  → markIdleStores()
├── storesToAdd.sig          → maybeAddStores() + sendHeartbeats()
└── receiveQueue.Sig()       → handleMessages()
```

### 2.2 核心执行路径详解

#### 路径 1：心跳发送流程（Support From）

```
时间线：
T0: 用户代码调用 sm.SupportFrom(remoteStore)
    └─> 检查 requesterStateHandler.getSupportFrom(remoteStore)
    └─> 如果不存在，加入 storesToAdd.addStore(remoteStore)
    └─> 信号通知 storesToAdd.sig

T1: startLoop 收到 storesToAdd.sig
    └─> maybeAddStores()
        └─> 从 storesToAdd 中取出所有待添加的 Store
        └─> 调用 requesterStateHandler.addStore(store)
        └─> 立即调用 sendHeartbeats()  ← 关键：不等待定时器

T2: sendHeartbeats()
    ├─> 检查 SupportFromEnabled (集群设置)
    ├─> requesterStateHandler.checkOutUpdate()  // 获取可变状态
    ├─> rsfu.getHeartbeatsToSend(...)          // 生成心跳消息
    │   └─> 为每个 Store 生成 MsgHeartbeat
    │       - From: 本地 StoreID
    │       - To: 远程 StoreID
    │       - Epoch: 本地 Epoch
    │       - Expiration: now + SupportDuration
    ├─> rsfu.write(ctx, engine)                 // 持久化请求状态
    ├─> requesterStateHandler.checkInUpdate()   // 提交状态
    └─> sender.EnqueueMessage(msg)             // 发送到 Transport

T3: 远程 Store 收到心跳，发送 MsgHeartbeatResp

T4: startLoop 收到 receiveQueue.Sig()
    └─> handleMessages()
        ├─> 从 receiveQueue.Drain() 取出所有消息
        ├─> 对每个 MsgHeartbeatResp:
        │   └─> rsfu.handleHeartbeatResponse(msg)
        │       └─> 更新 supportFrom[msg.From] = {Epoch, Expiration}
        ├─> 持久化 requester 和 supporter 状态
        └─> 提交状态更新

T5: 用户再次调用 sm.SupportFrom(remoteStore)
    └─> 现在可以返回有效的 (Epoch, Expiration)
```

#### 路径 2：提供支持流程（Support For）

```
时间线：
T0: 远程 Store 发送 MsgHeartbeat 到本地

T1: Transport 调用 sm.HandleMessage(msg)
    └─> receiveQueue.Append(msg)
    └─> 信号通知 receiveQueue.sig

T2: startLoop 收到 receiveQueue.Sig()
    └─> handleMessages()
        ├─> 从 receiveQueue 中取出所有消息
        ├─> 对每个 MsgHeartbeat:
        │   └─> ssfu.handleHeartbeat(ctx, msg)
        │       ├─> 如果是新 Store，创建 supportFor[msg.From]
        │       ├─> 更新 supportFor[msg.From] = {
        │       │       Epoch: msg.Epoch,
        │       │       Expiration: msg.Expiration
        │       │   }
        │       └─> 生成 MsgHeartbeatResp 返回
        ├─> 批量写盘 (batch.Commit)
        ├─> 提交状态
        └─> 发送所有响应消息

T3: 用户调用 sm.SupportFor(remoteStore)
    └─> 立即从 supporterStateHandler.getSupportFor() 返回
    └─> 不需要等待，直接读内存状态
```

#### 路径 3：支持撤回流程（Withdrawal）

```
时间线：
T0: supportExpiryTicker 触发（默认每秒一次）

T1: withdrawSupport()
    ├─> 获取当前时间 now
    ├─> 检查 now >= minWithdrawalTS  // 保护期检查
    ├─> ssfu.withdrawSupport(ctx, now)
    │   ├─> 遍历所有 supportFor 条目
    │   ├─> 如果 expiration < now，标记为已撤回
    │   │   └─> supportFor[storeID].Epoch++  // 增加 Epoch
    │   │   └─> supportFor[storeID].Expiration = Empty
    │   └─> 返回被撤回的 StoreID 集合
    ├─> 持久化支持状态（包括新的 MaxWithdrawn）
    └─> 如果有 withdrawalCallback，调用回调
        └─> 通常用于通知 Raft 层某些副本不再可用
```

### 2.3 触发方式分析

| 操作 | 触发类型 | 频率 | 是否阻塞 |
|------|---------|------|---------|
| `sendHeartbeats()` | 定时器（heartbeatTicker）<br>+ 被动（storesToAdd.sig） | 默认 3ms | 否（持久化可能慢） |
| `handleMessages()` | 信号驱动（receiveQueue.Sig） | 按消息到达率 | 否（批量处理） |
| `withdrawSupport()` | 定时器（supportExpiryTicker） | 默认 1ms | 否 |
| `SupportFrom()` | 同步查询 | 按调用频率 | **是**（读锁） |
| `SupportFor()` | 同步查询 | 按调用频率 | **是**（读锁） |

**关键观察：**

1. **定时器 + 信号混合**：
   - 定时器保证最小活性（即使没有外部触发，也会定期发送心跳）
   - 信号机制实现立即响应（不等待定时器触发）

2. **查询路径不阻塞主循环**：
   - `SupportFrom()` 和 `SupportFor()` 直接读取内存状态
   - 不需要进入 `startLoop` 的事件队列
   - 使用读锁保护并发访问

3. **写操作单线程化**：
   - 所有状态变更都在 `startLoop` 中串行执行
   - 避免复杂的并发控制
   - 简化了正确性推理

### 2.4 与其他模块的交互

#### 2.4.1 与 Raft 层的交互

Raft 通过 `Fabric` 接口查询 Store Liveness：

```go
// 在 Raft 中使用
func (r *Replica) checkQuorum() bool {
    for _, replicaDesc := range r.Desc().Replicas {
        storeID := replicaDesc.StoreID
        epoch, expiration := r.store.storeLiveness.SupportFrom(
            slpb.StoreIdent{StoreID: storeID}
        )
        if expiration.IsEmpty() {
            // 该副本不可达，无法形成 quorum
            return false
        }
    }
    return true
}
```

**关键点：**
- Raft 不直接持有 SupportManager，通过 Fabric 接口解耦
- 查询是非阻塞的（从内存读取）
- 如果首次查询返回空，会触发异步的心跳发送

#### 2.4.2 与 Transport 的交互

Transport 负责网络传输，是 `MessageSender` 接口的实现：

```go
type MessageSender interface {
    EnqueueMessage(ctx context.Context, msg slpb.Message) (sent bool)
}
```

**发送路径：**
```
SupportManager.sendHeartbeats()
    └─> sender.EnqueueMessage(msg)
        └─> Transport.sendAsync(msg)
            └─> gRPC stream 发送到远程节点
```

**接收路径：**
```
远程节点的 gRPC handler
    └─> Transport.HandleMessage(msg)
        └─> SupportManager.HandleMessage(msg)
            └─> receiveQueue.Append(msg)
```

**关键点：**
- 发送是异步的（不等待网络 ACK）
- 接收也是异步的（先入队，后处理）
- 消息可能丢失（网络不可靠）

#### 2.4.3 与 Clock 的交互

HLC（Hybrid Logical Clock）用于生成和比较时间戳：

```go
// 生成心跳过期时间
expiration := sm.clock.Now().AddDuration(sm.options.SupportDuration)

// 检查支持是否过期
if now.ToTimestamp().Less(supportState.Expiration) {
    // 仍然有效
}
```

**关键语义：**
- 使用 `ClockTimestamp` 而非 `Timestamp`，因为心跳不需要因果关系保证
- 重启时，时钟会被推进到 `MaxWithdrawn` 和 `MaxRequested`
- 这保证了重启后的心跳不会意外覆盖旧状态

#### 2.4.4 共享状态模型

SupportManager 的状态通过以下机制共享：

```
┌─────────────────────────────────────────┐
│          SupportManager                 │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │  requesterStateHandler            │ │
│  │  (读锁保护)                        │ │
│  │  ├── supportFrom map              │ │ ← SupportFrom() 读取
│  │  └── meta (MaxRequested, Epoch)   │ │
│  └───────────────────────────────────┘ │
│                ↑                        │
│                │ checkOut/checkIn       │
│                │ (COW模式)              │
│                ↓                        │
│       startLoop (独占写入)              │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │  supporterStateHandler            │ │
│  │  (读锁保护)                        │ │
│  │  ├── supportFor map               │ │ ← SupportFor() 读取
│  │  └── meta (MaxWithdrawn)          │ │
│  └───────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

**并发模型：**

1. **读路径（SupportFrom/SupportFor）**：
   - 直接访问内存状态
   - 使用读锁保护
   - 无需进入事件循环

2. **写路径（startLoop）**：
   - 使用 `checkOutUpdate()` 获取可变副本
   - 修改副本
   - 持久化到磁盘
   - 使用 `checkInUpdate()` 提交（内部用写锁）

3. **COW（Copy-on-Write）模式**：
   - 避免在持久化期间持有锁
   - 写盘失败不影响读路径
   - 保证了读写并发安全

---

## 三、关键函数与核心逻辑（How it works）

### 3.1 NewSupportManager：构造与初始化

```go
func NewSupportManager(
    storeID slpb.StoreIdent,
    engine storage.Engine,
    options Options,
    settings *clustersettings.Settings,
    stopper *stop.Stopper,
    clock *hlc.Clock,
    heartbeatTicker *timeutil.BroadcastTicker,
    sender MessageSender,
    knobs *SupportManagerKnobs,
) *SupportManager
```

**输入：**
- `storeID`：本地 Store 的唯一标识（NodeID + StoreID）
- `engine`：持久化引擎（通常是 Pebble）
- `options`：配置选项（心跳间隔、支持持续时间等）
- `clock`：HLC 时钟
- `heartbeatTicker`：节点级共享的广播定时器（减少 goroutine）
- `sender`：消息发送器（通常是 Transport）

**核心初始化逻辑：**

```go
return &SupportManager{
    storeID:               storeID,
    engine:                engine,
    options:               options,
    settings:              settings,
    stopper:               stopper,
    clock:                 clock,
    heartbeatTicker:       heartbeatTicker,
    sender:                sender,
    knobs:                 knobs,

    // === 关键：创建双向状态处理器 ===
    requesterStateHandler: newRequesterStateHandler(),
    supporterStateHandler: newSupporterStateHandler(),

    // === 关键：创建异步通信组件 ===
    receiveQueue:          newReceiveQueue(),
    storesToAdd:           newStoresToAdd(),

    metrics:               newSupportManagerMetrics(),
}
```

**为什么这样设计？**

1. **延迟启动**：构造函数不启动任何异步任务
   - 允许调用者在 `Start()` 之前完成额外配置
   - 避免构造过程中的竞态条件

2. **共享 heartbeatTicker**：
   - 一个节点可能有多个 Store
   - 共享定时器避免创建大量 goroutine
   - 使用 `BroadcastTicker` 模式，每个 Store 调用 `NewTicker()` 获取自己的通道

3. **可选的 knobs**：
   - 用于测试时注入行为（如禁用心跳、模拟磁盘故障）
   - 生产环境为 `nil`

### 3.2 Start：启动与状态恢复

```go
func (sm *SupportManager) Start(ctx context.Context) error {
    // 1. 同步调用 onRestart，恢复持久化状态
    if err := sm.onRestart(ctx); err != nil {
        return err
    }

    // 2. 启动异步事件循环
    ctx, hdl, err := sm.stopper.GetHandle(ctx, stop.TaskOpts{
        TaskName: "storeliveness.SupportManager: loop",
    })
    if err != nil {
        return err
    }
    go func(ctx context.Context) {
        defer hdl.Activate(ctx).Release(ctx)
        sm.startLoop(ctx)
    }(ctx)
    return nil
}
```

**为什么 onRestart 必须同步调用？**

这是一个关键的正确性保证：

```go
func (sm *SupportManager) onRestart(ctx context.Context) error {
    // 步骤 1: 从磁盘加载持久化状态
    if err := sm.supporterStateHandler.read(ctx, sm.engine); err != nil {
        return err
    }
    if err := sm.requesterStateHandler.read(ctx, sm.engine); err != nil {
        return err
    }

    // 步骤 2: 推进时钟到 MaxWithdrawn
    if err := sm.clock.UpdateAndCheckMaxOffset(
        ctx, sm.supporterStateHandler.supporterState.meta.MaxWithdrawn,
    ); err != nil {
        return err
    }

    // 步骤 3: 等待到 MaxRequested
    if err := sm.clock.SleepUntil(
        ctx, sm.requesterStateHandler.requesterState.meta.MaxRequested,
    ); err != nil {
        return err
    }

    // 步骤 4: 设置最小撤回时间（保护期）
    sm.minWithdrawalTS = sm.clock.Now().AddDuration(
        sm.options.SupportWithdrawalGracePeriod,
    )

    // 步骤 5: 增加 Epoch
    rsfu := sm.requesterStateHandler.checkOutUpdate()
    defer sm.requesterStateHandler.finishUpdate(rsfu)
    rsfu.incrementMaxEpoch()
    if err := rsfu.write(ctx, sm.engine); err != nil {
        return err
    }
    sm.requesterStateHandler.checkInUpdate(rsfu)
    return nil
}
```

**深度分析：**

**步骤 2 的关键性：推进时钟到 MaxWithdrawn**

假设场景：
- T1：本地时钟是 100
- T2：崩溃前，本地撤回了对 Store B 的支持，记录 `MaxWithdrawn = 200`
- T3：重启后，本地时钟回退到 150

如果不推进时钟：
```
现在时钟 = 150
MaxWithdrawn = 200
新心跳的 Expiration = 150 + 6ms = 156

问题：156 < 200，意味着新心跳的过期时间早于上次撤回时间
      这会导致接收方误认为这是一个"旧的、已过期的"支持承诺
```

**步骤 3 的关键性：等待到 MaxRequested**

假设场景：
- T1：崩溃前，本地请求支持到时间 `MaxRequested = 300`
- T2：重启后，本地时钟是 200

如果不等待：
```
立即发送心跳，Expiration = 200 + 6ms = 206

问题：远程 Store 可能仍然认为我们的支持有效到 300
      但我们自己认为只到 206
      这会导致支持状态不一致
```

通过等待到 300，我们保证：
- 旧的支持承诺已经过期
- 新的心跳不会与旧状态冲突

**步骤 5 的关键性：增加 Epoch**

Epoch 是一个单调递增的时代号，用于区分不同生命周期的心跳：

```
崩溃前: Epoch = 5, Expiration = 300
重启后: Epoch = 6, Expiration = 400

远程 Store 收到 Epoch=6 的心跳：
  if msg.Epoch > local.Epoch {
      // 这是新时代的心跳，接受并更新
      local.Epoch = msg.Epoch
      local.Expiration = msg.Expiration
  }
```

这避免了"僵尸心跳"问题：
- 网络中可能还有旧的（Epoch=5）心跳在传输
- 通过 Epoch 区分，接收方可以忽略旧心跳

### 3.3 startLoop：事件循环的核心

```go
func (sm *SupportManager) startLoop(ctx context.Context) {
    if sm.heartbeatTicker == nil {
        sm.heartbeatTicker = timeutil.NewBroadcastTicker(sm.options.HeartbeatInterval)
        defer sm.heartbeatTicker.Stop()
    }
    heartbeatTicker := sm.heartbeatTicker.NewTicker()
    defer heartbeatTicker.Stop()

    supportExpiryTicker := time.NewTicker(sm.options.SupportExpiryInterval)
    defer supportExpiryTicker.Stop()

    idleSupportFromTicker := time.NewTicker(sm.options.IdleSupportFromInterval)
    defer idleSupportFromTicker.Stop()

    for {
        // === 关键：动态选择是否监听 receiveQueue ===
        var receiveQueueSig <-chan struct{}
        if len(heartbeatTicker.C) == 0 &&
            len(supportExpiryTicker.C) == 0 &&
            len(sm.storesToAdd.sig) == 0 {
            receiveQueueSig = sm.receiveQueue.Sig()
        }

        select {
        case <-sm.storesToAdd.sig:
            sm.maybeAddStores(ctx)
            sm.sendHeartbeats(ctx)  // 立即发送心跳

        case <-heartbeatTicker.C:
            sm.sendHeartbeats(ctx)

        case <-supportExpiryTicker.C:
            sm.withdrawSupport(ctx)

        case <-idleSupportFromTicker.C:
            sm.requesterStateHandler.markIdleStores(ctx)

        case <-receiveQueueSig:
            msgs := sm.receiveQueue.Drain()
            sm.handleMessages(ctx, msgs)

        case <-sm.stopper.ShouldQuiesce():
            return
        }
    }
}
```

**为什么动态选择 receiveQueueSig？**

这是一个精妙的优先级控制设计：

```go
if len(heartbeatTicker.C) == 0 &&
    len(supportExpiryTicker.C) == 0 &&
    len(sm.storesToAdd.sig) == 0 {
    receiveQueueSig = sm.receiveQueue.Sig()
}
```

**问题场景：**
假设消息不断涌入，`receiveQueue.Sig()` 一直有信号。如果我们总是监听它，可能出现：

```
for {
    select {
    case <-heartbeatTicker.C:       // 永远无法执行
        sendHeartbeats()
    case <-receiveQueueSig:         // 总是这个分支
        handleMessages()
    }
}
```

由于 `select` 的随机选择语义，高频的接收消息可能**饿死心跳发送**。

**解决方案：**
只有在所有高优先级通道都空闲时，才监听 `receiveQueue`：

```
如果有待发送心跳     → 不监听 receiveQueue，优先发送心跳
如果有待撤回支持     → 不监听 receiveQueue，优先撤回
如果有待添加 Store   → 不监听 receiveQueue，优先添加

只有在以上都空闲时  → 才处理接收队列
```

这保证了：
1. 心跳发送不会被延迟（活性保证）
2. 支持撤回不会被延迟（安全性保证）
3. 消息处理可以批量进行（吞吐量优化）

**为什么可以这样做？**
因为消息接收本身是**幂等**的：
- 延迟处理消息不会导致正确性问题
- 远程 Store 会重试心跳
- 最终一定会被处理

### 3.4 sendHeartbeats：心跳发送的完整流程

```go
func (sm *SupportManager) sendHeartbeats(ctx context.Context) {
    // 1. 检查是否启用
    if !sm.SupportFromEnabled(ctx) {
        return
    }

    // 2. 检查测试 knobs
    if sm.knobs != nil && sm.knobs.DisableHeartbeats != nil &&
       sm.knobs.DisableHeartbeats.Load() == sm.storeID {
        return
    }

    // 3. 获取可变状态（COW 模式）
    rsfu := sm.requesterStateHandler.checkOutUpdate()
    defer sm.requesterStateHandler.finishUpdate(rsfu)

    // 4. 生成心跳消息
    livenessInterval := sm.options.SupportDuration
    heartbeats := rsfu.getHeartbeatsToSend(
        sm.storeID,
        sm.clock.Now(),
        livenessInterval,
    )

    // 5. 持久化状态
    beforePersist := timeutil.Now()
    if err := rsfu.write(ctx, sm.engine); err != nil {
        log.KvExec.Warningf(ctx, "failed to write requester meta: %v", err)
        sm.metrics.HeartbeatFailures.Inc(int64(len(heartbeats)))
        return
    }
    persistDur := timeutil.Since(beforePersist)
    sm.metrics.HeartbeatPersistDuration.RecordValue(persistDur.Nanoseconds())

    // 6. 提交状态（释放写锁）
    sm.requesterStateHandler.checkInUpdate(rsfu)

    // 7. 发送消息（不持有锁）
    successes := 0
    for _, msg := range heartbeats {
        if sent := sm.sender.EnqueueMessage(ctx, msg); sent {
            successes++
        } else {
            log.KvExec.Warningf(ctx, "failed to send heartbeat to store %+v", msg.To)
        }
    }
    sm.metrics.HeartbeatSuccesses.Inc(int64(successes))
    sm.metrics.HeartbeatFailures.Inc(int64(len(heartbeats) - successes))
}
```

**关键点分析：**

**（1）为什么先持久化再发送？**

这是一个关键的顺序保证：

```
错误顺序：先发送，后持久化
  T1: 发送心跳，Expiration = 300
  T2: 崩溃，持久化失败
  T3: 重启，本地不知道曾经请求过支持到 300
  T4: 远程 Store 仍然认为支持有效到 300
  → 状态不一致

正确顺序：先持久化，后发送
  T1: 持久化 Expiration = 300
  T2: 发送心跳
  T3: 崩溃
  T4: 重启，从磁盘恢复，知道曾经请求到 300
  T5: 等待到 300（在 onRestart 中）
  → 状态一致
```

**（2）为什么持久化后立即 checkInUpdate？**

```go
rsfu.write(ctx, sm.engine)         // 持久化到磁盘
sm.requesterStateHandler.checkInUpdate(rsfu)  // 提交到内存
// 此时释放写锁
sender.EnqueueMessage(msg)         // 发送消息（不持有锁）
```

这避免了在网络发送期间持有锁：
- 网络发送可能慢（几毫秒到几十毫秒）
- 持有锁会阻塞 `SupportFrom()` 的查询
- 先提交状态，保证查询能立即看到最新值

**（3）getHeartbeatsToSend 的逻辑**

```go
func (rsfu *requesterStateForUpdate) getHeartbeatsToSend(
    from slpb.StoreIdent,
    now hlc.ClockTimestamp,
    livenessInterval time.Duration,
) []slpb.Message {
    expiration := now.ToTimestamp().AddDuration(livenessInterval)
    var heartbeats []slpb.Message

    for storeID, state := range rsfu.supportFrom {
        // 更新 MaxRequested
        if rsfu.meta.MaxRequested.Less(expiration) {
            rsfu.meta.MaxRequested = expiration
        }

        heartbeats = append(heartbeats, slpb.Message{
            Type:       slpb.MsgHeartbeat,
            From:       from,
            To:         storeID,
            Epoch:      rsfu.meta.MaxEpoch,
            Expiration: expiration,
        })
    }
    return heartbeats
}
```

**关键不变量：**
- `MaxRequested` 是所有已发送心跳的最大过期时间
- 重启时，必须等待到 `MaxRequested` 才能发送新心跳
- 这保证了不会有两个冲突的支持承诺同时存在

---

## 四、消息处理与状态更新

### 4.1 handleMessages：批量处理的艺术

```go
func (sm *SupportManager) handleMessages(ctx context.Context, msgs []*slpb.Message) {
    log.KvExec.VInfof(ctx, "drained receive queue of size %d", len(msgs))

    // 1. 同时 checkout 两个状态处理器
    rsfu := sm.requesterStateHandler.checkOutUpdate()
    defer sm.requesterStateHandler.finishUpdate(rsfu)
    ssfu := sm.supporterStateHandler.checkOutUpdate()
    defer sm.supporterStateHandler.finishUpdate(ssfu)

    var responses []slpb.Message

    // 2. 处理所有消息（内存操作）
    for _, msg := range msgs {
        sm.metrics.ReceiveQueueSize.Dec(1)
        sm.metrics.ReceiveQueueBytes.Dec(int64(msg.Size()))

        switch msg.Type {
        case slpb.MsgHeartbeat:
            responses = append(responses, ssfu.handleHeartbeat(ctx, msg))
        case slpb.MsgHeartbeatResp:
            rsfu.handleHeartbeatResponse(ctx, msg)
        default:
            log.KvExec.Errorf(ctx, "unexpected message type: %v", msg.Type)
        }
    }

    // 3. 批量持久化（单次 fsync）
    batch := sm.engine.NewBatch()
    defer batch.Close()
    if err := rsfu.write(ctx, batch); err != nil {
        log.KvExec.Warningf(ctx, "failed to write requester meta: %v", err)
        sm.metrics.MessageHandleFailures.Inc(int64(len(msgs)))
        return
    }
    if err := ssfu.write(ctx, batch); err != nil {
        log.KvExec.Warningf(ctx, "failed to write supporter meta: %v", err)
        sm.metrics.MessageHandleFailures.Inc(int64(len(msgs)))
        return
    }

    beforePersist := timeutil.Now()
    if err := batch.Commit(true /* sync */); err != nil {
        log.KvExec.Warningf(ctx, "failed to sync supporter and requester state: %v", err)
        sm.metrics.MessageHandleFailures.Inc(int64(len(msgs)))
        return
    }
    persistDur := timeutil.Since(beforePersist)
    sm.metrics.MessageHandlePersistDuration.RecordValue(persistDur.Nanoseconds())

    // 4. 提交状态（释放锁）
    sm.requesterStateHandler.checkInUpdate(rsfu)
    sm.supporterStateHandler.checkInUpdate(ssfu)
    sm.metrics.MessageHandleSuccesses.Inc(int64(len(msgs)))

    // 5. 发送响应（不持有锁）
    for _, response := range responses {
        _ = sm.sender.EnqueueMessage(ctx, response)
    }
}
```

**批量处理的优势：**

1. **减少 fsync 次数**：
   ```
   无批量：100 条消息 → 100 次 fsync（~100ms）
   有批量：100 条消息 → 1 次 fsync（~1ms）
   ```

2. **减少锁竞争**：
   ```
   无批量：每条消息都要 checkout → modify → checkin
   有批量：一次 checkout → 修改 100 次 → 一次 checkin
   ```

3. **原子性保证**：
   - 要么所有消息都处理成功
   - 要么全部失败
   - 没有中间状态

**为什么可以批量？**

因为消息处理是**可交换**的（commutative）：

```
消息 A: Store X 请求支持到 100
消息 B: Store Y 请求支持到 200

处理顺序 A → B 或 B → A 结果相同：
  supportFor[X] = {Expiration: 100}
  supportFor[Y] = {Expiration: 200}
```

唯一例外是同一个 Store 的多个消息，但由于使用了 Epoch 机制，后到的消息会覆盖先到的，顺序无关。

### 4.2 withdrawSupport：支持撤回的精细控制

```go
func (sm *SupportManager) withdrawSupport(ctx context.Context) {
    now := sm.clock.NowAsClockTimestamp()

    // 1. 保护期检查（关键安全机制）
    if now.ToTimestamp().Less(sm.minWithdrawalTS) {
        return
    }

    // 2. Checkout 状态
    ssfu := sm.supporterStateHandler.checkOutUpdate()
    defer sm.supporterStateHandler.finishUpdate(ssfu)

    // 3. 执行撤回逻辑
    supportWithdrawnForStoreIDs := ssfu.withdrawSupport(ctx, now)
    numWithdrawn := len(supportWithdrawnForStoreIDs)
    if numWithdrawn == 0 {
        return
    }

    // 4. 持久化
    batch := sm.engine.NewBatch()
    defer batch.Close()
    if err := ssfu.write(ctx, batch); err != nil {
        log.KvExec.Warningf(ctx, "failed to write supporter meta and state: %v", err)
        sm.metrics.SupportWithdrawFailures.Inc(int64(numWithdrawn))
        return
    }

    beforePersist := timeutil.Now()
    if err := batch.Commit(true /* sync */); err != nil {
        log.KvExec.Warningf(ctx, "failed to commit supporter meta and state: %v", err)
        sm.metrics.SupportWithdrawFailures.Inc(int64(numWithdrawn))
        return
    }
    persistDur := timeutil.Since(beforePersist)
    sm.metrics.SupportWithdrawPersistDuration.RecordValue(persistDur.Nanoseconds())

    // 5. 提交状态
    sm.supporterStateHandler.checkInUpdate(ssfu)
    log.KvExec.Infof(ctx, "withdrew support from %d stores", numWithdrawn)
    sm.metrics.SupportWithdrawSuccesses.Inc(int64(numWithdrawn))

    // 6. 调用回调（关键：通知上层）
    if sm.withdrawalCallback != nil {
        beforeProcess := timeutil.Now()
        sm.withdrawalCallback(supportWithdrawnForStoreIDs)
        processDur := timeutil.Since(beforeProcess)
        if processDur > minCallbackDurationToRecord {
            sm.metrics.CallbacksProcessingDuration.RecordValue(processDur.Nanoseconds())
        }
        log.KvExec.Infof(ctx, "invoked callback for %d stores", numWithdrawn)
    }
}
```

**保护期（Grace Period）的关键作用：**

```go
// 在 onRestart 中设置
sm.minWithdrawalTS = sm.clock.Now().AddDuration(sm.options.SupportWithdrawalGracePeriod)
```

**为什么需要保护期？**

场景：
```
T0: 本地 Store 重启
T1: 加载磁盘状态，发现对 Store X 的支持已过期
T2: 如果立即撤回支持，会发生什么？

问题：
  - Store X 可能刚刚发送了心跳，但还在网络中
  - 如果我们撤回支持，然后心跳到达，又会重新提供支持
  - 然后很快又过期，又撤回
  - 导致支持状态抖动
```

**保护期解决方案：**
```
T0: 重启
T1: 设置 minWithdrawalTS = now + 5s
T2: 在接下来的 5s 内，不撤回任何支持
T3: 5s 后，可以安全地撤回过期支持
```

这给了远程 Store 足够的时间：
- 发现本地 Store 重启
- 重新发送心跳
- 重新建立支持关系

**withdrawalCallback 的用途：**

回调通常连接到 Raft 层：

```go
sm.RegisterSupportWithdrawalCallback(func(withdrawnStores map[roachpb.StoreID]struct{}) {
    for storeID := range withdrawnStores {
        // 通知 Raft：Store storeID 不再可达
        // 可能触发：
        // - 撤回 leaseholder（如果租约在该 Store 上）
        // - 标记副本为不可用
        // - 触发 Raft 配置变更（移除副本）
    }
})
```

这是一个**关键的反馈机制**：
- Store Liveness 检测到故障
- 通知 Raft 层
- Raft 层采取行动（转移租约、移除副本等）

---

## 五、设计模式分析（重点）

### 5.1 Event Loop（事件循环）模式

**模式定义：**
将所有异步事件（定时器、消息、信号）统一到一个单线程的循环中处理。

**在 SupportManager 中的应用：**

```go
for {
    select {
    case <-heartbeatTicker.C:       // 定时事件
        sendHeartbeats()
    case <-receiveQueueSig:         // 消息事件
        handleMessages()
    case <-supportExpiryTicker.C:   // 定时事件
        withdrawSupport()
    case <-storesToAdd.sig:         // 信号事件
        maybeAddStores()
    }
}
```

**为什么选择这个模式？**

1. **简化并发模型**：
   - 无需复杂的锁
   - 无需 channel 同步
   - 所有写操作串行化

2. **保证顺序性**：
   - 事件按到达顺序处理
   - 状态变更有明确的时间线

3. **易于推理**：
   - 没有并发竞争
   - 没有死锁风险

**代价：**
- 单个事件处理慢会阻塞后续事件
- 不适合 CPU 密集型任务

**为什么这里可以接受？**
- Store Liveness 的操作都很快（毫秒级）
- 主要开销是磁盘 IO，已经异步化（使用 batch）

### 5.2 Copy-on-Write（COW）状态更新模式

**模式定义：**
修改共享状态时，先复制一份，修改副本，然后原子替换。

**在 SupportManager 中的应用：**

```go
// 1. Checkout：获取可变副本
rsfu := sm.requesterStateHandler.checkOutUpdate()
defer sm.requesterStateHandler.finishUpdate(rsfu)

// 2. 修改副本（不持有锁）
rsfu.supportFrom[storeID] = newState

// 3. 持久化（不持有锁）
rsfu.write(ctx, engine)

// 4. CheckIn：原子替换（短暂持有写锁）
sm.requesterStateHandler.checkInUpdate(rsfu)
```

**实现细节：**

```go
type requesterStateHandler struct {
    mu struct {
        syncutil.RWMutex
        requesterState requesterState  // 只读副本
    }
    update struct {
        syncutil.Mutex
        requesterStateForUpdate *requesterStateForUpdate  // 可写副本
    }
}

func (rsh *requesterStateHandler) checkOutUpdate() *requesterStateForUpdate {
    rsh.update.Lock()  // 独占修改权
    // 复制当前状态
    rsh.update.requesterStateForUpdate = &requesterStateForUpdate{
        supportFrom: maps.Clone(rsh.mu.requesterState.supportFrom),
        meta:        rsh.mu.requesterState.meta,
    }
    return rsh.update.requesterStateForUpdate
}

func (rsh *requesterStateHandler) checkInUpdate(rsfu *requesterStateForUpdate) {
    rsh.mu.Lock()  // 写锁
    rsh.mu.requesterState = rsfu.toImmutable()
    rsh.mu.Unlock()
    // update.Mutex 在 finishUpdate 中释放
}
```

**为什么这样设计？**

传统方案（持有锁修改）：
```go
rsh.mu.Lock()
rsh.state.supportFrom[storeID] = newState
engine.Write(...)  // 持有锁期间写盘，可能几十毫秒
rsh.mu.Unlock()
```

问题：
- 写盘期间阻塞所有读操作（`SupportFrom()`）
- 导致查询延迟飙升

COW 方案：
```go
checkout (获取副本)
修改副本（不持有锁）
写盘（不持有锁）← 关键：读操作仍然可以进行
checkin (短暂持锁，原子替换)
```

优势：
- 读操作几乎不被阻塞
- 写盘失败不影响读路径
- 状态一致性由持久化顺序保证

**代价：**
- 需要复制状态（内存开销）
- 不适合超大状态（但 supportFrom map 通常很小）

### 5.3 Two-Phase Update（两阶段更新）模式

**模式定义：**
状态更新分为两个阶段：准备阶段（可取消）和提交阶段（不可取消）。

**在 SupportManager 中的应用：**

```go
// Phase 1: Prepare（可能失败）
rsfu := checkOutUpdate()          // 获取可变状态
rsfu.modify(...)                  // 修改
err := rsfu.write(ctx, engine)    // 持久化
if err != nil {
    finishUpdate(rsfu)            // 放弃修改
    return err
}

// Phase 2: Commit（不会失败）
checkInUpdate(rsfu)               // 原子提交到内存
finishUpdate(rsfu)                // 释放更新锁
```

**关键不变量：**
- 只有持久化成功后，才会提交到内存
- 如果持久化失败，内存状态不变
- 重启后，从磁盘恢复，保证一致性

**为什么需要两阶段？**

场景：
```
T1: 修改内存状态：supportFrom[X] = {Epoch: 5, Exp: 100}
T2: 尝试持久化，失败（磁盘故障）
T3: 崩溃
T4: 重启，从磁盘恢复，没有 supportFrom[X] 的记录
T5: 但远程 Store 认为我们支持它到 100
→ 状态不一致
```

两阶段解决：
```
T1: checkout（复制状态）
T2: 修改副本
T3: 持久化，失败
T4: 放弃副本，内存状态未变
T5: 用户看到错误，可以重试
→ 状态一致
```

### 5.4 Queue-Based Decoupling（队列解耦）模式

**模式定义：**
使用队列解耦生产者和消费者，允许异步处理和批量优化。

**在 SupportManager 中的应用：**

```go
// 生产者：网络层
func (sm *SupportManager) HandleMessage(msg *slpb.Message) error {
    return sm.receiveQueue.Append(msg)  // 不阻塞，立即返回
}

// 消费者：事件循环
case <-sm.receiveQueue.Sig():
    msgs := sm.receiveQueue.Drain()    // 批量取出
    sm.handleMessages(ctx, msgs)       // 批量处理
```

**关键设计：**

1. **有界队列**：
   ```go
   const maxReceiveQueueSize = 10_000

   func (q *receiveQueue) Append(msg *slpb.Message) error {
       if len(q.mu.msgs) >= maxReceiveQueueSize {
           return receiveQueueSizeLimitReachedErr
       }
       q.mu.msgs = append(q.mu.msgs, msg)
       return nil
   }
   ```

2. **背压（Backpressure）机制**：
   - 队列满时，拒绝新消息
   - 发送方看到错误，可以重试
   - 避免内存无限增长

3. **信号通知**：
   ```go
   select {
   case q.sig <- struct{}{}:  // 尝试发送信号
   default:                   // 如果通道已满，忽略
   }
   ```

   关键：使用缓冲为 1 的通道
   - 如果已经有信号，不需要重复发送
   - 避免信号积压

**为什么选择队列模式？**

直接调用（无队列）：
```go
func (sm *SupportManager) HandleMessage(msg *slpb.Message) error {
    sm.mu.Lock()
    sm.handleMessage(msg)
    sm.mu.Unlock()
    return nil
}
```

问题：
- 网络线程阻塞在消息处理上
- 影响网络吞吐量
- 无法批量处理

队列方案：
- 网络线程快速返回
- 消息在后台批量处理
- 减少 fsync 次数

### 5.5 State Machine（状态机）模式

**模式定义：**
将对象的行为建模为状态和转换的集合。

**在 SupportManager 中的应用：**

每个 Store 的支持状态是一个状态机：

```
           +----------------+
           |    Unknown     |  ← 初始状态
           +----------------+
                    |
                    | SupportFrom() 首次调用
                    ↓
           +----------------+
           |  Idle/Pending  |  ← 已添加，等待响应
           +----------------+
                    |
                    | 收到 MsgHeartbeatResp
                    ↓
           +----------------+
           |    Supported   |  ← 有效支持
           +----------------+
                    |
                    | Expiration 过期
                    ↓
           +----------------+
           |    Expired     |  ← 支持过期
           +----------------+
                    |
                    | 收到新的 MsgHeartbeatResp
                    ↓
           +----------------+
           |    Supported   |  ← 重新支持
           +----------------+
```

**状态转换的触发条件：**

| 当前状态 | 事件 | 新状态 | 副作用 |
|---------|------|--------|-------|
| Unknown | `SupportFrom()` | Idle | 加入 storesToAdd，触发心跳 |
| Idle | `MsgHeartbeatResp` | Supported | 更新 Expiration |
| Supported | 时间推进 > Expiration | Expired | SupportFrom() 返回空 |
| Expired | `MsgHeartbeatResp` | Supported | 重新支持 |
| Supported | 收到更高 Epoch | Supported | 更新 Epoch 和 Expiration |

**为什么使用状态机？**

- 明确定义了所有可能的状态
- 明确定义了所有合法的转换
- 易于验证正确性
- 易于测试（每个转换可以独立测试）

---

由于内容较长，我将在此处暂停，这是文档的上半部分。上半部分覆盖了：

1. ✅ 职责边界与设计动机（Why）
2. ✅ 控制流与组件协作（How it flows）
3. ✅ 关键函数与核心逻辑（How it works）的前半部分
4. ✅ 设计模式分析（重点）

下半部分将包括：
- 运行时行为与系统反馈
- 具体运行示例
- 设计取舍与替代方案
- 总结与心智模型

现在请输入"继续"，我将创建下半部分。
