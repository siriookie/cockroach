# SupportManager 深度剖析——基于 COW 模式的双向 Store 心跳与状态管理系统

## 一、职责边界与设计动机（Why）

### 1.1 背景：为什么需要 SupportManager？

在 CockroachDB 的分布式架构中，每个节点可能包含多个 Store，Store 之间需要知道彼此的存活状态。传统的节点级（Node Liveness）存活检测有以下局限：

**问题场景：**

1. **粒度过粗**：Node Liveness 只能检测整个节点是否存活
   - 节点存活 ≠ 所有 Store 都健康
   - 某个 Store 的磁盘可能出问题，但节点仍存活

2. **资源隔离不足**：
   - 一个 Store 的磁盘 stall 可能拖累整个节点
   - 无法单独标记某个 Store 为不可用

3. **副本放置决策受限**：
   - Allocator 需要知道具体哪个 Store 不健康
   - 不能仅凭节点存活性做副本迁移决策

4. **容错能力弱**：
   - 节点级心跳失败 → 整个节点被驱逐
   - Store 级心跳失败 → 仅该 Store 失去支持

**如果没有 SupportManager，系统会遇到：**

- **误判风险**：健康的 Store 因节点级问题被标记为不可用
- **恢复时间长**：需要等待整个节点恢复，而非单个 Store
- **资源浪费**：无法充分利用部分健康的节点
- **运维复杂**：无法精细化控制 Store 级别的存活性

### 1.2 SupportManager 的核心价值

SupportManager 提供了**双向的、持久化的、精细到 Store 级别的存活性检测机制**：

```
┌─────────────────────────────────────────────────────────┐
│  Store A                                                │
│  ├─ SupportManager                                      │
│  │   ├─ requesterStateHandler (请求支持)                │
│  │   │   └─ "我需要 Store B、C 支持我"                  │
│  │   │       └─ 发送心跳 → B, C                         │
│  │   │       └─ 接收心跳响应 ← B, C                     │
│  │   │                                                  │
│  │   └─ supporterStateHandler (提供支持)                │
│  │       └─ "我支持 Store X、Y"                          │
│  │           └─ 接收心跳 ← X, Y                         │
│  │           └─ 发送心跳响应 → X, Y                     │
│  │           └─ 定期检查超时，撤回支持                   │
└─────────────────────────────────────────────────────────┘
                    ↓ 心跳                ↑ 响应
┌─────────────────────────────────────────────────────────┐
│  Store B                                                │
│  └─ SupportManager (对称的双向结构)                      │
└─────────────────────────────────────────────────────────┘
```

**核心语义：**

- **双向心跳**：每个 Store 既是请求者（Requester）也是支持者（Supporter）
- **Epoch 机制**：每次重启递增 Epoch，防止脑裂
- **持久化保证**：关键状态写入磁盘，重启后恢复
- **Grace Period**：给其他 Store 一个缓冲期，避免误判

### 1.3 在系统中的位置

SupportManager 位于 **KV 层的 Store Liveness 子系统** 中：

```
┌─────────────────────────────────────────────────────┐
│  SQL Layer                                          │
└─────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────┐
│  KV Layer (Transaction & Replication)               │
│  ├─ Store                                           │
│  │   ├─ Node Liveness (节点级)                      │
│  │   └─ Store Liveness (Store 级) ← 我们在这里      │
│  │       └─ SupportManager                          │
│  │           ├─ requesterStateHandler               │
│  │           ├─ supporterStateHandler               │
│  │           └─ Transport (消息传输)                 │
└─────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────┐
│  Storage Layer (Pebble/RocksDB)                     │
└─────────────────────────────────────────────────────┘
```

**上游依赖：**
- **Allocator**：根据 Store Liveness 做副本放置决策
- **Replica**：检查 Store 是否有足够支持再执行操作
- **Admin Commands**：在执行前验证 Store 存活性

**下游依赖：**
- **Transport**：通过 gRPC 发送心跳消息
- **Storage Engine**：持久化 requesterState 和 supporterState
- **Clock**：HLC 时钟，用于时间戳和超时判断

### 1.4 核心抽象与对象

SupportManager 的设计围绕以下核心抽象：

#### 1.4.1 长期存在的核心对象

```go
type SupportManager struct {
    storeID  slpb.StoreIdent          // 本 Store 的标识
    engine   storage.Engine            // 持久化引擎
    clock    *hlc.Clock                // HLC 时钟
    sender   MessageSender             // 消息发送器
    stopper  *stop.Stopper             // 停止控制器

    // === 定时器 ===
    heartbeatTicker *timeutil.BroadcastTicker // 心跳定时器

    // === 异步通信 ===
    receiveQueue receiveQueue          // 接收消息队列
    storesToAdd  storesToAdd           // 待添加的 Store

    // === 双向状态 ===
    requesterStateHandler *requesterStateHandler // "我请求支持"
    supporterStateHandler *supporterStateHandler // "我提供支持"

    // === 配置与回调 ===
    options            Options
    withdrawalCallback func(map[roachpb.StoreID]struct{})
    minWithdrawalTS    hlc.Timestamp    // 最小撤回时间（保护期）
}
```

#### 1.4.2 核心状态：双向状态处理器

**Requester State（请求者状态）：**

```go
type requesterStateHandler struct {
    mu struct {
        syncutil.RWMutex
        requesterState requesterState  // COW 模式
    }
}

type requesterState struct {
    meta        requesterMeta                           // 元数据
    supportFrom map[slpb.StoreIdent]supportFromState   // 我从哪些 Store 获得支持
}

type supportFromState struct {
    Epoch      slpb.Epoch     // 支持者的 Epoch
    Expiration hlc.Timestamp  // 过期时间
}
```

**Supporter State（支持者状态）：**

```go
type supporterStateHandler struct {
    mu struct {
        syncutil.RWMutex
        supporterState supporterState  // COW 模式
    }
}

type supporterState struct {
    meta       supporterMeta                          // 元数据
    supportFor map[slpb.StoreIdent]supportForState   // 我支持哪些 Store
}

type supportForState struct {
    Epoch      slpb.Epoch     // 我给对方的 Epoch
    Expiration hlc.Timestamp  // 何时撤回支持
}
```

**关键设计决策：**

1. **双向对称**：每个 Store 同时扮演 Requester 和 Supporter 角色
2. **COW 模式**：使用 `checkOutUpdate()` / `checkInUpdate()` 实现写时复制
3. **Epoch 隔离**：每次重启递增 Epoch，旧消息自动失效
4. **持久化分离**：requesterState 和 supporterState 独立持久化

#### 1.4.3 异步通信队列

**receiveQueue（接收队列）：**

```go
type receiveQueue struct {
    mu struct {
        syncutil.Mutex
        msgs []*slpb.Message  // 接收的消息
    }
    sig chan struct{}  // 信号通道（缓冲为 1）
}
```

**storesToAdd（待添加 Store）：**

```go
type storesToAdd struct {
    mu     syncutil.Mutex
    sig    chan struct{}              // 信号通道（缓冲为 1）
    stores map[slpb.StoreIdent]struct{}  // 去重
}
```

**设计特点：**
- **有界队列**：receiveQueue 最多 10,000 条消息，防止内存爆炸
- **非阻塞信号**：使用 `select + default` 避免发送方阻塞
- **批量处理**：`Drain()` 一次性取出所有消息

#### 1.4.4 生命周期阶段

```
1. 创建阶段 (NewSupportManager)
   - 初始化所有字段
   - 创建 requesterStateHandler 和 supporterStateHandler
   - receiveQueue 和 storesToAdd 初始化为空

2. 启动阶段 (Start)
   ├─ 同步调用 onRestart()
   │  ├─ 从磁盘加载 requesterState 和 supporterState
   │  ├─ 推进时钟到 MaxWithdrawn（防止时钟回退）
   │  ├─ 等待到 MaxRequested（给其他 Store 缓冲期）
   │  ├─ 设置 minWithdrawalTS（Grace Period）
   │  └─ 递增 Epoch（隔离旧消息）
   └─ 异步启动 startLoop()

3. 运行阶段 (startLoop)
   - 监听 5 个事件源：
     a. heartbeatTicker - 定期发送心跳
     b. supportExpiryTicker - 定期检查过期支持
     c. idleSupportFromTicker - 标记空闲 Store
     d. storesToAdd.sig - 有新 Store 需要添加
     e. receiveQueue.sig - 有新消息需要处理
     f. stopper.ShouldQuiesce() - 停止信号

4. 调度优先级（关键设计）
   - 如果有心跳、过期检查、添加 Store 待处理
     → 不监听 receiveQueue（优先处理主动任务）
   - 否则才监听 receiveQueue（处理被动任务）

5. 关闭阶段
   - stopper.Stop() 触发退出
   - 所有 Ticker 停止
   - goroutine 结束
```

### 1.5 接口与契约

SupportManager 实现了 `Fabric` 接口：

```go
type Fabric interface {
    // SupportFor 返回本 Store 对指定 Store 的支持状态
    SupportFor(id slpb.StoreIdent) (slpb.Epoch, bool)

    // SupportFrom 返回本 Store 从指定 Store 获得的支持状态
    SupportFrom(id slpb.StoreIdent) (slpb.Epoch, hlc.Timestamp)

    // RegisterSupportWithdrawalCallback 注册支持撤回时的回调
    RegisterSupportWithdrawalCallback(cb func(map[roachpb.StoreID]struct{}))

    // SupportFromEnabled 返回是否启用 Store Liveness
    SupportFromEnabled(ctx context.Context) bool
}
```

同时实现了 `MessageHandler` 接口：

```go
type MessageHandler interface {
    // HandleMessage 处理接收到的消息
    HandleMessage(msg *slpb.Message) error
}
```

**接口契约：**

1. **SupportFor**：
   - 读操作，无需持锁（只读 COW 快照）
   - 返回空 Expiration 表示支持已过期
   - 幂等且无副作用

2. **SupportFrom**：
   - 首次调用会触发添加 Store（延迟初始化）
   - 可能触发从 Idle 恢复心跳
   - 线程安全（内部加锁）

3. **HandleMessage**：
   - 非阻塞，只做入队操作
   - 队列满时返回错误（丢弃消息）
   - 不保证消息顺序

---

## 二、控制流与组件协作（How it flows）

### 2.1 主要执行路径

SupportManager 的核心是 `startLoop()` 中的事件循环，它协调了 5 种不同的事件源：

```
┌─────────────────────────────────────────────────────┐
│  startLoop() 事件循环                                │
│                                                     │
│  select {                                           │
│  ├─ case <-heartbeatTicker.C:                       │
│  │     └─> sendHeartbeats()                         │
│  │                                                  │
│  ├─ case <-supportExpiryTicker.C:                   │
│  │     └─> withdrawSupport()                        │
│  │                                                  │
│  ├─ case <-idleSupportFromTicker.C:                 │
│  │     └─> markIdleStores()                         │
│  │                                                  │
│  ├─ case <-storesToAdd.sig:                         │
│  │     └─> maybeAddStores() + sendHeartbeats()      │
│  │                                                  │
│  ├─ case <-receiveQueue.sig:  (动态监听)            │
│  │     └─> handleMessages()                         │
│  │                                                  │
│  └─ case <-stopper.ShouldQuiesce():                 │
│        └─> return                                   │
│  }                                                  │
└─────────────────────────────────────────────────────┘
```

### 2.2 核心执行路径详解

#### 路径 1：启动与初始化（onRestart）

```
时间线：
T0: Store 启动，调用 SupportManager.Start()
    └─> 同步调用 onRestart()

T0+0ms: onRestart() 执行
    ├─> 步骤 1: 从磁盘加载状态
    │   ├─> supporterStateHandler.read(engine)
    │   │   └─> 读取 supporterMeta 和 supportForState
    │   └─> requesterStateHandler.read(engine)
    │       └─> 读取 requesterMeta 和 supportFromState
    │
    ├─> 步骤 2: 推进时钟到 MaxWithdrawn
    │   ├─> maxWithdrawn = supporterState.meta.MaxWithdrawn
    │   ├─> clock.UpdateAndCheckMaxOffset(maxWithdrawn)
    │   └─> 目的：防止时钟回退导致过早撤回支持
    │
    ├─> 步骤 3: 等待到 MaxRequested
    │   ├─> maxRequested = requesterState.meta.MaxRequested
    │   ├─> clock.SleepUntil(maxRequested)
    │   └─> 目的：给其他 Store 缓冲期，避免误判超时
    │
    ├─> 步骤 4: 设置最小撤回时间（Grace Period）
    │   ├─> minWithdrawalTS = clock.Now() + SupportWithdrawalGracePeriod
    │   └─> 目的：重启后不立即撤回支持，给系统稳定时间
    │
    └─> 步骤 5: 递增 Epoch
        ├─> rsfu = requesterStateHandler.checkOutUpdate()
        ├─> rsfu.incrementMaxEpoch()
        ├─> rsfu.write(engine)  // 持久化
        └─> requesterStateHandler.checkInUpdate(rsfu)

T0+100ms: onRestart() 完成，启动 startLoop() goroutine
```

**关键设计洞察：**

1. **时钟同步**：`UpdateAndCheckMaxOffset` 确保本地时钟不落后于上次撤回时间
2. **缓冲期等待**：`SleepUntil(MaxRequested)` 保证不会因重启而误判其他 Store 超时
3. **Epoch 隔离**：递增 Epoch 使旧消息自动失效，防止脑裂

#### 路径 2：发送心跳（sendHeartbeats）

```
时间线：
T1: heartbeatTicker 触发（每 HeartbeatInterval，如 4s）
    └─> sendHeartbeats() 执行

T1+0ms: 检查前置条件
    ├─> if !SupportFromEnabled(ctx): return  // 未启用
    ├─> if DisableHeartbeats: return          // 测试 knob
    └─> 继续执行

T1+1ms: 获取可变状态（COW 模式）
    ├─> rsfu = requesterStateHandler.checkOutUpdate()
    │   ├─> mu.Lock()
    │   ├─> copy = requesterState.clone()  // 深拷贝
    │   └─> mu.Unlock()
    │   └─> 返回 requesterStateForUpdate{state: copy}
    │
    └─> defer requesterStateHandler.finishUpdate(rsfu)

T1+2ms: 生成心跳消息
    ├─> livenessInterval = options.SupportDuration (如 10s)
    ├─> heartbeats = rsfu.getHeartbeatsToSend(storeID, now, livenessInterval)
    │   └─> 遍历 supportFrom map，生成心跳消息：
    │       for storeIdent, supportFrom := range rsfu.state.supportFrom {
    │           msg := slpb.Message{
    │               Type: MsgHeartbeat,
    │               From: sm.storeID,
    │               To:   storeIdent,
    │               Epoch: rsfu.state.meta.MaxEpoch,  // 本 Store 的 Epoch
    │               Expiration: now + livenessInterval,
    │           }
    │           heartbeats = append(heartbeats, msg)
    │           // 更新 MaxRequested
    │           rsfu.state.meta.MaxRequested = max(MaxRequested, msg.Expiration)
    │       }
    │
    └─> 示例：supportFrom = {StoreB, StoreC}
        → heartbeats = [
            {From: A, To: B, Epoch: 5, Expiration: T1+10s},
            {From: A, To: C, Epoch: 5, Expiration: T1+10s},
          ]

T1+3ms: 持久化状态
    ├─> rsfu.write(engine)
    │   └─> batch.Set(requesterMetaKey, meta)  // 持久化 MaxRequested
    └─> 测量持久化耗时，记录 metrics

T1+4ms: 提交状态（释放写锁）
    └─> requesterStateHandler.checkInUpdate(rsfu)
        ├─> mu.Lock()
        ├─> requesterState = rsfu.state  // 原子替换
        └─> mu.Unlock()

T1+5ms: 发送消息（不持有锁）
    ├─> for _, msg := range heartbeats {
    │       if sent := sender.EnqueueMessage(msg); sent {
    │           successes++
    │       }
    │   }
    └─> 更新 metrics：
        ├─> HeartbeatSuccesses.Inc(successes)
        └─> HeartbeatFailures.Inc(len(heartbeats) - successes)
```

**关键观察：**

1. **COW 模式**：修改在副本上进行，提交时原子替换，避免长时间持锁
2. **持久化优先**：先持久化再发送，确保重启后能恢复
3. **非阻塞发送**：发送消息不持有状态锁，提高并发性

#### 路径 3：接收心跳与响应（handleMessages）

```
时间线：
T2: 其他 Store 发送心跳到本 Store
    └─> Transport.HandleMessage(msg) 调用

T2+0ms: 入队操作
    ├─> SupportManager.HandleMessage(msg)
    │   ├─> receiveQueue.Append(msg)
    │   │   ├─> mu.Lock()
    │   │   ├─> if len(msgs) >= maxReceiveQueueSize: return error
    │   │   ├─> msgs = append(msgs, msg)
    │   │   ├─> select {
    │   │   │   case sig <- struct{}{}: // 发送信号
    │   │   │   default:                 // 信号已存在，跳过
    │   │   │   }
    │   │   └─> mu.Unlock()
    │   └─> 更新 metrics：ReceiveQueueSize.Inc(1)
    └─> 立即返回（非阻塞）

T2+10ms: startLoop() 收到 receiveQueue.sig
    ├─> msgs = receiveQueue.Drain()
    │   ├─> mu.Lock()
    │   ├─> msgs = queue.msgs
    │   ├─> queue.msgs = nil
    │   └─> mu.Unlock()
    │
    └─> handleMessages(msgs)

T2+11ms: handleMessages() 执行
    ├─> 获取双向状态
    │   ├─> rsfu = requesterStateHandler.checkOutUpdate()
    │   └─> ssfu = supporterStateHandler.checkOutUpdate()
    │
    ├─> 遍历消息
    │   for _, msg := range msgs {
    │       switch msg.Type {
    │       case MsgHeartbeat:
    │           ├─> response = ssfu.handleHeartbeat(msg)
    │           │   ├─> 检查 Epoch：
    │           │   │   if existing, ok := supportFor[msg.From]; ok {
    │           │   │       if msg.Epoch < existing.Epoch {
    │           │   │           // 旧 Epoch，忽略
    │           │   │           return emptyResponse
    │           │   │       }
    │           │   │   }
    │           │   ├─> 更新 supportFor：
    │           │   │   supportFor[msg.From] = supportForState{
    │           │   │       Epoch: msg.Epoch,
    │           │   │       Expiration: msg.Expiration,
    │           │   │   }
    │           │   ├─> 更新 MaxWithdrawn：
    │           │   │   meta.MaxWithdrawn = max(MaxWithdrawn, msg.Expiration)
    │           │   └─> 构造响应：
    │           │       response = slpb.Message{
    │           │           Type: MsgHeartbeatResp,
    │           │           From: sm.storeID,
    │           │           To:   msg.From,
    │           │           Epoch: ssfu.meta.MaxEpoch,  // 本 Store 的 Epoch
    │           │       }
    │           └─> responses = append(responses, response)
    │
    │       case MsgHeartbeatResp:
    │           └─> rsfu.handleHeartbeatResponse(msg)
    │               ├─> 检查 Epoch：
    │               │   if existing, ok := supportFrom[msg.From]; ok {
    │               │       if msg.Epoch < existing.Epoch {
    │               │           // 对方 Epoch 过旧，忽略
    │               │           return
    │               │       }
    │               │   }
    │               └─> 更新 supportFrom：
    │                   supportFrom[msg.From] = supportFromState{
    │                       Epoch: msg.Epoch,
    │                       Expiration: 根据心跳间隔计算,
    │                   }
    │       }
    │   }
    │
    ├─> 批量持久化（单次 Batch）
    │   ├─> batch = engine.NewBatch()
    │   ├─> rsfu.write(batch)
    │   ├─> ssfu.write(batch)
    │   └─> batch.Commit(sync: true)
    │
    ├─> 提交状态
    │   ├─> requesterStateHandler.checkInUpdate(rsfu)
    │   └─> supporterStateHandler.checkInUpdate(ssfu)
    │
    └─> 发送响应
        for _, response := range responses {
            sender.EnqueueMessage(response)
        }
```

**关键设计：**

1. **批量处理**：一次性处理所有消息，减少锁开销
2. **单次持久化**：使用 Batch 一次性写入双向状态
3. **Epoch 检查**：拒绝旧 Epoch 的消息，防止脑裂

#### 路径 4：撤回支持（withdrawSupport）

```
时间线：
T3: supportExpiryTicker 触发（每 SupportExpiryInterval，如 5s）
    └─> withdrawSupport() 执行

T3+0ms: 检查 Grace Period
    ├─> now = clock.NowAsClockTimestamp()
    ├─> if now < minWithdrawalTS:
    │       return  // 还在保护期内，不撤回
    └─> 继续执行

T3+1ms: 获取可变状态
    └─> ssfu = supporterStateHandler.checkOutUpdate()

T3+2ms: 检查过期支持
    ├─> withdrawnStores = ssfu.withdrawSupport(now)
    │   └─> for storeIdent, supportFor := range ssfu.state.supportFor {
    │           if supportFor.Expiration.Less(now) {
    │               // 支持已过期
    │               delete(ssfu.state.supportFor, storeIdent)
    │               withdrawnStores[storeIdent.StoreID] = struct{}{}
    │           }
    │       }
    │
    └─> 示例：
        supportFor = {
            StoreB: {Epoch: 3, Expiration: T3-1s},  // 已过期
            StoreC: {Epoch: 5, Expiration: T3+5s},  // 未过期
        }
        → withdrawnStores = {StoreB}
        → supportFor = {StoreC}

T3+3ms: 持久化（如果有撤回）
    ├─> if len(withdrawnStores) == 0: return
    ├─> batch = engine.NewBatch()
    ├─> ssfu.write(batch)
    │   └─> 删除 StoreB 的 supportFor 记录
    └─> batch.Commit(sync: true)

T3+4ms: 提交状态
    └─> supporterStateHandler.checkInUpdate(ssfu)

T3+5ms: 触发回调
    └─> if withdrawalCallback != nil {
            withdrawalCallback(withdrawnStores)
            // 回调可能触发：
            // - Allocator 重新平衡副本
            // - Lease 转移
            // - Range 合并/分裂
        }
```

**关键观察：**

1. **Grace Period 保护**：重启后不立即撤回，给系统恢复时间
2. **幂等性**：重复调用不会有副作用
3. **异步回调**：撤回操作本身不阻塞，但回调可能耗时

#### 路径 5：动态添加 Store（maybeAddStores）

```
时间线：
T4: 首次调用 SupportFrom(StoreD)
    └─> storesToAdd.addStore(StoreD)
        ├─> mu.Lock()
        ├─> stores[StoreD] = struct{}{}
        ├─> select {
        │   case sig <- struct{}{}: // 发送信号
        │   default:
        │   }
        └─> mu.Unlock()

T4+5ms: startLoop() 收到 storesToAdd.sig
    ├─> maybeAddStores()
    │   ├─> sta = storesToAdd.drainStoresToAdd()
    │   │   ├─> mu.Lock()
    │   │   ├─> s = maps.Keys(stores)
    │   │   ├─> clear(stores)  // 清空 map
    │   │   └─> mu.Unlock()
    │   │   └─> 返回 [StoreD]
    │   │
    │   └─> for _, store := range sta {
    │           if requesterStateHandler.addStore(store) {
    │               // 首次添加，初始化 supportFrom
    │               supportFrom[store] = supportFromState{
    │                   Epoch: 0,
    │                   Expiration: emptyTimestamp,
    │               }
    │               metrics.SupportFromStores.Inc(1)
    │           }
    │       }
    │
    └─> sendHeartbeats()  // 立即发送心跳到新 Store
```

**关键设计：**

1. **延迟初始化**：只在首次调用 `SupportFrom` 时添加 Store
2. **立即心跳**：添加后立即发送心跳，无需等待下次 ticker
3. **去重保证**：使用 map 自动去重

### 2.3 优先级调度机制（核心设计）

startLoop 中的动态监听逻辑是 SupportManager 的精髓：

```go
// === 关键：动态选择是否监听 receiveQueue ===
var receiveQueueSig <-chan struct{}
if len(heartbeatTicker.C) == 0 &&
    len(supportExpiryTicker.C) == 0 &&
    len(storesToAdd.sig) == 0 {
    receiveQueueSig = receiveQueue.Sig()
}

select {
case <-storesToAdd.sig:           // 优先级 1
    maybeAddStores()
    sendHeartbeats()

case <-heartbeatTicker.C:         // 优先级 2
    sendHeartbeats()

case <-supportExpiryTicker.C:     // 优先级 3
    withdrawSupport()

case <-idleSupportFromTicker.C:   // 优先级 4
    markIdleStores()

case <-receiveQueueSig:            // 优先级 5（动态）
    handleMessages()

case <-stopper.ShouldQuiesce():   // 优先级最高（退出）
    return
}
```

**为什么这样设计？**

**问题场景：**

```
假设没有动态监听：
T0: heartbeatTicker 触发，但还在 channel 里未处理
T1: 大量消息涌入 receiveQueue（如 100 条）
T2: select 随机选择 receiveQueueSig，处理 100 条消息（耗时 50ms）
T3: 处理完消息后，才处理 heartbeatTicker
    → 心跳延迟 50ms，可能导致其他 Store 误判超时！
```

**解决方案：**

```
使用动态监听：
T0: heartbeatTicker 触发，len(heartbeatTicker.C) > 0
T1: receiveQueueSig = nil（不监听接收队列）
T2: select 只能选择 heartbeatTicker
    → 优先发送心跳
T3: 心跳发送完毕，len(heartbeatTicker.C) == 0
T4: receiveQueueSig = receiveQueue.Sig()（恢复监听）
T5: 处理接收队列
```

**优先级原理：**

| 事件类型 | 优先级 | 原因 |
|---------|--------|------|
| **storesToAdd** | 最高（主动） | 新 Store 需要立即建立心跳 |
| **heartbeatTicker** | 高（主动） | 按时发送心跳是核心职责 |
| **supportExpiryTicker** | 中（主动） | 及时撤回支持释放资源 |
| **idleSupportFromTicker** | 低（主动） | 标记空闲 Store，非紧急 |
| **receiveQueueSig** | 最低（被动） | 仅在主动任务空闲时处理 |

### 2.4 与其他模块的交互

#### 2.4.1 与 Transport 的交互

```go
type MessageSender interface {
    EnqueueMessage(ctx context.Context, msg slpb.Message) (sent bool)
}
```

**发送流程：**
```
SupportManager.sendHeartbeats()
    └─> sender.EnqueueMessage(msg)
        └─> Transport.Send(msg)
            └─> gRPC 发送到远端 Store
                └─> 远端 Store 的 Transport.Receive()
                    └─> 远端 SupportManager.HandleMessage()
```

**接收流程：**
```
Transport.Receive(msg)
    └─> MessageHandler.HandleMessage(msg)
        └─> SupportManager.HandleMessage(msg)
            └─> receiveQueue.Append(msg)
                └─> 等待 startLoop() 处理
```

#### 2.4.2 与 Storage Engine 的交互

**持久化键值：**

```go
// Requester Meta Key
requesterMetaKey = []byte{localStorePrefix, ...}

// Supporter Meta Key
supporterMetaKey = []byte{localStorePrefix, ...}

// Support For State Key (per Store)
supportForStateKey(storeIdent) = []byte{localStorePrefix, storeID, nodeID, ...}
```

**写入模式：**
```go
// 单次写入
batch := engine.NewBatch()
batch.Set(requesterMetaKey, proto.Marshal(meta))
batch.Commit(sync: true)

// 批量写入（handleMessages 中）
batch := engine.NewBatch()
rsfu.write(batch)  // 写入 requester state
ssfu.write(batch)  // 写入 supporter state
batch.Commit(sync: true)  // 一次性提交
```

**为什么使用 sync: true？**
- **正确性优先**：心跳状态必须持久化，否则重启后丢失
- **一致性保证**：Epoch 递增必须持久化，否则可能脑裂

#### 2.4.3 与 Clock 的交互

**时钟用途：**

1. **生成时间戳**：
   ```go
   now := clock.Now()
   expiration := now.AddDuration(supportDuration)
   ```

2. **推进时钟**：
   ```go
   clock.UpdateAndCheckMaxOffset(maxWithdrawn)
   // 确保本地时钟不落后于上次撤回时间
   ```

3. **等待时间点**：
   ```go
   clock.SleepUntil(maxRequested)
   // 阻塞到指定时间，给其他 Store 缓冲期
   ```

**为什么使用 HLC 而非物理时钟？**

- **因果一致性**：HLC 捕获因果关系
- **时钟回退容忍**：HLC 可以处理时钟回退
- **分布式协调**：HLC 的逻辑时钟部分保证单调递增

---

## 三、关键函数与核心逻辑（How it works）

### 3.1 onRestart：启动时的状态恢复

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

    // 步骤 4: 设置最小撤回时间
    sm.minWithdrawalTS = sm.clock.Now().AddDuration(
        sm.options.SupportWithdrawalGracePeriod,
    )

    // 步骤 5: 递增 Epoch
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

**为什么需要这么复杂的初始化？**

**问题 1：时钟回退**

```
场景：
T0: Store A 上次运行时，MaxWithdrawn = T100
T1: Store A 重启，系统时钟被误调为 T50
T2: 如果不推进时钟，现在是 T50 < T100
    → 支持状态混乱（已撤回的支持看起来还未过期）

解决：
clock.UpdateAndCheckMaxOffset(T100)
→ 将本地时钟推进到至少 T100
→ 保证 now >= MaxWithdrawn
```

**问题 2：其他 Store 的超时误判**

```
场景：
T0: Store A 最后一次心跳时，MaxRequested = T100
T1: Store A 崩溃
T2: Store B 在 T100 前一直收到心跳
T3: T100 时 Store B 期待心跳，但 Store A 还未重启
T4: Store A 在 T105 重启
    如果立即发送心跳（T105），Store B 已经在 T100-T105 期间
    误判 Store A 超时并可能撤回支持

解决：
clock.SleepUntil(T100)
→ 阻塞到 T100
→ 在 T100 准时发送心跳，Store B 不会误判
```

**问题 3：Epoch 脑裂**

```
场景：
T0: Store A 正常运行，Epoch = 5
T1: Store A 崩溃，网络分区
T2: Store A 重启，如果不递增 Epoch，仍是 5
T3: 网络恢复，旧消息（Epoch = 5）和新消息（Epoch = 5）混淆
    → 脑裂！

解决：
incrementMaxEpoch()
→ Epoch 从 5 → 6
→ 旧消息（Epoch = 5）被自动忽略
```

**问题 4：重启后立即撤回支持**

```
场景：
T0: Store A 重启，立即检查过期支持
T1: 发现 Store B 的支持在 T-1 过期（因为崩溃期间未收到心跳）
T2: 立即撤回对 Store B 的支持
    → 但 Store B 可能是健康的，只是 Store A 崩溃期间未收到心跳

解决：
minWithdrawalTS = now + GracePeriod
→ 重启后 GracePeriod 内不撤回支持
→ 给其他 Store 时间重新建立心跳
```

### 3.2 sendHeartbeats：COW 模式的典型应用

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
    heartbeats := rsfu.getHeartbeatsToSend(sm.storeID, sm.clock.Now(), livenessInterval)

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

**COW 模式的实现细节：**

**checkOutUpdate（检出副本）：**

```go
func (h *requesterStateHandler) checkOutUpdate() *requesterStateForUpdate {
    h.mu.Lock()
    defer h.mu.Unlock()

    // 深拷贝当前状态
    copy := h.requesterState.clone()

    return &requesterStateForUpdate{
        state: copy,
    }
}
```

**checkInUpdate（检入副本）：**

```go
func (h *requesterStateHandler) checkInUpdate(rsfu *requesterStateForUpdate) {
    h.mu.Lock()
    defer h.mu.Unlock()

    // 原子替换
    h.requesterState = rsfu.state
}
```

**为什么使用 COW 而非直接加锁修改？**

**方案对比：**

| 维度 | 直接加锁 | COW 模式 |
|------|---------|---------|
| **持锁时间** | 长（包含磁盘 I/O 和网络） | 短（只在 clone 和替换时） |
| **读操作阻塞** | 读写互斥，读被阻塞 | 读不阻塞（读旧版本） |
| **死锁风险** | 高（持锁调用外部代码） | 低（锁粒度小） |
| **内存开销** | 低 | 中等（需要拷贝） |

**具体例子：**

```go
// 错误设计：直接加锁
func (sm *SupportManager) sendHeartbeats_Wrong() {
    sm.requesterStateHandler.mu.Lock()
    defer sm.requesterStateHandler.mu.Unlock()  // 问题：持锁时间太长！

    // 1. 生成心跳（可能耗时 1ms）
    heartbeats := generateHeartbeats()

    // 2. 持久化（可能耗时 10ms）
    write(sm.engine, state)  // 持有锁期间做磁盘 I/O！

    // 3. 发送消息（可能耗时 50ms）
    for _, msg := range heartbeats {
        sender.Send(msg)  // 持有锁期间做网络 I/O！
    }

    // 问题：整个过程持锁 60ms
    // → SupportFor() 被阻塞 60ms
    // → 严重影响读性能
}

// 正确设计：COW 模式
func (sm *SupportManager) sendHeartbeats_Correct() {
    // 1. 检出副本（持锁 < 1ms）
    rsfu := sm.requesterStateHandler.checkOutUpdate()
    defer sm.requesterStateHandler.finishUpdate(rsfu)

    // 2. 在副本上修改（不持锁）
    heartbeats := rsfu.getHeartbeatsToSend(...)

    // 3. 持久化（不持锁）
    rsfu.write(sm.engine)

    // 4. 检入副本（持锁 < 1ms）
    sm.requesterStateHandler.checkInUpdate(rsfu)

    // 5. 发送消息（不持锁）
    for _, msg := range heartbeats {
        sender.Send(msg)
    }

    // 优势：总持锁时间 < 2ms
    // → SupportFor() 几乎不阻塞
}
```

### 3.3 getHeartbeatsToSend：心跳生成逻辑

```go
func (rsfu *requesterStateForUpdate) getHeartbeatsToSend(
    from slpb.StoreIdent,
    now hlc.Timestamp,
    livenessInterval time.Duration,
) []slpb.Message {
    var heartbeats []slpb.Message
    expiration := now.AddDuration(livenessInterval)

    for storeIdent := range rsfu.state.supportFrom {
        msg := slpb.Message{
            Type:       slpb.MsgHeartbeat,
            From:       from,
            To:         storeIdent,
            Epoch:      rsfu.state.meta.MaxEpoch,
            Expiration: expiration,
        }
        heartbeats = append(heartbeats, msg)

        // 更新 MaxRequested
        if rsfu.state.meta.MaxRequested.Less(expiration) {
            rsfu.state.meta.MaxRequested = expiration
        }
    }

    return heartbeats
}
```

**为什么需要更新 MaxRequested？**

```
场景：
T0: Store A 发送心跳，Expiration = T10
    → MaxRequested = T10
    → 持久化到磁盘

T5: Store A 崩溃

T8: Store A 重启
    → 从磁盘加载 MaxRequested = T10
    → clock.SleepUntil(T10)
    → 阻塞到 T10

T10: 准时发送心跳
    → Store B 不会误判超时

如果没有 MaxRequested：
T8: Store A 重启后立即发送心跳
    → Store B 在 T5-T8 期间未收到心跳
    → 可能已经撤回支持
    → 副本放置决策受影响
```

### 3.4 handleHeartbeat：支持者响应心跳

```go
func (ssfu *supporterStateForUpdate) handleHeartbeat(
    ctx context.Context,
    msg *slpb.Message,
) slpb.Message {
    // 1. 检查 Epoch
    if existing, ok := ssfu.state.supportFor[msg.From]; ok {
        if msg.Epoch < existing.Epoch {
            // 旧 Epoch，忽略
            log.KvExec.VInfof(ctx, 2,
                "ignoring heartbeat from %+v with old epoch %d (current %d)",
                msg.From, msg.Epoch, existing.Epoch)
            return slpb.Message{}  // 空响应
        }
    }

    // 2. 更新 supportFor
    ssfu.state.supportFor[msg.From] = supportForState{
        Epoch:      msg.Epoch,
        Expiration: msg.Expiration,
    }

    // 3. 更新 MaxWithdrawn
    if ssfu.state.meta.MaxWithdrawn.Less(msg.Expiration) {
        ssfu.state.meta.MaxWithdrawn = msg.Expiration
    }

    // 4. 构造响应
    return slpb.Message{
        Type:  slpb.MsgHeartbeatResp,
        From:  ssfu.localStoreIdent,
        To:    msg.From,
        Epoch: ssfu.state.meta.MaxEpoch,
    }
}
```

**为什么需要更新 MaxWithdrawn？**

```
场景：
T0: Store A 收到 Store B 的心跳，Expiration = T20
    → MaxWithdrawn = T20
    → 持久化到磁盘

T10: Store A 崩溃

T15: Store A 重启
    → 从磁盘加载 MaxWithdrawn = T20
    → clock.UpdateAndCheckMaxOffset(T20)
    → 确保本地时钟至少是 T20

T20: 撤回对 Store B 的支持（如果未收到新心跳）

如果没有 MaxWithdrawn：
T15: Store A 重启，时钟可能是 T15
    → 立即检查过期支持
    → Store B 的 Expiration = T20 > T15
    → 看起来未过期，但实际上时钟可能回退
    → 不一致！
```

### 3.5 withdrawSupport：撤回支持的核心逻辑

```go
func (sm *SupportManager) withdrawSupport(ctx context.Context) {
    now := sm.clock.NowAsClockTimestamp()

    // 1. 检查 Grace Period
    if now.ToTimestamp().Less(sm.minWithdrawalTS) {
        return
    }

    // 2. 获取可变状态
    ssfu := sm.supporterStateHandler.checkOutUpdate()
    defer sm.supporterStateHandler.finishUpdate(ssfu)

    // 3. 撤回过期支持
    supportWithdrawnForStoreIDs := ssfu.withdrawSupport(ctx, now)
    if len(supportWithdrawnForStoreIDs) == 0 {
        return  // 无需撤回
    }

    // 4. 持久化
    batch := sm.engine.NewBatch()
    defer batch.Close()
    if err := ssfu.write(ctx, batch); err != nil {
        log.KvExec.Warningf(ctx, "failed to write supporter meta and state: %v", err)
        sm.metrics.SupportWithdrawFailures.Inc(int64(numWithdrawn))
        return
    }
    if err := batch.Commit(true /* sync */); err != nil {
        log.KvExec.Warningf(ctx, "failed to commit supporter meta and state: %v", err)
        sm.metrics.SupportWithdrawFailures.Inc(int64(numWithdrawn))
        return
    }

    // 5. 提交状态
    sm.supporterStateHandler.checkInUpdate(ssfu)

    // 6. 触发回调
    if sm.withdrawalCallback != nil {
        sm.withdrawalCallback(supportWithdrawnForStoreIDs)
    }
}
```

**withdrawSupport 的实现：**

```go
func (ssfu *supporterStateForUpdate) withdrawSupport(
    ctx context.Context,
    now hlc.ClockTimestamp,
) map[roachpb.StoreID]struct{} {
    withdrawn := make(map[roachpb.StoreID]struct{})

    for storeIdent, supportFor := range ssfu.state.supportFor {
        if supportFor.Expiration.Less(now.ToTimestamp()) {
            // 支持已过期
            delete(ssfu.state.supportFor, storeIdent)
            withdrawn[storeIdent.StoreID] = struct{}{}
            log.KvExec.Infof(ctx,
                "withdrew support for store %+v (expiration %s < now %s)",
                storeIdent, supportFor.Expiration, now)
        }
    }

    return withdrawn
}
```

**为什么使用 ClockTimestamp 而非 Timestamp？**

```go
type Timestamp struct {
    WallTime int64
    Logical  int32
}

type ClockTimestamp struct {
    WallTime int64
    Logical  int32
    Synthetic bool  // ← 关键区别
}
```

**区别：**

- **Timestamp**：可能是合成的（Synthetic），来自推进或不确定性间隔
- **ClockTimestamp**：必须是本地时钟读取，不能是合成的

**为什么撤回支持需要真实时钟？**

```
场景：
T0: Store A 从 TxnCoordSender 获得时间戳 T10 (Synthetic=true)
T1: 如果使用这个合成时间戳撤回支持
    → 但实际物理时钟可能是 T5
    → 撤回支持的时间点不准确
    → 可能过早或过晚撤回

解决：
使用 NowAsClockTimestamp()
→ 强制从本地 HLC 读取
→ Synthetic = false
→ 保证撤回时间与物理时钟一致
```

### 3.6 handleMessages：批量消息处理

```go
func (sm *SupportManager) handleMessages(ctx context.Context, msgs []*slpb.Message) {
    // 1. 获取双向状态
    rsfu := sm.requesterStateHandler.checkOutUpdate()
    defer sm.requesterStateHandler.finishUpdate(rsfu)
    ssfu := sm.supporterStateHandler.checkOutUpdate()
    defer sm.supporterStateHandler.finishUpdate(ssfu)

    // 2. 处理每条消息
    var responses []slpb.Message
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

    // 3. 批量持久化
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
    if err := batch.Commit(true /* sync */); err != nil {
        log.KvExec.Warningf(ctx, "failed to sync supporter and requester state: %v", err)
        sm.metrics.MessageHandleFailures.Inc(int64(len(msgs)))
        return
    }

    // 4. 提交状态
    sm.requesterStateHandler.checkInUpdate(rsfu)
    sm.supporterStateHandler.checkInUpdate(ssfu)

    // 5. 发送响应
    for _, response := range responses {
        _ = sm.sender.EnqueueMessage(ctx, response)
    }
}
```

**为什么使用单个 Batch？**

```
错误设计：分别持久化
for _, msg := range msgs {
    if msg.Type == MsgHeartbeat {
        ssfu.handleHeartbeat(msg)
        ssfu.write(engine)  // 每条消息都写磁盘！
    }
}
// 问题：100 条消息 = 100 次磁盘同步
// → fsync 开销巨大（每次 5-10ms）
// → 总耗时 500-1000ms

正确设计：批量持久化
for _, msg := range msgs {
    if msg.Type == MsgHeartbeat {
        ssfu.handleHeartbeat(msg)  // 只在内存修改
    }
}
batch := engine.NewBatch()
rsfu.write(batch)  // 写入 batch
ssfu.write(batch)  // 写入 batch
batch.Commit(true)  // 一次 fsync
// 优势：100 条消息 = 1 次磁盘同步
// → 总耗时 5-10ms
```

---

## 四、运行时行为与系统反馈（Runtime Behavior）

### 4.1 系统如何感知运行时信号

SupportManager 通过多个 Ticker 和信号通道感知不同类型的事件：

#### 4.1.1 周期性信号：Ticker 驱动

**1. heartbeatTicker（心跳定时器）**

```go
heartbeatTicker := sm.heartbeatTicker.NewTicker()
// 默认间隔：options.HeartbeatInterval = 4s
```

**触发行为：**
- 每 4s 触发一次
- 向所有 supportFrom 中的 Store 发送心跳
- 更新 MaxRequested 时间戳
- 持久化 requester state

**为什么是 4s？**
```
SupportDuration = 10s（支持持续时间）
HeartbeatInterval = 4s（心跳间隔）

容错能力：
- 一次心跳丢失 → 还剩 6s
- 两次心跳丢失 → 还剩 2s
- 三次心跳丢失 → 支持过期

推荐配置：HeartbeatInterval ≤ SupportDuration / 3
```

**2. supportExpiryTicker（支持过期定时器）**

```go
supportExpiryTicker := time.NewTicker(sm.options.SupportExpiryInterval)
// 默认间隔：options.SupportExpiryInterval = 5s
```

**触发行为：**
- 每 5s 检查一次 supportFor map
- 撤回所有 Expiration < now 的支持
- 持久化 supporter state
- 触发 withdrawalCallback

**为什么是 5s？**
```
SupportDuration = 10s
SupportExpiryInterval = 5s

检测延迟：
- 支持在 T0 过期
- 最晚在 T0+5s 检测到
- 检测延迟 ≤ 5s

权衡：
- 间隔太短 → CPU 开销大
- 间隔太长 → 检测延迟高
```

**3. idleSupportFromTicker（空闲 Store 定时器）**

```go
idleSupportFromTicker := time.NewTicker(sm.options.IdleSupportFromInterval)
// 默认间隔：options.IdleSupportFromInterval = 1min
```

**触发行为：**
- 每 1 分钟标记长时间未调用 SupportFrom 的 Store 为 Idle
- 标记为 Idle 后，下次 SupportFrom 调用时恢复心跳

**为什么需要 Idle 机制？**
```
场景：
T0: Store A 与 Store B 建立心跳
T1: Store B 上的副本全部迁移走
T2: Store A 不再需要 Store B 的支持
    → 但仍然每 4s 发送心跳
    → 浪费网络和 CPU

解决：
IdleSupportFromInterval = 1min
→ 如果 1 分钟内未调用 SupportFrom(B)
→ 标记为 Idle，停止发送心跳
→ 下次需要时自动恢复
```

#### 4.1.2 异步信号：Channel 驱动

**1. storesToAdd.sig（新 Store 信号）**

```go
sig: make(chan struct{}, 1)  // 缓冲为 1
```

**触发条件：**
- 首次调用 `SupportFrom(storeID)`
- Store 不在 supportFrom map 中

**触发流程：**
```
SupportFrom(StoreD)
    └─> if !exists(StoreD):
           storesToAdd.addStore(StoreD)
              └─> stores[StoreD] = struct{}{}
              └─> select {
                  case sig <- struct{}{}:
                  default:
                  }
```

**非阻塞设计：**
- 如果 sig 已满（已有信号待处理）→ default 分支，忽略
- 避免阻塞 SupportFrom 调用者

**2. receiveQueue.sig（接收队列信号）**

```go
sig: make(chan struct{}, 1)  // 缓冲为 1
```

**触发条件：**
- `HandleMessage()` 接收到新消息
- 队列从空变为非空

**信号合并：**
```go
q.mu.msgs = append(q.mu.msgs, msg)
select {
case q.sig <- struct{}{}:  // 尝试发送
default:                    // 已有信号，跳过
}
```

**为什么可以合并？**
- 信号的语义是"有消息待处理"
- 不关心有多少条消息
- `Drain()` 会一次性取出所有消息

### 4.2 信号如何影响决策

#### 4.2.1 优先级调度的动态决策

```go
// === 核心决策逻辑 ===
var receiveQueueSig <-chan struct{}
if len(heartbeatTicker.C) == 0 &&
    len(supportExpiryTicker.C) == 0 &&
    len(storesToAdd.sig) == 0 {
    receiveQueueSig = receiveQueue.Sig()
}
```

**决策矩阵：**

| 场景 | heartbeatTicker | supportExpiryTicker | storesToAdd | receiveQueueSig |
|------|-----------------|---------------------|-------------|----------------|
| 空闲 | 空 | 空 | 空 | **监听** ✓ |
| 有心跳待发 | **有** | 空 | 空 | nil（不监听）|
| 有支持待撤回 | 空 | **有** | 空 | nil（不监听）|
| 有 Store 待添加 | 空 | 空 | **有** | nil（不监听）|
| 多任务并发 | **有** | **有** | 空 | nil（不监听）|

**为什么这样设计？**

**场景 A：心跳优先**
```
T0: heartbeatTicker 触发，但还在 channel 缓冲中
T1: 100 条消息涌入 receiveQueue
T2: 如果监听 receiveQueue
    → 可能随机选择 receiveQueueSig
    → 处理 100 条消息（耗时 50ms）
    → 心跳延迟 50ms
    → 其他 Store 可能误判超时！

T2: 实际设计（不监听 receiveQueue）
    → 只能选择 heartbeatTicker
    → 优先发送心跳（耗时 5ms）
    → 然后再处理 receiveQueue
    → 心跳延迟 < 5ms
```

**场景 B：撤回支持优先**
```
T0: supportExpiryTicker 触发
T1: 如果优先处理 receiveQueue
    → 可能收到已过期 Store 的心跳
    → 响应心跳，更新 supportFor
    → 然后撤回支持
    → 资源浪费！

T1: 实际设计（优先撤回支持）
    → 先撤回过期支持
    → 后续心跳会被拒绝（Epoch 不匹配）
    → 避免无效工作
```

#### 4.2.2 Grace Period 的保护机制

```go
sm.minWithdrawalTS = sm.clock.Now().AddDuration(
    sm.options.SupportWithdrawalGracePeriod,
)

func (sm *SupportManager) withdrawSupport(ctx context.Context) {
    now := sm.clock.NowAsClockTimestamp()
    if now.ToTimestamp().Less(sm.minWithdrawalTS) {
        return  // 还在保护期，不撤回
    }
    // ...
}
```

**保护期的作用：**

**场景 1：重启后的缓冲期**
```
T0: Store A 崩溃
T1: Store A 重启（耗时 30s）
T2: 如果立即撤回支持
    → Store B 的支持在 T0+10s 过期
    → T1 时 Store A 重启，立即检查
    → 发现 Store B 过期，撤回支持
    → 但 Store B 实际上是健康的

T2: 使用 Grace Period（如 60s）
    → minWithdrawalTS = T1 + 60s
    → T1 到 T1+60s 期间不撤回支持
    → Store B 有时间重新发送心跳
    → 避免误判
```

**场景 2：网络波动的容忍**
```
T0: 网络出现短暂抖动（5s）
T5: 网络恢复
T10: Store B 的支持过期

如果没有 Grace Period：
→ T10 立即撤回支持
→ 触发副本迁移
→ 但网络已恢复，迁移是不必要的

使用 Grace Period：
→ T0 重启时设置 minWithdrawalTS = T0 + 60s
→ T10 时不撤回（还在保护期）
→ Store B 重新发送心跳
→ 避免不必要的副本迁移
```

### 4.3 为什么采用当前策略

#### 4.3.1 双向心跳 vs. 单向心跳

**当前设计：双向心跳**

```
Store A                Store B
  ├─> Heartbeat ─────────>
  <── HeartbeatResp <─────┤
```

**替代方案：单向心跳**

```
Store A                Store B
  ├─> Heartbeat ─────────>
  （无响应）
```

**对比：**

| 维度 | 双向心跳 | 单向心跳 |
|------|---------|---------|
| **网络开销** | 2x | 1x |
| **故障检测** | 双向确认 | 单向确认 |
| **一致性** | 强（双方都知道） | 弱（只有接收方知道） |
| **Epoch 同步** | 双方 Epoch 互相可见 | 只有发送方 Epoch 可见 |

**为什么选择双向？**

```
场景：Store A 认为 Store B 健康
单向心跳：
  - Store A 发送心跳到 Store B
  - Store A 不知道 Store B 是否真的收到
  - Store B 可能已崩溃，但 Store A 仍认为健康

双向心跳：
  - Store A 发送心跳到 Store B
  - Store B 响应心跳
  - Store A 收到响应，确认 Store B 健康
  - 如果未收到响应，Store A 知道 Store B 可能有问题
```

#### 4.3.2 推模式 vs. 拉模式

**当前设计：推模式（Push）**

```
定期发送心跳：
heartbeatTicker → sendHeartbeats()
```

**替代方案：拉模式（Pull）**

```
定期查询状态：
Store A → "Store B, 你健康吗？"
Store B → "我健康，这是我的状态"
```

**对比：**

| 维度 | 推模式 | 拉模式 |
|------|--------|--------|
| **实时性** | 高（主动通知） | 低（等待查询） |
| **网络开销** | 固定间隔 | 查询触发 |
| **扩展性** | 好（O(n)）| 差（O(n²)）|

**推模式的扩展性优势：**

```
n 个 Store，推模式：
  每个 Store 向 (n-1) 个 Store 发送心跳
  总心跳数 = n * (n-1) = O(n²)

  但实际上不需要全连接！
  每个 Store 只向有副本的 Store 发送心跳
  平均每个 Store 向 10-20 个 Store 发送心跳
  总心跳数 = n * 10-20 = O(n)

拉模式：
  每个 Store 需要查询所有其他 Store
  总查询数 = n * (n-1) = O(n²)
  无法优化（必须查询所有）
```

#### 4.3.3 持久化优先 vs. 内存优先

**当前设计：持久化优先**

```go
// 先持久化
rsfu.write(engine)

// 再提交状态
requesterStateHandler.checkInUpdate(rsfu)

// 最后发送消息
sender.EnqueueMessage(msg)
```

**替代方案：内存优先**

```go
// 先提交状态（内存）
requesterStateHandler.checkInUpdate(rsfu)

// 再发送消息
sender.EnqueueMessage(msg)

// 最后持久化（异步）
go rsfu.write(engine)
```

**对比：**

| 维度 | 持久化优先 | 内存优先 |
|------|-----------|---------|
| **性能** | 低（同步 fsync）| 高（异步写入）|
| **正确性** | 强（崩溃后恢复）| 弱（崩溃后丢失）|
| **一致性** | 强（持久化即生效）| 弱（内存与磁盘不一致）|

**为什么选择持久化优先？**

```
场景：Store A 发送心跳后立即崩溃
持久化优先：
  T0: rsfu.write(engine)  // MaxRequested 写入磁盘
  T1: checkInUpdate(rsfu)
  T2: sender.EnqueueMessage(msg)
  T3: 崩溃
  T4: 重启
  T5: 从磁盘加载 MaxRequested
  T6: clock.SleepUntil(MaxRequested)  // 等到承诺的时间
  T7: 发送心跳

内存优先：
  T0: checkInUpdate(rsfu)  // 只在内存
  T1: sender.EnqueueMessage(msg)  // 心跳已发送
  T2: 崩溃（持久化未完成）
  T3: 重启
  T4: 从磁盘加载旧的 MaxRequested
  T5: 比承诺的时间早发送心跳
  T6: Store B 期待心跳的时间点，Store A 未发送
  T7: Store B 误判 Store A 超时！
```

### 4.4 设计如何在目标间取得平衡

| 目标 | 实现机制 | 权衡 |
|------|---------|------|
| **可靠性** | • 持久化优先<br>• Epoch 隔离<br>• Grace Period 保护 | 牺牲性能<br>（同步 fsync）|
| **性能** | • COW 模式<br>• 批量持久化<br>• 非阻塞信号 | 增加内存<br>（副本开销）|
| **一致性** | • 双向心跳<br>• HLC 时钟<br>• MaxWithdrawn/MaxRequested | 增加网络开销<br>（双向通信）|
| **实时性** | • 优先级调度<br>• 心跳优先处理<br>• 精确 Ticker | 可能延迟被动任务<br>（receiveQueue）|
| **可扩展性** | • 推模式心跳<br>• 本地状态<br>• 无中心协调 | 需要更多持久化空间<br>（每个 Store 独立）|

**核心权衡：正确性 vs. 性能**

SupportManager 选择了**正确性优先**：

```go
// 正确性优先的证据：

// 1. 同步持久化
batch.Commit(true /* sync */)  // 强制 fsync

// 2. 双向心跳
// 牺牲网络带宽，确保双方状态一致

// 3. Grace Period
// 牺牲实时性，避免误判

// 4. Epoch 隔离
// 牺牲复杂性，防止脑裂

// 5. COW 模式
// 牺牲内存，保证读不阻塞
```

**何时可以牺牲正确性换取性能？**

如果场景满足以下条件：
- 心跳丢失可容忍
- 误判代价低
- 恢复时间短
- 无强一致性要求

则可以考虑：
- 异步持久化
- 单向心跳
- 无 Grace Period
- 无 Epoch 隔离

但 Store Liveness 是 CockroachDB 的基础设施，**必须保证正确性**。

---

## 五、设计模式分析（重点）

### 5.1 Copy-on-Write（COW）模式

**模式定义：**
在修改共享数据时，先复制一份副本进行修改，修改完成后原子替换原数据，避免长时间持锁。

**在 SupportManager 中的应用：**

```go
// === COW 模式的三个核心操作 ===

// 1. checkOutUpdate（检出副本）
func (h *requesterStateHandler) checkOutUpdate() *requesterStateForUpdate {
    h.mu.Lock()
    defer h.mu.Unlock()

    // 深拷贝
    copy := h.requesterState.clone()

    return &requesterStateForUpdate{
        state: copy,
    }
}

// 2. 在副本上修改（不持锁）
rsfu := handler.checkOutUpdate()
rsfu.state.meta.MaxRequested = newValue
rsfu.state.supportFrom[storeID] = newState

// 3. checkInUpdate（检入副本）
func (h *requesterStateHandler) checkInUpdate(rsfu *requesterStateForUpdate) {
    h.mu.Lock()
    defer h.mu.Unlock()

    // 原子替换
    h.requesterState = rsfu.state
}
```

**为什么选择这个模式？**

**1. 读写分离**

```go
// 读操作（无锁）
func (h *requesterStateHandler) getSupportFrom(id slpb.StoreIdent) (supportFromState, bool) {
    h.mu.RLock()
    defer h.mu.RUnlock()

    state, ok := h.requesterState.supportFrom[id]
    return state, ok
}
// 持锁时间：< 1μs（只读内存）

// 写操作（COW）
rsfu := h.checkOutUpdate()       // 持锁 < 100μs（拷贝）
rsfu.modify()                     // 不持锁（在副本上修改）
rsfu.write(engine)                // 不持锁（持久化，10ms）
h.checkInUpdate(rsfu)             // 持锁 < 1μs（原子替换）
// 总持锁时间：< 101μs
```

**2. 避免死锁**

```go
// 错误设计：持锁调用外部代码
func sendHeartbeats_Wrong() {
    handler.mu.Lock()
    defer handler.mu.Unlock()

    // 问题：持锁期间调用 engine.Write()
    // → engine 内部可能也有锁
    // → 如果 engine 反过来调用 SupportManager
    // → 死锁！
    handler.state.write(engine)
}

// 正确设计：COW 模式
func sendHeartbeats_Correct() {
    rsfu := handler.checkOutUpdate()  // 持锁，拷贝
    // 不持锁，调用外部代码
    rsfu.write(engine)
    handler.checkInUpdate(rsfu)       // 持锁，替换
}
```

**3. 快照一致性**

```go
// COW 保证快照一致性
rsfu := handler.checkOutUpdate()

// 在副本上的所有修改都是一致的
rsfu.updateA()
rsfu.updateB()
rsfu.updateC()

// 原子提交
handler.checkInUpdate(rsfu)

// 读操作要么看到旧版本（A、B、C 都未更新）
// 要么看到新版本（A、B、C 都已更新）
// 不会看到中间状态（只有 A、B 更新，C 未更新）
```

**这种模式在分布式系统中是否属于事实标准？**

是的，COW 在需要高并发读的系统中是事实标准：

- **Linux 内核**：fork() 使用 COW
- **Git**：每次 commit 都是 COW
- **RocksDB**：LSM-tree 的 MemTable 切换使用 COW
- **etcd**：MVCC 使用 COW

### 5.2 Producer-Consumer（生产者-消费者）模式

**模式定义：**
生产者生成数据放入队列，消费者从队列取出数据处理，通过队列解耦生产和消费。

**在 SupportManager 中的应用：**

```
生产者 (Transport)           队列               消费者 (startLoop)
────────────────────       ──────────         ─────────────────
HandleMessage(msg1) ──┐                     startLoop() goroutine
  └─> Append(msg1)    ├──> receiveQueue        ↓
                       │   [msg1,msg2,...]   Drain() → [msg1,msg2,...]
HandleMessage(msg2) ──┤                        ↓
  └─> Append(msg2)    │                     handleMessages([msg1,msg2,...])
                       │                        ↓
HandleMessage(msg3) ──┘                     processMsg1
  └─> Append(msg3)                             processMsg2
                                                processMsg3
```

**关键实现：**

```go
// 生产者：添加消息
func (q *receiveQueue) Append(msg *slpb.Message) error {
    q.mu.Lock()
    defer q.mu.Unlock()

    // 有界队列
    if len(q.mu.msgs) >= maxReceiveQueueSize {
        return receiveQueueSizeLimitReachedErr
    }

    q.mu.msgs = append(q.mu.msgs, msg)

    // 非阻塞通知
    select {
    case q.sig <- struct{}{}:
    default:
    }

    return nil
}

// 消费者：批量取出
func (q *receiveQueue) Drain() []*slpb.Message {
    q.mu.Lock()
    defer q.mu.Unlock()

    msgs := q.mu.msgs
    q.mu.msgs = nil  // 清空队列
    return msgs
}
```

**为什么选择这个模式？**

**1. 解耦生产和消费**

```
生产者（Transport）：
- 来自网络的消息
- 不可预测的速率
- 可能突发大量消息

消费者（startLoop）：
- 处理消息需要持久化（10ms/条）
- 处理速率有限
- 需要批量优化

如果直接同步处理：
HandleMessage(msg) {
    handleMessages([msg])  // 直接处理
    // 问题：
    // - 每条消息都持久化（慢）
    // - Transport 被阻塞
    // - 无法批量优化
}

使用队列解耦：
HandleMessage(msg) {
    queue.Append(msg)  // 立即返回（< 1μs）
}
// Transport 不阻塞

startLoop() {
    msgs := queue.Drain()  // 批量取出
    handleMessages(msgs)    // 批量处理
}
// 批量持久化，减少 fsync
```

**2. 削峰填谷**

```
消息到达速率：
T0-T1: 100 msg/s（突发）
T1-T2: 10 msg/s（正常）
T2-T3: 200 msg/s（突发）

队列大小变化：
T0: 队列 = 0
T0-T1: 队列 = 0 → 100（积压）
T1-T2: 队列 = 100 → 50（消化积压）
T2-T3: 队列 = 50 → 150（再次积压）

好处：
- 突发流量不丢失（缓冲）
- 消费者平稳处理（批量）
- 系统整体稳定
```

**3. 有界队列的背压**

```go
const maxReceiveQueueSize = 10_000

func (q *receiveQueue) Append(msg *slpb.Message) error {
    if len(q.mu.msgs) >= maxReceiveQueueSize {
        return receiveQueueSizeLimitReachedErr  // 拒绝新消息
    }
    // ...
}
```

**为什么需要有界？**

```
场景：磁盘 stall
T0: 磁盘出现问题，持久化变慢（100ms/条）
T1: 消息继续涌入（100 msg/s）
T2: 队列无界，持续积压
    → 1min 后队列 = 6000 条消息
    → 10min 后队列 = 60000 条消息
    → 内存爆炸！

使用有界队列：
T0: 磁盘 stall
T1: 队列达到 10000
T2: 新消息被拒绝
    → 返回错误给 Transport
    → Transport 可以选择丢弃或重试
    → 保护内存不爆炸
```

### 5.3 Dual-State Handler（双向状态处理器）模式

**模式定义：**
同一个组件同时扮演两种对称的角色，每种角色有独立的状态处理器。

**在 SupportManager 中的应用：**

```
┌─────────────────────────────────────────┐
│  SupportManager                         │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │  requesterStateHandler            │ │
│  │  "我请求别人支持我"                 │ │
│  │  ├─ supportFrom map               │ │
│  │  ├─ MaxRequested                  │ │
│  │  └─ MaxEpoch                      │ │
│  └───────────────────────────────────┘ │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │  supporterStateHandler            │ │
│  │  "我支持别人"                       │ │
│  │  ├─ supportFor map                │ │
│  │  ├─ MaxWithdrawn                  │ │
│  │  └─ MaxEpoch                      │ │
│  └───────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

**为什么选择这个模式？**

**1. 职责清晰**

```go
// 错误设计：混合状态
type SupportManager struct {
    mu struct {
        // 混在一起
        supportFrom map[StoreID]State
        supportFor  map[StoreID]State
        maxTimestamp Timestamp  // 哪个？MaxRequested 还是 MaxWithdrawn？
    }
}

// 正确设计：分离状态
type SupportManager struct {
    requesterStateHandler *requesterStateHandler  // 请求者状态
    supporterStateHandler *supporterStateHandler  // 支持者状态
}
```

**2. 独立演化**

```
Requester State 变更：
- 添加 IdleStore 标记
- 修改不影响 Supporter State

Supporter State 变更：
- 添加撤回原因
- 修改不影响 Requester State

如果混合在一起：
- 修改一个状态可能影响另一个
- 需要仔细考虑兼容性
```

**3. 对称性利用**

```go
// 对称的接口
type requesterStateHandler interface {
    checkOutUpdate() *requesterStateForUpdate
    checkInUpdate(rsfu *requesterStateForUpdate)
    read(engine) error
    write(engine) error
}

type supporterStateHandler interface {
    checkOutUpdate() *supporterStateForUpdate
    checkInUpdate(ssfu *supporterStateForUpdate)
    read(engine) error
    write(engine) error
}

// 对称的处理逻辑
func handleMessages() {
    rsfu := requesterHandler.checkOutUpdate()
    ssfu := supporterHandler.checkOutUpdate()

    // 并行处理
    for _, msg := range msgs {
        if msg.Type == MsgHeartbeat {
            ssfu.handle(msg)
        } else {
            rsfu.handle(msg)
        }
    }

    // 并行持久化
    batch := engine.NewBatch()
    rsfu.write(batch)
    ssfu.write(batch)
    batch.Commit()

    // 并行提交
    requesterHandler.checkInUpdate(rsfu)
    supporterHandler.checkInUpdate(ssfu)
}
```

### 5.4 Epoch-based Fencing（基于 Epoch 的隔离）模式

**模式定义：**
使用单调递增的 Epoch 版本号区分不同生命周期的实例，防止旧实例的消息干扰新实例。

**在 SupportManager 中的应用：**

```go
// 重启时递增 Epoch
func (sm *SupportManager) onRestart() {
    rsfu := sm.requesterStateHandler.checkOutUpdate()
    rsfu.incrementMaxEpoch()  // Epoch: 5 → 6
    rsfu.write(engine)
    sm.requesterStateHandler.checkInUpdate(rsfu)
}

// 处理消息时检查 Epoch
func (ssfu *supporterStateForUpdate) handleHeartbeat(msg *Message) Message {
    if existing, ok := ssfu.state.supportFor[msg.From]; ok {
        if msg.Epoch < existing.Epoch {
            // 旧 Epoch，忽略
            return emptyMessage
        }
    }
    // ...
}
```

**为什么选择这个模式？**

**1. 防止脑裂**

```
场景：网络分区
T0: Store A 正常运行，Epoch = 5
T1: 网络分区，Store A 与集群隔离
T2: Store A 继续发送心跳（Epoch = 5）
T3: Store A 崩溃
T4: 网络恢复
T5: Store A 重启，Epoch = 6
T6: 旧心跳（Epoch = 5）和新心跳（Epoch = 6）同时到达 Store B

不使用 Epoch：
Store B 无法区分旧心跳和新心跳
→ 可能接受旧心跳
→ 更新过期的支持状态
→ 脑裂！

使用 Epoch：
Store B 检查 Epoch
→ 拒绝旧心跳（Epoch = 5）
→ 接受新心跳（Epoch = 6）
→ 避免脑裂
```

**2. 单调性保证**

```go
// Epoch 单调递增
func (rsfu *requesterStateForUpdate) incrementMaxEpoch() {
    rsfu.state.meta.MaxEpoch++  // 5 → 6
}

// 不可能回退
// Epoch 持久化到磁盘
rsfu.write(engine)

// 重启后从磁盘加载
rsfu.read(engine)
// Epoch 仍是 6，不会回退到 5
```

**3. 简单的冲突解决**

```go
// 简单的规则：Epoch 大的胜出
if msg.Epoch < existing.Epoch {
    return  // 旧消息，丢弃
}

if msg.Epoch > existing.Epoch {
    // 新消息，替换
    supportFor[msg.From] = msg
}

// 不需要复杂的向量时钟或因果关系追踪
```

**这种模式在分布式系统中是否属于事实标准？**

是的，Epoch Fencing 是分布式系统的基础模式：

- **Raft**：Term 号就是 Epoch
- **ZooKeeper**：zxid 包含 Epoch
- **Kafka**：Controller Epoch
- **HDFS**：Generation Stamp

### 5.5 Grace Period（宽限期）模式

**模式定义：**
在执行破坏性操作前，给系统一个缓冲期，允许其他组件有时间恢复或适应。

**在 SupportManager 中的应用：**

```go
// 启动时设置宽限期
func (sm *SupportManager) onRestart() {
    sm.minWithdrawalTS = sm.clock.Now().AddDuration(
        sm.options.SupportWithdrawalGracePeriod,  // 60s
    )
}

// 撤回支持前检查宽限期
func (sm *SupportManager) withdrawSupport() {
    now := sm.clock.NowAsClockTimestamp()
    if now.ToTimestamp().Less(sm.minWithdrawalTS) {
        return  // 还在宽限期，不撤回
    }
    // ...
}
```

**为什么选择这个模式？**

**1. 容忍瞬态故障**

```
场景：短暂网络抖动
T0: 网络抖动，心跳丢失
T5: 网络恢复
T10: 支持过期

不使用 Grace Period：
T10: 立即撤回支持
   → 触发副本迁移
   → 网络已恢复，迁移是不必要的
   → 浪费资源

使用 Grace Period（60s）：
T0: 重启后设置 minWithdrawalTS = T0 + 60s
T10: 检查过期，但还在宽限期
T20: Store B 重新发送心跳
T30: 支持恢复，无需迁移
```

**2. 避免雪崩效应**

```
场景：集群重启
T0: 10 个 Store 同时重启
T10: Store A 先启动完成
T15: Store A 检查支持过期
     如果立即撤回所有支持
     → 9 个 Store 全部失去支持
     → 触发大量副本迁移
     → 网络和磁盘过载
     → 雪崩！

使用 Grace Period：
T10: Store A 启动，minWithdrawalTS = T70
T20: Store B 启动
T30: Store C 启动
...
T60: 大部分 Store 已启动
T70: Grace Period 结束
     → 大部分 Store 已重建心跳
     → 只有少数真正故障的 Store 失去支持
     → 副本迁移可控
```

**3. 调试友好**

```
重启后 60s 内：
- 支持不会被撤回
- 工程师有时间检查日志
- 可以手动干预
- 不会立即触发级联故障

60s 后：
- 系统认为已经稳定
- 开始正常的故障检测
```

**这种模式在分布式系统中是否属于事实标准？**

是的，Grace Period 在分布式系统中广泛使用：

- **Kubernetes**：Pod TerminationGracePeriodSeconds
- **Consul**：DeregisterCriticalServiceAfter
- **Cassandra**：hinted_handoff_throttle_delay

### 5.6 刻意避免的模式

#### 5.6.1 避免：Leader Election（主节点选举）

**为什么不用？**

```
Leader Election 的问题：
1. 单点瓶颈：
   - 所有心跳状态由 Leader 管理
   - Leader 崩溃 → 系统暂停
   - 需要重新选举

2. 扩展性差：
   - Leader 需要处理所有 Store 的心跳
   - n 个 Store → Leader 负载 O(n²)

3. 复杂性高：
   - 需要选举协议（Raft/Paxos）
   - 需要处理脑裂
   - 需要状态同步

SupportManager 的设计：
- 每个 Store 独立管理状态
- 无中心协调
- 无单点故障
- 线性扩展 O(n)
```

#### 5.6.2 避免：Gossip Protocol（流言协议）

**为什么不用？**

```
Gossip Protocol 的特点：
- 最终一致性（Eventual Consistency）
- 随机传播
- 收敛时间不确定

SupportManager 的需求：
- 强一致性（每个 Store 的支持状态必须准确）
- 精确时间戳（支持过期时间必须精确）
- 快速检测（心跳间隔 4s，需要快速反应）

Gossip 无法满足：
1. 收敛时间可能 > 心跳间隔
2. 无法保证精确的时间戳
3. 状态可能不一致

正确做法：
- 点对点心跳（P2P Heartbeat）
- 双向确认
- 精确时间戳
```

---

## 六、具体运行示例（必须有）

### 6.1 正常场景：三个 Store 的双向心跳

**初始状态：**
- Store A、B、C 都已启动
- 每个 Store 的 Epoch = 1
- HeartbeatInterval = 4s
- SupportDuration = 10s

**时间线：**

```
T=0s（初始化）
─────────────────────────────────────────────────────────────
[Store A]
  supportFrom = {}
  supportFor = {}

[Store B]
  supportFrom = {}
  supportFor = {}

[Store C]
  supportFrom = {}
  supportFor = {}
```

```
T=1s（Store A 需要 Store B 的支持）
─────────────────────────────────────────────────────────────
[操作] Store A 上创建了一个副本，需要 Store B 的支持
  └─> Allocator.SupportFrom(StoreB)

[内部] SupportFrom 执行
  ├─> requesterStateHandler.getSupportFrom(StoreB)
  │   └─> 返回 (epoch: 0, expiration: empty, ok: false)
  │
  ├─> 首次调用，添加到 storesToAdd
  │   └─> storesToAdd.addStore(StoreB)
  │       ├─> stores[StoreB] = struct{}{}
  │       └─> sig <- struct{}{}
  │
  └─> 返回 (0, emptyTimestamp) 给 Allocator

[状态] Store A
  supportFrom = {}  （还未添加，等待 startLoop 处理）
  supportFor = {}

T=1s+1ms: startLoop 收到 storesToAdd.sig
  ├─> maybeAddStores()
  │   ├─> drainStoresToAdd() → [StoreB]
  │   ├─> requesterStateHandler.addStore(StoreB)
  │   │   └─> supportFrom[StoreB] = {Epoch: 0, Expiration: empty}
  │   └─> log: "starting to heartbeat store StoreB"
  │
  └─> sendHeartbeats()  // 立即发送心跳

[状态] Store A
  supportFrom = {StoreB: {Epoch: 0, Expiration: empty}}
  supportFor = {}
```

```
T=1s+2ms（Store A 发送心跳到 Store B）
─────────────────────────────────────────────────────────────
[内部] sendHeartbeats 执行（Store A）
  ├─> rsfu = requesterStateHandler.checkOutUpdate()
  ├─> getHeartbeatsToSend(StoreA, T=1s, 10s)
  │   └─> 生成心跳消息：
  │       msg = {
  │           Type: MsgHeartbeat,
  │           From: StoreA,
  │           To: StoreB,
  │           Epoch: 1,  // Store A 的 Epoch
  │           Expiration: T=11s,  // T=1s + 10s
  │       }
  │   └─> 更新 MaxRequested = T=11s
  │
  ├─> rsfu.write(engine)  // 持久化 MaxRequested
  ├─> requesterStateHandler.checkInUpdate(rsfu)
  └─> sender.EnqueueMessage(msg)  // 发送心跳

[状态] Store A
  supportFrom = {StoreB: {Epoch: 0, Expiration: empty}}
  MaxRequested = T=11s
```

```
T=1s+10ms（Store B 收到心跳）
─────────────────────────────────────────────────────────────
[网络] 心跳消息到达 Store B
  └─> Transport.HandleMessage(msg)

[内部] HandleMessage 执行（Store B）
  ├─> receiveQueue.Append(msg)
  │   ├─> msgs = [{Type: MsgHeartbeat, From: StoreA, ...}]
  │   └─> sig <- struct{}{}
  └─> 立即返回（非阻塞）

T=1s+15ms: startLoop 收到 receiveQueue.sig
  ├─> msgs = receiveQueue.Drain() → [msg]
  └─> handleMessages([msg])

[内部] handleMessages 执行（Store B）
  ├─> rsfu = requesterStateHandler.checkOutUpdate()
  ├─> ssfu = supporterStateHandler.checkOutUpdate()
  │
  ├─> for msg in msgs:
  │   ├─> msg.Type == MsgHeartbeat
  │   └─> response = ssfu.handleHeartbeat(msg)
  │       ├─> 检查 Epoch：msg.Epoch (1) vs existing.Epoch (无)
  │       ├─> 更新 supportFor：
  │       │   supportFor[StoreA] = {Epoch: 1, Expiration: T=11s}
  │       ├─> 更新 MaxWithdrawn = T=11s
  │       └─> 构造响应：
  │           response = {
  │               Type: MsgHeartbeatResp,
  │               From: StoreB,
  │               To: StoreA,
  │               Epoch: 1,  // Store B 的 Epoch
  │           }
  │
  ├─> batch = engine.NewBatch()
  ├─> rsfu.write(batch)
  ├─> ssfu.write(batch)
  ├─> batch.Commit(sync: true)
  ├─> requesterStateHandler.checkInUpdate(rsfu)
  ├─> supporterStateHandler.checkInUpdate(ssfu)
  └─> sender.EnqueueMessage(response)  // 发送响应

[状态] Store B
  supportFrom = {}
  supportFor = {StoreA: {Epoch: 1, Expiration: T=11s}}
  MaxWithdrawn = T=11s
```

```
T=1s+20ms（Store A 收到心跳响应）
─────────────────────────────────────────────────────────────
[网络] 响应消息到达 Store A
  └─> Transport.HandleMessage(response)

[内部] handleMessages 执行（Store A）
  ├─> msg.Type == MsgHeartbeatResp
  └─> rsfu.handleHeartbeatResponse(msg)
      ├─> 检查 Epoch：msg.Epoch (1) vs existing.Epoch (无)
      ├─> 计算过期时间：
      │   expiration = now + SupportDuration
      │             = T=1s+20ms + 10s
      │             = T=11s+20ms
      └─> 更新 supportFrom：
          supportFrom[StoreB] = {Epoch: 1, Expiration: T=11s+20ms}

[状态] Store A
  supportFrom = {StoreB: {Epoch: 1, Expiration: T=11s+20ms}}
  supportFor = {}

[此时双向心跳建立成功]
```

```
T=5s（第二次心跳）
─────────────────────────────────────────────────────────────
[触发] heartbeatTicker 触发（T=1s + 4s）

[内部] sendHeartbeats 执行（Store A）
  └─> 生成心跳：
      msg = {
          From: StoreA,
          To: StoreB,
          Epoch: 1,
          Expiration: T=15s,  // T=5s + 10s
      }
  └─> 更新 MaxRequested = T=15s

[网络] → Store B 收到心跳
  └─> 更新 supportFor[StoreA].Expiration = T=15s
  └─> 发送响应

[网络] → Store A 收到响应
  └─> 更新 supportFrom[StoreB].Expiration = T=15s+20ms

[状态]
Store A: supportFrom = {StoreB: {Epoch: 1, Expiration: T=15s+20ms}}
Store B: supportFor = {StoreA: {Epoch: 1, Expiration: T=15s}}
```

```
T=12s（检查支持过期）
─────────────────────────────────────────────────────────────
[触发] supportExpiryTicker 触发（T=7s + 5s）

[内部] withdrawSupport 执行（Store B）
  ├─> now = T=12s
  ├─> ssfu = supporterStateHandler.checkOutUpdate()
  ├─> for store in supportFor:
  │   ├─> StoreA: Expiration = T=15s
  │   └─> T=15s > T=12s，未过期，保留
  └─> 无需撤回

[状态] Store B
  supportFor = {StoreA: {Epoch: 1, Expiration: T=15s}}  // 未变化
```

### 6.2 故障场景：Store A 崩溃与恢复

```
T=16s（Store A 崩溃）
─────────────────────────────────────────────────────────────
[事件] Store A 崩溃
  → 停止发送心跳
  → supportFrom 状态丢失（仅在内存）

[状态]
Store A: 崩溃（进程终止）
Store B: supportFor = {StoreA: {Epoch: 1, Expiration: T=19s}}
```

```
T=17s（Store B 检查过期）
─────────────────────────────────────────────────────────────
[触发] supportExpiryTicker 触发

[内部] withdrawSupport 执行（Store B）
  ├─> now = T=17s
  ├─> StoreA: Expiration = T=19s
  └─> T=19s > T=17s，未过期，保留

[状态] Store B
  supportFor = {StoreA: {Epoch: 1, Expiration: T=19s}}
```

```
T=22s（Store B 撤回支持）
─────────────────────────────────────────────────────────────
[触发] supportExpiryTicker 触发

[内部] withdrawSupport 执行（Store B）
  ├─> now = T=22s
  ├─> StoreA: Expiration = T=19s
  ├─> T=19s < T=22s，已过期！
  └─> delete(supportFor, StoreA)

[持久化]
  ├─> batch = engine.NewBatch()
  ├─> ssfu.write(batch)
  └─> batch.Commit(sync: true)

[回调]
  └─> withdrawalCallback({StoreA.StoreID})
      → Allocator 可能触发副本迁移

[状态] Store B
  supportFor = {}  // 已撤回对 Store A 的支持
  MaxWithdrawn = T=19s
```

```
T=30s（Store A 重启）
─────────────────────────────────────────────────────────────
[操作] Store A 重启

[内部] onRestart 执行（Store A）
  ├─> 步骤 1: 从磁盘加载状态
  │   ├─> requesterStateHandler.read(engine)
  │   │   └─> MaxRequested = T=15s（上次持久化的值）
  │   │   └─> supportFrom = {}（未持久化，为空）
  │   └─> supporterStateHandler.read(engine)
  │       └─> MaxWithdrawn = emptyTimestamp
  │
  ├─> 步骤 2: 推进时钟
  │   └─> clock.UpdateAndCheckMaxOffset(emptyTimestamp)
  │       → 无需推进
  │
  ├─> 步骤 3: 等待到 MaxRequested
  │   └─> clock.SleepUntil(T=15s)
  │       → T=30s > T=15s，无需等待
  │
  ├─> 步骤 4: 设置 Grace Period
  │   └─> minWithdrawalTS = T=30s + 60s = T=90s
  │
  └─> 步骤 5: 递增 Epoch
      ├─> MaxEpoch = 1 → 2
      └─> write(engine)

[状态] Store A（重启后）
  supportFrom = {}  // 需要重新建立
  Epoch = 2  // 递增了
  MaxRequested = T=15s
  minWithdrawalTS = T=90s
```

```
T=31s（重新建立心跳）
─────────────────────────────────────────────────────────────
[操作] Allocator 再次调用 SupportFrom(StoreB)

[内部] Store A
  └─> storesToAdd.addStore(StoreB)
      └─> startLoop 处理
          └─> sendHeartbeats()

[心跳] Store A → Store B
  msg = {
      From: StoreA,
      To: StoreB,
      Epoch: 2,  // 新 Epoch
      Expiration: T=41s,
  }

[内部] Store B 收到心跳
  ├─> existing = 无（已撤回）
  └─> supportFor[StoreA] = {Epoch: 2, Expiration: T=41s}

[响应] Store B → Store A
  response = {
      From: StoreB,
      To: StoreA,
      Epoch: 1,
  }

[内部] Store A 收到响应
  └─> supportFrom[StoreB] = {Epoch: 1, Expiration: T=41s+20ms}

[状态] 心跳重新建立
Store A: supportFrom = {StoreB: {Epoch: 1, Expiration: T=41s+20ms}}
Store B: supportFor = {StoreA: {Epoch: 2, Expiration: T=41s}}
```

**关键观察：**

1. **Epoch 隔离**：Store A 重启后 Epoch 从 1 → 2，旧消息自动失效
2. **Grace Period**：T=30s 到 T=90s，Store A 不会撤回支持
3. **自动恢复**：心跳自动重新建立，无需人工干预

### 6.3 边界场景：Epoch 冲突

```
T=50s（Epoch 不匹配的消息）
─────────────────────────────────────────────────────────────
[场景] Store B 收到一条延迟到达的旧心跳（网络重传）
  msg = {
      From: StoreA,
      Epoch: 1,  // 旧 Epoch（Store A 已升级到 2）
      Expiration: T=60s,
  }

[内部] handleHeartbeat 执行（Store B）
  ├─> existing = supportFor[StoreA]
  │   = {Epoch: 2, Expiration: T=45s}
  │
  ├─> 检查 Epoch：
  │   msg.Epoch (1) < existing.Epoch (2)
  │
  └─> 拒绝消息！
      log: "ignoring heartbeat from StoreA with old epoch 1 (current 2)"
      return emptyMessage  // 不发送响应

[状态] Store B
  supportFor = {StoreA: {Epoch: 2, Expiration: T=45s}}  // 未变化

[关键] 旧消息被自动过滤，不影响系统
```

### 6.4 压力场景：100 个 Store 同时心跳

```
T=0s（初始化）
─────────────────────────────────────────────────────────────
[配置] 集群规模
  - 100 个 Store
  - 每个 Store 与其他 20 个 Store 建立心跳
  - HeartbeatInterval = 4s

[计算] 每个 Store 的负载
  - 发送心跳：20 条/4s = 5 条/s
  - 接收心跳：平均 20 条/4s = 5 条/s
  - 总消息：10 条/s
```

```
T=4s（第一次心跳）
─────────────────────────────────────────────────────────────
[所有 Store 同时触发 heartbeatTicker]

[分析] Store 1 的处理
  ├─> sendHeartbeats()
  │   ├─> 生成 20 条心跳消息
  │   ├─> 持久化 MaxRequested（耗时 5ms）
  │   └─> 发送消息（耗时 2ms）
  │   └─> 总耗时：7ms
  │
  └─> receiveQueue 收到约 20 条心跳
      └─> handleMessages(20 条)
          ├─> 批量处理所有消息
          ├─> 批量持久化（1 次 fsync，耗时 10ms）
          └─> 发送 20 条响应（耗时 2ms）
          └─> 总耗时：12ms

[状态] Store 1
  - CPU：7ms + 12ms = 19ms / 4s = 0.5%
  - 磁盘：2 次 fsync / 4s = 0.5 次/s
  - 网络：40 条消息 / 4s = 10 条/s

[扩展性分析]
  每增加 1 个 Store：
  - 如果需要心跳：+1 条发送 + 1 条接收 = +2 条/s
  - 持久化开销不变（批量处理）
  - 线性扩展 O(n)
```

```
T=100s（稳态运行）
─────────────────────────────────────────────────────────────
[统计] Store 1 的累计指标
  - 发送心跳：20 条 × 25 次 = 500 条
  - 接收心跳：20 条 × 25 次 = 500 条
  - 持久化：50 次（发送 25 次 + 接收 25 次）
  - CPU 使用率：< 1%
  - 磁盘 IOPS：< 1/s
  - 内存占用：< 1MB（supportFrom + supportFor）

[关键观察]
  - 批量持久化大幅降低磁盘压力
  - COW 模式保证读不阻塞
  - 资源占用与 Store 数量线性相关
```

---

## 七、设计权衡与替代方案（Design Tradeoffs and Alternatives）

### 7.1 核心权衡决策

#### 7.1.1 权衡 1：双向心跳 vs. 单向心跳

**当前设计：双向心跳**

```
Store A ─────> Heartbeat ─────> Store B
Store A <──── HeartbeatResp <── Store B
```

**替代方案 A：单向心跳**

```
Store A ─────> Heartbeat ─────> Store B
（无响应）
```

**对比分析：**

| 维度 | 双向心跳 | 单向心跳 |
|------|---------|---------|
| **网络开销** | 2x（请求+响应）| 1x（只有请求）|
| **一致性保证** | 强（双方确认）| 弱（单方确认）|
| **Epoch 同步** | 双向可见 | 单向可见 |
| **故障检测** | 双向检测 | 单向检测 |
| **复杂度** | 高 | 低 |

**选择双向心跳的原因：**

1. **双向确认的必要性**
   ```
   场景：Store A 需要确认 Store B 确实收到心跳
   单向心跳：
     Store A 发送心跳
     → 不知道 Store B 是否收到
     → 不知道 Store B 是否支持自己
     → 无法做出可靠决策

   双向心跳：
     Store A 发送心跳
     Store B 响应心跳
     → Store A 确认 Store B 收到并支持自己
     → 可以做出可靠决策
   ```

2. **Epoch 双向同步**
   ```
   单向心跳：
     Store A 知道自己的 Epoch
     Store A 不知道 Store B 的 Epoch
     → 无法检测 Store B 是否重启

   双向心跳：
     响应中包含 Store B 的 Epoch
     → Store A 可以检测 Store B 重启
     → 及时更新状态
   ```

3. **网络开销可接受**
   ```
   网络开销：
   - 心跳消息：~100 字节
   - 响应消息：~50 字节
   - 总：~150 字节/心跳

   频率：
   - 4s 一次 × 20 个 Store = 5 条/s
   - 总带宽：150 字节 × 5 = 750 字节/s
   - 可忽略不计（相比 GB/s 的复制流量）
   ```

**何时应该选择单向心跳？**
- 只需检测可达性，不需要确认
- 网络带宽极度受限
- 一致性要求低

#### 7.1.2 权衡 2：COW 模式 vs. 直接加锁

**当前设计：COW 模式**

```go
rsfu := handler.checkOutUpdate()  // 拷贝副本
rsfu.modify()                      // 修改副本（不持锁）
rsfu.write(engine)                 // 持久化（不持锁）
handler.checkInUpdate(rsfu)        // 原子替换
```

**替代方案 B：直接加锁**

```go
handler.mu.Lock()
defer handler.mu.Unlock()

handler.state.modify()             // 直接修改
handler.state.write(engine)        // 持有锁写磁盘
```

**对比分析：**

| 维度 | COW 模式 | 直接加锁 |
|------|---------|---------|
| **持锁时间** | 短（< 1ms）| 长（10-50ms）|
| **读操作阻塞** | 不阻塞 | 阻塞 |
| **内存开销** | 高（拷贝副本）| 低（无拷贝）|
| **死锁风险** | 低 | 高（持锁调用外部）|
| **实现复杂度** | 高 | 低 |

**选择 COW 的原因：**

1. **读操作是热路径**
   ```go
   // SupportFor 被频繁调用（每次副本操作都会调用）
   func (sm *SupportManager) SupportFor(id StoreIdent) (Epoch, bool) {
       ss := sm.supporterStateHandler.getSupportFor(id)
       return ss.Epoch, !ss.Expiration.IsEmpty()
   }

   调用频率：
   - 每次 Replica 操作都调用
   - 每秒数千次
   - 必须极快（< 1μs）

   直接加锁：
   - 如果持锁写磁盘（10ms）
   - SupportFor 被阻塞 10ms
   - 严重影响性能！

   COW 模式：
   - 读操作只持读锁（< 1μs）
   - 不受写操作影响
   - 性能稳定
   ```

2. **避免死锁**
   ```
   直接加锁的死锁风险：
   Thread 1:
     handler.mu.Lock()
     engine.Write() → 内部可能持锁
       → 如果 engine 回调 handler
       → 死锁！

   COW 模式：
   Thread 1:
     rsfu = handler.checkOutUpdate()  // 持锁，拷贝
     rsfu.write(engine)  // 不持锁，调用外部
       → 无死锁风险
   ```

3. **快照一致性**
   ```
   场景：同时修改多个字段
   直接加锁：
     handler.mu.Lock()
     handler.state.MaxRequested = T10
     handler.state.write(engine)  // 持久化 MaxRequested
     handler.mu.Unlock()

     handler.mu.Lock()
     handler.state.supportFrom[S1] = ...
     handler.state.write(engine)  // 持久化 supportFrom
     handler.mu.Unlock()

     问题：两次持久化之间，状态可能不一致

   COW 模式：
     rsfu = handler.checkOutUpdate()
     rsfu.state.MaxRequested = T10
     rsfu.state.supportFrom[S1] = ...
     rsfu.write(engine)  // 一次性持久化所有修改
     handler.checkInUpdate(rsfu)

     优势：原子更新，快照一致
   ```

**何时应该选择直接加锁？**
- 读操作不频繁
- 内存受限（无法拷贝）
- 修改极少（持锁时间短）

#### 7.1.3 权衡 3：持久化优先 vs. 内存优先

**当前设计：持久化优先**

```go
rsfu.write(engine)                 // 先持久化
requesterStateHandler.checkInUpdate(rsfu)  // 再更新内存
sender.EnqueueMessage(msg)         // 最后发送消息
```

**替代方案 C：内存优先**

```go
requesterStateHandler.checkInUpdate(rsfu)  // 先更新内存
sender.EnqueueMessage(msg)                 // 发送消息
go rsfu.write(engine)                      // 异步持久化
```

**对比分析：**

| 维度 | 持久化优先 | 内存优先 |
|------|-----------|---------|
| **性能** | 低（同步 fsync）| 高（异步写入）|
| **正确性** | 强（崩溃可恢复）| 弱（崩溃丢失）|
| **延迟** | 高（10ms/写入）| 低（< 1ms）|
| **复杂度** | 低（顺序执行）| 高（异步处理错误）|

**选择持久化优先的原因：**

1. **正确性是核心需求**
   ```
   场景：发送心跳后立即崩溃
   持久化优先：
     T0: rsfu.write(engine)  // MaxRequested = T10 写入磁盘
     T1: checkInUpdate(rsfu)
     T2: sender.EnqueueMessage(msg)  // 心跳发送
     T3: 崩溃
     T4: 重启
     T5: 从磁盘加载 MaxRequested = T10
     T6: clock.SleepUntil(T10)
     T7: 准时发送心跳

   内存优先：
     T0: checkInUpdate(rsfu)  // MaxRequested = T10 只在内存
     T1: sender.EnqueueMessage(msg)  // 心跳发送
     T2: 崩溃（持久化未完成）
     T3: 重启
     T4: 从磁盘加载旧 MaxRequested = T5
     T5: 提前发送心跳
     T6: T10 时未发送心跳
     T7: Store B 误判超时！
   ```

2. **Epoch 必须持久化**
   ```
   场景：Epoch 递增但未持久化
   T0: Epoch 递增：1 → 2（只在内存）
   T1: 发送心跳（Epoch = 2）
   T2: 崩溃（Epoch = 2 未持久化）
   T3: 重启
   T4: 从磁盘加载 Epoch = 1
   T5: 发送心跳（Epoch = 1）
   T6: Store B 拒绝（已收到 Epoch = 2）
   T7: 脑裂！
   ```

3. **Grace Period 必须持久化**
   ```
   场景：minWithdrawalTS 未持久化
   T0: 重启，minWithdrawalTS = T60（只在内存）
   T1: 崩溃
   T2: 重启
   T3: minWithdrawalTS 重新计算 = T62
   T4: 但实际应该是 T60
   T5: 提前撤回支持！
   ```

**何时可以选择内存优先？**
- 可以容忍数据丢失
- 性能是首要目标
- 有其他恢复机制

#### 7.1.4 权衡 4：批量持久化 vs. 逐条持久化

**当前设计：批量持久化**

```go
batch := engine.NewBatch()
rsfu.write(batch)  // 写入 batch
ssfu.write(batch)  // 写入 batch
batch.Commit(true) // 一次 fsync
```

**替代方案 D：逐条持久化**

```go
rsfu.write(engine)  // 第一次 fsync
ssfu.write(engine)  // 第二次 fsync
```

**对比分析：**

| 维度 | 批量持久化 | 逐条持久化 |
|------|-----------|-----------|
| **fsync 次数** | 1 次 | 2 次 |
| **延迟** | 10ms | 20ms |
| **吞吐量** | 高（100 msg/s）| 低（50 msg/s）|
| **实现复杂度** | 高（需要 Batch）| 低（直接写入）|

**选择批量持久化的原因：**

1. **fsync 是瓶颈**
   ```
   磁盘性能：
   - 随机写入：10000 IOPS
   - fsync：100 次/s（受限于磁盘旋转）

   逐条持久化：
   - 100 条消息 = 200 次 fsync（requester + supporter）
   - 超过 fsync 上限！
   - 延迟爆炸

   批量持久化：
   - 100 条消息 = 1 次 fsync
   - 远低于上限
   - 延迟可控
   ```

2. **原子性保证**
   ```
   场景：同时更新 requester 和 supporter 状态
   逐条持久化：
     rsfu.write(engine)  // 成功
     ssfu.write(engine)  // 崩溃！
     → requester 状态已更新
     → supporter 状态未更新
     → 不一致！

   批量持久化：
     batch.write(rsfu)
     batch.write(ssfu)
     batch.Commit()  // 原子提交
     → 要么都成功，要么都失败
     → 保证一致性
   ```

**何时应该逐条持久化？**
- 消息量极少
- 单次更新即可
- 无需原子性

### 7.2 未选择的替代方案

#### 7.2.1 方案 E：集中式心跳管理

**设计：**

```
┌─────────────────────────────────┐
│  CentralHeartbeatManager        │
│  (单例，所有 Store 共享)         │
│  ├─ 管理所有 Store 的心跳状态    │
│  └─ 定期检查过期                 │
└─────────────────────────────────┘
           ↓ ↑
    所有 Store 都依赖
```

**为什么没选择？**

1. **单点瓶颈**
   ```
   100 个 Store：
   - 每个 Store 20 条心跳/4s = 5 条/s
   - 总：500 条/s
   - CentralManager 需要处理所有心跳
   - 锁竞争严重
   ```

2. **故障传播**
   ```
   CentralManager 崩溃：
   → 所有 Store 的心跳停止
   → 整个集群不可用
   ```

3. **不符合 CockroachDB 架构**
   ```
   CockroachDB 哲学：
   - 无中心节点
   - 对等架构
   - 故障隔离

   CentralManager：
   - 有中心节点
   - 非对等
   - 故障全局影响
   ```

#### 7.2.2 方案 F：基于 Gossip 的心跳

**设计：**

```
Store A ──> Gossip ──> Store B ──> Gossip ──> Store C
  ↑                                            ↓
  └──────────────── Gossip ────────────────────┘
```

**为什么没选择？**

1. **最终一致性不满足需求**
   ```
   Gossip 特性：
   - 最终一致性
   - 收敛时间不确定（可能几秒到几分钟）

   Store Liveness 需求：
   - 强一致性（必须准确知道支持状态）
   - 实时性（4s 心跳间隔）

   不匹配！
   ```

2. **无法保证精确时间戳**
   ```
   Gossip：
   - 消息多跳传播
   - 时间戳可能过期

   Store Liveness：
   - 需要精确的过期时间
   - 误差 > 1s 可能导致误判
   ```

3. **网络开销更大**
   ```
   点对点心跳：
   - 只发送给需要的 Store
   - O(n) 消息（n = Store 数量）

   Gossip：
   - 发送给随机 Store
   - 多跳传播
   - O(n log n) 消息
   ```

#### 7.2.3 方案 G：基于 Raft 的心跳

**设计：**

```
所有 Store 的心跳状态存储在 Raft Group 中
```

**为什么没选择？**

1. **Raft 写入延迟高**
   ```
   Raft 写入：
   - Leader 写入日志
   - 复制到 Majority
   - 提交
   - 应用到状态机
   - 总延迟：10-50ms

   心跳要求：
   - 4s 间隔
   - 需要快速响应
   - Raft 延迟太高
   ```

2. **Raft 写入开销大**
   ```
   100 个 Store：
   - 每个 Store 20 条心跳/4s
   - 总：500 条/s
   - 所有心跳都通过 Raft
   - Raft 日志爆炸
   ```

3. **依赖关系复杂**
   ```
   Store Liveness 是基础设施
   - 应该独立于 Raft
   - 不应该依赖 Raft（可能依赖 Store Liveness）
   ```

### 7.3 设计决策总结

| 决策点 | 选择 | 放弃方案 | 核心原因 |
|--------|------|---------|---------|
| 心跳方向 | 双向心跳 | 单向心跳 | 双向确认，Epoch 同步 |
| 并发模型 | COW 模式 | 直接加锁 | 读不阻塞，避免死锁 |
| 持久化时机 | 持久化优先 | 内存优先 | 正确性优先，崩溃可恢复 |
| 持久化粒度 | 批量持久化 | 逐条持久化 | 减少 fsync，原子更新 |
| 架构 | 本地自治 | 集中管理 | 无单点，故障隔离 |
| 一致性 | 强一致 | Gossip | 精确时间戳，实时检测 |
| 存储 | 本地磁盘 | Raft | 低延迟，独立性 |

**设计哲学：**

SupportManager 的设计哲学是**正确性优先，性能适当，简单可靠**：

1. **正确性优先**：
   - 持久化优先
   - 双向确认
   - Epoch 隔离
   - Grace Period

2. **性能适当**：
   - COW 模式
   - 批量持久化
   - 非阻塞信号

3. **简单可靠**：
   - 无中心节点
   - 对称设计
   - 本地状态

---

## 八、总结与心智模型（Summary and Mental Model）

### 8.1 核心概念总结

SupportManager 是一个**基于 COW 模式的双向 Store 心跳与状态管理系统**，它的本质可以用一句话概括：

> **每个 Store 通过双向心跳机制向其他 Store 请求支持，同时响应其他 Store 的请求，使用 COW 模式管理状态，持久化保证正确性，Epoch 隔离防止脑裂，Grace Period 避免误判。**

**五个核心抽象：**

1. **双向状态处理器（Dual-State Handler）**
   - requesterStateHandler：管理"我从谁获得支持"
   - supporterStateHandler：管理"我支持谁"
   - 对称设计，独立演化

2. **COW 模式（Copy-on-Write）**
   - checkOutUpdate：拷贝副本
   - 修改副本（不持锁）
   - checkInUpdate：原子替换
   - 读不阻塞，快照一致

3. **Epoch 隔离（Epoch-based Fencing）**
   - 每次重启递增 Epoch
   - 旧消息自动过滤
   - 防止脑裂

4. **Grace Period（宽限期）**
   - 重启后不立即撤回支持
   - 给系统恢复时间
   - 避免误判

5. **优先级调度（Priority Scheduling）**
   - 主动任务优先（心跳、撤回）
   - 被动任务延后（接收队列）
   - 动态监听机制

### 8.2 心智模型：双向信任关系

想象一个商业信用系统，可以帮助理解 SupportManager：

```
┌─────────────────────────────────────────────────────────┐
│  商业信用系统 (Business Credit System)                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  公司 A (Store A)                                       │
│  ├─ 请求信用 (Requester)                                │
│  │   └─ 向公司 B 请求信用额度（发送心跳）                │
│  │       └─ 定期更新信用请求（4s 一次）                   │
│  │       └─ 记录最晚承诺时间 (MaxRequested)              │
│  │                                                      │
│  └─ 提供信用 (Supporter)                                │
│      └─ 响应公司 X 的信用请求（响应心跳）                │
│          └─ 定期检查信用是否过期（5s 一次）              │
│          └─> 过期则撤回信用 (withdrawSupport)           │
│          └─> 记录最晚撤回时间 (MaxWithdrawn)             │
│                                                         │
│  重启 = 破产重组                                         │
│  ├─ 信用等级递增 (Epoch++)                              │
│  ├─ 旧信用关系失效（旧 Epoch 拒绝）                      │
│  └─ 60s 宽限期（不撤回他人信用）                         │
└─────────────────────────────────────────────────────────┘
```

**类比映射：**

| 商业概念 | SupportManager | 说明 |
|---------|----------------|------|
| 请求信用 | 发送心跳 | 向其他公司请求信用额度 |
| 响应信用 | 心跳响应 | 确认提供信用 |
| 信用过期 | Expiration | 未续约则信用失效 |
| 撤回信用 | withdrawSupport | 收回提供的信用 |
| 破产重组 | 重启 + Epoch++ | 重新开始，旧关系失效 |
| 承诺时间 | MaxRequested | 承诺提供服务到的时间 |
| 撤回时间 | MaxWithdrawn | 可能撤回信用的最晚时间 |
| 宽限期 | Grace Period | 破产后不立即撤回他人信用 |

**关键洞察：**

1. **双向关系**：公司 A 向 B 请求信用，同时也可能向 X 提供信用
2. **定期更新**：信用需要定期续约（心跳），否则失效
3. **破产保护**：破产后有宽限期，不立即撤回他人信用（避免连锁反应）
4. **信用等级**：每次破产重组，信用等级递增（Epoch），旧等级的信用关系失效

### 8.3 关键不变量（Invariants）

理解这些不变量可以快速推理系统行为：

#### 不变量 1：Epoch 单调递增

```go
// 任何时刻，Epoch 只能增加，不能减少
newEpoch >= oldEpoch

// 重启后 Epoch 必定递增
onRestart():
    oldEpoch = loadFromDisk()
    newEpoch = oldEpoch + 1
    saveToDisk(newEpoch)
```

**推论：**
- 旧 Epoch 的消息永远被拒绝
- 防止脑裂

#### 不变量 2：持久化先于生效

```go
// 任何状态更新，必须先持久化再生效
write(engine, state)  // 先持久化
checkInUpdate(state)  // 再生效

// 不允许：
checkInUpdate(state)  // 先生效
write(engine, state)  // 后持久化（可能丢失）
```

**推论：**
- 崩溃后可恢复
- 承诺的时间戳不会丢失

#### 不变量 3：MaxRequested 单调递增

```go
// MaxRequested 只能增加
newMaxRequested >= oldMaxRequested

// 每次心跳更新
heartbeat.Expiration > oldMaxRequested
→ newMaxRequested = heartbeat.Expiration
```

**推论：**
- 重启后等待到 MaxRequested
- 不会提前发送心跳

#### 不变量 4：Grace Period 保护撤回

```go
// Grace Period 期间不撤回支持
if now < minWithdrawalTS:
    return  // 不撤回

// 只有过了 Grace Period 才撤回
if now >= minWithdrawalTS && expiration < now:
    withdrawSupport()
```

**推论：**
- 重启后有缓冲期
- 避免立即撤回

#### 不变量 5：COW 原子替换

```go
// COW 模式保证原子更新
oldState = currentState
newState = oldState.clone()
newState.modify()
currentState = newState  // 原子替换

// 读操作只看到完整的旧版本或新版本
// 不会看到中间状态
```

**推论：**
- 快照一致性
- 读不阻塞

### 8.4 故障模式与容错

#### 故障模式 1：Store 崩溃

**现象：**Store A 崩溃，停止发送心跳

**检测：**
- Store B 在 `Expiration` 时间后检测到支持过期
- 通过 `supportExpiryTicker` 定期检查

**恢复：**
- Store B 撤回对 Store A 的支持
- 触发 `withdrawalCallback`
- Allocator 可能迁移副本

**容错：**
- SupportDuration = 10s（支持持续时间）
- 可容忍 2 次心跳丢失（4s × 2 = 8s < 10s）

#### 故障模式 2：网络分区

**现象：**Store A 与 Store B 网络分区

**检测：**
- Store A 发送心跳，无响应
- Store B 未收到心跳，支持过期

**恢复：**
- 网络恢复后，心跳自动重新建立
- Epoch 可能已递增（如果重启）

**容错：**
- Grace Period = 60s
- 短暂分区（< 60s）不触发撤回

#### 故障模式 3：时钟回退

**现象：**系统时钟被误调到过去

**检测：**
- `clock.UpdateAndCheckMaxOffset` 检测时钟回退
- 如果回退 > `maxOffset`，返回错误

**恢复：**
- 推进本地时钟到 `MaxWithdrawn`
- 保证时钟不落后于上次撤回时间

**容错：**
- HLC 的逻辑时钟部分保证单调性
- 即使物理时钟回退，逻辑时钟仍递增

### 8.5 快速诊断检查清单

当 SupportManager 行为异常时，按此清单排查：

**1. 心跳未发送？**
```go
// 检查点 1：是否启用？
if !SupportFromEnabled(ctx) {
    // Store Liveness 未启用
}

// 检查点 2：是否在 supportFrom 中？
supportFrom := requesterStateHandler.getSupportFrom(storeID)
if !ok {
    // Store 未添加到 supportFrom
}

// 检查点 3：持久化是否成功？
// 检查日志：failed to write requester meta
```

**2. 支持未建立？**
```go
// 检查点 1：心跳是否到达？
// 检查 receiveQueue 大小
if receiveQueueSize > maxReceiveQueueSize {
    // 队列满，消息被丢弃
}

// 检查点 2：Epoch 是否匹配？
if msg.Epoch < existing.Epoch {
    // 旧 Epoch，被拒绝
}

// 检查点 3：持久化是否成功？
// 检查日志：failed to write supporter meta
```

**3. 支持被误撤回？**
```go
// 检查点 1：是否在 Grace Period？
if now < minWithdrawalTS {
    // 应该不撤回，但撤回了 → Bug
}

// 检查点 2：Expiration 是否正确？
if expiration < now {
    // 确实过期，正常撤回
}

// 检查点 3：时钟是否准确？
realNow := time.Now()
clockNow := clock.Now()
if realNow.Sub(clockNow) > time.Second {
    // 时钟漂移
}
```

### 8.6 与其他系统的类比

理解 SupportManager 在更广泛的系统中的位置：

**1. 在 CockroachDB 中的位置**

```
┌─────────────────────────────────────────────────────┐
│  SQL Layer                                          │
│  (用户查询)                                          │
└─────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────┐
│  KV Layer                                           │
│  ├─ Allocator (副本放置)                            │
│  │   └─ 查询 SupportFor/SupportFrom                 │
│  ├─ Replica (副本操作)                               │
│  │   └─ 检查 Store Liveness                         │
│  └─ Store Liveness  ← 我们在这里                    │
│      └─ SupportManager                              │
│          ├─ requesterStateHandler                   │
│          └─ supporterStateHandler                   │
└─────────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────┐
│  Storage Layer                                      │
│  (Pebble/RocksDB)                                    │
└─────────────────────────────────────────────────────┘
```

**职责分离：**
- Allocator：决策（副本应该放在哪）
- SupportManager：检测（Store 是否存活）
- Storage：持久化（状态保存）

**2. 与 Node Liveness 的对比**

```
┌─────────────────────────────────────────────────────┐
│  Node Liveness (节点级)                              │
│  ├─ 粒度：节点                                       │
│  ├─ 机制：Raft + Lease                               │
│  ├─ 用途：节点是否参与共识                            │
│  └─ 一致性：强一致（通过 Raft）                       │
└─────────────────────────────────────────────────────┘
                      ↓ 补充
┌─────────────────────────────────────────────────────┐
│  Store Liveness (Store 级)  ← SupportManager        │
│  ├─ 粒度：Store                                      │
│  ├─ 机制：点对点心跳                                  │
│  ├─ 用途：Store 是否健康                             │
│  └─ 一致性：最终一致（心跳延迟）                       │
└─────────────────────────────────────────────────────┘
```

**为什么需要两层？**
- Node Liveness：保证节点参与共识，粗粒度
- Store Liveness：保证 Store 健康，细粒度
- 互补而非替代

### 8.7 一句话总结

如果要用一句话向新人解释 SupportManager：

> **SupportManager 就像一个商业信用管理系统，每个 Store 通过双向心跳建立信用关系，使用 COW 模式安全管理状态，持久化保证承诺不丢失，Epoch 递增防止旧关系干扰，Grace Period 避免破产连锁反应。**

**关键词：**
- 双向：请求支持 + 提供支持
- COW：读不阻塞，快照一致
- 持久化：承诺不丢失
- Epoch：隔离旧实例
- Grace Period：避免误判

### 8.8 进阶学习路径

想要深入理解 SupportManager，建议按以下顺序学习：

**Level 1：基础概念**
1. 双向心跳机制
2. COW 模式的原理
3. HLC 时钟的工作原理

**Level 2：设计模式**
1. Dual-State Handler 模式
2. Epoch-based Fencing 模式
3. Grace Period 模式

**Level 3：系统设计**
1. Store Liveness 的设计权衡
2. 与 Node Liveness 的关系
3. 在 Allocator 中的应用

**Level 4：对比学习**
1. Cassandra 的 Gossip
2. etcd 的 Lease
3. Consul 的 Health Check

**Level 5：实践应用**
1. 阅读测试代码
2. 分析 metrics
3. 排查实际问题

### 8.9 常见误解澄清

**误解 1："双向心跳浪费网络"**
❌ 双向心跳的网络开销可忽略（150 字节/心跳），但提供了强一致性保证

**误解 2："COW 模式浪费内存"**
❌ COW 的拷贝开销远小于长时间持锁的性能损失

**误解 3："持久化优先降低性能"**
❌ 正确性是首要目标，性能可以通过批量持久化优化

**误解 4："Grace Period 太长（60s）"**
❌ Grace Period 是为了避免误判，60s 是经过权衡的合理值

**误解 5："Epoch 递增会导致编号溢出"**
❌ Epoch 是 int64，即使每秒递增一次，也需要 2.9 亿年才会溢出

### 8.10 最后的建议

**阅读代码时：**
- 先理解双向状态处理器的对称性
- 关注 COW 模式的三个步骤
- 思考为什么需要 Epoch 和 Grace Period

**调试问题时：**
- 先检查 Epoch 是否匹配
- 验证持久化是否成功
- 对比 MaxRequested 和 MaxWithdrawn

**改进设计时：**
- 先理解现有权衡
- 考虑正确性优先原则
- 评估改动的影响范围

---

**文档完结**

本文档全面剖析了 SupportManager 的设计与实现，涵盖了从设计动机到工程权衡的方方面面。希望读者能够：

1. ✅ 理解为什么需要 Store Liveness
2. ✅ 掌握双向心跳和 COW 模式
3. ✅ 理解 Epoch 隔离和 Grace Period 的作用
4. ✅ 能够独立调试和优化 Store Liveness 相关问题
5. ✅ 在设计类似系统时做出正确的权衡决策

如有疑问，建议结合 Store Liveness 的实际运行 metrics 和日志进行分析验证。
