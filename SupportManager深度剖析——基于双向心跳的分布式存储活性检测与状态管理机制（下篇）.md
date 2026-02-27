# SupportManager 深度剖析——基于双向心跳的分布式存储活性检测与状态管理机制（下篇）

## 六、运行时行为与系统反馈（Runtime Behavior）

### 6.1 系统如何感知运行时信号

SupportManager 的设计精髓在于它如何**感知和响应**分布式环境中的各种信号。这些信号分为三大类：

#### 6.1.1 主动感知：定时器驱动

```go
// 配置示例
options := Options{
    HeartbeatInterval:       3 * time.Millisecond,     // 心跳发送频率
    SupportExpiryInterval:   1 * time.Millisecond,     // 过期检查频率
    IdleSupportFromInterval: 1 * time.Minute,          // 空闲检测频率
    SupportDuration:         6 * time.Millisecond,     // 支持有效期
}
```

**心跳间隔（HeartbeatInterval）的选择：**

- **默认值：3ms**
- **为什么这么快？**
  - Store Liveness 是 Raft 层的底层依赖
  - Raft 需要快速检测副本不可达（影响 quorum 判断）
  - 3ms 的间隔意味着 6ms（2 个周期）内就能检测到故障

**支持持续时间（SupportDuration）的选择：**

- **默认值：6ms**
- **为什么是心跳间隔的 2 倍？**
  ```
  T0: 发送心跳 A，承诺支持到 T0+6ms
  T3: 发送心跳 B，承诺支持到 T3+6ms

  如果心跳 B 丢失：
    - T3 时刻，心跳 A 仍然有效（T0+6 > T3）
    - T6 时刻，心跳 A 才过期
    - T6 时刻，应该已经收到心跳 C（T6+6ms）

  容忍一次丢包，不影响持续性
  ```

**过期检查间隔（SupportExpiryInterval）的选择：**

- **默认值：1ms**
- **为什么比心跳间隔更快？**
  - 需要及时撤回过期的支持
  - 1ms 的精度保证撤回延迟在几毫秒内
  - 快速撤回可以让 Raft 更早感知故障

#### 6.1.2 被动感知：消息驱动

```go
// 接收消息的流程
Transport 接收到网络包
    ↓
Transport.HandleMessage(msg)
    ↓
SupportManager.HandleMessage(msg)  // 不阻塞网络线程
    ↓
receiveQueue.Append(msg)           // 入队
    ↓
receiveQueue.sig <- struct{}{}     // 信号通知
    ↓
startLoop 收到信号
    ↓
handleMessages(msgs)               // 批量处理
```

**关键特性：异步解耦**

- 网络线程只做入队，立即返回（微秒级）
- 消息处理在后台批量进行（毫秒级）
- 队列满时，背压传递到发送方

#### 6.1.3 同步查询：读路径的实时性

```go
// Raft 层查询
epoch, expiration := sm.SupportFrom(remoteStore)

// 实现
func (sm *SupportManager) SupportFrom(id slpb.StoreIdent) (slpb.Epoch, hlc.Timestamp) {
    ss, ok, wasIdle := sm.requesterStateHandler.getSupportFrom(id)
    if !ok {
        sm.storesToAdd.addStore(id)  // 触发异步添加
        return 0, hlc.Timestamp{}     // 立即返回"未知"
    }
    if wasIdle {
        log.KvExec.Infof(context.TODO(), "store %+v is starting to heartbeat store %+v (after being idle)", sm.storeID, id)
    }
    return ss.Epoch, ss.Expiration
}
```

**关键观察：**

1. **首次查询触发心跳**：
   - 如果之前未心跳该 Store，立即加入待发送队列
   - 不等待定时器触发，下一次 `startLoop` 迭代就会发送

2. **读操作不阻塞**：
   - 直接读取内存状态（读锁保护）
   - 不进入事件循环
   - 延迟在微秒级

3. **懒惰初始化**：
   - 不预先心跳所有 Store
   - 只有被查询时才开始心跳
   - 减少不必要的网络流量

### 6.2 信号如何影响决策

#### 6.2.1 心跳丢失的处理

**场景：网络抖动导致心跳丢失**

```
T0: 发送心跳 A，Expiration = T6
T3: 应该发送心跳 B，但网络故障，未发送
T6: 心跳 A 过期

决策点：
  - supportExpiryTicker 在 T7 检测到过期
  - withdrawSupport() 撤回支持
  - withdrawalCallback() 通知 Raft
  - Raft 标记该副本不可达
```

**为什么是即时撤回，而非延迟容忍？**

- Store Liveness 的目标是**精确性**，而非容错性
- 如果心跳停止，可能意味着磁盘故障（严重问题）
- 快速撤回支持，让 Raft 转移租约到健康副本
- 避免请求路由到故障 Store（减少用户延迟）

#### 6.2.2 磁盘慢的处理

**场景：磁盘 IO stall 导致持久化慢**

```go
func (sm *SupportManager) sendHeartbeats(ctx context.Context) {
    rsfu := sm.requesterStateHandler.checkOutUpdate()
    defer sm.requesterStateHandler.finishUpdate(rsfu)

    heartbeats := rsfu.getHeartbeatsToSend(...)

    // 关键：持久化可能阻塞很久
    beforePersist := timeutil.Now()
    if err := rsfu.write(ctx, sm.engine); err != nil {
        log.KvExec.Warningf(ctx, "failed to write requester meta: %v", err)
        sm.metrics.HeartbeatFailures.Inc(int64(len(heartbeats)))
        return  // 失败，不发送心跳
    }
    persistDur := timeutil.Since(beforePersist)
    sm.metrics.HeartbeatPersistDuration.RecordValue(persistDur.Nanoseconds())

    // 只有持久化成功后，才发送心跳
    sm.requesterStateHandler.checkInUpdate(rsfu)
    for _, msg := range heartbeats {
        sm.sender.EnqueueMessage(ctx, msg)
    }
}
```

**关键决策：先持久化，后发送**

如果磁盘慢：
- 持久化耗时 100ms
- 这 100ms 内，心跳无法发送
- 远程 Store 的支持会过期
- 但这是**正确的行为**

**为什么？**

- 磁盘慢意味着本地 Store 可能无法正常服务
- 不应该让远程 Store 认为本地健康
- 快速失败（Fail Fast）是正确的

**测试验证：TestSupportManagerDiskStall**

```go
// 测试代码片段
engine.SetBlockOnWrite(true)  // 模拟磁盘阻塞

// 发送心跳会阻塞
go sm.sendHeartbeats(ctx)

// 但查询仍然可以进行
epoch, _ := sm.SupportFrom(remoteStore)  // 不阻塞
epoch, supported := sm.SupportFor(remoteStore)  // 不阻塞
```

这验证了：
- 读路径不依赖写路径
- 磁盘故障不影响查询

#### 6.2.3 接收队列满的处理

```go
const maxReceiveQueueSize = 10_000

func (q *receiveQueue) Append(msg *slpb.Message) error {
    q.mu.Lock()
    defer q.mu.Unlock()
    if len(q.mu.msgs) >= maxReceiveQueueSize {
        return receiveQueueSizeLimitReachedErr  // 拒绝新消息
    }
    q.mu.msgs = append(q.mu.msgs, msg)
    return nil
}
```

**为什么限制队列大小？**

1. **防止内存耗尽**：
   - 如果消息处理慢（磁盘故障），队列会无限增长
   - 限制大小，避免 OOM

2. **背压传递**：
   - 队列满时，返回错误
   - 发送方看到错误，会重试或放弃
   - 这是一种反馈机制

3. **故障隔离**：
   - 一个 Store 的故障不影响其他 Store
   - Transport 会记录错误，但不会崩溃

**为什么是 10,000？**

假设：
- 每条消息 ~100 字节
- 10,000 条消息 = ~1MB
- 即使队列满，内存占用也可控

### 6.3 设计如何在目标间取得平衡

| 目标 | 实现机制 | 权衡 |
|------|---------|------|
| **稳定性** | • 先持久化，后发送<br>• 有界队列<br>• 保护期机制 | 牺牲部分性能<br>（多一次 fsync） |
| **吞吐量** | • 批量处理消息<br>• 批量写盘（batch.Commit）<br>• 异步解耦 | 增加延迟<br>（批量积累时间） |
| **公平性** | • 优先级控制（心跳 > 消息）<br>• 所有 Store 平等对待 | 消息可能被延迟 |
| **资源利用率** | • 懒惰初始化<br>• 共享 heartbeatTicker<br>• 单线程事件循环 | 需要精心设计<br>（单线程瓶颈） |
| **实时性** | • 快速心跳（3ms）<br>• 快速撤回（1ms）<br>• 读路径不阻塞 | 增加 CPU 和网络开销 |

**核心权衡：一致性 vs 性能**

SupportManager 的设计清晰地选择了**一致性优先**：

```go
// 一致性保证：先持久化
if err := rsfu.write(ctx, sm.engine); err != nil {
    return  // 持久化失败，放弃发送
}
sm.sender.EnqueueMessage(msg)  // 只有持久化成功，才发送

// 而不是：性能优先
sm.sender.EnqueueMessage(msg)  // 先发送（快）
go rsfu.write(ctx, sm.engine)  // 异步持久化（可能失败）
```

**为什么选择一致性？**

- Store Liveness 是底层基础设施
- 不一致的活性信息会导致 Raft 错误决策
- 错误决策的代价远大于性能损失

---

## 七、具体运行示例（必须有）

### 7.1 正常场景：首次建立支持关系

**初始状态：**
- Store A (NodeID=1, StoreID=1)
- Store B (NodeID=2, StoreID=2)
- 两者从未交互

**时间线：**

```
T=0ms
─────────────────────────────────────────────────────────────
[Store A] Raft 层调用 sm.SupportFrom(Store B)
  └─> requesterStateHandler.getSupportFrom(Store B)
      └─> 返回 (ok=false)
  └─> storesToAdd.addStore(Store B)
  └─> 返回 (Epoch=0, Expiration=Empty) 给 Raft

[Store A] 状态：
  - supportFrom: {}
  - storesToAdd: {Store B}

[Raft] 收到空的 Expiration，认为 Store B 不可达
```

```
T=0.1ms
─────────────────────────────────────────────────────────────
[Store A] startLoop 收到 storesToAdd.sig
  └─> maybeAddStores()
      └─> requesterStateHandler.addStore(Store B)
          └─> supportFrom[Store B] = {Epoch: 0, Expiration: Empty}
      └─> metrics.SupportFromStores.Inc(1)
  └─> sendHeartbeats()  ← 立即发送，不等待定时器
      └─> rsfu.getHeartbeatsToSend(...)
          生成消息：
            - Type: MsgHeartbeat
            - From: Store A
            - To: Store B
            - Epoch: 1  ← 本地 MaxEpoch
            - Expiration: T=6.1ms  ← 当前时间 + 6ms
          更新 MaxRequested = T=6.1ms
      └─> rsfu.write(engine)  ← 持久化请求状态
          写入磁盘：
            - MaxRequested: T=6.1ms
            - MaxEpoch: 1
      └─> sender.EnqueueMessage(msg)
      └─> metrics.HeartbeatSuccesses.Inc(1)

[Store A] 状态：
  - supportFrom[Store B] = {Epoch: 0, Expiration: Empty}
  - MaxRequested: T=6.1ms
  - MaxEpoch: 1
```

```
T=0.5ms（网络延迟）
─────────────────────────────────────────────────────────────
[Store B] Transport 收到 MsgHeartbeat
  └─> sm.HandleMessage(msg)
      └─> receiveQueue.Append(msg)
      └─> receiveQueue.sig <- struct{}{}
      └─> metrics.ReceiveQueueSize.Inc(1)

[Store B] startLoop 收到 receiveQueue.Sig()
  └─> msgs := receiveQueue.Drain()  ← [MsgHeartbeat from A]
  └─> handleMessages(msgs)
      └─> ssfu.handleHeartbeat(msg)
          └─> supportFor[Store A] = {
                  Epoch: 1,
                  Expiration: T=6.1ms,
                  Withdrawn: false
              }
          生成响应：
            - Type: MsgHeartbeatResp
            - From: Store B
            - To: Store A
            - Epoch: 1
            - Expiration: T=6.1ms
          更新 MaxWithdrawn（如果需要）
      └─> batch.Write(ssfu.supportFor)
      └─> batch.Commit(sync=true)  ← fsync
      └─> ssfu.checkInUpdate()
      └─> sender.EnqueueMessage(MsgHeartbeatResp)
      └─> metrics.MessageHandleSuccesses.Inc(1)
      └─> metrics.SupportForStores.Update(1)

[Store B] 状态：
  - supportFor[Store A] = {Epoch: 1, Expiration: T=6.1ms}
  - MaxWithdrawn: T=0.5ms
```

```
T=1.0ms（网络延迟）
─────────────────────────────────────────────────────────────
[Store A] Transport 收到 MsgHeartbeatResp
  └─> sm.HandleMessage(resp)
      └─> receiveQueue.Append(resp)

[Store A] startLoop 处理消息
  └─> handleMessages([MsgHeartbeatResp])
      └─> rsfu.handleHeartbeatResponse(resp)
          └─> supportFrom[Store B] = {
                  Epoch: 1,
                  Expiration: T=6.1ms
              }
      └─> batch.Write(rsfu.supportFrom)
      └─> batch.Commit(sync=true)
      └─> rsfu.checkInUpdate()
      └─> metrics.MessageHandleSuccesses.Inc(1)

[Store A] 状态：
  - supportFrom[Store B] = {Epoch: 1, Expiration: T=6.1ms}
```

```
T=1.5ms
─────────────────────────────────────────────────────────────
[Store A] Raft 层再次调用 sm.SupportFrom(Store B)
  └─> requesterStateHandler.getSupportFrom(Store B)
      └─> 返回 (Epoch=1, Expiration=T=6.1ms)
  └─> 返回给 Raft

[Raft] 收到有效的 Expiration，认为 Store B 可达
  └─> 可以安全地向 Store B 发送 Raft 消息
  └─> 可以将租约转移给 Store B
```

```
T=3.0ms（心跳定时器触发）
─────────────────────────────────────────────────────────────
[Store A] heartbeatTicker.C 信号
  └─> sendHeartbeats()
      └─> 生成新的心跳：
          - Expiration: T=9.0ms  ← 当前时间 + 6ms
      └─> 持久化 MaxRequested = T=9.0ms
      └─> 发送心跳到 Store B

[Store A] 状态：
  - supportFrom[Store B] = {Epoch: 1, Expiration: T=6.1ms}
                                      ↑ 尚未更新，等待响应
  - MaxRequested: T=9.0ms
```

```
T=3.5ms
─────────────────────────────────────────────────────────────
[Store B] 收到新心跳，处理流程同上
  └─> 更新 supportFor[Store A] = {Epoch: 1, Expiration: T=9.0ms}
  └─> 发送响应

[Store A] 收到响应
  └─> 更新 supportFrom[Store B] = {Epoch: 1, Expiration: T=9.0ms}
```

**关键观察：**

1. **首次建立支持关系耗时 ~1ms**（包括网络延迟）
2. **持久化发生了 4 次**：
   - Store A 发送前持久化请求状态
   - Store B 处理心跳时持久化支持状态
   - Store B 发送响应（无需持久化）
   - Store A 处理响应时持久化更新状态

3. **稳态后，心跳周期为 3ms**，支持有效期为 6ms

### 7.2 故障场景：磁盘 IO stall

**初始状态：**
- Store A 和 Store B 已建立支持关系
- 当前时间 T=10ms
- supportFrom[Store B] = {Epoch: 1, Expiration: T=16ms}

**时间线：**

```
T=10ms
─────────────────────────────────────────────────────────────
[Store A] 磁盘开始变慢（IO stall）
  └─> Pebble 写入延迟从 1ms 增加到 50ms

[Store A] heartbeatTicker.C 触发
  └─> sendHeartbeats()
      └─> rsfu.getHeartbeatsToSend(...)
          生成心跳：Expiration = T=16ms
      └─> beforePersist = T=10ms
      └─> rsfu.write(engine)  ← 阻塞，等待磁盘
```

```
T=13ms（正常应该发送心跳的时间）
─────────────────────────────────────────────────────────────
[Store A] 仍在持久化中...
  └─> sendHeartbeats() 仍在 rsfu.write() 中阻塞

[Store A] startLoop 被阻塞
  └─> 无法处理其他事件
  └─> receiveQueue 继续接收消息（不阻塞网络）

[Store B] 检测到支持过期
  └─> supportExpiryTicker.C 触发（每 1ms）
  └─> withdrawSupport()
      └─> 检查 supportFor[Store A]
      └─> Expiration = T=16ms（上一次的）
      └─> now = T=13ms
      └─> 尚未过期，不撤回
```

```
T=16ms
─────────────────────────────────────────────────────────────
[Store B] 检测到支持过期
  └─> withdrawSupport()
      └─> 检查 supportFor[Store A]
      └─> Expiration = T=16ms
      └─> now = T=16ms
      └─> 过期！撤回支持
      └─> supportFor[Store A] = {
              Epoch: 2,  ← 增加 Epoch
              Expiration: Empty,
              Withdrawn: true
          }
      └─> 持久化状态
      └─> 调用 withdrawalCallback({StoreID: 1})

[Raft on Store B] 收到撤回通知
  └─> 标记 Store A 的副本不可达
  └─> 如果有租约在 Store A，考虑转移
  └─> 拒绝将新租约转移到 Store A
```

```
T=60ms（磁盘恢复）
─────────────────────────────────────────────────────────────
[Store A] 持久化完成
  └─> persistDur = 50ms
  └─> metrics.HeartbeatPersistDuration.RecordValue(50ms)
  └─> rsfu.checkInUpdate()
  └─> sender.EnqueueMessage(msg)
  └─> metrics.HeartbeatSuccesses.Inc(1)

但是：
  - 发送的心跳已经过期（Expiration = T=16ms，现在 T=60ms）
  - Store B 会忽略这个过期心跳
```

```
T=63ms（下一个心跳周期）
─────────────────────────────────────────────────────────────
[Store A] heartbeatTicker.C 触发
  └─> sendHeartbeats()
      └─> 生成心跳：Expiration = T=69ms
      └─> 持久化（假设磁盘已恢复，耗时 1ms）
      └─> 发送心跳

[Store B] 收到心跳
  └─> handleHeartbeat()
      └─> msg.Epoch = 1
      └─> local.Epoch = 2  ← 之前撤回时增加了
      └─> msg.Epoch < local.Epoch，拒绝旧心跳
      └─> 不更新状态
```

**问题：Epoch 不匹配！**

**解决方案：Store A 需要增加 Epoch**

实际上，Store A 在**重启后**会增加 Epoch，但在运行时不会。这意味着：

- 如果磁盘慢导致心跳停止
- Store B 撤回支持并增加 Epoch
- Store A 的心跳将被永久拒绝
- 直到 Store A 重启

**这是设计的权衡：**
- 不在运行时增加 Epoch（避免复杂性）
- 依赖重启机制恢复
- 实际上，磁盘 IO stall 通常意味着需要重启

### 7.3 边界场景：重启后的时钟推进

**初始状态：**
- Store A 在 T=100ms 崩溃
- 崩溃前：
  - MaxRequested = T=150ms
  - MaxWithdrawn = T=120ms
  - MaxEpoch = 5
- 重启后，本地时钟回退到 T=80ms

**时间线：**

```
T=80ms（重启时刻）
─────────────────────────────────────────────────────────────
[Store A] 调用 onRestart()

步骤 1: 加载持久化状态
  └─> supporterStateHandler.read(engine)
      └─> MaxWithdrawn = T=120ms
  └─> requesterStateHandler.read(engine)
      └─> MaxRequested = T=150ms
      └─> MaxEpoch = 5

步骤 2: 推进时钟到 MaxWithdrawn
  └─> clock.UpdateAndCheckMaxOffset(T=120ms)
      └─> 如果 offset 超过最大值，返回错误
      └─> 否则，推进时钟到 T=120ms
  └─> 当前时钟：T=120ms

步骤 3: 等待到 MaxRequested
  └─> clock.SleepUntil(T=150ms)
      └─> 阻塞 30ms
  └─> 当前时钟：T=150ms

步骤 4: 设置保护期
  └─> minWithdrawalTS = T=150ms + 5s = T=5150ms

步骤 5: 增加 Epoch
  └─> MaxEpoch = 6
  └─> 持久化

[Store A] onRestart() 完成
  └─> 当前时钟：T=150ms
  └─> MaxEpoch: 6
  └─> minWithdrawalTS: T=5150ms
```

**为什么这样设计？**

**问题 1：如果不推进时钟到 MaxWithdrawn**

```
场景：
  - 崩溃前，撤回对 Store X 的支持，记录 MaxWithdrawn = T=120ms
  - 重启后，时钟 = T=80ms
  - 发送新心跳，Expiration = T=80ms + 6ms = T=86ms

问题：
  - T=86ms < T=120ms
  - Store X 收到心跳，发现过期时间早于上次撤回时间
  - 可能误认为这是旧的、过期的心跳
```

**问题 2：如果不等待到 MaxRequested**

```
场景：
  - 崩溃前，请求支持到 T=150ms
  - 重启后，时钟 = T=120ms
  - 立即发送心跳，Expiration = T=120ms + 6ms = T=126ms

问题：
  - 远程 Store 可能仍然认为我们的支持有效到 T=150ms
  - 但我们自己认为只到 T=126ms
  - 不一致
```

通过等待到 T=150ms：
- 保证旧的支持承诺已过期
- 新的心跳不会冲突

**问题 3：如果不增加 Epoch**

```
场景：
  - 网络中可能有崩溃前（Epoch=5）的心跳还在传输
  - 重启后发送 Epoch=5 的新心跳

问题：
  - 远程 Store 无法区分旧心跳和新心跳
  - 可能接受旧心跳，导致状态混乱
```

通过增加到 Epoch=6：
- 远程 Store 可以识别这是新时代的心跳
- 旧心跳（Epoch=5）会被忽略

---

## 八、设计取舍与替代方案（Trade-offs）

### 8.1 当前方案的优势

| 维度 | 优势 | 具体表现 |
|------|------|---------|
| **正确性** | 强一致性保证 | • 先持久化，后发送<br>• 严格的 Epoch 机制<br>• 重启后的时钟推进 |
| **可调试性** | 清晰的状态机 | • 所有状态持久化<br>• 完善的指标<br>• 详细的日志 |
| **性能** | 批量优化 | • 批量处理消息<br>• 批量写盘<br>• 共享定时器 |
| **并发安全** | 单线程写入 | • Event Loop 模式<br>• COW 状态更新<br>• 读写分离 |
| **资源效率** | 懒惰初始化 | • 只心跳被查询的 Store<br>• 空闲检测机制<br>• 有界队列 |

### 8.2 当前方案的代价

| 维度 | 代价 | 影响 |
|------|------|------|
| **延迟** | 每次心跳需要 fsync | • 心跳发送延迟 +1ms（典型）<br>• 磁盘慢时更严重 |
| **吞吐量** | 单线程事件循环 | • 单个 Store 处理能力受限<br>• 约 100k 心跳/秒/Store |
| **内存** | COW 复制状态 | • 每次更新复制整个 map<br>• 但通常很小（<100 个 Store） |
| **复杂性** | 重启逻辑复杂 | • 时钟推进<br>• Epoch 管理<br>• 保护期 |
| **网络** | 频繁心跳 | • 3ms 间隔<br>• 每对 Store 6 条消息/秒 |

### 8.3 替代方案分析

#### 方案 1：基于 Lease 的活性检测

**设计：**
```go
type Lease struct {
    StoreID    roachpb.StoreID
    Expiration hlc.Timestamp
    LeaseID    uint64
}

// 不发送心跳，而是发送 Lease
func (sm *SupportManager) GrantLease(to slpb.StoreIdent, duration time.Duration) Lease {
    return Lease{
        StoreID:    to.StoreID,
        Expiration: sm.clock.Now().Add(duration),
        LeaseID:    sm.nextLeaseID.Inc(),
    }
}
```

**优势：**
- 减少网络流量（不需要定期心跳）
- Lease 可以更长（秒级），减少 fsync 次数

**劣势：**
- 故障检测延迟更高（秒级 vs 毫秒级）
- 租约续期复杂（需要协商）
- 不适合 Store Liveness 的快速检测需求

**为什么 CockroachDB 不选择？**
- Store Liveness 需要毫秒级检测
- Lease 模式更适合分钟级的节点活性（Node Liveness 已使用）

#### 方案 2：基于 Gossip 的活性广播

**设计：**
```go
// 每个 Store 定期广播自己的活性状态
type AliveBeacon struct {
    StoreID   roachpb.StoreID
    Timestamp hlc.Timestamp
    Epoch     uint64
}

// Gossip 协议自动传播
gossip.Broadcast(AliveBeacon{...})
```

**优势：**
- 去中心化，无单点故障
- 自动传播，减少点对点通信

**劣势：**
- 收敛时间不确定（Gossip 的固有问题）
- 无法区分"未收到"和"真的故障"
- 难以实现精确的双向确认

**为什么 CockroachDB 不选择？**
- Store Liveness 需要**精确的双向确认**
- Gossip 适合最终一致的信息传播，不适合活性检测

#### 方案 3：集中式活性服务

**设计：**
```go
// 专门的活性检测服务
type LivenessService struct {
    stores map[roachpb.StoreID]*StoreStatus
}

// 所有 Store 向中心服务报告
func (ls *LivenessService) Heartbeat(storeID roachpb.StoreID) {
    ls.stores[storeID].LastHeartbeat = time.Now()
}

// 查询活性
func (ls *LivenessService) IsAlive(storeID roachpb.StoreID) bool {
    return time.Since(ls.stores[storeID].LastHeartbeat) < threshold
}
```

**优势：**
- 简单，易于实现
- 全局视图，便于监控

**劣势：**
- 单点故障（中心服务挂了，整个集群瘫痪）
- 扩展性差（所有心跳集中到一个服务）
- 网络分区时无法工作

**为什么 CockroachDB 不选择？**
- CockroachDB 的设计哲学是**去中心化**
- 不依赖单点服务

#### 方案 4：基于 Raft Heartbeat 的复用

**设计：**
```go
// 复用 Raft 的心跳机制
// 如果 Raft 可以通信，就认为 Store 活着
func (sm *SupportManager) SupportFrom(storeID roachpb.StoreID) bool {
    // 检查是否有该 Store 上的副本
    for _, replica := range sm.store.Replicas() {
        if replica.ContainsStore(storeID) {
            return replica.RaftStatus().Progress[storeID].RecentActive
        }
    }
    return false
}
```

**优势：**
- 零额外开销（复用现有心跳）
- 简单，无需新协议

**劣势：**
- **依赖副本存在**：如果两个 Store 没有共同副本，无法检测活性
- **范围不全**：只能检测有副本关系的 Store
- **语义不匹配**：Raft 心跳检测的是副本活性，不是 Store 活性

**为什么 CockroachDB 不选择？**
- Store Liveness 需要检测**任意 Store** 的活性
- 不能依赖副本拓扑

### 8.4 当前方案的适用场景与限制

**适用场景：**

1. **低延迟要求**：需要毫秒级故障检测
2. **精确性要求**：需要双向确认的活性保证
3. **独立性要求**：Store 级别的活性，独立于节点和副本
4. **持久性要求**：活性状态需要在重启后恢复

**限制：**

1. **扩展性**：
   - 单个 Store 的处理能力受单线程限制
   - 不适合超过 10,000 个 Store 的集群

2. **网络开销**：
   - 每对 Store 需要持续心跳
   - N 个 Store 的集群，理论上需要 N*(N-1) 条心跳流
   - 实际通过懒惰初始化缓解

3. **磁盘依赖**：
   - 每次心跳需要 fsync
   - 磁盘慢会严重影响活性检测
   - 但这是**有意为之**（磁盘慢意味着 Store 不健康）

4. **时钟依赖**：
   - 严重依赖 HLC 的正确性
   - 时钟回退会导致重启延迟
   - 时钟偏移过大会导致启动失败

---

## 九、总结与心智模型（Mental Model）

### 9.1 核心思想总结

SupportManager 是一个**去中心化的、双向确认的、Store 级别的活性检测系统**。它的核心思想可以用一句话概括：

> **"每个 Store 通过持续的、可持久化的、双向心跳，向其他 Store 证明自己是健康的，并记录其他 Store 对自己的支持承诺。"**

**关键设计决策：**

1. **双向心跳**：
   - 请求方（Requester）主动发送心跳
   - 支持方（Supporter）被动响应并记录
   - 双方都持久化状态，保证重启一致性

2. **先持久化，后发送**：
   - 所有承诺必须先写盘
   - 磁盘故障时，宁可停止心跳，也不发送虚假承诺
   - 这是一致性优先的设计

3. **Epoch + Expiration**：
   - Epoch 区分不同生命周期
   - Expiration 提供精确的过期时间
   - 两者结合，解决了重启、网络延迟、消息重排等问题

4. **单线程事件循环 + COW**：
   - 所有写操作串行化，简化并发
   - COW 保证读路径不被阻塞
   - 批量处理提高吞吐量

5. **懒惰初始化 + 空闲检测**：
   - 只心跳被使用的 Store
   - 空闲的心跳关系会被清理
   - 减少不必要的资源消耗

### 9.2 可复用的心智模型

**如果你要设计一个类似的活性检测系统，可以这样思考：**

#### 模型 1：双向账本（Bilateral Ledger）

把活性检测想象成两个账本：

```
┌─────────────────────────────┐
│  我的"应收账款"（Requester） │
│  ─────────────────────────  │
│  Store B 欠我支持到 T=100   │
│  Store C 欠我支持到 T=120   │
│  ─────────────────────────  │
│  Total: 我请求了 2 个支持    │
└─────────────────────────────┘

┌─────────────────────────────┐
│  我的"应付账款"（Supporter） │
│  ─────────────────────────  │
│  我欠 Store X 支持到 T=90   │
│  我欠 Store Y 支持到 T=110  │
│  ─────────────────────────  │
│  Total: 我承诺了 2 个支持    │
└─────────────────────────────┘
```

**核心规则：**
- **应收账款**：我发送心跳，期望收到支持承诺
- **应付账款**：我收到心跳，承诺支持对方
- **双方记账**：双方都持久化，避免"赖账"
- **过期清算**：定期检查过期账款，撤回承诺

#### 模型 2：带保质期的食品（Perishable Goods）

把活性承诺想象成带保质期的食品：

```
心跳 = 一盒牛奶
保质期 = Expiration
生产日期 = 发送时间
批次号 = Epoch
```

**核心规则：**
- **生产者**：发送心跳时，标记保质期
- **消费者**：收到心跳时，检查是否过期
- **仓库管理**：定期检查库存，扔掉过期食品
- **批次管理**：新批次（Epoch）可以替换旧批次

#### 模型 3：租约协议（Lease Protocol）

把活性检测想象成租房协议：

```
租客（Requester） ←──→ 房东（Supporter）
    ↓                      ↓
发送租金（心跳）      提供房屋（支持）
    ↓                      ↓
记录付款凭证          记录租约合同
```

**核心规则：**
- **租客**：定期支付租金（发送心跳）
- **房东**：提供房屋使用权（支持承诺）
- **合同期限**：到期前可以续租（持续心跳）
- **违约处理**：租客停止付款，房东收回房屋（撤回支持）

### 9.3 关键不变量（Invariants）

在设计或修改 SupportManager 时，必须保证以下不变量：

**不变量 1：持久化优先**
```
∀ 心跳 msg ∈ 已发送的心跳:
    msg 的状态已写入磁盘 ⇒ msg 已发送到网络

推论：
    如果磁盘写入失败，心跳不会被发送
```

**不变量 2：Epoch 单调递增**
```
∀ Store S, ∀ 时间 t1 < t2:
    Epoch(S, t1) ≤ Epoch(S, t2)

推论：
    重启后，Epoch 必须增加
```

**不变量 3：MaxRequested 的等待**
```
∀ Store S, 如果 S 在时间 t 重启:
    ∀ 新心跳在时间 t' > t 发送:
        t' ≥ MaxRequested(S, t)

推论：
    重启后，必须等待旧的承诺过期
```

**不变量 4：支持的单向性**
```
∀ Store A, B:
    SupportFrom(A, B) 和 SupportFor(B, A) 是独立的

推论：
    A 支持 B 不意味着 B 支持 A
```

**不变量 5：查询的非阻塞性**
```
∀ SupportFrom() 和 SupportFor() 调用:
    不进入事件循环
    不等待网络或磁盘
    只读内存状态

推论：
    查询延迟在微秒级
```

### 9.4 扩展与演化方向

**如果 CockroachDB 需要扩展 Store Liveness，可能的方向：**

1. **分层心跳**：
   - 区分"关键 Store"和"普通 Store"
   - 关键 Store 使用更快的心跳（1ms）
   - 普通 Store 使用标准心跳（3ms）

2. **自适应间隔**：
   - 根据负载动态调整心跳间隔
   - 低负载时，间隔更长（节省资源）
   - 高负载时，间隔更短（快速检测）

3. **批量心跳**：
   - 将多个 Store 的心跳合并到一个消息
   - 减少网络包数量
   - 但增加复杂性

4. **基于事件的心跳**：
   - 不定期心跳，而是在有事件时才心跳
   - 例如：Raft 消息发送时，顺带心跳
   - 减少独立的网络流量

**当前设计的演化空间：**
- 已经非常优化（批量、懒惰、COW）
- 主要瓶颈是磁盘 fsync
- 未来可能考虑组提交（Group Commit）优化

### 9.5 最终建议

**如果你要实现或维护类似系统，记住：**

1. **一致性优先**：
   - 活性检测的错误会导致系统级故障
   - 宁可保守（误报故障），不可激进（漏报故障）

2. **持久化是关键**：
   - 重启是常态，不是异常
   - 状态必须可恢复

3. **时钟是基础**：
   - HLC 提供了全局时间语义
   - 但要处理时钟偏移和回退

4. **批量是优化**：
   - 单个操作慢，批量操作快
   - 但要平衡延迟和吞吐量

5. **测试是保障**：
   - 磁盘故障、网络丢包、时钟回退
   - 每个边界情况都要测试

---

## 附录：代码导航指南

### 关键文件与函数

| 文件 | 关键函数 | 职责 |
|------|---------|------|
| `support_manager.go` | `NewSupportManager` | 构造 |
|  | `Start` | 启动 |
|  | `onRestart` | 重启恢复 |
|  | `startLoop` | 事件循环 |
|  | `sendHeartbeats` | 发送心跳 |
|  | `handleMessages` | 处理消息 |
|  | `withdrawSupport` | 撤回支持 |
| `requester_state.go` | `checkOutUpdate` | COW checkout |
|  | `checkInUpdate` | COW checkin |
|  | `getHeartbeatsToSend` | 生成心跳 |
|  | `handleHeartbeatResponse` | 处理响应 |
| `supporter_state.go` | `handleHeartbeat` | 处理心跳 |
|  | `withdrawSupport` | 执行撤回 |
| `transport.go` | `EnqueueMessage` | 发送消息 |
|  | `HandleMessage` | 接收消息 |

### 调试技巧

1. **查看心跳状态**：
   ```sql
   SELECT * FROM crdb_internal.kv_store_liveness;
   ```

2. **查看指标**：
   ```
   sm.metrics.HeartbeatSuccesses.Count()
   sm.metrics.HeartbeatFailures.Count()
   sm.metrics.SupportFromStores.Value()
   sm.metrics.SupportForStores.Value()
   ```

3. **查看日志**：
   ```
   grep "storeliveness" cockroach.log
   ```

4. **测试心跳**：
   ```go
   sm.SupportFrom(remoteStore)  // 触发心跳
   time.Sleep(10 * time.Millisecond)
   epoch, exp := sm.SupportFrom(remoteStore)
   fmt.Printf("Epoch: %d, Expiration: %s\n", epoch, exp)
   ```

---

**全文完。**

这份文档系统地讲解了 SupportManager 的设计、实现、运行时行为和工程权衡，涵盖了从"为什么需要"到"如何实现"再到"如何扩展"的完整链路。希望能帮助你建立对 Store Liveness 的深刻理解。
