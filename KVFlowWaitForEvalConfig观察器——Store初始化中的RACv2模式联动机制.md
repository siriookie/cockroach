# KVFlowWaitForEvalConfig 观察器深度剖析
## Store 初始化中的 RACv2 模式联动与 Raft 流控决策

---

## 一、BFS Why：为什么需要动态联动流控策略？

### 1.1 问题背景：RACv2 演进的不同阶段

CockroachDB 的流量控制系统在演进过程中经历了多个阶段：

```
演进阶段 1: 无流控（历史状态）
  └─ Raft 消息无速率限制 → 接收端 Raft 队列堆积

演进阶段 2: 本地接收端保护（maxLen 限制）
  └─ raftReceiveQueue.enforceMaxLen = true
  └─ 每个 Range 最多 10 条消息 → 消息被拒绝 → 发送端重试

演进阶段 3: RACv2 部分部署（仅弹性工作）
  └─ cfg.Mode = ApplyToElastic
  └─ 发送端仅限制弹性工作 → 常规工作绕过限制

演进阶段 4: RACv2 完全部署（所有工作）
  └─ cfg.Mode = ApplyToAll
  └─ 所有发送端工作都受 token 池约束 → 本地 maxLen 冗余
```

**核心矛盾**：

在不同部署阶段，最优的流控策略完全不同：

| 阶段 | 情景 | RACv2 强度 | 最优 maxLen | 理由 |
|------|------|-----------|-----------|------|
| 早期 | 无 RACv2 | 无 | 强制执行 | 接收端唯一防线 |
| 中期 | 仅弹性 | 弱 | 强制执行 | 常规工作不受限 |
| 成熟 | 所有工作 | 强 | 关闭 | 发送端已控制，本地限制冗余 |

**问题**：如何在**运行时**自动切换这些策略，而不需要重启集群？

### 1.2 WaitForEvalConfig 的设计目标

```
目标 1: 自动检测系统当前处于哪个 RACv2 阶段
  └─ 读取 kvflowcontrol.Mode 设置
  └─ 无需人工干预，基于配置自动判断

目标 2: 在阶段变化时通知所有关心的组件
  └─ Raft 接收队列
  └─ Range 控制器
  └─ 其他流控相关的系统

目标 3: 零停机时间的配置切换
  └─ 配置变更 → 立即通知 → 新策略生效
  └─ 无需重启 Store / Raft Scheduler

目标 4: 避免状态转移时的竞态条件
  └─ 确保所有订阅者看到一致的状态转移
  └─ 处理并发设置修改
```

### 1.3 Store 初始化中的关键位置

```go
// store.go:1561-1576

// 第 1 步：创建 Raft 接收队列的内存监控器
s.raftRecvQueues.mon = mon.NewUnlimitedMonitor(...)

// 第 2 步：注册观察器 ← 这正是核心机制！
s.cfg.KVFlowWaitForEvalConfig.RegisterWatcher(func(wc rac2.WaitForEvalCategory) {
    // 当 RACv2 模式变化时，动态调整 Raft 队列的流控策略
    s.raftRecvQueues.SetEnforceMaxLen(wc != rac2.AllWorkWaitsForEval)
})
```

**为什么在 Store 初始化期间做这个？**

1. **生命周期匹配**：Store 的生命周期 ≈ WaitForEvalConfig 的生命周期
2. **Early Binding**：在 Store 运行前建立观察关系
3. **完整覆盖**：从第一条 Raft 消息起就应用最新的流控策略

---

## 二、BFS How：数据流从配置到动作

### 2.1 信息传递链路

```
集群配置变更事件
    │
    ├─ 运维设置：kvflowcontrol.Mode = "apply_to_all"
    │  或 kvflowcontrol.Enabled = false
    │
    ▼
WaitForEvalConfig 检测到变化
    │
    ├─ (line 55-59) 注册在 Settings 上的 SetOnChange 回调被触发
    │
    ▼
notifyChanged() 被调用
    │
    ├─ 计算新的 WaitForEvalCategory
    │  └─ computeCategory()
    │
    ├─ 比较新旧值：是否 decreased（缩小范围）？
    │  └─ 如果是，关闭旧的 waitCategoryDecreasedCh
    │  └─ 创建新的 waitCategoryDecreasedCh
    │
    ▼
通知所有已注册的 Watchers
    │
    ├─ 遍历 watcherMu.cbs 中的所有回调函数
    │
    └─ 调用：cb(wc)  // 传入新的 WaitForEvalCategory

        ↓
   [Store 的回调函数执行]
        │
        ├─ 接收到 wc (WaitForEvalCategory)
        │
        ├─ 决策：
        │  if wc == AllWorkWaitsForEval:
        │      SetEnforceMaxLen(false)  // 关闭本地队列长度限制
        │  else:
        │      SetEnforceMaxLen(true)   // 启用本地队列长度限制
        │
        ▼
   s.raftRecvQueues.SetEnforceMaxLen(...)
        │
        ├─ 原子更新 enforceMaxLen.Store(bool)
        │
        ├─ 遍历所有现存的 raftReceiveQueue
        │  └─ 对每个 queue 调用 SetEnforceMaxLen()
        │
        ▼
   从此时起，新的 Raft 消息 Append 时使用新策略
```

### 2.2 具体示例：从配置改变到影响

**时刻 T=0**: 集群处于 "ApplyToElastic" 模式
```
kvflowcontrol.Mode = "apply_to_elastic"
kvflowcontrol.Enabled = true

WaitForEvalConfig.Current()
  └─ Returns: WaitForEvalCategory = OnlyElasticWorkWaitsForEval (值=1)

Store 回调被触发时：
  SetEnforceMaxLen(OnlyElasticWorkWaitsForEval != AllWorkWaitsForEval)
  SetEnforceMaxLen(1 != 2)  // 1 != 2 为 true
  SetEnforceMaxLen(true)

所有新到达的 Raft 消息：
  - Append 时检查 enforceMaxLen (true)
  - 如果 len(queue) >= maxLen(=10)，拒绝消息
```

**时刻 T=100ms**: 运维执行 `SET CLUSTER SETTING kvflowcontrol.mode = 'apply_to_all'`
```
kvflowcontrol.Mode 设置变更
  ↓
Settings.SetOnChange() 回调触发
  ↓
w.notifyChanged() 执行

w.mu.Lock()
  新 waitCategory = w.computeCategory()
    └─ enabled = true
    └─ mode = "apply_to_all"
    └─ 返回 AllWorkWaitsForEval (值=2)

  // 比较旧值(1) 与 新值(2)
  if 1 > 2:  // false，没有 decreased
      // 不关闭 channel，因为约束范围在扩大

  w.mu.waitCategory = 2
w.mu.Unlock()

// 通知所有 watchers
for each cb in watchers:
    cb(2)  // 传入 AllWorkWaitsForEval

[Store 的回调被调用]
  SetEnforceMaxLen(AllWorkWaitsForEval != AllWorkWaitsForEval)
  SetEnforceMaxLen(2 != 2)  // false
  SetEnforceMaxLen(false)

  raftRecvQueues.SetEnforceMaxLen(false)
    └─ enforceMaxLen.Store(false)
    └─ 对所有现存 raftReceiveQueue：q.SetEnforceMaxLen(false)
```

**时刻 T=100ms+**: 新行为生效
```
所有新到达的 Raft 消息：
  - Append 时检查 enforceMaxLen (false)
  - enforceMaxLen == false，跳过 len 检查
  - 只要内存允许，消息被接受
  - 背压由 RACv2 在发送端控制
```

### 2.3 关键时序保证

```
观察器的 "first call happens within this method" 保证：
─────────────────────────────────────────────────────

RegisterWatcher(cb) 被调用时：

1. 获取 watcherMu
2. 读取当前的 waitCategory （带 mu.RLock）
3. 立即调用 cb(waitCategory)  ← 在返回前
4. 追加 cb 到列表

因此：
  ✓ 调用者知道当前的确切状态
  ✓ 无需轮询或猜测
  ✓ 初始状态保证与后续更新一致

示例：
  Store 初始化时，RegisterWatcher 被调用
    ↓
  cb 被立即调用一次，返回当前的 WaitForEvalCategory
    ↓
  Store 根据返回值初始化 raftRecvQueues.enforceMaxLen
    ↓
  Store 完全启动后，回调已被正确设置，监听后续变化
```

---

## 三、DFS How：WaitForEvalConfig 和 RegisterWatcher 的实现

### 3.1 WaitForEvalConfig 的结构

```go
// wait_for_eval_config.go:36-48
type WaitForEvalConfig struct {
    st *cluster.Settings  // 指向全局集群配置

    mu struct {
        syncutil.RWMutex

        waitCategory WaitForEvalCategory
        // 每当 waitCategory 减小时，这个 channel 被 close，
        // 然后创建新的。用于通知等待者约束范围已缩小。
        waitCategoryDecreasedCh chan struct{}
    }

    // 观察者列表，不受 mu 保护
    watcherMu struct {
        syncutil.Mutex
        cbs []WatcherCallback  // WatcherCallback func(wc WaitForEvalCategory)
    }
}
```

**互斥锁顺序约束**：
```
watcherMu 必须在 mu 之前获取
  └─ 这确保了调用 watchers 时不会死锁
  └─ 理由：watchers 可能需要读取 mu.waitCategory
```

### 3.2 初始化与配置监听

```go
// wait_for_eval_config.go:53-64
func NewWaitForEvalConfig(st *cluster.Settings) *WaitForEvalConfig {
    w := &WaitForEvalConfig{st: st}

    // 监听两个关键设置的变化
    kvflowcontrol.Mode.SetOnChange(&st.SV, func(ctx context.Context) {
        w.notifyChanged()  // Mode 变化 → 重新计算 category
    })
    kvflowcontrol.Enabled.SetOnChange(&st.SV, func(ctx context.Context) {
        w.notifyChanged()  // Enabled 变化 → 重新计算 category
    })

    // 初始化：计算初始状态并通知所有 watchers
    w.notifyChanged()

    return w
}
```

**关键决策逻辑**：

```go
// wait_for_eval_config.go:116-129
func (w *WaitForEvalConfig) computeCategory() WaitForEvalCategory {
    enabled := kvflowcontrol.Enabled.Get(&w.st.SV)
    if !enabled {
        // RACv2 完全禁用
        return NoWorkWaitsForEval  (0) // 无工作等待
    }

    mode := kvflowcontrol.Mode.Get(&w.st.SV)
    switch mode {
    case kvflowcontrol.ApplyToElastic:
        // RACv2 仅应用于弹性工作
        return OnlyElasticWorkWaitsForEval  (1) // 仅弹性工作等待
    case kvflowcontrol.ApplyToAll:
        // RACv2 应用于所有工作
        return AllWorkWaitsForEval  (2) // 所有工作等待
    }
    panic(errors.AssertionFailedf("unknown mode %v", mode))
}
```

三个阶段的含义：

```
NoWorkWaitsForEval (0)
  └─ 无任何工作类型需要等待 RACv2 tokens
  └─ 发生在：kvflowcontrol.Enabled = false

OnlyElasticWorkWaitsForEval (1)
  └─ 仅 ElasticWork (备份、恢复等) 需要等待 tokens
  └─ RegularWork (SQL 查询) 不受 RACv2 限制
  └─ 发生在：Mode = "apply_to_elastic"

AllWorkWaitsForEval (2)
  └─ 所有工作类型都需要等待 tokens
  └─ 包括 ElasticWork 和 RegularWork
  └─ 发生在：Mode = "apply_to_all"（最严格）
```

### 3.3 notifyChanged 的竞态处理

```go
// wait_for_eval_config.go:68-105
func (w *WaitForEvalConfig) notifyChanged() {
    // 第 1 步：在 mu 保护下计算新状态
    func() {
        w.mu.Lock()
        defer w.mu.Unlock()

        // 设置变化后立即计算，避免中间状态
        waitCategory := w.computeCategory()

        if w.mu.waitCategoryDecreasedCh == nil {
            // 初始化路径
            w.mu.waitCategoryDecreasedCh = make(chan struct{})
            w.mu.waitCategory = waitCategory
            return  // 初始化完成，不通知 watchers
        }

        // 第 2 步：检测是否约束范围缩小（decreased）
        if w.mu.waitCategory > waitCategory {
            // 从更严格的阶段 → 更宽松的阶段
            // 例如：AllWorkWaitsForEval (2) → OnlyElasticWorkWaitsForEval (1)

            // 关闭旧 channel，通知所有 waitCategoryDecreasedCh 的监听者
            close(w.mu.waitCategoryDecreasedCh)
            // 创建新 channel，用于下次 decrease
            w.mu.waitCategoryDecreasedCh = make(chan struct{})
        }
        // 若 waitCategory >= w.mu.waitCategory（范围扩大或不变）：
        // 无需通知，因为要约束的工作类型在增加或保持不变
        // 已在等待的请求不会因此更新。这是一个优化。

        w.mu.waitCategory = waitCategory
    }()

    // 第 3 步：在 watcherMu 保护下通知所有观察者
    w.watcherMu.Lock()
    defer w.watcherMu.Unlock()

    // 再次读取 mu 中的值（可能已变更）
    w.mu.RLock()
    wc := w.mu.waitCategory
    w.mu.RUnlock()

    // 按顺序调用所有回调
    for _, cb := range w.watcherMu.cbs {
        cb(wc)  // 传入最新的 WaitForEvalCategory
    }
}
```

**竞态处理的妙处**：

```
场景：三个 goroutine 并发修改设置

时刻 1: G1 执行 settings.Change(Mode, "apply_to_all")
       └─ 触发 SetOnChange 回调
       └─ 调用 notifyChanged()

时刻 2: G2 执行 settings.Change(Enabled, false)
       └─ 也触发 SetOnChange 回调
       └─ 也调用 notifyChanged()

时刻 3: G3 调用 RegisterWatcher(cb)
       └─ 想要获取当前状态

通过 mu（第一个 Lock）：
  ✓ G1 和 G2 的 computeCategory 顺序序列化
  ✓ 无论谁先完成，最终的 waitCategory 总是一致的

通过 watcherMu（第二个 Lock）：
  ✓ G1/G2 的 watchers 通知 vs G3 的 RegisterWatcher 不会交错
  ✓ G3 注册时总能得到最新值

结果：
  ✗ 不会有监听者收到"中间"的不一致状态
  ✓ 所有观察者看到从 old_state → new_state 的一致转移
```

### 3.4 RegisterWatcher 的实现

```go
// wait_for_eval_config.go:134-142
func (w *WaitForEvalConfig) RegisterWatcher(cb WatcherCallback) {
    w.watcherMu.Lock()
    defer w.watcherMu.Unlock()

    // 获取当前值，同时持有 watcherMu
    w.mu.RLock()
    wc := w.mu.waitCategory
    w.mu.RUnlock()

    // 立即调用一次，让观察者知道初始状态
    cb(wc)

    // 追加到列表，用于后续的 notifyChanged
    w.watcherMu.cbs = append(w.watcherMu.cbs, cb)
}
```

**关键保证**：

```
RegisterWatcher 返回前：
  ✓ 观察者已被调用一次，知道当前状态
  ✓ 观察者已被添加到列表
  ✓ 后续的 notifyChanged 会包含这个观察者

推论：
  ✓ 观察者永远不会错过任何状态变化
  ✓ 初始化 vs 后续更新保持一致
```

---

## 四、Runtime Behavior：运行时行为观察

### 4.1 集群启动时的初始化序列

```
时刻 T=0: 节点启动 → NewStore() 被调用
┌─────────────────────────────────┐
│ Store 初始化第 4 阶段开始         │
└────┬────────────────────────────┘
     │
     ├─ L1561: 创建 raftRecvQueues.mon
     │  s.raftRecvQueues.mon = mon.NewUnlimitedMonitor(...)
     │
     ├─ L1566: 注册观察器
     │  s.cfg.KVFlowWaitForEvalConfig.RegisterWatcher(func(wc) {
     │      s.raftRecvQueues.SetEnforceMaxLen(wc != AllWorkWaitsForEval)
     │  })
     │
     │  此时发生：
     │  1. RegisterWatcher 被调用
     │  2. 立即读取当前的 WaitForEvalCategory
     │     └─ 根据 kvflowcontrol settings 计算
     │     └─ 通常是 AllWorkWaitsForEval (2) 在完全部署集群中
     │  3. 回调被立即调用一次：
     │     cb(AllWorkWaitsForEval)
     │  4. 接收到 wc = 2 (AllWorkWaitsForEval)
     │  5. SetEnforceMaxLen(2 != 2) = SetEnforceMaxLen(false)
     │  6. 从此时起，Raft 队列不检查消息数限制
     │
     └─ L1590: 创建 Raft Scheduler
        s.scheduler = newRaftScheduler(...)

时刻 T=1s: Store 完全启动，开始接收 Raft 消息
```

### 4.2 配置变更的实时生效

```
时刻 T=60s: 运维执行配置改变

$ cockroach sql -e "SET CLUSTER SETTING kvflowcontrol.enabled = false"

事件链：
─────────

1. Settings 检测到 kvflowcontrol.enabled 的变化
   └─ enabled: true → false

2. 所有监听此设置的 SetOnChange 回调被触发
   ├─ WaitForEvalConfig 的回调被触发
   │  └─ w.notifyChanged() 被调用

3. 在 notifyChanged 内：
   ├─ w.mu.Lock()
   ├─ waitCategory = w.computeCategory()
   │  └─ enabled.Get() = false
   │  └─ 返回 NoWorkWaitsForEval (0)
   │
   ├─ 比较 mu.waitCategory (原为 2) 与新值 (0)
   │  └─ 2 > 0，发生了 decrease
   │  └─ close(mu.waitCategoryDecreasedCh)
   │  └─ 所有在等待 decrease 的客户端被唤醒
   │
   ├─ mu.waitCategory = 0
   └─ w.mu.Unlock()

4. w.watcherMu.Lock()
   ├─ 遍历所有已注册的 watchers
   │  └─ 调用 Store 的回调：cb(NoWorkWaitsForEval)
   │
   └─ Store 回调执行：
      ├─ SetEnforceMaxLen(0 != 2) = SetEnforceMaxLen(true)
      ├─ 原子更新：enforceMaxLen.Store(true)
      │  └─ 所有新的 LoadOrCreate 会立即看到新值
      │
      ├─ 遍历所有现存的 raftReceiveQueue
      │  └─ 每个 queue 的 mu.enforceMaxLen 被设置为 true
      │
      └─ 从下一条消息起，消息数检查被启用

时刻 T=60s+: 新行为生效
  Raft 消息 Append 时：
  ├─ 检查 enforceMaxLen (true)
  ├─ 如果 len(infos) >= maxLen (10)：
  │  └─ 返回 appended=false
  │  └─ 消息被拒绝
  └─ 发送端收到拒绝 → 重试或背压
```

### 4.3 高可用性部署中的行为

```
场景：3 节点集群，升级从 ApplyToElastic → ApplyToAll

时间轴：
──────

T=0: 所有节点处于 ApplyToElastic
  ├─ 节点 1: enforceMaxLen = true (因为 wc=1, 1 != 2)
  ├─ 节点 2: enforceMaxLen = true
  └─ 节点 3: enforceMaxLen = true

T=10s: 运维 SET CLUSTER SETTING kvflowcontrol.mode = 'apply_to_all'
  └─ 设置在 meta 范围上存储，通过 gossip 传播

T=11s: 节点 1 检测到设置变化
  ├─ s.cfg.KVFlowWaitForEvalConfig.notifyChanged() 被触发
  ├─ SetEnforceMaxLen(false) 被调用
  ├─ 此后 Raft 队列不再拒绝消息（依赖 RACv2 发送端控制）
  └─ 集群整体吞吐量可能增加

T=12s: 节点 2 检测到设置变化
  └─ [同上]

T=13s: 节点 3 检测到设置变化
  └─ [同上]

T=15s: 集群稳定在新配置
  └─ 所有节点使用 RACv2 发送端限制，本地 maxLen 关闭
```

### 4.4 观察器触发的其他组件

```
WaitForEvalConfig 的观察者不仅仅是 Store 的 raftRecvQueues：

其他订阅者包括：
────────────

1. RangeController (range_controller.go)
   └─ WaitForEvalConfig 用于决策 push vs pull 模式
   └─ 在 HandleRaftEventRaftMuLocked 中读取当前状态

2. ReplicaRACv2Processor (replica_rac2/processor.go)
   └─ 创建 RangeController 时注入 WaitForEvalConfig
   └─ 用于决定是否为 WaitForEval 分配 tokens

3. 自定义组件（测试或扩展）
   └─ 可以调用 RegisterWatcher 获得状态变化通知
```

---

## 五、Design Patterns：设计模式分析

### 5.1 Observer Pattern - 配置变化通知

```go
观察者模式的参与者：
─────────────────

Subject: WaitForEvalConfig
  ├─ 维护 waitCategory 状态
  ├─ 监听 Settings 变化
  └─ 通知所有 Observers

Observer: WatcherCallback
  └─ func(wc WaitForEvalCategory)
  └─ 在状态改变时被调用

ConcreteObserver: Store 的 RegisterWatcher 回调
  ├─ 执行 SetEnforceMaxLen(...)
  ├─ 可以是 RangeController 的回调
  ├─ 可以是其他流控组件的回调
  └─ 都是同一签名 WatcherCallback

优势：
  ✓ 解耦：WaitForEvalConfig 不需要知道谁在监听
  ✓ 灵活：新组件可以轻松注册
  ✓ 实时：配置变化立即传播
```

### 5.2 State Machine Pattern - WaitForEvalCategory 的三态

```go
┌──────────────────────────────────────────────────┐
│ 三态状态机：WaitForEvalCategory                  │
└──────────────────────────────────────────────────┘

状态 0: NoWorkWaitsForEval
  ├─ 名义：无工作等待 RACv2
  ├─ 何时发生：kvflowcontrol.Enabled = false
  ├─ 含义：流控完全关闭
  └─ 转移：← 仅可从 OnlyElasticWorkWaitsForEval 转移

状态 1: OnlyElasticWorkWaitsForEval
  ├─ 名义：仅弹性工作等待
  ├─ 何时发生：kvflowcontrol.Mode = "apply_to_elastic" && Enabled
  ├─ 含义：备份等受控，SQL 查询自由
  ├─ 特例：Bypass(RegularWorkClass) = true
  └─ 转移：← NoWorkWaitsForEval / AllWorkWaitsForEval

状态 2: AllWorkWaitsForEval
  ├─ 名义：所有工作等待
  ├─ 何时发生：kvflowcontrol.Mode = "apply_to_all" && Enabled
  ├─ 含义：最严格的流控
  └─ 转移：← 仅可从 OnlyElasticWorkWaitsForEval 转移

转移图：
──────

NoWorkWaitsForEval (0)
    ↕
OnlyElasticWorkWaitsForEval (1)
    ↕
AllWorkWaitsForEval (2)

规则：
  • 自己 → 自己：无操作
  • 0 ↔ 1：与 kvflowcontrol.Enabled 变化
  • 1 ↔ 2：与 kvflowcontrol.Mode 变化
  • 0 ↔ 2：不直接转移（总是经过 1）

特殊处理：
  • Decrease (2→1 或 1→0)：触发 close(waitCategoryDecreasedCh)
  • Increase (0→1 或 1→2)：无特殊处理（没有在等待的 requests）
```

### 5.3 Channel-based Signaling Pattern

```go
waitCategoryDecreasedCh 的用途：
────────────────────────────

问题：
  假设系统处于 AllWorkWaitsForEval (2)
  某个长期运行的 WaitForEval 已经被 blocked
  突然配置变更 → NoWorkWaitsForEval (0)

  这个被 blocked 的请求应该立即被唤醒
  因为 constraints 已经放松

解决方案：
  ├─ WaitForEval 代码获取 waitCategoryDecreasedCh
  ├─ 等待消息时同时监听这个 channel
  ├─ 若 channel 被 close，立即重新评估是否还需等待
  │
  └─ 伪代码：
     for {
         wc, ch := w.Current()
         if wc.Bypass(workPriority):
             return false  // 无需等待

         select {
         case <-ch:  // Decrease 事件
             continue  // 重新评估
         case <-tokens.Available():
             return true  // 获得 tokens
         case <-ctx.Done():
             return err  // 被取消
         }
     }

优势：
  ✓ 无需轮询
  ✓ 立即反应配置变化
  ✓ 避免未必要的等待
```

### 5.4 Initialization Pattern - 首次回调保证

```go
RegisterWatcher 的 "first call before return" 保证：
────────────────────────────────────────────────

问题：
  调用者无法确定调用时的系统状态
  可能导致初始化不一致

解决方案：
  RegisterWatcher(cb) 在返回前立即调用 cb(current_state)

好处：
  ├─ 调用者知道确切的初始状态
  ├─ 不存在"还没被初始化"的情况
  ├─ 后续的 notifyChanged 回调保持一致性
  └─ 简化调用者的逻辑

应用于 Store：
  ├─ Store.NewStore() 调用 RegisterWatcher
  ├─ 回调立即执行一次
  ├─ SetEnforceMaxLen 立即被调用，初始值就是正确的
  ├─ Store 不需要手动初始化 enforceMaxLen
  └─ 后续的配置变化通过 notifyChanged 保持同步
```

---

## 六、Concrete Examples：具体场景分析

### 6.1 示例 1：完全部署集群的典型行为

```
集群配置（生产推荐）：
  kvflowcontrol.Enabled = true
  kvflowcontrol.Mode = "apply_to_all"

时刻 T=0: 节点 1 启动
──────────────────
NewStore() 被调用
  ├─ Line 1561-1565: 创建 raftRecvQueues.mon (UnlimitedMonitor)
  ├─ Line 1566: RegisterWatcher 被调用
  │  └─ 回调立即执行
  │  └─ cb(wc):
  │     - wc = AllWorkWaitsForEval (2)
  │     - SetEnforceMaxLen(2 != 2) → false
  │     - enforceMaxLen.Store(false)
  │
  └─ Store 启动完成

行为观察（T=1s 后）：
──────────────────
Raft 消息源源不断到达
  ├─ 消息 1 (4KB): Append 到 Range 1
  │  ├─ 检查 enforceMaxLen (false) → 跳过长度检查
  │  ├─ q.acc.Grow(4096) → 成功
  │  └─ 消息被加入队列 len=1
  │
  ├─ 消息 2-10 (各 4KB): 依次 Append
  │  ├─ 所有通过长度检查
  │  └─ 队列增长 len=10
  │
  ├─ 消息 11 (4KB): 即使是第 11 条
  │  ├─ 检查 enforceMaxLen (false) → 跳过检查
  │  ├─ q.acc.Grow(4096) → 成功
  │  └─ 队列继续增长 len=11
  │
  └─ Raft Scheduler 处理消息
     ├─ 10ms 后 Drain 消息
     ├─ q.acc.Clear() → 释放所有字节
     └─ 队列回到 len=0

整体效果：
────────
  ✓ Raft 队列不再作为流控屏障
  ✓ 所有流控由 RACv2 发送端的 token 池控制
  ✓ 吞吐量最高，延迟最低
  ✓ 适合高性能集群
```

### 6.2 示例 2：向后兼容的过渡

```
集群升级场景：
  阶段 1 (Day 1): ApplyToElastic 模式
  阶段 2 (Day 7): 升级到 ApplyToAll 模式

时刻 T=Day1 12:00 (启用 ApplyToElastic)
──────────────────────────────────────
SET CLUSTER SETTING kvflowcontrol.mode = 'apply_to_elastic'

Node 1 的 WaitForEvalConfig 检测到变化：
  ├─ computeCategory()
  │  └─ enabled = true
  │  └─ mode = "apply_to_elastic"
  │  └─ 返回 OnlyElasticWorkWaitsForEval (1)
  │
  ├─ 通知 watchers: cb(1)
  │
  └─ Store 的回调：
     ├─ SetEnforceMaxLen(1 != 2) → true
     ├─ enforceMaxLen.Store(true)
     └─ 所有现存 raftReceiveQueue.mu.enforceMaxLen = true

行为观察（Day1 12:01）：
─────────────────────
Raft 消息到达
  ├─ 消息 1-9: 正常 Append
  │  └─ len=9
  │
  ├─ 消息 10: 到达
  │  ├─ 检查 enforceMaxLen (true)
  │  ├─ 检查 len(infos) < maxLen → 9 < 10 ✓
  │  └─ Append 成功，len=10
  │
  ├─ 消息 11: 到达
  │  ├─ 检查 enforceMaxLen (true)
  │  ├─ 检查 len(infos) >= maxLen → 10 >= 10 ✗
  │  ├─ Append 返回 appended=false
  │  └─ 消息被拒绝，len 仍=10
  │
  ├─ 发送端看到拒绝
  │  └─ 重试或背压
  │
  └─ Raft Scheduler Drain 后
     └─ len 恢复 0，可接受新消息

整体效果（Day1）：
────────────────
  ✓ 本地 maxLen 限制提供额外保护
  ✓ 补偿 RACv2 仅限制弹性工作的不足
  ✓ RegularWork 被限制（虽然 RACv2 不限制它）
  ✗ 一些合法消息被拒绝 → 重试 → 延迟增加
  ✗ 吞吐量低于 ApplyToAll


时刻 T=Day7 10:00 (升级到 ApplyToAll)
─────────────────────────────────────
SET CLUSTER SETTING kvflowcontrol.mode = 'apply_to_all'

Node 1 的 WaitForEvalConfig 检测到变化：
  ├─ computeCategory()
  │  └─ enabled = true
  │  └─ mode = "apply_to_all"
  │  └─ 返回 AllWorkWaitsForEval (2)
  │
  ├─ 检测 1 < 2 (增加约束，非 decrease)
  │  └─ 不关闭 waitCategoryDecreasedCh
  │
  ├─ 通知 watchers: cb(2)
  │
  └─ Store 的回调：
     ├─ SetEnforceMaxLen(2 != 2) → false
     ├─ enforceMaxLen.Store(false)
     └─ 所有现存 raftReceiveQueue.mu.enforceMaxLen = false

行为观察（Day7 10:01）：
──────────────────────
新到达的 Raft 消息
  ├─ 不再检查 enforceMaxLen
  ├─ 消息被接受（如果内存允许）
  └─ 背压由 RACv2 发送端的 token 池控制

整体效果（Day7+）：
────────────────
  ✓ 消息不被本地拒绝
  ✓ 吞吐量恢复，延迟减少
  ✓ 系统完全由 RACv2 控制
  ✓ 最优性能状态
```

### 6.3 示例 3：流控禁用（紧急降级）

```
紧急情况：
  运维发现 RACv2 存在问题 → 需要立即禁用

SET CLUSTER SETTING kvflowcontrol.enabled = false

所有节点的 WaitForEvalConfig 响应：
─────────────────────────────────
Node 1:
  ├─ computeCategory()
  │  └─ enabled = false
  │  └─ 返回 NoWorkWaitsForEval (0)
  │
  ├─ 比较 2 > 0 (decrease!)
  │  ├─ close(mu.waitCategoryDecreasedCh)  // 唤醒所有 WaitForEval 客户端
  │  └─ 创建新 channel
  │
  ├─ 通知 watchers: cb(0)
  │
  └─ Store 的回调：
     ├─ SetEnforceMaxLen(0 != 2) → true
     ├─ enforceMaxLen.Store(true)
     └─ 恢复本地 maxLen 检查

行为观察：
────────
新到达的 Raft 消息
  ├─ 恢复本地 maxLen 检查 (enforceMaxLen=true)
  ├─ 超过 10 条的消息被拒绝
  └─ 系统进入"保守模式"

WaitForEval 客户端反应：
  ├─ 监听到 waitCategoryDecreasedCh 被 close
  ├─ 重新评估是否需要等待
  ├─ 由于 NoWorkWaitsForEval，不再等待
  └─ 请求立即被承认

整体效果（禁用后）：
─────────────────
  ✓ RACv2 完全不活跃
  ✓ 系统回到基础本地保护
  ✓ 避免了 RACv2 bug 的影响
  ✗ 吞吐量大幅下降
  ✗ 仅作为临时应急措施
```

---

## 七、Trade-offs：设计权衡

### 7.1 动态配置 vs 静态配置

```
┌─────────────────────────────────────┐
│ 选择 1: 静态配置（部署时确定）       │
├─────────────────────────────────────┤
│ 优点：                              │
│ ✓ 简单：无需运行时观察者            │
│ ✓ 预测性：行为确定                  │
│ ✓ 性能：无观察者回调开销            │
│                                     │
│ 缺点：                              │
│ ✗ 僵化：配置改变需要重启            │
│ ✗ 风险：不能快速应急降级            │
│ ✗ 运维：难以平滑升级                │
└─────────────────────────────────────┘

┌─────────────────────────────────────┐
│ 选择 2: 动态配置（当前设计）        │
├─────────────────────────────────────┤
│ 优点：                              │
│ ✓ 灵活：无需重启即可改变行为        │
│ ✓ 风险低：可立即应急降级            │
│ ✓ 渐进式：支持平滑升级              │
│ ✓ 易控：运维有完全控制力            │
│                                     │
│ 缺点：                              │
│ ✗ 复杂：需要并发安全机制            │
│ ✗ 性能：观察者回调有成本            │
│ ✗ 调试：运行时状态变化难以追踪      │
└─────────────────────────────────────┘

CockroachDB 的选择：动态配置
原因：
  1. Raft 流控是关键路径，bug 需要快速修复
  2. RACv2 升级是渐进式，需要中间阶段
  3. 零停机升级是生产需求
  4. 性能开销可接受（仅在配置变更时触发）
```

### 7.2 观察者回调的并发成本

```
观察者机制的成本分析：
──────────────────

成本 1: RegisterWatcher 时的初始回调
  └─ 一次性，Store 启动时发生
  └─ 成本：~1μs (一个函数调用 + 一个简单操作)

成本 2: 配置变更时的 notifyChanged
  └─ 频率：稀少，通常每周 0-1 次
  └─ 成本：~10μs (两个 Lock，回调遍历)
  └─ 影响：完全在 Settings 变更路径上，不在 Raft 热路

成本 3: Raft 消息 Append 时的 enforceMaxLen 检查
  └─ 频率：每条 Raft 消息
  └─ 成本：~10ns (原子读)
  └─ 影响：与 Append 的其他成本相比可忽略

总体：
  ✓ 内存开销：~1KB (少量回调列表)
  ✗ 单位延迟：0 (观察者都在后台)
  ✓ 吞吐量影响：0 (不在 Raft 处理路径的关键部分)

结论：性能权衡是可接受的
```

### 7.3 Observer 模式 vs Polling

```
┌──────────────────────────────────────┐
│ 方案 A: Polling（定期检查）           │
├──────────────────────────────────────┤
│ Raft scheduler 定期调用：            │
│   wc, _ := cfg.WaitForEvalConfig.Current()
│   if enforce_mode_changed:
│       raftRecvQueues.SetEnforceMaxLen(...)
│
│ 优点：                               │
│ ✓ 简单：代码少                       │
│ ✗ 延迟：配置改变后需等下一个 tick     │
│ ✗ 浪费：即使无变化也检查              │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│ 方案 B: Observer（当前设计）         │
├──────────────────────────────────────┤
│ 在 RegisterWatcher 时声明感兴趣      │
│ 配置改变时立即被通知                 │
│
│ 优点：                               │
│ ✓ 低延迟：立即响应，无 tick 等待     │
│ ✓ 高效：仅在变化时触发               │
│ ✓ 推送模型：更好的实时性             │
│ ✗ 复杂：需要竞态处理                 │
└──────────────────────────────────────┘

CockroachDB 的选择：Observer
原因：
  1. 配置变更需要立即生效
  2. 集群规模大，轮询成本累加
  3. Settings 已提供 SetOnChange 机制
  4. 并发成本值得，换来实时性
```

### 7.4 waitCategoryDecreasedCh 的设计

```
问题：
  WaitForEval 的客户端可能会被 blocked
  配置改变（constraints 放松）时需要被唤醒

┌────────────────────────────────────────┐
│ 方案 A: Broadcast channel（每次 close）│
├────────────────────────────────────────┤
│ 每次配置改变时：
│   close(ch)
│   ch = make(chan struct{})
│
│ 优点：
│ ✓ 简单：close 能唤醒所有监听者
│ ✓ 高效：不需循环遍历
│
│ 缺点：
│ ✗ 频繁创建：每次改变都 new channel
│ ✗ 竞态：需精心处理 close 时序
│ ✓ 仅在 decrease 时触发（设计巧妙）
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│ 方案 B: Condition variable             │
├────────────────────────────────────────┤
│ Broadcast() 唤醒所有等待者
│
│ 优点：
│ ✓ 标准：Go 标准库支持
│
│ 缺点：
│ ✗ 频繁 spurious wakeups
│ ✗ 需要轮询检查
│ ✗ 性能不如 channel
└────────────────────────────────────────┘

CockroachDB 的选择：Broadcast channel + decrease 优化
优势：
  1. 利用 close() 广播能力
  2. 仅在 constraint decrease 时触发（大多数时间不触发）
  3. 避免了不必要的 spurious wakeups
  4. Go channel 原生支持，无需外部库
```

---

## 八、Mental Model：心智模型

### 8.1 流控"模式阶跃"的类比

```
类比：飞机的自动驾驶系统
═══════════════════════

现实场景：
  飞机起飞时使用 AP1（基础自动驾驶）
  cruising 高度时升级到 AP2（高级自动驾驶）
  紧急时候回退到 AP0（完全手动）

CockroachDB 的流控类比：
  启动：NoWorkWaitsForEval (0) - 无限制（无 RACv2）
    │
    ├─ 升级：OnlyElasticWorkWaitsForEval (1) - 基础限制
    │  └─ 备份等弹性工作被控制
    │  └─ SQL 查询不受限
    │
    ├─ 升级：AllWorkWaitsForEval (2) - 完全限制
    │  └─ 所有工作都受 token 限制
    │  └─ 最优化状态
    │
    └─ 降级：NoWorkWaitsForEval (0) - 应急模式
       └─ 流控完全禁用
       └─ 系统进入"保守"模式

转移的特点：
  ✓ 单向升级路径：0 → 1 → 2
  ✗ 降级需要经过中间阶段：2 → 1 → 0
  ✓ 无需停机的平滑转移
  ✓ 每个阶段都是稳定的
```

### 8.2 "观察者"在系统中的位置

```
想象一个电台节目系统：
════════════════════

电台（WaitForEvalConfig）：
  ├─ 播放不同频率的节目（WaitForEvalCategory）
  ├─ 维护节目表（watchers 列表）
  └─ 当节目改变时通知所有听众

听众 1（Store.raftRecvQueues）：
  ├─ 订阅节目变化事件
  ├─ 根据新频率调整接收器（SetEnforceMaxLen）
  └─ 影响本地 Raft 消息处理

听众 2（RangeController）：
  ├─ 也订阅同样的事件
  ├─ 根据 WaitForEvalCategory 决策 push vs pull 模式
  └─ 影响副本间流控

监听者的特点：
  ✓ 不知道其他听众存在
  ✓ 独立地响应节目变化
  ✓ 不需要同步
  ✗ 但通过观察者机制自动保持一致

优势：
  ✓ 每个监听者的逻辑独立
  ✓ 添加新监听者无需修改现存代码
  ✓ 分布式的决策，集中的信息源
```

### 8.3 状态转移的"梯级"特性

```
想象一座楼梯：
════════════

楼梯的三个阶段：
  ├─ 第 0 层：NoWorkWaitsForEval（地下室，最自由）
  ├─ 第 1 层：OnlyElasticWorkWaitsForEval（一楼，部分限制）
  └─ 第 2 层：AllWorkWaitsForEval（顶楼，最严格）

上楼时（增强约束）：
  0 → 1 → 2
  └─ 逐步增加限制
  └─ 已在 1 的人不需要特别反应（他们继续等待）
  └─ 无需通知所有已等待的请求

下楼时（放松约束）：
  2 → 1 → 0
  └─ 逐步减少限制
  └─ 需要唤醒在等待的请求（他们可能不再需要等）
  └─ 通过 close(waitCategoryDecreasedCh) 实现

代码体现：
  if w.mu.waitCategory > waitCategory:  // Decrease
      close(w.mu.waitCategoryDecreasedCh)  // 唤醒所有人
      w.mu.waitCategoryDecreasedCh = make(chan struct{})
  // 若 Increase，什么都不做
```

### 8.4 RegisterWatcher 的"即时性"保证

```
想象一个公告栏：
════════════

旧的方式（轮询）：
  1. 你来看公告栏
  2. 什么都没写
  3. 2 分钟后再看
  4. 现在有内容了
  ✗ 问题：可能错过前 2 分钟发生的事

新的方式（观察者）：
  1. 你向公告栏注册
  2. 公告栏立即告诉你最新信息："当前是 Mode=2"
  3. 你记下这个信息
  4. 今后任何变化都会立即通知你
  ✓ 优点：永不错过任何信息，包括初始值

代码体现：
  RegisterWatcher(func(wc WaitForEvalCategory) {
      // 这个回调现在被调用
      // wc 就是当前的值
      // 您已获得初始状态
      SetEnforceMaxLen(wc != AllWorkWaitsForEval)
  })
```

---

## 九、总结与关键洞察

### 9.1 核心机制的三层理解

```
Layer 1: 什么变化了？
  ├─ 集群管理员改变 kvflowcontrol settings
  ├─ Mode: "apply_to_elastic" → "apply_to_all"
  └─ 系统需要从阶段 1 升级到阶段 2

Layer 2: 如何被检测？
  ├─ Settings.SetOnChange 回调链
  ├─ notifyChanged() 中 computeCategory() 重新计算
  └─ 产生新的 WaitForEvalCategory 值

Layer 3: 如何被应用？
  ├─ 通知所有观察者（Store、RangeController 等）
  ├─ 每个观察者根据新值调整自己的行为
  ├─ Store: SetEnforceMaxLen(...)
  ├─ RangeController: 改变 push/pull 决策
  └─ 新的 Raft 消息立即使用新策略
```

### 9.2 为什么这个设计很重要

```
1. 零停机升级
   └─ 无需重启 Store 或 Node
   └─ 配置改变 → 立即生效
   └─ 完全符合生产可用性需求

2. 快速应急
   └─ RACv2 如果出现 bug → 立即禁用
   └─ SET CLUSTER SETTING → 全集群响应
   └─ 无需等待任何时间窗口

3. 渐进式升级
   └─ 不是非此即彼的二元选择
   └─ 支持 0 → 1 → 2 的中间阶段
   └─ 降低升级风险，易于观察问题

4. 自动化
   └─ 不需人工干预
   └─ Settings 驱动的决策
   └─ 从配置到行为的自动映射
```

### 9.3 与其他组件的关系图

```
┌─────────────────────────────────────────────────────┐
│ KVFlowWaitForEvalConfig (配置决策中枢)              │
├─────────────────────────────────────────────────────┤
│                                                     │
│  输入：                          输出：            │
│  ├─ kvflowcontrol.Enabled       ├─ Observer cb    │
│  ├─ kvflowcontrol.Mode          └─ WaitForEval    │
│  └─ Settings.SetOnChange                           │
│         ↓                               ↓           │
│   computeCategory()  ←────────→  notifyChanged()   │
│         ↓                               ↓           │
│   WaitForEvalCategory             RangeController  │
│   (0/1/2)                         raftRecvQueues   │
│                                   其他观察者       │
│                                                     │
│  并发安全机制：                                     │
│  ├─ mu: 保护 waitCategory 计算与更新               │
│  ├─ watcherMu: 保护观察者列表                      │
│  └─ waitCategoryDecreasedCh: 唤醒等待者            │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### 9.4 代码导航

| 文件 | 行号 | 作用 |
|------|------|------|
| [store.go](pkg/kv/kvserver/store.go) | 1561-1565 | 创建 raftRecvQueues.mon |
| [store.go](pkg/kv/kvserver/store.go) | 1566-1576 | 注册观察器，决定 enforceMaxLen |
| [wait_for_eval_config.go](pkg/kv/kvserver/kvflowcontrol/rac2/wait_for_eval_config.go) | 36-48 | WaitForEvalConfig 结构 |
| [wait_for_eval_config.go](pkg/kv/kvserver/kvflowcontrol/rac2/wait_for_eval_config.go) | 53-64 | 初始化与监听设置变化 |
| [wait_for_eval_config.go](pkg/kv/kvserver/kvflowcontrol/rac2/wait_for_eval_config.go) | 68-105 | notifyChanged 并发安全通知 |
| [wait_for_eval_config.go](pkg/kv/kvserver/kvflowcontrol/rac2/wait_for_eval_config.go) | 116-129 | computeCategory 状态决策 |
| [wait_for_eval_config.go](pkg/kv/kvserver/kvflowcontrol/rac2/wait_for_eval_config.go) | 134-142 | RegisterWatcher 首次回调保证 |
| [kvflowcontrol.go](pkg/kv/kvserver/kvflowcontrol/kvflowcontrol.go) | 26-50 | Mode 和 Enabled 设置定义 |
| [store_raft.go](pkg/kv/kvserver/store_raft.go) | 142-146 | raftReceiveQueues 结构定义 |
| [store_raft.go](pkg/kv/kvserver/store_raft.go) | 199-209 | SetEnforceMaxLen 实现 |

---

**文档完成**。本章系统地分析了 CockroachDB 中 KVFlowWaitForEvalConfig 观察器如何驱动 Store 初始化和 RACv2 模式联动，特别强调了动态配置、观察者模式和并发安全机制在零停机升级中的关键作用。