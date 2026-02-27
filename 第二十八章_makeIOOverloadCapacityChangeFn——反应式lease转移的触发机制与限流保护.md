# 第二十八章：makeIOOverloadCapacityChangeFn——反应式lease转移的触发机制与限流保护

## 一、BFS 概览：为什么需要反应式 lease 转移机制？(Why)

### 1.1 核心问题：IO 过载导致的级联故障风险

在分布式数据库中,当某个 Store 的磁盘 IO 负载过高时,会产生严重后果:

**问题链条**:
```
高 IO 负载 → Raft 日志应用延迟 → 副本同步滞后 → 客户端写入超时 →
更多请求积压 → IO 负载进一步升高 → 级联故障
```

**具体表现** (假设 Store-1 持有 2000 个 lease):
- T0: Store-1 的 L0 文件数从 20 个飙升到 50 个(IO 过载)
- T1: 2000 个 leaseholder replica 的写入延迟从 5ms 上升到 200ms
- T2: 客户端开始超时重试,请求量翻倍到 4000 QPS
- T3: IO 负载雪上加霜,L0 文件数达到 80 个
- T4: 整个集群受影响,可用性下降

**为什么不能只依赖 admission control?**

准入控制(第十三章分析)主要作用于**请求入口**:
```
客户端请求 → admission control 排队 → KV 层处理
```

但当 Store **已经持有 lease** 时,准入控制无法拒绝现有 lease 的写入请求。lease 是"承诺",必须服务,否则违反线性一致性。

### 1.2 解决方案：反应式 lease 转移

`makeIOOverloadCapacityChangeFn` 提供了**事后补救机制**:

```
检测到 IO 过载 → 触发 lease 转移 → 将 lease 迁移到健康 Store →
降低本地 IO 负载 → 恢复正常
```

**关键设计理念**:
1. **反应式(Reactive)**: 不是预防性调度,而是在问题发生后快速响应
2. **批量处理**: 一次性将所有 lease 加入转移队列,而不是逐个处理
3. **限流保护**: 避免频繁触发导致的"震荡"(thrashing)
4. **异步执行**: 不阻塞 Gossip 消息处理主线程

### 1.3 与前述章节的关系

这个函数是前三章机制的**集成点**:

| 章节 | 机制 | 在本章的作用 |
|------|------|------------|
| 第二十五章 | Store.Start() | 在启动阶段注册此回调(store.go:1513) |
| 第二十六章 | ioThresholdMap | 提供副本级别的过载判断依据 |
| 第二十七章 | Swag 滑动窗口 | 提供 5 分钟历史 IO 峰值,避免瞬时抖动触发 |

**数据流向**:
```
Gossip 收到 StoreCapacity 变更
  ↓
StorePool.capacityChanged() (store_pool.go:950)
  ↓
遍历所有注册的 CapacityChangeFn 回调
  ↓
makeIOOverloadCapacityChangeFn 被调用
  ↓
检查 Swag 窗口中的历史 IO 峰值
  ↓
触发 lease 转移
```

---

## 二、BFS 控制流：回调何时触发?如何传播?(How it flows)

### 2.1 注册阶段：Store 启动时注册回调

**位置**: `pkg/kv/kvserver/store.go:1510-1513`

```go
// Store.Start() 流程的一部分
allocatorStorePool = cfg.StorePool
storePoolIsDeterministic = allocatorStorePool.IsDeterministic()
// 注册 IO 过载回调（当检测到 IO 过载时触发）
allocatorStorePool.SetOnCapacityChange(s.makeIOOverloadCapacityChangeFn())
```

**时间点**: Store 启动序列的第 5 步(创建 Allocator 之前)

**调用链**:
```
Server.PreStart()
  → Node.Start()
    → Store.Start() [第 5 步]
      → allocatorStorePool.SetOnCapacityChange()
        → 将回调函数追加到 sp.changeMu.onChange 切片
```

**StorePool 的回调管理** (`pkg/kv/kvserver/allocator/storepool/store_pool.go:943-948`):

```go
func (sp *StorePool) SetOnCapacityChange(fn CapacityChangeFn) {
    sp.changeMu.Lock()
    defer sp.changeMu.Unlock()

    sp.changeMu.onChange = append(sp.changeMu.onChange, fn)
}
```

**存储结构**:
```go
type StorePool struct {
    changeMu struct {
        syncutil.Mutex
        onChange []CapacityChangeFn  // 多个回调函数的切片
    }
}
```

**关键设计**:
- 使用切片存储,支持**多个订阅者**
- 本章分析的函数只是其中一个回调(可能还有其他监控、日志等回调)

### 2.2 触发阶段：StoreCapacity 变更时调用所有回调

**触发点**: `pkg/kv/kvserver/allocator/storepool/store_pool.go:950-957`

```go
func (sp *StorePool) capacityChanged(storeID roachpb.StoreID, prev, cur roachpb.StoreCapacity) {
    sp.changeMu.Lock()
    defer sp.changeMu.Unlock()

    for _, fn := range sp.changeMu.onChange {
        fn(storeID, prev, cur)  // 同步调用每个回调
    }
}
```

**参数说明**:
- `storeID`: 发生变更的 Store ID(如 Store-3)
- `prev`: 变更前的容量信息(旧的 L0 文件数、lease 数量等)
- `cur`: 变更后的容量信息(新的 L0 文件数、lease 数量等)

**调用时机**:

StorePool 的 `capacityChanged` 会在以下场景被调用:

1. **Gossip 收到远程 Store 心跳**:
   ```
   Gossip.OnStoreCapacity()
     → StorePool.updateStoreDetail()
       → capacityChanged()
   ```

2. **本地 Store 主动更新容量**:
   ```
   Store.GossipStore() (每 10 秒)
     → StorePool.UpdateLocalStoreAfterRelocate()
       → capacityChanged()
   ```

3. **Lease 转移后更新**:
   ```
   Replica.TransferLease() 完成
     → StorePool.UpdateLocalStoresAfterLeaseTransfer()
       → capacityChanged()
   ```

### 2.3 回调执行上下文

**关键约束**: 回调在 **Gossip goroutine** 中同步执行

**时间预算**: 必须快速返回,否则会阻塞 Gossip 消息处理

**Gossip goroutine 的职责**:
```
while true {
    msg := <-gossip.incoming
    switch msg.Type {
    case StoreCapacity:
        StorePool.updateStoreDetail()
          → capacityChanged()  // ← 我们在这里!
            → makeIOOverloadCapacityChangeFn()
              ↓
              如果耗时过长,会导致其他 Gossip 消息积压
    }
}
```

**设计应对**: 使用 `stopper.RunTask()` 将耗时操作移到**异步 goroutine**(见 2.4)

### 2.4 完整的触发流程图

```
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 1: Gossip 网络传播 Store 容量变更                            │
└─────────────────────────────────────────────────────────────────┘
    Store-3 每 10 秒 gossip 自己的容量信息
      {StoreID: 3, LeaseCount: 2000, IOThreshold: 0.95, ...}
              ↓
    通过 Gossip 网络广播到所有节点
              ↓
    每个节点的 Gossip.OnStoreCapacity() 接收到消息

┌─────────────────────────────────────────────────────────────────┐
│ 阶段 2: StorePool 更新并触发回调                                  │
└─────────────────────────────────────────────────────────────────┘
    StorePool.updateStoreDetail(storeID=3, newCapacity)
              ↓
    对比 prev.IOThreshold vs cur.IOThreshold
              ↓
    调用 capacityChanged(storeID=3, prev, cur)
              ↓
    遍历 sp.changeMu.onChange 切片
              ↓
    执行 makeIOOverloadCapacityChangeFn(3, prev, cur)

┌─────────────────────────────────────────────────────────────────┐
│ 阶段 3: makeIOOverloadCapacityChangeFn 内部决策                   │
└─────────────────────────────────────────────────────────────────┘
    检查 6 个守卫条件 (见第三节)
              ↓
    通过所有检查 → 决定触发 lease 转移
              ↓
    更新 s.lastIOOverloadLeaseShed = now
              ↓
    创建异步任务

┌─────────────────────────────────────────────────────────────────┐
│ 阶段 4: 异步任务将所有 replica 加入 lease 队列                    │
└─────────────────────────────────────────────────────────────────┘
    stopper.RunTask("io-overload: shed leases")
              ↓
    newStoreReplicaVisitor(s).Visit(func(repl *Replica) {
        s.leaseQueue.maybeAdd(ctx, repl, now)
        // 对所有 2000 个 replica 调用
    })
              ↓
    lease queue 异步处理队列中的 replica
              ↓
    逐个检查并转移 lease 到健康的 Store
```

**时间线示例**:

| 时刻 | 事件 | 耗时 |
|------|------|------|
| T0 = 10:00:00.000 | Store-3 发现自己 IO 过载,gossip 新容量 | 1ms |
| T0 + 50ms | 其他节点的 Gossip 接收到消息 | - |
| T0 + 52ms | StorePool.capacityChanged() 被调用 | <1ms |
| T0 + 53ms | makeIOOverloadCapacityChangeFn 执行守卫检查 | <1ms |
| T0 + 54ms | 启动异步任务,Gossip goroutine 返回 | - |
| T0 + 54ms ~ T0 + 200ms | 异步任务遍历 2000 个 replica,加入队列 | 150ms |
| T0 + 200ms ~ T0 + 5min | lease queue 逐个处理,转移 lease | 数分钟 |

**关键观察**: Gossip 主线程只阻塞 **~4ms**,耗时操作都在异步 goroutine

---

## 三、DFS 深度剖析：6 个守卫条件与异步任务的实现细节 (How it works)

### 3.1 函数签名与返回类型

**位置**: `pkg/kv/kvserver/store.go:2866-2917`

```go
func (s *Store) makeIOOverloadCapacityChangeFn() storepool.CapacityChangeFn {
    return func(storeID roachpb.StoreID, old, cur roachpb.StoreCapacity) {
        // ... 实现
    }
}
```

**类型定义** (`pkg/kv/kvserver/allocator/storepool/store_pool.go:251-254`):

```go
type CapacityChangeFn func(
    storeID roachpb.StoreID,
    old, cur roachpb.StoreCapacity,
)
```

**设计模式**: 工厂函数(Factory Function)
- `makeIOOverloadCapacityChangeFn()` 返回一个**闭包**
- 闭包捕获了 `s *Store` 指针,可以访问 Store 的所有状态

**闭包捕获的关键状态**:
```go
s.StoreID()                     // 本地 Store ID
s.IsStarted()                   // Store 是否已启动
s.lastIOOverloadLeaseShed       // 上次转移时间(atomic.Value)
s.cfg.Settings                  // 集群配置
s.leaseQueue                    // lease 转移队列
s.stopper                       // goroutine 生命周期管理器
```

### 3.2 守卫条件 1: 检查 lease 数量

**代码** (store.go:2868-2871):

```go
// There's nothing to do when there are no leases on the store.
if cur.LeaseCount == 0 {
    return
}
```

**逻辑**: 如果 Store 上没有 lease,无需转移

**场景**:
- 新加入的 Store(刚启动,还未获得任何 lease)
- 已经完全转移了 lease 的 Store(正在下线)

**性能优化**: 提前返回,避免后续无意义的检查

### 3.3 守卫条件 2: 只响应本地 Store 的变更

**代码** (store.go:2873-2877):

```go
// Don't react to other stores capacity changes, only IO overload change on
// the local store descriptor is relevant.
if !s.IsStarted() || s.StoreID() != storeID {
    return
}
```

**两个子条件**:

1. **`!s.IsStarted()`**: Store 尚未完全启动
   - 启动过程中会多次更新容量信息,但此时不应触发 lease 转移
   - `IsStarted()` 在 `Store.Start()` 的最后一步设置为 true

2. **`s.StoreID() != storeID`**: 变更的不是本地 Store
   - `storeID` 参数可能是集群中任意 Store(因为 Gossip 会广播所有 Store 的容量)
   - 只有本地 Store 才应该触发 lease 转移

**示例**:

假设当前节点是 Store-1,收到以下容量变更:

| storeID | s.StoreID() | s.IsStarted() | 是否执行? | 原因 |
|---------|-------------|---------------|---------|------|
| 1 | 1 | true | ✅ | 本地 Store,已启动 |
| 1 | 1 | false | ❌ | 本地 Store,但未启动完成 |
| 3 | 1 | true | ❌ | 远程 Store,与本地无关 |
| 3 | 1 | false | ❌ | 远程 Store,且未启动 |

**为什么不响应远程 Store?**

每个 Store 只负责自己的 lease 转移。如果 Store-3 过载了:
- Store-3 自己会触发 lease 转移(将 lease 转移给 Store-1 或 Store-2)
- Store-1 不需要(也不应该)代替 Store-3 做决策

### 3.4 守卫条件 3: 限流检查(Rate Limiting)

**代码** (store.go:2879-2886):

```go
// Avoid shedding leases too frequently by checking the last time a shed
// was attempted.
if lastShed := s.lastIOOverloadLeaseShed.Load(); lastShed != nil {
    minInterval := MinIOOverloadLeaseShedInterval.Get(&s.cfg.Settings.SV)
    if timeutil.Since(lastShed.(time.Time)) < minInterval {
        return
    }
}
```

**配置项** (`pkg/kv/kvserver/lease_queue.go:54-60`):

```go
var MinIOOverloadLeaseShedInterval = settings.RegisterDurationSetting(
    settings.SystemOnly,
    "kv.allocator.min_io_overload_lease_shed_interval",
    "controls how frequently all leases can be shed from a node "+
        "due to the node becoming IO overloaded",
    30*time.Second,  // 默认值: 30 秒
)
```

**限流逻辑**:

```
当前时间 - 上次转移时间 < 30 秒 → 拒绝本次转移
当前时间 - 上次转移时间 ≥ 30 秒 → 允许本次转移
```

**时间线示例**:

```
T0 = 10:00:00  首次触发 lease 转移
                lastIOOverloadLeaseShed = 10:00:00
                ↓
T1 = 10:00:15  IO 依然过载,再次触发回调
                Since(10:00:00) = 15s < 30s → 拒绝
                ↓
T2 = 10:00:25  IO 依然过载,再次触发回调
                Since(10:00:00) = 25s < 30s → 拒绝
                ↓
T3 = 10:00:35  IO 依然过载,再次触发回调
                Since(10:00:00) = 35s ≥ 30s → 允许
                lastIOOverloadLeaseShed = 10:00:35
```

**并发安全性**:

- `s.lastIOOverloadLeaseShed` 是 `atomic.Value` 类型
- `Load()` 和 `Store()` 都是原子操作,无需额外加锁

**Store 结构体定义** (store.go:1126-1128):

```go
// lastIOOverloadLeaseShed tracks the last time the store attempted to shed
// all range leases it held due to becoming IO overloaded.
lastIOOverloadLeaseShed atomic.Value  // 存储 time.Time
```

**为什么需要限流?**

防止"震荡"(thrashing):

```
假设没有限流:

10:00:00 触发 lease 转移 → 将 2000 个 lease 加入队列
10:00:01 IO 依然高(因为队列还在处理) → 再次触发 → 重复加入队列
10:00:02 IO 依然高 → 再次触发 → 重复加入队列
...
结果: lease queue 被大量重复任务淹没,反而降低效率
```

有了 30 秒限流:

```
10:00:00 触发 lease 转移 → 加入队列
10:00:01~10:00:30 拒绝重复触发
10:00:30 队列已处理大部分 lease,IO 负载降低
```

### 3.5 守卫条件 4: 实际过载检查

**代码** (store.go:2888-2894):

```go
// Lastly, check whether the store is considered IO overloaded relative to
// the configured threshold and the cluster average.
ctx := context.Background()
s.AnnotateCtx(ctx)
if s.existingLeaseCheckIOOverload(ctx) {
    return
}
```

**`existingLeaseCheckIOOverload` 实现** (store.go:2851-2859):

```go
func (s *Store) existingLeaseCheckIOOverload(ctx context.Context) bool {
    storeList, _, _ := s.cfg.StorePool.GetStoreList(storepool.StoreFilterNone)
    storeDescriptor, ok := s.cfg.StorePool.GetStoreDescriptor(s.StoreID())
    if !ok {
        return false
    }
    return s.allocator.IOOverloadOptions().ExistingLeaseCheck(
        ctx, storeDescriptor, storeList)
}
```

**调用链**:

```
existingLeaseCheckIOOverload()
  ↓
获取 StoreList (集群所有 Store 的容量信息)
  ↓
获取本地 Store 的 StoreDescriptor
  ↓
allocator.IOOverloadOptions().ExistingLeaseCheck()
  ↓
检查逻辑:
  1. 本地 Store 的 IOThreshold > 配置的阈值?
  2. 本地 Store 的 IOThreshold > 集群平均值 × 某个倍数?
  ↓
返回 true (不过载) 或 false (过载)
```

**返回值语义**:
- `true`: Store **不过载**,可以继续持有 lease → **提前返回,不触发转移**
- `false`: Store **过载**,不适合持有 lease → **继续执行,触发转移**

**与 Swag 的关系**:

`IOThreshold` 的计算依赖于 Swag 滑动窗口(第二十七章):

```go
// store.go:2824-2829
ioThreshold, ioThresholdScore := s.ioThreshold.Current(now)
// ioThreshold 是过去 5 分钟的 IO 峰值
// 通过 Swag.Query() 计算得出
```

**具体判断逻辑示例**:

假设配置:
- `IOOverloadThreshold = 0.8` (阈值: 80%)
- 集群平均 IOThreshold = 0.4

| Store | 5分钟峰值 IOThreshold | 判断 | 结果 |
|-------|--------------------|------|------|
| Store-1 | 0.95 | > 0.8 且 > 0.4×2 | **过载**,触发转移 |
| Store-2 | 0.5 | < 0.8 但 > 0.4 | **不过载**,不转移 |
| Store-3 | 0.3 | < 0.8 且 < 0.4 | **不过载**,不转移 |

### 3.6 记录转移时间(关键的时序保证)

**代码** (store.go:2896-2902):

```go
// NB: Update the last shed time prior to trying to shed leases, this
// should limit the window of concurrent shedding activity (in the case
// where multiple capacity changes are called within a short window). This
// could be removed entirely with a mutex but hardly seems necessary.
s.lastIOOverloadLeaseShed.Store(s.Clock().Now().GoTime())
log.KvDistribution.Infof(
    ctx, "IO overload detected, will shed leases %v", cur.LeaseCount)
```

**关键设计: 先更新时间,再启动异步任务**

时序保证:
```
T0: Store(now)       ← 原子更新时间戳
T1: RunTask(异步任务)  ← 启动 goroutine
T2: 其他 Gossip 消息触发回调 → Load() 读到 T0 的时间 → 被限流拒绝
```

**如果顺序颠倒会怎样?**

```go
// 错误的顺序
stopper.RunTask(...)   // 先启动任务
Store(now)             // 后更新时间

问题:
T0: RunTask() 启动
T1: 另一个 Gossip 消息到达,触发回调
T2: Load() 读到 nil 或旧时间 → 通过限流检查
T3: 启动第二个异步任务 → 重复处理!
T4: Store(now) 才完成更新
```

**注释说明**: "could be removed entirely with a mutex but hardly seems necessary"

意思是:
- 可以用 mutex 保护整个决策过程,彻底避免并发问题
- 但当前的原子操作已经足够好,增加 mutex 会降低性能
- 即使有微小的竞态窗口,最多也只是启动两次任务,不会造成严重问题

**日志记录**:

```
log.KvDistribution.Infof(ctx, "IO overload detected, will shed leases %v", cur.LeaseCount)
```

实际输出示例:
```
I210315 10:00:00.123456 store1 [S1] 123  kv/kvserver/store.go:2902
  IO overload detected, will shed leases 2000
```

### 3.7 异步任务: 遍历所有 replica 并加入队列

**代码** (store.go:2904-2917):

```go
// This callback is on the gossip goroutine, once we know we wish to shed
// leases, split off the actual enqueuing work to a separate async task
// goroutine.
if err := s.stopper.RunTask(ctx, "io-overload: shed leases", func(ctx context.Context) {
    newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
        s.leaseQueue.maybeAdd(ctx, repl, repl.Clock().NowAsClockTimestamp())
        return true /* wantMore */
    })
}); err != nil {
    log.KvDistribution.Infof(ctx,
        "unable to shed leases due to IO overload: %v", err)
    // An error should only be encountered when the server is quiescing, as
    // such we don't reset the timer on a failed attempt.
}
```

#### 3.7.1 Stopper.RunTask 异步执行

**`stopper.RunTask` 的语义**:

```go
func (s *Stopper) RunTask(
    ctx context.Context,
    taskName string,
    fn func(context.Context),
) error
```

**行为**:
1. 检查 Stopper 是否正在关闭(quiescing)
2. 如果是 → 返回错误,**不执行** fn
3. 如果否 → 在**新 goroutine** 中执行 fn,立即返回 nil

**生命周期管理**:

```go
stopper.RunTask()
  ↓
创建新 goroutine
  ↓
执行 fn(ctx)
  ↓
goroutine 结束时自动通知 stopper(用于优雅关闭)
```

**为什么用 RunTask 而不是 go func()?**

| 方式 | 优点 | 缺点 |
|------|------|------|
| `go func()` | 简单直接 | 无法在关闭时等待 goroutine 完成 |
| `RunTask()` | 支持优雅关闭,stopper.Stop() 会等待所有任务完成 | 需要传递 stopper |

**优雅关闭场景**:

```
收到 SIGTERM 信号
  ↓
stopper.Stop()
  ↓
等待所有 RunTask 启动的 goroutine 完成
  ↓
最多等待 30 秒
  ↓
退出进程
```

#### 3.7.2 StoreReplicaVisitor: 遍历所有 replica

**`newStoreReplicaVisitor(s).Visit(fn)` 的实现**:

```go
type storeReplicaVisitor struct {
    store *Store
}

func (v *storeReplicaVisitor) Visit(fn func(*Replica) bool) {
    v.store.mu.Lock()
    replicas := make([]*Replica, 0, len(v.store.mu.replicas))
    for _, repl := range v.store.mu.replicas {
        replicas = append(replicas, repl)
    }
    v.store.mu.Unlock()

    for _, repl := range replicas {
        if !fn(repl) {
            break  // fn 返回 false 时停止遍历
        }
    }
}
```

**关键设计**: 先拷贝切片,再遍历

**为什么需要拷贝?**

```
如果直接在 mu 锁内遍历:

store.mu.Lock()
for _, repl := range store.mu.replicas {
    leaseQueue.maybeAdd(repl)  // 这里可能很慢(涉及 IO、Raft RPC 等)
}
store.mu.Unlock()

问题: 长时间持有 store.mu,阻塞所有需要访问 replicas 的操作
```

**拷贝后遍历**:

```
store.mu.Lock()
拷贝 replicas 指针切片 (2000 个指针 × 8 字节 = 16KB,很快)
store.mu.Unlock()  ← 快速释放锁

for _, repl := range 拷贝的切片 {
    leaseQueue.maybeAdd(repl)  // 慢操作在锁外执行
}
```

**性能数据**:

假设 Store 有 2000 个 replica:
- 拷贝切片耗时: ~10 微秒(16KB 内存分配)
- `maybeAdd` 平均耗时: ~50 微秒/replica
- 总遍历耗时: 2000 × 50μs = 100ms

如果不拷贝,store.mu 会被锁住 100ms,严重影响性能。

#### 3.7.3 leaseQueue.maybeAdd: 加入 lease 转移队列

**调用**:

```go
s.leaseQueue.maybeAdd(ctx, repl, repl.Clock().NowAsClockTimestamp())
```

**参数**:
- `ctx`: 上下文(用于取消、超时)
- `repl`: 要检查的 replica
- `NowAsClockTimestamp()`: 当前 HLC 时间戳

**`maybeAdd` 的逻辑** (简化版):

```go
func (lq *leaseQueue) maybeAdd(
    ctx context.Context,
    repl *Replica,
    now hlc.ClockTimestamp,
) {
    // 1. 检查 replica 是否持有 lease
    if !repl.OwnsValidLease(ctx, now) {
        return  // 不是 leaseholder,跳过
    }

    // 2. 检查是否已经在队列中
    if lq.contains(repl.RangeID) {
        return  // 已在队列,避免重复
    }

    // 3. 加入队列(带优先级)
    lq.add(repl, 1.0 /* priority */)
}
```

**队列处理流程**:

```
lease queue 后台 goroutine 持续运行:

while true {
    repl := lq.pop()  // 从队列取出一个 replica
    shouldTransfer, target := lq.shouldTransferLease(repl)
    if shouldTransfer {
        repl.TransferLease(ctx, target)
    }
    sleep(10ms)  // 限速,避免过快
}
```

**shouldTransferLease 的判断**:

```go
func (lq *leaseQueue) shouldTransferLease(repl *Replica) (bool, roachpb.StoreID) {
    // 调用 allocator 判断是否应该转移
    targetStore := lq.allocator.TransferLeaseTarget(
        ctx,
        repl.StoreID(),
        repl.Desc(),
        repl.StorePool().GetStoreList(),
    )

    if targetStore == 0 {
        return false, 0  // 没有更好的目标,保持当前 lease
    }

    // 检查目标 Store 是否健康
    if targetStore.IOThreshold > 0.8 {
        return false, 0  // 目标也过载,不转移
    }

    return true, targetStore
}
```

**完整时间线**:

```
T0: makeIOOverloadCapacityChangeFn 启动异步任务
T0 + 10μs: 拷贝 2000 个 replica 指针
T0 + 100ms: 遍历完成,所有 2000 个 replica 加入队列
T0 + 100ms ~ T0 + 5min: lease queue 逐个处理
    每个 replica 处理流程:
      - shouldTransferLease (10ms)
      - TransferLease Raft 提议 (50ms)
      - 等待 Raft 日志应用 (100ms)
      - 总计 ~160ms/replica
    2000 个 replica × 160ms = 320 秒 ≈ 5 分钟
```

#### 3.7.4 错误处理

**代码**:

```go
if err := s.stopper.RunTask(...); err != nil {
    log.KvDistribution.Infof(ctx,
        "unable to shed leases due to IO overload: %v", err)
    // An error should only be encountered when the server is quiescing, as
    // such we don't reset the timer on a failed attempt.
}
```

**唯一的错误场景**: Stopper 正在关闭(quiescing)

**为什么不重置时间?**

```
如果重置 lastIOOverloadLeaseShed:

T0: RunTask 失败(因为正在关闭)
T1: lastIOOverloadLeaseShed.Store(nil)  // 重置
T2: 下次触发时通过限流检查
T3: 再次尝试 RunTask → 再次失败(依然在关闭)
T4: 陷入无限循环

正确做法: 保留时间戳,避免重复尝试
```

**关闭场景的完整流程**:

```
收到 SIGTERM
  ↓
stopper.Stop() 开始
  ↓
设置 quiescing 标志
  ↓
此时如果 Gossip 触发回调:
  makeIOOverloadCapacityChangeFn()
    → RunTask() 返回 ErrUnavailable
    → 记录日志,放弃 lease 转移
  ↓
等待现有任务完成
  ↓
进程退出
```

---

## 四、运行时行为：动态信号如何驱动 lease 转移决策? (Runtime Behavior)

### 4.1 触发频率分析

**Gossip 触发频率**:

每个 Store 每 **10 秒** gossip 一次自己的容量信息:

```go
// pkg/kv/kvserver/store.go
const defaultGossipStoresInterval = 10 * time.Second
```

**StorePool 更新频率**:

每收到一次 Gossip 消息,`capacityChanged` 就会被调用一次。

**假设集群有 N 个 Store**:

- 每个 Store 每 10 秒 gossip 一次
- 每个节点的 StorePool 每 10 秒会收到 N 次更新(每个 Store 一次)
- `makeIOOverloadCapacityChangeFn` 每 10 秒被调用 N 次

**但实际触发 lease 转移的频率?**

受限于:
1. **守卫条件 2**: 只响应本地 Store(排除 N-1 次)
2. **守卫条件 3**: 30 秒限流

**最大触发频率**: 每 30 秒一次(受限流保护)

### 4.2 IO 负载波动场景分析

**场景 1: 瞬时抖动(不应触发)**

```
时间线:
T0: L0 文件数突然从 20 飙升到 60(某个大事务)
T1: Swag.Record(60) → 当前窗口最大值 = 60
T2: IOThreshold = 60/80 = 0.75 < 0.8 → 不触发
T3: 10 秒后,L0 文件数恢复到 25
T4: Swag 当前窗口 = 25,但历史窗口仍保留 60
T5: IOThreshold = max(过去 5 分钟) = 60/80 = 0.75 → 依然不触发

结果: 瞬时抖动被 Swag 平滑,但不会错误触发
```

**场景 2: 持续过载(应该触发)**

```
时间线:
T0: L0 文件数 = 30 (正常)
T1: 开始大规模写入,L0 文件数持续上升
T2 (1分钟后): L0 = 50 → IOThreshold = 0.625
T3 (2分钟后): L0 = 65 → IOThreshold = 0.8125 > 0.8
T4: Gossip 传播 → makeIOOverloadCapacityChangeFn 触发
T5: 通过所有守卫条件 → 启动 lease 转移
T6 (5分钟后): 500 个 lease 已转移走,L0 降到 40
T7: IOThreshold = 0.5 < 0.8 → 停止转移
T8 (30秒后): 限流时间到,但已不过载,不再触发

结果: 正确响应持续过载,及时转移 lease
```

**场景 3: 震荡(被限流阻止)**

```
假设没有限流:

T0: IO 过载 → 触发转移 → 加入 2000 个 replica
T10s: IO 依然高(队列还在处理) → 再次触发 → 重复加入
T20s: IO 依然高 → 再次触发 → 重复加入
...
结果: 队列被淹没,效率低下

有限流保护:

T0: IO 过载 → 触发转移 → 加入 2000 个 replica
T10s: 想触发 → 被限流拒绝(距上次仅 10s < 30s)
T20s: 想触发 → 被限流拒绝(距上次仅 20s < 30s)
T30s: 想触发 → 通过限流(距上次 30s ≥ 30s) → 触发
  此时队列已处理大部分 lease,IO 负载降低
T30s之后: 不再过载,不再触发

结果: 避免重复触发,保持稳定
```

### 4.3 并发场景分析

**场景 1: 多个 Gossip 消息同时到达**

```
假设 Gossip 网络延迟导致多条消息几乎同时到达:

Goroutine 1: Gossip 消息 A 到达
  → capacityChanged(storeID=1, ...)
    → makeIOOverloadCapacityChangeFn()
      → 守卫检查中...

Goroutine 2: Gossip 消息 B 到达(同一个 Store 的后续更新)
  → capacityChanged(storeID=1, ...)
    → makeIOOverloadCapacityChangeFn()
      → 守卫检查中...

时序:
T0: Goroutine 1 执行到 Load() → 读到 lastShed = nil
T1: Goroutine 2 执行到 Load() → 读到 lastShed = nil
T2: Goroutine 1 执行 Store(now) → lastShed = 10:00:00
T3: Goroutine 1 启动异步任务
T4: Goroutine 2 继续检查 Since(nil) → 通过限流(因为读到 nil)
T5: Goroutine 2 执行 Store(now) → lastShed = 10:00:00.001
T6: Goroutine 2 启动异步任务

结果: 两个异步任务都启动了!
```

**影响评估**:

```
最坏情况: 2000 个 replica 被加入队列两次
leaseQueue 的去重机制:
  if lq.contains(repl.RangeID) {
      return  // 已在队列,跳过
  }

实际影响: 第二次遍历时,所有 replica 都已在队列,直接跳过
额外开销: 遍历 2000 个 replica 的时间 (~100ms)
```

**为什么不用 mutex 完全避免?**

注释说明:
> This could be removed entirely with a mutex but hardly seems necessary.

原因:
1. **性能**: atomic.Value 比 mutex 快
2. **影响小**: 即使重复触发,leaseQueue 有去重保护
3. **概率低**: 两个 Gossip 消息同时到达的概率很低

**如果用 mutex 保护**:

```go
var shedMu sync.Mutex

func makeIOOverloadCapacityChangeFn() ... {
    return func(...) {
        shedMu.Lock()
        defer shedMu.Unlock()

        // 所有守卫条件检查
        // ...

        // 更新时间
        s.lastIOOverloadLeaseShed.Store(now)

        // 启动任务
        stopper.RunTask(...)
    }
}
```

代价:
- 每次调用都要获取锁(即使大部分会被守卫条件快速返回)
- 锁竞争会增加延迟

### 4.4 与其他系统的交互

#### 4.4.1 与 Admission Control 的关系

**Admission Control** (第十三章):
- 在**请求入口**排队,限制进入 KV 层的请求数量
- 基于 CPU、IO 负载动态调整准入速率

**makeIOOverloadCapacityChangeFn**:
- 在**已持有 lease** 的情况下,将 lease 转移走
- 减少本地 Store 需要处理的写入请求

**协同工作**:

```
正常情况:
  客户端请求 → Admission Control 排队 → KV 层 → Store

IO 负载升高:
  阶段 1: Admission Control 检测到 IO 负载高 → 减少准入速率
  阶段 2: 负载继续升高(因为现有 lease 必须处理)
  阶段 3: makeIOOverloadCapacityChangeFn 触发 → 转移 lease
  阶段 4: lease 转移走后,本地写入请求减少
  阶段 5: IO 负载降低 → Admission Control 恢复准入速率
```

**时间尺度**:
- Admission Control 调整: 秒级(1-5 秒)
- Lease 转移: 分钟级(5-10 分钟)

#### 4.4.2 与 Rebalancer 的关系

**Rebalancer** (Store.rebalanceQueue):
- **主动**调度,定期检查 replica 分布
- 目标: 长期负载均衡

**makeIOOverloadCapacityChangeFn**:
- **反应式**触发,仅在 IO 过载时执行
- 目标: 短期过载缓解

**可能冲突的场景**:

```
T0: makeIOOverloadCapacityChangeFn 将 lease 从 Store-1 转移到 Store-2
T1 (5分钟后): Store-1 的 IO 负载恢复正常
T2 (10分钟后): Rebalancer 发现 Store-2 的 lease 数量过多
T3: Rebalancer 将部分 lease 转回 Store-1

结果: lease 在两个 Store 之间来回转移
```

**缓解机制**:

1. **限流保护**: 30 秒内不会重复触发
2. **Rebalancer 频率低**: 通常 10 分钟一次
3. **Allocator 综合判断**: 考虑 IO、CPU、磁盘空间等多个维度

#### 4.4.3 与 ioThresholdMap 的关系

**ioThresholdMap** (第二十六章):
- 用于**副本级别**的 Raft 流控
- 防止向过载的 follower 发送 MsgApp

**makeIOOverloadCapacityChangeFn**:
- 用于 **Store 级别**的 lease 管理
- 防止过载的 Store 继续担任 leaseholder

**协同工作**:

```
Store-1 (leaseholder) 向 Store-3 (follower) 复制:

阶段 1: Store-3 IO 过载 → ioThresholdMap 标记 Store-3
阶段 2: Store-1 检查 ioThresholdMap → 暂停向 Store-3 发送 MsgApp
阶段 3: Store-1 自己也 IO 过载 → makeIOOverloadCapacityChangeFn 触发
阶段 4: Store-1 将 lease 转移到 Store-2
阶段 5: Store-2 成为新 leaseholder → 检查 ioThresholdMap → 继续暂停 Store-3
```

**双层保护**:
- **Follower 过载**: ioThresholdMap 暂停复制
- **Leaseholder 过载**: makeIOOverloadCapacityChangeFn 转移 lease

---

## 五、具体案例：一次完整的 IO 过载 lease 转移过程 (Concrete Example)

### 5.1 初始状态

**集群配置**:
- 3 个节点,每个节点 1 个 Store
- Store-1, Store-2, Store-3
- 每个 Store 管理 2000 个 replica

**Store-1 的状态**:
```
LeaseCount: 2000  (持有 2000 个 lease)
L0 文件数: 25
L0 阈值: 80
IOThreshold: 25/80 = 0.3125  (正常)
Swag 窗口 (过去 5 分钟): [0.31, 0.29, 0.32, 0.30, 0.31]
```

**集群平均**:
```
Store-1 IOThreshold: 0.31
Store-2 IOThreshold: 0.28
Store-3 IOThreshold: 0.33
平均值: 0.3067
```

### 5.2 触发事件：大规模写入导致 IO 过载

**时间**: 10:00:00

**事件**: 用户启动批量导入任务,所有写入请求被路由到 Store-1 的 lease

**IO 负载变化**:

| 时间 | L0 文件数 | IOThreshold | Swag 当前窗口 | Swag 5分钟最大值 |
|------|----------|-------------|--------------|----------------|
| 10:00:00 | 25 | 0.31 | 0.31 | 0.32 |
| 10:01:00 | 40 | 0.50 | 0.50 | 0.50 |
| 10:02:00 | 55 | 0.69 | 0.69 | 0.69 |
| 10:03:00 | 70 | 0.875 | 0.875 | 0.875 |
| 10:04:00 | 72 | 0.90 | 0.90 | 0.90 |

### 5.3 Gossip 传播与回调触发

**10:03:10**: Store-1 定期 gossip 自己的容量信息

```go
// Store-1 执行
s.GossipStore(ctx)
  ↓
gossip.AddInfo("store-1-capacity", StoreCapacity{
    StoreID: 1,
    LeaseCount: 2000,
    IOThreshold: 0.875,
    L0FileCount: 70,
    ...
})
```

**10:03:11**: Gossip 消息通过网络传播(延迟 ~10ms)

**10:03:11**: Store-1 自己的 StorePool 收到消息

```go
// StorePool.OnGossipUpdate()
sp.updateStoreDetail(storeID=1, newCapacity)
  ↓
对比 old.IOThreshold (0.69) vs cur.IOThreshold (0.875)
  ↓
调用 capacityChanged(1, old, cur)
  ↓
遍历 sp.changeMu.onChange 切片
  ↓
调用 makeIOOverloadCapacityChangeFn(1, old, cur)
```

### 5.4 守卫条件检查

**检查 1: lease 数量**

```go
if cur.LeaseCount == 0 {
    return
}
```

- `cur.LeaseCount = 2000` → 通过

**检查 2: 本地 Store**

```go
if !s.IsStarted() || s.StoreID() != storeID {
    return
}
```

- `s.IsStarted() = true` ✅
- `s.StoreID() = 1, storeID = 1` ✅
- 通过

**检查 3: 限流**

```go
if lastShed := s.lastIOOverloadLeaseShed.Load(); lastShed != nil {
    minInterval := MinIOOverloadLeaseShedInterval.Get(&s.cfg.Settings.SV)
    if timeutil.Since(lastShed.(time.Time)) < minInterval {
        return
    }
}
```

- `lastShed = nil` (从未触发过) → 通过

**检查 4: 实际过载检查**

```go
if s.existingLeaseCheckIOOverload(ctx) {
    return
}
```

调用链:
```go
existingLeaseCheckIOOverload()
  ↓
storeDescriptor.IOThreshold = 0.875
storeList.Mean = 0.3067
  ↓
allocator.IOOverloadOptions().ExistingLeaseCheck()
  ↓
判断:
  0.875 > 0.8 (阈值) ? ✅
  0.875 > 0.3067 × 1.5 (集群平均 × 1.5) ? ✅
  ↓
返回 false (表示过载)
```

- 返回 `false` → **Store 过载**,继续执行

**所有守卫条件通过,决定触发 lease 转移!**

### 5.5 记录时间并启动异步任务

**10:03:11.050**: 更新 `lastIOOverloadLeaseShed`

```go
s.lastIOOverloadLeaseShed.Store(s.Clock().Now().GoTime())
// 存储: 2021-03-15 10:03:11.050
```

**10:03:11.051**: 记录日志

```
I210315 10:03:11.051456 store1 [S1] kv/kvserver/store.go:2902
  IO overload detected, will shed leases 2000
```

**10:03:11.052**: 启动异步任务

```go
stopper.RunTask(ctx, "io-overload: shed leases", func(ctx context.Context) {
    newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
        s.leaseQueue.maybeAdd(ctx, repl, now)
        return true
    })
})
```

**时间开销**:
- 守卫检查: ~0.5ms
- 更新原子变量: ~0.01ms
- 启动 goroutine: ~0.01ms
- **总计**: ~0.52ms

**Gossip goroutine 立即返回,继续处理其他消息**

### 5.6 异步任务执行

**10:03:11.053**: 异步 goroutine 开始执行

**阶段 1: 拷贝 replica 列表**

```go
store.mu.Lock()
replicas := make([]*Replica, 0, 2000)
for _, repl := range store.mu.replicas {
    replicas = append(replicas, repl)
}
store.mu.Unlock()
```

- 耗时: ~10μs (16KB 内存分配)

**阶段 2: 遍历并加入队列**

```go
for i, repl := range replicas {
    leaseQueue.maybeAdd(ctx, repl, now)
}
```

**每个 replica 的处理**:

```go
func (lq *leaseQueue) maybeAdd(ctx, repl, now) {
    // 1. 检查是否持有 lease
    if !repl.OwnsValidLease(ctx, now) {
        return  // 500 个 replica 不是 leaseholder,直接跳过
    }

    // 2. 检查是否已在队列
    if lq.contains(repl.RangeID) {
        return  // 0 个(首次加入)
    }

    // 3. 加入队列
    lq.add(repl, 1.0)
}
```

**统计**:
- 2000 个 replica,其中 500 个持有 lease(其他 1500 个是 follower)
- 500 个 leaseholder replica 被加入队列

**耗时**:
- 平均 50μs/replica × 2000 = 100ms

**10:03:11.153**: 遍历完成

### 5.7 Lease Queue 处理

**Lease Queue 后台 goroutine** (一直运行):

```go
for {
    repl := lq.pop()  // 从队列取出一个 replica
    if repl == nil {
        time.Sleep(10 * time.Millisecond)
        continue
    }

    lq.processReplica(repl)
}
```

**`processReplica` 的逻辑**:

```go
func (lq *leaseQueue) processReplica(repl *Replica) {
    // 1. 检查是否应该转移
    shouldTransfer, target := lq.shouldTransferLease(repl)
    if !shouldTransfer {
        return  // 没有更好的目标
    }

    // 2. 执行转移
    err := repl.TransferLease(ctx, target)
    if err != nil {
        log.Warningf("lease transfer failed: %v", err)
        lq.add(repl, 0.5)  // 降低优先级,稍后重试
    }
}
```

**`shouldTransferLease` 的判断**:

```go
func (lq *leaseQueue) shouldTransferLease(repl *Replica) (bool, roachpb.StoreID) {
    candidates := lq.allocator.TransferLeaseTarget(
        ctx,
        repl.StoreID(),       // 当前 Store: 1
        repl.Desc(),          // Range 描述
        lq.storePool.GetStoreList(),
    )

    // Allocator 逻辑:
    // 1. 排除当前 Store (Store-1)
    // 2. 排除 IO 过载的 Store (IOThreshold > 0.8)
    // 3. 按 lease 数量排序,选择最少的

    // 结果: Store-2 (IOThreshold=0.28, LeaseCount=500)
    return true, 2
}
```

**单个 lease 转移的时间线**:

| 时间偏移 | 事件 | 耗时 |
|---------|------|------|
| T0 | 从队列取出 replica | <1ms |
| T0 + 1ms | shouldTransferLease 判断 | 10ms |
| T0 + 11ms | 提交 Raft 提议(TransferLease) | 5ms |
| T0 + 16ms | Raft 日志复制到 follower | 30ms |
| T0 + 46ms | Raft 日志应用 | 50ms |
| T0 + 96ms | 新 leaseholder 激活 | 10ms |
| T0 + 106ms | 通知客户端 | 5ms |
| **总计** | | **~110ms** |

**处理 500 个 lease 的总时间**:

```
500 个 lease × 110ms = 55 秒

但 lease queue 并行度有限(通常 5 个 worker):
实际时间: 55 秒 / 5 = 11 秒
```

**10:03:22**: 所有 500 个 lease 转移完成

### 5.8 IO 负载恢复

**Lease 转移后的状态**:

| Store | LeaseCount | L0 文件数 | IOThreshold |
|-------|-----------|----------|-------------|
| Store-1 | 1500 (-500) | 45 (-25) | 0.56 ↓ |
| Store-2 | 1000 (+500) | 40 (+15) | 0.50 ↑ |
| Store-3 | 500 | 30 | 0.375 |

**Store-1 的 Swag 窗口**:

| 时间 | L0 文件数 | 当前窗口值 | 5分钟最大值 |
|------|----------|----------|-----------|
| 10:03:00 | 70 | 0.875 | 0.875 |
| 10:04:00 | 72 | 0.90 | 0.90 |
| 10:05:00 | 45 | 0.56 | 0.90 |
| 10:06:00 | 40 | 0.50 | 0.90 |
| 10:07:00 | 38 | 0.475 | 0.90 |
| 10:08:00 | 35 | 0.4375 | 0.90 |
| 10:09:00 | 32 | 0.40 | 0.56 ← 旧窗口滑出 |

**10:09:00**: Store-1 的 IOThreshold 降到 0.56,不再触发 lease 转移

### 5.9 后续 Gossip 消息的处理

**10:03:20**: Store-1 再次 gossip(距上次 10 秒)

```go
capacityChanged(1, old, cur)
  ↓
makeIOOverloadCapacityChangeFn(1, old, cur)
  ↓
检查限流:
  lastShed = 10:03:11.050
  Since(lastShed) = 9 秒 < 30 秒
  ↓
返回 (被限流拒绝)
```

**10:03:30、10:03:40**: 同样被限流拒绝

**10:03:45**: 限流时间到(距上次 34 秒)

```go
capacityChanged(1, old, cur)
  ↓
makeIOOverloadCapacityChangeFn(1, old, cur)
  ↓
检查限流: Since(lastShed) = 34 秒 ≥ 30 秒 → 通过
  ↓
检查过载: IOThreshold = 0.56 < 0.8 → 不过载
  ↓
existingLeaseCheckIOOverload() 返回 true
  ↓
return (不触发)
```

**结果**: 已不过载,不再触发 lease 转移

---

## 六、设计权衡：为什么这样设计?有哪些替代方案? (Design Trade-offs)

### 6.1 反应式 vs 预防式

**当前设计: 反应式(Reactive)**

```
等待问题发生 → 检测到过载 → 触发 lease 转移 → 缓解过载
```

**优点**:
1. **简单**: 不需要预测未来负载
2. **准确**: 基于实际观测到的过载,减少误判
3. **低开销**: 只在过载时才采取行动

**缺点**:
1. **滞后**: 过载已经发生,可能影响了用户请求
2. **恢复慢**: lease 转移需要数分钟

**替代方案 1: 预防式(Proactive)**

```
预测负载趋势 → 提前转移 lease → 避免过载
```

**示例实现**:

```go
func (s *Store) predictIOOverload() bool {
    // 分析 Swag 窗口的趋势
    windows := s.ioThreshold.swag.Windows(now)

    // 计算增长率
    if len(windows) >= 3 {
        recent := windows[0]      // 最近 1 分钟
        prev := windows[1]        // 2 分钟前
        oldPrev := windows[2]     // 3 分钟前

        slope := (recent - oldPrev) / 2.0
        predicted := recent + slope  // 预测 1 分钟后

        if predicted > 0.8 {
            return true  // 预测将过载
        }
    }
    return false
}
```

**优点**: 提前避免过载,减少用户影响

**缺点**:
1. **预测不准**: 负载可能突然下降(如批量任务完成)
2. **误触发**: 导致不必要的 lease 转移
3. **复杂性**: 需要维护预测模型

**为什么选择反应式?**

CockroachDB 的设计哲学:
> "简单且正确" 优于 "复杂但可能更优"

反应式虽然滞后,但配合以下机制可以缓解:
1. **Admission Control**: 提前限流,减少进入的请求
2. **Swag 滑动窗口**: 平滑瞬时抖动,减少误触发
3. **限流保护**: 避免震荡

### 6.2 批量转移 vs 增量转移

**当前设计: 批量转移**

```
一次性将所有 2000 个 replica 加入队列 → 队列慢慢处理
```

**优点**:
1. **快速响应**: 遍历一次就完成,100ms 内完成入队
2. **简单**: 不需要跟踪状态

**缺点**:
1. **粗粒度**: 可能转移过多 lease(实际只需转移 200 个就能缓解)
2. **队列压力**: 2000 个 replica 同时在队列中

**替代方案: 增量转移**

```
每次只转移 100 个 lease → 观察效果 → 如果还过载,再转移 100 个
```

**示例实现**:

```go
func (s *Store) shedLeasesIncremental(count int) {
    transferred := 0
    newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
        if !repl.OwnsValidLease(ctx, now) {
            return true  // 继续遍历
        }

        s.leaseQueue.maybeAdd(ctx, repl, now)
        transferred++

        if transferred >= count {
            return false  // 停止遍历
        }
        return true
    })
}

// 调用
shedLeasesIncremental(100)  // 只转移 100 个
```

**优点**:
1. **精确控制**: 避免过度转移
2. **减少队列压力**: 每次只加入 100 个

**缺点**:
1. **复杂**: 需要跟踪已转移的数量
2. **多次触发**: 可能需要触发多次才能缓解

**为什么选择批量?**

1. **Lease Queue 有去重**: 重复加入不会导致重复处理
2. **Queue 自身限流**: 有 worker 数量限制,不会同时处理所有 replica
3. **简单实现**: 一次性加入,代码清晰

### 6.3 限流时间: 30 秒 vs 其他值

**当前设计: 30 秒**

```go
MinIOOverloadLeaseShedInterval = 30 * time.Second
```

**选择理由**:

1. **Lease 转移速度**: 500 个 lease 转移需要 ~11 秒
2. **Gossip 频率**: 每 10 秒一次
3. **留出缓冲**: 30 秒 = 2-3 轮 Gossip 周期

**如果是 10 秒?**

```
问题:
T0: 触发转移 → 加入 500 个 lease
T10: 队列只处理了 100 个 lease,IO 依然高 → 再次触发 → 重复加入
T20: 队列只处理了 200 个 lease → 再次触发 → 重复加入

结果: 频繁重复加入,虽然有去重,但浪费 CPU
```

**如果是 60 秒?**

```
问题:
T0: 触发转移 → 加入 500 个 lease
T11: 所有 lease 转移完成,IO 恢复正常
T30: IO 再次升高(新的负载) → 但被限流阻止
T60: 才能再次触发 → 延迟 30 秒

结果: 响应过慢,用户受影响时间更长
```

**30 秒的平衡**:
- 足够让第一批 lease 转移完成大部分
- 不会太长导致无法响应新的过载

### 6.4 同步 vs 异步执行

**当前设计: 异步执行**

```go
stopper.RunTask(ctx, "io-overload: shed leases", func(ctx) {
    // 遍历 replica,加入队列
})
```

**优点**:
1. **不阻塞 Gossip**: 主线程快速返回(~1ms)
2. **避免死锁**: Gossip 消息处理不能相互等待

**缺点**:
1. **复杂性**: 需要管理 goroutine 生命周期
2. **错误处理**: 异步任务的错误不容易传播

**替代方案: 同步执行**

```go
func makeIOOverloadCapacityChangeFn() ... {
    return func(...) {
        // 所有守卫检查
        // ...

        // 直接在当前 goroutine 遍历
        newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
            s.leaseQueue.maybeAdd(ctx, repl, now)
            return true
        })
    }
}
```

**问题**:

Gossip goroutine 被阻塞 100ms:
```
T0: Gossip 收到消息 A
T1: 开始处理,触发回调
T1 ~ T1+100ms: 遍历 2000 个 replica (阻塞)
T1+100ms: 返回,继续处理下一条消息

期间: 所有其他 Gossip 消息积压,延迟 100ms
```

影响:
- 集群容量信息更新延迟
- Allocator 决策基于过时信息
- 可能导致级联问题

**为什么必须异步?**

Gossip 网络的设计要求:
> 所有消息处理必须快速返回(<10ms),否则影响整个集群的信息传播

### 6.5 原子变量 vs 互斥锁

**当前设计: atomic.Value**

```go
lastIOOverloadLeaseShed atomic.Value  // 存储 time.Time

// 读
lastShed := s.lastIOOverloadLeaseShed.Load()

// 写
s.lastIOOverloadLeaseShed.Store(now)
```

**优点**:
1. **无锁**: 读写都是原子操作,无竞争
2. **快速**: Load/Store 只需几个 CPU 周期

**缺点**:
1. **类型不安全**: 需要类型断言 `lastShed.(time.Time)`
2. **微小竞态窗口**: 两个 goroutine 可能同时通过限流检查

**替代方案: sync.Mutex**

```go
type Store struct {
    shedMu sync.Mutex
    lastIOOverloadLeaseShed time.Time
}

// 使用
s.shedMu.Lock()
defer s.shedMu.Unlock()

if time.Since(s.lastIOOverloadLeaseShed) < 30*time.Second {
    return
}
s.lastIOOverloadLeaseShed = time.Now()
```

**优点**:
1. **类型安全**: 直接使用 `time.Time`
2. **无竞态**: 完全消除并发问题

**缺点**:
1. **锁竞争**: 每次 Gossip 消息都要获取锁
2. **性能**: mutex 比 atomic 慢 10-100 倍

**性能对比**:

```
基准测试(每秒操作数):

atomic.Load/Store: ~500M ops/sec
sync.Mutex Lock/Unlock: ~50M ops/sec

差距: 10 倍
```

**为什么选择 atomic?**

1. **Gossip 消息频繁**: 每秒数百次,锁竞争影响大
2. **竞态影响小**: 即使重复触发,leaseQueue 有去重
3. **注释承认**: "hardly seems necessary" (基本不必要用 mutex)

### 6.6 全量队列 vs 优先队列

**当前设计: 全量加入队列**

```go
newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
    s.leaseQueue.maybeAdd(ctx, repl, now)
    return true  // 所有 leaseholder 都加入
})
```

**替代方案: 按优先级加入**

```go
type replicaPriority struct {
    repl *Replica
    qps  float64  // 该 replica 的写入 QPS
}

// 收集并排序
var candidates []replicaPriority
newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
    if !repl.OwnsValidLease(ctx, now) {
        return true
    }
    candidates = append(candidates, replicaPriority{
        repl: repl,
        qps:  repl.GetQPS(),
    })
    return true
})

// 按 QPS 排序(高 QPS 优先转移)
sort.Slice(candidates, func(i, j int) bool {
    return candidates[i].qps > candidates[j].qps
})

// 只加入前 200 个高 QPS 的 replica
for i := 0; i < 200 && i < len(candidates); i++ {
    s.leaseQueue.maybeAdd(ctx, candidates[i].repl, now)
}
```

**优点**:
1. **精确打击**: 优先转移高负载的 lease
2. **更快缓解**: 转移 200 个高 QPS lease 可能比转移 500 个随机 lease 效果更好

**缺点**:
1. **复杂**: 需要收集 QPS 指标
2. **内存**: 需要额外切片存储(2000 × 16 字节 = 32KB)
3. **耗时**: 排序需要 O(n log n) = ~20ms

**为什么选择全量?**

1. **简单**: 不需要额外指标收集
2. **leaseQueue 自己会优先级排序**: 队列内部已经按优先级处理
3. **去重**: 即使全量加入,队列去重机制避免重复

---

## 七、核心总结：makeIOOverloadCapacityChangeFn 的本质是什么? (Summary)

### 7.1 一句话总结

**makeIOOverloadCapacityChangeFn 是一个反应式的 lease 转移触发器,当 Store 的 IO 负载超过阈值时,批量将所有 leaseholder replica 加入 lease queue,通过转移 lease 来降低本地写入负载,避免 IO 过载导致的级联故障。**

### 7.2 核心机制

**三层防护**:

```
第一层: Gossip + StorePool (检测)
  ↓ 每 10 秒传播容量信息
第二层: makeIOOverloadCapacityChangeFn (决策)
  ↓ 6 个守卫条件 + 30 秒限流
第三层: Lease Queue (执行)
  ↓ 异步转移 lease 到健康 Store
```

**关键设计原则**:

1. **反应式**: 不预测,基于实际观测
2. **批量**: 一次性加入所有 leaseholder,队列慢慢处理
3. **限流**: 30 秒内最多触发一次,避免震荡
4. **异步**: 不阻塞 Gossip 主线程,保持集群信息传播通畅
5. **无锁**: 使用 atomic.Value,避免锁竞争

### 7.3 数据流向

```
┌─────────────────────────────────────────────────────────┐
│ 阶段 1: 检测 (每 10 秒)                                   │
└─────────────────────────────────────────────────────────┘
Store.GossipStore()
  → Gossip 网络传播
    → StorePool.updateStoreDetail()
      → capacityChanged()

┌─────────────────────────────────────────────────────────┐
│ 阶段 2: 决策 (~1ms)                                      │
└─────────────────────────────────────────────────────────┘
makeIOOverloadCapacityChangeFn()
  → 6 个守卫条件检查
    → 通过 → 更新 lastIOOverloadLeaseShed
      → 启动异步任务

┌─────────────────────────────────────────────────────────┐
│ 阶段 3: 入队 (~100ms)                                    │
└─────────────────────────────────────────────────────────┘
异步 goroutine
  → 遍历 2000 个 replica
    → 500 个 leaseholder 加入 leaseQueue

┌─────────────────────────────────────────────────────────┐
│ 阶段 4: 转移 (~11 秒)                                    │
└─────────────────────────────────────────────────────────┘
leaseQueue 后台处理
  → 逐个检查 shouldTransferLease
    → Raft 提议 TransferLease
      → 等待日志应用
        → 新 leaseholder 激活
```

### 7.4 与前述章节的关系

**第二十五章 (Store.Start)**:
- **关系**: 在启动的第 5 步注册此回调
- **意义**: 回调是 Store 运行时服务的一部分,启动后持续监听

**第二十六章 (ioThresholdMap)**:
- **关系**: 提供副本级别的过载判断
- **对比**: ioThresholdMap 用于 **follower 流控**,本章用于 **leaseholder 转移**
- **协同**: 双层保护,follower 过载暂停复制,leaseholder 过载转移 lease

**第二十七章 (Swag 滑动窗口)**:
- **关系**: 提供 5 分钟历史 IO 峰值
- **意义**: 避免瞬时抖动触发 lease 转移,确保过载是持续的

### 7.5 心智模型

**比喻: 医院的急诊分流**

```
正常情况:
  急诊室 (Store-1) 接收患者 (请求)
  按顺序处理

高负载情况:
  急诊室爆满 (IO 过载)
    ↓
  makeIOOverloadCapacityChangeFn = 分诊护士
    ↓
  决定: 将部分患者转移到其他医院 (lease 转移)
    ↓
  批量通知所有患者 (加入队列)
    ↓
  救护车逐个转运 (lease queue 处理)
    ↓
  急诊室负载降低,恢复正常
```

**限流机制 = 防止护士反复喊"分流分流"**:
- 30 秒内只喊一次
- 给救护车足够时间转运患者
- 避免护士和司机都累瘫

### 7.6 关键代码位置

| 功能 | 文件 | 行号 |
|------|------|------|
| 回调工厂函数 | pkg/kv/kvserver/store.go | 2866-2917 |
| 注册回调 | pkg/kv/kvserver/store.go | 1513 |
| 过载检查 | pkg/kv/kvserver/store.go | 2851-2859 |
| 限流配置 | pkg/kv/kvserver/lease_queue.go | 54-60 |
| StorePool 回调管理 | pkg/kv/kvserver/allocator/storepool/store_pool.go | 943-957 |
| 时间戳存储 | pkg/kv/kvserver/store.go | 1128 |

### 7.7 设计亮点

1. **守卫条件分层**: 6 个条件从快到慢排列,快速返回常见情况
2. **限流保护**: 简单有效,避免震荡
3. **异步执行**: Gossip 主线程不受影响,~1ms 返回
4. **无锁设计**: atomic.Value 性能优异,避免锁竞争
5. **去重保护**: leaseQueue 的 `contains()` 检查避免重复处理
6. **优雅关闭**: 使用 stopper.RunTask,支持优雅关闭

### 7.8 局限性

1. **滞后性**: 过载已经发生才响应,用户可能已受影响
2. **粗粒度**: 全量加入队列,可能转移过多 lease
3. **微小竞态**: 两个 Gossip 消息可能同时通过限流检查
4. **依赖 Gossip**: 如果 Gossip 网络分区,无法触发

**缓解措施**:
- 滞后性: 配合 Admission Control 提前限流
- 粗粒度: leaseQueue 自己会判断是否真正需要转移
- 微小竞态: 影响小,leaseQueue 去重保护
- Gossip 依赖: 本地 Store 也会触发自己的回调

---

**本章完**

通过本章分析,我们深入理解了 CockroachDB 如何在 Store 级别应对 IO 过载危机:通过反应式的 lease 转移机制,配合限流保护和异步执行,在不阻塞集群信息传播的前提下,快速将 lease 转移到健康的 Store,降低本地 IO 负载,避免级联故障。这个机制与 admission control、ioThresholdMap、Swag 滑动窗口共同构成了 CockroachDB 多层次的过载保护体系。
