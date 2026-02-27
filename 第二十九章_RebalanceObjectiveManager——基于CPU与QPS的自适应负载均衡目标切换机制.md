# 第二十九章：RebalanceObjectiveManager——基于 CPU 与 QPS 的自适应负载均衡目标切换机制

## 一、BFS 概览：为什么需要动态的负载均衡目标？(Why)

### 1.1 核心问题：不同工作负载需要不同的均衡策略

在分布式数据库中,**如何定义"负载均衡"** 是一个关键问题:

**问题场景**:

```
场景 1: 均匀请求负载(如 OLTP 事务)
  - 每个请求的 CPU 开销相似(~1ms CPU time)
  - QPS 可以准确反映负载
  - 策略: 按 QPS 均衡即可

场景 2: 不均匀请求负载(如混合负载)
  - 简单查询: 0.1ms CPU, 100 QPS
  - 复杂查询: 50ms CPU, 10 QPS
  - QPS 无法反映真实负载,CPU 才是瓶颈
  - 策略: 必须按 CPU 使用率均衡
```

**具体案例**:

假设 3 个 Store,每个 Store 承载不同类型的 Range:

| Store | Range 类型 | QPS | CPU/sec | 实际负载 |
|-------|-----------|-----|---------|---------|
| Store-1 | 简单 KV 读 | 10,000 | 10s | 低 |
| Store-2 | 全表扫描 | 100 | 80s | 高 |
| Store-3 | 索引查询 | 5,000 | 45s | 中 |

**如果只按 QPS 均衡**:

```
系统判断: Store-1 QPS 最高(10,000) → 过载
实际情况: Store-1 CPU 最低(10s) → 最空闲

错误决策: 将 lease 从 Store-1 转移走
结果: Store-2 负载更高,性能恶化
```

**如果按 CPU 均衡**:

```
系统判断: Store-2 CPU 最高(80s) → 过载
实际情况: 确实过载

正确决策: 将 lease 从 Store-2 转移到 Store-1
结果: 负载趋于均衡,性能提升
```

### 1.2 解决方案：动态切换负载均衡目标

`RebalanceObjectiveManager` 提供了**运行时动态切换**负载均衡目标的能力:

```
配置: kv.allocator.load_based_rebalancing.objective = "cpu"
  ↓
RebalanceObjectiveManager 监听配置变更
  ↓
检查集群是否支持 CPU 测量(grunning)
  ↓
如果支持 → 使用 CPU 均衡
如果不支持 → 降级到 QPS 均衡
  ↓
通知所有相关组件更新策略:
  - Allocator (Replica/Lease 放置决策)
  - Load-Based Splitter (Range 分裂策略)
  - Store Rebalancer (主动再平衡)
```

**关键设计理念**:

1. **自适应降级**: 如果 CPU 测量不可用(如 ARM 架构),自动降级到 QPS
2. **集群一致性**: 如果集群中任意节点不支持 CPU 测量,全部降级
3. **热更新**: 无需重启,配置变更实时生效
4. **回调通知**: 目标变更时,自动更新所有 Replica 的分裂策略

### 1.3 在整个系统中的位置

**层次结构**:

```
┌─────────────────────────────────────────────────────────────┐
│ Store 启动流程 (Store.Start)                                  │
└─────────────────────────────────────────────────────────────┘
    第 5 步: 创建 RebalanceObjectiveManager
              ↓
    第 6 步: 创建 Allocator (使用 manager.Objective())
              ↓
    第 7 步: 创建 Rebalance Queue
              ↓
    第 8 步: 创建 Store Rebalancer

┌─────────────────────────────────────────────────────────────┐
│ RebalanceObjectiveManager 协作组件                            │
└─────────────────────────────────────────────────────────────┘
    → StorePool (监听 Gossip 容量变更)
    → Allocator (查询当前目标)
    → Load-Based Splitter (每个 Replica 一个)
    → Store Rebalancer (使用目标指导再平衡)
```

**与前述章节的关系**:

| 章节 | 机制 | 与本章的关系 |
|------|------|------------|
| 第二十五章 | Store.Start() | 在启动第 5 步创建 RebalanceObjectiveManager |
| 第二十八章 | makeIOOverloadCapacityChangeFn | 同样注册到 StorePool,监听容量变更 |
| 第二十七章 | Swag 滑动窗口 | 可用于追踪 CPU/QPS 历史峰值 |

**协作对象**:

```go
// pkg/kv/kvserver/store.go
type Store struct {
    // ...
    rebalanceObjManager *RebalanceObjectiveManager  // 本章主角
    allocator           *Allocator                  // 消费者 1
    storeRebalancer     *StoreRebalancer            // 消费者 2
}

// pkg/kv/kvserver/replica.go
type Replica struct {
    // ...
    loadBasedSplitter *loadBasedSplitter  // 消费者 3 (每个 Replica 一个)
}
```

---

## 二、BFS 控制流：目标如何被确定和传播？(How it flows)

### 2.1 创建阶段：Store 启动时的初始化

**位置**: `pkg/kv/kvserver/store.go:1515-1531`

```go
// Store.Start() 的第 5 步
s.rebalanceObjManager = newRebalanceObjectiveManager(
    ctx,
    s.cfg.AmbientCtx,
    s.cfg.Settings,
    func(ctx context.Context, obj LBRebalancingObjective) {
        // 回调函数: 当目标变化时,更新所有 Replica 的分裂策略
        s.VisitReplicas(func(r *Replica) (wantMore bool) {
            r.loadBasedSplitter.SetSplitObjective(
                s.Clock().PhysicalTime(),
                obj.ToSplitObjective(),
            )
            return true
        })
    },
    allocatorStorePool, /* storeDescProvider */
    allocatorStorePool, /* capacityChangeNotifier */
)
```

**时间点**: Store 启动序列的第 5 步(创建 Allocator 之前)

**调用链**:

```
Server.PreStart()
  → Node.Start()
    → Store.Start()
      → [第 4 步] 注册 IO 过载回调
      → [第 5 步] newRebalanceObjectiveManager() ← 我们在这里
        ↓ 构造 RebalanceObjectiveManager 对象
        ↓ 初始化当前目标 (ResolveLBRebalancingObjective)
        ↓ 注册 3 个监听器:
          1. LoadBasedRebalancingObjective.SetOnChange (配置变更)
          2. Version.SetOnChange (版本升级)
          3. StorePool.SetOnCapacityChange (容量变更)
      → [第 6 步] 创建 Allocator
      → [第 7 步] 创建 Rebalance Queue
```

### 2.2 初始化流程：确定初始目标

**步骤 1: 创建 RebalanceObjectiveManager 对象**

```go
// pkg/kv/kvserver/rebalance_objective.go:186-193
rom := &RebalanceObjectiveManager{
    st:                st,                  // cluster.Settings
    storeDescProvider: storeDescProvider,   // StorePool
    AmbientContext:    ambientCtx,
}
rom.AddLogTag("rebalance-objective", nil)
ctx = rom.AnnotateCtx(ctx)
```

**步骤 2: 解析初始目标**

```go
// rebalance_objective.go:194-195
rom.mu.obj = ResolveLBRebalancingObjective(ctx, st, storeDescProvider.GetStores())
rom.mu.onChange = onChange  // 保存回调函数
```

**`ResolveLBRebalancingObjective` 的逻辑** (rebalance_objective.go:256-289):

```go
func ResolveLBRebalancingObjective(
    ctx context.Context,
    st *cluster.Settings,
    descs map[roachpb.StoreID]roachpb.StoreDescriptor,
) LBRebalancingObjective {
    // 1. 读取配置
    set := LoadBasedRebalancingObjective.Get(&st.SV)

    // 2. 如果配置是 QPS,直接返回
    if set == LBRebalancingQueries {
        return LBRebalancingQueries
    }

    // 3. 检查本地架构是否支持 grunning (CPU 时间测量)
    if !grunning.Supported {
        log.KvDistribution.Infof(ctx,
            "cpu timekeeping unavailable on host, reverting to qps balance objective")
        return LBRebalancingQueries
    }

    // 4. 检查集群中所有 Store 是否都支持 CPU 测量
    for _, desc := range descs {
        if desc.Capacity.CPUPerSecond == -1 {  // -1 表示不支持
            log.KvDistribution.Warningf(ctx,
                "cpu timekeeping unavailable on node %d but available locally, "+
                "reverting to qps balance objective",
                desc.Node.NodeID)
            return LBRebalancingQueries
        }
    }

    // 5. 所有检查通过,返回配置的目标
    return set
}
```

**降级决策树**:

```
配置 = LBRebalancingCPU
  ↓
本地支持 grunning?
  ├─ No → 降级到 LBRebalancingQueries
  └─ Yes
       ↓
  集群中所有 Store 都支持 CPU 测量?
    ├─ No (有 Store.Capacity.CPUPerSecond == -1)
    │   → 降级到 LBRebalancingQueries
    └─ Yes
          → 返回 LBRebalancingCPU
```

**时间线示例**:

```
T0 = Store.Start() 开始
T1: newRebalanceObjectiveManager() 调用
T2: ResolveLBRebalancingObjective() 执行
  - 读取配置: "cpu"
  - 检查 grunning.Supported: true
  - 查询 StorePool.GetStores(): 3 个 Store
  - 检查每个 Store 的 CPUPerSecond:
    Store-1: 45.2 (支持)
    Store-2: 67.8 (支持)
    Store-3: -1 (不支持!) ← ARM 架构节点
  - 决策: 降级到 LBRebalancingQueries
T3: 设置 rom.mu.obj = LBRebalancingQueries
T4: 注册监听器
T5: newRebalanceObjectiveManager() 返回
```

### 2.3 监听器注册：三种触发机制

#### 2.3.1 监听器 1: 配置变更

**代码** (rebalance_objective.go:197-199):

```go
LoadBasedRebalancingObjective.SetOnChange(&rom.st.SV, func(ctx context.Context) {
    rom.maybeUpdateRebalanceObjective(rom.AnnotateCtx(ctx))
})
```

**触发场景**:

```sql
-- DBA 执行 SQL 更改配置
SET CLUSTER SETTING kv.allocator.load_based_rebalancing.objective = 'qps';
```

**调用链**:

```
SQL 层执行 SET CLUSTER SETTING
  ↓
cluster.Settings.Set()
  ↓
遍历所有注册的 onChange 回调
  ↓
rom.maybeUpdateRebalanceObjective()
  ↓
重新解析目标 (ResolveLBRebalancingObjective)
  ↓
如果目标变化 → 调用用户提供的 onChange 回调
```

#### 2.3.2 监听器 2: 版本升级

**代码** (rebalance_objective.go:200-202):

```go
rom.st.Version.SetOnChange(func(ctx context.Context, _ clusterversion.ClusterVersion) {
    rom.maybeUpdateRebalanceObjective(rom.AnnotateCtx(ctx))
})
```

**为什么需要监听版本变更?**

```
场景: 集群从 v22.2 升级到 v23.1
  - v22.2: 没有 grunning 支持,所有 Store.Capacity.CPUPerSecond = -1
  - v23.1: 引入 grunning 支持,Store 开始上报真实 CPU 值

升级过程:
  T0: 所有节点都是 v22.2 → 目标 = LBRebalancingQueries
  T1: 部分节点升级到 v23.1,但 CPUPerSecond 仍为 -1 (混合版本)
       → 目标保持 LBRebalancingQueries
  T2: 所有节点完成升级,CPUPerSecond 都有效
       → Version.SetOnChange 触发
       → maybeUpdateRebalanceObjective()
       → 检测到可以切换到 LBRebalancingCPU
       → 目标切换
```

#### 2.3.3 监听器 3: Store 容量变更

**代码** (rebalance_objective.go:212-221):

```go
capacityChangeNotifier.SetOnCapacityChange(
    func(storeID roachpb.StoreID, old, cur roachpb.StoreCapacity) {
        // 只关心 CPUPerSecond 的有效性变化
        if (old.CPUPerSecond < 0) != (cur.CPUPerSecond < 0) {
            // 从不支持 → 支持,或从支持 → 不支持
            cbCtx, span := rom.AnnotateCtxWithSpan(context.Background(), "capacity-change")
            defer span.Finish()
            rom.maybeUpdateRebalanceObjective(cbCtx)
        }
    })
```

**触发场景**:

**场景 1: 新节点加入集群**

```
T0: 集群有 3 个节点,都支持 CPU 测量
     → 目标 = LBRebalancingCPU

T1: 第 4 个节点加入,是 ARM 架构 (不支持 grunning)
     → Gossip 传播新 Store 容量信息
     → StorePool 收到: Store-4.Capacity.CPUPerSecond = -1
     → capacityChanged(storeID=4, old=nil, cur={CPUPerSecond=-1})
     → (old.CPUPerSecond < 0) = true (nil 默认 < 0)
     → (cur.CPUPerSecond < 0) = true
     → 条件不满足 (两边都 < 0),不触发

T2: StorePool 已知 Store-4 后,再次 Gossip
     → capacityChanged(storeID=4, old={CPUPerSecond=-1}, cur={CPUPerSecond=-1})
     → (old < 0) = true, (cur < 0) = true
     → 条件不满足,不触发

问题: 新节点加入时,容量变更回调可能检测不到!
```

**实际检测路径**: 配置变更或版本变更触发,或下次 Gossip 更新

**场景 2: Store 从不支持变为支持**

```
T0: Store-5 因软件 bug,CPUPerSecond = -1
T1: 修复 bug,重启后 CPUPerSecond = 52.3
T2: Gossip 传播
     → capacityChanged(5, old={CPUPerSecond=-1}, cur={CPUPerSecond=52.3})
     → (old < 0) = true, (cur < 0) = false
     → 条件满足! (true != false)
     → maybeUpdateRebalanceObjective()
     → 重新检查所有 Store
     → 可能切换到 LBRebalancingCPU
```

**注意**: 代码注释指出这个路径触发概率很低

> It is unlikely though that the conditions are satisfied (some node begins
> not supporting grunning or begin supporting grunning) to trigger the
> onChange callback here.

### 2.4 目标变更流程

**核心函数**: `maybeUpdateRebalanceObjective` (rebalance_objective.go:234-251)

```go
func (rom *RebalanceObjectiveManager) maybeUpdateRebalanceObjective(ctx context.Context) {
    rom.mu.Lock()
    defer rom.mu.Unlock()

    ctx = rom.AnnotateCtx(ctx)
    prev := rom.mu.obj
    next := ResolveLBRebalancingObjective(ctx, rom.st, rom.storeDescProvider.GetStores())

    // 目标没变,直接返回
    if prev == next {
        return
    }

    // 记录日志
    log.KvDistribution.Infof(ctx, "Updating the rebalance objective from %s to %s",
        prev, next)

    // 更新目标
    rom.mu.obj = next

    // 调用用户提供的回调
    rom.mu.onChange(ctx, rom.mu.obj)
}
```

**用户回调的实现** (store.go:1519-1527):

```go
func(ctx context.Context, obj LBRebalancingObjective) {
    // 遍历 Store 上所有 Replica
    s.VisitReplicas(func(r *Replica) (wantMore bool) {
        // 更新每个 Replica 的 load-based splitter
        r.loadBasedSplitter.SetSplitObjective(
            s.Clock().PhysicalTime(),
            obj.ToSplitObjective(),  // LBRebalancingCPU → SplitCPU
        )
        return true  // 继续遍历
    })
}
```

**`ToSplitObjective` 的映射** (replica_split_load.go:81-90):

```go
func (obj LBRebalancingObjective) ToSplitObjective() split.SplitObjective {
    switch obj {
    case LBRebalancingQueries:
        return split.SplitQPS      // 按 QPS 分裂
    case LBRebalancingCPU:
        return split.SplitCPU      // 按 CPU 使用率分裂
    default:
        panic("unknown objective")
    }
}
```

**完整的变更传播链**:

```
┌─────────────────────────────────────────────────────────────┐
│ 触发源 (3 种之一)                                             │
└─────────────────────────────────────────────────────────────┘
  1. 配置变更: SET CLUSTER SETTING ...
  2. 版本升级: 集群版本变化
  3. 容量变更: Store.Capacity.CPUPerSecond 有效性变化
                ↓
┌─────────────────────────────────────────────────────────────┐
│ 检查阶段                                                      │
└─────────────────────────────────────────────────────────────┘
  maybeUpdateRebalanceObjective()
    → ResolveLBRebalancingObjective()
      → 读取配置
      → 检查 grunning.Supported
      → 检查所有 Store 的 CPUPerSecond
      → 返回最终目标
                ↓
┌─────────────────────────────────────────────────────────────┐
│ 比较阶段                                                      │
└─────────────────────────────────────────────────────────────┘
  if prev == next:
    return  // 无变化,结束
                ↓
┌─────────────────────────────────────────────────────────────┐
│ 通知阶段                                                      │
└─────────────────────────────────────────────────────────────┘
  rom.mu.obj = next
  log.Infof("Updating objective from %s to %s", prev, next)
  rom.mu.onChange(ctx, next)
    ↓
  遍历所有 Replica (假设 2000 个)
    → r.loadBasedSplitter.SetSplitObjective(SplitCPU)
    → 耗时: 2000 × 10μs = 20ms
```

**时间线示例**:

```
T0: DBA 执行 SET CLUSTER SETTING
T1: Settings.Set() 触发
T2: maybeUpdateRebalanceObjective() 加锁
T3: ResolveLBRebalancingObjective() 执行 (~1ms)
  - 读取配置: "cpu"
  - 检查 grunning: true
  - 查询 StorePool: 3 个 Store,都支持 CPU
  - 返回: LBRebalancingCPU
T4: 比较: prev=LBRebalancingQueries, next=LBRebalancingCPU (不同)
T5: 记录日志
T6: 调用 onChange 回调
  - VisitReplicas() 遍历 2000 个 Replica
  - 每个 Replica 更新 loadBasedSplitter
  - 耗时: ~20ms
T7: maybeUpdateRebalanceObjective() 解锁,返回
T8: 总耗时: ~25ms
```

### 2.5 并发控制

**锁策略**:

```go
type RebalanceObjectiveManager struct {
    mu struct {
        syncutil.RWMutex
        obj      LBRebalancingObjective
        onChange func(ctx context.Context, obj LBRebalancingObjective)
    }
}
```

**读取路径** (Objective):

```go
func (rom *RebalanceObjectiveManager) Objective() LBRebalancingObjective {
    rom.mu.RLock()
    defer rom.mu.RUnlock()
    return rom.mu.obj
}
```

**写入路径** (maybeUpdateRebalanceObjective):

```go
func (rom *RebalanceObjectiveManager) maybeUpdateRebalanceObjective(ctx) {
    rom.mu.Lock()
    defer rom.mu.Unlock()
    // ...
}
```

**并发场景分析**:

**场景 1: 多个 goroutine 同时读取目标**

```
Goroutine 1: Allocator.AllocateVoter() 需要知道当前目标
  → rom.Objective()
    → rom.mu.RLock()
    → return obj
    → rom.mu.RUnlock()

Goroutine 2: Store Rebalancer 需要知道当前目标
  → rom.Objective()
    → rom.mu.RLock() (可以并发)
    → return obj
    → rom.mu.RUnlock()

并发安全: RWMutex 允许多个读者同时持有锁
```

**场景 2: 读取与更新并发**

```
Goroutine 1: Allocator 正在读取
  → rom.Objective()
    → rom.mu.RLock()
    → ...

Goroutine 2: 配置变更触发更新
  → maybeUpdateRebalanceObjective()
    → rom.mu.Lock() (等待 Goroutine 1 释放 RLock)
    → ...

顺序保证: 写锁会等待所有读锁释放后才能获取
```

**场景 3: 多个更新并发**

```
Goroutine 1: 配置变更触发
  → maybeUpdateRebalanceObjective()
    → rom.mu.Lock()
    → ResolveLBRebalancingObjective() (~1ms)
    → ...

Goroutine 2: 版本升级触发
  → maybeUpdateRebalanceObjective()
    → rom.mu.Lock() (等待 Goroutine 1 释放)
    → ...

顺序保证: Mutex 确保只有一个 goroutine 可以更新
```

**关键设计**: 回调函数在**锁内**执行

```go
rom.mu.Lock()
defer rom.mu.Unlock()
// ...
rom.mu.onChange(ctx, rom.mu.obj)  // ← 在锁内!
```

**影响**: 如果回调很慢(如遍历 10,000 个 Replica),会长时间持有锁

**缓解措施**: 回调通常很快(~20ms),且变更频率极低(配置变更是人工操作)

---

## 三、DFS 深入：关键函数与核心逻辑 (How it works)

### 3.1 newRebalanceObjectiveManager: 构造函数

**完整签名** (rebalance_objective.go:178-185):

```go
func newRebalanceObjectiveManager(
    ctx context.Context,
    ambientCtx log.AmbientContext,               // 用于日志上下文
    st *cluster.Settings,                         // 集群配置
    onChange func(ctx context.Context, obj LBRebalancingObjective),  // 变更回调
    storeDescProvider gossipStoreDescriptorProvider,      // 获取 Store 列表
    capacityChangeNotifier gossipStoreCapacityChangeNotifier,  // 容量变更通知
) *RebalanceObjectiveManager
```

**参数详解**:

1. **ambientCtx**: 日志上下文,自动添加 `[rebalance-objective]` 标签

2. **onChange**: 用户提供的回调函数
   - **何时调用**: 目标从 QPS 切换到 CPU,或从 CPU 切换到 QPS
   - **调用位置**: `maybeUpdateRebalanceObjective` 的锁内
   - **典型实现**: 遍历所有 Replica,更新 loadBasedSplitter

3. **storeDescProvider**: 接口,实际是 `StorePool`
   ```go
   type gossipStoreDescriptorProvider interface {
       GetStores() map[roachpb.StoreID]roachpb.StoreDescriptor
   }
   ```
   - **用途**: 获取集群中所有 Store 的容量信息
   - **调用时机**: 每次解析目标时

4. **capacityChangeNotifier**: 接口,也是 `StorePool`
   ```go
   type gossipStoreCapacityChangeNotifier interface {
       SetOnCapacityChange(fn storepool.CapacityChangeFn)
   }
   ```
   - **用途**: 注册容量变更回调
   - **实现**: `StorePool.SetOnCapacityChange`

**实现步骤**:

**步骤 1: 创建对象**

```go
rom := &RebalanceObjectiveManager{
    st:                st,
    storeDescProvider: storeDescProvider,
    AmbientContext:    ambientCtx,
}
rom.AddLogTag("rebalance-objective", nil)
ctx = rom.AnnotateCtx(ctx)
```

**步骤 2: 初始化目标**

```go
rom.mu.obj = ResolveLBRebalancingObjective(ctx, st, storeDescProvider.GetStores())
rom.mu.onChange = onChange
```

**关键**: 在注册监听器之前先设置初始目标,避免冷启动时没有目标

**步骤 3: 注册配置监听器**

```go
LoadBasedRebalancingObjective.SetOnChange(&rom.st.SV, func(ctx context.Context) {
    rom.maybeUpdateRebalanceObjective(rom.AnnotateCtx(ctx))
})
```

**`SetOnChange` 的实现** (cluster.Settings):

```go
func (s *EnumSetting) SetOnChange(sv *Values, fn func(context.Context)) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.mu.onChange = append(s.mu.onChange, fn)
}
```

**触发时机**: 当 `SET CLUSTER SETTING kv.allocator.load_based_rebalancing.objective = ...` 执行时

**步骤 4: 注册版本监听器**

```go
rom.st.Version.SetOnChange(func(ctx context.Context, _ clusterversion.ClusterVersion) {
    rom.maybeUpdateRebalanceObjective(rom.AnnotateCtx(ctx))
})
```

**为什么忽略 ClusterVersion 参数?**

```go
func(ctx context.Context, _ clusterversion.ClusterVersion) {
    // 不关心具体版本号,只需要知道版本变了
    // 重新解析目标时会查询最新的 Store 信息
    rom.maybeUpdateRebalanceObjective(rom.AnnotateCtx(ctx))
}
```

**步骤 5: 注册容量变更监听器**

```go
capacityChangeNotifier.SetOnCapacityChange(
    func(storeID roachpb.StoreID, old, cur roachpb.StoreCapacity) {
        if (old.CPUPerSecond < 0) != (cur.CPUPerSecond < 0) {
            cbCtx, span := rom.AnnotateCtxWithSpan(context.Background(), "capacity-change")
            defer span.Finish()
            rom.maybeUpdateRebalanceObjective(cbCtx)
        }
    })
```

**XOR 条件的含义**:

```go
(old.CPUPerSecond < 0) != (cur.CPUPerSecond < 0)

等价于:
(old 不支持 && cur 支持) || (old 支持 && cur 不支持)

真值表:
old < 0  | cur < 0  | 条件     | 含义
---------|---------|---------|--------------------
true     | true    | false   | 一直不支持,不触发
true     | false   | true    | 从不支持 → 支持,触发
false    | true    | true    | 从支持 → 不支持,触发
false    | false   | false   | 一直支持,不触发
```

**为什么在这里创建新的 context?**

注释说明:
> NB: On capacity changes we don't have access to a context. Create a
> background context on callback.

原因: `CapacityChangeFn` 没有传入 context,需要自己创建

**为什么使用 span?**

用于分布式追踪,记录容量变更导致的目标更新操作

### 3.2 ResolveLBRebalancingObjective: 目标解析核心逻辑

**函数签名** (rebalance_objective.go:256-258):

```go
func ResolveLBRebalancingObjective(
    ctx context.Context,
    st *cluster.Settings,
    descs map[roachpb.StoreID]roachpb.StoreDescriptor,
) LBRebalancingObjective
```

**输入**:
- `st`: 集群配置,包含 `kv.allocator.load_based_rebalancing.objective` 设置
- `descs`: 集群中所有 Store 的描述符,包含 `Capacity.CPUPerSecond`

**输出**:
- `LBRebalancingQueries` (0) 或 `LBRebalancingCPU` (1)

**逻辑步骤**:

**步骤 1: 读取配置**

```go
set := LoadBasedRebalancingObjective.Get(&st.SV)
```

**`LoadBasedRebalancingObjective` 的定义** (rebalance_objective.go:103-111):

```go
var LoadBasedRebalancingObjective = settings.RegisterEnumSetting(
    settings.SystemOnly,
    "kv.allocator.load_based_rebalancing.objective",
    "what objective does the cluster use to rebalance; if set to `qps` "+
        "the cluster will attempt to balance qps among stores, if set to "+
        "`cpu` the cluster will attempt to balance cpu usage among stores",
    "cpu",  // 默认值
    LoadBasedRebalancingObjectiveMap,
    settings.WithPublic)
```

**配置映射**:

```go
var LoadBasedRebalancingObjectiveMap = map[LBRebalancingObjective]string{
    LBRebalancingQueries: "qps",
    LBRebalancingCPU:     "cpu",
}
```

**步骤 2: QPS 快速路径**

```go
if set == LBRebalancingQueries {
    return LBRebalancingQueries
}
```

**优化**: QPS 不需要任何前提条件,直接返回

**步骤 3: 检查本地架构支持**

```go
if !grunning.Supported {
    log.KvDistribution.Infof(ctx,
        "cpu timekeeping unavailable on host, reverting to qps balance objective")
    return LBRebalancingQueries
}
```

**`grunning.Supported` 的含义**:

`grunning` 是一个 Go 包,用于测量 goroutine 的 CPU 时间:

```go
// pkg/util/grunning/grunning.go
package grunning

// Supported is true if the current platform supports CPU time measurement.
// This is false on:
// - Windows
// - ARM macOS (M1/M2 Macs)
// - Some ARM Linux distributions
var Supported bool = ...  // 由 build tag 控制
```

**检测逻辑**:

```go
// grunning_supported.go (Linux amd64)
//go:build linux && amd64
var Supported = true

// grunning_unsupported.go (其他平台)
//go:build !linux || !amd64
var Supported = false
```

**为什么某些平台不支持?**

需要访问 `/proc/self/task/[tid]/stat` 文件,这是 Linux 特有的

**步骤 4: 检查集群一致性**

```go
for _, desc := range descs {
    if desc.Capacity.CPUPerSecond == -1 {
        log.KvDistribution.Warningf(ctx,
            "cpu timekeeping unavailable on node %d but available locally, "+
            "reverting to qps balance objective",
            desc.Node.NodeID)
        return LBRebalancingQueries
    }
}
```

**为什么 CPUPerSecond == -1 表示不支持?**

**Store 上报容量时的逻辑** (假设在 `pkg/kv/kvserver/store.go`):

```go
func (s *Store) GossipStore(ctx context.Context) {
    capacity := roachpb.StoreCapacity{
        // ...
        QueriesPerSecond: s.metrics.QueriesPerSecond.Rate(),
    }

    if grunning.Supported {
        capacity.CPUPerSecond = s.metrics.CPUPerSecond.Rate()
    } else {
        capacity.CPUPerSecond = -1  // 哨兵值,表示不支持
    }

    gossip.AddInfo("store-capacity", capacity)
}
```

**集群混合场景**:

```
节点 1 (Linux amd64): grunning.Supported = true
  → CPUPerSecond = 45.2

节点 2 (macOS ARM): grunning.Supported = false
  → CPUPerSecond = -1

节点 3 (Linux amd64): grunning.Supported = true
  → CPUPerSecond = 67.8

ResolveLBRebalancingObjective() 的逻辑:
  for store in [Node1.Store, Node2.Store, Node3.Store]:
    if store.CPUPerSecond == -1:  // Node2 匹配
      return LBRebalancingQueries

结果: 即使大部分节点支持,只要有一个不支持,就降级到 QPS
```

**为什么这么严格?**

```
假设允许部分节点不支持:

Allocator 决策时:
  - 对于 Node1, Node3: 按 CPU 负载选择
  - 对于 Node2: CPU 信息缺失,无法参与 CPU 均衡

问题:
  - Node2 可能被错误地认为是"低负载"(CPUPerSecond = -1 被当作 0?)
  - Allocator 会将大量 lease 转移到 Node2
  - 实际上 Node2 可能已经过载,但无法感知

解决方案: 全有或全无,确保决策一致性
```

**步骤 5: 返回配置值**

```go
return set
```

**此时所有检查通过**:
- 配置 = LBRebalancingCPU
- 本地架构支持 grunning
- 集群中所有 Store 都支持 CPU 测量

### 3.3 maybeUpdateRebalanceObjective: 更新目标

**函数实现** (rebalance_objective.go:234-251):

```go
func (rom *RebalanceObjectiveManager) maybeUpdateRebalanceObjective(ctx context.Context) {
    rom.mu.Lock()
    defer rom.mu.Unlock()

    ctx = rom.AnnotateCtx(ctx)
    prev := rom.mu.obj
    next := ResolveLBRebalancingObjective(ctx, rom.st, rom.storeDescProvider.GetStores())

    if prev == next {
        return
    }

    log.KvDistribution.Infof(ctx, "Updating the rebalance objective from %s to %s",
        prev, next)

    rom.mu.obj = next
    rom.mu.onChange(ctx, rom.mu.obj)
}
```

**关键设计决策**:

**决策 1: 每次都重新解析**

```go
next := ResolveLBRebalancingObjective(ctx, rom.st, rom.storeDescProvider.GetStores())
```

不缓存解析结果,每次都重新检查:
- 配置可能变了
- Store 列表可能变了(新节点加入/离开)
- Store 的 CPUPerSecond 可能从 -1 变为有效值

**决策 2: 只在变化时调用回调**

```go
if prev == next {
    return  // 无变化,不调用回调
}
```

**避免无意义的工作**:

```
场景: Store 容量每 10 秒更新一次

T0: Gossip 更新,触发 capacityChangeNotifier
  → maybeUpdateRebalanceObjective()
  → Resolve: LBRebalancingCPU
  → prev = LBRebalancingCPU, next = LBRebalancingCPU
  → 提前返回,不调用回调

T10: 又一次 Gossip 更新
  → maybeUpdateRebalanceObjective()
  → 同样提前返回

好处: 避免每 10 秒就遍历所有 Replica
```

**决策 3: 回调在锁内执行**

```go
rom.mu.Lock()
// ...
rom.mu.onChange(ctx, rom.mu.obj)  // ← 锁还没释放
```

**优点**: 保证回调看到的目标与 `rom.mu.obj` 一致

**缺点**: 如果回调很慢,其他读取会被阻塞

**实际影响**: 变更频率极低(人工操作),可接受

### 3.4 Objective: 读取当前目标

**函数实现** (rebalance_objective.go:227-232):

```go
func (rom *RebalanceObjectiveManager) Objective() LBRebalancingObjective {
    rom.mu.RLock()
    defer rom.mu.RUnlock()

    return rom.mu.obj
}
```

**调用频率**: 非常高,每次 Allocator 决策都会调用

**调用路径示例**:

```
Replicate Queue 处理一个 Replica
  ↓
Allocator.AllocateVoter() 选择新 Replica 位置
  ↓
需要知道当前均衡目标(QPS 还是 CPU?)
  ↓
rom.Objective()
  ↓
返回 LBRebalancingCPU
  ↓
Allocator 按 CPU 负载排序候选 Store
```

**性能优化**: 使用 RWMutex 允许并发读取

**无锁方案是否可行?**

```go
// 假设使用 atomic.Value
type RebalanceObjectiveManager struct {
    obj atomic.Value  // stores LBRebalancingObjective
}

func (rom *RebalanceObjectiveManager) Objective() LBRebalancingObjective {
    return rom.obj.Load().(LBRebalancingObjective)
}
```

**问题**: 回调函数如何保护?

```go
type RebalanceObjectiveManager struct {
    obj      atomic.Value
    onChange func(...)  // 如何保护?
}
```

**结论**: 当前的 RWMutex 方案更简单,性能足够

### 3.5 ToDimension: 目标到维度的映射

**函数实现** (rebalance_objective.go:122-131):

```go
func (d LBRebalancingObjective) ToDimension() load.Dimension {
    switch d {
    case LBRebalancingQueries:
        return load.Queries
    case LBRebalancingCPU:
        return load.CPU
    default:
        panic("unknown dimension")
    }
}
```

**load.Dimension 的定义** (pkg/kv/kvserver/allocator/load/dimension.go:19-27):

```go
type Dimension int

const (
    Queries Dimension = iota  // 0
    CPU                       // 1

    nDimensionsTyped
    nDimensions = int(nDimensionsTyped)
)
```

**用途**: Allocator 内部使用多维负载向量

```go
// pkg/kv/kvserver/allocator/load/vector.go
type Vector [nDimensions]float64  // [2]float64

// 示例:
vec := load.Vector{
    load.Queries: 1250.5,  // QPS
    load.CPU:     45.2,    // CPU 秒/秒
}
```

**Allocator 如何使用**:

```go
objective := rebalanceObjManager.Objective()
dim := objective.ToDimension()  // load.CPU

// 按指定维度排序 Store
stores := storePool.GetStoreList()
sort.Slice(stores, func(i, j int) bool {
    return stores[i].Capacity.Load[dim] < stores[j].Capacity.Load[dim]
})
```

---

## 四、运行时行为：目标如何影响系统决策？(Runtime Behavior)

### 4.1 Allocator 决策: 选择 Replica 放置位置

**场景**: Replicate Queue 需要为一个 Range 选择新的 Replica 位置

**决策流程**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go
func (a *Allocator) AllocateVoter(
    ctx context.Context,
    conf roachpb.SpanConfig,
    existingVoters []roachpb.ReplicaDescriptor,
    existingNonVoters []roachpb.ReplicaDescriptor,
) (*roachpb.StoreDescriptor, string, error) {
    // 1. 获取当前均衡目标
    objective := a.storePool.RebalanceObjectiveProvider.Objective()
    dim := objective.ToDimension()

    // 2. 获取候选 Store 列表
    candidates := a.storePool.GetStoreList(storepool.StoreFilterNone)

    // 3. 排除不合格的 Store
    candidates = a.filterInvalidStores(candidates, existingVoters)

    // 4. 按负载维度排序
    sort.Slice(candidates, func(i, j int) bool {
        return candidates[i].Capacity.Load[dim] < candidates[j].Capacity.Load[dim]
    })

    // 5. 选择负载最低的 Store
    return &candidates[0], "chose least loaded store", nil
}
```

**QPS vs CPU 的区别**:

**场景**: 选择 Replica 放置位置

| Store | QPS | CPU (秒/秒) | Range 数 |
|-------|-----|-------------|---------|
| Store-1 | 1000 | 10 | 500 |
| Store-2 | 5000 | 50 | 1000 |
| Store-3 | 10000 | 30 | 2000 |

**如果目标 = LBRebalancingQueries**:

```
dim = load.Queries
排序: Store-1 (1000) < Store-2 (5000) < Store-3 (10000)
选择: Store-1
```

**如果目标 = LBRebalancingCPU**:

```
dim = load.CPU
排序: Store-1 (10) < Store-3 (30) < Store-2 (50)
选择: Store-1 (相同)
```

**不同的场景**:

| Store | QPS | CPU (秒/秒) |
|-------|-----|-------------|
| Store-1 | 1000 | 80 |
| Store-2 | 5000 | 20 |
| Store-3 | 10000 | 50 |

**如果目标 = LBRebalancingQueries**:

```
选择: Store-1 (QPS 最低)
结果: 将 Replica 放到 CPU 最高的 Store! ❌
```

**如果目标 = LBRebalancingCPU**:

```
选择: Store-2 (CPU 最低)
结果: 将 Replica 放到 CPU 最低的 Store ✅
```

### 4.2 Load-Based Splitter: 决定 Range 分裂策略

**场景**: Range 负载过高,需要分裂

**`loadBasedSplitter` 的职责**:

```
Range-1 的 QPS = 5000 (超过阈值 2500)
  ↓
loadBasedSplitter 决定分裂点
  ↓
根据当前均衡目标选择策略:
  - QPS 目标 → 按请求数量分裂(找到请求密集的 key 范围)
  - CPU 目标 → 按 CPU 使用率分裂(找到 CPU 密集的 key 范围)
```

**ToSplitObjective 的作用**:

```go
// pkg/kv/kvserver/replica_split_load.go
func (obj LBRebalancingObjective) ToSplitObjective() split.SplitObjective {
    switch obj {
    case LBRebalancingQueries:
        return split.SplitQPS
    case LBRebalancingCPU:
        return split.SplitCPU
    default:
        panic("unknown objective")
    }
}
```

**分裂策略的区别**:

**假设 Range-1 包含以下 key 范围**:

| Key 范围 | QPS | CPU (ms/sec) |
|---------|-----|--------------|
| [a, b) | 1000 | 5000 |
| [b, c) | 500 | 200 |
| [c, d) | 3000 | 800 |
| [d, e) | 500 | 4000 |

**总计**: QPS=5000, CPU=10000ms=10s

**如果目标 = SplitQPS**:

```
目标: 将 QPS 分成两半(各 2500)

分裂点计算:
  [a, b) + [b, c) = 1500 QPS
  [a, b) + [b, c) + [c, d) = 4500 QPS ← 最接近 2500

选择分裂点: c
结果:
  Range-1a: [a, c) → QPS=1500, CPU=5200ms
  Range-1b: [c, e) → QPS=3500, CPU=4800ms
```

**如果目标 = SplitCPU**:

```
目标: 将 CPU 分成两半(各 5s)

分裂点计算:
  [a, b) = 5000ms ← 最接近 5000ms

选择分裂点: b
结果:
  Range-1a: [a, b) → QPS=1000, CPU=5000ms
  Range-1b: [b, e) → QPS=4000, CPU=5000ms
```

**哪个更好?**

```
SplitQPS 的结果:
  Range-1a: CPU=5200ms (高)
  Range-1b: CPU=4800ms (稍高)

SplitCPU 的结果:
  Range-1a: CPU=5000ms
  Range-1b: CPU=5000ms (完美均衡)
```

**结论**: 当请求 CPU 开销不均匀时,SplitCPU 更准确

### 4.3 Store Rebalancer: 主动负载均衡

**Store Rebalancer 的职责**:

```
定期检查 Store 级别的负载
  ↓
如果本地 Store 过载 → 主动转移 lease/replica
  ↓
转移目标: 负载最低的 Store
```

**如何使用 RebalanceObjectiveManager**:

```go
// pkg/kv/kvserver/store_rebalancer.go
func (sr *StoreRebalancer) rebalance(ctx context.Context) {
    // 1. 获取当前目标
    objective := sr.rebalanceObjManager.Objective()
    dim := objective.ToDimension()

    // 2. 计算本地 Store 的负载
    localLoad := sr.store.Capacity().Load[dim]

    // 3. 计算集群平均负载
    storeList := sr.storePool.GetStoreList()
    var totalLoad float64
    for _, store := range storeList {
        totalLoad += store.Capacity.Load[dim]
    }
    meanLoad := totalLoad / float64(len(storeList))

    // 4. 判断是否过载
    if localLoad < meanLoad * 1.05 {
        return  // 不过载,无需再平衡
    }

    // 5. 选择要转移的 Range
    hottestRanges := sr.getRangesByLoad(dim)  // 按指定维度排序

    // 6. 转移 lease
    for _, r := range hottestRanges {
        target := sr.allocator.TransferLeaseTarget(r, dim)
        r.TransferLease(ctx, target)
    }
}
```

**QPS vs CPU 的影响**:

**场景**: Store-1 需要转移 lease

| Range | QPS | CPU (ms/sec) |
|-------|-----|--------------|
| Range-1 | 100 | 5000 |
| Range-2 | 5000 | 100 |
| Range-3 | 1000 | 1000 |

**如果目标 = LBRebalancingQueries**:

```
dim = load.Queries
hottestRanges = [Range-2 (5000), Range-3 (1000), Range-1 (100)]

转移顺序:
  1. Range-2 → 转移可减少 5000 QPS
  2. Range-3 → 转移可减少 1000 QPS
  3. Range-1 → 转移可减少 100 QPS
```

**如果目标 = LBRebalancingCPU**:

```
dim = load.CPU
hottestRanges = [Range-1 (5000), Range-3 (1000), Range-2 (100)]

转移顺序:
  1. Range-1 → 转移可减少 5000ms CPU
  2. Range-3 → 转移可减少 1000ms CPU
  3. Range-2 → 转移可减少 100ms CPU
```

**效果对比**:

**假设 Store-1 总负载**: QPS=6100, CPU=6100ms

**QPS 目标**: 转移 Range-2

```
转移后:
  Store-1: QPS=1100 (-5000), CPU=6000ms (-100)

CPU 仍然很高! 治标不治本
```

**CPU 目标**: 转移 Range-1

```
转移后:
  Store-1: QPS=6000 (-100), CPU=1100ms (-5000)

CPU 显著降低! 问题解决
```

### 4.4 目标切换的瞬时影响

**场景**: 目标从 QPS 切换到 CPU

**时间线**:

```
T0: 目标 = LBRebalancingQueries
  - Allocator 按 QPS 选择 Store
  - Load-Based Splitter 按 QPS 分裂
  - Store Rebalancer 按 QPS 转移 lease

T1: DBA 执行 SET CLUSTER SETTING
  SET CLUSTER SETTING kv.allocator.load_based_rebalancing.objective = 'cpu';

T2: 配置变更触发回调
  → maybeUpdateRebalanceObjective()
  → ResolveLBRebalancingObjective()
  → 检查 grunning.Supported: true
  → 检查所有 Store 的 CPUPerSecond: 都有效
  → 返回 LBRebalancingCPU

T3: 目标变更
  prev = LBRebalancingQueries
  next = LBRebalancingCPU
  → log.Infof("Updating objective from qps to cpu")

T4: 调用 onChange 回调
  → VisitReplicas() 遍历 2000 个 Replica
  → 每个 Replica 更新 loadBasedSplitter:
    loadBasedSplitter.SetSplitObjective(SplitCPU)
  → 耗时: ~20ms

T5: 新请求使用新目标
  - Allocator.AllocateVoter()
    → objective = rom.Objective()  // 返回 LBRebalancingCPU
    → dim = load.CPU
    → 按 CPU 负载排序
  - Store Rebalancer 下一次运行(1 分钟后)
    → 按 CPU 负载选择要转移的 Range
```

**并发请求的处理**:

```
假设 T3 时刻有 10 个并发请求正在 Allocator 中:

Goroutine 1: AllocateVoter() 在 T2 时刻调用 rom.Objective()
  → 读取到 LBRebalancingQueries
  → 按 QPS 排序 (旧逻辑)

Goroutine 2: AllocateVoter() 在 T4 时刻调用 rom.Objective()
  → 读取到 LBRebalancingCPU
  → 按 CPU 排序 (新逻辑)

结果: 短暂的不一致,但无害
  - Goroutine 1 的决策基于 QPS,可能不是最优
  - 但不会违反正确性(Zone Config 约束仍然满足)
  - 下次再平衡时会纠正
```

**长期影响**:

```
T5 ~ T10 (0-5 分钟后):
  - 新的 Replica 放置按 CPU 均衡
  - 旧的 Replica 仍然按 QPS 分布

T10 ~ T60 (5-60 分钟后):
  - Store Rebalancer 逐渐转移 lease
  - 按 CPU 负载高的 Range 优先转移
  - 负载逐渐趋于 CPU 均衡

T60+ (1 小时后):
  - 达到新的稳态
  - CPU 负载基本均衡
  - QPS 可能不均衡(但这是预期的)
```

---

## 五、具体案例：完整的目标切换过程 (Concrete Example)

### 5.1 初始状态：混合架构集群

**集群配置**:
- 4 个节点,每个节点 1 个 Store
- Node-1, Node-2, Node-3: Linux amd64 (支持 grunning)
- Node-4: macOS ARM (不支持 grunning)
- 配置: `kv.allocator.load_based_rebalancing.objective = "cpu"`

**初始化时的目标解析**:

```
T0 = Store-1 启动
T1: newRebalanceObjectiveManager() 调用
T2: ResolveLBRebalancingObjective() 执行
  - 配置: "cpu"
  - 检查 grunning.Supported: true (Node-1 是 Linux)
  - 查询 StorePool.GetStores():
    Store-1 (Node-1): CPUPerSecond = 45.2
    Store-2 (Node-2): CPUPerSecond = 52.1
    Store-3 (Node-3): CPUPerSecond = 38.7
    Store-4 (Node-4): CPUPerSecond = -1 ← 不支持!
  - 遍历检查:
    for store in stores:
      if store.CPUPerSecond == -1:  // Store-4 匹配
        log.Warningf("cpu timekeeping unavailable on node 4")
        return LBRebalancingQueries
T3: 设置 rom.mu.obj = LBRebalancingQueries
T4: 日志输出
  [rebalance-objective] cpu timekeeping unavailable on node 4 but available locally,
  reverting to qps balance objective
```

**结果**: 即使配置是 "cpu",实际使用 QPS 均衡

### 5.2 触发事件：Node-4 下线

**时间**: 10:00:00

**事件**: Node-4 发生硬件故障,从集群中移除

**Gossip 传播**:

```
10:00:05: Node-4 停止心跳
10:00:15: 其他节点的 liveness 检测到 Node-4 down
10:00:20: StorePool 将 Store-4 标记为 dead
  → StorePool.GetStores() 不再返回 Store-4
```

**目标重新评估**:

```
10:00:25: Store Rebalancer 运行(定期任务)
  → 没有直接触发 maybeUpdateRebalanceObjective()
  → 目标仍然是 LBRebalancingQueries

问题: 容量变更监听器不会触发!
  因为:
    - Store-4 直接从 StorePool 移除
    - 没有 capacityChanged(storeID=4, old={CPUPerSecond=-1}, cur={不存在})
    - 监听器的条件 (old < 0) != (cur < 0) 不满足
```

**实际触发路径**: 下次配置变更或版本升级

### 5.3 人工触发：DBA 重新设置配置

**时间**: 10:05:00

**事件**: DBA 发现 Node-4 已移除,手动触发配置重载

```sql
-- 先设置为 qps(无操作,但触发回调)
SET CLUSTER SETTING kv.allocator.load_based_rebalancing.objective = 'qps';

-- 再设置为 cpu(期望切换)
SET CLUSTER SETTING kv.allocator.load_based_rebalancing.objective = 'cpu';
```

**第一次设置 (qps)**:

```
10:05:00.100: SQL 层执行 SET
10:05:00.101: LoadBasedRebalancingObjective.SetOnChange 触发
10:05:00.102: maybeUpdateRebalanceObjective() 执行
  → rom.mu.Lock()
  → prev = LBRebalancingQueries
  → next = ResolveLBRebalancingObjective()
    - 配置: "qps"
    - 快速返回: LBRebalancingQueries
  → prev == next (都是 Queries)
  → return (不调用回调)
  → rom.mu.Unlock()
10:05:00.103: 完成,耗时 ~2ms
```

**第二次设置 (cpu)**:

```
10:05:01.200: SQL 层执行 SET
10:05:01.201: LoadBasedRebalancingObjective.SetOnChange 触发
10:05:01.202: maybeUpdateRebalanceObjective() 执行
  → rom.mu.Lock()
  → prev = LBRebalancingQueries
  → next = ResolveLBRebalancingObjective()
    - 配置: "cpu"
    - 检查 grunning.Supported: true
    - 查询 StorePool.GetStores():
      Store-1: CPUPerSecond = 47.8
      Store-2: CPUPerSecond = 61.2
      Store-3: CPUPerSecond = 41.5
      (Store-4 不在列表中)
    - 遍历检查: 所有 Store 的 CPUPerSecond >= 0
    - 返回: LBRebalancingCPU
  → prev != next (Queries != CPU)
  → log.Infof("Updating objective from qps to cpu")
  → rom.mu.obj = LBRebalancingCPU
  → rom.mu.onChange(ctx, LBRebalancingCPU)
    ↓
    VisitReplicas() 遍历 2000 个 Replica
      for each r:
        r.loadBasedSplitter.SetSplitObjective(
          now,
          LBRebalancingCPU.ToSplitObjective(),  // SplitCPU
        )
      耗时: 2000 × 10μs = 20ms
  → rom.mu.Unlock()
10:05:01.223: 完成,耗时 ~23ms
```

**日志输出**:

```
I210315 10:05:01.202456 [rebalance-objective] [S1]
  Updating the rebalance objective from qps to cpu
```

### 5.4 后续影响：Allocator 决策变化

**场景 1: 新 Replica 放置**

**10:05:02**: Replicate Queue 处理 Range-100,需要 up-replicate

```
AllocateVoter() 调用:
  → objective = rom.Objective()  // 返回 LBRebalancingCPU
  → dim = load.CPU
  → 获取候选 Store:
    Store-1: CPU=47.8s/s, QPS=5000
    Store-2: CPU=61.2s/s, QPS=3000
    Store-3: CPU=41.5s/s, QPS=8000
  → 按 CPU 排序: [Store-3, Store-1, Store-2]
  → 选择 Store-3 (CPU 最低)
```

**对比旧逻辑 (QPS)**:

```
如果还是 LBRebalancingQueries:
  → dim = load.Queries
  → 按 QPS 排序: [Store-2 (3000), Store-1 (5000), Store-3 (8000)]
  → 选择 Store-2 (QPS 最低)

区别:
  - 旧逻辑: 选择 Store-2 (QPS 低但 CPU 高)
  - 新逻辑: 选择 Store-3 (CPU 低但 QPS 高)
```

**场景 2: Range 分裂决策**

**10:05:10**: Range-200 的 CPU 使用率达到 80s/s,触发分裂

**Range-200 的 key 范围分析**:

| Key 范围 | QPS | CPU (s/s) |
|---------|-----|-----------|
| [user:1000, user:2000) | 500 | 30 |
| [user:2000, user:3000) | 100 | 5 |
| [user:3000, user:4000) | 800 | 15 |
| [user:4000, user:5000) | 200 | 30 |

**总计**: QPS=1600, CPU=80s

**旧逻辑 (SplitQPS)**:

```
目标: 将 QPS 分成两半(各 800)
  [user:1000, user:2000) + [user:2000, user:3000) = 600 QPS
  [user:1000, user:2000) + ... + [user:3000, user:4000) = 1400 QPS ← 最接近

分裂点: user:3000
结果:
  Range-200a: [user:1000, user:3000) → QPS=600, CPU=35s
  Range-200b: [user:3000, user:5000) → QPS=1000, CPU=45s
```

**新逻辑 (SplitCPU)**:

```
目标: 将 CPU 分成两半(各 40s)
  [user:1000, user:2000) + [user:2000, user:3000) = 35s
  [user:1000, user:2000) + ... + [user:3000, user:4000) = 50s ← 最接近

分裂点: user:3000 (相同!)

但决策理由不同:
  - 旧逻辑: 因为 QPS 平衡
  - 新逻辑: 因为 CPU 平衡
```

**在这个例子中恰好相同,但通常会不同**

### 5.5 长期效果：负载重新分布

**时间**: 10:05:01 ~ 10:15:01 (10 分钟后)

**Store Rebalancer 活动**:

```
10:06:01: Store Rebalancer 第一次运行(目标已切换)
  → 计算各 Store 的 CPU 负载
  → Store-2 (61.2s) 过载(平均 50.2s × 1.2 = 60.2s)
  → 选择 Store-2 上 CPU 最高的 Range 转移 lease
  → 转移 5 个 Range 的 lease 到 Store-1, Store-3

10:07:01: 第二次运行
  → Store-2: 58.5s (降低了)
  → 仍然过载,继续转移

10:08:01 ~ 10:15:01: 持续转移
  → 每分钟转移 5-10 个 lease
```

**负载变化趋势**:

| 时间 | Store-1 CPU | Store-2 CPU | Store-3 CPU | 均值 | 标准差 |
|------|-------------|-------------|-------------|------|-------|
| 10:05:00 | 47.8 | 61.2 | 41.5 | 50.2 | 8.2 |
| 10:06:00 | 49.1 | 59.8 | 43.2 | 50.7 | 7.0 |
| 10:07:00 | 50.2 | 58.1 | 44.9 | 51.1 | 5.5 |
| 10:08:00 | 51.5 | 56.2 | 46.8 | 51.5 | 3.9 |
| 10:10:00 | 52.8 | 54.1 | 49.2 | 52.0 | 2.0 |
| 10:15:00 | 51.9 | 52.5 | 51.7 | 52.0 | 0.3 |

**QPS 分布的变化**:

| 时间 | Store-1 QPS | Store-2 QPS | Store-3 QPS | 均值 | 标准差 |
|------|-------------|-------------|-------------|------|-------|
| 10:05:00 | 5000 | 3000 | 8000 | 5333 | 2055 |
| 10:15:00 | 6200 | 4800 | 7500 | 6167 | 1113 |

**观察**:
- CPU 负载显著趋于均衡(标准差从 8.2 降到 0.3)
- QPS 分布有所改善但不是主要目标(标准差从 2055 降到 1113)

---

## 六、设计权衡：为什么这样设计？(Trade-offs)

### 6.1 动态切换 vs 静态配置

**当前设计**: 动态切换,运行时可改

```sql
SET CLUSTER SETTING kv.allocator.load_based_rebalancing.objective = 'cpu';
```

**优点**:
1. **无需重启**: DBA 可以立即调整策略
2. **A/B 测试**: 可以在线对比不同策略的效果
3. **故障恢复**: 如果 CPU 策略表现不佳,可快速回退到 QPS

**缺点**:
1. **瞬时不一致**: 切换时,不同 goroutine 可能看到不同目标
2. **复杂性**: 需要监听器、回调等机制

**替代方案 1**: 静态配置,启动时确定

```go
// 启动参数
--load-balance-objective=cpu
```

**优点**:
- 简单,无需监听器
- 一致性强,所有组件使用相同目标

**缺点**:
- 变更需要重启
- 无法快速响应负载特征变化

**为什么选择动态?**

CockroachDB 的设计理念:
> 尽可能减少需要重启的操作,提高运维灵活性

### 6.2 全局降级 vs 部分降级

**当前设计**: 全局降级,只要有一个 Store 不支持 CPU 测量,全部降级到 QPS

```go
for _, desc := range descs {
    if desc.Capacity.CPUPerSecond == -1 {
        return LBRebalancingQueries  // 全部降级
    }
}
```

**优点**:
1. **决策一致性**: 所有 Allocator 决策基于相同维度
2. **避免偏见**: 不会因为部分 Store 缺失数据而被误判

**缺点**:
1. **降级激进**: 即使只有 1/10 的 Store 不支持,也会降级
2. **无法利用部分数据**: 支持的 Store 之间可以用 CPU 均衡

**替代方案**: 部分降级,对不支持的 Store 使用 QPS,其他用 CPU

```go
func (a *Allocator) AllocateVoter(...) {
    objective := a.rebalanceObjManager.Objective()

    for _, store := range candidates {
        var load float64
        if objective == LBRebalancingCPU && store.Capacity.CPUPerSecond >= 0 {
            load = store.Capacity.CPUPerSecond
        } else {
            load = store.Capacity.QueriesPerSecond
        }
        // 按 load 排序
    }
}
```

**问题**:

```
假设:
  Store-1: CPU=50s, QPS=5000 (支持 CPU)
  Store-2: CPU=-1, QPS=3000 (不支持 CPU)

按 load 排序:
  Store-2: load=3000
  Store-1: load=50

选择: Store-1 (load 更小)

但这是在比较苹果和橘子!
  - Store-1 的 50 是秒/秒
  - Store-2 的 3000 是请求/秒
  - 完全不可比
```

**为什么选择全局降级?**

```
保证决策一致性 > 利用部分数据

理由:
  - 不一致的决策可能导致负载振荡
  - 混合版本集群通常是临时状态(升级过程中)
  - 宁愿短期降级,也不要长期不稳定
```

### 6.3 配置监听 vs 定期轮询

**当前设计**: 配置监听,变更时立即触发

```go
LoadBasedRebalancingObjective.SetOnChange(&rom.st.SV, func(ctx context.Context) {
    rom.maybeUpdateRebalanceObjective(rom.AnnotateCtx(ctx))
})
```

**优点**:
1. **实时响应**: 配置变更后立即生效
2. **低延迟**: 无需等待下一次轮询

**缺点**:
1. **复杂性**: 需要回调机制
2. **并发控制**: 需要锁保护

**替代方案**: 定期轮询

```go
func (rom *RebalanceObjectiveManager) Run(ctx context.Context) {
    ticker := time.NewTicker(1 * time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            rom.maybeUpdateRebalanceObjective(ctx)
        case <-ctx.Done():
            return
        }
    }
}
```

**优点**:
- 简单,无需监听器
- 自然的批处理(1 分钟内的多次变更合并)

**缺点**:
- 延迟高(最多 1 分钟)
- 浪费 CPU(即使配置不变也要检查)

**为什么选择监听?**

```
配置变更频率: 极低(人工操作,可能几天一次)
配置变更重要性: 高(DBA 期望立即生效)

结论: 监听器的复杂性是值得的
```

### 6.4 锁内回调 vs 锁外回调

**当前设计**: 回调在锁内执行

```go
rom.mu.Lock()
defer rom.mu.Unlock()
// ...
rom.mu.onChange(ctx, rom.mu.obj)  // ← 锁内
```

**优点**:
1. **一致性**: 回调看到的目标与 `rom.mu.obj` 一致
2. **简单**: 无需复杂的同步机制

**缺点**:
1. **阻塞读取**: 回调执行期间(~20ms),其他读取被阻塞

**替代方案**: 回调在锁外执行

```go
rom.mu.Lock()
prev := rom.mu.obj
next := ResolveLBRebalancingObjective(...)
if prev == next {
    rom.mu.Unlock()
    return
}
rom.mu.obj = next
callback := rom.mu.onChange
rom.mu.Unlock()  // ← 提前解锁

callback(ctx, next)  // ← 锁外执行
```

**问题**:

```
时间线:

Goroutine 1: 配置变更触发
  T1: Lock
  T2: rom.mu.obj = LBRebalancingCPU
  T3: Unlock
  T4: callback() 开始 (~20ms)

Goroutine 2: Allocator 读取
  T2.5: RLock
  T2.6: obj = rom.mu.obj  // 读到 LBRebalancingCPU
  T2.7: RUnlock
  T2.8: dim = obj.ToDimension()  // load.CPU

Goroutine 3: 另一个配置变更触发
  T3.5: Lock
  T3.6: rom.mu.obj = LBRebalancingQueries
  T3.7: Unlock
  T3.8: callback() 开始

此时:
  - Goroutine 1 的 callback 还在执行(更新 Replica 为 SplitCPU)
  - Goroutine 3 的 callback 也在执行(更新 Replica 为 SplitQPS)
  - 两个 callback 并发遍历 Replica!

结果:
  - 竞态条件:某些 Replica 可能被设置两次
  - 最终状态不确定
```

**为什么选择锁内回调?**

```
变更频率: 极低(几天一次)
回调耗时: 可接受(20ms)
阻塞影响: 微小(读取被阻塞 20ms,但读取频率高,单次延迟低)

结论: 简单性和一致性 > 并发性能
```

### 6.5 回调遍历所有 Replica vs 惰性更新

**当前设计**: 目标变更时,立即遍历所有 Replica 更新 loadBasedSplitter

```go
s.VisitReplicas(func(r *Replica) (wantMore bool) {
    r.loadBasedSplitter.SetSplitObjective(...)
    return true
})
```

**优点**:
1. **一致性**: 所有 Replica 立即使用新目标
2. **简单**: 无需记录"哪些 Replica 已更新"

**缺点**:
1. **耗时**: 2000 个 Replica × 10μs = 20ms
2. **阻塞**: 锁持有时间长

**替代方案**: 惰性更新,Replica 下次分裂时才更新

```go
// loadBasedSplitter 中
func (lbs *loadBasedSplitter) MaybeSplitKey(ctx) {
    // 每次调用时检查目标是否变化
    currentObj := lbs.store.rebalanceObjManager.Objective()
    if lbs.cachedObjective != currentObj {
        lbs.SetSplitObjective(now, currentObj.ToSplitObjective())
        lbs.cachedObjective = currentObj
    }
    // 继续分裂逻辑
}
```

**优点**:
- 回调无需遍历,瞬间完成
- 锁持有时间短(<1ms)

**缺点**:
- 不一致窗口:部分 Replica 使用旧目标,部分使用新目标
- 复杂性:需要缓存和比较

**为什么选择立即遍历?**

```
Range 分裂频率: 低(可能几小时一次)
如果惰性更新:
  - 目标切换后,可能数小时内都不会分裂
  - 一旦分裂,可能仍使用旧目标(如果缓存未更新)

立即遍历:
  - 虽然耗时 20ms,但保证一致性
  - 变更频率极低,可接受
```

---

## 七、核心总结：RebalanceObjectiveManager 的本质 (Summary)

### 7.1 一句话总结

**RebalanceObjectiveManager 是一个动态配置管理器,它监听集群配置、版本变化和 Store 容量变更,在 QPS 均衡和 CPU 均衡两种策略之间自适应切换,并在切换时通知所有相关组件(Allocator、Load-Based Splitter、Store Rebalancer)更新决策策略,确保集群在不同负载特征下都能实现最优的负载均衡。**

### 7.2 核心机制

**三层架构**:

```
┌─────────────────────────────────────────────────────────────┐
│ 第一层: 监听触发源                                            │
└─────────────────────────────────────────────────────────────┘
  1. 配置变更: SET CLUSTER SETTING
  2. 版本升级: 集群版本变化
  3. 容量变更: Store.Capacity.CPUPerSecond 有效性变化

┌─────────────────────────────────────────────────────────────┐
│ 第二层: 目标解析                                              │
└─────────────────────────────────────────────────────────────┘
  ResolveLBRebalancingObjective():
    - 读取配置 (默认 "cpu")
    - 检查本地 grunning.Supported
    - 检查所有 Store 的 CPUPerSecond
    - 决定: LBRebalancingCPU 或 LBRebalancingQueries (降级)

┌─────────────────────────────────────────────────────────────┐
│ 第三层: 通知传播                                              │
└─────────────────────────────────────────────────────────────┘
  maybeUpdateRebalanceObjective():
    - 对比 prev vs next
    - 如果变化 → 调用 onChange 回调
      → VisitReplicas() 更新所有 Replica 的 loadBasedSplitter
```

**关键设计原则**:

1. **自适应降级**: 优雅处理异构集群(部分节点不支持 CPU 测量)
2. **全局一致性**: 要么全部 CPU,要么全部 QPS,不混用
3. **实时响应**: 配置变更立即生效,无需重启
4. **回调通知**: 目标变更时,自动更新所有依赖组件

### 7.3 心智模型

**比喻: 交通信号灯的控制策略**

```
城市交通管理中心 (RebalanceObjectiveManager)
  ↓
监听三种信号:
  1. 市长命令 (配置变更): "改用智能信号灯"
  2. 系统升级 (版本变化): "新系统支持车流量传感器"
  3. 设备状态 (容量变更): "某个路口的传感器坏了"
  ↓
决策:
  if 所有路口都有传感器:
    策略 = 按实时车流量调整
  else:
    策略 = 按固定时间调整
  ↓
下发指令:
  遍历所有路口的信号灯
  更新控制策略
```

**核心类比**:

| 交通系统 | RebalanceObjectiveManager |
|---------|--------------------------|
| 传感器是否可用 | grunning.Supported / CPUPerSecond >= 0 |
| 车流量传感器 | CPU 测量 |
| 固定时间调整 | QPS 均衡 (粗粒度) |
| 实时车流量调整 | CPU 均衡 (精细) |
| 策略切换 | 目标切换 (QPS ↔ CPU) |
| 所有路口 | 所有 Replica |

### 7.4 关键代码位置

| 功能 | 文件 | 行号 |
|------|------|------|
| 构造函数 | pkg/kv/kvserver/rebalance_objective.go | 178-224 |
| 目标解析 | pkg/kv/kvserver/rebalance_objective.go | 256-289 |
| 目标更新 | pkg/kv/kvserver/rebalance_objective.go | 234-251 |
| 读取目标 | pkg/kv/kvserver/rebalance_objective.go | 227-232 |
| Store 中创建 | pkg/kv/kvserver/store.go | 1515-1531 |
| 目标枚举 | pkg/kv/kvserver/rebalance_objective.go | 31-88 |
| 维度映射 | pkg/kv/kvserver/rebalance_objective.go | 122-131 |

### 7.5 设计亮点

1. **接口抽象**: `gossipStoreDescriptorProvider` 和 `gossipStoreCapacityChangeNotifier` 解耦了与 StorePool 的依赖,便于测试

2. **三重监听**: 配置、版本、容量三个维度的监听,确保不遗漏任何触发场景

3. **XOR 条件**: `(old < 0) != (cur < 0)` 优雅地检测 CPU 测量能力的变化

4. **提前返回优化**: `if prev == next { return }` 避免无意义的回调

5. **日志记录**: 目标变更时记录日志,便于运维追踪

6. **RWMutex**: 允许高频并发读取,低频串行写入

### 7.6 局限性

1. **容量变更监听的盲区**: 新节点加入时,容量变更回调可能检测不到(因为 old 和 cur 都是 -1)

2. **回调阻塞读取**: 目标变更时,回调在锁内执行(~20ms),期间读取被阻塞

3. **全局降级过于严格**: 即使只有 1 个节点不支持 CPU 测量,整个集群也会降级

4. **惰性传播**: Allocator 和 Store Rebalancer 不是立即感知目标变更,而是下次调用 `Objective()` 时

**缓解措施**:
- 盲区: 配置变更或版本变更会重新检查
- 阻塞: 变更频率极低,可接受
- 严格降级: 保证决策一致性,权衡之选
- 惰性传播: `Objective()` 调用频率高,延迟微小

---

**本章完**

通过本章分析,我们深入理解了 CockroachDB 如何在运行时动态切换负载均衡策略:RebalanceObjectiveManager 通过监听配置、版本和容量变更,自适应地在 QPS 均衡和 CPU 均衡之间切换,并通过回调机制通知所有 Replica 更新分裂策略。这种设计在保证集群一致性的同时,提供了极大的运维灵活性,使得 DBA 可以根据实际负载特征实时调整均衡策略,无需重启集群。与前述章节的 Store.Start()、makeIOOverloadCapacityChangeFn 等机制共同构成了 CockroachDB 完整的负载管理体系。
