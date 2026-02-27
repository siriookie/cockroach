# RebalanceTarget 深度剖析——基于等价类的副本重平衡决策引擎

> **核心问题**: `RebalanceTarget` 函数在 CockroachDB 的副本调度体系中如何实现"从哪里移走副本,移到哪里"的决策?它如何平衡约束满足、负载均衡、分散性等多维度目标?
>
> **解答路径**: 本节将深入分析 RebalanceTarget 的四阶段决策流程(等价类构造、最优目标选择、乒乓预防、MMA 冲突检测),揭示其如何通过"模拟移除"机制避免决策震荡,以及为什么采用等价类(equivalence class)而非全局排序。

---

## 一、第一轮 BFS:职责边界与设计动机(Why)

### 1.1 RebalanceTarget 解决的系统性问题

**问题背景**:

在分布式数据库中,副本的初始放置(Allocation)通常只考虑当前时刻的最优性。但随着时间推移,会出现多种失衡:

```
场景 1: 负载漂移
  - 初始状态: Store1=100 ranges, Store2=100 ranges, Store3=100 ranges
  - 6 个月后: Store1=150 ranges, Store2=80 ranges, Store3=170 ranges
  - 原因: 新增 Range 的 Allocation 总是偏向某些 Store

场景 2: 约束变更
  - 初始配置: 无特殊约束
  - 管理员修改配置: 要求"至少 1 个副本在 us-west"
  - 现状: 所有副本都在 us-east
  - 需求: 将部分副本从 us-east 迁移到 us-west

场景 3: 硬件替换
  - 节点 N1 磁盘快满(95%)
  - 新节点 N5 上线(磁盘空闲)
  - 需求: 将副本从 N1 迁移到 N5

场景 4: 热点缓解
  - Range R1 的 QPS 突增到 10000 qps
  - 其所在 Store S1 的总 QPS 超过阈值
  - 需求: 将 R1 的副本(或 Lease)迁移到负载更低的 Store
```

**核心挑战**:

如果只是简单地"选一个最优目标添加副本,然后移除一个最差副本",会导致:

```
问题 1: 立即后悔(Immediate Regret)
  - 添加副本到 S5(负载低)
  - 移除副本从 S1(负载高)
  - 结果: S5 成为新的"最佳候选"
  - 下一轮: 又把 S5 的副本移走
  - 导致: 副本在 S1 和 S5 之间乒乓(Ping-Pong)

问题 2: 全局竞争(Thundering Herd)
  - 100 个 Range 都发现 S5 是最优目标
  - 同时向 S5 添加副本
  - 结果: S5 瞬间过载
  - 需要: 分散决策,避免羊群效应

问题 3: 局部最优陷阱(Local Optimum)
  - 只比较"当前 Range 的副本所在 Store"
  - 忽略"整个集群的其他可行 Store"
  - 结果: 错过更好的全局解
```

**RebalanceTarget 的设计目标**:

1. **乒乓预防(Anti-Ping-Pong)**: 通过"模拟移除"确保新添加的副本不会立即成为移除候选
2. **等价类隔离(Equivalence Class Isolation)**: 只在"至少和现有副本一样好"的候选中选择,避免降低 Range 质量
3. **多维度平衡**: 同时考虑约束满足、分散性、负载均衡、磁盘容量
4. **渐进式改善(Incremental Improvement)**: 每次只改善一个维度,避免剧烈震荡
5. **MMA 协调(Multi-Metric Allocator Coordination)**: 与集群级调度器协调,避免冲突决策

### 1.2 在系统中的位置

```
┌─────────────────────────────────────────────────────────────────┐
│ StoreRebalancer (集群级周期性调度器)                             │
│   - 每 N 秒扫描所有 Store                                        │
│   - 识别过载/不均衡的 Range                                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ ReplicaPlanner (Range 级决策引擎)                                │
│   - 调用 Allocator.RebalanceTarget()                             │
│   - 决策: 从哪个 Store 移走,移到哪个 Store                       │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Allocator.RebalanceTarget() ←【本函数】                          │
│   输入: 现有副本、约束、Store 列表                               │
│   输出: (addTarget, removeTarget, details, ok)                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Raft Membership Change (执行层)                                  │
│   - ChangeReplicas RPC                                           │
│   - 添加副本到 addTarget                                         │
│   - 等待副本追赶(Catch-Up)                                       │
│   - 移除副本从 removeTarget                                      │
└─────────────────────────────────────────────────────────────────┘
```

**上游**:
- `StoreRebalancer`: 周期性调度器,触发 Rebalance
- `ReplicaPlanner`: 协调 Allocation/Removal/Rebalance 决策

**下游**:
- `Raft MembershipChange`: 执行实际的副本添加/移除
- `StorePool`: 提供 Store 信息与统计

**横向依赖**:
- `ConstraintsChecker`: 约束验证(前序章节已分析)
- `rankedCandidateListForRebalancing`: 候选评分(前序章节已分析)
- `MMARebalanceAdvisor`: 多指标协调器(避免冲突决策)

### 1.3 核心抽象

#### 1.3.1 rebalanceOptions (等价类)

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1565-1573
type rebalanceOptions struct {
    existing   candidate                      // 现有副本(源)
    candidates candidateList                  // 可替换的候选 Store 列表(目标)
    advisor    *mmaprototype.MMARebalanceAdvisor  // MMA 协调器(惰性初始化)
}
```

**设计理念**:

"等价类"的含义: 对于每个现有副本 `existing`,只考虑"至少和它一样好"的候选作为替换目标。

```
例子:
  现有副本 s1: valid=true, necessary=false, diversityScore=0.5, balanceScore=1

  候选 s2: valid=true, necessary=false, diversityScore=0.6, balanceScore=2
  → s2.diversityScore > s1.diversityScore → s2 至少和 s1 一样好 → 加入等价类

  候选 s3: valid=false, necessary=false, diversityScore=0.8, balanceScore=2
  → s3.valid=false < s1.valid=true → s3 不如 s1 → 排除

  候选 s4: valid=true, necessary=false, diversityScore=0.4, balanceScore=2
  → s4.diversityScore < s1.diversityScore → s4 不如 s1 → 排除

结果: rebalanceOptions { existing=s1, candidates=[s2] }
```

**为什么使用等价类而非全局排序?**

```
替代方案 1: 全局排序所有 Store,选分数最高的
  问题:
    - 可能选到"比现有副本更差"的 Store
    - 导致 Range 质量下降
    - 违反"渐进式改善"原则

替代方案 2: 只比较现有副本之间,选最差的移除
  问题:
    - 只移除不添加,无法改善 Range
    - 需要先 Allocate 新副本,然后 Remove 旧副本
    - 增加网络/磁盘开销

当前方案: 等价类 + 原子替换
  优点:
    - 保证不降低 Range 质量(只选"至少一样好"的候选)
    - 原子性: 一次操作完成 Add + Remove
    - 局部性: 只比较相关 Store,降低计算复杂度
```

#### 1.3.2 equivalenceClass (内部构造)

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1588-1595
type equivalenceClass struct {
    existing    roachpb.StoreDescriptor  // 现有 Store
    candidateSL storepool.StoreList      // 候选 Store 列表(StoreList 格式,带统计)
    candidates  candidateList            // 候选 Store 列表(candidate 格式,带评分)
}
```

**与 rebalanceOptions 的区别**:

```
equivalenceClass (内部使用):
  - 构造阶段使用
  - 包含统计信息(candidateSL)
  - 用于计算 balanceScore/convergesScore

rebalanceOptions (外部接口):
  - 返回给调用者
  - 包含 MMA advisor(惰性初始化)
  - 用于迭代选择最优目标
```

### 1.4 长期存在的状态与生命周期

**无状态设计**:

`RebalanceTarget` 是一个**纯函数**(除了随机数生成器):
- 输入: 现有副本、约束配置、Store 列表、评分选项
- 输出: (addTarget, removeTarget, details, ok)
- 无副作用(除了日志)

**为什么选择无状态?**

```
优点:
  1. 可测试性: 纯函数易于单元测试
  2. 并发安全: 无共享状态,天然线程安全
  3. 可重试性: 失败后可以无代价重试
  4. 可审计性: 所有输入输出都可记录

代价:
  1. 每次调用需要传入完整上下文
  2. 无法缓存中间结果(但通过 MMA advisor 部分缓存)
```

**临时状态的生命周期**:

```
1. rankedCandidateListForRebalancing() 构造 []rebalanceOptions
   ↓ (生命周期: 函数调用期间)
2. bestRebalanceTarget() 迭代选择最优目标
   ↓ (每次迭代删除已选择的候选)
3. simulateRemoveTarget() 模拟移除,检测乒乓
   ↓ (临时修改 StorePool 统计,调用后恢复)
4. 返回 (addTarget, removeTarget)
   ↓ (销毁所有临时状态)
```

---

## 二、第二轮 BFS:控制流与组件协作(How it flows)

### 2.1 主要执行路径

```
┌────────────────────────────────────────────────────────────────┐
│ 阶段 0: 输入准备 (Lines 1905-1970)                             │
├────────────────────────────────────────────────────────────────┤
│ 1. 获取 StoreList (可能 jitter 统计)                           │
│ 2. 分析约束 (Overall + Voter)                                  │
│ 3. 选择 constraintsChecker 策略                                │
│ 4. 调用 rankedCandidateListForRebalancing()                    │
│    → 返回: []rebalanceOptions (每个包含 existing + candidates) │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ 阶段 1: 迭代选择最优目标 (Lines 2019-2120, Loop)              │
├────────────────────────────────────────────────────────────────┤
│ for { // 可能多次迭代                                           │
│   1. bestRebalanceTarget() 选择 (target, existing, bestIdx)   │
│   2. 检查是否为 Critical Rebalance (约束/磁盘/分散性)          │
│   3. 如果非 Critical,检查 MMA 冲突                             │
│      - 如果冲突,continue (选择下一个候选)                      │
│   4. 构造 existingPlusOneNew (添加 target 到副本列表)         │
│   5. simulateRemoveTarget() 模拟移除                           │
│      → 返回: removeReplica                                     │
│   6. 检查乒乓: target.StoreID != removeReplica.StoreID?        │
│      - 如果相等,continue (会立即移除刚添加的副本)             │
│   7. 成功: break                                                │
│ }                                                               │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ 阶段 2: 返回结果 (Lines 2122-2142)                             │
├────────────────────────────────────────────────────────────────┤
│ 1. 编译 details (JSON 格式,记录到 system.rangelog)             │
│ 2. 构造 addTarget, removeTarget                                │
│ 3. 返回 (addTarget, removeTarget, details, ok=true)            │
└────────────────────────────────────────────────────────────────┘
```

### 2.2 关键分支路径

#### 2.2.1 空候选列表 (Early Exit)

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1972-1974
if len(results) == 0 {
    return zero, zero, "", false
}
```

**触发条件**:
- 所有 Store 都不满足约束
- 所有 Store 都比现有副本更差
- 所有 Store 都在 Dead 节点上

**系统行为**:
- 返回 `ok=false`
- 调用者不执行 Rebalance
- 等待下一轮周期重试(可能 Store 状态已改善)

#### 2.2.2 Critical Rebalance (跳过 MMA 检查)

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:2028-2049
if !existingCandidate.isCriticalRebalance(target) {
    // 非 Critical,检查 MMA 冲突
    if advisor := results[bestIdx].advisor; advisor != nil {
        if a.as.IsInConflictWithMMA(ctx, target.store.StoreID, advisor, false) {
            continue  // MMA 拒绝,选择下一个候选
        }
    }
}
```

**Critical Rebalance 的定义**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:882-906
func (source candidate) isCriticalRebalance(target *candidate) bool {
    // 1. 约束修复: source 不满足约束,target 满足
    if !source.valid && target.valid {
        return true
    }
    // 2. 磁盘满: source 磁盘满,target 磁盘未满
    if source.fullDisk && !target.fullDisk {
        return true
    }
    // 3. 必要性: source 不必要,target 必要
    if !source.necessary && target.necessary {
        return true
    }
    // 4. Voter 必要性
    if !source.voterNecessary && target.voterNecessary {
        return true
    }
    // 5. 分散性改善: target 的 diversityScore 显著高于 source
    if !scoresAlmostEqual(source.diversityScore, target.diversityScore) {
        if target.diversityScore > source.diversityScore {
            return true
        }
    }
    return false
}
```

**为什么 Critical Rebalance 跳过 MMA 检查?**

```
原因:
  - Critical Rebalance 修复系统"坏状态"(约束违反、磁盘满、分散性差)
  - 这些状态威胁系统可用性和数据安全
  - 必须立即修复,不能因为"负载均衡冲突"而延迟

示例:
  场景: Store S1 磁盘 99% 满,需要移副本到 S5
  MMA 建议: "S5 负载已高,应该移到 S3"
  决策: 忽略 MMA,立即移到 S5(磁盘满更紧急)
```

#### 2.2.3 Voter Necessary Promotion (跳过模拟移除)

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:2085-2091
if target.voterNecessary {
    removeReplica = roachpb.ReplicationTarget{
        NodeID:  existingCandidate.store.Node.NodeID,
        StoreID: existingCandidate.store.StoreID,
    }
    break  // 直接使用 existingCandidate 作为移除目标
}
```

**场景**: NonVoter 提升为 Voter

```
配置: VoterConstraints: 至少 1 个 Voter 在 us-west
当前副本:
  - s1: Voter,   region=us-east
  - s2: NonVoter, region=us-west  ← 需要提升
  - s3: NonVoter, region=us-east

决策:
  - target = s2 (提升为 Voter)
  - target.voterNecessary = true (满足 Voter 约束)
  - existing = s1 (降级为 NonVoter 或移除)
  - 不需要模拟移除,因为已知 s1 是最优移除候选
```

**为什么跳过模拟移除?**

```
原因:
  1. 提升场景的移除目标已确定(必须移除 existingCandidate)
  2. 模拟移除可能返回其他候选(如果 existingCandidate 不是最差)
  3. 但提升场景要求"原子交换"(s2 升级,s1 降级)

如果不跳过:
  - simulateRemoveTarget() 可能返回 s3
  - 导致: 添加 s2(Voter),移除 s3(NonVoter)
  - 结果: s1 仍然是 Voter,Voter 约束仍未满足
```

### 2.3 触发方式

**被动触发** (请求驱动):

```
触发源 1: StoreRebalancer (周期性)
  - 每个 Store 独立运行 StoreRebalancer goroutine
  - 周期: 默认 10 秒
  - 触发条件: Store 的 RangeCount/QPS 高于平均值

触发源 2: ReplicaPlanner (事件驱动)
  - Raft Leadership 变更
  - Range Split/Merge 后
  - 配置变更(Zone Config Update)

触发源 3: AdminScatterRequest (手动)
  - 管理员执行 SCATTER 命令
  - 测试场景
```

**非阻塞** (异步决策):

```
RebalanceTarget() 只返回决策,不执行操作:
  1. RebalanceTarget() 返回 (addTarget, removeTarget)
  2. 调用者将决策放入 Replication Queue
  3. Queue Worker 异步执行 Raft ChangeReplicas
  4. 执行期间,RebalanceTarget() 可以继续决策其他 Range
```

### 2.4 与其他模块的交互

#### 2.4.1 StorePool (共享状态)

```
读取:
  - GetStoreList(): 获取所有 Store 的统计信息
  - GetLocalitiesByStore(): 获取 Locality 映射
  - IsStoreReadyForRoutineReplicaTransfer(): 检查 Store 是否存活

写入 (临时):
  - UpdateLocalStoreAfterRebalance(): 模拟添加副本后的统计变化
  - 使用 defer 恢复原始统计
```

**为什么需要临时修改统计?**

```
场景: 模拟移除检测乒乓
  1. 当前: s1=100 ranges, s5=80 ranges
  2. 添加副本到 s5: s5 变为 81 ranges (临时修改统计)
  3. 调用 RemoveTarget(): 评估哪个副本应该移除
     - 如果 RemoveTarget 返回 s5 → 乒乓!
     - 如果 RemoveTarget 返回 s1 → OK
  4. defer 恢复统计: s5 恢复为 80 ranges

如果不临时修改:
  - RemoveTarget 看到的是"添加前的统计"
  - 可能错误判断 s5 仍是最佳目标
  - 导致检测失败
```

#### 2.4.2 MMARebalanceAdvisor (信号驱动)

```
MMA (Multi-Metric Allocator):
  - 集群级调度器
  - 维护每个 Store 的"目标负载"
  - 目标: 最小化集群范围的负载方差

协调机制:
  1. RebalanceTarget 选择 (target, existing)
  2. 构造 MMARebalanceAdvisor (bestIdx 首次选择时)
  3. 调用 IsInConflictWithMMA(target)
     → 检查: 将副本移到 target 是否与 MMA 目标冲突
  4. 如果冲突,选择下一个候选
  5. 如果不冲突,继续执行

冲突判定:
  - MMA 认为 target 的负载应该降低,但 Rebalance 会增加负载
  - MMA 认为 existing 的负载应该增加,但 Rebalance 会降低负载
```

**为什么需要 MMA 协调?**

```
问题: 局部最优 vs 全局最优
  - RebalanceTarget 只看"当前 Range 的副本"
  - 不知道"整个集群的其他 Range 也在迁移副本"
  - 可能所有 Range 都向同一个 Store 迁移

MMA 的作用:
  - 维护全局视图
  - 拒绝"局部最优但全局有害"的决策
  - 引导 Rebalance 朝全局最优方向收敛
```

#### 2.4.3 ConstraintsChecker (策略模式)

```
根据场景选择不同的约束检查策略:

removalConstraintsChecker:
  - 用于评估"移除哪个现有副本"
  - 策略: voterConstraintsCheckerForRemoval / nonVoterConstraintsCheckerForRemoval
  - 要求: 移除后不能违反约束

rebalanceConstraintsChecker:
  - 用于评估"替换现有副本"
  - 策略: voterConstraintsCheckerForRebalance / nonVoterConstraintsCheckerForRebalance
  - 要求: 替换后约束满足度不降低
```

**Rebalance vs Removal/Allocation 的区别**:

```
Allocation: 添加新副本
  - 要求: 新副本满足约束
  - 不关心: 其他副本是否满足约束(它们已存在)

Removal: 移除现有副本
  - 要求: 移除后剩余副本仍满足约束
  - 检查: 该副本是否"必要"(移除后约束会违反?)

Rebalance: 原子替换 (Remove + Add)
  - 要求 1: 添加的副本至少和移除的副本一样好
  - 要求 2: 替换后约束满足度不降低
  - 更严格: 必须保证"不降级"
```

---

## 三、DFS 深入:关键函数与核心逻辑(How it works)

### 3.1 rankedCandidateListForRebalancing() - 等价类构造

**函数签名**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1787-1798
func rankedCandidateListForRebalancing(
    ctx context.Context,
    allStores storepool.StoreList,                    // 集群所有 Store
    removalConstraintsChecker constraintsCheckFn,     // 移除约束检查
    rebalanceConstraintsChecker rebalanceConstraintsCheckFn,  // Rebalance 约束检查
    existingVotingReplicas, existingNonVotingReplicas []roachpb.ReplicaDescriptor,
    targetType TargetReplicaType,
    existingStoreLocalities map[roachpb.StoreID]roachpb.Locality,
    isStoreValidForRoutineReplicaTransfer func(context.Context, roachpb.StoreID) bool,
    options ScorerOptions,
    metrics AllocatorMetrics,
) []rebalanceOptions
```

**三阶段执行**:

#### 阶段 1: 评估现有副本 (Lines 1799-1845)

```go
// 目标: 判断每个现有副本是否"需要被替换"
var needRebalanceFrom bool  // 是否存在"坏副本"(约束违反/磁盘满)
curDiversityScore := RangeDiversityScore(existingStoreLocalities)

for _, store := range allStores.Stores {
    for _, repl := range existingReplicasForType {
        if store.StoreID != repl.StoreID {
            continue
        }

        // 检查约束和磁盘
        valid, necessary := removalConstraintsChecker(store)
        fullDisk := !options.getDiskOptions().maxCapacityCheck(store)

        if !valid || fullDisk {
            needRebalanceFrom = true  // 标记:必须从该 Store 移除副本
            log.VEventf("s%d: should-rebalance(invalid/full-disk)", store.StoreID)
        }

        // 记录到 existingStores
        existingStores[store.StoreID] = candidate{
            store:        store,
            valid:        valid,
            necessary:    necessary,
            fullDisk:     fullDisk,
            ioOverloaded: false,  // 移除时不考虑 IO 过载(避免雪崩)
            diversityScore: curDiversityScore,
        }
    }
}
```

**关键不变量**:

```
Invariant 1: 现有副本的 diversityScore 都相同
  - 因为 diversityScore 是 Range 级别的属性
  - 不是单个副本的属性

Invariant 2: ioOverloaded 总是 false
  - 移除副本时,不考虑源 Store 的 IO 过载
  - 原因: 如果源 Store 过载,移除副本会加剧过载(需要传输数据)
  - 应该等待过载缓解后再移除
```

#### 阶段 2: 构造等价类 (Lines 1866-1992)

```go
var equivalenceClasses []equivalenceClass
var needRebalanceTo bool  // 是否存在"更好的候选"

for _, storeID := range existingStoreList {
    existing := existingStores[storeID]
    var comparableCands candidateList

    // 对于每个现有副本,找到所有"可替换"的候选
    for _, store := range allStores.Stores {
        if store.StoreID == existing.store.StoreID {
            continue  // 跳过自己
        }

        if !isStoreValidForRoutineReplicaTransfer(ctx, store.StoreID) {
            continue  // 跳过 Dead 节点
        }

        // 计算候选的评分
        constraintsOK, necessary, voterNecessary := rebalanceConstraintsChecker(store, existing.store)
        diversityScore := diversityRebalanceFromScore(
            store.Locality(), existing.store.StoreID, existingStoreLocalities)

        cand := candidate{
            store:          store,
            valid:          constraintsOK,
            necessary:      necessary,
            voterNecessary: promotionCandidate && voterNecessary,
            fullDisk:       !options.getDiskOptions().maxCapacityCheck(store),
            diversityScore: diversityScore,
        }

        // 关键: 只添加"不比 existing 更差"的候选
        if !cand.less(existing) {
            comparableCands = append(comparableCands, cand)

            // 如果 cand 比 existing 更好,标记需要 Rebalance
            if !needRebalanceFrom && !needRebalanceTo && existing.less(cand) {
                needRebalanceTo = true
                log.VEventf("should-rebalance(better-candidate)")
            }
        }
    }

    // 排序候选列表
    sort.Sort(sort.Reverse(byScore(comparableCands)))

    // 提取"best"等价类(与第一名在关键维度上等价的所有候选)
    bestCands := comparableCands.best()

    // 构造 equivalenceClass
    eqClass := equivalenceClass{
        existing:    existing.store,
        candidateSL: storepool.MakeStoreList(bestStores),
        candidates:  bestCands,
    }
    equivalenceClasses = append(equivalenceClasses, eqClass)
}
```

**等价性判定**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1958
if !cand.less(existing) {
    // cand 不比 existing 更差
    comparableCands = append(comparableCands, cand)
}
```

**less() 的定义** (回顾前序章节):

```go
func (c candidate) less(o candidate) bool {
    return c.compare(o) < 0
}

func (c candidate) compare(o candidate) float64 {
    // 返回正数: c 更好
    // 返回负数: o 更好
    // 返回 0: 相等

    // 优先级:
    // 1. valid (600 points)
    // 2. !fullDisk (500 points)
    // 3. necessary (400 points)
    // 4. voterNecessary (350 points)
    // 5. diversityScore (300 points)
    // 6. !ioOverloaded (250 points)
    // 7. convergesScore (200 points)
    // 8. balanceScore (150 points)
    // 9. hasNonVoter (100 points)
    // 10. rangeCount (相对差异)
}
```

**示例**:

```
现有副本 existing:
  - valid=true, fullDisk=false, necessary=false
  - diversityScore=0.5, balanceScore=1, rangeCount=100

候选 cand1:
  - valid=true, fullDisk=false, necessary=false
  - diversityScore=0.6, balanceScore=2, rangeCount=90
  - compare(cand1, existing) = +300 (diversityScore 更高)
  - !cand1.less(existing) → 加入等价类

候选 cand2:
  - valid=true, fullDisk=false, necessary=false
  - diversityScore=0.4, balanceScore=2, rangeCount=80
  - compare(cand2, existing) = -300 (diversityScore 更低)
  - cand2.less(existing) → 排除

候选 cand3:
  - valid=false, fullDisk=false, necessary=false
  - diversityScore=0.8, balanceScore=2, rangeCount=70
  - compare(cand3, existing) = -600 (valid 更差)
  - cand3.less(existing) → 排除

结果: equivalenceClass { existing, candidates=[cand1] }
```

#### 阶段 3: 计算 balanceScore 和 convergesScore (Lines 1999-2099)

```go
needRebalance := needRebalanceFrom || needRebalanceTo
var shouldRebalanceCheck bool

if !needRebalance {
    // 如果没有"明显需要 Rebalance"的信号,检查是否基于阈值应该 Rebalance
    for _, eqClass := range equivalenceClasses {
        shouldRebalanceCheck = options.shouldRebalanceBasedOnThresholds(
            ctx, eqClass, metrics)
        if shouldRebalanceCheck {
            break
        }
    }
}

if !needRebalance && !shouldRebalanceCheck {
    // 不需要 Rebalance
    return nil
}

// 对每个等价类的候选,计算 balanceScore 和 convergesScore
var results []rebalanceOptions
for _, eqClass := range equivalenceClasses {
    // 只对"best"候选计算(前面已经用 best() 提取)
    for i := range eqClass.candidates {
        eqClass.candidates[i].balanceScore = options.balanceScore(
            eqClass.candidateSL, eqClass.candidates[i].store.Capacity)
        eqClass.candidates[i].convergesScore = options.rebalanceToConvergesScore(
            eqClass, eqClass.candidates[i].store)
        // ...
    }

    // 重新排序(加入 balanceScore 和 convergesScore 后)
    sort.Sort(sort.Reverse(byScore(eqClass.candidates)))

    // 计算 existing 的 convergesScore
    existingCand := existingStores[eqClass.existing.StoreID]
    existingCand.convergesScore = options.rebalanceFromConvergesScore(eqClass)
    existingCand.balanceScore = options.balanceScore(
        eqClass.candidateSL, eqClass.existing.Capacity)

    results = append(results, rebalanceOptions{
        existing:   existingCand,
        candidates: eqClass.candidates,
        advisor:    nil,  // 惰性初始化
    })
}

return results
```

**为什么分阶段计算评分?**

```
原因 1: 避免污染统计
  - diversityScore 依赖全局 Locality 信息
  - balanceScore 依赖等价类内的平均值
  - 如果在第一阶段就计算 balanceScore,可能用错误的基准

原因 2: 性能优化
  - 等价类通常很小(< 10 个候选)
  - 只对等价类内的候选计算 balanceScore
  - 避免对所有 Store 计算(O(N) → O(K),K << N)

原因 3: 语义清晰
  - 第一阶段: 硬性筛选(valid, fullDisk, necessary, diversity)
  - 第二阶段: 软性优化(balance, converges)
  - 分离关注点
```

### 3.2 bestRebalanceTarget() - 多源最优选择

**函数签名**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:2102-2142
func bestRebalanceTarget(
    randGen allocatorRand,
    options []rebalanceOptions,  // 多个等价类(每个对应一个现有副本)
    as *mmaintegration.AllocatorSync,  // MMA 协调器
) (target, existingCandidate *candidate, bestIdx int)
```

**核心逻辑**:

```go
bestIdx = -1
var bestTarget *candidate
var replaces candidate

// 遍历所有等价类
for i, option := range options {
    if len(option.candidates) == 0 {
        continue  // 该现有副本没有可替换的候选
    }

    // 从该等价类中选择最优候选
    target = option.candidates.selectBest(randGen)  // Power of Two Random Choices
    if target == nil {
        continue
    }

    existing := option.existing

    // 比较: 该 (target, existing) 对是否比当前的 (bestTarget, replaces) 对更好
    if betterRebalanceTarget(target, &existing, bestTarget, &replaces) == target {
        bestIdx = i
        bestTarget = target
        replaces = existing
    }
}

if bestIdx == -1 {
    return nil, nil, -1  // 没有找到合适的目标
}

// 惰性初始化 MMA advisor
if options[bestIdx].advisor == nil {
    stores := make([]roachpb.StoreID, 0, len(options[bestIdx].candidates))
    for _, cand := range options[bestIdx].candidates {
        stores = append(stores, cand.store.StoreID)
    }
    options[bestIdx].advisor = as.BuildMMARebalanceAdvisor(
        options[bestIdx].existing.store.StoreID, stores)
}

// 从候选列表中移除已选择的候选(避免下次再选)
copiedTarget := *bestTarget
options[bestIdx].candidates = options[bestIdx].candidates.removeCandidate(copiedTarget)

return &copiedTarget, &options[bestIdx].existing, bestIdx
```

**betterRebalanceTarget() 的比较逻辑**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:2147-2169
func betterRebalanceTarget(target1, existing1, target2, existing2 *candidate) *candidate {
    if target2 == nil {
        return target1  // 第一次调用,没有 target2
    }

    // 计算"改善幅度"
    comp1 := target1.compare(*existing1)  // target1 比 existing1 好多少
    comp2 := target2.compare(*existing2)  // target2 比 existing2 好多少

    if !scoresAlmostEqual(comp1, comp2) {
        if comp1 > comp2 {
            return target1  // target1 的改善幅度更大
        }
        if comp1 < comp2 {
            return target2  // target2 的改善幅度更大
        }
    }

    // 如果改善幅度相等,选择绝对分数更高的 target
    if target1.less(*target2) {
        return target2
    }
    return target1
}
```

**设计理念**: **相对改善 > 绝对质量**

```
示例:
  选项 1: existing1 (分数=100) → target1 (分数=150) // 改善 +50
  选项 2: existing2 (分数=120) → target2 (分数=160) // 改善 +40

  虽然 target2 的绝对分数(160)高于 target1(150),
  但选择选项 1,因为改善幅度更大(+50 > +40)

原因:
  - 目标是"改善 Range 的质量"
  - 不是"选择最好的 Store"
  - 改善幅度大的 Rebalance 对系统贡献更大
```

**为什么在选择后移除候选?**

```
场景: 迭代选择多次
  1. bestRebalanceTarget() 选择 (target=s5, existing=s1, bestIdx=0)
  2. 外层循环检测到 MMA 冲突,continue
  3. bestRebalanceTarget() 再次调用
     - 如果不移除 s5,会再次选择 s5
     - 导致无限循环
  4. 移除 s5 后,options[0].candidates = [s6, s7, ...]
  5. 下次调用会选择 s6
```

### 3.3 simulateRemoveTarget() - 乒乓预防

**函数签名**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1688-1742
func (a Allocator) simulateRemoveTarget(
    ctx context.Context,
    storePool storepool.AllocatorStorePool,
    targetStore roachpb.StoreID,           // 新添加的 Store
    conf *roachpb.SpanConfig,
    candidates []roachpb.ReplicaDescriptor, // 包括 targetStore 的副本列表
    existingVoters []roachpb.ReplicaDescriptor,
    existingNonVoters []roachpb.ReplicaDescriptor,
    sl storepool.StoreList,
    rangeUsageInfo allocator.RangeUsageInfo,
    targetType TargetReplicaType,
    options ScorerOptions,
) (roachpb.ReplicationTarget, string, error)
```

**核心逻辑**:

```go
// 1. 临时修改 StorePool 统计,模拟"添加副本到 targetStore"
switch targetType {
case VoterTarget:
    storePool.UpdateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.ADD_VOTER)
    defer storePool.UpdateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.REMOVE_VOTER)

case NonVoterTarget:
    storePool.UpdateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.ADD_NON_VOTER)
    defer storePool.UpdateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.REMOVE_NON_VOTER)
}

// 2. 调用 RemoveTarget,决定应该移除哪个副本
return a.RemoveTarget(
    ctx, storePool, conf, storepool.MakeStoreList(candidateStores),
    existingVoters, existingNonVoters, targetType, options,
)
```

**UpdateLocalStoreAfterRebalance 的实现** (伪代码):

```go
func (sp *StorePool) UpdateLocalStoreAfterRebalance(
    storeID roachpb.StoreID,
    rangeUsageInfo allocator.RangeUsageInfo,
    changeType roachpb.ReplicaChangeType,
) {
    desc := sp.getStoreDescriptor(storeID)

    switch changeType {
    case ADD_VOTER, ADD_NON_VOTER:
        desc.Capacity.RangeCount += 1
        desc.Capacity.QueriesPerSecond += rangeUsageInfo.QueriesPerSecond
        desc.Capacity.WritesPerSecond += rangeUsageInfo.WritesPerSecond
        // ...

    case REMOVE_VOTER, REMOVE_NON_VOTER:
        desc.Capacity.RangeCount -= 1
        desc.Capacity.QueriesPerSecond -= rangeUsageInfo.QueriesPerSecond
        desc.Capacity.WritesPerSecond -= rangeUsageInfo.WritesPerSecond
        // ...
    }

    sp.updateStoreDescriptor(storeID, desc)
}
```

**乒乓检测**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:2112-2120
removeReplica, removeDetails, err := a.simulateRemoveTarget(...)

if target.store.StoreID != removeReplica.StoreID {
    // 成功: 移除的是其他 Store,不是刚添加的 target
    break
}

log.VEventf(ctx, 2, "not rebalancing to s%d because we'd immediately remove it: %s",
    target.store.StoreID, removeDetails)
// 继续下一轮迭代,选择其他候选
```

**示例**:

```
场景 1: 正常 Rebalance
  现有副本: [s1(RangeCount=150), s2(RangeCount=100), s3(RangeCount=120)]
  候选 target: s4(RangeCount=80)

  模拟:
    1. 添加副本到 s4 → s4.RangeCount=81
    2. 调用 RemoveTarget([s1, s2, s3, s4])
    3. RemoveTarget 返回 s1 (RangeCount 最高)

  检测:
    removeReplica.StoreID(s1) != target.StoreID(s4) → OK

  结果: Rebalance from s1 to s4

场景 2: 乒乓检测
  现有副本: [s1(RangeCount=100), s2(RangeCount=100), s3(RangeCount=100)]
  候选 target: s4(RangeCount=99)

  模拟:
    1. 添加副本到 s4 → s4.RangeCount=100
    2. 调用 RemoveTarget([s1, s2, s3, s4])
    3. RemoveTarget 可能返回 s1, s2, s3, 或 s4 (都是 100)
    4. 假设随机选择返回 s4

  检测:
    removeReplica.StoreID(s4) == target.StoreID(s4) → 乒乓!

  结果: 拒绝 Rebalance,选择其他候选
```

**为什么使用 defer 恢复统计?**

```
原因:
  1. 模拟是临时的,不影响实际状态
  2. defer 保证即使 panic 也会恢复统计
  3. 避免污染后续决策
```

### 3.4 并发语义与加锁策略

**无锁设计** (Lock-Free):

`RebalanceTarget` 本身不持有任何锁,依赖:

```
1. StorePool 的内部锁:
   - GetStoreList() 内部使用 RLock
   - UpdateLocalStoreAfterRebalance() 内部使用 Lock
   - 锁的粒度: 单个 Store 的 Descriptor

2. 随机数生成器的锁:
   - randGen.Lock() / randGen.Unlock()
   - 锁的粒度: 随机数生成操作

3. 无全局锁:
   - 多个 Range 可以并发调用 RebalanceTarget
   - 各自读取 StorePool 的快照
   - 决策可能基于"稍微过时"的数据
```

**为什么不需要全局锁?**

```
原因 1: 最终一致性
  - Rebalance 不要求强一致性
  - 基于稍微过时的数据做决策是可接受的
  - 下一轮周期会修正错误决策

原因 2: 性能
  - 全局锁会成为瓶颈
  - 每个 Store 每 10 秒运行一次 Rebalancer
  - 集群可能有数千个 Range
  - 全局锁会导致串行化

原因 3: 避免死锁
  - 无全局锁 → 无死锁风险
  - 简化实现
```

**并发冲突的处理**:

```
场景: 两个 Range 同时选择同一个 target
  Range R1: existing=s1, target=s5
  Range R2: existing=s2, target=s5

  两者同时执行 ChangeReplicas:
    1. R1 添加副本到 s5 → s5.RangeCount = 81
    2. R2 添加副本到 s5 → s5.RangeCount = 82
    3. R1 移除副本从 s1 → s1.RangeCount = 149
    4. R2 移除副本从 s2 → s2.RangeCount = 99

  结果: s5 收到 2 个副本,负载可能略高

  修正: 下一轮 Rebalance 会发现 s5 负载高,避免再添加副本
```

---

## 四、运行时行为与系统反馈(Runtime Behavior)

### 4.1 系统信号的感知

#### 4.1.1 负载信号

**来源**: `roachpb.StoreCapacity`

```go
type StoreCapacity struct {
    RangeCount       int32   // Range 数量
    QueriesPerSecond float64 // QPS
    WritesPerSecond  float64 // 写入 QPS
    L0SubLevelCount  int64   // LSM L0 层数(IO 压力指标)
    // ...
}
```

**更新频率**: 每 10 秒(Gossip 心跳)

**影响决策**:

```
高负载 Store:
  - balanceScore = overfull
  - convergesScore = -1 (应该移走副本)
  - 成为 Rebalance 的源(existing)

低负载 Store:
  - balanceScore = underfull
  - convergesScore = 1 (应该添加副本)
  - 成为 Rebalance 的目标(target)
```

#### 4.1.2 磁盘容量信号

**来源**: `StoreCapacity.Capacity`

```go
type Capacity struct {
    Capacity  int64  // 总容量
    Available int64  // 可用容量
    Used      int64  // 已用容量
}

func (c Capacity) FractionUsed() float64 {
    return float64(c.Used) / float64(c.Capacity)
}
```

**阈值**:

```
maxDiskUtilizationThreshold = 0.95 (95%)
  - 超过 95%: fullDisk=true
  - 不能作为 Allocation/Rebalance 目标
  - 必须移除副本

rebalanceToMaxDiskUtilizationThreshold = 0.925 (92.5%)
  - 超过 92.5%: 不能作为 Rebalance 目标(但可以 Allocate)
  - 提供缓冲,避免 Rebalance 后立即满
```

**影响决策**:

```
fullDisk=true:
  - 该 Store 被标记为"Critical"
  - 必须立即移除副本
  - 跳过 MMA 检查(紧急情况)
```

#### 4.1.3 约束违反信号

**来源**: `constraint.AnalyzedConstraints`

```go
type AnalyzedConstraints struct {
    Constraints         []roachpb.ConstraintsConjunction
    SatisfiedBy         [][]roachpb.StoreID  // Constraint → Stores
    Satisfies           map[roachpb.StoreID][]int  // Store → Constraints
    UnconstrainedReplicas bool
}
```

**检测**: `constraintsChecker(store) → (valid, necessary)`

**影响决策**:

```
valid=false:
  - 该 Store 违反约束
  - 必须移除副本
  - 标记为 Critical Rebalance

necessary=true:
  - 该 Store 满足未充分满足的约束
  - 优先作为 Rebalance 目标
  - 高优先级(400 points)
```

### 4.2 决策的即时性 vs 滞后性

**即时决策** (< 1ms):

```
1. 约束检查:
   - 基于当前 Zone Config
   - 无延迟

2. 磁盘容量检查:
   - 基于最新的 Gossip 数据
   - 延迟 < 10 秒

3. 候选评分:
   - 基于本地计算
   - 无网络延迟
```

**滞后决策** (10 秒 ~ 1 分钟):

```
1. 负载统计:
   - 更新周期: 10 秒
   - QPS 统计基于滑动窗口(可能滞后 30 秒)

2. Gossip 传播:
   - 新 Store 上线 → 所有节点感知: 10-30 秒
   - Store 下线 → 所有节点感知: 10-60 秒(取决于心跳超时)

3. MMA 协调:
   - MMA 的目标负载基于全局统计
   - 全局统计汇聚周期: 10-30 秒
```

**为什么容忍滞后?**

```
原因 1: 性能
  - 即时同步全局状态成本太高
  - Gossip 协议已经优化为秒级延迟
  - 对 Rebalance 来说足够快

原因 2: 稳定性
  - 过于即时的反应导致"震荡"
  - 负载波动 → 立即 Rebalance → 新的负载波动
  - 滞后的数据起到"平滑"作用

原因 3: 最终一致性
  - Rebalance 是渐进式的
  - 错误决策会在下一轮修正
  - 不需要强一致性
```

### 4.3 反馈循环

#### 4.3.1 正反馈循环(Positive Feedback)

```
场景: 热点 Range 的 Rebalance

1. Range R1 的 QPS 突增 → s1 负载高
2. RebalanceTarget 决策: 从 s1 移到 s5
3. 执行 Rebalance (需要 10-60 秒)
4. 期间: R1 的 Lease 仍在 s1
5. s1 负载持续高
6. 其他 Range 也决策: 从 s1 移走
7. 多个 Range 同时从 s1 移走
8. s1 负载快速下降
9. s1 变成"低负载" Store
10. 新的 Allocation 又选择 s1
11. 循环...

缓解:
  - MMA 协调(避免过度迁移)
  - Lease 转移(先转移 Lease,降低 s1 负载)
  - Rate Limiting(限制 Rebalance 速率)
```

#### 4.3.2 负反馈循环(Negative Feedback)

```
场景: 负载均衡收敛

1. 初始: s1=150 ranges, s2=100 ranges, s3=120 ranges
2. RebalanceTarget 决策: 从 s1 移到 s2
3. 执行 Rebalance: s1=149, s2=101
4. 下一轮: 从 s1 或 s3 移到 s2
5. 执行 Rebalance: s1=148, s2=102 或 s3=119, s2=101
6. 多轮后: s1≈123, s2≈123, s3≈123 (收敛到均衡)

设计:
  - balanceScore 提供反馈
  - 负载高 → balanceScore=overfull → 更可能成为源
  - 负载低 → balanceScore=underfull → 更可能成为目标
  - 收敛后: 所有 Store 都是 aroundTheMean → 停止 Rebalance
```

### 4.4 策略选择的理由

#### 4.4.1 为什么采用"模拟移除"而非"事前分析"?

**替代方案**: 在选择 target 前,分析"添加后是否会立即移除"

```
问题:
  1. 需要枚举所有 (target, removeCandidate) 组合
  2. 复杂度: O(N * M),N=候选数,M=现有副本数
  3. 难以准确预测 RemoveTarget 的决策(涉及随机性)

当前方案: 先选择 target,再模拟移除
  1. 只模拟一次 RemoveTarget
  2. 复杂度: O(M)
  3. 准确预测(使用真实的 RemoveTarget 逻辑)

Trade-off:
  - 当前方案可能需要多次迭代(每次迭代 O(M))
  - 但大多数情况下 1-2 次迭代即可
  - 总体复杂度仍优于事前分析
```

#### 4.4.2 为什么采用"等价类"而非"全局排序"?

**等价类的优势**:

```
1. 语义清晰:
   - "只选不比现有副本更差的候选"
   - 保证 Range 质量不降低

2. 局部性:
   - 只比较"与现有副本相关"的 Store
   - 不需要全局排序所有 Store

3. 并发友好:
   - 每个 Range 独立决策
   - 不需要全局协调

4. 灵活性:
   - 每个现有副本有自己的等价类
   - 可以选择"改善幅度最大"的替换
```

**全局排序的劣势**:

```
1. 复杂度:
   - 需要排序所有 Store: O(N log N)
   - 等价类只需排序候选: O(K log K),K << N

2. 不适用性:
   - 全局排序无法表达"相对改善"
   - 可能选到"绝对最好但改善幅度小"的 Store

3. 并发冲突:
   - 所有 Range 都选同一个 Store
   - 导致羊群效应
```

#### 4.4.3 为什么采用"惰性 MMA 初始化"?

```
当前实现:
  - MMA advisor 在 bestRebalanceTarget 首次选择该源时初始化
  - 缓存在 rebalanceOptions.advisor

替代方案 1: 预先初始化所有 MMA advisor
  问题:
    - 需要为每个现有副本构造 advisor
    - 但可能只使用其中 1-2 个(大部分等价类为空)
    - 浪费 CPU

替代方案 2: 每次调用都重新构造 advisor
  问题:
    - bestRebalanceTarget 可能被调用多次(迭代)
    - 同一个源可能被多次选择
    - 重复构造 advisor 浪费 CPU

当前方案: 惰性 + 缓存
  优点:
    - 只构造"真正被使用"的 advisor
    - 同一个源的 advisor 只构造一次
    - 平衡 CPU 和内存
```

---

## 五、设计模式分析

### 5.1 策略模式(Strategy Pattern) - Constraints Checker

**显式应用**:

```go
type constraintsCheckFn func(roachpb.StoreDescriptor) (valid, necessary bool)

type rebalanceConstraintsCheckFn func(toStore, fromStore roachpb.StoreDescriptor) (
    valid, necessary, voterNecessary bool)
```

**具体策略**:

```
Removal:
  - voterConstraintsCheckerForRemoval
  - nonVoterConstraintsCheckerForRemoval

Rebalance:
  - voterConstraintsCheckerForRebalance
  - nonVoterConstraintsCheckerForRebalance

根据 targetType 选择:
  switch targetType {
  case VoterTarget:
      removalConstraintsChecker = voterConstraintsCheckerForRemoval(...)
      rebalanceConstraintsChecker = voterConstraintsCheckerForRebalance(...)
  case NonVoterTarget:
      removalConstraintsChecker = nonVoterConstraintsCheckerForRemoval(...)
      rebalanceConstraintsChecker = nonVoterConstraintsCheckerForRebalance(...)
  }
```

**为什么使用策略模式?**

```
原因:
  1. Voter 和 NonVoter 的约束检查逻辑不同
  2. Removal 和 Rebalance 的约束检查逻辑不同
  3. 组合产生 4 种策略
  4. 策略模式避免 if-else 嵌套

优点:
  - 易于扩展(添加新的 ReplicaType)
  - 易于测试(每个策略独立测试)
  - 代码复用(共享 AnalyzedConstraints)
```

### 5.2 迭代器模式(Iterator Pattern) - Candidate Selection

**隐式应用**:

```go
for {
    target, existingCandidate, bestIdx = bestRebalanceTarget(a.randGen, results, a.as)
    if target == nil {
        return zero, zero, "", false  // 迭代结束
    }

    // 处理 target
    // ...

    if success {
        break  // 提前退出
    }
    // 否则继续迭代(bestRebalanceTarget 已移除该 target)
}
```

**迭代器的特性**:

```
1. 状态维护:
   - results[bestIdx].candidates 维护剩余候选
   - bestRebalanceTarget 负责移除已选择的候选

2. 终止条件:
   - target == nil (所有候选都已尝试)
   - 或 success (找到合适的 target)

3. 惰性求值:
   - 不预先计算所有可能的 (target, existing) 对
   - 按需选择和检查
```

**为什么使用迭代器?**

```
原因:
  1. 避免预先生成所有组合(可能很大)
  2. 支持提前退出(找到合适的 target 后停止)
  3. 支持动态过滤(MMA 冲突检测、乒乓检测)

优点:
  - 内存效率(不需要存储所有组合)
  - CPU 效率(提前退出节省计算)
  - 灵活性(支持复杂的过滤逻辑)
```

### 5.3 模拟模式(Simulation Pattern) - Simulate Remove

**显式应用**:

```go
// 临时修改系统状态
storePool.UpdateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.ADD_VOTER)

// 使用 defer 保证恢复
defer storePool.UpdateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.REMOVE_VOTER)

// 在模拟状态下执行决策
removeReplica, removeDetails, err := a.RemoveTarget(...)
```

**模拟模式的特征**:

```
1. 临时状态变更:
   - 修改 StorePool 的统计信息
   - 不影响实际 Replica 分布

2. 自动恢复:
   - 使用 defer 保证恢复
   - 即使 panic 也会恢复

3. 决策预测:
   - 在模拟状态下调用决策函数
   - 预测"如果真的执行,会发生什么"
```

**为什么使用模拟模式?**

```
原因:
  1. 乒乓预防需要"预测未来"
  2. 不能真的添加副本再检查(成本太高)
  3. 模拟是唯一可行的方法

优点:
  - 准确预测(使用真实的决策逻辑)
  - 零成本(不涉及网络/磁盘)
  - 安全性(自动恢复,不污染状态)

替代方案:
  - 静态分析(不准确,难以实现)
  - 实际执行再回滚(成本太高,可能失败)
```

### 5.4 等价类模式(Equivalence Class Pattern)

**显式应用**:

```go
type equivalenceClass struct {
    existing    roachpb.StoreDescriptor
    candidateSL storepool.StoreList
    candidates  candidateList
}

// 只添加"不比 existing 更差"的候选
if !cand.less(existing) {
    comparableCands = append(comparableCands, cand)
}

// 提取"best"等价类
bestCands := comparableCands.best()
```

**等价类的数学定义**:

```
等价关系 ~ : "cand1 ~ cand2" iff "cand1 和 cand2 在关键维度上等价"

关键维度:
  - necessary
  - voterNecessary
  - diversityScore (容差 epsilon)
  - convergesScore
  - balanceScore
  - hasNonVoter

等价类: [cand1]~ = { cand | cand ~ cand1 }

best(): 返回"分数最高的等价类"
```

**为什么使用等价类?**

```
原因 1: 避免羊群效应
  - 如果只选"唯一的最优候选"
  - 所有 Range 都选同一个 Store
  - 导致热点

  等价类包含多个候选:
  - 在等价类中随机选择(Power of Two)
  - 分散负载

原因 2: 渐进式改善
  - 只选"至少一样好"的候选
  - 保证不降低 Range 质量
  - 符合"逐步优化"的哲学

原因 3: 局部性
  - 每个现有副本有自己的等价类
  - 不需要全局排序
  - 降低计算复杂度
```

**等价类模式在分布式系统中的地位**:

```
事实标准: 否(CockroachDB 特有)

类似思想:
  - Kubernetes Scheduler: NodeAffinity + Equivalent Classes
  - Hadoop YARN: Locality Levels (Node-Local, Rack-Local, Off-Switch)
  - Load Balancer: Weighted Round-Robin (等价权重的服务器)

CockroachDB 的创新:
  - 将等价类应用于 Rebalance 决策
  - 结合 Power of Two Random Choices
  - 实现"质量保证 + 负载分散"
```

### 5.5 惰性求值模式(Lazy Evaluation Pattern) - MMA Advisor

**显式应用**:

```go
type rebalanceOptions struct {
    existing   candidate
    candidates candidateList
    advisor    *mmaprototype.MMARebalanceAdvisor  // 惰性初始化
}

// 在 bestRebalanceTarget 中初始化
if options[bestIdx].advisor == nil {
    stores := make([]roachpb.StoreID, 0, len(options[bestIdx].candidates))
    for _, cand := range options[bestIdx].candidates {
        stores = append(stores, cand.store.StoreID)
    }
    options[bestIdx].advisor = as.BuildMMARebalanceAdvisor(
        options[bestIdx].existing.store.StoreID, stores)
}
```

**惰性求值的特征**:

```
1. 延迟初始化:
   - advisor 字段初始为 nil
   - 第一次使用时才初始化

2. 缓存结果:
   - 初始化后缓存在 rebalanceOptions 中
   - 后续使用直接读取缓存

3. 条件初始化:
   - 只初始化"真正被使用"的 advisor
   - 未被使用的保持 nil
```

**为什么使用惰性求值?**

```
原因:
  1. 避免浪费 CPU
     - 大部分等价类不会被选中
     - 预先初始化所有 advisor 浪费 CPU

  2. 支持迭代
     - bestRebalanceTarget 可能被多次调用
     - 同一个源的 advisor 只初始化一次

  3. 内存效率
     - advisor 对象较大(包含统计信息)
     - 只分配"真正需要"的内存

示例:
  10 个等价类,只有 2 个被选中
  - 惰性求值: 初始化 2 个 advisor
  - 预先求值: 初始化 10 个 advisor
  - 节省: 80% 的 CPU 和内存
```

### 5.6 职责链模式(Chain of Responsibility Pattern) - Filter Cascade

**隐式应用**:

```
过滤链条:
  1. maxCapacityCheck() - 磁盘容量过滤
  2. allocateReplicaToCheck() - IO 过载过滤
  3. constraintsCheck() - 约束过滤
  4. isStoreValidForRoutineReplicaTransfer() - 存活性过滤
  5. !cand.less(existing) - 等价性过滤
  6. isCriticalRebalance() - Critical 检查
  7. IsInConflictWithMMA() - MMA 冲突检查
  8. simulateRemoveTarget() - 乒乓检查
```

**职责链的特征**:

```
1. 顺序处理:
   - 按优先级依次检查
   - 硬性条件 → 软性条件

2. 提前退出:
   - 任何一个过滤器拒绝 → 立即排除
   - 不执行后续过滤器

3. 独立性:
   - 每个过滤器独立实现
   - 易于添加/删除过滤器
```

**为什么使用职责链?**

```
原因:
  1. 语义清晰
     - 每个过滤器代表一个独立的决策维度
     - 易于理解和维护

  2. 性能优化
     - 硬性条件先检查(快速排除不合格候选)
     - 软性条件后检查(只对少数候选执行)

  3. 可扩展性
     - 添加新的过滤器:只需插入链条
     - 不影响其他过滤器

示例:
  1000 个 Store
  → maxCapacityCheck 过滤掉 50 个(磁盘满)
  → constraintsCheck 过滤掉 800 个(不满足约束)
  → 等价性过滤掉 140 个(不如 existing)
  → 剩余 10 个候选
  → 只对 10 个候选执行 MMA 检查和乒乓检查
```

---

## 六、具体运行示例

### 6.1 正常场景:负载均衡 Rebalance

**初始状态**:

```
集群配置:
  - 5 个 Store: s1, s2, s3, s4, s5
  - 每个 Store 在不同 region

当前 Range R1 的副本:
  - s1: Voter, region=us-east-1,  RangeCount=150, QPS=50
  - s2: Voter, region=us-west-1,  RangeCount=145, QPS=45
  - s3: Voter, region=us-central, RangeCount=140, QPS=40

Store 统计:
  s1: RangeCount=150, QPS=1500, DiskUsed=70%
  s2: RangeCount=145, QPS=1450, DiskUsed=68%
  s3: RangeCount=140, QPS=1400, DiskUsed=65%
  s4: RangeCount=100, QPS=1000, DiskUsed=55%
  s5: RangeCount=105, QPS=1050, DiskUsed=58%

平均: RangeCount=128, QPS=1280
```

**执行 RebalanceTarget**:

```
步骤 1: 获取 StoreList 并分析约束
  - 无特殊约束
  - analyzedOverallConstraints: UnconstrainedReplicas=true

步骤 2: 调用 rankedCandidateListForRebalancing()

  2.1 评估现有副本:
    s1: valid=true, necessary=false, fullDisk=false, diversityScore=0.67
    s2: valid=true, necessary=false, fullDisk=false, diversityScore=0.67
    s3: valid=true, necessary=false, fullDisk=false, diversityScore=0.67

    needRebalanceFrom = false (无坏副本)

  2.2 构造等价类:

    对于 s1:
      候选 s2: 已有副本,跳过
      候选 s3: 已有副本,跳过
      候选 s4:
        - valid=true, necessary=false
        - diversityScore = diversityRebalanceFromScore(s4.Locality, s1, existingStoreLocalities)
          = (DiversityScore(us-west-1, us-central) + DiversityScore(us-west-2, us-central)) / 2
          = (1.0 + 1.0) / 2 = 1.0  // region 不同
        - !cand.less(existing): 1.0 > 0.67 → OK
        - 加入 comparableCands
      候选 s5:
        - diversityScore = 1.0
        - 加入 comparableCands

      排序: [s4, s5] (假设 s4.balanceScore 略高)
      best(): [s4, s5] (两者在关键维度上等价)

      等价类 1: { existing=s1, candidates=[s4, s5] }

    对于 s2:
      candidates=[s4, s5]
      等价类 2: { existing=s2, candidates=[s4, s5] }

    对于 s3:
      candidates=[s4, s5]
      等价类 3: { existing=s3, candidates=[s4, s5] }

  2.3 检查是否需要 Rebalance:
    needRebalance = needRebalanceFrom || needRebalanceTo = false || true = true
    (s4 和 s5 比现有副本更好,needRebalanceTo=true)

  2.4 计算 balanceScore 和 convergesScore:

    等价类 1 (s1):
      s4: balanceScore = underfull (100 < 105 < 128)
          convergesScore = 1 (添加副本到 s4 有助于收敛)
      s5: balanceScore = underfull (105 < 105 < 128)
          convergesScore = 1

      s1: convergesScore = -1 (从 s1 移除副本有助于收敛)
          balanceScore = overfull (150 > 134)

    重新排序: [s4, s5] (s4.balanceScore 可能略高)

    results[0] = { existing=s1, candidates=[s4, s5], advisor=nil }
    results[1] = { existing=s2, candidates=[s4, s5], advisor=nil }
    results[2] = { existing=s3, candidates=[s4, s5], advisor=nil }

步骤 3: 迭代选择最优目标

  迭代 1:
    调用 bestRebalanceTarget(results)

    对于 results[0]:
      target = selectBest([s4, s5]) → Power of Two 选择 s4
      existing = s1
      comp = target.compare(existing)
            = 计算 diversityScore 差异 + balanceScore 差异 + convergesScore 差异
            = 300*(1.0-0.67) + 150*(underfull-overfull) + 200*(1-(-1))
            = 99 + 300 + 400 = 799

    对于 results[1]:
      target = selectBest([s4, s5]) → Power of Two 选择 s4
      existing = s2
      comp = 类似计算 ≈ 750

    对于 results[2]:
      target = selectBest([s4, s5]) → Power of Two 选择 s5
      existing = s3
      comp ≈ 700

    betterRebalanceTarget 比较:
      results[0] 的改善幅度最大(799)

    选择: target=s4, existing=s1, bestIdx=0

  步骤 4: 检查 Critical Rebalance
    existingCandidate.isCriticalRebalance(target)?
      - s1.valid=true && s4.valid=true → 不是约束修复
      - s1.fullDisk=false → 不是磁盘满
      - s4.diversityScore(1.0) > s1.diversityScore(0.67) → 是分散性改善!

    isCritical = true → 跳过 MMA 检查

  步骤 5: 模拟移除
    构造 existingPlusOneNew = [s1, s2, s3, s4(新)]

    simulateRemoveTarget(targetStore=s4, candidates=[s1,s2,s3,s4]):
      1. UpdateLocalStoreAfterRebalance(s4, ADD_VOTER)
         → s4.RangeCount = 101

      2. RemoveTarget([s1,s2,s3,s4]):
         候选评分:
           s1: RangeCount=150, balanceScore=overfull, convergesScore=-1
           s2: RangeCount=145, balanceScore=overfull, convergesScore=-1
           s3: RangeCount=140, balanceScore=overfull, convergesScore=-1
           s4: RangeCount=101, balanceScore=underfull, convergesScore=1

         排序: [s4, s3, s2, s1] (s1 最差)
         selectWorst() → Power of Two 选择 s1

      3. defer 恢复: s4.RangeCount = 100

      返回: removeReplica=s1

  步骤 6: 检查乒乓
    target.StoreID(s4) != removeReplica.StoreID(s1)? → OK

    break (成功)

步骤 7: 返回结果
  addTarget = {NodeID=4, StoreID=s4}
  removeTarget = {NodeID=1, StoreID=s1}
  details = {"Target":"s4,...", "Existing":"s1,..."}
  ok = true
```

**结果**:

```
Rebalance 执行:
  1. 添加副本到 s4 (Raft AddVoter)
  2. 等待 s4 追赶 Raft Log (Catch-Up)
  3. 移除副本从 s1 (Raft RemoveVoter)

新状态:
  R1 副本: s4, s2, s3

  s1: RangeCount=149, QPS=1450 (减少)
  s4: RangeCount=101, QPS=1050 (增加)

  更均衡,且分散性改善
```

### 6.2 边界场景:乒乓检测

**初始状态**:

```
当前 Range R1 的副本:
  - s1: Voter, RangeCount=100
  - s2: Voter, RangeCount=100
  - s3: Voter, RangeCount=100

Store 统计:
  s1: RangeCount=100
  s2: RangeCount=100
  s3: RangeCount=100
  s4: RangeCount=99  // 略低于平均

平均: RangeCount=99.75
阈值: overfull=105, underfull=95
```

**执行 RebalanceTarget**:

```
步骤 1-3: 与正常场景类似
  选择: target=s4, existing=s1, bestIdx=0

步骤 4: Critical Rebalance?
  - 无约束违反
  - 无磁盘满
  - diversityScore 相同
  → isCritical = false

步骤 5: MMA 检查
  初始化 advisor:
    stores = [s4]
    advisor = BuildMMARebalanceAdvisor(s1, [s4])

  IsInConflictWithMMA(s4, advisor)?
    → 假设 MMA 允许 (s4 负载略低)

  继续

步骤 6: 模拟移除
  simulateRemoveTarget(targetStore=s4, candidates=[s1,s2,s3,s4]):
    1. s4.RangeCount = 100 (临时)

    2. RemoveTarget([s1,s2,s3,s4]):
       所有候选 RangeCount 都是 100
       selectWorst() → Power of Two 随机选择
       假设选择 s4

    返回: removeReplica=s4

步骤 7: 检查乒乓
  target.StoreID(s4) == removeReplica.StoreID(s4)? → 乒乓!

  log: "not rebalancing to s4 because we'd immediately remove it"

  continue (下一轮迭代)

迭代 2:
  bestRebalanceTarget(results):
    results[0].candidates 已移除 s4 (在迭代 1 中)
    candidates = [] (空)
    → target = nil

  返回: ok=false
```

**结果**:

```
RebalanceTarget 返回:
  ok = false (无合适的 Rebalance 目标)

系统行为:
  - 不执行 Rebalance
  - 保持当前状态
  - 等待下一轮周期重试(可能负载已变化)
```

### 6.3 压力场景:约束违反修复

**初始状态**:

```
Zone Config:
  - NumReplicas=3
  - Constraints: 至少 1 个副本在 us-west

当前 Range R1 的副本:
  - s1: Voter, region=us-east, RangeCount=120, DiskUsed=60%
  - s2: Voter, region=us-east, RangeCount=115, DiskUsed=58%
  - s3: Voter, region=us-central, RangeCount=110, DiskUsed=55%

Store 统计:
  s1: region=us-east,    RangeCount=120
  s2: region=us-east,    RangeCount=115
  s3: region=us-central, RangeCount=110
  s4: region=us-west,    RangeCount=150  // 负载高但满足约束
  s5: region=us-central, RangeCount=80   // 负载低但不满足约束

约束分析:
  - 当前无副本在 us-west → 约束违反!
```

**执行 RebalanceTarget**:

```
步骤 1: 分析约束
  analyzedOverallConstraints:
    Constraints: [至少 1 个副本在 us-west]
    SatisfiedBy: [[s4]]  // 只有 s4 满足
    Satisfies: {s4: [0]}
    UnconstrainedReplicas: false

步骤 2: rankedCandidateListForRebalancing()

  对于 s1:
    候选 s4:
      constraintsOK, necessary = rebalanceConstraintsChecker(s4, s1)
      → constraintsOK=true (s4 满足约束)
      → necessary=true (us-west 约束未满足)

      diversityScore = 1.0 (region 不同)

      !cand.less(existing)?
      → s4.necessary(true) > s1.necessary(false) → OK
      → 加入 comparableCands

    候选 s5:
      constraintsOK=false (不满足 us-west 约束)
      → 排除

    等价类 1: { existing=s1, candidates=[s4] }

  对于 s2, s3:
    类似,等价类都只包含 s4

步骤 3: bestRebalanceTarget()
  选择: target=s4, existing=s1, bestIdx=0
  (虽然 s4 负载高,但 necessary=true 优先级更高)

步骤 4: Critical Rebalance?
  existingCandidate.isCriticalRebalance(target)?
  → s1.necessary(false) && s4.necessary(true) → 是必要性改善!

  isCritical = true → 跳过 MMA 检查

步骤 5: 模拟移除
  simulateRemoveTarget(targetStore=s4, candidates=[s1,s2,s3,s4]):
    RemoveTarget():
      s1: necessary=false, diversityScore=0.5
      s2: necessary=false, diversityScore=0.5
      s3: necessary=false, diversityScore=0.67
      s4: necessary=true,  diversityScore=1.0

      排序: [s4, s3, s1, s2] (s4 最好,s2 最差)
      selectWorst() → s2

    返回: removeReplica=s2

步骤 6: 检查乒乓
  target.StoreID(s4) != removeReplica.StoreID(s2)? → OK

  break

步骤 7: 返回结果
  addTarget = s4
  removeTarget = s2
  ok = true
```

**结果**:

```
Rebalance 执行:
  1. 添加副本到 s4
  2. 移除副本从 s2

新状态:
  R1 副本: s1(us-east), s4(us-west), s3(us-central)

  约束满足: ✓ 有 1 个副本在 us-west

观察:
  - 虽然 s4 负载高(150 ranges),仍被选择
  - 因为约束修复是 Critical,优先级高于负载均衡
  - 后续 Rebalance 可能再将副本从 s4 移走(如果负载仍高)
```

---

## 七、设计取舍与替代方案(Trade-offs)

### 7.1 等价类 vs 全局排序

#### 当前方案:等价类

**优点**:
- 保证不降低 Range 质量
- 局部性(只比较相关 Store)
- 支持多源并发决策
- 避免羊群效应(等价类内随机)

**缺点**:
- 可能错过全局最优解
- 需要多次迭代(等价类可能为空)
- 实现复杂度高

#### 替代方案:全局排序

```go
// 伪代码
func RebalanceTargetGlobalSort(...) (add, remove, ok) {
    // 1. 全局排序所有 Store
    allStores := sortStoresByScore(storePool.GetStoreList())

    // 2. 选择分数最高的 Store 作为 target
    target := allStores[0]

    // 3. 选择分数最低的现有副本作为 existing
    existing := sortStoresByScore(existingReplicas)[len(existingReplicas)-1]

    // 4. 如果 target 比 existing 更好,执行 Rebalance
    if target.score > existing.score {
        return target, existing, true
    }
    return nil, nil, false
}
```

**优点**:
- 实现简单
- 保证选择全局最优 Store

**缺点**:
- 可能降低 Range 质量(target 可能不如某些 existing)
- 羊群效应(所有 Range 都选择同一个 target)
- 不支持并发(全局排序需要全局锁)

#### 对比

| 维度 | 等价类 | 全局排序 | CockroachDB 选择 |
|------|--------|----------|-------------------|
| 正确性 | ✓ 保证不降级 | ✗ 可能降级 | 等价类 |
| 性能 | ✓ 局部排序 | ✗ 全局排序 | 等价类 |
| 并发性 | ✓ 无全局锁 | ✗ 需要全局锁 | 等价类 |
| 负载分散 | ✓ 随机化 | ✗ 羊群效应 | 等价类 |
| 实现复杂度 | ✗ 高 | ✓ 低 | 等价类(值得) |

### 7.2 模拟移除 vs 事前分析

#### 当前方案:模拟移除

**优点**:
- 准确预测(使用真实的 RemoveTarget 逻辑)
- 支持复杂的移除策略(Power of Two, convergesScore, etc.)
- 代码复用(RemoveTarget)

**缺点**:
- 需要临时修改 StorePool 状态
- 可能需要多次迭代(乒乓检测失败)
- 复杂度 O(M),M=现有副本数

#### 替代方案:事前分析

```go
// 伪代码
func RebalanceTargetPreAnalysis(...) (add, remove, ok) {
    // 1. 枚举所有 (target, existing) 组合
    for _, target := range candidates {
        for _, existing := range existingReplicas {
            // 2. 分析:添加 target,移除 existing 后,是否会立即移除 target
            wouldRemoveTarget := analyzeRemoval(target, existing, existingReplicas)

            // 3. 如果不会立即移除,返回该组合
            if !wouldRemoveTarget {
                return target, existing, true
            }
        }
    }
    return nil, nil, false
}
```

**优点**:
- 不需要临时修改状态
- 可以预先过滤所有组合

**缺点**:
- 难以准确预测 RemoveTarget 的决策(涉及随机性)
- 需要重新实现 RemoveTarget 的逻辑(代码重复)
- 复杂度 O(N * M),N=候选数,M=现有副本数

#### 对比

| 维度 | 模拟移除 | 事前分析 | CockroachDB 选择 |
|------|----------|----------|-------------------|
| 准确性 | ✓ 准确 | ✗ 近似 | 模拟移除 |
| 代码复用 | ✓ 复用 RemoveTarget | ✗ 重新实现 | 模拟移除 |
| 复杂度 | O(M) 每次 | O(N*M) 预先 | 模拟移除(通常 1-2 次) |
| 状态修改 | ✗ 需要临时修改 | ✓ 无状态修改 | 模拟移除(defer 保证恢复) |

### 7.3 MMA 协调 vs 无协调

#### 当前方案: MMA 协调

**优点**:
- 避免局部最优陷阱
- 引导全局收敛
- 减少冲突决策

**缺点**:
- 增加决策延迟(需要与 MMA 通信)
- 可能拒绝"局部最优"的决策
- 实现复杂度高

#### 替代方案:无协调

```
每个 Range 独立决策,不与 MMA 协调:
  - 优点: 决策快速,实现简单
  - 缺点: 可能所有 Range 都向同一个 Store 迁移,导致热点
```

#### 对比

| 维度 | MMA 协调 | 无协调 | CockroachDB 选择 |
|------|----------|--------|-------------------|
| 全局最优 | ✓ 接近 | ✗ 局部最优 | MMA 协调 |
| 决策速度 | ✗ 稍慢 | ✓ 快速 | MMA 协调(可接受延迟) |
| 热点避免 | ✓ 有效 | ✗ 无保证 | MMA 协调 |
| 实现复杂度 | ✗ 高 | ✓ 低 | MMA 协调(值得) |

### 7.4 适用场景与局限性

#### 适用场景

**1. 负载均衡**:
- 集群负载不均(RangeCount/QPS 方差大)
- 需要渐进式改善(不剧烈迁移)
- 适合

**2. 约束修复**:
- Zone Config 变更后
- 需要满足新约束
- 非常适合(Critical Rebalance 机制)

**3. 磁盘容量平衡**:
- 某些 Store 磁盘快满
- 需要紧急迁移副本
- 非常适合(fullDisk 检测机制)

**4. 分散性改善**:
- 多个副本集中在同一故障域
- 需要增加分散性
- 适合(diversityScore 优先级高)

#### 不适用场景

**1. 实时负载响应**:
- 突发流量导致某个 Range 的 QPS 瞬间飙升
- RebalanceTarget 基于"Range 的历史 QPS"(滞后)
- 更适合: Lease Transfer(立即生效)

**2. 紧急故障恢复**:
- 节点突然 Down,需要立即补充副本
- RebalanceTarget 用于"替换",不是"添加"
- 更适合: AllocateTarget(添加新副本)

**3. 细粒度控制**:
- 管理员希望手动指定"从 s1 移到 s5"
- RebalanceTarget 有自己的决策逻辑(可能不选 s5)
- 更适合: AdminRelocateRange(手动控制)

**4. 跨数据中心迁移**:
- 需要将所有副本从 DC1 迁移到 DC2
- RebalanceTarget 基于"渐进式改善",速度慢
- 更适合: 批量迁移工具 + AdminScatterRequest

---

## 八、总结与心智模型(Mental Model)

### 8.1 核心思想总结

`RebalanceTarget` 的本质是:

> **基于等价类的渐进式副本重平衡决策引擎,通过多维度评分、乒乓预防、MMA 协调,实现"保证质量不降级"的前提下,逐步改善 Range 的放置位置,最终收敛到集群范围的负载均衡和约束满足。**

**关键原则**:

1. **渐进式改善**: 每次只替换 1 个副本,不追求"一步到位"
2. **质量保证**: 只选"至少和现有副本一样好"的候选,保证不降级
3. **多维度平衡**: 约束 > 分散性 > 负载均衡,优先级明确
4. **乒乓预防**: 通过模拟移除,避免"刚添加就移除"的震荡
5. **全局协调**: 通过 MMA,避免局部最优陷阱和羊群效应
6. **最终一致性**: 容忍基于"稍微过时"的数据决策,下一轮修正

### 8.2 心智模型

**类比 1: 公司团队重组**

```
场景: 公司有 10 个团队,需要调整人员分配以平衡负载

RebalanceTarget ≈ HR 决策流程:
  1. 评估每个团队的"质量"(技能、负载、多样性)
  2. 识别"需要调整"的团队(负载过高/技能不匹配)
  3. 对于每个"需要调整"的团队:
     - 找到"至少和当前成员一样好"的候选人
     - 模拟: 如果调该候选人进来,会不会立即调走?
     - 与公司整体规划协调(MMA)
     - 执行: 调候选人进来,调某个现有成员走
  4. 重复,直到所有团队"足够均衡"

关键:
  - 不是"解散团队重新分配"(太剧烈)
  - 是"逐个调整成员"(渐进式)
  - 保证团队质量不降低(只调入"至少一样好"的人)
```

**类比 2: 仓库货物重分配**

```
场景: 物流公司有 5 个仓库,货物分布不均,需要重新分配

RebalanceTarget ≈ 调度系统:
  1. 评估每个仓库的"状态"(容量、负载、地理位置)
  2. 识别"过载"的仓库(容量接近满)
  3. 对于每个过载仓库:
     - 找到"合适"的目标仓库(容量足够、位置合理)
     - 模拟: 如果把货物 A 移到目标仓库,会不会立即再移走?
     - 与整体调度计划协调(避免所有货物都移到同一个仓库)
     - 执行: 移动货物
  4. 重复,直到所有仓库"负载均衡"

关键:
  - 不是"清空仓库再重新分配"(成本太高)
  - 是"逐批次移动货物"(渐进式)
  - 避免"刚移入就移出"的浪费(模拟检测)
```

### 8.3 工程师可复用的理解

**当你面对类似的分布式资源调度问题时,可以借鉴**:

1. **等价类划分**:
   - 不要全局排序所有候选
   - 划分为"至少和当前一样好"的等价类
   - 在等价类内随机选择(避免羊群效应)

2. **渐进式改善**:
   - 不追求"一步到位"
   - 每次只改善一个维度
   - 通过多轮迭代达到最优

3. **模拟预测**:
   - 在执行前,模拟"执行后会发生什么"
   - 检测不良后果(乒乓、震荡)
   - 拒绝会导致不良后果的决策

4. **全局协调**:
   - 局部最优 ≠ 全局最优
   - 需要某种"全局视图"的协调器
   - 拒绝"局部最优但全局有害"的决策

5. **优先级分层**:
   - 硬性约束 > 软性优化 > 打破平局
   - 不同维度的优先级明确
   - Critical 情况跳过常规检查

6. **最终一致性**:
   - 容忍基于"稍微过时"的数据决策
   - 通过定期重试达到一致
   - 避免强一致性的性能代价

### 8.4 伪代码表示

```
function RebalanceTarget(existingReplicas, storePool, constraints, options):
    // 1. 构造等价类
    equivalenceClasses = []
    for each existing in existingReplicas:
        candidates = []
        for each store in storePool:
            if store is at least as good as existing:
                candidates.append(store)
        equivalenceClasses.append({existing, candidates})

    // 2. 迭代选择最优目标
    loop:
        // 2.1 选择改善幅度最大的 (target, existing) 对
        (target, existing, classIndex) = selectBestTarget(equivalenceClasses)
        if target == nil:
            return (nil, nil, false)  // 无合适目标

        // 2.2 Critical 检查
        if existing.isCritical(target):
            goto SIMULATE_REMOVE  // 跳过 MMA

        // 2.3 MMA 协调
        if mma.conflicts(target):
            continue  // 选择下一个候选

        // 2.4 模拟移除
        SIMULATE_REMOVE:
        simulate: add target to replicas
        removeCandidate = RemoveTarget(replicas + target)
        restore simulation

        // 2.5 乒乓检测
        if removeCandidate == target:
            log("ping-pong detected")
            continue  // 选择下一个候选

        // 2.6 成功
        return (target, removeCandidate, true)
```

---

**至此,`RebalanceTarget` 函数的深度剖析完成。本节通过职责边界分析、控制流梳理、关键函数深入、运行时行为分析、设计模式识别、具体示例、工程权衡,帮助读者构建对 CockroachDB 副本重平衡决策引擎的全面理解。**

**核心收获**:
- **等价类模式**: 保证质量不降级 + 避免羊群效应
- **模拟移除**: 准确预测乒乓,避免决策震荡
- **渐进式改善**: 每次只改善一个维度,最终收敛到全局最优
- **多维度协调**: 约束、分散性、负载均衡、MMA 的有机结合
