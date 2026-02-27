# 第三十一章_Allocator决策引擎——集群级副本放置的约束求解器(三)_设计模式、具体示例与工程权衡

> 本章为 Allocator 系列的第三部分(完结篇),聚焦于设计模式识别、端到端时间序列示例以及工程权衡分析。
>
> **核心问题**:Allocator 使用了哪些经典设计模式?如何在实际场景中权衡多个冲突的目标?为什么选择当前实现方案而非其他替代方案?
>
> **解答路径**:本章通过识别 8 大设计模式、提供 3 个完整时间序列示例(带具体数值)、分析 5 组关键工程权衡,帮助读者构建对 Allocator 的深层次理解。

---

## 一、设计模式识别与分析

### 1.1 策略模式(Strategy Pattern)- CandidateSelector

**模式定义**:定义一系列算法,把它们封装起来,并使它们可以相互替换。策略模式让算法独立于使用它的客户而变化。

**在 Allocator 中的应用**:

```go
// 策略接口
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1274-1278
type CandidateSelector interface {
    selectOne(cl candidateList) *candidate
}

// 具体策略1:BestCandidateSelector
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1280-1293
type BestCandidateSelector struct {
    randGen allocatorRand
}

func (s *BestCandidateSelector) selectOne(cl candidateList) *candidate {
    return cl.selectBest(s.randGen)  // 使用 Power of Two Random Choices
}

// 具体策略2:GoodCandidateSelector
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1295-1309
type GoodCandidateSelector struct {
    randGen allocatorRand
}

func (s *GoodCandidateSelector) selectOne(cl candidateList) *candidate {
    return cl.selectGood(s.randGen)  // 使用 Uniform Random
}

// 上下文(Context)
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1333-1338
var selector CandidateSelector
if replicaStatus == Alive || recoveryStoreSelector.Get(&a.st.SV) == "best" {
    selector = a.NewBestCandidateSelector()
} else {
    selector = a.NewGoodCandidateSelector()
}
```

**UML 类图**:
```
┌─────────────────────────────────┐
│   <<interface>>                 │
│   CandidateSelector             │
├─────────────────────────────────┤
│ + selectOne(candidateList)      │
│   : *candidate                  │
└─────────────────────────────────┘
           △
           │ implements
    ┌──────┴──────┐
    │             │
┌───┴────────┐  ┌─┴──────────┐
│BestCandidate│  │GoodCandidate│
│Selector     │  │Selector      │
├────────────┤  ├─────────────┤
│+ selectOne()│  │+ selectOne() │
│  (Power of │  │  (Uniform    │
│   Two)     │  │   Random)    │
└────────────┘  └─────────────┘
```

**设计意义**:
1. **算法可替换性**:无需修改 `AllocateTarget` 的核心逻辑,即可切换不同的选择策略
2. **开闭原则**:未来可扩展新的选择策略(如 WeightedSelector)而不影响现有代码
3. **测试友好**:可以独立测试每种策略的行为

**应用场景分析**:
```
场景1:Uprelication(健康状态下增加副本)
  - 选择策略:BestCandidateSelector
  - 原因:有足够时间选择最优候选,使用 Power of Two 防止热点
  - 结果:副本分布更均匀,长期系统更稳定

场景2:Recovery(故障后快速恢复副本)
  - 选择策略:GoodCandidateSelector
  - 原因:需要尽快恢复 quorum,只要满足约束即可
  - 结果:恢复速度快,避免 range 不可用时间过长
```

### 1.2 模板方法模式(Template Method Pattern)- ScorerOptions

**模式定义**:在一个方法中定义算法骨架,将某些步骤延迟到子类中。模板方法使得子类可以在不改变算法结构的情况下重定义某些步骤。

**在 Allocator 中的应用**:

```go
// 抽象类(接口)
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:285-349
type ScorerOptions interface {
    // 模板方法:定义算法骨架
    shouldRebalanceBasedOnThresholds(ctx context.Context, eqClass equivalenceClass, metrics AllocatorMetrics) bool
    balanceScore(sl storepool.StoreList, sc roachpb.StoreCapacity) balanceStatus
    rebalanceFromConvergesScore(eqClass equivalenceClass) int
    rebalanceToConvergesScore(eqClass equivalenceClass, candidate roachpb.StoreDescriptor) int
    removalMaximallyConvergesScore(removalCandStoreList storepool.StoreList, existing roachpb.StoreDescriptor) int

    // 辅助方法
    maybeJitterStoreStats(sl storepool.StoreList, allocRand allocatorRand) storepool.StoreList
    deterministicForTesting() bool
    adjustRangeCountForScoring(rangeCount int) int
    getIOOverloadOptions() IOOverloadOptions
    getDiskOptions() DiskCapacityOptions
}

// 具体类1:RangeCountScorerOptions(基于 Range 数量的均衡)
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:490-590
type RangeCountScorerOptions struct {
    BaseScorerOptions
    rangeRebalanceThreshold float64
}

func (o RangeCountScorerOptions) shouldRebalanceBasedOnThresholds(
    ctx context.Context, eqClass equivalenceClass, metrics AllocatorMetrics,
) bool {
    store := eqClass.existing
    sl := eqClass.candidateSL
    overfullThreshold := int32(math.Ceil(overfullRangeThreshold(&o, sl.CandidateRanges.Mean)))
    // 检查1:Store 的 RangeCount 是否远高于均值
    if store.Capacity.RangeCount > overfullThreshold {
        return true
    }
    // 检查2:Store 高于均值,且存在远低于均值的 Store
    if float64(store.Capacity.RangeCount) > sl.CandidateRanges.Mean {
        underfullThreshold := int32(math.Floor(underfullRangeThreshold(&o, sl.CandidateRanges.Mean)))
        for _, desc := range sl.Stores {
            if desc.Capacity.RangeCount < underfullThreshold {
                return true
            }
        }
    }
    return false
}

func (o *RangeCountScorerOptions) balanceScore(
    sl storepool.StoreList, sc roachpb.StoreCapacity,
) balanceStatus {
    maxRangeCount := overfullRangeThreshold(&o, sl.CandidateRanges.Mean)
    minRangeCount := underfullRangeThreshold(&o, sl.CandidateRanges.Mean)
    curRangeCount := float64(sc.RangeCount)
    if curRangeCount < minRangeCount {
        return underfull
    } else if curRangeCount >= maxRangeCount {
        return overfull
    }
    return aroundTheMean
}

// 具体类2:LoadScorerOptions(基于 QPS/CPU 的均衡)
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:596-749
type LoadScorerOptions struct {
    BaseScorerOptions
    LoadDims []load.Dimension
    LoadThreshold, MinLoadThreshold load.Load
    MinRequiredRebalanceLoadDiff load.Load
    RebalanceImpact load.Load
}

func (o LoadScorerOptions) shouldRebalanceBasedOnThresholds(
    ctx context.Context, eqClass equivalenceClass, metrics AllocatorMetrics,
) bool {
    bestStore, declineReason := o.getRebalanceTargetToMinimizeDelta(eqClass)
    // 使用不同的算法:负载差最小化
    return declineReason == shouldRebalance
}

func (o *LoadScorerOptions) balanceScore(
    sl storepool.StoreList, sc roachpb.StoreCapacity,
) balanceStatus {
    maxLoad := OverfullLoadThresholds(sl.LoadMeans(), o.LoadThreshold, o.MinLoadThreshold)
    minLoad := UnderfullLoadThresholds(sl.LoadMeans(), o.LoadThreshold, o.MinLoadThreshold)
    curLoad := sc.Load()
    if load.Less(curLoad, minLoad, o.LoadDims...) {
        return underfull
    } else if !load.Less(curLoad, maxLoad, o.LoadDims...) {
        return overfull
    } else {
        return aroundTheMean
    }
}

// 具体类3:BaseScorerOptionsNoConvergence(无收敛性检查)
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:437-484
type BaseScorerOptionsNoConvergence struct {
    BaseScorerOptions
}

func (bnc BaseScorerOptionsNoConvergence) shouldRebalanceBasedOnThresholds(
    _ context.Context, _ equivalenceClass, _ AllocatorMetrics,
) bool {
    return false  // 直接返回 false,不进行任何检查
}

func (bnc BaseScorerOptionsNoConvergence) balanceScore(
    _ storepool.StoreList, _ roachpb.StoreCapacity,
) balanceStatus {
    return aroundTheMean  // 所有 Store 都返回 aroundTheMean
}
```

**类层次结构**:
```
┌──────────────────────────────────────────┐
│   <<interface>> ScorerOptions            │
├──────────────────────────────────────────┤
│ + shouldRebalanceBasedOnThresholds()     │
│ + balanceScore()                         │
│ + rebalanceFromConvergesScore()          │
│ + rebalanceToConvergesScore()            │
│ + removalMaximallyConvergesScore()       │
└──────────────────────────────────────────┘
                  △
                  │ implements
    ┌─────────────┼─────────────┐
    │             │             │
┌───┴────────┐  ┌─┴──────────┐  ┌─┴──────────────┐
│RangeCount  │  │LoadScorer  │  │BaseScorerOptions│
│ScorerOptions│  │Options     │  │NoConvergence   │
├───────────┤  ├────────────┤  ├────────────────┤
│基于Range  │  │基于QPS/CPU │  │无收敛性检查    │
│数量均衡   │  │的均衡      │  │(MMA专用)       │
└───────────┘  └────────────┘  └────────────────┘
```

**设计意义**:
1. **算法骨架固定**:所有 ScorerOptions 都必须实现相同的方法集,确保 Allocator 可以统一调用
2. **实现细节可变**:不同场景使用不同的评分逻辑(Range 数量 vs QPS)
3. **复用基础逻辑**:BaseScorerOptions 提供通用实现(如 getIOOverloadOptions),子类只需覆盖特定方法

**实际应用场景**:
```
场景1:ReplicateQueue 使用 RangeCountScorerOptions
  - 目标:均衡 Range 数量
  - 理由:Range 数量是副本负载的直接指标,易于计算

场景2:StoreRebalancer 使用 LoadScorerOptions
  - 目标:均衡 QPS/CPU
  - 理由:QPS 反映实际业务负载,更精准

场景3:MMA 使用 BaseScorerOptionsNoConvergence
  - 目标:多维度联合优化
  - 理由:MMA 自身已进行收敛性检查,不需要单一维度的收敛逻辑
```

### 1.3 建造者模式(Builder Pattern)- AnalyzedConstraints

**模式定义**:将复杂对象的构建过程与表示分离,使得同样的构建过程可以创建不同的表示。

**在 Allocator 中的应用**:

```go
// 产品类
// pkg/kv/kvserver/constraint/analyzed_constraints.go
type AnalyzedConstraints struct {
    Constraints           []roachpb.ConstraintsConjunction  // 原始约束
    Satisfies             map[roachpb.StoreID][]int         // Store → 满足的约束索引
    SatisfiedBy           [][]roachpb.StoreID               // 约束索引 → 满足它的 Store 列表
    UnconstrainedReplicas bool                              // 是否允许不满足任何约束的副本
}

// 建造者函数(Builder)
// pkg/kv/kvserver/constraint/analyzed_constraints.go
func AnalyzeConstraints(
    storePool AllocatorStorePool,
    existingReplicas []roachpb.ReplicaDescriptor,
    numReplicas int32,
    constraints []roachpb.ConstraintsConjunction,
) AnalyzedConstraints {
    // 步骤1:初始化数据结构
    analyzed := AnalyzedConstraints{
        Constraints: constraints,
        Satisfies:   make(map[roachpb.StoreID][]int),
        SatisfiedBy: make([][]roachpb.StoreID, len(constraints)),
    }

    // 步骤2:对每个约束,找出所有满足它的 Store
    for i, constraint := range constraints {
        for _, replica := range existingReplicas {
            store, ok := storePool.GetStoreDescriptor(replica.StoreID)
            if !ok { continue }
            if CheckStoreConjunction(store, constraint.Constraints) {
                analyzed.SatisfiedBy[i] = append(analyzed.SatisfiedBy[i], store.StoreID)
            }
        }
    }

    // 步骤3:对每个 Store,找出它满足的约束
    for storeID := range existingReplicas {
        for i := range constraints {
            if containsStore(analyzed.SatisfiedBy[i], storeID) {
                analyzed.Satisfies[storeID] = append(analyzed.Satisfies[storeID], i)
            }
        }
    }

    // 步骤4:检查是否允许不满足约束的副本
    var totalConstrainedReplicas int32
    for _, constraint := range constraints {
        totalConstrainedReplicas += constraint.NumReplicas
    }
    analyzed.UnconstrainedReplicas = totalConstrainedReplicas < numReplicas

    return analyzed
}
```

**构建流程图**:
```
┌────────────────────────────────────────────────────────────┐
│ 输入:                                                       │
│   - storePool: 所有 Store 的状态                           │
│   - existingReplicas: 当前副本列表                         │
│   - numReplicas: 期望的副本总数                            │
│   - constraints: 用户配置的约束                            │
└────────────────────────────────────────────────────────────┘
                         ↓
┌────────────────────────────────────────────────────────────┐
│ 步骤1:初始化 AnalyzedConstraints                           │
│   - Constraints = constraints                              │
│   - Satisfies = {}                                          │
│   - SatisfiedBy = [[], [], ...]                            │
└────────────────────────────────────────────────────────────┘
                         ↓
┌────────────────────────────────────────────────────────────┐
│ 步骤2:构建 SatisfiedBy(约束 → Store 映射)                 │
│   for constraint_i in constraints:                         │
│     for replica in existingReplicas:                       │
│       if CheckStoreConjunction(replica.store, constraint_i):│
│         SatisfiedBy[i].append(replica.StoreID)             │
└────────────────────────────────────────────────────────────┘
                         ↓
┌────────────────────────────────────────────────────────────┐
│ 步骤3:构建 Satisfies(Store → 约束 映射)                   │
│   for storeID in existingReplicas:                         │
│     for i in range(len(constraints)):                      │
│       if storeID in SatisfiedBy[i]:                        │
│         Satisfies[storeID].append(i)                       │
└────────────────────────────────────────────────────────────┘
                         ↓
┌────────────────────────────────────────────────────────────┐
│ 步骤4:计算 UnconstrainedReplicas                           │
│   totalConstrainedReplicas = sum(c.NumReplicas for c in constraints)│
│   UnconstrainedReplicas = (totalConstrainedReplicas < numReplicas) │
└────────────────────────────────────────────────────────────┘
                         ↓
┌────────────────────────────────────────────────────────────┐
│ 输出: 完整的 AnalyzedConstraints 对象                       │
└────────────────────────────────────────────────────────────┘
```

**设计意义**:
1. **复杂对象构建**:AnalyzedConstraints 包含多个相互关联的数据结构,建造者模式封装了构建逻辑
2. **分步构建**:每个步骤独立完成一部分工作,易于理解和维护
3. **避免不一致状态**:构建完成前对象不可见,确保数据一致性

**使用示例**:
```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1507-1518
analyzedOverallConstraints := constraint.AnalyzeConstraints(
    storePool,
    existingReplicas,
    conf.NumReplicas,
    conf.Constraints,
)
analyzedVoterConstraints := constraint.AnalyzeConstraints(
    storePool,
    existingVoters,
    conf.GetNumVoters(),
    conf.VoterConstraints,
)
```

### 1.4 责任链模式(Chain of Responsibility)- 候选过滤链

**模式定义**:为请求创建一个接收者对象的链,每个接收者都包含对另一个接收者的引用。如果一个对象不能处理该请求,它会把请求传递给下一个接收者。

**在 Allocator 中的应用**(隐式责任链):

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1211-1303
func rankedCandidateListForAllocation(
    ctx context.Context,
    candidateStores storepool.StoreList,
    constraintsCheck constraintsCheckFn,
    existingReplicas []roachpb.ReplicaDescriptor,
    nonVoterReplicas []roachpb.ReplicaDescriptor,
    existingStoreLocalities map[roachpb.StoreID]roachpb.Locality,
    isStoreValidForRoutineReplicaTransfer func(context.Context, roachpb.StoreID) bool,
    allowMultipleReplsPerNode bool,
    options ScorerOptions,
    targetType TargetReplicaType,
) candidateList {
    var candidates candidateList
    existingReplTargets := roachpb.MakeReplicaSet(existingReplicas).ReplicationTargets()
    var nonVoterReplTargets []roachpb.ReplicationTarget

    // 过滤器链:
    // 过滤器1:移除不满足基本条件的 Store
    validCandidateStores := []roachpb.StoreDescriptor{}
    for _, s := range candidateStores.Stores {
        // 检查1:磁盘容量
        if !options.getDiskOptions().maxCapacityCheck(s) {
            continue  // 磁盘满,跳过
        }
        // 检查2:IO 过载
        if !options.getIOOverloadOptions().allocateReplicaToCheck(ctx, s, candidateStores) {
            continue  // IO 过载,跳过
        }
        // 检查3:约束
        if constraintsOK, _ := constraintsCheck(s); constraintsOK {
            validCandidateStores = append(validCandidateStores, s)
        }
    }

    validStoreList := storepool.MakeStoreList(validCandidateStores)

    // 过滤器2:移除已有副本的 Store
    for _, s := range validStoreList.Stores {
        if StoreHasReplica(s.StoreID, existingReplTargets) {
            continue  // 已有副本,跳过
        }
        // 过滤器3:检查同节点多副本限制
        if !allowMultipleReplsPerNode && nodeHasReplica(s.Node.NodeID, existingReplTargets) {
            continue  // 同节点已有副本,跳过
        }
        // 过滤器4:检查节点是否可用
        if !isStoreValidForRoutineReplicaTransfer(ctx, s.StoreID) {
            continue  // 节点不可用,跳过
        }

        // 通过所有过滤器,加入候选列表
        constraintsOK, necessary := constraintsCheck(s)
        diversityScore := diversityAllocateScore(s.Locality(), existingStoreLocalities)
        balanceScore := options.balanceScore(validStoreList, s.Capacity)
        var hasNonVoter bool
        if targetType == VoterTarget {
            if nonVoterReplTargets == nil {
                nonVoterReplTargets = roachpb.MakeReplicaSet(nonVoterReplicas).ReplicationTargets()
            }
            hasNonVoter = StoreHasReplica(s.StoreID, nonVoterReplTargets)
        }
        rangeCountScore := options.adjustRangeCountForScoring(int(s.Capacity.RangeCount))
        candidates = append(candidates, candidate{
            store:          s,
            necessary:      necessary,
            valid:          constraintsOK,
            diversityScore: diversityScore,
            balanceScore:   balanceScore,
            hasNonVoter:    hasNonVoter,
            rangeCount:     rangeCountScore,
        })
    }

    // 最终排序
    if options.deterministicForTesting() {
        sort.Sort(sort.Reverse(byScoreAndID(candidates)))
    } else {
        sort.Sort(sort.Reverse(byScore(candidates)))
    }
    return candidates
}
```

**责任链结构**:
```
Store候选 → 过滤器1(磁盘容量) → 过滤器2(IO过载) → 过滤器3(约束检查)
                ↓ 通过                ↓ 通过              ↓ 通过
          过滤器4(已有副本) → 过滤器5(同节点限制) → 过滤器6(节点可用性)
                ↓ 通过                ↓ 通过              ↓ 通过
                           → 候选列表(打分排序) →
```

**设计意义**:
1. **解耦过滤逻辑**:每个过滤器独立判断,易于增删改
2. **早期终止**:不满足条件的 Store 立即跳过,避免无效计算
3. **可扩展性**:未来可轻松添加新的过滤器(如网络延迟检查)

### 1.5 观察者模式(Observer Pattern)- StorePool 与 Gossip

**模式定义**:定义对象间的一对多依赖关系,当一个对象状态改变时,所有依赖它的对象都得到通知并自动更新。

**在 Allocator 中的应用**:

```go
// 主题(Subject):Gossip Network
// pkg/gossip/gossip.go
type Gossip struct {
    callbacks map[string][]callback  // key → 回调列表
    // ...
}

type callback struct {
    fn func(key string, value roachpb.Value)
}

func (g *Gossip) RegisterCallback(pattern string, fn func(key string, value roachpb.Value)) {
    g.callbacks[pattern] = append(g.callbacks[pattern], callback{fn: fn})
}

// 当 Gossip 收到更新时,通知所有注册的回调
func (g *Gossip) notifyCallbacks(key string, value roachpb.Value) {
    for pattern, cbs := range g.callbacks {
        if strings.HasPrefix(key, pattern) {
            for _, cb := range cbs {
                cb.fn(key, value)
            }
        }
    }
}

// 观察者(Observer):StorePool
// pkg/kv/kvserver/allocator/storepool/store_pool.go
type StorePool struct {
    gossip         *gossip.Gossip
    storeDetails   map[roachpb.StoreID]*storeDetail
    // ...
}

func NewStorePool(gossip *gossip.Gossip, ...) *StorePool {
    sp := &StorePool{
        gossip:       gossip,
        storeDetails: make(map[roachpb.StoreID]*storeDetail),
    }

    // 注册为 Gossip 的观察者
    gossip.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix),
        func(key string, value roachpb.Value) {
            sp.onStoreDescriptorUpdate(key, value)
        },
    )

    return sp
}

// StorePool 的更新回调
func (sp *StorePool) onStoreDescriptorUpdate(key string, value roachpb.Value) {
    var desc roachpb.StoreDescriptor
    if err := value.GetProto(&desc); err != nil {
        return
    }

    sp.mu.Lock()
    defer sp.mu.Unlock()

    // 更新缓存
    if detail, ok := sp.storeDetails[desc.StoreID]; ok {
        detail.desc = &desc
        detail.lastUpdatedTime = hlc.Now()
    } else {
        sp.storeDetails[desc.StoreID] = &storeDetail{
            desc:            &desc,
            lastUpdatedTime: hlc.Now(),
        }
    }

    // 重新计算集群均值
    sp.recalculateMeans()
}
```

**时序图**:
```
Store1          Gossip Network        StorePool         Allocator
  │                  │                    │                 │
  │ StoreDescriptor  │                    │                 │
  │ (每10秒广播)     │                    │                 │
  ├─────────────────>│                    │                 │
  │                  │ notifyCallbacks()  │                 │
  │                  ├───────────────────>│                 │
  │                  │                    │                 │
  │                  │                    │ onStoreDescriptorUpdate()
  │                  │                    ├─────────┐       │
  │                  │                    │ 更新缓存 │       │
  │                  │                    │ 重算均值 │       │
  │                  │                    │<────────┘       │
  │                  │                    │                 │
  │                  │                    │ GetStoreList()  │
  │                  │                    │<────────────────┤
  │                  │                    │                 │
  │                  │                    │ StoreList(最新) │
  │                  │                    ├────────────────>│
  │                  │                    │                 │
```

**设计意义**:
1. **解耦发布者和订阅者**:Store 通过 Gossip 广播状态,无需知道 StorePool 的存在
2. **一对多通知**:一个 Store 更新可以通知多个观察者(StorePool, ReplicateQueue 等)
3. **实时性**:StorePool 始终维护最新的集群状态,Allocator 可以基于最新数据决策

### 1.6 状态模式(State Pattern)- IOOverloadEnforcementLevel

**模式定义**:允许对象在内部状态改变时改变其行为,对象看起来好像修改了它的类。

**在 Allocator 中的应用**:

```go
// 状态枚举
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:110-131
type IOOverloadEnforcementLevel int64

const (
    IOOverloadThresholdIgnore           IOOverloadEnforcementLevel = iota  // 状态1:忽略
    IOOverloadThresholdBlockTransfers                                       // 状态2:阻止 Transfer
    IOOverloadThresholdBlockAll                                             // 状态3:阻止所有
    IOOverloadThresholdShed                                                 // 状态4:主动驱逐
)

// 上下文(Context)
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:2523-2539
type IOOverloadOptions struct {
    ReplicaEnforcementLevel IOOverloadEnforcementLevel  // 副本操作的执行级别
    LeaseEnforcementLevel   IOOverloadEnforcementLevel  // Lease 操作的执行级别

    ReplicaIOOverloadThreshold   float64
    LeaseIOOverloadThreshold     float64
    LeaseIOOverloadShedThreshold float64
    DiskUnhealthyScore           float64
}

// 状态依赖的行为
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:2651-2667
func (o IOOverloadOptions) allocateReplicaToCheck(
    ctx context.Context, store roachpb.StoreDescriptor, storeList storepool.StoreList,
) bool {
    score, diskUnhealthyScore := o.storeScore(store)
    avg := o.storeListAvgScore(storeList)

    // 根据 ReplicaEnforcementLevel 状态决定行为
    if ok, reason := ioOverloadCheck(score, avg, diskUnhealthyScore,
        o.ReplicaIOOverloadThreshold, IOOverloadMeanThreshold,
        o.ReplicaEnforcementLevel,
        IOOverloadThresholdBlockAll,  // 只有 BlockAll 状态会拒绝 allocation
    ); !ok {
        log.KvDistribution.VEventf(ctx, 3, "s%d: %s", store.StoreID, reason)
        return false
    }
    return true
}

func (o IOOverloadOptions) rebalanceReplicaToCheck(
    ctx context.Context, store roachpb.StoreDescriptor, storeList storepool.StoreList,
) bool {
    score, diskUnhealthyScore := o.storeScore(store)
    avg := o.storeListAvgScore(storeList)

    // 根据 ReplicaEnforcementLevel 状态决定行为
    if ok, reason := ioOverloadCheck(score, avg, diskUnhealthyScore,
        o.ReplicaIOOverloadThreshold, IOOverloadMeanThreshold,
        o.ReplicaEnforcementLevel,
        IOOverloadThresholdBlockTransfers, IOOverloadThresholdBlockAll,  // 两个状态都会拒绝 rebalance
    ); !ok {
        log.KvDistribution.VEventf(ctx, 3, "s%d: %s", store.StoreID, reason)
        return false
    }
    return true
}
```

**状态转换图**:
```
┌──────────────────────────────────────────────────────────────┐
│                 IOOverloadEnforcementLevel                    │
└──────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
  ┌─────────────┐     ┌──────────────┐     ┌──────────────┐
  │   Ignore    │     │BlockTransfers│     │   BlockAll   │
  │ 忽略IO过载  │     │阻止Transfer  │     │ 阻止所有操作 │
  │  (状态1)    │     │   (状态2)    │     │   (状态3)    │
  └─────────────┘     └──────────────┘     └──────────────┘
         │                    │                    │
         │ 集群负载升高       │ 负载进一步升高     │
         └───────────────────>└───────────────────>│
         │                    │                    │
         │ 集群负载降低       │ 负载降低           │
         │<───────────────────┘<───────────────────┘
         │                    │                    │
         │                    ▼                    ▼
         │            ┌──────────────┐     ┌──────────────┐
         │            │allocateReplica│     │rebalanceReplica│
         │            │   允许        │     │    拒绝        │
         │            └──────────────┘     └──────────────┘
         │
         ▼
  ┌──────────────┐
  │     Shed     │
  │ 主动驱逐Lease│
  │   (状态4)    │
  └──────────────┘
         │
         ▼
  ┌──────────────┐
  │ExistingLease │
  │    拒绝      │
  └──────────────┘
```

**行为矩阵**:

| 状态 / 操作          | allocateReplica | rebalanceReplica | transferLease | existingLease |
|----------------------|-----------------|------------------|---------------|---------------|
| Ignore               | 允许            | 允许             | 允许          | 允许          |
| BlockTransfers       | 允许            | 拒绝             | 拒绝          | 允许          |
| BlockAll             | 拒绝            | 拒绝             | 拒绝          | 允许          |
| Shed                 | N/A(仅用于Lease)| N/A              | 拒绝          | 拒绝          |

**设计意义**:
1. **状态封装**:IO Overload 的处理逻辑封装在状态中,调用者无需关心内部细节
2. **行为多态**:同一个操作(如 allocateReplica)在不同状态下有不同行为
3. **易于扩展**:未来可添加新的状态(如 BlockAllWithGracePeriod)而不影响现有代码

### 1.7 组合模式(Composite Pattern)- Constraints 层级结构

**模式定义**:将对象组合成树形结构以表示"部分-整体"的层次结构,使得用户对单个对象和组合对象的使用具有一致性。

**在 Allocator 中的应用**:

```go
// 组件(Component):Constraint
// pkg/roachpb/metadata.proto
message Constraint {
  Type type = 1;        // REQUIRED/PROHIBITED
  string key = 2;       // "region"/"zone"/"rack"
  string value = 3;     // "us-east"/"us-east-1a"
}

// 组合(Composite):ConstraintsConjunction(AND 组合)
message ConstraintsConjunction {
  int32 num_replicas = 1;                  // 满足此约束的副本数
  repeated Constraint constraints = 2;      // 多个约束的 AND 组合
}

// 更高层组合:SpanConfig(OR 组合)
message SpanConfig {
  int32 num_replicas = 1;
  repeated ConstraintsConjunction constraints = 2;         // 所有副本的约束(OR 组合)
  repeated ConstraintsConjunction voter_constraints = 3;   // Voter 副本的约束(OR 组合)
}
```

**树形结构**:
```
SpanConfig(Root)
  ├─ Constraints (OR)
  │   ├─ ConstraintsConjunction1 (AND, NumReplicas=2)
  │   │   ├─ Constraint{type=REQUIRED, key="region", value="us-east"}
  │   │   └─ Constraint{type=REQUIRED, key="zone", value="us-east-1a"}
  │   └─ ConstraintsConjunction2 (AND, NumReplicas=1)
  │       └─ Constraint{type=REQUIRED, key="region", value="us-west"}
  └─ VoterConstraints (OR)
      └─ ConstraintsConjunction3 (AND, NumReplicas=3)
          └─ Constraint{type=PROHIBITED, key="datacenter", value="dc-test"}
```

**检查算法**:
```go
// pkg/kv/kvserver/constraint/constraint.go
func CheckStoreConjunction(
    store roachpb.StoreDescriptor,
    constraints []roachpb.Constraint,
) bool {
    // 检查单个 ConstraintsConjunction(AND 组合)
    for _, constraint := range constraints {
        if !CheckStore(store, constraint) {
            return false  // 任何一个约束不满足,整个 AND 组合失败
        }
    }
    return true  // 所有约束都满足,AND 组合成功
}

func CheckStore(store roachpb.StoreDescriptor, constraint roachpb.Constraint) bool {
    // 检查单个 Constraint
    for _, tier := range store.Node.Locality.Tiers {
        if tier.Key == constraint.Key {
            match := tier.Value == constraint.Value
            if constraint.Type == roachpb.Constraint_REQUIRED {
                return match
            } else { // PROHIBITED
                return !match
            }
        }
    }
    // 未找到对应的 locality tier
    if constraint.Type == roachpb.Constraint_REQUIRED {
        return false  // REQUIRED 约束未满足
    }
    return true  // PROHIBITED 约束默认满足(未找到即不违反)
}

// pkg/kv/kvserver/constraint/analyzed_constraints.go
func AnalyzeConstraints(...) AnalyzedConstraints {
    // 对每个 ConstraintsConjunction(OR 组合的一员)进行检查
    for i, conjunction := range constraints {
        for _, replica := range existingReplicas {
            store, _ := storePool.GetStoreDescriptor(replica.StoreID)
            // 检查 AND 组合
            if CheckStoreConjunction(store, conjunction.Constraints) {
                analyzed.SatisfiedBy[i] = append(analyzed.SatisfiedBy[i], store.StoreID)
            }
        }
    }
    // ...
}
```

**设计意义**:
1. **统一接口**:单个约束和组合约束使用相同的检查逻辑
2. **灵活组合**:可以表达复杂的约束关系(如"(region=A AND zone=1a) OR (region=B AND zone=2b)")
3. **递归处理**:CheckStoreConjunction 递归检查所有子约束

**实际应用示例**:
```
用户配置:
  num_replicas: 5
  constraints: [
    {num_replicas: 2, constraints: [{type: REQUIRED, key: "region", value: "us-east"}]},
    {num_replicas: 2, constraints: [{type: REQUIRED, key: "region", value: "us-west"}]},
    {num_replicas: 1, constraints: [{type: REQUIRED, key: "region", value: "eu-central"}]},
  ]

解释:
  - 5 个副本总共
  - 其中 2 个必须在 us-east region
  - 其中 2 个必须在 us-west region
  - 其中 1 个必须在 eu-central region
  - 这 3 个约束是 OR 关系(每个副本满足其中一个即可)
```

### 1.8 备忘录模式(Memento Pattern)- UpdateLocalStoreAfterRebalance

**模式定义**:在不破坏封装的前提下,捕获对象的内部状态,并在该对象之外保存这个状态,以便日后恢复。

**在 Allocator 中的应用**:

```go
// 发起者(Originator):StorePool
// pkg/kv/kvserver/allocator/storepool/store_pool.go
type StorePool struct {
    storeDetails map[roachpb.StoreID]*storeDetail
    // ...
}

type storeDetail struct {
    desc *roachpb.StoreDescriptor  // 包含 RangeCount, QPS 等状态
    // ...
}

// 备忘录(Memento):隐式存储在 StoreDescriptor 中的原始值
// 操作方法
func (sp *StorePool) UpdateLocalStoreAfterRebalance(
    storeID roachpb.StoreID,
    rangeUsageInfo allocator.RangeUsageInfo,
    changeType roachpb.ReplicaChangeType,
) {
    sp.mu.Lock()
    defer sp.mu.Unlock()

    detail := sp.storeDetails[storeID]
    if detail == nil {
        return
    }

    // 修改状态(临时)
    if changeType == roachpb.ADD_VOTER {
        detail.desc.Capacity.RangeCount++
        detail.desc.Capacity.QueriesPerSecond += rangeUsageInfo.QueriesPerSecond
    } else if changeType == roachpb.REMOVE_VOTER {
        detail.desc.Capacity.RangeCount--
        detail.desc.Capacity.QueriesPerSecond -= rangeUsageInfo.QueriesPerSecond
    }
}

// 管理者(Caretaker):Allocator
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1594-1648
func (a Allocator) simulateRemoveTarget(...) {
    // 步骤1:保存原始状态(通过添加操作)
    storePool.UpdateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.ADD_VOTER)

    // 步骤2:使用修改后的状态进行决策
    defer storePool.UpdateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.REMOVE_VOTER)

    // 步骤3:在 defer 中恢复原始状态
    return a.RemoveTarget(
        ctx, storePool, conf, storepool.MakeStoreList(candidateStores),
        existingVoters, existingNonVoters, VoterTarget, options,
    )
}
```

**状态转换时序图**:
```
┌────────────────────────────────────────────────────────────────┐
│                    simulateRemoveTarget                        │
└────────────────────────────────────────────────────────────────┘
         │
         ├─ 步骤1:保存原始状态(隐式,通过 ADD 操作模拟)
         │  Store3.RangeCount = 100
         │  Store3.QPS = 900
         │  ↓
         │  storePool.UpdateLocalStoreAfterRebalance(Store3, rangeUsageInfo, ADD_VOTER)
         │  ↓
         │  Store3.RangeCount = 101 (临时修改)
         │  Store3.QPS = 910 (临时修改)
         │
         ├─ 步骤2:使用修改后的状态进行决策
         │  ↓
         │  RemoveTarget(...)  // 基于修改后的状态选择应移除的副本
         │  ↓
         │  返回:removeReplica = Store2
         │
         └─ 步骤3:恢复原始状态(defer 执行)
            ↓
            storePool.UpdateLocalStoreAfterRebalance(Store3, rangeUsageInfo, REMOVE_VOTER)
            ↓
            Store3.RangeCount = 100 (恢复)
            Store3.QPS = 900 (恢复)
```

**设计意义**:
1. **状态隔离**:模拟操作不会影响 StorePool 的真实状态
2. **事务性**:通过 defer 确保状态恢复,即使中间发生错误
3. **简洁性**:无需显式创建备忘录对象,通过 ADD/REMOVE 操作对实现撤销

---

## 二、端到端时间序列示例

### 2.1 示例1:正常 Uprelication(新增副本)

**初始场景**:
```
集群状态:
  - Store1: region=us-east, zone=us-east-1a, RangeCount=100, QPS=1000, IOScore=0.2
  - Store2: region=us-east, zone=us-east-1b, RangeCount=105, QPS=1050, IOScore=0.25
  - Store3: region=us-west, zone=us-west-2a, RangeCount=95, QPS=950, IOScore=0.15
  - Store4: region=us-west, zone=us-west-2b, RangeCount=98, QPS=980, IOScore=0.18
  - 集群均值:RangeCount=99.5, QPS=995, IOScore=0.195

Range R1 状态:
  - 当前副本:[Store1, Store2]
  - 期望副本数:3
  - 约束:num_replicas=3, constraints=[{num_replicas: 3, constraints: []}](无特殊约束)
  - Range 负载:QPS=10
```

**时间序列**:

**T0: ComputeAction 决策阶段**
```
调用:ComputeAction(ctx, conf, existingVoters=[Store1, Store2], existingNonVoters=[])

执行流程:
  1. 检查 Voter 状态:
     - neededVoters = GetNeededVoters(3, 4) = 3
     - haveVoters = 2
     - deadVoters = 0, decommissioningVoters = 0, liveVoters = 2
     - 判断:haveVoters < neededVoters → 需要添加 Voter

  2. 计算优先级:
     - 当前 quorum = (2 / 2) + 1 = 2(最小多数)
     - 添加后 quorum = (3 / 2) + 1 = 2(仍为 2)
     - priority = AllocatorAddVoter.Priority() + (2 - 2) * 100 = 500 + 0 = 500

  3. 返回:(AllocatorAddVoter, priority=500)
```

**T1: AllocateVoter 执行阶段**
```
调用:AllocateVoter(ctx, storePool, conf, existingVoters=[Store1, Store2], existingNonVoters=[], replacing=nil, replicaStatus=Alive)

执行流程:
  1. 选择 CandidateSelector:
     - replicaStatus == Alive → selector = BestCandidateSelector

  2. 获取候选 Store 列表:
     - candidateStoreList = storePool.GetStoreList(StoreFilterThrottled)
     - 返回:[Store1, Store2, Store3, Store4](无限流)

  3. 调用 allocateTargetFromList:
     - 约束检查器:voterConstraintsCheckerForAllocation(无特殊约束)
     - 过滤已有副本:移除 Store1, Store2
     - 候选列表:[Store3, Store4]
```

**T2: rankedCandidateListForAllocation 打分阶段**
```
输入:candidateStores=[Store3, Store4], existingReplicas=[Store1, Store2]

打分过程:
  Store3:
    - valid: true(满足约束)
    - necessary: false(约束不需要特定 Store)
    - fullDisk: false(容量检查通过)
    - ioOverloaded: false(IOScore=0.15 < 0.3)
    - diversityScore: diversityAllocateScore(Store3.Locality, {Store1.Locality, Store2.Locality})
      = (DiversityScore(us-west-2a, us-east-1a) + DiversityScore(us-west-2a, us-east-1b)) / 2
      = (1.0 + 1.0) / 2 = 1.0(完全不同的 region)
    - balanceScore: balanceScore(validStoreList, Store3.Capacity)
      - validStoreList.CandidateRanges.Mean = (95 + 98) / 2 = 96.5
      - overfullThreshold = 96.5 + max(96.5*0.05, 2) = 96.5 + 4.825 = 101.325
      - underfullThreshold = 96.5 - 4.825 = 91.675
      - Store3.RangeCount = 95
      - 91.675 < 95 < 101.325 → aroundTheMean
    - convergesScore: 0(allocation 不计算)
    - hasNonVoter: false
    - rangeCount: 95

  Store4:
    - valid: true
    - necessary: false
    - fullDisk: false
    - ioOverloaded: false(IOScore=0.18 < 0.3)
    - diversityScore: (DiversityScore(us-west-2b, us-east-1a) + DiversityScore(us-west-2b, us-east-1b)) / 2
      = (1.0 + 1.0) / 2 = 1.0(同样完全不同的 region)
    - balanceScore: aroundTheMean(同 Store3 计算逻辑)
    - convergesScore: 0
    - hasNonVoter: false
    - rangeCount: 98

排序结果:
  - Store3 和 Store4 的 valid/necessary/fullDisk/ioOverloaded/diversityScore/balanceScore 都相同
  - 差异仅在 rangeCount: Store3=95 < Store4=98
  - 排序:[Store3, Store4](Store3 优先,因为 RangeCount 更少)
```

**T3: BestCandidateSelector 选择阶段**
```
调用:selectBest([Store3, Store4])

执行流程:
  1. 筛选 best() 组:
     - Store3 和 Store4 的所有评分维度都相同(除了 rangeCount)
     - best() 返回:[Store3](只有一个,因为 rangeCount 不同导致排序区分)

  2. Power of Two Random Choices:
     - len(best) == 1 → 直接返回 Store3

结果:target = Store3
```

**T4: CheckAvoidsFragileQuorum 检查阶段**
```
调用:CheckAvoidsFragileQuorum(ctx, storePool, conf, existingVoters=[Store1, Store2], remainingLiveNonVoters=[], replicaStatus=Alive, replicaType=VoterTarget, newTarget=Store3, isReplacement=false)

执行流程:
  1. 检查是否会导致 fragile quorum:
     - newVoters = 1(不是替换,是新增)
     - neededVoters = 3
     - WillHaveFragileQuorum(len(existingVoters)=2, newVoters=1, neededVoters=3, clusterNodes=4)
       = (2 + 1 == 3 - 1) && (3 % 2 == 0) = (3 == 2) && false = false
     - 结果:不会导致 fragile quorum

  2. 返回:nil(无错误,允许添加)
```

**T5: 最终结果**
```
返回:
  - target: {NodeID: Store3.NodeID, StoreID: Store3.StoreID}
  - details: "s3, valid:true, fulldisk:false, diversity:1.00, balance:0, rangeCount:95"
  - error: nil

后续操作:
  - ReplicateQueue 发起 ChangeReplicas RPC 到 Raft leader
  - Raft leader 执行 AddReplica(Store3)
  - Store3 接收 snapshot 并追赶 log
  - Store3 成为 Learner → Voter
  - Range R1 的副本列表更新为:[Store1, Store2, Store3]
```

**关键时间点**:
- T0-T1: 决策阶段(约 1-2ms,纯计算)
- T1-T2: 候选筛选(约 1-3ms,需要访问 StorePool 缓存)
- T2-T3: 打分排序(约 2-5ms,计算 diversity/balance score)
- T3-T4: 选择与检查(约 1-2ms)
- T5-后续: Raft 同步(约 100-500ms,取决于 snapshot 大小和网络延迟)

### 2.2 示例2:负载均衡 Rebalance

**初始场景**:
```
集群状态:
  - Store1: region=us-east, zone=us-east-1a, RangeCount=120, QPS=1200, IOScore=0.35
  - Store2: region=us-east, zone=us-east-1b, RangeCount=105, QPS=1050, IOScore=0.25
  - Store3: region=us-west, zone=us-west-2a, RangeCount=95, QPS=950, IOScore=0.15
  - Store4: region=us-west, zone=us-west-2b, RangeCount=80, QPS=800, IOScore=0.10
  - 集群均值:RangeCount=100, QPS=1000, IOScore=0.2125

Range R2 状态:
  - 当前副本:[Store1, Store2, Store3]
  - 期望副本数:3
  - 约束:num_replicas=3(无特殊约束)
  - Range 负载:QPS=10
```

**T0: ComputeAction 决策阶段**
```
调用:ComputeAction(ctx, conf, existingVoters=[Store1, Store2, Store3], existingNonVoters=[])

执行流程:
  1. 检查 Voter 状态:
     - neededVoters = 3, haveVoters = 3 → 副本数正确
     - deadVoters = 0, decommissioningVoters = 0

  2. 检查是否需要 Replace:
     - 无 dead/decommissioning 副本 → 不需要 Replace

  3. 检查 NonVoter 状态:
     - neededNonVoters = 0, haveNonVoters = 0 → 正确

  4. 返回:(AllocatorConsiderRebalance, priority=0)
```

**T1: RebalanceVoter 执行阶段**
```
调用:RebalanceVoter(ctx, storePool, conf, raftStatus, existingVoters=[Store1, Store2, Store3], existingNonVoters=[], rangeUsageInfo={QPS: 10}, filter=StoreFilterNone, options=RangeCountScorerOptions)

执行流程:
  1. 获取 Store 列表:
     - sl = storePool.GetStoreList(StoreFilterNone)
     - 返回:[Store1, Store2, Store3, Store4]

  2. 调用 rankedCandidateListForRebalancing
```

**T2: rankedCandidateListForRebalancing 等价类构建阶段**
```
输入:allStores=[Store1, Store2, Store3, Store4], existingVoters=[Store1, Store2, Store3]

步骤1:标记现有副本的状态
  - Store1:valid=true, necessary=false, fullDisk=false, ioOverloaded=false, diversityScore=RangeDiversityScore({Store1, Store2, Store3})
    = average(DiversityScore(Store1, Store2), DiversityScore(Store1, Store3), DiversityScore(Store2, Store3))
    = average(0.5, 1.0, 1.0) = 0.833
  - Store2:valid=true, necessary=false, fullDisk=false, ioOverloaded=false, diversityScore=0.833
  - Store3:valid=true, necessary=false, fullDisk=false, ioOverloaded=false, diversityScore=0.833

步骤2:为每个现有副本构建等价类
  等价类1(existing=Store1):
    候选:
      - Store2:排除(已有副本)
      - Store3:排除(已有副本)
      - Store4:
        - constraintsOK = true, necessary = false, voterNecessary = false
        - fullDisk = false
        - diversityScore = diversityRebalanceFromScore(Store4.Locality, Store1.StoreID, existingLocalities)
          = average(
              DiversityScore(us-west-2b, us-east-1b),  // Store4 与 Store2
              DiversityScore(us-west-2b, us-west-2a),  // Store4 与 Store3
              DiversityScore(us-east-1b, us-west-2a)   // Store2 与 Store3(保留的副本之间)
            )
          = average(1.0, 0.5, 1.0) = 0.833
        - 判断:!Store4.less(Store1) → Store4 至少和 Store1 一样好
    等价类1 = {existing: Store1, candidates: [Store4]}

  等价类2(existing=Store2):
    候选:
      - Store4:
        - diversityScore = diversityRebalanceFromScore(Store4.Locality, Store2.StoreID, existingLocalities)
          = average(
              DiversityScore(us-west-2b, us-east-1a),  // Store4 与 Store1
              DiversityScore(us-west-2b, us-west-2a),  // Store4 与 Store3
              DiversityScore(us-east-1a, us-west-2a)   // Store1 与 Store3
            )
          = average(1.0, 0.5, 1.0) = 0.833
    等价类2 = {existing: Store2, candidates: [Store4]}

  等价类3(existing=Store3):
    候选:
      - Store4:
        - diversityScore = diversityRebalanceFromScore(Store4.Locality, Store3.StoreID, existingLocalities)
          = average(
              DiversityScore(us-west-2b, us-east-1a),  // Store4 与 Store1
              DiversityScore(us-west-2b, us-east-1b),  // Store4 与 Store2
              DiversityScore(us-east-1a, us-east-1b)   // Store1 与 Store2
            )
          = average(1.0, 1.0, 0.5) = 0.833
    等价类3 = {existing: Store3, candidates: [Store4]}

步骤3:判断是否需要 rebalance
  - needRebalanceFrom = false(无 invalid/fullDisk 副本)
  - needRebalanceTo = false(候选的 diversityScore 与 existing 相同)
  - shouldRebalanceCheck = ?

    对于等价类1(existing=Store1):
      - options.shouldRebalanceBasedOnThresholds(等价类1, metrics)
      - 检查:
        - Store1.RangeCount = 120
        - candidateSL.Mean = (120 + 80) / 2 = 100(只包含 Store1 和候选 Store4)
        - overfullThreshold = 100 + max(100*0.05, 2) = 105
        - Store1.RangeCount=120 > 105 → return true

  结果:shouldRebalanceCheck = true(Store1 过载)
```

**T3: 计算 convergesScore 和 balanceScore**
```
对于等价类1:
  existing(Store1):
    - convergesScore = rebalanceFromConvergesScore(等价类1)
      - 检查:从 Store1(RangeCount=120) 移除一个 Range 后,是否收敛到均值 100
      - rebalanceConvergesRangeCountOnMean(candidateSL, Store1.Capacity, 120-1)
      - 检查:|119 - 100| < |120 - 100| → 19 < 20 → true
      - 返回:0(收敛,允许从 Store1 移除)
    - balanceScore = balanceScore(candidateSL, Store1.Capacity)
      - candidateSL.Mean = 100
      - overfullThreshold = 105, underfullThreshold = 95
      - Store1.RangeCount = 120 > 105 → overfull
    - rangeCount = 120

  candidate(Store4):
    - convergesScore = rebalanceToConvergesScore(等价类1, Store4)
      - 检查:向 Store4(RangeCount=80) 添加一个 Range 后,是否收敛到均值 100
      - rebalanceConvergesRangeCountOnMean(candidateSL, Store4.Capacity, 80+1)
      - 检查:|81 - 100| < |80 - 100| → 19 < 20 → true
      - 返回:1(收敛,优先向 Store4 添加)
    - balanceScore = balanceScore(candidateSL, Store4.Capacity)
      - Store4.RangeCount = 80 < 95 → underfull
    - ioOverloaded = false(IOScore=0.10 < 0.3)
    - ioOverloadScore = 0.10
    - rangeCount = 80

对于等价类2和3:类似计算,但 Store2 和 Store3 的 RangeCount 都不超过 overfullThreshold
```

**T4: bestRebalanceTarget 选择阶段**
```
输入:results = [
  {existing: Store1(overfull, convergesScore=0, rangeCount=120), candidates: [Store4(underfull, convergesScore=1, rangeCount=80)]},
  {existing: Store2(aroundTheMean, convergesScore=1, rangeCount=105), candidates: [Store4(...)]},
  {existing: Store3(aroundTheMean, convergesScore=1, rangeCount=95), candidates: [Store4(...)]},
]

执行流程:
  1. 遍历所有 rebalanceOptions:
     - option1: target=Store4, existing=Store1
       - betterRebalanceTarget(Store4, Store1, nil, nil) = Store4(首次,直接返回)
       - bestIdx = 0, bestTarget = Store4, replaces = Store1

  2. 构建 MMA advisor(首次):
     - options[0].advisor = as.BuildMMARebalanceAdvisor(Store1.StoreID, [Store4.StoreID])

  3. MMA 冲突检查:
     - !Store1.isCriticalRebalance(Store4) → false(diversityScore 相同,不是 critical)
     - as.IsInConflictWithMMA(ctx, Store4.StoreID, advisor, false)
       - meansLoad = {QPS: (1200 + 800) / 2 = 1000}
       - Store4.QPS = 800 < 1000 * 1.2 = 1200 → 无冲突
     - 返回:false(允许 rebalance)

  4. 返回:target=Store4, existingCandidate=Store1, bestIdx=0
```

**T5: simulateRemoveTarget 乒乓检查阶段**
```
调用:simulateRemoveTarget(ctx, storePool, targetStore=Store4.StoreID, ...)

执行流程:
  1. 临时更新 StorePool:
     - storePool.UpdateLocalStoreAfterRebalance(Store4.StoreID, rangeUsageInfo={QPS: 10}, ADD_VOTER)
     - Store4.RangeCount: 80 → 81
     - Store4.QPS: 800 → 810

  2. 调用 RemoveTarget([Store1, Store2, Store3, Store4]):
     - 候选移除:
       - Store1: RangeCount=120, balanceScore=overfull, diversityRemovalScore=average(0.5, 1.0) = 0.75
       - Store2: RangeCount=105, balanceScore=aroundTheMean, diversityRemovalScore=average(0.5, 1.0) = 0.75
       - Store3: RangeCount=95, balanceScore=aroundTheMean, diversityRemovalScore=average(1.0, 1.0) = 1.0
       - Store4: RangeCount=81, balanceScore=underfull, diversityRemovalScore=average(1.0, 1.0, 0.5) = 0.833
     - worst() 筛选:
       - Store1 和 Store2 的 diversityRemovalScore 最低(0.75)
       - worst = [Store1, Store2]
     - 局部计算 convergesScore 和 balanceScore:
       - removalCandidateStoreList.Mean = (120 + 105) / 2 = 112.5
       - Store1: convergesScore=0(移除后收敛), balanceScore=overfull, rangeCount=120
       - Store2: convergesScore=1(移除后不收敛), balanceScore=aroundTheMean, rangeCount=105
     - selectWorst:选择 Store1(balanceScore 更差)

  3. 检查乒乓:
     - removeReplica.StoreID = Store1.StoreID
     - target.StoreID = Store4.StoreID
     - Store1 != Store4 → 无乒乓效应

  4. 恢复 StorePool(defer 执行):
     - storePool.UpdateLocalStoreAfterRebalance(Store4.StoreID, rangeUsageInfo, REMOVE_VOTER)
     - Store4.RangeCount: 81 → 80
     - Store4.QPS: 810 → 800

  5. 返回:removeReplica=Store1
```

**T6: 最终结果**
```
返回:
  - addTarget: {NodeID: Store4.NodeID, StoreID: Store4.StoreID}
  - removeTarget: {NodeID: Store1.NodeID, StoreID: Store1.StoreID}
  - details: "{"Target":"s4, valid:true, diversity:0.83, converges:1, balance:1, rangeCount:80","Existing":"s1, valid:true, diversity:0.83, converges:0, balance:-1, rangeCount:120"}"
  - ok: true

后续操作:
  - ReplicateQueue 发起 ChangeReplicas RPC 到 Raft leader
  - Raft leader 执行 AddReplica(Store4) + RemoveReplica(Store1)
  - 使用 Joint Consensus 原子地完成副本变更
  - Range R2 的副本列表更新为:[Store2, Store3, Store4]
```

**关键时间点**:
- T0-T1: 决策阶段(约 1-2ms)
- T1-T2: 等价类构建(约 5-10ms,需要计算多个 diversity score)
- T2-T3: 打分计算(约 3-5ms)
- T3-T4: 选择与 MMA 检查(约 2-3ms)
- T4-T5: 模拟移除检查(约 5-10ms,需要再次调用 RemoveTarget)
- T5-T6: Raft 同步(约 100-500ms)

### 2.3 示例3:IO Overload 驱动的 Lease 转移

**初始场景**:
```
集群状态:
  - Store1: region=us-east, zone=us-east-1a, RangeCount=100, QPS=1000, IOScore=0.45, DiskUnhealthy=false
  - Store2: region=us-east, zone=us-east-1b, RangeCount=100, QPS=1000, IOScore=0.25, DiskUnhealthy=false
  - Store3: region=us-west, zone=us-west-2a, RangeCount=100, QPS=1000, IOScore=0.20, DiskUnhealthy=false
  - 集群均值:IOScore=0.30

IO Overload 配置:
  - LeaseEnforcementLevel = IOOverloadThresholdShed
  - LeaseIOOverloadShedThreshold = 0.40
  - IOOverloadMeanShedThreshold = 1.75

Range R3 状态:
  - 当前副本:[Store1(Leaseholder), Store2, Store3]
  - Lease 在 Store1
```

**T0: LeaseQueue 检查阶段**
```
调用:leaseholderShouldMoveDueToIOOverload(ctx, storePool, existingReplicas=[Store1, Store2, Store3], leaseStoreID=Store1.StoreID, ioOverloadOptions)

执行流程:
  1. 获取 StoreList:
     - sl = storePool.GetStoreListFromIDs([Store1, Store2, Store3], StoreFilterSuspect)
     - 返回:[Store1, Store2, Store3]

  2. 检查当前 leaseholder(Store1):
     - store = Store1
     - ioOverloadOptions.ExistingLeaseCheck(ctx, Store1, sl)
       - score = Store1.IOScore = 0.45
       - avg = sl.CandidateIOOverloadScores.Mean = (0.45 + 0.25 + 0.20) / 3 = 0.30
       - diskUnhealthyScore = 0(DiskUnhealthy=false)
       - absThreshold = 0.40(LeaseIOOverloadShedThreshold)
       - meanThreshold = 1.75(IOOverloadMeanShedThreshold)
       - enforcement = IOOverloadThresholdShed
       - disallowed = [IOOverloadThresholdShed]

       - ioOverloadCheck(0.45, 0.30, 0, 0.40, 1.75, IOOverloadThresholdShed, [IOOverloadThresholdShed]):
         - absCheck = 0.45 < 0.40 → false
         - meanCheck = 0.45 < 0.30 * 1.75 = 0.525 → true
         - diskCheck = true
         - (false || true) && true → true → 通过基本检查
         - 检查 enforcement:IOOverloadThresholdShed in [IOOverloadThresholdShed] → 不通过
         - 返回:(false, "io overload 0.45 exceeds threshold 0.40, above average: 0.30, enforcement 3")

  3. 返回:true(应该转移 lease)
```

**T1: TransferLeaseTarget 执行阶段**
```
调用:TransferLeaseTarget(ctx, storePool, desc, conf, existing=[Store1, Store2, Store3], leaseRepl=Store1, usageInfo, opts={ExcludeLeaseRepl: true})

执行流程:
  1. 检查是否需要强制排除当前 leaseholder:
     - leaseholderShouldMoveDueToIOOverload(...) → true
     - 设置:excludeLeaseRepl = true

  2. 调用 ValidLeaseTargets:
     - 输入:existing=[Store1, Store2, Store3], excludeLeaseRepl=true
     - 过滤掉 Store1(当前 leaseholder)
     - 候选:[Store2, Store3]

  3. 调用 nonIOOverloadedLeaseTargets:
     - 对于 Store2:
       - IOScore = 0.25 < 0.30(LeaseIOOverloadThreshold) → 通过
       - 加入候选列表
     - 对于 Store3:
       - IOScore = 0.20 < 0.30 → 通过
       - 加入候选列表
     - 返回:[Store2, Store3]
```

**T2: 选择最佳 Lease 目标**
```
(实际实现中,TransferLeaseTarget 会使用额外的逻辑选择最佳候选,如 follow-the-workload、latency 等,这里简化处理)

假设选择 Store3(IOScore 最低):
  - target = Store3
```

**T3: 最终结果**
```
返回:
  - target: Store3.ReplicaDescriptor

后续操作:
  - LeaseQueue 发起 TransferLease RPC 到 Store1
  - Store1 执行 TransferLease(Store3)
  - Store3 成为新的 leaseholder
  - Range R3 的 lease 转移完成
```

**关键时间点**:
- T0-T1: IO Overload 检查(约 1-2ms,访问 StorePool 缓存)
- T1-T2: 候选筛选(约 1-2ms)
- T2-T3: Lease 转移(约 10-50ms,Raft 日志同步)

**效果**:
```
转移前:
  - Store1: IOScore=0.45(过载), 负责 lease 处理请求
  - Store2: IOScore=0.25(正常)
  - Store3: IOScore=0.20(空闲)

转移后:
  - Store1: IOScore 逐渐降低(不再处理 lease 请求)
  - Store3: IOScore 逐渐升高(开始处理 lease 请求)
  - 集群 IO 更均衡
```

---

## 三、工程权衡分析

### 3.1 权衡1:Power of Two Random Choices vs 完全随机 vs 全局最优

**三种方案对比**:

| 方案              | 算法描述                         | 优点                                  | 缺点                                    |
|-------------------|----------------------------------|---------------------------------------|-----------------------------------------|
| 完全随机          | 从候选列表中随机选一个           | 实现简单,避免热点                     | 负载不均衡,收敛慢                       |
| Power of Two      | 随机选2个,取更好的               | 负载均衡好,避免热点,实现简单          | 仍有一定随机性,非全局最优               |
| 全局最优          | 选择得分最高的候选               | 负载最均衡                            | 热点风险,多节点同时选中同一个"最优"Store |

**数学分析**:

假设集群有 N 个 Store,当前负载分布为 $L_1, L_2, ..., L_N$,期望添加 M 个新副本。

**完全随机**:
- 每个 Store 被选中的概率:$P_i = \frac{1}{N}$
- M 次选择后,Store $i$ 的期望负载:$E[L_i'] = L_i + M \cdot \frac{1}{N}$
- 方差:$Var[L_i'] = M \cdot \frac{1}{N} \cdot (1 - \frac{1}{N})$
- 收敛速度:$O(N \log N)$(根据 Balls and Bins 问题)

**Power of Two**:
- 每个 Store 被选中的概率:$P_i = P(i \text{ is chosen from 2 samples})$
- 收敛速度:$O(\log \log N)$(显著优于完全随机)
- 负载最大值:$E[\text{max } L_i] = O(\log \log N)$(vs 完全随机的 $O(\log N)$)

**全局最优**:
- 如果所有节点都选择当前负载最低的 Store,会导致"雷鸣群集"(thundering herd)
- 示例:
  ```
  初始状态:Store1=100, Store2=101, Store3=102
  10 个节点同时决策,都选择 Store1(最优)
  结果:Store1=110, Store2=101, Store3=102 → 负载反转
  ```

**CockroachDB 的选择:Power of Two**

**理由**:
1. **实证研究支持**:Michael Mitzenmacher 的论文证明 Power of Two 在负载均衡和随机性之间达到最佳平衡
2. **避免热点**:相比全局最优,不会导致所有节点同时选中同一个 Store
3. **收敛性**:相比完全随机,$O(\log \log N)$ vs $O(\log N)$ 的负载最大值
4. **实现简单**:只需在候选列表中随机选 2 个,无需全局协调

**具体实现**:
```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1133-1151
func (cl candidateList) selectBest(randGen allocatorRand) *candidate {
    cl = cl.best()  // 先筛选出所有"最佳"候选
    if len(cl) == 0 { return nil }
    if len(cl) == 1 { return &cl[0] }

    randGen.Lock()
    order := randGen.Perm(len(cl))  // 随机排列
    randGen.Unlock()
    best := &cl[order[0]]
    for i := 1; i < allocatorRandomCount; i++ {  // allocatorRandomCount = 2
        if best.less(cl[order[i]]) {
            best = &cl[order[i]]
        }
    }
    return best
}
```

### 3.2 权衡2:Local StorePool Cache vs 全局一致性快照

**两种方案对比**:

| 方案                | 数据来源               | 一致性         | 实时性        | 性能          |
|---------------------|------------------------|----------------|---------------|---------------|
| Local Cache(当前)  | Gossip + 本地缓存      | 最终一致       | 10s 延迟      | 高(无网络)    |
| 全局快照            | 分布式快照算法         | 强一致         | 实时          | 低(需协调)    |

**Local Cache 方案**:
```
每个节点维护自己的 StorePool 缓存
  ↓
每 10 秒通过 Gossip 接收其他 Store 的 StoreDescriptor 更新
  ↓
基于本地缓存的数据进行决策
  ↓
可能基于"过时"的数据决策(例如:Store1 的 RangeCount 可能已过时)
```

**全局快照方案**:
```
发起决策前,先获取全局一致性快照
  ↓
所有节点协调,生成 consistent snapshot
  ↓
基于快照数据进行决策
  ↓
保证决策基于一致的全局视图
```

**CockroachDB 的选择:Local Cache**

**理由**:
1. **可用性优先**:Allocator 是后台任务,可以容忍一定的不一致,但不能容忍阻塞
2. **性能考虑**:获取全局快照需要跨节点协调,延迟高(可能 100ms+),而 Local Cache 访问只需 1-2ms
3. **Gossip 的最终一致性**:10 秒的延迟在实际场景中可接受,因为 Range 的状态变化通常较慢
4. **自我修复**:即使基于过时数据做出错误决策,下一次决策周期(通常 1 分钟后)会自动修正

**潜在问题与缓解**:
```
问题1:多个节点同时基于过时数据决策,导致重复操作
  缓解:ReplicateQueue 有内置的并发控制,每个 Range 同时只有一个操作

问题2:某个 Store 的 RangeCount 快速增长,但 Gossip 延迟导致其他节点仍认为它"空闲"
  缓解:
    - Gossip 延迟通常只有 1-2 个周期(10-20s)
    - Allocator 有重试机制,错误决策会在下一轮修正
    - 使用 throttling 机制限制单个 Store 的快照接收速率

问题3:网络分区导致 StorePool 长期过时
  缓解:
    - StorePool 有 TTL 机制,超过一定时间未更新的 Store 标记为 "suspect"
    - Suspect Store 不参与 allocation/rebalance 决策
```

**数据新鲜度分析**:
```
T0: Store1 完成一个 Range 的添加,RangeCount: 100 → 101
T0+1s: Store1 构造新的 StoreDescriptor
T0+2s: Store1 通过 Gossip 广播 StoreDescriptor
T0+10s: 其他节点接收到 StoreDescriptor,更新 StorePool
T0+10s: Allocator 基于最新数据进行下一次决策

结果:10 秒的延迟窗口内,其他节点可能基于 RangeCount=100 的数据决策
```

### 3.3 权衡3:单一维度优化(Range Count)vs 多维度优化(MMA)

**两种方案对比**:

| 方案                  | 优化目标              | 实现复杂度 | 收敛速度      | 适用场景              |
|-----------------------|-----------------------|------------|---------------|-----------------------|
| 单一维度(RangeCount)  | 只均衡 Range 数量     | 低         | 快            | 同构集群,负载均匀     |
| 多维度(MMA)           | 同时均衡 Range/QPS/CPU| 高         | 慢            | 异构集群,负载不均     |

**单一维度优化**:
```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:497-533
func (o RangeCountScorerOptions) shouldRebalanceBasedOnThresholds(...) bool {
    store := eqClass.existing
    sl := eqClass.candidateSL
    overfullThreshold := int32(math.Ceil(overfullRangeThreshold(&o, sl.CandidateRanges.Mean)))
    if store.Capacity.RangeCount > overfullThreshold {
        return true  // 只检查 RangeCount
    }
    // ...
}
```

**问题**:
```
场景:异构集群
  - Store1: 配置为处理 70% 负载,RangeCount=70, QPS=7000
  - Store2: 配置为处理 30% 负载,RangeCount=30, QPS=3000
  - 均值:RangeCount=50

单一维度决策:
  - Store1.RangeCount=70 > 均值 50 → 判断为 overfull,触发 rebalance
  - 将 Range 从 Store1 转移到 Store2
  - 结果:Store2.QPS 过载(配置只能处理 30% 负载,现在处理更多)

根本问题:RangeCount 不能准确反映负载,因为不同 Range 的 QPS 差异巨大
```

**多维度优化(MMA)**:
```go
// pkg/kv/kvserver/mmaintegration/allocator_sync.go
func (as *AllocatorSync) IsInConflictWithMMA(
    ctx context.Context,
    targetStoreID roachpb.StoreID,
    advisor *mmaprototype.MMARebalanceAdvisor,
    log bool,
) bool {
    meansLoad := advisor.GetMeansLoad()
    // 同时检查 QPS 和 CPU
    for _, dim := range []load.Dimension{load.Queries, load.CPU} {
        targetLoad := targetStore.Load().Dim(dim)
        meanLoad := meansLoad.Dim(dim)
        if targetLoad > meanLoad * mmaConflictThreshold {  // 1.2
            return true  // 冲突,拒绝 rebalance
        }
    }
    return false
}
```

**CockroachDB 的选择:混合方案**

1. **默认使用 RangeCountScorerOptions**(ReplicateQueue):
   - 适用于大多数同构集群
   - 实现简单,收敛快
   - 计算开销低

2. **可选启用 MMA**(通过 cluster setting):
   - 检测到多维度不均衡时,MMA 可以拒绝 RangeCount 优化建议
   - 适用于异构集群或负载热点明显的场景
   - 计算开销高(需要维护多维度统计)

**混合逻辑**:
```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1933-1951
if !existingCandidate.isCriticalRebalance(target) {
    // 非关键 rebalance 需要通过 MMA 检查
    if advisor := results[bestIdx].advisor; advisor != nil {
        if a.as.IsInConflictWithMMA(ctx, target.store.StoreID, advisor, false) {
            continue  // MMA 拒绝,尝试下一个候选
        }
    }
}
```

**设计精妙之处**:
- **Critical Rebalance 跳过 MMA**:约束修复、磁盘满、分散性改善等关键操作不受 MMA 限制,确保系统可用性优先
- **MMA 作为"否决权"**:MMA 不主动提出 rebalance,只对 RangeCountScorer 的建议进行审查,避免过度干预

### 3.4 权衡4:Best Candidate vs Good Candidate(Recovery 场景)

**两种选择器对比**:

| 选择器               | 使用场景       | 选择策略            | 优先级                        | 恢复速度 |
|----------------------|----------------|---------------------|-------------------------------|----------|
| BestCandidateSelector| Uprelication   | Power of Two        | 追求最优(diversity/balance)   | 慢       |
| GoodCandidateSelector| Recovery       | Uniform Random      | 只要满足约束即可              | 快       |

**场景分析**:

**场景1:正常 Uprelication(Replica 状态 Alive)**
```
Range R1:当前 2 个副本,期望 3 个
  - 原因:配置变更(num_replicas: 2 → 3)
  - 状态:所有副本健康,Raft quorum 正常
  - 目标:找到最优位置放置第 3 个副本,优化分散性和负载均衡

选择器:BestCandidateSelector
  - 理由:有足够时间选择最优候选,不着急
  - 流程:
    1. rankedCandidateListForAllocation 生成候选列表,按 diversity/balance/rangeCount 排序
    2. best() 筛选出所有得分最高的候选(可能多个)
    3. Power of Two:从 best() 中随机选 2 个,取更好的
  - 结果:副本放置在最优位置,长期系统更稳定
```

**场景2:故障恢复(Replica 状态 Dead/Decommissioning)**
```
Range R2:当前 2 个副本(1 个 Dead),期望 3 个
  - 原因:Store1 故障,副本丢失
  - 状态:quorum=2,当前只有 2 个副本,处于"脆弱"状态
  - 风险:如果再失去 1 个副本,Range 将不可用
  - 目标:尽快恢复到 3 个副本,恢复 quorum 安全余量

选择器:GoodCandidateSelector
  - 理由:需要快速恢复副本数,避免 Range 不可用时间过长
  - 流程:
    1. rankedCandidateListForAllocation 生成候选列表
    2. good() 筛选出所有满足约束和分散性的候选(可能比 best() 多)
    3. Uniform Random:从 good() 中随机选 1 个
  - 结果:恢复速度快,即使不是最优位置也可接受

差异示例:
  候选列表:
    - Store3: diversity=1.0, balance=underfull, rangeCount=90 (best)
    - Store4: diversity=1.0, balance=aroundTheMean, rangeCount=100 (good but not best)
    - Store5: diversity=1.0, balance=overfull, rangeCount=110 (good but not best)

  BestCandidateSelector:
    - best() = [Store3]
    - 结果:必然选择 Store3

  GoodCandidateSelector:
    - good() = [Store3, Store4, Store5](diversity 相同,都是 good)
    - 结果:1/3 概率选择每个候选,恢复更快(无需等待"最优"候选可用)
```

**CockroachDB 的选择:动态切换**

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1333-1338
var selector CandidateSelector
if replicaStatus == Alive || recoveryStoreSelector.Get(&a.st.SV) == "best" {
    selector = a.NewBestCandidateSelector()
} else {
    selector = a.NewGoodCandidateSelector()
}
```

**设计意义**:
1. **可用性优先**:故障恢复场景下,快速恢复副本数比追求最优位置更重要
2. **灵活性**:通过 `recoveryStoreSelector` cluster setting 可以覆盖默认行为
3. **风险降低**:避免因"等待最优候选"而导致 Range 长时间处于脆弱状态

### 3.5 权衡5:模拟移除检查(Simulate Remove)vs 直接执行

**两种方案对比**:

| 方案                  | 检查方式                | 优点                          | 缺点                      |
|-----------------------|-------------------------|-------------------------------|---------------------------|
| 模拟移除(当前)        | 先模拟,检查无乒乓再执行 | 避免无意义的副本移动          | 计算开销高(需两次决策)    |
| 直接执行              | 直接执行 rebalance      | 计算开销低,实现简单           | 可能导致乒乓效应,浪费资源 |

**乒乓效应示例**:
```
初始状态:
  - Range R3:[Store1, Store2, Store3]
  - Store1: RangeCount=101
  - Store2: RangeCount=100
  - Store3: RangeCount=99
  - 均值:100

T0: Allocator 扫描 Range R3
  - 判断:Store1.RangeCount=101 略高于均值
  - 决策:rebalance Store1 → Store3
  - 执行:添加 Store3 副本,移除 Store1 副本
  - 结果:[Store2, Store3, Store4]

T1(1 分钟后): Allocator 再次扫描 Range R3
  - 当前状态:
    - Store2: RangeCount=100
    - Store3: RangeCount=100
    - Store4: RangeCount=99(刚添加了 R3)
  - 判断:Store4 现在是 RangeCount 最低的
  - 模拟:如果添加 Store1 副本,应该移除哪个?
    - 添加 Store1(RangeCount=100)
    - 候选移除:[Store2=100, Store3=100, Store4=100, Store1=100]
    - 结果:随机选一个(假设选中 Store4)
  - 检查乒乓:removeReplica == target? → Store4 == Store1? → false
  - 决策:rebalance Store4 → Store1(反向移动!)
  - 结果:陷入乒乓循环

如果没有模拟移除检查:
  - T0 和 T1 的操作都会执行
  - 结果:浪费网络带宽和 Raft 日志空间,副本不断在 Store1 和 Store4 之间移动
```

**CockroachDB 的选择:模拟移除检查**

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1997-2021
removeReplica, removeDetails, err = a.simulateRemoveTarget(
    ctx, storePool, target.store.StoreID, conf,
    replicaCandidates, existingVoters, otherReplicaSet,
    sl, rangeUsageInfo, targetType, options,
)
if err != nil {
    log.KvDistribution.Warningf(ctx, "simulating removal of %s failed: %+v", targetType, err)
    return zero, zero, "", false
}
if target.store.StoreID != removeReplica.StoreID {
    break  // 无乒乓效应,允许 rebalance
}

log.KvDistribution.VEventf(ctx, 2, "not rebalancing to s%d because we'd immediately remove it: %s",
    target.store.StoreID, removeDetails)
// 继续尝试下一个候选
```

**实现细节**:
```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator.go:1594-1648
func (a Allocator) simulateRemoveTarget(...) {
    // 步骤1:临时修改 StorePool 的统计
    storePool.UpdateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.ADD_VOTER)
    defer storePool.UpdateLocalStoreAfterRebalance(targetStore, rangeUsageInfo, roachpb.REMOVE_VOTER)

    // 步骤2:基于修改后的统计调用 RemoveTarget
    return a.RemoveTarget(
        ctx, storePool, conf, storepool.MakeStoreList(candidateStores),
        existingVoters, existingNonVoters, VoterTarget, options,
    )
}
```

**计算开销分析**:
```
正常 rebalance 流程:
  1. rankedCandidateListForRebalancing: O(N * M)(N=Store数, M=现有副本数)
  2. bestRebalanceTarget: O(M)
  3. simulateRemoveTarget:
     - UpdateLocalStoreAfterRebalance: O(1)
     - RemoveTarget: O(M)(再次扫描副本列表)
  4. 总计:O(N * M + M) ≈ O(N * M)

额外开销:
  - simulateRemoveTarget 增加约 20-30% 的计算开销(需再次调用 RemoveTarget)
  - 但避免了无效的 Raft 操作(snapshot 传输、log 同步),这些操作开销远大于计算

权衡:
  - 计算开销:+20-30%
  - 避免的 Raft 开销:可能节省 100-500ms 的网络传输和磁盘 IO
  - 结论:值得,特别是在高负载集群中
```

**设计意义**:
1. **稳定性优先**:避免频繁无意义的副本移动,减少系统抖动
2. **资源节约**:Raft 操作的开销远高于额外的计算开销
3. **可预测性**:用户不会看到副本在相同 Store 之间反复移动

---

## 四、总结与心智模型

### 4.1 Allocator 的完整决策链路

```
┌─────────────────────────────────────────────────────────────────┐
│                     决策触发(ReplicateQueue)                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 第一层:决策类型判断(ComputeAction)                               │
│   - 检查副本数是否正确(Add/Remove)                               │
│   - 检查是否有 Dead/Decommissioning 副本(Replace)                │
│   - 检查是否需要 Rebalance(ConsiderRebalance)                    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 第二层:候选过滤(rankedCandidateListFor...)                       │
│   - 过滤器链:Disk/IO/Constraints/ExistingReplicas/NodeAlive     │
│   - 构建等价类(Rebalance 场景)                                   │
│   - 生成候选列表(valid Store 的子集)                             │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 第三层:多维度打分                                                │
│   - Constraints(valid/necessary)                                 │
│   - Diversity(diversityScore)                                    │
│   - IO Overload(ioOverloaded/ioOverloadScore)                    │
│   - Load Balance(convergesScore/balanceScore/rangeCount)         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 第四层:候选排序与选择                                            │
│   - 按优先级排序(valid > fullDisk > necessary > diversity > ...) │
│   - CandidateSelector 策略(Best vs Good)                         │
│   - Power of Two Random Choices(避免热点)                        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 第五层:冲突检查(Rebalance 场景)                                  │
│   - MMA 多维度负载检查(IsInConflictWithMMA)                      │
│   - 模拟移除检查(simulateRemoveTarget)                           │
│   - 防止乒乓效应                                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 第六层:执行(Raft Conf Change)                                    │
│   - 构造 ChangeReplicas 请求                                     │
│   - 通过 Raft 日志同步到所有副本                                 │
│   - Joint Consensus(原子性副本变更)                              │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 8 大设计模式总结

| 设计模式     | 应用位置                  | 核心作用                                  | 关键优势                      |
|--------------|---------------------------|-------------------------------------------|-------------------------------|
| 策略模式     | CandidateSelector         | 算法可替换(Best vs Good)                  | 适应不同场景,易扩展           |
| 模板方法模式 | ScorerOptions             | 算法骨架固定,实现细节可变                 | 代码复用,统一接口             |
| 建造者模式   | AnalyzedConstraints       | 复杂对象分步构建                          | 封装构建逻辑,保证一致性       |
| 责任链模式   | 候选过滤链                | 多个过滤器依次检查                        | 解耦过滤逻辑,易于扩展         |
| 观察者模式   | Gossip → StorePool        | 状态变化自动通知                          | 解耦发布者和订阅者,实时更新   |
| 状态模式     | IOOverloadEnforcementLevel| 状态决定行为                              | 行为封装,易于扩展新状态       |
| 组合模式     | Constraints 层级结构      | 树形结构统一处理                          | 单个约束和组合约束统一接口    |
| 备忘录模式   | UpdateLocalStoreAfterRebalance | 状态保存与恢复                     | 事务性操作,状态隔离           |

### 4.3 5 组关键工程权衡总结

| 权衡问题                       | CockroachDB 的选择        | 核心理由                                  |
|--------------------------------|---------------------------|-------------------------------------------|
| 选择策略                       | Power of Two              | 平衡负载均衡和热点避免,$O(\log \log N)$ 收敛 |
| 数据一致性                     | Local Cache(最终一致)     | 可用性优先,10s 延迟可接受                 |
| 优化维度                       | 单维度 + MMA 审查         | 默认简单,异构场景启用 MMA                 |
| 恢复策略                       | 动态切换(Best vs Good)    | 故障恢复优先速度,正常场景追求最优         |
| 乒乓预防                       | 模拟移除检查              | 避免无效操作,节省 Raft 开销               |

### 4.4 完整心智模型:Allocator 的"三层决策框架"

```
┌─────────────────────────────────────────────────────────────────┐
│ 第一层:硬性约束(Must Satisfy)                                    │
│   - Zone Config Constraints(用户配置的约束)                      │
│   - Quorum Requirements(Raft 协议的多数派要求)                   │
│   - Disk Capacity(磁盘空间)                                      │
│   - Node Liveness(节点存活)                                      │
│ 决策原则:违反硬性约束的操作直接拒绝                              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 第二层:软性优化(Should Optimize)                                 │
│   - Diversity(容错能力)                                          │
│   - IO Overload(避免过载 Store)                                  │
│   - Load Balance(负载均衡)                                        │
│ 决策原则:在满足硬性约束的前提下,优先优化软性目标                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 第三层:随机选择(Break Tie)                                       │
│   - Power of Two Random Choices(避免热点)                        │
│   - Uniform Random(快速恢复场景)                                 │
│ 决策原则:在多个同等优秀的候选中,随机选择一个                    │
└─────────────────────────────────────────────────────────────────┘
```

**使用指南**:
1. **理解优先级**:硬性约束 > 软性优化 > 随机选择,这是 Allocator 的决策顺序
2. **识别场景**:不同场景(Allocation/Removal/Rebalance)使用不同的算法和权衡
3. **容忍不完美**:Allocator 基于 Local Cache 和最终一致性,可能做出非全局最优决策,但会在后续周期自动修正
4. **关注 Critical**:Critical Rebalance(约束修复、磁盘满、分散性改善)始终优先于负载均衡
5. **理解权衡**:Power of Two、Local Cache、MMA 审查、模拟移除等设计都是在性能、一致性、可用性之间的权衡

---

## 五、与前两章的关联

**第一章**:分析了 `ComputeAction` 如何确定"需要做什么"(Add/Remove/Replace/Rebalance)
**第二章**:分析了"如何选择目标 Store"(候选选择、打分、排序)
**第三章**:揭示了"为什么这样设计"(设计模式、具体示例、工程权衡)

**完整链路**:
```
ComputeAction(第一章)
  → 确定操作类型(Add/Remove/Rebalance)
  ↓
rankedCandidateListFor...(第二章)
  → 筛选候选、多维度打分、排序
  ↓
CandidateSelector(第三章)
  → 应用 Power of Two 策略选择最终目标
  ↓
MMA Check + Simulate Remove(第三章)
  → 冲突检查、乒乓预防
  ↓
Raft Conf Change
  → 执行副本变更
```

**设计哲学**:
- **分层决策**:将复杂的副本放置问题分解为多个独立的层次(ComputeAction → Filter → Score → Select → Check)
- **策略模式**:每一层都可以独立替换实现,不影响其他层
- **最终一致性**:容忍短期的非最优决策,通过周期性重试达到长期最优
- **可用性优先**:在一致性、性能、可用性的 CAP 权衡中,Allocator 优先保证可用性

**至此,Allocator 决策引擎的完整分析结束。三章合计超过 90,000 字,涵盖了从宏观架构到微观实现的所有细节,希望能够帮助读者构建对 CockroachDB 副本放置机制的深层次理解。**
