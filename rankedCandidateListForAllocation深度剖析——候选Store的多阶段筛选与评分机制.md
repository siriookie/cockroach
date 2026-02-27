# rankedCandidateListForAllocation 深度剖析——候选 Store 的多阶段筛选与评分机制

> **核心问题**: `rankedCandidateListForAllocation` 函数在 Allocator 决策流程中扮演什么角色?它如何从所有可用 Store 中筛选出合格的候选?它使用哪些评分标准来排序这些候选?
>
> **解答路径**: 本节通过深入分析函数的四个执行阶段(有效性过滤、排除过滤、多维度评分、排序)、揭示关键设计决策(为什么要构造 validStoreList、为什么要分离 valid 和 necessary 标志)、提供具体数值示例,帮助读者理解这个关键函数的工作机制。

---

## 一、函数定位与设计目标

### 1.1 在 Allocator 决策流程中的位置

```
AllocateTarget()                      ← 入口:需要新增一个副本
  ↓
确定 targetType(Voter/NonVoter)       ← 决定要添加的副本类型
  ↓
选择 constraintsChecker               ← 基于场景选择约束检查策略
  ↓
rankedCandidateListForAllocation()    ← 【本函数】筛选 + 评分 + 排序候选 Store
  ↓
CandidateSelector.selectOne()         ← 从排序后的候选中随机选一个
  ↓
执行 Raft Conf Change                 ← 实际添加副本
```

**核心职责**:
- **输入**: 所有可能的 Store、约束检查函数、现有副本信息
- **处理**: 四阶段过滤 + 多维度评分 + 排序
- **输出**: 按分数降序排列的候选列表(candidateList)

### 1.2 函数签名分析

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1211-1222
func rankedCandidateListForAllocation(
    ctx context.Context,
    candidateStores storepool.StoreList,              // 所有潜在的候选 Store
    constraintsCheck constraintsCheckFn,              // 约束检查函数(从前一节学习的策略函数)
    existingReplicas []roachpb.ReplicaDescriptor,     // 当前 Range 已有的副本
    nonVoterReplicas []roachpb.ReplicaDescriptor,     // 当前 Range 的 NonVoter 副本
    existingStoreLocalities map[roachpb.StoreID]roachpb.Locality,  // 已有副本的 Locality 信息
    isStoreValidForRoutineReplicaTransfer func(context.Context, roachpb.StoreID) bool,  // Store 是否存活检查
    allowMultipleReplsPerNode bool,                   // 是否允许同一节点多副本(测试用)
    options ScorerOptions,                            // 评分选项(磁盘、IO、负载均衡等)
    targetType TargetReplicaType,                     // 目标类型(Voter/NonVoter)
) candidateList {
    // ...
}
```

**参数设计哲学**:
- **分离关注点**: `candidateStores` 提供数据源,`constraintsCheck` 提供策略,`options` 提供配置
- **依赖注入**: `isStoreValidForRoutineReplicaTransfer` 作为函数参数注入,便于测试
- **上下文传递**: 现有副本信息(`existingReplicas`, `nonVoterReplicas`, `existingStoreLocalities`)提供决策依据

---

## 二、四阶段执行流程深度解析

### 2.1 阶段一:有效性过滤(Lines 1227-1249)

**目标**: 筛选出满足基本约束且磁盘/IO 不过载的 Store。

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1231-1245
validCandidateStores := []roachpb.StoreDescriptor{}
for _, s := range candidateStores.Stores {
    // 1. 检查磁盘是否满
    if !options.getDiskOptions().maxCapacityCheck(s) ||
        // 2. 检查 IO 是否过载
        !options.getIOOverloadOptions().allocateReplicaToCheck(
            ctx,
            s,
            candidateStores,
        ) {
        continue
    }

    // 3. 检查是否满足约束
    if constraintsOK, _ := constraintsCheck(s); constraintsOK {
        validCandidateStores = append(validCandidateStores, s)
    }
}
```

**关键设计决策**:

**为什么先过滤出 validCandidateStores?**
```
原因:避免用无效 Store 污染统计平均值
问题场景:
  - 集群有 10 个 Store,其中 3 个磁盘满(99%)
  - 如果不过滤,计算 balanceScore 时平均 Range Count 会被这 3 个 Store 拉低
  - 导致所有 Store 的 balanceScore 都偏高,无法正确识别真正的不均衡
解决方案:
  - 先过滤出有效 Store,然后基于这些 Store 重新计算平均值
  - validStoreList.Average() 只统计有效 Store 的平均值
```

**具体检查项**:

| 检查项 | 函数 | 失败条件 | 影响 |
|--------|------|----------|------|
| 磁盘容量 | `maxCapacityCheck(s)` | `s.Capacity.FractionUsed() > 0.95` | Store 将被排除 |
| IO 过载 | `allocateReplicaToCheck(ctx, s, candidateStores)` | IO Overload Score 超过阈值 | Store 将被排除 |
| 约束满足 | `constraintsCheck(s)` 返回 `constraintsOK=false` | Store 不满足 Zone Config 约束 | Store 将被排除 |

**示例**:

```
假设集群有 5 个 Store:
Store ID | Disk Used | IO Overload | Constraints OK | 是否通过阶段一
---------|-----------|-------------|----------------|------------------
s1       | 60%       | false       | true           | ✓ 通过
s2       | 98%       | false       | true           | ✗ 磁盘满
s3       | 50%       | true        | true           | ✗ IO 过载
s4       | 70%       | false       | false          | ✗ 不满足约束
s5       | 55%       | false       | true           | ✓ 通过

结果:validCandidateStores = [s1, s5]
```

### 2.2 阶段二:构造 validStoreList(Lines 1247-1249)

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1247-1249
// Create a new store list, which will update the average for each stat to
// only be the average value of valid candidates.
validStoreList := storepool.MakeStoreList(validCandidateStores)
```

**为什么要构造新的 StoreList?**

`storepool.StoreList` 不仅仅存储 Store 列表,还会自动计算统计信息:

```go
// pkg/kv/kvserver/allocator/storepool/store_list.go (简化版)
type StoreList struct {
    Stores []roachpb.StoreDescriptor

    // 自动计算的统计信息
    candidateRanges      candidateRanges    // Range Count 统计
    candidateLogicalBytes candidateBytes    // Logical Bytes 统计
    candidateQueriesPerSecond candidateQPS  // QPS 统计
    candidateL0SubLevelCount candidateL0    // LSM L0 统计
    // ...
}

type candidateRanges struct {
    mean  float64  // 平均 Range Count
    count int      // Store 数量
}
```

**对比原始 candidateStores 与新构造的 validStoreList**:

```
原始 candidateStores (所有 Store):
  s1: RangeCount=100
  s2: RangeCount=200 (磁盘满,无效)
  s3: RangeCount=150 (IO 过载,无效)
  s4: RangeCount=80  (不满足约束,无效)
  s5: RangeCount=110

  平均 RangeCount = (100 + 200 + 150 + 80 + 110) / 5 = 128

validStoreList (仅有效 Store):
  s1: RangeCount=100
  s5: RangeCount=110

  平均 RangeCount = (100 + 110) / 2 = 105  ← 更准确的基准
```

**为什么这很重要?**

后续的 `balanceScore` 计算依赖于平均值:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1278
balanceScore := options.balanceScore(validStoreList, s.Capacity)
```

`balanceScore` 会比较 Store 的 RangeCount 与平均值:
- 如果使用原始平均值 128,s1(100)会被认为"负载偏低",得分高
- 如果使用有效平均值 105,s1(100)接近平均,得分中等

**正确的基准很关键,否则会导致错误的负载均衡决策。**

### 2.3 阶段三:排除已有副本(Lines 1251-1275)

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1251-1275
for _, s := range validStoreList.Stores {
    // 1. 排除已有副本的 Store
    if StoreHasReplica(s.StoreID, existingReplTargets) {
        continue
    }

    // 2. 排除已有副本的 Node(除非允许同 Node 多副本)
    if !allowMultipleReplsPerNode && nodeHasReplica(s.Node.NodeID, existingReplTargets) {
        continue
    }

    // 3. 再次检查约束(这次记录 necessary 标志)
    constraintsOK, necessary := constraintsCheck(s)

    // 4. 检查 Store 是否在存活的 Node 上
    if !isStoreValidForRoutineReplicaTransfer(ctx, s.StoreID) {
        log.KvDistribution.VEventf(
            ctx,
            3,
            "not considering store s%d as a potential rebalance candidate because it is on a non-live node n%d",
            s.StoreID,
            s.Node.NodeID,
        )
        continue
    }

    // 通过所有检查,进入评分阶段
    // ...
}
```

**为什么要再次调用 constraintsCheck?**

```
第一次调用(阶段一):
  constraintsOK, _ := constraintsCheck(s)  // 只关心 constraintsOK
  目的:快速过滤不满足约束的 Store

第二次调用(阶段三):
  constraintsOK, necessary := constraintsCheck(s)  // 关心 necessary 标志
  目的:记录该 Store 是否"必要"满足约束

necessary 标志的含义:
  - 该 Store 满足某个约束,且该约束目前未被充分满足
  - 例如:约束要求"至少 1 个副本在 us-east",当前没有副本在 us-east
  - 如果该 Store 在 us-east,则 necessary=true

necessary 的作用:
  - 在后续排序时,necessary=true 的候选会排在前面
  - 保证约束优先得到满足
```

**示例**:

```
约束配置:
  - Constraint 1: NumReplicas=1, Constraints=[{Type: REQUIRED, Key: "region", Value: "us-east"}]
  - Constraint 2: NumReplicas=1, Constraints=[{Type: REQUIRED, Key: "region", Value: "us-west"}]

当前副本:
  - s1: region=us-west

候选 Store:
  - s2: region=us-east  → constraintsOK=true, necessary=true (满足 Constraint 1,且该约束未满足)
  - s3: region=us-west  → constraintsOK=true, necessary=false (满足 Constraint 2,但该约束已满足)
  - s4: region=us-central → constraintsOK=false, necessary=false (不满足任何约束)

筛选结果:
  - s2 进入候选,且 necessary=true,排序时会优先
  - s3 进入候选,但 necessary=false
  - s4 在阶段一就被过滤掉
```

### 2.4 阶段四:多维度评分(Lines 1277-1295)

**关键代码**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1277-1295
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
```

**评分维度详解**:

#### 2.4.1 Diversity Score(分散性评分)

**计算逻辑**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:2374-2393
func diversityAllocateScore(
    storeLocality roachpb.Locality,
    existingStoreLocalities map[roachpb.StoreID]roachpb.Locality,
) float64 {
    var sumScore float64
    var numSamples int

    // 计算候选 Store 与每个已有副本的 Locality 差异
    for _, locality := range existingStoreLocalities {
        newScore := storeLocality.DiversityScore(locality)
        sumScore += newScore
        numSamples++
    }

    // 如果 Range 还没有副本,任何 Store 都是完美的
    if numSamples == 0 {
        return roachpb.MaxDiversityScore  // 1.0
    }

    // 返回平均差异分数
    return sumScore / float64(numSamples)
}
```

**DiversityScore 的计算规则**:

```go
// pkg/roachpb/locality.go (简化版)
func (l Locality) DiversityScore(other Locality) float64 {
    // 比较两个 Locality 的每一层
    // 层级:region > zone > rack > host

    // 假设 Locality 有 4 层
    // 如果第 1 层就不同(region 不同) → 返回 1.0(最大差异)
    // 如果第 1 层相同,第 2 层不同(zone 不同) → 返回 0.75
    // 如果前 2 层相同,第 3 层不同(rack 不同) → 返回 0.5
    // 如果前 3 层相同,第 4 层不同(host 不同) → 返回 0.25
    // 如果完全相同 → 返回 0.0
}
```

**具体示例**:

```
已有副本:
  s1: region=us-east, zone=us-east-1, rack=r1, host=h1
  s2: region=us-west, zone=us-west-1, rack=r2, host=h2

候选 Store:
  s3: region=us-central, zone=us-central-1, rack=r3, host=h3
  s4: region=us-east, zone=us-east-2, rack=r4, host=h4
  s5: region=us-east, zone=us-east-1, rack=r5, host=h5

计算 diversityScore:

s3 的 diversityScore:
  - s3 vs s1: region 不同 → 1.0
  - s3 vs s2: region 不同 → 1.0
  - 平均: (1.0 + 1.0) / 2 = 1.0  ← 最高分(最分散)

s4 的 diversityScore:
  - s4 vs s1: region 相同,zone 不同 → 0.75
  - s4 vs s2: region 不同 → 1.0
  - 平均: (0.75 + 1.0) / 2 = 0.875

s5 的 diversityScore:
  - s5 vs s1: region 相同,zone 相同,rack 不同 → 0.5
  - s5 vs s2: region 不同 → 1.0
  - 平均: (0.5 + 1.0) / 2 = 0.75  ← 最低分(集中在 us-east-1)

排序: s3 > s4 > s5
```

**设计哲学**:
- **容错优先**: 跨 region 分散 > 跨 zone 分散 > 跨 rack 分散
- **平均差异**: 不是找"最远的",而是找"平均最分散的"
- **避免集中**: 防止多个副本集中在同一故障域

#### 2.4.2 Balance Score(负载均衡评分)

**计算逻辑**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go (简化版)
func (o *rangeCountScorerOptions) balanceScore(
    sl storepool.StoreList,
    sc roachpb.StoreCapacity,
) balanceStatus {
    // 比较该 Store 的 RangeCount 与平均值
    maxRangeCount := int(math.Ceil(sl.candidateRanges.mean * (1 + rangeRebalanceThreshold)))

    if int(sc.RangeCount) < maxRangeCount {
        // 该 Store 的 RangeCount 低于阈值,是好的候选
        if int(sc.RangeCount) < int(math.Floor(sl.candidateRanges.mean)) {
            return balanceAboveMean  // RangeCount 远低于平均,非常好
        }
        return balanceBelowMean  // RangeCount 低于平均,好
    }

    // 该 Store 的 RangeCount 高于阈值,不是好的候选
    return balanceAboveMax
}

// balanceStatus 枚举
const (
    balanceAboveMean  balanceStatus = 2  // 远低于平均,最佳
    balanceBelowMean  balanceStatus = 1  // 低于平均,好
    balanceAboveMax   balanceStatus = 0  // 高于阈值,差
)
```

**具体示例**:

```
validStoreList 统计:
  - 平均 RangeCount: 100
  - rangeRebalanceThreshold: 0.05 (5%)
  - maxRangeCount = ceil(100 * 1.05) = 105

候选 Store 评分:
  s1: RangeCount=90  → 90 < 100(floor(mean)) → balanceAboveMean (2)
  s5: RangeCount=102 → 102 >= 100 但 < 105   → balanceBelowMean (1)
  s6: RangeCount=110 → 110 >= 105            → balanceAboveMax (0)

排序: s1 > s5 > s6
```

**设计哲学**:
- **避免过度均衡**: 不要求完全均等,容忍 ±5% 的偏差
- **防止抖动**: 阈值设计防止副本在接近平均值的 Store 之间反复迁移
- **渐进式改善**: 优先将副本放到远低于平均的 Store,而非略低于平均的 Store

#### 2.4.3 hasNonVoter 标志

**目的**: 优先将 Voter 放到已有 NonVoter 的 Store 上,实现 NonVoter → Voter 的原地升级。

**场景**:

```
当前副本:
  - s1: Voter
  - s2: NonVoter

现在需要添加一个 Voter。

候选 Store:
  - s2: hasNonVoter=true  → 优先选择(可以原地升级 NonVoter → Voter)
  - s3: hasNonVoter=false → 次优选择(需要新建副本)

好处:
  - 节省网络传输(不需要发送 Snapshot)
  - 减少磁盘占用(不需要额外副本)
  - 降低 Raft Log 压力
```

**排序逻辑**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:900-942 (简化版)
func (c candidate) compare(o candidate) int {
    // ...

    // hasNonVoter is better if we're allocating a voter.
    if c.hasNonVoter && !o.hasNonVoter {
        return 1  // c 更好
    }
    if !c.hasNonVoter && o.hasNonVoter {
        return -1  // o 更好
    }

    // ...
}
```

#### 2.4.4 rangeCountScore(归一化的 RangeCount)

**目的**: 将 RangeCount 转换为可比较的分数,用于打破平局。

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go (简化版)
func (o *rangeCountScorerOptions) adjustRangeCountForScoring(rangeCount int) int {
    // 简化版:直接返回 RangeCount
    // 实际实现可能会做归一化处理
    return rangeCount
}
```

**作用**: 当两个候选在 diversityScore、balanceScore 都相同时,使用 rangeCount 打破平局,优先选择 RangeCount 更低的 Store。

### 2.5 阶段五:排序与返回(Lines 1297-1302)

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1297-1302
if options.deterministicForTesting() {
    sort.Sort(sort.Reverse(byScoreAndID(candidates)))
} else {
    sort.Sort(sort.Reverse(byScore(candidates)))
}
return candidates
```

**排序规则**:

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:886-967 (简化版)
func (c candidate) compare(o candidate) int {
    // 1. valid 更好(满足约束)
    if c.valid && !o.valid {
        return 1
    }
    if !c.valid && o.valid {
        return -1
    }

    // 2. !fullDisk 更好(磁盘未满)
    if !c.fullDisk && o.fullDisk {
        return 1
    }
    if c.fullDisk && !o.fullDisk {
        return -1
    }

    // 3. necessary 更好(约束必要性)
    if c.necessary && !o.necessary {
        return 1
    }
    if !c.necessary && o.necessary {
        return -1
    }

    // 4. voterNecessary 更好(Voter 约束必要性)
    if c.voterNecessary && !o.voterNecessary {
        return 1
    }
    if !c.voterNecessary && o.voterNecessary {
        return -1
    }

    // 5. diversityScore 更高更好(分散性)
    if !scoresAlmostEqual(c.diversityScore, o.diversityScore) {
        if c.diversityScore > o.diversityScore {
            return 1
        }
        return -1
    }

    // 6. !ioOverloaded 更好(IO 不过载)
    if !c.ioOverloaded && o.ioOverloaded {
        return 1
    }
    if c.ioOverloaded && o.ioOverloaded {
        return -1
    }

    // 7. convergesScore 更高更好(向均衡收敛)
    if c.convergesScore > o.convergesScore {
        return 1
    }
    if c.convergesScore < o.convergesScore {
        return -1
    }

    // 8. balanceScore 更高更好(负载均衡)
    if c.balanceScore > o.balanceScore {
        return 1
    }
    if c.balanceScore < o.balanceScore {
        return -1
    }

    // 9. hasNonVoter 更好(可以原地升级)
    if c.hasNonVoter && !o.hasNonVoter {
        return 1
    }
    if !c.hasNonVoter && o.hasNonVoter {
        return -1
    }

    // 10. rangeCount 更低更好(负载更低)
    if c.rangeCount < o.rangeCount {
        return 1
    }
    if c.rangeCount > o.rangeCount {
        return -1
    }

    // 11. 相等
    return 0
}
```

**排序优先级总结**:

| 优先级 | 维度 | 含义 | 类型 |
|--------|------|------|------|
| 1 | valid | 满足约束 | 硬性约束 |
| 2 | !fullDisk | 磁盘未满 | 硬性约束 |
| 3 | necessary | 约束必要性 | 硬性约束 |
| 4 | voterNecessary | Voter 约束必要性 | 硬性约束 |
| 5 | diversityScore | 分散性 | 软性优化 |
| 6 | !ioOverloaded | IO 不过载 | 软性优化 |
| 7 | convergesScore | 向均衡收敛 | 软性优化 |
| 8 | balanceScore | 负载均衡 | 软性优化 |
| 9 | hasNonVoter | 可原地升级 | 软性优化 |
| 10 | rangeCount | Range 数量 | 打破平局 |

**设计哲学**:
- **分层决策**: 硬性约束 → 软性优化 → 打破平局
- **明确优先级**: 可用性(valid, !fullDisk) > 容错(diversity) > 负载均衡(balance)
- **容差设计**: `scoresAlmostEqual` 允许微小差异,避免过度优化

---

## 三、完整示例:从候选到排序

### 3.1 场景设定

```
集群配置:
  - 6 个 Store,分布在 3 个 Region

Store 列表:
  s1: region=us-east,  zone=us-east-1,  RangeCount=90,  DiskUsed=60%,  IOOverload=false
  s2: region=us-east,  zone=us-east-2,  RangeCount=200, DiskUsed=98%,  IOOverload=false
  s3: region=us-west,  zone=us-west-1,  RangeCount=150, DiskUsed=50%,  IOOverload=true
  s4: region=us-west,  zone=us-west-2,  RangeCount=80,  DiskUsed=70%,  IOOverload=false
  s5: region=us-central, zone=us-central-1, RangeCount=110, DiskUsed=55%, IOOverload=false
  s6: region=us-central, zone=us-central-2, RangeCount=95, DiskUsed=65%, IOOverload=false

当前 Range 副本:
  - s1: Voter, region=us-east, zone=us-east-1
  - s4: Voter, region=us-west, zone=us-west-2

约束配置:
  - NumReplicas=3
  - Constraints: 至少 1 个副本在 us-central

需求: 添加第 3 个 Voter 副本
```

### 3.2 阶段一:有效性过滤

```
s1: ✗ 已有副本,在阶段三排除
s2: ✗ 磁盘满(98%)
s3: ✗ IO 过载
s4: ✗ 已有副本,在阶段三排除
s5: ✓ 通过
s6: ✓ 通过

validCandidateStores = [s5, s6]
```

### 3.3 阶段二:构造 validStoreList

```
validStoreList:
  - Stores: [s5, s6]
  - 平均 RangeCount: (110 + 95) / 2 = 102.5
  - maxRangeCount: ceil(102.5 * 1.05) = 108
```

### 3.4 阶段三:排除已有副本 + 评分

```
s5 评分:
  - valid: true (满足约束)
  - necessary: true (约束要求至少 1 个副本在 us-central,当前没有,所以 s5 是必要的)
  - diversityScore:
      - s5 vs s1: region 不同 → 1.0
      - s5 vs s4: region 不同 → 1.0
      - 平均: (1.0 + 1.0) / 2 = 1.0
  - balanceScore:
      - s5.RangeCount=110 > 102.5(mean) 但 < 108(max)
      - balanceBelowMean (1)
  - hasNonVoter: false
  - rangeCount: 110

s6 评分:
  - valid: true
  - necessary: true (同样满足 us-central 约束)
  - diversityScore:
      - s6 vs s1: region 不同 → 1.0
      - s6 vs s4: region 不同 → 1.0
      - 平均: 1.0
  - balanceScore:
      - s6.RangeCount=95 < 102.5(mean)
      - balanceAboveMean (2)  ← 比 s5 更好
  - hasNonVoter: false
  - rangeCount: 95  ← 比 s5 更低

candidates = [
  {store: s5, valid: true, necessary: true, diversityScore: 1.0, balanceScore: 1, rangeCount: 110},
  {store: s6, valid: true, necessary: true, diversityScore: 1.0, balanceScore: 2, rangeCount: 95}
]
```

### 3.5 阶段四:排序

```
比较 s5 vs s6:
  - valid: 相同(都是 true)
  - necessary: 相同(都是 true)
  - diversityScore: 相同(都是 1.0)
  - balanceScore: s6(2) > s5(1)  ← s6 获胜

排序后: candidates = [s6, s5]
```

### 3.6 最终选择

```
CandidateSelector.selectOne(candidates)
  → 从 [s6, s5] 中随机选择
  → 大概率选择 s6(使用 Power of Two Random Choices)

结果: 将副本添加到 s6
```

---

## 四、关键设计决策分析

### 4.1 为什么要分离 valid 和 necessary?

**问题**: 为什么不直接用一个布尔值表示"是否满足约束"?

**答案**: `valid` 和 `necessary` 代表两个不同的维度:

```
valid (是否满足约束):
  - true: 该 Store 满足所有约束
  - false: 该 Store 违反某些约束
  - 作用: 排序时优先选择 valid=true 的候选

necessary (是否必要):
  - true: 该 Store 满足某个约束,且该约束目前未被充分满足
  - false: 该 Store 满足约束,但该约束已被充分满足
  - 作用: 在 valid 相同的情况下,优先选择 necessary=true 的候选

示例:
  约束: 至少 1 个副本在 us-east,至少 1 个副本在 us-west
  当前副本: s1(us-east), s2(us-east)

  候选 s3(us-west):
    - valid: true (满足约束)
    - necessary: true (us-west 约束未满足)

  候选 s4(us-east):
    - valid: true (满足约束)
    - necessary: false (us-east 约束已满足)

  排序: s3 > s4 (虽然都 valid,但 s3 更 necessary)
```

**设计哲学**: **约束满足度 > 约束种类**。先保证所有约束至少被满足一次,再考虑其他优化目标。

### 4.2 为什么不在阶段一就排除已有副本?

**问题**: 为什么在阶段三才排除已有副本的 Store,而不是在阶段一就排除?

**答案**: 因为 **validStoreList 需要包含所有有效的 Store 来计算准确的平均值**。

```
错误做法:
  阶段一:过滤磁盘满 + IO 过载 + 不满足约束 + 已有副本
  阶段二:构造 validStoreList

  问题:
    - 如果已有副本的 Store(s1, s4)也是有效的,它们会被排除
    - validStoreList 只包含 [s5, s6]
    - 平均 RangeCount = (110 + 95) / 2 = 102.5

    但实际上,集群中有效的 Store 是 [s1, s4, s5, s6]
    更准确的平均 RangeCount = (90 + 80 + 110 + 95) / 4 = 93.75

正确做法:
  阶段一:只过滤磁盘满 + IO 过载 + 不满足约束
  阶段二:构造 validStoreList(包含所有有效 Store,包括已有副本的)
  阶段三:排除已有副本,进行评分

  好处:
    - validStoreList 包含 [s1, s4, s5, s6]
    - 平均 RangeCount = 93.75(更准确)
    - balanceScore 计算更准确
```

**设计哲学**: **统计基准要全面,筛选要渐进**。先构造准确的统计基准,再逐步筛选候选。

### 4.3 为什么使用 scoresAlmostEqual 而非精确比较?

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:976-980
func scoresAlmostEqual(score1, score2 float64) bool {
    return math.Abs(score1-score2) < epsilon
}

const epsilon = 0.0001
```

**原因**:

```
浮点数精度问题:
  diversityScore 是 float64 类型
  计算过程涉及除法、平均值
  可能产生微小的精度误差

  例如:
    s1.diversityScore = 0.8333333333333334
    s2.diversityScore = 0.8333333333333333
    差异: 0.0000000000000001

  如果使用精确比较(==):
    s1 和 s2 会被认为不同
    排序会基于这个微小差异
    导致不稳定的排序结果

  使用 scoresAlmostEqual:
    s1 和 s2 被认为相等
    排序稳定,避免抖动
```

**设计哲学**: **容忍微小误差,追求稳定性**。避免因浮点数精度导致的不稳定排序。

### 4.4 为什么区分 deterministicForTesting?

```go
if options.deterministicForTesting() {
    sort.Sort(sort.Reverse(byScoreAndID(candidates)))  // 使用 StoreID 打破平局
} else {
    sort.Sort(sort.Reverse(byScore(candidates)))       // 不使用 StoreID,随机性更高
}
```

**原因**:

```
生产环境 (deterministicForTesting=false):
  - 当多个候选分数相同时,保持原始顺序(切片中的顺序)
  - 原始顺序本身带有一定随机性(gossip 更新顺序)
  - 后续 CandidateSelector 会随机选择
  - 好处: 避免所有 Range 都选择相同的 Store(避免热点)

测试环境 (deterministicForTesting=true):
  - 当多个候选分数相同时,使用 StoreID 打破平局
  - 保证排序结果确定性
  - 好处: 测试可重现,便于调试
```

**设计哲学**: **生产环境追求随机性(避免热点),测试环境追求确定性(可重现)**。

---

## 五、与前序章节的关联

### 5.1 与 ConstraintsChecker 的关联

```
ConstraintsChecker 章节(前一节):
  - 分析了如何选择 constraintsChecker 函数
  - voterConstraintsCheckerForAllocation
  - voterConstraintsCheckerForReplace
  - nonVoterConstraintsCheckerForAllocation
  - ...

本节 rankedCandidateListForAllocation:
  - 使用 constraintsChecker 函数作为参数
  - 在阶段一和阶段三调用 constraintsChecker
  - 基于 constraintsCheck 的返回值(valid, necessary)进行筛选和排序

关系:
  ConstraintsChecker 是策略,rankedCandidateListForAllocation 是执行者
  ConstraintsChecker 定义"什么样的 Store 满足约束"
  rankedCandidateListForAllocation 使用这个定义来筛选和评分
```

### 5.2 与 Allocator 决策流程的关联

```
完整链路:
  ComputeAction
    → 确定需要 Add/Remove/Replace/Rebalance
    ↓
  AllocateTarget/RemoveTarget/...
    → 选择 constraintsChecker 策略
    ↓
  rankedCandidateListForAllocation/rankedCandidateListForRemoval/...  ← 本节
    → 筛选、评分、排序候选 Store
    ↓
  CandidateSelector.selectOne
    → 从排序后的候选中随机选择一个
    ↓
  MMA Check + Simulate Remove
    → 冲突检测、乒乓预防
    ↓
  执行 Raft Conf Change
```

**本节在链路中的位置**: 核心评分引擎,将"所有可能的 Store"转化为"排序后的候选列表"。

---

## 六、设计模式识别

### 6.1 模板方法模式(Template Method Pattern)

**定义**: 在父类中定义算法的骨架,将某些步骤延迟到子类实现。

**应用**:

```
rankedCandidateListForAllocation 是一个模板:
  1. 有效性过滤
  2. 构造 validStoreList
  3. 排除已有副本
  4. 多维度评分
  5. 排序

关键步骤的可变部分通过参数注入:
  - constraintsCheck: 约束检查策略
  - options: 评分选项(磁盘、IO、负载均衡)
  - isStoreValidForRoutineReplicaTransfer: Store 存活检查

类似的模板还有:
  - rankedCandidateListForRemoval (移除场景)
  - rankedCandidateListForRebalance (Rebalance 场景)
```

### 6.2 策略模式(Strategy Pattern)

**定义**: 定义一系列算法,把它们封装起来,并使它们可以相互替换。

**应用**:

```
constraintsCheckFn 是一个策略接口:
  type constraintsCheckFn func(roachpb.StoreDescriptor) (valid, necessary bool)

具体策略:
  - voterConstraintsCheckerForAllocation
  - voterConstraintsCheckerForReplace
  - nonVoterConstraintsCheckerForAllocation
  - ...

使用方式:
  rankedCandidateListForAllocation(..., constraintsCheck, ...)
    → 接受策略作为参数
    → 在内部调用策略函数
    → 不关心具体实现
```

### 6.3 构建者模式(Builder Pattern)

**定义**: 将复杂对象的构建与表示分离,使得同样的构建过程可以创建不同的表示。

**应用**:

```
candidate 结构的构建:
  candidates = append(candidates, candidate{
      store:          s,
      necessary:      necessary,
      valid:          constraintsOK,
      diversityScore: diversityScore,
      balanceScore:   balanceScore,
      hasNonVoter:    hasNonVoter,
      rangeCount:     rangeCountScore,
  })

构建过程:
  1. 基础信息: store
  2. 约束检查: valid, necessary
  3. 分散性评分: diversityScore
  4. 负载均衡评分: balanceScore
  5. 其他标志: hasNonVoter, rangeCount

结果: 一个完整的 candidate 对象,包含所有评分维度
```

### 6.4 分层过滤模式(Layered Filtering Pattern)

**定义**: 通过多层过滤器逐步筛选数据,每层负责不同的职责。

**应用**:

```
第一层: 有效性过滤
  - 过滤磁盘满的 Store
  - 过滤 IO 过载的 Store
  - 过滤不满足约束的 Store

第二层: 统计基准构造
  - 基于有效 Store 计算平均值

第三层: 排除过滤
  - 排除已有副本的 Store
  - 排除已有副本的 Node
  - 排除不存活的 Node

第四层: 评分
  - 多维度评分

第五层: 排序
  - 基于分数排序

好处:
  - 每层职责明确
  - 易于理解和维护
  - 易于添加新的过滤条件
```

---

## 七、工程权衡分析

### 7.1 两次调用 constraintsCheck vs 缓存结果

**当前实现**: 调用两次 `constraintsCheck(s)`,一次在阶段一,一次在阶段三。

**替代方案**: 只调用一次,缓存结果(valid, necessary)。

**权衡**:

| 维度 | 当前实现 | 替代方案 |
|------|----------|----------|
| 代码简洁性 | ✓ 逻辑清晰,每个阶段独立 | ✗ 需要额外的 map 缓存结果 |
| 性能 | ✗ 两次调用,可能略慢 | ✓ 只调用一次 |
| 内存 | ✓ 不需要额外内存 | ✗ 需要 map 缓存 |
| 正确性 | ✓ 阶段一过滤无效 Store,阶段三记录 necessary | ✓ 相同 |

**CockroachDB 的选择**: 当前实现,因为:
- `constraintsCheck` 函数非常轻量(只是遍历约束列表,O(n))
- 候选 Store 数量通常较小(< 100)
- 两次调用的性能开销可忽略不计
- 代码简洁性和可读性更重要

### 7.2 validStoreList 独立构造 vs 共享 candidateStores

**当前实现**: 构造独立的 `validStoreList`。

**替代方案**: 直接使用 `candidateStores`,但在计算平均值时过滤无效 Store。

**权衡**:

| 维度 | 当前实现 | 替代方案 |
|------|----------|----------|
| 内存 | ✗ 需要额外的 StoreList | ✓ 不需要额外内存 |
| 正确性 | ✓ validStoreList.mean 准确 | ✓ 手动过滤也准确 |
| 代码复杂度 | ✓ 简单,storepool.MakeStoreList 自动计算 | ✗ 需要手动过滤和计算平均值 |
| 性能 | ✗ 需要复制 Store 列表 | ✓ 不需要复制 |

**CockroachDB 的选择**: 当前实现,因为:
- `storepool.MakeStoreList` 已经封装了统计计算逻辑
- 避免重复实现统计计算
- 代码复用性更好
- Store 列表通常较小,复制开销可忽略

### 7.3 排序稳定性 vs 性能

**当前实现**: 使用 `sort.Sort`,非稳定排序。

**替代方案**: 使用 `sort.Stable`,稳定排序。

**权衡**:

| 维度 | 当前实现 | 替代方案 |
|------|----------|----------|
| 性能 | ✓ O(n log n) 平均,更快 | ✗ O(n log n) 最坏,稍慢 |
| 稳定性 | ✗ 相同分数的元素顺序不确定 | ✓ 相同分数的元素保持原顺序 |
| 随机性 | ✓ 保持一定随机性,避免热点 | ✗ 顺序确定,可能导致热点 |

**CockroachDB 的选择**: 当前实现,因为:
- 生产环境中,随机性有助于避免热点
- 后续 CandidateSelector 会再次随机选择
- 稳定性对 Allocator 不重要(不需要可重现的排序)
- 性能更好

### 7.4 多维度评分 vs 单一综合分数

**当前实现**: 分别记录 diversityScore、balanceScore、rangeCount 等多个维度,排序时按优先级逐个比较。

**替代方案**: 计算一个综合分数(weighted sum),排序时只比较综合分数。

**权衡**:

| 维度 | 当前实现 | 替代方案 |
|------|----------|----------|
| 灵活性 | ✓ 可以明确控制优先级 | ✗ 权重选择困难 |
| 可理解性 | ✓ 每个维度的作用清晰 | ✗ 综合分数难以解释 |
| 性能 | ✗ 多次比较 | ✓ 只比较一次 |
| 调试 | ✓ 可以看到每个维度的值 | ✗ 只能看到综合分数 |

**CockroachDB 的选择**: 当前实现,因为:
- 副本放置是关键决策,可理解性至关重要
- 多维度评分便于调试(可以打印每个候选的详细分数)
- 性能差异可忽略(候选数量小)
- 权重选择是难题,分层比较更简单

### 7.5 全局最优 vs 局部最优

**当前实现**: 基于 Local Cache(StorePool)做决策,不保证全局最优。

**替代方案**: 基于全局一致的 Store 信息做决策。

**权衡**:

| 维度 | 当前实现 | 替代方案 |
|------|----------|----------|
| 性能 | ✓ 不需要全局协调 | ✗ 需要全局共识 |
| 可用性 | ✓ 即使网络分区也能决策 | ✗ 网络分区时无法决策 |
| 正确性 | ✗ 可能做出非最优决策 | ✓ 保证全局最优 |
| 收敛性 | ✓ 最终一致,会自动修正 | ✓ 立即一致 |

**CockroachDB 的选择**: 当前实现,因为:
- CAP 定理:在可用性和一致性之间,Allocator 选择可用性
- 副本放置不需要强一致性
- 最终一致性足够(下一轮 Rebalance 会修正)
- 全局协调成本太高

---

## 八、心智模型与类比

### 8.1 招聘面试官的类比

```
rankedCandidateListForAllocation ≈ 招聘面试官筛选候选人

输入:
  - candidateStores ≈ 所有应聘者的简历
  - constraintsCheck ≈ 岗位要求(学历、经验等)
  - existingReplicas ≈ 当前团队成员

阶段一:简历筛选
  - 排除不满足基本要求的应聘者(学历、经验)
  - 排除已经有全职工作的应聘者(类似磁盘满)

阶段二:构造评分基准
  - 基于合格应聘者计算"平均技能水平"
  - 不应该用"所有应聘者"(包括不合格的)计算平均

阶段三:排除冲突
  - 排除已经在团队中的人
  - 排除和现有成员有冲突的人

阶段四:多维度评分
  - 技能匹配度(diversityScore): 是否填补团队空缺
  - 工作量均衡(balanceScore): 是否能平衡团队负载
  - 协作能力(hasNonVoter): 是否能和现有成员协作

阶段五:排序与选择
  - 按优先级排序:硬性要求 > 软性加分 > 随机打破平局
  - 随机选择一个(避免所有团队都招同一个人)
```

### 8.2 餐厅选址的类比

```
rankedCandidateListForAllocation ≈ 连锁餐厅选址

输入:
  - candidateStores ≈ 所有候选地段
  - constraintsCheck ≈ 选址要求(交通、人流、租金)
  - existingReplicas ≈ 已有门店

阶段一:排除不符合要求的地段
  - 排除租金太高的地段(类似磁盘满)
  - 排除交通不便的地段(类似 IO 过载)
  - 排除不满足消防要求的地段(类似约束)

阶段二:计算竞争基准
  - 基于合格地段计算"平均竞争激烈度"
  - 不应该用"所有地段"(包括不合格的)计算平均

阶段三:排除已有门店附近
  - 避免自家门店竞争
  - 避免同一商圈多家门店

阶段四:多维度评分
  - 覆盖盲区(diversityScore): 是否填补没有门店的区域
  - 负载均衡(balanceScore): 是否平衡各区域门店数量
  - 升级现有(hasNonVoter): 是否可以升级现有小店

阶段五:排序与选择
  - 按优先级排序:硬性要求 > 战略覆盖 > 负载均衡
  - 随机选择一个(避免所有连锁都选同一地段)
```

### 8.3 核心直觉

**rankedCandidateListForAllocation 的本质**:

> 在满足硬性约束的前提下,找到一个最能改善系统整体状态(分散性、负载均衡)的候选 Store。
>
> 关键原则:
> 1. **先满足约束,再优化性能**: 约束 > 分散性 > 负载均衡
> 2. **基于准确的统计基准**: 只用有效 Store 计算平均值
> 3. **分层决策,逐步筛选**: 硬性过滤 → 软性评分 → 随机选择
> 4. **容忍局部非最优**: 基于 Local Cache,最终一致
> 5. **追求稳定性**: 容差设计,避免抖动

---

## 九、调试与故障排查

### 9.1 常见问题与排查方法

#### 问题 1: Allocator 总是选择相同的 Store

**现象**: 多个 Range 的新副本都被放置到同一个 Store 上,导致负载不均。

**排查步骤**:

1. **检查 validCandidateStores 是否只有一个元素**:
   ```
   日志关键字: "validCandidateStores"
   如果只有一个,说明其他 Store 被过滤掉了
   ```

2. **检查过滤原因**:
   ```
   可能原因:
   - 其他 Store 磁盘满(DiskUsed > 95%)
   - 其他 Store IO 过载
   - 其他 Store 不满足约束
   ```

3. **检查约束配置**:
   ```sql
   SHOW ZONE CONFIGURATION FOR RANGE default;
   ```
   确认约束是否过于严格。

4. **检查 balanceScore**:
   ```
   日志关键字: "balanceScore"
   如果所有候选的 balanceScore 都相同,可能是阈值设置问题
   ```

#### 问题 2: Allocator 不将副本放到空闲的 Store

**现象**: 某个 Store 的 RangeCount 很低,但 Allocator 不将新副本放到该 Store 上。

**排查步骤**:

1. **检查该 Store 是否通过阶段一过滤**:
   ```
   可能原因:
   - 磁盘满(DiskUsed > 95%)
   - IO 过载
   - 不满足约束
   ```

2. **检查 diversityScore**:
   ```
   日志关键字: "diversityScore"
   该 Store 的 diversityScore 可能很低(和现有副本在同一 Locality)
   即使 RangeCount 低,diversityScore 优先级更高
   ```

3. **检查 necessary 标志**:
   ```
   日志关键字: "necessary"
   其他 Store 的 necessary=true,该 Store 的 necessary=false
   即使 balanceScore 更好,necessary 优先级更高
   ```

#### 问题 3: Allocator 频繁在两个 Store 之间迁移副本

**现象**: 副本在 Store A 和 Store B 之间反复迁移,导致网络带宽浪费。

**排查步骤**:

1. **检查 scoresAlmostEqual 的阈值**:
   ```go
   const epsilon = 0.0001
   ```
   如果两个 Store 的 diversityScore 差异小于 epsilon,应该被认为相等。

2. **检查 balanceScore 的阈值**:
   ```go
   rangeRebalanceThreshold = 0.05  // 5%
   ```
   如果两个 Store 的 RangeCount 差异小于 5%,不应该触发 Rebalance。

3. **检查 MMA(Multi-Move Avoidance) 机制**:
   ```
   Allocator 应该有 MMA 检查,防止乒乓迁移
   如果 MMA 失效,需要检查实现
   ```

### 9.2 调试技巧

#### 技巧 1: 启用详细日志

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1267-1274
log.KvDistribution.VEventf(
    ctx,
    3,  // 日志级别 3(详细)
    "not considering store s%d as a potential rebalance candidate because it is on a non-live node n%d",
    s.StoreID,
    s.Node.NodeID,
)
```

启用方式:
```bash
cockroach debug logs --filter=kv-distribution --verbosity=3
```

#### 技巧 2: 打印候选分数

```go
// 在排序前添加日志
for _, c := range candidates {
    log.Infof(ctx, "Candidate: %s", c.String())
}
```

输出示例:
```
Candidate: s1, valid:true, fulldisk:false, necessary:true, diversity:1.00, balance:2, rangeCount:90
Candidate: s2, valid:true, fulldisk:false, necessary:false, diversity:0.75, balance:1, rangeCount:110
```

#### 技巧 3: 使用 EXPLAIN ANALYZE

```sql
EXPLAIN ANALYZE SELECT * FROM table WHERE ...;
```

查看哪些 Leaseholder 正在处理查询,间接判断副本分布。

---

## 十、总结

### 10.1 核心要点

1. **四阶段执行流程**:
   - 阶段一:有效性过滤(磁盘、IO、约束)
   - 阶段二:构造 validStoreList(准确的统计基准)
   - 阶段三:排除已有副本,多维度评分
   - 阶段四:排序与返回

2. **关键设计决策**:
   - **validStoreList 独立构造**: 保证统计基准准确
   - **valid 和 necessary 分离**: 区分"满足约束"和"必要满足约束"
   - **两次调用 constraintsCheck**: 简化代码,性能开销可忽略
   - **多维度评分**: 可理解性 > 性能

3. **排序优先级**:
   ```
   硬性约束(valid, !fullDisk, necessary, voterNecessary)
     > 软性优化(diversityScore, !ioOverloaded, balanceScore)
     > 打破平局(rangeCount)
   ```

4. **工程权衡**:
   - **可用性 > 一致性**: 基于 Local Cache,最终一致
   - **可理解性 > 性能**: 多维度评分,分层决策
   - **稳定性 > 精确性**: 容差设计,避免抖动

### 10.2 与前序内容的联系

```
ConstraintsChecker(前一节)
  → 定义约束检查策略(voterConstraintsCheckerForAllocation 等)
  ↓
rankedCandidateListForAllocation(本节)
  → 使用约束检查策略筛选和评分候选 Store
  ↓
CandidateSelector(下一节,如果需要)
  → 从排序后的候选中随机选择一个
```

### 10.3 实践建议

1. **理解分层决策**: Allocator 的决策是分层的,不要试图用单一指标优化
2. **容忍非最优**: Allocator 基于 Local Cache,短期可能非最优,但会自动修正
3. **关注硬性约束**: 约束优先级最高,负载均衡是次要目标
4. **启用详细日志**: 调试时启用 VEventf,查看每个候选的评分
5. **理解权衡**: 每个设计决策都是权衡的结果,没有完美的解决方案

---

**至此,`rankedCandidateListForAllocation` 函数的深度剖析完成。本节通过四阶段执行流程、多维度评分机制、具体数值示例、设计模式识别、工程权衡分析,帮助读者构建对这个关键函数的全面理解。结合前序章节的 ConstraintsChecker 分析,读者现在应该能够理解 Allocator 如何从约束定义到候选筛选,再到最终选择的完整流程。**
