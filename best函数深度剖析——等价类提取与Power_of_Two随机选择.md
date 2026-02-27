# best() 函数深度剖析——等价类提取与 Power of Two 随机选择

> **核心问题**: `best()` 函数在 Allocator 的候选选择流程中扮演什么角色?为什么不直接选择分数最高的候选,而是要提取一个"等价类"?这个等价类如何与 Power of Two Random Choices 策略配合使用?
>
> **解答路径**: 本节通过深入分析 `best()` 函数的执行逻辑、等价性判定标准、与 `good()`/`worst()` 的对比、以及与 `selectBest()` 的协作关系,揭示 CockroachDB 如何在保证决策质量的前提下,通过随机化避免集群级的"羊群效应"。

---

## 一、函数定位与设计目标

### 1.1 在 Allocator 决策流程中的位置

```
rankedCandidateListForAllocation()
  ↓ (返回按分数降序排列的候选列表)
candidateList = [c1, c2, c3, ..., cn]  (已排序: c1.score ≥ c2.score ≥ ... ≥ cn.score)
  ↓
CandidateSelector.selectOne()
  ↓
selectBest(randGen)                    ← 入口:需要从候选中选一个
  ↓
cl.best()                              ← 【本函数】提取等价类
  ↓ (返回等价类)
equivalenceClass = [c1, c2, ..., ck]   (k ≤ n,所有候选在关键维度上等价)
  ↓
Power of Two Random Choices            ← 从等价类中随机选择
  ↓
最终选择: 返回 &c_selected
```

**核心职责**:
- **输入**: 已排序的候选列表(按分数降序)
- **处理**: 提取与第一名在关键维度上"等价"的所有候选
- **输出**: 等价类(equivalence class)

**关键设计理念**: **不是选"最优",而是选"等价最优类"**

### 1.2 为什么需要等价类?

**问题**: 为什么不直接选择 `candidates[0]`(分数最高的)?

**场景示例**:

```
假设集群有 100 个节点,每个节点运行 Allocator 独立决策。
当前有 1000 个 Range 需要添加副本。

如果所有节点都"确定性地"选择分数最高的候选:
  - 所有 1000 个 Range 都会选择 Store s1(假设 s1 分数最高)
  - s1 瞬间收到 1000 个副本添加请求
  - s1 负载飙升,成为新的热点
  - 下一轮 Rebalance 又要把这些副本移走

这就是"羊群效应"(Thundering Herd):
  - 所有决策者看到相同的信息
  - 做出相同的决策
  - 导致系统震荡
```

**解决方案: 等价类 + 随机选择**

```
1. 提取等价类:
   candidates = [s1, s2, s3, s4, s5, ...]
   s1.diversityScore = 0.95
   s2.diversityScore = 0.95
   s3.diversityScore = 0.90  ← 不在等价类中

   equivalenceClass = [s1, s2]  (只有 s1, s2 在关键维度上等价)

2. 随机选择:
   - 50% 的 Range 选择 s1
   - 50% 的 Range 选择 s2
   - 负载分散,避免热点

好处:
  - 保证决策质量(只在"最优"候选中选择)
  - 避免羊群效应(随机化分散负载)
  - 系统稳定性(没有单点热点)
```

---

## 二、函数实现深度解析

### 2.1 完整代码分析

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1056-1077
func (cl candidateList) best() candidateList {
    // 第一步：过滤非法 / 磁盘不健康的候选
    // 返回的仍然是一个【前缀】，并且保持原有"按分数倒序"的顺序。
    cl = cl.onlyValidAndHealthyDisk()
    if len(cl) <= 1 {
        return cl
    }

    // 第二步：从前往后扫描,找到第一个"不等价"的候选
    for i := 1; i < len(cl); i++ {
        // 下面这些字段，构成了 allocator 认为的
        // "是否与第一名在约束意义上等价"的判定条件
        if cl[i].necessary == cl[0].necessary &&
            cl[i].voterNecessary == cl[0].voterNecessary &&
            scoresAlmostEqual(cl[i].diversityScore, cl[0].diversityScore) &&
            cl[i].convergesScore == cl[0].convergesScore &&
            cl[i].balanceScore == cl[0].balanceScore &&
            cl[i].hasNonVoter == cl[0].hasNonVoter {
            continue  // 仍然等价,继续扫描
        }
        // 找到第一个不等价的,返回前缀 [0, i)
        return cl[:i]
    }
    // 所有候选都等价,返回整个列表
    return cl
}
```

### 2.2 两阶段执行流程

#### 阶段一: 过滤非法和不健康的候选

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1030-1048
func (cl candidateList) onlyValidAndHealthyDisk() candidateList {
    // 从列表末尾向前扫描
    // 因为：分数是倒序的，越靠后分数越低
    for i := len(cl) - 1; i >= 0; i-- {
        // 条件含义：
        // valid         ：满足所有硬约束（constraint、replica type 等）
        // !fullDisk     ：磁盘没有接近满
        // !ioOverloaded ：磁盘 IO 未过载
        if cl[i].valid && !cl[i].fullDisk && !cl[i].ioOverloaded {
            // 一旦找到"最靠后的一个合法且健康的候选"，
            // 那么 [0..i] 之间的所有候选：
            // - 分数 >= cl[i]
            // - 且已经在排序阶段保证了分数顺序
            // 所以可以直接返回前缀
            return cl[:i+1]
        }
    }
    return candidateList{}
}
```

**为什么从后往前扫描?**

```
候选列表已按分数降序排列:
  [c0, c1, c2, c3, c4, c5]  (c0 分数最高, c5 分数最低)
   ↑                    ↑
   最可能合法          最可能不合法

从后往前扫描的好处:
  1. 一旦找到最后一个合法的候选 ci,前面的所有候选 [c0, c1, ..., ci] 都可能合法
  2. 因为排序保证了: score(c0) ≥ score(c1) ≥ ... ≥ score(ci)
  3. 而 valid/fullDisk/ioOverloaded 在排序中优先级更高
  4. 所以前面的候选不会突然变成不合法

示例:
  输入: [c0(valid), c1(valid), c2(valid), c3(invalid), c4(invalid), c5(fullDisk)]

  从后往前扫描:
    i=5: c5.fullDisk=true → 跳过
    i=4: c4.valid=false → 跳过
    i=3: c3.valid=false → 跳过
    i=2: c2.valid=true && !c2.fullDisk && !c2.ioOverloaded → 找到!

  返回: cl[:3] = [c0, c1, c2]
```

**时间复杂度**: O(n),但实际上通常只扫描很少的元素,因为不合法的候选会集中在尾部。

#### 阶段二: 提取等价类

```go
for i := 1; i < len(cl); i++ {
    if cl[i].necessary == cl[0].necessary &&
        cl[i].voterNecessary == cl[0].voterNecessary &&
        scoresAlmostEqual(cl[i].diversityScore, cl[0].diversityScore) &&
        cl[i].convergesScore == cl[0].convergesScore &&
        cl[i].balanceScore == cl[0].balanceScore &&
        cl[i].hasNonVoter == cl[0].hasNonVoter {
        continue  // 等价,继续
    }
    return cl[:i]  // 不等价,返回前缀
}
return cl  // 全部等价
```

**等价性判定标准(6 个维度)**:

| 维度 | 类型 | 含义 | 为什么必须等价? |
|------|------|------|----------------|
| `necessary` | bool | 是否满足未满足的约束 | **硬性约束优先级最高**,不等价意味着约束满足度不同 |
| `voterNecessary` | bool | 是否满足 Voter 约束 | **Voter 约束优先级仅次于 overall 约束**,不等价影响 Raft 群组稳定性 |
| `diversityScore` | float64 | 分散性评分 | **容错能力核心指标**,不等价意味着故障域覆盖不同 |
| `convergesScore` | int | 向均衡收敛评分 | **负载均衡核心指标**,不等价意味着对负载均衡贡献不同 |
| `balanceScore` | balanceStatus | 负载均衡状态 | **当前负载状态**,不等价意味着会加剧或缓解不均衡 |
| `hasNonVoter` | bool | 是否可原地升级 | **性能优化指标**,不等价意味着网络/磁盘开销不同 |

**为什么 `rangeCount` 不在等价性判定中?**

```
rangeCount 的作用:
  - 打破平局(tie-breaker)
  - 当其他所有维度都等价时,使用 rangeCount 区分

但在 best() 中:
  - rangeCount 不参与等价性判定
  - 因为 rangeCount 是连续值,很难找到完全相等的候选
  - 如果要求 rangeCount 也等价,等价类会退化为单元素集合
  - 失去了随机化的意义

示例:
  c1: rangeCount=100, diversityScore=0.95, ...
  c2: rangeCount=101, diversityScore=0.95, ...
  c3: rangeCount=105, diversityScore=0.90, ...

  如果要求 rangeCount 等价:
    equivalenceClass = [c1]  (只有 c1)
    → 失去随机化,羊群效应

  当前实现:
    equivalenceClass = [c1, c2]  (diversityScore 等价,忽略 rangeCount 差异)
    → 保持随机化,避免羊群效应
```

### 2.3 具体示例:等价类提取

#### 示例 1: 正常场景

```
输入候选列表(已排序):
  c1: valid=true, necessary=true, voterNecessary=false, diversityScore=0.95,
      convergesScore=1, balanceScore=2(underfull), hasNonVoter=false, rangeCount=100

  c2: valid=true, necessary=true, voterNecessary=false, diversityScore=0.95,
      convergesScore=1, balanceScore=2(underfull), hasNonVoter=false, rangeCount=105

  c3: valid=true, necessary=true, voterNecessary=false, diversityScore=0.90,
      convergesScore=1, balanceScore=2(underfull), hasNonVoter=false, rangeCount=98

  c4: valid=true, necessary=false, voterNecessary=false, diversityScore=0.85,
      convergesScore=0, balanceScore=1(aroundTheMean), hasNonVoter=false, rangeCount=110

执行 best():
  1. onlyValidAndHealthyDisk() → 所有候选都合法,返回 [c1, c2, c3, c4]

  2. 等价性判定:
     - i=1: c2 vs c1
       - necessary: true == true ✓
       - voterNecessary: false == false ✓
       - diversityScore: 0.95 ≈ 0.95 ✓ (scoresAlmostEqual)
       - convergesScore: 1 == 1 ✓
       - balanceScore: 2 == 2 ✓
       - hasNonVoter: false == false ✓
       → 等价,继续

     - i=2: c3 vs c1
       - necessary: true == true ✓
       - voterNecessary: false == false ✓
       - diversityScore: 0.90 ≈ 0.95? ✗ (差异 0.05 > epsilon=0.0001)
       → 不等价,返回 cl[:2]

结果: equivalenceClass = [c1, c2]

解释:
  - c1 和 c2 在所有关键维度上等价,只有 rangeCount 不同(100 vs 105)
  - rangeCount 不参与等价性判定,所以被视为等价
  - c3 的 diversityScore=0.90,与 c1 的 0.95 差异显著,不等价
  - c4 的 necessary=false,与 c1 的 true 不同,不等价
```

#### 示例 2: 所有候选都等价

```
输入候选列表:
  c1: necessary=true, diversityScore=1.0, convergesScore=1, balanceScore=2, rangeCount=100
  c2: necessary=true, diversityScore=1.0, convergesScore=1, balanceScore=2, rangeCount=102
  c3: necessary=true, diversityScore=1.0, convergesScore=1, balanceScore=2, rangeCount=105
  c4: necessary=true, diversityScore=1.0, convergesScore=1, balanceScore=2, rangeCount=108

执行 best():
  1. onlyValidAndHealthyDisk() → 所有候选都合法

  2. 等价性判定:
     - i=1: c2 vs c1 → 所有维度都等价 ✓
     - i=2: c3 vs c1 → 所有维度都等价 ✓
     - i=3: c4 vs c1 → 所有维度都等价 ✓
     - 循环结束,没有找到不等价的

  3. 返回整个列表

结果: equivalenceClass = [c1, c2, c3, c4]

解释:
  - 所有候选在关键维度上完全等价
  - 只有 rangeCount 不同,但不影响等价性
  - 随机选择时会从 4 个候选中选一个,负载分散更均匀
```

#### 示例 3: 第一名独一无二

```
输入候选列表:
  c1: necessary=true, voterNecessary=true, diversityScore=1.0, convergesScore=2,
      balanceScore=2, hasNonVoter=true, rangeCount=80

  c2: necessary=true, voterNecessary=false, diversityScore=1.0, convergesScore=2,
      balanceScore=2, hasNonVoter=false, rangeCount=85

  c3: necessary=true, voterNecessary=false, diversityScore=0.95, convergesScore=1,
      balanceScore=2, hasNonVoter=false, rangeCount=90

执行 best():
  1. onlyValidAndHealthyDisk() → 所有候选都合法

  2. 等价性判定:
     - i=1: c2 vs c1
       - voterNecessary: false == true? ✗
       → 不等价,返回 cl[:1]

结果: equivalenceClass = [c1]

解释:
  - c1 的 voterNecessary=true,表示它满足 Voter 约束且该约束未被充分满足
  - c2 的 voterNecessary=false,表示它不满足该约束或约束已满足
  - 这是关键差异,c1 明显优于 c2
  - 等价类退化为单元素,只能选择 c1
  - 这种情况下,所有节点都会选择 c1(确定性),但这是合理的,因为 c1 确实是唯一的最优选择
```

#### 示例 4: 过滤后为空

```
输入候选列表:
  c1: valid=false, fullDisk=false, ioOverloaded=false, ...
  c2: valid=true, fullDisk=true, ioOverloaded=false, ...
  c3: valid=true, fullDisk=false, ioOverloaded=true, ...

执行 best():
  1. onlyValidAndHealthyDisk():
     - i=2: c3.ioOverloaded=true → 跳过
     - i=1: c2.fullDisk=true → 跳过
     - i=0: c1.valid=false → 跳过
     - 循环结束,没有找到合法且健康的候选
     → 返回空列表 []

  2. len(cl) == 0,直接返回

结果: equivalenceClass = []

解释:
  - 所有候选都不满足基本要求(valid && !fullDisk && !ioOverloaded)
  - 返回空列表
  - selectBest() 收到空列表后会返回 nil
  - AllocateTarget() 会报告"无可用候选"
```

---

## 三、与 good()/worst() 的对比

CockroachDB 提供了三个"提取子集"的函数: `best()`, `good()`, `worst()`,它们的区别在于"等价性"的宽松程度。

### 3.1 函数对比表

| 函数 | 目标 | 等价性标准 | 使用场景 |
|------|------|-----------|---------|
| `best()` | 提取"最优"等价类 | 6 维度全等价 | **Allocation**: 添加新副本,要求最严格 |
| `good()` | 提取"足够好"等价类 | 3 维度等价 | **Recovery**: 快速恢复,容忍次优选择 |
| `worst()` | 提取"最差"等价类 | 5 维度等价(从末尾) | **Removal**: 移除副本,选择最应该移除的 |

### 3.2 best() vs good() 详细对比

```go
// best() 的等价性标准: 6 个维度
if cl[i].necessary == cl[0].necessary &&
    cl[i].voterNecessary == cl[0].voterNecessary &&
    scoresAlmostEqual(cl[i].diversityScore, cl[0].diversityScore) &&
    cl[i].convergesScore == cl[0].convergesScore &&      // ← best() 要求
    cl[i].balanceScore == cl[0].balanceScore &&          // ← best() 要求
    cl[i].hasNonVoter == cl[0].hasNonVoter {            // ← best() 要求
    continue
}

// good() 的等价性标准: 3 个维度
if cl[i].necessary == cl[0].necessary &&
    cl[i].voterNecessary == cl[0].voterNecessary &&
    scoresAlmostEqual(cl[i].diversityScore, cl[0].diversityScore) {
    continue
}
```

**为什么 good() 更宽松?**

```
使用场景对比:

best() - 用于正常 Allocation:
  - 集群状态正常,有充足的时间选择
  - 目标:找到"绝对最优"的候选
  - 要求:在所有维度上都与第一名等价
  - 等价类较小,选择更精确

good() - 用于 Recovery:
  - 节点 Down/Decommission,需要快速恢复副本
  - 目标:找到"足够好"的候选,快速恢复
  - 要求:只在关键维度(necessary, voterNecessary, diversityScore)上等价
  - 等价类较大,有更多选择,选择更快
```

**示例对比**:

```
输入候选列表:
  c1: necessary=true, voterNecessary=false, diversityScore=0.95,
      convergesScore=1, balanceScore=2, hasNonVoter=false

  c2: necessary=true, voterNecessary=false, diversityScore=0.95,
      convergesScore=1, balanceScore=1, hasNonVoter=false

  c3: necessary=true, voterNecessary=false, diversityScore=0.95,
      convergesScore=0, balanceScore=2, hasNonVoter=true

best() 的结果:
  - c1 作为基准
  - c2: balanceScore 不同(2 vs 1) → 不等价
  - 返回: [c1]

good() 的结果:
  - c1 作为基准
  - c2: necessary, voterNecessary, diversityScore 都等价 → 等价
  - c3: necessary, voterNecessary, diversityScore 都等价 → 等价
  - 返回: [c1, c2, c3]

使用建议:
  - 正常 Allocation: 使用 best(),等价类 = [c1],选择最优
  - 快速 Recovery: 使用 good(),等价类 = [c1, c2, c3],选择更快
```

### 3.3 best() vs worst() 详细对比

```go
// best() 从前往后扫描,找第一名的等价类
for i := 1; i < len(cl); i++ {
    if /* 6 个维度等价 */ {
        continue
    }
    return cl[:i]  // 返回前缀
}

// worst() 从后往前扫描,找最后一名的等价类
for i := len(cl) - 2; i >= 0; i-- {
    if /* 5 个维度等价 */ {
        continue
    }
    return cl[i+1:]  // 返回后缀
}
```

**关键区别**:

| 维度 | best() | worst() | 原因 |
|------|--------|---------|------|
| 扫描方向 | 从前往后 | 从后往前 | best() 找最好的,worst() 找最差的 |
| 返回值 | 前缀 `cl[:i]` | 后缀 `cl[i+1:]` | 前缀是高分区,后缀是低分区 |
| 等价性 | 6 维度 | 5 维度 | worst() 不考虑 `hasNonVoter` |
| 优先处理 | valid, !fullDisk, !ioOverloaded | !valid, fullDisk, ioOverloaded | worst() 优先选择最差的 |

**为什么 worst() 不考虑 hasNonVoter?**

```
hasNonVoter 的含义:
  - true: 该 Store 已有 NonVoter,可以原地升级为 Voter
  - false: 该 Store 没有 NonVoter,需要新建副本

在 Removal 场景中:
  - 我们要移除副本,不是添加
  - hasNonVoter 只对添加副本有意义(可以节省网络传输)
  - 移除副本时,hasNonVoter 无关紧要
  - 所以 worst() 不考虑这个维度
```

**示例对比**:

```
输入候选列表(Removal 场景,从已有副本中移除一个):
  c1: valid=true, necessary=false, diversityScore=0.50, convergesScore=-1, balanceScore=1
  c2: valid=true, necessary=false, diversityScore=0.50, convergesScore=-1, balanceScore=0
  c3: valid=true, necessary=false, diversityScore=0.55, convergesScore=0, balanceScore=0

worst() 的结果:
  - 从后往前扫描
  - c2: 与 c3 在 necessary, diversityScore 上不等价(0.50 vs 0.55) → 不等价
  - 返回: [c2, c3]? 不对,应该返回 [c2]

实际逻辑:
  - c3 作为基准(最差的)
  - c2 vs c3:
    - necessary: false == false ✓
    - voterNecessary: false == false ✓
    - diversityScore: 0.50 ≈ 0.55? ✗
  - 返回: cl[2:] = [c3]

如果 c2 和 c3 的 diversityScore 都是 0.50:
  - c2 vs c3:
    - necessary: false == false ✓
    - voterNecessary: false == false ✓
    - diversityScore: 0.50 == 0.50 ✓
    - convergesScore: -1 == 0? ✗
  - 返回: cl[2:] = [c3]

如果所有维度都等价:
  - 返回: cl[1:] = [c2, c3]
```

---

## 四、与 selectBest() 的协作: Power of Two Random Choices

### 4.1 完整选择流程

```go
// pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go:1158-1176
func (cl candidateList) selectBest(randGen allocatorRand) *candidate {
    // 第一步: 提取等价类
    cl = cl.best()
    if len(cl) == 0 {
        return nil  // 没有合法候选
    }
    if len(cl) == 1 {
        return &cl[0]  // 只有一个候选,直接返回
    }

    // 第二步: Power of Two Random Choices
    randGen.Lock()
    order := randGen.Perm(len(cl))  // 生成随机排列
    randGen.Unlock()

    // 第三步: 从随机排列的前 allocatorRandomCount(=2) 个候选中选择最优的
    best := &cl[order[0]]
    for i := 1; i < allocatorRandomCount; i++ {  // allocatorRandomCount = 2
        if best.less(cl[order[i]]) {
            best = &cl[order[i]]
        }
    }
    return best
}
```

### 4.2 Power of Two Random Choices 详解

**算法核心思想**:

```
给定等价类: [c1, c2, c3, c4, c5]

方案 1: Uniform Random(均匀随机)
  - 从 5 个候选中随机选 1 个
  - 问题: 完全随机,可能选到"等价类中相对较差"的候选

方案 2: Best of N Random Choices(N 随机选择最优)
  - 从 5 个候选中随机选 N 个,选出其中最优的
  - 问题: N 太大,计算成本高; N 太小,效果不明显

方案 3: Power of Two Random Choices (折中方案,CockroachDB 采用)
  - 从 5 个候选中随机选 2 个,选出其中最优的
  - 好处: 计算成本低(只比较 2 个),效果显著
```

**数学原理**:

```
假设等价类中候选的 rangeCount 分布如下:
  c1: rangeCount=100
  c2: rangeCount=105
  c3: rangeCount=110
  c4: rangeCount=115
  c5: rangeCount=120

Uniform Random:
  - 每个候选被选中的概率: 1/5 = 20%
  - 期望 rangeCount: (100 + 105 + 110 + 115 + 120) / 5 = 110

Power of Two Random Choices:
  - 随机选 2 个,选其中 rangeCount 较小的
  - 期望 rangeCount: 约 105 (比均匀随机低 5)

  计算过程:
    选中 (c1, c2): 选 c1 (100)
    选中 (c1, c3): 选 c1 (100)
    选中 (c1, c4): 选 c1 (100)
    选中 (c1, c5): 选 c1 (100)
    选中 (c2, c3): 选 c2 (105)
    选中 (c2, c4): 选 c2 (105)
    选中 (c2, c5): 选 c2 (105)
    选中 (c3, c4): 选 c3 (110)
    选中 (c3, c5): 选 c3 (110)
    选中 (c4, c5): 选 c4 (115)

    期望: (100*4 + 105*3 + 110*2 + 115*1) / 10 = 105.5

结论:
  - Power of Two 比 Uniform Random 更倾向于选择 rangeCount 低的候选
  - 加速负载均衡收敛
  - 同时保持随机性,避免羊群效应
```

**为什么选择 2 而非 3 或更多?**

```
研究表明 (The Power of Two Random Choices, Mitzenmacher 1996):
  - N=2: 显著改善(指数级改善最大负载)
  - N=3: 改善幅度递减
  - N>3: 边际收益很小,不值得额外计算

CockroachDB 的权衡:
  - N=2: 只需要 1 次比较,成本低
  - 效果显著: 最大负载从 O(log n) 降低到 O(log log n)
  - 简单实现: 代码简洁,易于理解
```

### 4.3 完整示例: 从候选到最终选择

```
场景: 100 个 Range 需要添加副本,每个 Range 独立运行 Allocator

步骤 1: rankedCandidateListForAllocation()
  输入: 所有 Store
  输出: 排序后的候选列表
    [s1, s2, s3, s4, s5, s6, ...]

步骤 2: best()
  输入: [s1, s2, s3, s4, s5, s6, ...]
  处理:
    - s1: necessary=true, diversityScore=0.95, convergesScore=1, balanceScore=2, rangeCount=100
    - s2: necessary=true, diversityScore=0.95, convergesScore=1, balanceScore=2, rangeCount=105
    - s3: necessary=true, diversityScore=0.95, convergesScore=1, balanceScore=2, rangeCount=110
    - s4: necessary=true, diversityScore=0.90, convergesScore=1, balanceScore=2, rangeCount=95

    等价性判定:
      - s2 vs s1: 所有维度等价 ✓
      - s3 vs s1: 所有维度等价 ✓
      - s4 vs s1: diversityScore 不等价(0.90 vs 0.95) ✗

  输出: equivalenceClass = [s1, s2, s3]

步骤 3: selectBest() - Power of Two Random Choices
  对于每个 Range(假设 100 个 Range):

  Range 1:
    - 随机排列: order = [2, 0, 1] → [s3, s1, s2]
    - 取前 2 个: s3, s1
    - 比较: s1.rangeCount(100) < s3.rangeCount(110)
    - 选择: s1

  Range 2:
    - 随机排列: order = [1, 2, 0] → [s2, s3, s1]
    - 取前 2 个: s2, s3
    - 比较: s2.rangeCount(105) < s3.rangeCount(110)
    - 选择: s2

  Range 3:
    - 随机排列: order = [0, 1, 2] → [s1, s2, s3]
    - 取前 2 个: s1, s2
    - 比较: s1.rangeCount(100) < s2.rangeCount(105)
    - 选择: s1

  ... (继续 97 个 Range)

步骤 4: 统计最终选择分布
  假设 100 个 Range 的选择结果:
    - s1 被选中: 45 次 (rangeCount 最低,被选中概率最高)
    - s2 被选中: 35 次 (rangeCount 中等)
    - s3 被选中: 20 次 (rangeCount 最高,被选中概率最低)

对比 Uniform Random 的期望分布:
  - s1, s2, s3 各被选中 33 次

Power of Two 的优势:
  - s1 获得更多副本 → rangeCount 增加到 145
  - s2 获得中等副本 → rangeCount 增加到 140
  - s3 获得较少副本 → rangeCount 增加到 130
  - 负载更均衡,收敛更快
```

---

## 五、关键设计决策分析

### 5.1 为什么 diversityScore 使用 scoresAlmostEqual 而非严格相等?

```go
scoresAlmostEqual(cl[i].diversityScore, cl[0].diversityScore)
```

**原因**:

```
diversityScore 是 float64 类型,涉及浮点数计算:

示例:
  s1.diversityScore = 0.8333333333333334  (计算: (1.0 + 0.5 + 1.0) / 3)
  s2.diversityScore = 0.8333333333333333  (计算: (0.75 + 1.0 + 0.75) / 3)

  差异: 0.0000000000000001

如果使用严格相等 (==):
  - s1 和 s2 会被认为不等价
  - 等价类退化为 [s1]
  - 失去随机化的意义

使用 scoresAlmostEqual (epsilon = 1e-10):
  - s1 和 s2 被认为等价 (差异 < epsilon)
  - 等价类 = [s1, s2]
  - 保持随机化,避免羊群效应

trade-off:
  - 优点: 容忍浮点数误差,扩大等价类,增加随机性
  - 缺点: 可能将"确实略有差异"的候选视为等价

CockroachDB 的选择:
  - epsilon = 1e-10 非常小
  - 只容忍计算误差,不容忍实际差异
  - 合理的工程权衡
```

### 5.2 为什么 rangeCount 不使用 scoresAlmostEqual?

```go
// 在 compare() 函数中
if c.rangeCount < o.rangeCount {
    return float64(o.rangeCount-c.rangeCount) / float64(o.rangeCount)
}
```

**原因**:

```
rangeCount 是整数类型,不存在浮点数误差:
  - c1.rangeCount = 100
  - c2.rangeCount = 101

  差异: 1 (明确的整数差异)

如果使用 scoresAlmostEqual:
  - 需要定义"多少差异算等价"? 1? 5? 10?
  - 不同集群规模下,合理的差异阈值不同
  - 难以找到通用的阈值

当前实现:
  - rangeCount 用于打破平局,不参与等价性判定
  - 在 compare() 中使用相对差异: (o.rangeCount - c.rangeCount) / o.rangeCount
  - 合理反映负载差异

示例:
  c1.rangeCount = 100, c2.rangeCount = 101
  相对差异: (101 - 100) / 101 = 0.0099 (0.99%)

  c1.rangeCount = 1000, c2.rangeCount = 1001
  相对差异: (1001 - 1000) / 1001 = 0.001 (0.1%)

  相对差异反映了"差异的重要性"
```

### 5.3 为什么等价类提取在 selectBest() 中而非 rankedCandidateListForAllocation() 中?

**当前实现**:

```
rankedCandidateListForAllocation()
  ↓ 返回完整的排序列表
selectBest()
  ↓ cl.best() 提取等价类
  ↓ Power of Two 随机选择
```

**替代方案**:

```
rankedCandidateListForAllocation()
  ↓ 内部调用 best() 提取等价类
  ↓ 返回等价类
selectBest()
  ↓ 直接从等价类中随机选择
```

**为什么选择当前实现?**

```
职责分离:
  - rankedCandidateListForAllocation(): 负责"筛选和排序"
  - best(): 负责"提取等价类"
  - selectBest(): 负责"随机选择"
  - 每个函数职责明确,易于理解和测试

灵活性:
  - 调用者可以选择使用 best() 或 good()
  - 例如: Recovery 场景使用 good(),正常 Allocation 使用 best()
  - 如果等价类提取在 rankedCandidateListForAllocation() 中,就失去了这种灵活性

可测试性:
  - best() 可以独立测试
  - 测试用例可以直接构造 candidateList,调用 best(),验证结果
  - 不需要构造完整的 Store 环境

性能:
  - best() 的时间复杂度 O(k),k 是等价类大小,通常很小 (< 10)
  - 提前提取等价类不会带来显著的性能提升
  - 反而增加了代码复杂度
```

### 5.4 为什么 onlyValidAndHealthyDisk() 从后往前扫描?

```go
for i := len(cl) - 1; i >= 0; i-- {
    if cl[i].valid && !cl[i].fullDisk && !cl[i].ioOverloaded {
        return cl[:i+1]
    }
}
```

**原因**:

```
排序保证:
  - 候选列表按分数降序排列
  - valid > !valid
  - !fullDisk > fullDisk
  - !ioOverloaded > ioOverloaded
  - 所以: 不合法的候选会集中在尾部

从后往前扫描的效率:
  - 最坏情况: 所有候选都合法,扫描整个列表,O(n)
  - 最好情况: 最后一个候选不合法,扫描 1 个元素,O(1)
  - 平均情况: 假设 10% 的候选不合法,集中在尾部,扫描约 10% 的列表,O(0.1n)

从前往后扫描的效率:
  - 最坏情况: 所有候选都合法,扫描整个列表,O(n)
  - 最好情况: 第一个候选不合法,扫描 1 个元素,O(1)
  - 平均情况: 需要找到"第一个不合法的",可能在中间位置,O(0.5n)

实际情况:
  - 大多数情况下,候选列表中不合法的候选很少
  - 不合法的候选集中在尾部
  - 从后往前扫描更高效

示例:
  列表: [c1(valid), c2(valid), c3(valid), ..., c98(valid), c99(invalid), c100(fullDisk)]

  从后往前: 扫描 c100, c99,找到 c98,返回 cl[:99],扫描 2 次
  从前往后: 扫描 c1, c2, ..., c98, c99,找到 c99,返回 cl[:99],扫描 99 次
```

---

## 六、性能分析

### 6.1 时间复杂度

```
best() 函数:
  1. onlyValidAndHealthyDisk(): O(n)
     - 最坏情况: 扫描整个列表
     - 实际情况: 通常只扫描末尾几个元素,O(1) ~ O(10)

  2. 等价性判定循环: O(k)
     - k 是等价类大小
     - 最坏情况: k = n (所有候选都等价)
     - 实际情况: k << n,通常 k ≈ 2~5

  总时间复杂度: O(n)
  实际运行时间: 非常快,通常 < 1ms
```

### 6.2 空间复杂度

```
best() 函数:
  - 不分配额外内存
  - 返回原列表的切片(slice),共享底层数组
  - 空间复杂度: O(1)
```

### 6.3 性能优化技巧

**技巧 1: 早期返回**

```go
if len(cl) <= 1 {
    return cl  // 不需要等价性判定
}
```

**技巧 2: 切片共享**

```go
return cl[:i]  // 不复制,直接返回切片
```

**技巧 3: scoresAlmostEqual 的快速路径**

```go
func scoresAlmostEqual(score1, score2 float64) bool {
    return math.Abs(score1-score2) < epsilon  // 避免除法
}
```

---

## 七、常见问题与调试

### 7.1 问题 1: 等价类总是只有一个元素

**现象**: `best()` 总是返回 `[c1]`,无法分散负载。

**原因**: 第一名候选在某个维度上独一无二。

**排查步骤**:

1. **打印等价性判定过程**:
   ```go
   for i := 1; i < len(cl); i++ {
       log.Infof(ctx, "Comparing c%d with c0:", i)
       log.Infof(ctx, "  necessary: %t == %t? %t", cl[i].necessary, cl[0].necessary, cl[i].necessary == cl[0].necessary)
       log.Infof(ctx, "  voterNecessary: %t == %t? %t", cl[i].voterNecessary, cl[0].voterNecessary, cl[i].voterNecessary == cl[0].voterNecessary)
       log.Infof(ctx, "  diversityScore: %.4f ≈ %.4f? %t", cl[i].diversityScore, cl[0].diversityScore, scoresAlmostEqual(cl[i].diversityScore, cl[0].diversityScore))
       log.Infof(ctx, "  convergesScore: %d == %d? %t", cl[i].convergesScore, cl[0].convergesScore, cl[i].convergesScore == cl[0].convergesScore)
       log.Infof(ctx, "  balanceScore: %d == %d? %t", cl[i].balanceScore, cl[0].balanceScore, cl[i].balanceScore == cl[0].balanceScore)
       log.Infof(ctx, "  hasNonVoter: %t == %t? %t", cl[i].hasNonVoter, cl[0].hasNonVoter, cl[i].hasNonVoter == cl[0].hasNonVoter)
   }
   ```

2. **检查是否存在约束满足度差异**:
   - 如果 `c1.necessary=true, c2.necessary=false`,说明 c1 满足未满足的约束
   - 这种情况下,c1 确实应该被优先选择
   - 等价类退化为 `[c1]` 是合理的

3. **检查 diversityScore 差异**:
   - 如果 `c1.diversityScore=1.0, c2.diversityScore=0.95`,差异 0.05 >> epsilon
   - 说明 c1 在分散性上明显优于 c2
   - 等价类退化是合理的

**解决方案**:

- 如果确实存在唯一的最优候选,这是合理行为,不需要修复
- 如果希望增加等价类大小,可以考虑放宽等价性标准(使用 `good()` 而非 `best()`)

### 7.2 问题 2: 等价类包含所有候选

**现象**: `best()` 返回整个候选列表,没有筛选效果。

**原因**: 所有候选在关键维度上完全等价。

**排查步骤**:

1. **检查候选的 diversityScore**:
   ```
   如果所有候选都在同一 region:
     - diversityScore 都是 0.0(没有分散性)
     - 等价类包含所有候选
   ```

2. **检查 balanceScore**:
   ```
   如果所有候选的 RangeCount 都接近平均值:
     - balanceScore 都是 aroundTheMean(1)
     - 等价类包含所有候选
   ```

**解决方案**:

- 这种情况下,随机选择是合理的(因为所有候选确实等价)
- Power of Two 会倾向于选择 rangeCount 较低的候选,加速收敛

### 7.3 问题 3: Power of Two 没有生效

**现象**: 所有 Range 都选择相同的候选,没有分散。

**原因**: 随机数生成器不是真正随机。

**排查步骤**:

1. **检查 randGen 是否正确初始化**:
   ```go
   // 正确:
   randGen := makeAllocatorRand(rand.New(rand.NewSource(timeutil.Now().UnixNano())))

   // 错误:
   randGen := makeAllocatorRand(rand.New(rand.NewSource(0)))  // 固定种子
   ```

2. **检查是否在测试模式**:
   ```go
   if options.deterministicForTesting() {
       sort.Sort(sort.Reverse(byScoreAndID(candidates)))  // 使用 StoreID 排序,确定性
   }
   ```

**解决方案**:

- 确保 randGen 使用时间戳或其他真正随机的种子
- 生产环境不要启用 `deterministicForTesting`

---

## 八、设计模式识别

### 8.1 策略模式(Strategy Pattern)

**定义**: 定义一系列算法,把它们封装起来,并使它们可以相互替换。

**在 best()/good()/worst() 中的应用**:

```go
// 策略接口(隐式)
type candidateExtractor func(candidateList) candidateList

// 具体策略 1: best()
func (cl candidateList) best() candidateList {
    // 提取"最优"等价类,6 维度等价
}

// 具体策略 2: good()
func (cl candidateList) good() candidateList {
    // 提取"足够好"等价类,3 维度等价
}

// 具体策略 3: worst()
func (cl candidateList) worst() candidateList {
    // 提取"最差"等价类,5 维度等价(从尾部)
}

// 使用策略
func (cl candidateList) selectBest(randGen allocatorRand) *candidate {
    cl = cl.best()  // 使用 best 策略
    // ...
}

func (cl candidateList) selectGood(randGen allocatorRand) *candidate {
    cl = cl.good()  // 使用 good 策略
    // ...
}
```

### 8.2 模板方法模式(Template Method Pattern)

**定义**: 在父类中定义算法的骨架,将某些步骤延迟到子类实现。

**在 best()/good()/worst() 中的应用**:

```go
// 模板骨架(伪代码)
func extractEquivalenceClass(cl candidateList, checkEquivalence func(c1, c2 candidate) bool) candidateList {
    // 第一步: 过滤非法候选
    cl = cl.onlyValidAndHealthyDisk()
    if len(cl) <= 1 {
        return cl
    }

    // 第二步: 等价性判定(具体步骤由子类定义)
    for i := 1; i < len(cl); i++ {
        if !checkEquivalence(cl[i], cl[0]) {
            return cl[:i]
        }
    }
    return cl
}

// 具体实现 1: best()
func (cl candidateList) best() candidateList {
    return extractEquivalenceClass(cl, func(c1, c2 candidate) bool {
        return c1.necessary == c2.necessary &&
               c1.voterNecessary == c2.voterNecessary &&
               scoresAlmostEqual(c1.diversityScore, c2.diversityScore) &&
               c1.convergesScore == c2.convergesScore &&
               c1.balanceScore == c2.balanceScore &&
               c1.hasNonVoter == c2.hasNonVoter
    })
}

// 具体实现 2: good()
func (cl candidateList) good() candidateList {
    return extractEquivalenceClass(cl, func(c1, c2 candidate) bool {
        return c1.necessary == c2.necessary &&
               c1.voterNecessary == c2.voterNecessary &&
               scoresAlmostEqual(c1.diversityScore, c2.diversityScore)
    })
}
```

### 8.3 等价类(Equivalence Class)模式

**定义**: 将集合中的元素按照某种等价关系划分为若干个不相交的子集。

**在 best() 中的应用**:

```
原始集合: candidates = [c1, c2, c3, c4, c5, c6]

等价关系: "在 6 个维度上完全等价"
  - necessary 相同
  - voterNecessary 相同
  - diversityScore 相近(< epsilon)
  - convergesScore 相同
  - balanceScore 相同
  - hasNonVoter 相同

划分结果:
  等价类 1: [c1, c2, c3]  (在 6 个维度上等价)
  等价类 2: [c4, c5]      (在 6 个维度上等价,但与等价类 1 不等价)
  等价类 3: [c6]          (独立一类)

best() 的目标: 提取"第一个等价类"(分数最高的等价类)
  → 返回 [c1, c2, c3]
```

---

## 九、工程权衡分析

### 9.1 等价性标准的宽松程度

**当前实现**: `best()` 使用 6 维度等价,`good()` 使用 3 维度等价。

**替代方案 1**: 所有场景都使用 6 维度等价(最严格)

| 维度 | 当前实现 | 替代方案 1 | 权衡 |
|------|----------|-----------|------|
| 决策质量 | ✓ best() 质量高,good() 质量中等 | ✓ 所有场景质量都高 | 当前实现更灵活 |
| 选择速度 | ✓ good() 等价类大,选择快 | ✗ 等价类小,选择慢 | 当前实现在 Recovery 场景更快 |
| 负载分散 | ✓ good() 等价类大,分散好 | ✗ 等价类小,分散差 | 当前实现避免热点 |

**替代方案 2**: 所有场景都使用 3 维度等价(最宽松)

| 维度 | 当前实现 | 替代方案 2 | 权衡 |
|------|----------|-----------|------|
| 决策质量 | ✓ best() 质量高 | ✗ 质量中等,可能次优 | 当前实现保证正常 Allocation 质量 |
| 收敛速度 | ✓ best() 考虑 convergesScore,收敛快 | ✗ 不考虑收敛,收敛慢 | 当前实现加速负载均衡 |

**CockroachDB 的选择**: 当前实现,根据场景选择策略。

### 9.2 Power of Two vs Uniform Random

| 维度 | Power of Two | Uniform Random | CockroachDB 的选择 |
|------|--------------|----------------|-------------------|
| 计算成本 | 2 次比较 | 0 次比较 | Power of Two(成本可接受) |
| 负载均衡 | 指数级改善最大负载 | 无改善 | Power of Two(显著提升) |
| 随机性 | 保持随机性,避免热点 | 完全随机 | Power of Two(平衡) |
| 代码复杂度 | 稍复杂(+10 行代码) | 简单(1 行代码) | Power of Two(复杂度可接受) |

**数学支持**:
- 研究表明 Power of Two 将最大负载从 O(log n) 降低到 O(log log n)
- 以 1000 个 Store 为例: log(1000) ≈ 10, log(log(1000)) ≈ 2.3
- 最大负载改善 4 倍以上

**CockroachDB 的选择**: Power of Two,显著的性能提升值得微小的复杂度增加。

### 9.3 等价类提取位置: selectBest() vs rankedCandidateListForAllocation()

**当前实现**: 等价类提取在 `selectBest()` 中。

**替代方案**: 等价类提取在 `rankedCandidateListForAllocation()` 中。

| 维度 | 当前实现 | 替代方案 | 权衡 |
|------|----------|---------|------|
| 职责分离 | ✓ 每个函数职责明确 | ✗ rankedCandidateListForAllocation() 职责过重 | 当前实现更清晰 |
| 灵活性 | ✓ 调用者可选择 best()/good() | ✗ 策略固定 | 当前实现更灵活 |
| 性能 | ≈ best() 的 O(k) 可忽略 | ≈ 提前提取无性能提升 | 当前实现性能相同 |
| 可测试性 | ✓ best() 可独立测试 | ✗ 需要构造完整环境 | 当前实现更易测试 |

**CockroachDB 的选择**: 当前实现,职责分离和灵活性更重要。

---

## 十、心智模型与类比

### 10.1 招聘面试的类比

```
best() ≈ 从面试候选人中选择"最佳等价类"

场景:
  - 公司招聘软件工程师
  - 收到 100 份简历,筛选后剩 10 个候选
  - 面试后按综合评分排序

候选评分:
  c1: 技术=A, 沟通=A, 经验=5年, 期望薪资=30万
  c2: 技术=A, 沟通=A, 经验=5年, 期望薪资=32万
  c3: 技术=A, 沟通=A, 经验=5年, 期望薪资=35万
  c4: 技术=A, 沟通=B, 经验=5年, 期望薪资=28万
  c5: 技术=B, 沟通=A, 经验=4年, 期望薪资=26万

best() 的逻辑:
  1. 提取"最优等价类":
     - c1, c2, c3 在"技术"、"沟通"、"经验"上等价
     - 只有期望薪资不同,但这是"打破平局"的维度
     - 等价类 = [c1, c2, c3]

  2. Power of Two 随机选择:
     - 从 [c1, c2, c3] 中随机选 2 个
     - 比较期望薪资,选择较低的
     - 倾向于选择 c1(30万)

好处:
  - 保证决策质量(只在"最佳"候选中选择)
  - 避免"所有公司都抢 c1"的竞争(随机化)
  - 加速"薪资趋向市场均衡"(Power of Two)
```

### 10.2 餐厅选址的类比

```
best() ≈ 从候选地段中选择"最优等价类"

场景:
  - 连锁餐厅选择新店地址
  - 候选地段 10 个,按综合评分排序

候选评分:
  地段 1: 人流=高, 租金=高, 交通=便利, 竞争=低, 面积=100㎡
  地段 2: 人流=高, 租金=高, 交通=便利, 竞争=低, 面积=105㎡
  地段 3: 人流=高, 租金=高, 交通=便利, 竞争=低, 面积=110㎡
  地段 4: 人流=高, 租金=高, 交通=便利, 竞争=中, 面积=95㎡
  地段 5: 人流=中, 租金=中, 交通=便利, 竞争=低, 面积=90㎡

best() 的逻辑:
  1. 提取"最优等价类":
     - 地段 1, 2, 3 在"人流"、"租金"、"交通"、"竞争"上等价
     - 只有面积不同,但这是次要因素
     - 等价类 = [地段 1, 2, 3]

  2. Power of Two 随机选择:
     - 从 [地段 1, 2, 3] 中随机选 2 个
     - 比较面积,选择较小的(成本更低)
     - 倾向于选择地段 1(100㎡)

好处:
  - 保证选址质量(只在"最佳"地段中选择)
  - 避免"所有连锁品牌都选地段 1"的竞争(随机化)
  - 优化成本(Power of Two 倾向于选择面积小的)
```

### 10.3 核心直觉

**best() 的本质**:

> 在保证决策质量的前提下,通过等价类和随机化的结合,避免集群级的"羊群效应"。
>
> 关键原则:
> 1. **等价性判定**: 只在关键维度上等价,忽略次要维度
> 2. **提取最优**: 只提取分数最高的等价类,保证质量
> 3. **随机选择**: 在等价类中随机选择,避免热点
> 4. **Power of Two**: 倾向于选择负载低的候选,加速收敛
> 5. **场景适配**: 不同场景使用不同策略(best/good/worst)

---

## 十一、总结

### 11.1 核心要点

1. **等价类的作用**:
   - 提取"在关键维度上完全等价"的候选
   - 保证决策质量(只在"最优"中选择)
   - 增加随机性(避免羊群效应)

2. **等价性判定标准(6 维度)**:
   - necessary: 是否满足未满足的约束
   - voterNecessary: 是否满足 Voter 约束
   - diversityScore: 分散性评分
   - convergesScore: 向均衡收敛评分
   - balanceScore: 负载均衡状态
   - hasNonVoter: 是否可原地升级

3. **两阶段执行流程**:
   - 阶段一: 过滤非法和不健康的候选(onlyValidAndHealthyDisk)
   - 阶段二: 提取等价类(等价性判定循环)

4. **与 selectBest() 的协作**:
   - best() 提取等价类
   - selectBest() 使用 Power of Two Random Choices 选择

5. **设计哲学**:
   - **质量优先**: 只在"最优"候选中选择
   - **避免热点**: 随机化分散负载
   - **加速收敛**: Power of Two 倾向于选择负载低的候选
   - **场景适配**: best()/good()/worst() 适配不同场景

### 11.2 与前序内容的联系

```
rankedCandidateListForAllocation(前一节)
  → 筛选、评分、排序候选 Store
  ↓
best()(本节)
  → 提取等价类
  ↓
selectBest()(配合使用)
  → Power of Two Random Choices
  ↓
最终选择
```

### 11.3 实践建议

1. **理解等价性**: 等价不是"完全相同",而是"在关键维度上足够接近"
2. **场景选择**: Allocation 用 best(),Recovery 用 good(),Removal 用 worst()
3. **调试技巧**: 打印等价性判定过程,理解为什么某些候选不等价
4. **性能考虑**: best() 的性能开销可忽略,不要过度优化
5. **随机性**: 确保 randGen 使用真正随机的种子,生产环境避免固定种子

---

**至此,`best()` 函数的深度剖析完成。本节通过两阶段执行流程、等价性判定标准、与 good()/worst() 的对比、Power of Two Random Choices 的协作,帮助读者理解 CockroachDB 如何在保证决策质量的前提下,通过等价类和随机化避免集群级的羊群效应,实现高效的副本放置决策。**
