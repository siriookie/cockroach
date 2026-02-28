# SpanConfigStore.Apply 深度解析：区间树的维护与 B-Tree 选型分析

> **分析文件**：`pkg/spanconfig/spanconfigstore/store.go` + `span_store.go` + `system_store.go` + `entry_interval_btree.go` + `interner.go`
> **方法论**：BFS 控制流 → DFS 关键函数 → 具体运行示例 → B-Tree vs 跳表深度对比

---

## 一、背景：SpanConfig 与 SpanConfigStore 是什么？

### 1.1 SpanConfig 的作用

在 CockroachDB 中，每个 KV key range（Range）都有一组**配置属性**，统称为 `SpanConfig`。它控制：

- **GC TTL**：MVCC 历史保留时间
- **Replication Factor**：副本数量（3/5/7）
- **Lease Preferences**：Lease 偏好（如"尽量放在某个区域"）
- **Constraints**：副本放置约束
- **Protected Timestamps**：防止 GC 删除某时间点之前的数据

每个用户表、系统表、甚至整个 keyspace 都可以有自己的 `SpanConfig`。整个集群的 `SpanConfig` 构成了一张"覆盖整个 keyspace 的无重叠区间配置地图"。

### 1.2 SpanConfigStore 在架构中的位置

```
┌─────────────────────────────────────────────────────────┐
│  spanconfigmanager / spanconfigkvsubscriber              │  变更来源
│  （监听 span_configurations 系统表）                      │
├─────────────────────────────────────────────────────────┤
│  spanconfigstore.Store  ← 本文分析                        │  内存状态维护
│    ├── spanConfigStore  (Augmented B-Tree)               │
│    └── systemSpanConfigStore  (flat HashMap)             │
├─────────────────────────────────────────────────────────┤
│  kvserver.Replica.GetSpanConfigForKey()                  │  读取方（高频）
│  kvserver.StoreReplicaVisitor.NeedsSplit()               │
│  kvserver.allocator                                      │
└─────────────────────────────────────────────────────────┘
```

**Store 的核心 invariant**：`spanConfigStore` 内部的 B-Tree 中，任意两个 entry 的 span 不重叠，且共同覆盖配置地图中所有已配置的区间。

---

## 二、第一轮 BFS：Store.Apply 的控制流

### 2.1 数据结构全景

```
Store
├── mu.RWMutex
├── mu.spanConfigStore          ← 核心：存储 span → SpanConfig 的 B-Tree
│     ├── btree *btree          ← augmented interval B-Tree
│     ├── interner *interner    ← SpanConfig 对象池（引用计数）
│     └── treeIDAlloc uint64    ← 唯一 ID 分配器
│
├── mu.systemSpanConfigStore    ← 存储系统级配置（如 PTS）的 HashMap
│     └── store map[SystemTarget]SpanConfig
│
├── fallback roachpb.SpanConfig ← 兜底配置（无显式配置时使用）
├── boundsReader BoundsReader   ← secondary tenant 配置边界
└── settings *cluster.Settings  ← 动态集群设置
```

### 2.2 Apply 的完整执行路径

```
Store.Apply(ctx, updates...)
    │
    ├── 1. maybeLogUpdate() × N      [记录非 PTS 的配置变更日志]
    │
    └── 2. applyInternal(ctx, updates...)
            │
            ├── 3. 分类 updates
            │     ├── IsSpanTarget()   → spanStoreUpdates
            │     └── IsSystemTarget() → systemSpanConfigStoreUpdates
            │
            ├── 4. spanConfigStore.apply(ctx, spanStoreUpdates...)
            │     │
            │     ├── 4a. validateApplyArgs()   [校验 + 排序]
            │     ├── 4b. accumulateOpsFor()    [核心！计算 toDelete + toAdd]
            │     └── 4c. 执行删除 + 插入
            │           ├── btree.Delete(entry)
            │           ├── interner.remove(canonical)
            │           └── btree.Set(entry)
            │
            └── 5. systemSpanConfigStore.apply(systemSpanConfigStoreUpdates...)
                      └── 直接操作 HashMap（简单 CRUD）
```

### 2.3 触发方式分析

| 维度 | 说明 |
|---|---|
| **来源** | `spanconfigkvsubscriber` 通过 RangeFeed 监听系统表变更，收到事件后调用 `Store.Apply` |
| **频率** | 低频（相对读取而言）：只有 DDL 操作、配置变更、PTS 操作时才触发 |
| **并发语义** | `Apply` 持 **写锁**（`mu.Lock`）；读操作（`GetSpanConfigForKey` 等）持**读锁**（`mu.RLock`）|
| **批量性** | `updates` 是一批变更，设计为批量应用以减少锁竞争次数 |

### 2.4 关键共享状态

```
Apply (写路径)                    GetSpanConfigForKey (读路径，高频！)
    │                                         │
    ├── mu.Lock()                              ├── mu.RLock()
    │                                         │
    ├── btree.Delete/Set          ←──────────→├── btree.MakeIter().FirstOverlap()
    ├── interner.add/remove                   └── systemSpanConfigStore.combine()
    └── mu.Unlock()
```

读写分离通过 `syncutil.RWMutex` 实现：**多个读操作可以并发**，但写操作是排他的。这在 SpanConfig 的使用场景（低频写、高频读）下是最优选择。

---

## 三、DFS 深入：关键函数逐层解析

### 3.1 `applyInternal()` — 分类派发

**文件**：[store.go:250-303](pkg/spanconfig/spanconfigstore/store.go#L250)

```go
func (s *Store) applyInternal(
    ctx context.Context, updates ...spanconfig.Update,
) (deleted []spanconfig.Target, added []spanconfig.Record, err error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    spanStoreUpdates := make([]spanconfig.Update, 0, len(updates))
    systemSpanConfigStoreUpdates := make([]spanconfig.Update, 0, len(updates))
    for _, update := range updates {
        switch {
        case update.GetTarget().IsSpanTarget():
            spanStoreUpdates = append(spanStoreUpdates, update)
        case update.GetTarget().IsSystemTarget():
            systemSpanConfigStoreUpdates = append(systemSpanConfigStoreUpdates, update)
        default:
            return nil, nil, errors.AssertionFailedf("unknown target type")
        }
    }
    // ... 分别 apply 两类更新
}
```

**设计要点**：
1. 整个函数持**全局写锁**。若批量 apply 可以将多次 RPC 回调的更新合并，锁开销摊薄。
2. **两类存储后端完全解耦**：`spanConfigStore` 用 B-Tree（因为需要范围查询），`systemSpanConfigStore` 用 HashMap（因为 key 是 `SystemTarget`，点查即可）。
3. `errors.AssertionFailedf` → 若出现 panic，调用方 `Apply()` 直接 `log.Dev.Fatalf`（致命日志 + 进程退出）。这说明该错误**不应该**在生产环境发生。

---

### 3.2 `spanConfigStore.apply()` — B-Tree 修改入口

**文件**：[span_store.go:243-278](pkg/spanconfig/spanconfigstore/span_store.go#L243)

```go
func (s *spanConfigStore) apply(
    ctx context.Context, updates ...spanconfig.Update,
) (deleted []roachpb.Span, added []entry, err error) {
    if err := validateApplyArgs(updates...); err != nil {
        return nil, nil, err
    }

    // 1. 按 start key 排序
    sorted := make([]spanconfig.Update, len(updates))
    copy(sorted, updates)
    sort.Slice(sorted, func(i, j int) bool {
        return sorted[i].GetTarget().Less(sorted[j].GetTarget())
    })
    updates = sorted

    // 2. 只读遍历，计算需要 delete 的和需要 add 的 entry
    entriesToDelete, entriesToAdd, err := s.accumulateOpsFor(ctx, updates)
    if err != nil { return nil, nil, err }

    // 3. 执行实际的 B-Tree 变更
    for i := range entriesToDelete {
        entry := &entriesToDelete[i]
        s.btree.Delete(entry)
        s.interner.remove(ctx, entry.canonical)
        deleted[i] = entry.span
    }
    for i := range entriesToAdd {
        entry := &entriesToAdd[i]
        s.btree.Set(entry)      // interner 在 makeEntry 中已处理
        added[i] = *entry
    }
    return deleted, added, nil
}
```

**关键设计**：**两阶段提交（Two-Phase Application）**

- **阶段 1**（`accumulateOpsFor`）：只读遍历 B-Tree，计算出所有需要操作的 entry。此阶段**不修改** B-Tree。
- **阶段 2**：集中执行所有 Delete 和 Set 操作。

为什么分两阶段？因为 `accumulateOpsFor` 中的 B-Tree 迭代器是有状态的，若在迭代途中修改 B-Tree，迭代器行为未定义（可能跳过或重复节点）。

**排序的必要性**：`accumulateOpsFor` 的 `carry-over` 机制依赖 updates 按 start key 单调递增，若无序则 carry-over 逻辑会错误地携带不相关的段。

---

### 3.3 `accumulateOpsFor()` — 核心：区间分割与合并

**文件**：[span_store.go:280-483](pkg/spanconfig/spanconfigstore/span_store.go#L280)

这是整个模块中最复杂、最核心的函数。其核心问题是：

> 给定一组新的 `(span, config)` 更新，如何最小化对 B-Tree 的修改，同时保持"所有 span 不重叠"的不变量？

#### 3.3.1 单个 Update 的处理（基础情形）

代码注释中的伪代码（span_store.go:287-303）已经给出了清晰的逻辑：

```
for entry in btree.overlapping(update.span):
    union = union(update.span, entry.span)       # 包含两者的最大范围
    inter = intersect(update.span, entry.span)   # 两者重叠的部分
    pre  = span{union.start, inter.start}        # update 左侧保留部分
    post = span{inter.end, union.end}            # update 右侧保留部分

    delete {entry.span, entry.conf}              # 删除整个 existing entry

    if entry 包含 update 的起始 key:
        add {pre, entry.conf}   if pre 非空      # 左侧残留，保留 entry 的 conf
    if entry 包含 update 的终止 key:
        carry-over = {post, entry.conf}          # 右侧残留，携带到下一个 update

if 是 Addition:
    add {update.span, update.conf}               # 添加新的 entry
```

**图示（单个 update）**：

```
B-Tree 现有状态:
  |[---A---)         [----B----)            [-----C----)|
  a         d        g          k           m             q

新 update: [e, j):NewConf

Step 1: 找到所有与 [e,j) 重叠的 entry: [g,k):B
Step 2: 计算
  union = [e,k), inter = [g,j), pre = [e,g), post = [j,k)
  → delete [g,k):B
  → B 不包含 e（e < g），所以不 add pre [e,g)
  → B 包含 j（g ≤ j < k），所以 carry-over = [j,k):B

Step 3: update 是 Addition
  → add [e,j):NewConf

最终状态:
  |[---A---)  [e,j):NewConf  [j,k):B  [-----C----)|
  a         d e              j         k m          q
```

#### 3.3.2 多个 Updates 的处理（carry-over 机制）

多个 updates 同时 apply 时，关键问题是：一个 existing entry 可能同时与多个 updates 重叠。

```go
// span_store.go:376-399
var carryOver spanConfigPair
for _, update := range updates {
    var carriedOver spanConfigPair
    carriedOver, carryOver = carryOver, spanConfigPair{}

    if update.GetTarget().GetSpan().Overlaps(carriedOver.span) {
        // 填充上一个 update 和本 update 之间的 gap
        gapBetweenUpdates := roachpb.Span{
            Key:    carriedOver.span.Key,
            EndKey: update.GetTarget().GetSpan().Key,
        }
        if gapBetweenUpdates.Valid() {
            toAdd = append(toAdd, s.makeEntry(ctx, gapBetweenUpdates, carriedOver.config))
        }
        // 计算本 update 之后 carriedOver 剩余的部分
        carryOverSpanAfterUpdate := roachpb.Span{
            Key:    update.GetTarget().GetSpan().EndKey,
            EndKey: carriedOver.span.EndKey,
        }
        if carryOverSpanAfterUpdate.Valid() {
            carryOver = spanConfigPair{span: carryOverSpanAfterUpdate, config: carriedOver.config}
        }
    } else if !carriedOver.isEmpty() {
        // carriedOver 与本 update 不重叠，直接添加
        toAdd = append(toAdd, s.makeEntry(ctx, carriedOver.span, carriedOver.config))
    }

    // 处理当前 update 的重叠 entry...
    iter, query := s.btree.MakeIter(), makeQueryEntry(update.GetTarget().GetSpan())
    for iter.FirstOverlap(query); iter.Valid(); iter.NextOverlap(query) {
        existing := iter.Cur()
        if existing.span.Overlaps(carriedOver.span) {
            continue // 已在 carriedOver 处理阶段处理过，跳过
        }
        // ... 同单个 update 的处理逻辑
    }
}
```

**carry-over 的本质**：它是一个"游标"，携带上一个 update 处理后残留的右半段（`post`）。因为我们没有立即将其写入 B-Tree，所以在处理下一个 update 时，我们需要先检查这个残留段是否与新 update 重叠，并相应地截断它。

**图示（多个 updates）**：

```
keyspace:   a  b  c  d  e  f  g  h
existing:      [-------X-------)
updates:    [--A--)         [--B--)

处理 [a,c):A:
  与 [b,h):X 重叠
  → delete [b,h):X
  → X 包含 a? No (b>a)，所以不 add pre
  → X 包含 c? Yes，carry-over = [c,h):X

  add [a,c):A

处理 [f,h):B:
  carriedOver = [c,h):X
  [f,h):B 与 [c,h):X 重叠 → 填充 gap [c,f):X，add 之
  carry-over after [f,h) = [h,h) → 无效，carryOver = empty

  遍历 btree 中与 [f,h) 重叠的 entry:
  → [b,h):X 已在 carriedOver 中处理（existing.span.Overlaps(carriedOver.span)=true），跳过

  add [f,h):B

最终状态: [a,c):A, [c,f):X, [f,h):B
```

#### 3.3.3 peep-hole 优化

```go
// span_store.go:418-423
if update.Addition() {
    if existing.span.Equal(update.GetTarget().GetSpan()) && existingConf.Equal(update.GetConfig()) {
        skipAddingSelf = true
        break // no-op; peep-hole optimization
    }
}
```

若新 update 的 span 和 config 与现有 entry **完全相同**，则跳过所有操作（no-op）。这避免了大量无意义的 delete + re-add。在实际中，配置不变但时间推进导致的 span_configurations 表刷新可能会产生大量这样的 no-op update。

---

### 3.4 `interner` — SpanConfig 对象池与指针比较优化

**文件**：[interner.go](pkg/spanconfig/spanconfigstore/interner.go)

```go
type interner struct {
    configToCanonical map[spanConfigKey]*roachpb.SpanConfig  // 序列化key → 规范指针
    refCounts         map[*roachpb.SpanConfig]uint64          // 指针 → 引用计数
}
```

**工作原理**：

```go
// interner.go:34-48
func (i *interner) add(ctx context.Context, conf roachpb.SpanConfig) *roachpb.SpanConfig {
    marshalled, _ := protoutil.Marshal(&conf)
    if canonical, found := i.configToCanonical[spanConfigKey(marshalled)]; found {
        i.refCounts[canonical]++
        return canonical  // 返回已有的规范指针
    }
    i.configToCanonical[spanConfigKey(marshalled)] = &conf
    i.refCounts[&conf] = 1
    return &conf
}
```

`interner` 的价值：

1. **内存去重**：`SpanConfig` 是一个重量级 protobuf（包含副本约束、GC 策略、PTS 等），若每个 entry 都独立存储一份，内存开销巨大。大多数 Range 共享相同配置（如同一租户的所有 Range 默认使用 RANGE DEFAULT），interner 让它们共享同一个对象。

2. **快速比较**：`spanConfigPairInterned.canonical` 是指针。两个 entry 如果有相同的配置，它们的 `canonical` 指针**相同**。代码中大量使用指针相等（而不是 protobuf 字段逐一比较）来快速判断配置是否相同：

```go
// span_store.go:187 - 计算 split key 时的配置相等判断
if firstMatch.canonical != match.canonical || ...
```

3. **引用计数释放**：当 entry 从 B-Tree 删除时，`interner.remove` 减少引用计数；归零时才释放内存（从两个 map 中删除），避免悬空指针。

---

### 3.5 `augmented interval B-Tree` — 区间重叠查询的物理支撑

**文件**：[entry_interval_btree.go](pkg/spanconfig/spanconfigstore/entry_interval_btree.go)

#### 3.5.1 B-Tree + Augmentation：节点结构

```go
// entry_interval_btree.go:96-111
type node struct {
    ref   int32     // 引用计数（用于 Copy-on-Write）
    count int16     // 当前节点存储的 item 数

    maxKey []byte   // ← Augmentation! 该子树中所有 item 的最大 EndKey
    maxInc bool     // maxKey 是否 inclusive

    items    [maxItems]*entry  // maxItems = 2*degree-1 = 31
    children *childrenArray    // 内部节点才有，叶节点为 nil
}
```

**Augmentation**（增强）是区间树的核心：每个节点额外维护其子树中所有区间的**最大上界**（`maxKey`）。这使得区间重叠查询可以剪枝——若一个子树的 `maxKey < query.start`，那么该子树中**不可能**有与 query 重叠的区间，可以直接跳过。

这一设计直接来源于 **CLRS 第 14 章（Augmenting Data Structures）** 中的区间树算法，代码注释中明确引用了这一来源：

> // btree stores items in an ordered structure... It represents intervals and permits
> // an interval search operation **following the approach laid out in CLRS, Chapter 14**.

#### 3.5.2 `maxKey` 的维护：插入与删除时的向上传播

```go
// entry_interval_btree.go:630-644
func (n *node) adjustUpperBoundOnInsertion(item *entry, child *node) bool {
    up := upperBound(item)          // item 自身的 EndKey
    if child != nil {
        if childMax := child.max(); up.compare(childMax) < 0 {
            up = childMax           // 取 child 子树的最大值
        }
    }
    if n.max().compare(up) < 0 {
        n.setMax(up)                // 若新值更大，更新当前节点的 maxKey
        return true                 // 返回 true 表示上界发生了变化，调用方需继续向上传播
    }
    return false
}
```

**传播机制**：
- 插入时，从叶节点向上，逐层检查并更新 `maxKey`，直到某层不再变化（`return false`）。
- 删除时，需要重新计算 `maxKey`（可能减小），调用 `findUpperBound()` 遍历当前节点所有 item 和子节点的 max，O(degree) 开销。

```
插入前:
  root[max=j]
  ├── left[max=e]: [b,e) [c,d)
  └── right[max=j]: [f,j) [g,h)

插入 [i,m):
  叶节点 right 插入后: right[max=m]: [f,j) [g,h) [i,m)
  向上传播: root.maxKey = max(e, m) = m → root[max=m]

插入后:
  root[max=m]
  ├── left[max=e]: [b,e) [c,d)
  └── right[max=m]: [f,j) [g,h) [i,m)
```

#### 3.5.3 `FirstOverlap` / `NextOverlap` — 区间重叠查询算法

**文件**：[entry_interval_btree.go:1168-1284](pkg/spanconfig/spanconfigstore/entry_interval_btree.go#L1168)

```go
func (i *iterator) findNextOverlap(item *entry) {
    for {
        if i.pos > i.n.count {
            i.ascend()    // 当前节点遍历完，上升到父节点
        } else if !i.n.leaf() {
            // 判断是否需要下降到子树
            if i.o.inConstrMin || i.n.children[i.pos].max().contains(item) {
                // ↑ 关键剪枝：子树 maxKey >= query.start → 子树可能有重叠，下降
                i.descend(par, pos)
                continue
            }
            // 子树 maxKey < query.start → 整个子树不可能重叠，跳过（剪枝！）
        }
        // 到达叶节点或跳过子树后，检查当前 item 是否真正重叠
        // ...
    }
}
```

**查询的两个约束**：

```go
// constrainMinSearchBounds: 找到第一个 start_key >= query.start 的位置（soft lower bound）
// constrainMaxSearchBounds: 找到第一个 start_key > query.end 的位置（hard upper bound）
```

- **Hard upper bound**（`constrMaxPos`）：一旦 item 的 `start_key > query.end_key`，后面所有 item 的 start_key 更大，不可能与 query 重叠（因为 B-Tree 按 start_key 排序）。这是一个**精确剪枝**，时间复杂度上的保证。
- **Soft lower bound**（`constrMinPos`）：start_key < query.start 的 item **可能**与 query 重叠（如 `[a,z)` 的 start_key=a < query.start，但仍与 query 重叠）。这是"soft"的原因——还需要检查 EndKey。

**重叠判断时机**：遍历到一个 item 时，检查 `item.span.Overlaps(query.span)`，即 `item.start < query.end && item.end > query.start`。

#### 3.5.4 Copy-on-Write Clone — O(1) 的树复制

```go
// entry_interval_btree.go:694-715
func (t *btree) Clone() btree {
    c := *t         // 浅拷贝结构体
    if c.root != nil {
        c.root.incRef()    // 只增加根节点的引用计数！O(1)
    }
    return c
}
```

这是 **持久化数据结构（Persistent Data Structure）** 的经典实现，也称为**结构共享（Structural Sharing）**。

```go
// mut(): 当需要修改节点时，先检查引用计数
func mut(n **node) *node {
    if atomic.LoadInt32(&(*n).ref) == 1 {
        return *n    // 独占 → 直接修改
    }
    c := (*n).clone()      // 共享 → Copy-on-Write，克隆当前节点
    (*n).decRef(true)
    *n = c
    return *n
}
```

**Clone 的实际效果**：

```
Clone 前: Tree A (root.ref=1)
          root → [n1, n2, n3]

Clone() 调用后:
  Tree A (root.ref=2)    Tree B (root.ref=2)
  同一个 root →─────────────→ root

对 Tree B 执行修改 (btree.Set):
  mut(&root): ref=2 > 1 → clone root → root_B (ref=1)
  Tree B.root = root_B → [n1, n2, n3_new]
              ↓  (共享)        (新节点)
  Tree A.root (ref=1) → [n1, n2, n3]  # Tree A 不受影响
```

`spanConfigStore.clone()` 中使用 `btree.Clone()` 实现了 **O(1) 快照（Snapshot）**。这对 `Store.Clone()` 方法至关重要——它在不阻塞读写的情况下创建当前状态的完整快照，供 `spanconfigkvsubscriber` 做增量对比。

---

### 3.6 `systemSpanConfigStore.combine()` — 多层配置合并

**文件**：[system_store.go:67-112](pkg/spanconfig/spanconfigstore/system_store.go#L67)

```go
func (s *systemSpanConfigStore) combine(
    key roachpb.RKey, config roachpb.SpanConfig,
) (roachpb.SpanConfig, error) {
    // 确定适用于该 key 的系统配置来源（按优先级）：
    targets := []spanconfig.SystemTarget{
        spanconfig.MakeEntireKeyspaceTarget(),          // 全局配置（如集群级 PTS）
        hostSetOnTenant,                                // host 租户对该 secondary tenant 的配置
    }
    if tenID != roachpb.SystemTenantID {
        targets = append(targets, tenantSelfTarget)     // secondary tenant 自身的配置
    }

    for _, target := range targets {
        systemSpanConfig, found := s.store[target]
        if found {
            // 合并逻辑：目前只合并 ProtectionPolicies（PTS）
            config.GCPolicy.ProtectionPolicies = append(
                config.GCPolicy.ProtectionPolicies,
                systemSpanConfig.GCPolicy.ProtectionPolicies...,
            )
        }
    }
    return config, nil
}
```

**设计思想**：系统级配置（如受保护的时间戳）不与 span-level 配置"覆盖"，而是**叠加（combine）**。一个 Range 的最终 GC 保护策略是所有层级保护策略的并集：只要有任何一层说"不能 GC 这个时间点"，GC 就不能 GC 它。

**三层优先级**（从低到高叠加，而非覆盖）：

```
Range 最终配置 = span config (from B-Tree)
               + system config for entire keyspace
               + system config set by host on this tenant
               + system config set by this tenant on itself
```

---

### 3.7 `getSpanConfigForKeyRLocked()` — 读路径：查询 + 合并 + Clamp

**文件**：[store.go:129-167](pkg/spanconfig/spanconfigstore/store.go#L129)

```go
func (s *Store) getSpanConfigForKeyRLocked(ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, roachpb.Span, error) {
    // 1. 查 B-Tree
    conf, confSpan, found := s.mu.spanConfigStore.getSpanConfigForKey(ctx, key)
    if !found {
        conf = s.getFallbackConfig()  // 兜底
    }

    // 2. 合并系统级配置（PTS 等）
    conf, err = s.mu.systemSpanConfigStore.combine(key, conf)

    // 3. 若 boundsEnabled 且是 secondary tenant，Clamp 配置
    if !boundsEnabled.Get(&s.settings.SV) { return conf, confSpan, nil }
    _, tenID, err := keys.DecodeTenantPrefix(roachpb.Key(key))
    if tenID.IsSystem() { return conf, confSpan, nil }

    bounds, found := s.boundsReader.Bounds(tenID)
    if found {
        clamped := bounds.Clamp(&conf)  // 限制 secondary tenant 不能超过 host 设定的边界
    }
    return conf, confSpan, nil
}
```

**三步读取流水线**：

```
B-Tree.getSpanConfigForKey(key)
         ↓ 未找到时 fallback
    getFallbackConfig()
         ↓ 叠加系统配置
    systemSpanConfigStore.combine(key, conf)
         ↓ 若是 secondary tenant
    bounds.Clamp(&conf)    ← 防止 secondary tenant 配置超出 host 允许的范围
         ↓
    返回最终 SpanConfig
```

---

## 四、具体运行示例

### 4.1 正常场景：DDL 操作创建新表（单个 Addition Update）

**背景**：用户创建 `CREATE TABLE t (id INT PRIMARY KEY)`，系统为该表 span `[/Table/100/, /Table/101/)` 创建 SpanConfig。

**假设初始 B-Tree 状态**：

```
[/System/, /Table/): SystemConf
[/Table/99/, /Table/100/): DefaultConf
[/Table/100/, /Table/101/): (不存在，需要新建)
[/Table/101/, /Max/): DefaultConf
```

**Apply 调用**：

```go
Store.Apply(ctx, spanconfig.Update{
    Target: MakeTargetFromSpan(Span{"/Table/100/", "/Table/101/"}),
    Config: SpanConfig{GCTTLSeconds: 14400, NumReplicas: 3, ...},  // 新表配置
})
```

**执行过程**：

```
T=1  applyInternal 持写锁
T=2  分类: IsSpanTarget() → spanStoreUpdates

T=3  spanConfigStore.apply():
     validateApplyArgs(): span 有效，非重叠 ✓
     排序: 只有一个 update，无需排序

T=4  accumulateOpsFor():
     carryOver = empty

     处理 update [/Table/100/, /Table/101/):
       carriedOver = empty
       iter.FirstOverlap([/Table/100/, /Table/101/)):
         → 遍历 B-Tree:
           root.children[left].max = /Table/100/ → /Table/100/ < /Table/100/? No，可能重叠
           检查 [/Table/99/, /Table/100/):
             [/Table/99/,/Table/100/).Overlaps([/Table/100/,/Table/101/))? No（adjacent）
           检查 [/Table/101/, /Max/):
             start=/Table/101/ > end=/Table/101/? 触发 hard upper bound → 停止
         → 没有找到任何 overlapping entry

       skipAddingSelf = false
       update.Addition() = true → add {[/Table/100/, /Table/101/): NewConf}

     toDelete = [], toAdd = [{span:[/Table/100/,/Table/101/), conf:NewConf}]

T=5  执行变更:
     btree.Set(entry) → 插入新节点到 B-Tree
     adjustUpperBoundOnInsertion: 向上传播 maxKey（若 /Table/101/ 比 parent.max 更大）

T=6  释放写锁
T=7  返回: deleted=[], added=[Record{[/Table/100/,/Table/101/), NewConf}]
```

**B-Tree 变更后**：

```
[/System/, /Table/): SystemConf
[/Table/99/, /Table/100/): DefaultConf
[/Table/100/, /Table/101/): NewConf   ← 新插入
[/Table/101/, /Max/): DefaultConf
```

### 4.2 边界场景：分区配置覆盖现有 span（多 update，carry-over 触发）

**背景**：用户对表 `t`（span `[/Table/100/, /Table/101/)`）添加分区，分区配置：
- 分区 A：`[/Table/100/1/, /Table/100/5/)` → ConfA
- 分区 B：`[/Table/100/5/, /Table/100/9/)` → ConfB

剩余部分保留原配置 `OrigConf`。

**初始状态**：

```
B-Tree: ..., [/Table/100/, /Table/101/): OrigConf, ...
```

**Apply 调用**（两个 updates 批量）：

```go
Store.Apply(ctx,
    Update{Span: [/T/100/1, /T/100/5), Config: ConfA},
    Update{Span: [/T/100/5, /T/100/9), Config: ConfB},
)
```

**执行过程（关键部分）**：

```
排序后 updates = [
    [/T/100/1, /T/100/5): ConfA,
    [/T/100/5, /T/100/9): ConfB
]

accumulateOpsFor:

carryOver = {}

─── 处理 update[0]: [/T/100/1, /T/100/5): ConfA ───
  carriedOver = {}，无影响

  iter.FirstOverlap([/T/100/1, /T/100/5)):
    → 找到 [/T/100/, /T/101/): OrigConf (overlapping!)
    existing = [/T/100/, /T/101/)

    union = [/T/100/, /T/101/)  (最大范围)
    inter = [/T/100/1, /T/100/5)  (重叠部分)
    pre   = [/T/100/, /T/100/1)   (左残留)
    post  = [/T/100/5, /T/101/)   (右残留)

    toDelete ← [/T/100/, /T/101/): OrigConf

    existing.span.ContainsKey(/T/100/1/)? Yes (/T/100 ≤ /T/100/1 < /T/101)
      → pre 有效: toAdd ← [/T/100/, /T/100/1): OrigConf  (左截段保留原 conf)

    existing.span.ContainsKey(/T/100/5/)? Yes
      → carryOver = {span:[/T/100/5, /T/101/), config:OrigConf}  (右截段携带)

  update[0] is Addition → toAdd ← [/T/100/1, /T/100/5): ConfA

─── 处理 update[1]: [/T/100/5, /T/100/9): ConfB ───
  carriedOver = {[/T/100/5, /T/101/): OrigConf}

  [/T/100/5, /T/100/9).Overlaps([/T/100/5, /T/101/))? YES

  gap = {Key: /T/100/5, EndKey: /T/100/5} → Empty! 无 gap 需要填充

  carryOver = {Key: /T/100/9, EndKey: /T/101/}: OrigConf  (update 后的残留)

  iter.FirstOverlap([/T/100/5, /T/100/9)):
    → 找到 [/T/100/, /T/101/): OrigConf
    existing.span.Overlaps(carriedOver.span=[/T/100/5,/T/101/))? YES → continue (已处理)

  update[1] is Addition → toAdd ← [/T/100/5, /T/100/9): ConfB

─── carryOver 收尾 ───
  toAdd ← [/T/100/9, /T/101/): OrigConf

最终:
  toDelete = [[/T/100/, /T/101/): OrigConf]
  toAdd    = [
    [/T/100/, /T/100/1): OrigConf,
    [/T/100/1, /T/100/5): ConfA,
    [/T/100/5, /T/100/9): ConfB,
    [/T/100/9, /T/101/): OrigConf,
  ]
```

**B-Tree 变更后**：

```
...,
[/Table/100/,   /Table/100/1/):  OrigConf  ← 头部保留
[/Table/100/1/, /Table/100/5/):  ConfA     ← 分区A
[/Table/100/5/, /Table/100/9/):  ConfB     ← 分区B
[/Table/100/9/, /Table/101/):    OrigConf  ← 尾部保留
...
```

---

## 五、为什么选 B-Tree 而不是跳表？深度分析

这是本文最重要的一个设计权衡问题。我们从代码事实出发，逐维度分析。

### 5.1 核心需求决定数据结构

在这个场景中，数据结构需要支持：

| 操作 | 频率 | 要求 |
|---|---|---|
| **区间重叠查询** | 高频（每个 Range 切分/副本决策） | 尽量快，支持剪枝 |
| **点查询** | 高频（`GetSpanConfigForKey`） | O(log n) |
| **范围扫描** | 中频（`ForEachOverlapping`） | 有序，连续访问 |
| **插入/删除** | 低频（DDL、配置变更） | 可以稍慢 |
| **O(1) Snapshot** | 低频（`Clone()`） | 必须支持 |
| **Augmentation（maxKey）** | 与每次操作同步 | 结构内置支持 |

### 5.2 B-Tree 的关键优势

#### ① 原生支持 Augmentation（跳表最大的劣势）

**Augmentation**（CLRS Ch.14）是区间重叠查询从 O(n) 降到 O(k log n) 的关键（k = 结果数）。

在 **B-Tree** 中，每个节点天然存在"节点管辖子树"的概念：
- 每个节点维护自己子树的 `maxKey`。
- 插入/删除时，`maxKey` 的更新沿路径**向上传播**，路径长度为 O(log n)，每层 O(1) 更新。
- 查询时，若子树 `maxKey < query.start`，整个子树可以被**精确剪枝**。

在**跳表**中：
- 跳表的每层链表是单向的，没有"子树"的概念。
- 要实现区间重叠查询，需要扫描**所有** start_key ≤ query.end 的节点，然后逐一检查其 EndKey，最坏情况 O(n)。
- 即使在每个节点维护一个 `maxEndKey` 字段，也因为跳表的结构，无法在查询中进行有效的子树级剪枝。跳表的"上层节点"只能跳过整个节点，不能依据区间的 EndKey 做剪枝决策。

**结论**：跳表实现 Augmented Interval Tree 在理论上不如 B-Tree 自然，实践中性能差距显著。

#### ② 更好的缓存局部性（Cache Locality）

| 特性 | B-Tree (degree=16) | 跳表 |
|---|---|---|
| 每个节点 item 数 | 15-31 个 entry 指针 | 1 个 |
| 连续内存访问 | 同一 node 的 31 个 entry 连续 | 每次跳转随机内存位置 |
| 树高（10000 个 entry）| ~2-3 层 | 期望 O(log n) 层，每层独立指针 |
| L1/L2 缓存命中率 | 高（同节点内搜索命中缓存）| 低（随机指针跳转缓存 miss）|

现代 CPU 的 L1 缓存通常是 32-64 KB，L2 是 256 KB-4 MB。一个 B-Tree 节点（degree=16）包含 31 个 item 指针，约占 248 字节，一次 cache line 加载（64 字节）可以服务多个 entry 比较。

跳表的每次"跳"都是一次随机内存访问，极可能造成 cache miss（100ns+ 的延迟），在 10000 个 entry 规模下，性能差距会非常明显。

#### ③ Copy-on-Write（COW）Clone 的结构性优势

B-Tree 的 Clone 是 O(1) 的（只增加根节点引用计数），这直接来源于树状结构的**共享路径（Path Sharing）**语义：

```
Clone 后修改某个 entry:
  只需 Copy-on-Write 从根到目标叶节点的路径（O(log n) 个节点）
  其余所有节点继续被两棵树共享
```

对于跳表实现 COW Clone：
- Clone 需要复制整个跳表（至少所有节点），O(n)。
- 若使用"path copy"，跳表的每次修改会传播到所有层，每层都需要创建新节点，最坏情况仍然是 O(n)（所有 key 都在最高层）。

代码中 `Store.Clone()` 是一个被实际使用的能力（如 `spanconfigkvsubscriber` 在做对比时使用），O(1) 的 Clone 对系统性能至关重要。

#### ④ 有序遍历与范围扫描

B-Tree 的中序遍历天然有序，`forEachOverlapping(EverythingSpan, ...)` 就是线性的中序遍历，O(n)。跳表最低层也可以线性扫描，这一点两者相当。

### 5.3 跳表的潜在优势及为何不足以扭转选择

| 跳表优势 | 在本场景的适用性 |
|---|---|
| 实现更简单 | CockroachDB 用代码生成（`go:generate`），B-Tree 模板化，复杂度可控 |
| 并发性能更好（细粒度锁）| 整个 Store 已用 `syncutil.RWMutex` 保护，btree 本身非线程安全，无额外需求 |
| 插入/删除期望 O(log n) | 与 B-Tree 渐进复杂度相同，但 B-Tree 的常数因子更小（缓存） |
| 无需树平衡操作 | B-Tree 的分裂/合并代价在摊销意义上与跳表的插入相当 |

**最关键的一条**：跳表没有"子树上界"的语义，**无法高效实现 Augmented Interval Tree**。这一条已经足够否定跳表在此场景的应用。

### 5.4 与其他工业界数据库的对比佐证

| 系统 | 区间/Span 索引结构 | 原因 |
|---|---|---|
| CockroachDB spanconfigstore | Augmented interval B-Tree | 本文分析 |
| CockroachDB span.Frontier | B-Tree + MinHeap | 需要范围查询 + O(1) 最小值 |
| Google Spanner | B-Tree（SSTable 层） | 历史选型 |
| PostgreSQL | B-Tree（索引） | 范围查询、缓存友好 |
| Linux Kernel（虚拟内存管理）| Red-Black Tree（`vm_area_struct`）| 区间重叠查询 + augmentation |
| PostgreSQL rangetype | R-Tree / GiST | 多维区间，不同场景 |

Linux 内核中 `vm_area_struct`（虚拟内存区间）使用**红黑树 + augmentation** 做区间管理，与 CockroachDB 这里的选择在本质上是一致的：需要区间重叠查询时，树形结构 + augmentation 是标准答案。B-Tree 相比红黑树的进一步优势是更宽的节点（更好的缓存局部性）。

### 5.5 degree=16 的具体选择

```go
// entry_interval_btree.go:21-25
const (
    degree   = 16
    maxItems = 2*degree - 1    // = 31
    minItems = degree - 1      // = 15
)
```

`degree=16` 意味着每个节点存 15-31 个 item。为什么是 16？

- **缓存行对齐**：64 字节 cache line，指针 8 字节，31 个 entry 指针 = 248 字节 ≈ 4 个 cache line。一次节点访问需要 4 次 cache line 加载，但二分查找在同一节点内不跨 cache line 切换，整体缓存命中率高。
- **树高极低**：对于 CockroachDB 典型的 SpanConfig 数量（几千到几十万个），degree=16 的 B-Tree 高度只有 2-4 层，查询路径极短。
- **Google B-Tree 库的同款选择**：CockroachDB 参考了 `google/btree` 库的实现，该库也使用 degree=16/32 作为推荐值。

---

## 六、模块关系总结

```
Store.Apply(updates)
    │
    ├─ applyInternal()  ──[写锁]──→ 两路分发
    │
    ├─ spanConfigStore.apply()
    │      │
    │      ├─ validateApplyArgs()    校验：有效 span，无重叠
    │      │
    │      ├─ accumulateOpsFor()     核心算法：
    │      │    ├─ btree迭代器（只读）找重叠 entry
    │      │    ├─ 计算 pre/post 截段
    │      │    └─ carry-over 机制处理多 update 间的连续性
    │      │
    │      ├─ btree.Delete(entry)    → adjustUpperBoundOnRemoval → 向上传播 maxKey
    │      ├─ interner.remove()      → 引用计数 → 必要时释放内存
    │      └─ btree.Set(entry)       → adjustUpperBoundOnInsertion → 向上传播 maxKey
    │               └─ makeEntry() → interner.add() → 配置对象去重
    │
    └─ systemSpanConfigStore.apply()
           └─ HashMap CRUD（SystemTarget → SpanConfig）

读路径 Store.GetSpanConfigForKey():
    ├─ [读锁]
    ├─ btree.MakeIter().FirstOverlap(key)   O(log n + k)
    │      └─ 利用 maxKey 剪枝子树，避免全扫描
    ├─ systemSpanConfigStore.combine()      叠加 PTS 等系统配置
    └─ bounds.Clamp()                        限制 secondary tenant 上限
```

---

*文档生成时间：2026-02-28*
*分析版本：CockroachDB main branch（spanconfigstore 目录）*
