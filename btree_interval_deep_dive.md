# CockroachDB 增强区间 B-Tree 深度解析

> 文件：`pkg/util/span/btreefrontierentry_interval_btree.go`
>
> 本文件由 `go_generics` 工具从 `pkg/spanconfig/spanconfigstore/entry_interval_btree.go` 模板生成。两份文件共享几乎相同的算法，但实例化类型不同：前者服务于 `span.Frontier`（管理 `btreeFrontierEntry`），后者服务于 `SpanConfigStore`（管理 span config 条目）。

---

## 一、广度优先：架构全景

### 1.1 整体定位

```
span.Frontier
├── btreeFrontier                  ← 主结构 (frontier.go)
│   ├── tree   (btree)             ← 本文分析对象：增强区间 B-Tree
│   │   按 span.Key 排序的 B-Tree  │  用于范围查询（Forward 时找重叠 span）
│   │   每个节点维护子树的最大 EndKey │
│   └── minHeap (frontierHeap)     ← 最小堆（按 ts 排序，O(1) 查 Frontier()）
└── ...
```

**B-Tree 在 Frontier 中的职责**：当 `Forward(span, ts)` 被调用时，需要找到所有与 `span` 重叠的 `btreeFrontierEntry`（每个 entry 代表一个 span 及其当前时间戳），然后更新它们的时间戳并可能合并相邻 entry。这正是增强区间 B-Tree 的重叠查询功能所服务的场景。

### 1.2 关键常量

```go
degree   = 16        // B-Tree 的度数
maxItems = 2*degree - 1 = 31  // 每个节点最多存放 31 个 item
minItems = degree - 1 = 15    // 每个非根节点至少存放 15 个 item
```

**度数选择的工程考量**：
- `maxItems=31` 个指针（`*btreeFrontierEntry`，8字节）= 248 字节。
- `items` 数组 + 节点元信息约在 1-2 个 CPU cache line（64字节）的边界内。
- B-Tree 高度：对 10^6 个 entry，高度 ≈ log₃₁(10^6) ≈ 4，树高极低，缓存友好。

### 1.3 核心数据结构关系图

```
btree
├── root   *node         ← 根节点（nil 表示空树）
└── length int           ← entry 总数

node (每个节点)
├── ref    int32         ← atomic，COW 引用计数（1=独占，>1=共享）
├── count  int16         ← 当前节点存放的 item 数量（≤ 31）
├── maxKey []byte        ← 子树中所有 item 的最大 EndKey (增强字段)
├── maxInc bool          ← maxKey 是否为 inclusive 上界
├── items  [31]*entry    ← 按 Key 升序排列的 item 数组
└── children *childrenArray  ← [32]*node，nil 表示叶节点

childrenArray = [maxItems+1]*node = [32]*node
               ↑
               children[i] 的所有 key 都在 items[i-1] 和 items[i] 之间
```

**内存池（两个 sync.Pool）**：

```
leafPool  → 复用 *node（children==nil 的叶节点）
nodePool  → 复用内联了 childrenArray 的 *interiorNode
             ↑ 关键：nodePool 分配时将 children 指向 node 内联的数组
```

内联 `childrenArray` 的技巧将内部节点所需的 `node + childrenArray` 两次分配合并为一次，减少 GC 压力。

---

## 二、深度优先：关键函数代码细节

### 2.1 node 结构与内存布局

```go
// 文件：btreefrontierentry_interval_btree.go:96-113
type node struct {
    ref   int32     // 4 bytes
    count int16     // 2 bytes
    maxInc bool     // 1 byte（与 maxKey 共同构成 keyBound，但内联以节省 padding）
    maxKey []byte   // 24 bytes（slice header: ptr+len+cap）
    items  [maxItems]*btreeFrontierEntry  // 31 * 8 = 248 bytes
    children *childrenArray              // 8 bytes（nil iff leaf）
}
```

**重要设计注释（第100-102行）**：

> These fields form a keyBound, but by inlining them into node we can avoid the extra word that would be needed to pad out maxInc if it were part of its own struct.

如果将 `maxKey`+`maxInc` 包装成独立的 `keyBound` 结构体，Go 编译器会为 `bool` 字段添加 7 字节 padding（对齐到 8 字节），浪费内存。内联可将 `bool` 与 `int16` 紧密排列。

### 2.2 keyBound：增强字段的上界表示

```go
// 文件：61-94
type keyBound struct {
    key []byte
    inc bool  // true = inclusive (key 本身也在范围内)
}
```

`upperBound(c)` 函数：
```go
func upperBound(c *btreeFrontierEntry) keyBound {
    if len(c.EndKey()) != 0 {
        return keyBound{key: c.EndKey()}  // inc=false（默认），表示 [Key, EndKey) 区间
    }
    return keyBound{key: c.Key(), inc: true}  // 点 key，inclusive
}
```

`keyBound.contains(a)` 判断 entry `a` 的 Key 是否在上界范围内：
```go
func (b keyBound) contains(a *btreeFrontierEntry) bool {
    c := bytes.Compare(a.Key(), b.key)
    if c == 0 {
        return b.inc  // key 等于上界时，只有 inclusive 才算"包含"
    }
    return c < 0     // key 严格小于上界时才算"包含"
}
```

### 2.3 sync.Pool 节点池：零 GC 分配

```go
// 文件：115-131
var leafPool = sync.Pool{
    New: func() interface{} { return new(node) },
}

var nodePool = sync.Pool{
    New: func() interface{} {
        type interiorNode struct {
            node
            children childrenArray  // 内联 children 数组，避免额外 heap 分配
        }
        n := new(interiorNode)
        n.node.children = &n.children  // 指向内联数组
        return &n.node
    },
}
```

**内部节点内存布局对比**：

```
方案 A（朴素）：                     方案 B（本实现）：
  node  →  堆对象1（node）            node  →  堆对象1（interiorNode）
             + 指针 children              包含 node + children 数组
                ↓                        children 指向内联数组
             堆对象2（childrenArray）     ↑
                                        只需 1 次 heap 分配
```

**回收流程**（`decRef` 第197-216行）：
```go
func (n *node) decRef(recursive bool) {
    if atomic.AddInt32(&n.ref, -1) > 0 {
        return  // 还有其他引用，不释放
    }
    if n.leaf() {
        *n = node{}         // 清空所有字段（items 指针置 nil，防止内存泄漏）
        leafPool.Put(n)
    } else {
        if recursive {
            for i := int16(0); i <= n.count; i++ {
                n.children[i].decRef(true)  // 递归释放子树
            }
        }
        *n = node{children: n.children}  // 保留 children 指针（是内联数组的地址）
        *n.children = childrenArray{}    // 清空 children 数组
        nodePool.Put(n)
    }
}
```

**关键细节**：释放内部节点时必须保留 `children` 指针，因为它指向同一个 `interiorNode` 对象内的内联数组，不能置 nil——否则下次从 pool 取出时 `children` 会是 nil，节点会被误判为叶节点。

### 2.4 COW（Copy-on-Write）机制：`mut()` + `clone()`

COW 是整个 B-Tree 并发安全策略的基石。规则：写之前先检查 `ref`，若 `>1` 则先 clone。

```go
// 文件：154-169
func mut(n **node) *node {
    if atomic.LoadInt32(&(*n).ref) == 1 {
        return *n  // 独占，可直接修改
    }
    // 共享，需要 clone 以获得独占所有权
    c := (*n).clone()
    (*n).decRef(true /* recursive */)
    *n = c
    return *n
}
```

`clone()` 函数（第220-241行）：

```go
func (n *node) clone() *node {
    var c *node
    if n.leaf() {
        c = newLeafNode()
    } else {
        c = newNode()
    }
    // 字段级复制（不触碰 ref，避免 race detector 误报）
    c.count = n.count
    c.maxKey = n.maxKey
    c.maxInc = n.maxInc
    c.items = n.items
    if !c.leaf() {
        *c.children = *n.children  // 复制子节点指针数组
        for i := int16(0); i <= c.count; i++ {
            c.children[i].incRef()  // 每个子节点引用计数 +1
        }
    }
    return c  // 新节点 ref=1（独占）
}
```

**COW 传播规律**（写操作的路径上每个节点都会触发 clone）：

```
初始状态（Clone() 后两棵树共享）:
  Tree1.root → Node_A (ref=2)
                 ├── Node_B (ref=2)
                 └── Node_C (ref=2)

Tree1 执行 Set(x)（需要修改 Node_A → Node_B 路径）:
  mut(&Tree1.root)     → ref=2 → clone Node_A → Node_A' (ref=1)
                                  同时 Node_B.ref++, Node_C.ref++
                                  Node_A.decRef(recursive=true)
                                  → Node_A.ref 从 2→1（其他树仍持有）

  mut(&Node_A'.children[i]) → ref=2 → clone Node_B → Node_B' (ref=1)
                               Node_B.decRef(recursive=true)
                               → Node_B.ref 从 2→1

最终状态:
  Tree1: root → Node_A' → Node_B'（已修改）→ Node_C（共享）
  Tree2: root → Node_A  → Node_B（未修改）→ Node_C（共享）
```

### 2.5 `btree.Clone()`：O(1) 克隆

```go
// 文件：694-715
func (t *btree) Clone() btree {
    c := *t                  // 复制 btree 结构体（root 指针和 length）
    if c.root != nil {
        c.root.incRef()      // 只需增加根节点的引用计数！
    }
    return c
}
```

**为什么只操作根节点就足够？**

注释（第699-714行）解释了完整的正确性论证：
1. Clone 后两棵树的 root 的 ref ≥ 2。
2. 任何写操作沿树向下时，每次 `mut()` 检查 ref > 1 → 触发 clone → clone 时对所有子节点 `incRef()`。
3. 这保证了被修改的路径上所有节点都会被 clone，子节点的 ref 也会 ≥ 2。
4. 从而整棵树的不可变性以递归方式得到保证，而无需在 Clone 时遍历整棵树。

**对比**：若 Clone 时需要完整复制树，时间复杂度 O(n)；现在 O(1)，使 `span.Frontier` 的快照功能极其高效。

### 2.6 `Set()` 与 `Delete()`：顶层写操作

**`Set(item)`（第738-754行）**：

```go
func (t *btree) Set(item *btreeFrontierEntry) {
    if t.root == nil {
        t.root = newLeafNode()
    } else if t.root.count >= maxItems {
        // 根节点已满，先分裂根节点
        splitLa, splitNode := mut(&t.root).split(maxItems / 2)
        newRoot := newNode()
        newRoot.count = 1
        newRoot.items[0] = splitLa
        newRoot.children[0] = t.root
        newRoot.children[1] = splitNode
        newRoot.setMax(newRoot.findUpperBound())
        t.root = newRoot
    }
    if replaced, _ := mut(&t.root).insert(item); !replaced {
        t.length++
    }
}
```

**预防性分裂策略（proactive split）**：在下行过程中，若发现节点已满（`count >= maxItems`），提前分裂，而非等到插入失败再回溯。这是 B-Tree 的标准 top-down 插入策略，确保插入操作永远不需要回溯——单次下行即可完成。

**`Delete(item)`（第718-734行）**：

```go
func (t *btree) Delete(item *btreeFrontierEntry) {
    if t.root == nil || t.root.count == 0 { return }
    if out, _ := mut(&t.root).remove(item); out != nilT {
        t.length--
    }
    // 根节点为空时，收缩树高
    if t.root.count == 0 {
        old := t.root
        if t.root.leaf() {
            t.root = nil
        } else {
            t.root = t.root.children[0]
        }
        old.decRef(false /* recursive */)
    }
}
```

### 2.7 `node.insert()`：递归下行插入

```go
// 文件：399-428
func (n *node) insert(item *btreeFrontierEntry) (replaced, newBound bool) {
    i, found := n.find(item)
    if found {
        n.items[i] = item
        return true, false  // 替换已有 item，上界不变
    }
    if n.leaf() {
        n.insertAt(i, item, nil)
        return false, n.adjustUpperBoundOnInsertion(item, nil)
    }
    // 内部节点：若目标子节点已满，先分裂
    if n.children[i].count >= maxItems {
        splitLa, splitNode := mut(&n.children[i]).split(maxItems / 2)
        n.insertAt(i, splitLa, splitNode)
        // 调整 i 指向正确的子节点
        switch v := compare(item, n.items[i]); {
        case v < 0:   // 不变，走左子树
        case v > 0: i++ // 走右子树
        default:      // item 与分裂项相同，替换
            n.items[i] = item
            return true, false
        }
    }
    replaced, newBound = mut(&n.children[i]).insert(item)
    if newBound {
        newBound = n.adjustUpperBoundOnInsertion(item, nil)
    }
    return replaced, newBound
}
```

**`node.find(item)`：内联二分查找（第322-339行）**

```go
func (n *node) find(item *btreeFrontierEntry) (index int, found bool) {
    i, j := 0, int(n.count)
    for i < j {
        h := int(uint(i+j) >> 1)
        v := compare(item, n.items[h])
        if v == 0 {
            return h, true
        } else if v > 0 {
            i = h + 1
        } else {
            j = h
        }
    }
    return i, false
}
```

注释说明了内联二分查找（而非调用 `sort.Search`）带来了 11% 的性能提升（BenchmarkBTreeDeleteInsert 中实测）。

### 2.8 `node.split()`：节点分裂

```go
// 文件：362-393
func (n *node) split(i int) (*btreeFrontierEntry, *node) {
    out := n.items[i]  // 中间项，将被提升到父节点
    var next *node     // 分裂出的右半部分
    if n.leaf() { next = newLeafNode() } else { next = newNode() }

    next.count = n.count - int16(i+1)
    copy(next.items[:], n.items[i+1:n.count])
    for j := int16(i); j < n.count; j++ { n.items[j] = nilT }
    if !n.leaf() {
        copy(next.children[:], n.children[i+1:n.count+1])
        for j := int16(i+1); j <= n.count; j++ { n.children[j] = nil }
    }
    n.count = int16(i)

    // 维护增强字段（最关键的部分）
    nextMax := next.findUpperBound()
    next.setMax(nextMax)
    nMax := n.max()
    if nMax.compare(nextMax) != 0 && nMax.compare(upperBound(out)) != 0 {
        // 原节点的 max 既不来自右半部分，也不来自被提升的项
        // → 它一定来自左半部分，不需要重新计算
    } else {
        n.setMax(n.findUpperBound())  // 需要重新扫描左半部分
    }
    return out, next
}
```

**分裂示意图**（index=1，3个item的节点分裂）：

```
分裂前:
  Node: [A B C]
         ↑
         index=1（middle）

分裂后:
  提升项: B → 插入父节点
  Left (n): [A]
  Right (next): [C]

增强字段更新:
  next.max = max(upperBound(C), children[2].max, ...)
  n.max = 若原 max 不来自 next 或 B，则无需重算；否则重算
```

### 2.9 `rebalanceOrMerge()`：删除时的再平衡

当目标子节点 `count <= minItems` 时调用此函数，确保删除后子节点仍满足最小 item 数约束。

```
三种策略（按优先级）：

1. 从左兄弟借（左兄弟富余）:
   Left: [... x]   Parent: y   Child: []
   →
   Left: [...]   Parent: x   Child: [y ...]
   左兄弟的最大 item 上移，父节点对应 item 下移到 child 前端。

2. 从右兄弟借（右兄弟富余）:
   Child: []   Parent: y   Right: [x ...]
   →
   Child: [... y]   Parent: x   Right: [...]
   右兄弟的最小 item 上移，父节点对应 item 下移到 child 后端。

3. 合并（两个兄弟都只有 minItems）:
   Child: [x]   Parent: y   Sibling: [z]
   →
   Merged: [x y z]（合并到 child，从父节点移除 y 和 Sibling）
```

每次旋转/合并后都要调用 `adjustUpperBoundOnInsertion` / `adjustUpperBoundOnRemoval` 维护增强字段。

### 2.10 增强字段维护：`adjustUpperBoundOnInsertion/Removal()`

**插入时（第633-645行）**：

```go
func (n *node) adjustUpperBoundOnInsertion(item *btreeFrontierEntry, child *node) bool {
    up := upperBound(item)
    if child != nil {
        if childMax := child.max(); up.compare(childMax) < 0 {
            up = childMax  // 新子树的 max 可能更大
        }
    }
    if n.max().compare(up) < 0 {
        n.setMax(up)  // 新 item/子树扩大了上界
        return true
    }
    return false
}
```

**删除时（第650-664行）**：

```go
func (n *node) adjustUpperBoundOnRemoval(item *btreeFrontierEntry, child *node) bool {
    up := upperBound(item)
    if child != nil {
        if childMax := child.max(); up.compare(childMax) < 0 {
            up = childMax
        }
    }
    if n.max().compare(up) == 0 {
        // up 是原来的上界，需要重新计算
        max := n.findUpperBound()
        n.setMax(max)
        return max.compare(up) != 0
    }
    return false  // 删除的 item/子树不影响上界
}
```

**核心思想**：
- 插入：若新 item 的 upper bound > 当前节点 max → 更新，无需完整重算。O(1)。
- 删除：若被删 item 的 upper bound == 当前节点 max → 旧上界可能是这个 item 贡献的，必须完整重算。`findUpperBound()` 遍历当前节点所有 item 和直接子节点的 max，O(maxItems) = O(31)，为常数。

**`findUpperBound()`（第611-628行）**：

```go
func (n *node) findUpperBound() keyBound {
    var max keyBound
    for i := int16(0); i < n.count; i++ {
        up := upperBound(n.items[i])
        if max.compare(up) < 0 { max = up }
    }
    if !n.leaf() {
        for i := int16(0); i <= n.count; i++ {
            up := n.children[i].max()  // 直接读子节点的 max，不递归
            if max.compare(up) < 0 { max = up }
        }
    }
    return max
}
```

注意：只需看当前节点的 items 和直接子节点的 `max`（不递归），因为子节点的 `max` 已经是其子树的正确上界（递归维护的不变量）。

### 2.11 iterStack：小数组优化

```go
// 文件：815-866
type iterStack struct {
    a    iterStackArr   // [3]iterFrame  ← 栈内 3 帧，零分配
    aLen int16          // -1 表示已切换到 slice 模式
    s    []iterFrame    // 超过 3 帧后使用 heap slice
}

type iterStackArr [3]iterFrame  // 3个帧 = 3层 B-Tree 节点

type iterFrame struct {
    n   *node
    pos int16
}
```

**状态机**：

```
初始状态: aLen=0（数组模式）

push(f):
  aLen != -1 且 aLen < 3 → a[aLen]=f; aLen++     // 栈未满，直接写数组
  aLen != -1 且 aLen == 3 → 分配 slice，复制，aLen=-1 // 切换到 slice 模式
  aLen == -1              → s = append(s, f)        // slice 追加

pop():
  aLen != -1 → aLen--; return a[aLen]   // 数组模式弹出
  aLen == -1 → f=s[last]; s=s[:last-1]  // slice 模式弹出
```

**为什么是 3 帧**：
- degree=16 时，10^6 个 entry 的树高 ≈ 4。
- 遍历时栈深 = 树高 - 1（叶层不需要保存父帧）≈ 3。
- 绝大多数情况下 3 帧足够，实现了零 heap 分配的迭代器。

### 2.12 overlapScan：重叠扫描的精华

```go
// 文件：1157-1166
type overlapScan struct {
    // "软"下界约束：从 search item 的 start key 开始，第一个 start key ≥ 搜索 start key 的位置
    constrMinN   *node
    constrMinPos int16
    inConstrMin  bool  // 已进入 constrMin 区域（之后所有 item 必然重叠，无需比较 end key）

    // "硬"上界约束：第一个 start key > 搜索 end key 的位置（到此截止）
    constrMaxN   *node
    constrMaxPos int16
}
```

**软下界 vs 硬上界的本质差别**：

```
搜索区间:  [sStart ---------> sEnd)
B-Tree 按 start key 排序:

          [a,?) [b,?) [sStart,?) [c,?) [d,?) [sEnd,?) [e,?)
                       ↑                        ↑
                   constrMinPos             constrMaxPos

硬上界: constrMaxPos 之后的 item，start key > sEnd，不可能重叠 → 终止扫描（确定性）
软下界: constrMinPos 之前的 item，start key < sStart，不一定不重叠！
        例如 [a, sEnd+1) 的 start key < sStart，但 end key > sStart → 仍重叠！
        所以 constrMinPos 之前的 item 还需要检查 end key（"软"的含义）
        constrMinPos 之后的 item，start key ≥ sStart 且 end key > start key > sStart → 必然重叠
```

### 2.13 `constrainMin/MaxSearchBounds()`：在根节点设置初始约束

```go
// 文件：1223-1248
func (i *iterator) constrainMinSearchBounds(item *btreeFrontierEntry) {
    k := item.Key()  // 搜索 start key
    j := sort.Search(int(i.n.count), func(j int) bool {
        return bytes.Compare(k, i.n.items[j].Key()) <= 0
    })
    // j = 第一个 items[j].Key() >= k 的位置（即 start key ≥ 搜索 start key）
    i.o.constrMinN = i.n
    i.o.constrMinPos = int16(j)
}

func (i *iterator) constrainMaxSearchBounds(item *btreeFrontierEntry) {
    up := upperBound(item)  // 搜索区间的上界
    j := sort.Search(int(i.n.count), func(j int) bool {
        return !up.contains(i.n.items[j])
    })
    // j = 第一个 items[j].Key() > sEnd 的位置
    i.o.constrMaxN = i.n
    i.o.constrMaxPos = int16(j)
}
```

两个约束都在根节点的 item 数组上二分查找，给出在根节点层面的位置约束。在向下遍历时，若进入约束位置的子节点，会**精化**（refine）约束到该子节点层面（`findNextOverlap` 第1263-1268行）。

### 2.14 `findNextOverlap()`：正向重叠扫描状态机

```go
// 文件：1250-1299
func (i *iterator) findNextOverlap(item *btreeFrontierEntry) {
    for {
        if i.pos > i.n.count {
            i.ascend()  // 越界，回溯到父节点
        } else if !i.n.leaf() {
            // 内部节点：尝试向下
            if i.o.inConstrMin || i.n.children[i.pos].max().contains(item) {
                // 子树 max 包含搜索 item → 子树中可能有重叠，下行
                par := i.n
                pos := i.pos
                i.descend(par, pos)
                // 精化约束
                if par == i.o.constrMinN && pos == i.o.constrMinPos {
                    i.constrainMinSearchBounds(item)
                }
                if par == i.o.constrMaxN && pos == i.o.constrMaxPos {
                    i.constrainMaxSearchBounds(item)
                }
                continue
            }
            // 子树 max < 搜索 start key → 子树中不可能有重叠，跳过子树
        }

        // 硬上界检查：到达 constrMaxPos → 停止
        if i.n == i.o.constrMaxN && i.pos == i.o.constrMaxPos {
            i.pos = i.n.count  // 标记无效
            return
        }
        // 软下界检查：到达 constrMinPos → 进入 inConstrMin 状态
        if i.n == i.o.constrMinN && i.pos == i.o.constrMinPos {
            i.o.inConstrMin = true
        }

        // 检查当前 item 是否重叠
        if i.pos < i.n.count {
            if i.o.inConstrMin {
                return  // 快路径：start key ≥ sStart，无需比较 end key
            }
            if upperBound(i.n.items[i.pos]).contains(item) {
                return  // 慢路径：end key ≥ sStart
            }
        }
        i.pos++
    }
}
```

**状态转换图**：

```
初始状态: pos=0, root, inConstrMin=false

                    pos > count
                        ↓
                    ascend()  ← 回溯

  内部节点 && child.max.contains(item) →  descend()（可能精化约束）
                        ↓
        到达 constrMaxN,constrMaxPos → 终止（return invalid）
        到达 constrMinN,constrMinPos → inConstrMin=true
                        ↓
  pos < count:
    inConstrMin=true  → return（有效，快路径）
    !inConstrMin      → upperBound.contains → return（有效，慢路径）
  pos >= count → pos++（继续下一个）
```

**核心优化**：
1. `i.n.children[i.pos].max().contains(item)` —— O(1) 子树剪枝，跳过不可能有重叠的子树。
2. `inConstrMin` 快路径 —— 一旦进入 constrMin 区域，无需再比较 end key，大量减少比较次数。
3. 硬上界提前终止 —— 到达 constrMaxPos 立即停止，不扫描无意义的尾部。

### 2.15 `findPrevOverlap()`：反向重叠扫描的差异

反向扫描与正向扫描的关键区别（代码注释第1125-1156行已详述）：

```
正向扫描:
  - 起始 inConstrMin=false，进入 constrMin 后变为 true（且不再回退）
  - 硬上界在右边，遇到截止
  - 子节点比 item 多一个（N children, N-1 items），从 children[0] 开始

反向扫描:
  - 起始 inConstrMin=true（从硬上界之前"跳入"，已知 start key < sEnd）
  - 遇到 constrMin 时，inConstrMin 变为 false（需要开始检查 end key）
  - 子节点从 children[count] 开始（最右），需要 pos 减法
  - "跳跃"优化：pos > constrMaxPos 时，直接跳到 constrMaxPos（跳过右侧不相关项）
```

反向扫描的 pos 递减时机（第1370行 `i.pos--`）：在"先检查子节点，再检查当前 item"的语义中，pos 递减必须在检查子节点之后、检查 item 之前，与正向扫描相反（正向在 item 检查之后递增）。

---

## 三、具体运行示例

### 3.1 插入导致根节点分裂

**初始状态**：树已有 31 个 entry（根节点满），现在插入第 32 个。

```
Set(newEntry) 调用时：
  t.root.count == 31 >= maxItems(31) → 触发根节点预防性分裂

  split(maxItems/2 = 15):
    out = t.root.items[15]         ← 中间项，提升为新根的第一个 item
    next = 新节点（包含 items[16..30]）
    t.root 保留 items[0..14]

    更新增强字段:
      next.max = findUpperBound(next)
      t.root.max = (可能需要 findUpperBound，取决于原 max 是否来自被分裂部分)

  newRoot = 新内部节点:
    count = 1
    items[0] = out          ← 中间项
    children[0] = old root  ← 左子树（items[0..14]）
    children[1] = next      ← 右子树（items[16..30]）
    newRoot.max = findUpperBound(newRoot)  ← 综合两个子树的 max

  t.root = newRoot

  然后 mut(&t.root).insert(newEntry):
    在 newRoot（内部节点）中 find 位置 → 决定走左子树还是右子树
    左/右子树 count=15 < maxItems，无需再分裂 → 直接叶层插入
    自底向上调用 adjustUpperBoundOnInsertion
```

**状态对比**：

```
分裂前（树高=1）:             分裂后（树高=2）:
  root: [a₀..a₃₀]              root: [a₁₅]
                                      /    \
                               [a₀..a₁₄]  [a₁₆..a₃₀]
                               + newEntry（插入其中一侧）
```

### 3.2 重叠扫描示例：`Forward([b,d), ts)`

**场景**：`span.Frontier` 调用 `Forward([b,d), ts)` 时，需要找出所有与 `[b,d)` 重叠的 entry。

假设树中有以下 entry（按 start key 排序）：

```
[a, c)   ← start=a < b，但 end=c > b → 重叠！
[b, e)   ← start=b >= b → 必然重叠（inConstrMin 后的快路径）
[c, f)   ← start=c >= b → 必然重叠
[d, g)   ← start=d >= d → start >= sEnd，不重叠（硬上界截止）
[e, h)   ← 更后，不扫描
```

**FirstOverlap([b,d)) 执行流程**：

```
T=1: reset()，pos=0，n=root
T=2: constrainMinSearchBounds([b,d))
     → 二分找第一个 items[j].Key() >= "b"
     → constrMinN=root, constrMinPos=1（items[1]=[b,e)）
T=3: constrainMaxSearchBounds([b,d))
     → 二分找第一个 items[j].Key() > "d"（upperBound=[d,false).contains()）
     → constrMaxN=root, constrMaxPos=3（items[3]=[d,g)）

T=4: findNextOverlap([b,d))
  Loop 1: pos=0, n=root（内部节点）
    children[0].max.contains([b,d)) ?
      → max of left child = max{upperBound of all entries in subtree}
      → 假设 max={"c",false}（[a,c) 的 end），contains([b,d)) 含义：b < c → true
    → descend 到 children[0]
    → children[0] 是否在 constrMinN 或 constrMaxN 的约束位置？否

  Loop 2: pos=0, n=children[0]（叶节点，包含 [a,c)）
    pos=0 < count → 检查 item
    inConstrMin=false
    → upperBound([a,c)) = {"c",false}.contains([b,d)) ？
       "b" < "c" → true → return！

  ✓ 找到第一个重叠 [a,c)

NextOverlap([b,d)):
  pos++ → pos=1
  Loop 1: pos=1，n=leaves，count=1 → pos >= count → ascend to root
  Loop 2: pos=0, n=root
    pos=0 == constrMinN=root, constrMinPos=1? 否（pos=0 < 1）
    inConstrMin=false
    pos < count (0<3): upperBound([a,c)).contains([b,d)) → 不对，pos 已移动...
    [重新描述：ascend 后 pos 恢复到 descend 时保存的 pos，即 0]
    pos++ → pos=1

  Loop 3: pos=1, n=root（内部节点）
    pos=1 == constrMinPos=1 → inConstrMin=true
    children[1].max.contains([b,d)) → true（max >= b）
    → descend 到 children[1]（包含 [b,e), [c,f)）
    精化约束: constrMinPos → find in children[1]
              constrMaxPos → find in children[1]

  Loop 4: pos=0, n=children[1]（叶，inConstrMin=true）
    inConstrMin=true → 直接 return！

  ✓ 找到 [b,e)

NextOverlap([b,d)):
  pos++ → 1 → inConstrMin=true → 直接 return [c,f)

NextOverlap([b,d)):
  pos++ → 2 → pos=constrMaxPos → 无效，终止！

  ✓ 扫描结束
```

**扫描统计**：
- 找到 3 个重叠：[a,c)（软下界区，需检查 end key）、[b,e)、[c,f)（硬下界区，快路径）
- 剪枝了 [d,g) 和之后的所有 entry（硬上界截止）

### 3.3 `decRef(recursive=true)` 释放子树

**场景**：Clone 后，原树被丢弃（`Reset()`），触发全树引用计数递减。

```
Clone 后状态:
  Tree1.root → Node_A (ref=2)
  Tree2.root → Node_A (ref=2)
                 ├── Node_B (ref=2)
                 └── Node_C (ref=2)

Tree1.Reset():
  t.root.decRef(recursive=true)
    → Node_A.ref: 2→1 > 0 → return（Node_B、Node_C 不变）

Tree2.Reset():
  t.root.decRef(recursive=true)
    → Node_A.ref: 1→0 → 开始释放
      → Node_B.decRef(recursive=true): ref 1→0 → 释放，放回 nodePool
      → Node_C.decRef(recursive=true): ref 1→0 → 释放，放回 nodePool
    → Node_A 本身放回 nodePool

总 pool 回收: 3个节点，零 GC 分配
```

---

## 四、补充知识

### 4.1 CLRS Chapter 14：增强数据结构理论

本实现直接对应 CLRS（算法导论）第14章"数据结构的扩张"的区间树算法：

| CLRS 概念 | 本实现对应 |
|---|---|
| 红黑树 | B-Tree（更宽，缓存友好） |
| 区间 `[low, high]` | `btreeFrontierEntry` 的 `[Key(), EndKey())` |
| `max[x]` 增强字段 | `node.maxKey` + `node.maxInc` |
| `Interval-Search(i)` | `FirstOverlap(item)` + `NextOverlap(item)` |
| 旋转后维护 max | `split`/`rebalanceOrMerge` 中的 `adjustUpperBound*` |

**核心定理（CLRS 14.3）**：区间树的 `Interval-Search` 操作可在 O(log n) 时间内找到任一与给定区间重叠的节点。本实现的 `findNextOverlap` 通过子树 `max` 剪枝实现了等效保证。

**B-Tree vs 红黑树的取舍**：

| 维度 | 红黑树 | B-Tree (degree=16) |
|---|---|---|
| 每节点 item 数 | 1 | 1-31 |
| 树高（n=10^6）| ~20 | ~4 |
| Cache miss（查找）| ~20 次 | ~4 次 |
| 节点分配次数（插入）| O(log n) | O(log n / log degree) ≈ O(1) 平均 |
| 代码复杂度 | 较低 | 较高（分裂、合并逻辑） |
| 并发 COW 开销 | 每次 clone 一个节点 | 每次 clone 一个节点，但节点更大 |

CockroachDB 选择 B-Tree：因为 `span.Frontier` 和 `SpanConfigStore` 都有大量顺序/范围扫描，B-Tree 的更好缓存局部性（一个节点包含 31 个 item）远优于红黑树。

### 4.2 go_generics 代码生成

本文件的注释 `// Code generated by go_generics. DO NOT EDIT.` 表明它从模板生成。模板参数化了 item 类型（`*btreeFrontierEntry` vs `*storeEntry`），但算法完全共享。这是 Go 泛型（`1.18+` 之前）的一种实用解决方案，通过代码生成实现类型安全的高性能数据结构。

生成命令通常类似于：
```bash
go_generics -i entry_interval_btree.go \
  -o btreefrontierentry_interval_btree.go \
  -t T=btreeFrontierEntry
```

### 4.3 `compare()` 函数：三元组排序键

```go
// (a.Key(), a.EndKey(), a.ID())
```

使用三元组排序确保了：
1. **主键**：`Key()`（start key）按区间起点排序，这是 B-Tree 有序性和二分查找的基础。
2. **次键**：`EndKey()`，相同 start key 的区间按结束点排序。
3. **三键**：`ID()`（唯一标识符），确保即使两个区间完全相同，B-Tree 也能区分它们，不会发生意外的"替换"（只有 ID 也相同时才真正替换）。

### 4.4 与 `span.Frontier.Forward()` 的集成

```
btreeFrontier.Forward(span, ts) 调用路径:

1. iter.FirstOverlap(searchEntry)  ← 找第一个与 span 重叠的 entry
2. for iter.Valid():
     entry := iter.Cur()
     if entry.span ⊆ span:
         entry.ts = ts           ← 更新时间戳
         btree.Set(entry)        ← 原地更新（COW 保护）
         heap.Fix(entry)         ← 维护 minHeap 有序性
     elif entry.span 跨越 span 边界:
         拆分 entry，更新/插入
     iter.NextOverlap(searchEntry)  ← 找下一个重叠
3. 合并相邻 ts 相同的 entry（减少 B-Tree 节点数）
```

B-Tree 的重叠扫描是整个 `Forward()` 操作的核心计算路径，其 O(k log n) 的时间复杂度（k 为重叠 entry 数）直接决定了 `Frontier` 的更新性能。

### 4.5 并发安全性说明

B-Tree 本身**不是并发安全的**（注释第676行："Write operations are not safe for concurrent mutation by multiple goroutines, but Read operations are."）。

`span.Frontier` 通过以下机制保证安全：
- `btreeFrontier`（未加锁）：在 `run()` 的单线程中使用（`processEventsTask` 是串行的）。
- `concurrentFrontier`：用 `sync.Mutex` 包装 `btreeFrontier`，在 `runInitialScan` 的并行扫描中使用。

COW 的价值不在于并发写，而在于允许**读者在不加锁的情况下安全遍历**——因为 `btree.Clone()` 后读者持有的旧树节点不会被写者修改（写者会 clone 路径上的节点）。

---

## 五、完整调用链一览

```
btree.Set(item)
└── [根节点满] mut(&t.root).split(15) → 创建新根
└── mut(&t.root).insert(item)
    └── [子节点满] mut(&children[i]).split(15) → 提前分裂
    └── [叶节点] insertAt(i, item) → 直接插入
    └── adjustUpperBoundOnInsertion(item) → 自底向上更新 max
        └── n.max().compare(up) < 0 → n.setMax(up) → return true

btree.Delete(item)
└── mut(&t.root).remove(item)
    └── [子节点太小] rebalanceOrMerge(i)
        ├── [左兄弟富余] 旋转 + adjustUpperBound*
        ├── [右兄弟富余] 旋转 + adjustUpperBound*
        └── [均不富余] 合并 + adjustUpperBoundOnInsertion
    └── [叶节点] removeAt(i) + adjustUpperBoundOnRemoval
    └── [内部节点且找到] child.removeMax() + adjustUpperBoundOnRemoval

iterator.FirstOverlap(item)
└── reset()
└── constrainMinSearchBounds(item)  ← 二分查找软下界
└── constrainMaxSearchBounds(item)  ← 二分查找硬上界
└── findNextOverlap(item)
    ├── 内部节点：child.max().contains(item) ? descend : skip（剪枝）
    ├── 到达 constrMaxPos → 终止
    ├── 到达 constrMinPos → inConstrMin=true
    └── 叶节点：inConstrMin ? 快路径 return : upperBound.contains ? 慢路径 return : pos++

iterator.NextOverlap(item)
└── pos++
└── findNextOverlap(item)  ← 沿用已有的 overlapScan 状态（约束已精化）

btree.Clone()             ← O(1)
└── c.root.incRef()       ← 仅根节点 ref++

node.decRef(recursive)    ← COW 引用计数回收
└── atomic.AddInt32(&n.ref, -1)
└── [ref→0 && recursive] 递归 decRef 子节点
└── 清空并放回 leafPool/nodePool
```

---

*文档生成时间：2026-02-28*
*分析文件版本：CockroachDB main branch（commit cc6689469ff 附近）*
*作者：Claude Code（基于源代码静态分析）*
