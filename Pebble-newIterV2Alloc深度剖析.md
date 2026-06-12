# Pebble newIterV2Alloc 深度剖析——Iterator V2 的对象池、预分配缓冲与复用生命周期

## 一、先给结论

`newIterV2Alloc()` 是 Pebble 创建 **IteratorStackV2** 时用来获取一整套迭代器分配缓冲的函数。

源码位置：

```go
func newIterV2Alloc() *iterV2Alloc {
    buf := iterV2AllocPool.Get().(*iterV2Alloc)
    if invariants.Enabled {
        buf.assertZeroed()
    }
    return buf
}
```

它本身很短，但背后的含义很多：

```text
1. 从 sync.Pool 里拿一个 *iterV2Alloc。
2. 如果池里没有，就通过 New 创建一个新的。
3. invariants 模式下检查这个对象是否已经被正确清零。
4. 返回给 DB.newIter / Iterator.Clone 使用。
```

一句话：

```text
newIterV2Alloc 不是“创建一个普通小对象”，
而是从对象池里借出一整套 Iterator V2 运行时需要的结构体和预分配数组。
```

它服务的是 Pebble 热路径：

```text
DB.NewIter
  -> DB.newIter
     -> newIterV2Alloc
        -> Iterator 使用 allocV2 中的 dbi / mergingIterV2 / levelIterV2 / InterleavingIter
           -> Iterator.Close
              -> 清理状态
              -> iterV2AllocPool.Put
```

## 二、它解决什么问题

Pebble 是 LSM 存储引擎。

一次普通迭代，比如：

```go
iter, _ := db.NewIter(nil)
defer iter.Close()

for valid := iter.SeekGE([]byte("user:1")); valid; valid = iter.Next() {
    ...
}
```

看起来只是创建一个 `Iterator`。

但底层不是一个简单游标。

它可能要同时读这些来源：

```text
1. indexed batch 里的未提交写入。
2. mutable memtable。
3. immutable memtables。
4. L0 的多个 sublevel。
5. L1-L6 的 SST 文件。
6. range deletion。
7. range key。
```

Iterator V2 需要把这些来源合并成一个有序视图：

```text
batch + memtables + L0 + L1 + ... + L6
          ↓
      mergingIterV2
          ↓
      Iterator 对外暴露 Seek/Next/Prev
```

如果每次 `NewIter()` 都临时分配所有中间结构：

```text
1. Iterator 本体分配一次。
2. mergingIterV2 分配一次。
3. heap items 分配一次。
4. level iterators 分配多次。
5. interleaving iterators 分配多次。
6. batch state 分配一次。
7. key/bounds buffer 分配多次。
```

在 CockroachDB 这种大量 scan / lookup 的系统里，GC 压力会非常高。

所以 Pebble 把这些东西打包进一个大对象：

```text
iterV2Alloc
```

并用 `sync.Pool` 复用。

## 三、核心数据结构

### 3.1 iterAllocCommon：V1/V2 共用部分

源码：

```go
type iterAllocCommon struct {
    keyBuf              []byte    `invariants:"reused"`
    boundsBuf           [2][]byte `invariants:"reused"`
    prefixOrFullSeekKey []byte    `invariants:"reused"`
    batchState          iteratorBatchState
    dbi                 Iterator
}
```

它是 V1 和 V2 iterator 分配结构的公共部分。

字段含义：

| 字段 | 作用 |
|---|---|
| `keyBuf` | Iterator 当前 key 的复用 buffer |
| `boundsBuf` | LowerBound / UpperBound 的复用 buffer |
| `prefixOrFullSeekKey` | prefix seek / full seek 用的复用 key buffer |
| `batchState` | indexed batch 相关的 iterator 状态 |
| `dbi` | 真正对外返回的 `Iterator` 对象 |

注意这里最关键的是：

```text
Iterator 本体也被嵌在 iterAllocCommon 里。
```

所以 `newIterV2Alloc()` 不是只给 Iterator 分配辅助内存。

它连 `Iterator` 本体也一起复用。

### 3.2 iterV2Alloc：IteratorStackV2 专用部分

源码：

```go
type iterV2Alloc struct {
    iterAllocCommon
    merging mergingIterV2
    interleavingIters [2]iterv2.InterleavingIter
    levelIters [3 + numLevels]levelIterV2
}
```

字段含义：

| 字段 | 作用 |
|---|---|
| `iterAllocCommon` | Iterator 本体和公共 buffer |
| `merging` | V2 合并迭代器，负责合并多个子 iterator |
| `interleavingIters` | 预分配的 memtable interleaving iterator |
| `levelIters` | 预分配的 L0/L1+ level iterator |

它和 V1 的 `iterAlloc` 类似，但 V2 用的是：

```text
mergingIterV2
levelIterV2
iterv2.InterleavingIter
```

### 3.3 为什么 `interleavingIters` 是 `[2]`

注释里写：

```go
// We need InterleavingIters for the memtable iterators.
interleavingIters [2]iterv2.InterleavingIter
```

memtable 迭代时，不只是点 key。

它还可能要把：

```text
point keys
range deletions
```

交织成一个统一的 span/point 视图。

`InterleavingIter` 的作用就是把 point iterator 和 range deletion iterator 包起来。

预分配 `[2]` 是为了覆盖常见场景：

```text
mutable memtable
immutable memtable
```

如果超过预分配数量，builder 会退化成堆分配：

```go
return &iterv2.InterleavingIter{}
```

所以 `[2]` 不是硬上限。

它是：

```text
常见情况的零额外分配优化。
```

### 3.4 为什么 `levelIters` 是 `[3 + numLevels]`

源码注释：

```go
// Preallocate enough level iterators for 4 L0 sublevels.
levelIters [3 + numLevels]levelIterV2
```

Pebble 默认有多个 LSM level：

```text
L0
L1
L2
...
L6
```

但 L0 特殊。

L0 可以有多个 sublevel，因为 L0 文件之间可能 key range 重叠。

`[3 + numLevels]` 的意思大致是：

```text
numLevels 覆盖常规 level
额外 3 个覆盖常见的 L0 sublevel 增量
```

注释说“enough for 4 L0 sublevels”，是因为 `numLevels` 里已经包含一个 L0 的基础位置，再额外加 3 个，就能覆盖 4 个 L0 sublevel 的常见情况。

如果 L0 sublevels 更多怎么办？

不会坏。

builder 里有兜底：

```go
func (bld *pointIterV2Builder) LevelIter() *levelIterV2 {
    if len(bld.levelIters) > 0 {
        iter := &bld.levelIters[0]
        bld.levelIters = bld.levelIters[1:]
        return iter
    }
    return &levelIterV2{}
}
```

预分配数组用完后，就堆分配新的 `levelIterV2`。

所以这里也是：

```text
常见情况无分配；
极端情况仍然正确。
```

## 四、newIterV2Alloc 的逐行解释

### 4.1 从对象池取对象

```go
buf := iterV2AllocPool.Get().(*iterV2Alloc)
```

`iterV2AllocPool` 是：

```go
var iterV2AllocPool = sync.Pool{
    New: func() interface{} {
        return &iterV2Alloc{}
    },
}
```

`sync.Pool.Get()` 的语义：

```text
1. 如果当前 P 的本地池里有对象，就取一个。
2. 否则可能从 victim cache / 其它路径取。
3. 如果取不到，就调用 New 创建新对象。
```

所以这里通常有两种情况：

```text
热路径：
  之前 Close 的 Iterator 把 iterV2Alloc 放回池里，
  这里直接拿出来复用。

冷路径：
  池里没有对象，
  New 返回 &iterV2Alloc{}。
```

类型断言：

```go
.(*iterV2Alloc)
```

是因为 `sync.Pool` 的 API 返回 `any`。

### 4.2 invariants 模式下检查清零

```go
if invariants.Enabled {
    buf.assertZeroed()
}
```

Pebble 有 invariants 构建模式，用来做更强的内部正确性检查。

`assertZeroed()` 会递归检查：

```text
这个从池里拿出来的对象，除了明确标记为 reused 的字段外，
其它字段都必须是零值。
```

为什么要检查？

因为对象池复用最怕一种 bug：

```text
上一个 Iterator 的状态没有清干净，
下一个 Iterator 拿到后继承了脏状态。
```

例如：

```text
上一个 iterator 的 upper bound = "m"
Close 时没清掉
下一个 iterator 没设置 upper bound
结果莫名只读到 "m" 前
```

这类 bug 很隐蔽。

所以 invariants 模式下会强制检查：

```text
你放回池之前必须清理干净。
```

### 4.3 返回 buffer

```go
return buf
```

返回后，调用方会把它嵌入到 `Iterator` 的生命周期里。

注意：

```text
调用方拿到的是借来的对象。
最终必须通过 Iterator.Close 归还。
```

用户代码不是直接调用 `iterV2AllocPool.Put()`。

用户只需要：

```go
iter, _ := db.NewIter(nil)
defer iter.Close()
```

如果忘记 `Close()`：

```text
1. readState ref 不释放。
2. SST / memtable 可能被 pin。
3. iterV2Alloc 不能回池。
4. 内存和文件引用都可能增长。
```

## 五、创建路径：它在哪里被调用

### 5.1 DB.newIter 中选择 V2

核心代码：

```go
var buf *iterAllocCommon
if d.opts.IteratorStack == IteratorStackV2 {
    a := newIterV2Alloc()
    a.dbi.allocV2 = a
    buf = &a.iterAllocCommon
} else {
    a := newIterAlloc()
    a.dbi.alloc = a
    buf = &a.iterAllocCommon
}
dbi := &buf.dbi
```

意思是：

```text
如果配置使用 IteratorStackV2：
  从 iterV2AllocPool 借一个 iterV2Alloc。
  把这个 alloc 记录到 Iterator.allocV2。
  后续通过 &buf.dbi 拿到 Iterator 本体。

否则：
  使用 V1 的 iterAlloc。
```

关键点：

```text
Iterator 是 alloc 结构体内部的字段，不是单独 new 出来的。
```

内存结构可以这样理解：

```text
iterV2Alloc
├── iterAllocCommon
│   ├── keyBuf
│   ├── boundsBuf
│   ├── prefixOrFullSeekKey
│   ├── batchState
│   └── dbi  <------------- 对外返回的 *Iterator 指向这里
├── merging
├── interleavingIters
└── levelIters
```

### 5.2 Iterator 反向指回 allocV2

```go
a.dbi.allocV2 = a
```

这个字段非常重要。

`Iterator.Close()` 时，它要知道：

```text
我是从哪个池借出来的？
关闭时应该还给 V1 池还是 V2 池？
```

所以 `Iterator` 里有两个字段：

```go
alloc   *iterAlloc
allocV2 *iterV2Alloc
```

注释说：

```text
Exactly one of alloc and allocV2 is non-nil.
```

也就是：

```text
V1 iterator:
  alloc != nil
  allocV2 == nil

V2 iterator:
  alloc == nil
  allocV2 != nil
```

### 5.3 填充 Iterator 字段

拿到 `dbi := &buf.dbi` 后，`DB.newIter` 会填充：

```go
dbi.ctx = ctx
dbi.merge = d.merge
dbi.comparer = d.opts.Comparer
dbi.readState = readState
dbi.version = newIterOpts.snapshot.vers
dbi.keyBuf = buf.keyBuf
dbi.prefixOrFullSeekKey = buf.prefixOrFullSeekKey
dbi.boundsBuf = buf.boundsBuf
dbi.fc = d.fileCache
dbi.seqNum = seqNum
```

注意几个 buffer：

```text
dbi.keyBuf = buf.keyBuf
dbi.prefixOrFullSeekKey = buf.prefixOrFullSeekKey
dbi.boundsBuf = buf.boundsBuf
```

这些是从上一次 iterator Close 后保留下来的小 buffer。

作用是减少重复分配：

```text
当前 key 拷贝
bounds 拷贝
prefix seek key 拷贝
```

## 六、V2 iterator 树如何使用 iterV2Alloc

### 6.1 constructPointIterV2

V2 的点 key iterator 栈在这里构造：

```go
func (i *Iterator) constructPointIterV2(
    ctx context.Context, memtables flushableList, internalOpts internalIterOpts,
) {
    buf := i.allocV2
    ...
}
```

它先计算需要合并多少个 child：

```text
1. TriggerIter
2. indexed batch
3. memtables
4. ingested flushable
5. L0 sublevels
6. L1+
```

然后创建 builder：

```go
bld := makePointIterV2Builder(
    i.allocV2,
    numMergingLevels,
    i.comparer,
    i.opts.LowerBound,
    i.opts.UpperBound,
)
```

### 6.2 makePointIterV2Builder

源码：

```go
func makePointIterV2Builder(
    buf *iterV2Alloc, numLevels int, cmp *base.Comparer, lower, upper []byte,
) pointIterV2Builder {
    bld := pointIterV2Builder{
        levels:            slices.Grow(buf.merging.levels[:0], numLevels),
        interleavingIters: buf.interleavingIters[:],
        levelIters:        buf.levelIters[:],
        ...
    }
    return bld
}
```

这里做了三件事：

```text
1. 复用 buf.merging.levels 作为 merging levels slice。
2. 把 buf.interleavingIters 交给 builder。
3. 把 buf.levelIters 交给 builder。
```

`slices.Grow(buf.merging.levels[:0], numLevels)` 的意思：

```text
把 levels 长度重置为 0；
如果容量不够 numLevels，就扩容；
如果容量够，就复用原 backing array。
```

这就是 V2 alloc 的核心：

```text
用一个池化大对象承载多个运行时 slice/struct，
下次 iterator 创建时尽量复用这些 backing array 和结构体。
```

### 6.3 InterleavingIter 的分配策略

```go
func (bld *pointIterV2Builder) InterleavingIter() *iterv2.InterleavingIter {
    if len(bld.interleavingIters) > 0 {
        iter := &bld.interleavingIters[0]
        bld.interleavingIters = bld.interleavingIters[1:]
        return iter
    }
    return &iterv2.InterleavingIter{}
}
```

含义：

```text
优先从 iterV2Alloc 的预分配数组里切一个；
如果预分配数组用完，再堆分配。
```

### 6.4 levelIterV2 的分配策略

```go
func (bld *pointIterV2Builder) LevelIter() *levelIterV2 {
    if len(bld.levelIters) > 0 {
        iter := &bld.levelIters[0]
        bld.levelIters = bld.levelIters[1:]
        return iter
    }
    return &levelIterV2{}
}
```

同样是：

```text
常见情况从预分配数组里拿；
极端情况堆分配。
```

### 6.5 mergingIterV2 的初始化

构造完所有 child levels 后：

```go
m := &buf.merging
m.Init(
    i.comparer,
    i.opts.getLogger(),
    i.opts.LowerBound,
    i.opts.UpperBound,
    &i.stats.InternalStats,
    i.seqNum,
    batchSnapshot,
    batchLevelIdx,
    bld.Levels(),
)
i.mergingV2 = m
i.pointIter = m
```

也就是说：

```text
iterV2Alloc.merging 是当前 Iterator 的点 key 合并迭代器。
```

`mergingIterV2` 本身负责：

```text
1. 持有所有 child levels。
2. 用 heap 找当前最小/最大 key。
3. 正向迭代用 min-heap。
4. 反向迭代用 max-heap。
5. 根据 sequence number 隐藏不可见版本。
6. 根据 range deletion 隐藏被删除覆盖的 point key。
```

## 七、关闭路径：对象怎么归还

`newIterV2Alloc()` 借出来的对象，最终在 `Iterator.Close()` 里归还。

关键代码：

```go
allocV1 := i.alloc
allocV2 := i.allocV2

var alloc *iterAllocCommon
switch {
case allocV1 != nil:
    alloc = &allocV1.iterAllocCommon
case allocV2 != nil:
    alloc = &allocV2.iterAllocCommon
default:
    return err
}
```

这里先判断当前 Iterator 是 V1 还是 V2。

对 V2 来说：

```text
allocV2 != nil
alloc = &allocV2.iterAllocCommon
```

### 7.1 保留小 buffer

Close 里会尝试保留小 buffer：

```go
maybeReuseKeyBuf(&alloc.keyBuf, i.keyBuf)
maybeReuseKeyBuf(&alloc.prefixOrFullSeekKey, i.prefixOrFullSeekKey)
for j := range i.boundsBuf {
    maybeReuseKeyBuf(&alloc.boundsBuf[j], i.boundsBuf[j])
}
```

`maybeReuseKeyBuf`：

```go
const maxKeyBufCacheSize = 4 << 10 // 4 KB

func maybeReuseKeyBuf(save *[]byte, buf []byte) {
    if cap(buf) < maxKeyBufCacheSize {
        *save = buf
    }
}
```

含义：

```text
小 buffer 保留，方便下次复用。
大 buffer 不保留，避免一个偶发大 key 把池对象长期撑大。
```

例如：

```text
一次 scan 遇到 1MB key buffer。
如果直接放回池并长期保留，
后续每个普通 iterator 都可能挂着 1MB backing array。
```

所以 Pebble 只缓存小于 4KB 的 key buffer。

### 7.2 清理 Iterator 本体

```go
alloc.dbi.clearForReuse()
```

`clearForReuse` 使用 unsafe 按字节清零一段 Iterator 字段：

```go
func (i *Iterator) clearForReuse() {
    *(*[clearLen]byte)(unsafe.Add(unsafe.Pointer(i), clearOff)) = [clearLen]byte{}
}
```

为什么不用：

```go
*i = Iterator{}
```

注释里说了原因：

```text
1. 避免 stack allocated autotmp iterAlloc。
2. 可以更细粒度控制哪些字段清零、哪些 buffer 保留。
```

这是性能热路径里的写法。

### 7.3 V2 专用清理

V2 分支：

```go
allocV2.merging.PrepareForReuse()
clear(allocV2.interleavingIters[:])
clear(allocV2.levelIters[:])
iterV2AllocPool.Put(allocV2)
```

逐个看。

#### merging.PrepareForReuse

```go
func (m *mergingIterV2) PrepareForReuse() {
    levels := m.levels
    items := m.heap.items
    *m = mergingIterV2{}
    m.levels = levels[:0]
    m.heap.items = items[:0]
}
```

它清掉 `mergingIterV2` 的逻辑状态，但保留：

```text
1. levels 的 backing array。
2. heap.items 的 backing array。
```

目的：

```text
下一个 iterator 构造 mergingIterV2 时可以复用这些数组。
```

#### clear interleavingIters / levelIters

```go
clear(allocV2.interleavingIters[:])
clear(allocV2.levelIters[:])
```

这两个是固定数组，里面可能含有上一个 iterator 的状态和引用。

必须清掉。

否则可能出现：

```text
1. 下一个 iterator 继承旧状态。
2. 对旧 SST / memtable / bounds 的引用没断开。
3. GC 无法释放旧对象。
```

#### 放回池

```go
iterV2AllocPool.Put(allocV2)
```

到这里，生命周期闭环：

```text
newIterV2Alloc.Get
  -> Iterator 使用
     -> Iterator.Close
        -> Put
```

## 八、完整生命周期图

```mermaid
flowchart TD
    A[DB.NewIter / Iterator.Clone] --> B{IteratorStack == V2?}
    B -- yes --> C[newIterV2Alloc]
    C --> D[iterV2AllocPool.Get]
    D --> E{pool 有对象?}
    E -- no --> F[New: &iterV2Alloc{}]
    E -- yes --> G[复用旧 alloc]
    F --> H[invariants assertZeroed]
    G --> H
    H --> I[a.dbi.allocV2 = a]
    I --> J[dbi := &a.iterAllocCommon.dbi]
    J --> K[填充 Iterator 字段]
    K --> L[finishInitializingIter]
    L --> M[constructPointIterV2]
    M --> N[复用 merging / interleavingIters / levelIters]
    N --> O[Iterator 对外使用 Seek/Next/Prev]
    O --> P[Iterator.Close]
    P --> Q[关闭 child iterators / release readState]
    Q --> R[保留小 key/bounds buffer]
    R --> S[clear Iterator / V2 专用状态]
    S --> T[iterV2AllocPool.Put]
    T --> D
```

## 九、实际例子

### 9.1 普通 DB.NewIter

用户代码：

```go
iter, _ := db.NewIter(nil)
defer iter.Close()
```

底层：

```text
1. DB.NewIter 调 DB.newIter。
2. 如果 IteratorStack == IteratorStackV2，调用 newIterV2Alloc。
3. 从 iterV2AllocPool 借一个 alloc。
4. 使用 alloc.dbi 作为 Iterator 本体。
5. 使用 alloc.merging 构造 mergingIterV2。
6. 使用 alloc.interleavingIters 包装 memtable iterators。
7. 使用 alloc.levelIters 构造 L0/L1+ iterators。
8. 返回 *Iterator。
9. Iterator.Close 时清理并放回池。
```

### 9.2 L0 sublevels 很多

假设当前 LSM：

```text
L0 有 10 个 sublevels
L1-L6 都有文件
```

`iterV2Alloc.levelIters` 预分配：

```text
3 + numLevels
```

可能不够。

这时：

```go
LevelIter()
```

会在预分配数组用完后：

```go
return &levelIterV2{}
```

所以结果是：

```text
前面一部分 levelIterV2 无分配复用；
超出的部分堆分配；
功能不受影响。
```

这就是典型设计：

```text
优化常见路径，不牺牲极端路径正确性。
```

### 9.3 一次大 key 不会污染池

假设某次 iterator 读到了很大的 key，导致：

```text
i.keyBuf cap = 1MB
```

Close 时：

```go
maybeReuseKeyBuf(&alloc.keyBuf, i.keyBuf)
```

发现：

```text
cap(buf) >= 4KB
```

于是不会保存到 `alloc.keyBuf`。

结果：

```text
这个 1MB backing array 不会跟着 iterV2AllocPool 长期存活。
```

这是对象池设计里非常重要的一点：

```text
复用小对象，拒绝缓存异常大对象。
```

### 9.4 忘记 Close 会怎样

错误用法：

```go
iter, _ := db.NewIter(nil)
iter.SeekGE([]byte("a"))
// 忘记 iter.Close()
```

后果：

```text
1. iterV2Alloc 不会回到 pool。
2. readState / version 引用不释放。
3. SST 文件可能不能被删除。
4. memtable / compaction 相关资源可能被 pin。
5. 长期看像内存或磁盘泄漏。
```

所以 Pebble Iterator 的使用规范是：

```go
iter, err := db.NewIter(opts)
if err != nil {
    return err
}
defer iter.Close()
```

## 十、为什么不用每次 new

如果每次都：

```go
iter := &Iterator{}
merging := &mergingIterV2{}
levels := make([]levelIterV2, n)
interleaving := make([]InterleavingIter, m)
```

问题是：

```text
1. 每个 iterator 创建产生多次分配。
2. scan/get 热路径 GC 压力增加。
3. CPU 花在 allocator 和 GC 上。
4. 内存局部性更差。
```

`iterV2Alloc` 的设计是：

```text
把一组生命周期完全相同的对象放在一起分配和回收。
```

这些对象生命周期一致：

```text
Iterator 创建时一起需要；
Iterator Close 时一起不用。
```

所以适合打包：

```text
Iterator 本体
batch state
merging iterator
level iterators
interleaving iterators
临时 key/bounds buffer
```

## 十一、为什么用 sync.Pool

`sync.Pool` 适合：

```text
1. 临时对象。
2. 高频分配。
3. 对象可以被任意 goroutine 复用。
4. 丢了也没关系。
```

Iterator alloc 正好符合：

```text
1. NewIter 创建。
2. Close 归还。
3. 下次 NewIter 可复用。
4. GC 可以随时清掉 pool 里的对象，不影响正确性。
```

注意：

```text
sync.Pool 不是缓存语义。
```

你不能依赖：

```text
Put 进去的对象一定会被 Get 出来。
```

它只是性能优化。

如果 GC 清掉 pool：

```text
newIterV2Alloc 会通过 New 创建新的 iterV2Alloc。
```

正确性不依赖池。

## 十二、和 V1 iterAlloc 的区别

V1：

```go
type iterAlloc struct {
    iterAllocCommon
    merging          mergingIter
    mlevels          [3 + numLevels]mergingIterLevel
    levels           [3 + numLevels]levelIter
    levelsPositioned [3 + numLevels]bool
}
```

V2：

```go
type iterV2Alloc struct {
    iterAllocCommon
    merging mergingIterV2
    interleavingIters [2]iterv2.InterleavingIter
    levelIters [3 + numLevels]levelIterV2
}
```

差异：

| 维度 | V1 | V2 |
|---|---|---|
| 合并器 | `mergingIter` | `mergingIterV2` |
| level iterator | `levelIter` | `levelIterV2` |
| range deletion 处理 | V1 旧路径 | V2 通过 span/interleaving/slab 模型 |
| 预分配结构 | `mlevels/levels/levelsPositioned` | `interleavingIters/levelIters` |
| 归还池 | `iterAllocPool` | `iterV2AllocPool` |

`newIterV2Alloc` 就是 V2 版本的池化入口。

## 十三、关键设计模式

### 13.1 Object Pool

```text
iterV2AllocPool = sync.Pool
```

减少热路径重复分配。

### 13.2 Arena-like Bundling

虽然不是传统 arena，但思想类似：

```text
把生命周期相同的一组对象打包到一个大结构里。
```

这样：

```text
一次借出
一起使用
一次归还
```

### 13.3 Fast Path Preallocation

固定数组：

```go
interleavingIters [2]...
levelIters [3 + numLevels]...
```

优化常见路径。

不够时堆分配，保证极端路径正确。

### 13.4 Defensive Zeroing

`Iterator.Close()` 清零，`invariants.Enabled` 下 `assertZeroed()` 验证。

这是对象池复用的核心安全措施。

## 十四、最终心智模型

把 `newIterV2Alloc()` 想成：

```text
Iterator V2 的“装备包领取处”。
```

每个装备包里有：

```text
1. 一个 Iterator 本体。
2. 一个 mergingIterV2。
3. 一组常用 levelIterV2。
4. 一组常用 InterleavingIter。
5. key/bounds/prefix buffer。
6. indexed batch state。
```

使用流程：

```text
领取装备包:
  newIterV2Alloc()

装配 iterator:
  DB.newIter / constructPointIterV2

执行扫描:
  Iterator.SeekGE / Next / Prev

清理装备包:
  Iterator.Close

归还装备包:
  iterV2AllocPool.Put
```

最终记住这句话：

```text
newIterV2Alloc 的价值不在函数本身的 5 行代码，
而在它把 Iterator V2 创建过程中的一批高频小分配合并成一个可复用的大对象，
从而降低 Pebble 读路径的分配次数和 GC 压力。
```

