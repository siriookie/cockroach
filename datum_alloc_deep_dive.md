# DatumAlloc 深度解析：SQL 执行层的批量内存分配机制

> **分析文件**：`pkg/sql/sem/tree/datum_alloc.go`
> **关联文件**：`pkg/sql/colconv/vec_to_datum.eg.go`、`pkg/util/nocopy.go`
> **方法论**：BFS 控制流 → DFS 关键函数 → 具体运行示例 → 与常见 Alloc 算法对比

---

## 一、背景：为什么需要 DatumAlloc？

### 1.1 问题根源：Datum 是接口类型，必须堆分配

CockroachDB SQL 层的所有值都以 `Datum` 接口表示：

```go
// pkg/sql/sem/tree/datum.go（概念示意）
type Datum interface {
    // ... 40+ 个方法
}

type DInt int64      // 实现 Datum
type DFloat float64  // 实现 Datum
type DString string  // 实现 Datum
// ... 约 30 种具体类型
```

当代码持有 `Datum` 接口值时，Go runtime 将其表示为 `(type_pointer, data_pointer)` 两个 word 的 fat pointer。若值本身是指针类型（如 `*DInt`），则 data_pointer 指向堆上的 DInt。

**核心矛盾**：SQL 查询在处理一批行时（如 `SELECT * FROM t LIMIT 1024`），需要为每行的每列分配一个 Datum 指针。若直接 `new(DInt)` × N，则产生 N 次独立的 `runtime.mallocgc` 调用，每次调用需要：
- 从 GC 维护的 `mcache` 中获取内存
- 更新 GC 元数据（写屏障）
- 在 GC 压力下可能触发 STW（Stop-The-World）

在 OLAP 场景中，一个查询可能涉及 **百万级** Datum 分配，逐个 `new` 是无法接受的。

### 1.2 解决方案：Slab 分配器

`DatumAlloc` 是一个**类型专化的 Slab 分配器**（Type-Specialized Slab Allocator）：

```
传统方式:                          DatumAlloc 方式:
new(DInt) → GC heap               首次: make([]DInt, 16) → GC heap (1次)
new(DInt) → GC heap               取 &buf[0], buf = buf[1:]
new(DInt) → GC heap               取 &buf[1], buf = buf[1:]
...                                ...（共 16 次，只有 1 次 GC 分配）
new(DInt) → GC heap               buf 耗尽 → 再 make([]DInt, 16)
（N 次独立 GC 分配）               （N/16 次 GC 分配，N 次取地址）
```

---

## 二、第一轮 BFS：数据结构与整体协作

### 2.1 数据结构全景

```go
// datum_alloc.go:18-63
type DatumAlloc struct {
    _                util.NoCopy   // ← 编译期防拷贝保护

    DefaultAllocSize int           // 下次批量分配的大小（可动态调整）
    typeAllocSizes   typeSizes     // 按类型细化的批量大小（预测 hint）

    // ─── 每种 Datum 类型一个独立 slice buffer ───
    datumAlloc        []Datum       // Datum 接口 slice（NewDatums 用）
    dintAlloc         []DInt
    dfloatAlloc       []DFloat
    dbitArrayAlloc    []DBitArray
    ddecimalAlloc     []DDecimal
    ddateAlloc        []DDate
    denumAlloc        []DEnum
    // ... 约 20 个类型 buffer

    // ─── 字符串类型共享一个 buffer ───
    stringAlloc       []string      // DString / DBytes / DEncodedKey 共用

    // ─── 地理空间类型特殊处理 ───
    ewkbAlloc               []byte
    curEWKBAllocSize        int
    lastEWKBBeyondAllocSize bool

    env CollationEnvironment        // 字符集排序环境（复用，避免重复初始化）
}
```

**关键常量**：
```go
const defaultDatumAllocSize = 16    // 默认每批分配 16 个（最小批次）
const datumAllocMultiplier = 4      // NewDatums 时的乘数因子
const defaultEWKBAllocSize = 4096   // EWKB 初始 4KB
const maxEWKBAllocSize = 16384      // EWKB 最大 16KB
```

### 2.2 在执行层中的位置

```
┌────────────────────────────────────────────────┐
│  colexec / colfetcher（向量化执行引擎）          │
│    VecToDatumConverter.ConvertBatch()           │
│      ├── da.ResetTypeAllocSizes()               │
│      ├── da.AddTypeAllocSize(batchLen, type)    │  ← 预测分配量
│      └── ColVecToDatum(converted, col, ..., da) │
│            └── da.NewDInt / NewDString / ...    │
├────────────────────────────────────────────────┤
│  tree.DatumAlloc  ← 本文分析                    │  内存分配层
│    ├── 类型 buffer 池（每个类型独立 slice）       │
│    ├── typeAllocSizes（预测 hint）               │
│    └── EWKB 特殊分配器                          │
├────────────────────────────────────────────────┤
│  Go runtime (mallocgc)                          │  实际内存
└────────────────────────────────────────────────┘
```

**触发方式**：
- **主动调用**：执行器在处理每个 batch 前调用 `ResetTypeAllocSizes` + `AddTypeAllocSize` 预告分配需求。
- **按需分配**：`NewDInt` 等方法在 buffer 耗尽时才触发 `make()`，属于惰性扩展。
- **非并发**：`DatumAlloc` 不是线程安全的（`NoCopy` 也部分强制了这一点），每个执行上下文持有自己的 `DatumAlloc`。

---

## 三、DFS 深入：关键机制逐层解析

### 3.1 `util.NoCopy` — 编译期防拷贝保护

```go
// pkg/util/nocopy.go
type NoCopy struct{}
func (*NoCopy) Lock()   {}
func (*NoCopy) Unlock() {}
```

```go
// datum_alloc.go:19
type DatumAlloc struct {
    _ util.NoCopy   // 嵌入匿名字段
    // ...
}
```

**工作原理**：Go 的 `go vet` 工具内置了 `-copylocks` 检查器：若一个结构体实现了 `Lock()/Unlock()` 方法（即实现了 `sync.Locker` 接口），则该结构体被认为是"锁语义"对象，拷贝时 vet 会报错。

`NoCopy` 实现了 `Lock()/Unlock()` 但实际上是空方法（no-op），所以：
- **运行时**：零开销，什么都不做。
- **静态检查时**：`go vet` 看到 `DatumAlloc` 包含实现 Locker 的字段，任何 `v2 := v1`（值拷贝）都会触发 vet 警告。

**为什么 DatumAlloc 不能被拷贝？**

```go
// 危险示例（vet 会报警）
original := tree.DatumAlloc{}
original.DefaultAllocSize = 128

copy := original  // ← vet 警告！

// 此后 original 和 copy 共享同一个 dintAlloc slice 底层数组
// 但 slice header（len/cap）是独立拷贝的
// original.NewDInt(1) 返回 &buf[0]，并将 buf 更新为 buf[1:]
// copy.NewDInt(2) 也返回 &buf[0]（因为 copy.buf 还是旧的 header）
// ← 两个指针指向同一个内存位置！数据被覆盖
```

所以 `NoCopy` 是正确性的保证，而不仅仅是性能优化。文档注释明确写道：`NOTE: it *must* be passed in by a pointer.`

---

### 3.2 标准 Slab 分配模式 — 以 `NewDInt` 为例

```go
// datum_alloc.go:152-172
func (a *DatumAlloc) NewDInt(v DInt) *DInt {
    if a == nil {                    // ← ① nil 保护
        r := new(DInt)
        *r = v
        return r
    }
    buf := &a.dintAlloc              // ← ② 取 buffer 的指针（间接寻址）
    if len(*buf) == 0 {              // ← ③ buffer 耗尽检测
        allocSize := defaultDatumAllocSize   // 默认 16
        if a.typeAllocSizes.ints != 0 {
            allocSize = a.typeAllocSizes.ints   // ← ④ 类型专化 hint 优先
        } else if a.DefaultAllocSize != 0 {
            allocSize = a.DefaultAllocSize       // ← ⑤ 全局 hint 次之
        }
        *buf = make([]DInt, allocSize)          // ← ⑥ 一次性批量分配
    }
    r := &(*buf)[0]                  // ← ⑦ 取第一个元素的地址
    *r = v                           // ← ⑧ 写入值
    *buf = (*buf)[1:]                // ← ⑨ 将 slice header 向前移动一位
    return r
}
```

**逐步解析关键细节**：

**① nil 保护（回退路径）**

调用方可以传入 `nil` 的 `*DatumAlloc`（表示"不需要批量分配"），此时回退到标准 `new(DInt)`。这使得 `DatumAlloc` 的使用是**可选的**，不破坏现有代码。

**② `buf := &a.dintAlloc` — 为什么取指针？**

若写成 `buf := a.dintAlloc`，buf 是 slice header 的**副本**。步骤 ⑨ 的 `buf = buf[1:]` 只修改局部变量，`a.dintAlloc` 不变，下次调用仍然返回同一个 `&buf[0]` — 严重 bug。

取指针 `buf := &a.dintAlloc` 后，`*buf = (*buf)[1:]` 直接修改 `a.dintAlloc` 的 slice header，效果持久。

**③④⑤ 三级分配大小决策（优先级从高到低）**：

```
typeAllocSizes.ints（类型专化预测）
    > DefaultAllocSize（全局预测）
        > defaultDatumAllocSize（常量默认值 16）
```

这三级形成了一个**分级 hint 系统**：调用方（如 `ColVecToDatum`）在批量转换前通过 `AddTypeAllocSize` 提供精确预测，若没有预测则退回全局 hint，最差情况用默认值 16。

**⑦⑨ Slab 核心操作**：

```
初始状态:  buf = [DInt₀, DInt₁, ..., DInt₁₅]
                  ^len=16, cap=16

NewDInt(1):
  r = &buf[0]          → r 指向 DInt₀
  *r = DInt(1)         → DInt₀ = 1
  buf = buf[1:]        → buf = [DInt₁, ..., DInt₁₅], len=15, cap=15
  return r             → 返回 &DInt₀

NewDInt(2):
  r = &buf[0]          → r 指向 DInt₁
  *r = DInt(2)         → DInt₁ = 2
  buf = buf[1:]        → buf = [DInt₂, ..., DInt₁₅], len=14, cap=14
  return r             → 返回 &DInt₁

...（共 16 次，零额外 GC 分配）

NewDInt(17):           ← 第 17 次，buf 耗尽
  len(*buf) == 0 → make([]DInt, allocSize)
  ... 重新开始
```

**底层内存布局**：

```
GC heap:
  [DInt₀|DInt₁|...|DInt₁₅]   ← 连续的 16*8=128 字节
   ↑          ↑
   &r₁        &r₂              ← 相邻地址，缓存友好！
```

这与逐个 `new(DInt)` 完全不同——后者每个 DInt 在堆上的位置是分散的。

---

### 3.3 `NewDatums` — 批量分配 Datum 接口 slice

```go
// datum_alloc.go:131-149
func (a *DatumAlloc) NewDatums(num int) Datums {
    if a == nil {
        return make(Datums, num)
    }
    buf := &a.datumAlloc
    if len(*buf) < num {                        // ← 注意：< num，不是 == 0
        extensionSize := defaultDatumAllocSize  // 16
        if a.DefaultAllocSize != 0 {
            extensionSize = a.DefaultAllocSize
        }
        if extTupleLen := num * datumAllocMultiplier; extensionSize < extTupleLen {
            extensionSize = extTupleLen          // ← 至少 num*4
        }
        *buf = make(Datums, extensionSize)
    }
    r := (*buf)[:num]        // ← 取前 num 个元素
    *buf = (*buf)[num:]      // ← 推进 num 步
    return r
}
```

**与 `NewDInt` 的关键差异**：

| 维度 | `NewDInt` | `NewDatums` |
|---|---|---|
| 触发重分配条件 | `len == 0`（完全耗尽）| `len < num`（不够用）|
| 每次取用量 | 固定 1 个 | 可变 `num` 个 |
| 最小批次 | `allocSize`（默认 16）| `max(16, num * 4)` |
| 用途 | 单值分配 | 为一行分配 Datum 接口 slice |

**`num * datumAllocMultiplier`（乘数 4）的设计意图**：

若 `num=3`（3 列查询），扩展为 `3*4=12`，比默认的 16 小，用 16。
若 `num=100`（100 列查询），扩展为 `400`，比 16 大，用 400。

这确保了对于宽表（列数多），一次分配能够**预留足够的未来用量**，而不是频繁重分配。

---

### 3.4 `newString()` — 多类型复用同一 buffer

```go
// datum_alloc.go:218-232
func (a *DatumAlloc) newString() *string {
    buf := &a.stringAlloc
    if len(*buf) == 0 {
        allocSize := defaultDatumAllocSize
        if a.typeAllocSizes.strings != 0 {
            allocSize = a.typeAllocSizes.strings
        } else if a.DefaultAllocSize != 0 {
            allocSize = a.DefaultAllocSize
        }
        *buf = make([]string, allocSize)
    }
    r := &(*buf)[0]
    *buf = (*buf)[1:]
    return r
}

// 三种类型都使用同一个 newString() 的底层 buffer
func (a *DatumAlloc) NewDString(v DString) *DString {
    r := (*DString)(a.newString())   // ← 类型转换！
    *r = v
    return r
}
func (a *DatumAlloc) NewDBytes(v DBytes) *DBytes {
    r := (*DBytes)(a.newString())    // ← 同一个 buffer
    *r = v
    return r
}
func (a *DatumAlloc) NewDEncodedKey(v DEncodedKey) *DEncodedKey {
    r := (*DEncodedKey)(a.newString())  // ← 同一个 buffer
    *r = v
    return r
}
```

**类型转换的安全性**：

`DString`、`DBytes`、`DEncodedKey` 在 Go 中都是 `string` 的别名：

```go
type DString  string
type DBytes   string
type DEncodedKey string
```

它们的底层内存布局**完全相同**（都是 `{ptr *byte, len int}`，16 字节）。因此 `(*DString)(stringPtr)` 这样的 unsafe-free 类型转换是完全安全的——它们只是在共享同一块 `[]string` buffer 中取地址，类型系统上安全，内存上合法。

**为什么合并而不是三个独立 buffer？**

在实际查询中，一个 batch 中同时包含 DString、DBytes、DEncodedKey 的情况较少（通常是其中一种）。合并后，三者共享同一个 buffer，**资源利用率更高**，且 `typeAllocSizes.strings` 能正确反映三种类型的总分配需求。

---

### 3.5 EWKB 分配 — 地理空间数据的特殊挑战

地理空间类型（`DGeography`、`DGeometry`）包含一个变长字节序列 EWKB（Extended Well-Known Binary），长度从几十字节到几十 KB 不等。标准的 Slab 分配（固定大小 slice）无法处理变长数据，因此有特殊逻辑。

#### 3.5.1 `giveBytesToEWKB` — 预分配 EWKB 缓冲区

```go
// datum_alloc.go:503-518
func (a *DatumAlloc) giveBytesToEWKB(so *geopb.SpatialObject) {
    if a == nil { return }
    if a.ewkbAlloc == nil && !a.lastEWKBBeyondAllocSize {
        // 没有可用的 ewkb 残留，且上次没超出最大值 → 重新分配
        if a.curEWKBAllocSize == 0 {
            a.curEWKBAllocSize = defaultEWKBAllocSize  // 4096
        } else if a.curEWKBAllocSize < maxEWKBAllocSize {
            a.curEWKBAllocSize *= 2                    // 指数增长
        }
        so.EWKB = make([]byte, 0, a.curEWKBAllocSize) // len=0, cap=N
    } else {
        so.EWKB = a.ewkbAlloc    // 复用上次剩余的空间
        a.ewkbAlloc = nil
    }
}
```

**增长策略**：EWKB 的 buffer 大小按指数增长：4KB → 8KB → 16KB → 停止（`maxEWKBAllocSize=16384`）。这是**指数退避预热（Exponential Warmup）**策略：第一次可能分配不够，后续逐渐适应实际数据大小，最终稳定在 16KB 上限。

#### 3.5.2 `DoneInitNewDGeo` — 分配后归还残留空间

```go
// datum_alloc.go:457-471
func (a *DatumAlloc) DoneInitNewDGeo(so *geopb.SpatialObject) {
    if a == nil { return }
    // 记录本次 EWKB 是否超出了最大预分配大小
    a.lastEWKBBeyondAllocSize = len(so.EWKB) > maxEWKBAllocSize
    c := cap(so.EWKB)
    l := len(so.EWKB)
    if (c - l) > l {
        // 剩余空间 > 已用空间 → 值得归还，避免浪费
        a.ewkbAlloc = so.EWKB[l:l:c]  // 将残余 cap 归还给 ewkbAlloc
        so.EWKB = so.EWKB[:l:l]       // 缩减 SpatialObject 的 cap，防止意外扩展
    }
}
```

**示例**：预分配 4096 字节，实际 EWKB 只用了 512 字节：

```
预分配:     so.EWKB = [0..4095], len=0, cap=4096
反序列化后: so.EWKB = [data..], len=512, cap=4096

DoneInitNewDGeo:
  c=4096, l=512
  (c-l)=3584 > l=512 → 值得归还

  a.ewkbAlloc = so.EWKB[512:512:4096]  ← 3584 字节空间归还
  so.EWKB = so.EWKB[:512:512]          ← SpatialObject 只保留已用部分
```

**下次 giveBytesToEWKB**：`a.ewkbAlloc != nil`，直接使用这 3584 字节，零 GC 分配。

**`lastEWKBBeyondAllocSize` 旗标**：

```go
a.lastEWKBBeyondAllocSize = len(so.EWKB) > maxEWKBAllocSize  // 16KB
```

若某次 EWKB 超过 16KB，设置旗标为 `true`。下次 `giveBytesToEWKB` 看到旗标为 `true`（且 `ewkbAlloc==nil`），**不会**预分配，改为让反序列化自己按需增长。逻辑如下：

```go
if a.ewkbAlloc == nil && !a.lastEWKBBeyondAllocSize {
    // 预分配 curEWKBAllocSize 字节
} else {
    // 复用残留（或者 lastEWKBBeyondAllocSize=true，两个条件都不满足，fall through）
    so.EWKB = a.ewkbAlloc  // 若 ewkbAlloc 也是 nil，则 so.EWKB = nil
}
```

这是一个**自适应策略**：对超大 EWKB（>16KB）放弃预分配，因为预分配多半会浪费；对正常大小的 EWKB 持续预分配复用。

---

### 3.6 `typeAllocSizes` / `ResetTypeAllocSizes` / `AddTypeAllocSize` — 预测驱动的分配

这是与向量化执行引擎协作的核心接口。

**调用方（vec_to_datum_tmpl.go:179-184）**：

```go
c.da.ResetTypeAllocSizes()                          // 清空上一批的 hint
for _, vecIdx := range c.vecIdxsToConvert {
    c.da.AddTypeAllocSize(batchLength, vecs[vecIdx].Type().Family())
    // 例如: AddTypeAllocSize(1024, types.IntFamily)
    //       AddTypeAllocSize(1024, types.FloatFamily)
}
```

**`AddTypeAllocSize` 实现（datum_alloc.go:98-128）**：

```go
func (a *DatumAlloc) AddTypeAllocSize(size int, t types.Family) {
    switch t {
    case types.IntFamily:
        a.typeAllocSizes.ints += size    // 累加，支持多列同类型
    case types.FloatFamily:
        a.typeAllocSizes.floats += size
    // ...
    }
}
```

**效果链路**：

```
调用方预报: da.AddTypeAllocSize(1024, IntFamily)
                     ↓
          typeAllocSizes.ints = 1024
                     ↓
NewDInt 首次调用:  len(buf)==0
  allocSize = typeAllocSizes.ints = 1024   ← 使用预报值
  buf = make([]DInt, 1024)
                     ↓
后续 1024 次 NewDInt 调用: 零 GC 分配！
```

**对比没有 hint 时（仅默认 16）**：

处理 1024 行整数列：
- 无 hint：1024/16 = **64 次** `make([]DInt, 16)` = 64 次 GC 分配
- 有 hint：1024/1024 = **1 次** `make([]DInt, 1024)` = 1 次 GC 分配

这 64 倍的差距在高频查询中非常显著。

---

## 四、具体运行示例

### 4.1 正常场景：处理一个 1024 行、3 列的 batch（Int + String + Float）

```
查询: SELECT id, name, score FROM t LIMIT 1024
列类型: id: INT, name: STRING, score: FLOAT

初始状态:
  da = DatumAlloc{}  // 所有 buffer 均为 nil/empty

─── 阶段1：批量转换前的 hint 预报 ───
da.ResetTypeAllocSizes()
  typeAllocSizes = {0,0,0,...}

da.AddTypeAllocSize(1024, IntFamily)     → typeAllocSizes.ints = 1024
da.AddTypeAllocSize(1024, StringFamily)  → typeAllocSizes.strings = 1024
da.AddTypeAllocSize(1024, FloatFamily)   → typeAllocSizes.floats = 1024

─── 阶段2：ColVecToDatum 处理 id 列（INT） ───

da.NewDInt(row0.id):
  len(dintAlloc)==0 → make([]DInt, 1024)  [第1次 GC malloc]
  r = &dintAlloc[0], *r = row0.id
  dintAlloc = dintAlloc[1:]  (len=1023)

da.NewDInt(row1.id):
  len=1023 > 0 → 无 malloc
  r = &dintAlloc[0], *r = row1.id
  dintAlloc = dintAlloc[1:]  (len=1022)

... (id 列共 1024 次 NewDInt，只有第 1 次 GC 分配)

─── 阶段3：ColVecToDatum 处理 name 列（STRING） ───

da.NewDString(row0.name):
  len(stringAlloc)==0 → make([]string, 1024)  [第2次 GC malloc]
  r = (*DString)(&stringAlloc[0])
  *r = DString(row0.name)
  stringAlloc = stringAlloc[1:]

... (name 列 1024 次 NewDString，只有第 1 次 GC 分配)

─── 阶段4：ColVecToDatum 处理 score 列（FLOAT） ───

da.NewDFloat(row0.score):
  len(dfloatAlloc)==0 → make([]DFloat, 1024)  [第3次 GC malloc]
  ...

─── 统计 ───
总行数: 1024 × 3列 = 3072 个 Datum
GC 分配次数: 3次（每种类型 1 次 make）
vs. 无 DatumAlloc: 3072 次 new(T)

内存布局:
  dintAlloc 底层数组:  [id₀][id₁]...[id₁₀₂₃]   ← 连续 8192 字节
  stringAlloc:        [n₀][n₁]...[n₁₀₂₃]      ← 连续 16384 字节
  dfloatAlloc:        [s₀][s₁]...[s₁₀₂₃]      ← 连续 8192 字节
```

**第二个 batch 到来时**：

```
da.DefaultAllocSize 已被调用方更新（若 batchLen 没变）:
  若 batchLen 仍为 1024:
    ResetTypeAllocSizes() 后 AddTypeAllocSize(1024, ...)
    此时 typeAllocSizes.ints = 1024

  da.NewDInt(row0.id):
    dintAlloc 上一批用完后 len=0
    → make([]DInt, 1024)  [又一次 GC 分配]
```

注意：`DatumAlloc` **不跨批次复用**已分配的 buffer（那些已经以指针形式返回给外部，不能回收）。每批次独立分配。

### 4.2 边界场景：NewDatums 处理宽表（100 列）

```
查询: SELECT c1, c2, ..., c100 FROM wide_table LIMIT 1

da.NewDatums(100):
  len(datumAlloc)=0 < 100
  extensionSize = defaultDatumAllocSize = 16
  extTupleLen = 100 * 4 = 400
  extensionSize = max(16, 400) = 400    ← 乘数因子起效
  *buf = make(Datums, 400)             ← 1次 GC 分配
  r = (*buf)[:100]                     ← 取前 100 个
  *buf = (*buf)[100:]                  ← 剩余 300 个

da.NewDatums(100):  // 第 2 行
  len(datumAlloc)=300 >= 100
  → 无 GC 分配
  r = (*buf)[:100]
  *buf = (*buf)[100:]  ← 剩余 200 个

da.NewDatums(100):  // 第 3 行
  len=200 >= 100 → 无 GC 分配，剩余 100

da.NewDatums(100):  // 第 4 行
  len=100 >= 100 → 无 GC 分配，剩余 0

da.NewDatums(100):  // 第 5 行，耗尽
  len=0 < 100 → make(Datums, 400)  ← 第 2 次 GC 分配
```

结论：每 4 行触发 1 次 GC 分配，而不是每行 1 次。

---

## 五、与常见 Alloc 算法的深度对比

### 5.1 对比矩阵

| 特性 | DatumAlloc | `sync.Pool` | `runtime.mallocgc` (new) | Arena/TCMalloc | Bump Pointer |
|---|---|---|---|---|---|
| **归还内存** | ❌ 不支持 | ✅ GC 自动回收 | ✅ GC 回收 | ✅ 手动 Free | ❌ 只能整体释放 |
| **线程安全** | ❌ 单线程 | ✅ | ✅ | 取决于实现 | ❌ |
| **内存碎片** | 低（类型连续）| 中 | 高（分散） | 低 | 极低 |
| **GC 压力** | 低（批量分配）| 极低（Pool 复用）| 高 | 低 | 极低 |
| **类型专化** | ✅ 每类型独立 | ❌ 通用 | ❌ 通用 | ❌ 通用 | ❌ 通用 |
| **可变大小** | ❌ 固定类型 | ❌ | ✅ | ✅ | ❌ |
| **实现复杂度** | 低 | 极低 | 系统内置 | 高 | 极低 |
| **分配速度** | 极快（slice 操作）| 快（TLS cache）| 中 | 快 | 极快 |

### 5.2 与 `sync.Pool` 的本质区别

`sync.Pool` 的设计目标是**复用已释放的对象**，典型用法：

```go
// sync.Pool：对象复用
var pool = sync.Pool{New: func() any { return new(DInt) }}

// 获取
p := pool.Get().(*DInt)
*p = DInt(42)
// 使用...
pool.Put(p)  // 归还（必须手动！）
```

`DatumAlloc` 的设计目标是**批量分配新对象**，不复用：

```go
// DatumAlloc：批量分配
da := DatumAlloc{}
p1 := da.NewDInt(42)   // 从 slab 取第 1 个
p2 := da.NewDInt(43)   // 从 slab 取第 2 个
// p1, p2 返回给外部，DatumAlloc 不再管理它们
// 外部代码可以长时间持有 p1, p2
```

**核心区别**：

| 维度 | sync.Pool | DatumAlloc |
|---|---|---|
| 对象生命周期 | 短暂（用完归还）| 任意（取走不归还）|
| 使用场景 | 临时对象（缓冲区、请求对象）| 查询行级数据（需持久存活）|
| GC 行为 | Pool 中对象在 GC 时可能被清空 | 数组存活至持有方不再引用 |
| 并发安全 | ✅（内置 per-P 本地缓存）| ❌（单线程）|

**为什么 SQL 执行不能用 sync.Pool？**

Datum 指针返回给查询执行器后，可能被：
- 存入 `[]Datum` 行数据
- 传递给排序/聚合算子
- 最终写入网络缓冲区发送给客户端

它们的生命周期由查询树决定，**不能在"用完"后立即归还**。`sync.Pool` 需要明确的 `Put` 时机，而行级 Datum 没有明确的释放点（在 Go GC 中靠引用计数自动回收）。

### 5.3 与 Arena / Region-Based Allocation 的对比

**Arena 分配器**（如 Go 1.20 引入的 `arena.Arena`，或 C++ 的 `std::pmr::monotonic_buffer_resource`）：

```
Arena 内存布局:
  [block₁: 4MB] → [block₂: 4MB] → [block₃: 4MB]
       ↑ bump pointer

  alloc(size) → 从当前 block 尾部线性分配（极快，O(1)）
  free all    → 整个 Arena 一次性释放
```

**DatumAlloc 与 Arena 的异同**：

| 特性 | DatumAlloc | Arena |
|---|---|---|
| 分配策略 | 按类型独立 slab，每次 batch | 统一 bump pointer |
| 释放粒度 | 依靠 GC（逐 batch 自然死亡）| 整个 Arena 一次性释放 |
| 跨类型碎片 | 无（每类型独立连续）| 极少（线性分配） |
| 内存上限 | 无明确上限 | Arena 大小预定 |
| 类型安全 | ✅ Go 类型系统 | ❌ 需 unsafe 或手动管理 |

DatumAlloc 实际上是 **多个类型特化的微型 Arena**，而不是一个统一的 Arena。这样的设计好处是：
- 同类型的 Datum 物理连续，提升了同类型访问的缓存命中率。
- 不需要对齐填充（每种类型都是相同大小对象的数组，天然对齐）。

### 5.4 与 TCMalloc / jemalloc 的对比

TCMalloc（Google）和 jemalloc（Facebook）是系统级通用内存分配器，采用**尺寸分级（Size Class）**策略：

```
TCMalloc 尺寸分级（示意）：
  class 1:  8字节对象
  class 2:  16字节对象
  class 3:  32字节对象
  ...
  class N:  大对象直接从 pageheap 分配

每个 class 维护一个 ThreadCache（线程本地）
```

**DatumAlloc 本质上是 TCMalloc Thread Cache 的 Go 应用层实现，但更专化**：

| 维度 | TCMalloc ThreadCache | DatumAlloc |
|---|---|---|
| 尺寸分级 | 按字节大小分级 | 按 Go 类型分级 |
| 归还 | ✅ `free()` | ❌ GC |
| 跨线程 | ✅（有 CentralCache）| ❌（单线程） |
| 元数据开销 | 每个对象有 header | 零开销（slice index）|
| 实现层次 | OS 系统调用层 | Go 应用层 |

**DatumAlloc 比 TCMalloc 更适合此场景的原因**：

1. **类型信息已知**：SQL 层在编译期（或查询优化期）就知道每列的类型，可以直接用类型化 slice，避免 TCMalloc 运行时的尺寸查找。
2. **零归还负担**：DatumAlloc 不需要实现 `free()`，由 GC 统一处理，大幅简化实现。
3. **Go 运行时友好**：直接使用 `make([]T, n)` 触发 Go GC 批量分配，GC 能正确追踪所有引用，无需额外的内存安全机制。

### 5.5 与 Bump Pointer Allocator 的对比

Bump Pointer 是最简单的分配器：

```
memory: [XXXXXXXXXX             ]
         ↑ bump ptr → 每次分配向右移动

alloc(n): ptr = bump; bump += n; return ptr
free all: bump = start  // 整个 region 一次性释放
```

DatumAlloc 的每个类型 buffer（如 `dintAlloc`）在语义上**就是**一个 Bump Pointer 分配器：

```
dintAlloc: [DInt₀|DInt₁|...|DInt₁₅]
                              ↑
                          当前 "bump ptr"（slice header 的 ptr + len）
```

`*buf = (*buf)[1:]` 就是 bump 操作，时间复杂度 O(1)，零额外开销。

**DatumAlloc 与纯 Bump Pointer 的差距**：DatumAlloc 不需要手动"重置"整个 region，而是依靠 Go GC。当 batch 处理完成，持有 `DatumAlloc` 的执行算子失去引用，GC 自动回收所有相关内存（包括已从 buffer 切出去的那些对象）。

---

## 六、设计权衡总结

### 6.1 DatumAlloc 的设计决策树

```
为 SQL 执行层分配 Datum 指针

需求:
  ├── 对象类型已知？ YES → 类型专化 slab（而非通用 malloc）
  ├── 对象需要复用？ NO → 不用 sync.Pool
  ├── 需要跨 goroutine 共享？ NO → 不需要锁，性能更高
  ├── 对象生命周期由 GC 管理？ YES → 不用手动 free，简化实现
  ├── 批量大小可预测？ YES → typeAllocSizes hint 系统
  └── 有变长数据（地理空间）？ YES → 独立的 EWKB 分配器 + 归还机制
```

### 6.2 核心设计原则

| 原则 | 体现 |
|---|---|
| **类型专化**（Type Specialization）| 每种 Datum 类型独立 buffer，避免类型转换开销和碎片 |
| **摊销分配**（Amortized Allocation）| N 个对象只触发 O(N/batch) 次 GC malloc |
| **惰性分配**（Lazy Allocation）| buffer 耗尽时才 make，不预先分配所有类型 |
| **预测驱动**（Prediction-Driven）| typeAllocSizes hint 在批量转换前精确预告分配量 |
| **不变量保护**（Invariant Protection）| NoCopy 防止拷贝导致的 buffer 共享 bug |
| **渐进适应**（Progressive Adaptation）| EWKB 指数增长，自适应数据大小 |

---

*文档生成时间：2026-02-28*
*分析版本：CockroachDB main branch（datum_alloc.go）*
