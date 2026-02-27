# 第三十四章 Go Runtime Maps 深度剖析——从传统 Bucket 到 Swiss Table 的并行查找革命

## 源码位置
**传统 Map**: `go/src/runtime/map_noswiss.go` (Go 1.23 及之前)
**Swiss Map**: `go/src/runtime/map_swiss.go` + `go/src/internal/runtime/maps/` (Go 1.24+)
**构建标签**: `-tags=goexperiment.swissmap`（实验性功能）

---

## 一、BFS Why：为什么需要重新设计 Map？

### 1.1 传统 Go Map 的架构限制

**传统设计**（自 Go 1.0 起使用）：
```go
// 源码：map_noswiss.go:149-160
type bmap struct {
    tophash [8]uint8  // 8 个 key 的高 8 位哈希
    // 后跟 8 个 keys
    // 后跟 8 个 elems
    // 后跟 overflow 指针
}

type hmap struct {
    count      int           // 元素数量
    B          uint8         // log2(bucket 数量)
    buckets    unsafe.Pointer // 2^B 个 bucket
    oldbuckets unsafe.Pointer // 扩容时的旧 bucket
}
```

**查找流程**（传统）：
```
1. hash(key) → 64 位哈希
2. 低 B 位 → 选择 bucket
3. 高 8 位 → tophash，顺序比较 8 个槽
4. tophash 匹配 → 完整 key 比较
5. 未找到 → 检查 overflow chain
```

**性能瓶颈**：
| 问题 | 具体表现 | 影响 |
|------|---------|------|
| **顺序查找** | 8 个 tophash 逐一比较 | CPU 分支预测失败 |
| **缓存不友好** | key/elem 分离存储 | Cache Miss 增加 |
| **Overflow Chain** | 高负载时链表过长 | O(n) 退化 |
| **增量扩容复杂** | 渐进式 evacuation | 维护 oldbuckets 开销 |

### 1.2 Swiss Table 的设计动机

**Google Abseil 的 Swiss Table**（2017 年）：
- 使用 SIMD 指令并行比较 16 个槽（SSE2）
- 控制字节（control byte）与数据分离
- 开放寻址 + 二次探测（Quadratic Probing）

**Go 的 Swiss Map 改进**（2024 年）：
```
Abseil Swiss Table          Go Swiss Map
┌─────────────────┐         ┌─────────────────┐
│ 16 槽/组 (SSE2) │  →     │ 8 槽/组 (通用)   │
├─────────────────┤         ├─────────────────┤
│ 固定单表         │  →     │ 多表 + 目录索引  │
├─────────────────┤         ├─────────────────┤
│ C++ 专用        │  →     │ 跨平台纯 Go     │
└─────────────────┘         └─────────────────┘
```

**关键创新**：
1. **可扩展哈希（Extendible Hashing）**：支持增量表分裂
2. **Control Word 并行匹配**：8 字节控制字一次比较 8 个槽
3. **更好的迭代语义**：无需全局锁，支持并发修改下的正确性

---

## 二、BFS How：Swiss Map 的三层架构

### 2.1 整体架构图

```
                  [Go map[K]V]
                       ↓
            ┌──────────────────────┐
            │   Map (顶层结构)     │
            │  - used: 元素总数    │
            │  - seed: 哈希种子    │
            │  - globalDepth: 3    │  ← 目录深度
            └──────────┬───────────┘
                       │
         ┌─────────────┴─────────────┐
         │  Directory (目录数组)     │
         │  [2^globalDepth]*table   │
         │                           │
         │  [000] → table0          │
         │  [001] → table0  ────┐   │  ← 多个索引指向同一表
         │  [010] → table1      │   │
         │  [011] → table1  ────┤   │
         │  [100] → table2      │   │
         │  [101] → table2  ────┤   │
         │  [110] → table3      │   │
         │  [111] → table3  ────┘   │
         └──────────┬────────────────┘
                    ↓
     ┌──────────────────────────────┐
     │  Table (单个哈希表)          │
     │  - used: 表内元素数          │
     │  - capacity: 容量            │
     │  - localDepth: 2             │  ← 本表深度
     │  - groups: 组数组            │
     └──────────────┬───────────────┘
                    ↓
  ┌─────────────────────────────────┐
  │  Groups (组数组，2^N 个)        │
  │                                 │
  │  Group 0: [ctrl | 8 slots]    │
  │  Group 1: [ctrl | 8 slots]    │
  │  Group 2: [ctrl | 8 slots]    │
  │  ...                            │
  └─────────────────────────────────┘
             ↓
  ┌──────────────────────────────────┐
  │  Group (8 槽 + 控制字)           │
  │                                  │
  │  Control Word (8 字节):         │
  │  ┌───┬───┬───┬───┬───┬───┬───┬───┐
  │  │C0 │C1 │C2 │C3 │C4 │C5 │C6 │C7 │
  │  └───┴───┴───┴───┴───┴───┴───┴───┘
  │    ↓   ↓   ↓   ↓   ↓   ↓   ↓   ↓
  │  [K0|E0][K1|E1][K2|E2]...[K7|E7]│
  └──────────────────────────────────┘
```

### 2.2 控制字节（Control Byte）的编码

```go
// 源码：internal/runtime/maps/group.go:115-121
type ctrl uint8

// 三种状态的位模式：
const (
    ctrlEmpty   ctrl = 0b10000000  // 空槽
    ctrlDeleted ctrl = 0b11111110  // 已删除（墓碑）
    // full       = 0b0HHHHHHH     // 已占用 + H2 哈希
)
```

**编码设计**：
- **Bit 7 = 1**: empty 或 deleted（快速判断）
- **Bit 1-6**: 区分 empty vs deleted
- **Bit 0-6**: H2 哈希（已占用时）

**示例**：
```
槽状态       Ctrl Byte   二进制         说明
Empty        0x80        10000000       空槽
Deleted      0xFE        11111110       墓碑
Full(H2=42)  0x2A        00101010       已占用，H2=42
Full(H2=127) 0x7F        01111111       H2 最大值
```

### 2.3 哈希分割：H1 与 H2

```go
// 源码：internal/runtime/maps/map.go:182-192
func h1(hash uintptr) uintptr {
    return hash >> 7  // 高 57 位 (64位系统)
}

func h2(hash uintptr) uintptr {
    return hash & 0x7f  // 低 7 位
}
```

**用途分工**：
```
64 位哈希值：
┌────────────── 57 位 ──────────────┬─── 7 位 ──┐
│            H1                     │    H2     │
└───────────────────────────────────┴───────────┘
        ↓                                 ↓
   目录索引 + 组选择              控制字节匹配

目录索引：H1 的高 globalDepth 位
组选择：  H1 的剩余位 % groupCount
H2 匹配：  控制字节中的 7 位
```

**为什么是 7 位？**
- 8 槽 × 7 位 = 56 位，正好填满 64 位哈希的大部分
- 假阳性率 = 1/128 ≈ 0.78%（可接受）
- 方便位操作（与 0x7F 掩码）

---

## 三、DFS How：核心操作的深入实现

### 3.1 并行匹配：ctrlGroup.matchH2

```go
// 源码：internal/runtime/maps/group.go:161-172
func ctrlGroupMatchH2(g ctrlGroup, h uintptr) bitset {
    // g = 8 字节控制字，h = 目标 H2
    v := uint64(g) ^ (bitsetLSB * uint64(h))
    // bitsetLSB = 0x0101010101010101
    // 异或后，匹配的字节变为 0x00

    // 魔法公式：检测 v 中哪些字节为 0
    return bitset(((v - bitsetLSB) &^ v) & bitsetMSB)
    // bitsetMSB = 0x8080808080808080
}
```

**算法原理**（Hacker's Delight 技巧）：
```
示例：查找 H2 = 0x2A 的槽

Control Word：
  Byte:  [0x80] [0x2A] [0xFE] [0x2A] [0x00] [0x50] [0x2A] [0x80]
  Index:   0      1      2      3      4      5      6      7

步骤 1：异或广播
  v = g ^ 0x2A2A2A2A2A2A2A2A
    = [0xAA] [0x00] [0xD4] [0x00] [0x2A] [0x7A] [0x00] [0xAA]
       ↑      ↑      ↑      ↑      ↑      ↑      ↑      ↑
     非零    零    非零    零    非零   非零    零    非零

步骤 2：检测零字节
  v - 0x0101010101010101 = [0xA9][0xFF][0xD3][0xFF][0x29][0x79][0xFF][0xA9]
  ~v                     = [0x55][0xFF][0x2B][0xFF][0xD5][0x85][0xFF][0x55]
  相与                   = [0x01][0xFF][0x03][0xFF][0x01][0x01][0xFF][0x01]

步骤 3：提取 MSB
  & 0x8080808080808080    = [0x00][0x80][0x00][0x80][0x00][0x00][0x80][0x00]
                             索引 1 ✓   索引 3 ✓           索引 6 ✓

结果 bitset：0x0000808000800080
```

**AMD64 优化**：
```go
// 在 AMD64 上，matchH2 被替换为 SIMD intrinsic：
// PCMPEQB xmm0, xmm1  // 并行比较 16 字节
// PMOVMSKB eax, xmm0  // 提取匹配位到整数
```

### 3.2 查找操作：Table.getWithKey

```go
// 源码：internal/runtime/maps/table.go:166-224
func (t *table) getWithKey(typ *abi.SwissMapType, hash uintptr, key unsafe.Pointer) (unsafe.Pointer, unsafe.Pointer, bool) {
    // 二次探测序列
    seq := makeProbeSeq(h1(hash), t.groups.lengthMask)

    for ; ; seq = seq.next() {
        g := t.groups.group(typ, seq.offset)

        // 并行匹配 H2
        match := g.ctrls().matchH2(h2(hash))

        // 遍历所有匹配的槽
        for match != 0 {
            i := match.first()

            slotKey := g.key(typ, i)
            if typ.IndirectKey() {
                slotKey = *((*unsafe.Pointer)(slotKey))
            }

            // 完整 key 比较
            if typ.Key.Equal(key, slotKey) {
                slotElem := g.elem(typ, i)
                if typ.IndirectElem() {
                    slotElem = *((*unsafe.Pointer)(slotElem))
                }
                return slotKey, slotElem, true
            }

            match = match.removeFirst()
        }

        // 检查空槽（探测终止条件）
        match = g.ctrls().matchEmpty()
        if match != 0 {
            return nil, nil, false
        }
    }
}
```

**探测序列**（Quadratic Probing）：
```go
// 源码：internal/runtime/maps/table.go（未展示）
type probeSeq struct {
    offset     uint64
    multiplier uint64
}

func (s probeSeq) next() probeSeq {
    // 三角数序列：0, 1, 3, 6, 10, 15, 21, ...
    s.offset += s.multiplier
    s.multiplier++
    s.offset &= lengthMask  // 模运算
    return s
}
```

**为什么用二次探测？**
- **线性探测**：聚集问题（Clustering）
- **二次探测**：分散冲突，保证遍历所有组（当组数为 2^N 时）

### 3.3 插入操作：Map.putSlot

```go
// 源码：internal/runtime/maps/map.go:516-530
func (m *Map) putSlot(typ *abi.SwissMapType, hash uintptr, key unsafe.Pointer) unsafe.Pointer {
    for {
        idx := m.directoryIndex(hash)  // 哈希高位 → 目录索引
        elem, ok := m.directoryAt(idx).PutSlot(typ, m, hash, key)
        if !ok {
            // 表分裂了，重试
            continue
        }
        return elem
    }
}

// Table 层的插入
func (t *table) PutSlot(typ *abi.SwissMapType, m *Map, hash uintptr, key unsafe.Pointer) (unsafe.Pointer, bool) {
    seq := makeProbeSeq(h1(hash), t.groups.lengthMask)

    var firstDeletedGroup groupReference
    var firstDeletedSlot uintptr

    for ; ; seq = seq.next() {
        g := t.groups.group(typ, seq.offset)
        match := g.ctrls().matchH2(h2(hash))

        // 1. 查找已存在的 key
        for match != 0 {
            i := match.first()
            slotKey := g.key(typ, i)
            if typ.IndirectKey() {
                slotKey = *((*unsafe.Pointer)(slotKey))
            }
            if typ.Key.Equal(key, slotKey) {
                // 更新场景
                if typ.NeedKeyUpdate() {
                    typedmemmove(typ.Key, slotKey, key)
                }
                return g.elem(typ, i), true
            }
            match = match.removeFirst()
        }

        // 2. 记录第一个已删除槽（墓碑复用）
        if firstDeletedGroup.data == nil {
            match = g.ctrls().matchDeleted()
            if match != 0 {
                firstDeletedGroup = g
                firstDeletedSlot = match.first()
            }
        }

        // 3. 检查空槽（插入终点）
        match = g.ctrls().matchEmpty()
        if match != 0 {
            // 优先使用墓碑
            var insertGroup groupReference
            var insertSlot uintptr
            if firstDeletedGroup.data != nil {
                insertGroup = firstDeletedGroup
                insertSlot = firstDeletedSlot
            } else {
                insertGroup = g
                insertSlot = match.first()
            }

            // 分配并插入
            slotKey := insertGroup.key(typ, insertSlot)
            if typ.IndirectKey() {
                kmem := newobject(typ.Key)
                *(*unsafe.Pointer)(slotKey) = kmem
                slotKey = kmem
            }
            typedmemmove(typ.Key, slotKey, key)

            slotElem := insertGroup.elem(typ, insertSlot)
            if typ.IndirectElem() {
                emem := newobject(typ.Elem)
                *(*unsafe.Pointer)(slotElem) = emem
                slotElem = emem
            }

            // 更新控制字节
            insertGroup.ctrls().set(insertSlot, ctrl(h2(hash)))
            t.used++
            t.growthLeft--

            // 检查是否需要扩容/分裂
            if t.growthLeft == 0 {
                t.rehash(typ, m)
                return nil, false  // 需要重试
            }

            return slotElem, true
        }
    }
}
```

**墓碑复用策略**：
```
探测序列：Group0 → Group1 → Group2 → ...

Group0: [Full] [Full] [Deleted] [Empty] ...
                        ↑
                   记录墓碑位置

Group1: [Full] [Full] [Full] [Full] ...
        继续探测

Group2: [Empty] ...
        ↑
    找到空槽，回到 Group0 的墓碑插入
```

### 3.4 可扩展哈希：表分裂机制

```go
// 源码：internal/runtime/maps/map.go（未完整展示）
func (t *table) rehash(typ *abi.SwissMapType, m *Map) {
    if t.capacity < maxTableCapacity {
        // 单表扩容（容量翻倍）
        t.growInPlace(typ, m)
    } else {
        // 表分裂（Split）
        t.split(typ, m)
    }
}

func (t *table) split(typ *abi.SwissMapType, m *Map) {
    // 创建两个新表
    newLocalDepth := t.localDepth + 1
    t0 := newTable(typ, maxTableCapacity, t.index, newLocalDepth)
    t1 := newTable(typ, maxTableCapacity, t.index+(1<<t.localDepth), newLocalDepth)

    // 重新分配旧表的元素
    for each entry in t {
        hash := typ.Hasher(key, m.seed)
        bit := (h1(hash) >> (64 - newLocalDepth)) & 1
        if bit == 0 {
            t0.insert(key, elem)
        } else {
            t1.insert(key, elem)
        }
    }

    // 更新目录
    if m.globalDepth == t.localDepth {
        // 需要扩展目录
        m.growDirectory()
    }
    m.directory[t.index] = t0
    m.directory[t.index + (1 << t.localDepth)] = t1

    // 标记旧表为失效
    t.index = -1
}
```

**分裂示例**：
```
初始状态（globalDepth=2, table0.localDepth=2）：
Directory:
  [00] → table0 (1024 slots, 90% full)
  [01] → table1
  [10] → table2
  [11] → table3

table0 触发分裂 → 创建 table0a 和 table0b（localDepth=3）

新状态（globalDepth=3）：
Directory:
  [000] → table0a  ← 重哈希，H1 第 3 位 = 0
  [001] → table1   ← 原有指针复制
  [010] → table2   ← 原有指针复制
  [011] → table3   ← 原有指针复制
  [100] → table0b  ← 重哈希，H1 第 3 位 = 1
  [101] → table1   ← 原有指针复制
  [110] → table2   ← 原有指针复制
  [111] → table3   ← 原有指针复制
```

---

## 四、运行时行为

### 4.1 小 Map 优化（Small Map）

```go
// 源码：internal/runtime/maps/map.go:267-282
func NewMap(mt *abi.SwissMapType, hint uintptr, m *Map, maxAlloc uintptr) *Map {
    if hint <= abi.SwissMapGroupSlots { // 8
        // 小 Map：直接使用单个 group，无目录
        return m
    }
    // 大 Map：分配目录和表
    // ...
}
```

**小 Map 内存布局**：
```
Map 结构：
┌──────────────────────┐
│ used: 3              │
│ seed: 0x1234...      │
│ dirPtr → group       │  ← 直接指向 group
│ dirLen: 0            │  ← 标记为小 Map
└──────────────────────┘
            ↓
      ┌────────────────────────┐
      │ Group (8 槽)           │
      │ Ctrl: [Full][Full][Full][Empty]... │
      │ [K0|E0][K1|E1][K2|E2][  ][  ]...   │
      └────────────────────────┘
```

**优势**：
- 无目录开销（节省 1 次指针间接访问）
- 无墓碑（删除直接标记为 Empty）
- 可在栈上分配

### 4.2 负载因子与扩容

```go
// 源码：internal/runtime/maps/group.go:14-20
const maxAvgGroupLoad = 7  // 7/8 = 87.5%
```

**扩容触发条件**：
```
used + tombstones > (capacity * 7) / 8

示例：
- capacity = 64 槽
- maxLoad = 56 槽
- 当 used=50, tombstones=7 时触发扩容
```

**对比传统 Map**：
| Map 类型 | 负载因子 | 扩容阈值 | Overflow 概率 |
|----------|---------|---------|---------------|
| 传统 Map | 6.5/8 (81%) | used > 52 | ~20% |
| Swiss Map | 7/8 (87.5%) | used+tomb > 56 | <5% |

### 4.3 完整操作序列

**场景：插入 1000 个元素**

```
T0: NewMap(hint=1000)
    → targetCapacity = 1000 * 8 / 7 ≈ 1143
    → dirSize = ⌈1143/1024⌉ = 2
    → 创建 2 个表，每表 1024 槽

T1: 插入第 1-896 个元素
    → table0: used=448, table1: used=448
    → 无扩容

T2: 插入第 897 个元素
    → table0.growthLeft = 0 → 扩容
    → capacity: 1024 → 2048
    → 重哈希 448 个元素

T3: 插入第 897-2048 个元素
    → 稳态运行

T4: 删除 100 个元素
    → tombstones += 100
    → used -= 100

T5: 继续插入
    → 墓碑复用
    → growthLeft 考虑 tombstones
```

---

## 五、设计模式识别

### 5.1 SIMD-Friendly 数据布局

```
传统设计（AoS - Array of Structs）：
[Slot0: K|E][Slot1: K|E][Slot2: K|E]...
↑ 遍历时 Cache Miss 多

Swiss Table（SoA 变体）：
[Ctrl0][Ctrl1]...[Ctrl7] | [K0|E0][K1|E1]...
↑ 控制字连续 → SIMD 友好
```

### 5.2 分治策略（Extendible Hashing）

```
单一巨型表                     多表 + 目录
┌──────────────┐              ┌────┐
│ 100万槽      │              │目录 │
│              │     →        ├────┤
│ 扩容需拷贝   │              │T0  │ 1024槽
│ 全部元素     │              │T1  │ 1024槽
└──────────────┘              │... │ ...
  ↓ 扩容延迟高                └────┘
                                ↓ 单表扩容
```

### 5.3 Tombstone 复用（Lazy Deletion）

```
立即清理：
Delete(K2) → 移动后续元素 → 破坏探测序列

Tombstone：
Delete(K2) → 标记 Deleted → 保持探测完整性
Insert(K5) → 优先填充 Deleted 槽
```

### 5.4 Two-Level Metadata（控制字 + H2）

```
单级：直接比较 key
  → 每次比较都需要加载完整 key（昂贵）

两级：Ctrl Byte → Key
  1. 匹配 H2 (7 位) → 筛选候选
  2. 完整 key 比较 → 最终验证
  → 减少 91.4% (127/128) 的 key 比较
```

---

## 六、具体示例

### 6.1 手动模拟查找过程

```go
// 场景：map[int]string，查找 key=12345
hash := 0xABCD1234567890EF
H1 := 0xABCD1234567890EF >> 7 = 0x00015799A26A8CF1
H2 := 0xEF & 0x7F = 0x6F

// 假设 globalDepth=2，4 个表
dirIndex := (H1 >> 62) & 0b11 = 0b10 = 2
table := directory[2]

// table 有 8 个 group
groupIndex := H1 % 8 = 1
group := table.groups[1]

// 并行匹配
ctrlWord := group.ctrls() = 0x806F506F80FEFE6F
//           Slot:   0   1   2   3   4   5   6   7
//           Ctrl: 0x80 0x6F 0x50 0x6F 0x80 0xFE 0xFE 0x6F

matchH2(0x6F):
  v = 0x806F506F80FEFE6F ^ 0x6F6F6F6F6F6F6F6F
    = 0xEF00300080919100
  bitset = detect_zero_bytes(v) = 0x0080008000000080
  → 匹配槽：1, 3, 7

// 遍历匹配
for i in [1, 3, 7]:
    if group.key(i) == 12345:
        return group.elem(i)  // 找到

// 未找到 → 检查空槽
if group.ctrls().matchEmpty() != 0:
    return nil, false  // 确认不存在

// 继续探测下一组
probeSeq.next() → group2 → ...
```

### 6.2 CockroachDB 中的 Map 使用

```go
// CockroachDB 典型场景：Raft Progress 追踪
type Status struct {
    Progress map[raftpb.PeerID]tracker.Progress
}

// 初始化
status := &Status{
    Progress: make(map[raftpb.PeerID]tracker.Progress),
}

// 频繁访问
for peerID, progress := range status.Progress {
    if progress.Match < commitIndex {
        // 需要发送 catch-up 消息
    }
}
```

**性能影响**：
- 传统 Map：每次访问 ~5-10 ns（顺序扫描 tophash）
- Swiss Map：每次访问 ~3-5 ns（SIMD 并行匹配）
- 热点场景（Leader 检查 100 个 replica）：节省 ~200-500 ns/次

### 6.3 迭代语义的挑战

```go
// Go 规范要求：迭代中可修改 map
m := map[int]int{1: 10, 2: 20, 3: 30}

for k, v := range m {
    if k == 2 {
        delete(m, 1)  // 删除已遍历的
        m[4] = 40     // 添加新元素
    }
    fmt.Println(k, v)
}
// 输出可能包含 (4, 40)，但绝不重复输出 (1, 10)
```

**Swiss Map 的解决方案**：
```go
// 源码：internal/runtime/maps/iter.go
type Iter struct {
    // 保留对旧表的引用（即使表已分裂）
    tab *table

    // 记录已遍历的槽
    groupIdx uint64
    slotIdx  uintptr

    // 检测 map 清空
    clearSeq uint64
}

func (it *Iter) Next() {
    // 1. 从旧表读取 key
    key := it.tab.groups.group(typ, it.groupIdx).key(it.slotIdx)

    // 2. 在新表中重新查找（获取最新值）
    _, elem, ok := it.m.currentTable().getWithKey(key)

    // 3. 返回最新值或跳过已删除
    if !ok {
        continue  // key 被删除
    }
    return key, elem
}
```

---

## 七、工程权衡

### 7.1 Swiss Map vs 传统 Map

| 维度 | 传统 Map | Swiss Map | 优势方 |
|------|---------|-----------|--------|
| **查找性能** | 5-10 ns | 3-5 ns | Swiss (-40%) |
| **插入性能** | 8-15 ns | 5-10 ns | Swiss (-37%) |
| **删除性能** | 相近 | 墓碑开销 | 传统 (略) |
| **内存占用** | 更优（无控制字） | +12.5% | 传统 |
| **扩容延迟** | 全表拷贝 | 单表分裂 | Swiss |
| **代码复杂度** | 简单 | 复杂 | 传统 |
| **SIMD 优化** | 不支持 | 支持 | Swiss |

### 7.2 内存开销分析

**传统 Map（8 槽 bucket）**：
```
Bucket: [8 tophash] + [8 keys] + [8 elems] + [overflow ptr]
      = 8 + 8*sizeof(K) + 8*sizeof(E) + 8

以 map[int64]int64 为例：
      = 8 + 64 + 64 + 8 = 144 字节/bucket
      → 18 字节/slot（12.5% 开销）
```

**Swiss Map（8 槽 group）**：
```
Group: [8 ctrl] + [8 keys] + [8 elems]
     = 8 + 8*sizeof(K) + 8*sizeof(E)

以 map[int64]int64 为例：
     = 8 + 64 + 64 = 136 字节/group
     → 17 字节/slot（6.25% 开销）

但需要目录：
Directory: 2^globalDepth * 8 字节
Table overhead: ~32 字节/table

总开销：约 15-20%（包含元数据）
```

### 7.3 为什么 Go 1.24 仍未默认启用？

**实验性标签的原因**：
1. **迭代语义复杂**：需要大量测试验证正确性
2. **二进制兼容性**：`unsafe.Sizeof(map)` 变化影响反射
3. **性能回归风险**：小 map 场景可能变慢
4. **代码成熟度**：需要更多实战验证

**启用方式**（Go 1.25.4）：
```bash
go build -tags=goexperiment.swissmap
# 或
GOEXPERIMENT=swissmap go build
```

---

## 八、心智模型

### 8.1 Swiss Table 的三层过滤器

```
            [哈希值]
                ↓
    ┌───────────────────────┐
    │ L1: 目录索引          │ ← 高 3 位选表
    │ 命中率: 100%          │
    │ 延迟: <1ns (数组访问) │
    └───────────────────────┘
                ↓
    ┌───────────────────────┐
    │ L2: 控制字并行匹配    │ ← H2 匹配
    │ 命中率: ~1% (假阳性)  │
    │ 延迟: ~1ns (SIMD)     │
    └───────────────────────┘
                ↓
    ┌───────────────────────┐
    │ L3: 完整 Key 比较     │ ← 最终验证
    │ 命中率: 100% (正确)   │
    │ 延迟: 可变 (依赖类型) │
    └───────────────────────┘

类比：安检流程
L1 = 看票面 → 分流到候车厅
L2 = X 光机 → 筛选可疑物品
L3 = 人工检查 → 确认违禁品
```

### 8.2 可扩展哈希的电话簿类比

```
传统 Map = 单一大电话簿
- 增加条目 → 重印整本
- 查找 → 翻整本书

Swiss Map = 分册电话簿 + 索引
- 索引：A-D, E-H, I-L, M-P, ...
- 某册满了 → 只拆分该册（如 A-D → A-B, C-D）
- 查找 → 查索引 → 定位分册 → 翻页
```

### 8.3 探测序列的跳房子游戏

```
组数组：[G0][G1][G2][G3][G4][G5][G6][G7]

线性探测（传统）：
起点 G2 → G3 → G4 → G5 → ... (连续跳)
问题：聚集区域拥堵

二次探测（Swiss）：
起点 G2 → G3(+1) → G5(+2) → G0(+3) → G4(+4) → ...
优势：分散冲突
```

---

## 九、高级主题

### 9.1 SIMD Intrinsics（AMD64）

**Go 1.25 的编译器优化**：
```go
// 源码：cmd/compile/internal/ssa/rewriteAMD64.go
// matchH2 在 AMD64 上被替换为：
TEXT runtime.ctrlGroupMatchH2(SB)
    MOVQ    g+0(FP), X0       // 加载控制字到 XMM0
    PXOR    X1, X1            // 清零 XMM1
    MOVB    h+8(FP), AX       // 加载目标 H2
    MOVQ    AX, X1
    PSHUFB  X2, X1            // 广播 H2 到所有字节
    PCMPEQB X0, X1            // 并行比较（8x8=64 位）
    PMOVMSKB X1, AX           // 提取匹配掩码
    MOVQ    AX, ret+16(FP)
    RET
```

**性能提升**：
- 纯 Go 实现：~8 条指令，~3ns
- SIMD intrinsic：~5 条指令，~1ns

### 9.2 删除的两种模式

**完全删除**（matchEmpty 后）：
```
Group: [Full] [Full] [Empty] ...
                      ↑
Delete(slot1) → [Full] [Empty] [Empty]
                        ↑ 直接标记 Empty
```

**墓碑删除**（无 Empty）：
```
Group: [Full] [Full] [Full] [Full] [Full] [Full] [Full] [Full]
Delete(slot1) → [Full] [Deleted] [Full] ...
                        ↑ 必须用墓碑，否则破坏探测链
```

### 9.3 并发安全机制

```go
// 源码：internal/runtime/maps/map.go:237
type Map struct {
    writing uint8  // 写入标志（XOR 切换）
}

func (m *Map) Put(...) {
    if m.writing != 0 {
        fatal("concurrent map writes")
    }
    m.writing ^= 1  // 切换为 1

    // ... 写入操作 ...

    if m.writing == 0 {
        fatal("concurrent map writes")  // 检测并发
    }
    m.writing ^= 1  // 切换回 0
}
```

**为什么用 XOR？**
```
正常流程：0 → 1 → 0
并发写入：0 → 1 ─┬→ 1 (另一个 goroutine)
              └→ 0 (当前 goroutine)
              → 检测到异常值
```

---

## 十、总结

### Swiss Map 的核心创新

1. **并行查找**：
   - 控制字 + H2 → 8 槽一次性比较
   - SIMD 加速 → 减少 40% 查找延迟

2. **可扩展哈希**：
   - 多表 + 目录 → 增量扩容
   - 单表分裂 → 避免全局停顿

3. **墓碑管理**：
   - 延迟清理 → 保持探测完整性
   - 优先复用 → 减少内存碎片

4. **迭代语义**：
   - 旧表保留 → 保证不重复
   - 重新查找 → 反映最新修改

### 适用场景

**✅ Swiss Map 更优**：
- 大型 map（>100 元素）
- 查找密集型（读多写少）
- 需要低尾延迟
- 支持 SIMD 的平台（AMD64）

**❌ 传统 Map 更优**：
- 微型 map（<10 元素）
- 删除密集型（墓碑开销）
- 内存敏感场景
- 追求代码简洁

### 未来展望

**Go 1.26+ 可能方向**：
1. **默认启用**：成熟后替换传统实现
2. **16 槽组**：利用 AVX2 扩展到 16 字节控制字
3. **自适应策略**：小 map 用传统，大 map 用 Swiss
4. **泛型优化**：为特定类型生成专用代码

Swiss Map 代表了 Go runtime 朝着现代哈希表设计的演进，平衡了**性能、可维护性和 Go 独特的语义需求**，是编译器优化、数据结构和系统编程的精彩结合。
