# 第三十七章 RaftEntryCache深度剖析——基于分区LRU的高并发Raft日志缓存系统（上篇）

## 一、BFS Why：设计动机与问题域分析

### 1.1 Raft日志读取的性能挑战

在分布式共识系统中，Raft日志的读取是一个频繁发生的操作。传统的直接从磁盘读取模式存在严重的性能问题：

**典型场景分析：**
```
Leader向Follower发送日志复制请求：
1. Leader需要读取entries[100-200]
2. 从Pebble读取100个entries
3. 每个entry反序列化：Protobuf解码
4. 组装MsgApp消息发送

问题：
- 磁盘IO延迟：1-5ms per scan
- 反序列化开销：每个entry ~10μs
- 重复读取：相同entries被多次读取
  - 发送给多个Follower
  - 重试机制
  - Snapshot生成
```

**性能数据（典型48核机器）：**
```
场景：1000个Range，每个Range 10个副本

无缓存：
- 每秒Raft消息：100,000条
- 每条消息平均50个entries
- 总entry读取：5,000,000次/秒
- 磁盘IO：5,000,000 × 1ms = 5,000秒（不可能！）
- 实际：大量重复读取，磁盘IO成为瓶颈

有缓存（90%命中率）：
- 缓存命中：4,500,000次（内存读取，10ns）
- 磁盘读取：500,000次
- 总延迟：500,000 × 1ms = 500ms
- 性能提升：10倍+
```

### 1.2 Sideloaded Entry的特殊问题

CockroachDB支持Sideloaded Entry（大型SSTable数据存储在单独文件）：

**Sideloaded Entry结构：**
```protobuf
message Entry {
    uint64 index = 1;
    uint64 term = 2;
    bytes data = 3;  // 可能是"thin"（只有指针）或"fat"（包含实际数据）
}

// Thin entry (磁盘存储)
Entry {
    index: 1000,
    term: 5,
    data: [prefix + RaftCommand {
        ReplicatedEvalResult {
            AddSSTable { Data: nil }  // ← 空！数据在sideload文件
        }
    }]
}

// Fat entry (缓存中)
Entry {
    index: 1000,
    term: 5,
    data: [prefix + RaftCommand {
        ReplicatedEvalResult {
            AddSSTable { Data: [5MB SSTable] }  // ← 已加载！
        }
    }]
}
```

**问题：**
```
场景1：读取sideloaded entry
1. 从Pebble读取thin entry
2. 检测到是sideloaded
3. 从磁盘读取sideload文件 (5MB)
4. 反序列化并inline到entry
5. 返回给调用方

延迟：1ms (Pebble) + 10ms (5MB读取) + 5ms (反序列化) = 16ms

场景2：再次读取相同entry（无缓存）
- 又是16ms！完全重复的工作

场景3：使用缓存
- 第一次：16ms，存入缓存（fat entry）
- 后续读取：从缓存取，<1μs
- 提升：16,000倍！
```

### 1.3 并发访问的竞争问题

**全局锁方案（简单但低效）：**
```go
// ❌ 简单但性能差的设计
type SimpleCache struct {
    mu      sync.RWMutex
    entries map[RangeID]map[Index]Entry
    maxSize int
}

func (c *SimpleCache) Get(rangeID, index) Entry {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.entries[rangeID][index]
}

问题：
1. Range1的读取阻塞Range2的读取
2. 所有操作串行化
3. 在1000个Range的系统中，锁竞争严重
```

**性能测试数据：**
```
全局RWMutex设计：
- 8核并发Get操作
- 测试1000个不同Range
- 结果：吞吐量 ~500k ops/sec
- CPU使用率：80%（大部分时间在等锁）

分区设计：
- 8核并发Get操作
- 测试1000个不同Range
- 结果：吞吐量 ~8M ops/sec （16倍提升！）
- CPU使用率：60%
```

### 1.4 内存管理的权衡

**问题：如何限制缓存大小？**
```
挑战1：精确计算大小很困难
- Entry.Size()可能不准确
- 序列化vs反序列化的大小差异
- 元数据开销（map、指针等）

挑战2：全局vs局部内存策略
方案A（全局限制）：
- 总大小不超过128MB
- 某些热点Range可能占满缓存
- 冷门Range被挤出

方案B（per-Range限制）：
- 每个Range限制128KB
- 1000个Range = 128MB总大小
- 但热点Range无法利用空闲内存

选择：全局限制 + LRU = 灵活且高效
```

### 1.5 Eviction的公平性问题

**场景：不同Range的访问模式差异巨大**
```
Range类型分布：
- 热点Range（10%）：每秒1000次访问
- 活跃Range（20%）：每秒100次访问
- 普通Range（50%）：每秒10次访问
- 冷门Range（20%）：每秒1次访问

简单LRU的问题：
- 热点Range占据大部分缓存
- 冷门Range频繁被evict
- 即使冷门Range的数据很小

需求：
- 基于Range的粒度管理（不是单个entry）
- LRU基于Range访问时间
- 一次evict整个Range的数据
```

### 1.6 设计目标总结

基于以上分析，RaftEntry Cache的设计目标：

1. **高命中率**：90%+的缓存命中率
2. **低竞争**：不同Range的操作无锁竞争
3. **全局内存控制**：精确控制总内存使用
4. **Sideloaded优化**：缓存已inline的fat entry
5. **公平性**：基于访问模式的智能eviction
6. **并发安全**：支持多goroutine并发访问
7. **简单性**：代码可维护，逻辑清晰

---

## 二、BFS How：整体架构设计

### 2.1 核心架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                        Store (kvserver)                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  raftEntryCache *Cache  (全局单例，每个Store一个)       │  │
│  │  容量：cfg.RaftEntryCacheSize (默认 128MB)              │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ↓
        ┌──────────────────────────────────────┐
        │         Cache (raftentry)            │
        ├──────────────────────────────────────┤
        │ maxBytes: int32      // 128MB        │
        │ bytes: atomic int32  // 当前使用     │
        │ entries: atomic int32 // entry计数   │
        │                                      │
        │ mu: Mutex            // 保护parts和lru│
        │ parts: map[RangeID]*partition        │
        │ lru: partitionList   // 双向链表     │
        │                                      │
        │ metrics: Metrics     // 监控指标     │
        └──────────────────────────────────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
        ↓                                     ↓
┌───────────────────┐              ┌───────────────────┐
│ partition (Range1)│              │ partition (Range2)│
├───────────────────┤              ├───────────────────┤
│ id: RangeID       │              │ id: RangeID       │
│                   │              │                   │
│ mu: RWMutex       │              │ mu: RWMutex       │
│ ringBuf           │              │ ringBuf           │
│  ├─ buf: []Entry  │              │  ├─ buf: []Entry  │
│  ├─ head: int     │              │  ├─ head: int     │
│  └─ len: int      │              │  └─ len: int      │
│                   │              │                   │
│ size: cacheSize   │              │ size: cacheSize   │
│  (atomic uint64)  │              │  (atomic uint64)  │
│                   │              │                   │
│ next, prev: *partition // LRU链 │              │ next, prev: *partition │
└───────────────────┘              └───────────────────┘
        │                                     │
        └────────LRU顺序─────────────────────┘
        (最近使用)  ←────→  (最久未使用)
```

### 2.2 两级锁机制

**设计精髓：分离关注点**

```
Level 1: Cache.mu (粗粒度)
职责：
  - 定位partition（map lookup）
  - 更新LRU状态（moveToFront）
  - 创建新partition
  - Eviction决策
持有时间：短暂（微秒级）
作用域：整个Cache

Level 2: partition.mu (细粒度)
职责：
  - 读写ringBuf数据
  - add/get/scan entries
  - truncateFrom/clearTo
持有时间：较长（毫秒级，取决于操作）
作用域：单个partition

优势：
✓ 不同Range的操作完全并行
✓ 即使LRU更新也不阻塞数据访问
✓ Eviction不阻塞其他Range
```

**操作流程示例：**
```go
// Get操作的锁序列
func (c *Cache) Get(rangeID, index) (Entry, bool) {
    // 阶段1：快速定位partition
    c.mu.Lock()
    p := c.parts[rangeID]
    if p != nil {
        c.lru.moveToFront(p)  // 更新LRU
    }
    c.mu.Unlock()
    // ← Cache.mu已释放！其他Range可以并发操作

    if p == nil {
        return Entry{}, false
    }

    // 阶段2：访问partition数据
    p.mu.RLock()  // ← 只锁这个Range
    entry, ok := p.ringBuf.get(index)
    p.mu.RUnlock()

    return entry, ok
}

// 并发场景演示
时间轴：
T0: Goroutine1 Get(Range1, 100)
    ├─ T1: Cache.mu.Lock()
    ├─ T2: 定位partition1
    ├─ T3: moveToFront(partition1)
    ├─ T4: Cache.mu.Unlock()  ← 释放全局锁
    ├─ T5: partition1.mu.RLock()
    └─ T6: 读取entry...  (可能很慢)

T2: Goroutine2 Get(Range2, 200)  // 在T4之后开始
    ├─ T4: Cache.mu.Lock()  ← 无需等待！T4已释放
    ├─ T5: 定位partition2
    ├─ T6: moveToFront(partition2)
    ├─ T7: Cache.mu.Unlock()
    ├─ T8: partition2.mu.RLock()
    └─ T9: 读取entry...  // 与Range1完全并行！
```

### 2.3 Partition设计

**为什么需要partition？**
```
选项A：Cache直接存储所有entries
type Cache struct {
    mu      sync.RWMutex
    entries map[RangeID]map[Index]Entry  // 两层map
}

问题：
- 所有Range共享一个锁
- Eviction需要遍历所有entries
- 无法实现per-Range的智能管理

选项B：每个Range一个partition（当前设计）
type Cache struct {
    mu    sync.Mutex
    parts map[RangeID]*partition  // 一层map
}

type partition struct {
    mu      sync.RWMutex
    ringBuf ringBuf  // 专属存储
    size    cacheSize
}

优势：
✓ Range级别的并发
✓ Eviction以partition为单位
✓ 每个partition可以独立优化
✓ 内存局部性更好
```

**Partition的生命周期：**
```
[创建]
触发：首次Add/Get该Range
条件：需要缓存数据
操作：
  - 分配partition对象
  - 插入parts map
  - 加入LRU链表头部
  - 计入bytes统计

[使用]
- Get: 读取数据，更新LRU
- Add: 添加数据，更新LRU
- Scan: 批量读取
- Clear: 清理旧数据

[Eviction]
触发：Cache.bytes > maxBytes
选择：LRU链表尾部的partition
操作：
  - 原子设置size为evicted
  - 从parts删除
  - 从LRU移除
  - 减少bytes统计

[并发访问与Eviction的竞态]
T0: Add操作正在修改partition
T1: Eviction选中该partition
T2: Eviction设置size=evicted
T3: Add的recordUpdate检测到evicted
T4: Add放弃更新统计（数据已无效）

→ 设计保证：被evict的数据不会影响Cache统计
```

### 2.4 数据结构详解

**Cache核心字段：**
```go
type Cache struct {
    // ======== 容量配置 ========
    maxBytes int32  // 最大字节数（不可变）
                    // 例如：128MB = 134,217,728 bytes

    // ======== 原子统计 ========
    bytes   int32   // 当前使用字节数（atomic）
    entries int32   // 当前entry数量（atomic）
    // 为什么用atomic？
    // - 高频更新（每次Add/Clear）
    // - 读取无锁（metrics查询）
    // - 与partition.size配合实现无锁统计

    // ======== 锁保护的索引 ========
    mu    syncutil.Mutex
    parts map[roachpb.RangeID]*partition
    // parts的作用：
    // 1. O(1)查找partition
    // 2. 判断Range是否在缓存中
    // 3. Eviction时删除

    lru   partitionList  // 双向循环链表
    // lru的作用：
    // 1. 追踪访问顺序
    // 2. O(1)移动到头部
    // 3. O(1)获取尾部（eviction）

    // ======== 监控指标 ========
    metrics Metrics
    // - Size: 当前entry数量
    // - Bytes: 当前字节数
    // - Accesses: 总访问次数
    // - Hits: 命中次数
    // - ReadBytes: 累计读取字节数
}
```

**Partition核心字段：**
```go
type partition struct {
    id roachpb.RangeID  // Range标识

    // ======== 数据存储 ========
    mu      syncutil.RWMutex
    ringBuf ringBuf  // 环形缓冲区
    // 为什么用ringBuf而不是map？
    // 1. entries通常是连续的index
    // 2. ringBuf内存紧凑，缓存友好
    // 3. 支持高效的范围扫描
    // 4. 自动处理index重叠和gap

    // ======== 原子大小统计 ========
    size cacheSize  // atomic uint64
    // 编码：高32位=entries数，低32位=bytes数
    // 为什么不用两个独立的atomic？
    // - 单次CAS可以同时更新两个值
    // - 避免中间状态（bytes变了但entries没变）
    // - 性能更好

    // ======== LRU链表指针 ========
    next, prev *partition
    // 访问规则：
    // - 读写需要Cache.mu
    // - 嵌入式链表，无额外分配
}

const partitionSize = int32(unsafe.Sizeof(partition{}))
// 约48字节（在64位系统上）
// 用于计算partition本身的内存开销
```

**cacheSize的巧妙编码：**
```go
type cacheSize uint64

// 布局：
// ┌────────────────────┬────────────────────┐
// │   高32位: entries  │   低32位: bytes    │
// └────────────────────┴────────────────────┘

func newCacheSize(bytes, entries int32) cacheSize {
    return cacheSize((uint64(entries) << 32) | uint64(bytes))
}

func (cs cacheSize) bytes() int32 {
    return int32(cs & 0xFFFFFFFF)
}

func (cs cacheSize) entries() int32 {
    return int32(cs >> 32)
}

// 使用CAS原子操作
func (p *partition) setSize(old, new cacheSize) bool {
    return atomic.CompareAndSwapUint64(
        (*uint64)(&p.size),
        uint64(old),
        uint64(new),
    )
}

// 特殊值
const evicted cacheSize = 0  // 表示已被evict

优势：
1. 单次CAS原子更新两个值
2. 无需两次原子操作
3. 避免bytes和entries不一致的中间状态
4. 性能：单次CAS vs 两次atomic操作
```

### 2.5 RingBuf详解

**为什么使用环形缓冲区？**
```
Raft log的特点：
1. Index连续：[100, 101, 102, 103, ...]
2. 顺序追加：新entries追加到尾部
3. 从头清理：旧entries从头部删除
4. 范围查询：Scan [lo, hi)

环形缓冲区完美匹配这些特点！
```

**RingBuf结构：**
```go
type ringBuf struct {
    buf  []raftpb.Entry  // 底层数组（容量是2的幂）
    head int             // 第一个有效entry的位置
    len  int             // 有效entry数量
}

// 可视化示例：
buf.len = 8, head = 2, len = 5

     0    1    2    3    4    5    6    7
  ┌────┬────┬────┬────┬────┬────┬────┬────┐
  │    │    │ E1 │ E2 │ E3 │ E4 │ E5 │    │
  └────┴────┴────┴────┴────┴────┴────┴────┘
            ↑                      ↑
          head              (head+len-1)%8

环形特性：如果继续添加E6, E7, E8
     0    1    2    3    4    5    6    7
  ┌────┬────┬────┬────┬────┬────┬────┬────┐
  │ E7 │ E8 │ E1 │ E2 │ E3 │ E4 │ E5 │ E6 │
  └────┴────┴────┴────┴────┴────┴────┴────┘
            ↑
          head
  len = 8 (满了)
```

**关键操作：**
```go
// 1. 添加entries
func (b *ringBuf) add(ents []Entry) (bytesAdded, entriesAdded int32) {
    // 步骤1：检查是否需要扩展
    // - 如果ents是连续的（紧接着最后一个entry）
    // - 如果buf空间不足，扩展buf（realloc）

    // 步骤2：处理index重叠
    // - 如果ents[0].Index <= lastCachedIndex
    // - 覆盖重叠的部分

    // 步骤3：写入新entries
    // - 使用iterator遍历
    // - 处理环形边界

    return bytesAdded, entriesAdded
}

// 2. 获取单个entry
func (b *ringBuf) get(index Index) (Entry, bool) {
    // 步骤1：计算offset
    offset := int(index) - int(first(b).index(b))
    if offset < 0 || offset >= b.len {
        return Entry{}, false  // 不在缓存范围
    }

    // 步骤2：定位实际位置
    pos := (b.head + offset) % len(b.buf)

    return b.buf[pos], true
}

// 3. 范围扫描
func (b *ringBuf) scan(
    ents []Entry, lo, hi Index, maxBytes uint64,
) ([]Entry, uint64, Index, bool) {
    // 步骤1：定位起始位置
    it, ok := iterateFrom(b, lo)
    if !ok {
        return ents, 0, lo, false
    }

    // 步骤2：顺序读取
    var bytes uint64
    for ok && it.index(b) < hi {
        e := it.entry(b)
        size := uint64(e.Size())

        // 检查大小限制
        if bytes+size > maxBytes && len(ents) > 0 {
            return ents, bytes, it.index(b), true
        }

        bytes += size
        ents = append(ents, *e)
        it, ok = it.next(b)
    }

    return ents, bytes, hi, false
}

// 4. 清理旧entries
func (b *ringBuf) clearTo(hi Index) (bytesRemoved, entriesRemoved int32) {
    // 从head开始清理，直到index > hi
    it := first(b)
    for ok := it.valid(b); ok && it.index(b) <= hi; {
        bytesRemoved += int32(it.entry(b).Size())
        entriesRemoved++
        it.clear(b)  // 清零entry
        it, ok = it.next(b)
    }

    // 移动head指针
    b.head = (b.head + int(entriesRemoved)) % len(b.buf)
    b.len -= int(entriesRemoved)

    // 如果使用率太低，收缩buf
    if b.len < len(b.buf)/8 {
        realloc(b, 0, b.len)
    }

    return bytesRemoved, entriesRemoved
}
```

**自动扩展和收缩：**
```go
const (
    shrinkThreshold = 8   // 收缩阈值
    minBufSize      = 16  // 最小大小
)

// 扩展策略
func reallocLen(need int) int {
    if need <= minBufSize {
        return minBufSize
    }
    // 找到大于等于need的最小2的幂
    return 1 << bits.Len(uint(need))
}

// 示例：
// need=10  → 16  (2^4)
// need=20  → 32  (2^5)
// need=100 → 128 (2^7)

// 收缩时机
func clearTo(hi Index) {
    // 清理后
    if b.len < len(b.buf)/shrinkThreshold {
        realloc(b, 0, b.len)
    }
}

// 例如：buf.len=128, b.len=10
// 10 < 128/8 = 16，触发收缩
// 重新分配buf为16（minBufSize）
```

---

## 三、DFS How（上）：核心数据结构与基础操作

### 3.1 NewCache() - 构造函数

**完整实现：**
```go
func NewCache(maxBytes uint64) *Cache {
    // 限制：maxBytes必须小于math.MaxInt32
    // 原因：内部使用int32存储大小
    if maxBytes > math.MaxInt32 {
        maxBytes = math.MaxInt32  // 2GB上限
    }

    return &Cache{
        maxBytes: int32(maxBytes),
        metrics:  makeMetrics(),
        parts:    map[roachpb.RangeID]*partition{},
        // 注意：lru和bytes/entries使用零值初始化
    }
}
```

**调用位置**（pkg/kv/kvserver/store.go:1609）：
```go
func NewStore(...) *Store {
    // ... 其他初始化 ...

    // 11. 创建 Raft entry 缓存
    s.raftEntryCache = raftentry.NewCache(cfg.RaftEntryCacheSize)
    s.metrics.registry.AddMetricStruct(s.raftEntryCache.Metrics())

    // cfg.RaftEntryCacheSize 默认值：
    // 128MB = 128 * 1024 * 1024 = 134,217,728 bytes
}
```

**配置参数：**
```go
// 配置文件或环境变量
--raft-entry-cache-size=128MiB

// 动态调整？
// 不支持！Cache大小在启动时固定
// 修改需要重启Store

// 为什么不支持动态调整？
// - maxBytes是int32常量
// - Eviction逻辑依赖固定阈值
// - 动态调整需要复杂的并发控制
```

### 3.2 Add() - 添加Entries

**函数签名：**
```go
func (c *Cache) Add(
    id roachpb.RangeID,
    ents []raftpb.Entry,
    truncate bool,  // 是否先截断
)
```

**完整流程：**
```go
func (c *Cache) Add(id roachpb.RangeID, ents []Entry, truncate bool) {
    // ======== 阶段0：快速返回 ========
    if len(ents) == 0 {
        return
    }

    // ======== 阶段1：分析entries ========
    bytesGuessed := analyzeEntries(ents)
    // analyzeEntries做什么？
    // 1. 计算总大小：sum(e.Size())
    // 2. 验证连续性：e[i].Index = e[i-1].Index + 1
    // 3. 验证term单调：e[i].Term >= e[i-1].Term

    add := bytesGuessed <= c.maxBytes
    // 如果entries太大（超过整个cache），不添加
    // 例如：ents总大小200MB，cache只有128MB

    if !add {
        bytesGuessed = 0  // 标记为不添加
    }

    // ======== 阶段2：定位或创建partition ========
    c.mu.Lock()
    p := c.getPartLocked(id, add /* create */, true /* recordUse */)
    // getPartLocked逻辑：
    // - 查找parts[id]
    // - 如果不存在且create=true，创建新partition
    // - 如果recordUse=true，移动到LRU头部

    // ======== 阶段3：预留空间 ========
    if bytesGuessed > 0 {
        // 3.1 先evict以腾出空间
        c.evictLocked(bytesGuessed)
        // evictLocked会：
        // - 从LRU尾部开始evict
        // - 直到bytes + bytesGuessed <= maxBytes

        // 3.2 如果evict了所有partition（极端情况）
        if len(c.parts) == 0 {
            // 重新创建当前Range的partition
            p = c.getPartLocked(id, true, false)
        }

        // 3.3 乐观地增加partition.size（猜测值）
        for {
            prev := p.loadSize()
            if p.setSize(prev, prev.add(bytesGuessed, 0)) {
                break
            }
            // CAS失败，重试
        }
    }
    c.mu.Unlock()
    // ← 关键：Cache.mu已释放！

    // ======== 阶段4：检查partition ========
    if p == nil {
        // partition不存在且没有创建
        // 只有在!add时可能发生
        return
    }

    // ======== 阶段5：修改partition数据 ========
    p.mu.Lock()
    defer p.mu.Unlock()

    var bytesAdded, entriesAdded int32
    var bytesRemoved, entriesRemoved int32

    // 5.1 可选：先截断
    if truncate {
        // 删除index >= ents[0].Index的所有旧entries
        truncIdx := RaftIndex(ents[0].Index)
        bytesRemoved, entriesRemoved = p.truncateFrom(truncIdx)
        // 为什么需要truncate？
        // - Leader改变时，新Leader可能覆盖旧entries
        // - 例如：旧[100,101,102]，新[100,101,103]
        // - 必须先删除102
    }

    // 5.2 添加新entries
    if add {
        bytesAdded, entriesAdded = p.add(ents)
        // ringBuf.add会智能处理：
        // - 连续追加
        // - 重叠覆盖
        // - 扩展buf
    }

    // ======== 阶段6：更新统计 ========
    c.recordUpdate(
        p,
        bytesAdded-bytesRemoved,  // 实际字节变化
        bytesGuessed,             // 之前猜测的字节
        entriesAdded-entriesRemoved,
    )
    // recordUpdate的复杂逻辑在下篇详解
}
```

**调用示例**（pkg/kv/kvserver/logstore/logstore.go:666）：
```go
func LoadEntries(...) ([]Entry, error) {
    // ... 从Pebble读取entries ...

    // 将读取到的entries加入缓存
    eCache.Add(rangeID, ents, false /* truncate */)

    return ents, nil
}
```

### 3.3 Get() - 读取单个Entry

**完整实现：**
```go
func (c *Cache) Get(
    id roachpb.RangeID,
    idx kvpb.RaftIndex,
) (e raftpb.Entry, ok bool) {
    // ======== 阶段1：记录访问 ========
    c.metrics.Accesses.Inc(1)

    // ======== 阶段2：定位partition ========
    c.mu.Lock()
    p := c.getPartLocked(id, false /* create */, true /* recordUse */)
    c.mu.Unlock()
    // 注意：
    // - create=false：不创建新partition（Get不应该修改cache）
    // - recordUse=true：更新LRU（标记为最近使用）

    if p == nil {
        // Cache miss：该Range不在缓存中
        return e, false
    }

    // ======== 阶段3：读取数据 ========
    p.mu.RLock()  // 读锁，允许并发读
    defer p.mu.RUnlock()

    e, ok = p.get(idx)
    // ringBuf.get逻辑：
    // - 计算offset = idx - firstIndex
    // - 检查offset是否在[0, len)范围
    // - 返回buf[(head+offset) % len(buf)]

    // ======== 阶段4：更新指标 ========
    if ok {
        c.metrics.Hits.Inc(1)
        c.metrics.ReadBytes.Inc(int64(e.Size()))
    }

    return e, ok
}
```

**性能特点：**
```
缓存命中路径：
1. Accesses.Inc(1)        - 原子操作，10ns
2. Cache.mu.Lock()        - 无竞争时，20ns
3. map lookup             - O(1)，30ns
4. moveToFront(p)         - 指针操作，20ns
5. Cache.mu.Unlock()      - 10ns
6. partition.mu.RLock()   - 20ns
7. ringBuf.get()          - 计算+数组访问，30ns
8. partition.mu.RUnlock() - 10ns
9. metrics更新            - 30ns

总计：~180ns（无竞争）

缓存未命中：
- 前5步相同：~90ns
- 返回false：10ns
总计：~100ns

对比直接磁盘读取：
- Pebble读取：1-5ms = 1,000,000-5,000,000ns
- 性能提升：5,000-50,000倍！
```

### 3.4 Scan() - 批量读取

**函数签名：**
```go
func (c *Cache) Scan(
    ents []raftpb.Entry,  // 输入buffer（复用内存）
    id roachpb.RangeID,
    lo, hi kvpb.RaftIndex, // [lo, hi) 左闭右开
    maxBytes uint64,       // 大小限制
) (
    _ []raftpb.Entry,      // 返回的entries
    bytes uint64,          // 实际字节数
    nextIdx kvpb.RaftIndex,// 下一个要读取的index
    exceededMaxBytes bool, // 是否因大小限制停止
)
```

**完整实现：**
```go
func (c *Cache) Scan(
    ents []Entry, id RangeID, lo, hi RaftIndex, maxBytes uint64,
) ([]Entry, uint64, RaftIndex, bool) {
    // ======== 阶段1：记录访问 ========
    c.metrics.Accesses.Inc(1)

    // ======== 阶段2：定位partition ========
    c.mu.Lock()
    p := c.getPartLocked(id, false, true /* recordUse */)
    c.mu.Unlock()

    if p == nil {
        // 未命中：返回空结果
        return ents, 0, lo, false
    }

    // ======== 阶段3：扫描数据 ========
    p.mu.RLock()
    defer p.mu.RUnlock()

    ents, bytes, nextIdx, exceededMaxBytes := p.scan(ents, lo, hi, maxBytes)
    // ringBuf.scan的逻辑：
    // 1. 从lo开始迭代
    // 2. 累加bytes，检查是否超过maxBytes
    // 3. 遇到gap或hi停止
    // 4. 返回读取到的entries和停止位置

    // ======== 阶段4：更新指标 ========
    c.metrics.ReadBytes.Inc(int64(bytes))

    // 判断是否算"命中"
    // 命中条件：读取了所有请求的entries OR 因maxBytes停止
    if nextIdx == hi || exceededMaxBytes {
        c.metrics.Hits.Inc(1)
    }
    // 如果因为cache miss停止（nextIdx < hi && !exceededMaxBytes）
    // 不算命中

    return ents, bytes, nextIdx, exceededMaxBytes
}
```

**使用示例**（pkg/kv/kvserver/logstore/logstore.go:608）：
```go
func LoadEntries(...) ([]Entry, error) {
    ents := make([]Entry, 0, min(hi-lo, 100))

    // 先尝试从缓存读取
    ents, _, hitIndex, _ := eCache.Scan(ents, rangeID, lo, hi, maxBytes)

    if len(ents) == int(hi-lo) {
        // 全部命中！直接返回
        return ents, cachedSize, 0, nil
    }

    // 部分命中或完全未命中，从磁盘读取剩余部分
    expectedIndex := hitIndex  // 从未命中的位置开始

    // ... 从Pebble读取 [expectedIndex, hi) ...

    // 将从磁盘读取的entries加入缓存
    eCache.Add(rangeID, ents, false)

    return ents, cachedSize, loadedSize, nil
}
```

**性能分析：**
```
场景1：完全命中 (90%情况)
Request: Scan(Range1, [100-200], maxBytes=1MB)
Cache状态：[100-250]都在缓存

执行：
- 定位partition：100ns
- RLock：20ns
- 扫描100个entries：100 × 50ns = 5μs
- RUnlock：10ns
总计：~5.2μs

场景2：部分命中 (5%情况)
Request: Scan(Range1, [100-200], maxBytes=1MB)
Cache状态：[100-150]在缓存，[151-200]不在

执行：
- 缓存读取[100-150]：2.6μs
- 返回nextIdx=151
- 调用方从Pebble读取[151-200]：1ms
总计：~1ms (但减少了50%的磁盘IO)

场景3：完全未命中 (5%情况)
- 定位partition：100ns
- 发现p=nil或范围不匹配
- 返回lo
总计：~100ns (快速失败)
```

### 3.5 Clear() - 清理旧Entries

**函数签名：**
```go
func (c *Cache) Clear(id roachpb.RangeID, hi kvpb.RaftIndex)
// 删除该Range中所有 index <= hi 的entries
```

**完整实现：**
```go
func (c *Cache) Clear(id RangeID, hi RaftIndex) {
    // ======== 阶段1：定位partition ========
    c.mu.Lock()
    p := c.getPartLocked(id, false /* create */, false /* recordUse */)
    // 注意：
    // - create=false：Clear不创建partition
    // - recordUse=false：Clear不算"使用"（避免影响LRU）

    if p == nil {
        c.mu.Unlock()
        return  // 该Range不在缓存中，无需清理
    }
    c.mu.Unlock()

    // ======== 阶段2：清理数据 ========
    p.mu.Lock()  // 写锁，独占访问
    defer p.mu.Unlock()

    bytesRemoved, entriesRemoved := p.clearTo(hi)
    // ringBuf.clearTo逻辑：
    // 1. 从head开始遍历
    // 2. 清零所有index <= hi的entries
    // 3. 移动head指针
    // 4. 可能触发buf收缩

    // ======== 阶段3：更新统计 ========
    c.recordUpdate(
        p,
        -1*bytesRemoved,  // 负值，减少bytes
        0,                // 没有guess
        -1*entriesRemoved,
    )
}
```

**使用场景：**
```go
// 场景1：Raft log压缩（最常见）
func (r *Replica) TruncateLog(index RaftIndex) {
    // 截断Raft log
    r.raftMu.stateLoader.SetRaftTruncatedState(ctx, batch, truncState)

    // 清理缓存中的旧entries
    r.store.raftEntryCache.Clear(r.RangeID, index-1)
    // 例如：truncate到index=1000
    // 清理缓存中[1-999]的所有entries
}

// 场景2：Range分裂
func (r *Replica) splitTrigger(...) {
    // 左半Range保留 [oldStart, splitKey)
    // 右半Range使用 [splitKey, oldEnd)

    // 清理不属于该Range的缓存
    if isRightRange {
        r.store.raftEntryCache.Clear(r.RangeID, splitIndex-1)
    }
}

// 场景3：Range合并
func (r *Replica) mergeTrigger(...) {
    // 被合并的Range（右半）的缓存需要清理
    r.store.raftEntryCache.Drop(rhsRangeID)
    // Drop vs Clear:
    // - Drop: 删除整个partition
    // - Clear: 删除部分entries
}
```

---

## 四、总结与下篇预告

### 4.1 上篇核心要点

1. **设计动机**：
   - Raft日志读取频繁，磁盘IO成为瓶颈
   - Sideloaded entry需要缓存fat entry
   - 并发访问需要低竞争设计

2. **两级锁架构**：
   - Cache.mu：保护partition索引和LRU
   - partition.mu：保护实际数据
   - 不同Range完全并行

3. **核心数据结构**：
   - Cache：全局管理器
   - partition：per-Range存储
   - ringBuf：高效的环形缓冲区
   - cacheSize：巧妙的原子编码

4. **基础操作**：
   - NewCache：创建缓存
   - Add：添加entries（两阶段）
   - Get：读取单个entry（快速路径）
   - Scan：批量读取（高效扫描）
   - Clear：清理旧数据

### 4.2 下篇内容预告

在下篇中，我们将深入分析：

**1. 高级操作与并发控制：**
   - recordUpdate的原子协议
   - evictLocked的LRU eviction
   - Drop的partition删除
   - 并发竞态的完整分析

**2. Runtime Behavior：**
   - 完整的读写流程timeline
   - Sideloaded entry的特殊处理
   - Eviction触发的详细过程
   - 内存使用的动态分析

**3. Design Patterns：**
   - 两级锁的并发模式
   - 乐观锁+CAS的无锁协议
   - LRU+Partition的混合策略
   - 对象池化的考量

**4. Concrete Examples：**
   - Leader发送MsgApp的完整流程
   - Follower接收日志的缓存命中
   - Log truncation的缓存更新
   - Range split的partition处理

**5. Trade-offs：**
   - 全局vs分区缓存
   - 精确vs估算的大小
   - Eviction粒度的权衡
   - 性能vs内存的平衡

**6. Mental Model：**
   - 缓存层次模型
   - 并发访问的思维框架
   - Eviction的直觉理解
   - 调试和诊断思路

---

**继续阅读下篇，了解RaftEntry Cache的完整实现细节和高级特性！**

---

## 附录A：快速参考

**关键类型：**
```go
Cache           // 全局缓存管理器
partition       // per-Range数据存储
ringBuf         // 环形缓冲区
cacheSize       // 原子编码的大小（uint64 = bytes + entries）
partitionList   // LRU双向链表
```

**关键常量：**
```go
partitionSize = 48字节    // partition对象大小
evicted = 0               // 标记已evict
shrinkThreshold = 8       // 收缩阈值
minBufSize = 16          // 最小buf大小
```

**关键配置：**
```go
maxBytes = 128MB         // 默认缓存大小
RaftEntryCacheSize       // 配置参数
```

**性能指标：**
```
Get延迟（命中）：~180ns
Get延迟（未命中）：~100ns
Scan延迟（100 entries）：~5μs
Add延迟：~10μs（含eviction）
```
