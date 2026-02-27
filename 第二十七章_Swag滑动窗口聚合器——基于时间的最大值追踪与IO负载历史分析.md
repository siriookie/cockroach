# 第二十七章 Swag 滑动窗口聚合器——基于时间的最大值追踪与 IO 负载历史分析

## 引言：为何需要追踪 IO 指标的历史最大值

在分布式数据库中，**瞬时 IO 指标**（如 Pebble 的 L0 文件数、L0 子层数）可能因为 compaction 完成而骤降，但这**不意味着系统已完全恢复**。真实场景中：

1. **Compaction 完成后的短暂平静**：L0 文件从 1000 降至 200，但积压的写入请求可能在数秒内再次推高 L0
2. **准入控制的滞后性**：如果仅基于瞬时值决策，可能在系统未真正稳定前就放开流控，导致再次过载
3. **分配器决策的稳定性**：决定是否将 lease 从某个 Store 迁移时，需要基于"过去 N 分钟的最差状态"而非"当前瞬时值"

为了解决这些问题，CockroachDB 实现了 **`Swag`（Sliding Window Aggregator）** —— 一种**时间窗口内的聚合器**，能够高效追踪**过去 5 分钟内的最大值**，为准入控制、副本分配、lease 迁移等决策提供**平滑且保守**的依据。

本章将深入分析 `Swag` 的实现原理、生命周期以及在 CockroachDB 的 IO 负载监控中的具体应用。

---

## I. 鸟瞰：Swag 是什么，为什么需要它

### 1.1 核心职责

`Swag` 是一个**基于时间分桶的滑动窗口聚合器**（Sliding Window Aggregator），支持任意**结合律（associative）二元操作**。它的核心职责是：

**输入**（构造时）：
- `now`：当前时间（用于初始化窗口）
- `interval`：每个窗口的时间跨度（如 1 分钟）
- `size`：窗口数量（如 5 个窗口）
- `binOp`：二元聚合函数（如 `max(a, b)`、`sum(a, b)`）

**输入**（运行时）：
- `Record(now, val)`：记录一个新值到当前窗口
- `Query(now)`：查询所有窗口的聚合结果

**输出**：
- 聚合值（如过去 5 分钟的最大值）
- 实际覆盖的时间跨度（如果系统运行不足 5 分钟，返回实际跨度）

**时间复杂度**：
- `Record()`：平均 **O(1)**（无需轮转时），最坏 **O(k)**（需要轮转 k 个窗口）
- `Query()`：**O(k)**，k = 窗口数量

**空间复杂度**：
- **32 + 8k 字节**（对于 5 个窗口，约 72 字节）

### 1.2 为何需要滑动窗口而非单一时间点

**对比方案 1：仅记录瞬时值**

```go
// 简单方案
type SimpleTracker struct {
    currentValue float64
}

func (t *SimpleTracker) Record(val float64) {
    t.currentValue = val
}

func (t *SimpleTracker) Query() float64 {
    return t.currentValue
}
```

**问题**：
- **时序 T0**：L0 文件数 = 1000（严重过载）
- **时序 T0+10s**：Compaction 完成，L0 文件数 = 200（看似恢复）
- **时序 T0+15s**：写入激增，L0 文件数 = 950（再次过载）

**后果**：
- 如果在 T0+10s 基于 `currentValue=200` 放开流控，T0+15s 会再次过载
- 系统陷入"过载 → 放开 → 过载"的振荡循环

**对比方案 2：固定时间窗口（如最近 5 分钟的所有采样点）**

```go
type HistoryTracker struct {
    samples []struct {
        timestamp time.Time
        value     float64
    }
}

func (t *HistoryTracker) Record(now time.Time, val float64) {
    t.samples = append(t.samples, struct{...}{now, val})
    // 移除 5 分钟前的样本
    cutoff := now.Add(-5 * time.Minute)
    for len(t.samples) > 0 && t.samples[0].timestamp.Before(cutoff) {
        t.samples = t.samples[1:]
    }
}

func (t *HistoryTracker) QueryMax() float64 {
    max := 0.0
    for _, s := range t.samples {
        if s.value > max {
            max = s.value
        }
    }
    return max
}
```

**问题**：
- **空间开销**：如果每秒采样一次，5 分钟 = 300 个样本，每样本 16 字节（time.Time + float64），总计 **4.8 KB**
- **查询复杂度**：O(n)，n = 样本数（300）
- **内存分配**：`append()` 可能触发频繁的切片扩容

**Swag 的优势**：

| 维度 | 方案 1（瞬时值） | 方案 2（全历史） | **Swag（分桶窗口）** |
|------|----------------|-----------------|-------------------|
| **空间** | 8 字节 | 4.8 KB | **72 字节**（5 窗口） |
| **记录** | O(1) | O(n)（移除旧样本） | **O(1)** 平均 |
| **查询** | O(1) | O(n) | **O(k)** (k=5) |
| **平滑性** | ❌ 振荡 | ✅ 平滑 | ✅ 平滑 |
| **内存分配** | 无 | 频繁 | **无**（预分配） |

### 1.3 在整个系统中的位置

```
┌─────────────────────────────────────────────────────────────┐
│              Pebble LSM-Tree (存储引擎)                      │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  实时指标更新（Compaction、Flush、Write Stall）      │   │
│  │  - L0NumSubLevels: 18 (当前值)                       │   │
│  │  - L0NumFiles: 950 (当前值)                          │   │
│  │  - L0Size: 512 MB (当前值)                           │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓ 每 15 秒读取
┌─────────────────────────────────────────────────────────────┐
│      IOLoadListener (pkg/util/admission/io_load_listener.go)│
│  ┌──────────────────────────────────────────────────────┐   │
│  │  计算 IOThreshold（基于 LSM 状态）                    │   │
│  │  - L0NumSubLevels: 18                                │   │
│  │  - L0NumFiles: 950                                   │   │
│  │  - L0Size: 512 MB                                    │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓ 调用 Store.UpdateIOThreshold()
┌─────────────────────────────────────────────────────────────┐
│             Store.ioThreshold (本章主角)                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  三个 Swag 实例（5 分钟窗口，1 分钟粒度）              │   │
│  │  1. maxL0NumSubLevels.Record(18)                     │   │
│  │     → Query() = max(15, 17, 18, 16, 14) = 18         │   │
│  │  2. maxL0NumFiles.Record(950)                        │   │
│  │     → Query() = max(800, 920, 950, 880, 750) = 950   │   │
│  │  3. maxL0Size.Record(512)                            │   │
│  │     → Query() = max(480, 510, 512, 490, 460) = 512   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓ 通过 Gossip 传播
┌─────────────────────────────────────────────────────────────┐
│           StorePool (所有节点共享的 Store 信息)              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  StoreDescriptor {                                   │   │
│  │    Capacity {                                        │   │
│  │      IOThreshold: 瞬时值 (L0SubLevels=18)            │   │
│  │      IOThresholdMax: 最大值 (L0SubLevels=18)         │   │
│  │    }                                                 │   │
│  │  }                                                   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓ 被多个组件使用
┌─────────────────────────────────────────────────────────────┐
│                    消费者 (决策系统)                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  1. Allocator (分配器)                               │   │
│  │     - 基于 IOThresholdMax 决定是否迁移副本            │   │
│  │  2. LeaseQueue (租约队列)                            │   │
│  │     - 基于 IOThresholdMax 决定是否转移 lease          │   │
│  │  3. ioThresholdMap (Replica 流控)                   │   │
│  │     - 基于 IOThreshold.Score() 暂停 follower         │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**关键交互**：
- **数据源**：Pebble 引擎的实时指标
- **中间层**：IOLoadListener 每 15 秒计算一次 IOThreshold
- **Swag 层**：Store.ioThreshold 维护 5 分钟滑动窗口
- **传播层**：Gossip 将 IOThreshold + IOThresholdMax 传播到集群
- **决策层**：Allocator、LeaseQueue、Replica 流控等消费者

---

## II. 控制流：从构造到查询的完整生命周期

### 2.1 阶段 1：构造 Swag 实例（Store 启动时）

**触发时机**：Store 初始化（[pkg/kv/kvserver/store.go:1501-1503](pkg/kv/kvserver/store.go#L1501-L1503)）

```go
func NewStore(...) *Store {
    // ... 省略其他初始化 ...

    s.ioThreshold.t = &admissionpb.IOThreshold{}
    // 创建 3 个 Swag 实例，用于追踪过去 5 分钟的最大值
    now := cfg.Clock.Now().GoTime()
    s.ioThreshold.maxL0NumSubLevels = slidingwindow.NewMaxSwag(now, time.Minute, 5)
    s.ioThreshold.maxL0NumFiles = slidingwindow.NewMaxSwag(now, time.Minute, 5)
    s.ioThreshold.maxL0Size = slidingwindow.NewMaxSwag(now, time.Minute, 5)

    // ... 后续初始化 ...
}
```

**`NewMaxSwag()` 实现**（[pkg/util/slidingwindow/helpers.go:11-23](pkg/util/slidingwindow/helpers.go#L11-L23)）：

```go
func NewMaxSwag(now time.Time, interval time.Duration, size int) *Swag {
    return NewSwag(
        now,
        interval,
        size,
        func(acc, val float64) float64 {
            if acc > val {
                return acc
            }
            return val
        },
    )
}
```

**关键参数**：
- `now`：Store 启动时间（如 2025-01-15 10:00:00）
- `interval`：1 分钟（每个窗口覆盖 1 分钟）
- `size`：5（总共 5 个窗口，覆盖 5 分钟）
- `binOp`：`max(a, b)` 函数（返回两者中的最大值）

**`NewSwag()` 核心逻辑**（[pkg/util/slidingwindow/sliding_window.go:42-55](pkg/util/slidingwindow/sliding_window.go#L42-L55)）：

```go
func NewSwag(
    now time.Time, interval time.Duration, size int, binOp func(acc, val float64) float64,
) *Swag {
    windows := make([]*float64, size)  // 分配 5 个指针位置
    var first float64  // 初始化第一个窗口为 0
    windows[0] = &first
    return &Swag{
        curIdx:         0,              // 当前窗口索引 = 0
        windows:        windows,        // [&0, nil, nil, nil, nil]
        lastRotate:     now,            // 上次轮转时间 = now
        rotateInterval: interval,       // 1 分钟
        binOp:          binOp,          // max 函数
    }
}
```

**初始状态**（假设 now = 10:00:00）：
```
windows = [&0.0, nil, nil, nil, nil]
           ↑ curIdx
lastRotate = 10:00:00
rotateInterval = 1m
```

**内存布局**：
```
Swag 结构体：
  curIdx:         4 字节 (int)
  windows:        24 字节 (slice header: ptr + len + cap)
  lastRotate:     16 字节 (time.Time: wall + ext + loc)
  rotateInterval: 8 字节 (time.Duration)
  binOp:          8 字节 (function pointer)
总计：60 字节 + 5 × 8 字节 (float64 指针) = 100 字节

实际 float64 值：5 × 8 字节 = 40 字节
总内存：~140 字节/Swag（实际因对齐可能更大）
```

### 2.2 阶段 2：记录新值（每 15 秒一次）

**触发时机**：IOLoadListener 完成指标计算（[pkg/kv/kvserver/store.go:2824-2826](pkg/kv/kvserver/store.go#L2824-L2826)）

```go
func (s *Store) UpdateIOThreshold(ioThreshold *admissionpb.IOThreshold) {
    now := s.Clock().Now().GoTime()

    s.ioThreshold.Lock()
    s.ioThreshold.t = ioThreshold
    // 记录到滑动窗口（关键操作）
    s.ioThreshold.maxL0NumSubLevels.Record(now, float64(ioThreshold.L0NumSubLevels))
    s.ioThreshold.maxL0NumFiles.Record(now, float64(ioThreshold.L0NumFiles))
    s.ioThreshold.maxL0Size.Record(now, float64(ioThreshold.L0Size))
    // ... 后续查询和 gossip ...
}
```

**`Record()` 核心逻辑**（[pkg/util/slidingwindow/sliding_window.go:59-62](pkg/util/slidingwindow/sliding_window.go#L59-L62)）：

```go
func (s *Swag) Record(now time.Time, val float64) {
    s.maybeRotate(now)  // 1. 检查是否需要轮转窗口
    *s.windows[s.curIdx] = s.binOp(*s.windows[s.curIdx], val)  // 2. 聚合到当前窗口
}
```

#### 步骤 1：`maybeRotate()` 检查窗口轮转（[pkg/util/slidingwindow/sliding_window.go:68-82](pkg/util/slidingwindow/sliding_window.go#L68-L82)）

```go
func (s *Swag) maybeRotate(now time.Time) {
    sinceLastRotate := now.Sub(s.lastRotate)
    if sinceLastRotate < s.rotateInterval {
        return  // 未到 1 分钟，无需轮转
    }

    size := len(s.windows)
    shift := int(sinceLastRotate / s.rotateInterval)  // 计算需要轮转的窗口数
    for i := 0; i < shift; i++ {
        s.curIdx = (s.curIdx + 1) % size  // 循环移动索引
        s.lastRotate = s.lastRotate.Add(s.rotateInterval)  // 更新轮转时间
        var next float64  // 新窗口初始化为 0
        s.windows[s.curIdx] = &next
    }
}
```

**关键设计**：
- **循环数组**：`(curIdx + 1) % size` 实现环形缓冲区
- **批量轮转**：如果系统暂停 3 分钟，`shift=3`，一次性轮转 3 个窗口
- **惰性轮转**：只在 `Record()` 或 `Query()` 时检查，无需后台定时器

**示例时间线**（假设初始 lastRotate = 10:00:00）：

| 时刻 | now | sinceLastRotate | shift | curIdx | windows 状态 |
|------|-----|----------------|-------|--------|-------------|
| T0 | 10:00:00 | 0s | 0 | 0 | [&0, nil, nil, nil, nil] |
| T1 | 10:00:45 | 45s | 0 | 0 | [&15, nil, nil, nil, nil] (Record 15) |
| T2 | 10:01:05 | 65s | 1 | 1 | [&15, &0, nil, nil, nil] (轮转) |
| T3 | 10:01:20 | 20s | 0 | 1 | [&15, &18, nil, nil, nil] (Record 18) |
| T4 | 10:02:10 | 70s | 1 | 2 | [&15, &18, &0, nil, nil] (轮转) |

#### 步骤 2：聚合到当前窗口

```go
*s.windows[s.curIdx] = s.binOp(*s.windows[s.curIdx], val)
```

**对于 `max` 操作**：
```go
// 当前窗口值为 15，新值为 18
*s.windows[curIdx] = max(15, 18) = 18
```

**对于 `sum` 操作**（如果用于计数）：
```go
// 当前窗口值为 100，新值为 50
*s.windows[curIdx] = sum(100, 50) = 150
```

### 2.3 阶段 3：查询聚合值（每次需要时）

**触发时机**：紧随 `Record()` 之后（[pkg/kv/kvserver/store.go:2827-2829](pkg/kv/kvserver/store.go#L2827-L2829)）

```go
func (s *Store) UpdateIOThreshold(ioThreshold *admissionpb.IOThreshold) {
    // ... Record 后 ...
    maxL0NumSubLevels, _ := s.ioThreshold.maxL0NumSubLevels.Query(now)
    maxL0NumFiles, _ := s.ioThreshold.maxL0NumFiles.Query(now)
    maxL0Size, _ := s.ioThreshold.maxL0Size.Query(now)
    s.ioThreshold.Unlock()

    ioThresholdMax := protoutil.Clone(ioThreshold).(*admissionpb.IOThreshold)
    ioThresholdMax.L0NumSubLevels = int64(maxL0NumSubLevels)
    ioThresholdMax.L0NumFiles = int64(maxL0NumFiles)
    ioThresholdMax.L0Size = int64(maxL0Size)
}
```

**`Query()` 核心逻辑**（[pkg/util/slidingwindow/sliding_window.go:86-101](pkg/util/slidingwindow/sliding_window.go#L86-L101)）：

```go
func (s *Swag) Query(now time.Time) (float64, time.Duration) {
    windows := s.Windows(now)  // 获取所有有效窗口（从新到旧）
    timeSinceRotate := now.Sub(s.lastRotate)

    var accumulator float64
    var duration time.Duration
    for i, next := range windows {
        accumulator = s.binOp(accumulator, next)  // 累积聚合
        if i == 0 {
            duration += time.Duration(float64(timeSinceRotate))  // 当前窗口的部分时间
        } else {
            duration += time.Duration(float64(s.rotateInterval))  // 完整窗口时间
        }
    }
    return accumulator, duration
}
```

**`Windows()` 获取有效窗口**（[pkg/util/slidingwindow/sliding_window.go:106-119](pkg/util/slidingwindow/sliding_window.go#L106-L119)）：

```go
func (s *Swag) Windows(now time.Time) []float64 {
    s.maybeRotate(now)  // 确保窗口是最新的
    size := len(s.windows)
    ret := make([]float64, 0, 1)

    for i := 0; i < size; i++ {
        // 从 curIdx 向后遍历（最新到最旧）
        next := s.windows[(s.curIdx+size-i)%size]
        if next == nil {
            break  // 遇到未初始化的窗口，停止
        }
        ret = append(ret, *next)
    }
    return ret  // 返回 [最新窗口, 次新窗口, ..., 最旧窗口]
}
```

**示例查询**（假设当前状态）：
```
时刻 = 10:05:30
lastRotate = 10:05:00
curIdx = 0
windows = [&18, &17, &16, &15, &14]
           ↑ curIdx

Windows() 返回：
  [(curIdx+5-0)%5] = windows[0] = 18  (10:05:00 - 10:05:30, 30秒)
  [(curIdx+5-1)%5] = windows[4] = 14  (10:04:00 - 10:05:00, 1分钟)
  [(curIdx+5-2)%5] = windows[3] = 15  (10:03:00 - 10:04:00, 1分钟)
  [(curIdx+5-3)%5] = windows[2] = 16  (10:02:00 - 10:03:00, 1分钟)
  [(curIdx+5-4)%5] = windows[1] = 17  (10:01:00 - 10:02:00, 1分钟)
  → [18, 14, 15, 16, 17]

Query() 聚合：
  accumulator = 0
  accumulator = max(0, 18) = 18
  accumulator = max(18, 14) = 18
  accumulator = max(18, 15) = 18
  accumulator = max(18, 16) = 18
  accumulator = max(18, 17) = 18
  → 返回 (18, 4m30s)
```

### 2.4 阶段 4：Gossip 传播（异步，如果阈值变化）

```go
// UpdateIOThreshold() 的最后一步
s.storeGossip.RecordNewIOThreshold(*ioThreshold, *ioThresholdMax)
```

**`RecordNewIOThreshold()` 逻辑**（简化）：
```go
func (sg *storeGossip) RecordNewIOThreshold(
    ioThreshold, ioThresholdMax admissionpb.IOThreshold,
) {
    sg.mu.Lock()
    defer sg.mu.Unlock()

    // 如果 IOThresholdMax 的 Score 显著增加，触发 gossip
    oldScore, _ := sg.mu.lastGossipedIOThresholdMax.Score()
    newScore, _ := ioThresholdMax.Score()

    if newScore > oldScore*1.05 {  // 增加 5% 以上才 gossip
        sg.mu.lastGossipedIOThresholdMax = ioThresholdMax
        sg.triggerGossip()  // 异步通知集群
    }
}
```

**效果**：
- 其他节点的 StorePool 接收到更新
- Allocator、LeaseQueue 等组件基于 `IOThresholdMax` 做出决策

---

## III. 深度剖析：核心函数的输入、输出与不变式

### 3.1 NewSwag()：构造滑动窗口聚合器

**函数签名**：
```go
func NewSwag(
    now time.Time,
    interval time.Duration,
    size int,
    binOp func(acc, val float64) float64,
) *Swag
```

**输入**：
- `now`：初始时间戳（用于 `lastRotate`）
- `interval`：窗口轮转间隔（如 1 分钟）
- `size`：窗口数量（如 5）
- `binOp`：聚合函数（必须满足结合律）

**输出**：
- 初始化的 `Swag` 对象，第一个窗口已分配并初始化为 0

**不变式**：
1. **结合律要求**：`binOp(binOp(a,b), c) == binOp(a, binOp(b,c))`
   - ✅ `max(max(a,b), c) == max(a, max(b,c))`
   - ✅ `sum(sum(a,b), c) == sum(a, sum(b,c))`
   - ❌ `avg(avg(a,b), c) != avg(a, avg(b,c))`（需要特殊处理）

2. **curIdx 范围**：`0 <= curIdx < size`
3. **windows[curIdx]**：始终非 nil（当前窗口必须已分配）
4. **未来窗口**：`windows[(curIdx+1)%size]` 到 `windows[(curIdx+size-1)%size]` 可能为 nil（取决于运行时长）

**为何第一个窗口初始化为 0**：
- 对于 `max` 操作，任何正值都会覆盖 0
- 对于 `sum` 操作，0 是加法单位元（不影响结果）
- 如果需要其他初始值（如 `-∞`），需在 `binOp` 中特殊处理

### 3.2 Record()：记录新值到当前窗口

**函数签名**：
```go
func (s *Swag) Record(now time.Time, val float64)
```

**输入**：
- `now`：当前时间戳（用于检查轮转）
- `val`：要记录的新值

**输出**：
- 无返回值（副作用：修改 `windows[curIdx]`）

**算法流程**：

#### 情况 1：无需轮转（now - lastRotate < interval）

```go
// 假设 lastRotate = 10:05:00, now = 10:05:30, interval = 1m
sinceLastRotate = 30s < 1m  // 跳过轮转

// 直接聚合到当前窗口
*windows[curIdx] = binOp(*windows[curIdx], val)
```

**时间复杂度**：**O(1)**

#### 情况 2：需要轮转 1 个窗口（interval <= now - lastRotate < 2*interval）

```go
// 假设 lastRotate = 10:05:00, now = 10:06:10, interval = 1m
sinceLastRotate = 70s
shift = int(70s / 1m) = 1

// 轮转一次
curIdx = (curIdx + 1) % size
lastRotate = lastRotate.Add(1m) = 10:06:00
windows[curIdx] = &0.0  // 新窗口

// 聚合到新窗口
*windows[curIdx] = binOp(0, val)
```

**时间复杂度**：**O(1)**

#### 情况 3：需要轮转多个窗口（系统暂停或延迟）

```go
// 假设 lastRotate = 10:05:00, now = 10:08:30, interval = 1m
sinceLastRotate = 210s
shift = int(210s / 1m) = 3

// 轮转 3 次
for i := 0; i < 3; i++ {
    curIdx = (curIdx + 1) % size
    lastRotate = lastRotate.Add(1m)
    windows[curIdx] = &0.0
}
// lastRotate 现在为 10:08:00
// 聚合到当前窗口
*windows[curIdx] = binOp(0, val)
```

**时间复杂度**：**O(shift)**，最坏情况 **O(size)**（如果 shift >= size）

**为何不限制 shift <= size**：
- 如果系统暂停 10 分钟，`shift=10 > 5`
- 轮转 10 次后，所有窗口都被清空（都是新分配的 0 值）
- 效果：丢弃所有旧数据，符合"滑动窗口"语义

**平均情况分析**：
- 如果每 15 秒调用一次 `Record()`，interval = 1 分钟
- 每 4 次调用中，只有 1 次需要轮转（60s / 15s = 4）
- **平均时间复杂度**：`(3×O(1) + 1×O(1)) / 4 = O(1)`

### 3.3 Query()：查询聚合值

**函数签名**：
```go
func (s *Swag) Query(now time.Time) (float64, time.Duration)
```

**输入**：
- `now`：当前时间戳（用于计算实际覆盖时长）

**输出**：
- `float64`：聚合结果（如过去 5 分钟的最大值）
- `time.Duration`：实际覆盖的时间跨度（如果系统运行不足 5 分钟，返回实际值）

**算法流程**：

#### 步骤 1：获取所有有效窗口

```go
windows := s.Windows(now)  // 内部调用 maybeRotate()
```

**`Windows()` 遍历逻辑**：
```go
for i := 0; i < size; i++ {
    idx := (curIdx + size - i) % size  // 从最新到最旧
    if windows[idx] == nil {
        break  // 遇到未初始化窗口，停止
    }
    ret = append(ret, *windows[idx])
}
```

**示例**（size=5, curIdx=2, 运行 3 分钟）：
```
windows = [&15, &17, &18, nil, nil]
            ↑                ↑ 未初始化（系统运行不足 5 分钟）
          idx=0            idx=3

遍历顺序：
  i=0: idx=(2+5-0)%5=2 → windows[2]=18 → append(18)
  i=1: idx=(2+5-1)%5=1 → windows[1]=17 → append(17)
  i=2: idx=(2+5-2)%5=0 → windows[0]=15 → append(15)
  i=3: idx=(2+5-3)%5=4 → windows[4]=nil → break

返回：[18, 17, 15]（3 个窗口）
```

#### 步骤 2：累积聚合

```go
var accumulator float64  // 初始值为 0（对于 max，任何正值都会覆盖）
for i, next := range windows {
    accumulator = s.binOp(accumulator, next)
}
```

**对于 `max` 操作**：
```go
accumulator = 0
accumulator = max(0, 18) = 18
accumulator = max(18, 17) = 18
accumulator = max(18, 15) = 18
→ 返回 18
```

**对于 `sum` 操作**：
```go
accumulator = 0
accumulator = sum(0, 100) = 100
accumulator = sum(100, 200) = 300
accumulator = sum(300, 150) = 450
→ 返回 450
```

#### 步骤 3：计算时间跨度

```go
var duration time.Duration
for i, _ := range windows {
    if i == 0 {
        duration += time.Duration(float64(timeSinceRotate))  // 当前窗口的部分时间
    } else {
        duration += time.Duration(float64(s.rotateInterval))  // 完整窗口
    }
}
```

**示例**（3 个窗口，timeSinceRotate = 30s）：
```
duration = 0
i=0: duration += 30s → 30s
i=1: duration += 1m → 1m30s
i=2: duration += 1m → 2m30s
→ 返回 2m30s
```

**时间复杂度**：**O(k)**，k = 有效窗口数（≤ size）

**不变式**：
- **Query() 的结果是确定性的**：相同的 `now` 和历史数据，总是返回相同的结果
- **结合律保证正确性**：即使窗口轮转，聚合顺序不影响结果

### 3.4 maybeRotate()：惰性窗口轮转

**函数签名**：
```go
func (s *Swag) maybeRotate(now time.Time)
```

**输入**：
- `now`：当前时间戳

**输出**：
- 无返回值（副作用：可能修改 `curIdx`, `lastRotate`, `windows`）

**关键设计决策**：

#### 为何使用惰性轮转而非定时器

**惰性轮转的优点**：

| 维度 | 惰性轮转 | 后台定时器 |
|------|---------|-----------|
| **线程开销** | 0（无额外 goroutine） | 1 goroutine/Swag（3 个 Swag = 3 个 goroutine） |
| **锁竞争** | 低（仅在 Record/Query 时加锁） | 高（定时器与 Record 竞争锁） |
| **时钟精度** | 依赖调用频率（15s 延迟可接受） | 高精度（但无必要） |
| **复杂性** | 简单（无需管理 goroutine 生命周期） | 复杂（需要 stopper 管理） |

**后台定时器的代价**（假设的实现）：
```go
func (s *Swag) startRotateTimer() {
    go func() {
        ticker := time.NewTicker(s.rotateInterval)
        defer ticker.Stop()
        for {
            select {
            case now := <-ticker.C:
                s.mu.Lock()
                s.maybeRotate(now)
                s.mu.Unlock()
            case <-s.stopper.ShouldQuiesce():
                return
            }
        }
    }()
}
```

**问题**：
- **内存泄漏风险**：如果忘记停止 ticker，goroutine 永久存在
- **锁竞争**：每 1 分钟抢占 `Record()` 的锁
- **不必要的精度**：IO 指标每 15 秒更新一次，1 分钟精度足够

#### 批量轮转的正确性

**场景**：系统暂停 3 分钟后恢复

```go
// 假设 lastRotate = 10:00:00, now = 10:03:05, interval = 1m
sinceLastRotate = 185s
shift = int(185s / 1m) = 3

for i := 0; i < 3; i++ {
    curIdx = (curIdx + 1) % size
    lastRotate = lastRotate.Add(1m)
    windows[curIdx] = &0.0
}
// 最终 lastRotate = 10:03:00（不是 10:03:05！）
```

**关键点**：
- `lastRotate` 始终对齐到 interval 的倍数（10:00, 10:01, 10:02, ...）
- 剩余的 5 秒（10:03:00 到 10:03:05）会在下次轮转时累积

**为何不立即轮转到 10:03:05**：
- 保持窗口边界对齐（便于调试和推理）
- 避免窗口大小不一致（所有窗口都覆盖完整的 1 分钟，除了当前窗口）

---

## IV. 运行时行为：动态信号与负载管理

### 4.1 IO 指标的传播延迟分析

**完整时间线**（从 Pebble 指标变化到决策生效）：

| 阶段 | 操作 | 延迟 | 累计延迟 | 详情 |
|------|------|------|---------|------|
| **1** | Pebble compaction 完成 | 0s | 0s | L0 文件数从 1000 → 200 |
| **2** | 等待下一次指标采样 | 0-15s | 15s | IOLoadListener 每 15s 采样一次 |
| **3** | UpdateIOThreshold() 调用 | <1ms | 15s | Record() 到 Swag |
| **4** | Query() 读取最大值 | <1ms | 15s | 返回过去 5 分钟的最大值 = 1000（未降！） |
| **5** | Gossip 传播（如果阈值变化） | 1-3s | 18s | 异步广播到集群 |
| **6** | StorePool 更新 | <100ms | 18s | 其他节点接收到更新 |
| **7** | Allocator/LeaseQueue 决策 | 变化 | 18s+ | 基于 IOThresholdMax 决策 |

**关键观察**：
- **平滑效应**：即使 L0 文件数瞬间降至 200，Swag 仍返回 1000（过去 5 分钟的最大值）
- **保守策略**：只有在 **5 分钟内** L0 文件数持续低于某个阈值，IOThresholdMax 才会下降
- **延迟成本**：系统需要 5 分钟才能"忘记"一次 IO 尖峰

### 4.2 Swag 如何防止振荡

**场景**：IO 负载在阈值附近振荡

```
时刻 T0:   L0 文件数 = 1000 (Score=1.0, 严重过载)
时刻 T0+1m: L0 文件数 = 200  (Score=0.2, compaction 完成)
时刻 T0+2m: L0 文件数 = 950  (Score=0.95, 写入激增)
时刻 T0+3m: L0 文件数 = 180  (Score=0.18, 再次 compaction)
时刻 T0+4m: L0 文件数 = 900  (Score=0.9, 再次激增)
```

**如果使用瞬时值**（没有 Swag）：

| 时刻 | Score | Allocator 决策 | 副作用 |
|------|-------|---------------|--------|
| T0 | 1.0 | 迁移 lease 离开该 Store | lease 转移开始（耗时 1-5s） |
| T0+1m | 0.2 | 允许 lease 迁回 | lease 转回（耗时 1-5s） |
| T0+2m | 0.95 | 再次迁移 lease | 频繁 lease 转移导致服务抖动 |
| T0+3m | 0.18 | 再次迁回 | 客户端经历多次 NotLeaseHolderError |

**使用 Swag（5 分钟窗口）**：

| 时刻 | 瞬时 Score | Swag Query() | Allocator 决策 | 说明 |
|------|-----------|-------------|---------------|------|
| T0 | 1.0 | 1.0 | 迁移 lease | 开始迁移 |
| T0+1m | 0.2 | 1.0 | **保持决策** | 窗口内最大值仍为 1.0 |
| T0+2m | 0.95 | 1.0 | **保持决策** | 新尖峰未超过历史最大值 |
| T0+3m | 0.18 | 1.0 | **保持决策** | 仍有历史窗口记录 1.0 |
| T0+4m | 0.9 | 1.0 | **保持决策** | 持续保护 |
| T0+5m | 0.1 | 0.95 | 可能允许迁回 | T0 的窗口已过期，最大值降为 T0+2m 的 0.95 |
| T0+7m | 0.1 | 0.1 | 允许迁回 | 所有高值窗口已过期 |

**效果**：
- **稳定性**：5 分钟内只做一次 lease 迁移决策
- **保守性**：即使中间有短暂恢复，也不会立即迁回

### 4.3 窗口大小的权衡

**当前配置**：5 个窗口 × 1 分钟 = 5 分钟

#### 如果窗口更小（如 3 分钟）

| 优点 | 缺点 |
|------|------|
| 更快响应 IO 恢复（3 分钟后降低阈值） | 更容易振荡（IO 尖峰 3 分钟后就"忘记"） |
| 减少保守过度（不会长时间拒绝 lease） | 可能在 compaction 周期内频繁变化决策 |

#### 如果窗口更大（如 10 分钟）

| 优点 | 缺点 |
|------|------|
| 极强的稳定性（10 分钟内不会振荡） | 恢复过慢（IO 问题解决后，10 分钟才能重新接受 lease） |
| 适合周期性 compaction（避免在 compaction 间隙误判） | 内存开销增加（10 个窗口 × 8 字节 = 80 字节/Swag） |

**实际选择的理由**：
- **5 分钟**是 Pebble compaction 周期的典型值（L0 → L1 compaction 通常 2-5 分钟）
- 既能平滑短期波动，又不会过度延迟恢复
- 对应 CockroachDB 的默认 `admission.kv.pause_replication_io_threshold` 检查周期

### 4.4 与其他时间窗口机制的对比

**CockroachDB 中的其他滑动窗口**：

| 组件 | 窗口大小 | 粒度 | 用途 | 实现方式 |
|------|---------|------|------|---------|
| **Swag (本章)** | 5 分钟 | 1 分钟 | IO 指标最大值追踪 | 固定大小环形缓冲区 |
| **时间序列指标** | 10 分钟 | 10 秒 | Prometheus 指标聚合 | 时间戳数组 + 过期清理 |
| **kvSlotAdjuster** | 15 秒 | 瞬时 | CPU 负载追踪 | 单个 EWMA（指数加权移动平均） |
| **Lease 过期** | 9 秒 | 不适用 | 租约有效性 | 单一过期时间戳 |

**Swag 的独特优势**：
- **固定内存**：无论运行多久，内存始终为 O(size)
- **无 GC 压力**：预分配数组，无需动态扩容
- **时间对齐**：窗口边界对齐到 interval 倍数，便于调试

---

## V. 具体示例：IO 负载波动场景的完整时间线

### 5.1 场景设定

**Store 配置**：
- 3 个 Swag 实例（maxL0NumSubLevels, maxL0NumFiles, maxL0Size）
- 窗口配置：5 个窗口 × 1 分钟 = 5 分钟
- 采样间隔：15 秒

**IO 负载时间线**（Pebble 指标）：

| 时刻 | L0NumSubLevels | L0NumFiles | L0Size (MB) | 事件 |
|------|---------------|-----------|------------|------|
| 10:00:00 | 5 | 200 | 100 | 正常运行 |
| 10:01:00 | 8 | 350 | 180 | 写入增加 |
| 10:02:00 | 15 | 800 | 420 | 负载激增 |
| 10:03:00 | 18 | 950 | 510 | 接近过载（阈值：L0SubLevels=20） |
| 10:04:00 | 19 | 980 | 525 | 达到峰值 |
| 10:05:00 | 6 | 220 | 115 | Compaction 完成，骤降 |
| 10:06:00 | 4 | 180 | 95 | 持续低负载 |
| 10:07:00 | 3 | 150 | 80 | 完全恢复 |

### 5.2 时间线分解

#### T0: 10:00:00 - Store 启动，初始化 Swag

```go
now := 10:00:00
s.ioThreshold.maxL0NumSubLevels = NewMaxSwag(now, 1m, 5)
s.ioThreshold.maxL0NumFiles = NewMaxSwag(now, 1m, 5)
s.ioThreshold.maxL0Size = NewMaxSwag(now, 1m, 5)
```

**内部状态**（maxL0NumSubLevels 为例）：
```
curIdx = 0
lastRotate = 10:00:00
windows = [&0, nil, nil, nil, nil]
```

#### T1: 10:00:15 - 第一次 UpdateIOThreshold() 调用

```go
ioThreshold := &admissionpb.IOThreshold{
    L0NumSubLevels: 5,
    L0NumFiles:     200,
    L0Size:         100 * 1024 * 1024,
}
s.UpdateIOThreshold(ioThreshold)
```

**Swag 内部操作**（maxL0NumSubLevels）：

```go
// 1. Record(10:00:15, 5)
maybeRotate(10:00:15)
  → sinceLastRotate = 15s < 1m，跳过轮转

*windows[0] = max(0, 5) = 5

// 状态更新
windows = [&5, nil, nil, nil, nil]

// 2. Query(10:00:15)
Windows(10:00:15) → [5]  // 只有一个窗口有数据
accumulator = max(0, 5) = 5
duration = 15s
→ 返回 (5, 15s)
```

**gossip 传播**：
```go
ioThresholdMax.L0NumSubLevels = 5  // 当前最大值
```

#### T2: 10:01:05 - 第二次调用（需要轮转）

```go
ioThreshold := &admissionpb.IOThreshold{
    L0NumSubLevels: 8,
    L0NumFiles:     350,
    L0Size:         180 * 1024 * 1024,
}
s.UpdateIOThreshold(ioThreshold)
```

**Swag 内部操作**：

```go
// 1. Record(10:01:05, 8)
maybeRotate(10:01:05)
  → sinceLastRotate = 65s
  → shift = int(65s / 1m) = 1
  → curIdx = (0 + 1) % 5 = 1
  → lastRotate = 10:01:00
  → windows[1] = &0

*windows[1] = max(0, 8) = 8

// 状态更新
windows = [&5, &8, nil, nil, nil]
           ↑    ↑ curIdx
         10:00 10:01

// 2. Query(10:01:05)
Windows(10:01:05) → [8, 5]  // 从新到旧
accumulator = max(0, 8) = 8
accumulator = max(8, 5) = 8
duration = 5s (当前窗口) + 1m (前一窗口) = 1m5s
→ 返回 (8, 1m5s)
```

#### T3: 10:04:00 - 达到峰值（5 次调用后）

**当前 Swag 状态**（经过多次 Record）：

```
curIdx = 4
lastRotate = 10:04:00
windows = [&5, &8, &15, &18, &19]
          10:00 10:01 10:02 10:03 10:04
                                   ↑ curIdx
```

**Query() 结果**：
```go
Windows(10:04:00) → [19, 18, 15, 8, 5]
accumulator = max(0, 19, 18, 15, 8, 5) = 19
duration = 0s (当前窗口刚轮转) + 4×1m = 4m
→ 返回 (19, 4m)
```

**gossip 传播**：
```go
ioThresholdMax.L0NumSubLevels = 19  // 过去 4 分钟的最大值
```

#### T4: 10:05:00 - Compaction 完成，L0 骤降

```go
ioThreshold := &admissionpb.IOThreshold{
    L0NumSubLevels: 6,   // 从 19 降至 6
    L0NumFiles:     220, // 从 980 降至 220
    L0Size:         115 * 1024 * 1024,
}
s.UpdateIOThreshold(ioThreshold)
```

**Swag 内部操作**：

```go
// 1. Record(10:05:00, 6)
maybeRotate(10:05:00)
  → sinceLastRotate = 60s
  → shift = 1
  → curIdx = (4 + 1) % 5 = 0
  → lastRotate = 10:05:00
  → windows[0] = &0  // 覆盖旧的 &5

*windows[0] = max(0, 6) = 6

// 状态更新
windows = [&6, &8, &15, &18, &19]
           ↑ curIdx
         10:05 10:01 10:02 10:03 10:04

// 2. Query(10:05:00)
Windows(10:05:00) → [6, 19, 18, 15, 8]
                    新  ↑ 历史峰值仍在窗口内
accumulator = max(0, 6, 19, 18, 15, 8) = 19  // 最大值未变！
duration = 0s + 4×1m = 4m
→ 返回 (19, 4m)
```

**关键观察**：
- ✅ **瞬时值**：6（已降低）
- ⚠️ **Swag 最大值**：19（未变，因为 10:04 的窗口仍在范围内）
- 📢 **gossip 传播**：`IOThresholdMax.L0NumSubLevels = 19`（保持不变）

**Allocator 决策**：
- 仍然认为该 Store **过载**（Score = 19/20 = 0.95）
- **不会**将 lease 迁回该 Store

#### T5: 10:06:00 - 持续低负载

```go
ioThreshold := &admissionpb.IOThreshold{
    L0NumSubLevels: 4,
    L0NumFiles:     180,
    L0Size:         95 * 1024 * 1024,
}
s.UpdateIOThreshold(ioThreshold)
```

**Swag 状态**：

```
curIdx = 1
lastRotate = 10:06:00
windows = [&6, &4, &15, &18, &19]
         10:05 10:06 10:02 10:03 10:04
                ↑ curIdx

Query() → [4, 6, 19, 18, 15]
          最大值仍为 19（10:04 的窗口仍在）
→ 返回 (19, 4m)
```

#### T6: 10:09:00 - 历史峰值窗口过期

**经过 3 次 Record（10:07, 10:08, 10:09）**：

```
curIdx = 4
lastRotate = 10:09:00
windows = [&6, &4, &3, &3, &3]
         10:05 10:06 10:07 10:08 10:09
                                  ↑ curIdx

Query() → [3, 3, 3, 6, 4]
          最大值 = 6（10:04 的窗口已被 10:09 覆盖）
→ 返回 (6, 4m)
```

**gossip 传播**：
```go
ioThresholdMax.L0NumSubLevels = 6  // 降低！
```

**Allocator 决策**：
- Score = 6/20 = 0.3（低于阈值 0.8）
- **允许**将 lease 迁回该 Store

### 5.3 指标变化时间线

| 时刻 | 瞬时 L0SubLevels | Swag Query() | IOThresholdMax | Allocator 决策 |
|------|-----------------|--------------|---------------|---------------|
| 10:00:00 | 5 | 5 | 5 | 正常 |
| 10:01:00 | 8 | 8 | 8 | 正常 |
| 10:02:00 | 15 | 15 | 15 | 警告 |
| 10:03:00 | 18 | 18 | 18 | 接近过载 |
| 10:04:00 | 19 | 19 | 19 | **过载，开始迁移 lease** |
| 10:05:00 | 6 (骤降) | 19 (保持) | 19 | **保持迁移决策** |
| 10:06:00 | 4 | 19 (保持) | 19 | **保持迁移决策** |
| 10:07:00 | 3 | 19 (保持) | 19 | **保持迁移决策** |
| 10:08:00 | 3 | 19 (保持) | 19 | **保持迁移决策** |
| 10:09:00 | 3 | 6 (降低) | 6 | **允许迁回** |

**累计影响**：
- **保护期**：从 10:04（过载）到 10:09（恢复）= **5 分钟**
- **稳定性**：即使 10:05 瞬时值降至 6，系统仍保护该 Store 5 分钟
- **平滑性**：避免在 10:05-10:09 期间因瞬时恢复而频繁调整决策

---

## VI. 设计取舍与权衡

### 6.1 固定窗口 vs 指数加权移动平均（EWMA）

**当前设计**：固定大小滑动窗口（5 个 1 分钟窗口）

**对比方案**：EWMA（如 kvSlotAdjuster 使用的）

```go
type EWMA struct {
    value float64
    alpha float64  // 平滑系数（如 0.2）
}

func (e *EWMA) Update(newValue float64) {
    e.value = e.alpha*newValue + (1-e.alpha)*e.value
}
```

#### 优缺点对比

| 维度 | Swag (固定窗口) | EWMA |
|------|----------------|------|
| **内存** | 72 字节（5 窗口） | 16 字节（单个值） |
| **历史保留** | ✅ 精确保留 5 分钟历史 | ❌ 历史衰减，无法追溯 |
| **最大值语义** | ✅ 准确返回历史最大值 | ❌ 只能近似（需复杂调参） |
| **配置复杂度** | 简单（size, interval） | 复杂（alpha 选择影响响应速度） |
| **适用场景** | **峰值追踪**（如 IO 过载） | **趋势追踪**（如 CPU 负载） |

**为何不用 EWMA 追踪 IO 最大值**：
- **EWMA 的平滑特性**：`max` 操作需要**精确**的历史最大值，而 EWMA 会让峰值"衰减"
  ```
  假设 alpha=0.2, 历史峰值=1000, 当前值=200
  EWMA = 0.2×200 + 0.8×1000 = 840（错误！实际最大值应为 1000）
  ```
- **无法保证单调性**：EWMA 可能在没有新峰值时下降，但 `max` 语义要求"5 分钟内的最大值"

**EWMA 适合的场景**（kvSlotAdjuster）：
- **CPU 负载**是**连续信号**，需要平滑短期波动
- 不需要精确的峰值，只需"当前趋势"

### 6.2 环形缓冲区 vs 时间戳数组

**当前设计**：环形缓冲区（固定大小，循环覆盖）

**对比方案**：时间戳数组（动态过期）

```go
type TimestampArray struct {
    samples []struct {
        timestamp time.Time
        value     float64
    }
}

func (ta *TimestampArray) Record(now time.Time, val float64) {
    ta.samples = append(ta.samples, struct{...}{now, val})
    // 移除 5 分钟前的样本
    cutoff := now.Add(-5 * time.Minute)
    for len(ta.samples) > 0 && ta.samples[0].timestamp.Before(cutoff) {
        ta.samples = ta.samples[1:]
    }
}
```

#### 优缺点对比

| 维度 | Swag (环形缓冲) | 时间戳数组 |
|------|----------------|-----------|
| **内存** | 固定 72 字节 | 动态（采样频率 × 窗口大小） |
| **查询复杂度** | O(k) k=5 | O(n) n=采样数（可能数百） |
| **内存分配** | 无（预分配） | 频繁（append 扩容） |
| **精度** | 粒度=interval（1 分钟） | 粒度=采样间隔（15 秒） |
| **GC 压力** | 低（固定大小） | 高（动态切片） |

**精度差异示例**：

**场景**：10:00:45 出现短暂尖峰（L0=1000），10:01:00 降至 200

| 方案 | 捕获尖峰？ | Query() 结果 | 说明 |
|------|----------|-------------|------|
| **Swag** | ✅ 是 | 1000 | 10:00:45 的值聚合到 10:00-10:01 窗口 |
| **时间戳数组** | ✅ 是 | 1000 | 精确保留 10:00:45 的样本 |

**如果采样间隔更长（如 1 分钟）**：

| 方案 | 捕获尖峰？ | 说明 |
|------|----------|------|
| **Swag** | ✅ 可能 | 如果 10:00:45 的 Record() 在 10:01:00 窗口轮转前调用 |
| **时间戳数组** | ❌ 否 | 如果 10:00 和 10:01 的采样都错过了尖峰 |

**实际选择的理由**：
- **Swag 的粒度（1 分钟）足够**：IO 过载通常持续数分钟，15 秒采样频率下不会错过
- **避免内存膨胀**：如果未来采样频率提高到 1 秒，时间戳数组会膨胀到 300 个样本 × 16 字节 = 4.8 KB
- **简单性**：无需管理过期逻辑，无需担心切片扩容

### 6.3 聚合函数的可扩展性

**当前支持的操作**：`max`（通过 `NewMaxSwag`）

**理论上可支持的操作**（需满足结合律）：

#### ✅ 支持的操作

| 操作 | binOp 定义 | 用途 |
|------|-----------|------|
| **max** | `max(a, b)` | 峰值追踪（当前用途） |
| **min** | `min(a, b)` | 最低值追踪 |
| **sum** | `a + b` | 累计值（如总请求数） |
| **count** | `1 + 1` | 计数（需特殊处理初始值） |
| **boolean OR** | `a \|\| b` | 任一窗口为 true？ |
| **boolean AND** | `a && b` | 所有窗口为 true？ |

#### ❌ 不支持的操作

| 操作 | 为何不支持 | 解决方案 |
|------|----------|---------|
| **avg (平均值)** | 不满足结合律：`avg(avg(a,b), c) != avg(a,b,c)` | 需分别追踪 sum 和 count |
| **median (中位数)** | 需保留所有样本并排序 | 使用时间戳数组 + 排序 |
| **percentile (百分位)** | 同 median | 需特殊数据结构（如 t-digest） |

**扩展示例**：追踪过去 5 分钟的**总请求数**

```go
s.requestCountSwag = slidingwindow.NewSwag(
    now,
    time.Minute,
    5,
    func(acc, val float64) float64 {
        return acc + val  // sum 操作
    },
)

// 每分钟记录该分钟的请求数
s.requestCountSwag.Record(now, float64(requestsThisMinute))

// 查询过去 5 分钟的总请求数
totalRequests, _ := s.requestCountSwag.Query(now)
```

### 6.4 分桶粒度的权衡

**当前配置**：1 分钟/窗口

#### 如果粒度更细（如 10 秒/窗口，需 30 个窗口覆盖 5 分钟）

| 优点 | 缺点 |
|------|------|
| 更精确的时间对齐（误差 ±10s vs ±60s） | 内存增加 6 倍（30×8字节 vs 5×8字节） |
| 更平滑的过期（每 10 秒轮转一次） | Query() 复杂度增加 6 倍（O(30) vs O(5)） |

#### 如果粒度更粗（如 5 分钟/窗口，仅需 1 个窗口）

| 优点 | 缺点 |
|------|------|
| 内存最小（1×8字节） | 无法追踪 5 分钟内的变化趋势 |
| Query() 最快（O(1)） | 时间精度差（误差 ±5分钟） |

**实际选择（1 分钟）的理由**：
- **平衡精度与开销**：1 分钟误差对 5 分钟窗口来说是 20%，可接受
- **与其他系统对齐**：Prometheus 默认采样间隔为 15 秒，1 分钟窗口能容纳 4 个样本
- **计算成本**：O(5) 的 Query() 对 CPU 的影响可忽略（<1μs）

---

## VII. 总结与核心思想

### 7.1 Swag 的本质

**核心定位**：**基于时间分桶的滑动窗口聚合器**，用于追踪**过去 N 分钟内的聚合值**（如最大值、总和），提供**平滑且保守**的决策依据。

**核心公式**：
```
Swag = {
  窗口数组：[w0, w1, ..., w_{k-1}]  (环形缓冲区)
  当前索引：curIdx ∈ [0, k-1]
  轮转时间：lastRotate (对齐到 interval 的倍数)
  聚合函数：binOp (必须满足结合律)
}

Record(now, val):
  if now - lastRotate >= interval:
    轮转窗口（可能多次）
  windows[curIdx] ← binOp(windows[curIdx], val)

Query(now):
  轮转窗口（如果需要）
  accumulator ← 0
  for w in [windows[curIdx], ..., windows[curIdx-k+1]]:  (从新到旧)
    if w == nil: break
    accumulator ← binOp(accumulator, w)
  return accumulator
```

### 7.2 三层设计原则

#### 7.2.1 **惰性计算（Lazy Evaluation）**

```go
// 无需后台定时器，轮转在访问时触发
func (s *Swag) maybeRotate(now time.Time) {
    if now.Sub(s.lastRotate) >= s.rotateInterval {
        // 执行轮转
    }
}
```

**优点**：
- 无需管理 goroutine 生命周期
- 无锁竞争（Record 和定时器不冲突）
- 时钟精度需求低（15 秒延迟可接受）

#### 7.2.2 **固定内存（Zero Allocation）**

```go
// 构造时预分配所有内存
windows := make([]*float64, size)
var first float64
windows[0] = &first
```

**优点**：
- 无 GC 压力（运行时不分配新内存）
- 内存可预测（size=5 → 72 字节）
- 适合高频调用场景（每 15 秒一次）

#### 7.2.3 **结合律约束（Associativity Requirement）**

```go
// binOp 必须满足：binOp(binOp(a,b), c) == binOp(a, binOp(b,c))
binOp := func(acc, val float64) float64 {
    return max(acc, val)  // ✅ max 满足结合律
}
```

**优点**：
- 简化实现（无需特殊的 lift/lower 函数）
- 查询高效（O(k) 累积，无需特殊合并逻辑）

**限制**：
- 不支持平均值、中位数等非结合律操作

### 7.3 应用场景指南

**何时使用 Swag**：
- ✅ 需要追踪**历史峰值**（如 IO 最大值、最高 QPS）
- ✅ 决策需要**保守且稳定**（避免基于瞬时值振荡）
- ✅ 内存预算有限（每个 Swag 仅 ~100 字节）
- ✅ 聚合操作满足**结合律**（max, min, sum, OR, AND）

**何时不用 Swag**：
- ❌ 需要**精确的时间戳**（如"过去 5 分钟内每秒的值"）
- ❌ 需要**非结合律聚合**（如平均值、百分位数）
- ❌ 需要**高时间精度**（Swag 的粒度为 interval，如 1 分钟）
- ❌ 窗口大小需要**动态调整**（Swag 窗口固定）

### 7.4 心智模型

**如果只记住一件事**：

> `Swag` 是一个**环形缓冲区**，把时间分成固定长度的"桶"（如 1 分钟），每个桶记录该时间段内的聚合值（如最大值）。查询时，从**最新的桶**开始向后累积，直到覆盖整个窗口（如 5 个桶 = 5 分钟）。这样设计让系统"记住"过去的峰值，避免因短暂恢复而放松保护。

**类比**：
- **体温监测**：医生不会因为你此刻体温正常（36.5°C），就认为你没发烧——他会查看过去 24 小时的最高体温
- **Swag** 就像**体温图表**，记录每小时的最高体温，Query() 返回 24 小时内的峰值
- **环形缓冲区**就像**循环日历**，今天的数据覆盖 24 小时前的数据

### 7.5 简化伪代码

```go
// 构造 Swag
swag := Swag{
    windows: [nil, nil, nil, nil, nil],  // 5 个空桶
    curIdx: 0,
    lastRotate: now,
    interval: 1m,
}
swag.windows[0] = 0.0  // 初始化第一个桶

// 记录值（每 15 秒调用）
func Record(now, val):
    // 检查是否需要轮转（如果过了 1 分钟）
    while now - lastRotate >= 1m:
        curIdx = (curIdx + 1) % 5  // 循环移动
        windows[curIdx] = 0.0      // 清空新桶
        lastRotate += 1m

    // 聚合到当前桶
    windows[curIdx] = max(windows[curIdx], val)

// 查询最大值（按需调用）
func Query(now):
    Record(now, 0)  // 先轮转窗口（如果需要）

    maxVal = 0
    for i in [0, 1, 2, 3, 4]:
        idx = (curIdx - i + 5) % 5  // 从新到旧
        if windows[idx] == nil:
            break
        maxVal = max(maxVal, windows[idx])

    return maxVal
```

---

## 附录：代码位置索引

| 组件 | 代码位置 |
|------|---------|
| **Swag 定义** | [pkg/util/slidingwindow/sliding_window.go:33-39](pkg/util/slidingwindow/sliding_window.go#L33-L39) |
| **NewSwag()** | [pkg/util/slidingwindow/sliding_window.go:42-55](pkg/util/slidingwindow/sliding_window.go#L42-L55) |
| **NewMaxSwag()** | [pkg/util/slidingwindow/helpers.go:11-23](pkg/util/slidingwindow/helpers.go#L11-L23) |
| **Record()** | [pkg/util/slidingwindow/sliding_window.go:59-62](pkg/util/slidingwindow/sliding_window.go#L59-L62) |
| **Query()** | [pkg/util/slidingwindow/sliding_window.go:86-101](pkg/util/slidingwindow/sliding_window.go#L86-L101) |
| **maybeRotate()** | [pkg/util/slidingwindow/sliding_window.go:68-82](pkg/util/slidingwindow/sliding_window.go#L68-L82) |
| **Windows()** | [pkg/util/slidingwindow/sliding_window.go:106-119](pkg/util/slidingwindow/sliding_window.go#L106-L119) |
| **Store 初始化 Swag** | [pkg/kv/kvserver/store.go:1501-1503](pkg/kv/kvserver/store.go#L1501-L1503) |
| **UpdateIOThreshold() 调用 Record/Query** | [pkg/kv/kvserver/store.go:2824-2829](pkg/kv/kvserver/store.go#L2824-L2829) |
| **IOLoadListener 计算 IOThreshold** | [pkg/util/admission/io_load_listener.go:539-592](pkg/util/admission/io_load_listener.go#L539-L592) |

---

**下一章预告**：
[第二十八章] 将分析 **StoreGrantCoordinators 的 Pebble 指标采集循环**，深入探讨：
- 如何每 15 秒从所有 Store 采集 Pebble 指标
- IOLoadListener 如何基于 LSM 状态计算准入控制 tokens
- 为何 Swag 的 5 分钟窗口与 15 秒采样周期配合使用
- StoreGrantCoordinator 与 Node.UpdateIOThreshold() 的完整交互流程
