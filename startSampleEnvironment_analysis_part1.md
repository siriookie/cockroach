# `startSampleEnvironment` 深度源码分析（上）

## 目录

1. [系统定位与架构概览](#1-系统定位与架构概览)
2. [第一轮 BFS：控制流与组件协作](#2-第一轮-bfs控制流与组件协作)
3. [DFS 深入：核心基础设施](#3-dfs-深入核心基础设施)
   - 3.1 profiler 基类与 High-Water Mark 机制
   - 3.2 profileStore 与 Ramp-Up GC
   - 3.3 RuntimeStatSampler.SampleEnvironment
   - 3.4 Jemalloc Purge 机制
   - 3.5 GoroutineDumper

---

## 1. 系统定位与架构概览

`startSampleEnvironment` 是 CockroachDB 节点级的**运行时健康监控主循环**。它不是请求路径上的组件，而是一个 **定时驱动 (timer-driven)** 的后台观测系统，负责：

| 职责 | 子系统 | 触发方式 |
|------|--------|----------|
| 运行时指标采样 | `RuntimeStatSampler` | 每个 tick 无条件执行 |
| 内存碎片治理 | `CGoMemMaybePurge` (jemalloc) | 每个 tick 有条件执行 |
| 协程泄漏诊断 | `GoroutineDumper` | 启发式 (heuristic) |
| Go 堆内存快照 | `HeapProfiler` | High-Water Mark |
| BytesMonitor 快照 | `MemoryMonitoringProfiler` | High-Water Mark |
| 非 Go 内存快照 | `NonGoAllocProfiler` | High-Water Mark |
| RSS 内存统计 | `StatsProfiler` | High-Water Mark |
| 活跃 SQL 导出 | `ActiveQueryProfiler` | OOM 概率预测 |
| CPU 使用率快照 | `CPUProfiler` | High-Water Mark + floor |
| 执行轨迹记录 | `SimpleFlightRecorder` | 独立定时循环 |

**设计哲学**：这是一个 **"被动式黑匣子"** 系统。它不干预业务逻辑，只是在异常发生时自动留存现场快照 (profiles)，供事后分析。这种设计在 Google 内部被称为 "always-on profiling"，已成为大规模分布式系统的工程事实标准。

---

## 2. 第一轮 BFS：控制流与组件协作

### 2.1 启动时序（初始化阶段）

```
startSampleEnvironment(ctx, srvCfg, pebbleCacheSize, stopper, runtimeSampler, sessionRegistry, rootMemMonitor)
│
├── [Step 1] 确定采样间隔 metricsSampleInterval (默认 10s)
│   └── 如果 TestingKnobs 有覆盖，使用测试值
│
├── [Step 2] 构造 sampleEnvironmentCfg
│   └── cgoMemTarget = max(pebbleCacheSize, 128MiB)
│
├── [Step 3] 初始化 GoroutineDumper
│   ├── os.MkdirAll(goroutineDumpDirName)
│   └── goroutinedumper.NewGoroutineDumper(ctx, dir, st)
│
├── [Step 4] 初始化 Profiler 集群 (6 个)
│   ├── os.MkdirAll(heapProfileDirName)
│   ├── profiler.NewHeapProfiler(ctx, dir, st)
│   ├── profiler.NewMemoryMonitoringProfiler(ctx, dir, st)
│   ├── profiler.NewNonGoAllocProfiler(ctx, dir, st)
│   ├── profiler.NewStatsProfiler(ctx, dir, st)
│   ├── profiler.NewActiveQueryProfiler(ctx, dir, st)  // 读取 cgroup 内存限制
│   └── profiler.NewCPUProfiler(ctx, cpuProfileDir, st)
│
├── [Step 5] 初始化 FlightRecorder
│   ├── goexectrace.NewFlightRecorder(st, 10s, dir)
│   └── simpleFlightRecorder.Start(ctx, stopper)  // 启动独立 goroutine
│
└── [Step 6] 启动主采样循环 (stopper.RunAsyncTaskEx)
    └── goroutine: "mem-logger"
```

**关键观察**：

- **FlightRecorder 拥有独立循环**：它通过 `Start()` 自己启动一个 goroutine，不依赖主循环的 tick。主循环中 6 个 profiler + 1 个 dumper 共享同一个 timer。
- **初始化失败策略**：GoroutineDumper / Profiler 初始化失败时使用 `errors.Wrap` 返回 fatal error 终止节点启动；而 QueryProfiler / CPUProfiler / FlightRecorder 只 log warning，降级运行。这反映了 **核心 vs. 辅助** 的区分。
- **目录创建语义**：`os.MkdirAll` 保证幂等，支持节点重启后复用已有目录。

### 2.2 主循环执行路径（运行阶段）

每个 tick (默认 10s) 执行的操作，严格顺序如下：

```
timer fires (每 10s)
│
├── [1] status.GetCGoMemStats(ctx)
│   └── 调用 jemalloc 获取 (CGoAllocatedBytes, CGoTotalBytes)
│
├── [2] cfg.runtime.SampleEnvironment(ctx, cgoStats)
│   └── 采样 ~80 项指标: Go 堆/栈/GC、进程 CPU、系统 CPU、RSS、FD、磁盘 IO、网络
│
├── [3] CGoMemMaybePurge (有条件)
│   ├── 条件: overhead > 0 && period > 0
│   └── 判断 jemalloc overhead 是否超阈值，超则 purge dirty pages
│
├── [4] goroutineDumper.MaybeDump (有条件)
│   └── 如果协程数 > 阈值 且 >= 2 × 上次 dump 时的数量 → 写 gzip 快照
│
├── [5] heapProfiler.MaybeTakeProfile(GoAllocBytes)
│   └── 如果 Go 堆分配 > high-water mark → 写 .pprof
│
├── [6] memMonitoringProfiler.MaybeTakeMemoryMonitoringDump(GoAllocBytes, rootMemMonitor)
│   └── 如果 Go 堆分配 > high-water mark → 遍历 BytesMonitor 树写 .txt
│
├── [7] nonGoAllocProfiler.MaybeTakeProfile(CgoTotalBytes)
│   └── 如果 CGo 内存 > high-water mark → 调用 jemalloc heap dump
│
├── [8] statsProfiler.MaybeTakeProfile(RSSBytes, cgoStats)
│   └── 如果 RSS > high-water mark → 写 runtime.MemStats + CGoMemStats JSON
│
├── [9] queryProfiler.MaybeDumpQueries(sessionRegistry)
│   └── 如果内存增长速率预测 OOM → 写活跃查询 CSV
│
└── [10] cpuProfiler.MaybeTakeProfile(CPUCombinedPercentNorm * 100)
    └── 如果 CPU% > high-water mark (floor=65%) → 启动 10s pprof CPU profile
```

**与其他模块的交互方式**：

| 交互对象 | 交互方式 | 方向 |
|----------|----------|------|
| `status.RuntimeStatSampler` | 直接方法调用，共享 metric gauge | 写入 |
| `stop.Stopper` | `ShouldQuiesce()` channel | 监听 |
| `cluster.Settings` (SV) | 每 tick 读 setting 值 | 只读 |
| `sql.SessionRegistry` | 快照读取所有 session | 只读 |
| `mon.BytesMonitor` | 树遍历读取内存使用 | 只读 |
| jemalloc | CGo FFI 调用 | 读+写(purge) |
| 磁盘文件系统 | 写 profile 文件 + GC 旧文件 | 写 |

---

## 3. DFS 深入：核心基础设施

### 3.1 `profiler` 基类与 High-Water Mark 机制

**文件**：`pkg/server/profiler/profiler_common.go`

这是所有内存/CPU profiler 共享的核心决策引擎。其设计目标是：**只在"创新高"时抓快照，避免重复采样浪费磁盘**。

#### 数据结构

```go
type profiler struct {
    store              *profileStore
    highWaterMarkFloor func() int64     // 动态地板值
    resetInterval      func() time.Duration  // 水位重置周期
    knobs              testingKnobs

    lastProfileTime    time.Time  // 上次 profile 时间戳
    highWaterMark      int64      // 当前水位线
}
```

**核心不变量 (Invariant)**：
- `highWaterMark >= highWaterMarkFloor()` 在 reset 之后恒成立
- Profile 仅在 `thresholdValue > highWaterMark` 时被触发
- 每次成功触发后 `highWaterMark` 被提升到 `thresholdValue`

#### 关键函数：`maybeTakeProfile`

```go
func (o *profiler) maybeTakeProfile(
    ctx context.Context,
    thresholdValue int64,
    takeProfileFn func(ctx context.Context, path string, args ...interface{}) bool,
    args ...interface{},
) {
    if o.resetInterval() == 0 {
        return  // 禁用
    }

    now := o.now()
    // 重置逻辑：地板值上调 OR 超时
    if floor := o.highWaterMarkFloor(); o.highWaterMark < floor ||
       now.Sub(o.lastProfileTime) >= o.resetInterval() {
        o.highWaterMark = floor
    }

    takeProfile := thresholdValue > o.highWaterMark
    if !takeProfile {
        return
    }

    o.highWaterMark = thresholdValue
    o.lastProfileTime = now

    success := takeProfileFn(ctx, o.store.makeNewFileName(now, thresholdValue), args...)
    if success {
        o.store.gcProfiles(ctx, now)
    }
}
```

**为什么只能这么做**：

1. **High-Water Mark 模式** 保证在内存单调递增（典型的 OOM 前兆）过程中，每次"创新高"都会抓一个快照。这比固定间隔采样更有诊断价值——最终你得到的是一个递增序列的快照，可以观察到内存是从哪一步开始膨胀的。

2. **`resetInterval` 的存在**是为了防止 high-water mark 永远递增导致永远不再抓 profile 的问题。例如 CPU profiler 的 `resetInterval` 是 20 分钟，意味着即使 CPU 一直很高，最多 20 分钟后会重置水位，重新开始一轮观察。

3. **`highWaterMarkFloor` 是动态的**（通过 `func() int64` 而非固定值），因为它绑定 cluster setting。对于 CPUProfiler，floor 是 `cpuUsageCombined.Get(&st.SV)` (默认 65%)；对于 HeapProfiler，floor 是 `zeroFloor` (0)。这允许不同 profiler 有不同的"起始关注阈值"。

4. **只在 `success` 时 GC**：避免在 profile 写入失败时删除旧的有价值 profile。这是一个防御性设计——如果磁盘满导致写入失败，保留旧 profile 比删掉旧的却没有新的要好。

### 3.2 `profileStore` 与 Ramp-Up GC 策略

**文件**：`pkg/server/profiler/profilestore.go`

`profileStore` 管理磁盘上的 profile 文件。它实现了一个**基于 ramp-up 检测的 GC 策略**。

#### 文件命名格式

```go
func (s *profileStore) makeNewFileName(timestamp time.Time, curHeap int64) string {
    fileName := fmt.Sprintf("%s.%s.%d%s",
        s.prefix, timestamp.Format(timestampFormat), curHeap, s.suffix)
    return s.GetFullPath(fileName)
}
// 示例: memprof.2025-05-20T14_30_05.000.1073741824.pprof
//       ^prefix  ^timestamp               ^heap(bytes) ^suffix
```

将 heap usage 编码进文件名，使得 GC 时可以仅通过 `parseFileName` 还原出时间和内存量，**无需读取文件内容**。

#### Ramp-Up GC 算法：`cleanupLastRampup`

```go
func (s *profileStore) cleanupLastRampup(
    ctx context.Context, files []os.DirEntry, maxP int64, fn func(string) error,
) (preserved map[int]bool) {
    preserved = make(map[int]bool)
    curMaxHeap := uint64(math.MaxUint64)
    numFiles := int64(0)
    for i := len(files) - 1; i >= 0; i-- {  // 从最新文件往前扫描
        ok, _, curHeap := s.parseFileName(ctx, files[i].Name())
        if !ok { continue }

        if curHeap > curMaxHeap {
            break  // 检测到 ramp-up 边界：当前文件的 heap > 后面的 → 这是上一轮的
        }
        curMaxHeap = curHeap
        numFiles++
        if numFiles > maxP {
            fn(files[i].Name())  // 超过 maxP，删除最老的
        } else {
            preserved[i] = true
        }
    }
    return preserved
}
```

**算法逻辑**：

- **Ramp-up 定义**：一个 heap usage 单调递增的连续文件序列。
- 从最新文件倒序扫描，遇到 `curHeap > curMaxHeap` 时说明进入了上一轮 ramp-up，停止。
- 在当前 ramp-up 内，最多保留 `maxP`（默认 5）个文件，多余的删除最老的。
- 上一轮 ramp-up 的文件交由外层 `DumpStore` 按总大小 GC。

**为什么这个算法是合理的**：在 OOM 诊断场景中，最有价值的信息是**最近一轮内存持续上升到崩溃前**的 profile 序列。保留一个完整的 ramp-up 序列远比散乱的历史快照更有诊断意义。

### 3.3 `RuntimeStatSampler.SampleEnvironment`

**文件**：`pkg/server/status/runtime.go`

这是主循环中**每个 tick 无条件执行**的核心函数，负责将系统状态映射到 CockroachDB 的 metric 体系中。

#### 采样内容分类

| 类别 | 采样来源 | 代表指标 |
|------|----------|----------|
| Go 运行时 | `runtime/metrics` API | `GoAllocBytes`, `Goroutines`, `GcPauseNS` |
| 进程 CPU | `GetProcCPUTime()` (procfs) | `CPUUserPercent`, `CPUSysPercent` |
| 系统 CPU | `cpu.Times()` (gopsutil) | `HostCPUCombinedPercentNorm` |
| 进程内存 | `gosigar.ProcMem.Get(pid)` | `RSSBytes` |
| CGo 内存 | 传入参数 `CGoMemStats` | `CgoAllocBytes`, `CgoTotalBytes` |
| 文件描述符 | `gosigar.ProcFDUsage.Get(pid)` | `FDOpen`, `FDSoftLimit` |
| 磁盘 IO | `getSummedDiskCounters()` | `HostDiskReadBytes`, `IopsInProgress` |
| 网络 | `getSummedNetStats()` | `HostNetRecvBytes`, `HostNetSendTCPRetransSegs` |

#### CPU 利用率计算

这里有一个典型的**增量比率计算模式**：

```go
now := rsr.clock.Now().UnixNano()
dur := float64(now - rsr.last.now)
procUtime := userTimeMillis * 1e6  // ms → ns
procStime := sysTimeMillis * 1e6

var procUrate, procSrate float64
if rsr.last.now != 0 {  // 第一次迭代无法计算速率
    procUrate = float64(procUtime-rsr.last.procUtime) / dur
    procSrate = float64(procStime-rsr.last.procStime) / dur
}

combinedNormalizedProcPerc := (procSrate + procUrate) / cpuCapacity
```

- `procUtime` 是从进程启动至今累计的用户态 CPU 时间（ns）
- `rsr.last.procUtime` 是上次采样时的累计值
- 差值 / 时间间隔 = 这段时间的 CPU 使用率
- 除以 `cpuCapacity`（= 逻辑 CPU 数，可受 cgroup 限制）= **归一化 CPU 使用率**

**`CPUCombinedPercentNorm` 是 CPUProfiler 的输入源**：主循环传入 `int64(cfg.runtime.CPUCombinedPercentNorm.Value() * 100)` 作为 threshold。

#### Runnable Goroutines 采样

```go
runnableSum := goschedstats.CumulativeNormalizedRunnableGoroutines()
runnableAvg := (runnableSum - rsr.last.runnableSum) * 1e9 / dur
```

这是一个**积分-微分模式**：`CumulativeNormalizedRunnableGoroutines()` 返回的是累计值（对时间的积分），通过两次采样求差再除以时间间隔，得到**平均每 CPU 的 runnable goroutine 数**。这比瞬时值更稳定，避免了调度器毛刺的干扰。

### 3.4 Jemalloc Purge 机制

**文件**：`pkg/server/env_sampler.go` (调用方) + `pkg/server/status/runtime.go` (实现)

```go
// env_sampler.go 主循环中:
if overhead, period := jemallocPurgeOverhead.Get(&cfg.st.SV),
   jemallocPurgePeriod.Get(&cfg.st.SV); overhead > 0 && period > 0 {
    status.CGoMemMaybePurge(ctx,
        cgoStats.CGoAllocatedBytes, cgoStats.CGoTotalBytes,
        cfg.cgoMemTarget, int(overhead), period)
}
```

参数语义：
- `cgoAllocMem`：jemalloc 实际分配给应用的字节数
- `cgoTotalMem`：jemalloc 向 OS 申请的总字节数（包含 metadata、碎片、dirty pages）
- `cgoTargetMem`：`max(pebbleCacheSize, 128MiB)` — 最低内存目标
- `overheadPercent`：默认 20%
- `minPeriod`：默认 2 分钟

**Purge 决策逻辑**：
- overhead = `(cgoTotalMem - max(cgoAllocMem, cgoTargetMem)) / max(cgoAllocMem, cgoTargetMem) * 100`
- 如果 overhead > 20% 且距上次 purge >= 2 min → 执行 `mallctl("arena..purge")`

**为什么需要 `cgoTargetMem`**：Pebble (存储引擎) 会预分配一个 block cache。当 Pebble cache 释放部分 block 时，jemalloc 可能仍持有这些 dirty pages 未归还 OS。如果没有 `cgoTargetMem` 作为下限，overhead 计算可能在低内存使用时频繁触发 purge，造成不必要的 TLB flush 和 page fault。设定 `max(pebbleCacheSize, 128MiB)` 作为 target 确保在 cache 正常运作范围内不会过度 purge。

**实现层面**：`CGoMemMaybePurge` 是一个函数指针间接调用（`var cgoMemMaybePurge func(...)`），实际实现在平台特定文件中通过 `init()` 注入，典型的 **编译时依赖注入** 模式，用于隔离 CGo/jemalloc 链接依赖。

### 3.5 `GoroutineDumper`

**文件**：`pkg/server/goroutinedumper/goroutinedumper.go`

与 profiler 系列不同，GoroutineDumper 使用**独立的启发式 (heuristic) 系统**而非 high-water mark。

#### 数据结构

```go
type GoroutineDumper struct {
    goroutines          int64  // 当前协程数（每 tick 更新）
    goroutinesThreshold int64  // cluster setting 阈值（默认 1000）
    maxGoroutinesDumped int64  // 上次 dump 时的协程数
    heuristics          []heuristic
    currentTime         func() time.Time
    takeGoroutineDump   func(path string) error
    store               *dumpstore.DumpStore
}
```

#### 启发式触发条件

```go
var doubleSinceLastDumpHeuristic = heuristic{
    name: "double_since_last_dump",
    isTrue: func(gd *GoroutineDumper) bool {
        return gd.goroutines > gd.goroutinesThreshold &&
            gd.goroutines >= 2*gd.maxGoroutinesDumped
    },
}
```

**双重条件**：
1. `goroutines > goroutinesThreshold`（默认 1000）：过滤掉正常范围的协程数
2. `goroutines >= 2 * maxGoroutinesDumped`：**指数增长检测**。只有协程数翻倍才 dump

这个设计的含义是：
- 第一次 dump 发生在协程数首次超过 1000 时（此时 `maxGoroutinesDumped` = 0，条件 2 自动满足）
- 之后 dump 发生在 2000, 4000, 8000, 16000... （指数间隔）
- 如果 cluster setting 被修改（阈值变化），`maxGoroutinesDumped` 重置为 0，重新开始序列

#### MaybeDump 完整流程

```go
func (gd *GoroutineDumper) MaybeDump(ctx context.Context, st *cluster.Settings, goroutines int64) {
    gd.goroutines = goroutines
    if gd.goroutinesThreshold != numGoroutinesThreshold.Get(&st.SV) {
        gd.goroutinesThreshold = numGoroutinesThreshold.Get(&st.SV)
        gd.maxGoroutinesDumped = 0  // 重置序列
    }
    for _, h := range gd.heuristics {
        if h.isTrue(gd) {
            now := gd.currentTime()
            filename := fmt.Sprintf(
                "%s.%s.%s.%09d",
                goroutineDumpPrefix, now.Format(timeFormat), h.name, goroutines,
            )
            path := gd.store.GetFullPath(filename)
            if err := gd.takeGoroutineDump(path); err != nil {
                log.Dev.Warningf(ctx, "error dumping goroutines: %s", err)
                continue
            }
            gd.maxGoroutinesDumped = goroutines
            gd.gcDumps(ctx, now)
            break  // 一次调用最多产生一个 dump
        }
    }
}
```

**Dump 内容**：

```go
func takeGoroutineDump(path string) error {
    path += ".txt.gz"
    f, err := os.Create(path)
    // ...
    w := gzip.NewWriter(f)
    if _, err := w.Write(allstacks.Get()); err != nil { ... }
    // ...
}
```

`allstacks.Get()` 获取所有 goroutine 的堆栈信息（等价于 `runtime.Stack(buf, true)`），然后 gzip 压缩写入。文件名格式：

```
goroutine_dump.2025-05-20T14_30_05.000.double_since_last_dump.000008192.txt.gz
               ^timestamp              ^heuristic_name          ^goroutine_count
```

**GC 策略**：`dumpstore.DumpStore` 按 `server.goroutine_dump.total_dump_size_limit`（默认 500MiB）总大小 GC，`PreFilter` 保证始终保留最新一个 dump 文件。
