# `startSampleEnvironment` 深度源码分析（下）

## 目录

4. [DFS 深入：各子系统实现](#4-dfs-深入各子系统实现)
   - 4.1 HeapProfiler
   - 4.2 MemoryMonitoringProfiler
   - 4.3 NonGoAllocProfiler
   - 4.4 StatsProfiler
   - 4.5 ActiveQueryProfiler
   - 4.6 CPUProfiler
   - 4.7 SimpleFlightRecorder
5. [具体运行示例](#5-具体运行示例)
6. [设计模式总结](#6-设计模式总结)

---

## 4. DFS 深入：各子系统实现

### 4.1 HeapProfiler

**文件**：`pkg/server/profiler/heapprofiler.go`

HeapProfiler 是最简单的 profiler 实现，也是理解整个 profiler 体系的最佳入口。

#### 结构

```go
type HeapProfiler struct {
    profiler  // 嵌入基类
}
```

#### 初始化

```go
func NewHeapProfiler(ctx context.Context, dir string, st *cluster.Settings) (*HeapProfiler, error) {
    dumpStore := dumpstore.NewStore(dir, maxCombinedFileSize, st)  // 512MiB 上限
    hp := &HeapProfiler{
        profiler: makeProfiler(
            newProfileStore(dumpStore, heapFileNamePrefix, heapFileNameSuffix, st),
            zeroFloor,           // floor = 0，任何非零堆分配都可能触发
            envMemprofInterval,  // 默认 1h 重置水位
        ),
    }
    return hp, nil
}
```

**参数选择的工程含义**：
- `highWaterMarkFloor = zeroFloor (0)`：不设最低阈值。第一个 tick 就会抓 profile（因为 `GoAllocBytes` 几乎总是 > 0）。
- `resetInterval = COCKROACH_MEMPROF_INTERVAL (默认 1h)`：每小时重置水位，保证每小时至少抓一次 profile。
- `maxCombinedFileSize = 512MiB`：profile 文件的总磁盘预算。

#### 触发与写入

```go
func (o *HeapProfiler) MaybeTakeProfile(ctx context.Context, curHeap int64) {
    o.maybeTakeProfile(ctx, curHeap, takeHeapProfile)
}

func takeHeapProfile(ctx context.Context, path string, _ ...interface{}) (success bool) {
    f, err := os.Create(path)
    if err != nil { return false }
    defer f.Close()
    if err = pprof.WriteHeapProfile(f); err != nil { return false }
    return true
}
```

`pprof.WriteHeapProfile(f)` 是 Go 标准库函数，写出当前 heap 的完整分配采样数据。输出文件可直接用 `go tool pprof` 打开分析。

**主循环中的调用**（`env_sampler.go:238`）：

```go
heapProfiler.MaybeTakeProfile(ctx, cfg.runtime.GoAllocBytes.Value())
```

输入 `GoAllocBytes` 是 `SampleEnvironment` 刚刚采样更新的值，来自 `runtime/metrics` 的 `/memory/classes/heap/objects:bytes`。

### 4.2 MemoryMonitoringProfiler

**文件**：`pkg/server/profiler/memory_monitoring_profiler.go`

这个 profiler 是 HeapProfiler 的**伴生系统**，与 HeapProfiler 使用相同的触发指标（`GoAllocBytes`），但输出的是 CockroachDB 内部 `BytesMonitor` 树的结构化内存使用信息。

#### 设计动机

Go 的 heap profile 告诉你"哪些函数分配了内存"，但不告诉你"这些内存属于哪个逻辑组件"。CockroachDB 的 `mon.BytesMonitor` 系统追踪每个 SQL 执行器、每个事务、每个 DistSQL 流的内存配额使用。MemoryMonitoringProfiler 在内存高峰时导出这棵树，补全了 heap profile 缺失的"语义归属"信息。

#### 触发逻辑

```go
func (mmp *MemoryMonitoringProfiler) MaybeTakeMemoryMonitoringDump(
    ctx context.Context, curHeap int64, root *mon.BytesMonitor, st *cluster.Settings,
) {
    if !memoryMonitoringDumpsEnabled.Get(&st.SV) {
        return
    }
    mmp.maybeTakeProfile(ctx, curHeap, takeMemoryMonitoringDump, root)
}
```

注意 `root` 作为 `args ...interface{}` 传入 `maybeTakeProfile`，再透传给 `takeMemoryMonitoringDump`。

#### 写入逻辑

```go
func takeMemoryMonitoringDump(ctx context.Context, path string, args ...interface{}) (success bool) {
    root, ok := args[0].(*mon.BytesMonitor)
    // ...
    f, err := os.Create(path)
    defer f.Close()
    if err = root.TraverseTree(getMonitorStateCb(f)); err != nil { ... }
    return true
}

func getMonitorStateCb(f io.Writer) func(state mon.MonitorState) error {
    return func(s mon.MonitorState) error {
        if s.Stopped { return nil }  // 跳过已停止的 monitor
        if s.Used == 0 && s.ReservedUsed == 0 && s.ReservedReserved == 0 {
            return nil  // 跳过无内存使用的 monitor
        }
        info := fmt.Sprintf("%s%s %s",
            strings.Repeat(" ", 4*s.Level), s.Name, humanize.IBytes(uint64(s.Used)))
        if s.ReservedUsed != 0 || s.ReservedReserved != 0 {
            info += fmt.Sprintf(" (%s / %s)",
                humanize.IBytes(uint64(s.ReservedUsed)),
                humanize.IBytes(uint64(s.ReservedReserved)))
        }
        f.Write([]byte(info))
        f.Write([]byte{'\n'})
        return err
    }
}
```

**输出示例**：

```
root 2.5 GiB
    sql 1.8 GiB
        session-1 512 MiB (256 MiB / 1.0 GiB)
        session-2 1.2 GiB (1.0 GiB / 2.0 GiB)
    kv 700 MiB
```

缩进通过 `strings.Repeat(" ", 4*s.Level)` 表示树层级。`(Used / Reserved)` 括号内分别是 reserved pool 中已使用和已保留的字节数。

**cluster setting 控制**：`diagnostics.memory_monitoring_dumps.enabled`（默认 true）。磁盘预算 4MiB（因为纯文本文件很小）。

### 4.3 NonGoAllocProfiler

**文件**：`pkg/server/profiler/cgoprofiler.go`

监控 **Go 进程外** 的内存分配，主要是 jemalloc 管理的 C/C++ 侧内存（Pebble 的 block cache、RocksDB 遗留组件等）。

#### 触发指标

```go
// env_sampler.go:242
nonGoAllocProfiler.MaybeTakeProfile(ctx, cfg.runtime.CgoTotalBytes.Value())
```

输入是 `CgoTotalBytes` — jemalloc 向 OS 申请的总字节数，而非仅应用分配的字节数。选择 total 而非 allocated 是因为内存碎片和 dirty pages 同样是 OOM 的诱因。

#### 写入逻辑

```go
func takeJemallocProfile(ctx context.Context, path string, _ ...interface{}) (success bool) {
    if jemallocHeapDump == nil {
        return true  // jemalloc profiling 未启用，静默返回
    }
    if err := jemallocHeapDump(path); err != nil { return false }
    return true
}
```

`jemallocHeapDump` 是一个可注入的函数指针：

```go
var jemallocHeapDump func(string) error

func SetJemallocHeapDumpFn(fn func(filename string) error) {
    if jemallocHeapDump != nil {
        panic("jemallocHeapDump is already set")
    }
    jemallocHeapDump = fn
}
```

**为什么用函数指针注入而非直接调用**：jemalloc profiling 需要特殊的链接标志（`-ljemalloc -lpthread` 等），直接 import 会导致 `go test` 无法在没有 jemalloc 的环境下运行。通过 CLI 层 `SetJemallocHeapDumpFn` 注入，将链接依赖隔离到最终二进制构建阶段。

**启用条件**：需要 `MALLOC_CONF=prof:true` 环境变量，或 `/etc/malloc.conf` 符号链接指向 `prof:true`。

### 4.4 StatsProfiler

**文件**：`pkg/server/profiler/statsprofiler.go`

StatsProfiler 在 RSS 创新高时，同时导出 Go `runtime.MemStats` 和 `CGoMemStats` 的完整快照。

#### 触发指标

```go
// env_sampler.go:244
statsProfiler.MaybeTakeProfile(ctx, cfg.runtime.RSSBytes.Value(), cgoStats)
```

使用 RSS（Resident Set Size）作为 threshold，因为 RSS 是 OOM killer 实际依据的指标。

#### 写入逻辑

```go
func (o *StatsProfiler) MaybeTakeProfile(ctx context.Context, curRSS int64, cs *status.CGoMemStats) {
    o.maybeTakeProfile(ctx, curRSS,
        func(ctx context.Context, path string, _ ...interface{}) bool {
            return saveStats(ctx, path, cs)
        })
}

func saveStats(ctx context.Context, path string, cs *status.CGoMemStats) bool {
    f, err := os.Create(path)
    defer f.Close()
    ms := &runtime.MemStats{}
    runtime.ReadMemStats(ms)          // STW 操作（短暂暂停所有 goroutine）
    msJ, _ := json.MarshalIndent(ms, "", "  ")
    csJ, _ := json.MarshalIndent(cs, "", "  ")
    fmt.Fprintf(f, "Go memory stats:\n%s\n----\nNon-Go stats:\n%s\n", msJ, csJ)
    return true
}
```

**注意**：`runtime.ReadMemStats(ms)` 会触发一次 **Stop-The-World (STW)** 暂停来收集精确的内存统计。但这只在 RSS 创新高时才调用（不是每 tick），所以 STW 频率很低。

**输出格式**：JSON 格式的完整 MemStats + CGoMemStats，文件名如 `memstats.2025-05-20T14_30_05.000.4294967296.txt`。

### 4.5 ActiveQueryProfiler

**文件**：`pkg/server/profiler/activequeryprofiler.go`

这是整个 profiler 体系中**唯一不使用 high-water mark 基类触发逻辑**的 profiler。它实现了一个**基于内存增长速率的 OOM 预测模型**。

#### 数据结构

```go
type ActiveQueryProfiler struct {
    profiler                   // 嵌入但主要用 store 和 knobs，不用 maybeTakeProfile
    cgroupMemLimit int64       // cgroup 内存上限（初始化时读取，不可变）
    mu             struct {
        syncutil.Mutex
        prevMemUsage int64     // 上次采样的内存使用量
    }
}
```

#### 初始化

```go
func NewActiveQueryProfiler(ctx context.Context, dir string, st *cluster.Settings,
) (*ActiveQueryProfiler, error) {
    maxMem, warn, err := memLimitFn()  // cgroups.GetMemoryLimit()
    if err != nil {
        return nil, errors.Wrap(err, "failed to detect cgroup memory limit")
    }
    // ...
}
```

**强依赖 cgroup**：如果无法读取 cgroup 内存限制，profiler 直接初始化失败。这意味着在没有 cgroup 的环境（某些裸金属部署），ActiveQueryProfiler 不会启动——但这是合理的，因为没有 cgroup 限制就没有精确的 OOM 边界可供预测。

#### 核心决策：`shouldDump`

```go
func (o *ActiveQueryProfiler) shouldDump(ctx context.Context, st *cluster.Settings) (bool, int64) {
    if !ActiveQueryDumpsEnabled.Get(&st.SV) {
        return false, 0
    }

    cgMemUsage, _, _ := memUsageFn()              // cgroups.GetMemoryUsage()
    cgInactiveFileUsage, _, _ := memInactiveFileUsageFn()  // cgroups.GetMemoryInactiveFileUsage()
    curMemUsage := cgMemUsage - cgInactiveFileUsage  // "实际"内存使用

    defer func() {
        o.mu.Lock()
        defer o.mu.Unlock()
        o.mu.prevMemUsage = curMemUsage
    }()

    o.mu.Lock()
    defer o.mu.Unlock()
    if o.mu.prevMemUsage == 0 || curMemUsage <= o.mu.prevMemUsage || o.knobs.dontWriteProfiles {
        return false, curMemUsage
    }

    diff := curMemUsage - o.mu.prevMemUsage
    return curMemUsage + diff >= o.cgroupMemLimit - varianceBytes, curMemUsage
}
```

**OOM 预测模型**：

设：
- `cur` = 当前内存使用
- `prev` = 上一 tick 的内存使用
- `diff` = `cur - prev`（10s 内的增量）
- `limit` = cgroup 内存上限
- `variance` = 64MiB（安全余量）

预测公式：**如果 `cur + diff >= limit - 64MiB`，则预测下一个 tick 会 OOM**。

这是一个**线性外推**：假设内存增长速率保持不变，那么下一个 10s 后内存将达到 `cur + diff`。如果这个值逼近 cgroup limit，就提前 dump 活跃查询。

**为什么减去 `cgInactiveFileUsage`**：cgroup 的 `memory.usage_in_bytes` 包含 page cache 中的 inactive file-backed pages。这些页面在内存压力下可以被内核回收，不应计入"不可释放内存"。减去它使预测更准确。

**为什么用 `varianceBytes = 64MiB`**：这是一个保守偏移量。因为采样间隔是 10s，在高负载下内存增长可能不是线性的（可能加速）。64MiB 的余量给了提前 dump 的空间，避免"预测准确但 dump 太晚"的情况。

#### 写入逻辑

```go
func (o *ActiveQueryProfiler) takeQueryProfile(
    ctx context.Context, registry *sql.SessionRegistry, now time.Time, curMemUsage int64,
) bool {
    path := o.store.makeNewFileName(now, curMemUsage)
    f, err := os.Create(path)
    defer f.Close()
    writer := debug.NewActiveQueriesWriter(registry.SerializeAll(), f)
    err = writer.Write()
    return err == nil
}
```

`registry.SerializeAll()` 遍历所有活跃 SQL session，序列化其当前执行的查询信息。输出为 CSV 格式，包含查询文本、执行时间、分配内存等字段。

**主循环中的调用**（`env_sampler.go:248`）：

```go
queryProfiler.MaybeDumpQueries(ctx, cfg.sessionRegistry, cfg.st)
```

注意这里传入的是 `sessionRegistry` 而非 metric 值——ActiveQueryProfiler 自行从 cgroup 文件系统读取内存数据。

### 4.6 CPUProfiler

**文件**：`pkg/server/profiler/cpuprofiler.go`

CPUProfiler 是唯一使用**非零 floor** 的 profiler，也是唯一一个在 profile 过程中会**阻塞当前 goroutine** 的 profiler。

#### 初始化

```go
func NewCPUProfiler(ctx context.Context, dir string, st *cluster.Settings) (*CPUProfiler, error) {
    cp := &CPUProfiler{
        profiler: makeProfiler(
            newProfileStore(dumpStore, cpuProfFileNamePrefix, heapFileNameSuffix, st),
            func() int64 { return cpuUsageCombined.Get(&st.SV) },  // floor = 65（默认）
            func() time.Duration { return cpuProfileInterval.Get(&st.SV) },  // reset = 20min
        ),
        st: st,
    }
    return cp, nil
}
```

**关键差异**：
- `highWaterMarkFloor = cpuUsageCombined.Get()`（默认 65），即 CPU 使用率低于 65% 时永远不会触发 profile
- `resetInterval = cpuProfileInterval.Get()`（默认 20min），比内存类的 1h 短得多，因为 CPU spike 通常更短暂

#### 触发与写入

```go
func (cp *CPUProfiler) MaybeTakeProfile(ctx context.Context, currentCpuUsage int64) {
    defer func() {
        if p := recover(); p != nil {
            logcrash.ReportPanic(ctx, &cp.st.SV, p, 1)
        }
    }()
    cp.profiler.maybeTakeProfile(ctx, currentCpuUsage, cp.takeCPUProfile)
}
```

**Panic recovery**：CPUProfiler 有显式的 `recover()`，这在其他 profiler 中不存在（ActiveQueryProfiler 也有）。因为 `pprof.StartCPUProfile` 涉及 OS 信号处理（SIGPROF），在边缘情况下可能 panic。

#### `takeCPUProfile` 实现

```go
func (cp *CPUProfiler) takeCPUProfile(ctx context.Context, path string, _ ...interface{}) (success bool) {
    if err := debug.CPUProfileDo(cp.st, cluster.CPUProfileWithLabels, func() error {
        f, err := os.Create(path)
        defer f.Close()
        if err := pprof.StartCPUProfile(f); err != nil { return err }
        defer pprof.StopCPUProfile()

        dur := cpuProfileDuration.Get(&cp.st.SV)  // 默认 10s
        log.Dev.Infof(ctx, "taking CPU profile for %.2fs", dur.Seconds())
        select {
        case <-ctx.Done():
        case <-time.After(dur):
        }
        return nil
    }); err != nil {
        log.Dev.Infof(ctx, "error during CPU profile: %s", err)
        return false
    }
    return true
}
```

**关键行为**：

1. **阻塞 10s**：`time.After(cpuProfileDuration)` 会使主循环的当前 tick 延长 10s。在这期间，其他 profiler 不会被调用。这是可接受的，因为 CPU profile 是一个低频操作（至少 20min 间隔）。

2. **`debug.CPUProfileDo` 包装**：提供互斥保护——同一时间只能有一个 CPU profile 在运行（避免与 `net/http/pprof` 端点冲突）。

3. **`pprof.StartCPUProfile` / `StopCPUProfile`**：Go 标准库的 CPU profiling 机制，基于 OS 定时信号（Linux 上是 `setitimer(ITIMER_PROF)`），以 100Hz 采样调用栈。

**主循环中的调用**（`env_sampler.go:252`）：

```go
cpuProfiler.MaybeTakeProfile(ctx, int64(cfg.runtime.CPUCombinedPercentNorm.Value()*100))
```

`CPUCombinedPercentNorm` 是归一化到 [0, 1] 的 CPU 使用率，乘以 100 转换为百分比整数。默认 floor 65 意味着只有 CPU > 65% 时才开始关注。

### 4.7 SimpleFlightRecorder

**文件**：`pkg/util/tracing/goexectrace/simple_flight_recorder.go`

FlightRecorder 是整个体系中**唯一独立于主循环运行**的子系统。它基于 Go 1.22+ 的 `golang.org/x/exp/trace.FlightRecorder` API。

#### 核心概念

`trace.FlightRecorder` 是 Go 运行时提供的执行轨迹记录器。它不同于 CPU profiler（采样调用栈），而是记录**精确的调度事件**：goroutine 创建/阻塞/唤醒、系统调用、GC 事件、网络 IO 等。类似飞机黑匣子，它在一个环形缓冲区中持续记录，可以随时 snapshot 出最近 N 秒的执行轨迹。

#### 数据结构

```go
type SimpleFlightRecorder struct {
    fr                   *trace.FlightRecorder
    dumpStore            *dumpstore.DumpStore
    sv                   *settings.Values
    directory            string
    enabledCheckInterval time.Duration  // 检查是否启用的间隔（初始化参数 10s）
    enabled              atomic.Bool
}
```

#### 初始化

```go
func NewFlightRecorder(
    st *cluster.Settings, enabledCheckInterval time.Duration, directory string,
) (*SimpleFlightRecorder, error) {
    fr := trace.NewFlightRecorder()
    if directory == "" {
        return nil, errors.New("...")
    }
    if err := os.MkdirAll(directory, 0755); err != nil { return nil, ... }
    return &SimpleFlightRecorder{
        fr:                   fr,
        dumpStore:            dumpstore.NewStore(directory, executionTracerTotalDumpSizeLimit, st),
        sv:                   &st.SV,
        directory:            directory,
        enabledCheckInterval: enabledCheckInterval,  // 10s
    }, nil
}
```

**磁盘预算 4GiB**（`executionTracerTotalDumpSizeLimit`），远大于其他 profiler，因为执行轨迹文件体积较大且该功能默认关闭，一旦启用通常需要留存大量数据。

#### 独立循环：`Start`

```go
func (sfr *SimpleFlightRecorder) Start(ctx context.Context, stopper *stop.Stopper) error {
    return stopper.RunAsyncTask(ctx, "simple-flight-recorder", func(ctx context.Context) {
        t := timeutil.Timer{}
        t.Reset(sfr.enabledCheckInterval)  // 首次检查间隔 10s

        defer func() {
            if sfr.fr.Enabled() {
                sfr.fr.Stop()
                sfr.enabled.Store(false)
            }
        }()

        for {
            select {
            case <-t.C:
                startTime := timeutil.Now()
                interval := ExecutionTracerInterval.Get(sfr.sv)  // 默认 0（禁用）

                if interval == 0 {
                    // 功能被禁用
                    if sfr.fr.Enabled() {
                        sfr.fr.Stop()
                        sfr.enabled.Store(false)
                    }
                    t.Reset(sfr.enabledCheckInterval)  // 10s 后再检查
                    continue
                }

                if !sfr.fr.Enabled() {
                    // 功能刚被启用，启动 FlightRecorder
                    duration := ExecutionTracerDuration.Get(sfr.sv)  // 默认 10s
                    sfr.fr.SetPeriod(duration)
                    err := sfr.fr.Start()
                    if err != nil {
                        t.Reset(max(interval-timeutil.Since(startTime), 0))
                        continue
                    }
                    sfr.enabled.Store(true)
                }

                // 导出当前缓冲区到文件
                filename := sfr.TimestampedFilename()
                destFile, _ := os.Create(filename)
                sfr.fr.WriteTo(destFile)  // 非阻塞：snapshot 环形缓冲区

                sfr.dumpStore.GC(ctx, timeutil.Now(), sfr)
                t.Reset(max(interval-timeutil.Since(startTime), 0))

            case <-stopper.ShouldQuiesce():
                return
            case <-ctx.Done():
                return
            }
        }
    })
}
```

**状态机**：

```
                 interval=0          interval>0
          ┌───────────────┐    ┌──────────────────┐
          │               │    │                  │
          ▼               │    ▼                  │
    ┌──────────┐    ┌──────────┐    ┌──────────────┐
    │ DISABLED │───►│ STARTING │───►│   RUNNING    │
    │          │    │          │    │ (recording)  │
    └──────────┘    └──────────┘    └──────────────┘
          ▲                               │
          │         interval=0            │
          └───────────────────────────────┘
```

**与主循环的关键区别**：

| 特性 | 主循环 (mem-logger) | FlightRecorder |
|------|---------------------|----------------|
| goroutine | `RunAsyncTaskEx` (SterileRootSpan) | `RunAsyncTask` |
| 定时器 | 固定 10s | 动态 (`ExecutionTracerInterval`) |
| 默认状态 | 启用 | 禁用 (`interval=0`) |
| 输出模式 | 条件触发 | 每个 interval 无条件导出 |
| 开销 | 低（仅读取 metric） | 较高（运行时 tracing） |

**`t.Reset(max(interval - timeutil.Since(startTime), 0))` 的含义**：确保 timer 周期是从 tick 开始时算起，而非从处理完成后算起。减去处理耗时，避免 drift。如果处理耗时超过 interval，立即触发下一次（Reset(0)）。

---

## 5. 具体运行示例

### 场景一：正常运行 + Go 堆内存缓慢增长

**初始条件**：
- `metricsSampleInterval` = 10s
- `COCKROACH_MEMPROF_INTERVAL` = 1h（默认）
- HeapProfiler `highWaterMark` = 0, `lastProfileTime` = T0
- `GoAllocBytes` 初始值 = 200MiB
- `CgoTotalBytes` = 1GiB
- Goroutine count = 500
- CPU = 30%
- `cpuUsageCombined` floor = 65

**时间线**：

| 时间 | GoAllocBytes | 事件 | HeapProfiler 状态 |
|------|-------------|------|-------------------|
| T0+10s | 200MiB | tick #1: `200MiB > 0` → **profile!** | hwm=200MiB, file: `memprof.T0+10s.209715200.pprof` |
| T0+20s | 210MiB | tick #2: `210MiB > 200MiB` → **profile!** | hwm=210MiB |
| T0+30s | 205MiB | tick #3: `205MiB < 210MiB` → skip | hwm=210MiB |
| T0+40s | 220MiB | tick #4: `220MiB > 210MiB` → **profile!** | hwm=220MiB |
| ... | 稳定在 ~220MiB | tick #5..#360: 无新高 → 全部 skip | hwm=220MiB |
| T0+1h+10s | 218MiB | 距上次 profile >= 1h → **重置水位到 0**，`218MiB > 0` → **profile!** | hwm=218MiB |

**同一时间段其他子系统**：

- **GoroutineDumper**：500 < 1000 (阈值)，所有 tick 均不触发
- **CPUProfiler**：30 < 65 (floor)，不触发
- **NonGoAllocProfiler**：1GiB 在 tick #1 触发一次（hwm=0），之后 CgoTotal 稳定则不再触发
- **Jemalloc Purge**：假设 `CGoAllocatedBytes=800MiB, CGoTotalBytes=1GiB, cgoMemTarget=512MiB`
  - overhead = `(1024 - max(800, 512)) / max(800, 512) * 100 = 224/800*100 = 28%` > 20% → purge（如果距上次 >= 2min）

### 场景二：OOM 压力场景

**初始条件**：
- cgroup 内存限制 = 8GiB
- 某个复杂查询开始执行，持续消耗内存
- `ActiveQueryDumpsEnabled` = true

**时间线**：

| 时间 | curMemUsage | prevMemUsage | diff | cur+diff | 判定 |
|------|-------------|-------------|------|----------|------|
| T+0s | 5.0GiB | 0 | - | - | `prev=0` → skip |
| T+10s | 5.5GiB | 5.0GiB | 0.5GiB | 6.0GiB | 6.0 < 8.0-0.064=7.936 → skip |
| T+20s | 6.2GiB | 5.5GiB | 0.7GiB | 6.9GiB | 6.9 < 7.936 → skip |
| T+30s | 7.0GiB | 6.2GiB | 0.8GiB | 7.8GiB | 7.8 < 7.936 → skip |
| T+40s | 7.5GiB | 7.0GiB | 0.5GiB | 8.0GiB | **8.0 >= 7.936** → **DUMP!** |

T+40s 时：
1. `shouldDump` 返回 `(true, 7.5GiB)`
2. `takeQueryProfile` 调用 `registry.SerializeAll()` 快照所有活跃查询
3. 写入 `activequeryprof.T+40s.8053063680.csv`
4. 文件内容包含那个消耗大量内存的查询的完整 SQL 文本和执行统计

**同时**，HeapProfiler 在 T+10s/T+20s/T+30s/T+40s 每次 GoAllocBytes 创新高时都会抓 heap profile。MemoryMonitoringProfiler 同步写出 BytesMonitor 树，显示那个查询的 session monitor 持有 2GiB+ 内存。StatsProfiler 在 RSS 创新高时导出 runtime.MemStats JSON。

**事后分析时**，工程师可以：
1. 从 `activequeryprof` CSV 找到 OOM 元凶 SQL
2. 从 `memprof` pprof 找到内存分配热点函数
3. 从 `memmonitoring` txt 找到内存归属的逻辑组件
4. 从 `memstats` txt 对比 Go/CGo 内存比例

这四类文件**联合**提供了完整的 OOM 现场还原能力。

### 场景三：CPU spike 触发 CPU profile

**条件**：
- 当前 `CPUCombinedPercentNorm` = 0.82 (82%)
- CPUProfiler `highWaterMark` = 65 (floor)
- `cpuProfileDuration` = 10s

**流程**：

1. 主循环 tick 到达，`SampleEnvironment` 计算出 `CPUCombinedPercentNorm = 0.82`
2. 传入 `cpuProfiler.MaybeTakeProfile(ctx, 82)`
3. `maybeTakeProfile`: `82 > 65` (hwm) → 触发
4. `takeCPUProfile`:
   - `pprof.StartCPUProfile(f)` 开始采样
   - **主循环在此阻塞 10 秒**
   - `pprof.StopCPUProfile()` 停止采样
5. `highWaterMark` 更新为 82
6. 下次 tick（10s 后），如果 CPU = 78%，`78 < 82` → 不触发
7. 如果 CPU = 90%，`90 > 82` → 再次触发
8. 20 分钟后水位重置到 65，新一轮开始

---

## 6. 设计模式总结

### 6.1 模板方法模式 (Template Method)

`profiler.maybeTakeProfile` 是模板方法，定义了 "检查水位 → 决策 → 执行 → GC" 的骨架。各具体 profiler 只需提供 `takeProfileFn`（策略方法）。

### 6.2 策略模式 (Strategy)

- `highWaterMarkFloor func() int64`：不同 profiler 使用不同的 floor 策略
- `resetInterval func() time.Duration`：不同的重置周期
- `takeProfileFn`：不同的 profile 写入逻辑

使用 `func()` 而非固定值，实现了**运行时可变策略**（绑定 cluster setting）。

### 6.3 编译时依赖注入

`jemallocHeapDump`、`getCgoMemStats`、`cgoMemMaybePurge` 均通过 `var fn func(...)` + `SetXxxFn()` 模式注入。这是 Go 中隔离平台特定 CGo 链接依赖的标准做法，确保核心包可以独立 `go test`。

### 6.4 启发式触发 (Heuristic-based)

GoroutineDumper 使用可扩展的 `[]heuristic` 列表，当前只有 `doubleSinceLastDumpHeuristic`。这个设计预留了添加更多启发式规则的扩展点（如 "goroutine 增长速率"、"特定 goroutine 类型比例"）。

### 6.5 Ramp-Up 感知 GC

`profileStore.cleanupLastRampup` 不是简单的 FIFO 或 LRU 淘汰。它识别内存使用的"爬坡序列"，保留最近一轮完整的爬坡快照。这是一个领域特定的 GC 策略，直接服务于 OOM 诊断的信息需求。

### 6.6 独立循环 vs. 共享循环

FlightRecorder 使用独立循环是因为：
1. 它的开启/关闭是动态的（通过 cluster setting）
2. 它的 interval 与主循环不同
3. `trace.FlightRecorder` 有自己的生命周期管理 (`Start/Stop`)

其他 profiler 共享主循环是因为它们的调用开销低（仅比较一个整数），不值得独立循环的 goroutine 开销。

### 6.7 防御性编程

- Profile 写入失败时不 GC 旧文件
- `recover()` 保护 CPU/Query profiler
- 目录不存在时降级（禁用对应功能）而非 crash
- cluster setting 变化时重置状态（如 goroutinesThreshold 变更时重置 maxGoroutinesDumped）
