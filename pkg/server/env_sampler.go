// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/goroutinedumper"
	"github.com/cockroachdb/cockroach/pkg/server/profiler"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/goexectrace"
	"github.com/cockroachdb/errors"
)

var jemallocPurgeOverhead = settings.RegisterIntSetting(
	settings.SystemVisible,
	"server.jemalloc_purge_overhead_percent",
	"a purge of jemalloc dirty pages is issued once the overhead exceeds this percent (0 disables purging)",
	20,
	settings.NonNegativeInt,
)

var jemallocPurgePeriod = settings.RegisterDurationSettingWithExplicitUnit(
	settings.SystemVisible,
	"server.jemalloc_purge_period",
	"minimum amount of time that must pass between two jemalloc dirty page purges (0 disables purging)",
	2*time.Minute,
)

type sampleEnvironmentCfg struct {
	st                    *cluster.Settings
	stopper               *stop.Stopper
	minSampleInterval     time.Duration
	goroutineDumpDirName  string
	heapProfileDirName    string
	cpuProfileDirName     string
	executionTraceDirName string
	runtime               *status.RuntimeStatSampler
	sessionRegistry       *sql.SessionRegistry
	rootMemMonitor        *mon.BytesMonitor
	cgoMemTarget          uint64
}

// startSampleEnvironment 启动一个后台周期性循环，负责监控节点的运行环境健康状况。
// 
// 核心作用：
// 1. 自我诊断：定期采样运行时指标（如内存、CPU、Goroutine 数量）。
// 2. 自动快照：当资源消耗（如内存或 CPU）超过设定的阈值时，自动触发 Dump 操作（如堆内存快照、堆栈追踪）。
// 3. 性能辅助：根据内存使用情况，自动触发 Jemalloc 内存释放，防止内存碎片导致的 OOM。
// 4. 故障溯源：支持记录执行轨迹（Flight Recorder）和导出活跃查询列表，方便事后分析。
//
// 包含的子系统：
// - GoroutineDumper: 监控协程数量。
// - HeapProfiler: 监控 Go 堆内存。
// - CPUProfiler: 监控 CPU 使用率。
// - FlightRecorder: 记录最近一段时间的系统执行轨迹（类似飞机的黑匣子）。
// - QueryProfiler: 记录高负载时的活跃 SQL。
//
// 例子：
// - 场景：集群突然内存飙升。
// - 过程：该循环检测到 `GoAllocBytes` 超过了集群设置定义的阈值。
// - 结果：`heapProfiler.MaybeTakeProfile` 会立即生成一个 `.pprof` 文件并保存到磁盘。
//   这样工程师不用手动去抓，就能直接拿到负载最高瞬间的现场数据，极大方便了线上排查。
// startSampleEnvironment 启动一个后台周期性循环，负责监控节点的运行环境健康状况。
// 
// 核心作用：
// 1. 自我诊断：定期采样运行时指标（如内存、CPU、Goroutine 数量）。
// 2. 自动快照：当资源消耗（如内存或 CPU）超过设定的阈值时，自动触发 Dump 操作（如堆内存快照、堆栈追踪）。
// 3. 性能辅助：根据内存使用情况，自动触发 Jemalloc 内存释放，防止内存碎片导致的 OOM。
// 4. 故障溯源：支持记录执行轨迹（Flight Recorder）和导出活跃查询列表，方便事后分析。
//
// 包含的子系统：
// - GoroutineDumper: 监控协程数量。
// - HeapProfiler: 监控 Go 堆内存。
// - CPUProfiler: 监控 CPU 使用率。
// - FlightRecorder: 记录最近一段时间的系统执行轨迹（类似飞机的黑匣子）。
// - QueryProfiler: 记录高负载时的活跃 SQL。
//
// 例子：
// - 场景：集群突然内存飙升。
// - 过程：该循环检测到 `GoAllocBytes` 超过了集群设置定义的阈值。
// - 结果：`heapProfiler.MaybeTakeProfile` 会立即生成一个 `.pprof` 文件并保存到磁盘。
//   这样工程师不用手动去抓，就能直接拿到负载最高瞬间的现场数据，极大方便了线上排查。
func startSampleEnvironment(
	ctx context.Context,
	srvCfg *BaseConfig,
	pebbleCacheSize int64,
	stopper *stop.Stopper,
	runtimeSampler *status.RuntimeStatSampler,
	sessionRegistry *sql.SessionRegistry,
	rootMemMonitor *mon.BytesMonitor,
) error {
	// 确定采样间隔。默认为 10s，如果提供了测试参数则使用测试参数。
	metricsSampleInterval := base.DefaultMetricsSampleInterval
	if p, ok := srvCfg.TestingKnobs.Server.(*TestingKnobs); ok && p.EnvironmentSampleInterval != time.Duration(0) {
		metricsSampleInterval = p.EnvironmentSampleInterval
	}
	cfg := sampleEnvironmentCfg{
		st:                    srvCfg.Settings,
		stopper:               stopper,
		minSampleInterval:     metricsSampleInterval,
		goroutineDumpDirName:  srvCfg.GoroutineDumpDirName,
		heapProfileDirName:    srvCfg.HeapProfileDirName,
		cpuProfileDirName:     srvCfg.CPUProfileDirName,
		executionTraceDirName: srvCfg.ExecutionTraceDirName,
		runtime:               runtimeSampler,
		sessionRegistry:       sessionRegistry,
		rootMemMonitor:        rootMemMonitor,
		// 设置 CGo 内存释放的目标：通常不低于 Pebble 缓存大小。
		cgoMemTarget:          max(uint64(pebbleCacheSize), 128*1024*1024),
	}

	// === 第一步：初始化 Goroutine 导出器 ===
	var goroutineDumper *goroutinedumper.GoroutineDumper
	if cfg.goroutineDumpDirName != "" {
		hasValidDumpDir := true
		// 确保输出目录存在。
		if err := os.MkdirAll(cfg.goroutineDumpDirName, 0755); err != nil {
			log.Dev.Warningf(ctx, "cannot create goroutine dump dir -- goroutine dumps will be disabled: %v", err)
			hasValidDumpDir = false
		}
		if hasValidDumpDir {
			var err error
			goroutineDumper, err = goroutinedumper.NewGoroutineDumper(ctx, cfg.goroutineDumpDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting goroutine dumper worker")
			}
		}
	}

	// === 第二步：初始化各种内存/CPU 分析器 (Profilers) ===
	var heapProfiler *profiler.HeapProfiler
	var memMonitoringProfiler *profiler.MemoryMonitoringProfiler
	var nonGoAllocProfiler *profiler.NonGoAllocProfiler
	var statsProfiler *profiler.StatsProfiler
	var queryProfiler *profiler.ActiveQueryProfiler
	var cpuProfiler *profiler.CPUProfiler
	if cfg.heapProfileDirName != "" {
		hasValidDumpDir := true
		if err := os.MkdirAll(cfg.heapProfileDirName, 0755); err != nil {
			log.Dev.Warningf(ctx, "cannot create memory dump dir -- memory profile dumps will be disabled: %v", err)
			hasValidDumpDir = false
		}

		if hasValidDumpDir {
			var err error
			// 初始化常规堆内存分析器。
			heapProfiler, err = profiler.NewHeapProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting heap profiler worker")
			}
			// 初始化内存监控分析器（监控 BytesMonitor 指标）。
			memMonitoringProfiler, err = profiler.NewMemoryMonitoringProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting memory monitoring profiler worker")
			}
			// 初始化非 Go 内存分析器。
			nonGoAllocProfiler, err = profiler.NewNonGoAllocProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting non-go alloc profiler worker")
			}
			// 初始化统计信息分析器。
			statsProfiler, err = profiler.NewStatsProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting memory stats collector worker")
			}
			// 初始化查询分析器，用于在问题发生时导出活跃 SQL。
			queryProfiler, err = profiler.NewActiveQueryProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				log.Dev.Warningf(ctx, "failed to start query profiler worker: %v", err)
			}
			// 初始化 CPU 分析器。
			cpuProfiler, err = profiler.NewCPUProfiler(ctx, cfg.cpuProfileDirName, cfg.st)
			if err != nil {
				log.Dev.Warningf(ctx, "failed to start cpu profiler worker: %v", err)
			}
		}
	}

	// === 第三步：启动执行轨迹记录仪 (Flight Recorder) ===
	// 它的作用像“黑匣子”，循环覆盖记录最近 10 秒的系统执行细节。
	simpleFlightRecorder, err := goexectrace.NewFlightRecorder(cfg.st, 10*time.Second, cfg.executionTraceDirName)
	if err != nil {
		log.Dev.Warningf(ctx, "failed to initialize flight recorder: %v", err)
	} else {
		err = simpleFlightRecorder.Start(ctx, cfg.stopper)
		if err != nil {
			log.Dev.Warningf(ctx, "failed to start flight recorder: %v", err)
		}
	}

	// === 第四步：进入主循环 (采样核心逻辑) ===
	return cfg.stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{TaskName: "mem-logger", SpanOpt: stop.SterileRootSpan},
		func(ctx context.Context) {
			var timer timeutil.Timer
			defer timer.Stop()
			// 按配置的采样间隔进行循环。
			timer.Reset(cfg.minSampleInterval)

			for {
				select {
				case <-cfg.stopper.ShouldQuiesce():
					return // 节点安全退出
				case <-timer.C:
					timer.Reset(cfg.minSampleInterval)

					// 1. 获取 CGo 层面的底层内存统计信息。
					cgoStats := status.GetCGoMemStats(ctx)
					// 2. 将采集到的指标喂给 RuntimeSampler 记录。
					cfg.runtime.SampleEnvironment(ctx, cgoStats)

					// 3. 内存释放逻辑：如果满足条件，触发 jemalloc 主动归还内存给系统。
					if overhead, period := jemallocPurgeOverhead.Get(&cfg.st.SV), jemallocPurgePeriod.Get(&cfg.st.SV); overhead > 0 && period > 0 {
						status.CGoMemMaybePurge(ctx, cgoStats.CGoAllocatedBytes, cgoStats.CGoTotalBytes, cfg.cgoMemTarget, int(overhead), period)
					}

					// 4. 各种自查快照逻辑：如果检测到相应指标过高，由各自的 MaybeDump/MaybeTakeProfile 决定是否写快照文件。
					if goroutineDumper != nil {
						// 检查协程数量。
						goroutineDumper.MaybeDump(ctx, cfg.st, cfg.runtime.Goroutines.Value())
					}
					if heapProfiler != nil {
						// 检查堆内存。
						heapProfiler.MaybeTakeProfile(ctx, cfg.runtime.GoAllocBytes.Value())
						// 检查 BytesMonitor。
						memMonitoringProfiler.MaybeTakeMemoryMonitoringDump(ctx, cfg.runtime.GoAllocBytes.Value(), cfg.rootMemMonitor, cfg.st)
						// 检查非 Go 分配内存。
						nonGoAllocProfiler.MaybeTakeProfile(ctx, cfg.runtime.CgoTotalBytes.Value())
						// 检查 RSS 状态。
						statsProfiler.MaybeTakeProfile(ctx, cfg.runtime.RSSBytes.Value(), cgoStats)
					}
					if queryProfiler != nil {
						// 检查并记录长时间运行或其他触发阈值的 SQL。
						queryProfiler.MaybeDumpQueries(ctx, cfg.sessionRegistry, cfg.st)
					}
					if cpuProfiler != nil {
						// 检查 CPU 使用率。
						cpuProfiler.MaybeTakeProfile(ctx, int64(cfg.runtime.CPUCombinedPercentNorm.Value()*100))
					}
				}
			}
		})
}
