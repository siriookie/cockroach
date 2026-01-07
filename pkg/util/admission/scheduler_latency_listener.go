// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/logtags"
)

var _ metric.Struct = &schedulerLatencyListenerMetrics{}

type schedulerLatencyListener struct {
	ctx               context.Context
	elasticCPULimiter elasticCPULimiter
	coord             *ElasticCPUGrantCoordinator
	metrics           *schedulerLatencyListenerMetrics
	settings          *cluster.Settings

	testingParams *schedulerLatencyListenerParams
}

func newSchedulerLatencyListener(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	metrics *schedulerLatencyListenerMetrics,
	e elasticCPULimiter,
) *schedulerLatencyListener {
	ctx := ambientCtx.AnnotateCtx(context.Background())
	ctx = logtags.AddTag(ctx, "scheduler-latency-listener", "")
	return &schedulerLatencyListener{
		ctx:               ctx,
		settings:          st,
		metrics:           metrics,
		elasticCPULimiter: e,
	}
}

func (e *schedulerLatencyListener) setCoord(coord *ElasticCPUGrantCoordinator) {
	e.coord = coord
}

// SchedulerLatency is part of the SchedulerLatencyListener interface. It
// controls the elastic CPU % limit based on scheduling latency and elastic CPU
// utilization data. Every tick we measure scheduling_p99 and execute the
// following:
//
//		IF scheduling_p99 > target_p99:
//			utilization_limit = max(utilization_limit – delta * factor, min_utilization)
//		ELSE:
//			IF requests_waiting:
//					utilization_limit = min(utilization_limit + delta, max_utilization)
//	       ELSE:
//				utilization_limit = max(utilization_limit – delta * inactive_factor, inactive_utilization)
//
// Definitions:
//
//	 scheduling_p99        Observed p99 scheduling latency a recent time window.
//	 target_p99            Target p99 scheduling latency.
//	 min_utilization       Floor on per-node elastic work CPU % utilization.
//	 max_utilization       Ceiling on per-node elastic work CPU % utilization.
//	 inactive_utilization  The CPU % utilization we decrease to when there's no utilization.
//	 delta                 Per-tick adjustment of CPU %.
//	 factor                Multiplicative factor for delta, used when decreasing utilization.
//	 inactive_factor       Multiplicative factor for delta, used when decreasing utilization when inactive.
//	 requests_waiting      Whether there are requests waiting due to insufficient utilization limit.
//	 utilization_limit     CPU % utilization limit for elastic work.
//
//	The controller uses fixed deltas for adjustments, adjusting down a bit more
//	aggressively than adjusting up. This is due to the nature of the work being
//	paced — we care more about quickly introducing a ceiling rather than
//	staying near it (though experimentally we’re able to stay near it just
//	fine). It adjusts upwards only when seeing waiting requests that could use
//	more quota (assuming it’s under the p99 target). The adjustments are small
//	to reduce {over,under}shoot and controller instability at the cost of being
//	somewhat dampened. We use a relatively long duration for measuring scheduler
//	latency data; since the p99 is computed off of histogram data, we saw a lot
//	more jaggedness when taking p99s off of a smaller set of scheduler events
//	(last 50ms for ex.) compared to computing p99s over a larger set of
//	scheduler events (last 2500ms). This, with the small deltas used for
//	adjustments, can make for a dampened response, but assuming a stable-ish
//	foreground CPU load against a node, it works fine. The controller output is
//	limited to a well-defined range that can be tuned through cluster settings.
//	This controller can be made more involved if we find good reasons for
//	it; this is just the first version that worked well-enough.
// SchedulerLatency 是 SchedulerLatencyListener 接口的一部分。
// 它根据调度延迟（scheduling latency）和弹性 CPU 利用率数据，
// 动态控制弹性 CPU 的百分比上限（elastic CPU % limit）。
//
// 在每一次周期性 tick 中，我们都会测量 scheduling_p99，
// 并执行如下控制逻辑：
//
//     IF scheduling_p99 > target_p99:
//         utilization_limit = max(utilization_limit – delta * factor, min_utilization)
//     ELSE:
//         IF requests_waiting:
//             utilization_limit = min(utilization_limit + delta, max_utilization)
//         ELSE:
//             utilization_limit = max(utilization_limit – delta * inactive_factor, inactive_utilization)
//
// 变量定义：
//
//     scheduling_p99
//         最近一个时间窗口内观测到的调度延迟 p99 值。
//
//     target_p99
//         期望的调度延迟 p99 目标值。
//
//     min_utilization
//         每个节点上弹性工作负载（elastic work）CPU 使用率的下限。
//
//     max_utilization
//         每个节点上弹性工作负载（elastic work）CPU 使用率的上限。
//
//     inactive_utilization
//         当没有利用率需求时，CPU 使用率会逐步下降到的目标值。
//
//     delta
//         每个 tick 中 CPU 百分比的调整步长。
//
//     factor
//         在降低利用率时，对 delta 进行放大的乘数因子。
//
//     inactive_factor
//         在“无请求活动”情况下，降低利用率时使用的 delta 放大因子。
//
//     requests_waiting
//         是否存在由于当前利用率上限不足而处于等待状态的请求。
//
//     utilization_limit
//         弹性工作负载可使用的 CPU 百分比上限。
//
// 控制器采用固定步长（delta）来进行调整，并且在“向下调整”时
// 比“向上调整”更加激进一些。这是因为该工作负载具有被节奏控制
//（paced）的特性 —— 我们更关心的是能够快速引入一个 CPU 使用上限，
// 而不是始终精确地贴近该上限（尽管从实验结果来看，实际上也能保持
// 得相当接近）。
//
// 控制器只有在满足以下条件时才会向上调整利用率：
// 在调度延迟 p99 未超过目标值的前提下，确实存在正在等待、
// 且可以利用更多 CPU 配额的请求。
//
// 每次调整的幅度都被刻意设计得较小，以减少过冲（overshoot）和
// 欠冲（undershoot）以及控制器的不稳定性，代价是响应会稍显迟缓。
// 我们使用了相对较长的时间窗口来统计调度延迟数据；
// 由于 p99 是基于直方图数据计算的，我们发现如果只使用较小
// 的调度事件集合（例如最近 50ms），p99 曲线会非常“锯齿化”，
// 而使用更大的时间窗口（例如最近 2500ms）来计算 p99，
// 结果会平滑得多。
//
// 结合较小的调整步长，这种设计可能会使控制器的响应显得有些被“阻尼”
//（dampened），但在节点前台 CPU 负载相对稳定的前提下，
// 该策略运行效果良好。
//
// 控制器的输出被限制在一个明确定义的范围内，
// 并且可以通过集群配置项进行调节。
// 如果将来发现有充分的理由，
// 这个控制器是可以进一步变得更加复杂的；
// 目前这只是第一个“效果足够好”的版本。

func (e *schedulerLatencyListener) SchedulerLatency(p99, period time.Duration) {
	// 步骤 1: 加载动态配置参数
	// Load dynamic configuration parameters
	// 从 cluster.Settings 读取实时配置,支持在线调整
	params := e.getParams(period)
	if !params.enabled {
		return // nothing to do
	}
	// 步骤 2: 记录 P99 到 Prometheus
	// Record P99 to Prometheus for monitoring
	e.metrics.P99SchedulerLatency.Update(p99.Nanoseconds())

	// 步骤 3: 查询系统状态
	// Query system state: are there waiting requests?
	// 这是一个关键信号: 有需求 vs 无需求
	hasWaitingRequests := e.elasticCPULimiter.hasWaitingRequests()
	oldUtilizationLimit := e.elasticCPULimiter.getUtilizationLimit() //默认0.05
	newUtilizationLimit := oldUtilizationLimit

	// === 状态机核心 ===
	// State Machine Core
	if p99 > params.targetP99 { // over latency target; decrease limit
		// 状态 1: 过载 → 快速降低配额
		// State 1: Overloaded → Rapidly decrease quota
		//
		// 数学原理:
		//   新配额 = 旧配额 - 步长 × 放大因子
		//   例: 50% - 0.01% × 2 = 49.98%
		//
		// 设计理念:
		//   宁可"误杀"(过度限制),不可"漏网"(延迟暴涨)
		//   因为前台延迟直接影响用户体验
		newUtilizationLimit = oldUtilizationLimit -
			(params.adjustmentDelta * params.multiplicativeFactorOnDecrease)
		// 裁剪到 [min, max] 区间
		// Clamp to [min, max] range
		newUtilizationLimit = clamp(params.minUtilization, params.maxUtilization, newUtilizationLimit)
		if log.V(1) {
			log.Dev.Infof(e.ctx, "clamp(%0.2f%% - %0.2f%%) => %0.2f%%",
				100*oldUtilizationLimit, 100*params.adjustmentDelta*params.multiplicativeFactorOnDecrease,
				100*newUtilizationLimit)
		}
	} else { // under latency target// p99 <= target_p99
		if hasWaitingRequests { // increase limit if there are waiting requests
			// 状态 2: 延迟达标 + 有需求 → 缓慢增加配额
			// State 2: Under target + demand → Slowly increase quota
			//
			// 数学原理:
			//   新配额 = 旧配额 + 步长
			//   例: 50% + 0.01% = 50.01%
			//
			// 设计理念:
			//   保守增长,避免超调(overshoot)导致延迟突增
			//   给系统足够时间适应新的 CPU 水平
			newUtilizationLimit = oldUtilizationLimit + params.adjustmentDelta
			newUtilizationLimit = clamp(params.minUtilization, params.maxUtilization, newUtilizationLimit)
			if log.V(1) {
				log.Dev.Infof(e.ctx, "clamp(%0.2f%% + %0.2f%%) => %0.2f%%",
					100*oldUtilizationLimit, 100*params.adjustmentDelta,
					100*newUtilizationLimit)
			}
		} else { // unused limit; slowly decrease it
			// 状态 3: 延迟达标 + 无需求 → 超慢衰减
			// State 3: Under target + no demand → Ultra-slow decay
			//
			// 计算 inactive 目标值:
			//   例: min=5%, max=75%, inactivePoint=0.1
			//   → inactiveTarget = 5% + 0.1 × (75% - 5%) = 12%
			inactiveUtilizationLimit := params.minUtilization +
				params.inactivePoint*(params.maxUtilization-params.minUtilization)
			if oldUtilizationLimit > inactiveUtilizationLimit {
				// 只在高于 inactive 目标时才衰减
				// Only decay if above inactive target
				//
				// 数学原理:
				//   新配额 = 旧配额 - 步长 × 空闲因子
				//   例: 50% - 0.01% × 0.25 = 49.9975%
				//
				// 设计理念:
				//   极慢的衰减避免"抖动":
				//   如果任务突然恢复,不必从很低的配额重新爬升
				newUtilizationLimit = oldUtilizationLimit -
					(params.adjustmentDelta * params.multiplicativeFactorOnInactiveDecrease)
				newUtilizationLimit = clamp(inactiveUtilizationLimit, params.maxUtilization, newUtilizationLimit)
				if log.V(1) {
					log.Dev.Infof(e.ctx, "clamp(%0.2f%% - %0.2f%%) => %0.2f%% (inactive)",
						100*oldUtilizationLimit, 100*params.adjustmentDelta*params.multiplicativeFactorOnInactiveDecrease,
						100*newUtilizationLimit)
				}
			}
		}
	}
	// 步骤 4: 应用新的配额限制
	// Step 4: Apply new quota limit
	// 这会立即影响后续的 CPU token 发放速率
	e.elasticCPULimiter.setUtilizationLimit(newUtilizationLimit)
	// 步骤 5: 更新监控指标
	// Step 5: Update monitoring metrics
	e.elasticCPULimiter.computeUtilizationMetric()
	// 步骤 6: 触发准入控制协调器
	// Step 6: Trigger admission control coordinator
	// 这是一个"心跳"机制:
	//   每次调度延迟回调都会尝试授予等待中的请求
	//   如果配额增加,可能会立即释放被阻塞的任务
	if e.coord != nil { // only nil in tests
		// TODO(irfansharif): Right now this is the only ticking mechanism for
		// elastic CPU grants; consider some form of explicit ticking instead.
		// We have this need for fine-granularity explicit ticking for the IO
		// tokens too, where the 250ms granularity is too coarse. Ideally a 1ms
		// granularity would be good. We've had problems with that in unloaded
		// systems, see the samplePeriod{Short,Long} logic goschedstats, so
		// maybe we can generalize that period switching into a struct where the
		// coarser period is used only when some func indicates that the
		// relevant "resource" is underloaded -- for goschedstats this resource
		// is the CPU, and for these token buckets it will be based on how many
		// tokens are still available.
		e.coord.tryGrant()
	}
}

func (e *schedulerLatencyListener) getParams(period time.Duration) schedulerLatencyListenerParams {
	if e.testingParams != nil {
		return *e.testingParams
	}

	enabled := elasticCPUControlEnabled.Get(&e.settings.SV)
	targetP99 := elasticCPUSchedulerLatencyTarget.Get(&e.settings.SV)
	minUtilization := elasticCPUMinUtilization.Get(&e.settings.SV)
	maxUtilization := elasticCPUMaxUtilization.Get(&e.settings.SV)
	if minUtilization > maxUtilization { // user error
		defaultMinUtilization := elasticCPUMinUtilization.Default()
		defaultMaxUtilization := elasticCPUMaxUtilization.Default()
		log.Dev.Errorf(e.ctx, "min utilization (%0.2f%%) > max utilization (%0.2f%%); resetting to defaults [%0.2f%%, %0.2f%%]",
			minUtilization*100, maxUtilization*100, defaultMinUtilization*100, defaultMaxUtilization*100,
		)
		minUtilization, maxUtilization = defaultMinUtilization, defaultMaxUtilization
	}
	inactivePoint := elasticCPUInactivePoint.Get(&e.settings.SV)
	adjustmentDeltaPerSecond := elasticCPUAdjustmentDeltaPerSecond.Get(&e.settings.SV)
	adjustmentDelta := adjustmentDeltaPerSecond * period.Seconds()
	multiplicativeFactorOnDecrease := elasticCPUMultiplicativeFactorOnDecrease.Get(&e.settings.SV)
	multiplicativeFactorOnInactiveDecrease := elasticCPUMultiplicativeFactorOnInactiveDecrease.Get(&e.settings.SV)

	return schedulerLatencyListenerParams{
		enabled:                                enabled,
		targetP99:                              targetP99,
		minUtilization:                         minUtilization,
		maxUtilization:                         maxUtilization,
		inactivePoint:                          inactivePoint,
		adjustmentDelta:                        adjustmentDelta,
		multiplicativeFactorOnDecrease:         multiplicativeFactorOnDecrease,
		multiplicativeFactorOnInactiveDecrease: multiplicativeFactorOnInactiveDecrease,
	}
}

type schedulerLatencyListenerParams struct {
	enabled                                bool
	targetP99                              time.Duration // target p99 scheduling latency
	minUtilization, maxUtilization         float64       // {floor,ceiling} on per-node CPU % utilization for elastic work
	inactivePoint                          float64       // point between {min,max} utilization we'll decrease to when inactive
	adjustmentDelta                        float64       // adjustment delta for CPU % limit applied elastic work
	multiplicativeFactorOnDecrease         float64       // multiplicative factor applied to additive delta when reducing limit
	multiplicativeFactorOnInactiveDecrease float64       // multiplicative factor applied to additive delta when reducing limit due to inactivity
}

var ( // cluster settings to control how elastic CPU % is adjusted
	elasticCPUMaxUtilization = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.max_utilization",
		"sets the ceiling on per-node elastic work CPU % utilization",
		0.75, // 75%
		settings.FloatInRange(0.05, 1.0),
	)

	elasticCPUMinUtilization = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.min_utilization",
		"sets the floor on per-node elastic work CPU % utilization",
		0.05, // 5%
		settings.FloatInRange(0.01, 1.0),
	)

	elasticCPUInactivePoint = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.inactive_point",
		"the point between {min,max}_utilization the CPU % decreases to when there's no elastic work",
		0.10, // 10% of the way between {min,max} utilization -- 12% if [min,max] = [5%,75%]
		settings.Fraction,
	)

	elasticCPUAdjustmentDeltaPerSecond = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.adjustment_delta_per_second",
		"sets the per-second % adjustment used when when adapting elastic work CPU %s",
		0.001, // 0.1%, takes 10s to add 1% to elastic CPU limit
		settings.FloatInRange(0.0001, 1.0),
	)

	elasticCPUMultiplicativeFactorOnDecrease = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.multiplicative_factor_on_decrease",
		"sets the multiplier on negative adjustments to elastic work CPU %",
		2, // 2 * 0.1%, takes 5s to subtract 1% from elastic CPU limit
	)

	elasticCPUMultiplicativeFactorOnInactiveDecrease = settings.RegisterFloatSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.multiplicative_factor_on_inactive_decrease",
		"sets the multiplier on negative adjustments to elastic work CPU % when inactive",
		0.25, // 0.25 * 0.1%, takes 40s to subtract 1% from elastic CPU limit
	)

	elasticCPUSchedulerLatencyTarget = settings.RegisterDurationSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.scheduler_latency_target",
		"sets the p99 scheduling latency the elastic CPU controller aims for",
		time.Millisecond,
		settings.DurationInRange(50*time.Microsecond, time.Second),
	)
)

var (
	p99SchedulerLatency = metric.Metadata{
		Name:        "admission.scheduler_latency_listener.p99_nanos",
		Help:        "The scheduling latency at p99 as observed by the scheduler latency listener",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// schedulerLatencyListenerMetrics are the metrics associated with an instance
// of the schedulerLatencyListener.
type schedulerLatencyListenerMetrics struct {
	P99SchedulerLatency *metric.Gauge
}

func makeSchedulerLatencyListenerMetrics() *schedulerLatencyListenerMetrics {
	return &schedulerLatencyListenerMetrics{
		P99SchedulerLatency: metric.NewGauge(p99SchedulerLatency),
	}
}

// MetricStruct implements the metric.Struct interface.
func (k *schedulerLatencyListenerMetrics) MetricStruct() {}

func clamp(min, max, val float64) float64 {
	if buildutil.CrdbTestBuild && min > max {
		log.Dev.Fatalf(context.Background(), "min (%f) > max (%f)", min, max)
	}
	if val < min {
		val = min // floor
	}
	if val > max {
		val = max // ceiling
	}
	return val
}
