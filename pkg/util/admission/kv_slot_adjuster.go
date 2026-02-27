// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// KVSlotAdjusterOverloadThreshold sets a goroutine runnable threshold at
// which the CPU will be considered overloaded, when running in a node that
// executes KV operations.
var KVSlotAdjusterOverloadThreshold = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"admission.kv_slot_adjuster.overload_threshold",
	"when the number of runnable goroutines per CPU is greater than this threshold, the "+
		"slot adjuster considers the cpu to be overloaded",
	32, settings.PositiveInt)

// kvSlotAdjuster is an implementer of CPULoadListener and
// cpuOverloadIndicator.
type kvSlotAdjuster struct {
	settings *cluster.Settings // 集群配置
	granter  *slotGranter      // 关联的 slotGranter（KVWork）

	// ========== 边界约束 ==========
	minCPUSlots int // 最小 slots（默认 1）
	maxCPUSlots int // 最大 slots（默认 100,000）

	// ========== Metrics（可观测性）==========
	totalSlotsMetric                 *metric.Gauge   // 当前 totalSlots
	cpuLoadShortPeriodDurationMetric *metric.Counter // 短周期采样时长统计
	cpuLoadLongPeriodDurationMetric  *metric.Counter // 长周期采样时长统计
	slotAdjusterIncrementsMetric     *metric.Counter // slot 增加次数
	slotAdjusterDecrementsMetric     *metric.Counter // slot 减少次数
}

// 接口实现： 如果没实现的话在编译阶段就会报错
var _ cpuOverloadIndicator = &kvSlotAdjuster{}
var _ CPULoadListener = &kvSlotAdjuster{}

// 接收 CPU 负载信号
func (kvsa *kvSlotAdjuster) CPULoad(
	runnable int, // 可运行 goroutine 数量
	procs int, // GOMAXPROCS
	samplePeriod time.Duration, // 采样周期
) {
	//每个cpu可以运行的goroutine数量，默认是32
	threshold := int(KVSlotAdjusterOverloadThreshold.Get(&kvsa.settings.SV))
	//默认的cpu采样周期，1ms，大于这个值认为cpu已经因为延迟导致采样周期变长
	periodDurationMicros := samplePeriod.Microseconds()
	if samplePeriod > time.Millisecond {
		kvsa.cpuLoadLongPeriodDurationMetric.Inc(periodDurationMicros)
	} else {
		kvsa.cpuLoadShortPeriodDurationMetric.Inc(periodDurationMicros)
	}

	// Simple heuristic, which worked ok in experiments. More sophisticated ones
	// could be devised.
	usedSlots := kvsa.granter.usedSlots
	tryDecreaseSlots := func(total int, adjustMetric bool) int {
		// 条件 1: usedSlots > 0
		// 原因: 如果没有 slot 在用，说明没有负载，不需要减少
		if usedSlots > 0 &&

			// 条件 2: total > kvsa.minCPUSlots
			// 原因: 保证至少有 minCPUSlots（默认 1）个 slot
			total > kvsa.minCPUSlots &&

			// 条件 3: usedSlots ≤ total (关键！)
			// 原因: 如果 usedSlots > total，说明之前的减少还未生效
			//      （有请求 bypass 或正在处理中）
			//      此时继续减少会导致过度反应
			usedSlots <= total {

			total-- // 减少 1 个 slot

			if adjustMetric {
				kvsa.slotAdjusterDecrementsMetric.Inc(1)
			}
		}
		return total
	}
	tryIncreaseSlots := func(total int, adjustMetric bool) int {
		// 条件 1: usedSlots >= total (slots 被用满或接近用满)
		// 原因: 只有需求饱和时才增加供给
		if usedSlots >= total &&

			// 条件 2: total < kvsa.maxCPUSlots
			// 原因: 不超过上限（默认 100,000）
			total < kvsa.maxCPUSlots {

			total++ // 增加 1 个 slot

			if adjustMetric {
				kvsa.slotAdjusterIncrementsMetric.Inc(1)
			}
		}
		return total
	}
	//- IO-bound 负载：slots 会持续增长到数千
	//- 因为 goroutine 大量阻塞在 IO，usedSlots 总是满的
	//- 但 runnable goroutines 很少（大部分在等待 IO）
	//- kvSlotAdjuster 会持续增加 slots
	//- CPU-bound 负载：可以每秒减少 1000 个 slots
	//- 1ms 间隔 × 1000 次/秒 = 1000 slots/秒
	//- 快速响应负载切换
	if runnable >= threshold*procs {
		// Overloaded.
		kvsa.granter.setTotalSlotsLocked(
			tryDecreaseSlots(kvsa.granter.totalSlots, true))
	} else if float64(runnable) <= float64((threshold*procs)/2) {
		// Underloaded -- can afford to increase regular slots.
		kvsa.granter.setTotalSlotsLocked(
			tryIncreaseSlots(kvsa.granter.totalSlots, true))
	}

	kvsa.totalSlotsMetric.Update(int64(kvsa.granter.totalSlots))
}

// 检查 CPU 是否过载
func (kvsa *kvSlotAdjuster) isOverloaded() bool {
	//已使用的槽位大于所有的槽位，并且没有因为负载过重强制跳过准入校验
	return kvsa.granter.usedSlots >= kvsa.granter.totalSlots && !kvsa.granter.skipSlotEnforcement
}
