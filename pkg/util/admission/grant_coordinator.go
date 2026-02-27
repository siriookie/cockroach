// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// GrantCoordinators holds {regular,elastic} GrantCoordinators for
// {regular,elastic} work, and a StoreGrantCoordinators that allows for
// per-store GrantCoordinators for KVWork that involves writes.
// 三个协调器的分工：
// ┌─────────────────────────────────────────────────────┐
// │             GrantCoordinators (容器)                 │
// ├─────────────────────────────────────────────────────┤
// │                                                     │
// │  ┌─────────────────────────────────────────────┐    │
// │  │ RegularCPU (GrantCoordinator)               │    │
// │  │ - 管理 CPU-bound regular work                │    │
// │  │ - WorkKinds: KVWork, SQLKVResponseWork,     │    │
// │  │              SQLSQLResponseWork             │    │
// │  │ - Grant Chain: Enabled                      │    │
// │  └─────────────────────────────────────────────┘    │
// │                                                     │
// │  ┌─────────────────────────────────────────────┐    │
// │  │ ElasticCPU (ElasticCPUGrantCoordinator)     │    │
// │  │ - 管理 CPU-bound elastic work                │    │
// │  │ - 低优先级后台任务                             │    │
// │  │ - 可以被 RegularCPU 抢占                      │    │
// │  └─────────────────────────────────────────────┘    │
// │                                                     │
// │  ┌─────────────────────────────────────────────┐    │
// │  │ Stores (StoreGrantCoordinators)             │    │
// │  │ - 管理每个 Store 的 IO-bound work             │    │
// │  │ - 每个 Store 有独立的 GrantCoordinator        │    │
// │  │ - Grant Chain: Disabled (IO 不需要)          │    │
// │  └─────────────────────────────────────────────┘    │
// └─────────────────────────────────────────────────────┘
// 写入请求的双重准入：
//
//  1. Store 层准入（IO 资源）：
//     ├─► StoreGrantCoordinators.GetWorkQueue(storeID, regularOrElastic)
//     ├─► StoreWorkQueue.Admit(...)
//     │   ├─► 检查 IO tokens
//     │   ├─► 检查 L0 容量
//     │   └─► 成功 or 排队等待
//     └─► 获得 IO 准入
//
//  2. CPU 层准入（CPU 资源）：
//     ├─► RegularCPU.GetWorkQueue(KVWork)
//     ├─► WorkQueue.Admit(...)
//     │   ├─► 检查 CPU slots
//     │   └─► 成功 or 排队等待
//     └─► 获得 CPU 准入
//
//  3. 执行工作：
//     └─► 写入 Pebble
//
//  4. 释放资源：
//     ├─► WorkQueue.AdmittedWorkDone() (CPU)
//     └─► StoreWorkQueue.AdmittedWorkDone() (IO)
//
// 为什么这样设计？
// - 先检查 IO 资源，避免占用 CPU slots 后发现 IO 不足
// - IO 准入和 CPU 准入独立，可以并行调整
// - 每层专注于自己的资源管理
// ┌─────────────────────────────────────────────────────────────┐
// │                    CockroachDB Node                          │
// └─────────────────────────────────────────────────────────────┘
//
//	│
//	├─► Server 初始化（pkg/server/node.go）
//	│   └─► NewGrantCoordinators()
//	│       └─► 创建 GrantCoordinators 容器
//	│
//	↓
//
// ┌─────────────────────────────────────────────────────────────┐
// │              GrantCoordinators（容器结构体）                  │
// │  源代码位置: grant_coordinator.go:82-86                       │
// ├─────────────────────────────────────────────────────────────┤
// │                                                               │
// │  type GrantCoordinators struct {                             │
// │      RegularCPU *GrantCoordinator           // CPU 层        │
// │      ElasticCPU *ElasticCPUGrantCoordinator // CPU 层（低优先）│
// │      Stores     *StoreGrantCoordinators     // IO 层（每 Store）│
// │  }                                                            │
// └─────────────────────────────────────────────────────────────┘
//
//	│
//	├──────────────────┬──────────────────┬──────────────────
//	│                  │                  │
//	↓                  ↓                  ↓
//
// RegularCPU      ElasticCPU         Stores
// GrantCoordinator GrantCoordinator   StoreGrantCoordinators
//
//	│                  │                  │
//	│                  │                  └─► Store 1, 2, ..., N
//	│                  │                      (每个 Store 独立)
//	↓                  ↓
//
// 管理 3 种 WorkKind:  管理弹性 CPU work
// - KVWork            - 低优先级后台任务
// - SQLKVResponseWork - 可被 RegularCPU 抢占
// - SQLSQLResponseWork
type GrantCoordinators struct {
	RegularCPU *GrantCoordinator           // Regular work 的 CPU 协调器
	ElasticCPU *ElasticCPUGrantCoordinator // Elastic work 的 CPU 协调器
	Stores     *StoreGrantCoordinators     // 每个 Store 的协调器
}

// Close implements the stop.Closer interface.
func (gcs GrantCoordinators) Close() {
	gcs.Stores.close()
	gcs.RegularCPU.Close()
	gcs.ElasticCPU.close()
}

// GrantCoordinator is the top-level object that coordinates grants across
// different WorkKinds (for more context see the comment in admission.go, and
// the comment where WorkKind is declared). Typically there will be one
// GrantCoordinator in a node for CPU intensive regular work, and for nodes that
// also have the KV layer, one GrantCoordinator per store (these are managed by
// StoreGrantCoordinators) for KVWork that uses that store. See the
// NewGrantCoordinators and NewGrantCoordinatorSQL functions.
type GrantCoordinator struct {
	ambientCtx log.AmbientContext

	settings *cluster.Settings

	// mu is ordered before any mutex acquired in a requester implementation.
	mu struct {
		syncutil.Mutex
		// grantChainActive indicates whether a grant chain is active. If active,
		// grantChainID is the ID of that chain. If !active, grantChainID is the ID
		// of the next chain that will become active. IDs are assigned by
		// incrementing grantChainID. If !useGrantChains, grantChainActive is never
		// true.
		// ===== Grant Chain 状态 =====
		//### Grant Chain 是什么？
		//**Grant Chain（授权链）** 是一种批量授权优化机制：
		//1. **批量授权**：一次授权多个请求（默认 `numProcs * multiplier` 个）
		//2. **链式延续**：最后一个被授权的请求负责触发下一轮授权
		//3. **自然节流**：利用 goroutine 调度器的能力，避免过度授权
		grantChainActive bool         // Grant Chain 是否活跃
		grantChainID     grantChainID // 当前或下一个 Chain ID
		// Index into granters, which represents the current WorkKind at which the
		// grant chain is operating. Only relevant when grantChainActive is true.
		grantChainIndex WorkKind // 当前处理的 WorkKind 索引
		// See the comment at delayForGrantChainTermination for motivation.
		grantChainStartTime time.Time // Chain 开始时间

		// The cpu fields can be nil, and the IO field below (ioLoadListener)
		// can be nil, since a GrantCoordinator typically handles one of these
		// two resources.
		// ===== CPU 负载监听 =====
		cpuOverloadIndicator cpuOverloadIndicator // CPU 过载指示器
		cpuLoadListener      CPULoadListener      // CPU 负载监听器

		// The latest value of GOMAXPROCS, received via CPULoad. Only initialized if
		// the cpu resource is being handled by this GrantCoordinator.
		numProcs int // GOMAXPROCS
	}

	lastCPULoadSamplePeriod time.Duration // 上次 CPU 采样周期

	// ===== Granter-Requester 对 =====
	// NB: Some granters can be nil.
	// None of the references are changing, so mu protection is unnecessary
	granters [numWorkKinds]granterWithLockedCalls // 资源授予器数组
	// The WorkQueues behaving as requesters in each granterWithLockedCalls.
	// This is kept separately only to service GetWorkQueue calls and to call
	// close().
	queues [numWorkKinds]requesterClose // WorkQueue 数组

	// ===== 配置 =====
	// See the comment at continueGrantChain that explains how a grant chain
	// functions and the motivation. When !useGrantChains, grant chains are
	// disabled.
	useGrantChains bool // 是否启用 Grant Chain
	// The admission control code needs high sampling frequency of the cpu load,
	// and turns off admission control enforcement when the sampling frequency
	// is too low. For testing queueing behavior, we do not want the enforcement
	// to be turned off in a non-deterministic manner so add a testing flag to
	// disable that feature. False in production.
	//
	// TODO(irfansharif): Fold into the testing knobs struct below.
	testingDisableSkipEnforcement bool

	knobs *TestingKnobs
}

var _ CPULoadListener = &GrantCoordinator{}

// Options for constructing GrantCoordinators.
type Options struct {
	MinCPUSlots                   int
	MaxCPUSlots                   int
	SQLKVResponseBurstTokens      int64
	SQLSQLResponseBurstTokens     int64
	TestingDisableSkipEnforcement bool
	// Only non-nil for tests.
	makeRequesterFunc      makeRequesterFunc
	makeStoreRequesterFunc makeStoreRequesterFunc
}

var _ base.ModuleTestingKnobs = &Options{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*Options) ModuleTestingKnobs() {}

// DefaultOptions are the default settings for various admission control knobs.
var DefaultOptions = Options{
	MinCPUSlots:               1,
	MaxCPUSlots:               100000, /* TODO(sumeer): add cluster setting */
	SQLKVResponseBurstTokens:  100000, /* TODO(sumeer): add cluster setting */
	SQLSQLResponseBurstTokens: 100000, /* TODO(sumeer): add cluster setting */
}

// Override applies values from "override" to the receiver that differ from Go
// defaults.
func (o *Options) Override(override *Options) {
	if override.MinCPUSlots != 0 {
		o.MinCPUSlots = override.MinCPUSlots
	}
	if override.MaxCPUSlots != 0 {
		o.MaxCPUSlots = override.MaxCPUSlots
	}
	if override.SQLKVResponseBurstTokens != 0 {
		o.SQLKVResponseBurstTokens = override.SQLKVResponseBurstTokens
	}
	if override.SQLSQLResponseBurstTokens != 0 {
		o.SQLSQLResponseBurstTokens = override.SQLSQLResponseBurstTokens
	}
	if override.TestingDisableSkipEnforcement {
		o.TestingDisableSkipEnforcement = true
	}
}

type makeRequesterFunc func(
	_ log.AmbientContext, workKind WorkKind, granter granter, settings *cluster.Settings,
	metrics *WorkQueueMetrics, opts workQueueOptions) requester

// NewGrantCoordinators constructs GrantCoordinators and WorkQueues for a
// node. Caller is responsible for:
// - hooking up GrantCoordinators.RegularCPU to receive calls to CPULoad, and
// - to set a PebbleMetricsProvider on GrantCoordinators.Stores
//
// Regular and elastic requests pass through GrantCoordinators.{Regular,Elastic}
// respectively, and a subset of requests pass through each store's
// GrantCoordinator. We arrange these such that requests (that need to) first
// pass through a store's GrantCoordinator and then through the
// {regular,elastic} one. This ensures that we are not using slots/elastic CPU
// tokens in the latter level on requests that are blocked elsewhere for
// admission. Additionally, we don't want the CPU scheduler signal that is
// implicitly used in grant chains to delay admission through the per store
// GrantCoordinators since they are not trying to control CPU usage, so we turn
// off grant chaining in those coordinators.
// ┌─────────────────────────────────────────────────────┐
// │              第一层：GrantCoordinators               │
// │  (顶层容器，管理多个 GrantCoordinator 实例)           │
// ├─────────────────────────────────────────────────────┤
// │                                                       │
// │  ┌─────────────────┐  ┌──────────────────────────┐  │
// │  │ RegularCPU      │  │ Stores                   │  │
// │  │ (regular work)  │  │ (每个 Store 一个         │  │
// │  │                 │  │  GrantCoordinator)       │  │
// │  └─────────────────┘  └──────────────────────────┘  │
// │  ┌─────────────────┐                                 │
// │  │ ElasticCPU      │                                 │
// │  │ (elastic work)  │                                 │
// │  └─────────────────┘                                 │
// └─────────────────────────────────────────────────────┘
//
//	↓                              ↓
//
// ┌─────────────────────────┐  ┌─────────────────────────┐
// │   第二层：GrantCoordinator│  │  StoreGrantCoordinators │
// │   (单个协调器实例)        │  │  (管理多个 Store)        │
// ├─────────────────────────┤  ├─────────────────────────┤
// │ - granters[numWorkKinds]│  │ - 每个 Store 有独立的   │
// │ - queues[numWorkKinds]  │  │   GrantCoordinator      │
// │ - Grant Chain 状态      │  │ - IOLoadListener        │
// │ - CPU Load Listener     │  │ - Per-Store metrics     │
// └─────────────────────────┘  └─────────────────────────┘
//
//	↓
//
// ┌─────────────────────────────────────────────────────┐
// │        第三层：Granter + WorkQueue 对                │
// │        (每个 WorkKind 一对)                          │
// ├─────────────────────────────────────────────────────┤
// │                                                       │
// │  ┌──────────────┐          ┌──────────────┐          │
// │  │ slotGranter  │◄────────►│ WorkQueue    │          │
// │  │ (KVWork)     │          │ (KVWork)     │          │
// │  └──────────────┘          └──────────────┘          │
// │                                                       │
// │  ┌──────────────┐          ┌──────────────┐          │
// │  │ tokenGranter │◄────────►│ WorkQueue    │          │
// │  │(SQLKVResp..) │          │(SQLKVResp..) │          │
// │  └──────────────┘          └──────────────┘          │
// │                                                       │
// │  ┌──────────────┐          ┌──────────────┐          │
// │  │ tokenGranter │◄────────►│ WorkQueue    │          │
// │  │(SQLSQLResp..)│          │(SQLSQLResp..)│          │
// │  └──────────────┘          └──────────────┘          │
// └─────────────────────────────────────────────────────┘
// 初始化时间线:
// T=0: Server 启动
//
//	↓
//
// pkg/server/node.go: Node.Start()
//
//	↓
//
// 创建 GrantCoordinators
//
//	↓
//
// ┌─────────────────────────────────────────────────────────────┐
// │ 步骤 1: 创建 StoreGrantCoordinators                          │
// └─────────────────────────────────────────────────────────────┘
//
//	↓
//
// makeStoresGrantCoordinators()
//
//	├─ 创建空的 gcMap (稍后动态添加 Store)
//	└─ 返回 *StoreGrantCoordinators
//
// ┌─────────────────────────────────────────────────────────────┐
// │ 步骤 2: 创建 RegularCPU GrantCoordinator                     │
// └─────────────────────────────────────────────────────────────┘
//
//	↓
//
// makeRegularGrantCoordinator()
//
//	├─ 创建 kvSlotAdjuster (CPU 负载监听器)
//	│   ├─ minCPUSlots = 1
//	│   ├─ maxCPUSlots = 100,000
//	│   └─ threshold = 32 (每核可运行 goroutine 阈值)
//	│
//	├─ 创建 GrantCoordinator
//	│   ├─ useGrantChains = true
//	│   ├─ mu.cpuLoadListener = kvSlotAdjuster
//	│   └─ mu.cpuOverloadIndicator = kvSlotAdjuster
//	│
//	├─ 为 3 个 WorkKind 创建 granter-requester 对:
//	│   ├─ KVWork:
//	│   │   ├─ granter = slotGranter (slot-based)
//	│   │   └─ requester = WorkQueue
//	│   │
//	│   ├─ SQLKVResponseWork:
//	│   │   ├─ granter = tokenGranter (token-based)
//	│   │   └─ requester = WorkQueue
//	│   │
//	│   └─ SQLSQLResponseWork:
//	│       ├─ granter = tokenGranter (token-based)
//	│       └─ requester = WorkQueue
//	│
//	└─ 返回 *GrantCoordinator
//
// ┌─────────────────────────────────────────────────────────────┐
// │ 步骤 3: 创建 ElasticCPU GrantCoordinator                     │
// └─────────────────────────────────────────────────────────────┘
//
//	↓
//
// makeElasticCPUGrantCoordinator()
//
//	├─ 创建 elasticCPUGranter (token-based)
//	├─ 创建 schedulerLatencyListener
//	│   └─ 监听 p99 调度延迟
//	│
//	├─ 创建 ElasticCPUWorkQueue
//	└─ 返回 *ElasticCPUGrantCoordinator
//
// T=100ms: goschedstats 开始工作
//
//	↓
//
// 每 1ms 调用 RegularCPU.CPULoad(runnable, procs, 1ms)
//
//	├─ 传递给 kvSlotAdjuster
//	└─ 动态调整 KVWork 的 totalSlots
//
// T=1s: Stores 开始接收 Pebble metrics
//
//	↓
//
// SetPebbleMetricsProvider(pebbleMetricsProvider)
//
//	↓
//
// 为每个 Store 创建 storeGrantCoordinator
//
//	├─ Store 1 → gcMap[1] = storeGrantCoordinator{...}
//	├─ Store 2 → gcMap[2] = storeGrantCoordinator{...}
//	└─ Store N → gcMap[N] = storeGrantCoordinator{...}
//	↓
//
// 启动后台 goroutine，每 1s:
//
//	├─ 调用 pebbleMetricsTick() (更新 L0 metrics)
//	└─ 调用 allocateIOTokensTick() (补充 IO tokens)
func NewGrantCoordinators(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	opts Options,
	registry *metric.Registry,
	onLogEntryAdmitted OnLogEntryAdmitted,
	knobs *TestingKnobs,
) GrantCoordinators {
	metrics := makeGrantCoordinatorMetrics()
	registry.AddMetricStruct(metrics)

	if knobs == nil {
		knobs = &TestingKnobs{}
	}

	return GrantCoordinators{
		Stores:     makeStoresGrantCoordinators(ambientCtx, opts, st, onLogEntryAdmitted, knobs),
		RegularCPU: makeRegularGrantCoordinator(ambientCtx, opts, st, metrics, registry, knobs),
		ElasticCPU: makeElasticCPUGrantCoordinator(ambientCtx, st, registry),
	}
}

func makeRegularGrantCoordinator(
	ambientCtx log.AmbientContext,
	opts Options,
	st *cluster.Settings,
	metrics GrantCoordinatorMetrics,
	registry *metric.Registry,
	knobs *TestingKnobs,
) *GrantCoordinator {
	makeRequester := makeWorkQueue
	if opts.makeRequesterFunc != nil {
		makeRequester = opts.makeRequesterFunc
	}

	kvSlotAdjuster := &kvSlotAdjuster{
		settings:                         st,
		minCPUSlots:                      opts.MinCPUSlots,
		maxCPUSlots:                      opts.MaxCPUSlots,
		totalSlotsMetric:                 metrics.KVTotalSlots,
		cpuLoadShortPeriodDurationMetric: metrics.KVCPULoadShortPeriodDuration,
		cpuLoadLongPeriodDurationMetric:  metrics.KVCPULoadLongPeriodDuration,
		slotAdjusterIncrementsMetric:     metrics.KVSlotAdjusterIncrements,
		slotAdjusterDecrementsMetric:     metrics.KVSlotAdjusterDecrements,
	}
	coord := &GrantCoordinator{
		ambientCtx:                    ambientCtx,
		settings:                      st,
		useGrantChains:                true,
		testingDisableSkipEnforcement: opts.TestingDisableSkipEnforcement,
		knobs:                         knobs,
	}
	coord.mu.grantChainID = 1
	coord.mu.cpuOverloadIndicator = kvSlotAdjuster
	coord.mu.cpuLoadListener = kvSlotAdjuster
	coord.mu.numProcs = 1

	kvg := &slotGranter{
		coord:                        coord,
		workKind:                     KVWork,
		totalSlots:                   opts.MinCPUSlots,
		skipSlotEnforcement:          !goschedstats.Supported,
		usedSlotsMetric:              metrics.KVUsedSlots,
		slotsExhaustedDurationMetric: metrics.KVSlotsExhaustedDuration,
	}

	kvSlotAdjuster.granter = kvg
	wqMetrics := makeWorkQueueMetrics(KVWork.String(), registry, admissionpb.NormalPri, admissionpb.LockingNormalPri)
	req := makeRequester(ambientCtx, KVWork, kvg, st, wqMetrics, makeWorkQueueOptions(KVWork))
	coord.queues[KVWork] = req
	kvg.requester = req
	coord.granters[KVWork] = kvg

	tg := &tokenGranter{
		coord:                coord,
		workKind:             SQLKVResponseWork,
		availableBurstTokens: opts.SQLKVResponseBurstTokens,
		maxBurstTokens:       opts.SQLKVResponseBurstTokens,
		cpuOverload:          kvSlotAdjuster,
	}
	wqMetrics = makeWorkQueueMetrics(SQLKVResponseWork.String(), registry, admissionpb.NormalPri, admissionpb.LockingNormalPri)
	req = makeRequester(
		ambientCtx, SQLKVResponseWork, tg, st, wqMetrics, makeWorkQueueOptions(SQLKVResponseWork))
	coord.queues[SQLKVResponseWork] = req
	tg.requester = req
	coord.granters[SQLKVResponseWork] = tg

	tg = &tokenGranter{
		coord:                coord,
		workKind:             SQLSQLResponseWork,
		availableBurstTokens: opts.SQLSQLResponseBurstTokens,
		maxBurstTokens:       opts.SQLSQLResponseBurstTokens,
		cpuOverload:          kvSlotAdjuster,
	}
	wqMetrics = makeWorkQueueMetrics(SQLSQLResponseWork.String(), registry, admissionpb.NormalPri, admissionpb.LockingNormalPri)
	req = makeRequester(ambientCtx,
		SQLSQLResponseWork, tg, st, wqMetrics, makeWorkQueueOptions(SQLSQLResponseWork))
	coord.queues[SQLSQLResponseWork] = req
	tg.requester = req
	coord.granters[SQLSQLResponseWork] = tg

	return coord
}

// GetWorkQueue returns the WorkQueue for a particular WorkKind. Can be nil if
// the NewGrantCoordinator* function does not construct a WorkQueue for that
// work.
// Implementation detail: don't use this method when the GrantCoordinator is
// created by the StoreGrantCoordinators since those have a StoreWorkQueues.
// The TryGetQueueForStore is the external facing method in that case since
// the individual GrantCoordinators are hidden.
func (coord *GrantCoordinator) GetWorkQueue(workKind WorkKind) *WorkQueue {
	return coord.queues[workKind].(*WorkQueue)
}

// CPULoad implements CPULoadListener and is called periodically (see
// CPULoadListener for details). The same frequency is used for refilling the
// burst tokens since synchronizing the two means that the refilled burst can
// take into account the latest schedulers stats (indirectly, via the
// implementation of cpuOverloadIndicator).
// 每次cpu采样的callback调用的时候就会去触发准入和填token
func (coord *GrantCoordinator) CPULoad(runnable int, procs int, samplePeriod time.Duration) {
	ctx := coord.ambientCtx.AnnotateCtx(context.Background())

	if log.V(1) {
		if coord.lastCPULoadSamplePeriod != 0 && coord.lastCPULoadSamplePeriod != samplePeriod &&
			KVAdmissionControlEnabled.Get(&coord.settings.SV) {
			log.Dev.Infof(ctx, "CPULoad switching to period %s", samplePeriod.String())
		}
	}
	coord.lastCPULoadSamplePeriod = samplePeriod

	coord.mu.Lock()
	defer coord.mu.Unlock()
	// 传递给 CPU Load Listener（通常是 kvSlotAdjuster）
	coord.mu.numProcs = procs // 更新 GOMAXPROCS
	coord.mu.cpuLoadListener.CPULoad(runnable, procs, samplePeriod)

	// Slot adjustment and token refilling requires 1ms periods to work well. If
	// the CPULoad ticks are less frequent, there is no guarantee that the
	// tokens or slots will be sufficient to service requests. This is
	// particularly the case for slots where we dynamically adjust them, and
	// high contention can suddenly result in high slot utilization even while
	// cpu utilization stays low. We don't want to artificially bottleneck
	// request processing when we are in this slow CPULoad ticks regime since we
	// can't adjust slots or refill tokens fast enough. So we explicitly tell
	// the granters to not do token or slot enforcement.
	skipEnforcement := samplePeriod > time.Millisecond || !goschedstats.Supported
	// 刷新 token granters（周期性补充 tokens）
	coord.granters[SQLKVResponseWork].(*tokenGranter).refillBurstTokens(skipEnforcement)
	coord.granters[SQLSQLResponseWork].(*tokenGranter).refillBurstTokens(skipEnforcement)
	if coord.testingDisableSkipEnforcement {
		// This testing option only applies to KV work.
		skipEnforcement = false
	}
	kvg := coord.granters[KVWork].(*slotGranter)
	kvg.skipSlotEnforcement = skipEnforcement
	// 尝试授权等待的请求
	if coord.mu.grantChainActive && !coord.tryTerminateGrantChain() {
		return
	}
	coord.tryGrantLocked()
}

// 调用链：
// Requester (WorkQueue):
//
//	├─► tryGet() 快速路径
//	│   └─► coord.tryGet(workKind, count, demuxHandle)
//	│       ├─► coord.mu.Lock()
//	│       ├─► granter.tryGetLocked(count, demuxHandle)
//	│       │   ├─► slotGranter: 检查 usedSlots < totalSlots
//	│       │   └─► tokenGranter: 检查 availableBurstTokens > 0
//	│       ├─► 根据 grantResult 决定是否终止 Grant Chain
//	│       └─► coord.mu.Unlock()
//	└─► 如果失败 → 慢路径（入队）
//
// tryGet is called by granter.tryGet with the WorkKind.
func (coord *GrantCoordinator) tryGet(
	workKind WorkKind, count int64, demuxHandle int8,
) (granted bool) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	// It is possible that a grant chain is active, and has not yet made its way
	// to this workKind. So it may be more reasonable to queue. But we have some
	// concerns about incurring the delay of multiple goroutine context switches
	// so we ignore this case.
	// 尝试获取资源
	res := coord.granters[workKind].tryGetLocked(count, demuxHandle)
	switch res {
	case grantSuccess:
		// 授权成功
		// Grant chain may be active, but it did not get in the way of this grant,
		// and the effect of this grant in terms of overload will be felt by the
		// grant chain.
		return true
	case grantFailDueToSharedResource:
		// CPU 过载，终止 Grant Chain
		// This could be a transient overload, that may not be noticed by the
		// grant chain. We don't want it to continue granting to lower priority
		// WorkKinds, while a higher priority one is waiting, so we terminate it.
		if coord.mu.grantChainActive && coord.mu.grantChainIndex >= workKind {
			coord.tryTerminateGrantChain()
		}
		return false
	case grantFailLocal:
		// 本地资源不足（slots/tokens 耗尽）
		return false
	default:
		panic(errors.AssertionFailedf("unknown grantResult"))
	}
}

// returnGrant（归还资源）
// **决策逻辑**：
//
// ```
// returnGrant 被调用：
//
// Grant Chain 活跃？
// ├─► No → 直接调用 tryGrantLocked() 尝试授权
// │
// └─► Yes → 检查 Grant Chain 的位置
//
//	├─► grantChainIndex <= workKind
//	│   └─► Grant Chain 会处理这个 WorkKind → 不做任何事
//	│
//	└─► grantChainIndex > workKind
//	    ├─► 该 WorkKind 有等待请求？
//	    │   ├─► Yes → 终止 Grant Chain，重新授权
//	    │   └─► No → 不做任何事
//	    │
//	    └─► 终止成功？
//	        ├─► Yes → 调用 tryGrantLocked()
//	        └─► No → 返回（等待 Grant Chain 自然结束）
//
// ```
//
// returnGrant is called by granter.returnGrant with the WorkKind.
func (coord *GrantCoordinator) returnGrant(workKind WorkKind, count int64, demuxHandle int8) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	// 归还资源
	coord.granters[workKind].returnGrantLocked(count, demuxHandle)
	if coord.mu.grantChainActive {
		// 检查是否需要终止当前 Grant Chain
		if coord.mu.grantChainIndex > workKind &&
			coord.granters[workKind].requesterHasWaitingRequests() {
			// 有高优先级工作在等待，但 Grant Chain 在处理低优先级工作
			// 终止 Chain，重新开始
			// There are waiting requests that will not be served by the grant chain.
			// Better to terminate it and start afresh.
			if !coord.tryTerminateGrantChain() {
				return
			}
		} else {
			// Else either the grant chain will get to this workKind, or there are no waiting requests.
			// Grant Chain 会处理这个 WorkKind，或者没有等待请求
			return
		}
	}
	// 尝试授权等待的请求
	coord.tryGrantLocked()
}

// tookWithoutPermission is called by granter.tookWithoutPermission with the
// WorkKind.
func (coord *GrantCoordinator) tookWithoutPermission(
	workKind WorkKind, count int64, demuxHandle int8,
) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	coord.granters[workKind].tookWithoutPermissionLocked(count, demuxHandle)
}

// continueGrantChain is called by granter.continueGrantChain with the
// WorkKind. Never called if !coord.useGrantChains.
// **Grant Chain 的生命周期**：
//
// ```
//
//  1. 启动 Grant Chain：
//     ├─► returnGrant() or tryGet() 失败后
//     ├─► coord.mu.grantChainActive = false
//     └─► 调用 tryGrantLocked()
//     ├─► 授权 grantBurstLimit 个请求
//     ├─► 最后一个请求获得 grantChainID
//     ├─► coord.mu.grantChainActive = true
//     └─► return
//
//  2. 延续 Grant Chain：
//     ├─► 最后一个被授权的 goroutine 运行
//     ├─► 调用 continueGrantChain(grantChainID)
//     └─► 检查 grantChainID 是否匹配
//     ├─► 匹配 → 调用 tryGrantLocked() 继续授权
//     └─► 不匹配 → Chain 已被终止，退出
//
//  3. 终止 Grant Chain：
//     ├─► tryGet() 检测到 CPU 过载
//     ├─► returnGrant() 检测到高优先级工作等待
//     ├─► 调用 tryTerminateGrantChain()
//     │   ├─► 检查是否可以终止（启动时间 < 100ms？）
//     │   ├─► coord.mu.grantChainID++  ← 关键！让老 Chain 失效
//     │   └─► coord.mu.grantChainActive = false
//     └─► 下次 continueGrantChain() 检测到 ID 不匹配，退出
//
// ```
func (coord *GrantCoordinator) continueGrantChain(_ WorkKind, grantChainID grantChainID) {
	if grantChainID == noGrantChain {
		return
	}
	coord.mu.Lock()
	defer coord.mu.Unlock()
	// 检查 Grant Chain 是否已被终止
	if coord.mu.grantChainID != grantChainID {
		// Chain 已被终止（ID 不匹配）
		// Someone terminated grantChainID by incrementing coord.grantChainID.
		return
	}
	// 继续授权
	coord.tryGrantLocked()
}

// delayForGrantChainTermination causes a delay in terminating a grant chain.
// Terminating a grant chain immediately typically causes a new one to start
// immediately that can burst up to its maximum initial grant burst. Which
// means frequent terminations followed by new starts impose little control
// over the rate at which tokens are granted (slots are better controlled
// since we know when the work finishes). This causes huge spikes in the
// runnable goroutine count, observed at 1ms granularity. This spike causes
// the kvSlotAdjuster to ratchet down the totalSlots for KV work all the way
// down to 1, which later causes the runnable gorouting count to crash down
// to a value close to 0, leading to under-utilization.
//
// TODO(sumeer): design admission behavior metrics that can be used to
// understand the behavior in detail and to quantify improvements when changing
// heuristics. One metric would be mean and variance of the runnable count,
// computed using the 1ms samples, and exported/logged every 60s.
// 立即终止 Grant Chain 通常会导致立即启动新的 Chain，新 Chain 可以 burst
// 到最大初始授权量。这意味着频繁的终止和启动对令牌授予速率控制很少。
// 这导致 runnable goroutine 数量出现巨大尖峰（以 1ms 粒度观察）。这个尖峰
// 导致 kvSlotAdjuster 将 KV 工作的 totalSlots 一路降到 1，后来导致 runnable
// goroutine 数量崩溃到接近 0，导致利用率不足。
// 延迟终止的权衡：
// 不延迟（立即终止）：
// ├─► 优点：快速响应高优先级工作
// └─► 缺点：
//
//	├─► 频繁启动/终止 Grant Chain
//	├─► runnable count 剧烈波动
//	├─► kvSlotAdjuster 误判，将 slots 降到 1
//	└─► CPU 利用率不足
//
// 延迟 100ms：
// ├─► 优点：
// │   ├─► 减少 Grant Chain 启动/终止频率
// │   ├─► runnable count 更稳定
// │   └─► kvSlotAdjuster 更准确
// └─► 缺点：
//
//	├─► 高优先级工作可能延迟 100ms
//	└─► 可接受，因为 100ms << 典型超时（5s+）
//
// 设计选择：
// 优先保证系统稳定性和 CPU 利用率，而不是绝对的优先级响应速度
var delayForGrantChainTermination = 100 * time.Millisecond

// tryTerminateGrantChain attempts to terminate the current grant chain, and
// returns true iff it is terminated, in which case a new one can be
// immediately started.
// REQUIRES: coord.grantChainActive==true
// ### 终止触发条件
// **条件 1：CPU 过载**
// ```go
// // pkg/util/admission/grant_coordinator.go:351
// case grantFailDueToSharedResource:
//
//	// CPU 过载
//	if coord.mu.grantChainActive && coord.mu.grantChainIndex >= workKind {
//	    coord.tryTerminateGrantChain()
//	}
//	return false
//
// ```
// **触发场景**：
// 场景：KVWork 的 tryGet() 失败
//
// slotGranter.tryGetLocked():
// ├─► usedSlots >= totalSlots
// ├─► workKind == KVWork
// └─► return grantFailDueToSharedResource
//
// GrantCoordinator.tryGet():
// ├─► 收到 grantFailDueToSharedResource
// ├─► grantChainActive = true
// ├─► grantChainIndex >= KVWork
// └─► 终止 Grant Chain
//
// 原因：
// - KVWork 是最高优先级
// - 如果连 KVWork 都无法获取资源，说明 CPU 真的过载了
// - 必须停止低优先级工作的授权
// **条件 2：高优先级工作等待**
//
// ```go
// // pkg/util/admission/grant_coordinator.go:372
//
//	if coord.mu.grantChainActive {
//	   if coord.mu.grantChainIndex > workKind &&
//	       coord.granters[workKind].requesterHasWaitingRequests() {
//	       // 有高优先级工作等待，终止 Grant Chain
//	       if !coord.tryTerminateGrantChain() {
//	           return
//	       }
//	   } else {
//	       return
//	   }
//	}
//
// ```
//
// **触发场景**：
//
// ```
// 场景：归还 KVWork slot 时
//
// returnGrant(KVWork, 1):
// ├─► returnGrantLocked(KVWork, 1)
// │   └─► usedSlots--
// │
// ├─► grantChainActive = true
// ├─► grantChainIndex = 2 (SQLSQLResponseWork)
// │   > workKind = 0 (KVWork)
// │
// ├─► KVWork 有等待请求？
// │   └─► Yes
// │
// └─► 终止 Grant Chain，重新从 KVWork 开始授权
//
// 原因：
// - Grant Chain 正在处理低优先级工作（SQLSQLResponseWork）
// - 但高优先级工作（KVWork）有等待请求
// - 应该优先处理 KVWork
// ```
func (coord *GrantCoordinator) tryTerminateGrantChain() bool {
	now := timeutil.Now()
	if delayForGrantChainTermination > 0 &&
		now.Sub(coord.mu.grantChainStartTime) < delayForGrantChainTermination {
		return false // Grant Chain 刚启动不久（< 100ms），暂时不终止
	}
	// Incrementing the ID will cause the existing grant chain to die out when
	// the grantee calls continueGrantChain.
	// 终止 Grant Chain
	coord.mu.grantChainID++ // 增加 ID，让老 Chain 失效
	coord.mu.grantChainActive = false
	coord.mu.grantChainStartTime = time.Time{}
	return true
}

// tryGrantLocked tries to either continue an existing grant chain, or if no grant
// chain is active, tries to start a new grant chain when grant chaining is
// enabled, or grants as much as it can when grant chaining is disabled.
// **流程图**：
//
// ```
// tryGrantLocked() 开始：
//
// grantChainActive?
// ├─► No → startingChain = true, grantChainIndex = 0
// └─► Yes → startingChain = false（延续现有 Chain）
//
// grantBurstCount = 0
// grantBurstLimit = numProcs * multiplier
//
// 循环授权：
// for workKind in [KVWork, SQLKVResponseWork, SQLSQLResponseWork]:
//
//	├─► granter 存在？
//	│   └─► No → 跳过
//	│
//	├─► 有等待请求？
//	│   └─► No → 继续下一个 WorkKind
//	│
//	└─► 循环授权：
//	    ├─► grantBurstCount + 1 == grantBurstLimit?
//	    │   ├─► Yes → chainID = grantChainID（最后一个请求）
//	    │   └─► No → chainID = noGrantChain
//	    │
//	    ├─► res = granter.tryGrantLocked(chainID)
//	    │
//	    └─► switch res:
//	        ├─► grantSuccess:
//	        │   ├─► grantBurstCount++
//	        │   └─► grantBurstCount == grantBurstLimit?
//	        │       ├─► Yes (且 useGrantChains):
//	        │       │   ├─► grantChainActive = true
//	        │       │   ├─► 记录 grantChainStartTime
//	        │       │   └─► return（等待 continueGrantChain）
//	        │       └─► No → 继续授权
//	        │
//	        ├─► grantFailDueToSharedResource:
//	        │   └─► break OuterLoop（停止所有授权）
//	        │
//	        └─► grantFailLocal:
//	            └─► 继续下一个 WorkKind
//
// 所有 WorkKind 处理完毕：
// ├─► grantChainActive = false（Chain 未启动或已结束）
// └─► 如果是延续的 Chain：grantChainID++
// ```
//
// ---
func (coord *GrantCoordinator) tryGrantLocked() {
	startingChain := false
	if !coord.mu.grantChainActive {
		// NB: always set to true when !coord.useGrantChains, and we won't
		// actually use this to start a grant chain (see below).
		startingChain = true
		coord.mu.grantChainIndex = 0
	}
	// Assume that we will not be able to start a new grant chain, or that the
	// existing one will die out. The code below will set it to true if neither
	// is true.
	coord.mu.grantChainActive = false
	grantBurstCount := 0
	// Grant in a burst proportional to numProcs, to generate a runnable for
	// each.
	//假设：
	//- numProcs = 8（8 核 CPU）
	//- KVSlotAdjusterOverloadThreshold = 32
	//- multiplier = 32 / 4 = 8
	//
	//grantBurstLimit = 8 * 8 = 64
	//
	//解释：
	//- 每次授权最多 64 个请求
	//- 目标是为每个 CPU 核心产生足够的 runnable goroutines
	//- multiplier 确保有足够的 burst，提高 CPU 利用率
	//- 但不能过大，否则低优先级工作会挤占 KVWork 的 slots
	grantBurstLimit := coord.mu.numProcs
	// Additionally, increase the burst size proportional to a fourth of the
	// overload threshold. We experimentally observed that this resulted in
	// better CPU utilization. We don't use the full overload threshold since we
	// don't want to over grant for non-KV work since that causes the KV slots
	// to (unfairly) start decreasing, since we lose control over how many
	// goroutines are runnable.
	multiplier := int(KVSlotAdjusterOverloadThreshold.Get(&coord.settings.SV) / 4)
	if multiplier == 0 {
		multiplier = 1
	}
	grantBurstLimit *= multiplier
	// Only the case of a grant chain being active returns from within the
	// OuterLoop.
OuterLoop:
	for ; coord.mu.grantChainIndex < numWorkKinds; coord.mu.grantChainIndex++ {
		localDone := false

		granter := coord.granters[coord.mu.grantChainIndex]
		if granter == nil { // 这个 WorkKind 未配置
			// A GrantCoordinator can be limited to certain WorkKinds, and the
			// remaining will be nil.
			continue
		}
		for granter.requesterHasWaitingRequests() && !localDone {
			chainID := noGrantChain
			// grantChainID（最后一个请求）
			if grantBurstCount+1 == grantBurstLimit && coord.useGrantChains {
				// 达到 burst limit，启动 Grant Chain
				chainID = coord.mu.grantChainID
			}
			res := granter.tryGrantLocked(chainID)
			switch res {
			case grantSuccess:
				grantBurstCount++
				if grantBurstCount == grantBurstLimit && coord.useGrantChains {
					//已经授权了 burst limit 个请求，启动 Grant Chain
					//          │       ├─► Yes (且 useGrantChains):
					//            │       │   ├─► grantChainActive = true
					//            │       │   ├─► 记录 grantChainStartTime
					//            │       │   └─► return（等待 continueGrantChain）
					coord.mu.grantChainActive = true
					if startingChain {
						coord.mu.grantChainStartTime = timeutil.Now()
					}
					return
				}
			case grantFailDueToSharedResource:
				break OuterLoop //（停止所有授权）
			case grantFailLocal:
				localDone = true //继续下一个 WorkKind
			default:
				panic(errors.AssertionFailedf("unknown grantResult"))
			}
		}
	}
	// INVARIANT: !grantChainActive. The chain either did not start or the
	// existing one died. If the existing one died, we increment grantChainID
	// since it represents the ID to be used for the next chain. Note that
	// startingChain is always true when !useGrantChains, so this if-block is
	// not executed.
	// Grant Chain 未启动或已结束，增加 Chain ID
	if !startingChain {
		coord.mu.grantChainID++
	}
}

// Close implements the stop.Closer interface.
func (coord *GrantCoordinator) Close() {
	//    gcs.Stores.close()
	//    gcs.RegularCPU.Close()
	//    gcs.ElasticCPU.close()
	for i := range coord.queues {
		if coord.queues[i] != nil {
			coord.queues[i].close()
		}
	}
}

func (coord *GrantCoordinator) String() string {
	return redact.StringWithoutMarkers(coord)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (coord *GrantCoordinator) SafeFormat(s redact.SafePrinter, _ rune) {
	coord.mu.Lock()
	defer coord.mu.Unlock()
	s.Printf("(chain: id: %d active: %t index: %d)",
		coord.mu.grantChainID, coord.mu.grantChainActive, coord.mu.grantChainIndex,
	)

	spaceStr := redact.RedactableString(" ")
	newlineStr := redact.RedactableString("\n")
	curSep := spaceStr
	for i := range coord.granters {
		kind := WorkKind(i)
		switch kind {
		case KVWork:
			switch g := coord.granters[i].(type) {
			case *slotGranter:
				s.Printf("%s%s: used: %d, total: %d", curSep, kind, g.usedSlots, g.totalSlots)
			default:
				s.Printf("unknown granter")
			}
		case SQLKVResponseWork, SQLSQLResponseWork:
			if coord.granters[i] != nil {
				g := coord.granters[i].(*tokenGranter)
				s.Printf("%s%s: avail: %d", curSep, kind, g.availableBurstTokens)
				if kind == SQLKVResponseWork {
					curSep = newlineStr
				} else {
					curSep = spaceStr
				}
			}
		}
	}
}

// GrantCoordinatorMetrics are metrics associated with a GrantCoordinator.
type GrantCoordinatorMetrics struct {
	KVTotalSlots                 *metric.Gauge   // KV 总 slots
	KVUsedSlots                  *metric.Gauge   // KV 已用 slots
	KVSlotsExhaustedDuration     *metric.Counter // Slots 耗尽时长
	KVCPULoadShortPeriodDuration *metric.Counter // 短周期 CPU load 时长
	KVCPULoadLongPeriodDuration  *metric.Counter // 长周期 CPU load 时长
	KVSlotAdjusterIncrements     *metric.Counter // Slots 增加次数
	KVSlotAdjusterDecrements     *metric.Counter // Slots 减少次数
}

// MetricStruct implements the metric.Struct interface.
func (GrantCoordinatorMetrics) MetricStruct() {}

func makeGrantCoordinatorMetrics() GrantCoordinatorMetrics {
	return GrantCoordinatorMetrics{
		KVTotalSlots:                 metric.NewGauge(totalSlots),
		KVUsedSlots:                  metric.NewGauge(addName(KVWork.String(), usedSlots)),
		KVSlotsExhaustedDuration:     metric.NewCounter(kvSlotsExhaustedDuration),
		KVCPULoadShortPeriodDuration: metric.NewCounter(kvCPULoadShortPeriodDuration),
		KVCPULoadLongPeriodDuration:  metric.NewCounter(kvCPULoadLongPeriodDuration),
		KVSlotAdjusterIncrements:     metric.NewCounter(kvSlotAdjusterIncrements),
		KVSlotAdjusterDecrements:     metric.NewCounter(kvSlotAdjusterDecrements),
	}
}
