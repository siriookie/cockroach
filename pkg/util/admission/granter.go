// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// noGrantChain is a sentinel value representing that the grant is not
// responsible for continuing a grant chain. It is only used internally in
// this file -- requester implementations do not need to concern themselves
// with this value.
var noGrantChain grantChainID = 0

type requesterClose interface {
	close()
}

// slotGranter implements granterWithLockedCalls.
type slotGranter struct {
	coord      *GrantCoordinator
	workKind   WorkKind
	requester  requester
	usedSlots  int // 已使用的槽位数
	totalSlots int // 总槽位数
	// skipSlotEnforcement is a dynamic value that changes based on the sampling
	// period of cpu load. It is always true when !goschedstats.Supported (see
	// https://github.com/cockroachdb/cockroach/issues/142262).
	skipSlotEnforcement bool //是否跳过槽位强制（CPU负载采样周期 > 1ms时为true）

	usedSlotsMetric              *metric.Gauge
	slotsExhaustedDurationMetric *metric.Counter
	exhaustedStart               time.Time //槽位耗尽的起始时间（用于统计）
}

var _ granterWithLockedCalls = &slotGranter{}
var _ granter = &slotGranter{}

// tryGet implements granter.
func (sg *slotGranter) tryGet(_ burstQualification, count int64) bool {
	return sg.coord.tryGet(sg.workKind, count, 0 /*arbitrary*/)
}

// 尝试获取槽位
// tryGetLocked implements granterWithLockedCalls.
func (sg *slotGranter) tryGetLocked(count int64, _ int8) grantResult {
	//槽位必须是1：slots总是以单个单位分配（与tokens不同）
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	//- 正常情况：`usedSlots < totalSlots`
	//- 特殊情况：`skipSlotEnforcement == true`（CPU监控失效时）
	if sg.usedSlots < sg.totalSlots || sg.skipSlotEnforcement {
		sg.usedSlots++
		if sg.usedSlots == sg.totalSlots {
			//精确记录资源耗尽的时刻，用于性能分析
			sg.exhaustedStart = timeutil.Now()
		}
		sg.usedSlotsMetric.Update(int64(sg.usedSlots))
		return grantSuccess
	}
	//- KVWork失败 → `grantFailDueToSharedResource`（因为KVWork是最高优先级，它的失败意味着CPU等共享资源不足）
	//- 其他 → `grantFailLocal`
	if sg.workKind == KVWork {
		return grantFailDueToSharedResource
	}
	return grantFailLocal
}

// returnGrant implements granter.
func (sg *slotGranter) returnGrant(count int64) {
	sg.coord.returnGrant(sg.workKind, count, 0 /*arbitrary*/)
}

// returnGrantLocked implements granterWithLockedCalls.
func (sg *slotGranter) returnGrantLocked(count int64, _ int8) {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	if sg.usedSlots == sg.totalSlots {
		now := timeutil.Now()
		exhaustedMicros := now.Sub(sg.exhaustedStart).Microseconds()
		sg.slotsExhaustedDurationMetric.Inc(exhaustedMicros) // 累计耗尽时长
	}
	sg.usedSlots--
	if sg.usedSlots < 0 {
		panic(errors.AssertionFailedf("used slots is negative %d", sg.usedSlots))
	}
	//实时更新Prometheus指标
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}

// tookWithoutPermission implements granter.
func (sg *slotGranter) tookWithoutPermission(count int64) {
	sg.coord.tookWithoutPermission(sg.workKind, count, 0 /*arbitrary*/)
}

// tookWithoutPermissionLocked implements granterWithLockedCalls.
// - 直接增加usedSlots，不检查是否超过totalSlots
// - 允许usedSlots > totalSlots（过载情况）
// - 这是高优先级工作或避免死锁所必需的
func (sg *slotGranter) tookWithoutPermissionLocked(count int64, _ int8) {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	sg.usedSlots++
	if sg.usedSlots == sg.totalSlots {
		sg.exhaustedStart = timeutil.Now()
	}
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}

// continueGrantChain implements granter.
func (sg *slotGranter) continueGrantChain(grantChainID grantChainID) {
	sg.coord.continueGrantChain(sg.workKind, grantChainID)
}

// requesterHasWaitingRequests implements granterWithLockedCalls.
func (sg *slotGranter) requesterHasWaitingRequests() bool {
	hasWaiting, _ := sg.requester.hasWaitingRequests()
	return hasWaiting
}

// tryGrantLocked implements granterWithLockedCalls.
func (sg *slotGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	res := sg.tryGetLocked(1, 0 /*arbitrary*/)
	if res == grantSuccess {
		slots := sg.requester.granted(grantChainID)
		if slots == 0 {
			// Did not accept grant.
			sg.returnGrantLocked(1, 0 /*arbitrary*/)
			return grantFailLocal
		} else if slots != 1 {
			panic(errors.AssertionFailedf("unexpected count %d", slots))
		}
	}
	return res
}

//gcassert:inline
func (sg *slotGranter) setTotalSlotsLockedInternal(totalSlots int) {
	// ============================================================
	// 分支 1: slots 增加
	// ============================================================
	if totalSlots > sg.totalSlots {
		// 检查: 是否从耗尽状态恢复？
		//
		// 条件:
		// - 旧值: totalSlots ≤ usedSlots (耗尽)
		// - 新值: totalSlots > usedSlots (有余量)
		if sg.totalSlots <= sg.usedSlots && totalSlots > sg.usedSlots {
			// 计算耗尽持续时间
			now := timeutil.Now()
			exhaustedMicros := now.Sub(sg.exhaustedStart).Microseconds()

			// 累计到 metric
			//这个 metric 回答了一个关键问题:
			//"系统有多少时间处于 slot 耗尽状态？"
			//
			//用途:
			//1. 容量规划: exhaustedDuration 高 → 需要增加节点
			//2. 调优: exhaustedDuration 低 → 可以降低 maxCPUSlots
			//3. 告警: exhaustedDuration 突增 → 可能有异常负载
			//
			//示例:
			//    1 小时 = 3,600,000,000μs
			//    slotsExhaustedDuration = 360,000,000μs
			//    → 耗尽比例 = 10%
			//    → 系统 10% 的时间处于 slot 饱和状态
			sg.slotsExhaustedDurationMetric.Inc(exhaustedMicros)

			// 示例:
			// T=0: totalSlots=50, usedSlots=50, exhaustedStart=T0
			// T=10ms: totalSlots→51, usedSlots=50
			// → exhaustedMicros = 10,000μs
			// → slotsExhaustedDurationMetric += 10,000
		}

		// ============================================================
		// 分支 2: slots 减少
		// ============================================================
	} else if totalSlots < sg.totalSlots {
		// 检查: 是否进入耗尽状态？
		//
		// 条件:
		// - 旧值: totalSlots > usedSlots (有余量)
		// - 新值: totalSlots ≤ usedSlots (耗尽)
		if sg.totalSlots > sg.usedSlots && totalSlots <= sg.usedSlots {
			// 记录耗尽开始时间
			sg.exhaustedStart = timeutil.Now()

			// 示例:
			// T=0: totalSlots=51, usedSlots=50
			// T=1ms: totalSlots→50, usedSlots=50
			// → 进入耗尽状态
			// → exhaustedStart = T1
		}
	}

	// ============================================================
	// 最终: 更新 totalSlots
	// ============================================================
	sg.totalSlots = totalSlots
}

// tokenGranter implements granterWithLockedCalls.
// **与slotGranter的关键区别**：
// 1. **令牌可以批量分配**：`count`可以 > 1
// 2. **有突发限制**：`maxBurstTokens`限制短时间内的令牌消耗
// 3. **CPU过载检查**：每次分配都检查`cpuOverload.isOverloaded()`
type tokenGranter struct {
	coord                *GrantCoordinator
	workKind             WorkKind
	requester            requester
	availableBurstTokens int64 // 当前可用的突发令牌数
	maxBurstTokens       int64 // 最大突发令牌数
	skipTokenEnforcement bool
	// Non-nil for all uses of tokenGranter (SQLKVResponseWork and
	// SQLSQLResponseWork).
	cpuOverload cpuOverloadIndicator // CPU过载指示器
}

var _ granterWithLockedCalls = &tokenGranter{}
var _ granter = &tokenGranter{}

// refillBurstTokens 周期性补充令牌
func (tg *tokenGranter) refillBurstTokens(skipTokenEnforcement bool) {
	tg.availableBurstTokens = tg.maxBurstTokens
	tg.skipTokenEnforcement = skipTokenEnforcement
}

// tryGet implements granter.
func (tg *tokenGranter) tryGet(_ burstQualification, count int64) bool {
	return tg.coord.tryGet(tg.workKind, count, 0 /*arbitrary*/)
}

// tryGetLocked implements granterWithLockedCalls.
//  1. **CPU过载优先检查**：即使有令牌，CPU过载也会拒绝
//  2. **令牌可以透支**：
//     ```
//     假设 maxBurstTokens = 100, availableBurstTokens = 10
//     请求 count = 50
//     结果：availableBurstTokens = -40 (允许！)
//     ```
//  3. **为什么允许透支**：
//     - 请求已经在队列中等待
//     - 拒绝会导致饿死（starvation）
//     - 下次refill时会恢复正常
func (tg *tokenGranter) tryGetLocked(count int64, _ int8) grantResult {
	// CPU过载，拒绝授权
	if tg.cpuOverload.isOverloaded() {
		return grantFailDueToSharedResource
	}
	if tg.availableBurstTokens > 0 || tg.skipTokenEnforcement {
		tg.availableBurstTokens -= count // 可以变成负数！
		return grantSuccess
	}
	return grantFailLocal
}

// returnGrant implements granter.
func (tg *tokenGranter) returnGrant(count int64) {
	tg.coord.returnGrant(tg.workKind, count, 0 /*arbitrary*/)
}

// returnGrantLocked implements granterWithLockedCalls.
func (tg *tokenGranter) returnGrantLocked(count int64, _ int8) {
	tg.availableBurstTokens += count
	if tg.availableBurstTokens > tg.maxBurstTokens { // 上限限制
		tg.availableBurstTokens = tg.maxBurstTokens
	}
}

// tookWithoutPermission implements granter.
func (tg *tokenGranter) tookWithoutPermission(count int64) {
	tg.coord.tookWithoutPermission(tg.workKind, count, 0 /*arbitrary*/)
}

// tookWithoutPermissionLocked implements granterWithLockedCalls.
func (tg *tokenGranter) tookWithoutPermissionLocked(count int64, _ int8) {
	tg.availableBurstTokens -= count
}

// continueGrantChain implements granter.
func (tg *tokenGranter) continueGrantChain(grantChainID grantChainID) {
	tg.coord.continueGrantChain(tg.workKind, grantChainID)
}

// requesterHasWaitingRequests implements granterWithLockedCalls.
func (tg *tokenGranter) requesterHasWaitingRequests() bool {
	hasWaiting, _ := tg.requester.hasWaitingRequests()
	return hasWaiting
}

// tryGrantLocked implements granterWithLockedCalls.
// tryGrantLocked 向等待队列授权
// 1. 先乐观地获取1个令牌
// 2. 通知requester有可用资源
// 3. requester返回实际需要的令牌数
// 4. 如果需要更多，用`tookWithoutPermission`补足
func (tg *tokenGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	res := tg.tryGetLocked(1, 0 /*arbitrary*/) //可用token先-1
	if res == grantSuccess {
		tokens := tg.requester.granted(grantChainID) // 通知requester
		if tokens == 0 {
			// Did not accept grant. (请求已取消)
			tg.returnGrantLocked(1, 0 /*arbitrary*/)
			return grantFailLocal
		} else if tokens > 1 {
			// 实际需要更多令牌，再-token数的可用令牌
			tg.tookWithoutPermissionLocked(tokens-1, 0 /*arbitrary*/)
		}
	}
	return res
}

// kvStoreTokenGranter is used for grants to KVWork to a store, that is
// limited by IO tokens. It encapsulates three granter-requester pairs, for
// the two workClasses and for incoming snapshots. The granter in these pairs
// is implemented by kvStoreTokenChildGranter, and the requester either by
// WorkQueue or SnapshotQueue. We have separate WorkQueues for the work
// classes so that we don't have a situation where tenant1's elastic work is
// queued ahead of tenant2's regular work (due to inter-tenant fairness) and
// blocks the latter from getting tokens, because elastic tokens are exhausted
// (and tokens for regular work are not exhausted).
//
// The kvStoreTokenChildGranters delegate the actual interaction to
// their "parent", kvStoreTokenGranter. For elasticWorkClass, multiple kinds
// of tokens need to be acquired, (a) the usual IO tokens (based on
// compactions out of L0 and flushes into L0) and (b) elastic disk bandwidth
// tokens, which are based on disk bandwidth as a constrained resource, and
// apply to all the elastic incoming bytes into the LSM.
type kvStoreTokenGranter struct {
	knobs *TestingKnobs
	//每个requester通过kvStoreTokenChildGranter与parent交互
	//parent管理三种令牌：
	//  1. IO Tokens (L0刷新/压缩容量)
	//  2. Elastic IO Tokens (弹性工作专用)
	//  3. Disk Bandwidth Tokens (磁盘带宽)
	regularRequester  requester //(常规工作队列)
	elasticRequester  requester //(弹性工作队列)
	snapshotRequester requester // (快照接收队列)

	mu struct {
		syncutil.Mutex
		// There is no rate limiting in granting these tokens. That is, they are
		// all burst tokens.

		// The "IO" tokens represent flush/compaction capacity into/out of L0. All
		// work deducts from both availableIOTokens and availableElasticIOTokens.
		// Regular work blocks if availableIOTokens is <= 0 and elastic work
		// blocks if availableElasticIOTokens <= 0.
		//1. 中文翻译
		//“发放这些令牌（Tokens）时没有速率限制。也就是说，它们全都是突发令牌（Burst Tokens）。”
		//
		//“‘IO’ 令牌代表进入或离开 L0 层的冲刷/压缩能力。
		//所有的工作都会同时扣减 availableIOTokens（可用 IO 令牌）和
		//availableElasticIOTokens（可用弹性 IO 令牌）。当 availableIOTokens 小于等于 0 时，
		//**常规任务（Regular work）**将会阻塞；
		//当 availableElasticIOTokens 小于等于 0 时，**弹性任务（Elastic work）**将会阻塞。
		// "IO" tokens代表L0的刷新/压缩容量
		// 所有工作都从availableIOTokens中扣除
		// 常规工作：availableIOTokens[RegularWorkClass] > 0
		// 弹性工作：availableIOTokens[ElasticWorkClass] > 0
		availableIOTokens            [admissionpb.NumWorkClasses]int64
		elasticIOTokensUsedByElastic int64 // Elastic 专用（进一步限制弹性工作）
		// TODO(aaditya): add support for read/IOPS tokens.
		// Disk bandwidth tokens.
		// 磁盘带宽令牌
		diskTokensAvailable diskTokens
		diskTokensError     struct {
			// prevObserved{Writes,Reads} is the observed disk metrics in the last
			// call to adjustDiskTokenErrorLocked. These are used to compute the
			// delta.
			prevObservedWrites             uint64
			prevObservedReads              uint64
			diskWriteTokensAlreadyDeducted int64
			diskReadTokensAlreadyDeducted  int64
		}
		diskTokensUsed [admissionpb.NumStoreWorkTypes]diskTokens
		// exhaustedStart is the time when the corresponding availableIOTokens
		// became <= 0. Ignored when the corresponding availableIOTokens is > 0.
		// 耗尽时间追踪
		exhaustedStart [admissionpb.NumWorkClasses]time.Time
		// startingIOTokens is the number of tokens set by setAvailableTokens for
		// regular work. It is used to compute the tokens used, by computing
		// startingIOTokens-availableIOTokens[RegularWorkClass].
		startingIOTokens int64

		// Estimation models.
		// 估算模型
		l0WriteLM, l0IngestLM, ingestLM, writeAmpLM tokensLinearModel
	}

	ioTokensExhaustedDurationMetric [admissionpb.NumWorkClasses]*metric.Counter
	availableTokensMetric           [admissionpb.NumWorkClasses]*metric.Gauge
	tokensReturnedMetric            *metric.Counter
	tokensTakenMetric               *metric.Counter
}

var _ granterWithIOTokens = &kvStoreTokenGranter{}

// kvStoreTokenChildGranter handles a particular workClass. Its methods
// pass-through to the parent after adding the workClass as a parameter.
// 委托模式
// 在设计模式中，委托模式（Delegation Pattern） 是一种基本技巧，
// 它的核心思想是：一个对象（委托者）不亲自处理某项任务，而是将其交给另一个辅助对象（受托者）去执行。
//
// 简单来说，就是“这件事我不亲自做，我找专业的人帮我做”。

// 3. 为什么使用委托模式？
// 减小耦合：委托者不需要知道任务的具体实现细节，只需要知道受托者能完成任务。
//
// 代码复用：多个类可以共用同一个受托者。
//
// 灵活性：你可以在运行时动态地更换受托者（比如把程序员从 A 换成 B）。
type kvStoreTokenChildGranter struct {
	workType admissionpb.StoreWorkType
	parent   *kvStoreTokenGranter
}

var _ granterWithStoreReplicatedWorkAdmitted = &kvStoreTokenChildGranter{}
var _ granter = &kvStoreTokenChildGranter{}

// tryGet implements granter.
func (cg *kvStoreTokenChildGranter) tryGet(_ burstQualification, count int64) bool {
	return cg.parent.tryGet(cg.workType, count)
}

// returnGrant implements granter.
func (cg *kvStoreTokenChildGranter) returnGrant(count int64) {
	cg.parent.returnGrant(cg.workType, count)
}

// tookWithoutPermission implements granter.
func (cg *kvStoreTokenChildGranter) tookWithoutPermission(count int64) {
	cg.parent.tookWithoutPermission(cg.workType, count)
}

// continueGrantChain implements granter.
func (cg *kvStoreTokenChildGranter) continueGrantChain(grantChainID grantChainID) {
	// Ignore since grant chains are not used for store tokens.
}

// storeWriteDone implements granterWithStoreReplicatedWorkAdmitted.
func (cg *kvStoreTokenChildGranter) storeWriteDone(
	originalTokens int64, doneInfo StoreWorkDoneInfo,
) (additionalTokens int64) {
	// NB: the token/metric adjustments we want to make here are the same as we
	// want to make through the storeReplicatedWorkAdmittedLocked, so we (ab)use
	// it. The one difference is that post token adjustments, if we observe the
	// granter was previously exhausted but is no longer so, we're allowed to
	// admit other waiting requests.
	return cg.parent.storeReplicatedWorkAdmitted(
		cg.workType, originalTokens, storeReplicatedWorkAdmittedInfo(doneInfo), true /* canGrantAnother */)
}

// storeReplicatedWorkAdmitted implements granterWithStoreReplicatedWorkAdmitted.
func (cg *kvStoreTokenChildGranter) storeReplicatedWorkAdmittedLocked(
	originalTokens int64, admittedInfo storeReplicatedWorkAdmittedInfo,
) (additionalTokens int64) {
	return cg.parent.storeReplicatedWorkAdmittedLocked(cg.workType, originalTokens, admittedInfo, false /* canGrantAnother */)
}

func (sg *kvStoreTokenGranter) tryGet(workType admissionpb.StoreWorkType, count int64) bool {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	return sg.tryGetLocked(count, workType)
}

// tryGetLocked is the real implementation of tryGet from the granter
// interface.
func (sg *kvStoreTokenGranter) tryGetLocked(count int64, wt admissionpb.StoreWorkType) bool {
	// NB: ideally if regularRequester.hasWaitingRequests() returns true and
	// wc==elasticWorkClass we should reject this request, since it means that
	// more important regular work is waiting. However, we rely on the
	// assumption that elasticWorkClass, once throttled, will have a non-empty
	// queue, and since the only case where tryGetLocked is called for
	// elasticWorkClass is when the queue is empty, this case should be rare
	// (and not cause a performance isolation failure).

	// NB: For disk write tokens, we apply `writeAmpLM` to account for write
	// amplification. This model is used again later in
	// `storeReplicatedWorkAdmittedLocked()`. There is an obvious gap in
	// accounting here, since the model could change between the two calls for the
	// same request. For example:
	// (1) We have a request that requests 50 tokens, and write-amp LM is
	//     currently 10.0x + 1. We will deduct 501 tokens for disk writes.
	// (2) Before we adjust the actual write bytes used by the write request, the
	//     write-amp model is updated to 5.0x + 1.
	// (3) In `storeReplicatedWorkAdmittedLocked()`, we learn that the request
	//     used 200 actual bytes. Now we will apply the new LM to get 251 tokens
	//     initially deducted, and apply the LM for 1001 actual bytes used. We
	//     will now deduct 750 additional tokens from the bucket. Now we have
	//     deducted 1251 tokens rather than 1001.
	// This can also go the other way, where we deduct fewer tokens than actually
	// needed. We are generally okay with this since the model changes
	// infrequently (every 15s), and the disk bandwidth limiter is designed to
	// generally under admit and only pace elastic work.
	//1. 第一段：关于“排队优先级”的潜规则【直白翻译】注： 理想情况下，如果“常规任务”正在排队，
	//而现在来了一个“弹性任务”，我们理应直接拒绝掉这个不重要的弹性任务，
	//先紧着重要的常规活儿干。但我们这儿有个假设：弹性任务一旦被限流，
	//它的队列肯定不是空的。而我们现在的代码逻辑是：只有在弹性任务队列为空的情况下，
	//才会尝试去抢占资源。所以，常规任务在排队、弹性任务却来抢资源的情况应该极少发生，
	//不会破坏不同任务之间的性能隔离。
	//2. 第二段：关于“写放大”造成的账单乌龙【直白翻译】注：
	//在计算硬盘写入令牌时，我们会用一个模型（LM）来预估“写放大”——也就是你写 1GB 数据，
	//硬盘实际可能要处理 10GB。但这里有个明显的漏洞：我们在预扣款和最终结算这两个点之间，
	//预估模型可能会变，导致账单对不上。举个例子：刚开始，模型认为写放大是 10 倍。你请求写 50 字节，
	//我们先扣了 $50 \times 10 + 1 = 51$ 个令牌。还没等正式写完，模型更新了，认为写放大只要 5 倍就行。
	//结果结算时，发现你实际写了 200 字节。按新模型算，本来该扣 1001 个，但因为预扣和结算用了不同标准，
	//最后反倒多扣了你 250 个。当然，少扣的情况也会发生。但我们觉得没关系，因为模型每 15 秒才变一次，
	//而且这个限流器主要就是为了压榨“弹性任务”的，不求绝对精确。

	// 应用写放大模型计算磁盘写令牌
	diskWriteTokens := count
	if wt != admissionpb.SnapshotIngestStoreWorkType {
		// Snapshot ingests do not incur the write amplification described above, so
		// we skip applying the model for those writes.
		// 写放大模型：实际磁盘写入 = 逻辑写入 * 放大系数
		// 例如：count=100, writeAmpLM=10x+1 → diskWriteTokens=1001
		diskWriteTokens = sg.mu.writeAmpLM.applyLinearModel(count)
	}
	//**三种工作类型的令牌需求**：
	//| 工作类型 | IO Tokens (Regular) | IO Tokens (Elastic) | Disk Write Tokens |
	//| --- | --- | --- | --- |
	//| Regular | ✓ | × | ✓ |
	//| Elastic | ✓ | ✓ | ✓ |
	//| Snapshot Ingest | × | × | ✓ |
	//**设计理由**：
	//
	//1. **Regular工作优先级最高**：只要RegularWorkClass的IO令牌足够就能执行
	switch wt {
	// 常规工作只需检查IO令牌
	case admissionpb.RegularStoreWorkType:
		if sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0 {
			// 授予令牌
			sg.subtractIOTokensLocked(count, count, false)
			sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return true
		}
		// 弹性工作需要检查三种令牌
	case admissionpb.ElasticStoreWorkType:
		if sg.mu.diskTokensAvailable.writeByteTokens > 0 &&
			sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0 &&
			sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] > 0 {
			sg.subtractIOTokensLocked(count, count, false)
			sg.mu.elasticIOTokensUsedByElastic += count
			sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			// 授予令牌
			return true
		}
	case admissionpb.SnapshotIngestStoreWorkType:
		// Snapshot ingests do not go into L0, so we only subject them to
		// writeByteTokens.
		// 快照摄入只检查磁盘写令牌（不进L0）
		if sg.mu.diskTokensAvailable.writeByteTokens > 0 {
			sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			// 授予令牌
			return true
		}
	}
	return false
}

func (sg *kvStoreTokenGranter) returnGrant(workType admissionpb.StoreWorkType, count int64) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.returnGrantLocked(count, workType)
	// Try granting, since tokens have been returned.
	sg.tryGrantLocked()
}

// returnGrantLocked is the real implementation of returnGrant from the
// granter interface.
func (sg *kvStoreTokenGranter) returnGrantLocked(count int64, wt admissionpb.StoreWorkType) {
	// Return store tokens.
	sg.subtractTokensForStoreWorkTypeLocked(wt, -count)
}

// subtractTokensForStoreWorkTypeLocked is a helper function that subtracts
// tokens from all store tokens for a given admissionpb.StoreWorkType. Count can
// be negative to add tokens.
func (sg *kvStoreTokenGranter) subtractTokensForStoreWorkTypeLocked(
	wt admissionpb.StoreWorkType, count int64,
) {
	if wt != admissionpb.SnapshotIngestStoreWorkType {
		// Adjust count tokens for "IO tokens".
		sg.subtractIOTokensLocked(count, count, false)
	}
	if wt == admissionpb.ElasticStoreWorkType {
		sg.mu.elasticIOTokensUsedByElastic += count
	}
	// Adjust tokens for disk bandwidth bucket.
	switch wt {
	case admissionpb.RegularStoreWorkType, admissionpb.ElasticStoreWorkType:
		diskTokenCount := sg.mu.writeAmpLM.applyLinearModel(count)
		sg.mu.diskTokensAvailable.writeByteTokens -= diskTokenCount
		sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskTokenCount
		sg.mu.diskTokensUsed[wt].writeByteTokens += diskTokenCount
	case admissionpb.SnapshotIngestStoreWorkType:
		// Do not apply the writeAmpLM since these writes do not incur additional
		// write-amp.
		sg.mu.diskTokensAvailable.writeByteTokens -= count
		sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += count
		sg.mu.diskTokensUsed[wt].writeByteTokens += count
	}
}

func (sg *kvStoreTokenGranter) adjustDiskTokenError(m StoreMetrics) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.adjustDiskTokenErrorLocked(m.DiskStats.BytesRead, m.DiskStats.BytesWritten)
}

// adjustDiskTokenErrorLocked is used to account for extra reads and writes that
// are in excess of tokens already deducted.
//
// (1) For writes, we deduct tokens at admission for each request. If the actual
// writes seen on disk for a given interval is higher than the tokens already
// deducted, the delta is the write error. This value is then subtracted from
// available disk tokens.
//
// (2) For reads, we do not deduct any tokens at admission. However, we deduct
// tokens in advance during token estimation in the diskBandwidthLimiter for the
// next adjustment interval. These pre-deducted tokens are then allocated at the
// same interval as write tokens. Any additional reads in the interval are
// considered error and are subtracted from the available disk write tokens.
//
// For both reads, and writes, we reset the
// disk{read,write}TokensAlreadyDeducted to 0 for the next adjustment interval.
// For writes, we do this so that we are accounting for errors only in the given
// interval, and not across them. For reads, this is so that we don't grow
// arbitrarily large "burst" tokens, since they are not capped to an allocation
// period.
// **误差产生的原因**：
//
// 1. **写入误差**：
//   - 估算的写放大 ≠ 实际写放大
//   - 后台压缩/刷新导致的额外写入
//
// 2. **读取误差**：
//   - 读取在准入时不预扣令牌
//   - 但读取会消耗磁盘带宽
//
// 校正策略：
// - 每个调整周期检查实际磁盘IO
// - 如果实际 > 已扣除，从可用令牌中补扣
// - 防止磁盘带宽被低估
func (sg *kvStoreTokenGranter) adjustDiskTokenErrorLocked(readBytes uint64, writeBytes uint64) {
	intWrites := int64(writeBytes - sg.mu.diskTokensError.prevObservedWrites)
	intReads := int64(readBytes - sg.mu.diskTokensError.prevObservedReads)

	// Compensate for error due to writes.
	// 补偿写入误差
	writeError := intWrites - sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted
	if writeError > 0 {
		sg.mu.diskTokensAvailable.writeByteTokens -= writeError
	}
	// 补偿读取误差
	// Compensate for error due to reads.
	readError := intReads - sg.mu.diskTokensError.diskReadTokensAlreadyDeducted
	if readError > 0 {
		sg.mu.diskTokensAvailable.writeByteTokens -= readError
	}

	// We have compensated for error, if any, in this interval, so we reset the
	// deducted count for the next compensation interval.
	// 重置扣除计数，准备下一个周期
	sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted = 0
	sg.mu.diskTokensError.diskReadTokensAlreadyDeducted = 0

	sg.mu.diskTokensError.prevObservedWrites = writeBytes
	sg.mu.diskTokensError.prevObservedReads = readBytes
}

func (sg *kvStoreTokenGranter) tookWithoutPermission(
	workType admissionpb.StoreWorkType, count int64,
) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.tookWithoutPermissionLocked(count, workType)
}

// tookWithoutPermissionLocked is the real implementation of
// tookWithoutPermission from the granter interface.
func (sg *kvStoreTokenGranter) tookWithoutPermissionLocked(
	count int64, wt admissionpb.StoreWorkType,
) {
	// Deduct store tokens.
	sg.subtractTokensForStoreWorkTypeLocked(wt, count)
}

// subtractIOTokensLocked is a helper function that subtracts count tokens (count
// can be negative, in which case this is really an addition).
func (sg *kvStoreTokenGranter) subtractIOTokensLocked(
	count int64, elasticCount int64, settingAvailableTokens bool,
) {
	sg.subtractTokensLockedForWorkClass(admissionpb.RegularWorkClass, count, settingAvailableTokens)
	sg.subtractTokensLockedForWorkClass(admissionpb.ElasticWorkClass, elasticCount, settingAvailableTokens)
	if !settingAvailableTokens {
		if count > 0 {
			sg.tokensTakenMetric.Inc(count)
		} else {
			sg.tokensReturnedMetric.Inc(-count)
		}
	}
}

func (sg *kvStoreTokenGranter) subtractTokensLockedForWorkClass(
	wc admissionpb.WorkClass, count int64, settingAvailableTokens bool,
) {
	avail := sg.mu.availableIOTokens[wc]
	sg.mu.availableIOTokens[wc] -= count
	sg.availableTokensMetric[wc].Update(sg.mu.availableIOTokens[wc])
	if count > 0 && avail > 0 && sg.mu.availableIOTokens[wc] <= 0 {
		// Transition from > 0 to <= 0.
		sg.mu.exhaustedStart[wc] = timeutil.Now()
	} else if count < 0 && avail <= 0 && (sg.mu.availableIOTokens[wc] > 0 || settingAvailableTokens) {
		// Transition from <= 0 to > 0, or if we're newly setting available
		// tokens. The latter ensures that if the available tokens stay <= 0, we
		// don't show a sudden change in the metric after minutes of exhaustion
		// (we had observed such behavior prior to this change).
		now := timeutil.Now()
		exhaustedMicros := now.Sub(sg.mu.exhaustedStart[wc]).Microseconds()
		sg.ioTokensExhaustedDurationMetric[wc].Inc(exhaustedMicros)
		if sg.mu.availableIOTokens[wc] <= 0 {
			sg.mu.exhaustedStart[wc] = now
		}
	}
}

func (sg *kvStoreTokenGranter) tryGrant() {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.tryGrantLocked()
}

// tryGrantLocked attempts to grant to as many requests as possible.
func (sg *kvStoreTokenGranter) tryGrantLocked() {
	for sg.tryGrantLockedOne() {
	}
}

// tryGrantLocked is used to attempt to grant to waiting requests. Used by
// storeGrantCoordinator. It successfully grants to at most one waiting
// request. If there are no waiting requests, or all waiters reject the grant,
// it returns false.
// 优先级排序
// **授权优先级**：
// ```
// 1. Regular (常规工作) - 用户前台请求
// 2. Snapshot Ingest (快照摄入) - 节点再平衡
// 3. Elastic (弹性工作) - 后台任务
// ```
// **设计理由**：
// - Regular优先保证用户体验
// - Snapshot优先保证集群健康（副本恢复）
// - Elastic最后执行，可以被抢占
func (sg *kvStoreTokenGranter) tryGrantLockedOne() bool {
	// NB: We grant work in the following priority order: regular, snapshot
	// ingest, elastic work. Snapshot ingests are a special type of elastic work.
	// They queue separately in the SnapshotQueue and get priority over other
	// elastic work since they are used for node re-balancing and up-replication,
	// which are typically higher priority than other background writes.
	for wt := admissionpb.StoreWorkType(0); wt < admissionpb.NumStoreWorkTypes; wt++ {
		req := sg.regularRequester
		if wt == admissionpb.ElasticStoreWorkType {
			req = sg.elasticRequester
		} else if wt == admissionpb.SnapshotIngestStoreWorkType {
			req = sg.snapshotRequester
		}
		hasWaiting, _ := req.hasWaitingRequests()
		if hasWaiting {
			res := sg.tryGetLocked(1, wt)
			if res {
				tookTokenCount := req.granted(noGrantChain)
				if tookTokenCount == 0 {
					// Did not accept grant.
					sg.returnGrantLocked(1, wt)
					// Continue with the loop since this requester does not have waiting
					// requests. If the loop terminates we will correctly return
					// grantFailLocal.
				} else {
					// May have taken more.
					if tookTokenCount > 1 {
						sg.tookWithoutPermissionLocked(tookTokenCount-1, wt)
					}
					return true
				}
			} else {
				// Was not able to get token. Do not continue with looping to grant to
				// less important work (though it would be harmless since won't be
				// able to get a token for that either).
				// 无法获取令牌，不继续尝试更低优先级的工作
				return res
			}
		}
	}
	return false
}

// setAvailableTokens implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) setAvailableTokens(
	ioTokens int64,
	elasticIOTokens int64,
	diskWriteTokens int64,
	diskReadTokens int64,
	ioTokenCapacity int64,
	elasticIOTokenCapacity int64,
	diskWriteTokensCapacity int64,
	lastTick bool,
) (ioTokensUsed int64, ioTokensUsedByElasticWork int64) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	ioTokensUsed = sg.mu.startingIOTokens - sg.mu.availableIOTokens[admissionpb.RegularWorkClass]
	ioTokensUsedByElasticWork = sg.mu.elasticIOTokensUsedByElastic
	sg.mu.elasticIOTokensUsedByElastic = 0

	// It is possible for availableIOTokens to be negative because of
	// tookWithoutPermission or because tryGet will satisfy requests until
	// availableIOTokens become <= 0. We want to remember this previous
	// over-allocation.
	sg.subtractIOTokensLocked(-ioTokens, -elasticIOTokens, true)
	if sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > ioTokenCapacity {
		sg.mu.availableIOTokens[admissionpb.RegularWorkClass] = ioTokenCapacity
	}
	if sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] > elasticIOTokenCapacity {
		sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] = elasticIOTokenCapacity
	}
	// availableIOTokens[ElasticWorkClass] can become very negative since it can
	// be fewer than the tokens for regular work, and regular work deducts from it
	// without blocking. This behavior is desirable, but we don't want deficits to
	// accumulate indefinitely. We've found that resetting on the lastTick
	// provides a good enough frequency for resetting the deficit. That is, we are
	// resetting every 15s.
	if lastTick {
		sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] =
			max(sg.mu.availableIOTokens[admissionpb.ElasticWorkClass], 0)
		// It is possible that availableIOTokens[RegularWorkClass] is negative, in
		// which case we want availableIOTokens[ElasticWorkClass] to not exceed it.
		sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] =
			min(sg.mu.availableIOTokens[admissionpb.ElasticWorkClass], sg.mu.availableIOTokens[admissionpb.RegularWorkClass])
		// We also want to avoid very negative disk write tokens, so we reset them.
		sg.mu.diskTokensAvailable.writeByteTokens = max(sg.mu.diskTokensAvailable.writeByteTokens, 0)
	}
	var w admissionpb.WorkClass
	for w = 0; w < admissionpb.NumWorkClasses; w++ {
		sg.availableTokensMetric[w].Update(sg.mu.availableIOTokens[w])
	}
	sg.mu.startingIOTokens = sg.mu.availableIOTokens[admissionpb.RegularWorkClass]

	// Allocate disk write and read tokens.
	sg.mu.diskTokensAvailable.writeByteTokens += diskWriteTokens
	if sg.mu.diskTokensAvailable.writeByteTokens > diskWriteTokensCapacity {
		sg.mu.diskTokensAvailable.writeByteTokens = diskWriteTokensCapacity
	}
	// NB: We don't cap the disk read tokens as they are only deducted during the
	// error accounting loop. So essentially, we give reads the "burst" capacity
	// of the error accounting interval. See `adjustDiskTokenErrorLocked` for the
	// error accounting logic, and where we reset this bucket to 0.
	sg.mu.diskTokensError.diskReadTokensAlreadyDeducted += diskReadTokens

	return ioTokensUsed, ioTokensUsedByElasticWork
}

// getDiskTokensUsedAndResetLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) getDiskTokensUsedAndReset() (
	usedTokens [admissionpb.NumStoreWorkTypes]diskTokens,
) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	for i := 0; i < admissionpb.NumStoreWorkTypes; i++ {
		usedTokens[i] = sg.mu.diskTokensUsed[i]
		sg.mu.diskTokensUsed[i] = diskTokens{}
	}
	return usedTokens
}

// setAdmittedModelsLocked implements granterWithIOTokens.
func (sg *kvStoreTokenGranter) setLinearModels(
	l0WriteLM tokensLinearModel,
	l0IngestLM tokensLinearModel,
	ingestLM tokensLinearModel,
	writeAmpLM tokensLinearModel,
) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.mu.l0WriteLM = l0WriteLM
	sg.mu.l0IngestLM = l0IngestLM
	sg.mu.ingestLM = ingestLM
	sg.mu.writeAmpLM = writeAmpLM
}

func (sg *kvStoreTokenGranter) storeReplicatedWorkAdmitted(
	wt admissionpb.StoreWorkType,
	originalTokens int64,
	admittedInfo storeReplicatedWorkAdmittedInfo,
	canGrantAnother bool,
) (additionalTokens int64) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	return sg.storeReplicatedWorkAdmittedLocked(wt, originalTokens, admittedInfo, canGrantAnother)
}

func (sg *kvStoreTokenGranter) storeReplicatedWorkAdmittedLocked(
	wt admissionpb.StoreWorkType,
	originalTokens int64,
	admittedInfo storeReplicatedWorkAdmittedInfo,
	canGrantAnother bool,
) (additionalTokens int64) {
	//**两阶段令牌扣除**：
	//
	//1. **准入时（Admit）**：
	//    - 使用估算值预扣令牌
	//    - 基于`RequestedCount`和线性模型
	//2. **完成时（Done/Admitted）**：
	//    - 使用实际值调整令牌
	//    - 可能补扣（实际 > 估算）
	//    - 也可能退还（实际 < 估算）
	//
	//**为什么需要两阶段**：
	//- 准入时不知道实际写入量
	//- 异步复制（below-raft）进一步延迟了实际值的获知
	//- 估算模型会定期更新（每15秒）
	// Reminder: coord.mu protects the state in the kvStoreTokenGranter.
	wc := admissionpb.WorkClassFromStoreWorkType(wt)
	exhaustedFunc := func() bool {
		return sg.mu.availableIOTokens[admissionpb.RegularWorkClass] <= 0 ||
			(wc == admissionpb.ElasticWorkClass && (sg.mu.diskTokensAvailable.writeByteTokens <= 0 ||
				sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] <= 0))
	}
	wasExhausted := exhaustedFunc()
	// 应用L0写入模型
	actualL0WriteTokens := sg.mu.l0WriteLM.applyLinearModel(admittedInfo.WriteBytes)
	// 应用L0摄入模型
	actualL0IngestTokens := sg.mu.l0IngestLM.applyLinearModel(admittedInfo.IngestedBytes)
	actualL0Tokens := actualL0WriteTokens + actualL0IngestTokens
	// 计算需要额外扣除的令牌
	additionalL0TokensNeeded := actualL0Tokens - originalTokens
	sg.subtractIOTokensLocked(additionalL0TokensNeeded, additionalL0TokensNeeded, false)
	if wt == admissionpb.ElasticStoreWorkType {
		sg.mu.elasticIOTokensUsedByElastic += additionalL0TokensNeeded
	}

	// Adjust disk write tokens.
	// 调整磁盘写令牌
	ingestIntoLSM := sg.mu.ingestLM.applyLinearModel(admittedInfo.IngestedBytes)
	totalBytesIntoLSM := actualL0WriteTokens + ingestIntoLSM
	actualDiskWriteTokens := sg.mu.writeAmpLM.applyLinearModel(totalBytesIntoLSM)
	originalDiskTokens := sg.mu.writeAmpLM.applyLinearModel(originalTokens)
	additionalDiskWriteTokens := actualDiskWriteTokens - originalDiskTokens
	sg.mu.diskTokensAvailable.writeByteTokens -= additionalDiskWriteTokens
	sg.mu.diskTokensUsed[wt].writeByteTokens += additionalDiskWriteTokens
	// 如果从耗尽状态恢复，尝试授权更多请求
	if canGrantAnother && (additionalL0TokensNeeded < 0) {
		isExhausted := exhaustedFunc()
		if (wasExhausted && !isExhausted) || sg.knobs.AlwaysTryGrantWhenAdmitted {
			sg.tryGrantLocked() // 尝试授权等待队列中的请求
		}
	}
	// For multi-tenant fairness accounting, we choose to ignore disk bandwidth
	// tokens. Ideally, we'd have multiple resource dimensions for the fairness
	// decisions, but we don't necessarily need something more sophisticated
	// like "Dominant Resource Fairness".
	return additionalL0TokensNeeded
}

// StoreMetrics are the metrics and some config information for a store.
type StoreMetrics struct {
	StoreID roachpb.StoreID
	*pebble.Metrics
	WriteStallCount int64
	// DiskUnhealthy corresponds to Engine.GetDiskUnhealthy.
	DiskUnhealthy bool
	// Optional.
	DiskStats DiskStats
	// Config.
	MemTableSizeForStopWrites uint64
}

// DiskStats provide low-level stats about the disk resources used for a
// store. We assume that the disk is not shared across multiple stores.
// However, transient and moderate usage that is not due to the store is
// tolerable, since the diskBandwidthLimiter is only using this to compute
// elastic tokens and is designed to deal with significant attribution
// uncertainty.
//
// DiskStats are not always populated. A ProvisionedBandwidth of 0 represents
// that the stats should be ignored.
type DiskStats struct {
	// BytesRead is the cumulative bytes read.
	BytesRead uint64
	// BytesWritten is the cumulative bytes written.
	BytesWritten uint64
	// ProvisionedBandwidth is the total provisioned bandwidth in bytes/s.
	ProvisionedBandwidth int64
}

var (
	totalSlots = metric.Metadata{
		Name:        "admission.granter.total_slots.kv",
		Help:        "Total slots for kv work",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	usedSlots = metric.Metadata{
		// Note: we append a WorkKind string to this name.
		Name:        "admission.granter.used_slots.",
		Help:        "Used slots",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	// NB: this metric is independent of whether slots enforcement is happening
	// or not.
	kvSlotsExhaustedDuration = metric.Metadata{
		Name: "admission.granter.slots_exhausted_duration.kv",
		Help: "Total duration (in micros) when KV slots were exhausted, as observed by the slot " +
			"granter (not waiters)",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	// We have a metric for both short and long period. These metrics use the
	// period provided in CPULoad and not wall time. So if the sum of the rate
	// of these two is < 1sec/sec, the CPULoad ticks are not happening at the
	// expected frequency (this could happen due to CPU overload).
	kvCPULoadShortPeriodDuration = metric.Metadata{
		Name:        "admission.granter.cpu_load_short_period_duration.kv",
		Help:        "Total duration when CPULoad was being called with a short period, in micros",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	kvCPULoadLongPeriodDuration = metric.Metadata{
		Name:        "admission.granter.cpu_load_long_period_duration.kv",
		Help:        "Total duration when CPULoad was being called with a long period, in micros",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	kvSlotAdjusterIncrements = metric.Metadata{
		Name:        "admission.granter.slot_adjuster_increments.kv",
		Help:        "Number of increments of the total KV slots",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	kvSlotAdjusterDecrements = metric.Metadata{
		Name:        "admission.granter.slot_adjuster_decrements.kv",
		Help:        "Number of decrements of the total KV slots",
		Measurement: "Slots",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensExhaustedDuration = metric.Metadata{
		Name: "admission.granter.io_tokens_exhausted_duration.kv",
		Help: "Total duration (in micros) when IO tokens were exhausted, as observed by " +
			"the token granter (not waiters)",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	kvElasticIOTokensExhaustedDuration = metric.Metadata{
		Name: "admission.granter.elastic_io_tokens_exhausted_duration.kv",
		Help: "Total duration (in micros) when Elastic IO tokens were exhausted, as observed by " +
			"the token granter (not waiters)",
		Measurement: "Microseconds",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensTaken = metric.Metadata{
		Name:        "admission.granter.io_tokens_taken.kv",
		Help:        "Total number of tokens taken",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensReturned = metric.Metadata{
		Name:        "admission.granter.io_tokens_returned.kv",
		Help:        "Total number of tokens returned",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensBypassed = metric.Metadata{
		Name:        "admission.granter.io_tokens_bypassed.kv",
		Help:        "Total number of tokens taken by work bypassing admission control (for example, follower writes without flow control)",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	kvIOTokensAvailable = metric.Metadata{
		Name:        "admission.granter.io_tokens_available.kv",
		Help:        "Number of tokens available",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	kvElasticIOTokensAvailable = metric.Metadata{
		Name:        "admission.granter.elastic_io_tokens_available.kv",
		Help:        "Number of tokens available",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	l0CompactedBytes = metric.Metadata{
		Name:        "admission.l0_compacted_bytes.kv",
		Help:        "Total bytes compacted out of L0 (used to generate IO tokens)",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
	l0TokensProduced = metric.Metadata{
		Name:        "admission.l0_tokens_produced.kv",
		Help:        "Total bytes produced for L0 writes",
		Measurement: "Tokens",
		Unit:        metric.Unit_COUNT,
	}
)

// TODO(irfansharif): we are lacking metrics for IO tokens and load, including
// metrics from helper classes used by ioLoadListener, like the code in
// disk_bandwidth.go and store_token_estimation.go. Additionally, what we have
// below is per node, while we want such metrics per store. We should add
// these metrics via StoreGrantCoordinators.SetPebbleMetricsProvider, which is
// used to construct the per-store GrantCoordinator. These metrics should be
// embedded in kvserver.StoreMetrics. We should also separate the metrics
// related to cpu slots from the IO metrics.
