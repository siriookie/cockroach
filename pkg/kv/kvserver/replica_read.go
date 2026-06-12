// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// executeReadOnlyBatch is the execution logic for client requests which do not
// mutate the range's replicated state. The method uses a single RocksDB
// iterator to evaluate the batch and then updates the timestamp cache to
// reflect the key spans that it read.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba *kvpb.BatchRequest, g *concurrency.Guard,
) (
	br *kvpb.BatchResponse,
	_ *concurrency.Guard,
	_ *kvadmission.StoreWriteBytes,
	pErr *kvpb.Error,
) {
	// readOnlyCmdMu 用来和 Replica 上某些需要排斥读命令的状态转换同步。
	// 读路径只拿 RLock，多个 read-only batch 之间仍然可以并发执行。
	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Verify that the batch can be executed.
	// 在创建 storage snapshot 之前先做一次执行前检查。
	// 这里会确认当前 replica 是否具备执行条件，比如 lease / range 状态等。
	st, err := r.checkExecutionCanProceedBeforeStorageSnapshot(ctx, ba, g)
	if err != nil {
		// 还没真正执行请求，guard 仍然交还给上层重试外壳处理。
		return nil, g, nil, kvpb.NewError(err)
	}

	// 测试钩子：允许测试在“检查完成、storage snapshot 创建之前”插入逻辑。
	if fn := r.store.TestingKnobs().PreStorageSnapshotButChecksCompleteInterceptor; fn != nil {
		fn(r)
	}

	// Compute the transaction's local uncertainty limit using observed
	// timestamps, which can help avoid uncertainty restarts.
	// 根据 batch header、当前 replica state、最大时钟偏移，计算本次读的 uncertainty interval。
	// 这个 interval 后续用于判断读到“未来版本”时能不能在服务端刷新时间戳继续读。
	ui := uncertainty.ComputeInterval(&ba.Header, st, r.Clock().MaxOffset())

	// Evaluate read-only batch command.
	// 构造 evaluation context。它把 Replica、已拿到的 latch spans、closed timestamp
	// 约束、admission header 等执行所需上下文封装起来，供 batcheval 使用。
	rec := NewReplicaEvalContext(
		ctx, r, g.LatchSpans(), ba.RequiresClosedTSOlderThanStorageSnapshot(), ba.AdmissionHeader)
	// evaluation context 内部可能持有可复用资源，退出时释放。
	defer rec.Release()

	// TODO(irfansharif): It's unfortunate that in this read-only code path,
	// we're stuck with a ReadWriter because of the way evaluateBatch is
	// designed.
	// 为读请求创建一个只读 storage batch/reader。
	// 虽然类型叫 ReadWriter，但这里使用 NewReadOnly，不会产生写入。
	rw := r.store.StateEngine().NewReadOnly(storage.StandardDurability)
	if !rw.ConsistentIterators() {
		// This is not currently needed for correctness, but future optimizations
		// may start relying on this, so we assert here.
		// 读路径依赖同一个 reader 创建出的 iterator 看到一致的 engine snapshot。
		panic("expected consistent iterators")
	}
	// Pin engine state eagerly so that all iterators created over this Reader are
	// based off the state of the engine as of this point and are mutually
	// consistent.
	// 选择本次读的 category，供 Pebble / storage 层做观测和调度分类。
	readCategory := fs.BatchEvalReadCategory
	for _, union := range ba.Requests {
		inner := union.GetInner()
		switch inner.(type) {
		case *kvpb.ScanRequest, *kvpb.ReverseScanRequest:
			// Scan / ReverseScan 可能读很多 key，使用专门的 scan read category。
			readCategory = batcheval.ScanReadCategory(ba.AdmissionHeader)
		}
		// 这里只需要知道 batch 里是否存在 scan 类请求，检查到第一个请求后就退出循环。
		break
	}
	if err := rw.PinEngineStateForIterators(readCategory); err != nil {
		// 提前 pin engine state，让之后从这个 reader 创建的 iterator 都基于同一时刻的快照。
		return nil, g, nil, kvpb.NewError(err)
	}
	if util.RaceEnabled {
		// race 构建下，用 spanset 包装 reader，校验 evaluation 只访问声明过的 latch spans。
		// 如果读到了未声明的 span，测试会更容易暴露并发控制 span 收集错误。
		spans := g.LatchSpans()
		spans.AddForbiddenMatcher(overlapsUnreplicatedRangeIDLocalKeys)
		spans.AddForbiddenMatcher(overlapsStoreLocalKeys)
		rw = spanset.NewReadWriterAt(rw, spans, ba.Timestamp)
	}
	// 关闭只读 reader，释放底层 iterator / snapshot 相关资源。
	defer rw.Close()

	// storage snapshot 已经创建，再检查一次执行条件。
	// 这类检查用于防止 snapshot 创建前后 lease/range 状态发生关键变化。
	if err := r.checkExecutionCanProceedAfterStorageSnapshot(ctx, ba, st); err != nil {
		return nil, g, nil, kvpb.NewError(err)
	}
	// 判断这个 read-only batch 是否可以在真正 evaluation 前提前释放 latches。
	// ok=true 表示已经确认不会和 lock table 中的锁冲突，可以先更新 timestamp cache 再释放 latches。
	// stillNeedsInterleavedIntents 表示后续 evaluation 是否仍需要处理 interleaved intents。
	ok, stillNeedsInterleavedIntents, pErr := r.canDropLatchesBeforeEval(ctx, rw, ba, g, st)
	if pErr != nil {
		// 这里的错误通常仍需要外层并发重试逻辑接管，所以把 guard 一起返回。
		return nil, g, nil, pErr
	}
	// 默认读路径：evaluation 时仍可能看到 interleaved intents。
	evalPath := readOnlyDefault
	if ok {
		// Since the concurrency manager has sequenced this request all the intents
		// that are in the concurrency manager's lock table, and we've scanned the
		// replicated lock-table keyspace above in `canDropLatchesBeforeEval`, we
		// can be sure that if we reached this point, we will not conflict with any
		// of them during evaluation. This in turn means that we can bump the
		// timestamp cache *before* evaluation without risk of starving writes.
		// Consequently, we're free to release latches here since we've acquired a
		// pebble iterator as long as we're performing a non-locking read (also
		// checked in `canDropLatchesBeforeEval`). Note that this also requires that
		// the request is not being optimistically evaluated (optimistic evaluation
		// does not wait for latches or check locks).
		//
		// 中文总结：
		// 对某些非锁定读请求，只要已经确认 lock table / replicated lock table 没有冲突，
		// 并且已经拿到了稳定的 Pebble snapshot，就可以提前释放 latches。
		// 这样后面的真实读 evaluation 不会继续挡住其它写请求。
		log.VEventf(ctx, 3, "lock table scan complete without conflicts; dropping latches early")
		r.store.metrics.ReplicaReadBatchDroppedLatchesBeforeEval.Inc(1)
		if !stillNeedsInterleavedIntents {
			// 如果确认不需要处理 interleaved intents，可以使用更轻的 evaluation 路径。
			r.store.metrics.ReplicaReadBatchWithoutInterleavingIter.Inc(1)
			evalPath = readOnlyWithoutInterleavedIntents
		}
		// 先更新 timestamp cache，再释放 latches。
		// 因为 latches 一旦释放，写请求就可能进来；timestamp cache 必须先记录这次读。
		r.updateTimestampCacheAndDropLatches(ctx, g, ba, nil /* br */, nil /* pErr */, st)
		// g 已经释放，后续不要再交给外层 defer/调用方释放。
		g = nil
	}

	var result result.Result
	// 真正执行 read-only batch。
	// 这里可能内部做 server-side refresh：比如读到 uncertainty interval 内的版本时，
	// 尝试前推 timestamp 后重新读，而不是直接把错误返回给客户端。
	ba, br, result, pErr = r.executeReadOnlyBatchWithServersideRefreshes(ctx, rw, rec, ba, g, &st, ui, evalPath)

	// If the request hit a server-side concurrency retry error, immediately
	// propagate the error. Don't assume ownership of the concurrency guard.
	// 如果 evaluation 返回的是外层 executeBatchWithConcurrencyRetries 能处理的并发重试错误，
	// 这里不要自行释放 guard，而是把 guard 原样交还给外层。
	if isConcurrencyRetryError(pErr) {
		if g != nil && g.EvalKind == concurrency.OptimisticEval {
			// Since this request was not holding latches, it could have raced with
			// intent resolution. So we can't trust it to add discovered locks, if
			// there is a latch conflict. This means that a discovered lock plus a
			// latch conflict will likely cause the request to evaluate at least 3
			// times: optimistically; pessimistically and add the discovered lock;
			// wait until resolution and evaluate pessimistically again.
			//
			// TODO(sumeer): scans and gets are correctly setting the resume span
			// when returning a LockConflictError. I am not sure about other
			// concurrency errors. We could narrow the spans we check the latch
			// conflicts for by using collectSpansRead as done below in the
			// non-error path.
			//
			// 乐观执行没有真正等待 latches，因此发现并发错误时还要额外确认：
			// 这次乐观读期间是否和其它请求发生了 latch 冲突。
			// 如果有冲突，就返回 OptimisticEvalConflictsError，让外层改用悲观执行重试。
			if !g.CheckOptimisticNoLatchConflicts() {
				log.Eventf(ctx, "optimistic evaluation failed with %s", pErr)
				return nil, g, nil, kvpb.NewError(kvpb.NewOptimisticEvalConflictsError())
			}
		}
		// 把当前 lease 信息附到错误上，帮助上层/客户端刷新路由或诊断 lease 问题。
		pErr = maybeAttachLease(pErr, &st.Lease)
		return nil, g, nil, pErr
	}

	if g != nil && g.EvalKind == concurrency.OptimisticEval {
		if pErr == nil {
			// Gather the spans that were read -- we distinguish the spans in the
			// request from the spans that were actually read, using resume spans in
			// the response.
			// 乐观读成功后，根据 response resume span 反推“实际读到的 spans”。
			// 这样可以只检查真实读过的范围，而不是整个请求声明的最大范围。
			latchSpansRead, lockSpansRead, err := r.collectSpansRead(ba, br)
			if err != nil {
				return nil, g, nil, kvpb.NewError(err)
			}
			defer latchSpansRead.Release()
			defer lockSpansRead.Release()
			// 乐观执行结束后做冲突校验。
			// 如果期间有重叠写或锁冲突，就让外层用悲观路径重新执行。
			if ok := g.CheckOptimisticNoConflicts(latchSpansRead, lockSpansRead); !ok {
				return nil, g, nil, kvpb.NewError(kvpb.NewOptimisticEvalConflictsError())
			}
		} else {
			// There was an error, that was not classified as a concurrency retry
			// error, and this request was not holding latches. This should be rare,
			// and in the interest of not having subtle correctness bugs, we retry
			// pessimistically.
			// 乐观执行时没有拿住 latches。如果出现非并发重试错误，为了避免隐藏的正确性问题，
			// 这里统一让外层转悲观路径重试一次。
			return nil, g, nil, kvpb.NewError(kvpb.NewOptimisticEvalConflictsError())
		}
	}

	// Handle any local (leaseholder-only) side-effects of the request.
	//
	// Processing these is fine for optimistic evaluation since these were non
	// conflicting intents so intent resolution could have been racing with this
	// request even if latches were held.
	// 取出 evaluation 过程中遇到的 intents。
	// 这些 intents 不属于本次读的返回数据，但需要后台/半同步尝试清理。
	intents := result.Local.DetachEncounteredIntents()

	// If QueryIntent reports a lock as missing, we must report it to the lock
	// manager.
	// QueryIntent 如果证明某个 lock 已经不存在，要通知 lock manager，
	// 避免之后还把这个 lock 当成真实冲突继续等待。
	missingLocks := result.Local.DetachMissingLocks()
	for i := range missingLocks {
		r.concMgr.OnLockMissing(ctx, &missingLocks[i])
	}

	if pErr == nil {
		// 处理 read-only evaluation 产生的本地副作用，比如 metrics / leaseholder-local 状态。
		pErr = r.handleReadOnlyLocalEvalResult(ctx, ba, result.Local)
	}
	if g != nil {
		// If we didn't already drop latches earlier, do so now.
		// 如果前面没有提前释放 latches，这里在读完成后更新 timestamp cache 并释放 guard。
		r.updateTimestampCacheAndDropLatches(ctx, g, ba, br, pErr, st)
		g = nil
	}

	// Semi-synchronously process any intents that need resolving here in order
	// to apply back pressure on the client which generated them. The resolution
	// is semi-synchronous in that there is a limited number of outstanding
	// asynchronous resolution tasks allowed after which further calls will
	// block. The limited number of asynchronous resolution tasks ensures that
	// the number of goroutines doing intent resolution does not diverge from
	// the number of workload goroutines (see
	// https://github.com/cockroachdb/cockroach/issues/4925#issuecomment-193015586
	// for an old problem predating such a limit).
	if len(intents) > 0 {
		// 把读过程中遇到的 intents 交给 intent resolver。
		// 这不是本次读的主路径返回值，但会帮助后续读减少冲突和等待。
		log.Eventf(ctx, "submitting %d intents to asynchronous processing", len(intents))
		// We only allow synchronous intent resolution for consistent requests.
		// Intent resolution is async/best-effort for inconsistent requests and
		// for requests using the SkipLocked wait policy.
		//
		// An important case where this logic is necessary is for RangeLookup
		// requests. In their case, synchronous intent resolution can deadlock
		// if the request originated from the local node which means the local
		// range descriptor cache has an in-flight RangeLookup request which
		// prohibits any concurrent requests for the same range. See #17760.
		// 只有一致性读且不是 SKIP LOCKED 时，才允许半同步清理 intents。
		// 不一致读 / SKIP LOCKED 本来就允许绕开部分锁语义，所以 intent 清理走异步 best-effort。
		allowSyncProcessing := ba.ReadConsistency == kvpb.CONSISTENT &&
			ba.WaitPolicy != lock.WaitPolicy_SkipLocked
		if err := r.store.intentResolver.CleanupIntentsAsync(
			ctx,
			ba.AdmissionHeader,
			intents,
			allowSyncProcessing,
		); err != nil {
			log.KvExec.Warningf(ctx, "%v", err)
		}
	}

	if pErr != nil {
		// 读最终失败，记录 verbose 错误事件。
		log.VErrEventf(ctx, 3, "%v", pErr.String())
	} else {
		// 读成功，统计本次读到的 key 数和字节数，供 load based split / replica load 等逻辑使用。
		keysRead, bytesRead := getBatchResponseReadStats(br)
		r.loadStats.RecordReadKeys(keysRead)
		r.loadStats.RecordReadBytes(bytesRead)
		log.Event(ctx, "read completed")
	}
	// read-only batch 不产生 StoreWriteBytes；guard 已经在本函数内释放或错误路径交还给外层。
	return br, nil, nil, pErr
}

// updateTimestampCacheAndDropLatches updates the timestamp cache and releases
// the concurrency guard.
// Note:
// - If `br` is nil, then this method assumes that latches are being released
// before evaluation of the request, and the timestamp cache is updated based
// only on the spans declared in the request.
// - The update to the timestamp cache is not gated on pErr == nil, since
// certain semantic errors (e.g. ConditionFailedError on CPut) require updating
// the timestamp cache (see updatesTSCacheOnErr).
// - For optimistic evaluation, used for limited scans, the update to the
// timestamp cache limits itself to the spans that were read, by using the
// ResumeSpans.
func (r *Replica) updateTimestampCacheAndDropLatches(
	ctx context.Context,
	g *concurrency.Guard,
	ba *kvpb.BatchRequest,
	br *kvpb.BatchResponse,
	pErr *kvpb.Error,
	st kvserverpb.LeaseStatus,
) {
	ec := makeUnreplicatedEndCmds(r, g, st)
	ec.done(ctx, ba, br, pErr)
}

var allowDroppingLatchesBeforeEval = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.transaction.dropping_latches_before_eval.enabled",
	"if enabled, allows certain read-only KV requests to drop latches before they evaluate",
	true,
)

// canDropLatchesBeforeEval 判断当前请求是否可以"在 evaluation 前提前释放 latches"。
// 背景：
//   正常情况下，一个读请求在 evaluation 期间一直持有 latches，
//   这会阻塞所有与该 key range 重叠的写请求，直到读完成。
//   但实际上，读请求在 pin 了 Pebble snapshot 之后，如果 lock table
//   里没有冲突的 intent，读结果就不会被并发写影响。
//   因此我们可以在 pin engine state + 确认无冲突后，提前释放 latches，
//   让写请求可以立即执行——这显著提高了并发度。
//
// 返回值：
//   - ok: true 表示可以提前释放 latches
//   - stillNeedsIntentInterleaving: true 表示 evaluation 仍然需要
//     intent interleaving iterator（能同时看到 intent 提交前后的版本）
//   - pErr: 如果发现了冲突的 intent，返回 LockConflictError
//
// 这个函数的工作流程：
//
//  1. 资格检查（allowDroppingLatchesBeforeEval 开关 + canReadOnlyRequestDropLatchesBeforeEval）
//     只有非锁定的悲观读请求才有资格。
//     - 乐观执行没资格：因为不知道实际会读到哪些 key，无法提前更新 timestamp cache
//     - 锁定读没资格：锁定读需要在持锁状态下执行
//     - SkipLocked 没资格：跳过的 key 不该进 timestamp cache
//
//  2. 扫描 lock table keyspace（replicated locks 的存储区域）
//     通过 ScanConflictingIntentsForDroppingLatchesEarly 在 Pebble
//     上直接扫描 engine key 层的锁记录，找到和当前请求 key range
//     有重叠的 intent。
//     - 如果没有冲突 → ok=true，可以安全释放 latches
//     - 如果发现冲突 → 返回 LockConflictError，让外层并发重试逻辑处理
//
//  3. 如果有 intent 需要读取"意图历史"（intent 的旧版本也对这个请求可见），
//     返回 stillNeedsIntentInterleaving=true，后续 evaluation 使用
//     readOnlyDefault（带 interleaving iterator）路径而非
//     readOnlyWithoutInterleavedIntents 路径。
//
// [1] 为什么提前释放 latches 是安全的？
//     读请求已经在前面 pin 了 Pebble engine state（PinEngineStateForIterators），
//     所以后续所有 iterator 看到的都是同一个 snapshot。
//     只要 lock table 中没有冲突的 intent，这个 snapshot 的数据就是最终数据。
//     后来的写请求即使拿到 latches 开始写，也不会影响已 pin 的 snapshot。
//
// [2] 为什么还要更新 timestamp cache 再释放 latches？
//     在释放 latches 之前，必须先更新 timestamp cache（记录这次读的时间戳）。
//     这样后来的写请求在 timestamp cache 中能看到"这个 key range 在 ts=X 被读过"，
//     从而知道自己的写时间戳至少要 > X。如果不先更新就释放 latches，
//     后来的写请求可能写一个比这次读更早的时间戳，破坏快照隔离。
func (r *Replica) canDropLatchesBeforeEval(
	ctx context.Context,
	rw storage.ReadWriter,
	ba *kvpb.BatchRequest,
	g *concurrency.Guard,
	st kvserverpb.LeaseStatus,
) (ok, stillNeedsIntentInterleaving bool, pErr *kvpb.Error) {
	// ──────────────────────────────────────────────
	// 步骤 1：资格检查
	// ──────────────────────────────────────────────
	// 两个条件：
	//   - 集群设置允许（默认开启）
	//   - 请求满足资格：非锁定、悲观执行、非 SkipLocked、一致性读
	if !allowDroppingLatchesBeforeEval.Get(&r.store.cfg.Settings.SV) ||
		!canReadOnlyRequestDropLatchesBeforeEval(ba, g) {
		// 没有资格：不提前释放 latches，使用普通 interleaving iterator
		return false /* ok */, true /* stillNeedsIntentInterleaving */, nil
	}

	log.VEventf(
		ctx, 3, "can drop latches early for batch (%v); scanning lock table first to detect conflicts", ba,
	)

	// ──────────────────────────────────────────────
	// 步骤 2：扫描 Pebble engine key 层的 lock 记录
	// ──────────────────────────────────────────────
	// 这里直接读 Pebble 上的 replicated lock table keyspace，
	// 而不是读内存 lock table。因为锁可能还没被换入内存锁表——
	// 锁表是在 evaluation 过程中遇到 intent 时才动态填充的。
	//
	// 限制每次最多扫描的冲突数量，避免一次发现太多冲突导致 OOM。
	maxLockConflicts := storage.MaxConflictsPerLockConflictError.Get(&r.store.cfg.Settings.SV)
	targetLockConflictBytes := storage.TargetBytesPerLockConflictError.Get(&r.store.cfg.Settings.SV)
	var intents []roachpb.Intent

	for _, req := range ba.Requests {
		reqHeader := req.GetInner().Header()
		start, end := reqHeader.Key, reqHeader.EndKey
		// 如果是事务请求，传 txnID 以便识别自己的 intent（自己的不算冲突）。
		var txnID uuid.UUID
		if ba.Txn != nil {
			txnID = ba.Txn.ID
		}
		// 在 Pebble 上扫描 [start, end) 范围内的 engine key，
		// 找到属于其他事务的 intent。
		needsIntentInterleavingForThisRequest, err := storage.ScanConflictingIntentsForDroppingLatchesEarly(
			ctx, rw, txnID, ba.Header.Timestamp, start, end, &intents, maxLockConflicts, targetLockConflictBytes,
		)
		if err != nil {
			return false /* ok */, true /* stillNeedsIntentInterleaving */, kvpb.NewError(
				errors.Wrap(err, "scanning intents"),
			)
		}
		// 只要有一个 request 需要看 intent 历史，整个 batch 就需要 interleaving iterator。
		stillNeedsIntentInterleaving = stillNeedsIntentInterleaving || needsIntentInterleavingForThisRequest
		// 达到冲突上限就不继续扫了。
		if maxLockConflicts != 0 && int64(len(intents)) >= maxLockConflicts {
			break
		}
	}

	// ──────────────────────────────────────────────
	// 步骤 3：根据扫描结果决定
	// ──────────────────────────────────────────────
	if len(intents) > 0 {
		// 发现了冲突的 intent → 不能提前释放 latches。
		// 返回 LockConflictError，外层 executeBatchWithConcurrencyRetries
		// 会处理这些冲突（把 intent 加入内存锁表、等待锁释放等）。
		return false /* ok */, false /* stillNeedsIntentInterleaving */, maybeAttachLease(
			kvpb.NewError(&kvpb.LockConflictError{Locks: roachpb.AsLocks(intents)}), &st.Lease,
		)
	}

	// 没有冲突的 intent → 可以安全地提前释放 latches！
	// 调用方（executeReadOnlyBatch 中的 updateTimestampCacheAndDropLatches）
	// 会先更新 timestamp cache（记录这次读的时间戳），再释放 latches。
	return true /* ok */, stillNeedsIntentInterleaving, nil
}

// evalContextWithAccount wraps an EvalContext to provide a non-nil
// mon.BoundAccount. This wrapping is conditional on various factors, and
// specific to a request (see executeReadOnlyBatchWithServersideRefreshes),
// which is why the implementation of EvalContext by Replica does not by
// default provide a non-nil mon.BoundAccount.
//
// If we start using evalContextWithAccount on more code paths we should
// consider using it everywhere and lift it to an earlier point in the code.
// Then code that decides that we need a non-nil BoundAccount can set a field
// instead of wrapping.
type evalContextWithAccount struct {
	batcheval.EvalContext
	memAccount *mon.BoundAccount
}

var evalContextWithAccountPool = sync.Pool{
	New: func() interface{} {
		return &evalContextWithAccount{}
	},
}

// newEvalContextWithAccount creates an evalContextWithAccount with an account
// connected to the given monitor. It uses a sync.Pool.
func newEvalContextWithAccount(
	ctx context.Context, evalCtx batcheval.EvalContext, mon *mon.BytesMonitor,
) *evalContextWithAccount {
	ec := evalContextWithAccountPool.Get().(*evalContextWithAccount)
	ec.EvalContext = evalCtx
	if ec.memAccount != nil {
		ec.memAccount.Init(ctx, mon)
	} else {
		acc := mon.MakeBoundAccount()
		ec.memAccount = &acc
	}
	return ec
}

// close returns the accounted memory and returns objects to the sync.Pool.
func (e *evalContextWithAccount) close(ctx context.Context) {
	e.memAccount.Close(ctx)
	// Clear the BoundAccount struct, so it can be later reused.
	*e.memAccount = mon.BoundAccount{}
	evalContextWithAccountPool.Put(e)
}
func (e evalContextWithAccount) GetResponseMemoryAccount() *mon.BoundAccount {
	return e.memAccount
}

// batchEvalPath enumerates the different evaluation paths that can be taken by
// a batch.
type batchEvalPath int

const (
	// readOnlyDefault is the default evaluation path taken by read only requests.
	readOnlyDefault batchEvalPath = iota
	// readOnlyWithoutInterleavedIntents indicates that the request does not need
	// an intent interleaving iterator during its evaluation.
	readOnlyWithoutInterleavedIntents
	readWrite
)

// executeReadOnlyBatchWithServersideRefreshes 执行实际的读 batch evaluation，
// 并在遇到某些可重试错误时，尝试在服务端前推时间戳后重试一次。
//
// 核心设计：server-side refresh（服务端刷新）
//
//   读请求在 MVCC 迭代过程中可能碰到两种时间戳相关的错误：
//   1. ReadWithinUncertaintyIntervalError：读到了 uncertainty interval 内的值
//      （某个已提交的值的时间戳在我们的 read ts 之上、但在 GlobalLimit 之下）
//   2. WriteTooOldError：读到了一个比自己时间戳更"新"的值（非事务请求）
//
//   这两种情况下，如果条件允许，可以直接在服务端把 batch 的时间戳前推，
//   然后重新执行 evaluateBatch，而不需要把错误返回给客户端。
//
//   限制：最多重试 1 次（retries > 0 → break）。
//
//   重要前提：如果 latches 已经释放（g == nil），则不能做 server-side retry。
//   因为 released latches 意味着"这个 read timestamp 已经被写入了
//   timestamp cache"，后来的写请求已经可以看到并跳过它。
//   如果用更高的时间戳重试，新时间戳没有被 latches 保护，
//   也超出了 timestamp cache 的范围——不安全。
//
// 参数：
//   - rw: 已 pin 的 Pebble snapshot reader
//   - evalPath: 执行路径选择（readOnlyDefault / readOnlyWithoutInterleavedIntents）
//   - g: latches guard，nil 表示 latches 已经被提前释放
func (r *Replica) executeReadOnlyBatchWithServersideRefreshes(
	ctx context.Context,
	rw storage.ReadWriter,
	rec batcheval.EvalContext,
	ba *kvpb.BatchRequest,
	g *concurrency.Guard,
	st *kvserverpb.LeaseStatus,
	ui uncertainty.Interval,
	evalPath batchEvalPath,
) (_ *kvpb.BatchRequest, br *kvpb.BatchResponse, res result.Result, pErr *kvpb.Error) {
	log.Event(ctx, "executing read-only batch")

	// ──────────────────────────────────────────────
	// 内存分配追踪（memory accounting）
	// ──────────────────────────────────────────────
	// 对非本地请求（来自 gRPC）和未在源端预留内存的本地请求做内存追踪。
	// DistSQL 本地请求在发 KV 请求之前已经做了 accounting，不需要重复算。
	var rootMonitor *mon.BytesMonitor
	if ba.AdmissionHeader.SourceLocation != kvpb.AdmissionHeader_LOCAL ||
		ba.AdmissionHeader.NoMemoryReservedAtSource {
		rootMonitor = r.store.getRootMemoryMonitorForKV()
	}
	var boundAccount *mon.BoundAccount
	if rootMonitor != nil {
		evalCtx := newEvalContextWithAccount(ctx, rec, rootMonitor)
		boundAccount = evalCtx.memAccount
		defer evalCtx.close(ctx)
		rec = evalCtx
	}

	// ──────────────────────────────────────────────
	// 主循环：evaluateBatch → 判断错误 → (可能) server-side retry
	// ──────────────────────────────────────────────
	// 最多 2 次：首次 + 最多 1 次 server-side retry
	for retries := 0; ; retries++ {
		if retries > 0 {
			// 重试前清理上一次 evaluation 分配的内存。
			if boundAccount != nil {
				boundAccount.Clear(ctx)
			}
			log.VEventf(ctx, 2, "server-side retry of batch")
		}

		now := timeutil.Now()
		// ★ 真正的 evaluation：遍历 MVCC key，读数据。
		// evalPath 决定了是否使用 intent interleaving iterator。
		// ui 决定了什么范围内的值会触发 ReadWithinUncertaintyIntervalError。
		br, res, pErr = evaluateBatch(
			ctx, kvserverbase.CmdIDKey(""), rw, rec, nil /* ms */, ba, g,
			st, ui, evalPath, false, /* omitInRangefeeds */
		)
		r.store.metrics.ReplicaReadBatchEvaluationLatency.RecordValue(timeutil.Since(now).Nanoseconds())

		// 成功或已经重试过了（最多 1 次 retry）→ 退出
		if pErr == nil || retries > 0 {
			break
		}

		// ──────────────────────────────────────────────
		// 判断是否可以 server-side retry
		// ──────────────────────────────────────────────
		// 如果 latches 已提前释放（g == nil），禁止 retry。
		// 原因：latches 保护的是旧时间戳下的隔离性。
		//       timestamp cache 已经用旧时间戳更新过了。
		//       用新时间戳重试得不到应有的保护。
		latchesHeld := g != nil
		var ok bool
		if latchesHeld {
			// 检查：错误类型是否允许 retry？
			//   - 事务请求：ReadWithinUncertaintyIntervalError / WriteTooOld，
			//     且 ba.CanForwardReadTimestamp == true
			//   - 非事务请求：WriteTooOldError / ReadWithinUncertaintyIntervalError
			// 通过 tryBumpBatchTimestamp 提升 batch 时间戳。
			ba, ok = canDoServersideRetry(ctx, pErr, ba, g, hlc.Timestamp{})
		}
		if !ok {
			// 不能 retry → 错误返回给调用方
			r.store.Metrics().ReadEvaluationServerSideRetryFailure.Inc(1)
			break
		} else {
			// 可以 retry → ba 已经更新了新时间戳，回到循环顶部重新 evaluateBatch
			r.store.Metrics().ReadEvaluationServerSideRetrySuccess.Inc(1)
		}
	}

	// ──────────────────────────────────────────────
	// 错误时保留必要的 intent 相关数据
	// ──────────────────────────────────────────────
	// 即使读失败了，evaluation 过程中遇到的 intent 和 missing lock
	// 仍需要交给上层做后续处理（如 intent 清理、lock table 更新）。
	if pErr != nil {
		res.Local = result.LocalResult{
			ReportedMissingLocks: res.Local.ReportedMissingLocks,
			EncounteredIntents:   res.Local.DetachEncounteredIntents(),
			Metrics:              res.Local.Metrics,
		}
		return ba, nil, res, pErr
	}
	return ba, br, res, nil
}

func (r *Replica) handleReadOnlyLocalEvalResult(
	ctx context.Context, ba *kvpb.BatchRequest, lResult result.LocalResult,
) *kvpb.Error {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the method (which checks
	// that all fields were handled).
	{
		lResult.Reply = nil
	}

	if lResult.AcquiredLocks != nil {
		// These will all be unreplicated locks.
		log.Eventf(ctx, "acquiring %d unreplicated locks", len(lResult.AcquiredLocks))
		for i := range lResult.AcquiredLocks {
			r.concMgr.OnLockAcquired(ctx, &lResult.AcquiredLocks[i])
		}
		lResult.AcquiredLocks = nil
	}

	if !lResult.IsZero() {
		log.KvExec.Fatalf(ctx, "unhandled field in LocalEvalResult: %s", pretty.Diff(lResult, result.LocalResult{}))
	}
	return nil
}

// collectSpansRead uses the spans declared in the requests, and the resume
// spans in the responses, to construct the effective spans that were read,
// and uses that to compute the latch and lock spans.
func (r *Replica) collectSpansRead(
	ba *kvpb.BatchRequest, br *kvpb.BatchResponse,
) (latchSpans *spanset.SpanSet, lockSpans *lockspanset.LockSpanSet, _ error) {
	baCopy := *ba
	baCopy.Requests = make([]kvpb.RequestUnion, 0, len(ba.Requests))
	for i := 0; i < len(ba.Requests); i++ {
		baReq := ba.Requests[i]
		req := baReq.GetInner()
		header := req.Header()
		resp := br.Responses[i].GetInner()

		if ba.WaitPolicy == lock.WaitPolicy_SkipLocked && kvpb.CanSkipLocked(req) {
			if ba.IndexFetchSpec != nil {
				return nil, nil, errors.AssertionFailedf("unexpectedly IndexFetchSpec is set with SKIP LOCKED wait policy")
			}
			// If the request is using a SkipLocked wait policy, it behaves as if run
			// at a lower isolation level for any keys that it skips over. If the read
			// request did not return a key, it does not need to check for conflicts
			// with latches held on that key. Instead, the request only needs to check
			// for conflicting latches on the keys that were returned.
			//
			// To achieve this, we add a Get request for each of the keys in the
			// response's result set, even if the original request was a ranged scan.
			// This will lead to the returned span set (which is used for optimistic
			// eval validation) containing a set of point latch spans which correspond
			// to the response keys. Note that the Get requests are constructed with
			// the same key locking mode as the original read.
			//
			// This is similar to how the timestamp cache and refresh spans handle the
			// SkipLocked wait policy.
			if err := kvpb.ResponseKeyIterate(req, resp, func(key roachpb.Key) {
				// TODO(nvanbenschoten): we currently perform a per-response key memory
				// allocation. If this becomes an issue, we could pre-allocate chunks of
				// these structs to amortize the cost.
				getAlloc := new(struct {
					get   kvpb.GetRequest
					union kvpb.RequestUnion_Get
				})
				getAlloc.get.Key = key
				getAlloc.get.KeyLockingStrength,
					getAlloc.get.KeyLockingDurability = req.(kvpb.LockingReadRequest).KeyLocking()
				getAlloc.union.Get = &getAlloc.get
				ru := kvpb.RequestUnion{Value: &getAlloc.union}
				baCopy.Requests = append(baCopy.Requests, ru)
			}, false /* includeLockedNonExisting */); err != nil {
				return nil, nil, err
			}
			continue
		}

		if resp.Header().ResumeSpan == nil {
			// Fully evaluated.
			baCopy.Requests = append(baCopy.Requests, baReq)
			continue
		}

		switch t := resp.(type) {
		case *kvpb.GetResponse:
			// The request did not evaluate. Ignore it.
			continue
		case *kvpb.ScanResponse:
			if header.Key.Equal(t.ResumeSpan.Key) {
				// The request did not evaluate. Ignore it.
				continue
			}
			// The scan will resume at the inclusive ResumeSpan.Key. So
			// ResumeSpan.Key has not been read and becomes the exclusive end key of
			// what was read.
			header.EndKey = t.ResumeSpan.Key
		case *kvpb.ReverseScanResponse:
			if header.EndKey.Equal(t.ResumeSpan.EndKey) {
				// The request did not evaluate. Ignore it.
				continue
			}
			// The scan will resume at the exclusive ResumeSpan.EndKey and proceed
			// towards the current header.Key. So ResumeSpan.EndKey has been read
			// and becomes the inclusive start key of what was read.
			header.Key = t.ResumeSpan.EndKey
		default:
			// Consider it fully evaluated, which is safe.
			baCopy.Requests = append(baCopy.Requests, baReq)
			continue
		}
		// The ResumeSpan has changed the header.
		var ru kvpb.RequestUnion
		req = req.ShallowCopy()
		req.SetHeader(header)
		ru.MustSetInner(req)
		baCopy.Requests = append(baCopy.Requests, ru)
	}

	// Collect the batch's declared spans again, this time with the
	// span bounds constrained to what was read.
	var err error
	latchSpans, lockSpans, _, err = r.collectSpans(&baCopy)
	return latchSpans, lockSpans, err
}

func getBatchResponseReadStats(br *kvpb.BatchResponse) (float64, float64) {
	var keys, bytes float64
	for _, reply := range br.Responses {
		h := reply.GetInner().Header()
		if keysRead := h.NumKeys; keysRead > 0 {
			keys += float64(keysRead)
			bytes += float64(h.NumBytes)
		}
	}
	return keys, bytes
}
