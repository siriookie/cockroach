// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"reflect"
	"runtime/trace"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/poison"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicastats"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/obs/workloadid"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var optimisticEvalLimitedScans = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.concurrency.optimistic_eval_limited_scans.enabled",
	"when true, limited scans are optimistically evaluated in the sense of not checking for "+
		"conflicting latches or locks up front for the full key range of the scan, and instead "+
		"subsequently checking for conflicts only over the key range that was read",
	true,
)

// Send executes a command on this range, dispatching it to the
// read-only, read-write, or admin execution path as appropriate.
// ctx should contain the log tags from the store (and up).
//
// A rough schematic for the path requests take through a Replica
// is presented below, with a focus on where requests may spend
// most of their time (once they arrive at the Node.Batch endpoint).
//
//	                  DistSender (tenant)
//	                       │
//	                       ┆ (RPC)
//	                       │
//	                       ▼
//	                  Node.Batch (host cluster)
//	                       │
//	                       ▼
//	               Admission control
//	                       │
//	                       ▼
//	                  Replica.Send
//	                       │
//	                 Circuit breaker
//	                       │
//	                       ▼
//	       Replica.maybeBackpressureBatch (if Range too large)
//	                       │
//	                       ▼
//	Replica.maybeRateLimitBatch (tenant rate limits)
//	                       │
//	                       ▼
//	  Replica.maybeCommitWaitBeforeCommitTrigger (if committing with commit-trigger)
//	                       │
//
// read-write ◄─────────────────────────┴────────────────────────► read-only
//
//	│                                                               │
//	│                                                               │
//	├─────────────► executeBatchWithConcurrencyRetries ◄────────────┤
//	│               (handles leases and txn conflicts)              │
//	│                                                               │
//	▼                                                               │
//
// executeWriteBatch                                                   │
//
//	│                                                               │
//	▼                                                               ▼
//
// evalAndPropose         (turns the BatchRequest        executeReadOnlyBatch
//
//	│                   into pebble WriteBatch)
//	│
//	├──────────────────► (writes that can use async consensus do not
//	│                     wait for replication and are done here)
//	│
//	├──────────────────► maybeAcquireProposalQuota
//	│                    (applies backpressure in case of
//	│                     lagging Raft followers)
//	│
//	│
//	▼
//
// handleRaftReady        (drives the Raft loop, first appending to the log
//
//	to commit the command, then signaling proposer and
//	applying the command)
func (r *Replica) Send(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	br, writeBytes, pErr := r.SendWithWriteBytes(ctx, ba)
	writeBytes.Release()
	return br, pErr
}

// SendWithWriteBytes is the implementation of Send with an additional
// *StoreWriteBytes return value.
func (r *Replica) SendWithWriteBytes(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvadmission.StoreWriteBytes, *kvpb.Error) {
	// 从 ctx 中取出客户端 tenant。取不到时返回 zero tenant。
	// 这个 tenant 主要用于 profiling label、rate limiter 归因等观测/限流逻辑。
	tenantIDOrZero, _ := roachpb.ClientTenantFromContext(ctx)

	// Record the CPU time processing the request for this replica. This is
	// recorded regardless of errors that are encountered.
	// 记录本次请求在这个 replica 上消耗 CPU 的起点。
	// 无论后面成功还是失败，defer 都会把 CPU 用量记到 replica 指标里。
	startCPU := grunning.Time()
	defer r.MeasureReqCPUNanos(ctx, startCPU)

	if r.store.cfg.Settings.CPUProfileType() == cluster.CPUProfileWithLabels {
		// 如果开启带标签的 CPU profile，把 range 和 tenant 信息挂到 goroutine label。
		// 这样 profile 里能看到 CPU 是哪个 range / tenant 消耗的。
		var reset func()
		if tenantIDOrZero.IsSet() {
			ctx, reset = pprofutil.SetProfilerLabels(ctx, "range_str", r.rangeStr.ID(), "tenant_id", tenantIDOrZero.String())
		} else {
			ctx, reset = pprofutil.SetProfilerLabels(ctx, "range_str", r.rangeStr.ID())
		}
		// 请求结束后恢复原 label，避免污染同一个 goroutine 后续处理的请求。
		defer reset()
	}

	if trace.IsEnabled() {
		// runtime/trace 开启时，尝试从 BatchRequest.ProfileLabels 中找到 workload tag。
		// 找到后用它建立 trace region，方便把这段 Replica 执行归到具体 workload。
		foundLabel := ""
		for i, l := range ba.ProfileLabels {
			if i%2 == 0 && l == workloadid.ProfileTag && i < len(ba.ProfileLabels)-1 {
				// This label is set in conn_executor_exec if tracing is active.
				foundLabel = ba.ProfileLabels[i+1]
				break
			}
		}
		// This construction avoids calling `defer` in a loop which is
		// not permitted by our linter.
		if foundLabel != "" {
			defer trace.StartRegion(ctx, foundLabel).End()
		}
		// 再加一个 range 维度的 trace region，标明请求正在这个 replica/range 上执行。
		defer trace.StartRegion(ctx, r.rangeStr.String() /* cheap */).End()
	}
	// Add the range log tag.
	// 给 ctx 加上 range 相关 log tag，后续日志能带上 rXX 等信息。
	ctx = r.AnnotateCtx(ctx)

	// 先判断 batch 是否纯读。后面会据此选择 read-only 还是 read-write 执行路径。
	isReadOnly := ba.IsReadOnly()
	// 做 Replica 级基本校验，比如请求类型和 range 状态是否匹配。
	if err := r.checkBatchRequest(ba, isReadOnly); err != nil {
		return nil, nil, kvpb.NewError(err)
	}

	// 如果 range 过大或其它条件要求对请求施加 backpressure，这里会提前拦住。
	if err := r.maybeBackpressureBatch(ctx, ba); err != nil {
		return nil, nil, kvpb.NewError(err)
	}
	// tenant / range 级速率限制检查。触发时直接返回错误，不继续执行请求。
	if err := r.maybeRateLimitBatch(ctx, ba, tenantIDOrZero); err != nil {
		return nil, nil, kvpb.NewError(err)
	}
	// 如果这个 EndTxn 带 commit trigger，可能需要在执行 trigger 前提前做 commit-wait。
	// 这能避免 commit trigger 的副作用先于事务其它写入对外可见。
	if err := r.maybeCommitWaitBeforeCommitTrigger(ctx, ba); err != nil {
		return nil, nil, kvpb.NewError(err)
	}

	// NB: must be performed before collecting request spans.
	// 清理 batch 中已经不需要的 in-flight writes。
	// 这一步必须在收集 request spans 前完成，否则 span 计算可能包含不该参与的写。
	ba, err := maybeStripInFlightWrites(ba)
	if err != nil {
		return nil, nil, kvpb.NewError(err)
	}

	// 测试钩子：允许测试在真正执行前注入错误或观察请求。
	if filter := r.store.cfg.TestingKnobs.TestingRequestFilter; filter != nil {
		if pErr := filter(ctx, ba); pErr != nil {
			return nil, nil, pErr
		}
	}

	// Differentiate between read-write, read-only, and admin.
	// 根据 batch 类型进入不同执行路径。
	// read-only/read-write 都会经过 executeBatchWithConcurrencyRetries，
	// 由它处理 latch/lock/lease 等并发重试。
	var br *kvpb.BatchResponse
	var pErr *kvpb.Error
	var writeBytes *kvadmission.StoreWriteBytes
	if isReadOnly {
		log.Event(ctx, "read-only path")
		// 纯读路径：不会产生写入字节数，所以忽略第二个返回值。
		fn := (*Replica).executeReadOnlyBatch
		br, _, pErr = r.executeBatchWithConcurrencyRetries(ctx, ba, fn)
	} else if ba.IsWrite() {
		log.Event(ctx, "read-write path")
		// 写路径：执行后会返回 StoreWriteBytes，用于上层 admission 反馈实际写入量。
		fn := (*Replica).executeWriteBatch
		br, writeBytes, pErr = r.executeBatchWithConcurrencyRetries(ctx, ba, fn)
	} else if ba.IsAdmin() {
		log.Event(ctx, "admin path")
		// admin request 走独立路径，不通过普通 read/write batch 执行器。
		br, pErr = r.executeAdminBatch(ctx, ba)
	} else if len(ba.Requests) == 0 {
		// empty batch; shouldn't happen (we could handle it, but it hints
		// at someone doing weird things, and once we drop the key range
		// from the header it won't be clear how to route those requests).
		log.KvExec.Fatalf(ctx, "empty batch")
	} else {
		log.KvExec.Fatalf(ctx, "don't know how to handle command %s", ba)
	}
	if pErr != nil {
		// 记录 replica 执行返回的结构化 KV 错误，比如 NotLeaseHolder、
		// RangeKeyMismatch、TransactionRetry 等。
		log.Eventf(ctx, "replica.Send got error: %s", pErr)
	} else {
		// 测试钩子：允许测试在响应返回前检查或注入错误。
		if filter := r.store.cfg.TestingKnobs.TestingResponseFilter; filter != nil {
			pErr = filter(ctx, ba, br)
		}
	}

	// 计算本次请求在 Replica.SendWithWriteBytes 中消耗的 CPU 时间。
	cpuTime := grunning.Difference(startCPU, grunning.Time())
	if br != nil {
		// 把 CPU 时间放进 BatchResponse，供上层统计/观测使用。
		br.CPUTime = int64(cpuTime)
	}
	if pErr == nil {
		// Return range information if it was requested. Note that we don't return it
		// on errors because the code doesn't currently support returning both a br
		// and a pErr here. Also, some errors (e.g. NotLeaseholderError) have custom
		// ways of returning range info.
		// 成功时按需把 range info 加到响应里，帮助客户端更新 range cache。
		r.maybeAddRangeInfoToResponse(ctx, ba, br)
		// Handle load-based splitting, if necessary.
		// 根据这次请求的负载记录信息，必要时触发 load-based split。
		r.recordBatchForLoadBasedSplitting(ctx, ba, br, int(cpuTime))
	}

	// Record summary throughput information about the batch request for
	// accounting.
	// 记录 batch 级吞吐统计，用于 replica/load/admission 等观测和调度。
	r.recordBatchRequestLoad(ctx, ba)
	if writeBytes != nil {
		// 写请求返回的实际写入字节数，也记到 replica 负载统计里。
		r.recordRequestWriteBytes(writeBytes.WriteBytes + writeBytes.IngestedBytes)
	}
	// 把本次请求对 rate limiter 的影响记录回去，供后续限流判断使用。
	r.recordImpactOnRateLimiter(ctx, br, isReadOnly)
	// 返回响应、写入字节数以及结构化 KV 错误。
	// writeBytes 会在上层 Node.batchInternal 中交给 KVAdmissionController 做结算。
	return br, writeBytes, pErr
}

// maybeCommitWaitBeforeCommitTrigger detects batches that are attempting to
// commit a transaction with a commit trigger and that will need to perform a
// commit-wait at some point. For reasons described below, transactions with
// commit triggers need to perform their commit wait sleep before their trigger
// runs, so this function eagerly performs that sleep before the batch moves on
// to evaluation. The function guarantees that if the transaction ends up
// committing with its current provisional commit timestamp, it will not need to
// commit wait any further.
func (r *Replica) maybeCommitWaitBeforeCommitTrigger(
	ctx context.Context, ba *kvpb.BatchRequest,
) error {
	// 先看 batch 里有没有 EndTxnRequest。
	// 只有事务提交阶段才可能需要 commit-wait；普通读写请求直接跳过。
	args, hasET := ba.GetArg(kvpb.EndTxn)
	if !hasET {
		return nil
	}
	et := args.(*kvpb.EndTxnRequest)
	// 只有“提交事务 + 带内部 commit trigger”的 EndTxn 才需要特殊处理。
	// abort 事务、普通 EndTxn、没有 trigger 的事务都可以走普通路径。
	if !et.Commit || et.InternalCommitTrigger == nil {
		// Not committing with a commit trigger.
		return nil
	}
	txn := ba.Txn
	// 如果事务的读时间戳和写时间戳已经不一致，并且这个 batch 又不能前推读时间戳，
	// 那么这次提交不可能以当前状态成功；这里没必要提前 commit-wait。
	if txn.ReadTimestamp != txn.WriteTimestamp && !ba.CanForwardReadTimestamp {
		// The commit can not succeed.
		return nil
	}

	// A transaction is committing with a commit trigger. This means that it has
	// side-effects beyond those of the intents that it has written.
	//
	// If the transaction has a commit timestamp in the future of present time, it
	// will need to commit-wait before acknowledging the client. Typically, this
	// is performed in the TxnCoordSender after the transaction has committed and
	// resolved its intents (see TxnCoordSender.maybeCommitWait). It is safe to
	// wait after a future-time transaction has committed and resolved intents
	// without compromising linearizability because the uncertainty interval of
	// concurrent and later readers ensures atomic visibility of the effects of
	// the transaction. In other words, all of the transaction's intents will
	// become visible and will remain visible at once, which is sometimes called
	// "monotonic reads". This is true even if the resolved intents are at a high
	// enough timestamp such that they are not visible to concurrent readers
	// immediately after they are resolved, but only become visible sometime
	// during the writer's commit-wait sleep. This property is central to the
	// correctness of non-blocking transactions. See:
	//   https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20200811_non_blocking_txns.md
	//
	// However, if a transaction has a commit trigger, the side-effects of the
	// trigger will go into effect immediately after the EndTxn's Raft command is
	// applied to the Raft state machine. This poses a problem, because we do not
	// want part of a transaction's effects (e.g. its commit trigger) to become
	// visible to onlookers before the rest of its effects do (e.g. its intent
	// writes). To avoid this problem, we perform the commit-wait stage of a
	// transaction with a commit trigger early, before its commit triggers fire.
	// This results in the transaction waiting longer to commit and resolve its
	// intents, but is otherwise safe and effective.
	//
	// NOTE: it would be easier to perform this wait during the evaluation of the
	// corresponding EndTxn request instead of detecting the case here. However,
	// we intentionally do not commit wait during evaluation because we do not
	// want to sleep while holding latches and blocking other requests. So
	// instead, we commit wait here and then assert that transactions with commit
	// triggers do not need to commit wait further by the time they reach command
	// evaluation.
	//
	// 中文版核心逻辑：
	// 普通事务的写入是 intents。即使 commit timestamp 在未来，
	// 提交后再 sleep 一段 commit-wait 也可以，因为读者的 uncertainty interval
	// 能保证事务写入不会被“半可见”地观察到。
	//
	// 但带 commit trigger 的事务不一样。commit trigger 会在 EndTxn 的 Raft command
	// apply 时立即产生副作用，比如 split、merge、descriptor 变更等。
	// 如果先 apply trigger，再在客户端侧 commit-wait，就可能出现：
	//   trigger 的副作用已经对外可见，
	//   但事务其它 intent 写入还因为未来时间戳暂时不可见。
	// 这会破坏事务效果的原子可见性。
	//
	// 所以这里在进入 evaluation、拿 latch、执行 trigger 之前，就先完成 commit-wait。
	if txn.WriteTimestamp.LessEq(r.Clock().Now()) {
		// No wait fast-path. This is the common case for most transactions. Only
		// transactions who have their commit timestamp bumped into the future will
		// need to wait.
		// 如果写时间戳已经不在当前物理时间的未来，就不需要 sleep。
		// 这是绝大多数事务的快速路径。
		return nil
	}

	// 需要等到本地物理时间追上事务的写时间戳。
	// 等完后，如果事务按这个 provisional commit timestamp 提交，
	// 后续就不需要再做 commit-wait。
	waitUntil := txn.WriteTimestamp
	before := r.Clock().PhysicalTime()
	est := waitUntil.GoTime().Sub(before)
	log.VEventf(ctx, 1, "performing server-side commit-wait sleep for ~%s", est)

	// SleepUntil 会响应 ctx cancellation。
	// 如果请求超时或节点关闭，会把错误返回给上层，不继续执行 commit trigger。
	if err := r.Clock().SleepUntil(ctx, waitUntil); err != nil {
		return err
	}

	after := r.Clock().PhysicalTime()
	log.VEventf(ctx, 1, "completed server-side commit-wait sleep, took %s", after.Sub(before))
	// 记录一次“在 commit trigger 前提前做 commit-wait”的指标。
	r.store.metrics.CommitWaitsBeforeCommitTrigger.Inc(1)
	return nil
}

// maybeAddRangeInfoToResponse populates br.RangeInfo if the client doesn't
// have up-to-date info about the range's descriptor and lease.
func (r *Replica) maybeAddRangeInfoToResponse(
	ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse,
) {
	// Only return range info if ClientRangeInfo is non-empty. In particular, we
	// don't want to populate this for lease requests, since these bypass
	// DistSender and never use ClientRangeInfo.
	//
	// From 23.2, all DistSenders ensure ExplicitlyRequested is set when otherwise
	// empty.
	if ba.ClientRangeInfo == (roachpb.ClientRangeInfo{}) {
		return
	}

	// Compare the client's info with the replica's info to detect if the client
	// has stale knowledge. Note that the client can have more recent knowledge
	// than the replica in case this is a follower.
	cri := &ba.ClientRangeInfo
	ri := r.GetRangeInfo(ctx)
	needInfo := cri.ExplicitlyRequested ||
		(cri.DescriptorGeneration < ri.Desc.Generation) ||
		(cri.LeaseSequence < ri.Lease.Sequence) ||
		(cri.ClosedTimestampPolicy != ri.ClosedTimestampPolicy)
	if !needInfo {
		return
	}
	log.VEventf(ctx, 3, "client had stale range info; returning an update")
	br.RangeInfos = []roachpb.RangeInfo{ri}

	// We're going to sometimes return info on the ranges coming right before or
	// right after r, if it looks like r came from a range that has recently split
	// and the client doesn't know about it. After a split, the client benefits
	// from learning about both resulting ranges.

	if cri.DescriptorGeneration >= ri.Desc.Generation {
		return
	}

	maybeAddRange := func(repl *Replica) {
		if repl.Desc().Generation != ri.Desc.Generation {
			// The next range does not look like it came from a split that produced
			// both r and this next range. Of course, this has false negatives (e.g.
			// if either the LHS or the RHS split multiple times since the client's
			// version). For best fidelity, the client could send the range's start
			// and end keys and the server could use that to return all overlapping
			// descriptors (like we do for RangeKeyMismatchErrors), but sending those
			// keys on every RPC seems too expensive.
			return
		}

		// Note that we return the lease even if it's expired. The kvclient can
		// use it as it sees fit.
		br.RangeInfos = append(br.RangeInfos, repl.GetRangeInfo(ctx))
	}

	if repl := r.store.lookupPrecedingReplica(ri.Desc.StartKey); repl != nil {
		maybeAddRange(repl)
	}
	if repl := r.store.LookupReplica(ri.Desc.EndKey); repl != nil {
		maybeAddRange(repl)
	}
}

// batchExecutionFn is a method on Replica that executes a BatchRequest. It is
// called with the batch, along with a guard for the latches protecting the
// request.
//
// The function will return either a batch response or an error. The function
// also has the option to pass ownership of the concurrency guard back to the
// caller. However, it does not need to. Instead, it can assume responsibility
// for releasing the concurrency guard it was provided by returning nil. This is
// useful is cases where the function:
//  1. eagerly released the concurrency guard after it determined that isolation
//     from conflicting requests was no longer needed.
//  2. is continuing to execute asynchronously and needs to maintain isolation
//     from conflicting requests throughout the lifetime of its asynchronous
//     processing. The most prominent example of asynchronous processing is
//     with requests that have the "async consensus" flag set. A more subtle
//     case is with requests that are acknowledged by the Raft machinery after
//     their Raft entry has been committed but before it has been applied to
//     the replicated state machine. In all of these cases, responsibility
//     for releasing the concurrency guard is handed to Raft.
//
// However, this option is not permitted if the function returns a "server-side
// concurrency retry error" (see isConcurrencyRetryError for more details). If
// the function returns one of these errors, it must also pass ownership of the
// concurrency guard back to the caller.
//
// 中文总结：
// batchExecutionFn 是 Replica 上真正执行 read-only / read-write batch 的函数类型。
// 调用方会把已经获取好的 concurrency guard 传进去。这个 guard 代表当前请求
// 持有的 latches / lock wait-queues。执行函数可以选择：
// 1. 自己负责释放 guard，比如异步共识请求把释放责任交给 Raft。
// 2. 把 guard 还给调用方，让调用方释放或在 server-side retry 中复用。
// 但是如果执行函数返回的是并发重试错误，就必须把 guard 还回来，
// 因为重试循环需要释放/复用它来处理冲突后再次执行。
type batchExecutionFn func(
	*Replica, context.Context, *kvpb.BatchRequest, *concurrency.Guard,
) (*kvpb.BatchResponse, *concurrency.Guard, *kvadmission.StoreWriteBytes, *kvpb.Error)

var _ batchExecutionFn = (*Replica).executeWriteBatch
var _ batchExecutionFn = (*Replica).executeReadOnlyBatch

// executeBatchWithConcurrencyRetries is the entry point for client (non-admin)
// requests that execute against the range's state. The method coordinates the
// execution of requests that may require multiple retries due to interactions
// with concurrent transactions.
//
// The method acquires latches for the request, which synchronizes it with
// conflicting requests. This permits the execution function to run without
// concern of coordinating with logically conflicting operations, although it
// still needs to worry about coordinating with non-conflicting operations when
// accessing shared data structures.
//
// If the execution function hits a concurrency error like a LockConflictError or
// a TransactionPushError it will propagate the error back to this method, which
// handles the process of retrying batch execution after addressing the error.
//
// 中文总结：
// 这是普通客户端读写请求进入 Replica 并发控制层的重试外壳。
// 它先收集请求会访问的 latch spans / lock spans，再通过 concurrency manager
// 排队获取 latches、等待冲突锁。真正的读写逻辑由 fn 执行。
// 如果 fn 遇到可在服务端解决的并发错误，比如锁冲突、事务 push、lease 无效、
// merge 进行中、uncertainty interval 等，这里会处理错误并重试同一个 batch。
// 如果不是这类并发重试错误，就直接把错误返回给上层。
func (r *Replica) executeBatchWithConcurrencyRetries(
	ctx context.Context, ba *kvpb.BatchRequest, fn batchExecutionFn,
) (br *kvpb.BatchResponse, writeBytes *kvadmission.StoreWriteBytes, pErr *kvpb.Error) {
	// Try to execute command; exit retry loop on success.
	// latchSpans：请求需要互斥访问的 key span，用来和其它请求做 latch 排队。
	// lockSpans：请求需要检查/等待事务锁的 key span。
	// requestEvalKind：本轮是乐观执行、悲观执行，还是乐观失败后的悲观执行。
	var latchSpans *spanset.SpanSet
	var lockSpans *lockspanset.LockSpanSet
	var requestEvalKind concurrency.RequestEvalKind
	// g 是 concurrency manager 返回的 guard，代表本请求持有的 latches
	// 以及可能加入的 lock wait-queues。只要 g 还非 nil，函数退出前必须释放。
	var g *concurrency.Guard
	defer func() {
		// NB: wrapped to delay g evaluation to its value when returning.
		if g != nil {
			// 防止异常路径忘记释放 guard。正常路径里有些执行函数会把 g 置 nil，
			// 表示它们已经自己接管或释放了 guard。
			r.concMgr.FinishReq(ctx, g)
		}
	}()
	// 默认情况下，如果 Replica circuit breaker 被 poison/trip，遇到 poisoned latch
	// 就返回错误，避免继续向不可用 replica proposal。
	pp := poison.Policy_Error
	if r.signallerForBatch(ba).C() == nil {
		// The request wishes to ignore the circuit breaker, i.e. attempt to propose
		// commands and wait even if the circuit breaker is tripped.
		// 某些请求明确要求绕过 circuit breaker；这时遇到 poisoned latch 选择等待，
		// 而不是立刻报 ReplicaUnavailable。
		pp = poison.Policy_Wait
	}
	for {
		// Exit loop if context has been canceled or timed out.
		// 每轮重试开始前先检查 ctx，避免请求超时后仍继续做并发等待或执行。
		if err := ctx.Err(); err != nil {
			return nil, nil, kvpb.NewError(err)
		}

		// Determine the maximal set of key spans that the batch will operate on.
		// This is used below to sequence the request in the concurrency manager.
		//
		// Only do so if the latchSpans and lockSpans are not being preserved from a
		// prior iteration, either directly or in a concurrency guard that we intend
		// to re-use during sequencing.
		//
		// 如果上一轮重试选择复用 span sets 或 guard，就不要重新 collect。
		// 否则根据当前 ba 计算最大访问范围，用于后面 SequenceReq。
		if latchSpans == nil && g == nil {
			var err error
			latchSpans, lockSpans, requestEvalKind, err = r.collectSpans(ba)
			if err != nil {
				return nil, nil, kvpb.NewError(err)
			}
		}

		// Acquire latches to prevent overlapping requests from executing until
		// this request completes. After latching, wait on any conflicting locks
		// to ensure that the request has full isolation during evaluation. This
		// returns a request guard that must be eventually released.
		//
		// 进入 concurrency manager：
		// 1. 获取 latches，阻止 key span 重叠的请求同时 evaluation。
		// 2. 检查/等待事务锁，保证 evaluation 时不会读写到未解决冲突。
		// 3. 返回 guard，后面必须 FinishReq 释放。
		var resp []kvpb.ResponseUnion
		g, resp, pErr = r.concMgr.SequenceReq(ctx, g, concurrency.Request{
			Txn:             ba.Txn,
			Timestamp:       ba.Timestamp,
			NonTxnPriority:  ba.UserPriority,
			ReadConsistency: ba.ReadConsistency,
			WaitPolicy:      ba.WaitPolicy,
			LockTimeout:     ba.LockTimeout,
			DeadlockTimeout: ba.DeadlockTimeout,
			AdmissionHeader: ba.AdmissionHeader,
			PoisonPolicy:    pp,
			Requests:        ba.Requests,
			LatchSpans:      latchSpans, // nil if g != nil
			LockSpans:       lockSpans,  // nil if g != nil
			Batch:           ba,
		}, requestEvalKind)
		if pErr != nil {
			if poisonErr := (*poison.PoisonedError)(nil); errors.As(pErr.GoError(), &poisonErr) {
				// It's possible that intent resolution accessed txn info anchored on a
				// different range and hit a poisoned latch there, in which case we want
				// to propagate its ReplicaUnavailableError instead of creating one for
				// this range (which likely isn't tripped).
				if !errors.HasType(pErr.GoError(), (*kvpb.ReplicaUnavailableError)(nil)) {
					// NB: we make the breaker error (which may be nil at this point, but
					// usually is not) a secondary error, meaning it is not in the error
					// chain. That is fine; the important bits to investigate
					// programmatically are the ReplicaUnavailableError (which contains the
					// descriptor) and the *PoisonedError (which contains the concrete
					// subspan that caused this request to fail). We mark
					// circuit.ErrBreakerOpen into the chain as well so that we have the
					// invariant that all replica circuit breaker errors contain both
					// ErrBreakerOpen and ReplicaUnavailableError.
					pErr = kvpb.NewError(r.replicaUnavailableError(errors.CombineErrors(
						errors.Mark(poisonErr, circuit.ErrBreakerOpen),
						r.breaker.Signal().Err(),
					)))
				}
			}
			// SequenceReq 阶段就失败了，说明还没真正进入 read/write evaluation。
			// 直接把错误返回给上层。
			return nil, nil, pErr
		} else if resp != nil {
			// concurrency manager 有时可以直接构造响应，不需要继续调用 fn。
			// 例如某些 wait policy / lock 处理路径可能在排序阶段就得出结果。
			br = new(kvpb.BatchResponse)
			br.Responses = resp
			return br, nil, nil
		}
		// 到这里，latchSpans / lockSpans 的所有权已经转移进 guard。
		// 避免下一轮误以为自己还持有这些独立 span sets。
		latchSpans, lockSpans = nil, nil // ownership released

		// 调用真正的执行函数：
		// - executeReadOnlyBatch
		// - executeWriteBatch
		// fn 可能返回新的 guard，也可能自己释放/接管 guard 后返回 nil。
		br, g, writeBytes, pErr = fn(r, ctx, ba, g)
		if pErr == nil {
			// Success.
			// 执行成功，返回 response 和写入字节数。
			return br, writeBytes, nil
		} else if !isConcurrencyRetryError(pErr) {
			// Propagate error.
			// 不是 server-side concurrency retry error，当前循环无法修复，直接返回。
			return nil, nil, pErr
		}

		log.VErrEventf(ctx, 2, "concurrency retry error: %s", pErr)

		// The batch execution func returned a server-side concurrency retry error.
		// It may have either handed back ownership of the concurrency guard without
		// having already released the guard's latches, or in case of certain types
		// of read-only requests (see `canReadOnlyRequestDropLatchesBeforeEval`), it
		// may have released the guard's latches.
		//
		// 并发重试前需要释放 guard 持有的资源。
		// 某些错误处理希望复用原来的 latch/lock span 集合，避免重新 collect。
		dropLatchesAndLockWaitQueues := func(reuseLatchAndLockSpans bool) {
			if g != nil {
				latchSpans, lockSpans = nil, nil
				if reuseLatchAndLockSpans {
					// 从 guard 中取回 span sets，下一轮 SequenceReq 可以直接复用。
					latchSpans, lockSpans = g.TakeSpanSets()
				}
				// 释放本轮持有的 latches 和 lock wait-queues。
				r.concMgr.FinishReq(ctx, g)
				g = nil
			}
		}

		// 测试钩子：观察或干预并发重试。
		if filter := r.store.cfg.TestingKnobs.TestingConcurrencyRetryFilter; filter != nil {
			filter(ctx, ba, pErr)
		}

		// Typically, retries are marked PessimisticEval. The one exception is a
		// pessimistic retry immediately after an optimistic eval which failed
		// when checking for conflicts, which is handled below. Note that an
		// optimistic eval failure for any other reason will also retry as
		// PessimisticEval.
		//
		// 默认下一轮使用悲观执行：先完整获取 latches / 检查 locks，再 evaluation。
		requestEvalKind = concurrency.PessimisticEval

		switch t := pErr.GetDetail().(type) {
		case *kvpb.LockConflictError:
			// Drop latches, but retain lock wait-queues.
			// 遇到锁冲突：释放 latches，但保留/使用 lock wait queue 等锁持有者推进。
			g.AssertLatches()
			if g, pErr = r.handleLockConflictError(ctx, ba, g, pErr, t); pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.TransactionPushError:
			// Drop latches, but retain lock wait-queues.
			// 遇到事务 push：尝试 push 对方事务，处理完后重试。
			g.AssertLatches()
			if g, pErr = r.handleTransactionPushError(ctx, ba, g, pErr, t); pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.IndeterminateCommitError:
			// 不确定提交状态：释放当前并发资源，但复用 span sets；
			// 然后尝试恢复事务状态到 COMMITTED 或 ABORTED。
			dropLatchesAndLockWaitQueues(true /* reuseLatchAndLockSpans */)
			// Then launch a task to handle the indeterminate commit error. No error
			// is returned if the transaction is recovered successfully to either a
			// COMMITTED or ABORTED state.
			if pErr = r.handleIndeterminateCommitError(ctx, ba, pErr, t); pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.ReadWithinUncertaintyIntervalError:
			// If the batch is able to perform a server-side retry in order to avoid
			// the uncertainty error, it will have a new timestamp. Force a refresh of
			// the latch and lock spans.
			// 读进了 uncertainty interval：如果可以服务端前推时间戳重试，
			// 新 timestamp 可能改变访问/锁语义，所以不复用原 span sets。
			dropLatchesAndLockWaitQueues(false /* reuseLatchAndLockSpans */)
			// Attempt to adjust the batch's timestamp to avoid the uncertainty error
			// and allow for a server-side retry. For transactional requests, there
			// are strict conditions that must be met for this to be permitted. For
			// non-transactional requests, this is always allowed. If successful, an
			// updated BatchRequest will be returned. If unsuccessful, the provided
			// read within uncertainty interval error will be returned so that we can
			// propagate it.
			ba, pErr = r.handleReadWithinUncertaintyIntervalError(ctx, ba, pErr, t)
			if pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.InvalidLeaseError:
			// lease 无效：释放当前并发资源，尝试获取 lease 或让上层转发到当前 leaseholder。
			dropLatchesAndLockWaitQueues(true /* reuseLatchAndLockSpans */)
			// Then attempt to acquire the lease if not currently held by any
			// replica or redirect to the current leaseholder if currently held
			// by a different replica.
			if pErr = r.handleInvalidLeaseError(ctx, ba); pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.MergeInProgressError:
			// range 正在 merge：释放当前并发资源，等待 merge 完成后再重试。
			dropLatchesAndLockWaitQueues(true /* reuseLatchAndLockSpans */)
			// Then listen for the merge to complete.
			if pErr = r.handleMergeInProgressError(ctx, ba, pErr, t); pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.OptimisticEvalConflictsError:
			// We are deliberately not dropping latches. Note that the latches are
			// also optimistically acquired, in the sense of being inserted but not
			// waited on. The next iteration will wait on these latches to ensure
			// acquisition, and then pessimistically check for locks while holding
			// these latches. If conflicting locks are found, the request will queue
			// for those locks and release latches.
			// 乐观执行发现冲突：不要释放已经乐观插入的 latches。
			// 下一轮会先等待这些 latches 真正获取成功，然后改用悲观方式检查 locks。
			requestEvalKind = concurrency.PessimisticAfterFailedOptimisticEval
		default:
			// isConcurrencyRetryError 认为它可重试，但这里没有处理分支，说明代码不一致。
			log.KvExec.Fatalf(ctx, "unexpected concurrency retry error %T", t)
		}
		// Retry...
		// 处理完冲突后回到 for 循环顶部，重新检查 ctx、重新排序/执行 batch。
	}
}

// isConcurrencyRetryError returns whether or not the provided error is a
// "concurrency retry error" that will be captured and retried by
// executeBatchWithConcurrencyRetries. Most concurrency retry errors are
// handled by dropping a request's latches, waiting for and/or ensuring that
// the condition which caused the error is handled, re-sequencing through the
// concurrency manager, and executing the request again. The one exception is
// OptimisticEvalConflictsError, where there is no need to drop latches, and
// the request can immediately proceed to retrying pessimistically.
func isConcurrencyRetryError(pErr *kvpb.Error) bool {
	switch pErr.GetDetail().(type) {
	case *kvpb.LockConflictError:
		// If a request hits a LockConflictError, it adds the conflicting intent
		// to the lockTable through a process called "lock discovery". It then
		// waits in the lock's wait-queue during its next sequencing pass.
	case *kvpb.TransactionPushError:
		// If a PushTxn request hits a TransactionPushError, it attempted to
		// push another transactions record but did not succeed. It enqueues the
		// pushee transaction in the txnWaitQueue and waits on the record to
		// change or expire during its next sequencing pass.
	case *kvpb.IndeterminateCommitError:
		// If a PushTxn hits a IndeterminateCommitError, it attempted to push an
		// expired transaction record in the STAGING state. It's unclear whether
		// the pushee is aborted or committed, so the request must kick off the
		// "transaction recovery procedure" to resolve this ambiguity before
		// retrying.
	case *kvpb.ReadWithinUncertaintyIntervalError:
		// If a request hits a ReadWithinUncertaintyIntervalError, it was performing
		// a non-locking read [1] and encountered a (committed or provisional) write
		// within the uncertainty interval of the reader. Depending on the state of
		// the request (see conditions in canDoServersideRetry), it may be able to
		// adjust its timestamp and retry on the server.
		//
		// This is similar to other server-side retries that we allow below
		// latching, like for WriteTooOld errors. However, because uncertainty
		// errors are specific to non-locking reads, they can not [2] be retried
		// without first dropping and re-acquiring their read latches at a higher
		// timestamp. This is unfortunate for uncertainty errors, as it leads to
		// some extra work.
		//
		// On the other hand, it is more important for other forms of retry errors
		// to be handled without dropping latches because they could be starved by
		// repeated conflicts. For instance, if WriteTooOld errors caused a write
		// request to drop and re-acquire latches, it is possible that the request
		// could return after each retry to find a new WriteTooOld conflict, never
		// managing to complete. This is not the case for uncertainty errors, which
		// can not occur indefinitely. A request (transactional or otherwise) has a
		// fixed uncertainty window and, once exhausted, will never hit an
		// uncertainty error again.
		//
		// [1] if a locking read observes a write at a later timestamp, it returns a
		// WriteTooOld error. It's uncertainty interval does not matter.
		// [2] in practice, this is enforced by tryBumpBatchTimestamp's call to
		// (*concurrency.Guard).IsolatedAtLaterTimestamps.
	case *kvpb.InvalidLeaseError:
		// If a request hits an InvalidLeaseError, the replica it is being
		// evaluated against does not have a valid lease under which it can
		// serve the request. The request cannot proceed until a new lease is
		// acquired. If the acquisition process determines that the lease now
		// lives elsewhere, the request should be redirected (using a
		// NotLeaseHolderError) to the new leaseholder.
	case *kvpb.MergeInProgressError:
		// If a request hits a MergeInProgressError, the replica it is being
		// evaluated against is in the process of being merged into its left-hand
		// neighbor. The request cannot proceed until the range merge completes,
		// either successfully or unsuccessfully, so it waits before retrying.
		// If the merge does complete successfully, the retry will be rejected
		// with an error that will propagate back to the client.
	case *kvpb.OptimisticEvalConflictsError:
		// Optimistic evaluation encountered a conflict. The request will
		// immediately retry pessimistically.
	default:
		return false
	}
	return true
}

// maybeAttachLease is used to augment a concurrency retry error with
// information about the lease that the operation which hit this error was
// operating under. If the operation was performed on a follower that does not
// hold the lease (e.g. a follower read), the provided lease will be empty.
func maybeAttachLease(pErr *kvpb.Error, lease *roachpb.Lease) *kvpb.Error {
	if lcErr, ok := pErr.GetDetail().(*kvpb.LockConflictError); ok {
		// If we hit an intent on the leaseholder, attach information about the
		// lease to LockConflictErrors, which is necessary to keep the lock-table
		// in sync with the applied state.
		//
		// However, if we hit an intent during a follower read, the lock-table will
		// be disabled, so we won't be able to use it to wait for the resolution of
		// the intent. Instead of waiting locally, we replace the LockConflictError
		// with an InvalidLeaseError so that the request will be redirected to the
		// leaseholder. Beyond implementation constraints, waiting for conflicting
		// intents on the leaseholder instead of on a follower is preferable
		// because:
		// - the leaseholder is notified of and reactive to lock-table state
		//   transitions.
		// - the leaseholder is able to more efficiently resolve intents, if
		//   necessary, without the risk of multiple follower<->leaseholder
		//   round-trips compounding. If the follower was to attempt to resolve
		//   multiple intents during a follower read then the PushTxn and
		//   ResolveIntent requests would quickly be more expensive (in terms of
		//   latency) than simply redirecting the entire read request to the
		//   leaseholder and letting the leaseholder coordinate the intent
		//   resolution.
		// - after the leaseholder has received a response from a ResolveIntent
		//   request, it has a guarantee that the intent resolution has been applied
		//   locally and that no future read will observe the intent. This is not
		//   true on follower replicas. Due to the asynchronous nature of Raft, both
		//   due to quorum voting and due to async commit acknowledgement from
		//   leaders to followers, it is possible for a ResolveIntent request to
		//   complete and then for a future read on a follower to observe the
		//   pre-resolution state of the intent. This effect is transient and will
		//   eventually disappear once the follower catches up on its Raft log, but
		//   it creates an opportunity for momentary thrashing if a follower read
		//   was to resolve an intent and then immediately attempt to read again.
		//
		// This behavior of redirecting follower read attempts to the leaseholder
		// replica if they encounter conflicting intents on a follower means that
		// follower read eligibility is a function of the "resolved timestamp" over
		// a read's key span, and not just the "closed timestamp" over its key span.
		// Architecturally, this is consistent with Google Spanner, who maintains a
		// concept of "safe time", "paxos safe time", "transaction manager safe
		// time". "safe time" is analogous to the "resolved timestamp" in
		// CockroachDB and "paxos safe time" is analogous to the "closed timestamp"
		// in CockroachDB. In Spanner, it is the "safe time" of a replica that
		// determines follower read eligibility.
		if lease.Empty() /* followerRead */ {
			return kvpb.NewErrorWithTxn(&kvpb.InvalidLeaseError{}, pErr.GetTxn())
		}
		lcErr.LeaseSequence = lease.Sequence
		return kvpb.NewErrorWithTxn(lcErr, pErr.GetTxn())
	}
	return pErr
}

func (r *Replica) handleLockConflictError(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	g *concurrency.Guard,
	pErr *kvpb.Error,
	t *kvpb.LockConflictError,
) (*concurrency.Guard, *kvpb.Error) {
	if r.store.cfg.TestingKnobs.DontPushOnLockConflictError {
		return g, pErr
	}
	// g's latches will be dropped, but it retains its spot in lock wait-queues.
	return r.concMgr.HandleLockConflictError(ctx, g, t.LeaseSequence, t)
}

func (r *Replica) handleTransactionPushError(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	g *concurrency.Guard,
	pErr *kvpb.Error,
	t *kvpb.TransactionPushError,
) (*concurrency.Guard, *kvpb.Error) {
	// On a transaction push error, retry immediately if doing so will enqueue
	// into the txnWaitQueue in order to await further updates to the unpushed
	// txn's status. We check ShouldPushImmediately to avoid retrying
	// non-queueable PushTxnRequests (see #18191).
	dontRetry := r.store.cfg.TestingKnobs.DontRetryPushTxnFailures
	if !dontRetry && ba.IsSinglePushTxnRequest() {
		pushReq := ba.Requests[0].GetInner().(*kvpb.PushTxnRequest)
		dontRetry = txnwait.ShouldPushImmediately(pushReq, t.PusheeTxn.Status, ba.WaitPolicy)
	}
	if dontRetry {
		return g, pErr
	}
	// g's latches will be dropped, but it retains its spot in lock wait-queues
	// (though a PushTxn shouldn't be in any lock wait-queues).
	return r.concMgr.HandleTransactionPushError(ctx, g, t), nil
}

func (r *Replica) handleIndeterminateCommitError(
	ctx context.Context, ba *kvpb.BatchRequest, pErr *kvpb.Error, t *kvpb.IndeterminateCommitError,
) *kvpb.Error {
	if r.store.cfg.TestingKnobs.DontRecoverIndeterminateCommits {
		return pErr
	}
	// On an indeterminate commit error, attempt to recover and finalize the
	// stuck transaction. Retry immediately if successful.
	if _, err := r.store.recoveryMgr.ResolveIndeterminateCommit(ctx, t); err != nil {
		// Do not propagate ambiguous results; assume success and retry original op.
		if errors.HasType(err, (*kvpb.AmbiguousResultError)(nil)) {
			return nil
		}
		// Propagate new error. Preserve the error index.
		newPErr := kvpb.NewError(err)
		newPErr.Index = pErr.Index
		return newPErr
	}
	// We've recovered the transaction that blocked the request; retry.
	return nil
}

func (r *Replica) handleReadWithinUncertaintyIntervalError(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	pErr *kvpb.Error,
	t *kvpb.ReadWithinUncertaintyIntervalError,
) (*kvpb.BatchRequest, *kvpb.Error) {
	// Attempt a server-side retry of the request. Note that we pass nil for
	// latchSpans, because we have already released our latches and plan to
	// re-acquire them if the retry is allowed.
	var ok bool
	ba, ok = canDoServersideRetry(ctx, pErr, ba, nil /* g */, hlc.Timestamp{} /* deadline */)
	if !ok {
		r.store.Metrics().ReadWithinUncertaintyIntervalErrorServerSideRetryFailure.Inc(1)
		return nil, pErr
	}
	r.store.Metrics().ReadWithinUncertaintyIntervalErrorServerSideRetrySuccess.Inc(1)

	if ba.Txn == nil {
		// If the request is non-transactional and it was refreshed into the future
		// after observing a value with a timestamp in the future, immediately sleep
		// until its new read timestamp becomes present. We don't need to do this
		// for transactional requests because they will do this during their
		// commit-wait sleep after committing.
		//
		// See TxnCoordSender.maybeCommitWait for a discussion about why doing this
		// is necessary to preserve real-time ordering for transactions that write
		// into the future.
		var cancel func()
		ctx, cancel = r.store.Stopper().WithCancelOnQuiesce(ctx)
		defer cancel()
		if err := r.Clock().SleepUntil(ctx, ba.Timestamp); err != nil {
			return nil, kvpb.NewError(err)
		}
	}
	return ba, nil
}

func (r *Replica) handleInvalidLeaseError(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
	// On an invalid lease error, attempt to acquire a new lease. If in the
	// process of doing so, we determine that the lease now lives elsewhere,
	// redirect.
	_, pErr := r.redirectOnOrAcquireLeaseForRequest(ctx, ba.Timestamp, r.signallerForBatch(ba))
	// If we managed to get a lease (i.e. pErr == nil), the request evaluation
	// will be retried.
	return pErr
}

func (r *Replica) handleMergeInProgressError(
	ctx context.Context, ba *kvpb.BatchRequest, pErr *kvpb.Error, t *kvpb.MergeInProgressError,
) *kvpb.Error {
	// A merge was in progress. We need to retry the command after the merge
	// completes, as signaled by the closing of the replica's mergeComplete
	// channel. Note that the merge may have already completed, in which case
	// its mergeComplete channel will be nil.
	mergeCompleteCh := r.getMergeCompleteCh()
	if mergeCompleteCh == nil {
		// Merge no longer in progress. Retry the command.
		return nil
	}
	// Check to see if the request is a lease transfer. If so, reject it
	// immediately instead of a waiting for the merge to complete. This is
	// necessary because the merge may need to acquire a range lease in order to
	// complete if it still needs to perform its Subsume request, which it
	// likely will if this lease transfer revoked the leaseholder's existing
	// range lease. Any concurrent lease acquisition attempt will be blocked on
	// this lease transfer because a replica only performs a single lease
	// operation at a time, so we reject to prevent a deadlock.
	//
	// NOTE: it would not be sufficient to check for an in-progress merge in
	// AdminTransferLease because the range may notice the in-progress merge
	// after the lease transfer is initiated but before the lease transfer
	// acquires latches.
	if ba.IsSingleTransferLeaseRequest() {
		return kvpb.NewErrorf("cannot transfer lease while merge in progress")
	}
	log.Event(ctx, "waiting on in-progress range merge")
	select {
	case <-mergeCompleteCh:
		// Merge complete. Retry the command.
		return nil
	case <-ctx.Done():
		return kvpb.NewError(errors.Wrap(ctx.Err(), "aborted during merge"))
	case <-r.store.stopper.ShouldQuiesce():
		return kvpb.NewError(&kvpb.NodeUnavailableError{})
	}
}

// executeAdminBatch executes the command directly. There is no interaction
// with the spanlatch manager or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
// Admin commands must run on the lease holder replica. Batch support here is
// limited to single-element batches; everything else catches an error.
func (r *Replica) executeAdminBatch(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	if len(ba.Requests) != 1 {
		return nil, kvpb.NewErrorf("only single-element admin batches allowed")
	}

	args := ba.Requests[0].GetInner()

	sArg := reflect.TypeOf(args).String()
	ctx = logtags.AddTag(ctx, sArg, "")

	ctx, sp := tracing.EnsureChildSpan(ctx, r.AmbientContext.Tracer, sArg)
	defer sp.Finish()

	// Verify that the batch can be executed, which includes verifying that the
	// current replica has the range lease.
	// NB: we pass nil for the spanlatch guard because we haven't acquired
	// latches yet. This is ok because each individual request that the admin
	// request sends will acquire latches.
	for {
		if err := ctx.Err(); err != nil {
			return nil, kvpb.NewError(err)
		}

		_, err := r.checkExecutionCanProceedRWOrAdmin(ctx, ba, nil /* g */)
		if err == nil {
			err = r.signallerForBatch(ba).Err()
		}
		if err == nil {
			break
		}
		switch {
		case errors.HasType(err, (*kvpb.InvalidLeaseError)(nil)):
			// If the replica does not have the lease, attempt to acquire it, or
			// redirect to the current leaseholder by returning an error.
			_, pErr := r.redirectOnOrAcquireLeaseForRequest(ctx, ba.Timestamp, r.signallerForBatch(ba))
			if pErr != nil {
				return nil, pErr
			}
			// Retry...
		default:
			return nil, kvpb.NewError(err)
		}
	}

	var resp kvpb.Response
	var pErr *kvpb.Error
	switch tArgs := args.(type) {
	case *kvpb.AdminSplitRequest:
		var reply kvpb.AdminSplitResponse
		reply, pErr = r.AdminSplit(ctx, *tArgs, manualAdminReason)
		resp = &reply

	case *kvpb.AdminUnsplitRequest:
		var reply kvpb.AdminUnsplitResponse
		reply, pErr = r.AdminUnsplit(ctx, *tArgs, manualAdminReason)
		resp = &reply

	case *kvpb.AdminMergeRequest:
		var reply kvpb.AdminMergeResponse
		reply, pErr = r.AdminMerge(ctx, *tArgs, manualAdminReason)
		resp = &reply

	case *kvpb.AdminTransferLeaseRequest:
		pErr = kvpb.NewError(r.AdminTransferLease(ctx, tArgs.Target, tArgs.BypassSafetyChecks))
		resp = &kvpb.AdminTransferLeaseResponse{}

	case *kvpb.AdminChangeReplicasRequest:
		chgs := tArgs.Changes()
		desc, err := r.ChangeReplicas(ctx, &tArgs.ExpDesc, kvserverpb.ReasonAdminRequest, "", chgs)
		pErr = kvpb.NewError(err)
		if pErr != nil {
			resp = &kvpb.AdminChangeReplicasResponse{}
		} else {
			resp = &kvpb.AdminChangeReplicasResponse{
				Desc: *desc,
			}
		}

	case *kvpb.AdminRelocateRangeRequest:
		// Transferring the lease to the first voting replica in the target slice is
		// pre-22.1 behavior.
		// We revert to that behavior if the request is coming
		// from a 21.2 node that doesn't yet know about this change in contract.
		transferLeaseToFirstVoter := !tArgs.TransferLeaseToFirstVoterAccurate
		// We also revert to that behavior if the caller specifically asked for it.
		transferLeaseToFirstVoter = transferLeaseToFirstVoter || tArgs.TransferLeaseToFirstVoter
		err := r.AdminRelocateRange(
			ctx, *r.Desc(), tArgs.VoterTargets, tArgs.NonVoterTargets, transferLeaseToFirstVoter,
		)
		pErr = kvpb.NewError(err)
		resp = &kvpb.AdminRelocateRangeResponse{}

	case *kvpb.CheckConsistencyRequest:
		var reply kvpb.CheckConsistencyResponse
		reply, pErr = r.CheckConsistency(ctx, *tArgs)
		resp = &reply

	case *kvpb.AdminScatterRequest:
		reply, err := r.adminScatter(ctx, *tArgs)
		pErr = kvpb.NewError(err)
		resp = &reply

	default:
		return nil, kvpb.NewErrorf("unrecognized admin command: %T", args)
	}

	if pErr != nil {
		return nil, pErr
	}

	br := &kvpb.BatchResponse{}
	br.Add(resp)
	br.Txn = resp.Header().Txn
	return br, nil
}

func (r *Replica) recordBatchRequestLoad(ctx context.Context, ba *kvpb.BatchRequest) {
	if r.loadStats == nil {
		log.VEventf(
			ctx,
			3,
			"Unable to record load of batch request for r%d, load stats is not initialized",
			ba.Header.RangeID,
		)
		return
	}

	// adjustedQPS is the adjusted number of queries per second, that is a cost
	// estimate of a BatchRequest. See getBatchRequestQPS() for the
	// calculation.
	adjustedQPS := r.getBatchRequestQPS(ctx, ba)

	r.loadStats.RecordBatchRequests(adjustedQPS, ba.Header.GatewayNodeID)
	r.loadStats.RecordRequests(float64(len(ba.Requests)))
}

// getBatchRequestQPS calculates the cost estimation of a BatchRequest. The
// estimate returns Queries Per Second (QPS), representing the abstract
// resource cost associated with this request. BatchRequests are calculated as
// 1 QPS, unless an AddSSTableRequest exists, in which case the sum of all
// AddSSTableRequest's data size is divided by a factor and added to QPS. This
// specific treatment of QPS is a special case to account for the mismatch
// between AddSSTableRequest and other requests in terms of resource use.
func (r *Replica) getBatchRequestQPS(ctx context.Context, ba *kvpb.BatchRequest) float64 {
	var count float64 = 1

	// For divisors less than 1, use the default treatment of QPS.
	requestFact := replicastats.AddSSTableRequestSizeFactor.Get(&r.store.cfg.Settings.SV)
	if requestFact < 1 {
		return count
	}

	var addSSTSize float64 = 0
	for _, req := range ba.Requests {
		switch t := req.GetInner().(type) {
		case *kvpb.AddSSTableRequest:
			addSSTSize += float64(len(t.Data))
		default:
			continue
		}
	}

	count += addSSTSize / float64(requestFact)
	return count
}

// recordRequestWriteBytes records the write bytes from a replica batch
// request.
func (r *Replica) recordRequestWriteBytes(writeBytes int64) {
	// TODO(kvoli): Consider recording the ingested bytes (AddSST) separately
	// to the write bytes.
	r.loadStats.RecordWriteBytes(float64(writeBytes))
}

// checkBatchRequest verifies BatchRequest validity requirements. In particular,
// the batch must have an assigned timestamp, and either all requests must be
// read-only, or none.
//
// TODO(tschottdorf): should check that request is contained in range and that
// EndTxn only occurs at the very end.
func (r *Replica) checkBatchRequest(ba *kvpb.BatchRequest, isReadOnly bool) error {
	if ba.Timestamp.IsEmpty() {
		// For transactional requests, Store.Send sets the timestamp. For non-
		// transactional requests, the client sets the timestamp. Either way, we
		// need to have a timestamp at this point.
		return errors.New("Replica.checkBatchRequest: batch does not have timestamp assigned")
	}
	consistent := ba.ReadConsistency == kvpb.CONSISTENT
	if isReadOnly {
		if !consistent && ba.Txn != nil {
			// Disallow any inconsistent reads within txns.
			return errors.Errorf("cannot allow %v reads within a transaction", ba.ReadConsistency)
		}
	} else if !consistent {
		return errors.Errorf("%v mode is only available to reads", ba.ReadConsistency)
	}

	return nil
}

// collectSpans 为当前 BatchRequest 计算出两组 span 集合以及一个执行策略。
//
// 返回值：
//   - latchSpans: 需要向 latch manager 声明的、本次请求会读/写的键范围。
//     这些 span 用于和其他并发请求做互斥/排队。分为 read-only 和 read-write 两种访问类型，
//     以及 global（MVCC 数据）和 local（非 MVCC，如事务记录、range descriptor）两种作用域。
//   - lockSpans: 需要由 lock table（事务锁管理器）检查的键范围。
//     用于检测/等待其它事务持有的锁（intents / shared locks 等），保证 evaluation 时已完全隔离。
//   - requestEvalKind: 执行策略——PessimisticEval（先拿全 latches 再 evaluation）
//     还是 OptimisticEval（乐观执行：只插入 latches 但不等待，evaluation 结束后再校验冲突）。
//
// 函数内部流程：
//  1. 根据 batch 类型预估容量，减少后续 append 的堆分配
//  2. 调用 DeclareKeysForBatch 声明 batch header 级别的 span（如 AbortSpan）
//  3. 遍历 batch 中的每个 request，调用对应 command 的 DeclareKeys 方法，
//     由每个 command 自己决定要 latch / lock 哪些 key range
//  4. 排序去重（SortAndDedup），因为多个 request 可能声明重复范围
//  5. 校验 span 合法性
//  6. 根据 batch 特征（SkipLocked、limit scan 等）决定是否用乐观执行
func (r *Replica) collectSpans(
	ba *kvpb.BatchRequest,
) (
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	requestEvalKind concurrency.RequestEvalKind,
	_ error,
) {
	// 创建空的 SpanSet 和 LockSpanSet，后续由各个 command 填充。
	latchSpans, lockSpans = spanset.New(), lockspanset.New()

	// 快照读 range 描述符和 live key count。range 描述符用于 command 的 key
	// 声明（比如知道 range 的 start/end key），liveCount 用于后面判断是否走
	// 乐观执行（limit scan 场景下的上界估算）。
	r.mu.RLock()
	desc := r.descRLocked()
	liveCount := r.shMu.state.Stats.LiveCount
	r.mu.RUnlock()

	// TODO(bdarnell): need to make this less global when local
	// latches are used more heavily. For example, a split will
	// have a large read-only span but also a write (see #10084).
	// Currently local spans are the exception, so preallocate for the
	// common case in which all are global. We rarely mix read and
	// write commands, so preallocate for writes if there are any
	// writes present in the batch.
	//
	// 预分配 SpanSet 内部 slice 容量，减少动态 append 时可能的堆分配。
	// 根据 batch 是否包含写入，决定用 read-write 还是 read-only 类型预分配。
	if ba.IsLocking() {
		// batch 中有 locking 请求（写请求、locking read 等）。
		// 先按 request 数量预分配 latch spans。
		latchGuess := len(ba.Requests)
		// EndTxn 特殊处理：除了自身的 request，还需要为 LockSpans 中的每个
		// intent span 额外声明一个 write latch。
		if et, ok := ba.GetArg(kvpb.EndTxn); ok {
			// EndTxn 会为它的每个 LockSpan 声明一个 global write latch
			// （见 declareKeysEndTxn 中 et.LockSpans 的处理）。
			latchGuess += len(et.(*kvpb.EndTxnRequest).LockSpans) - 1
		}
		latchSpans.Reserve(spanset.SpanReadWrite, spanset.SpanGlobal, latchGuess)
		// 锁 span 按 intent 强度预分配。
		lockSpans.Reserve(lock.Intent, len(ba.Requests))
	} else {
		// 纯读 batch：全部预分配为 read-only 类型。
		latchSpans.Reserve(spanset.SpanReadOnly, spanset.SpanGlobal, len(ba.Requests))
		lockSpans.Reserve(lock.None, len(ba.Requests))
	}

	// Note that we are letting locking readers be considered for optimistic
	// evaluation. This is correct, though not necessarily beneficial.
	// 判断是否可能走乐观执行路径：
	// 前提：纯读 + 所有请求都是事务性的 + 集群设置允许 optimistic eval。
	considerOptEval := ba.IsReadOnly() && ba.IsAllTransactional() &&
		optimisticEvalLimitedScans.Get(&r.ClusterSettings().SV)

	// If the request is using a SkipLocked wait policy, we always perform
	// optimistic evaluation. In Replica.collectSpansRead, SkipLocked reads are
	// able to constraint their read spans down to point reads on just those keys
	// that were returned and were not already locked. This means that there is a
	// good chance that some or all of the write latches that the SkipLocked read
	// would have blocked on won't overlap with the keys that the request ends up
	// returning, so they won't conflict when checking for optimistic conflicts.
	//
	// 如果 wait policy 是 SkipLocked，总是用乐观执行。
	// 原因：SkipLocked 读取不会返回已被锁住的 key，所以真实访问的 key range
	// 往往比声明的最大范围小得多。乐观执行可以避免等待很多写锁，
	// 评估结束后再用真实读到的 key 去做冲突校验即可。
	optEvalForSkipLocked := considerOptEval && ba.Header.WaitPolicy == lock.WaitPolicy_SkipLocked

	// If the request is using a key limit, we may want it to perform optimistic
	// evaluation, depending on how large the limit is.
	// 如果请求带有 key limit（MaxSpanRequestKeys），也会考虑乐观执行。
	// 具体是否走乐观执行取决于 limit 和实际 live key count 的比较（见后面启发式）。
	considerOptEvalForLimit := considerOptEval && ba.Header.MaxSpanRequestKeys > 0
	// 用于后续 limit 启发式的临时变量。
	hasScans := false
	numGets := 0

	// For non-local, MVCC spans we annotate them with the request timestamp
	// during declaration. This is the timestamp used during latch acquisitions.
	// For read requests this works as expected, reads are performed at the same
	// timestamp. During writes however, we may encounter a versioned value newer
	// than the request timestamp, and may have to retry at a higher timestamp.
	// This is still safe as we're only ever writing at timestamps higher than the
	// timestamp any write latch would be declared at.
	//
	// Step 1: 声明 batch header 级别的 span。
	// 例如如果事务已经获取过锁，需要在 AbortSpan key 上加 read latch，
	// 以防事务在 evaluation 期间被其它 push 机制中止。
	err := batcheval.DeclareKeysForBatch(desc, &ba.Header, latchSpans)
	if err != nil {
		return nil, nil, concurrency.PessimisticEval, err
	}

	// Step 2: 遍历 batch 中的每个 request，调用其 command 的 DeclareKeys 方法。
	// 每个 command 自己决定要对哪些 key 获取何种类型的 latch/lock：
	//   - Put/Delete 等写命令 → DefaultDeclareKeys/DefaultDeclareIsolatedKeys
	//     声明 read-write latch + intent lock
	//   - Get/Scan 等非锁定读 → DefaultDeclareIsolatedKeys
	//     声明 read-only latch（范围扩展到 GlobalUncertaintyLimit 以确保正确性）
	//     + None lock（不参与 lock table 等待，但可能触发 uncertainty 检查）
	//   - Scan with Exclusive/Shared lock → 声明对应强度的 lock + read/write latch
	//   - EndTxn → 声明 txn record latch + LockSpans 中每个 intent 的 latch 等
	for _, union := range ba.Requests {
		inner := union.GetInner()
		if cmd, ok := batcheval.LookupCommand(inner.Method()); ok {
			err := cmd.DeclareKeys(desc, &ba.Header, inner, latchSpans, lockSpans, r.Clock().MaxOffset())
			if err != nil {
				return nil, nil, concurrency.PessimisticEval, err
			}
			// 同步统计 batch 中 scan/get 的数量，用于 limit 启发式。
			if considerOptEvalForLimit {
				switch inner.(type) {
				case *kvpb.ScanRequest, *kvpb.ReverseScanRequest:
					hasScans = true
				case *kvpb.GetRequest:
					numGets++
				}
			}
		} else {
			return nil, nil, concurrency.PessimisticEval, errors.Errorf("unrecognized command %s", inner.Method())
		}
	}

	// Step 3: 排序并合并重复 span。
	// 多个 command 可能会声明重叠甚至相同的 key range（比如同一个 batch 里
	// 有多个 Get 请求），去重可以显著减少后续 {latchManager,lockTable} 的处理开销。
	latchSpans.SortAndDedup()
	lockSpans.SortAndDeDup()

	// Step 4: 校验 span 合法性。
	// 例如检查是否有非法 key、是否有超出 range 边界等。
	// 不合法时直接返回错误，避免后续 {latchManager,lockTable} panic。
	if err := latchSpans.Validate(); err != nil {
		return nil, nil, concurrency.PessimisticEval, err
	}
	if err := lockSpans.Validate(); err != nil {
		return nil, nil, concurrency.PessimisticEval, err
	}

	// Step 5: 根据 limit 启发式决定是否走乐观执行。
	//
	// 启发式逻辑：
	//   如果 key limit（MaxSpanRequestKeys）小于 range 中理论能返回的 key 数上界，
	//   那么"声明的最大范围"远大于"实际会读的范围"，乐观执行可以避免等待很多不必要的
	//   latch/lock。
	//
	//   上界计算：
	//     - 有 scan 请求时：上界 = liveCount（整个 range 的 live key 数）
	//     - 只有 get 请求时：上界 = min(liveCount, gets 数量)
	//     - 如果 limit < 上界 → 乐观执行
	//
	//   例子：一个 Scan 声明了整个 range 但带了 LIMIT 1，那么它实际只会碰到 1 个 key
	//   上的冲突。乐观执行可以不做全 range 的 latch 等待，读完之后只检查真正读到的
	//   key range 里有没有冲突即可。
	optEvalForLimit := false
	if considerOptEvalForLimit {
		const k = 1
		upperBoundKeys := k * liveCount
		if !hasScans && int64(numGets) < upperBoundKeys {
			upperBoundKeys = int64(numGets)
		}
		if ba.Header.MaxSpanRequestKeys < upperBoundKeys {
			optEvalForLimit = true
		}
	}

	// Step 6: 综合决定执行策略。
	requestEvalKind = concurrency.PessimisticEval // 默认悲观执行
	if optEvalForSkipLocked || optEvalForLimit {
		requestEvalKind = concurrency.OptimisticEval // SkipLocked 或 limit 启发式命中 → 乐观执行
	}

	return latchSpans, lockSpans, requestEvalKind, nil
}

// endCmds holds necessary information to end a batch after command processing,
// either after a write request has achieved consensus and been applied to Raft
// or after a read-only request has finished evaluation.
type endCmds struct {
	repl             *Replica
	g                *concurrency.Guard
	st               kvserverpb.LeaseStatus // empty for follower reads
	replicatingSince time.Time
}

// makeUnreplicatedEndCmds sets up an endCmds to track an unreplicated,
// that is, read-only, command.
func makeUnreplicatedEndCmds(
	repl *Replica, g *concurrency.Guard, st kvserverpb.LeaseStatus,
) endCmds {
	return makeReplicatedEndCmds(repl, g, st, time.Time{})
}

// makeReplicatedEndCmds initializes an endCmds representing a command that
// needs to undergo replication. This is not used for read-only commands
// (including read-write commands that end up not queueing any mutations to the
// state machine).
func makeReplicatedEndCmds(
	repl *Replica, g *concurrency.Guard, st kvserverpb.LeaseStatus, replicatingSince time.Time,
) endCmds {
	return endCmds{repl: repl, g: g, st: st, replicatingSince: replicatingSince}
}

func makeEmptyEndCmds() endCmds {
	return endCmds{}
}

// move moves the endCmds into the return value, clearing and making a call to
// done on the receiver a no-op.
func (ec *endCmds) move() endCmds {
	res := *ec
	*ec = makeEmptyEndCmds()
	return res
}

// poison marks the Guard held by the endCmds as poisoned, which
// induces fail-fast behavior for requests waiting for our latches.
// This method must only be called for commands in the Replica.mu.proposals
// map and the Replica mutex must be held throughout.
func (ec *endCmds) poison() {
	if ec.repl == nil {
		// Already cleared. This path may no longer be hit thanks to a re-work
		// of reproposals[1]. The caller to poison holds the replica mutex and
		// the command is in r.mu.proposals, meaning the command hasn't been
		// signaled by log application yet, i.e. latches must not have been
		// released (even if refreshProposalsLocked has already signaled the
		// client with an ambiguous result).
		//
		// [1]: https://github.com/cockroachdb/cockroach/pull/106750
		//
		// TODO(repl): verify that and put an assertion here. This is similar to
		// the TODO on ProposalData.endCmds.
		return
	}
	ec.repl.concMgr.PoisonReq(ec.g)
}

// done releases the latches acquired by the command and updates the timestamp
// cache using the final timestamp of each command. If `br` is nil, it is
// assumed that `done` is being called by a request that's dropping its latches
// before evaluation.
//
// No-op if the receiver has been zeroed out by a call to move. Idempotent and
// is safe to call more than once.
func (ec *endCmds) done(
	ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse, pErr *kvpb.Error,
) {
	if ec.repl == nil {
		// The endCmds were cleared. This may no longer be necessary, see the comment on
		// ProposalData.endCmds.
		return
	}
	defer ec.move() // clear

	// Update the timestamp cache. Each request within the batch is considered in
	// turn; only those marked as affecting the cache are processed. However, only
	// do so if the request is consistent and was operating on the leaseholder
	// under a valid range lease.
	if ba.ReadConsistency == kvpb.CONSISTENT && ec.st.State == kvserverpb.LeaseState_VALID {
		ec.repl.updateTimestampCache(ctx, ba, br, pErr)
	}

	if ts := ec.replicatingSince; !ts.IsZero() {
		ec.repl.store.metrics.RaftReplicationLatency.RecordValue(timeutil.Since(ts).Nanoseconds())
	}

	// Release the latches acquired by the request and exit lock wait-queues. Must
	// be done AFTER the timestamp cache is updated. ec.g is set both for reads
	// and for writes. For writes, it is set only when the Raft proposal has
	// assumed responsibility for the request.
	//
	// TODO(replication): at the time of writing, there is no code path in which
	// this method is called and the Guard is not set. Consider removing this
	// check and upgrading the previous observation to an invariant.
	if ec.g != nil {
		ec.repl.concMgr.FinishReq(ctx, ec.g)
	}
}
