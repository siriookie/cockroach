// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// abortTxnAsyncTimeout is the context timeout for abortTxnAsyncLocked()
// rollbacks. If the intent resolver has spare async task capacity, this timeout
// only needs to be long enough for the EndTxn request to make it through Raft,
// but if the cleanup task is synchronous (to backpressure clients) then cleanup
// will be abandoned when the timeout expires. We generally want to clean up if
// possible, but not at any cost, so we set it high at 1 minute.
const abortTxnAsyncTimeout = time.Minute

// heartbeatTxnBufferPeriod is a buffer period used to determine when to start
// the heartbeat loop for the transaction. If the first locking operation
// occurs within this buffer period of expiration of the transaction, the
// transaction heartbeat should happen immediately, otherwise the heartbeat
// loop should start (at the latest) by this buffer period prior to the
// expiration. The buffer period should ensure that it is not possible
// for intents to be written prior to the transaction being considered expired.
// This attempts to avoid a transaction being considered expired (due to
// lacking a transaction record) by another pushing transaction that encounters
// its intents, as this will result in the transaction being aborted.
const heartbeatTxnBufferPeriod = 200 * time.Millisecond

// RandomizedTxnAnchorKeyEnabled dictates whether a transactions anchor key is
// chosen at random from all keys being locked in its first locking batch;
// otherwise, it's set to the first ever key that's locked by the transaction.
var RandomizedTxnAnchorKeyEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.transaction.randomized_anchor_key.enabled",
	"dictates whether a transactions anchor key is randomized or not",
	metamorphic.ConstantWithTestBool("kv.transaction.randomized_anchor_key.enabled", false),
	settings.WithPublic,
)

// txnHeartbeater is a txnInterceptor in charge of a transaction's heartbeat
// loop. Transaction coordinators heartbeat their transaction record
// periodically to indicate the liveness of their transaction. Other actors like
// concurrent transactions and GC processes observe a transaction record's last
// heartbeat time to learn about its disposition and to determine whether it
// should be considered abandoned. When a transaction is considered abandoned,
// other actors are free to abort it at will. As such, it is important for a
// transaction coordinator to heartbeat its transaction record with a
// periodicity well below the abandonment threshold.
//
// Transaction coordinators only need to perform heartbeats for transactions
// that risk running for longer than the abandonment duration. For transactions
// that finish well beneath this time, a heartbeat will never be sent and the
// EndTxn request will create and immediately finalize the transaction. However,
// for transactions that live long enough that they risk running into issues
// with other's perceiving them as abandoned, the first HeartbeatTxn request
// they send will create the transaction record in the PENDING state. Future
// heartbeats will update the transaction record to indicate progressively
// larger heartbeat timestamps.
//
// NOTE: there are other mechanisms by which concurrent actors could determine
// the liveness of transactions. One proposal is to have concurrent actors
// communicate directly with transaction coordinators themselves. This would
// avoid the need for transaction heartbeats and the PENDING transaction state
// entirely. Another proposal is to detect abandoned transactions and failed
// coordinators at an entirely different level - by maintaining a node health
// plane. This would function under the idea that if the node a transaction's
// coordinator is running on is alive then that transaction is still in-progress
// unless it specifies otherwise. These are both approaches we could consider in
// the future.
type txnHeartbeater struct {
	log.AmbientContext
	stopper      *stop.Stopper
	clock        *hlc.Clock
	metrics      *TxnMetrics
	loopInterval time.Duration
	st           *cluster.Settings
	knobs        *ClientTestingKnobs

	// wrapped is the next sender in the interceptor stack.
	wrapped lockedSender
	// gatekeeper is the sender to which heartbeat requests need to be sent. It is
	// set to the gatekeeper interceptor, so sending directly to it will bypass
	// all the other interceptors; heartbeats don't need them and they can only
	// hurt - we don't want heartbeats to get sequence numbers or to check any
	// intents. Note that the async rollbacks that this interceptor sometimes
	// sends got through `wrapped`, not directly through `gatekeeper`.
	gatekeeper lockedSender

	// mu contains state protected by the TxnCoordSender's mutex.
	mu struct {
		sync.Locker

		// txn is a reference to the TxnCoordSender's proto.
		txn *roachpb.Transaction

		// loopStarted indicates whether the heartbeat loop has been launched
		// for the transaction or not. It remains true once the loop terminates.
		loopStarted bool

		// loopCancel is a function to cancel the context of the heartbeat loop.
		// Non-nil if the heartbeat loop is currently running.
		loopCancel func()

		// finalObservedStatus is the finalized status that the heartbeat loop
		// observed while heartbeating the transaction's record. As soon as the
		// heartbeat loop observes a finalized status, it shuts down.
		//
		// If the status here is COMMITTED then the transaction definitely
		// committed. However, if the status here is ABORTED then the
		// transaction may or may not have been aborted. Instead, it's possible
		// that the transaction was committed by an EndTxn request and then its
		// record was garbage collected before the heartbeat request reached the
		// record. The only way to distinguish this situation from a truly
		// aborted transaction is to consider whether or not the transaction
		// coordinator sent an EndTxn request and, if so, consider whether it
		// succeeded or not.
		//
		// Because of this ambiguity, the status is not used to immediately
		// update txn in case the heartbeat loop raced with an EndTxn request.
		// Instead, it is used by the transaction coordinator to reject any
		// future requests sent though it (which indicates that the heartbeat
		// loop did not race with an EndTxn request).
		finalObservedStatus roachpb.TransactionStatus

		// ifReqs tracks the number of in-flight requests. This is expected to
		// be either 0 or 1, but we let the txnLockGatekeeper enforce that.
		//
		// This is used to make sure we don't send EndTxn(commit=false) from
		// abortTxnAsyncLocked() concurrently with another in-flight request.
		// The TxnCoordSender assumes synchronous operation; in particular,
		// the txnPipeliner must update its lock spans with pending responses
		// before attaching the final lock spans to the EndTxn request.
		ifReqs uint8

		// abortTxnAsyncPending, if true, signals that an abortTxnAsyncLocked()
		// call is waiting for in-flight requests to complete. Once the last
		// request returns (setting ifReqs=0), it calls abortTxnAsyncLocked().
		abortTxnAsyncPending bool

		// abortTxnAsyncResultC is non-nil when an abortTxnAsyncLocked()
		// rollback is in-flight. If a client rollback arrives concurrently, it
		// will wait for the result on this channel, collapsing the requests to
		// prevent concurrent rollbacks. Only EndTxn(commit=false) requests can
		// arrive during rollback, the TxnCoordSender blocks any others due to
		// finalObservedStatus.
		abortTxnAsyncResultC chan abortTxnAsyncResult
	}
}

type abortTxnAsyncResult struct {
	br   *kvpb.BatchResponse
	pErr *kvpb.Error
}

// init initializes the txnHeartbeater. This method exists instead of a
// constructor because txnHeartbeaters live in a pool in the TxnCoordSender.
func (h *txnHeartbeater) init(
	ac log.AmbientContext,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	metrics *TxnMetrics,
	loopInterval time.Duration,
	gatekeeper lockedSender,
	mu sync.Locker,
	txn *roachpb.Transaction,
	settings *cluster.Settings,
	testingKnobs *ClientTestingKnobs,
) {
	if testingKnobs == nil {
		testingKnobs = &ClientTestingKnobs{}
	}
	h.AmbientContext = ac
	h.stopper = stopper
	h.clock = clock
	h.metrics = metrics
	h.loopInterval = loopInterval
	h.st = settings
	h.knobs = testingKnobs
	h.gatekeeper = gatekeeper
	h.mu.Locker = mu
	h.mu.txn = txn
}

// SendLocked is part of the txnInterceptor interface.
func (h *txnHeartbeater) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	// 看当前 batch 里有没有 EndTxn。EndTxn 表示事务要 commit/rollback/prepare。
	etArg, hasET := ba.GetArg(kvpb.EndTxn)
	// 从当前 batch 中随机找一个会加锁/写 intent 的请求下标。
	// 如果 batch 完全是只读、非锁请求，这里会返回 -1。
	randLockingIdx, pErr := h.randLockingIndex(ba)
	if pErr != nil {
		// 随机选择 locking request 的过程中发现 batch 不合法，直接返回错误。
		return nil, pErr
	}
	if randLockingIdx != -1 {
		// 当前 batch 会留下锁/intent，说明事务需要一个服务端 txn record。
		// 如果 txn record 的 anchor key 还没确定，就用这个 batch 里的一个 locking key。
		// 如果已经确定过，则必须保持不变，因为 txn record 的位置不能在事务中途漂移。
		if len(h.mu.txn.Key) == 0 {
			// 取这个 locking request 的 key 作为 txn record 的 anchor key。
			anchor := ba.Requests[randLockingIdx].GetInner().Header().Key
			// 更新 TxnCoordSender 本地持有的 txn proto。
			h.mu.txn.Key = anchor
			// 同步更新当前 BatchRequest 上的 txn proto。
			// 这个 batch 是在设置 anchor key 之前构造的，所以这里要补进去。
			ba.Txn.Key = anchor
		}

		// 只要事务开始留下锁/intent，就需要 heartbeat txn record，
		// 防止事务记录长时间不更新后被其它事务当成过期事务 push/abort。
		if !h.mu.loopStarted {
			// 启动后台 heartbeat loop；后续会周期性发送 HeartbeatTxnRequest。
			h.startHeartbeatLoopLocked(ctx)
		}
	}

	if hasET {
		// EndTxnRequest 可能是 commit，也可能是 rollback。
		et := etArg.(*kvpb.EndTxnRequest)

		// 如果是 rollback，先主动停止 heartbeat loop。
		// 如果是 commit，不能在这里提前停 heartbeat，因为 commit 可能失败并返回 retryable error，
		// 事务还可能进入下一个 epoch 继续重试。
		if !et.Commit {
			// rollback 不需要继续 heartbeat；事务马上要被 abort。
			h.cancelHeartbeatLoopLocked()

			// 如果之前已经有 abortTxnAsyncLocked() 发起了异步 rollback，
			// 这里等待那个异步 rollback 的结果，避免再发送一个并发 rollback。
			// 否则栈底的 txnLockGatekeeper 会因为 root txn 不允许并发请求而报错。
			if resultC := h.mu.abortTxnAsyncResultC; resultC != nil {
				// 等待异步 rollback 结果时必须释放 TxnCoordSender 锁。
				// 因为异步请求返回时，txnLockGatekeeper 也需要重新拿这把锁。
				// 拿到异步 rollback 结果后，再重新加锁并把结果返回给当前调用方。
				h.mu.Unlock()
				defer h.mu.Lock()
				select {
				case res := <-resultC:
					// 异步 rollback 已经完成，直接复用它的 response/error。
					return res.br, res.pErr
				case <-ctx.Done():
					// 当前调用方不想等了，返回 context 错误。
					return nil, kvpb.NewError(ctx.Err())
				}
			}
		}
	}

	// 把 batch 继续交给下一层 interceptor。
	// ifReqs 记录当前有多少请求正在下层执行，用来和 abortTxnAsyncLocked() 协调：
	// 如果 heartbeat 发现事务已经被 abort，它可能要等正在执行的请求结束后再发异步 abort。
	// 注意：真正发送到下层时，栈底 txnLockGatekeeper 会释放 TxnCoordSender 锁。
	h.mu.ifReqs++
	br, pErr := h.wrapped.SendLocked(ctx, ba)
	h.mu.ifReqs--

	// 如果有异步 abort 正在等待所有 in-flight 请求结束，并且当前已经没有请求在飞，
	// 就在这里真正发起 abortTxnAsyncLocked()。
	// 此时 finalObservedStatus 已经被设置，普通后续请求会被 TxnCoordSender 拦住；
	// rollback 作为清理请求仍可能被允许。
	if h.mu.abortTxnAsyncPending && h.mu.ifReqs == 0 {
		h.abortTxnAsyncLocked(ctx)
		h.mu.abortTxnAsyncPending = false
	}

	// 返回下一层 interceptor / DistSender / KVServer 的结果。
	return br, pErr
}

// setWrapped is part of the txnInterceptor interface.
func (h *txnHeartbeater) setWrapped(wrapped lockedSender) {
	h.wrapped = wrapped
}

// populateLeafInputState is part of the txnInterceptor interface.
func (*txnHeartbeater) populateLeafInputState(*roachpb.LeafTxnInputState, interval.Tree) {}

// initializeLeaf is part of the txnInterceptor interface.
func (*txnHeartbeater) initializeLeaf(tis *roachpb.LeafTxnInputState) {}

// populateLeafFinalState is part of the txnInterceptor interface.
func (*txnHeartbeater) populateLeafFinalState(*roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (*txnHeartbeater) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) error {
	return nil
}

// epochBumpedLocked is part of the txnInterceptor interface.
func (h *txnHeartbeater) epochBumpedLocked() {}

// createSavepointLocked is part of the txnInterceptor interface.
func (*txnHeartbeater) createSavepointLocked(context.Context, *savepoint) {}

// releaseSavepointLocked is part of the txnInterceptor interface.
func (*txnHeartbeater) releaseSavepointLocked(context.Context, *savepoint) {}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (*txnHeartbeater) rollbackToSavepointLocked(context.Context, savepoint) {}

// closeLocked is part of the txnInterceptor interface.
func (h *txnHeartbeater) closeLocked() {
	h.cancelHeartbeatLoopLocked()
}

// startHeartbeatLoopLocked 在后台 goroutine 中启动事务 heartbeat loop。
func (h *txnHeartbeater) startHeartbeatLoopLocked(ctx context.Context) {
	if h.loopInterval < 0 {
		// 测试或特殊配置可以把 heartbeat interval 设成负数，表示完全禁用 heartbeat loop。
		log.KvExec.Infof(ctx, "coordinator heartbeat loop disabled")
		return
	}
	if h.mu.loopStarted {
		// 一个事务只允许启动一个 heartbeat loop；重复启动说明协调器状态机出错。
		log.KvExec.Fatal(ctx, "attempting to start a second heartbeat loop")
	}
	// 记录 trace/log 事件：即将启动 heartbeat loop。
	log.VEventf(ctx, 2, kvbase.SpawningHeartbeatLoopMsg)
	// 标记 heartbeat loop 已经启动过，后续 SendLocked 不会再次启动。
	h.mu.loopStarted = true
	// 不能在 init() 里加这个 log tag，因为 init() 时 txn 还没有填充，是零值。
	// 这里 txn 已经有 ID，所以可以把短事务 ID 加到日志 tag 中，方便定位 heartbeat 日志。
	h.AmbientContext.AddLogTag("txn-hb", h.mu.txn.Short())

	// 创建新的后台 context，避免 heartbeat loop 继承当前请求的 cancellation 或 tracing span。
	// 否则一次 SQL/KV 请求结束后，heartbeat loop 可能被错误取消。
	hbCtx, hbCancel := context.WithCancel(h.AnnotateCtx(context.Background()))

	// 默认第一次 heartbeat 会延迟 loopInterval 后再启动，以减少短事务的后台 goroutine 成本。
	// 但如果等到 heartbeatTxnBufferPeriod 后事务就会被认为过期，
	// 那就必须在启动 loop 前同步 heartbeat 一次，避免事务记录被别人当成过期事务处理。
	heartbeatLoopDelay := h.loopInterval
	now := h.clock.Now()
	if txnwait.IsExpired(
		now.Add(heartbeatTxnBufferPeriod.Nanoseconds(), 0 /* logical */),
		h.mu.txn,
	) {
		// 当前事务离过期太近，马上发送一次 HeartbeatTxnRequest。
		log.VEventf(ctx, 2, "heartbeating immediately to avoid expiration")
		h.heartbeatLocked(ctx)
	} else {
		// 事务还没接近过期，但可能在正常 loopInterval 到来前就接近过期。
		// 这里计算距离过期还有多久，用来决定是否提前启动 heartbeat loop。
		timeUntilExpiry := txnwait.TxnExpiration(h.mu.txn).GoTime().Sub(now.GoTime())
		if (timeUntilExpiry - heartbeatTxnBufferPeriod) < heartbeatLoopDelay {
			// 正常 loopInterval 太晚了，提前到“过期前 buffer 时间”启动 heartbeat loop。
			log.VEventf(ctx, 2, "scheduling heartbeat early to avoid expiration")
			heartbeatLoopDelay = timeUntilExpiry - heartbeatTxnBufferPeriod
		}
	}
	// 延迟启动 heartbeat loop goroutine：
	// - 要么等到第一个 loopInterval；
	// - 要么等到事务过期前的 buffer 时间；
	// 取更早的那个时间点。
	// 这么做是为了避免短写事务也立刻创建后台 goroutine。
	// benchmark 显示，在高并发点写场景下，这能提升约 3% 吞吐。
	timer := time.AfterFunc(heartbeatLoopDelay, func() {
		const taskName = "kv.TxnCoordSender: heartbeat loop"
		var span *tracing.Span
		// 给 heartbeat loop 创建独立 tracing span。
		hbCtx, span = h.AmbientContext.Tracer.StartSpanCtx(hbCtx, taskName)
		defer span.Finish()

		// 通过 stopper 注册后台任务，保证节点关闭时任务能被统一管理。
		// RunTask 只会在 stopper quiesce 时返回错误，这里可以忽略。
		_ = h.stopper.RunTask(hbCtx, taskName, h.heartbeatLoop)
	})

	h.mu.loopCancel = func() {
		// 如果 timer 还没触发，停止它，避免 heartbeat loop 被启动。
		timer.Stop()
		// 如果 heartbeat loop 已经启动或即将使用 hbCtx，取消这个后台 context。
		hbCancel()
	}
}

func (h *txnHeartbeater) cancelHeartbeatLoopLocked() {
	// If the heartbeat loop has already started, cancel it.
	if h.heartbeatLoopRunningLocked() {
		h.mu.loopCancel()
		h.mu.loopCancel = nil
	}
}

func (h *txnHeartbeater) heartbeatLoopRunningLocked() bool {
	return h.mu.loopCancel != nil
}

// heartbeatLoop periodically sends a HeartbeatTxn request to the transaction
// record, stopping in the event the transaction is aborted or committed after
// attempting to resolve the intents.
func (h *txnHeartbeater) heartbeatLoop(ctx context.Context) {
	defer func() {
		h.mu.Lock()
		h.cancelHeartbeatLoopLocked()
		h.mu.Unlock()
	}()

	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(h.loopInterval)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	// Loop is only spawned after loopInterval, so heartbeat immediately.
	if !h.heartbeat(ctx) {
		return
	}

	// Loop with ticker for periodic heartbeats.
	for {
		select {
		case <-tickChan:
			if !h.heartbeat(ctx) {
				// The heartbeat noticed a finalized transaction,
				// so shut down the heartbeat loop.
				return
			}
		case <-ctx.Done():
			// Transaction finished normally.
			return
		case <-h.stopper.ShouldQuiesce():
			return
		}
	}
}

// heartbeat is a convenience method to be called by the heartbeat loop, acquiring
// the mutex and issuing a request using heartbeatLocked before releasing it.
// See comment on heartbeatLocked for more explanation.
func (h *txnHeartbeater) heartbeat(ctx context.Context) bool {
	// Like with the TxnCoordSender, the locking here is peculiar. The lock is not
	// held continuously throughout the heartbeatLocked method: we acquire the
	// lock here and then, inside the wrapped.Send() call, the interceptor at the
	// bottom of the stack will unlock until it receives a response.
	h.mu.Lock()
	defer h.mu.Unlock()

	// The heartbeat loop might have raced with the cancellation of the heartbeat.
	if ctx.Err() != nil {
		return false
	}

	return h.heartbeatLocked(ctx)
}

// heartbeatLocked 向事务记录发送 HeartbeatTxnRequest。
// 返回 true 表示还应该继续 heartbeat；
// 返回 false 表示事务已经不再是 PENDING，没有必要继续 heartbeat。
func (h *txnHeartbeater) heartbeatLocked(ctx context.Context) bool {
	// heartbeat 只对 PENDING 事务有意义。
	// 其它状态要么已经稳定，要么已经终止。
	switch h.mu.txn.Status {
	case roachpb.PENDING:
		// PENDING 表示事务仍在运行，继续发送 heartbeat。
	case roachpb.PREPARED:
		// PREPARED 表示事务已经进入两阶段提交的 prepared 状态。
		// prepared 事务不靠 heartbeat 保活；它会一直保持 active，直到 commit 或 rollback。
		return false
	case roachpb.ABORTED:
		// ABORTED 表示事务已经被 abort，继续 heartbeat 没有意义。
		// 客户端后续需要发送 rollback 做清理。
		return false
	case roachpb.COMMITTED:
		// COMMITTED 后 heartbeat loop 应该早就被停止。
		// 如果还在跑，说明本地状态机没有正确关闭 heartbeat loop。
		log.KvExec.Fatalf(ctx, "txn committed but heartbeat loop hasn't been signaled to stop: %s", h.mu.txn)
	default:
		// 事务状态枚举出现未知值，直接 fatal。
		log.KvExec.Fatalf(ctx, "unexpected txn status in heartbeat loop: %s", h.mu.txn)
	}

	// 克隆当前事务 proto，放到 HeartbeatTxnRequest 所属的 BatchRequest 里。
	// 这样下层发送过程中即使本地 txn 状态变化，也不会直接修改这个请求里的 txn。
	txn := h.mu.txn.Clone()
	if txn.Key == nil {
		// heartbeat 必须知道 txn record anchor key。
		// 没有 txn.Key 就无法定位 keys.TransactionKey(txn.Key, txn.ID)。
		log.KvExec.Fatalf(ctx, "attempting to heartbeat txn without anchor key: %v", txn)
	}
	// 构造一个只包含 HeartbeatTxnRequest 的 batch。
	ba := &kvpb.BatchRequest{}
	// 把事务 proto 放进 batch header，表示这是这个事务自己的 heartbeat。
	ba.Txn = txn
	ba.Add(&kvpb.HeartbeatTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			// HeartbeatTxnRequest 的 key 必须是 txn.Key，也就是 txn record anchor key。
			Key: txn.Key,
		},
		// 告诉服务端“协调器当前还活着”的时间点。
		Now: h.clock.Now(),
	})

	// heartbeat 直接通过 gatekeeper 发送，而不是从 interceptor 栈顶重新走一遍。
	// 这样可以避开 seq num、write buffer、pipeliner、committer 等事务请求处理逻辑。
	// gatekeeper 仍然负责发送期间释放 TxnCoordSender 锁，并调用底层 DistSender。
	log.VEvent(ctx, 2, "heartbeat")
	br, pErr := h.gatekeeper.SendLocked(ctx, ba)

	// heartbeat RPC 返回时，本地事务可能已经被客户端 commit/rollback 改成非 PENDING。
	// 如果已经不是 PENDING，就忽略这次 heartbeat 结果，并停止 heartbeat loop。
	if h.mu.txn.Status != roachpb.PENDING {
		return false
	}

	var respTxn *roachpb.Transaction
	if pErr != nil {
		// heartbeat 失败。下面只对 TransactionAbortedError 做特殊处理；
		// 其它错误会从错误里取 txn proto，用于后续判断。
		log.VEventf(ctx, 2, "heartbeat failed for %s: %s", h.mu.txn, pErr)

		// 这里必须处理一种特殊情况：
		// TransactionAbortedError 可能没有携带 transaction proto。
		//
		// TODO(nvanbenschoten): 后续希望只有这种情况会返回 aborted txn。
		if _, ok := pErr.GetDetail().(*kvpb.TransactionAbortedError); ok {
			// 注意：这里也可能是事务其实已经 commit，但 txn record 已经被 GC，
			// 服务端看不到记录后合成了 aborted 结果。
			// 这种情况下再发 abort 不会破坏正确性，因为 intents 已经被清理。
			//
			// 这里不能把这个 heartbeat 错误直接告诉客户端。
			// 客户端会通过自己的 commit/rollback 请求拿到确定结果或 ambiguous 结果；
			// heartbeat 这边没有更清晰的信息可以提供。
			// 但我们要阻止客户端继续在这个事务上发普通请求。
			log.VEventf(ctx, 1, "Heartbeat detected aborted txn, cleaning up for %s", h.mu.txn)
			// 异步发送 rollback，用来清理事务 intent。
			h.abortTxnAsyncLocked(ctx)
			// 记录 heartbeat 已经观察到事务最终为 ABORTED。
			// maybeRejectClientLocked 后续会据此阻止普通请求继续执行。
			h.mu.finalObservedStatus = roachpb.ABORTED
			return false
		}

		// 非 TransactionAbortedError，尝试从错误中取回响应事务 proto。
		respTxn = pErr.GetTxn()
	} else {
		// heartbeat 成功时，响应 header 里会带回服务端看到的事务 proto。
		respTxn = br.Txn
	}

	// 如果服务端返回的事务 proto 已经 finalized，说明事务已经结束，
	// 这里要停止 heartbeat loop。
	if respTxn != nil && respTxn.Status.IsFinalized() {
		switch respTxn.Status {
		case roachpb.COMMITTED:
			// 事务已经 COMMITTED，直接停 heartbeat loop。
			// 通常说明 heartbeat 和 EndTxn(commit=true) 并发了，commit 先成功。
		case roachpb.ABORTED:
			// 服务端看到事务已经 ABORTED。
			// 异步发送 rollback，清理这个事务留下的 intents，然后停止 heartbeat loop。
			log.VEventf(ctx, 1, "Heartbeat detected aborted txn, cleaning up for %s", h.mu.txn)
			h.abortTxnAsyncLocked(ctx)
		}
		// 记录 heartbeat 观察到的最终状态，供 TxnCoordSender 拒绝后续普通请求。
		h.mu.finalObservedStatus = respTxn.Status
		return false
	}
	// 服务端仍认为事务是 PENDING，继续下一轮 heartbeat。
	return true
}

// abortTxnAsyncLocked sends an EndTxn(commit=false) asynchronously.
// The purpose of the async cleanup is to resolve transaction intents as soon
// as possible when a transaction coordinator observes an ABORTED transaction.
func (h *txnHeartbeater) abortTxnAsyncLocked(ctx context.Context) {

	// If a request is in flight, we must wait for it to complete first such
	// that txnPipeliner can record its lock spans and attach them to the EndTxn
	// request we'll send.
	if h.mu.ifReqs > 0 {
		h.mu.abortTxnAsyncPending = true
		log.VEventf(ctx, 2, "async abort waiting for in-flight request for txn %s", h.mu.txn)
		return
	}

	// Construct a batch with an EndTxn request.
	txn := h.mu.txn.Clone()
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: txn}
	ba.Add(&kvpb.EndTxnRequest{
		Commit: false,
		// Resolved intents should maintain an abort span entry to prevent
		// concurrent requests from failing to notice the transaction was aborted.
		Poison: true,
	})
	// NB: Setting `Source: kvpb.AdmissionHeader_OTHER` means this request will
	// bypass AC.
	ba.AdmissionHeader = kvpb.AdmissionHeader{
		Priority:   txn.AdmissionPriority,
		CreateTime: h.clock.PhysicalTime().UnixNano(),
		Source:     kvpb.AdmissionHeader_OTHER,
	}

	const taskName = "txnHeartbeater: aborting txn"
	log.VEventf(ctx, 2, "async abort for txn: %s", txn)

	work := func(ctx context.Context) {
		if err := timeutil.RunWithTimeout(ctx, taskName, abortTxnAsyncTimeout,
			func(ctx context.Context) error {
				h.mu.Lock()
				defer h.mu.Unlock()

				// If we find an abortTxnAsyncResultC, that means an async
				// rollback request is already in flight, so there's no
				// point in us running another. This can happen because the
				// TxnCoordSender also calls abortTxnAsyncLocked()
				// independently of the heartbeat loop.
				if h.mu.abortTxnAsyncResultC != nil {
					log.VEventf(ctx, 2,
						"skipping async abort due to concurrent async abort for %s", txn)
					return nil
				}

				// TxnCoordSender allows EndTxn(commit=false) through even
				// after we set finalObservedStatus, and that request can
				// race with us for the mutex. Thus, if we find an in-flight
				// request here, after checking ifReqs=0 before being spawned,
				// we deduce that it must have been a rollback and there's no
				// point in sending another rollback.
				if h.mu.ifReqs > 0 {
					log.VEventf(ctx, 2,
						"skipping async abort due to client rollback for %s", txn)
					return nil
				}

				// Set up a result channel to signal to an incoming client
				// rollback that an async rollback is already in progress,
				// and pass it the result. The buffer allows storing the
				// result even when no client rollback arrives. Recall that
				// the SendLocked() call below releases the mutex while
				// running, allowing concurrent incoming requests.
				h.mu.abortTxnAsyncResultC = make(chan abortTxnAsyncResult, 1)

				// Send the abort request through the interceptor stack. This is
				// important because we need the txnPipeliner to append lock spans
				// to the EndTxn request.
				br, pErr := h.wrapped.SendLocked(ctx, ba)
				if pErr != nil {
					log.VErrEventf(ctx, 1, "async abort failed for %s: %s ", txn, pErr)
					h.metrics.AsyncRollbacksFailed.Inc(1)
				}

				// Pass the result to a waiting client rollback, if any, and
				// remove the channel since we're no longer in flight.
				h.mu.abortTxnAsyncResultC <- abortTxnAsyncResult{br: br, pErr: pErr}
				h.mu.abortTxnAsyncResultC = nil
				return nil
			},
		); err != nil {
			log.VEventf(ctx, 1, "async abort failed for %s: %s", txn, err)
		}
	}

	asyncCtx, hdl, err := h.stopper.GetHandle(h.AnnotateCtx(context.Background()), stop.TaskOpts{
		TaskName: taskName,
	})
	if err != nil {
		log.KvExec.Warningf(ctx, "%v", err)
		h.metrics.AsyncRollbacksFailed.Inc(1)
		return
	}
	go func(ctx context.Context) {
		defer hdl.Activate(ctx).Release(ctx)
		work(ctx)
	}(asyncCtx)
}

// randLockingIndex 返回 BatchRequest 中某个会获取锁的请求下标。
// 如果这个 batch 没有任何会获取锁/写 intent 的请求，则返回 -1。
// 它还会顺便校验：如果 batch 里包含 EndTxnRequest，那么 EndTxn 必须是最后一个请求。
func (h *txnHeartbeater) randLockingIndex(ba *kvpb.BatchRequest) (int, *kvpb.Error) {
	// 这里事先不知道 batch 里有多少个 locking request。
	// 为了在只遍历一遍 batch 的情况下，从所有 locking request 中“等概率随机选一个”，
	// 使用 reservoir sampling：
	// - numLocking 记录目前已经见过多少个 locking request。
	// - idx 记录当前被选中的 locking request 下标。
	numLocking := 0
	idx := -1
	// 测试 knob 或 cluster setting 可以关闭随机化。
	// 关闭后就不做 reservoir sampling，直接选择第一个 locking request。
	disableRandomization := h.knobs.DisableTxnAnchorKeyRandomization ||
		!RandomizedTxnAnchorKeyEnabled.Get(&h.st.SV)
	// 遍历 batch 中的每个请求，寻找会加锁/写 intent 的请求。
	for i, ru := range ba.Requests {
		args := ru.GetInner()
		if i < len(ba.Requests)-1 /* if not last */ {
			if _, ok := args.(*kvpb.EndTxnRequest); ok {
				// EndTxn 只能作为 batch 的最后一个请求。
				// 因为 EndTxn 表示事务结束，后面不应该再跟普通 KV 操作。
				return -1, kvpb.NewErrorf("%s sent as non-terminal call", args.Method())
			}
		}
		// IsLocking 表示这个请求会获取锁或写 intent。
		// 这类请求说明事务需要服务端 txn record，也就是 SendLocked 里要设置 anchor key。
		if kvpb.IsLocking(args) {
			if disableRandomization {
				// 随机化关闭时，直接返回第一个 locking request 的下标。
				return i, nil
			}
			// 又看到一个 locking request。
			numLocking++
			if numLocking == 1 {
				// 第一个 locking request 必然先成为候选，不需要生成随机数。
				idx = i
			} else if randutil.FastUint32()%uint32(numLocking) == 0 {
				// reservoir sampling：第 N 个 locking request 以 1/N 的概率替换当前候选。
				// 这样遍历结束后，每个 locking request 被选中的概率都是 1/总数。
				idx = i
			}
		}
	}
	// 没有 locking request 时 idx 仍是 -1；否则返回被选中的 locking request 下标。
	return idx, nil
}
