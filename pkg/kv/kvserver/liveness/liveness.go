// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package liveness

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	diskStorage "github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	timeUntilNodeDeadSettingName    = "server.time_until_store_dead"
	timeAfterNodeSuspectSettingName = "server.time_after_store_suspect"
)

// Setting this to less than the interval for gossiping stores is a big
// no-no, since this value is compared to the age of the most recent gossip
// from each store to determine whether that store is live. Put a buffer of
// 15 seconds on top to allow time for gossip to propagate.
const minTimeUntilNodeDead = gossip.StoresInterval + 15*time.Second

// TimeUntilNodeDead wraps "server.time_until_store_dead".
var TimeUntilNodeDead = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	timeUntilNodeDeadSettingName,
	"the time after which if there is no new gossiped information about a store, it is considered dead",
	5*time.Minute,
	settings.DurationWithMinimum(minTimeUntilNodeDead),
	settings.WithPublic,
)

// Setting this to less than the interval for gossiping stores is a big
// no-no, since this value is compared to the age of the most recent gossip
// from each store to determine whether that store is live.
const minTimeUntilNodeSuspect = gossip.StoresInterval

// We enforce a maximum value of 5 minutes for this settings, as setting this
// to high may result in a prolonged period of unavailability as a recovered
// store will not be able to acquire leases or replicas for a long time.
const maxTimeAfterNodeSuspect = 5 * time.Minute

// TimeAfterNodeSuspect measures how long we consider a store suspect since
// it's last failure.
var TimeAfterNodeSuspect = settings.RegisterDurationSetting(
	settings.SystemOnly,
	timeAfterNodeSuspectSettingName,
	"the amount of time we consider a node suspect for after it becomes unavailable."+
		" A suspect node is typically treated the same as an unavailable node.",
	30*time.Second,
	settings.DurationInRange(minTimeUntilNodeSuspect, maxTimeAfterNodeSuspect),
)

var (
	// ErrMissingRecord is returned when asking for liveness information
	// about a node for which nothing is known. This happens when attempting to
	// {d,r}ecommission a non-existent node.
	ErrMissingRecord = errors.New("missing liveness record")

	// ErrRecordCacheMiss is returned when asking for the liveness
	// record of a given node and it is not found in the in-memory cache.
	ErrRecordCacheMiss = errors.New("liveness record not found in cache")

	// errChangeMembershipStatusFailed is returned when we're not able to
	// conditionally write the target membership status. It's safe to retry
	// when encountering this error.
	errChangeMembershipStatusFailed = errors.New("failed to change the membership status")

	// ErrEpochIncremented is returned when a heartbeat request fails because
	// the underlying liveness record has had its epoch incremented.
	ErrEpochIncremented = errors.New("heartbeat failed on epoch increment")

	// ErrEpochAlreadyIncremented is returned by IncrementEpoch when
	// someone else has already incremented the epoch to the desired
	// value.
	ErrEpochAlreadyIncremented = errors.New("epoch already incremented")
)

type ErrEpochCondFailed struct {
	expected, actual livenesspb.Liveness
}

// SafeFormatError implements errors.SafeFormatter.
func (e *ErrEpochCondFailed) SafeFormatError(p errors.Printer) error {
	p.Printf(
		"liveness record changed while incrementing epoch for %+v; actual is %+v; is the node still live?",
		redact.Safe(e.expected), redact.Safe(e.actual))
	return nil
}

func (e *ErrEpochCondFailed) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

func (e *ErrEpochCondFailed) Error() string {
	return fmt.Sprint(e)
}

type errRetryLiveness struct {
	error
}

func (e *errRetryLiveness) Cause() error {
	return e.error
}

func (e *errRetryLiveness) Error() string {
	return fmt.Sprintf("%T: %s", *e, e.error)
}

func isErrRetryLiveness(ctx context.Context, err error) bool {
	if errors.HasType(err, (*kvpb.AmbiguousResultError)(nil)) {
		// We generally want to retry ambiguous errors immediately, except if the
		// ctx is canceled - in which case the ambiguous error is probably caused
		// by the cancellation (and in any case it's pointless to retry with a
		// canceled ctx).
		return ctx.Err() == nil
	} else if errors.HasType(err, (*kvpb.TransactionStatusError)(nil)) {
		// 21.2 nodes can return a TransactionStatusError when they should have
		// returned an AmbiguousResultError.
		// TODO(andrei): Remove this in 22.2.
		return true
	} else if errors.Is(err, kv.OnePCNotAllowedError{}) {
		return true
	}
	return false
}

// Node liveness metrics counter names.
var (
	metaLiveNodes = metric.Metadata{
		Name:        "liveness.livenodes",
		Help:        "Number of live nodes in the cluster (will be 0 if this node is not itself live)",
		Measurement: "Nodes",
		Unit:        metric.Unit_COUNT,
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_REPLICATION,
		HowToUse:    "This is a critical metric that tracks the live nodes in the cluster.",
	}
	metaHeartbeatsInFlight = metric.Metadata{
		Name:        "liveness.heartbeatsinflight",
		Help:        "Number of in-flight liveness heartbeats from this node",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatSuccesses = metric.Metadata{
		Name:        "liveness.heartbeatsuccesses",
		Help:        "Number of successful node liveness heartbeats from this node",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatFailures = metric.Metadata{
		Name:        "liveness.heartbeatfailures",
		Help:        "Number of failed node liveness heartbeats from this node",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
		Visibility:  metric.Metadata_SUPPORT,
	}
	metaEpochIncrements = metric.Metadata{
		Name:        "liveness.epochincrements",
		Help:        "Number of times this node has incremented its liveness epoch",
		Measurement: "Epochs",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatLatency = metric.Metadata{
		Name:        "liveness.heartbeatlatency",
		Help:        "Node liveness heartbeat latency",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
		Visibility:  metric.Metadata_ESSENTIAL,
		Category:    metric.Metadata_REPLICATION,
		HowToUse:    "If this metric exceeds 1 second, it is a sign of cluster instability.",
	}
)

// Metrics holds metrics for use with node liveness activity.
type Metrics struct {
	LiveNodes          *metric.Gauge
	HeartbeatsInFlight *metric.Gauge
	HeartbeatSuccesses *metric.Counter
	HeartbeatFailures  telemetry.CounterWithMetric
	EpochIncrements    telemetry.CounterWithMetric
	HeartbeatLatency   metric.IHistogram
}

// IsLiveCallback is invoked when a node's IsLive state changes to true.
// Callbacks can be registered via NodeLiveness.RegisterCallback().
type IsLiveCallback func(livenesspb.Liveness)

// HeartbeatCallback is invoked whenever this node updates its own liveness status,
// indicating that it is alive.
// TODO(baptist): Remove this callback. The only usage of this is for logging an
// event at startup. This is a little heavyweight of a mechanism for that.
type HeartbeatCallback func(context.Context)

// NodeLiveness is a centralized failure detector that coordinates
// with the epoch-based range system to provide for leases of
// indefinite length (replacing frequent per-range lease renewals with
// heartbeats to the liveness system).
//
// It is also used as a general-purpose failure detector, but it is
// not ideal for this purpose. It is inefficient due to the use of
// replicated durable writes, and is not very sensitive (it primarily
// tests connectivity from the node to the liveness range; a node with
// a failing disk could still be considered live by this system).
//
// The persistent state of node liveness is stored in the KV layer,
// near the beginning of the keyspace. These are normal MVCC keys,
// written by CPut operations in 1PC transactions (the use of
// transactions and MVCC is regretted because it means that the
// liveness span depends on MVCC GC and can get overwhelmed if GC is
// not working. Transactions were used only to piggyback on the
// transaction commit trigger). The leaseholder of the liveness range
// gossips its contents whenever they change (only the changed
// portion); other nodes rarely read from this range directly.
//
// The use of conditional puts is crucial to maintain the guarantees
// needed by epoch-based leases. Both the Heartbeat and IncrementEpoch
// on this type require an expected value to be passed in; see
// comments on those methods for more.
//
// TODO(bdarnell): Also document interaction with draining and decommissioning.
type NodeLiveness struct {
	ambientCtx        log.AmbientContext
	stopper           *stop.Stopper
	clock             *hlc.Clock
	storage           Storage
	livenessThreshold time.Duration
	cache             *Cache
	renewalDuration   time.Duration
	selfSem           chan struct{}
	otherSem          chan struct{}
	// heartbeatPaused contains an atomically-swapped number representing a bool
	// (1 or 0). heartbeatToken is a channel containing a token which is taken
	// when heartbeating or when pausing the heartbeat. Used for testing.
	heartbeatPaused       uint32
	heartbeatToken        chan struct{}
	metrics               Metrics
	onNodeDecommissioned  func(id roachpb.NodeID) // noop if nil
	onNodeDecommissioning func(id roachpb.NodeID) // noop if nil
	engineSyncs           *singleflight.Group

	// onIsLiveMu holds callback registered by stores.
	// They fire when a node transitions from not live to live.
	onIsLiveMu struct {
		syncutil.Mutex
		callbacks []IsLiveCallback
	} // see RegisterCallback

	// onSelfHeartbeat is invoked after every successful heartbeat
	// of the local liveness instance's heartbeat loop.
	onSelfHeartbeat HeartbeatCallback

	// engines is written to before heartbeating to avoid maintaining liveness
	// when a local disks is stalled.
	engines []diskStorage.Engine

	// Set to true once Start is called. RegisterCallback can not be called after
	// Start is called.
	started atomic.Bool
}

// Record is a liveness record that has been read from the database, together
// with its database encoding. The encoding is useful for CPut-ing an update to
// the liveness record: the raw value will act as the expected value. This way
// the proto's encoding can change without the CPut failing.
type Record struct {
	livenesspb.Liveness
	// raw represents the raw bytes read from the database - suitable to pass to a
	// CPut. Nil if the value doesn't exist in the DB.
	raw []byte
}

// NodeLivenessOptions is the input to NewNodeLiveness.
//
// The IsLiveCallbacks are registered after construction but before Start is
// called. Everything else is initialized through these Options.
type NodeLivenessOptions struct {
	AmbientCtx              log.AmbientContext
	Stopper                 *stop.Stopper
	Clock                   *hlc.Clock
	Storage                 Storage
	LivenessThreshold       time.Duration
	RenewalDuration         time.Duration
	HistogramWindowInterval time.Duration
	// OnNodeDecommissioned is invoked whenever the instance learns that a
	// node was permanently removed from the cluster. This method must be
	// idempotent as it may be invoked multiple times and defaults to a
	// noop.
	OnNodeDecommissioned func(id roachpb.NodeID)
	// OnNodeDecommissioning is invoked when a node is detected to be
	// decommissioning.
	OnNodeDecommissioning func(id roachpb.NodeID)
	Engines               []diskStorage.Engine
	OnSelfHeartbeat       HeartbeatCallback
	Cache                 *Cache
}

// NewNodeLiveness returns a new instance of NodeLiveness configured
// with the specified gossip instance.
func NewNodeLiveness(opts NodeLivenessOptions) *NodeLiveness {
	nl := &NodeLiveness{
		ambientCtx:            opts.AmbientCtx,
		stopper:               opts.Stopper,
		clock:                 opts.Clock,
		storage:               opts.Storage,
		livenessThreshold:     opts.LivenessThreshold,
		renewalDuration:       opts.RenewalDuration,
		selfSem:               make(chan struct{}, 1),
		otherSem:              make(chan struct{}, 1),
		heartbeatToken:        make(chan struct{}, 1),
		onNodeDecommissioned:  opts.OnNodeDecommissioned,
		onNodeDecommissioning: opts.OnNodeDecommissioning,
		engineSyncs:           singleflight.NewGroup("engine sync", "engine"),
		engines:               opts.Engines,
		onSelfHeartbeat:       opts.OnSelfHeartbeat,
		cache:                 opts.Cache,
	}
	nl.metrics = Metrics{
		LiveNodes:          metric.NewFunctionalGauge(metaLiveNodes, nl.numLiveNodes),
		HeartbeatsInFlight: metric.NewGauge(metaHeartbeatsInFlight),
		HeartbeatSuccesses: metric.NewCounter(metaHeartbeatSuccesses),
		HeartbeatFailures:  telemetry.NewCounterWithMetric(metaHeartbeatFailures),
		EpochIncrements:    telemetry.NewCounterWithMetric(metaEpochIncrements),
		HeartbeatLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaHeartbeatLatency,
			Duration:     opts.HistogramWindowInterval,
			BucketConfig: metric.IOLatencyBuckets,
		}),
	}
	nl.cache.setLivenessChangedFn(nl.cacheUpdated)
	nl.heartbeatToken <- struct{}{}

	return nl
}

var errNodeDrainingSet = errors.New("node is already draining")

func (nl *NodeLiveness) sem(nodeID roachpb.NodeID) chan struct{} {
	if nodeID == nl.cache.selfID() {
		return nl.selfSem
	}
	return nl.otherSem
}

// SetDraining attempts to update this node's liveness record to put itself
// into the draining state.
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (nl *NodeLiveness) SetDraining(
	ctx context.Context, drain bool, reporter func(int, redact.SafeString),
) error {
	ctx = nl.ambientCtx.AnnotateCtx(ctx)
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = nl.stopper.ShouldQuiesce()
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		oldLivenessRec, ok := nl.cache.self()
		if !ok {
			// There was a cache miss, let's now fetch the record from KV
			// directly.
			nodeID := nl.cache.selfID()
			livenessRec, err := nl.getLivenessRecordFromKV(ctx, nodeID)
			if err != nil {
				return err
			}
			oldLivenessRec = livenessRec
		}
		if err := nl.setDrainingInternal(ctx, oldLivenessRec, drain, reporter); err != nil {
			if log.V(1) {
				log.KvExec.Infof(ctx, "attempting to set liveness draining status to %v: %v", drain, err)
			}
			if grpcutil.IsConnectionRejected(err) {
				return err
			}
			continue
		}
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("failed to drain self")
}

// SetMembershipStatus changes the liveness record to reflect the target
// membership status. It does so idempotently, and may retry internally until it
// observes its target state durably persisted. It returns whether it was able
// to change the membership status (as opposed to it returning early when
// finding the target status possibly set by another node).
func (nl *NodeLiveness) SetMembershipStatus(
	ctx context.Context, nodeID roachpb.NodeID, targetStatus livenesspb.MembershipStatus,
) (statusChanged bool, err error) {
	ctx = nl.ambientCtx.AnnotateCtx(ctx)

	attempt := func() (bool, error) {
		// Allow only one decommissioning attempt in flight per node at a time.
		// This is required for correct results since we may otherwise race with
		// concurrent `IncrementEpoch` calls and get stuck in a situation in
		// which the cached liveness is has decommissioning=false while it's
		// really true, and that means that SetDecommissioning becomes a no-op
		// (which is correct) but that our cached liveness never updates to
		// reflect that.
		//
		// See https://github.com/cockroachdb/cockroach/issues/17995.
		sem := nl.sem(nodeID)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return false, ctx.Err()
		}
		defer func() {
			<-sem
		}()

		// We need the current liveness in each iteration.
		//
		// We ignore any liveness record in Gossip because we may have to fall back
		// to the KV store anyway. The scenario in which this is needed is:
		// - kill node 2 and stop node 1
		// - wait for node 2's liveness record's Gossip entry to expire on all surviving nodes
		// - restart node 1; it'll never see node 2 in `GetLiveness` unless the whole
		//   node liveness span gets regossiped (unlikely if it wasn't the lease holder
		//   for that span)
		// - can't decommission node 2 from node 1 without KV fallback.
		//
		// See #20863.
		oldLivenessRec, err := nl.getLivenessRecordFromKV(ctx, nodeID)
		if err != nil {
			return false, err
		}

		return nl.setMembershipStatusInternal(ctx, oldLivenessRec, targetStatus)
	}

	for {
		statusChanged, err := attempt()
		if errors.Is(err, errChangeMembershipStatusFailed) {
			// Expected when epoch incremented, it's safe to retry.
			continue
		}
		return statusChanged, err
	}
}

func (nl *NodeLiveness) setDrainingInternal(
	ctx context.Context, oldLivenessRec Record, drain bool, reporter func(int, redact.SafeString),
) error {
	sem := nl.selfSem
	// Allow only one attempt to set the draining field at a time.
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-sem
	}()

	if oldLivenessRec.Liveness == (livenesspb.Liveness{}) {
		return errors.AssertionFailedf("invalid old liveness record; found to be empty")
	}

	// Let's compute what our new liveness record should be. We start off with a
	// copy of our existing liveness record.
	newLiveness := oldLivenessRec.Liveness

	if reporter != nil && drain && !newLiveness.Draining {
		// Report progress to the Drain RPC.
		reporter(1, "liveness record")
	}
	newLiveness.Draining = drain

	update := LivenessUpdate{
		oldLiveness: oldLivenessRec.Liveness,
		newLiveness: newLiveness,
		oldRaw:      oldLivenessRec.raw,
	}
	// TODO(baptist): retry on failure.
	written, err := nl.updateLiveness(ctx, update, func(actual Record) error {
		// Handle a stale cache by updating with the value we just read.
		nl.cache.maybeUpdate(ctx, actual)

		if actual.Draining == update.newLiveness.Draining {
			return errNodeDrainingSet
		}
		return errors.New("failed to update liveness record because record has changed")
	})
	if err != nil {
		if log.V(1) {
			log.KvExec.Infof(ctx, "updating liveness record: %v", err)
		}
		if errors.Is(err, errNodeDrainingSet) {
			return nil
		}
		return err
	}

	nl.cache.maybeUpdate(ctx, written)
	return nil
}

func (nl *NodeLiveness) cacheUpdated(old livenesspb.Liveness, new livenesspb.Liveness) {
	// TODO(baptist): This won't work correctly we remove expiration timestamp.
	// Need to use a different signal to determine if liveness changed.
	now := nl.clock.Now()
	if !old.IsLive(now) && new.IsLive(now) {
		for _, fn := range nl.callbacks() {
			fn(new)
		}
	}
	if !old.Membership.Decommissioned() && new.Membership.Decommissioned() && nl.onNodeDecommissioned != nil {
		nl.onNodeDecommissioned(new.NodeID)
	}
	if !old.Membership.Decommissioning() && new.Membership.Decommissioning() && nl.onNodeDecommissioning != nil {
		nl.onNodeDecommissioning(new.NodeID)
	}
	if log.V(2) {
		log.KvExec.Infof(nl.ambientCtx.AnnotateCtx(context.Background()), "received liveness update: %s", new)
	}
}

// CreateLivenessRecord creates a liveness record for the node specified by the
// given node ID. This is typically used when adding a new node to a running
// cluster, or when bootstrapping a cluster through a given node.
//
// This is a pared down version of Start; it exists only to durably
// persist a liveness to record the node's existence. Nodes will heartbeat their
// records after starting up, and incrementing to epoch=1 when doing so, at
// which point we'll set an appropriate expiration timestamp, gossip the
// liveness record, and update our in-memory representation of it.
//
// NB: An existing liveness record is not overwritten by this method, we return
// an error instead.
func (nl *NodeLiveness) CreateLivenessRecord(ctx context.Context, nodeID roachpb.NodeID) error {
	return nl.storage.Create(ctx, nodeID)
}

func (nl *NodeLiveness) setMembershipStatusInternal(
	ctx context.Context, oldLivenessRec Record, targetStatus livenesspb.MembershipStatus,
) (statusChanged bool, err error) {
	if valid, err := livenesspb.ValidateTransition(oldLivenessRec.Liveness, targetStatus); !valid {
		return false, err
	}

	// Let's compute what our new liveness record should be. We start off with a
	// copy of our existing liveness record.
	newLiveness := oldLivenessRec.Liveness
	newLiveness.Membership = targetStatus

	update := LivenessUpdate{
		newLiveness: newLiveness,
		oldLiveness: oldLivenessRec.Liveness,
		oldRaw:      oldLivenessRec.raw,
	}
	statusChanged = true
	if _, err := nl.updateLiveness(ctx, update, func(actual Record) error {
		if actual.Membership != update.newLiveness.Membership {
			// We're racing with another attempt at updating the liveness
			// record, we error out in order to retry.
			return errChangeMembershipStatusFailed
		}
		// The found liveness membership status is the same as the target one,
		// so we consider our work done. We inform the caller that this attempt
		// was a no-op.
		statusChanged = false
		return nil
	}); err != nil {
		return false, err
	}

	return statusChanged, nil
}

// Start starts a periodic heartbeat to refresh this node's last
// heartbeat in the node liveness table. The optionally provided
// HeartbeatCallback will be invoked whenever this node updates its
// own liveness. The slice of engines will be written to before each
// heartbeat to avoid maintaining liveness in the presence of disk stalls.
// TODO(baptist): If we completely remove epoch leases, this can be merged with
// the NewNodeLiveness function. Currently the liveness is required prior to
// Start getting called in replica_range_lease. For non-epoch leases this should
// be possible.
// Start 函数启动一个后台常驻协程来执行周期性的心跳。
//
// 核心作用：
// 1. 宣告存活：定时刷新分布式 KV 中的过期时间，向集群证明节点在线。
// 2. 纪元管理 (Epoch)：在节点启动时增加 Epoch，用来废除该节点之前可能残留的租约记录。
// 3. 故障安全性：如果心跳因为磁盘卡死或网络彻底中断而停止，该节点的存活状态会由于没有被及时续期而过期，
//    从而允许集群安全地将数据租约（Lease）重定位到其他健康节点。
//
// 例子：
// - 节点启动 (Epoch=10)：
//   - 第一次打卡：调用 (Epoch: 10 -> 11, Expiration: 12:00:09)。成功后，该节点正式“上线”。
//   - 后续打卡：每 4.5s 执行一次。调用 (Epoch: 11, Expiration: 12:00:13.5)。只增加 Expiration，保证 Epoch 稳定。
// - 如果节点网络断了 15s：
//   - 其 Expiration (12:00:13.5) 会到期。集群判定该节点死亡，允许其他节点通过增加 Epoch (11 -> 12) 来接管该节点的 Range 租约。
func (nl *NodeLiveness) Start(ctx context.Context) {
	log.VEventf(ctx, 1, "starting node liveness instance")
	if nl.started.Load() {
		// 避免重复启动。
		log.KvExec.Fatal(ctx, "liveness already started")
	}

	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = nl.stopper.ShouldQuiesce()

	nl.started.Store(true)
	// 启动后台异步任务执行心跳循环。
	_ = nl.stopper.RunAsyncTaskEx(ctx, stop.TaskOpts{TaskName: "liveness-hb", SpanOpt: stop.SterileRootSpan}, func(context.Context) {
		ambient := nl.ambientCtx
		ambient.AddLogTag("liveness-hb", nil)
		ctx, cancel := nl.stopper.WithCancelOnQuiesce(context.Background())
		defer cancel()
		ctx, sp := ambient.AnnotateCtxWithSpan(ctx, "liveness heartbeat loop")
		defer sp.Finish()

		// 标志第一次心跳是否需要增加 epoch。节点刚启动时必须通过增加 epoch
		// 来确保让任何先前运行的实例所持有的旧租约失效并作废。
		incrementEpoch := true

		// 计算心跳尝试的频率。通常设为 (阈值 - 续期耗时上限)。
		heartbeatInterval := nl.livenessThreshold - nl.renewalDuration
		// 1. 启动周期性定时器。
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-nl.heartbeatToken: // 仅用于单元测试控制。
			case <-nl.stopper.ShouldQuiesce():
				return // 节点优雅退出。
			}
			// 2. 限制单次心跳操作的耗时，防止请求卡在网络层无法返回。
			if err := timeutil.RunWithTimeout(ctx, "node liveness heartbeat", nl.renewalDuration,
				func(ctx context.Context) error {
					nl.cache.checkForStaleEntries(gossip.StoreTTL)
					// 3. 心跳写入循环。
					for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
						// 3.a 优先从本地缓存获取自己的心跳状态。
						oldLiveness, ok := nl.Self()
						if !ok {
							// 如果缓存为空（初始时刻），则执行网络 IO 从全局 KV 中读取一次记录。
							nodeID := nl.cache.selfID()
							liveness, err := nl.getLivenessRecordFromKV(ctx, nodeID)
							if err != nil {
								log.KvExec.Infof(ctx, "unable to get liveness record from KV: %s", err)
								if grpcutil.IsConnectionRejected(err) {
									return err
								}
								continue
							}
							oldLiveness = liveness.Liveness
						}
						// 3.b 执行真正的条件更新。底层会验证 epoch。
						if err := nl.heartbeatInternal(ctx, oldLiveness, incrementEpoch); err != nil {
							// 如果因为 epoch 已被其他更年轻的节点非法推进了，则本节点需要重新同步状态。
							if errors.Is(err, ErrEpochIncremented) {
								log.KvExec.Infof(ctx, "%s; retrying", err)
								continue
							}
							return err
						}
						// 成功后，将增量 epoch 设置为 false，后续心跳只续期存活时间。
						incrementEpoch = false
						break
					}
					return nil
				}); err != nil {
				// 记录心跳失败日志。如果持续失败，该节点将被宣告死亡，丢失所有租约。
				log.KvExec.Warningf(ctx, heartbeatFailureLogFormat, err)
			} else if nl.onSelfHeartbeat != nil {
				// 4. 心跳成功后的扩展回调（如本地磁盘打卡）。
				nl.onSelfHeartbeat(ctx)
			}

			nl.heartbeatToken <- struct{}{}
			// 5. 等待下一个节拍。
			select {
			case <-ticker.C:
			case <-nl.stopper.ShouldQuiesce():
				return
			}
		}
	})
}

const heartbeatFailureLogFormat = `failed node liveness heartbeat: %v

An inability to maintain liveness will prevent a node from participating in a
cluster. If this problem persists, it may be a sign of resource starvation or
of network connectivity problems. For help troubleshooting, visit:

    https://www.cockroachlabs.com/docs/stable/cluster-setup-troubleshooting.html#node-liveness-issues

`

var errNodeAlreadyLive = errors.New("node already live")

// Heartbeat is called to update a node's expiration timestamp. This
// method does a conditional put on the node liveness record, and if
// successful, stores the updated liveness record in the nodes map.
//
// The liveness argument is the expected previous value of this node's
// liveness.
//
// If this method returns nil, the node's liveness has been extended,
// relative to the previous value. It may or may not still be alive
// when this method returns. It may also not have been extended as far
// as the livenessThreshold, because the caller may have raced with
// another heartbeater.
//
// On failure, this method returns ErrEpochIncremented, although this
// may not necessarily mean that the epoch was actually incremented.
// TODO(bdarnell): Fix error semantics here.
//
// This method is rarely called directly; heartbeats are normally sent
// by the Start loop.
// TODO(bdarnell): Should we just remove this synchronous heartbeat completely?
func (nl *NodeLiveness) Heartbeat(ctx context.Context, liveness livenesspb.Liveness) error {
	if buildutil.CrdbTestBuild && !nl.started.Load() {
		// This check was added as part of resolving #106706. We were previously
		// accidentally relying on synchronous heartbeats to paper over problems,
		// which only worked most of the time but could lead to hangs.
		// In our test builds, we only allow heartbeats of any kind once the
		// liveness loop has started.
		//
		// See: https://github.com/cockroachdb/cockroach/issues/106706#issuecomment-1640254715
		return errors.New("liveness heartbeat not started yet")
	}
	return nl.heartbeatInternal(ctx, liveness, false /* increment epoch */)
}

func (nl *NodeLiveness) callbacks() []IsLiveCallback {
	nl.onIsLiveMu.Lock()
	defer nl.onIsLiveMu.Unlock()
	return append([]IsLiveCallback(nil), nl.onIsLiveMu.callbacks...)
}

func (nl *NodeLiveness) notifyIsAliveCallbacks(fns []IsLiveCallback) {
	for _, entry := range nl.ScanNodeVitalityFromCache() {
		if entry.IsLive(livenesspb.IsAliveNotification) {
			for _, fn := range fns {
				fn(entry.GetInternalLiveness())
			}
		}
	}
}

// 总结来说，
//
// heartbeatInternal
// 是整个心跳机制将数据实际落地到数据库的核心步骤，也是节点向全集群其它节点“宣示存活”最后的一道门。其主要的执行步骤如下：
//
// 取号和防重复（惊群效应防御）：心跳可能因为网络、CPU抖动而大量重试触发。系统先通过 selfSem 这个单通道的锁机制让各种高并发触发的心跳串行化。并在入队列前后计算时间戳差异，如果发现有人跑在前面替自己续上约了，就直接提前成功返回，不再重复向数据库发包。
// 强制提升纪元 (Epoch Increment) 规则：日常续命是不改 Epoch 的（只延长过期时间）。如果外界告诉该节点“别人觉得你死了”（或者该节点刚重启发现它自己过期了），此节点就会开启“死而复生”模式，强行累加老周期的
//
// Epoch
// 从而进入新时代，这一举动能强制让之前旧纪元发起的所以读写老租约瞬间作废，防止脑裂（Split Brain）脏写问题。
// 计算出全新的过期时间
//
// Expiration
// ：它结合当前墙上时钟+给定的生命阈值常量，计算出一个未来的截止期限。
// 发起强一致更新（Conditional PutCAS 更新）：代码通过调用 nl.updateLiveness 将新旧两个状态传给底层的 KV 分布式引擎。它是一次附带前提条件的写入：“如果现在底层存的状态还是我这里保存的这个 oldLiveness 状态，就请把它更新成我现在的这个新状态；如果底层的数据不是我传的这坨 old 了，就抛错！”。
// 处理失败或成功的结果：
// 失败：如果是普通失败，但是检查底库发现在底层存活记录还是生效中，我们就当做心跳没发出去但也无能大碍（因为底层的过期时间依然在未来），转为成功返回；如果是
//
// Epoch
// 被硬串改了证明有别的大事发生，抛出严重错误 ErrEpochIncremented 让上游启动重走重新读取接管最新状态的流程。
// 成功：将新获得的 KV 回包更新到本地缓存里（调用我们前文提到的
//
// maybeUpdate
// ）。至此不仅外界所有节点认知到了你的最新存活时间，你自己本地也认可了这个时间，整个心跳流程完美闭环。
// heartbeatInternal 内部心跳的核心实现：它通过 CAS（Conditional Put）方式将节点的新过期时间更新到底层 KV
func (nl *NodeLiveness) heartbeatInternal(
	ctx context.Context, oldLiveness livenesspb.Liveness, incrementEpoch bool,
) (err error) {
	ctx, sp := tracing.EnsureChildSpan(ctx, nl.ambientCtx.Tracer, "liveness heartbeat")
	defer sp.Finish()
	defer func(start time.Time) {
		dur := timeutil.Since(start)
		// 记录每次心跳的延迟，用于监控报警
		nl.metrics.HeartbeatLatency.RecordValue(dur.Nanoseconds())
		if dur > time.Second {
			log.KvExec.Warningf(ctx, "slow heartbeat took %s; err=%v", dur, err)
		}
	}(timeutil.Now())

	// 1. 在入队等待锁之前，先抓取一次时钟，以此估算一个“期望的最短存活时间”。
	// 这样可以避免如果锁排队时间过长，刚发出去的心跳一瞬间就过期了。
	beforeQueueTS := nl.clock.Now()
	minExpiration := beforeQueueTS.Add(nl.livenessThreshold.Nanoseconds(), 0).ToLegacyTimestamp()

	// 标记有心跳正在运行中
	nl.metrics.HeartbeatsInFlight.Inc(1)
	defer nl.metrics.HeartbeatsInFlight.Dec(1)

	// 2. 限制本节点的心跳只能串行发生（同一时间内只能有一个在飞）
	sem := nl.selfSem
	select {
	case sem <- struct{}{}: // 获取令牌
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-sem // 释放令牌
	}()

	// 3. 冗余心跳拦截（惊群效应防御）
	// 如果这只是一个普通的续期(不改变纪元)，而且在排队等锁的这段时间里，
	// 其他更早发起的心跳已经成功把过期时间更新地足够远了，那这次心跳其实就没有必要再去调用底层系统了，直接视为成功即可。
	if !incrementEpoch {
		curLiveness, ok := nl.Self()
		if ok && minExpiration.Less(curLiveness.Expiration) {
			return nil
		}
	}

	if oldLiveness == (livenesspb.Liveness{}) {
		return errors.AssertionFailedf("invalid old liveness record; found to be empty")
	}

	// 4. 构建新的心跳包
	newLiveness := oldLiveness
	if incrementEpoch {
		// 如果在外界发现本节点已经被宣判死亡，或者节点刚启动接管，
		// 这时候心跳的性质变了，必须强行把纪元(Epoch)加1，这会使得其他所有基于老纪元的租约立刻失效。
		newLiveness.Epoch++
		newLiveness.Draining = false // 增加纪元时重置 Draining 状态
	}

	// 再次抓取当前时钟（因为上面等排队可能过了很久），得出实际的未来新过期时间（如 : 当前+9s）
	afterQueueTS := nl.clock.Now()
	newLiveness.Expiration = afterQueueTS.Add(nl.livenessThreshold.Nanoseconds(), 0).ToLegacyTimestamp()

	// 5. 校验：不能允许时间的倒流。如果机器时钟调错了，会导致拒绝心跳。
	if newLiveness.Expiration.Less(oldLiveness.Expiration) {
		return errors.Errorf("proposed liveness update expires earlier than previous record")
	}

	update := LivenessUpdate{
		oldLiveness: oldLiveness,
		newLiveness: newLiveness,
	}
	// 6. 核心动作：向底层的 CockroachDB 数据范围发起 Conditional Put
	// 这是保证所有节点对“谁活着”具备强一致认知的关键屏障
	written, err := nl.updateLiveness(ctx, update, func(actual Record) error {
		// 回调函数只有在更新失败时触发（即实际在底层的记录和预期的 oldLiveness 不一致）

		// 不管怎样，既然到底层查出了新数据，顺便就用这个新数据更新一下本地的缓存字典
		nl.cache.maybeUpdate(ctx, actual)

		// 特殊情况：如果当前节点心跳失败，但底层记录显示这节点依然“活着”（虽然记录的数值和预期的有偏差），
		// 而且我们不是在做强制的纪元增加，那其实我们想要维持节点存活的目的已经达到了，姑且假装心跳成功了。
		if actual.IsLive(nl.clock.Now()) && !incrementEpoch {
			return errNodeAlreadyLive
		}
		// 否则，这说明底层被人硬改了（纪元增加了）。这是一个严重的错误，会让上层进入死循环重试逻辑。
		return ErrEpochIncremented
	})

	// 7. 处理各种结果并记录指标
	if err != nil {
		if errors.Is(err, errNodeAlreadyLive) {
			// 因前面提到的“虽然失败但还是活着的”情况被包容，转为成功
			nl.metrics.HeartbeatSuccesses.Inc(1)
			return nil
		}
		nl.metrics.HeartbeatFailures.Inc()
		return err // 真正失败的错误抛出去（大概率是 ErrEpochIncremented）
	}

	log.VEventf(ctx, 1, "heartbeat %+v", written.Expiration)
	// 发送成功，更新本地缓存为这笔最新的记录，同时可能触发节点状态由死转生的回调
	nl.cache.maybeUpdate(ctx, written)
	nl.metrics.HeartbeatSuccesses.Inc(1)
	return nil
}

// Self returns the liveness record for this node. ErrMissingRecord
// is returned in the event that the node has neither heartbeat its
// liveness record successfully, nor received a gossip message containing
// a former liveness update on restart.
func (nl *NodeLiveness) Self() (_ livenesspb.Liveness, ok bool) {
	rec, ok := nl.cache.self()
	if !ok {
		return livenesspb.Liveness{}, false
	}
	return rec.Liveness, true
}

// GetIsLiveMap returns a map of nodeID to boolean liveness status of
// each node. This excludes nodes that were removed completely (dead +
// decommissioning).
// TODO(baptist): Remove.
func (nl *NodeLiveness) GetIsLiveMap() livenesspb.IsLiveMap {
	return nl.cache.getIsLiveMap()
}

// ScanNodeVitalityFromCache returns a map of nodeID to boolean liveness status
// of each node from the cache. This excludes nodes that were decommissioned.
// Decommissioned nodes are kept in the KV store and the cache forever, but are
// typically not referenced in normal usage. The method ScanNodeVitalityFromKV
// does return decommissioned nodes.
func (nl *NodeLiveness) ScanNodeVitalityFromCache() livenesspb.NodeVitalityMap {
	return nl.cache.ScanNodeVitalityFromCache()
}

// ScanNodeVitalityFromKV returns the status for all the nodes from KV including
// nodes that have been decommissioned. This method is typically used when the
// set of results must be accurate as of a point in time. since decisions can be
// made based on the values. Most code should call either
// ScanNodeVitalityFromCache or GetNodeVitalityFromCache.
func (nl *NodeLiveness) ScanNodeVitalityFromKV(
	ctx context.Context,
) (livenesspb.NodeVitalityMap, error) {
	records, err := nl.storage.Scan(ctx)
	if err != nil {
		return nil, err
	}

	statusMap := make(map[roachpb.NodeID]livenesspb.NodeVitality, len(records))
	for _, liveness := range records {
		vitality := nl.cache.convertToNodeVitality(liveness.Liveness)
		nl.cache.maybeUpdate(ctx, liveness)
		statusMap[liveness.NodeID] = vitality
	}
	return statusMap, nil
}

// GetNodeVitalityFromCache returns the current status of the node. This method
// is "time sensitive", so the result of calling it should not be cached. The
// liveness is calculated based at the time this method is called. The return
// NodeVitality records are "static" and calculated based on the HLC clock when
// this method is called. The results should not be cached externally as they
// may no longer be accurate in the future. See livenesspb.NodeVitality for
// using this method.
func (nl *NodeLiveness) GetNodeVitalityFromCache(nodeID roachpb.NodeID) livenesspb.NodeVitality {
	return nl.cache.GetNodeVitality(nodeID)
}

// GetLiveness returns the liveness record for the specified nodeID. If the
// liveness record is not found (due to gossip propagation delays or due to the
// node not existing), we surface that to the caller. The record returned also
// includes the raw, encoded value that the database has for this liveness
// record in addition to the decoded liveness proto.
// TODO(baptist): Remove.
func (nl *NodeLiveness) GetLiveness(nodeID roachpb.NodeID) (_ Record, ok bool) {
	return nl.cache.getLiveness(nodeID)
}

// getLivenessRecordFromKV fetches the liveness record from KV for a given node,
// and updates the internal in-memory cache when doing so. It returns a Record
// with the encoded value that the database has for this liveness record in
// addition to the decoded liveness proto. The Record is required for updates.
func (nl *NodeLiveness) getLivenessRecordFromKV(
	ctx context.Context, nodeID roachpb.NodeID,
) (Record, error) {
	livenessRec, err := nl.storage.Get(ctx, nodeID)
	if err == nil {
		// Update our cache with the liveness record we just found.
		nl.cache.maybeUpdate(ctx, livenessRec)
	}

	return livenessRec, err
}

// IncrementEpoch is called to attempt to revoke another node's
// current epoch, causing an expiration of all its leases. This method
// does a conditional put on the node liveness record, and if
// successful, stores the updated liveness record in the nodes map. If
// this method is called on a node ID which is considered live
// according to the most recent information gathered through gossip,
// an error is returned.
//
// The liveness argument is used as the expected value on the
// conditional put. If this method returns nil, there was a match and
// the epoch has been incremented. This means that the expiration time
// in the supplied liveness accurately reflects the time at which the
// epoch ended.
//
// If this method returns ErrEpochAlreadyIncremented, the epoch has
// already been incremented past the one in the liveness argument, but
// the conditional put did not find a match. This means that another
// node performed a successful IncrementEpoch, but we can't tell at
// what time the epoch actually ended. (Usually when multiple
// IncrementEpoch calls race, they're using the same expected value.
// But when there is a severe backlog, it's possible for one increment
// to get stuck in a queue long enough for the dead node to make
// another successful heartbeat, and a second increment to come in
// after that)
func (nl *NodeLiveness) IncrementEpoch(ctx context.Context, liveness livenesspb.Liveness) error {
	// Allow only one increment at a time.
	sem := nl.sem(liveness.NodeID)
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-sem
	}()

	if liveness.IsLive(nl.clock.Now()) {
		return errors.Errorf("cannot increment epoch on live node: %+v", liveness)
	}

	update := LivenessUpdate{
		newLiveness: liveness,
		oldLiveness: liveness,
	}
	update.newLiveness.Epoch++

	written, err := nl.updateLiveness(ctx, update, func(actual Record) error {
		nl.cache.maybeUpdate(ctx, actual)

		if actual.Epoch > liveness.Epoch {
			return ErrEpochAlreadyIncremented
		} else if actual.Epoch < liveness.Epoch {
			return errors.Errorf("unexpected liveness epoch %d; expected >= %d", actual.Epoch, liveness.Epoch)
		}
		return &ErrEpochCondFailed{
			expected: liveness,
			actual:   actual.Liveness,
		}
	})
	if err != nil {
		return err
	}

	log.KvExec.Infof(ctx, "incremented n%d liveness epoch to %d", written.NodeID, written.Epoch)
	nl.cache.maybeUpdate(ctx, written)
	nl.metrics.EpochIncrements.Inc()
	return nil
}

// Metrics returns a struct which contains metrics related to node
// liveness activity.
func (nl *NodeLiveness) Metrics() Metrics {
	return nl.metrics
}

// RegisterCallback registers a callback to be invoked any time a node's
// IsLive() state changes to true. The provided callback will be invoked
// synchronously from RegisterCallback if the node is currently live.
func (nl *NodeLiveness) RegisterCallback(cb IsLiveCallback) {
	nl.onIsLiveMu.Lock()
	nl.onIsLiveMu.callbacks = append(nl.onIsLiveMu.callbacks, cb)
	nl.onIsLiveMu.Unlock()

	nl.notifyIsAliveCallbacks([]IsLiveCallback{cb})
}

// updateLiveness 是对底层 KV 存储执行“条件更新（Conditional Put）”的入口函数。
// 它的职责是：在正式更新心跳前校验磁盘健康状态，并处理更新过程中可能出现的瞬态错误（如网络波动或不确定的结果）。
//
// 逻辑流程：
//  1. 校验磁盘健康 (verifyDiskHealth)：在续期心跳前，先尝试对所有存储引擎执行一次同步写操作。
//     如果磁盘 IO 卡死或报错，则本函数直接返回错误导致心跳续期失败。这是一种自我保护机制：
//     如果机器磁盘坏了，节点应该主动“自杀”（丢失存活状态和租约），让集群其它健康节点接管。
//  2. 容错重试 (Retry Loop)：使用 retry 策略包裹更新逻辑。
//  3. 执行单次尝试 (updateLivenessAttempt)：调用底层方法真正向 KV 系统发起包含 1PC 事务的 Conditional Put 请求。
//  4. 结果分发：如果遇到这类可以重试的错误（errRetryLiveness），则继续下一轮循环；
//     如果 CPut 因为记录不匹配失败，会触发 handleCondFailed 回调（通常由 heartbeatInternal 传入）。
func (nl *NodeLiveness) updateLiveness(
	ctx context.Context, update LivenessUpdate, handleCondFailed func(actual Record) error,
) (Record, error) {
	if err := nl.verifyDiskHealth(ctx); err != nil {
		return Record{}, err
	}
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = nl.stopper.ShouldQuiesce()
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		written, err := nl.updateLivenessAttempt(ctx, update, handleCondFailed)
		if err != nil {
			if errors.HasType(err, (*errRetryLiveness)(nil)) {
				log.KvExec.Infof(ctx, "retrying liveness update after %s", err)
				continue
			}
			return Record{}, err
		}
		return written, nil
	}
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	return Record{}, errors.New("retry loop ended without error - likely shutting down")
}

// verifyDiskHealth 的作用：这是 CockroachDB 的一种“自杀式”磁盘健康检查机制。
// 在节点进行心跳续期（Liveness Heartbeat）之前，它会强制要求对本地所有的存储盘执行一次同步写（Sync Write）操作。
//
// 核心逻辑：
//  1. 为什么要做这个？
//     在分布式系统中，如果一个机器的磁盘 IO 彻底卡死（Stalled），但 CPU 和网络还是通的，
//     如果没有这个检查，心跳依然能发出去（因为心跳只是一次网络请求），这会导致该节点占着租约（Lease）却无法读写磁盘。
//     通过这个函数，如果磁盘卡死超过了心跳阈值，续期就会失败。
//  2. 实现方式：
//     - 它遍历所有的引擎（nl.engines），对每一个引擎调用 diskStorage.WriteSyncNoop。
//     - 使用 singleflight 机制 (nl.engineSyncs.DoChan)：这意味着如果同时有多个地方想做磁盘检查（比如心跳跑得快），
//     它们会合并成一次真实的磁盘同步写，避免给正在报警的磁盘雪上加霜。
//     - 异步与超时处理：它会尊重传入的 ctx（包含心跳超时时间）。如果磁盘超过几秒没响应，WaitForResult(ctx) 就会报错。
//  3. 后果：
//     如果这个函数报错返回，updateliveness 就会失败，节点很快会因为无法续期而丢失 Liveness，
//     从而触发全集群范围内的租约迁移，让健康的节点接管这台挂掉的机器。
func (nl *NodeLiveness) verifyDiskHealth(ctx context.Context) error {
	// resultCs 存储每个存储引擎同步写的异步结果（Future）
	resultCs := make([]singleflight.Future, len(nl.engines))
	for i, eng := range nl.engines {
		// 使用 singleflight 串行化/合并并发的磁盘写校验请求
		resultCs[i], _ = nl.engineSyncs.DoChan(ctx,
			strconv.Itoa(i), // 每个引擎 ID 独立一个 singleflight group
			singleflight.DoOpts{
				Stop:               nl.stopper,
				InheritCancelation: false,
			},
			func(ctx context.Context) (interface{}, error) {
				// 执行一次实际的同步写测试（不带数据的同步操作，类似于 fsync 一个文件）
				return nil, diskStorage.WriteSyncNoop(eng)
			})
	}
	// 等待所有磁盘的校验结果返回
	for _, resultC := range resultCs {
		r := resultC.WaitForResult(ctx)
		if r.Err != nil {
			// 只要有一个磁盘出问题，就认为当前节点磁盘健康状态异常
			return errors.Wrapf(r.Err, "disk write failed while updating node liveness")
		}
	}
	return nil
}

// updateLivenessAttempt 是 updateLiveness 的具体执行单次尝试的函数。
// 它的主要职责是确保在发起底层的事务性写入前，我们拥有正确的“旧状态”数据，
// 并作为中间层触发逻辑冲突的回调。
//
// 逻辑说明：
//  1. 检查 oldRaw（旧状态的原始二进制数据）：
//     Conditional Put 需要知道数据库里“原本应该长什么样”。如果调用者没提供，
//     它会尝试从本地 cache 中读取当前缓存的记录。
//  2. 缓存一致性预检：
//     如果本地 cache 里的记录（l.Liveness）已经和调用者预期的“旧记录”（update.oldLiveness）不一样了，
//     说明在发起网络请求前，我们就已经知道这次 CPut 肯定会失败。
//     这时候直接在本地调用 handleCondFailed(l) 回调处理冲突，而不必再浪费一次网络 IO。
//  3. 执行真实写入：
//     调用 nl.storage.Update，将请求真正发送到 KV 存储层（liveness 范围所在的 Leaseholder 节点）。
func (nl *NodeLiveness) updateLivenessAttempt(
	ctx context.Context, update LivenessUpdate, handleCondFailed func(actual Record) error,
) (Record, error) {
	// 如果调用者没有手动提供 update.oldRaw 中的先前值，我们需要从缓存中读取它。
	if update.oldRaw == nil {
		l, ok := nl.cache.getLiveness(update.newLiveness.NodeID)
		if !ok {
			// 如果缓存里完全没找到这个节点的记录，说明状态有问题。
			return Record{}, ErrRecordCacheMiss
		}
		// 如果缓存的记录与调用者期望用来做对比的旧记录不一致，
		// 说明 CPut 必定失败，直接调用 handleCondFailed 并返回。
		if l.Liveness != update.oldLiveness {
			return Record{}, handleCondFailed(l)
		}
		// 填充原始二进制数据，用于底层的字节级别对比。
		update.oldRaw = l.raw
	}
	// 调用存储接口，发起真实的分布式写入。
	//由于 Liveness 记录存储在系统的预留 Range（通常是第一个 Range）里，
	//这个 Range 是有多个副本的（默认 3 个或 5 个）。
	//因此，这笔心跳写入必须通过 Raft 协议 复制到该 Range 的多数派成员上并持久化到它们的物理存储（Pebble）中，
	//才算心跳成功。
	return nl.storage.Update(ctx, update, handleCondFailed)
}

// numLiveNodes is used to populate a metric that tracks the number of live
// nodes in the cluster. Returns 0 if this node is not itself live, to avoid
// reporting potentially inaccurate data.
// We export this metric from every live node rather than a single particular
// live node because liveness information is gossiped and thus may be stale.
// That staleness could result in no nodes reporting the metric or multiple
// nodes reporting the metric, so it's simplest to just have all live nodes
// report it.
func (nl *NodeLiveness) numLiveNodes() int64 {

	selfID := nl.cache.selfID()
	// if our node id isn't set, don't return a count
	if selfID == 0 {
		return 0
	}

	var liveNodes int64
	for n, v := range nl.ScanNodeVitalityFromCache() {
		if v.IsLive(livenesspb.IsAliveNotification) {
			liveNodes++
		}
		// If this node isn't live, we don't want to report its view of node liveness
		// because it's more likely to be inaccurate than the view of a live node.
		if n == selfID && !v.IsLive(livenesspb.IsAliveNotification) {
			return 0
		}
	}
	return liveNodes
}
