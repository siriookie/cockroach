// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package sidetransport contains definitions for the sidetransport layer of
// the kvserver.
package sidetransport

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/policyrefresher"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/taskpacer"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
)

// Sender represents the sending-side of the closed timestamps "side-transport".
// Its role is to periodically advance the closed timestamps of all the ranges
// with leases on the current node and to communicate these closed timestamps to
// all other nodes that have replicas for any of these ranges.
//
// This side-transport is particularly important for range that are not seeing
// frequent writes; in the absence of Raft proposals, this is the only way for
// the closed timestamps to advance.
//
// The Sender is notified when leases are acquired or lost by the current node.
// The sender periodically loops through all the ranges with local leases, tries
// to advance the closed timestamp of each range according to its policy, and
// then publishes a message with the update to all other nodes with
// non-leaseholder replicas. Every node receives the same messages; for
// efficiency the sender does not keep track of which follower node is
// interested in which ranges. On the receiver side the closed timestamp updates
// are processed lazily, so it doesn't particularly matter that each receiver is
// told about ranges that it doesn't care about.
type Sender struct {
	stopper *stop.Stopper
	st      *cluster.Settings
	clock   *hlc.Clock
	nodeID  roachpb.NodeID
	// connFactory is used to establish new connections.
	connFactory connFactory

	trackedMu struct {
		syncutil.Mutex
		streamState
		// closingFailures buckets the failures to advance the closed timestamps of
		// ranges for the last publishing cycle.
		closingFailures [MaxReason]int
	}

	leaseholdersMu struct {
		syncutil.Mutex
		leaseholders map[roachpb.RangeID]leaseholder
	}

	// buf contains recent messages published to connections. Adding a message
	// to this buffer signals the connections to send it on their streams.
	buf *updatesBuf

	// conns contains connections to all nodes with follower replicas of any of
	// the registered leaseholder. connections are added as nodes get replicas for
	// ranges with local leases and removed when the respective node no longer has
	// any replicas with local leases. A conn persists in this map across
	// underlying network connects and disconnects. As long as it's in the map, it
	// will continuously try to reconnect.
	connsMu struct {
		syncutil.Mutex
		conns map[roachpb.NodeID]conn
	}
}

// streamState encapsulates the state that's tracked by a stream. Both the
// Sender and the Receiver use this struct and, for a given stream, both ends
// are supposed to correspond (modulo message delays), in wonderful symmetry.
type streamState struct {
	// lastSeqNum is the sequence number of the last message published.
	lastSeqNum ctpb.SeqNum
	// lastClosed is the closed timestamp published for each policy in the last
	// message. During mixed version clusters with nodes running both v25.1 and
	// v25.2, lastClosed will only be populated for enums with a corresponding
	// roachpb.RangeClosedTimestampPolicy (LAG_BY_CLUSTER_SETTING and
	// LEAD_FOR_GLOBAL_READS) until the cluster is fully upgraded to v25.2.
	lastClosed map[ctpb.RangeClosedTimestampPolicy]hlc.Timestamp
	// tracked maintains the information that was communicated to connections in
	// the last sent message (implicitly or explicitly). A range enters this
	// structure as soon as it's included in a message, and exits it when it's
	// removed through Update.Removed.
	tracked map[roachpb.RangeID]trackedRange
}

type connTestingKnobs struct {
	beforeSend         func(destNodeID roachpb.NodeID, msg *ctpb.Update)
	sleepOnErrOverride time.Duration
}

// pacerConfig implements taskpacer.Config using cluster settings.
type pacerConfig struct {
	st *cluster.Settings
}

// GetRefresh implements the taskpacer.Config interface
func (c *pacerConfig) GetRefresh() time.Duration {
	return closedts.SideTransportPacingRefreshInterval.Get(&c.st.SV)
}

// GetSmear implements the taskpacer.Config interface
func (c *pacerConfig) GetSmear() time.Duration {
	return closedts.SideTransportPacingSmearInterval.Get(&c.st.SV)
}

// trackedRange contains the information that the side-transport last published
// about a particular range.
type trackedRange struct {
	lai    kvpb.LeaseAppliedIndex
	policy ctpb.RangeClosedTimestampPolicy
}

// leaseholder represents a leaseholder replicas that has been registered with
// the sender and can send closed timestamp updates through the side transport.
type leaseholder struct {
	Replica
	leaseSeq roachpb.LeaseSequence
}

// Replica represents a *Replica object, but with only the capabilities needed
// by the closed timestamp side transport to accomplish its job.
type Replica interface {
	policyrefresher.Replica
	// Accessors.
	StoreID() roachpb.StoreID
	GetRangeID() roachpb.RangeID

	// BumpSideTransportClosed advances the range's closed timestamp if it can.
	// If the closed timestamp is advanced, the function synchronizes with
	// incoming requests, making sure that future requests are not allowed to
	// write below the new closed timestamp.
	//
	// Returns false is the desired timestamp could not be closed. This can
	// happen if the lease is no longer valid, if the range has proposals
	// in-flight, if there are requests evaluating above the desired closed
	// timestamp, or if the range has already closed a higher timestamp.
	//
	// If the closed timestamp was advanced, the function returns a LAI to be
	// attached to the newly closed timestamp.
	//
	// The desired closed timestamp is passed as a map from range policy to
	// timestamp; this function looks up the entry for this range.
	BumpSideTransportClosed(
		ctx context.Context,
		now hlc.ClockTimestamp,
		targetByPolicy map[ctpb.RangeClosedTimestampPolicy]hlc.Timestamp,
	) BumpSideTransportClosedResult
}

// BumpSideTransportClosedResult represents the retval of BumpSideTransportClosed.
type BumpSideTransportClosedResult struct {
	// OK is set if the desired timestamp can be closed. If not set, FailReason is
	// set.
	OK         bool
	FailReason CantCloseReason

	// Desc is set regardless of OK.
	Desc *roachpb.RangeDescriptor

	// Fields only set when ok.

	// The range's current LAI, to be associated with the closed timestamp.
	LAI kvpb.LeaseAppliedIndex
	// The range's current policy.
	Policy ctpb.RangeClosedTimestampPolicy
}

// CantCloseReason enumerates the reasons why BunpSideTransportClosed might fail
// to close a timestamp.
type CantCloseReason int

//go:generate stringer -type=CantCloseReason

// Reasons for failing to close a timestamp.
const (
	ReasonUnknown CantCloseReason = iota
	ReplicaDestroyed
	InvalidLease
	TargetOverLeaseExpiration
	MergeInProgress
	ProposalsInFlight
	RequestsEvaluatingBelowTarget
	MaxReason
)

// NewSender creates a Sender. Run must be called on it afterwards to get it to
// start publishing closed timestamps.
func NewSender(
	stopper *stop.Stopper, st *cluster.Settings, clock *hlc.Clock, dialer *nodedialer.Dialer,
) *Sender {
	return newSenderWithConnFactory(stopper, st, clock, newRPCConnFactory(dialer, connTestingKnobs{}))
}

func newSenderWithConnFactory(
	stopper *stop.Stopper, st *cluster.Settings, clock *hlc.Clock, connFactory connFactory,
) *Sender {
	s := &Sender{
		stopper:     stopper,
		st:          st,
		clock:       clock,
		connFactory: connFactory,
		buf:         newUpdatesBuf(st),
	}
	s.trackedMu.tracked = make(map[roachpb.RangeID]trackedRange)
	s.trackedMu.lastClosed = make(map[ctpb.RangeClosedTimestampPolicy]hlc.Timestamp)
	s.leaseholdersMu.leaseholders = make(map[roachpb.RangeID]leaseholder)
	s.connsMu.conns = make(map[roachpb.NodeID]conn)
	return s
}

// Run starts a goroutine that periodically closes new timestamps for all the
// ranges where the leaseholder is on this node.
//
// nodeID is the id of the local node. Used to avoid connecting to ourselves.
// This is not know at construction time.
func (s *Sender) Run(ctx context.Context, nodeID roachpb.NodeID) {
	// 记录本节点的 NodeID；后续建立 closed timestamp side transport 连接时会用它避免连接自己。
	s.nodeID = nodeID
	// confCh 用来接收 SideTransportCloseInterval 配置变更通知，容量为 1 表示只需要记住“发生过变更”。
	confCh := make(chan struct{}, 1)
	// confChanged 是 cluster setting 变更回调；它只负责唤醒后台循环重新读取 interval。
	confChanged := func(ctx context.Context) {
		// 非阻塞发送，避免配置频繁变化时回调卡住；如果 channel 已经有通知，说明后台循环迟早会醒来。
		select {
		case confCh <- struct{}{}:
		default:
		}
	}
	// 当 closed timestamp side transport 的关闭时间推进周期发生变化时，触发 confChanged。
	closedts.SideTransportCloseInterval.SetOnChange(&s.st.SV, confChanged)

	// Set up callback for pacing refresh interval changes.
	// pacingConfChanged 是 pacing 配置变更回调；pacing 控制发送更新时如何分批/节流。
	pacingConfChanged := func(ctx context.Context) {
		// 重新根据最新 settings 更新 buffer 内部的 pacer。
		s.buf.updatePacer(s.st)
	}
	// 当 side transport pacing refresh interval 变化时，刷新发送缓冲区的 pacer。
	closedts.SideTransportPacingRefreshInterval.SetOnChange(&s.st.SV, pacingConfChanged)

	// 启动 closed timestamp side transport 的 publisher 后台任务。
	_ /* err */ = s.stopper.RunAsyncTask(ctx, "closedts side-transport publisher",
		func(ctx context.Context) {
			// 后台任务退出时关闭 buffer；buffer 关闭会通知所有 side transport 连接退出。
			defer func() {
				// Closing the buffer signals all connections to quit.
				s.buf.Close()
			}()

			// timer 控制 publish 的周期。
			var timer timeutil.Timer
			// 退出 goroutine 时停止 timer，释放 timer 资源。
			defer timer.Stop()
			// 周期性推进并发布本节点 leaseholder ranges 的 closed timestamp。
			for {
				// 每轮都重新读取配置，确保 setting 变化后能生效。
				interval := closedts.SideTransportCloseInterval.Get(&s.st.SV)
				// interval > 0 表示启用 side transport publisher。
				if interval > 0 {
					// 设置下一次 publish 的等待时间。
					timer.Reset(interval)
				} else {
					// Disable the side-transport.
					// interval <= 0 表示禁用 side transport，停止 timer，等待配置变更或 server 退出。
					timer.Stop()
				}
				// 等待 timer 到期、配置变化，或者 server 开始关闭。
				select {
				case <-timer.C:
					// 到达周期点：收集 closed timestamp 更新并发布给其它节点。
					s.publish(ctx)
				case <-confCh:
					// Loop around to use the updated timer.
					// 配置变化：回到循环开头重新读取 interval 并重置 timer。
					continue
				case <-s.stopper.ShouldQuiesce():
					// server 正在关闭：退出后台任务，defer 会关闭 buffer 并让连接退出。
					return
				}
			}
		})
}

// RegisterLeaseholder adds a replica to the leaseholders collection. From now
// on, until the replica is unregistered, the side-transport will try to advance
// this replica's closed timestamp.
func (s *Sender) RegisterLeaseholder(
	ctx context.Context, r Replica, leaseSeq roachpb.LeaseSequence,
) {
	s.leaseholdersMu.Lock()
	defer s.leaseholdersMu.Unlock()

	if lh, ok := s.leaseholdersMu.leaseholders[r.GetRangeID()]; ok {
		// The leaseholder is already registered. If we're already aware of this
		// or a newer lease, there's nothing to do.
		if lh.leaseSeq >= leaseSeq {
			return
		}
		// Otherwise, update the leaseholder, which may be different object if
		// the lease moved between replicas for the same range on the same node
		// but on different stores.
	}
	s.leaseholdersMu.leaseholders[r.GetRangeID()] = leaseholder{
		Replica:  r,
		leaseSeq: leaseSeq,
	}
}

// UnregisterLeaseholder removes a replica from the leaseholders collection, if
// the replica is currently tracked.
func (s *Sender) UnregisterLeaseholder(
	ctx context.Context, storeID roachpb.StoreID, rangeID roachpb.RangeID,
) {
	s.leaseholdersMu.Lock()
	defer s.leaseholdersMu.Unlock()

	if lh, ok := s.leaseholdersMu.leaseholders[rangeID]; ok && lh.StoreID() == storeID {
		delete(s.leaseholdersMu.leaseholders, rangeID)
	}
}

// maxClosedTimestampPolicy returns the distinct number of closed timestamp
// policies for which the side transport should send updates.
func (s *Sender) maxClosedTimestampPolicy() int {
	if s.st.Version.IsActive(context.TODO(), clusterversion.V25_2) {
		return int(ctpb.MAX_CLOSED_TIMESTAMP_POLICY)
	}
	// If the cluster is not fully upgraded, only look at closed timestamps
	// policies without considering locality info.
	return int(roachpb.MAX_CLOSED_TIMESTAMP_POLICY)
}

func (s *Sender) publish(ctx context.Context) hlc.ClockTimestamp {
	// trackedMu 保护 Sender 已经通过 side transport 跟踪的 range 状态。
	s.trackedMu.Lock()
	// publish 会读写 tracked map、lastClosed、lastSeqNum 等状态，结束时统一释放锁。
	defer s.trackedMu.Unlock()
	// 打 verbose 日志，表示本轮开始生成一条新的 closed timestamp side transport 消息。
	log.VEventf(ctx, 4, "side-transport generating a new message")
	// 重置本轮推进 closed timestamp 失败原因的统计。
	s.trackedMu.closingFailures = [MaxReason]int{}
	// TODO(wenyihu6): a cluster version check is needed here once we add new
	// policies to ctpb.RangeClosedTimestampPolicies that do not have a
	// corresponding roachpb.RangeClosedTimestampPolicy.
	// 计算当前集群版本下需要发布多少种 closed timestamp policy。
	numPolicies := s.maxClosedTimestampPolicy()
	// 为本轮每种 policy 准备一个目标 closed timestamp 记录。
	s.trackedMu.lastClosed = make(map[ctpb.RangeClosedTimestampPolicy]hlc.Timestamp, numPolicies)
	// 构造要推送给 follower 节点的 update 消息。
	msg := &ctpb.Update{
		// 标记这条消息来自哪个节点。
		NodeID: s.nodeID,
		// 每种 policy 都有一组独立的 closed timestamp 更新。
		ClosedTimestamps: make([]ctpb.Update_GroupUpdate, numPolicies),
	}

	// Determine the message's sequence number.
	// 递增发送序号；receiver 用它判断消息顺序以及是否需要快照。
	s.trackedMu.lastSeqNum++
	// 把本轮序号写入消息。
	msg.SeqNum = s.trackedMu.lastSeqNum
	// The first message produced is essentially a snapshot, since it has no
	// previous state to reference.
	// 第一条消息没有历史状态可依赖，所以标记为 snapshot，让 receiver 按全量状态处理。
	msg.Snapshot = msg.SeqNum == 1

	// Fix the closed timestamps that will be communicated to by this message.
	// These timestamps (one per range policy) will apply to all the ranges
	// included in message.
	// 读取当前 HLC 时间，作为计算本轮目标 closed timestamp 的基准。
	now := s.clock.NowAsClockTimestamp()
	// 读取最大时钟偏移；跨节点 closed timestamp 计算需要考虑时钟不确定性。
	maxClockOffset := s.clock.MaxOffset()
	// 读取目标滞后时长，通常表示 closed timestamp 应该落后当前时间多少。
	lagTargetDuration := closedts.TargetDuration.Get(&s.st.SV)
	// 读取 global reads 的 lead override，用于对应 policy 下的目标时间计算。
	leadTargetOverride := closedts.LeadForGlobalReadsOverride.Get(&s.st.SV)
	// 读取 side transport 推进周期，用于计算目标时间。
	sideTransportCloseInterval := closedts.SideTransportCloseInterval.Get(&s.st.SV)
	// 读取 pacing 刷新周期，用于计算带 pacing 的目标时间。
	sideTransportPacingInterval := closedts.SideTransportPacingRefreshInterval.Get(&s.st.SV)
	// 为每种 closed timestamp policy 分别计算本轮想要关闭到的时间戳。
	for pol := ctpb.RangeClosedTimestampPolicy(0); pol < ctpb.RangeClosedTimestampPolicy(numPolicies); pol++ {
		// 根据当前时间、时钟偏移、配置和 policy 算出目标 closed timestamp。
		target := closedts.TargetForPolicy(
			now,
			maxClockOffset,
			lagTargetDuration,
			leadTargetOverride,
			sideTransportCloseInterval,
			sideTransportPacingInterval,
			pol,
		)
		// 记录本轮该 policy 的目标时间，后续 bump 每个 range 时会使用。
		s.trackedMu.lastClosed[pol] = target
		// 把该 policy 的目标 closed timestamp 写入消息体。
		msg.ClosedTimestamps[pol] = ctpb.Update_GroupUpdate{
			// 当前 group 对应的 policy。
			Policy: pol,
			// 该 policy 下本轮发布的 closed timestamp。
			ClosedTimestamp: target,
		}
	}

	// Make a copy of the leaseholders map, in order to release its mutex
	// quickly. We can't hold this mutex while calling into any replicas (and
	// locking the replica) because replicas call into the Sender and take
	// leaseholdersMu through Register/UnregisterLeaseholder.
	// 拷贝 leaseholders map 前先加锁，避免 Register/UnregisterLeaseholder 并发修改。
	s.leaseholdersMu.Lock()
	// 创建一份当前 leaseholder 集合的快照，后续遍历这份副本。
	leaseholders := make(map[roachpb.RangeID]leaseholder, len(s.leaseholdersMu.leaseholders))
	// 拷贝本节点当前持有 lease 的所有 ranges。
	for k, v := range s.leaseholdersMu.leaseholders {
		leaseholders[k] = v
	}
	// 快照拷贝完成后立即释放锁，避免后面调用 Replica 方法时形成锁顺序问题。
	s.leaseholdersMu.Unlock()

	// We'll accumulate all the nodes we need to connect to in order to check if
	// we need to open new connections or close existing ones.
	// 记录本轮发现的 follower 所在节点；后面根据它维护 side transport 连接集合。
	nodesWithFollowers := intsets.MakeFast()

	// If there's any tracked ranges for which we're not the leaseholder any more,
	// we need to untrack them and tell the connections about it.
	// 遍历之前已经通过 side transport 跟踪的 ranges。
	for rid := range s.trackedMu.tracked {
		// 如果本节点已经不再是这个 range 的 leaseholder，需要通知 receiver 移除它。
		if _, ok := leaseholders[rid]; !ok {
			// 把 rangeID 加进 Removed，receiver 收到后会停止按本 Sender 的消息更新该 range。
			msg.Removed = append(msg.Removed, rid)
			// 从本地 tracked 集合删除，后续不再认为这个 range 由本节点负责发布。
			delete(s.trackedMu.tracked, rid)
		}
	}

	// Iterate through each leaseholder and determine whether it can be part of
	// this update or not.
	// 遍历本节点当前作为 leaseholder 的所有 ranges，尝试推进它们的 side transport closed timestamp。
	for _, lh := range leaseholders {
		// 当前 leaseholder replica 对应的 rangeID。
		lhRangeID := lh.GetRangeID()
		// 查看这个 range 上一轮是否已经被 side transport 跟踪，以及上次发布的 LAI/policy。
		lastMsg, tracked := s.trackedMu.tracked[lhRangeID]

		// Check whether the desired timestamp can be closed on this range.
		// 尝试把当前 range 的 side transport closed timestamp 推进到本轮目标时间。
		// 这里会检查 lease、LAI、proposal 状态等条件，只有安全时才会返回 OK。
		closeRes := lh.BumpSideTransportClosed(ctx, now, s.trackedMu.lastClosed)

		// Ensure that we're communicating with all of the range's followers. Note
		// that we're including this range's followers before deciding below if the
		// current message will include this range; we don't want dynamic conditions
		// about the activity of this range to dictate the opening and closing of
		// connections to the other nodes.
		// 从 range descriptor 中拿到所有 replica 描述。
		repls := closeRes.Desc.Replicas().Descriptors()
		// 遍历所有 replica，找出 follower 所在节点。
		for i := range repls {
			// We want to track all followers including ones running on the same node
			// but different store. We want to bump side transport to update followers
			// even if two replicas are colocated on the same node during rebalancing.
			// 排除当前 leaseholder 所在 store；其它 store 上的 replica 都需要接收 closed timestamp。
			if repls[i].StoreID != lh.StoreID() {
				// 记录 follower 所在 node，后面会确保到这些 node 的连接存在。
				nodesWithFollowers.Add(int(repls[i].NodeID))
			}
		}

		// 如果这个 range 当前不能安全关闭到目标时间，需要记录失败并可能取消跟踪。
		if !closeRes.OK {
			// 统计失败原因，供 debug 页面或指标观察。
			s.trackedMu.closingFailures[closeRes.FailReason]++
			// We can't close the desired timestamp. If this range was tracked, we
			// need to un-track it.
			// 如果之前已经跟踪过该 range，需要通知 receiver 移除，避免 receiver 继续用旧状态隐式推进。
			if tracked {
				// 告诉 receiver 这个 range 本轮开始不再由隐式更新维护。
				msg.Removed = append(msg.Removed, lhRangeID)
				// 从本地 tracked 集合移除。
				delete(s.trackedMu.tracked, lhRangeID)
			}
			// 当前 range 处理完毕，继续看下一个 leaseholder range。
			continue
		}

		// Check whether the range needs to be explicitly updated through the
		// current message, or if its update can be implicit.
		// needExplicit 表示本轮消息是否必须显式携带这个 range 的 LAI/policy。
		needExplicit := false
		// 如果这个 range 之前没有被跟踪，需要显式加入，receiver 才知道从现在开始可以跟踪它。
		if !tracked {
			// If the range was not included in the last message, we need to include
			// it now to start "tracking" it in the side-transport.
			needExplicit = true
			// 如果 LAI 变大，说明 range 有新的 lease-applied index，需要显式发布给 receiver。
		} else if lastMsg.lai < closeRes.LAI {
			// If the range's LAI has changed, we need to explicitly publish the new
			// LAI.
			needExplicit = true
			// 如果 closed timestamp policy 变了，也需要显式发布，让 receiver 更新 bookkeeping。
		} else if lastMsg.policy != closeRes.Policy {
			// If the policy changed, we need to explicitly publish that; the
			// receiver will updates its bookkeeping to indicate that this range is
			// updated through implicit updates for the new policy.
			needExplicit = true
		}
		// 如果需要显式更新，就把 range 的 LAI/policy 放入 AddedOrUpdated。
		if needExplicit {
			msg.AddedOrUpdated = append(msg.AddedOrUpdated, ctpb.Update_RangeUpdate{
				// 被新增或更新的 rangeID。
				RangeID: lhRangeID,
				// 当前 range 可以安全发布到 follower 的 lease-applied index。
				LAI: closeRes.LAI,
				// 当前 range 使用的 closed timestamp policy。
				Policy: closeRes.Policy,
			})
			// 更新本地 tracked 状态；后续如果 LAI/policy 不变，可以只靠 group closed timestamp 隐式推进。
			s.trackedMu.tracked[lhRangeID] = trackedRange{lai: closeRes.LAI, policy: closeRes.Policy}
		}
	}

	// Close connections to the nodes that no longer need any info from us
	// (because they don't have replicas for any of the ranges with leases on this
	// node).
	{
		// 加锁保护 side transport 连接集合。
		s.connsMu.Lock()
		// 遍历当前已有的连接。
		for nodeID, c := range s.connsMu.conns {
			// 如果这个 node 已经没有任何 follower 需要接收本节点的 closed timestamp，关闭连接。
			if !nodesWithFollowers.Contains(int(nodeID)) {
				// 从连接表删除。
				delete(s.connsMu.conns, nodeID)
				// 关闭连接，停止继续向该 node 发送 update。
				c.close()
			}
		}

		// Open connections to any node that needs info from us and is missing a conn.
		// 遍历本轮需要接收 closed timestamp 的 follower 节点。
		nodesWithFollowers.ForEach(func(nid int) {
			// int 转回 NodeID。
			nodeID := roachpb.NodeID(nid)
			// We don't need to update leaseholders because timestamps we are closing
			// are written directly to the sideTransportClosedTimestamp fields of the
			// local replicas in BumpSideTransportClosed.
			// At the same time we can have followers colocated on the same node in
			// different stores (e.g. during store rebalancing) so we can create
			// connection to ourselves if we find such replicas.
			// 如果到这个 node 的连接还不存在，就创建并启动一条连接。
			if _, ok := s.connsMu.conns[nodeID]; !ok {
				// 通过 connFactory 创建连接对象；真实实现通常是 RPC stream。
				c := s.connFactory.new(s, nodeID)
				// 启动连接后台任务，它会从 s.buf 读取 update 并发给目标 node。
				c.run(ctx, s.stopper)
				// 保存连接，下一轮 publish 可以复用。
				s.connsMu.conns[nodeID] = c
			}
		})
		// 连接集合维护完毕，释放锁。
		s.connsMu.Unlock()
	}

	// Publish the new message to all connections.
	// 记录本轮发布的 closed timestamp 消息内容。
	log.VEventf(ctx, 4, "side-transport publishing message with closed timestamps: %v (%v)", msg.ClosedTimestamps, msg)
	// 把消息推入共享 buffer；所有 connection 会从 buffer 看到这条消息并发送给对应 follower node。
	s.buf.Push(ctx, msg)

	// Return the publication time, for tests.
	// 返回本轮 publish 使用的 now，主要给测试断言使用。
	return now
}

// GetLeaseholders returns a slice of all replicas that are currently
// leaseholders on this node.
func (s *Sender) GetLeaseholders() []policyrefresher.Replica {
	s.leaseholdersMu.Lock()
	defer s.leaseholdersMu.Unlock()
	leaseholders := make([]policyrefresher.Replica, 0, len(s.leaseholdersMu.leaseholders))
	for _, lh := range s.leaseholdersMu.leaseholders {
		leaseholders = append(leaseholders, lh.Replica)
	}
	return leaseholders
}

// GetSnapshot generates an update that contains all the sender's state (as
// opposed to being an incremental delta since a previous message). The returned
// msg will have the `snapshot` field set, and a sequence number indicating
// where to resume sending incremental updates.
func (s *Sender) GetSnapshot() *ctpb.Update {
	s.trackedMu.Lock()
	defer s.trackedMu.Unlock()

	msg := &ctpb.Update{
		NodeID: s.nodeID,
		// Assigning this SeqNum means that the next incremental sent needs to be
		// lastSeqNum+1. Notice that GetSnapshot synchronizes with the publishing of
		// of incremental messages.
		SeqNum:           s.trackedMu.lastSeqNum,
		Snapshot:         true,
		ClosedTimestamps: make([]ctpb.Update_GroupUpdate, 0, len(s.trackedMu.lastClosed)),
		AddedOrUpdated:   make([]ctpb.Update_RangeUpdate, 0, len(s.trackedMu.tracked)),
	}
	for pol, ts := range s.trackedMu.lastClosed {
		msg.ClosedTimestamps = append(msg.ClosedTimestamps, ctpb.Update_GroupUpdate{
			Policy:          pol,
			ClosedTimestamp: ts,
		})
	}
	for rid, r := range s.trackedMu.tracked {
		msg.AddedOrUpdated = append(msg.AddedOrUpdated, ctpb.Update_RangeUpdate{
			RangeID: rid,
			LAI:     r.lai,
			Policy:  r.policy,
		})
	}
	return msg
}

// updatesBuf is a circular buffer of Updates. It's created with a given
// capacity and, once it fills up, new items overwrite the oldest ones. It lets
// consumers query for the update with a particular sequence number and it lets
// queries block until the next update is produced.
type updatesBuf struct {
	mu struct {
		syncutil.Mutex

		// pacer controls the timing of broadcast updates to avoid overloading the
		// system.
		pacer *taskpacer.Pacer

		// We use two condition variables that we atomically swap to avoid signaling
		// the same goroutine multiple times. Without this, a goroutine could:
		// 1. Wake up from a signal.
		// 2. Process its message.
		// 3. Start waiting again on the same condition variable.
		// 4. Get signaled again before other waiting goroutines get a chance.
		//
		// By swapping to a new condition variable when publishing a message, we
		// ensure that goroutines can only wait on the new one. This lets us safely
		// signal all waiters on the old condition variable without racing against
		// goroutines re-queuing on it.
		updated1    sync.Cond
		numWaiters1 int

		// updated2 is signaled when a new item is inserted.
		updated2    sync.Cond
		numWaiters2 int

		// activeCondVar is 0 or 1, indicating which conditional variable new
		// goroutines should be waiting on. This is flipped after each push.
		activeCondVar int

		// data contains pointers to the Updates.
		data []*ctpb.Update
		// head points to the earliest update in the buffer. If the buffer is empty,
		// head is 0 and the respective slot is nil.
		//
		// tail points to the next slot to be written to. When the buffer is full,
		// tail == head meaning that the head will be overwritten by the next
		// insertion.
		head, tail int
		// closed is set by the producer to signal the consumers to exit.
		closed bool
	}
}

// Size the buffer such that a stream sender goroutine can be blocked for a
// little while and not have to send a snapshot when it resumes.
const updatesBufSize = 50

func newUpdatesBuf(st *cluster.Settings) *updatesBuf {
	buf := &updatesBuf{}
	buf.mu.updated1.L = &buf.mu
	buf.mu.updated2.L = &buf.mu
	buf.mu.data = make([]*ctpb.Update, updatesBufSize)
	buf.mu.pacer = taskpacer.New(&pacerConfig{st: st})
	return buf
}

// updatePacer atomically replaces the task pacer with a new one using the current cluster settings.
func (b *updatesBuf) updatePacer(st *cluster.Settings) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.pacer = taskpacer.New(&pacerConfig{st: st})
}

// Push adds a new update to the back of the buffer.
func (b *updatesBuf) Push(ctx context.Context, update *ctpb.Update) {
	b.mu.Lock()

	// The goal here is that we want to signal all goroutines who were waiting
	// for the next update. We know that the goroutine that we will signal will
	// wake up, and then perform some work and wait again for the next update. We
	// want to avoid the race condition where we can't differentiate between a
	// goroutine that is waiting for the next update and a goroutine that was
	// waiting, got woken up, performed some work, and is then waiting for the
	// next update.
	var condVar *sync.Cond
	var numWaiters *int
	if b.mu.activeCondVar == 0 {
		condVar = &b.mu.updated1
		numWaiters = &b.mu.numWaiters1
	} else {
		condVar = &b.mu.updated2
		numWaiters = &b.mu.numWaiters2
	}

	// At this point, we know that any goroutine that we wake up will wait on the
	// next conditional variable.
	b.mu.activeCondVar = (b.mu.activeCondVar + 1) % 2

	// If the buffer is not empty, sanity check the seq num.
	if b.sizeLocked() != 0 {
		lastIdx := b.lastIdxLocked()
		if prevSeq := b.mu.data[lastIdx].SeqNum; prevSeq != update.SeqNum-1 {
			log.KvExec.Fatalf(ctx, "bad sequence number; expected %d, got %d", prevSeq+1, update.SeqNum)
		}
	}

	overwrite := b.fullLocked()
	b.mu.data[b.mu.tail] = update
	b.mu.tail = (b.mu.tail + 1) % len(b.mu.data)
	// If the tail just overwrote the head, move the head.
	if overwrite {
		b.mu.head = (b.mu.head + 1) % len(b.mu.data)
	}

	// Notify everybody who might have been waiting for this message - we expect
	// all the connections to be blocked waiting.
	b.mu.Unlock()
	b.PaceBroadcastUpdate(ctx, condVar, numWaiters)
}

// PaceBroadcastUpdate paces the conditional variable signaling to avoid overloading the system.
func (b *updatesBuf) PaceBroadcastUpdate(ctx context.Context, condVar *sync.Cond, numWaiters *int) {
	b.mu.Lock()
	originalNumWaiters := *numWaiters
	if originalNumWaiters <= 0 {
		b.mu.Unlock()
		return
	}

	// Get the current pacer (which uses the cluster setting refresh interval).
	pacer := b.mu.pacer
	b.mu.Unlock()

	pacer.StartTask(crtime.NowMono())

	workLeft := originalNumWaiters
	for workLeft > 0 {
		todo, by := pacer.Pace(crtime.NowMono(), workLeft)

		b.mu.Lock()
		for i := 0; i < todo && workLeft > 0; i++ {
			condVar.Signal()
			workLeft--
		}
		b.mu.Unlock()

		if workLeft > 0 {
			if wait := by.Sub(crtime.NowMono()); wait > 0 {
				time.Sleep(wait)
			}
		}
	}
}

func (b *updatesBuf) lastIdxLocked() int {
	lastIdx := b.mu.tail - 1
	if lastIdx < 0 {
		lastIdx += len(b.mu.data)
	}
	return lastIdx
}

// GetBySeq looks through the buffer and returns the update with the requested
// sequence number. It's OK to request a seqNum one higher than the highest
// produced; the call will block until the message is produced.
//
// If the requested message is too old and is no longer in the buffer, returns nil.
//
// The bool retval is set to false if the producer has closed the buffer. In
// that case, the consumers should quit.
func (b *updatesBuf) GetBySeq(ctx context.Context, seqNum ctpb.SeqNum) (*ctpb.Update, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var condVar *sync.Cond
	var numWaiters *int
	if b.mu.activeCondVar == 0 {
		condVar = &b.mu.updated1
		numWaiters = &b.mu.numWaiters1
	} else {
		condVar = &b.mu.updated2
		numWaiters = &b.mu.numWaiters2
	}

	// Increment the number of waiters on the active conditional variable, and
	// decrement the same counter when we return.
	*numWaiters++
	defer func() {
		*numWaiters--
	}()

	// Loop until the requested seqNum is added to the buffer.
	for {
		if b.mu.closed {
			return nil, false
		}

		var firstSeq, lastSeq ctpb.SeqNum
		if b.sizeLocked() == 0 {
			firstSeq, lastSeq = 0, 0
		} else {
			firstSeq, lastSeq = b.mu.data[b.mu.head].SeqNum, b.mu.data[b.lastIdxLocked()].SeqNum
		}
		if seqNum < firstSeq {
			// Requesting a message that's not in the buffer any more.
			return nil, true
		}
		// If the requested msg has not been produced yet, block.
		if seqNum == lastSeq+1 {
			condVar.Wait()
			continue
		}
		if seqNum > lastSeq+1 {
			log.KvExec.Fatalf(ctx, "skipping sequence numbers; requested: %d, last: %d", seqNum, lastSeq)
		}
		idx := (b.mu.head + (int)(seqNum-firstSeq)) % len(b.mu.data)
		return b.mu.data[idx], true
	}
}

func (b *updatesBuf) sizeLocked() int {
	if b.mu.head < b.mu.tail {
		return b.mu.tail - b.mu.head
	} else if b.mu.head == b.mu.tail {
		// The buffer is either empty or full.
		// Since there's no popping from the buffer, it can only be empty if nothing
		// was ever pushed to it.
		if b.mu.head == 0 && b.mu.data[0] == nil {
			return 0
		}
		return len(b.mu.data)
	} else {
		return len(b.mu.data) + b.mu.tail - b.mu.head
	}
}

func (b *updatesBuf) fullLocked() bool {
	return b.sizeLocked() == len(b.mu.data)
}

// Close unblocks all the consumers and signals them to exit.
func (b *updatesBuf) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.closed = true
	b.mu.updated1.Broadcast()
	b.mu.updated2.Broadcast()
}

// TestingGetTotalNumWaiters returns the total number of goroutines waiting
// on the buffer. For testing purposes only.
func (b *updatesBuf) TestingGetTotalNumWaiters() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.numWaiters1 + b.mu.numWaiters2
}

// connFactory is capable of creating new connections to specific nodes.
type connFactory interface {
	new(*Sender, roachpb.NodeID) conn
}

// conn is a side-transport connection to a node. A conn watches an updatesBuf
// and streams all the messages to the respective node.
type conn interface {
	run(context.Context, *stop.Stopper)
	close()
	getState() connState
}

// rpcConnFactory is an implementation of connFactory that establishes
// connections to other nodes using gRPC.
type rpcConnFactory struct {
	dialer       rpcbase.NodeDialer
	testingKnobs connTestingKnobs
}

func newRPCConnFactory(dialer rpcbase.NodeDialer, testingKnobs connTestingKnobs) connFactory {
	return &rpcConnFactory{
		dialer:       dialer,
		testingKnobs: testingKnobs,
	}
}

// new implements the connFactory interface.
func (f *rpcConnFactory) new(s *Sender, nodeID roachpb.NodeID) conn {
	return newRPCConn(f.dialer, s, nodeID, f.testingKnobs)
}

// On sending errors, we sleep a bit as to not spin on a tripped
// circuit-breaker in the Dialer.
const sleepOnErr = time.Second

// rpcConn is an implementation of conn that is implemented using a gRPC stream.
//
// The connection will read messages from producer.buf. If the buffer overflows
// (because this stream is disconnected for long enough), we'll have to send a
// snapshot before we can resume sending regular messages.
type rpcConn struct {
	log.AmbientContext
	dialer       rpcbase.NodeDialer
	producer     *Sender
	nodeID       roachpb.NodeID
	testingKnobs connTestingKnobs

	stream   ctpb.RPCSideTransport_PushUpdatesClient
	lastSent ctpb.SeqNum
	// cancelStreamCtx cleans up the resources (goroutine) associated with stream.
	// It needs to be called whenever stream is discarded.
	cancelStreamCtx context.CancelFunc
	closed          int32 // atomic

	mu struct {
		syncutil.Mutex
		state connState
	}
}

func newRPCConn(
	dialer rpcbase.NodeDialer, producer *Sender, nodeID roachpb.NodeID, testingKnobs connTestingKnobs,
) conn {
	r := &rpcConn{
		dialer:       dialer,
		producer:     producer,
		nodeID:       nodeID,
		testingKnobs: testingKnobs,
	}
	r.mu.state.connected = false
	r.AddLogTag("ctstream", nodeID)
	return r
}

// cleanupStream releases the resources associated with r.stream and marks the conn
// as needing a new stream.
//
// err is the communication error that led to the stream being closed. Can be
// nil if the stream was closed because we're shutting down.
func (r *rpcConn) cleanupStream(err error) {
	if r.stream == nil {
		return
	}
	_ /* err */ = r.stream.CloseSend()
	r.stream = nil
	r.cancelStreamCtx()
	r.cancelStreamCtx = nil
	// If we've been disconnected, reset the message sequence. If we ever
	// reconnect, we'll ask the buffer for message 1, which was a snapshot.
	// Generally, the buffer is not going to have that message any more and so
	// we'll generate a new snapshot.
	r.lastSent = 0

	r.mu.Lock()
	r.mu.state.connected = false
	r.mu.state.lastDisconnect = err
	r.mu.state.lastDisconnectTime = timeutil.Now()
	r.mu.Unlock()
}

// close makes the connection stop sending messages. The run() goroutine will
// exit asynchronously. The parent Sender is expected to remove this connection
// from its list.
func (r *rpcConn) close() {
	atomic.StoreInt32(&r.closed, 1)
}

func (r *rpcConn) maybeConnect(ctx context.Context, _ *stop.Stopper) error {
	if r.stream != nil {
		// Already connected.
		return nil
	}

	client, err := ctpb.DialSideTransportClient(r.dialer, ctx, r.nodeID, rpcbase.SystemClass, r.producer.st)
	if err != nil {
		return err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := client.PushUpdates(streamCtx)
	if err != nil {
		cancel()
		return err
	}
	r.recordConnect()
	r.stream = stream
	// This will need to be called when we're done with the stream.
	r.cancelStreamCtx = cancel
	return nil
}

// run implements the conn interface.
func (r *rpcConn) run(ctx context.Context, stopper *stop.Stopper) {
	_ /* err */ = stopper.RunAsyncTask(ctx, fmt.Sprintf("closedts publisher for n%d", r.nodeID),
		func(ctx context.Context) {
			// This WithCancelOnQuiesce serves to interrupt r.stream.Send() calls. The
			// cancelation will be inherited by all the gRPC streams.
			ctx, cancel := stopper.WithCancelOnQuiesce(r.AnnotateCtx(ctx))
			defer cancel()

			defer r.cleanupStream(nil /* err */)
			everyN := log.Every(10 * time.Second)

			errSleepTime := sleepOnErr
			if r.testingKnobs.sleepOnErrOverride > 0 {
				errSleepTime = r.testingKnobs.sleepOnErrOverride
			}

			for {
				if ctx.Err() != nil {
					return
				}
				if atomic.LoadInt32(&r.closed) > 0 {
					return
				}
				if err := r.maybeConnect(ctx, stopper); err != nil {
					if !errors.HasType(err, (*netutil.InitialHeartbeatFailedError)(nil)) && everyN.ShouldLog() {
						log.KvExec.Infof(ctx, "side-transport failed to connect to n%d: %s", r.nodeID, err)
					}
					time.Sleep(errSleepTime)
					continue
				}

				var msg *ctpb.Update
				var ok bool
				msg, ok = r.producer.buf.GetBySeq(ctx, r.lastSent+1)
				// We can be signaled to stop in two ways: the buffer can be closed (in
				// which case all connections must exit), or this connection was closed
				// via close(). In either case, we quit.
				if !ok {
					return
				}

				if msg == nil {
					// The sequence number we've requested is no longer in the buffer. We
					// need to generate a snapshot in order to re-initialize the stream.
					// The snapshot will give us the sequence number to use for future
					// incrementals.
					msg = r.producer.GetSnapshot()
				}
				r.lastSent = msg.SeqNum

				if fn := r.testingKnobs.beforeSend; fn != nil {
					fn(r.nodeID, msg)
				}
				if err := r.stream.Send(msg); err != nil {
					if err != io.EOF && everyN.ShouldLog() {
						log.KvExec.Warningf(ctx, "failed to send closed timestamp message %d to n%d: %s",
							r.lastSent, r.nodeID, err)
					}
					// Keep track of the fact that we need a new connection.
					//
					// TODO(andrei): Instead of simply trying to establish a connection
					// again when the next message needs to be sent and get rejected by
					// the circuit breaker if the remote node is still unreachable, we
					// should have a blocking version of Dial() that we just leave hanging
					// and get a notification when it succeeds.
					r.cleanupStream(err)
					time.Sleep(errSleepTime)
				}
			}
		})
}

type connState struct {
	connected          bool
	connectedTime      time.Time
	lastDisconnect     error
	lastDisconnectTime time.Time
}

func (r *rpcConn) getState() connState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.state
}

func (r *rpcConn) recordConnect() {
	r.mu.Lock()
	r.mu.state.connected = true
	r.mu.state.connectedTime = timeutil.Now()
	r.mu.Unlock()
}

func (s streamState) String() string {
	sb := &strings.Builder{}

	fmt.Fprintf(sb, "ranges tracked: %d\n", len(s.tracked))

	// List the closed timestamps.
	sb.WriteString("closed timestamps: ")
	now := timeutil.Now()
	for policy, closedTS := range s.lastClosed {
		if policy != 0 {
			sb.WriteString(", ")
		}
		ago := now.Sub(closedTS.GoTime()).Truncate(time.Millisecond)
		var agoMsg string
		if ago >= 0 {
			agoMsg = fmt.Sprintf("%s ago", ago)
		} else {
			agoMsg = fmt.Sprintf("%s in the future", -ago)
		}
		fmt.Fprintf(sb, "%s:%s (%s)", policy, closedTS, agoMsg)
	}

	// List the tracked ranges.
	sb.WriteString("\nTracked ranges by policy: (<range>:<LAI>)\n")
	type rangeInfo struct {
		id roachpb.RangeID
		trackedRange
	}
	rangesByPolicy := make(map[ctpb.RangeClosedTimestampPolicy][]rangeInfo)
	for rid, info := range s.tracked {
		rangesByPolicy[info.policy] = append(rangesByPolicy[info.policy], rangeInfo{id: rid, trackedRange: info})
	}
	for policy, ranges := range rangesByPolicy {
		fmt.Fprintf(sb, "%s: ", policy)
		slices.SortFunc(ranges, func(a, b rangeInfo) int {
			return cmp.Compare(a.id, b.id)
		})
		for i, rng := range ranges {
			if i > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(sb, "r%d:%d", rng.id, rng.lai)
		}
		if len(ranges) != 0 {
			sb.WriteRune('\n')
		}
	}
	return sb.String()
}
