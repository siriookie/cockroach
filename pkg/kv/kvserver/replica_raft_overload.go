// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"cmp"
	"context"
	"math/rand"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

// pauseReplicationIOThreshold is the admission.io.overload threshold at which
// we pause replication to non-essential followers.
var pauseReplicationIOThreshold = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"admission.kv.pause_replication_io_threshold",
	"pause replication to non-essential followers when their I/O admission control score exceeds the given threshold (zero to disable)",
	0,
	settings.FloatWithMinimumOrZeroDisable(0.3),
)

type ioThresholdMapI interface {
	// AbovePauseThreshold returns true if the store's score exceeds the threshold
	// set for trying to pause replication traffic to followers on it.
	AbovePauseThreshold(_ roachpb.StoreID) bool
}

type computeExpendableOverloadedFollowersInput struct {
	self          roachpb.ReplicaID  // 本副本的 ID
	replDescs     roachpb.ReplicaSet // Range 的所有副本
	ioOverloadMap ioThresholdMapI    // IO 阈值快照
	// getProgressMap returns Raft's view of the progress map. This is only called
	// when needed, and at most once.
	getProgressMap func(context.Context) map[raftpb.PeerID]tracker.Progress // 惰性获取 Raft 进度
	// seed is used to randomize selection of which followers to pause in case
	// there are multiple followers that qualify, but quorum constraints require
	// picking a subset. In practice, we set this to the RangeID to ensure maximum
	// stability of the selection on a per-Range basis while encouraging randomness
	// across ranges (which in turn should reduce load on all overloaded followers).
	seed int64 // 随机种子（使用 RangeID）
	// In addition to being in StateReplicate in the progress map, we also only
	// consider a follower live it its Match index matches or exceeds
	// minLiveMatchIndex. This makes sure that a follower that is behind is not
	// mistaken for one that can meaningfully contribute to quorum in the short
	// term. Without this, it is - at least in theory - possible that as an
	// overloaded formerly expendable store becomes non-overloaded, we will
	// quickly mark another overloaded store as expendable under the assumption
	// that the original store can now contribute to quorum. However, that store
	// is likely behind on the log, and we should consider it as non-live until
	// it has caught up.
	minLiveMatchIndex kvpb.RaftIndex // 落后的 follower 不计入 quorum
}

type nonLiveReason byte

const (
	nonLiveReasonInactive nonLiveReason = iota
	nonLiveReasonPaused
	nonLiveReasonBehind
)

// computeExpendableOverloadedFollowers computes a set of followers that we can
// intentionally exempt from replication traffic (MsgApp) to help them avoid I/O
// overload.
//
// In the common case (no store or at least no follower store close to I/O
// overload), this method does very little work.
//
// If at least one follower is (close to being) overloaded, we determine the
// maximum set of such followers that we can afford not to replicate to without
// losing quorum by successively reducing the set of overloaded followers by one
// randomly selected overloaded voter. The randomness makes it more likely that
// when there are multiple overloaded stores in the system that cannot be
// jointly excluded, both stores will in aggregate be relieved from
// approximately 50% of follower raft traffic.
//
// This method uses Raft's view of liveness and in particular will consider
// followers that haven't responded recently (including heartbeats) or are
// waiting for a snapshot as not live. In particular, a follower that is
// initially in the map may transfer out of the map by virtue of being cut off
// from the raft log via a truncation. This is acceptable, since the snapshot
// prevents the replica from receiving log traffic.
// 计算可暂停集合
func computeExpendableOverloadedFollowers(
	ctx context.Context, d computeExpendableOverloadedFollowersInput,
) (map[roachpb.ReplicaID]struct{},    /*可暂停的 follower 集合*/
	map[roachpb.ReplicaID]nonLiveReason /*非活跃副本及原因（Inactive/Paused/Behind）*/) {
	var nonLive map[roachpb.ReplicaID]nonLiveReason
	var liveOverloadedVoterCandidates map[roachpb.ReplicaID]struct{}
	var liveOverloadedNonVoterCandidates map[roachpb.ReplicaID]struct{}
	var prs map[raftpb.PeerID]tracker.Progress

	for _, replDesc := range d.replDescs.Descriptors() {
		// 跳过未过载的 Store 和本副本
		if pausable := d.ioOverloadMap.AbovePauseThreshold(replDesc.StoreID); !pausable || replDesc.ReplicaID == d.self {
			continue
		}
		// There's at least one overloaded follower, so initialize
		// extra state to determine which traffic we can drop without
		// losing quorum.
		// 首次发现过载 follower 时，惰性获取 Raft 进度
		// 惰性计算：只有在发现至少一个过载 follower 时才调用 getProgressMap()（避免无效的 Raft 状态读取）
		//- 活跃性判断：结合 RecentActive、IsPaused、Match Index 三个维度
		if prs == nil {
			prs = d.getProgressMap(ctx)
			nonLive = map[roachpb.ReplicaID]nonLiveReason{}
			for id, pr := range prs {
				// NB: RecentActive is populated by updateRaftProgressFromActivity().
				if !pr.RecentActive {
					nonLive[roachpb.ReplicaID(id)] = nonLiveReasonInactive
				}
				if pr.IsPaused() {
					nonLive[roachpb.ReplicaID(id)] = nonLiveReasonPaused
				}
				if kvpb.RaftIndex(pr.Match) < d.minLiveMatchIndex {
					nonLive[roachpb.ReplicaID(id)] = nonLiveReasonBehind
				}
			}
			// 区分 voter 和 non-voter
			liveOverloadedVoterCandidates = map[roachpb.ReplicaID]struct{}{}
			liveOverloadedNonVoterCandidates = map[roachpb.ReplicaID]struct{}{}
		}

		// Mark replica on overloaded store as possibly pausable.
		//
		// NB: we make no distinction between non-live and live replicas at this
		// point. That is, even if a replica is considered "non-live", we will still
		// consider "additionally" pausing it. The first instinct was to avoid
		// layering anything on top of a non-live follower, however a paused
		// follower immediately becomes non-live, so if we want stable metrics on
		// which followers are "paused", then we need the "pausing" state to
		// overrule the "non-live" state.
		if prs[raftpb.PeerID(replDesc.ReplicaID)].IsLearner {
			liveOverloadedNonVoterCandidates[replDesc.ReplicaID] = struct{}{}
		} else {
			liveOverloadedVoterCandidates[replDesc.ReplicaID] = struct{}{}
		}
	}

	// Start out greedily with all overloaded candidates paused, and remove
	// randomly chosen candidates until we think the raft group can obtain quorum.
	//贪心缩减候选集（保证 quorum）
	var rnd *rand.Rand
	for len(liveOverloadedVoterCandidates) > 0 {
		// 检查当前候选集是否能保证 quorum

		up := d.replDescs.CanMakeProgress(func(replDesc roachpb.ReplicaDescriptor) bool {
			rid := replDesc.ReplicaID
			if _, ok := nonLive[rid]; ok {
				return false // 非活跃副本不计入 quorum
			}
			if _, ok := liveOverloadedVoterCandidates[rid]; ok {
				return false // 暂停候选不计入 quorum
			}
			if _, ok := liveOverloadedNonVoterCandidates[rid]; ok {
				return false // non-voter 不影响 quorum（但也暂停）
			}
			return true // 健康副本计入 quorum
		})
		if up {
			// 找到最大可暂停集合，退出循环

			// We've found the largest set of voters to drop traffic to
			// without losing quorum.
			break
		}
		// Quorum 不足，随机移除一个 voter 候选
		var sl []roachpb.ReplicaID
		for sid := range liveOverloadedVoterCandidates {
			sl = append(sl, sid)
		}
		// Sort for determinism during tests.
		slices.Sort(sl) // 排序保证测试确定性
		// Remove a random voter candidate, and loop around to see if we now have
		// quorum.
		if rnd == nil {
			rnd = rand.New(rand.NewSource(d.seed)) // 使用 RangeID 作为种子
		}
		delete(liveOverloadedVoterCandidates, sl[rnd.Intn(len(sl))])
	}
	// 返回 voter 和 non-voter 的并集
	// Return union of non-voter and voter candidates.
	for nonVoter := range liveOverloadedNonVoterCandidates {
		liveOverloadedVoterCandidates[nonVoter] = struct{}{}
	}
	return liveOverloadedVoterCandidates, nonLive
}

type ioThresholdMap struct {
	threshold float64 // threshold at which the score indicates pausability  根据准入控制算出来的score
	seq       int     // bumped on creation if pausable set changed
	m         map[roachpb.StoreID]*admissionpb.IOThreshold
}

func (osm ioThresholdMap) String() string {
	return redact.StringWithoutMarkers(osm)
}

var _ redact.SafeFormatter = (*ioThresholdMap)(nil)

func (osm ioThresholdMap) SafeFormat(s redact.SafePrinter, verb rune) {
	var sl []roachpb.StoreID
	for id := range osm.m {
		sl = append(sl, id)
	}
	slices.SortFunc(sl, func(a, b roachpb.StoreID) int {
		aScore, _ := osm.m[a].Score()
		bScore, _ := osm.m[b].Score()
		return cmp.Compare(aScore, bScore)
	})
	for i, id := range sl {
		if i > 0 {
			s.SafeString(", ")
		}
		s.Printf("s%d: %s", id, osm.m[id])
	}
	if len(sl) > 0 {
		s.Printf(" [pausable-threshold=%.2f]", osm.threshold)
	}
}

var _ ioThresholdMapI = (*ioThresholdMap)(nil)

// Score 计算（在 admissionpb.IOThreshold 中）：
//
//	func (iot *IOThreshold) Score() (float64, bool) {
//	   if iot.L0NumSubLevelsThreshold == 0 || iot.L0NumFilesThreshold == 0 {
//	       return 0, false  // 未初始化
//	   }
//
//	   // 计算两个维度的负载比例
//	   subLevelScore := float64(iot.L0NumSubLevels) / float64(iot.L0NumSubLevelsThreshold)
//	   fileScore := float64(iot.L0NumFiles) / float64(iot.L0NumFilesThreshold)
//
//	   // 返回最大值（最严重的维度）
//	   return max(subLevelScore, fileScore), true
//	}
//
// ​
// 示例计算：
// - 正常 Store：L0SubLevels=5（阈值 20），L0Files=100（阈值 1000）
// - subLevelScore = 5/20 = 0.25
// - fileScore = 100/1000 = 0.10
// - Score = max(0.25, 0.10) = 0.25（未超过 0.8 阈值）
// 过载 Store：L0SubLevels=18（阈值 20），L0Files=950（阈值 1000）
// subLevelScore = 18/20 = 0.90
// fileScore = 950/1000 = 0.95
// Score = max(0.90, 0.95) = 0.95（超过 0.8 阈值）
// AbovePauseThreshold implements ioThresholdMapI.
func (osm *ioThresholdMap) AbovePauseThreshold(id roachpb.StoreID) bool {
	sc, _ := osm.m[id].Score()
	return sc > osm.threshold
}

func (osm *ioThresholdMap) AnyAbovePauseThreshold(repls roachpb.ReplicaSet) bool {
	descs := repls.Descriptors()
	for i := range descs {
		if osm.AbovePauseThreshold(descs[i].StoreID) {
			return true
		}
	}
	return false
}

func (osm *ioThresholdMap) IOThreshold(id roachpb.StoreID) *admissionpb.IOThreshold {
	return osm.m[id]
}

// Sequence allows distinguishing sets of overloaded stores. Whenever an
// ioThresholdMap is created, it inherits the sequence of its predecessor,
// incrementing only when the set of pausable stores has changed in the
// transition.
func (osm *ioThresholdMap) Sequence() int {
	return osm.seq
}

// `ioThresholdMap` 是一个**不可变的快照对象**（immutable snapshot），记录了集群中**所有 Store 的 IO 健康状况**。它的核心职责是：
//
// **输入**（构造时）：
// - 从 StorePool 获取的 `map[StoreID]*admissionpb.IOThreshold`（每个 Store 的 IO 负载评分）
// - `pauseReplicationIOThreshold`（集群设置，默认 0 表示禁用，生产环境通常设为 0.8-0.9）
//
// **输出**（运行时查询）：
// - `AbovePauseThreshold(storeID)`：判断某个 Store 是否超过暂停阈值（应该暂停向其复制）
// - `Sequence()`：序列号，仅当”可暂停的 Store 集合”发生变化时递增（用于触发 Replica 重新评估）
//
// **生命周期特征**：
// - **创建频率**：每次 Raft tick（默认 100ms）创建一次新的 `ioThresholdMap`
// - **作用域**：整个 Store（所有 Replica 共享同一个 `ioThresholdMap` 实例）
// - **并发访问**：通过 `ioThresholds` 容器的读写锁保护，确保 Replica 读取时看到一致的快照
type ioThresholds struct {
	mu struct {
		syncutil.Mutex
		// 原子替换，旧快照可被 Replica 持续使用
		inner *ioThresholdMap // always replaced wholesale, so can leak out of mu
	}
}

func (osm *ioThresholds) Current() *ioThresholdMap {
	osm.mu.Lock()
	defer osm.mu.Unlock()
	return osm.mu.inner
}

// Replace replaces the stored view of stores for which we track IOThresholds.
// If the set of overloaded stores (i.e. with a score of >= seqThreshold)
// changes in the process, the updated view will have an incremented Sequence().
// **输入**：
// - `m`：新的 Store → IOThreshold 映射
// - `seqThreshold`：暂停阈值（如 0.8）
//
// **输出**：
// - `prev`：替换前的快照
// - `cur`：新创建的快照
func (osm *ioThresholds) Replace(
	m map[roachpb.StoreID]*admissionpb.IOThreshold, seqThreshold float64,
) (prev, cur *ioThresholdMap) {
	osm.mu.Lock()
	defer osm.mu.Unlock()
	last := osm.mu.inner
	if last == nil {
		last = &ioThresholdMap{} // 初始化
	}
	// 1. 创建新快照（继承旧序列号）
	next := &ioThresholdMap{threshold: seqThreshold, seq: last.seq, m: m}
	// 2. 检查"可暂停 Store 集合"是否变化
	var delta int
	for id := range last.m {
		if last.AbovePauseThreshold(id) != next.AbovePauseThreshold(id) {
			delta = 1
			break
		}
	}
	for id := range next.m {
		if last.AbovePauseThreshold(id) != next.AbovePauseThreshold(id) {
			delta = 1
			break
		}
	}
	// 3. 如果集合变化，递增序列号
	next.seq += delta
	// 4. 原子替换
	osm.mu.inner = next
	return last, next
}

func (r *Replica) updatePausedFollowersLocked(ctx context.Context, ioThresholdMap *ioThresholdMap) {
	r.mu.pausedFollowers = nil // 清空旧状态

	desc := r.descRLocked()
	repls := desc.Replicas()
	// 1. 快速路径：如果没有任何 Store 超过阈值，直接返回
	if !ioThresholdMap.AnyAbovePauseThreshold(repls) {
		return
	}
	// 2. 只有 Raft leader 才暂停 follower
	if !r.isRaftLeaderRLocked() {
		// Only the raft leader pauses followers. Followers never send meaningful
		// amounts of data in raft messages, so pausing doesn't make sense on them.
		return
	}
	// 3. 如果启用了 RAC v2（pull-mode），跳过暂停机制
	if r.shouldReplicationAdmissionControlUsePullMode(ctx) {
		// Replication admission control is enabled and is using pull-mode which
		// allows for formation of a send-queue. The send-queue and pull-mode
		// behavior is RAC2 subsumes follower pausing, so do not pause.
		return
	}
	// 4. 关键 Range（如 liveness range）不暂停
	if !quotaPoolEnabledForRange(desc) {
		// If the quota pool isn't enabled (like for the liveness range), play it
		// safe. The range is unlikely to be a major contributor to any follower's
		// I/O and wish to reduce the likelihood of a problem in replication pausing
		// contributing to an outage of that critical range.
		return
	}
	// 5. 只有 leaseholder 才能暂停 follower（防止脑裂）
	status := r.leaseStatusAtRLocked(ctx, r.Clock().NowAsClockTimestamp())
	if !status.IsValid() || !status.OwnedBy(r.StoreID()) {
		// If we're not the leaseholder (which includes the case in which we just
		// transferred the lease away), leave all followers unpaused. Otherwise, the
		// leaseholder won't learn that the entries it submitted were committed
		// which effectively causes range unavailability.
		return
	}

	// When multiple followers are overloaded, we may not be able to exclude all
	// of them from replication traffic due to quorum constraints. We would like
	// a given Range to deterministically exclude the same store (chosen
	// randomly), so that across multiple Ranges we have a chance of removing
	// load from all overloaded Stores in the cluster. (It would be a bad idea
	// to roll a per-Range dice here on every tick, since that would rapidly
	// include and exclude individual followers from replication traffic, which
	// would be akin to a high rate of packet loss. Once we've decided to ignore
	// a follower, this decision should be somewhat stable for at least a few
	// seconds).
	//当多个 follower 节点同时过载时，
	//由于 Raft 的 quorum 约束（比如 3 副本至少要和 2 个节点通信），
	//我们不可能把所有过载的 follower 都从复制流量中剔除。

	//我们希望：对于某一个 Range，
	//它在需要“牺牲”一个 follower 时，
	//能够**确定性地排除同一个 store**
	//（这个 store 最初是随机选出来的）。

	//这样一来，在整个集群的多个 Range 上：
	//- 不同 Range 会排除不同的 store
	//- 从而在“全局层面”有机会
	//  把复制负载从所有过载的 store 上慢慢卸下来

	//如果我们在每一个调度周期（tick）里，
	//都为每个 Range 重新随机选一个要排除的 follower，
	//那将是一个非常糟糕的设计。因为这样会导致：
	//- 某个 follower 一会儿被排除
	//- 一会儿又被重新加入复制流量,这种行为在效果上
	//等价于“极高丢包率”：
	//- 复制请求不断被打断
	//- Raft 延迟、重试、超时激增,因此，一旦我们决定暂时忽略某个 follower：
	//- 这个决定必须保持一段时间的稳定性
	//- 至少持续几秒钟
	seed := int64(r.RangeID) // 使用 RangeID 作为随机种子（确保稳定性）
	now := r.store.Clock().Now().GoTime()
	d := computeExpendableOverloadedFollowersInput{
		self:          r.replicaID,
		replDescs:     repls,
		ioOverloadMap: ioThresholdMap,
		getProgressMap: func(_ context.Context) map[raftpb.PeerID]tracker.Progress {
			prs := r.mu.internalRaftGroup.Status().Progress
			updateRaftProgressFromActivity(ctx, prs, repls.Descriptors(), func(id roachpb.ReplicaID) bool {
				return r.mu.lastUpdateTimes.isFollowerActiveSince(id, now, r.store.cfg.RangeLeaseDuration)
			})
			return prs
		},
		minLiveMatchIndex: r.mu.proposalQuotaBaseIndex, // 落后的 follower 不计入 quorum
		seed:              seed,
	}
	r.mu.pausedFollowers, _ = computeExpendableOverloadedFollowers(ctx, d)
	// 7. 通知 Raft 这些 follower 不可达（让 Raft 进入 probing 状态）
	bypassFn := r.store.TestingKnobs().RaftReportUnreachableBypass
	for replicaID := range r.mu.pausedFollowers {
		if bypassFn != nil && bypassFn(replicaID) {
			continue
		}
		// We're dropping messages to those followers (see handleRaftReady) but
		// it's a good idea to tell raft not to even bother sending in the first
		// place. Raft will react to this by moving the follower to probing state
		// where it will be contacted only sporadically until it responds to an
		// MsgApp (which it can only do once we stop dropping messages). Something
		// similar would result naturally if we didn't report as unreachable, but
		// with more wasted work.
		r.mu.internalRaftGroup.ReportUnreachable(raftpb.PeerID(replicaID))
	}
}
