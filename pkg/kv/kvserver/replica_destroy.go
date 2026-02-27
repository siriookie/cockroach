// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// DestroyReason indicates if a replica is alive, destroyed, corrupted or pending destruction.
type DestroyReason int

// **两种销毁原因**:
// **1. `destroyReasonRemoved`**: Replica 已从 Raft group 移除
// ```
// 触发场景:
//   - Rebalancing: ChangeReplicas([Remove (n1,s1):1])
//   - Range 合并: 右半部分被合并到左半部分
//   - Replica 损坏: 被标记为 corrupted
//
// 状态设置:
//
//	r.shMu.destroyStatus.Set(
//	    errors.New("removed from raft group"),
//	    destroyReasonRemoved,
//	)
//
// 后续处理:
//   - 所有 Queue 都应该跳过 (除非 processDestroyedReplicas=true)
//   - Replica 等待 GC 清理
//
// ```
// **2. `destroyReasonMergePending`**: Range 合并待定
// ```
// 触发场景:
//   - Range-99 和 Range-100 合并
//   - Range-100 (右侧) 被标记为 destroyReasonMergePending
//   - 等待 Range-99 (左侧) 完成 subsumption
//
// 状态设置:
//
//	r.shMu.destroyStatus.Set(
//	    errors.New("merge pending"),
//	    destroyReasonMergePending,
//	)
//
// 问题: 如果 Range-99 的 leaseholder 故障,subsumption 可能一直不完成
// 解决: GC Queue 可以处理 destroyReasonMergePending (如果超时)
// ```
// // 只有 GC Queue 设置为 true
// gcQueue := newGCQueue(store, cfg)
// gcQueue.queueConfig.processDestroyedReplicas = true
//
// // 其他 Queue 都是 false
// replicateQueue := newReplicateQueue(store, cfg)
// replicateQueue.queueConfig.processDestroyedReplicas = false
const (
	// The replica is alive.
	destroyReasonAlive DestroyReason = iota
	// The replica has been GCed or is in the process of being synchronously
	// removed.
	destroyReasonRemoved
	// The replica has been merged into its left-hand neighbor, but its left-hand
	// neighbor hasn't yet subsumed it.
	destroyReasonMergePending
)

type destroyStatus struct {
	reason DestroyReason
	err    error
}

func (s destroyStatus) String() string {
	return fmt.Sprintf("{%v %d}", s.err, s.reason)
}

func (s *destroyStatus) Set(err error, reason DestroyReason) {
	s.err = err
	s.reason = reason
}

// IsAlive returns true when a replica is alive.
func (s destroyStatus) IsAlive() bool {
	return s.reason == destroyReasonAlive
}

// Removed returns whether the replica has been removed.
func (s destroyStatus) Removed() bool {
	return s.reason == destroyReasonRemoved
}

// postDestroyRaftMuLocked is called after the replica destruction is durably
// written to Pebble.
func (r *Replica) postDestroyRaftMuLocked(ctx context.Context) error {
	// TODO(#136416): at node startup, we should remove all on-disk directories
	// belonging to replicas which aren't present. A crash before a call to
	// postDestroyRaftMuLocked will currently leave the files around forever.
	if err := r.logStorage.ls.Sideload.Clear(ctx); err != nil {
		return err
	}

	// Release the reference to this tenant in metrics, we know the tenant ID is
	// valid if the replica is initialized.
	if r.tenantMetricsRef != nil {
		r.store.metrics.releaseTenant(ctx, r.tenantMetricsRef)
	}

	// Unhook the tenant rate limiter if we have one.
	if r.tenantLimiter != nil {
		r.store.tenantRateLimiters.Release(r.tenantLimiter)
	}

	return nil
}

// destroyRaftMuLocked deletes data associated with a replica, leaving a
// tombstone. The Replica may not be initialized in which case only the
// range ID local data is removed.
func (r *Replica) destroyRaftMuLocked(ctx context.Context, nextReplicaID roachpb.ReplicaID) error {
	startTime := timeutil.Now()

	ms := r.GetMVCCStats()
	batch := r.store.TODOEngine().NewWriteBatch()
	defer batch.Close()

	// TODO(sep-raft-log): need both engines separately here.
	if err := kvstorage.DestroyReplica(
		ctx, kvstorage.TODOReaderWriter(r.store.TODOEngine(), batch),
		r.destroyInfoRaftMuLocked(), nextReplicaID,
	); err != nil {
		return err
	}
	preTime := timeutil.Now()

	// We need to sync here because we are potentially deleting sideloaded
	// proposals from the file system next. We could write the tombstone only in
	// a synchronous batch first and then delete the data alternatively, but
	// then need to handle the case in which there is both the tombstone and
	// leftover replica data.
	if err := batch.Commit(true); err != nil {
		return err
	}
	commitTime := timeutil.Now()

	if err := r.postDestroyRaftMuLocked(ctx); err != nil {
		return err
	}
	if r.IsInitialized() {
		log.KvDistribution.Infof(ctx, "removed %d (%d+%d) keys in %0.0fms [clear=%0.0fms commit=%0.0fms]",
			ms.KeyCount+ms.SysCount, ms.KeyCount, ms.SysCount,
			commitTime.Sub(startTime).Seconds()*1000,
			preTime.Sub(startTime).Seconds()*1000,
			commitTime.Sub(preTime).Seconds()*1000)
	} else {
		log.KvDistribution.Infof(ctx, "removed uninitialized range in %0.0fms [clear=%0.0fms commit=%0.0fms]",
			commitTime.Sub(startTime).Seconds()*1000,
			preTime.Sub(startTime).Seconds()*1000,
			commitTime.Sub(preTime).Seconds()*1000)
	}
	return nil
}

// disconnectReplicationRaftMuLocked is called when a Replica is being removed.
// It cancels all outstanding proposals, closes the proposalQuota if there
// is one, releases all held flow tokens, and removes the in-memory raft state.
func (r *Replica) disconnectReplicationRaftMuLocked(ctx context.Context) {
	r.raftMu.AssertHeld()
	r.flowControlV2.OnDestroyRaftMuLocked(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	// NB: In the very rare scenario that we're being removed but currently
	// believe we are the leaseholder and there are more requests waiting for
	// quota than total quota then failure to close the proposal quota here could
	// leave those requests stuck forever.
	if pq := r.mu.proposalQuota; pq != nil {
		pq.Close("destroyed")
	}
	r.mu.proposalBuf.FlushLockedWithoutProposing(ctx)
	for _, p := range r.mu.proposals {
		r.cleanupFailedProposalLocked(p)
		// NB: each proposal needs its own version of the error (i.e. don't try to
		// share the error across proposals).
		p.finishApplication(ctx, makeProposalResultErr(kvpb.NewAmbiguousResultError(apply.ErrRemoved)))
	}

	if !r.shMu.destroyStatus.Removed() {
		log.KvDistribution.Fatalf(ctx, "removing raft group before destroying replica %s", r)
	}
	r.mu.internalRaftGroup = nil
	r.mu.raftTracer.Close()
}
