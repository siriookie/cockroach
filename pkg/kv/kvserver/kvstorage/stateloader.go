// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// StateLoader contains accessor methods to read or write the
// fields of kvserverbase.ReplicaState. It contains an internal buffer
// which is reused to avoid an allocation on frequently-accessed code
// paths.
//
// Because of this internal buffer, this struct is not safe for
// concurrent use, and the return values of methods that return keys
// are invalidated the next time any method is called.
//
// It is safe to have multiple replicaStateLoaders for the same
// Replica. Reusable replicaStateLoaders are typically found in a
// struct with a mutex, and temporary loaders may be created when
// locking is less desirable than an allocation.
type StateLoader struct {
	logstore.StateLoader
}

// MakeStateLoader creates a StateLoader.
func MakeStateLoader(rangeID roachpb.RangeID) StateLoader {
	return StateLoader{
		StateLoader: logstore.NewStateLoader(rangeID),
	}
}

// Load a ReplicaState from disk. The exception is the Desc field, which is
// updated transactionally, and is populated from the supplied RangeDescriptor
// under the convention that that is the latest committed version.
func (s StateLoader) Load(
	ctx context.Context, stateRO StateRO, desc *roachpb.RangeDescriptor,
) (kvserverpb.ReplicaState, error) {
	var r kvserverpb.ReplicaState
	// TODO(tschottdorf): figure out whether this is always synchronous with
	// on-disk state (likely iffy during Split/ChangeReplica triggers).
	//. 初始化描述符 (Desc)
	//它并不是从磁盘读取描述符，而是克隆（Clone）传入的 desc 参数。
	//逻辑背景：在 CockroachDB 中，Range 描述符通常是事务性更新的，且在内存中已有最新版本。
	//Load
	// 函数遵循“传入的 desc 是最新已提交版本”的约定。
	r.Desc = protoutil.Clone(desc).(*roachpb.RangeDescriptor)
	// Read the range lease.
	//调用 s.LoadLease 从磁盘读取该 Range 的当前租约信息。
	//这是决定谁是该 Range “主节点”的关键数据。
	lease, err := s.LoadLease(ctx, stateRO)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	r.Lease = &lease
	//3. 加载垃圾回收相关信息 (GC)
	//LoadGCThreshold
	//: 加载 GC 阈值时间戳（在此时间戳之前的数据可以被清理）。
	//LoadGCHint
	//: 加载 GC 提示信息，用于优化清理逻辑。
	if r.GCThreshold, err = s.LoadGCThreshold(ctx, stateRO); err != nil {
		return kvserverpb.ReplicaState{}, err
	}

	if r.GCHint, err = s.LoadGCHint(ctx, stateRO); err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	//加载强制刷新索引 (ForceFlushIndex)
	//LoadRangeForceFlushIndex
	//: 加载该 Range 建议/强制刷新数据的索引位置。
	if r.ForceFlushIndex, err = s.LoadRangeForceFlushIndex(ctx, stateRO); err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	//加载应用状态 (Applied State) - 核心数据
	//这部分从
	//LoadRangeAppliedState
	// 中读取，包含了关键的 Raft 进度：
	//RaftAppliedIndex: 状态机已经执行（应用）到的 Raft 条目索引。
	//RaftAppliedIndexTerm: 对应应用条目的任期。
	//LeaseAppliedIndex: 与租约变更相关的应用索引。
	//MVCC Stats: 加载该 Range 的统计信息（如：数据占用了多少字节，有多少条记录等）。
	//RaftClosedTimestamp: 分布式事务中用于确立“已关闭时间戳”的数据。
	as, err := s.LoadRangeAppliedState(ctx, stateRO)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	r.RaftAppliedIndex = as.RaftAppliedIndex
	r.RaftAppliedIndexTerm = as.RaftAppliedIndexTerm
	r.LeaseAppliedIndex = as.LeaseAppliedIndex
	ms := as.RangeStats.ToStats()
	r.Stats = &ms
	r.RaftClosedTimestamp = as.RaftClosedTimestamp

	// Invariant: TruncatedState == nil. The field is being phased out. The
	// RaftTruncatedState must be loaded separately.
	r.TruncatedState = nil

	//加载该副本的内部引擎版本号。
	version, err := s.LoadVersion(ctx, stateRO)
	if err != nil {
		return kvserverpb.ReplicaState{}, err
	}
	if (version != roachpb.Version{}) {
		r.Version = &version
	}

	return r, nil
}

// Save persists the given ReplicaState to disk. It assumes that the contained
// Stats are up-to-date and returns the stats which result from writing the
// updated State.
//
// As an exception to the rule, the Desc field (whose on-disk state is special
// in that it's a full MVCC value and updated transactionally) is only used for
// its RangeID.
//
// TODO(tschottdorf): test and assert that none of the optional values are
// missing whenever save is called. Optional values should be reserved
// strictly for use in Result. Do before merge.
func (s StateLoader) Save(
	ctx context.Context, stateRW StateRW, state kvserverpb.ReplicaState,
) (enginepb.MVCCStats, error) {
	ms := state.Stats
	if err := s.SetLease(ctx, stateRW, ms, *state.Lease); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := s.SetGCThreshold(ctx, stateRW, ms, state.GCThreshold); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if err := s.SetGCHint(ctx, stateRW, ms, state.GCHint); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if state.Version != nil {
		if err := s.SetVersion(ctx, stateRW, ms, state.Version); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	state.Stats = ms // no-op, just an acknowledgement that the stats were updated

	as := state.ToRangeAppliedState()
	if err := s.SetRangeAppliedState(ctx, stateRW, &as); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return *ms, nil
}

// LoadLease loads the lease.
func (s StateLoader) LoadLease(ctx context.Context, stateRO StateRO) (roachpb.Lease, error) {
	var lease roachpb.Lease
	//构造 Key (s.RangeLeaseKey())：
	//它计算出该 Range 对应的租约存储 Key。租约是存储在 Range Local Key 区域的（前缀通常是 /Local/RangeID/...），这意味着它与用户数据是分开存储的。
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeLeaseKey(),
		hlc.Timestamp{}, &lease, storage.MVCCGetOptions{})
	//底层查询 (storage.MVCCGetProto)：
	//查询 Pebble：调用 MVCCGetProto 去底层存储引擎中查找该 Key。
	//空时间戳 (hlc.Timestamp{})：查询时传入了空时间戳。这是因为租约信息属于“单版本”系统元数据，它不使用 MVCC 的多版本控制，始终保持最新。
	//反序列化：将查到的二进制数据反序列化为 roachpb.Lease 协议缓冲区（protobuf）对象。
	return lease, err
}

// SetLease persists a lease.
func (s StateLoader) SetLease(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, lease roachpb.Lease,
) error {
	return storage.MVCCPutProto(ctx, stateRW, s.RangeLeaseKey(),
		hlc.Timestamp{}, &lease, storage.MVCCWriteOptions{Stats: ms})
}

// SetLeaseBlind persists a lease using a blind write, updating the MVCC stats
// based on prevLease. This is particularly beneficial for expiration lease
// extensions, which do a write per range every 3 seconds. Seeking to the
// existing record has a significant aggregate cost with many ranges, and can
// also cause Pebble block cache thrashing.
//
// NB: prevLease is usually passed from the in-memory replica state. Since lease
// requests don't hold latches (they're evaluated on the local replica),
// prevLease may be modified concurrently. In that case the lease request will
// fail below Raft, so it doesn't matter if the stats are wrong.
func (s StateLoader) SetLeaseBlind(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, lease, prevLease roachpb.Lease,
) error {
	key := s.RangeLeaseKey()
	var value, prevValue roachpb.Value
	if err := value.SetProto(&lease); err != nil {
		return err
	}
	value.InitChecksum(key)
	// NB: We persist an empty lease record when writing the initial range state,
	// so we should always pass a non-empty prevValue.
	if err := prevValue.SetProto(&prevLease); err != nil {
		return err
	}
	prevValue.InitChecksum(key)
	return storage.MVCCBlindPutInlineWithPrev(ctx, stateRW, ms, key, value, prevValue)
}

// LoadRangeAppliedState loads the Range applied state.
func (s StateLoader) LoadRangeAppliedState(
	ctx context.Context, stateRO StateRO,
) (*kvserverpb.RangeAppliedState, error) {
	var as kvserverpb.RangeAppliedState
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeAppliedStateKey(), hlc.Timestamp{}, &as,
		storage.MVCCGetOptions{})
	return &as, err
}

// LoadMVCCStats loads the MVCC stats.
func (s StateLoader) LoadMVCCStats(
	ctx context.Context, stateRO StateRO,
) (enginepb.MVCCStats, error) {
	// Check the applied state key.
	as, err := s.LoadRangeAppliedState(ctx, stateRO)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	return as.RangeStats.ToStats(), nil
}

// SetRangeAppliedState overwrites the range applied state. This state is a
// combination of the Raft and lease applied indices, along with the MVCC stats.
//
// The applied indices and the stats used to be stored separately in different
// keys. We now deem those keys to be "legacy" because they have been replaced
// by the range applied state key.
func (s StateLoader) SetRangeAppliedState(
	ctx context.Context, stateRW StateRW, as *kvserverpb.RangeAppliedState,
) error {
	// The RangeAppliedStateKey is not included in stats. This is also reflected
	// in ComputeStats.
	ms := (*enginepb.MVCCStats)(nil)
	return storage.MVCCPutProto(ctx, stateRW, s.RangeAppliedStateKey(),
		hlc.Timestamp{}, as, storage.MVCCWriteOptions{Stats: ms, Category: fs.ReplicationReadCategory})
}

// SetMVCCStats overwrites the MVCC stats. This needs to perform a read on the
// RangeAppliedState key before overwriting the stats. Use SetRangeAppliedState
// when performance is important.
func (s StateLoader) SetMVCCStats(
	ctx context.Context, stateRW StateRW, newMS *enginepb.MVCCStats,
) error {
	as, err := s.LoadRangeAppliedState(ctx, stateRW)
	if err != nil {
		return err
	}
	as.RangeStats = kvserverpb.MVCCPersistentStats(*newMS)
	return s.SetRangeAppliedState(ctx, stateRW, as)
}

// SetClosedTimestamp overwrites the closed timestamp.
func (s StateLoader) SetClosedTimestamp(
	ctx context.Context, stateRW StateRW, closedTS hlc.Timestamp,
) error {
	as, err := s.LoadRangeAppliedState(ctx, stateRW)
	if err != nil {
		return err
	}
	as.RaftClosedTimestamp = closedTS
	return s.SetRangeAppliedState(ctx, stateRW, as)
}

// LoadGCThreshold loads the GC threshold.
// 1. 针对什么数据？
// 它针对的是 Pebble 存储引擎中的旧版本数据。 由于 CockroachDB 采用 MVCC 机制，每一次更新（Update）或删除（Delete）操作并不会立即覆盖旧数据，而是会写入一个带时间戳的新版本：
//
// 旧版本值：同一个 Key 的旧版本数据。
// 删除标记（Tombstones）：当用户执行 DELETE 时，系统会写入一个特殊的标记。
// 2. 哪些数据可以被清除？
// 在这个时间轴（GCThreshold）之前的所有“非活跃”版本都可以被清除。具体规则如下：
//
// 过期的旧版本：如果一个 Key 有多个版本（例如时间戳 10, 20, 30），而
// GCThreshold
// 是 25。那么时间戳为 10 和 20 的版本即被视为“不可见”的冗余数据，可以被物理删除。
// 过期的删除标记：如果一个 Key 在时间戳 15 被删除了（写入了 Tombstone），而
// GCThreshold
// 是 25。由于删除时间早于阈值，这个 Key 的所有历史记录以及删除标记本身都可以从磁盘上彻底抹去。
// 例外情况： 如果一个 Key 最新的版本虽然早于
// GCThreshold
// （比如最后一次修改在时间戳 5），但它是该 Key 的当前唯一有效值，那么它不会被删除。GC 永远不会删除数据的“当前状态”，只会删除“历史状态”。
//
// 3. 从什么地方被清除？
// 数据是从 底层存储引擎（Pebble）的 LSM-Tree 中物理清除的。
//
// 这个清除过程通常分为两步：
//
// 逻辑识别：gcQueue（垃圾回收队列）会定期扫描各个 Range，对比每个 Key 的时间戳与
// GCThreshold
// 。
// 物理删除：通过调用 Pebble 的 Delete 或 SingleDelete 操作，或者通过 Compaction（压缩）过程，将这些旧数据从磁盘文件中彻底移除。
// 总结：GC 是怎么落地的？
// 逻辑触发：CockroachDB 的 gcQueue 检查到
// GCThreshold
// 推进了。
// 标记工作：它会通过 Raft 协议在所有副本上更新
// GCThreshold
// 这一元数据（就是你代码里看到的
// LoadGCThreshold
// 加载的那部分）。
// 物理收割：底层 Pebble 引擎 在磁盘后台进行 Compaction（压缩） 时，像筛子一样把早于这个阈值的旧版本数据过滤掉，不让它们进入下一层的 SSTable。
// 所以，最准确的答案是：发生在磁盘上的 SSTable 合并（Compaction）过程中。
func (s StateLoader) LoadGCThreshold(ctx context.Context, stateRO StateRO) (*hlc.Timestamp, error) {
	var t hlc.Timestamp
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeGCThresholdKey(),
		hlc.Timestamp{}, &t, storage.MVCCGetOptions{ReadCategory: fs.MVCCGCReadCategory})
	return &t, err
}

// SetGCThreshold sets the GC threshold.
func (s StateLoader) SetGCThreshold(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, threshold *hlc.Timestamp,
) error {
	if threshold == nil {
		return errors.New("cannot persist nil GCThreshold")
	}
	return storage.MVCCPutProto(ctx, stateRW, s.RangeGCThresholdKey(),
		hlc.Timestamp{}, threshold, storage.MVCCWriteOptions{Stats: ms})
}

// LoadGCHint loads GC hint.
func (s StateLoader) LoadGCHint(ctx context.Context, stateRO StateRO) (*roachpb.GCHint, error) {
	var h roachpb.GCHint
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeGCHintKey(),
		hlc.Timestamp{}, &h, storage.MVCCGetOptions{ReadCategory: fs.MVCCGCReadCategory})
	if err != nil {
		return nil, err
	}
	return &h, nil
}

// SetGCHint writes the GC hint.
func (s StateLoader) SetGCHint(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, hint *roachpb.GCHint,
) error {
	if hint == nil {
		return errors.New("cannot persist nil GCHint")
	}
	return storage.MVCCPutProto(ctx, stateRW, s.RangeGCHintKey(),
		hlc.Timestamp{}, hint, storage.MVCCWriteOptions{Stats: ms})
}

// LoadVersion loads the replica version.
func (s StateLoader) LoadVersion(ctx context.Context, stateRO StateRO) (roachpb.Version, error) {
	var version roachpb.Version
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeVersionKey(),
		hlc.Timestamp{}, &version, storage.MVCCGetOptions{})
	return version, err
}

// SetVersion sets the replica version.
func (s StateLoader) SetVersion(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, version *roachpb.Version,
) error {
	return storage.MVCCPutProto(ctx, stateRW, s.RangeVersionKey(),
		hlc.Timestamp{}, version, storage.MVCCWriteOptions{Stats: ms})
}

// LoadRangeForceFlushIndex loads the force-flush index.
// 在 CockroachDB 中，
// ForceFlushIndex
// 通常用于 数据一致性和持久化检查：
//
// Raft 交互：有时候系统需要知道：截至 Raft Index X 的所有日志和其对应的状态机变更，是否不仅被“写入”了磁盘，而且已经通过 fsync 等原子操作刷新到了物理介质上。
// WAL 截断：在截断 Raft 日志（WAL）之前，系统可能需要检查
// ForceFlushIndex
// 。只有那些已经被安全“刷新”到 SSTable 或持久化元数据中的索引，其对应的日志才能被安全删除。
// 备份/快照：在进行一致性快照或冷备时，该索引用来追踪磁盘上数据的“鲜活程度”

// LoadRangeForceFlushIndex
// 就是去 Pebble 中查一下该副本目前承诺的、已经完全落地（刷盘）的最后一个 Raft 索引位置是多少。这为系统提供了一个安全边界，确保在发生意外宕机恢复时，数据不会因为尚在内存 Buffer 中而丢失。
func (s StateLoader) LoadRangeForceFlushIndex(
	ctx context.Context, stateRO StateRO,
) (roachpb.ForceFlushIndex, error) {
	var ffIndex roachpb.ForceFlushIndex
	// If not found, ffIndex.Index will stay 0.
	_, err := storage.MVCCGetProto(ctx, stateRO, s.RangeForceFlushKey(),
		hlc.Timestamp{}, &ffIndex, storage.MVCCGetOptions{})
	return ffIndex, err
}

// SetForceFlushIndex sets the force-flush index.
func (s StateLoader) SetForceFlushIndex(
	ctx context.Context, stateRW StateRW, ms *enginepb.MVCCStats, ffIndex *roachpb.ForceFlushIndex,
) error {
	return storage.MVCCPutProto(ctx, stateRW, s.RangeForceFlushKey(),
		hlc.Timestamp{}, ffIndex, storage.MVCCWriteOptions{Stats: ms})
}

// LoadRaftReplicaID loads the RaftReplicaID. Returns an empty RaftReplicaID if
// the key is not found, which can only happen if the replica does not exist.
// The caller must assert if they don't expect a missing replica.
func (s StateLoader) LoadRaftReplicaID(
	ctx context.Context, stateRO StateRO,
) (kvserverpb.RaftReplicaID, error) {
	var replicaID kvserverpb.RaftReplicaID
	if ok, err := storage.MVCCGetProto(
		ctx, stateRO, s.RaftReplicaIDKey(), hlc.Timestamp{}, &replicaID,
		storage.MVCCGetOptions{ReadCategory: fs.ReplicationReadCategory},
	); err != nil || !ok {
		// NB: when err == nil && !ok, there is no RaftReplicaID. This can happen
		// only if the replica does not exist.
		return kvserverpb.RaftReplicaID{}, err
	}
	return replicaID, nil
}

// SetRaftReplicaID overwrites the RaftReplicaID.
func (s StateLoader) SetRaftReplicaID(
	ctx context.Context, stateWO StateWO, replicaID roachpb.ReplicaID,
) error {
	rid := kvserverpb.RaftReplicaID{ReplicaID: replicaID}
	// "Blind" because opts.Stats == nil and timestamp.IsEmpty().
	return storage.MVCCBlindPutProto(
		ctx,
		stateWO,
		s.RaftReplicaIDKey(),
		hlc.Timestamp{}, /* timestamp */
		&rid,
		storage.MVCCWriteOptions{}, /* opts */
	)
}

// ClearRaftReplicaID clears the RaftReplicaID key.
func (s StateLoader) ClearRaftReplicaID(stateWO StateWO) error {
	return stateWO.ClearUnversioned(s.RaftReplicaIDKey(), storage.ClearOptions{})
}

// LoadRangeTombstone loads the RangeTombstone of the range.
func (s StateLoader) LoadRangeTombstone(
	ctx context.Context, stateRO StateRO,
) (kvserverpb.RangeTombstone, error) {
	var ts kvserverpb.RangeTombstone
	if ok, err := storage.MVCCGetProto(
		ctx, stateRO, s.RangeTombstoneKey(), hlc.Timestamp{}, &ts, storage.MVCCGetOptions{},
	); err != nil || !ok {
		// NB: when err == nil && !ok, there is no RangeTombstone. It is valid to
		// return RangeTombstone{} with a zero NextReplicaID, signifying that there
		// hasn't been a single replica removed for the RangeID.
		return kvserverpb.RangeTombstone{}, err
	}
	return ts, nil
}

// SetRangeTombstone writes the RangeTombstone.
func (s StateLoader) SetRangeTombstone(
	ctx context.Context, stateWO StateWO, ts kvserverpb.RangeTombstone,
) error {
	// "Blind" because ms == nil and timestamp.IsEmpty().
	return storage.MVCCBlindPutProto(ctx, stateWO, s.RangeTombstoneKey(),
		hlc.Timestamp{}, &ts, storage.MVCCWriteOptions{})
}

// UninitializedReplicaState returns the ReplicaState of an uninitialized
// Replica with the given range ID. It is equivalent to StateLoader.Load from an
// empty storage.
func UninitializedReplicaState(rangeID roachpb.RangeID) kvserverpb.ReplicaState {
	return kvserverpb.ReplicaState{
		Desc:           &roachpb.RangeDescriptor{RangeID: rangeID},
		Lease:          &roachpb.Lease{},
		TruncatedState: nil, // Invariant: always nil.
		GCThreshold:    &hlc.Timestamp{},
		Stats:          &enginepb.MVCCStats{},
		GCHint:         &roachpb.GCHint{},
	}
}

// The rest is not technically part of ReplicaState.

// SynthesizeRaftState creates a Raft state which synthesizes HardState from
// pre-seeded data in the engine: the state machine state created by
// WriteInitialReplicaState on a split, and the existing HardState of an
// uninitialized replica.
//
// TODO(sep-raft-log): this is now only used in splits, when initializing a
// replica. Make the implementation straightforward, most of the stuff here is
// constant except the existing HardState.
func (s StateLoader) SynthesizeRaftState(ctx context.Context, stateRO StateRO, raftRW Raft) error {
	hs, err := s.LoadHardState(ctx, raftRW.RO)
	if err != nil {
		return err
	}
	as, err := s.LoadRangeAppliedState(ctx, stateRO)
	if err != nil {
		return err
	}
	applied := logstore.EntryID{
		Index: as.RaftAppliedIndex,
		Term:  as.RaftAppliedIndexTerm,
	}
	return s.SynthesizeHardState(ctx, raftRW.WO, hs, applied)
}
