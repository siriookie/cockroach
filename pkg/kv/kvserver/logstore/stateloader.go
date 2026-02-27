// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// StateLoader gives access to read or write the state of the Raft log. It
// contains an internal buffer which is reused to avoid an allocation on
// frequently-accessed code paths.
//
// Because of this internal buffer, this struct is not safe for concurrent use,
// and the return values of methods that return keys are invalidated the next
// time any method is called.
//
// It is safe to have multiple state loaders for the same replica. Reusable
// loaders are typically found in a struct with a mutex, and temporary loaders
// may be created when locking is less desirable than an allocation.
//
// TODO(pavelkalinnikov): understand the split between logstore and raftlog
// packages, reshuffle or merge them, including this StateLoader.
type StateLoader struct {
	keys.RangeIDPrefixBuf
}

// NewStateLoader creates a log StateLoader for the given range.
func NewStateLoader(rangeID roachpb.RangeID) StateLoader {
	return StateLoader{
		RangeIDPrefixBuf: keys.MakeRangeIDPrefixBuf(rangeID),
	}
}

// EntryID is an (index, term) pair identifying a raft log entry.
//
// TODO(pav-kv): should be the other way around - RaftTruncatedState is an
// EntryID.
type EntryID = kvserverpb.RaftTruncatedState

// LoadLastEntryID loads the ID of the last entry in the raft log. Returns the
// passed in RaftTruncatedState if the log has no entries. RaftTruncatedState
// must have been just read, or otherwise exist in memory and be consistent with
// the content of the log.
func (sl StateLoader) LoadLastEntryID(
	ctx context.Context, reader storage.Reader, ts kvserverpb.RaftTruncatedState,
) (EntryID, error) {
	//prefix := sl.RaftLogPrefix()：它首先计算出当前 Range 的 Raft Log 在存储引擎中的 Key 前缀。所有的 Raft Log 条目都存储在这个前缀下。
	//例如：/Local/RangeID/<id>/RaftLog/。
	prefix := sl.RaftLogPrefix()
	// NB: raft log has no intents.
	//它向底层存储（Pebble）申请一个迭代器。
	//LowerBound: prefix：这告诉迭代器，我们只关心这个前缀之后的数据，优化查询性能。
	iter, err := reader.NewMVCCIterator(
		ctx, storage.MVCCKeyIterKind, storage.IterOptions{
			LowerBound: prefix, ReadCategory: fs.ReplicationReadCategory})
	if err != nil {
		return EntryID{}, err
	}
	defer iter.Close()

	var last EntryID
	//它构造了一个指向该前缀下 理论上最大可能 Key（RaftLogKeyFromPrefix(prefix, math.MaxUint64)）的 Key。
	//然后它让迭代器去寻找 小于 (Less Than) 这个最大 Key 的第一条记录。
	//因为数据库中的 Key 是有序的，查找“小于无穷大的最大值”实际上就是在找该前缀下的 最后一条记录。
	iter.SeekLT(storage.MakeMVCCMetadataKey(keys.RaftLogKeyFromPrefix(prefix, math.MaxUint64)))
	if ok, _ := iter.Valid(); ok {
		key := iter.UnsafeKey().Key
		if len(key) < len(prefix) {
			log.KvExec.Fatalf(ctx, "unable to decode Raft log index key: len(%s) < len(%s)", key.String(), prefix.String())
		}
		suffix := key[len(prefix):]
		var err error
		last.Index, err = keys.DecodeRaftLogKeyFromSuffix(suffix)
		if err != nil {
			log.KvExec.Fatalf(ctx, "unable to decode Raft log index key: %s; %v", key.String(), err)
		}
		v, err := iter.UnsafeValue()
		if err != nil {
			log.KvExec.Fatalf(ctx, "unable to read Raft log entry %d (%s): %v", last.Index, key.String(), err)
		}
		entry, err := raftlog.RaftEntryFromRawValue(v)
		if err != nil {
			log.KvExec.Fatalf(ctx, "unable to decode Raft log entry %d (%s): %v", last.Index, key.String(), err)
		}
		last.Term = kvpb.RaftTerm(entry.Term)
	}
	//如果没找到任何记录（last.Index == 0）：这意味着日志是空的。
	//原因可能是：这还是个新 Range，或者所有的日志都已经被截断（Truncated）并转为快照了。
	//此时，它会直接返回传入的
	//ts
	// (TruncatedState)。因为如果日志为空，那么逻辑上的“最后一条日志”其实就是截断点的信息（快照中的 Index 和 Term）。
	if last.Index == 0 {
		// The log is empty, which means we are either starting from scratch
		// or the entire log has been truncated away.
		return ts, nil
	}
	return last, nil
}

// LoadRaftTruncatedState loads the truncated state.
// 构造 Key：
// 调用 sl.RaftTruncatedStateKey() 生成该 Range 对应的截断状态 Key。
// 这个 Key 是 Range 专有的本地 Key（Range Local Key）。
// 读取数据 (MVCCGetProto)：
// 调用 storage.MVCCGetProto 从传入的 reader（通常是 Pebble 引擎的 Reader）中读取数据。
// 忽略时间戳：传入了 hlc.Timestamp{}（空时间戳），这在读取 Range Local 数据时通常意味着直接读取最新值（因为这些元数据通常不使用 MVCC多版本并发控制，或者是 Blind Put 更新的）。
// 反序列化：将读取到的 Value 反序列化为 kvserverpb.RaftTruncatedState 结构体。
// 返回结果：
// 如果是新副本或者从未发生过截断，这通常会返回零值（Index=0, Term=0）。
// 如果发生过截断，返回的结构体包含：
// Index: 被截断丢弃的最后一条日志的 Index。
// Term: 被截断日志中最后一条日志的 Term。
func (sl StateLoader) LoadRaftTruncatedState(
	ctx context.Context, reader storage.Reader,
) (kvserverpb.RaftTruncatedState, error) {
	var truncState kvserverpb.RaftTruncatedState
	if _, err := storage.MVCCGetProto(
		ctx, reader, sl.RaftTruncatedStateKey(), hlc.Timestamp{}, &truncState,
		storage.MVCCGetOptions{ReadCategory: fs.ReplicationReadCategory},
	); err != nil {
		return kvserverpb.RaftTruncatedState{}, err
	}
	return truncState, nil
}

// SetRaftTruncatedState overwrites the truncated state.
func (sl StateLoader) SetRaftTruncatedState(
	ctx context.Context, writer storage.Writer, truncState *kvserverpb.RaftTruncatedState,
) error {
	if (*truncState == kvserverpb.RaftTruncatedState{}) {
		return errors.New("cannot persist empty RaftTruncatedState")
	}
	// "Blind" because opts.Stats == nil and timestamp.IsEmpty().
	return storage.MVCCBlindPutProto(
		ctx,
		writer,
		sl.RaftTruncatedStateKey(),
		hlc.Timestamp{}, /* timestamp */
		truncState,
		storage.MVCCWriteOptions{}, /* txn */
	)
}

// ClearRaftTruncatedState clears the RaftTruncatedState.
func (sl StateLoader) ClearRaftTruncatedState(writer storage.Writer) error {
	return writer.ClearUnversioned(sl.RaftTruncatedStateKey(), storage.ClearOptions{})
}

// LoadHardState loads the HardState.
func (sl StateLoader) LoadHardState(
	ctx context.Context, reader storage.Reader,
) (raftpb.HardState, error) {
	var hs raftpb.HardState
	found, err := storage.MVCCGetProto(ctx, reader, sl.RaftHardStateKey(),
		hlc.Timestamp{}, &hs, storage.MVCCGetOptions{ReadCategory: fs.ReplicationReadCategory})

	if !found || err != nil {
		return raftpb.HardState{}, err
	}
	return hs, nil
}

// SetHardState overwrites the HardState.
func (sl StateLoader) SetHardState(
	ctx context.Context, writer storage.Writer, hs raftpb.HardState,
) error {
	// "Blind" because opts.Stats == nil and timestamp.IsEmpty().
	return storage.MVCCBlindPutProto(
		ctx,
		writer,
		sl.RaftHardStateKey(),
		hlc.Timestamp{}, /* timestamp */
		&hs,
		storage.MVCCWriteOptions{}, /* opts */
	)
}

// SynthesizeHardState synthesizes an on-disk HardState from the given input,
// taking care that a HardState compatible with the existing data is written.
func (sl StateLoader) SynthesizeHardState(
	ctx context.Context, writer storage.Writer, oldHS raftpb.HardState, applied EntryID,
) error {
	newHS := raftpb.HardState{
		Term: uint64(applied.Term),
		// NB: when applying a Raft snapshot, the applied index is equal to the
		// Commit index represented by the snapshot.
		Commit: uint64(applied.Index),
	}

	if oldHS.Commit > newHS.Commit {
		return errors.Newf("can't decrease HardState.Commit from %d to %d",
			redact.Safe(oldHS.Commit), redact.Safe(newHS.Commit))
	}

	// TODO(arul): This function can be called with an empty OldHS. In all other
	// cases, where a term is included, we should be able to assert that the term
	// isn't regressing (i.e. oldHS.Term >= newHS.Term).

	if oldHS.Term > newHS.Term {
		// The existing HardState is allowed to be ahead of us, which is
		// relevant in practice for the split trigger. We already checked above
		// that we're not rewinding the acknowledged index, and we haven't
		// updated votes yet.
		newHS.Term = oldHS.Term
	}
	// If the existing HardState voted in this term and knows who the leader is,
	// remember that.
	if oldHS.Term == newHS.Term {
		newHS.Vote = oldHS.Vote
		newHS.Lead = oldHS.Lead
		newHS.LeadEpoch = oldHS.LeadEpoch
	}
	err := sl.SetHardState(ctx, writer, newHS)
	return errors.Wrapf(err, "writing HardState %+v", &newHS)
}
