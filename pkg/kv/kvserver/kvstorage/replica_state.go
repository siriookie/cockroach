// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// LoadedReplicaState represents the state of a Replica loaded from storage, and
// is used to initialize the in-memory Replica instance.
// TODO(pavelkalinnikov): integrate with kvstorage.Replica.
// - **生命周期**：瞬态，从 `Replica.Load()` 返回后立即用于初始化 `kvserver.Replica`
// - **作用域**：传递给 `kvserver.Replica` 的初始化参数
// - **语义**：**完整的 Replica 状态**，包含所有必要的字段
type LoadedReplicaState struct {
	ReplicaID   roachpb.ReplicaID
	LastEntryID logstore.EntryID
	ReplState   kvserverpb.ReplicaState
	TruncState  kvserverpb.RaftTruncatedState

	hardState raftpb.HardState
}

// LoadReplicaState loads the state necessary to create a Replica with the
// specified range descriptor, which can be either initialized or uninitialized.
// It also verifies replica state invariants.
// TODO(pavelkalinnikov): integrate with stateloader.
func LoadReplicaState(
	ctx context.Context,
	stateRO StateRO,
	raftRO RaftRO,
	storeID roachpb.StoreID,
	desc *roachpb.RangeDescriptor,
	replicaID roachpb.ReplicaID,
) (LoadedReplicaState, error) {
	sl := MakeStateLoader(desc.RangeID)
	id, err := sl.LoadRaftReplicaID(ctx, stateRO)
	if err != nil {
		return LoadedReplicaState{}, err
	}
	if loaded := id.ReplicaID; loaded != replicaID {
		return LoadedReplicaState{}, errors.AssertionFailedf(
			"r%d: loaded RaftReplicaID %d does not match %d", desc.RangeID, loaded, replicaID)
	}

	ls := LoadedReplicaState{ReplicaID: replicaID}
	if ls.hardState, err = sl.LoadHardState(ctx, raftRO); err != nil {
		return LoadedReplicaState{}, err
	}
	if ls.TruncState, err = sl.LoadRaftTruncatedState(ctx, raftRO); err != nil {
		return LoadedReplicaState{}, err
	}
	if ls.LastEntryID, err = sl.LoadLastEntryID(ctx, raftRO, ls.TruncState); err != nil {
		return LoadedReplicaState{}, err
	}
	if ls.ReplState, err = sl.Load(ctx, stateRO, desc); err != nil {
		return LoadedReplicaState{}, err
	}

	if err := ls.check(storeID); err != nil {
		return LoadedReplicaState{}, err
	}
	return ls, nil
}

func (r LoadedReplicaState) FullReplicaID() roachpb.FullReplicaID {
	return roachpb.FullReplicaID{RangeID: r.ReplState.Desc.RangeID, ReplicaID: r.ReplicaID}
}

// check makes sure that the replica invariants hold for the loaded state.
// 其核心作用是：在数据从磁盘加载到内存后，进行“生存校验”，确保该副本的数据没损坏且真的属于当前节点。
func (r LoadedReplicaState) check(storeID roachpb.StoreID) error {
	desc := r.ReplState.Desc
	if r.ReplicaID == 0 {
		return errors.AssertionFailedf("r%d: replicaID is 0", desc.RangeID)
	}

	if !desc.IsInitialized() {
		// An uninitialized replica must have an empty HardState.Commit at all
		// times. Failure to maintain this invariant indicates corruption. And yet,
		// we have observed this in the wild. See #40213.
		//针对那些尚未初始化（即只有 RangeID，还没有同步到数据的“空壳”副本）的检查：
		//
		//代码：if !desc.IsInitialized() { if hs.Commit != 0 { ... } }
		//逻辑：对于一个没初始化的副本，它的 Raft 硬状态（HardState）中的 Commit 索引必须是 0。
		//原因：如果你还没拿到数据（未初始化），但磁盘上却记录你已经提交了某些日志，这说明状态机发生了混乱或磁盘数据出现了非法篡改（Corruption）。
		if hs := r.hardState; hs.Commit != 0 {
			return errors.AssertionFailedf(
				"r%d/%d: non-zero HardState.Commit on uninitialized replica: %+v", desc.RangeID, r.ReplicaID, hs)
		}
		// TODO(pavelkalinnikov): assert r.lastIndex == 0?
		return nil
	}
	// desc.IsInitialized() == true

	// INVARIANT: a replica's RangeDescriptor always contains the local Store.
	if replDesc, ok := desc.GetReplicaDescriptor(storeID); !ok {
		return errors.AssertionFailedf("%+v does not contain local store s%d", desc, storeID)
	} else if replDesc.ReplicaID != r.ReplicaID {
		return errors.AssertionFailedf(
			"%+v does not contain replicaID %d for local store s%d", desc, r.ReplicaID, storeID)
	}
	return nil
}

// CreateUninitReplicaTODO is the plan for splitting CreateUninitializedReplica
// into cross-engine writes.
//
//  1. Log storage write (durable):
//     1.1. Write WAG node with the state machine mutation (2).
//  2. State machine mutation:
//     2.1. Write the new RaftReplicaID.
//
// TODO(sep-raft-log): support the status quo in which only 2.1 is written.
const CreateUninitReplicaTODO = 0

// CreateUninitializedReplica creates an uninitialized replica in storage.
// Returns kvpb.RaftGroupDeletedError if this replica can not be created
// because it has been deleted.
func CreateUninitializedReplica(
	ctx context.Context,
	stateRW State,
	raftRO RaftRO,
	storeID roachpb.StoreID,
	id roachpb.FullReplicaID,
) error {
	sl := MakeStateLoader(id.RangeID)
	// Before creating the replica, see if there is a tombstone or a newer replica
	// which would indicate that our replica has been removed.
	if ts, err := sl.LoadRangeTombstone(ctx, stateRW.RO); err != nil {
		return err
	} else if id.ReplicaID < ts.NextReplicaID {
		return &kvpb.RaftGroupDeletedError{}
	} else if rID, err := sl.LoadRaftReplicaID(ctx, stateRW.RO); err != nil {
		return err
	} else if rID.ReplicaID > id.ReplicaID {
		return &kvpb.RaftGroupDeletedError{}
	} else if rID.ReplicaID == id.ReplicaID {
		return nil // the replica already exists
	} else if rID.ReplicaID != 0 {
		// TODO(pav-kv): there is a replica with an older ReplicaID. We must destroy
		// it, and create a new one. Right now, the code falls through and writes
		// the new RaftReplicaID, but this replica can already have a non-empty
		// HardState. This is a bug.
	}

	// Write the RaftReplicaID for this replica. This is the only place in the
	// CockroachDB code that we are creating a new *uninitialized* replica.
	//
	// Before this point, raft and state machine state of this replica are
	// non-existent. The only RangeID-specific key that can be present is the
	// RangeTombstone inspected above.
	_ = CreateUninitReplicaTODO
	if err := sl.SetRaftReplicaID(ctx, stateRW.WO, id.ReplicaID); err != nil {
		return err
	}

	// Make sure that storage invariants for this uninitialized replica hold.
	uninitDesc := roachpb.RangeDescriptor{RangeID: id.RangeID}
	_, err := LoadReplicaState(ctx, stateRW.RO, raftRO, storeID, &uninitDesc, id.ReplicaID)
	return err
}
