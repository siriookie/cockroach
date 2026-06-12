// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ----------------------------------------------------------------------------
// This files contains functions performing updates to a BatchRequest performed
// on the server-side, specifically after the request has been routed to a
// replica (and thus the request has been split based on range boundaries).
// As per the client.Sender contract, these function need to consider the input
// batches as copy-on-write.
// ----------------------------------------------------------------------------

// maybeStripInFlightWrites attempts to remove all point writes and query
// intents that ended up in the same batch as an EndTxn request from that EndTxn
// request's "in-flight" write set. The entire batch will commit atomically, so
// there is no need to consider the writes in the same batch concurrent.
//
// The transformation can lead to bypassing the STAGING state for a transaction
// entirely. This is possible if the function removes all of the in-flight
// writes from an EndTxn request that was committing in parallel with writes
// which all happened to be on the same range as the transaction record.
func maybeStripInFlightWrites(ba *kvpb.BatchRequest) (*kvpb.BatchRequest, error) {
	// 只处理带 EndTxn 的 batch。没有 EndTxn，就不涉及并行提交状态转换。
	args, hasET := ba.GetArg(kvpb.EndTxn)
	if !hasET {
		return ba, nil
	}

	et := args.(*kvpb.EndTxnRequest)
	// EndTxn 通常位于 batch 末尾，这里把 EndTxn 前面的请求都视为“同批次其它请求”。
	// 如果这些请求里有 EndTxn.InFlightWrites 对应的点写，就可以把它们剥离出来。
	otherReqs := ba.Requests[:len(ba.Requests)-1]
	// 只有 parallel commit 的 EndTxn 才有 InFlightWrites 语义。
	// 如果没有其它请求同批到达，也没有什么可以从 InFlightWrites 中剥离。
	if !et.IsParallelCommit() || len(otherReqs) == 0 {
		return ba, nil
	}

	// Clone the BatchRequest and the EndTxn request before modifying it. We nil
	// out the request's in-flight writes and make the lock spans immutable on
	// append. Code below can use origET to recreate the in-flight write set if
	// any elements remain in it.
	//
	// 这里遵守 client.Sender 的 copy-on-write 约定：
	// 不能直接改调用方传入的 BatchRequest / EndTxnRequest，
	// 所以先浅拷贝 EndTxn，再浅拷贝 BatchRequest 和 RequestUnion slice。
	origET := et
	etAlloc := new(struct {
		et    kvpb.EndTxnRequest
		union kvpb.RequestUnion_EndTxn
	})
	etAlloc.et = *origET // shallow copy
	etAlloc.union.EndTxn = &etAlloc.et
	et = &etAlloc.et
	// 先清空新 EndTxn 的 InFlightWrites，后面只把“仍不在当前 batch 中”的写加回来。
	et.InFlightWrites = nil
	// 把 LockSpans 的容量限制为当前长度，确保后续 append 不会修改 origET.LockSpans
	// 共享的底层数组。
	et.LockSpans = et.LockSpans[:len(et.LockSpans):len(et.LockSpans)] // immutable
	ba = ba.ShallowCopy()
	ba.Requests = append([]kvpb.RequestUnion(nil), ba.Requests...)
	// 用拷贝出来的 EndTxn 替换 batch 末尾的 EndTxn。
	ba.Requests[len(ba.Requests)-1].Value = &etAlloc.union

	// Fast-path: If we know that this batch contains all of the transaction's
	// in-flight writes, then we can avoid searching in the in-flight writes set
	// for each request. Instead, we can blindly merge all in-flight writes into
	// the lock spans and clear out the in-flight writes set.
	//
	// 快路径：如果当前 batch 中可能包含的写请求数量已经不少于 InFlightWrites 数量，
	// 就先数一下当前 batch 里可并行提交的点写 / QueryIntent 数量。
	// 如果数量刚好等于 InFlightWrites，说明所有 in-flight writes 都在当前 batch 中，
	// 可以一次性清空 InFlightWrites。
	if len(otherReqs) >= len(origET.InFlightWrites) {
		writes := 0
		for _, ru := range otherReqs {
			req := ru.GetInner()
			switch {
			case kvpb.CanParallelCommit(req):
				// Concurrent point write being committed in parallel.
				// 当前 batch 里和 EndTxn 一起提交的并行点写。
				writes++
			case req.Method() == kvpb.QueryIntent:
				// Earlier pipelined point write that hasn't been proven yet.
				// QueryIntent 用来证明早先 pipelined point write 是否成功。
				writes++
			default:
				// Ranged write or read. See below.
				// 范围写或读请求不属于 InFlightWrites 的点写集合。
			}
		}
		if len(origET.InFlightWrites) < writes {
			// 当前 batch 中声明的并行写数量比 EndTxn 里记录的 in-flight writes 还多，
			// 说明请求结构不一致，返回错误。
			return ba, errors.New("more write in batch with EndTxn than listed in in-flight writes")
		} else if len(origET.InFlightWrites) == writes {
			// 所有 in-flight writes 都在当前 batch 中。
			// 它们会和 EndTxn 在同一个 Replica 上原子执行，所以不再需要作为
			// “仍在飞行的写”记录在 EndTxn.InFlightWrites 里。
			et.LockSpans = make([]roachpb.Span, len(origET.LockSpans)+len(origET.InFlightWrites))
			copy(et.LockSpans, origET.LockSpans)
			for i, w := range origET.InFlightWrites {
				// 被剥离的 in-flight write 仍然需要进入 LockSpans，
				// 这样 EndTxn 的锁/intent 语义仍覆盖这些 key。
				et.LockSpans[len(origET.LockSpans)+i] = roachpb.Span{Key: w.Key}
			}
			// See below for why we set Header.DistinctSpans here.
			// 合并 LockSpans，同时设置 DistinctSpans，供后续并发控制判断 span 是否去重。
			et.LockSpans, ba.Header.DistinctSpans = roachpb.MergeSpans(et.LockSpans)
			return ba, nil
		}
	}

	// Slow-path: If not then we remove each transaction write in the batch from
	// the in-flight write set and merge it into the lock spans.
	//
	// 慢路径：当前 batch 只包含部分 in-flight writes。
	// 逐个扫描当前 batch 中的点写，把匹配到的写从 InFlightWrites 中移除，
	// 剩余没出现在当前 batch 中的写继续保留在 EndTxn.InFlightWrites。
	copiedTo := 0
	for _, ru := range otherReqs {
		req := ru.GetInner()
		seq := req.Header().Sequence
		switch {
		case kvpb.CanParallelCommit(req):
			// Concurrent point write being committed in parallel.
			// 只有可并行提交的点写才尝试从 InFlightWrites 中删除。
		case req.Method() == kvpb.QueryIntent:
			// Earlier pipelined point write that hasn't been proven yet. We
			// could remove from the in-flight writes set when we see these,
			// but doing so would prevent us from using the optimization we
			// have below where we rely on increasing sequence numbers for
			// each subsequent request.
			//
			// We already don't intend on sending QueryIntent requests in the
			// same batch as EndTxn requests because doing so causes a pipeline
			// stall, so this doesn't seem worthwhile to support.
			// 慢路径下不处理 QueryIntent，保持它对应的写仍在 InFlightWrites 中。
			continue
		default:
			// Ranged write or read. These can make it into the final batch with
			// a parallel committing EndTxn request if the entire batch issued
			// by DistSender lands on the same range. Skip.
			// 范围写或读可能和 EndTxn 同批到达，但它们不对应 InFlightWrites 中的点写。
			continue
		}

		// Remove the write from the in-flight writes set. We only need to
		// search from after the previously removed sequence number forward
		// because both the InFlightWrites and the Requests in the batch are
		// stored in increasing sequence order.
		//
		// Maintaining an iterator into the in-flight writes slice and scanning
		// instead of performing a binary search on each request changes the
		// complexity of this loop from O(n*log(m)) to O(m) where n is the
		// number of point writes in the batch and m is the number of in-flight
		// writes. These complexities aren't directly comparable, but copying
		// all unstripped writes back into et.InFlightWrites is already O(m),
		// so the approach here was preferred over repeat binary searches.
		//
		// InFlightWrites 和 batch Requests 都按 sequence 递增排列。
		// copiedTo 是当前已经处理到的 InFlightWrites 游标；
		// 后续只需要从 copiedTo 往后找匹配 sequence，避免每次从头二分/扫描。
		match := -1
		for i, w := range origET.InFlightWrites[copiedTo:] {
			if w.Sequence == seq {
				match = i + copiedTo
				break
			}
		}
		if match == -1 {
			// 当前 batch 中有一个并行提交点写，但 EndTxn.InFlightWrites 没记录它。
			// 这表示请求状态不一致，不能安全改写。
			return ba, errors.New("write in batch with EndTxn missing from in-flight writes")
		}
		w := origET.InFlightWrites[match]
		// copiedTo 到 match 之间的写没有出现在当前 batch 中，仍然是 in-flight，
		// 需要保留回新的 et.InFlightWrites。
		notInBa := origET.InFlightWrites[copiedTo:match]
		et.InFlightWrites = append(et.InFlightWrites, notInBa...)
		copiedTo = match + 1

		// Move the write to the lock spans set since it's no
		// longer being tracked in the in-flight write set.
		// 当前写已经和 EndTxn 同批原子执行，不再是 in-flight；
		// 但它的 key 仍要加入 LockSpans，保证锁覆盖范围正确。
		et.LockSpans = append(et.LockSpans, roachpb.Span{Key: w.Key})
	}
	if et != origET {
		// Finish building up the remaining in-flight writes.
		// 把最后一段还没复制的 in-flight writes 追加回去。
		notInBa := origET.InFlightWrites[copiedTo:]
		et.InFlightWrites = append(et.InFlightWrites, notInBa...)
		// Re-sort and merge the lock spans. We can set the batch request's
		// DistinctSpans flag based on whether any of in-flight writes in this
		// batch overlap with each other. This will have (rare) false negatives
		// when the in-flight writes overlap with existing lock spans, but never
		// false positives.
		// 重新合并 LockSpans，并更新 DistinctSpans。
		// 如果被移动过来的 in-flight writes 之间有重叠，MergeSpans 会发现；
		// 和原有 lock spans 的重叠可能产生 false negative，但不会产生 false positive。
		et.LockSpans, ba.Header.DistinctSpans = roachpb.MergeSpans(et.LockSpans)
	}
	return ba, nil
}

// maybeBumpReadTimestampToWriteTimestamp bumps the batch's read timestamp to
// the write timestamp for transactional batches where these timestamp have
// diverged and where bumping is possible. When possible, this allows the
// transaction to commit without having to retry.
//
// Returns true if the timestamp was bumped. Also returns the possibly updated
// batch request, which is shallow-copied on write.
//
// Note that this, like all the server-side bumping of the read timestamp, only
// works for batches that exclusively contain writes; reads cannot be bumped
// like this because they've already acquired timestamp-aware latches.
func maybeBumpReadTimestampToWriteTimestamp(
	ctx context.Context, ba *kvpb.BatchRequest, g *concurrency.Guard,
) (*kvpb.BatchRequest, bool) {
	if ba.Txn == nil {
		return ba, false
	}
	if !ba.CanForwardReadTimestamp {
		return ba, false
	}
	if ba.Txn.ReadTimestamp == ba.Txn.WriteTimestamp {
		return ba, false
	}
	arg, ok := ba.GetArg(kvpb.EndTxn)
	if !ok {
		return ba, false
	}
	et := arg.(*kvpb.EndTxnRequest)
	if batcheval.IsEndTxnExceedingDeadline(ba.Txn.WriteTimestamp, et.Deadline) {
		return ba, false
	}
	return tryBumpBatchTimestamp(ctx, ba, g, ba.Txn.WriteTimestamp)
}

// tryBumpBatchTimestamp attempts to bump ba's read and write timestamps to ts.
//
// This function is called both below and above latching, which is indicated by
// the concurrency guard argument. The concurrency guard, if not nil, indicates
// that the caller is holding latches and cannot adjust its timestamp beyond the
// limits of what is protected by those latches. If the concurrency guard is
// nil, the caller indicates that it is not holding latches and can therefore
// more freely adjust its timestamp because it will re-acquire latches at
// whatever timestamp the batch is bumped to.
//
// Returns true if the timestamp was bumped. Returns false if the timestamp
// could not be bumped. Also returns the possibly updated batch request, which
// is shallow-copied on write.
func tryBumpBatchTimestamp(
	ctx context.Context, ba *kvpb.BatchRequest, g *concurrency.Guard, ts hlc.Timestamp,
) (*kvpb.BatchRequest, bool) {
	if g != nil && !g.IsolatedAtLaterTimestamps() {
		return ba, false
	}
	if ts.Less(ba.Timestamp) {
		log.KvExec.Fatalf(ctx, "trying to bump to %s <= ba.Timestamp: %s", ts, ba.Timestamp)
	}
	if ba.Txn == nil {
		log.VEventf(ctx, 2, "bumping batch timestamp to %s from %s", ts, ba.Timestamp)
		ba = ba.ShallowCopy()
		ba.Timestamp = ts
		return ba, true
	}
	if ts.Less(ba.Txn.ReadTimestamp) || ts.Less(ba.Txn.WriteTimestamp) {
		log.KvExec.Fatalf(ctx, "trying to bump to %s inconsistent with ba.Txn.ReadTimestamp: %s, "+
			"ba.Txn.WriteTimestamp: %s", ts, ba.Txn.ReadTimestamp, ba.Txn.WriteTimestamp)
	}
	log.VEventf(ctx, 2, "bumping batch timestamp to: %s from read: %s, write: %s",
		ts, ba.Txn.ReadTimestamp, ba.Txn.WriteTimestamp)
	ba = ba.ShallowCopy()
	ba.Txn = ba.Txn.Clone()
	ba.Txn.BumpReadTimestamp(ts)
	ba.Timestamp = ba.Txn.ReadTimestamp // Refresh just updated ReadTimestamp
	return ba, true
}
