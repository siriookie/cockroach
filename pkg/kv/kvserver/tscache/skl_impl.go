// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tscache

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// sklImpl implements the Cache interface. It maintains a collection of
// skiplists containing keys or key ranges and the timestamps at which
// they were most recently read or written. If a timestamp was read or
// written by a transaction, the txn ID is stored with the timestamp to
// avoid advancing timestamps on successive requests from the same
// transaction.
type sklImpl struct {
	cache   *intervalSkl
	clock   *hlc.Clock
	metrics Metrics
}

var _ Cache = &sklImpl{}

// newSklImpl returns a new treeImpl with the supplied hybrid clock.
func newSklImpl(clock *hlc.Clock) *sklImpl {
	tc := sklImpl{clock: clock, metrics: makeMetrics()}
	tc.clear(clock.Now()) // 初始化底层 intervalSkl，设置 floorTS
	return &tc
}

// clear clears the cache and resets the low-water mark.
// lowWater：新的低水位时间戳，所有查找操作返回的时间戳至少为此值
func (tc *sklImpl) clear(lowWater hlc.Timestamp) {
	tc.cache = newIntervalSkl(tc.clock, MinRetentionWindow, tc.metrics.Skl)
	tc.cache.floorTS = lowWater
}

// Add implements the Cache interface.
// 点写入：len(end) == 0 → 调用Add()，记录单个Key
// - 范围写入：len(end) > 0 → 调用AddRange()，记录Key Range
func (tc *sklImpl) Add(
	ctx context.Context, start, end roachpb.Key, ts hlc.Timestamp, txnID uuid.UUID,
) {
	start, end = tc.boundKeyLengths(start, end) // 防止 Key 过长

	val := cacheValue{ts: ts, txnID: txnID}
	if len(end) == 0 {
		tc.cache.Add(ctx, nonNil(start), val)
	} else {
		tc.cache.AddRange(ctx, nonNil(start), end, excludeTo, val)
	}
}

// getLowWater implements the Cache interface.
// 返回缓存的低水位时间戳（floor timestamp）。
func (tc *sklImpl) getLowWater() hlc.Timestamp {
	return tc.cache.FloorTS()
}

// GetMax implements the Cache interface.
// 查询给定 Key 或 Key Range 的最大时间戳。
func (tc *sklImpl) GetMax(ctx context.Context, start, end roachpb.Key) (hlc.Timestamp, uuid.UUID) {
	var val cacheValue
	if len(end) == 0 {
		val = tc.cache.LookupTimestamp(ctx, nonNil(start))
	} else {
		val = tc.cache.LookupTimestampRange(ctx, nonNil(start), end, excludeTo)
	}
	return val.ts, val.txnID //如果该时间戳属于单个事务，返回事务 ID；否则返回 noTxnID
}

// Serialize implements the Cache interface.
// 将指定 Key Range 的缓存内容序列化为可传输的格式。
//
//	type Segment struct {
//	   LowWater  hlc.Timestamp  // 低水位时间戳
//	   ReadSpans []ReadSpan     // 读取跨度列表
//	}
//
//	type ReadSpan struct {
//	   Key       roachpb.Key
//	   EndKey    roachpb.Key    // 如果为空，表示单点
//	   Timestamp hlc.Timestamp
//	   TxnID     uuid.UUID
//	}
func (tc *sklImpl) Serialize(ctx context.Context, start, end roachpb.Key) rspb.Segment {
	if len(end) == 0 {
		end = start.Next() // 单 Key 转换为 [start, start.Next())
	}
	return tc.cache.Serialize(ctx, nonNil(start), end)
}

// boundKeyLengths makes sure that the key lengths provided are well below the
// size of each sklPage, otherwise we'll never be successful in adding it to
// an intervalSkl.
// 限制 Key 长度，防止过长的 Key 导致 Arena 溢出。
func (tc *sklImpl) boundKeyLengths(start, end roachpb.Key) (roachpb.Key, roachpb.Key) {
	// We bound keys to 1/32 of the page size. These could be slightly larger
	// and still not trigger the "key range too large" panic in intervalSkl,
	// but anything larger could require multiple page rotations before it's
	// able to fit in if other ranges are being added concurrently.
	maxKeySize := int(maximumSklPageSize / 32) // 32MB / 32 = 1MB

	// If either key is too long, truncate its length, making sure to always
	// grow the [start,end) range instead of shrinking it. This will reduce the
	// precision of the entry in the cache, which could allow independent
	// requests to interfere, but will never permit consistency anomalies.
	if l := len(start); l > maxKeySize {
		start = start[:maxKeySize]
		log.KvExec.Warningf(context.TODO(), "start key with length %d exceeds maximum key length of %d; "+
			"losing precision in timestamp cache", l, maxKeySize)
	}
	if l := len(end); l > maxKeySize {
		end = end[:maxKeySize].PrefixEnd() // PrefixEnd to grow range
		log.KvExec.Warningf(context.TODO(), "end key with length %d exceeds maximum key length of %d; "+
			"losing precision in timestamp cache", l, maxKeySize)
	}
	return start, end
}

// Metrics implements the Cache interface.
func (tc *sklImpl) Metrics() Metrics {
	return tc.metrics
}

// intervalSkl doesn't handle nil keys the same way as empty keys. Cockroach's
// KV API layer doesn't make a distinction.
var emptyStartKey = []byte("")

func nonNil(b []byte) []byte {
	if b == nil {
		return emptyStartKey
	}
	return b
}
