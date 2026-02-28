// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
)

// runInitialScan will attempt to perform an initial data scan of all spans in
// the passed frontier that are below f.initialTimestamp.
//
// It will retry in the face of errors and will only return upon
// success, context cancellation, or an error handling function which indicates
// that an error is unrecoverable. The return value will be true if the context
// was canceled or if the OnInitialScanError function indicated that the
// RangeFeed should stop.
// 初始扫描解决的问题：RangeFeed 的 catchup scan 只能提供订阅时间点之后的变更。
// 但很多场景需要在订阅前先获取当前数据快照（如 settingswatcher 启动时需要全部当前设置值）。
func (f *RangeFeed) runInitialScan(
	ctx context.Context, n *log.EveryN, r *retry.Retry, frontier span.Frontier,
) (canceled bool) {
	// 将 KV 行转换为 RangeFeedValue 事件
	onValue := func(kv roachpb.KeyValue) {
		v := kvpb.RangeFeedValue{
			Key:   kv.Key,
			Value: kv.Value,
		}

		// Mark the data as occurring at the initial timestamp, which is the
		// timestamp at which it was read.
		//- 设置后（`useRowTimestampInInitialScan=true`）：保留行的真实 MVCC 时间戳。
		//    - 使用场景：需要精确知道每行最后更新时间的场景（如某些 CDC 应用）。
		if !f.useRowTimestampInInitialScan {
			//所有扫描结果的时间戳统一设为 initialTimestamp。
			v.Value.Timestamp = f.initialTimestamp
		}

		// Supply the value from the scan as also the previous value to avoid
		// indicating that the value was previously deleted.
		if f.withDiff {
			v.PrevValue = v.Value // 初始扫描无"上一个值"，用自身填充
			v.PrevValue.Timestamp = hlc.Timestamp{}
		}

		// It's something of a bummer that we must allocate a new value for each
		// of these but the contract doesn't indicate that the value cannot be
		// retained so we have to assume that the callback may retain the value.
		f.onValue(ctx, &v)
	}

	var onValues func(kvs []kv.KeyValue)
	if f.onValues != nil {
		onValues = func(kvs []kv.KeyValue) {
			f.onValues(ctx, kvs)
		}
	}

	// Ensure the frontier is compatible with scan parallelism, which could be
	// enabled mid-call since it is controlled via callback.\
	// 关键：将 frontier 包装为线程安全版本（支持并发扫描）
	//`MakeFrontier` 返回的 `btreeFrontier` **不是线程安全的**。初始扫描支持并发（`WithInitialScanParallelismFn`），多个 goroutine 可能同时调用 `OnSpanDone`，进而并发调用 `frontier.Forward()`。`MakeConcurrentFrontier` 包装了一个 `syncutil.Mutex`，使得并发 Forward 安全。
	//
	//扫描完成后，这个并发版 frontier 被丢弃，`run()` 中后续的访问仍使用原始的非并发 frontier（因为 processEvents 是单线程的）。
	frontier = span.MakeConcurrentFrontier(frontier)

	// Adjust the span completion callback to advance the frontier in addition to
	// calling the user's callback, if any. We don't need to undo after we're done
	// since it is never called again once we're done.
	// 包装 OnSpanDone：扫描完一个 span 后推进 frontier
	userSpanDoneCallback := f.scanConfig.OnSpanDone
	f.scanConfig.OnSpanDone = func(ctx context.Context, sp roachpb.Span) error {
		if userSpanDoneCallback != nil {
			if err := userSpanDoneCallback(ctx, sp); err != nil {
				return err
			}
		}
		advanced, err := frontier.Forward(sp, f.initialTimestamp)
		if err != nil {
			return err
		}
		if f.frontierVisitor != nil {
			f.frontierVisitor(ctx, advanced, frontier)
		}
		return nil
	}

	toScan := make(roachpb.Spans, 0, frontier.Len())
	// 重试循环
	// We will exit this for loop only when we return false for failed/cancelled
	// from inside the loop, or when we break due to the callback or the retry
	// helper returns false for Next (ctx cancel/retry limit).
	r.Reset()
	for r.Next() {
		// Figure out what spans are left to scan.
		toScan = toScan[:0]
		for sp, ts := range frontier.Entries() {
			if ts.IsEmpty() || ts.Less(f.initialTimestamp) {
				toScan = append(toScan, sp) // 只扫描未完成的 span
			}
		}

		// Scan the spans.
		if len(toScan) > 0 {
			if err := f.client.Scan(ctx, toScan, f.initialTimestamp, onValue, onValues, f.scanConfig); err != nil {
				if f.onInitialScanError != nil {
					if shouldStop := f.onInitialScanError(ctx, err); shouldStop {
						log.VEventf(ctx, 1, "stopping due to error: %v", err)
						break
					}
				}
				if n.ShouldLog() {
					log.Dev.Warningf(ctx, "failed to perform initial scan: %v", err)
				}
				continue
			}
		}
		// 全部完成
		// If we got here, we either had nothing to do or did it all; we're done.
		if f.onInitialScanDone != nil {
			f.onInitialScanDone(ctx)
		}
		return false // ctx 被取消
	}

	// We left the for loop without returning false, so we were cancelled.
	return true
}
