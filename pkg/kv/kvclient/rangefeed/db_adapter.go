// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// dbAdapter is an implementation of the DB interface using a real *kv.DB.
type dbAdapter struct {
	db         *kv.DB
	st         *cluster.Settings
	distSender *kvcoord.DistSender
}

var _ DB = (*dbAdapter)(nil)

var maxScanParallelism = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.rangefeed.max_scan_parallelism",
	"maximum number of concurrent scan requests that can be issued during initial scan",
	64,
)

// newDBAdapter construct a DB using a *kv.DB.
func newDBAdapter(db *kv.DB, st *cluster.Settings) (*dbAdapter, error) {
	var distSender *kvcoord.DistSender
	{
		txnWrapperSender, ok := db.NonTransactionalSender().(*kv.CrossRangeTxnWrapperSender)
		if !ok {
			return nil, errors.Errorf("failed to extract a %T from %T",
				(*kv.CrossRangeTxnWrapperSender)(nil), db.NonTransactionalSender())
		}
		distSender, ok = txnWrapperSender.Wrapped().(*kvcoord.DistSender)
		if !ok {
			return nil, errors.Errorf("failed to extract a %T from %T",
				(*kvcoord.DistSender)(nil), txnWrapperSender.Wrapped())
		}
	}
	return &dbAdapter{
		db:         db,
		st:         st,
		distSender: distSender,
	}, nil
}

// RangeFeed is part of the DB interface.
func (dbc *dbAdapter) RangeFeed(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	//StartAfter 是 exclusive（不包含该时间戳），即服务端只发送 ts > startFrom 的事件。
	//这与 runInitialScan 在 initialTimestamp 处扫描数据形成互补：
	//扫描获取 ts <= initialTimestamp 的快照，RangeFeed 获取 ts > initialTimestamp 的增量。
	timedSpans := make([]kvcoord.SpanTimePair, 0, len(spans))
	for _, sp := range spans {
		timedSpans = append(timedSpans, kvcoord.SpanTimePair{
			Span:       sp,
			StartAfter: startFrom, // 注意：exclusive
		})
	}
	return dbc.distSender.RangeFeed(ctx, timedSpans, eventC, opts...)
}

// RangeFeedFromFrontier is part of the DB interface.
func (dbc *dbAdapter) RangeFeedFromFrontier(
	ctx context.Context,
	frontier span.Frontier,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	timedSpans := make([]kvcoord.SpanTimePair, 0, frontier.Len())
	for sp, ts := range frontier.Entries() {
		timedSpans = append(timedSpans, kvcoord.SpanTimePair{
			// Clone the span as the rangefeed progress tracker will manipulate the
			// original frontier.
			//DistSender 内部会操作传入的 span（如截断、分割）。若不 clone，会直接修改 frontier 中的 span 底层字节，
			//导致 frontier 数据结构损坏（B-Tree 的 Key 被改变，但树的排序没有更新）。
			Span:       sp.Clone(), // 注意：必须 Clone！
			StartAfter: ts,
		})
	}
	return dbc.distSender.RangeFeed(ctx, timedSpans, eventC, opts...)
}

// Scan is part of the DB interface.
func (dbc *dbAdapter) Scan(
	ctx context.Context,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	rowsFn func([]kv.KeyValue),
	cfg scanConfig,
) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("expected at least 1 span, got none")
	}

	var acc *mon.ConcurrentBoundAccount
	if cfg.mon != nil {
		acc = cfg.mon.MakeConcurrentBoundAccount()
		defer acc.Close(ctx)
	} else {
		acc = mon.NewStandaloneUnlimitedConcurrentAccount()
	}
	// 无并行：串行扫描每个 span
	// If we don't have parallelism configured, just scan each span in turn.
	if cfg.scanParallelism == nil {
		for _, sp := range spans {
			if err := dbc.scanSpan(ctx, sp, asOf, rowFn, rowsFn, cfg.targetScanBytes, cfg.OnSpanDone, cfg.overSystemTable, acc); err != nil {
				return err
			}
		}
		return nil
	}
	// 有并行：用 divideAndSendScanRequests 按 Range 边界分割并并发扫描
	parallelismFn := cfg.scanParallelism
	if parallelismFn == nil {
		parallelismFn = func() int { return 1 }
	} else {
		highParallelism := log.Every(30 * time.Second)
		userSuppliedFn := parallelismFn
		parallelismFn = func() int {
			p := userSuppliedFn()
			if p < 1 {
				p = 1
			}
			maxP := int(maxScanParallelism.Get(&dbc.st.SV))
			if p > maxP {
				if highParallelism.ShouldLog() {
					log.Dev.Warningf(ctx,
						"high scan parallelism %d limited via 'kv.rangefeed.max_scan_parallelism' to %d", p, maxP)
				}
				p = maxP
			}
			return p
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g := ctxgroup.WithContext(ctx)
	err := dbc.divideAndSendScanRequests(
		ctx, &g, spans, asOf, rowFn, rowsFn,
		parallelismFn, cfg.targetScanBytes, cfg.OnSpanDone, cfg.overSystemTable, acc)
	if err != nil {
		cancel()
	}
	return errors.CombineErrors(err, g.Wait())
}

func (dbc *dbAdapter) scanSpan(
	ctx context.Context,
	span roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	rowsFn func([]kv.KeyValue),
	targetScanBytes int64,
	onScanDone OnScanCompleted,
	overSystemTable bool,
	acc *mon.ConcurrentBoundAccount,
) error {
	if acc != nil {
		if err := acc.Grow(ctx, targetScanBytes); err != nil {
			return err
		}
		defer acc.Shrink(ctx, targetScanBytes)
	}

	admissionPri := admissionpb.BulkNormalPri
	if overSystemTable {
		admissionPri = admissionpb.NormalPri
	}
	return dbc.db.TxnWithAdmissionControl(ctx,
		kvpb.AdmissionHeader_ROOT_KV,
		admissionPri, // BulkNormalPri 或 NormalPri
		kv.SteppingDisabled,
		func(ctx context.Context, txn *kv.Txn) error {
			if err := txn.SetFixedTimestamp(ctx, asOf); err != nil {
				return err
			} // as-of 查询
			sp := span
			var b kv.Batch
			for {
				//`TargetBytes = 512 KiB` 告诉服务端每次 RPC 最多返回 512 KiB 数据。若数据超过这个大小，服务端返回 `ResumeSpan` 指向下一页的起始 key。客户端更新 `sp = res.ResumeSpanAsValue()` 后继续下一轮 Scan，直到 `ResumeSpan == nil`。
				//
				//这避免了一次扫描可能拉取 GB 级数据导致 OOM 的问题。配合 `WithMemoryMonitor` 还可以控制内存预算。
				b.Header.TargetBytes = targetScanBytes // 512 KiB 分页
				b.Scan(sp.Key, sp.EndKey)
				if err := txn.Run(ctx, &b); err != nil {
					return err
				}
				res := b.Results[0]
				if rowsFn != nil {
					rowsFn(res.Rows)
				} else {
					for _, row := range res.Rows {
						rowFn(roachpb.KeyValue{Key: row.Key, Value: *row.Value})
					}
				}
				if res.ResumeSpan == nil {
					if onScanDone != nil {
						return onScanDone(ctx, sp)
					}
					return nil
				}

				if onScanDone != nil {
					if err := onScanDone(ctx, roachpb.Span{Key: sp.Key, EndKey: res.ResumeSpan.Key}); err != nil {
						return err
					}
				}

				sp = res.ResumeSpanAsValue()
				b = kv.Batch{}
			}
		})
}

// divideAndSendScanRequests divides spans into small ranges based on range boundaries,
// and adds those scan requests to the workGroup.  The caller is expected to wait for
// the workGroup completion, or to cancel the work group in case of an error.
func (dbc *dbAdapter) divideAndSendScanRequests(
	ctx context.Context,
	workGroup *ctxgroup.Group,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	rowsFn func(values []kv.KeyValue),
	parallelismFn func() int,
	targetScanBytes int64,
	onSpanDone OnScanCompleted,
	overSystemTable bool,
	acc *mon.ConcurrentBoundAccount,
) error {
	// Build a span group so that we can iterate spans in order.
	var sg roachpb.SpanGroup
	sg.Add(spans...)

	currentScanLimit := parallelismFn()
	exportLim := limit.MakeConcurrentRequestLimiter("rangefeedScanLimiter", parallelismFn())
	ri := kvcoord.MakeRangeIterator(dbc.distSender)

	for _, sp := range sg.Slice() {
		nextRS, err := keys.SpanAddr(sp)
		if err != nil {
			return err
		}

		for ri.Seek(ctx, nextRS.Key, kvcoord.Ascending); ri.Valid(); ri.Next(ctx) {
			desc := ri.Desc()
			partialRS, err := nextRS.Intersect(desc.RSpan())
			if err != nil {
				return err
			}
			nextRS.Key = partialRS.EndKey

			if newLimit := parallelismFn(); newLimit != currentScanLimit {
				currentScanLimit = newLimit
				exportLim.SetLimit(newLimit)
			}

			limAlloc, err := exportLim.Begin(ctx) // 获取并发令牌
			if err != nil {
				return err
			}

			sp := partialRS.AsRawSpanWithNoLocals()
			workGroup.GoCtx(func(ctx context.Context) error {
				defer limAlloc.Release()
				return dbc.scanSpan(ctx, sp, asOf, rowFn, rowsFn, targetScanBytes, onSpanDone, overSystemTable, acc)
			})

			if !ri.NeedAnother(nextRS) {
				break
			}
		}
		if err := ri.Error(); err != nil {
			return ri.Error()
		}
	}

	return nil
}
