// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

//go:generate mockgen -destination=mocks_generated_test.go --package=rangefeed . DB

// TODO(ajwerner): Expose hooks for metrics.
// TODO(ajwerner): Expose access to checkpoints and the frontier.
// TODO(ajwerner): Expose better control over how the exponential backoff gets
// reset when the feed has been running successfully for a while.
// TODO(yevgeniy): Instead of rolling our own logic to parallelize scans, we should
// use streamer API instead (https://github.com/cockroachdb/cockroach/pull/68430)

// DB is an adapter to the underlying KV store.
type DB interface {

	// RangeFeed runs a rangefeed on a given span with the given arguments.
	// It encapsulates the RangeFeed method on roachpb.Internal.
	RangeFeed(
		ctx context.Context,
		spans []roachpb.Span,
		startFrom hlc.Timestamp,
		eventC chan<- kvcoord.RangeFeedMessage,
		opts ...kvcoord.RangeFeedOption,
	) error

	// RangefeedFromFrontier runs a rangefeed on the frontier's spans, and
	// starting at each span's associated timestamp.
	RangeFeedFromFrontier(
		ctx context.Context,
		frontier span.Frontier,
		eventC chan<- kvcoord.RangeFeedMessage,
		opts ...kvcoord.RangeFeedOption,
	) error

	// Scan encapsulates scanning a key span at a given point in time. The method
	// deals with pagination, calling the caller back for each row. Note that
	// the API does not require that the rows be ordered to allow for future
	// parallelism.
	Scan(
		ctx context.Context,
		spans []roachpb.Span,
		asOf hlc.Timestamp,
		rowFn func(value roachpb.KeyValue),
		rowsFn func([]kv.KeyValue),
		cfg scanConfig,
	) error
}

// Factory is used to construct RangeFeeds.
type Factory struct {
	stopper *stop.Stopper // 生命周期管理
	client  DB            // 向下层对接的接口（可 mock）
	knobs   *TestingKnobs // 测试注入点
}

// TestingKnobs is used to inject behavior into a rangefeed for testing.
type TestingKnobs struct {

	// OnRangefeedRestart is called when a rangefeed restarts.
	OnRangefeedRestart func()

	// IgnoreOnDeleteRangeError will ignore any errors where a DeleteRange event
	// is emitted without an OnDeleteRange handler. This can be used e.g. with
	// StoreTestingKnobs.GlobalMVCCRangeTombstone, to prevent the global tombstone
	// causing rangefeed errors for consumers who don't expect it.
	IgnoreOnDeleteRangeError bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)

// NewFactory constructs a new Factory.
func NewFactory(
	stopper *stop.Stopper, db *kv.DB, st *cluster.Settings, knobs *TestingKnobs,
) (*Factory, error) {
	kvDB, err := newDBAdapter(db, st)
	if err != nil {
		return nil, err
	}
	return newFactory(stopper, kvDB, knobs), nil
}

func newFactory(stopper *stop.Stopper, client DB, knobs *TestingKnobs) *Factory {
	return &Factory{
		stopper: stopper,
		client:  client,
		knobs:   knobs,
	}
}

// RangeFeed constructs a new rangefeed and runs it in an async task.
//
// The rangefeed can be stopped via Close(); otherwise, it will stop when the
// server shuts down. The only error which can be returned will indicate that
// the server is being shut down.
//
// Rangefeeds do not support inline (unversioned) values, and may omit them or
// error on them. Similarly, rangefeeds will error if MVCC history is mutated
// via e.g. ClearRange. Do not use rangefeeds across such key spans.
//
// NB: for the rangefeed itself, initialTimestamp is exclusive, i.e. the first
// possible event emitted by the server (including the catchup scan) is at
// initialTimestamp.Next(). This follows from the gRPC API semantics. However,
// the initial scan (if any) is run at initialTimestamp.
func (f *Factory) RangeFeed(
	ctx context.Context,
	name string,
	spans []roachpb.Span,
	initialTimestamp hlc.Timestamp,
	onValue OnValue,
	options ...Option,
) (_ *RangeFeed, err error) {
	r := f.New(name, initialTimestamp, onValue, options...)
	if err := r.Start(ctx, spans); err != nil {
		return nil, err
	}
	return r, nil
}

// New constructs a new RangeFeed (without running it).
func (f *Factory) New(
	name string, initialTimestamp hlc.Timestamp, onValue OnValue, options ...Option,
) *RangeFeed {
	r := RangeFeed{
		client:  f.client,
		stopper: f.stopper,
		knobs:   f.knobs,

		initialTimestamp: initialTimestamp,
		name:             name,
		onValue:          onValue,
	}
	initConfig(&r.config, options)
	return &r
}

// OnValue is called for each rangefeed value.
type OnValue func(ctx context.Context, value *kvpb.RangeFeedValue)

// OnValue is called for a batch of rangefeed values.
type OnValues func(ctx context.Context, values []kv.KeyValue)

// RangeFeed represents a running RangeFeed.
type RangeFeed struct {
	config  // 所有 Option 配置
	name    string
	client  DB
	stopper *stop.Stopper
	knobs   *TestingKnobs

	initialTimestamp hlc.Timestamp  // 订阅起始时间（exclusive）
	spans            []roachpb.Span // 监听范围
	spansDebugStr    string         // Debug string describing spans

	onValue OnValue

	cancel  context.CancelFunc // 停止信号
	running sync.WaitGroup     // 等待后台 goroutine 退出
	started int32              // 一次性启动保护
}

// Start kicks off the rangefeed in an async task, it can only be invoked once.
// All the installed callbacks (OnValue, OnCheckpoint, OnFrontierAdvance,
// OnInitialScanDone) are called in said async task in a single thread.
func (f *RangeFeed) Start(ctx context.Context, spans []roachpb.Span) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("expected at least 1 span, got none")
	}

	// Maintain a frontier in order to resume at a reasonable timestamp.
	// TODO(ajwerner): Consider exposing the frontier through a RangeFeed method.
	// Doing so would require some synchronization.
	frontier, err := span.MakeFrontier(spans...)
	if err != nil {
		return err
	}
	return f.start(ctx, frontier, true, false)
}

// StartFromFrontier is like Start but allows passing a frontier containing the
// spans on which to create the feed, which can reflect any previous progress,
// unlike passing just the spans to Start().
//
// The rangefeed takes ownership of the passed frontier until it is closed; the
// caller must not interact with it until Close returns. The caller remains
// responsible for releasing the frontier thereafter however.
func (f *RangeFeed) StartFromFrontier(ctx context.Context, frontier span.Frontier) error {
	return f.start(ctx, frontier, false, true)
}

func (f *RangeFeed) start(
	ctx context.Context, frontier span.Frontier, ownsFrontier bool, resumeFromFrontier bool,
) error {
	//- `started` 是 `int32`，用 atomic CAS 保证只有一个调用者能从 0 改为 1。
	//- 为什么不用 `sync.Once`？因为 `sync.Once` 不会返回错误；而这里需要通知调用方”你调用错了”。
	if !atomic.CompareAndSwapInt32(&f.started, 0, 1) {
		return errors.AssertionFailedf("rangefeed already started")
	}

	// Frontier merges and de-dups passed in spans.  So, use frontier to initialize
	// sorted list of spans.
	for sp := range frontier.Entries() {
		f.spans = append(f.spans, sp)
	}

	runWithFrontier := func(ctx context.Context) {
		if ownsFrontier {
			//ownsFrontier=true，RangeFeed 自己创建 frontier，用完自己释放（将 B-Tree 节点归还对象池）。
			defer frontier.Release()
		}
		// pprof.Do function does exactly what we do here, but it also results in
		// pprof.Do function showing up in the stack traces -- so, just set and reset
		// labels manually.
		//- 在所有由这个 RangeFeed 产生的 goroutine 调用栈上打上 `rangefeed=<name>` 标签。
		//- 在 `go tool pprof` 中可以用这些标签过滤，精确定位哪个订阅者在消耗 CPU。
		ctx, reset := pprofutil.SetProfilerLabels(
			ctx, append(f.extraPProfLabels, "rangefeed", f.name)...,
		)
		//因为 Go 的 goroutine 是复用的（尤其是 net/http server、gRPC、worker pool），
		//如果不恢复标签，后续请求/任务就会继承上一个请求的标签，导致 profile 彻底混乱（“标签污染”）。
		defer reset()

		if f.invoker != nil {
			_ = f.invoker(func() error {
				f.run(ctx, frontier, resumeFromFrontier)
				return nil
			})
			return
		}
		f.run(ctx, frontier, resumeFromFrontier)
	}

	if l := frontier.Len(); l == 1 {
		f.spansDebugStr = frontier.PeekFrontierSpan().String()
	} else {
		var buf strings.Builder
		for sp := range frontier.Entries() {
			if buf.Len() > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(sp.String())
			if buf.Len() >= 400 {
				fmt.Fprintf(&buf, "… [%d spans]", l)
				break
			}
		}
		f.spansDebugStr = buf.String()
	}

	ctx = logtags.AddTag(ctx, "rangefeed", f.name)
	ctx, f.cancel = f.stopper.WithCancelOnQuiesce(ctx)
	f.running.Add(1)
	if err := f.stopper.RunAsyncTask(ctx, "rangefeed", runWithFrontier); err != nil {
		f.cancel()
		f.running.Done()
		return err
	}
	return nil
}

// Close closes the RangeFeed and waits for it to shut down; it does so
// idempotently. It waits for the currently running handler, if any, to complete
// and guarantees that no future handlers will be invoked after this point.
func (f *RangeFeed) Close() {
	f.cancel()
	f.running.Wait()
}

// Run the rangefeed in a loop in the case of failure, likely due to node
// failures or general unavailability. If the rangefeed runs successfully for at
// least this long, then after subsequent failures we would like to reset the
// exponential backoff to experience long delays between retry attempts.
// This is the threshold of successful running after which the backoff state
// will be reset.
const resetThreshold = 30 * time.Second

// run will run the RangeFeed until the context is canceled or if the client
// indicates that an initial scan error is non-recoverable. The
// resumeWithFrontier arg enables the client to resume the rangefeed using the
// span frontier instead of from the frontier's low water mark.
func (f *RangeFeed) run(ctx context.Context, frontier span.Frontier, resumeWithFrontier bool) {
	defer f.running.Done()
	r := retry.StartWithCtx(ctx, f.retryOptions)
	restartLogEvery := log.Every(10 * time.Second)
	// ─── 阶段 1：初始扫描（仅首次，阻塞完成） ───
	//withInitialScan=true:
	//    runInitialScan() 完成后，frontier 中每个 span 的 ts = initialTimestamp
	//    → 下一次 RangeFeed 从 initialTimestamp+1 开始（catchup scan 区间 = 空集）
	//
	//withInitialScan=false, resumeWithFrontier=false:
	//    手动 frontier.Forward(sp, initialTimestamp)
	//    → 和上面等价，但跳过了数据扫描
	//
	//resumeWithFrontier=true (StartFromFrontier 路径):
	//    frontier 已由调用方设置好每个 span 的时间戳
	//    → RangeFeed 从各 span 自己的时间戳开始，支持分 span 进度恢复
	if f.withInitialScan {
		// runInitialScan() 完成后，frontier 中每个 span 的 ts = initialTimestamp
		if failed := f.runInitialScan(ctx, &restartLogEvery, &r, frontier); failed {
			return
		}
	} else if !resumeWithFrontier {
		// 没有初始扫描，手动将 frontier 推进到 initialTimestamp
		for _, sp := range f.spans {
			if _, err := frontier.Forward(sp, f.initialTimestamp); err != nil {
				if fn := f.onUnrecoverableError; fn != nil {
					fn(ctx, err)
				}
				return
			}
		}
	}

	// Check the context before kicking off a rangefeed.
	if ctx.Err() != nil {
		return
	}

	// TODO(ajwerner): Consider adding event buffering. Doing so would require
	// draining when the rangefeed fails.
	// ─── 阶段 2：构建 RangeFeed 选项 ───
	eventCh := make(chan kvcoord.RangeFeedMessage)

	var rangefeedOpts []kvcoord.RangeFeedOption
	// We can unconditionally enable bulk-delivery from the server at least as far
	// as to this client; if an onValues is configured we can also bulk-process
	// values, but even if it isn't we know how to unwrap a bulk delivery and pass
	// each event to the caller's individual event handlers.
	//告诉服务端：可以将多个事件打包为一个 BulkEvents 消息发送，减少 channel 写入次数和反序列化开销。
	rangefeedOpts = append(rangefeedOpts, kvcoord.WithBulkDelivery())

	if f.scanConfig.overSystemTable {
		rangefeedOpts = append(rangefeedOpts, kvcoord.WithSystemTablePriority())
	}
	if f.withDiff {
		rangefeedOpts = append(rangefeedOpts, kvcoord.WithDiff())
	}
	if f.withFiltering {
		rangefeedOpts = append(rangefeedOpts, kvcoord.WithFiltering())
	}
	if len(f.withMatchingOriginIDs) != 0 {
		rangefeedOpts = append(rangefeedOpts, kvcoord.WithMatchingOriginIDs(f.withMatchingOriginIDs...))
	}
	if f.onMetadata != nil {
		rangefeedOpts = append(rangefeedOpts, kvcoord.WithMetadata())
	}
	rangefeedOpts = append(rangefeedOpts, kvcoord.WithConsumerID(f.consumerID))
	// ─── 阶段 3：重试主循环 ───
	for i := 0; r.Next(); i++ {
		ts := frontier.Frontier()
		if log.ExpensiveLogEnabled(ctx, 1) {
			log.Eventf(ctx, "starting rangefeed from %v on %v", ts, f.spansDebugStr)
		}

		start := crtime.NowMono()

		rangeFeedTask := func(ctx context.Context) error {
			if f.invoker == nil {
				return f.client.RangeFeed(ctx, f.spans, ts, eventCh, rangefeedOpts...)
			}
			return f.invoker(func() error {
				return f.client.RangeFeed(ctx, f.spans, ts, eventCh, rangefeedOpts...)
			})
		}

		if resumeWithFrontier {
			rangeFeedTask = func(ctx context.Context) error {
				if f.invoker == nil {
					return f.client.RangeFeedFromFrontier(ctx, frontier, eventCh, rangefeedOpts...)
				}
				return f.invoker(func() error {
					return f.client.RangeFeedFromFrontier(ctx, frontier, eventCh, rangefeedOpts...)
				})
			}
		}
		processEventsTask := func(ctx context.Context) error {
			if f.invoker != nil {
				return f.invoker(func() error {
					return f.processEvents(ctx, frontier, eventCh)
				})
			}
			return f.processEvents(ctx, frontier, eventCh)
		}
		// ─── 阶段 4：错误分类处理 ───
		//1. 将两个 task 在同一个子 ctx 下并发运行。
		//2. 任意一个 task 返回非 nil 错误时，取消子 ctx，等待另一个 task 也退出。
		//3. 返回第一个出现的非 nil 错误（或若都成功则返回 nil）。
		err := ctxgroup.GoAndWait(ctx, rangeFeedTask, processEventsTask)
		//MVCC 历史已被 GC 删除，无法从该时间点重建事件流
		if errors.HasType(err, &kvpb.BatchTimestampBeforeGCError{}) ||
			//历史被物理破坏，订阅者无法获得正确的事件序列
			errors.HasType(err, &kvpb.MVCCHistoryMutationError{}) {
			if errCallback := f.onUnrecoverableError; errCallback != nil {
				errCallback(ctx, err)
				log.VEventf(ctx, 1, "exiting rangefeed due to internal error: %v", err)
			} else {
				log.Dev.Warningf(ctx, "exiting rangefeed because of internal error with no OnInternalError callback: %s", err.Error())
			}
			return
		}
		if err != nil && ctx.Err() == nil && restartLogEvery.ShouldLog() {
			log.Dev.Warningf(ctx, "rangefeed failed %d times, restarting: %v",
				redact.Safe(i), err)
		}
		if ctx.Err() != nil {
			log.VEventf(ctx, 1, "exiting rangefeed")
			return
		}
		// ─── 阶段 5：退避重置判断 ───
		ranFor := start.Elapsed()
		log.VEventf(ctx, 1, "restarting rangefeed for %v after %v",
			f.spansDebugStr, ranFor)
		if f.knobs != nil && f.knobs.OnRangefeedRestart != nil {
			f.knobs.OnRangefeedRestart()
		}

		// If the rangefeed ran successfully for long enough, reset the retry
		// state so that the exponential backoff begins from its minimum value.
		if ranFor > resetThreshold { // 30秒
			i = 1
			//假设 RangeFeed 稳定运行了 2 小时，然后因为一次短暂的节点重启失败。如果不重置，
			//下一次重试的等待时间会是 maxBackoff（可能 10 秒），合理；但实际上此时可能只需要等 1-2 秒节点就恢复了。
			r.Reset()
		}
	}
}

// processEvents processes events sent by the rangefeed on the eventCh.
func (f *RangeFeed) processEvents(
	ctx context.Context, frontier span.Frontier, eventCh <-chan kvcoord.RangeFeedMessage,
) error {
	for {
		select {
		case ev := <-eventCh:
			if err := f.processEvent(ctx, frontier, ev.RangeFeedEvent, ev.RegisteredSpan); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *RangeFeed) processEvent(
	ctx context.Context, frontier span.Frontier, ev *kvpb.RangeFeedEvent, registeredSpan roachpb.Span,
) error {
	switch {
	case ev.Val != nil: // 普通值变更
		//回调是同步的。用户回调不能阻塞太久，否则会积压 eventCh，进而背压到服务端（channel 满了生产者会 block）。
		f.onValue(ctx, ev.Val)
	case ev.Checkpoint != nil: // watermark 推进
		ts := ev.Checkpoint.ResolvedTS
		if f.frontierQuantize != 0 {
			//将时间戳截断到最近的量化边界（如 1 秒）。目的：减少 B-Tree frontier 中的 span 碎片。
			//当多个 span 的时间戳量化到同一值时，它们可以合并为一个节点，减少内存和查找开销。
			ts.Logical = 0
			ts.WallTime -= ts.WallTime % int64(f.frontierQuantize)
		}
		//`frontier.Forward()` 内部调用 `btreeFrontier.forward()`，会执行：
		//- 在 B-Tree 中找到与 `span` 重叠的所有 `btreeFrontierEntry`。
		//- 若某个 entry 的 ts 小于新 ts，更新它（也需要更新 minHeap 中的位置）。
		//- 若 span 边界不对齐（如传入 span 比某个 entry 大），需要分裂 entry。
		//- 相邻 entry 时间戳相同时合并（减少碎片）。
		//
		//`advanced=true` 当且仅当全局最小时间戳（`frontier.Frontier()` = minHeap 堆顶）也随之提升了。
		advanced, err := frontier.Forward(ev.Checkpoint.Span, ts)
		if err != nil {
			return err
		}
		//onCheckpoint:         每次收到 checkpoint 都调用（可能不 advance frontier 整体）
		//onFrontierAdvance:    只在 frontier 整体推进时调用（下游关心"最慢的 span 到哪了"）
		//frontierVisitor:      每次 checkpoint 后都调用，传入完整的 frontier 快照
		if f.onCheckpoint != nil {
			f.onCheckpoint(ctx, ev.Checkpoint)
		}
		if advanced && f.onFrontierAdvance != nil {
			f.onFrontierAdvance(ctx, frontier.Frontier())
		}
		if f.frontierVisitor != nil {
			f.frontierVisitor(ctx, advanced, frontier)
		}
	case ev.SST != nil: // SST 文件注入
		if f.onSSTable == nil {
			return errors.AssertionFailedf(
				"received unexpected rangefeed SST event with no OnSSTable handler")
		}
		f.onSSTable(ctx, ev.SST, registeredSpan)
	case ev.DeleteRange != nil: // MVCC 范围删除
		if f.onDeleteRange == nil {
			if f.knobs != nil && f.knobs.IgnoreOnDeleteRangeError {
				return nil
			}
			return errors.AssertionFailedf(
				"received unexpected rangefeed DeleteRange event with no OnDeleteRange handler: %s", ev)
		}
		f.onDeleteRange(ctx, ev.DeleteRange)
	case ev.Metadata != nil: // 元数据事件
		if f.onMetadata == nil {
			return errors.AssertionFailedf("received unexpected metadata event with no OnMetadata handler")
		}
		f.onMetadata(ctx, ev.Metadata)
	case ev.Error != nil: // 错误（静默，由 RangeFeed RPC 返回值携带）
	//服务端会在关闭流之前发送一个 Error 事件，然后关闭 gRPC stream，使得 DB.RangeFeed() 的调用返回非 nil 错误。
	//在客户端这里收到 Error 事件时什么都不做，等待 RPC 调用自然返回。这避免了重复处理错误的问题。
	// Intentionally do nothing, we'll get an error returned from the
	// call to RangeFeed.
	case ev.BulkEvents != nil: // 批量事件（优化路径）
		if f.onValues != nil {
			// We can optimistically assume the bulk event consists of all value
			// events, and allocate a buffer for them to be passed to onValues. In the
			// rare case we hit a non-value event (it would have to be a range key as
			// only a catch-up scan currently produces bulk events), we will throw out
			// this buffer and any events we might have copied to it so far and just
			// fallback to to processing each event, but this should be so uncommon it
			// is not worth worrying about the potential wasted work.
			//快速路径（全是 Val 事件 + 用户注册了 onValues）：
			//一次性构建 []kv.KeyValue，调用批量回调，避免 N 次函数调用和 N 次 interface dispatch 开销。
			allValues := true
			buf := make([]kv.KeyValue, len(ev.BulkEvents.Events))
			for i := range ev.BulkEvents.Events {
				if ev.BulkEvents.Events[i].Val != nil {
					buf[i] = kv.KeyValue{
						Key:   ev.BulkEvents.Events[i].Val.Key,
						Value: &ev.BulkEvents.Events[i].Val.Value,
					}
				} else {
					allValues = false
					break
				}
			}
			if allValues {
				f.onValues(ctx, buf)
				return nil
			}
		}
		// Either the bulk event contains non-value events or a onValues handler is
		// not configured, so process each event individually.
		//- 回退路径：遇到非 Val 事件（如 range tombstone），或用户没注册 onValues，逐个递归调用 processEvent。
		for _, e := range ev.BulkEvents.Events {
			if err := f.processEvent(ctx, frontier, e, registeredSpan); err != nil {
				return err
			}
		}

	}
	return nil
}
