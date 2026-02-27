// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/pebble/record"
)

// syncWaiter is capable of waiting for a disk write to be durably committed.
// syncWaiter - 等待磁盘写入完成的能力
type syncWaiter interface {
	// SyncWait waits for the write to be durable.
	SyncWait() error // 阻塞直到fsync完成
	// Close closes the syncWaiter and releases associated resources.
	// Must be called after SyncWait returns.
	Close() // 释放资源
}

// storage.Batch 实现了 syncWaiter 接口
var _ syncWaiter = storage.Batch(nil)

// syncWaiterCallback is a callback provided to a SyncWaiterLoop.
// The callback is structured as an interface instead of a closure to allow
// users to batch the callback and its inputs into a single heap object, and
// then pool the allocation of that object.
// syncWaiterCallback - 回调执行接口
type syncWaiterCallback interface {
	// run executes the callback.
	run() // 在fsync完成后执行
}

// SyncWaiterLoop waits on a sequence of in-progress disk writes, notifying
// callbacks when their corresponding disk writes have completed.
// Invariant: The callbacks are notified in the order that they were enqueued
// and without concurrency.
type SyncWaiterLoop struct {
	q       chan syncBatch
	stopped chan struct{}

	logEveryEnqueueBlocked log.EveryN
}

type syncBatch struct {
	wg syncWaiter
	cb syncWaiterCallback
}

// NewSyncWaiterLoop constructs a SyncWaiterLoop. It must be Started before use.
func NewSyncWaiterLoop() *SyncWaiterLoop {
	return &SyncWaiterLoop{
		// We size the waiter loop's queue to twice the size of Pebble's sync
		// concurrency, which is the maximum number of pending syncWaiter's that
		// pebble allows. Doubling the size gives us headroom to prevent the sync
		// waiter loop from blocking on calls to enqueue, even if consumption from
		// the queue is delayed. If the pipeline is going to block, we'd prefer for
		// it to do so during the call to batch.CommitNoSyncWait.
		/// 队列大小：2 × Pebble并发sync上限
		//        // record.SyncConcurrency = 4096
		//        // 2倍大小 = 8192，给予缓冲避免阻塞enqueue
		q:                      make(chan syncBatch, 2*record.SyncConcurrency),
		stopped:                make(chan struct{}),
		logEveryEnqueueBlocked: log.Every(1 * time.Second), //限流日志（每秒最多1次）
	}
}

// Start launches the loop.
func (w *SyncWaiterLoop) Start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{
			TaskName: "raft-logstore-sync-waiter-loop",
			// This task doesn't reference a parent because it runs for the server's
			// lifetime.
			// SterileRootSpan：不继承父span，独立生命周期
			SpanOpt: stop.SterileRootSpan,
		},
		func(ctx context.Context) {
			w.waitLoop(ctx, stopper)
		})
}

// waitLoop pulls off the SyncWaiterLoop's queue. For each syncWaiter, it waits
// for the sync to complete and then calls the associated callback.
func (w *SyncWaiterLoop) waitLoop(ctx context.Context, stopper *stop.Stopper) {
	defer close(w.stopped) // ← 退出时关闭stopped channel

	for {
		select {
		case w := <-w.q: // ← 从队列读取syncBatch
			// 步骤1：等待fsync完成（阻塞）
			if err := w.wg.SyncWait(); err != nil {
				log.KvExec.Fatalf(ctx, "SyncWait error: %+v", err)
			}
			// 步骤2：执行回调（串行，保证顺序）
			w.cb.run()
			// 步骤3：关闭batch，释放资源
			w.wg.Close()
		case <-stopper.ShouldQuiesce():
			return // ← 收到停止信号
		}
	}
}

// enqueue registers the syncWaiter with the SyncWaiterLoop. The provided
// callback will be called once the syncWaiter's associated disk write has been
// durably committed. It may never be called in case the stopper stops.
//
// The syncWaiter will be Closed after its SyncWait method completes. It must
// not be Closed by the caller. The cb is called before the syncWaiter is
// closed, in case the cb implementation needs to extract something form the
// syncWaiter.
//
// If the SyncWaiterLoop has already been stopped, the callback will never be
// called.
func (w *SyncWaiterLoop) enqueue(ctx context.Context, wg syncWaiter, cb syncWaiterCallback) {
	b := syncBatch{wg, cb}
	// 快速路径：尝试非阻塞发送
	select {
	case w.q <- b:
		// 成功入队，立即返回
	case <-w.stopped:
		// 已停止，丢弃请求
	default:
		// Channel满了，需要阻塞等待
		// 先记录日志（限流）
		if w.logEveryEnqueueBlocked.ShouldLog() {
			// NOTE: we don't expect to hit this because we size the enqueue channel
			// with enough capacity to hold more in-progress sync operations than
			// Pebble allows (pebble/record.SyncConcurrency). However, we can still
			// see this in cases where consumption from the queue is delayed.
			log.KvExec.VWarningf(ctx, 1, "SyncWaiterLoop.enqueue blocking due to insufficient channel capacity")
		}
		// 慢速路径：阻塞发送
		select {
		case w.q <- b:
			// 成功入队
		case <-w.stopped:
			// 等待期间系统停止
		}
	}
}
