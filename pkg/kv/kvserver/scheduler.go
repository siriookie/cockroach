// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/crlib/crtime"
)

const rangeIDChunkSize = 1000

type testProcessorI interface {
	processTestEvent(queuedRangeID, *raftSchedulerShard, raftScheduleState)
}

type rangeIDChunk[T any] struct {
	// Valid contents are buf[rd:wr], read at buf[rd], write at buf[wr].
	buf    [rangeIDChunkSize]T
	rd, wr int
}

func (c *rangeIDChunk[T]) PushBack(item T) bool {
	if c.WriteCap() == 0 {
		return false
	}
	c.buf[c.wr] = item
	c.wr++
	return true
}

func (c *rangeIDChunk[T]) PopFront() (T, bool) {
	if c.Len() == 0 {
		var empty T
		return empty, false
	}
	id := c.buf[c.rd]
	c.rd++
	return id, true
}

func (c *rangeIDChunk[T]) WriteCap() int {
	return len(c.buf) - c.wr
}

func (c *rangeIDChunk[T]) Len() int {
	return c.wr - c.rd
}

// rangeIDQueue is a chunked queue of range IDs. Instead of a separate list
// element for every range ID, it uses a rangeIDChunk to hold many range IDs,
// amortizing the allocation/GC cost. Using a chunk queue avoids any copying
// that would occur if a slice were used (the copying would occur on slice
// reallocation).
//
// The queue implements a FIFO queueing policy with no prioritization of some
// ranges over others.
type rangeIDQueue[T any] struct {
	len    int
	chunks list.List // TODO(pav-kv): use a typed generic list
}

func (q *rangeIDQueue[T]) Push(item T) {
	q.len++
	if q.chunks.Len() == 0 || q.back().WriteCap() == 0 {
		q.chunks.PushBack(&rangeIDChunk[T]{})
	}
	if !q.back().PushBack(item) {
		panic(fmt.Sprintf(
			"unable to push rangeID to chunk: len=%d, cap=%d",
			q.back().Len(), q.back().WriteCap()))
	}
}

func (q *rangeIDQueue[T]) PopFront() (T, bool) {
	if q.len == 0 {
		var empty T
		return empty, false
	}
	q.len--
	frontElem := q.chunks.Front()
	front := frontElem.Value.(*rangeIDChunk[T])
	id, ok := front.PopFront()
	if !ok {
		panic("encountered empty chunk")
	}
	if front.Len() == 0 && front.WriteCap() == 0 {
		q.chunks.Remove(frontElem)
	}
	return id, true
}

func (q *rangeIDQueue[T]) Len() int {
	return q.len
}

func (q *rangeIDQueue[T]) back() *rangeIDChunk[T] {
	return q.chunks.Back().Value.(*rangeIDChunk[T])
}

type raftProcessor interface {
	// Process a raft.Ready struct containing entries and messages that are
	// ready to read, be saved to stable storage, committed, or sent to other
	// peers.
	//
	// This method does not take a ctx; the implementation is expected to use a
	// ctx annotated with the range information, according to RangeID.
	processReady(roachpb.RangeID)
	// Process all queued messages for the specified range.
	// Return true if the range should be queued for ready processing.
	processRequestQueue(context.Context, roachpb.RangeID) bool
	// Process a raft tick for the specified range.
	// Return true if the range should be queued for ready processing.
	processTick(context.Context, roachpb.RangeID) bool
	// Process piggybacked admitted vectors that may advance admitted state for
	// the given range's peer replicas. Used for RACv2.
	processRACv2PiggybackedAdmitted(ctx context.Context, id roachpb.RangeID)
	// Process the RACv2 RangeController.
	processRACv2RangeController(ctx context.Context, id roachpb.RangeID)
}

type raftScheduleFlags int

const (
	//    = 1 << 0  // 已入队
	//    stateRaftReady                   = 1 << 1  // 需要处理 Ready
	//    stateRaftRequest                 = 1 << 2  // 有待处理消息
	//    stateRaftTick                    = 1 << 3  // 有待处理 Tick
	//    stateRACv2PiggybackedAdmitted    = 1 << 4  // RACv2 准入状态
	//    stateRACv2RangeController        = 1 << 5  // RACv2 控制器
	stateQueued raftScheduleFlags = 1 << iota
	stateRaftReady
	stateRaftRequest
	stateRaftTick
	stateRACv2PiggybackedAdmitted
	stateRACv2RangeController
	stateTestIntercept // used for testing, CrdbTestBuild only
)

type raftScheduleState struct {
	flags raftScheduleFlags // 待处理事件集合（bitmap）
	// The number of ticks queued. Usually it's 0 or 1, but may go above if the
	// scheduling or processing is slow. It is limited by raftScheduler.maxTicks,
	// so that the cost of processing all the ticks doesn't grow uncontrollably.
	// If ticks consistently reaches maxTicks, the node/range is too slow, and it
	// is safer to not deliver all the ticks as it may cause a cascading effect
	// (the range events take longer and longer to process).
	// TODO(pavelkalinnikov): add a node health metric for the ticks.
	//
	// INVARIANT: flags&stateRaftTick == 0 iff ticks == 0.
	ticks int64
}

var raftSchedulerBatchPool = sync.Pool{
	New: func() interface{} {
		return new(raftSchedulerBatch)
	},
}

// raftSchedulerBatch is a batch of range IDs to enqueue. It enables
// efficient per-shard enqueueing.
type raftSchedulerBatch struct {
	rangeIDs    [][]roachpb.RangeID // by shard
	priorityIDs map[roachpb.RangeID]bool
}

func newRaftSchedulerBatch(
	numShards int, priorityIDs *syncutil.Set[roachpb.RangeID],
) *raftSchedulerBatch {
	b := raftSchedulerBatchPool.Get().(*raftSchedulerBatch)
	if cap(b.rangeIDs) >= numShards {
		b.rangeIDs = b.rangeIDs[:numShards]
	} else {
		b.rangeIDs = make([][]roachpb.RangeID, numShards)
	}
	if b.priorityIDs == nil {
		b.priorityIDs = make(map[roachpb.RangeID]bool, 8) // expect few ranges, if any
	}
	// Cache the priority range IDs in an owned map, since we expect this to be
	// very small or empty and we do a lookup for every Add() call.
	priorityIDs.Range(func(id roachpb.RangeID) bool {
		b.priorityIDs[id] = true
		return true
	})
	return b
}

func (b *raftSchedulerBatch) Add(id roachpb.RangeID) {
	shardIdx := shardIndex(id, len(b.rangeIDs), b.priorityIDs[id])
	b.rangeIDs[shardIdx] = append(b.rangeIDs[shardIdx], id)
}

func (b *raftSchedulerBatch) Close() {
	for i := range b.rangeIDs {
		b.rangeIDs[i] = b.rangeIDs[i][:0]
	}
	for i := range b.priorityIDs {
		delete(b.priorityIDs, i)
	}
	raftSchedulerBatchPool.Put(b)
}

// shardIndex returns the raftScheduler shard index of the given range ID based
// on the shard count and the range's priority. Priority ranges are assigned to
// the reserved shard 0, other ranges are modulo range ID (ignoring shard 0).
// numShards will always be 2 or more (1 priority, 1 regular).
func shardIndex(id roachpb.RangeID, numShards int, priority bool) int {
	if priority {
		return 0
	}
	return 1 + int(int64(id)%int64(numShards-1)) // int64s to avoid overflow
}

type raftScheduler struct {
	ambientContext log.AmbientContext
	processor      raftProcessor // 实际处理接口（指向 Store）
	metrics        *StoreMetrics
	// shards contains scheduler shards. Ranges and workers are allocated to
	// separate shards to reduce contention at high worker counts. Allocation
	// is modulo range ID, with shard 0 reserved for priority ranges.
	//普通 Range
	//根据 RangeID 做 hash（取模）
	//稳定映射到某一个 shard
	//同一个 Range 永远进同一个 shard
	//优先 Range
	//强制进入 shard[0]
	//与普通 Range 完全隔离
	shards      []*raftSchedulerShard         // 1 + RangeID % (len(shards) - 1)
	priorityIDs syncutil.Set[roachpb.RangeID] // 优先级 Range 集合
	done        sync.WaitGroup
}

type queuedRangeID struct {
	rangeID roachpb.RangeID
	// queued is the moment in time when the rangeID was added to the queue.
	queued crtime.Mono
}

// 单个调度分片（每个 shard 独立锁）
type raftSchedulerShard struct {
	syncutil.Mutex                                       // 保护以下字段
	cond           *sync.Cond                            // worker 唤醒信号
	queue          rangeIDQueue[queuedRangeID]           // 待处理 Range 队列
	state          map[roachpb.RangeID]raftScheduleState // 每个 Range 的状态
	numWorkers     int                                   // 本 shard 的 worker 数
	maxTicks       int64                                 // tick 限流阈值
	stopped        bool
}

func newRaftScheduler(
	ambient log.AmbientContext,
	metrics *StoreMetrics,
	processor raftProcessor,
	numWorkers int,
	shardSize int,
	priorityWorkers int,
	maxTicks int64,
) *raftScheduler {
	s := &raftScheduler{
		ambientContext: ambient,
		processor:      processor,
		metrics:        metrics,
	}
	// ========================================
	// 第一步：创建 Priority Shard (shard 0)
	// ========================================
	// Priority shard at index 0.
	if priorityWorkers <= 0 {
		priorityWorkers = 1
	}
	s.shards = append(s.shards, newRaftSchedulerShard(priorityWorkers, maxTicks))

	// Regular shards, excluding priority shard.
	// ========================================
	// 第二步：计算 Regular Shard 数量
	// ========================================
	numShards := 1
	if shardSize > 0 && numWorkers > shardSize {
		numShards = (numWorkers-1)/shardSize + 1 // ceiling division // 向上取整
	}
	// 示例：numWorkers=384, shardSize=256
	//   numShards = 383/256 + 1 = 2

	// ========================================
	// 第三步：分配 Worker 到各 Shard
	// ========================================
	for i := 0; i < numShards; i++ {
		shardWorkers := numWorkers / numShards // 基础分配
		if i < numWorkers%numShards {          // distribute remainder// 分配余数
			shardWorkers++
		}
		// 示例：384 workers, 2 shards
		//   shard 0 (priority): 1 worker
		//   shard 1: 192 workers
		//   shard 2: 192 workers

		if shardWorkers <= 0 {
			shardWorkers = 1 // ensure we always have a worker// 保证每个 shard 至少 1 worker
		}
		s.shards = append(s.shards, newRaftSchedulerShard(shardWorkers, maxTicks))
	}
	return s
}

func newRaftSchedulerShard(numWorkers int, maxTicks int64) *raftSchedulerShard {
	shard := &raftSchedulerShard{
		state:      map[roachpb.RangeID]raftScheduleState{},
		numWorkers: numWorkers,
		maxTicks:   maxTicks,
	}
	shard.cond = sync.NewCond(&shard.Mutex)
	return shard
}

func (s *raftScheduler) Start(stopper *stop.Stopper) {
	stopper.OnQuiesce(func() {
		for _, shard := range s.shards {
			shard.Lock()
			shard.stopped = true
			shard.Unlock()
			shard.cond.Broadcast()
		}
	})

	ctx := s.ambientContext.AnnotateCtx(context.Background())
	for _, shard := range s.shards {
		s.done.Add(shard.numWorkers)
		f := func(ctx context.Context, hdl *stop.Handle) {
			defer hdl.Activate(ctx).Release(ctx)
			defer s.done.Done()
			shard.worker(ctx, s.processor, s.metrics)
		}

		for i := 0; i < shard.numWorkers; i++ {
			ctx, hdl, err := stopper.GetHandle(ctx,
				stop.TaskOpts{
					TaskName: "raft-worker",
					// This task doesn't reference a parent because it runs for the server's
					// lifetime.
					SpanOpt: stop.SterileRootSpan,
				})
			if err != nil {
				s.done.Done()
				continue
			}
			go f(ctx, hdl)
		}
	}
}

func (s *raftScheduler) Wait(context.Context) {
	s.done.Wait()
}

// AddPriorityID adds the given range ID to the set of priority ranges.
func (s *raftScheduler) AddPriorityID(rangeID roachpb.RangeID) {
	s.priorityIDs.Add(rangeID)
}

// RemovePriorityID removes the given range ID from the set of priority ranges.
func (s *raftScheduler) RemovePriorityID(rangeID roachpb.RangeID) {
	s.priorityIDs.Remove(rangeID)
}

// PriorityIDs returns the current priority ranges.
func (s *raftScheduler) PriorityIDs() []roachpb.RangeID {
	var priorityIDs []roachpb.RangeID
	s.priorityIDs.Range(func(id roachpb.RangeID) bool {
		priorityIDs = append(priorityIDs, id)
		return true
	})
	return priorityIDs
}

func (ss *raftSchedulerShard) worker(
	ctx context.Context, processor raftProcessor, metrics *StoreMetrics,
) {
	// We use a sync.Cond for worker notification instead of a buffered
	// channel. Buffered channels have internal overhead for maintaining the
	// buffer even when the elements are empty. And the buffer isn't necessary as
	// the raftScheduler work is already buffered on the internal queue. Lastly,
	// signaling a sync.Cond is significantly faster than selecting and sending
	// on a buffered channel.
	// ========================================
	// 阶段1：等待工作
	// ========================================
	ss.Lock()
	for {
		var q queuedRangeID
		for {
			if ss.stopped {
				ss.Unlock()
				return
			}
			var ok bool
			if q, ok = ss.queue.PopFront(); ok {
				break
			}
			ss.cond.Wait() // 释放锁，睡眠，被唤醒后重新获取锁
		}

		// Grab and clear the existing state for the range ID. Note that we leave
		// the range ID marked as "queued" so that a concurrent Enqueue* will not
		// queue the range ID again.
		// ========================================
		// 阶段2：获取状态并清空（保留stateQueued）
		// ========================================
		state := ss.state[q.rangeID]
		ss.state[q.rangeID] = raftScheduleState{flags: stateQueued}
		ss.Unlock() // ← 关键：处理期间不持锁

		// Record the scheduling latency for the range.
		// 记录调度延迟
		metrics.RaftSchedulerLatency.RecordValue(int64(q.queued.Elapsed()))

		// Process requests first. This avoids a scenario where a tick and a
		// "quiesce" message are processed in the same iteration and intervening
		// raft ready processing unquiesces the replica because the tick triggers
		// an election.
		// ========================================
		// 阶段3：按顺序处理事件
		// ========================================

		// [1] Request 优先于 Tick
		// 原因：避免 Quiesce 竞争
		if state.flags&stateRaftRequest != 0 {
			// processRequestQueue returns true if the range should perform ready
			// processing. Do not reorder this below the call to processReady.
			if processor.processRequestQueue(ctx, q.rangeID) {
				state.flags |= stateRaftReady
			}
		}

		if util.RaceEnabled { // assert the ticks invariant
			if tick := state.flags&stateRaftTick != 0; tick != (state.ticks != 0) {
				log.KvExec.Fatalf(ctx, "stateRaftTick is %v with ticks %v", tick, state.ticks)
			}
		}
		// 场景：Range收到MsgApp和MsgQuiesce，必须先处理MsgApp（Step）
		// 才能正确处理Quiesce。若先Tick可能触发选举，打破Quiesce。

		// [2] Tick（可能批量）
		if state.flags&stateRaftTick != 0 {
			for t := state.ticks; t > 0; t-- {
				// processRaftTick returns true if the range should perform ready
				// processing. Do not reorder this below the call to processReady.
				if processor.processTick(ctx, q.rangeID) {
					state.flags |= stateRaftReady
				}
			}
		}
		// 场景：积累的10个tick需要逐一处理，确保心跳超时递增正确。

		// [3] RACv2 Piggyback（准入状态更新）
		if state.flags&stateRACv2PiggybackedAdmitted != 0 {
			processor.processRACv2PiggybackedAdmitted(ctx, q.rangeID)
		}
		// [4] Ready（最后处理）
		// 原因：Ready是Step/Tick的输出，必须在它们之后
		if state.flags&stateRaftReady != 0 {
			processor.processReady(q.rangeID)
		}
		// 场景：Step(MsgApp) → hasReady → handleRaftReady() → apply entries

		// [5] RACv2 Controller
		if state.flags&stateRACv2RangeController != 0 {
			processor.processRACv2RangeController(ctx, q.rangeID)
		}
		if buildutil.CrdbTestBuild && state.flags&stateTestIntercept != 0 {
			processor.(testProcessorI).processTestEvent(q, ss, state)
		}
		// ========================================
		// 阶段4：检查新事件并决定是否重新入队
		// ========================================
		ss.Lock()
		state = ss.state[q.rangeID]
		if state.flags == stateQueued { // 无新事件
			// No further processing required by the range ID, clear it from the
			// state map.
			delete(ss.state, q.rangeID) // 清理
		} else { // 有新事件
			// There was a concurrent call to one of the Enqueue* methods. Queue
			// the range ID for further processing.
			//
			// Even though the Enqueue* method did not signal after detecting
			// that the range was being processed, there also is no need for us
			// to signal the condition variable. This is because this worker
			// goroutine will loop back around and continue working without ever
			// going back to sleep.
			//
			// We can prove this out through a short derivation.
			// - For optimal concurrency, we want:
			//     awake_workers = min(max_workers, num_ranges)
			// - The condition variable / mutex structure ensures that:
			//     awake_workers = cur_awake_workers + num_signals
			// - So we need the following number of signals for optimal concurrency:
			//     num_signals = min(max_workers, num_ranges) - cur_awake_workers
			// - If we re-enqueue a range that's currently being processed, the
			//   num_ranges does not change once the current iteration completes
			//   and the worker does not go back to sleep between the current
			//   iteration and the next iteration, so no change to num_signals
			//   is needed.
			//
			// NB: this is a new insertion into the queue, so we set a new timestamp.
			// We do not want the scheduler latency to pick up the time spent handling
			// this replica.
			// 重新入队（使用新时间戳）
			ss.queue.Push(queuedRangeID{rangeID: q.rangeID, queued: crtime.NowMono()})
			// 不需要signal：当前worker会继续循环，不会睡眠

		}
	}
}

// NewEnqueueBatch creates a new range ID batch for enqueueing via
// EnqueueRaft(Ticks|Requests). The caller must call Close() on the batch when
// done.
func (s *raftScheduler) NewEnqueueBatch() *raftSchedulerBatch {
	return newRaftSchedulerBatch(len(s.shards), &s.priorityIDs)
}

func (ss *raftSchedulerShard) enqueue1Locked(
	addFlags raftScheduleFlags, id roachpb.RangeID, now crtime.Mono,
) int {
	// ========================================
	// 第一步：计算 ticks 增量（仅对 stateRaftTick）
	// ========================================
	ticks := int64((addFlags & stateRaftTick) / stateRaftTick) // 0 or 1
	// 位操作技巧：
	//   addFlags=stateRaftTick(0b1000) → ticks=1
	//   addFlags=stateRaftReady(0b0010) → ticks=0

	// ========================================
	// 第二步：检查是否已有完全相同的标志
	// ========================================
	prevState := ss.state[id]
	if prevState.flags&addFlags == addFlags && ticks == 0 {
		return 0 // 幂等：无需重复设置
	}
	// 示例：
	//   prevState.flags = stateQueued | stateRaftReady
	//   addFlags = stateRaftReady
	//   → prevState.flags&addFlags = stateRaftReady == addFlags
	//   → return 0（已有该标志）

	// ========================================
	// 第三步：合并状态
	// ========================================
	var queued int
	newState := prevState
	newState.flags = newState.flags | addFlags
	newState.ticks += ticks
	// 限流：截断 ticks
	if newState.ticks > ss.maxTicks {
		newState.ticks = ss.maxTicks
	}
	// ========================================
	// 第四步：决定是否入队
	// ========================================
	if newState.flags&stateQueued == 0 {
		newState.flags |= stateQueued
		queued++
		ss.queue.Push(queuedRangeID{rangeID: id, queued: now})
	}
	// 否则：Range 已在队列中，仅更新状态表
	ss.state[id] = newState
	return queued
}

func (s *raftScheduler) enqueue1(addFlags raftScheduleFlags, id roachpb.RangeID) {
	now := crtime.NowMono()
	hasPriority := s.priorityIDs.Contains(id)
	shardIdx := shardIndex(id, len(s.shards), hasPriority)
	shard := s.shards[shardIdx]
	shard.Lock()
	n := shard.enqueue1Locked(addFlags, id, now)
	shard.Unlock()
	shard.signal(n)
}

func (ss *raftSchedulerShard) enqueueN(addFlags raftScheduleFlags, ids ...roachpb.RangeID) int {
	// Enqueue the ids in chunks to avoid holding mutex for too long.
	const enqueueChunkSize = 128

	// Avoid locking for 0 new ranges.
	if len(ids) == 0 {
		return 0
	}

	now := crtime.NowMono()
	ss.Lock()
	var count int
	for i, id := range ids {
		count += ss.enqueue1Locked(addFlags, id, now)
		if (i+1)%enqueueChunkSize == 0 {
			ss.Unlock()
			now = crtime.NowMono()
			ss.Lock()
		}
	}
	ss.Unlock()
	return count
}

func (s *raftScheduler) enqueueBatch(addFlags raftScheduleFlags, batch *raftSchedulerBatch) {
	for shardIdx, ids := range batch.rangeIDs {
		count := s.shards[shardIdx].enqueueN(addFlags, ids...)
		s.shards[shardIdx].signal(count)
	}
}

func (ss *raftSchedulerShard) signal(count int) {
	if count >= ss.numWorkers {
		ss.cond.Broadcast()
	} else {
		for i := 0; i < count; i++ {
			ss.cond.Signal()
		}
	}
}

func (s *raftScheduler) EnqueueRaftReady(id roachpb.RangeID) {
	s.enqueue1(stateRaftReady, id)
}

func (s *raftScheduler) EnqueueRaftRequest(id roachpb.RangeID) {
	s.enqueue1(stateRaftRequest, id)
}

func (s *raftScheduler) EnqueueRaftRequests(batch *raftSchedulerBatch) {
	s.enqueueBatch(stateRaftRequest, batch)
}

func (s *raftScheduler) EnqueueRaftTicks(batch *raftSchedulerBatch) {
	s.enqueueBatch(stateRaftTick, batch)
}

func (s *raftScheduler) EnqueueRACv2PiggybackAdmitted(id roachpb.RangeID) {
	s.enqueue1(stateRACv2PiggybackedAdmitted, id)
}

func (s *raftScheduler) EnqueueRACv2RangeController(id roachpb.RangeID) {
	s.enqueue1(stateRACv2RangeController, id)
}

type racV2Scheduler raftScheduler

var _ rac2.Scheduler = &racV2Scheduler{}

// ScheduleControllerEvent implements rac2.Scheduler.
func (s *racV2Scheduler) ScheduleControllerEvent(rangeID roachpb.RangeID) {
	(*raftScheduler)(s).EnqueueRACv2RangeController(rangeID)
}
