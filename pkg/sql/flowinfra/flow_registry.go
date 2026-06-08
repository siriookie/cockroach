// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flowinfra

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/proto"
)

// errNoInboundStreamConnection is the error propagated through the flow when
// the timeout to setup the flow is exceeded.
var errNoInboundStreamConnection = errors.New("no inbound stream connection")

// IsNoInboundStreamConnectionError returns true if err's Cause is an
// errNoInboundStreamConnection.
func IsNoInboundStreamConnectionError(err error) bool {
	return errors.Is(err, errNoInboundStreamConnection)
}

// SettingFlowStreamTimeout is a cluster setting that sets the default flow
// stream timeout.
var SettingFlowStreamTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.distsql.flow_stream_timeout",
	"amount of time incoming streams wait for a flow to be set up before erroring out",
	10*time.Second,
	settings.WithName("sql.distsql.flow_stream.timeout"),
)

// expectedConnectionTime is the expected time taken by a flow to connect to its
// consumers.
const expectedConnectionTime time.Duration = 500 * time.Millisecond

// InboundStreamInfo represents the endpoint where a data stream from another
// node connects to a flow. The external node initiates this process through a
// FlowStream RPC, which uses FlowRegistry.ConnectInboundStream() to associate
// the stream to a receiver to push rows to.
type InboundStreamInfo struct {
	mu struct {
		syncutil.Mutex
		connected bool
		// if set, indicates that we waited too long for an inbound connection,
		// or we don't want this stream to connect anymore due to flow
		// cancellation.
		canceled bool
		// finished is set when onFinish has already been called (which signaled
		// that the stream is done transferring rows to the flow's wait group).
		finished bool
	}
	// receiver is the entity that will receive rows from another host, which is
	// part of a processor (normally an input synchronizer) for row-based
	// execution and a colrpc.Inbox for vectorized execution.
	//
	// During a FlowStream RPC, the stream is handed off to this strategy to
	// process.
	receiver InboundStreamHandler
	// onFinish will be called when the corresponding inbound stream is done.
	onFinish func()
}

// NewInboundStreamInfo returns a new InboundStreamInfo.
func NewInboundStreamInfo(
	receiver InboundStreamHandler, waitGroup *sync.WaitGroup,
) *InboundStreamInfo {
	return &InboundStreamInfo{
		receiver: receiver,
		// Do not hold onto the whole wait group to limit the coupling of the
		// InboundStreamInfo with the FlowBase.
		onFinish: waitGroup.Done,
	}
}

// finishLocked marks s as finished and calls onFinish. The mutex of s must be
// held when calling this method.
func (s *InboundStreamInfo) finishLocked() {
	if !s.mu.connected && !s.mu.canceled {
		panic("finishing inbound stream that didn't connect or time out")
	}
	if s.mu.finished {
		panic("double finish")
	}
	s.mu.finished = true
	s.onFinish()
}

// cancelIfNotConnected 如果 stream 尚未连接、尚未完成、也未被取消，则取消它。
// 返回 true 表示本次确实取消了该 stream，false 表示它已经被处理过。
//
// connected=true：FlowStream RPC 已到达，ConnectInboundStream 已成功完成握手
// finished=true：流已完成数据传输
// canceled=true：已被之前的超时或其他错误取消
func (s *InboundStreamInfo) cancelIfNotConnected(err error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.connected || s.mu.finished || s.mu.canceled {
		return false
	}
	s.cancelLocked(err)
	return true
}

// cancelLocked 将 stream 标记为已取消，完成清理，并向 receiver 推送错误。
// 必须在持有 s.mu 时调用。
//
// 两步操作：
//   1. finishLocked()：标记 finished=true，调用 onFinish() = WaitGroup.Done()
//      释放 flow 的 WaitGroup 计数，确保 flow 不会被永久阻塞
//   2. 异步推送 error 到 receiver（processor 的 input synchronizer 或 inbox）：
//      错误沿 processor 链传播，最终导致整个 flow 被 drain
//      用 goroutine 是因为 receiver.Timeout 可能阻塞
func (s *InboundStreamInfo) cancelLocked(err error) {
	s.mu.AssertHeld()
	s.mu.canceled = true
	s.finishLocked()
	go func() {
		s.receiver.Timeout(err)
	}()
}

// finish is the same as finishLocked when the mutex of s is not held already.
func (s *InboundStreamInfo) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishLocked()
}

// flowEntry is a structure associated with a (potential) flow.
//
// All fields in flowEntry are protected by the FlowRegistry mutex and, thus,
// must be accessed while holding the lock.
type flowEntry struct {
	// waitCh is set if one or more clients are waiting for the flow; the
	// channel gets closed when the flow is registered.
	waitCh chan struct{}  // FlowStream 先到时在此等待

	// refCount is used to allow multiple clients to wait for a flow - if the
	// flow never shows up, the refCount is used to decide which client cleans
	// up the entry.
	refCount int // 引用计数

	flow *FlowBase // 实际的 flow 对象

	// inboundStreams are streams that receive data from other hosts, through
	// the FlowStream API. This map is set in Flow.Setup(), so it is safe to
	// lookup into concurrently later.
	//
	// Each InboundStreamInfo has its own mutex, separate from the FlowRegistry
	// mutex. These two mutexes should **not** be held at the same time.
	inboundStreams map[execinfrapb.StreamID]*InboundStreamInfo // 所有远程入站流

	// streamTimer is a timer that fires after a timeout and verifies that all
	// inbound streams have been connected.
	streamTimer *time.Timer  // 超时定时器
}

// FlowRegistry allows clients to look up flows by ID and to wait for flows to
// be registered. Multiple clients can wait concurrently for the same flow.
type FlowRegistry struct {
	syncutil.Mutex

	// All fields in the flowEntry's are protected by the FlowRegistry mutex,
	// except flow, whose methods can be called freely.
	flows map[execinfrapb.FlowID]*flowEntry// flowID → flow

	// draining specifies whether the FlowRegistry is in drain mode. If it is,
	// the FlowRegistry will not accept new flows.
	draining bool // 是否在排空中

	// flowDone is signaled whenever the size of flows decreases.
	flowDone *sync.Cond// flow 数减少时通知 Drain

	// testingRunBeforeDrainSleep is a testing knob executed when a draining
	// FlowRegistry has no registered flows but must still wait for a minimum time
	// for any incoming flows to register.
	testingRunBeforeDrainSleep func()
}

// NewFlowRegistry creates a new FlowRegistry.
// FlowRegistry` 是每个节点上管理**分布式 flow** 的注册中心，解决一个核心时序问题：**`SetupFlow` RPC 和 `FlowStream` RPC 到达顺序不确定**。
// 看一张图就清楚了：

// ```
// 节点 A (Gateway)                         节点 B (Remote)
//      │                                       │
//      ├── SetupFlow RPC ────────────────────► │ 创建 flow，但没有数据接收者
//      │                                       │ 把 flow 注册到 FlowRegistry
//      │                                       │ 等待 inbound streams 连接...
//      │                                       │
//      ├── FlowStream RPC ───────────────────► │ 数据来了！
//      │   (携带 FlowID + StreamID)             │ FlowRegistry.ConnectInboundStream()
//      │                                       │   → 按 FlowID 找到 flow
//      │                                       │   → 按 StreamID 找到对应的 InboundStreamInfo
//      │                                       │   → 把 gRPC stream 交给 processor 的 input sync
//      │                                       │
//      │   ←── 数据流 ──────────────────────   │ processor 开始消费数据
// ```

// ### 它解决的具体问题

// SetupFlow 和 FlowStream 是**两个独立的 gRPC 调用**，谁先到不一定：

// | 场景 | 处理方式 |
// |------|---------|
// | SetupFlow 先到 | flow 注册好，等在 registry 里，FlowStream 来了直接找到 |
// | FlowStream 先到 | `waitForFlow()` 创建 `waitCh`，阻塞等待，SetupFlow 来的时候 close(waitCh) 唤醒 |
// | 超时 | `streamTimer` 触发 → `cancelPendingStreams()` → 推 error 给所有等待的 receiver |
// | 节点关闭 | `Drain()` 拒绝新 flow，等正在跑的结束，超时后 `flow.Cancel()` 全部取消
func NewFlowRegistry() *FlowRegistry {
	fr := &FlowRegistry{flows: make(map[execinfrapb.FlowID]*flowEntry)}
	fr.flowDone = sync.NewCond(fr)
	return fr
}

// getEntryLocked 获取 flowID 对应的 flowEntry，如果不存在则创建空 entry 并插入 map。
// 必须在持有 FlowRegistry 锁时调用。
//
// 这是 RegisterFlow 和 ConnectInboundStream 共享的入口点——无论谁先到，
// 都会拿到同一个 flowEntry 对象，从而实现了两者之间的同步：
//   - FlowStream 先到：ConnectInboundStream 调用本函数创建空 entry（含 waitCh），
//     RegisterFlow 后到拿到同一个 entry，通过 close(waitCh) 唤醒等待者。
//   - SetupFlow 先到：RegisterFlow 创建 entry 并设置 entry.flow，
//     ConnectInboundStream 后到直接拿到 entry.flow。
func (fr *FlowRegistry) getEntryLocked(id execinfrapb.FlowID) *flowEntry {
	entry, ok := fr.flows[id]
	if !ok {
		entry = &flowEntry{}
		fr.flows[id] = entry
	}
	return entry
}

// releaseEntryLocked 减少 entry 的引用计数，计数归零时从 map 中删除。
// 必须在持有 FlowRegistry 锁时调用。
//
// 引用计数的来源：
//   - RegisterFlow 中 +1（对应 UnregisterFlow 中 -1）
//   - ConnectInboundStream 中 +1（对应其 defer 中 -1）
//
// 当所有引用者都释放后，删除 entry 并通过 flowDone.Signal() 通知
// 可能正在 Drain 中等待的 goroutine（见 Drain 的 flowDone.Wait() 循环）。
func (fr *FlowRegistry) releaseEntryLocked(id execinfrapb.FlowID) {
	entry := fr.flows[id]
	if entry.refCount > 1 {
		entry.refCount--
	} else {
		if entry.refCount != 1 {
			panic(errors.AssertionFailedf("invalid refCount: %d", entry.refCount))
		}
		delete(fr.flows, id)
		fr.flowDone.Signal()
	}
}

type flowRetryableError struct {
	cause error
}

var _ errors.Wrapper = &flowRetryableError{}

func (e *flowRetryableError) Error() string { return e.cause.Error() }
func (e *flowRetryableError) Cause() error  { return e.cause }
func (e *flowRetryableError) Unwrap() error { return e.Cause() }

func decodeFlowRetryableError(
	_ context.Context, cause error, _ string, _ []string, _ proto.Message,
) error {
	return &flowRetryableError{cause: cause}
}

func init() {
	errors.RegisterWrapperDecoder(errors.GetTypeKey((*flowRetryableError)(nil)), decodeFlowRetryableError)
}

// IsFlowRetryableError returns true if an error represents a retryable
// flow error.
func IsFlowRetryableError(e error) bool {
	return errors.HasType(e, (*flowRetryableError)(nil))
}

// RegisterFlow 将一个 flow 注册到 FlowRegistry 中，使其可被 ConnectInboundStream 找到。
//
// 调用时机：在远程节点上，SetupFlow RPC 处理完毕后，flow 启动时调用。
// 调用路径：FlowBase.StartInternal() → RegisterFlow()。
//
// 核心功能：解决 SetupFlow 和 FlowStream 两个独立 RPC 的竞态问题。
//
// 两种时序：
//   时序 1：SetupFlow 先到 → RegisterFlow 注册 flow，close(waitCh) 唤醒等待者
//   时序 2：FlowStream 先到 → ConnectInboundStream 调用 getEntryLocked 创建空 entry 和 waitCh，
//          然后 waitForFlow 阻塞在 waitCh 上，RegisterFlow 到来后 close(waitCh) 唤醒
//
// inboundStreams 是这个 flow 期望接收的所有远程入站流，在 Flow.Setup() 时创建好，
// 每个 InboundStreamInfo 的 WaitGroup 已经 +1。RegisterFlow 接管这些 stream 的生命周期：
//   - 正常流程：ConnectInboundStream 接管，调用 stream.finish() → onFinish() → WaitGroup.Done()
//   - 注册失败：本函数通过 defer 直接调 stream.onFinish() 释放
//   - 超时：timer 触发 cancelPendingStreams → cancelLocked → finishLocked → onFinish()
//
// 参数：
//   id       - flow 的唯一标识，由 gateway 生成
//   f        - 已装配好的 FlowBase（processor 链已就绪，只等数据流入）
//   inboundStreams - 期望的远程入站流映射，key 是 StreamID
//   timeout  - 等待 inbound stream 连接的超时，默认 10s（SettingFlowStreamTimeout）
func (fr *FlowRegistry) RegisterFlow(
	ctx context.Context,
	id execinfrapb.FlowID,
	f *FlowBase,
	inboundStreams map[execinfrapb.StreamID]*InboundStreamInfo,
	timeout time.Duration,
) (retErr error) {
	// 持锁保护整个注册过程，保证与 ConnectInboundStream 之间的并发安全。
	fr.Lock()
	defer fr.Unlock()

	// 注册失败时的回滚逻辑：逐个调用 stream.onFinish()（即 WaitGroup.Done()）。
	// 为什么必须这样做：inboundStreams 在创建时 WaitGroup 已经 +1，
	// 如果注册失败，这些 stream 永远不会被 ConnectInboundStream 接管，
	// 不调 Done() 会导致 Flow.Wait() 永久阻塞。
	defer func() {
		if retErr != nil {
			for _, stream := range inboundStreams {
				stream.onFinish()
			}
		}
	}()

	// 检查是否在排空模式。如果节点正在关闭，拒绝注册新 flow。
	// 返回 flowRetryableError：gateway 收到后可重试（重新规划为本地执行或发到其他节点）。
	draining := fr.draining
	// 测试钩子：允许测试注入自定义的 draining 状态，覆盖真实的 fr.draining。
	if f.Cfg != nil {
		if knobs, ok := f.Cfg.TestingKnobs.Flowinfra.(*TestingKnobs); ok && knobs != nil && knobs.FlowRegistryDraining != nil {
			draining = knobs.FlowRegistryDraining()
		}
	}
	if draining {
		return &flowRetryableError{cause: errors.Errorf(
			"could not register flow because the registry is draining",
		)}
	}

	// 获取或创建 flowEntry。这里不会分配新 entry 的情况：
	// 如果 ConnectInboundStream（FlowStream RPC）先到，它已经通过 getEntryLocked
	// 创建了这个 entry 并设置了 waitCh。此时我们拿到的是同一个 entry 对象。
	entry := fr.getEntryLocked(id)

	// 防御：同一个 FlowID 不允许重复注册。
	if entry.flow != nil {
		return errors.Errorf(
			"flow already registered: flowID: %s.\n"+
				"Current flow: %+v\nExisting flow: %+v",
			f.spec.FlowID, f.spec, entry.flow.spec)
	}

	// 引用计数 +1。配对操作：
	//   - UnregisterFlow() 中 releaseEntryLocked → refCount--，归零时从 map 删除
	//   - ConnectInboundStream() 中也会 refCount++，其 defer 中 releaseEntryLocked
	// 引用计数保证了：只要还有等待者（ConnectInboundStream）或 flow 自身在运行，
	// entry 就不会被清理。
	entry.refCount++
	entry.flow = f
	entry.inboundStreams = inboundStreams

	// 唤醒所有等待者。如果 FlowStream RPC 先到，它的 waitForFlow() 正阻塞在
	// entry.waitCh 上（见 waitForFlow 实现：持锁创建 waitCh，释放锁后 select 等待）。
	// close(waitCh) 会让所有阻塞在 <-waitCh 上的 goroutine 立即返回。
	if entry.waitCh != nil {
		close(entry.waitCh)
	}

	// 如果这个 flow 期望有远程入站流（分布式查询才有，本地 flow 的 inboundStreams 为空），
	// 启动超时定时器。超时后，对所有尚未连接的 stream 推送错误，错误会沿 processor 链传播，
	// 最终导致整个 flow 失败。
	if len(inboundStreams) > 0 {
		entry.streamTimer = time.AfterFunc(timeout, func() {
			// 在独立的 timer goroutine 中执行，不持有 FlowRegistry 锁。
			// cancelPendingStreams 遍历所有 inboundStreams，对每个未连接的 stream：
			//   1. cancelIfNotConnected 检查是否已 connected/finished/canceled
			//   2. 如果都没有 → cancelLocked → finishLocked（调 WaitGroup.Done）
			//      → goroutine 中调 receiver.Timeout(err) 把错误推入数据流
			numTimedOutReceivers := fr.cancelPendingStreams(id, errNoInboundStreamConnection)
			if numTimedOutReceivers != 0 {
				// ctx 的 tracing span 此时可能已经结束，
				// 所以清除 span 信息避免日志携带无效 trace。
				timeoutCtx := tracing.ContextWithSpan(ctx, nil)
				log.Dev.Errorf(
					timeoutCtx,
					"flow id:%s : %d inbound streams timed out after %s; propagated error throughout flow",
					id,
					numTimedOutReceivers,
					timeout,
				)
			}
		})
	}
	return nil
}

// cancelPendingStreams 取消 flow 中所有尚未连接的入站流。
//
// 调用时机：由 RegisterFlow 中的 streamTimer 在超时后触发（timer goroutine 中执行）。
// 调用时不持有 FlowRegistry 锁。
//
// 对每个 InboundStreamInfo 调用 cancelIfNotConnected：
//   - 已连接的 stream（FlowStream RPC 已到达并完成握手）→ 跳过，由正常流程处理
//   - 已完成的 stream → 跳过
//   - 已取消的 stream → 跳过
//   - 其余（还在等 FlowStream RPC 到来）→ cancelLocked：
//       finishLocked() 调 WaitGroup.Done()，然后 goroutine 中 receiver.Timeout(err) 推送错误
//
// 返回实际被取消的 stream 数量。
func (fr *FlowRegistry) cancelPendingStreams(
	id execinfrapb.FlowID, pendingReceiverErr error,
) (numTimedOutReceivers int) {
	var inboundStreams map[execinfrapb.StreamID]*InboundStreamInfo
	// 短暂持锁获取 inboundStreams map 的引用。
	// inboundStreams 在 RegisterFlow 中设置后不会被修改，所以可以无锁访问。
	fr.Lock()
	entry := fr.flows[id]
	if entry != nil && entry.flow != nil {
		inboundStreams = entry.inboundStreams
	}
	fr.Unlock()
	if inboundStreams == nil {
		return 0
	}
	for _, is := range inboundStreams {
		// cancelIfNotConnected 内部使用 InboundStreamInfo 自己的 mutex，
		// 与 ConnectInboundStream 中设置 connected=true 互斥。
		if is.cancelIfNotConnected(pendingReceiverErr) {
			numTimedOutReceivers++
		}
	}
	return numTimedOutReceivers
}

// UnregisterFlow removes a flow from the registry. Any subsequent
// ConnectInboundStream calls for the flow will fail to find it and time out.
func (fr *FlowRegistry) UnregisterFlow(id execinfrapb.FlowID) {
	fr.Lock()
	defer fr.Unlock()
	entry := fr.flows[id]
	if entry.streamTimer != nil {
		entry.streamTimer.Stop()
		entry.streamTimer = nil
	}
	fr.releaseEntryLocked(id)
}

// waitForFlow 等待指定 flowID 的 flow 被注册，最多等 timeout 时长。
// 调用路径：ConnectInboundStream → waitForFlow。
// 调用时不应持有 FlowRegistry 锁。
//
// 两种结果：
//   1. flow 在超时前被注册（RegisterFlow close(waitCh)）→ 返回 FlowBase
//   2. 超时或 ctx 取消 → 返回 nil（调用方 ConnectInboundStream 会返回 error）
//
// 注意 select 之后重新持锁读取 entry.flow，因为在释放锁期间 flow 可能已被注册。
func (fr *FlowRegistry) waitForFlow(
	ctx context.Context, id execinfrapb.FlowID, timeout time.Duration,
) *FlowBase {
	fr.Lock()
	entry := fr.getEntryLocked(id)
	// 快速路径：在发送握手消息的间隙，flow 恰好被注册了。
	if entry.flow != nil {
		return entry.flow
	}

	// 创建/复用等待 channel。如果已有其他 ConnectInboundStream 调用者
	// 为同一个 flowID 创建了 waitCh，则复用同一个 channel（广播唤醒）。
	waitCh := entry.waitCh
	if waitCh == nil {
		waitCh = make(chan struct{})
		entry.waitCh = waitCh
	}
	fr.Unlock() // 释放锁，等待唤醒

	// 三种唤醒条件：
	//   waitCh 关闭 → RegisterFlow 已注册该 flow
	//   time.After → 超过 SettingFlowStreamTimeout（默认 10s）
	//   ctx.Done  → RPC 上下文被取消
	select {
	case <-waitCh:
	case <-time.After(timeout):
	case <-ctx.Done():
	}

	// 重新持锁读取结果：entry.flow 可能为 nil（超时/取消），也可能非 nil（注册成功）。
	fr.Lock()
	return entry.flow
}

// Drain waits at most flowDrainWait for currently running flows to finish and
// at least minFlowDrainWait for any incoming flows to be registered. If there
// are still flows active after flowDrainWait, Drain waits an extra
// expectedConnectionTime so that any flows that were registered at the end of
// the time window have a reasonable amount of time to connect to their
// consumers, thus unblocking them. All flows that are still running at this
// point are canceled.
//
// The FlowRegistry rejects any new flows once it has finished draining.
//
// Note that since local flows are not added to the registry, they are not
// waited for. However, this is fine since there should be no local flows
// running when the FlowRegistry drains as the draining logic starts with
// draining all client connections to a node.
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (fr *FlowRegistry) Drain(
	flowDrainWait time.Duration,
	minFlowDrainWait time.Duration,
	reporter func(int, redact.SafeString),
) {
	allFlowsDone := make(chan struct{}, 1)
	start := timeutil.Now()
	stopWaiting := false

	sleep := func(t time.Duration) {
		if fr.testingRunBeforeDrainSleep != nil {
			fr.testingRunBeforeDrainSleep()
		}
		time.Sleep(t)
	}

	defer func() {
		// At this stage, we have either hit the flowDrainWait timeout or we have no
		// flows left. We wait for an expectedConnectionTime longer so that we give
		// any flows that were registered in the
		// flowDrainWait - expectedConnectionTime window enough time to establish
		// connections to their consumers so that the consumers do not block for a
		// long time waiting for a connection to be established.
		fr.Lock()
		fr.draining = true
		if len(fr.flows) > 0 {
			fr.Unlock()
			time.Sleep(expectedConnectionTime)
			fr.Lock()
		}
		// Now cancel all still running flows.
		for _, f := range fr.flows {
			if f.flow != nil {
				// f.flow might be nil when ConnectInboundStream() was
				// called, but the consumer of that inbound stream hasn't
				// been scheduled yet.
				f.flow.Cancel()
			}
		}
		fr.Unlock()
	}()

	fr.Lock()
	if len(fr.flows) == 0 {
		fr.Unlock()
		sleep(minFlowDrainWait)
		fr.Lock()
		// No flows were registered, return.
		if len(fr.flows) == 0 {
			fr.Unlock()
			return
		}
	}
	if reporter != nil {
		// Report progress to the Drain RPC.
		reporter(len(fr.flows), "distSQL execution flows")
	}

	go func() {
		select {
		case <-time.After(flowDrainWait):
			fr.Lock()
			stopWaiting = true
			fr.flowDone.Signal()
			fr.Unlock()
		case <-allFlowsDone:
		}
	}()

	for !(stopWaiting || len(fr.flows) == 0) {
		fr.flowDone.Wait()
	}
	fr.Unlock()

	// If we spent less time waiting for all registered flows to finish, wait
	// for the minimum time for any new incoming flows and wait for these to
	// finish.
	waitTime := timeutil.Since(start)
	if waitTime < minFlowDrainWait {
		sleep(minFlowDrainWait - waitTime)
		fr.Lock()
		for !(stopWaiting || len(fr.flows) == 0) {
			fr.flowDone.Wait()
		}
		fr.Unlock()
	}

	allFlowsDone <- struct{}{}
}

// undrain causes the FlowRegistry to start accepting flows again.
func (fr *FlowRegistry) undrain() {
	fr.Lock()
	fr.draining = false
	fr.Unlock()
}

// ConnectInboundStream 将 FlowStream RPC 的入站 gRPC 流连接到已注册的 flow。
//
// 调用路径：FlowStream RPC → flowStreamInt() → ConnectInboundStream()
// 调用时机：远程节点的 outbox 向本节点发起 FlowStream RPC，携带 FlowID + StreamID。
//
// 与 RegisterFlow 的竞态处理（时序图见文件顶部注释）：
//
//   时序 1：SetupFlow 先到，FlowStream 后到
//     flow 已注册 → flow != nil → 跳过 waitForFlow → 直接查找 stream → 连接成功
//
//   时序 2：FlowStream 先到，SetupFlow 后到
//     flow 未注册 → 发握手(ConsumerScheduled=false) → waitForFlow 阻塞等待
//     → RegisterFlow 到来 close(waitCh) → waitForFlow 返回 → 查找 stream → 连接成功
//
//   时序 3：FlowStream 到来但 SetupFlow 超时未到
//     waitForFlow 超时返回 nil → 返回 error
//
// 两次握手的协议设计：
//   第一次握手（flow==nil 时）：通知 producer "consumer 还没就绪，你等着"
//   第二次握手（找到 flow 后）：通知 producer "consumer 已就绪，开始传数据"
//   这样避免 producer 在 consumer 未就绪时空转等待。
//
// 返回值：
//   FlowBase              - 数据要流入的 flow
//   InboundStreamHandler  - 数据接收者（processor 的 input synchronizer 或 vectorized inbox）
//   cleanup               - 调用方在数据传输完成后调用，最终触发 WaitGroup.Done()
//   error                 - 连接失败时非 nil，此时 cleanup 为 nil
func (fr *FlowRegistry) ConnectInboundStream(
	ctx context.Context,
	flowID execinfrapb.FlowID,
	streamID execinfrapb.StreamID,
	stream execinfrapb.RPCDistSQL_FlowStreamStream,
	timeout time.Duration,
) (_ *FlowBase, _ InboundStreamHandler, cleanup func(), retErr error) {
	// 获取/创建 flowEntry，获取当前 flow 的注册状态。
	fr.Lock()
	entry := fr.getEntryLocked(flowID)
	flow := entry.flow // 可能为 nil（SetupFlow 还没到）
	// 引用计数 +1，确保 entry 不会在本函数执行期间被清理。
	// 对应的 releaseEntryLocked 在 defer 中。
	entry.refCount++
	fr.Unlock()

	// defer 中释放本函数持有的引用。如果这是最后一个引用（flow 注册失败等），
	// releaseEntryLocked 会从 map 中删除 entry 并 signal flowDone。
	defer func() {
		fr.Lock()
		defer fr.Unlock()
		fr.releaseEntryLocked(flowID)
	}()

	// --- 情况 A：flow 尚未注册（FlowStream 先于 SetupFlow 到达）---
	if flow == nil {
		// 第一次握手：告诉 producer（远端 outbox）"我这边的 consumer 还没调度好，等着"。
		// ConsumerScheduled=false 表示 processor 还没就绪，
		// ConsumerScheduleDeadline 告诉 producer 最多等到什么时候。
		deadline := timeutil.Now().Add(timeout)
		if err := stream.Send(&execinfrapb.ConsumerSignal{
			Handshake: &execinfrapb.ConsumerHandshake{
				ConsumerScheduled:        false,
				ConsumerScheduleDeadline: &deadline,
			},
		}); err != nil {
			// 连握手消息都发不出去，说明 gRPC 连接有问题，直接失败。
			return nil, nil, nil, err
		}
		// 阻塞等待 flow 被注册。内部机制：
		//   getEntryLocked → 拿到的就是上面同一个 entry（已经存在）
		//   创建 waitCh → 放到 entry.waitCh
		//   释放锁 → select 等待 waitCh 关闭 / 超时 / ctx 取消
		//   RegisterFlow 到来时 close(waitCh) → waitForFlow 返回 FlowBase
		flow = fr.waitForFlow(ctx, flowID, timeout)
		if flow == nil {
			// 超时或取消：SetupFlow 没来得及到达。
			return nil, nil, nil, errors.Errorf("flow %s not found", flowID)
		}
	}

	// --- 此时 flow 一定不为 nil ---

	// 如果后续步骤出错（stream 找不到、握手失败、已被取消等），
	// 取消整个 flow。因为一个 inbound stream 失败意味着分布式查询无法完成，
	// 需要让所有节点上的相关 flow 都停止。
	defer func() {
		if retErr != nil {
			flow.Cancel()
		}
	}()

	// 从 entry.inboundStreams 中按 streamID 查找对应的 InboundStreamInfo。
	// 这个 map 在 Flow.Setup() 时创建，之后不会被修改，所以不需要持锁读取。
	s, ok := entry.inboundStreams[streamID]
	if !ok {
		return nil, nil, nil, errors.Errorf("flow %s: no inbound stream %d", flowID, streamID)
	}

	// 第二次握手：告诉 producer "consumer 已就绪，可以开始发数据了"。
	// ConsumerScheduled=true 是 producer 开始推送数据的信号。
	handshakeErr := stream.Send(&execinfrapb.ConsumerSignal{
		Handshake: &execinfrapb.ConsumerHandshake{
			ConsumerScheduled: true,
		},
	})

	// 持 stream 自己的锁，检查状态并标记为已连接。
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查 1：stream 是否已被取消（超时 timer 先触发）
	if s.mu.canceled {
		// 不管握手成不成功，这个 stream 已经被 timer 取消并完成了清理。
		return nil, nil, nil, errors.Errorf("flow %s: inbound stream %d came too late", flowID, streamID)
	}

	// 检查 2：握手是否失败
	if handshakeErr != nil {
		// 握手失败 → 取消这个 stream，推错误给 receiver，
		// 错误会沿 processor 链传播并 drain 整个 flow。
		s.cancelLocked(handshakeErr)
		return nil, nil, nil, handshakeErr
	}

	// 检查 3：是否重复连接（同一个 StreamID 被连接两次）
	if s.mu.connected {
		// 这是第二次 FlowStream RPC——另一个 goroutine 已经处理了同一个 stream。
		// 不需要再次清理，直接返回错误。
		return nil, nil, nil, errors.AssertionFailedf("flow %s: inbound stream %d already connected", flowID, streamID)
	}

	// 标记为已连接。此时 producer 可以安全地开始推送数据到 receiver。
	s.mu.connected = true

	// 返回给调用方（FlowStream RPC handler）：
	//   flow     - 用于获取 AmbientContext 做日志标注
	//   receiver - processor 的数据入口（row-based: input synchronizer; vectorized: inbox）
	//   finish   - 数据传输完成后调用，内部调 onFinish() → WaitGroup.Done()
	return flow, s.receiver, s.finish, nil
}
