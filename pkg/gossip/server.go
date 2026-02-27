// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gossip

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type serverInfo struct {
	createdAt time.Time
	peerID    roachpb.NodeID
}

// server maintains an array of connected peers to which it gossips
// newly arrived information on a periodic basis.
type server struct {
	log.AmbientContext

	clusterID *base.ClusterIDContainer
	NodeID    *base.NodeIDContainer

	stopper *stop.Stopper

	mu struct {
		syncutil.RWMutex
		is          *infoStore                         // InfoStore：存储所有 gossip 信息
		incoming    nodeSet                            // 入向连接的节点集合
		nodeMap     map[util.UnresolvedAddr]serverInfo // 入向客户端地址 -> 信息
		lastTighten time.Time                          // 最后一次 tighten 网络的时间
	}

	// ready broadcasts a wakeup to waiting gossip requests. This is done
	// via closing the current ready channel and opening a new one. This
	// is required due to the fact that condition variables are not
	// composable. There's an open proposal to add them:
	// https://github.com/golang/go/issues/16620
	//在 newServer 函数中，初始化一个未关闭的 channel。这个 channel 的状态：
	//- 初始状态：打开（open），未关闭
	//- 等待行为：任何在 select 中等待 <-ready 的 goroutine 会阻塞
	//- 关闭后：关闭的 channel 会立即唤醒所有等待的 goroutine
	//### 3.2 为什么使用”关闭 channel”而不是”发送信号”？
	//
	//**方案对比**：
	//
	//| 方案 | 实现 | 问题 |
	//| --- | --- | --- |
	//| **关闭 channel（当前）** | `close(oldChan)` | ✅ 可以唤醒**所有**等待者 |
	//| **发送信号** | `oldChan <- struct{}{}` | ❌ 只能唤醒**一个**等待者（需要循环发送） |
	ready   atomic.Value  // 类型为 chan struct{}，用于唤醒等待的 gossip 请求
	tighten chan struct{} // 用于触发网络 tighten 操作

	nodeMetrics   Metrics // 节点级别的 metrics
	serverMetrics Metrics // 服务器级别的 metrics

	simulationCycler *sync.Cond // Used when simulating the network to signal next cycle
}

// newServer creates and returns a server struct.
func newServer(
	ambient log.AmbientContext,
	clusterID *base.ClusterIDContainer,
	nodeID *base.NodeIDContainer,
	stopper *stop.Stopper,
	registry *metric.Registry,
) *server {
	s := &server{
		AmbientContext: ambient,
		clusterID:      clusterID,
		NodeID:         nodeID,
		stopper:        stopper,
		tighten:        make(chan struct{}, 1),
		nodeMetrics:    makeMetrics(),
		serverMetrics:  makeMetrics(),
	}

	s.mu.is = newInfoStore(s.AmbientContext, nodeID, util.UnresolvedAddr{}, stopper, s.nodeMetrics)
	s.mu.incoming = makeNodeSet(minPeers, metric.NewGauge(MetaConnectionsIncomingGauge))
	s.mu.nodeMap = make(map[util.UnresolvedAddr]serverInfo)
	s.ready.Store(make(chan struct{}))

	registry.AddMetric(s.mu.incoming.gauge)
	registry.AddMetricStruct(s.nodeMetrics)

	return s
}

// GetNodeMetrics returns this server's node metrics struct.
func (s *server) GetNodeMetrics() *Metrics {
	return &s.nodeMetrics
}

// Gossip receives gossiped information from a peer node.
// The received delta is combined with the infostore, and this
// node's own gossip is returned to requesting client.
func (s *server) Gossip(stream Gossip_GossipServer) error {
	return s.gossip(stream)
}

// gossip is the shared implementation for Gossip for both gRPC and DRPC.
func (s *server) gossip(stream RPCGossip_GossipStream) error {
	// 1️⃣ 接收初始请求
	args, err := stream.Recv()
	if err != nil {
		return err
	}
	if (args.ClusterID != uuid.UUID{}) && args.ClusterID != s.clusterID.Get() {
		return errors.Errorf("gossip connection refused from different cluster %s", args.ClusterID)
	}
	// 2️⃣ 校验集群 ID
	ctx, cancel := context.WithCancel(s.AnnotateCtx(stream.Context()))
	defer cancel()
	syncChan := make(chan struct{}, 1) // 保证 send 串行化\
	// 3️⃣ 封装 send 函数（加 metrics 和并发控制
	send := func(reply *Response) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case syncChan <- struct{}{}:
			defer func() { <-syncChan }()

			bytesSent := int64(reply.Size())
			infoCount := int64(len(reply.Delta))
			s.nodeMetrics.BytesSent.Inc(bytesSent)
			s.nodeMetrics.InfosSent.Inc(infoCount)
			s.nodeMetrics.MessagesSent.Inc(1)
			s.serverMetrics.BytesSent.Inc(bytesSent)
			s.serverMetrics.InfosSent.Inc(infoCount)
			s.serverMetrics.MessagesSent.Inc(1)

			return stream.Send(reply)
		}
	}
	// syncChan acts as a semaphore to serialize stream.Send calls and
	// ensures graceful draining of in-flight sends on function exit.
	defer func() { syncChan <- struct{}{} }() // 确保 send 完成

	errCh := make(chan error, 1)

	// Maintain what were the recently sent high water stamps to avoid resending
	// them.
	lastSentHighWaterStamps := make(map[roachpb.NodeID]int64)
	// 4️⃣ 启动接收 goroutine
	if err := s.stopper.RunAsyncTask(ctx, "gossip receiver", func(ctx context.Context) {
		errCh <- s.gossipReceiver(ctx, &args, &lastSentHighWaterStamps, send, stream.Recv)
	}); err != nil {
		return err
	}

	reply := new(Response)

	// 5️⃣ 发送 goroutine 主循环
	for init := true; ; init = false {
		// Remember the old ready so that if it gets replaced with a new one and is
		// closed, we still trigger the select below.
		ready := s.ready.Load().(chan struct{})
		s.mu.Lock()
		delta := s.mu.is.delta(args.HighWaterStamps)
		if init {
			s.mu.is.populateMostDistantMarkers(delta)
		}
		if args.HighWaterStamps == nil {
			args.HighWaterStamps = make(map[roachpb.NodeID]int64)
		}

		// Send a response if this is the first response on the connection, or if
		// there are deltas to send. The first condition is necessary to make sure
		// the remote node receives our high water stamps in a timely fashion.
		if infoCount := len(delta); init || infoCount > 0 {
			// 更新客户端的高水位
			if log.V(1) {
				log.Dev.Infof(ctx, "returning %d info(s) to n%d: %s",
					infoCount, args.NodeID, extractKeys(delta))
			}
			// Ensure that the high water stamps for the remote client are kept up to
			// date so that we avoid resending the same gossip infos as infos are
			// updated locally.
			for _, i := range delta {
				ratchetHighWaterStamp(args.HighWaterStamps, i.NodeID, i.OrigStamp)
			}

			var diffStamps map[roachpb.NodeID]int64
			lastSentHighWaterStamps, diffStamps =
				s.mu.is.getHighWaterStampsWithDiff(lastSentHighWaterStamps)
			*reply = Response{
				NodeID:          s.NodeID.Get(),
				HighWaterStamps: diffStamps,
				Delta:           delta,
			}

			s.mu.Unlock()
			if err := send(reply); err != nil {
				return err
			}
		} else {
			s.mu.Unlock()
		}

		select {
		case <-s.stopper.ShouldQuiesce():
			return nil
		case err := <-errCh:
			return err
		case <-ready:
			// We just sleep here instead of calling batchAndConsume() because the
			// channel is closed, and sleeping won't block the sender of the channel.
			time.Sleep(infosBatchDelay)
		}
	}
}

func (s *server) gossipReceiver(
	ctx context.Context,
	argsPtr **Request,
	lastSentHighWaterStampsPtr *map[roachpb.NodeID]int64,
	senderFn func(*Response) error,
	receiverFn func() (*Request, error),
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	reply := new(Response)

	// Track whether we've decided whether or not to admit the gossip connection
	// from this node. We only want to do this once so that we can do a duplicate
	// connection check based on node ID here.
	nodeIdentified := false

	// This loop receives gossip from the client. It does not attempt to send the
	// server's gossip to the client.
	for {
		args := *argsPtr
		if args.NodeID == 0 {
			// Let the connection through so that the client can get a node ID. Once it
			// has one, we'll run the logic below to decide whether to keep the
			// connection to it or to forward it elsewhere.
			// 初始连接，允许通过以便对方获取 node ID
			log.Dev.Infof(ctx, "received initial cluster-verification connection from %s", args.Addr)
		} else if !nodeIdentified {
			nodeIdentified = true

			// Decide whether or not we can accept the incoming connection
			// as a permanent peer.
			if args.NodeID == s.NodeID.Get() {
				// This is an incoming loopback connection which should be closed by
				// the client.
				// 回环连接，忽略
				if log.V(2) {
					log.Dev.Infof(ctx, "ignoring gossip from n%d (loopback)", args.NodeID)
				}
			} else if _, ok := s.mu.nodeMap[args.Addr]; ok {
				// 重复连接（可能通过 load balancer），拒绝
				// This is a duplicate incoming connection from the same node as an existing
				// connection. This can happen when bootstrap connections are initiated
				// through a load balancer.
				if log.V(2) {
					log.Dev.Infof(ctx, "duplicate connection received from n%d at %s", args.NodeID, args.Addr)
				}
				return errors.Errorf("duplicate connection from node at %s", args.Addr)
			} else if s.mu.incoming.hasSpace() {
				// 有空间，接受连接
				log.VEventf(ctx, 2, "adding n%d to incoming set", args.NodeID)

				s.mu.incoming.addNode(args.NodeID)
				s.mu.nodeMap[args.Addr] = serverInfo{
					peerID:    args.NodeID,
					createdAt: timeutil.Now(),
				}

				//nolint:deferloop (this happens at most once).
				defer func(nodeID roachpb.NodeID, addr util.UnresolvedAddr) {
					log.VEventf(ctx, 2, "removing n%d from incoming set", args.NodeID)
					s.mu.incoming.removeNode(nodeID)
					delete(s.mu.nodeMap, addr)
				}(args.NodeID, args.Addr)
			} else {
				// 满载，转发到随机的已连接节点
				// If we don't have any space left, forward the client along to a peer.
				var alternateAddr util.UnresolvedAddr
				var alternateNodeID roachpb.NodeID
				// Choose a random peer for forwarding.
				//1. 为什么不直接用 for...range 的第一个？
				//如果你直接写 for addr := range s.mu.nodeMap { selected = addr; break }，会存在两个主要问题：
				//
				//A. 概率分布不均匀 (Non-Uniform Distribution)
				//Go map 的底层结构是由 Buckets（桶） 组成的。迭代器随机选择一个桶作为起点，然后遍历桶内的 8 个单元。
				//
				//如果某些桶里的元素比较稀疏，而某些桶很满，那么处于稀疏桶中的元素被选中的概率会显著高于满桶中的元素。
				//
				//在分布式系统（如 CockroachDB 的 Gossip 协议）中，我们需要的是真正的均匀随机，否则某些节点可能会被频繁选中，而某些节点长时间被“冷落”，导致信息同步不及时。
				//
				//B. 官方的随机性只是“扰动”
				//Go 运行时（Runtime）对 map 的随机处理主要是为了防止程序逻辑依赖特定顺序。它并不保证采样符合概率统计上的均匀分布。
				altIdx := rand.Intn(len(s.mu.nodeMap))
				for addr, info := range s.mu.nodeMap {
					if altIdx == 0 {
						alternateAddr = addr
						alternateNodeID = info.peerID
						break
					}
					altIdx--
				}

				s.nodeMetrics.ConnectionsRefused.Inc(1)
				log.Dev.Infof(ctx, "refusing gossip from n%d (max %d conns); forwarding to n%d (%s)",
					args.NodeID, s.mu.incoming.maxSize, alternateNodeID, alternateAddr)

				*reply = Response{
					NodeID:          s.NodeID.Get(),
					AlternateAddr:   &alternateAddr,
					AlternateNodeID: alternateNodeID,
				}

				s.mu.Unlock()
				err := senderFn(reply)
				s.mu.Lock()
				// Naively, we would return err here unconditionally, but that
				// introduces a race. Specifically, the client may observe the
				// end of the connection before it has a chance to receive and
				// process this message, which instructs it to hang up anyway.
				// Instead, we send the message and proceed to gossip
				// normally, depending on the client to end the connection.
				if err != nil {
					return err
				}
			}
		}

		bytesReceived := int64(args.Size())
		infosReceived := int64(len(args.Delta))
		s.nodeMetrics.BytesReceived.Inc(bytesReceived)
		s.nodeMetrics.InfosReceived.Inc(infosReceived)
		s.nodeMetrics.MessagesReceived.Inc(1)
		s.serverMetrics.BytesReceived.Inc(bytesReceived)
		s.serverMetrics.InfosReceived.Inc(infosReceived)
		s.serverMetrics.MessagesReceived.Inc(1)

		freshCount, err := s.mu.is.combine(args.Delta, args.NodeID)
		if err != nil {
			log.Dev.Warningf(ctx, "failed to fully combine gossip delta from n%d: %s", args.NodeID, err)
		}
		if log.V(1) {
			log.Dev.Infof(ctx, "received %s from n%d (%d fresh)", extractKeys(args.Delta), args.NodeID, freshCount)
		}
		s.maybeTightenLocked()

		var diffStamps map[roachpb.NodeID]int64
		*lastSentHighWaterStampsPtr, diffStamps =
			s.mu.is.getHighWaterStampsWithDiff(*lastSentHighWaterStampsPtr)
		*reply = Response{
			NodeID:          s.NodeID.Get(),
			HighWaterStamps: diffStamps,
		}

		s.mu.Unlock()
		err = senderFn(reply)
		s.mu.Lock()
		if err != nil {
			return err
		}

		if cycler := s.simulationCycler; cycler != nil {
			cycler.Wait()
		}

		s.mu.Unlock()
		recvArgs, err := receiverFn()
		s.mu.Lock()
		if err != nil {
			return err
		}

		// *argsPtr holds the remote peer state; we need to update it whenever we
		// receive a new non-nil request. We avoid assigning to *argsPtr directly
		// because the gossip sender above has closed over *argsPtr and will NPE if
		// *argsPtr were set to nil.
		mergeHighWaterStamps(&recvArgs.HighWaterStamps, (*argsPtr).HighWaterStamps)
		*argsPtr = recvArgs
	}
}

// **何时调用 `maybeTightenLocked()`？**
// 1. **服务端收到 gossip delta**（`server.gossipReceiver`）
// 2. **客户端收到 gossip delta**（`client.handleResponse`）
//
// **为什么频繁检查？**
// - 每次收到新信息时，可能发现某些节点的跳数过高
// - 及时 tighten 可以加速信息传播
func (s *server) maybeTightenLocked() {
	now := timeutil.Now()
	if now.Before(s.mu.lastTighten.Add(gossipTightenInterval)) {
		return // 距离上次 tighten 不到 1 秒，跳过
	}
	select {
	case s.tighten <- struct{}{}:
	default: // channel 已满，跳过
	}
}

// start initializes the infostore with the rpc server address and
// then begins processing connecting clients in an infinite select
// loop via goroutine. Periodically, clients connected and awaiting
// the next round of gossip are awoken via the conditional variable.
func (s *server) start(addr net.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.is.NodeAddr = util.MakeUnresolvedAddr(addr.Network(), addr.String())

	broadcast := func() {
		// 关闭旧的 ready channel 并创建新的，广播给所有等待者
		// Close the old ready and open a new one. This will broadcast to all
		// receivers and setup a fresh channel to replace the closed one.
		close(s.ready.Swap(make(chan struct{})).(chan struct{}))
	}

	// We require redundant callbacks here as the broadcast callback is
	// propagating gossip infos to other nodes and needs to propagate the new
	// expiration info.
	// 注册回调：任何 gossip 信息变更时广播通知
	unregister := s.mu.is.registerCallback(".*", func(_ string, _ roachpb.Value, _ int64) {
		broadcast()
	}, Redundant /*Redundant 回调：即使信息内容未变（只是更新了 TTL），也需要广播，因为过期信息的传播同样重要。*/)
	// 在 stopper 关闭时取消注册
	waitQuiesce := func(context.Context) {
		<-s.stopper.ShouldQuiesce()

		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			unregister()
		}()

		broadcast()
	}
	bgCtx := s.AnnotateCtx(context.Background())
	if err := s.stopper.RunAsyncTask(bgCtx, "gossip-wait-quiesce", waitQuiesce); err != nil {
		waitQuiesce(bgCtx)
	}
}

func (s *server) status() ServerStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var status ServerStatus
	status.ConnStatus = make([]ConnStatus, 0, len(s.mu.nodeMap))
	status.MaxConns = int32(s.mu.incoming.maxSize)
	status.MetricSnap = s.serverMetrics.Snapshot()

	for addr, info := range s.mu.nodeMap {
		status.ConnStatus = append(status.ConnStatus, ConnStatus{
			NodeID:   info.peerID,
			Address:  addr.String(),
			AgeNanos: timeutil.Since(info.createdAt).Nanoseconds(),
		})
	}
	return status
}

func roundSecs(d time.Duration) time.Duration {
	return time.Duration(d.Seconds()+0.5) * time.Second
}

// GetNodeAddr returns the node's address stored in the Infostore.
func (s *server) GetNodeAddr() *util.UnresolvedAddr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &s.mu.is.NodeAddr
}
