// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gossip

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	drpc "storj.io/drpc"
)

// client is a client-side RPC connection to a gossip peer node.
type client struct {
	log.AmbientContext

	createdAt time.Time
	// peerID is the node ID of the peer we're connected to. This is set when we
	// receive a response from the peer. The gossip mu should be held when
	// accessing.
	peerID              roachpb.NodeID       // 对端节点 ID（收到响应后设置）
	resolvedPlaceholder bool                 // 是否已将 placeholder 解析为实际 NodeID
	addr                net.Addr             // 对端地址
	locality            roachpb.Locality     // 对端 locality（如果已知）
	forwardAddr         *util.UnresolvedAddr // 如果被转发，记录转发地址

	prevHighWaterStamps   map[roachpb.NodeID]int64 // 上次发送给对端的高水位
	remoteHighWaterStamps map[roachpb.NodeID]int64 // 对端的高水位

	closer        chan struct{} // 用于关闭客户端的 channel
	clientMetrics Metrics       // 客户端 metrics
	nodeMetrics   Metrics       // 引用 node 级别的 metrics
}

// extractKeys returns a string representation of a gossip delta's keys.
func extractKeys(delta map[string]*Info) string {
	keys := make([]string, 0, len(delta))
	for key := range delta {
		keys = append(keys, key)
	}
	return fmt.Sprintf("%s", keys)
}

// newClient creates and returns a client struct.
func newClient(
	ambient log.AmbientContext, addr net.Addr, locality roachpb.Locality, nodeMetrics Metrics,
) *client {
	return &client{
		AmbientContext:        ambient,
		createdAt:             timeutil.Now(),
		addr:                  addr,
		locality:              locality,
		remoteHighWaterStamps: map[roachpb.NodeID]int64{},
		closer:                make(chan struct{}),
		clientMetrics:         makeMetrics(),
		nodeMetrics:           nodeMetrics,
	}
}

var logFailedStartEvery = log.Every(5 * time.Second)

// start dials the remote addr and commences gossip once connected. Upon exit,
// the client is sent on the disconnected channel. This method starts client
// processing in a goroutine and returns immediately.
// ### 核心职责
//
// 1. **建立 RPC 连接**：通过 gRPC 或 DRPC 连接到对端节点
// 2. **启动 Gossip 流**：持续交换 gossip delta
// 3. **处理连接生命周期**：连接成功、失败、断开的处理
func (c *client) startLocked(
	g *Gossip, disconnected chan *client, rpcCtx *rpc.Context, stopper *stop.Stopper,
) {
	// Add a placeholder for the new outgoing connection because we may not know
	// the ID of the node we're connecting to yet. This will be resolved in
	// (*client).handleResponse once we know the ID.
	// Step 1: 添加 placeholder（占位符）
	g.outgoing.addPlaceholder() // 在 startClientLocked 调用前

	ctx, cancel := context.WithCancel(c.AnnotateCtx(context.Background()))
	if err := stopper.RunAsyncTask(ctx, "gossip-client", func(ctx context.Context) {
		var wg sync.WaitGroup
		defer func() {
			// This closes the outgoing stream, causing any attempt to send or
			// receive to return an error.
			//
			// Note: it is still possible for incoming gossip to be processed after
			// this point.
			cancel() // 关闭出向流

			// The stream is closed, but there may still be some incoming gossip
			// being processed. Wait until that is complete to avoid racing the
			// client's removal against the discovery of its remote's node ID.
			wg.Wait()         // 等待入向 gossip 处理完成
			disconnected <- c // 通知 management goroutine
		}()
		// 2️⃣ 建立连接和 gossip 流
		stream, err := func() (RPCGossip_GossipClient, error) {
			gc, err := c.dialGossipClient(ctx, rpcCtx)
			if err != nil {
				return nil, err
			}

			stream, err := gc.Gossip(ctx)
			if err != nil {
				return nil, err
			}
			// 3️⃣ 发送初始请求（包含本节点的 HighWaterStamps）
			if err := c.requestGossip(g, stream); err != nil {
				return nil, err
			}
			return stream, nil
		}()
		if err != nil {
			if logFailedStartEvery.ShouldLog() {
				log.Dev.Warningf(ctx, "failed to start gossip client to %s: %s", c.addr, err)
			}
			return
		}

		// Start gossiping.
		log.Dev.Infof(ctx, "started gossip client to n%d (%s)", c.peerID, c.addr)
		// 4️⃣ 启动 gossip 循环
		if err := c.gossip(ctx, g, stream, stopper, &wg); err != nil {
			if !grpcutil.IsClosedConnection(err) {
				peerID, addr := func() (roachpb.NodeID, net.Addr) {
					g.mu.RLock()
					defer g.mu.RUnlock()
					return c.peerID, c.addr
				}()
				if peerID != 0 {
					log.Dev.Infof(ctx, "closing client to n%d (%s): %s", peerID, addr, err)
				} else {
					log.Dev.Infof(ctx, "closing client to %s: %s", addr, err)
				}
			}
		}
	}); err != nil {
		disconnected <- c
	}
}

// close stops the client gossip loop and returns immediately.
func (c *client) close() {
	select {
	case <-c.closer:
	default:
		close(c.closer)
	}
}

// requestGossip requests the latest gossip from the remote server by
// supplying a map of this node's knowledge of other nodes' high water
// timestamps.
// ### 职责
//
// 发送初始的 `Request`，告诉服务端：
// 1. **我是谁**：`NodeID` 和 `Addr`
// 2. **我知道什么**：`HighWaterStamps`（各个 Node 的最新时间戳）
// 3. **我属于哪个集群**：`ClusterID`
func (c *client) requestGossip(g *Gossip, stream RPCGossip_GossipClient) error {
	// 1️⃣ 获取本节点的信息（需要持有读锁）
	nodeAddr, highWaterStamps := func() (util.UnresolvedAddr, map[roachpb.NodeID]int64) {
		g.mu.RLock()
		defer g.mu.RUnlock()
		return g.mu.is.NodeAddr, g.mu.is.getHighWaterStamps()
	}()
	// 2️⃣ 构造请求

	args := &Request{
		NodeID:          g.NodeID.Get(),
		Addr:            nodeAddr,
		HighWaterStamps: highWaterStamps,
		ClusterID:       g.clusterID.Get(),
	}
	// 3️⃣ 记录 metrics 和状态

	bytesSent := int64(args.Size())
	c.clientMetrics.BytesSent.Inc(bytesSent)
	c.nodeMetrics.BytesSent.Inc(bytesSent)
	c.prevHighWaterStamps = args.HighWaterStamps

	return stream.Send(args)
}

// sendGossip sends the latest gossip to the remote server, based on
// the remote server's notion of other nodes' high water timestamps.
func (c *client) sendGossip(g *Gossip, stream RPCGossip_GossipClient, firstReq bool) error {
	g.mu.Lock()
	delta := g.mu.is.delta(c.remoteHighWaterStamps)
	if firstReq {
		g.mu.is.populateMostDistantMarkers(delta)
	}
	if len(delta) > 0 {
		// Ensure that the high water stamps for the remote server are kept up to
		// date so that we avoid resending the same gossip infos as infos are
		// updated locally.
		for _, i := range delta {
			ratchetHighWaterStamp(c.remoteHighWaterStamps, i.NodeID, i.OrigStamp)
		}

		// Only send the high water stamps that are different from the previously
		// sent high water stamps.
		var diffStamps map[roachpb.NodeID]int64
		c.prevHighWaterStamps, diffStamps = g.mu.is.getHighWaterStampsWithDiff(c.prevHighWaterStamps)

		args := Request{
			NodeID:          g.NodeID.Get(),
			Addr:            g.mu.is.NodeAddr,
			Delta:           delta,
			HighWaterStamps: diffStamps,
			ClusterID:       g.clusterID.Get(),
		}

		bytesSent := int64(args.Size())
		infosSent := int64(len(delta))
		c.clientMetrics.BytesSent.Inc(bytesSent)
		c.clientMetrics.InfosSent.Inc(infosSent)
		c.clientMetrics.MessagesSent.Inc(1)
		c.nodeMetrics.BytesSent.Inc(bytesSent)
		c.nodeMetrics.InfosSent.Inc(infosSent)
		c.nodeMetrics.MessagesSent.Inc(1)

		if log.V(1) {
			ctx := c.AnnotateCtx(stream.Context())
			if c.peerID != 0 {
				log.Dev.Infof(ctx, "sending %s to n%d (%s)", extractKeys(args.Delta), c.peerID, c.addr)
			} else {
				log.Dev.Infof(ctx, "sending %s to %s", extractKeys(args.Delta), c.addr)
			}
		}
		g.mu.Unlock()
		return stream.Send(&args)
	}
	g.mu.Unlock()
	return nil
}

// handleResponse handles errors, remote forwarding, and combines delta
// gossip infos from the remote server with this node's infostore.
// 处理服务端响应
// ### 核心职责
//
// 1. **合并 Gossip Delta**：将对端的信息合并到本地 InfoStore
// 2. **解析 Placeholder**：首次收到 `reply.NodeID` 时解析 placeholder
// 3. **处理转发**：如果对端满载，接受转发到其他节点
// 4. **检测重复连接**：避免同一对节点之间的多条连接
func (c *client) handleResponse(ctx context.Context, g *Gossip, reply *Response) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	// 1️⃣ 记录 metrics
	bytesReceived := int64(reply.Size())
	infosReceived := int64(len(reply.Delta))
	c.clientMetrics.BytesReceived.Inc(bytesReceived)
	c.clientMetrics.InfosReceived.Inc(infosReceived)
	c.clientMetrics.MessagesReceived.Inc(1)
	c.nodeMetrics.BytesReceived.Inc(bytesReceived)
	c.nodeMetrics.InfosReceived.Inc(infosReceived)
	c.nodeMetrics.MessagesReceived.Inc(1)
	// 2️⃣ 合并 gossip delta
	// Combine remote node's infostore delta with ours.
	if reply.Delta != nil {
		freshCount, err := g.mu.is.combine(reply.Delta, reply.NodeID)
		if err != nil {
			log.Dev.Warningf(ctx, "failed to fully combine delta from n%d: %s", reply.NodeID, err)
		}
		if infoCount := len(reply.Delta); infoCount > 0 {
			if log.V(1) {
				log.Dev.Infof(ctx, "received %s from n%d (%d fresh)", extractKeys(reply.Delta), reply.NodeID, freshCount)
			}
		}
		g.maybeTightenLocked()
	}
	// 3️⃣ 记录对端 NodeID 和高水位
	c.peerID = reply.NodeID
	mergeHighWaterStamps(&c.remoteHighWaterStamps, reply.HighWaterStamps)

	// If we haven't yet recorded which node ID we're connected to in the outgoing
	// nodeSet, do so now. Note that we only want to do this if the peer has a
	// node ID allocated (i.e. if it's nonzero), because otherwise it could change
	// after we record it.
	// Step 2: 连接成功后，解析 placeholder
	if !c.resolvedPlaceholder && c.peerID != 0 {
		c.resolvedPlaceholder = true
		g.outgoing.resolvePlaceholder(c.peerID)
	}

	// Handle remote forwarding.
	// 5️⃣ 处理转发
	if reply.AlternateAddr != nil {
		if g.hasIncomingLocked(reply.AlternateNodeID) || g.hasOutgoingLocked(reply.AlternateNodeID) {
			return errors.Errorf(
				"received forward from n%d to n%d (%s); already have active connection, skipping",
				reply.NodeID, reply.AlternateNodeID, reply.AlternateAddr)
		}
		c.forwardAddr = reply.AlternateAddr
		return errors.Errorf("received forward from n%d to n%d (%s)",
			reply.NodeID, reply.AlternateNodeID, reply.AlternateAddr)
	}

	// Check whether we're connected at this point.
	// 6️⃣ 检查连接状态
	g.signalConnectedLocked()

	// Check whether this outgoing client is duplicating work already
	// being done by an incoming client, either because an outgoing
	// matches an incoming or the client is connecting to itself.
	// 7️⃣ 检测重复连接
	if nodeID := g.NodeID.Get(); nodeID == c.peerID {
		return errors.Errorf("stopping outgoing client to n%d (%s); loopback connection", c.peerID, c.addr)
	} else if g.hasIncomingLocked(c.peerID) && nodeID > c.peerID {
		// 双向连接冲突解决：NodeID 较大的一方关闭出向连接
		// To avoid mutual shutdown, we only shutdown our client if our
		// node ID is higher than the peer's.
		return errors.Errorf("stopping outgoing client to n%d (%s); already have incoming", c.peerID, c.addr)
	}

	return nil
}

// gossip loops, sending deltas of the infostore and receiving deltas
// in turn. If an alternate is proposed on response, the client addr
// is modified and method returns for forwarding by caller.
func (c *client) gossip(
	ctx context.Context,
	g *Gossip,
	stream RPCGossip_GossipClient,
	stopper *stop.Stopper,
	wg *sync.WaitGroup,
) error {
	sendGossipChan := make(chan struct{}, 1)

	// Register a callback for gossip updates.
	// 1️⃣ 注册回调：本地信息变更时触发发送
	updateCallback := func(_ string, _ roachpb.Value, _ int64) {
		select {
		//- **批量发送**：多个信息变更只触发一次发送
		//- **背压控制**：如果发送速度跟不上，跳过部分触发（下次发送会包含所有未发送的 delta）
		case sendGossipChan <- struct{}{}: // 非阻塞发送
		default: // 如果 channel 已满，跳过（已有待处理的发送任务）
		}
	}

	errCh := make(chan error, 1)
	initCh := make(chan struct{}, 1)

	// This wait group is used to allow the caller to wait until gossip
	// processing is terminated.
	wg.Add(1)
	// 2️⃣ 启动接收 goroutine
	if err := stopper.RunAsyncTask(ctx, "client-gossip", func(ctx context.Context) {
		defer wg.Done()

		errCh <- func() error {
			initCh := initCh
			for init := true; ; init = false {
				reply, err := stream.Recv()
				if err != nil {
					return err
				}
				if err := c.handleResponse(ctx, g, reply); err != nil {
					return err
				}
				if init {
					initCh <- struct{}{} // 首次响应后通知主循环
				}
			}
		}()
	}); err != nil {
		wg.Done()
		return err
	}

	// We attempt to defer registration of the callback until we've heard a
	// response from the remote node which will contain the remote's high water
	// stamps. This prevents the client from sending all of its infos to the
	// remote (which would happen if we don't know the remote's high water
	// stamps). Unfortunately, versions of cockroach before 2.1 did not always
	// send a response when receiving an incoming connection, so we also start a
	// timer and perform initialization after 1s if we haven't heard from the
	// remote.
	// 3️⃣ 延迟注册回调（等待首次响应或 1 秒超时）
	//**1. 为什么延迟注册回调？**
	//
	//**问题**：如果在连接建立后立即注册回调，本地的所有 info 都会触发发送，导致**全量同步**。
	//
	//**解决方案**：
	//- 等待首次响应（包含对端的 `HighWaterStamps`）
	//- 或者超时 1 秒（防止旧版本节点不发送初始响应）
	//- 之后才注册回调，实现**增量同步**
	//
	var unregister func()
	defer func() {
		if unregister != nil {
			unregister()
		}
	}()
	maybeRegister := func() {
		if unregister == nil {
			// We require redundant callbacks here as the update callback is
			// propagating gossip infos to other nodes and needs to propagate the new
			// expiration info.
			unregister = g.RegisterCallback(".*", updateCallback, Redundant)
		}
	}
	// 版本 < 2.1 的节点可能不发送初始响应
	// 所以设置 1 秒超时，强制注册回调
	initTimer := time.NewTimer(time.Second)
	defer initTimer.Stop()
	// 4️⃣ 主循环：处理发送和控制信号
	for count := 0; ; {
		select {
		//manage() goroutine
		//  ↓
		//c.close()  // 关闭 c.closer channel
		//  ↓
		//c.gossip() 主循环检测到 c.closer 关闭
		//  ↓
		//c.gossip() 返回 nil
		//  ↓
		//startLocked() 的 defer 函数执行
		//  ↓
		//disconnected <- c  // 向 channel 发送 client
		//  ↓
		//manage() goroutine 的 select 接收到 disconnected
		//  ↓
		//doDisconnected(c, rpcContext)  // 处理断开连接
		case <-c.closer: // 客户端被关闭
			return nil
		case <-stopper.ShouldQuiesce(): // 节点正在关闭
			return nil
		case err := <-errCh: // 接收 goroutine 出错
			return err
		case <-initCh:
			maybeRegister() // 收到首次响应，注册回调
		case <-initTimer.C:
			maybeRegister() // 超时 1 秒，强制注册回调
		case <-sendGossipChan:
			// We need to send the gossip delta to the remote server. Wait a bit to
			// batch the updates in one message.
			// 5️⃣ 批量发送 gossip delta
			batchAndConsume(sendGossipChan, infosBatchDelay)
			if err := c.sendGossip(g, stream, count == 0); err != nil {
				return err
			}
			count++
		}
	}
}

// dials the peer node and returns a gRPC connection to the peer node.
func (c *client) dial(ctx context.Context, rpcCtx *rpc.Context) (*grpc.ClientConn, error) {
	// Note: avoid using `grpc.WithBlock` here. This code is already
	// asynchronous from the caller's perspective, so the only effect of
	// `WithBlock` here is blocking shutdown - at the time of this writing,
	// that ends ups up making `kv` tests take twice as long.
	var conn *rpc.GRPCConnection
	if c.peerID != 0 {
		conn = rpcCtx.GRPCDialNode(c.addr.String(), c.peerID, c.locality, rpcbase.SystemClass)
	} else {
		// TODO(baptist): Use this as a temporary connection for getting
		// onto gossip and then replace with a validated connection.
		log.Dev.Infof(ctx, "unvalidated bootstrap gossip dial to %s", c.addr)
		conn = rpcCtx.GRPCUnvalidatedDial(c.addr.String(), c.locality)
	}

	return conn.Connect(ctx)
}

// dials the peer node and returns a DRPC connection to the peer node.
func (c *client) drpcDial(ctx context.Context, rpcCtx *rpc.Context) (drpc.Conn, error) {
	var conn *rpc.DRPCConnection
	if c.peerID != 0 {
		conn = rpcCtx.DRPCDialNode(c.addr.String(), c.peerID, c.locality, rpcbase.SystemClass)
	} else {
		// TODO(baptist): Use this as a temporary connection for getting
		// onto gossip and then replace with a validated connection.
		log.Dev.Infof(ctx, "unvalidated bootstrap gossip dial to %s", c.addr)
		conn = rpcCtx.DRPCUnvalidatedDial(c.addr.String(), c.locality)
	}

	return conn.Connect(ctx)
}

// dialGossipClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// RPCGossipClient.
func (c *client) dialGossipClient(
	ctx context.Context, rpcCtx *rpc.Context,
) (RPCGossipClient, error) {
	if !rpcbase.DRPCEnabled(ctx, rpcCtx.Settings) {
		conn, err := c.dial(ctx, rpcCtx)
		if err != nil {
			return nil, err
		}
		return NewGRPCGossipClientAdapter(conn), nil
	}
	conn, err := c.drpcDial(ctx, rpcCtx)
	if err != nil {
		return nil, err
	}
	return NewDRPCGossipClientAdapter(conn), nil
}
