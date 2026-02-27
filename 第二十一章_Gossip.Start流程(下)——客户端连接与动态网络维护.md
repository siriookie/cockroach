# 第二十一章 Gossip.Start 流程（下）——客户端连接与动态网络维护

## 三、关键函数深入分析（DFS Part 2）

### 3.5 `client.startLocked()` — 启动客户端连接

**位置**：[client.go:78-145](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\client.go#L78-L145)

#### 核心职责

1. **建立 RPC 连接**：通过 gRPC 或 DRPC 连接到对端节点
2. **启动 Gossip 流**：持续交换 gossip delta
3. **处理连接生命周期**：连接成功、失败、断开的处理

#### Placeholder 机制

**问题**：启动客户端时，我们可能还不知道对端的 `NodeID`（首次连接时）。但 `outgoing` nodeSet 需要跟踪所有出向连接的 NodeID。

**解决方案**：使用 **Placeholder** 机制
```go
// Step 1: 添加 placeholder（占位符）
g.outgoing.addPlaceholder()  // 在 startClientLocked 调用前

// Step 2: 连接成功后，解析 placeholder
if !c.resolvedPlaceholder && c.peerID != 0 {
    c.resolvedPlaceholder = true
    g.outgoing.resolvePlaceholder(c.peerID)
}
```

**`nodeSet` 的 Placeholder 实现**（简化）：
```go
type nodeSet struct {
    nodes       map[roachpb.NodeID]struct{}
    maxSize     int
    placeholders int  // 占位符计数
}

func (ns *nodeSet) addPlaceholder() {
    ns.placeholders++
}

func (ns *nodeSet) resolvePlaceholder(nodeID roachpb.NodeID) {
    ns.placeholders--
    ns.nodes[nodeID] = struct{}{}
}

func (ns *nodeSet) len() int {
    return len(ns.nodes) + ns.placeholders
}
```

**为什么需要 Placeholder？**
- 避免并发问题：如果先连接、后计数，可能导致超出 `maxSize`
- 精确控制连接数：确保 `outgoing.len() <= maxSize`

#### 完整执行流程

```go
func (c *client) startLocked(
    g *Gossip, disconnected chan *client, rpcCtx *rpc.Context, stopper *stop.Stopper,
) {
    // 1️⃣ 添加 placeholder（由调用方在持有锁时完成）
    // g.outgoing.addPlaceholder()  // 实际在 g.startClientLocked 中

    ctx, cancel := context.WithCancel(c.AnnotateCtx(context.Background()))

    if err := stopper.RunAsyncTask(ctx, "gossip-client", func(ctx context.Context) {
        var wg sync.WaitGroup
        defer func() {
            cancel()       // 关闭出向流
            wg.Wait()      // 等待入向 gossip 处理完成
            disconnected <- c  // 通知 management goroutine
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

        log.Dev.Infof(ctx, "started gossip client to n%d (%s)", c.peerID, c.addr)

        // 4️⃣ 启动 gossip 循环
        if err := c.gossip(ctx, g, stream, stopper, &wg); err != nil {
            if !grpcutil.IsClosedConnection(err) {
                // 记录非正常关闭的错误
                log.Dev.Infof(ctx, "closing client to n%d (%s): %s", c.peerID, c.addr, err)
            }
        }
    }); err != nil {
        disconnected <- c
    }
}
```

**错误处理**：
- **拨号失败**：立即返回，通过 `disconnected` channel 通知 management goroutine
- **Gossip 失败**：记录错误（除非是正常的连接关闭），然后通过 `disconnected` 通知

---

### 3.6 `client.requestGossip()` — 发送初始请求

**位置**：[client.go:159-178](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\client.go#L159-L178)

#### 职责

发送初始的 `Request`，告诉服务端：
1. **我是谁**：`NodeID` 和 `Addr`
2. **我知道什么**：`HighWaterStamps`（各个 Node 的最新时间戳）
3. **我属于哪个集群**：`ClusterID`

#### 实现

```go
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

    // 4️⃣ 发送请求
    return stream.Send(args)
}
```

**HighWaterStamps 的作用**：
- 告诉服务端"我已经知道的信息"
- 服务端只发送 `OrigStamp > HighWaterStamps[NodeID]` 的 info
- **增量同步**：避免重复传输相同信息

**为什么是 map 而非单个时间戳？**
- Gossip 网络是**去中心化**的，每个 Node 独立生成信息
- 需要跟踪**每个 Node** 的最新时间戳，而非全局单一时间戳

---

### 3.7 `client.gossip()` — 核心 Gossip 循环

**位置**：[client.go:305-398](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\client.go#L305-L398)

#### 双向通信模型

Gossip 协议是**全双工**的：
- **接收方向**：持续接收对端的 gossip delta
- **发送方向**：本地信息变更时发送 delta

#### 架构设计

```go
func (c *client) gossip(
    ctx context.Context,
    g *Gossip,
    stream RPCGossip_GossipClient,
    stopper *stop.Stopper,
    wg *sync.WaitGroup,
) error {
    sendGossipChan := make(chan struct{}, 1)

    // 1️⃣ 注册回调：本地信息变更时触发发送
    updateCallback := func(_ string, _ roachpb.Value, _ int64) {
        select {
        case sendGossipChan <- struct{}{}:  // 非阻塞发送
        default:  // 如果 channel 已满，跳过（已有待处理的发送任务）
        }
    }

    errCh := make(chan error, 1)
    initCh := make(chan struct{}, 1)

    // 2️⃣ 启动接收 goroutine
    wg.Add(1)
    if err := stopper.RunAsyncTask(ctx, "client-gossip", func(ctx context.Context) {
        defer wg.Done()

        errCh <- func() error {
            for init := true; ; init = false {
                reply, err := stream.Recv()
                if err != nil {
                    return err
                }
                if err := c.handleResponse(ctx, g, reply); err != nil {
                    return err
                }
                if init {
                    initCh <- struct{}{}  // 首次响应后通知主循环
                }
            }
        }()
    }); err != nil {
        wg.Done()
        return err
    }

    // 3️⃣ 延迟注册回调（等待首次响应或 1 秒超时）
    var unregister func()
    defer func() {
        if unregister != nil {
            unregister()
        }
    }()
    maybeRegister := func() {
        if unregister == nil {
            unregister = g.RegisterCallback(".*", updateCallback, Redundant)
        }
    }
    initTimer := time.NewTimer(time.Second)
    defer initTimer.Stop()

    // 4️⃣ 主循环：处理发送和控制信号
    for count := 0; ; {
        select {
        case <-c.closer:
            return nil  // 客户端被关闭
        case <-stopper.ShouldQuiesce():
            return nil  // 节点正在关闭
        case err := <-errCh:
            return err  // 接收 goroutine 出错
        case <-initCh:
            maybeRegister()  // 收到首次响应，注册回调
        case <-initTimer.C:
            maybeRegister()  // 超时 1 秒，强制注册回调
        case <-sendGossipChan:
            // 5️⃣ 批量发送 gossip delta
            batchAndConsume(sendGossipChan, infosBatchDelay)
            if err := c.sendGossip(g, stream, count == 0); err != nil {
                return err
            }
            count++
        }
    }
}
```

#### 关键设计决策

**1. 为什么延迟注册回调？**

**问题**：如果在连接建立后立即注册回调，本地的所有 info 都会触发发送，导致**全量同步**。

**解决方案**：
- 等待首次响应（包含对端的 `HighWaterStamps`）
- 或者超时 1 秒（防止旧版本节点不发送初始响应）
- 之后才注册回调，实现**增量同步**

**兼容性考虑**：
```go
// 版本 < 2.1 的节点可能不发送初始响应
// 所以设置 1 秒超时，强制注册回调
initTimer := time.NewTimer(time.Second)
```

**2. 为什么用 buffered channel（容量为 1）？**

```go
sendGossipChan := make(chan struct{}, 1)

updateCallback := func(...) {
    select {
    case sendGossipChan <- struct{}{}:
    default:  // 已有待处理任务，跳过
    }
}
```

- **批量发送**：多个信息变更只触发一次发送
- **背压控制**：如果发送速度跟不上，跳过部分触发（下次发送会包含所有未发送的 delta）

**3. `batchAndConsume()` 的作用**

**位置**：推测在 `pkg/gossip/util.go`（未在当前代码中）

**目的**：
- 消费 channel 中的所有待处理信号
- 等待 `infosBatchDelay`（10ms）以批量处理

**伪代码**：
```go
func batchAndConsume(ch chan struct{}, delay time.Duration) {
    // 消费所有已有信号
    for {
        select {
        case <-ch:
        default:
            goto Wait
        }
    }
Wait:
    // 等待一小段时间，批量处理
    time.Sleep(delay)
}
```

---

### 3.8 `client.handleResponse()` — 处理服务端响应

**位置**：[client.go:235-300](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\client.go#L235-L300)

#### 核心职责

1. **合并 Gossip Delta**：将对端的信息合并到本地 InfoStore
2. **解析 Placeholder**：首次收到 `reply.NodeID` 时解析 placeholder
3. **处理转发**：如果对端满载，接受转发到其他节点
4. **检测重复连接**：避免同一对节点之间的多条连接

#### 实现细节

```go
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
        g.maybeTightenLocked()  // 可能触发 tighten 网络
    }

    // 3️⃣ 记录对端 NodeID 和高水位
    c.peerID = reply.NodeID
    mergeHighWaterStamps(&c.remoteHighWaterStamps, reply.HighWaterStamps)

    // 4️⃣ 解析 placeholder
    if !c.resolvedPlaceholder && c.peerID != 0 {
        c.resolvedPlaceholder = true
        g.outgoing.resolvePlaceholder(c.peerID)
    }

    // 5️⃣ 处理转发
    if reply.AlternateAddr != nil {
        // 检查是否已有到 AlternateNodeID 的连接
        if g.hasIncomingLocked(reply.AlternateNodeID) || g.hasOutgoingLocked(reply.AlternateNodeID) {
            return errors.Errorf(
                "received forward from n%d to n%d (%s); already have active connection, skipping",
                reply.NodeID, reply.AlternateNodeID, reply.AlternateAddr)
        }
        c.forwardAddr = reply.AlternateAddr
        return errors.Errorf("received forward from n%d to n%d (%s)",
            reply.NodeID, reply.AlternateNodeID, reply.AlternateAddr)
    }

    // 6️⃣ 检查连接状态
    g.signalConnectedLocked()

    // 7️⃣ 检测重复连接
    if nodeID := g.NodeID.Get(); nodeID == c.peerID {
        return errors.Errorf("stopping outgoing client to n%d (%s); loopback connection", c.peerID, c.addr)
    } else if g.hasIncomingLocked(c.peerID) && nodeID > c.peerID {
        // 双向连接冲突解决：NodeID 较大的一方关闭出向连接
        return errors.Errorf("stopping outgoing client to n%d (%s); already have incoming", c.peerID, c.addr)
    }

    return nil
}
```

#### 重复连接的解决策略

**场景**：节点 A 和节点 B 同时向对方发起出向连接，导致双向连接。

**问题**：
- 浪费资源（两条连接传输相同信息）
- 违反 `maxPeers` 限制

**解决方案**：
```go
if g.hasIncomingLocked(c.peerID) && nodeID > c.peerID {
    return errors.Errorf("stopping outgoing client to n%d; already have incoming", c.peerID)
}
```

- **规则**：NodeID 较大的一方关闭出向连接
- **为什么比较 NodeID？**：
  - 确定性：双方执行相同的规则，只有一方会关闭
  - 避免振荡：不会出现双方都关闭或都不关闭的情况

**示例**：
```
Node 1 连接到 Node 2（出向）
Node 2 连接到 Node 1（出向）

结果：
- Node 1 收到来自 Node 2 的入向连接，检查 1 < 2，保持出向连接
- Node 2 收到来自 Node 1 的入向连接，检查 2 > 1，关闭出向连接
- 最终：Node 1 → Node 2（出向），Node 2 ← Node 1（入向）
```

---

### 3.9 `server.gossip()` — 服务端处理入向连接

**位置**：[server.go:110-214](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\server.go#L110-L214)

服务端的 gossip 循环与客户端类似，但有几个关键区别：

#### 关键区别

| 方面 | 客户端 | 服务端 |
|------|-------|-------|
| **连接发起** | 主动拨号 | 被动接受 |
| **初始请求** | 客户端发送 | 服务端等待接收 |
| **连接管理** | 可能被转发 | 可以拒绝或转发客户端 |
| **并发模型** | 一个 stream = 一个 goroutine | 两个 goroutine（发送 + 接收） |

#### 双 Goroutine 架构

```go
func (s *server) gossip(stream RPCGossip_GossipStream) error {
    // 1️⃣ 接收初始请求
    args, err := stream.Recv()
    if err != nil {
        return err
    }

    // 2️⃣ 校验集群 ID
    if (args.ClusterID != uuid.UUID{}) && args.ClusterID != s.clusterID.Get() {
        return errors.Errorf("gossip connection refused from different cluster %s", args.ClusterID)
    }

    ctx, cancel := context.WithCancel(s.AnnotateCtx(stream.Context()))
    defer cancel()
    syncChan := make(chan struct{}, 1)  // 保证 send 串行化

    // 3️⃣ 封装 send 函数（加 metrics 和并发控制）
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

    defer func() { syncChan <- struct{}{} }()  // 确保 send 完成

    errCh := make(chan error, 1)

    // 4️⃣ 启动接收 goroutine
    if err := s.stopper.RunAsyncTask(ctx, "gossip receiver", func(ctx context.Context) {
        errCh <- s.gossipReceiver(ctx, &args, &lastSentHighWaterStamps, send, stream.Recv)
    }); err != nil {
        return err
    }

    reply := new(Response)

    // 5️⃣ 发送 goroutine 主循环
    for init := true; ; init = false {
        ready := s.ready.Load().(chan struct{})
        s.mu.Lock()
        delta := s.mu.is.delta(args.HighWaterStamps)
        if init {
            s.mu.is.populateMostDistantMarkers(delta)
        }

        if infoCount := len(delta); init || infoCount > 0 {
            // 更新客户端的高水位
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

        // 6️⃣ 等待事件
        select {
        case <-s.stopper.ShouldQuiesce():
            return nil
        case err := <-errCh:
            return err
        case <-ready:
            time.Sleep(infosBatchDelay)  // 批量发送
        }
    }
}
```

**为什么用两个 goroutine？**
- **发送 goroutine**：等待 `ready` channel，发送增量 delta
- **接收 goroutine**：持续接收客户端的 gossip delta

**为什么需要 `syncChan`？**
- gRPC/DRPC 的 `stream.Send()` 不是线程安全的
- `syncChan` 充当互斥锁，确保同时只有一个 goroutine 调用 `Send()`

---

## 四、动态行为分析（Runtime Behavior）

### 4.1 网络 Tighten 机制

#### 触发条件

```go
func (s *server) maybeTightenLocked() {
    now := timeutil.Now()
    if now.Before(s.mu.lastTighten.Add(gossipTightenInterval)) {
        return  // 距离上次 tighten 不到 1 秒，跳过
    }
    select {
    case s.tighten <- struct{}{}:
    default:  // channel 已满，跳过
    }
}
```

**何时调用 `maybeTightenLocked()`？**
1. **服务端收到 gossip delta**（`server.gossipReceiver`）
2. **客户端收到 gossip delta**（`client.handleResponse`）

**为什么频繁检查？**
- 每次收到新信息时，可能发现某些节点的跳数过高
- 及时 tighten 可以加速信息传播

#### Tighten 决策逻辑

**位置**：[gossip.go:1428-1457](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L1428-L1457)

```go
func (g *Gossip) tightenNetwork(ctx context.Context, rpcContext *rpc.Context) {
    g.mu.Lock()
    defer g.mu.Unlock()

    // 1️⃣ 防抖：距离上次 tighten 不到 1 秒，跳过
    now := timeutil.Now()
    if now.Before(g.mu.lastTighten.Add(gossipTightenInterval)) {
        return
    }
    g.mu.lastTighten = now

    // 2️⃣ 检查是否有空间添加新连接
    if g.outgoing.hasSpace() {
        // 3️⃣ 找到最远的节点（且没有出向连接）
        distantNodeID, distantHops := g.mu.is.mostDistant(g.hasOutgoingLocked)
        log.VEventf(ctx, 2, "distantHops: %d from %d", distantHops, distantNodeID)

        if distantHops <= maxHops {
            return  // 网络已经足够紧密
        }

        // 4️⃣ 如果需要 tighten，重置 lastTighten（允许持续 tighten）
        g.mu.lastTighten = time.Time{}

        // 5️⃣ 连接到最远的节点
        if nodeAddr, locality, err := g.getNodeIDAddress(distantNodeID, true); err == nil {
            log.Health.Infof(ctx, "starting client to n%d (%d > %d) to tighten network graph",
                distantNodeID, distantHops, maxHops)
            g.startClientLocked(*nodeAddr, locality, rpcContext)
        }
    }
}
```

**`mostDistant()` 的实现逻辑**（简化）：
```go
func (is *infoStore) mostDistant(hasOutgoing func(roachpb.NodeID) bool) (roachpb.NodeID, uint32) {
    var maxHops uint32
    var maxNodeID roachpb.NodeID

    for nodeID, info := range is.Infos {
        if hasOutgoing(nodeID) {
            continue  // 已有出向连接，跳过
        }
        if info.Hops > maxHops {
            maxHops = info.Hops
            maxNodeID = nodeID
        }
    }
    return maxNodeID, maxHops
}
```

**跳数（Hops）如何计算？**
- 每个 info 包含 `Hops` 字段
- 转发时 `Hops++`
- 例如：Node A → Node B → Node C，则 Node C 收到的 info 的 Hops = 2

---

### 4.2 连接淘汰（Cull）机制

#### 触发时机

每隔 `cullInterval`（默认 60s）触发一次。

#### 选择策略

**位置**：[gossip.go:1376-1405](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L1376-L1405)

```go
case <-cullTimer.C:
    cullTimer.Reset(jitteredInterval(g.cullInterval, rng))
    func() {
        g.mu.Lock()
        if !g.outgoing.hasSpace() {
            // 找到"最不有用"的客户端
            leastUsefulID := g.mu.is.leastUseful(g.outgoing)

            if c := g.findClient(func(c *client) bool {
                return c.peerID == leastUsefulID
            }); c != nil {
                log.Health.Infof(ctx, "closing gossip client n%d %s", c.peerID, c.addr)
                c.close()

                // 释放锁后，阻塞等待客户端断开
                defer func() {
                    g.doDisconnected(<-g.disconnected, rpcContext)
                }()
            }
        }
        g.mu.Unlock()
    }()
```

**`leastUseful()` 的判断标准**（推测）：
1. **信息新鲜度**：最近提供的 info 是否过时
2. **跳数**：提供的 info 跳数是否过高
3. **连接时间**：连接时间最长的可能优先级较低（假设已充分利用）

**为什么需要 Cull？**
- **动态优化**：集群拓扑可能变化，原本有用的连接可能变得低效
- **为 Tighten 让路**：释放空间给更优的连接

---

### 4.3 故障恢复与 Bootstrap 重试

#### 检测 Stall 的逻辑

**位置**：[gossip.go:1476-1513](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L1476-L1513)

```go
func (g *Gossip) maybeSignalStatusChangeLocked() {
    orphaned := g.outgoing.len()+g.mu.incoming.len() == 0
    multiNode := len(g.bootstrapInfo.Addresses) > 0

    // Stalled 的两种情况：
    // 1. 多节点集群但没有任何连接
    // 2. 缺少 sentinel key
    stalled := (orphaned && multiNode) || g.mu.is.getInfo(KeySentinel) == nil

    if stalled {
        if !g.stalled {
            log.Eventf(ctx, "now stalled")
            // 记录不同的 stall 原因
            if orphaned {
                if len(g.addresses) == 0 {
                    log.Ops.Warningf(ctx, "no addresses found; use --join to specify a connected node")
                } else {
                    log.Health.Warningf(ctx, "no incoming or outgoing connections")
                }
            } else if len(g.addressesTried) == len(g.addresses) {
                log.Health.Warningf(ctx, "first range unavailable; addresses exhausted")
            } else {
                log.Health.Warningf(ctx, "first range unavailable; trying remaining addresses")
            }
        }
        if len(g.addresses) > 0 {
            g.signalStalledLocked()  // 唤醒 bootstrap goroutine
        }
    } else {
        if g.stalled {
            log.Ops.Infof(ctx, "node has connected to cluster via gossip")
            g.signalConnectedLocked()
        }
        g.maybeCleanupBootstrapAddressesLocked()
    }
    g.stalled = stalled
}
```

#### Bootstrap 重试策略

**问题**：如果所有 bootstrap 地址都失败怎么办？

**解决方案**：
1. **Round-Robin 重试**：持续尝试所有地址
2. **指数退避**：`bootstrapInterval` 可配置（默认 1s）
3. **动态地址发现**：
   - 从持久化存储读取之前连接过的节点地址
   - 从 gossip 中学习到的 `NodeDescriptor` 中提取地址

**持久化 Bootstrap 地址**（[gossip.go:438-491](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L438-L491)）：
```go
func (g *Gossip) SetStorage(storage Storage) error {
    // 1️⃣ 读取持久化的 bootstrap 信息
    var storedBI BootstrapInfo
    if err := storage.ReadBootstrapInfo(&storedBI); err != nil {
        log.Ops.Warningf(ctx, "failed to read gossip bootstrap info: %s", err)
    }

    g.mu.Lock()
    defer g.mu.Unlock()
    g.storage = storage

    // 2️⃣ 合并持久化地址和当前地址
    existing := map[string]struct{}{}
    for _, addr := range g.bootstrapInfo.Addresses {
        existing[makeKey(addr)] = struct{}{}
    }
    for _, addr := range storedBI.Addresses {
        if _, ok := existing[makeKey(addr)]; !ok && addr != g.mu.is.NodeAddr {
            g.maybeAddBootstrapAddressLocked(addr, unknownNodeID)
        }
    }

    // 3️⃣ 持久化合并后的地址
    if numAddrs := len(g.bootstrapInfo.Addresses); numAddrs > len(storedBI.Addresses) {
        if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
            log.Dev.Errorf(ctx, "%v", err)
        }
    }

    // 4️⃣ 如果发现新地址，立即触发 bootstrap
    if newAddressFound {
        g.signalStalledLocked()
    }
    return nil
}
```

**从 Gossip 学习地址**（[gossip.go:761-822](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L761-L822)）：
```go
func (g *Gossip) updateNodeAddress(key string, content roachpb.Value, _ int64) {
    var desc roachpb.NodeDescriptor
    if err := content.GetProto(&desc); err != nil {
        return
    }

    g.mu.Lock()
    defer g.mu.Unlock()

    // 1️⃣ 更新 nodeDescs 缓存
    g.nodeDescs.Store(desc.NodeID, &desc)
    g.recomputeMaxPeersLocked()

    // 2️⃣ 跳过自己的地址
    if desc.Address == g.mu.is.NodeAddr {
        return
    }

    // 3️⃣ 添加到 addresses 和 bootstrapInfo
    g.maybeAddAddressLocked(desc.Address)
    added := g.maybeAddBootstrapAddressLocked(desc.Address, desc.NodeID)

    // 4️⃣ 持久化新地址
    if added && g.storage != nil {
        if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
            log.Dev.Errorf(ctx, "%v", err)
        }
    }
}
```

---

### 4.4 连接数动态调整

#### `maxPeers()` 计算

**位置**：[gossip.go:731-752](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L731-L752)

```go
func maxPeers(nodeCount int) int {
    // 公式：maxPeers = ceil(e^(log(nodeCount) / (maxHops-2)))
    //
    // 推导（简化）：
    // - 假设每个节点连接 P 个 peer
    // - 信息在 H 跳内覆盖 P^H 个节点
    // - 要求 P^H >= N（总节点数）
    // - 则 P >= N^(1/H)
    //
    // 使用 maxHops-2（而非 maxHops）是为了留出"余量"
    maxPeers := int(math.Ceil(math.Exp(math.Log(float64(nodeCount)) / float64(maxHops-2))))
    if maxPeers < minPeers {
        return minPeers
    }
    return maxPeers
}
```

**示例**：
| 节点数 | maxHops-2 | 计算公式 | maxPeers |
|-------|----------|---------|---------|
| 27 | 3 | 27^(1/3) = 3 | 3 |
| 64 | 3 | 64^(1/3) = 4 | 4 |
| 125 | 3 | 125^(1/3) = 5 | 5 |
| 1000 | 3 | 1000^(1/3) = 10 | 10 |

**动态调整**（[gossip.go:852-861](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L852-L861)）：
```go
func (g *Gossip) recomputeMaxPeersLocked() {
    var n int
    g.nodeDescs.Range(func(_ roachpb.NodeID, _ *roachpb.NodeDescriptor) bool {
        n++
        return true
    })
    maxPeers := maxPeers(n)
    g.mu.incoming.setMaxSize(maxPeers)
    g.outgoing.setMaxSize(maxPeers)
}
```

**触发时机**：
- 每次收到 `NodeDescriptor` 更新时（通过 `updateNodeAddress` 回调）
- 这确保了 `maxPeers` 随集群规模动态调整

---

## 五、具体示例（Concrete Examples）

### 5.1 场景 1：首次启动节点

#### 初始条件

- **Node A**：新启动的节点
- **Bootstrap 地址**：`--join=nodeB:26257,nodeC:26257`
- **集群状态**：Node B 和 Node C 已经在集群中

#### 时间线

| 时间 | 事件 | 状态变化 |
|-----|------|---------|
| T0 | `Gossip.Start()` 被调用 | `started = true` |
| T0+1ms | `server.start()` 完成 | 开始接受入向连接 |
| T0+2ms | `bootstrap` goroutine 启动 | 检查 `outgoing.len() == 0` 且 `sentinel == nil` |
| T0+3ms | 尝试连接 Node B | `g.startClientLocked(nodeB, ...)` |
| T0+100ms | 连接 Node B 成功 | `outgoing.len() = 1`，但仍缺 sentinel |
| T0+110ms | 收到 Node B 的 gossip delta | 包含 `KeySentinel`，`sentinel != nil` |
| T0+111ms | `maybeSignalStatusChangeLocked()` | `stalled = false`，关闭 `Connected` channel |
| T0+112ms | `signalConnectedLocked()` | 其他组件（如 DistSender）可以开始工作 |

#### 详细日志示例

```
T0      [gossip] starting gossip server
T0+2ms  [gossip-bootstrap] now stalled: no incoming or outgoing connections
T0+3ms  [gossip-bootstrap] starting new client to nodeB:26257
T0+100ms [gossip-client] started gossip client to n2 (nodeB:26257)
T0+110ms [gossip-client] received [KeySentinel, KeyNodeDesc-2, KeyFirstRangeDesc] from n2 (3 fresh)
T0+111ms [gossip] connected: node has connected to cluster via gossip
T0+112ms [gossip] cleaning up bootstrap addresses
```

---

### 5.2 场景 2：网络 Tighten

#### 初始拓扑

```
集群：5 个节点（Node 1-5）
当前连接：
  Node 1 → Node 2
  Node 2 → Node 3
  Node 3 → Node 4
  Node 4 → Node 5
```

#### 问题

Node 1 到 Node 5 的信息需要 **4 跳**，超过 `maxHops = 5`（虽然没超，但接近阈值）。

#### Tighten 过程

| 时间 | 事件 | 跳数变化 |
|-----|------|---------|
| T0 | Node 1 收到来自 Node 5 的 info（Hops = 4） | `distantHops = 4` |
| T0+1ms | `maybeTightenLocked()` 被调用 | 检查 `4 <= 5`，暂不 tighten |
| T1 | 集群扩展到 10 个节点 | Node 1 到 Node 10 的 Hops = 6 |
| T1+1ms | Node 1 收到来自 Node 10 的 info（Hops = 6） | `distantHops = 6` |
| T1+2ms | `tightenNetwork()` 被触发 | `6 > 5`，需要 tighten |
| T1+3ms | Node 1 直接连接 Node 10 | `Node 1 → Node 10` |
| T1+100ms | Node 1 收到来自 Node 10 的 info（Hops = 1） | `distantHops` 降低 |

#### Tighten 后的拓扑

```
新连接：
  Node 1 → Node 2
  Node 1 → Node 10  ← 新增的直连
  Node 2 → Node 3
  ...
```

**效果**：
- Node 1 到 Node 10 的 Hops 从 6 降低到 1
- 整个集群的信息传播速度加快

---

### 5.3 场景 3：连接淘汰与替换

#### 初始状态

- **Node A**：已有 3 个出向连接（达到 `maxPeers = 3`）
  - Client 1 → Node B（连接时间：5 分钟，最近提供的 info 时间戳：T-60s）
  - Client 2 → Node C（连接时间：2 分钟，最近提供的 info 时间戳：T-10s）
  - Client 3 → Node D（连接时间：3 分钟，最近提供的 info 时间戳：T-5s）

#### Cull 决策

| 时间 | 事件 | 决策 |
|-----|------|------|
| T0 | `cullTimer` 触发（60s 间隔） | 调用 `leastUseful()` |
| T0+1ms | `leastUseful()` 计算 | Client 1 最不有用（信息最旧） |
| T0+2ms | 关闭 Client 1 | `c.close()`，等待断开 |
| T0+50ms | Client 1 断开 | `g.doDisconnected(Client 1)` |
| T0+51ms | `maybeSignalStatusChangeLocked()` | `outgoing.len() = 2 < maxPeers` |
| T0+52ms | 不触发 stalled（仍有连接且有 sentinel） | 不唤醒 bootstrap |

#### 新连接建立

| 时间 | 事件 | 说明 |
|-----|------|------|
| T1 | Node A 收到 Node E 的 info（Hops = 6） | `distantHops = 6 > 5` |
| T1+1ms | `tightenNetwork()` 被触发 | 检查 `outgoing.hasSpace() = true` |
| T1+2ms | 连接到 Node E | `Client 4 → Node E` |

**最终状态**：
- Client 2 → Node C
- Client 3 → Node D
- Client 4 → Node E（替换了 Client 1）

---

## 六、设计权衡与取舍（Trade-offs）

### 6.1 去中心化 vs 强一致性

| 方案 | Gossip 协议 | 中心化协调者（如 ZooKeeper） |
|------|-----------|---------------------------|
| **一致性** | 最终一致性 | 强一致性（Linearizable） |
| **可用性** | 高（AP in CAP） | 中等（CP in CAP） |
| **故障容忍** | 单节点失败不影响整体 | 需要 majority quorum |
| **延迟** | O(log N) 轮收敛 | O(1)（读 leader） |
| **扩展性** | 优秀（每节点 O(log N) 连接） | 受限于协调者性能 |

**CockroachDB 的选择**：
- Gossip 用于**非关键路径**的信息传播（如 NodeDescriptor、StoreDescriptor）
- Raft 用于**关键路径**的强一致性（如 Range 元数据、事务记录）

### 6.2 Lazy Tighten vs 主动维护

| 方案 | Lazy Tighten（CockroachDB） | 主动维护（定时 Tighten） |
|------|---------------------------|----------------------|
| **触发时机** | 收到高跳数 info 时 | 定时检查 |
| **CPU 开销** | 低（仅在需要时） | 高（持续检查） |
| **响应速度** | 快（立即 tighten） | 慢（等待定时器） |
| **复杂度** | 中等（需要防抖） | 简单 |

**CockroachDB 的设计**：
- **Lazy 触发** + **防抖**（`gossipTightenInterval = 1s`）
- 平衡了响应速度和 CPU 开销

### 6.3 固定连接数 vs 动态调整

| 方案 | 固定连接数 | 动态调整（CockroachDB） |
|------|---------|---------------------|
| **适应性** | 差（集群扩缩容时不优） | 优（自动调整） |
| **复杂度** | 简单 | 中等（需要 `recomputeMaxPeers`） |
| **性能** | 小集群浪费、大集群不足 | 平衡 |

**`maxPeers()` 公式**：
```
maxPeers = ceil(nodeCount^(1/(maxHops-2)))
```

- **小集群**（<27 节点）：3 个连接足够
- **大集群**（1000 节点）：~10 个连接

### 6.4 全量同步 vs 增量同步

| 方案 | 全量同步 | 增量同步（HighWaterStamps） |
|------|---------|-------------------------|
| **带宽** | O(N) per sync | O(changed) per sync |
| **复杂度** | 简单 | 中等（需跟踪高水位） |
| **适用场景** | 小集群 | 大集群 |

**CockroachDB 的实现**：
- 每个连接跟踪对端的 `HighWaterStamps`
- 只发送 `OrigStamp > HighWaterStamps[NodeID]` 的 info

### 6.5 Cull 策略：最不有用 vs 最老连接

| 策略 | 最不有用（CockroachDB） | 最老连接 |
|------|----------------------|---------|
| **目标** | 优化信息传播效率 | 公平性 |
| **复杂度** | 高（需计算"有用性"） | 低（按时间排序） |
| **效果** | 更优的网络拓扑 | 可能保留低效连接 |

**`leastUseful()` 的判断标准**（推测）：
1. **信息新鲜度**：最近提供的 info 是否过时
2. **跳数**：提供的 info 跳数是否过高
3. **重复度**：是否与其他连接高度重叠

---

## 七、总结与心智模型

### 7.1 核心思想

**Gossip 协议是一个自组织、去中心化的信息传播网络，通过动态调整连接拓扑，在 O(log N) 轮内将信息传播到全集群。**

### 7.2 心智模型

**"如果只记住一件事，那就是……"**

> **Gossip 就像社交网络中的谣言传播**：
> - 每个人（节点）与少数几个朋友（peers）保持联系
> - 听到新消息时，立即告诉所有朋友
> - 如果发现某个朋友总是提供过时信息，就换一个朋友
> - 如果发现某个远方的人（高跳数），直接加为好友（tighten）
> - 最终，所有消息会在几轮传播后到达所有人

### 7.3 简化伪代码

```python
class Gossip:
    def start(self, addresses):
        # 启动三大循环
        spawn_goroutine(self.accept_incoming)  # 接受入向连接
        spawn_goroutine(self.bootstrap, addresses)  # 主动连接
        spawn_goroutine(self.manage)  # 维护连接

    def bootstrap(self, addresses):
        while not stopped:
            if not self.has_clients or not self.has_sentinel:
                addr = self.next_bootstrap_address(addresses)
                self.start_client(addr)
            sleep(bootstrap_interval)
            wait_for_stall_signal()

    def manage(self):
        while not stopped:
            event = select(
                disconnected_channel,
                tighten_channel,
                cull_timer,
                stall_timer
            )
            if event == disconnected:
                self.remove_client(client)
                if client.forward_addr:
                    self.start_client(client.forward_addr)
            elif event == tighten:
                if self.outgoing.has_space():
                    distant_node = self.most_distant_node()
                    if distant_node.hops > MAX_HOPS:
                        self.start_client(distant_node)
            elif event == cull:
                if not self.outgoing.has_space():
                    least_useful = self.find_least_useful_client()
                    least_useful.close()
            elif event == stall:
                self.check_stall_status()

    def gossip_client(self, stream):
        # 双向循环
        spawn_goroutine(lambda: self.recv_loop(stream))
        while not stopped:
            wait_for_local_update()
            delta = self.compute_delta(remote_high_water_stamps)
            stream.send(delta)

    def gossip_server(self, stream):
        # 双 goroutine
        spawn_goroutine(lambda: self.recv_loop(stream))
        while not stopped:
            wait_for_info_change()
            delta = self.compute_delta(client_high_water_stamps)
            stream.send(delta)
```

### 7.4 关键不变量总结

1. **连接数约束**：
   - `outgoing.len() + outgoing.placeholders <= maxPeers(nodeCount)`
   - `incoming.len() <= maxPeers(nodeCount)`

2. **连接状态**：
   - 一旦 `Connected` channel 被关闭，就不再重新打开
   - `stalled = true` ⟺ `(没有连接 && 多节点) || 缺少 sentinel`

3. **信息传播**：
   - 每个 info 至多在 `maxHops` 跳内传播到所有节点
   - 通过 tighten 机制确保 `maxHops` 不被超越

4. **并发安全**：
   - `g.mu` 保护 InfoStore 和核心状态
   - `g.clientsMu` 保护 clients 列表
   - `stream.Send()` 通过 `syncChan` 串行化

### 7.5 最终建议

**在使用或修改 Gossip 代码时**：
1. **理解三大循环的职责边界**：不要让它们越权
2. **尊重锁的层次**：`g.mu` > `g.clientsMu`，避免死锁
3. **信任增量同步**：不要尝试"优化"HighWaterStamps 机制
4. **谨慎调整定时器**：`stallInterval`、`cullInterval` 等已经过生产验证
5. **观察 metrics**：`gossip.connections.{incoming,outgoing}`、`gossip.bytes.{sent,received}` 等

---

## 附录：关键常量和配置

| 常量 | 值 | 说明 |
|------|---|------|
| `maxHops` | 5 | 最大跳数阈值 |
| `minPeers` | 3 | 最小连接数 |
| `defaultStallInterval` | 1s | 检查 stall 的间隔 |
| `defaultBootstrapInterval` | 1s | Bootstrap 重试间隔 |
| `defaultCullInterval` | 60s | 淘汰连接的间隔 |
| `gossipTightenInterval` | 1s | Tighten 防抖间隔 |
| `infosBatchDelay` | 10ms | Batch 发送延迟 |
| `NodeDescriptorInterval` | 1h | NodeDescriptor 重新 gossip 间隔 |
| `NodeDescriptorTTL` | 2h | NodeDescriptor TTL |
| `StoresInterval` | 10s | StoreDescriptor 重新 gossip 间隔 |
| `StoreTTL` | 20s | StoreDescriptor TTL |

---

**全文完**
