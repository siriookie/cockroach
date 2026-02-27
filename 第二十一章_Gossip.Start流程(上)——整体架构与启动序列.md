# 第二十一章 Gossip.Start 流程（上）——整体架构与启动序列

## 一、第一轮 BFS：整体职责与设计动机（Why）

### 1.1 背景与要解决的问题

在分布式数据库系统中，集群中的所有节点需要共享一些全局状态信息，例如：
- **节点拓扑**：哪些节点存活、它们的地址、位置信息
- **Range 元数据**：第一个 Range 的 descriptor（用于 KV 层路由）
- **系统配置**：集群级别的配置信息
- **Store 状态**：各个 Store 的负载、容量等

这些信息需要在集群中**快速、可靠地传播**，但又不能依赖于中心化的协调者（避免单点故障）。Gossip 协议正是为此而生。

**核心问题**：
1. **如何在无中心的前提下发现和连接其他节点？**
2. **如何高效传播信息，同时避免信息风暴？**
3. **如何应对节点故障、网络分区等异常情况？**

### 1.2 Gossip 在 CockroachDB 中的位置

```
┌─────────────────────────────────────────┐
│          SQL Layer (pkg/sql/)           │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│      KV Transaction Layer (pkg/kv/)     │
│  ├─ DistSender (需要 Range 路由信息)    │
│  └─ NodeLiveness (节点存活状态)         │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│         Gossip (pkg/gossip/)            │ ◄─── 我们在这里
│  ├─ 分布式信息传播                      │
│  ├─ 节点发现与连接管理                  │
│  └─ KeyFirstRangeDescriptor 等关键信息  │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│      RPC Layer (pkg/rpc/)               │
│      Gossip Protocol (gRPC/DRPC)        │
└─────────────────────────────────────────┘
```

**协作关系**：
- **上游依赖方**：
  - `DistSender`：通过 Gossip 获取 `KeyFirstRangeDescriptor`，用于 KV 请求路由
  - `NodeLiveness`：通过 Gossip 传播节点存活信息
  - `StorePool`：通过 Gossip 获取 Store 状态，用于 Rebalancing 决策

- **下游依赖**：
  - `rpc.Context`：提供 RPC 连接能力（gRPC/DRPC）
  - `stop.Stopper`：用于优雅关闭所有 goroutine

### 1.3 核心对象与数据结构

#### 1.3.1 `Gossip` 结构体（[gossip.go:252](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L252)）

```go
type Gossip struct {
    started bool                    // 标记是否已启动（用于断言）
    *server                         // 嵌入的 Gossip RPC 服务器

    // 连接状态
    Connected     chan struct{}     // 首次连接成功时关闭此 channel
    hasConnected  bool              // 是否已连接过
    outgoing      nodeSet           // 出向连接的节点集合

    // Bootstrap 相关
    storage       Storage           // 持久化存储接口
    bootstrapInfo BootstrapInfo     // 持久化的 bootstrap 地址
    bootstrapping map[string]struct{} // 当前正在尝试的 bootstrap 地址

    // Client 管理
    clientsMu struct {
        syncutil.Mutex
        clients []*client           // 所有出向 gossip 客户端
    }
    disconnected chan *client       // 断开连接的客户端通知 channel

    // 状态管理
    stalled       bool              // 是否处于 stalled 状态（无连接或缺 sentinel）
    stalledCh     chan struct{}     // 用于唤醒 bootstrap goroutine

    // 定时器配置
    stallInterval     time.Duration // 检查 stall 的间隔（默认 1s）
    bootstrapInterval time.Duration // Bootstrap 重试间隔（默认 1s）
    cullInterval      time.Duration // 淘汰低效连接的间隔（默认 60s）

    // 地址管理
    addresses      []util.UnresolvedAddr       // bootstrap 地址列表
    addressIdx     int                         // 当前尝试的地址索引
    addressesTried map[int]struct{}            // 已尝试过的地址索引
    bootstrapAddrs map[util.UnresolvedAddr]roachpb.NodeID // bootstrap 地址 -> NodeID 映射

    // 节点描述符缓存
    nodeDescs  syncutil.Map[roachpb.NodeID, roachpb.NodeDescriptor]
    storeDescs syncutil.Map[roachpb.StoreID, roachpb.StoreDescriptor]

    locality roachpb.Locality      // 本节点的 locality 信息
}
```

**关键不变量**：
- `outgoing.len() + mu.incoming.len() > 0`（一旦连接成功）：必须保持至少一个连接
- `mu.is.getInfo(KeySentinel) != nil`（一旦连接成功）：必须能获取到 sentinel key（表示连接到了正常的集群）
- `outgoing.len() <= maxPeers(nodeCount)`：出向连接数受限于集群规模

#### 1.3.2 `server` 结构体（[server.go:35](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\server.go#L35)）

```go
type server struct {
    log.AmbientContext
    clusterID *base.ClusterIDContainer
    NodeID    *base.NodeIDContainer
    stopper   *stop.Stopper

    mu struct {
        syncutil.RWMutex
        is       *infoStore                         // InfoStore：存储所有 gossip 信息
        incoming nodeSet                            // 入向连接的节点集合
        nodeMap  map[util.UnresolvedAddr]serverInfo // 入向客户端地址 -> 信息
        lastTighten time.Time                       // 最后一次 tighten 网络的时间
    }

    ready   atomic.Value        // 类型为 chan struct{}，用于唤醒等待的 gossip 请求
    tighten chan struct{}       // 用于触发网络 tighten 操作

    nodeMetrics   Metrics       // 节点级别的 metrics
    serverMetrics Metrics       // 服务器级别的 metrics
}
```

**职责**：
- **接收入向 gossip 连接**：处理来自其他节点的 gossip 请求
- **管理 InfoStore**：存储和合并所有 gossip 信息
- **连接管理**：决定是否接受新连接或转发到其他节点

#### 1.3.3 `client` 结构体（[client.go:28](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\client.go#L28)）

```go
type client struct {
    log.AmbientContext

    createdAt time.Time
    peerID    roachpb.NodeID               // 对端节点 ID（收到响应后设置）
    resolvedPlaceholder bool               // 是否已将 placeholder 解析为实际 NodeID
    addr      net.Addr                     // 对端地址
    locality  roachpb.Locality             // 对端 locality（如果已知）
    forwardAddr *util.UnresolvedAddr       // 如果被转发，记录转发地址

    prevHighWaterStamps   map[roachpb.NodeID]int64 // 上次发送给对端的高水位
    remoteHighWaterStamps map[roachpb.NodeID]int64 // 对端的高水位

    closer        chan struct{}            // 用于关闭客户端的 channel
    clientMetrics Metrics                  // 客户端 metrics
    nodeMetrics   Metrics                  // 引用 node 级别的 metrics
}
```

**职责**：
- **维护出向 RPC 连接**：持续与对端节点交换 gossip delta
- **跟踪高水位**：避免重复发送已知信息
- **处理转发**：如果对端满载，处理转发到其他节点的逻辑


### 1.4 为什么需要 Gossip？

**对比其他方案**：

| 方案 | 优点 | 缺点 | 为何不选 |
|------|------|------|---------|
| **中心化协调者（如 ZooKeeper）** | 强一致性、简单 | 单点故障、性能瓶颈 | CockroachDB 追求去中心化 |
| **广播（Broadcast）** | 简单、可靠 | O(N²) 消息量、网络风暴 | 大规模集群不可行 |
| **DHT（如 Chord）** | 结构化、可预测 | 查找延迟高、维护成本 | 不适合频繁变化的信息 |
| **Gossip 协议** | 去中心化、可扩展、自愈 | 最终一致性 | **CockroachDB 的选择** |

**Gossip 的核心优势**：
1. **O(log N) 跳数收敛**：信息在 O(log N) 轮内传播到全集群
2. **自组织网络**：无需手动配置拓扑，节点自动发现和连接
3. **故障容忍**：单个节点失败不影响整体传播
4. **负载均衡**：每个节点只维护少量连接（~3-5 个）


---

## 二、第二轮 BFS：控制流与交互关系（How it flows）

### 2.1 `Gossip.Start()` 的顶层流程

`Gossip.Start()` 是 Gossip 生命周期的**唯一入口**，在 `pkg/server/server.go` 中的 `Server.PreStart()` 阶段被调用。

#### 源码位置：[gossip.go:1219-1228](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L1219-L1228)

```go
func (g *Gossip) Start(
    advertAddr net.Addr, addresses []util.UnresolvedAddr, rpcContext *rpc.Context,
) {
    g.AssertNotStarted(context.Background())  // 1️⃣ 断言未启动
    g.started = true                          // 2️⃣ 标记已启动
    g.setAddresses(addresses)                 // 3️⃣ 设置 bootstrap 地址
    g.server.start(advertAddr)                // 4️⃣ 启动 Gossip 服务器
    g.bootstrap(rpcContext)                   // 5️⃣ 启动 bootstrap 循环
    g.manage(rpcContext)                      // 6️⃣ 启动连接管理循环
}
```

**三大核心 goroutine**：
1. **Server Goroutine（`g.server.start`）**：接受入向 gossip 连接
2. **Bootstrap Goroutine（`g.bootstrap`）**：主动连接 bootstrap 节点
3. **Management Goroutine（`g.manage`）**：管理出向连接、淘汰低效连接、处理断开连接

---

### 2.2 三大循环的触发时机与交互

#### 2.2.1 **Server Loop：被动接受连接**

**时机**：其他节点主动连接本节点时
**位置**：[server.go:385-418](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\server.go#L385-L418)

```go
func (s *server) start(addr net.Addr) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.mu.is.NodeAddr = util.MakeUnresolvedAddr(addr.Network(), addr.String())

    broadcast := func() {
        // 关闭旧的 ready channel 并创建新的，广播给所有等待者
        close(s.ready.Swap(make(chan struct{})).(chan struct{}))
    }

    // 注册回调：任何 gossip 信息变更时广播通知
    unregister := s.mu.is.registerCallback(".*", func(_ string, _ roachpb.Value, _ int64) {
        broadcast()
    }, Redundant)

    // 在 stopper 关闭时取消注册
    waitQuiesce := func(context.Context) {
        <-s.stopper.ShouldQuiesce()
        // ... 取消注册并广播
    }
    _ = s.stopper.RunAsyncTask(bgCtx, "gossip-wait-quiesce", waitQuiesce)
}
```

**关键点**：
- **`ready` channel 的巧妙设计**：使用 `atomic.Value` 存储一个 `chan struct{}`，每次信息变更时**关闭旧 channel 并替换为新的**。这样所有等待在旧 channel 上的 goroutine 都会被唤醒。
- **为什么不用 `sync.Cond`？**：`Cond` 不可组合（不能放在 `select` 中），而 channel 可以。
- **Redundant 回调**：即使信息内容未变（只是更新了 TTL），也需要广播，因为过期信息的传播同样重要。

**处理入向连接的逻辑**（[server.go:216-367](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\server.go#L216-L367)）：
```go
// 简化版逻辑
if args.NodeID == 0 {
    // 初始连接，允许通过以便对方获取 node ID
} else if !nodeIdentified {
    nodeIdentified = true
    if args.NodeID == s.NodeID.Get() {
        // 回环连接，忽略
    } else if _, ok := s.mu.nodeMap[args.Addr]; ok {
        // 重复连接（可能通过 load balancer），拒绝
        return errors.Errorf("duplicate connection")
    } else if s.mu.incoming.hasSpace() {
        // 有空间，接受连接
        s.mu.incoming.addNode(args.NodeID)
        defer func() {
            s.mu.incoming.removeNode(args.NodeID)
            delete(s.mu.nodeMap, args.Addr)
        }()
    } else {
        // 满载，转发到随机的已连接节点
        *reply = Response{
            NodeID:          s.NodeID.Get(),
            AlternateAddr:   &alternateAddr,
            AlternateNodeID: alternateNodeID,
        }
        // 客户端会断开并尝试连接 AlternateAddr
    }
}
```

---

#### 2.2.2 **Bootstrap Loop：主动连接 Bootstrap 节点**

**时机**：
1. **启动时立即触发**（如果没有连接或缺少 sentinel）
2. **连接断开后被唤醒**（通过 `g.stalledCh`）
3. **定期检查**（`bootstrapInterval`，默认 1s）

**位置**：[gossip.go:1283-1346](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L1283-L1346)

```go
func (g *Gossip) bootstrap(rpcContext *rpc.Context) {
    ctx := g.AnnotateCtx(context.Background())
    _ = g.server.stopper.RunAsyncTask(ctx, "gossip-bootstrap", func(ctx context.Context) {
        var bootstrapTimer timeutil.Timer
        defer bootstrapTimer.Stop()

        for {
            // 1️⃣ 检查是否需要 bootstrap
            func(ctx context.Context) {
                g.mu.Lock()
                defer g.mu.Unlock()

                haveClients := g.outgoing.len() > 0
                haveSentinel := g.mu.is.getInfo(KeySentinel) != nil

                if !haveClients || !haveSentinel {
                    // 需要 bootstrap
                    if addr := g.getNextBootstrapAddressLocked(); !addr.IsEmpty() {
                        locality := roachpb.Locality{}
                        nodeID := g.bootstrapAddrs[addr]
                        if nd, err := g.getNodeDescriptor(nodeID, true); err == nil {
                            locality = nd.Locality
                        }
                        g.startClientLocked(addr, locality, rpcContext)
                    } else {
                        // 没有可用地址，标记为 stalled
                        g.maybeSignalStatusChangeLocked()
                    }
                }
            }(ctx)

            // 2️⃣ 暂停一段时间后继续
            bootstrapTimer.Reset(g.bootstrapInterval)
            select {
            case <-bootstrapTimer.C:
                // 定期检查
            case <-g.server.stopper.ShouldQuiesce():
                return
            }

            // 3️⃣ 阻塞直到需要再次 bootstrap
            select {
            case <-g.stalledCh:
                // 被唤醒，继续 bootstrap
            case <-g.server.stopper.ShouldQuiesce():
                return
            }
        }
    })
}
```

**关键决策点**：
- **何时 bootstrap？**
  - 条件 1：`g.outgoing.len() == 0`（没有出向连接）
  - 条件 2：`g.mu.is.getInfo(KeySentinel) == nil`（缺少 sentinel，表示未连接到集群）

- **Sentinel Key 的作用**：
  - Sentinel 是一个由集群的"第一个节点"定期 gossip 的特殊 key
  - 如果一个节点能收到 sentinel，说明它连接到了正常的集群
  - 如果收不到，说明可能处于网络分区或所有连接都是无效的

**地址选择策略**（[gossip.go:1261-1275](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L1261-L1275)）：
```go
func (g *Gossip) getNextBootstrapAddressLocked() util.UnresolvedAddr {
    // Round-robin 遍历所有地址
    for range g.addresses {
        g.addressIdx++
        g.addressIdx %= len(g.addresses)
        g.addressesTried[g.addressIdx] = struct{}{}
        addr := g.addresses[g.addressIdx]

        // 跳过正在尝试的地址
        if _, addrActive := g.bootstrapping[addr.String()]; !addrActive {
            g.bootstrapping[addr.String()] = struct{}{}
            return addr
        }
    }
    return util.UnresolvedAddr{} // 所有地址都在尝试中
}
```

---

#### 2.2.3 **Management Loop：连接维护与优化**

**时机**：
1. **客户端断开连接时**（`g.disconnected` channel）
2. **收到 tighten 信号时**（`g.tighten` channel）
3. **定期淘汰低效连接**（`cullTimer`，默认 60s）
4. **定期检查 stall 状态**（`stallTimer`，默认 1s）

**位置**：[gossip.go:1358-1416](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L1358-L1416)

```go
func (g *Gossip) manage(rpcContext *rpc.Context) {
    ctx := g.AnnotateCtx(context.Background())
    _ = g.server.stopper.RunAsyncTask(ctx, "gossip-manage", func(ctx context.Context) {
        var cullTimer, stallTimer timeutil.Timer
        defer cullTimer.Stop()
        defer stallTimer.Stop()
        rng, _ := randutil.NewPseudoRand()

        cullTimer.Reset(jitteredInterval(g.cullInterval, rng))
        stallTimer.Reset(jitteredInterval(g.stallInterval, rng))

        for {
            select {
            case <-g.server.stopper.ShouldQuiesce():
                return

            case c := <-g.disconnected:
                g.doDisconnected(c, rpcContext)

            case <-g.tighten:
                g.tightenNetwork(ctx, rpcContext)

            case <-cullTimer.C:
                cullTimer.Reset(jitteredInterval(g.cullInterval, rng))
                func() {
                    g.mu.Lock()
                    if !g.outgoing.hasSpace() {
                        leastUsefulID := g.mu.is.leastUseful(g.outgoing)
                        if c := g.findClient(func(c *client) bool {
                            return c.peerID == leastUsefulID
                        }); c != nil {
                            log.Health.Infof(ctx, "closing gossip client n%d %s", c.peerID, c.addr)
                            c.close()
                            defer func() {
                                g.doDisconnected(<-g.disconnected, rpcContext)
                            }()
                        }
                    }
                    g.mu.Unlock()
                }()

            case <-stallTimer.C:
                stallTimer.Reset(jitteredInterval(g.stallInterval, rng))
                func() {
                    g.mu.Lock()
                    defer g.mu.Unlock()
                    g.maybeSignalStatusChangeLocked()
                }()
            }
        }
    })
}
```

**四种触发场景**：

1. **客户端断开（`g.disconnected`）**：
   ```go
   func (g *Gossip) doDisconnected(c *client, rpcContext *rpc.Context) {
       g.mu.Lock()
       defer g.mu.Unlock()
       g.removeClientLocked(c)  // 从 outgoing 中移除

       // 如果断开时带了转发地址，立即连接
       if c.forwardAddr != nil {
           locality := roachpb.Locality{}
           if nd, err := g.getNodeDescriptor(c.peerID, true); err == nil {
               locality = nd.Locality
           }
           g.startClientLocked(*c.forwardAddr, locality, rpcContext)
       }
       g.maybeSignalStatusChangeLocked()  // 可能需要重新 bootstrap
   }
   ```

2. **Tighten 网络（`g.tighten`）**：
   - **触发时机**：收到新的 gossip 信息后，发现某些节点的跳数 > `maxHops`（5）
   - **目的**：直接连接到"远距离"节点，缩短信息传播路径
   ```go
   func (g *Gossip) tightenNetwork(ctx context.Context, rpcContext *rpc.Context) {
       g.mu.Lock()
       defer g.mu.Unlock()

       // 防抖：距离上次 tighten 不到 1s，跳过
       if now.Before(g.mu.lastTighten.Add(gossipTightenInterval)) {
           return
       }
       g.mu.lastTighten = now

       if g.outgoing.hasSpace() {
           distantNodeID, distantHops := g.mu.is.mostDistant(g.hasOutgoingLocked)
           if distantHops <= maxHops {
               return  // 网络已经足够紧密
           }

           // 连接到最远的节点
           if nodeAddr, locality, err := g.getNodeIDAddress(distantNodeID, true); err == nil {
               log.Health.Infof(ctx, "starting client to n%d (%d > %d) to tighten network graph",
                   distantNodeID, distantHops, maxHops)
               g.startClientLocked(*nodeAddr, locality, rpcContext)
           }
       }
   }
   ```

3. **淘汰低效连接（`cullTimer`）**：
   - **触发时机**：每隔 `cullInterval`（默认 60s）
   - **目的**：释放出空间，为更优的连接让路
   - **选择策略**：关闭"最不有用"的客户端（`leastUseful`，基于信息新鲜度和跳数计算）

4. **检查 Stall 状态（`stallTimer`）**：
   - **触发时机**：每隔 `stallInterval`（默认 1s）
   - **目的**：检测是否失去连接或 sentinel，及时触发 bootstrap


---

### 2.3 状态转换图

```
                     启动
                      │
                      ▼
           ┌──────────────────┐
           │  Not Connected   │ ◄───────────┐
           │ (stalled=true)   │             │
           └──────────────────┘             │
                      │                     │
                      │ bootstrap           │ 失去连接/sentinel
                      │ 成功连接            │
                      ▼                     │
           ┌──────────────────┐             │
           │    Connected     │ ────────────┘
           │ (stalled=false)  │
           │  - has clients   │
           │  - has sentinel  │
           └──────────────────┘
                      │
                      │ 持续运行
                      ▼
           ┌──────────────────────────────┐
           │  Maintaining Connections     │
           │  - tighten network           │
           │  - cull inefficient clients  │
           │  - handle disconnections     │
           └──────────────────────────────┘
```

**状态判断逻辑**（[gossip.go:1476-1513](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L1476-L1513)）：
```go
func (g *Gossip) maybeSignalStatusChangeLocked() {
    orphaned := g.outgoing.len()+g.mu.incoming.len() == 0
    multiNode := len(g.bootstrapInfo.Addresses) > 0

    // 判断是否 stalled
    stalled := (orphaned && multiNode) || g.mu.is.getInfo(KeySentinel) == nil

    if stalled {
        if !g.stalled {
            log.Eventf(ctx, "now stalled")
            if orphaned {
                if len(g.addresses) == 0 {
                    log.Ops.Warningf(ctx, "no addresses found; use --join to specify a connected node")
                } else {
                    log.Health.Warningf(ctx, "no incoming or outgoing connections")
                }
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

---

### 2.4 信息传播的触发路径

**Gossip Delta 的生成与发送**有两个方向：

#### 2.4.1 **服务端主动推送（Server → Client）**

```
新 Gossip Info 写入
    │
    ├─► infoStore.addInfo()
    │       │
    │       └─► 触发 callbacks (registerCallback)
    │               │
    │               └─► broadcast()  // 关闭并替换 ready channel
    │                       │
    │                       └─► 所有等待在 ready 上的 server goroutine 被唤醒
    │                               │
    │                               └─► server.gossip() 发送 delta 给客户端
```

#### 2.4.2 **客户端主动推送（Client → Server）**

```
本地 Gossip Info 更新
    │
    ├─► infoStore.addInfo()
    │       │
    │       └─► 触发 callbacks (g.RegisterCallback)
    │               │
    │               └─► updateCallback()  // client.gossip 中注册
    │                       │
    │                       └─► sendGossipChan <- struct{}{}
    │                               │
    │                               └─► client.sendGossip() 发送 delta 给服务器
```

**高水位机制（High Water Stamps）**：
- **目的**：避免重复发送相同信息
- **原理**：
  - 每个 Node 维护一个 `OrigStamp`（原始时间戳）
  - 每个连接跟踪对端的 `HighWaterStamps`（每个 Node 最后一次看到的时间戳）
  - 只发送 `OrigStamp > HighWaterStamps[NodeID]` 的 info

**示例**：
```
Node A 的 InfoStore:
  - KeyNodeDesc-1: {NodeID: 1, OrigStamp: 100}
  - KeyNodeDesc-2: {NodeID: 2, OrigStamp: 200}

Node B 上次告诉 A 它的 HighWaterStamps:
  - Node 1: 50
  - Node 2: 200

则 A 只需发送:
  - KeyNodeDesc-1: {NodeID: 1, OrigStamp: 100}  // 100 > 50
  # 跳过 KeyNodeDesc-2，因为 200 == 200
```

---

### 2.5 关键数据流图

```
┌─────────────────────────────────────────────────────────────┐
│                     Gossip Instance                          │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   Server     │    │  Bootstrap   │    │  Management  │  │
│  │   Goroutine  │    │  Goroutine   │    │  Goroutine   │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                    │                    │          │
│         │ 入向连接           │ 出向连接           │ 管理     │
│         ▼                    ▼                    ▼          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │             InfoStore (mu.is)                        │  │
│  │  - Infos: map[string]*Info                           │  │
│  │  - HighWaterStamps: map[NodeID]int64                 │  │
│  │  - Callbacks: []callback                             │  │
│  └──────────────────────────────────────────────────────┘  │
│         │                    │                    │          │
│         ├────────────────────┼────────────────────┤          │
│         ▼                    ▼                    ▼          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  incoming    │    │   outgoing   │    │  clients[]   │  │
│  │   nodeSet    │    │   nodeSet    │    │   []*client  │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
         ▲                                            │
         │ RPC Gossip Protocol                        │
         │ (gRPC/DRPC)                                │
         └────────────────────────────────────────────┘
                    Network (其他节点)
```

---

## 三、关键函数深入分析（DFS Part 1）

### 3.1 `Gossip.Start()` — 启动入口

**位置**：[gossip.go:1219-1228](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L1219-L1228)

#### 输入参数

| 参数 | 类型 | 说明 |
|-----|------|------|
| `advertAddr` | `net.Addr` | 本节点的广告地址（其他节点用此地址连接本节点） |
| `addresses` | `[]util.UnresolvedAddr` | Bootstrap 地址列表（`--join` 参数） |
| `rpcContext` | `*rpc.Context` | RPC 上下文（提供 gRPC/DRPC 连接能力） |

#### 不变量

**前置条件**：
- `g.started == false`（通过 `AssertNotStarted` 检查）
- `g.server != nil`（在 `New()` 时已初始化）
- `rpcContext != nil`（必须提供有效的 RPC 上下文）

**后置条件**：
- `g.started == true`
- 三个核心 goroutine 已启动并运行
- 如果 `addresses` 非空，bootstrap 循环会开始尝试连接

#### 执行步骤拆解

```go
func (g *Gossip) Start(
    advertAddr net.Addr, addresses []util.UnresolvedAddr, rpcContext *rpc.Context,
) {
    // Step 1: 断言未启动（防御性编程）
    g.AssertNotStarted(context.Background())

    // Step 2: 标记已启动
    g.started = true

    // Step 3: 设置 bootstrap 地址
    g.setAddresses(addresses)
    //   - 将 addresses 存入 g.addresses
    //   - 初始化 g.addressIdx = len(addresses) - 1
    //   - 清空 g.addressesTried
    //   - 发送信号到 g.stalledCh（如果有地址）

    // Step 4: 启动 Gossip 服务器
    g.server.start(advertAddr)
    //   - 设置 g.mu.is.NodeAddr = advertAddr
    //   - 注册全局回调：任何 info 变更 → broadcast()
    //   - 启动 "gossip-wait-quiesce" goroutine

    // Step 5: 启动 Bootstrap 循环
    g.bootstrap(rpcContext)
    //   - 启动 "gossip-bootstrap" goroutine
    //   - 持续检查是否需要 bootstrap
    //   - 通过 g.stalledCh 被唤醒

    // Step 6: 启动 Management 循环
    g.manage(rpcContext)
    //   - 启动 "gossip-manage" goroutine
    //   - 处理断开连接、tighten、cull、stall 检查
}
```

---

### 3.2 `server.start()` — 启动 Gossip 服务器

**位置**：[server.go:385-418](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\server.go#L385-L418)

#### 核心职责

1. **设置 NodeAddr**：将本节点的广告地址存入 InfoStore
2. **注册广播回调**：任何 Gossip 信息变更时唤醒所有等待的连接
3. **启动 Quiesce 监听**：在 Stopper 关闭时清理资源

#### `ready` channel 的原子替换机制

**问题**：如何在多个 goroutine 中高效地"广播"事件？

**传统方案的问题**：
- **`sync.Cond`**：
  - 优点：专为广播设计
  - 缺点：不可组合（不能用于 `select`），必须持有 mutex 才能 Wait

- **单个 `chan struct{}`**：
  - 优点：可组合，可用于 `select`
  - 缺点：关闭后无法重用，需要手动替换

**CockroachDB 的方案**：
```go
// 原子替换 ready channel
broadcast := func() {
    oldReady := s.ready.Load().(chan struct{})
    newReady := make(chan struct{})
    s.ready.Store(newReady)
    close(oldReady)  // 唤醒所有等待在 oldReady 上的 goroutine
}
```

**为什么有效？**
1. **等待者的视角**：
   ```go
   ready := s.ready.Load().(chan struct{})  // 获取当前 ready
   select {
   case <-ready:  // 如果 ready 被关闭，立即触发
       // 处理事件
   case <-otherChan:
       // 其他逻辑
   }
   ```
   - 即使在 `Load()` 和 `select` 之间 `ready` 被替换，也不会丢失事件
   - 因为旧的 `ready` 已经被关闭，`select` 会立即返回

2. **广播者的视角**：
   - 替换 + 关闭是原子的（在持有锁的情况下）
   - 所有等待在旧 `ready` 上的 goroutine 都会被唤醒
   - 新来的等待者会使用新的 `ready`

**回调注册**（[server.go:399-401](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\server.go#L399-L401)）：
```go
unregister := s.mu.is.registerCallback(".*", func(_ string, _ roachpb.Value, _ int64) {
    broadcast()
}, Redundant)
```

- **模式 `".*"`**：匹配所有 key
- **Redundant 选项**：即使 value 未变（仅 TTL 变化），也触发回调
- **为什么需要 Redundant？**：
  - 过期信息的传播同样重要（例如节点下线）
  - 客户端需要知道信息的 TTL 变化

---

### 3.3 `Gossip.bootstrap()` — Bootstrap 循环

**位置**：[gossip.go:1283-1346](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L1283-L1346)

#### 核心逻辑

**双层循环结构**：
```go
for {
    // 外层循环：持续运行直到 stopper 关闭

    // 1️⃣ 尝试 bootstrap（如果需要）
    if !haveClients || !haveSentinel {
        if addr := g.getNextBootstrapAddressLocked(); !addr.IsEmpty() {
            g.startClientLocked(addr, locality, rpcContext)
        }
    }

    // 2️⃣ 等待 bootstrapInterval
    bootstrapTimer.Reset(g.bootstrapInterval)
    select {
    case <-bootstrapTimer.C:
    case <-g.server.stopper.ShouldQuiesce():
        return
    }

    // 3️⃣ 阻塞直到需要再次 bootstrap
    select {
    case <-g.stalledCh:
        // 被 maybeSignalStatusChangeLocked 唤醒
    case <-g.server.stopper.ShouldQuiesce():
        return
    }
}
```

**为什么需要双层循环？**
1. **外层循环**：确保 bootstrap 持续运行
2. **内层等待**：
   - 第一次等待 `bootstrapInterval`：防止连续失败导致忙等
   - 第二次等待 `stalledCh`：节能设计，只在必要时唤醒

**地址选择策略**（Round-Robin）：
```go
func (g *Gossip) getNextBootstrapAddressLocked() util.UnresolvedAddr {
    for range g.addresses {
        g.addressIdx = (g.addressIdx + 1) % len(g.addresses)
        g.addressesTried[g.addressIdx] = struct{}{}
        addr := g.addresses[g.addressIdx]

        if _, addrActive := g.bootstrapping[addr.String()]; !addrActive {
            g.bootstrapping[addr.String()] = struct{}{}
            return addr
        }
    }
    return util.UnresolvedAddr{}  // 所有地址都在尝试中
}
```

**为什么用 Round-Robin 而非随机？**
- **公平性**：确保所有地址都有机会被尝试
- **可预测性**：便于调试和复现问题
- **避免饥饿**：不会因为随机数问题导致某些地址永远不被尝试

---

### 3.4 `Gossip.startClientLocked()` — 启动出向客户端

**位置**：[gossip.go:1540-1553](c:\Users\able2207008\公司\project\cockroach\pkg\gossip\gossip.go#L1540-L1553)

#### 职责

创建一个新的 `client` 实例，连接到指定地址，并启动 gossip 循环。

#### 实现细节

```go
func (g *Gossip) startClientLocked(
    addr util.UnresolvedAddr, locality roachpb.Locality, rpcContext *rpc.Context,
) {
    g.clientsMu.Lock()
    defer g.clientsMu.Unlock()

    ctx := g.AnnotateCtx(context.TODO())
    log.VEventf(ctx, 1, "starting new client to %s", addr)

    // 1️⃣ 创建新客户端
    c := newClient(g.server.AmbientContext, &addr, locality, g.serverMetrics)

    // 2️⃣ 加入客户端列表
    g.clientsMu.clients = append(g.clientsMu.clients, c)

    // 3️⃣ 启动客户端（异步）
    c.startLocked(g, g.disconnected, rpcContext, g.server.stopper)
}
```

**为什么需要 `clientsMu` 锁？**
- `g.mu` 保护 Gossip 的核心状态（InfoStore、nodeSet 等）
- `g.clientsMu` 只保护 `clients` 列表
- **分离锁粒度**：避免在 RPC 操作时持有 `g.mu`（会降低并发性）

---

这里我们完成了第一部分的分析，涵盖了：
1. **Why**：Gossip 存在的原因、在系统中的位置
2. **How it flows**：三大循环的触发时机与交互关系
3. **DFS Part 1**：关键函数的前半部分（启动流程）

第二部分将继续分析：
- `client.startLocked()` 和 `client.gossip()` 的完整流程
- 动态行为分析（tighten、cull、状态转换）
- 具体示例（连接建立、信息传播、故障恢复）
- 设计权衡与心智模型
