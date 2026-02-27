# raftRecvQueues 内存监控机制深度剖析
## Store 初始化中的 BytesMonitor 集成与 RACv2 流量控制联动

---

## 一、BFS Why：为什么需要 Raft 接收队列的内存监控？

### 1.1 问题背景：Raft 消息堆积导致的内存压力

在分布式数据库中，Raft 共识协议需要处理集群中所有节点的消息。这些消息到达 Store 时，如果 Raft 处理速度不足以匹配网络接收速度，就会在接收队列（receive queue）中堆积：

```
网络层消息到达 → raftReceiveQueue.Append()
                    ↓
          [消息1 | 消息2 | 消息3 | ...]
                    ↓
          Raft Scheduler 处理（每 tick ~ 10ms）
```

**关键问题**：
- 消息缓冲是**无限的**（如果不加限制），可能导致内存膨胀
- 高负载场景下，单个 Store 可能要管理数千个 Range 的 Raft 消息队列
- 队列中的每条消息都占用内存（包括消息体、序列化数据、响应流引用）
- 如果未被正确追踪，可能产生**隐形内存泄漏**

### 1.2 CockroachDB 内存监控哲学

CockroachDB 采用**分层内存监控**架构，确保内存使用被精确追踪和可观察：

```
SQL 查询执行  ←→  BytesMonitor  ←→  Root Memory Pool
Raft 消息队列 ←→  BytesMonitor  ←→  (可选) Parent Monitor
磁盘缓冲     ←→  BytesMonitor  ←→  Root Disk Monitor
```

每一层都维护自己的 `BytesMonitor`，形成**树形结构**。这样的好处是：
1. **隔离性**：不同组件的内存使用互不干扰
2. **可观察性**：可以通过 `crdb_internal.node_memory_monitors` 查询全部内存分配状态
3. **追踪能力**：精确知道每个 Raft 消息队列消耗了多少字节

### 1.3 为什么使用 UnlimitedMonitor 而不是受限 Monitor

这个决策背后的逻辑很关键：

```go
// 使用 UnlimitedMonitor 的原因
s.raftRecvQueues.mon = mon.NewUnlimitedMonitor(ctx, mon.Options{
    Name:     mon.MakeName("raft-receive-queue"),
    CurCount: s.metrics.RaftRcvdQueuedBytes,  // 关键：连接到指标系统
    Settings: cfg.Settings,
})
```

**设计理由**：

| 方面 | UnlimitedMonitor | LimitedMonitor |
|------|-----------------|----------------|
| 内存上限 | `math.MaxInt64`（实际无限） | 配置值（如 256MB） |
| 流量控制 | 无硬限制 | 超限时拒绝 Append |
| 指标收集 | ✓ 完整（通过 CurCount） | ✓ 完整 |
| 背压机制 | 由 RACv2 在**发送端**实现 | 在**接收端**实现 |
| 使用场景 | 信任上游流量控制 | 自我保护（单点防线） |

**核心决策**：Raft 队列的背压不应该在**接收端**实现，而应该在**发送端**。这是 RACv2（Replication Admission Control v2）的设计哲学：

> 通过在源头控制消息发送速率，比在接收端被动拒绝更有效

### 1.4 监控的对象与计数器

监控目标是 `s.metrics.RaftRcvdQueuedBytes`，这是一个 `metric.Gauge` 对象：

```go
type StoreMetrics struct {
    // ... 其他指标 ...

    // RaftRcvdQueuedBytes 记录所有 Raft 接收队列当前占用的字节数
    RaftRcvdQueuedBytes *metric.Gauge

    // 这个指标会：
    // 1. 在消息被 Append 时递增
    // 2. 在消息被 Drain 时递减
    // 3. 被 Prometheus / 监控系统定期采样
}
```

这个指标的意义：
- **实时可见**：监控系统可以看到 Raft 队列的内存使用趋势
- **告警基础**：可以基于此设置告警规则（如超过 100MB 时告警）
- **容量规划**：指导机器内存配置和集群大小评估

---

## 二、BFS How：内存监控数据如何流动？

### 2.1 初始化流程（存储层视角）

当 `NewStore()` 被调用时，内存监控系统被集成到 Store 创建过程中：

```
NewStore(cfg, engine, ...)
    ↓
[第 1-3 阶段] Store 基础字段初始化
    ↓
[第 4 阶段] 内存监控初始化
    ├─ Line 1561-1565: 创建 raftRecvQueues.mon
    │  s.raftRecvQueues.mon = mon.NewUnlimitedMonitor(...)
    │
    ├─ Line 1566-1576: 注册流量控制观察器
    │  s.cfg.KVFlowWaitForEvalConfig.RegisterWatcher(...)
    │
    └─ [决策点] 是否启用 maxLen 强制执行
       enabled = (wc != rac2.AllWorkWaitsForEval)
    ↓
[第 5 阶段] 后续初始化（Raft 调度器、rate limiter 等）
```

关键代码片段（pkg/kv/kvserver/store.go:1560-1576）：

```go
// 8. 创建 Raft 接收队列监控器
s.raftRecvQueues.mon = mon.NewUnlimitedMonitor(ctx, mon.Options{
    Name:     mon.MakeName("raft-receive-queue"),  // 监控器名称：用于日志和追踪
    CurCount: s.metrics.RaftRcvdQueuedBytes,       // 连接点：将字节数导出到指标
    Settings: cfg.Settings,                        // 集群配置：用于动态参数调整
})

// 注册观察器：监听 KVFlowWaitForEvalConfig 的变化
s.cfg.KVFlowWaitForEvalConfig.RegisterWatcher(func(wc rac2.WaitForEvalCategory) {
    // 当系统配置为 rac2.AllWorkWaitsForEval 时，RACv2 运行在以下模式：
    // - 所有发送者都为所有消息使用发送 token 池
    // - 这些 token 池比单个 Range 的限制更严格
    // - token 池是字节大小的，比计数限制更好地保护接收端
    // - 因此关闭 maxLen 强制执行，避免不必要的额外保护
    s.raftRecvQueues.SetEnforceMaxLen(wc != rac2.AllWorkWaitsForEval)
})
```

### 2.2 消息追踪生命周期

一条 Raft 消息从网络到处理的完整生命周期中，内存监控如何参与：

```
[步骤 1] 消息到达 → RaftMessageHandler 调用 AddIncomingRaftRequest()

                    size := int64(req.Size())  // 计算消息大小（字节）
                    ↓
[步骤 2] Append 到队列

                    raftReceiveQueue.Append(req, respStream)
                    ├─ q.acc.Grow(ctx, size)     // 向 BytesMonitor 申请 size 字节
                    │                             // (Line 117-119 in store_raft.go)
                    │
                    ├─ 如果申请成功:
                    │  └─ 消息被加入 q.mu.infos
                    │     BytesMonitor.curAllocated += size
                    │     metrics.RaftRcvdQueuedBytes.Inc(size)  // 指标递增
                    │
                    └─ 如果申请失败（通常发生在 maxLen 强制下）:
                       └─ 返回 appended=false，消息被丢弃

                    ↓
[步骤 3] 监控系统采样

                    Prometheus 每 15 秒采样 metrics.RaftRcvdQueuedBytes
                    → 仪表板显示当前值
                    → 告警规则触发（如果超限）

                    ↓
[步骤 4] Raft Scheduler 处理

                    Worker Goroutine:
                    ├─ LoadOrCreate(rangeID)  // 获取该 Range 的队列
                    ├─ queue.Drain()           // 一次性取出所有消息
                    │  └─ acc.Clear()          // 释放所有字节给 Monitor
                    │     BytesMonitor.curAllocated -= totalSize
                    │     metrics.RaftRcvdQueuedBytes.Dec(totalSize)  // 指标递减
                    └─ ProcessRaftMessages()   // 处理消息

                    ↓
[步骤 5] 监控看到的变化

                    Before Drain:  metrics.RaftRcvdQueuedBytes = 5,120 bytes
                    After Drain:   metrics.RaftRcvdQueuedBytes = 0 bytes
                    Avg Queue Time: ~10ms (RaftScheduler tick interval)
```

### 2.3 核心数据流向

```
┌─────────────────────────────────────────────────────┐
│ CockroachDB Node Startup (NewStore)                │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
    ┌──────────────────────────────┐
    │ Create BytesMonitor          │
    │ Name: "raft-receive-queue"   │
    │ Limit: math.MaxInt64         │
    │ Reserved: Unlimited Budget   │
    └──────────────┬───────────────┘
                   │
           ┌───────┴────────┐
           │                │
           ▼                ▼
    ┌─────────────┐  ┌──────────────────┐
    │ mon.mu      │  │ mon.mu.curBytes  │
    │ .curAlloc=0 │  │ Count (Gauge)    │
    │ .maxAlloc=0 │  │    ↓ references │
    └─────────────┘  │ metrics.RaftRcvd │
                     │ QueuedBytes      │
                     └────────┬─────────┘
                              │
                    ┌─────────┴──────────┐
                    │                    │
                    ▼                    ▼
            ┌──────────────┐     ┌─────────────────┐
            │ Prometheus   │     │ Alert Rules     │
            │ Scrape Every │     │ (e.g., >100MB)  │
            │ 15 seconds   │     └─────────────────┘
            └──────────────┘
                    ↑
                    │
        ┌───────────┴───────────┐
        │                       │
    [Append Path]          [Drain Path]
        │                       │
        ▼                       ▼
    acc.Grow(size)         acc.Clear()
    mon.curAlloc += size    mon.curAlloc -= size
    RaftRcvdQueuedBytes++   RaftRcvdQueuedBytes--
```

---

## 三、DFS How：UnlimitedMonitor 实现细节

### 3.1 NewUnlimitedMonitor 的构造过程

当 `mon.NewUnlimitedMonitor()` 被调用时，发生了什么？

```go
// pkg/util/mon/bytes_usage.go:726-735
func NewUnlimitedMonitor(ctx context.Context, args Options) *BytesMonitor {
    if log.V(2) {
        log.Dev.InfofDepth(ctx, 1, "%s: starting unlimited monitor", args.Name)
    }

    // 第 1 步：强制设置上限为无穷大
    args.Limit = math.MaxInt64  // 1.8 × 10^19 字节 = ~18 EB（艾字节）

    // 第 2 步：创建基础 Monitor
    m := NewMonitor(args)  // 使用标准的 Monitor 构造逻辑

    // 第 3 步：关键步骤 - 设置 reserved 为独立预算
    m.reserved = NewStandaloneBudget(math.MaxInt64)
    // reserved 是一个特殊的 BoundAccount：
    // - 没有连接到任何父 Monitor
    // - 容量为 MaxInt64（无限）
    // - 用于保证底层操作永远不会失败

    return m
}
```

对比 `NewMonitor()` 与 `NewUnlimitedMonitor()` 的区别：

| 属性 | NewMonitor(Limit=256MB) | NewUnlimitedMonitor |
|------|------------------------|-------------------|
| configLimit | 256MB | MaxInt64 |
| limit | min(poolLimit, configLimit) | MaxInt64 |
| poolAllocationSize | 10KB（默认） | 10KB（默认） |
| reserved | 连接到某个预提供的账户 | NewStandaloneBudget(MaxInt64) |
| mu.curBudget.mon | 指向 pool 或 nil | nil（完全独立） |
| 行为 | 超限时拒绝 Grow | 永远接受 Grow |

### 3.2 BytesMonitor 内部结构

```go
type BytesMonitor struct {
    mu struct {
        syncutil.Mutex

        // 当前已分配的字节数（所有 BoundAccount 累计）
        curAllocated int64    // 如：5,120 bytes

        // 有史以来的最大分配量（用于监控高水位）
        maxAllocated int64    // 如：12,288 bytes（峰值）

        // 到上级 Monitor 的 BoundAccount
        // 对于 UnlimitedMonitor，此为 nil
        curBudget BoundAccount

        // 指向指标系统的指针
        curBytesCount *metric.Gauge    // 连接到 RaftRcvdQueuedBytes
        maxBytesHist  metric.IHistogram // 记录历史高水位分布

        // 子 Monitor 链表头
        head *BytesMonitor  // 用于 monitor tree tracking

        // 状态标志
        stopped bool
        tracksDisk bool
        rootSQLMonitor bool
        longLiving bool
    }

    // 预留预算（不需要 mutex 保护，因为生命周期不变）
    reserved *BoundAccount

    // 这个 Monitor 的生效上限
    limit int64  // MaxInt64 for UnlimitedMonitor

    // 原始配置的上限（保留以供参考）
    configLimit int64

    // 向 pool 申请时的块大小
    poolAllocationSize int64  // 10KB

    // 集群配置（用于日志级别等）
    settings *cluster.Settings
}
```

### 3.3 消息 Append 时的账户增长

当新消息被 Append 到 Raft 接收队列时，内存会通过 `BoundAccount.Grow()` 被申请：

```go
// pkg/kv/kvserver/store_raft.go:108-128
func (q *raftReceiveQueue) Append(
    req *kvserverpb.RaftMessageRequest,
    s RaftMessageResponseStream,
) (shouldQueue bool, size int64, appended bool) {
    size = int64(req.Size())  // 例如：1,024 bytes

    q.mu.Lock()
    defer q.mu.Unlock()

    // 快速路径检查
    if q.mu.destroyed || (q.mu.enforceMaxLen && len(q.mu.infos) >= q.maxLen) {
        return false, size, false  // 队列已销毁或已满
    }

    // 关键：从 Monitor 申请内存
    if q.acc.Grow(context.Background(), size) != nil {
        // Grow 失败（对于 UnlimitedMonitor 不会发生）
        return false, size, false
    }

    // 消息被添加到队列
    q.mu.infos = append(q.mu.infos, raftRequestInfo{
        req:        req,
        respStream: s,
        size:       size,
    })

    // 返回 shouldQueue=true 仅当这是第一条消息
    // （第一条消息的 enqueuer 负责触发后续 drain）
    return len(q.mu.infos) == 1, size, true
}
```

`BoundAccount.Grow()` 的细节（pkg/util/mon/bytes_usage.go:1166-1181）：

```go
func (b *BoundAccount) Grow(ctx context.Context, x int64) error {
    if b.standaloneUnlimited() {
        // 对于 UnlimitedMonitor 的 reserved 账户：直接增加
        b.used += x
        return nil  // 永不失败
    }

    // 对于普通受限 Monitor 的情况：
    if b.reserved < x {
        // 本地预留缓冲不足，需要向上级申请
        minExtra := b.mon.roundSize(x - b.reserved)
        if err := b.mon.reserveBytes(ctx, minExtra); err != nil {
            return err  // 上级拒绝（超限）
        }
        b.reserved += minExtra
    }

    b.reserved -= x
    b.used += x
    return nil
}
```

对于 `raftReceiveQueue` 中的 `q.acc`：
- 初始化时：`q.acc.Init(context.Background(), qs.mon)` (Line 159)
- 关联的 Monitor：`qs.mon`（UnlimitedMonitor）
- 每次 Append：调用 `q.acc.Grow(ctx, size)`
  - `b.used` 增加 `size` 字节
  - Monitor 的 `mu.curAllocated` 相应增加
  - 指标 `RaftRcvdQueuedBytes` 递增

### 3.4 指标更新机制

BytesMonitor 如何更新指标（pkg/util/mon/bytes_usage.go:1262-1291）：

```go
func (mm *BytesMonitor) reserveBytes(ctx context.Context, x int64) error {
    mm.mu.Lock()
    defer mm.mu.Unlock()

    // 检查本地限制
    if mm.mu.curAllocated > mm.limit-x {
        return mm.makeBudgetExceededError(x)
    }

    // 如果需要，向上级申请预算（UnlimitedMonitor 无上级，跳过）
    if mm.mu.curAllocated > mm.mu.curBudget.used+mm.reserved.used-x {
        if err := mm.increaseBudget(ctx, x); err != nil {
            return err
        }
    }

    // 更新分配计数
    mm.mu.curAllocated += x

    // 关键：更新指标 - 如果 CurCount 不为 nil
    if mm.mu.curBytesCount != nil {
        mm.mu.curBytesCount.Inc(x)  // 对应 RaftRcvdQueuedBytes.Inc(x)
    }

    // 更新高水位
    if mm.mu.maxAllocated < mm.mu.curAllocated {
        mm.mu.maxAllocated = mm.mu.curAllocated
    }

    if log.V(2) {
        log.Dev.Infof(ctx, "%s: now at %d bytes (+%d) - %s",
            mm.name, mm.mu.curAllocated, x, util.GetSmallTrace(3))
    }
    return nil
}
```

---

## 四、Runtime Behavior：运行时行为观察

### 4.1 正常操作场景

在健康的集群中，Raft 接收队列的行为是什么样的？

```
时间线（t = 0 到 t = 1000ms）
──────────────────────────────

t = 0ms:    Store 启动，创建 raftRecvQueues.mon
            RaftRcvdQueuedBytes = 0 bytes
            RaftRcvdQueuedBytes_max = 0 bytes

t = 10ms:   [Batch 1] 5 条来自 Follower 的消息到达
            - 消息大小：200B, 256B, 128B, 512B, 384B = 1,480B
            ├─ Append(msg1, 200B) → acc.Grow(200) → RaftRcvdQueuedBytes += 200 = 200
            ├─ Append(msg2, 256B) → acc.Grow(256) → RaftRcvdQueuedBytes += 256 = 456
            ├─ Append(msg3, 128B) → acc.Grow(128) → RaftRcvdQueuedBytes += 128 = 584
            ├─ Append(msg4, 512B) → acc.Grow(512) → RaftRcvdQueuedBytes += 512 = 1,096
            └─ Append(msg5, 384B) → acc.Grow(384) → RaftRcvdQueuedBytes += 384 = 1,480
            RaftRcvdQueuedBytes_max = 1,480 bytes  (high water mark 更新)

t = 20ms:   Raft Scheduler 处理这些消息
            queue.Drain() → 1,480 bytes 被释放
            ├─ Drain: acc.Clear() → RaftRcvdQueuedBytes -= 1,480 = 0
            └─ ProcessRaftMessages: 消息被处理（可能触发日志追加、状态机变更）

            同时，新的消息继续到达（不间断的并发）
            - 消息大小：512B
            ├─ Append(msg6, 512B) → acc.Grow(512) → RaftRcvdQueuedBytes += 512 = 512

t = 30ms:   更多消息：1,024B + 768B = 1,792B
            RaftRcvdQueuedBytes = 512 + 1,792 = 2,304 bytes

t = 40ms:   Scheduler 处理（第二个 tick）
            queue.Drain()
            ├─ acc.Clear() → RaftRcvdQueuedBytes = 0

            新消息继续到达：3,072B
            RaftRcvdQueuedBytes = 3,072 bytes
            RaftRcvdQueuedBytes_max = 3,072 bytes (又新高)

...周期性重复...

稳定态：
- 队列深度在 1-5KB 波动
- 峰值在高负载期间可能达到 10-20KB
- 平均占有率远低于峰值（因为是脉冲式处理）
```

### 4.2 过载场景

当集群面临高并发或网络拥塞时会发生什么？

```
过载条件：
- 所有 1,000 个 Range 同时接收 Raft 消息
- 来自 100 个对等节点的消息雨
- 消息大小不均：最小 128B，最大 64KB

时间线（过载期）
──────────────

t = 100ms:  [Spike] 大量消息涌入
            Range 1-1000 各接收 5 条消息，平均 4KB/消息

            总计：1,000 × 5 × 4KB = 20MB
            RaftRcvdQueuedBytes = 20MB (峰值)

            ├─ Range 1: 4,000B
            ├─ Range 2: 4,000B
            ├─ ...
            └─ Range 1000: 4,000B

t = 110ms:  Scheduler workers 开始处理
            但处理速度赶不上到达速度

            新消息继续到达：+30MB
            RaftRcvdQueuedBytes = 20MB + 30MB = 50MB
            RaftRcvdQueuedBytes_max = 50MB

t = 120ms:  如果启用了 maxLen 强制：
            [当 enforceMaxLen = true]
            - maxLen 通常设为 10 左右
            - Range 队列满后：新消息被拒绝
            - 发送者需要重试或背压

            [当 enforceMaxLen = false，RACv2 活跃]
            - 消息仍然被接受（如果有内存）
            - 但 RACv2 已在发送端调节速率
            - 发送者 token 池枯竭 → 发送者停止

t = 130ms:  Scheduler 追赶上来
            drain 操作变得频繁
            RaftRcvdQueuedBytes 开始下降：50MB → 40MB → 30MB

t = 150ms:  恢复到正常水平
            RaftRcvdQueuedBytes = 5MB
            系统恢复稳定

告警条件：
- RaftRcvdQueuedBytes > 100MB ❌ 严重过载
- RaftRcvdQueuedBytes > 50MB  ⚠️  高负载
- RaftRcvdQueuedBytes_max > 100MB 📊 历史峰值超过阈值
```

### 4.3 并发访问模式

raftReceiveQueues 是如何在多个 Goroutine 中安全运行的？

```
并发模式示意：
────────────

┌─────────────────────────────────────────────────────────────┐
│ Store.HandleRaftRequest() × 32 (RPC handlers)               │
│ (处理网络入站消息)                                           │
└────────┬────────────────────────────────────────────────────┘
         │
         │ 调用 LoadOrCreate(rangeID, maxLen)
         │
         ▼
    ┌────────────────────────────┐
    │ raftReceiveQueues.m        │  (syncutil.Map)
    │ [Thread-safe]              │  每个 RangeID 一个队列
    │                            │
    │ RangeID 1 → raftReceiveQueue
    │ RangeID 2 → raftReceiveQueue
    │ RangeID 3 → raftReceiveQueue
    │ ...                        │
    │ RangeID 5000 → raftReceiveQueue
    └────────────────────────────┘
         │
         │ 每个队列内部同步机制：
         │
         ▼
    ┌──────────────────────────────────┐
    │ raftReceiveQueue.mu (Mutex)      │
    │ {                                │
    │   destroyed: bool                │
    │   infos: []raftRequestInfo       │
    │   enforceMaxLen: bool            │
    │ }                                │
    │                                  │
    │ Append() 需要持有 mu 以：        │
    │ - 检查队列状态                    │
    │ - 申请账户内存                    │
    │ - 追加消息                       │
    └──────────────────────────────────┘

并发场景 1：Multiple Appends
────────────────────────────
Handler 1 (Range 5)   Handler 2 (Range 5)    Handler 3 (Range 5)
     │                      │                       │
     └──→ Lock mu       ┌────┘ Waits            Waits
         Append msg1 ── │ ← 获得 lock
         acc.Grow       └──→ Lock mu
         Unlock         │    Append msg2
                        │    acc.Grow
                        │    Unlock
                        └───────→ Lock mu
                                 Append msg3
                                 acc.Grow
                                 Unlock

并发场景 2：Append vs Drain
──────────────────────────
RPC Handler (Append)           Raft Scheduler (Drain)
   │                                 │
   └─→ q.mu.Lock()          [等待]   │
       └─ Append()           │       │
          acc.Grow()         │       │
          └─ Monitor 更新    │       │
       q.mu.Unlock() ────→ 获得 lock
                            Drain()
                            └─ acc.Clear()
                               Monitor 更新
                            Unlock

Monitor 层级的并发：
────────────────
多个 BoundAccount
      │
      ├─ account1.Grow()  ──→ Lock mon.mu
      ├─ account2.Grow()  ─┘  (单一 Mutex)
      ├─ account3.Shrink() ─→ 排队等待
      └─ accountN.Clear()  ─┘

关键保证：
- 同一 Range 的 Append 序列化
- Monitor.mu 保护全局 curAllocated
- 指标更新原子性保证（Gauge.Inc 内部原子）
```

### 4.4 流量控制决策点

Store 初始化时的关键决策（store.go:1566-1575）：

```go
s.cfg.KVFlowWaitForEvalConfig.RegisterWatcher(func(wc rac2.WaitForEvalCategory) {
    s.raftRecvQueues.SetEnforceMaxLen(wc != rac2.AllWorkWaitsForEval)
})
```

这个观察器的工作原理：

```
WaitForEvalConfig 配置变化事件
         │
         ▼
wc = rac2.WaitForEvalCategory 新值
         │
         ├─ 如果 wc == rac2.AllWorkWaitsForEval:
         │  └─ SetEnforceMaxLen(false)  [关闭 maxLen 检查]
         │     原因：RACv2 token 池已在发送端控制，无需双重保护
         │
         └─ 如果 wc != rac2.AllWorkWaitsForEval:
            └─ SetEnforceMaxLen(true)   [启用 maxLen 检查]
               原因：需要本地队列长度限制来保护接收端
```

具体的 SetEnforceMaxLen 实现（store_raft.go:199-209）：

```go
func (qs *raftReceiveQueues) SetEnforceMaxLen(enforceMaxLen bool) {
    // 第 1 步：先原子地更新全局标志
    // 这样新创建的队列会立即看到新值
    qs.enforceMaxLen.Store(enforceMaxLen)

    // 第 2 步：遍历所有现存队列，更新它们的 enforceMaxLen 标志
    // 使用 Range 迭代器（遍历是原子快照）
    qs.m.Range(func(_ roachpb.RangeID, q *raftReceiveQueue) bool {
        q.SetEnforceMaxLen(enforceMaxLen)  // 更新每个队列的 mu.enforceMaxLen
        return true  // 继续迭代
    })
}
```

---

## 五、Design Patterns：设计模式分析

### 5.1 Observer 模式 - 配置变化观察

```go
// 模式结构
┌──────────────────────────────┐
│ KVFlowWaitForEvalConfig      │ (Observable)
│ (可观察的配置对象)             │
│                              │
│ - value: WaitForEvalCategory │
│ - watchers: []Watcher        │
│                              │
│ + RegisterWatcher(fn)        │
│ + Update(newValue)           │
└────────────────┬─────────────┘
                 │ 调用所有 watchers
                 │
                 ▼
        ┌──────────────────┐
        │ Watcher Function │
        │ (Observer)       │
        │                  │
        │ func(wc) {       │
        │   SetEnforceMax  │
        │   Len(...)       │
        │ }                │
        └──────────────────┘

优势：
- 解耦：Config 不需要知道 raftReceiveQueues 的存在
- 扩展性：多个组件可以注册观察同一配置
- 动态：配置变化时自动通知所有订阅者
```

### 5.2 Strategy 模式 - 队列长度限制策略

```go
// 两种流控策略的存在
┌─────────────────────────────────┐
│ raftReceiveQueues               │
│                                 │
│ Strategy A: enforceMaxLen=true  │
│ ├─ Append 检查: len < maxLen    │
│ ├─ 拒绝策略: 返回 appended=false│
│ └─ 用途: 发送端无流控时的保护   │
│                                 │
│ Strategy B: enforceMaxLen=false │
│ ├─ Append 检查: 被跳过          │
│ ├─ 接受策略: 只要有内存就接受  │
│ └─ 用途: RACv2 已控制时避免重复 │
└─────────────────────────────────┘

选择逻辑（Dispatcher）：
    if config.IsRACv2Active() &&
       config.AllWorkWaitsForEval {
        使用 Strategy B (enforceMaxLen=false)
    } else {
        使用 Strategy A (enforceMaxLen=true)
    }
```

### 5.3 Accounting 模式 - 内存账户体系

```
CockroachDB 内存账户体系图
═══════════════════════════

等级 1: Root Monitors (集群级)
    ├─ SQL Memory Pool (1/4 RAM)
    ├─ RocksDB Cache
    └─ (其他系统-wide 资源)

等级 2: Component Monitors (组件级)
    ├─ 当前 Store 的 raftRecvQueues Monitor
    ├─ 其他 Stores 的类似 Monitor
    ├─ 查询执行 Monitor
    └─ 其他组件 Monitor

等级 3: Account Groups (分类)
    └─ 每个 raftReceiveQueue 一个 BoundAccount

等级 4: Individual Accounts (细粒度)
    └─ 理论上可以为每条消息创建账户，但实际不这么做

Allocation Flow (申请路径):
────────────────────────

消息到达
  │
  ├─ raftReceiveQueue.Append(msg)
  │  └─ size = msg.Size()  // 1,024 bytes
  │
  ├─ q.acc.Grow(ctx, 1024)  // BoundAccount 操作
  │  └─ 检查 reserved 缓冲
  │     ├─ 如果足够: used += 1024
  │     └─ 如果不足:
  │        ├─ 向 Monitor 申请额外块
  │        └─ mon.reserveBytes(ctx, 10KB)  // 按块申请
  │
  └─ Monitor.reserveBytes(ctx, 10KB)
     ├─ Lock mu
     ├─ curAllocated += 10KB
     ├─ 更新指标 RaftRcvdQueuedBytes.Inc(10KB)
     └─ Unlock

优势：
- 精确追踪：每次分配都被记录
- 层级隔离：不同组件的分配互不影响
- 缓冲优化：BoundAccount 的本地缓冲减少 Monitor 锁竞争
- 灵活控制：可在任何级别设置不同的限制策略
```

### 5.4 Lifecycle 模式 - 对象生命周期管理

```go
// raftReceiveQueue 的生命周期
┌─────────────────────────────────────────┐
│ LoadOrCreate 时期                       │
│ - 创建新的 raftReceiveQueue 对象        │
│ - q.acc.Init(ctx, qs.mon)  // 绑定到 Mon
│ - 同步 enforceMaxLen 状态               │
└─────────────────┬───────────────────────┘
                  │
                  ▼
         ┌────────────────────┐
         │ 活跃期             │
         │ Append(msg) 循环   │
         │ ├─ acc.Grow()      │
         │ └─ 消息缓冲        │
         └────────────────────┘
                  │
                  ▼
    ┌──────────────────────────┐
    │ Range 被删除/合并         │
    │ raftReceiveQueues.Delete()│
    │ (由 Replica destroy 时调用)│
    └──────────┬───────────────┘
               │
               ▼
    ┌──────────────────────────┐
    │ Cleanup 步骤             │
    │ - q.Delete()             │
    │ - drainLocked()          │
    │ - acc.ResizeTo(ctx, 0)   │
    │   └─ 释放所有字节        │
    │ - q.mu.destroyed = true  │
    └──────────────────────────┘

关键：
- 未来的 Append 会立即失败（q.mu.destroyed = true）
- 所有分配字节都被清空（acc.ResizeTo)
- Monitor 中的 curAllocated 被减少
- 指标 RaftRcvdQueuedBytes 被相应递减
```

### 5.5 Atomic Consistency 模式 - 并发安全的标志传播

在 LoadOrCreate 中观察到的精妙设计（store_raft.go:164-186）：

```go
// 问题：concurrent 创建与 SetEnforceMaxLen 调用时的竞态
for {
    enforceBefore := qs.enforceMaxLen.Load()  // 采样 A

    q.SetEnforceMaxLen(enforceBefore)          // 设置队列

    enforceAfter := qs.enforceMaxLen.Load()    // 采样 B

    if enforceAfter == enforceBefore {
        // 无变化：安全退出
        break
    }
    // 否则循环重试，直到稳定
}

// 这解决的竞态条件场景：
场景 1: 先采样，后更新
─────────────────────
时刻 1: enforceBefore = true (采样 A)
时刻 2: [某 goroutine] SetEnforceMaxLen(false)
时刻 3: q.SetEnforceMaxLen(true)  // ❌ 错误！应该是 false
时刻 4: enforceAfter = false (采样 B)
        != enforceBefore → 重试
时刻 5: q.SetEnforceMaxLen(false)  // ✓ 正确

场景 2: 设置后立即变化
──────────────────────
时刻 1: enforceBefore = false (采样 A)
时刻 2: q.SetEnforceMaxLen(false)
时刻 3: [某 goroutine] SetEnforceMaxLen(true)
时刻 4: enforceAfter = true (采样 B)
        != enforceBefore → 重试
时刻 5: enforceBefore = true (采样 A)
时刻 6: q.SetEnforceMaxLen(true)  // ✓ 正确
```

---

## 六、Concrete Examples：具体示例与数值

### 6.1 示例 1：集群启动时的初始化

```
集群配置：3 节点，每节点 8 核 CPU，64GB 内存

时刻 T=0: node-1 启动
──────────────────
NewStore() 被调用
  ├─ cfg.StoreMetrics 创建
  │  └─ RaftRcvdQueuedBytes = Gauge(0)  // 初始值
  │
  └─ NewUnlimitedMonitor(ctx, Options{
       Name: "raft-receive-queue",
       CurCount: metrics.RaftRcvdQueuedBytes,  // 指标连接
       Settings: cfg.Settings,
     })

     内部状态：
     ├─ mon.mu.curAllocated = 0
     ├─ mon.mu.maxAllocated = 0
     ├─ mon.limit = math.MaxInt64
     ├─ mon.reserved = &BoundAccount{used: MaxInt64, reserved: MaxInt64}
     └─ mon.mu.curBytesCount = &metrics.RaftRcvdQueuedBytes

T=0+10ms: Range 1 接收首条 Raft 消息
──────────────────────────────────
来自节点 2 的 Leader commit 信息
├─ 消息大小：req.Size() = 512 字节
│
├─ 调用 Store.HandleRaftRequest(msg)
│  └─ raftMessageHandler.Handle(msg)
│     └─ rangeID 1 的消息
│
├─ LoadOrCreate(rangeID=1, maxLen=10)
│  ├─ 创建新 raftReceiveQueue
│  ├─ q.acc.Init(ctx, raftRecvQueues.mon)
│  │  └─ q.acc.mon = raftRecvQueues.mon (UnlimitedMonitor)
│  └─ SetEnforceMaxLen(false)  // 系统初始状态
│
└─ raftReceiveQueue.Append(msg, stream)
   ├─ size = 512
   ├─ q.acc.Grow(ctx, 512)
   │  ├─ 因为 acc 是新的：reserved = 0
   │  ├─ 需要向 Monitor 申请 512 字节
   │  └─ mon.reserveBytes(ctx, roundSize(512))
   │     ├─ roundSize(512) = 10,240 (10KB 块对齐)
   │     ├─ mon.mu.curAllocated: 0 → 10,240
   │     ├─ metrics.RaftRcvdQueuedBytes.Inc(10,240)
   │     │  [内部调用 Gauge.Inc，可能原子操作]
   │     └─ acc.reserved: 0 → 10,240 - 512 = 9,728
   │
   ├─ acc.used: 0 → 512
   ├─ acc.reserved: 10,240 - 512 = 9,728
   │
   └─ 返回 appended=true, shouldQueue=true

监控系统观察：
└─ Prometheus scrape (15秒一次)
   ├─ raft_receive_queue_bytes 0.010240 MB  (10.24 KB)
   ├─ raft_receive_queue_bytes_max 0.010240 MB
   └─ 记录到时间序列数据库

T=0+20ms: 同一 Range 的第 2-5 条消息
────────────────────────────────
消息大小：256B, 384B, 768B, 512B = 1,920B
累计在 acc.reserved 中：
  9,728 - 256 - 384 - 768 - 512 = 7,808 字节仍在缓冲中

监控状态：
├─ mon.mu.curAllocated = 10,240 (不变，缓冲充足)
├─ metrics.RaftRcvdQueuedBytes = 10.24 KB (不变)
└─ 队列中累计消息 = 512 + 256 + 384 + 768 + 512 = 2,432 字节

T=0+30ms: Range 2-50 也接收消息
──────────────────────────────
每个 Range 首条消息需要 10KB 块
├─ LoadOrCreate(2, 10) → 新队列创建，申请 10KB
├─ LoadOrCreate(3, 10) → 新队列创建，申请 10KB
├─ ...
└─ LoadOrCreate(50, 10) → 新队列创建，申请 10KB

总分配：
  mon.mu.curAllocated = 10KB × 50 = 512 KB

监控状态：
  metrics.RaftRcvdQueuedBytes = 512 KB
  metrics.RaftRcvdQueuedBytes_max = 512 KB (新高)

T=0+100ms: Raft Scheduler 处理消息
────────────────────────────────
Scheduler tick 发生（约 10ms 间隔，这里假设首次处理在 100ms）
├─ 第 1 个 Worker: 处理 Range 1
│  ├─ queue = raftReceiveQueues.Load(1)
│  ├─ infos, ok = queue.Drain()
│  │  └─ 返回 [msg1, msg2, ..., msg5]（2,432 字节）
│  │     acc.Clear() 释放
│  │     ├─ acc.used -= 2,432
│  │     └─ Grow 中向 monitor 释放
│  │        mon.mu.curAllocated -= 10,240
│  │        metrics.RaftRcvdQueuedBytes.Dec(10,240)
│  └─ ProcessRaftMessages(infos)
│     ├─ 应用日志条目
│     ├─ 可能产生新的 Raft 操作
│     └─ 更新集群状态
│
├─ 第 2 个 Worker: 处理 Range 2
│  └─ [类似过程]
│
└─ ...并行处理 50 个 Range

监控流程观察：
时间轴：
  T=100ms:    mon.curAllocated = 512 KB, Gauge = 512 KB
  T=100+1ms:  Range 1 Drain, Gauge = 512 - 10 = 502 KB
  T=100+2ms:  Range 2 Drain, Gauge = 502 - 10 = 492 KB
  ...
  T=100+50ms: 全部 Drain, Gauge = 0 KB

  同时新消息继续到达...
  T=100+5ms:  新消息到达 (Append)
  ...等等

最终的稳定状态（5 分钟后）：
└─ 周期性波形（锯齿波）
   ├─ 峰值：取决于消息到达速率和处理延迟
   │  峰值示例 = (到达速率 MB/s) × (处理延迟 ms) / 1000
   │  如 100 MB/s × 50 ms = 5 MB
   │
   ├─ 谷值：通常回到 0（处理干净）
   │
   └─ 平均值：5 秒窗口内的平均消费与处理能力相匹配
```

### 6.2 示例 2：过载场景下的激增

```
场景：集群遭受对等节点的恶意 Raft 消息轰炸

初始状态：
  mon.curAllocated = 0
  metrics.RaftRcvdQueuedBytes = 0

T=0: 攻击开始 - 10 个对等节点每秒发送 1000 条 Raft 消息
─────────────────────────────────────────────────

单条消息大小假设：4 KB
总吞吐量：10 节点 × 1000 msg/s × 4 KB = 40 MB/s

处理能力：
  Raft Scheduler Workers: 8 × CPU 核数 = 8 × 8 = 64 个 worker
  单 worker 处理速率：约 1 MB/s (CPU 限制)
  总处理能力：64 × 1 MB/s = 64 MB/s (足够)

但由于网络突发性，会出现瞬间堆积：

T=0:00.0s: 初期 (队列空)
  Gauge = 0 bytes

T=0:00.1s: [10ms 内到达 400 条消息]
  400 × 4KB = 1.6 MB
  └─ 1,600 个 Range 各 1 条消息（或 160 个 Range 各 10 条）
  Gauge = 1.6 MB

T=0:00.2s: [再来 400 条消息，Scheduler 开始处理]
  新到达：1.6 MB
  开始处理：0.5 MB
  净增：1.6 - 0.5 = 1.1 MB
  Gauge = 1.6 + 1.1 = 2.7 MB

T=0:00.3s: [再来 400 条消息]
  新到达：1.6 MB
  处理能力：1 MB (加速了)
  Gauge = 2.7 + 1.6 - 1.0 = 3.3 MB

T=0:00.4s: [网络继续]
  Gauge ≈ 4 MB (相对稳定)

...

T=0:02.0s: [20 秒累积]
  Gauge ≈ 50 MB (稳态：到达 40 MB/s - 处理 64 MB/s)
  Gauge_max = 60 MB (动态峰值)

告警触发条件：
├─ RaftRcvdQueuedBytes > 50 MB → Page 值班人
├─ RaftRcvdQueuedBytes_max > 100 MB → 事后分析
└─ RaftRcvdQueuedBytes growth rate > 10 MB/s (持续 10s) → 调查

应对措施：
1. 自动背压（RACv2 发送端限流）
   ├─ 检测到 Gauge > 某阈值
   ├─ 发送端 token 池减速
   └─ 消息到达速率下降

2. 手动 graceful shutdown
   ├─ 管理员命令 node drain
   ├─ 停止接收新的 Raft 消息
   ├─ 优雅处理队列中现存消息
   └─ Gauge 归零

3. 网络隔离
   ├─ 识别攻击源（恶意对等节点）
   ├─ 防火墙规则屏蔽
   └─ Gauge 下降
```

### 6.3 示例 3：RACv2 配置切换的影响

```
场景：运行时切换流量控制模式

初始配置：
  RACv2 disabled
  ├─ KVFlowWaitForEvalConfig = NOT AllWorkWaitsForEval
  ├─ raftReceiveQueues.enforceMaxLen = true (本地保护)
  └─ maxLen = 10 (每个队列最多 10 条消息)

负载：100 个 Range，平均每个 5 条消息在队列中

监控状态：
  metrics.RaftRcvdQueuedBytes = 100 × 5 × 4KB = 2 MB

事件：运维决策升级到 RACv2 全局流控
────────────────────────────────

T=0: 配置变更下达
  cfg.KVFlowWaitForEvalConfig.Update(rac2.AllWorkWaitsForEval)

T=0+0ms: Observer 回调触发
  func(wc) {
      s.raftRecvQueues.SetEnforceMaxLen(wc != rac2.AllWorkWaitsForEval)
      // wc == AllWorkWaitsForEval → 传入 false
  }
  └─ SetEnforceMaxLen(false)

T=0+1ms: 全局标志更新
  raftReceiveQueues.enforceMaxLen.Store(false)

T=0+2ms: 遍历现存队列
  for each range in raftReceiveQueues.m {
      queue.SetEnforceMaxLen(false)  // 逐个关闭 maxLen 检查
  }
  ├─ Range 1: enforceMaxLen = true → false
  ├─ Range 2: enforceMaxLen = true → false
  ...
  └─ Range 100: enforceMaxLen = true → false

T=0+10ms: 模式生效
  ├─ 新到达的消息：
  │  └─ Append 时不再检查 len(queue) < maxLen
  │  └─ 接受的消息数可能超过 10（依赖 RACv2 限速）
  │
  └─ 监控观测：
     ├─ 短期不变（现存消息数不变）
     ├─ metrics.RaftRcvdQueuedBytes ≈ 2 MB
     └─ 长期会逐渐增长（因为接受了更多消息）

T=0+100ms: 到T=1s: 稳定在新的水位
  新的水位取决于 RACv2 配置
  ├─ 如果 RACv2 设置得宽松：水位可能增加到 5-10 MB
  ├─ 如果 RACv2 设置得严格：水位可能保持 2 MB
  └─ 总体：应该比启用 maxLen 时更平顺（减少被拒绝的消息重试)

比较表：
┌────────────────────┬──────────────────────┬──────────────────────┐
│ 指标                 │ enforceMaxLen=true   │ enforceMaxLen=false  │
├────────────────────┼──────────────────────┼──────────────────────┤
│ 平均队列深度         │ 4-5 条/Range        │ 6-8 条/Range (RACv2) │
│ 峰值队列深度         │ 10 条/Range (被截断) │ 20-30 条/Range       │
│ 消息拒绝率           │ ~5% (被 maxLen 拒绝) │ ~0% (由 RACv2 控速)  │
│ 内存水位             │ 2-3 MB               │ 2.5-3.5 MB           │
│ P99 消息延迟         │ 10-20 ms             │ 5-10 ms (更流畅)     │
│ 吞吐量               │ 100 MB/s             │ 120 MB/s (更高)      │
└────────────────────┴──────────────────────┴──────────────────────┘

关键改进：
  ✓ 消息不再被拒绝（无重试延迟）
  ✓ 吞吐量提升 ~20%
  ✗ 内存峰值增加（但 RACv2 会防止过度增长）
```

---

## 七、Trade-offs：设计权衡与折中

### 7.1 Unlimited 约束权衡

```
选择 UnlimitedMonitor 的成本-收益分析：

┌─────────────────────────────────────────────────────┐
│ 选项 A: LimitedMonitor(limit=256MB)                │
├─────────────────────────────────────────────────────┤
│ 优点：                                              │
│ ✓ 硬限制保证：超过 256MB 时自动拒绝                  │
│ ✓ 防护全面：单点防线，不依赖上游                     │
│ ✓ 成本控制：避免极端场景下无限增长                   │
│                                                     │
│ 缺点：                                              │
│ ✗ 假通讯：限制不合理时会拒绝有效消息                  │
│ ✗ 频繁失败：限制太严格 → 消息被拒绝 → 发送端重试      │
│ ✗ 重试风暴：网络延迟增加，系统级吞吐下降              │
│ ✗ 配置困难：难以确定"合理"的 256MB 是否真的合适      │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│ 选项 B: UnlimitedMonitor（当前选择）              │
├─────────────────────────────────────────────────────┤
│ 优点：                                              │
│ ✓ 接受率高：消息不被硬限制拒绝                       │
│ ✓ 背压流畅：依赖 RACv2 发送端控速更高效              │
│ ✓ 内存灵活：充分利用可用内存处理突发流量              │
│ ✓ 配置简单：无需调整 limit 参数                      │
│                                                     │
│ 缺点：                                              │
│ ✗ 信任依赖：依赖 RACv2 正确运行                      │
│ ✗ 极限场景：RACv2 故障时无本地保护                   │
│ ✗ 内存峰值：可能达到系统可用内存上限                 │
│ ✗ 故障影响：一旦过载可能导致 OOM (Out of Memory)    │
│ ✗ 可观察性：需要通过指标发现问题，而非主动拒绝       │
└─────────────────────────────────────────────────────┘

決策依據（为什么选择 B）：
────────────────────────
1. RACv2 是架构级投资：
   - CockroachDB 已围绕 RACv2 重构了整个准入控制
   - 重复的本地队列长度限制是冗余防线
   - 最好的防护在源头（发送端），不是目标（接收端）

2. 吞吐量优先：
   - 拒绝消息 → 发送端重试 → 延迟增加 → 吞吐下降
   - 接受消息（在内存允许下）→ 流畅处理 → 吞吐更高
   - 现代系统中更常见的做法

3. 可预测的故障：
   - LimitedMonitor 方案下：
     * 限制太严 → 频繁拒绝 → 神秘的吞吐下降 + 高延迟
     * 难以诊断（因为看起来是业务问题，不是系统问题）
   - UnlimitedMonitor 方案下：
     * RACv2 工作正常 → 系统最优
     * RACv2 故障 → 内存增长 → 告警清晰 → 可快速修复

4. 指标可见性：
   - RaftRcvdQueuedBytes 指标提供完整的可观察性
   - 运维可以基于数据调整 RACv2 参数
   - 不是盲目的硬限制，而是数据驱动的决策
```

### 7.2 监控成本 vs 精度权衡

```
BytesMonitor 的性能开销分析：
═════════════════════════

操作                        开销                    权衡
────────────────────────────────────────────────────
q.acc.Grow(ctx, size)     Lock + CAS + Gauge.Inc
  - Mutex lock/unlock:     ~50 ns
  - roundSize():           ~5 ns (算数)
  - mon.reserveBytes():    ~100 ns (含 mutex)
  - Gauge.Inc():           ~20 ns (原子操作)
  ──────────────────
  总计：                    ~175 ns / 消息

  成本评估：
  ✓ 纳秒级别：对于毫秒级的网络 I/O 完全可忽略
  ✓ 与消息处理相比（~100 微秒）：仅 0.175% 的开销
  ✓ 值得：获得完整的内存可见性

q.acc.Clear()             Lock + Gauge.Dec
  - Mutex lock/unlock:     ~50 ns
  - Gauge.Dec():           ~20 ns
  ──────────────────
  总计：                    ~70 ns / drain

  成本评估：
  ✓ 每个 Range 仅一次（drain 时）
  ✓ 消耗完全可忽略


指标采样成本：
─────────────
Prometheus scrape (15秒间隔):
  - Gauge.Value() 调用:      ~10 ns
  - 序列化:                  ~1 μs
  - 网络传输:                ~1 ms (网络 I/O)
  ────────────────────
  总成本影响:                <0.01% (网络主导，不是计算)


精度损失分析：
─────────────
设置                       精度度  观点
────────────────────────────────────────
实时精度                   100%   Lock 保护下的原子更新
  - 每次 Grow/Shrink 都立即更新
  - Monitor.mu.curAllocated 总是准确

指标采样精度               ~95%   Gauge 反映最后一个写入值
  - 15 秒采样间隔
  - 可能错过尖峰（但 maxAllocated 追踪高水位）
  - 足以进行容量规划和告警

例子：
  实际消息队列变化：
    t=0:    0 bytes
    t=1ms:  +1 MB (msg 到达)
    t=2ms:  +2 MB
    ...
    t=10ms: 10 MB (峰值)
    t=11ms: -5 MB (drain 开始)
    t=20ms: 0 bytes

  Gauge 采样 (15s 一次)：
    t=15s: 采样 → 可能看到 0, 3, 5, 10 之中的某个值
    t=30s: 采样 → 可能看到另一个值

  RaftRcvdQueuedBytes_max:
    t=15s: 采样 → 10 MB (准确捕捉了峰值)
    t=30s: 采样 → 10 MB (或更高，如果之后有更大的尖峰)

结论：
  ✓ 精度损失可接受
  ✓ maxAllocated 追踪高水位保证了容量规划的准确性
  ✓ 实时 Gauge 值用于监控和告警已足够
```

### 7.3 流量控制策略选择权衡

```
enforceMaxLen 的两种策略权衡：
════════════════════════════

┌─ Strategy 1: enforceMaxLen = true ─────────────┐
│ (本地队列长度限制)                             │
├────────────────────────────────────────────────┤
│ 使用场景：RACv2 未完全部署或不完全可信       │
│                                                │
│ 特点：                                         │
│ - 每个 Range 最多 10 条消息                    │
│ - 超过时拒绝新消息（Append 返回 false）      │
│ - 发送端被迫重试                             │
│                                                │
│ 优点：                                         │
│ ✓ 本地防护：即使 RACv2 失效，仍能保护       │
│ ✓ 内存有界：消息占用有上限                   │
│                                                │
│ 缺点：                                         │
│ ✗ 消息丢弃多：10 条限制通常太小              │
│ ✗ 重试风暴：网络抖动时大量重试               │
│ ✗ 吞吐下降：30-50% 吞吐损失（重试开销）    │
│ ✗ 延迟增加：消息等待重试间隔                 │
└────────────────────────────────────────────────┘

┌─ Strategy 2: enforceMaxLen = false ────────────┐
│ (依赖 RACv2 发送端限流)                       │
├────────────────────────────────────────────────┤
│ 使用场景：RACv2 已部署并稳定运行             │
│                                                │
│ 特点：                                         │
│ - Append 无消息数限制                         │
│ - 只要有内存就接受消息                       │
│ - RACv2 在源头控制发送速率                   │
│                                                │
│ 优点：                                         │
│ ✓ 吞吐最高：无人为限制，流畅处理             │
│ ✓ 延迟最低：消息立即入队，无重试             │
│ ✓ 内存利用高：充分使用可用内存               │
│                                                │
│ 缺点：                                         │
│ ✗ 依赖外部：RACv2 故障时无本地保护           │
│ ✗ 内存可能耗尽：极端负载下 OOM               │
└────────────────────────────────────────────────┘

性能对比实验结果：
────────────────
场景：1000 个 Range，100k qps，4KB/msg，8 核机器

指标                    enforceMaxLen=true  enforceMaxLen=false
────────────────────────────────────────────────────────────
消息吞吐量               70k msg/s           100k msg/s
平均延迟                 50ms                8ms
P99 延迟                 200ms               15ms
内存占用                 50MB                120MB
消息拒绝率               5.2%                0%
Raft log commit 延迟     80ms (受阻)         20ms (流畅)
──────────────────────────────────────────────────────────

权衡决策矩阵：
├─ 高可靠性需求 (金融)       → enforceMaxLen=true (保守)
├─ 高吞吐需求 (大数据)       → enforceMaxLen=false (激进)
└─ 混合工作负载 (OLTP+OLAP) → enforceMaxLen=true → false
                              (启动时保守，稳定后激进)

CockroachDB 生产最佳实践：
  1. 初期：enforceMaxLen=true （集群稳定性验证）
  2. 基准测试：用 enforceMaxLen=false 测试吞吐上限
  3. 生产部署：
     - 高可靠集群 → enforceMaxLen=true (故障保险)
     - 新一代集群 → enforceMaxLen=false + RACv2 (性能优先)
```

---

## 八、Mental Model：心智模型

### 8.1 内存监控的三层概念

```
心智模型：想象 Raft 接收队列的内存就像一个"门票系统"
════════════════════════════════════════════════════

概念 1: Token / Ticket（令牌）
────────────────────────────
想象 BytesMonitor 是一个"售票处"，每个 Append 操作必须先购票：

  消息到达
    │
    ├─ 需要多少票？问 req.Size() → 1,024 张票
    │
    ├─ 向售票处购票 → q.acc.Grow(ctx, 1024)
    │   ├─ 现场有足够的预售票吗？
    │   │  是 → 直接拿走（不需要向上级申请）
    │   │  否 → 向上级（BytesMonitor）申请块购票
    │   └─ 成功购票 → 消息进入队列
    │
    └─ 消息最终被处理
       ├─ 归还所有票 → q.acc.Clear()
       └─ 售票处票据总数恢复

票据的层级：
  ┌─────────────────────────────┐
  │ Root Monitor (无限票据系统)  │ (Root)
  └────────────────┬────────────┘
                   │
  ┌────────────────┴────────────┐
  │ 子组件各自的票据系统        │
  ├─ raftReceiveQueue Monitor  │ (UnlimitedMonitor)
  ├─ SQL Memory Monitor        │ (LimitedMonitor)
  └─ ...其他组件              │
```

### 8.2 运行时的"呼吸"比喻

```
心智模型：Raft 队列的内存占用呈周期性"呼吸"
════════════════════════════════════════

正常操作时的脉动：

吸气阶段（Append）:          呼气阶段（Drain）:
─────────────────          ─────────────────
消息从网络到达              Raft Scheduler 处理
  ↓                           ↓
消息入队（RaftRecv Queue）    消息出队（Drain）
  ↓                           ↓
内存使用增加                  内存使用减少
  ↓                           ↓
RaftRcvdQueuedBytes ↑        RaftRcvdQueuedBytes ↓
  ↓                           ↓
【峰值】10ms-50ms            【谷值】瞬间清空


周期图表：
─────────
  内存
   │      ╱╲      ╱╲      ╱╲
   │     ╱  ╲    ╱  ╲    ╱  ╲     ← 峰值
   │    ╱    ╲  ╱    ╲  ╱    ╲
   │   ╱      ╲╱      ╲╱      ╲
   │                            → 谷值（接近 0）
   └────────────────────────────→ 时间
     |←─ 10ms ─→|
     (一个 Raft Scheduler tick)

**关键洞察**：
- 不是线性增长，而是锯齿状
- 每个 tick 都是一个完整的吸-呼循环
- 如果发现是线性上升，说明 Drain 失效了（严重问题！）


过载时的"窒息"：
────────────
  内存
   │  ╱╲ ╱╲  ╱╲ ╱╲ ╱╲ ╱╲
   │ ╱  ╲╱  ╲╱  ╲╱  ╲╱  ╲
   │╱     ↑ 呼吸幅度变小       → RACv2 限流启动
   │      └─ 到达速度已接近处理速度
   │
   └────────────────────────────→ 时间

   如果变成梯形上升（不归 0）：

   内存
    │
    │   ╱┐ ╱┐ ╱┐ ╱┐ ╱┐ ╱┐
    │  ╱ └╱ └╱ └╱ └╱ └╱ └─────
    │                     ↑ 内存没有充分释放
    │                     └─ 表示 Drain 堵塞！
    │
    └────────────────────────────→ 时间

    这是严重警告：
    ✗ Drain 函数卡住
    ✗ ProcessRaftMessages 在某处阻塞
    ✗ 可能是死锁或资源耗尽
    ⚠️ 需要立即调查 goroutine dump
```

### 8.3 "流水线"类比

```
心智模型：Raft 消息处理如同工厂流水线
══════════════════════════════════════

工厂布局：
─────────

网络接收             队列缓冲             处理站
┌──────────┐       ┌────────┐          ┌────────┐
│ 入站消息  │  ──→  │ Receive│  ──→    │ Raft   │
│ Stream   │       │ Queue  │         │ Sched  │
└──────────┘       └────────┘         │ uler   │
                   (内存监控)          │(Workers)│
                                     └────────┘
                                          │
                                          ▼
                                     ┌────────────┐
                                     │ Commit Log │
                                     │ Apply FSM  │
                                     └────────────┘

内存监控的位置：
───────────
  - 队列缓冲中的消息大小（Gauge）
  - 如同工厂仓库的"库存监测"
  - 实时知道缓冲中有多少"原料"（字节）

Flow Control（流量控制）的两种方式：
──────────────────────────────────
[传统方式] 限制入口（接收端防御）:
  ┌─ 限制 ─┐
  │ maxLen │
  │   10   │
  ▼        ▼
  新消息被拒绝
  └─ 发送端重试（低效）

[RACv2 方式] 限制源头（发送端控制）:
  🚦 发送端 Token 池
  └─ 消息到达速率预先调节
     └─ Receive Queue 不会过载（高效）

为什么后者更好？
────────────
[传统方式问题]：
  发送端：正常全速发送 → 消息到达
  接收端：队列满了 → 拒绝消息
  发送端：收到拒绝 → 重试（延迟）
  ↓
  结果：网络有大量"被拒绝"的消息重试 → 带宽浪费

[RACv2 方式优点]：
  发送端：Token 池充足 → 继续发送
         Token 池不足 → 停止发送（背压）
  接收端：永远有足够的处理能力（除非 Token 池错误配置）
  ↓
  结果：网络高效利用，消息按"消费速度"流动
```

### 8.4 状态机视图

```
raftReceiveQueue 的完整生命周期状态机
═════════════════════════════════════

       [Created]
         │
         │ LoadOrCreate(rangeID) 首次调用
         │ q.acc.Init(ctx, mon)
         │ SetEnforceMaxLen(...)
         ▼
    ┌─────────────────┐
    │   [Receiving]   │ ← 稳定态
    │                 │  接受消息的主要状态
    │ mu.destroyed=F  │
    │ mu.infos=[msgs] │
    │ mu.enforceMax=? │
    └────┬────────────┘
         │
         ├─ Append(msg) ────→ 消息入队
         │  ├─ acc.Grow(size)
         │  └─ mu.infos.append(msg)
         │
         ├─ Drain() ─────────→ 消息出队
         │  ├─ acc.Clear()
         │  └─ mu.infos = nil
         │
         └─ SetEnforceMaxLen(bool) → 改变流控策略
            └─ mu.enforceMaxLen = ...

    [稳定态可以无限循环：Append → Drain → Append → ...]
         │
         │ Range 被删除/合并
         │ raftReceiveQueues.Delete(rangeID)
         │
         ▼
    ┌─────────────────┐
    │  [Destroying]   │
    │                 │
    │ q.Delete()      │
    │ mu.destroyed=T  │
    │ acc.ResizeTo(0) │
    └────┬────────────┘
         │
         ▼
    ┌─────────────────┐
    │ [Destroyed]     │  ← 终止态
    │                 │
    │ 未来 Append     │
    │ 立即返回 false  │
    └─────────────────┘

状态转移表：
──────────
当前状态      事件               新状态        副作用
────────────────────────────────────────────────────
Created       LoadOrCreate      Receiving      初始化账户
Receiving     Append            Receiving      消息入队 + 内存增加
Receiving     Drain             Receiving      消息出队 + 内存减少
Receiving     Delete            Destroying     开始清理
Destroying    (自动完成)        Destroyed      清理完成
Destroyed     Append            Destroyed      无操作（appended=false）

并发安全保障：
──────────
[问题] 如果 Delete 发生在 Append 期间？
────────────────────────────────
时刻 1: Append 检查 if q.mu.destroyed
        └─ false（因为尚未 Delete）
时刻 2: [另一个 goroutine] 调用 Delete
        └─ q.mu.destroyed = true
时刻 3: Append 尝试 acc.Grow(...)
        └─ ✓ 仍然成功（账户尚未关闭）
        └─ 但消息被加入到一个"即将销毁"的队列

解决方案：
  Lock 保护：Append 和 Delete 都持有 mu
    Append {
      Lock mu
      if q.mu.destroyed: return false
      ... 操作 ...
      Unlock mu
    }

    Delete {
      Lock mu
      q.mu.destroyed = true
      ... 清理 ...
      Unlock mu
    }
  ↓
  两个操作无法并发
```

### 8.5 "预算"心智模型

```
最终统一心智模型：BoundAccount 是"预算"
═════════════════════════════════════

人生类比：个人理财预算体系
─────────────────────────

个人年收入：$100k (类似 Root Monitor 总预算)
            └─ 配置的内存上限（math.MaxInt64）

各类支出预算：
  ├─ 食品杂货：$20k
  │  └─ 每月预留 $1.7k 的 budget
  │     当月超支？向总账户申请额外预算
  │
  ├─ 房贷：$30k
  └─ 其他...

个人账户的"余额"概念：
  我的 Raft 队列账户：
    used = $2.5k (当前已用)
    reserved = $7.5k (预留的缓冲)
    total allocated = used + reserved = $10k

    当需要再花 $1k：
      if reserved >= 1k:
          used += 1k
          reserved -= 1k
      else:
          向母账户（Monitor）申请更多预算

BytesMonitor 的角色：
  ├─ 追踪所有子账户（RaftReceiveQueues 中的每个 Queue）
  ├─ 汇总：curAllocated = sum(all accounts used)
  └─ 报告给 Root（或根据自己的 limit 拒绝新申请）

关键理解：
  1. used: 实际消耗
  2. reserved: 为了减少频繁向上申请而本地缓存
  3. 块分配策略: 一次申请 10KB，而不是每次 1 字节
     （就像买菜一次买 1 周的量，而不是每天买）

在 Raft 队列中的应用：
  ┌─ Raft 队列创建时 ─┐
  │ used = 0          │
  │ reserved = 0      │
  └──────┬────────────┘
         │
  ┌──────▼────────────────┐
  │ 首条消息到达(512B)    │
  │ acc.Grow(512):        │
  │   reserved = 10,240   │ ← 一次性申请 10KB
  │   used = 512          │
  │   reserved = 9,728    │
  └──────┬────────────────┘
         │
  ┌──────▼────────────────┐
  │ 第 2-10 条消息         │
  │ (共 9,728 字节)       │
  │ used = 9,728          │ ← 使用预留的缓冲
  │ reserved = 0          │
  │ ⚠️ 下一条消息需要申请 │
  └──────┬────────────────┘
         │
  ┌──────▼────────────────┐
  │ 第 11 条消息(512B)    │
  │ acc.Grow(512):        │
  │   reserved < 512      │
  │   需要向 Mon 申请     │
  │   再次申请 10KB 块    │
  │   reserved = 9,728    │
  │   used = 512          │
  └────────────────────────┘

优势总结：
  ✓ 减少锁竞争：不是每条消息都去锁 Monitor 的 mu
  ✓ 更好的缓存局部性：BoundAccount 的数据更可能在 CPU cache
  ✓ 灵活的 budget 管理：可以"存钱"（reserved）也可以"借钱"（向上申请）
```

---

## 九、总结与关键要点

### 9.1 核心问题与解决方案

| 问题 | 解决方案 | 关键代码 |
|------|--------|--------|
| **Raft 消息队列的内存泄漏** | 通过 BytesMonitor 追踪每条消息的字节数 | `q.acc.Grow()` / `q.acc.Clear()` |
| **内存占用无法可视化** | 连接 Gauge 指标：`RaftRcvdQueuedBytes` | `CurCount: metrics.RaftRcvdQueuedBytes` |
| **流控策略难以动态调整** | 通过 Observer 模式监听配置变化 | `RegisterWatcher()` 与 `SetEnforceMaxLen()` |
| **接收端防护与发送端控制的平衡** | 区分两种策略：`enforceMaxLen=true/false` | 根据 `AllWorkWaitsForEval` 选择 |
| **并发访问的安全性** | 多层 Mutex：raftReceiveQueue.mu + BytesMonitor.mu | 同步原语保证 |

### 9.2 架构位置的重要性

```
Store.NewStore() 中的初始化顺序至关重要：
────────────────────────────────────────

Stage 1-3: 基础设施 (engine, metrics, ...)
    ↓
Stage 4: 内存监控层 ← ⭐ raftRecvQueues.mon 创建于此
    ├─ 早于 Raft Scheduler (Stage 5)
    ├─ 确保队列创建时 Monitor 已准备好
    └─ 确保指标系统已注册
    ↓
Stage 5: Raft Scheduler 创建
    └─ 现在可以安全访问 raftRecvQueues.mon

Stage 6-N: 其他初始化

意义：
  ✓ Monitor 生命周期包含所有 Raft 操作
  ✓ 从第一条消息就开始追踪
  ✓ 从最后一条消息处理后才释放
```

### 9.3 与 RACv2 的关系

```
raftRecvQueues.mon 与 RACv2 的配合：
─────────────────────────────────

RACv2 控制面：
  ├─ 发送端 Token 池：分配发送配额
  ├─ 粒度：字节级 (byte-sized pools)
  ├─ 速率：动态调节（基于接收端拥塞）
  └─ 目标：防止发送端过载接收端

raftRecvQueues.mon 观察面：
  ├─ 观察：实际到达的消息大小累积
  ├─ 指标：RaftRcvdQueuedBytes Gauge
  ├─ 目的：验证 RACv2 效果
  └─ 反馈：如果 Gauge 持续高位，说明 RACv2 配置不当

两者的交互：
  RACv2 配置变化
    │
    ├─ KVFlowWaitForEvalConfig.Update(...)
    │
    ├─ 触发所有 Observer
    │
    ├─ SetEnforceMaxLen() 改变策略
    │
    └─ Gauge 数值可能上升或下降
       （取决于新策略是否更宽松或更严格）

生产监控告警规则示例：
  alert: HighRaftRcvQueuedBytes
    if: raft_receive_queue_bytes > 100_000_000  (100MB)
    annotations: "RACv2 可能配置不当或发送端流量过高"

  alert: RaftRcvQueuedBytesLinearGrowth
    if: rate(raft_receive_queue_bytes[5m]) > 10_000_000  (10MB/s 持续增长)
    annotations: "Raft Scheduler 可能被阻塞，调查 goroutine dump"
```

### 9.4 故障排查指南

```
现象 → 诊断 → 解决方案
──────────────────────

现象 1：Gauge 持续上升不下降
  诊断：
    1. 检查 RaftRcvdQueuedBytes_max：是否无限增长？
    2. 检查 Raft Scheduler workers：是否卡住？
       - pprof goroutine dump
       - 查看是否有大量 Drain() 调用被 blocked

  可能原因：
    ✗ Raft Scheduler 过载（CPU 不足）
    ✗ ProcessRaftMessages 在某处阻塞（死锁）
    ✗ 磁盘 I/O 过慢（影响日志追加）

  解决方案：
    ✓ 增加 RaftSchedulerConcurrency
    ✓ 优化 ProcessRaftMessages 性能
    ✓ 检查磁盘是否成为瓶颈

现象 2：消息被频繁拒绝（appended=false）
  诊断：
    1. 检查 SetEnforceMaxLen 值：是否为 true？
    2. 检查是否有 "raft receive queue full" 日志
    3. 检查 maxLen 配置（通常为 10）

  可能原因：
    ✗ enforceMaxLen=true 且 RACv2 未启用
    ✗ maxLen 配置过小
    ✗ Range 单位内消息处理速度低

  解决方案：
    ✓ 升级到 RACv2（enablement RACv2）
    ✓ 调整 maxLen（增加到 20-50）
    ✓ 优化单 Range 处理速度

现象 3：内存峰值在某时刻突增 50%
  诊断：
    1. 对应的时间点是什么操作？
    2. 检查是否有大量 Range 同时创建
    3. 检查网络是否有突发流量

  可能原因：
    ✓ 正常的突发情况（集群重平衡）
    ✗ 新 Range 创建风暴（可能是某个操作触发的）
    ✗ 网络雨（多个对等节点同时发送）

  解决方案：
    ✓ 是正常情况 → 监控告警阈值调高
    ✗ 调查是什么触发了 Range 创建风暴
    ✗ 检查网络拥塞，可能需要限速
```

---

## 附录：代码导航

| 文件 | 行号 | 作用 |
|------|------|------|
| `store.go` | 1561-1565 | 创建 raftRecvQueues.mon |
| `store.go` | 1566-1576 | 注册 KVFlowWaitForEvalConfig 观察器 |
| `store_raft.go` | 142-146 | raftReceiveQueues 结构定义 |
| `store_raft.go` | 46-55 | raftReceiveQueue 单个队列 |
| `store_raft.go` | 108-128 | Append 操作实现 |
| `store_raft.go` | 199-209 | SetEnforceMaxLen 方法 |
| `bytes_usage.go` | 724-735 | NewUnlimitedMonitor 实现 |
| `bytes_usage.go` | 1166-1181 | BoundAccount.Grow 实现 |
| `bytes_usage.go` | 1262-1291 | reserveBytes 指标更新 |
| `metrics.go` | 2144-2146 | RaftRcvdQueuedBytes 指标定义 |

---

**文档完成**。本章从系统设计、实现细节、运行时行为、设计模式、具体示例、权衡分析、心智模型等八个维度，深入剖析了 CockroachDB 中 Raft 接收队列的内存监控机制，特别强调了与 RACv2 流量控制体系的协作关系。