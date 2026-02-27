# IntentResolver 深度剖析——基于异步清理与批量合并的分布式事务意图解析系统

> **背景**: 本文深入分析 CockroachDB 中 `IntentResolver` 的设计与实现，聚焦于 Store 初始化过程中 IntentResolver 的创建及其在整个分布式事务系统中的关键作用。
>
> **目标读者**: 具备扎实后端与系统基础、理解分布式事务基本概念，但尚未深入阅读该代码的工程师。

---

## 一、第一轮 BFS：职责边界与设计动机（Why）

### 1.1 问题背景：分布式事务中的"意图"（Intent）

在分布式数据库系统中，特别是采用 **MVCC（Multi-Version Concurrency Control）** 的系统中，当一个事务正在进行写入操作时，它会在相应的键上留下 **"意图"（Intent）**。这些 Intent 本质上是：

- **未提交的写入占位符**
- **阻塞性标记**：其他想要读取或写入相同键的事务会被阻塞或需要进行冲突解决
- **事务状态的延伸**：携带事务 ID、时间戳等元数据

```
┌─────────────────────────────────────────────────────┐
│   Key: /users/alice                                 │
│   ┌───────────────────────────────────────────┐    │
│   │ Intent (TxnID: abc123, Status: PENDING)  │    │
│   │ Value: {"name": "Alice Updated"}          │    │
│   └───────────────────────────────────────────┘    │
│                                                      │
│   ← 阻塞其他读/写操作，直到 Intent 被解析          │
└─────────────────────────────────────────────────────┘
```

**核心问题**：
1. **Intent 阻塞问题**：如果 Intent 不被及时清理，会造成持续的读写阻塞
2. **事务终态不确定性**：事务可能 COMMIT、ABORT 或因超时被强制中止
3. **分布式性**：一个事务可能在多个 Range 上留下 Intent，跨越多个 Store 节点
4. **并发冲突**：多个事务可能同时尝试解析同一个 Intent

### 1.2 IntentResolver 的核心职责

`IntentResolver` 的存在是为了**系统性地管理和清理 Intent**，确保：

| 职责维度 | 具体内容 |
|---------|---------|
| **Intent 解析** | 将 Intent 转换为实际的已提交值（COMMIT）或删除（ABORT） |
| **事务推进** | 通过 PushTxn 机制主动推进阻塞事务的状态 |
| **并发控制** | 避免多个解析请求对同一事务产生冲突 |
| **异步批量化** | 在不阻塞前台操作的情况下高效清理 Intent |
| **事务记录 GC** | 在所有 Intent 解析完成后，清理事务记录本身 |

**如果没有 IntentResolver**：
- ❌ Intent 会无限期阻塞后续操作
- ❌ 系统无法自动清理已终止事务的残留
- ❌ 每个 Replica 需要自己处理 Intent 冲突，导致逻辑分散
- ❌ 缺乏统一的批量优化和并发控制

### 1.3 在系统中的位置

```
┌────────────────────────────────────────────────────────────┐
│                     SQL Layer                              │
│         (Distributed SQL, Transaction Coordinator)         │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│                    KV Layer                                │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────┐     │
│  │  TxnCoordSender │  │ DistSender  │  │ RangeCache   │     │
│  └──────────────┘  └─────────────┘  └──────────────┘     │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│                    Store Layer                             │
│  ┌────────────────────────────────────────────────────┐   │
│  │                   Store                            │   │
│  │  ┌──────────────────────────────────────────────┐ │   │
│  │  │       IntentResolver (本文焦点)              │ │   │
│  │  │  • Push transactions                         │ │   │
│  │  │  • Resolve intents (async + batching)       │ │   │
│  │  │  • GC transaction records                    │ │   │
│  │  └──────────────────────────────────────────────┘ │   │
│  │                                                    │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌──────────┐ │   │
│  │  │  Replicas   │  │  Raft       │  │ Queues   │ │   │
│  │  └─────────────┘  └─────────────┘  └──────────┘ │   │
│  └────────────────────────────────────────────────────┘   │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│               Storage Engine (Pebble)                      │
│          MVCC versioned keys + Intent markers              │
└────────────────────────────────────────────────────────────┘
```

**上游调用者**：
- **Replica 执行层**：在遇到 Intent 冲突时调用 IntentResolver
- **EndTxn 流程**：事务提交/中止时异步清理 Intent
- **MVCC GC Queue**：定期清理过期事务的 Intent

**下游依赖**：
- **DB（kv.DB）**：通过分布式 KV 客户端发送 ResolveIntent RPC
- **RangeDescriptorCache**：查找 Intent 所属的 Range，用于批量优化
- **Stopper**：生命周期管理，确保优雅关闭

### 1.4 核心抽象与生命周期

**核心结构体** (`intentresolver/intent_resolver.go:174-203`):

```go
type IntentResolver struct {
    Metrics Metrics

    // 全局依赖
    clock        *hlc.Clock
    db           *kv.DB        // 发送 ResolveIntent RPC
    stopper      *stop.Stopper
    settings     *cluster.Settings

    // 并发控制
    sem          *quotapool.IntPool  // 限制异步任务数量（默认 1000）

    // 批量处理器（核心优化）
    gcBatcher      *requestbatcher.RequestBatcher  // GC 事务记录
    irBatcher      *requestbatcher.RequestBatcher  // 单点 Intent 解析
    irRangeBatcher *requestbatcher.RequestBatcher  // Range Intent 解析

    mu struct {
        syncutil.Mutex
        // 防止并发 Push 同一事务
        inFlightPushes map[uuid.UUID]int
        // 防止并发清理同一事务的 Intent
        inFlightTxnCleanups map[uuid.UUID]struct{}
    }
}
```

**生命周期**：
1. **创建**：在 `Store.NewStore()` 中通过 `intentresolver.New()` 创建
2. **启动**：通过 `stopper` 管理三个 RequestBatcher 的后台 goroutine
3. **运行**：响应前台请求和异步任务
4. **关闭**：`stopper` 触发优雅关闭，等待所有异步任务完成

---

## 二、第二轮 BFS：控制流与组件协作（How it flows）

### 2.1 主要执行路径

IntentResolver 有两种典型的调用路径：

#### 路径 A：同步 Intent 解析（前台请求驱动）

```
时间线（同步流程，延迟敏感）：

T0: Replica 执行 Get/Put 请求
    │
    ├─ 遇到 Intent 冲突
    │
T1: 调用 intentResolver.ResolveIntent()
    │  (opts.sendImmediately = true，绕过批处理)
    │
T2: 直接构造 ResolveIntentRequest
    │
T3: 通过 db.Run() 发送 RPC
    │  ├─ 到达 Intent 所在的 Replica
    │  ├─ 检查事务状态
    │  └─ 清理 Intent（或等待事务完成）
    │
T4: 返回结果，请求继续执行
```

**关键决策点**：
- **是否需要 Push 事务**？
  - 如果 Intent 的事务已过期 → 先 PushTxn ABORT
  - 如果事务仍活跃 → 根据冲突类型决定（读写冲突 push timestamp，写写冲突 abort）

#### 路径 B：异步 Intent 清理（后台任务）

```
时间线（异步流程，吞吐优先）：

T0: EndTxn 评估完成
    │  事务 Txn123 提交，留下 10 个 Intent
    │
T1: 调用 intentResolver.CleanupTxnIntentsAsync()
    │  ├─ 检查 sem（信号量）容量
    │  └─ 启动异步 goroutine
    │
    ┌──────────────────────────────────────┐
    │  后台 goroutine（与前台请求分离）   │
    ├──────────────────────────────────────┤
    │                                      │
T2: │  lockInFlightTxnCleanup(Txn123)    │
    │   └─ 防止重复清理                   │
    │                                      │
T3: │  resolveIntents()                   │
    │   ├─ 将 10 个 Intent 拆分成请求     │
    │   └─ 提交到 irBatcher/irRangeBatcher│
    │       （批量合并，等待其他请求）     │
    │                                      │
T4: │  批处理器定时触发（10ms 或满 100）  │
    │   └─ 合并多个事务的 Intent          │
    │       发送一个大 Batch RPC           │
    │                                      │
T5: │  gcTxnRecord()                      │
    │   └─ 在新 goroutine 中 GC 事务记录  │
    │       （超时 20s）                   │
    │                                      │
    └──────────────────────────────────────┘
```

**关键优化**：
- **异步化**：前台 EndTxn 立即返回，不等待 Intent 清理
- **批量化**：多个事务的 Intent 请求合并为一个 RPC
- **两阶段清理**：先 resolve intents，再 GC txn record

### 2.2 触发方式分类

| 触发源 | 场景 | 同步/异步 | 关键方法 |
|--------|------|-----------|----------|
| **Replica 冲突** | Get/Put 遇到 Intent | 同步 | `ResolveIntent(sendImmediately=true)` |
| **EndTxn** | 事务提交/中止后 | 异步 | `CleanupTxnIntentsAsync()` |
| **MVCC GC Queue** | 清理过期事务 | 异步 | `CleanupTxnIntentsOnGCAsync()` |
| **不一致读** | Follower Read 遇到 Intent | 异步（可选） | `CleanupIntentsAsync()` |

### 2.3 与其他模块交互

**关键交互点**：

1. **与 TxnCoordSender 协作**：
   - TxnCoordSender 在 EndTxn 时通过 `EndTxnIntents` 告知 IntentResolver
   - IntentResolver 不直接修改事务状态，而是发送 PushTxn 请求

2. **与 Replica 协作**：
   - Replica 遇到 Intent 冲突时调用 `ResolveIntent()`
   - IntentResolver 返回后，Replica 重试原操作

3. **与 RequestBatcher 协作**（核心）：
   ```
   intentResolver.ResolveIntents()
       │
       ├─ 将每个 Intent 转换为 ResolveIntentRequest
       │
       ├─ 按 RangeID 分组（通过 RangeDescriptorCache 查询）
       │
       └─ 提交到 RequestBatcher
           │
           ├─ 等待 MaxWait (10ms) 或达到 MaxMsgsPerBatch (100)
           │
           └─ 合并为单个 Batch 发送
               ├─ DistSender 按 Range 拆分
               └─ 并行发送到各个 Replica
   ```

4. **与 Stopper 协作**：
   - 所有异步任务通过 `stopper.GetHandle()` 获取执行权限
   - 关闭时等待所有 Intent 清理完成（或超时强制退出）

### 2.4 状态流转图

```
┌──────────────────────────────────────────────────────────────┐
│                   Intent 生命周期                            │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  [事务写入]                                                  │
│      │                                                       │
│      ├─ Key 上留下 Intent (TxnMeta + Value)                 │
│      │                                                       │
│      ▼                                                       │
│  ┌─────────────────┐                                        │
│  │  Intent Exists  │◄─────────┐                             │
│  └─────────────────┘          │                             │
│      │                        │                             │
│      │ (其他事务访问)          │ (事务仍在运行)             │
│      ▼                        │                             │
│  ┌─────────────────┐          │                             │
│  │ Conflict Detect │          │                             │
│  └─────────────────┘          │                             │
│      │                        │                             │
│      ├─ 读写冲突？ ──────────►│ PushTxn (PUSH_TIMESTAMP)    │
│      │                        │                             │
│      ├─ 写写冲突？ ──────────►│ PushTxn (PUSH_ABORT)        │
│      │                        │                             │
│      │ (事务已结束)            │                             │
│      ▼                        │                             │
│  ┌─────────────────┐          │                             │
│  │ Resolve Intent  │          │                             │
│  └─────────────────┘          │                             │
│      │                        │                             │
│      ├─ Txn COMMITTED ────► 写入实际值，删除 Intent         │
│      │                                                       │
│      └─ Txn ABORTED ──────► 删除 Intent，可选 Poison cache  │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 Store 初始化中的 IntentResolver 创建

让我们从您提供的代码片段开始分析（`store.go` 中）：

```go
// Create the intent resolver.
var intentResolverRangeCache intentresolver.RangeCache
rngCache := s.cfg.RangeDescriptorCache
if s.cfg.RangeDescriptorCache != nil {
    intentResolverRangeCache = rngCache
}

s.intentResolver = intentresolver.New(intentresolver.Config{
    Clock:                s.cfg.Clock,
    DB:                   s.db,
    Stopper:              stopper,
    Settings:             s.cfg.Settings,
    TaskLimit:            s.cfg.IntentResolverTaskLimit,
    AmbientCtx:           s.cfg.AmbientCtx,
    TestingKnobs:         s.cfg.TestingKnobs.IntentResolverKnobs,
    RangeDescriptorCache: intentResolverRangeCache,
})
```

**分析**：

1. **可选的 RangeCache**：
   ```go
   if s.cfg.RangeDescriptorCache != nil {
       intentResolverRangeCache = rngCache
   }
   ```
   - **为什么可选**？某些测试场景不需要 RangeCache
   - **作用**：允许 IntentResolver 查询 Intent 所在的 RangeID
   - **优化价值**：同一 Range 的 Intent 可以合并在同一个 Batch 中

2. **TaskLimit 的关键作用**：
   ```go
   TaskLimit: s.cfg.IntentResolverTaskLimit,
   ```
   - **默认值**：1000（`defaultTaskLimit`）
   - **语义**：最多允许 1000 个并发异步 goroutine
   - **背压机制**：达到上限时，新的异步任务会：
     - 同步执行（如果 `allowSyncProcessing = true`）
     - 返回错误（如果 `allowSyncProcessing = false`）

### 3.2 IntentResolver 构造函数 `New()`

```go
func New(c Config) *IntentResolver {
    setConfigDefaults(&c)
    ir := &IntentResolver{
        clock:    c.Clock,
        db:       c.DB,
        stopper:  c.Stopper,
        sem:      quotapool.NewIntPool("intent resolver", uint64(c.TaskLimit)),
        // ...
    }

    // 创建三个 RequestBatcher（核心优化）
    ir.gcBatcher = requestbatcher.New(requestbatcher.Config{
        Name:            "intent_resolver_gc_batcher",
        MaxMsgsPerBatch: gcBatchSize,         // 1024
        MaxWait:         c.MaxGCBatchWait,    // 1s
        MaxIdle:         c.MaxGCBatchIdle,    // -1 (disabled)
        Stopper:         c.Stopper,
        Sender:          c.DB.NonTransactionalSender(),
    })

    ir.irBatcher = requestbatcher.New(requestbatcher.Config{
        Name:                   "intent_resolver_ir_batcher",
        MaxMsgsPerBatch:        intentResolverBatchSize,        // 100
        TargetBytesPerBatchReq: intentResolverRequestTargetBytes, // 4MB
        MaxWait:                c.MaxIntentResolutionBatchWait,  // 10ms
        MaxIdle:                c.MaxIntentResolutionBatchIdle,  // 5ms
        Stopper:                c.Stopper,
        Sender:                 c.DB.NonTransactionalSender(),
    })

    ir.irRangeBatcher = requestbatcher.New(requestbatcher.Config{
        Name:                   "intent_resolver_ir_range_batcher",
        MaxMsgsPerBatch:        intentResolverRangeBatchSize,    // 10
        MaxKeysPerBatchReq:     intentResolverRangeRequestSize,  // 200
        TargetBytesPerBatchReq: intentResolverRequestTargetBytes, // 4MB
        MaxWait:                c.MaxIntentResolutionBatchWait,   // 10ms
        Stopper:                c.Stopper,
        Sender:                 c.DB.NonTransactionalSender(),
    })

    return ir
}
```

**核心设计点**：

1. **三种 Batcher 的分工**：
   | Batcher | 用途 | 批量大小 | 等待时间 |
   |---------|------|---------|---------|
   | `gcBatcher` | GC 事务记录 | 1024 | 1s（低优先级） |
   | `irBatcher` | 单点 Intent 解析 | 100 | 10ms（高优先级） |
   | `irRangeBatcher` | Range Intent 解析 | 10 | 10ms（高优先级） |

2. **为什么 Range Batcher 批量小**？
   - 单个 RangeRequest 可能覆盖数百个 Intent
   - 避免单个 Batch 过大导致超时

3. **TargetBytesPerBatchReq 的作用**（4MB）：
   - 防止单个 RPC 过大
   - 触发分页机制（ResumeSpan）

### 3.3 异步任务调度：`runAsyncTask()`

```go
func (ir *IntentResolver) runAsyncTask(
    origCtx context.Context, allowSyncProcessing bool, taskFn func(context.Context),
) error {
    // 1. 检查是否禁用异步
    if ir.testingKnobs.DisableAsyncIntentResolution || asyncIntentResolutionDisabled {
        return errors.New("intents not processed as async resolution is disabled")
    }

    // 2. 尝试获取信号量
    ctx, hdl, err := ir.stopper.GetHandle(
        ir.ambientCtx.AnnotateCtx(context.Background()),
        stop.TaskOpts{
            TaskName:   "storage.IntentResolver: processing intents",
            Sem:        ir.sem,         // 关键：限制并发数
            WaitForSem: false,          // 不阻塞等待
        })

    if err != nil {
        // 3. 达到并发上限
        if errors.Is(err, stop.ErrThrottled) {
            ir.Metrics.IntentResolverAsyncThrottled.Inc(1)
            if allowSyncProcessing {
                // 4. 降级为同步执行
                taskFn(origCtx)
                return nil
            }
        }
        return errors.Wrapf(err, "during async intent resolution")
    }

    // 5. 启动异步 goroutine
    go func(ctx context.Context) {
        defer hdl.Activate(ctx).Release(ctx)  // 释放信号量
        taskFn(ctx)
    }(ctx)
    return nil
}
```

**关键不变量**：
- **INV-1**：任何时刻，活跃的异步 goroutine 数量 ≤ TaskLimit
- **INV-2**：如果 `allowSyncProcessing = true`，任务保证被执行（同步或异步）
- **INV-3**：异步任务使用独立的 `context.Background()`，与调用者的超时隔离

**并发语义**：
- **无锁设计**：`quotapool.IntPool` 内部使用 channel 实现并发安全
- **降级策略**：过载时转为同步执行，保证关键路径不丢失

### 3.4 核心方法：`CleanupTxnIntentsAsync()`

这是 EndTxn 后调用的主要入口：

```go
func (ir *IntentResolver) CleanupTxnIntentsAsync(
    ctx context.Context,
    rangeID roachpb.RangeID,
    endTxns []result.EndTxnIntents,
    allowSyncProcessing bool,
) error {
    for i := range endTxns {
        et := &endTxns[i]

        if err := ir.runAsyncTask(ctx, allowSyncProcessing, func(ctx context.Context) {
            // 1. 锁定事务清理（防止并发重复）
            locked, release := ir.lockInFlightTxnCleanup(ctx, et.Txn.ID)
            if !locked {
                return  // 已有其他 goroutine 在清理
            }
            defer release()

            // 2. 执行清理
            if err := ir.cleanupFinishedTxnIntents(
                ctx,
                kv.AdmissionHeaderForLockUpdateForTxn(et.Txn),
                rangeID,
                et.Txn,
                et.Poison,  // 是否毒化 abort span
                func(err error) {
                    if err != nil {
                        ir.Metrics.FinalizedTxnCleanupFailed.Inc(1)
                    }
                },
            ); err != nil {
                if ir.every.ShouldLog() {
                    log.KvExec.Warningf(ctx, "failed to cleanup transaction intents: %v", err)
                }
            }
        }); err != nil {
            ir.Metrics.FinalizedTxnCleanupFailed.Inc(int64(len(endTxns) - i))
            return err
        }
    }
    return nil
}
```

**关键逻辑**：

1. **锁定机制** `lockInFlightTxnCleanup()`：
   ```go
   func (ir *IntentResolver) lockInFlightTxnCleanup(
       ctx context.Context, txnID uuid.UUID,
   ) (locked bool, release func()) {
       ir.mu.Lock()
       defer ir.mu.Unlock()

       // 检查是否已有清理任务
       _, inFlight := ir.mu.inFlightTxnCleanups[txnID]
       if inFlight {
           log.Eventf(ctx, "skipping txn resolved; already in flight")
           return false, nil
       }

       // 加锁
       ir.mu.inFlightTxnCleanups[txnID] = struct{}{}
       return true, func() {
           ir.mu.Lock()
           delete(ir.mu.inFlightTxnCleanups, txnID)
           ir.mu.Unlock()
       }
   }
   ```
   - **目的**：防止同一事务的 Intent 被多次清理
   - **场景**：EndTxn 和 MVCC GC Queue 可能同时尝试清理

2. **Poison 参数的语义**：
   ```go
   et.Poison  // true 表示需要毒化 abort span
   ```
   - **何时为 true**：事务被 ABORT（主动或被动）
   - **作用**：在 Range 的 abort span 中记录，防止事务协调器重试时读到自己的旧写入

### 3.5 核心方法：`cleanupFinishedTxnIntents()`

```go
func (ir *IntentResolver) cleanupFinishedTxnIntents(
    ctx context.Context,
    admissionHeader kvpb.AdmissionHeader,
    rangeID roachpb.RangeID,
    txn *roachpb.Transaction,
    poison bool,
    onComplete func(error),
) (err error) {
    defer func() {
        if err != nil && onComplete != nil {
            onComplete(err)
        }
    }()

    // 1. 解析所有 Intent
    opts := ResolveOptions{
        Poison:          poison,
        MinTimestamp:    txn.MinTimestamp,  // 优化 Range 查询
        AdmissionHeader: admissionHeader,
    }
    if pErr := ir.resolveIntents(ctx, (*txnLockUpdates)(txn), opts); pErr != nil {
        return errors.Wrapf(pErr.GoError(), "failed to resolve intents")
    }

    // 2. 启动新 goroutine GC 事务记录（与 Intent 解析分离）
    ctx, hdl, err := ir.stopper.GetHandle(
        ir.ambientCtx.AnnotateCtx(context.Background()),
        stop.TaskOpts{
            TaskName: "storage.IntentResolver: cleanup txn records",
        })
    if err != nil {
        return err
    }

    go func(ctx context.Context) {
        defer hdl.Activate(ctx).Release(ctx)

        // 3. GC 事务记录（超时 20s）
        err := timeutil.RunWithTimeout(ctx, "cleanup txn record",
            gcTxnRecordTimeout,  // 20s
            func(ctx context.Context) error {
                return ir.gcTxnRecord(ctx, rangeID, txn)
            })

        if onComplete != nil {
            onComplete(err)
        }

        if err != nil && ir.every.ShouldLog() {
            log.KvExec.Warningf(ctx, "failed to gc transaction record: %v", err)
        }
    }(ctx)
    return nil
}
```

**关键设计点**：

1. **两阶段清理**：
   ```
   Phase 1: resolveIntents() → 清理所有 Range 上的 Intent
               │
               └─ 使用 irBatcher/irRangeBatcher 批量化

   Phase 2: gcTxnRecord() → 删除事务记录（单个 GCRequest）
               │
               └─ 在新 goroutine 中执行，不阻塞 Phase 1
   ```

2. **为什么分离**？
   - Intent 解析是高优先级的（影响并发控制）
   - 事务记录 GC 是低优先级的（纯粹的垃圾回收）
   - 分离后可以更快释放锁（`inFlightTxnCleanups`）

3. **超时保护**：
   ```go
   timeutil.RunWithTimeout(ctx, "cleanup txn record", gcTxnRecordTimeout, ...)
   ```
   - 防止 GC 请求卡住导致 goroutine 泄漏
   - 20 秒超时后放弃，留待下次 GC Queue 处理

### 3.6 核心方法：`MaybePushTransactions()`

这是冲突解决的核心机制：

```go
func (ir *IntentResolver) MaybePushTransactions(
    ctx context.Context,
    pushTxns map[uuid.UUID]*enginepb.TxnMeta,
    h kvpb.Header,
    pushType kvpb.PushTxnType,
    skipIfInFlight bool,
) (_ map[uuid.UUID]*roachpb.Transaction, anyAmbiguousAbort bool, _ *kvpb.Error) {

    // 1. 过滤正在被 Push 的事务（去重）
    ir.mu.Lock()
    for txnID := range pushTxns {
        _, pushTxnInFlight := ir.mu.inFlightPushes[txnID]
        if pushTxnInFlight && skipIfInFlight {
            log.Infof(ctx, "skipping PushTxn for %s; attempt already in flight", txnID)
            delete(pushTxns, txnID)
        } else {
            ir.mu.inFlightPushes[txnID]++  // 引用计数 +1
        }
    }
    ir.mu.Unlock()

    // 2. 构造 PushTxn 请求
    pushTo := h.Timestamp.Next()
    b := &kv.Batch{}
    for _, pushTxn := range pushTxns {
        b.AddRawRequest(&kvpb.PushTxnRequest{
            RequestHeader: kvpb.RequestHeader{Key: pushTxn.Key},
            PusherTxn:     getPusherTxn(h),
            PusheeTxn:     *pushTxn,
            PushTo:        pushTo,
            PushType:      pushType,  // PUSH_TIMESTAMP / PUSH_ABORT / PUSH_TOUCH
        })
    }

    // 3. 发送 Batch
    err := ir.db.Run(ctx, b)

    // 4. 清理引用计数
    ir.mu.Lock()
    for txnID := range pushTxns {
        ir.mu.inFlightPushes[txnID]--
        if ir.mu.inFlightPushes[txnID] == 0 {
            delete(ir.mu.inFlightPushes, txnID)
        }
    }
    ir.mu.Unlock()

    if err != nil {
        return nil, false, b.MustPErr()
    }

    // 5. 返回 Push 后的事务状态
    pushedTxns := make(map[uuid.UUID]*roachpb.Transaction)
    for _, resp := range b.RawResponse().Responses {
        resp := resp.GetInner().(*kvpb.PushTxnResponse)
        pushedTxns[resp.PusheeTxn.ID] = &resp.PusheeTxn
        anyAmbiguousAbort = anyAmbiguousAbort || resp.AmbiguousAbort
    }
    return pushedTxns, anyAmbiguousAbort, nil
}
```

**PushType 的三种模式**：

| PushType | 语义 | 使用场景 |
|----------|------|---------|
| `PUSH_TIMESTAMP` | 将事务时间戳推到更晚 | 读写冲突（读者需要看到更新的版本） |
| `PUSH_ABORT` | 强制中止事务 | 写写冲突或事务超时 |
| `PUSH_TOUCH` | 仅查询事务状态 | 异步清理（不强制中止） |

**并发控制的精妙之处**：
- **引用计数机制**：
  ```go
  ir.mu.inFlightPushes[txnID]++  // 允许多个请求同时 Push
  ```
  - **为什么允许**？Push 是幂等的，多次 Push 不会造成问题
  - **引用计数的作用**：只是记录有多少个请求在等待，不阻塞

- **skipIfInFlight 的优化**：
  ```go
  if pushTxnInFlight && skipIfInFlight {
      delete(pushTxns, txnID)  // 跳过已在 Push 的事务
  }
  ```
  - **适用场景**：异步清理（`PUSH_TOUCH`）
  - **为什么可以跳过**？既然已有人在 Push，就不需要重复工作

### 3.7 核心方法：`resolveIntents()`

```go
func (ir *IntentResolver) resolveIntents(
    ctx context.Context, intents lockUpdates, opts ResolveOptions,
) (pErr *kvpb.Error) {
    if intents.Len() == 0 {
        return nil
    }

    // 1. 构造请求
    var singleReq [1]kvpb.Request  // 栈上分配，避免堆分配
    reqs := resolveIntentReqs(intents, opts, singleReq[:])

    // 2. 选择执行路径
    if opts.sendImmediately {
        // 路径 A：立即发送（延迟敏感）
        b := &kv.Batch{}
        b.AdmissionHeader = opts.AdmissionHeader
        b.AddRawRequest(reqs...)
        if err := ir.db.Run(ctx, b); err != nil {
            return b.MustPErr()
        }
        return nil
    }

    // 路径 B：使用 Batcher（吞吐优先）
    respChan := make(chan requestbatcher.Response, len(reqs))
    for _, req := range reqs {
        var batcher *requestbatcher.RequestBatcher
        switch req.Method() {
        case kvpb.ResolveIntent:
            batcher = ir.irBatcher
        case kvpb.ResolveIntentRange:
            batcher = ir.irRangeBatcher
        }

        // 3. 查询 RangeID（用于批量优化）
        rangeID := ir.lookupRangeID(ctx, req.Header().Key)

        // 4. 提交到 Batcher
        if err := batcher.SendWithChan(ctx, respChan, rangeID, req, opts.AdmissionHeader); err != nil {
            return kvpb.NewError(err)
        }
    }

    // 5. 收集响应
    for range reqs {
        select {
        case resp := <-respChan:
            if resp.Err != nil {
                return kvpb.NewError(resp.Err)
            }
        case <-ctx.Done():
            return kvpb.NewError(ctx.Err())
        }
    }
    return nil
}
```

**关键优化**：

1. **栈上分配**：
   ```go
   var singleReq [1]kvpb.Request  // 在栈上
   reqs := resolveIntentReqs(intents, opts, singleReq[:])
   ```
   - 对于单个 Intent（最常见情况），避免堆分配

2. **RangeID 查询**：
   ```go
   rangeID := ir.lookupRangeID(ctx, req.Header().Key)
   ```
   - 允许 RequestBatcher 将同一 Range 的请求合并
   - 查询失败时返回 0（Best-effort，不影响正确性）

3. **异步收集响应**：
   ```go
   respChan := make(chan requestbatcher.Response, len(reqs))
   ```
   - 有缓冲的 channel，避免 Batcher 阻塞
   - 通过 `select` 支持超时和取消

---

## 四、运行时行为与系统反馈（Runtime Behavior）

### 4.1 负载感知与自适应

IntentResolver 通过多种机制感知运行时负载：

#### 机制 1：信号量背压

```go
sem := quotapool.NewIntPool("intent resolver", uint64(c.TaskLimit))
```

**运行时行为**：
```
正常负载（< 1000 并发）：
    ├─ 异步任务立即启动
    └─ Metrics.IntentResolverAsyncThrottled = 0

高负载（≥ 1000 并发）：
    ├─ 新任务被拒绝（ErrThrottled）
    ├─ Metrics.IntentResolverAsyncThrottled++
    └─ 决策分支：
        ├─ allowSyncProcessing = true → 降级为同步执行（阻塞调用者）
        └─ allowSyncProcessing = false → 返回错误（放弃清理）
```

**系统反馈**：
- **即时反馈**：`GetHandle()` 立即返回是否可以启动异步任务
- **局部决策**：每个 Store 独立限流，不需要全局协调

#### 机制 2：批处理延迟控制

RequestBatcher 根据两个维度触发发送：
```go
MaxWait: 10ms   // 最大等待时间（延迟上限）
MaxIdle: 5ms    // 空闲等待时间（吞吐优化）
```

**自适应行为**：
```
低 QPS 场景：
    ├─ 请求到达后等待 5ms（MaxIdle）
    ├─ 如果没有更多请求 → 立即发送（避免无谓等待）
    └─ 平均延迟 ≈ 5-10ms

高 QPS 场景：
    ├─ 请求快速填满 Batch（达到 MaxMsgsPerBatch = 100）
    ├─ 立即发送，不等待 MaxWait
    └─ 平均延迟 ≈ 1-2ms（批量效率高）
```

**为什么不是固定延迟**？
- 固定延迟在低 QPS 时浪费时间
- 自适应延迟在高 QPS 时提高吞吐，低 QPS 时降低延迟

### 4.2 故障处理与降级

#### 场景 1：Intent 解析超时

```go
MaxTimeout: intentResolverSendBatchTimeout,  // 1 分钟
```

**运行时行为**：
```
T0: Batch 发送（100 个 Intent）
    │
T0+60s: 超时触发
    │
    ├─ RequestBatcher 返回错误
    ├─ Metrics.IntentResolutionFailed += 100
    └─ 记录警告日志

T1: 下次访问这些 Intent 时
    │
    └─ 前台请求遇到 Intent → 同步解析（绕过 Batcher）
```

**为什么可以容忍失败**？
- Intent 清理是 **Best-effort**，不影响正确性
- 失败的 Intent 会在下次访问时被解决（惰性清理）

#### 场景 2：事务记录 GC 失败

```go
err := timeutil.RunWithTimeout(ctx, "cleanup txn record",
    gcTxnRecordTimeout,  // 20s
    func(ctx context.Context) error {
        return ir.gcTxnRecord(ctx, rangeID, txn)
    })
```

**运行时行为**：
```
T0: Intent 解析完成
    │
T1: 启动 gcTxnRecord() goroutine
    │
T1+20s: 超时
    │
    ├─ onComplete(err) 被调用
    ├─ Metrics.FinalizedTxnCleanupFailed++
    └─ 事务记录仍然存在

T2: MVCC GC Queue 下次扫描时
    │
    └─ 重新触发 CleanupTxnIntentsOnGCAsync()
```

**为什么可以容忍**？
- 事务记录是不可变的（已 Finalized）
- 留在磁盘上不影响正确性，只是浪费空间
- MVCC GC Queue 会定期重试

### 4.3 并发冲突的处理

#### 场景：多个请求同时解析同一个事务的 Intent

```
时间线：

T0: Replica A 遇到 Txn123 的 Intent
    │
    ├─ 调用 ResolveIntent(key1, Txn123)
    │  └─ 提交到 irBatcher
    │
T1: Replica B 遇到 Txn123 的 Intent
    │
    ├─ 调用 ResolveIntent(key2, Txn123)
    │  └─ 提交到 irBatcher
    │
T2: Batcher 触发（10ms 后）
    │
    ├─ 合并为单个 Batch：
    │   ├─ ResolveIntentRequest(key1, Txn123)
    │   └─ ResolveIntentRequest(key2, Txn123)
    │
    └─ 发送到各自的 Range
        ├─ Range 1 解析 key1
        └─ Range 2 解析 key2
```

**关键点**：
- **不冲突**：不同 Intent 的解析是独立的
- **批量优化**：同一事务的多个 Intent 可以合并在一个 Batch 中

#### 场景：EndTxn 和 MVCC GC 同时清理同一个事务

```
时间线：

T0: EndTxn 触发 CleanupTxnIntentsAsync(Txn123)
    │
    ├─ lockInFlightTxnCleanup(Txn123) → 成功
    │  └─ inFlightTxnCleanups[Txn123] = struct{}{}
    │
T1: MVCC GC 触发 CleanupTxnIntentsOnGCAsync(Txn123)
    │
    ├─ lockInFlightTxnCleanup(Txn123) → 失败（已加锁）
    │  └─ 返回 locked=false，跳过
    │
T2: EndTxn 的 goroutine 完成清理
    │
    └─ release() → delete(inFlightTxnCleanups, Txn123)
```

**保证**：
- **幂等性**：同一事务的 Intent 只被清理一次
- **无浪费**：后来的请求直接跳过，不消耗资源

### 4.4 系统级反馈循环

```
┌─────────────────────────────────────────────────────────────┐
│            Intent 清理的负反馈循环                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  [高事务提交率]                                             │
│       │                                                     │
│       ├─ 产生大量 Intent                                    │
│       │                                                     │
│       ▼                                                     │
│  [IntentResolver 过载]                                      │
│       │                                                     │
│       ├─ sem 达到上限 (1000)                                │
│       ├─ 异步任务转同步执行                                 │
│       │                                                     │
│       ▼                                                     │
│  [EndTxn 延迟增加]                                          │
│       │                                                     │
│       ├─ 事务协调器阻塞在 EndTxn                             │
│       │                                                     │
│       ▼                                                     │
│  [事务提交率下降] ◄───────┐                                 │
│       │                   │                                 │
│       └───────────────────┘ (负反馈)                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**为什么这种设计是稳定的**？
- **自限流**：过载时同步执行，自然降低吞吐
- **不丢失**：即使失败，Intent 会在下次访问时被解决
- **优先级保证**：前台请求始终优先（sendImmediately）

---

## 五、设计模式分析（Design Patterns）

IntentResolver 中运用了多种经典和演化的设计模式：

### 5.1 异步任务池模式（Async Task Pool Pattern）

**模式描述**：
- 使用信号量限制并发异步任务数量
- 支持同步降级（overflow handling）

**代码体现**：
```go
type IntentResolver struct {
    sem *quotapool.IntPool  // 信号量：最多 1000 个并发任务
}

func (ir *IntentResolver) runAsyncTask(...) error {
    ctx, hdl, err := ir.stopper.GetHandle(..., stop.TaskOpts{
        Sem:        ir.sem,
        WaitForSem: false,  // 非阻塞获取
    })

    if err != nil {
        if errors.Is(err, stop.ErrThrottled) {
            // 降级为同步执行
            taskFn(origCtx)
            return nil
        }
    }

    go func(ctx context.Context) {
        defer hdl.Release(ctx)
        taskFn(ctx)
    }(ctx)
}
```

**为什么选择这种模式**？
- ✅ **防止 goroutine 泄漏**：上限 1000，不会无限创建
- ✅ **保证进度**：降级机制确保任务不会被丢弃
- ✅ **弹性**：正常时异步，过载时同步

**事实标准地位**：
- 类似于 Java 的 `ThreadPoolExecutor` + `RejectedExecutionHandler`
- Kubernetes Workqueue 也采用类似模式

### 5.2 请求批处理器模式（Request Batching Pattern）

**模式描述**：
- 将多个小请求合并为一个大请求
- 使用时间窗口和大小阈值触发

**代码体现**：
```go
type RequestBatcher struct {
    MaxMsgsPerBatch: 100,   // 大小阈值
    MaxWait:         10ms,  // 时间上限
    MaxIdle:         5ms,   // 空闲超时
}

// 内部逻辑（简化）：
func (rb *RequestBatcher) worker() {
    var batch []Request
    timer := time.NewTimer(MaxIdle)

    for {
        select {
        case req := <-rb.reqChan:
            batch = append(batch, req)
            if len(batch) >= MaxMsgsPerBatch {
                rb.sendBatch(batch)
                batch = batch[:0]
            }
            timer.Reset(MaxWait)

        case <-timer.C:
            if len(batch) > 0 {
                rb.sendBatch(batch)
                batch = batch[:0]
            }
        }
    }
}
```

**为什么选择这种模式**？
- ✅ **吞吐量优化**：单个 RPC 可以携带 100 个 Intent
- ✅ **延迟可控**：MaxWait 保证不会无限等待
- ✅ **自适应**：高 QPS 时快速填满，低 QPS 时快速发送

**事实标准地位**：
- Kafka Producer 的 `linger.ms` + `batch.size`
- gRPC 的 Client-side Batching
- 数据库连接池的 Statement Batching

### 5.3 两阶段清理模式（Two-Phase Cleanup Pattern）

**模式描述**：
- 将清理任务分为高优先级和低优先级两个阶段
- 高优先级阻塞后续工作，低优先级可以延迟

**代码体现**：
```go
func (ir *IntentResolver) cleanupFinishedTxnIntents(...) error {
    // Phase 1: 解析 Intent（高优先级）
    if pErr := ir.resolveIntents(ctx, (*txnLockUpdates)(txn), opts); pErr != nil {
        return pErr
    }

    // Phase 2: GC 事务记录（低优先级，异步）
    go func() {
        timeutil.RunWithTimeout(ctx, "cleanup txn record", 20*time.Second,
            func(ctx context.Context) error {
                return ir.gcTxnRecord(ctx, rangeID, txn)
            })
    }()
    return nil
}
```

**为什么选择这种模式**？
- ✅ **优先级分离**：Intent 解析是性能关键路径，事务记录 GC 不是
- ✅ **快速释放锁**：不等待 GC 完成就释放 `inFlightTxnCleanups`
- ✅ **容错性**：GC 失败不影响 Intent 解析的成功

**事实标准地位**：
- MySQL 的 InnoDB Purge Thread（分离 undo log 清理）
- PostgreSQL 的 VACUUM（延迟空间回收）
- Linux 内核的 RCU（分离读取和回收）

### 5.4 引用计数去重模式（Reference Counting Deduplication Pattern）

**模式描述**：
- 允许多个请求同时等待同一个操作
- 使用引用计数避免重复执行

**代码体现**：
```go
type IntentResolver struct {
    mu struct {
        inFlightPushes map[uuid.UUID]int  // TxnID → 引用计数
    }
}

func (ir *IntentResolver) MaybePushTransactions(...) {
    ir.mu.Lock()
    for txnID := range pushTxns {
        if _, inFlight := ir.mu.inFlightPushes[txnID]; inFlight && skipIfInFlight {
            delete(pushTxns, txnID)  // 跳过
        } else {
            ir.mu.inFlightPushes[txnID]++  // 引用计数 +1
        }
    }
    ir.mu.Unlock()

    // ... 执行 Push ...

    ir.mu.Lock()
    for txnID := range pushTxns {
        ir.mu.inFlightPushes[txnID]--
        if ir.mu.inFlightPushes[txnID] == 0 {
            delete(ir.mu.inFlightPushes, txnID)
        }
    }
    ir.mu.Unlock()
}
```

**为什么选择这种模式**？
- ✅ **允许并发**：多个请求可以同时 Push（幂等操作）
- ✅ **可选去重**：`skipIfInFlight` 用于不需要等待结果的场景
- ✅ **细粒度控制**：比简单的互斥锁更灵活

**与经典模式的对比**：
| 模式 | 并发度 | 适用场景 |
|------|--------|---------|
| **互斥锁** | 串行执行 | 非幂等操作 |
| **Singleflight** | 合并请求 | 需要共享结果的幂等操作 |
| **引用计数** | 完全并发 | 幂等操作 + 可选去重 |

**事实标准地位**：
- C++ 的 `std::shared_ptr`
- Python 的引用计数 GC
- 文件系统的 inode 引用计数

### 5.5 观察者模式的隐式应用（Implicit Observer Pattern）

**模式描述**：
- `onComplete` 回调函数作为观察者
- 清理完成时通知调用者

**代码体现**：
```go
func (ir *IntentResolver) CleanupTxnIntentsAsync(
    ...
    endTxns []result.EndTxnIntents,
    ...
) error {
    for i := range endTxns {
        onComplete := func(err error) {
            if err != nil {
                ir.Metrics.FinalizedTxnCleanupFailed.Inc(1)
            }
        }

        ir.runAsyncTask(..., func(ctx context.Context) {
            err := ir.cleanupFinishedTxnIntents(..., onComplete)
            // ...
        })
    }
}
```

**为什么选择这种模式**？
- ✅ **解耦**：调用者不需要轮询状态
- ✅ **灵活**：可以传递不同的 `onComplete` 逻辑
- ✅ **异步友好**：适合长时间运行的任务

**事实标准地位**：
- JavaScript 的 Promise/Callback
- Java 的 CompletableFuture
- Reactor 模式的事件通知

### 5.6 策略模式：ResolveOptions

**模式描述**：
- 通过 `ResolveOptions` 参数化解析行为
- 不同场景使用不同策略

**代码体现**：
```go
type ResolveOptions struct {
    Poison          bool         // 是否毒化 abort span
    MinTimestamp    hlc.Timestamp // 优化 Range 查询
    sendImmediately bool         // 是否绕过批处理
}

// 场景 1：前台冲突解析
opts := ResolveOptions{sendImmediately: true}

// 场景 2：后台异步清理
opts := ResolveOptions{Poison: true, sendImmediately: false}
```

**为什么选择这种模式**？
- ✅ **灵活性**：同一个方法支持多种场景
- ✅ **可扩展**：新增选项不影响现有调用
- ✅ **类型安全**：比 `map[string]interface{}` 更安全

### 5.7 刻意避免的模式：全局锁

**IntentResolver 刻意避免使用全局锁**，而是采用细粒度锁：

```go
// ❌ 不使用全局锁
type IntentResolver struct {
    mu sync.Mutex  // 保护所有状态
}

// ✅ 使用细粒度锁
type IntentResolver struct {
    mu struct {
        syncutil.Mutex
        inFlightPushes      map[uuid.UUID]int
        inFlightTxnCleanups map[uuid.UUID]struct{}
    }
    sem *quotapool.IntPool  // 独立的并发控制
}
```

**为什么避免**？
- ❌ 全局锁会阻塞所有并发请求
- ✅ 细粒度锁只阻塞冲突的事务
- ✅ 信号量控制资源，不阻塞逻辑

---

## 六、具体运行示例（Concrete Example）

让我们通过一个完整的例子理解 IntentResolver 的运行。

### 6.1 场景设置

**系统状态**：
- 3 个 Store 节点：Store1, Store2, Store3
- Range R1 在 Store1, Store2, Store3 上有副本
- 事务 Txn-Alice 在 3 个 Range 上写入：
  - R1: `/users/alice` (Store1 是 leaseholder)
  - R2: `/orders/123` (Store2 是 leaseholder)
  - R3: `/payments/456` (Store3 是 leaseholder)

**时间线**：

```
T0: 事务 Txn-Alice 开始
    TxnID: abc-123
    Coordinator: Store1

T1: 写入 3 个 key
    ├─ /users/alice → Intent (Txn=abc-123, Value="Alice Updated")
    ├─ /orders/123 → Intent (Txn=abc-123, Value="Order Created")
    └─ /payments/456 → Intent (Txn=abc-123, Value="Payment Pending")

T2: Coordinator 发送 EndTxn (COMMIT)
    └─ Store1 的 Replica R1 评估 EndTxn
        ├─ 写入事务记录：status=COMMITTED
        ├─ 应用 Raft log entry
        └─ 返回结果给 Coordinator

T3: EndTxn 评估完成后，触发异步清理
    Store1.intentResolver.CleanupTxnIntentsAsync()
```

### 6.2 异步清理流程（详细步骤）

**Step 1: 启动异步任务**

```go
// Store1 上的代码
endTxns := []result.EndTxnIntents{
    {
        Txn: &roachpb.Transaction{
            ID:     abc-123,
            Status: COMMITTED,
            LockSpans: []roachpb.Span{
                {Key: "/users/alice"},
                {Key: "/orders/123"},
                {Key: "/payments/456"},
            },
        },
        Poison: false,  // COMMIT 不需要 poison
    },
}

ir.CleanupTxnIntentsAsync(ctx, rangeID=1, endTxns, allowSync=true)
```

**Step 2: 获取信号量**

```
T3+1ms: runAsyncTask() 调用
    ├─ ir.sem.Acquire() → 成功（当前并发：234/1000）
    └─ 启动 goroutine G1
```

**Step 3: 锁定事务清理**

```
Goroutine G1:
    lockInFlightTxnCleanup(abc-123)
        ├─ ir.mu.Lock()
        ├─ 检查 inFlightTxnCleanups[abc-123] → 不存在
        ├─ inFlightTxnCleanups[abc-123] = struct{}{}
        └─ ir.mu.Unlock()

    返回 locked=true, release=func()
```

**Step 4: 解析 Intent（批量优化）**

```
resolveIntents(ctx, (*txnLockUpdates)(txn), opts)
    │
    ├─ 转换为 3 个 ResolveIntentRequest:
    │   ├─ Req1: Key="/users/alice", IntentTxn=abc-123, Status=COMMITTED
    │   ├─ Req2: Key="/orders/123", IntentTxn=abc-123, Status=COMMITTED
    │   └─ Req3: Key="/payments/456", IntentTxn=abc-123, Status=COMMITTED
    │
    ├─ opts.sendImmediately = false → 使用 Batcher
    │
    └─ 提交到 irBatcher:
        ├─ lookupRangeID("/users/alice") → RangeID=1
        ├─ batcher.SendWithChan(ctx, respChan, 1, Req1, ...)
        ├─ lookupRangeID("/orders/123") → RangeID=2
        ├─ batcher.SendWithChan(ctx, respChan, 2, Req2, ...)
        ├─ lookupRangeID("/payments/456") → RangeID=3
        └─ batcher.SendWithChan(ctx, respChan, 3, Req3, ...)
```

**Step 5: Batcher 批量处理**

```
T3+5ms: 假设此时还有其他事务的 Intent 请求

irBatcher 内部状态:
    pendingRequests = {
        RangeID=1: [Req1, Req4, Req7],   // 3 个请求
        RangeID=2: [Req2, Req5],         // 2 个请求
        RangeID=3: [Req3, Req6, Req8, Req9], // 4 个请求
    }

T3+10ms: MaxWait 触发
    │
    ├─ 为每个 RangeID 构造一个 Batch:
    │   ├─ Batch1 (to Range1): [Req1, Req4, Req7]
    │   ├─ Batch2 (to Range2): [Req2, Req5]
    │   └─ Batch3 (to Range3): [Req3, Req6, Req8, Req9]
    │
    └─ 并行发送 3 个 RPC:
        ├─ db.NonTransactionalSender().Send(Batch1) → Store1
        ├─ db.NonTransactionalSender().Send(Batch2) → Store2
        └─ db.NonTransactionalSender().Send(Batch3) → Store3
```

**Step 6: Range 端执行**

```
Store1 (Range1 leaseholder):
    收到 Batch1 = [Req1, Req4, Req7]
    │
    ├─ 对于 Req1 (Key="/users/alice", Txn=abc-123):
    │   ├─ 查找 MVCC key: /users/alice@<ts>
    │   ├─ 检查 Intent: TxnID=abc-123, Status=PENDING
    │   ├─ 更新为 COMMITTED:
    │   │   ├─ 删除 Intent metadata
    │   │   └─ 保留实际值: "Alice Updated"
    │   └─ 返回成功
    │
    ├─ 对于 Req4, Req7: ...（类似处理）
    │
    └─ 返回 BatchResponse

Store2, Store3: 类似处理
```

**Step 7: 收集响应**

```
Goroutine G1:
    for i := 0; i < 3; i++ {
        resp := <-respChan
        // resp.Err == nil
    }

    // 所有 Intent 解析成功
```

**Step 8: GC 事务记录**

```
cleanupFinishedTxnIntents() 继续执行:
    │
    ├─ Intent 解析完成，返回 nil
    │
    └─ 启动新 goroutine G2:
        go func() {
            gcTxnRecord(ctx, rangeID=1, txn=abc-123)
                │
                ├─ 构造 GCRequest:
                │   Key: /Local/Transaction/abc-123
                │
                ├─ 提交到 gcBatcher:
                │   batcher.Send(ctx, rangeID=1, &gcRequest)
                │
                └─ gcBatcher 延迟 1s 后发送:
                    ├─ 合并其他事务的 GC 请求
                    └─ 发送到 Store1
        }()
```

**Step 9: 清理完成**

```
T3+15ms: Goroutine G1 释放锁
    release()
        ├─ ir.mu.Lock()
        ├─ delete(inFlightTxnCleanups, abc-123)
        └─ ir.mu.Unlock()

    ir.sem.Release()  // 释放信号量 (233/1000)

T4+1s: Goroutine G2 完成 GC
    └─ 事务记录从 Store1 的 Pebble 中删除
```

### 6.3 性能数据

**资源消耗**：
```
时间：
├─ 异步任务启动：< 1ms
├─ 批处理等待：10ms (MaxWait)
├─ Intent 解析 RPC：5-20ms (取决于网络)
└─ 事务记录 GC：1s+ (低优先级)

并发：
├─ 信号量占用：1 个槽位（234 → 235 → 234）
├─ Goroutine：2 个（G1 + G2）
└─ RPC：3 个并行（到 3 个 Store）

吞吐优化：
├─ 批处理合并：9 个请求 → 3 个 RPC（减少 66%）
└─ 并行执行：3 个 RPC 同时进行
```

### 6.4 故障场景：Store2 宕机

假设在 T3+8ms 时 Store2 宕机：

```
T3+8ms: Store2 宕机
    │
T3+10ms: Batcher 发送 Batch2 到 Store2
    │
    ├─ RPC 超时（1 分钟后）
    │
T4+10ms: 超时触发
    │
    ├─ Batch2 返回错误
    ├─ respChan <- Response{Err: "context deadline exceeded"}
    │
    └─ Goroutine G1 收到错误:
        ├─ ir.Metrics.IntentResolutionFailed += 2  // Req2, Req5
        ├─ 返回错误
        └─ onComplete(err) → Metrics.FinalizedTxnCleanupFailed++

后续处理：
    ├─ /orders/123 的 Intent 仍然存在
    └─ 下次有请求访问该 key 时:
        ├─ 遇到 Intent (Txn=abc-123, Status=PENDING)
        ├─ 查询事务记录 → Status=COMMITTED
        ├─ 同步解析 Intent (sendImmediately=true)
        └─ 请求继续执行
```

**关键点**：
- ✅ 故障不影响已成功的 Intent 解析（R1, R3）
- ✅ 失败的 Intent 在下次访问时自动修复（惰性清理）
- ✅ 系统保持可用性，不会因为单点故障卡住

---

## 七、设计取舍与替代方案（Trade-offs）

### 7.1 异步 vs 同步

**当前设计：异步优先 + 同步降级**

```go
ir.runAsyncTask(ctx, allowSyncProcessing=true, taskFn)
```

| 维度 | 异步设计 | 纯同步设计 |
|------|---------|-----------|
| **EndTxn 延迟** | 低（立即返回） | 高（等待所有 Intent 解析） |
| **吞吐量** | 高（批量化） | 低（每个事务单独发送） |
| **资源使用** | 需要 goroutine 池 | 无额外资源 |
| **故障影响** | Intent 清理失败不影响事务提交 | Intent 清理失败导致事务失败 |
| **复杂度** | 高（需要管理异步任务） | 低（串行逻辑） |

**为什么选择异步**？
- ✅ **性能关键**：EndTxn 是事务的关键路径，延迟直接影响 TPS
- ✅ **批量优化**：异步可以合并多个事务的 Intent
- ✅ **容错性**：Intent 清理失败可以延迟修复

**代价**：
- ❌ 需要信号量限制并发
- ❌ 需要处理异步失败场景
- ❌ 调试复杂度增加

### 7.2 批量化 vs 即时发送

**当前设计：可选批量化**

```go
if opts.sendImmediately {
    // 立即发送
} else {
    // 提交到 Batcher
}
```

| 维度 | 批量化 | 即时发送 |
|------|--------|---------|
| **延迟** | 高（+10ms MaxWait） | 低（无等待） |
| **吞吐量** | 高（减少 RPC 数量） | 低（每个 Intent 单独 RPC） |
| **公平性** | 可能饥饿（低 QPS key） | 公平 |
| **网络负载** | 低（合并请求） | 高（大量小 RPC） |

**为什么混合设计**？
- ✅ **前台请求优先**：`sendImmediately=true` 保证低延迟
- ✅ **后台批量化**：异步清理可以容忍延迟
- ✅ **自适应**：根据场景选择策略

**替代方案：纯批量化**
- ❌ 前台请求延迟增加 10ms（不可接受）
- ❌ 需要更复杂的优先级机制

### 7.3 信号量上限（1000）的权衡

**当前设计：固定上限 1000**

```go
defaultTaskLimit = 1000
```

| 上限值 | 优点 | 缺点 |
|-------|------|------|
| **100** | 资源占用少 | 高 TPS 时频繁触发背压 |
| **1000（当前）** | 平衡 | 极端场景下仍可能饥饿 |
| **10000** | 几乎不触发背压 | 内存占用大，goroutine 过多 |
| **无限** | 无背压 | 可能 OOM，goroutine 泄漏 |

**为什么选择 1000**？
- ✅ **实践经验**：大多数场景下足够
- ✅ **可配置**：通过环境变量调整
- ✅ **安全性**：避免 goroutine 爆炸

**动态调整的可能性**：
```go
// 理想设计（未实现）
func (ir *IntentResolver) adjustSemCapacity() {
    qps := ir.metrics.TxnCommitsPerSecond.Rate()
    if qps > 10000 {
        ir.sem.SetCapacity(2000)  // 动态扩容
    }
}
```
- ❌ 增加复杂度
- ❌ 难以确定合理的调整策略
- ✅ 固定值更容易推理和调试

### 7.4 两阶段清理 vs 一体化清理

**当前设计：分离 Intent 解析和事务记录 GC**

```go
// Phase 1: Intent 解析
ir.resolveIntents(...)

// Phase 2: 事务记录 GC（新 goroutine）
go func() {
    ir.gcTxnRecord(...)
}()
```

| 设计 | 优点 | 缺点 |
|------|------|------|
| **分离（当前）** | Intent 解析快速完成，不阻塞 | 可能 GC 失败，留下垃圾 |
| **一体化** | 保证完整清理 | 延长 Intent 解析时间 |

**为什么分离**？
- ✅ **性能关键**：Intent 解析影响并发控制
- ✅ **容错性**：GC 失败不影响功能
- ✅ **优先级**：GC 是低优先级任务

**代价**：
- ❌ 可能留下"孤儿"事务记录
- ❌ 需要 MVCC GC Queue 定期清理

### 7.5 引用计数 vs Singleflight

**当前设计：引用计数 + 可选跳过**

```go
ir.mu.inFlightPushes[txnID]++  // 允许并发
```

**替代方案：Singleflight（合并请求）**

```go
// 伪代码
result, err := ir.singleflight.Do(txnID, func() (interface{}, error) {
    return ir.pushTxn(txnID)
})
```

| 方案 | 并发度 | 适用场景 |
|------|--------|---------|
| **引用计数（当前）** | 完全并发 | Push 是幂等的，无需共享结果 |
| **Singleflight** | 串行化 | 需要共享结果，避免重复计算 |

**为什么选择引用计数**？
- ✅ **并发性**：多个 Push 可以并行执行
- ✅ **灵活性**：`skipIfInFlight` 支持不等待的场景
- ✅ **简单性**：不需要管理结果共享

**Singleflight 的问题**：
- ❌ Push 本身可以并行，强制串行化降低吞吐
- ❌ 需要管理结果的生命周期

### 7.6 集中式 vs 分布式清理

**当前设计：分布式（每个 Store 独立）**

```
Store1: IntentResolver1
Store2: IntentResolver2
Store3: IntentResolver3
```

**替代方案：集中式清理器**

```
Cluster: 单个 IntentResolver Master
    └─ 所有 Intent 清理请求发送到 Master
```

| 设计 | 优点 | 缺点 |
|------|------|------|
| **分布式（当前）** | 无单点，可扩展 | 可能重复清理同一事务 |
| **集中式** | 全局去重 | 单点瓶颈，扩展性差 |

**为什么选择分布式**？
- ✅ **可扩展性**：随 Store 数量线性扩展
- ✅ **容错性**：单个 Store 故障不影响其他 Store
- ✅ **局部性**：Intent 清理就近执行

**重复清理的代价**：
- ✅ **幂等性**：重复清理不影响正确性
- ✅ **去重机制**：`lockInFlightTxnCleanup` 避免同一 Store 内重复

---

## 八、总结与心智模型（Mental Model）

### 8.1 核心思想总结

IntentResolver 是一个 **异步、批量、分布式的垃圾回收器**，专门用于清理分布式事务系统中的"意图"（Intent）标记。它的核心价值在于：

1. **解耦性能与正确性**：
   - 事务可以立即提交，Intent 清理异步进行
   - 清理失败不影响事务的正确性（惰性修复）

2. **批量优化**：
   - 通过 RequestBatcher 将多个事务的 Intent 合并为一个 RPC
   - 在延迟和吞吐之间动态平衡

3. **并发控制**：
   - 信号量限制异步任务数量，防止过载
   - 引用计数允许并发 Push，细粒度锁避免阻塞

4. **优先级分离**：
   - 前台请求使用 `sendImmediately`，绕过批处理
   - 后台任务使用批量化，优化吞吐
   - Intent 解析优先于事务记录 GC

### 8.2 心智模型

**你可以把 IntentResolver 理解为：**

> 一个**异步的、带背压控制的、批量优化的垃圾回收器**，负责清理分布式事务留下的"占位符"。
>
> 它像一个**清洁工团队**：
> - **前台清洁工**（同步路径）：立即清理阻塞通道的垃圾（`sendImmediately`）
> - **后台清洁工**（异步路径）：定期巡视，批量清理散落的垃圾（Batcher）
> - **团队规模限制**：最多 1000 个清洁工同时工作（信号量）
> - **去重机制**：避免多个清洁工清理同一片区域（`lockInFlightTxnCleanup`）
> - **优先级**：先清理阻塞通道的垃圾（Intent），再清理垃圾桶（事务记录）

### 8.3 关键不变量

在理解和修改代码时，务必保持以下不变量：

**INV-1**: 任何时刻，活跃的异步 goroutine 数量 ≤ TaskLimit
```go
ir.sem.Acquire() → 保证
```

**INV-2**: 同一事务的 Intent 清理任务只执行一次（同一 Store 内）
```go
ir.mu.inFlightTxnCleanups[txnID] → 保证
```

**INV-3**: Intent 解析是幂等的，多次解析同一 Intent 不影响正确性
```go
ResolveIntentRequest → 幂等性保证
```

**INV-4**: 事务记录 GC 失败不影响 Intent 解析的成功
```go
ir.resolveIntents() + go ir.gcTxnRecord() → 分离保证
```

**INV-5**: 前台请求的 Intent 解析优先于后台批量化
```go
opts.sendImmediately → 优先级保证
```

### 8.4 扩展性考虑

如果需要修改或扩展 IntentResolver，以下是关键考虑点：

1. **增加新的清理策略**：
   - 扩展 `ResolveOptions` 添加新字段
   - 在 `resolveIntents()` 中处理新策略

2. **优化批量化延迟**：
   - 调整 `MaxWait` / `MaxIdle` 参数
   - 考虑动态调整（基于 QPS）

3. **增加清理优先级**：
   - 引入优先级队列（类似 Raft Scheduler 的 Priority Shard）
   - 高优先级 Intent 优先处理

4. **监控与可观测性**：
   - 已有指标：`IntentResolverAsyncThrottled`, `FinalizedTxnCleanupFailed`
   - 可新增：批量化效率、Intent 清理延迟分布

### 8.5 与其他系统的对比

| 系统 | Intent 清理机制 | 差异点 |
|------|----------------|--------|
| **CockroachDB** | IntentResolver（本文） | 分布式、异步、批量化 |
| **TiDB** | GC Worker | 集中式 GC，定期扫描 MVCC 版本 |
| **PostgreSQL** | VACUUM | 后台进程，清理 dead tuples |
| **MySQL InnoDB** | Purge Thread | 后台线程，清理 undo log |

**CockroachDB 的独特之处**：
- ✅ **即时清理**：事务提交后立即触发（不等待定期扫描）
- ✅ **分布式**：每个 Store 独立清理，无单点
- ✅ **批量优化**：跨事务合并清理请求

---

## 九、参考资料与延伸阅读

### 9.1 相关代码文件

- **核心实现**：`pkg/kv/kvserver/intentresolver/intent_resolver.go`
- **Store 集成**：`pkg/kv/kvserver/store.go`（NewStore 和 Start）
- **批处理器**：`pkg/internal/client/requestbatcher/batcher.go`
- **事务协调**：`pkg/kv/kvclient/kvcoord/txn_coord_sender.go`

### 9.2 相关概念

- **MVCC Intent**：CockroachDB 的未提交写入标记
- **PushTxn**：事务冲突解决机制
- **Abort Span**：防止已中止事务的重复读取
- **RequestBatcher**：请求批量化框架

### 9.3 论文与设计文档

- CockroachDB 事务系统设计：https://www.cockroachlabs.com/docs/stable/architecture/transaction-layer.html
- MVCC 与并发控制：https://www.cockroachlabs.com/docs/stable/architecture/storage-layer.html

### 9.4 调试与可观测性

**关键日志**：
```go
log.Eventf(ctx, "resolving %d intents", intents.Len())
log.Eventf(ctx, "skipping txn resolved; already in flight")
```

**关键指标**：
```go
ir.Metrics.IntentResolverAsyncThrottled  // 异步任务被限流次数
ir.Metrics.FinalizedTxnCleanupFailed     // 事务清理失败次数
ir.Metrics.IntentResolutionFailed        // Intent 解析失败次数
```

**调试技巧**：
- 设置 `COCKROACH_ASYNC_INTENT_RESOLUTION_DISABLED=true` 禁用异步清理
- 调整 `COCKROACH_ASYNC_INTENT_RESOLVER_TASK_LIMIT` 观察背压行为
- 使用 `log.V(1)` 查看详细日志

---

## 总结

IntentResolver 是 CockroachDB 分布式事务系统中的关键组件，通过 **异步化、批量化、分布式** 的设计，实现了高性能的 Intent 清理。它的核心价值在于：

1. **解耦事务提交与清理**：提高事务吞吐量
2. **批量优化**：减少网络开销
3. **并发控制**：防止过载和资源泄漏
4. **容错性**：清理失败不影响系统正确性

在 Store 初始化过程中，通过 `intentresolver.New()` 创建的 IntentResolver 实例，配备了三个 RequestBatcher（GC、单点 Intent、Range Intent），并通过信号量限制并发数量。这种设计在延迟、吞吐、可靠性之间取得了良好的平衡，是分布式事务系统中垃圾回收机制的典范实现。

---

**文档版本**: v1.0
**作者**: 基于 CockroachDB 源码分析
**最后更新**: 2026-02-03
