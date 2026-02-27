# 第十三章：GrantCoordinator详解——跨WorkKind资源协调的指挥中心

## 引言

在前面的章节中，我们深入学习了准入控制系统的各个组件：
- **WorkQueue**：负责排队和公平调度
- **Granter**：负责资源授予（slots/tokens）
- **Epoch 机制**：实现 LIFO 排序优化

但这些组件是如何协同工作的？谁负责统一管理多个 WorkKind（KVWork、SQLKVResponseWork、SQLSQLResponseWork）？谁决定授权的顺序和优先级？

答案就是 **GrantCoordinator**——准入控制系统的**指挥中心**，它负责跨 WorkKind 的资源协调，实现优先级控制，并通过 **Grant Chain（授权链）** 机制优化性能。

本章将以 **BFS → DFS** 的方式，从宏观到微观，全面剖析 GrantCoordinator 的设计哲学、实现细节和协调机制。

---

## BFS 层次 1：GrantCoordinator 的概念模型

### 1.1 什么是 GrantCoordinator？

**GrantCoordinator 是准入控制系统的协调器**，它管理多个 WorkKind 的授权流程，确保：

1. **跨 WorkKind 优先级控制**：KVWork > SQLKVResponseWork > SQLSQLResponseWork
2. **资源统一管理**：协调 CPU slots 和 tokens 的分配
3. **Grant Chain 优化**：批量授权以减少上下文切换
4. **过载保护**：在共享资源（CPU）过载时终止低优先级工作

```
准入控制架构（单节点 CPU 资源）：

┌─────────────────────────────────────────────────────┐
│                 GrantCoordinator                     │
│  (CPU 资源的统一协调器)                               │
├─────────────────────────────────────────────────────┤
│                                                       │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  │
│  │  KVWork     │  │ SQLKVResp... │  │ SQLSQLResp │  │
│  │  (slots)    │  │ (tokens)     │  │ (tokens)   │  │
│  ├─────────────┤  ├──────────────┤  ├────────────┤  │
│  │ slotGranter │  │tokenGranter  │  │tokenGranter│  │
│  │ ↕           │  │ ↕            │  │ ↕          │  │
│  │ WorkQueue   │  │ WorkQueue    │  │ WorkQueue  │  │
│  └─────────────┘  └──────────────┘  └────────────┘  │
│         ↑                ↑                  ↑         │
│         └────────────────┴──────────────────┘         │
│              Grant Chain 机制                         │
└─────────────────────────────────────────────────────┘
```

### 1.2 GrantCoordinator 解决什么问题？

#### 问题 1：跨 WorkKind 优先级混乱

**没有 GrantCoordinator 的情况**：

```
场景：3 个独立的 WorkQueue，各自授权

KVWork Queue：
├─► 有 5 个等待请求
└─► slots 可用 → 授权

SQLKVResponseWork Queue：
├─► 有 10 个等待请求
└─► tokens 可用 → 授权

SQLSQLResponseWork Queue：
├─► 有 20 个等待请求
└─► tokens 可用 → 授权

问题：
- 3 个 WorkKind 同时授权，没有优先级控制
- CPU 被三种工作平均分配
- 高优先级的 KVWork 无法得到优先处理
```

**有 GrantCoordinator 的情况**：

```
场景：GrantCoordinator 统一协调授权

GrantCoordinator.tryGrantLocked():
├─► WorkKind 0 (KVWork)：尝试授权
│   ├─► 有等待请求 → 授权
│   ├─► 有等待请求 → 授权
│   └─► 队列为空 or slots 耗尽 → 继续
│
├─► WorkKind 1 (SQLKVResponseWork)：尝试授权
│   ├─► 有等待请求 → 授权
│   └─► 队列为空 or tokens 耗尽 → 继续
│
└─► WorkKind 2 (SQLSQLResponseWork)：尝试授权
    └─► 有等待请求 → 授权

结果：
- 严格按照 WorkKind 顺序授权
- KVWork 优先得到 slots
- 只有当 KVWork 队列为空或资源耗尽时，才授权 SQL 工作
```

#### 问题 2：上下文切换开销大

**朴素的授权方式**：

```
每次有资源返回时：

返回 1 个 slot：
├─► 加锁
├─► 从 KVWork queue 出队 1 个请求
├─► 授权
├─► 解锁
└─► 唤醒 1 个 goroutine

问题：
- 每次授权都需要加锁/解锁
- 每次授权都唤醒 1 个 goroutine
- 如果有 100 个等待请求，需要 100 次上下文切换
```

**Grant Chain 优化**：

```
批量授权（Grant Chain）：

返回 1 个 slot 后触发：
├─► 加锁
├─► 授权 numProcs * multiplier 个请求（例如 40 个）
├─► 最后一个被授权的请求获得 grantChainID
├─► 解锁
└─► 唤醒 40 个 goroutines

最后一个 goroutine 运行时：
├─► 调用 continueGrantChain(grantChainID)
├─► 触发下一轮批量授权
└─► 链式继续，直到队列为空或资源耗尽

优势：
- 减少加锁次数：100 次 → 3 次（假设每次授权 40 个）
- 批量唤醒 goroutines，减少调度开销
- 利用 goroutine 调度器的自然节流（natural throttling）
```

#### 问题 3：共享资源过载无法快速响应

**问题场景**：

```
时刻 T0：CPU 正常
├─► GrantCoordinator 正在批量授权 SQLSQLResponseWork
├─► Grant Chain 还有 30 个请求待授权
└─► 一切正常

时刻 T1：突然大量 KVWork 到达
├─► KVWork 队列积压 100 个请求
├─► 但 Grant Chain 还在授权 SQLSQLResponseWork
├─► 高优先级的 KVWork 被阻塞

时刻 T2：CPU 过载
├─► kvSlotAdjuster 检测到 CPU 过载
├─► 但 Grant Chain 继续授权低优先级工作
└─► 雪崩风险

问题：
Grant Chain 一旦启动，无法中途终止，导致：
- 高优先级工作等待时间过长
- CPU 过载时继续授权，加剧过载
```

**GrantCoordinator 的解决方案**：

```go
// pkg/util/admission/grant_coordinator.go:335
func (coord *GrantCoordinator) tryGet(
    workKind WorkKind, count int64, demuxHandle int8,
) (granted bool) {
    coord.mu.Lock()
    defer coord.mu.Unlock()

    res := coord.granters[workKind].tryGetLocked(count, demuxHandle)
    switch res {
    case grantSuccess:
        return true
    case grantFailDueToSharedResource:
        // CPU 过载！终止 Grant Chain
        if coord.mu.grantChainActive && coord.mu.grantChainIndex >= workKind {
            coord.tryTerminateGrantChain()
        }
        return false
    case grantFailLocal:
        return false
    }
}
```

**效果**：

```
时刻 T2：CPU 过载检测
├─► KVWork 的 tryGet() 返回 grantFailDueToSharedResource
├─► 检测到 Grant Chain 正在运行
├─► 立即终止 Grant Chain
└─► 停止低优先级工作的授权

时刻 T3：重新开始授权
├─► 从 KVWork 开始
├─► 优先处理高优先级工作
└─► 避免雪崩
```

### 1.3 GrantCoordinator 的三层架构

```
┌─────────────────────────────────────────────────────┐
│              第一层：GrantCoordinators               │
│  (顶层容器，管理多个 GrantCoordinator 实例)           │
├─────────────────────────────────────────────────────┤
│                                                       │
│  ┌─────────────────┐  ┌──────────────────────────┐  │
│  │ RegularCPU      │  │ Stores                   │  │
│  │ (regular work)  │  │ (每个 Store 一个         │  │
│  │                 │  │  GrantCoordinator)       │  │
│  └─────────────────┘  └──────────────────────────┘  │
│  ┌─────────────────┐                                 │
│  │ ElasticCPU      │                                 │
│  │ (elastic work)  │                                 │
│  └─────────────────┘                                 │
└─────────────────────────────────────────────────────┘
           ↓                              ↓
┌─────────────────────────┐  ┌─────────────────────────┐
│   第二层：GrantCoordinator│  │  StoreGrantCoordinators │
│   (单个协调器实例)        │  │  (管理多个 Store)        │
├─────────────────────────┤  ├─────────────────────────┤
│ - granters[numWorkKinds]│  │ - 每个 Store 有独立的   │
│ - queues[numWorkKinds]  │  │   GrantCoordinator      │
│ - Grant Chain 状态      │  │ - IOLoadListener        │
│ - CPU Load Listener     │  │ - Per-Store metrics     │
└─────────────────────────┘  └─────────────────────────┘
           ↓
┌─────────────────────────────────────────────────────┐
│        第三层：Granter + WorkQueue 对                │
│        (每个 WorkKind 一对)                          │
├─────────────────────────────────────────────────────┤
│                                                       │
│  ┌──────────────┐          ┌──────────────┐          │
│  │ slotGranter  │◄────────►│ WorkQueue    │          │
│  │ (KVWork)     │          │ (KVWork)     │          │
│  └──────────────┘          └──────────────┘          │
│                                                       │
│  ┌──────────────┐          ┌──────────────┐          │
│  │ tokenGranter │◄────────►│ WorkQueue    │          │
│  │(SQLKVResp..) │          │(SQLKVResp..) │          │
│  └──────────────┘          └──────────────┘          │
│                                                       │
│  ┌──────────────┐          ┌──────────────┐          │
│  │ tokenGranter │◄────────►│ WorkQueue    │          │
│  │(SQLSQLResp..)│          │(SQLSQLResp..)│          │
│  └──────────────┘          └──────────────┘          │
└─────────────────────────────────────────────────────┘
```

---

## BFS 层次 2：GrantCoordinator 的核心组件

### 2.1 数据结构

```go
// pkg/util/admission/grant_coordinator.go:46
type GrantCoordinator struct {
    ambientCtx log.AmbientContext
    settings   *cluster.Settings

    mu struct {
        syncutil.Mutex

        // ===== Grant Chain 状态 =====
        grantChainActive bool      // Grant Chain 是否活跃
        grantChainID     grantChainID  // 当前或下一个 Chain ID
        grantChainIndex  WorkKind   // 当前处理的 WorkKind 索引
        grantChainStartTime time.Time  // Chain 开始时间

        // ===== CPU 负载监听 =====
        cpuOverloadIndicator cpuOverloadIndicator  // CPU 过载指示器
        cpuLoadListener      CPULoadListener       // CPU 负载监听器
        numProcs             int                   // GOMAXPROCS
    }

    lastCPULoadSamplePeriod time.Duration  // 上次 CPU 采样周期

    // ===== Granter-Requester 对 =====
    granters [numWorkKinds]granterWithLockedCalls  // 资源授予器数组
    queues   [numWorkKinds]requesterClose         // WorkQueue 数组

    // ===== 配置 =====
    useGrantChains bool  // 是否启用 Grant Chain
    testingDisableSkipEnforcement bool
    knobs *TestingKnobs
}
```

**字段详解**：

| 字段 | 类型 | 作用 |
|-----|------|------|
| `grantChainActive` | `bool` | Grant Chain 是否正在运行 |
| `grantChainID` | `grantChainID` | Chain 的唯一标识，用于检测过期 Chain |
| `grantChainIndex` | `WorkKind` | 当前 Chain 处理到哪个 WorkKind |
| `grantChainStartTime` | `time.Time` | Chain 开始时间，用于防止过早终止 |
| `cpuOverloadIndicator` | `cpuOverloadIndicator` | 检测 CPU 是否过载 |
| `cpuLoadListener` | `CPULoadListener` | 接收 CPU 负载通知 |
| `numProcs` | `int` | 用于计算 grant burst limit |
| `granters` | `[numWorkKinds]...` | 每个 WorkKind 的 Granter |
| `queues` | `[numWorkKinds]...` | 每个 WorkKind 的 WorkQueue |
| `useGrantChains` | `bool` | 是否启用 Grant Chain 优化 |

### 2.2 核心接口

#### 接口 1：tryGet（快速路径）

```go
// pkg/util/admission/grant_coordinator.go:335
func (coord *GrantCoordinator) tryGet(
    workKind WorkKind, count int64, demuxHandle int8,
) (granted bool) {
    coord.mu.Lock()
    defer coord.mu.Unlock()

    // 尝试获取资源
    res := coord.granters[workKind].tryGetLocked(count, demuxHandle)

    switch res {
    case grantSuccess:
        // 授权成功
        return true

    case grantFailDueToSharedResource:
        // CPU 过载，终止 Grant Chain
        if coord.mu.grantChainActive && coord.mu.grantChainIndex >= workKind {
            coord.tryTerminateGrantChain()
        }
        return false

    case grantFailLocal:
        // 本地资源不足（slots/tokens 耗尽）
        return false
    }
}
```

**调用链**：

```
Requester (WorkQueue):
    ├─► tryGet() 快速路径
    │   └─► coord.tryGet(workKind, count, demuxHandle)
    │       ├─► coord.mu.Lock()
    │       ├─► granter.tryGetLocked(count, demuxHandle)
    │       │   ├─► slotGranter: 检查 usedSlots < totalSlots
    │       │   └─► tokenGranter: 检查 availableBurstTokens > 0
    │       ├─► 根据 grantResult 决定是否终止 Grant Chain
    │       └─► coord.mu.Unlock()
    └─► 如果失败 → 慢路径（入队）
```

#### 接口 2：returnGrant（归还资源）

```go
// pkg/util/admission/grant_coordinator.go:367
func (coord *GrantCoordinator) returnGrant(
    workKind WorkKind, count int64, demuxHandle int8,
) {
    coord.mu.Lock()
    defer coord.mu.Unlock()

    // 归还资源
    coord.granters[workKind].returnGrantLocked(count, demuxHandle)

    if coord.mu.grantChainActive {
        // 检查是否需要终止当前 Grant Chain
        if coord.mu.grantChainIndex > workKind &&
            coord.granters[workKind].requesterHasWaitingRequests() {
            // 有高优先级工作在等待，但 Grant Chain 在处理低优先级工作
            // 终止 Chain，重新开始
            if !coord.tryTerminateGrantChain() {
                return
            }
        } else {
            // Grant Chain 会处理这个 WorkKind，或者没有等待请求
            return
        }
    }

    // 尝试授权等待的请求
    coord.tryGrantLocked()
}
```

**决策逻辑**：

```
returnGrant 被调用：

Grant Chain 活跃？
├─► No → 直接调用 tryGrantLocked() 尝试授权
│
└─► Yes → 检查 Grant Chain 的位置
    ├─► grantChainIndex <= workKind
    │   └─► Grant Chain 会处理这个 WorkKind → 不做任何事
    │
    └─► grantChainIndex > workKind
        ├─► 该 WorkKind 有等待请求？
        │   ├─► Yes → 终止 Grant Chain，重新授权
        │   └─► No → 不做任何事
        │
        └─► 终止成功？
            ├─► Yes → 调用 tryGrantLocked()
            └─► No → 返回（等待 Grant Chain 自然结束）
```

#### 接口 3：continueGrantChain（延续授权链）

```go
// pkg/util/admission/grant_coordinator.go:399
func (coord *GrantCoordinator) continueGrantChain(
    _ WorkKind, grantChainID grantChainID,
) {
    if grantChainID == noGrantChain {
        return
    }

    coord.mu.Lock()
    defer coord.mu.Unlock()

    // 检查 Grant Chain 是否已被终止
    if coord.mu.grantChainID != grantChainID {
        // Chain 已被终止（ID 不匹配）
        return
    }

    // 继续授权
    coord.tryGrantLocked()
}
```

**Grant Chain 的生命周期**：

```
1. 启动 Grant Chain：
   ├─► returnGrant() or tryGet() 失败后
   ├─► coord.mu.grantChainActive = false
   └─► 调用 tryGrantLocked()
       ├─► 授权 grantBurstLimit 个请求
       ├─► 最后一个请求获得 grantChainID
       ├─► coord.mu.grantChainActive = true
       └─► return

2. 延续 Grant Chain：
   ├─► 最后一个被授权的 goroutine 运行
   ├─► 调用 continueGrantChain(grantChainID)
   └─► 检查 grantChainID 是否匹配
       ├─► 匹配 → 调用 tryGrantLocked() 继续授权
       └─► 不匹配 → Chain 已被终止，退出

3. 终止 Grant Chain：
   ├─► tryGet() 检测到 CPU 过载
   ├─► returnGrant() 检测到高优先级工作等待
   ├─► 调用 tryTerminateGrantChain()
   │   ├─► 检查是否可以终止（启动时间 < 100ms？）
   │   ├─► coord.mu.grantChainID++  ← 关键！让老 Chain 失效
   │   └─► coord.mu.grantChainActive = false
   └─► 下次 continueGrantChain() 检测到 ID 不匹配，退出
```

### 2.3 Grant Chain 机制详解

#### Grant Chain 是什么？

**Grant Chain（授权链）** 是一种批量授权优化机制：

1. **批量授权**：一次授权多个请求（默认 `numProcs * multiplier` 个）
2. **链式延续**：最后一个被授权的请求负责触发下一轮授权
3. **自然节流**：利用 goroutine 调度器的能力，避免过度授权

#### 为什么需要 Grant Chain？

**没有 Grant Chain 的问题**：

```
场景：每次只授权 1 个请求

Time: 0ms
├─► 返回 1 个 slot
├─► 授权 1 个 KVWork 请求
└─► 唤醒 goroutine A

Time: 0.1ms
├─► 返回 1 个 slot
├─► 授权 1 个 KVWork 请求
└─► 唤醒 goroutine B

Time: 0.2ms
├─► 返回 1 个 slot
├─► 授权 1 个 KVWork 请求
└─► 唤醒 goroutine C

...（重复 100 次）

开销：
- 100 次加锁/解锁
- 100 次 goroutine 唤醒
- 100 次上下文切换
- 总耗时：10ms+
```

**有 Grant Chain 的优势**：

```
场景：批量授权 40 个请求

Time: 0ms
├─► 返回 1 个 slot
├─► 批量授权 40 个 KVWork 请求
│   ├─► 请求 1-39: 普通授权
│   └─► 请求 40: 获得 grantChainID
├─► 唤醒 40 个 goroutines
└─► 总耗时：1ms

Time: 1ms
├─► Goroutine 40 运行
├─► 调用 continueGrantChain(grantChainID)
├─► 批量授权下一批 40 个请求
└─► ...

开销减少：
- 加锁次数：100 次 → 3 次
- 上下文切换开销减少 60%+
- 总耗时：10ms+ → 3ms
```

#### Grant Chain 的实现细节

**grantBurstLimit 计算**：

```go
// pkg/util/admission/grant_coordinator.go:463
grantBurstCount := 0
grantBurstLimit := coord.mu.numProcs  // 默认 = GOMAXPROCS
multiplier := int(KVSlotAdjusterOverloadThreshold.Get(&coord.settings.SV) / 4)
if multiplier == 0 {
    multiplier = 1
}
grantBurstLimit *= multiplier
```

**示例计算**：

```
假设：
- numProcs = 8（8 核 CPU）
- KVSlotAdjusterOverloadThreshold = 32
- multiplier = 32 / 4 = 8

grantBurstLimit = 8 * 8 = 64

解释：
- 每次授权最多 64 个请求
- 目标是为每个 CPU 核心产生足够的 runnable goroutines
- multiplier 确保有足够的 burst，提高 CPU 利用率
- 但不能过大，否则低优先级工作会挤占 KVWork 的 slots
```

**授权循环**：

```go
// pkg/util/admission/grant_coordinator.go:480
OuterLoop:
for ; coord.mu.grantChainIndex < numWorkKinds; coord.mu.grantChainIndex++ {
    granter := coord.granters[coord.mu.grantChainIndex]
    if granter == nil {
        continue  // 这个 WorkKind 未配置
    }

    for granter.requesterHasWaitingRequests() && !localDone {
        chainID := noGrantChain
        if grantBurstCount+1 == grantBurstLimit && coord.useGrantChains {
            // 达到 burst limit，启动 Grant Chain
            chainID = coord.mu.grantChainID
        }

        res := granter.tryGrantLocked(chainID)
        switch res {
        case grantSuccess:
            grantBurstCount++
            if grantBurstCount == grantBurstLimit && coord.useGrantChains {
                // 已经授权了 burst limit 个请求，启动 Grant Chain
                coord.mu.grantChainActive = true
                if startingChain {
                    coord.mu.grantChainStartTime = timeutil.Now()
                }
                return  // 退出，等待 continueGrantChain()
            }
        case grantFailDueToSharedResource:
            // CPU 过载，停止所有授权
            break OuterLoop
        case grantFailLocal:
            // 本地资源不足，继续下一个 WorkKind
            localDone = true
        }
    }
}

// Grant Chain 未启动或已结束，增加 Chain ID
if !startingChain {
    coord.mu.grantChainID++
}
```

**流程图**：

```
tryGrantLocked() 开始：

grantChainActive?
├─► No → startingChain = true, grantChainIndex = 0
└─► Yes → startingChain = false（延续现有 Chain）

grantBurstCount = 0
grantBurstLimit = numProcs * multiplier

循环授权：
for workKind in [KVWork, SQLKVResponseWork, SQLSQLResponseWork]:
    ├─► granter 存在？
    │   └─► No → 跳过
    │
    ├─► 有等待请求？
    │   └─► No → 继续下一个 WorkKind
    │
    └─► 循环授权：
        ├─► grantBurstCount + 1 == grantBurstLimit?
        │   ├─► Yes → chainID = grantChainID（最后一个请求）
        │   └─► No → chainID = noGrantChain
        │
        ├─► res = granter.tryGrantLocked(chainID)
        │
        └─► switch res:
            ├─► grantSuccess:
            │   ├─► grantBurstCount++
            │   └─► grantBurstCount == grantBurstLimit?
            │       ├─► Yes (且 useGrantChains):
            │       │   ├─► grantChainActive = true
            │       │   ├─► 记录 grantChainStartTime
            │       │   └─► return（等待 continueGrantChain）
            │       └─► No → 继续授权
            │
            ├─► grantFailDueToSharedResource:
            │   └─► break OuterLoop（停止所有授权）
            │
            └─► grantFailLocal:
                └─► 继续下一个 WorkKind

所有 WorkKind 处理完毕：
├─► grantChainActive = false（Chain 未启动或已结束）
└─► 如果是延续的 Chain：grantChainID++
```

---

## DFS 层次 3：关键机制深度剖析

### 3.1 Grant Chain 终止机制

#### 为什么需要终止 Grant Chain？

**问题场景**：

```
时刻 T0：启动 Grant Chain
├─► 授权 64 个 SQLSQLResponseWork 请求
├─► grantChainActive = true
├─► grantChainIndex = 2 (SQLSQLResponseWork)
└─► grantChainID = 100

时刻 T1：大量 KVWork 请求到达
├─► KVWork queue 积压 200 个请求
├─► 但 Grant Chain 还在处理 SQLSQLResponseWork
└─► 高优先级工作被阻塞

时刻 T2：CPU 开始过载
├─► kvSlotAdjuster.isOverloaded() = true
├─► 但 Grant Chain 继续授权低优先级工作
└─► 雪崩风险增加

如果不终止 Grant Chain：
- 需要等待所有 64 个 SQLSQLResponseWork 完成
- 高优先级 KVWork 等待时间过长
- CPU 过载加剧，系统崩溃
```

#### 终止机制实现

**tryTerminateGrantChain**：

```go
// pkg/util/admission/grant_coordinator.go:433
func (coord *GrantCoordinator) tryTerminateGrantChain() bool {
    now := timeutil.Now()

    // 检查是否可以终止
    if delayForGrantChainTermination > 0 &&
        now.Sub(coord.mu.grantChainStartTime) < delayForGrantChainTermination {
        // Grant Chain 刚启动不久（< 100ms），暂时不终止
        return false
    }

    // 终止 Grant Chain
    coord.mu.grantChainID++         // 增加 ID，让老 Chain 失效
    coord.mu.grantChainActive = false
    coord.mu.grantChainStartTime = time.Time{}
    return true
}
```

**为什么要延迟 100ms？**

```go
// pkg/util/admission/grant_coordinator.go:412
// delayForGrantChainTermination causes a delay in terminating a grant chain.
// Terminating a grant chain immediately typically causes a new one to start
// immediately that can burst up to its maximum initial grant burst. Which
// means frequent terminations followed by new starts impose little control
// over the rate at which tokens are granted (slots are better controlled
// since we know when the work finishes). This causes huge spikes in the
// runnable goroutine count, observed at 1ms granularity. This spike causes
// the kvSlotAdjuster to ratchet down the totalSlots for KV work all the way
// down to 1, which later causes the runnable gorouting count to crash down
// to a value close to 0, leading to under-utilization.

翻译：
立即终止 Grant Chain 通常会导致立即启动新的 Chain，新 Chain 可以 burst
到最大初始授权量。这意味着频繁的终止和启动对令牌授予速率控制很少。
这导致 runnable goroutine 数量出现巨大尖峰（以 1ms 粒度观察）。这个尖峰
导致 kvSlotAdjuster 将 KV 工作的 totalSlots 一路降到 1，后来导致 runnable
goroutine 数量崩溃到接近 0，导致利用率不足。
```

**延迟终止的权衡**：

```
不延迟（立即终止）：
├─► 优点：快速响应高优先级工作
└─► 缺点：
    ├─► 频繁启动/终止 Grant Chain
    ├─► runnable count 剧烈波动
    ├─► kvSlotAdjuster 误判，将 slots 降到 1
    └─► CPU 利用率不足

延迟 100ms：
├─► 优点：
│   ├─► 减少 Grant Chain 启动/终止频率
│   ├─► runnable count 更稳定
│   └─► kvSlotAdjuster 更准确
└─► 缺点：
    ├─► 高优先级工作可能延迟 100ms
    └─► 可接受，因为 100ms << 典型超时（5s+）

设计选择：
优先保证系统稳定性和 CPU 利用率，而不是绝对的优先级响应速度
```

#### 终止触发条件

**条件 1：CPU 过载**

```go
// pkg/util/admission/grant_coordinator.go:351
case grantFailDueToSharedResource:
    // CPU 过载
    if coord.mu.grantChainActive && coord.mu.grantChainIndex >= workKind {
        coord.tryTerminateGrantChain()
    }
    return false
```

**触发场景**：

```
场景：KVWork 的 tryGet() 失败

slotGranter.tryGetLocked():
├─► usedSlots >= totalSlots
├─► workKind == KVWork
└─► return grantFailDueToSharedResource

GrantCoordinator.tryGet():
├─► 收到 grantFailDueToSharedResource
├─► grantChainActive = true
├─► grantChainIndex >= KVWork
└─► 终止 Grant Chain

原因：
- KVWork 是最高优先级
- 如果连 KVWork 都无法获取资源，说明 CPU 真的过载了
- 必须停止低优先级工作的授权
```

**条件 2：高优先级工作等待**

```go
// pkg/util/admission/grant_coordinator.go:372
if coord.mu.grantChainActive {
    if coord.mu.grantChainIndex > workKind &&
        coord.granters[workKind].requesterHasWaitingRequests() {
        // 有高优先级工作等待，终止 Grant Chain
        if !coord.tryTerminateGrantChain() {
            return
        }
    } else {
        return
    }
}
```

**触发场景**：

```
场景：归还 KVWork slot 时

returnGrant(KVWork, 1):
├─► returnGrantLocked(KVWork, 1)
│   └─► usedSlots--
│
├─► grantChainActive = true
├─► grantChainIndex = 2 (SQLSQLResponseWork)
│   > workKind = 0 (KVWork)
│
├─► KVWork 有等待请求？
│   └─► Yes
│
└─► 终止 Grant Chain，重新从 KVWork 开始授权

原因：
- Grant Chain 正在处理低优先级工作（SQLSQLResponseWork）
- 但高优先级工作（KVWork）有等待请求
- 应该优先处理 KVWork
```

### 3.2 WorkKind 优先级控制

#### 严格的优先级顺序

```go
// pkg/util/admission/admission.go:559
const (
    KVWork WorkKind = iota              // 优先级 0 (最高)
    SQLKVResponseWork                   // 优先级 1
    SQLSQLResponseWork                  // 优先级 2 (最低)
)
```

**为什么这样排序？**

```
设计原则（来自 admission.go 的注释）：

1. KVWork > SQLKVResponseWork > SQLSQLResponseWork

2. KVWork 最高优先级的原因：
   ├─► 包含非 SQL 的 KV 层工作（node liveness, range splits, etc.）
   ├─► 防止 KV 层工作被 SQL 层饿死
   └─► 是整个系统的基础层

3. SQLKVResponseWork > SQLSQLResponseWork 的原因：
   ├─► SQLKVResponseWork 包含 DistSQL 叶子节点处理
   ├─► 尽早释放 RPC 树底层节点的内存
   └─► 减少分布式查询的总延迟

4. 延迟 SQLSQLResponseWork 的好处：
   ├─► 自然的背压机制
   ├─► 减少新工作的发起速率
   └─► 避免雪崩

示例：单节点 OLAP vs OLTP

时刻 T0：OLAP 查询占用所有 KVWork slots
├─► OLTP 查询到达，在 KVWork 队列中等待
└─► OLAP 的 KVWork 完成

时刻 T1：OLAP 尝试执行 SQLKVResponseWork
├─► 但 OLTP 的 KVWork 开始执行，占用 CPU
├─► OLAP 的 SQLKVResponseWork 被阻塞
└─► 等待 OLTP 的 KVWork 完成

时刻 T2：OLTP 的 KVWork 完成，执行 SQLKVResponseWork
├─► OLAP 和 OLTP 的 SQLKVResponseWork 竞争
├─► WorkQueue 优先处理高优先级的 OLTP 请求
└─► OLAP 查询被进一步延迟

结果：
- OLTP 查询快速完成 ✓
- OLAP 查询被自然降级 ✓
- 无需显式的 query priority 设置 ✓
```

#### 授权顺序实现

```go
// pkg/util/admission/grant_coordinator.go:480
for ; coord.mu.grantChainIndex < numWorkKinds; coord.mu.grantChainIndex++ {
    granter := coord.granters[coord.mu.grantChainIndex]
    if granter == nil {
        continue
    }

    // 尝试授权当前 WorkKind
    for granter.requesterHasWaitingRequests() && !localDone {
        res := granter.tryGrantLocked(chainID)
        switch res {
        case grantSuccess:
            // 成功授权，继续
            grantBurstCount++
        case grantFailDueToSharedResource:
            // CPU 过载，停止所有授权
            break OuterLoop
        case grantFailLocal:
            // 本地资源不足，继续下一个 WorkKind
            localDone = true
        }
    }
}
```

**授权流程可视化**：

```
假设状态：
- KVWork: 5 个等待，10 个 slots 可用
- SQLKVResponseWork: 20 个等待，1000 个 tokens 可用
- SQLSQLResponseWork: 50 个等待，1000 个 tokens 可用
- grantBurstLimit = 15

授权过程：

Step 1：grantChainIndex = 0 (KVWork)
├─► 授权 KVWork 请求 #1 → success (grantBurstCount = 1)
├─► 授权 KVWork 请求 #2 → success (grantBurstCount = 2)
├─► 授权 KVWork 请求 #3 → success (grantBurstCount = 3)
├─► 授权 KVWork 请求 #4 → success (grantBurstCount = 4)
├─► 授权 KVWork 请求 #5 → success (grantBurstCount = 5)
└─► 队列为空 → grantChainIndex++

Step 2：grantChainIndex = 1 (SQLKVResponseWork)
├─► 授权 请求 #1 → success (grantBurstCount = 6)
├─► 授权 请求 #2 → success (grantBurstCount = 7)
├─► ...
├─► 授权 请求 #10 → success (grantBurstCount = 15)
└─► grantBurstCount == grantBurstLimit → 启动 Grant Chain

Grant Chain 启动：
├─► grantChainActive = true
├─► 最后一个请求（#10）获得 grantChainID = 100
├─► return（等待 continueGrantChain）

延续 Grant Chain：
├─► 请求 #10 的 goroutine 运行
├─► 调用 continueGrantChain(100)
├─► 继续从 grantChainIndex = 1 开始授权
│   ├─► 授权 请求 #11-#25 (15 个)
│   └─► 继续...
└─► 直到队列为空或资源耗尽

最终结果：
- KVWork 的所有 5 个请求都被授权 ✓
- SQLKVResponseWork 授权了 20 个请求 ✓
- SQLSQLResponseWork 可能一个都没授权（如果前面耗尽了 burst）
```

### 3.3 CPU 负载监听与槽位调整

#### CPULoad 接口

```go
// pkg/util/admission/grant_coordinator.go:292
func (coord *GrantCoordinator) CPULoad(
    runnable int, procs int, samplePeriod time.Duration,
) {
    ctx := coord.ambientCtx.AnnotateCtx(context.Background())
    coord.lastCPULoadSamplePeriod = samplePeriod

    // 传递给 CPU Load Listener（通常是 kvSlotAdjuster）
    coord.mu.Lock()
    coord.mu.numProcs = procs  // 更新 GOMAXPROCS
    if coord.mu.cpuLoadListener != nil {
        coord.mu.cpuLoadListener.CPULoad(runnable, procs, samplePeriod)
    }
    coord.mu.Unlock()

    // 刷新 token granters（周期性补充 tokens）
    for i := range coord.granters {
        if coord.granters[i] != nil {
            switch tg := coord.granters[i].(type) {
            case *tokenGranter:
                tg.refillBurstTokens(skipTokenEnforcement)
            }
        }
    }

    // 尝试授权等待的请求
    coord.mu.Lock()
    defer coord.mu.Unlock()
    coord.tryGrantLocked()
}
```

**调用链**：

```
外部组件（如 goschedstats）：
    └─► coord.CPULoad(runnable, procs, samplePeriod)
        ├─► 更新 coord.mu.numProcs
        ├─► 调用 kvSlotAdjuster.CPULoad(...)
        │   ├─► 计算 CPU 利用率
        │   ├─► 调整 totalSlots
        │   └─► 更新 cpuOverloadIndicator
        ├─► 刷新所有 tokenGranter 的 availableBurstTokens
        └─► 调用 tryGrantLocked() 尝试授权
```

**kvSlotAdjuster 的作用**：

```
kvSlotAdjuster 实现两个接口：
1. cpuOverloadIndicator：提供 isOverloaded() 方法
2. CPULoadListener：接收 CPULoad 通知，调整 slots

工作流程：

CPULoad(runnable, procs, samplePeriod):
├─► 计算 CPU 利用率 = runnable / procs
├─► 利用率 > 阈值？
│   ├─► Yes → 减少 totalSlots
│   └─► No → 增加 totalSlots
├─► 更新 isOverloaded 状态
└─► 通知 slotGranter 更新 totalSlots

slotGranter 收到通知：
├─► 更新 totalSlots
├─► 如果从耗尽状态恢复 → 调用 tryGrantLocked()
└─► 尝试授权等待的请求

tokenGranter 收到通知：
├─► refillBurstTokens()
├─► availableBurstTokens = maxBurstTokens
└─► 尝试授权等待的请求
```

---

## DFS 层次 4：多层协调器架构

### 4.1 GrantCoordinators 容器

```go
// pkg/util/admission/grant_coordinator.go:23
type GrantCoordinators struct {
    RegularCPU *GrantCoordinator           // Regular work 的 CPU 协调器
    ElasticCPU *ElasticCPUGrantCoordinator // Elastic work 的 CPU 协调器
    Stores     *StoreGrantCoordinators     // 每个 Store 的协调器
}
```

**三个协调器的分工**：

```
┌─────────────────────────────────────────────────────┐
│             GrantCoordinators (容器)                 │
├─────────────────────────────────────────────────────┤
│                                                       │
│  ┌─────────────────────────────────────────────┐    │
│  │ RegularCPU (GrantCoordinator)               │    │
│  │ - 管理 CPU-bound regular work               │    │
│  │ - WorkKinds: KVWork, SQLKVResponseWork,     │    │
│  │              SQLSQLResponseWork             │    │
│  │ - Grant Chain: Enabled                      │    │
│  └─────────────────────────────────────────────┘    │
│                                                       │
│  ┌─────────────────────────────────────────────┐    │
│  │ ElasticCPU (ElasticCPUGrantCoordinator)     │    │
│  │ - 管理 CPU-bound elastic work               │    │
│  │ - 低优先级后台任务                           │    │
│  │ - 可以被 RegularCPU 抢占                     │    │
│  └─────────────────────────────────────────────┘    │
│                                                       │
│  ┌─────────────────────────────────────────────┐    │
│  │ Stores (StoreGrantCoordinators)             │    │
│  │ - 管理每个 Store 的 IO-bound work           │    │
│  │ - 每个 Store 有独立的 GrantCoordinator      │    │
│  │ - Grant Chain: Disabled (IO 不需要)         │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

**请求流程**：

```
写入请求的双重准入：

1. Store 层准入（IO 资源）：
   ├─► StoreGrantCoordinators.GetWorkQueue(storeID, regularOrElastic)
   ├─► StoreWorkQueue.Admit(...)
   │   ├─► 检查 IO tokens
   │   ├─► 检查 L0 容量
   │   └─► 成功 or 排队等待
   └─► 获得 IO 准入

2. CPU 层准入（CPU 资源）：
   ├─► RegularCPU.GetWorkQueue(KVWork)
   ├─► WorkQueue.Admit(...)
   │   ├─► 检查 CPU slots
   │   └─► 成功 or 排队等待
   └─► 获得 CPU 准入

3. 执行工作：
   └─► 写入 Pebble

4. 释放资源：
   ├─► WorkQueue.AdmittedWorkDone() (CPU)
   └─► StoreWorkQueue.AdmittedWorkDone() (IO)

为什么这样设计？
- 先检查 IO 资源，避免占用 CPU slots 后发现 IO 不足
- IO 准入和 CPU 准入独立，可以并行调整
- 每层专注于自己的资源管理
```

### 4.2 StoreGrantCoordinators 详解

#### 架构

```go
// pkg/util/admission/store_grant_coordinator.go:53
type StoreGrantCoordinators struct {
    ambientCtx             log.AmbientContext
    settings               *cluster.Settings
    makeStoreRequesterFunc makeStoreRequesterFunc

    // 每个 StoreID → storeGrantCoordinator
    gcMap syncutil.Map[roachpb.StoreID, storeGrantCoordinator]

    numStores                      int
    setPebbleMetricsProviderCalled bool
    onLogEntryAdmitted             OnLogEntryAdmitted
    closeCh                        chan struct{}

    knobs *TestingKnobs
}
```

**每个 Store 的协调器**：

```go
type storeGrantCoordinator struct {
    coord *GrantCoordinator
    queues [admissionpb.NumWorkClasses]*StoreWorkQueue
}
```

**为什么每个 Store 需要独立的 GrantCoordinator？**

```
原因：

1. IO 资源隔离：
   ├─► 每个 Store 对应一个磁盘
   ├─► 磁盘之间的 IO 容量独立
   └─► 需要独立的 IO token 管理

2. L0 容量独立：
   ├─► 每个 Store 有独立的 Pebble 实例
   ├─► L0 子层数独立
   └─► 需要独立的 L0 token 管理

3. 磁盘健康状态独立：
   ├─► 一个 Store 的磁盘可能过载
   ├─► 其他 Store 的磁盘正常
   └─► 需要独立的准入决策

示例：双 Store 节点

Store 1：
├─► L0 子层数 = 15（接近上限）
├─► 磁盘带宽 = 80%（接近饱和）
└─► IO tokens = 0（停止准入）

Store 2：
├─► L0 子层数 = 3（正常）
├─► 磁盘带宽 = 30%（正常）
└─► IO tokens = 10000（正常准入）

结果：
- Store 1 的写入请求被阻塞 ✓
- Store 2 的写入请求正常处理 ✓
- 避免 Store 2 被 Store 1 拖累 ✓
```

#### WorkClass 分离

```go
const (
    RegularWorkClass admissionpb.WorkClass = iota  // 常规工作
    ElasticWorkClass                               // 弹性工作
)
```

**每个 Store 有 2 个 WorkQueue**：

```
Store 1:
├─► RegularWorkQueue (kv-regular-store-queue)
│   ├─► 用户前台请求
│   ├─► 高优先级
│   └─► 只检查 IO tokens[RegularWorkClass]
│
└─► ElasticWorkQueue (kv-elastic-store-queue)
    ├─► 后台任务（compaction triggers, GC, etc.）
    ├─► 低优先级
    └─► 检查 IO tokens[ElasticWorkClass] + Disk Bandwidth tokens

优先级保证：
- RegularWorkQueue 优先获得 IO tokens
- ElasticWorkQueue 只在 Regular 不需要时执行
- 防止后台任务影响用户请求
```

**kvStoreTokenGranter 的三种令牌**：

```go
type kvStoreTokenGranter struct {
    mu struct {
        syncutil.Mutex

        // IO tokens (L0 容量)
        availableIOTokens [admissionpb.NumWorkClasses]int64

        // Elastic 专用（进一步限制弹性工作）
        elasticIOTokensUsedByElastic int64

        // 磁盘带宽 tokens
        diskTokensAvailable diskTokens
    }
}
```

**授权决策表**：

| WorkClass | IO Tokens (Regular) | IO Tokens (Elastic) | Disk Bandwidth | 结果 |
|-----------|--------------------|--------------------|----------------|------|
| Regular   | > 0                | -                  | -              | ✓ 授权 |
| Regular   | <= 0               | -                  | -              | ✗ 拒绝 |
| Elastic   | > 0                | > 0                | > 0            | ✓ 授权 |
| Elastic   | <= 0               | -                  | -              | ✗ 拒绝 |
| Elastic   | > 0                | <= 0               | -              | ✗ 拒绝 |
| Elastic   | > 0                | > 0                | <= 0           | ✗ 拒绝 |

---

## DFS 层次 5：完整的请求生命周期

### 5.1 KVWork 请求流程

```
┌─────────────────────────────────────────────────────┐
│             KVWork 请求生命周期                       │
└─────────────────────────────────────────────────────┘

Step 1：请求到达
├─► KV 层收到请求（读/写）
└─► 需要 CPU 资源

Step 2：CPU 准入（RegularCPU GrantCoordinator）
├─► kvQueue := RegularCPU.GetWorkQueue(KVWork)
├─► kvQueue.Admit(ctx, WorkInfo{...})
│   │
│   ├─► [Fast Path] 队列为空且 slots 可用
│   │   ├─► granter.tryGet(count=1)
│   │   │   └─► coord.tryGet(KVWork, 1, 0)
│   │   │       ├─► slotGranter.tryGetLocked(1, 0)
│   │   │       │   ├─► usedSlots < totalSlots?
│   │   │       │   │   ├─► Yes → usedSlots++, return grantSuccess
│   │   │       │   │   └─► No → return grantFailDueToSharedResource
│   │   │       │   │
│   │   │       └─► grantSuccess → return true
│   │   └─► 直接执行，跳过排队
│   │
│   └─► [Slow Path] 队列非空或 slots 不足
│       ├─► 创建 waitingWork
│       ├─► 入队到 tenantHeap
│       ├─► 等待授权
│       │   ├─► 阻塞在 channel
│       │   └─► 等待 granter.granted(grantChainID)
│       │
│       └─► 收到授权
│           ├─► Granter 调用 WorkQueue.granted(grantChainID)
│           ├─► 从 heap 出队
│           ├─► 通过 channel 通知等待的 goroutine
│           └─► 返回 (enabled=true, err=nil)
│
└─► 获得 CPU 准入

Step 3：执行 KV 工作
├─► 读取数据 or 写入数据
└─► 占用 1 个 CPU slot

Step 4：完成并释放资源
├─► kvQueue.AdmittedWorkDone(tenantID)
├─► slotGranter.returnGrant(1)
│   └─► coord.returnGrant(KVWork, 1, 0)
│       ├─► slotGranter.returnGrantLocked(1, 0)
│       │   ├─► usedSlots--
│       │   └─► 更新 metrics
│       │
│       └─► coord.tryGrantLocked()
│           ├─► 检查是否需要终止 Grant Chain
│           └─► 授权等待的请求
│
└─► 资源释放完成

Step 5：Grant Chain 延续（如果适用）
├─► 如果这个请求获得了 grantChainID
├─► 在执行完成后调用 continueGrantChain(grantChainID)
└─► 触发下一轮批量授权
```

### 5.2 SQLKVResponseWork 请求流程

```
┌─────────────────────────────────────────────────────┐
│        SQLKVResponseWork 请求生命周期                 │
└─────────────────────────────────────────────────────┘

Step 1：KV 响应到达
├─► SQL 层收到 KV 层的响应
├─► 需要处理响应（反序列化、聚合等）
└─► 需要 CPU 资源

Step 2：CPU 准入（RegularCPU GrantCoordinator）
├─► sqlKVQueue := RegularCPU.GetWorkQueue(SQLKVResponseWork)
├─► sqlKVQueue.Admit(ctx, WorkInfo{...})
│   │
│   ├─► [Fast Path] 队列为空且 tokens 可用
│   │   ├─► granter.tryGet(count=estimatedTokens)
│   │   │   └─► coord.tryGet(SQLKVResponseWork, estimatedTokens, 0)
│   │   │       ├─► tokenGranter.tryGetLocked(estimatedTokens, 0)
│   │   │       │   ├─► cpuOverload.isOverloaded()?
│   │   │       │   │   ├─► Yes → return grantFailDueToSharedResource
│   │   │       │   │   └─► No → 继续
│   │   │       │   │
│   │   │       │   ├─► availableBurstTokens > 0?
│   │   │       │   │   ├─► Yes → availableBurstTokens -= count
│   │   │       │   │   │          return grantSuccess
│   │   │       │   │   └─► No → return grantFailLocal
│   │   │       │   │
│   │   │       └─► grantSuccess → return true
│   │   └─► 直接执行
│   │
│   └─► [Slow Path] 排队等待
│       └─► 入队到 waitingWorkHeap（FIFO or Epoch-LIFO）
│
└─► 获得 CPU 准入

Step 3：处理 KV 响应
├─► 反序列化数据
├─► 聚合/过滤数据
└─► 占用 CPU tokens

Step 4：完成（tokens 自动管理）
├─► tokenGranter 的 tokens 会周期性刷新
├─► 不需要显式调用 AdmittedWorkDone()
└─► 下次 CPULoad() 时刷新 availableBurstTokens
```

### 5.3 Store 写入请求流程（双重准入）

```
┌─────────────────────────────────────────────────────┐
│          Store 写入请求生命周期（双重准入）            │
└─────────────────────────────────────────────────────┘

Step 1：写入请求到达
├─► KV 层需要写入 Store 1
└─► 请求类型：Regular or Elastic

Step 2：第一层准入（Store IO 资源）
├─► storeQueue := Stores.GetWorkQueue(storeID=1, workClass=Regular)
├─► storeQueue.Admit(ctx, WorkInfo{RequestedCount: 1000})
│   │
│   ├─► [Fast Path] 队列为空且 IO tokens 可用
│   │   ├─► kvStoreTokenGranter.tryGet(count=1000)
│   │   │   ├─► availableIOTokens[RegularWorkClass] > 0?
│   │   │   │   └─► Yes → 扣除 tokens, return true
│   │   │   └─► No → return false
│   │   └─► 获得 IO 准入
│   │
│   └─► [Slow Path] 排队等待
│       └─► 入队，等待 IO tokens 可用
│
└─► 获得 Store 层准入

Step 3：第二层准入（CPU 资源）
├─► kvQueue := RegularCPU.GetWorkQueue(KVWork)
├─► kvQueue.Admit(ctx, WorkInfo{...})
│   └─► （同 5.1 KVWork 流程）
│
└─► 获得 CPU 层准入

Step 4：执行写入
├─► 写入 Pebble
├─► 数据写入 MemTable
└─► 可能触发 flush/compaction

Step 5：异步准入（below-raft）
├─► 写入被复制到其他副本
├─► 每个副本异步调用 AdmittedWorkDone()
├─► 根据实际 WriteBytes 和 IngestedBytes 调整 tokens
└─► 使用线性模型估算 L0 增长

Step 6：释放资源
├─► storeQueue.AdmittedWorkDone(...)
│   ├─► 根据实际 IO 调整 tokens
│   └─► 调用 tryGrantLocked() 授权等待请求
│
├─► kvQueue.AdmittedWorkDone(tenantID)
│   └─► 释放 CPU slot
│
└─► 资源完全释放
```

---

## DFS 层次 6：性能优化与监控

### 6.1 Grant Chain 的性能优化

#### 优化 1：批量授权减少锁竞争

**对比**：

```
朴素方式（每次授权 1 个）：

for i := 0; i < 100; i++ {
    coord.mu.Lock()
    granter.tryGrantLocked(1, 0)
    requester.granted(noGrantChain)
    coord.mu.Unlock()
}

开销：
- 加锁/解锁：100 次
- 上下文切换：100 次
- 总耗时：10ms+

Grant Chain 方式：

// 第一轮批量授权
coord.mu.Lock()
for i := 0; i < 40; i++ {
    granter.tryGrantLocked(chainID if i==39 else noGrantChain)
    requester.granted(chainID if i==39 else noGrantChain)
}
coord.mu.Unlock()

// 第二轮（由最后一个 goroutine 触发）
continueGrantChain(chainID)
    ├─► coord.mu.Lock()
    ├─► 再授权 40 个
    └─► coord.mu.Unlock()

// 第三轮
continueGrantChain(chainID)
    └─► ...

开销：
- 加锁/解锁：3 次
- 上下文切换：3 次（批量唤醒 goroutines）
- 总耗时：3ms
```

#### 优化 2：自然节流（Natural Throttling）

**问题**：

```
如果批量授权过快：

Time: 0ms
├─► 授权 100 个请求
└─► 立即唤醒 100 个 goroutines

Time: 0.1ms
├─► 100 个 goroutines 同时竞争 CPU
├─► runnable count 暴涨
└─► kvSlotAdjuster 误判为过载，减少 slots

Time: 1ms
├─► slots 被减少到 10
├─► 只有 10 个 goroutines 能执行
└─► CPU 利用率下降（under-utilization）
```

**Grant Chain 的解决方案**：

```
批量授权 + 链式延续：

Time: 0ms
├─► 授权 40 个请求（grantBurstLimit）
├─► 最后一个获得 grantChainID
└─► 唤醒 40 个 goroutines

Time: 0.5ms
├─► 40 个 goroutines 开始调度
├─► runnable count 逐渐上升到 40
└─► 最后一个 goroutine 还未运行

Time: 1ms
├─► 最后一个 goroutine 运行
├─► 调用 continueGrantChain(grantChainID)
└─► 授权下一批 40 个请求

关键：
- 只有当最后一个 goroutine 真正运行时，才授权下一批
- 利用 goroutine 调度器的自然能力，避免过度授权
- runnable count 更平稳，kvSlotAdjuster 更准确
```

#### 优化 3：延迟终止避免抖动

**问题**：

```
立即终止 Grant Chain 的问题：

Time: 0ms
├─► 启动 Grant Chain A (ID=100)
└─► 授权 40 个 SQLSQLResponseWork

Time: 1ms
├─► CPU 短暂过载
├─► 立即终止 Grant Chain A (ID++)
└─► Grant Chain A 死亡

Time: 2ms
├─► CPU 恢复正常
├─► 启动新 Grant Chain B (ID=101)
└─► 授权 40 个 KVWork

Time: 3ms
├─► CPU 再次短暂过载
├─► 立即终止 Grant Chain B
└─► ...

结果：
- Grant Chain 频繁启动/终止
- runnable count 剧烈波动
- kvSlotAdjuster 持续减少 slots
- CPU 利用率下降
```

**延迟终止的效果**：

```
延迟 100ms 后才终止：

Time: 0ms
├─► 启动 Grant Chain A (ID=100)
└─► 授权 40 个 SQLSQLResponseWork

Time: 1ms
├─► CPU 短暂过载
├─► 尝试终止 Grant Chain A
└─► 启动时间 < 100ms，不终止

Time: 50ms
├─► CPU 恢复正常
└─► Grant Chain A 继续运行

Time: 101ms
├─► Grant Chain A 自然结束（队列为空）
└─► 系统平稳

结果：
- Grant Chain 更稳定
- runnable count 平稳
- kvSlotAdjuster 准确
- CPU 利用率更高
```

### 6.2 监控指标

#### GrantCoordinator 指标

```go
type GrantCoordinatorMetrics struct {
    KVTotalSlots                 *metric.Gauge    // KV 总 slots
    KVUsedSlots                  *metric.Gauge    // KV 已用 slots
    KVSlotsExhaustedDuration     *metric.Counter  // Slots 耗尽时长
    KVCPULoadShortPeriodDuration *metric.Counter  // 短周期 CPU load 时长
    KVCPULoadLongPeriodDuration  *metric.Counter  // 长周期 CPU load 时长
    KVSlotAdjusterIncrements     *metric.Counter  // Slots 增加次数
    KVSlotAdjusterDecrements     *metric.Counter  // Slots 减少次数
}
```

**关键指标解读**：

| 指标 | 正常值 | 异常值 | 处理方法 |
|-----|--------|--------|---------|
| `KVUsedSlots / KVTotalSlots` | < 80% | > 95% | CPU 过载，需要优化查询 |
| `KVSlotsExhaustedDuration` | 偶尔短暂 | 持续高值 | Slots 不足，调整配置 |
| `KVSlotAdjusterIncrements` | 稳定增长 | 停止增长 | 已达最大值或系统过载 |
| `KVSlotAdjusterDecrements` | 偶尔 | 频繁 | CPU 持续过载 |
| `CPULoadShortPeriod + CPULoadLongPeriod` | ≈ 1s/s | < 0.5s/s | CPULoad 采样频率不足 |

#### Grant Chain 调试信息

```go
func (coord *GrantCoordinator) SafeFormat(s redact.SafePrinter, _ rune) {
    coord.mu.Lock()
    defer coord.mu.Unlock()

    s.Printf("(chain: id: %d active: %t index: %d)",
        coord.mu.grantChainID,
        coord.mu.grantChainActive,
        coord.mu.grantChainIndex,
    )

    for i := range coord.granters {
        kind := WorkKind(i)
        switch kind {
        case KVWork:
            g := coord.granters[i].(*slotGranter)
            s.Printf(" %s: used: %d, total: %d", kind, g.usedSlots, g.totalSlots)
        case SQLKVResponseWork, SQLSQLResponseWork:
            if coord.granters[i] != nil {
                g := coord.granters[i].(*tokenGranter)
                s.Printf(" %s: avail: %d", kind, g.availableBurstTokens)
            }
        }
    }
}
```

**输出示例**：

```
(chain: id: 1523 active: true index: 1) KVWork: used: 45, total: 50
SQLKVResponseWork: avail: 80000
SQLSQLResponseWork: avail: 95000

解读：
- Grant Chain 活跃，ID=1523
- 当前处理 index=1（SQLKVResponseWork）
- KVWork: 45/50 slots 已用（90%）
- SQLKVResponseWork: 80000 tokens 可用
- SQLSQLResponseWork: 95000 tokens 可用
```

### 6.3 调试技巧

#### 场景 1：高优先级工作等待时间过长

**症状**：

```
- KVWork 队列积压
- 但 SQLSQLResponseWork 仍在大量授权
- KVWork 等待时间 > 1s
```

**排查步骤**：

```
1. 检查 Grant Chain 状态：
   └─► grantChainActive=true, grantChainIndex=2
       → Grant Chain 正在处理 SQLSQLResponseWork

2. 检查是否应该终止 Grant Chain：
   ├─► CPU 过载？
   │   ├─► kvSlotAdjuster.isOverloaded() = false
   │   └─► 不应该终止
   │
   └─► Grant Chain 启动时间？
       ├─► now - grantChainStartTime = 50ms
       ├─► < delayForGrantChainTermination (100ms)
       └─► 还不能终止

3. 等待 Grant Chain 自然结束或达到终止条件
   └─► 50ms 后可以终止

可能原因：
- delayForGrantChainTermination 太长（100ms）
- grantBurstLimit 太大，授权过多低优先级工作
```

**解决方案**：

```
1. 调整 delayForGrantChainTermination：
   └─► 减少到 50ms（需要权衡稳定性）

2. 调整 grantBurstLimit：
   └─► 减少 KVSlotAdjusterOverloadThreshold
       → multiplier 减少
       → grantBurstLimit 减少

3. 增加 KVWork slots：
   └─► 调整 MinCPUSlots/MaxCPUSlots
       → totalSlots 增加
       → KVWork 更快被处理
```

#### 场景 2：Grant Chain 频繁启动/终止

**症状**：

```
- grantChainID 快速增长（1s 内增加 10+）
- runnable count 剧烈波动
- CPU 利用率不稳定
```

**排查步骤**：

```
1. 检查终止原因：
   ├─► CPU 过载频繁？
   │   ├─► 检查 runnable count 趋势
   │   └─► 检查 kvSlotAdjuster 日志
   │
   └─► 高优先级工作频繁到达？
       └─► 检查 KVWork 队列入队频率

2. 检查 delayForGrantChainTermination：
   ├─► 是否被禁用（测试环境）？
   └─► 是否太短？

3. 检查 grantBurstLimit：
   ├─► 是否太小？
   └─► 导致 Grant Chain 过快结束
```

**解决方案**：

```
1. 恢复 delayForGrantChainTermination = 100ms

2. 增加 grantBurstLimit：
   └─► 增加 KVSlotAdjusterOverloadThreshold
       → multiplier 增加
       → 每次授权更多请求

3. 优化工作负载：
   └─► 减少小请求数量
       → 合并批处理
       → 减少授权频率
```

---

## 总结

### GrantCoordinator 的核心价值

1. **统一的资源协调**：
   - 管理多个 WorkKind 的授权流程
   - 实现严格的优先级控制
   - 协调 CPU 和 IO 资源

2. **Grant Chain 性能优化**：
   - 批量授权减少锁竞争（100x → 3x）
   - 自然节流避免过度授权
   - 链式延续提高吞吐量

3. **智能的过载响应**：
   - CPU 过载时立即终止低优先级授权
   - 高优先级工作优先处理
   - 延迟终止避免系统抖动

4. **多层架构设计**：
   - GrantCoordinators 容器管理多个实例
   - RegularCPU 处理 CPU-bound work
   - Stores 处理 IO-bound work（每个 Store 独立）

### 关键设计决策

| 决策 | 原因 |
|-----|------|
| Grant Chain 批量授权 | 减少锁竞争，提高吞吐量 |
| grantBurstLimit = numProcs * multiplier | 为每个 CPU 核心产生足够的 runnable goroutines |
| delayForGrantChainTermination = 100ms | 避免频繁启动/终止导致的系统抖动 |
| WorkKind 严格优先级 | KVWork > SQLKVResponseWork > SQLSQLResponseWork |
| 每个 Store 独立 GrantCoordinator | IO 资源隔离，磁盘健康独立 |
| Grant Chain 只用于 CPU 资源 | IO 资源不需要（已知完成时间） |

### 适用场景

✓ **适合**：
- 多 WorkKind 混合负载
- 需要严格优先级控制
- CPU-bound 工作为主
- 高并发场景

✗ **不适合**：
- 单一 WorkKind
- 无优先级需求
- IO-bound 工作为主（用 StoreGrantCoordinators）

---

**下一章预告**：第十四章将深入探讨 **kvSlotAdjuster 动态槽位调整机制**，看它如何根据 CPU 负载动态调整 KVWork 的 slots 数量，实现自适应的准入控制。