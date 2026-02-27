# RateLimiter深度剖析——基于TokenBucket的速率限制与准入控制机制

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 一、第一轮 BFS：职责边界与设计动机（Why）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 1.1 系统性问题与存在背景

在分布式数据库系统中，**速率限制（Rate Limiting）** 是保护系统稳定性的关键机制之一：

**核心困境**：
- **资源保护**：某些操作（如一致性检查）消耗大量 CPU/IO 资源，如果不加限制会影响正常读写请求。
- **突发流量控制**：允许短时间内的突发请求（burst），但限制长期平均速率，避免系统过载。
- **公平性保证**：在多租户或多组件环境下，确保各类操作都能获得合理的资源配额。
- **动态配置**：速率限制需要能动态调整（如通过 cluster settings），无需重启服务。

**CockroachDB 中的具体场景**：

```go
// pkg/kv/kvserver/store.go (简化示例)
s.consistencyLimiter = quotapool.NewRateLimiter(
    "ConsistencyQueue",
    quotapool.Limit(consistencyCheckRate.Get(&cfg.Settings.SV)),
    consistencyCheckRate.Get(&cfg.Settings.SV)*consistencyCheckRateBurstFactor,
    quotapool.WithMinimumWait(consistencyCheckRateMinWait))
```

**这段代码的背景**：
- **ConsistencyQueue**：后台一致性检查队列，定期验证 Raft replica 之间的数据一致性。
- **速率限制必要性**：一致性检查需要读取大量数据并计算校验和，如果不限速，会导致：
  1. CPU 资源被占满，影响前台读写
  2. 磁盘 IO 饱和，增加读写延迟
  3. 网络带宽被消耗，影响 Raft 复制

**没有 RateLimiter 的后果**：
1. **系统过载**：一致性检查风暴导致 P99 延迟飙升。
2. **资源竞争**：后台任务与前台请求争抢 CPU/IO。
3. **雪崩效应**：过载导致 Raft 心跳超时，触发 leader 切换，进一步加剧不稳定。

### 1.2 系统中的位置与上下游关系

**所属子系统**：
- **准入控制层（Admission Control）**
- 位于 `pkg/util/quotapool/` 包中

**在 CockroachDB 架构中的位置**：
```
┌─────────────────────────────────────────┐
│   SQL / KV Request Layer                │
└─────────────────┬───────────────────────┘
                  │
                  ↓
┌─────────────────────────────────────────┐
│   Store / Replica Layer                 │
│   ├─ Replica Queues (GC, Split, etc.)  │  ← 本模块位置
│   │   ├─ ConsistencyQueue               │
│   │   │   └─ consistencyLimiter         │  ← RateLimiter 实例
│   │   └─ Other Queues                   │
└─────────────────┬───────────────────────┘
                  │
                  ↓
┌─────────────────────────────────────────┐
│   Storage Engine (Pebble)               │
└─────────────────────────────────────────┘
```

**上游调用者**：
- **ConsistencyQueue**：后台一致性检查队列
- **其他 Replica Queues**：如 GC queue、Raft snapshot queue 等
- **任何需要速率限制的组件**

**下游依赖**：
- **TokenBucket**（github.com/cockroachdb/tokenbucket）：底层 token bucket 算法实现
- **AbstractPool**（pkg/util/quotapool）：通用的资源池抽象
- **time.Timer**：定时器，用于延迟等待

### 1.3 核心抽象与生命周期

#### 核心对象

**1. `RateLimiter` 结构体**（长期存在，整个服务生命周期）
```go
type RateLimiter struct {
    qp    *AbstractPool  // 底层抽象资源池
    isInf atomic.Bool    // 是否无限速率（优化路径）
}
```

**2. `TokenBucket` 结构体**（作为 Resource 实现）
```go
// 来自 github.com/cockroachdb/tokenbucket
type TokenBucket struct {
    // 核心状态（内部实现，简化示例）
    rate       TokensPerSecond  // 速率：每秒补充多少 token
    burst      Tokens           // 桶容量：最多存储多少 token
    availTokens Tokens          // 当前可用 token
    lastUpdate  time.Time       // 上次更新时间
}
```

**3. `RateAlloc` 结构体**（短生命周期，可归还的配额）
```go
type RateAlloc struct {
    alloc int64        // 获取的 token 数量
    rl    *RateLimiter // 所属的 RateLimiter
}
```

**4. `rateRequest` 结构体**（请求对象，使用 sync.Pool 复用）
```go
type rateRequest struct {
    want int64  // 请求的 token 数量
}
```

#### Token Bucket 算法原理

**核心思想**：
```
想象一个水桶（Token Bucket）：
1. 桶的容量：burst（如 100 个 token）
2. 水龙头速率：rate（如每秒 10 个 token）
3. 每次请求消耗若干 token
4. 如果桶里 token 不足，请求需要等待

时间推移：
  T0: 桶满（100 token）
  T1: 请求 50 token → 成功（剩余 50）
  T2: 请求 60 token → 需等待 1 秒（rate=10，需等待 (60-50)/10=1秒）
  T3 (T2+1秒): 桶里现在有 50+10=60 token → 成功
```

**数学表达**：
```
可用 token = min(burst, availTokens + rate * (now - lastUpdate))

等待时间 = max(0, (want - availTokens) / rate)
```

#### 生命周期

```
[RateLimiter 生命周期]

1. NewRateLimiter(name, rate, burst, options...)
   └─> 创建 TokenBucket（初始满桶）
   └─> 创建 AbstractPool
   └─> 设置 isInf 标志

2. 持续接收请求
   └─> WaitN(ctx, n) / Acquire(ctx, n) / AdmitN(n)

3. 请求处理流程
   └─> isInf=true → 立即返回（无限速率）
   └─> isInf=false → 尝试获取 token
       ├─> token 足够 → 立即返回
       ├─> token 不足 → 计算等待时间
       │   └─> 创建 Timer
       │   └─> select { case <-timer / case <-ctx.Done() }
       └─> 成功 → 返回 RateAlloc（可归还）

4. 配额归还（可选）
   └─> RateAlloc.Return() → 调整 TokenBucket

5. 动态更新（运行时）
   └─> UpdateLimit(newRate, newBurst)
       └─> 更新 TokenBucket 配置
       └─> 调整当前 availTokens
```

```
[RateAlloc 生命周期]

T0: Acquire(ctx, 50)
    └─> 创建 RateAlloc{alloc: 50, rl: ...}
    └─> 从 sync.Pool 获取对象

T1: 使用配额执行操作
    └─> doConsistencyCheck()

T2a: Return() - 归还配额
    └─> TokenBucket.Adjust(+50)
    └─> 放回 sync.Pool

或

T2b: Consume() - 消耗配额
    └─> 直接放回 sync.Pool（不归还）
```

### 1.4 核心设计意图总结

RateLimiter 的设计目标是：

1. **平滑速率控制**：基于 Token Bucket 算法，允许突发但限制长期速率。
2. **可归还配额**：支持"预留-归还"模式，适配任务可能取消的场景。
3. **动态配置**：运行时调整速率和 burst，无需重启。
4. **高性能**：
   - 无限速率时零开销（isInf 优化）
   - sync.Pool 复用请求对象
   - 原子操作避免锁竞争
5. **上下文感知**：支持 context 取消和超时。

**关键洞察**：RateLimiter 是一个**自适应、可归还、高性能的速率限制器**，通过 Token Bucket 算法实现了**突发容忍 + 长期限速**的平衡。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 二、第二轮 BFS：控制流与组件协作（How it flows）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 2.1 主要执行路径与状态流转

#### 路径 1：WaitN（纯等待，不可归还）

```
[初始状态] rate=10/s, burst=100, availTokens=100

Step 1: WaitN(ctx, 5)
   ├─> 检查 isInf.Load() == false（需要限速）
   ├─> newRateRequest(5) → 从 sync.Pool 获取
   │   └─> rateRequest{want: 5}
   │
   └─> qp.Acquire(ctx, request)
       │
       ├─> AbstractPool 调用 request.Acquire(res)
       │   └─> tb.TryToFulfill(Tokens(5))
       │       ├─> 更新 availTokens（补充自上次的 token）
       │       ├─> availTokens = 100 + 10*(now-lastUpdate)
       │       ├─> 检查 availTokens >= 5
       │       │   └─> 是：扣除 5，返回 (fulfilled=true, tryAgainAfter=0)
       │       └─> lastUpdate = now
       │
       ├─> fulfilled=true → 立即返回
       │
       └─> putRateRequest(request) → 放回 sync.Pool

[终态] availTokens=95

────────────────────────────────────────────

Step 2: WaitN(ctx, 100)（不足，需等待）
   ├─> newRateRequest(100)
   │
   └─> qp.Acquire(ctx, request)
       │
       ├─> tb.TryToFulfill(Tokens(100))
       │   ├─> availTokens = 95
       │   ├─> availTokens < 100 → 不足！
       │   ├─> 计算等待时间：
       │   │   shortage = 100 - 95 = 5
       │   │   waitDuration = shortage / rate = 5 / 10 = 0.5 秒
       │   └─> 返回 (fulfilled=false, tryAgainAfter=500ms)
       │
       ├─> AbstractPool 创建 Timer(500ms)
       │
       ├─> select {
       │       case <-timer.C:
       │           // 重新尝试 TryToFulfill
       │       case <-ctx.Done():
       │           return ctx.Err()
       │   }
       │
       └─> 500ms 后重试成功
           └─> availTokens = 95 + 10*0.5 = 100
           └─> 扣除 100，返回

[终态] availTokens=0
```

#### 路径 2：Acquire（可归还配额）

```
[初始状态] availTokens=50

Step 1: Acquire(ctx, 30)
   ├─> WaitN(ctx, 30) → 成功（availTokens 足够）
   │
   └─> newRateAlloc(30)
       └─> 从 sync.Pool 获取 rateAlloc
       └─> rateAlloc{alloc: 30, rl: ...}
       └─> 返回 (*RateAlloc)(rateAlloc)

[终态] availTokens=20，持有 RateAlloc{alloc: 30}

────────────────────────────────────────────

场景 A：归还配额
Step 2a: rateAlloc.Return()
   ├─> qp.Update(func(res Resource) bool {
   │       tb := res.(*TokenBucket)
   │       tb.Adjust(Tokens(30))  // 归还 30
   │       return true  // shouldNotify=true
   │   })
   │
   ├─> TokenBucket.Adjust(+30)
   │   └─> availTokens += 30
   │
   └─> putRateAlloc(rateAlloc) → 放回 sync.Pool

[终态] availTokens=50（已归还）

────────────────────────────────────────────

场景 B：消耗配额
Step 2b: rateAlloc.Consume()
   └─> putRateAlloc(rateAlloc) → 直接放回 sync.Pool

[终态] availTokens=20（未归还）
```

#### 路径 3：AdmitN（非阻塞尝试）

```
[初始状态] availTokens=15

Step 1: AdmitN(10)
   ├─> isInf.Load() == false
   │
   ├─> newRateRequest(10)
   │
   └─> qp.Acquire(context.Background(), (*rateRequestNoWait)(request))
       │
       ├─> rateRequestNoWait.ShouldWait() → 返回 false
       │   （告诉 AbstractPool 不要阻塞等待）
       │
       ├─> tb.TryToFulfill(Tokens(10))
       │   └─> availTokens=15 >= 10 → 成功
       │
       └─> 返回 nil（成功）

[终态] availTokens=5，返回 true

────────────────────────────────────────────

Step 2: AdmitN(20)（不足，但不等待）
   ├─> tb.TryToFulfill(Tokens(20))
   │   └─> availTokens=5 < 20 → 失败
   │
   └─> AbstractPool 检测到 ShouldWait()=false
       └─> 直接返回 error

[终态] availTokens=5，返回 false
```

#### 路径 4：UpdateLimit（动态调整）

```
[初始状态] rate=10/s, burst=100, availTokens=50

Step 1: UpdateLimit(rate=20/s, burst=200)
   ├─> qp.Update(func(res Resource) bool {
   │       isInf.Store(math.IsInf(20, 1))  // false
   │       tb := res.(*TokenBucket)
   │       tb.UpdateConfig(TokensPerSecond(20), Tokens(200))
   │       return true  // shouldNotify
   │   })
   │
   └─> TokenBucket.UpdateConfig(20, 200)
       ├─> 计算 burst delta = 200 - 100 = +100
       ├─> 调整当前可用 token：
       │   availTokens = 50 + 100 = 150
       ├─> 更新 rate = 20
       └─> 更新 burst = 200

[终态] rate=20/s, burst=200, availTokens=150

说明：
- 如果 burst 增加：availTokens 也相应增加（立即释放更多配额）
- 如果 burst 减少：availTokens 也减少（可能变负，进入债务状态）
```

### 2.2 触发方式

| 操作 | 触发方式 | 调用者 | 阻塞性 |
|-----|---------|-------|-------|
| `WaitN()` | **请求驱动** | 业务代码（如 ConsistencyQueue） | 阻塞（token 不足时） |
| `Acquire()` | **请求驱动** | 业务代码（需要归还配额的场景） | 阻塞（token 不足时） |
| `AdmitN()` | **请求驱动** | 业务代码（快速失败场景） | 非阻塞（token 不足立即返回 false） |
| `UpdateLimit()` | **配置驱动** | Cluster Settings 监听器 | 非阻塞（异步更新） |
| `Return()` | **主动触发** | 业务代码（操作取消或完成） | 非阻塞 |

**关键设计**：
- **完全事件驱动**：无后台 goroutine，所有操作由调用者触发。
- **零开销快速路径**：`isInf=true` 时，所有操作立即返回，无需访问 TokenBucket。
- **惰性补充 token**：只在 `TryToFulfill` 时才计算并补充 token，而非定时补充。

### 2.3 与其他模块的交互方式

#### 与 TokenBucket 的交互

**TokenBucket 的核心方法**：
```go
// TryToFulfill 尝试满足 token 请求
// 返回：
//   - fulfilled: 是否立即满足
//   - tryAgainAfter: 如果不满足，需要等待多久
func (tb *TokenBucket) TryToFulfill(want Tokens) (fulfilled bool, tryAgainAfter time.Duration)

// Adjust 调整当前可用 token（归还或扣除）
func (tb *TokenBucket) Adjust(delta Tokens)

// UpdateConfig 更新速率和 burst
func (tb *TokenBucket) UpdateConfig(rate TokensPerSecond, burst Tokens)
```

**RateLimiter 如何使用 TokenBucket**：
```go
// WaitN 内部
tb.TryToFulfill(want) → (fulfilled, waitDuration)
if !fulfilled {
    time.Sleep(waitDuration)  // 实际由 AbstractPool 的 Timer 实现
    // 重试
}

// Return 内部
tb.Adjust(+alloc)  // 归还 token

// UpdateLimit 内部
tb.UpdateConfig(newRate, newBurst)
```

#### 与 AbstractPool 的交互

**AbstractPool 的职责**：
- 管理 Resource（TokenBucket）
- 处理请求队列与等待逻辑
- 提供 Update 机制（用于动态配置）

**交互流程**：
```go
// RateLimiter 创建时
qp = New(name, tokenBucket, options...)

// WaitN 调用时
request := &rateRequest{want: n}
qp.Acquire(ctx, request)
    ├─> 调用 request.Acquire(tokenBucket)
    │   └─> 返回 (fulfilled, tryAgainAfter)
    ├─> 如果 !fulfilled：
    │   └─> 创建 Timer(tryAgainAfter)
    │   └─> 等待并重试
    └─> 成功返回

// UpdateLimit 调用时
qp.Update(func(res Resource) bool {
    tb := res.(*TokenBucket)
    tb.UpdateConfig(...)
    return true  // 通知等待者重新尝试
})
```

#### 与 Cluster Settings 的交互

**配置动态更新流程**：
```go
// pkg/kv/kvserver/store.go
consistencyCheckRate.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
    newRate := consistencyCheckRate.Get(&cfg.Settings.SV)
    newBurst := newRate * consistencyCheckRateBurstFactor

    s.consistencyLimiter.UpdateLimit(
        quotapool.Limit(newRate),
        newBurst,
    )
})
```

**效果**：
1. 管理员通过 SQL 修改 cluster setting
2. 所有节点的 Settings 对象接收变更通知
3. 回调函数调用 `UpdateLimit()`
4. RateLimiter 立即应用新配置
5. 等待中的请求被唤醒，使用新配置重新尝试

### 2.4 并发控制模型

#### 无锁设计（大部分情况）

**TokenBucket 的并发安全**：
```go
// github.com/cockroachdb/tokenbucket 内部实现（简化）
type TokenBucket struct {
    mu           sync.Mutex  // 保护所有字段
    rate         TokensPerSecond
    burst        Tokens
    availTokens  Tokens
    lastUpdate   time.Time
}

func (tb *TokenBucket) TryToFulfill(want Tokens) (bool, time.Duration) {
    tb.mu.Lock()
    defer tb.mu.Unlock()

    // 1. 补充 token
    now := tb.timeSource.Now()
    elapsed := now.Sub(tb.lastUpdate)
    tb.availTokens += Tokens(tb.rate * TokensPerSecond(elapsed.Seconds()))
    if tb.availTokens > tb.burst {
        tb.availTokens = tb.burst
    }
    tb.lastUpdate = now

    // 2. 尝试满足请求
    if tb.availTokens >= want {
        tb.availTokens -= want
        return true, 0
    }

    // 3. 计算等待时间
    shortage := want - tb.availTokens
    waitDuration := time.Duration(float64(shortage) / float64(tb.rate) * float64(time.Second))
    return false, waitDuration
}
```

**关键点**：
- TokenBucket 内部使用 `sync.Mutex` 保护状态
- 每次操作（TryToFulfill / Adjust / UpdateConfig）都获取锁
- 锁持有时间极短（只做算术运算）

#### isInf 优化（原子操作）

```go
type RateLimiter struct {
    isInf atomic.Bool  // 无锁原子操作
}

func (rl *RateLimiter) WaitN(ctx context.Context, n int64) error {
    if rl.isInf.Load() {  // 原子读
        return nil  // 快速路径，无需访问 TokenBucket
    }
    // 正常路径
}
```

**优势**：
- 无限速率时（如测试环境），完全无锁无开销
- 生产环境中，`isInf.Load()` 只是一次原子读（几纳秒）

#### sync.Pool 复用（减少分配）

```go
var rateRequestSyncPool = sync.Pool{
    New: func() interface{} { return new(rateRequest) },
}

func (rl *RateLimiter) newRateRequest(v int64) *rateRequest {
    r := rateRequestSyncPool.Get().(*rateRequest)
    *r = rateRequest{want: v}
    return r
}

func (rl *RateLimiter) putRateRequest(r *rateRequest) {
    *r = rateRequest{}  // 清零避免内存泄漏
    rateRequestSyncPool.Put(r)
}
```

**效果**：
- 高频调用 `WaitN()` 时，避免频繁分配 `rateRequest` 对象
- 减少 GC 压力

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 三、DFS 深入：关键函数与核心逻辑（How it works）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 3.1 `NewRateLimiter()` - 构造函数

**函数签名**：
```go
func NewRateLimiter(name string, rate Limit, burst int64, options ...Option) *RateLimiter
```

**输入**：
- `name`：限流器名称（用于监控和调试）
- `rate`：速率（每秒 token 数），类型 `Limit` 是 `float64` 的别名
- `burst`：桶容量（最大可用 token 数）
- `options`：可选配置（如 `WithMinimumWait`）

**核心逻辑**：

```go
func NewRateLimiter(name string, rate Limit, burst int64, options ...Option) *RateLimiter {
    rl := &RateLimiter{}
    tb := &tokenbucket.TokenBucket{}  // Step 1: 创建 TokenBucket

    // Step 2: 创建 AbstractPool
    rl.qp = New(name, tb, options...)

    // Step 3: 初始化 TokenBucket
    // InitWithNowFn 允许注入时间源（用于测试）
    tb.InitWithNowFn(
        tokenbucket.TokensPerSecond(rate),  // 速率
        tokenbucket.Tokens(burst),          // 桶容量
        rl.qp.timeSource.Now,               // 时间函数
    )

    // Step 4: 设置无限速率标志
    rl.isInf.Store(math.IsInf(float64(rate), 1))

    return rl
}
```

**关键点 1：TokenBucket 初始状态**

```go
tb.InitWithNowFn(rate, burst, nowFn)
// 内部逻辑（简化）：
tb.rate = rate
tb.burst = burst
tb.availTokens = burst  // 初始满桶
tb.lastUpdate = nowFn()
```

**为什么初始满桶？**
- 启动时允许突发请求立即通过
- 符合 Token Bucket 语义（系统刚启动时应有最大容量）

**关键点 2：`WithMinimumWait` 选项**

```go
// 使用示例
rl := NewRateLimiter(
    "test",
    2e9,  // 20亿 token/秒
    1e10, // 100亿 token
    WithMinimumWait(time.Microsecond),  // 最小等待时间 1µs
)
```

**作用**：
- 当计算出的等待时间小于 `minimumWait` 时，强制等待 `minimumWait`
- **用途**：避免"忙等待"（如等待时间为 0.5 纳秒时，实际无法精确等待）
- **测试**：`TestRateLimiterMinimumWait` 验证了此行为（见后文）

**关键点 3：isInf 优化**

```go
rl.isInf.Store(math.IsInf(float64(rate), 1))
```

**检查条件**：
```go
math.IsInf(rate, 1)  // rate == +∞
```

**效果**：
- 如果 `rate = Inf()`，所有 `WaitN/AdmitN` 调用直接返回，无需访问 TokenBucket
- 用于测试或禁用限流的场景

### 3.2 `WaitN()` - 阻塞等待配额

**函数签名**：
```go
func (rl *RateLimiter) WaitN(ctx context.Context, n int64) error
```

**输入**：
- `ctx`：上下文（支持取消和超时）
- `n`：请求的 token 数量

**返回**：
- `error`：成功返回 `nil`，取消或超时返回 `ctx.Err()`

**核心逻辑**：

```go
func (rl *RateLimiter) WaitN(ctx context.Context, n int64) error {
    // Fast path 1: 零请求
    if n == 0 {
        return nil
    }

    // Fast path 2: 无限速率
    if rl.isInf.Load() {
        return nil
    }

    // Step 1: 从 sync.Pool 获取 rateRequest
    r := rl.newRateRequest(n)
    defer rl.putRateRequest(r)

    // Step 2: 调用 AbstractPool.Acquire
    if err := rl.qp.Acquire(ctx, r); err != nil {
        return err
    }

    return nil
}
```

**为什么使用 defer？**
```go
r := rl.newRateRequest(n)
defer rl.putRateRequest(r)
```
- 保证 `rateRequest` 一定被放回 sync.Pool
- 即使 `Acquire` 返回 error（如 context 取消），也能正确清理

**`rateRequest.Acquire()` 的实现**：

```go
func (i *rateRequest) Acquire(
    ctx context.Context, res Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
    tb := res.(*TokenBucket)
    return tb.TryToFulfill(tokenbucket.Tokens(i.want))
}
```

**交互流程**：
```
WaitN(ctx, 50)
    ↓
newRateRequest(50) → {want: 50}
    ↓
qp.Acquire(ctx, request)
    ↓
request.Acquire(res) → tb.TryToFulfill(50)
    ├─> fulfilled=true, tryAgainAfter=0 → 立即返回
    └─> fulfilled=false, tryAgainAfter=500ms
        └─> AbstractPool 创建 Timer(500ms)
        └─> select { case <-timer / case <-ctx.Done() }
        └─> 超时后重试 TryToFulfill
```

### 3.3 `Acquire()` - 获取可归还配额

**函数签名**：
```go
func (rl *RateLimiter) Acquire(ctx context.Context, n int64) (*RateAlloc, error)
```

**返回**：
- `*RateAlloc`：可归还的配额对象
- `error`：失败时的错误

**核心逻辑**：

```go
func (rl *RateLimiter) Acquire(ctx context.Context, n int64) (*RateAlloc, error) {
    // Step 1: 先等待 token 可用
    if err := rl.WaitN(ctx, n); err != nil {
        return nil, err
    }

    // Step 2: 创建 RateAlloc
    return (*RateAlloc)(rl.newRateAlloc(n)), nil
}
```

**为什么分两步？**
1. **复用 WaitN 逻辑**：等待部分完全一样
2. **简化错误处理**：如果 WaitN 失败，无需创建 RateAlloc

**RateAlloc 的方法**：

```go
// Return 归还配额
func (ra *RateAlloc) Return() {
    ra.rl.qp.Update(func(res Resource) (shouldNotify bool) {
        tb := res.(*TokenBucket)
        tb.Adjust(tokenbucket.Tokens(ra.alloc))  // 归还 token
        return true  // 通知等待者
    })
    ra.rl.putRateAlloc((*rateAlloc)(ra))
}

// Consume 消耗配额（不归还）
func (ra *RateAlloc) Consume() {
    ra.rl.putRateAlloc((*rateAlloc)(ra))
}
```

**关键设计**：
- `Return()` 调用 `Update()`，确保归还操作原子完成
- `shouldNotify=true` 会唤醒等待中的请求
- `Consume()` 只回收对象，不归还 token

### 3.4 `AdmitN()` - 非阻塞尝试

**函数签名**：
```go
func (rl *RateLimiter) AdmitN(n int64) bool
```

**返回**：
- `bool`：成功返回 `true`，失败返回 `false`

**核心逻辑**：

```go
func (rl *RateLimiter) AdmitN(n int64) bool {
    // Fast path: 无限速率
    if rl.isInf.Load() {
        return true
    }

    // Step 1: 创建请求
    r := rl.newRateRequest(n)
    defer rl.putRateRequest(r)

    // Step 2: 使用 rateRequestNoWait
    return rl.qp.Acquire(context.Background(), (*rateRequestNoWait)(r)) == nil
}
```

**关键：`rateRequestNoWait` 类型**

```go
type rateRequestNoWait rateRequest

func (r *rateRequestNoWait) Acquire(
    ctx context.Context, resource Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
    // 复用 rateRequest.Acquire
    return (*rateRequest)(r).Acquire(ctx, resource)
}

func (r *rateRequestNoWait) ShouldWait() bool {
    return false  // 关键！告诉 AbstractPool 不要等待
}
```

**AbstractPool 的处理逻辑**（简化）：
```go
func (qp *AbstractPool) Acquire(ctx context.Context, req Request) error {
    fulfilled, tryAgainAfter := req.Acquire(qp.resource)

    if fulfilled {
        return nil
    }

    // 关键检查
    if !req.ShouldWait() {
        return ErrNotEnoughQuota  // 直接返回失败
    }

    // 创建 Timer 等待（WaitN 的路径）
    timer := time.NewTimer(tryAgainAfter)
    // ...
}
```

**为什么不直接用 context.WithTimeout？**
```go
// 不推荐
ctx, cancel := context.WithTimeout(context.Background(), 0)
defer cancel()
rl.WaitN(ctx, n)

// 当前实现（推荐）
rl.AdmitN(n)
```
- `AdmitN` 避免了创建 Timer 的开销
- 语义更清晰（"尝试"vs"等待超时"）

### 3.5 `UpdateLimit()` - 动态调整配置

**函数签名**：
```go
func (rl *RateLimiter) UpdateLimit(rate Limit, burst int64)
```

**核心逻辑**：

```go
func (rl *RateLimiter) UpdateLimit(rate Limit, burst int64) {
    rl.qp.Update(func(res Resource) (shouldNotify bool) {
        // Step 1: 更新 isInf 标志
        rl.isInf.Store(math.IsInf(float64(rate), 1))

        // Step 2: 更新 TokenBucket 配置
        tb := res.(*TokenBucket)
        tb.UpdateConfig(tokenbucket.TokensPerSecond(rate), tokenbucket.Tokens(burst))

        // Step 3: 通知等待者
        return true
    })
}
```

**TokenBucket.UpdateConfig() 的内部逻辑**（简化）：

```go
func (tb *TokenBucket) UpdateConfig(newRate TokensPerSecond, newBurst Tokens) {
    tb.mu.Lock()
    defer tb.mu.Unlock()

    // Step 1: 先补充 token（基于旧速率）
    tb.advance(tb.timeSource.Now())

    // Step 2: 计算 burst 变化
    burstDelta := newBurst - tb.burst

    // Step 3: 调整当前可用 token
    tb.availTokens += burstDelta

    // Step 4: 更新配置
    tb.rate = newRate
    tb.burst = newBurst

    // Step 5: 确保不超过新 burst
    if tb.availTokens > tb.burst {
        tb.availTokens = tb.burst
    }
}
```

**关键场景分析**：

**场景 1：增加 burst**
```go
// 当前状态：rate=10, burst=100, availTokens=50
UpdateLimit(10, 200)

// TokenBucket 内部：
burstDelta = 200 - 100 = +100
availTokens = 50 + 100 = 150

// 结果：立即释放了 100 个 token
```

**场景 2：减少 burst**
```go
// 当前状态：rate=10, burst=100, availTokens=90
UpdateLimit(10, 50)

// TokenBucket 内部：
burstDelta = 50 - 100 = -50
availTokens = 90 - 50 = 40

// 结果：扣除了 50 个 token
```

**场景 3：减少 burst 导致负债**
```go
// 当前状态：rate=10, burst=100, availTokens=80
UpdateLimit(10, 30)

// TokenBucket 内部：
burstDelta = 30 - 100 = -70
availTokens = 80 - 70 = 10

// 结果：availTokens=10（正常）

// 但如果：
// 当前状态：rate=10, burst=100, availTokens=20
UpdateLimit(10, 10)

// TokenBucket 内部：
burstDelta = 10 - 100 = -90
availTokens = 20 - 90 = -70  // 负债！

// 结果：availTokens=-70
// 后续请求需要等待 70/10 = 7 秒才能通过
```

**为什么允许负债？**
- 符合 Token Bucket 语义：如果之前允许突发，现在减少 burst，应该"还债"
- 保证配置变更的单调性（避免瞬间释放大量请求）

**shouldNotify=true 的作用**：
```go
return true  // 通知等待者
```
- AbstractPool 会唤醒所有等待中的请求
- 它们会使用新配置重新尝试 `TryToFulfill`
- 如果新配置更宽松（如增加 rate），请求可能提前满足

### 3.6 TokenBucket.TryToFulfill() - 核心算法

**函数签名**（简化）：
```go
func (tb *TokenBucket) TryToFulfill(want Tokens) (fulfilled bool, tryAgainAfter time.Duration)
```

**伪代码实现**：

```go
func (tb *TokenBucket) TryToFulfill(want Tokens) (bool, time.Duration) {
    tb.mu.Lock()
    defer tb.mu.Unlock()

    // Step 1: 补充 token（惰性补充）
    now := tb.timeSource.Now()
    elapsed := now.Sub(tb.lastUpdate)
    tokensToAdd := Tokens(tb.rate * TokensPerSecond(elapsed.Seconds()))

    tb.availTokens += tokensToAdd
    if tb.availTokens > tb.burst {
        tb.availTokens = tb.burst  // 不超过桶容量
    }
    tb.lastUpdate = now

    // Step 2: 尝试满足请求
    if tb.availTokens >= want {
        tb.availTokens -= want
        return true, 0
    }

    // Step 3: 计算等待时间
    shortage := want - tb.availTokens
    waitSeconds := float64(shortage) / float64(tb.rate)
    waitDuration := time.Duration(waitSeconds * float64(time.Second))

    return false, waitDuration
}
```

**关键点 1：惰性补充（Lazy Refill）**

```go
elapsed := now.Sub(tb.lastUpdate)
tokensToAdd := Tokens(tb.rate * elapsed.Seconds())
```

**为什么不定时补充？**
```go
// 不采用的方案：
go func() {
    ticker := time.NewTicker(time.Second / rate)
    for range ticker.C {
        tb.mu.Lock()
        tb.availTokens = min(tb.availTokens + 1, tb.burst)
        tb.mu.Unlock()
    }
}()
```

**当前方案的优势**：
- **零后台开销**：无 goroutine，无 ticker
- **精确计算**：基于精确的 elapsed 时间
- **高并发友好**：无定时器竞争

**关键点 2：等待时间计算**

```go
shortage := want - tb.availTokens
waitDuration := shortage / rate
```

**数学推导**：
```
假设：
  rate = 10 tokens/s
  availTokens = 5
  want = 20

shortage = 20 - 5 = 15
waitDuration = 15 / 10 = 1.5 秒

验证：
  1.5 秒后补充 token = 10 * 1.5 = 15
  availTokens = 5 + 15 = 20（刚好满足）
```

**关键点 3：非常小的等待时间处理**

```go
// 测试用例：TestRateLimiterWithVerySmallDelta
rate = 2e9  // 20亿 token/秒
want = 1
availTokens = 0

waitDuration = 1 / (2e9) = 0.5 纳秒
```

**问题**：Go 的 Timer 精度有限（通常 1 微秒），0.5 纳秒无法精确等待。

**解决方案**：`WithMinimumWait` 选项
```go
rl := NewRateLimiter(..., WithMinimumWait(time.Nanosecond))

// AbstractPool 内部：
if waitDuration < minimumWait {
    waitDuration = minimumWait
}
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 四、运行时行为与系统反馈（Runtime Behavior）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 4.1 系统如何感知运行时信号

#### 信号 1：Token 不足

**检测点**：`TryToFulfill()` 返回 `fulfilled=false`

```go
fulfilled, waitDuration := tb.TryToFulfill(want)
if !fulfilled {
    // 系统感知到："token 不足，需要等待"
    timer := time.NewTimer(waitDuration)
    select {
    case <-timer.C:
        // 等待结束，重试
    case <-ctx.Done():
        // 用户取消
    }
}
```

**反馈机制**：
- **即时**：检测到不足时立即返回 `waitDuration`
- **局部**：每个请求独立计算等待时间
- **精确**：基于数学公式计算，无估算误差

#### 信号 2：配置变更

**检测点**：`UpdateLimit()` 调用

```go
UpdateLimit(newRate, newBurst)
    └─> qp.Update(func(res Resource) bool {
            // 更新配置
            return true  // shouldNotify
        })
```

**反馈机制**：
- **主动通知**：`shouldNotify=true` 唤醒所有等待者
- **全局**：影响所有等待中的请求
- **即时**：配置变更立即生效

#### 信号 3：配额归还

**检测点**：`RateAlloc.Return()`

```go
ra.Return()
    └─> qp.Update(func(res Resource) bool {
            tb.Adjust(+alloc)
            return true  // shouldNotify
        })
```

**反馈机制**：
- **被动触发**：由用户代码主动归还
- **即时**：归还的 token 立即可用
- **唤醒等待**：通知等待者重新尝试

### 4.2 信号如何影响决策

#### 场景 1：突发流量

```
T0: availTokens=100, burst=100

T1: 10 个请求同时到达，每个请求 10 token
    ├─> 前 10 个请求：立即通过（availTokens=0）
    ├─> 第 11 个请求：
    │   └─> TryToFulfill(10)
    │       └─> availTokens=0 < 10
    │       └─> waitDuration = 10 / rate
    │
    └─> 策略：拒绝（如果用 AdmitN）或等待（如果用 WaitN）

决策：
- 短期突发：允许（前 10 个请求）
- 持续突发：限制（第 11 个及以后）
```

#### 场景 2：配置增加 burst

```
T0: availTokens=50, burst=100, rate=10/s
    waiting: [req1(60), req2(60)]  // 两个请求等待中

T1: UpdateLimit(10, 200)
    ├─> burstDelta = +100
    ├─> availTokens = 50 + 100 = 150
    └─> shouldNotify=true
        ├─> 唤醒 req1
        │   └─> TryToFulfill(60) → 成功（availTokens=90）
        └─> 唤醒 req2
            └─> TryToFulfill(60) → 成功（availTokens=30）

决策：
- 配置放宽：立即释放等待的请求
- 避免延迟：无需等待 token 自然补充
```

#### 场景 3：配置减少 burst

```
T0: availTokens=80, burst=100, rate=10/s

T1: UpdateLimit(10, 50)
    ├─> burstDelta = -50
    ├─> availTokens = 80 - 50 = 30
    └─> 后续请求受限

T2: WaitN(ctx, 40)
    └─> availTokens=30 < 40
    └─> waitDuration = (40-30) / 10 = 1 秒

决策：
- 配置收紧：立即减少可用配额
- 平滑过渡：通过负债机制避免瞬间释放
```

### 4.3 策略选择的权衡

#### 惰性补充 vs 定时补充

**当前策略**：惰性补充（TryToFulfill 时计算）

**优势**：
- **零后台开销**：无 goroutine
- **精确**：基于实际时间间隔
- **公平**：无定时器调度误差

**劣势**：
- **需要锁**：每次 TryToFulfill 都获取锁
- **计算开销**：每次都计算 elapsed

**为什么选择惰性？**
- 请求频率远高于补充频率（如 rate=10/s，但请求可能每秒数千次）
- 定时补充会引入额外的定时器开销和调度延迟

#### 即时唤醒 vs 延迟唤醒

**当前策略**：即时唤醒（UpdateLimit 和 Return 时 `shouldNotify=true`）

**优势**：
- **低延迟**：配置变更或归还后立即生效
- **响应快**：等待者无需等到下次 Timer 触发

**劣势**：
- **可能浪费**：如果归还的 token 很少，唤醒可能失败（然后继续等待）
- **锁竞争**：多个等待者同时被唤醒，竞争 TokenBucket 的锁

**为什么选择即时？**
- CockroachDB 的场景下，配置变更和归还都是"重要事件"
- 低延迟优先于效率（数据库内核的一般原则）

#### 允许负债 vs 拒绝负债

**当前策略**：允许负债（availTokens 可以为负）

**示例**：
```go
availTokens = -50  // 负债 50 个 token
waitDuration = 50 / rate  // 需要等待 50/rate 秒才能归零
```

**优势**：
- **配置变更平滑**：减少 burst 不会瞬间释放积压请求
- **语义一致**：Token Bucket 算法的标准行为

**劣势**：
- **恢复时间长**：如果负债过多，需要很长时间才能恢复
- **可能误导**：用户可能不理解为什么"桶已满"但请求仍被拒绝

**为什么允许？**
- 符合 Token Bucket 标准语义
- CockroachDB 的配置变更场景需要平滑过渡

### 4.4 性能特性

#### 吞吐量分析

**无限速率时**：
```go
if rl.isInf.Load() {  // 1 次原子读
    return nil
}
```
- **延迟**：~1-5 纳秒（原子读）
- **吞吐量**：每秒数亿次（受限于 CPU）

**有限速率时（token 充足）**：
```go
TryToFulfill(want)
    └─> tb.mu.Lock()
    └─> 补充 token（算术运算）
    └─> 扣除 token
    └─> tb.mu.Unlock()
```
- **延迟**：~100-500 纳秒（锁 + 算术）
- **吞吐量**：每秒数百万次（受限于锁竞争）

**有限速率时（token 不足）**：
```go
time.NewTimer(waitDuration)
select { case <-timer.C / case <-ctx.Done() }
```
- **延迟**：waitDuration（可能数秒）
- **吞吐量**：受 rate 限制

#### 锁竞争分析

**TokenBucket 的锁持有时间**：
```go
tb.mu.Lock()
// 1. 读取 now（系统调用，~10ns）
// 2. 计算 elapsed（减法，~1ns）
// 3. 计算 tokensToAdd（乘法，~1ns）
// 4. 更新 availTokens（加法，~1ns）
// 5. 更新 lastUpdate（赋值，~1ns）
tb.mu.Unlock()

// 总计：~20-50 纳秒
```

**高并发场景**：
- 1000 个请求同时调用 `WaitN`
- 锁竞争导致串行化
- 总耗时：~20-50 微秒（可接受）

**为什么不使用无锁算法？**
- Token Bucket 的状态更新（availTokens + lastUpdate）需要原子性
- 无锁实现复杂度高，且收益有限（锁持有时间已经很短）

#### 内存占用

**每个 RateLimiter**：
```
RateLimiter: ~40 bytes
  - qp: 8 bytes (指针)
  - isInf: 8 bytes (atomic.Bool 实际占用)

AbstractPool: ~100 bytes
  - mu: 8 bytes
  - resource: 8 bytes (指针)
  - ... (其他字段)

TokenBucket: ~80 bytes
  - mu: 8 bytes
  - rate: 8 bytes (float64)
  - burst: 8 bytes (float64)
  - availTokens: 8 bytes (float64)
  - lastUpdate: 24 bytes (time.Time)
  - timeSource: 8 bytes (指针)

总计：~220 bytes per RateLimiter
```

**sync.Pool 的开销**：
```
rateRequest: 8 bytes
rateAlloc: 16 bytes

Pool 本身的开销可忽略（全局共享）
```

**结论**：内存占用极小，适合大量实例化（如每个 Store 一个 consistencyLimiter）。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 五、设计模式分析（Design Patterns）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 5.1 Token Bucket 模式（核心算法模式）

**模式识别**：
```go
type TokenBucket struct {
    rate        TokensPerSecond  // 补充速率
    burst       Tokens           // 桶容量
    availTokens Tokens           // 当前 token
    lastUpdate  time.Time        // 上次更新时间
}
```

**模式描述**：
- **经典流量控制算法**，广泛用于网络 QoS、API 限流等场景
- **核心思想**：令牌以固定速率放入桶中，请求消耗令牌，桶满时令牌溢出

**在代码中的体现**：
```go
// 惰性补充
elapsed := now.Sub(tb.lastUpdate)
tokensToAdd := rate * elapsed
availTokens += tokensToAdd
if availTokens > burst {
    availTokens = burst  // 溢出
}

// 消耗
if availTokens >= want {
    availTokens -= want
}
```

**为什么选择 Token Bucket？**
- **允许突发**：短时间内可以消耗最多 `burst` 个 token（如一次性检查多个 replica）
- **长期限速**：长期平均速率受 `rate` 限制
- **平滑控制**：比固定窗口（Fixed Window）算法更平滑，无窗口边界效应

**Token Bucket vs Leaky Bucket**：
| 特性 | Token Bucket（当前） | Leaky Bucket |
|-----|---------------------|--------------|
| **突发处理** | 允许（桶满时可立即消耗 burst） | 不允许（漏桶以固定速率流出） |
| **速率保证** | 长期平均 | 瞬时精确 |
| **实现复杂度** | 简单（记录 token 数量） | 需要队列 |
| **适用场景** | 后台任务限流 | 实时流量整形 |

**Token Bucket vs Fixed Window**：
```
Fixed Window（固定窗口）示例：
  窗口 1 (0-1s): 允许 10 个请求
  窗口 2 (1-2s): 允许 10 个请求

  问题：0.9s 时来 10 个请求，1.1s 时来 10 个请求
        → 200ms 内通过 20 个请求（突破限制）

Token Bucket（当前）：
  无论何时，最多消耗 burst 个 token
  → 保证任何时间段内的突发都受控
```

**在分布式系统中的地位**：
- **事实标准**：Token Bucket 是流量控制的 de facto standard
- **工业实践**：Google GFE、AWS API Gateway、Nginx 等都使用此算法
- **变种**：一些系统使用 Sliding Window（滑动窗口）或 Adaptive Token Bucket（自适应令牌桶）

### 5.2 Object Pool 模式（对象复用）

**模式识别**：
```go
var rateRequestSyncPool = sync.Pool{
    New: func() interface{} { return new(rateRequest) },
}

var rateAllocSyncPool = sync.Pool{
    New: func() interface{} { return new(rateAlloc) },
}
```

**模式描述**：
- **对象池模式**，复用短生命周期对象，减少 GC 压力
- Go 标准库 `sync.Pool` 提供的线程安全对象池

**在代码中的体现**：
```go
func (rl *RateLimiter) newRateRequest(v int64) *rateRequest {
    r := rateRequestSyncPool.Get().(*rateRequest)  // 从池中获取
    *r = rateRequest{want: v}                      // 重新初始化
    return r
}

func (rl *RateLimiter) putRateRequest(r *rateRequest) {
    *r = rateRequest{}             // 清零（避免内存泄漏）
    rateRequestSyncPool.Put(r)     // 放回池中
}
```

**为什么使用 Object Pool？**
- **高频分配**：`WaitN` 每次调用都需要一个 `rateRequest` 对象
- **生命周期短**：对象在一次调用内创建和销毁
- **GC 友好**：复用对象避免频繁分配，减少 GC 扫描

**关键设计点**：
1. **必须清零**：
```go
*r = rateRequest{}  // 防止对象状态泄漏
```
- 如果不清零，下次 Get 可能拿到脏数据

2. **使用 defer**：
```go
r := rl.newRateRequest(n)
defer rl.putRateRequest(r)  // 保证一定放回
```
- 即使 panic 或 error，也能正确清理

**性能收益**：
```
压测数据（假设）：
  无 Pool：每秒 100万次 WaitN → 100万次分配 → GC 频繁
  有 Pool：每秒 100万次 WaitN → ~0次分配 → GC 几乎无压力

实测延迟：
  无 Pool：P99 = 50µs（包含 GC pause）
  有 Pool：P99 = 20µs
```

**注意事项**：
- `sync.Pool` 不保证对象一定存在（GC 时可能被清空）
- 只适合无状态或可重置的对象

### 5.3 Strategy 模式（隐式）

**模式识别**：
```go
// 不同的请求策略
type rateRequest struct { want int64 }       // 可等待
type rateRequestNoWait rateRequest           // 不可等待

func (r *rateRequest) ShouldWait() bool      { return true }
func (r *rateRequestNoWait) ShouldWait() bool { return false }
```

**模式描述**：
- **策略模式**，通过类型区分不同的行为策略
- Go 中通过方法实现（而非显式 interface，虽然实际遵循 `Request` interface）

**在代码中的体现**：
```go
// WaitN 使用 rateRequest（阻塞策略）
rl.qp.Acquire(ctx, &rateRequest{want: n})

// AdmitN 使用 rateRequestNoWait（非阻塞策略）
rl.qp.Acquire(ctx, (*rateRequestNoWait)(&rateRequest{want: n}))
```

**AbstractPool 的策略判断**：
```go
if !req.ShouldWait() {
    // 非阻塞策略：立即返回失败
    return ErrNotEnoughQuota
}
// 阻塞策略：创建 Timer 等待
```

**为什么使用 Strategy？**
- **代码复用**：`WaitN` 和 `AdmitN` 的 token 获取逻辑完全一样
- **灵活扩展**：未来可以增加更多策略（如 `TryWaitWithTimeout`）
- **类型安全**：编译期检查，不会误用

**改进建议**：
```go
// 当前实现：通过类型转换区分策略
(*rateRequestNoWait)(r)

// 更清晰的实现（未采用）：
type RequestOption int
const (
    Blocking RequestOption = iota
    NonBlocking
)

request := &rateRequest{want: n, option: NonBlocking}
```
- 当前实现更 Go 风格（类型即策略）
- 备选方案更 OOP 风格（配置即策略）

### 5.4 Resource 抽象模式（Dependency Injection）

**模式识别**：
```go
// AbstractPool 依赖 Resource 接口
type Resource interface {
    // 具体方法由实现定义
}

// TokenBucket 实现 Resource
type TokenBucket struct { ... }

// RateLimiter 注入 TokenBucket
rl.qp = New(name, tb, options...)
```

**模式描述**：
- **依赖注入 + 资源抽象**
- AbstractPool 不关心 Resource 的具体实现，只通过 `Request.Acquire(res)` 调用

**在代码中的体现**：
```go
// Request 接口
type Request interface {
    Acquire(ctx context.Context, res Resource) (fulfilled bool, tryAgainAfter time.Duration)
    ShouldWait() bool
}

// rateRequest 实现
func (r *rateRequest) Acquire(ctx context.Context, res Resource) (bool, time.Duration) {
    tb := res.(*TokenBucket)  // 类型断言
    return tb.TryToFulfill(Tokens(r.want))
}
```

**为什么使用 Resource 抽象？**
- **解耦**：AbstractPool 可以用于任何资源管理（不仅是 RateLimiter）
- **可测试**：可以注入 Mock Resource 进行测试
- **可扩展**：未来可以有 `IntPool`、`BytePool` 等不同实现

**实际使用场景**：
```go
// quotapool 包中的其他用途
intPool := NewIntPool(name, initialQuota)  // Resource = IntAlloc
// 用于并发数限制（如 MultiQueue）
```

**权衡**：
- **类型安全性降低**：使用 `interface{}` 和类型断言
- **灵活性提升**：同一套框架支持多种资源类型

### 5.5 Template Method 模式（隐式）

**模式识别**：
```go
// AbstractPool.Acquire 是模板方法
func (qp *AbstractPool) Acquire(ctx context.Context, req Request) error {
    for {
        // Step 1: 尝试获取资源（由 Request.Acquire 实现）
        fulfilled, tryAgainAfter := req.Acquire(ctx, qp.resource)

        if fulfilled {
            return nil
        }

        // Step 2: 检查策略（由 Request.ShouldWait 实现）
        if !req.ShouldWait() {
            return ErrNotEnoughQuota
        }

        // Step 3: 等待（模板固定逻辑）
        timer := time.NewTimer(tryAgainAfter)
        select {
        case <-timer.C:
            continue  // 重试
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}
```

**模式描述**：
- **模板方法模式**，定义算法骨架，具体步骤由子类实现
- Go 中通过 interface 方法实现（而非继承）

**算法骨架**：
```
1. 尝试获取资源 → Request.Acquire()（可变）
2. 检查是否等待 → Request.ShouldWait()（可变）
3. 等待并重试 → Timer + select（固定）
```

**为什么使用 Template Method？**
- **统一流程**：所有资源获取都遵循相同的等待-重试逻辑
- **扩展性**：新增 Request 类型无需修改 AbstractPool
- **避免重复**：等待逻辑只在 AbstractPool 中实现一次

### 5.6 Observer 模式（变种：shouldNotify）

**模式识别**：
```go
func (rl *RateLimiter) UpdateLimit(rate Limit, burst int64) {
    rl.qp.Update(func(res Resource) bool {
        // 更新资源
        tb.UpdateConfig(...)
        return true  // shouldNotify=true
    })
}

// AbstractPool 内部
if shouldNotify {
    // 唤醒所有等待者
    notifyWaiters()
}
```

**模式描述**：
- **观察者模式**的简化版本
- 不是显式的 Subject-Observer 关系，而是通过 `shouldNotify` 标志触发通知

**在代码中的体现**：
```go
// Update 方法
func (qp *AbstractPool) Update(f func(Resource) bool) {
    qp.mu.Lock()
    shouldNotify := f(qp.resource)
    qp.mu.Unlock()

    if shouldNotify {
        // 唤醒等待的 goroutine
        // 实际实现中通过 condition variable 或 channel
    }
}
```

**为什么使用 Observer？**
- **解耦**：资源变更逻辑不需要知道有多少等待者
- **及时响应**：配置变更或配额归还时立即通知等待者
- **避免轮询**：等待者不需要主动检查资源状态

**与经典 Observer 的区别**：
| 特性 | 经典 Observer | 当前实现 |
|-----|--------------|---------|
| **注册** | 显式 register/unregister | 隐式（Acquire 时加入等待队列） |
| **通知** | Subject.Notify() | Update(...) 返回 shouldNotify |
| **回调** | Observer.Update() | 唤醒 goroutine，重试 Acquire |

### 5.7 Defensive Programming（防御性编程）

**模式识别**：
```go
func (rl *RateLimiter) putRateRequest(r *rateRequest) {
    *r = rateRequest{}  // 清零，防止内存泄漏
    rateRequestSyncPool.Put(r)
}

func (rl *RateLimiter) WaitN(ctx context.Context, n int64) error {
    if n == 0 {
        return nil  // 特殊情况处理
    }
    // ...
}
```

**模式描述**：
- **防御性编程**，预防潜在的错误和边界情况
- 不信任调用者，显式处理边界条件

**在代码中的体现**：

**1. 零请求处理**：
```go
if n == 0 {
    return nil  // 避免无意义的 Pool 操作
}
```

**2. 对象清零**：
```go
*r = rateRequest{}  // 防止下次 Get 时拿到脏数据
```

**3. isInf 快速路径**：
```go
if rl.isInf.Load() {
    return nil  // 避免访问 TokenBucket（可能未初始化）
}
```

**4. defer 保证清理**：
```go
r := rl.newRateRequest(n)
defer rl.putRateRequest(r)  // 即使 panic 也会执行
```

**为什么强调 Defensive Programming？**
- **生产环境稳定性**：数据库内核不能有未处理的 panic
- **调试友好**：边界情况显式处理，错误更容易定位
- **性能优化**：快速路径避免不必要的操作

### 5.8 模式总结

| 模式 | 类型 | 在代码中的形式 | 工程价值 |
|-----|------|--------------|---------|
| **Token Bucket** | 算法模式 | 显式实现 | 流量控制的事实标准 |
| **Object Pool** | 性能优化模式 | sync.Pool | 减少 GC 压力，提升吞吐 |
| **Strategy** | 行为模式 | 类型 + ShouldWait() | 复用代码，灵活扩展 |
| **Resource 抽象** | 结构模式 | interface + DI | 解耦，可测试，可扩展 |
| **Template Method** | 行为模式 | interface 方法 | 统一流程，避免重复 |
| **Observer（变种）** | 行为模式 | shouldNotify 标志 | 及时响应，避免轮询 |
| **Defensive Programming** | 工程模式 | 边界检查 + defer | 稳定性，可维护性 |

**关键洞察**：
- 代码**没有过度设计**：所有模式都有明确的工程目的
- **Go 风格**：通过 interface 和类型实现模式，而非继承
- **性能优先**：Object Pool、快速路径等优化无处不在

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 六、具体运行示例（Concrete Examples）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 6.1 正常场景：ConsistencyQueue 使用 RateLimiter

**背景设定**：
- 配置：`consistencyCheckRate = 10.0`（每秒 10 次）
- burst factor：`5.0`
- 最终：`rate = 10/s, burst = 50`

**时间线**：

```
═══════════════════════════════════════════════════════════════════
T0: 系统启动，创建 RateLimiter
═══════════════════════════════════════════════════════════════════
NewRateLimiter("ConsistencyQueue", 10.0, 50, WithMinimumWait(1ms))
    ├─> 创建 TokenBucket{rate=10/s, burst=50, availTokens=50}
    ├─> isInf.Store(false)
    └─> RateLimiter 就绪

State:
  availTokens = 50
  lastUpdate = T0

═══════════════════════════════════════════════════════════════════
T1 (T0 + 0ms): 第一次一致性检查请求
═══════════════════════════════════════════════════════════════════
consistencyQueue.process(replica1)
    ├─> rl.WaitN(ctx, 1)
    │   ├─> isInf.Load() = false → 正常路径
    │   ├─> newRateRequest(1)
    │   └─> qp.Acquire(ctx, request)
    │       └─> tb.TryToFulfill(Tokens(1))
    │           ├─> elapsed = 0ms
    │           ├─> tokensToAdd = 10 * 0 = 0
    │           ├─> availTokens = 50 (不变)
    │           ├─> availTokens >= 1 → fulfilled=true
    │           └─> availTokens = 50 - 1 = 49
    │
    └─> 检查成功，执行一致性验证

State:
  availTokens = 49
  lastUpdate = T0

═══════════════════════════════════════════════════════════════════
T2 (T0 + 100ms): 突发：10 个并发请求
═══════════════════════════════════════════════════════════════════
for i := 0; i < 10; i++ {
    go rl.WaitN(ctx, 1)
}

并发执行（假设串行化，实际有锁保护）：
    ├─> Request 1: availTokens=49 → 成功 → 48
    ├─> Request 2: availTokens=48 → 成功 → 47
    ├─> Request 3: availTokens=47 → 成功 → 46
    ├─> Request 4: availTokens=46 → 成功 → 45
    ├─> Request 5: availTokens=45 → 成功 → 44
    ├─> Request 6: availTokens=44 → 成功 → 43
    ├─> Request 7: availTokens=43 → 成功 → 42
    ├─> Request 8: availTokens=42 → 成功 → 41
    ├─> Request 9: availTokens=41 → 成功 → 40
    └─> Request 10: availTokens=40 → 成功 → 39

State:
  availTokens = 39
  lastUpdate = T0（TryToFulfill 时更新为 T2）

═══════════════════════════════════════════════════════════════════
T3 (T0 + 5秒): 长时间后的请求
═══════════════════════════════════════════════════════════════════
rl.WaitN(ctx, 1)
    └─> tb.TryToFulfill(1)
        ├─> elapsed = T3 - T2 = 5000ms
        ├─> tokensToAdd = 10 * 5 = 50
        ├─> availTokens = 39 + 50 = 89
        ├─> availTokens > burst → 截断为 50
        ├─> availTokens = 50 - 1 = 49
        └─> fulfilled=true

State:
  availTokens = 49
  lastUpdate = T3

说明：
- 经过 5 秒，token 已完全恢复到 burst 上限
- 后续请求可以再次突发

═══════════════════════════════════════════════════════════════════
T4 (T3 + 1ms): 超大请求（超过 burst）
═══════════════════════════════════════════════════════════════════
rl.WaitN(ctx, 60)  // 请求 60 个 token（超过 burst=50）
    └─> tb.TryToFulfill(60)
        ├─> elapsed = 1ms
        ├─> tokensToAdd = 10 * 0.001 = 0.01 ≈ 0
        ├─> availTokens = 49
        ├─> availTokens < 60 → fulfilled=false
        ├─> shortage = 60 - 49 = 11
        ├─> waitDuration = 11 / 10 = 1.1 秒
        └─> 返回 (false, 1.1s)

AbstractPool 处理：
    ├─> 创建 Timer(1.1s)
    ├─> select {
    │       case <-timer.C:  // 1.1 秒后
    │           // 重新 TryToFulfill
    │       case <-ctx.Done():
    │           return ctx.Err()
    │   }
    │
    └─> 1.1秒后重试：
        └─> tb.TryToFulfill(60)
            ├─> elapsed = 1.1s
            ├─> tokensToAdd = 10 * 1.1 = 11
            ├─> availTokens = 49 + 11 = 60
            ├─> availTokens >= 60 → fulfilled=true
            └─> availTokens = 60 - 60 = 0

State:
  availTokens = 0
  lastUpdate = T4 + 1.1s

说明：
- 请求超过 burst 时，系统会等待桶"充满"
- 等待时间精确计算，无需轮询
```

### 6.2 边界场景：配额归还

**场景**：任务取消导致配额归还

```
═══════════════════════════════════════════════════════════════════
T0: 初始状态
═══════════════════════════════════════════════════════════════════
State: availTokens = 10, rate = 10/s, burst = 50

═══════════════════════════════════════════════════════════════════
T1: 预留配额（可能取消的任务）
═══════════════════════════════════════════════════════════════════
alloc, err := rl.Acquire(ctx, 5)
    ├─> WaitN(ctx, 5) → 成功
    └─> newRateAlloc(5) → RateAlloc{alloc: 5, rl: ...}

State: availTokens = 5, 持有 RateAlloc{alloc: 5}

═══════════════════════════════════════════════════════════════════
T2 (T1 + 100ms): 另一个请求尝试获取
═══════════════════════════════════════════════════════════════════
rl.WaitN(ctx, 10)
    └─> tb.TryToFulfill(10)
        ├─> elapsed = 0.1s
        ├─> tokensToAdd = 10 * 0.1 = 1
        ├─> availTokens = 5 + 1 = 6
        ├─> availTokens < 10 → 需等待
        ├─> shortage = 10 - 6 = 4
        ├─> waitDuration = 4 / 10 = 0.4s
        └─> 进入等待

Goroutine 状态：
  Waiting: [req(10, waitUntil=T2+0.4s)]

═══════════════════════════════════════════════════════════════════
T3 (T1 + 200ms): 任务取消，归还配额
═══════════════════════════════════════════════════════════════════
alloc.Return()
    ├─> qp.Update(func(res Resource) bool {
    │       tb.Adjust(Tokens(5))  // 归还 5 个 token
    │       return true  // shouldNotify
    │   })
    │
    └─> TokenBucket.Adjust(+5)
        └─> availTokens = 6 + 5 = 11

State: availTokens = 11

AbstractPool 响应：
    ├─> shouldNotify=true → 唤醒所有等待者
    └─> 等待的 req(10) 被唤醒
        └─> 重新 TryToFulfill(10)
            ├─> availTokens = 11 >= 10 → 成功
            └─> availTokens = 11 - 10 = 1

State: availTokens = 1

说明：
- 归还的 token 立即可用
- 等待者无需等到原定时间（T2+0.4s），而是立即被唤醒
- 节省了 200ms 的等待时间（原本需等到 T2+0.4s）
```

### 6.3 压力场景：配置动态调整

**场景**：运行时降低速率限制

```
═══════════════════════════════════════════════════════════════════
T0: 初始配置（高速率）
═══════════════════════════════════════════════════════════════════
rate = 100/s, burst = 500
State: availTokens = 500

═══════════════════════════════════════════════════════════════════
T1: 大量请求消耗配额
═══════════════════════════════════════════════════════════════════
for i := 0; i < 10; i++ {
    rl.WaitN(ctx, 50)  // 每个请求 50 token
}

State: availTokens = 500 - 500 = 0

═══════════════════════════════════════════════════════════════════
T2 (T1 + 100ms): 新请求到达并等待
═══════════════════════════════════════════════════════════════════
go rl.WaitN(ctx, 50)  // 请求 A
    └─> tb.TryToFulfill(50)
        ├─> tokensToAdd = 100 * 0.1 = 10
        ├─> availTokens = 0 + 10 = 10
        ├─> availTokens < 50 → 需等待
        ├─> waitDuration = (50 - 10) / 100 = 0.4s
        └─> 等待到 T2 + 0.4s

Waiting: [reqA(50, waitUntil=T2+0.4s)]

═══════════════════════════════════════════════════════════════════
T3 (T2 + 200ms): 管理员降低速率限制
═══════════════════════════════════════════════════════════════════
// DBA 执行：SET CLUSTER SETTING kv.range_consistency_check.rate = 10.0
// 触发回调
rl.UpdateLimit(10.0, 50)  // rate: 100→10, burst: 500→50

UpdateLimit 执行：
    ├─> qp.Update(func(res Resource) bool {
    │       tb.UpdateConfig(10, 50)
    │       return true
    │   })
    │
    └─> TokenBucket.UpdateConfig(10, 50)
        ├─> elapsed = T3 - T1 = 300ms
        ├─> tokensToAdd = 100 * 0.3 = 30
        ├─> availTokens = 10 + 30 = 40
        ├─> burstDelta = 50 - 500 = -450
        ├─> availTokens = 40 - 450 = -410（负债！）
        ├─> rate = 10
        └─> burst = 50

State: availTokens = -410, rate = 10/s, burst = 50

AbstractPool 响应：
    ├─> shouldNotify=true → 唤醒 reqA
    └─> reqA 重新 TryToFulfill(50)
        ├─> availTokens = -410 < 50 → 仍不满足
        ├─> shortage = 50 - (-410) = 460
        ├─> waitDuration = 460 / 10 = 46 秒
        └─> 重新等待（等到 T3 + 46s）

Waiting: [reqA(50, waitUntil=T3+46s)]

说明：
- 配置突然收紧导致系统进入负债状态
- 负债 410 个 token，需要 41 秒才能恢复到 0
- reqA 需要额外等待 46 秒（而非原来的 0.2 秒）
- 这是预期行为：防止之前的"高速率"导致的积压立即释放

═══════════════════════════════════════════════════════════════════
T4 (T3 + 46秒): reqA 满足
═══════════════════════════════════════════════════════════════════
    └─> tb.TryToFulfill(50)
        ├─> elapsed = 46s
        ├─> tokensToAdd = 10 * 46 = 460
        ├─> availTokens = -410 + 460 = 50
        ├─> availTokens >= 50 → fulfilled=true
        └─> availTokens = 50 - 50 = 0

State: availTokens = 0

说明：
- 系统经过 46 秒"还债"，恢复正常
- 后续请求使用新配置（rate=10/s）
```

### 6.4 特殊场景：极小等待时间

**场景**：超高速率导致等待时间小于 Timer 精度

```
═══════════════════════════════════════════════════════════════════
T0: 配置超高速率
═══════════════════════════════════════════════════════════════════
rate = 2e9 (20亿/秒), burst = 1e10 (100亿)
rl := NewRateLimiter("test", 2e9, 1e10, WithMinimumWait(1µs))

State: availTokens = 1e10

═══════════════════════════════════════════════════════════════════
T1: 消耗大部分配额
═══════════════════════════════════════════════════════════════════
rl.WaitN(ctx, 1e10)  // 消耗所有 token
State: availTokens = 0

═══════════════════════════════════════════════════════════════════
T2: 请求 1 个 token
═══════════════════════════════════════════════════════════════════
rl.WaitN(ctx, 1)
    └─> tb.TryToFulfill(1)
        ├─> availTokens = 0
        ├─> shortage = 1
        ├─> waitDuration = 1 / 2e9 = 0.5 纳秒
        └─> 返回 (false, 0.5ns)

AbstractPool 处理：
    ├─> 检查 minimumWait = 1µs
    ├─> waitDuration = 0.5ns < 1µs
    ├─> 调整：waitDuration = 1µs
    └─> 创建 Timer(1µs)

实际等待时间：1 微秒（而非 0.5 纳秒）

说明：
- 如果没有 minimumWait，Timer(0.5ns) 无法精确执行
- minimumWait 避免了"忙等待"或计时器误差
- 对于超高速率场景，这是必要的保护机制

测试验证（int_rate_test.go:234-262）：
    require.EqualValues(t, t0.Add(time.Microsecond), mt.Timers()[0])
    // 验证实际等待时间是 1µs，而非 0.5ns
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 七、设计取舍与替代方案（Trade-offs & Alternatives）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 7.1 当前设计的核心权衡

#### 权衡 1：Token Bucket vs Leaky Bucket

**当前选择**：Token Bucket

**替代方案**：Leaky Bucket（漏桶算法）
```go
// Leaky Bucket 伪代码
type LeakyBucket struct {
    queue    chan Request
    rate     time.Duration  // 每个请求的处理间隔
}

func (lb *LeakyBucket) Process() {
    ticker := time.NewTicker(lb.rate)
    for range ticker.C {
        <-lb.queue  // 固定速率处理
    }
}
```

**对比分析**：

| 维度 | Token Bucket（当前） | Leaky Bucket |
|-----|---------------------|--------------|
| **突发处理** | 允许（桶满时可突发 burst） | 不允许（严格匀速） |
| **延迟特性** | 低延迟（token 足够时立即返回） | 高延迟（必须排队等待） |
| **实现复杂度** | 简单（只需记录 token 数量） | 复杂（需要队列 + 定时器） |
| **后台开销** | 零（惰性补充） | 高（必须有 goroutine 处理队列） |
| **适用场景** | 后台任务、突发容忍 | 实时流量整形、严格匀速 |

**当前设计胜在**：
- **突发友好**：ConsistencyQueue 可能突然有大量 replica 需要检查，Token Bucket 允许短时间内处理完毕
- **零后台开销**：无需 goroutine 和 ticker

**当前设计输在**：
- **无法严格匀速**：如果需要"每 100ms 恰好 1 次"，Token Bucket 无法保证
- **可能瞬间过载**：如果 burst 很大，可能瞬间消耗大量资源

**为什么选择 Token Bucket？**
- CockroachDB 的一致性检查是**后台任务**，允许突发
- 需要**低延迟**（有 token 时立即执行，无需排队）
- **无需严格匀速**（只要长期平均速率受控即可）

#### 权衡 2：惰性补充 vs 定时补充

**当前选择**：惰性补充（TryToFulfill 时计算）

**替代方案**：定时补充
```go
// 定时补充伪代码
go func() {
    ticker := time.NewTicker(time.Second / rate)
    for range ticker.C {
        tb.mu.Lock()
        tb.availTokens = min(tb.availTokens + 1, tb.burst)
        tb.mu.Unlock()
    }
}()
```

**对比分析**：

| 维度 | 惰性补充（当前） | 定时补充 |
|-----|-----------------|---------|
| **后台开销** | 零（无 goroutine） | 高（1 goroutine + ticker） |
| **精度** | 精确（基于 elapsed 时间） | 取决于 ticker 精度 |
| **锁竞争** | 只在请求时 | 定时 + 请求时都需要锁 |
| **内存占用** | 零额外开销 | ticker + goroutine stack |
| **适用场景** | 请求驱动 | 持续流量 |

**当前设计胜在**：
- **零开销**：无 goroutine，无 ticker，完全事件驱动
- **精确**：基于精确的 `elapsed` 时间计算，无舍入误差
- **扩展性好**：可以实例化成千上万个 RateLimiter（如每个 queue 一个）

**当前设计输在**：
- **每次请求都需要计算**：`elapsed * rate`（虽然开销极小）
- **需要持锁**：TryToFulfill 必须获取锁

**为什么选择惰性？**
- CockroachDB 中可能有**数百个 RateLimiter 实例**（每个 queue 一个）
- 定时补充会产生数百个后台 goroutine（资源浪费）
- 请求是**事件驱动**的，惰性补充更自然

#### 权衡 3：允许负债 vs 拒绝负债

**当前选择**：允许负债（availTokens 可以为负）

**替代方案**：拒绝负债
```go
// 拒绝负债伪代码
if newBurst < oldBurst {
    burstDelta := newBurst - oldBurst
    availTokens = max(0, availTokens + burstDelta)  // 不允许负债
}
```

**对比分析**：

| 场景 | 允许负债（当前） | 拒绝负债 |
|-----|-----------------|---------|
| **减少 burst** | availTokens 可能变负，需"还债" | availTokens 最小为 0 |
| **后续请求** | 必须等待"还债"完成 | 立即可以获取（如果 token > 0） |
| **配置变更平滑性** | 平滑（逐步收紧） | 突变（可能瞬间释放积压） |

**示例**：
```go
// 当前状态：availTokens=200, burst=500
UpdateLimit(rate, 100)  // 减少 burst 到 100

// 允许负债（当前）：
availTokens = 200 + (100 - 500) = -200
// 需要 20 秒（假设 rate=10）才能恢复到 0

// 拒绝负债（替代方案）：
availTokens = max(0, 200 - 400) = 0
// 立即恢复，后续请求可以通过
```

**当前设计胜在**：
- **配置变更平滑**：避免突然释放大量积压请求
- **符合 Token Bucket 语义**：桶容量减少应该"收回" token

**当前设计输在**：
- **恢复时间长**：如果负债严重，可能需要很长时间才能恢复
- **用户困惑**：管理员可能不理解为什么"增加速率"后请求仍被限制

**为什么允许负债？**
- **生产环境稳定性**：配置变更不应导致瞬间流量激增
- **符合算法标准**：Token Bucket 的标准实现都允许负债

#### 权衡 4：归还配额 vs 不可归还

**当前选择**：支持归还（Acquire + Return）

**替代方案**：只支持 WaitN（不可归还）

**对比分析**：

| 维度 | 支持归还（当前） | 不支持归还 |
|-----|-----------------|-----------|
| **API 复杂度** | 高（Acquire + Return + Consume） | 低（只有 WaitN） |
| **内存开销** | 需要 RateAlloc 对象 + sync.Pool | 零 |
| **使用场景** | 任务可能取消 | 任务必定执行 |
| **资源利用率** | 高（归还的 token 立即可用） | 低（已消耗的 token 不可恢复） |

**示例场景**：
```go
// 场景：一致性检查可能因 context 取消而中断

// 支持归还（当前）：
alloc, _ := rl.Acquire(ctx, 1)
defer func() {
    if canceled {
        alloc.Return()  // 归还 token
    } else {
        alloc.Consume()
    }
}()
doConsistencyCheck()

// 不支持归还（替代方案）：
rl.WaitN(ctx, 1)
doConsistencyCheck()
// 即使 canceled，token 也不会归还
```

**当前设计胜在**：
- **资源利用率高**：取消的任务归还配额，其他任务可以立即使用
- **灵活性**：支持"预留-执行-归还"模式

**当前设计输在**：
- **API 复杂**：用户需要理解 Acquire vs WaitN
- **对象分配**：需要分配 RateAlloc（虽然有 sync.Pool 优化）

**为什么支持归还？**
- **CockroachDB 场景常见**：后台任务经常因 context 取消、leader 切换等原因中断
- **资源宝贵**：一致性检查的 token 很宝贵（rate 很低），归还可以提升整体吞吐

#### 权衡 5：isInf 优化 vs 统一路径

**当前选择**：isInf 快速路径

**替代方案**：统一处理（无快速路径）
```go
// 统一路径伪代码
func (rl *RateLimiter) WaitN(ctx context.Context, n int64) error {
    // 无论 rate 是否为 Inf，都走 TokenBucket 路径
    return rl.qp.Acquire(ctx, rl.newRateRequest(n))
}
```

**对比分析**：

| 维度 | isInf 优化（当前） | 统一路径 |
|-----|-------------------|---------|
| **无限速率性能** | 极快（1 次原子读） | 慢（需要锁 + 算术） |
| **代码复杂度** | 高（需要维护 isInf） | 低（无特殊路径） |
| **正确性风险** | 中（isInf 和 rate 必须同步） | 低（无状态不一致风险） |

**性能差异**：
```
无限速率场景（测试环境常见）：
  isInf 优化：1-5 纳秒
  统一路径：100-500 纳秒

差异：~100 倍
```

**当前设计胜在**：
- **测试环境友好**：测试时通常设置 `rate=Inf()`，完全无开销
- **生产环境可选**：可以通过配置禁用限流（如紧急情况）

**当前设计输在**：
- **状态同步**：`UpdateLimit` 必须同时更新 isInf 和 TokenBucket
- **代码分支**：每个方法都有 `if isInf` 检查

**为什么选择 isInf 优化？**
- **测试频繁**：CockroachDB 的测试套件会频繁调用限流器
- **性能差异显著**：100 倍的差异在高频场景下很明显
- **同步简单**：`UpdateLimit` 一次调用即可同步

### 7.2 替代架构分析

#### 替代方案 1：基于 Semaphore 的并发限制

**实现思路**：
```go
type Semaphore struct {
    permits chan struct{}
}

func (s *Semaphore) Acquire(ctx context.Context) error {
    select {
    case <-s.permits:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (s *Semaphore) Release() {
    s.permits <- struct{}{}
}
```

**与 RateLimiter 对比**：

| 特性 | RateLimiter（当前） | Semaphore |
|-----|---------------------|-----------|
| **限制类型** | 速率限制（rate/s） | 并发数限制 |
| **突发** | 支持（burst） | 不支持 |
| **归还** | 支持 | 支持 |
| **动态调整** | 支持 | 复杂（需重建 channel） |
| **适用场景** | 后台任务速率控制 | 并发工作者数量控制 |

**结论**：Semaphore 适合**并发数限制**（如"最多 10 个并发请求"），RateLimiter 适合**速率限制**（如"每秒最多 10 次"）。

#### 替代方案 2：基于 time.Rate 的标准库实现

**实现思路**：
```go
import "golang.org/x/time/rate"

limiter := rate.NewLimiter(rate.Limit(10), 50)
limiter.Wait(ctx)  // 等待 1 个 token
```

**与当前实现对比**：

| 特性 | RateLimiter（当前） | golang.org/x/time/rate |
|-----|---------------------|----------------------|
| **归还配额** | 支持 | 不支持 |
| **抽象池集成** | 是（基于 AbstractPool） | 否 |
| **sync.Pool 优化** | 是 | 否 |
| **最小等待时间** | 支持（WithMinimumWait） | 不支持 |
| **适用场景** | 数据库内核集成 | 通用 rate limiting |

**为什么不使用标准库？**
1. **需要归还配额**：标准库不支持 `Acquire + Return`
2. **需要与 AbstractPool 集成**：统一资源管理框架
3. **需要定制化**：如 minimumWait、isInf 优化等

**结论**：标准库适合简单场景，CockroachDB 的需求更复杂。

#### 替代方案 3：分层限流器

**实现思路**：
```
┌─────────────────────────┐
│  Global RateLimiter     │  ← 全局速率限制（如每秒 1000 次）
└────────┬────────────────┘
         │
    ┌────┴────┬────────┐
    ↓         ↓        ↓
┌────────┐ ┌────────┐ ┌────────┐
│Queue 1 │ │Queue 2 │ │Queue 3 │  ← 每个队列独立限流
└────────┘ └────────┘ └────────┘
```

**优势**：
- **更细粒度控制**：全局 + 局部双重限制
- **防止单一队列霸占**：每个队列有自己的配额

**劣势**：
- **复杂度高**：需要管理多层限流器
- **配置困难**：需要调优每个队列的速率

**适用场景**：
- 多租户系统（每个租户一个队列）
- 需要严格 QoS 保证的系统

**当前实现的选择**：
- CockroachDB 当前使用**单层限流**（每个 queue 独立）
- 简单有效，满足当前需求

### 7.3 性能权衡分析

#### 时间复杂度对比

| 操作 | 当前实现 | 最优理论 | 差距原因 |
|-----|---------|---------|---------|
| WaitN（token 足够） | O(1) | O(1) | 已最优 |
| WaitN（token 不足） | O(wait time) | O(wait time) | 已最优 |
| Acquire | O(1) + Pool 分配 | O(1) | Pool 开销可忽略 |
| Return | O(1) + 唤醒 | O(1) | 唤醒开销取决于等待者数量 |
| UpdateLimit | O(1) + 唤醒所有 | O(1) | 唤醒所有等待者 |

**瓶颈分析**：
- **唤醒开销**：如果有 1000 个 goroutine 等待，UpdateLimit 会唤醒所有
- **改进方案**：批量唤醒，或只唤醒可能满足的请求

#### 空间复杂度对比

**当前实现**：
```
RateLimiter: ~220 bytes
每个等待的 goroutine: ~2KB（goroutine stack）

最坏情况（1000 个等待）：
  220 bytes + 1000 * 2KB ≈ 2 MB
```

**如果使用队列而非 goroutine**：
```
Request Queue: N * sizeof(Request)
  假设 Request = 64 bytes
  1000 个请求 = 64 KB

节省：~1.9 MB
```

**为什么不使用队列？**
- **代码复杂度**：需要管理队列 + 调度逻辑
- **goroutine 是 Go 的设计哲学**：lightweight，可以大量创建
- **实际场景**：ConsistencyQueue 不太可能有 1000 个等待（通常<10）

### 7.4 工程实践权衡

#### 当前设计的优势

1. **简洁**：~200 行代码，易于理解和维护
2. **高性能**：
   - isInf 快速路径
   - sync.Pool 复用
   - 惰性补充
3. **灵活**：支持归还、动态配置、最小等待时间
4. **集成良好**：基于 AbstractPool，与其他 quotapool 组件一致

#### 当前设计的不足

1. **负债恢复时间长**：配置收紧后可能需要很久才能恢复
2. **唤醒效率**：UpdateLimit 会唤醒所有等待者（可能浪费）
3. **监控缺失**：无内置指标（如当前 availTokens、等待者数量）
4. **文档不足**：代码注释较少，使用示例缺乏

#### 可能的改进方向

**1. 增加可观测性**
```go
type RateLimiter struct {
    // ... 现有字段
    metrics struct {
        totalAcquired   int64
        totalReturned   int64
        currentWaiters  int64
        avgWaitTime     time.Duration
    }
}
```

**2. 智能唤醒**
```go
// 只唤醒可能满足的请求
if shouldNotify {
    wakeUpN := availTokens / averageRequestSize
    notifyWaiters(wakeUpN)  // 而非唤醒所有
}
```

**3. 负债保护**
```go
// 限制负债上限
if availTokens < -maxDebt {
    availTokens = -maxDebt
}
```

### 7.5 总结：当前设计的适用边界

**最适合的场景**：
- 后台任务速率控制（如一致性检查、GC、compaction）
- 允许突发但限制长期速率
- 任务可能取消（需要归还配额）
- 需要动态调整速率

**不适合的场景**：
- 严格匀速要求（用 Leaky Bucket）
- 并发数限制（用 Semaphore）
- 前台请求限流（延迟敏感度高，Token Bucket 的等待可能不可接受）

**扩展方向**：
- **水平扩展**：分布式 rate limiting（基于 Redis 或 Raft）
- **自适应速率**：根据系统负载动态调整 rate

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 八、总结与心智模型（Mental Model & Summary）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 8.1 核心思想总结

**一句话概括**：
RateLimiter 是一个**基于 Token Bucket 算法的速率限制器**，通过**惰性 token 补充 + 可归还配额 + 动态配置**，实现了**突发容忍 + 长期限速 + 资源高效利用**的平衡。

**设计哲学**：
1. **算法经典**：Token Bucket 是流量控制的事实标准
2. **零后台开销**：惰性补充，完全事件驱动
3. **可归还配额**：支持任务取消场景，提升资源利用率
4. **动态配置**：运行时调整速率，无需重启

### 8.2 可复用的心智模型

**模型 1：水龙头与水桶（Token Bucket）**

```
想象一个水桶系统：
- 水桶容量：burst（如 50 升）
- 水龙头速率：rate（如每秒 10 升）
- 每次请求消耗若干升水
- 桶满时水溢出（不再补充）

规则：
1. 桶里有水 → 立即取水
2. 桶里没水 → 等待水龙头补充
3. 请求超过桶容量 → 等待桶"充满"后一次性取走

特点：
- 允许突发：桶满时可以一次取走 50 升
- 长期限速：长期平均速率 = 10 升/秒
- 平滑控制：水龙头持续补充，无"窗口边界"效应
```

**模型 2：预订与归还（Acquire + Return）**

```
想象一个图书馆的预订系统：
1. 读者预订书籍（Acquire）
   - 如果有库存 → 立即借出
   - 如果无库存 → 等待（计算等待时间）

2. 读者可能取消预订（Return）
   - 归还预订配额（其他人可以立即使用）

3. 读者借走书籍（Consume）
   - 不归还配额（真正消耗）

优势：
- 取消的预订不会浪费配额
- 系统资源利用率更高
```

**模型 3：负债与还债（Negative Tokens）**

```
想象一个信用卡系统：
- 初始额度：burst（如 $500）
- 每月还款：rate（如每月 $100）
- 当前余额：availTokens

场景：
1. 正常情况：availTokens = $200（有余额）
   - 可以立即消费

2. 欠款情况：availTokens = -$300（负债）
   - 需要等待 3 个月才能恢复到 $0
   - 期间无法消费

触发负债：
- 配置收紧（减少 burst）
  之前：burst=$500, availTokens=$400
  现在：burst=$100, availTokens=$400-$400=-$0（计算方式：$400 + ($100-$500)）
  → 负债 $300，需"还债"
```

### 8.3 设计原则提炼

从 RateLimiter 中可以提炼的通用设计原则：

**1. 惰性优于主动（Lazy over Eager）**
```
❌ 主动补充：定时器每秒补充 token
✅ 惰性补充：请求到达时计算并补充

优势：零后台开销，精确计算
```

**2. 事件驱动优于轮询（Event-driven over Polling）**
```
❌ 轮询等待：while (availTokens < want) { sleep(100ms) }
✅ 事件驱动：Timer + select（精确等待）

优势：低延迟，高效率
```

**3. 可归还优于一次性（Returnable over One-shot）**
```
❌ 一次性消耗：WaitN(n) → 无法归还
✅ 可归还配额：Acquire(n) → Return() 或 Consume()

优势：资源利用率高，适配任务取消场景
```

**4. 动态配置优于静态配置（Dynamic over Static）**
```
❌ 静态配置：rate 在初始化时固定
✅ 动态配置：UpdateLimit() 运行时调整

优势：无需重启，快速响应
```

**5. 快速路径优于统一路径（Fast Path over Unified Path）**
```
❌ 统一路径：所有请求都走 TokenBucket
✅ 快速路径：isInf=true 时直接返回

优势：测试环境零开销，生产环境可选
```

### 8.4 高度抽象的伪代码

```pseudo
class RateLimiter:
    state:
        tokenBucket: TokenBucket
        isInf: bool
        requestPool: sync.Pool

    function WaitN(ctx, n):
        // Fast path 1: 零请求
        if n == 0:
            return success

        // Fast path 2: 无限速率
        if isInf:
            return success

        // Normal path
        request = requestPool.Get()
        defer requestPool.Put(request)

        loop:
            // 尝试获取 token
            fulfilled, waitDuration = tokenBucket.TryToFulfill(n)

            if fulfilled:
                return success

            // 等待
            select:
                case <-Timer(waitDuration):
                    continue  // 重试
                case <-ctx.Done():
                    return ctx.Err()

    function Acquire(ctx, n):
        if err = WaitN(ctx, n):
            return error

        alloc = allocPool.Get()
        alloc.alloc = n
        return alloc

    function Return(alloc):
        tokenBucket.Adjust(+alloc.alloc)  // 归还 token
        notifyWaiters()                    // 唤醒等待者
        allocPool.Put(alloc)

    function UpdateLimit(newRate, newBurst):
        isInf = (newRate == Inf)
        tokenBucket.UpdateConfig(newRate, newBurst)
        notifyWaiters()  // 唤醒所有等待者

class TokenBucket:
    state:
        rate: float
        burst: float
        availTokens: float
        lastUpdate: Time

    function TryToFulfill(want):
        lock()
        defer unlock()

        // 1. 惰性补充
        now = Now()
        elapsed = now - lastUpdate
        tokensToAdd = rate * elapsed
        availTokens += tokensToAdd
        availTokens = min(availTokens, burst)
        lastUpdate = now

        // 2. 尝试满足
        if availTokens >= want:
            availTokens -= want
            return (true, 0)

        // 3. 计算等待时间
        shortage = want - availTokens
        waitDuration = shortage / rate
        return (false, waitDuration)

    function Adjust(delta):
        lock()
        availTokens += delta
        unlock()

    function UpdateConfig(newRate, newBurst):
        lock()
        // 先补充（基于旧速率）
        advance(Now())

        // 调整 availTokens
        burstDelta = newBurst - burst
        availTokens += burstDelta

        // 更新配置
        rate = newRate
        burst = newBurst
        unlock()
```

### 8.5 关键要点速查

**核心数据结构**：
- `RateLimiter`：外层封装，提供 API
- `TokenBucket`：核心算法，管理 token
- `RateAlloc`：可归还配额对象
- `rateRequest`：请求对象（sync.Pool 复用）

**核心操作流程**：
```
WaitN → TryToFulfill → (fulfilled 或 Timer 等待)
Acquire → WaitN + newRateAlloc
Return → Adjust + notifyWaiters
UpdateLimit → UpdateConfig + notifyWaiters
```

**核心不变量**：
1. `0 <= availTokens <= burst`（负债时可能 < 0）
2. `lastUpdate <= Now()`
3. `isInf == (rate == Inf())`

**核心模式**：
- Token Bucket（算法）
- Object Pool（优化）
- Strategy（行为）
- Resource 抽象（结构）
- Observer 变种（通知）

### 8.6 实战应用指导

**何时使用 RateLimiter**：
- 后台任务需要速率控制
- 允许短时间突发
- 任务可能取消（需要归还配额）
- 需要动态调整速率

**何时不使用 RateLimiter**：
- 需要严格匀速（用 Leaky Bucket）
- 并发数限制（用 Semaphore）
- 前台请求（延迟敏感，考虑其他方案）

**集成建议**：
```go
// 1. 创建 RateLimiter
rl := quotapool.NewRateLimiter(
    "MyQueue",
    10.0,  // rate: 10 次/秒
    50,    // burst: 50
    quotapool.WithMinimumWait(time.Millisecond),
)

// 2. 阻塞等待（任务必定执行）
if err := rl.WaitN(ctx, 1); err != nil {
    return err
}
doWork()

// 3. 可归还配额（任务可能取消）
alloc, err := rl.Acquire(ctx, 1)
if err != nil {
    return err
}
if canceled := doWork(); canceled {
    alloc.Return()  // 归还
} else {
    alloc.Consume()  // 消耗
}

// 4. 非阻塞尝试（快速失败）
if !rl.AdmitN(1) {
    return ErrRateLimited
}
doWork()

// 5. 动态调整
rl.UpdateLimit(20.0, 100)  // 增加速率和 burst
```

### 8.7 进一步学习路径

**相关技术**：
1. **流量控制算法**：Token Bucket、Leaky Bucket、Sliding Window
2. **并发控制**：Semaphore、Channel、sync.WaitGroup
3. **资源管理**：sync.Pool、Object Pool、Free List

**推荐阅读**：
1. Token Bucket 算法论文（Wikipedia）
2. Go time/rate 标准库源码
3. CockroachDB admission control 文档
4. Google SRE Book - Rate Limiting 章节

**扩展实验**：
1. 对比 Token Bucket vs Leaky Bucket 性能
2. 实现分布式 rate limiter（基于 Redis）
3. 压测不同 burst 配置的影响
4. 监控 RateLimiter 的运行时指标

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 附录：代码索引
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

**核心函数位置**：
- [NewRateLimiter](pkg/util/quotapool/int_rate.go#L40-L47)
- [WaitN](pkg/util/quotapool/int_rate.go#L60-L74)
- [Acquire](pkg/util/quotapool/int_rate.go#L51-L56)
- [AdmitN](pkg/util/quotapool/int_rate.go#L79-L87)
- [UpdateLimit](pkg/util/quotapool/int_rate.go#L95-L102)
- [RateAlloc.Return](pkg/util/quotapool/int_rate.go#L113-L120)
- [RateAlloc.Consume](pkg/util/quotapool/int_rate.go#L124-L126)

**测试用例参考**：
- [TestRateLimiterBasic](pkg/util/quotapool/int_rate_test.go#L21-L201)：基本功能验证
- [TestRateLimiterWithVerySmallDelta](pkg/util/quotapool/int_rate_test.go#L206-L232)：极小等待时间
- [TestRateLimiterMinimumWait](pkg/util/quotapool/int_rate_test.go#L235-L262)：最小等待时间验证

**核心依赖**：
- [TokenBucket](github.com/cockroachdb/tokenbucket)：底层 token bucket 算法
- [AbstractPool](pkg/util/quotapool/abstract.go)：通用资源池抽象

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**文档结束**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
