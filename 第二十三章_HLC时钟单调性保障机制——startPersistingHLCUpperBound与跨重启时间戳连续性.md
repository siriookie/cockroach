# 第二十三章 HLC 时钟单调性保障机制——startPersistingHLCUpperBound 与跨重启时间戳连续性

## 一、第一轮 BFS：整体职责与设计动机（Why）

### 1.1 背景与要解决的问题

分布式数据库的核心挑战之一是**维护全局时间的因果一致性**。CockroachDB 使用 **HLC（Hybrid Logical Clock，混合逻辑时钟）** 作为时间戳系统，用于：
- 事务排序（决定哪个事务先发生）
- Lease 有效性判断（谁持有某个 Range 的 lease）
- MVCC 版本控制（多版本并发控制）
- 分布式快照隔离

HLC 的核心不变量是：**时间戳必须单调递增，永远不能后退**。

**关键问题**：节点重启时，如何保证新生成的时间戳**不会小于**重启前生成的最大时间戳？

#### 典型的破坏场景

假设节点在重启前使用了时间戳 `T1 = 1000`：

```
时刻 T0: 节点运行中
  HLC.Now() = 1000
  客户端写入 key="foo", ts=1000

时刻 T1: 节点崩溃并重启
  系统时钟回拨（NTP 调整、虚拟机时间同步等）
  物理时钟 = 800

时刻 T2: 节点重启后
  HLC.Now() = 800  ← 小于 1000！
  客户端读取 key="foo" → 可能看不到 ts=1000 的写入
  → 违反了因果一致性！
```

**后果**：
1. **Lease 混乱**：节点 A 的 lease 过期时间为 `ts=1000`，但节点 B 重启后生成 `ts=800` 的 lease → 双 lease 问题
2. **事务异常**：`ts=900` 的事务可能读不到 `ts=1000` 的已提交数据 → 违反快照隔离
3. **MVCC 错乱**：新版本的 `ts=800` 可能被插入到旧版本 `ts=1000` 之前 → 数据损坏

### 1.2 在系统中的位置

```
Server.Start()
  ├─ PreStart()
  │   ├─ 初始化 HLC Clock
  │   └─ 打开存储引擎（Pebble）
  │
  ├─ checkHLCUpperBoundExistsAndEnsureMonotonicity()  // 重启时检查并等待
  │   ├─ ReadMaxHLCUpperBound() → 从所有 Store 读取上次持久化的 HLC 上界
  │   └─ ensureClockMonotonicity() → 如果物理时钟 < 上界，则 sleep 直到赶上
  │
  ├─ Node.start()
  │   └─ 启动 Store 和 Raft
  │
  ├─ startPersistingHLCUpperBound()  // ← 本章重点：启动定期持久化
  │   ├─ 启动后台 goroutine
  │   └─ 每隔 N 秒持久化 "HLC.Now() + 3*N" 作为上界
  │
  └─ AcceptClients()
```

**协作模块**：

| 模块 | 交互方式 | 目的 |
|------|---------|------|
| **HLC Clock** | `Clock.RefreshHLCUpperBound()` | 计算并持久化 HLC 上界 |
| **Store** | `Store.WriteHLCUpperBound()` | 将上界写入磁盘（系统键） |
| **Engine (Pebble)** | `MVCCPutProto()` + `sync` | 确保上界持久化到磁盘 |
| **Cluster Settings** | `persistHLCUpperBoundInterval` | 动态控制持久化间隔 |

### 1.3 核心对象与关键状态

#### 长期存在的结构体

```go
// pkg/util/hlc/hlc.go
type Clock struct {
    wallClock    WallClock      // 物理时钟（通常是 time.Now）
    maxOffset    time.Duration  // 集群节点间最大时钟偏差（如 500ms）

    // 上次测量的物理时间（用于检测时钟跳跃）
    lastPhysicalTime int64  // 原子访问

    mu struct {
        syncutil.Mutex
        timestamp ClockTimestamp  // 当前 HLC 时间戳

        // HLC 上界：已持久化的"未来时间"
        // 保证：所有通过 Now() 生成的时间戳 < wallTimeUpperBound
        wallTimeUpperBound int64
    }
}
```

#### 持久化存储

**系统键**：`keys.StoreHLCUpperBoundKey()`（每个 Store 一份）

```
磁盘上的布局（Pebble LSM）：
  Key:   "\x01k{storeID}hlc-ub\x00"  // hlc-ub = HLC upper bound
  Value: {WallTime: 1705435200000000000}  // 单位：纳秒
  Sync:  必须 fsync（保证崩溃后可见）
```

#### 关键状态变量

```go
// pkg/server/clock_monotonicity.go: periodicallyPersistHLCUpperBound()
persistInterval time.Duration  // 持久化间隔（由集群设置控制）
  默认值: 0（禁用）
  推荐值: 5s - 30s

ticker *time.Ticker  // 定时器（动态启停）
```

**核心不变量**：

1. **持久化不变量**：
   ```
   persisted_upper_bound >= max(所有已生成的 HLC.Now() 值)
   ```

2. **重启不变量**：
   ```
   重启后首个 HLC.Now() >= 上次持久化的 upper_bound
   ```

3. **Delta 不变量**：
   ```
   upper_bound = HLC.Now() + 3 * persistInterval
   ```
   （3 倍是为了容忍最多 2 次持久化失败）

---

## 二、第二轮 BFS：控制流与交互关系（How it flows）

### 2.1 主要执行路径

`startPersistingHLCUpperBound()` 是一个**异步启动函数**，在 `Server.Start()` 中被调用，位于以下时间点：

```go
// pkg/server/server.go: Server.Start()
func (s *Server) Start(ctx context.Context) error {
    // ... PreStart() 完成 ...

    // 1. 检查是否已有持久化的 HLC 上界
    hlcUpperBoundExists, err := s.checkHLCUpperBoundExistsAndEnsureMonotonicity(ctx, initialStart)
    if err != nil {
        return err
    }

    // 2. 启动节点（Store、Raft）
    if err := s.node.start(...); err != nil {
        return err
    }

    // 3. 启动 HLC 上界持久化（本章重点）
    if err := s.startPersistingHLCUpperBound(ctx, hlcUpperBoundExists); err != nil {
        return err
    }

    // 4. 开始接受客户端连接
    s.AcceptClients()
}
```

### 2.2 函数内部时间线

#### 阶段 0：初始化配置（L269-L283）

```go
func (s *topLevelServer) startPersistingHLCUpperBound(
    ctx context.Context,
    hlcUpperBoundExists bool,
) error {
    // 1. 定义 ticker 工厂函数（用于创建定时器）
    tickerFn := time.NewTicker

    // 2. 定义持久化函数（闭包）
    persistHLCUpperBoundFn := func(t int64) error {
        // 调用 Node.SetHLCUpperBound → Store.WriteHLCUpperBound
        return s.node.SetHLCUpperBound(context.Background(), t)
    }

    // 3. 创建通道，用于动态接收持久化间隔的变化
    persistHLCUpperBoundIntervalCh := make(chan time.Duration, 1)

    // 4. 初始化通道（发送当前设置的值）
    persistHLCUpperBoundIntervalCh <- persistHLCUpperBoundInterval.Get(&s.st.SV)

    // 5. 监听集群设置变化（当用户通过 SQL 修改设置时触发）
    persistHLCUpperBoundInterval.SetOnChange(&s.st.SV, func(context.Context) {
        persistHLCUpperBoundIntervalCh <- persistHLCUpperBoundInterval.Get(&s.st.SV)
    })
```

**设计要点**：
- 使用 **buffered channel**（容量 1），避免设置变化时阻塞
- `SetOnChange` 不会自动触发回调（L277-L279 注释），因此需要手动发送初始值

#### 阶段 1：立即持久化（如果已存在上界）（L285-L296）

```go
    if hlcUpperBoundExists {
        // 说明：
        // - hlcUpperBoundExists=true：磁盘上已有上界（从旧版本升级或重启）
        // - 目的：在后台 goroutine 启动前，立即持久化一个新上界
        // - 原因：确保从"重启等待"（ensureClockMonotonicity）到"定期持久化启动"
        //         之间的时间窗口（~10ms）也受到保护

        if err := s.clock.RefreshHLCUpperBound(
            persistHLCUpperBoundFn,
            int64(5*time.Second),  // delta = 5 秒（临时值，后台任务会用 3*interval）
        ); err != nil {
            return errors.Wrap(err, "refreshing HLC upper bound")
        }
    }
```

**Why 需要立即持久化？**

```
T0: checkHLCUpperBoundExistsAndEnsureMonotonicity()
    → 读取到 upper_bound = 1000
    → sleep 直到物理时钟 >= 1001

T1: ensureClockMonotonicity() 返回
    → 物理时钟 = 1001
    → HLC.Now() 可以安全生成 >= 1001 的时间戳

T2: startPersistingHLCUpperBound() 开始
    → 如果不立即持久化，HLC.Now() 可能生成 1002, 1003...
    → 但磁盘上的 upper_bound 仍是 1000
    → 如果此时崩溃 → 重启后又会回到 1000 → 违反单调性

T3: 立即持久化（hlcUpperBoundExists=true）
    → 持久化 upper_bound = HLC.Now() + 5s = 1001 + 5000ms = 6001
    → 即使崩溃，重启后也会等待到 6001
```

#### 阶段 2：启动后台 goroutine（L298-L312）

```go
    _ = s.stopper.RunAsyncTask(
        ctx,
        "persist-hlc-upper-bound",
        func(context.Context) {
            periodicallyPersistHLCUpperBound(
                s.clock,
                persistHLCUpperBoundIntervalCh,
                persistHLCUpperBoundFn,
                tickerFn,
                s.stopper.ShouldQuiesce(),  // 停止信号
                nil,                         // tick 回调（测试用）
            )
        },
    )
    return nil
}
```

**后台任务的生命周期**：
- 启动：`Server.Start()` 调用后立即启动
- 运行：持续运行直到节点关闭
- 停止：`s.stopper.ShouldQuiesce()` 通道关闭时退出

### 2.3 periodicallyPersistHLCUpperBound() 的事件循环

这是实际执行持久化的核心函数（L187-L257）：

```go
func periodicallyPersistHLCUpperBound(
    clock *hlc.Clock,
    persistHLCUpperBoundIntervalCh chan time.Duration,
    persistHLCUpperBoundFn func(int64) error,
    tickerFn func(d time.Duration) *time.Ticker,
    stopCh <-chan struct{},
    tickCallback func(),
) {
    // 1. 创建 ticker（初始为停止状态）
    ticker := tickerFn(time.Hour)
    ticker.Stop()

    var persistInterval time.Duration  // 当前持久化间隔

    // 2. 定义持久化逻辑
    persistHLCUpperBound := func() {
        if err := clock.RefreshHLCUpperBound(
            persistHLCUpperBoundFn,
            int64(persistInterval*3),  // delta = 3 倍间隔（关键设计）
        ); err != nil {
            log.Ops.Fatalf(
                context.Background(),
                "error persisting HLC upper bound: %v",
                err,
            )
        }
    }

    // 3. 事件循环
    for {
        select {
        case updatedPersistInterval := <-persistHLCUpperBoundIntervalCh:
            // 场景 1：持久化间隔发生变化
            if updatedPersistInterval == persistInterval {
                continue  // 无变化，跳过
            }
            persistInterval = updatedPersistInterval

            ticker.Stop()  // 停止旧 ticker

            if persistInterval > 0 {
                // 启用持久化
                ticker = tickerFn(persistInterval)
                persistHLCUpperBound()  // 立即执行一次
                log.Ops.Infof(ctx, "persisting HLC upper bound is enabled [every %.2fs]",
                    persistInterval.Seconds())
            } else {
                // 禁用持久化（间隔设为 0）
                if err := clock.ResetHLCUpperBound(persistHLCUpperBoundFn); err != nil {
                    log.Ops.Fatalf(ctx, "error resetting hlc upper bound: %v", err)
                }
                log.Ops.Info(ctx, "persisting HLC upper bound is disabled")
            }

        case <-ticker.C:
            // 场景 2：定时器触发
            if persistInterval > 0 {
                persistHLCUpperBound()
            }

        case <-stopCh:
            // 场景 3：节点停止
            ticker.Stop()
            return
        }

        if tickCallback != nil {
            tickCallback()  // 测试用回调
        }
    }
}
```

### 2.4 触发时机

**非定时触发**：
1. **服务器启动时**（L280）：发送初始间隔到通道
2. **集群设置变化时**（L281-L283）：用户执行 SQL：
   ```sql
   SET CLUSTER SETTING server.clock.persist_upper_bound_interval = '10s';
   ```
   → 触发 `SetOnChange` 回调 → 发送新间隔到通道

**定时触发**：
- **Ticker 触发**（L243）：每隔 `persistInterval` 触发一次
- 默认：禁用（`persistInterval = 0`）
- 推荐：5s - 30s

### 2.5 与其他组件的交互

#### 与 HLC Clock 的交互

```
periodicallyPersistHLCUpperBound()
  └─ clock.RefreshHLCUpperBound(persistFn, delta)
      ├─ 计算 upper_bound = clock.Now().WallTime + delta
      ├─ 调用 persistFn(upper_bound)  // 持久化到磁盘
      │   └─ s.node.SetHLCUpperBound(upper_bound)
      │       └─ n.stores.VisitStores(func(s *Store) {
      │           s.WriteHLCUpperBound(upper_bound)
      │       })
      │           └─ MVCCPutProto(StoreHLCUpperBoundKey, upper_bound)
      │               └─ batch.Commit(sync=true)  // fsync
      │
      └─ clock.mu.Lock()
          └─ clock.mu.wallTimeUpperBound = upper_bound  // 更新内存值
```

#### 与 Store 的交互

```go
// pkg/kv/kvserver/store.go: WriteHLCUpperBound()
func (s *Store) WriteHLCUpperBound(ctx context.Context, time int64) error {
    ts := hlc.Timestamp{WallTime: time}
    batch := s.LogEngine().NewBatch()
    defer batch.Close()

    // 1. 写入系统键
    if err := storage.MVCCPutProto(
        ctx,
        batch,
        keys.StoreHLCUpperBoundKey(),  // Key: "\x01k{storeID}hlc-ub\x00"
        hlc.Timestamp{},               // MVCC timestamp = 0（系统键不需要版本）
        &ts,                           // Value: {WallTime: time}
        storage.MVCCWriteOptions{},
    ); err != nil {
        return err
    }

    // 2. 同步刷盘（关键：必须 fsync）
    return batch.Commit(true /* sync */)
}
```

**Why 必须 sync=true？**

```
场景：节点在持久化后立即崩溃

不使用 sync（错误）：
  T0: batch.Commit(false) 返回成功
      → 数据仍在 page cache 中，未写入磁盘
  T1: 节点崩溃
      → page cache 丢失
  T2: 重启后读取 upper_bound
      → 读到旧值或 0
      → HLC 可能回退

使用 sync（正确）：
  T0: batch.Commit(true) 返回成功
      → 数据已通过 fsync 持久化到磁盘
  T1: 节点崩溃
  T2: 重启后读取 upper_bound
      → 读到正确的值
      → HLC 单调性得到保证
```

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 Clock.RefreshHLCUpperBound() 的实现

```go
// pkg/util/hlc/hlc.go
func (c *Clock) RefreshHLCUpperBound(
    persistFn func(int64) error,
    delta int64,
) error {
    // 1. 验证 delta 为正数
    if delta < 0 {
        return errors.Errorf("HLC upper bound delta %d should be positive", delta)
    }

    // 2. 计算新的上界 = 当前 HLC 时间 + delta
    //    关键：使用 Now() 而非 PhysicalNow()
    //    原因：Now() 考虑了来自其他节点的"未来时间"
    upperBound := c.Now().WallTime + delta

    // 3. 持久化并更新内存
    return c.persistHLCUpperBound(persistFn, upperBound)
}
```

**输入/输出**：
- **输入**：
  - `persistFn`：持久化函数（闭包），签名为 `func(int64) error`
  - `delta`：时间增量（单位：纳秒）
- **输出**：
  - 成功：返回 `nil`，同时内存中的 `c.mu.wallTimeUpperBound` 被更新
  - 失败：返回错误，内存值**不变**（保证原子性）

**不变量**：

1. **上界必须在未来**：
   ```
   upper_bound = Now() + delta
   upper_bound > Now()  （因为 delta > 0）
   ```

2. **持久化先于内存更新**：
   ```go
   if err := persistFn(hlcUpperBound); err != nil {
       return err  // 持久化失败 → 不更新内存
   }
   c.mu.wallTimeUpperBound = hlcUpperBound  // 持久化成功 → 更新内存
   ```

### 3.2 persistHLCUpperBound() 的原子性保证

```go
func (c *Clock) persistHLCUpperBound(
    persistFn func(int64) error,
    hlcUpperBound int64,
) error {
    // 1. 先持久化到磁盘
    if err := persistFn(hlcUpperBound); err != nil {
        return err
    }

    // 2. 持久化成功后，再更新内存（在锁保护下）
    c.mu.Lock()
    defer c.mu.Unlock()
    c.mu.wallTimeUpperBound = hlcUpperBound
    return nil
}
```

**并发安全**：
- `persistFn` 可能耗时较长（涉及磁盘 I/O 和 fsync），**不持有锁**执行
- 只有在持久化成功后，才短暂加锁更新内存值
- 如果持久化失败，内存值保持不变 → 下次 `Now()` 仍会检查旧上界

**Why 这个顺序？**

```
场景 1：先更新内存，后持久化（错误）
  T0: c.mu.wallTimeUpperBound = 2000
  T1: persistFn() 开始（耗时 10ms）
  T2: HLC.Now() 被调用
      → 生成 ts=1800（< 2000）
      → enforceWallTimeWithinBoundLocked() 检查通过
  T3: persistFn() 失败
      → 磁盘上仍是旧值 1000
  T4: 节点崩溃
  T5: 重启后 HLC 可能回退到 1000

场景 2：先持久化，后更新内存（正确）
  T0: persistFn(2000) 开始
  T1: HLC.Now() 被调用
      → 检查 c.mu.wallTimeUpperBound = 1000（旧值）
      → 生成 ts=1800（< 1000 会 panic，≥ 1000 通过）
  T2: persistFn() 成功
  T3: c.mu.wallTimeUpperBound = 2000
  T4: 后续 Now() 检查新上界 2000
```

### 3.3 enforceWallTimeWithinBoundLocked() 的运行时检查

每次 `HLC.Now()` 或 `HLC.Update()` 被调用时，都会执行此检查：

```go
// pkg/util/hlc/hlc.go
func (c *Clock) enforceWallTimeWithinBoundLocked() {
    // 前置条件：调用者必须持有 c.mu
    if c.mu.wallTimeUpperBound != 0 &&
       c.mu.timestamp.WallTime > c.mu.wallTimeUpperBound {
        // 致命错误：HLC 时间超过了持久化的上界
        c.logger.Fatalf(
            context.TODO(),
            "wall time %d is not allowed to be greater than upper bound of %d.",
            redact.Safe(c.mu.timestamp.WallTime),
            redact.Safe(c.mu.wallTimeUpperBound),
        )
    }
}
```

**触发场景**：

1. **正常情况**：永远不会触发
   - 持久化的 `upper_bound = Now() + 3*interval`（远超当前时间）
   - HLC 增长速度受物理时钟限制（最多 1 纳秒/纳秒）

2. **异常场景（会 panic）**：
   - **时钟向前大跳**：物理时钟从 1000 跳到 5000，而 `upper_bound=2000`
   - **持久化滞后**：长时间未持久化（> 3*interval），HLC 持续增长
   - **磁盘损坏**：读取到错误的 `upper_bound` 值

**Why Fatalf（panic）？**

这是一个**拜占庭错误**检测机制：
- 如果 HLC 超过了持久化的上界 → 说明系统的时间假设被破坏
- 继续运行可能导致更严重的数据不一致（如双 lease、事务错序）
- **Fail-stop 语义**：立即崩溃，避免扩散损坏

### 3.4 ensureClockMonotonicity() 的启动等待机制

在节点重启时调用（`checkHLCUpperBoundExistsAndEnsureMonotonicity` → `ensureClockMonotonicity`）：

```go
// pkg/server/clock_monotonicity.go
func ensureClockMonotonicity(
    ctx context.Context,
    clock *hlc.Clock,
    startTime time.Time,
    prevHLCUpperBound int64,
    sleepUntilFn func(context.Context, hlc.Timestamp) error,
) {
    var sleepUntil int64

    if prevHLCUpperBound != 0 {
        // 场景 1：磁盘上有持久化的上界
        // 目标：等待到上界 + 1（确保新时间戳 > 旧时间戳）
        sleepUntil = prevHLCUpperBound + 1
    } else {
        // 场景 2：磁盘上无上界（首次启动或功能未启用）
        // 目标：等待 MaxOffset（保守策略）
        // 原因：重启前，HLC 可能被其他节点"推高"了 MaxOffset
        //       如不等待，可能生成比重启前更早的时间戳
        sleepUntil = startTime.UnixNano() + int64(clock.MaxOffset()) + 1
    }

    currentWallTime := clock.Now().WallTime
    delta := time.Duration(sleepUntil - currentWallTime)

    if delta > 0 {
        log.Ops.Infof(
            ctx,
            "Sleeping till wall time %v catches up to %v to ensure monotonicity. Duration: %v",
            currentWallTime,
            sleepUntil,
            delta,
        )
        _ = sleepUntilFn(ctx, hlc.Timestamp{WallTime: sleepUntil})
    }
}
```

**输入/输出**：
- **输入**：
  - `prevHLCUpperBound`：从磁盘读取的上界（0 表示未找到）
  - `startTime`：节点启动的物理时间
  - `sleepUntilFn`：睡眠函数（通常是 `clock.SleepUntil`）
- **输出**：无返回值，但会阻塞直到 HLC 达到目标时间

**关键分支**：

1. **prevHLCUpperBound != 0**（已启用持久化）：
   - 精确等待：`sleepUntil = prevHLCUpperBound + 1`
   - 优点：等待时间最短（通常 < 1 秒）
   - 前提：持久化间隔不能太长（否则等待时间会很长）

2. **prevHLCUpperBound == 0**（未启用持久化）：
   - 保守等待：`sleepUntil = startTime + MaxOffset`
   - 原因：无法确定重启前的最大 HLC 值，只能假设最坏情况
   - MaxOffset 通常是 500ms → 启动时等待 500ms

**为什么要 +1？**

```
prevHLCUpperBound = 1000

错误做法：sleepUntil = 1000
  → HLC.Now() 可能生成 1000（等于旧上界）
  → 如果重启前恰好生成了 1000 → 时间戳重复！

正确做法：sleepUntil = 1001
  → HLC.Now() 最早生成 1001（大于旧上界）
  → 保证单调性
```

---

## 四、动态行为分析（Runtime 行为）

### 4.1 持久化间隔的动态调整

#### 启用持久化（从 0 到非 0）

**用户操作**：
```sql
SET CLUSTER SETTING server.clock.persist_upper_bound_interval = '10s';
```

**系统响应**（时间线）：

```
T0: SQL 命令执行
    → 集群设置被更新
    → 触发 SetOnChange 回调

T1: 回调执行（L281-L283）
    persistHLCUpperBoundIntervalCh <- 10*time.Second

T2: 后台 goroutine 收到消息（L219）
    case updatedPersistInterval := <-persistHLCUpperBoundIntervalCh:

T3: 更新 persistInterval（L224）
    persistInterval = 10*time.Second

T4: 停止旧 ticker（L226）
    ticker.Stop()

T5: 创建新 ticker（L228）
    ticker = time.NewTicker(10*time.Second)

T6: 立即执行一次持久化（L229）
    persistHLCUpperBound()
      └─ clock.RefreshHLCUpperBound(persistFn, 30*time.Second)
          // delta = 3 * 10s = 30s

T7: 持久化到磁盘（L273-L275）
    s.node.SetHLCUpperBound(HLC.Now() + 30s)
      └─ Store.WriteHLCUpperBound() → MVCCPutProto → fsync

T8: 更新内存上界（L640-L641）
    c.mu.wallTimeUpperBound = upper_bound

T9: 记录日志（L230-L231）
    log.Ops.Infof("persisting HLC upper bound is enabled [every 10.00s]")
```

**后续定时持久化**：

```
T0: ticker 触发（10 秒后）
T1: 执行 persistHLCUpperBound()
    → upper_bound = HLC.Now() + 30s
    → 写入磁盘 + fsync
    → 更新内存

T10s: 再次触发
T20s: 再次触发
...
```

#### 禁用持久化（从非 0 到 0）

**用户操作**：
```sql
SET CLUSTER SETTING server.clock.persist_upper_bound_interval = '0';
```

**系统响应**：

```
T0: 收到新间隔 0

T1: 停止 ticker（L226）

T2: 重置 HLC 上界（L233-L239）
    clock.ResetHLCUpperBound(persistFn)
      └─ persistHLCUpperBound(persistFn, 0)
          ├─ persistFn(0)  // 写入 0 到磁盘
          └─ c.mu.wallTimeUpperBound = 0  // 内存中也设为 0

T3: 记录日志（L240）
    log.Ops.Info("persisting HLC upper bound is disabled")
```

**影响**：
- `enforceWallTimeWithinBoundLocked()` 检查被跳过（因为 `wallTimeUpperBound == 0`）
- 节点重启时会回退到 **MaxOffset 等待策略**（保守）

### 4.2 Delta = 3 * Interval 的设计原理

**核心问题**：为什么是 3 倍，而不是 1 倍或 2 倍？

#### 场景 1：Delta = 1 * Interval（不足）

```
假设 interval = 10s

T0: 持久化 upper_bound = Now() + 10s = 1000 + 10000 = 11000
T10s: ticker 触发，准备持久化
      → 但由于 CPU 负载高，持久化被延迟 5 秒
T15s: 持久化完成，upper_bound = Now() + 10s = 15000 + 10000 = 25000
      → 在 T10s - T15s 这 5 秒内，HLC 持续增长
      → 可能生成 ts=12000, 13000, 14000...
      → 如果在 T14s 崩溃 → 磁盘上仍是 11000
      → 重启后 HLC 会回退！
```

#### 场景 2：Delta = 2 * Interval（勉强）

```
T0: upper_bound = Now() + 20s = 1000 + 20000 = 21000
T10s: ticker 触发，延迟 5 秒
      → HLC 最多增长到 15000（< 21000）
T15s: 持久化，upper_bound = 15000 + 20000 = 35000

如果连续 2 次持久化都延迟 5 秒：
  T0: upper_bound = 21000
  T15s: 持久化 upper_bound = 35000（应该在 T10s）
  T25s: 持久化 upper_bound = 45000（应该在 T20s）
  → 在 T20s - T25s 之间，HLC 可能超过 21000
```

#### 场景 3：Delta = 3 * Interval（当前设计）

```
T0: upper_bound = Now() + 30s = 1000 + 30000 = 31000
T10s: ticker 触发，延迟 5 秒
T15s: 持久化，upper_bound = 15000 + 30000 = 45000

容忍：
  - 1 次持久化延迟：5s（T10s → T15s）
  - 1 次持久化失败：10s（跳过 T20s，等到 T30s）
  → 总共容忍 15s 延迟
  → 仍在 30s 的缓冲范围内
```

**数学证明**：

假设：
- 持久化间隔：`I`
- Delta 倍数：`N`
- 持久化延迟：`D`（最坏情况）

单调性条件：
```
upper_bound(T) ≥ HLC.Now(T + I + D)
Now() + N*I ≥ Now() + I + D
N*I ≥ I + D
N ≥ 1 + D/I
```

如果 `D = I`（持久化延迟 = 间隔，极端情况）：
```
N ≥ 1 + 1 = 2
```

如果 `D = 2*I`（连续 2 次失败）：
```
N ≥ 1 + 2 = 3
```

**结论**：`N = 3` 可以容忍最多 2 次持久化失败（或等价的延迟）。

### 4.3 持久化失败的处理策略

```go
// L204-L214
persistHLCUpperBound := func() {
    if err := clock.RefreshHLCUpperBound(
        persistHLCUpperBoundFn,
        int64(persistInterval*3),
    ); err != nil {
        log.Ops.Fatalf(
            context.Background(),
            "error persisting HLC upper bound: %v",
            err,
        )
    }
}
```

**策略**：**Fatalf → 节点崩溃**

**Why 不重试？**

```
场景：持久化失败后继续运行

T0: 持久化失败（磁盘满、I/O 错误等）
    → 磁盘上的 upper_bound 仍是旧值 1000
T1: HLC 持续增长到 2000
T2: 节点崩溃（其他原因）
T3: 重启，读取 upper_bound = 1000
    → HLC 回退到 1000
    → 违反单调性

如果在 T0 立即崩溃：
  → 磁盘上有有效的旧 upper_bound（虽然旧，但仍能保护部分时间窗口）
  → 重启后等待到旧上界，至少不会回退到旧上界之前
```

**替代方案的权衡**：

| 策略 | 优点 | 缺点 |
|------|-----|-----|
| **Fatalf（当前）** | ✅ 保证单调性<br>✅ 快速失败 | ❌ 可用性下降（节点不可用） |
| **忽略错误** | ✅ 可用性高 | ❌ 违反单调性<br>❌ 数据不一致风险 |
| **重试 N 次** | ⚖️ 平衡可用性与正确性 | ⚠️ 复杂度高<br>⚠️ 仍可能失败 |

CockroachDB 选择 **正确性 > 可用性**（CAP 定理中的 CP）。

---

## 五、具体示例（必须有）

### 5.1 完整的生命周期示例（从启动到运行）

#### 场景设置

- 集群设置：`server.clock.persist_upper_bound_interval = '10s'`
- 节点已运行一段时间，然后重启

#### 时刻 T0：节点重启前（运行中）

```
HLC.Now() = 1705435200000000000  // 2024-01-16 12:00:00 UTC
持久化间隔 = 10s
上次持久化时间：T-8s（2 秒前）
```

**磁盘状态**（Pebble）：
```
Key:   "\x01k1hlc-ub\x00"  // Store 1 的 HLC 上界
Value: {WallTime: 1705435200000000000 + 30*10^9}
       = 1705435230000000000  // 比当前时间晚 30 秒
```

#### 时刻 T1：节点崩溃

```
T1: 节点因硬件故障崩溃
    → 内存中的 HLC 状态丢失
    → 磁盘上的 upper_bound 仍是 1705435230000000000
```

#### 时刻 T2：节点重启开始（T1 + 5 秒）

```
物理时钟 = 1705435205000000000  // 12:00:05 UTC

执行路径：
  Server.Start()
    → checkHLCUpperBoundExistsAndEnsureMonotonicity()
```

**L108：读取磁盘上的 HLC 上界**

```go
hlcUpperBound, err := kvserver.ReadMaxHLCUpperBound(ctx, s.engines)
// 返回：1705435230000000000
```

**L116-L122：执行单调性检查**

```go
ensureClockMonotonicity(
    ctx,
    s.clock,
    s.startTime,  // 1705435205000000000
    hlcUpperBound, // 1705435230000000000
    s.clock.SleepUntil,
)
```

**ensureClockMonotonicity() 内部**：

```go
sleepUntil = prevHLCUpperBound + 1
           = 1705435230000000000 + 1
           = 1705435230000000001

currentWallTime = clock.Now().WallTime
                = 1705435205000000000  // 当前物理时钟

delta = sleepUntil - currentWallTime
      = 1705435230000000001 - 1705435205000000000
      = 25000000001 纳秒
      = 25.000000001 秒

log.Ops.Infof("Sleeping till wall time %v catches up to %v. Duration: %v",
    currentWallTime,    // 1705435205000000000
    sleepUntil,         // 1705435230000000001
    delta)              // 25s
```

**输出日志**：
```
I240116 12:00:05.000000 1 server/clock_monotonicity.go:166  Sleeping till wall time 1705435205000000000 catches up to 1705435230000000001 to ensure monotonicity. Duration: 25.000000001s
```

**L173：执行睡眠**

```go
_ = sleepUntilFn(ctx, hlc.Timestamp{WallTime: sleepUntil})
```

**Clock.SleepUntil() 内部**（简化）：

```go
for {
    now := c.Now().WallTime
    if now >= t.WallTime {
        return nil  // 达到目标时间
    }
    remaining := time.Duration(t.WallTime - now)
    if remaining > time.Second {
        time.Sleep(time.Second)  // 每秒检查一次（避免时钟跳跃）
    } else {
        time.Sleep(remaining)
    }
}
```

#### 时刻 T3：睡眠期间（T2 + 1s）

```
物理时钟 = 1705435206000000000  // 12:00:06 UTC
HLC.Now() = 1705435206000000000  // 等于物理时钟（因为尚未收到其他节点消息）
剩余睡眠时间 = 24s
```

#### 时刻 T4：睡眠结束（T2 + 25s）

```
物理时钟 = 1705435230000000001  // 12:00:30.000000001 UTC
HLC.Now() = 1705435230000000001

ensureClockMonotonicity() 返回
  → checkHLCUpperBoundExistsAndEnsureMonotonicity() 返回 hlcUpperBoundExists=true
```

#### 时刻 T5：Node.start() 完成（T4 + 8s）

```
Store 启动完成（加载 10,000 个 Replica，耗时 8 秒）
物理时钟 = 1705435238000000000  // 12:00:38 UTC
```

#### 时刻 T6：startPersistingHLCUpperBound() 开始（T5 + 0.01s）

**L285：检查 hlcUpperBoundExists**

```go
if hlcUpperBoundExists {  // true
    // 立即持久化新上界
    if err := s.clock.RefreshHLCUpperBound(
        persistHLCUpperBoundFn,
        int64(5*time.Second),
    ); err != nil {
        return errors.Wrap(err, "refreshing HLC upper bound")
    }
}
```

**RefreshHLCUpperBound() 执行**：

```go
upperBound = c.Now().WallTime + delta
           = 1705435238000000000 + 5*10^9
           = 1705435243000000000  // 12:00:43 UTC

persistFn(1705435243000000000)
  → s.node.SetHLCUpperBound(1705435243000000000)
      → Store.WriteHLCUpperBound()
          → MVCCPutProto(StoreHLCUpperBoundKey, 1705435243000000000)
          → batch.Commit(sync=true)  // fsync 到磁盘
```

**磁盘状态更新**：
```
Key:   "\x01k1hlc-ub\x00"
Value: {WallTime: 1705435243000000000}  // 新上界：12:00:43 UTC
```

**内存状态更新**：
```go
c.mu.Lock()
c.mu.wallTimeUpperBound = 1705435243000000000
c.mu.Unlock()
```

#### 时刻 T7：后台 goroutine 启动（T6 + 0.001s）

```go
_ = s.stopper.RunAsyncTask(ctx, "persist-hlc-upper-bound", func(context.Context) {
    periodicallyPersistHLCUpperBound(...)
})
```

**后台任务内部**：

```
T7+0ms: 创建 ticker（停止状态）

T7+1ms: 收到初始间隔 10s（从通道）
  → persistInterval = 10s
  → ticker = time.NewTicker(10s)
  → 立即执行 persistHLCUpperBound()
      upper_bound = HLC.Now() + 30s
                  = 1705435238000000000 + 30*10^9
                  = 1705435268000000000  // 12:01:08 UTC
      写入磁盘 + fsync
  → log: "persisting HLC upper bound is enabled [every 10.00s]"
```

**磁盘状态再次更新**：
```
Value: {WallTime: 1705435268000000000}  // 新上界：12:01:08 UTC
```

#### 时刻 T8：第一次定时持久化（T7 + 10s）

```
ticker 触发
  → HLC.Now() = 1705435248000000000  // 12:00:48 UTC
  → upper_bound = 1705435248000000000 + 30*10^9
                = 1705435278000000000  // 12:01:18 UTC
  → 写入磁盘
```

#### 时刻 T9：第二次定时持久化（T8 + 10s）

```
ticker 触发
  → HLC.Now() = 1705435258000000000  // 12:00:58 UTC
  → upper_bound = 1705435288000000000  // 12:01:28 UTC
  → 写入磁盘
```

**持续运行**：每 10 秒持久化一次，持续到节点停止。

### 5.2 持久化失败的崩溃示例

#### 场景：磁盘空间耗尽

```
T0: 后台任务尝试持久化
    HLC.Now() = 1705435300000000000
    upper_bound = 1705435330000000000

T1: 调用 Store.WriteHLCUpperBound()
    → MVCCPutProto() 成功（数据写入 page cache）
    → batch.Commit(sync=true) 开始
        → Pebble 尝试 fsync
        → 磁盘返回 ENOSPC（空间不足）

T2: WriteHLCUpperBound() 返回错误
    err = "error syncing log: no space left on device"

T3: RefreshHLCUpperBound() 返回错误（未更新内存）
    c.mu.wallTimeUpperBound 仍是旧值 1705435268000000000

T4: periodicallyPersistHLCUpperBound() 收到错误
    log.Ops.Fatalf("error persisting HLC upper bound: %v", err)

T5: 节点 panic 并崩溃
```

**日志输出**：
```
F240116 12:01:40.123456 1 server/clock_monotonicity.go:209  error persisting HLC upper bound: error syncing log: no space left on device
panic: error persisting HLC upper bound: error syncing log: no space left on device

goroutine 123 [running]:
...
```

**为什么这样做是正确的？**

如果不崩溃：
```
T4: 忽略错误，继续运行
T5: HLC 继续增长到 1705435350000000000
T6: 节点因其他原因崩溃
T7: 重启，读取 upper_bound = 1705435268000000000（旧值）
    → 等待到 12:01:08
    → 但实际应该等待到 12:02:30
    → HLC 回退！
```

立即崩溃：
```
T4: 节点崩溃
T5: 重启，读取 upper_bound = 1705435268000000000
    → 等待到 12:01:08（正确的旧上界）
    → 不会生成 < 12:01:08 的时间戳
    → 单调性得到保护（虽然保护范围缩小）
```

---

## 六、设计取舍与权衡（Trade-offs）

### 6.1 持久化 vs 不持久化

| 维度 | 不持久化（传统方案） | 持久化 HLC 上界（当前方案） |
|------|-------------------|------------------------|
| **启动等待时间** | MaxOffset（固定 500ms） | prevUpperBound - Now()（动态，通常 < 30s） |
| **单调性保证** | ⚠️ 弱（仅保证 500ms 范围） | ✅ 强（保证到上次持久化时间） |
| **磁盘开销** | ✅ 无 | ⚠️ 每 N 秒一次 fsync |
| **崩溃恢复速度** | ✅ 快（只等 500ms） | ⚠️ 慢（可能等 30s） |
| **适用场景** | 开发/测试环境 | 生产环境（高可靠性要求） |

**权衡点**：

**不持久化的问题**：
```
场景：节点 A 重启前，其 HLC 被推高到 Now() + 400ms
  → 重启后只等待 MaxOffset=500ms
  → 如果其他节点在这 500ms 内使用了 Now()+400ms 的时间戳
  → 节点 A 重启后可能生成 < Now()+400ms 的时间戳
  → 违反单调性
```

**持久化的代价**：
- **性能**：每次持久化需要 fsync（~1-10ms，取决于磁盘）
- **启动延迟**：重启时可能等待 10-30 秒（如果持久化间隔较长）

### 6.2 Delta 倍数的选择

| Delta | 容错能力 | 启动等待时间 | 磁盘开销 |
|-------|---------|------------|---------|
| **1x** | ❌ 无法容忍延迟 | ✅ 最短（interval） | ⚠️ 频繁写入 |
| **2x** | ⚠️ 容忍 1 次失败 | ⚖️ 中等（2*interval） | ⚖️ 中等 |
| **3x**（当前） | ✅ 容忍 2 次失败 | ⚠️ 较长（3*interval） | ✅ 较低 |
| **5x** | ✅ 容忍 4 次失败 | ❌ 很长（5*interval） | ✅ 很低 |

**当前选择（3x）的理由**：

1. **工程实践**：容忍 2 次持久化失败已足够
   - 磁盘 I/O 延迟通常 < interval
   - 连续 2 次失败的概率极低（除非磁盘故障，此时应崩溃）

2. **启动时间**：
   - 如果 interval=10s → 启动等待最多 30s（可接受）
   - 如果 interval=5s → 启动等待最多 15s（更好）

3. **灵活性**：用户可调整 interval 平衡性能和等待时间

### 6.3 持久化间隔的选择

| 间隔 | 优点 | 缺点 |
|------|-----|-----|
| **1s** | ✅ 启动等待短（3s）<br>✅ 单调性保护范围窄 | ❌ 磁盘开销高（每秒 fsync）<br>❌ CPU 开销高 |
| **10s**（推荐） | ⚖️ 平衡性能与可靠性 | ⚖️ 启动等待中等（30s） |
| **60s** | ✅ 磁盘开销低 | ❌ 启动等待长（180s=3 分钟） |
| **0**（禁用） | ✅ 无磁盘开销 | ❌ 无强单调性保证 |

**推荐配置**：

- **生产环境**：10s - 30s
  - 平衡可靠性和性能
  - 启动等待时间可接受

- **开发/测试**：0（禁用）
  - 避免等待时间（频繁重启）
  - 单调性要求较低

- **高可靠性场景**：5s
  - 缩短启动等待时间
  - 增加持久化频率（性能影响有限）

### 6.4 失败处理：Fatalf vs 重试

| 策略 | 优点 | 缺点 | 适用场景 |
|------|-----|-----|---------|
| **Fatalf（当前）** | ✅ 保证正确性<br>✅ 实现简单<br>✅ 快速失败 | ❌ 可用性下降 | CP 系统（CockroachDB） |
| **重试 N 次** | ⚖️ 平衡可用性与正确性 | ⚠️ 复杂度高<br>⚠️ 可能仍失败 | 中间方案 |
| **忽略错误** | ✅ 可用性高 | ❌ 违反单调性<br>❌ 数据不一致 | AP 系统（不适合数据库） |
| **降级模式** | ⚖️ 部分可用 | ⚠️ 复杂度极高<br>⚠️ 正确性难保证 | 未采纳 |

**CockroachDB 的选择**：

遵循 **"正确性第一"** 原则（CAP 中的 C）：
- 宁可不可用，也不违反单调性
- 分布式系统中，时间戳错误会导致**级联故障**：
  - Lease 混乱 → 多个节点同时写入 → 数据损坏
  - 事务时间戳错序 → 快照读取到未来数据 → 违反隔离性

**工程上的妥协**：

实际上，持久化失败通常是**系统性故障**（如磁盘满、硬件故障）：
- 即使重试也会失败
- 继续运行只会延长错误窗口
- **Fail-fast** 让运维团队尽早发现问题

### 6.5 锁竞争与并发

#### 锁的使用

```go
// Clock.persistHLCUpperBound()
func (c *Clock) persistHLCUpperBound(persistFn func(int64) error, hlcUpperBound int64) error {
    // 1. 先持久化（不持有锁，耗时操作）
    if err := persistFn(hlcUpperBound); err != nil {
        return err
    }

    // 2. 持久化成功后，短暂加锁更新内存（纳秒级）
    c.mu.Lock()
    defer c.mu.Unlock()
    c.mu.wallTimeUpperBound = hlcUpperBound
    return nil
}
```

**优点**：
- 锁持有时间极短（只有内存赋值）
- 持久化（可能耗时 10ms）在锁外执行
- 与 `HLC.Now()` 的锁竞争极小

#### 与 Now() 的交互

```go
// Clock.NowAsClockTimestamp()
func (c *Clock) NowAsClockTimestamp() ClockTimestamp {
    physicalClock := c.getPhysicalClockAndCheck(context.TODO())  // 锁外
    c.mu.Lock()
    defer c.mu.Unlock()

    // 更新 timestamp（纳秒级操作）
    if c.mu.timestamp.WallTime >= physicalClock {
        c.mu.timestamp.Logical++
    } else {
        atomic.StoreInt64(&c.mu.timestamp.WallTime, physicalClock)
        c.mu.timestamp.Logical = 0
    }

    c.enforceWallTimeWithinBoundLocked()  // 检查上界（纳秒级）
    return c.mu.timestamp
}
```

**性能特征**：
- `Now()` 的锁持有时间：~100 纳秒
- `persistHLCUpperBound()` 的锁持有时间：~50 纳秒
- 冲突概率：极低（每 10 秒才持久化一次）

**实测数据**（假设）：
```
HLC.Now() 调用频率：1,000,000 次/秒
持久化频率：0.1 次/秒（每 10 秒一次）
锁冲突概率：< 0.00001%（可忽略）
```

---

## 七、总结与心智模型

### 7.1 核心思想

`startPersistingHLCUpperBound()` 实现了**基于磁盘持久化的 HLC 单调性保证机制**，通过以下三步确保节点重启后时间戳不会回退：

1. **定期持久化"未来上界"**：
   - 每隔 `interval` 秒，持久化 `HLC.Now() + 3*interval` 到磁盘
   - 3 倍缓冲容忍持久化延迟和失败

2. **重启时等待**：
   - 读取磁盘上的上界
   - 如果物理时钟 < 上界，则 sleep 直到赶上
   - 保证新生成的时间戳 > 上界

3. **运行时强制检查**：
   - 每次 `HLC.Now()` 检查是否超过上界
   - 如果超过 → 立即 panic（防止时钟跳跃破坏单调性）

### 7.2 心智模型

**如果只记住一件事，那就是**：

> HLC 持久化机制是一个**"未来保证"系统**：
> 节点在运行时持续向磁盘承诺"我不会生成超过 T 的时间戳"，
> 重启后通过等待来兑现这个承诺，
> 从而确保时间永远向前流动，不会倒退。

**类比**：

这类似于**"预支信用卡"**机制：
- **持久化**：向银行承诺"我未来 30 天内不会花超过 $1000"（写入上界）
- **运行时**：每天花费 $10-50（生成时间戳）
- **重启**：如果账户被冻结（崩溃），重新激活时必须等到承诺期满（等待到上界）
- **超支保护**：如果尝试花超过 $1000 → 交易被拒绝（panic）

### 7.3 简化伪代码

```python
# ========== 启动时 ==========
def start_persisting_hlc_upper_bound(hlc_upper_bound_exists):
    persist_fn = lambda t: write_to_all_stores(t)
    interval_ch = Channel(buffer=1)

    # 初始化间隔
    interval_ch.send(get_cluster_setting("persist_interval"))

    # 监听设置变化
    on_setting_change("persist_interval", lambda new_val:
        interval_ch.send(new_val)
    )

    # 如果重启前启用了持久化，立即持久化一次
    if hlc_upper_bound_exists:
        hlc.refresh_upper_bound(persist_fn, delta=5_seconds)

    # 启动后台任务
    spawn_goroutine(periodically_persist, hlc, interval_ch, persist_fn)


# ========== 后台任务 ==========
def periodically_persist(hlc, interval_ch, persist_fn):
    ticker = Ticker(stopped=True)
    interval = 0

    while True:
        select:
            case new_interval = <-interval_ch:
                # 设置变化
                ticker.stop()
                interval = new_interval

                if interval > 0:
                    ticker = Ticker(interval)
                    persist_upper_bound(hlc, persist_fn, interval)
                    log("Enabled: every {interval}")
                else:
                    hlc.reset_upper_bound(persist_fn)
                    log("Disabled")

            case <-ticker.tick():
                # 定时触发
                persist_upper_bound(hlc, persist_fn, interval)

            case <-stop_signal:
                # 停止信号
                return


def persist_upper_bound(hlc, persist_fn, interval):
    upper_bound = hlc.now() + 3 * interval  # 3 倍缓冲
    if persist_fn(upper_bound) != OK:
        FATAL("Persist failed")  # 崩溃，保证正确性
    hlc.update_memory_upper_bound(upper_bound)


# ========== 重启时 ==========
def ensure_clock_monotonicity(prev_upper_bound):
    if prev_upper_bound > 0:
        sleep_until = prev_upper_bound + 1
    else:
        sleep_until = start_time + max_offset + 1

    if hlc.now() < sleep_until:
        log(f"Sleeping {sleep_until - hlc.now()} to ensure monotonicity")
        hlc.sleep_until(sleep_until)


# ========== 运行时 ==========
def hlc_now():
    physical_clock = read_wall_clock()

    lock(hlc.mu):
        if hlc.timestamp < physical_clock:
            hlc.timestamp = physical_clock
            hlc.logical = 0
        else:
            hlc.logical += 1

        # 强制检查上界
        if hlc.upper_bound > 0 and hlc.timestamp > hlc.upper_bound:
            PANIC("HLC exceeded upper bound")

        return hlc.timestamp
```

---

## 附录：关键代码位置索引

| 功能 | 文件 | 行号 |
|------|-----|-----|
| `startPersistingHLCUpperBound()` | `pkg/server/clock_monotonicity.go` | L269-L313 |
| `periodicallyPersistHLCUpperBound()` | `pkg/server/clock_monotonicity.go` | L187-L257 |
| `ensureClockMonotonicity()` | `pkg/server/clock_monotonicity.go` | L134-L175 |
| `Clock.RefreshHLCUpperBound()` | `pkg/util/hlc/hlc.go` | L619-L624 |
| `Clock.persistHLCUpperBound()` | `pkg/util/hlc/hlc.go` | L634-L643 |
| `Clock.enforceWallTimeWithinBoundLocked()` | `pkg/util/hlc/hlc.go` | L499-L511 |
| `Store.WriteHLCUpperBound()` | `pkg/kv/kvserver/store.go` | L2942-L2959 |
| `ReadMaxHLCUpperBound()` | `pkg/kv/kvserver/store.go` | L2980-L2989 |

---

## 参考资料

- [Hybrid Logical Clocks 论文](https://cse.buffalo.edu/tech-reports/2014-04.pdf)
- [CockroachDB Time-Travel Queries](https://www.cockroachlabs.com/docs/stable/as-of-system-time.html)
- [Clock Synchronization in Distributed Systems](https://en.wikipedia.org/wiki/Clock_synchronization)
- [CockroachDB Clock Skew RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20210719_clock_skew.md)（如果存在）

---

**本章完**。下一章将分析 **Gossip 网络的引导与连接管理**，深入解释节点如何通过 Gossip 发现彼此并保持集群拓扑的一致性。
