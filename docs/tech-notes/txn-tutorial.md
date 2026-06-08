# CockroachDB 事务实现详解

Author: learning note

从 `pkg/kv/db.go` 的 `DB.Txn` 入口出发，逐层解析 CockroachDB 分布式事务的实现。

---

## 目录

- [第一章：从 `DB.Txn` 这个门面函数开始](#第一章从-dbtxn-这个门面函数开始)
- [第二章：事务对象的诞生](#第二章事务对象的诞生--kvtxn-与-roachpbtransaction)
- [第三章：Retry Loop 与请求路径](#第三章retry-loop-与请求路径--txnexec-到-sendersend)
- [第四章：TxnCoordSender 内部](#第四章txncoordsender-内部--seqnumallocator-与-spanrefresher)

---

# 第一章：从 `DB.Txn` 这个门面函数开始

## 1.1 我们要研究什么

CockroachDB 是一个分布式 SQL 数据库，它对外提供了 PostgreSQL 兼容的 SQL 协议。但是在内部，所有数据最终都是以「分布式事务 + KV 写入」的方式落到存储引擎 Pebble 上的。

所以理解 CockroachDB 的事务实现，最自然的切入口是：

> **当一段代码想以「事务」的方式读写 KV，最外层入口长什么样？**

答案就是 `pkg/kv/db.go:1051`：

```go
func (db *DB) Txn(ctx context.Context, retryable func(context.Context, *Txn) error) error {
	return db.TxnWithAdmissionControl(
		ctx, kvpb.AdmissionHeader_OTHER, admissionpb.NormalPri,
		SteppingDisabled, retryable,
	)
}
```

这个函数极短，看起来好像没做什么事——但它是 CockroachDB 内部最常被调用的事务入口之一，几乎所有「内部 KV 操作」都会从它开始。

---

## 1.2 第一眼：这是一个典型的"门面函数（Facade）"

注意它的函数体只有一行：把自己原样转发给 `db.TxnWithAdmissionControl`，并塞进去几个固定的默认值。

这是 Go 里非常常见的模式：

- 真正干活的是底层那个**长签名**的函数 (`TxnWithAdmissionControl`)。
- 上层提供一个**短签名**的便捷入口 (`Txn`)，把不常变的参数固定掉。

文件里同一段代码下面紧接着就有：

```text
DB.Txn                       // 默认入口，admission source = OTHER
DB.TxnWithAdmissionControl   // 真正干活的实现
DB.TxnWithSteppingEnabled    // SQL 层用，会走 stepping
DB.TxnRootKV                 // KV 内部根任务用，会过 admission
```

这四个最终都会汇到同一个 `runTxn`，区别只在**附加的元数据**不同。

> **`DB.Txn` 不是"一种特殊事务"。它是"用一组默认参数发起一个普通事务"的快捷入口。**

---

## 1.3 函数签名一层层拆

```go
func (db *DB) Txn(ctx context.Context, retryable func(context.Context, *Txn) error) error
```

### 1.3.1 接收者 `db *DB`

`*DB` 是 `pkg/kv/db.go` 里定义的类型，可以理解为「KV 客户端句柄」。它持有：

- 时钟 (`clock`)，事务需要它分配时间戳；
- 一个 `TxnSenderFactory`，用来真正把请求发给 KV 集群；
- 节点 ID、cluster settings、admission 钩子等。

可以把它类比成 `database/sql` 里的 `*sql.DB`：**长生命周期、并发安全、内部维护连接/路由**。

### 1.3.2 上下文 `ctx context.Context`

它在事务执行过程中会被向下传递，承担两件事：

- **取消/超时**：上层 ctx 被 cancel，事务里所有 RPC 都会响应 cancel。
- **追踪 / 日志**：tracing span 与各种日志 tag 都挂在 ctx 上。

注意：**`ctx` 不是 transaction object**。在 CockroachDB 里事务对象是 `*Txn`，这个区分很重要。

### 1.3.3 回调 `retryable func(context.Context, *Txn) error`

这是这个函数最重要的参数。它的名字叫 `retryable`，**这个名字不是随便起的**——它就是想告诉你：

> 你写在这里的代码，可能会被运行**不止一次**。

为什么会被运行多次？因为 CockroachDB 是基于**乐观并发控制** + **HLC 时间戳**的分布式事务模型。当遇到写写冲突、时间戳被推高、commit 时无法 refresh 等情况，KV 层会返回 `TransactionRetryWithProtoRefreshError`，`DB.Txn` 会**重置事务状态、再次调用 `retryable`**。

```text
attempt 1:  retryable(ctx, txn)   ↯ 冲突 → 内部捕获到可重试错
attempt 2:  retryable(ctx, txn)   ✓ 成功 → 自动 commit
```

这直接推出了**写 `retryable` 必须遵守**的规则：

| 可以做                                | 不能做                                          |
|---------------------------------------|-------------------------------------------------|
| `txn.Get` / `txn.Put` / `txn.Del`     | 发邮件、发 webhook、调外部支付 API              |
| 读写本地变量并通过返回值传出          | 累加事务外的全局计数器                          |
| 任何幂等的纯计算                      | 任何"做完一次就回不去"的副作用                  |

### 1.3.4 返回值 `error`

只返回 error，没有返回值。业务数据通过闭包变量带出去：

```go
var name string
err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
    kv, err := txn.Get(ctx, key)
    if err != nil {
        return err
    }
    name = string(kv.ValueBytes())   // 通过闭包带出去
    return nil
})
```

---

## 1.4 函数体那一行到底固定了什么

```go
return db.TxnWithAdmissionControl(
    ctx, kvpb.AdmissionHeader_OTHER, admissionpb.NormalPri,
    SteppingDisabled, retryable,
)
```

### 1.4.1 `kvpb.AdmissionHeader_OTHER` —— admission 来源

CockroachDB 有一套叫 **Admission Control（准入控制）** 的机制，用来在节点过载时按优先级排队。`AdmissionHeader_OTHER` 在当前的 KV 准入实现里**会被识别为"绕过 KV admission control"**。

这正是 `db.go:1047` 上面那条 TODO 注释提到的事：

```go
// TODO(irfansharif): Audit uses of this since API since it bypasses AC for the
// system tenant. Make the other variant (TxnWithAdmissionControl) the default,
// or maybe rename this to be more explicit (TxnWithoutAdmissionControl)
```

> **重要提醒：绕过 admission control ≠ 绕过 KV 栈本身。** 请求依然会走完整的 TxnCoordSender → DistSender → leaseholder → MVCC 路径；只是不进 admission 队列。

### 1.4.2 `admissionpb.NormalPri` —— 准入优先级

如果将来请求确实进入了 admission 队列，它会以 `NormalPri` 排队。`Txn` 写死 `NormalPri` 表示："默认以普通工作的身份排队，不抢资源也不让出。"

### 1.4.3 `SteppingDisabled` —— 是否启用 stepping 模式

Stepping 是 SQL 层用到的机制，让同一个事务内部按"步"推进读取的可见性快照。KV 层的普通调用者不需要它，所以这里默认关闭。

---

## 1.5 一个最小可运行的例子

```go
err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
    // 读
    kv1, err := txn.Get(ctx, roachpb.Key("/account/alice"))
    if err != nil {
        return err
    }
    balance := decode(kv1.ValueBytes())
    if balance < 100 {
        return errors.New("insufficient funds")
    }
    // 写
    return txn.Put(ctx, roachpb.Key("/account/alice"), encode(balance-100))
})
```

它在内部对应这条调用链：

```text
db.Txn(ctx, fn)
  └─ db.TxnWithAdmissionControl(ctx, OTHER, NormalPri, SteppingDisabled, fn)
       ├─ NewTxnWithAdmissionControl(ctx, db, nodeID, OTHER, NormalPri)
       ├─ txn.SetDebugName("unnamed")
       ├─ txn.ConfigureStepping(ctx, SteppingDisabled)
       └─ runTxn(ctx, txn, fn)
            └─ txn.exec(ctx, fn)
```

---

## 1.6 相关函数对比

| 函数 | source | stepping | 说明 |
|---|---|---|---|
| `DB.Txn` | OTHER（绕过 AC） | 禁用 | 默认入口 |
| `DB.TxnRootKV` | ROOT_KV（过 AC） | 禁用 | KV 内部根任务 |
| `DB.TxnWithSteppingEnabled` | FROM_SQL | 启用 | SQL 层用 |

三个最终都走同一套 KV 路径，区别只在元数据。

---

# 第二章：事务对象的诞生 —— `*kv.Txn` 与 `roachpb.Transaction`

## 2.1 顺着 `TxnWithAdmissionControl` 往下走

```go
func (db *DB) TxnWithAdmissionControl(
    ctx context.Context,
    source kvpb.AdmissionHeader_Source,
    priority admissionpb.WorkPriority,
    steppingMode SteppingMode,
    retryable func(context.Context, *Txn) error,
) error {
    nodeID, _ := db.ctx.NodeID.OptionalNodeID()
    txn := NewTxnWithAdmissionControl(ctx, db, nodeID, source, priority)
    txn.SetDebugName("unnamed")
    txn.ConfigureStepping(ctx, steppingMode)
    return runTxn(ctx, txn, retryable)
}
```

按时间顺序做了 5 件事：

```text
1. 取本地节点 ID                        ← 给事务标记发起方
2. NewTxnWithAdmissionControl           ← 真正"造"出 *kv.Txn
3. 设置 debug name（"unnamed"）         ← 方便日志/追踪
4. 配置 stepping 模式                   ← SQL 才用得到的细节
5. runTxn(...)                          ← 进入 retry loop
```

---

## 2.2 两层事务表示

> CockroachDB 里实际上有**两层**事务表示：
> - `roachpb.Transaction`：**数据**（protobuf），可以被序列化、跨进程传递、写到磁盘上的事务记录里。
> - `*kv.Txn`：**句柄**（Go struct），活在内存里，封装了 mutex、sender、retry 状态等运行时状态。

`*kv.Txn` 内部包着一个 `roachpb.Transaction`，并通过 `TxnCoordSender` 维护它。这两者的关系就像 Go 的 `*os.File` 和操作系统里的 fd——一个是封装句柄、一个是底层身份。

---

## 2.3 `NewTxnWithAdmissionControl`：事务对象的构造函数

`pkg/kv/txn.go:147`：

```go
func NewTxnWithAdmissionControl(...) *Txn {
    now := db.clock.NowAsClockTimestamp()
    kvTxn := roachpb.MakeTransaction(
        "unnamed",
        nil,                    // baseKey
        isolation.Serializable,
        roachpb.NormalUserPriority,
        now.ToTimestamp(),
        db.clock.MaxOffset().Nanoseconds(),
        int32(db.ctx.NodeID.SQLInstanceID()),
        priority,
        false,                  // omitInRangefeeds
    )
    txn := NewTxnFromProto(ctx, db, gatewayNodeID, now, RootTxn, &kvTxn)
    txn.admissionHeader = kvpb.AdmissionHeader{
        CreateTime: db.clock.PhysicalNow(),
        Priority:   int32(priority),
        Source:     source,
    }
    return txn
}
```

这段代码做了 3 件关键的事：

1. **从 HLC 时钟取"现在的时间戳"** (`now`)
2. **构造 `roachpb.Transaction` 协议结构体** (`kvTxn`) —— 事务的"档案"
3. **把协议结构体包装成 `*kv.Txn`** —— Go 代码里能操作的对象

---

## 2.4 HLC 时间戳：事务"出生"那一刻

```go
now := db.clock.NowAsClockTimestamp()
```

CockroachDB 用的是 **HLC（Hybrid Logical Clock，混合逻辑时钟）**：

```text
HLC timestamp = (physical_wall_clock, logical_counter)
                       ↑                    ↑
                  本机墙上时钟          单调递增的计数器
```

设计目标：
- 大多数情况下接近真实墙上时钟；
- 永远单调递增（即使墙上时钟回拨也不会"倒退"）；
- 节点间通信时，把对方的 HLC 时间"吸收"进自己，从而保证全局可比较。

**事务诞生时拿到的这个 `now` 就是它的"出生时间戳"**，被同时用作：

| 字段                     | 含义                                       |
|--------------------------|--------------------------------------------|
| `WriteTimestamp`         | 事务打算把 intent 写在哪个时间点           |
| `MinTimestamp`           | 事务存在过的最早时间，用来判断超时/heartbeat |
| `ReadTimestamp`          | 事务"看见"哪个版本的数据                   |
| `LastHeartbeat`          | 协调者活跃的最后心跳                       |
| `GlobalUncertaintyLimit` | 不确定性窗口右端                           |

`WriteTimestamp` 和 `ReadTimestamp` 一开始相等，但**会在事务执行过程中分裂开**——这是 CockroachDB 隔离级别和重试机制的核心。

---

## 2.5 不确定性窗口：`GlobalUncertaintyLimit`

```go
gul := now.Add(maxOffsetNs, 0)
```

**问题**：节点之间时钟差几毫秒到几十毫秒是常态。如果节点 A 在它本地时间 100ms 写了一个 key，节点 B 在它本地时间 80ms 读这个 key——B 的事务时间戳是 80ms，但实际上 A 已经在更后面写了。如果 B 直接说"我没看到 A 的写"，就违反了线性一致性。

**CockroachDB 的解法**：每个事务在 `ReadTimestamp` 到 `ReadTimestamp + maxOffset` 这个区间里都要"小心"。如果在这个区间里看到别的事务的提交版本，必须把自己的时间戳推进到那个版本之后并重新读（`ReadWithinUncertaintyIntervalError`）。

这个上界就是 `gul = now + maxOffsetNs`，在事务诞生时就被钉死。

---

## 2.6 `roachpb.Transaction` 的结构

```go
return Transaction{
    TxnMeta: enginepb.TxnMeta{
        Key:               baseKey,     // nil，第一次写时填进来
        ID:                u,           // UUID v4，全局唯一
        IsoLevel:          isoLevel,    // Serializable
        WriteTimestamp:    now,
        MinTimestamp:      now,
        Priority:          MakePriority(userPriority),
        Sequence:          0,           // 1-indexed，每个请求递增
        CoordinatorNodeID: coordinatorNodeID,
    },
    Name:                   name,       // "unnamed"
    LastHeartbeat:          now,
    ReadTimestamp:          now,
    GlobalUncertaintyLimit: gul,        // now + maxOffset
    AdmissionPriority:      int32(admissionPriority),
}
```

特别注意 **`TxnMeta` 这个嵌入字段**。当事务写一个 key 时，它实际上写一个叫 **intent**（意向写）的中间状态，intent 里嵌入的就是这份 `TxnMeta`。这意味着：

> 别的事务在读到 intent 时，**当场就能从 intent 里知道**：是谁写的（ID）、什么时候写的（WriteTimestamp）、它的 push 优先级是多少（Priority）。

这是 CockroachDB 写冲突解决能在 KV 层完成、不需要全局协调器的根基。

---

## 2.7 `NewTxnFromProto`：把数据变成对象

```go
func NewTxnFromProto(..., typ TxnType, proto *roachpb.Transaction) *Txn {
    if gatewayNodeID != 0 && typ == RootTxn {
        proto.UpdateObservedTimestamp(gatewayNodeID, now)
    }
    txn := &Txn{db: db, typ: typ, gatewayNodeID: gatewayNodeID}
    txn.mu.ID = proto.ID
    txn.mu.userPriority = roachpb.NormalUserPriority
    txn.mu.sender = db.factory.RootTransactionalSender(proto, txn.mu.userPriority)
    return txn
}
```

三件事：

**① 记录 ObservedTimestamp**：把 gateway 节点当前时间记入 proto。对来自 gateway 节点本身的请求，不再有不确定性窗口——因为我们刚才看到的就是它。

**② RootTxn vs LeafTxn**：

```text
RootTxn  ← 总指挥：维护事务记录、心跳、commit/rollback，一个事务只有一个
LeafTxn  ← 分布式手脚：DistSQL 在多个节点并发执行时用，不心跳、不 commit
```

**③ 创建 Sender**：`txn.mu.sender = factory.RootTransactionalSender(...)` 返回一个 `TxnCoordSender`，这是事务的"大脑"，后面会详细讲。

---

## 2.8 两种 priority 的区别

```go
txn.admissionHeader = kvpb.AdmissionHeader{
    CreateTime: db.clock.PhysicalNow(),
    Priority:   int32(priority),  // admission priority
    Source:     source,
}
```

| 名字              | 用在哪里                     | 谁高谁赢                       |
|-------------------|------------------------------|--------------------------------|
| UserPriority      | 写写冲突时 push 决斗         | 影响事务能不能"挤掉"对方       |
| AdmissionPriority | 节点过载时 admission 队列    | 影响事务能不能"早一点"被调度   |

---

## 2.9 诞生过程总览

```text
DB.TxnWithAdmissionControl(...)
│
├─ now = HLC.NowAsClockTimestamp()
│
├─ kvTxn = roachpb.MakeTransaction(...)
│   ↳ 生成 UUID
│   ↳ WriteTs = ReadTs = MinTs = LastHeartbeat = now
│   ↳ GlobalUncertaintyLimit = now + maxOffset
│   ↳ Sequence = 0
│
├─ txn = NewTxnFromProto(..., RootTxn, &kvTxn)
│   ↳ 给 gateway 节点登记 ObservedTimestamp
│   ↳ txn.mu.sender = factory.RootTransactionalSender(...)
│
└─ txn.admissionHeader = {CreateTime, NormalPri, OTHER}
```

---

# 第三章：Retry Loop 与请求路径 —— `txn.exec` 到 `sender.Send`

## 3.1 `runTxn` 是最外层的错误门卫

```go
func runTxn(ctx context.Context, txn *Txn, retryable func(context.Context, *Txn) error) error {
    err := txn.exec(ctx, retryable)
    if err != nil {
        if rollbackErr := txn.Rollback(ctx); rollbackErr != nil {
            log.Eventf(ctx, "failure aborting transaction: %s; abort caused by: %s", rollbackErr, err)
        }
    }
    if errors.HasType(err, (*kvpb.TransactionRetryWithProtoRefreshError)(nil)) {
        return errors.Wrapf(err, "terminated retryable error")
    }
    return err
}
```

两件事：

- 调用 `txn.exec`（所有逻辑都在里面）；
- 如果 exec 返回错误，尝试 rollback（rollback 失败不掩盖原始错误，只记日志）；
- 把漏网的 `TransactionRetryWithProtoRefreshError` 变成普通错误，防止上层误以为还能继续重试。

`runTxn` 是"最终兜底的清洁工"，不做重试，只做善后。

---

## 3.2 `txn.exec`：真正的 retry loop

### 3.2.1 循环骨架

```go
for attempt := 1; ; attempt++ {
    if err := ctx.Err(); err != nil {
        return errors.Wrap(err, "txn exec")
    }
    err = fn(ctx, txn)   // ← 调用用户回调
    ...
}
```

无限循环，每轮 `attempt` 加一。**第一件事是检查 ctx 是否已经被取消**——超时或父级 cancel 会在这里拦住。

### 3.2.2 回调返回 nil 时：auto-commit

```go
if err == nil {
    if !txn.isClientFinalized() {
        err = txn.Commit(ctx)
        if err != nil {
            if !errors.HasType(err, (*kvpb.TransactionRetryWithProtoRefreshError)(nil)) {
                err = &AutoCommitError{cause: err}
            }
        }
    }
}
```

用户回调返回 `nil` 时，exec 会**自动发一个 Commit 请求**——除非用户已经手动 commit 或 rollback 了。这就是为什么 `DB.Txn` 的回调可以只做读写、不用显式 commit。

### 3.2.3 错误分类：什么是"可重试"

| 错误类型 | 说明 | exec 怎么处理 |
|---|---|---|
| `TransactionRetryWithProtoRefreshError` | TxnCoordSender 已包装好的可重试错误，携带新事务 proto | 设 `retryable = true`，进入下一轮 |
| `UnhandledRetryableError` | 没被 TxnCoordSender 拦截住的原始重试错误 | root txn 遇到直接 fatal（这是 bug） |
| 其他任何错误 | 业务错误、网络错误等 | 直接 break，退出循环 |

两个护栏：
- **txnID 不匹配**：错误是"别的事务"产生的，不重试；
- **已经 finalized**：用户在回调里手动 commit/rollback 后还返回了重试错误，assert 报错。

### 3.2.4 超出重试上限

```go
maxRetries := txn.MaxAutoRetries()  // 默认 100，由 cluster setting 控制
if attempt > maxRetries {
    rollbackErr := txn.Rollback(ctx)
    err = errors.Mark(..., ErrAutoRetryLimitExhausted)
    break
}
```

超过后强制 rollback 并返回带 `ErrAutoRetryLimitExhausted` mark 的错误。每 10 次打一条 warning 日志。

### 3.2.5 真正重置事务状态：`PrepareForRetry`

```go
if err := txn.PrepareForRetry(ctx); err != nil {
    return err
}
```

根据错误类型做两件事之一：
- **部分重试**（时间戳被推进但事务没被 abort）：清除 sender 里的 retryable error 状态，用新 WriteTimestamp 继续；
- **完全重启**（`TransactionAbortedError`，事务被别人 abort 掉）：用新 proto 完全替换 sender，事务得到新 ID、新时间戳，从头开始。

---

## 3.3 retry loop 完整流程图

```text
txn.exec(ctx, fn)
│
├─[loop start]
│  check ctx.Err()
│  err = fn(ctx, txn)               ← 用户回调
│
│  err == nil?
│    └─ isClientFinalized()?
│         ├─ yes → 什么也不做
│         └─ no  → txn.Commit()     ← auto-commit
│              └─ 成功 → break (返回 nil)
│              └─ RetryWithProtoRefresh → 继续
│
│  err != nil?
│    ├─ UnhandledRetryableError + RootTxn → fatal (bug)
│    ├─ TransactionRetryWithProtoRefreshError
│    │     ├─ txnID 不匹配 → return "retryable from another txn"
│    │     ├─ already finalized → assert error
│    │     └─ retryable = true
│    └─ 其它错误 → break (返回 err)
│
│  retryable == false → break
│
│  attempt > maxRetries?
│    └─ Rollback + 返回 ErrAutoRetryLimitExhausted
│
│  PrepareForRetry(ctx)             ← 重置 sender 状态
└─[loop back]
```

---

## 3.4 请求路径：`txn.Get` 是怎么把请求发出去的

### 3.4.1 第一层：`txn.Get` 构造 Batch

```go
func (txn *Txn) Get(ctx context.Context, key interface{}) (KeyValue, error) {
    b := txn.NewBatch()
    b.Get(key)
    return getOneRow(txn.Run(ctx, b), b)
}
```

`Get` 不直接发请求，它新建一个 `Batch`，往里追加一个 `GetRequest`，然后调用 `txn.Run` 发送。

**Batch 的设计**是 CockroachDB KV API 的核心之一：所有请求都是批量化的。哪怕只读一个 key，底层也是 `BatchRequest{Requests: [GetRequest{Key: k}]}`。

### 3.4.2 第二层：`b.Get` 构造 `GetRequest`

```go
func (b *Batch) get(key interface{}, str kvpb.KeyLockingStrengthType, ...) {
    k, err := marshalKey(key)
    ...
    b.appendReqs(kvpb.NewGet(k))
    b.initResult(1, 1, notRaw, nil)
}
```

`marshalKey` 把 Go 的 `interface{}` 编码成 `roachpb.Key`（字节切片）。注意 `NonLocking`——普通 `Get` 不获取锁，只是快照读。

### 3.4.3 第三层：`txn.Run` → `sendAndFill`

```go
func sendAndFill(ctx context.Context, send SenderFunc, b *Batch) error {
    ba := &kvpb.BatchRequest{}
    ba.Requests = b.reqs
    ba.Header = b.Header
    ba.AdmissionHeader = b.AdmissionHeader
    b.response, b.pErr = send(ctx, ba)
    b.fillResults(ctx)
    ...
}
```

打包：把 `Batch` 里攒的所有请求组装成一个 `BatchRequest` proto。
拆包：拿到 `BatchResponse` 之后，把每个子请求的响应填回 `b.Results` 对应的槽位。

### 3.4.4 第四层：`txn.Send` —— 填 Header + 转发给 sender

```go
func (txn *Txn) Send(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
    if txn.gatewayNodeID != 0 {
        ba.Header.GatewayNodeID = txn.gatewayNodeID
    }
    if ba.AdmissionHeader.CreateTime == 0 {
        ba.AdmissionHeader = txn.AdmissionHeader()
    }
    txn.mu.Lock()
    sender := txn.mu.sender
    txn.mu.Unlock()
    br, pErr := txn.db.sendUsingSender(ctx, ba, sender)
    ...
}
```

`txn.Send` 只做三件轻量的事：
1. 填 `GatewayNodeID`；
2. 补充 `AdmissionHeader`；
3. 把 ba 和 `TxnCoordSender` 一起交给 `sendUsingSender`。

**`txn.Send` 本身不给 batch 填事务 ID、不设 ReadTimestamp**——这些都是 `TxnCoordSender` 的工作。

### 3.4.5 第五层：`sendUsingSender`

```go
func (db *DB) sendUsingSender(ctx context.Context, ba *kvpb.BatchRequest, sender Sender) (...) {
    ...
    br, pErr := sender.Send(ctx, ba)
    ...
}
```

一点合法性校验和默认值填充后，调用 `sender.Send`，进入 `TxnCoordSender`。

---

## 3.5 整条请求路径总览

```text
txn.Get(ctx, key)
│
├─ txn.NewBatch()
├─ b.Get(key) → b.appendReqs(kvpb.NewGet(k))
│
└─ txn.Run(ctx, b)
    └─ sendAndFill(ctx, txn.Send, b)
        ├─ 打包: BatchRequest{Requests: [GetRequest]}
        └─ txn.Send(ctx, ba)
            ├─ 填 ba.Header.GatewayNodeID
            ├─ 填 ba.AdmissionHeader
            └─ db.sendUsingSender(ctx, ba, txnCoordSender)
                └─ txnCoordSender.Send(ctx, ba)
                    │
                    ├─[interceptor 链]
                    │  heartbeater         ← 维护心跳
                    │  seqNumAllocator     ← 给每个请求分配 Sequence 号
                    │  pipeliner           ← 流水线化 writes
                    │  spanRefresher       ← 管理 read spans，决定是否能 refresh
                    │  committer           ← 处理 1PC 和 parallel commit
                    │  metricRecorder      ← 统计指标
                    │  lockGatekeeper      ← 填事务元数据到 ba.Txn
                    │
                    └─ DistSender.Send(ctx, ba)
                        ├─ 根据 key 查 range cache，找到负责的 range
                        ├─ 找到 leaseholder 副本
                        └─ gRPC → leaseholder 节点
                            └─ MVCC read from Pebble
```

---

## 3.6 `TransactionRetryWithProtoRefreshError` 的产生路径

1. 某请求到达 leaseholder，KV 层发现冲突，返回 `WriteTooOldError` 或 `ReadWithinUncertaintyIntervalError`；
2. `spanRefresher` 拦截，尝试在新时间戳下 refresh 读集合；
3. refresh 成功：时间戳推进，继续执行，不暴露给外层；
4. refresh 失败：包装成 `TransactionRetryWithProtoRefreshError`，往上冒泡；
5. 冒到 `txn.exec`，被识别为 `retryable = true`，进入 `PrepareForRetry`，事务以新时间戳重启。

---

# 第四章：`TxnCoordSender` 内部 —— seqNumAllocator 与 spanRefresher

## 4.1 拦截器链的运转方式

`TxnCoordSender` 内部是一条**有序的 `lockedSender` 链**，每个拦截器实现 `SendLocked(ctx, ba)` 接口，在里面做完自己的工作后调用 `wrapped.SendLocked` 把 batch 传给下一层。

请求向下流：

```text
TxnCoordSender
  → txnHeartbeater
  → txnSeqNumAllocator    ← 本章第一个重点
  → txnPipeliner
  → txnSpanRefresher      ← 本章第二个重点
  → txnCommitter
  → txnMetricRecorder
  → txnLockGatekeeper
  → DistSender
```

响应向上返回时，每一层还可以在 `SendLocked` 的返回路径上做后处理。

---

## 4.2 `txnSeqNumAllocator`：为什么每个写操作都要有自己的序列号

### 4.2.1 它做的事

`pkg/kv/kvclient/kvcoord/txn_interceptor_seq_num_allocator.go:79`：

```go
func (s *txnSeqNumAllocator) SendLocked(ctx context.Context, ba *kvpb.BatchRequest) (...) {
    for _, ru := range ba.Requests {
        req := ru.GetInner()
        oldHeader := req.Header()
        if kvpb.IsIntentWrite(req) || req.Method() == kvpb.EndTxn {
            s.stepWriteSeqLocked(ctx)  // writeSeq++
            oldHeader.Sequence = s.writeSeq
        } else {
            oldHeader.Sequence = s.readSeq  // 读用当前 readSeq
        }
        req.SetHeader(oldHeader)
    }
    return s.wrapped.SendLocked(ctx, ba)
}
```

两个计数器：

```go
writeSeq enginepb.TxnSeq  // 当前最大写序号，每次写 +1
readSeq  enginepb.TxnSeq  // 读可见的最高写序号
```

### 4.2.2 序列号的四个用途

**① 同一 key 上读写的排序**

序列号让存储层知道"这个读是发生在哪次写之后的"——读用 `readSeq`，写用自己的 `writeSeq`，MVCC 层按 sequence 排序，保证读到事务自己最新的写。

**② 唯一标识每次写操作**

三元组 `(txn_id, txn_epoch, seq)` 在整个集群里唯一标识一次写。pipeliner 在 commit 之前用 `QueryIntent` 确认异步写有没有落地，就靠这个三元组来查。

**③ 判断一个 batch 是否包含完整的写集合**

`BatchRequest.IsCompleteTransaction()` 利用序列号检测。这是 parallel commit 和 1PC 优化的前提。

**④ 幂等性保证（重试安全）**

当 in-flight write 因为网络问题需要重发时，存储层会检查：

- 已存在相同 seq 的 intent：第一次其实成功了，**静默丢弃**；
- 已存在更大 seq 的 intent：过期请求，返回错误；
- 不存在 intent：正常写入。

**没有序列号，重试就可能导致重复写入。**

### 4.2.3 Stepping 模式的影响

在 `SteppingDisabled`（`DB.Txn` 的默认值）时，每次写之后 `readSeq` 会自动推进——事务总是能读到自己最新的写（read-own-writes）。

在 `SteppingEnabled`（SQL 层用）时，`readSeq` 不会自动推进，只有显式调用 `TxnCoordSender.Step()` 时才前进。这让 SQL 的游标语义成为可能：一条 `SELECT` 看到的是"该语句开始时"的快照。

---

## 4.3 `txnSpanRefresher`：避免事务重启的关键机制

### 4.3.1 问题：为什么时间戳会被推高

事务在执行期间，`WriteTimestamp` 可能被推高到比 `ReadTimestamp` 更大的位置：

| 原因 | 具体场景 |
|---|---|
| `WriteTooOld` | 事务想在时间戳 T 写 key K，但 K 已经有了比 T 更新的版本 |
| `ReadWithinUncertaintyInterval` | 事务读到了落在不确定性窗口内的值（时钟偏差问题） |
| 事务被 push | 别的事务在等待锁时把本事务的时间戳往前推 |

当 `WriteTimestamp > ReadTimestamp` 时，事务进入矛盾状态：写打算在时间戳 W 落地，但之前的读是在时间戳 R（< W）做的。如果在 R 到 W 之间有新写，读结果就"过期"了。

### 4.3.2 解法：Refresh

> **不重跑事务，而是检查读过的那些 key，在 R 到 W 这个时间段里有没有新写。如果没有，说明读依然有效，可以把 ReadTimestamp 安全地推到 W，然后继续提交。**

```text
没有 refresh 机制：
  "你的读取结果可能过期了，从头再来一遍"       ← 全部重跑

有 refresh 机制：
  "我去检查一下你读过的地方有没有变"
  → 没变：时间戳推高，继续提交                  ← 原地化解
  → 变了：抱歉，从头来吧                        ← 只在确实需要时重跑
```

### 4.3.3 `SendLocked` 的完整流程

```go
func (sr *txnSpanRefresher) SendLocked(ctx context.Context, ba *kvpb.BatchRequest) (...) {
    // 阶段一：设置 CanForwardReadTimestamp 标志
    ba.CanForwardReadTimestamp = sr.canForwardReadTimestampWithoutRefresh(ba.Txn)

    // 阶段二：发前的抢先 refresh（preemptive）
    ba, pErr := sr.maybeRefreshPreemptively(ctx, ba)
    if pErr != nil { return nil, pErr }

    // 阶段三：发送，并在收到错误时尝试 refresh 化解
    br, pErr := sr.sendLockedWithRefreshAttempts(ctx, ba, sr.maxRefreshAttempts())
    ...

    // 阶段四：记录本次读取的 spans
    sr.appendRefreshSpans(ctx, ba, br)
    return br, nil
}
```

**阶段一：`CanForwardReadTimestamp`**

这个 flag 告诉服务端："客户端允许你在服务端直接把 ReadTimestamp 推高"。如果事务还没有积累任何 refresh spans（没有读过任何 key），那么推高时间戳是免费的——没有历史读需要验证。

**阶段二：抢先 refresh（`maybeRefreshPreemptively`）**

```go
// 只有在时间戳已经被推高时才做
if ba.Txn.ReadTimestamp == ba.Txn.WriteTimestamp {
    return ba, nil  // 没被推高，不需要
}
// 两种情况下做抢先 refresh：
// 1. refreshFree：没有任何积累的 refresh spans，refresh 是零代价
// 2. refreshInevitable：batch 里有 EndTxn commit，而时间戳已推高
//    → 不做 refresh 直接发肯定会被拒绝，不如提前做
```

场景二（refreshInevitable）本质上就是 OCC（乐观并发控制）模型里的"**validate 阶段**"——确认之前读的数据在 commit 时还是一致的。CockroachDB 只在必要时做，不是每次 commit 都做，是重要的性能优化。

**阶段三：发送后的 refresh（`maybeRefreshAndRetrySend`）**

```go
func (sr *txnSpanRefresher) maybeRefreshAndRetrySend(...) (...) {
    // 检查这个错误是否是"可以通过 refresh 化解"的
    ok, refreshTS := kvpb.TransactionRefreshTimestamp(pErr)
    if !ok {
        return nil, pErr
    }

    // 尝试把所有读过的 spans 在新时间戳下重新验证
    if refreshErr := sr.tryRefreshTxnSpans(ctx, refreshFrom, refreshToTxn); refreshErr != nil {
        return nil, pErr  // refresh 失败，原始错误往上冒泡
    }

    // refresh 成功！用新时间戳重试这个 batch
    ba.UpdateTxn(refreshToTxn)
    return sr.sendLockedWithRefreshAttempts(ctx, ba, maxRefreshAttempts-1)
}
```

单次 batch 最多允许 **5 次 refresh 尝试**（`maxTxnRefreshAttempts = 5`）。

**阶段四：积累 refresh spans**

每次成功的 `SendLocked` 之后，`appendRefreshSpans` 把本次读过的所有 key 范围记入 `refreshFootprint`，供下次 refresh 使用。

---

## 4.4 refresh 的真实开销与内存上限

refresh 是有真实网络开销的：需要向涉及的每个 range 发 `Refresh` 或 `RefreshRange` 请求，让服务端检查时间范围内有没有新写入。

```go
var MaxTxnRefreshSpansBytes = settings.RegisterIntSetting(
    ..., "kv.transaction.max_refresh_spans_bytes", ...,
    1<<22, /* 4 MB */
)
```

超过 4MB 时，尝试合并相邻 spans。如果合并后还是超了，设 `refreshInvalid = true`——**放弃追踪，所有 refresh 都会失败，事务被迫重启**。

这意味着：**读取范围极大的长事务天然不适合 refresh 机制，它们遇到时间戳推高时几乎必然重启**。这是推荐"保持事务短小"的原因之一。

---

## 4.5 一个完整场景：时间戳推高到成功提交

```text
事务 T1 诞生：ReadTs = WriteTs = 100

操作 1: txn.Get(key_A)
  → seqNumAllocator: Sequence = 0 (读，不递增)
  → spanRefresher: ReadTs == WriteTs，不做 preemptive refresh
  → 服务端读取 key_A @100，成功
  → spanRefresher: 把 key_A 加入 refreshFootprint

操作 2: txn.Put(key_B, "x")
  → seqNumAllocator: writeSeq = 1，Sequence = 1
  → 服务端发现 key_B 已经有一个 @110 的版本
  → 返回 WriteTooOldError，告知 WriteTimestamp 需要推到 111

  现在：ReadTs = 100，WriteTs = 111

操作 3: txn.Commit()
  → spanRefresher: ReadTs(100) ≠ WriteTs(111)，且 batch 有 EndTxn
    → refreshInevitable = true，执行 preemptive refresh
    → tryRefreshTxnSpans: 发 Refresh(key_A, from=100, to=111)
      → 服务端检查 key_A 在 [100, 111) 内有没有新写
      → 没有！refresh 成功
    → ReadTs 推到 111，现在 ReadTs = WriteTs = 111
  → 用新时间戳发出 EndTxn，提交成功

结果：事务成功提交 @111，整个过程没有触发用户回调的重试
```

如果 key_A 在 100 到 111 之间有新写（比如 @105），refresh 失败，事务只能重启。

---

## 4.6 代码阅读地图

```text
事务入口：
  pkg/kv/db.go
    DB.Txn / DB.TxnWithAdmissionControl / DB.TxnRootKV / runTxn

事务对象：
  pkg/kv/txn.go
    NewTxnWithAdmissionControl / NewTxnFromProto / Txn.exec / Txn.Send

事务 proto：
  pkg/roachpb/data.go
    MakeTransaction

拦截器链：
  pkg/kv/kvclient/kvcoord/
    txn_interceptor_seq_num_allocator.go
    txn_interceptor_span_refresher.go
    txn_interceptor_pipeliner.go
    txn_interceptor_committer.go
    txn_interceptor_heartbeater.go
    txn_coord_sender.go
```

---

# 第五章：`txnPipeliner` 与 `txnCommitter` —— 写流水线与并行提交

## 5.1 为什么这两个拦截器是性能关键

在标准的分布式事务里，一次 commit 至少需要：

```text
写 intent（Raft 共识）→ 写事务记录（Raft 共识）→ 解析 intent（Raft 共识）
```

三轮 Raft 串行。在 CockroachDB 里，`txnPipeliner` + `txnCommitter` 协作，把这个变成：

```text
写 intent（异步，不等）→ [同时] 写事务记录 + 验证 intent（一轮 Raft）→ 异步显式化 commit
```

**整个事务的关键路径只需要一轮 Raft 共识**。这是 CockroachDB 吞吐量和延迟表现的基础。

---

## 5.2 `txnPipeliner`：让写请求不阻塞后续操作

### 5.2.1 核心思路

普通事务里，每次 `txn.Put` 都要等 Raft 达成共识（成功写入 intent）才返回，然后才能发下一个请求。

pipeliner 的优化是：**把写请求设置为 `AsyncConsensus = true`，立刻返回而不等 Raft 完成**。这个写被标记为 in-flight write，放入 `ifWrites` 集合里追踪。

```text
普通写：
  txn.Put(A) → 等 Raft → 返回 → txn.Put(B) → 等 Raft → ...
  时间线: |---Raft A---|---Raft B---|

流水线写：
  txn.Put(A) → 不等，立刻返回 → txn.Put(B) → 不等，立刻返回 → ...
  Raft 在后台并行运转: |---Raft A---|
                              |---Raft B---|
```

### 5.2.2 `SendLocked` 里的三个关键步骤

`pkg/kv/kvclient/kvcoord/txn_interceptor_pipeliner.go:311`：

```go
func (tp *txnPipeliner) SendLocked(ctx context.Context, ba *kvpb.BatchRequest) (...) {
    // 1. 如果有 EndTxn，把 in-flight writes 和 lock footprint 附加上去
    ba, pErr := tp.attachLocksToEndTxn(ctx, ba)

    // 2. 检查是否可以 async consensus
    ba.AsyncConsensus = tp.canUseAsyncConsensus(ctx, ba)

    // 3. 让新请求"链接"到还未完成的 in-flight writes 上
    ba = tp.chainToInFlightWrites(ba)

    // 发出去
    br, pErr := tp.wrapped.SendLocked(ctx, ba)

    // 4. 根据响应更新 in-flight 和 lock 追踪
    tp.updateLockTracking(ctx, ba, br, pErr, ...)
    return tp.stripQueryIntents(br), nil
}
```

**步骤一：`attachLocksToEndTxn`**

当 batch 里有 EndTxn（commit）时，把所有还没被证明成功的 in-flight writes 以及整个 lock footprint 填入 `et.InFlightWrites` 和 `et.LockSpans`。这样 commit 时服务端就知道：要想让这个事务进入隐式提交状态，这些 writes 都必须在正确时间戳上落地。

**步骤二：`canUseAsyncConsensus`**

并非所有请求都能 pipeline：

| 条件 | 能否 pipeline |
|---|---|
| batch 含 EndTxn | 不能 |
| cluster setting 关闭 | 不能 |
| batch 超过 `max_batch_size`（默认 128） | 不能 |
| 请求类型不支持（如 Get/Scan） | 不能 |
| 内存预算超限 | 不能 |
| 以上都不符合 | 可以 |

注意：**单条 SQL 的 implicit txn（无显式 BEGIN）几乎不会 pipeline**——因为它通常走 1PC 快速路径，或者 batch size 超过了 128 的上限。Pipeline 主要收益在显式的多语句事务里。

**步骤三：`chainToInFlightWrites`**

如果新 batch 里有请求与某个 in-flight write 在同一个 key 上有交叠，pipeliner 会在新 batch 前面插入一个 `QueryIntent` 请求。

```text
假设之前异步写了 Put(key_A)，现在又来一个 Get(key_A)：

chainToInFlightWrites 在 Get(key_A) 前插入 QueryIntent(key_A)

batch 变成：[QueryIntent(key_A), Get(key_A)]
```

为什么要这样做？因为 DistSender 使用普通的 gRPC 单次 RPC，无法保证先发的写先到。如果先前的 Put 还在网络上飘，后来的 Get 可能比它先到 leaseholder，导致读不到自己的写。`QueryIntent` 强制等待前面的 intent 落地，然后 Get 才能安全执行。

**步骤四：`updateLockTracking`**

收到响应后：
- 成功的 async write → 从 `ifWrites` 移走，加入 `lockFootprint`（变成"已知锁"）；
- 失败的 async write → 从 `ifWrites` 移走，触发错误处理；
- QueryIntent 成功 → 对应的 in-flight write 被证明，从 `ifWrites` 移走。

### 5.2.3 内存预算

pipeliner 追踪的 in-flight writes 和 lock footprint 都有内存上限：

```go
var TrackedWritesMaxSize = settings.RegisterIntSetting(
    ..., "kv.transaction.max_intents_bytes", ...,
    1<<22, /* 4 MB */
)
```

超过 4MB 时，lock spans 会被合并成更宽的范围（用范围代替点），这会导致 intent 清理时需要扫描更大的 key 范围，代价更高。可以通过 `kv.transaction.reject_over_max_intents_budget.enabled = true` 改为直接拒绝这类大事务。

---

## 5.3 `txnCommitter`：并行提交

### 5.3.1 标准提交的问题

在 pipeliner 之后，所有 write 都异步完成了，in-flight writes 记在 `et.InFlightWrites`。但 commit 时还有个问题：

```text
普通提交流程：
  1. 发 EndTxn → leaseholder 写事务记录为 COMMITTED（一轮 Raft）
  2. 异步清理所有 intent（多轮 Raft，每个 range 一轮）
```

第一步是串行的，而且必须等所有 in-flight writes 都被 prove 成功后才能安全提交——如果先提交后发现某个 write 其实失败了，就出现了"已经 COMMITTED 但部分写没成功"的不一致状态。

### 5.3.2 并行提交：STAGING 状态

parallel commit 的核心思想是引入一个中间状态 **STAGING**：

```text
并行提交流程：
  ┌─ EndTxn 写事务记录为 STAGING（一轮 Raft）
  ├─ QueryIntent(write_1)                         ← 证明各个 in-flight write
  ├─ QueryIntent(write_2)
  └─ ... 以上全部并行发出，打包成一个 batch ─────┘

  如果以上全部成功 → 事务满足"隐式提交"条件
  → 异步地把事务记录从 STAGING → COMMITTED
```

定义两种提交条件：

1. **显式提交**：事务记录状态 = `COMMITTED`；
2. **隐式提交**：事务记录状态 = `STAGING`，且所有声明为 in-flight 的写都已在正确时间戳落地。

一旦满足任一条件，事务对外观察者来说就是已提交的。STAGING 状态本质上是一个分布式条件，因此之后会有一个异步任务把它变成 `COMMITTED`，让这个条件变成本地可查询的。

### 5.3.3 `txnCommitter.SendLocked` 里的逻辑

`pkg/kv/kvclient/kvcoord/txn_interceptor_committer.go:138`：

```go
func (tc *txnCommitter) SendLocked(ctx context.Context, ba *kvpb.BatchRequest) (...) {
    // 没有 EndTxn 就直接透传
    if !hasET { return tc.wrapped.SendLocked(ctx, ba) }

    // 纯读事务：不需要写事务记录，直接 elide EndTxn
    if len(et.LockSpans) == 0 && len(et.InFlightWrites) == 0 {
        return tc.sendLockedWithElidedEndTxn(ctx, ba, et)
    }

    // 检查是否满足并行提交条件
    if len(et.InFlightWrites) > 0 && !tc.canCommitInParallel(ba, et) {
        // 不满足：把 in-flight writes 降级为 lock spans，串行提交
        et.LockSpans, _ = mergeIntoSpans(et.LockSpans, et.InFlightWrites)
        et.InFlightWrites = nil
    }

    // 发出 batch（可能是 STAGING 提交，也可能是直接 COMMITTED）
    br, pErr := tc.wrapped.SendLocked(ctx, ba)

    switch br.Txn.Status {
    case roachpb.COMMITTED:
        return br, nil     // 1PC 直接提交成功
    case roachpb.STAGING:
        // 检查是否满足隐式提交条件
        implicitCommit, _ := isTxnCommitImplicit(br)
        if !implicitCommit {
            // 不满足：某个 in-flight write 被推到了比 STAGING 时间戳更高的位置
            // 重试：只重发 EndTxn，让它直接变 COMMITTED
            return tc.retryTxnCommitAfterFailedParallelCommit(ctx, ba, br)
        }
        // 满足隐式提交！异步推进到 COMMITTED
        tc.makeTxnCommitExplicitAsync(ctx, br.Txn, mergedLockSpans)
        br.Txn = cloneWithStatus(br.Txn, roachpb.COMMITTED)
        return br, nil
    }
}
```

### 5.3.4 `canCommitInParallel` 的条件

并行提交需要每个 batch 里的请求都"可被 STAGING 状态的 recovery 流程独立验证"。以下情况不能并行提交：

- cluster setting 关闭；
- 这是 Rollback 而非 Commit；
- EndTxn 有 commit trigger（改变内部系统状态的钩子）；
- batch 里有 Get/Scan/DeleteRange 等读请求——这些请求结果不被 recovery 流程考虑，混在同一个 batch 里无法保证正确恢复。

### 5.3.5 `isTxnCommitImplicit` 的判断逻辑

```go
func isTxnCommitImplicit(br *kvpb.BatchResponse) (bool, error) {
    etResp := br.Responses[last].GetEndTxn()
    // StagingTimestamp 是事务记录写入 STAGING 时使用的时间戳
    // br.Txn.WriteTimestamp 是 batch 中所有写操作实际使用的最高时间戳
    // 如果有任何写被 push 到比 StagingTimestamp 更高的位置，隐式提交条件失败
    failed := etResp.StagingTimestamp.Less(br.Txn.WriteTimestamp)
    return !failed, nil
}
```

用具体数字说明：

```text
事务 STAGING @100：
  → in-flight write_1 @100 ✓
  → in-flight write_2 @100 ✓
  → 所有写都在 StagingTimestamp 上 → 隐式提交满足 ✓

事务 STAGING @100，但 write_2 被 push：
  → in-flight write_1 @100 ✓
  → in-flight write_2 @105 ← 被推高了
  → br.Txn.WriteTimestamp = 105 > StagingTimestamp(100)
  → 隐式提交不满足
  → retryTxnCommitAfterFailedParallelCommit 重发 EndTxn，直接写为 COMMITTED
```

### 5.3.6 纯读事务的优化：elide EndTxn

```go
if len(et.LockSpans) == 0 && len(et.InFlightWrites) == 0 {
    return tc.sendLockedWithElidedEndTxn(ctx, ba, et)
}
```

如果整个事务没有任何写，就根本不需要发 `EndTxn` 请求——没有什么需要清理的。`sendLockedWithElidedEndTxn` 会直接把 EndTxn 从 batch 里去掉发出去，节省一个 RPC 和一次 Raft 写。

---

## 5.4 完整写事务流程

```text
事务 T：
  txn.Put(key_A, "v1")   → batch_1: [Put(A)]
  txn.Put(key_B, "v2")   → batch_2: [Put(B)]
  txn.Commit()           → batch_3: [QueryIntent(A), QueryIntent(B), EndTxn(commit)]

─────────────────────────────────────────────────────────────────────
pipeliner 处理 batch_1:
  canUseAsyncConsensus = true → ba.AsyncConsensus = true
  发出：[Put(A)]（异步，不等 Raft）
  ifWrites = {A}，lockFootprint = {}

pipeliner 处理 batch_2:
  canUseAsyncConsensus = true → ba.AsyncConsensus = true
  chainToInFlightWrites: A 和 B 不重叠，不需要插入 QueryIntent
  发出：[Put(B)]（异步，不等 Raft）
  ifWrites = {A, B}，lockFootprint = {}

pipeliner 处理 batch_3:
  attachLocksToEndTxn: et.InFlightWrites = [A, B]，et.LockSpans = []
  canUseAsyncConsensus: 含 EndTxn → false
  chainToInFlightWrites: 插入 QueryIntent(A) 和 QueryIntent(B)
  batch 变成：[QueryIntent(A), QueryIntent(B), EndTxn(commit)]
  发出（同步）

─────────────────────────────────────────────────────────────────────
committer 处理 batch_3:
  canCommitInParallel = true（只有 QueryIntent 和 EndTxn，全部兼容）
  发出：[QueryIntent(A), QueryIntent(B), EndTxn(commit=STAGING)]

─────────────────────────────────────────────────────────────────────
服务端执行 batch_3：
  QueryIntent(A) → A 的 intent @100 存在 ✓
  QueryIntent(B) → B 的 intent @100 存在 ✓
  EndTxn → 写事务记录为 STAGING @100

  响应：br.Txn.Status = STAGING，StagingTimestamp = 100

─────────────────────────────────────────────────────────────────────
committer 收到响应：
  isTxnCommitImplicit: StagingTimestamp(100) == WriteTimestamp(100) → 满足！
  makeTxnCommitExplicitAsync: 后台任务把事务记录 STAGING → COMMITTED
  返回给上层：br.Txn.Status = COMMITTED

─────────────────────────────────────────────────────────────────────
整个事务关键路径：
  Put(A)、Put(B) 的 Raft 共识 + EndTxn batch 的 Raft 共识 = 并行！
  事务延迟 ≈ max(Put Raft 延迟, EndTxn Raft 延迟) 而不是它们的之和
```

---

## 5.5 本章小结

1. **`txnPipeliner` 让写请求不阻塞**：设置 `AsyncConsensus = true`，写立刻返回，Raft 共识在后台进行。未被证明的写被追踪为 in-flight writes。
2. **chaining 保证写后读的正确性**：新请求与 in-flight write 重叠时，自动插入 `QueryIntent` 强制等待。
3. **`txnCommitter` 实现并行提交**：把 EndTxn 的状态先设为 `STAGING`，与最后一批写和所有 QueryIntent 打包在一次 Raft 共识里完成。
4. **隐式提交条件**：STAGING 记录 + 所有 in-flight writes 都在正确时间戳落地。满足后异步推进到 COMMITTED。
5. **纯读事务省掉 EndTxn**：没有 lock spans 就不需要发 EndTxn，节省一次 RPC。
6. **整个事务的关键路径只需一轮 Raft 共识**，这是 CockroachDB 延迟表现的基础。

---

# 第六章：从 DistSender 到 MVCC —— 请求如何真正落盘

## 6.1 DistSender 在整个栈里的位置

到目前为止我们一直在客户端。`TxnCoordSender` 的最后一层 `txnLockGatekeeper` 把事务元数据填入 `ba.Txn`，然后把 batch 交给 `DistSender`。

`DistSender` 是客户端和服务端之间的边界：它负责把一个逻辑 `BatchRequest` 翻译成一个或多个物理 RPC，发到持有相关数据的 leaseholder 副本上。

```text
TxnCoordSender (客户端)
  └─ txnLockGatekeeper
      └─ DistSender          ← 跨越这条线进入网络
          └─ gRPC → leaseholder replica (服务端)
              └─ Raft consensus
                  └─ MVCC / Pebble
```

---

## 6.2 DistSender 的核心职责：range 路由

CockroachDB 把整个 keyspace 切分成若干 **range**（默认 512MB），每个 range 由多个副本（通常 3 个）通过 Raft 保持一致。其中一个副本是 **leaseholder**，负责处理所有读写请求。

`DistSender.Send` 做的核心事情是：

```text
输入: BatchRequest（可能跨越多个 range）
输出: 每个 range 各自收到一个子 batch，leaseholder 处理它
```

### 6.2.1 第一层：`Send` —— 切分 batch

`pkg/kv/kvclient/kvcoord/dist_sender.go:1259`：

```go
func (ds *DistSender) Send(ctx context.Context, ba *kvpb.BatchRequest) (...) {
    rs, err := keys.Range(ba.Requests)  // 计算 batch 覆盖的 key range

    if withParallelCommit {
        return ds.divideAndSendParallelCommit(ctx, ba, rs, 0)
    }
    return ds.divideAndSendBatchToRanges(ctx, ba, rs, withCommit, 0)
}
```

如果 batch 的 epoch > 0（重试中的事务）且含有 EndTxn，强制把 EndTxn 拆到自己的 batch 里单独发——防止 EndTxn 和写请求因为跨 range 而分属不同子 batch，导致提交语义出问题。

### 6.2.2 第二层：`divideAndSendBatchToRanges` —— 迭代 range

```go
ri := MakeRangeIterator(ds)
ri.Seek(ctx, seekKey, Ascending)

// 快速路径：整个 batch 落在一个 range 里
if !ri.NeedAnother(rs) {
    return ds.sendPartialBatch(ctx, ba, rs, ...)
}

// 慢速路径：batch 跨多个 range，拆开并发发送
for ri.Valid() { ... }
```

`RangeIterator` 内部查的是 **range cache**——本地缓存的 range 描述符表，记录"哪个 key range 归哪个 RangeID、当前 leaseholder 在哪个节点"。第一次遇到某个 range 时需要去 meta range 查询，之后缓存命中就很快。

**跨 range 的 batch 会被并发分发**，各子 batch 独立到达各自的 leaseholder，并发处理。这是 CockroachDB scan 和 range delete 能扩展到大 key 范围的基础。

### 6.2.3 第三层：`sendPartialBatch` + `sendToReplicas` —— 实际 RPC

对单个 range 的子 batch，`sendPartialBatch` 启动一个重试循环：

```go
for r := retry.StartWithCtx(ctx, ds.rpcRetryOptions); r.Next(); {
    // 1. 查 range cache 获取路由信息（descriptor + leaseholder）
    routingTok, err = ds.getRoutingInfo(ctx, descKey, prevTok, ba.IsReverse)

    // 2. 发给 leaseholder
    reply, err = ds.sendToReplicas(ctx, ba, routingTok, withCommit)

    // 3. 根据错误决定是否重试
    // - NotLeaseHolderError：换 leaseholder 重试
    // - RangeKeyMismatchError：range 已经分裂，重新查缓存
    // - SendError（所有副本都不通）：驱逐缓存条目，重新查
}
```

`sendToReplicas` 内部遍历候选副本列表，优先发给已知的 leaseholder（按延迟排序）：

```go
replicas.MoveToFront(leaseholderIdx)  // leaseholder 排到最前面

for first := true; ; first = false {
    curReplica := transport.NextReplica()
    ba.Replica = curReplica
    ba.RangeID = desc.RangeID
    br, err = transport.SendNext(ctx, ba)
    // 成功返回 / 换下一个副本 / 报告 ambiguous error
}
```

如果在 commit batch 下收到网络错误，返回 `AmbiguousResultError`——不确定请求是否已被执行。上层 `txn.exec` 遇到这个错误会触发事务重试。

---

## 6.3 Follower reads：不一定要去 leaseholder

```go
if ba.RoutingPolicy == kvpb.RoutingPolicy_LEASEHOLDER &&
    CanSendToFollower(ctx, ds.st, ds.clock, closedTSPolicy, ba) {
    ba.RoutingPolicy = kvpb.RoutingPolicy_NEAREST
}
```

如果读取时间戳 ≤ range 的 **closed timestamp**，DistSender 会把路由策略改为 `NEAREST`，允许将请求发给延迟最低的副本，而不是强制去 leaseholder。

这是 CockroachDB 多区域部署中降低读延迟的关键：客户端在本地 DC 就能读到旧了一点点的数据（取决于 closed timestamp 的推进速度），避免跨大陆的 RPC。

---

## 6.4 请求到达 leaseholder：replica 的处理流程

RPC 到达目标节点后，服务端的入口是 `(*Node).Batch`，经过路由后到达 `(*Replica).executeBatchWithConcurrencyRetries`。核心流程：

```text
Replica.executeBatchWithConcurrencyRetries
  ├─ latchManager.Acquire(keySpan)    ← 获取 key range 级别的 rwlock
  ├─ lockTable.ScanAndEnqueue(ba)     ← 检查有无冲突的 intent
  │   └─ 如果有别人的 intent → 进入等待队列
  ├─ [leaseholder check]              ← 确认自己还是 leaseholder
  └─ proposeRaftCommand(cmd)          ← 写请求走 Raft
      └─ Raft.Propose(entry)
          ├─ 写入 leader 的 raft log
          ├─ 复制到 followers（通常 2 个）
          ├─ majority ack → 标记为 committed
          └─ apply → MVCCPut(...)
```

**Latch 机制**是 CockroachDB 并发控制的底层：

- 每个请求声明它要访问的 key range；
- latch manager 按 key range 串行化冲突请求（写-写、写-读 互斥，读-读 共享）；
- latch 在请求完成后立刻释放，比持有整个 range 锁细粒度得多。

写请求必须走 Raft，因为它需要在多数副本上达成共识。读请求（对 leaseholder）可以本地执行——leaseholder 保证了它持有最新的已提交数据。

---

## 6.5 MVCC：数据真正的存储形式

CockroachDB 底层存储引擎是 **Pebble**（RocksDB 的 Go 实现）。但 CockroachDB 并不直接存 `key → value`，而是用 **MVCC（多版本并发控制）** 在 Pebble 上实现了时间版本管理。

### 6.5.1 MVCC key 的编码格式

每个 MVCC 记录的 key 是 `(user_key, timestamp)` 的组合编码：

```text
/account/alice@timestamp=100  → "alice 在时间戳 100 的值"
/account/alice@timestamp=85   → "alice 在时间戳 85 的值"
/account/alice@timestamp=60   → "alice 在时间戳 60 的值"
```

时间戳**降序排列**：最新版本排在最前面，扫描时直接拿第一个即可。

### 6.5.2 intent 的存储形式

事务写入时，不直接写已提交的 MVCC 版本，而是写一个 **intent**（意向写）：

```text
正常 MVCC 版本：
  /account/alice@100   →  value="bob"   (已提交)

Intent（未提交）：
  lock table entry:  /account/alice  →  TxnMeta{ID=T1, WriteTimestamp=100}
  MVCC 数据：        /account/alice@100  →  value="charlie"  (provisional)
```

intent 有两个部分：
- **lock table 条目**：存在独立 key space（`/local/lock_table/...`），记录谁在哪个 key 上有意向写；
- **MVCC 数据版本**：和已提交版本共存，但在 metadata 里标记 `meta.Txn != nil`。

其他事务读到这个 key 时，看到 lock table 里的条目，知道有未提交的 intent，然后按隔离级别决定：等待、推进时间戳、或者报冲突。

### 6.5.3 `MVCCPut` 做了什么

`pkg/storage/mvcc.go:1932`，简化流程：

```go
func MVCCPut(ctx, rw, key, timestamp, value, opts) {
    // 1. 读取当前 meta（如果有）
    mvccGetMetadata(iter, metaKey, &meta)

    // 2. 扫描 lock table，检查是否有冲突的 intent
    ltScanner.scan(key)
    // 别的事务的 intent → LockConflictError

    // 3a. 如果是事务写，且已有自己事务的 intent：
    //     更新 IntentHistory（保留旧值供 savepoint rollback 使用）
    //     覆盖写到新时间戳
    // 3b. 如果没有 intent：
    //     写入新的 MVCC 版本（或 intent）

    // 4. 更新 MVCC 统计（key 大小、版本数等）
}
```

**IntentHistory** 是 savepoint rollback 的基础：同一事务在同一 key 上多次写入时，旧值被保存到 `IntentHistory` 里，回滚到某个保存点时从这里恢复，不需要重启整个事务。

### 6.5.4 MVCC 读的时间戳可见性

```text
可见的版本 = 时间戳 ≤ txn.ReadTimestamp 的已提交版本
           + 时间戳 ≤ txn.WriteTimestamp 的本事务自己的 intent
```

这是 Serializable 隔离级别的快照读语义：你只看到 `ReadTimestamp` 及之前提交的所有写。

---

## 6.6 一次完整的 `txn.Put` 服务端路径

```text
DistSender.sendToReplicas(ctx, ba)
  └─ gRPC → Node.Batch(ctx, ba)
      └─ Store.ExecuteRequest(ctx, ba)
          └─ Replica.executeBatchWithConcurrencyRetries(ctx, ba)
              ├─ latchManager.Acquire(keySpan)
              ├─ lockTable.ScanAndEnqueue(ba)
              │   └─ 有别人的 intent → 等待队列
              ├─ leaseholder check
              └─ proposeRaftCommand(cmd)
                  └─ Raft.Propose
                      ├─ 写 raft log → 复制到 followers → majority ack
                      └─ apply:
                          └─ MVCCPut(key, WriteTimestamp, value, {Txn: txnMeta})
                              ├─ 检查 lock table 冲突
                              ├─ 写 intent（lock table entry + MVCC 版本）
                              └─ 更新 MVCC stats
```

---

## 6.7 commit 时 intent 如何变成正式版本

`EndTxn(commit=true)` 执行时，服务端做两件事：

**① 写事务记录**：在事务的 anchor key 上写 `COMMITTED` 状态的事务记录。

**② 触发 intent 解析（异步）**：启动异步任务遍历所有 lock spans，把每个 intent 转换成正式提交的 MVCC 版本：

```text
解析前：/account/alice@100 → {value="charlie", meta.Txn=T1}  (intent)
解析后：/account/alice@100 → {value="charlie", meta.Txn=nil}  (已提交)
        lock table 里 T1 对 /account/alice 的条目被删除
```

**为什么异步？** commit 语义不需要等 intent 清理完。一旦事务记录变成 `COMMITTED`，其他事务读到 intent 时发现对应事务已 COMMITTED，直接使用该值即可。intent 清理只是为了后续读取不再需要查事务记录。

这也解释了为什么 **parallel commit 的 STAGING 状态能工作**：其他事务读到 STAGING 事务的 intent 时，会检查所有 in-flight writes 是否都已落地，如果是，就判定为已提交。这个过程叫做 **transaction recovery**。

---

## 6.8 本章小结

1. **DistSender 是客户端-服务端的边界**，负责把 batch 按 range 边界切分，路由到正确的 leaseholder。
2. **range cache 缓存路由信息**。`NotLeaseHolderError`、`RangeKeyMismatchError` 等错误会驱逐缓存条目并重试。
3. **跨 range 的 batch 被并发分发**，各子 batch 独立处理。
4. **Follower reads** 允许从延迟最低的副本读取，时间戳 ≤ closed timestamp 的读不需要去 leaseholder。
5. **服务端写请求**经过 latch → lock table 检查 → Raft 提案 → MVCC 写入。
6. **MVCC 以 `(key, timestamp)` 为 key 存储版本**，最新版本在前。事务写入的是 intent，提交后异步转化为正式版本。
7. **intent 解析是异步的**，不阻塞 commit。其他事务遇到 intent 时检查事务记录状态来判断可见性。

---

# 第七章：Lock Table 与事务冲突解决

## 7.1 为什么需要 lock table

上一章提到 MVCC 层在读到别人的 intent 时会返回 `LockConflictError`。但仅仅"发现冲突并报错"是不够的——数据库需要一套机制来**排队等待**、**推进阻塞者**、**检测死锁**，才能在高并发下既保证正确性又避免无限等待。

CockroachDB 的并发控制系统（`pkg/kv/kvserver/concurrency/`）提供了这套机制，核心是两个组件：

- **`lockTable`**：维护每个 key 上的锁和等待队列；
- **`lockTableWaiter`**：等待队列里的请求通过它决定"我该继续等、还是去推（push）锁持有者"。

---

## 7.2 Lock Table 的数据结构

`lockTableImpl` 的核心是一棵 B 树：

```go
type lockTableImpl struct {
    btree                       // 按 key 组织的 B 树，每个 key 上有一个 keyLocks
    numKeysLocked atomic.Int64  // key 上锁的总数，用于内存控制（默认上限 10000）
}
```

每个 `keyLocks` 大致包含：

```text
keyLocks for key K:
  ├─ holder: TxnMeta{ID=T1, ...}      ← 持有锁的事务（如果有）
  ├─ waitQueue: [req_A, req_B, ...]   ← 等待获取这个 key 的请求队列
  └─ 锁类型: Exclusive / Shared / Intent
```

Lock table 是**完全内存中的**（in-memory），存在于每个 leaseholder replica 上。当 range 发生 lease transfer 时，lock table 会根据磁盘上的 intent 重建。

---

## 7.3 一个请求如何进入 lock table

在 `Replica.executeBatchWithConcurrencyRetries` 里，每个请求在拿到 latch 之后都要调用 `lockTable.ScanAndEnqueue`：

```text
ScanAndEnqueue(req, guard)
  └─ 扫描 req 覆盖的所有 key range
      └─ 对每个有锁的 key：
          ├─ 如果锁持有者是自己 → 不冲突，继续
          ├─ 如果锁类型与自己请求类型兼容 → 不冲突，继续
          └─ 有冲突 → 把 req 加入该 key 的 waitQueue，返回 Guard
```

如果 `ScanAndEnqueue` 发现有冲突，请求转入等待循环：

```go
for {
    guard, err := lt.ScanAndEnqueue(req, guard)
    if !guard.ShouldWait() {
        break  // 没有冲突，可以执行了
    }
    err = ltWaiter.WaitOn(ctx, req, guard)
    // WaitOn 返回后重新 ScanAndEnqueue，因为可能有新冲突
}
```

---

## 7.4 waitingState：等待状态的分类

`lockTableGuard.CurState()` 返回当前的等待状态，有五种：

| 状态 | 含义 | 动作 |
|---|---|---|
| `waitFor` | 正在等待另一个事务释放锁 | 延迟后发起 push |
| `waitSelf` | 同一事务的另一个请求已"认领"这个锁 | 安静等待，不 push |
| `waitElsewhere` | lock table 内存超限，放弃精确排队 | 立刻 push |
| `waitQueueMaxLengthExceeded` | 等待队列超出长度限制 | 返回错误 |
| `doneWaiting` | 等待结束，可以重新扫描 | 继续执行 |

---

## 7.5 `lockTableWaiter.WaitOn`：等待与 push 的决策逻辑

`WaitOn` 是等待队列里的"事件循环"，核心是一个 `select` 循环：

```go
func (w *lockTableWaiterImpl) WaitOn(ctx context.Context, req Request, guard lockTableGuard) *Error {
    for {
        select {
        case <-newStateC:    // lock table 状态变了
            state := guard.CurState()
            switch state.kind {
            case waitFor:
                delay := LockTableDeadlockOrLivenessDetectionPushDelay  // 默认 100ms
                if priorityPush || waitPolicyError {
                    delay = 0  // 高优先级或 WaitPolicy_Error，立刻 push
                }
                timer.Reset(delay)
            case waitElsewhere:
                return w.pushLockTxn(ctx, req, state)  // 立刻 push
            case waitSelf:
                continue  // 继续等
            case doneWaiting:
                return w.ResolveDeferredIntents(...)
            }
        case <-timerC:     // push 延迟到了
            go watchForNotifications(pushCtx, pushCancel, newStateC)
            w.pushLockTxn(ctx, req, state)
        }
    }
}
```

### 7.5.1 为什么要延迟 push（默认 100ms）

push 需要发一个 `PushTxnRequest` RPC 到被 push 事务的事务记录所在 range。在大多数情况下锁持有者正在正常执行，很快会释放锁。如果立刻 push，相当于每次轻微竞争都要发一次额外 RPC，浪费了。

所以策略是：**先等 100ms，期望锁持有者自己释放**。如果 100ms 后还没释放，才 push，此时有两种可能：协调者宕机（coordinator failure）或存在死锁。

### 7.5.2 高优先级立刻 push

```go
priorityPush := canPushWithPriority(req, state)
if priorityPush {
    delay = 0  // 立刻 push
}
```

如果推者的 `UserPriority` 高于被推者，或被推者是最低优先级，就立刻 push 不等延迟。确保高优先级事务不被低优先级事务无谓阻塞。

---

## 7.6 两种 push 类型

```go
func (w *lockTableWaiterImpl) pushLockTxn(ctx context.Context, req Request, ws waitingState) *Error {
    var pushType kvpb.PushTxnType
    if ws.guardStrength == lock.None {
        pushType = kvpb.PUSH_TIMESTAMP  // 读请求 → push 时间戳
    } else {
        pushType = kvpb.PUSH_ABORT      // 写请求 → abort 对方
    }
    pusheeTxn, _, err := w.ir.PushTransaction(ctx, ws.txn, h, pushType)
    ...
}
```

| Push 类型 | 使用场景 | 效果 |
|---|---|---|
| `PUSH_TIMESTAMP` | 读请求等待写 intent | 把锁持有者的 WriteTimestamp 推到读者之上，读者跳过该 intent，继续在更低时间戳读取 |
| `PUSH_ABORT` | 写请求等待锁（写-写冲突） | 如果推者优先级更高，直接 abort 锁持有者 |

**PUSH_TIMESTAMP 的直觉**：

T1（写，WriteTimestamp=100）持有 key_A 的 intent，T2（读，ReadTimestamp=105）被阻塞。T2 执行 `PUSH_TIMESTAMP`，把 T1 的时间戳推到 106。现在 T1 的 intent 时间戳是 106 > T2.ReadTimestamp(105)，T2 在 105 以下的时间轴上看不到这个 intent，直接读取 105 以下的已提交版本。T1 则因时间戳被推高而可能需要 refresh 或重启。

**PUSH_ABORT 的直觉**：

T1（写）和 T2（写）争 key_A，发生写-写冲突。T2 试图 abort T1——但前提是 T1 的 `Priority` 低于 T2。如果 T1 优先级更高，push 失败，T2 等待。这确保高优先级事务不被低优先级事务 abort。

---

## 7.7 死锁检测

经典死锁：

```text
T1 持有 key_A，等 key_B
T2 持有 key_B，等 key_A
→ 相互等待，死锁
```

等待 100ms 后，T1 和 T2 各自发起 push。Push 到达对方的事务记录所在 range，执行 `PushTxn`：

- 如果被推者已 `COMMITTED`/`ABORTED`：push 成功，推者继续；
- 如果被推者是 `PENDING`：比较优先级，低优先级的一方被 abort。

因为每个事务的 `Priority` 在诞生时基于 `userPriority` 加随机扰动生成，死锁中总有一方优先级更低会被 abort，避免了活锁。

---

## 7.8 一个完整冲突解决场景

```text
T1: Put(key_A, "v1")  → intent 写在 key_A@100
T2: Get(key_A)        → ReadTimestamp=105，遇到 T1 的 intent

─────────────────────────────────────────────────────────
T2 到达 leaseholder，MVCC 扫描发现 meta.Txn = T1：
  → LockConflictError

concurrency manager：
  lockTable.ScanAndEnqueue(T2_req)
  → key_A 上有 T1 的 intent，T2 进入 waitQueue
  → 状态: waitFor(T1)

lockTableWaiter.WaitOn(T2_req, guard):
  guardStrength = lock.None（读请求）
  delay = 100ms，timer.Reset(100ms)

[等待 100ms...]

timerC 触发：
  pushLockTxn → pushType = PUSH_TIMESTAMP
  PushTransaction(T1, pushTimestamp=105)

─────────────────────────────────────────────────────────
PushTxn 到达 T1 事务记录所在 range：
  T1 状态 PENDING，T2 优先级 > T1
  → T1.WriteTimestamp 被推到 106

返回给 T2: T1.WriteTimestamp = 106

─────────────────────────────────────────────────────────
T2 收到 push 结果：
  T1 的 intent 时间戳被推到 106 > T2.ReadTimestamp(105)
  → T2 在 105 以下看不到这个 intent
  → T2 从 waitQueue 移除，状态变为 doneWaiting

T2 重新 ScanAndEnqueue → 无冲突 → 继续读取
  拿到 key_A 在 ReadTimestamp(105) 以下的最新已提交版本

─────────────────────────────────────────────────────────
T1 的影响：
  WriteTimestamp = 106，ReadTimestamp = 100
  WriteTimestamp > ReadTimestamp → T1 需要 refresh 或重启
  （由 spanRefresher 处理，见第四章）
```

---

## 7.9 Contention 的可观测性

CockroachDB 把每次竞争事件记录到 tracing span 里，可以通过系统表观察：

```sql
SELECT * FROM crdb_internal.transaction_contention_events
ORDER BY collection_ts DESC LIMIT 10;
```

记录的信息包括：等待的 key、等待时长、被哪个事务阻塞、最终是通过 push 还是等待解决的。这是诊断"为什么事务慢"的重要工具。

---

## 7.10 本章小结

1. **Lock table 是纯内存的等待队列**，按 key 组织，记录锁持有者和等待者。
2. **`ScanAndEnqueue` 发现冲突后进入 `WaitOn` 等待循环**，通过 channel 通知机制异步等待，不阻塞 replica goroutine。
3. **等待 100ms 再 push**，给锁持有者自然释放的机会，避免不必要的 RPC。
4. **两种 push 类型**：读-写冲突用 `PUSH_TIMESTAMP`（推高对方时间戳，不 abort）；写-写冲突用 `PUSH_ABORT`（直接 abort 低优先级事务）。
5. **死锁通过优先级比较打破**：随机 priority + push 机制确保死锁中总有一方被 abort。
6. **高优先级事务可以立刻 push**，不需要等延迟。
7. **竞争事件是可观测的**，记录在 tracing span 和系统表里。

---

## 7.11 全系列回顾

走完七章，CockroachDB 事务的完整生命周期就清晰了：

```text
┌──────────────────────────── 客户端 ────────────────────────────┐
│                                                                  │
│  DB.Txn(fn)                                                      │
│    → NewTxnWithAdmissionControl                                  │
│        HLC 取时间戳 → 生成 UUID → 建 TxnCoordSender             │
│    → txn.exec (retry loop)                                       │
│        fn(ctx, txn)                                              │
│          txn.Get/Put → Batch → sendAndFill → txn.Send            │
│            TxnCoordSender 拦截器链：                              │
│              seqNumAllocator  分配序列号                          │
│              pipeliner        异步写，追踪 in-flight              │
│              spanRefresher    追踪读 spans，尝试 refresh          │
│              committer        并行提交，STAGING 状态              │
│              lockGatekeeper   填入 ba.Txn 元数据                  │
│                                                                  │
│  DistSender                                                      │
│    → range cache 查路由 → 按 range 边界切分 → gRPC               │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                          ↕ 网络
┌──────────────────── 服务端（leaseholder） ─────────────────────┐
│                                                                  │
│  Replica.executeBatchWithConcurrencyRetries                      │
│    latchManager.Acquire      key range 级别的 rwlock             │
│    lockTable.ScanAndEnqueue  检查 intent 冲突                    │
│    → 有冲突: lockTableWaiter.WaitOn                              │
│        等待 100ms → push (PUSH_TIMESTAMP / PUSH_ABORT)           │
│        → push 成功 → 重新扫描 → 继续                             │
│    proposeRaftCommand                                            │
│      Raft.Propose → 复制到多数副本 → apply                       │
│        MVCCPut                                                   │
│          写 intent（lock table entry + MVCC 版本）                │
│                                                                  │
│  EndTxn(commit=true)                                             │
│    → 写事务记录 COMMITTED（或先 STAGING 再异步 COMMITTED）        │
│    → 异步 intent resolution（intent → 正式版本）                  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```
