# KVSubscriber 模块深度源码分析（下）

## 目录

7. [DFS 深入：handleCompleteUpdate — Store 原子替换](#7-dfs-深入handlecompleteupdate--store-原子替换)
8. [DFS 深入：handlePartialUpdate — 增量更新排序与应用](#8-dfs-深入handlepartialupdate--增量更新排序与应用)
9. [DFS 深入：Handler 通知模式](#9-dfs-深入handler-通知模式)
10. [DFS 深入：StoreReader 代理方法](#10-dfs-深入storereader-代理方法)
11. [DFS 深入：GetProtectionTimestamps 实现](#11-dfs-深入getprotectiontimestamps-实现)
12. [具体运行时示例](#12-具体运行时示例)
13. [设计模式总结](#13-设计模式总结)

---

## 7. DFS 深入：handleCompleteUpdate — Store 原子替换

**文件**：`kvsubscriber.go:411-430`

这是 KVSubscriber 最关键的函数之一，负责在 rangefeed 初始扫描完成或重建时**原子替换**整个内存 Store。

### 7.1 源码与流程

```go
func (s *KVSubscriber) handleCompleteUpdate(
    ctx context.Context, ts hlc.Timestamp, events []*BufferEvent,
) {
    // [1] 在锁外创建全新的 Store 并填充
    freshStore := spanconfigstore.New(s.fallback, s.settings, s.boundsReader, s.knobs)
    for _, ev := range events {
        freshStore.Apply(ctx, ev.Update)
    }

    // [2] 在锁内原子替换 Store 指针 + 更新时间戳 + 获取 handlers 快照
    handlers := func() []handler {
        s.mu.Lock()
        defer s.mu.Unlock()
        s.mu.internal = freshStore          // ★ 原子替换
        s.setLastUpdatedLocked(ts)
        return s.mu.handlers                // 返回 handler 快照
    }()

    // [3] 在锁外通知所有 handlers
    for i := range handlers {
        handler := &handlers[i]              // 取指针，因为 invoke 会修改 initialized 字段
        handler.invoke(ctx, keys.EverythingSpan)  // 全量通知
    }
}
```

### 7.2 为什么必须在锁外填充 Store

如果在 `mu.Lock()` 内部填充 freshStore：

```go
// ❌ 错误做法
s.mu.Lock()
freshStore := spanconfigstore.New(...)
for _, ev := range events {
    freshStore.Apply(ctx, ev.Update)  // 可能有上万个事件
}
s.mu.internal = freshStore
s.mu.Unlock()
```

初始扫描可能包含**上万条**span config 记录。`Apply` 操作涉及区间树的插入/合并，每次 Apply 的复杂度约 O(log N)。持锁期间所有读请求（`GetSpanConfigForKey`, `NeedsSplit` 等）都会阻塞，直接影响 range 的分裂和 GC 决策。

正确做法是：**在锁外完成所有耗时操作，在锁内只做指针交换**。锁内操作是 O(1) 的。

### 7.3 Store 替换的一致性保证

```
Time ────────────────────────────────────────────────────────────────►
            t0                      t1              t2
            │                       │               │
            │                       │ mu.Lock()     │
 Reader A:  │  RLock → oldStore.Get │ blocked       │ RLock → newStore.Get
            │                       │               │
 Reader B:  │                       │ blocked       │ RLock → newStore.Get
            │                       │               │
 Writer:    │  填充 freshStore      │ swap ptr      │
            │  (锁外，不阻塞读)      │ mu.Unlock()  │
```

**不变量**：
- **替换前**：所有读者看到的是 `oldStore` 的一致视图（反映替换前的 span config 状态）
- **替换后**：所有读者看到的是 `freshStore` 的一致视图（反映初始扫描截止时间 `ts` 的状态）
- **不存在中间状态**：没有读者会看到一个"半填充"的 Store

### 7.4 handler 通知语义

CompleteUpdate 时，所有 handler 收到 `keys.EverythingSpan`（`[/Min, /Max)`）。这告诉 handler：**所有 span 的配置都可能已经变化，请全量刷新你的视图**。

这是必要的，因为 CompleteUpdate 意味着整个 Store 被替换，我们无法精确知道哪些 span 的配置真正发生了变化（与之前 Store 的对比开销过大且无必要）。

### 7.5 setLastUpdatedLocked

```go
func (s *KVSubscriber) setLastUpdatedLocked(ts hlc.Timestamp) {
    s.mu.lastUpdated = ts
    nanos := timeutil.Since(s.mu.lastUpdated.GoTime()).Nanoseconds()
    s.metrics.UpdateBehindNanos.Update(nanos)
}
```

在更新 `lastUpdated` 的同时立即更新 `UpdateBehindNanos` 指标。由于此函数在持锁时调用，指标更新和时间戳更新是原子的——外部读者不会看到时间戳已更新但指标未更新的状态。

---

## 8. DFS 深入：handlePartialUpdate — 增量更新排序与应用

**文件**：`kvsubscriber.go:438-483`

### 8.1 源码与流程

```go
func (s *KVSubscriber) handlePartialUpdate(
    ctx context.Context, ts hlc.Timestamp, events []*BufferEvent,
) {
    // [1] 排序：同时间戳的事件中，删除排在添加之前
    sort.Slice(events, func(i, j int) bool {
        switch events[i].Timestamp().Compare(events[j].Timestamp()) {
        case -1: return true   // ts(i) < ts(j)
        case 1:  return false  // ts(i) > ts(j)
        case 0:  return events[i].Deletion()  // 同时间戳：删除优先
        default: panic("unexpected")
        }
    })

    // [2] 在锁内逐个应用更新
    handlers := func() []handler {
        s.mu.Lock()
        defer s.mu.Unlock()
        for _, ev := range events {
            s.mu.internal.Apply(ctx, ev.Update)  // 逐个 Apply，非批量
        }
        s.setLastUpdatedLocked(ts)
        return s.mu.handlers
    }()

    // [3] 在锁外通知 handlers（每个事件单独通知）
    for i := range handlers {
        handler := &handlers[i]
        for _, ev := range events {
            target := ev.Update.GetTarget()
            handler.invoke(ctx, target.KeyspaceTargeted())
        }
    }
}
```

### 8.2 为什么必须 "删除先于添加" 排序

考虑 range 分裂场景：

```
事务前: system.span_configurations 有一行
  [/Table/53, /Table/54) → {num_replicas=3}

事务中（range 分裂）:
  DELETE [/Table/53, /Table/54)        @ ts=T1
  INSERT [/Table/53, /Table/53/1/100)  @ ts=T1  → {num_replicas=3}
  INSERT [/Table/53/1/100, /Table/54)  @ ts=T1  → {num_replicas=5}
```

这三个操作在同一事务中执行，因此具有**相同的时间戳 T1**。Rangefeed 不保证同时间戳事件的顺序。如果应用顺序是：

```
❌ 错误顺序：
  1. INSERT [/Table/53, /Table/53/1/100)    → Store 中添加子 span
  2. DELETE [/Table/53, /Table/54)           → Store 中删除父 span（连带删除子 span！）
  3. INSERT [/Table/53/1/100, /Table/54)     → 只有这个子 span 存活

结果：[/Table/53, /Table/53/1/100) 的配置丢失！
```

```
✓ 正确顺序（删除先于添加）：
  1. DELETE [/Table/53, /Table/54)           → 清除旧配置
  2. INSERT [/Table/53, /Table/53/1/100)     → 添加新子 span
  3. INSERT [/Table/53/1/100, /Table/54)     → 添加新子 span

结果：两个子 span 都正确存在
```

排序使用 `events[i].Deletion()` 作为 tie-breaker：`Deletion()` 返回 true 的事件排在前面。

### 8.3 为什么不能批量 Apply

注释中明确解释：

```go
// NB: Even though the StoreWriter can apply a batch of updates
// atomically, the updates need to be non-overlapping. That's not the case
// here because we can have deletion events followed by additions for
// overlapping spans.
```

`spanconfigstore.Store.Apply` 支持批量更新 (`Apply(ctx, updates...)`)，但要求**批次内的更新不能重叠**。在 range 分裂场景中，DELETE `[/Table/53, /Table/54)` 和 INSERT `[/Table/53, /Table/53/1/100)` 明显重叠。因此必须逐个 Apply，确保每次 Apply 都是在前一次的基础上操作。

### 8.4 增量更新 vs 全量更新的锁持有时间对比

| 更新类型 | 锁内操作 | 典型持锁时间 |
|----------|---------|-------------|
| CompleteUpdate | 指针交换 (O(1)) | ~纳秒级 |
| IncrementalUpdate | N 次 Apply (O(N log M)) | ~微秒到毫秒级 |

其中 N = 事件数，M = Store 中的条目数。

IncrementalUpdate 的事件数通常很少（正常运行时每 3s 只有几个配置变更），所以持锁时间短。但在极端情况下（如大规模 schema 变更导致数百个 span config 同时更新），持锁时间可能达到毫秒级。

### 8.5 handler 通知粒度

与 CompleteUpdate 不同，IncrementalUpdate 为**每个事件**单独通知 handler：

```go
for _, ev := range events {
    target := ev.Update.GetTarget()
    handler.invoke(ctx, target.KeyspaceTargeted())
    // KeyspaceTargeted() 返回受影响的 span
    // 例如：[/Table/53, /Table/54)
}
```

这允许 handler 做**精确的增量处理**。例如 split queue 只需检查受影响的 span 是否需要分裂，而不是扫描所有 range。

---

## 9. DFS 深入：Handler 通知模式

**文件**：`kvsubscriber.go:485-501`

### 9.1 handler 结构体

```go
type handler struct {
    initialized bool  // 是否已完成首次全量通知
    fn          func(ctx context.Context, update roachpb.Span)
}
```

### 9.2 invoke 方法

```go
func (h *handler) invoke(ctx context.Context, update roachpb.Span) {
    if !h.initialized {
        h.fn(ctx, keys.EverythingSpan)  // 首次调用：全量通知
        h.initialized = true

        if update.Equal(keys.EverythingSpan) {
            return  // 优化：如果 update 本身就是全量，不重复调用
        }
    }

    h.fn(ctx, update)
}
```

**首次通知语义**：

当一个 handler 通过 `Subscribe()` 注册后，它可能在 KVSubscriber 已经运行了一段时间之后才注册。此时 handler 不知道当前 Store 中有什么内容。`invoke` 的 `initialized` 机制确保：

1. **第一次被调用时**：无论传入的 `update` 是什么，handler 首先收到 `EverythingSpan` 通知
2. **此后**：handler 只收到精确的增量通知

这保证了 handler 有机会全量扫描一次当前状态，之后只做增量处理。

### 9.3 handler 为什么取指针

```go
for i := range handlers {
    handler := &handlers[i]  // 取指针，不是拷贝
    handler.invoke(ctx, ...)
}
```

`invoke` 会修改 `h.initialized` 字段。如果用值拷贝 (`handler := handlers[i]`)，修改的是副本，原始的 `handlers[i].initialized` 不会被更新，导致每次 `invoke` 都触发全量通知。

### 9.4 Subscribe 方法

```go
func (s *KVSubscriber) Subscribe(fn func(context.Context, roachpb.Span)) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.mu.handlers = append(s.mu.handlers, handler{fn: fn})
}
```

**线程安全性**：`Subscribe` 使用写锁，`handleCompleteUpdate` / `handlePartialUpdate` 在持写锁时获取 handlers 快照（`return s.mu.handlers`），然后在锁外遍历快照。

**注意**：`handleUpdate` 中获取的是 slice header 的拷贝。如果在 handler 遍历期间有新的 `Subscribe` 调用，`s.mu.handlers` 的 slice 可能被 `append` 替换为新的底层数组，但遍历中的 `handlers` 变量仍指向旧数组——不会产生竞态条件。这是 Go slice 语义的天然保护。

### 9.5 Handler 注册者

在 CockroachDB 中，典型的 handler 注册者包括：

| 注册者 | 目的 | 响应动作 |
|--------|------|----------|
| `kvserver.Store` | Range 配置变更感知 | 触发 split/merge queue 检查 |
| `kvserver.replicaQueue` | GC 策略变更 | 调整 GC threshold |
| `kvserver.replicateQueue` | 副本配置变更 | 触发副本重平衡 |
| `spanconfig.Reporter` | 一致性报告 | 更新上报的 span 配置一致性状态 |

---

## 10. DFS 深入：StoreReader 代理方法

**文件**：`kvsubscriber.go:339-364`

KVSubscriber 实现了 `StoreReader` 接口，但所有方法都是简单的**读锁代理**：

### 10.1 NeedsSplit

```go
func (s *KVSubscriber) NeedsSplit(ctx context.Context, start, end roachpb.RKey) (bool, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.mu.internal.NeedsSplit(ctx, start, end)
}
```

判断 `[start, end)` 范围内是否有 span config 边界。如果有，说明当前 range 跨越了不同的配置区域，需要分裂。

### 10.2 ComputeSplitKey

```go
func (s *KVSubscriber) ComputeSplitKey(
    ctx context.Context, start, end roachpb.RKey,
) (roachpb.RKey, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.mu.internal.ComputeSplitKey(ctx, start, end)
}
```

计算 `[start, end)` 范围内的最佳分裂点。返回的 key 是 span config 边界的位置。

### 10.3 GetSpanConfigForKey

```go
func (s *KVSubscriber) GetSpanConfigForKey(
    ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, roachpb.Span, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.mu.internal.GetSpanConfigForKey(ctx, key)
}
```

获取指定 key 的 span config。同时返回该 config 适用的 span 范围（调用者可以据此判断请求是否完全在一个 config 范围内）。

**代理模式的好处**：
- 所有读操作共享 `RLock`，并发读不互斥
- `mu.internal` 可以在 `handleCompleteUpdate` 中被原子替换，读者无需感知
- Store 的内部实现可以独立演进（例如从 btree 改为其他数据结构），KVSubscriber 无需修改

---

## 11. DFS 深入：GetProtectionTimestamps 实现

**文件**：`kvsubscriber.go:367-400`

这是 `ProtectedTSReader` 接口的实现，用于查询某个 span 范围内所有活跃的受保护时间戳 (Protected Timestamp, PTS)。

### 11.1 完整源码

```go
func (s *KVSubscriber) GetProtectionTimestamps(
    ctx context.Context, sp roachpb.Span,
) (protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, _ error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    if err := s.mu.internal.ForEachOverlappingSpanConfig(ctx, sp,
        func(sp roachpb.Span, config roachpb.SpanConfig) error {
            for _, protection := range config.GCPolicy.ProtectionPolicies {
                // [过滤1] 排除不需要备份的系统 span
                if keys.ExcludeFromBackupSpan.Contains(sp) {
                    continue
                }
                // [过滤2] 排除备份写入的 PTS（如果 span 本身被排除出备份）
                if config.ExcludeDataFromBackup && protection.IgnoreIfExcludedFromBackup {
                    continue
                }
                protectionTimestamps = append(protectionTimestamps, protection.ProtectedTimestamp)
            }
            return nil
        }); err != nil {
        return nil, hlc.Timestamp{}, err
    }

    return protectionTimestamps, s.mu.lastUpdated, nil
}
```

### 11.2 调用链分析

```
GetProtectionTimestamps(ctx, span)
│
├── mu.RLock()
│
├── mu.internal.ForEachOverlappingSpanConfig(ctx, span, callback)
│   │
│   ├── 遍历 Store 中与 span 重叠的所有 SpanConfig
│   │   例如 span = [/Table/53, /Table/54)
│   │   Store 中可能有：
│   │     [/Table/53, /Table/53/1/100) → config_A
│   │     [/Table/53/1/100, /Table/54) → config_B
│   │
│   └── 对每个重叠的 (span, config)：
│       ├── 遍历 config.GCPolicy.ProtectionPolicies
│       ├── 过滤系统 span（NodeLiveness, Timeseries）
│       ├── 过滤已排除备份的 span 上的备份 PTS
│       └── 收集 protection.ProtectedTimestamp
│
├── mu.RUnlock()
│
└── return (timestamps, lastUpdated, nil)
```

### 11.3 两层过滤的设计理由

**过滤 1：ExcludeFromBackupSpan**

```go
if keys.ExcludeFromBackupSpan.Contains(sp) {
    continue
}
```

`ExcludeFromBackupSpan` 包含 **NodeLiveness** 和 **Timeseries** 等系统 span。这些 span 的特点是：

- **高写入频率**：NodeLiveness 每 4.5s 心跳一次，Timeseries 每 10s 写入指标
- **不需要备份**：集群重启后会自动重建
- **MVCC 垃圾积累快**：如果 PTS 阻止了 GC，这些 span 的数据膨胀极快

因此，即使有全局 PTS（如全集群备份），也不应该在这些 span 上生效。

**过滤 2：ExcludeDataFromBackup + IgnoreIfExcludedFromBackup**

```go
if config.ExcludeDataFromBackup && protection.IgnoreIfExcludedFromBackup {
    continue
}
```

这是一个**双向协商**机制：
- Span 侧：`config.ExcludeDataFromBackup = true` 表示"我不需要被备份"
- PTS 侧：`protection.IgnoreIfExcludedFromBackup = true` 表示"我是备份创建的，如果 span 不需要备份就忽略我"

只有两个条件同时满足时才跳过。非备份创建的 PTS（如 CDC changefeed 的 PTS，其 `IgnoreIfExcludedFromBackup = false`）不受此影响。

### 11.4 返回值语义

```go
return protectionTimestamps, s.mu.lastUpdated, nil
```

- `protectionTimestamps`：所有符合条件的受保护时间戳列表（可能包含重复，因为同一 PTS 可能出现在多个 span config 中）
- `asOf = s.mu.lastUpdated`：表示这个结果的"新鲜度"——截止到什么时间的 span config 状态
- 调用者（通常是 GC queue）使用 `asOf` 来判断结果是否足够新鲜

---

## 12. 具体运行时示例

### 12.1 正常启动与初始加载

**集群场景**：3 节点集群，100 张表，每张表有独立的 zone configuration。

```
=== 启动序列 ===

[Node 1 启动]
│
├── New() 被调用
│   ├── spanConfigStore = spanconfigstore.New(fallbackConfig, ...)
│   │   └── 空 Store，仅有 fallback 配置
│   │
│   ├── Watcher 创建
│   │   ├── bufferSize = 64MB / 5KB = 13107
│   │   ├── spans = [system.span_configurations 表的主键范围]
│   │   ├── translateEvent = SpanConfigDecoder.TranslateEvent
│   │   └── onUpdate = KVSubscriber.handleUpdate
│   │
│   └── mu.internal = 空 Store
│
├── Start() 被调用
│   ├── 启动 metrics poller goroutine (每 5s)
│   └── rangefeedcache.Start() 启动 Watcher
│
├── Watcher.Run() 开始
│   ├── buffer = New(MAX_INT)  // 无限 buffer
│   ├── initialScanTS = clock.Now() = {WallTime: 1716000000000000000, Logical: 0}
│   │
│   ├── rangeFeed.Start()
│   │   └── 初始扫描 system.span_configurations 表
│   │       └── 100 张表 × ~2 条记录/表 = ~200 条 span config 记录
│   │
│   ├── [每条记录触发 onValue]
│   │   └── SpanConfigDecoder.TranslateEvent(ev)
│   │       ├── deleted = false (初始扫描都是存在的行)
│   │       ├── decode(kv) → spanconfig.Record{Target, Config}
│   │       └── return &BufferEvent{Update(record), ev.Timestamp}
│   │   └── buffer.Add(event) → 200 个事件入 buffer
│   │
│   ├── [初始扫描完成]
│   │   └── initialScanDoneCh <- struct{}{}
│   │
│   ├── handleUpdate(ctx, buffer, initialScanTS, CompleteUpdate)
│   │   └── buffer.Flush(initialScanTS) → 返回全部 200 个事件
│   │
│   └── → KVSubscriber.handleCompleteUpdate(ctx, initialScanTS, 200 events)
│
├── handleCompleteUpdate:
│   ├── freshStore = spanconfigstore.New(...)  // 新 Store
│   ├── for 200 events: freshStore.Apply(ev.Update)
│   │   └── 200 条 span config 插入区间 B-tree
│   │
│   ├── mu.Lock()
│   ├── mu.internal = freshStore  // 原子替换
│   ├── mu.lastUpdated = initialScanTS
│   ├── handlers = mu.handlers  // 快照
│   ├── mu.Unlock()
│   │
│   └── for each handler: handler.invoke(ctx, EverythingSpan)
│       └── Split Queue, GC Queue 等收到全量通知
│           └── 遍历所有 range 检查配置一致性
│
├── buffer.SetLimit(13107)  // 切换为有限 buffer
│
└── 进入增量更新循环 ←── 等待 frontierBumpedCh
```

### 12.2 表 Zone 配置变更

**场景**：管理员修改表 `t` 的副本数从 3 改为 5。

```
=== 配置变更 ===

[SQL Layer]
  ALTER TABLE t CONFIGURE ZONE USING num_replicas=5;
  │
  ├── SQLTranslator.Translate()
  │   └── 计算 table t 的 span: [/Table/53, /Table/54)
  │
  ├── Reconciler → KVAccessor.UpdateSpanConfigRecords()
  │   ├── toDelete = [{Target: [/Table/53, /Table/54)}]        (旧配置)
  │   └── toUpsert = [{Target: [/Table/53, /Table/54),         (新配置)
  │                     Config: {NumReplicas: 5, ...}}]
  │
  └── KV Write: system.span_configurations 表更新
      ├── DELETE [/Table/53, /Table/54) old config @ ts=T1
      └── INSERT [/Table/53, /Table/54) new config @ ts=T1
      (同一事务，同一时间戳)

[Rangefeed on Node 1]
  ├── RangeFeedValue{Key=..., Value=tombstone, PrevValue=old_row, TS=T1}
  │   └── TranslateEvent → BufferEvent{Deletion([/Table/53, /Table/54)), ts=T1}
  │
  └── RangeFeedValue{Key=..., Value=new_row, PrevValue=old_row, TS=T1}
      └── TranslateEvent → BufferEvent{Update([/Table/53, /Table/54) → {NR:5}), ts=T1}

[~3s 后，Frontier 推进到 T2 >= T1]
  frontierBumpedCh <- struct{}{}
  │
  ├── buffer.Flush(T2) → [deletion_event, update_event]
  └── handleUpdate(ctx, buffer, T2, IncrementalUpdate)

[handlePartialUpdate]
  ├── sort events:
  │   ├── deletion_event: ts=T1, Deletion()=true  → 排在前
  │   └── update_event:   ts=T1, Deletion()=false → 排在后
  │
  ├── mu.Lock()
  ├── Apply(Deletion([/Table/53, /Table/54)))      → 从 B-tree 删除旧条目
  ├── Apply(Update([/Table/53, /Table/54) → {NR:5})) → 插入新条目
  ├── mu.lastUpdated = T2
  ├── mu.Unlock()
  │
  └── for each handler:
      ├── handler.invoke(ctx, [/Table/53, /Table/54))  (deletion)
      └── handler.invoke(ctx, [/Table/53, /Table/54))  (update)
          │
          └── Split Queue: 检查 [/Table/53, /Table/54) 是否需要分裂
              → NeedsSplit() → false (配置范围没变，只是内容变了)
              → 但 replicateQueue 可能触发副本调整 (3→5)
```

### 12.3 Buffer 溢出恢复

**场景**：大规模 schema 变更导致短时间内产生大量 span config 更新。

```
=== Buffer 溢出场景 ===

[前置状态]
  KVSubscriber 正常运行中
  buffer limit = 13107
  mu.internal = Store_v1 (包含 10000 条 span config)
  mu.lastUpdated = T_old

[触发]
  大规模 schema 变更产生 15000 个 span config 更新
  在一个 frontier bump 间隔（~3s）内到达

[Rangefeed onValue callback]
  ├── event 1: buffer.Add(ev1) → OK (buffer count: 1)
  ├── event 2: buffer.Add(ev2) → OK (buffer count: 2)
  ├── ...
  ├── event 13107: buffer.Add(ev13107) → OK (buffer count: 13107)
  ├── event 13108: buffer.Add(ev13108) → ERROR: buffer overflow
  │
  └── errCh <- error("buffer overflow")

[Watcher.Run() 主循环]
  select {
  case err := <-errCh:
      return err  // Run 退出
  }

[Watcher.Start() retry loop]
  ├── err = "buffer overflow"
  ├── log.Warning("spanconfig-subscriber: failed with buffer overflow, retrying...")
  ├── 等待退避时间 (50ms, 100ms, 200ms, ...)
  │
  └── Run() 重新开始
      ├── buffer = New(MAX_INT)  // 新的无限 buffer
      ├── initialScanTS = clock.Now() = T_new
      ├── rangeFeed.Start()  // 全新的初始扫描
      │
      ├── [初始扫描：所有 ~25000 条记录重新加载]
      │   └── 全部进入无限 buffer
      │
      ├── initialScanDoneCh 信号
      ├── handleCompleteUpdate(ctx, T_new, 25000 events)
      │   ├── freshStore = New(...)
      │   ├── 25000 次 Apply → 填充完整的 Store_v2
      │   ├── mu.Lock()
      │   ├── mu.internal = Store_v2  // 原子替换
      │   ├── mu.lastUpdated = T_new
      │   ├── mu.Unlock()
      │   └── handlers 收到 EverythingSpan 通知
      │
      └── buffer.SetLimit(13107)  // 恢复正常限制

[恢复完成]
  在整个恢复过程中：
  - 读者一直可以读取 Store_v1（旧数据，但一致）
  - Store_v2 切换后，读者立即看到最新数据
  - 无数据丢失，无不一致窗口
```

### 12.4 GC 决策中的 PTS 查询

**场景**：GC queue 判断是否可以 GC 某个 range 的旧版本数据。

```
=== GC 决策流程 ===

[GC Queue 处理 range r52 = [/Table/53, /Table/53/1/100)]
│
├── GetSpanConfigForKey(ctx, /Table/53)
│   ├── mu.RLock()
│   ├── mu.internal.GetSpanConfigForKey(ctx, /Table/53)
│   │   └── 区间 B-tree 查找 → config = {GCPolicy.TTLSeconds: 3600}
│   ├── mu.RUnlock()
│   └── return (config, [/Table/53, /Table/53/1/100), nil)
│
├── GC TTL = 3600s → gcThreshold = now - 3600s
│
├── GetProtectionTimestamps(ctx, [/Table/53, /Table/53/1/100))
│   ├── mu.RLock()
│   ├── ForEachOverlappingSpanConfig(ctx, span, callback)
│   │   └── span config [/Table/53, /Table/53/1/100) → config_A
│   │       └── config_A.GCPolicy.ProtectionPolicies = [
│   │             {ProtectedTimestamp: T_backup, IgnoreIfExcludedFromBackup: true},
│   │           ]
│   │
│   │   过滤检查：
│   │   ├── ExcludeFromBackupSpan.Contains([/Table/53, /Table/53/1/100))? → false
│   │   ├── config.ExcludeDataFromBackup = false
│   │   └── → 收集 T_backup
│   │
│   ├── mu.RUnlock()
│   └── return ([T_backup], lastUpdated=T_recent, nil)
│
├── protectedTS = T_backup
│   如果 T_backup > gcThreshold:
│   │   └── gcThreshold = T_backup  // PTS 阻止 GC 到 T_backup 之前
│   │
│   └── GC 只能清理 T_backup 之前的版本
│
└── 执行 GC (DeleteRange 早于 gcThreshold 的 MVCC 版本)
```

---

## 13. 设计模式总结

### 13.1 原子替换 (Atomic Swap) 模式

```
                 ┌──────────┐
  Reader A ────► │ mu.RLock │──► oldStore.Get()
  Reader B ────► │          │──► oldStore.Get()
                 └──────────┘
                      ↕  (mu.Lock 瞬间切换)
                 ┌──────────┐
  Reader A ────► │ mu.RLock │──► newStore.Get()
  Reader B ────► │          │──► newStore.Get()
                 └──────────┘
```

**核心要点**：
- 耗时操作（Store 填充）在锁外完成
- 锁内只做 O(1) 的指针交换
- 读者在替换前后都看到一致的视图
- 替换过程对读者几乎无感知（微秒级锁定）

**适用场景**：任何需要"一次性替换整个数据结构"的场景。与 RCU (Read-Copy-Update) 类似，但更简单（不需要 grace period）。

### 13.2 Frontier-Buffered Rangefeed 模式

```
RangeFeed Events → Buffer → Frontier Checkpoint → Flush → Batch Update
                   ↑                                        │
                   └── overflow → restart ──────────────────┘
```

**核心要点**：
- Rangefeed 事件是无序的（同 key 有序，不同 key 无序）
- Buffer 累积事件直到 frontier（resolved timestamp）推进
- Frontier 保证：所有 `ts <= frontier` 的事件都已收到
- Flush 一次性交付一个一致性快照
- Buffer 溢出时通过重启 rangefeed（带初始扫描）恢复

**延迟影响**：
- 正常延迟 = `kv.closed_timestamp.target_duration` (3s) + `kv.rangefeed.closed_timestamp_refresh_interval` (3s) ≈ **3-6s**
- 这意味着 span config 变更从 SQL 提交到 KV 层感知需要 3-6 秒

### 13.3 First-Invoke 初始化模式

```go
type handler struct {
    initialized bool
    fn          func(...)
}

func (h *handler) invoke(ctx, update) {
    if !h.initialized {
        h.fn(ctx, EVERYTHING)  // 全量通知
        h.initialized = true
    }
    h.fn(ctx, update)  // 增量通知
}
```

**核心要点**：
- 新注册的 handler 可能错过之前的所有更新
- 首次调用时发送全量通知，确保 handler 可以建立完整的初始视图
- 之后只发送增量通知
- 与 gRPC 的 "initial state + delta" 模式相同

### 13.4 Deletion-Before-Addition 排序模式

```
同一事务中的操作:
  DELETE [A, C)          @ ts=T1
  INSERT [A, B) → cfg1   @ ts=T1
  INSERT [B, C) → cfg2   @ ts=T1

排序后:
  1. DELETE [A, C)        (Deletion()=true → 排前)
  2. INSERT [A, B) → cfg1 (Deletion()=false → 排后)
  3. INSERT [B, C) → cfg2 (Deletion()=false → 排后)
```

**核心要点**：
- 同一事务的写入具有相同时间戳
- Rangefeed 不保证同时间戳事件的顺序
- 删除必须在添加之前处理，否则添加会被后续的删除覆盖
- 这是 span config 更新正确性的基础保证

### 13.5 Setting-Aware Timer 模式

```go
settingChangeCh := make(chan struct{}, 1)
setting.SetOnChange(&sv, func(ctx) {
    select {
    case settingChangeCh <- struct{}{}:
    default:  // 非阻塞：丢弃重复通知
    }
})

for {
    interval := setting.Get(&sv)
    timer.Reset(interval)
    select {
    case <-timer.C:
        doWork()
    case <-settingChangeCh:
        continue  // 重新读取 setting，重置 timer
    case <-stopper.ShouldQuiesce():
        return
    }
}
```

**核心要点**：
- Cluster setting 可以在运行时动态变更
- `SetOnChange` 回调通过 buffered channel 通知主循环
- Buffered channel (容量=1) + `default` 分支 = 非阻塞、去重
- 主循环收到通知后重新读取最新 setting 值
- 这个模式在 CockroachDB 中被广泛使用（KVSubscriber metrics poller, TS poller 等）

### 13.6 总体架构评价

KVSubscriber 的设计体现了 CockroachDB 在**一致性与性能**之间的平衡哲学：

| 设计决策 | 一致性保证 | 性能代价 |
|----------|-----------|---------|
| Frontier-buffered rangefeed | 按时间戳排序的一致更新 | 3-6s 延迟 |
| Store 原子替换 | 读者永远看到一致视图 | 重建时内存翻倍（新旧 Store 共存） |
| 删除排在添加之前 | 避免中间状态 | sort 开销 O(N log N)，但 N 通常很小 |
| 无限初始扫描 buffer | 保证初始加载成功 | 极端情况下内存峰值高 |
| 增量 buffer 有上限 | 防止内存 OOM | 溢出时需要重建（代价高但罕见） |

这些权衡在实际生产中被证明是合理的：span config 变更频率低（通常 < 1/s），数据量小（通常 < 10000 条记录），3-6s 的延迟对于配置传播完全可以接受。
