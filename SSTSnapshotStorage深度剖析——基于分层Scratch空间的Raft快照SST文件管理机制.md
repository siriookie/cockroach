# SSTSnapshotStorage深度剖析——基于分层Scratch空间的Raft快照SST文件管理机制

## 一、第一轮 BFS：职责边界与设计动机（Why）

### 1.1 系统性问题背景

在 CockroachDB 的 Raft 复制协议中，当一个副本（Replica）落后太多时，leader 不会通过逐条发送 Raft log entries 来追赶它，而是直接发送一个**快照（Snapshot）**。这个快照包含了整个 Range 的完整状态（通常是数百 MB 甚至 GB 级别的数据）。

快照传输面临的核心问题：

**问题 1：内存压力**
- 如果将整个快照数据缓存在内存中再写入磁盘，会导致接收节点的内存占用激增
- 多个 Range 同时接收快照时，内存压力会成倍增加
- 可能触发 OOM 或影响正常读写请求

**问题 2：磁盘 IO 管理**
- 快照写入是大量连续的磁盘写操作，需要进行**速率限制**（rate limiting）
- 如果不加控制，会影响正常的事务处理 IO
- 需要与其他 IO 操作（如 compaction、normal writes）协调

**问题 3：快照生命周期管理**
- 快照接收是一个**可能失败**的过程（网络中断、节点崩溃、校验失败等）
- 失败的快照需要及时清理，避免磁盘空间泄漏
- 多个快照可能针对同一个 Range（例如重试、并发发送）

**问题 4：文件组织与原子性**
- Raft 快照包含多个 SST 文件（RocksDB/Pebble 的 Sorted String Table）
- 这些 SST 文件分属不同的 key span（local keys、MVCC keys 等）
- 需要确保快照接收的**原子性**：要么全部成功，要么全部回滚

### 1.2 如果没有 SSTSnapshotStorage 会怎样

假设没有专门的快照存储管理机制：

1. **内存管理混乱**：应用层需要手动管理内存缓冲区，容易导致内存泄漏
2. **磁盘空间泄漏**：失败的快照文件无法被自动清理，需要手动 GC
3. **IO 风暴**：快照写入无法与其他 IO 操作协调，导致性能抖动
4. **并发安全问题**：多个 goroutine 同时处理不同 Range 的快照时，文件路径冲突
5. **恢复复杂性**：节点重启后，无法识别哪些快照文件是"孤儿"（对应已中止的快照）

### 1.3 在系统中的位置

```
                    Raft Snapshot Reception Flow
                              |
         +--------------------+--------------------+
         |                                         |
    Store.receiveSnapshot()              Store.processRaftSnapshot()
         |                                         |
         +---> SSTSnapshotStorage.NewScratchSpace() <--- (单例，Store 级别)
                         |
                         v
              SSTSnapshotStorageScratch -----> MultiSSTWriter
                         |                           |
                         v                           v
              SSTSnapshotStorageFile          [SST 文件写入]
                         |
                         v
                  [磁盘：auxiliary/sstsnapshot/{rangeID}/{snapUUID}/]
```

**上游调用者**：
- `kvserver.Store`：在初始化时创建 `SSTSnapshotStorage` 单例
- Raft 快照接收逻辑：调用 `NewScratchSpace()` 为每个快照创建独立的临时工作区

**下游依赖**：
- `storage.Engine`：提供文件系统抽象（`fs.Env`）
- `rate.Limiter`：提供 IO 速率限制（从 `Store.limiters.BulkIOWriteRate` 获取）
- `MultiSSTWriter`：消费 `SSTSnapshotStorageScratch`，将快照数据分割成多个 SST 文件

### 1.4 核心抽象与生命周期

#### 核心对象 1：SSTSnapshotStorage（单例，Store 级别）

```go
type SSTSnapshotStorage struct {
    env     *fs.Env           // 文件系统抽象（支持测试用的虚拟文件系统）
    limiter *rate.Limiter     // IO 速率限制器（全局共享）
    dir     string            // 根目录：{engine_aux_dir}/sstsnapshot
    mu      struct {
        syncutil.Mutex
        rangeRefCount map[roachpb.RangeID]int  // 每个 Range 的活跃 Scratch 计数
    }
}
```

**生命周期**：
- 创建：`Store.Start()` 初始化阶段
- 销毁：`Store.Stop()` 时调用 `Clear()` 删除整个 `sstsnapshot` 目录

**职责**：
- 管理快照文件的根目录
- 跟踪每个 Range 的活跃 Scratch 数量（引用计数）
- 在所有 Scratch 关闭后，自动清理 Range 级别的目录

#### 核心对象 2：SSTSnapshotStorageScratch（每个快照一个实例）

```go
type SSTSnapshotStorageScratch struct {
    storage    *SSTSnapshotStorage   // 反向引用父对象
    st         *cluster.Settings     // 集群设置（用于 SST 写入器）
    rangeID    roachpb.RangeID       // 所属 Range
    ssts       []string              // 已创建的 SST 文件路径列表
    snapDir    string                // 该快照的专属目录：{dir}/{rangeID}/{snapUUID}
    dirCreated bool                  // 延迟创建标记
    closed     bool                  // 防止重复关闭
}
```

**生命周期**：
- 创建：每次接收快照时调用 `NewScratchSpace(rangeID, snapUUID, settings)`
- 销毁：快照接收完成（成功或失败）后调用 `Close()`

**职责**：
- 延迟创建快照专属目录（只有在第一次写入时才创建）
- 管理该快照的所有 SST 文件
- 关闭时清理自身的 `snapDir` 目录

#### 核心对象 3：SSTSnapshotStorageFile（每个 SST 文件一个实例）

```go
type SSTSnapshotStorageFile struct {
    scratch      *SSTSnapshotStorageScratch
    created      bool           // 延迟创建标记
    file         vfs.File       // Pebble 的虚拟文件系统接口
    filename     string         // 文件路径：{snapDir}/{id}.sst
    ctx          context.Context
    bytesPerSync int64          // 每写入 N 字节后调用 Sync()（平滑 IO）
}
```

**生命周期**：
- 创建：调用 `Scratch.NewFile(ctx, bytesPerSync)` 时
- 销毁：调用 `Finish()` 或 `Abort()` 后

**职责**：
- 延迟创建物理文件（只有在第一次 `Write()` 时才创建）
- 实现 `objstorage.Writable` 接口（Pebble 的 SST 写入接口）
- 写入时通过 `limiter` 进行速率限制
- 支持 `bytesPerSync` 参数，定期调用 `Sync()` 平滑磁盘写入

### 1.5 设计动机总结

**核心理念**：**分层的、延迟创建的、自动清理的临时文件管理器**

1. **分层隔离**：
   - Storage 层：管理所有快照的根目录
   - Scratch 层：管理单个快照的所有文件
   - File 层：管理单个 SST 文件的 IO

2. **延迟创建**：
   - 目录和文件只有在**真正需要写入时**才创建
   - 避免空目录和空文件的垃圾残留

3. **引用计数**：
   - 通过 `rangeRefCount` 跟踪每个 Range 的活跃快照数量
   - 当最后一个快照关闭时，自动删除整个 Range 目录

4. **速率限制集成**：
   - 所有写入操作统一通过 `limiter` 进行流控
   - 避免快照传输影响正常事务 IO

---

## 二、第二轮 BFS：控制流与组件协作（How it flows）

### 2.1 主要执行路径：快照接收的完整生命周期

#### 阶段 0：Store 初始化（一次性）

```
Store.Start()
    └─> s.sstSnapshotStorage = snaprecv.NewSSTSnapshotStorage(
            s.StateEngine(),           // Pebble engine
            s.limiters.BulkIOWriteRate // rate.Limiter
        )
```

**状态变化**：
- `SSTSnapshotStorage` 对象被创建
- `dir` 设置为 `{engine_aux_dir}/sstsnapshot`
- `rangeRefCount` 初始化为空 map

**关键点**：此时**不会**创建任何目录，完全延迟到第一次快照接收时。

---

#### 阶段 1：接收快照开始（每个快照）

```
Raft Snapshot Received for Range R1 with UUID snap-123
    └─> scratch := sstSnapshotStorage.NewScratchSpace(R1, snap-123, settings)
```

**执行步骤**：

```go
// sst_snapshot_storage.go:56-69
func (s *SSTSnapshotStorage) NewScratchSpace(
    rangeID roachpb.RangeID, snapUUID uuid.UUID, st *cluster.Settings,
) *SSTSnapshotStorageScratch {
    // Step 1: 增加引用计数（加锁保护）
    s.mu.Lock()
    s.mu.rangeRefCount[rangeID]++
    s.mu.Unlock()

    // Step 2: 构造快照专属目录路径
    snapDir := filepath.Join(s.dir, strconv.Itoa(int(rangeID)), snapUUID.String())
    // 例如：/data/auxiliary/sstsnapshot/123/550e8400-e29b-41d4-a716-446655440000

    // Step 3: 返回 Scratch 对象（此时目录尚未创建）
    return &SSTSnapshotStorageScratch{
        storage: s,
        st:      st,
        rangeID: rangeID,
        snapDir: snapDir,
    }
}
```

**状态变化**：
- `rangeRefCount[R1]` 从 0 变为 1（或递增）
- 返回的 `scratch` 对象的 `dirCreated` 为 `false`

**关键点**：
- 此时**没有任何磁盘 IO**
- 如果快照接收立即失败（例如参数校验失败），不会留下任何文件

---

#### 阶段 2：创建 SST 文件（首次写入时）

```
MultiSSTWriter.initSST(ctx)
    └─> file := scratch.NewFile(ctx, 512<<10 /* 512 KB */)
        └─> [延迟创建]
    └─> file.Write(sstData)
        └─> [触发目录和文件的实际创建]
```

**执行步骤**：

**2.1 创建 File 对象（无 IO）**

```go
// sst_snapshot_storage.go:126-142
func (s *SSTSnapshotStorageScratch) NewFile(
    ctx context.Context, bytesPerSync int64,
) (*SSTSnapshotStorageFile, error) {
    if s.closed {
        return nil, errors.AssertionFailedf("SSTSnapshotStorageScratch closed")
    }

    // 分配文件 ID（从 0 开始递增）
    id := len(s.ssts)
    filename := s.filename(id)  // {snapDir}/{id}.sst

    // 追加到文件列表（即使文件尚未创建）
    s.ssts = append(s.ssts, filename)

    // 返回 File 对象
    return &SSTSnapshotStorageFile{
        scratch:      s,
        filename:     filename,
        ctx:          ctx,
        bytesPerSync: bytesPerSync,
    }, nil
}
```

**2.2 首次写入时创建目录和文件**

```go
// sst_snapshot_storage.go:240-253
func (f *SSTSnapshotStorageFile) Write(contents []byte) error {
    if len(contents) == 0 {
        return nil  // 空写入不触发创建
    }

    // Step 1: 确保文件已创建
    if err := f.ensureFile(); err != nil {
        return err
    }

    // Step 2: 速率限制（关键！）
    if err := kvserverbase.LimitBulkIOWrite(
        f.ctx, f.scratch.storage.limiter, len(contents)
    ); err != nil {
        return err  // 返回 context.Canceled 或速率限制错误
    }

    // Step 3: 实际写入
    _, err := f.file.Write(contents)
    return err
}

// sst_snapshot_storage.go:206-232
func (f *SSTSnapshotStorageFile) ensureFile() error {
    if f.created {
        if f.file == nil {
            return errors.Errorf("file has already been closed")
        }
        return nil  // 已创建，直接返回
    }

    // Step 1: 如果目录未创建，先创建目录
    if !f.scratch.dirCreated {
        if err := f.scratch.createDir(); err != nil {
            return err
        }
    }

    // Step 2: 创建文件
    var err error
    if f.bytesPerSync > 0 {
        // 使用同步写入（定期调用 Sync）
        f.file, err = fs.CreateWithSync(
            f.scratch.storage.env, f.filename,
            int(f.bytesPerSync), fs.RaftSnapshotWriteCategory
        )
    } else {
        // 普通写入
        f.file, err = f.scratch.storage.env.Create(
            f.filename, fs.RaftSnapshotWriteCategory
        )
    }
    if err != nil {
        return err
    }

    f.created = true
    return nil
}
```

**状态变化**：
- 首次写入时：
  - `scratch.dirCreated` 变为 `true`
  - 磁盘上创建目录：`/data/auxiliary/sstsnapshot/123/snap-123/`
  - 磁盘上创建文件：`/data/auxiliary/sstsnapshot/123/snap-123/0.sst`
  - `file.created` 变为 `true`

**关键点**：
- **延迟创建**的精髓：只有在真正需要写入数据时才创建文件
- **速率限制**在写入时自动应用，无需上层逻辑处理

---

#### 阶段 3：SST 文件写入完成

```
MultiSSTWriter.Finish()
    └─> file.Finish()
```

**执行步骤**：

```go
// sst_snapshot_storage.go:256-269
func (f *SSTSnapshotStorageFile) Finish() error {
    // 检查：空文件是错误的（SST 必须包含数据）
    if !f.created {
        return errors.New("file is empty")
    }

    // Step 1: 同步数据到磁盘
    errSync := f.file.Sync()

    // Step 2: 关闭文件句柄
    errClose := f.file.Close()
    f.file = nil

    // Step 3: 返回错误（优先返回 Sync 错误）
    if errSync != nil {
        return errSync
    }
    return errClose
}
```

**状态变化**：
- 文件数据落盘（持久化）
- `f.file` 设置为 `nil`（防止重复关闭）

**关键点**：
- 如果 `Finish()` 返回错误，上层逻辑会调用 `scratch.Close()` 清理所有文件
- 空文件检查确保不会生成无效的 SST（Pebble 无法 ingest 空 SST）

---

#### 阶段 4：快照接收完成（成功或失败）

```
Snapshot reception completed (success or failure)
    └─> scratch.Close()
        └─> 删除 snapDir 目录
        └─> storage.scratchClosed(rangeID)
            └─> 减少引用计数
            └─> 如果引用计数为 0，删除 Range 目录
```

**执行步骤**：

```go
// sst_snapshot_storage.go:184-191
func (s *SSTSnapshotStorageScratch) Close() error {
    if s.closed {
        return nil  // 幂等性：重复关闭不报错
    }
    s.closed = true

    // Step 1: 通知父对象（引用计数 -1）
    defer s.storage.scratchClosed(s.rangeID)

    // Step 2: 删除快照目录（包括所有 SST 文件）
    return s.storage.env.RemoveAll(s.snapDir)
}

// sst_snapshot_storage.go:80-96
func (s *SSTSnapshotStorage) scratchClosed(rangeID roachpb.RangeID) {
    s.mu.Lock()
    defer s.mu.Unlock()

    // Step 1: 减少引用计数
    val := s.mu.rangeRefCount[rangeID]
    if val <= 0 {
        panic("inconsistent scratch ref count")  // 防御性检查
    }
    val--
    s.mu.rangeRefCount[rangeID] = val

    // Step 2: 如果没有更多活跃快照，清理 Range 目录
    if val == 0 {
        delete(s.mu.rangeRefCount, rangeID)

        // 删除 Range 目录（抑制错误）
        // 注释：孤儿目录最多影响 pebble.Capacity() 性能，不影响正确性
        _ = s.env.RemoveAll(filepath.Join(s.dir, strconv.Itoa(int(rangeID))))
    }
}
```

**状态变化**：
- `snapDir` 被删除（无论快照成功还是失败）
- `rangeRefCount[rangeID]` 减 1
- 如果 `rangeRefCount[rangeID]` 变为 0：
  - 从 map 中删除该 Range 的条目
  - 删除整个 Range 目录（例如 `/data/auxiliary/sstsnapshot/123/`）

**关键点**：
- **自动清理**：无需手动调用 GC 逻辑
- **引用计数**：支持同一 Range 的多个并发快照
- **错误抑制**：Range 目录删除失败不会影响快照的逻辑状态

---

### 2.2 并发场景：同一 Range 的多个快照

**场景**：Range R1 同时接收两个快照（例如重试、或从不同 leader 发送）

```
Time    Thread 1 (snap-123)                     Thread 2 (snap-456)
----    ---------------------                   ---------------------
T0      NewScratchSpace(R1, snap-123)
        → rangeRefCount[R1] = 1

T1                                              NewScratchSpace(R1, snap-456)
                                                → rangeRefCount[R1] = 2

T2      scratch1.NewFile(...)
        → 创建 /sstsnapshot/1/snap-123/0.sst

T3                                              scratch2.NewFile(...)
                                                → 创建 /sstsnapshot/1/snap-456/0.sst

T4      scratch1.Close()
        → 删除 /sstsnapshot/1/snap-123/
        → rangeRefCount[R1] = 1
        → Range 目录保留（还有活跃快照）

T5                                              scratch2.Close()
                                                → 删除 /sstsnapshot/1/snap-456/
                                                → rangeRefCount[R1] = 0
                                                → 删除 /sstsnapshot/1/（Range 目录）
```

**关键保证**：
- 每个快照有独立的 `snapDir`（通过 UUID 隔离）
- 引用计数确保不会过早删除 Range 目录
- 锁保护确保 `rangeRefCount` 的修改是原子的

---

### 2.3 异常场景：快照接收失败

**场景 1：网络中断，快照接收中止**

```
NewScratchSpace(R1, snap-123)
    → rangeRefCount[R1] = 1
NewFile(ctx, ...)
    → 创建 /sstsnapshot/1/snap-123/0.sst
Write(data1)
    → 写入部分数据
[网络中断]
scratch.Close()
    → 删除 /sstsnapshot/1/snap-123/（包括部分写入的文件）
    → rangeRefCount[R1] = 0
    → 删除 /sstsnapshot/1/
```

**结果**：没有任何残留文件。

---

**场景 2：节点崩溃，快照接收中断**

```
NewScratchSpace(R1, snap-123)
    → rangeRefCount[R1] = 1（内存状态）
NewFile(ctx, ...)
    → 创建 /sstsnapshot/1/snap-123/0.sst（磁盘状态）
[节点崩溃]
[节点重启]
Store.Start()
    → sstSnapshotStorage.Clear()
        → 删除整个 /sstsnapshot/ 目录
```

**结果**：重启时删除所有孤儿文件。

**关键设计**：`SSTSnapshotStorage` 的状态（`rangeRefCount`）是**纯内存的**，重启后会丢失，因此需要在启动时调用 `Clear()` 清理所有残留。

---

### 2.4 触发方式总结

| 操作                  | 触发方式           | 频率           |
|-----------------------|-------------------|---------------|
| NewSSTSnapshotStorage | Store 启动时       | 一次          |
| NewScratchSpace       | 每个快照接收       | 按需          |
| NewFile               | 每个 SST 文件      | 按需          |
| Write                 | SST 数据写入       | 频繁          |
| Finish                | SST 文件完成       | 按需          |
| scratch.Close         | 快照完成或失败     | 按需          |
| Clear                 | Store 启动或停止   | 一次          |

---

## 三、DFS 深入：关键函数与核心逻辑（How it works）

### 3.1 NewSSTSnapshotStorage：单例初始化

```go
// sst_snapshot_storage.go:42-52
func NewSSTSnapshotStorage(engine storage.Engine, limiter *rate.Limiter) SSTSnapshotStorage {
    return SSTSnapshotStorage{
        env:     engine.Env(),
        limiter: limiter,
        dir:     filepath.Join(engine.GetAuxiliaryDir(), "sstsnapshot"),
        mu: struct {
            syncutil.Mutex
            rangeRefCount map[roachpb.RangeID]int
        }{rangeRefCount: make(map[roachpb.RangeID]int)},
    }
}
```

**输入**：
- `engine storage.Engine`：Pebble 存储引擎实例
- `limiter *rate.Limiter`：全局 IO 速率限制器

**输出**：
- `SSTSnapshotStorage` 结构体（**注意不是指针**）

**关键点 1：为什么返回值不是指针？**

```go
// store.go 中的使用
s.sstSnapshotStorage = snaprecv.NewSSTSnapshotStorage(...)
```

这是 Go 的一个常见模式：
- 返回结构体值（而不是指针）可以避免堆分配
- `SSTSnapshotStorage` 结构体本身很小（3 个字段 + 一个嵌套结构体）
- 由于 `rangeRefCount` 是 map（引用类型），实际的 map 数据仍然在堆上
- 这种设计在**拷贝开销小且需要值语义**时很常见

**关键点 2：`engine.GetAuxiliaryDir()` 的作用**

```go
// 假设 engine 的主数据目录是 /data/cockroach
engine.GetAuxiliaryDir()  // 返回 /data/cockroach/auxiliary
```

Auxiliary 目录用于存储**非持久化的、可重建的**临时数据：
- Raft 快照接收的 SST 文件（本模块）
- 临时排序文件
- 日志文件

与主数据目录分离的好处：
- 可以单独清理而不影响持久化数据
- 可以挂载到不同的磁盘（例如更快的 SSD）

**关键点 3：为什么 `rangeRefCount` 用 map 而不是 sync.Map？**

- 访问模式：**写多读少**（每个快照开始和结束时各写一次）
- 锁粒度：操作非常快（只是 map 的增删改），持锁时间极短
- `sync.Map` 适用于**读多写少**的场景，这里用普通 map + Mutex 更简单高效

**不变量**：
- `rangeRefCount[rangeID] >= 1`（如果 rangeID 在 map 中）
- 如果 `rangeRefCount[rangeID] == 0`，该 rangeID 必须从 map 中删除

---

### 3.2 NewScratchSpace：创建快照工作区

```go
// sst_snapshot_storage.go:56-69
func (s *SSTSnapshotStorage) NewScratchSpace(
    rangeID roachpb.RangeID, snapUUID uuid.UUID, st *cluster.Settings,
) *SSTSnapshotStorageScratch {
    s.mu.Lock()
    s.mu.rangeRefCount[rangeID]++
    s.mu.Unlock()

    snapDir := filepath.Join(s.dir, strconv.Itoa(int(rangeID)), snapUUID.String())

    return &SSTSnapshotStorageScratch{
        storage: s,
        st:      st,
        rangeID: rangeID,
        snapDir: snapDir,
    }
}
```

**输入**：
- `rangeID`：快照所属的 Range ID
- `snapUUID`：快照的唯一标识符（用于区分同一 Range 的不同快照）
- `st`：集群设置（传递给 `MultiSSTWriter`）

**输出**：
- `*SSTSnapshotStorageScratch` 指针

**关键点 1：为什么这次返回指针？**

- `Scratch` 对象需要被多个组件共享（`MultiSSTWriter`、`SSTSnapshotStorageFile`）
- `Scratch` 有状态（`ssts` 列表、`dirCreated` 标志、`closed` 标志）
- 必须确保所有引用者看到同一个对象

**关键点 2：目录结构的设计**

```
{engine_aux_dir}/sstsnapshot/
    ├── 1/                          ← Range ID 1
    │   ├── 550e8400.../            ← Snapshot UUID 1
    │   │   ├── 0.sst
    │   │   ├── 1.sst
    │   │   └── 2.sst
    │   └── 7c9e6679.../            ← Snapshot UUID 2
    │       └── 0.sst
    └── 42/                         ← Range ID 42
        └── 9b3f5a82.../
            └── 0.sst
```

**为什么要两层目录？**

1. **第一层（Range ID）**：
   - 方便在所有快照完成后删除整个 Range 目录
   - 支持同一 Range 的多个并发快照

2. **第二层（Snapshot UUID）**：
   - 保证不同快照的文件不会冲突
   - UUID 是全局唯一的，无需担心命名冲突

**关键点 3：引用计数的原子性**

```go
s.mu.Lock()
s.mu.rangeRefCount[rangeID]++  // 必须在锁内完成
s.mu.Unlock()
```

**为什么不能使用 `atomic.AddInt32`？**

```go
// ❌ 错误示例
atomic.AddInt32(&s.mu.rangeRefCount[rangeID], 1)
```

问题：
- `map[...]int` 不是线程安全的
- 即使每个元素用原子操作，map 本身的增删也需要锁

**并发安全分析**：

| 场景                          | 保护机制           |
|-------------------------------|-------------------|
| 同一 Range 的多个快照          | 锁保护引用计数     |
| 不同 Range 的快照              | 无竞争（不同 key） |
| 快照创建和关闭的并发           | 锁保护引用计数     |

---

### 3.3 SSTSnapshotStorageFile.Write：速率限制的写入

```go
// sst_snapshot_storage.go:240-253
func (f *SSTSnapshotStorageFile) Write(contents []byte) error {
    if len(contents) == 0 {
        return nil
    }

    if err := f.ensureFile(); err != nil {
        return err
    }

    // 关键：速率限制
    if err := kvserverbase.LimitBulkIOWrite(
        f.ctx, f.scratch.storage.limiter, len(contents)
    ); err != nil {
        return err
    }

    _, err := f.file.Write(contents)
    return err
}
```

**输入**：
- `contents []byte`：要写入的数据

**输出**：
- `error`：成功返回 `nil`，失败返回错误

**关键点 1：速率限制的实现**

```go
// 简化版的 LimitBulkIOWrite 逻辑
func LimitBulkIOWrite(ctx context.Context, limiter *rate.Limiter, bytes int) error {
    // 预留 tokens
    reservation := limiter.ReserveN(time.Now(), bytes)

    if !reservation.OK() {
        return errors.Errorf("rate limit exceeded")
    }

    // 等待直到允许写入
    delay := reservation.Delay()
    if delay > 0 {
        select {
        case <-time.After(delay):
            return nil
        case <-ctx.Done():
            reservation.Cancel()  // 归还 tokens
            return ctx.Err()
        }
    }

    return nil
}
```

**为什么在 `Write()` 中而不是在 `Finish()` 中限流？**

| 时机         | 优点                        | 缺点                      |
|--------------|----------------------------|--------------------------|
| Write() 中   | 实时限流，平滑 IO           | 每次写入都有开销          |
| Finish() 中  | 开销集中                    | 无法平滑 IO，容易突发     |

CockroachDB 选择在 `Write()` 中限流，因为：
- 快照写入通常是**大块数据**（几百 KB 到几 MB）
- 实时限流可以避免 IO 突发
- 配合 `bytesPerSync` 参数，可以平滑磁盘压力

**关键点 2：Context 取消的响应性**

```go
if err := LimitBulkIOWrite(f.ctx, ...); err != nil {
    return err  // 可能是 context.Canceled
}
```

测试用例验证了这一点：

```go
// sst_snapshot_storage_test.go:258-261
cancel()
err = f.Write([]byte("bar"))
require.ErrorIs(t, err, context.Canceled)
```

**设计意图**：
- 如果快照接收被取消（例如 Raft 状态变化），应该立即中止写入
- 避免浪费 IO 带宽和磁盘空间

**关键点 3：空写入的处理**

```go
if len(contents) == 0 {
    return nil  // 不触发文件创建
}
```

**为什么要特殊处理？**

- 某些调用者可能会先调用 `Write(nil)` 进行"探测"
- 空写入不应该触发目录和文件的创建
- 避免生成空文件（`Finish()` 会检查并报错）

---

### 3.4 ensureFile：延迟创建的实现

```go
// sst_snapshot_storage.go:206-232
func (f *SSTSnapshotStorageFile) ensureFile() error {
    // Step 1: 快速路径（文件已创建）
    if f.created {
        if f.file == nil {
            return errors.Errorf("file has already been closed")
        }
        return nil
    }

    // Step 2: 创建目录（如果需要）
    if !f.scratch.dirCreated {
        if err := f.scratch.createDir(); err != nil {
            return err
        }
    }

    // Step 3: 检查 Scratch 是否已关闭
    if f.scratch.closed {
        return errors.AssertionFailedf("SSTSnapshotStorageScratch closed")
    }

    // Step 4: 创建文件
    var err error
    if f.bytesPerSync > 0 {
        f.file, err = fs.CreateWithSync(
            f.scratch.storage.env, f.filename,
            int(f.bytesPerSync), fs.RaftSnapshotWriteCategory
        )
    } else {
        f.file, err = f.scratch.storage.env.Create(
            f.filename, fs.RaftSnapshotWriteCategory
        )
    }
    if err != nil {
        return err
    }

    f.created = true
    return nil
}
```

**不变量**：
- 在 `ensureFile()` 返回 `nil` 后，`f.file` 必须是有效的文件句柄
- `f.created == true` 意味着文件已创建且未关闭
- `f.created == true && f.file == nil` 意味着文件已关闭

**关键点 1：`createDir()` 的幂等性**

```go
// sst_snapshot_storage.go:115-119
func (s *SSTSnapshotStorageScratch) createDir() error {
    err := s.storage.env.MkdirAll(s.snapDir, os.ModePerm)
    s.dirCreated = s.dirCreated || err == nil
    return err
}
```

**为什么要 `s.dirCreated || err == nil` 而不是简单的 `s.dirCreated = true`？**

```go
// 第一次调用
createDir()  // err == nil, dirCreated = false || true = true

// 第二次调用（假设目录被外部删除）
createDir()  // err != nil, dirCreated = true || false = true
```

**设计意图**：
- 一旦目录创建成功，`dirCreated` 就永远为 `true`
- 即使后续调用失败（例如权限变化），也不会重置标志
- 避免反复尝试创建已删除的目录

**关键点 2：`fs.CreateWithSync` 的作用**

```go
if f.bytesPerSync > 0 {
    f.file, err = fs.CreateWithSync(
        ..., int(f.bytesPerSync), fs.RaftSnapshotWriteCategory
    )
}
```

`CreateWithSync` 返回的文件会：
- 每写入 `bytesPerSync` 字节后自动调用 `Sync()`
- 平滑磁盘写入压力（避免大量数据积压在 page cache）

**为什么不是每次 `Write()` 都调用 `Sync()`？**

| 策略            | 优点              | 缺点                    |
|-----------------|------------------|------------------------|
| 每次 Write 都 Sync | 最强持久性        | 性能极差（系统调用开销） |
| 定期 Sync      | 平衡性能和持久性   | 需要额外逻辑            |
| 只在 Finish 时 Sync | 性能最佳          | 写入压力集中            |

CockroachDB 选择**定期 Sync**（通过 `bytesPerSync` 控制），测试用例中使用 512 KB：

```go
// sst_snapshot_storage.go:167
f, err := s.NewFile(ctx, 512<<10 /* 512 KB */)
```

**关键点 3：`fs.RaftSnapshotWriteCategory` 的作用**

这是一个**IO 优先级标签**，用于：
- 在文件系统层面区分不同类型的 IO
- 可能映射到 Linux 的 `ioprio_set()` 系统调用
- 允许操作系统或存储层对不同类型的 IO 进行调度

例如：
- `RaftSnapshotWriteCategory`：可能是低优先级（后台任务）
- `ForegroundWriteCategory`：可能是高优先级（用户请求）

---

### 3.5 scratchClosed：引用计数与清理

```go
// sst_snapshot_storage.go:80-96
func (s *SSTSnapshotStorage) scratchClosed(rangeID roachpb.RangeID) {
    s.mu.Lock()
    defer s.mu.Unlock()

    // Step 1: 减少引用计数
    val := s.mu.rangeRefCount[rangeID]
    if val <= 0 {
        panic("inconsistent scratch ref count")
    }
    val--
    s.mu.rangeRefCount[rangeID] = val

    // Step 2: 如果没有更多活跃快照，清理 Range 目录
    if val == 0 {
        delete(s.mu.rangeRefCount, rangeID)

        // 删除 Range 目录（抑制错误）
        _ = s.env.RemoveAll(filepath.Join(s.dir, strconv.Itoa(int(rangeID))))
    }
}
```

**输入**：
- `rangeID`：快照所属的 Range ID

**输出**：无

**关键点 1：为什么 `panic` 而不是返回错误？**

```go
if val <= 0 {
    panic("inconsistent scratch ref count")
}
```

这是一个**防御性断言**：
- 如果引用计数小于等于 0，说明代码有 bug（例如重复调用 `Close()`）
- 这种情况**不应该**被静默处理，必须立即暴露问题
- 在生产环境中，panic 会被上层的 `recover()` 捕获并记录

**关键点 2：为什么抑制 `RemoveAll` 的错误？**

```go
_ = s.env.RemoveAll(...)  // 忽略错误
```

代码注释解释了原因：

```go
// Suppressing an error here is okay, as orphaned directories are at worst
// a performance issue when we later walk directories in pebble.Capacity()
// but not a correctness issue.
```

**原因**：
- 删除目录失败（例如权限问题、磁盘满）**不影响**系统的正确性
- 孤儿目录最多会：
  - 浪费磁盘空间（但 auxiliary 目录不计入数据容量）
  - 影响 `pebble.Capacity()` 的性能（需要遍历更多文件）
- 如果返回错误，上层逻辑需要处理（例如重试、记录），增加复杂度

**权衡**：
- 优点：简化错误处理，避免误导性的错误传播
- 缺点：可能隐藏配置问题（例如磁盘权限错误）

**最佳实践**：在生产环境中，应该有独立的监控来检测孤儿目录的积累。

**关键点 3：为什么在删除 map 条目后再删除目录？**

```go
if val == 0 {
    delete(s.mu.rangeRefCount, rangeID)  // 先删除
    _ = s.env.RemoveAll(...)             // 后删除目录
}
```

**如果顺序反过来会怎样？**

```go
// ❌ 错误顺序
if val == 0 {
    _ = s.env.RemoveAll(...)             // 先删除目录
    delete(s.mu.rangeRefCount, rangeID)  // 后删除
}
```

**潜在问题**：
- 在删除目录和删除 map 条目之间，可能有**新的快照**开始
- 新快照会递增 `rangeRefCount[rangeID]`（从 0 变为 1）
- 但目录已经被删除，新快照会创建新的目录

**结果**：逻辑上正确，但会导致目录被重建。

**当前顺序的优势**：
- 先删除 map 条目，确保不会有并发的快照看到 `rangeRefCount[rangeID] == 0`
- 再删除目录，确保目录删除失败不会影响引用计数的一致性

---

## 四、运行时行为与系统反馈（Runtime Behavior）

### 4.1 系统如何感知运行时信号

#### 信号 1：快照接收速率（吞吐量）

**感知方式**：通过 `rate.Limiter` 间接感知

```go
// 假设 limiter 配置为 100 MB/s
limiter := rate.NewLimiter(100*1024*1024, 10*1024*1024)  // 100 MB/s，10 MB burst
```

**反馈路径**：

```
快照数据到达
    ↓
SSTSnapshotStorageFile.Write()
    ↓
LimitBulkIOWrite(ctx, limiter, len(contents))
    ↓
limiter.ReserveN(time.Now(), bytes)
    ↓
[如果速率超限] → 阻塞写入
    ↓
快照接收速度降低
```

**关键点**：
- 速率限制是**全局的**（所有快照共享同一个 `limiter`）
- 如果多个 Range 同时接收快照，总速率不会超过限制
- 速率限制是**事前的**（预留 tokens 后再写入），而不是事后惩罚

#### 信号 2：磁盘空间不足

**感知方式**：通过文件系统的写入错误

```go
_, err := f.file.Write(contents)
if err != nil {
    // 可能的错误：
    // - "no space left on device"
    // - "disk quota exceeded"
    return err
}
```

**反馈路径**：

```
磁盘空间不足
    ↓
Write() 返回错误
    ↓
MultiSSTWriter 捕获错误
    ↓
Scratch.Close()（清理已写入的文件）
    ↓
快照接收失败
    ↓
Raft 层重新请求快照
```

**关键点**：
- **没有主动的空间检查**（例如 `statfs()`）
- 依赖文件系统的错误信号
- 失败后的清理确保不会浪费已用空间

#### 信号 3：Context 取消（外部中断）

**感知方式**：通过 `ctx.Done()` 通道

```go
// 在 LimitBulkIOWrite 中
select {
case <-time.After(delay):
    return nil
case <-ctx.Done():
    reservation.Cancel()
    return ctx.Err()  // context.Canceled 或 context.DeadlineExceeded
}
```

**反馈路径**：

```
Raft 状态变化（例如 leader 变更）
    ↓
取消快照接收的 Context
    ↓
Write() 返回 context.Canceled
    ↓
MultiSSTWriter 中止
    ↓
Scratch.Close()
    ↓
清理所有文件
```

**关键点**：
- Context 取消是**立即生效**的（不会等待当前写入完成）
- 速率限制会**归还已预留的 tokens**（避免资源泄漏）

---

### 4.2 这些信号如何影响决策

#### 决策 1：是否创建新文件

**决策点**：`ensureFile()` 中

```go
if f.created {
    return nil  // 复用已创建的文件
}

// 否则创建新文件
f.file, err = fs.CreateWithSync(...)
```

**影响因素**：
- 如果是首次写入 → 创建文件
- 如果文件已创建 → 直接写入
- 如果 Scratch 已关闭 → 返回错误

**即时 vs 滞后**：**即时**（在每次 `Write()` 时检查）

#### 决策 2：是否删除 Range 目录

**决策点**：`scratchClosed()` 中

```go
if val == 0 {
    delete(s.mu.rangeRefCount, rangeID)
    _ = s.env.RemoveAll(...)
}
```

**影响因素**：
- 引用计数为 0 → 删除目录
- 引用计数 > 0 → 保留目录

**即时 vs 滞后**：**即时**（在 `Scratch.Close()` 时立即决策）

**局部 vs 全局**：**全局**（通过 `rangeRefCount` 协调所有快照）

#### 决策 3：是否阻塞写入

**决策点**：`LimitBulkIOWrite()` 中

```go
delay := reservation.Delay()
if delay > 0 {
    time.Sleep(delay)  // 阻塞当前 goroutine
}
```

**影响因素**：
- 当前速率 < 限制 → 立即写入
- 当前速率 >= 限制 → 阻塞直到允许

**即时 vs 滞后**：**即时**（基于 token bucket 算法）

---

### 4.3 为什么采用当前策略

#### 策略 1：延迟创建（Lazy Initialization）

**原因**：
- 避免空目录和空文件的垃圾残留
- 快照接收可能在创建 `Scratch` 后立即失败（例如参数校验失败）
- 减少不必要的系统调用

**替代方案**：**预创建所有文件**

| 方案        | 优点                  | 缺点                    |
|-------------|----------------------|------------------------|
| 延迟创建    | 无垃圾文件             | 首次写入有创建开销       |
| 预创建      | 写入路径无分支         | 产生大量空文件           |

**为什么选择延迟创建**：
- 快照接收失败的概率不低（网络、磁盘、Raft 状态变化）
- 空文件的清理比延迟创建的开销更大

#### 策略 2：引用计数（Reference Counting）

**原因**：
- 支持同一 Range 的多个并发快照
- 自动清理 Range 目录（避免手动 GC）

**替代方案**：**每个快照独立清理，不共享 Range 目录**

| 方案        | 优点                  | 缺点                    |
|-------------|----------------------|------------------------|
| 引用计数    | 自动清理 Range 目录    | 需要同步（锁）           |
| 独立清理    | 无需同步               | Range 目录可能积累       |

**为什么选择引用计数**：
- Range 目录的存在是一个实现细节，不应该暴露给调用者
- 自动清理减少了维护负担

#### 策略 3：全局速率限制（Global Rate Limiting）

**原因**：
- 确保快照接收不会影响正常事务 IO
- 全局限制比单个快照的限制更容易调整

**替代方案**：**每个快照独立的速率限制**

| 方案        | 优点                  | 缺点                    |
|-------------|----------------------|------------------------|
| 全局限制    | 总 IO 可控             | 单个快照可能被饿死       |
| 独立限制    | 公平性更好             | 总 IO 可能超限           |

**为什么选择全局限制**：
- 快照接收是**后台任务**，不应该与前台事务竞争 IO
- 如果多个快照同时接收，全局限制确保总 IO 不会超标

---

### 4.4 设计如何在目标间取得平衡

#### 目标 1：稳定性（Stability）

**机制**：
- 速率限制：避免 IO 风暴
- 延迟创建：避免空文件积累
- 引用计数：确保目录的正确清理

**代价**：
- 速率限制可能延长快照接收时间
- 引用计数需要锁（但开销极小）

#### 目标 2：吞吐量（Throughput）

**机制**：
- `bytesPerSync` 参数：平滑写入压力
- 批量写入：`Write()` 可以接受大块数据

**代价**：
- `bytesPerSync` 会增加系统调用次数
- 速率限制会降低峰值吞吐量

#### 目标 3：公平性（Fairness）

**机制**：
- 全局速率限制：所有快照共享相同的 IO 配额

**代价**：
- 如果某个快照非常大，可能会"抢占"其他快照的配额
- 需要上层逻辑（Raft 层）来协调快照的并发数量

#### 目标 4：资源利用率（Resource Utilization）

**机制**：
- 延迟创建：避免浪费磁盘空间
- 自动清理：避免孤儿文件积累

**代价**：
- 延迟创建增加了首次写入的延迟（但通常可忽略）

**总体权衡**：

| 目标        | 权重 | 实现手段                        |
|-------------|------|--------------------------------|
| 稳定性      | 高   | 速率限制 + 引用计数             |
| 吞吐量      | 中   | 批量写入 + bytesPerSync         |
| 公平性      | 低   | 全局速率限制（依赖上层协调）     |
| 资源利用率  | 高   | 延迟创建 + 自动清理             |

---

## 五、设计模式分析

### 5.1 模式 1：Scratch Space Pattern（临时工作区模式）

#### 定义

为临时、可失败的操作提供一个**隔离的工作区**，操作完成后清理工作区。

#### 在代码中的体现

```go
// 创建工作区
scratch := sstSnapshotStorage.NewScratchSpace(rangeID, snapUUID, settings)

// 在工作区中工作
file := scratch.NewFile(ctx, bytesPerSync)
file.Write(data)
file.Finish()

// 完成后清理（无论成功还是失败）
defer scratch.Close()
```

#### 为什么选择这种模式

**问题**：快照接收是一个**多步骤、可失败**的过程
- 如果在主数据目录中直接操作，失败后需要手动清理
- 如果文件散落在不同位置，难以追踪和清理

**解决方案**：
- 为每个快照分配一个**独立的目录**（Scratch）
- 所有临时文件都在这个目录中
- 操作完成后，删除整个目录（一次性清理）

**类比**：
- 编译器的临时目录（`/tmp/gcc-xxx/`）
- 数据库的临时表空间
- 容器的 overlay 文件系统

#### 是否属于事实标准

是的，在分布式系统和数据库中非常常见：

- **Kafka**：每个消费者有独立的 `fetch` 缓冲区
- **Cassandra**：SSTable 压缩过程使用临时目录
- **Kubernetes**：Pod 的 `emptyDir` 卷

---

### 5.2 模式 2：Reference Counting Pattern（引用计数模式）

#### 定义

通过跟踪**活跃引用的数量**，在最后一个引用释放时自动清理资源。

#### 在代码中的体现

```go
// 创建 Scratch 时增加引用计数
func (s *SSTSnapshotStorage) NewScratchSpace(...) *SSTSnapshotStorageScratch {
    s.mu.Lock()
    s.mu.rangeRefCount[rangeID]++  // +1
    s.mu.Unlock()
    ...
}

// 关闭 Scratch 时减少引用计数
func (s *SSTSnapshotStorage) scratchClosed(rangeID roachpb.RangeID) {
    s.mu.Lock()
    defer s.mu.Unlock()

    val := s.mu.rangeRefCount[rangeID]
    val--
    s.mu.rangeRefCount[rangeID] = val

    if val == 0 {
        // 最后一个引用释放，清理资源
        delete(s.mu.rangeRefCount, rangeID)
        _ = s.env.RemoveAll(...)
    }
}
```

#### 为什么选择这种模式

**问题**：同一个 Range 可能有多个并发快照
- 如果第一个快照完成后立即删除 Range 目录，第二个快照会失败
- 需要知道"何时可以安全删除 Range 目录"

**解决方案**：
- 跟踪每个 Range 的活跃快照数量
- 只有当所有快照都完成时，才删除 Range 目录

**类比**：
- C++ 的 `std::shared_ptr`
- Python 的垃圾回收
- Kubernetes 的 `OwnerReference`

#### 是否属于事实标准

是的，在系统编程和资源管理中是标准模式：

- **Linux 内核**：`struct kref`（内核对象引用计数）
- **Pebble**：SSTable 的引用计数（确保正在读取的 SST 不会被删除）
- **CockroachDB**：Replica 的 lease 引用计数

---

### 5.3 模式 3：Lazy Initialization Pattern（延迟初始化模式）

#### 定义

推迟对象的创建，直到**真正需要**时才创建。

#### 在代码中的体现

```go
// NewFile 只是记录文件路径，不创建实际文件
func (s *SSTSnapshotStorageScratch) NewFile(...) (*SSTSnapshotStorageFile, error) {
    filename := s.filename(id)
    s.ssts = append(s.ssts, filename)  // 记录路径

    return &SSTSnapshotStorageFile{
        filename: filename,
        created:  false,  // 标记为未创建
        ...
    }, nil
}

// Write 时才创建文件
func (f *SSTSnapshotStorageFile) Write(contents []byte) error {
    if err := f.ensureFile(); err != nil {  // 确保文件已创建
        return err
    }
    ...
}
```

#### 为什么选择这种模式

**问题**：快照接收可能在任何阶段失败
- 如果预创建所有文件，失败后会留下空文件
- 空文件的清理需要额外的逻辑

**解决方案**：
- 只有在**真正需要写入**时才创建文件
- 如果快照接收在写入前失败，不会留下任何文件

**性能考虑**：
- 创建文件的开销通常可忽略（相对于写入数据）
- 测试用例验证了延迟创建的正确性

```go
// sst_snapshot_storage_test.go:62-66
_, err := eng.Env().Stat(scratch.snapDir)
if !oserror.IsNotExist(err) {
    t.Fatalf("expected %s to not exist", scratch.snapDir)
}
```

#### 是否属于事实标准

是的，在系统编程和数据库中非常常见：

- **Java**：懒加载的单例（Singleton）
- **数据库**：懒加载的索引（只有在查询时才构建）
- **操作系统**：延迟分配的内存页（Copy-on-Write）

---

### 5.4 模式 4：Rate Limiting Pattern（速率限制模式）

#### 定义

通过**预留资源配额**（tokens），控制操作的速率。

#### 在代码中的体现

```go
// Write 时预留 tokens
func (f *SSTSnapshotStorageFile) Write(contents []byte) error {
    ...
    if err := kvserverbase.LimitBulkIOWrite(
        f.ctx, f.scratch.storage.limiter, len(contents)
    ); err != nil {
        return err
    }
    ...
}

// 简化版的 LimitBulkIOWrite
func LimitBulkIOWrite(ctx context.Context, limiter *rate.Limiter, bytes int) error {
    reservation := limiter.ReserveN(time.Now(), bytes)
    if !reservation.OK() {
        return errors.Errorf("rate limit exceeded")
    }

    delay := reservation.Delay()
    if delay > 0 {
        select {
        case <-time.After(delay):
            return nil
        case <-ctx.Done():
            reservation.Cancel()  // 归还 tokens
            return ctx.Err()
        }
    }

    return nil
}
```

#### 为什么选择这种模式

**问题**：快照写入是大量连续的 IO 操作
- 如果不加控制，会影响正常事务的 IO
- 需要在**快照接收速度**和**系统稳定性**之间取得平衡

**解决方案**：
- 使用 **Token Bucket 算法** 进行速率限制
- 每次写入前预留 tokens，写入后消耗 tokens
- 如果 tokens 不足，阻塞写入直到 tokens 恢复

**为什么是 Token Bucket 而不是 Leaky Bucket？**

| 算法          | 特点                        | 适用场景                |
|---------------|----------------------------|------------------------|
| Token Bucket  | 允许突发流量（burst）       | 后台任务（例如快照）     |
| Leaky Bucket  | 平滑流量（fixed rate）      | 前台任务（例如请求处理） |

Token Bucket 更适合快照接收，因为：
- 快照数据可能以**突发方式**到达（网络缓冲区满后一次性写入）
- Token Bucket 允许一定的突发，但长期平均速率受限

#### 是否属于事实标准

是的，在网络和分布式系统中是标准模式：

- **API 网关**：限制每秒请求数（QPS）
- **云服务**：限制 IOPS 和带宽
- **Kubernetes**：Pod 的 CPU/内存配额

---

### 5.5 模式 5：Resource Acquisition Is Initialization（RAII）

#### 定义

将资源的生命周期与对象的生命周期绑定：
- 对象创建时获取资源
- 对象销毁时释放资源

#### 在代码中的体现（变种）

虽然 Go 没有 C++ 的析构函数，但 CockroachDB 通过**显式的 `Close()` 模式**模拟了 RAII：

```go
// 创建资源
scratch := sstSnapshotStorage.NewScratchSpace(...)
defer scratch.Close()  // 确保资源被释放

// 使用资源
file := scratch.NewFile(...)
defer file.Finish()  // 或 file.Abort()
```

测试用例验证了清理的正确性：

```go
// sst_snapshot_storage_test.go:108-117
require.NoError(t, scratch.Close())
_, err = eng.Env().Stat(scratch.snapDir)
if !oserror.IsNotExist(err) {
    t.Fatalf("expected %s to not exist", scratch.snapDir)
}
```

#### 为什么选择这种模式（变种）

**问题**：Go 没有析构函数，资源可能泄漏
- 如果忘记调用 `Close()`，文件句柄和磁盘空间会泄漏
- 需要一种**约定**来确保资源被释放

**解决方案**：
- 所有资源对象都实现 `Close()` 方法
- 调用者使用 `defer` 确保 `Close()` 被调用
- 测试用例验证清理逻辑

**为什么不使用 `finalizer`（Go 的析构函数）？**

```go
// ❌ 不推荐
runtime.SetFinalizer(scratch, func(s *SSTSnapshotStorageScratch) {
    s.Close()
})
```

**问题**：
- Finalizer 的执行时机**不确定**（依赖 GC）
- 文件句柄等资源可能很长时间不被释放
- Finalizer 的错误无法被上层逻辑处理

#### 是否属于事实标准

是的，在 Go 生态中是标准模式：

- **`io.Closer`**：标准库的关闭接口
- **`database/sql`**：数据库连接的 `Close()`
- **`os.File`**：文件的 `Close()`

---

### 5.6 模式总结

| 模式                | 解决的问题                | 是否标准 | 相关代码                          |
|---------------------|--------------------------|---------|----------------------------------|
| Scratch Space       | 临时文件的隔离与清理       | ✅      | `NewScratchSpace`                |
| Reference Counting  | 共享资源的生命周期管理     | ✅      | `rangeRefCount`                  |
| Lazy Initialization | 避免不必要的资源创建       | ✅      | `ensureFile`, `createDir`        |
| Rate Limiting       | 控制 IO 速率              | ✅      | `LimitBulkIOWrite`               |
| RAII (变种)         | 确保资源被释放            | ✅      | `Close()` + `defer`              |

**核心理念**：
- **防御性编程**：通过模式减少人为错误
- **自动化**：尽可能减少手动清理
- **可测试性**：所有模式都有对应的测试用例

---

## 六、具体运行示例

### 6.1 正常场景：成功接收快照

**初始状态**：
- Store 已启动，`SSTSnapshotStorage` 已创建
- `rangeRefCount` 为空
- 磁盘上没有 `/sstsnapshot/` 目录

**步骤 1：创建 Scratch（T0）**

```go
scratch := sstSnapshotStorage.NewScratchSpace(
    rangeID=123,
    snapUUID=550e8400-e29b-41d4-a716-446655440000,
    settings=...
)
```

**状态变化**：
```
rangeRefCount = {123: 1}
snapDir = "/data/auxiliary/sstsnapshot/123/550e8400-e29b-41d4-a716-446655440000"
dirCreated = false
```

**磁盘状态**：无变化

---

**步骤 2：创建第一个 SST 文件（T1）**

```go
file0 := scratch.NewFile(ctx, 512*1024)
```

**状态变化**：
```
scratch.ssts = [
    "/data/auxiliary/sstsnapshot/123/550e8400.../0.sst"
]
file0.created = false
```

**磁盘状态**：无变化

---

**步骤 3：首次写入（T2）**

```go
data := make([]byte, 1024*1024)  // 1 MB
file0.Write(data)
```

**执行流程**：
1. `ensureFile()` → `createDir()` → 创建目录
2. `CreateWithSync()` → 创建文件 `0.sst`
3. `LimitBulkIOWrite()` → 预留 1 MB tokens
4. `file.Write()` → 写入数据

**状态变化**：
```
scratch.dirCreated = true
file0.created = true
limiter.tokens -= 1*1024*1024
```

**磁盘状态**：
```
/data/auxiliary/sstsnapshot/
    └── 123/
        └── 550e8400.../
            └── 0.sst (1 MB)
```

---

**步骤 4：继续写入（T3-T5）**

```go
file0.Write(make([]byte, 2*1024*1024))  // 2 MB
file0.Write(make([]byte, 1*1024*1024))  // 1 MB
```

**状态变化**：
```
limiter.tokens -= 3*1024*1024
```

**磁盘状态**：
```
/data/auxiliary/sstsnapshot/
    └── 123/
        └── 550e8400.../
            └── 0.sst (4 MB)
```

---

**步骤 5：完成第一个 SST（T6）**

```go
file0.Finish()
```

**执行流程**：
1. `file.Sync()` → 数据落盘
2. `file.Close()` → 关闭文件句柄

**状态变化**：
```
file0.file = nil
```

**磁盘状态**：`0.sst` 持久化完成

---

**步骤 6：创建第二个 SST 文件（T7-T9）**

```go
file1 := scratch.NewFile(ctx, 512*1024)
file1.Write(make([]byte, 5*1024*1024))  // 5 MB
file1.Finish()
```

**状态变化**：
```
scratch.ssts = [
    "/data/auxiliary/sstsnapshot/123/550e8400.../0.sst",
    "/data/auxiliary/sstsnapshot/123/550e8400.../1.sst"
]
limiter.tokens -= 5*1024*1024
```

**磁盘状态**：
```
/data/auxiliary/sstsnapshot/
    └── 123/
        └── 550e8400.../
            ├── 0.sst (4 MB)
            └── 1.sst (5 MB)
```

---

**步骤 7：快照接收完成（T10）**

```go
scratch.Close()
```

**执行流程**：
1. `RemoveAll(snapDir)` → 删除快照目录
2. `scratchClosed(123)` → 减少引用计数
3. 引用计数为 0 → 删除 Range 目录

**状态变化**：
```
rangeRefCount = {}  // 空
```

**磁盘状态**：
```
/data/auxiliary/sstsnapshot/  (空目录或不存在)
```

---

### 6.2 边界场景 1：速率限制触发

**场景**：limiter 配置为 10 MB/s，快照尝试以 20 MB/s 写入

**初始状态**：
```
limiter = rate.NewLimiter(10*1024*1024, 2*1024*1024)  // 10 MB/s, 2 MB burst
limiter.tokens = 2*1024*1024  // 初始 burst
```

---

**时间 T0：写入 2 MB（burst 范围内）**

```go
file.Write(make([]byte, 2*1024*1024))
```

**执行流程**：
```
LimitBulkIOWrite(ctx, limiter, 2*1024*1024)
    → reservation = limiter.ReserveN(now, 2*1024*1024)
    → reservation.Delay() = 0  (tokens 足够)
    → 立即返回
```

**状态变化**：
```
limiter.tokens = 0  (消耗完 burst)
```

**写入时间**：立即

---

**时间 T0 + 10ms：写入 2 MB（超过速率）**

```go
file.Write(make([]byte, 2*1024*1024))
```

**执行流程**：
```
LimitBulkIOWrite(ctx, limiter, 2*1024*1024)
    → reservation = limiter.ReserveN(now, 2*1024*1024)
    → reservation.Delay() = 190ms  (需要等待 tokens 恢复)
    → time.Sleep(190ms)
    → 写入
```

**计算**：
```
需要的 tokens = 2 MB = 2*1024*1024 bytes
当前速率 = 10 MB/s = 10*1024*1024 bytes/s
恢复 2 MB 需要时间 = 2*1024*1024 / (10*1024*1024) = 0.2s = 200ms
减去已经过的 10ms = 190ms
```

**状态变化**：
```
阻塞 190ms
```

**实际写入时间**：T0 + 10ms + 190ms = T0 + 200ms

---

**总结**：
- 第一次写入利用了 burst（2 MB 立即写入）
- 第二次写入触发了速率限制（等待 190ms）
- 长期平均速率被限制在 10 MB/s

---

### 6.3 边界场景 2：并发快照接收

**场景**：Range 123 同时接收两个快照

**初始状态**：
```
rangeRefCount = {}
```

---

**时间 T0：第一个快照开始**

```go
// Thread 1
scratch1 := sstSnapshotStorage.NewScratchSpace(
    rangeID=123,
    snapUUID=uuid1,
    ...
)
```

**状态变化**：
```
rangeRefCount = {123: 1}
```

---

**时间 T1：第二个快照开始（并发）**

```go
// Thread 2
scratch2 := sstSnapshotStorage.NewScratchSpace(
    rangeID=123,
    snapUUID=uuid2,
    ...
)
```

**状态变化**：
```
rangeRefCount = {123: 2}
```

**磁盘结构**（预期）：
```
/data/auxiliary/sstsnapshot/
    └── 123/
        ├── {uuid1}/
        │   └── 0.sst
        └── {uuid2}/
            └── 0.sst
```

---

**时间 T5：第一个快照完成**

```go
// Thread 1
scratch1.Close()
```

**执行流程**：
```
RemoveAll("/data/auxiliary/sstsnapshot/123/{uuid1}")  // 删除快照目录
scratchClosed(123)
    → rangeRefCount[123] = 2 - 1 = 1
    → 1 != 0, 不删除 Range 目录
```

**状态变化**：
```
rangeRefCount = {123: 1}
```

**磁盘状态**：
```
/data/auxiliary/sstsnapshot/
    └── 123/
        └── {uuid2}/
            └── 0.sst
```

---

**时间 T10：第二个快照完成**

```go
// Thread 2
scratch2.Close()
```

**执行流程**：
```
RemoveAll("/data/auxiliary/sstsnapshot/123/{uuid2}")  // 删除快照目录
scratchClosed(123)
    → rangeRefCount[123] = 1 - 1 = 0
    → 0 == 0, 删除 Range 目录
    → RemoveAll("/data/auxiliary/sstsnapshot/123")
```

**状态变化**：
```
rangeRefCount = {}
```

**磁盘状态**：
```
/data/auxiliary/sstsnapshot/  (空)
```

---

**关键保证**：
- 两个快照的文件隔离（通过 UUID）
- Range 目录在最后一个快照完成后才删除
- 引用计数的原子性（通过 Mutex）

---

### 6.4 异常场景：磁盘空间不足

**场景**：磁盘空间即将用尽

**初始状态**：
```
磁盘剩余空间 = 5 MB
```

---

**时间 T0：创建 Scratch 并写入 4 MB**

```go
scratch := sstSnapshotStorage.NewScratchSpace(...)
file := scratch.NewFile(ctx, 0)
file.Write(make([]byte, 4*1024*1024))  // 成功
```

**状态变化**：
```
磁盘剩余空间 = 1 MB
```

---

**时间 T1：尝试写入 2 MB（失败）**

```go
err := file.Write(make([]byte, 2*1024*1024))
// err = "write /data/.../0.sst: no space left on device"
```

**执行流程**：
```
LimitBulkIOWrite()  → 成功（tokens 足够）
file.Write()  → 失败（磁盘空间不足）
```

**返回错误**：`syscall.ENOSPC`

---

**时间 T2：上层逻辑清理**

```go
// MultiSSTWriter 捕获错误
if err != nil {
    file.Abort()  // 关闭文件句柄
    scratch.Close()  // 清理所有文件
    return err
}
```

**磁盘状态**：
```
/data/auxiliary/sstsnapshot/123/{uuid}/0.sst  (部分写入，4 MB)
    ↓ (Close 后)
/data/auxiliary/sstsnapshot/  (空)
```

**恢复的磁盘空间**：4 MB

---

**关键点**：
- 错误在写入时立即暴露（而不是在 `Finish()` 时）
- `Close()` 确保部分写入的文件被删除
- 释放的磁盘空间可以被其他操作使用

---

## 七、设计取舍与替代方案（Trade-offs）

### 7.1 当前方案 vs 直接写入主数据目录

#### 当前方案：独立的 Scratch 目录

**优点**：
- 清理简单：删除整个 `snapDir` 即可
- 隔离性好：不会与主数据混在一起
- 可恢复性强：重启时可以安全删除整个 `sstsnapshot` 目录

**缺点**：
- 需要额外的目录结构管理
- 需要引用计数逻辑

#### 替代方案：直接写入 Pebble 的 ingest 目录

```go
// ❌ 假设的替代方案
file := engine.CreateIngestableSST(rangeID, snapUUID)
```

**优点**：
- 无需额外的目录管理
- Pebble 自带 SST 清理逻辑

**缺点**：
- 如果快照接收失败，需要手动通知 Pebble 删除 SST
- Pebble 的 ingest 目录与实际数据混在一起，清理复杂
- 无法区分"正在接收的快照"和"已完成的快照"

**为什么选择当前方案**：
- **隔离性优先**：快照接收是临时操作，应该与持久化数据分离
- **清理简单优先**：删除整个目录比逐个文件删除更可靠

---

### 7.2 当前方案 vs 内存缓冲 + 批量写入

#### 当前方案：流式写入（边接收边写入）

**优点**：
- 内存占用低：只缓冲少量数据（取决于 `bytesPerSync`）
- 支持大快照：快照大小不受内存限制

**缺点**：
- 磁盘 IO 频繁：每次 `Write()` 都可能触发系统调用

#### 替代方案：先缓存在内存，再批量写入

```go
// ❌ 假设的替代方案
buffer := make([]byte, 0, 100*1024*1024)  // 100 MB 缓冲区
buffer = append(buffer, data...)
// 缓冲区满后再写入磁盘
```

**优点**：
- 减少系统调用次数
- 可能提高吞吐量（顺序写入）

**缺点**：
- 内存占用高：每个快照需要 100 MB+ 内存
- 多快照并发时内存压力巨大
- 快照失败时内存浪费

**为什么选择当前方案**：
- **内存优先**：CockroachDB 的内存预算非常宝贵
- **可扩展性优先**：流式写入支持任意大小的快照

---

### 7.3 当前方案 vs 预创建文件

#### 当前方案：延迟创建（Lazy Initialization）

**优点**：
- 无垃圾文件：如果快照接收失败，不会留下空文件
- 资源利用率高：避免不必要的 inode 分配

**缺点**：
- 首次写入有额外开销（创建目录和文件）
- 代码复杂度略高（需要 `ensureFile()` 逻辑）

#### 替代方案：在 `NewFile()` 时立即创建文件

```go
// ❌ 假设的替代方案
func (s *SSTSnapshotStorageScratch) NewFile(...) (*SSTSnapshotStorageFile, error) {
    // 立即创建文件
    f, err := s.storage.env.Create(filename, ...)
    if err != nil {
        return nil, err
    }
    return &SSTSnapshotStorageFile{file: f, ...}, nil
}
```

**优点**：
- 代码简单：无需 `ensureFile()` 逻辑
- 首次写入无额外开销

**缺点**：
- 产生垃圾文件：如果 `NewFile()` 后从未调用 `Write()`
- 资源浪费：inode、目录项、元数据

**为什么选择当前方案**：
- **垃圾清理优先**：避免空文件的积累
- **复杂度可控**：`ensureFile()` 的逻辑很简单

**测试用例验证**：

```go
// sst_snapshot_storage_test.go:71-80
// 即使调用了 NewFile，文件也不会立即创建
f, err := scratch.NewFile(ctx, 0)
require.NoError(t, err)
for _, fileName := range scratch.SSTs() {
    _, err := eng.Env().Stat(fileName)
    if !oserror.IsNotExist(err) {
        t.Fatalf("expected %s to not exist", fileName)
    }
}
```

---

### 7.4 当前方案 vs 单个 Range 单个目录

#### 当前方案：Range 目录 + 快照子目录

```
/sstsnapshot/
    └── {rangeID}/
        ├── {snapUUID1}/
        └── {snapUUID2}/
```

**优点**：
- 支持并发快照：同一 Range 可以同时接收多个快照
- 隔离性强：不同快照的文件不会冲突

**缺点**：
- 目录层级深（两层）
- 需要引用计数管理 Range 目录

#### 替代方案：扁平结构

```
/sstsnapshot/
    ├── {rangeID}_{snapUUID1}/
    └── {rangeID}_{snapUUID2}/
```

**优点**：
- 目录结构简单（一层）
- 无需引用计数

**缺点**：
- 无法批量删除某个 Range 的所有快照
- 目录数量可能很大（如果 Range 数量很多）

**为什么选择当前方案**：
- **并发支持优先**：两层目录结构更适合并发场景
- **引用计数的复杂度可接受**：逻辑简单且有测试覆盖

---

### 7.5 当前方案 vs 无速率限制

#### 当前方案：全局速率限制

**优点**：
- 保护系统稳定性：避免 IO 风暴
- 公平性：所有快照共享相同的 IO 配额

**缺点**：
- 可能延长快照接收时间
- 如果配置不当，可能导致快照接收过慢

#### 替代方案：无速率限制（尽力而为）

**优点**：
- 最大化快照接收速度
- 代码简单

**缺点**：
- 可能影响正常事务 IO
- 磁盘 IO 突发可能导致延迟抖动

**为什么选择当前方案**：
- **稳定性优先**：快照接收是后台任务，不应该影响前台
- **可配置性**：速率限制可以根据硬件调整

---

### 7.6 总结：当前方案的权衡

| 维度          | 当前方案得分 | 说明                                      |
|---------------|-------------|-------------------------------------------|
| 内存占用      | ⭐⭐⭐⭐⭐  | 流式写入，内存占用低                       |
| 磁盘占用      | ⭐⭐⭐⭐⭐  | 延迟创建，无垃圾文件                       |
| 吞吐量        | ⭐⭐⭐      | 速率限制降低了峰值吞吐量                   |
| 并发支持      | ⭐⭐⭐⭐⭐  | 引用计数支持同一 Range 的并发快照          |
| 代码复杂度    | ⭐⭐⭐⭐    | 引用计数和延迟创建增加了少量复杂度         |
| 可测试性      | ⭐⭐⭐⭐⭐  | 有完善的单元测试覆盖                       |
| 稳定性        | ⭐⭐⭐⭐⭐  | 速率限制和自动清理确保系统稳定             |

**核心权衡**：
- **优先保证稳定性和资源利用率**，而不是峰值吞吐量
- **优先支持并发和大快照**，而不是简化代码
- **优先自动化清理**，而不是手动管理

---

## 八、总结与心智模型（Mental Model）

### 8.1 核心思想总结

`SSTSnapshotStorage` 是一个**分层的、延迟创建的、自动清理的**临时文件管理器，专门为 Raft 快照接收设计。它通过以下机制解决快照接收的核心挑战：

1. **Scratch Space 模式**：为每个快照分配独立的工作区，操作完成后一次性清理
2. **引用计数**：支持同一 Range 的并发快照，自动管理 Range 目录的生命周期
3. **延迟创建**：只有在真正需要写入时才创建文件，避免空文件残留
4. **速率限制**：通过 Token Bucket 算法控制 IO 速率，保护系统稳定性
5. **RAII 变种**：通过 `Close()` + `defer` 确保资源被释放

### 8.2 可复用的心智模型

**你可以把 `SSTSnapshotStorage` 理解为：**

> **一个具有自动清理功能的临时文件系统（类似 Docker 的 overlay 文件系统）**
>
> - 每个快照对应一个**隔离的层**（Scratch）
> - 层内的文件只有在**真正需要时才创建**（延迟初始化）
> - 多个快照可以**共享同一个 Range 目录**（引用计数）
> - 所有写入操作都经过**全局的流量控制阀门**（速率限制）
> - 操作完成后，层会被**自动删除**（无论成功还是失败）

**类比**：

| SSTSnapshotStorage   | 类比                            |
|----------------------|--------------------------------|
| Scratch              | Docker 的 layer（层）           |
| Range 目录           | 共享的基础镜像                   |
| 引用计数             | Docker 的镜像引用计数            |
| 延迟创建             | Copy-on-Write                  |
| 速率限制             | 网络流量整形器                   |
| Close()              | 容器的 cleanup hook             |

### 8.3 设计哲学

1. **防御性编程**：
   - 假设操作会失败（延迟创建、自动清理）
   - 假设会有并发（引用计数、Mutex）
   - 假设会有资源竞争（速率限制）

2. **简单性优于性能**：
   - 全局速率限制比每个快照独立限制更简单
   - 延迟创建比预创建更简单（虽然首次写入有开销）

3. **自动化优于手动**：
   - 引用计数自动删除 Range 目录
   - `Close()` 自动清理所有文件

4. **隔离性优于共享**：
   - 每个快照有独立的目录
   - 快照文件与主数据分离

### 8.4 使用建议

**对于使用者（例如 `MultiSSTWriter`）：**

```go
// ✅ 正确用法
scratch := sstSnapshotStorage.NewScratchSpace(rangeID, snapUUID, settings)
defer scratch.Close()  // 确保资源被释放

file := scratch.NewFile(ctx, 512*1024)
defer func() {
    if err != nil {
        file.Abort()  // 失败时中止
    } else {
        file.Finish()  // 成功时完成
    }
}()

// 写入数据
for _, data := range snapshotData {
    if err := file.Write(data); err != nil {
        return err  // defer 会自动清理
    }
}
```

**常见错误**：

```go
// ❌ 错误 1：忘记 defer Close()
scratch := sstSnapshotStorage.NewScratchSpace(...)
// 如果后续代码 panic，Scratch 不会被清理

// ❌ 错误 2：忘记 defer Finish() / Abort()
file := scratch.NewFile(...)
file.Write(data)
// 如果后续代码返回错误，文件句柄不会被关闭

// ❌ 错误 3：在 Close() 后继续使用
scratch.Close()
file := scratch.NewFile(...)  // 会返回错误
```

### 8.5 扩展性考虑

**如果未来需要支持以下场景，当前设计的适应性：**

| 场景                        | 适应性 | 说明                                      |
|-----------------------------|--------|-------------------------------------------|
| 更大的快照（10 GB+）         | ⭐⭐⭐⭐⭐ | 流式写入无内存限制                         |
| 更多并发快照（100+ 并发）    | ⭐⭐⭐⭐   | 引用计数支持，但锁可能成为瓶颈             |
| 不同优先级的快照             | ⭐⭐⭐    | 需要修改速率限制为分优先级                 |
| 跨节点的快照共享             | ⭐⭐     | 当前设计是本地的，需要大幅改造             |
| 快照压缩                     | ⭐⭐⭐⭐⭐ | 可以在 `Write()` 前或后透明地压缩          |

### 8.6 最终心智模型（伪代码）

```go
// 高度抽象的伪代码
type SSTSnapshotStorage = {
    root_dir: "/data/auxiliary/sstsnapshot",
    rate_limiter: GlobalIOLimiter,
    active_ranges: RefCountedMap<RangeID, int>,

    CreateScratch(rangeID, snapUUID) -> Scratch {
        active_ranges[rangeID]++
        return Scratch {
            dir: root_dir / rangeID / snapUUID,
            lazy_created: false,
        }
    },

    CloseScratch(scratch) {
        DeleteDir(scratch.dir)
        if --active_ranges[scratch.rangeID] == 0 {
            DeleteDir(root_dir / scratch.rangeID)
        }
    },
}

type Scratch = {
    dir: Path,
    lazy_created: bool,
    files: List<File>,

    NewFile() -> File {
        files.append(File {
            path: dir / files.len() + ".sst",
            lazy_created: false,
        })
    },

    Close() {
        DeleteDir(dir)
        storage.CloseScratch(this)
    },
}

type File = {
    path: Path,
    lazy_created: bool,
    handle: FileHandle?,

    Write(data) {
        if !lazy_created {
            CreateDir(path.parent)
            handle = CreateFile(path)
            lazy_created = true
        }

        WaitForRateLimit(rate_limiter, len(data))
        handle.write(data)
    },

    Finish() {
        handle.sync()
        handle.close()
    },
}
```

---

**结束语**：

`SSTSnapshotStorage` 是一个**教科书式的资源管理模块**，它通过经典的设计模式（Scratch Space、引用计数、延迟创建、速率限制）解决了快照接收的所有核心挑战。理解这个模块的关键是理解**为什么需要这些模式**，而不仅仅是**这些模式如何实现**。

这种"先理解问题，再理解解决方案"的思维方式，是系统编程和分布式系统设计的核心能力。
