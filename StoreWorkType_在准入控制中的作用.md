# StoreWorkType 在 CockroachDB 准入控制中的作用

## 概述

`StoreWorkType` 是 CockroachDB 准入控制系统中的一个关键枚举类型，用于对存储层的工作进行分类和优先级管理。它在 `pkg/util/admission/admissionpb` 包中定义，是 CockroachDB 资源管理和性能隔离机制的核心组件之一。

## StoreWorkType 的定义

`StoreWorkType` 是一个 `int8` 类型的枚举，定义了三种不同的存储工作类型：

```go
type StoreWorkType int8

const (
    // RegularStoreWorkType 是对应 RegularWorkClass 的存储特定工作类型
    RegularStoreWorkType StoreWorkType = iota
    // SnapshotIngestStoreWorkType 是快照工作类型，分类为 ElasticWorkClass
    // 但优先级高于该类的其他工作
    SnapshotIngestStoreWorkType = 1
    // ElasticStoreWorkType 是对应 ElasticWorkClass 的存储特定工作
    // 不包括 SnapshotIngestStoreWorkType
    ElasticStoreWorkType = 2
    // NumStoreWorkTypes 是存储工作类型的数量
    NumStoreWorkTypes = 3
)
```

## StoreWorkType 与 WorkClass 的关系

`StoreWorkType` 与 `WorkClass` 之间存在映射关系，通过 `WorkClassFromStoreWorkType` 函数实现：

```go
func WorkClassFromStoreWorkType(workType StoreWorkType) WorkClass {
    var class WorkClass
    switch workType {
    case RegularStoreWorkType:
        class = RegularWorkClass
    case ElasticStoreWorkType:
        class = ElasticWorkClass
    case SnapshotIngestStoreWorkType:
        class = ElasticWorkClass
    }
    return class
}
```

这意味着：
- `RegularStoreWorkType` → `RegularWorkClass`（通吐量和延迟敏感的工作）
- `ElasticStoreWorkType` → `ElasticWorkClass`（可以处理减少吐吐量的工作）
- `SnapshotIngestStoreWorkType` → `ElasticWorkClass`（但具有更高优先级）

## 准入控制中的作用

### 1. 资源分配和优先级管理

在准入控制系统中，`StoreWorkType` 用于区分不同类型的存储工作，并为它们分配不同的资源和优先级。这主要通过 `kvStoreTokenGranter` 实现，它根据工作类型决定是否授予 IO 令牌。

### 2. IO 令牌分配机制

在 `granter.go` 中，`tryGrantLocked` 方法根据 `StoreWorkType` 实现不同的令牌分配逻辑：

```go
switch wt {
case admissionpb.RegularStoreWorkType:
    // 常规工作只需检查 IO 令牌
    if sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0 {
        // 授予令牌
        return true
    }
case admissionpb.ElasticStoreWorkType:
    // 弹性工作需要检查三种令牌
    if sg.mu.diskTokensAvailable.writeByteTokens > 0 &&
        sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0 &&
        sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] > 0 {
        // 授予令牌
        return true
    }
case admissionpb.SnapshotIngestStoreWorkType:
    // 快照摄入只检查磁盘写令牌（不进 L0）
    if sg.mu.diskTokensAvailable.writeByteTokens > 0 {
        // 授予令牌
        return true
    }
}
```

### 3. 磁盘带宽管理

在 `disk_bandwidth.go` 中，`StoreWorkType` 用于跟踪和管理不同工作类型的磁盘带宽使用情况：

```go
// 用于跟踪每种工作类型的磁盘令牌使用情况
usedTokens [admissionpb.NumStoreWorkTypes]diskTokens

// 在日志输出中显示每种工作类型的令牌使用情况
ib(d.state.usedTokens[admissionpb.ElasticStoreWorkType].writeByteTokens),
ib(d.state.usedTokens[admissionpb.SnapshotIngestStoreWorkType].writeByteTokens),
ib(d.state.usedTokens[admissionpb.RegularStoreWorkType].writeByteTokens),
```

## 与 WorkQueue 的关系

### 1. 存储工作队列

`StoreWorkType` 与 `WorkQueue` 通过 `StoreWorkQueue` 连接起来。`StoreWorkQueue` 是专门处理存储工作的队列，它根据 `StoreWorkType` 对工作进行分类和调度。

### 2. 请求者和授予者模型

在准入控制系统中，采用了请求者（requester）和授予者（granter）模型：

- **请求者**：`StoreWorkQueue` 作为请求者，负责管理特定 `StoreWorkType` 的工作请求
- **授予者**：`kvStoreTokenGranter` 作为授予者，根据 `StoreWorkType` 决定是否授予 IO 令牌

### 3. 令牌分配流程

1. 工作请求到达 `StoreWorkQueue`
2. 根据 `StoreWorkType` 确定工作类型
3. `StoreWorkQueue` 向 `kvStoreTokenGranter` 请求令牌
4. `kvStoreTokenGranter` 根据 `StoreWorkType` 检查可用令牌
5. 如果令牌可用，授予令牌并允许工作执行
6. 工作完成后，令牌被返回到池中

## 具体工作类型分析

### 1. RegularStoreWorkType

- **特征**：对应 RegularWorkClass，通吐量和延迟敏感
- **令牌要求**：只需要 RegularWorkClass 的 IO 令牌
- **优先级**：最高优先级
- **用例**：用户查询、事务处理等关键操作

### 2. ElasticStoreWorkType

- **特征**：对应 ElasticWorkClass，可以处理减少吐吐量
- **令牌要求**：需要三种令牌（磁盘写令牌、Regular IO 令牌、Elastic IO 令牌）
- **优先级**：中等优先级
- **用例**：批量操作、后台任务、索引构建等

### 3. SnapshotIngestStoreWorkType

- **特征**：对应 ElasticWorkClass 但具有更高优先级
- **令牌要求**：只需要磁盘写令牌（不进 L0）
- **优先级**：高于其他 Elastic 工作
- **用例**：快照摄入操作

## 实现细节

### 1. 令牌扣除机制

在 `subtractTokensForStoreWorkTypeLocked` 方法中，根据 `StoreWorkType` 执行不同的令牌扣除逻辑：

```go
func (sg *kvStoreTokenGranter) subtractTokensForStoreWorkTypeLocked(
    wt admissionpb.StoreWorkType, count int64,
) {
    if wt != admissionpb.SnapshotIngestStoreWorkType {
        // 对于非快照摄入工作，调整 IO 令牌
        sg.subtractIOTokensLocked(count, count, false)
    }
    if wt == admissionpb.ElasticStoreWorkType {
        sg.mu.elasticIOTokensUsedByElastic += count
    }
    // 根据工作类型调整磁盘令牌
    switch wt {
    case admissionpb.RegularStoreWorkType, admissionpb.ElasticStoreWorkType:
        diskTokenCount := sg.mu.writeAmpLM.applyLinearModel(count)
        sg.mu.diskTokensAvailable.writeByteTokens -= diskTokenCount
        // ...
    case admissionpb.SnapshotIngestStoreWorkType:
        // 不应用 writeAmpLM，因为这些写入不会导致额外的写入放大
        sg.mu.diskTokensAvailable.writeByteTokens -= count
        // ...
    }
}
```

### 2. 错误调整机制

系统还实现了错误调整机制，用于处理实际磁盘 IO 与预估令牌之间的差异：

```go
func (sg *kvStoreTokenGranter) adjustDiskTokenError(m StoreMetrics) {
    sg.mu.Lock()
    defer sg.mu.Unlock()
    sg.adjustDiskTokenErrorLocked(m.DiskStats.BytesRead, m.DiskStats.BytesWritten)
}
```

## 性能影响

### 1. 资源隔离

通过 `StoreWorkType` 的分类，CockroachDB 可以实现：
- **Regular 工作**优先执行，确保关键操作的低延迟
- **Elastic 工作**在资源充足时执行，避免影响关键操作
- **快照摄入**获得特殊处理，避免 L0 压缩压力

### 2. 动态调整

系统根据实际磁盘 IO 负载动态调整令牌分配，确保：
- 资源利用率最大化
- 公平分配资源给不同优先级的工作
- 防止单一工作类型垄断资源

## 结论

`StoreWorkType` 是 CockroachDB 准入控制系统的核心组件，它通过对存储工作进行精细分类，实现了：

1. **资源隔离**：不同优先级的工作获得不同的资源分配
2. **性能保证**：关键操作获得优先执行权
3. **公平调度**：所有工作类型都能获得合理的资源分配
4. **动态适应**：根据实际负载调整资源分配策略

通过与 `WorkQueue` 和令牌授予机制的结合，`StoreWorkType` 使得 CockroachDB 能够在多租户环境下提供稳定的性能和良好的资源管理。