# 第九章补充：Granter深度解析——准入控制的资源守门人

## 引言

在CockroachDB的准入控制系统中，如果说WorkQueue是请求的排队场所，那么Granter就是资源的守门人（Gatekeeper）。Granter负责决定是否授予资源（slots或tokens）给等待的工作，它是整个准入控制机制的核心执行者。本章将深入剖析Granter的设计哲学、实现细节以及它在系统中的关键作用。

## 1. Granter接口体系：层次化的资源管理

### 1.1 granter接口：基础契约

granter接口定义在`pkg/util/admission/admission.go:190-254`，它是所有granter实现必须遵守的基础契约：

```go
// granter is paired with a requester in that a requester for a particular
// WorkKind will interact with a granter. See admission.go for an overview of
// how this fits into the overall structure.
type granter interface {
	// tryGet is used by a requester to get slots/tokens for a piece of work
	// that has encountered no waiting/queued work. This is the fast path that
	// avoids queueing in the requester.
	tryGet(getterQual burstQualification, count int64) (granted bool)

	// returnGrant is called for:
	// - returning slots after use.
	// - returning tokens after use, if all the granted tokens were not used.
	// - returning either slots or tokens when the grant raced with the work
	//   being canceled, and the grantee did not end up doing any work.
	returnGrant(count int64)

	// tookWithoutPermission informs the granter that a slot or tokens were
	// taken unilaterally, without permission.
	tookWithoutPermission(count int64)

	// continueGrantChain is called by the requester at some point after grant
	// was called on the requester.
	continueGrantChain(grantChainID grantChainID)
}
```

**接口设计要点**：

1. **tryGet**：快速路径（fast path）
   - 当没有排队工作时，直接尝试获取资源
   - 避免了不必要的入队和出队开销
   - 参数`burstQualification`用于区分是否允许突发（burst）

2. **returnGrant**：资源归还
   - slots使用完毕后必须归还
   - tokens如果没有全部使用也可以归还
   - 处理取消竞态（cancellation race）

3. **tookWithoutPermission**：强制占用通知
   - 高优先级工作可以绕过准入控制
   - 用于避免死锁（如意图解析生成的KV工作）
   - granter需要知道这些"黑市"资源使用

4. **continueGrantChain**：授权链延续
   - 实现自然节流（natural throttling）
   - 考虑了goroutine调度器的实际能力

### 1.2 granterWithLockedCalls接口：锁定调用的实现层

```go
// granterWithLockedCalls is an encapsulation of typically one
// granter-requester pair. It is used as an internal
// implementation detail of the GrantCoordinator.
type granterWithLockedCalls interface {
	// tryGetLocked is the real implementation of tryGet from the granter
	// interface. demuxHandle is an opaque handle that was passed into the
	// GrantCoordinator.
	tryGetLocked(count int64, demuxHandle int8) grantResult

	// returnGrantLocked is the real implementation of returnGrant from the
	// granter interface.
	returnGrantLocked(count int64, demuxHandle int8)

	// tookWithoutPermissionLocked is the real implementation of
	// tookWithoutPermission from the granter interface.
	tookWithoutPermissionLocked(count int64, demuxHandle int8)

	// The following methods are for direct use by GrantCoordinator.

	// requesterHasWaitingRequests returns whether some requester associated
	// with the granter has waiting requests.
	requesterHasWaitingRequests() bool

	// tryGrantLocked is used to attempt to grant to waiting requests.
	tryGrantLocked(grantChainID grantChainID) grantResult
}
```

**设计目的**：
- 将锁管理集中在GrantCoordinator中
- 避免granter和requester之间的锁竞争
- 提供更细粒度的控制

**锁顺序规则**（重要！）：
> granter的锁必须在requester的锁之前获取。requester在调用granter时不能持有自己的锁。

### 1.3 grantResult：授权结果的三态表示

```go
type grantResult int8

const (
	grantSuccess grantResult = iota
	// grantFailDueToSharedResource is returned when the granter is unable to
	// grant because a shared resource (CPU or memory) is overloaded.
	grantFailDueToSharedResource
	// grantFailLocal is returned when the granter is unable to grant due to (a)
	// a local constraint -- insufficient tokens or slots, or (b) no work is
	// waiting.
	grantFailLocal
)
```

**三种结果的含义**：

1. **grantSuccess**：授权成功
   - 资源充足，工作可以开始执行

2. **grantFailDueToSharedResource**：共享资源过载
   - CPU或内存等共享资源不足
   - 对于授权链（grant chain），这是终止信号

3. **grantFailLocal**：本地约束失败
   - 本granter的tokens/slots不足
   - 或者没有等待的工作

## 2. slotGranter：基于槽位的资源管理

### 2.1 结构体定义

```go
// slotGranter implements granterWithLockedCalls.
type slotGranter struct {
	coord      *GrantCoordinator
	workKind   WorkKind
	requester  requester
	usedSlots  int        // 已使用的槽位数
	totalSlots int        // 总槽位数
	// skipSlotEnforcement is a dynamic value that changes based on the sampling
	// period of cpu load.
	skipSlotEnforcement bool

	usedSlotsMetric              *metric.Gauge
	slotsExhaustedDurationMetric *metric.Counter
	exhaustedStart               time.Time
}
```

**关键字段**：
- `usedSlots`：当前正在使用的槽位数
- `totalSlots`：总共可用的槽位数
- `skipSlotEnforcement`：是否跳过槽位强制（CPU负载采样周期 > 1ms时为true）
- `exhaustedStart`：槽位耗尽的起始时间（用于统计）

### 2.2 核心方法实现

#### tryGetLocked：尝试获取槽位

```go
// tryGetLocked implements granterWithLockedCalls.
func (sg *slotGranter) tryGetLocked(count int64, _ int8) grantResult {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	if sg.usedSlots < sg.totalSlots || sg.skipSlotEnforcement {
		sg.usedSlots++
		if sg.usedSlots == sg.totalSlots {
			sg.exhaustedStart = timeutil.Now()  // 记录耗尽时间
		}
		sg.usedSlotsMetric.Update(int64(sg.usedSlots))
		return grantSuccess
	}
	if sg.workKind == KVWork {
		return grantFailDueToSharedResource  // KVWork失败表示共享资源不足
	}
	return grantFailLocal
}
```

**实现要点**：

1. **槽位必须是1**：slots总是以单个单位分配（与tokens不同）

2. **两种授权条件**：
   - 正常情况：`usedSlots < totalSlots`
   - 特殊情况：`skipSlotEnforcement == true`（CPU监控失效时）

3. **耗尽时间追踪**：精确记录资源耗尽的时刻，用于性能分析

4. **失败类型区分**：
   - KVWork失败 → `grantFailDueToSharedResource`（因为KVWork是最高优先级，它的失败意味着CPU等共享资源不足）
   - 其他 → `grantFailLocal`

#### returnGrantLocked：归还槽位

```go
// returnGrantLocked implements granterWithLockedCalls.
func (sg *slotGranter) returnGrantLocked(count int64, _ int8) {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	if sg.usedSlots == sg.totalSlots {
		now := timeutil.Now()
		exhaustedMicros := now.Sub(sg.exhaustedStart).Microseconds()
		sg.slotsExhaustedDurationMetric.Inc(exhaustedMicros)  // 累计耗尽时长
	}
	sg.usedSlots--
	if sg.usedSlots < 0 {
		panic(errors.AssertionFailedf("used slots is negative %d", sg.usedSlots))
	}
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}
```

**实现要点**：

1. **耗尽时长统计**：在从"完全耗尽"状态恢复时，记录持续时间

2. **安全检查**：确保usedSlots不会变成负数（这表明代码有bug）

3. **指标更新**：实时更新Prometheus指标

#### tookWithoutPermissionLocked：强制占用

```go
// tookWithoutPermissionLocked implements granterWithLockedCalls.
func (sg *slotGranter) tookWithoutPermissionLocked(count int64, _ int8) {
	if count != 1 {
		panic(errors.AssertionFailedf("unexpected count: %d", count))
	}
	sg.usedSlots++
	if sg.usedSlots == sg.totalSlots {
		sg.exhaustedStart = timeutil.Now()
	}
	sg.usedSlotsMetric.Update(int64(sg.usedSlots))
}
```

**实现要点**：
- 直接增加`usedSlots`，不检查是否超过`totalSlots`
- 允许`usedSlots > totalSlots`（过载情况）
- 这是高优先级工作或避免死锁所必需的

### 2.3 动态槽位调整

```go
func (sg *slotGranter) setTotalSlotsLocked(totalSlots int) {
	if totalSlots == sg.totalSlots {
		return
	}
	sg.setTotalSlotsLockedInternal(totalSlots)
}

func (sg *slotGranter) setTotalSlotsLockedInternal(totalSlots int) {
	if totalSlots > sg.totalSlots {
		// 增加槽位
		if sg.totalSlots <= sg.usedSlots && totalSlots > sg.usedSlots {
			// 从耗尽状态恢复
			now := timeutil.Now()
			exhaustedMicros := now.Sub(sg.exhaustedStart).Microseconds()
			sg.slotsExhaustedDurationMetric.Inc(exhaustedMicros)
		}
	} else if totalSlots < sg.totalSlots {
		// 减少槽位
		if sg.totalSlots > sg.usedSlots && totalSlots <= sg.usedSlots {
			// 进入耗尽状态
			sg.exhaustedStart = timeutil.Now()
		}
	}
	sg.totalSlots = totalSlots
}
```

**动态调整的场景**：
- CPU负载变化 → kvSlotAdjuster动态调整总槽位数
- 从过载恢复 → 增加槽位
- 检测到过载 → 减少槽位

## 3. tokenGranter：基于令牌的资源管理

### 3.1 结构体定义

```go
// tokenGranter implements granterWithLockedCalls.
type tokenGranter struct {
	coord                *GrantCoordinator
	workKind             WorkKind
	requester            requester
	availableBurstTokens int64  // 当前可用的突发令牌数
	maxBurstTokens       int64  // 最大突发令牌数
	skipTokenEnforcement bool
	// Non-nil for all uses of tokenGranter (SQLKVResponseWork and
	// SQLSQLResponseWork).
	cpuOverload cpuOverloadIndicator  // CPU过载指示器
}
```

**与slotGranter的关键区别**：

1. **令牌可以批量分配**：`count`可以 > 1
2. **有突发限制**：`maxBurstTokens`限制短时间内的令牌消耗
3. **CPU过载检查**：每次分配都检查`cpuOverload.isOverloaded()`

### 3.2 核心方法实现

#### refillBurstTokens：周期性补充令牌

```go
func (tg *tokenGranter) refillBurstTokens(skipTokenEnforcement bool) {
	tg.availableBurstTokens = tg.maxBurstTokens
	tg.skipTokenEnforcement = skipTokenEnforcement
}
```

**调用时机**：
- GrantCoordinator.CPULoad() 定期调用（通常每1ms）
- 模拟令牌桶的"令牌补充"机制

#### tryGetLocked：尝试获取令牌

```go
// tryGetLocked implements granterWithLockedCalls.
func (tg *tokenGranter) tryGetLocked(count int64, _ int8) grantResult {
	if tg.cpuOverload.isOverloaded() {
		return grantFailDueToSharedResource  // CPU过载，拒绝授权
	}
	if tg.availableBurstTokens > 0 || tg.skipTokenEnforcement {
		tg.availableBurstTokens -= count  // 可以变成负数！
		return grantSuccess
	}
	return grantFailLocal
}
```

**重要特性**：

1. **CPU过载优先检查**：即使有令牌，CPU过载也会拒绝

2. **令牌可以透支**：
   ```
   假设 maxBurstTokens = 100, availableBurstTokens = 10
   请求 count = 50
   结果：availableBurstTokens = -40 (允许！)
   ```

3. **为什么允许透支**：
   - 请求已经在队列中等待
   - 拒绝会导致饿死（starvation）
   - 下次refill时会恢复正常

#### returnGrantLocked：归还令牌

```go
// returnGrantLocked implements granterWithLockedCalls.
func (tg *tokenGranter) returnGrantLocked(count int64, _ int8) {
	tg.availableBurstTokens += count
	if tg.availableBurstTokens > tg.maxBurstTokens {
		tg.availableBurstTokens = tg.maxBurstTokens  // 上限限制
	}
}
```

**实现要点**：
- 归还的令牌有上限（`maxBurstTokens`）
- 防止令牌无限累积

### 3.3 tryGrantLocked：向等待队列授权

```go
// tryGrantLocked implements granterWithLockedCalls.
func (tg *tokenGranter) tryGrantLocked(grantChainID grantChainID) grantResult {
	res := tg.tryGetLocked(1, 0 /*arbitrary*/)  // 先尝试获取1个令牌
	if res == grantSuccess {
		tokens := tg.requester.granted(grantChainID)  // 通知requester
		if tokens == 0 {
			// Did not accept grant. (请求已取消)
			tg.returnGrantLocked(1, 0 /*arbitrary*/)
			return grantFailLocal
		} else if tokens > 1 {
			// 实际需要更多令牌
			tg.tookWithoutPermissionLocked(tokens-1, 0 /*arbitrary*/)
		}
	}
	return res
}
```

**工作流程**：

1. 先乐观地获取1个令牌
2. 通知requester有可用资源
3. requester返回实际需要的令牌数
4. 如果需要更多，用`tookWithoutPermission`补足

**设计理由**：
- 在调用requester之前，不知道实际需要多少令牌
- WorkInfo.RequestedCount可能是估算值
- 允许requester动态调整需求

## 4. kvStoreTokenGranter：存储层的复杂令牌管理

### 4.1 架构概述

kvStoreTokenGranter是最复杂的granter实现，它管理存储层的IO令牌，涉及多个维度：

```
kvStoreTokenGranter
├── regularRequester  (常规工作队列)
├── elasticRequester  (弹性工作队列)
└── snapshotRequester (快照接收队列)
     ↓
每个requester通过kvStoreTokenChildGranter与parent交互
     ↓
parent管理三种令牌：
  1. IO Tokens (L0刷新/压缩容量)
  2. Elastic IO Tokens (弹性工作专用)
  3. Disk Bandwidth Tokens (磁盘带宽)
```

### 4.2 结构体定义

```go
type kvStoreTokenGranter struct {
	knobs             *TestingKnobs
	regularRequester  requester
	elasticRequester  requester
	snapshotRequester requester

	mu struct {
		syncutil.Mutex

		// "IO" tokens代表L0的刷新/压缩容量
		// 所有工作都从availableIOTokens中扣除
		// 常规工作：availableIOTokens[RegularWorkClass] > 0
		// 弹性工作：availableIOTokens[ElasticWorkClass] > 0
		availableIOTokens            [admissionpb.NumWorkClasses]int64
		elasticIOTokensUsedByElastic int64

		// 磁盘带宽令牌
		diskTokensAvailable diskTokens
		diskTokensError     struct {
			prevObservedWrites             uint64
			prevObservedReads              uint64
			diskWriteTokensAlreadyDeducted int64
			diskReadTokensAlreadyDeducted  int64
		}
		diskTokensUsed [admissionpb.NumStoreWorkTypes]diskTokens

		// 耗尽时间追踪
		exhaustedStart [admissionpb.NumWorkClasses]time.Time
		startingIOTokens int64

		// 估算模型
		l0WriteLM, l0IngestLM, ingestLM, writeAmpLM tokensLinearModel
	}

	ioTokensExhaustedDurationMetric [admissionpb.NumWorkClasses]*metric.Counter
	availableTokensMetric           [admissionpb.NumWorkClasses]*metric.Gauge
	tokensReturnedMetric            *metric.Counter
	tokensTakenMetric               *metric.Counter
}
```

### 4.3 多维度令牌管理

#### tryGetLocked：多种令牌的协同判断

```go
func (sg *kvStoreTokenGranter) tryGetLocked(count int64, wt admissionpb.StoreWorkType) bool {
	// 应用写放大模型计算磁盘写令牌
	diskWriteTokens := count
	if wt != admissionpb.SnapshotIngestStoreWorkType {
		diskWriteTokens = sg.mu.writeAmpLM.applyLinearModel(count)
	}

	switch wt {
	case admissionpb.RegularStoreWorkType:
		// 常规工作只需检查IO令牌
		if sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0 {
			sg.subtractIOTokensLocked(count, count, false)
			sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return true
		}
	case admissionpb.ElasticStoreWorkType:
		// 弹性工作需要检查三种令牌
		if sg.mu.diskTokensAvailable.writeByteTokens > 0 &&
			sg.mu.availableIOTokens[admissionpb.RegularWorkClass] > 0 &&
			sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] > 0 {
			sg.subtractIOTokensLocked(count, count, false)
			sg.mu.elasticIOTokensUsedByElastic += count
			sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return true
		}
	case admissionpb.SnapshotIngestStoreWorkType:
		// 快照摄入只检查磁盘写令牌（不进L0）
		if sg.mu.diskTokensAvailable.writeByteTokens > 0 {
			sg.mu.diskTokensAvailable.writeByteTokens -= diskWriteTokens
			sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted += diskWriteTokens
			sg.mu.diskTokensUsed[wt].writeByteTokens += diskWriteTokens
			return true
		}
	}
	return false
}
```

**三种工作类型的令牌需求**：

| 工作类型 | IO Tokens (Regular) | IO Tokens (Elastic) | Disk Write Tokens |
|---------|---------------------|---------------------|-------------------|
| Regular | ✓ | × | ✓ |
| Elastic | ✓ | ✓ | ✓ |
| Snapshot Ingest | × | × | ✓ |

**设计理由**：

1. **Regular工作优先级最高**：只要RegularWorkClass的IO令牌足够就能执行

2. **Elastic工作受多重限制**：
   - 必须有足够的Regular IO令牌（避免影响常规工作）
   - 必须有足够的Elastic IO令牌（自己的配额）
   - 必须有足够的磁盘带宽令牌（避免磁盘带宽饱和）

3. **Snapshot Ingest特殊处理**：
   - 不经过L0，所以不需要IO令牌
   - 只消耗磁盘带宽

### 4.4 令牌估算与调整

#### storeReplicatedWorkAdmittedLocked：事后调整

```go
func (sg *kvStoreTokenGranter) storeReplicatedWorkAdmittedLocked(
	wt admissionpb.StoreWorkType,
	originalTokens int64,
	admittedInfo storeReplicatedWorkAdmittedInfo,
	canGrantAnother bool,
) (additionalTokens int64) {
	// 应用L0写入模型
	actualL0WriteTokens := sg.mu.l0WriteLM.applyLinearModel(admittedInfo.WriteBytes)
	// 应用L0摄入模型
	actualL0IngestTokens := sg.mu.l0IngestLM.applyLinearModel(admittedInfo.IngestedBytes)
	actualL0Tokens := actualL0WriteTokens + actualL0IngestTokens

	// 计算需要额外扣除的令牌
	additionalL0TokensNeeded := actualL0Tokens - originalTokens
	sg.subtractIOTokensLocked(additionalL0TokensNeeded, additionalL0TokensNeeded, false)

	if wt == admissionpb.ElasticStoreWorkType {
		sg.mu.elasticIOTokensUsedByElastic += additionalL0TokensNeeded
	}

	// 调整磁盘写令牌
	ingestIntoLSM := sg.mu.ingestLM.applyLinearModel(admittedInfo.IngestedBytes)
	totalBytesIntoLSM := actualL0WriteTokens + ingestIntoLSM
	actualDiskWriteTokens := sg.mu.writeAmpLM.applyLinearModel(totalBytesIntoLSM)
	originalDiskTokens := sg.mu.writeAmpLM.applyLinearModel(originalTokens)
	additionalDiskWriteTokens := actualDiskWriteTokens - originalDiskTokens
	sg.mu.diskTokensAvailable.writeByteTokens -= additionalDiskWriteTokens
	sg.mu.diskTokensUsed[wt].writeByteTokens += additionalDiskWriteTokens

	// 如果从耗尽状态恢复，尝试授权更多请求
	if canGrantAnother && (additionalL0TokensNeeded < 0) {
		exhaustedFunc := func() bool {
			return sg.mu.availableIOTokens[admissionpb.RegularWorkClass] <= 0 ||
				(wc == admissionpb.ElasticWorkClass && (sg.mu.diskTokensAvailable.writeByteTokens <= 0 ||
					sg.mu.availableIOTokens[admissionpb.ElasticWorkClass] <= 0))
		}
		wasExhausted := exhaustedFunc()
		isExhausted := exhaustedFunc()
		if (wasExhausted && !isExhausted) || sg.knobs.AlwaysTryGrantWhenAdmitted {
			sg.tryGrantLocked()  // 尝试授权等待队列中的请求
		}
	}

	return additionalL0TokensNeeded
}
```

**两阶段令牌扣除**：

1. **准入时（Admit）**：
   - 使用估算值预扣令牌
   - 基于`RequestedCount`和线性模型

2. **完成时（Done/Admitted）**：
   - 使用实际值调整令牌
   - 可能补扣（实际 > 估算）
   - 也可能退还（实际 < 估算）

**为什么需要两阶段**：
- 准入时不知道实际写入量
- 异步复制（below-raft）进一步延迟了实际值的获知
- 估算模型会定期更新（每15秒）

### 4.5 磁盘令牌误差校正

#### adjustDiskTokenErrorLocked：处理估算误差

```go
func (sg *kvStoreTokenGranter) adjustDiskTokenErrorLocked(readBytes uint64, writeBytes uint64) {
	intWrites := int64(writeBytes - sg.mu.diskTokensError.prevObservedWrites)
	intReads := int64(readBytes - sg.mu.diskTokensError.prevObservedReads)

	// 补偿写入误差
	writeError := intWrites - sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted
	if writeError > 0 {
		sg.mu.diskTokensAvailable.writeByteTokens -= writeError
	}

	// 补偿读取误差
	readError := intReads - sg.mu.diskTokensError.diskReadTokensAlreadyDeducted
	if readError > 0 {
		sg.mu.diskTokensAvailable.writeByteTokens -= readError
	}

	// 重置扣除计数，准备下一个周期
	sg.mu.diskTokensError.diskWriteTokensAlreadyDeducted = 0
	sg.mu.diskTokensError.diskReadTokensAlreadyDeducted = 0

	sg.mu.diskTokensError.prevObservedWrites = writeBytes
	sg.mu.diskTokensError.prevObservedReads = readBytes
}
```

**误差产生的原因**：

1. **写入误差**：
   - 估算的写放大 ≠ 实际写放大
   - 后台压缩/刷新导致的额外写入

2. **读取误差**：
   - 读取在准入时不预扣令牌
   - 但读取会消耗磁盘带宽

**校正策略**：
- 每个调整周期检查实际磁盘IO
- 如果实际 > 已扣除，从可用令牌中补扣
- 防止磁盘带宽被低估

### 4.6 授权优先级

#### tryGrantLockedOne：优先级排序

```go
func (sg *kvStoreTokenGranter) tryGrantLockedOne() bool {
	// NB: We grant work in the following priority order: regular, snapshot
	// ingest, elastic work.
	for wt := admissionpb.StoreWorkType(0); wt < admissionpb.NumStoreWorkTypes; wt++ {
		req := sg.regularRequester
		if wt == admissionpb.ElasticStoreWorkType {
			req = sg.elasticRequester
		} else if wt == admissionpb.SnapshotIngestStoreWorkType {
			req = sg.snapshotRequester
		}
		hasWaiting, _ := req.hasWaitingRequests()
		if hasWaiting {
			res := sg.tryGetLocked(1, wt)
			if res {
				tookTokenCount := req.granted(noGrantChain)
				if tookTokenCount == 0 {
					// Did not accept grant.
					sg.returnGrantLocked(1, wt)
				} else {
					// May have taken more.
					if tookTokenCount > 1 {
						sg.tookWithoutPermissionLocked(tookTokenCount-1, wt)
					}
					return true
				}
			} else {
				// 无法获取令牌，不继续尝试更低优先级的工作
				return res
			}
		}
	}
	return false
}
```

**授权优先级**：
```
1. Regular (常规工作) - 用户前台请求
2. Snapshot Ingest (快照摄入) - 节点再平衡
3. Elastic (弹性工作) - 后台任务
```

**设计理由**：
- Regular优先保证用户体验
- Snapshot优先保证集群健康（副本恢复）
- Elastic最后执行，可以被抢占

## 5. kvStoreTokenChildGranter：委托模式

### 5.1 结构体定义

```go
// kvStoreTokenChildGranter handles a particular workClass. Its methods
// pass-through to the parent after adding the workClass as a parameter.
type kvStoreTokenChildGranter struct {
	workType admissionpb.StoreWorkType
	parent   *kvStoreTokenGranter
}
```

### 5.2 委托实现

```go
// tryGet implements granter.
func (cg *kvStoreTokenChildGranter) tryGet(_ burstQualification, count int64) bool {
	return cg.parent.tryGet(cg.workType, count)
}

// returnGrant implements granter.
func (cg *kvStoreTokenChildGranter) returnGrant(count int64) {
	cg.parent.returnGrant(cg.workType, count)
}

// tookWithoutPermission implements granter.
func (cg *kvStoreTokenChildGranter) tookWithoutPermission(count int64) {
	cg.parent.tookWithoutPermission(cg.workType, count)
}

// continueGrantChain implements granter.
func (cg *kvStoreTokenChildGranter) continueGrantChain(grantChainID grantChainID) {
	// Ignore since grant chains are not used for store tokens.
}
```

**设计模式：委托（Delegation）**

```
WorkQueue → kvStoreTokenChildGranter → kvStoreTokenGranter
            (接口适配器)              (真正的实现)
```

**为什么需要Child Granter**：

1. **接口统一**：WorkQueue只需要与granter接口交互
2. **工作类型注入**：childGranter携带workType信息
3. **代码复用**：parent实现被三个child共享

## 6. Granter与Requester的协作模式

### 6.1 快速路径（Fast Path）

```
┌─────────┐
│WorkQueue│ (requester)
└────┬────┘
     │ 1. q.granter.tryGet(count)
     ↓
┌─────────┐
│ Granter │
└────┬────┘
     │ 2. 检查资源
     ↓
   成功？
   ├── Yes → 立即执行
   └── No  → 进入慢路径
```

**代码示例**（WorkQueue.Admit）：

```go
if granted := q.granter.tryGet(canBurst, requestedCount); granted {
	// 快速路径成功，无需排队
	q.metrics.incAdmitted(info.Priority)
	return true, nil
}
// 失败，需要入队等待
```

### 6.2 慢路径（Slow Path）

```
┌─────────┐
│WorkQueue│
└────┬────┘
     │ 1. 入队等待
     ↓
┌──────────┐
│等待队列   │
└────┬─────┘
     │
     ↓
┌────────────────┐
│GrantCoordinator│
└────┬───────────┘
     │ 2. 周期性/事件驱动调用
     ↓
┌─────────┐
│ Granter │
└────┬────┘
     │ 3. granter.tryGrantLocked()
     ↓
┌─────────┐
│WorkQueue│
└────┬────┘
     │ 4. requester.granted()
     ↓
  通知等待的
  goroutine
```

### 6.3 授权链（Grant Chain）

```
请求1                    请求2                    请求3
  │                       │                       │
  ↓ granted()             │                       │
执行中...                  │                       │
  │                       │                       │
  ↓ continueGrantChain() ──┘                       │
  完成                    ↓ granted()              │
                        执行中...                  │
                          │                       │
                          ↓ continueGrantChain() ──┘
                          完成                    ↓ granted()
                                                执行中...
```

**设计目的**：
- 自然节流：goroutine调度器的实际能力决定授权速率
- 避免突发：不会一次性授权大量请求
- 减少调度延迟：减少了5倍的突发性，将2秒的p99延迟从调度器转移到准入控制

**实现机制**：

```go
// 在WorkQueue.Admit中
case chainID, ok := <-work.ch:
	// 收到授权
	// ...执行工作...
	q.granter.continueGrantChain(chainID)  // 触发下一次授权
```

## 7. 实战场景分析

### 7.1 场景1：CPU密集型查询的槽位管理

**场景描述**：
- 10个并发的CPU密集型KV查询
- slotGranter配置：totalSlots = 8

**执行流程**：

```
时间线：
T0: 8个请求通过tryGet立即获得slots (快速路径)
    usedSlots = 8, 剩余2个请求进入队列

T1: 某个请求完成，调用AdmittedWorkDone()
    → granter.returnGrant(1)
    → usedSlots = 7
    → GrantCoordinator注意到有等待请求
    → granter.tryGrantLocked()
    → requester.granted()
    → 队列中的1个请求被唤醒
    → usedSlots = 8

T2: 另一个请求完成...
    （循环）
```

**关键点**：
- 并发度严格控制在8
- 保护CPU不被过度占用
- FIFO或优先级队列决定谁先获得slots

### 7.2 场景2：SQL响应处理的令牌爆发

**场景描述**：
- 100个DistSQL leaf node同时完成
- 100个SQLKVResponseWork请求几乎同时到达
- tokenGranter配置：maxBurstTokens = 1000

**执行流程**：

```
时间线：
T0 (ms 0):
  - 前10个请求通过tryGet (快速路径)
  - availableBurstTokens = 1000 → 990
  - 后90个请求入队

T0 (ms 0.5):
  - GrantCoordinator循环
  - tryGrantLocked() 连续授权
  - availableBurstTokens = 990 → 900 → 810 → ...
  - 直到availableBurstTokens <= 0 或 CPU过载

T1 (ms 1):
  - refillBurstTokens() 被CPULoad调用
  - availableBurstTokens = 1000 (重新填充)
  - 继续授权剩余请求

T2 (ms 1.5):
  - 所有请求处理完成
```

**关键点**：
- 令牌限制短期爆发（1ms内最多1000个）
- CPU过载检查作为安全阀
- 周期性补充维持稳定吞吐

### 7.3 场景3：存储IO的多维度控制

**场景描述**：
- 常规写入：100 MB/s
- 弹性写入：50 MB/s
- 快照摄入：200 MB
- 磁盘带宽限制：300 MB/s

**令牌分配**：

```
每15秒调整周期：

1. 计算IO令牌（基于L0压缩）：
   regularIOTokens = 1500 MB (假设)
   elasticIOTokens = 750 MB

2. 计算磁盘带宽令牌：
   diskWriteTokens = 4500 MB (15s * 300 MB/s)

3. setAvailableTokens()设置
```

**执行流程**：

```
T0:
  常规写入请求100 MB
  → 检查：availableIOTokens[Regular] > 0 ✓
  → 扣除：100 MB IO令牌
  → 应用写放大模型：100 MB × 3 = 300 MB
  → 扣除：300 MB 磁盘令牌
  → 授权成功

T1:
  弹性写入请求50 MB
  → 检查：availableIOTokens[Regular] > 0 ✓
  → 检查：availableIOTokens[Elastic] > 0 ✓
  → 检查：diskTokensAvailable > 0 ✓
  → 扣除：50 MB IO令牌 (Regular和Elastic都扣)
  → 应用写放大模型：50 MB × 3 = 150 MB
  → 扣除：150 MB 磁盘令牌
  → 授权成功

T2:
  快照摄入请求200 MB
  → 检查：diskTokensAvailable > 0 ✓
  → 扣除：200 MB 磁盘令牌（不应用写放大！）
  → 授权成功

T3:
  磁盘令牌耗尽，弹性工作被阻塞
  常规工作和快照继续（它们的IO令牌还够）

T4 (下个周期):
  调整磁盘令牌误差
  → 实际磁盘写入 > 已扣除令牌
  → 补扣差额
  → 重新分配令牌
```

**关键点**：
- 多维度限制保护不同资源
- 弹性工作受最严格约束
- 误差校正防止资源低估

## 8. 设计模式总结

### 8.1 策略模式（Strategy Pattern）

```
granter接口 (策略接口)
    ↓
├── slotGranter (基于槽位的策略)
├── tokenGranter (基于令牌的策略)
└── kvStoreTokenGranter (复杂令牌策略)
```

**优点**：
- WorkQueue不关心具体的授权算法
- 可以根据WorkKind选择不同的策略
- 易于测试和扩展

### 8.2 委托模式（Delegation Pattern）

```
kvStoreTokenChildGranter → kvStoreTokenGranter
```

**优点**：
- 接口适配
- 工作类型注入
- 代码复用

### 8.3 双层锁设计（Two-Level Locking）

```
granter接口层：
  tryGet(), returnGrant()  (无锁版本)
      ↓
  通过GrantCoordinator
      ↓
granterWithLockedCalls接口层：
  tryGetLocked(), returnGrantLocked()  (持锁版本)
```

**优点**：
- 锁管理集中在GrantCoordinator
- 避免granter和requester之间的死锁
- 清晰的锁职责

### 8.4 模板方法模式（Template Method）

```go
// tryGrantLocked的模板
func (g *granter) tryGrantLocked(grantChainID grantChainID) grantResult {
	res := g.tryGetLocked(1, 0)           // 步骤1：获取资源
	if res == grantSuccess {
		tokens := g.requester.granted(...)  // 步骤2：通知requester
		if tokens == 0 {
			g.returnGrantLocked(1, 0)        // 步骤3a：未接受，归还
			return grantFailLocal
		} else if tokens > 1 {
			g.tookWithoutPermission(...)     // 步骤3b：需要更多，补扣
		}
	}
	return res
}
```

## 9. 最佳实践与注意事项

### 9.1 使用Granter的原则

1. **快速路径优先**：
   ```go
   if granted := q.granter.tryGet(canBurst, count); granted {
       // 立即执行，避免入队开销
   }
   ```

2. **及时归还资源**：
   ```go
   defer func() {
       if admitted {
           q.granter.returnGrant(count)
       }
   }()
   ```

3. **正确处理取消竞态**：
   ```go
   select {
   case <-ctx.Done():
       // 已获得授权但需要取消
       q.granter.returnGrant(count)
       return ctx.Err()
   default:
       // 执行工作
   }
   ```

### 9.2 实现自定义Granter的建议

1. **线程安全**：
   - 所有方法必须是线程安全的
   - 使用mu.Lock()保护共享状态

2. **指标追踪**：
   - 记录资源使用情况
   - 追踪耗尽时间
   - 监控授权/拒绝率

3. **避免死锁**：
   - 遵守锁顺序：granter锁 → requester锁
   - 不在持锁时调用外部代码

4. **性能考虑**：
   - tryGet应该非常快（纳秒级）
   - 避免复杂计算
   - 使用原子操作where possible

### 9.3 调试技巧

1. **检查槽位/令牌泄漏**：
   ```sql
   -- Prometheus查询
   admission_granter_used_slots_kv
   admission_granter_total_slots_kv

   -- 如果used持续增长而不下降，可能有泄漏
   ```

2. **分析耗尽时间**：
   ```sql
   rate(admission_granter_slots_exhausted_duration_kv[5m])

   -- 高耗尽率表明资源不足
   ```

3. **追踪令牌流动**：
   ```sql
   rate(admission_granter_io_tokens_taken_kv[5m])
   rate(admission_granter_io_tokens_returned_kv[5m])

   -- taken >> returned 表明可能有问题
   ```

## 10. 总结

### 10.1 Granter的核心职责

1. **资源守护**：决定是否授予slots/tokens
2. **过载保护**：基于CPU、IO等指标拒绝请求
3. **公平调度**：配合requester实现租户公平
4. **性能优化**：快速路径、授权链等机制

### 10.2 三种Granter的对比

| 特性 | slotGranter | tokenGranter | kvStoreTokenGranter |
|------|-------------|--------------|---------------------|
| **资源类型** | 槽位（整数） | 令牌（批量） | 多种令牌（IO+磁盘） |
| **分配粒度** | 1个slot | 可变数量tokens | 可变，多维度 |
| **补充机制** | 动态调整totalSlots | 周期性refill | 基于IO指标 |
| **过载检查** | 槽位数量 | CPU指示器 | IO令牌+磁盘带宽 |
| **适用场景** | KVWork | SQL响应处理 | 存储IO |
| **复杂度** | 低 | 中 | 高 |

### 10.3 架构洞察

Granter的设计体现了几个重要原则：

1. **分层抽象**：granter → granterWithLockedCalls → 具体实现
2. **策略分离**：不同WorkKind使用不同granter策略
3. **资源多样性**：支持slots、tokens、IO tokens等多种资源模型
4. **可观测性**：丰富的指标和统计信息
5. **性能优先**：快速路径、内联优化、批量操作

这些设计使得CockroachDB能够精细控制资源使用，在多租户环境下提供性能隔离和公平性保证。

---

## 参考源代码位置

- granter接口定义：[pkg/util/admission/admission.go:190-254](pkg/util/admission/admission.go#L190-L254)
- granterWithLockedCalls接口：[pkg/util/admission/admission.go:268-289](pkg/util/admission/admission.go#L268-L289)
- slotGranter实现：[pkg/util/admission/granter.go:30-163](pkg/util/admission/granter.go#L30-L163)
- tokenGranter实现：[pkg/util/admission/granter.go:165-251](pkg/util/admission/granter.go#L165-L251)
- kvStoreTokenGranter实现：[pkg/util/admission/granter.go:269-798](pkg/util/admission/granter.go#L269-L798)
- grantResult定义：[pkg/util/admission/admission.go:427-439](pkg/util/admission/admission.go#L427-L439)

---

**本章完**
