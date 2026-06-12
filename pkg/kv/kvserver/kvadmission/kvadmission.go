// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvadmission is the integration layer between KV and admission
// control.
package kvadmission

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// elasticCPUDurationPerExportRequest controls how many CPU tokens are allotted
// for each export request.
var elasticCPUDurationPerExportRequest = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.elastic_cpu.duration_per_export_request",
	"controls how many CPU tokens are allotted for each export request",
	admission.MaxElasticCPUDuration,
	settings.DurationInRange(admission.MinElasticCPUDuration, admission.MaxElasticCPUDuration),
)

// elasticCPUDurationPerInternalLowPriRead controls how many CPU tokens are
// allotted for each internally submitted low priority read request.
var elasticCPUDurationPerInternalLowPriRead = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.elastic_cpu.duration_per_low_pri_read",
	"controls how many CPU tokens are allotted for each internally submitted low priority read request",
	10*time.Millisecond,
	settings.DurationInRange(admission.MinElasticCPUDuration, admission.MaxElasticCPUDuration),
)

// elasticAdmissionAllLowPri determines whether internally
// submitted low bulk pri requests integrate with elastic CPU control.
var elasticAdmissionAllLowPri = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kvadmission.elastic_control_bulk_low_priority.enabled",
	"determines whether the all low bulk priority requests integrate with elastic CPU control",
	true,
)

// exportRequestElasticControlEnabled determines whether export requests
// integrate with elastic CPU control.
var exportRequestElasticControlEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kvadmission.export_request_elastic_control.enabled",
	"determines whether the export requests integrate with elastic CPU control",
	true,
)

// elasticCPUDurationPerRangefeedScanUnit controls how many CPU tokens are
// allotted for each unit of work during rangefeed catchup scans. Only takes
// effect if kvadmission.rangefeed_catchup_scan_elastic_control.enabled is set.
var elasticCPUDurationPerRangefeedScanUnit = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.elastic_cpu.duration_per_rangefeed_scan_unit",
	"controls how many CPU tokens are allotted for each unit of work during rangefeed catchup scans",
	admission.MaxElasticCPUDuration,
	settings.DurationInRange(admission.MinElasticCPUDuration, admission.MaxElasticCPUDuration),
)

// rangefeedCatchupScanElasticControlEnabled determines whether rangefeed catch
// up scans integrate with elastic CPU control.
var rangefeedCatchupScanElasticControlEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kvadmission.rangefeed_catchup_scan_elastic_control.enabled",
	"determines whether rangefeed catchup scans integrate with the elastic CPU control",
	true,
)

// ProvisionedBandwidth set a value of the provisioned
// bandwidth for each store in the cluster.
var ProvisionedBandwidth = settings.RegisterByteSizeSetting(
	settings.SystemOnly, "kvadmission.store.provisioned_bandwidth",
	"if set to a non-zero value, this is used as the provisioned bandwidth (in bytes/s), for "+
		"each store. It can be overridden on a per-store basis using the --store flag. Note that "+
		"setting the provisioned bandwidth to a positive value may enable disk bandwidth based "+
		"admission control, since admission.disk_bandwidth_tokens.elastic.enabled defaults to true",
	0,
	settings.WithPublic)

// FlowTokenDropInterval determines the frequency at which we check for pending
// flow token dispatches to nodes we're no longer connected to, in order to drop
// them.
var FlowTokenDropInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.flow_token.drop_interval",
	"the interval at which the raft transport checks for pending flow token dispatches "+
		"to nodes we're no longer connected to, in order to drop them; set to 0 to disable the mechanism",
	30*time.Second,
)

// FlowTokenDispatchInterval determines the frequency at which we check for
// pending flow token dispatches from idle connections, in order to deliver
// them.
var FlowTokenDispatchInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.flow_token.dispatch_interval",
	"the interval at which the raft transport checks for pending flow token dispatches "+
		"from idle connections and delivers them",
	time.Second,
	settings.PositiveDuration, settings.NonNegativeDurationWithMaximum(time.Minute),
)

// FlowTokenDispatchMaxBytes determines the maximum number of bytes of dispatch
// messages that are annotated onto a single RaftTransport message.
var FlowTokenDispatchMaxBytes = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kvadmission.flow_control.dispatch.max_bytes",
	"limits the size of flow control dispatch messages being attached to a single raft message",
	64<<20,                         // 64 MB
	settings.IntWithMinimum(1<<20), // 1 MB
)

// ConnectedStoreExpiration controls how long the RaftTransport layers considers
// a stream connected without it having observed any messages from it.
var ConnectedStoreExpiration = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kvadmission.raft_transport.connected_store_expiration",
	"the interval at which the raft transport prunes its set of connected stores; set to 0 to disable the mechanism",
	5*time.Minute,
)

// useRangeTenantIDForNonAdminEnabled determines whether the range's tenant ID
// is used by admission control when called by the system tenant. When false,
// the requester's tenant ID (i.e., the system tenant ID) is used.
var useRangeTenantIDForNonAdminEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kvadmission.use_range_tenant_id_for_non_admin.enabled",
	"when true, and the caller is the system tenant, the tenantID used by admission control "+
		"for non-admin requests is overridden to the range's tenantID",
	true,
)

// Controller provides admission control for the KV layer.
type Controller interface {
	// AdmitKVWork must be called before performing KV work.
	// BatchRequest.AdmissionHeader and BatchRequest.Replica.StoreID must be
	// populated for admission to work correctly. The requestTenantID represents
	// the authenticated caller and must be populated. The rangeTenantID
	// represents the tenant of the range on which the work is being performed
	// -- in rare cases it may be unpopulated.
	//
	// If err is non-nil, the returned handle can be ignored. If err is nil,
	// AdmittedKVWorkDone must be called after the KV work is done executing.
	AdmitKVWork(
		_ context.Context, requestTenantID roachpb.TenantID, rangeTenantID roachpb.TenantID,
		_ *kvpb.BatchRequest,
	) (Handle, error)
	// AdmittedKVWorkDone is called after the admitted KV work is done
	// executing.
	AdmittedKVWorkDone(Handle, *StoreWriteBytes)
	// AdmitRangefeedRequest must be called before serving rangefeed requests.
	// If enabled, it returns a non-nil Pacer that's to be used within rangefeed
	// catchup scans (typically CPU-intensive and affecting scheduling
	// latencies).
	AdmitRangefeedRequest(roachpb.TenantID, *kvpb.RangeFeedRequest) *admission.Pacer
	// SetTenantWeightProvider is used to set the provider that will be
	// periodically polled for weights. The stopper should be used to terminate
	// the periodic polling.
	SetTenantWeightProvider(TenantWeightProvider, *stop.Stopper)
	// SnapshotIngestedOrWritten informs admission control about a range
	// snapshot ingestion or a range snapshot written as a normal write.
	// writeBytes should roughly correspond to the size of the write when
	// flushed to a sstable.
	SnapshotIngestedOrWritten(_ roachpb.StoreID, _ pebble.IngestOperationStats, writeBytes uint64)
	// FollowerStoreWriteBytes informs admission control about writes
	// replicated to a raft follower, that have not been subject to admission
	// control.
	FollowerStoreWriteBytes(roachpb.StoreID, FollowerStoreWriteBytes)
	replica_rac2.ACWorkQueue
	// GetSnapshotQueue returns the SnapshotQueue which is used for ingesting raft
	// snapshots.
	GetSnapshotQueue(roachpb.StoreID) *admission.SnapshotQueue
}

// TenantWeightProvider can be periodically asked to provide the tenant
// weights.
type TenantWeightProvider interface {
	GetTenantWeights() TenantWeights
}

// TenantWeights contains the various tenant weights.
type TenantWeights struct {
	// Node is the node level tenant ID => weight.
	Node map[uint64]uint32
	// Stores contains the per-store tenant weights.
	Stores []TenantWeightsForStore
}

// TenantWeightsForStore contains the tenant weights for a store.
type TenantWeightsForStore struct {
	roachpb.StoreID
	// Weights is tenant ID => weight.
	Weights map[uint64]uint32
}

// controllerImpl implements Controller interface.
type controllerImpl struct {
	nodeID *base.NodeIDContainer

	// Admission control queues and coordinators. All three should be nil or
	// non-nil.
	kvAdmissionQ               *admission.WorkQueue
	storeGrantCoords           *admission.StoreGrantCoordinators
	elasticCPUGrantCoordinator *admission.ElasticCPUGrantCoordinator
	kvflowHandles              kvflowcontrol.ReplicationAdmissionHandles

	settings *cluster.Settings
	every    log.EveryN
}

var _ Controller = &controllerImpl{}

// Handle groups data around some piece admitted work. Depending on the
// type of work, it holds (a) references to specific work queues, (b) state
// needed to inform said work queues of what work was done after the fact, and
// (c) information around how much work a request is allowed to do (used for
// cooperative scheduling with elastic CPU granters).
type Handle struct {
	tenantID             roachpb.TenantID
	storeAdmissionQ      *admission.StoreWorkQueue
	storeWorkHandle      admission.StoreWorkHandle
	elasticCPUWorkHandle *admission.ElasticCPUWorkHandle
	raftAdmissionMeta    *kvflowcontrolpb.RaftAdmissionMeta

	callAdmittedWorkDoneOnKVAdmissionQ bool
	cpuStart                           time.Duration
}

// AnnotateCtx annotates the given context with request-scoped admission
// data, plumbed through the KV stack using context.Contexts.
func (h *Handle) AnnotateCtx(ctx context.Context) context.Context {
	if h.elasticCPUWorkHandle != nil {
		ctx = admission.ContextWithElasticCPUWorkHandle(ctx, h.elasticCPUWorkHandle)
	}
	if h.raftAdmissionMeta != nil {
		ctx = kvflowcontrol.ContextWithMeta(ctx, h.raftAdmissionMeta)
	}
	return ctx
}

// MakeController returns a Controller. All three parameters must together be
// nil or non-nil.
func MakeController(
	nodeID *base.NodeIDContainer,
	kvAdmissionQ *admission.WorkQueue,
	elasticCPUGrantCoordinator *admission.ElasticCPUGrantCoordinator,
	storeGrantCoords *admission.StoreGrantCoordinators,
	kvflowHandles kvflowcontrol.ReplicationAdmissionHandles,
	settings *cluster.Settings,
) Controller {
	return &controllerImpl{
		nodeID:                     nodeID,
		kvAdmissionQ:               kvAdmissionQ,
		storeGrantCoords:           storeGrantCoords,
		elasticCPUGrantCoordinator: elasticCPUGrantCoordinator,
		kvflowHandles:              kvflowHandles,
		settings:                   settings,
		every:                      log.Every(10 * time.Second),
	}
}

// AdmitKVWork implements the Controller interface.
//
// TODO(irfansharif): There's a fair bit happening here and there's no test
// coverage. Fix that.
func (n *controllerImpl) AdmitKVWork(
	ctx context.Context,
	requestTenantID roachpb.TenantID,
	rangeTenantID roachpb.TenantID,
	ba *kvpb.BatchRequest,
) (_ Handle, retErr error) {
	// 如果 kvAdmissionQ 为空，说明当前节点没有启用 KV admission control。
	// 这种情况下直接放行，返回一个空 Handle；调用方后续调用
	// AdmittedKVWorkDone 时也不会产生任何结算动作。
	if n.kvAdmissionQ == nil {
		return Handle{}, nil
	}
	// 根据 BatchRequest 提取 admission 所需的统一信息：
	// - 该按哪个 tenant 计费/限流。
	// - 请求优先级是什么。
	// - 请求创建时间是什么。
	// - 是否应该绕过 admission 阻塞，但仍可能需要做 token accounting。
	admissionInfo := workInfoForBatch(n.settings, requestTenantID, rangeTenantID, ba)
	// Handle 会被返回给调用方，后续：
	// - AnnotateCtx() 会把 raft/elastic CPU 元数据塞进 ctx。
	// - AdmittedKVWorkDone() 会根据 Handle 做 CPU/token/write-bytes 结算。
	ah := Handle{tenantID: admissionInfo.TenantID}
	// admissionEnabled 表示后续是否还要进入节点级 CPU admission。
	// 某些 store admission 路径只做一次性 accounting，不需要后续 done 回调，
	// 这时会把 admissionEnabled 关掉，避免重复 admission。
	admissionEnabled := true
	// Don't subject HeartbeatTxnRequest to the storeAdmissionQ. Even though
	// it would bypass admission, it would consume a slot. When writes are
	// throttled, we start generating more txn heartbeats, which then consume
	// all the slots, causing no useful work to happen. We do want useful work
	// to continue even when throttling since there are often significant
	// number of tokens available.
	//
	// 对写请求先走写入相关的 admission 控制。事务 heartbeat 是特殊请求：
	// 写入被限流时 heartbeat 数量反而可能变多，如果它们也占 storeAdmissionQ
	// slot，就可能把真正能推进事务的写请求饿死，所以这里排除掉。
	if ba.IsWrite() && !ba.IsSingleHeartbeatTxnRequest() {
		var admitted bool
		// kvflowcontrol 是 raft replication admission control：
		// 写请求进入 Raft 前，先检查对应 range 的复制流是否还有 flow tokens。
		attemptFlowControl := kvflowcontrol.Enabled.Get(&n.settings.SV)
		if attemptFlowControl && !admissionInfo.BypassAdmission {
			// 每个 range 会有一个 replication admission handle。
			// 找不到通常表示这个 range 当前没有可用的 flow-control 状态，
			// 这里选择不阻塞，直接放行。
			// 
			// 这段代码是在问：“当前 node 上有没有这个 range 的 Raft flow-control 控制器？
			// 如果有，就先等复制流控 token；如果连本地 replica 都找不到，就不要在 admission 层瞎拦，
			// 直接放过，让后面的 KV 执行路径返回正常的 range/lease 路由错误。”
			kvflowHandle, found := n.kvflowHandles.LookupReplicationAdmissionHandle(ba.RangeID)
			if !found {
				return Handle{}, nil
			}
			var err error
			// 向 range 级 flow control 申请 admission。
			// admitted=true 表示请求已经通过 raft flow-control admission，
			// 后面就不需要再走 leaseholder 本地的 storeAdmissionQ。
			admitted, err = kvflowHandle.Admit(
				ctx, admissionInfo.Priority, timeutil.FromUnixNanos(admissionInfo.CreateTime))
			if err != nil {
				return Handle{}, err
			} else if admitted {
				// NB: It's possible for us to be waiting for available flow tokens
				// for a different set of streams that the ones we'll eventually
				// deduct tokens from, if the range experiences a split between now
				// and the point of deduction. That's ok, there's no strong
				// synchronization needed between these two points.
				//
				// 把 admission 元数据放进 Handle，调用方随后会通过
				// Handle.AnnotateCtx() 写入 ctx，后续 Raft command 编码时会携带这些
				// 信息，用于真正扣减 replication flow tokens。
				ah.raftAdmissionMeta = &kvflowcontrolpb.RaftAdmissionMeta{
					// NOTE: The priority is identical for v1 and v2, a
					// admissionpb.WorkPriority,  until we encode the command in
					// replica_raft, where if the range is using racv2 encoding we will
					// convert the priority to a raftpb.Priority.
					AdmissionPriority:   int32(admissionInfo.Priority),
					AdmissionCreateTime: admissionInfo.CreateTime,
					AdmissionOriginNode: n.nodeID.Get(),
				}
			}
		}
		// If flow control is disabled or if work bypasses flow control, we still
		// subject it above-raft, leaseholder-only IO admission control.
		//
		// 如果 raft flow control 没启用，或者这次没有被 flow control admit，
		// 就退回到 store 级 admission：这是 leaseholder 本地、Raft 之上的
		// IO/token admission，用来保护本 store 不被写入压力打爆。
		if !attemptFlowControl || !admitted {
			// 根据目标 replica 所在 store 找到对应的 store admission queue。
			// 如果没有队列，说明这个 store 当前不做这层 admission，直接继续。
			storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(ba.Replica.StoreID)
			if storeAdmissionQ != nil {
				//  NB: Even though we would know here we're bypassing admission (via
				//  `bypassAdmission`), we still have to explicitly invoke `.Admit()`.
				//  We do it for correct token accounting (i.e. we deduct tokens without
				//  blocking).
				//
				// 即使 admissionInfo.BypassAdmission 为 true，也仍然调用 Admit：
				// bypass 只表示不排队等待，但仍要扣 token/记录 accounting，
				// 否则 admission 控制器看到的负载会失真。
				storeWorkHandle, err := storeAdmissionQ.Admit(
					ctx, admission.StoreWriteWorkInfo{WorkInfo: admissionInfo})
				if err != nil {
					return Handle{}, err
				}
				// 有些 store admission 路径不要求后续 AdmittedWorkDone 回调。
				// UseAdmittedWorkDone() 告诉这里是否需要把 handle 保存下来，
				// 等请求真正执行完后再用写入字节数做结算。
				admissionEnabled = storeWorkHandle.UseAdmittedWorkDone()
				if admissionEnabled {
					defer func() {
						if retErr != nil {
							// No bytes were written.
							// 这里函数使用命名返回值 retErr：
							// 如果 store admission 已经成功，但后续 CPU admission 又失败，
							// 实际 KV work 没有执行，需要立刻归还/结算这次 store admission。
							_ = storeAdmissionQ.AdmittedWorkDone(ah.storeWorkHandle, admission.StoreWorkDoneInfo{})
						}
					}()
					// 保存 store admission 的队列和 handle，供调用方执行完请求后
					// 调用 AdmittedKVWorkDone() 做最终写入字节数 accounting。
					ah.storeAdmissionQ, ah.storeWorkHandle = storeAdmissionQ, storeWorkHandle
				}
			}
		}
	}
	if admissionEnabled {
		// 通过写入侧 admission 后，还要进入节点级 CPU admission。
		// 这里有两套机制：
		// - elastic CPU：给 export/bulk low priority 这类弹性低优先级工作发 CPU 配额。
		// - slots-based queue：普通 KV work 通过 slot 控制并发和 CPU accounting。
		// - Backups generate batches with single export requests, which we
		//   admit through the elastic CPU work queue. We grant this
		//   CPU-intensive work a set amount of CPU time and expect it to
		//   terminate (cooperatively) once it exceeds its grant. The amount
		//   disbursed is 100ms, which we've experimentally found to be long
		//   enough to do enough useful work per-request while not causing too
		//   much in the way of scheduling delays on individual cores. Within
		//   admission control we have machinery that observes scheduling
		//   latencies periodically and reduces the total amount of CPU time
		//   handed out through this mechanism, as a way to provide latency
		//   isolation to non-elastic ("latency sensitive") work running on the
		//   same machine.
		// - We do the same for internally submitted bulk low priority requests in
		//   general (notably, for KV work done on the behalf of row-level TTL
		//   reads or other jobs). Everything admissionpb.UserLowPri and above uses
		//   the slots mechanism.
		//
		// shouldUseElasticCPU 判断这次请求是否属于“可被弹性调度”的低优先级 CPU work。
		// 典型例子是备份 ExportRequest，或者内部提交的 BulkLowPri 读请求。
		shouldUseElasticCPU :=
			(exportRequestElasticControlEnabled.Get(&n.settings.SV) && ba.IsSingleExportRequest()) ||
				(admissionInfo.Priority <= admissionpb.BulkLowPri && elasticAdmissionAllLowPri.Get(&n.settings.SV))

		if shouldUseElasticCPU {
			var admitDuration time.Duration
			if ba.IsSingleExportRequest() {
				// ExportRequest 通常 CPU 消耗较大，使用专门的 export CPU 配额。
				admitDuration = elasticCPUDurationPerExportRequest.Get(&n.settings.SV)
			} else { // isInternalLowPriRead
				// 其它内部低优先级读请求使用单独的低优先级读 CPU 配额。
				admitDuration = elasticCPUDurationPerInternalLowPriRead.Get(&n.settings.SV)
			}

			// TODO(irfansharif): For export requests it's possible to preempt,
			// i.e. once the CPU slice is used up we terminate the work. We
			// don't do this for the general case of low priority internal
			// reads, so in some sense, the integration is incomplete. This is
			// probably harmless.
			//
			// 向 elastic CPU 队列申请一段 CPU 时间。
			// 返回的 elasticWorkHandle 会放进 ctx，底层执行 export/bulk read 时
			// 可以协作式地检查配额是否用完。
			elasticWorkHandle, err := n.elasticCPUGrantCoordinator.ElasticCPUWorkQueue.Admit(
				ctx, admitDuration, admissionInfo,
			)
			if err != nil {
				return Handle{}, err
			}
			// 保存 elastic CPU handle，供 AnnotateCtx() 和最终 AdmittedKVWorkDone() 使用。
			ah.elasticCPUWorkHandle = elasticWorkHandle
			defer func() {
				if retErr != nil {
					// No elastic work was done.
					// elastic CPU admission 已成功，但后续步骤失败时，实际 work 没执行，
					// 需要立即结束这次 elastic handle，避免配额一直被认为占用中。
					n.elasticCPUGrantCoordinator.ElasticCPUWorkQueue.AdmittedWorkDone(ah.elasticCPUWorkHandle)
				}
			}()
		} else {
			// Use the slots-based mechanism for everything else.
			//
			// 普通 KV 请求进入 slots-based admission queue。
			// 这里可能会排队等待 slot；拿到 slot 后，返回值告诉我们后续是否需要
			// 调用 AdmittedWorkDone 来归还 slot/记录 CPU 使用量。
			callAdmittedWorkDoneOnKVAdmissionQ, err := n.kvAdmissionQ.Admit(ctx, admissionInfo)
			if err != nil {
				return Handle{}, err
			}
			if callAdmittedWorkDoneOnKVAdmissionQ {
				// We include the time to do other activities like intent resolution,
				// since it is acceptable to charge them to the tenant.
				//
				// 记录当前 goroutine 已消耗 CPU 时间的起点。
				// 请求完成时会用 grunning.Time() - cpuStart 估算这次 KV work
				// 应该向 tenant admission accounting 计入多少 CPU。
				ah.cpuStart = grunning.Time()
			}
			// 保存是否需要在 AdmittedKVWorkDone() 中通知 kvAdmissionQ。
			ah.callAdmittedWorkDoneOnKVAdmissionQ = callAdmittedWorkDoneOnKVAdmissionQ
		}
	}
	// 返回 admission handle。请求真正执行前，调用方会用它 annotate ctx；
	// 请求执行结束后，调用方必须把它传给 AdmittedKVWorkDone 做收尾 accounting。
	return ah, nil
}

// AdmittedKVWorkDone implements the Controller interface.
func (n *controllerImpl) AdmittedKVWorkDone(ah Handle, writeBytes *StoreWriteBytes) {
	n.elasticCPUGrantCoordinator.ElasticCPUWorkQueue.AdmittedWorkDone(ah.elasticCPUWorkHandle)
	if ah.callAdmittedWorkDoneOnKVAdmissionQ {
		cpuTime := grunning.Time() - ah.cpuStart
		if cpuTime < 0 {
			// We sometimes see cpuTime to be negative. We use 1ns here, arbitrarily.
			// This issue is tracked by
			// https://github.com/cockroachdb/cockroach/issues/126681.
			if buildutil.CrdbTestBuild {
				log.KvDistribution.Warningf(context.Background(), "grunning.Time() should be non-decreasing, cpuTime=%s", cpuTime)
			}
			cpuTime = 1
		}
		n.kvAdmissionQ.AdmittedWorkDone(ah.tenantID, cpuTime)
	}
	if ah.storeAdmissionQ != nil {
		var doneInfo admission.StoreWorkDoneInfo
		if writeBytes != nil {
			doneInfo = admission.StoreWorkDoneInfo(*writeBytes)
		}
		err := ah.storeAdmissionQ.AdmittedWorkDone(ah.storeWorkHandle, doneInfo)
		if err != nil {
			// This shouldn't be happening.
			if buildutil.CrdbTestBuild {
				log.KvDistribution.Fatalf(context.Background(), "%s", errors.WithAssertionFailure(err))
			}
			if n.every.ShouldLog() {
				log.KvDistribution.Errorf(context.Background(), "%s", err)
			}
		}
	}
}

// AdmitRangefeedRequest implements the Controller interface.
func (n *controllerImpl) AdmitRangefeedRequest(
	tenantID roachpb.TenantID, request *kvpb.RangeFeedRequest,
) *admission.Pacer {
	if !rangefeedCatchupScanElasticControlEnabled.Get(&n.settings.SV) {
		return nil
	}

	return n.elasticCPUGrantCoordinator.NewPacer(
		elasticCPUDurationPerRangefeedScanUnit.Get(&n.settings.SV),
		admission.WorkInfo{
			TenantID:        tenantID,
			Priority:        admissionpb.WorkPriority(request.AdmissionHeader.Priority),
			CreateTime:      request.AdmissionHeader.CreateTime,
			BypassAdmission: false,
		})
}

// SetTenantWeightProvider implements the Controller interface.
func (n *controllerImpl) SetTenantWeightProvider(
	provider TenantWeightProvider, stopper *stop.Stopper,
) {
	// TODO(irfansharif): Use a stopper here instead.
	go func() {
		const weightCalculationPeriod = 10 * time.Minute
		ticker := time.NewTicker(weightCalculationPeriod)
		// Used for short-circuiting the weights calculation if all weights are
		// disabled.
		allWeightsDisabled := false
		for {
			select {
			case <-ticker.C:
				kvDisabled := !admission.KVTenantWeightsEnabled.Get(&n.settings.SV)
				kvStoresDisabled := !admission.KVStoresTenantWeightsEnabled.Get(&n.settings.SV)
				if allWeightsDisabled && kvDisabled && kvStoresDisabled {
					// Have already transitioned to disabled, so noop.
					continue
				}
				weights := provider.GetTenantWeights()
				if kvDisabled {
					weights.Node = nil
				}
				n.kvAdmissionQ.SetTenantWeights(weights.Node)
				n.elasticCPUGrantCoordinator.ElasticCPUWorkQueue.SetTenantWeights(weights.Node)

				for _, storeWeights := range weights.Stores {
					q := n.storeGrantCoords.TryGetQueueForStore(storeWeights.StoreID)
					if q != nil {
						if kvStoresDisabled {
							storeWeights.Weights = nil
						}
						q.SetTenantWeights(storeWeights.Weights)
					}
				}
				allWeightsDisabled = kvDisabled && kvStoresDisabled
			case <-stopper.ShouldQuiesce():
				ticker.Stop()
				return
			}
		}
	}()
}

// SnapshotIngestedOrWritten implements the Controller interface.
func (n *controllerImpl) SnapshotIngestedOrWritten(
	storeID roachpb.StoreID, ingestStats pebble.IngestOperationStats, writeBytes uint64,
) {
	storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(storeID)
	if storeAdmissionQ == nil {
		return
	}
	storeAdmissionQ.StatsToIgnore(ingestStats, writeBytes)
}

// FollowerStoreWriteBytes implements the Controller interface.
func (n *controllerImpl) FollowerStoreWriteBytes(
	storeID roachpb.StoreID, followerWriteBytes FollowerStoreWriteBytes,
) {
	if followerWriteBytes.WriteBytes == 0 && followerWriteBytes.IngestedBytes == 0 {
		return
	}
	storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(storeID)
	if storeAdmissionQ == nil {
		return
	}
	storeAdmissionQ.BypassedWorkDone(
		followerWriteBytes.NumEntries, followerWriteBytes.StoreWorkDoneInfo)
}

var _ replica_rac2.ACWorkQueue = &controllerImpl{}

// Admit implements replica_rac2.ACWorkQueue. It is only used for the RACv2 protocol.
func (n *controllerImpl) Admit(ctx context.Context, entry replica_rac2.EntryForAdmission) bool {
	storeAdmissionQ := n.storeGrantCoords.TryGetQueueForStore(entry.StoreID)
	if storeAdmissionQ == nil {
		log.KvDistribution.Errorf(ctx, "unable to find queue for store: %s", entry.StoreID)
		return false // nothing to do
	}

	if entry.RequestedCount == 0 {
		log.KvDistribution.Fatal(ctx, "found (unexpected) empty raft command for below-raft admission")
	}
	wi := admission.WorkInfo{
		TenantID:        entry.TenantID,
		Priority:        entry.Priority,
		CreateTime:      entry.CreateTime,
		BypassAdmission: false,
		RequestedCount:  entry.RequestedCount,
	}
	wi.ReplicatedWorkInfo = admission.ReplicatedWorkInfo{
		Enabled:    true,
		RangeID:    entry.RangeID,
		ReplicaID:  entry.ReplicaID,
		LeaderTerm: entry.CallbackState.Mark.Term,
		LogPosition: admission.LogPosition{
			Term:  0, // Ignored by callback in RACv2.
			Index: entry.CallbackState.Mark.Index,
		},
		RaftPri:  entry.CallbackState.Priority,
		Ingested: entry.Ingested,
	}

	handle, err := storeAdmissionQ.Admit(ctx, admission.StoreWriteWorkInfo{
		WorkInfo: wi,
	})
	if err != nil {
		log.KvDistribution.Errorf(ctx, "error while admitting to store admission queue: %v", err)
		return false
	}
	if handle.UseAdmittedWorkDone() {
		log.KvDistribution.Fatalf(ctx, "unexpected handle.UseAdmittedWorkDone")
	}
	return true
}

func (n *controllerImpl) GetSnapshotQueue(storeID roachpb.StoreID) *admission.SnapshotQueue {
	sq := n.storeGrantCoords.TryGetSnapshotQueueForStore(storeID)
	if sq == nil {
		return nil
	}
	return sq.(*admission.SnapshotQueue)
}

// FollowerStoreWriteBytes captures stats about writes done to a store by a
// replica that is not the leaseholder. These are used for admission control.
type FollowerStoreWriteBytes struct {
	NumEntries int64
	admission.StoreWorkDoneInfo
}

// Merge follower store write statistics using the given data.
func (f *FollowerStoreWriteBytes) Merge(from FollowerStoreWriteBytes) {
	f.NumEntries += from.NumEntries
	f.WriteBytes += from.WriteBytes
	f.IngestedBytes += from.IngestedBytes
}

// StoreWriteBytes aliases admission.StoreWorkDoneInfo, since the notion of
// "work is done" is specific to admission control and doesn't need to leak
// everywhere.
type StoreWriteBytes admission.StoreWorkDoneInfo

var storeWriteBytesPool = sync.Pool{
	New: func() interface{} { return &StoreWriteBytes{} },
}

// NewStoreWriteBytes constructs a new StoreWriteBytes.
func NewStoreWriteBytes() *StoreWriteBytes {
	wb := storeWriteBytesPool.Get().(*StoreWriteBytes)
	*wb = StoreWriteBytes{}
	return wb
}

// Release returns the *StoreWriteBytes to the pool.
func (wb *StoreWriteBytes) Release() {
	if wb == nil {
		return
	}
	storeWriteBytesPool.Put(wb)
}

func workInfoForBatch(
	st *cluster.Settings,
	requestTenantID roachpb.TenantID,
	rangeTenantID roachpb.TenantID,
	ba *kvpb.BatchRequest,
) admission.WorkInfo {
	// 默认按请求发起方的 tenant 计费/限流。
	// 对 system tenant 的请求，某些场景下会改用 range 所属 tenant，
	// 这样系统内部工作也能更贴近“谁在消耗这份资源”的语义。
	bypassAdmission := ba.IsAdmin()
	// admission source 记录这批请求来自哪里：
	// - SQL gateway
	// - 其它来源（比如内部系统请求）
	// 这个字段会影响后续是否允许 bypass admission。
	source := ba.AdmissionHeader.Source
	tenantID := requestTenantID
	if requestTenantID.IsSystem() {
		// system tenant 的请求比较特殊：
		// 如果开启了设置，并且这不是一个显式 bypass 的 admin 请求，
		// 同时 rangeTenantID 也有效，那么把计费 tenant 切到 range 所属 tenant。
		if useRangeTenantIDForNonAdminEnabled.Get(&st.SV) && !bypassAdmission &&
			rangeTenantID.IsSet() {
			tenantID = rangeTenantID
		}
		// Else, either it is an admin request (common), or rangeTenantID is not
		// set (rare), or the cluster setting is disabled, so continue using the
		// SystemTenantID.
	} else {
		// Request is from a SQL node.
		// 非 system tenant 的请求，说明它来自 SQL 层，不允许再把它当成
		// admin 请求来绕过 admission。
		bypassAdmission = false
		source = kvpb.AdmissionHeader_FROM_SQL
	}
	// AdmissionHeader.Source 为 OTHER 时，表示这是明确不该进入 admission 的请求。
	if source == kvpb.AdmissionHeader_OTHER {
		bypassAdmission = true
	}
	// TODO(abaptist): Revisit and deprecate this setting in v23.1.
	// 旧的 bulk-only admission 模式下，普通优先级及以上请求直接绕过 admission。
	if admission.KVBulkOnlyAdmissionControlEnabled.Get(&st.SV) {
		if admissionpb.WorkPriority(ba.AdmissionHeader.Priority) >= admissionpb.NormalPri {
			bypassAdmission = true
		}
	}
	// LeaseInfo requests are used as makeshift replica health probes by
	// DistSender circuit breakers, make sure they bypass AC.
	//
	// TODO(erikgrinaker): the various bypass conditions here should be moved to
	// one or more request flags.
	// LeaseInfo 请求常被 DistSender 用作副本健康探测，不应该因为 admission
	// 而被延迟，否则连“探活”本身都可能超时。
	if ba.IsSingleLeaseInfoRequest() {
		bypassAdmission = true
	}
	createTime := ba.AdmissionHeader.CreateTime
	if !bypassAdmission && createTime == 0 {
		// TODO(sumeer): revisit this for multi-tenant. Specifically, the SQL use
		// of zero CreateTime needs to be revisited. It should use high priority.
		// 如果没有显式的创建时间，并且又不是 bypass 请求，就在这里补一个当前时间。
		// 这样 admission 控制器可以按请求“年龄”做排序或公平性判断。
		createTime = timeutil.Now().UnixNano()
	}
	// 把 batch header 中的 admission 信息统一整理成 WorkInfo。
	// 这个结构会被后面的 admission / flow-control / CPU queue 复用。
	admissionInfo := admission.WorkInfo{
		TenantID:        tenantID,
		Priority:        admissionpb.WorkPriority(ba.AdmissionHeader.Priority),
		CreateTime:      createTime,
		BypassAdmission: bypassAdmission,
	}
	return admissionInfo
}
