// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package policyrefresher

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// PolicyRefresher periodically refreshes the closed timestamp policies for
// ranges that have leaseholders on the node on which it is running or when
// there are on-demand refresh requests from the replica.
//
// When LeadForGlobalReadsAutoTuneEnabled is enabled, ranges configured with
// global reads consider network latencies between the leaseholder and its
// furthest follower when calculating closed timestamps.
type PolicyRefresher struct {
	stopper  *stop.Stopper
	settings *cluster.Settings
	knobs    *TestingKnobs

	// getLeaseholderReplicas returns the set of replicas that are currently
	// leaseholders of the node.
	getLeaseholderReplicas func() []Replica

	// refreshNotificationCh is used to signal when replicas need their policies
	// refreshed outside the normal refresh interval. They are added on demand
	// when there is a leaseholder change or when there is a span config change.
	refreshNotificationCh chan struct{}

	// getNodeLatencies returns a map of node IDs to their observed round-trip
	// latencies from the current node. Replicas use this information to determine
	// appropriate closed timestamp policies. See rpc.RemoteClockMonitor for more
	// details.
	getNodeLatencies func() map[roachpb.NodeID]time.Duration

	// latencyCache caches the latency information from getNodeLatencies. This
	// can be expensive, so we only do this periodically based on the configured
	// refresh interval.
	mu struct {
		syncutil.RWMutex
		latencyCache map[roachpb.NodeID]time.Duration
	}

	// rMu protects access to the list of replicas needing policy refresh.
	rMu struct {
		syncutil.Mutex
		pendingReplicas []Replica
	}
}

func NewPolicyRefresher(
	stopper *stop.Stopper,
	settings *cluster.Settings,
	getLeaseholderReplicas func() []Replica,
	getNodeLatencies func() map[roachpb.NodeID]time.Duration,
	knobs *TestingKnobs,
) *PolicyRefresher {
	if getLeaseholderReplicas == nil || getNodeLatencies == nil {
		log.KvExec.Fatalf(context.Background(), "getLeaseholderReplicas and getNodeLatencies must be non-nil")
		return nil
	}
	refresher := &PolicyRefresher{
		stopper:                stopper,
		settings:               settings,
		knobs:                  knobs,
		getLeaseholderReplicas: getLeaseholderReplicas,
		getNodeLatencies:       getNodeLatencies,
		refreshNotificationCh:  make(chan struct{}, 1),
	}
	return refresher
}

// Replica defines a thin interface to update closed timestamp policies.
type Replica interface {
	// RefreshPolicy informs the replica that it should refresh its closed
	// timestamp policy. A latency map, which includes round-trip observed latency
	// to other nodes in the system by the PolicyRefresher may be supplied (or may
	// be nil), in which case it may be used to correctly place a replica in its
	// latency based global reads bucket.
	RefreshPolicy(map[roachpb.NodeID]time.Duration)
}

// detachReplicas atomically retrieves and clears the list of replicas needing
// policy refresh.
func (pr *PolicyRefresher) detachReplicas() []Replica {
	pr.rMu.Lock()
	defer pr.rMu.Unlock()
	toRefresh := pr.rMu.pendingReplicas
	pr.rMu.pendingReplicas = nil
	return toRefresh
}

// EnqueueReplicaForRefresh adds a replica to the list of those needing policy
// refresh and signals the refresh goroutine.
func (pr *PolicyRefresher) EnqueueReplicaForRefresh(replica Replica) {
	if pr == nil {
		return
	}
	pr.rMu.Lock()
	defer pr.rMu.Unlock()
	pr.rMu.pendingReplicas = append(pr.rMu.pendingReplicas, replica)
	// Note that refreshNotificationCh is non-blocking.
	select {
	case pr.refreshNotificationCh <- struct{}{}:
	default:
	}
}

// updateLatencyCache refreshes the cached round-trip latency information by
// fetching fresh measurements from the actual RPC context. This can be
// expensive, so we only do this periodically based on the configured refresh
// interval.
func (pr *PolicyRefresher) updateLatencyCache() {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.mu.latencyCache = pr.getNodeLatencies()
}

// getCurrentLatencies returns the current round-trip latency information if
// auto-tuning is enabled and the cluster has been fully upgraded to v25.2, or
// nil otherwise.
func (pr *PolicyRefresher) getCurrentLatencies() map[roachpb.NodeID]time.Duration {
	// Testing knobs only.
	if pr.knobs != nil && pr.knobs.InjectedLatencies != nil {
		return pr.knobs.InjectedLatencies()
	}
	if !closedts.LeadForGlobalReadsAutoTuneEnabled.Get(&pr.settings.SV) || !pr.settings.Version.IsActive(context.TODO(), clusterversion.V25_2) {
		return nil
	}
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	return pr.mu.latencyCache
}

// refreshPolicies updates the closed timestamp policy for the given
// leaseholders based on current latency information. This can be called from
// the store or from pr.Run.
func (pr *PolicyRefresher) refreshPolicies(leaseholders []Replica) {
	latencies := pr.getCurrentLatencies()
	for _, leaseholder := range leaseholders {
		leaseholder.RefreshPolicy(latencies)
	}
}

// Run launches pr.run in the background if no error is returned. pr.run
// continues running until the context is done or the stopper is quiesced.
func (pr *PolicyRefresher) Run(ctx context.Context) {
	_ /* err */ = pr.stopper.RunAsyncTask(ctx, "closed timestamp policy refresher",
		func(ctx context.Context) {
			pr.run(ctx)
		})
}

// run periodically refreshes the closed timestamp policies for the leaseholder
// replicas based on the current latency information. It also handles on-demand
// refresh requests from EnqueueReplicaForRefresh.
func (pr *PolicyRefresher) run(ctx context.Context) {
	// configUpdateCh 用来接收 policy refresh interval 配置变化通知，容量为 1 表示合并重复通知。
	configUpdateCh := make(chan struct{}, 1)
	// Note that the config channel doesn't subscribe to cluster version
	// changes. We rely on the relatively short
	// RangeClosedTimestampPolicyRefreshInterval to ensure timely updates when
	// cluster version changes occur.
	// onConfigChange 是 closed timestamp policy 刷新周期的 setting 变更回调。
	onConfigChange := func(ctx context.Context) {
		// 非阻塞发送，避免 setting 回调被后台刷新 goroutine 卡住。
		select {
		case configUpdateCh <- struct{}{}:
		default:
		}
	}
	// 注册刷新周期 setting 的变更回调；配置变化后会重置 policyRefresherTimer。
	closedts.RangeClosedTimestampPolicyRefreshInterval.SetOnChange(&pr.settings.SV, onConfigChange)

	// latencyRefresherConfigUpdateCh 用来接收“节点延迟缓存刷新周期”的配置变化通知。
	latencyRefresherConfigUpdateCh := make(chan struct{}, 1)
	// onLatencyRefresherConfigChange 是 latency refresh interval 的 setting 变更回调。
	onLatencyRefresherConfigChange := func(ctx context.Context) {
		// 非阻塞发送，多个配置变化可以合并成一次 timer 重置。
		select {
		case latencyRefresherConfigUpdateCh <- struct{}{}:
		default:
		}
	}
	// 注册延迟缓存刷新周期 setting 的变更回调。
	closedts.RangeClosedTimestampPolicyLatencyRefreshInterval.SetOnChange(&pr.settings.SV, onLatencyRefresherConfigChange)

	// getPolicyRefresh 读取 range closed timestamp policy 的周期性刷新间隔。
	getPolicyRefresh := func() time.Duration {
		return closedts.RangeClosedTimestampPolicyRefreshInterval.Get(&pr.settings.SV)
	}
	// getLatencyRefresh 读取节点延迟缓存的周期性刷新间隔。
	getLatencyRefresh := func() time.Duration {
		return closedts.RangeClosedTimestampPolicyLatencyRefreshInterval.Get(&pr.settings.SV)
	}

	// policyRefresherTimer 控制“给本节点所有 leaseholder replicas 刷新 policy”的周期。
	var policyRefresherTimer timeutil.Timer
	// 启动第一次 policy 刷新定时。
	policyRefresherTimer.Reset(getPolicyRefresh())
	// run 退出时停止 timer。
	defer policyRefresherTimer.Stop()

	// latencyRefresherTimer 控制“刷新节点 RTT 延迟缓存”的周期。
	var latencyRefresherTimer timeutil.Timer
	// 启动第一次 latency cache 刷新定时。
	latencyRefresherTimer.Reset(getLatencyRefresh())
	// run 退出时停止 timer。
	defer latencyRefresherTimer.Stop()

	// 后台循环持续处理即时刷新、周期刷新、配置变更和退出信号。
	for {
		// 等待任意一个事件发生。
		select {
		// Refresh the policy on-demand.
		// 某些 replica 因为 leaseholder 变化或 span config 变化主动请求刷新 policy。
		case <-pr.refreshNotificationCh:
			// 取出并清空待刷新 replicas 列表，然后只刷新这些 replicas。
			pr.refreshPolicies(pr.detachReplicas())

		// Refresh the policy for ranges with leaseholders on the node.
		// 周期到期：刷新本节点当前所有 leaseholder replicas 的 closed timestamp policy。
		case <-policyRefresherTimer.C:
			// 重置下一次 policy 周期刷新时间。
			policyRefresherTimer.Reset(getPolicyRefresh())
			// 获取本节点当前 leaseholder replicas，并基于当前延迟信息刷新它们的 policy。
			pr.refreshPolicies(pr.getLeaseholderReplicas())
		case <-configUpdateCh:
			// policy 刷新周期配置变化：按最新配置重置 timer。
			policyRefresherTimer.Reset(getPolicyRefresh())

		// Refresh the latency cache.
		// latency cache 周期到期：刷新本节点观察到的到其它节点的 RTT 延迟缓存。
		case <-latencyRefresherTimer.C:
			// 重置下一次 latency cache 刷新时间。
			latencyRefresherTimer.Reset(getLatencyRefresh())
			// 从 RPC clock monitor 等来源拉取最新节点延迟，并写入本地缓存。
			pr.updateLatencyCache()
		case <-latencyRefresherConfigUpdateCh:
			// latency 刷新周期配置变化：按最新配置重置 timer。
			latencyRefresherTimer.Reset(getLatencyRefresh())

		// Stop the for loop.
		// stopper 进入 quiesce，说明 server 正在关闭，退出后台循环。
		case <-pr.stopper.ShouldQuiesce():
			return
		// 外层 context 被取消，也退出后台循环。
		case <-ctx.Done():
			return
		}
	}
}
