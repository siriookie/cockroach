// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigmanager

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// checkReconciliationJobInterval is a cluster setting to control how often we
// check if the span config reconciliation job exists. If it's not found, it
// will be started. It has no effect unless
// spanconfig.reconciliation_job.enabled is configured.
var checkReconciliationJobInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"spanconfig.reconciliation_job.check_interval",
	"the frequency at which to check if the span config reconciliation job exists (and to start it if not)",
	10*time.Minute,
)

// jobEnabledSetting gates the activation of the span config reconciliation job.
//
// TODO(irfansharif): This should be a tenant read-only setting once the work
// for #73349 is completed.
var JobEnabledSetting = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"spanconfig.reconciliation_job.enabled",
	"enable the use of the kv accessor", true)

// Manager is the coordinator of the span config subsystem. It ensures that
// there's only one span config reconciliation job[1] for every tenant. It also
// captures all relevant dependencies for the job.
//
// [1]: The reconciliation job is responsible for reconciling a tenant's zone
//
//	configurations with the clusters span configurations.
type Manager struct {
	db       isql.DB
	jr       *jobs.Registry
	stopper  *stop.Stopper
	settings *cluster.Settings
	knobs    *spanconfig.TestingKnobs

	spanconfig.Reconciler
}

// New constructs a new Manager.
func New(
	idb isql.DB,
	jr *jobs.Registry,
	stopper *stop.Stopper,
	settings *cluster.Settings,
	reconciler spanconfig.Reconciler,
	knobs *spanconfig.TestingKnobs,
) *Manager {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &Manager{
		db:         idb,
		jr:         jr,
		stopper:    stopper,
		settings:   settings,
		Reconciler: reconciler,
		knobs:      knobs,
	}
}

// Start 创建一个后台任务，启动自动跨度配置（auto span config）对账作业。
// 它还会定期确保该作业存在，如果不存在则重新创建。
func (m *Manager) Start(ctx context.Context) error {
	return m.stopper.RunAsyncTask(ctx, "span-config-mgr", func(ctx context.Context) {
		m.run(ctx)
	})
}

func (m *Manager) run(ctx context.Context) {
	jobCheckCh := make(chan struct{}, 1)
	triggerJobCheck := func() {
		select {
		case jobCheckCh <- struct{}{}:
		default:
		}
	}

	// We have a few conditions that should trigger a job check:
	// - when the setting to enable/disable the reconciliation job is toggled;
	// - when the setting controlling the reconciliation job check interval is
	//   changed;
	// - when the cluster version is changed; if we don't it's possible to have
	//   started a tenant pod with a conservative view of the cluster version,
	//   skip starting the reconciliation job, learning about the cluster
	//   version shortly, and only checking the job after an interval has
	//   passed.
	JobEnabledSetting.SetOnChange(&m.settings.SV, func(ctx context.Context) {
		triggerJobCheck()
	})
	checkReconciliationJobInterval.SetOnChange(&m.settings.SV, func(ctx context.Context) {
		triggerJobCheck()
	})
	m.settings.Version.SetOnChange(func(_ context.Context, _ clusterversion.ClusterVersion) {
		triggerJobCheck()
	})

	checkJob := func() {
		if fn := m.knobs.ManagerCheckJobInterceptor; fn != nil {
			fn()
		}

		if !JobEnabledSetting.Get(&m.settings.SV) {
			return
		}

		started, err := m.createAndStartJobIfNoneExists(ctx, m.settings)
		if err != nil {
			log.Dev.Errorf(ctx, "error starting auto span config reconciliation job: %v", err)
		}
		if started {
			log.Dev.Infof(ctx, "started auto span config reconciliation job")
		}
	}

	// Periodically check if the span config reconciliation job exists and start
	// it if it doesn't.
	var timer timeutil.Timer
	defer timer.Stop()

	triggerJobCheck()
	for {
		timer.Reset(checkReconciliationJobInterval.Get(&m.settings.SV))
		select {
		case <-jobCheckCh:
			checkJob()
		case <-timer.C:
			checkJob()
		case <-m.stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		}
	}
}

// createAndStartJobIfNoneExists creates span config reconciliation job iff it
// hasn't been created already and notifies the jobs registry to adopt it.
// Returns a boolean indicating if the job was created.
func (m *Manager) createAndStartJobIfNoneExists(
	ctx context.Context, cs *cluster.Settings,
) (bool, error) {
	// 测试/knob 开关：如果禁用了 job 创建，直接返回“没有创建”。
	if m.knobs.ManagerDisableJobCreation {
		return false, nil
	}
	// 构造 auto span config reconciliation job 的 job record。
	// 这个 record 描述了要写入 system.jobs 的 job 元信息。
	record := jobs.Record{
		// 为新 job 分配一个全局唯一的 job ID。
		JobID: m.jr.MakeJobID(),
		// job 在 system.jobs 中显示的描述。
		Description: "reconciling span configurations",
		// 这个系统 job 以 node 用户身份运行。
		Username: username.NodeUserName(),
		// job details 标记这是 AutoSpanConfigReconciliation 类型 job。
		Details: jobspb.AutoSpanConfigReconciliationDetails{},
		// 初始化 job progress。
		Progress: jobspb.AutoSpanConfigReconciliationProgress{},
		// span config reconciliation 是系统维护 job，不允许用户取消。
		NonCancelable: true,
	}

	// 保存事务中创建出来的 job。
	// 如果事务里发现已有 running job，这里会保持 nil。
	var job *jobs.Job
	// 在一个内部 SQL transaction 中检查并创建 job，避免并发 manager 同时创建多个 job。
	if err := m.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// 检查 system.jobs 中是否已经存在 running 的 auto span config reconciliation job。
		exists, err := jobs.RunningJobExists(
			ctx, cs, jobspb.InvalidJobID, txn, jobspb.TypeAutoSpanConfigReconciliation,
		)
		// 查询失败时返回错误，外层会把错误交给调用者。
		if err != nil {
			return err
		}

		// 测试 hook：允许测试在“是否已有 running job”检查后观察/阻塞。
		if fn := m.knobs.ManagerAfterCheckedReconciliationJobExistsInterceptor; fn != nil {
			fn(exists)
		}

		// 如果已经存在 running job，就不再创建新的 reconciliation job。
		if exists {
			// Nothing to do here.
			// 显式保持 job 为 nil，表示本轮没有创建 job。
			job = nil
			return nil
		}
		// 不存在 running job 时，在当前事务内创建新的 job 记录。
		// CreateJobWithTxn 会把 job record 写入 system.jobs 以及相关 job metadata。
		job, err = m.jr.CreateJobWithTxn(ctx, record, record.JobID, txn)
		// 返回创建错误；如果 nil，事务会继续提交。
		return err
	}); err != nil {
		// 事务失败，返回“没有创建”以及具体错误。
		return false, err
	}

	// job 为 nil 表示事务中发现已有 running job，所以无需通知 registry resume。
	if job == nil {
		return false, nil
	}

	// 测试 hook：允许测试观察刚创建出来的 job。
	if fn := m.knobs.ManagerCreatedJobInterceptor; fn != nil {
		fn(job)
	}
	// 通知 jobs registry 尝试 resume/adopt 这个刚创建的 job。
	// 注意这里在事务提交之后执行，避免 registry 看到未提交的 job。
	m.jr.NotifyToResume(ctx, job.ID())
	// 返回 true，表示本轮确实创建并通知了一个新 job。
	return true, nil
}
