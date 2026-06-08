// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

// CreatedByScheduledJobs identifies the job that was created
// by scheduled jobs system.
const CreatedByScheduledJobs = "crdb_schedule"

// jobScheduler is responsible for finding and starting scheduled
// jobs that need to be executed.
type jobScheduler struct {
	*scheduledjobs.JobExecutionConfig
	env      scheduledjobs.JobSchedulerEnv
	registry *metric.Registry
	metrics  SchedulerMetrics
}

func newJobScheduler(
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	registry *metric.Registry,
) *jobScheduler {
	if env == nil {
		env = scheduledjobs.ProdJobSchedulerEnv
	}

	stats := MakeSchedulerMetrics()
	registry.AddMetricStruct(stats)

	return &jobScheduler{
		JobExecutionConfig: cfg,
		env:                env,
		registry:           registry,
		metrics:            stats,
	}
}

const allSchedules = 0

var errScheduleNotRunnable = errors.New("schedule not runnable")

// loadCandidateScheduleForExecution looks up and locks candidate schedule for execution.
// The schedule is locked via FOR UPDATE clause to ensure that only this scheduler can modify it.
// If schedule cannot execute, a errScheduleNotRunnable error is returned.
func (s scheduledJobStorageTxn) loadCandidateScheduleForExecution(
	// ctx 控制本次 schedule 读取 SQL 的生命周期。
	ctx context.Context, scheduleID int64, env scheduledjobs.JobSchedulerEnv,
) (*ScheduledJob, error) {
	// 构造查询：只加载 next_run 已到期的 schedule，并用 FOR UPDATE 锁住这一行。
	lookupStmt := fmt.Sprintf(
		"SELECT * FROM %s WHERE schedule_id=%d AND next_run < %s FOR UPDATE",
		env.ScheduledJobsTableName(), scheduleID, env.NowExpr())
	// 在当前事务里查询 schedule 行；FOR UPDATE 保证并发 scheduler 中只有一个能修改它。
	row, cols, err := s.txn.QueryRowExWithCols(
		ctx, "find-scheduled-jobs-exec",
		s.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		lookupStmt)
	// 查询失败，返回错误。
	if err != nil {
		return nil, err
	}

	// row 为空说明这个 schedule 已经不满足执行条件，或者被其他事务处理后 next_run 改了。
	if row == nil {
		return nil, errScheduleNotRunnable
	}

	// 创建 ScheduledJob 对象，并从 SQL row 反序列化 schedule 内容。
	j := NewScheduledJob(env)
	// 把 datums/cols 转成 ScheduledJob 内部字段。
	if err := j.InitFromDatums(row, cols); err != nil {
		return nil, err
	}
	// 返回已锁定且可执行的 schedule。
	return j, nil
}

// lookupNumRunningJobs returns the number of running jobs for the specified schedule.
func lookupNumRunningJobs(
	// ctx 控制本次查询生命周期。
	ctx context.Context,
	// scheduleID 是当前 schedule 的 ID。
	scheduleID jobspb.ScheduleID,
	// env 提供 system.jobs 表名等环境信息。
	env scheduledjobs.JobSchedulerEnv,
	// txn 是当前 scheduler 事务。
	txn isql.Txn,
) (int64, error) {
	// 查询当前 schedule 已经创建且仍处于非终止状态的 jobs 数量。
	lookupStmt := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE created_by_type = '%s' AND created_by_id = %d AND status IN %s",
		env.SystemJobsTableName(), CreatedByScheduledJobs, scheduleID, NonTerminalStateTupleString)
	// 在事务中执行 count 查询。
	row, err := txn.QueryRowEx(
		ctx, "lookup-num-running",
		txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		lookupStmt)
	// 查询失败，返回错误。
	if err != nil {
		return 0, err
	}
	// count(*) 返回 DInt，转换成 int64。
	return int64(tree.MustBeDInt(row[0])), nil
}

const recheckRunningAfter = 1 * time.Minute

func (s *jobScheduler) processSchedule(
	// ctx 是本次处理 schedule 的上下文。
	ctx context.Context, schedule *ScheduledJob, numRunning int64, txn isql.Txn,
) error {
	// 用当前事务包装 scheduled_jobs 表的读写操作。
	scheduleStorage := ScheduledJobTxn(txn)
	// 如果这个 schedule 已经有未结束的 job，需要按 schedule 的 Wait 策略处理。
	if numRunning > 0 {
		// Wait 字段决定遇到已有 running job 时是等待还是跳过本次。
		switch schedule.ScheduleDetails().Wait {
		// WAIT：不启动新 job，把 next_run 推迟一小段时间后再检查。
		case jobspb.ScheduleDetails_WAIT:
			// TODO(yevgeniy): We might need to persist more state.
			// In particular, it'd be nice to add more time when repeatedly rescheduling
			// a job.  It would also be nice not to log each event.
			// 设置下一次检查时间。
			schedule.SetNextRun(s.env.Now().Add(recheckRunningAfter))
			// 写入 schedule 状态，说明这次因为已有 running job 被 delay。
			schedule.SetScheduleStatusf("delayed due to %d already running", numRunning)
			// 记录 WAIT 重调度指标。
			s.metrics.RescheduleWait.Inc(1)
			// 保存 schedule 更新并结束本次处理。
			return scheduleStorage.Update(ctx, schedule)
		// SKIP：跳过当前触发时间，直接计算下一次正常运行时间。
		case jobspb.ScheduleDetails_SKIP:
			// 根据 recurrence 计算下一次运行时间。
			if err := schedule.ScheduleNextRun(); err != nil {
				return err
			}
			// 写入状态，说明本次因为已有 running job 被 skip。
			schedule.SetScheduleStatusf("rescheduled due to %d already running", numRunning)
			// 记录 SKIP 重调度指标。
			s.metrics.RescheduleSkip.Inc(1)
			// 保存 schedule 更新并结束本次处理。
			return scheduleStorage.Update(ctx, schedule)
		}
	}

	// 没有 running job 阻挡，清掉之前可能留下的状态信息。
	schedule.ClearScheduleStatus()

	// Schedule the next job run.
	// We do this step early, before the actual execution, to grab a lock on
	// the scheduledjobs table.
	// 周期性 schedule：先计算下一次 next_run，避免当前 job 创建成功后重复触发同一时间点。
	if schedule.HasRecurringSchedule() {
		if err := schedule.ScheduleNextRun(); err != nil {
			return err
		}
	} else {
		// It's a one-off schedule.  Clear next run to indicate that this schedule executed.
		// 一次性 schedule：清空 next_run，表示已经执行过。
		schedule.SetNextRun(time.Time{})
	}

	// 先把 next_run 更新写回 system.scheduled_jobs。
	if err := scheduleStorage.Update(ctx, schedule); err != nil {
		return err
	}

	// 根据 schedule 里的 executor type 找到对应执行器，例如 backup schedule executor。
	executor, err := GetScheduledJobExecutor(schedule.ExecutorType())
	// executor 不存在时返回错误。
	if err != nil {
		return err
	}

	// Grab job executor and execute the job.
	log.Dev.Infof(ctx,
		"Starting job for schedule %d (%q); scheduled to run at %s; next run scheduled for %s",
		schedule.ScheduleID(), schedule.ScheduleLabel(),
		schedule.ScheduledRunTime(), schedule.NextRun())

	// 给日志上下文打上 schedule id tag，方便排查。
	execCtx := logtags.AddTag(ctx, "schedule", schedule.ScheduleID())
	// 真正创建/启动 job。通常会写 system.jobs，并把 job 标记为 crdb_schedule 创建。
	if err := executor.ExecuteJob(execCtx, txn, s.JobExecutionConfig, s.env, schedule); err != nil {
		return errors.Wrapf(err, "executing schedule %d", schedule.ScheduleID())
	}

	// 记录成功启动 job 的指标。
	s.metrics.NumStarted.Inc(1)

	// Persist any mutations to the underlying schedule.
	// executor 可能修改了 schedule 元数据，这里再持久化一次。
	return scheduleStorage.Update(ctx, schedule)
}

type savePointError struct {
	err error
}

func (e savePointError) Error() string {
	return e.err.Error()
}

// withSavePoint executes function fn() wrapped with savepoint.
// The savepoint is either released (upon successful completion of fn())
// or it is rolled back.
// If an error occurs while performing savepoint operations, an instance
// of savePointError is returned.  If fn() returns an error, then that
// error is returned.
func withSavePoint(ctx context.Context, txn *kv.Txn, fn func() error) error {
	// 在当前事务里创建 savepoint，方便局部回滚 schedule 处理错误。
	sp, err := txn.CreateSavepoint(ctx)
	// savepoint 创建失败，包装成 savePointError，外层会放弃本轮事务。
	if err != nil {
		return &savePointError{err}
	}
	// 执行调用方传入的 schedule 处理逻辑。
	execErr := fn()

	// 处理成功时释放 savepoint。
	if execErr == nil {
		if err := txn.ReleaseSavepoint(ctx, sp); err != nil {
			return &savePointError{err}
		}
		return nil
	}

	// 事务重试错误不能简单 rollback 到 savepoint，必须让外层事务重试。
	if errors.HasType(execErr, (*kvpb.TransactionRetryWithProtoRefreshError)(nil)) {
		// If function execution failed because transaction was restarted,
		// treat this error as a savePointError so that the execution code bails out
		// and retries scheduling loop.
		return &savePointError{execErr}
	}

	// 普通执行错误：回滚到 savepoint，撤销 fn 对当前事务做的局部修改。
	if err := txn.RollbackToSavepoint(ctx, sp); err != nil {
		return &savePointError{errors.WithDetail(err, execErr.Error())}
	}
	// 返回 fn 自己的错误，外层可以记录 schedule 失败状态。
	return execErr
}

// executeCandidateSchedule attempts to execute schedule.
// The schedule is executed only if it's running.
func (s *jobScheduler) executeCandidateSchedule(
	// ctx 是本次 candidate 执行上下文。
	ctx context.Context, candidate int64, txn isql.Txn,
) error {
	// 把当前 txn 包装成 scheduled job storage。
	sj := scheduledJobStorageTxn{txn}
	// 重新加载 candidate schedule，并用 FOR UPDATE 锁住它。
	schedule, err := sj.loadCandidateScheduleForExecution(
		ctx, candidate, s.env,
	)
	// 加载失败时区分“不可运行”和真正错误。
	if err != nil {
		// 不可运行通常说明它已经被其他 scheduler 处理，或者 next_run 不再到期。
		if errors.Is(err, errScheduleNotRunnable) {
			return nil
		}
		// 解析/读取失败，计入 malformed schedule。
		s.metrics.NumMalformedSchedules.Inc(1)
		log.Dev.Errorf(ctx, "error parsing schedule %d: %s", candidate, err)
		return err
	}

	// 如果这个 executor 类型被环境禁用，就忽略这个 schedule。
	if !s.env.IsExecutorEnabled(schedule.ExecutorType()) {
		log.Dev.Infof(ctx, "Ignoring schedule %d: %s executor disabled",
			schedule.ScheduleID(), schedule.ExecutorType())
		return nil
	}

	// 查询这个 schedule 当前已有多少个未完成 jobs。
	numRunning, err := lookupNumRunningJobs(
		ctx, schedule.ScheduleID(), s.env, txn,
	)
	// 查询 running jobs 失败，返回错误。
	if err != nil {
		return err
	}

	// 读取单个 schedule 执行处理的超时时间。
	timeout := schedulerScheduleExecutionTimeout.Get(&s.Settings.SV)
	// 用 savepoint 包住 processSchedule，失败时能局部回滚并继续记录失败状态。
	if processErr := withSavePoint(ctx, txn.KV(), func() error {
		// timeout > 0 时，在超时控制下处理 schedule。
		if timeout > 0 {
			return timeutil.RunWithTimeout(
				ctx, redact.Sprintf("process-schedule-%d", schedule.ScheduleID()), timeout,
				func(ctx context.Context) error {
					return s.processSchedule(ctx, schedule, numRunning, txn)
				})
		}
		// timeout 为 0 表示禁用超时。
		return s.processSchedule(ctx, schedule, numRunning, txn)
	}); processErr != nil {
		// savepoint 自身错误或事务重试错误，不能继续尝试记录失败状态，直接返回。
		if errors.HasType(processErr, (*savePointError)(nil)) {
			return errors.Wrapf(processErr, "savepoint error for schedule %d", schedule.ScheduleID())
		}

		// Failed to process schedule.
		// 记录 schedule 处理失败指标和日志。
		s.metrics.NumErrSchedules.Inc(1)
		log.Dev.Errorf(ctx,
			"error processing schedule %d: %+v", schedule.ScheduleID(), processErr)

		// Try updating schedule record to indicate schedule execution error.
		// 尝试把失败状态写回 schedule；这里也用 savepoint，避免失败状态写入影响整个事务。
		if err := withSavePoint(ctx, txn.KV(), func() error {
			// Discard changes already made to the schedule, and treat schedule
			// execution failure the same way we treat job failure.
			// 丢弃本地 dirty 变更，重新按失败运行处理。
			schedule.ClearDirty()
			// 根据默认失败策略更新 schedule 状态和下一次运行时间。
			DefaultHandleFailedRun(schedule,
				"failed to create job for schedule %d: err=%s",
				schedule.ScheduleID(), processErr)

			// DefaultHandleFailedRun assumes schedule already had its next run set.
			// So, if the policy is to reschedule based on regular recurring schedule,
			// we need to set next run again..
			// 如果失败策略要求按正常 schedule 重试，需要重新计算 next_run。
			if schedule.HasRecurringSchedule() &&
				schedule.ScheduleDetails().OnError == jobspb.ScheduleDetails_RETRY_SCHED {
				if err := schedule.ScheduleNextRun(); err != nil {
					return err
				}
			}
			// 保存失败状态。
			return sj.Update(ctx, schedule)
		}); err != nil {
			// savepoint 错误需要返回给外层。
			if errors.HasType(err, (*savePointError)(nil)) {
				return errors.Wrapf(err,
					"savepoint error for schedule %d", schedule.ScheduleID())
			}
			// 失败状态写不进去时只记录日志；原始 processErr 已经被处理为本轮失败。
			log.Dev.Errorf(ctx, "error recording processing error for schedule %d: %+v",
				schedule.ScheduleID(), err)
		}
	}
	// candidate schedule 处理完成。
	return nil
}

func (s *jobScheduler) executeSchedules(ctx context.Context, maxSchedules int64) (retErr error) {
	// Lookup a set of candidate schedules to execute.
	// NB: this lookup happens under nil txn; we ensure the schedule is still runnable
	// using per-schedule exclusive (for update) query.
	// limitClause 用来限制本轮最多处理多少个 schedule。
	limitClause := ""
	// allSchedules 表示不限制数量。
	if maxSchedules != allSchedules {
		limitClause = fmt.Sprintf("LIMIT %d", maxSchedules)
	}

	// 先查出 next_run 已到期的 schedule_id，随机排序避免多个 daemon 总是抢同一批 schedule。
	findSchedulesStmt := fmt.Sprintf(
		`SELECT schedule_id FROM %s WHERE next_run < %s ORDER BY random() %s`,
		s.env.ScheduledJobsTableName(), s.env.NowExpr(), limitClause)
	// 不在事务中执行 candidate 列表查询；真正执行前会按 schedule 单独 FOR UPDATE 校验。
	it, err := s.DB.Executor().QueryIteratorEx(
		ctx, "find-scheduled-jobs",
		/*txn=*/ nil,
		sessiondata.NodeUserSessionDataOverride,
		findSchedulesStmt)

	// candidate 查询失败，整轮扫描失败。
	if err != nil {
		return err
	}

	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	// 确保 iterator 被关闭，并把关闭错误合并到返回错误里。
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	// The loop below might encounter an error after some schedules have been
	// executed (i.e. previous iterations succeeded), and this is ok.
	// 遍历本轮查到的 candidate schedules。
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		// 当前行只有 schedule_id。
		row := it.Cur()
		// 取出 candidate schedule id。
		candidateID := int64(tree.MustBeDInt(row[0]))
		// 每个 candidate schedule 单独开事务处理。
		if err := s.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			// 在事务里重新锁定并执行这个 schedule。
			return s.executeCandidateSchedule(ctx, candidateID, txn)
		}); err != nil {
			// 单个 schedule 执行失败只记录日志，不阻断其他 candidates。
			log.Dev.Errorf(ctx, "error executing candidate schedule %d: %s", candidateID, err)
		}
	}

	// 返回 iterator.Next 的最终错误。
	return err
}

type syncCancelFunc struct {
	syncutil.Mutex
	context.CancelFunc
}

// newCancelWhenDisabled arranges for scheduler enabled setting callback to cancel
// currently executing context.
func newCancelWhenDisabled(sv *settings.Values) *syncCancelFunc {
	// 保存当前正在执行的 cancel func。
	sf := &syncCancelFunc{}
	// 注册 setting change 回调；当 jobs.scheduler.enabled 变为 false 时取消当前执行。
	schedulerEnabledSetting.SetOnChange(sv, func(ctx context.Context) {
		// 只在关闭 scheduler 时触发取消。
		if !schedulerEnabledSetting.Get(sv) {
			// 加锁保护 CancelFunc 读写。
			sf.Lock()
			defer sf.Unlock()
			// 如果当前有执行中的扫描/处理上下文，就取消它。
			if sf.CancelFunc != nil {
				sf.CancelFunc()
			}
		}
	})
	// 返回可复用的取消控制对象。
	return sf
}

// withCancelOnDisabled executes provided function with the context which will be cancelled
// if scheduler is disabled.
func (sf *syncCancelFunc) withCancelOnDisabled(
	// ctx 是原始执行上下文。
	ctx context.Context, sv *settings.Values, f func(ctx context.Context) error,
) error {
	// 创建一个可被 scheduler disabled setting 取消的子上下文。
	ctx, cancel := func() (context.Context, context.CancelFunc) {
		// 加锁设置当前 CancelFunc，避免和 setting callback 并发。
		sf.Lock()
		defer sf.Unlock()

		// 创建子 context。
		ctx, cancel := context.WithCancel(ctx)
		// 保存 cancel，setting callback 可以调用它。
		sf.CancelFunc = cancel

		// 如果进入时 scheduler 已经 disabled，立刻取消。
		if !schedulerEnabledSetting.Get(sv) {
			log.Dev.Warning(ctx, "scheduled job system disabled by setting, cancelling execution")
			cancel()
		}

		// 返回包装后的 cancel；执行完成后清空 CancelFunc。
		return ctx, func() {
			// 加锁清理 CancelFunc。
			sf.Lock()
			defer sf.Unlock()
			// 取消子 context。
			cancel()
			// 清空当前 cancel，避免后续 setting callback 误取消已结束任务。
			sf.CancelFunc = nil
		}
	}()
	// 执行完 f 后一定清理 cancel。
	defer cancel()
	// 用可被 disabled setting 取消的 context 执行调用方逻辑。
	return f(ctx)
}

func (s *jobScheduler) runDaemon(ctx context.Context, stopper *stop.Stopper) {
	// 启动 job-scheduler 后台任务；这里忽略返回值，因为 stopper draining 时启动失败也只意味着不再调度。
	_ = stopper.RunAsyncTask(ctx, "job-scheduler", func(ctx context.Context) {
		// server quiesce 时取消 daemon context。
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		// daemon 退出时释放 context。
		defer cancel()

		// 第一次扫描前等待一个随机/测试指定的 delay，避免所有节点同时扫描。
		initialDelay := getInitialScanDelay(s.TestingKnobs)
		log.Dev.Infof(ctx, "waiting %v before scheduled jobs daemon start", initialDelay)

		// 注册所有 scheduled job executor 的 metrics。
		if err := RegisterExecutorsMetrics(s.registry); err != nil {
			log.Dev.Errorf(ctx, "error registering executor metrics: %+v", err)
		}

		// 创建一个 helper，用来在 scheduler 被 setting 关闭时取消当前扫描。
		whenDisabled := newCancelWhenDisabled(&s.Settings.SV)

		// 用 timer 周期性扫描 system.scheduled_jobs。
		for timer := time.NewTimer(initialDelay); ; timer.Reset(
			getWaitPeriod(ctx, &s.Settings.SV, jitter, s.TestingKnobs)) {
			// 等待 shutdown 或 timer 到期。
			select {
			// server 正在关闭，退出 daemon。
			case <-stopper.ShouldQuiesce():
				return
			// 到达扫描时间。
			case <-timer.C:
				// 如果 scheduler 被 setting 关闭，本轮不扫描。
				if !schedulerEnabledSetting.Get(&s.Settings.SV) {
					log.Dev.Warning(ctx, "scheduled job system disabled by setting")
					continue
				}

				// 读取本轮最多启动多少个 schedule。
				maxSchedules := schedulerMaxJobsPerIterationSetting.Get(&s.Settings.SV)
				// 在可被 disabled setting 取消的 context 下执行一轮扫描。
				if err := whenDisabled.withCancelOnDisabled(ctx, &s.Settings.SV, func(ctx context.Context) error {
					return s.executeSchedules(ctx, maxSchedules)
				}); err != nil {
					// 本轮扫描失败只记录日志，下一轮继续。
					log.Dev.Errorf(ctx, "error executing schedules: %v", err)
				}
			}
		}
	})
}

var schedulerEnabledSetting = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"jobs.scheduler.enabled",
	"enable/disable job scheduler",
	true,
)

var schedulerPaceSetting = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"jobs.scheduler.pace",
	"how often to scan system.scheduled_jobs table",
	time.Minute,
	settings.PositiveDuration,
)

var schedulerMaxJobsPerIterationSetting = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"jobs.scheduler.max_jobs_per_iteration",
	"how many schedules to start per iteration; setting to 0 turns off this limit",
	5,
)

var schedulerScheduleExecutionTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"jobs.scheduler.schedule_execution.timeout",
	"sets a timeout on for schedule execution; 0 disables timeout",
	30*time.Second,
)

// Returns the amount of time to wait before starting initial scan.
func getInitialScanDelay(knobs base.ModuleTestingKnobs) time.Duration {
	// 测试可以覆盖第一次扫描 delay。
	if k, ok := knobs.(*TestingKnobs); ok && k.SchedulerDaemonInitialScanDelay != nil {
		return k.SchedulerDaemonInitialScanDelay()
	}

	// By default, we'll wait between 2 and 5 minutes before performing initial scan.
	// 生产默认随机等待 2 到 4 分钟，避免集群节点同时启动时一起扫 scheduled_jobs。
	return time.Minute * time.Duration(2+rand.Intn(3))
}

// Fastest pace for the scheduler.
const minPacePeriod = 10 * time.Second

// Frequency to recheck if the daemon is enabled.
const recheckEnabledAfterPeriod = 5 * time.Minute

var warnIfPaceTooLow = log.Every(time.Minute)

type jitterFn func(duration time.Duration) time.Duration

// Returns duration to wait before scanning system.scheduled_jobs.
func getWaitPeriod(
	// ctx 用于记录 pace 太低的 warning 日志。
	ctx context.Context, sv *settings.Values, jitter jitterFn, knobs base.ModuleTestingKnobs,
) time.Duration {
	// 测试可以覆盖扫描间隔。
	if k, ok := knobs.(*TestingKnobs); ok && k.SchedulerDaemonScanDelay != nil {
		return k.SchedulerDaemonScanDelay()
	}

	// scheduler 被关闭时，不按正常 pace 扫描，只周期性醒来检查是否重新开启。
	if !schedulerEnabledSetting.Get(sv) {
		return recheckEnabledAfterPeriod
	}

	// 读取 jobs.scheduler.pace。
	pace := schedulerPaceSetting.Get(sv)
	// 防止 pace 太低导致频繁扫描 system.scheduled_jobs。
	if pace < minPacePeriod {
		if warnIfPaceTooLow.ShouldLog() {
			log.Dev.Warningf(ctx,
				"job.scheduler.pace setting too low (%s < %s)", pace, minPacePeriod)
		}
		// 强制最小扫描周期。
		pace = minPacePeriod
	}

	// 返回带 jitter 的扫描间隔，避免多个节点固定同一时刻扫描。
	return jitter(pace)
}

// StartJobSchedulerDaemon starts a daemon responsible for periodically scanning
// system.scheduled_jobs table to find and executing eligible scheduled jobs.
func StartJobSchedulerDaemon(
	// ctx 是启动 daemon 的上下文。
	ctx context.Context,
	// stopper 管理 daemon 生命周期。
	stopper *stop.Stopper,
	// registry 用来注册 scheduler metrics。
	registry *metric.Registry,
	// cfg 包含 settings、InternalDB、testing knobs、plan hook maker 等执行配置。
	cfg *scheduledjobs.JobExecutionConfig,
	// env 提供 scheduled jobs 使用的表名、时间函数和 executor 开关。
	env scheduledjobs.JobSchedulerEnv,
) {
	// Since the job scheduler system is not under user control, exclude it from
	// from cost accounting and control. Individual jobs are not part of this
	// exclusion.
	// scheduler daemon 本身不是用户 workload，免除 tenant cost control；它创建出的具体 jobs 不在这个豁免里。
	ctx = multitenant.WithTenantCostControlExemption(ctx)

	// 默认使用调用方传入的 scheduler env。
	schedulerEnv := env
	// daemonKnobs 是 jobs 测试 knobs 的具体类型。
	var daemonKnobs *TestingKnobs
	// 如果 cfg.TestingKnobs 是 jobs.TestingKnobs，就取出来做测试覆盖。
	if jobsKnobs, ok := cfg.TestingKnobs.(*TestingKnobs); ok {
		daemonKnobs = jobsKnobs
	}

	// 测试可以捕获 JobExecutionConfig，用于断言或注入行为。
	if daemonKnobs != nil && daemonKnobs.CaptureJobExecutionConfig != nil {
		daemonKnobs.CaptureJobExecutionConfig(cfg)
	}
	// 测试可以替换 scheduler env，比如用测试表名或测试时间。
	if daemonKnobs != nil && daemonKnobs.JobSchedulerEnv != nil {
		schedulerEnv = daemonKnobs.JobSchedulerEnv
	}

	// 创建 jobScheduler 对象，并注册 scheduler metrics。
	scheduler := newJobScheduler(cfg, schedulerEnv, registry)

	// 测试可以接管 schedule 执行，不真正启动 daemon loop。
	if daemonKnobs != nil && daemonKnobs.TakeOverJobsScheduling != nil {
		daemonKnobs.TakeOverJobsScheduling(
			func(ctx context.Context, maxSchedules int64) error {
				// 暴露一轮 executeSchedules 给测试调用。
				return scheduler.executeSchedules(ctx, maxSchedules)
			})
		return
	}

	// 测试可以捕获 scheduler 对象。
	if daemonKnobs != nil && daemonKnobs.CaptureJobScheduler != nil {
		daemonKnobs.CaptureJobScheduler(scheduler)
	}

	// 启动真正的后台调度循环。
	scheduler.runDaemon(ctx, stopper)
}
