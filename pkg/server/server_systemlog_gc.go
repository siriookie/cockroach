// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var (
	systemLogGCPeriod = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.log_gc.period",
		"the period at which log-like system tables are checked for old entries",
		time.Hour,
		settings.WithPublic)

	systemLogGCLimit = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"server.log_gc.max_deletions_per_cycle",
		"the maximum number of entries to delete on each purge of log-like system tables",
		1000,
		settings.WithPublic)

	// rangeLogTTL is the TTL for rows in system.rangelog. If non zero, range log
	// entries are periodically garbage collected.
	rangeLogTTL = settings.RegisterDurationSetting(
		settings.SystemOnly,
		"server.rangelog.ttl",
		"if nonzero, entries in system.rangelog older than this duration are periodically purged",
		30*24*time.Hour, // 30 days
		settings.WithPublic)

	// eventLogTTL is the TTL for rows in system.eventlog. If non zero, event log
	// entries are periodically garbage collected.
	eventLogTTL = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.eventlog.ttl",
		"if nonzero, entries in system.eventlog older than this duration are periodically purged",
		90*24*time.Hour, // 90 days
		settings.WithPublic)

	webSessionPurgeTTL = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.web_session.purge.ttl",
		"if nonzero, entries in system.web_sessions older than this duration are periodically purged",
		time.Hour,
		settings.WithPublic)
)

// gcSystemLog deletes entries in the given system log table between
// timestampLowerBound and timestampUpperBound.
// The system log table is expected to have a "timestamp" column.
// It returns the timestampLowerBound to be used in the next iteration, number
// of rows affected and error (if any).
func gcSystemLog(
	// ctx 控制本次内部 SQL 查询/删除的生命周期。
	ctx context.Context,
	// sqlServer 提供 internalExecutor，用来以 node user 身份执行内部 SQL。
	sqlServer *SQLServer,
	// opName 是内部 SQL 操作名，出现在日志、trace、statement stats 中。
	opName redact.RedactableString,
	// table 是 system.public 下要清理的表名，tsCol 是用来判断过期时间的列名。
	table, tsCol string,
	// timestampLowerBound 是本轮删除的起点，timestampUpperBound 是本轮删除的终点。
	timestampLowerBound, timestampUpperBound time.Time,
	// limit 是单个 DELETE batch 最多删除多少行。
	limit int64,
) (time.Time, int64, error) {
	// 累计本次 gcSystemLog 调用总共删除了多少行。
	var totalRowsAffected int64

	// 动态构造 DELETE 语句；表名和列名用 EscapeSQLIdent 防止标识符注入或非法字符问题。
	deleteStmt := fmt.Sprintf(
		`WITH d AS (
   DELETE FROM system.public.%[1]s
    WHERE %[2]s >= $1 AND %[2]s <= $2
    LIMIT $3
RETURNING %[2]s
)
SELECT count(1), max(%[2]s) FROM d`,
		// 转义 system 表名。
		lexbase.EscapeSQLIdent(table),
		// 转义时间戳列名。
		lexbase.EscapeSQLIdent(tsCol),
	)

	// 循环执行 batch delete，直到当前时间窗口里没有可删除的行。
	for {
		// rowsAffected 记录当前 batch 删除了多少行。
		var rowsAffected int64
		// 用立即执行函数包住一轮 SQL 执行，方便在内部用 return err 退出这一轮。
		err := func() error {
			// 通过 internalExecutor 执行 DELETE，并返回 count 和 max(timestamp)。
			row, err := sqlServer.internalExecutor.QueryRowEx(
				// 传入调用方上下文，支持取消。
				ctx,
				// 内部 SQL 操作名。
				opName,
				// 不传外部 txn，让 internalExecutor 自己为这条 SQL 创建事务。
				nil, /* txn */
				// 以 node user session data 执行，避免依赖普通用户权限。
				sessiondata.NodeUserSessionDataOverride,
				// 要执行的 DELETE CTE SQL。
				deleteStmt,
				// $1：删除范围下界。
				timestampLowerBound,
				// $2：删除范围上界。
				timestampUpperBound,
				// $3：单批删除上限。
				limit,
			)
			// SQL 执行失败，返回错误给外层。
			if err != nil {
				return err
			}

			// QueryRowEx 正常情况下会返回一行：count(1), max(timestamp)。
			if row != nil {
				// 第一列是本 batch 删除的行数。
				rowCount, ok := row[0].(*tree.DInt)
				// 类型不是 DInt，说明 SQL 返回值和预期不一致。
				if !ok {
					return errors.Errorf("row count is of unknown type %T", row[0])
				}
				// count 不应该是 nil；nil 表示解析异常。
				if rowCount == nil {
					return errors.New("error parsing row count")
				}
				// 记录本 batch 删除行数。
				rowsAffected = int64(*rowCount)

				// 只有本 batch 真删到了行，max(timestamp) 才有意义。
				if rowsAffected > 0 {
					// 第二列是本 batch 删除行里的最大 timestamp。
					maxTimestamp, ok := row[1].(*tree.DTimestamp)
					// 类型不是 DTimestamp，说明 SQL 返回值和预期不一致。
					if !ok {
						return errors.Errorf("timestamp is of unknown type %T", row[1])
					}
					// 删除了行但 max(timestamp) 是 nil，说明结果异常。
					if maxTimestamp == nil {
						return errors.New("error parsing timestamp")
					}
					// 推进 lower bound，下一个 batch 从刚删到的最大 timestamp 继续。
					timestampLowerBound = maxTimestamp.Time
				}
			}
			// 当前 batch 处理成功。
			return nil
		}()

		// 把当前 batch 删除数量累加到总删除数量。
		totalRowsAffected += rowsAffected
		// 如果当前 batch 出错，返回当前 lower bound、累计删除数量和错误。
		if err != nil {
			return timestampLowerBound, totalRowsAffected, err
		}

		// 当前 batch 没删到任何行，说明这个时间窗口内已经清理完。
		if rowsAffected == 0 {
			// 返回 upper bound 作为新的 lower bound，下一轮 GC 可以从这个上界之后继续。
			return timestampUpperBound, totalRowsAffected, nil
		}
	}
}

// systemLogGCConfig has configurations for gc of systemlog.
type systemLogGCConfig struct {
	// onlySystemTenant indicates whether the cleanup for this
	// table should occur in secondary tenants or not.
	onlySystemTenant bool

	// table is the name of the system table.
	table string

	// timestampCol is the name of the timestamp column, used
	// as condition to cut old values.
	timestampCol string

	// ttl is the time to live for rows in systemlog table.
	ttl *settings.DurationSetting

	// timestampLowerBound is the timestamp below which rows are gc'ed.
	// It is maintained to avoid hitting tombstones during gc and is updated
	// after every gc run.
	timestampLowerBound time.Time
}

func runSystemLogGCForOneTable(
	// ctx 是本轮 GC 使用的上下文，取消时 SQL 删除也会被取消。
	ctx context.Context, sqlServer *SQLServer, st *cluster.Settings, gcConfig *systemLogGCConfig,
) (int64, error) {
	// 从 gcConfig 对应的 cluster setting 里读取这张表的 TTL。
	ttl := gcConfig.ttl.Get(&st.SV)
	// TTL 为 0 表示这张表关闭 GC。
	if ttl == 0 {
		// GC disabled for this table. Do nothing.
		return 0, nil
	}

	// 构造内部 SQL executor 的 opName，便于日志、trace、statement stats 区分是哪张表的 GC。
	opName := redact.Sprintf("%s-%s-gc", gcConfig.table, gcConfig.timestampCol)
	// 读取每轮最多删除多少行，避免一次 DELETE 太大。
	limit := systemLogGCLimit.Get(&st.SV)
	// 计算本轮可删除数据的时间上界：当前物理时间 - TTL。
	timestampUpperBound := timeutil.Unix(0, sqlServer.execCfg.Clock.PhysicalNow()-int64(ttl))
	// 执行真正的 DELETE：删除 [timestampLowerBound, timestampUpperBound] 范围内的过期行。
	newTimestampLowerBound, rowsAffected, err := gcSystemLog(
		ctx, sqlServer, opName,
		// 要清理的 system 表名和作为时间条件的列名。
		gcConfig.table, gcConfig.timestampCol,
		// 从上次 GC 推进到的位置继续删，避免反复扫已经处理过的旧 tombstone。
		gcConfig.timestampLowerBound,
		// 只删除 TTL 之前的数据，更新的数据必须保留。
		timestampUpperBound,
		// 每个 DELETE batch 的最大行数。
		limit,
	)
	// 如果删除过程中出错，返回已经影响的行数和错误。
	if err != nil {
		return rowsAffected, err
	}
	// 删除成功后，保存新的 lower bound，下一轮从这里继续。
	gcConfig.timestampLowerBound = newTimestampLowerBound
	// 返回本轮删除的总行数。
	return rowsAffected, nil
}

func runSystemLogGC(
	ctx context.Context, sqlServer *SQLServer, st *cluster.Settings, gcConfigs []systemLogGCConfig,
) {
	forSystemTenant := sqlServer.execCfg.Codec.ForSystemTenant()
	for i := range gcConfigs {
		gcConfig := &gcConfigs[i]
		if gcConfig.onlySystemTenant && !forSystemTenant {
			continue
		}

		if rowsAffected, err := runSystemLogGCForOneTable(ctx, sqlServer, st, gcConfig); err != nil {
			log.Dev.Warningf(ctx, "error garbage collecting %s.%s: %v", gcConfig.table, gcConfig.timestampCol, err)
		} else {
			log.Dev.Infof(ctx, "garbage collected %d rows from %s.%s", rowsAffected, gcConfig.table, gcConfig.timestampCol)
		}
	}
}

func getTablesToGC() []systemLogGCConfig {
	// NB: we need to reconstruct the slice anew every time
	// because the timestampLowerBound field is modified in-place
	// by the GC task.
	return []systemLogGCConfig{
		{true, "rangelog", "timestamp", rangeLogTTL, timeutil.Unix(0, 0)},
		{false, "eventlog", "timestamp", eventLogTTL, timeutil.Unix(0, 0)},
		{false, "web_sessions", "expiresAt", webSessionPurgeTTL, timeutil.Unix(0, 0)},
		{false, "web_sessions", "revokedAt", webSessionPurgeTTL, timeutil.Unix(0, 0)},
	}
}

// startSystemLogsGC starts a worker which periodically GCs system.rangelog
// and system.eventlog.
// The TTLs for each of these logs is retrieved from cluster settings.
//
// TODO(knz): This should be best replaced by SQL-level row TTL.
// See: https://github.com/cockroachdb/cockroach/issues/89453
func startSystemLogsGC(ctx context.Context, sqlServer *SQLServer) error {
	// systemLogsToGC stores the state of the GC loops,
	// including the lower bound for the deletion timestamps.
	// 构造需要 GC 的 system 表配置列表，并保存每张表当前 GC 的 timestampLowerBound。
	systemLogsToGC := getTablesToGC()

	// cfg 保存 server 的配置和 testing knobs，下面会用来读取测试覆盖项。
	cfg := sqlServer.cfg
	// st 是 cluster settings，GC 周期、TTL、每轮删除上限都从这里读取。
	st := sqlServer.execCfg.Settings

	// 启动一个由 stopper 管理的后台任务；server quiesce 时这个任务会退出。
	return sqlServer.stopper.RunAsyncTask(ctx, "system-log-gc", func(ctx context.Context) {
		// getPeriod 每次循环都会重新读取当前 GC 周期，这样 cluster setting 变更可以生效。
		getPeriod := func() time.Duration {
			// 从 server.log_gc.period 读取正常的 GC 周期。
			period := systemLogGCPeriod.Get(&st.SV)
			// 防止用户把周期调得太小，导致后台任务 busy loop。
			if period < 1*time.Second {
				// Clamp the period to 1 second to avoid busy looping.
				period = 1 * time.Second
			}
			// 测试里可以通过 StoreTestingKnobs 覆盖 GC 周期，让测试不用等真实周期。
			if storeKnobs, ok := cfg.TestingKnobs.Store.(*kvserver.StoreTestingKnobs); ok && storeKnobs.SystemLogsGCPeriod != 0 {
				period = storeKnobs.SystemLogsGCPeriod
			}
			// 返回最终使用的周期。
			return period
		}
		// 读取当前周期，第一次启动 timer 时会用到。
		period := getPeriod()
		// timeutil.Timer 是可复用 timer，避免循环里反复创建新 timer。
		var timer timeutil.Timer
		// 后台任务退出时停止 timer，释放底层资源。
		defer timer.Stop()
		// The very first period is 25% off the configured period, to
		// avoid a thundering herd effect on the system table between
		// nodes when multiple nodes are started simultaneously.
		// 第一次执行加随机 jitter，避免多个节点同时启动后一起打 system 表造成尖峰。
		timer.Reset(jitteredInterval(period))

		// 后续每次循环结束后，重新读取周期并 reset timer。
		for ; ; timer.Reset(getPeriod()) {
			// 等待 timer、server shutdown 或 ctx cancellation 中的任意一个事件。
			select {
			// timer 到期，开始做一轮 system logs GC。
			case <-timer.C:
				// Do the work for all system tables.
				// 遍历 systemLogsToGC 里配置的表，删除超过 TTL 的旧数据。
				runSystemLogGC(ctx, sqlServer, st, systemLogsToGC)

				// If we are in a test, coordinate with the test.
				// 测试场景下，如果测试注入了通知 channel，就在一轮 GC 完成后通知测试。
				if storeKnobs, ok := cfg.TestingKnobs.Store.(*kvserver.StoreTestingKnobs); ok && storeKnobs.SystemLogsGCGCDone != nil {
					// 通知测试时也监听 shutdown/cancel，避免测试结束后 goroutine 卡住。
					select {
					// 成功通知测试：这一轮 GC 已完成，可以进入下一轮循环。
					case storeKnobs.SystemLogsGCGCDone <- struct{}{}:
						// Step has made one step forward. We can continue to iterate.
					// server 正在关闭，直接退出后台任务。
					case <-sqlServer.stopper.ShouldQuiesce():
						// Test has finished.
						return
					// ctx 被取消，直接退出后台任务。
					case <-ctx.Done():
						// Test also has finished.
						return
					}
				}
			// server 进入 quiesce/shutdown，停止 GC loop。
			case <-sqlServer.stopper.ShouldQuiesce():
				// Server is shutting down.
				return
			// 外层 context 被取消，停止 GC loop。
			case <-ctx.Done():
				// Something is telling us to go away.
				return
			}
		}
	})
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from the interval.
func jitteredInterval(interval time.Duration) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*rand.Float64()))
}
