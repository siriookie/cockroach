// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insightspb"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
)

// lockingRegistry is the central object in the insights subsystem. It observes
// statement execution to determine which statements are outliers and
// writes insights into the provided sink.
type lockingRegistry struct {
	detector detector
	causes   *causes
	store    *LockingStore
}

// Instead of creating and allocating a map to track duplicate
// causes each time, we just iterate through the array since
// we don't expect it to be very large.
func addCause(arr []insightspb.Cause, n insightspb.Cause) []insightspb.Cause {
	for i := range arr {
		if arr[i] == n {
			return arr
		}
	}
	return append(arr, n)
}

// Instead of creating and allocating a map to track duplicate
// problems each time, we just iterate through the array since
// we don't expect it to be very large.
func addProblem(arr []insightspb.Problem, n insightspb.Problem) []insightspb.Problem {
	for i := range arr {
		if arr[i] == n {
			return arr
		}
	}
	return append(arr, n)
}

// observeTransaction determines if a transaction is slow or failed and records
// an insight if so. It does so by examining the statements in the transaction
// and marking them as slow or failed if they are.
// the given sessionID. The buffer is returned to the pool along with the
// statements it contains.
func (r *lockingRegistry) observeTransaction(
	transactionStats *sqlstats.RecordedTxnStats, statements []*sqlstats.RecordedStmtStats,
) {
	// 如果 insights detector 没启用，直接跳过，不做任何分析。
	if !r.enabled() {
		return
	}

	// 如果 transaction 没有有效的 TransactionID，也跳过。
	// insights 需要依赖执行 ID 来标识一次具体执行。
	if transactionStats.TransactionID.Equal(uuid.Nil) {
		return
	}

	// 取出 sessionID，后面创建 insight 时会用它标识所属 session。
	sessionID := transactionStats.SessionID

	// Mark statements which are detected as slow or have a failed status.
	// slowOrFailedStatements 用 statement 在 statements slice 里的下标来记录：
	// 哪些 statement 是慢 SQL 或执行失败的 SQL。
	var slowOrFailedStatements intsets.Fast
	// 遍历 transaction 中的所有 statement。
	for i, s := range statements {
		// 如果 statement 不应该被忽略，并且它被 detector 判定为慢 SQL，或者它执行失败，
		// 就把它的下标加入 slowOrFailedStatements。
		if !shouldIgnoreStatement(s) && (r.detector.isSlow(s) || s.Failed) {
			slowOrFailedStatements.Add(i)
		}
	}

	// So far this is the only case when a transaction is considered slow.
	// In the future, we may want to make a detector for transactions if there
	// are more cases.
	// 当前 transaction 级别的慢执行判断只看 contention time。
	// 如果事务 contention time 超过 insights 的 latency threshold，就认为有 high contention。
	highContention := transactionStats.ExecStats.ContentionTime.Seconds() >= LatencyThreshold.Get(&r.causes.st.SV).Seconds()

	// transaction 没有 committed，就认为事务失败。
	txnFailed := !transactionStats.Committed
	// 如果没有慢/失败 statement，也没有高 contention，事务本身也没有失败，
	// 那就没有值得记录的 insight，直接返回。
	if slowOrFailedStatements.Empty() && !highContention && !txnFailed {
		// We only record an insight if we have slow statements, high txn contention, or failed executions.
		return
	}

	// Note that we'll record insights for every statement, not just for
	// the slow ones.
	// 根据 transactionStats 构造 transaction insight 的基础信息。
	transaction := makeTxnInsight(transactionStats)
	// 根据 sessionID 和 transaction insight 创建完整 insight 对象。
	insight := makeInsight(sessionID, transaction)

	// 如果事务存在高 contention，把事务标记为慢执行问题，并记录原因是 HighContention。
	if highContention {
		insight.Transaction.Problems = addProblem(insight.Transaction.Problems, insightspb.Problem_SlowExecution)
		insight.Transaction.Causes = addCause(insight.Transaction.Causes, insightspb.Cause_HighContention)
	}

	// 如果事务失败，把事务标记为 FailedExecution 问题。
	if txnFailed {
		insight.Transaction.Problems = addProblem(insight.Transaction.Problems, insightspb.Problem_FailedExecution)
	}

	// The transaction status will reflect the status of its statements; it will
	// default to completed unless a failed statement status is found. Note that
	// this does not take into account the "Cancelled" transaction status.
	// lastStmtErr 保存最后一个需要冒泡到事务级别的 statement 错误消息。
	var lastStmtErr redact.RedactableString
	// lastStmtErrCode 保存最后一个需要冒泡到事务级别的 statement 错误码。
	var lastStmtErrCode string
	// 遍历 transaction 中的所有 statements。
	for i, recordedStmt := range statements {
		// 把 RecordedStmtStats 转成 insights 子系统使用的 statement insight。
		s := makeStmtInsight(recordedStmt)
		// 如果当前 statement 是之前识别出的慢 SQL 或失败 SQL，补充问题和原因。
		if slowOrFailedStatements.Contains(i) {
			// 根据 statement 的执行状态决定记录慢执行还是失败执行。
			switch s.Status {
			case insightspb.Statement_Completed:
				// 已完成但被 detector 判定为慢 SQL，标记为 SlowExecution。
				s.Problem = insightspb.Problem_SlowExecution
				// 进一步分析慢 SQL 的具体原因，例如 high contention、plan regression 等。
				s.Causes = r.causes.examine(s.Causes, s)
			case insightspb.Statement_Failed:
				// statement 执行失败，标记为 FailedExecution。
				s.Problem = insightspb.Problem_FailedExecution
				// 如果 transaction 本身没有错误码，就暂存 statement 的错误信息，
				// 后面可以把 statement failure 冒泡到 transaction 级别。
				if transaction.LastErrorCode == "" {
					lastStmtErr = s.ErrorMsg
					lastStmtErrCode = s.ErrorCode
				}
			}

			// Bubble up stmt problems and causes.
			// 把 statement 级别的 causes 冒泡到 transaction 级别。
			for i := range s.Causes {
				insight.Transaction.Causes = addCause(insight.Transaction.Causes, s.Causes[i])
			}
			// 把 statement 级别的问题也冒泡到 transaction 级别。
			insight.Transaction.Problems = addProblem(insight.Transaction.Problems, s.Problem)
		}

		// 记录 transaction 包含的 statement execution ID。
		insight.Transaction.StmtExecutionIDs = append(insight.Transaction.StmtExecutionIDs, s.ID)
		// 把当前 statement insight 放入完整 insight。
		insight.Statements = append(insight.Statements, s)
	}

	// 如果 transaction 本身没有错误码，但 statement 有错误码，
	// 就把 statement failure 信息补到 transaction 上。
	if transaction.LastErrorCode == "" && lastStmtErrCode != "" {
		// Stmt failure equates to transaction failure. Sometimes we
		// can't propagate the error up to the transaction level so
		// we manually set the transaction's failure info here.
		// 设置 transaction 的最后错误消息。
		transaction.LastErrorMsg = lastStmtErr
		// 设置 transaction 的最后错误码。
		transaction.LastErrorCode = lastStmtErrCode
	}

	// 最后把完整 insight 写入内存 store，供后续查询/展示。
	r.store.addInsight(insight)
}

// TODO(todd):
//
//	Once we can handle sufficient throughput to live on the hot
//	execution path in #81021, we can probably get rid of this external
//	concept of "enabled" and let the detectors just decide for themselves
//	internally.
func (r *lockingRegistry) enabled() bool {
	return r.detector.enabled()
}

func newRegistry(st *cluster.Settings, detector detector, store *LockingStore) *lockingRegistry {
	return &lockingRegistry{
		detector: detector,
		causes:   &causes{st: st},
		store:    store,
	}
}
