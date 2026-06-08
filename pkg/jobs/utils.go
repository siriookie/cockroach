// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/errors"
)

var testingAvoidFullScans = metamorphic.ConstantWithTestBool(
	"jobs.avoid_full_scans_in_find_running_jobs",
	true, /* defaultValue */
)

var avoidFullScans = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"jobs.avoid_full_scans_in_find_running_jobs.enabled",
	"when true, enables hints to avoid full scans for internal, jobs-related queries",
	testingAvoidFullScans)

// RunningJobExists checks that whether there are any job of the given types
// in the pending, running, or paused state, optionally ignoring the job with
// the ID specified by ignoreJobID as well as any jobs created after it, if
// the passed ID is not InvalidJobID.
func RunningJobExists(
	ctx context.Context,
	cs *cluster.Settings,
	ignoreJobID jobspb.JobID,
	txn isql.Txn,
	jobTypes ...jobspb.Type,
) (exists bool, retErr error) {
	// 把传入的 jobspb.Type 列表转换成 SQL 中可用的字符串 tuple。
	// 例如转换成：('BACKUP', 'RESTORE') 这种形式。
	typeStrs, err := getJobTypeStrs(jobTypes)
	// 如果 job type 转换失败，直接返回错误。
	if err != nil {
		return false, err
	}

	// 默认按 created 排序。
	// 当需要 ignoreJobID 时，后面会依赖这个排序判断“第一个匹配 job 是否就是被忽略的 job”。
	orderBy := "ORDER BY created"
	// 如果没有指定要忽略的 job，就不需要排序。
	// LIMIT 1 只要找到任意一个匹配 job 即可。
	if ignoreJobID == jobspb.InvalidJobID {
		// There is no need to order by the created column if there is no job to
		// ignore.
		orderBy = ""
	}

	// 默认使用 jobs_status_created_idx 索引。
	hint := "jobs_status_created_idx"
	// 如果 cluster setting 开启了避免全表扫描，就加 FORCE_INDEX 和 AVOID_FULL_SCAN hint。
	// 这样查询 system.jobs 时会尽量走 status/created 相关索引，避免后台查询扫全表。
	if avoidFullScans.Get(&cs.SV) {
		hint = "{FORCE_INDEX=jobs_status_created_idx,AVOID_FULL_SCAN}"
	}

	// 构造查询模板：
	// 从 system.jobs 中查找指定 job_type 且状态为非终止状态的 job，最多返回 1 行。
	q := `SELECT id FROM system.jobs@%s WHERE job_type IN %s AND status IN %s %s LIMIT 1`
	// 把 index hint、job type tuple、非终止状态 tuple、ORDER BY 拼入 SQL。
	stmt := fmt.Sprintf(q, hint, typeStrs, NonTerminalStateTupleString, orderBy)

	// 在调用方传入的内部 SQL transaction 中执行查询。
	it, err := txn.QueryIterator(
		ctx,
		"find-running-jobs-of-type",
		txn.KV(),
		stmt,
	)
	// 查询启动失败时直接返回错误。
	if err != nil {
		return false, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	// 确保函数退出时关闭 iterator。
	// errors.CombineErrors 保证如果查询过程和 Close 都有错误，不会丢掉其中一个。
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	// 读取第一行结果。
	// 因为 SQL 里 LIMIT 1，这里最多只会有一行。
	ok, err := it.Next(ctx)
	// 读取 iterator 出错时返回错误。
	if err != nil {
		return false, err
	}
	// The query is ordered by `created` so if the first is the ignored ID, then
	// any additional rows that would match the passed types must be created after
	// the ignored ID and are also supposed to be ignored, meaning we only return
	// true when the there are non-zero results and the first does not match.
	// 如果没有查到任何行，ok=false，说明不存在匹配的非终止 job。
	// 如果查到的第一行就是 ignoreJobID，也返回 false；
	// 因为按 created 排序时，后续匹配行都比 ignoreJobID 更新，也按调用语义一起忽略。
	// 只有查到了行，并且第一行不是 ignoreJobID，才表示确实存在一个需要关注的 running/non-terminal job。
	return ok && jobspb.JobID(*it.Cur()[0].(*tree.DInt)) != ignoreJobID, nil
}

// RunningJobs returns the IDs of all jobs of the given types in the pending,
// running, or paused state, optionally ignoring the job with the ID specified
// by ignoreJobID as well as any jobs created after it, if the passed ID is not
// InvalidJobID.
func RunningJobs(
	ctx context.Context,
	cs *cluster.Settings,
	ignoreJobID jobspb.JobID,
	txn isql.Txn,
	jobTypes ...jobspb.Type,
) (jobIDs []jobspb.JobID, retErr error) {
	typeStrs, err := getJobTypeStrs(jobTypes)
	if err != nil {
		return jobIDs, err
	}

	orderBy := "ORDER BY created"
	if ignoreJobID == jobspb.InvalidJobID {
		// There is no need to order by the created column if there is no job to
		// ignore.
		orderBy = ""
	}

	hint := "jobs_status_created_idx"
	if avoidFullScans.Get(&cs.SV) {
		hint = "{FORCE_INDEX=jobs_status_created_idx,AVOID_FULL_SCAN}"
	}

	q := `SELECT id FROM system.jobs@%s WHERE job_type IN %s AND status IN %s %s`
	stmt := fmt.Sprintf(q, hint, typeStrs, NonTerminalStateTupleString, orderBy)
	it, err := txn.QueryIterator(
		ctx,
		"find-all-running-jobs-of-type",
		txn.KV(),
		stmt,
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return jobIDs, err
		}
		if !ok {
			break
		}
		jobID := jobspb.JobID(*it.Cur()[0].(*tree.DInt))
		// If we encounter the jobID to ignore, we can break early since all
		// additional rows must be created after the ignored ID.
		if jobID == ignoreJobID {
			break
		}
		jobIDs = append(jobIDs, jobID)
	}
	return jobIDs, nil
}

// JobExists returns true if there is a row corresponding to jobID in the
// system.jobs table.
func JobExists(
	ctx context.Context, jobID jobspb.JobID, txn *kv.Txn, ex isql.Executor,
) (bool, error) {
	row, err := ex.QueryRow(ctx, "check-for-job", txn, `SELECT id FROM system.jobs WHERE id = $1`, jobID)
	if err != nil {
		return false, err
	}
	return row != nil, nil
}

// JobCoordinatorID returns the coordinator node ID of the job.
func JobCoordinatorID(
	ctx context.Context, jobID jobspb.JobID, txn *kv.Txn, ex isql.Executor,
) (int32, error) {
	row, err := ex.QueryRow(ctx, "fetch-job-coordinator", txn, `SELECT claim_instance_id FROM system.jobs WHERE id = $1`, jobID)
	if err != nil {
		return 0, err
	}
	if row == nil {
		return 0, errors.Errorf("coordinator not found for job %d", jobID)
	}
	coordinatorID, ok := row[0].(*tree.DInt)
	if !ok {
		return 0, errors.AssertionFailedf("expected coordinator ID to be an int, got %T", row[0])
	}
	return int32(*coordinatorID), nil
}

// getJobTypeStrs is a helper function that returns a string representation of
// the job types for use in SQL queries, such as `('type1', 'type2')`. It
// returns an error if no job types are provided.
func getJobTypeStrs(jobTypes []jobspb.Type) (string, error) {
	var typeStrs string
	switch len(jobTypes) {
	case 0:
		return "", errors.AssertionFailedf("must specify job types")
	case 1:
		typeStrs = fmt.Sprintf("('%s')", jobTypes[0].String())
	case 2:
		typeStrs = fmt.Sprintf("('%s', '%s')", jobTypes[0].String(), jobTypes[1].String())
	default:
		var s strings.Builder
		fmt.Fprintf(&s, "('%s'", jobTypes[0].String())
		for _, typ := range jobTypes[1:] {
			fmt.Fprintf(&s, ", '%s'", typ.String())
		}
		s.WriteByte(')')
		typeStrs = s.String()
	}
	return typeStrs, nil
}
