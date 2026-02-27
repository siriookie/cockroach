// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

var (
	forwardClockJumpCheckEnabled = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"server.clock.forward_jump_check_enabled",
		"if enabled, forward clock jumps > max_offset/2 will cause a panic",
		false,
		settings.WithName("server.clock.forward_jump_check.enabled"),
		settings.WithPublic)

	persistHLCUpperBoundInterval = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.clock.persist_upper_bound_interval",
		"the interval between persisting the wall time upper bound of the clock. The clock "+
			"does not generate a wall time greater than the persisted timestamp and will panic if "+
			"it sees a wall time greater than this value. When cockroach starts, it waits for the "+
			"wall time to catch-up till this persisted timestamp. This guarantees monotonic wall "+
			"time across server restarts. Not setting this or setting a value of 0 disables this "+
			"feature.",
		0,
		settings.WithPublic)
)

// startMonitoringForwardClockJumps starts a background task to monitor forward
// clock jumps based on a cluster setting.

// startMonitoringForwardClockJumps 函数的作用：
// 启动一个后台任务，用于监控系统时钟的“向前跳跃”（forward clock jumps），
// 例如由于 NTP 同步、手动调整时间、虚拟机时间漂移等原因导致的时钟突然前进很多。
// CockroachDB 是分布式数据库，对时钟同步非常敏感（依赖 HLC 混合逻辑时钟），
// 时钟向前跳跃如果过大，可能导致事务时间戳混乱、租约（lease）失效、数据不一致等问题。

func (s *topLevelServer) startMonitoringForwardClockJumps(ctx context.Context) error {
	// 创建一个带缓冲的 bool 通道，用于动态接收集群设置的变化
	// （是否启用向前时钟跳跃检查）
	forwardJumpCheckEnabled := make(chan bool, 1)

	// 当服务器停止时（stopper.Stop() 被调用），关闭这个通道，确保后台监控任务能优雅退出
	s.stopper.AddCloser(stop.CloserFn(func() { close(forwardJumpCheckEnabled) }))

	// 监听集群设置 server.clock.forward_jump_check.enabled 的变化
	// 每当该设置在集群中被修改（通过 SET CLUSTER SETTING），就会执行回调
	forwardClockJumpCheckEnabled.SetOnChange(&s.st.SV, func(context.Context) {
		// 把当前设置的值（true/false）发送到通道中
		// 注意：通道有缓冲，所以不会阻塞
		forwardJumpCheckEnabled <- forwardClockJumpCheckEnabled.Get(&s.st.SV)
	})

	// 调用物理时钟（PhysicalClock）的 StartMonitoringForwardClockJumps 方法
	// 参数：
	//   - ctx：上下文
	//   - forwardJumpCheckEnabled：一个 chan bool 通道
	//     后台监控 goroutine 会不断从这个通道读取 bool 值：
	//       true  → 启用时钟跳跃检测
	//       false → 暂停检测
	//     通道关闭 → 停止监控（优雅退出）
	//   - time.NewTicker：提供定时器工厂函数，用于周期性检查时钟
	//   - nil：tick 回调（这里不需要额外回调）
	if err := s.clock.StartMonitoringForwardClockJumps(
		ctx,
		forwardJumpCheckEnabled,
		time.NewTicker,
		nil, /* tick callback */
	); err != nil {
		return errors.Wrap(err, "monitoring forward clock jumps")
	}

	// 记录信息日志，表示已成功启动时钟跳跃监控，并受集群设置控制
	log.Ops.Info(ctx, "monitoring forward clock jumps based on server.clock.forward_jump_check.enabled")

	return nil
}

// checkHLCUpperBoundExists determines whether there's an HLC
// upper bound that will need to refreshed/persisted after
// the server has initialized.
func (s *topLevelServer) checkHLCUpperBoundExistsAndEnsureMonotonicity(
	ctx context.Context, initialStart bool,
) (hlcUpperBoundExists bool, err error) {
	if initialStart {
		// Clock monotonicity checks can be skipped on server bootstrap
		// because the server has never
		// been used before.
		return false, nil
	}

	// TODO(sep-raft-log): make sure we're reading only from log engines. There
	// seems to be no harm in reading from state engines too (and seeing zeroes),
	// but the HLC key is in the log engines.
	hlcUpperBound, err := kvserver.ReadMaxHLCUpperBound(ctx, s.engines)
	if err != nil {
		return false, errors.Wrap(err, "reading max HLC upper bound")
	}
	hlcUpperBoundExists = hlcUpperBound > 0

	// If the server is being restarted, sleep to ensure monotonicity of the HLC
	// clock.
	ensureClockMonotonicity(
		ctx,
		s.clock,
		s.startTime,
		hlcUpperBound,
		s.clock.SleepUntil,
	)

	return hlcUpperBoundExists, nil
}

// ensureClockMonotonicity sleeps till the wall time reaches
// prevHLCUpperBound. prevHLCUpperBound > 0 implies we need to guarantee HLC
// monotonicity across server restarts. prevHLCUpperBound is the last
// successfully persisted timestamp greater then any wall time used by the
// server.
//
// If prevHLCUpperBound is 0, the function sleeps up to max offset.
func ensureClockMonotonicity(
	ctx context.Context,
	clock *hlc.Clock,
	startTime time.Time,
	prevHLCUpperBound int64,
	sleepUntilFn func(context.Context, hlc.Timestamp) error,
) {
	var sleepUntil int64
	if prevHLCUpperBound != 0 {
		// Sleep until previous HLC upper bound to ensure wall time monotonicity
		sleepUntil = prevHLCUpperBound + 1
	} else {
		// Previous HLC Upper bound is not known
		// We might have to sleep a bit to protect against this node producing non-
		// monotonic timestamps. Before restarting, its clock might have been driven
		// by other nodes' fast clocks, but when we restarted, we lost all this
		// information. For example, a client might have written a value at a
		// timestamp that's in the future of the restarted node's clock, and if we
		// don't do something, the same client's read would not return the written
		// value. So, we wait up to MaxOffset; we couldn't have served timestamps more
		// than MaxOffset in the future (assuming that MaxOffset was not changed, see
		// #9733).
		//
		// As an optimization for tests, we don't sleep if all the stores are brand
		// new. In this case, the node will not serve anything anyway until it
		// synchronizes with other nodes.
		sleepUntil = startTime.UnixNano() + int64(clock.MaxOffset()) + 1
	}

	currentWallTime := clock.Now().WallTime
	delta := time.Duration(sleepUntil - currentWallTime)
	if delta > 0 {
		log.Ops.Infof(
			ctx,
			"Sleeping till wall time %v to catches up to %v to ensure monotonicity. Sub: %v",
			currentWallTime,
			sleepUntil,
			delta,
		)
		_ = sleepUntilFn(ctx, hlc.Timestamp{WallTime: sleepUntil})
	}
}

// periodicallyPersistHLCUpperBound periodically persists an upper bound of
// the HLC's wall time. The interval for persisting is read from
// persistHLCUpperBoundIntervalCh. An interval of 0 disables persisting.
//
// persistHLCUpperBoundFn is used to persist the hlc upper bound, and should
// return an error if the persist fails.
//
// tickerFn is used to create the ticker used for persisting
//
// tickCallback is called whenever a tick is processed
// periodicallyPersistHLCUpperBound 是一个死循环任务，负责执行 HLC 上限持久化的具体调度逻辑。
//
// 该函数通过 select 监听三个关键信号：
// 1. 设置变更 (persistHLCUpperBoundIntervalCh)：支持动态开启、关闭或调整打卡频率。
// 2. 定时触发 (ticker.C)：按照设定的频率定期向磁盘续写上限值。
// 3. 停止信号 (stopCh)：确保在服务器关闭时能安全退出。
//
// 核心逻辑：
// - 缓冲区设计：每次持久化计算上限时，会使用 3 倍的间隔时间作为增量（delta）。
//   这意味着如果每 10 秒打一次卡，写入磁盘的上限值通常是“当前时间 + 30 秒”。
//   这样即使某次打卡因为短暂的磁盘延迟稍微晚了一点，系统依然处于之前预告的 30 秒保护范围内，是安全的。
//
// 例子：
// 假设 persistInterval 设为 10s：
// 1. [T = 0s]：Loop 启动，触发第一次持久化。
//    - 调用 clock.RefreshHLCUpperBound，计算：上限 = 现在(0s) + 3 * 10s = 30s。
//    - 将 30s 写入磁盘。
// 2. [T = 10s]：定时器触发第二次持久化。
//    - 计算：上限 = 现在(10s) + 3 * 10s = 40s。
//    - 将 40s 覆盖写入磁盘。
// 3. [T = 15s]：服务器突然崩溃。
// 4. [重启]：服务器读取磁盘发现上限是 40s。即使当前主板电池没电时钟跳回了 1970 年，
//    服务器也会强行等待（或报错），直到物理时间超过 40s，保证了跨重启的绝对单调性。
func periodicallyPersistHLCUpperBound(
	clock *hlc.Clock,
	persistHLCUpperBoundIntervalCh chan time.Duration,
	persistHLCUpperBoundFn func(int64) error,
	tickerFn func(d time.Duration) *time.Ticker,
	stopCh <-chan struct{},
	tickCallback func(),
) {
	// 初始化一个定时器，默认先停用
	ticker := tickerFn(time.Hour)
	ticker.Stop()

	var persistInterval time.Duration

	// 定义具体的持久化执行闭包
	persistHLCUpperBound := func() {
		// 使用 3 倍的间隔时间作为安全冗余计算上限
		if err := clock.RefreshHLCUpperBound(
			persistHLCUpperBoundFn,
			int64(persistInterval*3),
		); err != nil {
			log.Ops.Fatalf(
				context.Background(),
				"error persisting HLC upper bound: %v",
				err,
			)
		}
	}

	for {
		select {
		case updatedPersistInterval := <-persistHLCUpperBoundIntervalCh:
			// 1. 处理设置变更信号
			if updatedPersistInterval == persistInterval {
				continue
			}
			persistInterval = updatedPersistInterval

			ticker.Stop()
			if persistInterval > 0 {
				// 如果开启了该功能，则重置定时器并立即打卡一次
				ticker = tickerFn(persistInterval)
				persistHLCUpperBound()
				log.Ops.Infof(context.Background(), "persisting HLC upper bound is enabled [every %.2fs]",
					persistInterval.Seconds())
			} else {
				// 如果禁用了该功能，则清理磁盘上的上限标记
				if err := clock.ResetHLCUpperBound(persistHLCUpperBoundFn); err != nil {
					log.Ops.Fatalf(
						context.Background(),
						"error resetting hlc upper bound: %v",
						err,
					)
				}
				log.Ops.Info(context.Background(), "persisting HLC upper bound is disabled")
			}

		case <-ticker.C:
			// 2. 处理定时打卡信号
			if persistInterval > 0 {
				persistHLCUpperBound()
			}

		case <-stopCh:
			// 3. 优雅退出
			ticker.Stop()
			return
		}

		if tickCallback != nil {
			tickCallback()
		}
	}
}

// startPersistingHLCUpperBound starts a goroutine to persist an upper bound
// to the HLC.
//
// persistHLCUpperBoundFn is used to persist upper bound of the HLC, and should
// return an error if the persist fails
//
// tickerFn is used to create a new ticker
//
// tickCallback is called whenever persistHLCUpperBoundCh or a ticker tick is
// processed
// startPersistingHLCUpperBound 启动一个后台任务，定期将 HLC（混合逻辑时钟）的“物理时间上限”持久化到磁盘上。
//
// 该函数的核心作用是保证：即使服务器发生崩溃并重启，系统时钟依然保持“单调递增（Monotonicity）”，
// 绝不会产生比崩溃前更旧的时间戳。
//
// 工作原理（Safety in the Future）：
// 1. 预报上限：服务器会周期性（如每 10 秒）向磁盘写入一个“未来值”，例如：当前时间 + 30 秒。
// 2. 软约束：一旦写入磁盘，本地时钟就被限制住，绝对不允许产生超过这个“上限值”的时间戳。
// 3. 重启校验：如果服务器突然崩溃并立即重启，它会先从磁盘读出这个最后写入的“上限值”。
// 4. 等待追赶：重启后的服务器会进入休眠，直到物理时钟真正超过这个磁盘记录的“上限值”，才会开始对外服务。
//
// 例子：
// - T = 10:00:00：系统启动，往磁盘写下 UpperBound = 10:00:30。
// - T = 10:00:05：服务器由于断电突然崩溃。
// - T = 10:00:06：服务器立即重启。
// - 逻辑判断：此时物理时间才 10:00:06，但磁盘记录了 10:00:30。
// - 结果：为了绝对安全，服务器会傻等 24 秒（直到物理时间追上 10:00:30），然后再开始工作。
//   这样就杜绝了服务器在重启后产生“10:00:07”这种可能已经在崩溃前被其他事务用过的时间戳。
func (s *topLevelServer) startPersistingHLCUpperBound(
	ctx context.Context, hlcUpperBoundExists bool,
) error {
	tickerFn := time.NewTicker
	// 定义持久化函数：调用 node 接口将上限值写入所有本地存储引擎。
	persistHLCUpperBoundFn := func(t int64) error {
		return s.node.SetHLCUpperBound(context.Background(), t)
	}
	persistHLCUpperBoundIntervalCh := make(chan time.Duration, 1)

	// 获取集群设置并监听其动态变化。
	persistHLCUpperBoundIntervalCh <- persistHLCUpperBoundInterval.Get(&s.st.SV)
	persistHLCUpperBoundInterval.SetOnChange(&s.st.SV, func(context.Context) {
		persistHLCUpperBoundIntervalCh <- persistHLCUpperBoundInterval.Get(&s.st.SV)
	})

	// 如果磁盘上已经存在上限记录（说明上一次运行启用了此功能），
	// 在启动后台循环前，先手动刷新一次上限值，确保持续的单调性。
	if hlcUpperBoundExists {
		if err := s.clock.RefreshHLCUpperBound(
			persistHLCUpperBoundFn,
			int64(5*time.Second),
		); err != nil {
			return errors.Wrap(err, "refreshing HLC upper bound")
		}
	}

	// 启动异步后台任务执行周期性的打卡（写上限）循环。
	_ = s.stopper.RunAsyncTask(
		ctx,
		"persist-hlc-upper-bound",
		func(context.Context) {
			periodicallyPersistHLCUpperBound(
				s.clock,
				persistHLCUpperBoundIntervalCh,
				persistHLCUpperBoundFn,
				tickerFn,
				s.stopper.ShouldQuiesce(),
				nil, /* tick callback */
			)
		},
	)
	return nil
}
