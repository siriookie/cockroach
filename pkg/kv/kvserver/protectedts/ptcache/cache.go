// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ptcache

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Cache implements protectedts.Cache.
// TODO(#119243): delete this in 24.2
type Cache struct {
	db       isql.DB
	storage  protectedts.Manager
	stopper  *stop.Stopper
	settings *cluster.Settings
	sf       *singleflight.Group
	mu       struct {
		syncutil.RWMutex

		started bool

		// Updated in doUpdate().
		lastUpdate hlc.Timestamp
		state      ptpb.State

		// Updated in doUpdate but mutable. The records in the map are not mutated
		// and should not be modified by any client.
		recordsByID map[uuid.UUID]*ptpb.Record

		// TODO(ajwerner): add a more efficient lookup structure such as an
		// interval.Tree for Iterate.
	}
}

// Config configures a Cache.
type Config struct {
	DB       isql.DB
	Storage  protectedts.Manager
	Settings *cluster.Settings
}

// New returns a new cache.
func New(config Config) *Cache {
	c := &Cache{
		db:       config.DB,
		storage:  config.Storage,
		settings: config.Settings,
		sf:       singleflight.NewGroup("refresh-protectedts-cache", singleflight.NoTags),
	}
	c.mu.recordsByID = make(map[uuid.UUID]*ptpb.Record)
	return c
}

var _ protectedts.Cache = (*Cache)(nil)

// Iterate is part of the protectedts.Cache interface.
func (c *Cache) Iterate(
	_ context.Context, from, to roachpb.Key, it protectedts.Iterator,
) (asOf hlc.Timestamp) {
	c.mu.RLock()
	state, lastUpdate := c.mu.state, c.mu.lastUpdate
	c.mu.RUnlock()

	sp := roachpb.Span{
		Key:    from,
		EndKey: to,
	}
	for i := range state.Records {
		r := &state.Records[i]
		if !overlaps(r, sp) {
			continue
		}
		if wantMore := it(r); !wantMore {
			break
		}
	}
	return lastUpdate
}

// QueryRecord is part of the protectedts.Cache interface.
func (c *Cache) QueryRecord(_ context.Context, id uuid.UUID) (exists bool, asOf hlc.Timestamp) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists = c.mu.recordsByID[id]
	return exists, c.mu.lastUpdate
}

// refreshKey is used for the singleflight.
const refreshKey = ""

// Refresh is part of the protectedts.Cache interface.
func (c *Cache) Refresh(ctx context.Context, asOf hlc.Timestamp) error {
	for !c.upToDate(asOf) {
		future, _ := c.sf.DoChan(ctx,
			refreshKey,
			singleflight.DoOpts{
				Stop:               c.stopper,
				InheritCancelation: false,
			},
			c.doSingleFlightUpdate,
		)
		res := future.WaitForResult(ctx)
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}

// GetProtectionTimestamps is part of the spanconfig.ProtectedTSReader
// interface.
func (c *Cache) GetProtectionTimestamps(
	ctx context.Context, sp roachpb.Span,
) (protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, err error) {
	readAt := c.Iterate(ctx,
		sp.Key,
		sp.EndKey,
		func(rec *ptpb.Record) (wantMore bool) {
			protectionTimestamps = append(protectionTimestamps, rec.Timestamp)
			return true
		})
	return protectionTimestamps, readAt, nil
}

// Start 开启缓存的周期性刷新任务。
// 在缓存被启动之前，不能执行查询操作。
//
// 逻辑流程：
// 1. 状态检查：确保不会被重复启动。
// 2. 任务分发：通过 Stopper 启动一个名为 "periodically-refresh-protectedts-cache" 的后台异步任务。
func (c *Cache) Start(ctx context.Context, stopper *stop.Stopper) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.started {
		return errors.New("cannot start a Cache more than once")
	}
	c.mu.started = true
	c.stopper = stopper
	return c.stopper.RunAsyncTask(ctx, "periodically-refresh-protectedts-cache",
		c.periodicallyRefreshProtectedtsCache)
}

// periodicallyRefreshProtectedtsCache 是后台刷新的主循环。
// 它会根据集群设置中的 PollInterval 定期从系统表中同步受保护的时间戳状态。
//
// 核心逻辑：
// 1. 设置监听：监听 PollInterval 设置的变化。
// 2. 立即运行：节点启动后立即执行第一次读取，以确保缓存尽快可用。
// 3. 循环等待：
//   - 定时时间到：调用 doSingleFlightUpdate 更新缓存。
//   - 设置变更：动态调整定时器间隔。
//   - 系统关闭：安全退出。
//
// 例子：
// 比如集群设置了每 2 分钟刷新一次。
// 这个循环会每 2 分钟醒来一次，看看系统表里有没有新的“受保护记录”（例如某个备份任务正在运行中，
// 声明了某个时间点之后的数据不能被清理），然后更新内存里的 recordsByID 映射。
func (c *Cache) periodicallyRefreshProtectedtsCache(ctx context.Context) {
	settingChanged := make(chan struct{}, 1)
	protectedts.PollInterval.SetOnChange(&c.settings.SV, func(ctx context.Context) {
		select {
		case settingChanged <- struct{}{}:
		default:
		}
	})
	var timer timeutil.Timer
	defer timer.Stop()
	timer.Reset(0) // 启动后立即读取
	var lastReset time.Time
	for {
		select {
		case <-timer.C:
			// 使用 singleflight 确保即便有多个并发触发，也只有一个真正的数据库读取在运行。
			future, _ := c.sf.DoChan(ctx,
				refreshKey,
				singleflight.DoOpts{
					Stop:               c.stopper,
					InheritCancelation: false,
				},
				c.doSingleFlightUpdate,
			)

			select {
			case <-future.C():
			case <-c.stopper.ShouldQuiesce():
				return
			}
			res := future.WaitForResult(ctx)
			if res.Err != nil {
				if ctx.Err() == nil {
					log.KvDistribution.Errorf(ctx, "failed to refresh protected timestamps: %v", res.Err)
				}
			}
			// 重置定时器。
			timer.Reset(protectedts.PollInterval.Get(&c.settings.SV))
			lastReset = timeutil.Now()

		case <-settingChanged:
			// 处理采样间隔动态变更。
			interval := protectedts.PollInterval.Get(&c.settings.SV)
			nextUpdate := interval - timeutil.Since(lastReset)
			timer.Reset(nextUpdate)
			lastReset = timeutil.Now()

		case <-c.stopper.ShouldQuiesce():
			return
		}
	}
}

func (c *Cache) getMetadata() ptpb.Metadata {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.state.Metadata
}

// 从系统表里拉取当前所有 protected records，并缓存起来
func (c *Cache) doSingleFlightUpdate(ctx context.Context) (interface{}, error) {

	// 这个函数在 singleflight 保护下执行，
	// 保证同一时间只有一个 goroutine 进入这里，
	// 所以对 cache 的修改不会产生并发竞争。

	// 读取当前缓存中的 metadata（包含 version）
	prev := c.getMetadata()

	var (
		versionChanged bool          // 标记 version 是否变化
		state          ptpb.State    // 如果变化，则需要拉完整 state
		ts             hlc.Timestamp // 记录读取时的 HLC 时间戳
	)

	// 开启一个只读事务
	err := c.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {

		// 事务成功提交后，记录本次读取的 read timestamp
		// 这个时间戳用于标记 cache 的“最新更新时间”
		defer func() {
			if err == nil {
				ts = txn.KV().ReadTimestamp()
			}
		}()

		// 通过当前事务构造 storage 访问器
		pts := c.storage.WithTxn(txn)

		// 1️⃣ 读取 metadata（轻量）
		md, err := pts.GetMetadata(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to fetch protectedts metadata")
		}

		// 判断 version 是否变化
		versionChanged = md.Version != prev.Version

		// 如果 version 没变，直接返回
		// 不需要拉完整 state
		if !versionChanged {
			return nil
		}

		// 2️⃣ 如果 version 变了，读取完整 state（包含所有记录）
		state, err = pts.GetState(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to fetch protectedts state")
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// 开始更新 cache 内部状态
	c.mu.Lock()
	defer c.mu.Unlock()

	// 更新最后一次读取时间戳
	c.mu.lastUpdate = ts

	// 如果版本变化，重建本地缓存
	if versionChanged {

		// 更新整体 state
		c.mu.state = state

		// 清空原有的 records 索引
		for id := range c.mu.recordsByID {
			delete(c.mu.recordsByID, id)
		}

		// 重建 recordsByID 索引（UUID -> record）
		for i := range state.Records {
			r := &state.Records[i]
			c.mu.recordsByID[r.ID.GetUUID()] = r
		}
	}

	return nil, nil
}

// upToDate returns true if the lastUpdate for the cache is at least asOf.
func (c *Cache) upToDate(asOf hlc.Timestamp) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return asOf.LessEq(c.mu.lastUpdate)
}

func overlaps(r *ptpb.Record, sp roachpb.Span) bool {
	for i := range r.DeprecatedSpans {
		if r.DeprecatedSpans[i].Overlaps(sp) {
			return true
		}
	}
	return false
}
