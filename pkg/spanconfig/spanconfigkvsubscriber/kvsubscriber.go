// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigkvsubscriber

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var updateBehindNanos = metric.Metadata{
	Name: "spanconfig.kvsubscriber.update_behind_nanos",
	Help: "Difference between the current time and when the KVSubscriber received its last update" +
		" (an ever increasing number indicates that we're no longer receiving updates)",
	Measurement: "Nanoseconds",
	Unit:        metric.Unit_NANOSECONDS,
}

var protectedRecordCount = metric.Metadata{
	Name:        "spanconfig.kvsubscriber.protected_record_count",
	Help:        "Number of protected timestamp records, as seen by KV",
	Measurement: "Records",
	Unit:        metric.Unit_COUNT,
}

var oldestProtectedRecordNanos = metric.Metadata{
	Name: "spanconfig.kvsubscriber.oldest_protected_record_nanos",
	Help: "Difference between the current time and the oldest protected timestamp" +
		" (sudden drops indicate a record being released; an ever increasing" +
		" number indicates that the oldest record is around and preventing GC if > configured GC TTL)",
	Measurement: "Nanoseconds",
	Unit:        metric.Unit_NANOSECONDS,
}

// metricsPollerInterval determines the frequency at which we refresh internal
// metrics.
var metricsPollerInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"spanconfig.kvsubscriber.metrics_poller_interval",
	"the interval at which the spanconfig.kvsubscriber.* metrics are kept up-to-date; set to 0 to disable the mechanism",
	5*time.Second,
)

// KVSubscriber 用于订阅全局范围配置（Span Configuration）的变更。
// 它是 spanconfig.KVSubscriber 接口的具体实现。
//
// 预期的使用方式是启动（Start）一次，之后一个或多个订阅者可以监听更新。
// 在内部，我们对全局范围配置存储（system.span_configurations 表）维护一个 rangefeed，
// 并将其更新应用到内部的 spanconfig.Store 中。该数据结构的只读视图（spanconfig.StoreReader）
// 作为 KVSubscriber 接口的一部分暴露出来。
//
// 直接使用的 Rangefeed 在处理非重叠键的更新时不提供任何顺序保证，而这正是我们所关心的 [1]。
// 因此，我们使用了 rangefeed 缓冲区，累积原始的 rangefeed 更新，并在 rangefeed frontier（前沿）
// 推进时按时间戳顺序批量刷新它们 [2]。如果缓冲区溢出（由实例化 KVSubscriber 时的内存限制决定），
// 旧的 rangefeed 将被关闭并重建一个新的。
//
// 当遇到上述内部错误时，重新建立底层 rangefeed 是安全的。在建立新 rangefeed 并使用初始扫描 [3]
// 的内容填充 spanconfig.Store 时，我们希望保留现有的 spanconfig.StoreReader。
// 丢弃它将意味着要么阻塞所有外部读取器直到新的 spanconfig.StoreReader 被完全填充，
// 要么呈现一个正在填充中的、不一致的 spanconfig.Store 视图。
//
// 对于新 rangefeed，我们的做法是将初始扫描的所有更新路由到一个全新的 spanconfig.Store，
// 一旦初始扫描完成，就在源头将导出的 spanconfig.StoreReader 切换为新的。
// 在初始扫描期间，并发读取器将继续观察到上一个（如果有的话）spanconfig.StoreReader。
// 切换后，它将观察到更新后的源。未来的增量更新也将针对新源。
// 当这种源切换发生时，我们会通知处理器可能需要刷新其对所有配置的视图。
//
// [1]: 对于给定的键 k，其配置可能作为较大范围 S 的一部分存储。如果范围正在分裂，
//
//	S 可能会在同一事务中被删除并替换为子范围 S1...SN。在应用这些更新时，
//	我们需要确保在处理 S1...SN 之前先处理 S 的删除事件。
//
// [2]: 在上面的示例中，删除 S 的配置并添加 S1...SN 的配置，我们希望确保一次性应用
//
//	整套更新——以免暴露中间状态（即 S 的配置已删除但 S1...SN 的配置尚未应用）。
//
// [3]: 当由于底层错误拆除订阅者时，我们也可以捕获一个检查点，以便下次建立订阅者时使用。
//
//	这样我们可以避免对范围配置状态进行完整的初始扫描，只需从现有 spanconfig.Store 离开的地方继续。
//
// KVSubscriber 会监听 system.span_configurations 表的任何 INSERT / UPDATE / DELETE 变更（只要事务成功 commit），通过 Rangefeed 实时推送。
// 它不是“偶尔监听”，而是节点启动后就永久订阅的全局入口。只要表里有记录被修改，它就会立刻收到事件，更新内存缓存，并通知已注册的 handler（split queue、GC queue 等）。
// 下面用真实场景举例说明它具体监听哪些变更、在什么情况下触发：
// 例子1：用户修改表/数据库的 Zone Configuration（最常见）
// SQLALTER TABLE users CONFIGURE ZONE USING
//
//	num_replicas = 5,
//	gc.ttlseconds = 3600,
//	constraints = '[+region=us-east1]';
//
// 触发情况：你执行 ALTER ... CONFIGURE ZONE（或 CREATE TABLE 时带 ZONE），或者修改数据库/索引的 zone config。
// 内部发生什么：AUTO SPAN CONFIG RECONCILIATION job（一直运行的自动 reconciliation job）检测到变化，把 zone config 翻译成 span config，然后 INSERT 或 UPDATE system.span_configurations 表里对应表 span 的那一行。
// KVSubscriber 反应：立刻收到 Rangefeed 事件 → 更新内存 spanconfig.Store → 通知 split queue（看新配置要不要 split range）和 GC queue（应用新的 GC TTL）。
//
// 例子2：设置 Protected Timestamp（备份、CDC、Restore 保护）
//
// 触发情况：
// 执行 BACKUP / RESTORE 时系统自动保护数据；
// 创建带 protected timestamp 的 changefeed；
// 手动调用 protected timestamp API（或内部如 schema change 期间保护）。
//
// 内部发生什么：Protected Timestamp Manager 在对应 span 的配置里写入保护时间戳（gcPolicy.protection 字段），UPDATE 或 INSERT system.span_configurations 表。
// KVSubscriber 反应：收到变更 → 更新内部保护时间戳缓存 → 下次 GC queue 调用 GetProtectionTimestamps 接口时，就能拿到最新保护时间（“这个 span 的数据不能被 GC 掉”），防止意外删除备份/CDC 需要的数据。
//
// 例子3：DROP TABLE / DROP PARTITION / 删除 Tenant
// SQLDROP TABLE users;
// -- 或在多租户集群：DROP TENANT oldtenant;
//
// 触发情况：表、分区、索引被删除，或 tenant 被 drop。
// 内部发生什么：reconciler job 或 schema change 清理对应 span 的配置，DELETE system.span_configurations 表中的行。
// KVSubscriber 反应：收到删除事件 → 内存缓存里移除该 span → 通知 GC queue（现在可以安全清理旧数据）和 split queue。
//
// 例子4：Range Split / Merge 后的配置继承
//
// 触发情况：系统自动 split 一个大 range 时（或 merge）。
// 内部发生什么：新产生的右半 span（或 merge 后的 span）需要继承/调整 config，reconciler 会 INSERT 一条新记录（或 UPDATE）。
// KVSubscriber 反应：收到后通知 split queue 和其他 handler，确保新 range 立刻使用正确的 replication/GC 配置。
type KVSubscriber struct {
	// fallback 是在找不到特定配置时使用的默认范围配置。
	fallback roachpb.SpanConfig
	// knobs 用于测试目的，允许注入特定的行为。
	knobs *spanconfig.TestingKnobs
	// settings 提供对集群设置的访问。
	settings *cluster.Settings

	// rfc 是底层 rangefeed 缓存的监听器。
	rfc *rangefeedcache.Watcher[*BufferEvent]

	mu struct { // 序列化 Start 方法和外部线程之间的访问
		syncutil.RWMutex
		// lastUpdated 记录了缓存最后一次同步到的 HLC 时间戳。
		lastUpdated hlc.Timestamp
		// internal 是由 KVSubscriber 维护的内部 spanconfig.Store。
		// 该存储的只读视图通过接口公开。在重新订阅时，会填充一个新的
		// spanconfig.Store，而公开的 StoreReader 看起来是静态的。
		// 一旦追赶上进度，新的 Store 就会被切换进来。
		internal spanconfig.Store
		// handlers 存储了所有注册的更新处理器。
		handlers []handler
	}

	// clock 用于获取当前时间或 HLC 时间戳。
	clock *hlc.Clock
	// metrics 记录该订阅者的各项运行指标。
	metrics *Metrics

	// boundsReader 提供对全局 SpanConfigBounds 状态的访问。
	boundsReader spanconfigstore.BoundsReader
}

var _ spanconfig.KVSubscriber = &KVSubscriber{}

// Metrics are the Metrics associated with an instance of the
// KVSubscriber.
type Metrics struct {
	// UpdateBehindNanos is the difference between the current time and when the
	// last update was received by the KVSubscriber. This metric should be
	// interpreted as a measure of the KVSubscribers' staleness.
	UpdateBehindNanos *metric.Gauge
	// ProtectedRecordCount is total number of protected timestamp records, as
	// seen by KV.
	ProtectedRecordCount *metric.Gauge
	// OldestProtectedRecord is  between the current time and the oldest
	// protected timestamp.
	OldestProtectedRecordNanos *metric.Gauge
}

func makeKVSubscriberMetrics() *Metrics {
	return &Metrics{
		UpdateBehindNanos:          metric.NewGauge(updateBehindNanos),
		ProtectedRecordCount:       metric.NewGauge(protectedRecordCount),
		OldestProtectedRecordNanos: metric.NewGauge(oldestProtectedRecordNanos),
	}
}

// MetricStruct implements the metric.Struct interface.
func (k *Metrics) MetricStruct() {}

var _ metric.Struct = &Metrics{}

// spanConfigurationsTableRowSize is an estimate of the size of a single row in
// the system.span_configurations table (size of start/end key, and size of a
// marshaled span config proto). The value used here was pulled out of thin air
// -- it only serves to coarsely limit how large the KVSubscriber's underlying
// rangefeed buffer can get.
const spanConfigurationsTableRowSize = 5 << 10 // 5 KB

// New instantiates a KVSubscriber.
func New(
	clock *hlc.Clock,
	rangeFeedFactory *rangefeed.Factory,
	spanConfigurationsTableID uint32,
	bufferMemLimit int64,
	fallback roachpb.SpanConfig,
	settings *cluster.Settings,
	boundsReader spanconfigstore.BoundsReader,
	knobs *spanconfig.TestingKnobs,
	registry *metric.Registry,
) *KVSubscriber {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	//计算监听范围
	spanConfigTableStart := keys.SystemSQLCodec.IndexPrefix(
		spanConfigurationsTableID,
		keys.SpanConfigurationsTablePrimaryKeyIndexID,
	)
	spanConfigTableSpan := roachpb.Span{
		Key:    spanConfigTableStart,
		EndKey: spanConfigTableStart.PrefixEnd(),
	}
	spanConfigStore := spanconfigstore.New(fallback, settings, boundsReader, knobs)
	s := &KVSubscriber{
		fallback:     fallback,
		knobs:        knobs,
		settings:     settings,
		clock:        clock,
		boundsReader: boundsReader,
	}
	var rfCacheKnobs *rangefeedcache.TestingKnobs
	if knobs != nil {
		rfCacheKnobs, _ = knobs.KVSubscriberRangeFeedKnobs.(*rangefeedcache.TestingKnobs)
	}
	//创建 rangefeedcache.Watcher
	s.rfc = rangefeedcache.NewWatcher(
		"spanconfig-subscriber",
		clock, rangeFeedFactory,
		//**具体数值示例**：
		//- 假设 `bufferMemLimit = 64MB`（典型值）
		//- `bufferSize = 64 * 1024 * 1024 / 5120 = 13,107` 个事件
		//- 这意味着在两次 frontier bump 之间（~3s），buffer 最多容纳 13,107 个 span config 变更事件
		//- 注释明确说明 `spanConfigurationsTableRowSize` 是一个粗略估计值，仅用于限制 buffer 大小
		//
		//**一旦 buffer 溢出**：rangefeedcache.Watcher 返回错误，
		//触发 retry loop 重建整个 rangefeed（包括初始扫描）。
		int(bufferMemLimit/spanConfigurationsTableRowSize),
		[]roachpb.Span{spanConfigTableSpan},
		true, //  // 需要 PrevValue 来解码删除事件
		true, //  初始扫描也需要行时间戳
		NewSpanConfigDecoder().TranslateEvent,
		s.handleUpdate,
		rfCacheKnobs,
	)
	s.mu.internal = spanConfigStore
	s.metrics = makeKVSubscriberMetrics()
	if registry != nil {
		registry.AddMetricStruct(s.metrics)
	}
	return s
}

// Start establishes a subscription (internally: rangefeed) over the global
// store of span configs. It fires off an async task to do so, re-establishing
// internally when retryable errors[1] occur and stopping only when the surround
// stopper is quiescing or the context canceled. All installed handlers are
// invoked in the single async task thread.
//
// [1]: It's possible for retryable errors to occur internally, at which point
//
//	we tear down the existing subscription and re-establish another. When
//	unsubscribed, the exposed spanconfig.StoreReader continues to be
//	readable (though no longer incrementally maintained -- the view gets
//	progressively staler overtime). Existing handlers are kept intact and
//	notified when the subscription is re-established. After re-subscribing,
//	the exported StoreReader will be up-to-date and continue to be
//	incrementally maintained.
//
// Start 启动对全局范围配置（Span Config）变更的订阅。
//
// 核心作用：
// 1. 建立订阅：通过内部的 Rangefeed 机制，实时监听系统表（system.span_configurations）中的配置变更。
// 2. 指标监控：启动后台任务定期刷新监控指标（如更新延迟、受保护的时间戳数量等）。
// 3. 异步处理：所有的指标更新和底层的 Rangefeed 处理都在后台协程中异步执行，确保不阻塞主流程。
//
// 例子：
// - 集群中某个表的过期策略（GCPolicy）发生了变化。
// - `KVSubscriber` 会通过底层的 `rangefeed` 捕获到这一变化，并将其更新到内存中的 `spanconfig.Store`。
// - 此时，已订阅的各种处理器（Handlers）会被通知，从而让该变化在全节点生效。
func (s *KVSubscriber) Start(ctx context.Context, stopper *stop.Stopper) error {
	// 1. 启动用于更新指标（Metrics）的异步后台任务。
	if err := stopper.RunAsyncTask(ctx, "kvsubscriber-metrics",
		func(ctx context.Context) {
			// 创建用于感知指标刷新频率设置（metricsPollerInterval）变更的通道。
			settingChangeCh := make(chan struct{}, 1)
			metricsPollerInterval.SetOnChange(
				&s.settings.SV, func(ctx context.Context) {
					select {
					case settingChangeCh <- struct{}{}:
					default:
					}
				})

			var timer timeutil.Timer
			defer timer.Stop()

			// 指标刷新主循环。
			for {
				// 获取最新的指标刷新间隔设置。
				interval := metricsPollerInterval.Get(&s.settings.SV)
				if interval > 0 {
					timer.Reset(interval)
				} else {
					// 如果设置为 0，则停止刷新机制。
					timer.Stop()
				}
				select {
				case <-timer.C:
					// 2. 定时器触发，执行真实的指标更新逻辑（抓取当前延迟和受保护记录数）。
					s.updateMetrics(ctx)
					continue

				case <-settingChangeCh:
					// 3. 设置发生变化，立即进入下一轮循环以应用新的定时器间隔。
					continue

				case <-stopper.ShouldQuiesce():
					// 收到系统优雅退出信号。
					return
				}
			}
		}); err != nil {
		return err
	}

	// 4. 启动核心的 Rangefeed 订阅。
	// 这行代码将真正连接到 KV 层，开始监听 system.span_configurations 表的变化。
	return rangefeedcache.Start(ctx, stopper, s.rfc, nil /* onError */)
}

func (s *KVSubscriber) updateMetrics(ctx context.Context) {
	protectedTimestamps, lastUpdated, err := s.GetProtectionTimestamps(ctx, keys.EverythingSpan)
	if err != nil {
		log.Dev.Errorf(ctx, "while refreshing kvsubscriber metrics: %v", err)
		return
	}

	earliestTS := hlc.Timestamp{}
	for _, protectedTimestamp := range protectedTimestamps {
		if earliestTS.IsEmpty() || protectedTimestamp.Less(earliestTS) {
			earliestTS = protectedTimestamp
		}
	}
	//| 指标 | 含义 | 告警场景 |
	//| --- | --- | --- |
	//| `UpdateBehindNanos` | `now - lastUpdated` | 持续增长 → rangefeed 断开，配置不再更新 |
	//| `ProtectedRecordCount` | 全局受保护时间戳数 | 过多 → 可能阻止 GC |
	//| `OldestProtectedRecordNanos` | `now - 最老的受保护时间戳` | 持续增长 → 某个备份/CDC 任务挂起 |
	now := s.clock.PhysicalTime()
	s.metrics.ProtectedRecordCount.Update(int64(len(protectedTimestamps)))
	s.metrics.UpdateBehindNanos.Update(now.Sub(lastUpdated.GoTime()).Nanoseconds())
	if earliestTS.IsEmpty() {
		s.metrics.OldestProtectedRecordNanos.Update(0)
	} else {
		s.metrics.OldestProtectedRecordNanos.Update(now.Sub(earliestTS.GoTime()).Nanoseconds())
	}
}

// Subscribe installs a callback that's invoked with whatever span may have seen
// a config update.
func (s *KVSubscriber) Subscribe(fn func(context.Context, roachpb.Span)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.handlers = append(s.mu.handlers, handler{fn: fn})
}

// LastUpdated is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) LastUpdated() hlc.Timestamp {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.lastUpdated
}

// NeedsSplit is part of the spanconfig.KVSubscriber interface.
// 判断 [start, end) 范围内是否有 span config 边界。如果有，说明当前 range 跨越了不同的配置区域，需要分裂。
func (s *KVSubscriber) NeedsSplit(ctx context.Context, start, end roachpb.RKey) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.internal.NeedsSplit(ctx, start, end)
}

// ComputeSplitKey is part of the spanconfig.KVSubscriber interface.
// 计算 [start, end) 范围内的最佳分裂点。返回的 key 是 span config 边界的位置。
func (s *KVSubscriber) ComputeSplitKey(
	ctx context.Context, start, end roachpb.RKey,
) (roachpb.RKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.internal.ComputeSplitKey(ctx, start, end)
}

// GetSpanConfigForKey is part of the spanconfig.KVSubscriber interface.
// 获取指定 key 的 span config。同时返回该 config 适用的 span 范围（调用者可以据此判断请求是否完全在一个 config 范围内）。
func (s *KVSubscriber) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, roachpb.Span, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.internal.GetSpanConfigForKey(ctx, key)
}

// GetProtectionTimestamps is part of the spanconfig.KVSubscriber interface.
// 用于查询某个 span 范围内所有活跃的受保护时间戳
func (s *KVSubscriber) GetProtectionTimestamps(
	ctx context.Context, sp roachpb.Span,
) (protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, _ error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.mu.internal.ForEachOverlappingSpanConfig(ctx, sp,
		func(sp roachpb.Span, config roachpb.SpanConfig) error {
			for _, protection := range config.GCPolicy.ProtectionPolicies {
				// If the current span is a subset of the key space we exclude from full
				// cluster backups, then we ignore it. This avoids placing a protected
				// timestamp and hold up GC on spans not needed for backup (i.e.
				// NodeLiveness, Timeseries). These spans tend to be high churn,
				// accumulating high amounts of MVCC garbage. Placing a PTS on these
				// spans can thus be detrimental.
				// [过滤1] 排除不需要备份的系统 span
				//`ExcludeFromBackupSpan` 包含 **NodeLiveness** 和 **Timeseries** 等系统 span。这些 span 的特点是：
				//
				//- **高写入频率**：NodeLiveness 每 4.5s 心跳一次，Timeseries 每 10s 写入指标
				//- **不需要备份**：集群重启后会自动重建
				//- **MVCC 垃圾积累快**：如果 PTS 阻止了 GC，这些 span 的数据膨胀极快
				if keys.ExcludeFromBackupSpan.Contains(sp) {
					continue
				}
				// If the SpanConfig that applies to this span indicates that the span
				// is going to be excluded from backup, and the protection policy was
				// written by a backup, then ignore it. This prevents the
				// ProtectionPolicy from holding up GC over the span.
				// [过滤2] 排除备份写入的 PTS（如果 span 本身被排除出备份）
				//这是一个**双向协商**机制：
				//- Span 侧：`config.ExcludeDataFromBackup = true` 表示”我不需要被备份”
				//- PTS 侧：`protection.IgnoreIfExcludedFromBackup = true` 表示”我是备份创建的，如果 span 不需要备份就忽略我”
				//
				//只有两个条件同时满足时才跳过。非备份创建的 PTS（如 CDC changefeed 的 PTS，其 `IgnoreIfExcludedFromBackup = false`）不受此影响。
				if config.ExcludeDataFromBackup && protection.IgnoreIfExcludedFromBackup {
					continue
				}
				protectionTimestamps = append(protectionTimestamps, protection.ProtectedTimestamp)
			}
			return nil
		}); err != nil {
		return nil, hlc.Timestamp{}, err
	}

	return protectionTimestamps, s.mu.lastUpdated, nil
}

func (s *KVSubscriber) handleUpdate(ctx context.Context, u rangefeedcache.Update[*BufferEvent]) {
	switch u.Type {
	case rangefeedcache.CompleteUpdate:
		s.handleCompleteUpdate(ctx, u.Timestamp, u.Events)
	case rangefeedcache.IncrementalUpdate:
		s.handlePartialUpdate(ctx, u.Timestamp, u.Events)
	}
}

func (s *KVSubscriber) handleCompleteUpdate(
	ctx context.Context, ts hlc.Timestamp, events []*BufferEvent,
) {
	// [1] 在锁外创建全新的 Store 并填充
	freshStore := spanconfigstore.New(s.fallback, s.settings, s.boundsReader, s.knobs)
	for _, ev := range events {
		//初始扫描可能包含上万条span config 记录。Apply 操作涉及区间树的插入/合并，每次 Apply 的复杂度约 O(log N)。
		// 如果加锁，持锁期间所有读请求（GetSpanConfigForKey, NeedsSplit 等）都会阻塞，
		//直接影响 range 的分裂和 GC 决策。
		freshStore.Apply(ctx, ev.Update) //应用变更事件
	}
	// [2] 在锁内原子替换 Store 指针 + 更新时间戳 + 获取 handlers 快照
	handlers := func() []handler {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.mu.internal = freshStore // ★ 原子替换
		s.setLastUpdatedLocked(ts)
		return s.mu.handlers // 返回 handler 快照
	}()

	for i := range handlers {
		handler := &handlers[i]                  // mutated by invoke// 取指针，因为 invoke 会修改 initialized 字段
		handler.invoke(ctx, keys.EverythingSpan) // 全量通知
	}
}

func (s *KVSubscriber) setLastUpdatedLocked(ts hlc.Timestamp) {
	s.mu.lastUpdated = ts
	nanos := timeutil.Since(s.mu.lastUpdated.GoTime()).Nanoseconds()
	s.metrics.UpdateBehindNanos.Update(nanos)
}

func (s *KVSubscriber) handlePartialUpdate(
	ctx context.Context, ts hlc.Timestamp, events []*BufferEvent,
) {
	// The events we've received from the rangefeed buffer are sorted in
	// increasing timestamp order. However, any updates with the same timestamp
	// may be ordered arbitrarily. That's okay if they don't overlap. However, if
	// they do overlap, the assumption is that an overlapping delete should be
	// ordered before an addition it overlaps with -- not doing would cause the
	// addition to get clobbered by the deletion, which will result in the store
	// having missing span configurations. As such, we re-sort the list of events
	// before applying it to our store, using Deletion() as a tie-breaker when
	// timestamps are equal.
	// [1] 排序：同时间戳的事件中，删除排在添加之前
	sort.Slice(events, func(i, j int) bool {
		switch events[i].Timestamp().Compare(events[j].Timestamp()) {
		case -1: // ts(i) < ts(j)
			return true
		case 1: // ts(i) > ts(j)
			return false
		case 0: // ts(i) == ts(j); deletions sort before additions
			return events[i].Deletion() // no need to worry about the sort being stable
		default:
			panic("unexpected")
		}
	})
	handlers := func() []handler {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, ev := range events {
			// NB: Even though the StoreWriter can apply a batch of updates
			// atomically, the updates need to be non-overlapping. That's not the case
			// here because we can have deletion events followed by additions for
			// overlapping spans.
			s.mu.internal.Apply(ctx, ev.Update) // 逐个 Apply，非批量
		}
		s.setLastUpdatedLocked(ts)
		return s.mu.handlers
	}()

	for i := range handlers {
		handler := &handlers[i] // mutated by invoke
		for _, ev := range events {
			target := ev.Update.GetTarget()
			handler.invoke(ctx, target.KeyspaceTargeted())
		}
	}
}

type handler struct {
	initialized bool // 是否已完成首次全量通知
	fn          func(ctx context.Context, update roachpb.Span)
}

func (h *handler) invoke(ctx context.Context, update roachpb.Span) {
	if !h.initialized {
		h.fn(ctx, keys.EverythingSpan) // 首次调用：全量通知
		h.initialized = true

		if update.Equal(keys.EverythingSpan) { // 优化：如果 update 本身就是全量，不重复调用
			return // we can opportunistically avoid re-invoking with the same update
		}
	}

	h.fn(ctx, update)
}

type BufferEvent struct {
	spanconfig.Update               // 嵌入：包含 Target 和 SpanConfig
	ts                hlc.Timestamp // 事件的 MVCC 时间戳
}

// Timestamp implements the rangefeedbuffer.Event interface.
func (w *BufferEvent) Timestamp() hlc.Timestamp {
	return w.ts
}

var _ rangefeedbuffer.Event = &BufferEvent{}
