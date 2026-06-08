// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	prometheus "github.com/prometheus/client_model/go"
)

// defaultFlushInterval specifies a default for the amount of time an ingester
// will go before flushing its contents to the registry.
const defaultFlushInterval = time.Millisecond * 500

const forceFlushTransactionFingerprintId = appstatspb.TransactionFingerprintID(math.MaxUint64)

// Metrics holds running measurements of various ingester-related runtime stats.
type Metrics struct {
	// NumProcessed tracks how many items have been processed.
	NumProcessed *metric.Counter

	// QueueSize tracks the current number of items queued to be processed.
	QueueSize *metric.Gauge
}

// MetricStruct marks Metrics for automatic member metric registration.
func (Metrics) MetricStruct() {}

var _ metric.Struct = Metrics{}

// NewIngesterMetrics builds a new instance of our Metrics struct.
func NewIngesterMetrics() Metrics {
	return Metrics{
		NumProcessed: metric.NewCounter(metric.Metadata{
			Name:        "sql.stats.ingester.num_processed",
			Help:        "Number of items processed by the SQL stats ingester",
			Measurement: "Items",
			Unit:        metric.Unit_COUNT,
			MetricType:  prometheus.MetricType_COUNTER,
		}),
		QueueSize: metric.NewGauge(metric.Metadata{
			Name:        "sql.stats.ingester.queue_size",
			Help:        "Current number of items queued in the SQL stats ingester",
			Measurement: "Items",
			Unit:        metric.Unit_COUNT,
			MetricType:  prometheus.MetricType_GAUGE,
		}),
	}
}

type SQLStatsSink interface {
	// ObserveTransaction is called by the ingester to pass along a transaction event (possibly nil) and its
	// statementsBySessionID.
	// Note that the sink should transform the transaction and statementsBySessionID into the appropriate format
	// as these objects will be returned to the pool.
	ObserveTransaction(ctx context.Context, transaction *sqlstats.RecordedTxnStats, statements []*sqlstats.RecordedStmtStats)
}

// SQLStatsIngester collects and buffers statements and transactions per session
// before passing them to the sinks when we receive complete information for a transaction.
// Built around contentionutils.ConcurrentBufferGuard.
type SQLStatsIngester struct {
	guard struct {
		*contentionutils.ConcurrentBufferGuard
		eventBuffer *eventBuffer
	}

	opts struct {
		// noTimedFlush prevents time-triggered flushes from being scheduled.
		noTimedFlush bool
		// flushInterval is an optional override flush interval
		// a value of zero will be set to the 500ms default.
		flushInterval time.Duration
	}

	// We buffer ingested statementsBySessionID by session id.
	statementsBySessionID map[clusterunique.ID]*statementBuf
	resetStatementsBuf    atomic.Bool

	sinks []SQLStatsSink

	eventBufferCh chan eventBufChPayload

	closeCh chan struct{}

	// syncStatsTestingCh is a channel used to synchronize the writing
	// of stats on transaction execution. This channel is only set when
	// SynchronousSQLStats on sqlstats.TestingKnobs is set to true.
	syncStatsTestingCh chan struct{}

	settings *cluster.Settings

	testingKnobs *sqlstats.TestingKnobs

	metrics Metrics
}

type eventBufChPayload struct {
	resetStatementsBuf bool
	events             *eventBuffer
}

type statementBuf []*sqlstats.RecordedStmtStats

func (b *statementBuf) release() {
	for i, n := 0, len(*b); i < n; i++ {
		(*b)[i] = nil
	}
	*b = (*b)[:0]
	statementsBufPool.Put(b)
}

var statementsBufPool = sync.Pool{
	New: func() interface{} {
		return new(statementBuf)
	},
}

// SQLStatsIngester buffers the "events" it sees (via BufferStatement
// and BufferTransaction) and passes them along to the underlying registry
// once its buffer is full. (Or once a timeout has passed, for low-traffic
// clusters and tests.)
//
// The bufferSize was set at 8192 after experimental micro-benchmarking ramping
// up the number of goroutines writing through the ingester concurrently.
// Performance was deemed acceptable under 10,000 concurrent goroutines.
const bufferSize = 8192

type eventBuffer [bufferSize]event

var eventBufferPool = sync.Pool{
	New: func() interface{} { return new(eventBuffer) },
}

// event is a single event that can be observed by the ingester.
// At most one of transaction or statement will be non-nil.
type event struct {
	sessionID   clusterunique.ID
	transaction *sqlstats.RecordedTxnStats
	statement   *sqlstats.RecordedStmtStats
	forceFlush  bool
}

type BufferOpt func(i *SQLStatsIngester)

// WithoutTimedFlush prevents the ConcurrentBufferIngester from performing
// timed flushes to the underlying registry. Generally only useful for
// testing purposes.
func WithoutTimedFlush() BufferOpt {
	return func(i *SQLStatsIngester) {
		i.opts.noTimedFlush = true
	}
}

// WithFlushInterval allows for the override of the default flush interval
func WithFlushInterval(intervalMS int) BufferOpt {
	return func(i *SQLStatsIngester) {
		i.opts.flushInterval = time.Millisecond * time.Duration(intervalMS)
	}
}

func (i *SQLStatsIngester) Start(ctx context.Context, stopper *stop.Stopper, opts ...BufferOpt) {
	for _, opt := range opts {
		opt(i)
	}
	syncForTesting := i.testingKnobs != nil && i.testingKnobs.SynchronousSQLStats

	_ = stopper.RunAsyncTask(ctx, "sql-stats-ingester", func(ctx context.Context) {

		for {
			select {
			case payload := <-i.eventBufferCh:
				i.ingest(ctx, payload.events) // note that ingest clears the buffer
				if payload.resetStatementsBuf {
					i.statementsBySessionID = make(map[clusterunique.ID]*statementBuf)
				}
				eventBufferPool.Put(payload.events)

				if syncForTesting {
					i.syncStatsTestingCh <- struct{}{}
				}

				if i.testingKnobs != nil && i.testingKnobs.OnIngesterFlush != nil {
					i.testingKnobs.OnIngesterFlush()
				}
			case <-stopper.ShouldQuiesce():
				if i.syncStatsTestingCh != nil {
					close(i.syncStatsTestingCh)
				}
				close(i.closeCh)
				return
			}
		}
	})

	if !syncForTesting && !i.opts.noTimedFlush {
		flushInterval := i.opts.flushInterval
		if flushInterval == 0 {
			flushInterval = defaultFlushInterval
		}
		// This task eagerly flushes partial buffers into the channel, to avoid
		// delays identifying insights in low-traffic clusters and tests.
		_ = stopper.RunAsyncTask(ctx, "insights-ingester-flush", func(ctx context.Context) {
			ticker := time.NewTicker(flushInterval)

			for {
				select {
				case <-ticker.C:
					i.guard.ForceSync()
				case <-stopper.ShouldQuiesce():
					ticker.Stop()
					return
				}
			}
		})
	}
}

// Clear flushes the underlying buffer, and signals the underlying registry
// to clear any remaining cached data afterward. This is an async operation.
func (i *SQLStatsIngester) Clear() {
	i.guard.ForceSyncExec(func() {
		// Our flush function defined on the guard is responsible for setting resetStatementsBuf back to 0.
		i.resetStatementsBuf.Store(true)
	})
}

func (i *SQLStatsIngester) ingest(ctx context.Context, events *eventBuffer) {
	// 遍历这一批 eventBuffer 里的事件。
	for idx, e := range events {
		// eventBuffer 是固定长度数组，不是 slice。
		// 没写满的位置会是零值 event{}。
		// 遇到第一个空 event，说明后面没有有效事件了，停止处理。
		if e == (event{}) {
			break
		}

		// 如果这个事件是 statement 统计事件。
		if e.statement != nil {
			// 把 statement 按 SessionID 缓存起来。
			// 注意：这里还没有写入下游，因为它要等 transaction 一起到来。
			i.processStatement(e.statement)

			// 已处理事件数 +1。
			i.metrics.NumProcessed.Inc(1)

			// 队列大小 -1，因为这个事件已经被消费了。
			i.metrics.QueueSize.Dec(1)

			// 如果这个 statement 是在 outer transaction 下执行的，
			// 它不会有普通 transaction event 可以关联。
			// 所以这里直接把这个 session 的 statement 刷给下游，
			// transaction 参数传 nil。
			if e.statement.UnderOuterTxn {
				i.flushBuffer(ctx, e.statement.SessionID, nil)
			}

			// 如果这个事件是 transaction 统计事件。
		} else if e.transaction != nil {
			// transaction 到了，说明这个 session 的事务完整了。
			// 把之前缓存的 statements 取出来，和这个 transaction 一起发给 sinks。
			i.flushBuffer(ctx, e.transaction.SessionID, e.transaction)

			// 已处理事件数 +1。
			i.metrics.NumProcessed.Inc(1)

			// 队列大小 -1。
			i.metrics.QueueSize.Dec(1)

			// 既不是 statement，也不是 transaction。
			// 这种 event 用来表示控制信号：force flush 或 clear session。
		} else {
			// 如果是强制 flush 信号。
			if e.forceFlush {
				// 只把这个 session 已缓存的 statements 刷出去。
				i.flushStatementsOnly(ctx, e.sessionID)
			} else {
				i.clearSession(e.sessionID)
			}
			i.metrics.NumProcessed.Inc(1)
			i.metrics.QueueSize.Dec(1)
		}
		events[idx] = event{}
	}
}

func (i *SQLStatsIngester) RecordStatement(statement *sqlstats.RecordedStmtStats) {
	i.BufferStatement(statement)
	if i.testingKnobs != nil && i.testingKnobs.SynchronousSQLStats {
		// Flush buffer and wait for the stats ingester to finish writing.
		i.guard.ForceSync()
		<-i.syncStatsTestingCh
	}
}

func (i *SQLStatsIngester) RecordTransaction(transaction *sqlstats.RecordedTxnStats) {
	i.BufferTransaction(transaction)

	if i.testingKnobs != nil && i.testingKnobs.SynchronousSQLStats {
		// Flush buffer and wait for the stats ingester to finish writing.
		i.guard.ForceSync()
		<-i.syncStatsTestingCh
	}
}

func (i *SQLStatsIngester) BufferStatement(statement *sqlstats.RecordedStmtStats) {
	if i.testingKnobs != nil && i.testingKnobs.IngesterStmtInterceptor != nil {
		i.testingKnobs.IngesterStmtInterceptor(statement.SessionID, statement)
		return
	}

	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			statement: statement,
		}
		i.metrics.QueueSize.Inc(1)
	})
}

func (i *SQLStatsIngester) BufferTransaction(transaction *sqlstats.RecordedTxnStats) {
	if i.testingKnobs != nil && i.testingKnobs.IngesterTxnInterceptor != nil {
		i.testingKnobs.IngesterTxnInterceptor(transaction.SessionID, transaction)
		return
	}

	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			transaction: transaction,
		}
		i.metrics.QueueSize.Inc(1)
	})
}

// ClearSession sends a signal to the underlying registry to clear any cached
// data associated with the given sessionID. This is an async operation.
func (i *SQLStatsIngester) ClearSession(sessionID clusterunique.ID) {
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			sessionID: sessionID,
		}
		i.metrics.QueueSize.Inc(1)
	})
}

// FlushBuffer sends a signal to the underlying registry to flush any cached
// data associated with the given sessionID. This is an async operation.
func (i *SQLStatsIngester) FlushBuffer(sessionID clusterunique.ID) {
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			sessionID:  sessionID,
			forceFlush: true,
		}
		i.metrics.QueueSize.Inc(1)
	})
}

func NewSQLStatsIngester(
	st *cluster.Settings, knobs *sqlstats.TestingKnobs, metrics Metrics, sinks ...SQLStatsSink,
) *SQLStatsIngester {
	i := &SQLStatsIngester{
		// A channel size of 1 is sufficient to avoid unnecessarily
		// synchronizing producer (our clients) and consumer (the underlying
		// registry): moving from 0 to 1 here resulted in a 25% improvement
		// in the micro-benchmarks, but further increases had no effect.
		// Otherwise, we rely solely on the size of the eventBuffer for
		// adjusting our carrying capacity.
		eventBufferCh:         make(chan eventBufChPayload, 1),
		closeCh:               make(chan struct{}),
		statementsBySessionID: make(map[clusterunique.ID]*statementBuf),
		sinks:                 sinks,
		settings:              st,
		testingKnobs:          knobs,
		metrics:               metrics,
	}

	if knobs != nil && knobs.SynchronousSQLStats {
		i.syncStatsTestingCh = make(chan struct{})
	}

	i.guard.eventBuffer = eventBufferPool.Get().(*eventBuffer)
	i.guard.ConcurrentBufferGuard = contentionutils.NewConcurrentBufferGuard(
		func() int64 {
			return bufferSize
		},
		func(currentWriterIndex int64) {
			clearBuf := i.resetStatementsBuf.Load()
			if clearBuf {
				defer func() {
					i.resetStatementsBuf.Store(false)
				}()
			}
			select {
			case i.eventBufferCh <- eventBufChPayload{
				resetStatementsBuf: clearBuf,
				events:             i.guard.eventBuffer,
			}:
			case <-i.closeCh:
			}
			i.guard.eventBuffer = eventBufferPool.Get().(*eventBuffer)
		},
	)
	return i
}

// clearSession removes the session from the registry and releases the
// associated statement buffer.
func (i *SQLStatsIngester) clearSession(sessionID clusterunique.ID) {
	if b, ok := i.statementsBySessionID[sessionID]; ok {
		delete(i.statementsBySessionID, sessionID)
		b.release()

		if i.testingKnobs != nil && i.testingKnobs.OnIngesterSessionClear != nil {
			i.testingKnobs.OnIngesterSessionClear(sessionID)
		}

	}
}

func (i *SQLStatsIngester) processStatement(statement *sqlstats.RecordedStmtStats) {
	b, ok := i.statementsBySessionID[statement.SessionID]
	if !ok {
		b = statementsBufPool.Get().(*statementBuf)
		i.statementsBySessionID[statement.SessionID] = b
	}
	*b = append(*b, statement)
}

// flushBuffer sends the buffered statementsBySessionID and provided transaction
// to the registered sinks. The transaction may be nil.
func (i *SQLStatsIngester) flushBuffer(
	ctx context.Context, sessionID clusterunique.ID, transaction *sqlstats.RecordedTxnStats,
) {
	// 从 statementsBySessionID 中取出当前 sessionID 对应的 statement 缓冲。
	statements, ok := func() (*statementBuf, bool) {
		// 查找这个 session 当前累计的 statement 统计。
		statements, ok := i.statementsBySessionID[sessionID]
		// 如果没有找到，说明这个 session 没有待 flush 的 statement。
		if !ok {
			// 返回 nil 和 false，告诉外层不需要继续处理。
			return nil, false
		}
		// 找到后先从 map 中删除，避免同一批 statements 被重复 flush。
		delete(i.statementsBySessionID, sessionID)
		// 返回取出的 statement 缓冲和 true。
		return statements, true
	}()
	// 如果当前 session 没有 statement 缓冲，直接返回。
	if !ok {
		return
	}
	// 函数结束时把 statement 缓冲释放回对象池，减少内存分配。
	defer statements.release()

	// 如果缓冲存在但里面没有 statement，也没有任何内容需要写给 sink。
	if len(*statements) == 0 {
		return
	}

	// 如果传入了 transaction，说明这次 flush 是由事务统计触发的。
	if transaction != nil {
		// Here we'll set the transaction fingerprint ID for each statement if the
		// below cluster setting is enabled.
		// 读取集群设置，判断是否要把 statement 关联到 transaction fingerprint。
		shouldAssociateWithTxn := AssociateStmtWithTxnFingerprint.Get(&i.settings.SV)
		// These values are only known at the time of the transaction.
		// 遍历当前 session 中这次要 flush 的所有 statements。
		for _, s := range *statements {
			// 如果设置开启，就把 transaction 的 fingerprint ID 写到 statement 上。
			if shouldAssociateWithTxn {
				s.TransactionFingerprintID = transaction.FingerprintID
			}
			// 如果 statement 和 transaction 的 ImplicitTxn 状态一致，就不需要改 statement fingerprint。
			if s.ImplicitTxn == transaction.ImplicitTxn {
				continue
			}
			// We need to recompute the fingerprint ID.
			// 如果 ImplicitTxn 不一致，先把 statement 的 ImplicitTxn 修正为 transaction 的值。
			s.ImplicitTxn = transaction.ImplicitTxn
			// ImplicitTxn 会参与 statement fingerprint 计算，所以这里必须重新构造 fingerprint ID。
			s.FingerprintID = appstatspb.ConstructStatementFingerprintID(
				s.Query, s.ImplicitTxn, s.Database)
		}
	}

	// 把 transaction 和这批 statements 发送给所有注册的 sink。
// 	真实实现主要有两个：
// 1. `sslocal.SQLStats`

// [sql_stats.go](D:/opensource/cockroach/pkg/sql/sqlstats/sslocal/sql_stats.go:188)

// ```go
// func (s *SQLStats) ObserveTransaction(...)
// ```

// 它负责把 statement / transaction 统计写进 SQL stats container，也就是常规 SQL 统计数据。

// 2. `insights.Provider`

// [provider.go](D:/opensource/cockroach/pkg/sql/sqlstats/insights/provider.go:33)

// ```go
// func (p *Provider) ObserveTransaction(...)
// ```

// 它负责把事务统计送进 insights 子系统，用来分析慢 SQL、异常、执行洞察这些信息。
for _, sink := range i.sinks {
		// sink 负责把这批统计写入具体目标，比如 SQL stats container 或 insights。
		sink.ObserveTransaction(ctx, transaction, *statements)
	}
}

// flushStatementsOnly sends the buffered statementsBySessionID to the registered sinks
func (i *SQLStatsIngester) flushStatementsOnly(ctx context.Context, sessionID clusterunique.ID) {
	var statements statementBuf
	if sessionStatements, ok := i.statementsBySessionID[sessionID]; ok {
		defer sessionStatements.release()
		statements = *sessionStatements
		delete(i.statementsBySessionID, sessionID)
	} else {
		// No statements to flush, return early
		return
	}

	// Here we'll set the transaction fingerprint ID for each statement if the
	// below cluster setting is enabled.
	if AssociateStmtWithTxnFingerprint.Get(&i.settings.SV) {
		// These values are only known at the time of the transaction.
		for _, s := range statements {
			s.TransactionFingerprintID = forceFlushTransactionFingerprintId
		}
	}

	for _, sink := range i.sinks {
		sink.ObserveTransaction(ctx, nil, statements)
	}
}
