// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyvissubscriber

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// keyvissubscriber manages the rangefeed on the SpanStatsTenantBoundariesTable.
// When there is a rangefeed update, the update is decoded into a
// keyvispb.UpdateBoundariesRequest and is passed to the SpanStatsCollector
// via handleBoundaryUpdate.

// Start starts the rangefeed on the SpanStatsTenantBoundariesTable.
// When a new event is received, the event is decoded and delegated to handleBoundaryUpdate.
func Start(
	// ctx 控制启动和 rangefeed 运行期间的生命周期。
	ctx context.Context,
	// stopper 管理后台任务退出；server 关闭时 rangefeed 会随之停止。
	stopper *stop.Stopper,
	// db 是 KV DB，rangefeed 通过它订阅底层 KV span 的变化。
	db *kv.DB,
	// settings 提供 rangefeed factory 需要的 cluster settings。
	settings *cluster.Settings,
	// sysTableResolver 用来把系统表名解析成当前集群里的 table ID。
	sysTableResolver catalog.SystemTableIDResolver,
	// initialTimestamp 表示 rangefeed 从哪个 HLC 时间戳开始监听变化。
	initialTimestamp hlc.Timestamp,
	// handleBoundaryUpdate 是上层传入的回调；解码出边界更新后交给它处理。
	handleBoundaryUpdate func(update *keyvispb.UpdateBoundariesRequest),
) error {

	// 解析 system.span_stats_tenant_boundaries 表的 descriptor ID。
	// 这里用 RunIdempotentWithRetryEx 是因为 server 启动阶段系统表可能还没完全可用，需要可重试。
	tableID, err := startup.RunIdempotentWithRetryEx(ctx, stopper.ShouldQuiesce(),
		"obs lookup system table",
		func(ctx context.Context) (descpb.ID, error) {
			// 按系统表名查出该表的 ID。
			return sysTableResolver.LookupSystemTableID(
				ctx, systemschema.SpanStatsTenantBoundariesTable.GetName())
		})
	// 系统表 ID 查找失败，Start 不能继续。
	if err != nil {
		return err
	}

	// 根据 tableID 构造该系统表在 KV 层的 table prefix。
	tablePrefix := keys.SystemSQLCodec.TablePrefix(uint32(tableID))
	// 构造覆盖整张 system.span_stats_tenant_boundaries 表的 KV span。
	tableSpan := roachpb.Span{
		// span 起点是表前缀。
		Key: tablePrefix,
		// span 终点是表前缀的 PrefixEnd，表示覆盖这个表的全部 KV key。
		EndKey: tablePrefix.PrefixEnd(),
	}

	// 取出系统表的 public columns，后面按这些列的 schema 解码 value tuple。
	columns := systemschema.SpanStatsTenantBoundariesTable.PublicColumns()
	// 创建 value-side decoder，用来解码表 value 中的非主键列。
	decoder := valueside.MakeDecoder(columns)
	// DatumAlloc 用来在 SQL datum 解码过程中复用/分配对象。
	da := &tree.DatumAlloc{}

	// decodeRow decodes a row of the system.span_stats_tenant_boundaries table.
	decodeRow := func(
		// ctx 控制本次 KV 行解码的上下文。
		ctx context.Context,
		// kv 是 rangefeed 推送过来的某一行 KV 更新。
		kv *kvpb.RangeFeedValue,
	) (*roachpb.TenantID, *keyvispb.UpdateBoundariesRequest, error) {

		// First, decode the tenant_id column.
		// tenant_id 是主键列，编码在 KV key 里；先准备它的 SQL 类型。
		colTypes := []*types.T{columns[0].GetType()}
		// tenantIDRow 只存一个 EncDatum，用来接收 key 里解出来的 tenant_id。
		tenantIDRow := make([]rowenc.EncDatum, 1)
		// 从 KV key 中解码 tenant_id 主键列。
		if _, err := rowenc.DecodeIndexKey(
			// 系统表使用 SystemSQLCodec 编码。
			keys.SystemSQLCodec,
			// 解码结果写入 tenantIDRow。
			tenantIDRow,
			// 没有显式列方向，按默认升序解码。
			nil, /* colDirs */
			// rangefeed 事件里的 KV key。
			kv.Key,
		); err != nil {
			return nil, nil, err
		}
		// 把 EncDatum 真正解码成 tree.Datum，方便后面转成 TenantID。
		if err := tenantIDRow[0].EnsureDecoded(colTypes[0], da); err != nil {
			return nil, nil, err
		}

		// Next, decode the boundaries column, which is a boundary encoded proto.
		// rangefeed value 存的是 SQL row 的 value-side tuple。
		bytes, err := kv.Value.GetTuple()
		// 如果 value 不是 tuple 或读取失败，返回错误。
		if err != nil {
			return nil, nil, err
		}

		// 按表 columns schema 解码 value tuple，得到每个 value-side 列的 Datum。
		datums, err := decoder.Decode(da, bytes)
		// value tuple 解码失败，返回错误。
		if err != nil {
			return nil, nil, err
		}

		// 准备用来接收 boundaries proto 的结构体。
		decoded := keyvispb.UpdateBoundariesRequest{}
		// datums[1] 是 boundaries 列，里面存的是序列化后的 UpdateBoundariesRequest bytes。
		updateEncoded := []byte(tree.MustBeDBytes(datums[1]))

		// 把 boundaries 列里的 bytes 反序列化成 UpdateBoundariesRequest proto。
		if err := protoutil.Unmarshal(updateEncoded, &decoded); err != nil {
			return nil, nil, err
		}

		// 把 key 中解出来的 tenant_id datum 转成 roachpb.TenantID。
		tID, err := roachpb.MakeTenantID(uint64(tree.MustBeDInt(tenantIDRow[0].Datum)))
		// tenant_id 非法时返回错误。
		if err != nil {
			return nil, nil, err
		}
		// 返回该行所属 tenant 和解码后的边界更新请求。
		return &tID, &decoded, nil
	}

	// 创建 rangefeed factory，用于启动对 tableSpan 的 rangefeed 订阅。
	f, err := rangefeed.NewFactory(stopper, db, settings, nil)
	// rangefeed factory 创建失败，Start 不能继续。
	if err != nil {
		return err
	}
	// 启动 rangefeed，监听 system.span_stats_tenant_boundaries 表对应的 KV span。
	_, err = f.RangeFeed(
		// rangefeed 生命周期上下文。
		ctx,
		// rangefeed 名称，用于日志、pprof、诊断。
		"tenant-boundaries-watcher",
		// 监听的 KV spans；这里只有一张系统表的完整 span。
		[]roachpb.Span{tableSpan},
		// 从 initialTimestamp 开始接收变化事件。
		initialTimestamp,
		// 每收到一条 rangefeed value 事件，就执行这个回调。
		func(ctx context.Context, kv *kvpb.RangeFeedValue) {
			// 把 KV 行更新解码成 tenantID 和边界更新请求。
			tID, update, err := decodeRow(ctx, kv)
			// 解码失败只记录 warning，不让整个 rangefeed 因为单行坏数据退出。
			if err != nil {
				log.Dev.Warningf(ctx,
					"tenant boundary decoding failed with error: %v", err)
				return
			}

			// A row in the system.span_stats_tenant_boundaries table might
			// belong to a secondary tenant, but we don't support that right
			// now. During a rolling update, a node that hasn't been updated
			// should just no-op if it receives a row for a secondary tenant.
			// 当前只支持 system tenant 的 key visualizer 边界更新。
			if *tID == roachpb.SystemTenantID {
				// 把解码出的边界更新交给上层 collector/cache 处理。
				handleBoundaryUpdate(update)
			} else {
				// secondary tenant 的边界更新当前不处理，只记录 warning。
				log.Dev.Warningf(ctx,
					"can not update boundaries for secondary tenants")
			}
		},
	)
	// 返回 rangefeed 启动结果；成功时 err 为 nil。
	return err
}
