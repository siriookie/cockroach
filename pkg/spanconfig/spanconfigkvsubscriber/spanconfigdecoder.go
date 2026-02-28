// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigkvsubscriber

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// SpanConfigDecoder decodes rows from system.span_configurations. It's not
// safe for concurrent use.
// SpanConfigDecoder 不是线程安全的（注释明确说明 “not safe for concurrent use”）。
// 这没问题，因为 rangefeed 的 onValue 回调在 rangefeed 的事件处理协程中串行调用。
type SpanConfigDecoder struct {
	alloc   tree.DatumAlloc   // 内存分配器（减少 GC 压力）
	columns []catalog.Column  // system.span_configurations 表的列定义
	decoder valueside.Decoder // KV value → datum 解码器
}

// NewSpanConfigDecoder instantiates a SpanConfigDecoder.
func NewSpanConfigDecoder() *SpanConfigDecoder {
	columns := systemschema.SpanConfigurationsTable.PublicColumns()
	return &SpanConfigDecoder{
		columns: columns,
		decoder: valueside.MakeDecoder(columns),
	}
}

// decode a span config entry given a KV from the
// system.span_configurations table.
// **表结构映射**：
//
// | 列序号 | 列名 | 存储位置 | 解码方式 |
// | --- | --- | --- | --- |
// | 0 | `start_key` | 主键 (KV key) | `DecodeIndexKey` |
// | 1 | `end_key` | Value (column family) | `valueside.Decoder` |
// | 2 | `config` | Value (column family) | `protoutil.Unmarshal` |
//
// **为什么 `start_key` 在 key 中而 `end_key` 在 value 中**：
// `start_key` 是主键列，CockroachDB 按照主键编码 KV key。`end_key`
// 和 `config` 是非主键列，按列族 (column family) 编码在 value 中。
func (sd *SpanConfigDecoder) decode(kv roachpb.KeyValue) (spanconfig.Record, error) {
	// First we need to decode the start_key field from the index key.
	var rawSp roachpb.Span
	var conf roachpb.SpanConfig
	{
		// [1] 从主键解码 start_key
		types := []*types.T{sd.columns[0].GetType()}
		startKeyRow := make([]rowenc.EncDatum, 1)
		if _, err := rowenc.DecodeIndexKey(keys.SystemSQLCodec, startKeyRow, nil /* colDirs */, kv.Key); err != nil {
			return spanconfig.Record{}, errors.Wrapf(err, "failed to decode key: %v", kv.Key)
		}
		if err := startKeyRow[0].EnsureDecoded(types[0], &sd.alloc); err != nil {
			return spanconfig.Record{}, err
		}
		rawSp.Key = []byte(tree.MustBeDBytes(startKeyRow[0].Datum))
	}
	if !kv.Value.IsPresent() {
		return spanconfig.Record{},
			errors.AssertionFailedf("missing value for start key: %s", rawSp.Key)
	}

	// The remaining columns are stored as a family.
	// [2] 从 value (column family) 解码 end_key 和 config
	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return spanconfig.Record{}, err
	}

	datums, err := sd.decoder.Decode(&sd.alloc, bytes)
	if err != nil {
		return spanconfig.Record{}, err
	}
	if endKey := datums[1]; endKey != tree.DNull {
		rawSp.EndKey = []byte(tree.MustBeDBytes(endKey))
	}
	if config := datums[2]; config != tree.DNull {
		if err := protoutil.Unmarshal([]byte(tree.MustBeDBytes(config)), &conf); err != nil {
			return spanconfig.Record{}, err
		}
	}

	return spanconfig.MakeRecord(spanconfig.DecodeTarget(rawSp), conf)
}

// **事件类型分类**：
//
// | 场景 | `ev.Value` | `ev.PrevValue` | 处理 |
// | --- | --- | --- | --- |
// | 新增配置 | Present | N/A | `spanconfig.Update(record)` |
// | 修改配置 | Present | Present | `spanconfig.Update(record)` (新值) |
// | 删除配置 | Empty (tombstone) | Present | `spanconfig.Deletion(target)` |
// | tombstone-on-tombstone | Empty | Empty | `return nil, false` (忽略) |
func (sd *SpanConfigDecoder) TranslateEvent(
	ctx context.Context, ev *kvpb.RangeFeedValue,
) (*BufferEvent, bool) {
	deleted := !ev.Value.IsPresent() // Value 为空 = 行被删除
	var value roachpb.Value
	if deleted {
		if !ev.PrevValue.IsPresent() { // tombstone-on-tombstone，忽略
			// It's possible to write a KV tombstone on top of another KV
			// tombstone -- both the new and old value will be empty. We simply
			// ignore these events.
			return nil, false
		}

		// Since the end key is not part of the primary key, we need to
		// decode the previous value in order to determine what it is.
		value = ev.PrevValue // ★ 使用 PrevValue 解码被删除的行
	} else {
		value = ev.Value // 正常行：使用当前 Value
	}
	record, err := sd.decode(roachpb.KeyValue{
		Key:   ev.Key,
		Value: value,
	})
	if err != nil { // 不可重试 → fatal
		log.Dev.Fatalf(ctx, "failed to decode row: %v", err) // non-retryable error; just fatal
	}

	if log.ExpensiveLogEnabled(ctx, 1) {
		log.Dev.Infof(ctx, "received span configuration update for %s (deleted=%t)",
			record.GetTarget(), deleted)
	}

	var update spanconfig.Update
	if deleted {
		update, err = spanconfig.Deletion(record.GetTarget())
		if err != nil {
			log.Dev.Fatalf(ctx, "failed to construct Deletion: %+v", err)
		}
	} else {
		update = spanconfig.Update(record)
	}

	return &BufferEvent{update, ev.Value.Timestamp}, true
}

// TestingDecoderFn exports the decoding routine for testing purposes.
func TestingDecoderFn() func(roachpb.KeyValue) (spanconfig.Record, error) {
	return NewSpanConfigDecoder().decode
}
