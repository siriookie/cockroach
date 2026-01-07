// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// settingsCacheWriter is responsible for persisting the cluster
// settings on KV nodes across restarts.
type settingsCacheWriter struct {
	stopper *stop.Stopper
	eng     storage.Engine

	mu struct {
		syncutil.Mutex
		currentlyWriting bool

		queuedToWrite []roachpb.KeyValue
	}
}

func newSettingsCacheWriter(eng storage.Engine, stopper *stop.Stopper) *settingsCacheWriter {
	return &settingsCacheWriter{
		eng:     eng,
		stopper: stopper,
	}
}

func (s *settingsCacheWriter) SnapshotKVs(ctx context.Context, kvs []roachpb.KeyValue) {
	if !s.queueSnapshot(kvs) {
		return
	}
	if err := s.stopper.RunAsyncTask(ctx, "snapshot-settings-cache", func(
		ctx context.Context,
	) {
		defer s.doneWriting()
		for toWrite, ok := s.getToWrite(); ok; toWrite, ok = s.getToWrite() {
			if err := storeCachedSettingsKVs(ctx, s.eng, toWrite); err != nil {
				log.Dev.Warningf(ctx, "failed to write settings snapshot: %v", err)
			}
		}
	}); err != nil {
		s.doneWriting()
	}
}

func (s *settingsCacheWriter) queueSnapshot(kvs []roachpb.KeyValue) (shouldRun bool) {
	s.mu.Lock() // held into the async task
	if s.mu.currentlyWriting {
		s.mu.queuedToWrite = kvs
		s.mu.Unlock()
		return false
	}
	s.mu.currentlyWriting = true
	s.mu.queuedToWrite = kvs
	s.mu.Unlock()
	return true
}

func (s *settingsCacheWriter) doneWriting() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.currentlyWriting = false
}

func (s *settingsCacheWriter) getToWrite() (toWrite []roachpb.KeyValue, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	toWrite, s.mu.queuedToWrite = s.mu.queuedToWrite, nil
	return toWrite, toWrite != nil
}

var _ settingswatcher.Storage = (*settingsCacheWriter)(nil)

// storeCachedSettingsKVs stores or caches node's settings locally.
// This helps in restoring the node restart with the at least the same settings with which it died.
func storeCachedSettingsKVs(ctx context.Context, eng storage.Engine, kvs []roachpb.KeyValue) error {
	batch := eng.NewBatch()
	defer batch.Close()

	// Remove previous entries -- they are now stale.
	if _, _, _, _, err := storage.MVCCDeleteRange(ctx, batch,
		keys.LocalStoreCachedSettingsKeyMin,
		keys.LocalStoreCachedSettingsKeyMax,
		0 /* no limit */, hlc.Timestamp{}, storage.MVCCWriteOptions{}, false /* returnKeys */); err != nil {
		return err
	}

	// Now we can populate the cache with new entries.
	for _, kv := range kvs {
		kv.Value.Timestamp = hlc.Timestamp{} // nb: Timestamp is not part of checksum
		cachedSettingsKey := keys.StoreCachedSettingsKey(kv.Key)
		// A new value is added, or an existing value is updated.
		log.VEventf(ctx, 1, "storing cached setting: %s -> %+v", cachedSettingsKey, kv.Value)
		if _, err := storage.MVCCPut(
			ctx, batch, cachedSettingsKey, hlc.Timestamp{}, kv.Value, storage.MVCCWriteOptions{},
		); err != nil {
			return err
		}
	}
	return batch.Commit(false /* sync */)
}

// loadCachedSettingsKVs loads locally stored cached settings.
// loadCachedSettingsKVs 是 CockroachDB 节点启动或初始化时，从本地存储引擎（Pebble）中读取缓存的集群设置（cluster settings） 的函数。
//
// CockroachDB 的很多配置（如 server.time_until_store_dead、sql.stats.response_size 等）是集群级设置（cluster settings），可以通过 SET CLUSTER SETTING 动态修改。
// 这些设置会被持久化到每个节点的本地存储中（在 keys.LocalStoreCachedSettingsKeyMin 到 keys.LocalStoreCachedSettingsKeyMax 这个 key 范围内）。
// 节点启动时，需要先把这些本地缓存的设置加载到内存，这样后续 SQL 层和 KV 层就能使用最新的配置值，而不用每次都去 gossip 或系统表查。
//
// 简单说：让节点快速恢复上一次关机前的集群设置状态。
func loadCachedSettingsKVs(ctx context.Context, reader storage.Reader) ([]roachpb.KeyValue, error) {
	//准备一个切片，用于存放读取到的设置键值对。
	var settingsKVs []roachpb.KeyValue
	//使用 MVCCIterate 在存储引擎中范围扫描（range scan）一个特定的 key 区间：
	//从 LocalStoreCachedSettingsKeyMin 到 LocalStoreCachedSettingsKeyMax
	//只扫描 point keys（不扫描 range keys）
	//迭代器类型是 KeyAndIntents（但这里实际只关心 key/value）
	if err := reader.MVCCIterate(ctx, keys.LocalStoreCachedSettingsKeyMin,
		keys.LocalStoreCachedSettingsKeyMax, storage.MVCCKeyAndIntentsIterKind,
		storage.IterKeyTypePointsOnly, fs.UnknownReadCategory,
		func(kv storage.MVCCKeyValue, _ storage.MVCCRangeKeyStack) error {
			//每个 key 都是经过特殊编码的“cached settings key”。
			//解码得到真正的设置 key（如 /Table/51 对应的设置名）。
			settingKey, err := keys.DecodeStoreCachedSettingsKey(kv.Key.Key)
			if err != nil {
				return err
			}
			meta := enginepb.MVCCMetadata{}
			//在 CockroachDB 的 MVCC 中，value 本身不直接存设置值，而是存一个 MVCCMetadata，里面有一个 RawBytes 字段才真正存设置的序列化值。
			//这里反序列化 metadata，取出真正的设置值。
			if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
				return err
			}
			settingsKVs = append(settingsKVs, roachpb.KeyValue{
				Key:   settingKey,
				Value: roachpb.Value{RawBytes: meta.RawBytes},
			})
			return nil
		}); err != nil {
		return nil, err
	}
	return settingsKVs, nil
}

func initializeCachedSettings(
	ctx context.Context, codec keys.SQLCodec, updater settings.Updater, kvs []roachpb.KeyValue,
) error {
	dec := settingswatcher.MakeRowDecoder(codec)
	for _, kv := range kvs {
		settingKeyS, val, _, err := dec.DecodeRow(kv, nil /* alloc */)
		if err != nil {
			return errors.WithHint(errors.Wrap(err, "while decoding settings data"),
				"This likely indicates the settings table structure or encoding has been altered;"+
					" skipping settings updates.")
		}
		settingKey := settings.InternalKey(settingKeyS)
		log.VEventf(ctx, 1, "loaded cached setting: %s -> %+v", settingKey, val)
		if err := updater.Set(ctx, settingKey, val); err != nil {
			log.Dev.Warningf(ctx, "setting %q to %v failed: %+v", settingKey, val, err)
		}
	}
	updater.ResetRemaining(ctx)
	return nil
}
