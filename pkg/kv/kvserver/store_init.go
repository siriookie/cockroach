// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func WriteInitialClusterData(
	ctx context.Context,
	eng storage.Engine, // 要写入初始化数据的底层 Engine（一个 store / 一个磁盘）
	initialValues []roachpb.KeyValue, // 额外需要写入的初始 KV（比如 zone config）
	bootstrapVersion roachpb.Version, // 集群 bootsServeAndWaittrap 时使用的版本
	numStores int, // 当前节点包含的 store 数量
	splits []roachpb.RKey, // 初始 Range 切分点（已排序）
	nowNanos int64, // 初始化写入时使用的时间戳
	knobs StoreTestingKnobs,
) error {

	// Bootstrap version information. We'll add the "bootstrap version" to the
	// list of initialValues, so that we don't have to handle it specially
	// (particularly since we don't want to manually figure out which range it
	// falls into).

	// 中文：把 bootstrapVersion 当成一个普通 KV，写进系统 keyspace
	bootstrapVal := roachpb.Value{}
	if err := bootstrapVal.SetProto(&bootstrapVersion); err != nil {
		return err
	}
	initialValues = append(initialValues,
		roachpb.KeyValue{Key: keys.BootstrapVersionKey, Value: bootstrapVal})

	// Initialize various sequence generators.

	// 中文：初始化各种 ID 生成器（NodeID / StoreID / RangeID）
	var nodeIDVal, storeIDVal, rangeIDVal, livenessVal roachpb.Value

	// 第一个节点的 NodeID 固定是 FirstNodeID（通常是 1）
	nodeIDVal.SetInt(int64(kvstorage.FirstNodeID))

	// 当前节点会拥有 [FirstStoreID, FirstStoreID + numStores - 1]
	storeIDVal.SetInt(int64(kvstorage.FirstStoreID) + int64(numStores) - 1)

	// RangeID 是连续的，最后一个 range 的 ID = splits + 1
	rangeIDVal.SetInt(int64(len(splits) + 1))

	// We're the first node in the cluster, let's seed our liveness record.
	// 中文：初始化第一个 Node 的 liveness 记录（epoch=0）
	livenessRecord := livenesspb.Liveness{NodeID: kvstorage.FirstNodeID, Epoch: 0}
	if err := livenessVal.SetProto(&livenessRecord); err != nil {
		return err
	}

	// 中文：把 ID 生成器和 liveness 都写成初始 KV
	initialValues = append(initialValues,
		roachpb.KeyValue{Key: keys.NodeIDGenerator, Value: nodeIDVal},
		roachpb.KeyValue{Key: keys.StoreIDGenerator, Value: storeIDVal},
		roachpb.KeyValue{Key: keys.RangeIDGenerator, Value: rangeIDVal},
		roachpb.KeyValue{Key: keys.NodeLivenessKey(kvstorage.FirstNodeID), Value: livenessVal})

	// meta2RangeMS 用来累积 meta2 range 的 MVCC 统计信息
	meta2RangeMS := &enginepb.MVCCStats{}

	// 中文：根据 RangeDescriptor 过滤 initialValues，只留下属于该 Range 的 KV
	filterInitialValues := func(desc *roachpb.RangeDescriptor) []roachpb.KeyValue {
		var r []roachpb.KeyValue
		for _, kv := range initialValues {
			if desc.ContainsKey(roachpb.RKey(kv.Key)) {
				r = append(r, kv)
			}
		}
		return r
	}

	// 中文：初始 Replica 版本通常等于 bootstrapVersion
	initialReplicaVersion := bootstrapVersion
	if knobs.InitialReplicaVersionOverride != nil {
		initialReplicaVersion = *knobs.InitialReplicaVersionOverride
	}

	// 中文：是否对 meta2 进行 split（某些测试场景不会）
	shouldSplitMeta2 := slices.ContainsFunc(splits, func(split roachpb.RKey) bool {
		return split.Equal(keys.Meta2Prefix)
	})

	// We iterate through the ranges backwards
	// 中文：倒序创建 Range，因为所有 Range 都会往 meta2 写记录，
	//       meta2 自身的 stats 必须在最后计算完成
	startKey := roachpb.RKeyMax
	for i := len(splits) - 1; i >= -1; i-- {
		endKey := startKey
		rangeID := roachpb.RangeID(i + 2) // RangeID 从 1 开始
		if i >= 0 {
			startKey = splits[i]
		} else {
			startKey = roachpb.RKeyMin
		}

		// 中文：构造 RangeDescriptor（定义这个 Range 管哪些 key）
		desc := &roachpb.RangeDescriptor{
			RangeID:       rangeID,
			StartKey:      startKey,
			EndKey:        endKey,
			NextReplicaID: 2,
		}

		const firstReplicaID = 1

		// 中文：第一个 Range 的第一个 Replica，放在 node=1, store=1
		replicas := []roachpb.ReplicaDescriptor{
			{
				NodeID:    kvstorage.FirstNodeID,
				StoreID:   kvstorage.FirstStoreID,
				ReplicaID: firstReplicaID,
			},
		}
		desc.SetReplicas(roachpb.MakeReplicaSet(replicas))
		if err := desc.Validate(); err != nil {
			return err
		}

		rangeInitialValues := filterInitialValues(desc)

		// 中文：真正开始往 Engine 写数据
		err := func() error {
			batch := eng.NewBatch()
			defer batch.Close()

			now := hlc.Timestamp{
				WallTime: nowNanos,
				Logical:  0,
			}

			// 写 RangeDescriptor
			if err := storage.MVCCPutProto(
				ctx, batch, keys.RangeDescriptorKey(desc.StartKey),
				now, desc, storage.MVCCWriteOptions{},
			); err != nil {
				return err
			}

			// 写 Replica GC 时间戳
			if err := storage.MVCCPutProto(
				ctx, batch, keys.RangeLastReplicaGCTimestampKey(desc.RangeID),
				hlc.Timestamp{}, &now, storage.MVCCWriteOptions{},
			); err != nil {
				return err
			}

			// 在 meta2 中写入 range addressing
			metaKey := keys.RangeMetaKey(endKey)
			if err := storage.MVCCPutProto(
				ctx, batch, metaKey.AsRawKey(),
				now, desc, storage.MVCCWriteOptions{Stats: meta2RangeMS},
			); err != nil {
				return err
			}

			// 写 meta1（用于定位 meta2）
			if startKey.Equal(keys.Meta2Prefix) || !shouldSplitMeta2 {
				meta1Key := keys.RangeMetaKey(keys.RangeMetaKey(roachpb.RKeyMax))
				if err := storage.MVCCPutProto(
					ctx, batch, meta1Key.AsRawKey(), now, desc, storage.MVCCWriteOptions{},
				); err != nil {
					return err
				}
			}

			// 写入 default / initial KV（如 zone config）
			for _, kv := range rangeInitialValues {
				kv.Value.InitChecksum(kv.Key)
				if _, err := storage.MVCCPut(
					ctx, batch, kv.Key, now, kv.Value, storage.MVCCWriteOptions{},
				); err != nil {
					return err
				}
			}

			// 写入 RangeState（raft 状态 / applied index 等）
			if err := kvstorage.WriteInitialRangeState(
				ctx, batch, batch,
				*desc, firstReplicaID, initialReplicaVersion,
			); err != nil {
				return err
			}

			// 计算并写入 MVCCStats
			//虽然我们刚写入了数据，但写入操作本身也会产生 MVCC 元数据（如时间戳、校验和）。
			//ComputeStatsForRange 通过扫描整个 Range，精确计算统计信息，确保数据一致性
			computedStats, err := rditer.ComputeStatsForRange(
				ctx, desc, batch, fs.UnknownReadCategory, now.WallTime)
			if err != nil {
				return err
			}

			sl := kvstorage.MakeStateLoader(rangeID)
			if err := sl.SetMVCCStats(ctx, batch, &computedStats); err != nil {
				return err
			}

			return batch.Commit(true /* sync */)
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

// writeGlobalMVCCRangeTombstone writes an MVCC range tombstone across the
// entire table data keyspace of the range. This is used to test that storage
// operations are correct and performant in the presence of range tombstones. An
// MVCC range tombstone below all other data should in principle not affect
// anything at all.
func writeGlobalMVCCRangeTombstone(
	ctx context.Context, w storage.Writer, desc *roachpb.RangeDescriptor, ts hlc.Timestamp,
) error {
	rangeKey := storage.MVCCRangeKey{
		StartKey:  desc.StartKey.AsRawKey(),
		EndKey:    desc.EndKey.AsRawKey(),
		Timestamp: ts,
	}
	if rangeKey.EndKey.Compare(keys.TableDataMin) <= 0 {
		return nil
	}
	if rangeKey.StartKey.Compare(keys.TableDataMin) < 0 {
		rangeKey.StartKey = keys.TableDataMin
	}
	if err := w.PutMVCCRangeKey(rangeKey, storage.MVCCValue{}); err != nil {
		return err
	}
	log.KvDistribution.Warningf(ctx, "wrote global MVCC range tombstone %s", rangeKey)
	return nil
}
