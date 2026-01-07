// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keys

import (
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// makeKey 将多个字节切片连接成一个键
// 通过 bytes.Join 将传入的所有键片段无分隔符地拼接在一起
func makeKey(keys ...[]byte) []byte {
	return bytes.Join(keys, nil)
}

// MakeStoreKey creates a store-local key based on the metadata key
// suffix, and optional detail.
// MakeStoreKey 创建一个存储级别的本地键，基于元数据键后缀和可选的详细信息
// 这些键存储在单个 Store 上，不会通过 Raft 复制到其他副本
// 用于存储 Store 的身份信息、Gossip 数据、HLC 上界等本地元数据
func MakeStoreKey(suffix, detail roachpb.RKey) roachpb.Key {
	key := make(roachpb.Key, 0, len(LocalStorePrefix)+len(suffix)+len(detail))
	key = append(key, LocalStorePrefix...)
	key = append(key, suffix...)
	key = append(key, detail...)
	return key
}

// DecodeStoreKey returns the suffix and detail portions of a local
// store key.
// DecodeStoreKey 解码存储级别的本地键，返回后缀和详细信息部分
// 用于从完整的 Store 键中提取出后缀（标识元数据类型）和详细信息（如节点 ID）
func DecodeStoreKey(key roachpb.Key) (suffix, detail roachpb.RKey, err error) {
	if !bytes.HasPrefix(key, LocalStorePrefix) {
		return nil, nil, errors.Errorf("key %s does not have %s prefix", key, LocalStorePrefix)
	}
	// Cut the prefix, the Range ID, and the infix specifier.
	key = key[len(LocalStorePrefix):]
	if len(key) < localSuffixLength {
		return nil, nil, errors.Errorf("malformed key does not contain local store suffix")
	}
	suffix = roachpb.RKey(key[:localSuffixLength])
	detail = roachpb.RKey(key[localSuffixLength:])
	return suffix, detail, nil
}

// StoreIdentKey returns a store-local key for the store metadata.
// StoreIdentKey 返回存储标识信息的键
// 用于存储 Store 的唯一标识符和基本元数据
func StoreIdentKey() roachpb.Key {
	return MakeStoreKey(localStoreIdentSuffix, nil)
}

// StoreGossipKey returns a store-local key for the gossip bootstrap metadata.
// StoreGossipKey 返回 Gossip 引导元数据的键
// Gossip 是 CockroachDB 的节点发现和信息传播协议，此键存储引导所需的初始节点信息
func StoreGossipKey() roachpb.Key {
	return MakeStoreKey(localStoreGossipSuffix, nil)
}

// DeprecatedStoreClusterVersionKey returns a store-local key for the cluster version.
//
// We no longer use this key, but still write it out for interoperability with
// older versions.
// DeprecatedStoreClusterVersionKey 返回集群版本的键（已废弃）
// 虽然不再使用此键，但为了与旧版本的互操作性仍然会写入
func DeprecatedStoreClusterVersionKey() roachpb.Key {
	return MakeStoreKey(localStoreClusterVersionSuffix, nil)
}

// StoreLastUpKey returns the key for the store's "last up" timestamp.
// StoreLastUpKey 返回 Store 最后启动时间戳的键
// 用于记录 Store 上次启动的时间，帮助检测节点是否长时间离线
func StoreLastUpKey() roachpb.Key {
	return MakeStoreKey(localStoreLastUpSuffix, nil)
}

// StoreHLCUpperBoundKey returns the store-local key for storing an upper bound
// to the wall time used by HLC.
// StoreHLCUpperBoundKey 返回 HLC（混合逻辑时钟）墙上时间上界的键
// 用于存储 HLC 使用的墙上时间的上界，确保时钟单调性，防止时钟回退
func StoreHLCUpperBoundKey() roachpb.Key {
	return MakeStoreKey(localStoreHLCUpperBoundSuffix, nil)
}

// StoreNodeTombstoneKey returns the key for storing a node tombstone for nodeID.
// StoreNodeTombstoneKey 返回指定节点 ID 的墓碑标记键
// 墓碑标记表示节点已被永久移除，防止被移除的节点重新加入集群
func StoreNodeTombstoneKey(nodeID roachpb.NodeID) roachpb.Key {
	return MakeStoreKey(localStoreNodeTombstoneSuffix, encoding.EncodeUint32Ascending(nil, uint32(nodeID)))
}

// DecodeNodeTombstoneKey returns the NodeID for the node tombstone.
// DecodeNodeTombstoneKey 从墓碑键中解码并返回节点 ID
func DecodeNodeTombstoneKey(key roachpb.Key) (roachpb.NodeID, error) {
	suffix, detail, err := DecodeStoreKey(key)
	if err != nil {
		return 0, err
	}
	if !suffix.Equal(localStoreNodeTombstoneSuffix) {
		return 0, errors.Errorf("key with suffix %q != %q", suffix, localStoreNodeTombstoneSuffix)
	}
	detail, nodeID, err := encoding.DecodeUint32Ascending(detail)
	if len(detail) != 0 {
		return 0, errors.Errorf("invalid key has trailing garbage: %q", detail)
	}
	return roachpb.NodeID(nodeID), err
}

// StoreLivenessRequesterMetaKey returns the key for the local store's Store
// Liveness requester metadata.
// StoreLivenessRequesterMetaKey 返回 Store 活跃度请求者元数据的键
// 用于存储本地 Store 作为活跃度请求方的元数据信息
func StoreLivenessRequesterMetaKey() roachpb.Key {
	return MakeStoreKey(localStoreLivenessRequesterMeta, nil)
}

// StoreLivenessSupporterMetaKey returns the key for the local store's Store
// Liveness supporter metadata.
// StoreLivenessSupporterMetaKey 返回 Store 活跃度支持者元数据的键
// 用于存储本地 Store 作为活跃度支持方的元数据信息
func StoreLivenessSupporterMetaKey() roachpb.Key {
	return MakeStoreKey(localStoreLivenessSupporterMeta, nil)
}

// StoreLivenessSupportForKey returns the key for the Store Liveness support
// by the local store for a given store identified by nodeID and storeID.
// StoreLivenessSupportForKey 返回本地 Store 对指定 Store 的活跃度支持记录键
// 将节点 ID 和 Store ID 打包成一个 64 位整数进行编码，用于跟踪 Store 之间的活跃度支持关系
func StoreLivenessSupportForKey(nodeID roachpb.NodeID, storeID roachpb.StoreID) roachpb.Key {
	nodeIDAndStoreID := uint64(nodeID)<<32 | uint64(storeID)
	return MakeStoreKey(
		localStoreLivenessSupportFor, encoding.EncodeUint64Ascending(nil, nodeIDAndStoreID),
	)
}

// DecodeStoreLivenessSupportForKey returns the node ID and store ID of a given
// localStoreLivenessSupportFor key.
// DecodeStoreLivenessSupportForKey 从活跃度支持键中解码并返回节点 ID 和 Store ID
// 从打包的 64 位整数中提取高 32 位作为节点 ID，低 32 位作为 Store ID
func DecodeStoreLivenessSupportForKey(key roachpb.Key) (roachpb.NodeID, roachpb.StoreID, error) {
	suffix, detail, err := DecodeStoreKey(key)
	if err != nil {
		return 0, 0, err
	}
	if !suffix.Equal(localStoreLivenessSupportFor) {
		return 0, 0, errors.Errorf("key with suffix %q != %q", suffix, localStoreLivenessSupportFor)
	}
	detail, nodeIDAndStoreID, err := encoding.DecodeUint64Ascending(detail)
	if err != nil {
		return 0, 0, err
	}
	if len(detail) != 0 {
		return 0, 0, errors.Errorf("invalid key has trailing garbage: %q", detail)
	}
	nodeID := roachpb.NodeID(nodeIDAndStoreID >> 32)
	storeID := roachpb.StoreID(nodeIDAndStoreID & math.MaxUint32)
	return nodeID, storeID, nil
}

// StoreWAGPrefix returns the key prefix for WAG nodes.
// StoreWAGPrefix 返回 WAG（Write-Ahead Graph）节点的键前缀
// WAG 用于跟踪写操作的依赖关系图
func StoreWAGPrefix() roachpb.Key {
	return MakeStoreKey(localStoreWAGNodeSuffix, nil)
}

// StoreWAGNodeKey returns the key for a WAG node at the given index.
// StoreWAGNodeKey 返回指定索引位置的 WAG 节点键
func StoreWAGNodeKey(index uint64) roachpb.Key {
	return MakeStoreKey(localStoreWAGNodeSuffix, encoding.EncodeUint64Ascending(nil, index))
}

// DecodeWAGNodeKey returns the index of the WAG node from its key.
// DecodeWAGNodeKey 从 WAG 节点键中解码并返回索引值
func DecodeWAGNodeKey(key roachpb.Key) (uint64, error) {
	suffix, detail, err := DecodeStoreKey(key)
	if err != nil {
		return 0, err
	}
	if !suffix.Equal(localStoreWAGNodeSuffix) {
		return 0, errors.Errorf("key with suffix %q != %q", suffix, localStoreWAGNodeSuffix)
	}
	detail, index, err := encoding.DecodeUint64Ascending(detail)
	if len(detail) != 0 {
		return 0, errors.Errorf("invalid key has trailing garbage: %q", detail)
	}
	return index, err
}

// StoreCachedSettingsKey returns a store-local key for store's cached settings.
// StoreCachedSettingsKey 返回 Store 缓存设置的键
// 用于在本地缓存集群设置，避免每次都需要从系统表中读取
func StoreCachedSettingsKey(settingKey roachpb.Key) roachpb.Key {
	return MakeStoreKey(localStoreCachedSettingsSuffix, encoding.EncodeBytesAscending(nil, settingKey))
}

// DecodeStoreCachedSettingsKey returns the setting's key of the cached settings kvs.
// DecodeStoreCachedSettingsKey 从缓存设置键中解码并返回原始的设置键
func DecodeStoreCachedSettingsKey(key roachpb.Key) (settingKey roachpb.Key, err error) {
	var suffix, detail roachpb.RKey
	suffix, detail, err = DecodeStoreKey(key)
	if err != nil {
		return nil, err
	}
	if !suffix.Equal(localStoreCachedSettingsSuffix) {
		return nil, errors.Errorf(
			"key with suffix %q != %q",
			suffix,
			localStoreCachedSettingsSuffix,
		)
	}
	detail, settingKey, err = encoding.DecodeBytesAscending(detail, nil)
	if len(detail) != 0 {
		return nil, errors.Errorf("invalid key has trailing garbage: %q", detail)
	}
	return
}

// StoreLossOfQuorumRecoveryStatusKey is a key used for storing results of loss
// of quorum recovery plan application.
// StoreLossOfQuorumRecoveryStatusKey 返回仲裁丢失恢复计划应用结果的键
// 用于存储在多数副本丢失后的恢复操作状态
func StoreLossOfQuorumRecoveryStatusKey() roachpb.Key {
	return MakeStoreKey(localStoreLossOfQuorumRecoveryStatusSuffix, nil)
}

// StoreLossOfQuorumRecoveryCleanupActionsKey is a key used for storing data for
// post recovery cleanup actions node would perform after restart if plan was
// applied.
// StoreLossOfQuorumRecoveryCleanupActionsKey 返回恢复后清理操作的键
// 如果应用了恢复计划，存储节点重启后需要执行的清理操作数据
func StoreLossOfQuorumRecoveryCleanupActionsKey() roachpb.Key {
	return MakeStoreKey(localStoreLossOfQuorumRecoveryCleanupActionsSuffix, nil)
}

// StoreUnsafeReplicaRecoveryKey creates a key for loss of quorum replica
// recovery entry. Those keys are written by `debug recover apply-plan` command
// on the store while node is stopped. Once node boots up, entries are
// translated into structured log events to leave audit trail of recovery
// operation.
// StoreUnsafeReplicaRecoveryKey 创建不安全副本恢复条目的键
// 这些键由 `debug recover apply-plan` 命令在节点停止时写入
// 节点启动后，这些条目会被转换为结构化日志事件，留下恢复操作的审计跟踪
func StoreUnsafeReplicaRecoveryKey(uuid uuid.UUID) roachpb.Key {
	key := make(roachpb.Key, 0, len(LocalStoreUnsafeReplicaRecoveryKeyMin)+len(uuid))
	key = append(key, LocalStoreUnsafeReplicaRecoveryKeyMin...)
	key = append(key, uuid.GetBytes()...)
	return key
}

// DecodeStoreUnsafeReplicaRecoveryKey decodes uuid key used to create record
// key for unsafe replica recovery record.
// DecodeStoreUnsafeReplicaRecoveryKey 从不安全副本恢复键中解码并返回 UUID
func DecodeStoreUnsafeReplicaRecoveryKey(key roachpb.Key) (uuid.UUID, error) {
	if !bytes.HasPrefix(key, LocalStoreUnsafeReplicaRecoveryKeyMin) {
		return uuid.UUID{},
			errors.Errorf("key %q does not have %q prefix", string(key), LocalRangeIDPrefix)
	}
	remainder := key[len(LocalStoreUnsafeReplicaRecoveryKeyMin):]
	entryID, err := uuid.FromBytes(remainder)
	if err != nil {
		return entryID, errors.Wrap(err, "failed to get uuid from unsafe replica recovery key")
	}
	return entryID, nil
}

// NodeLivenessKey returns the key for the node liveness record.
// NodeLivenessKey 返回节点活跃度记录的键
// 节点活跃度用于跟踪集群中每个节点是否存活，这是租约和故障检测的基础
func NodeLivenessKey(nodeID roachpb.NodeID) roachpb.Key {
	key := make(roachpb.Key, 0, len(NodeLivenessPrefix)+9)
	key = append(key, NodeLivenessPrefix...)
	key = encoding.EncodeUvarintAscending(key, uint64(nodeID))
	return key
}

// NodeStatusKey returns the key for accessing the node status for the
// specified node ID.
// NodeStatusKey 返回访问指定节点状态信息的键
// 用于存储节点的状态指标，如 CPU、内存、磁盘使用等
func NodeStatusKey(nodeID roachpb.NodeID) roachpb.Key {
	key := make(roachpb.Key, 0, len(StatusNodePrefix)+9)
	key = append(key, StatusNodePrefix...)
	key = encoding.EncodeUvarintAscending(key, uint64(nodeID))
	return key
}

// makePrefixWithRangeID 使用 Range ID 创建键前缀
// 这是一个内部辅助函数，用于构造带有 Range ID 的本地键前缀
func makePrefixWithRangeID(prefix []byte, rangeID roachpb.RangeID, infix roachpb.RKey) roachpb.Key {
	// Size the key buffer so that it is large enough for most callers.
	key := make(roachpb.Key, 0, 32)
	key = append(key, prefix...)
	key = encoding.EncodeUvarintAscending(key, uint64(rangeID))
	key = append(key, infix...)
	return key
}

// MakeRangeIDPrefix creates a range-local key prefix from
// rangeID for both replicated and unreplicated data.
// MakeRangeIDPrefix 根据 Range ID 创建 Range 本地键前缀
// 用于存储与特定 Range 相关的数据（包括复制和非复制的数据）
func MakeRangeIDPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return makePrefixWithRangeID(LocalRangeIDPrefix, rangeID, nil)
}

// MakeRangeIDReplicatedPrefix creates a range-local key prefix from
// rangeID for all Raft replicated data.
// MakeRangeIDReplicatedPrefix 根据 Range ID 创建 Raft 复制数据的键前缀
// 用于存储需要通过 Raft 协议在副本之间复制的 Range 元数据
func MakeRangeIDReplicatedPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return makePrefixWithRangeID(LocalRangeIDPrefix, rangeID, LocalRangeIDReplicatedInfix)
}

// makeRangeIDReplicatedKey creates a range-local key based on the range's
// Range ID, metadata key suffix, and optional detail.
// makeRangeIDReplicatedKey 创建基于 Range ID 的复制键
// 这是一个内部函数，用于构造需要通过 Raft 复制的 Range 本地键
func makeRangeIDReplicatedKey(rangeID roachpb.RangeID, suffix, detail roachpb.RKey) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}

	key := MakeRangeIDReplicatedPrefix(rangeID)
	key = append(key, suffix...)
	key = append(key, detail...)
	return key
}

// DecodeRangeIDKey parses a local range ID key into range ID, infix,
// suffix, and detail.
// DecodeRangeIDKey 解析 Range ID 本地键，提取 Range ID、中缀、后缀和详细信息
// 用于从完整的 Range 本地键中提取各个组成部分
func DecodeRangeIDKey(
	key roachpb.Key,
) (rangeID roachpb.RangeID, infix, suffix, detail roachpb.Key, err error) {
	if !bytes.HasPrefix(key, LocalRangeIDPrefix) {
		return 0, nil, nil, nil, errors.Errorf("key %s does not have %s prefix", key, LocalRangeIDPrefix)
	}
	// Cut the prefix, the Range ID, and the infix specifier.
	b := key[len(LocalRangeIDPrefix):]
	b, rangeInt, err := encoding.DecodeUvarintAscending(b)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	if len(b) < localSuffixLength+1 {
		return 0, nil, nil, nil, errors.Errorf("malformed key does not contain range ID infix and suffix")
	}
	infix = b[:1]
	b = b[1:]
	suffix = b[:localSuffixLength]
	b = b[localSuffixLength:]

	return roachpb.RangeID(rangeInt), infix, suffix, b, nil
}

// DecodeRangeIDPrefix parses a local range ID prefix into range ID.
// DecodeRangeIDPrefix 从 Range ID 前缀中解析出 Range ID
func DecodeRangeIDPrefix(key roachpb.Key) (roachpb.RangeID, error) {
	if !bytes.HasPrefix(key, LocalRangeIDPrefix) {
		return 0, errors.Errorf("key %s does not have %s prefix", key, LocalRangeIDPrefix)
	}
	// Cut the prefix, the Range ID, and the infix specifier.
	b := key[len(LocalRangeIDPrefix):]
	_, rangeInt, err := encoding.DecodeUvarintAscending(b)
	return roachpb.RangeID(rangeInt), err
}

// AbortSpanKey returns a range-local key by Range ID for an
// AbortSpan entry, with detail specified by encoding the
// supplied transaction ID.
// AbortSpanKey 返回 AbortSpan 条目的 Range 本地键
// AbortSpan 记录已中止的事务，防止僵尸事务（已中止但仍有未完成操作的事务）造成数据不一致
func AbortSpanKey(rangeID roachpb.RangeID, txnID uuid.UUID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).AbortSpanKey(txnID)
}

// ReplicatedSharedLocksTransactionLatchingKey returns a range-local key, based
// on the provided range ID and transaction ID, that all replicated shared
// locking requests from the specified transaction should use to serialize on
// latches.
// ReplicatedSharedLocksTransactionLatchingKey 返回复制共享锁事务闩锁键
// 用于确保来自同一事务的所有复制共享锁请求在闩锁上串行化执行
func ReplicatedSharedLocksTransactionLatchingKey(
	rangeID roachpb.RangeID, txnID uuid.UUID,
) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).ReplicatedSharedLocksTransactionLatchingKey(txnID)
}

// DecodeAbortSpanKey decodes the provided AbortSpan entry,
// returning the transaction ID.
// DecodeAbortSpanKey 从 AbortSpan 键中解码并返回事务 ID
func DecodeAbortSpanKey(key roachpb.Key, dest []byte) (uuid.UUID, error) {
	_, _, suffix, detail, err := DecodeRangeIDKey(key)
	if err != nil {
		return uuid.UUID{}, err
	}
	if !bytes.Equal(suffix, LocalAbortSpanSuffix) {
		return uuid.UUID{}, errors.Errorf("key %s does not contain the AbortSpan suffix %s",
			key, LocalAbortSpanSuffix)
	}
	// Decode the id.
	detail, idBytes, err := encoding.DecodeBytesAscending(detail, dest)
	if err != nil {
		return uuid.UUID{}, err
	}
	if len(detail) > 0 {
		return uuid.UUID{}, errors.Errorf("key %q has leftover bytes after decode: %s; indicates corrupt key", key, detail)
	}
	txnID, err := uuid.FromBytes(idBytes)
	return txnID, err
}

// RangeAppliedStateKey returns a system-local key for the range applied state key.
// RangeAppliedStateKey 返回 Range 已应用状态的系统本地键
// 记录 Raft 日志已应用到状态机的位置，用于崩溃恢复时确定从哪里开始重放日志
func RangeAppliedStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeAppliedStateKey()
}

// RangeForceFlushKey returns a system-local key for the range force flush key.
// RangeForceFlushKey 返回 Range 强制刷盘的系统本地键
// 用于标记需要强制将数据刷写到磁盘
func RangeForceFlushKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeForceFlushKey()
}

// RangeLeaseKey returns a system-local key for a range lease.
// RangeLeaseKey 返回 Range 租约的系统本地键
// Range 租约决定哪个副本可以处理读写请求，是实现一致性和负载均衡的关键机制
func RangeLeaseKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeLeaseKey()
}

// RangePriorReadSummaryKey returns a system-local key for a range's prior read
// summary.
// RangePriorReadSummaryKey 返回 Range 先前读取摘要的系统本地键
// 用于优化闭时间戳（closed timestamp）的计算，跟踪之前的读取操作
func RangePriorReadSummaryKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangePriorReadSummaryKey()
}

// RangeGCThresholdKey returns a system-local key for last used GC threshold on the
// user keyspace. Reads and writes <= this timestamp will not be served.
// RangeGCThresholdKey 返回用户键空间上最后使用的 GC 阈值的系统本地键
// 早于或等于此时间戳的读写请求将不会被处理，因为数据可能已被垃圾回收
func RangeGCThresholdKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeGCThresholdKey()
}

// RangeGCHintKey returns a system-local key for GC hint data. This data is used
// by GC queue to adjust how replicas are being queued for GC.
// RangeGCHintKey 返回 GC 提示数据的系统本地键
// GC 队列使用此数据调整副本进入 GC 队列的方式，优化垃圾回收策略
func RangeGCHintKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeGCHintKey()
}

// MVCCRangeKeyGCKey returns a range local key protecting range
// tombstone mvcc stats calculations during range tombstone GC.
// MVCCRangeKeyGCKey 返回保护 Range 墓碑 MVCC 统计计算的 Range 本地键
// 在 Range 墓碑垃圾回收期间保护 MVCC 统计信息的计算
func MVCCRangeKeyGCKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).MVCCRangeKeyGCKey()
}

// RangeVersionKey returns a system-local for the range version.
// RangeVersionKey 返回 Range 版本的系统本地键
// 用于跟踪 Range 的版本信息，支持版本迁移和兼容性检查
func RangeVersionKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeVersionKey()
}

// MakeRangeIDUnreplicatedPrefix creates a range-local key prefix from
// rangeID for all unreplicated data.
// MakeRangeIDUnreplicatedPrefix 根据 Range ID 创建非复制数据的键前缀
// 用于存储不需要通过 Raft 复制的 Range 本地数据，如 Raft 日志本身
func MakeRangeIDUnreplicatedPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return makePrefixWithRangeID(LocalRangeIDPrefix, rangeID, localRangeIDUnreplicatedInfix)
}

// makeRangeIDUnreplicatedKey creates a range-local unreplicated key based
// on the range's Range ID, metadata key suffix, and optional detail.
// makeRangeIDUnreplicatedKey 创建基于 Range ID 的非复制键
// 这是一个内部函数，用于构造不需要通过 Raft 复制的 Range 本地键
func makeRangeIDUnreplicatedKey(
	rangeID roachpb.RangeID, suffix roachpb.RKey, detail roachpb.RKey,
) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}

	key := MakeRangeIDUnreplicatedPrefix(rangeID)
	key = append(key, suffix...)
	key = append(key, detail...)
	return key
}

// RangeTombstoneKey returns a system-local key for a range tombstone.
// RangeTombstoneKey 返回 Range 墓碑的系统本地键
// Range 墓碑标记一个 Range 已被删除（如合并后），防止旧副本重新激活
func RangeTombstoneKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeTombstoneKey()
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
// RaftTruncatedStateKey 返回 Raft 截断状态的系统本地键
// 记录 Raft 日志已被截断的位置，用于释放旧日志占用的空间
func RaftTruncatedStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftTruncatedStateKey()
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
// RaftHardStateKey 返回 Raft HardState 的系统本地键
// HardState 包含 Raft 协议的持久化状态，如当前任期、投票记录等
func RaftHardStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftHardStateKey()
}

// RaftLogPrefix returns the system-local prefix shared by all Entries
// in a Raft log.
// RaftLogPrefix 返回 Raft 日志所有条目共享的系统本地前缀
// 用于快速定位某个 Range 的所有 Raft 日志条目
func RaftLogPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftLogPrefix()
}

// RaftLogKey returns a system-local key for a Raft log entry.
// RaftLogKey 返回 Raft 日志条目的系统本地键
// 每个 Raft 日志条目都有一个唯一的索引，此函数根据索引生成对应的键
func RaftLogKey(rangeID roachpb.RangeID, logIndex kvpb.RaftIndex) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftLogKey(logIndex)
}

// RaftLogKeyFromPrefix returns a system-local key for a Raft log entry, using
// the provided Raft log prefix.
// RaftLogKeyFromPrefix 使用提供的 Raft 日志前缀返回 Raft 日志条目的系统本地键
// 性能优化版本，避免重复生成前缀
func RaftLogKeyFromPrefix(raftLogPrefix []byte, logIndex kvpb.RaftIndex) roachpb.Key {
	return encoding.EncodeUint64Ascending(raftLogPrefix, uint64(logIndex))
}

// DecodeRaftLogKeyFromSuffix parses the suffix of a system-local key for a Raft
// log entry and returns the entry's log index.
// DecodeRaftLogKeyFromSuffix 解析 Raft 日志条目键的后缀并返回日志索引
func DecodeRaftLogKeyFromSuffix(raftLogSuffix []byte) (kvpb.RaftIndex, error) {
	_, logIndex, err := encoding.DecodeUint64Ascending(raftLogSuffix)
	return kvpb.RaftIndex(logIndex), err
}

// RaftReplicaIDKey returns a system-local key for a RaftReplicaID.
// RaftReplicaIDKey 返回 Raft 副本 ID 的系统本地键
// 存储此副本在 Raft 组中的唯一标识符
func RaftReplicaIDKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftReplicaIDKey()
}

// RangeLastReplicaGCTimestampKey returns a range-local key for
// the range's last replica GC timestamp.
// RangeLastReplicaGCTimestampKey 返回 Range 最后副本 GC 时间戳的 Range 本地键
// 记录上次对副本进行垃圾回收的时间，用于控制副本清理的频率
func RangeLastReplicaGCTimestampKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeLastReplicaGCTimestampKey()
}

// MakeRangeKey creates a range-local key based on the range
// start key, metadata key suffix, and optional detail (e.g. the
// transaction ID for a txn record, etc.).
// MakeRangeKey 创建基于 Range 起始键的 Range 本地键
// 这类键与特定的 Range 起始键关联，用于存储事务记录、队列处理时间等元数据
func MakeRangeKey(key, suffix, detail roachpb.RKey) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}
	buf := makeRangeKeyPrefixWithExtraCapacity(key, len(suffix)+len(detail))
	buf = append(buf, suffix...)
	buf = append(buf, detail...)
	return buf
}

// MakeRangeKeyPrefix creates a key prefix under which all range-local keys
// can be found.
// gcassert:inline
// MakeRangeKeyPrefix 创建 Range 本地键的前缀，所有 Range 本地键都在此前缀下
func MakeRangeKeyPrefix(key roachpb.RKey) roachpb.Key {
	return makeRangeKeyPrefixWithExtraCapacity(key, 0)
}

// makeRangeKeyPrefixWithExtraCapacity 创建带有额外容量的 Range 键前缀
// 这是一个内部优化函数，预分配额外空间以避免后续追加操作时重新分配内存
func makeRangeKeyPrefixWithExtraCapacity(key roachpb.RKey, extra int) roachpb.Key {
	keyLen := len(LocalRangePrefix) + encoding.EncodeBytesSize(key)
	buf := make(roachpb.Key, 0, keyLen+extra)
	buf = append(buf, LocalRangePrefix...)
	buf = encoding.EncodeBytesAscending(buf, key)
	return buf
}

// DecodeRangeKey decodes the range key into range start key,
// suffix and optional detail (may be nil).
// DecodeRangeKey 解码 Range 键，提取 Range 起始键、后缀和可选的详细信息
func DecodeRangeKey(key roachpb.Key) (startKey, suffix, detail roachpb.Key, err error) {
	if !bytes.HasPrefix(key, LocalRangePrefix) {
		return nil, nil, nil, errors.Errorf("key %q does not have %q prefix",
			key, LocalRangePrefix)
	}
	// Cut the prefix and the Range ID.
	b := key[len(LocalRangePrefix):]
	b, startKey, err = encoding.DecodeBytesAscending(b, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(b) < localSuffixLength {
		return nil, nil, nil, errors.Errorf("key %q does not have suffix of length %d",
			key, localSuffixLength)
	}
	// Cut the suffix.
	suffix = b[:localSuffixLength]
	detail = b[localSuffixLength:]
	return
}

// RangeDescriptorKey returns a range-local key for the descriptor
// for the range with specified key.
// RangeDescriptorKey 返回指定键所在 Range 的描述符键
// Range 描述符包含 Range 的边界、副本列表等关键元数据
func RangeDescriptorKey(key roachpb.RKey) roachpb.Key {
	return MakeRangeKey(key, LocalRangeDescriptorSuffix, nil)
}

// TransactionKey returns a transaction key based on the provided
// transaction key and ID. The base key is encoded in order to
// guarantee that all transaction records for a range sort together.
// TransactionKey 返回基于提供的事务键和 ID 的事务键
// 对基础键进行编码以确保一个 Range 的所有事务记录排序在一起
func TransactionKey(key roachpb.Key, txnID uuid.UUID) roachpb.Key {
	return MakeRangeKey(MustAddr(key), LocalTransactionSuffix, txnID.GetBytes())
}

// QueueLastProcessedKey returns a range-local key for last processed
// timestamps for the named queue. These keys represent per-range last
// processed times.
// QueueLastProcessedKey 返回指定队列的最后处理时间戳的 Range 本地键
// 这些键表示每个 Range 上队列的最后处理时间（如 GC 队列、复制队列等）
func QueueLastProcessedKey(key roachpb.RKey, queue string) roachpb.Key {
	return MakeRangeKey(key, LocalQueueLastProcessedSuffix, roachpb.RKey(queue))
}

// RangeProbeKey returns a range-local key for probing. The
// purpose of the key is to test CRDB in production; if any data is present at
// the key, it has no purpose except in allowing testing CRDB in production.
// RangeProbeKey 返回用于探测的 Range 本地键
// 此键的目的是在生产环境中测试 CRDB；如果此键处有任何数据，除了允许在生产环境中测试 CRDB 外没有其他用途
func RangeProbeKey(key roachpb.RKey) roachpb.Key {
	return MakeRangeKey(key, LocalRangeProbeSuffix, nil)
}

// LockTableSingleKey creates a key under which all single-key locks for the
// given key can be found. buf is used as scratch-space, up to its capacity,
// to avoid allocations -- its contents will be overwritten and not appended
// to.
// Note that there can be multiple locks for the given key, but those are
// distinguished using the "version" which is not in scope of the keys
// package.
// For a scan [start, end) the corresponding lock table scan is
// [LTSK(start), LTSK(end)).
// LockTableSingleKey 创建单键锁表键，该键下可以找到给定键的所有单键锁
// buf 用作临时空间（最多使用其容量）以避免分配 -- 其内容将被覆盖而不是追加
// 注意：给定键可以有多个锁，但这些锁通过"版本"区分，版本不在 keys 包的范围内
// 对于扫描 [start, end)，对应的锁表扫描是 [LTSK(start), LTSK(end))
func LockTableSingleKey(key roachpb.Key, buf []byte) (roachpb.Key, []byte) {
	keyLen := len(LocalRangeLockTablePrefix) + len(LockTableSingleKeyInfix) + encoding.EncodeBytesSize(key)
	if cap(buf) < keyLen {
		buf = make([]byte, 0, keyLen)
	} else {
		buf = buf[:0]
	}

	// Don't unwrap any local prefix on key using Addr(key). This allow for
	// doubly-local lock table keys. For example, local range descriptor keys can
	// be locked during split and merge transactions.
	buf = append(buf, LocalRangeLockTablePrefix...)
	buf = append(buf, LockTableSingleKeyInfix...)
	buf = encoding.EncodeBytesAscending(buf, key)
	return buf, buf
}

// LockTableSingleNextKey is equivalent to LockTableSingleKey(key.Next(), buf)
// but avoids an extra allocation in cases where key.Next() must allocate.
// LockTableSingleNextKey 等价于 LockTableSingleKey(key.Next(), buf)
// 但在 key.Next() 需要分配内存的情况下避免额外的分配，提高性能
func LockTableSingleNextKey(key roachpb.Key, buf []byte) (roachpb.Key, []byte) {
	keyLen := len(LocalRangeLockTablePrefix) + len(LockTableSingleKeyInfix) + encoding.EncodeNextBytesSize(key)
	if cap(buf) < keyLen {
		buf = make([]byte, 0, keyLen)
	} else {
		buf = buf[:0]
	}
	// Don't unwrap any local prefix on key using Addr(key). This allow for
	// doubly-local lock table keys. For example, local range descriptor keys can
	// be locked during split and merge transactions.
	buf = append(buf, LocalRangeLockTablePrefix...)
	buf = append(buf, LockTableSingleKeyInfix...)
	buf = encoding.EncodeNextBytesAscending(buf, key)
	return buf, buf
}

// DecodeLockTableSingleKey decodes the single-key lock table key to return the key
// that was locked.
// DecodeLockTableSingleKey 解码单键锁表键并返回被锁定的原始键
func DecodeLockTableSingleKey(key roachpb.Key) (lockedKey roachpb.Key, err error) {
	if !bytes.HasPrefix(key, LocalRangeLockTablePrefix) {
		return nil, errors.Errorf("key %q does not have %q prefix",
			key, LocalRangeLockTablePrefix)
	}
	// Cut the prefix.
	b := key[len(LocalRangeLockTablePrefix):]
	if !bytes.HasPrefix(b, LockTableSingleKeyInfix) {
		return nil, errors.Errorf("key %q is not for a single-key lock", key)
	}
	b = b[len(LockTableSingleKeyInfix):]
	// We pass nil as the second parameter instead of trying to reuse a
	// previously allocated buffer since escaping of \x00 to \x00\xff is not
	// common. And when there is no such escaping, lockedKey will be a sub-slice
	// of b.
	b, lockedKey, err = encoding.DecodeBytesAscending(b, nil)
	if err != nil {
		return nil, err
	}
	if len(b) != 0 {
		return nil, errors.Errorf("key %q has left-over bytes %d after decoding",
			key, len(b))
	}
	return lockedKey, err
}

// ValidateLockTableSingleKey is like DecodeLockTableSingleKey, except that it
// discards the decoded key. It returns nil iff the provided key is a valid
// single-key lock table key.
// ValidateLockTableSingleKey 类似于 DecodeLockTableSingleKey，但丢弃解码后的键
// 当且仅当提供的键是有效的单键锁表键时，返回 nil
func ValidateLockTableSingleKey(key roachpb.Key) error {
	if !bytes.HasPrefix(key, LocalRangeLockTablePrefix) {
		return errors.Errorf("key %q does not have %q prefix",
			key, LocalRangeLockTablePrefix)
	}
	// Cut the prefix.
	b := key[len(LocalRangeLockTablePrefix):]
	// Check that it's a single-key lock.
	if !bytes.HasPrefix(b, LockTableSingleKeyInfix) {
		return errors.Errorf("key %q is not for a single-key lock", key)
	}
	// Cut the prefix.
	b = b[len(LockTableSingleKeyInfix):]
	var err error
	b, err = encoding.ValidateDecodeBytesAscending(b)
	if err != nil {
		return err
	}
	if len(b) != 0 {
		return errors.Errorf("key %q has left-over bytes %d after decoding",
			key, len(b))
	}
	return nil
}

// IsLocal performs a cheap check that returns true iff a range-local key is
// passed, that is, a key for which `Addr` would return a non-identical RKey
// (or a decoding error).
//
// TODO(tschottdorf): we need a better name for these keys as only some of
// them are local and it's been identified as an area that is not understood
// by many of the team's developers. An obvious suggestion is "system" (as
// opposed to "user") keys, but unfortunately that name has already been
// claimed by a related (but not identical) concept.
// IsLocal 执行一个廉价的检查，当且仅当传入的是 Range 本地键时返回 true
// 即对于调用 `Addr` 会返回不同的 RKey（或解码错误）的键
//
// TODO(tschottdorf): 我们需要为这些键找一个更好的名字，因为只有其中一些是本地的
// 这被认为是团队许多开发人员不理解的领域。一个明显的建议是"系统"键（相对于"用户"键）
// 但不幸的是这个名字已经被一个相关（但不完全相同）的概念占用了
func IsLocal(k roachpb.Key) bool {
	return bytes.HasPrefix(k, LocalPrefix)
}

// Addr returns the address for the key, used to lookup the range containing the
// key. In the normal case, this is simply the key's value. However, for local
// keys, such as transaction records, the address is the inner encoded key, with
// the local key prefix and the suffix and optional detail removed. This address
// unwrapping is performed repeatedly in the case of doubly-local keys. In this
// way, local keys address to the same range as non-local keys, but are stored
// separately so that they don't collide with user-space or global system keys.
//
// Logically, the keys are arranged as follows:
//
// k1 /local/k1/KeyMin ... /local/k1/KeyMax k1\x00 /local/k1/x00/KeyMin ...
//
// However, not all local keys are addressable in the global map. Only range
// local keys incorporating a range key (start key or transaction key) are
// addressable (e.g. range metadata and txn records). Range local keys
// incorporating the Range ID are not (e.g. AbortSpan Entries, and range
// stats).
//
// See AddrUpperBound which is to be used when `k` is the EndKey of an interval.
// Addr 返回键的地址，用于查找包含该键的 Range
// 在正常情况下，这只是键的值。但对于本地键（如事务记录），地址是内部编码的键
// 去掉了本地键前缀、后缀和可选的详细信息。对于双重本地键，此地址解包会重复执行
// 通过这种方式，本地键寻址到与非本地键相同的 Range，但单独存储以避免与用户空间或全局系统键冲突
//
// 逻辑上，键的排列如下：
//
// k1 /local/k1/KeyMin ... /local/k1/KeyMax k1\x00 /local/k1/x00/KeyMin ...
//
// 但是，并非所有本地键都可在全局映射中寻址。只有包含 Range 键（起始键或事务键）的 Range 本地键
// 是可寻址的（例如 Range 元数据和事务记录）。包含 Range ID 的 Range 本地键不可寻址
// （例如 AbortSpan 条目和 Range 统计信息）
//
// 参见 AddrUpperBound，当 `k` 是区间的 EndKey 时应使用该函数
func Addr(k roachpb.Key) (roachpb.RKey, error) {
	if !IsLocal(k) {
		return roachpb.RKey(k), nil
	}

	for {
		if bytes.HasPrefix(k, LocalStorePrefix) {
			return nil, errors.Errorf("store-local key %q is not addressable", k)
		}
		if bytes.HasPrefix(k, LocalRangeIDPrefix) {
			return nil, errors.Errorf("local range ID key %q is not addressable", k)
		}
		if !bytes.HasPrefix(k, LocalRangePrefix) {
			return nil, errors.Errorf("local key %q malformed; should contain prefix %q",
				k, LocalRangePrefix)
		}
		k = k[len(LocalRangePrefix):]
		var err error
		// Decode the encoded key, throw away the suffix and detail.
		if _, k, err = encoding.DecodeBytesAscending(k, nil); err != nil {
			return nil, err
		}
		if !IsLocal(k) {
			break
		}
	}
	return roachpb.RKey(k), nil
}

// MustAddr calls Addr and panics on errors.
// MustAddr 调用 Addr 并在出错时 panic
// 用于确信键有效的场景，简化错误处理
func MustAddr(k roachpb.Key) roachpb.RKey {
	rk, err := Addr(k)
	if err != nil {
		panic(errors.Wrapf(err, "could not take address of '%s'", k))
	}
	return rk
}

// AddrUpperBound returns the address of an (exclusive) EndKey, used to lookup
// ranges containing the keys strictly smaller than that key. However, unlike
// Addr, it will return the following key that local range keys address to. This
// is necessary because range-local keys exist conceptually in the space between
// regular keys. Addr() returns the regular key that is just to the left of a
// range-local key, which is guaranteed to be located on the same range.
// AddrUpperBound() returns the regular key that is just to the right, which may
// not be on the same range but is suitable for use as the EndKey of a span
// involving a range-local key. The one exception to this is the local key
// prefix itself; that continues to return the left key as having that as the
// upper bound excludes all local keys.
//
// Logically, the keys are arranged as follows:
//
// k1 /local/k1/KeyMin ... /local/k1/KeyMax k1\x00 /local/k1/x00/KeyMin ...
//
// and so any end key /local/k1/x corresponds to an address-resolved end key of
// k1\x00, with the exception of /local/k1 itself (no suffix) which corresponds
// to an address-resolved end key of k1.
// AddrUpperBound 返回（不包含的）EndKey 的地址，用于查找包含严格小于该键的键的 Range
// 但与 Addr 不同，它将返回本地 Range 键寻址到的下一个键
// 这是必要的，因为 Range 本地键在概念上存在于常规键之间的空间中
// Addr() 返回 Range 本地键左侧的常规键，该键保证位于同一 Range 上
// AddrUpperBound() 返回右侧的常规键，该键可能不在同一 Range 上
// 但适合用作涉及 Range 本地键的跨度的 EndKey
// 唯一的例外是本地键前缀本身；它继续返回左侧的键，因为将其作为上界会排除所有本地键
//
// 逻辑上，键的排列如下：
//
// k1 /local/k1/KeyMin ... /local/k1/KeyMax k1\x00 /local/k1/x00/KeyMin ...
//
// 因此任何结束键 /local/k1/x 对应于地址解析后的结束键 k1\x00
// 但 /local/k1 本身（无后缀）除外，它对应于地址解析后的结束键 k1
func AddrUpperBound(k roachpb.Key) (roachpb.RKey, error) {
	rk, err := Addr(k)
	if err != nil {
		return rk, err
	}
	// If k is the RangeKeyPrefix, it excludes all range local keys under rk.
	// The Next() is not necessary.
	if IsLocal(k) && !k.Equal(MakeRangeKeyPrefix(rk)) {
		// The upper bound for a range-local key that addresses to key k
		// is the key directly after k.
		rk = rk.Next()
	}
	return rk, nil
}

// SpanAddr is like Addr, but it takes a Span instead of a single key and
// applies the key transformation to the start and end keys in the span,
// returning an RSpan.
// SpanAddr 类似于 Addr，但它接受一个 Span 而不是单个键
// 将键转换应用于跨度中的起始键和结束键，返回 RSpan
func SpanAddr(span roachpb.Span) (roachpb.RSpan, error) {
	rk, err := Addr(span.Key)
	if err != nil {
		return roachpb.RSpan{}, err
	}
	var rek roachpb.RKey
	if len(span.EndKey) > 0 {
		rek, err = Addr(span.EndKey)
		if err != nil {
			return roachpb.RSpan{}, err
		}
	}
	return roachpb.RSpan{Key: rk, EndKey: rek}, nil
}

// RangeMetaKey returns a range metadata (meta1, meta2) indexing key for the
// given key.
//
// - For RKeyMin, KeyMin is returned.
// - For a meta1 key, KeyMin is returned.
// - For a meta2 key, a meta1 key is returned.
// - For an ordinary key, a meta2 key is returned.
//
// NOTE(andrei): This function has special handling for RKeyMin, but it does not
// handle RKeyMin.Next() properly: RKeyMin.Next() maps for a Meta2 key, rather
// than mapping to RKeyMin. This issue is not trivial to fix, because there's
// code that has come to rely on it: kvclient.ScanMetaKVs(RKeyMin,RKeyMax) ends up
// scanning from RangeMetaKey(RkeyMin.Next()), and what it wants is to scan only
// the Meta2 ranges. Even if it were fine with also scanning Meta1, there's
// other problems: a scan from RKeyMin is rejected by the store because it mixes
// local and non-local keys. The [KeyMin,localPrefixByte) key space doesn't
// really exist and we should probably have the first range start at
// meta1PrefixByte, not at KeyMin, but it might be too late for that.
// RangeMetaKey 返回给定键的 Range 元数据（meta1, meta2）索引键
//
// - 对于 RKeyMin，返回 KeyMin
// - 对于 meta1 键，返回 KeyMin
// - 对于 meta2 键，返回 meta1 键
// - 对于普通键，返回 meta2 键
//
// 注意(andrei): 此函数对 RKeyMin 有特殊处理，但它不能正确处理 RKeyMin.Next()：
// RKeyMin.Next() 映射为 Meta2 键，而不是映射到 RKeyMin。这个问题不容易修复
// 因为有代码依赖于此：kvclient.ScanMetaKVs(RKeyMin,RKeyMax) 最终从 RangeMetaKey(RkeyMin.Next()) 开始扫描
// 它只想扫描 Meta2 范围。即使它也可以扫描 Meta1，还有其他问题：
// 从 RKeyMin 开始的扫描会被存储拒绝，因为它混合了本地和非本地键
// [KeyMin,localPrefixByte) 键空间实际上并不存在，我们可能应该让第一个 Range 从 meta1PrefixByte 开始
// 而不是 KeyMin，但现在可能已经太晚了
func RangeMetaKey(key roachpb.RKey) roachpb.RKey {
	if len(key) == 0 { // key.Equal(roachpb.RKeyMin)
		return roachpb.RKeyMin
	}
	var prefix roachpb.Key
	switch key[0] {
	case meta1PrefixByte:
		return roachpb.RKeyMin
	case meta2PrefixByte:
		prefix = Meta1Prefix
		key = key[len(Meta2Prefix):]
	default:
		prefix = Meta2Prefix
	}

	buf := make(roachpb.RKey, 0, len(prefix)+len(key))
	buf = append(buf, prefix...)
	buf = append(buf, key...)
	return buf
}

// UserKey returns an ordinary key for the given range metadata (meta1, meta2)
// indexing key.
//
// - For RKeyMin, Meta1Prefix is returned.
// - For a meta1 key, a meta2 key is returned.
// - For a meta2 key, an ordinary key is returned.
// - For an ordinary key, the input key is returned.
// UserKey 为给定的 Range 元数据（meta1, meta2）索引键返回普通键
//
// - 对于 RKeyMin，返回 Meta1Prefix
// - 对于 meta1 键，返回 meta2 键
// - 对于 meta2 键，返回普通键
// - 对于普通键，返回输入键
func UserKey(key roachpb.RKey) roachpb.RKey {
	if len(key) == 0 { // key.Equal(roachpb.RKeyMin)
		return roachpb.RKey(Meta1Prefix)
	}
	var prefix roachpb.Key
	switch key[0] {
	case meta1PrefixByte:
		prefix = Meta2Prefix
		key = key[len(Meta1Prefix):]
	case meta2PrefixByte:
		key = key[len(Meta2Prefix):]
	}

	buf := make(roachpb.RKey, 0, len(prefix)+len(key))
	buf = append(buf, prefix...)
	buf = append(buf, key...)
	return buf
}

// InMeta1 returns true iff a key is in the meta1 range (which includes RKeyMin).
// InMeta1 当且仅当键在 meta1 范围内（包括 RKeyMin）时返回 true
func InMeta1(k roachpb.RKey) bool {
	return k.Equal(roachpb.RKeyMin) || bytes.HasPrefix(k, MustAddr(Meta1Prefix))
}

// validateRangeMetaKey validates that the given key is a valid Range Metadata
// key. This checks only the constraints common to forward and backwards scans:
// correct prefix and not exceeding KeyMax.
// validateRangeMetaKey 验证给定的键是否为有效的 Range 元数据键
// 仅检查前向和后向扫描的公共约束：正确的前缀且不超过 KeyMax
func validateRangeMetaKey(key roachpb.RKey) error {
	// KeyMin is a valid key.
	if key.Equal(roachpb.RKeyMin) {
		return nil
	}
	// Key must be at least as long as Meta1Prefix.
	if len(key) < len(Meta1Prefix) {
		return NewInvalidRangeMetaKeyError("too short", key)
	}

	prefix, body := key[:len(Meta1Prefix)], key[len(Meta1Prefix):]
	if !prefix.Equal(Meta2Prefix) && !prefix.Equal(Meta1Prefix) {
		return NewInvalidRangeMetaKeyError("not a meta key", key)
	}

	if roachpb.RKeyMax.Less(body) {
		return NewInvalidRangeMetaKeyError("body of meta key range lookup is > KeyMax", key)
	}
	return nil
}

// MetaScanBounds returns the range [start,end) within which the desired meta
// record can be found by means of an engine scan. The given key must be a
// valid RangeMetaKey as defined by validateRangeMetaKey.
// TODO(tschottdorf): a lot of casting going on inside.
// MetaScanBounds 返回通过引擎扫描可以找到所需元记录的范围 [start,end)
// 给定的键必须是 validateRangeMetaKey 定义的有效 RangeMetaKey
// TODO(tschottdorf): 内部有很多类型转换
func MetaScanBounds(key roachpb.RKey) (roachpb.RSpan, error) {
	if err := validateRangeMetaKey(key); err != nil {
		return roachpb.RSpan{}, err
	}

	if key.Equal(Meta2KeyMax) {
		return roachpb.RSpan{},
			NewInvalidRangeMetaKeyError("Meta2KeyMax can't be used as the key of scan", key)
	}

	if key.Equal(roachpb.RKeyMin) {
		// Special case KeyMin: find the first entry in meta1.
		return roachpb.RSpan{
			Key:    roachpb.RKey(Meta1Prefix),
			EndKey: roachpb.RKey(Meta1Prefix.PrefixEnd()),
		}, nil
	}
	if key.Equal(Meta1KeyMax) {
		// Special case Meta1KeyMax: this is the last key in Meta1, we don't want
		// to start at Next().
		return roachpb.RSpan{
			Key:    roachpb.RKey(Meta1KeyMax),
			EndKey: roachpb.RKey(Meta1Prefix.PrefixEnd()),
		}, nil
	}

	// Otherwise find the first entry greater than the given key in the same meta prefix.
	start := key.Next()
	end := key[:len(Meta1Prefix)].PrefixEnd()
	return roachpb.RSpan{Key: start, EndKey: end}, nil
}

// MetaReverseScanBounds returns the range [start,end) within which the desired
// meta record can be found by means of a reverse engine scan. The given key
// must be a valid RangeMetaKey as defined by validateRangeMetaKey.
// MetaReverseScanBounds 返回通过反向引擎扫描可以找到所需元记录的范围 [start,end)
// 给定的键必须是 validateRangeMetaKey 定义的有效 RangeMetaKey
func MetaReverseScanBounds(key roachpb.RKey) (roachpb.RSpan, error) {
	if err := validateRangeMetaKey(key); err != nil {
		return roachpb.RSpan{}, err
	}

	if key.Equal(roachpb.RKeyMin) || key.Equal(Meta1Prefix) {
		return roachpb.RSpan{},
			NewInvalidRangeMetaKeyError("KeyMin and Meta1Prefix can't be used as the key of reverse scan", key)
	}
	if key.Equal(Meta2Prefix) {
		// Special case Meta2Prefix: this is the first key in Meta2, and the scan
		// interval covers all of Meta1.
		return roachpb.RSpan{
			Key:    roachpb.RKey(Meta1Prefix),
			EndKey: roachpb.RKey(key.Next().AsRawKey()),
		}, nil
	}

	// Otherwise find the first entry greater than the given key and find the last entry
	// in the same prefix. For MVCCReverseScan the endKey is exclusive, if we want to find
	// the range descriptor the given key specified,we need to set the key.Next() as the
	// MVCCReverseScan`s endKey. For example:
	// If we have ranges [a,f) and [f,z), then we'll have corresponding meta records
	// at f and z. If you're looking for the meta record for key f, then you want the
	// second record (exclusive in MVCCReverseScan), hence key.Next() below.
	start := key[:len(Meta1Prefix)]
	end := key.Next()
	return roachpb.RSpan{Key: start, EndKey: end}, nil
}

// MakeTableIDIndexID returns the key for the table id and index id by appending
// to the passed key. The key must already contain a tenant id.
// MakeTableIDIndexID 通过追加到传入的键返回表 ID 和索引 ID 的键
// 键必须已包含租户 ID
func MakeTableIDIndexID(key []byte, tableID uint32, indexID uint32) []byte {
	key = encoding.EncodeUvarintAscending(key, uint64(tableID))
	key = encoding.EncodeUvarintAscending(key, uint64(indexID))
	return key
}

// MakeFamilyKey returns the key for the family in the given row by appending to
// the passed key.
// MakeFamilyKey 通过追加到传入的键返回给定行中列族的键
func MakeFamilyKey(key []byte, famID uint32) []byte {
	if famID == 0 {
		// As an optimization, family 0 is encoded without a length suffix.
		return encoding.EncodeUvarintAscending(key, 0)
	}
	size := len(key)
	key = encoding.EncodeUvarintAscending(key, uint64(famID))
	// Note that we assume that `len(key)-size` will always be encoded to a
	// single byte by EncodeUvarint. This is currently always true because the
	// varint encoding will encode 1-9 bytes.
	return encoding.EncodeUvarintAscending(key, uint64(len(key)-size))
}

// DecodeFamilyKey returns the family ID in the given row key. Returns an error
// if the key does not contain a family ID.
// DecodeFamilyKey 返回给定行键中的列族 ID
// 如果键不包含列族 ID，则返回错误
func DecodeFamilyKey(key []byte) (uint32, error) {
	n, err := GetRowPrefixLength(key)
	if err != nil {
		return 0, err
	}
	if n <= 0 || n >= len(key) {
		return 0, errors.Errorf("invalid row prefix, got prefix length %d for key %s", n, key)
	}
	_, colFamilyID, err := encoding.DecodeUvarintAscending(key[n:])
	if err != nil {
		return 0, err
	}
	if colFamilyID > math.MaxUint32 {
		return 0, errors.Errorf("column family ID overflow, got %d", colFamilyID)
	}
	return uint32(colFamilyID), nil
}

// DecodeTableIDIndexID decodes a table id followed by an index id from the
// provided key. The input key must already have its tenant id removed.
// DecodeTableIDIndexID 从提供的键中解码表 ID 和索引 ID
// 输入键必须已移除租户 ID
func DecodeTableIDIndexID(key []byte) ([]byte, uint32, uint32, error) {
	var tableID uint64
	var indexID uint64
	var err error

	key, tableID, err = encoding.DecodeUvarintAscending(key)
	if err != nil {
		return nil, 0, 0, err
	}
	key, indexID, err = encoding.DecodeUvarintAscending(key)
	if err != nil {
		return nil, 0, 0, err
	}
	return key, uint32(tableID), uint32(indexID), nil
}

// GetRowPrefixLength returns the length of the row prefix of the key. A table
// key's row prefix is defined as the maximal prefix of the key that is also a
// prefix of every key for the same row. (Any key with this maximal prefix is
// also guaranteed to be part of the input key's row.)
// For secondary index keys, the row prefix is defined as the entire key.
// GetRowPrefixLength 返回键的行前缀长度
// 表键的行前缀定义为键的最大前缀，该前缀也是同一行的每个键的前缀
// （任何具有此最大前缀的键也保证是输入键的行的一部分）
// 对于二级索引键，行前缀定义为整个键
func GetRowPrefixLength(key roachpb.Key) (int, error) {
	n := len(key)

	// Strip tenant ID prefix to get a "SQL key" starting with a table ID.
	sqlKey, _, err := DecodeTenantPrefix(key)
	if err != nil {
		return 0, errors.Errorf("%s: not a valid table key", key)
	}
	sqlN := len(sqlKey)

	// Check that the prefix contains a valid TableID.
	if encoding.PeekType(sqlKey) != encoding.Int {
		// Not a table key, so the row prefix is the entire key.
		return n, nil
	}
	tableIDLen, err := encoding.GetUvarintLen(sqlKey)
	if err != nil {
		return 0, err
	}

	// Check whether the prefix contains a valid IndexID after the TableID. Not
	// all keys contain an index ID.
	if encoding.PeekType(sqlKey[tableIDLen:]) != encoding.Int {
		return n, nil
	}
	indexIDLen, err := encoding.GetUvarintLen(sqlKey[tableIDLen:])
	if err != nil {
		return 0, err
	}
	// If the IndexID is the last part of the key, the entire key is the prefix.
	if tableIDLen+indexIDLen == sqlN {
		return n, nil
	}

	// The column family ID length is encoded as a varint and we take advantage
	// of the fact that the column family ID itself will be encoded in 0-9 bytes
	// and thus the length of the column family ID data will fit in a single
	// byte.
	colFamIDLenByte := sqlKey[sqlN-1:]
	if encoding.PeekType(colFamIDLenByte) != encoding.Int {
		// The last byte is not a valid column family ID suffix.
		return 0, errors.Errorf("%s: not a valid column family ID suffix", key)
	}

	// Strip off the column family ID suffix from the buf. The last byte of the
	// buf contains the length of the column family ID suffix, which might be 0
	// if the buf does not contain a column ID suffix or if the column family is
	// 0 (see the optimization in MakeFamilyKey).
	_, colFamIDLen, err := encoding.DecodeUvarintAscending(colFamIDLenByte)
	if err != nil {
		return 0, errors.Wrapf(err, "could not decode column family ID length")
	}
	// Note how this next comparison (and by extension the code after it) is
	// overflow-safe. There are more intuitive ways of writing this that aren't
	// as safe. See #18628.
	if colFamIDLen > uint64(sqlN-1) {
		// The column family ID length was impossible. colFamIDLen is the length
		// of the encoded column family ID suffix. We add 1 to account for the
		// byte holding the length of the encoded column family ID and if that
		// total (colFamIDLen+1) is greater than the key suffix (sqlN ==
		// len(sqlKey)) then we bail. Note that we don't consider this an error
		// because EnsureSafeSplitKey can be called on keys that look like table
		// keys but which do not have a column family ID length suffix (e.g by
		// SystemConfig.ComputeSplitKey).
		return 0, errors.Errorf("%s: malformed table key", key)
	}
	return n - int(colFamIDLen) - 1, nil
}

// EnsureSafeSplitKey transforms an SQL table key such that it is a valid split key
// (i.e. does not occur in the middle of a row).
// EnsureSafeSplitKey 转换 SQL 表键，使其成为有效的分裂键
// （即不出现在行的中间）
func EnsureSafeSplitKey(key roachpb.Key) (roachpb.Key, error) {
	// The row prefix for a key is unique to keys in its row - no key without the
	// row prefix will be in the key's row. Therefore, we can be certain that
	// using the row prefix for a key as a split key is safe: it doesn't occur in
	// the middle of a row.
	idx, err := GetRowPrefixLength(key)
	if err != nil {
		return nil, err
	}
	return key[:idx], nil
}

// Range returns a key range encompassing the key ranges of all requests.
// Range 返回包含所有请求的键范围的键范围
func Range(reqs []kvpb.RequestUnion) (roachpb.RSpan, error) {
	from := roachpb.RKeyMax
	to := roachpb.RKeyMin
	for _, arg := range reqs {
		req := arg.GetInner()
		h := req.Header()
		if !kvpb.IsRange(req) && len(h.EndKey) != 0 {
			return roachpb.RSpan{}, errors.Errorf("end key specified for non-range operation: %s", req)
		}

		key, err := Addr(h.Key)
		if err != nil {
			return roachpb.RSpan{}, err
		}
		if key.Less(from) {
			// Key is smaller than `from`.
			from = key
		}
		if !key.Less(to) {
			// Key.Next() is larger than `to`.
			if bytes.Compare(key, roachpb.RKeyMax) > 0 {
				return roachpb.RSpan{}, errors.Errorf("%s must be less than KeyMax", key)
			}
			to = key.Next()
		}

		if len(h.EndKey) == 0 {
			continue
		}
		endKey, err := AddrUpperBound(h.EndKey)
		if err != nil {
			return roachpb.RSpan{}, err
		}
		if bytes.Compare(roachpb.RKeyMax, endKey) < 0 {
			return roachpb.RSpan{}, errors.Errorf("%s must be less than or equal to KeyMax", endKey)
		}
		if to.Less(endKey) {
			// EndKey is larger than `to`.
			to = endKey
		}
	}
	return roachpb.RSpan{Key: from, EndKey: to}, nil
}

// RangeIDPrefixBuf provides methods for generating range ID local keys while
// avoiding an allocation on every key generated. The generated keys are only
// valid until the next call to one of the key generation methods.
// RangeIDPrefixBuf 提供生成 Range ID 本地键的方法，同时避免每次生成键时的分配
// 生成的键仅在下次调用键生成方法之前有效
type RangeIDPrefixBuf roachpb.Key

// MakeRangeIDPrefixBuf creates a new range ID prefix buf suitable for
// generating the various range ID local keys.
// MakeRangeIDPrefixBuf 创建一个新的 Range ID 前缀缓冲区，用于生成各种 Range ID 本地键
func MakeRangeIDPrefixBuf(rangeID roachpb.RangeID) RangeIDPrefixBuf {
	return RangeIDPrefixBuf(MakeRangeIDPrefix(rangeID))
}

// replicatedPrefix 返回复制数据的前缀
func (b RangeIDPrefixBuf) replicatedPrefix() roachpb.Key {
	return append(roachpb.Key(b), LocalRangeIDReplicatedInfix...)
}

// unreplicatedPrefix 返回非复制数据的前缀
func (b RangeIDPrefixBuf) unreplicatedPrefix() roachpb.Key {
	return append(roachpb.Key(b), localRangeIDUnreplicatedInfix...)
}

// AbortSpanKey returns a range-local key by Range ID for an AbortSpan
// entry, with detail specified by encoding the supplied transaction ID.
// AbortSpanKey 返回 AbortSpan 条目的 Range 本地键
// 通过编码提供的事务 ID 指定详细信息
func (b RangeIDPrefixBuf) AbortSpanKey(txnID uuid.UUID) roachpb.Key {
	key := append(b.replicatedPrefix(), LocalAbortSpanSuffix...)
	return encoding.EncodeBytesAscending(key, txnID.GetBytes())
}

// ReplicatedSharedLocksTransactionLatchingKey returns a range-local key, by
// range ID, for a key on which all replicated shared locking requests from a
// specific transaction should serialize on latches. The per-transaction bit is
// achieved by encoding the supplied transaction ID into the key.
// ReplicatedSharedLocksTransactionLatchingKey 返回 Range 本地键
// 用于所有来自特定事务的复制共享锁请求应在闩锁上串行化的键
// 通过将提供的事务 ID 编码到键中来实现每事务位
func (b RangeIDPrefixBuf) ReplicatedSharedLocksTransactionLatchingKey(txnID uuid.UUID) roachpb.Key {
	key := append(b.replicatedPrefix(), LocalReplicatedSharedLocksTransactionLatchingKeySuffix...)
	return encoding.EncodeBytesAscending(key, txnID.GetBytes())
}

// RangeAppliedStateKey returns a system-local key for the range applied state key.
// See comment on RangeAppliedStateKey function.
// RangeAppliedStateKey 返回 Range 已应用状态键的系统本地键
// 参见 RangeAppliedStateKey 函数的注释
func (b RangeIDPrefixBuf) RangeAppliedStateKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeAppliedStateSuffix...)
}

// RangeForceFlushKey returns a system-local key for the range force flush
// key.
// RangeForceFlushKey 返回 Range 强制刷盘键的系统本地键
func (b RangeIDPrefixBuf) RangeForceFlushKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeForceFlushSuffix...)
}

// RangeLeaseKey returns a system-local key for a range lease.
// RangeLeaseKey 返回 Range 租约的系统本地键
func (b RangeIDPrefixBuf) RangeLeaseKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeLeaseSuffix...)
}

// RangePriorReadSummaryKey returns a system-local key for a range's prior read
// summary.
// RangePriorReadSummaryKey 返回 Range 先前读取摘要的系统本地键
func (b RangeIDPrefixBuf) RangePriorReadSummaryKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangePriorReadSummarySuffix...)
}

// RangeGCThresholdKey returns a system-local key for the GC threshold.
// RangeGCThresholdKey 返回 GC 阈值的系统本地键
func (b RangeIDPrefixBuf) RangeGCThresholdKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeGCThresholdSuffix...)
}

// RangeGCHintKey returns a range-local key for the GC hint data.
// RangeGCHintKey 返回 GC 提示数据的 Range 本地键
func (b RangeIDPrefixBuf) RangeGCHintKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeGCHintSuffix...)
}

// RangeVersionKey returns a system-local key for the range version.
// RangeVersionKey 返回 Range 版本的系统本地键
func (b RangeIDPrefixBuf) RangeVersionKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeVersionSuffix...)
}

// RangeTombstoneKey returns a system-local key for a range tombstone.
// RangeTombstoneKey 返回 Range 墓碑的系统本地键
func (b RangeIDPrefixBuf) RangeTombstoneKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRangeTombstoneSuffix...)
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
// RaftTruncatedStateKey 返回 Raft 截断状态的系统本地键
func (b RangeIDPrefixBuf) RaftTruncatedStateKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftTruncatedStateSuffix...)
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
// RaftHardStateKey 返回 Raft HardState 的系统本地键
func (b RangeIDPrefixBuf) RaftHardStateKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftHardStateSuffix...)
}

// RaftLogPrefix returns the system-local prefix shared by all Entries
// in a Raft log.
// RaftLogPrefix 返回 Raft 日志中所有条目共享的系统本地前缀
func (b RangeIDPrefixBuf) RaftLogPrefix() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftLogSuffix...)
}

// RaftLogKey returns a system-local key for a Raft log entry.
// RaftLogKey 返回 Raft 日志条目的系统本地键
func (b RangeIDPrefixBuf) RaftLogKey(logIndex kvpb.RaftIndex) roachpb.Key {
	return RaftLogKeyFromPrefix(b.RaftLogPrefix(), logIndex)
}

// RaftReplicaIDKey returns a system-local key for a RaftReplicaID.
// RaftReplicaIDKey 返回 Raft 副本 ID 的系统本地键
func (b RangeIDPrefixBuf) RaftReplicaIDKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftReplicaIDSuffix...)
}

// RangeLastReplicaGCTimestampKey returns a range-local key for
// the range's last replica GC timestamp.
// RangeLastReplicaGCTimestampKey 返回 Range 最后副本 GC 时间戳的 Range 本地键
func (b RangeIDPrefixBuf) RangeLastReplicaGCTimestampKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRangeLastReplicaGCTimestampSuffix...)
}

// MVCCRangeKeyGCKey returns a range local key protecting range
// tombstone mvcc stats calculations during range tombstone GC.
// MVCCRangeKeyGCKey 返回保护 Range 墓碑 MVCC 统计计算的 Range 本地键
// 在 Range 墓碑 GC 期间使用
func (b RangeIDPrefixBuf) MVCCRangeKeyGCKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRangeMVCCRangeKeyGCLockSuffix...)
}
