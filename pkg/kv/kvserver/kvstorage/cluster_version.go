// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// WriteClusterVersion 将给定的集群版本写入引擎的最小版本文件（min version file）。
// 这个版本信息被持久化在磁盘上，用于下次启动时通过 MinVersion() 读取。
func WriteClusterVersion(
	ctx context.Context, eng storage.Engine, cv clusterversion.ClusterVersion,
) error {
	// SetMinVersion 是 Engine 接口的方法，负责在磁盘上标记该存储引擎
	// 能够兼容的最低程序版本。
	return eng.SetMinVersion(cv.Version)
}

// WriteClusterVersionToEngines 将给定的版本写入所有指定的引擎。
// 如果任何一个引擎写入失败，则返回遇到的第一个错误。
// 该函数不会对传入的版本进行校验（校验通常在调用方或 Synthesize 方法中完成）。
//
// 该函数的主要使用场景：
// 1. 引导启动（Bootstrap）：创建新集群时初始化版本。
// 2. 初始服务器启动：可能需要为新添加的存储引擎补全版本信息。
// 3. 集群版本升级：当管理员确认升级（Version Bump）时，将新版本持久化到所有磁盘。
func WriteClusterVersionToEngines(
	ctx context.Context, engines []storage.Engine, cv clusterversion.ClusterVersion,
) error {
	for _, eng := range engines {
		if err := WriteClusterVersion(ctx, eng, cv); err != nil {
			return errors.Wrapf(err, "error writing version to engine %s", eng)
		}
	}
	return nil
}

// SynthesizeClusterVersionFromEngines returns the cluster version that was read
// from the engines or, if none are initialized, binaryMinSupportedVersion.
// Typically all initialized engines will have the same version persisted,
// though ill-timed crashes can result in situations where this is not the
// case. Then, the largest version seen is returned.
//
// binaryVersion is the version of this binary. An error is returned if
// any engine has a higher version, as this would indicate that this node
// has previously acked the higher cluster version but is now running an
// old binary, which is unsafe.
//
// binaryMinSupportedVersion is the minimum version supported by this binary. An
// error is returned if any engine has a version lower that this.
// 代码遵循以下步骤来确定版本：
// 扫描所有引擎：循环检查节点上的所有硬盘存储（engines），读取它们记录的最小版本（MinVersion）。
// 上限检查（禁止降级）：
// 如果硬盘里的数据版本是 v23.1，但你尝试用 v22.2 的程序启动，代码会直接报错。
// 理由：一旦数据被新版本处理过，旧版本程序可能无法解析新格式，强制运行会导致数据损坏（Unsafe）。
// 确定最小版本：找到所有引擎中记录的最旧版本（minStoreVersion）。
// 下限检查（禁止跨度过大）：
// 每个程序都有一个最低支持版本（binaryMinSupportedVersion）。
// 如果硬盘数据太老了（比如是 3 年前的版本），新程序也会拒绝启动，要求你先进行中间版本的平滑升级。
// 初始化兜底：如果是一个全新的节点（没有任何数据），则默认使用该二进制程序支持的最小版本。
func SynthesizeClusterVersionFromEngines(
	ctx context.Context,
	engines []storage.Engine,
	binaryVersion, binaryMinSupportedVersion roachpb.Version,
) (clusterversion.ClusterVersion, error) {
	// Find the most recent bootstrap info.
	type originVersion struct {
		roachpb.Version
		origin string
	}

	maxPossibleVersion := roachpb.Version{Major: math.MaxInt32} // Sort above any real version.
	minStoreVersion := originVersion{
		Version: maxPossibleVersion,
		origin:  "(no store)",
	}

	// We run this twice because it's only after having seen all the versions
	// that we can decide whether the node catches a version error. However, we
	// also want to name at least one engine that violates the version
	// constraints, which at the latest the second loop will achieve (because
	// then minStoreVersion don't change any more).
	for _, eng := range engines {
		engVer := eng.MinVersion()
		if engVer == (roachpb.Version{}) {
			return clusterversion.ClusterVersion{}, errors.AssertionFailedf("store %s has no version", eng)
		}

		// Avoid running a binary with a store that is too new. For example,
		// restarting into 1.1 after having upgraded to 1.2 doesn't work.
		if binaryVersion.Less(engVer) {
			return clusterversion.ClusterVersion{}, errors.Errorf(
				"cockroach version v%s is incompatible with data in store %s; use version v%s or later",
				binaryVersion, eng, engVer)
		}

		// Track smallest use version encountered.
		if engVer.Less(minStoreVersion.Version) {
			minStoreVersion.Version = engVer
			minStoreVersion.origin = fmt.Sprint(eng)
		}
	}

	// If no use version was found, fall back to our binaryMinSupportedVersion. This
	// is the case when a brand new node is joining an existing cluster (which
	// may be on any older version this binary supports).
	if minStoreVersion.Version == maxPossibleVersion {
		minStoreVersion.Version = binaryMinSupportedVersion
	}

	cv := clusterversion.ClusterVersion{
		Version: minStoreVersion.Version,
	}
	log.Eventf(ctx, "read clusterVersion %+v", cv)

	if minStoreVersion.Version.Less(binaryMinSupportedVersion) {
		// We now check for old versions before opening the store. This case should
		// no longer be possible.
		return clusterversion.ClusterVersion{}, errors.AssertionFailedf("store %s, last used with cockroach version v%s, "+
			"is too old for running version v%s (which requires data from v%s or later)",
			minStoreVersion.origin, minStoreVersion.Version, binaryVersion, binaryMinSupportedVersion)
	}
	return cv, nil
}
