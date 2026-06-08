// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tsdumpmeta"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	yaml "gopkg.in/yaml.v2"
)

// maxBatchSize governs the maximum size of the kv batch comprising of ts data
// to be written to the DB.
const maxBatchSize = 10000

func maybeImportTS(ctx context.Context, s *topLevelServer) (returnErr error) {
	// We don't want to do startup retries as this is not meant to be run in
	// production.
	// 这是 debug/测试用的 timeseries 导入入口，不希望走正常生产启动重试检查。
	ctx = startup.WithoutChecks(ctx)

	// deferError 用来收集“非致命但需要报告”的错误，比如部分 metric/mapping 检查异常。
	var deferError func(error)
	{
		// defErr 聚合所有延迟返回的错误。
		var defErr error
		// 记录错误日志，并把错误合并到 defErr。
		deferError = func(err error) {
			log.Dev.Infof(ctx, "%v", err)
			defErr = errors.CombineErrors(defErr, err)
		}
		// 函数返回前，如果主体没有更严重的 returnErr，就返回聚合的延迟错误。
		defer func() {
			if returnErr == nil {
				returnErr = defErr
			}
		}()
	}
	// 读取 server testing knobs；timeseries 导入只通过 TestingKnobs 开启。
	knobs, _ := s.cfg.TestingKnobs.Server.(*TestingKnobs)
	// 没有 server knobs，说明未启用这个 debug 导入功能。
	if knobs == nil {
		return nil
	}
	// ImportTimeseriesFile 是要导入的 raw tsdump gob 文件路径。
	tsImport := knobs.ImportTimeseriesFile
	// 未指定导入文件时，正常启动，不做任何事。
	if tsImport == "" {
		return nil
	}

	// Suppress writing of node statuses for the local node (n1). If it wrote one,
	// and the imported data also contains n1 but with a different set of stores,
	// we'd effectively clobber the timeseries display for n1 (which relies on the
	// store statuses to map the store timeseries to the node under which they
	// fall). An alternative to this is setting a FirstStoreID and FirstNodeID that
	// is not in use in the data set to import.
	// 禁止当前本地节点写自己的 NodeStatus，避免覆盖导入数据里的历史 node/store 映射。
	s.node.suppressNodeStatus.Store(true)

	// Disable raft log synchronization to make the server generally faster.
	// 关闭 raft log sync，让 debug 导入大量 timeseries KV 时更快。
	logstore.DisableSyncRaftLog.Override(ctx, &s.cfg.Settings.SV, true)

	// Disable writing of new timeseries, as well as roll-ups and deletion.
	// 关闭正常 timeseries 写入，并把 TTL 拉长，防止导入过程中系统生成/rollup/删除 timeseries 数据。
	for _, stmt := range []string{
		"SET CLUSTER SETTING timeseries.storage.enabled = 'false';",
		"SET CLUSTER SETTING timeseries.storage.resolution_10s.ttl = '99999h';",
		"SET CLUSTER SETTING timeseries.storage.resolution_30m.ttl = '99999h';",
	} {
		// 通过 internal executor 执行 cluster setting 修改。
		if _, err := s.sqlServer.internalExecutor.ExecEx(
			ctx, "tsdump-cfg", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			stmt,
		); err != nil {
			// 任意 setting 设置失败都直接终止导入。
			return errors.Wrapf(err, "%s", stmt)
		}
	}
	// "-" 表示不重新导入，只允许启动后查看上一次已经加载到本地集群里的 timeseries 数据。
	if tsImport == "-" {
		// YOLO mode to look at timeseries after previous import error. This setting is used
		// to override errors seen during a prior run of tsdump. Since the timeseries data
		// has already been loaded during the previous tsdump run, we can simply return
		// here and allow the user to proceed with viewing the loaded timeseries data.
		return nil
	}
	// In practice we only allow populating time series in `start-single-node` due
	// to complexities detailed below. Additionally, we allow it only on a fresh
	// single-node single-store cluster and we also guard against join flags even
	// though there shouldn't be any.
	// 只允许在全新的单节点单 store 集群里导入，避免把历史 tsdump 混进已有生产/多节点数据。
	if !s.InitialStart() || len(s.cfg.JoinList) > 0 || len(s.cfg.Stores.Specs) != 1 {
		return errors.New("cannot import timeseries into an existing cluster or a multi-{store,node} cluster")
	}

	// 打开 raw tsdump gob 文件。
	f, err := os.Open(tsImport)
	if err != nil {
		return err
	}
	// 导入结束后关闭文件。
	defer f.Close()

	// Build store-to-node mapping either from an explicit YAML file or, if
	// not provided, from embedded metadata inside the tsdump (when present).
	// storeToNode 记录导入数据里 storeID 属于哪个 nodeID；DB Console 图表展示需要这个映射。
	storeToNode := map[roachpb.StoreID]roachpb.NodeID{}

	// Try to read embedded metadata from the tsdump file itself.
	// raw tsdump 可能在文件开头内嵌了 store->node 元数据，优先从 gob 中读取。
	dec := gob.NewDecoder(f)
	// 如果读到了内嵌 metadata，并且里面有 StoreToNodeMap，就使用它。
	if md, err := tsdumpmeta.Read(dec); err == nil && md != nil && len(md.StoreToNodeMap) > 0 {
		// 遍历 metadata 中的 storeID -> nodeID 字符串映射。
		for sID, nID := range md.StoreToNodeMap {
			// 把 store id 字符串转成整数。
			si, err1 := strconv.Atoi(strings.TrimSpace(sID))
			if err1 != nil {
				return errors.Wrapf(err1, "invalid store id in embedded metadata: %q:%q", sID, nID)
			}

			// 把 node id 字符串转成整数。
			ni, err2 := strconv.Atoi(strings.TrimSpace(nID))
			if err2 != nil {
				return errors.Wrapf(err2, "invalid node id in embedded metadata: %q:%q", sID, nID)
			}
			// 保存为 roachpb.StoreID -> roachpb.NodeID。
			storeToNode[roachpb.StoreID(si)] = roachpb.NodeID(ni)
		}
		log.Dev.Infof(ctx, "Using embedded store-to-node mapping from the tsdump file itself. "+
			"Skipped check for explicit store-to-node mapping file.")
	} else {
		// Reset file to beginning for subsequent decoding.
		// 没读到内嵌 metadata，要把文件 seek 回开头，后续继续按 timeseries KV 读取。
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		// 重新创建 gob decoder，从文件开头读取真正的 timeseries KV。
		dec = gob.NewDecoder(f)
		log.Dev.Infof(ctx, "Embedded metadata not found in the tsdump file. "+
			"Reading from the store-to-node mapping file.")

		// Fallback to explicit YAML mapping file.
		// 如果没有显式 mapping 文件路径，默认使用 "<tsdump>.yaml"。
		if knobs.ImportTimeseriesMappingFile == "" {
			knobs.ImportTimeseriesMappingFile = knobs.ImportTimeseriesFile + ".yaml"
		}
		// 读取 YAML 格式的 storeID -> nodeID 映射文件。
		mapBytes, err := os.ReadFile(knobs.ImportTimeseriesMappingFile)
		if err != nil {
			// mapping 文件不存在时，返回带操作说明的错误。
			if oserror.IsNotExist(err) {
				err = errors.Wrapf(err, "need to specify COCKROACH_DEBUG_TS_IMPORT_MAPPING_FILE; it should point at "+
					"a YAML file that maps StoreID to NodeID. To generate from the source cluster, run the following command:\n \n"+
					"cockroach sql --url \"<(unix/sql) url>\" --format tsv -e \\\n  \"select concat(store_id::string, ': ', node_id::string)"+
					"from crdb_internal.kv_store_status\" | \\\n  grep -E '[0-9]+: [0-9]+' | tee tsdump.gob.yaml\n \n"+
					"To create from a debug.zip file, run the following command:\n \n"+
					"tail -n +2 debug/crdb_internal.kv_store_status.txt | awk '{print $2 \": \" $1}' > tsdump.gob.yaml\n \n"+
					"Then export the created file: export COCKROACH_DEBUG_TS_IMPORT_MAPPING_FILE=tsdump.gob.yaml\n \n")
			}
			return err
		}
		// 把 YAML 内容解码到 storeToNode map。
		if err := yaml.NewDecoder(bytes.NewReader(mapBytes)).Decode(&storeToNode); err != nil {
			return err
		}
	}
	// batch 用来批量写入导入的 timeseries KV。
	batch := &kv.Batch{}

	// batchSize 记录当前 batch 中已经累计多少个 KV put。
	var batchSize int
	// maybeFlush 在 batch 达到 maxBatchSize 或 force=true 时把 batch 写入 DB。
	maybeFlush := func(force bool) error {
		// 当前 batch 为空，没有东西要写。
		if batchSize == 0 {
			// Nothing to write to DB.
			return nil
		}
		// batch 还没到阈值且不是强制 flush，就继续累积。
		if batchSize < maxBatchSize && !force {
			return nil
		}
		// 把当前 batch 通过 KV DB 写入本地集群。
		err := s.db.Run(ctx, batch)
		if err != nil {
			return err
		}
		// 记录本次导入数量。
		log.Dev.Infof(ctx, "imported %d ts pairs\n", batchSize)
		// 重置 batch 和计数，准备下一批。
		*batch, batchSize = kv.Batch{}, 0
		return nil
	}

	// nodeIDs 收集导入数据中出现过的 node source id。
	nodeIDs := map[string]struct{}{}
	// storeIDs 收集导入数据中出现过的 store source id。
	storeIDs := map[string]struct{}{}
	// nodeMetrics 收集 node 级别 metric 名称。
	nodeMetrics := map[string]bool{}
	// storeMetrics 收集 store 级别 metric 名称。
	storeMetrics := map[string]bool{}

	// node metric 的 timeseries name 前缀。
	nodeMetricIdentifier := "cr.node."
	// store metric 的 timeseries name 前缀。
	storeMetricIdentifier := "cr.store."

	// 持续从 gob 文件中读取 roachpb.KeyValue，直到 EOF。
	for {
		// v 是 tsdump 中的一条 raw timeseries KV。
		var v roachpb.KeyValue
		// 从 gob 文件中解码下一条 KV。
		err := dec.Decode(&v)
		if err != nil {
			// EOF 表示文件读完；强制 flush 最后一批 batch。
			if err == io.EOF {
				if err := maybeFlush(true /* force */); err != nil {
					return err
				}
				break
			}
			// 非 EOF 解码错误直接终止导入。
			return err
		}

		// 解码 timeseries key，取出 metric name 和 source。
		name, source, _, _, err := ts.DecodeDataKey(v.Key)
		if err != nil {
			// 单条 key 解码失败不立即退出，先记录延迟错误并跳过该 KV。
			deferError(err)
			continue
		}

		// node 级别 metric：source 表示 node id。
		if strings.HasPrefix(name, nodeMetricIdentifier) {
			nodeIDs[source] = struct{}{}
			// 去掉 cr.node. 前缀，保存 metric 短名。
			metricName := strings.Replace(name, nodeMetricIdentifier, "", 1)
			nodeMetrics[metricName] = true
			// store 级别 metric：source 表示 store id。
		} else if strings.HasPrefix(name, storeMetricIdentifier) {
			storeIDs[source] = struct{}{}
			// 去掉 cr.store. 前缀，保存 metric 短名。
			metricName := strings.Replace(name, storeMetricIdentifier, "", 1)
			storeMetrics[metricName] = true
		} else {
			// 只支持 cr.node.* 和 cr.store.* timeseries。
			deferError(errors.Errorf("unknown metric %s", name))
			continue
		}

		// 构造 inline PutRequest 写入原始 timeseries KV。
		p := kvpb.NewPut(v.Key, v.Value)
		// timeseries KV 使用 inline value，不带 MVCC timestamp。
		p.(*kvpb.PutRequest).Inline = true
		// 把原始请求加入 batch。
		batch.AddRawRequest(p)
		// 更新 batch 当前大小。
		batchSize++
		// batch 达到阈值时写入 DB。
		if err := maybeFlush(false /* force */); err != nil {
			return err
		}
	}

	// 根据 store->node 映射和导入数据中出现的 metric 名称，构造假的 NodeStatus/StoreStatus。
	fakeStatuses := makeFakeNodeStatuses(storeToNode, nodeMetrics, storeMetrics)
	// 检查 mapping 文件里的 stores 和实际导入数据里的 store metrics 是否匹配。
	if err := checkFakeStatuses(fakeStatuses, storeIDs); err != nil {
		// The checks are pretty strict and in particular make sure that there is at
		// least one data point for each store. Sometimes stores are down for the
		// time periods passed to tsdump or decommissioned nodes show up, etc; these
		// are important to point out but often the data set is actually OK and we
		// want to be able to view it.
		deferError(errors.Wrapf(err, "consider updating the mapping file %s or restarting the server with "+
			"COCKROACH_DEBUG_TS_IMPORT_FILE=- to ignore the error", knobs.ImportTimeseriesMappingFile))
	}

	// Write the statuses.
	// 把伪造的 NodeStatus 写入 KV；DB Console 需要它来把 node/store metric 对应到节点和 store。
	for _, status := range fakeStatuses {
		// 每个 NodeStatus 写到对应的 node status key。
		key := keys.NodeStatusKey(status.Desc.NodeID)
		// NodeStatus 是 inline 写入的系统状态数据。
		if err := s.db.PutInline(ctx, key, &status); err != nil {
			return err
		}
	}

	// 主体导入成功；如果 deferError 收集过错误，defer 会把它作为 returnErr 返回。
	return nil
}

func makeFakeNodeStatuses(
	storeToNode map[roachpb.StoreID]roachpb.NodeID,
	nodeMetrics map[string]bool,
	storeMetrics map[string]bool,
) []statuspb.NodeStatus {

	var sl []statuspb.NodeStatus
	nodeToStore := map[roachpb.NodeID][]roachpb.StoreID{}
	for sid, nid := range storeToNode {
		nodeToStore[nid] = append(nodeToStore[nid], sid)
	}

	for nodeID, storeIDs := range nodeToStore {
		sort.Slice(storeIDs, func(i, j int) bool {
			return storeIDs[i] < storeIDs[j]
		})
		nodeStatus := statuspb.NodeStatus{
			Desc: roachpb.NodeDescriptor{
				NodeID: nodeID,
			},
		}

		// Populate nodeStatus.Metrics and storeStatus.Metrics
		// with the metric names found in the dump.
		//
		// A metric name prefixed with the `storeMetricIdentifier` should be
		// included in both `nodeStatus.Metrics` and `storeStatus.Metrics`.
		//
		// A metric name prefixed with the `nodeMetricIdentifier` should only be
		// included in `nodeStatus.Metrics`.
		//
		// The value of the metric is not consequential here.

		if nodeMetrics != nil || storeMetrics != nil {
			nodeStatus.Metrics = map[string]float64{}
		}

		for metricName := range nodeMetrics {
			nodeStatus.Metrics[metricName] = 0.0
		}

		for metricName := range storeMetrics {
			nodeStatus.Metrics[metricName] = 0.0
		}

		for _, storeID := range storeIDs {
			storeStatus := statuspb.StoreStatus{
				Desc: roachpb.StoreDescriptor{
					Node:    nodeStatus.Desc, // don't want cycles here
					StoreID: storeID,
				},
			}

			if storeMetrics != nil {
				storeStatus.Metrics = map[string]float64{}
			}

			for metricName := range storeMetrics {
				storeStatus.Metrics[metricName] = 0.0
			}

			nodeStatus.StoreStatuses = append(nodeStatus.StoreStatuses, storeStatus)
		}

		sl = append(sl, nodeStatus)
	}
	sort.Slice(sl, func(i, j int) bool {
		return sl[i].Desc.NodeID < sl[j].Desc.NodeID
	})
	return sl
}

func checkFakeStatuses(fakeStatuses []statuspb.NodeStatus, storeIDs map[string]struct{}) error {
	for _, status := range fakeStatuses {
		for _, ss := range status.StoreStatuses {
			storeID := ss.Desc.StoreID
			strID := fmt.Sprint(storeID)
			if _, ok := storeIDs[strID]; !ok {
				// This is likely an mistake and where it isn't (for example since store
				// is long gone and hasn't supplied metrics in a long time) the user can
				// react by removing the assignment from the yaml file and trying again.
				return errors.Errorf(
					"s%d supplied in input mapping, but no timeseries found for it",
					ss.Desc.StoreID,
				)
			}

			delete(storeIDs, strID)
		}
	}
	if len(storeIDs) > 0 {
		return errors.Errorf(
			"need to map the remaining stores %v to nodes", storeIDs)
	}
	return nil
}
