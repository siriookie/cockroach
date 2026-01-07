// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"storj.io/drpc/drpcclient"
)

// ErrClusterInitialized is reported when the Bootstrap RPC is run on
// a node that is already part of an initialized cluster.
var ErrClusterInitialized = fmt.Errorf("cluster has already been initialized")

// ErrIncompatibleBinaryVersion is returned when a CRDB node with a binary version X
// attempts to join a cluster with an active version that's higher. This is not
// allowed.
var ErrIncompatibleBinaryVersion = fmt.Errorf("binary is incompatible with the cluster attempted to join")

// initServer handles the bootstrapping process. It is instantiated early in the
// server startup sequence to determine whether a NodeID and ClusterID are
// available (true if and only if an initialized store is present). If all
// engines are empty, either a new cluster needs to be started (via incoming
// Bootstrap RPC) or an existing one joined (via the outgoing Join RPC). Either
// way, the goal is to learn a ClusterID and NodeID (and initialize at least one
// store). All of this subtlety is encapsulated by the initServer, which offers
// a primitive ServeAndWait() after which point the startup code can assume that
// the Node/ClusterIDs are known.
type initServer struct {
	log.AmbientContext
	// config houses a few configuration options needed by the init server.
	config initServerCfg

	mu struct {
		// This mutex is used to serialize bootstrap attempts.
		syncutil.Mutex

		// We use this field to guard against doubly bootstrapping clusters.
		bootstrapped bool

		// If we encounter an unrecognized error during bootstrap, we use this
		// field to block out future bootstrap attempts.
		rejectErr error
	}

	// inspectedDiskState captures the relevant bits of the on-disk state needed
	// by the init server. It's through this that the init server knows whether
	// or not this node needs to be bootstrapped. It does so by checking to see
	// if any engines were already initialized. If so, there's nothing left for
	// the init server to, it simply returns the inspected disk state in
	// ServeAndWait.
	//
	// Another function the inspected disk state provides is that it relays the
	// synthesized cluster version (this binary's minimum supported version if
	// there are no initialized engines). This is used as the cluster version if
	// we end up connecting to an existing cluster via gossip.
	//
	// TODO(irfansharif): The above function goes away once we remove the use of
	// gossip to join running clusters in 21.1.
	inspectedDiskState *initState

	// If this CRDB node was `cockroach init`-ialized, the resulting init state
	// will be passed through to this channel.
	bootstrapReqCh chan *initState
}

// NeedsBootstrap returns true if we haven't already been bootstrapped or
// haven't yet been able to join a running cluster.
func (s *initServer) NeedsBootstrap() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return !s.mu.bootstrapped
}

func newInitServer(
	actx log.AmbientContext, inspectedDiskState *initState, config initServerCfg,
) *initServer {
	initServer := &initServer{
		AmbientContext:     actx,
		bootstrapReqCh:     make(chan *initState, 1),
		config:             config,
		inspectedDiskState: inspectedDiskState,
	}
	// If we were already bootstrapped, we mark ourselves as such to prevent
	// future bootstrap attempts.
	if inspectedDiskState.bootstrapped() {
		initServer.mu.bootstrapped = true
	}
	return initServer
}

// initState is the entirety of what the init server is tasked with
// constructing. It's a view of our on-disk state, instantiated through
// inspectEngines (and inspectEngines alone).
//
// The init server is tasked with durably persisting state on-disk when this
// node is bootstrapped, or is able to join an already bootstrapped cluster.
// By state here we mean the cluster ID, node ID, at least one initialized
// engine, etc. After having persisted the relevant state, the init server
// constructs an initState with the details needed to fully start up a CRDB
// server.
type initState struct {
	nodeID               roachpb.NodeID
	clusterID            uuid.UUID
	clusterVersion       clusterversion.ClusterVersion
	initializedEngines   []storage.Engine
	uninitializedEngines []storage.Engine
	initialSettingsKVs   []roachpb.KeyValue
	initType             serverpb.InitType
}

// bootstrapped is a shorthand to check if there exists at least one initialized
// engine.
func (i *initState) bootstrapped() bool {
	return len(i.initializedEngines) > 0
}

// validate asserts that the init state is a fully fleshed out one (i.e. with a
// non-empty cluster ID and node ID).
func (i *initState) validate() error {
	if (i.clusterID == uuid.UUID{}) {
		return errors.New("missing cluster ID")
	}
	if i.nodeID == 0 {
		return errors.New("missing node ID")
	}
	return nil
}

// joinResult is used to represent the result of a node attempting to join
// an already bootstrapped cluster.
type joinResult struct {
	state *initState
	err   error
}

// ServeAndWait waits until the server is initialized, i.e. has a cluster ID,
// node ID and has permission to join the cluster. In the common case of
// restarting an existing node, this immediately returns. When starting with a
// blank slate (i.e. only empty engines), it waits for incoming Bootstrap
// request or for a successful outgoing Join RPC, whichever happens earlier.
//
// The returned initState reflects a bootstrapped cluster (i.e. it has a cluster
// ID and a node ID for this server).
//
// This method must be called only once.
//
// NB: A gotcha that may not immediately be obvious is that we can never hope to
// have all stores initialized by the time ServeAndWait returns. This is because
// if this server is already bootstrapped, it might hold a replica of the range
// backing the StoreID allocating counter, and letting this server start may be
// necessary to restore quorum to that range. So in general, after this method,
// we will always leave this method with at least one store initialized, but not
// necessarily all. This is fine, since initializing additional stores later is
// easy (see `initializeAdditionalStores`).
//
// `initialBoot` is true if this is a new node. This flag should only be used
// for logging and reporting. A newly bootstrapped single-node cluster is
// functionally equivalent to one that restarted; any decisions should be made
// on persisted data instead of this flag.
func (s *initServer) ServeAndWait(
	ctx context.Context, stopper *stop.Stopper, sv *settings.Values,
) (state *initState, initialBoot bool, err error) {

	// 中文解释：
	// 如果磁盘上已经检测到这是一个“已引导（bootstrapped）”的节点，
	// 说明这是节点重启场景，直接返回已有的初始化状态即可。
	if s.inspectedDiskState.bootstrapped() {
		return s.inspectedDiskState, false, nil
	}

	log.Dev.Info(ctx, "no stores initialized")
	log.Dev.Info(ctx, "awaiting `cockroach init` or join with an already initialized node")

	// 中文解释：
	// joinCh 用于接收 Join RPC 成功后的结果（加入已有集群）
	// cancelJoin 用于取消 Join 循环
	// wg 用于等待 Join goroutine 正常退出
	var joinCh chan joinResult
	var cancelJoin = func() {}
	var wg sync.WaitGroup

	if len(s.config.bootstrapAddresses) == 0 {
		// 中文解释：
		// 如果没有配置 bootstrapAddresses，说明：
		// 1）要么是单节点
		// 2）要么只指向自己
		// 这种情况下，节点不会主动去 Join 其他集群，
		// 而是等待运维人员手动执行 `cockroach init`
		// 所以不启动 join loop。
	} else {
		joinCh = make(chan joinResult, 1)
		wg.Add(1)

		var joinCtx context.Context
		joinCtx, cancelJoin = context.WithCancel(ctx)
		defer cancelJoin()

		// 中文解释：
		// 启动一个异步任务，循环尝试通过 Join RPC
		// 加入一个已经存在的 CockroachDB 集群
		err := stopper.RunAsyncTask(joinCtx, "init server: join loop",
			func(ctx context.Context) {
				defer wg.Done()

				state, err := s.startJoinLoop(ctx, stopper)
				joinCh <- joinResult{state: state, err: err}
			})
		if err != nil {
			wg.Done()
			return nil, false, err
		}
	}

	// 中文解释：
	// 进入主等待循环：
	// 等待三种事件之一发生：
	// 1）收到 Bootstrap 请求（我们自己创建新集群）
	// 2）Join RPC 成功（加入已有集群）
	// 3）节点被要求退出
	for {
		select {
		case state := <-s.bootstrapReqCh:
			// 中文解释：
			// 收到 bootstrap 请求，说明我们正在创建一个全新的集群。
			// 既然是我们自己引导集群，就不需要再继续 join 其他集群了。
			cancelJoin()
			wg.Wait()

			// 中文解释：
			// Bootstrap 已经把 bootstrapVersion 写入磁盘。
			// 现在可以安全地初始化 cluster version setting，
			// 将集群版本提升到 bootstrapVersion（通常就是当前二进制版本）。
			if err := clusterversion.Initialize(ctx, state.clusterVersion.Version, sv); err != nil {
				return nil, false, err
			}

			log.Dev.Infof(ctx, "cluster %s has been created", state.clusterID)
			log.Dev.Infof(ctx, "allocated node ID: n%d (for self)", state.nodeID)
			log.Dev.Infof(ctx, "active cluster version: %s", state.clusterVersion)

			// 中文解释：
			// 返回初始化状态：
			// - state：包含 clusterID、nodeID、clusterVersion
			// - initialBoot=true：表示这是一次“首次引导”
			return state, true, nil

		case result := <-joinCh:
			// 中文解释：
			// Join RPC 返回结果（成功或失败）
			wg.Wait()

			if err := result.err; err != nil {
				if errors.Is(err, ErrIncompatibleBinaryVersion) {
					return nil, false, err
				}

				// 中文解释：
				// 理论上 Join RPC 会自动重试所有网络错误。
				// 如果还能走到这里，说明发生了不该发生的错误，
				// 属于断言失败级别的问题。
				return nil, false, errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error")
			}

			state := result.state

			log.Dev.Infof(ctx, "joined cluster %s through join rpc", state.clusterID)
			log.Dev.Infof(ctx, "received node ID: %d", state.nodeID)
			log.Dev.Infof(ctx, "received cluster version: %s", state.clusterVersion)

			// 中文解释：
			// 成功加入已有集群：
			// - 获取 clusterID
			// - 被分配 nodeID
			// - 获取当前 cluster version
			return state, true, nil

		case <-stopper.ShouldQuiesce():
			// 中文解释：
			// 节点正在关闭（如进程退出、服务下线）
			return nil, false, stop.ErrUnavailable
		}
	}
}

var errInternalBootstrapError = errors.New("unable to bootstrap due to internal error")

// Bootstrap 实现了 serverpb.Init 服务接口。
// 用户通过调用该接口在集群中 **恰好一个节点** 上初始化一个新的 CRDB 集群
// （通常只在该节点上重试）。这个接口正是 `cockroach init` 命令的底层实现。
// 如果尝试对已经初始化过的节点再次 bootstrap，会返回 ErrClusterInitialized 错误。
//
// 注意：
//   - 该接口 **没有防止用户在多个节点上重复 bootstrap** 的保护。
//     如果用户错误地在多个节点上 bootstrap，会形成多个独立集群，
//     节点之间可能会 panic 或拒绝互相连接。
func (s *initServer) Bootstrap(
	ctx context.Context, r *serverpb.BootstrapRequest,
) (*serverpb.BootstrapResponse, error) {

	// 1. Bootstrap 只允许成功响应一次。
	// 其他请求会返回错误：
	//    - 成功 bootstrap 后：ErrClusterInitialized
	//    - 内部错误：errInternalBootstrapError

	s.mu.Lock()         // 加锁保护 initServer 的状态
	defer s.mu.Unlock() // 函数返回时解锁

	// 2. 如果已经 bootstrapped，返回错误
	if s.mu.bootstrapped {
		return nil, ErrClusterInitialized
	}

	// 3. 如果之前有拒绝的错误，返回该错误
	if s.mu.rejectErr != nil {
		return nil, s.mu.rejectErr
	}

	// 4. 调用 bootstrapCluster 执行真正的集群初始化逻辑
	//    - inspectedDiskState.uninitializedEngines：未初始化的存储引擎
	//    - s.config：节点配置
	state, err := bootstrapCluster(ctx, s.inspectedDiskState.uninitializedEngines, s.config)
	if err != nil {
		// 初始化失败，记录日志
		log.Dev.Errorf(ctx, "bootstrap: %v", err)
		// 标记内部错误以拒绝后续请求
		s.mu.rejectErr = errInternalBootstrapError
		return nil, s.mu.rejectErr
	}

	// 5. 设置初始化类型（单节点 / 多节点等）
	state.initType = r.InitType

	// 6. 成功 bootstrap，标记状态，防止未来再次 bootstrap
	s.mu.bootstrapped = true

	// 7. 将初始化状态发送到 bootstrapReqCh 通道供其他组件使用
	s.bootstrapReqCh <- state

	// 8. 返回成功响应
	return &serverpb.BootstrapResponse{}, nil
}

// startJoinLoop continuously tries connecting to nodes specified in the join
// list in order to determine what the cluster ID is, and to be allocated a
// node+store ID. It can return errJoinRPCUnsupported, in which case the caller
// is expected to fall back to the gossip-based cluster ID discovery mechanism.
// It can also fail with ErrIncompatibleBinaryVersion, in which case we know we're
// running a binary that's too old to join the rest of the cluster.
func (s *initServer) startJoinLoop(ctx context.Context, stopper *stop.Stopper) (*initState, error) {
	if len(s.config.bootstrapAddresses) == 0 {
		return nil, errors.AssertionFailedf("expected to find at least one bootstrap address, found none")
	}

	// Iterate through all the bootstrap addresses at least once to reduce time
	// taken to cluster convergence. Keep this code block roughly in sync with the
	// one below.
	for _, addr := range s.config.bootstrapAddresses {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case <-stopper.ShouldQuiesce():
			return nil, stop.ErrUnavailable
		default:
		}

		resp, err := s.attemptJoinTo(ctx, addr.String())
		if errors.Is(err, ErrIncompatibleBinaryVersion) {
			// Propagate upwards; this is an error condition the caller knows
			// to expect.
			return nil, err
		}
		if err != nil {
			// Try the next node if unsuccessful.

			if grpcutil.IsWaitingForInit(err) {
				log.Dev.Infof(ctx, "%s is itself waiting for init, will retry", addr)
			} else {
				log.Dev.Warningf(ctx, "outgoing join rpc to %s unsuccessful: %v", addr, err.Error())
			}
			continue
		}

		state, err := s.initializeFirstStoreAfterJoin(ctx, resp)
		if err != nil {
			return nil, err
		}

		// We mark ourselves as bootstrapped to prevent future bootstrap attempts.
		s.mu.Lock()
		s.mu.bootstrapped = true
		s.mu.Unlock()

		return state, nil
	}

	const joinRPCBackoff = time.Second
	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(joinRPCBackoff)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	for idx := 0; ; idx = (idx + 1) % len(s.config.bootstrapAddresses) {
		addr := s.config.bootstrapAddresses[idx].String()
		select {
		case <-tickChan:
			resp, err := s.attemptJoinTo(ctx, addr)
			if errors.Is(err, ErrIncompatibleBinaryVersion) {
				// Propagate upwards; this is an error condition the caller
				// knows to expect.
				return nil, err
			}
			if err != nil {
				// Blindly retry for all other errors, logging them for visibility.

				// TODO(irfansharif): If startup logging gets too spammy, we
				// could match against connection errors to generate nicer
				// logging. See grpcutil.connectionRefusedRe.

				if grpcutil.IsWaitingForInit(err) {
					log.Dev.Infof(ctx, "%s is itself waiting for init, will retry", addr)
				} else {
					log.Dev.Warningf(ctx, "outgoing join rpc to %s unsuccessful: %v", addr, err.Error())
				}
				continue
			}

			// We were able to successfully join an existing cluster. We'll now
			// initialize our first store, using the store ID handed to us.
			state, err := s.initializeFirstStoreAfterJoin(ctx, resp)
			if err != nil {
				return nil, err
			}

			// We mark ourselves as bootstrapped to prevent future bootstrap attempts.
			s.mu.Lock()
			s.mu.bootstrapped = true
			s.mu.Unlock()

			return state, nil
		case <-ctx.Done():
			return nil, context.Canceled
		case <-stopper.ShouldQuiesce():
			return nil, stop.ErrUnavailable
		}
	}
}

// attemptJoinTo attempts to join to the node running at the given address.
func (s *initServer) attemptJoinTo(
	ctx context.Context, addr string,
) (*kvpb.JoinNodeResponse, error) {
	conn, initClient, err := s.newNodeClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	latestVersion := s.config.latestVersion
	req := &kvpb.JoinNodeRequest{
		BinaryVersion: &latestVersion,
	}
	resp, err := initClient.Join(ctx, req)
	if err != nil {
		status, ok := grpcstatus.FromError(errors.UnwrapAll(err))
		if !ok {
			return nil, errors.Wrap(err, "failed to join cluster")
		}

		// TODO(irfansharif): Here we're logging the error and also returning
		// it. We should wrap the logged message with the right error instead.
		// The caller code, as written, switches on the error type; that'll need
		// to be changed as well.

		if status.Code() == codes.PermissionDenied {
			log.Dev.Infof(ctx, "%s is running a version higher than our binary version %s", addr, req.BinaryVersion.String())
			return nil, ErrIncompatibleBinaryVersion
		}

		return nil, err
	}

	return resp, nil
}

// DiskClusterVersion returns the cluster version synthesized from disk. This
// is always non-zero since it falls back to the MinSupportedVersion.
func (s *initServer) DiskClusterVersion() clusterversion.ClusterVersion {
	return s.inspectedDiskState.clusterVersion
}

// initializeFirstStoreAfterJoin initializes the first store after a successful
// join attempt. It re-constructs the store identifier from the join response
// and persists the appropriate cluster version to disk. After having done so,
// it returns an initState that captures the newly initialized store.
func (s *initServer) initializeFirstStoreAfterJoin(
	ctx context.Context, resp *kvpb.JoinNodeResponse,
) (*initState, error) {
	// We expect all the stores to be empty at this point, except for
	// the store cluster version key. Assert so.
	//
	// TODO(jackson): Eventually we should be able to avoid opening the
	// engines altogether until here, but that requires us to move the
	// store cluster version key outside of the storage engine.
	if err := assertEnginesEmpty(s.inspectedDiskState.uninitializedEngines); err != nil {
		return nil, err
	}

	firstEngine := s.inspectedDiskState.uninitializedEngines[0]
	clusterVersion := clusterversion.ClusterVersion{Version: *resp.ActiveVersion}
	if err := kvstorage.WriteClusterVersion(ctx, firstEngine, clusterVersion); err != nil {
		return nil, err
	}

	sIdent, err := resp.CreateStoreIdent()
	if err != nil {
		return nil, err
	}
	if err := kvstorage.InitEngine(ctx, firstEngine, sIdent); err != nil {
		return nil, err
	}

	return inspectEngines(
		ctx, s.inspectedDiskState.uninitializedEngines,
		s.config.latestVersion, s.config.minSupportedVersion,
	)
}

// assertEnginesEmpty 确认传入的每个存储引擎（Engine）都是“空的”，
// 除了可能存在的 store cluster version key。用于在集群初始化前
// 验证引擎状态，确保不会在已有数据的引擎上执行 bootstrap。
func assertEnginesEmpty(engines []storage.Engine) error {
	// 旧的存储集群版本 key，可能已经写入，但不算作非空
	storeClusterVersionKey := keys.DeprecatedStoreClusterVersionKey()

	// TODO(sumeer): 如果必要的话，应该传入上下文
	ctx := context.Background()

	// 遍历每个引擎
	for _, engine := range engines {
		err := func() error {
			// 创建一个迭代器，扫描整个引擎
			iter, err := engine.NewEngineIterator(ctx, storage.IterOptions{
				KeyTypes:   storage.IterKeyTypePointsAndRanges, // 遍历 point key 和 range key
				UpperBound: roachpb.KeyMax,                     // 遍历到最大 key
			})
			if err != nil {
				return err
			}
			defer iter.Close()

			// 从最小 key 开始查找
			valid, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: roachpb.KeyMin})
			for ; valid && err == nil; valid, err = iter.NextEngineKey() {
				// 获取当前 key
				k, err := iter.UnsafeEngineKey()
				if err != nil {
					return err
				}
				// 判断当前 key 是 point key 还是 range key
				hasPoint, hasRange := iter.HasPointAndRange()

				// 如果 key 是 store cluster version key，忽略它
				if hasPoint && !hasRange && storeClusterVersionKey.Equal(k.Key) {
					continue
				}

				// 如果遇到其他 key，则认为引擎不为空
				return errors.New("engine is not empty")
			}
			return err
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *initServer) newNodeClient(
	ctx context.Context, addr string,
) (io.Closer, kvpb.RPCNodeClient, error) {
	if !s.config.useDRPC {
		dialOpts, err := s.config.getGRPCDialOpts(ctx, addr, rpcbase.SystemClass)
		if err != nil {
			return nil, nil, err
		}
		conn, err := grpc.DialContext(ctx, addr, dialOpts...)
		if err != nil {
			return nil, nil, err
		}
		return conn, kvpb.NewGRPCInternalClientAdapter(conn), nil
	}

	dialOpts, err := s.config.getDRPCDialOpts(ctx, addr, rpcbase.SystemClass)
	if err != nil {
		return nil, nil, err
	}
	conn, err := drpcclient.DialContext(ctx, addr, dialOpts...)
	if err != nil {
		return nil, nil, err
	}
	return conn, kvpb.NewDRPCNodeClientAdapter(conn), nil
}

// initServerCfg is a thin wrapper around the server Config object, exposing
// only the fields needed by the init server.
type initServerCfg struct {
	advertiseAddr           string
	minSupportedVersion     roachpb.Version
	latestVersion           roachpb.Version // the version used during bootstrap
	defaultSystemZoneConfig zonepb.ZoneConfig
	defaultZoneConfig       zonepb.ZoneConfig

	// getGRPCDialOpts retrieves the gRPC dial options to use to issue Join RPCs.
	getGRPCDialOpts func(ctx context.Context, target string, class rpcbase.ConnectionClass) ([]grpc.DialOption, error)

	// getDRPCDialOpts retrieves the DRPC dial options to use to issue Join RPCs.
	getDRPCDialOpts func(ctx context.Context, target string, class rpcbase.ConnectionClass) ([]drpcclient.DialOption, error)

	// bootstrapAddresses is a list of node addresses (populated using --join
	// addresses) that is used to form a connected graph/network of CRDB servers.
	// Once a strongly connected graph is constructed, it suffices for any node in
	// the network to be initialized (which would then then propagates the cluster
	// ID to the rest of the nodes).
	//
	// NB: Not that this does not work for weakly connected graphs. Let's
	// consider a network where n3 points only to n2 (and not vice versa). If
	// n2 is `cockroach init`-ialized, n3 will learn about it. The reverse will
	// not be true.
	bootstrapAddresses []util.UnresolvedAddr

	// useDRPC determines whether to use DRPC for internode communication
	// instead of gRPC.
	//
	// NB: This configuration option is provided via a CLI flag and is not
	// controlled by the "rpc.experimental_drpc.enabled" cluster setting.
	useDRPC bool

	// testingKnobs is used for internal test controls only.
	testingKnobs base.TestingKnobs
}

func newInitServerConfig(
	ctx context.Context,
	cfg Config,
	getGRPCDialOpts func(context.Context, string, rpcbase.ConnectionClass) ([]grpc.DialOption, error),
	getDRPCDialOpts func(context.Context, string, rpcbase.ConnectionClass) ([]drpcclient.DialOption, error),
) initServerCfg {
	latestVersion := cfg.Settings.Version.LatestVersion()
	minSupportedVersion := cfg.Settings.Version.MinSupportedVersion()
	if knobs := cfg.TestingKnobs.Server; knobs != nil {
		if overrideVersion := knobs.(*TestingKnobs).ClusterVersionOverride; overrideVersion != (roachpb.Version{}) {
			// We are customizing the cluster version. We can only bootstrap a fresh
			// cluster at specific versions (specifically, the current version and
			// previously released versions down to the minimum supported). We choose
			// the closest version that's not newer than the target version.; later
			// on, we will upgrade to `ClusterVersionOverride` (this happens
			// separately when we Activate the server).
			var bootstrapVersion roachpb.Version
			for _, v := range bootstrap.VersionsWithInitialValues() {
				if !overrideVersion.Less(v.Version()) {
					bootstrapVersion = v.Version()
					break
				}
			}
			if bootstrapVersion == (roachpb.Version{}) {
				panic(fmt.Sprintf("ClusterVersionOverride version %s too low", overrideVersion))
			}
			latestVersion = bootstrapVersion
		}
	}
	if latestVersion.Less(minSupportedVersion) {
		log.Dev.Fatalf(ctx, "binary version (%s) less than min supported version (%s)",
			latestVersion, minSupportedVersion)
	}

	bootstrapAddresses := cfg.FilterGossipBootstrapAddresses(context.Background())
	return initServerCfg{
		advertiseAddr:           cfg.AdvertiseAddr,
		minSupportedVersion:     minSupportedVersion,
		latestVersion:           latestVersion,
		defaultSystemZoneConfig: cfg.DefaultSystemZoneConfig,
		defaultZoneConfig:       cfg.DefaultZoneConfig,
		getGRPCDialOpts:         getGRPCDialOpts,
		getDRPCDialOpts:         getDRPCDialOpts,
		useDRPC:                 cfg.UseDRPC,
		bootstrapAddresses:      bootstrapAddresses,
		testingKnobs:            cfg.TestingKnobs,
	}
}

// inspectEngines goes through engines and constructs an initState. The
// initState returned by this method will reflect a zero NodeID if none has
// been assigned yet (i.e. if none of the engines is initialized). See
// commentary on initState for the intended usage of inspectEngines.
// inspectEngines 是 CockroachDB 节点启动过程中最关键的初始化检查函数之一，它遍历节点的所有存储引擎（engines，通常是多个磁盘目录），检查它们的初始化状态，并汇总出一个 initState 结构体，用于后续决定节点该如何启动。
// 简单来说：它回答了三个问题：
//
// 这个节点属于哪个集群？（ClusterID）
// 这个节点的 NodeID 是多少？（如果还没分配，返回 0）
// 哪些引擎已经初始化（有 StoreIdent），哪些还是全新的？
// 当前集群版本是什么？（用于版本升级检查）
// 有没有初始的集群设置（cached settings）？
func inspectEngines(
	ctx context.Context, engines []storage.Engine, latestVersion, minSupportedVersion roachpb.Version,
) (*initState, error) {
	var clusterID uuid.UUID
	var nodeID roachpb.NodeID
	var initializedEngines, uninitializedEngines []storage.Engine
	var initialSettingsKVs []roachpb.KeyValue

	for _, eng := range engines {
		// 从任意一个引擎加载缓存的集群设置（只需加载一次）
		if len(initialSettingsKVs) == 0 {
			initialSettingsKVs, _ = loadCachedSettingsKVs(ctx, eng)
		}

		// 读取引擎的 StoreIdent（关键标识：ClusterID、NodeID、StoreID）
		storeIdent, err := kvstorage.ReadStoreIdent(ctx, eng)

		if errors.HasType(err, (*kvstorage.NotBootstrappedError)(nil)) {
			// 这个引擎是全新的（从未初始化过）
			uninitializedEngines = append(uninitializedEngines, eng)
			continue
		} else if err != nil {
			return nil, err // 其他错误，直接失败
		}

		if clusterID != uuid.Nil && clusterID != storeIdent.ClusterID {
			return nil, errors.Errorf("conflicting store ClusterIDs: %s, %s", storeIdent.ClusterID, clusterID)
		}
		clusterID = storeIdent.ClusterID

		if storeIdent.StoreID == 0 || storeIdent.NodeID == 0 || storeIdent.ClusterID == uuid.Nil {
			return nil, errors.Errorf("partially initialized store: %+v", storeIdent)
		}

		if nodeID != 0 && nodeID != storeIdent.NodeID {
			return nil, errors.Errorf("conflicting store NodeIDs: %s, %s", storeIdent.NodeID, nodeID)
		}
		nodeID = storeIdent.NodeID

		if err := eng.SetStoreID(ctx, int32(storeIdent.StoreID)); err != nil {
			return nil, err
		}
		initializedEngines = append(initializedEngines, eng)
	}
	clusterVersion, err := kvstorage.SynthesizeClusterVersionFromEngines(
		ctx, initializedEngines, latestVersion, minSupportedVersion,
	)
	if err != nil {
		return nil, err
	}

	state := &initState{
		clusterID:            clusterID,
		nodeID:               nodeID,
		initializedEngines:   initializedEngines,
		uninitializedEngines: uninitializedEngines,
		clusterVersion:       clusterVersion,
		initialSettingsKVs:   initialSettingsKVs,
	}
	return state, nil
}
