// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

type RPCListenerFactory func(
	ctx context.Context,
	addr, advertiseAddr *string,
	connName string,
	acceptProxyProtocolHeaders bool,
) (net.Listener, error)

// startListenRPCAndSQL starts the RPC and SQL listeners. It returns:
//   - The listener for pgwire connections coming over the network. This will be used
//     to start the SQL server when initialization has completed.
//   - The listener for internal sql connections running over our pipes interface.
//   - A dialer function that can be used to open a connection to the RPC loopback interface.
//   - A function that starts the RPC server, when the cluster is known to have
//     bootstrapped or when waiting for init().
//
// This does not start *accepting* connections just yet.
// startListenRPCAndSQL 用于：
// 启动数据库节点对外提供服务的监听端口：
// SQL（pgwire）协议
// gRPC
// 自定义 DRPC（分布式 RPC）
// 支持 loopback 连接（节点自己访问自己）。
// 支持在 节点未完全初始化前先不接收连接。
// 处理 shutdown/stop 信号（stopper）的清理工作。
// 返回：
// SQL 网络监听器
// 内部 loopback 监听器
// loopback 拨号函数
// 一个启动 RPC 服务的函数（在集群 ready 后调用）
func startListenRPCAndSQL(
	ctx, workersCtx context.Context,
	cfg BaseConfig,
	stopper *stop.Stopper,
	grpc *grpcServer,
	drpc *drpcServer,
	rpcListenerFactory RPCListenerFactory,
	enableSQLListener bool,
	acceptProxyProtocolHeaders bool,
) (
	sqlListener net.Listener,
	pgLoopbackListener *netutil.LoopbackListener,
	grpcLoopbackDial func(context.Context) (net.Conn, error),
	drpcLoopbackDial func(context.Context) (net.Conn, error),
	startRPCServer func(ctx context.Context),
	err error,
) {
	rpcChanName := "rpc/sql"
	//如果 SQL 和 RPC 使用同一个端口（未拆分）且 SQL 启用，就用 "rpc/sql"。
	//否则只处理 "rpc"。
	if cfg.SplitListenSQL || !enableSQLListener {
		rpcChanName = "rpc"
	}
	var ln net.Listener
	if k := cfg.TestingKnobs.Server; k != nil {
		knobs := k.(*TestingKnobs)
		ln = knobs.RPCListener
	}
	if ln == nil {
		var err error
		ln, err = rpcListenerFactory(ctx, &cfg.Addr, &cfg.AdvertiseAddr, rpcChanName, acceptProxyProtocolHeaders)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		log.Eventf(ctx, "listening on port %s", cfg.Addr)
	}

	var pgL net.Listener
	if cfg.SplitListenSQL && enableSQLListener {
		if cfg.SQLAddrListener == nil {
			//如果 SQL 单独拆端口，就创建新的 listener。
			pgL, err = ListenAndUpdateAddrs(ctx, &cfg.SQLAddr, &cfg.SQLAdvertiseAddr, "sql", acceptProxyProtocolHeaders)
		} else {
			pgL = cfg.SQLAddrListener
		}
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		// The SQL listener shutdown worker, which closes everything under
		// the SQL port when the stopper indicates we are shutting down.
		//并注册一个异步任务，当 stopper.ShouldQuiesce() 发出信号时关闭 SQL listener。
		waitQuiesce := func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			// NB: we can't do this as a Closer because (*Server).ServeWith is
			// running in a worker and usually sits on accept() which unblocks
			// only when the listener closes. In other words, the listener needs
			// to close when quiescing starts to allow that worker to shut down.
			if err := pgL.Close(); err != nil {
				log.Ops.Fatalf(ctx, "%v", err)
			}
		}
		if err := stopper.RunAsyncTask(workersCtx, "wait-quiesce", waitQuiesce); err != nil {
			waitQuiesce(workersCtx)
			return nil, nil, nil, nil, nil, err
		}
		log.Eventf(ctx, "listening on sql port %s", cfg.SQLAddr)
	}

	// serveOnMux is used to ensure that the mux gets listened on eventually,
	// either via the returned startRPCServer() or upon stopping.
	//用来保证 cmux 的 Serve() 只会被调用一次。
	//因为 cmux 的 Serve() 会阻塞在 Accept()，不能多次调用，否则会报错。
	//后续在 startRPCServer() 或 shutdown 时可能都会调用 Serve()，所以用 sync.Once 确保只执行一次。
	var serveOnMux sync.Once

	m := cmux.New(ln)
	// cmux auto-retries Accept() by default. Tell it
	// to stop doing work if we see a request to shut down.
	//cmux 默认会在 Accept() 失败时 自动重试。
	//HandleError 可以拦截这些错误，告诉 cmux 是否继续重试。
	//逻辑：
	//如果 stopper.ShouldQuiesce() 发出信号（节点正在关闭），返回 false：
	//告诉 cmux 停止 Accept()，不再重试。
	//否则返回 true：
	//cmux 继续重试 Accept()，保持监听。
	m.HandleError(func(err error) bool {
		select {
		case <-stopper.ShouldQuiesce():
			log.Dev.Infof(workersCtx, "server shutting down: instructing cmux to stop accepting")
			return false
		default:
			return true
		}
	})

	if !cfg.SplitListenSQL && enableSQLListener {
		//条件判断
		//!cfg.SplitListenSQL：SQL 没有单独拆端口。
		//enableSQLListener：SQL 功能启用。
		//即 SQL 复用 RPC listener。
		// If the pg port is split, it will be opened above. Otherwise,
		// we make it hang off the RPC listener via cmux here.
		//使用 cmux 匹配 SQL 流量
		//pgwire.Match(r) 会检测 TCP 流量是否符合 PostgreSQL 协议。
		//返回一个 listener pgL，专门接收 SQL 流量。
		pgL = m.Match(func(r io.Reader) bool {
			return pgwire.Match(r)
		})
		//由于 SQL 和 RPC 共用端口，所以 SQL 地址等于 RPC 地址。
		//UpdateAddrs 用于更新 advertised address，保证节点向外汇报的地址正确（尤其是端口自动分配时）。
		// Also if the pg port is not split, the actual listen address for
		// SQL become equal to that of RPC.
		cfg.SQLAddr = cfg.Addr
		// Then we update the advertised addr with the right port, if
		// the port had been auto-allocated.
		if err := UpdateAddrs(ctx, &cfg.SQLAddr, &cfg.SQLAdvertiseAddr, ln.Addr()); err != nil {
			return nil, nil, nil, nil, nil, errors.Wrapf(err, "internal error")
		}
	}
	// dropDRPCHeaderListener 的作用：
	// 在把连接交给 drpc server 之前，先把前面的协议头丢掉。
	//
	// 原因是：
	// - drpc 本身期望拿到的是“纯净的 drpc 数据流”
	// - 但我们这里用了 cmux 来做多协议复用
	// - cmux 在 Match 时会把用于匹配的协议前缀“留在连接里”
	//
	// 如果使用的是 drpcmigrate.ListenMux，这一步是不需要的，
	// 因为 drpcmigrate 会在内部正确处理协议头。
	// 但 cmux 不会自动移除前缀，所以必须手动丢弃。
	var drpcL net.Listener = &dropDRPCHeaderListener{
		// m.Match(drpcMatcher) 表示：
		// 从 cmux 中分流出“符合 drpc 协议特征”的连接
		wrapped: m.Match(drpcMatcher),
	}

	// grpcL：
	// 从 cmux 中匹配“剩余的所有流量”
	// 换句话说：
	// - 不是 pgwire
	// - 不是 drpc
	// 那就统统认为是 gRPC
	grpcL := m.Match(cmux.Any())

	// 测试用的 server knobs（测试开关）
	// 只有在测试环境下才可能存在
	if serverTestKnobs, ok := cfg.TestingKnobs.Server.(*TestingKnobs); ok {
		// 如果注入了“人为延迟控制器”（InjectedLatencyOracle）
		// 就对 listener 进行一层包装
		if serverTestKnobs.ContextTestingKnobs.InjectedLatencyOracle != nil {
			// 对 gRPC listener 注入延迟
			// 用于测试慢网络、超时、竞态等问题
			grpcL = rpc.NewDelayingListener(
				grpcL,
				serverTestKnobs.ContextTestingKnobs.InjectedLatencyEnabled,
			)

			// 对 DRPC listener 同样注入延迟
			drpcL = rpc.NewDelayingListener(
				drpcL,
				serverTestKnobs.ContextTestingKnobs.InjectedLatencyEnabled,
			)
		}
	}

	// grpcLoopbackL：
	// 创建一个“仅用于本进程内部”的 gRPC listener
	// - 不走真实 TCP
	// - 通过内存管道通信
	// - 用于 node 内部组件互相调用
	grpcLoopbackL := netutil.NewLoopbackListener(ctx, stopper)

	// sqlLoopbackL：
	// 同样是 loopback listener
	// - 专供 SQL 层内部使用
	// - 避免经过网络栈，提高性能
	sqlLoopbackL := netutil.NewLoopbackListener(ctx, stopper)

	// drpcCtx / drpcCancel：
	// 给 DRPC 服务单独创建一个 context
	// 这样在节点进入 quiesce（静默关闭）阶段时
	// 可以只取消 DRPC 相关任务
	drpcCtx, drpcCancel := context.WithCancel(workersCtx)

	// 创建一个专用的 DRPC loopback listener
	//
	// 注意：
	// - 这个 listener 只服务 DRPC
	// - 不需要做协议头识别（不经过 cmux）
	// - 因为 loopback 本身就已经“确定协议类型”
	drpcLoopbackL := netutil.NewLoopbackListener(ctx, stopper)
	// The remainder shutdown worker.
	// 剩余资源的“关机协程”：
	// 负责在节点进入 quiesce（静默关闭）阶段时，
	// 统一关闭所有 listener 和相关上下文。
	waitForQuiesce := func(context.Context) {
		// 阻塞等待 stopper 发出“开始静默关闭”的信号
		<-stopper.ShouldQuiesce()

		// 取消 DRPC 专用的 context，
		// 通知所有 DRPC 相关 goroutine 尽快退出
		drpcCancel()

		// 关闭对外的 gRPC listener
		netutil.FatalIfUnexpected(grpcL.Close())

		// 关闭进程内的 gRPC loopback listener
		netutil.FatalIfUnexpected(grpcLoopbackL.Close())

		// 关闭对外的 DRPC listener
		// 注：关闭这个 listener 等价于关闭 drpcTLSL
		netutil.FatalIfUnexpected(drpcL.Close())

		// 关闭 DRPC 的 loopback listener
		// 注：等价于关闭 drpcLoopbackTLSL
		netutil.FatalIfUnexpected(drpcLoopbackL.Close())

		// 关闭 SQL 层使用的 loopback listener
		netutil.FatalIfUnexpected(sqlLoopbackL.Close())

		// 最后关闭最底层的网络 listener（RPC / cmux 的底座）
		netutil.FatalIfUnexpected(ln.Close())
	}

	// stopGRPC 定义一个“关闭 RPC 服务”的函数
	stopGRPC := func() {
		// 停止 gRPC server：
		// - 不再接收新请求
		// - 等待正在处理的 RPC 尽量完成
		grpc.Stop()

		// 确保 cmux 的 Serve() 至少被调用一次
		serveOnMux.Do(func() {
			// 重要说明：
			// cmux 的 Match listener 只有在 Serve() 被调用后，
			// 才能正确地响应 Close() 并退出 Accept()
			//
			// 如果服务器还没正式启动就进入 shutdown，
			// 这里必须补一次 Serve()，否则会卡死
			netutil.FatalIfUnexpected(m.Serve())
		})
	}

	// 将 stopGRPC 注册为 stopper 的 Closer：
	// 在 stopper 关闭时自动调用
	stopper.AddCloser(stop.CloserFn(stopGRPC))

	// 启动一个异步任务，专门负责监听 quiesce 信号并执行资源清理
	if err := stopper.RunAsyncTask(
		workersCtx,          // worker 使用的上下文
		"grpc-drpc-quiesce", // 任务名称（用于 debug / tracing）
		waitForQuiesce,      // 实际执行的清理逻辑
	); err != nil {
		// 如果任务启动失败，直接同步执行清理逻辑
		waitForQuiesce(ctx)

		// 同时强制停止 gRPC
		stopGRPC()

		// 取消 DRPC context，防止 goroutine 泄漏
		drpcCancel()

		// 向上返回错误
		return nil, nil, nil, nil, nil, err
	}

	// 再次注册 stopGRPC，确保在不同关闭路径下都能执行
	stopper.AddCloser(stop.CloserFn(stopGRPC))

	// startRPCServer 用于真正“开始对外提供 RPC 服务”
	//
	// 注意：
	// - 这里不立即启动
	// - 而是把启动逻辑封装成一个函数返回给调用方
	// - 只有当集群完成 bootstrap / init 后才会调用
	startRPCServer = func(ctx context.Context) {

		// 启动对外的 gRPC 服务
		_ = stopper.RunAsyncTask(workersCtx, "serve-grpc", func(context.Context) {
			// grpc.Serve 会阻塞在 Accept()，直到 listener 关闭
			netutil.FatalIfUnexpected(grpc.Serve(grpcL))
		})

		// 启动对外的 DRPC 服务
		_ = stopper.RunAsyncTask(drpcCtx, "serve-drpc", func(ctx context.Context) {
			// 如果配置了 TLS，则包一层 TLS listener
			if cfg := drpc.tlsCfg; cfg != nil {
				drpcTLSL := tls.NewListener(drpcL, cfg)
				netutil.FatalIfUnexpected(drpc.Serve(ctx, drpcTLSL))
			} else {
				// 否则直接使用明文 DRPC
				netutil.FatalIfUnexpected(drpc.Serve(ctx, drpcL))
			}
		})

		// 启动进程内的 gRPC loopback 服务
		_ = stopper.RunAsyncTask(workersCtx, "serve-loopback-grpc", func(context.Context) {
			netutil.FatalIfUnexpected(grpc.Serve(grpcLoopbackL))
		})

		// 启动进程内的 DRPC loopback 服务
		_ = stopper.RunAsyncTask(workersCtx, "serve-loopback-drpc", func(context.Context) {
			if cfg := drpc.tlsCfg; cfg != nil {
				drpcdrpcLoopbackTLSL := tls.NewListener(drpcLoopbackL, cfg)
				netutil.FatalIfUnexpected(drpc.Serve(ctx, drpcdrpcLoopbackTLSL))
			} else {
				netutil.FatalIfUnexpected(drpc.Serve(ctx, drpcLoopbackL))
			}
		})

		// 启动 cmux 的 Serve()：
		// - 开始从 ln.Accept() 接收连接
		// - 并根据协议将连接分发给 pgwire / gRPC / DRPC
		_ = stopper.RunAsyncTask(ctx, "serve-mux", func(context.Context) {
			serveOnMux.Do(func() {
				netutil.FatalIfUnexpected(m.Serve())
			})
		})
	}

	return pgL, sqlLoopbackL, grpcLoopbackL.Connect, drpcLoopbackL.Connect, startRPCServer, nil
}
