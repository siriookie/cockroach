// Copyright 2022 The Cockroach Authors.
// CockroachDB 的软件许可说明见 /LICENSE 文件

package server

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

// grpcGatewayServer 表示一个 gRPC 服务，同时通过 grpc-gateway 提供 HTTP/JSON 端点
type grpcGatewayServer interface {
	RegisterService(g *grpc.Server) // 注册 gRPC 服务
	RegisterGateway(
		ctx context.Context,
		mux *gwruntime.ServeMux, // grpc-gateway 的 ServeMux
		conn *grpc.ClientConn, // gRPC 客户端连接
	) error
}

// 确保以下类型实现了 grpcGatewayServer 接口
var _ grpcGatewayServer = (*adminServer)(nil)
var _ grpcGatewayServer = (*statusServer)(nil)
var _ grpcGatewayServer = authserver.Server(nil)
var _ grpcGatewayServer = (*ts.Server)(nil)

// configureGRPCGateway 初始化 grpc-gateway 所需的服务
// grpcAddr 是服务器的 gRPC 地址
// 返回值：
// - ServeMux：HTTP/JSON 转发到 gRPC 的路由器
// - 上下文：供注册 Gateway 时使用
// - gRPC 客户端连接
// - 错误信息
func configureGRPCGateway(
	ctx, workersCtx context.Context,
	ambientCtx log.AmbientContext, // 用于日志和上下文注释
	rpcContext *rpc.Context, // RPC 上下文
	stopper *stop.Stopper, // 用于管理 goroutine 生命周期
	grpcAddr string, // gRPC 服务器地址
) (*gwruntime.ServeMux, context.Context, *grpc.ClientConn, error) {

	// JSON 序列化配置：枚举以 int 表示，输出默认值，并使用缩进
	jsonpb := &protoutil.JSONPb{
		EnumsAsInts:  true,
		EmitDefaults: true,
		Indent:       "  ",
	}

	// Proto 序列化配置
	protopb := new(protoutil.ProtoPb)

	// 创建 grpc-gateway 的 ServeMux，并配置各种 Content-Type 的序列化器
	gwMux := gwruntime.NewServeMux(
		gwruntime.WithMarshalerOption(gwruntime.MIMEWildcard, jsonpb),
		gwruntime.WithMarshalerOption(httputil.JSONContentType, jsonpb),
		gwruntime.WithMarshalerOption(httputil.AltJSONContentType, jsonpb),
		gwruntime.WithMarshalerOption(httputil.ProtoContentType, protopb),
		gwruntime.WithMarshalerOption(httputil.AltProtoContentType, protopb),
		gwruntime.WithOutgoingHeaderMatcher(authserver.AuthenticationHeaderMatcher),
		gwruntime.WithMetadata(authserver.TranslateHTTPAuthInfoToGRPCMetadata),
		gwruntime.WithMetadata(rpc.MarkGatewayRequest),
	)

	// 创建一个可取消的上下文，Stopper 在关闭时会调用 cancel
	gwCtx, gwCancel := context.WithCancel(ambientCtx.AnnotateCtx(context.Background()))
	stopper.AddCloser(stop.CloserFn(gwCancel))

	// 获取 gRPC 连接配置（不使用 rpcContext.GRPCDial 避免不必要的中间层）
	dialOpts, err := rpcContext.GRPCDialOptions(ctx, grpcAddr, rpcbase.DefaultClass)
	if err != nil {
		return nil, nil, nil, err
	}

	// 拦截器：统计每个 gRPC 方法的调用次数
	callCountInterceptor := func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		telemetry.Inc(getServerEndpointCounter(method)) // 统计方法调用次数
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	// 建立 gRPC 客户端连接
	conn, err := grpc.DialContext(ctx, grpcAddr, append(
		dialOpts,
		grpc.WithUnaryInterceptor(callCountInterceptor),
	)...)
	if err != nil {
		return nil, nil, nil, err
	}

	// 启动一个 goroutine，在 Stopper 发出 quiesce 信号时关闭 gRPC 连接
	{
		waitQuiesce := func(workersCtx context.Context) {
			<-stopper.ShouldQuiesce() // 等待停止信号
			// 注意：不能作为 Closer，因为 ServeWith 可能阻塞在 accept() 上
			err := conn.Close() // 关闭 gRPC 连接
			if err != nil {
				log.Ops.Fatalf(workersCtx, "%v", err)
			}
		}
		if err := stopper.RunAsyncTask(workersCtx, "wait-quiesce", waitQuiesce); err != nil {
			waitQuiesce(workersCtx)
		}
	}

	// 返回 grpc-gateway ServeMux，上下文，gRPC 连接
	return gwMux, gwCtx, conn, nil
}

// getServerEndpointCounter 返回对应 gRPC 方法的 telemetry Counter
func getServerEndpointCounter(method string) telemetry.Counter {
	const counterPrefix = "http.grpc-gateway"
	return telemetry.GetCounter(fmt.Sprintf("%s.%s", counterPrefix, method))
}
