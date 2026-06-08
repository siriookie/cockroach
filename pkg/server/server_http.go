// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/inspectz"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/privchecker"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
)

type httpServer struct {
	cfg BaseConfig
	mux http.ServeMux
	// gzMux is an HTTP handler that gzip-compresses mux.
	gzMux http.Handler
	proxy *nodeProxy
}

func newHTTPServer(
	cfg BaseConfig,
	rpcContext *rpc.Context,
	parseNodeID ParseNodeIDFn,
	getNodeIDHTTPAddress GetNodeIDHTTPAddressFn,
) *httpServer {
	server := &httpServer{
		cfg: cfg,
		proxy: &nodeProxy{
			scheme:               cfg.HTTPRequestScheme(),
			parseNodeID:          parseNodeID,
			getNodeIDHTTPAddress: getNodeIDHTTPAddress,
			rpcContext:           rpcContext,
		},
	}
	server.gzMux = gziphandler.GzipHandler(http.HandlerFunc(server.mux.ServeHTTP))
	return server
}

// HSTSEnabled is a boolean that enables HSTS headers on the HTTP
// server. These instruct a valid user agent to use HTTPS *only*
// for all future connections to this host.
var HSTSEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.hsts.enabled",
	"if true, HSTS headers will be sent along with all HTTP "+
		"requests. The headers will contain a max-age setting of one "+
		"year. Browsers honoring the header will always use HTTPS to "+
		"access the DB Console. Ensure that TLS is correctly configured "+
		"prior to enabling.",
	false,
	settings.WithPublic)

const hstsHeaderKey = "Strict-Transport-Security"

// hstsHeaderValue contains the static HSTS header value we return when
// HSTS is enabled via the clusters setting above. It sets the expiry
// of 1 year that the browser should remember to only use HTTPS for
// this site.
const hstsHeaderValue = "max-age=31536000"

const healthPath = "/health"

func (s *httpServer) handleHealth(healthHandler http.Handler) {
	s.mux.Handle(healthPath, healthHandler)
}

const virtualClustersPath = "/virtual_clusters"

// virtualClustersResp is the response returned by the virtual clusters
// endpoint that contains a list of active tenant sessions in the
// current session cookie. This allows the DB Console to populate a
// dropdown allowing the user to select a cluster.
type virtualClustersResp struct {
	// VirtualClusters is a list of virtual cluster names.
	VirtualClusters []string `json:"virtual_clusters"`
}

// virtualClustersHandler parses the session cookie from the request
// and returns a JSON body with the list of cluster names contained
// within the session. If the session does not contain any cluster
// names, it returns an empty list. If the cookie is missing, it
// returns 200 OK with an empty request body.
var virtualClustersHandler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
	sessionCookie, err := req.Cookie(authserver.SessionCookieName)
	if err != nil {
		if errors.Is(err, http.ErrNoCookie) {
			w.Header().Add("Content-Type", "application/json")
			if _, err := w.Write([]byte(`{"virtual_clusters":[]}`)); err != nil {
				log.Dev.Errorf(req.Context(), "unable to write virtual clusters response: %s", err.Error())
			}
			return
		}
		http.Error(w, "unable to decode session cookie", http.StatusInternalServerError)
		return
	}
	sessionsAndClusters := strings.Split(sessionCookie.Value, ",")
	resp := &virtualClustersResp{
		VirtualClusters: make([]string, 0),
	}
	for i, c := range sessionsAndClusters {
		if i%2 == 1 {
			resp.VirtualClusters = append(resp.VirtualClusters, c)
		}
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "unable to marshal virtual clusterse JSON", http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	if _, err := w.Write(respBytes); err != nil {
		log.Dev.Errorf(req.Context(), "unable to write virtual clusters response: %s", err.Error())
	}
})

// setupRoutes configures HTTP routes for the server.
func (s *httpServer) setupRoutes(
	// ctx 是启动阶段传进来的上下文，用来记录事件日志和初始化依赖。
	ctx context.Context,
	// execCfg 是 SQL executor 的配置，OIDC 等模块会用它执行 SQL 或访问 settings。
	execCfg *sql.ExecutorConfig,
	// authnServer 负责 HTTP 登录、登出、session 校验等认证逻辑。
	authnServer authserver.Server,
	// adminServer 提供 Admin API 里一些特殊 handler，例如 statement/txn bundle 下载。
	adminServer *adminServer,
	// adminAuthzCheck 用来检查 debug/inspectz 请求是否有 admin 或 VIEWDEBUG 权限。
	adminAuthzCheck privchecker.CheckerForRPCHandlers,
	// metricSource 是 metrics 数据来源，/vars、/metrics、/_status/load 会用到。
	metricSource metricMarshaler,
	// runtimeStatSampler 提供 runtime 采样数据，load endpoint 会用到。
	runtimeStatSampler *status.RuntimeStatSampler,
	// unauthenticatedAPIInternalServer 是不包认证的内部 API handler。
	unauthenticatedAPIInternalServer http.Handler,
	// handleDebugUnauthenticated 是原始 debug handler，后面会按配置决定是否包认证授权。
	handleDebugUnauthenticated http.Handler,
	// handleInspectzUnauthenticated 是原始 inspectz handler，后面会按配置决定是否包认证授权。
	handleInspectzUnauthenticated http.Handler,
	// apiServer 是 v2 HTTP API handler；为空时不注册 v2 API 路由。
	apiServer http.Handler,
	// flags 是传给 DB Console 前端的 feature flags。
	flags serverpb.FeatureFlags,
	// drpcEnabled 决定 login/logout 使用直接 HTTP handler 还是旧 grpc-gateway 路径。
	drpcEnabled bool,
) error {
	// OIDC Configuration must happen prior to the UI Handler being defined below so that we have
	// the system settings initialized for it to pick up from the oidcAuthenticationServer.
	// 初始化 OIDC 模块，并让它先注册自己的 /oidc/v1/* 路由。
	oidc, err := authserver.ConfigureOIDC(
		// 传入 settings、locality、HTTP route 注册函数、SSO 登录回调、日志上下文、cluster ID 和 SQL executor 配置。
		ctx, s.cfg.Settings, s.cfg.Locality,
		s.mux.Handle, authnServer.UserLoginFromSSO, s.cfg.AmbientCtx, s.cfg.ClusterIDContainer.Get(), execCfg,
	)
	// OIDC 初始化失败时，HTTP 路由整体启动失败。
	if err != nil {
		return err
	}

	// Define the http.Handler for UI assets.
	// 创建 DB Console 静态资源和 index.html 的 handler。
	assetHandler := ui.Handler(ui.Config{
		// 告诉 UI 当前是否绕过 web 认证。
		Insecure: s.cfg.InsecureWebAccess(),
		// 给 UI 注入当前 node id。
		NodeID: s.cfg.IDContainer,
		// 给 UI 注入 OIDC 配置读取接口，例如是否展示 OIDC 登录按钮。
		OIDC: oidc,
		// GetUser 从请求上下文里取当前登录用户，供 UI 模板显示用户名。
		GetUser: func(ctx context.Context) *string {
			// auth mux 如果验证出用户，会把用户信息放进 context。
			if user, ok := authserver.MaybeUserFromHTTPAuthInfoContext(ctx); ok {
				// UI 只需要 normalized string，不需要 SQLUsername 对象。
				ustring := user.Normalized()
				return &ustring
			}
			// 没有登录用户时返回 nil，UI 看到的就是匿名状态。
			return nil
		},
		// 注入前端 feature flags。
		Flags: flags,
		// 注入 settings，UI handler 会读取 license 等运行时信息。
		Settings: s.cfg.Settings,
	})

	// The authentication mux used here is created in "allow anonymous" mode so that the UI
	// assets are served up whether or not there is a session. If there is a session, the mux
	// adds it to the context, and it is templated into index.html so that the UI can show
	// the username of the currently-logged-in user.
	// 给 UI handler 包一层认证 mux，但允许匿名访问。
	authenticatedUIHandler := authserver.NewMux(
		authnServer, assetHandler, true /* allowAnonymous */)
	// 根路径挂 DB Console。静态资源和 index.html 都会从这里进入。
	s.mux.Handle("/", authenticatedUIHandler)

	// 默认先使用不带认证包装的内部 API handler。
	authenticatedAPIInternalServer := unauthenticatedAPIInternalServer
	// 如果没有开启 insecure web access，就给内部 API 包一层 session 认证。
	if !s.cfg.InsecureWebAccess() {
		authenticatedAPIInternalServer = authserver.NewMux(
			authnServer, authenticatedAPIInternalServer, false /* allowAnonymous */)
	}
	// 注册 /_status 前缀，DB Console 的很多状态 API 会走这里。
	s.mux.Handle(apiconstants.StatusPrefix, authenticatedAPIInternalServer)

	// Add HTTP authentication to the gRPC-gateway endpoints used by the UI,
	// if not disabled by configuration.
	// statement bundle 下载 handler，默认直接指向 adminServer 的方法。
	var stmtBundleHandlerFunc = http.HandlerFunc(adminServer.StmtBundleHandler)
	// transaction bundle 下载 handler，默认直接指向 adminServer 的方法。
	var txnBundleHandlerFunc = http.HandlerFunc(adminServer.TxnBundleHandler)
	// 安全模式下，bundle 下载也需要登录认证。
	if !s.cfg.InsecureWebAccess() {
		stmtBundleHandlerFunc = authserver.NewMux(authnServer, stmtBundleHandlerFunc, false).ServeHTTP
		txnBundleHandlerFunc = authserver.NewMux(authnServer, txnBundleHandlerFunc, false).ServeHTTP
	}

	// Login and logout paths.
	// Registration of login and logout is present here since they don't actually
	// rely on RPC but deal with HTTP cookies, which require different handling
	// mechanisms. When DRPC is enabled, we use direct HTTP handlers that can set
	// cookies via http.ResponseWriter. When DRPC is disabled, we use the old
	// grpc-gateway path which sets cookies via gRPC metadata headers for backward
	// compatibility.
	// DRPC 开启时，登录登出直接用 HTTP handler，因为它们需要直接读写 cookie。
	if drpcEnabled {
		// 注册 /login，登录成功时 LoginHandler 会直接设置 session cookie。
		s.mux.Handle(authserver.LoginPath, http.HandlerFunc(authnServer.LoginHandler))
		// logout 默认使用直接 HTTP handler。
		logoutHandlerfunc := http.HandlerFunc(authnServer.LogoutHandler)
		// 安全模式下，logout 也需要先验证当前 session。
		if !s.cfg.InsecureWebAccess() {
			logoutHandlerfunc = authserver.NewMux(authnServer, logoutHandlerfunc, false /* allowAnonymous */).ServeHTTP
		}
		// 注册 /logout。
		s.mux.Handle(authserver.LogoutPath, logoutHandlerfunc)
	} else {
		// DRPC 未开启时，/login 走旧的 grpc-gateway handler。
		s.mux.Handle(authserver.LoginPath, unauthenticatedAPIInternalServer)
		// DRPC 未开启时，/logout 走经过认证包装的内部 API handler。
		s.mux.Handle(authserver.LogoutPath, authenticatedAPIInternalServer)
	}

	// 注册 virtual clusters 信息接口。
	s.mux.Handle(virtualClustersPath, virtualClustersHandler)
	// The login path for 'cockroach demo', if we're currently running
	// that.
	// cockroach demo 场景下，额外开放 demo 自动登录入口。
	if s.cfg.EnableDemoLoginEndpoint {
		s.mux.Handle(authserver.DemoLoginPath, http.HandlerFunc(authnServer.DemoLogin))
	}

	// 注册 /_admin 前缀，Admin API 走这里。
	s.mux.Handle(apiconstants.AdminPrefix, authenticatedAPIInternalServer)

	// Handlers for statement diagnostic bundle download. These are special
	// because they return zip files, not protobufs or JSON.
	// 注册 statement diagnostic bundle 下载接口，返回 zip 文件。
	s.mux.Handle(apiconstants.AdminStmtBundle, stmtBundleHandlerFunc)
	// 注册 transaction diagnostic bundle 下载接口，返回 zip 文件。
	s.mux.Handle(apiconstants.AdminTxnBundle, txnBundleHandlerFunc)

	// The timeseries endpoint, used to produce graphs.
	// 注册 time series 查询接口，DB Console 图表会用它。
	s.mux.Handle(ts.URLPrefix, authenticatedAPIInternalServer)

	// Exempt the 2nd health check endpoint from authentication.
	// (This simply mirrors /health and exists for backward compatibility.)
	// 注册不需要认证的 health endpoint，主要给探活和兼容旧路径用。
	s.mux.Handle(apiconstants.AdminHealth, unauthenticatedAPIInternalServer)
	// The /_status/vars and /metrics endpoint is not authenticated either. Useful for monitoring.
	// 注册 /_status/vars，暴露变量和 metrics 文本，供监控系统抓取。
	s.mux.Handle(apiconstants.StatusVars, http.HandlerFunc(varsHandler{metricSource, s.cfg.Settings, false /* useStaticLabels */}.handleVars))
	// 注册 /metrics，和 /_status/vars 类似，但使用 static labels。
	s.mux.Handle(apiconstants.MetricsPath, http.HandlerFunc(varsHandler{metricSource, s.cfg.Settings, true /* useStaticLabels */}.handleVars))
	// Same for /_status/load.
	// 构造 /_status/load handler，用来暴露 runtime/load 相关状态。
	le, err := newLoadEndpoint(runtimeStatSampler, metricSource)
	// load endpoint 构造失败时，路由初始化失败。
	if err != nil {
		return err
	}
	// 注册 /_status/load，同样不包认证，便于监控读取。
	s.mux.Handle(apiconstants.LoadStatusVars, le)

	// apiServer 非空说明启用了新的 v2 HTTP API tree。
	if apiServer != nil {
		// The new "v2" HTTP API tree.
		// 注册 /api/v2 前缀。
		s.mux.Handle(apiconstants.APIV2Path, apiServer)
	}

	// Register debugging endpoints.
	// debug/inspectz 先使用未认证版本，后面根据安全配置决定是否包装。
	handleDebugAuthenticated := handleDebugUnauthenticated
	handleInspectzAuthenticated := handleInspectzUnauthenticated
	// 安全模式下，debug/inspectz 不仅要认证，还要做 admin/VIEWDEBUG 授权检查。
	if !s.cfg.InsecureWebAccess() {
		// Mandate both authentication and admin authorization.
		// 先给 debug handler 包 admin/VIEWDEBUG 授权检查。
		handleDebugAuthenticated = makeAdminAuthzCheckHandler(adminAuthzCheck, handleDebugAuthenticated)
		// 再给 debug handler 包登录认证。
		handleDebugAuthenticated = authserver.NewMux(authnServer, handleDebugAuthenticated, false /* allowAnonymous */)

		// inspectz 和 debug 一样，也需要 admin/VIEWDEBUG 授权。
		handleInspectzAuthenticated = makeAdminAuthzCheckHandler(adminAuthzCheck, handleInspectzAuthenticated)
		// 再给 inspectz handler 包登录认证。
		handleInspectzAuthenticated = authserver.NewMux(authnServer, handleInspectzAuthenticated, false /* allowAnonymous */)
	}
	// 注册 debug endpoint。
	s.mux.Handle(debug.Endpoint, handleDebugAuthenticated)
	// 注册 inspectz endpoint。
	s.mux.Handle(inspectz.URLPrefix, handleInspectzAuthenticated)

	// 记录一条启动事件，表示 HTTP endpoints 已经完成注册。
	log.Event(ctx, "added http endpoints")

	// 路由全部注册成功。
	return nil
}

func makeAdminAuthzCheckHandler(
	adminAuthzCheck privchecker.CheckerForRPCHandlers, handler http.Handler,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Retrieve the username embedded in the grpc metadata, if any.
		// This will be provided by the authenticationMux.
		md := authserver.TranslateHTTPAuthInfoToGRPCMetadata(req.Context(), req)
		authCtx := metadata.NewIncomingContext(req.Context(), md)
		// Check the privileges of the requester.
		err := adminAuthzCheck.RequireViewDebugPermission(authCtx)
		if err != nil {
			http.Error(w, "admin privilege or VIEWDEBUG global privilege required", http.StatusUnauthorized)
			return
		}
		// Forward the request to the inner handler.
		handler.ServeHTTP(w, req)
	})
}

// startHTTPService starts the network listener for the HTTP interface
// and also starts accepting incoming HTTP connections.
func startHTTPService(
	ctx, workersCtx context.Context,
	cfg *BaseConfig,
	uiTLSConfig *tls.Config,
	stopper *stop.Stopper,
	handler http.HandlerFunc,
) error {
	httpLn, err := ListenAndUpdateAddrs(ctx, &cfg.HTTPAddr, &cfg.HTTPAdvertiseAddr, "http", cfg.AcceptProxyProtocolHeaders)
	if err != nil {
		return err
	}
	log.Eventf(ctx, "listening on http port %s", cfg.HTTPAddr)

	// The HTTP listener shutdown worker, which closes everything under
	// the HTTP port when the stopper indicates we are shutting down.
	waitQuiesce := func(ctx context.Context) {
		// NB: we can't do this as a Closer because (*Server).ServeWith is
		// running in a worker and usually sits on accept() which unblocks
		// only when the listener closes. In other words, the listener needs
		// to close when quiescing starts to allow that worker to shut down.
		<-stopper.ShouldQuiesce()
		if err := httpLn.Close(); err != nil {
			log.Ops.Warningf(ctx, "%v", err)
		}
	}
	if err := stopper.RunAsyncTask(workersCtx, "wait-quiesce", waitQuiesce); err != nil {
		waitQuiesce(workersCtx)
		return err
	}

	if uiTLSConfig != nil {
		httpMux := cmux.NewWithTimeout(httpLn, time.Minute)
		clearL := httpMux.Match(cmux.HTTP1())
		tlsL := httpMux.Match(cmux.Any())

		// Dispatch incoming requests to either clearL or tlsL.
		if err := stopper.RunAsyncTask(workersCtx, "serve-ui", func(context.Context) {
			netutil.FatalIfUnexpected(httpMux.Serve())
		}); err != nil {
			return err
		}

		// Serve the plain HTTP (non-TLS) connection over clearL.
		// This produces a HTTP redirect to the `https` URL for the path /,
		// handles the request normally (via s.baseHandler) for the path /health,
		// and produces 404 for anything else.
		if err := stopper.RunAsyncTask(workersCtx, "serve-health", func(context.Context) {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				if HSTSEnabled.Get(&cfg.Settings.SV) {
					w.Header().Set(hstsHeaderKey, hstsHeaderValue)
				}
				http.Redirect(w, r, "https://"+r.Host+r.RequestURI, http.StatusTemporaryRedirect)
			})
			mux.Handle(healthPath, handler)

			plainRedirectServer := netutil.MakeHTTPServer(workersCtx, stopper, nil /* tlsConfig */, mux)

			netutil.FatalIfUnexpected(plainRedirectServer.Serve(clearL))
		}); err != nil {
			return err
		}

		httpLn = newTLSRestrictLn(tlsL, uiTLSConfig)
	}

	// The connManager is responsible for tearing down the net.Conn
	// objects when the stopper tells us to shut down.
	connManager := netutil.MakeHTTPServer(workersCtx, stopper, uiTLSConfig, handler)

	// Serve the HTTP endpoint. This will be the original httpLn
	// listening on --http-addr without TLS if uiTLSConfig was
	// nil, or overridden above if uiTLSConfig was not nil to come from
	// the TLS negotiation over the HTTP port.
	return stopper.RunAsyncTask(workersCtx, "server-http", func(context.Context) {
		netutil.FatalIfUnexpected(connManager.Serve(httpLn))
	})
}

type tlsRestrictLn struct {
	net.Listener
}

// Accept wraps the net.Listener Accept method to apply http based tls
// constraints on the incoming connection.
func (l tlsRestrictLn) Accept() (c net.Conn, err error) {
	if c, err = l.Listener.Accept(); err == nil {
		if err = security.TLSCipherRestrict(c); err != nil {
			_ = c.Close()
			return
		}
	}
	return
}

func newTLSRestrictLn(ln net.Listener, config *tls.Config) tlsRestrictLn {
	return tlsRestrictLn{
		Listener: tls.NewListener(ln, config),
	}
}

// baseHandler is the top-level HTTP handler for all HTTP traffic, before
// authentication and authorization.
//
// The server handles the node-to-node proxying first, and then runs the
// gzip middleware, then delegates to the mux for handling the request.
func (s *httpServer) baseHandler(w http.ResponseWriter, r *http.Request) {
	// Disable caching of responses.
	w.Header().Set("Cache-control", "no-store")

	if HSTSEnabled.Get(&s.cfg.Settings.SV) {
		w.Header().Set("Strict-Transport-Security", hstsHeaderValue)
	}

	// This is our base handler.
	// Intercept all panics, log them, and return an internal server error as a response.
	// There is an exception made for the `http.ErrAbortHandler` which the
	// `net/http` library suppresses stacktrace printing on. Since this defer
	// runs before the `http.Server` panic catcher, it should also ignore that
	// specific panic.
	defer func() {
		if p := recover(); p != nil && p != http.ErrAbortHandler {
			// Note: use of a background context here so we can log even with the absence of a client.
			// Assumes appropriate timeouts are used.
			logcrash.ReportPanic(context.Background(), &s.cfg.Settings.SV, p, 1 /* depth */)
			http.Error(w, srverrors.ErrAPIInternalErrorString, http.StatusInternalServerError)
		}
	}()

	s.proxy.nodeProxyHandler(w, r, s.gzMux.ServeHTTP)
}
