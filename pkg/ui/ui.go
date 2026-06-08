// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package ui embeds the assets for the web UI into the Cockroach binary.
//
// By default, it serves a stub web UI. Linking with distccl will replace the
// stubs with the OSS UI or the CCL UI, respectively. The exported symbols in
// this package are thus function pointers instead of functions so that they can
// be mutated by init hooks.
package ui

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	_ "github.com/cockroachdb/cockroach/pkg/ui/settings" // Import the settings package to register UI-related settings for doc generation.
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	cspHeader = "default-src 'self'; " +
		"style-src 'self' 'unsafe-inline'; " +
		"font-src 'self' data:; " +
		"img-src 'self' data:; " +
		"connect-src 'self' https://register.cockroachdb.com;"
)

// TODO(davidh): This setting can be removed after 24.3 since it only
// affects legacy DB page.
var DatabaseLocalityMetadataEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"ui.database_locality_metadata.enabled",
	"if enabled shows extended locality data about databases and tables in DB Console which can be expensive to compute",
	true,
	settings.WithPublic)

// Assets is used for embedded JS assets required for UI.
// In case the binary is built without UI, it provides single index.html file with
// the same content as indexHTML as a fallback.
var Assets fs.FS

// HaveUI tells whether the admin UI has been linked into the binary.
var HaveUI = false

// indexHTML contains HTML which includes the UI JavaScript bundles for
// DB console. It contains a template variable for the nonce attribute to
// provide with the "Content-Security-Policy" header.
var indexHTML = []byte(`<!DOCTYPE html>
<html>
	<head>
		<title>Cockroach Console</title>
		<meta charset="UTF-8">
		<link href="favicon.ico" rel="shortcut icon">
	</head>
	<body>
		<div id="react-layout"></div>
		<script src="bundle.js" type="text/javascript"></script>
	</body>
</html>
`)

type indexHTMLArgs struct {
	// Insecure means disable auth entirely - anyone can use.
	Insecure         bool
	LoggedInUser     *string
	Tag              string
	Version          string
	NodeID           string
	OIDCAutoLogin    bool
	OIDCLoginEnabled bool
	OIDCButtonText   string
	FeatureFlags     serverpb.FeatureFlags

	OIDCGenerateJWTAuthTokenEnabled bool

	LicenseType               string
	SecondsUntilLicenseExpiry int64
	IsManaged                 bool
}

// OIDCUIConf is a variable that stores data required by the
// Admin UI to display and manage the OIDC login flow. It is
// provided by the `oidcAuthenticationServer` at runtime
// since that's where all the OIDC configuration is centralized.
type OIDCUIConf struct {
	ButtonText string
	AutoLogin  bool
	Enabled    bool

	GenerateJWTAuthTokenEnabled bool
}

// OIDCUI is an interface that our OIDC configuration must implement in order to be able
// to pass relevant configuration info to the ui module. This is to pass through variables that
// are necessary to render an appropriate user interface for OIDC support and to set the state
// cookie that OIDC requires for securing auth requests.
type OIDCUI interface {
	GetOIDCConf() OIDCUIConf
}

// bareIndexHTML is used in place of indexHTMLTemplate when the binary is built
// without the web UI.
var bareIndexHTML = []byte(fmt.Sprintf(`<!DOCTYPE html>
<title>CockroachDB</title>
Binary built without web UI.
<hr>
<em>%s</em>`, build.GetInfo().Short()))

// Config contains the configuration parameters for Handler.
type Config struct {
	Insecure bool
	NodeID   *base.NodeIDContainer
	GetUser  func(ctx context.Context) *string
	OIDC     OIDCUI
	Flags    serverpb.FeatureFlags
	Settings *cluster.Settings
}

var uiConfigPath = regexp.MustCompile("^/uiconfig$")

// Handler returns an http.Handler that serves the UI,
// including index.html, which has some login-related variables
// templated into it, as well as static assets.
func Handler(cfg Config) http.Handler {
	// etags is used to provide a unique per-file checksum for each served file,
	// which enables client-side caching using Cache-Control and ETag headers.
	// 创建一个 map，用来保存每个 UI 静态资源文件对应的 ETag。
	etags := make(map[string]string)
	// 只有当前二进制包含 UI 资源，并且 Assets 不为空时，才需要计算静态资源的 ETag。
	if HaveUI && Assets != nil {
		// Only compute hashes for UI-enabled builds
		// 遍历 Assets 中的静态文件，为每个文件计算 checksum，并写入 etags。
		err := httputil.ComputeEtags(Assets, etags)
		// ETag 计算失败不阻断 UI 服务，只记录日志。
		if err != nil {
			log.Dev.Errorf(context.Background(), "Unable to compute asset hashes: %+v", err)
		}
	}

	// 构造静态文件 handler，并在外层包一层 EtagHandler。
	fileHandlerChain := httputil.EtagHandler(
		// 把前面计算出来的 etags 传进去，响应静态资源时可以带 ETag。
		etags,
		// http.FileServer 负责从文件系统抽象里读取并返回静态文件。
		http.FileServer(
			// Assets 是 embed 出来的前端资源，这里转成 http.FileSystem 可识别的形式。
			http.FS(Assets),
		),
	)
	// 读取当前二进制的 build 信息，后面要用于 index.html 的缓存时间和模板变量。
	buildInfo := build.GetInfo()

	// 返回一个真正处理 DB Console HTTP 请求的 handler。
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 查询当前 license 类型，用来注入给 UI 展示和逻辑判断。
		licenseType, err := base.LicenseType(cfg.Settings)
		// license 类型获取失败不阻断 UI 返回，只记录日志。
		if err != nil {
			log.Dev.Errorf(context.Background(), "unable to get license type: %+v", err)
		}
		// 计算 license 距离过期还有多少秒，也会作为 UI 配置返回给前端。
		licenseTTL := base.GetLicenseTTL(r.Context(), cfg.Settings, timeutil.DefaultTimeSource{})
		// 从 OIDC 模块读取给 UI 用的运行时配置，例如是否启用、按钮文案、是否 autologin。
		oidcConf := cfg.OIDC.GetOIDCConf()
		// 读取当前 build 对应的 release series，用来生成 UI 里的版本字符串。
		major, minor := build.BranchReleaseSeries()
		// 组装注入 index.html 或 /uiconfig 的配置数据。
		args := indexHTMLArgs{
			// 是否是 insecure web access；如果是，UI 可以知道当前认证被关闭。
			Insecure: cfg.Insecure,
			// 当前已登录用户；匿名访问或 session 不存在时是 nil。
			LoggedInUser: cfg.GetUser(r.Context()),
			// 当前 build tag，例如正式版本号或 dev tag。
			Tag: buildInfo.Tag,
			// UI 展示用的主版本号字符串。
			Version: fmt.Sprintf("v%d.%d", major, minor),
			// OIDC 是否开启自动跳转登录。
			OIDCAutoLogin: oidcConf.AutoLogin,
			// OIDC 登录按钮是否应该展示。
			OIDCLoginEnabled: oidcConf.Enabled,
			// OIDC 登录按钮上显示的文字。
			OIDCButtonText: oidcConf.ButtonText,
			// 后端下发给 UI 的 feature flags。
			FeatureFlags: cfg.Flags,

			// 是否展示“生成 JWT auth token for cluster SSO”的 UI 入口。
			OIDCGenerateJWTAuthTokenEnabled: oidcConf.GenerateJWTAuthTokenEnabled,

			// 当前 license 类型。
			LicenseType: licenseType,
			// license 剩余有效时间，单位是秒。
			SecondsUntilLicenseExpiry: licenseTTL,
			// log.RedactionPolicyManaged is set to true only when cluster is running under
			// managed service control.
			// 标记当前集群是否运行在 managed service control 下。
			IsManaged: log.RedactionPolicyManaged,
		}
		// NodeID 可能为空；只有非空时才把它写入 UI 配置。
		if cfg.NodeID != nil {
			// 把 NodeID 转成字符串，方便模板和前端 JSON 使用。
			args.NodeID = cfg.NodeID.String()
		}
		// /uiconfig 是一个专门给前端读取运行时配置的 JSON endpoint。
		if uiConfigPath.MatchString(r.URL.Path) {
			// 把上面组装好的 args 序列化成 JSON。
			argBytes, err := json.Marshal(args)
			// JSON 序列化失败时，返回 500。
			if err != nil {
				log.Dev.Errorf(r.Context(), "unable to deserialize ui config args: %v", err)
				http.Error(w, err.Error(), 500)
				return
			}
			// 把 JSON 配置写回给前端。
			_, err = w.Write(argBytes)
			// 写响应失败时，记录日志并返回 500。
			if err != nil {
				log.Dev.Errorf(r.Context(), "unable to write ui config args: %v", err)
				http.Error(w, err.Error(), 500)
				return
			}
			// /uiconfig 请求已经处理完，直接返回。
			return
		}

		// 开发模式请求会带 Crdb-Development header，用来直接返回 indexHTML。
		if r.Header.Get("Crdb-Development") != "" {
			// 给 index.html 设置 Content-Security-Policy。
			w.Header().Set("Content-Security-Policy", cspHeader)
			// 返回内存里的 index.html，并使用 build 时间作为修改时间。
			http.ServeContent(w, r, "index.html", buildInfo.GoTime(), bytes.NewReader(indexHTML))
			return
		}

		// 如果当前二进制没有打包 UI，就返回一个极简的提示页面。
		if !HaveUI {
			// bareIndexHTML 告诉用户这个二进制没有 web UI。
			http.ServeContent(w, r, "index.html", buildInfo.GoTime(), bytes.NewReader(bareIndexHTML))
			return
		}

		// 非根路径请求一般是静态资源，例如 bundle.js、css、图片等。
		if r.URL.Path != "/" {
			// 交给静态文件 handler 处理，并自动带上 ETag 相关逻辑。
			fileHandlerChain.ServeHTTP(w, r)
			return
		}

		// 根路径 "/" 返回 DB Console 的 index.html。
		w.Header().Set("Content-Security-Policy", cspHeader)
		// ServeContent 会处理缓存相关 header，并把 indexHTML 写给浏览器。
		http.ServeContent(w, r, "index.html", buildInfo.GoTime(), bytes.NewReader(indexHTML))
	})
}
