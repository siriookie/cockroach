# Codelab: 理解 OIDC Authentication

## 背景

这篇 codelab 讲 CockroachDB 里的 OIDC 登录是怎么接到 DB Console 上的。
它不是一篇只列配置项的使用手册，而是一篇源码阅读教程：先用配置把功能跑通，
再顺着 HTTP 请求进入 Go handler、UI、token 校验、SQL 用户名映射，以及可选的
SQL role 同步逻辑。

OIDC 建在 OAuth 2.0 之上。在这个项目里，可以先把它理解成下面这条链路：

```text
Browser -> CockroachDB DB Console -> external OIDC provider -> CockroachDB callback
```

这个登录流程里，CockroachDB 不直接让用户输入 SQL 密码。外部身份提供方负责认证
用户，然后把浏览器重定向回 CockroachDB，并带上 authorization code。CockroachDB
再拿这个 code 去换 token，验证 ID token，把 token 里的 claim 映射成 SQL 用户名，
最后创建普通 DB Console web session。

## 准备知识

读这篇之前，最好先知道普通 DB Console 登录的大致流程：用户输入 SQL 用户名和密码，
服务端验证成功后创建一个 web session cookie。OIDC 复用了后半段逻辑：外部 provider
证明用户身份之后，CockroachDB 调 `UserLoginFromSSO` 创建同一种 DB Console session
cookie。

主要源码在这些文件里：

```text
pkg/server/server_http.go
pkg/server/authserver/authentication.go
pkg/server/authserver/api.go
pkg/server/authserver/cookie.go
pkg/ccl/ccl_init.go
pkg/ccl/oidcccl/authentication_oidc.go
pkg/ccl/oidcccl/settings.go
pkg/ccl/oidcccl/state.go
pkg/ccl/oidcccl/authorization_oidc.go
pkg/ui/ui.go
pkg/ui/workspaces/db-console/src/views/login/oidc.tsx
pkg/ui/workspaces/db-console/src/views/jwt/jwtAuthToken.tsx
```

## 实现分层

这个项目里的 OIDC 可以分成三层。

第一层是 OSS hook，在 `pkg/server/authserver/authentication.go` 里。这里定义了
`OIDC` interface 和一个包变量 `ConfigureOIDC`。默认实现返回 `noOIDCConfigured`，
也就是不启用 OIDC。这样不带 CCL OIDC 包的构建不会有 OIDC 行为。

第二层是 CCL 实现，在 `pkg/ccl/oidcccl` 里。`pkg/ccl/ccl_init.go` 会 blank import
`pkg/ccl/oidcccl`，然后 `pkg/ccl/oidcccl/authentication_oidc.go` 的 `init` 函数把
真正的实现挂到 OSS hook 上：

```go
func init() {
  authserver.ConfigureOIDC = ConfigureOIDC
}
```

第三层是 UI。`pkg/server/server_http.go` 创建 DB Console UI handler 之前，会先调用
`authserver.ConfigureOIDC`，然后把返回的 `oidc` 对象塞进 `ui.Config`。UI 不直接读
cluster settings，而是通过 `GetOIDCConf` 拿到几个展示相关的值：OIDC 是否启用、
按钮文案、是否 autologin、是否展示生成 JWT auth token 的入口。

## 最小配置

真实部署时，operator 需要先在外部 identity provider 里把 CockroachDB 注册成一个
OIDC client，然后把 provider URL、client ID、client secret 和 redirect URL 配到
cluster settings 里。

一个单 DB Console endpoint 的核心配置大概是：

```sql
SET CLUSTER SETTING server.oidc_authentication.client_id = 'cockroachdb-console';
SET CLUSTER SETTING server.oidc_authentication.client_secret = 'secret-from-provider';
SET CLUSTER SETTING server.oidc_authentication.provider_url = 'https://idp.example.com';
SET CLUSTER SETTING server.oidc_authentication.redirect_url =
  'https://db.example.com:8080/oidc/v1/callback';
SET CLUSTER SETTING server.oidc_authentication.scopes = 'openid email profile';
SET CLUSTER SETTING server.oidc_authentication.claim_json_key = 'email';
SET CLUSTER SETTING server.oidc_authentication.principal_regex = '^([^@]+)@.*$';
SET CLUSTER SETTING server.oidc_authentication.enabled = true;
```

`scopes` 必须包含 `openid`。这是 OIDC 和普通 OAuth 2.0 的关键区别之一：包含
`openid` 后，provider 才会返回 ID token。

`claim_json_key` 和 `principal_regex` 决定 ID token 里的哪个字段会变成 SQL 用户名。
以上面的配置为例：

```text
ID token claim:  "email": "alice@example.com"
claim_json_key:  email
principal_regex: ^([^@]+)@.*$
SQL username:    alice
```

注意，这里的 SQL user 仍然需要已经存在，并且允许登录 DB Console。这个 OIDC 登录
路径不会自动创建 SQL user。

## 启动阶段：注册路由

启动入口在 `pkg/server/server_http.go` 的 `setupRoutes`。

DB Console UI handler 创建之前，server 会调用：

```go
oidc, err := authserver.ConfigureOIDC(...)
```

如果 CCL OIDC 包存在，这里会注册三个 HTTP route：

```text
/oidc/v1/login
/oidc/v1/callback
/oidc/v1/jwt
```

同一个 `oidc` 对象还会传给 `ui.Handler`。这点很重要：UI 不直接读 OIDC cluster
settings，而是通过 Go 侧的 `GetOIDCConf` 拿运行时配置。

注册 route 之后，`ConfigureOIDC` 会调用 `reloadConfig` 读取当前配置。它还会给 OIDC
相关 cluster settings 注册 `SetOnChange` callback，所以改这些 settings 时，内存里的
OIDC 配置会重新加载，不需要重启节点。

## 配置 reload

配置 reload 逻辑在 `pkg/ccl/oidcccl/authentication_oidc.go` 的 `reloadConfigLocked`。

这个函数会把所有 OIDC cluster settings 读进 `oidcAuthenticationConf`，然后构造一个
HTTP client。这个 client 会使用配置里的超时时间，也可以使用自定义 CA。之后代码会
解析 redirect URL，按 locality 选择 region-specific callback URL，拆分 scope 字符串，
最后创建 `oidcManager`。

`oidcManager` 保存 provider 相关的几个对象：

```text
oidc.Provider
oauth2.Config
oidc.IDTokenVerifier
HTTP client
```

创建 `oidc.Provider` 时，会访问 provider 的 OIDC discovery endpoint。比如 provider
URL 是：

```text
https://idp.example.com
```

那么这个地址下面必须能访问：

```text
https://idp.example.com/.well-known/openid-configuration
```

如果 provider 初始化失败，代码会保留“用户想启用 OIDC”这个意图，但不会标记为
initialized。后续 `/oidc/v1/login` 和 `/oidc/v1/callback` handler 会调用
`maybeInitializeLocked` 再试一次。这样 provider 临时不可用时，不一定需要 operator
手动重启节点。

## 登录流程

DB Console 里的 OIDC 按钮在
`pkg/ui/workspaces/db-console/src/views/login/oidc.tsx`。如果 OIDC 已启用，按钮链接到：

```text
oidc/v1/login
```

如果 autologin 打开，登录页会自动跳转到这个地址。URL 里带
`oidc_auto_login=false` 时，会跳过自动跳转。

`/oidc/v1/login` handler 做的事情是：

1. 锁住 OIDC server 的状态。
2. 确认 provider 已初始化。
3. 如果 OIDC 已禁用，直接拒绝请求。
4. 判断是普通登录模式，还是 JWT auth token 生成模式。
5. 生成随机 HMAC secret 和随机 state token。
6. 把 HMAC secret 放进安全的、HTTP-only 的 `oidc_secret` cookie。
7. 把带签名的 state token 交给 `AuthCodeURL`。
8. 重定向浏览器到外部 identity provider。

这里最关键的安全点是：发给 provider 的 state value 和浏览器里的 cookie 是通过 HMAC
绑定在一起的。callback 回来时，两者都必须存在，并且能验证通过。

## State 和 Cookie 防护

state 相关代码在 `pkg/ccl/oidcccl/state.go`。

`newKeyAndSignedToken` 会生成两个值：

```text
secret cookie: random HMAC key, base64 encoded
state value:   protobuf containing random token, HMAC(token), and mode
```

cookie 由 `pkg/server/authserver/cookie.go` 里的 `CreateOIDCCookie` 创建。它带有
`HttpOnly`、`Secure` 和 `SameSite=Lax`。

callback 阶段，`validate` 会 decode state，decode cookie 里的 HMAC key，重新计算
random token 的 HMAC，然后用 `hmac.Equal` 比较。

所以 callback 不是只要带着一个合法 OIDC code 就能通过。它还必须对应一次这个浏览器
从 CockroachDB 发起过的登录请求。

## Callback 流程

用户在 provider 侧认证成功后，provider 会把浏览器重定向到配置里的 callback URL：

```text
https://db.example.com:8080/oidc/v1/callback?state=...&code=...
```

`/oidc/v1/callback` handler 之后会走这条路径：

1. 锁住 OIDC server 的状态。
2. 确认 provider 已初始化。
3. 如果 OIDC 已禁用，直接拒绝请求。
4. 读取 query parameter 里的 `state`。
5. 读取浏览器里的 `oidc_secret` cookie。
6. 校验 cookie/state HMAC pair。
7. 用 authorization code 换 token。
8. 校验 ID token。
9. 从 ID token 提取 claims。
10. 把配置指定的 claim 映射成 SQL username。
11. 可选：根据 OIDC groups 同步 SQL roles。
12. 调 `UserLoginFromSSO` 创建普通 DB Console session。
13. 设置 web session cookie。
14. 重定向回 DB Console base path。

code exchange 和 token verification 封装在 `oidcManager` 的
`ExchangeVerifyGetTokenInfo` 里。exchange 会访问 OAuth2 token endpoint。ID token
verification 会使用 provider 的 verifier，检查签名和 client audience。

## Claim 到 SQL 用户名的映射

ID token 是 JSON。CockroachDB 需要从 JSON 里取一个字段作为 SQL username。

这个 setting 决定读哪个 claim：

```sql
SET CLUSTER SETTING server.oidc_authentication.claim_json_key = 'email';
```

这个 setting 决定怎么转换 claim value：

```sql
SET CLUSTER SETTING server.oidc_authentication.principal_regex = '^([^@]+)@.*$';
```

比如：

```text
Provider claim:
  "email": "bob@corp.example"

Regex:
  ^([^@]+)@.*$

Captured group:
  bob

SQL username:
  bob
```

regex 需要有捕获组，因为 CockroachDB 会拿捕获出来的值当 SQL principal。如果 claim
不存在，或者 claim value 不能被转换成 username，login 会在创建 web session 之前
失败。

## 创建 Web Session

拿到 SQL username 之后，callback handler 会调用
`pkg/server/authserver/authentication.go` 里的 `UserLoginFromSSO`。

这个函数会先 normalize SQL username，然后通过 SQL 查询用户 session 初始化信息，
确认用户存在并且允许登录 DB Console。通过后，它调用 `createSessionFor`。

`createSessionFor` 创建 row-backed web session，并返回标准 DB Console session
cookie。从这里开始，OIDC 就不特殊了：用户后续请求依赖的是普通 DB Console
cookie/session 机制。

## 可选：基于 Group 的授权同步

OIDC authentication 回答的是“这个用户是谁”。可选的 authorization 功能还会回答
“这个用户应该属于哪些 SQL roles”。

开启方式：

```sql
SET CLUSTER SETTING server.oidc_authentication.authorization.enabled = true;
```

开启后，`pkg/ccl/oidcccl/authorization_oidc.go` 里的 `authorize` 会尝试从这些地方读
groups：

```text
ID token
access token
userinfo endpoint
```

group claim 默认叫 `groups`，可以改：

```sql
SET CLUSTER SETTING server.oidc_authentication.group_claim = 'groups';
SET CLUSTER SETTING server.oidc_authentication.userinfo_group_key = 'groups';
```

实现会把 group names 转成 SQL role names，然后调用：

```go
sql.EnsureUserOnlyBelongsToRoles(...)
```

这意味着 SQL role membership 会被同步成 OIDC groups 的结果。如果开启了 authorization，
但是没有找到 group claim，或者 group list 是空的，登录会被拒绝。注意这个拒绝发生在
role 同步之后，所以它也可能已经清理掉用户原来的 role memberships。

## 可选：生成 JWT Auth Token

同一套 OIDC 配置还可以用来生成 cluster SSO 的 JWT auth token。开关是：

```sql
SET CLUSTER SETTING server.oidc_authentication.generate_cluster_sso_token.enabled = true;
```

开启后，UI 会展示第二个按钮，指向：

```text
oidc/v1/login?jwt
```

这个 query parameter 会把 state mode 从普通登录切到
`MODE_GENERATE_JWT_AUTH_TOKEN`。callback handler 识别到这个 mode 后，不会创建普通
web session，而是把浏览器重定向到 `/#/jwt/...` 这个 UI route。React 页面再调用：

```text
oidc/v1/jwt?state=...&code=...
```

`/oidc/v1/jwt` handler 会再次校验 state，用 code 换 token，根据
`generate_cluster_sso_token.use_token` 选择 ID token 或 access token，校验选中的
token，把 token principal 映射成 SQL usernames，最后返回 JSON 格式的连接信息。

## Multi-Region Redirect URLs

单 endpoint 部署时，`server.oidc_authentication.redirect_url` 可以是普通 URL 字符串：

```sql
SET CLUSTER SETTING server.oidc_authentication.redirect_url =
  'https://db.example.com:8080/oidc/v1/callback';
```

multi-region 部署时，同一个 setting 可以写成 JSON：

```sql
SET CLUSTER SETTING server.oidc_authentication.redirect_url = '
{
  "redirect_urls": {
    "us-east-1": "https://us-east.example.com:8080/oidc/v1/callback",
    "eu-west-1": "https://eu-west.example.com:8080/oidc/v1/callback"
  }
}';
```

当节点 locality 里有 `region` tier 时，reload 代码会选择该 region 对应的 callback
URL。如果 JSON map 里没有当前 region，OIDC 初始化会失败，直到 setting 修正。

## 排错思路

如果 DB Console 上看不到 OIDC 按钮，先查：

```sql
SHOW CLUSTER SETTING server.oidc_authentication.enabled;
SHOW CLUSTER SETTING server.oidc_authentication.button_text;
SHOW CLUSTER SETTING server.oidc_authentication.autologin.enabled;
```

如果点按钮后还没跳到 provider 就失败，优先查 provider 初始化相关配置：

```sql
SHOW CLUSTER SETTING server.oidc_authentication.provider_url;
SHOW CLUSTER SETTING server.oidc_authentication.redirect_url;
SHOW CLUSTER SETTING server.oidc_authentication.scopes;
```

provider URL 必须能被 CockroachDB 节点访问，并且必须能在
`/.well-known/openid-configuration` 暴露 discovery document。

如果用户已经在 provider 侧认证成功，但 callback 失败，重点查：

```sql
SHOW CLUSTER SETTING server.oidc_authentication.claim_json_key;
SHOW CLUSTER SETTING server.oidc_authentication.principal_regex;
```

然后看 provider 返回的 ID token claims。常见问题是配置了
`claim_json_key = 'email'`，但是请求的 scopes 没有让 provider 把 `email` claim 放进
ID token。

如果开启 authorization 后登录返回 forbidden，查 group claim 配置和 provider 返回的
token/userinfo 结构：

```sql
SHOW CLUSTER SETTING server.oidc_authentication.authorization.enabled;
SHOW CLUSTER SETTING server.oidc_authentication.group_claim;
SHOW CLUSTER SETTING server.oidc_authentication.userinfo_group_key;
```

还要确认 OIDC groups 是合法 SQL role names，或者能被 normalize 成合法 SQL role
names。

## 源码阅读顺序

按下面这个顺序读源码，基本能把 OIDC 端到端串起来：

1. `pkg/server/authserver/authentication.go`：默认 `ConfigureOIDC` hook 和 `OIDC`
   interface。
2. `pkg/ccl/ccl_init.go`：blank import 激活 CCL OIDC 包。
3. `pkg/ccl/oidcccl/settings.go`：cluster settings 和校验。
4. `pkg/server/server_http.go`：HTTP startup 路径，调用 `authserver.ConfigureOIDC`
   并把 OIDC config 传给 UI。
5. `pkg/ui/ui.go`：把 OIDC UI config 注入 index.html / uiconfig。
6. `pkg/ui/workspaces/db-console/src/views/login/oidc.tsx`：登录按钮和 autologin。
7. `pkg/ccl/oidcccl/authentication_oidc.go`：route 注册、config reload、login
   handler、callback handler、token exchange、JWT-token handler。
8. `pkg/ccl/oidcccl/state.go`：state/cookie HMAC 防护。
9. `pkg/server/authserver/authentication.go`：`UserLoginFromSSO` 和 session 创建。
10. `pkg/ccl/oidcccl/authorization_oidc.go`：可选的 group-to-role 同步。

这就是 OIDC 的核心路径：settings 配 provider，启动阶段注册 HTTP routes，UI 把浏览器
送到 `/oidc/v1/login`，provider 回调 `/oidc/v1/callback`，CockroachDB 校验 token 并把
claim 映射成 SQL user，最后交给普通 DB Console session 机制接管。
