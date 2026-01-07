// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverctl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
)

type ServerStartupInterface interface {
	ServerShutdownInterface

	// ClusterSettings retrieves this server's settings.
	ClusterSettings() *cluster.Settings

	// LogicalClusterID retrieves this server's logical cluster ID.
	LogicalClusterID() uuid.UUID

	// PreStart starts the server on the specified port(s) and
	// initializes subsystems.
	// It does not activate the pgwire listener over the network / unix
	// socket, which is done by the AcceptClients() method. The separation
	// between the two exists so that SQL initialization can take place
	// before the first client is accepted.
	// PreStart starts the server on the specified port(s) and
	// initializes subsystems.
	// PreStart 方法的作用：在指定的端口上启动服务器，并初始化各种子系统（subsystems）。
	// 具体包括：
	//   - 绑定并监听网络端口（RPC、HTTP、SQL 等端口）
	//   - 初始化存储引擎（Pebble/RocksDB）
	//   - 加载或创建集群 ID、节点 ID
	//- 检查 license、加密设置
	//- 启动后台任务（如 GC、replication、statistics 等）
	//   - 尝试加入现有集群（join）或准备 bootstrapping
	//   - 等等

	// It does not activate the pgwire listener over the network / unix
	// socket, which is done by the AcceptClients() method.
	// PreStart **不会**激活 pgwire（PostgreSQL wire protocol）监听器，即不会真正开始接受外部 SQL 客户端的连接（无论是 TCP 还是 Unix socket）。
	// pgwire 监听器的真正激活是由后续的 AcceptClients() 方法完成的。

	// The separation between the two exists so that SQL initialization can take place
	// before the first client is accepted.
	// 为什么要把 PreStart 和 AcceptClients 分开？
	// 核心原因：**为了在接受第一个外部 SQL 客户端连接之前，先完成必要的 SQL 系统初始化工作**。
	// 这些初始化工作包括：
	//   - 创建 system database 和系统表（如果这是新集群的第一节点）
	//   - 执行初始 SQL（如创建默认用户、设置、权限等）—— 见 RunInitialSQL()
	//   - 等待集群完全 bootstrapped（例如执行 cockroach init 或加入现有集群成功）
	//   - 确保系统表已就绪、range 已分配、replication 正常
	// 如果在这些初始化完成前就接受客户端连接，客户端可能会：
	//   - 连接成功但执行 SQL 时发现系统表不存在 → 报错
	//   - 看到集群处于不一致状态
	//   - 触发不必要的重试或超时
	// 通过 PreStart 先把端口绑好、内部准备就绪，但暂时不接受外部 SQL 连接；等到 AcceptClients() 时再放行，确保客户端一连接上来就能正常使用。
	PreStart(ctx context.Context) error

	// AcceptClients starts listening for incoming SQL clients over the network.
	AcceptClients(ctx context.Context) error
	// AcceptInternalClients starts listening for incoming internal SQL clients over the
	// loopback interface.
	AcceptInternalClients(ctx context.Context) error

	// InitialStart returns whether this node is starting for the first time.
	// This is (currently) used when displaying the server status report
	// on the terminal & in logs. We know that some folk have automation
	// that depend on certain strings displayed from this when orchestrating
	// KV-only nodes.
	InitialStart() bool

	// RunInitialSQL runs the SQL initialization for brand new clusters,
	// if the cluster is being started for the first time.
	// The arguments are:
	// - startSingleNode is used by 'demo' and 'start-single-node'.
	// - adminUser/adminPassword is used for 'demo'.
	RunInitialSQL(ctx context.Context, startSingleNode bool, adminUser, adminPassword string) error
}

// ServerShutdownInterface is the subset of the APIs on a server
// object that's sufficient to run a server shutdown.
type ServerShutdownInterface interface {
	AnnotateCtx(context.Context) context.Context
	Drain(ctx context.Context, verbose bool) (uint64, redact.RedactableString, error)
	ShutdownRequested() <-chan ShutdownRequest
}

// ShutdownRequest is used to signal a request to shutdown the server through
// server.stopTrigger. It carries the reason for the shutdown.
type ShutdownRequest struct {
	// Reason identifies the cause of the shutdown.
	Reason ShutdownReason
	// Err is populated for reason ServerStartupError and FatalError.
	Err error
}

// ShutdownReason identifies the reason for a ShutdownRequest.
type ShutdownReason int

const (
	// ShutdownReasonDrainRPC represents a drain RPC with the shutdown flag set.
	ShutdownReasonDrainRPC ShutdownReason = iota
	// ShutdownReasonServerStartupError means that the server startup process
	// failed.
	ShutdownReasonServerStartupError
	// ShutdownReasonFatalError identifies an error that requires the server be
	// terminated immediately.
	ShutdownReasonFatalError
	// ShutdownReasonGracefulStopRequestedByOrchestration is used when a graceful shutdown
	// was requested by orchestration.
	ShutdownReasonGracefulStopRequestedByOrchestration
)
