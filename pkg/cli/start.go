// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflagcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/profiler"
	"github.com/cockroachdb/cockroach/pkg/server/serverctl"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/cgroups"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/sdnotify"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
	"github.com/spf13/cobra"
)

// debugTSImportFile is the path to a file (containing data coming from
// `./cockroach debug tsdump --format=raw` that will be ingested upon server
// start. This is an experimental feature and may break clusters it is invoked
// against. The data will not display properly in the UI unless the source
// cluster had one store to each node, with the store ID and node ID lining up.
// Additionally, the local server's stores and nodes must match this pattern as
// well. The only expected use case for this env var is against local single
// node throwaway clusters and consequently this variable is only used for
// the start-single-node command.
//
// To be able to visualize the timeseries data properly, a mapping file must be
// provided as well. This maps StoreIDs to the owning NodeID, i.e. the file
// looks like this (if s1 is on n3 and s2 is on n4):
// 1: 3
// 2: 4
// [...]
//
// See #64329 for details.
var debugTSImportFile = envutil.EnvOrDefaultString("COCKROACH_DEBUG_TS_IMPORT_FILE", "")
var debugTSImportMappingFile = envutil.EnvOrDefaultString("COCKROACH_DEBUG_TS_IMPORT_MAPPING_FILE", "")

// startCmd starts a node by initializing the stores and joining
// the cluster.
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start a node in a multi-node cluster",
	Long: `
Start a CockroachDB node, which will export data from one or more
storage devices, specified via --store flags.

Specify the --join flag to point to another node or nodes that are
part of the same cluster. The other nodes do not need to be started
yet, and if the address of the other nodes to be added are not yet
known it is legal for the first node to join itself.

To initialize the cluster, use 'cockroach init'.
`,
	Example: `  cockroach start --insecure --store=attrs=ssd,path=/mnt/ssd1 --join=host:port,[host:port]`,
	Args:    cobra.NoArgs,
	RunE:    clierrorplus.MaybeShoutError(clierrorplus.MaybeDecorateError(runStartJoin)),
}

// startSingleNodeCmd starts a node by initializing the stores.
var startSingleNodeCmd = &cobra.Command{
	Use:   "start-single-node",
	Short: "start a single-node cluster",
	Long: `
Start a CockroachDB node, which will export data from one or more
storage devices, specified via --store flags.
The cluster will also be automatically initialized with
replication disabled (replication factor = 1).
`,
	Example: `  cockroach start-single-node --insecure --store=attrs=ssd,path=/mnt/ssd1`,
	Args:    cobra.NoArgs,
	RunE:    clierrorplus.MaybeShoutError(clierrorplus.MaybeDecorateError(runStartSingleNode)),
}

// StartCmds lists the commands that start KV nodes as a server.
// This includes 'start' and 'start-single-node' but excludes
// the MT SQL server (not KV node) and 'demo' (not a server).
var StartCmds = []*cobra.Command{startCmd, startSingleNodeCmd}

// serverCmds lists the commands that start servers.
var serverCmds = append(StartCmds, mtStartSQLCmd)

// customLoggingSetupCmds lists the commands that call setupLogging()
// after other types of configuration.
var customLoggingSetupCmds = append(
	serverCmds, debugCheckLogConfigCmd, demoCmd, statementBundleRecreateCmd,
)

// RegisterCommandWithCustomLogging is used by cliccl to note commands which
// want to suppress default logging setup.
func RegisterCommandWithCustomLogging(cmd *cobra.Command) {
	customLoggingSetupCmds = append(customLoggingSetupCmds, cmd)
}

func initBlockProfile() {
	// Enable the block profile for a sample of mutex and channel operations.
	// Smaller values provide more accurate profiles but are more
	// expensive. 0 and 1 are special: 0 disables the block profile and
	// 1 captures 100% of block events. For other values, the profiler
	// will sample one event per X nanoseconds spent blocking.
	//
	// The block profile can be viewed with `pprof http://HOST:PORT/debug/pprof/block`
	//
	// The utility of the block profile (aka blocking profile) has diminished
	// with the advent of the mutex profile. We currently leave the block profile
	// disabled by default as it has a non-zero performance impact.
	d := envutil.EnvOrDefaultInt64("COCKROACH_BLOCK_PROFILE_RATE", 0)
	runtime.SetBlockProfileRate(int(d))
}

func initMutexProfile() {
	// Enable the mutex profile for a fraction of mutex contention events.
	// Smaller values provide more accurate profiles but are more expensive. 0
	// and 1 are special: 0 disables the mutex profile and 1 captures 100% of
	// mutex contention events. For other values, the profiler will sample on
	// average 1/X events.
	//
	// The mutex profile can be viewed with `pprof http://HOST:PORT/debug/pprof/mutex`
	d := envutil.EnvOrDefaultInt("COCKROACH_MUTEX_PROFILE_RATE",
		1000 /* 1 sample per 1000 mutex contention events */)
	runtime.SetMutexProfileFraction(d)
}

func initTraceDir(ctx context.Context, dir string) {
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		// This is possible when running with only in-memory stores;
		// in that case the start-up code sets the output directory
		// to the current directory (.). If running the process
		// from a directory which is not writable, we won't
		// be able to create a sub-directory here.
		err = errors.WithHint(err, "Try changing the CWD of the cockroach process to a writable directory.")
		log.Dev.Warningf(ctx, "cannot create trace dir; traces will not be dumped: %v", err)
		return
	}
}

// SQL 操作的磁盘溢出：比如排序（ORDER BY 无 LIMIT）、哈希连接（hash joins）、聚合、行容器（row containers）等 DistSQL 处理器，当处理的数据量太大、无法全部放进内存时，会把中间结果临时写到磁盘。
// 大批量导入（IMPORT）：IMPORT 操作特别依赖它，所有导入的数据都会先缓冲到 temp store 中，进行排序和处理。
// 其他需要临时缓冲大量数据的 SQL 操作。
//
// 它本质上是一个独立的存储实例（基于 Pebble/RocksDB），但使用模式和主存储不同：写一大批数据、读一遍就删，不需要持久化。节点启动时会清理旧的临时目录。
func initTempStorageConfig(
	ctx context.Context, st *cluster.Settings, stopper *stop.Stopper, stores base.StoreSpecList,
) (base.TempStorageConfig, error) {
	// Initialize the target directory for temporary storage. If encryption at
	// rest is enabled in any fashion, we'll want temp storage to be encrypted
	// too. To achieve this, we use the first encrypted store as temp dir
	// target, if any. If we can't find one, we use the first StoreSpec in the
	// list.
	//
	// Note that we already cleaned up any abandoned temporary directories from
	// the previous process earlier in the startup sequence (see
	// reclaimDiskSpace).
	//初始化临时存储的目标目录。
	//如果系统中启用了任何形式的“静态加密”（encryption at rest，即数据在磁盘上存储时加密），
	//那么临时存储目录也应该同样被加密。为了实现这一点，
	//我们会优先选择列表中第一个已启用加密的存储（store）作为临时目录的目标。
	//如果找不到任何加密的存储，就使用列表中的第一个 StoreSpec（存储规格）。
	//注意： 在启动序列的早期，我们已经清理了上一个进程遗留的任何废弃临时目录（参见 reclaimDiskSpace 函数）。
	//简单来说，这是数据库（如 CockroachDB）或其他分布式存储系统在启动时，选择临时文件目录的逻辑，
	//目的是确保如果主存储支持加密，临时文件也不会泄露明文数据，同时提到之前已经处理了旧的临时垃圾目录。
	specIdxDisk := -1
	specIdxEncrypted := -1
	for i, spec := range stores.Specs {
		if spec.InMemory {
			continue
		}
		if spec.IsEncrypted() && specIdxEncrypted == -1 {
			// TODO(jackson): One store's EncryptionOptions may say to encrypt
			// with a real key, while another store's say to use key=plain.
			// This provides no guarantee that we'll use the encrypted one's.
			specIdxEncrypted = i
		}
		if specIdxDisk == -1 {
			specIdxDisk = i
		}
	}

	// Use first store by default. This might be an in-memory store.
	specIdx := 0
	if specIdxEncrypted >= 0 {
		// Prefer an encrypted store.
		specIdx = specIdxEncrypted
	} else if specIdxDisk >= 0 {
		// Prefer a non-encrypted on-disk store.
		specIdx = specIdxDisk
	}
	useStore := stores.Specs[specIdx]

	// The temp store size can depend on the location of the first regular store
	// (if it's expressed as a percentage), so we resolve that flag here.
	var tempStorePercentageResolver percentResolverFunc
	if !useStore.InMemory {
		dir := useStore.Path
		// Create the store dir, if it doesn't exist. The dir is required to exist
		// by diskPercentResolverFactory.
		if err := os.MkdirAll(dir, 0755); err != nil {
			return base.TempStorageConfig{}, errors.Wrapf(err, "failed to create dir for first store: %s", dir)
		}
		var err error
		tempStorePercentageResolver, err = diskPercentResolverFactory(dir)
		if err != nil {
			return base.TempStorageConfig{}, errors.Wrapf(err, "failed to create resolver for: %s", dir)
		}
	} else {
		tempStorePercentageResolver = memoryPercentResolver
	}
	var tempStorageMaxSizeBytes int64
	if err := startCtx.diskTempStorageSizeValue.Resolve(
		&tempStorageMaxSizeBytes, tempStorePercentageResolver,
	); err != nil {
		return base.TempStorageConfig{}, err
	}
	if !startCtx.diskTempStorageSizeValue.IsSet() {
		// 如果用户没有显式设置磁盘临时存储的大小（即 --max-disk-temp-storage 参数未被设置）
		// 那么我们需要使用默认值
		if startCtx.tempDir == "" && useStore.InMemory {
			// 情况1：临时存储目录未指定（tempDir == ""），并且第一个主存储是内存存储（InMemory）
			// 这意味着整个节点运行在纯内存模式，临时存储也会落在内存中
			// 此时使用较小的默认上限，防止内存被临时数据耗尽
			tempStorageMaxSizeBytes = base.DefaultInMemTempStorageMaxSizeBytes
		} else {
			// 情况2：其他所有情况（包括有指定 tempDir，或第一个 store 是磁盘存储）
			// 临时存储会落在磁盘上，可以使用更大的默认上限
			tempStorageMaxSizeBytes = base.DefaultTempStorageMaxSizeBytes
		}
	}
	// If all stores are in-memory and no temp dir was specified, the temp
	// store will also be in memory. This is a testing scenario.
	if startCtx.tempDir == "" && useStore.InMemory {
		return base.NewTempStorageConfig(
			ctx,
			st,
			true, // inMem
			"",   // path
			useStore.EncryptionOptions,
			tempStorageMaxSizeBytes,
			"", // tempDirsRecordPath
		), nil
	}

	// Set temp directory to first store's path if the temp storage is not
	// in memory.
	parentDir := startCtx.tempDir
	if parentDir == "" {
		parentDir = useStore.Path
	}

	tmpPath, unlockDirFn, err := fs.CreateTempDir(parentDir, server.TempDirPrefix)
	if err != nil {
		return base.TempStorageConfig{}, errors.Wrap(err, "could not create temporary directory for temp storage")
	}

	recordPath := ""
	if useStore.InMemory {
		stopper.AddCloser(stop.CloserFn(func() {
			unlockDirFn()
			// Remove the temp directory directly since there is no record file.
			if err := os.RemoveAll(tmpPath); err != nil {
				log.Dev.Errorf(ctx, "could not remove temporary store directory: %v", err.Error())
			}
		}))
	} else {
		recordPath = filepath.Join(useStore.Path, server.TempDirsRecordFilename)
		if err := fs.RecordTempDir(recordPath, tmpPath); err != nil {
			return base.TempStorageConfig{}, errors.Wrapf(
				err,
				"could not record temporary directory path to record file: %s",
				recordPath,
			)
		}
		// Remove temporary directory on shutdown.
		stopper.AddCloser(stop.CloserFn(func() {
			unlockDirFn()
			if err := fs.CleanupTempDirs(ctx, vfs.Default, recordPath); err != nil {
				log.Dev.Errorf(ctx, "could not remove temporary store directory: %v", err.Error())
			}
		}))
	}

	// Initialize a base.TempStorageConfig based on first store's spec and
	// cli flags.
	return base.NewTempStorageConfig(
		ctx,
		st,
		false, // inMem
		tmpPath,
		useStore.EncryptionOptions,
		tempStorageMaxSizeBytes,
		recordPath,
	), nil
}

type newServerFn func(ctx context.Context, serverCfg server.Config, stopper *stop.Stopper) (serverctl.ServerStartupInterface, error)

var errCannotUseJoin = errors.New("cannot use --join with 'cockroach start-single-node' -- use 'cockroach start' instead")

func runStartSingleNode(cmd *cobra.Command, args []string) error {
	joinFlag := cliflagcfg.FlagSetForCmd(cmd).Lookup(cliflags.Join.Name)
	if joinFlag.Changed {
		return errCannotUseJoin
	}
	// Now actually set the flag as changed so that the start code
	// doesn't warn that it was not set. This is all to let `start-single-node`
	// get by without the use of --join flags.
	joinFlag.Changed = true

	// Make the node auto-init the cluster if not done already.
	serverCfg.AutoInitializeCluster = true

	// Allow passing in a timeseries file.
	if debugTSImportFile != "" {
		serverCfg.TestingKnobs.Server = &server.TestingKnobs{
			ImportTimeseriesFile:        debugTSImportFile,
			ImportTimeseriesMappingFile: debugTSImportMappingFile,
		}
	}

	return runStart(cmd, args, true /*startSingleNode*/)
}

func runStartJoin(cmd *cobra.Command, args []string) error {
	return runStart(cmd, args, false /*startSingleNode*/)
}

// runStart starts the cockroach node using --store as the list of
// storage devices ("stores") on this machine and --join as the list
// of other active nodes used to join this node to the cockroach
// cluster, if this is its first time connecting.
//
// The argument startSingleNode is morally equivalent to `cmd ==
// startSingleNodeCmd`, and triggers special initialization specific
// to one-node clusters. See server/initial_sql.go for details.
//
// We need a separate argument instead of solely relying on cmd
// because we cannot refer to startSingleNodeCmd under
// runStartInternal: there would be a cyclic dependency between
// runStart, runStartSingleNode and runStartSingleNodeCmd.
func runStart(cmd *cobra.Command, args []string, startSingleNode bool) error {
	const serverType redact.SafeString = "node"

	newServerFn := func(_ context.Context, serverCfg server.Config, stopper *stop.Stopper) (serverctl.ServerStartupInterface, error) {
		// Beware of not writing simply 'return server.NewServer()'. This is
		// because it would cause the serverctl.ServerStartupInterface reference to
		// always be non-nil, even if NewServer returns a nil pointer (and
		// an error). The code below is dependent on the interface
		// reference remaining nil in case of error.
		s, err := server.NewServer(serverCfg, stopper)
		if err != nil {
			return nil, err
		}
		return s, nil
	}

	return runStartInternal(cmd, serverType, serverCfg.InitNode, newServerFn, startSingleNode)
}

// runStartInternal contains the code common to start a regular server
// or a SQL-only server.
func runStartInternal(
	cmd *cobra.Command,
	serverType redact.SafeString,
	initConfigFn func(context.Context) error,
	newServerFn newServerFn,
	startSingleNode bool,
) error {
	tBegin := timeutil.Now()

	// First things first: if the user wants background processing,
	// relinquish the terminal ASAP by forking and exiting.
	//
	// If executing in the background, the function returns ok == true in
	// the parent process (regardless of err) and the parent exits at
	// this point.
	//首先要处理的是： 如果用户想要在后台处理（运行），
	//程序应通过 fork（派生子进程） 和 exit（退出当前进程） 的方式，尽快（ASAP）让出对终端控制台的控制权。
	//如果程序设定为在后台执行，该函数在父进程中会返回 ok == true（无论是否报错），随后父进程会在此处直接退出。
	if ok, err := maybeRerunBackground(); ok {
		return err
	}

	// Change the permission mask for all created files.
	//
	// We're considering everything produced by a cockroach node
	// to potentially contain sensitive information, so it should
	// not be world-readable.
	//修改所有新建文件的权限掩码（permission mask）。
	//我们认为 CockroachDB 节点生成的任何内容都可能包含敏感信息，
	//因此这些内容不应该对系统中所有用户可见（world-readable）
	disableOtherPermissionBits()

	// Set up the signal handlers. This also ensures that any of these
	// signals received beyond this point do not interrupt the startup
	// sequence until the point signals are checked below.
	// We want to set up signal handling before starting logging, because
	// logging uses buffering, and we want to be able to sync
	// the buffers in the signal handler below. If we started capturing
	// signals later, some startup logging might be lost.
	//设置信号处理器（Signal Handlers）。
	//这也确保了从此刻起收到的任何（系统）信号，在运行到下文具体的信号检查点之前，都不会中断程序的启动流程。
	//我们希望在启动日志记录之前就设置好信号处理，因为日志记录使用了缓冲机制（buffering），
	//而我们希望能在下文的信号处理器中同步（sync，即强制写入磁盘）这些缓冲区
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, DrainSignals...)
	if exitAbruptlySignal != nil {
		signal.Notify(signalCh, exitAbruptlySignal)
	}

	// Set up a cancellable context for the entire start command.
	// The context will be canceled at the end.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Check for stores with full disks and exit with an informative exit
	// code. This needs to happen early during start, before we perform any
	// writes to the filesystem including log rotation. We need to guarantee
	// that the process continues to exit with the Disk Full exit code. A
	// flapping exit code can affect alerting, including the alerting
	// performed within CockroachCloud.
	//检查存储目录（stores）是否存在磁盘空间已满的情况，如果已满，
	//则带着一个**具有明确含义的退出状态码（exit code）**退出程序。这一检查必须在启动过程的早期阶段进行，
	//要在对文件系统执行任何写入操作（包括日志滚动处理）之前完成。
	if err := exitIfDiskFull(ctx, vfs.Default, serverCfg.Stores.Specs); err != nil {
		return err
	}

	// If any store has something to say against a server start-up
	// (e.g. previously detected corruption), listen to them now.
	if err := serverCfg.Stores.PriorCriticalAlertError(); err != nil {
		return clierror.NewError(err, exit.FatalError())
	}

	// Set a MakeProcessUnavailableFunc that will close all sockets. This guards
	// against a persistent disk stall that prevents the process from exiting or
	// making progress.
	//这段代码设置了一个“自杀预案”。如果日志系统发现磁盘长时间无响应（无法写日志），它会调用 closeAllSockets。
	log.SetMakeProcessUnavailableFunc(closeAllSockets)

	// The context annotation ensures that server identifiers show up
	// in the logging metadata as soon as they are known.
	ambientCtx := serverCfg.AmbientCtx

	// Annotate the context, and set up a tracing span for the start process.
	//
	// The context annotation ensures that server identifiers show up
	// in the logging metadata as soon as they are known.
	//
	// The tracing span is because we want any logging happening beyond
	// this point to be accounted to this start context, including
	// logging related to the initialization of the logging
	// infrastructure below.  This span concludes when the startup
	// goroutine started below has completed.  TODO(andrei): we don't
	// close the span on the early returns below.
	//：为“服务器启动”这个过程开启一个追踪跨度（Tracing Span）。
	//
	//意义：数据库启动包含成百上千个步骤（加载配置、初始化磁盘、开启网络等）。通过这个 Span，
	//开发者可以在可视化界面（如 Jaeger 或 Zipkin）中清晰地看到启动过程中每一步花了多少时间，哪一步卡住了。
	var startupSpan *tracing.Span
	ctx, startupSpan = ambientCtx.AnnotateCtxWithSpan(ctx, "server start")

	// Set up the logging and profiling output.
	//
	// We want to do this as early as possible, because most of the code
	// in CockroachDB may use logging, and until logging has been
	// initialized log files will be created in $TMPDIR instead of their
	// expected location.
	//
	// This initialization uses the various configuration parameters
	// initialized by flag handling (before runStart was called). Any
	// additional server configuration tweaks for the startup process
	// must be necessarily non-logging-related, as logging parameters
	// cannot be picked up beyond this point.
	stopper, err := setupAndInitializeLoggingAndProfiling(ctx, cmd, true /* isServerCmd */)
	if err != nil {
		return err
	}
	stopper.SetTracer(serverCfg.BaseConfig.AmbientCtx.Tracer)

	// Tweak GOMAXPROCS if we're in a cgroup / container that has cpu limits set.
	// The GO default for GOMAXPROCS is NumCPU(), however this is less
	// than ideal if the cgroup is limited to a number lower than that.
	//
	// TODO(bilal): various global settings have already been initialized based on
	// GOMAXPROCS(0) by now.
	cgroups.AdjustMaxProcs(ctx)

	fs := cliflagcfg.FlagSetForCmd(cmd)

	// Check the --join flag.
	if fl := fs.Lookup(cliflags.Join.Name); fl != nil && !fl.Changed {
		err := errors.WithHint(
			errors.New("no --join flags provided to 'cockroach start'"),
			"Consider using 'cockroach init' or 'cockroach start-single-node' instead")
		return err
	}

	// Check the --tenant-id-file flag.
	if fl := fs.Lookup(cliflags.TenantIDFile.Name); fl != nil && fl.Changed {
		fileName := fl.Value.String()
		serverCfg.DelayedSetTenantID = func(
			ctx context.Context,
		) (roachpb.TenantID, roachpb.Locality, error) {
			tenantID, err := tenantIDFromFile(ctx, fileName, nil, nil, nil)
			if err != nil {
				return roachpb.TenantID{}, roachpb.Locality{}, err
			}
			if err := tryReadLocalityFileFlag(fs); err != nil {
				return roachpb.TenantID{}, roachpb.Locality{}, err
			}
			return tenantID, serverCfg.Locality, nil
		}
	}

	// Now perform additional configuration tweaks specific to the start
	// command.

	// Set the soft memory limit on the Go runtime.
	if err = func() error {
		if startCtx.goMemLimitValue.IsSet() {
			if goMemLimit < 0 {
				return errors.New("--max-go-memory must be non-negative")
			} else if goMemLimit > 0 && goMemLimit < defaultGoMemLimitMinValue {
				log.Ops.Shoutf(
					ctx, severity.WARNING, "--max-go-memory (%s) is smaller "+
						"than the recommended minimum (%s), consider increasing it",
					humanizeutil.IBytes(goMemLimit), humanizeutil.IBytes(defaultGoMemLimitMinValue),
				)
			}
		} else {
			if envVarLimitString, envVarSet := envutil.ExternalEnvString("GOMEMLIMIT", 1); envVarSet {
				// When --max-go-memory is not specified, but the env var is
				// set, we don't change it, so we just log a warning if the
				// value is too small.
				envVarLimit, err := humanizeutil.ParseBytes(envVarLimitString)
				if err != nil {
					return errors.Wrapf(err, "couldn't parse GOMEMLIMIT value %s", envVarLimitString)
				}
				if envVarLimit < defaultGoMemLimitMinValue {
					log.Ops.Shoutf(
						ctx, severity.WARNING, "GOMEMLIMIT (%s) is smaller "+
							"than the recommended minimum (%s), consider increasing it",
						humanizeutil.IBytes(envVarLimit), humanizeutil.IBytes(defaultGoMemLimitMinValue),
					)
				}
				return nil
			}
			// If --max-go-memory wasn't specified, we set it to a reasonable
			// default value.
			goMemLimit = getDefaultGoMemLimit(ctx)
		}
		if goMemLimit == 0 {
			// Value of 0 indicates that the soft memory limit should be
			// disabled.
			goMemLimit = math.MaxInt64
		} else {
			log.Ops.Infof(ctx, "soft memory limit of Go runtime is set to %s", humanizeutil.IBytes(goMemLimit))
		}
		debug.SetMemoryLimit(goMemLimit)
		return nil
	}(); err != nil {
		return err
	}

	// Set the GC target percent on the Go runtime.
	if err := func() error {
		var goGCPercent int
		if fs.Changed(cliflags.GoGCPercent.Name) {
			goGCPercent = startCtx.goGCPercent
		} else {
			if _, envVarSet := envutil.ExternalEnvString("GOGC", 1); envVarSet {
				// When --go-gc-percent is not specified but the env var is, we defer to
				// the env var.
				return nil
			}
			// When neither the --go-gc-percent flag nor the GOGC env var is set,
			// increase the GC target percent to 300% (default 100%) to reduce the
			// frequency of GC cycles. However, only do so if a soft memory limit is
			// also configured, to avoid introducing OOMs.
			goMemLimit := debug.SetMemoryLimit(-1 /* get without adjusting */)
			if goMemLimit == math.MaxInt64 {
				// If the soft memory limit is disabled, don't adjust the GC percent.
				// Leave it at the default 100%.
				return nil
			}
			goGCPercent = 300
		}
		var goGCPercentStr redact.RedactableString
		if goGCPercent < 0 {
			goGCPercentStr = `"off"`
		} else {
			goGCPercentStr = redact.Sprintf("%d%%", goGCPercent)
		}
		log.Ops.Infof(ctx, "GC target percentage of Go runtime is set to %s", goGCPercentStr)
		debug.SetGCPercent(goGCPercent)
		return nil
	}(); err != nil {
		return err
	}

	// Initialize the node's configuration from startup parameters.
	// This also reads the part of the configuration that comes from
	// environment variables.
	if err := initConfigFn(ctx); err != nil {
		return errors.Wrapf(err, "failed to initialize %s", serverType)
	}

	// Derive temporary/auxiliary directory specifications.
	serverCfg.ExternalIODir = startCtx.externalIODir

	st := serverCfg.BaseConfig.Settings
	if serverCfg.SQLConfig.TempStorageConfig, err = initTempStorageConfig(
		ctx, st, stopper, serverCfg.Stores,
	); err != nil {
		return err
	}

	// The configuration is now ready to report to the user and the log file.
	// 配置现在已经完全就绪，可以向用户（例如通过启动日志或 --verbosity 输出）和日志文件报告当前的配置内容。

	// We had to wait after InitNode() so that all configuration environment variables,
	// which are reported too, have been read and registered.
	// 之所以必须等到 InitNode() 执行完成后才能报告配置，是因为：
	//   - CockroachDB 的很多配置参数可以通过环境变量（ENV VARIABLES）来覆盖命令行参数。
	//   - 这些环境变量的解析和注册工作是在 InitNode()（或更早的初始化阶段）中完成的。
	//   - 在 InitNode() 之前调用 reportConfiguration()，会导致部分环境变量覆盖的配置项还没有生效，报告出来的配置不准确或不完整。
	//   - 因此，这里特意等到 InitNode() 之后，确保所有配置来源（命令行标志、环境变量、集群设置等）都已经完全加载和应用完毕，再统一打印/记录最终生效的配置。

	// reportConfiguration(ctx)
	// 这个函数会：
	//   - 将所有生效的命令行参数、环境变量覆盖的配置、存储路径、临时存储配置等信息格式化输出。
	//   - 通常出现在节点启动日志的最开头部分，便于用户和运维人员快速了解当前节点到底使用了哪些配置启动。
	//   - 同时也会记录到日志文件中，方便后续排查问题。
	reportConfiguration(ctx)

	// ReadyFn will be called when the server has started listening on
	// its network sockets, but perhaps before it has done bootstrapping
	// and thus before Start() completes.
	// ReadyFn 是一个回调函数（callback），会在服务器开始在网络端口上监听（即网络 socket 已就绪，能接受外部连接）时被调用，
	// 但此时服务器可能还没有完成完整的初始化（bootstrapping），因此 Start() 方法可能还未返回。

	// 具体来说：
	//   - CockroachDB 节点启动时，Start() 方法会启动各种组件（gRPC/HTTP 服务、存储引擎初始化、集群引导等），这个过程可能需要几秒到几十秒。
	//   - 一旦网络监听端口（SQL、RPC、HTTP 等）开启，外部客户端（如负载均衡器、Kubernetes、系统健康检查脚本）就能连接到该端口。
	//   - 但此时节点可能还处于“初始化中”状态，还不能真正处理 SQL 请求或参与集群。

	// 这里设置 ReadyFn 的目的：
	//   - 让外部系统（如 Kubernetes 的 Readiness Probe）能够区分“端口已开放但节点未就绪”和“节点真正就绪”两种状态。
	//   - 通过 reportReadinessExternally(ctx, cmd, waitForInit) 函数来报告节点的就绪状态。
	// DelayedBootstrapFn will be called if the bootstrap process is taking a bit long.
	// DelayedBootstrapFn 是一个延迟回调函数（delayed callback），
	// 如果节点的“集群引导”（bootstrap）过程耗时过长（即尝试加入集群或初始化集群花了较长时间），这个函数就会被触发。

	// 背景解释：
	// CockroachDB 节点启动后，会尝试联系 --join 参数指定的其他节点来加入现有集群。
	// - 如果是新集群的第一节点，它会等待其他节点加入，或等待用户手动执行 cockroach init 来初始化集群。
	// - 如果是后续节点，它会主动尝试连接 --join 列表中的节点。
	// 这个过程如果一直无法完成（例如网络不通、地址配置错、其他节点没启动），就会卡住一段时间。
	// CockroachDB 为了帮助用户快速定位问题，会在等待一段时间后触发这个 DelayedBootstrapFn，打印一条友好的警告信息。

	serverCfg.DelayedBootstrapFn = func() {
		// 定义警告信息内容
		const msg = `The server appears to be unable to contact the other nodes in the cluster. Please try:

- starting the other nodes, if you haven't already;
- double-checking that the '--join' and '--listen'/'--advertise' flags are set up correctly;
- running the 'cockroach init' command if you are trying to initialize a new cluster.

If problems persist, please see %s.`

		// 生成官方文档的故障排查链接
		docLink := docs.URL("cluster-setup-troubleshooting.html")

		// 如果节点不是后台模式启动（即直接在前台运行 cockroach start），使用 Shoutf 高亮警告（会用大写加感叹号输出到 stderr）
		if !startCtx.inBackground {
			log.Ops.Shoutf(ctx, severity.WARNING, msg, docLink)
		} else {
			// 如果是后台模式（--background 或 daemon 方式），进程可能已经 detached（脱离终端），
			// 这时不能再向 stderr Shout（没人看得见），所以改用普通 Warningf，只写到日志文件
			log.Ops.Warningf(ctx, msg, docLink)
		}
	}

	initGEOS(ctx)

	// Beyond this point, the configuration is set and the server is
	// ready to start.

	// 到这里为止，所有配置（命令行参数、环境变量、存储路径、临时存储、加密等）都已经解析、验证并应用完毕。
	// 服务器（CockroachDB node）已经具备启动条件。

	// Run the rest of the startup process in a goroutine separate from
	// the main goroutine to avoid preventing proper handling of signals
	// if we get stuck on something during initialization (#10138).

	// 将后续的启动过程（真正创建并启动服务器）放到一个独立的 goroutine 中执行，而不是在主 goroutine 中直接运行。
	// 原因：防止启动过程中某个步骤卡住（例如网络连接超时、存储初始化缓慢），导致主 goroutine 被阻塞，从而无法及时响应系统信号（SIGINT、SIGTERM 等）。
	// 如果主 goroutine 被阻塞，进程就无法优雅关闭（graceful shutdown），可能导致强制 kill 或数据不一致。
	// 参考 issue：#10138（历史问题，正是因为启动卡住导致信号处理失效）。

	srvStatus, serverShutdownReqC := createAndStartServerAsync(ctx,
		tBegin, &serverCfg, stopper, startupSpan, newServerFn, startSingleNode, serverType)
	return waitForShutdown(
		// NB: we delay the access to s, as it is assigned
		// asynchronously in a goroutine above.
		stopper, serverShutdownReqC, signalCh,
		srvStatus)
}

const (
	// defaultGoMemLimitSQLMultiple determines the multiple of SQL memory pool
	// size that we use in the calculation of the default value of the
	// goMemLimit.
	//
	// Since not every memory allocation is registered with the memory
	// accounting system of CRDB, we need to give it some room to prevent Go GC
	// from being too aggressive to stay under the GOMEMLIMIT. The default
	// multiple of 2.25x over the memory pool size should give enough room for
	// those unaccounted for allocations.
	defaultGoMemLimitSQLMultiple = 2.25

	// Lower bound on the default value for goMemLimit. Lower bound has higher
	// precedence that the upper bound when two bounds conflict with each other.

	// defaultGoMemLimitMinValue determines the lower bound on the default value
	// of the goMemLimit.
	defaultGoMemLimitMinValue = 256 << 20 /* 256MiB */

	// Upper bound on the default value for goMemLimit is computed as follows:
	//
	//   upper bound = 0.9 * SystemMemory - 1.15 * PebbleCache
	//
	// The rationale for this formula is as follows:
	// - we don't want for the estimated max memory usage to exceed 90% of the
	// available RAM to prevent the OOMs
	// - Go runtime doesn't control the pebble cache, so we need to subtract it
	// - anecdotally, the pebble cache can have some slop over its target size
	// (perhaps, due to memory fragmentation), so we adjust the footprint of the
	// cache by 15%.

	// defaultGoMemLimitMaxTotalSystemMemUsage determines the maximum percentage
	// of the system memory that goMemLimit and the pebble cache can use
	// together.
	defaultGoMemLimitMaxTotalSystemMemUsage = 0.9
	// defaultGoMemLimitCacheSlopMultiple determines a "slop" multiple that we
	// use on top of the pebble cache size when computing the upper bound.
	defaultGoMemLimitCacheSlopMultiple = 1.15
)

// getDefaultGoMemLimit returns a reasonable default value for the soft memory
// limit of the Go runtime based on SQL memory pool and the cache sizes (which
// must be already set in serverCfg). It also warns the user in some cases when
// suboptimal flags or hardware is detected.
func getDefaultGoMemLimit(ctx context.Context) int64 {
	sysMem, err := status.GetTotalMemory(ctx)
	if err != nil {
		return 0
	}
	//$$maxGoMemLimit = (总内存 \times 比例) - (缓存大小 \times 冗余倍数)$$
	//在数据库这种内存密集型应用中，如果 CGo 缓存 占了 10G，Go 堆内存 又占了 10G，而总内存只有 16G，系统就会因 OOM (Out of Memory) 杀掉进程。
	//
	//这段代码的作用：它动态计算出一个“天花板”。它告诉 Go 运行时：“嘿，扣掉那些 CGo 占用的固定缓存后，你最多只能用这么多 。
	//快到这个数的时候，请拼命进行垃圾回收（GC），千万别让系统把我杀了！”
	maxGoMemLimit := int64(defaultGoMemLimitMaxTotalSystemMemUsage*float64(sysMem) -
		defaultGoMemLimitCacheSlopMultiple*float64(serverCfg.CacheSize))
	if maxGoMemLimit < defaultGoMemLimitMinValue {
		// Most likely, --cache is set to at least 75% of available RAM which
		// has already triggered a warning in maybeWarnMemorySizes(), so we
		// don't shout here.
		maxGoMemLimit = defaultGoMemLimitMinValue
	}
	limit := int64(defaultGoMemLimitSQLMultiple * float64(serverCfg.MemoryPoolSize))
	if limit < defaultGoMemLimitMinValue {
		log.Ops.Shoutf(
			ctx, severity.WARNING, "--max-sql-memory (%s) is set too low, "+
				"consider increasing it", humanizeutil.IBytes(serverCfg.MemoryPoolSize),
		)
		limit = defaultGoMemLimitMinValue
	}
	if limit > maxGoMemLimit {
		log.Ops.Shoutf(
			ctx, severity.WARNING, "recommended default value of "+
				"--max-go-memory (%s) was truncated to %s, consider reducing "+
				"--max-sql-memory (%s) and / or --cache (%s); total system/cgroup memory: %s.",
			humanizeutil.IBytes(limit), humanizeutil.IBytes(maxGoMemLimit),
			humanizeutil.IBytes(serverCfg.MemoryPoolSize),
			humanizeutil.IBytes(serverCfg.CacheSize),
			humanizeutil.IBytes(sysMem),
		)
		limit = maxGoMemLimit
	}
	return limit
}

// createAndStartServerAsync starts an async goroutine which instantiates
// the server and starts it.
// We run it in a separate goroutine because the instantiation&start
// could block, and we want to retain the option to start shutting down
// the process (e.g. via Ctrl+C on the terminal) even in that case.
// The shutdown logic thus starts running asynchronously, via waitForShutdown,
// concurrently with createAndStartServerAsync.
//
// The arguments are as follows:
//   - tBegin: time when startup began; used to report statistics at the end of startup.
//   - serverCfg: the server configuration.
//   - stopper: the stopper used to start all the async tasks. This is the stopper
//     used by the shutdown logic.
//   - startupSpan: the tracing span for the context that was started earlier
//     during startup. It needs to be finalized when the async goroutine completes.
//   - newServerFn: a constructor function for the server object.
//   - serverType: a title used for the type of server. This is used
//     when reporting the startup messages on the terminal & logs.
//
// // createAndStartServerAsync 函数的作用：在独立的 goroutine 中异步执行 CockroachDB 服务器的创建和启动过程。
// // 这样做的核心目的是：即使启动过程卡住（例如端口被占、join 集群超时、存储初始化慢），
// 主 goroutine 仍能及时响应系统信号（Ctrl+C、SIGTERM），实现优雅关闭。
func createAndStartServerAsync(
	ctx context.Context,
	tBegin time.Time,
	serverCfg *server.Config,
	stopper *stop.Stopper,
	startupSpan *tracing.Span,
	newServerFn newServerFn,
	startSingleNode bool,
	serverType redact.SafeString,
) (srvStatus *serverStatus, serverShutdownReqC <-chan serverctl.ShutdownRequest) {
	var serverStatusMu serverStatus
	var s serverctl.ServerStartupInterface
	shutdownReqC := make(chan serverctl.ShutdownRequest, 1)

	log.Ops.Infof(ctx, "starting cockroach %s", serverType)

	go func() {
		// Ensure that the log files see the startup messages immediately.
		defer log.FlushAllSync()
		// If anything goes dramatically wrong, use Go's panic/recover
		// mechanism to intercept the panic and log the panic details to
		// the error reporting server.
		defer func() {
			var sv *settings.Values
			if s != nil {
				sv = &s.ClusterSettings().SV
			}
			if r := recover(); r != nil {
				// This ensures that the panic, if any, is also reported on stderr.
				// The settings.Values, if available, determines whether a Sentry
				// report should be sent. No Sentry report is sent if sv is nil.
				logcrash.ReportPanic(ctx, sv, r, 1 /* depth */)
				panic(r)
			}
		}()
		// When the start up goroutine completes, so can the start up span.
		defer startupSpan.Finish()

		// Any error beyond this point is reported through shutdownReqC.
		if err := func() error {
			// Instantiate the server.
			var err error
			s, err = newServerFn(ctx, *serverCfg, stopper)
			if err != nil {
				return errors.Wrap(err, "failed to start server")
			}

			// Have we already received a signal to terminate? If so, just
			// stop here.
			if serverStatusMu.shutdownInProgress() {
				return nil
			}

			// Attempt to start the server.
			if err := s.PreStart(ctx); err != nil {
				if le := (*server.ListenError)(nil); errors.As(err, &le) {
					const errorPrefix = "consider changing the port via --%s"
					if le.Addr == serverCfg.Addr {
						err = errors.Wrapf(err, errorPrefix, cliflags.ListenAddr.Name)
					} else if le.Addr == serverCfg.HTTPAddr {
						err = errors.Wrapf(err, errorPrefix, cliflags.ListenHTTPAddr.Name)
					}
				}

				return errors.Wrap(err, "cockroach server exited with error")
			}
			// Server started, notify the shutdown monitor running concurrently.
			if shutdownInProgress := serverStatusMu.setStarted(s, stopper); shutdownInProgress {
				// A shutdown was requested already, e.g. by sending SIGTERM to the process:
				// maybeWaitForShutdown (which runs concurrently with this goroutine) has
				// called serverStatusMu.startShutdown() already.
				// However, because setStarted() had not been called before,
				// maybeWaitForShutdown did not call Stop on the stopper.
				// So we do it here.
				stopper.Stop(ctx)
				return nil
			}
			// After this point, if a shutdown is requested concurrently
			// with the startup steps below, the stopper.Stop() method will
			// be called by the shutdown goroutine, which in turn will cause
			// all these startup steps to fail. So we do not need to look at
			// the "shutdown status" in serverStatusMu any more.

			// Accept internal clients early, as RunInitialSQL might need it.
			if err := s.AcceptInternalClients(ctx); err != nil {
				return err
			}

			// Run one-off cluster initialization.
			if err := s.RunInitialSQL(ctx, startSingleNode, "" /* adminUser */, "" /* adminPassword */); err != nil {
				return err
			}

			// Now let SQL clients in.
			if err := s.AcceptClients(ctx); err != nil {
				return err
			}

			// Now inform the user that the server is running and tell the
			// user about its run-time derived parameters.
			return reportServerInfo(ctx, tBegin, serverCfg, s.ClusterSettings(),
				serverType, s.InitialStart(), s.LogicalClusterID(), startCtx.externalIODir)
		}(); err != nil {
			shutdownReqC <- serverctl.MakeShutdownRequest(
				serverctl.ShutdownReasonServerStartupError, errors.Wrapf(err, "server startup failed"))
		} else {
			// Start a goroutine that watches for shutdown requests and notifies
			// errChan.
			go func() {
				select {
				case req := <-s.ShutdownRequested():
					shutdownCtx := s.AnnotateCtx(context.Background())
					log.Dev.Infof(shutdownCtx, "server requesting spontaneous shutdown: %v", req.ShutdownCause())
					shutdownReqC <- req
				case <-stopper.ShouldQuiesce():
				}
			}()
		}
	}()

	serverShutdownReqC = shutdownReqC
	srvStatus = &serverStatusMu
	return srvStatus, serverShutdownReqC
}

// serverStatus coordinates the async goroutine that starts the server
// up (e.g. in runStart) and the async goroutine that stops the server
// (in waitForShutdown).
//
// We need this intermediate coordination because it isn't safe to try
// to drain a server that doesn't exist or is in the middle of
// starting up, or to start a server after shutdown has begun.
type serverStatus struct {
	syncutil.Mutex
	// s is a reference to the server, to be used by the shutdown process. This
	// starts as nil, and is set by setStarted(). Once set, a graceful shutdown
	// should use a soft drain.
	s serverctl.ServerShutdownInterface
	// stopper is the server's stopper. This is set in setStarted(), together with
	// `s`. The stopper is handed out to callers of startShutdown(), who will
	// Stop() it.
	stopper *stop.Stopper
	// shutdownRequested indicates that shutdown has started
	// already. After draining has become true, server startup should
	// stop.
	shutdownRequested bool
}

// setStarted marks the server as started. The serverStatus receives a reference
// to the server and to the server's stopper. These references will be handed to
// the shutdown process, which calls startShutdown(). In particular, the
// shutdown process will take resposibility for calling stopper.Stop().
//
// setStarted returns whether shutdown has been requested already. If it has,
// then the serverStatus does not take ownership of the stopper; the caller is
// responsible for calling stopper.Stop().
func (s *serverStatus) setStarted(
	server serverctl.ServerShutdownInterface, stopper *stop.Stopper,
) bool {
	s.Lock()
	defer s.Unlock()
	if s.shutdownRequested {
		return true
	}
	s.s = server
	s.stopper = stopper
	return false
}

// shutdownInProgress returns whether a shutdown has been requested
// already.
func (s *serverStatus) shutdownInProgress() bool {
	s.Lock()
	defer s.Unlock()
	return s.shutdownRequested
}

// startShutdown registers the shutdown request and returns whether the server
// was started already. If the server started, a reference to the server is also
// returned, and a reference to the stopper that the caller needs to eventually
// Stop().
func (s *serverStatus) startShutdown() (bool, serverctl.ServerShutdownInterface, *stop.Stopper) {
	s.Lock()
	defer s.Unlock()
	s.shutdownRequested = true
	return s.s != nil, s.s, s.stopper
}

// waitForShutdown blocks until interrupted by a shutdown signal, which can come
// in several forms:
//   - a shutdown request coming from an internal module being signaled on
//     shutdownC. This can be some internal error or a drain RPC.
//   - receiving a Unix signal on signalCh.
//   - a log.Fatal() call.
//
// Depending on what interruption is received, the server might be drained
// before shutting down.
func waitForShutdown(
	stopper *stop.Stopper,
	shutdownC <-chan serverctl.ShutdownRequest,
	signalCh <-chan os.Signal,
	serverStatusMu *serverStatus,
) (returnErr error) {
	shutdownCtx, shutdownSpan := serverCfg.AmbientCtx.AnnotateCtxWithSpan(context.Background(), "server shutdown")
	defer shutdownSpan.Finish()

	stopWithoutDrain := make(chan struct{}) // closed if interrupted very early
	msgDrain := redact.SafeString("initiating graceful shutdown of server")

	select {
	case shutdownRequest := <-shutdownC:
		returnErr = shutdownRequest.ShutdownCause()
		drain := shutdownRequest.TerminateUsingGracefulDrain()
		if !drain {
			msgDrain = "initiating hard shutdown of server"
		}
		startShutdownAsync(serverStatusMu, stopWithoutDrain, drain)

	case sig := <-signalCh:
		// We start flushing log writes from here, because if a
		// signal was received there is a non-zero chance the sender of
		// this signal will follow up with SIGKILL if the shutdown is not
		// timely, and we don't want logs to be lost.
		log.StartAlwaysFlush()

		if sig == exitAbruptlySignal {
			log.Ops.Shoutf(shutdownCtx, severity.ERROR, "received signal '%s', exiting", redact.Safe(sig))
			exit.WithCode(exit.Killed())
		}

		log.Ops.Infof(shutdownCtx, "received signal '%s'", sig)
		if sig == os.Interrupt {
			// Graceful shutdown after an interrupt should cause the process
			// to terminate with a non-zero exit code; however SIGTERM is
			// "legitimate" and should be acknowledged with a success exit
			// code. So we keep the error state here for later.
			returnErr = clierror.NewErrorWithSeverity(
				errors.New("interrupted"),
				exit.Interrupted(),
				// INFO because a single interrupt is rather innocuous.
				severity.INFO,
			)
			if !startCtx.inBackground {
				msgDouble := "Note: a second interrupt will skip graceful shutdown and terminate forcefully"
				fmt.Fprintln(os.Stdout, msgDouble)
			}
		}

		startShutdownAsync(serverStatusMu, stopWithoutDrain, true /* shouldDrain */)
	// Don't return: we're shutting down gracefully.

	case <-log.FatalChan():
		// A fatal error has occurred. Stop everything (gracelessly) to
		// avoid serving incorrect data while the final log messages are
		// being written.
		// https://github.com/cockroachdb/cockroach/issues/23414
		// TODO(bdarnell): This could be more graceless, for example by
		// reaching into the server objects and closing all the
		// connections while they're in use. That would be more in line
		// with the expected effect of a log.Fatal.
		stopper.Stop(shutdownCtx)
		// The logging goroutine is now responsible for killing this
		// process, so just block this goroutine.
		select {}
	}

	// At this point, a signal has been received to shut down the
	// process, and a goroutine is busy telling the server to drain and
	// stop. From this point on, we just have to wait until the server
	// indicates it has stopped.

	log.Ops.Infof(shutdownCtx, "%s", msgDrain)
	if !startCtx.inBackground {
		fmt.Fprintln(os.Stdout, msgDrain)
	}

	// Notify the user every 5 second of the shutdown progress.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Ops.Infof(shutdownCtx, "%d running tasks", stopper.NumTasks())
			case <-stopper.IsStopped():
				return
			case <-stopWithoutDrain:
				return
			}
		}
	}()

	// Meanwhile, we don't want to wait too long either, in case the
	// server is getting stuck and doesn't shut down in a timely manner.
	//
	// So we also pay attention to any additional signal received beyond
	// this point (maybe some service monitor was impatient and sends
	// another signal to hasten the shutdown process).
	//
	// If any such trigger to hasten occurs, we simply return, which
	// will cause the process to exit and the server goroutines to be
	// forcefully terminated.

	const hardShutdownHint = " - node may take longer to restart & clients may need to wait for leases to expire"
	for {
		select {
		case sig := <-signalCh:
			switch sig {
			case termSignal:
				// Double SIGTERM, or SIGTERM after another signal: continue
				// the graceful shutdown.
				log.Ops.Infof(shutdownCtx, "received additional signal '%s'; continuing graceful shutdown", sig)
				continue
			}

			// This new signal is not welcome, as it interferes with the graceful
			// shutdown process.
			log.Ops.Shoutf(shutdownCtx, severity.ERROR,
				"received signal '%s' during shutdown, initiating hard shutdown%s",
				redact.Safe(sig), redact.Safe(hardShutdownHint))
			handleSignalDuringShutdown(sig)
			panic("unreachable")

		case shutdownRequest := <-shutdownC:
			if shutdownRequest.TerminateUsingGracefulDrain() {
				// Internal graceful drain request during graceful shutdown:
				// continue the graceful shutdown.
				log.Ops.Infof(shutdownCtx, "received additional shutdown request '%s'; continuing graceful shutdown", shutdownRequest.ShutdownCause())
				continue
			} else {
				// A non-graceful shutdown request: we're done.
				const msgDone = "internal error during drain; used hard shutdown instead"
				returnErr = shutdownRequest.ShutdownCause()
				log.Ops.Infof(shutdownCtx, "received additional shutdown request '%s'; initiating hard shutdown%s",
					shutdownRequest.ShutdownCause(), redact.Safe(hardShutdownHint))
				if !startCtx.inBackground {
					fmt.Fprintln(os.Stdout, msgDone)
				}
			}

		case <-stopper.IsStopped():
			const msgDone = "server drained and shutdown completed"
			log.Ops.Infof(shutdownCtx, msgDone)
			if !startCtx.inBackground {
				fmt.Fprintln(os.Stdout, msgDone)
			}

		case <-stopWithoutDrain:
			const msgDone = "too early to drain; used hard shutdown instead"
			log.Ops.Infof(shutdownCtx, msgDone)
			if !startCtx.inBackground {
				fmt.Fprintln(os.Stdout, msgDone)
			}
		}
		break
	}

	return returnErr
}

// startShutdown begins the process that stops the server, asynchronously.
//
// The shouldDrain argument indicates that the shutdown is happening
// some time after server startup has completed, and we are thus
// interested in being graceful to application load.
func startShutdownAsync(
	serverStatusMu *serverStatus, stopWithoutDrain chan<- struct{}, shouldDrain bool,
) {
	// StartAlwaysFlush both flushes and ensures that subsequent log
	// writes are flushed too.
	log.StartAlwaysFlush()

	// Start the draining process in a separate goroutine so that it
	// runs concurrently with the timeout check in waitForShutdown().
	go func() {
		// The return value of startShutdown indicates whether the
		// server has started already, and the graceful shutdown should
		// call the Drain method. We cannot call Drain if the server has
		// not started yet.
		canUseDrain, s, stopper := serverStatusMu.startShutdown()

		if !canUseDrain {
			// The server has not started yet. We can't use the Drain() call.
			close(stopWithoutDrain)
			return
		}

		// Don't use ctx because this is in a goroutine that may
		// still be running after shutdownCtx's span has been finished.
		drainCtx := logtags.AddTag(s.AnnotateCtx(context.Background()), "server drain process", nil)

		if shouldDrain {
			// Perform a graceful drain. This function keeps retrying and
			// the call might never complete (e.g. due to some
			// unavailability preventing progress). This is intentional. If
			// the operator wants to expedite the shutdown, they will need
			// to make it ungraceful by sending a second signal to the
			// process, which will tickle the shortcut in waitForShutdown().
			server.CallDrainServerSide(drainCtx, s.Drain)
		}

		stopper.Stop(drainCtx)
	}()
}

// cgoAllocConfStr is populated from start_jemalloc.go if we are using jemalloc.
// Otherwise, it stays empty.
var cgoAllocConfStr string

// reportServerInfo prints out the server version and network details
// in a standardized format.
func reportServerInfo(
	ctx context.Context,
	startTime time.Time,
	serverCfg *server.Config,
	st *cluster.Settings,
	serverType redact.SafeString,
	initialStart bool,
	tenantClusterID uuid.UUID,
	externalIODir string,
) error {
	var buf redact.StringBuilder
	info := build.GetInfo()
	buf.Printf("CockroachDB %s starting at %s (took %0.1fs)\n", serverType, timeutil.Now(), timeutil.Since(startTime).Seconds())
	buf.Printf("build:\t%s %s @ %s (%s)\n",
		redact.Safe(info.Distribution), redact.Safe(info.Tag), redact.Safe(info.Time), redact.Safe(info.GoVersion))
	if cgoAllocConfStr != "" {
		buf.Printf("malloc_conf:\t%s\n", redact.Safe(cgoAllocConfStr))
	}
	buf.Printf("webui:\t%s\n", log.SafeManaged(serverCfg.AdminURL()))

	// (Re-)compute the client connection URL. We cannot do this
	// earlier (e.g. above, in the runStart function) because
	// at this time the address and port have not been resolved yet.
	clientConnOptions, serverParams := server.MakeServerOptionsForURL(serverCfg.Config)
	pgURL, err := clientsecopts.MakeURLForServer(clientConnOptions, serverParams, url.User(username.RootUser))
	if err != nil {
		log.Ops.Errorf(ctx, "failed computing the URL: %v", err)
		return err
	}
	buf.Printf("sql:\t%s\n", log.SafeManaged(pgURL.ToPQ()))
	buf.Printf("sql (JDBC):\t%s\n", log.SafeManaged(pgURL.ToJDBC()))

	buf.Printf("RPC client flags:\t%s\n", log.SafeManaged(clientFlagsRPC()))
	if len(serverCfg.SocketFile) != 0 {
		buf.Printf("socket:\t%s\n", log.SafeManaged(serverCfg.SocketFile))
	}
	logNum := 1
	_ = cliCtx.logConfig.IterateDirectories(func(d string) error {
		if logNum == 1 {
			// Backward-compatibility.
			buf.Printf("logs:\t%s\n", log.SafeManaged(d))
		} else {
			buf.Printf("logs[%d]:\t%s\n", log.SafeManaged(logNum), log.SafeManaged(d))
		}
		logNum++
		return nil
	})
	if serverCfg.Attrs != "" {
		buf.Printf("attrs:\t%s\n", log.SafeManaged(serverCfg.Attrs))
	}
	if len(serverCfg.Locality.Tiers) > 0 {
		buf.Printf("locality:\t%s\n", log.SafeManaged(serverCfg.Locality))
	}
	if tmpDir := serverCfg.SQLConfig.TempStorageConfig.Path; tmpDir != "" {
		buf.Printf("temp dir:\t%s\n", log.SafeManaged(tmpDir))
	}
	if externalIODir != "" {
		buf.Printf("external I/O path: \t%s\n", log.SafeManaged(externalIODir))
	} else {
		buf.Printf("external I/O path: \t<disabled>\n")
	}
	for i, spec := range serverCfg.Stores.Specs {
		buf.Printf("store[%d]:\t%s\n", i, log.SafeManaged(base.StoreSpecCmdLineString(spec)))
	}

	// Print the commong server identifiers.
	if baseCfg.ClusterName != "" {
		buf.Printf("cluster name:\t%s\n", log.SafeManaged(baseCfg.ClusterName))
	}
	clusterID := serverCfg.BaseConfig.ClusterIDContainer.Get()
	if tenantClusterID.Equal(clusterID) {
		buf.Printf("clusterID:\t%s\n", log.SafeManaged(clusterID))
	} else {
		buf.Printf("storage clusterID:\t%s\n", log.SafeManaged(clusterID))
		buf.Printf("tenant clusterID:\t%s\n", log.SafeManaged(tenantClusterID))
	}
	nodeID := serverCfg.BaseConfig.IDContainer.Get()
	if serverCfg.SQLConfig.TenantID.IsSystem() {
		if initialStart {
			if nodeID == kvstorage.FirstNodeID {
				buf.Printf("status:\tinitialized new cluster\n")
			} else {
				buf.Printf("status:\tinitialized new node, joined pre-existing cluster\n")
			}
		} else {
			buf.Printf("status:\trestarted pre-existing node\n")
		}
		// Report the server identifiers.
		buf.Printf("nodeID:\t%d\n", nodeID)
	} else {
		// Report the SQL server identifiers.
		buf.Printf("tenantID:\t%s\n", serverCfg.SQLConfig.TenantID)
		buf.Printf("instanceID:\t%d\n", nodeID)

		if kvAddrs := serverCfg.SQLConfig.TenantKVAddrs; len(kvAddrs) > 0 {
			// Report which KV servers are connected.
			buf.Printf("KV addresses:\t")
			comma := redact.SafeString("")
			for _, addr := range serverCfg.SQLConfig.TenantKVAddrs {
				buf.Printf("%s%s", comma, log.SafeManaged(addr))
				comma = ", "
			}
			buf.Printf("\n")
		}
	}

	// Collect the formatted string and show it to the user.
	msg, err := util.ExpandTabsInRedactableBytes(buf.RedactableBytes())
	if err != nil {
		return err
	}
	msgS := msg.ToString()
	log.Ops.Infof(ctx, "%s startup completed:\n%s", serverType, msgS)
	if !startCtx.inBackground && !log.LoggingToStderr(severity.INFO) {
		fmt.Print(msgS.StripMarkers())
	}

	return nil
}

func hintServerCmdFlags(ctx context.Context, cmd *cobra.Command) {
	pf := cliflagcfg.FlagSetForCmd(cmd)

	sqlAddrSpecified := pf.Lookup(cliflags.ListenSQLAddr.Name).Changed
	if !sqlAddrSpecified {
		log.Ops.Shoutf(ctx, severity.WARNING,
			"Running a server without --sql-addr, with a combined RPC/SQL listener, is deprecated.\n"+
				"This feature will be removed in a later version of CockroachDB.")
	}

	changed := func(flagName string) bool {
		fl := pf.Lookup(flagName)
		return fl != nil && fl.Changed
	}

	listenAddrSpecified := changed(cliflags.ListenAddr.Name) || changed(cliflags.ServerHost.Name)
	advAddrSpecified := changed(cliflags.AdvertiseAddr.Name) || changed(cliflags.AdvertiseHost.Name)
	if !listenAddrSpecified && !advAddrSpecified {
		host, _, _ := net.SplitHostPort(serverCfg.AdvertiseAddr)
		log.Ops.Shoutf(ctx, severity.WARNING,
			"neither --listen-addr nor --advertise-addr was specified.\n"+
				"The server will advertise %q to other nodes, is this routable?\n\n"+
				"Consider using:\n"+
				"- for local-only servers:  --listen-addr=localhost:36257 --sql-addr=localhost:26257\n"+
				"- for multi-node clusters: --listen-addr=:36257 --sql-addr=:26257 --advertise-addr=<host/IP addr>\n", host)
	}
}

func clientFlagsRPC() string {
	flags := []string{os.Args[0], "<client cmd>"}
	if serverCfg.AdvertiseAddr != "" {
		flags = append(flags, "--host="+serverCfg.AdvertiseAddr)
	}
	if startCtx.serverInsecure {
		flags = append(flags, "--insecure")
	} else {
		flags = append(flags, "--certs-dir="+startCtx.serverSSLCertsDir)
	}
	return strings.Join(flags, " ")
}

func reportConfiguration(ctx context.Context) {
	serverCfg.Report(ctx)
	if envVarsUsed := envutil.GetEnvVarsUsed(); len(envVarsUsed) > 0 {
		log.Ops.Infof(ctx, "using local environment variables:\n%s", redact.Join("\n", envVarsUsed))
	}
	// If a user ever reports "bad things have happened", any
	// troubleshooting steps will want to rule out that the user was
	// running as root in a multi-user environment, or using different
	// uid/gid across runs in the same data directory. To determine
	// this, it's easier if the information appears in the log file.
	log.Ops.Infof(ctx, "process identity: %s", log.SafeManaged(sysutil.ProcessIdentity()))
}

func maybeWarnMemorySizes(ctx context.Context) {
	// Is the cache configuration OK?
	if !startCtx.cacheSizeValue.IsSet() {
		var buf redact.StringBuilder
		buf.Printf("Using the default setting for --cache (%s).\n", &startCtx.cacheSizeValue)
		buf.Printf("  A significantly larger value is usually needed for good performance.\n")
		if size, err := status.GetTotalMemory(ctx); err == nil {
			buf.Printf("  If you have a dedicated server a reasonable setting is --cache=.25 (%s).",
				humanizeutil.IBytes(size/4))
		} else {
			buf.Printf("  If you have a dedicated server a reasonable setting is 25%% of physical memory.")
		}
		log.Ops.Warningf(ctx, "%s", buf.RedactableString())
	}

	// Check that the total suggested "max" memory is well below the available memory.
	if maxMemory, err := status.GetTotalMemory(ctx); err == nil {
		requestedMem := serverCfg.CacheSize + serverCfg.MemoryPoolSize + serverCfg.TimeSeriesServerConfig.QueryMemoryMax
		// TODO(yuzefovich): we might want to adjust this warning higher now
		// that GOMEMLIMIT is used.
		maxRecommendedMem := int64(.75 * float64(maxMemory))
		if requestedMem > maxRecommendedMem {
			log.Ops.Shoutf(ctx, severity.WARNING,
				"the sum of --max-sql-memory (%s), --cache (%s), and --max-tsdb-memory (%s) is larger than 75%% of total RAM (%s).\nThis server is running at increased risk of memory-related failures.",
				&startCtx.sqlSizeValue, &startCtx.cacheSizeValue, &startCtx.tsdbSizeValue,
				humanizeutil.IBytes(maxRecommendedMem))
		}
	}
}

func exitIfDiskFull(ctx context.Context, fs vfs.FS, specs []base.StoreSpec) error {
	// First try to reclaim disk space by cleaning up obsolete files. It's
	// possible this will free up enough space to allow us to start when we
	// otherwise would not be able to.
	//首先尝试通过清理陈旧（废弃）的文件来**回收（reclaim）**磁盘空间。
	if err := reclaimDiskSpace(ctx, fs, specs); err != nil {
		return err
	}

	var cause error
	var ballastPaths []string
	var ballastMissing bool
	for _, spec := range specs {
		isDiskFull, err := storage.IsDiskFull(fs, spec)
		if err != nil {
			return err
		}
		if !isDiskFull {
			continue
		}
		path := base.EmergencyBallastFile(fs.PathJoin, spec.Path)
		ballastPaths = append(ballastPaths, path)
		if _, err := fs.Stat(path); oserror.IsNotExist(err) {
			ballastMissing = true
		}
		cause = errors.CombineErrors(cause, errors.Newf(`store %s: out of disk space`, spec.Path))
	}
	if cause == nil {
		return nil
	}

	// TODO(jackson): Link to documentation surrounding the ballast.

	err := clierror.NewError(cause, exit.DiskFull())
	if ballastMissing {
		return errors.WithHint(err, `At least one ballast file is missing.
You may need to replace this node because there is
insufficient disk space to start.`)
	}

	ballastPathsStr := strings.Join(ballastPaths, "\n")
	err = errors.WithHintf(err, `Deleting or truncating the ballast file(s) at
%s
may reclaim enough space to start. Proceed with caution. Complete
disk space exhaustion may result in node loss.`, ballastPathsStr)
	return err
}

// reclaimDiskSpace attempts to reclaim disk space by cleaning up obsolete files
// leftover from the previous process. For example, SQL temporary directories
// become obsolete when a process exits and can be reclaimed.
func reclaimDiskSpace(ctx context.Context, rootFS vfs.FS, specs []base.StoreSpec) error {
	var err error
	// Reclaim any temporary directories.
	for _, spec := range specs {
		if spec.InMemory {
			continue
		}
		recordPath := filepath.Join(spec.Path, server.TempDirsRecordFilename)
		err = errors.CombineErrors(err, fs.CleanupTempDirs(ctx, rootFS, recordPath))
	}
	return err
}

// setupAndInitializeLoggingAndProfiling does what it says on the label.
// Prior to this however it determines suitable defaults for the
// logging output directory and the verbosity level of stderr logging.
// We only do this for special commands which do not use default logging like
// "start" and "start-sql" commands which is why this work occurs here and not
// in an OnInitialize function.
func setupAndInitializeLoggingAndProfiling(
	ctx context.Context, cmd *cobra.Command, isServerCmd bool,
) (stopper *stop.Stopper, err error) {
	if err := setupLogging(ctx, cmd, isServerCmd, true /* applyConfig */); err != nil {
		return nil, err
	}

	if startCtx.serverInsecure {
		// Use a non-annotated context here since the annotation just looks funny,
		// particularly to new users (made worse by it always printing as [n?]).
		addr := startCtx.serverListenAddr
		if addr == "" {
			addr = "any of your IP addresses"
		}
		log.Ops.Shoutf(ctx, severity.WARNING,
			"ALL SECURITY CONTROLS HAVE BEEN DISABLED!\n\n"+
				"This mode is intended for non-production testing only.\n"+
				"\n"+
				"In this mode:\n"+
				"- Your cluster is open to any client that can access %s.\n"+
				"- Intruders with access to your machine or network can observe client-server traffic.\n"+
				"- Intruders can log in without password and read or write any data in the cluster.\n"+
				"- Intruders can consume all your server's resources and cause unavailability.",
			log.SafeManaged(addr))
		log.Ops.Shoutf(ctx, severity.INFO,
			"To start a secure server without mandating TLS for clients,\n"+
				"consider --accept-sql-without-tls instead. For other options, see:\n\n"+
				"- %s\n"+
				"- %s",
			redact.SafeString(build.MakeIssueURL(53404)),
			redact.SafeString(docs.URL("secure-a-cluster.html")),
		)
	}

	// The new multi-region abstractions require that nodes be labeled in "regions"
	// (and optionally, "zones").  To push users in that direction, we warn them here
	// if they've provided a --locality value which doesn't include a "region" tier.
	if len(serverCfg.Locality.Tiers) > 0 {
		if _, containsRegion := serverCfg.Locality.Find("region"); !containsRegion {
			const warningString string = "The --locality flag does not contain a \"region\" tier. To add regions\n" +
				"to databases, the --locality flag must contain a \"region\" tier.\n" +
				"For more information, see:\n\n" +
				"- %s"
			log.Dev.Shoutf(ctx, severity.WARNING, warningString,
				redact.Safe(docs.URL("cockroach-start.html#locality")))
		}
	}

	maybeWarnMemorySizes(ctx)

	// We log build information to stdout (for the short summary), but also
	// to stderr to coincide with the full logs.
	info := build.GetInfo()
	log.Ops.Infof(ctx, "%s", info.Short())

	// Disable Stopper task tracking as performing that call site tracking is
	// moderately expensive (certainly outweighing the infrequent benefit it
	// provides).
	stopper = stop.NewStopper()
	initTraceDir(ctx, serverCfg.InflightTraceDirName)
	initBlockProfile()
	initMutexProfile()

	var gceContinuousProfilerEnabled = envutil.EnvOrDefaultBool("COCKROACH_GOOGLE_CONTINUOUS_PROFILER_ENABLED", false)
	if gceContinuousProfilerEnabled {
		profiler.InitGoogleProfiler(ctx)
	}

	log.Event(ctx, "initialized profiles")

	return stopper, nil
}

// initGEOS sets up the Geospatial library.
// initGEOS 函数负责初始化 CockroachDB 内置的地理空间（Geospatial）库。
// 地理空间功能依赖第三方库 GEOS（Geometry Engine - Open Source），它提供几何运算（如 ST_Contains、ST_Intersects、ST_DWithin 等）的底层实现。

// We need to make sure this happens before any queries involving geospatial data is executed.
// 必须确保这个初始化在任何涉及地理空间数据的 SQL 查询执行之前完成。
// 因为一旦有查询调用空间函数（如 ST_AsText、ST_Distance 等），如果 GEOS 还没加载成功，这些函数会直接不可用或报错。

func initGEOS(ctx context.Context) {
	// geos.EnsureInit 是 CockroachDB 对 GEOS 库的封装初始化函数
	// 参数：
	//   - geos.EnsureInitErrorDisplayPrivate：错误处理策略，表示如果初始化失败，只记录警告日志，不对外暴露敏感路径细节
	//   - startCtx.geoLibsDir：用户通过 --geo-libs-dir 参数指定的外部 GEOS 库目录（可选）
	//     如果指定了这个目录，CockroachDB 会优先从这里加载 libgeos.so / libgeos_c.so
	//     如果没指定，则使用内置的捆绑版本（bundled GEOS）
	loc, err := geos.EnsureInit(geos.EnsureInitErrorDisplayPrivate, startCtx.geoLibsDir)

	if err != nil {
		// 初始化失败：记录警告日志
		// 空间函数将不可用，但节点仍能正常启动（地理空间功能是可选特性）
		log.Ops.Warningf(ctx,
			"could not initialize GEOS - spatial functions may not be available: %v",
			log.SafeManaged(err)) // SafeManaged 防止错误信息泄露敏感路径
	} else {
		// 初始化成功：记录信息日志，显示实际加载的 GEOS 库路径
		log.Ops.Infof(ctx, "GEOS loaded from directory %s", log.SafeManaged(loc))
	}
}

// reportReadinessExternally reports when the server has finished initializing
// and is ready to receive requests. This is useful for other processes on the
// same machine (e.g. a process manager, a test) that are waiting for a signal
// that they can start monitoring or using the server process.
func reportReadinessExternally(ctx context.Context, cmd *cobra.Command, waitForInit bool) {
	// Inform the user if the network settings are suspicious. We need
	// to do that after starting to listen because we need to know
	// which advertise address NewServer() has decided.
	hintServerCmdFlags(ctx, cmd)

	// If another process was waiting on the PID (e.g. using a FIFO),
	// this is when we can tell them the node has started listening.
	if startCtx.pidFile != "" {
		log.Ops.Infof(ctx, "PID file: %s", startCtx.pidFile)
		if err := os.WriteFile(startCtx.pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644); err != nil {
			log.Ops.Errorf(ctx, "failed writing the PID: %v", err)
		}
	}

	// If the invoker has requested an URL update, do it now that
	// the server is ready to accept SQL connections.
	// (Note: as stated above, ReadyFn is called after the server
	// has started listening on its socket, but possibly before
	// the cluster has been initialized and can start processing requests.
	// This is OK for SQL clients, as the connection will be accepted
	// by the network listener and will just wait/suspend until
	// the cluster initializes, at which point it will be picked up
	// and let the client go through, transparently.)
	if startCtx.listeningURLFile != "" {
		log.Ops.Infof(ctx, "listening URL file: %s", startCtx.listeningURLFile)
		// (Re-)compute the client connection URL. We cannot do this
		// earlier (e.g. above, in the runStart function) because
		// at this time the address and port have not been resolved yet.
		clientConnOptions, serverParams := server.MakeServerOptionsForURL(serverCfg.Config)
		pgURL, err := clientsecopts.MakeURLForServer(clientConnOptions, serverParams, url.User(username.RootUser))
		if err != nil {
			log.Dev.Errorf(ctx, "failed computing the URL: %v", err)
			return
		}

		if err = os.WriteFile(startCtx.listeningURLFile, []byte(fmt.Sprintf("%s\n", pgURL.ToPQ())), 0644); err != nil {
			log.Ops.Errorf(ctx, "failed writing the URL: %v", err)
		}
	}

	if waitForInit {
		log.Ops.Shout(ctx, severity.INFO,
			"initial startup completed.\n"+
				"Node will now attempt to join a running cluster, or wait for `cockroach init`.\n"+
				"Client connections will be accepted after this completes successfully.\n"+
				"Check the log file(s) for progress. ")
	}

	// Try to signal readiness. This unblocks the process when running under
	// systemd. Otherwise, it will be a no-op.
	if err := sdnotify.Ready(func() {
		// Ensure the configuration logging is written to disk in case a
		// process is waiting for the sdnotify readiness to read important
		// information from there. Given that this blocks and is only used for
		// the sdnotify readiness, we will only run it conditionally.
		log.FlushAllSync()
	}); err != nil {
		log.Ops.Errorf(ctx, "failed to signal readiness using systemd protocol: %s", err)
	}
}
