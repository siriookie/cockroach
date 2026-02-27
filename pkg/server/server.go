// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/inspectz"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvissubscriber"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/spanstatskvaccessor"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangestats"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvprober"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/policyrefresher"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/sidetransport"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/node_rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/mmaintegration"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptprovider"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	serverrangefeed "github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangelog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/reports"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesauthorizer"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitieswatcher"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiinternal"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/privchecker"
	"github.com/cockroachdb/cockroach/pkg/server/serverctl"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverrules"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/structlogging"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/server/tenantsettingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	_ "github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigjob" // register jobs declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvsubscriber"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreporter"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/sql"
	_ "github.com/cockroachdb/cockroach/pkg/sql/bulkingest" // register bulk ingest implementation
	_ "github.com/cockroachdb/cockroach/pkg/sql/bulkmerge"  // register bulk merge implementation
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	_ "github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry" // register schedules declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	_ "github.com/cockroachdb/cockroach/pkg/sql/gcjob"    // register jobs declared outside of pkg/sql
	_ "github.com/cockroachdb/cockroach/pkg/sql/importer" // register jobs/planHooks declared outside of pkg/sql
	_ "github.com/cockroachdb/cockroach/pkg/sql/inspect"  // register job and planHook declared outside of pkg/sql
	_ "github.com/cockroachdb/cockroach/pkg/sql/isession" // register isession constructor hook
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	_ "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scjob" // register jobs declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionprotectedts"
	_ "github.com/cockroachdb/cockroach/pkg/sql/ttl/ttljob"      // register jobs declared outside of pkg/sql
	_ "github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlschedule" // register schedules declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc/logger"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventlog"
	"github.com/cockroachdb/cockroach/pkg/util/log/logmetrics"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/schedulerlatency"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/ptp"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/getsentry/sentry-go"
	"google.golang.org/grpc/codes"
)

// topLevelServer is the cockroach server node.
type topLevelServer struct {
	// The following fields are populated in NewServer.

	nodeIDContainer *base.NodeIDContainer
	cfg             Config
	st              *cluster.Settings
	clock           *hlc.Clock
	rpcContext      *rpc.Context
	engines         Engines

	// The gRPC and DRPC servers on which the different RPC handlers will be
	// registered.
	grpc *grpcServer
	drpc *drpcServer

	gossip       *gossip.Gossip
	kvNodeDialer *nodedialer.Dialer
	nodeLiveness *liveness.NodeLiveness
	storePool    *storepool.StorePool
	tcsFactory   *kvcoord.TxnCoordSenderFactory
	distSender   *kvcoord.DistSender
	db           *kv.DB
	node         *Node

	// Metric registries. See their definition in NewServer for details.
	nodeRegistry *metric.Registry
	appRegistry  *metric.Registry
	sysRegistry  *metric.Registry

	recorder             *status.MetricsRecorder
	runtime              *status.RuntimeStatSampler
	ruleRegistry         *metric.RuleRegistry
	promRuleExporter     *metric.PrometheusRuleExporter
	updates              *diagnostics.UpdateChecker
	ctSender             *sidetransport.Sender
	policyRefresher      *policyrefresher.PolicyRefresher
	nodeCapacityProvider *load.NodeCapacityProvider

	http            *httpServer
	adminAuthzCheck privchecker.CheckerForRPCHandlers
	admin           *systemAdminServer
	status          *systemStatusServer
	drain           *drainServer
	decomNodeMap    *decommissioningNodeMap
	authentication  authserver.Server
	migrationServer *migrationServer
	tsDB            *ts.DB
	tsServer        *ts.Server

	// keyVisualizerServer implements `keyvispb.KeyVisualizerServer`
	keyVisualizerServer *KeyVisualizerServer

	recoveryServer *loqrecovery.Server
	raftTransport  *kvserver.RaftTransport
	storeLiveness  *storeliveness.NodeContainer
	stopper        *stop.Stopper
	stopTrigger    *stopTrigger

	debug          *debug.Server
	kvProber       *kvprober.Prober
	inspectzServer *inspectz.Server

	replicationReporter *reports.Reporter
	protectedtsProvider protectedts.Provider

	spanConfigSubscriber spanconfig.KVSubscriber
	spanConfigReporter   spanconfig.Reporter

	tenantCapabilitiesWatcher *tenantcapabilitieswatcher.Watcher

	// pgL is the SQL listener for pgwire connections coming over the network.
	pgL net.Listener
	// loopbackPgL is the SQL listener for internal pgwire connections.
	loopbackPgL *netutil.LoopbackListener

	// pgPreServer handles SQL connections prior to routing them to a
	// specific tenant.
	pgPreServer *pgwire.PreServeConnHandler

	// TODO(knz): pull this down under the serverController.
	sqlServer *SQLServer

	// serverController is responsible for on-demand instantiation
	// of services.
	serverController *serverController

	// Created in NewServer but initialized (made usable) in `(*Server).PreStart`.
	externalStorageBuilder *externalStorageBuilder

	storeGrantCoords *admission.StoreGrantCoordinators
	// kvMemoryMonitor is a child of the rootSQLMemoryMonitor and is used to
	// account for and bound the memory used for request processing in the KV
	// layer.
	kvMemoryMonitor *mon.BytesMonitor

	// The following fields are populated at start time, i.e. in `(*Server).Start`.
	startTime time.Time
}

// NewServer creates a Server from a server.Config.
//
// The caller is responsible for listening on the server's ShutdownRequested()
// channel and calling stopper.Stop().
func NewServer(cfg Config, stopper *stop.Stopper) (serverctl.ServerStartupInterface, error) {
	ctx := cfg.AmbientCtx.AnnotateCtx(context.Background())

	if err := cfg.ValidateAddrs(ctx); err != nil {
		return nil, err
	}

	st := cfg.Settings

	// Ensure that we don't mistakenly reuse the same Values container
	// across servers (e.g. misuse of TestServer API).
	st.SV.SpecializeForSystemInterface()

	if cfg.AmbientCtx.Tracer == nil {
		panic(errors.New("no tracer set in AmbientCtx"))
	}

	clock, err := newClockFromConfig(ctx, cfg.BaseConfig)
	if err != nil {
		return nil, err
	}

	// nodeRegistry holds metrics that are specific to the storage and KV layer.
	// Do not use this for metrics that could possibly be reported by secondary
	// tenants, i.e. those also registered in tenant.go.
	nodeRegistry := metric.NewRegistry()
	// appRegistry holds application-level metrics. These are the metrics
	// that are also registered in tenant.go anew for each tenant.
	appRegistry := metric.NewRegistry()
	// sysRegistry holds process-level metrics. These are metrics
	// that are collected once per process and are not specific to
	// any particular tenant.
	sysRegistry := metric.NewRegistry()

	ruleRegistry := metric.NewRuleRegistry()
	promRuleExporter := metric.NewPrometheusRuleExporter(ruleRegistry)
	stopper.SetTracer(cfg.AmbientCtx.Tracer)
	stopper.AddCloser(cfg.AmbientCtx.Tracer)

	// Add a dynamic log tag value for the node ID.
	//
	// We need to pass an ambient context to the various server components, but we
	// won't know the node ID until we Start(). At that point it's too late to
	// change the ambient contexts in the components (various background processes
	// will have already started using them).
	//
	// NodeIDContainer allows us to add the log tag to the context now and update
	// the value asynchronously. It's not significantly more expensive than a
	// regular tag since it's just doing an (atomic) load when a log/trace message
	// is constructed. The node ID is set by the Store if this host was
	// bootstrapped; otherwise a new one is allocated in Node.
	nodeIDContainer := cfg.IDContainer
	idContainer := base.NewSQLIDContainerForNode(nodeIDContainer)

	admissionOptions := admission.DefaultOptions
	if opts, ok := cfg.TestingKnobs.AdmissionControlOptions.(*admission.Options); ok {
		admissionOptions.Override(opts)
	}

	engines, err := cfg.CreateEngines(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create engines")
	}
	stopper.AddCloser(&engines)

	// Loss of quorum recovery store is created and pending plan is applied to
	// engines as soon as engines are created and before any data is read in a
	// way similar to offline engine content patching.
	planStore, err := newPlanStore(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create loss of quorum plan store")
	}
	if err := loqrecovery.MaybeApplyPendingRecoveryPlan(ctx, planStore, engines, timeutil.DefaultTimeSource{}); err != nil {
		return nil, errors.Wrap(err, "failed to apply loss of quorum recovery plan")
	}

	nodeTombStorage, decommissionCheck := getPingCheckDecommissionFn(engines)

	g := gossip.New(
		cfg.AmbientCtx,
		cfg.ClusterIDContainer,
		nodeIDContainer,
		stopper,
		nodeRegistry,
		cfg.Locality,
	)

	tenantCapabilitiesTestingKnobs, _ := cfg.TestingKnobs.TenantCapabilitiesTestingKnobs.(*tenantcapabilities.TestingKnobs)
	authorizer := tenantcapabilitiesauthorizer.New(cfg.Settings, tenantCapabilitiesTestingKnobs)
	rpcCtxOpts := rpc.ServerContextOptionsFromBaseConfig(cfg.BaseConfig.Config)

	rpcCtxOpts.TenantID = roachpb.SystemTenantID
	rpcCtxOpts.UseNodeAuth = true
	rpcCtxOpts.NodeID = nodeIDContainer
	rpcCtxOpts.StorageClusterID = cfg.ClusterIDContainer
	rpcCtxOpts.Clock = clock.WallClock()
	rpcCtxOpts.ToleratedOffset = clock.ToleratedOffset()
	rpcCtxOpts.FatalOnOffsetViolation = true
	rpcCtxOpts.Stopper = stopper
	rpcCtxOpts.Settings = cfg.Settings
	rpcCtxOpts.OnOutgoingPing = func(ctx context.Context, req *rpc.PingRequest) error {
		// Outgoing ping will block requests with codes.FailedPrecondition to
		// notify caller that this replica is decommissioned but others could
		// still be tried as caller node is valid, but not the destination.
		return decommissionCheck(ctx, req.TargetNodeID, codes.FailedPrecondition)
	}
	rpcCtxOpts.TenantRPCAuthorizer = authorizer
	rpcCtxOpts.NeedsDialback = true
	rpcCtxOpts.Locality = cfg.Locality

	if knobs := cfg.TestingKnobs.Server; knobs != nil {
		serverKnobs := knobs.(*TestingKnobs)
		rpcCtxOpts.Knobs = serverKnobs.ContextTestingKnobs
	}
	rpcContext := rpc.NewContext(ctx, rpcCtxOpts)

	rpcContext.OnIncomingPing = func(ctx context.Context, req *rpc.PingRequest, resp *rpc.PingResponse) error {
		// Decommission state is only tracked for the system tenant.
		if tenantID, isTenant := roachpb.ClientTenantFromContext(ctx); !isTenant ||
			roachpb.IsSystemTenantID(tenantID.ToUint64()) {
			// Incoming ping will reject requests with codes.PermissionDenied to
			// signal remote node that it is not considered valid anymore and
			// operations should fail immediately.
			if err := decommissionCheck(ctx, req.OriginNodeID, codes.PermissionDenied); err != nil {
				return err
			}
		}
		// VerifyDialback verifies if a reverse connection to the sending node can
		// be established.
		return rpc.VerifyDialback(ctx, rpcContext, req, resp, cfg.Locality, &rpcContext.Settings.SV)
	}

	appRegistry.AddMetricStruct(rpcContext.Metrics())

	// Attempt to load TLS configs right away, failures are permanent.
	if !cfg.Insecure {
		// TODO(peter): Call methods on CertificateManager directly. Need to call
		// base.wrapError or similar on the resulting error.
		if _, err := rpcContext.GetServerTLSConfig(); err != nil {
			return nil, err
		}
		if _, err := rpcContext.GetUIServerTLSConfig(); err != nil {
			return nil, err
		}
		if _, err := rpcContext.GetClientTLSConfig(); err != nil {
			return nil, err
		}
		cm, err := rpcContext.GetCertificateManager()
		if err != nil {
			return nil, err
		}
		// Expose cert expirations in metrics.
		appRegistry.AddMetricStruct(cm.Metrics())
	}

	// Check the compatibility between the configured addresses and that
	// provided in certificates. This also logs the certificate
	// addresses in all cases to aid troubleshooting.
	// This must be called after the certificate manager was initialized
	// and after ValidateAddrs().
	rpcContext.CheckCertificateAddrs(ctx)

	requestMetrics := rpc.NewRequestMetrics()
	appRegistry.AddMetricStruct(requestMetrics)

	grpcServer, err := newGRPCServer(ctx, rpcContext, requestMetrics)
	if err != nil {
		return nil, err
	}

	drpcServer, err := newDRPCServer(ctx, rpcContext, requestMetrics)
	if err != nil {
		return nil, err
	}

	gossip.RegisterGossipServer(grpcServer.Server, g)
	if err := gossip.DRPCRegisterGossip(drpcServer, g.AsDRPCServer()); err != nil {
		return nil, err
	}

	var dialerKnobs nodedialer.DialerTestingKnobs
	if dk := cfg.TestingKnobs.DialerKnobs; dk != nil {
		dialerKnobs = dk.(nodedialer.DialerTestingKnobs)
	}

	kvNodeDialer := nodedialer.NewWithOpt(rpcContext, gossip.AddressResolver(g),
		nodedialer.DialerOpt{TestingKnobs: dialerKnobs})

	livenessCache := liveness.NewCache(g, clock, cfg.Settings, kvNodeDialer)

	runtimeSampler := status.NewRuntimeStatSampler(ctx, clock.WallClock())
	sysRegistry.AddMetricStruct(runtimeSampler)
	// Save a reference to this sampler for use by additional servers
	// started via the server controller.
	cfg.RuntimeStatSampler = runtimeSampler

	appRegistry.AddMetric(metric.NewFunctionalGauge(base.LicenseTTLMetadata, func() int64 {
		return base.GetLicenseTTL(ctx, cfg.Settings, timeutil.DefaultTimeSource{})
	}))
	appRegistry.AddMetric(metric.NewFunctionalGauge(base.AdditionalLicenseTTLMetadata, func() int64 {
		return base.GetLicenseTTL(ctx, cfg.Settings, timeutil.DefaultTimeSource{})
	}))

	// Create and add KV metric rules.
	kvserver.CreateAndAddRules(ctx, ruleRegistry)
	// Create and add server metric rules.
	serverrules.CreateAndAddRules(ctx, ruleRegistry)

	// A custom RetryOptions is created which uses stopper.ShouldQuiesce() as
	// the Closer. This prevents infinite retry loops from occurring during
	// graceful server shutdown
	//
	// Such a loop occurs when the DistSender attempts a connection to the
	// local server during shutdown, and receives an internal server error (HTTP
	// Code 5xx). This is the correct error for a server to return when it is
	// shutting down, and is normally retryable in a cluster environment.
	// However, on a single-node setup (such as a test), retries will never
	// succeed because the only server has been shut down; thus, the
	// DistSender needs to know that it should not retry in this situation.
	var clientTestingKnobs kvcoord.ClientTestingKnobs
	if kvKnobs := cfg.TestingKnobs.KVClient; kvKnobs != nil {
		clientTestingKnobs = *kvKnobs.(*kvcoord.ClientTestingKnobs)
	}
	retryOpts := cfg.RetryOptions
	if retryOpts == (retry.Options{}) {
		retryOpts = base.DefaultRetryOptions()
	}
	retryOpts.Closer = stopper.ShouldQuiesce()
	distSenderCfg := kvcoord.DistSenderConfig{
		AmbientCtx:         cfg.AmbientCtx,
		Settings:           st,
		Clock:              clock,
		NodeDescs:          g,
		Stopper:            stopper,
		LatencyFunc:        rpcContext.RemoteClocks.Latency,
		RPCRetryOptions:    &retryOpts,
		TransportFactory:   kvcoord.GRPCTransportFactory(kvNodeDialer),
		FirstRangeProvider: g,
		Locality:           cfg.Locality,
		TestingKnobs:       clientTestingKnobs,
		HealthFunc: func(id roachpb.NodeID) bool {
			return livenessCache.GetNodeVitality(id).IsLive(livenesspb.DistSender)
		},
	}
	distSender := kvcoord.NewDistSender(distSenderCfg)
	appRegistry.AddMetricStruct(distSender.Metrics())

	txnMetrics := kvcoord.MakeTxnMetrics(cfg.HistogramWindowInterval())
	appRegistry.AddMetricStruct(txnMetrics)
	txnCoordSenderFactoryCfg := kvcoord.TxnCoordSenderFactoryConfig{
		AmbientCtx:   cfg.AmbientCtx,
		Settings:     st,
		Clock:        clock,
		Stopper:      stopper,
		Linearizable: cfg.Linearizable,
		Metrics:      txnMetrics,
		TestingKnobs: clientTestingKnobs,
	}
	tcsFactory := kvcoord.NewTxnCoordSenderFactory(txnCoordSenderFactoryCfg, distSender)

	dbCtx := kv.DefaultDBContext(st, stopper)
	dbCtx.NodeID = idContainer
	db := kv.NewDBWithContext(cfg.AmbientCtx, tcsFactory, clock, dbCtx)

	nlActive, nlRenewal := cfg.NodeLivenessDurations()
	if knobs := cfg.TestingKnobs.NodeLiveness; knobs != nil {
		nlKnobs := knobs.(kvserver.NodeLivenessTestingKnobs)
		if duration := nlKnobs.LivenessDuration; duration != 0 {
			nlActive = duration
		}
		if duration := nlKnobs.RenewalDuration; duration != 0 {
			nlRenewal = duration
		}
	}

	rangeFeedKnobs, _ := cfg.TestingKnobs.RangeFeed.(*rangefeed.TestingKnobs)
	rangeFeedFactory, err := rangefeed.NewFactory(stopper, db, st, rangeFeedKnobs)
	if err != nil {
		return nil, err
	}

	stores := kvserver.NewStores(cfg.AmbientCtx, clock)

	decomNodeMap := &decommissioningNodeMap{
		nodes: make(map[roachpb.NodeID]interface{}),
	}
	nodeLiveness := liveness.NewNodeLiveness(liveness.NodeLivenessOptions{
		AmbientCtx:              cfg.AmbientCtx,
		Stopper:                 stopper,
		Clock:                   clock,
		Storage:                 liveness.NewKVStorage(db),
		LivenessThreshold:       nlActive,
		RenewalDuration:         nlRenewal,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
		// When we learn that a node is decommissioning, we want to proactively
		// enqueue the ranges we have that also have a replica on the
		// decommissioning node.
		OnNodeDecommissioning: decomNodeMap.makeOnNodeDecommissioningCallback(stores),
		OnNodeDecommissioned: func(id roachpb.NodeID) {
			if knobs, ok := cfg.TestingKnobs.Server.(*TestingKnobs); ok && knobs.OnDecommissionedCallback != nil {
				knobs.OnDecommissionedCallback(id)
			}
			if err := nodeTombStorage.SetDecommissioned(
				ctx, id, clock.PhysicalTime().UTC(),
			); err != nil {
				log.Dev.Fatalf(ctx, "unable to add tombstone for n%d: %s", id, err)
			}

			decomNodeMap.onNodeDecommissioned(id)
		},
		Engines: engines,
		//这就像是一个“签到中心”，每当节点心跳成功时，
		//它会跑到自己名下的**每一个仓库（Store）**里盖个章，
		//上面刻着：“本仓库在 [当前时间] 仍然处于健康服务的状态”。
		OnSelfHeartbeat: func(ctx context.Context) {
			now := clock.Now()
			if err := stores.VisitStores(func(s *kvserver.Store) error {
				return s.WriteLastUpTimestamp(ctx, now)
			}); err != nil {
				log.Ops.Warningf(ctx, "writing last up timestamp: %v", err)
			}
		},
		Cache: livenessCache,
	})

	nodeRegistry.AddMetricStruct(nodeLiveness.Metrics())

	nodeLivenessFn := storepool.MakeStorePoolNodeLivenessFunc(nodeLiveness)
	if nodeLivenessKnobs, ok := cfg.TestingKnobs.NodeLiveness.(kvserver.NodeLivenessTestingKnobs); ok {
		if nodeLivenessKnobs.StorePoolNodeLivenessFn != nil {
			nodeLivenessFn = nodeLivenessKnobs.StorePoolNodeLivenessFn
		}

		if nodeLivenessKnobs.IsLiveCallback != nil {
			nodeLiveness.RegisterCallback(nodeLivenessKnobs.IsLiveCallback)
		}
	}
	nodeLiveCountFn := func() int {
		var count int
		for _, nv := range nodeLiveness.ScanNodeVitalityFromCache() {
			if !nv.IsDecommissioning() && !nv.IsDecommissioned() {
				count++
			}
		}
		return count
	}
	storePool := storepool.NewStorePool(
		cfg.AmbientCtx,
		st,
		g,
		clock,
		nodeLiveCountFn,
		nodeLivenessFn,
		/* deterministic */ false,
	)

	storesForRACv2 := kvserver.MakeStoresForRACv2(stores)
	admissionKnobs, ok := cfg.TestingKnobs.AdmissionControl.(*admission.TestingKnobs)
	if !ok {
		admissionKnobs = &admission.TestingKnobs{}
	}
	gcoords := admission.NewGrantCoordinators(
		cfg.AmbientCtx,
		st,
		admissionOptions,
		nodeRegistry,
		storesForRACv2,
		admissionKnobs,
	)
	db.SQLKVResponseAdmissionQ = gcoords.RegularCPU.GetWorkQueue(admission.SQLKVResponseWork)
	db.AdmissionPacerFactory = gcoords.ElasticCPU
	goschedstats.RegisterSettings(st)
	if goschedstats.Supported {
		cbID := goschedstats.RegisterRunnableCountCallback(gcoords.RegularCPU.CPULoad)
		stopper.AddCloser(stop.CloserFn(func() {
			goschedstats.UnregisterRunnableCountCallback(cbID)
		}))
	} else {
		log.Dev.Warning(ctx, "goschedstats not supported; admission control will be impaired")
	}
	stopper.AddCloser(gcoords)

	var admissionControl struct {
		schedulerLatencyListener admission.SchedulerLatencyListener
		kvAdmissionController    kvadmission.Controller
		racHandles               kvflowcontrol.ReplicationAdmissionHandles
	}
	admissionControl.schedulerLatencyListener = gcoords.ElasticCPU.SchedulerLatencyListener
	admissionControl.racHandles = kvserver.MakeRACHandles(stores)
	admissionControl.kvAdmissionController = kvadmission.MakeController(
		nodeIDContainer,
		gcoords.RegularCPU.GetWorkQueue(admission.KVWork),
		gcoords.ElasticCPU,
		gcoords.Stores,
		admissionControl.racHandles,
		cfg.Settings,
	)

	admittedPiggybacker := node_rac2.NewAdmittedPiggybacker()
	streamTokenCounterProvider := rac2.NewStreamTokenCounterProvider(st, clock)
	sendTokenWatcher := rac2.NewSendTokenWatcher(stopper, timeutil.DefaultTimeSource{})
	waitForEvalConfig := rac2.NewWaitForEvalConfig(st)
	evalWaitMetrics := rac2.NewEvalWaitMetrics()
	rangeControllerMetrics := rac2.NewRangeControllerMetrics()
	nodeRegistry.AddMetricStruct(evalWaitMetrics)
	nodeRegistry.AddMetricStruct(rangeControllerMetrics)
	nodeRegistry.AddMetricStruct(streamTokenCounterProvider.Metrics())

	var raftTransportKnobs *kvserver.RaftTransportTestingKnobs
	if knobs := cfg.TestingKnobs.RaftTransport; knobs != nil {
		raftTransportKnobs = knobs.(*kvserver.RaftTransportTestingKnobs)
	}
	raftTransport := kvserver.NewRaftTransport(
		cfg.AmbientCtx,
		st,
		stopper,
		clock,
		kvNodeDialer,
		grpcServer.Server,
		drpcServer.DRPCServer,
		admittedPiggybacker,
		storesForRACv2,
		raftTransportKnobs,
	)
	nodeRegistry.AddMetricStruct(raftTransport.Metrics())

	var storeLiveness *storeliveness.NodeContainer
	{
		livenessInterval, heartbeatInterval := cfg.StoreLivenessDurations()
		supportGracePeriod := rpcContext.StoreLivenessWithdrawalGracePeriod()
		options := storeliveness.NewOptions(livenessInterval, heartbeatInterval, supportGracePeriod)
		transport, err := storeliveness.NewTransport(
			cfg.AmbientCtx, stopper, clock, kvNodeDialer,
			grpcServer.Server, drpcServer.DRPCServer, st, nil, /* knobs */
		)
		if err != nil {
			return nil, err
		}
		nodeRegistry.AddMetricStruct(transport.Metrics())
		var knobs *storeliveness.TestingKnobs
		if storeKnobs := cfg.TestingKnobs.Store; storeKnobs != nil {
			knobs = storeKnobs.(*kvserver.StoreTestingKnobs).StoreLivenessKnobs
		}
		storeLiveness = storeliveness.NewNodeContainer(stopper, options, transport, knobs)
	}

	ctSender := sidetransport.NewSender(stopper, st, clock, kvNodeDialer)
	ctReceiver := sidetransport.NewReceiver(nodeIDContainer, stopper, stores, nil /* testingKnobs */)
	var policyRefresher *policyrefresher.PolicyRefresher
	{
		var knobs *policyrefresher.TestingKnobs
		if policyRefresherKnobs := cfg.TestingKnobs.PolicyRefresherTestingKnobs; policyRefresherKnobs != nil {
			knobs = policyRefresherKnobs.(*policyrefresher.TestingKnobs)
		}
		policyRefresher = policyrefresher.NewPolicyRefresher(stopper, st, ctSender.GetLeaseholders,
			rpcContext.RemoteClocks.AllLatencies, knobs)
	}

	cpuUsageRefreshInterval := base.DefaultCPUUsageRefreshInterval
	cpuCapacityRefreshInterval := base.DefaultCPUCapacityRefreshInterval
	if ncpKnobs, _ := cfg.TestingKnobs.NodeCapacityProviderKnobs.(*load.NodeCapacityProviderTestingKnobs); ncpKnobs != nil {
		cpuUsageRefreshInterval = ncpKnobs.CpuUsageRefreshInterval
		cpuCapacityRefreshInterval = ncpKnobs.CpuCapacityRefreshInterval
	}
	nodeCapacityProviderConfig := load.NodeCapacityProviderConfig{
		CPUUsageRefreshInterval:    cpuUsageRefreshInterval,
		CPUCapacityRefreshInterval: cpuCapacityRefreshInterval,
		CPUUsageMovingAverageAge:   base.DefaultCPUUsageMovingAverageAge,
	}
	nodeCapacityProvider := load.NewNodeCapacityProvider(stopper, stores, nodeCapacityProviderConfig)

	// The Executor will be further initialized later, as we create more
	// of the server's components. There's a circular dependency - many things
	// need an Executor, but the Executor needs an executorConfig,
	// which in turn needs many things. That's why everybody that needs an
	// Executor uses this one instance.
	internalExecutor := &sql.InternalExecutor{}
	insqlDB := sql.NewShimInternalDB(db)
	jobRegistry := &jobs.Registry{} // ditto

	// Create an ExternalStorageBuilder. This is only usable after Start() where
	// we initialize all the configuration params.
	externalStorageBuilder := &externalStorageBuilder{}
	externalStorage := externalStorageBuilder.makeExternalStorage
	externalStorageFromURI := externalStorageBuilder.makeExternalStorageFromURI

	protectedtsKnobs, _ := cfg.TestingKnobs.ProtectedTS.(*protectedts.TestingKnobs)
	protectedtsProvider, err := ptprovider.New(ptprovider.Config{
		DB:       insqlDB,
		Settings: st,
		Knobs:    protectedtsKnobs,
		ReconcileStatusFuncs: ptreconcile.StatusFuncs{
			jobsprotectedts.GetMetaType(jobsprotectedts.Jobs): jobsprotectedts.MakeStateFunc(
				jobRegistry, jobsprotectedts.Jobs,
			),
			jobsprotectedts.GetMetaType(jobsprotectedts.Schedules): jobsprotectedts.MakeStateFunc(
				jobRegistry, jobsprotectedts.Schedules,
			),
			sessionprotectedts.SessionMetaType: sessionprotectedts.MakeStatusFunc(),
		},
	})
	if err != nil {
		return nil, err
	}
	appRegistry.AddMetricStruct(protectedtsProvider.Metrics())

	// Break a circular dependency: we need the rootSQLMemoryMonitor to construct
	// the KV memory monitor for the StoreConfig.
	sqlMonitorAndMetrics := newRootSQLMemoryMonitor(monitorAndMetricsOptions{
		memoryPoolSize:          cfg.MemoryPoolSize,
		histogramWindowInterval: cfg.HistogramWindowInterval(),
		settings:                cfg.Settings,
	})
	kvMemoryMonitor := mon.NewMonitorInheritWithLimit(
		mon.MakeName("kv-mem"), 0 /* limit */, sqlMonitorAndMetrics.rootSQLMemoryMonitor,
		true,                     /* longLiving */
	)
	kvMemoryMonitor.StartNoReserved(ctx, sqlMonitorAndMetrics.rootSQLMemoryMonitor)
	rangeFeedBudgetFactory := serverrangefeed.NewBudgetFactory(
		ctx,
		serverrangefeed.CreateBudgetFactoryConfig(
			kvMemoryMonitor,
			cfg.MemoryPoolSize,
			cfg.HistogramWindowInterval(),
			func(limit int64) int64 {
				if !serverrangefeed.RangefeedBudgetsEnabled.Get(&st.SV) {
					return 0
				}
				if raftCmdLimit := kvserverbase.MaxCommandSize.Get(&st.SV); raftCmdLimit > limit {
					return raftCmdLimit
				}
				return limit
			},
			&st.SV))
	if rangeFeedBudgetFactory != nil {
		nodeRegistry.AddMetricStruct(rangeFeedBudgetFactory.Metrics())
	}

	raftEntriesMonitor := logstore.NewRaftEntriesSoftLimit()
	nodeRegistry.AddMetric(raftEntriesMonitor.Metric)

	stopper.AddCloser(stop.CloserFn(func() {
		// Stop the root SQL monitor to enforce (in test builds) that all
		// short-living descendants are stopped too.
		//
		// Note that we don't do this for SQL servers of tenants since there we
		// can have ungraceful shutdown whenever the node is quiescing, so we
		// have some short-living monitors that aren't stopped.
		sqlMonitorAndMetrics.rootSQLMemoryMonitor.EmergencyStop(ctx)
	}))

	tsDB := ts.NewDB(db, cfg.Settings)
	nodeRegistry.AddMetricStruct(tsDB.Metrics())
	nodeCountFn := func() int64 {
		return nodeLiveness.Metrics().LiveNodes.Value()
	}
	sTS := ts.MakeServer(
		cfg.AmbientCtx, tsDB, nodeCountFn, cfg.TimeSeriesServerConfig,
		sqlMonitorAndMetrics.rootSQLMemoryMonitor, stopper,
	)

	systemConfigWatcher := systemconfigwatcher.New(
		keys.SystemSQLCodec, clock, rangeFeedFactory, &cfg.DefaultZoneConfig,
	)

	tenantCapabilitiesWatcher := tenantcapabilitieswatcher.New(
		clock,
		cfg.Settings,
		rangeFeedFactory,
		keys.TenantsTableID,
		stopper,
		1<<20, /* 1 MB */
		tenantCapabilitiesTestingKnobs,
	)

	var spanConfig struct {
		// kvAccessor powers the span configuration RPCs and the host tenant's
		// reconciliation job.
		kvAccessor spanconfig.KVAccessor
		// reporter is used to report over span config conformance.
		reporter spanconfig.Reporter
		// subscriber is used by stores to subscribe to span configuration updates.
		subscriber spanconfig.KVSubscriber
		// kvAccessorForTenantRecords is when creating/destroying secondary
		// tenant records.
		kvAccessorForTenantRecords spanconfig.KVAccessor
	}
	spanConfigKnobs, _ := cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs)
	// We use the span configs infra to control whether rangefeeds are
	// enabled on a given range. At the moment this only applies to
	// system tables (on both host and secondary tenants). We need to
	// consider two things:
	// - The sql-side reconciliation process runs asynchronously. When
	//   the config for a given range is requested, we might not yet have
	//   it, thus falling back to the static config below.
	// - Various internal subsystems rely on rangefeeds to function.
	//
	// Consequently, we configure our static fallback config to actually
	// allow rangefeeds. As the sql-side reconciliation process kicks
	// off, it'll install the actual configs that we'll later consult.
	// For system table ranges we install configs that allow for
	// rangefeeds. Until then, we simply allow rangefeeds when a more
	// targeted config is not found.
	fallbackConf := cfg.DefaultZoneConfig.AsSpanConfig()
	fallbackConf.RangefeedEnabled = true
	// We do the same for opting out of strict GC enforcement; it
	// really only applies to user table ranges
	fallbackConf.GCPolicy.IgnoreStrictEnforcement = true

	spanConfig.subscriber = spanconfigkvsubscriber.New(
		clock,
		rangeFeedFactory,
		keys.SpanConfigurationsTableID,
		4<<20, /* 4 MB */
		fallbackConf,
		cfg.Settings,
		spanconfigstore.NewBoundsReader(tenantCapabilitiesWatcher),
		spanConfigKnobs,
		nodeRegistry,
	)

	if spanConfigKnobs != nil && spanConfigKnobs.StoreKVSubscriberOverride != nil {
		spanConfig.subscriber = spanConfigKnobs.StoreKVSubscriberOverride(spanConfig.subscriber)
	}

	scKVAccessor := spanconfigkvaccessor.New(
		db, internalExecutor, cfg.Settings, clock,
		systemschema.SpanConfigurationsTableName.FQString(),
		spanConfigKnobs,
	)
	spanConfig.kvAccessor, spanConfig.kvAccessorForTenantRecords = scKVAccessor, scKVAccessor
	spanConfig.reporter = spanconfigreporter.New(
		nodeLiveness,
		storePool,
		spanConfig.subscriber,
		rangedesc.NewScanner(db),
		cfg.Settings,
		spanConfigKnobs,
	)

	var protectedTSReader spanconfig.ProtectedTSReader
	if cfg.TestingKnobs.SpanConfig != nil &&
		cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs).ProtectedTSReaderOverrideFn != nil {
		fn := cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs).ProtectedTSReaderOverrideFn
		protectedTSReader = fn(clock)
	} else {
		protectedTSReader = spanconfigptsreader.NewAdapter(protectedtsProvider.(*ptprovider.Provider).Cache,
			spanConfig.subscriber, cfg.Settings)
	}

	rangeLogWriter := rangelog.NewWriter(
		keys.SystemSQLCodec,
		func() int64 {
			return unique.GenerateUniqueInt(
				unique.ProcessUniqueID(nodeIDContainer.Get()),
			)
		},
	)
	eagerLeaseAcquisitionLimiter := quotapool.NewIntPool("eager-lease-acquisitions",
		uint64(kvserver.EagerLeaseAcquisitionConcurrency.Get(&cfg.Settings.SV)))
	kvserver.EagerLeaseAcquisitionConcurrency.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		eagerLeaseAcquisitionLimiter.UpdateCapacity(
			uint64(kvserver.EagerLeaseAcquisitionConcurrency.Get(&cfg.Settings.SV)))
	})

	mmaAlloc, mmaAllocSync := func() (mmaprototype.Allocator, *mmaintegration.AllocatorSync) {
		mmaAllocState := mmaprototype.NewAllocatorState(timeutil.DefaultTimeSource{},
			rand.New(rand.NewSource(timeutil.Now().UnixNano())))
		allocatorSync := mmaintegration.NewAllocatorSync(storePool, mmaAllocState, st, nil)
		// We make sure that mmaAllocState is returned through the `Allocator`
		// interface so that when looking up callers to the interface, we see this
		// call site.
		return mmaAllocState, allocatorSync
	}()

	g.RegisterCallback(
		gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix),
		func(_ string, content roachpb.Value, origTimestampNanos int64) {
			var storeDesc roachpb.StoreDescriptor
			if err := content.GetProto(&storeDesc); err != nil {
				log.Dev.Errorf(ctx, "%v", err)
				return
			}
			storeLoadMsg := mmaintegration.MakeStoreLoadMsg(storeDesc, origTimestampNanos)
			mmaAlloc.SetStore(state.StoreAttrAndLocFromDesc(storeDesc))
			mmaAlloc.ProcessStoreLoadMsg(ctx, &storeLoadMsg)
		},
	)

	storeCfg := kvserver.StoreConfig{
		DefaultSpanConfig:            cfg.DefaultZoneConfig.AsSpanConfig(),
		Settings:                     st,
		AmbientCtx:                   cfg.AmbientCtx,
		RaftConfig:                   cfg.RaftConfig,
		Clock:                        clock,
		DB:                           db,
		Gossip:                       g,
		NodeLiveness:                 nodeLiveness,
		StoreLiveness:                storeLiveness,
		StorePool:                    storePool,
		MMAllocator:                  mmaAlloc,
		AllocatorSync:                mmaAllocSync,
		Transport:                    raftTransport,
		NodeDialer:                   kvNodeDialer,
		RPCContext:                   rpcContext,
		ScanInterval:                 cfg.ScanInterval,
		ScanMinIdleTime:              cfg.ScanMinIdleTime,
		ScanMaxIdleTime:              cfg.ScanMaxIdleTime,
		HistogramWindowInterval:      cfg.HistogramWindowInterval(),
		LogRangeAndNodeEvents:        cfg.EventLogEnabled,
		RangeDescriptorCache:         distSender.RangeDescriptorCache(),
		TimeSeriesDataStore:          tsDB,
		ClosedTimestampSender:        ctSender,
		ClosedTimestampReceiver:      ctReceiver,
		PolicyRefresher:              policyRefresher,
		NodeCapacityProvider:         nodeCapacityProvider,
		ProtectedTimestampReader:     protectedTSReader,
		EagerLeaseAcquisitionLimiter: eagerLeaseAcquisitionLimiter,
		KVMemoryMonitor:              kvMemoryMonitor,
		RangefeedBudgetFactory:       rangeFeedBudgetFactory,
		RaftEntriesMonitor:           raftEntriesMonitor,
		SharedStorageEnabled:         cfg.StorageConfig.SharedStorage.URI != "",
		SystemConfigProvider:         systemConfigWatcher,
		SpanConfigSubscriber:         spanConfig.subscriber,
		RangeLogWriter:               rangeLogWriter,
		KVAdmissionController:        admissionControl.kvAdmissionController,
		KVFlowAdmittedPiggybacker:    admittedPiggybacker,
		KVFlowStreamTokenProvider:    streamTokenCounterProvider,
		KVFlowSendTokenWatcher:       sendTokenWatcher,
		KVFlowWaitForEvalConfig:      waitForEvalConfig,
		KVFlowEvalWaitMetrics:        evalWaitMetrics,
		KVFlowRangeControllerMetrics: rangeControllerMetrics,
		SchedulerLatencyListener:     admissionControl.schedulerLatencyListener,
		RangeCount:                   &atomic.Int64{},
	}
	if storeTestingKnobs := cfg.TestingKnobs.Store; storeTestingKnobs != nil {
		storeCfg.TestingKnobs = *storeTestingKnobs.(*kvserver.StoreTestingKnobs)
	}
	storeCfg.SetDefaults(len(engines))

	systemTenantNameContainer := roachpb.NewTenantNameContainer(catconstants.SystemTenantName)

	recorder := status.NewMetricsRecorder(
		rpcContext.TenantID,
		systemTenantNameContainer,
		nodeLiveness,
		rpcContext.RemoteClocks,
		clock.WallClock(),
		st,
	)
	appRegistry.AddMetricStruct(rpcContext.RemoteClocks.Metrics())

	updates := &diagnostics.UpdateChecker{
		StartTime:        timeutil.Now(),
		AmbientCtx:       &cfg.AmbientCtx,
		Config:           cfg.BaseConfig.Config,
		Settings:         cfg.Settings,
		StorageClusterID: rpcContext.StorageClusterID.Get,
		LogicalClusterID: rpcContext.LogicalClusterID.Get,
		NodeID:           nodeIDContainer.Get,
		SQLInstanceID:    idContainer.SQLInstanceID,
	}

	if cfg.TestingKnobs.Server != nil {
		updates.TestingKnobs = &cfg.TestingKnobs.Server.(*TestingKnobs).DiagnosticsTestingKnobs
	}

	tenantUsage := NewTenantUsageServer(st, db, insqlDB)
	nodeRegistry.AddMetricStruct(tenantUsage.Metrics())

	tenantSettingsWatcher := tenantsettingswatcher.New(
		clock, rangeFeedFactory, stopper, st,
	)

	node := NewNode(
		storeCfg,
		recorder,
		nodeRegistry,
		stopper,
		txnMetrics,
		stores,
		cfg.ClusterIDContainer,
		gcoords.RegularCPU.GetWorkQueue(admission.KVWork),
		gcoords.ElasticCPU,
		gcoords.Stores,
		tenantUsage,
		tenantSettingsWatcher,
		tenantCapabilitiesWatcher,
		spanConfig.kvAccessor,
		spanConfig.reporter,
		distSender,
		cfg.LicenseEnforcer,
	)
	kvpb.RegisterInternalServer(grpcServer.Server, node)
	if err := kvpb.DRPCRegisterKVBatch(drpcServer, node.AsDRPCKVBatchServer()); err != nil {
		return nil, err
	}
	if err := kvpb.DRPCRegisterRangeFeed(drpcServer, node.AsDRPCRangeFeedServer()); err != nil {
		return nil, err
	}
	if err := kvpb.DRPCRegisterTenantService(drpcServer, node.AsDRPCTenantServiceServer()); err != nil {
		return nil, err
	}
	if err := kvpb.DRPCRegisterTenantUsage(drpcServer, node); err != nil {
		return nil, err
	}
	if err := kvpb.DRPCRegisterTenantSpanConfig(drpcServer, node); err != nil {
		return nil, err
	}
	if err := kvpb.DRPCRegisterNode(drpcServer, node); err != nil {
		return nil, err
	}
	if err := kvpb.DRPCRegisterQuorumRecovery(drpcServer, node); err != nil {
		return nil, err
	}
	kvserver.RegisterPerReplicaServer(grpcServer.Server, node.perReplicaServer)
	if err := kvserver.DRPCRegisterPerReplica(drpcServer, node.perReplicaServer); err != nil {
		return nil, err
	}
	kvserver.RegisterPerStoreServer(grpcServer.Server, node.perReplicaServer)
	if err := kvserver.DRPCRegisterPerStore(drpcServer, node.perReplicaServer); err != nil {
		return nil, err
	}
	ctpb.RegisterSideTransportServer(grpcServer.Server, ctReceiver)
	if err := ctpb.DRPCRegisterSideTransport(drpcServer, ctReceiver.AsDRPCServer()); err != nil {
		return nil, err
	}

	// Create blob service for inter-node file sharing.
	blobService, err := blobs.NewBlobService(cfg.ExternalIODir)
	if err != nil {
		return nil, errors.Wrap(err, "creating blob service")
	}
	blobspb.RegisterBlobServer(grpcServer.Server, blobService)
	if err := blobspb.DRPCRegisterBlob(drpcServer, blobService.AsDRPCServer()); err != nil {
		return nil, err
	}

	replicationReporter := reports.NewReporter(
		db, node.stores, storePool, st, nodeLiveness, internalExecutor, systemConfigWatcher,
	)

	lateBoundServer := &topLevelServer{}

	// The following initialization is mirrored in NewTenantServer().
	// Please keep them in sync.

	// Instantiate the API privilege checker.
	//
	// TODO(tbg): give adminServer only what it needs (and avoid circular deps).
	adminAuthzCheck := privchecker.NewChecker(internalExecutor, st)

	// Instantiate the HTTP server.
	// These callbacks help us avoid a dependency on gossip in httpServer.
	parseNodeIDFn := func(s string) (roachpb.NodeID, bool, error) {
		return parseNodeID(g, s)
	}
	getNodeIDHTTPAddressFn := func(id roachpb.NodeID) (*util.UnresolvedAddr, roachpb.Locality, error) {
		return g.GetNodeIDHTTPAddress(id)
	}
	sHTTP := newHTTPServer(cfg.BaseConfig, rpcContext, parseNodeIDFn, getNodeIDHTTPAddressFn)

	// Instantiate the SQL session registry.
	sessionRegistry := sql.NewSessionRegistry()

	// Instantiate the cache of closed SQL sessions.
	closedSessionCache := sql.NewClosedSessionCache(cfg.Settings, sqlMonitorAndMetrics.rootSQLMemoryMonitor, time.Now)

	// Instantiate the distSQL remote flow runner.
	remoteFlowRunnerAcc := sqlMonitorAndMetrics.rootSQLMemoryMonitor.MakeBoundAccount()
	remoteFlowRunner := flowinfra.NewRemoteFlowRunner(cfg.AmbientCtx, stopper, &remoteFlowRunnerAcc)

	serverIterator := &kvFanoutClient{
		gossip:       g,
		rpcCtx:       rpcContext,
		db:           db,
		nodeLiveness: nodeLiveness,
		clock:        clock,
		st:           st,
		ambientCtx:   cfg.AmbientCtx,
	}

	// Instantiate the status API server.
	var serverTestingKnobs *TestingKnobs
	if cfg.TestingKnobs.Server != nil {
		serverTestingKnobs = cfg.TestingKnobs.Server.(*TestingKnobs)
	}

	sStatus := newSystemStatusServer(
		cfg.AmbientCtx,
		st,
		cfg.Config,
		adminAuthzCheck,
		db,
		g,
		recorder,
		nodeLiveness,
		storePool,
		rpcContext,
		node.stores,
		&engines,
		stopper,
		sessionRegistry,
		closedSessionCache,
		remoteFlowRunner,
		internalExecutor,
		serverIterator,
		spanConfig.reporter,
		clock,
		rangestats.NewFetcher(db),
		node,
		serverTestingKnobs,
	)

	keyVisualizerServer := &KeyVisualizerServer{
		ie:           internalExecutor,
		settings:     st,
		kvNodeDialer: kvNodeDialer,
		status:       sStatus,
		node:         node,
	}
	keyVisServerAccessor := spanstatskvaccessor.New(keyVisualizerServer)

	// Instantiate the KV prober.
	kvProber := kvprober.NewProber(kvprober.Opts{
		Tracer:                  cfg.AmbientCtx.Tracer,
		DB:                      db,
		Settings:                st,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
	})
	nodeRegistry.AddMetricStruct(kvProber.Metrics())

	// The settings cache writer is responsible for persisting the
	// cluster settings on KV nodes across restarts.
	settingsWriter := newSettingsCacheWriter(engines[0], stopper)
	stopTrigger := newStopTrigger()

	// Initialize the pgwire pre-server, which initializes connections,
	// sets up TLS and reads client status parameters.
	pgPreServer := pgwire.NewPreServeConnHandler(
		cfg.AmbientCtx,
		cfg.Config,
		cfg.Settings,
		rpcContext.GetServerTLSConfig,
		cfg.HistogramWindowInterval(),
		sqlMonitorAndMetrics.rootSQLMemoryMonitor,
		true, /* acceptTenantName */
	)
	for _, m := range pgPreServer.Metrics() {
		appRegistry.AddMetricStruct(m)
	}

	inspectzServer := inspectz.NewServer(
		cfg.BaseConfig.AmbientCtx,
		storesForRACv2,
		node.storeCfg.KVFlowStreamTokenProvider,
		kvserver.MakeStoresForStoreLiveness(stores),
	)
	if err = cfg.CidrLookup.Start(ctx, stopper); err != nil {
		return nil, err
	}

	// Instantiate the SQL server proper.
	sqlServer, err := newSQLServer(ctx, sqlServerArgs{
		sqlServerOptionalKVArgs: sqlServerOptionalKVArgs{
			nodesStatusServer:        serverpb.MakeOptionalNodesStatusServer(sStatus),
			nodeLiveness:             optionalnodeliveness.MakeContainer(nodeLiveness),
			gossip:                   gossip.MakeOptionalGossip(g),
			grpcServer:               grpcServer.Server,
			drpcMux:                  drpcServer.DRPCServer,
			nodeIDContainer:          idContainer,
			externalStorage:          externalStorage,
			externalStorageFromURI:   externalStorageFromURI,
			isMeta1Leaseholder:       node.stores.IsMeta1Leaseholder,
			sqlSQLResponseAdmissionQ: gcoords.RegularCPU.GetWorkQueue(admission.SQLSQLResponseWork),
			spanConfigKVAccessor:     spanConfig.kvAccessorForTenantRecords,
			kvStoresIterator:         kvserver.MakeStoresIterator(node.stores),
			inspectzServer:           inspectzServer,

			notifyChangeToSystemVisibleSettings: tenantSettingsWatcher.SetAlternateDefaults,
		},
		SQLConfig:                &cfg.SQLConfig,
		BaseConfig:               &cfg.BaseConfig,
		stopper:                  stopper,
		stopTrigger:              stopTrigger,
		clock:                    clock,
		runtime:                  runtimeSampler,
		rpcContext:               rpcContext,
		nodeDescs:                g,
		systemConfigWatcher:      systemConfigWatcher,
		spanConfigAccessor:       spanConfig.kvAccessor,
		spanConfigReporter:       spanConfig.reporter,
		keyVisServerAccessor:     keyVisServerAccessor,
		kvNodeDialer:             kvNodeDialer,
		distSender:               distSender,
		db:                       db,
		registry:                 appRegistry,
		sysRegistry:              sysRegistry,
		recorder:                 recorder,
		sessionRegistry:          sessionRegistry,
		closedSessionCache:       closedSessionCache,
		remoteFlowRunner:         remoteFlowRunner,
		circularInternalExecutor: internalExecutor,
		internalDB:               insqlDB,
		circularJobRegistry:      jobRegistry,
		protectedtsProvider:      protectedtsProvider,
		rangeFeedFactory:         rangeFeedFactory,
		sqlStatusServer:          sStatus,
		tenantStatusServer:       sStatus,
		tenantUsageServer:        tenantUsage,
		monitorAndMetrics:        sqlMonitorAndMetrics,
		settingsStorage:          settingsWriter,
		admissionPacerFactory:    gcoords.ElasticCPU,
		rangeDescIteratorFactory: rangedesc.NewIteratorFactory(db),
		tenantCapabilitiesReader: sql.MakeSystemTenantOnly[tenantcapabilities.Reader](tenantCapabilitiesWatcher),
	})
	if err != nil {
		return nil, err
	}

	// Tell the authz server how to connect to SQL.
	adminAuthzCheck.SetAuthzAccessorFactory(func(opName redact.SafeString) (sql.AuthorizationAccessor, func()) {
		// This is a hack to get around a Go package dependency cycle. See comment
		// in sql/jobs/registry.go on planHookMaker.
		txn := db.NewTxn(ctx, "check-system-privilege")
		p, cleanup := sql.NewInternalPlanner(
			opName,
			txn,
			username.NodeUserName(),
			&sql.MemoryMetrics{},
			sqlServer.execCfg,
			sql.NewInternalSessionData(ctx, sqlServer.execCfg.Settings, opName),
		)
		return p.(sql.AuthorizationAccessor), cleanup
	})

	// Create the authentication RPC server (login/logout).
	sAuth := authserver.NewServer(cfg.Config, sqlServer)

	// Create a drain server.
	drain := newDrainServer(cfg.BaseConfig, stopper, stopTrigger, grpcServer, drpcServer, sqlServer)
	drain.setNode(node, nodeLiveness)

	// Instantiate the admin API server.
	sAdmin := newSystemAdminServer(
		sqlServer,
		cfg.Settings,
		adminAuthzCheck,
		internalExecutor,
		cfg.BaseConfig.AmbientCtx,
		recorder,
		db,
		nodeLiveness,
		rpcContext,
		serverIterator,
		clock,
		distSender,
		grpcServer,
		drpcServer,
		drain,
		lateBoundServer,
	)

	// Connect the various servers to RPC.
	for i, gw := range []grpcGatewayServer{sAdmin, sStatus, sAuth, &sTS} {
		if reflect.ValueOf(gw).IsNil() {
			return nil, errors.Errorf("%d: nil", i)
		}
		gw.RegisterService(grpcServer.Server)
	}

	for _, s := range []drpcServiceRegistrar{sAdmin, sStatus, &sTS} {
		if err := s.RegisterDRPCService(drpcServer); err != nil {
			return nil, err
		}
	}

	// Tell the node event logger (join, restart) how to populate SQL entries
	// into system.eventlog.
	node.InitLogger(sqlServer.execCfg)

	// Tell the status server how to access SQL structures.
	sStatus.setStmtDiagnosticsRequester(sqlServer.execCfg.StmtDiagnosticsRecorder)
	sStatus.setTxnDiagnosticsRequester(sqlServer.execCfg.TxnDiagnosticsRecorder)
	sStatus.baseStatusServer.sqlServer = sqlServer

	// Create a server controller.
	sc := newServerController(ctx,
		cfg.BaseConfig.AmbientCtx,
		node, cfg.BaseConfig.IDContainer,
		stopper, st,
		lateBoundServer,
		&systemServerWrapper{server: lateBoundServer},
		systemTenantNameContainer,
		pgPreServer.SendRoutingError,
		tenantCapabilitiesWatcher,
		cfg.DisableSQLServer,
		cfg.BaseConfig.DisableTLSForHTTP,
		cfg.Insecure,
	)
	drain.serverCtl = sc

	// Create the debug API server.
	debugServer := debug.NewServer(
		cfg.BaseConfig.AmbientCtx,
		st,
		sqlServer.pgServer.HBADebugFn(),
		sqlServer.execCfg.SQLStatusServer,
		roachpb.SystemTenantID,
		authorizer,
	)

	recoveryServer := loqrecovery.NewServer(
		nodeIDContainer,
		st,
		stores,
		planStore,
		g,
		cfg.Locality,
		rpcContext,
		cfg.TestingKnobs.LOQRecovery,
		func(ctx context.Context, id roachpb.NodeID) error {
			return nodeTombStorage.SetDecommissioned(ctx, id, timeutil.Now())
		},
	)

	*lateBoundServer = topLevelServer{
		nodeIDContainer:           nodeIDContainer,
		cfg:                       cfg,
		st:                        st,
		clock:                     clock,
		rpcContext:                rpcContext,
		engines:                   engines,
		grpc:                      grpcServer,
		drpc:                      drpcServer,
		gossip:                    g,
		kvNodeDialer:              kvNodeDialer,
		nodeLiveness:              nodeLiveness,
		storePool:                 storePool,
		tcsFactory:                tcsFactory,
		distSender:                distSender,
		db:                        db,
		node:                      node,
		nodeRegistry:              nodeRegistry,
		appRegistry:               appRegistry,
		sysRegistry:               sysRegistry,
		recorder:                  recorder,
		ruleRegistry:              ruleRegistry,
		promRuleExporter:          promRuleExporter,
		updates:                   updates,
		ctSender:                  ctSender,
		policyRefresher:           policyRefresher,
		nodeCapacityProvider:      nodeCapacityProvider,
		runtime:                   runtimeSampler,
		http:                      sHTTP,
		adminAuthzCheck:           adminAuthzCheck,
		admin:                     sAdmin,
		status:                    sStatus,
		drain:                     drain,
		decomNodeMap:              decomNodeMap,
		authentication:            sAuth,
		tsDB:                      tsDB,
		tsServer:                  &sTS,
		recoveryServer:            recoveryServer,
		raftTransport:             raftTransport,
		storeLiveness:             storeLiveness,
		stopper:                   stopper,
		stopTrigger:               stopTrigger,
		debug:                     debugServer,
		kvProber:                  kvProber,
		replicationReporter:       replicationReporter,
		protectedtsProvider:       protectedtsProvider,
		spanConfigSubscriber:      spanConfig.subscriber,
		spanConfigReporter:        spanConfig.reporter,
		tenantCapabilitiesWatcher: tenantCapabilitiesWatcher,
		pgPreServer:               pgPreServer,
		sqlServer:                 sqlServer,
		serverController:          sc,
		externalStorageBuilder:    externalStorageBuilder,
		storeGrantCoords:          gcoords.Stores,
		kvMemoryMonitor:           kvMemoryMonitor,
		keyVisualizerServer:       keyVisualizerServer,
		inspectzServer:            inspectzServer,
	}

	return lateBoundServer, err
}

// newClockFromConfig creates a HLC clock from the server configuration.
func newClockFromConfig(ctx context.Context, cfg BaseConfig) (*hlc.Clock, error) {
	maxOffset := time.Duration(cfg.MaxOffset)
	toleratedOffset := cfg.ToleratedOffset()
	var clock *hlc.Clock
	if cfg.ClockDevicePath != "" {
		ptpClock, err := ptp.MakeClock(ctx, cfg.ClockDevicePath)
		if err != nil {
			return nil, errors.Wrap(err, "instantiating clock source")
		}
		clock = hlc.NewClock(ptpClock, maxOffset, toleratedOffset, logger.CRDBLogger)
	} else if cfg.TestingKnobs.Server != nil &&
		cfg.TestingKnobs.Server.(*TestingKnobs).WallClock != nil {
		clock = hlc.NewClock(cfg.TestingKnobs.Server.(*TestingKnobs).WallClock,
			maxOffset, toleratedOffset, logger.CRDBLogger)
	} else {
		clock = hlc.NewClockWithSystemTimeSource(maxOffset, toleratedOffset, logger.CRDBLogger)
	}
	return clock, nil
}

// ClusterSettings returns the cluster settings.
func (s *topLevelServer) ClusterSettings() *cluster.Settings {
	return s.st
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *topLevelServer) AnnotateCtx(ctx context.Context) context.Context {
	return s.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// AnnotateCtxWithSpan is a convenience wrapper; see AmbientContext.
func (s *topLevelServer) AnnotateCtxWithSpan(
	ctx context.Context, opName string,
) (context.Context, *tracing.Span) {
	return s.cfg.AmbientCtx.AnnotateCtxWithSpan(ctx, opName)
}

// StorageClusterID returns the ID of the storage cluster this server is a part of.
func (s *topLevelServer) StorageClusterID() uuid.UUID {
	return s.rpcContext.StorageClusterID.Get()
}

// NodeID returns the ID of this node within its cluster.
func (s *topLevelServer) NodeID() roachpb.NodeID {
	return s.node.Descriptor.NodeID
}

// InitialStart returns whether this is the first time the node has started (as
// opposed to being restarted). Only intended to help print debugging info
// during server startup.
func (s *topLevelServer) InitialStart() bool {
	return s.node.initialStart
}

// listenerInfo is a helper used to write files containing various listener
// information to the store directories. In contrast to the "listening url
// file", these are written once the listeners are available, before the server
// is necessarily ready to serve.
type listenerInfo struct {
	listenRPC    string // the (RPC) listen address, rewritten after name resolution and port allocation
	advertiseRPC string // contains the original addr part of --listen/--advertise, with actual port number after port allocation if original was 0
	listenSQL    string // the SQL endpoint, rewritten after name resolution and port allocation
	advertiseSQL string // contains the original addr part of --sql-addr, with actual port number after port allocation if original was 0
	listenHTTP   string // the HTTP endpoint
}

// Iter returns a mapping of file names to desired contents.
func (li listenerInfo) Iter() map[string]string {
	return map[string]string{
		"cockroach.listen-addr":        li.listenRPC,
		"cockroach.advertise-addr":     li.advertiseRPC,
		"cockroach.sql-addr":           li.listenSQL,
		"cockroach.advertise-sql-addr": li.advertiseSQL,
		"cockroach.http-addr":          li.listenHTTP,
	}
}

// PreStart starts the server on the specified port, starts gossip and
// initializes the node using the engines from the server's context.
//
// PreStart 方法会在指定的端口上启动服务器，启动 Gossip（CockroachDB 的集群发现和信息传播协议），
// 并使用服务器上下文中的存储引擎（engines）来初始化节点。

// It does not activate the pgwire listener over the network / unix
// socket, which is done by the AcceptClients() method. The separation
// between the two exists so that SQL initialization can take place
// before the first client is accepted.
//
// PreStart **不会**激活 pgwire 监听器（即 PostgreSQL wire protocol 的网络/Unix socket 监听），
// 这部分工作由后续的 AcceptClients() 方法完成。
// 将两者分开的原因是：**在接受第一个外部客户端连接之前，需要先完成 SQL 层面的初始化**（如创建系统表、初始用户等）。

// PreStart is complex since it sets up the listeners and the associated
// port muxing, but especially since it has to solve the
// "bootstrapping problem": nodes need to connect to Gossip fairly
// early, but what drives Gossip connectivity are the first range
// replicas in the kv store. This in turn suggests opening the Gossip
// server early. However, naively doing so also serves most other
// services prematurely, which exposes a large surface of potentially
// underinitialized services. This is avoided with some additional
// complexity that can be summarized as follows:
//
// PreStart 实现起来较为复杂，主要有以下原因：
//   - 需要设置各种监听器（listeners）和端口复用（port muxing）
//   - 更重要的是需要解决“自举问题”（bootstrapping problem）：
//     节点需要尽早连接到 Gossip 网络（用于集群发现、节点间通信），
//     但 Gossip 的连通性依赖于 KV 存储中第一个 Range 的副本（replicas）。
//     这意味着必须尽早开启 Gossip 服务。
//     但是，如果简单粗暴地过早开启 Gossip 服务，会导致其他大部分服务也随之暴露，
//     而这些服务此时可能尚未完成初始化，从而暴露大量潜在不稳定的接口。
//     为了避免这种情况，CockroachDB 引入了一些额外的复杂逻辑，概括如下：
//
//   - before blocking trying to connect to the Gossip network, we already open
//     the admin UI (so that its diagnostics are available)
//     → 在阻塞式尝试连接 Gossip 网络之前，就先开放 Admin UI（管理界面），这样诊断信息（如节点状态、日志）就能提前可用。
//
//   - we also allow our Gossip and our connection health Ping service
//     → 同时允许 Gossip 服务本身以及节点连接健康检查（Ping 服务）正常工作。
//
//   - everything else returns Unavailable errors (which are retryable)
//     → 其他所有服务暂时返回 gRPC 的 Unavailable 错误（这种错误是可重试的），防止客户端过早使用未就绪的服务。
//
//   - once the node has started, unlock all RPCs.
//     → 一旦节点完全启动成功，就解锁所有 RPC 服务，让它们正常响应。

// The passed context can be used to trace the server startup. The context
// should represent the general startup operation.
// 传入的 context 可用于追踪服务器启动过程。该 context 应该代表整个启动操作的 tracing span。
func (s *topLevelServer) PreStart(ctx context.Context) error {
	ctx = s.AnnotateCtx(ctx)
	done := startup.Begin(ctx)
	defer done()

	// The following initialization is mirrored in
	// (*SQLServerWrapper).PreStart. Please keep them in sync.

	// Start a context for the asynchronous network workers.
	workersCtx := s.AnnotateCtx(context.Background())

	if !s.cfg.Insecure {
		cm, err := s.rpcContext.GetCertificateManager()
		if err != nil {
			return err
		}
		// Ensure that SIGHUP will make this cert manager reload its certs
		// from disk.
		if err := cm.RegisterSignalHandler(workersCtx, s.stopper); err != nil {
			return err
		}
	}

	// Start the time sanity checker.
	s.startTime = timeutil.Now()
	if err := s.startMonitoringForwardClockJumps(workersCtx); err != nil {
		return err
	}

	// Connect the node as loopback handler for RPC requests to the
	// local node.
	// 这段代码的作用：
	// 将当前节点自身的 KV 服务（s.node）注册为“本地内部服务器”（local internal server），
	// 让节点内部发往本节点的 RPC 请求可以**绕过 gRPC 网络层**，直接通过函数调用处理（loopback 模式）。
	// 这样可以大幅提升节点内部通信的性能，避免不必要的序列化、网络栈开销和上下文切换。
	s.rpcContext.SetLocalInternalServer(
		s.node,
		s.grpc.serverInterceptorsInfo, s.rpcContext.ClientInterceptors())

	// Load the TLS configuration for the HTTP server.
	uiTLSConfig, err := s.rpcContext.GetUIServerTLSConfig()
	if err != nil {
		return err
	}

	// Start the admin UI server. This opens the HTTP listen socket,
	// optionally sets up TLS, and dispatches the server worker for the
	// web UI.
	if err := startHTTPService(ctx,
		workersCtx, &s.cfg.BaseConfig, uiTLSConfig, s.stopper, s.serverController.httpMux); err != nil {
		return err
	}

	// Filter out self from the gossip bootstrap addresses.
	// 这行代码的作用：
	// 在启动节点时，从用户提供的 Gossip bootstrap 地址列表（--join 参数指定的地址）中，
	// 过滤掉“自己”的地址（即本节点的监听地址或广告地址），
	// 防止节点尝试“加入自己”（join itself），避免无意义的自我连接或循环。
	filtered := s.cfg.FilterGossipBootstrapAddresses(ctx)

	// Set up the init server. We have to do this relatively early because we
	// can't call RegisterInitServer() after `grpc.Serve`, which is called in
	// startRPCServer (and for the loopback grpc-gw connection).
	var initServer *initServer
	{
		getGRPCDialOpts := s.rpcContext.GRPCDialOptions
		getDRPCDialOpts := s.rpcContext.DRPCDialOptions
		initConfig := newInitServerConfig(ctx, s.cfg, getGRPCDialOpts, getDRPCDialOpts)
		inspectedDiskState, err := inspectEngines(
			ctx,
			s.engines,
			s.cfg.Settings.Version.LatestVersion(),
			s.cfg.Settings.Version.MinSupportedVersion(),
		)
		if err != nil {
			return err
		}

		initServer = newInitServer(s.cfg.AmbientCtx, inspectedDiskState, initConfig)
	}

	initialDiskClusterVersion := initServer.DiskClusterVersion()
	{
		// The invariant we uphold here is that any version bump needs to be
		// persisted on all engines before it becomes "visible" to the version
		// setting. To this end, we:
		//
		// a) write back the disk-loaded cluster version to all engines,
		// b) initialize the version setting (using the disk-loaded version).
		//
		// Note that "all engines" means "all engines", not "all initialized
		// engines". We cannot initialize engines this early in the boot
		// sequence.
		//
		// The version setting loaded from disk is the maximum cluster version
		// seen on any engine. If new stores are being added to the server right
		// now, or if the process crashed earlier half-way through the callback,
		// that version won't be on all engines. For that reason, we backfill
		// once.
		//1. 核心逻辑拆解：为什么要这么做？
		//在分布式数据库中，一个节点可能挂载了多个磁盘（Engines）。如果节点在版本升级的过程中突然断电，可能会出现“版本分裂”：有的盘记着新版本，有的盘记着旧版本。
		//
		//为了解决这个问题，代码执行了两个关键步骤：
		//
		//A. 回写补全 (Backfill/Write-back)
		//逻辑：不管之前发生了什么，先把从磁盘扫描到的“最大版本”重新强行写入所有挂载的引擎。
		//
		//目的：修复可能存在的“版本不一”情况。即使是新加入的空硬盘，也要立刻标记上当前集群的版本。
		if err := kvstorage.WriteClusterVersionToEngines(
			ctx, s.engines, initialDiskClusterVersion,
		); err != nil {
			return err
		}

		// Note that at this point in the code we don't know if we'll bootstrap
		// or join an existing cluster, so we have to conservatively go with the
		// version from disk. If there are no initialized engines, this is the
		// binary min supported version.
		//B. 初始化内存变量 (Initialize)
		//逻辑：将这个确定后的物理版本加载到内存中的全局设置（Settings.SV）里。
		//
		//目的：从此开始，整个程序运行时的逻辑（比如是否支持某种新的 SQL 语法）将以此版本为准。
		if err := clusterversion.Initialize(ctx, initialDiskClusterVersion.Version, &s.cfg.Settings.SV); err != nil {
			return err
		}

		// At this point, we've established the invariant: all engines hold the
		// version currently visible to the setting. Going forward whenever we
		// set an active cluster version (`SetActiveVersion`), we'll
		// persist it to all the engines first (`WriteClusterVersionToEngines`).
		// This happens at two places:
		//
		// - Right below, if we learn that we're the bootstrapping node, given
		//   we'll be setting the active cluster version as the binary version.
		// - Within the BumpClusterVersion RPC, when we're informed by another
		//   node what our new active cluster version should be.
	}

	serverpb.RegisterInitServer(s.grpc.Server, initServer)
	if err := serverpb.DRPCRegisterInit(s.drpc.DRPCServer, initServer); err != nil {
		return err
	}

	// Register the Migration service, to power internal crdb upgrades.
	migrationServer := &migrationServer{server: s}
	serverpb.RegisterMigrationServer(s.grpc.Server, migrationServer)
	if err := serverpb.DRPCRegisterMigration(s.drpc.DRPCServer, migrationServer); err != nil {
		return err
	}
	s.migrationServer = migrationServer // only for testing via testServer

	// Register the KeyVisualizer Server
	keyvispb.RegisterKeyVisualizerServer(s.grpc.Server, s.keyVisualizerServer)
	if err := keyvispb.DRPCRegisterKeyVisualizer(s.drpc.DRPCServer, s.keyVisualizerServer); err != nil {
		return err
	}

	// Start the RPC server. This opens the RPC/SQL listen socket,
	// and dispatches the server worker for the RPC.
	// The SQL listener is returned, to start the SQL server later
	// below when the server has initialized.
	// 启动 RPC 和 SQL 监听器
	pgL, loopbackPgL, grpcLoopbackDialFn, drpcLoopbackDialFn, startRPCServer, err := startListenRPCAndSQL(
		ctx, workersCtx, s.cfg.BaseConfig,
		s.stopper, s.grpc, s.drpc, ListenAndUpdateAddrs, true /* enableSQLListener */, s.cfg.AcceptProxyProtocolHeaders)
	if err != nil {
		return err
	}

	// 保存 PostgreSQL 网络监听器和本地 loopback 监听器
	s.pgL = pgL
	s.loopbackPgL = loopbackPgL

	// 设置 RPC 上下文的 loopback dialer，用于内存中直接连接 gRPC 和 DRPC
	s.rpcContext.SetLoopbackDialer(grpcLoopbackDialFn)
	s.rpcContext.SetLoopbackDRPCDialer(drpcLoopbackDialFn)

	// 如果开启了测试钩子（TestingKnobs），处理测试相关的同步逻辑
	if s.cfg.TestingKnobs.Server != nil {
		knobs := s.cfg.TestingKnobs.Server.(*TestingKnobs)

		// 如果配置了 SignalAfterGettingRPCAddress，说明测试需要等待 RPC 地址准备好
		if knobs.SignalAfterGettingRPCAddress != nil {
			log.Dev.Infof(ctx, "signaling caller that RPC address is ready")
			close(knobs.SignalAfterGettingRPCAddress) // 通知测试代码 RPC 地址已经准备好
		}

		// 如果配置了 PauseAfterGettingRPCAddress，则等待外部信号再继续初始化
		if knobs.PauseAfterGettingRPCAddress != nil {
			log.Dev.Infof(ctx, "waiting for signal from caller to proceed with initialization")
			select {
			case <-knobs.PauseAfterGettingRPCAddress:
				// 正常情况，收到信号，继续初始化

			case <-ctx.Done():
				// 上下文取消（可能是测试超时或其他条件），提前停止
				return errors.CombineErrors(errors.New("server stopping prematurely from context shutdown"), ctx.Err())

			case <-s.stopper.ShouldQuiesce():
				// Stopper 发出停止指令，服务器在启动完成前就被要求停止
				return errors.New("server stopping prematurely")
			}
			log.Dev.Infof(ctx, "caller is letting us proceed with initialization")
		}
	}

	// 初始化 grpc-gateway 的 mux 和上下文，以便 /health 接口在节点完全初始化前也能工作
	gwMux, gwCtx, conn, err := configureGRPCGateway(
		ctx,
		workersCtx,
		s.cfg.AmbientCtx,
		s.rpcContext,
		s.stopper,
		s.cfg.AdvertiseAddr,
	)
	if err != nil {
		return err
	}

	// 将各个 RPC 处理器注册到 grpc-gateway，确保它们能通过 HTTP/JSON 访问
	for _, gw := range []grpcGatewayServer{s.admin, s.status, s.authentication, s.tsServer} {
		if err := gw.RegisterGateway(gwCtx, gwMux, conn); err != nil {
			return err
		}
	}

	// 提前处理 /health 接口
	// 这个接口用于集群编排（orchestration）和健康检查
	// 注意 /health 接口没有认证，这样它在集群未完全启动时也能返回
	// 另外，它还需要兼容不认证的监控工具
	s.http.handleHealth(gwMux)
	// 1. 提前写入监听器信息文件，用于记录 RPC、SQL、HTTP 的监听地址。
	// listenerInfo 是一个封装了文件名与对应值的结构
	listenerFiles := listenerInfo{
		listenRPC:    s.cfg.Addr,
		advertiseRPC: s.cfg.AdvertiseAddr,
		listenSQL:    s.cfg.SQLAddr,
		advertiseSQL: s.cfg.SQLAdvertiseAddr,
		listenHTTP:   s.cfg.HTTPAdvertiseAddr,
	}.Iter() // Iter() 返回一个 map[string]string: 文件名 -> 值

	encryptedStore := false                        // 标记是否存在加密存储
	for _, storeSpec := range s.cfg.Stores.Specs { // 遍历每个存储配置
		if storeSpec.InMemory {
			continue // 内存存储不写文件
		}
		if storeSpec.IsEncrypted() {
			encryptedStore = true // 发现加密存储
		}

		// 遍历监听器文件并写入当前存储路径
		for name, val := range listenerFiles {
			file := filepath.Join(storeSpec.Path, name)
			if err := os.WriteFile(file, []byte(val), 0644); err != nil {
				return errors.Wrapf(err, "failed to write %s", file)
			}
		}
		// TODO: 是否需要在每个存储目录都写监听器文件？还是只写第一个目录即可
	}

	// 如果配置了 DelayedBootstrapFn，延迟执行 30 秒后再停止
	if s.cfg.DelayedBootstrapFn != nil {
		defer time.AfterFunc(30*time.Second, s.cfg.DelayedBootstrapFn).Stop()
	}

	// 2. 自行初始化集群
	selfBootstrap := s.cfg.AutoInitializeCluster && initServer.NeedsBootstrap()
	if selfBootstrap {
		if _, err := initServer.Bootstrap(ctx, &serverpb.BootstrapRequest{}); err != nil {
			return err
		}
	}

	// 3. 设置 Ready 回调函数
	// 用于控制 `cockroach start --background` 命令何时返回
	var onSuccessfulReturnFn, onInitServerReady func()
	{
		readyFn := func(bool) {}
		if s.cfg.ReadyFn != nil {
			readyFn = s.cfg.ReadyFn
		}

		// 如果不需要 bootstrap 或已经自启动，则立即调用 ready
		if !initServer.NeedsBootstrap() || selfBootstrap {
			onSuccessfulReturnFn = func() { readyFn(false /* waitForInit */) }
			onInitServerReady = func() {}
		} else { // 需要加入已有集群
			onSuccessfulReturnFn = func() {}
			onInitServerReady = func() { readyFn(true /* waitForInit */) }
		}
	}

	// 4. 打开主监听器，开启 RPC 服务
	startRPCServer(workersCtx)
	onInitServerReady() // 当监听器打开后，可调用 ready 回调

	// 启动初始化服务器，并等待其完成初始化
	state, initialStart, err := initServer.ServeAndWait(workersCtx, s.stopper, &s.cfg.Settings.SV)
	if err != nil {
		return errors.Wrap(err, "during init")
	}

	// 验证初始化状态是否合法
	if err := state.validate(); err != nil {
		return errors.Wrap(err, "invalid init state")
	}

	// 5. 尽早应用初始缓存的系统设置，并启动 gossip 监听器
	if err := initializeCachedSettings(
		ctx, keys.SystemSQLCodec, s.st.MakeUpdater(), state.initialSettingsKVs,
	); err != nil {
		return errors.Wrap(err, "during initializing settings updater")
	}

	// 6. 检查集群版本是否与磁盘上的初始版本不同
	if state.clusterVersion != initialDiskClusterVersion {
		// 如果不同，说明是新启动节点或加入已有集群，需要更新集群版本
		// 先写入存储引擎
		if err := kvstorage.WriteClusterVersionToEngines(
			ctx, s.engines, state.clusterVersion,
		); err != nil {
			return err
		}

		// 再更新内存中 ClusterSettings 的 active 版本
		if err := s.ClusterSettings().Version.SetActiveVersion(ctx, state.clusterVersion); err != nil {
			return err
		}
	}
	s.rpcContext.StorageClusterID.Set(ctx, state.clusterID)
	s.rpcContext.NodeID.Set(ctx, state.nodeID)

	// Ensure components in the DistSQLPlanner that rely on the node ID are
	// initialized before store startup continues.
	//
	// 中文翻译：
	// 确保 DistSQLPlanner 中那些依赖 node ID 的组件，
	// 在 store（存储层）继续启动之前就已经完成初始化。
	s.sqlServer.execCfg.DistSQLPlanner.SetGatewaySQLInstanceID(base.SQLInstanceID(state.nodeID))
	s.sqlServer.execCfg.DistSQLPlanner.ConstructAndSetSpanResolver(ctx, state.nodeID, s.cfg.Locality)

	// TODO(irfansharif): Now that we have our node ID, we should run another
	// check here to make sure we've not been decommissioned away (if we're here
	// following a server restart). See the discussions in #48843 for how that
	// could be done, and what's motivating it.
	//
	// 中文翻译：
	// TODO：既然现在我们已经拿到了自己的 node ID，
	// 那么（如果这是一次服务器重启）此处应该再做一次检查，
	// 确保该节点没有被“下线 / 退役（decommission）”。
	// 具体如何实现，以及背后的动机，可以参考 issue #48843 中的讨论。
	//
	// In summary: We'd consult our local store keys to see if they contain a
	// kill file informing us we've been decommissioned away (the
	// decommissioning process, that prefers to decommission live targets, will
	// inform the target node to persist such a file).
	//
	// 中文翻译：
	// 总结一下：
	// 我们会查看本地 store 中的元数据（keys），
	// 看是否存在一个“kill 文件”，
	// 这个文件用于告知当前节点：你已经被 decommission 了。
	// 在 decommission 流程中（优先对在线节点执行），
	// 系统会要求目标节点将这个文件持久化到本地。
	//
	// Short of that, if we were decommissioned in absentia, we'd attempt to
	// reach out to already connected nodes in our join list to see if they have
	// any knowledge of our node ID being decommissioned. This is something the
	// decommissioning node will broadcast (best-effort) to cluster if the
	// target node is unavailable, and is only done with the operator guarantee
	// that this node is indeed never coming back. If we learn that we're not
	// decommissioned, we'll solicit the decommissioned list from the already
	// connected node to be able to respond to inbound decomm check requests.
	//
	// 中文翻译：
	// 如果没有本地 kill 文件（例如：节点当时不在线，被“缺席退役”），
	// 那我们就会尝试联系 join 列表中已经连接上的其他节点，
	// 询问它们是否知道“我们的 node ID 已被 decommission”。
	//
	// 当目标节点不可用时，执行 decommission 的节点会以“尽力而为”的方式，
	// 向整个集群广播该节点已被退役的信息。
	// 这类操作只会在运维人员确信该节点永远不会再回来的前提下进行。
	//
	// 如果我们确认自己并没有被 decommission，
	// 那我们还会从已连接的节点那里拉取一份
	// “已退役节点列表”，
	// 以便将来能正确响应其他节点发来的 decommission 校验请求。
	//
	// As for the problem of the ever growing list of decommissioned node IDs
	// being maintained on each node, given that we're populating+broadcasting
	// this list in best effort fashion (like said above, we're relying on the
	// operator to guarantee that the target node is never coming back), perhaps
	// it's also fine for us to age out the node ID list we maintain if it gets
	// too large. Though even maintaining a max of 64 MB of decommissioned node
	// IDs would likely outlive us all
	//
	//   536,870,912 bits/64 bits = 8,388,608 decommissioned node IDs.
	//
	// 中文翻译：
	// 至于“每个节点都要维护一个不断增长的已退役 node ID 列表”这个问题：
	// 考虑到我们是以“尽力而为”的方式来维护和广播这个列表，
	// 并且依赖运维人员保证这些节点确实不会再回来，
	// 那么当列表变得过大时，对其进行老化淘汰（age out）可能也是可以接受的。
	//
	// 即便我们最多维护 64MB 的退役 node ID 列表，
	// 这个规模大概也足够“用到天荒地老”了：
	// 536,870,912 bits / 64 bits = 8,388,608 个已退役节点 ID。

	// TODO(tbg): split this method here. Everything above this comment is
	// the early stage of startup -- setting up listeners and determining the
	// initState -- and everything after it is actually starting the server,
	// using the listeners and init state.
	//
	// 中文翻译：
	// TODO：这里应该把这个方法拆分开。
	// 这个注释之上的代码属于“启动早期阶段”：
	//   - 设置监听器
	//   - 确定 initState
	// 而注释之后的代码才是真正开始启动服务器，
	// 并使用前面准备好的监听器和初始化状态。

	// Spawn a goroutine that will print a nice message when Gossip connects.
	// Note that we already know the clusterID, but we don't know that Gossip
	// has connected. The pertinent case is that of restarting an entire
	// cluster. Someone has to gossip the ClusterID before Gossip is connected,
	// but this gossip only happens once the first range has a leaseholder, i.e.
	// when a quorum of nodes has gone fully operational.
	// 中文翻译：
	// 启动一个 goroutine，在 Gossip 网络成功连接时打印一条友好的日志。
	// 注意：此时我们已经知道 clusterID 了，
	// 但我们还不知道 Gossip 是否已经真正建立连接。
	// 一个典型场景是“整个集群同时重启”：
	// 在 Gossip 连接建立之前，必须先有节点把 ClusterID 通过 Gossip 传播出去。
	// 而这件事只有在第一个 range 拥有 leaseholder 之后才会发生，
	// 也就是说，必须等到至少一个 quorum 的节点完全启动并可用。
	//① 已知 clusterID ≠ 集群已可用
	//本节点：
	//已经知道 clusterID
	//已经知道 nodeID
	//但 Gossip 可能还没连上
	//没有 Gossip：
	//节点之间不知道彼此
	//元数据无法传播
	//集群逻辑上还没“连成一张网”
	//② 为什么「整个集群重启」时会卡在这一步？
	//这是注释里最重要的真实场景：
	//restarting an entire cluster
	//流程是这样的：
	//所有节点同时启动
	//没有任何节点在 Gossip 网络里
	//Gossip 需要 ClusterID 被 gossip 出去
	//但：
	//只有当某个 range 有了 leaseholder
	//leaseholder 又需要 raft quorum 可用
	//所以：
	//至少要有 一组节点 fully operational
	//才能出现第一个 leaseholder
	//才能开始 gossip ClusterID
	//Gossip 才能真正 Connected
	//👉 这是一个“鸡生蛋、蛋生鸡”的启动闭环
	//③ 为什么要起一个 goroutine 专门等 Gossip？
	//目的只有一个：
	//告诉运维 / 日志系统：
	//“现在这个节点，已经真正接入集群了”
	//不是功能必须，而是：
	//启动可观测性
	//排错体验
	//集群健康确认
	_ = s.stopper.RunAsyncTask(workersCtx, "connect-gossip", func(ctx context.Context) {
		log.Ops.Infof(ctx, "connecting to gossip network to verify cluster ID %q", state.clusterID)
		select {
		case <-s.gossip.Connected:
			log.Ops.Infof(ctx, "node connected via gossip")
		case <-ctx.Done():
		case <-s.stopper.ShouldQuiesce():
		}
	})

	// Start measuring the Go scheduler latency.
	// 监视p99延迟来对准入控制进行校准
	if err := schedulerlatency.StartSampler(
		workersCtx, s.st, s.stopper, s.sysRegistry, base.DefaultMetricsSampleInterval,
		// Wire up admission control's scheduler latency listener.
		s.node.storeCfg.SchedulerLatencyListener,
	); err != nil {
		return err
	}

	// Check that the HLC clock is only moving forward.
	//保证hlc上界一直在往前走，如果出现较大的差距那就panic
	hlcUpperBoundExists, err := s.checkHLCUpperBoundExistsAndEnsureMonotonicity(ctx, initialStart)
	if err != nil {
		return err
	}

	// Record a walltime that is lower than the lowest hlc timestamp this current
	// instance of the node can use. We do not use startTime because it is lower
	// than the timestamp used to create the bootstrap schema.
	//
	// TODO(tbg): clarify the contract here and move closer to usage if possible.
	orphanedLeasesTimeThresholdNanos := s.clock.Now().WallTime

	onSuccessfulReturnFn()

	// NB: This needs to come after `startListenRPCAndSQL`, which determines
	// what the advertised addr is going to be if nothing is explicitly
	// provided.
	advAddrU := util.NewUnresolvedAddr("tcp", s.cfg.AdvertiseAddr)

	// We're going to need to start gossip before we spin up Node below.
	//启动gossip，开始连接 加入集群
	s.gossip.Start(advAddrU, filtered, s.rpcContext)
	log.Event(ctx, "started gossip")

	// Now that we have a monotonic HLC wrt previous incarnations of the process,
	// init all the replicas. At this point *some* store has been initialized or
	// we're joining an existing cluster for the first time.
	advSQLAddrU := util.NewUnresolvedAddr("tcp", s.cfg.SQLAdvertiseAddr)

	advHTTPAddrU := util.NewUnresolvedAddr("tcp", s.cfg.HTTPAdvertiseAddr)

	// Registers an event log writer for the system tenant. This will enable the
	// ability to persist structured events to the system tenant's
	//system.eventlog table.
	//`eventlog.Register`函数是CockroachDB事件日志系统的初始化入口，主要解决以下问题：
	//1. **集中式事件管理**：为系统中的重要事件提供统一的存储和查询接口
	//2. **持久化机制**：将结构化事件持久化存储到`system.eventlog`表中
	//3. **测试控制**：提供测试钩子控制写入行为（同步/异步）
	eventlog.Register(ctx, s.cfg.TestingKnobs.EventLog, s.node.execCfg.InternalDB, s.stopper, s.cfg.AmbientCtx, s.ClusterSettings())
	//node 启动
	if err := s.node.start(
		ctx, workersCtx,
		advAddrU,
		advSQLAddrU,
		advHTTPAddrU,
		*state,
		initialStart,
		s.cfg.ClusterName,
		s.cfg.NodeAttributes,
		s.cfg.Locality,
		s.cfg.LocalityAddresses,
	); err != nil {
		return err
	}

	log.Event(ctx, "started node")
	//周期性的更新HLC的上界
	if err := s.startPersistingHLCUpperBound(ctx, hlcUpperBoundExists); err != nil {
		return err
	}
	s.replicationReporter.Start(workersCtx, s.stopper)

	// Configure the Sentry reporter to add some additional context to reports.
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTags(map[string]string{
			"cluster":         s.StorageClusterID().String(),
			"node":            s.NodeID().String(),
			"server_id":       fmt.Sprintf("%s-%s", s.StorageClusterID().Short(), s.NodeID()),
			"encrypted_store": strconv.FormatBool(encryptedStore),
		})
	})

	// Init a log metrics registry.
	logRegistry := logmetrics.NewRegistry()
	if logRegistry == nil {
		panic(errors.AssertionFailedf("nil log metrics registry at server startup"))
	}

	// We can now connect the metric registries to the recorder.
	s.recorder.AddNode(
		s.nodeRegistry, s.appRegistry,
		logRegistry, s.sysRegistry,
		s.node.Descriptor,
		s.node.startedAt,
		s.cfg.AdvertiseAddr,
		s.cfg.HTTPAdvertiseAddr,
		s.cfg.SQLAdvertiseAddr,
	)

	// Begin recording runtime statistics.
	if err := startSampleEnvironment(workersCtx,
		&s.cfg.BaseConfig,
		s.cfg.CacheSize,
		s.stopper,
		s.runtime,
		s.status.sessionRegistry,
		s.sqlServer.execCfg.RootMemoryMonitor,
	); err != nil {
		return err
	}

	// Begin recording time series data collected by the status monitor.
	// The writes will be async; we'll wait for the first one to go through
	// later in this method, using the returned channel.
	firstTSDBPollDone := s.tsDB.PollSource(
		s.cfg.AmbientCtx, s.recorder, base.DefaultMetricsSampleInterval, ts.Resolution10s, s.stopper, false,
	)

	// high cardinality child metrics collector, we don't need to wait for the first one
	s.tsDB.PollSource(
		s.cfg.AmbientCtx, s.recorder, base.DefaultHighCardinalityMetricsSampleInterval, ts.Resolution1m, s.stopper, true,
	)

	// Export statistics to graphite, if enabled by configuration.
	var graphiteOnce sync.Once
	graphiteEndpoint.SetOnChange(&s.st.SV, func(context.Context) {
		if graphiteEndpoint.Get(&s.st.SV) != "" {
			graphiteOnce.Do(func() {
				startGraphiteStatsExporter(workersCtx, s.stopper, s.recorder, s.st)
			})
		}
	})

	// Start the protected timestamp subsystem. Note that this needs to happen
	// before the modeOperational switch below, as the protected timestamps
	// subsystem will crash if accessed before being Started (and serving general
	// traffic may access it).
	//
	// See https://github.com/cockroachdb/cockroach/issues/73897.
	if err := s.protectedtsProvider.Start(workersCtx, s.stopper); err != nil {
		// TODO(knz,arul): This mechanism could probably be removed now.
		// The PTS Cache is a thing from the past when secondary tenants
		// couldn’t use protected timestamps. We started using span configs
		// (in both the system and secondary tenants) to store PTS
		// information in 22.1, at which point the PTS cache was only kept
		// around to migrate between the old and new subsystems.
		return err
	}

	// After setting modeOperational, we can block until all stores are fully
	// initialized.
	s.grpc.setMode(modeOperational)
	s.drpc.setMode(modeOperational)

	s.nodeLiveness.Start(workersCtx)

	// We'll block here until all stores are fully initialized. We do this here
	// for several reasons:
	// - some of the components below depend on all stores being fully
	//   initialized (like the debug server registration for e.g.)
	// - we'll need to do it after having opened up the RPC floodgates (due to
	//   the hazard described in Node.start, around initializing additional
	//   stores)
	// - we'll need to do it after starting node liveness, see:
	//   https://github.com/cockroachdb/cockroach/issues/106706#issuecomment-1640254715
	s.node.waitForAdditionalStoreInit()

	// Connect the engines to the disk stats map constructor. This needs to
	// wait until after waitForAdditionalStoreInit returns since it realizes on
	// wholly initialized stores (it reads the StoreIdentKeys). It also needs
	// to come before the call into SetPebbleMetricsProvider, which internally
	// uses the disk stats map we're initializing.
	var pmp admission.PebbleMetricsProvider
	if pmp, err = s.node.registerEnginesForDiskStatsMap(
		s.cfg.Stores.Specs, s.engines, (*diskMonitorManager)(s.cfg.DiskMonitorManager)); err != nil {
		return errors.Wrapf(err, "failed to register engines for the disk stats map")
	}

	// Set up a store metrics registry provider to register AC store-level
	// metrics.
	mrp := s.node.makeStoreRegistryProvider()

	// Stores have been initialized, so Node can now provide Pebble metrics.
	//
	// Note that all existing stores will be operational before Pebble-level
	// admission control is online. However, we won’t have started to heartbeat
	// our liveness record until after we call SetPebbleMetricsProvider, so the
	// existing stores shouldn’t be able to acquire leases yet. Although, below
	// Raft commands like log application and snapshot application may be able
	// to bypass admission control.
	s.storeGrantCoords.SetPebbleMetricsProvider(ctx, pmp, mrp, s.node)

	// Once all stores are initialized, check if offline storage recovery
	// was done prior to start and record any actions appropriately.
	logPendingLossOfQuorumRecoveryEvents(workersCtx, s.node.stores)

	// Report server listen addresses to logs.
	log.Ops.Infof(ctx, "starting %s server at %s (use: %s)",
		redact.Safe(s.cfg.HTTPRequestScheme()), log.SafeManaged(s.cfg.HTTPAddr), log.SafeManaged(s.cfg.HTTPAdvertiseAddr))
	rpcConnType := redact.SafeString("grpc/postgres")
	if s.cfg.SplitListenSQL {
		rpcConnType = "grpc"
		log.Ops.Infof(ctx, "starting postgres server at %s (use: %s)",
			log.SafeManaged(s.cfg.SQLAddr), log.SafeManaged(s.cfg.SQLAdvertiseAddr))
	}
	log.Ops.Infof(ctx, "starting %s server at %s", log.SafeManaged(rpcConnType), log.SafeManaged(s.cfg.Addr))
	log.Ops.Infof(ctx, "advertising CockroachDB node at %s", log.SafeManaged(s.cfg.AdvertiseAddr))

	log.Event(ctx, "accepting connections")

	// Begin recording status summaries.
	if err := s.node.startWriteNodeStatus(base.DefaultMetricsSampleInterval); err != nil {
		return err
	}

	if subscriber, ok := s.spanConfigSubscriber.(*spanconfigkvsubscriber.KVSubscriber); ok {
		if err := subscriber.Start(workersCtx, s.stopper); err != nil {
			return err
		}
	}

	// Record node start in telemetry. Get the right counter for this storage
	// engine type as well as type of start (initial boot vs restart).
	nodeStartCounter := "storage.engine.pebble."
	if s.InitialStart() {
		nodeStartCounter += "initial-boot"
	} else {
		nodeStartCounter += "restart"
	}
	telemetry.Count(nodeStartCounter)

	// Record that this node joined the cluster in the event log. Since this
	// executes a SQL query, this must be done after the SQL layer is ready.
	s.node.recordJoinEvent(ctx)

	if !s.cfg.DisableSQLServer {
		// Start the SQL subsystem.
		if err := s.sqlServer.preStart(
			workersCtx,
			s.stopper,
			s.cfg.TestingKnobs,
			orphanedLeasesTimeThresholdNanos,
		); err != nil {
			return err
		}
	}
	var apiInternalServer http.Handler
	var drpcEnabled = false
	if rpcbase.TODODRPC && rpcbase.DRPCEnabled(ctx, s.cfg.Settings) {
		drpcEnabled = true
		// Pass our own node ID to connect to local RPC servers
		apiInternalServer, err = apiinternal.NewAPIInternalServer(
			ctx, s.kvNodeDialer, s.rpcContext.NodeID.Get(), s.cfg.Settings)
		if err != nil {
			return err
		}
	} else {
		apiInternalServer = gwMux
	}

	// Connect the HTTP endpoints. This also wraps the privileged HTTP
	// endpoints served by gwMux by the HTTP cookie authentication
	// check.
	// NB: This must occur after sqlServer.preStart() which initializes
	// the cluster version from storage as the http auth server relies on
	// the cluster version being initialized.
	if err := s.http.setupRoutes(ctx,
		s.sqlServer.ExecutorConfig(), /* execCfg */
		s.authentication,             /* authnServer */
		s.admin.adminServer,          /* adminServer */
		s.adminAuthzCheck,            /* adminAuthzCheck */
		s.recorder,                   /* metricSource */
		s.runtime,                    /* runtimeStatsSampler */
		apiInternalServer,            /* unauthenticatedAPIInternalServer */
		s.debug,                      /* handleDebugUnauthenticated */
		s.inspectzServer,             /* handleInspectzUnauthenticated */
		newAPIV2Server(ctx, &apiV2ServerOpts{
			admin:            s.admin,
			status:           s.status,
			promRuleExporter: s.promRuleExporter,
			sqlServer:        s.sqlServer,
			db:               s.db,
		}), /* apiServer */
		serverpb.FeatureFlags{
			CanViewKvMetricDashboards:   s.rpcContext.TenantID.Equal(roachpb.SystemTenantID),
			DisableKvLevelAdvancedDebug: false,
		},
		drpcEnabled,
	); err != nil {
		return err
	}

	// Start garbage collecting system events.
	if err := startSystemLogsGC(workersCtx, s.sqlServer); err != nil {
		return err
	}

	// Initialize the external storage builders configuration params now that the
	// engines have been created. The object can be used to create ExternalStorage
	// objects hereafter.
	ieMon := sql.MakeInternalExecutorMemMonitor(sql.MemoryMetrics{}, s.ClusterSettings())
	ieMon.StartNoReserved(ctx, s.PGServer().SQLServer.GetBytesMonitor())
	s.stopper.AddCloser(stop.CloserFn(func() { ieMon.Stop(ctx) }))
	s.externalStorageBuilder.init(
		s.cfg.EarlyBootExternalStorageAccessor,
		s.cfg.ExternalIODirConfig,
		s.st,
		s.sqlServer.sqlIDContainer,
		s.kvNodeDialer,
		s.cfg.TestingKnobs,
		true, /* allowLocalFastPath */
		s.sqlServer.execCfg.InternalDB.CloneWithMemoryMonitor(sql.MemoryMetrics{}, ieMon),
		nil, /* TenantExternalIORecorder */
		s.appRegistry,
		s.cfg.ExternalIODir,
	)

	if err := s.runIdempontentSQLForInitType(ctx, state.initType); err != nil {
		return err
	}

	// Start the job scheduler now that the SQL Server and
	// external storage is initialized.
	if err := s.initJobScheduler(ctx); err != nil {
		return err
	}

	// If enabled, start reporting diagnostics.
	if s.cfg.StartDiagnosticsReporting && !cluster.TelemetryOptOut {
		s.startDiagnostics(workersCtx)
	}

	if storage.WorkloadCollectorEnabled {
		if err := s.debug.RegisterWorkloadCollector(s.node.stores); err != nil {
			return errors.Wrapf(err, "failed to register workload collector with debug server")
		}
	}

	// Register the engines debug endpoints.
	if err := s.debug.RegisterEngines(s.engines); err != nil {
		return errors.Wrapf(err, "failed to register engines with debug server")
	}

	// Register the ctc debug endpoints.
	s.debug.RegisterClosedTimestampSideTransport(s.ctSender, s.node.storeCfg.ClosedTimestampReceiver)

	// Start the closed timestamp loop.
	s.ctSender.Run(workersCtx, state.nodeID)

	// Start the closed timestamp policy refresher in the background. It refreshes
	// closed timestamp policies for ranges periodically.
	s.policyRefresher.Run(workersCtx)

	// Start node capacity provider in the background. It refreshes node cpu usage
	// and capacity for store descriptor.
	s.nodeCapacityProvider.Run(workersCtx)

	// Start dispatching extant flow tokens.
	if err := s.raftTransport.Start(workersCtx); err != nil {
		return err
	}

	// Attempt to upgrade cluster version now that the sql server has been
	// started. At this point we know that all startupmigrations and permanent
	// upgrades have successfully been run so it is safe to upgrade to the
	// binary's current version.
	//
	// NB: We run this under the startup ctx (not workersCtx) so as to ensure
	// all the upgrade steps are traced, for use during troubleshooting.
	if err := s.startAttemptUpgrade(ctx); err != nil {
		return errors.Wrap(err, "cannot start upgrade task")
	}

	// Initialize the key visualizer boundary subscriber rangefeed,
	// and start the rangefeed to broadcast updates to the collector.
	if err := keyvissubscriber.Start(
		ctx,
		s.stopper,
		s.db,
		s.ClusterSettings(),
		s.sqlServer.execCfg.SystemTableIDResolver,
		s.clock.Now(),
		func(update *keyvispb.UpdateBoundariesRequest) {
			s.node.spanStatsCollector.SaveBoundaries(update.Boundaries, update.Time)
		}); err != nil {
		return err
	}

	startSettingsWatcher := true
	if serverKnobs := s.cfg.TestingKnobs.Server; serverKnobs != nil {
		if serverKnobs.(*TestingKnobs).DisableSettingsWatcher {
			startSettingsWatcher = false
		}
	}
	if startSettingsWatcher {
		if err := s.node.tenantSettingsWatcher.Start(workersCtx, s.sqlServer.execCfg.SystemTableIDResolver); err != nil {
			return errors.Wrap(err, "failed to initialize the tenant settings watcher")
		}
	}
	if err := s.tenantCapabilitiesWatcher.Start(ctx); err != nil {
		return errors.Wrap(err, "initializing tenant capabilities")
	}
	// Now that we've got the tenant capabilities subsystem all started, we bind
	// the Reader to the TenantRPCAuthorizer, so that it has a handle into the
	// global tenant capabilities state.
	s.rpcContext.TenantRPCAuthorizer.BindReader(s.tenantCapabilitiesWatcher)

	if err := s.kvProber.Start(workersCtx, s.stopper); err != nil {
		return errors.Wrapf(err, "failed to start KV prober")
	}

	// Perform loss of quorum recovery cleanup if any actions were scheduled.
	// Cleanup actions rely on node being connected to the cluster and hopefully
	// in a healthy or healthier stats to update node liveness records.
	maybeRunLossOfQuorumRecoveryCleanup(
		ctx,
		s.node.execCfg.InternalDB.Executor(),
		s.node.stores,
		s,
		s.stopper)

	// Let the server controller start watching tenant service mode changes.
	if err := s.serverController.start(workersCtx,
		s.node.execCfg.InternalDB.Executor(),
	); err != nil {
		return errors.Wrap(err, "failed to start the server controller")
	}

	log.Event(ctx, "server initialized")

	// Wait for the first ts poll to have succeeded before acknowledging server
	// start. This helps with predictable tests.
	select {
	case <-s.stopper.ShouldQuiesce():
	case <-firstTSDBPollDone:
	}
	return maybeImportTS(ctx, s)
}

// initJobScheduler starts the job scheduler. This must be called
// after sqlServer.preStart and after our external storage providers
// have been initialized.
//
// TODO(ssd): We need to clean up the ordering/ownership here. The SQL
// server owns the job scheduler because the job scheduler needs an
// internal executor. But, the topLevelServer owns initialization of
// the external storage providers.
func (s *topLevelServer) initJobScheduler(ctx context.Context) error {
	if s.cfg.DisableSQLServer {
		return nil
	}
	// The job scheduler may immediately start jobs that require
	// external storage providers to be available. We expect the
	// server start up ordering to ensure this. Hitting this error
	// is a programming error somewhere in server startup.
	if err := s.externalStorageBuilder.assertInitComplete(); err != nil {
		return err
	}
	s.sqlServer.startJobScheduler(ctx, s.cfg.TestingKnobs)
	return nil
}

// runIdempontentSQLForInitType runs one-time initialization steps via
// SQL based on the given InitType.
func (s *topLevelServer) runIdempontentSQLForInitType(
	ctx context.Context, typ serverpb.InitType,
) error {
	if typ == serverpb.InitType_NONE || typ == serverpb.InitType_DEFAULT {
		return nil
	}

	initAttempt := func() error {
		const defaulVirtualClusterName = "main"
		switch typ {
		case serverpb.InitType_VIRTUALIZED:
			ie := s.sqlServer.execCfg.InternalDB.Executor()
			_, err := ie.Exec(ctx, "init-create-app-tenant", nil, /* txn */
				"CREATE VIRTUAL CLUSTER IF NOT EXISTS $1", defaulVirtualClusterName)
			if err != nil {
				return err
			}
			_, err = ie.Exec(ctx, "init-default-app-tenant", nil, /* txn */
				"ALTER VIRTUAL CLUSTER $1 START SERVICE SHARED", defaulVirtualClusterName)
			if err != nil {
				return err
			}
			fallthrough
		case serverpb.InitType_VIRTUALIZED_EMPTY:
			ie := s.sqlServer.execCfg.InternalDB.Executor()
			_, err := ie.Exec(ctx, "init-default-target-cluster-setting", nil, /* txn */
				"SET CLUSTER SETTING server.controller.default_target_cluster = $1", defaulVirtualClusterName)
			if err != nil {
				return err
			}
			_, err = ie.Exec(ctx, "init-rangefeed-enabled-cluster-setting", nil, /* txn */
				"SET CLUSTER SETTING kv.rangefeed.enabled = true")
			if err != nil {
				return err
			}
		default:
			return errors.Errorf("unknown bootstrap init type: %d", typ)
		}
		return nil
	}

	rOpts := retry.Options{
		MaxBackoff:     10 * time.Second,
		InitialBackoff: time.Second,
	}
	for r := retry.StartWithCtx(ctx, rOpts); r.Next(); {
		if err := initAttempt(); err != nil {
			log.Dev.Errorf(ctx, "cluster initialization attempt failed: %s", err.Error())
			continue
		}
		return nil
	}
	return errors.Errorf("cluster initialization failed; cluster may need to be manually configured")
}

// AcceptClients starts listening for incoming SQL clients over the network.
// This mirrors the implementation of (*SQLServerWrapper).AcceptClients.
// TODO(knz): Find a way to implement this method only once for both.
func (s *topLevelServer) AcceptClients(ctx context.Context) error {
	// Don't listen on the SQL port if the SQL Server is not starting.
	if s.cfg.DisableSQLServer {
		return nil
	}
	workersCtx := s.AnnotateCtx(context.Background())

	if err := startServeSQL(
		workersCtx,
		s.stopper,
		s.pgPreServer,
		s.serverController.sqlMux,
		s.pgL,
		s.ClusterSettings(),
		&s.cfg.SocketFile,
	); err != nil {
		return err
	}

	if err := structlogging.StartSystemHotRangesLogger(
		ctx,
		s.stopper,
		s.status,
		s.ClusterSettings(),
	); err != nil {
		return err
	}

	s.sqlServer.isReady.Store(true)

	log.Event(ctx, "server ready")
	return nil
}

// AcceptInternalClients starts listening for incoming SQL connections on the
// internal loopback interface.
func (s *topLevelServer) AcceptInternalClients(ctx context.Context) error {
	// Don't listen on the SQL port if the SQL Server is not starting.
	if s.cfg.DisableSQLServer {
		return nil
	}
	connManager := netutil.MakeTCPServer(ctx, s.stopper)

	return s.stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{TaskName: "sql-internal-listener", SpanOpt: stop.SterileRootSpan},
		func(ctx context.Context) {
			err := connManager.ServeWith(ctx, s.loopbackPgL, func(ctx context.Context, conn net.Conn) {
				connCtx := s.pgPreServer.AnnotateCtxForIncomingConn(ctx, conn)
				connCtx = logtags.AddTag(connCtx, "internal-conn", nil)

				conn, status, err := s.pgPreServer.PreServe(connCtx, conn, pgwire.SocketInternalLoopback)
				if err != nil {
					log.Ops.Errorf(connCtx, "serving SQL client conn: %v", err)
					return
				}
				defer status.ReleaseMemory(ctx)

				if err := s.serverController.sqlMux(connCtx, conn, status); err != nil {
					log.Ops.Errorf(connCtx, "serving internal SQL client conn: %s", err)
				}
			})
			netutil.FatalIfUnexpected(err)
		})
}

// ShutdownRequested returns a channel that is signaled when a subsystem wants
// the server to be shut down.
func (s *topLevelServer) ShutdownRequested() <-chan serverctl.ShutdownRequest {
	return s.stopTrigger.C()
}

// TempDir returns the filepath of the temporary directory used for temp storage.
// It is empty for an in-memory temp storage.
func (s *topLevelServer) TempDir() string {
	return s.cfg.TempStorageConfig.Path
}

// PGServer exports the pgwire server. Used by tests.
func (s *topLevelServer) PGServer() *pgwire.Server {
	return s.sqlServer.pgServer
}

// SpanConfigReporter returns the spanconfig.Reporter. Used by tests.
func (s *topLevelServer) SpanConfigReporter() spanconfig.Reporter {
	return s.spanConfigReporter
}

// LogicalClusterID implements cli.serverStartupInterface. This
// implementation exports the logical cluster ID of the system tenant.
func (s *topLevelServer) LogicalClusterID() uuid.UUID {
	return s.sqlServer.LogicalClusterID()
}

// startDiagnostics starts periodic diagnostics reporting and update checking.
func (s *topLevelServer) startDiagnostics(ctx context.Context) {
	s.updates.PeriodicallyCheckForUpdates(ctx, s.stopper)
	s.sqlServer.StartDiagnostics(ctx)
}

func init() {
	tracing.RegisterTagRemapping("n", "node")
}

// Insecure returns true iff the server has security disabled.
func (s *topLevelServer) Insecure() bool {
	return s.cfg.Insecure
}

// TenantCapabilitiesReader returns the Server's tenantcapabilities.Reader.
func (s *topLevelServer) TenantCapabilitiesReader() tenantcapabilities.Reader {
	return s.tenantCapabilitiesWatcher
}

// Drain idempotently activates the draining mode.
// Note: new code should not be taught to use this method
// directly. Use the Drain() RPC instead with a suitably crafted
// DrainRequest.
//
// On failure, the system may be in a partially drained
// state; the client should either continue calling Drain() or shut
// down the server.
//
// The reporter function, if non-nil, is called for each
// packet of load shed away from the server during the drain.
//
// TODO(knz): This method is currently exported for use by the
// shutdown code in cli/start.go; however, this is a mis-design. The
// start code should use the Drain() RPC like quit does.
func (s *topLevelServer) Drain(
	ctx context.Context, verbose bool,
) (remaining uint64, info redact.RedactableString, err error) {
	return s.drain.runDrain(ctx, verbose)
}

// MakeServerOptionsForURL creates the input for MakeURLForServer().
// Beware of not calling this too early; the server address
// is finalized late in the network initialization sequence.
func MakeServerOptionsForURL(
	baseCfg *base.Config,
) (clientsecopts.ClientSecurityOptions, clientsecopts.ServerParameters) {
	clientConnOptions := clientsecopts.ClientSecurityOptions{
		Insecure: baseCfg.Insecure,
		CertsDir: baseCfg.SSLCertsDir,
	}
	serverParams := clientsecopts.ServerParameters{
		ServerAddr:      baseCfg.SQLAdvertiseAddr,
		DefaultPort:     base.DefaultPort,
		DefaultDatabase: catalogkeys.DefaultDatabaseName,
	}
	return clientConnOptions, serverParams
}
