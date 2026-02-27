// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Each node attempts to contact peer nodes to gather all Infos in
the system with minimal total hops. The algorithm is as follows:

 0 Node starts up gossip server to accept incoming gossip requests.
   Continue to step #1 to join the gossip network.

 1 Node selects random peer from bootstrap list, excluding its own
   address for its first outgoing connection. Node starts client and
   continues to step #2.

 2 Node requests gossip from peer. Gossip requests (and responses)
   contain a map from node ID to info about other nodes in the
   network. Each node maintains its own map as well as the maps of
   each of its peers. The info for each node includes the most recent
   timestamp of any Info originating at that node, as well as the min
   number of hops to reach that node. Requesting node times out at
   checkInterval. On timeout, client is closed and GC'd. If node has
   no outgoing connections, goto #1.

   a. When gossip is received, infostore is augmented. If new Info was
      received, the client in question is credited. If node has no
      outgoing connections, goto #1.

   b. If any gossip was received at > maxHops and num connected peers
      < maxPeers(), choose random peer from those originating Info >
      maxHops, start it, and goto #2.

   c. If sentinel gossip keyed by KeySentinel is missing or expired,
      node is considered partitioned; goto #1.

 3 On connect, if node has too many connected clients, gossip requests
   are returned immediately with an alternate address set to a random
   selection from amongst already-connected clients.
*/

package gossip

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

const (
	// maxHops is the maximum number of hops which any gossip info
	// should require to transit between any two nodes in a gossip
	// network.
	maxHops = 5

	// minPeers is the minimum number of peers which the maxPeers()
	// function will return. This is set higher than one to prevent
	// excessive tightening of the network.
	minPeers = 3

	// defaultStallInterval is the default interval for checking whether
	// the incoming and outgoing connections to the gossip network are
	// insufficient to keep the network connected.
	defaultStallInterval = 1 * time.Second

	// defaultBootstrapInterval is the minimum time between successive
	// bootstrapping attempts to avoid busy-looping trying to find the
	// sentinel gossip info.
	defaultBootstrapInterval = 1 * time.Second

	// defaultCullInterval is the default interval for culling the least
	// "useful" outgoing gossip connection to free up space for a more
	// efficiently targeted connection to the most distant node.
	defaultCullInterval = 60 * time.Second

	// NodeDescriptorInterval is the interval for gossiping the node descriptor.
	// Note that increasing this duration may increase the likelihood of gossip
	// thrashing, since node descriptors are used to determine the number of gossip
	// hops between nodes (see #9819 for context).
	NodeDescriptorInterval = 1 * time.Hour

	// NodeDescriptorTTL is time-to-live for node ID -> descriptor.
	NodeDescriptorTTL = 2 * NodeDescriptorInterval

	// StoresInterval is the default interval for gossiping store descriptors.
	// Note that there are additional conditions that trigger reactive store
	// gossip.
	StoresInterval = 10 * time.Second

	// StoreTTL is time-to-live for store-related info.
	StoreTTL = 2 * StoresInterval

	// gossipTightenInterval is how long to wait between tightenNetwork checks if
	// we didn't need to tighten the last time we checked.
	gossipTightenInterval = time.Second

	// infosBatchDelay controls how much time do we wait to batch infos before
	// sending them.
	infosBatchDelay = 10 * time.Millisecond

	unknownNodeID roachpb.NodeID = 0
)

// Gossip metrics counter names.
var (
	MetaConnectionsIncomingGauge = metric.Metadata{
		Name:        "gossip.connections.incoming",
		Help:        "Number of active incoming gossip connections",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaConnectionsOutgoingGauge = metric.Metadata{
		Name:        "gossip.connections.outgoing",
		Help:        "Number of active outgoing gossip connections",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaConnectionsRefused = metric.Metadata{
		Name:        "gossip.connections.refused",
		Help:        "Number of refused incoming gossip connections",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaMessagesSent = metric.Metadata{
		Name:        "gossip.messages.sent",
		Help:        "Number of sent gossip messages",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	MetaMessagesReceived = metric.Metadata{
		Name:        "gossip.messages.received",
		Help:        "Number of received gossip messages",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	MetaInfosSent = metric.Metadata{
		Name:        "gossip.infos.sent",
		Help:        "Number of sent gossip Info objects",
		Measurement: "Infos",
		Unit:        metric.Unit_COUNT,
	}
	MetaInfosReceived = metric.Metadata{
		Name:        "gossip.infos.received",
		Help:        "Number of received gossip Info objects",
		Measurement: "Infos",
		Unit:        metric.Unit_COUNT,
	}
	MetaBytesSent = metric.Metadata{
		Name:        "gossip.bytes.sent",
		Help:        "Number of sent gossip bytes",
		Measurement: "Gossip Bytes",
		Unit:        metric.Unit_BYTES,
	}
	MetaBytesReceived = metric.Metadata{
		Name:        "gossip.bytes.received",
		Help:        "Number of received gossip bytes",
		Measurement: "Gossip Bytes",
		Unit:        metric.Unit_BYTES,
	}
	MetaCallbacksProcessed = metric.Metadata{
		Name:        "gossip.callbacks.processed",
		Help:        "Number of gossip callbacks processed",
		Measurement: "Callbacks",
		Unit:        metric.Unit_COUNT,
	}
	MetaCallbacksPending = metric.Metadata{
		Name:        "gossip.callbacks.pending",
		Help:        "Number of gossip callbacks waiting to be processed",
		Measurement: "Callbacks",
		Unit:        metric.Unit_COUNT,
	}
	MetaCallbacksProcessingDuration = metric.Metadata{
		Name:        "gossip.callbacks.processing_duration",
		Help:        "Duration of gossip callback processing",
		Measurement: "Duration",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaCallbacksPendingDuration = metric.Metadata{
		Name:        "gossip.callbacks.pending_duration",
		Help:        "Duration of gossip callback queueing to be processed",
		Measurement: "Duration",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// KeyNotPresentError is returned by gossip when queried for a key that doesn't
// exist or has expired.
type KeyNotPresentError struct {
	key string
}

// Error implements the error interface.
func (err KeyNotPresentError) Error() string {
	return fmt.Sprintf("KeyNotPresentError: gossip key %q does not exist or has expired", err.key)
}

// NewKeyNotPresentError creates a new KeyNotPresentError.
func NewKeyNotPresentError(key string) error {
	return KeyNotPresentError{key: key}
}

// AddressResolver is a thin wrapper around gossip's GetNodeIDAddress
// that allows it to be used as a nodedialer.AddressResolver.
func AddressResolver(gossip *Gossip) nodedialer.AddressResolver {
	return func(nodeID roachpb.NodeID) (net.Addr, roachpb.Locality, error) {
		return gossip.GetNodeIDAddress(nodeID)
	}
}

// Storage is an interface which allows the gossip instance
// to read and write bootstrapping data to persistent storage
// between instantiations.
type Storage interface {
	// ReadBootstrapInfo fetches the bootstrap data from the persistent
	// store into the provided bootstrap protobuf. Returns nil or an
	// error on failure.
	ReadBootstrapInfo(*BootstrapInfo) error
	// WriteBootstrapInfo stores the provided bootstrap data to the
	// persistent store. Returns nil or an error on failure.
	WriteBootstrapInfo(*BootstrapInfo) error
}

// Gossip is an instance of a gossip node. It embeds a gossip server.
// During bootstrapping, the bootstrap list contains candidates for
// entry to the gossip network.
type Gossip struct {
	// 标记是否已启动（用于断言）
	started bool // for assertions
	// 嵌入的 Gossip RPC 服务器
	*server // Embedded gossip RPC server

	// 连接状态
	Connected    chan struct{} // 首次连接成功时关闭此 channel
	hasConnected bool          // 是否已连接过
	outgoing     nodeSet       // 出向连接的节点集合
	// Bootstrap 相关
	storage       Storage             // 持久化存储接口
	bootstrapInfo BootstrapInfo       // 持久化的 bootstrap 地址
	bootstrapping map[string]struct{} // 当前正在尝试的 bootstrap 地址

	hasCleanedBS bool

	// Note that access to each client's internal state is serialized by the
	// embedded server's mutex. This is surprising!
	// Client 管理
	clientsMu struct {
		syncutil.Mutex
		clients []*client // 所有出向 gossip 客户端
	}
	disconnected chan *client // 断开连接的客户端通知 channel
	// 状态管理
	stalled   bool          // 是否处于 stalled 状态（无连接或缺 sentinel）
	stalledCh chan struct{} // 用于唤醒 bootstrap goroutine

	// 定时器配置
	stallInterval     time.Duration // 检查 stall 的间隔（默认 1s）
	bootstrapInterval time.Duration // Bootstrap 重试间隔（默认 1s）
	cullInterval      time.Duration // 淘汰低效连接的间隔（默认 60s）

	// TODO(baptist): Remember the localities for each remote address. Then pass
	// it into the Dial.
	// addresses is a list of bootstrap host addresses for
	// connecting to the gossip network.
	// 地址管理
	addresses      []util.UnresolvedAddr                  // bootstrap 地址列表
	addressIdx     int                                    // 当前尝试的地址索引
	addressesTried map[int]struct{}                       // 已尝试过的地址索引
	bootstrapAddrs map[util.UnresolvedAddr]roachpb.NodeID // bootstrap 地址 -> NodeID 映射
	addressExists  map[util.UnresolvedAddr]bool
	// 节点描述符缓存
	nodeDescs  syncutil.Map[roachpb.NodeID, roachpb.NodeDescriptor]
	storeDescs syncutil.Map[roachpb.StoreID, roachpb.StoreDescriptor]

	// Membership sets for bootstrap addresses. bootstrapAddrs also tracks which
	// address is associated with which node ID to enable faster node lookup by
	// address.

	locality roachpb.Locality // 本节点的 locality 信息
}

// New creates an instance of a gossip node.
// The higher level manages the ClusterIDContainer and NodeIDContainer instances
// (which can be shared by various server components).
// The struct returned is started by calling Start and passing a rpc.Context.
func New(
	ambient log.AmbientContext,
	clusterID *base.ClusterIDContainer,
	nodeID *base.NodeIDContainer,
	stopper *stop.Stopper,
	registry *metric.Registry,
	locality roachpb.Locality,
) *Gossip {
	g := &Gossip{
		server:            newServer(ambient, clusterID, nodeID, stopper, registry),
		Connected:         make(chan struct{}),
		outgoing:          makeNodeSet(minPeers, metric.NewGauge(MetaConnectionsOutgoingGauge)),
		bootstrapping:     map[string]struct{}{},
		disconnected:      make(chan *client, 10),
		stalledCh:         make(chan struct{}, 1),
		stallInterval:     defaultStallInterval,
		bootstrapInterval: defaultBootstrapInterval,
		cullInterval:      defaultCullInterval,
		addressesTried:    map[int]struct{}{},
		addressExists:     map[util.UnresolvedAddr]bool{},
		bootstrapAddrs:    map[util.UnresolvedAddr]roachpb.NodeID{},
		locality:          locality,
	}

	registry.AddMetric(g.outgoing.gauge)

	g.mu.Lock()
	defer g.mu.Unlock()
	// Add ourselves as a node descriptor watcher.
	g.mu.is.registerCallback(MakePrefixPattern(KeyNodeDescPrefix), g.updateNodeAddress)
	g.mu.is.registerCallback(MakePrefixPattern(KeyStoreDescPrefix), g.updateStoreMap)

	return g
}

// NewTest is a simplified wrapper around New that creates the
// ClusterIDContainer and NodeIDContainer internally. Used for testing.
func NewTest(nodeID roachpb.NodeID, stopper *stop.Stopper, registry *metric.Registry) *Gossip {
	return NewTestWithLocality(nodeID, stopper, registry, roachpb.Locality{})
}

// NewTestWithLocality calls NewTest with an explicit locality value.
func NewTestWithLocality(
	nodeID roachpb.NodeID,
	stopper *stop.Stopper,
	registry *metric.Registry,
	locality roachpb.Locality,
) *Gossip {
	c := &base.ClusterIDContainer{}
	n := &base.NodeIDContainer{}
	var ac log.AmbientContext
	ac.AddLogTag("n", n)
	gossip := New(ac, c, n, stopper, registry, locality)
	if nodeID != 0 {
		n.Set(context.TODO(), nodeID)
	}
	return gossip
}

type drpcGossip Gossip

// AsDRPCServer returns the DRPC server implementation for the Gossip service.
func (n *Gossip) AsDRPCServer() DRPCGossipServer {
	return (*drpcGossip)(n)
}

// Gossip implements the DRPC service. It receives gossiped information from a
// peer node. The received delta is combined with the infostore, and this node's
// own gossip is returned to requesting client.
func (g *drpcGossip) Gossip(stream DRPCGossip_GossipStream) error {
	return (*Gossip)(g).gossip(stream)
}

// AssertNotStarted fatals if the Gossip instance was already started.
func (g *Gossip) AssertNotStarted(ctx context.Context) {
	if g.started {
		log.Dev.Fatalf(ctx, "gossip instance was already started")
	}
}

// GetNodeID gets the NodeID.
func (g *Gossip) GetNodeID() roachpb.NodeID {
	return g.NodeID.Get()
}

// GetNodeMetrics returns the gossip node metrics.
func (g *Gossip) GetNodeMetrics() *Metrics {
	return g.server.GetNodeMetrics()
}

// SetNodeDescriptor adds the node descriptor to the gossip network.
func (g *Gossip) SetNodeDescriptor(desc *roachpb.NodeDescriptor) error {
	ctx := g.AnnotateCtx(context.TODO())
	log.Dev.VInfof(ctx, 1, "NodeDescriptor set to %+v", desc)
	if desc.Address.IsEmpty() {
		log.Dev.Fatalf(ctx, "n%d address is empty", desc.NodeID)
	}
	if err := g.AddInfoProto(MakeNodeIDKey(desc.NodeID), desc, NodeDescriptorTTL); err != nil {
		return errors.Wrapf(err, "n%d: couldn't gossip descriptor", desc.NodeID)
	}
	return nil
}

// SetStallInterval sets the interval between successive checks
// to determine whether this host is not connected to the gossip
// network, or else is connected to a partition which doesn't
// include the host which gossips the sentinel info.
func (g *Gossip) SetStallInterval(interval time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stallInterval = interval
}

// SetBootstrapInterval sets a minimum interval between successive
// attempts to connect to new hosts in order to join the gossip
// network.
func (g *Gossip) SetBootstrapInterval(interval time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.bootstrapInterval = interval
}

// SetCullInterval sets the interval between periodic shutdown of
// outgoing gossip client connections in an effort to improve the
// fitness of the network.
func (g *Gossip) SetCullInterval(interval time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.cullInterval = interval
}

// SetStorage provides an instance of the Storage interface
// for reading and writing gossip bootstrap data from persistent
// storage. This should be invoked as early in the lifecycle of a
// gossip instance as possible, but can be called at any time.
// - Gossip 需要持久化已知节点的地址（用于下次启动时的 bootstrap）
// - 通过 n.stores 实现 gossip.Storage 接口，将地址写入 Store 的系统键
func (g *Gossip) SetStorage(storage Storage) error {
	ctx := g.AnnotateCtx(context.TODO())
	// Maintain lock ordering.
	var storedBI BootstrapInfo
	if err := storage.ReadBootstrapInfo(&storedBI); err != nil {
		log.Ops.Warningf(ctx, "failed to read gossip bootstrap info: %s", err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	g.storage = storage

	// Merge the stored bootstrap info addresses with any we've become
	// aware of through gossip.
	existing := map[string]struct{}{}
	makeKey := func(a util.UnresolvedAddr) string { return fmt.Sprintf("%s,%s", a.Network(), a.String()) }
	for _, addr := range g.bootstrapInfo.Addresses {
		existing[makeKey(addr)] = struct{}{}
	}
	for _, addr := range storedBI.Addresses {
		// If the address is new, and isn't our own address, add it.
		if _, ok := existing[makeKey(addr)]; !ok && addr != g.mu.is.NodeAddr {
			g.maybeAddBootstrapAddressLocked(addr, unknownNodeID)
		}
	}
	// Persist merged addresses.
	if numAddrs := len(g.bootstrapInfo.Addresses); numAddrs > len(storedBI.Addresses) {
		if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
			log.Dev.Errorf(ctx, "%v", err)
		}
	}

	// Cycle through all persisted bootstrap hosts and add addresses that
	// don't already exist.
	newAddressFound := false
	for _, addr := range g.bootstrapInfo.Addresses {
		if !g.maybeAddAddressLocked(addr) {
			continue
		}
		// If we find a new address, reset the address index so that the
		// next address we try is the first of the new addresses.
		if !newAddressFound {
			newAddressFound = true
			g.addressIdx = len(g.addresses) - 1
		}
	}

	// If a new address was found, immediately signal bootstrap.
	if newAddressFound {
		log.Ops.VInfof(ctx, 1, "found new addresses from storage; signaling bootstrap")
		g.signalStalledLocked()
	}
	return nil
}

// setAddresses initializes the set of gossip addresses used to find
// nodes to bootstrap the gossip network.
func (g *Gossip) setAddresses(addresses []util.UnresolvedAddr) {
	if addresses == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Start index at end because get next address loop logic increments as first step.
	g.addressIdx = len(addresses) - 1
	g.addresses = addresses
	g.addressesTried = map[int]struct{}{}

	// Start new bootstrapping immediately instead of waiting for next bootstrap interval.
	g.maybeSignalStatusChangeLocked()
}

// GetAddresses returns a copy of the addresses slice.
func (g *Gossip) GetAddresses() []util.UnresolvedAddr {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return append([]util.UnresolvedAddr(nil), g.addresses...)
}

// GetNodeIDAddress looks up the RPC address of the node by ID.
func (g *Gossip) GetNodeIDAddress(
	nodeID roachpb.NodeID,
) (*util.UnresolvedAddr, roachpb.Locality, error) {
	return g.getNodeIDAddress(nodeID, false /* locked */)
}

// GetNodeIDSQLAddress looks up the SQL address of the node by ID.
func (g *Gossip) GetNodeIDSQLAddress(
	nodeID roachpb.NodeID,
) (*util.UnresolvedAddr, roachpb.Locality, error) {
	nd, err := g.getNodeDescriptor(nodeID, false /* locked */)
	if err != nil {
		return nil, roachpb.Locality{}, err
	}
	return &nd.SQLAddress, nd.Locality, nil
}

// GetNodeIDHTTPAddress looks up the HTTP address of the node by ID.
func (g *Gossip) GetNodeIDHTTPAddress(
	nodeID roachpb.NodeID,
) (*util.UnresolvedAddr, roachpb.Locality, error) {
	nd, err := g.getNodeDescriptor(nodeID, false /* locked */)
	if err != nil {
		return nil, roachpb.Locality{}, err
	}
	return &nd.HTTPAddress, nd.Locality, nil
}

// GetNodeDescriptor looks up the descriptor of the node by ID.
func (g *Gossip) GetNodeDescriptor(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	return g.getNodeDescriptor(nodeID, false /* locked */)
}

// GetNodeDescriptorCount gets the number of node descriptors.
func (g *Gossip) GetNodeDescriptorCount() int {
	count := 0
	g.nodeDescs.Range(func(_ roachpb.NodeID, _ *roachpb.NodeDescriptor) bool {
		count++
		return true
	})
	return count
}

// TODO(baptist): StoreDescriptors don't belong in the Gossip package at all.
// This method should be moved out of gossip.
// GetStoreDescriptor looks up the descriptor of the node by ID.
func (g *Gossip) GetStoreDescriptor(storeID roachpb.StoreID) (*roachpb.StoreDescriptor, error) {
	if desc, ok := g.storeDescs.Load(storeID); ok {
		return desc, nil
	}
	return nil, kvpb.NewStoreNotFoundError(storeID)
}

// LogStatus logs the current status of gossip such as the incoming and
// outgoing connections.
func (g *Gossip) LogStatus() {
	n, status := func() (int, redact.SafeString) {
		var inc int
		g.mu.RLock()
		defer g.mu.RUnlock()
		g.nodeDescs.Range(func(_ roachpb.NodeID, _ *roachpb.NodeDescriptor) bool {
			inc++
			return true
		})
		s := redact.SafeString("ok")
		if g.mu.is.getInfo(KeySentinel) == nil {
			s = redact.SafeString("stalled")
		}
		return inc, s
	}()
	ctx := g.AnnotateCtx(context.TODO())
	log.Health.Infof(ctx, "gossip status (%s, %d node%s)\n%s%s",
		status, n, util.Pluralize(int64(n)),
		g.clientStatus(), g.server.status())
}

func (g *Gossip) clientStatus() ClientStatus {
	g.mu.RLock()
	defer g.mu.RUnlock()
	g.clientsMu.Lock()
	defer g.clientsMu.Unlock()

	var status ClientStatus

	status.MaxConns = int32(g.outgoing.maxSize)
	status.ConnStatus = make([]OutgoingConnStatus, 0, len(g.clientsMu.clients))
	for _, c := range g.clientsMu.clients {
		status.ConnStatus = append(status.ConnStatus, OutgoingConnStatus{
			ConnStatus: ConnStatus{
				NodeID:   c.peerID,
				Address:  c.addr.String(),
				AgeNanos: timeutil.Since(c.createdAt).Nanoseconds(),
			},
			MetricSnap: c.clientMetrics.Snapshot(),
		})
	}
	return status
}

// EnableSimulationCycler is for TESTING PURPOSES ONLY. It sets a
// condition variable which is signaled at each cycle of the
// simulation via SimulationCycle(). The gossip server makes each
// connecting client wait for the cycler to signal before responding.
func (g *Gossip) EnableSimulationCycler(enable bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if enable {
		g.simulationCycler = sync.NewCond(&g.mu)
	} else {
		// TODO(spencer): remove this nil check when gossip/simulation is no
		// longer used in kv tests.
		if g.simulationCycler != nil {
			g.simulationCycler.Broadcast()
			g.simulationCycler = nil
		}
	}
}

// SimulationCycle cycles this gossip node's server by allowing all
// connected clients to proceed one step.
func (g *Gossip) SimulationCycle() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.simulationCycler != nil {
		g.simulationCycler.Broadcast()
	}
}

// maybeAddAddressLocked validates and adds the given address unless it already
// exists. Returns true if the address was new. The caller must hold the gossip
// mutex.
func (g *Gossip) maybeAddAddressLocked(addr util.UnresolvedAddr) bool {
	if g.addressExists[addr] {
		return false
	}
	ctx := g.AnnotateCtx(context.TODO())
	if addr.Network() != "tcp" {
		log.Ops.Warningf(ctx, "unknown address network %q for %v", addr.Network(), addr)
		return false
	}
	g.addresses = append(g.addresses, addr)
	g.addressExists[addr] = true
	log.Eventf(ctx, "add address %s", addr)
	return true
}

// maybeAddBootstrapAddressLocked adds the specified address to the list
// of bootstrap addresses if not already present. Returns whether a new
// bootstrap address was added. The caller must hold the gossip mutex.
func (g *Gossip) maybeAddBootstrapAddressLocked(
	addr util.UnresolvedAddr, nodeID roachpb.NodeID,
) bool {
	if existingNodeID, ok := g.bootstrapAddrs[addr]; ok {
		if existingNodeID == unknownNodeID || existingNodeID != nodeID {
			g.bootstrapAddrs[addr] = nodeID
		}
		return false
	}
	g.bootstrapInfo.Addresses = append(g.bootstrapInfo.Addresses, addr)
	g.bootstrapAddrs[addr] = nodeID
	ctx := g.AnnotateCtx(context.TODO())
	log.Eventf(ctx, "add bootstrap %s", addr)
	return true
}

// maybeCleanupBootstrapAddresses cleans up the stored bootstrap addresses to
// include only those currently available via gossip. The gossip mutex must
// be held by the caller.
func (g *Gossip) maybeCleanupBootstrapAddressesLocked() {
	if g.storage == nil || g.hasCleanedBS {
		return
	}
	defer func() { g.hasCleanedBS = true }()
	ctx := g.AnnotateCtx(context.TODO())
	log.Event(ctx, "cleaning up bootstrap addresses")

	g.addresses = g.addresses[:0]
	g.addressIdx = 0
	g.bootstrapInfo.Addresses = g.bootstrapInfo.Addresses[:0]
	g.bootstrapAddrs = map[util.UnresolvedAddr]roachpb.NodeID{}
	g.addressExists = map[util.UnresolvedAddr]bool{}
	g.addressesTried = map[int]struct{}{}

	var desc roachpb.NodeDescriptor
	if err := g.mu.is.visitInfos(func(key string, i *Info) error {
		if strings.HasPrefix(key, KeyNodeDescPrefix) {
			if err := i.Value.GetProto(&desc); err != nil {
				return err
			}
			if desc.Address.IsEmpty() || desc.Address == g.mu.is.NodeAddr {
				return nil
			}
			g.maybeAddAddressLocked(desc.Address)
			g.maybeAddBootstrapAddressLocked(desc.Address, desc.NodeID)
		}
		return nil
	}, true /* deleteExpired */); err != nil {
		log.Dev.Errorf(ctx, "%v", err)
		return
	}

	if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
		log.Dev.Errorf(ctx, "%v", err)
	}
}

// maxPeers returns the maximum number of peers each gossip node
// may connect to. This is based on maxHops, which is a preset
// maximum for number of hops allowed before the gossip network
// will seek to "tighten" by creating new connections to distant
// nodes.
// 连接数动态调整
func maxPeers(nodeCount int) int {
	// 公式：maxPeers = ceil(e^(log(nodeCount) / (maxHops-2)))
	//
	// 推导（简化）：
	// - 假设每个节点连接 P 个 peer
	// - 信息在 H 跳内覆盖 P^H 个节点
	// - 要求 P^H >= N（总节点数）
	// - 则 P >= N^(1/H)
	//
	// 使用 maxHops-2（而非 maxHops）是为了留出"余量"
	// This formula uses maxHops-2, instead of maxHops, to provide a
	// "fudge" factor for max connected peers, to account for the
	// arbitrary, decentralized way in which gossip networks are created.
	// This will return the following maxPeers for the given number of nodes:
	//	 <= 27 nodes -> 3 peers
	//   <= 64 nodes -> 4 peers
	//   <= 125 nodes -> 5 peers
	//   <= n^3 nodes -> n peers
	//
	// Quick derivation of the formula for posterity (without the fudge factor):
	// maxPeers^maxHops > nodeCount
	// maxHops * log(maxPeers) > log(nodeCount)
	// log(maxPeers) > log(nodeCount) / maxHops
	// maxPeers > e^(log(nodeCount) / maxHops)
	// hence maxPeers = ceil(e^(log(nodeCount) / maxHops)) should work
	maxPeers := int(math.Ceil(math.Exp(math.Log(float64(nodeCount)) / float64(maxHops-2))))
	if maxPeers < minPeers {
		return minPeers
	}
	return maxPeers
}

// updateNodeAddress is a gossip callback which fires with each
// update to a node descriptor. This allows us to compute the
// total size of the gossip network (for determining max peers
// each gossip node is allowed to have), as well as to create
// new addresses for each encountered host and to write the
// set of gossip node addresses to persistent storage when it
// changes.
func (g *Gossip) updateNodeAddress(key string, content roachpb.Value, _ int64) {
	ctx := g.AnnotateCtx(context.TODO())
	var desc roachpb.NodeDescriptor
	if err := content.GetProto(&desc); err != nil {
		log.Dev.Errorf(ctx, "%v", err)
		return
	}

	log.Dev.VInfof(ctx, 1, "updateNodeAddress called on %q with desc %+v", key, desc)

	g.mu.Lock()
	defer g.mu.Unlock()

	// If desc is the empty descriptor, that indicates that the node has been
	// removed from the cluster. If that's the case, remove it from our map of
	// nodes to prevent other parts of the system from trying to talk to it.
	// We can't directly compare the node against the empty descriptor because
	// the proto has a repeated field and thus isn't comparable.
	if desc.NodeID == 0 || desc.Address.IsEmpty() {
		nodeID, err := DecodeNodeDescKey(key, KeyNodeDescPrefix)
		if err != nil {
			log.Health.Errorf(ctx, "unable to update node address for removed node: %s", err)
			return
		}
		log.Health.Infof(ctx, "removed n%d from gossip", nodeID)
		g.removeNodeDescriptorLocked(nodeID)
		return
	}

	if existingDesc, ok := g.nodeDescs.Load(desc.NodeID); ok {
		if !existingDesc.Equal(&desc) {
			g.nodeDescs.Store(desc.NodeID, &desc)
		}
		// Skip all remaining logic if the address hasn't changed, since that's all
		// the logic cares about.
		if existingDesc.Address == desc.Address {
			return
		}
	} else {
		g.nodeDescs.Store(desc.NodeID, &desc)
	}
	g.recomputeMaxPeersLocked()

	// Skip if it's our own address.
	if desc.Address == g.mu.is.NodeAddr {
		return
	}

	// Add this new node address (if it's not already there) to our list
	// of addresses so we can keep connecting to gossip if the original
	// addresses go offline.
	g.maybeAddAddressLocked(desc.Address)

	// Add new address (if it's not already there) to bootstrap info and
	// persist if possible.
	added := g.maybeAddBootstrapAddressLocked(desc.Address, desc.NodeID)
	if added && g.storage != nil {
		if err := g.storage.WriteBootstrapInfo(&g.bootstrapInfo); err != nil {
			log.Dev.Errorf(ctx, "%v", err)
		}
	}
}

func (g *Gossip) removeNodeDescriptorLocked(nodeID roachpb.NodeID) {
	g.nodeDescs.Delete(nodeID)
	g.recomputeMaxPeersLocked()
}

// updateStoreMap is a gossip callback which is used to update storeMap.
func (g *Gossip) updateStoreMap(key string, content roachpb.Value, _ int64) {
	ctx := g.AnnotateCtx(context.TODO())
	var desc roachpb.StoreDescriptor
	if err := content.GetProto(&desc); err != nil {
		log.Dev.Errorf(ctx, "%v", err)
		return
	}

	log.Dev.VInfof(ctx, 1, "updateStoreMap called on %q with desc %+v", key, desc)

	g.storeDescs.Store(desc.StoreID, &desc)
}

// recomputeMaxPeersLocked recomputes max peers based on size of
// network and set the max sizes for incoming and outgoing node sets.
//
// Note: if we notice issues with never-ending connection refused errors
// in real deployments, consider allowing more incoming connections than
// outgoing connections. As of now, the cluster's steady state is to have
// all nodes fill up, which can make rebalancing of connections tough.
// I'm not making this change now since it tends to lead to less balanced
// networks and I'm not sure what all the consequences of that might be.
func (g *Gossip) recomputeMaxPeersLocked() {
	var n int
	g.nodeDescs.Range(func(_ roachpb.NodeID, _ *roachpb.NodeDescriptor) bool {
		n++
		return true
	})
	maxPeers := maxPeers(n)
	g.mu.incoming.setMaxSize(maxPeers)
	g.outgoing.setMaxSize(maxPeers)
}

// getNodeDescriptor looks up the descriptor of the node by ID. The method
// accepts a flag indicating whether the mutex is held by the caller. This
// method is called externally via GetNodeDescriptor and internally by
// getNodeIDAddress.
func (g *Gossip) getNodeDescriptor(
	nodeID roachpb.NodeID, locked bool,
) (*roachpb.NodeDescriptor, error) {
	if desc, ok := g.nodeDescs.Load(nodeID); ok {
		if desc.Address.IsEmpty() {
			log.Dev.Fatalf(g.AnnotateCtx(context.Background()), "n%d has an empty address", nodeID)
		}
		return desc, nil
	}

	// Fallback to retrieving the node info and unmarshalling the node
	// descriptor. This path occurs in tests which add a node descriptor to
	// gossip and then immediately try retrieve it.
	nodeIDKey := MakeNodeIDKey(nodeID)

	if !locked {
		g.mu.RLock()
		defer g.mu.RUnlock()
	}

	// We can't use GetInfoProto here because that method grabs the lock.
	if i := g.mu.is.getInfo(nodeIDKey); i != nil {
		if err := i.Value.Verify([]byte(nodeIDKey)); err != nil {
			return nil, err
		}
		nodeDescriptor := &roachpb.NodeDescriptor{}
		if err := i.Value.GetProto(nodeDescriptor); err != nil {
			return nil, err
		}
		// Don't return node descriptors that are empty, because that's meant to
		// indicate that the node has been removed from the cluster.
		if nodeDescriptor.NodeID == 0 || nodeDescriptor.Address.IsEmpty() {
			return nil, errors.Errorf("n%d has been removed from the cluster", nodeID)
		}

		return nodeDescriptor, nil
	}

	return nil, kvpb.NewNodeDescNotFoundError(nodeID)
}

// getNodeIDAddress looks up the address of the node by ID. The method accepts a
// flag indicating whether the mutex is held by the caller. This method is
// called externally via GetNodeIDAddress or internally when looking up a
// "distant" node address to connect directly to.
func (g *Gossip) getNodeIDAddress(
	nodeID roachpb.NodeID, locked bool,
) (*util.UnresolvedAddr, roachpb.Locality, error) {
	nd, err := g.getNodeDescriptor(nodeID, locked)
	if err != nil {
		return nil, roachpb.Locality{}, err
	}
	return nd.AddressForLocality(g.locality), nd.Locality, nil
}

// AddInfo adds or updates an info object. Returns an error if info
// couldn't be added.
func (g *Gossip) AddInfo(key string, val []byte, ttl time.Duration) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.addInfoLocked(key, val, ttl)
}

// addInfoLocked adds or updates an info object. The mutex is assumed held by
// the caller. Returns an error if info couldn't be added.
func (g *Gossip) addInfoLocked(key string, val []byte, ttl time.Duration) error {
	err := g.mu.is.addInfo(key, g.mu.is.newInfo(val, ttl))
	if err == nil {
		g.signalConnectedLocked()
	}
	return err
}

// AddInfoProto adds or updates an info object. Returns an error if info
// couldn't be added.
func (g *Gossip) AddInfoProto(key string, msg protoutil.Message, ttl time.Duration) error {
	bytes, err := protoutil.Marshal(msg)
	if err != nil {
		return err
	}
	return g.AddInfo(key, bytes, ttl)
}

// AddInfoIfNotRedundant adds or updates an info object if it isn't already
// present in the local infoStore with exactly the same value and with this
// node as the source. Motivated by the node liveness range's desire to only
// gossip changed entries.
//
// Assumes the values have a TTL of 0 (always adding the gossip anew if the
// stored value wasn't added with a TTL of 0), because if a value had a non-0
// TTL then we'd have to make some sort of value judgment about whether it's
// worth re-gossiping yet given how old the existing matching info was.
func (g *Gossip) AddInfoIfNotRedundant(key string, val []byte) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.addInfoIfNotRedundantLocked(key, val)
}

// addInfoIfNotRedundantLocked implements AddInfoIfNotRedundant.
func (g *Gossip) addInfoIfNotRedundantLocked(key string, val []byte) error {
	info := g.mu.is.getInfo(key)

	if info != nil {
		infoBytes, err := info.Value.GetBytes()
		if err == nil && bytes.Equal(infoBytes, val) && g.infoOriginatedHere(info) && info.TTLStamp == math.MaxInt64 {
			// Nothing has changed, so no need to re-gossip.
			return nil
		}
	}

	// Something is different, so we do need to add the provided key/value.
	return g.addInfoLocked(key, val, 0 /* ttl */)
}

// InfoToAdd contains a single Gossip info to be added to the network.
type InfoToAdd struct {
	Key string
	Val []byte
}

// BulkAddInfoIfNotRedundant matches the semantics of AddInfoIfNotRedundant
// except for a batch of gossip infos rather than for a single info. The
// benefit of batching is not needing to repeatedly lock and unlock the gossip
// mutex for each info.
func (g *Gossip) BulkAddInfoIfNotRedundant(toAdd []InfoToAdd) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for i := range toAdd {
		if err := g.addInfoIfNotRedundantLocked(toAdd[i].Key, toAdd[i].Val); err != nil {
			return err
		}
	}
	return nil
}

// AddClusterID is a convenience method for gossipping the cluster ID. There's
// no TTL - the record lives forever.
func (g *Gossip) AddClusterID(val uuid.UUID) error {
	return g.AddInfo(KeyClusterID, val.GetBytes(), 0 /* ttl */)
}

// GetClusterID returns the cluster ID if it has been gossipped. If it hasn't,
// (so if this gossip instance is not "connected"), an error is returned.
func (g *Gossip) GetClusterID() (uuid.UUID, error) {
	uuidBytes, err := g.GetInfo(KeyClusterID)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "unable to ascertain cluster ID from gossip network")
	}
	clusterID, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		return uuid.Nil, errors.Wrap(err, "unable to parse cluster ID from gossip network")
	}
	return clusterID, nil
}

// GetInfo returns an info value by key or an KeyNotPresentError if specified
// key does not exist or has expired.
func (g *Gossip) GetInfo(key string) ([]byte, error) {
	i := func() *Info {
		g.mu.RLock()
		defer g.mu.RUnlock()
		return g.mu.is.getInfo(key)
	}()

	if i != nil {
		if err := i.Value.Verify([]byte(key)); err != nil {
			return nil, err
		}
		return i.Value.GetBytes()
	}
	return nil, NewKeyNotPresentError(key)
}

// GetInfoProto returns an info value by key or KeyNotPresentError if specified
// key does not exist or has expired.
func (g *Gossip) GetInfoProto(key string, msg protoutil.Message) error {
	bytes, err := g.GetInfo(key)
	if err != nil {
		return err
	}
	return protoutil.Unmarshal(bytes, msg)
}

// TryClearInfo attempts to clear an info object from the cluster's gossip
// network. It does so by retrieving the object with the corresponding key. If
// one does not exist, there's nothing to do and the method returns false.
// Otherwise, the method re-gossips the same key-value pair with a TTL that is
// long enough to reasonably ensure full propagation to all nodes in the cluster
// but short enough to expire quickly once propagated.
//
// The method is best-effort. It is possible for the info object with the low
// TTL to fail to reach full propagation before reaching its TTL. For instance,
// this is possible during a transient network partition. The effect of this is
// that the existing gossip info object with a higher (or no) TTL would remain
// in the gossip network on some nodes and may eventually propagate back out to
// other nodes once the partition heals.
func (g *Gossip) TryClearInfo(key string) (bool, error) {
	// Long enough to propagate to all nodes, short enough to expire quickly.
	const ttl = 1 * time.Minute
	return g.tryClearInfoWithTTL(key, ttl)
}

func (g *Gossip) tryClearInfoWithTTL(key string, ttl time.Duration) (bool, error) {
	val, err := g.GetInfo(key)
	if err != nil {
		if errors.HasType(err, KeyNotPresentError{}) {
			// Info object not known on this node. We can't force a deletion
			// preemptively, e.g. with a poison entry, because we do not have a valid
			// value object to populate and consumers may make assumptions about the
			// format of the value.
			return false, nil
		}
		return false, err
	}
	if err := g.AddInfo(key, val, ttl); err != nil {
		return false, err
	}
	return true, nil
}

// InfoOriginatedHere returns true iff the latest info for the provided key
// originated on this node. This is useful for ensuring that the system config
// is regossiped as soon as possible when its lease changes hands.
func (g *Gossip) InfoOriginatedHere(key string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	info := g.mu.is.getInfo(key)
	return g.infoOriginatedHere(info)
}

// infoOriginatedHere is a simple reusable helper for InfoOriginatedHere that
// doesn't involve taking the mutex.
func (g *Gossip) infoOriginatedHere(info *Info) bool {
	return info != nil && info.NodeID == g.NodeID.Get()
}

// GetInfoStatus returns the a copy of the contents of the infostore.
func (g *Gossip) GetInfoStatus() InfoStatus {
	clientStatus := g.clientStatus()
	serverStatus := g.server.status()

	g.mu.RLock()
	defer g.mu.RUnlock()
	is := InfoStatus{
		Infos:           make(map[string]Info),
		Client:          clientStatus,
		Server:          serverStatus,
		HighWaterStamps: g.mu.is.getHighWaterStamps(),
	}
	for k, v := range g.mu.is.Infos {
		is.Infos[k] = *protoutil.Clone(v).(*Info)
	}
	return is
}

// IterateInfos visits all infos matching the given prefix.
func (g *Gossip) IterateInfos(prefix string, visit func(k string, info Info) error) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for k, v := range g.mu.is.Infos {
		if strings.HasPrefix(k, prefix+separator) {
			if err := visit(k, *(protoutil.Clone(v).(*Info))); err != nil {
				return err
			}
		}
	}
	return nil
}

// Callback is a callback method to be invoked on gossip update of info denoted
// by key. origTimestamp is the info wall time when generated by the originating
// node.
type Callback func(key string, value roachpb.Value, origTimestamp int64)

// CallbackOption is a marker interface that callback options must implement.
type CallbackOption interface {
	apply(cb *callback)
}

type redundantCallbacks struct {
}

func (redundantCallbacks) apply(cb *callback) {
	cb.redundant = true
}

// Redundant is a callback option that specifies that the callback should be
// invoked even if the gossip value has not changed.
var Redundant redundantCallbacks

// RegisterCallback registers a callback for a key pattern to be
// invoked whenever new info for a gossip key matching pattern is
// received. The callback method is invoked with the info key which
// matched pattern. Returns a function to unregister the callback.
func (g *Gossip) RegisterCallback(pattern string, method Callback, opts ...CallbackOption) func() {
	unregister := func() func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.mu.is.registerCallback(pattern, method, opts...)
	}()
	return func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		unregister()
	}
}

// Incoming returns a slice of incoming gossip client connection
// node IDs.
func (g *Gossip) Incoming() []roachpb.NodeID {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.mu.incoming.asSlice()
}

// Outgoing returns a slice of outgoing gossip client connection
// node IDs. Note that these outgoing client connections may not
// actually be legitimately connected. They may be in the process
// of trying, or may already have failed, but haven't yet been
// processed by the gossip instance.
func (g *Gossip) Outgoing() []roachpb.NodeID {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.outgoing.asSlice()
}

// MaxHops returns the maximum number of hops to reach any other
// node in the system, according to the infos which have reached
// this node via gossip network.
func (g *Gossip) MaxHops() uint32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, maxHops := g.mu.is.mostDistant(func(_ roachpb.NodeID) bool { return false })
	return maxHops
}

// Start launches the gossip instance, which commences joining the
// gossip network using the supplied rpc server and previously known
// peer addresses in addition to any bootstrap addresses specified via
// --join and passed to this method via the addresses parameter.
//
// The supplied advertised address is used to identify the gossip
// instance in the gossip network; it will be used by other instances
// to connect to this instance.
//
// This method starts bootstrap loop, gossip server, and client management in
// separate goroutines and returns.
//
// The rpcContext is passed in here rather than at struct creation time to allow
// a looser coupling between the objects at construction.
func (g *Gossip) Start(
	advertAddr net.Addr, addresses []util.UnresolvedAddr, rpcContext *rpc.Context,
) {
	// Step 1: 断言未启动（防御性编程）
	g.AssertNotStarted(context.Background())

	// Step 2: 标记已启动
	g.started = true

	// Step 3: 设置 bootstrap 地址
	g.setAddresses(addresses)
	//   - 将 addresses 存入 g.addresses
	//   - 初始化 g.addressIdx = len(addresses) - 1
	//   - 清空 g.addressesTried
	//   - 发送信号到 g.stalledCh（如果有地址）

	// Step 4: 启动 Gossip 服务器
	g.server.start(advertAddr)
	//   - 设置 g.mu.is.NodeAddr = advertAddr
	//   - 注册全局回调：任何 info 变更 → broadcast()
	//   - 启动 "gossip-wait-quiesce" goroutine

	// Step 5: 启动 Bootstrap 循环
	g.bootstrap(rpcContext)
	//   - 启动 "gossip-bootstrap" goroutine
	//   - 持续检查是否需要 bootstrap
	//   - 通过 g.stalledCh 被唤醒

	// Step 6: 启动 Management 循环
	g.manage(rpcContext)
	//   - 启动 "gossip-manage" goroutine
	//   - 处理断开连接、tighten、cull、stall 检查
}

// hasIncomingLocked returns whether the server has an incoming gossip
// client matching the provided node ID. Mutex should be held by
// caller.
func (g *Gossip) hasIncomingLocked(nodeID roachpb.NodeID) bool {
	return g.mu.incoming.hasNode(nodeID)
}

// hasOutgoingLocked returns whether the server has an outgoing gossip
// client matching the provided node ID. Mutex should be held by
// caller.
func (g *Gossip) hasOutgoingLocked(nodeID roachpb.NodeID) bool {
	// We have to use findClient and compare node addresses rather than using the
	// outgoing nodeSet due to the way that outgoing clients' node IDs are only
	// resolved once the connection has been established (rather than as soon as
	// we've created it).
	nodeAddr, _, err := g.getNodeIDAddress(nodeID, true /* locked */)
	if err != nil {
		// If we don't have the address, fall back to using the outgoing nodeSet
		// since at least it's better than nothing.
		ctx := g.AnnotateCtx(context.TODO())
		log.Dev.Errorf(ctx, "unable to get address for n%d: %s", nodeID, err)
		return g.outgoing.hasNode(nodeID)
	}
	c := g.findClient(func(c *client) bool {
		return c.addr.String() == nodeAddr.String()
	})
	return c != nil
}

// getNextBootstrapAddress returns the next available bootstrap
// address. The caller must hold the lock.
func (g *Gossip) getNextBootstrapAddressLocked() util.UnresolvedAddr {
	// Round-robin 遍历所有地址
	for range g.addresses {
		g.addressIdx++
		g.addressIdx %= len(g.addresses)
		g.addressesTried[g.addressIdx] = struct{}{}
		addr := g.addresses[g.addressIdx]

		// 跳过正在尝试的地址
		if _, addrActive := g.bootstrapping[addr.String()]; !addrActive {
			g.bootstrapping[addr.String()] = struct{}{}
			return addr
		}
	}
	return util.UnresolvedAddr{} // 所有地址都在尝试中
}

// bootstrap connects the node to the gossip network. Bootstrapping
// commences in the event there are no connected clients or the
// sentinel gossip info is not available. After a successful bootstrap
// connection, this method will block on the stalled condvar, which
// receives notifications that gossip network connectivity has been
// lost and requires re-bootstrapping.
// ### **主动连接 Bootstrap 节点**
// **时机**：
// 1. **启动时立即触发**（如果没有连接或缺少 sentinel）
// 2. **连接断开后被唤醒**（通过 `g.stalledCh`）
// 3. **定期检查**（`bootstrapInterval`，默认 1s）
// **关键决策点**：
// - **何时 bootstrap？**
// - 条件 1：`g.outgoing.len() == 0`（没有出向连接）
// - 条件 2：`g.mu.is.getInfo(KeySentinel) == nil`（缺少 sentinel，表示未连接到集群）
//
// - **Sentinel Key 的作用**：
//   - Sentinel 是一个由集群的”第一个节点”定期 gossip 的特殊 key
//   - 如果一个节点能收到 sentinel，说明它连接到了正常的集群
//   - 如果收不到，说明可能处于网络分区或所有连接都是无效的
func (g *Gossip) bootstrap(rpcContext *rpc.Context) {
	ctx := g.AnnotateCtx(context.Background())
	_ = g.server.stopper.RunAsyncTask(ctx, "gossip-bootstrap", func(ctx context.Context) {
		ctx = logtags.AddTag(ctx, "bootstrap", nil)
		var bootstrapTimer timeutil.Timer
		defer bootstrapTimer.Stop()
		for {
			// 1️⃣ 检查是否需要 bootstrap
			func(ctx context.Context) {
				g.mu.Lock()
				defer g.mu.Unlock()
				haveClients := g.outgoing.len() > 0
				haveSentinel := g.mu.is.getInfo(KeySentinel) != nil
				log.Eventf(ctx, "have clients: %t, have sentinel: %t", haveClients, haveSentinel)
				if !haveClients || !haveSentinel {
					// 需要 bootstrap
					// Try to get another bootstrap address.
					//
					// TODO(baptist): The bootstrap address from the
					// configuration does not have locality information. We
					// could use "our" locality or leave it blank like it
					// currently does. Alternatively we could break and
					// reconnect once we determine the remote locality from
					// gossip.
					if addr := g.getNextBootstrapAddressLocked(); !addr.IsEmpty() {
						locality := roachpb.Locality{}
						nodeID := g.bootstrapAddrs[addr]
						// We may not have a node descriptor for this node yet, in which case we dial without one.
						if nd, err := g.getNodeDescriptor(nodeID, true); err == nil {
							locality = nd.Locality
						}
						g.startClientLocked(addr, locality, rpcContext)
					} else {
						// 没有可用地址，标记为 stalled
						bootstrapAddrs := make([]string, 0, len(g.bootstrapping))
						for addr := range g.bootstrapping {
							bootstrapAddrs = append(bootstrapAddrs, addr)
						}
						log.Eventf(ctx, "no next bootstrap address; currently bootstrapping: %v", bootstrapAddrs)
						// We couldn't start a client, signal that we're stalled so that
						// we'll retry.
						// 没有可用地址，标记为 stalled
						g.maybeSignalStatusChangeLocked()
					}
				}
			}(ctx)

			// 2️⃣ 暂停一段时间后继续
			// Pause an interval before next possible bootstrap.
			bootstrapTimer.Reset(g.bootstrapInterval)
			log.Eventf(ctx, "sleeping %s until bootstrap", g.bootstrapInterval)
			select {
			case <-bootstrapTimer.C:
				// continue
			case <-g.server.stopper.ShouldQuiesce():
				return
			}
			log.Eventf(ctx, "idling until bootstrap required")
			// Block until we need bootstrapping again.
			// 3️⃣ 阻塞直到需要再次 bootstrap
			select {
			case <-g.stalledCh:
				log.Eventf(ctx, "detected stall; commencing bootstrap")
				// continue
			case <-g.server.stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// manage manages outgoing clients. Periodically, the infostore is
// scanned for infos with hop count exceeding the maxHops
// threshold. If the number of outgoing clients doesn't exceed
// maxPeers(), a new gossip client is connected to a randomly selected
// peer beyond maxHops threshold. Otherwise, the least useful peer
// node is cut off to make room for a replacement. Disconnected
// clients are processed via the disconnected channel and taken out of
// the outgoing address set. If there are no longer any outgoing
// connections or the sentinel gossip is unavailable, the bootstrapper
// is notified via the stalled conditional variable.
// Management Loop：连接维护与优化
// 时机：
// 1. 客户端断开连接时（g.disconnected channel）
// 2. 收到 tighten 信号时（g.tighten channel）
// 3. 定期淘汰低效连接（cullTimer，默认 60s）
// 4. 定期检查 stall 状态（stallTimer，默认 1s）
func (g *Gossip) manage(rpcContext *rpc.Context) {
	ctx := g.AnnotateCtx(context.Background())
	_ = g.server.stopper.RunAsyncTask(ctx, "gossip-manage", func(ctx context.Context) {
		var cullTimer, stallTimer timeutil.Timer
		defer cullTimer.Stop()
		defer stallTimer.Stop()
		rng, _ := randutil.NewPseudoRand()

		cullTimer.Reset(jitteredInterval(g.cullInterval, rng))
		stallTimer.Reset(jitteredInterval(g.stallInterval, rng))
		for {
			select {
			case <-g.server.stopper.ShouldQuiesce():
				return
			case c := <-g.disconnected:
				//客户端断开
				g.doDisconnected(c, rpcContext)
			case <-g.tighten: //Tighten 网络
				g.tightenNetwork(ctx, rpcContext)
			case <-cullTimer.C:
				//淘汰低效连接（cullTimer）：
				//- **触发时机**：每隔 `cullInterval`（默认 60s）
				//- **目的**：释放出空间，为更优的连接让路
				//- **选择策略**：关闭”最不有用”的客户端（`leastUseful`，基于信息新鲜度和跳数计算）
				cullTimer.Reset(jitteredInterval(g.cullInterval, rng))
				func() {
					g.mu.Lock()
					if !g.outgoing.hasSpace() {
						leastUsefulID := g.mu.is.leastUseful(g.outgoing)

						if c := g.findClient(func(c *client) bool {
							return c.peerID == leastUsefulID
						}); c != nil {
							log.VEventf(ctx, 1, "closing least useful client %+v to tighten network graph", c)
							log.Health.Infof(ctx, "closing gossip client n%d %s", c.peerID, c.addr)
							c.close()

							// After releasing the lock, block until the client disconnects.
							defer func() {
								g.doDisconnected(<-g.disconnected, rpcContext)
							}()
						} else {
							if log.V(1) {
								func() {
									g.clientsMu.Lock()
									defer g.clientsMu.Unlock()
									log.Dev.Infof(ctx, "couldn't find least useful client among %+v", g.clientsMu.clients)
								}()
							}
						}
					}
					g.mu.Unlock()
				}()
			case <-stallTimer.C:
				//2. **检查 Stall 状态（`stallTimer`）**：
				//    - **触发时机**：每隔 `stallInterval`（默认 1s）
				//    - **目的**：检测是否失去连接或 sentinel，及时触发 bootstrap
				stallTimer.Reset(jitteredInterval(g.stallInterval, rng))
				func() {
					g.mu.Lock()
					defer g.mu.Unlock()
					g.maybeSignalStatusChangeLocked()
				}()
			}
		}
	})
}

// jitteredInterval returns a randomly jittered (+/-25%) duration
// from checkInterval.
func jitteredInterval(interval time.Duration, rng *rand.Rand) time.Duration {
	return time.Duration(float64(interval) * (0.75 + 0.5*rng.Float64()))
}

// tightenNetwork "tightens" the network by starting a new gossip client to the
// client to the most distant node to which we don't already have an outgoing
// connection. Does nothing if we don't have room for any more outgoing
// connections.
func (g *Gossip) tightenNetwork(ctx context.Context, rpcContext *rpc.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := timeutil.Now()
	// 防抖：距离上次 tighten 不到 1s，跳过
	if now.Before(g.mu.lastTighten.Add(gossipTightenInterval)) {
		// It hasn't been long since we last tightened the network, so skip it.
		return
	}
	g.mu.lastTighten = now

	if g.outgoing.hasSpace() {
		distantNodeID, distantHops := g.mu.is.mostDistant(g.hasOutgoingLocked)
		log.VEventf(ctx, 2, "distantHops: %d from %d", distantHops, distantNodeID)
		if distantHops <= maxHops {
			return // 网络已经足够紧密
		}
		// If tightening is needed, then reset lastTighten to avoid restricting how
		// soon we try again.
		// 连接到最远的节点
		g.mu.lastTighten = time.Time{}
		if nodeAddr, locality, err := g.getNodeIDAddress(distantNodeID, true /* locked */); err != nil || nodeAddr == nil {
			log.Health.Errorf(ctx, "unable to get address for n%d: %s", distantNodeID, err)
		} else {
			log.Health.Infof(ctx, "starting client to n%d (%d > %d) to tighten network graph",
				distantNodeID, distantHops, maxHops)
			log.Eventf(ctx, "tightening network with new client to %s", nodeAddr)
			g.startClientLocked(*nodeAddr, locality, rpcContext)
		}
	}
}

func (g *Gossip) doDisconnected(c *client, rpcContext *rpc.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removeClientLocked(c) // 从 outgoing 中移除

	// If the client was disconnected with a forwarding address, connect now.
	// 如果断开时带了转发地址，立即连接
	if c.forwardAddr != nil {
		locality := roachpb.Locality{}
		// If we have a node descriptor for this node use it when dialing.
		if nd, err := g.getNodeDescriptor(c.peerID, true); err == nil {
			locality = nd.Locality
		}
		g.startClientLocked(*c.forwardAddr, locality, rpcContext)
	}
	g.maybeSignalStatusChangeLocked()
}

// maybeSignalStatusChangeLocked checks whether gossip should transition its
// internal state from connected to stalled or vice versa.
// ## **使用上下文**
// 这个函数在以下情况下被调用：
// 1. 网络连接状态可能因失去连接而变化时
// 2. 在维护例程中定期检查网络状态时
// 3. 在更新节点地址列表后需要重新评估连接状态时
// 4. 当客户端断开连接后需要检查是否要切换状态时
func (g *Gossip) maybeSignalStatusChangeLocked() {
	ctx := g.AnnotateCtx(context.TODO())
	//是否有传入和传出的连接（为0表示无连接）
	orphaned := g.outgoing.len()+g.mu.incoming.len() == 0
	//是否是一个多节点集群（根据引导地址列表长度判断）
	multiNode := len(g.bootstrapInfo.Addresses) > 0
	// We're stalled if we don't have the sentinel key, or if we're a multi node
	// cluster and have no gossip connections.
	//- 网络是否处于"stalled"状态，条件是：
	//    - 同时是多节点集群且无连接（orphaned && multiNode）
	//    - 或者缺少主信息（g.mu.is.getInfo(KeySentinel) == nil）
	stalled := (orphaned && multiNode) || g.mu.is.getInfo(KeySentinel) == nil
	if stalled {
		//如果检测到stalled状态且之前不是stalled状态：
		// We employ the stalled boolean to avoid filling logs with warnings.
		if !g.stalled {
			log.Eventf(ctx, "now stalled")
			if orphaned {
				// 分情况输出了不同警告的日志记录
				if len(g.addresses) == 0 {
					log.Ops.Warningf(ctx, "no addresses found; use --join to specify a connected node")
				} else {
					log.Health.Warningf(ctx, "no incoming or outgoing connections")
				}
			} else if len(g.addressesTried) == len(g.addresses) {
				log.Health.Warningf(ctx, "first range unavailable; addresses exhausted")
			} else {
				log.Health.Warningf(ctx, "first range unavailable; trying remaining addresses")
			}
		}
		if len(g.addresses) > 0 {
			g.signalStalledLocked() //发送stalled信号
		}
	} else {
		//处理恢复正常连接状态
		if g.stalled {
			log.Eventf(ctx, "connected")
			log.Ops.Infof(ctx, "node has connected to cluster via gossip")
			g.signalConnectedLocked() //发送已连接信号
		}
		g.maybeCleanupBootstrapAddressesLocked() //清理引导地址
	}
	//状态更新
	g.stalled = stalled
}

func (g *Gossip) signalStalledLocked() {
	select {
	case g.stalledCh <- struct{}{}:
	default:
	}
}

// signalConnectedLocked checks whether this gossip instance is connected to
// enough of the gossip network that it has received the cluster ID gossip
// info. Once connected, the "Connected" channel is closed to signal to any
// waiters that the gossip instance is ready. The gossip mutex should be held
// by caller.
//
// TODO(tschottdorf): this is called from various locations which seem ad-hoc
// (with the exception of the call bootstrap loop) yet necessary. Consolidate
// and add commentary at each callsite.
func (g *Gossip) signalConnectedLocked() {
	// Check if we have the cluster ID gossip to start.
	// If so, then mark ourselves as trivially connected to the gossip network.
	if !g.hasConnected && g.mu.is.getInfo(KeyClusterID) != nil {
		g.hasConnected = true
		close(g.Connected)
	}
}

// startClientLocked launches a new client connected to remote address.
// The client is added to the outgoing address set and launched in
// 创建一个新的 client 实例，连接到指定地址，并启动 gossip 循环。
// a goroutine.
func (g *Gossip) startClientLocked(
	addr util.UnresolvedAddr, locality roachpb.Locality, rpcContext *rpc.Context,
) {
	g.clientsMu.Lock()
	defer g.clientsMu.Unlock()
	ctx := g.AnnotateCtx(context.TODO())
	log.VEventf(ctx, 1, "starting new client to %s", addr)
	// 1️⃣ 创建新客户端
	c := newClient(g.server.AmbientContext, &addr, locality, g.serverMetrics)
	// 2️⃣ 加入客户端列表
	g.clientsMu.clients = append(g.clientsMu.clients, c)
	// 3️⃣ 启动客户端（异步）
	c.startLocked(g, g.disconnected, rpcContext, g.server.stopper)
}

// removeClientLocked removes the specified client. Called when a client
// disconnects.
func (g *Gossip) removeClientLocked(target *client) {
	g.clientsMu.Lock()
	defer g.clientsMu.Unlock()
	for i, candidate := range g.clientsMu.clients {
		if candidate == target {
			ctx := g.AnnotateCtx(context.TODO())
			log.VEventf(ctx, 1, "client %s disconnected", candidate.addr)
			g.clientsMu.clients = append(g.clientsMu.clients[:i], g.clientsMu.clients[i+1:]...)
			delete(g.bootstrapping, candidate.addr.String())
			g.outgoing.removeNode(candidate.peerID)
			break
		}
	}
}

func (g *Gossip) findClient(match func(*client) bool) *client {
	g.clientsMu.Lock()
	defer g.clientsMu.Unlock()
	for _, c := range g.clientsMu.clients {
		if match(c) {
			return c
		}
	}
	return nil
}

// TestingAddInfoProtoAndWaitForAllCallbacks adds an info proto, and waits for all
// matching callbacks to get called before returning. It's only intended to be
// used for tests that assert on the result of the gossip propagation.
func (g *Gossip) TestingAddInfoProtoAndWaitForAllCallbacks(
	key string, msg protoutil.Message, ttl time.Duration,
) error {
	// Take the lock to avoid races where a callback could be added while this
	// method is waiting for matching callbacks to be called.
	g.mu.Lock()
	defer g.mu.Unlock()

	wg := &sync.WaitGroup{}

	// Increment the wait group once per matching callback. It will be decremented
	// once the processing is complete.
	for _, cb := range g.mu.is.callbacks {
		if cb.matcher.MatchString(key) {
			wg.Add(1)
		}
	}

	// Add the target info to the infoStore. This will trigger the registered
	// callbacks to be called.
	bytes, err := protoutil.Marshal(msg)
	if err != nil {
		return err
	}
	if err := g.addInfoLocked(key, bytes, ttl); err != nil {
		return err
	}

	// At this point, we know that the callbacks that will be called have been
	// added to the work queues. Now, we can append an entry item at the end of
	// the matching callback's work queue that will decrement the wait group that
	// was incremented earlier. This ensures that ALL matching callbacks have
	// been called.
	for _, cb := range g.mu.is.callbacks {
		if cb.matcher.MatchString(key) {
			cb.cw.mu.Lock()
			cb.cw.mu.workQueue = append(cb.cw.mu.workQueue, callbackWorkItem{
				method: func(_ string, _ roachpb.Value, _ int64) {
					wg.Done()
				},
				schedulingTime: crtime.NowMono(),
			})
			cb.cw.mu.Unlock()
		}

		// Make sure to notify the callback worker that there is work to do.
		select {
		case cb.cw.callbackCh <- struct{}{}:
		default:
		}
	}

	// Wait for all the callbacks to finish processing.
	wg.Wait()
	return nil
}

// A firstRangeMissingError indicates that the first range has not yet
// been gossiped. This will be the case for a node which hasn't yet
// joined the gossip network.
type firstRangeMissingError struct{}

// Error is part of the error interface.
func (f firstRangeMissingError) Error() string {
	return "the descriptor for the first range is not available via gossip"
}

// GetFirstRangeDescriptor implements kvcoord.FirstRangeProvider.
func (g *Gossip) GetFirstRangeDescriptor() (*roachpb.RangeDescriptor, error) {
	desc := &roachpb.RangeDescriptor{}
	if err := g.GetInfoProto(KeyFirstRangeDescriptor, desc); err != nil {
		return nil, firstRangeMissingError{}
	}
	return desc, nil
}

// OnFirstRangeChanged implements kvcoord.FirstRangeProvider.
func (g *Gossip) OnFirstRangeChanged(cb func(*roachpb.RangeDescriptor)) {
	g.RegisterCallback(KeyFirstRangeDescriptor, func(_ string, value roachpb.Value, _ int64) {
		ctx := context.Background()
		desc := &roachpb.RangeDescriptor{}
		if err := value.GetProto(desc); err != nil {
			log.Dev.Errorf(ctx, "unable to parse gossiped first range descriptor: %s", err)
		} else {
			cb(desc)
		}
	})
}

// MakeOptionalGossip initializes an OptionalGossip instance wrapping a
// (possibly nil) *Gossip.
//
// Use of Gossip from within the SQL layer is **deprecated**. Please do not
// introduce new uses of it.
//
// See TenantSQLDeprecatedWrapper for details.
func MakeOptionalGossip(g *Gossip) OptionalGossip {
	return OptionalGossip{
		w: errorutil.MakeTenantSQLDeprecatedWrapper(g, g != nil),
	}
}

// OptionalGossip is a Gossip instance in a SQL tenant server.
//
// Use of Gossip from within the SQL layer is **deprecated**. Please do not
// introduce new uses of it.
//
// See TenantSQLDeprecatedWrapper for details.
type OptionalGossip struct {
	w errorutil.TenantSQLDeprecatedWrapper
}

// OptionalErr returns the Gossip instance if the wrapper was set up to allow
// it. Otherwise, it returns an error referring to the optionally passed in
// issues.
//
// Use of Gossip from within the SQL layer is **deprecated**. Please do not
// introduce new uses of it.
func (og OptionalGossip) OptionalErr(issue int) (*Gossip, error) {
	v, err := og.w.OptionalErr(issue)
	if err != nil {
		return nil, err
	}
	// NB: some tests use a nil Gossip.
	g, _ := v.(*Gossip)
	return g, nil
}

// Optional is like OptionalErr, but returns false if Gossip is not exposed.
//
// Use of Gossip from within the SQL layer is **deprecated**. Please do not
// introduce new uses of it.
func (og OptionalGossip) Optional(issue int) (*Gossip, bool) {
	v, ok := og.w.Optional()
	if !ok {
		return nil, false
	}
	// NB: some tests use a nil Gossip.
	g, _ := v.(*Gossip)
	return g, true
}
