// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package settingswatcher provides utilities to update cluster settings using
// a range feed.
package settingswatcher

import (
	"context"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// SettingsWatcher is used to watch for cluster settings changes with a
// rangefeed.
type SettingsWatcher struct {
	clock    *hlc.Clock
	codec    keys.SQLCodec
	settings *cluster.Settings
	f        *rangefeed.Factory
	stopper  *stop.Stopper
	dec      RowDecoder

	// storage is used to persist a local cache of the setting
	// overrides, for use when a node starts up before KV is ready.
	storage Storage
	// snapshot is what goes into the local cache.
	snapshot []roachpb.KeyValue

	overridesMonitor OverridesMonitor

	mu struct {
		syncutil.Mutex

		updater settings.Updater

		// values represent the values read from the system.settings
		// table.
		values map[settings.InternalKey]settingsValue
		// overrides represent the values obtained from the overrides
		// monitor, if any.
		overrides map[settings.InternalKey]settings.EncodedValue

		// storageClusterVersion is the cache of the storage cluster version
		// inside secondary tenants. It will be uninitialized in a system
		// tenant.
		storageClusterVersion clusterversion.ClusterVersion

		// Used by TestingRestart.
		updateWait chan struct{}
	}

	// notifySystemVisibleChange is called when one or more
	// SystemVisible setting changes. It is only set when the
	// SettingsWatcher is created with NewWithNotifier. It is used by
	// the tenant setting override watcher to pick up defaults set via
	// system.settings in the system tenant.
	//
	// The callee function can assume that the slice in the second
	// argument is sorted by InternalKey.
	notifySystemVisibleChange func(context.Context, []kvpb.TenantSetting)

	// testingWatcherKnobs allows the client to inject testing knobs into
	// the underlying rangefeedcache.Watcher.
	testingWatcherKnobs *rangefeedcache.TestingKnobs

	// rfc provides access to the underlying rangefeedcache.Watcher for
	// testing.
	rfc *rangefeedcache.Watcher[*kvpb.RangeFeedValue]
}

// Storage is used to write a snapshot of KVs out to disk for use upon restart.
type Storage interface {
	SnapshotKVs(ctx context.Context, kvs []roachpb.KeyValue)
}

// New constructs a new SettingsWatcher.
func New(
	clock *hlc.Clock,
	codec keys.SQLCodec,
	settingsToUpdate *cluster.Settings,
	f *rangefeed.Factory,
	stopper *stop.Stopper,
	storage Storage, // optional
) *SettingsWatcher {
	s := &SettingsWatcher{
		clock:    clock,
		codec:    codec,
		settings: settingsToUpdate,
		f:        f,
		stopper:  stopper,
		dec:      MakeRowDecoder(codec),
		storage:  storage,
	}
	s.mu.updateWait = make(chan struct{})
	return s
}

// NewWithNotifier constructs a new SettingsWatcher which notifies
// an observer about changes to SystemVisible settings.
func NewWithNotifier(
	ctx context.Context,
	clock *hlc.Clock,
	codec keys.SQLCodec,
	settingsToUpdate *cluster.Settings,
	f *rangefeed.Factory,
	stopper *stop.Stopper,
	notify func(context.Context, []kvpb.TenantSetting),
	storage Storage, // optional
) *SettingsWatcher {
	w := New(clock, codec, settingsToUpdate, f, stopper, storage)
	w.notifySystemVisibleChange = notify
	return w
}

// NewWithOverrides constructs a new SettingsWatcher which allows external
// overrides, discovered through an OverridesMonitor.
func NewWithOverrides(
	clock *hlc.Clock,
	codec keys.SQLCodec,
	settingsToUpdate *cluster.Settings,
	f *rangefeed.Factory,
	stopper *stop.Stopper,
	overridesMonitor OverridesMonitor,
	storage Storage,
) *SettingsWatcher {
	s := New(clock, codec, settingsToUpdate, f, stopper, storage)
	s.overridesMonitor = overridesMonitor
	settingsToUpdate.OverridesInformer = s
	return s
}

// Start 启动 SettingsWatcher。它在检索到初始设置后返回。
// 如果在检索到初始数据之前上下文被取消或停止器停止，则返回错误。
func (s *SettingsWatcher) Start(ctx context.Context) error {
	// 1. [初始默认值] 加载 SystemVisible 设置的编译时默认值。
	// 确保在从磁盘加载值之前，内存中已有基本的默认值。
	s.loadInitialReadOnlyDefaults(ctx)

	settingsTablePrefix := s.codec.TablePrefix(keys.SettingsTableID)
	settingsTableSpan := roachpb.Span{
		Key:    settingsTablePrefix,
		EndKey: settingsTablePrefix.PrefixEnd(),
	}
	s.resetUpdater()
	initialScan := struct {
		ch   chan struct{}
		done bool
		err  error
	}{
		ch: make(chan struct{}),
	}
	// noteUpdate 处理来自 rangefeedcache 的更新事件。
	noteUpdate := func(update rangefeedcache.Update[*kvpb.RangeFeedValue]) {
		if update.Type != rangefeedcache.CompleteUpdate {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.mu.updater.ResetRemaining(ctx)
		// 初始扫描完成后，关闭通道以解除 Start 方法的阻塞。
		if !initialScan.done {
			log.Dev.VInfof(ctx, 1, "initial settings scan complete")
			initialScan.done = true
			close(initialScan.ch)
		}
		// 用于 TestingRestart()。
		close(s.mu.updateWait)
		s.mu.updateWait = make(chan struct{})
	}

	s.mu.values = make(map[settings.InternalKey]settingsValue)

	// 2. [配置覆盖] 如果配置了覆盖监控器，则先初始化覆盖。
	if s.overridesMonitor != nil {
		s.mu.overrides = make(map[settings.InternalKey]settings.EncodedValue)
		// 等待监控器就绪。
		if err := s.overridesMonitor.WaitForStart(ctx); err != nil {
			return err
		}
		// 同步获取初始覆盖。
		overridesCh := s.updateOverrides(ctx)
		log.Dev.Infof(ctx, "applied initial setting overrides")

		// 启动异步 worker 监听后续覆盖变更。
		if err := s.stopper.RunAsyncTask(ctx, "setting-overrides", func(ctx context.Context) {
			for {
				select {
				case <-overridesCh:
					overridesCh = s.updateOverrides(ctx)

				case <-s.stopper.ShouldQuiesce():
					return
				}
			}
		}); err != nil {
			return err
		}
	}

	var bufferSize int
	if s.storage != nil {
		bufferSize = settings.MaxSettings * 3
	}

	// 3. [RangeFeed 监听] 创建 Watcher 监听 system.settings 表。
	c := rangefeedcache.NewWatcher(
		"settings-watcher",
		s.clock, s.f,
		bufferSize,
		[]roachpb.Span{settingsTableSpan},
		false, // withPrevValue
		true,  // withRowTSInInitialScan
		func(ctx context.Context, kv *kvpb.RangeFeedValue) (*kvpb.RangeFeedValue, bool) {
			// 处理具体的 KV 更新事件。
			return s.handleKV(ctx, kv)
		},
		func(ctx context.Context, update rangefeedcache.Update[*kvpb.RangeFeedValue]) {
			noteUpdate(update)
		},
		s.testingWatcherKnobs,
	)
	s.rfc = c

	allowedFailures := 10

	// 启动 rangefeedcache。
	if err := rangefeedcache.Start(ctx, s.stopper, c, func(err error) {
		if !initialScan.done {
			// 在租户处于 service mode "none" 时允许一定次数的失败重试。
			if strings.Contains(err.Error(), `operation not allowed when in service mode "none"`) && allowedFailures > 0 {
				allowedFailures--
			} else {
				initialScan.err = err
				initialScan.done = true
				close(initialScan.ch)
			}
		} else {
			s.resetUpdater()
		}
	}); err != nil {
		return err
	}

	// 4. [阻塞等待] 等待初始扫描完成。
	select {
	case <-initialScan.ch:
		return initialScan.err

	case <-s.stopper.ShouldQuiesce():
		return errors.Wrap(stop.ErrUnavailable, "failed to retrieve initial cluster settings")

	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "failed to retrieve initial cluster settings")
	}
}

func (s *SettingsWatcher) loadInitialReadOnlyDefaults(ctx context.Context) {
	if s.notifySystemVisibleChange == nil {
		return
	}

	// When there is no explicit value in system.settings for a SystemVisible
	// setting, we still want to propagate the system tenant's idea
	// of the default value as an override to secondary tenants.
	//
	// This is because the secondary tenant may be using another version
	// of the executable, where there is another default value for the
	// setting. We want to make sure that the secondary tenant's idea of
	// the default value is the same as the system tenant's.

	tenantReadOnlyKeys := settings.SystemVisibleKeys()
	payloads := make([]kvpb.TenantSetting, 0, len(tenantReadOnlyKeys))
	for _, key := range tenantReadOnlyKeys {
		knownSetting, payload := s.getSettingAndValue(key)
		if !knownSetting {
			panic(errors.AssertionFailedf("programming error: unknown setting %s", key))
		}
		payloads = append(payloads, payload)
	}
	// Make sure the payloads are sorted, as this is required by the
	// notify API.
	sort.Slice(payloads, func(i, j int) bool { return payloads[i].InternalKey < payloads[j].InternalKey })
	s.notifySystemVisibleChange(ctx, payloads)
}

// TestingRestart restarts the rangefeeds and waits for the initial
// update after the rangefeed update to be processed.
func (s *SettingsWatcher) TestingRestart() {
	if s.rfc != nil {
		s.mu.Lock()
		waitCh := s.mu.updateWait
		s.mu.Unlock()
		s.rfc.TestingRestart()
		<-waitCh
	}
}

// handleKV 处理来自 rangefeed 的 KV 事件。
func (s *SettingsWatcher) handleKV(
	ctx context.Context, kv *kvpb.RangeFeedValue,
) (*kvpb.RangeFeedValue, bool) {
	rkv := roachpb.KeyValue{
		Key:   kv.Key,
		Value: kv.Value,
	}

	var alloc tree.DatumAlloc
	// 解码 system.settings 表的行。
	settingKeyS, val, tombstone, err := s.dec.DecodeRow(rkv, &alloc)
	if err != nil {
		err = errors.NewAssertionErrorWithWrappedErrf(err, "failed to decode settings row %v", kv.Key)
		logcrash.ReportOrPanic(ctx, &s.settings.SV, "%w", err)
		return nil, false
	}
	settingKey := settings.InternalKey(settingKeyS)

	// 查找对应的本地设置。
	setting, ok := settings.LookupForLocalAccessByKey(settingKey, s.codec.ForSystemTenant())
	if !ok {
		log.Dev.Warningf(ctx, "unknown setting %s, skipping update", settingKey)
		return nil, false
	}
	// 安全检查。
	if !s.codec.ForSystemTenant() {
		if setting.Class() != settings.ApplicationLevel {
			log.Dev.Warningf(ctx, "ignoring read-only setting %s", settingKey)
			return nil, false
		}
	}

	log.VEventf(ctx, 1, "found rangefeed event for %q = %+v (tombstone=%v)", settingKey, val, tombstone)

	// 如果配置了存储，则持久化到本地缓存快照。
	if s.storage != nil {
		s.snapshot = rangefeedbuffer.MergeKVs(s.snapshot, []roachpb.KeyValue{rkv})
		s.storage.SnapshotKVs(ctx, s.snapshot)
	}

	// 尝试将新值设置到内存中的 settings 结构中。
	s.maybeSet(ctx, settingKey, settingsValue{
		val:       val,
		ts:        kv.Value.Timestamp,
		tombstone: tombstone,
	}, setting.Class())
	if s.storage != nil {
		return kv, true
	}
	return nil, false
}

// maybeSet will update the stored value and the corresponding setting
// in response to a kv event, assuming that event is new.
func (s *SettingsWatcher) maybeSet(
	ctx context.Context, key settings.InternalKey, sv settingsValue, class settings.Class,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Skip updates which have an earlier timestamp to avoid regressing on the
	// value of a setting. Note that we intentionally process values at the same
	// timestamp as the current value. This is important to deal with cases where
	// the underlying rangefeed restarts. When that happens, we'll construct a
	// new settings updater and expect to re-process every setting which is
	// currently set.
	if existing, ok := s.mu.values[key]; ok && sv.ts.Less(existing.ts) {
		return
	}
	_, hasOverride := s.mu.overrides[key]
	s.mu.values[key] = sv
	if !hasOverride {
		// We only update the in-RAM value of the setting if there is no
		// override. If there was an override, the override would have
		// been set already via updateOverrides().
		s.applyValueFromSystemSettingsOrDefaultLocked(ctx, key)
	}

	if class == settings.SystemVisible {
		// Notify the tenant settings watcher there is a new fallback
		// default for this setting.
		s.setSystemVisibleDefault(ctx, key)
	}
}

// settingValue tracks an observed value from the rangefeed. By tracking the
// timestamp, we can avoid regressing the settings values in the face of
// rangefeed restarts.
type settingsValue struct {
	val       settings.EncodedValue
	ts        hlc.Timestamp
	tombstone bool
}

// set the current value of a setting.
func (s *SettingsWatcher) setLocked(
	ctx context.Context,
	key settings.InternalKey,
	val settings.EncodedValue,
	origin settings.ValueOrigin,
) {
	// Both the system tenant and secondary tenants no longer use this code
	// path to propagate cluster version changes (they rely on
	// BumpClusterVersion instead). The secondary tenants however, still rely
	// on the initial pass through this code (in settingsWatcher.Start()) to
	// bootstrap the initial cluster version on tenant startup. In all other
	// instances, this code should no-op (either because we're in the system
	// tenant, or because the new version <= old version).
	if key == clusterversion.KeyVersionSetting && !s.codec.ForSystemTenant() {
		var newVersion clusterversion.ClusterVersion
		oldVersion := s.settings.Version.ActiveVersionOrEmpty(ctx)
		if err := protoutil.Unmarshal([]byte(val.Value), &newVersion); err != nil {
			log.Dev.Warningf(ctx, "failed to set cluster version: %s", err.Error())
		} else if newVersion.LessEq(oldVersion.Version) {
			// Nothing to do.
		} else {
			// Check if cluster version setting is initialized. If it is empty then it is not
			// initialized.
			if oldVersion.Version.Equal(roachpb.Version{}) {
				// Cluster version setting not initialized.
				if err := clusterversion.Initialize(ctx, newVersion.Version, &s.settings.SV); err != nil {
					log.Dev.Fatalf(ctx, "failed to initialize cluster version setting: %s", err.Error())
					return
				}
			}
			if err := s.settings.Version.SetActiveVersion(ctx, newVersion); err != nil {
				log.Dev.Warningf(ctx, "failed to set cluster version: %s", err.Error())
			} else {
				log.Dev.Infof(ctx, "set cluster version from %v to: %v", oldVersion, newVersion)
			}
		}
		return
	}

	if err := s.mu.updater.SetFromStorage(ctx, key, val, origin); err != nil {
		log.Dev.Warningf(ctx, "failed to set setting %s to %s: %v", key, val.Value, err)
	}
}

// setDefaultLocked sets a setting to its default value.
func (s *SettingsWatcher) setDefaultLocked(ctx context.Context, key settings.InternalKey) {
	if err := s.mu.updater.SetToDefault(ctx, key); err != nil {
		log.Dev.Warningf(ctx, "failed to set setting %s to default: %v", key, err)
	}
}

// updateOverrides updates the overrides map and updates any settings
// accordingly.
func (s *SettingsWatcher) updateOverrides(ctx context.Context) (updateCh <-chan struct{}) {
	var newOverrides map[settings.InternalKey]settings.EncodedValue
	newOverrides, updateCh = s.overridesMonitor.Overrides()

	s.mu.Lock()
	defer s.mu.Unlock()

	for key, val := range newOverrides {
		if key == clusterversion.KeyVersionSetting {
			var newVersion clusterversion.ClusterVersion
			if err := protoutil.Unmarshal([]byte(val.Value), &newVersion); err != nil {
				log.Dev.Warningf(ctx, "ignoring invalid cluster version: %s - %v\n"+
					"Note: the lack of a refreshed storage cluster version in a secondary tenant may prevent tenant upgrade.",
					newVersion, err)
			} else {
				// We don't want to fully process the override in the case
				// where we're dealing with the "version" setting, as we want
				// the tenant to have full control over its version setting.
				// Instead, we take the override value and cache it as the
				// storageClusterVersion for use in determining if it's safe to
				// upgrade the tenant (since we don't want to upgrade tenants
				// to a version that's beyond that of the storage cluster).
				log.Dev.Infof(ctx, "updating storage cluster cached version from: %v to: %v", s.mu.storageClusterVersion, newVersion)
				s.mu.storageClusterVersion = newVersion
			}
			continue
		}
		if oldVal, hasExisting := s.mu.overrides[key]; hasExisting && oldVal == val {
			// We already have the same override in place; ignore.
			continue
		}
		// A new override was added or an existing override has changed.
		s.mu.overrides[key] = val
		log.VEventf(ctx, 2, "applying override for %s = %q", key, val.Value)
		s.setLocked(ctx, key, val, settings.OriginExternallySet)
	}

	// Clean up any overrides that were removed.
	for key := range s.mu.overrides {
		if _, ok := newOverrides[key]; !ok {
			delete(s.mu.overrides, key)

			s.applyValueFromSystemSettingsOrDefaultLocked(ctx, key)
		}
	}

	return updateCh
}

// applyValueFromSystemSettingsOrDefaultLocked loads the value stored
// in system.settings into the in-RAM store, or resets the setting to
// default if the entry in system.settings was known to be deleted.
func (s *SettingsWatcher) applyValueFromSystemSettingsOrDefaultLocked(
	ctx context.Context, key settings.InternalKey,
) {
	if sv, ok := s.mu.values[key]; !ok || sv.tombstone {
		// Value deleted from system.settings. Reset to default.
		s.setDefaultLocked(ctx, key)
	} else {
		// Value added/updated in system.settings. Update the in-RAM value.
		s.setLocked(ctx, key, sv.val, settings.OriginExplicitlySet)
	}
}

func (s *SettingsWatcher) resetUpdater() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.updater = s.settings.MakeUpdater()
}

// SetTestingKnobs is used by tests to set testing knobs.
func (s *SettingsWatcher) SetTestingKnobs(knobs *rangefeedcache.TestingKnobs) {
	s.testingWatcherKnobs = knobs
}

// IsOverridden implements cluster.OverridesInformer.
func (s *SettingsWatcher) IsOverridden(settingKey settings.InternalKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.mu.overrides[settingKey]
	return exists
}

// GetStorageClusterActiveVersion returns the storage cluster version cached in
// the SettingsWatcher. The storage cluster version info in the settings watcher
// is populated by a cluster settings override sent from the system tenant to
// all tenants, anytime the storage cluster version changes (or when a new
// cluster is initialized in version 23.1 or later). In cases where the storage
// cluster version is not initialized, we assume that it's running version 22.2,
// the last version which did not properly initialize this value.
func (s *SettingsWatcher) GetStorageClusterActiveVersion() clusterversion.ClusterVersion {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.storageClusterVersion.Equal(clusterversion.ClusterVersion{Version: roachpb.Version{Major: 0, Minor: 0}}) {
		// If the storage cluster version is not initialized in the
		// settingswatcher, it means that the storage cluster has not yet been
		// upgraded to 23.1. As a result, assume that storage cluster is at
		// version 22.2.
		storageClusterVersion := roachpb.Version{Major: 22, Minor: 2, Internal: 0}
		return clusterversion.ClusterVersion{Version: storageClusterVersion}
	}
	return s.mu.storageClusterVersion
}

// errVersionSettingNotFound is returned by GetClusterVersionFromStorage if the
// 'version' setting is not present in the system.settings table.
var errVersionSettingNotFound = errors.New("got nil value for tenant cluster version row")

// GetClusterVersionFromStorage 通过给定的事务从存储（system.settings 表）中直接读取集群版本。
func (s *SettingsWatcher) GetClusterVersionFromStorage(
	ctx context.Context, txn *kv.Txn,
) (clusterversion.ClusterVersion, error) {
	// 构造 settings 表的主键前缀。
	indexPrefix := s.codec.IndexPrefix(keys.SettingsTableID, uint32(1))
	// 编码 key: "version"。
	key := encoding.EncodeUvarintAscending(encoding.EncodeStringAscending(indexPrefix, "version"), uint64(0))
	row, err := txn.Get(ctx, key)
	if err != nil {
		return clusterversion.ClusterVersion{}, err
	}
	if row.Value == nil {
		return clusterversion.ClusterVersion{}, errVersionSettingNotFound
	}
	// 解码行数据。
	_, val, _, err := s.dec.DecodeRow(roachpb.KeyValue{Key: row.Key, Value: *row.Value}, nil /* alloc */)
	if err != nil {
		return clusterversion.ClusterVersion{}, err
	}
	var version clusterversion.ClusterVersion
	// 反序列化版本信息。
	if err := protoutil.Unmarshal([]byte(val.Value), &version); err != nil {
		return clusterversion.ClusterVersion{}, err
	}
	return version, nil
}

func (s *SettingsWatcher) GetTenantClusterVersion() clusterversion.Handle {
	return s.settings.Version
}

// setSystemVisibleDefault is called by the watcher above for any
// changes to system.settings made on a setting with class
// SystemVisible.
func (s *SettingsWatcher) setSystemVisibleDefault(ctx context.Context, key settings.InternalKey) {
	if s.notifySystemVisibleChange == nil {
		return
	}

	found, payload := s.getSettingAndValue(key)
	if !found {
		// We are observing an update for a setting that does not exist
		// (any more). This can happen if there was a customization in the
		// system.settings table from a previous version and the setting
		// was retired.
		return
	}

	log.VEventf(ctx, 1, "propagating read-only default %+v", payload)

	// Inject the current cluster version in the overrides to work
	// around the possibility of incorrect data in the
	// `system.tenant_settings` table. See #125702 for details.
	//
	// TODO(multitenant): remove this logic once the minimum supported
	// version is 24.2+.
	overrides := []kvpb.TenantSetting{
		payload,
		{
			InternalKey: clusterversion.KeyVersionSetting,
			Value: settings.EncodedValue{
				Type:  settings.VersionSettingValueType,
				Value: s.settings.Version.ActiveVersion(ctx).String(),
			},
		},
	}
	s.notifySystemVisibleChange(ctx, overrides)
}

func (s *SettingsWatcher) getSettingAndValue(key settings.InternalKey) (bool, kvpb.TenantSetting) {
	setting, ok := settings.LookupForLocalAccessByKey(key, settings.ForSystemTenant)
	if !ok {
		return false, kvpb.TenantSetting{}
	}
	payload := kvpb.TenantSetting{InternalKey: key, Value: settings.EncodedValue{
		Value: setting.Encoded(&s.settings.SV),
		Type:  setting.Typ(),
	}}
	return true, payload
}

func (s *SettingsWatcher) GetPreserveDowngradeVersionSettingValue() string {
	return clusterversion.PreserveDowngradeVersion.Get(&s.settings.SV)
}

func (s *SettingsWatcher) GetAutoUpgradeEnabledSettingValue() bool {
	return clusterversion.AutoUpgradeEnabled.Get(&s.settings.SV)
}
