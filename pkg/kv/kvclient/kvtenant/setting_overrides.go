// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtenant

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
)

// runTenantSettingsSubscription 持续监听租户设置覆盖（Tenant Setting Overrides）的变化。
// 一旦成功获取到初始的一组覆盖项，它就会关闭 startupCh 通道。
func (c *connector) runTenantSettingsSubscription(ctx context.Context, startupCh chan<- error) {
	for ctx.Err() == nil {
		// 获取 RPC 客户端。
		client, err := c.getClient(ctx)
		if err != nil {
			continue
		}
		// 发起 TenantSettings 订阅请求，这是一个长链接流（Stream）。
		stream, err := client.TenantSettings(ctx, &kvpb.TenantSettingsRequest{
			TenantID: c.tenantID,
		})
		if err != nil {
			log.Dev.Warningf(ctx, "error issuing TenantSettings RPC: %v", err)
			c.tryForgetClient(ctx, client)
			continue
		}

		// 每次重连时，重置哨兵检查标志。
		func() {
			c.settingsMu.Lock()
			defer c.settingsMu.Unlock()
			c.settingsMu.receivedFirstAllTenantOverrides = false
			c.settingsMu.receivedFirstSpecificOverrides = false
		}()
		func() {
			c.metadataMu.Lock()
			defer c.metadataMu.Unlock()
			c.metadataMu.receivedFirstMetadata = false
		}()

		for {
			e, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break // 正常结束。
				}
				// 软 RPC 错误：重试。
				log.Dev.Warningf(ctx, "error consuming TenantSettings RPC: %v", err)
				c.tryForgetClient(ctx, client)
				break
			}
			if e.Error != (errorspb.EncodedError{}) {
				// 逻辑错误或硬错误。
				err := errors.DecodeError(ctx, e.Error)
				log.Dev.Errorf(ctx, "error consuming TenantSettings RPC: %v", err)
				// 如果配置了“缺少记录时尽早关闭”且确实缺少记录。
				if startupCh != nil && errors.Is(err, &kvpb.MissingRecordError{}) && c.earlyShutdownIfMissingTenantRecord {
					select {
					case startupCh <- err:
					case <-ctx.Done():
						return
					}

					close(startupCh)
					c.tryForgetClient(ctx, client)
					return
				}
				// 等待一秒后重试，避免重试风暴。
				select {
				case <-time.After(1 * time.Second):
				case <-ctx.Done():
					return
				}
				continue
			}

			if c.testingEmulateOldVersionSettingsClient {
				e.EventType = kvpb.TenantSettingsEvent_SETTING_EVENT
			}

			var reconnect bool
			switch e.EventType {
			case kvpb.TenantSettingsEvent_METADATA_EVENT:
				// 处理元数据事件（租户名称、数据状态、服务模式等）。
				err := c.processMetadataEvent(ctx, e)
				if err != nil {
					log.Dev.Errorf(ctx, "error processing tenant settings event: %v", err)
					reconnect = true
				}

			case kvpb.TenantSettingsEvent_SETTING_EVENT:
				// 处理设置覆盖事件。
				settingsReady, err := c.processSettingsEvent(ctx, e)
				if err != nil {
					log.Dev.Errorf(ctx, "error processing tenant settings event: %v", err)
					reconnect = true
					break
				}

				// 一旦“设置已就绪”（即收到初始的非增量设置覆盖），信号启动完成。
				if settingsReady {
					log.Dev.Infof(ctx, "received initial tenant settings")

					if startupCh != nil {
						select {
						case startupCh <- nil:
						case <-ctx.Done():
							return
						}
						close(startupCh)
						startupCh = nil
					}
				}
			}

			if reconnect {
				_ = stream.CloseSend()
				c.tryForgetClient(ctx, client)
				break
			}
		}
	}
}

// processMetadataEvent 根据收到的事件更新租户元数据。
func (c *connector) processMetadataEvent(ctx context.Context, e *kvpb.TenantSettingsEvent) error {
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()

	c.metadataMu.receivedFirstMetadata = true
	c.metadataMu.capabilities = e.Capabilities // 租户能力（Capabilities）
	c.metadataMu.tenantName = e.Name           // 租户显示名称
	// 将协议层定义的 int 状态转换为 mtinfopb 定义的状态。
	c.metadataMu.dataState = mtinfopb.TenantDataState(e.DataState)
	c.metadataMu.serviceMode = mtinfopb.TenantServiceMode(e.ServiceMode)
	c.metadataMu.clusterInitGracePeriodTS = e.ClusterInitGracePeriodEndTS

	log.Dev.Infof(ctx, "received tenant metadata: name=%q dataState=%v serviceMode=%v clusterInitGracePeriodTS=%s\ncapabilities=%+v",
		c.metadataMu.tenantName, c.metadataMu.dataState, c.metadataMu.serviceMode,
		timeutil.Unix(c.metadataMu.clusterInitGracePeriodTS, 0), c.metadataMu.capabilities)

	// 信号通知所有正在观察元数据变化的组件。
	close(c.metadataMu.notifyCh)
	c.metadataMu.notifyCh = make(chan struct{})

	return nil
}

// TenantInfo accesses the tenant metadata.
func (c *connector) TenantInfo() (tenantcapabilities.Entry, <-chan struct{}) {
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()

	return tenantcapabilities.Entry{
		TenantID:           c.tenantID,
		TenantCapabilities: c.metadataMu.capabilities,
		Name:               c.metadataMu.tenantName,
		DataState:          c.metadataMu.dataState,
		ServiceMode:        c.metadataMu.serviceMode,
	}, c.metadataMu.notifyCh
}

// ReadFromTenantInfo allows retrieving the other tenant, if any, from which the
// calling tenant should configure itself to read, along with the latest
// timestamp at which it should perform such reads at this time.
func (c *connector) ReadFromTenantInfo(
	ctx context.Context,
) (roachpb.TenantID, hlc.Timestamp, error) {
	if c.tenantID.IsSystem() {
		return roachpb.TenantID{}, hlc.Timestamp{}, nil
	}

	client, err := c.getClient(ctx)
	if err != nil {
		return roachpb.TenantID{}, hlc.Timestamp{}, err
	}
	resp, err := client.ReadFromTenantInfo(ctx, &serverpb.ReadFromTenantInfoRequest{TenantID: c.tenantID})
	if err != nil {
		return roachpb.TenantID{}, hlc.Timestamp{}, err
	}
	return resp.ReadFrom, resp.ReadAt, nil
}

// processSettingsEvent 根据收到的事件更新设置覆盖（Setting Overrides）。
func (c *connector) processSettingsEvent(
	ctx context.Context, e *kvpb.TenantSettingsEvent,
) (settingsReady bool, err error) {
	c.settingsMu.Lock()
	defer c.settingsMu.Unlock()

	var m map[settings.InternalKey]settings.EncodedValue
	// 区分“全租户覆盖”和“特定租户覆盖”。
	switch e.Precedence {
	case kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES:
		if !c.settingsMu.receivedFirstAllTenantOverrides && e.Incremental {
			return false, errors.Newf(
				"need to receive non-incremental setting event first for precedence %v",
				e.Precedence,
			)
		}

		c.settingsMu.receivedFirstAllTenantOverrides = true
		m = c.settingsMu.allTenantOverrides

	case kvpb.TenantSettingsEvent_TENANT_SPECIFIC_OVERRIDES:
		if !c.settingsMu.receivedFirstSpecificOverrides && e.Incremental {
			return false, errors.Newf(
				"need to receive non-incremental setting events first for precedence %v",
				e.Precedence,
			)
		}
		c.settingsMu.receivedFirstSpecificOverrides = true
		m = c.settingsMu.specificOverrides

	default:
		return false, errors.Newf("unknown precedence value %d", e.Precedence)
	}

	log.Dev.Infof(ctx, "received %d setting overrides with precedence %v (incremental=%v)", len(e.Overrides), e.Precedence, e.Incremental)

	// 如果事件不是增量的（Incremental），则清空当前映射，全量替换。
	if !e.Incremental {
		for k := range m {
			delete(m, k)
		}
	}
	// 合并覆盖项变更。
	for _, o := range e.Overrides {
		if o.Value == (settings.EncodedValue{}) {
			// 空值表示移除该覆盖。
			log.VEventf(ctx, 1, "removing %v override for %q", e.Precedence, o.InternalKey)
			delete(m, o.InternalKey)
		} else {
			// 添加或更新覆盖。
			log.VEventf(ctx, 1, "adding %v override for %q = %q", e.Precedence, o.InternalKey, o.Value.Value)
			m[o.InternalKey] = o.Value
		}
	}

	// 信号通知观察者。
	close(c.settingsMu.notifyCh)
	// 为后续观察者定义一个新的通知通道。
	c.settingsMu.notifyCh = make(chan struct{})

	// 协议定义：服务器必须为两种优先级都发送一个初始的非增量消息。
	// 当两者都收到后，认为设置已就绪。
	settingsReady = c.settingsMu.receivedFirstAllTenantOverrides && c.settingsMu.receivedFirstSpecificOverrides
	return settingsReady, nil
}

// Overrides is part of the settingswatcher.OverridesMonitor interface.
func (c *connector) Overrides() (map[settings.InternalKey]settings.EncodedValue, <-chan struct{}) {
	c.settingsMu.Lock()
	defer c.settingsMu.Unlock()

	res := make(map[settings.InternalKey]settings.EncodedValue, len(c.settingsMu.allTenantOverrides)+len(c.settingsMu.specificOverrides))

	// First copy the all-tenant overrides.
	for key, val := range c.settingsMu.allTenantOverrides {
		res[key] = val
	}
	// Then copy the specific overrides (which can overwrite some all-tenant
	// overrides).
	for key, val := range c.settingsMu.specificOverrides {
		res[key] = val
	}
	return res, c.settingsMu.notifyCh
}

func (c *connector) GetClusterInitGracePeriodTS() int64 {
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()
	return c.metadataMu.clusterInitGracePeriodTS
}
