// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package slprovider exposes an implementation of the sqlliveness.Provider
// interface.
package slprovider

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// New constructs a new Provider.
//
// sessionEvents, if not nil, gets notified of some session state transitions.
func New(
	ambientCtx log.AmbientContext,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	db *kv.DB,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	settingsWatcher *settingswatcher.SettingsWatcher,
	testingKnobs *sqlliveness.TestingKnobs,
	sessionEvents slinstance.SessionEventListener,
) sqlliveness.Provider {
	storage := slstorage.NewStorage(ambientCtx, stopper, clock, db, codec, settings, settingsWatcher)
	instance := slinstance.NewSQLInstance(ambientCtx, stopper, clock, storage, settings, testingKnobs, sessionEvents)
	return &provider{
		Storage:  storage,
		Instance: instance,
	}
}

type provider struct {
	*slstorage.Storage
	*slinstance.Instance
}

var _ sqlliveness.Provider = &provider{}

func (p *provider) Start(ctx context.Context, regionPhysicalRep []byte) {
	// 启动存储层，主要负责定期清理过期的会话记录。
	p.Storage.Start(ctx)
	// 启动实例层，负责创建和维护当前 SQL 实例的活跃会话（心跳）。
	p.Instance.Start(ctx, regionPhysicalRep)
}

func (p *provider) Metrics() metric.Struct {
	return p.Storage.Metrics()
}
