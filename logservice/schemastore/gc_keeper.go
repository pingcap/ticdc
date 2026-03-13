// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schemastore

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"go.uber.org/zap"
)

const (
	schemaStoreGCRefreshInterval  = 10 * time.Second
	schemaStoreGCServiceKeeperTag = "-keeper-"
)

type schemaStoreGCKeeper struct {
	pdCli        gc.GCServiceClient
	keyspaceMeta common.KeyspaceMeta
	// gcServiceIDTag separates schema store GC services from user changefeeds.
	gcServiceIDTag string
	// gcServiceIDParts carries the keyspace/name pair used by the existing GC
	// helper API to build a stable internal service ID.
	gcServiceIDParts common.ChangeFeedID
}

func newSchemaStoreGCKeeper(pdCli gc.GCServiceClient, keyspaceMeta common.KeyspaceMeta) *schemaStoreGCKeeper {
	return &schemaStoreGCKeeper{
		pdCli:          pdCli,
		keyspaceMeta:   keyspaceMeta,
		gcServiceIDTag: defaultSchemaStoreGcServiceID + schemaStoreGCServiceKeeperTag,
		gcServiceIDParts: common.NewChangeFeedIDWithName(
			fmt.Sprintf("node_%s_keyspace_%d", sanitizeSchemaStoreNodeID(config.GetGlobalServerConfig().AdvertiseAddr), keyspaceMeta.ID),
			keyspaceMeta.Name,
		),
	}
}

func (k *schemaStoreGCKeeper) initialize(ctx context.Context, gcSafePoint uint64) error {
	return k.refreshWithTs(ctx, gcSafePoint)
}

func (k *schemaStoreGCKeeper) refresh(ctx context.Context, resolvedTs uint64) error {
	return k.refreshWithTs(ctx, resolvedTs)
}

func (k *schemaStoreGCKeeper) refreshWithTs(ctx context.Context, ts uint64) error {
	// EnsureChangefeedStartTsSafety is defined in terms of changefeed startTs: it
	// keeps "startTs - 1" readable, not startTs itself.
	//
	// Schema store needs the snapshot at ts to stay readable, and it pulls
	// incremental DDLs starting from ts. So ts itself must not be
	// collected yet. To express that requirement with the helper's startTs
	// convention, schema store passes ts + 1 here.
	startTs := ts
	if startTs != math.MaxUint64 {
		startTs++
	}
	return gc.EnsureChangefeedStartTsSafety(
		ctx,
		k.pdCli,
		k.gcServiceIDTag,
		k.keyspaceMeta.ID,
		k.gcServiceIDParts,
		defaultGcServiceTTL,
		startTs,
	)
}

func (k *schemaStoreGCKeeper) close(ctx context.Context) error {
	return gc.UndoEnsureChangefeedStartTsSafety(
		ctx,
		k.pdCli,
		k.keyspaceMeta.ID,
		k.gcServiceIDTag,
		k.gcServiceIDParts,
	)
}

func (k *schemaStoreGCKeeper) run(ctx context.Context, resolvedTsGetter func() uint64) {
	ticker := time.NewTicker(schemaStoreGCRefreshInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := k.refresh(ctx, resolvedTsGetter()); err != nil {
					log.Warn("refresh schema store gc safepoint failed",
						zap.Any("keyspace", k.keyspaceMeta),
						zap.String("serviceID", k.serviceID()),
						zap.Error(err))
				}
			}
		}
	}()
}

// serviceID returns the exact PD GC service ID used by this schema store keeper.
func (k *schemaStoreGCKeeper) serviceID() string {
	return k.gcServiceIDTag + k.gcServiceIDParts.Keyspace() + "_" + k.gcServiceIDParts.Name()
}

// sanitizeSchemaStoreNodeID normalizes the node identity before embedding it in
// the GC service ID, so addresses like "127.0.0.1:8300" become a stable
// identifier without characters such as ':' or '/'.
func sanitizeSchemaStoreNodeID(nodeID string) string {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return "unknown"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_':
			return r
		default:
			return '_'
		}
	}, nodeID)
}
