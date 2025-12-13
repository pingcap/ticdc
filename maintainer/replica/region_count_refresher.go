// Copyright 2025 PingCAP, Inc.
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

package replica

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type regionCountRefresher struct {
	regionCache split.RegionCache
	interval    time.Duration

	traced sync.Map // map[common.DispatcherID]*heartbeatpb.TableSpan
	counts sync.Map // map[common.DispatcherID]int
}

func newRegionCountRefresher(regionCache split.RegionCache, interval time.Duration) *regionCountRefresher {
	return &regionCountRefresher{
		regionCache: regionCache,
		interval:    interval,
	}
}

func (r *regionCountRefresher) addDispatcher(ctx context.Context, id common.DispatcherID, span *heartbeatpb.TableSpan) {
	r.traced.Store(id, span)
	backoff := tikv.NewBackoffer(ctx, 2000)
	regions, err := r.regionCache.LoadRegionsInKeyRange(backoff, span.StartKey, span.EndKey)
	if err != nil {
		log.Warn("load regions failed, just continue",
			zap.Stringer("dispatcherID", id),
			zap.String("span", common.FormatTableSpan(span)),
			zap.Error(err))
	}
	r.counts.Store(id, len(regions))
}

func (r *regionCountRefresher) removeDispatcher(id common.DispatcherID) {
	r.traced.Delete(id)
	r.counts.Delete(id)
}

func (r *regionCountRefresher) getRegionCount(id common.DispatcherID) int {
	value, ok := r.counts.Load(id)
	if !ok {
		return 0
	}
	return value.(int)
}

func (r *regionCountRefresher) refreshRegionCounts(
	ctx context.Context,
) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("region count refresher exited")
			return
		case <-ticker.C:
			r.queryRegionCount(ctx)
		}
	}
}

func (r *regionCountRefresher) queryRegionCount(ctx context.Context) {
	backoff := tikv.NewBackoffer(ctx, 2000)

	var tableCount int
	start := time.Now()
	r.traced.Range(func(key, value any) bool {
		dispatcherID := key.(common.DispatcherID)
		span := value.(*heartbeatpb.TableSpan)
		regions, err := r.regionCache.LoadRegionsInKeyRange(
			backoff,
			span.StartKey,
			span.EndKey,
		)
		if err != nil {
			log.Warn("load regions failed, just continue",
				zap.Stringer("dispatcherID", dispatcherID),
				zap.String("span", common.FormatTableSpan(span)),
				zap.Error(err))
			return true
		}
		r.counts.Store(dispatcherID, len(regions))
		tableCount++
		return true
	})

	if tableCount > 0 {
		log.Info("refresh region count for all tables",
			zap.Int("tableCount", tableCount), zap.Duration("duration", time.Since(start)))
	}
}
