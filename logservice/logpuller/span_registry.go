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

package logpuller

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type spanRegistry struct {
	sync.RWMutex
	spans map[SubscriptionID]*subscribedSpan

	upstream *upstreamHandle
}

func newSpanRegistry(upstream *upstreamHandle) *spanRegistry {
	return &spanRegistry{
		spans:    make(map[SubscriptionID]*subscribedSpan),
		upstream: upstream,
	}
}

func (r *spanRegistry) Add(span *subscribedSpan) {
	r.Lock()
	defer r.Unlock()
	r.spans[span.subID] = span
}

func (r *spanRegistry) Get(subID SubscriptionID) *subscribedSpan {
	r.RLock()
	defer r.RUnlock()
	return r.spans[subID]
}

func (r *spanRegistry) Remove(subID SubscriptionID) {
	r.Lock()
	defer r.Unlock()
	delete(r.spans, subID)
}

func (r *spanRegistry) UpdateMetrics() {
	count := 0
	pullerMinResolvedTs := uint64(0)
	r.RLock()
	for _, span := range r.spans {
		count += span.rangeLock.Len()
		resolvedTs := span.resolvedTs.Load()
		if pullerMinResolvedTs == 0 || resolvedTs < pullerMinResolvedTs {
			pullerMinResolvedTs = resolvedTs
		}
	}
	r.RUnlock()
	metrics.SubscriptionClientSubscribedRegionCount.Set(float64(count))

	if pullerMinResolvedTs == 0 {
		return
	}
	pdTime := r.upstream.pdClock.CurrentTime()
	phyResolvedTs := oracle.ExtractPhysical(pullerMinResolvedTs)
	resolvedTsLag := float64(oracle.GetPhysical(pdTime)-phyResolvedTs) / 1e3
	if resolvedTsLag > 0 {
		metrics.LogPullerResolvedTsLag.Set(resolvedTsLag)
	}
}

func (r *spanRegistry) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return r.runResolveLockChecker(ctx) })
	g.Go(func() error { return r.logSlowRegions(ctx) })
	return g.Wait()
}

func (r *spanRegistry) runResolveLockChecker(ctx context.Context) error {
	resolveLockTicker := time.NewTicker(resolveLockTickInterval)
	defer resolveLockTicker.Stop()
	maxCacheSize := 1024
	subSpanAndTsCache := make([]subscriptionAndTargetTs, 0, maxCacheSize)
	// getResolvedTargetTs returns the targetTs to resolve stale locks. 0 means no need to resolve.
	getResolvedTargetTs := func(subSpan *subscribedSpan, currentTime time.Time, currentTs uint64) uint64 {
		resolvedTsUpdated := time.Unix(subSpan.resolvedTsUpdated.Load(), 0)
		if !subSpan.initialized.Load() || time.Since(resolvedTsUpdated) < resolveLockFence {
			return 0
		}
		resolvedTs := subSpan.resolvedTs.Load()
		resolvedTime := oracle.GetTimeFromTS(resolvedTs)
		if currentTime.Sub(resolvedTime) < resolveLockFence {
			return 0
		}
		return min(currentTs, oracle.GoTimeToTS(resolvedTime.Add(resolveLockFence)))
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resolveLockTicker.C:
		}

		physical, logic, err := r.upstream.pd.GetTS(ctx)
		if err != nil {
			log.Warn("get ts from pd failed", zap.Error(err))
			continue
		}
		currentTs := oracle.ComposeTS(physical, logic)
		currentTime := r.upstream.pdClock.CurrentTime()
		r.RLock()
		for _, subSpan := range r.spans {
			if subSpan != nil {
				targetTs := getResolvedTargetTs(subSpan, currentTime, currentTs)
				if targetTs > 0 {
					subSpanAndTsCache = append(subSpanAndTsCache, subscriptionAndTargetTs{
						subSpan:  subSpan,
						targetTs: targetTs,
					})
				}
			}
		}
		r.RUnlock()
		for _, subSpanAndTs := range subSpanAndTsCache {
			subSpanAndTs.subSpan.resolveStaleLocks(subSpanAndTs.targetTs)
		}
		subSpanAndTsCache = subSpanAndTsCache[:0]
		if cap(subSpanAndTsCache) > maxCacheSize {
			subSpanAndTsCache = make([]subscriptionAndTargetTs, 0, maxCacheSize)
		}
	}
}

func (r *spanRegistry) logSlowRegions(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		currTime := r.upstream.pdClock.CurrentTime()
		r.RLock()
		for subscriptionID, rt := range r.spans {
			attr := rt.rangeLock.IterAll(nil)
			ckptTime := oracle.GetTimeFromTS(attr.SlowestRegion.ResolvedTs)
			if attr.SlowestRegion.Initialized {
				if currTime.Sub(ckptTime) > 6*resolveLockMinInterval {
					log.Info("subscription client finds a initialized slow region",
						zap.Uint64("subscriptionID", uint64(subscriptionID)),
						zap.Any("slowRegion", attr.SlowestRegion))
				}
			} else if currTime.Sub(attr.SlowestRegion.Created) > 10*time.Minute {
				log.Info("subscription client initializes a region too slow",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			} else if currTime.Sub(ckptTime) > 10*time.Minute {
				log.Info("subscription client finds a uninitialized slow region",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("slowRegion", attr.SlowestRegion))
			}
			if len(attr.UnLockedRanges) > 0 {
				log.Info("subscription client holes exist",
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Any("holes", attr.UnLockedRanges))
			}
		}
		r.RUnlock()
	}
}
