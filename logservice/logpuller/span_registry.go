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
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const kvEventsCacheMaxSize = 32

// subscribedSpan is the local state for one subscribed table span.
type subscribedSpan struct {
	subID   SubscriptionID
	startTs uint64
	// Whether to filter out the value written by TiCDC itself.
	// It should be `true` in BDR mode.
	filterLoop bool

	// The target span.
	span heartbeatpb.TableSpan
	// The range lock of the span,
	// it is used to prevent duplicate requests to the same region range,
	// and it also used to calculate this table's resolvedTs.
	rangeLock *regionlock.RangeLock

	consumeKVEvents func(events []common.RawKVEntry, wakeCallback func()) bool

	advanceResolvedTs func(ts uint64)

	advanceInterval int64

	kvEventsCache []common.RawKVEntry

	// To handle span removing.
	stopped atomic.Bool

	// To handle stale lock resolvings.
	tryResolveLock     func(regionID uint64, state *regionlock.LockedRangeState)
	staleLocksTargetTs atomic.Uint64

	lastAdvanceTime atomic.Int64

	initialized       atomic.Bool
	resolvedTsUpdated atomic.Int64
	resolvedTs        atomic.Uint64
}

type spanRegistry struct {
	sync.RWMutex
	spans map[SubscriptionID]*subscribedSpan

	upstream *upstreamHandle
}

type spanAndTargetTs struct {
	span     *subscribedSpan
	targetTs uint64
}

func newSubscribedSpan(
	ctx context.Context,
	resolveLockRateLimiter *resolveLockRateLimiter,
	resolveLockTaskCh chan resolveLockTask,
	subID SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	filterLoop bool,
) *subscribedSpan {
	rangeLock := regionlock.NewRangeLock(uint64(subID), span.StartKey, span.EndKey, startTs)

	rt := &subscribedSpan{
		subID:      subID,
		span:       span,
		startTs:    startTs,
		filterLoop: filterLoop,
		rangeLock:  rangeLock,

		consumeKVEvents:   consumeKVEvents,
		advanceResolvedTs: advanceResolvedTs,
		advanceInterval:   advanceInterval,
	}
	rt.initialized.Store(false)
	rt.resolvedTsUpdated.Store(time.Now().Unix())
	rt.resolvedTs.Store(startTs)

	rt.tryResolveLock = func(regionID uint64, state *regionlock.LockedRangeState) {
		targetTs := rt.staleLocksTargetTs.Load()
		if !state.Initialized.Load() || state.ResolvedTs.Load() >= targetTs {
			return
		}
		key := resolveLockKey{keyspaceID: span.KeyspaceID, regionID: regionID}
		if !resolveLockRateLimiter.trySchedule(key, time.Now()) {
			return
		}
		select {
		case <-ctx.Done():
			resolveLockRateLimiter.cancel(key)
		case resolveLockTaskCh <- resolveLockTask{
			keyspaceID: span.KeyspaceID,
			regionID:   regionID,
			targetTs:   targetTs,
			state:      state,
		}:
		// it is ok to ignore resolve lock task when the channel is full
		default:
			resolveLockRateLimiter.cancel(key)
			metrics.SubscriptionClientResolveLockTaskDropCounter.Inc()
		}
	}
	return rt
}

func (span *subscribedSpan) clearKVEventsCache() {
	if cap(span.kvEventsCache) > kvEventsCacheMaxSize {
		span.kvEventsCache = nil
	} else {
		span.kvEventsCache = span.kvEventsCache[:0]
	}
}

func (span *subscribedSpan) resolveStaleLocks(targetTs uint64) {
	util.MustCompareAndMonotonicIncrease(&span.staleLocksTargetTs, targetTs)
	res := span.rangeLock.IterAll(span.tryResolveLock)
	log.Debug("subscription client finds slow locked ranges",
		zap.Uint64("subscriptionID", uint64(span.subID)),
		zap.Any("ranges", res))
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
	spanAndTsCache := make([]spanAndTargetTs, 0, maxCacheSize)
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
					spanAndTsCache = append(spanAndTsCache, spanAndTargetTs{
						span:     subSpan,
						targetTs: targetTs,
					})
				}
			}
		}
		r.RUnlock()
		for _, spanAndTs := range spanAndTsCache {
			spanAndTs.span.resolveStaleLocks(spanAndTs.targetTs)
		}
		spanAndTsCache = spanAndTsCache[:0]
		if cap(spanAndTsCache) > maxCacheSize {
			spanAndTsCache = make([]spanAndTargetTs, 0, maxCacheSize)
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
