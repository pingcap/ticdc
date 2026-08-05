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
	"math/rand/v2"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var (
	metricFeedNotLeaderCounter        = metrics.EventFeedErrorCounter.WithLabelValues("NotLeader")
	metricFeedEpochNotMatchCounter    = metrics.EventFeedErrorCounter.WithLabelValues("EpochNotMatch")
	metricFeedRegionNotFoundCounter   = metrics.EventFeedErrorCounter.WithLabelValues("RegionNotFound")
	metricFeedDuplicateRequestCounter = metrics.EventFeedErrorCounter.WithLabelValues("DuplicateRequest")
	metricFeedUnknownErrorCounter     = metrics.EventFeedErrorCounter.WithLabelValues("Unknown")
	metricFeedRPCCtxUnavailable       = metrics.EventFeedErrorCounter.WithLabelValues("RPCCtxUnavailable")
	metricGetStoreErr                 = metrics.EventFeedErrorCounter.WithLabelValues("GetStoreErr")
	metricStoreSendRequestErr         = metrics.EventFeedErrorCounter.WithLabelValues("SendRequestToStore")
	metricKvIsBusyCounter             = metrics.EventFeedErrorCounter.WithLabelValues("KvIsBusy")
	metricKvCongestedCounter          = metrics.EventFeedErrorCounter.WithLabelValues("KvCongested")
)

// regionFailureHandler handles failed regions and owns retry and reschedule decisions.
type regionFailureHandler struct {
	cache         *errCache
	regionCache   *tikv.RegionCache
	recoveryMu    sync.Mutex
	recoveries    map[regionRecoveryKey]*regionRecoveryState
	recoveryDelay func(uint32) time.Duration

	onTableDrained        func(*subscribedSpan)
	scheduleRegionRequest func(context.Context, regionInfo)
	scheduleRangeRequest  func(context.Context, rangeTask)
}

const (
	regionRecoveryBaseDelay = 50 * time.Millisecond
	regionRecoveryMaxDelay  = 2 * time.Second
	regionRecoveryStateTTL  = 5 * time.Minute
)

// regionRecoveryKey keeps backoff state across region ID and epoch changes for
// the same logical range.
type regionRecoveryKey struct {
	subscriptionID SubscriptionID
	startKey       string
	endKey         string
}

type regionRecoveryState struct {
	attempt    uint32
	generation uint64
	pending    bool
	timer      *time.Timer
}

func newRegionRecoveryKey(
	subscriptionID SubscriptionID,
	span heartbeatpb.TableSpan,
) regionRecoveryKey {
	return regionRecoveryKey{
		subscriptionID: subscriptionID,
		startKey:       string(span.StartKey),
		endKey:         string(span.EndKey),
	}
}

func regionRecoveryDelay(attempt uint32) time.Duration {
	if attempt == 0 {
		attempt = 1
	}
	exponent := attempt - 1
	if exponent > 16 {
		exponent = 16
	}
	delay := regionRecoveryBaseDelay << exponent
	if delay > regionRecoveryMaxDelay {
		delay = regionRecoveryMaxDelay
	}
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(delay-half)+1))
}

func newRegionFailureHandler(
	regionCache *tikv.RegionCache,
	onTableDrained func(*subscribedSpan),
	scheduleRegionRequest func(context.Context, regionInfo),
	scheduleRangeRequest func(context.Context, rangeTask),
) *regionFailureHandler {
	return &regionFailureHandler{
		cache:                 newErrCache(),
		regionCache:           regionCache,
		recoveries:            make(map[regionRecoveryKey]*regionRecoveryState),
		recoveryDelay:         regionRecoveryDelay,
		onTableDrained:        onTableDrained,
		scheduleRegionRequest: scheduleRegionRequest,
		scheduleRangeRequest:  scheduleRangeRequest,
	}
}

func (r *regionFailureHandler) scheduleRecovery(
	ctx context.Context,
	subscribedSpan *subscribedSpan,
	span heartbeatpb.TableSpan,
	minDelay time.Duration,
	retry func(),
) {
	if subscribedSpan == nil || subscribedSpan.stopped.Load() {
		return
	}
	key := newRegionRecoveryKey(subscribedSpan.subID, span)

	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	state := r.recoveries[key]
	if state == nil {
		state = &regionRecoveryState{}
		r.recoveries[key] = state
	}
	if state.pending {
		return
	}
	if state.timer != nil {
		state.timer.Stop()
	}
	if state.attempt < 32 {
		state.attempt++
	}
	state.generation++
	generation := state.generation
	state.pending = true
	delay := r.recoveryDelay(state.attempt)
	if minDelay > delay {
		delay = minDelay
	}
	state.timer = time.AfterFunc(delay, func() {
		r.recoveryMu.Lock()
		current := r.recoveries[key]
		if current == nil || current.generation != generation || !current.pending {
			r.recoveryMu.Unlock()
			return
		}
		// Keep the state after dispatch so the next failure of this range advances
		// the backoff attempt. Successful initialization resets it.
		current.pending = false
		current.timer = time.AfterFunc(regionRecoveryStateTTL, func() {
			r.expireRecovery(key, generation)
		})
		r.recoveryMu.Unlock()

		if ctx.Err() != nil || subscribedSpan.stopped.Load() {
			r.resetRecovery(key)
			return
		}
		retry()
	})
}

func (r *regionFailureHandler) expireRecovery(key regionRecoveryKey, generation uint64) {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	state := r.recoveries[key]
	if state != nil && state.generation == generation && !state.pending {
		delete(r.recoveries, key)
	}
}

func (r *regionFailureHandler) resetRecovery(key regionRecoveryKey) {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	if state := r.recoveries[key]; state != nil {
		if state.timer != nil {
			state.timer.Stop()
		}
		delete(r.recoveries, key)
	}
}

func (r *regionFailureHandler) resetRegionRecovery(region regionInfo) {
	r.resetRecovery(newRegionRecoveryKey(region.subscribedSpan.subID, region.span))
}

func (r *regionFailureHandler) cancelRecoveries() {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	for key, state := range r.recoveries {
		if state.timer != nil {
			state.timer.Stop()
		}
		delete(r.recoveries, key)
	}
}

// Report admits a region failure into the recovery pipeline. It releases the
// corresponding range lock before enqueueing the failure so new range tasks are
// not blocked by stale region ownership.
func (r *regionFailureHandler) Report(errInfo regionErrorInfo) {
	if errInfo.subscribedSpan.rangeLock.UnlockRange(
		errInfo.span.StartKey, errInfo.span.EndKey,
		errInfo.verID.GetID(), errInfo.verID.GetVer(), errInfo.resolvedTs()) {
		r.onTableDrained(errInfo.subscribedSpan)
		return
	}
	r.cache.add(errInfo)
}

func (r *regionFailureHandler) Run(ctx context.Context) error {
	log.Info("region failure handler starts")
	defer log.Info("region failure handler exits")
	defer r.cancelRecoveries()

	handleCachedErrors := func() error {
		for {
			batch := r.cache.popBatch(errCacheBatchSize)
			for _, errInfo := range batch {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if err := r.handleError(ctx, errInfo); err != nil {
					return err
				}
			}
			if len(batch) < errCacheBatchSize {
				return nil
			}
		}
	}

	// r.cache.ready() should handle failures promptly in normal flow. The ticker is only a
	// fallback scan and is not expected to be needed in practice.
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := handleCachedErrors(); err != nil {
				return err
			}
		case <-r.cache.ready():
			if err := handleCachedErrors(); err != nil {
				return err
			}
		}
	}
}

func (r *regionFailureHandler) handleError(ctx context.Context, errInfo regionErrorInfo) error {
	err := errors.Cause(errInfo.err)
	retryRegion := func(minDelay time.Duration) {
		r.scheduleRecovery(
			ctx,
			errInfo.subscribedSpan,
			errInfo.span,
			minDelay,
			func() {
				r.scheduleRegionRequest(ctx, errInfo.regionInfo)
			},
		)
	}
	retryRange := func() {
		priority := normalizeScanPriority(errInfo.scanPriority)
		if priority == cdcpb.ScanPriority_SCAN_PRIORITY_LOW {
			priority = errInfo.subscribedSpan.priorityPolicy.resolve(
				priority,
				errInfo.resolvedTs(),
				errInfo.subscribedSpan.priorityPolicy.pdClock.CurrentTime(),
			)
		}
		task := rangeTask{
			span:           errInfo.span,
			subscribedSpan: errInfo.subscribedSpan,
			filterLoop:     errInfo.filterLoop,
			priority:       priority,
		}
		r.scheduleRecovery(
			ctx,
			task.subscribedSpan,
			task.span,
			0,
			func() {
				r.scheduleRangeRequest(ctx, task)
			},
		)
	}

	//nolint:errorlint // converting large type switch to errors.As is a significant refactor
	if _, requestCancelled := err.(*requestCancelledErr); !requestCancelled {
		log.Debug("cdc region error",
			zap.Uint64("subscriptionID", uint64(errInfo.subscribedSpan.subID)),
			zap.Uint64("regionID", errInfo.verID.GetID()),
			zap.Error(err))
	}

	//nolint:errorlint // converting large type switch to errors.As is a significant refactor
	switch eerr := err.(type) {
	case *eventError:
		innerErr := eerr.err
		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			metricFeedNotLeaderCounter.Inc()
			leader := notLeader.GetLeader()
			if leader == nil || leader.GetId() == 0 || leader.GetStoreId() == 0 || errInfo.rpcCtx == nil {
				r.regionCache.InvalidateCachedRegion(errInfo.verID)
				retryRange()
				return nil
			}
			r.regionCache.UpdateLeader(errInfo.verID, leader, errInfo.rpcCtx.AccessIdx)
			retryRegion(0)
			return nil
		}
		if innerErr.GetEpochNotMatch() != nil {
			metricFeedEpochNotMatchCounter.Inc()
			retryRange()
			return nil
		}
		if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			retryRange()
			return nil
		}
		if innerErr.GetCongested() != nil {
			metricKvCongestedCounter.Inc()
			retryRegion(0)
			return nil
		}
		if busy := innerErr.GetServerIsBusy(); busy != nil {
			metricKvIsBusyCounter.Inc()
			retryRegion(time.Duration(busy.GetBackoffMs()) * time.Millisecond)
			return nil
		}
		if duplicated := innerErr.GetDuplicateRequest(); duplicated != nil {
			// TODO(qupeng): It's better to add a new machanism to deregister one region.
			metricFeedDuplicateRequestCounter.Inc()
			return errors.New("duplicate request")
		}
		if compatibility := innerErr.GetCompatibility(); compatibility != nil {
			return errors.ErrVersionIncompatible.GenWithStackByArgs(compatibility)
		}
		if mismatch := innerErr.GetClusterIdMismatch(); mismatch != nil {
			return errors.ErrClusterIDMismatch.GenWithStackByArgs(mismatch.Current, mismatch.Request)
		}

		log.Warn("empty or unknown cdc error",
			zap.Uint64("subscriptionID", uint64(errInfo.subscribedSpan.subID)),
			zap.Stringer("error", innerErr))
		metricFeedUnknownErrorCounter.Inc()
		retryRegion(0)
		return nil
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		retryRange()
		return nil
	case *getStoreErr:
		metricGetStoreErr.Inc()
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		// cannot get the store the region belongs to, so we need to reload the region.
		r.regionCache.OnSendFail(bo, errInfo.rpcCtx, true, err)
		retryRange()
		return nil
	case *storeStreamErr:
		metricStoreSendRequestErr.Inc()
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		r.regionCache.OnSendFail(bo, errInfo.rpcCtx, regionScheduleReload, err)
		retryRegion(0)
		return nil
	case *requestCancelledErr:
		// the corresponding subscription has been unsubscribed, just ignore.
		if errInfo.subscribedSpan != nil {
			r.resetRegionRecovery(errInfo.regionInfo)
		}
		return nil
	default:
		// TODO(qupeng): for some errors it's better to just deregister the region from TiKVs.
		log.Warn("region failure cannot be recovered, fail the changefeed",
			zap.Uint64("subscriptionID", uint64(errInfo.subscribedSpan.subID)),
			zap.Error(err))
		return err
	}
}

type errCache struct {
	sync.Mutex
	cache  []regionErrorInfo
	notify chan struct{}
}

const errCacheBatchSize = 1024

func newErrCache() *errCache {
	return &errCache{
		cache:  make([]regionErrorInfo, 0, 1024),
		notify: make(chan struct{}, 1),
	}
}

func (e *errCache) add(errInfo regionErrorInfo) {
	e.Lock()
	defer e.Unlock()
	e.cache = append(e.cache, errInfo)
	select {
	case e.notify <- struct{}{}:
	default:
	}
}

func (e *errCache) popBatch(limit int) []regionErrorInfo {
	e.Lock()
	defer e.Unlock()
	if len(e.cache) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(e.cache) {
		limit = len(e.cache)
	}
	batch := make([]regionErrorInfo, limit)
	copy(batch, e.cache[:limit])
	clear(e.cache[:limit])
	if limit == len(e.cache) {
		e.cache = e.cache[:0]
	} else {
		e.cache = e.cache[limit:]
	}
	return batch
}

func (e *errCache) ready() <-chan struct{} {
	return e.notify
}
