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
	cache       *errCache
	regionCache *tikv.RegionCache
	retryMu     sync.Mutex
	retries     map[regionRetryKey]*regionRetryState
	retryDelay  func(uint32) time.Duration

	onTableDrained        func(*subscribedSpan)
	scheduleRegionRequest func(context.Context, regionInfo)
	scheduleRangeRequest  func(context.Context, rangeTask)
}

const (
	regionRetryBaseDelay = 50 * time.Millisecond
	regionRetryMaxDelay  = 2 * time.Second
	regionRetryStateTTL  = 5 * time.Minute
)

type regionRetryKey struct {
	subscriptionID SubscriptionID
	regionID       uint64
}

type regionRetryState struct {
	attempt    uint32
	generation uint64
	pending    bool
	timer      *time.Timer
}

func notLeaderRetryDelay(attempt uint32) time.Duration {
	if attempt == 0 {
		attempt = 1
	}
	exponent := attempt - 1
	if exponent > 16 {
		exponent = 16
	}
	delay := regionRetryBaseDelay << exponent
	if delay > regionRetryMaxDelay {
		delay = regionRetryMaxDelay
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
		retries:               make(map[regionRetryKey]*regionRetryState),
		retryDelay:            notLeaderRetryDelay,
		onTableDrained:        onTableDrained,
		scheduleRegionRequest: scheduleRegionRequest,
		scheduleRangeRequest:  scheduleRangeRequest,
	}
}

func (r *regionFailureHandler) scheduleRegionRetry(
	ctx context.Context,
	region regionInfo,
	retry func(),
) {
	if region.subscribedSpan == nil || region.subscribedSpan.stopped.Load() {
		return
	}
	key := regionRetryKey{
		subscriptionID: region.subscribedSpan.subID,
		regionID:       region.verID.GetID(),
	}

	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	state := r.retries[key]
	if state == nil {
		state = &regionRetryState{}
		r.retries[key] = state
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
	delay := r.retryDelay(state.attempt)
	state.timer = time.AfterFunc(delay, func() {
		r.retryMu.Lock()
		current := r.retries[key]
		if current == nil || current.generation != generation || !current.pending {
			r.retryMu.Unlock()
			return
		}
		current.pending = false
		current.timer = time.AfterFunc(regionRetryStateTTL, func() {
			r.expireRegionRetry(key, generation)
		})
		r.retryMu.Unlock()

		if ctx.Err() != nil || region.subscribedSpan.stopped.Load() {
			r.resetRegionRetry(key.subscriptionID, key.regionID)
			return
		}
		retry()
	})
}

func (r *regionFailureHandler) expireRegionRetry(key regionRetryKey, generation uint64) {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	state := r.retries[key]
	if state != nil && state.generation == generation && !state.pending {
		delete(r.retries, key)
	}
}

func (r *regionFailureHandler) resetRegionRetry(subscriptionID SubscriptionID, regionID uint64) {
	key := regionRetryKey{subscriptionID: subscriptionID, regionID: regionID}
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	if state := r.retries[key]; state != nil {
		if state.timer != nil {
			state.timer.Stop()
		}
		delete(r.retries, key)
	}
}

func (r *regionFailureHandler) cancelRegionRetries() {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	for key, state := range r.retries {
		if state.timer != nil {
			state.timer.Stop()
		}
		delete(r.retries, key)
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
	defer r.cancelRegionRetries()

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
	rescheduleRange := func() {
		priority := normalizeScanPriority(errInfo.scanPriority)
		if priority == cdcpb.ScanPriority_SCAN_PRIORITY_LOW {
			priority = errInfo.subscribedSpan.priorityPolicy.resolve(
				priority,
				errInfo.resolvedTs(),
				errInfo.subscribedSpan.priorityPolicy.pdClock.CurrentTime(),
			)
		}
		r.scheduleRangeRequest(ctx, rangeTask{
			span:           errInfo.span,
			subscribedSpan: errInfo.subscribedSpan,
			filterLoop:     errInfo.filterLoop,
			priority:       priority,
		})
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
				r.scheduleRegionRetry(ctx, errInfo.regionInfo, rescheduleRange)
				return nil
			}
			r.regionCache.UpdateLeader(errInfo.verID, leader, errInfo.rpcCtx.AccessIdx)
			r.scheduleRegionRetry(ctx, errInfo.regionInfo, func() {
				r.scheduleRegionRequest(ctx, errInfo.regionInfo)
			})
			return nil
		}
		if innerErr.GetEpochNotMatch() != nil {
			metricFeedEpochNotMatchCounter.Inc()
			rescheduleRange()
			return nil
		}
		if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			rescheduleRange()
			return nil
		}
		if innerErr.GetCongested() != nil {
			metricKvCongestedCounter.Inc()
			r.scheduleRegionRequest(ctx, errInfo.regionInfo)
			return nil
		}
		if innerErr.GetServerIsBusy() != nil {
			metricKvIsBusyCounter.Inc()
			r.scheduleRegionRequest(ctx, errInfo.regionInfo)
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
		r.scheduleRegionRequest(ctx, errInfo.regionInfo)
		return nil
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		rescheduleRange()
		return nil
	case *getStoreErr:
		metricGetStoreErr.Inc()
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		// cannot get the store the region belongs to, so we need to reload the region.
		r.regionCache.OnSendFail(bo, errInfo.rpcCtx, true, err)
		rescheduleRange()
		return nil
	case *storeStreamErr:
		metricStoreSendRequestErr.Inc()
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		r.regionCache.OnSendFail(bo, errInfo.rpcCtx, regionScheduleReload, err)
		r.scheduleRegionRequest(ctx, errInfo.regionInfo)
		return nil
	case *requestCancelledErr:
		// the corresponding subscription has been unsubscribed, just ignore.
		if errInfo.subscribedSpan != nil {
			r.resetRegionRetry(errInfo.subscribedSpan.subID, errInfo.verID.GetID())
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
