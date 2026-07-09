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

type regionFailureHandler struct {
	cache  *errCache
	client *subscriptionClient
}

func newRegionFailureHandler(client *subscriptionClient) *regionFailureHandler {
	return &regionFailureHandler{
		cache:  newErrCache(),
		client: client,
	}
}

// Report admits a region failure into the recovery pipeline. It releases the
// corresponding range lock before enqueueing the failure so new range tasks are
// not blocked by stale region ownership.
func (r *regionFailureHandler) Report(errInfo regionErrorInfo) {
	if errInfo.subscribedSpan.rangeLock.UnlockRange(
		errInfo.span.StartKey, errInfo.span.EndKey,
		errInfo.verID.GetID(), errInfo.verID.GetVer(), errInfo.resolvedTs()) {
		r.client.onTableDrained(errInfo.subscribedSpan)
		return
	}
	r.cache.add(errInfo)
}

func (r *regionFailureHandler) Run(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

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

	for {
		select {
		case <-ctx.Done():
			log.Info("subscription client handle errors and exit")
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
			r.client.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader(), errInfo.rpcCtx.AccessIdx)
			r.client.scheduleRegionRequest(ctx, errInfo.regionInfo, TaskHighPrior)
			return nil
		}
		if innerErr.GetEpochNotMatch() != nil {
			metricFeedEpochNotMatchCounter.Inc()
			r.client.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedSpan, errInfo.filterLoop, TaskHighPrior)
			return nil
		}
		if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			r.client.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedSpan, errInfo.filterLoop, TaskHighPrior)
			return nil
		}
		if innerErr.GetCongested() != nil {
			metricKvCongestedCounter.Inc()
			r.client.scheduleRegionRequest(ctx, errInfo.regionInfo, TaskLowPrior)
			return nil
		}
		if innerErr.GetServerIsBusy() != nil {
			metricKvIsBusyCounter.Inc()
			r.client.scheduleRegionRequest(ctx, errInfo.regionInfo, TaskLowPrior)
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
		r.client.scheduleRegionRequest(ctx, errInfo.regionInfo, TaskHighPrior)
		return nil
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		r.client.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedSpan, errInfo.filterLoop, TaskHighPrior)
		return nil
	case *getStoreErr:
		metricGetStoreErr.Inc()
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		// cannot get the store the region belongs to, so we need to reload the region.
		r.client.regionCache.OnSendFail(bo, errInfo.rpcCtx, true, err)
		r.client.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedSpan, errInfo.filterLoop, TaskHighPrior)
		return nil
	case *storeStreamErr:
		metricStoreSendRequestErr.Inc()
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		r.client.regionCache.OnSendFail(bo, errInfo.rpcCtx, regionScheduleReload, err)
		r.client.scheduleRegionRequest(ctx, errInfo.regionInfo, TaskHighPrior)
		return nil
	case *requestCancelledErr:
		// the corresponding subscription has been unsubscribed, just ignore.
		return nil
	default:
		// TODO(qupeng): for some errors it's better to just deregister the region from TiKVs.
		log.Warn("subscription client meets an internal error, fail the changefeed",
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
