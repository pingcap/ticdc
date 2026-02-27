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

package logpuller

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	checkStaleRequestInterval    = time.Second * 10
	requestGCLifeTime            = time.Minute * 180
	addReqRetryInterval          = time.Millisecond * 1
	addReqRetryLimit             = 3
	abnormalRequestDurationInSec = 60 * 60 * 2 // 2 hours
)

// regionReq represents a wrapped region request with state
type regionReq struct {
	regionInfo regionInfo
	createTime time.Time
}

func newRegionReq(region regionInfo) regionReq {
	return regionReq{
		regionInfo: region,
		createTime: time.Now(),
	}
}

func (r *regionReq) isStale() bool {
	return time.Since(r.createTime) > requestGCLifeTime
}

// requestCache manages region requests with flow control
type requestCache struct {
	// pending requests waiting to be sent
	pendingQueue chan regionReq

	// sent requests waiting for initialization (subscriptionID -> regions -> regionReq)
	sentRequests struct {
		sync.RWMutex
		regionReqs map[SubscriptionID]map[uint64]regionReq
	}

	// pendingCount is a flow control slot counter.
	// A slot is acquired when a request is successfully enqueued into pendingQueue (see add),
	// and is released when the request is finished/removed (resolve/markStopped/markDone/clear).
	// pop and markSent don't change it. If markSent overwrites an existing request for the same region,
	// it will release a slot for the replaced request to avoid leaking pendingCount.
	pendingCount atomic.Int64
	// maximum number of pending requests allowed
	maxPendingCount int64

	// channel to signal when space becomes available
	spaceAvailable chan struct{}

	lastCheckStaleRequestTime atomic.Time
}

func newRequestCache(maxPendingCount int) *requestCache {
	res := &requestCache{
		pendingQueue: make(chan regionReq, maxPendingCount), // Large buffer to reduce blocking
		sentRequests: struct {
			sync.RWMutex
			regionReqs map[SubscriptionID]map[uint64]regionReq
		}{regionReqs: make(map[SubscriptionID]map[uint64]regionReq)},
		pendingCount:    atomic.Int64{},
		maxPendingCount: int64(maxPendingCount),
		spaceAvailable:  make(chan struct{}, 16), // Buffered to avoid blocking
	}

	res.lastCheckStaleRequestTime.Store(time.Now())
	return res
}

// add adds a new region request to the cache
// It blocks if pendingCount >= maxPendingCount until there's space or ctx is cancelled
func (c *requestCache) add(ctx context.Context, region regionInfo, force bool) (bool, error) {
	start := time.Now()
	ticker := time.NewTicker(addReqRetryInterval)
	defer ticker.Stop()
	addReqRetryLimit := addReqRetryLimit

	for {
		current := c.pendingCount.Load()
		if current < c.maxPendingCount || force {
			// Try to add the request
			req := newRegionReq(region)
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case c.pendingQueue <- req:
				c.pendingCount.Inc()
				cost := time.Since(start)
				metrics.SubscriptionClientAddRegionRequestDuration.Observe(cost.Seconds())
				return true, nil
			case <-ticker.C:
				addReqRetryLimit--
				if addReqRetryLimit <= 0 {
					return false, nil
				}
				continue
			}
		}

		// Wait for space to become available
		select {
		case <-ticker.C:
			addReqRetryLimit--
			if addReqRetryLimit <= 0 {
				return false, nil
			}
			continue
		case <-c.spaceAvailable:
			continue
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

// pop gets the next pending request.
// Note: it doesn't change pendingCount. The slot acquired in add() should be released later
// (e.g. resolve/markStopped/markDone).
func (c *requestCache) pop(ctx context.Context) (regionReq, error) {
	select {
	case req := <-c.pendingQueue:
		return req, nil
	case <-ctx.Done():
		return regionReq{}, ctx.Err()
	}
}

// markSent marks a request as sent and adds it to sent requests.
// It doesn't change pendingCount: the slot is released when the request is finished/removed.
func (c *requestCache) markSent(req regionReq) {
	c.sentRequests.Lock()
	defer c.sentRequests.Unlock()

	m, ok := c.sentRequests.regionReqs[req.regionInfo.subscribedSpan.subID]

	if !ok {
		m = make(map[uint64]regionReq)
		c.sentRequests.regionReqs[req.regionInfo.subscribedSpan.subID] = m
	}

	if oldReq, exists := m[req.regionInfo.verID.GetID()]; exists {
		log.Warn("region request overwritten",
			zap.Uint64("subID", uint64(req.regionInfo.subscribedSpan.subID)),
			zap.Uint64("regionID", req.regionInfo.verID.GetID()),
			zap.Float64("oldAgeSec", time.Since(oldReq.createTime).Seconds()),
			zap.Float64("newAgeSec", time.Since(req.createTime).Seconds()),
			zap.Int("pendingCount", int(c.pendingCount.Load())),
			zap.Int("pendingQueueLen", len(c.pendingQueue)))
		c.markDone()
	}
	m[req.regionInfo.verID.GetID()] = req
}

// markStopped removes a sent request and releases a slot.
func (c *requestCache) markStopped(subID SubscriptionID, regionID uint64) {
	c.sentRequests.Lock()
	defer c.sentRequests.Unlock()

	regionReqs, ok := c.sentRequests.regionReqs[subID]
	if !ok {
		return
	}

	_, exists := regionReqs[regionID]
	if !exists {
		return
	}

	delete(regionReqs, regionID)
	c.markDone()
}

// resolve marks a region as initialized and removes it from sent requests
func (c *requestCache) resolve(subscriptionID SubscriptionID, regionID uint64) bool {
	c.sentRequests.Lock()
	defer c.sentRequests.Unlock()
	regionReqs, ok := c.sentRequests.regionReqs[subscriptionID]
	if !ok {
		return false
	}

	req, exists := regionReqs[regionID]
	if !exists {
		return false
	}

	// Check if the subscription ID matches
	if req.regionInfo.subscribedSpan.subID == subscriptionID {
		delete(regionReqs, regionID)
		c.markDone()
		cost := time.Since(req.createTime).Seconds()
		if cost > 0 && cost < abnormalRequestDurationInSec {
			log.Debug("cdc resolve region request", zap.Uint64("subID", uint64(subscriptionID)), zap.Uint64("regionID", regionID), zap.Float64("cost", cost), zap.Int("pendingCount", int(c.pendingCount.Load())), zap.Int("pendingQueueLen", len(c.pendingQueue)))
			metrics.RegionRequestFinishScanDuration.Observe(cost)
		} else {
			log.Info("region request duration abnormal, skip metric", zap.Float64("cost", cost), zap.Uint64("regionID", regionID))
		}
		return true
	}

	return false
}

// clearStaleRequest clears stale requests from the cache
// Note: Sometimes, the CDC sends the same region request to TiKV multiple times. In such cases, this method is needed to reduce the pendingSize.
func (c *requestCache) clearStaleRequest() {
	if time.Since(c.lastCheckStaleRequestTime.Load()) < checkStaleRequestInterval {
		return
	}
	c.sentRequests.Lock()
	defer c.sentRequests.Unlock()
	for subID, regionReqs := range c.sentRequests.regionReqs {
		for regionID, regionReq := range regionReqs {
			if regionReq.regionInfo.isStopped() ||
				regionReq.regionInfo.subscribedSpan.stopped.Load() ||
				regionReq.regionInfo.lockedRangeState.Initialized.Load() ||
				regionReq.isStale() {
				c.markDone()
				log.Warn("region worker delete stale region request",
					zap.Uint64("subID", uint64(subID)),
					zap.Uint64("regionID", regionID),
					zap.Int("pendingCount", int(c.pendingCount.Load())),
					zap.Int("pendingQueueLen", len(c.pendingQueue)),
					zap.Bool("isRegionStopped", regionReq.regionInfo.isStopped()),
					zap.Bool("isSubscribedSpanStopped", regionReq.regionInfo.subscribedSpan.stopped.Load()),
					zap.Bool("isStale", regionReq.isStale()),
					zap.Time("createTime", regionReq.createTime))
				delete(regionReqs, regionID)
			}
		}
		if len(regionReqs) == 0 {
			delete(c.sentRequests.regionReqs, subID)
		}
	}

	c.lastCheckStaleRequestTime.Store(time.Now())
}

// clear removes all requests and returns them
func (c *requestCache) clear() []regionInfo {
	var regions []regionInfo

	// Drain pending requests from channel
LOOP:
	for {
		select {
		case req := <-c.pendingQueue:
			regions = append(regions, req.regionInfo)
			c.markDone()
		default:
			break LOOP
		}
	}

	c.sentRequests.Lock()
	defer c.sentRequests.Unlock()

	for subID, regionReqs := range c.sentRequests.regionReqs {
		for regionID := range regionReqs {
			regions = append(regions, regionReqs[regionID].regionInfo)
			delete(regionReqs, regionID)
			c.markDone()
		}
		delete(c.sentRequests.regionReqs, subID)
	}
	return regions
}

// getPendingCount returns the current pending count
func (c *requestCache) getPendingCount() int {
	return int(c.pendingCount.Load())
}

func (c *requestCache) markDone() {
	newCount := c.pendingCount.Dec()
	if newCount < 0 {
		c.pendingCount.Store(0)
	}
	// Notify waiting add operations that there's space available.
	select {
	case c.spaceAvailable <- struct{}{}:
	default: // If channel is full, skip notification
	}
}
