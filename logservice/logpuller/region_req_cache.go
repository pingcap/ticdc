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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	checkStaleRequestInterval = time.Second * 5
	requestGCLifeTime         = time.Minute * 20
)

// regionReq represents a wrapped region request with state
type regionReq struct {
	regionInfo regionInfo
	createTime time.Time
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

	// counter for sent but not initialized requests
	pendingCount atomic.Int64
	// maximum number of pending requests allowed
	maxPendingCount int64

	// channel to signal when space becomes available
	spaceAvailable chan struct{}

	lastCheckStaleRequestTime time.Time
}

func newRequestCache(maxPendingCount int) *requestCache {
	log.Info("fizz cdc new request cache", zap.Int("maxPendingCount", maxPendingCount))
	return &requestCache{
		pendingQueue: make(chan regionReq, maxPendingCount), // Large buffer to reduce blocking
		sentRequests: struct {
			sync.RWMutex
			regionReqs map[SubscriptionID]map[uint64]regionReq
		}{regionReqs: make(map[SubscriptionID]map[uint64]regionReq)},
		pendingCount:              atomic.Int64{},
		maxPendingCount:           int64(maxPendingCount),
		spaceAvailable:            make(chan struct{}, 16), // Buffered to avoid blocking
		lastCheckStaleRequestTime: time.Now(),
	}
}

// add adds a new region request to the cache
// It blocks if pendingCount >= maxPendingCount until there's space or ctx is cancelled
func (c *requestCache) add(ctx context.Context, region regionInfo, force bool) error {
	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 1)
	defer ticker.Stop()

	c.clearStaleRequest()
	for {
		current := c.pendingCount.Load()
		if current < c.maxPendingCount || force {
			// Try to add the request
			req := regionReq{
				regionInfo: region,
				createTime: time.Now(),
			}

			select {
			case c.pendingQueue <- req:
				cost := time.Since(start)
				metrics.SubscriptionClientAddRegionRequestCost.Observe(cost.Seconds())
				log.Info("fizz cdc add region request success", zap.String("regionID", fmt.Sprintf("%d", region.verID.GetID())), zap.Float64("cost", cost.Seconds()), zap.Int("pendingCount", int(current)), zap.Int("pendingQueueLen", len(c.pendingQueue)))
				c.pendingCount.Add(1)
				metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Inc()
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Wait for space to become available
		select {
		case <-ticker.C:
			continue
		case <-c.spaceAvailable:
			continue // Retry
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// pop gets the next pending request, returns nil if queue is empty
func (c *requestCache) pop(ctx context.Context) (*regionReq, error) {
	select {
	case req := <-c.pendingQueue:
		return &req, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// markSent marks a request as sent and adds it to sent requests
func (c *requestCache) markSent(req regionReq) {
	c.sentRequests.Lock()
	defer c.sentRequests.Unlock()
	if _, ok := c.sentRequests.regionReqs[req.regionInfo.subscribedSpan.subID]; !ok {
		c.sentRequests.regionReqs[req.regionInfo.subscribedSpan.subID] = make(map[uint64]regionReq)
	}
	c.sentRequests.regionReqs[req.regionInfo.subscribedSpan.subID][req.regionInfo.verID.GetID()] = req
}

// markStopped removes a sent request without changing pending count (for stopped regions)
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
	c.pendingCount.Add(-1)
	log.Info("fizz cdc mark stopped region request", zap.Uint64("subID", uint64(subID)), zap.Uint64("regionID", regionID), zap.Int("pendingCount", int(c.pendingCount.Load())), zap.Int("pendingQueueLen", len(c.pendingQueue)))
	metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Dec()
	// Notify waiting add operations that there's space available
	select {
	case c.spaceAvailable <- struct{}{}:
	default: // If channel is full, skip notification
	}
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
		c.pendingCount.Add(-1)
		metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Dec()
		cost := time.Since(req.createTime)
		log.Info("fizz cdc resolve region request", zap.String("regionID", fmt.Sprintf("%d", regionID)), zap.Float64("cost", cost.Seconds()), zap.Int("pendingCount", int(c.pendingCount.Load())), zap.Int("pendingQueueLen", len(c.pendingQueue)))
		metrics.RegionRequestFinishScanDuration.WithLabelValues(fmt.Sprintf("%d", regionID)).Observe(cost.Seconds())
		// Notify waiting add operations that there's space available
		select {
		case c.spaceAvailable <- struct{}{}:
		default: // If channel is full, skip notification
		}
		return true
	}

	return false
}

func (c *requestCache) clearStaleRequest() {
	if time.Since(c.lastCheckStaleRequestTime) < checkStaleRequestInterval {
		return
	}
	c.sentRequests.Lock()
	defer c.sentRequests.Unlock()
	for subID, regionReqs := range c.sentRequests.regionReqs {
		for regionID, regionReq := range regionReqs {
			if regionReq.regionInfo.isStopped() || regionReq.isStale() {
				c.pendingCount.Add(-1)
				metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Dec()
				log.Info("fizz cdc delete stale region request", zap.Uint64("subID", uint64(subID)), zap.Uint64("regionID", regionID), zap.Int("pendingCount", int(c.pendingCount.Load())), zap.Int("pendingQueueLen", len(c.pendingQueue)), zap.Bool("isStopped", regionReq.regionInfo.isStopped()), zap.Bool("isStale", regionReq.isStale()))
				delete(regionReqs, regionID)
			}
		}
		if len(regionReqs) == 0 {
			delete(c.sentRequests.regionReqs, subID)
		}
	}

	c.lastCheckStaleRequestTime = time.Now()
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
			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Dec()
		default:
			break LOOP
		}
	}

	c.sentRequests.Lock()
	defer c.sentRequests.Unlock()

	for subID, regionReqs := range c.sentRequests.regionReqs {
		for regionID := range regionReqs {
			regions = append(regions, regionReqs[regionID].regionInfo)
			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Dec()
			delete(regionReqs, regionID)
		}
		delete(c.sentRequests.regionReqs, subID)
	}
	// Reset counter
	c.pendingCount.Store(0)
	return regions
}

// getPendingCount returns the current pending count
func (c *requestCache) getPendingCount() int {
	return int(c.pendingCount.Load())
}
