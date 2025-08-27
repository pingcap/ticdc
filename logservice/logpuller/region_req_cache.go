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

type regionReqState int

const (
	regionReqStatePending regionReqState = iota
	regionReqStateSent
	regionReqStateInitialized
)

// regionReq represents a wrapped region request with state
type regionReq struct {
	regionInfo regionInfo
	state      regionReqState
	createTime time.Time
}

// requestCache manages region requests with flow control
type requestCache struct {
	// pending requests waiting to be sent
	pendingQueue chan regionReq

	// sent requests waiting for initialization (regionID -> regionReq)
	sentRequests sync.Map

	// counter for sent but not initialized requests
	pendingCount atomic.Int64

	// maximum number of pending requests allowed
	maxPendingCount int64

	// channel to signal when space becomes available
	spaceAvailable chan struct{}
}

func newRequestCache(maxPendingCount int) *requestCache {
	log.Info("fizz cdc new request cache", zap.Int("maxPendingCount", maxPendingCount))

	return &requestCache{
		pendingQueue:    make(chan regionReq, maxPendingCount), // Large buffer to reduce blocking
		sentRequests:    sync.Map{},
		pendingCount:    atomic.Int64{},
		maxPendingCount: int64(maxPendingCount),
		spaceAvailable:  make(chan struct{}, 1), // Buffered to avoid blocking
	}
}

// add adds a new region request to the cache
// It blocks if pendingCount >= maxPendingCount until there's space or ctx is cancelled
func (c *requestCache) add(ctx context.Context, region regionInfo, force bool) error {
	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 1)
	defer ticker.Stop()

	for {
		current := c.pendingCount.Load()
		if current < c.maxPendingCount || force {
			// Try to add the request
			req := regionReq{
				regionInfo: region,
				state:      regionReqStatePending,
				createTime: time.Now(),
			}

			select {
			case c.pendingQueue <- req:
				cost := time.Since(start)
				metrics.SubscriptionClientAddRegionRequestCost.Observe(cost.Seconds())

				log.Info("fizz cdc add region request success", zap.String("regionID", fmt.Sprintf("%d", region.verID.GetID())), zap.Float64("cost", cost.Seconds()))
				c.pendingCount.Add(1)
				metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Inc()
				return nil
			case <-ctx.Done():
				log.Info("fizz cdc add region request cancelled", zap.String("regionID", fmt.Sprintf("%d", region.verID.GetID())), zap.Error(ctx.Err()), zap.Int("pendingCount", int(current)), zap.Int("pendingQueueLen", len(c.pendingQueue)))

				return ctx.Err()
			}
		}

		// Wait for space to become available
		select {
		case <-ticker.C:
			log.Info("fizz cdc add region request wait for space", zap.Int("pendingCount", int(current)), zap.Int("maxPendingCount", int(c.maxPendingCount)), zap.Int("pendingQueueLen", len(c.pendingQueue)), zap.String("regionID", fmt.Sprintf("%d", region.verID.GetID())))
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
func (c *requestCache) markSent(req *regionReq) {
	req.state = regionReqStateSent
	regionID := req.regionInfo.verID.GetID()
	c.sentRequests.Store(regionID, *req)
}

// markStopped removes a sent request without changing pending count (for stopped regions)
func (c *requestCache) markStopped(regionID uint64) {
	if _, exists := c.sentRequests.LoadAndDelete(regionID); exists {
		c.pendingCount.Add(-1)
		metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Dec()
		// Notify waiting add operations that there's space available
		select {
		case c.spaceAvailable <- struct{}{}:
		default: // If channel is full, skip notification
		}
	}
}

// resolve marks a region as initialized and removes it from sent requests
func (c *requestCache) resolve(subscriptionID SubscriptionID, regionID uint64) bool {
	if value, exists := c.sentRequests.Load(regionID); exists {
		req := value.(regionReq)
		// Check if the subscription ID matches
		if req.regionInfo.subscribedSpan.subID == subscriptionID {
			c.sentRequests.Delete(regionID)
			c.pendingCount.Add(-1)
			metrics.SubscriptionClientRequestedRegionCount.WithLabelValues("pending").Dec()
			cost := time.Since(req.createTime)
			log.Info("fizz cdc resolve region request", zap.String("regionID", fmt.Sprintf("%d", regionID)), zap.Float64("cost", cost.Seconds()))
			metrics.RegionRequestFinishScanDuration.WithLabelValues(fmt.Sprintf("%d", regionID)).Observe(cost.Seconds())
			// Notify waiting add operations that there's space available
			select {
			case c.spaceAvailable <- struct{}{}:
			default: // If channel is full, skip notification
			}
			return true
		}
	}
	return false
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

	// sentRequests:
	// 	// Collect sent requests
	// 	c.sentRequests.Range(func(key, value interface{}) bool {
	// 		req := value.(regionReq)
	// 		regions = append(regions, req.regionInfo)
	// 		c.sentRequests.Delete(key)
	// 		return true
	// 	})

	// Reset counter
	c.pendingCount.Store(0)

	return regions
}

// getPendingCount returns the current pending count
func (c *requestCache) getPendingCount() int {
	return int(c.pendingCount.Load())
}
