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
	"github.com/pingcap/ticdc/utils/notifyqueue"
	"go.uber.org/zap"
)

const (
	addReqRetryInterval          = time.Millisecond
	addReqRetryLimit             = 3
	abnormalRequestDurationInSec = 60 * 60 * 2 // 2 hours
)

// regionReq tracks one region request from admission to cleanup.
type regionReq struct {
	regionInfo regionInfo
	createTime time.Time
}

func newRegionReq(region regionInfo) *regionReq {
	return &regionReq{
		regionInfo: region,
		createTime: time.Now(),
	}
}

// requestCache owns flow-control slots from enqueue until the initial region
// scan finishes or the request is aborted.
type requestCache struct {
	mu sync.Mutex

	maxPendingCount int
	// requests is the source of truth for every live request.
	requests map[*regionReq]struct{}
	queue    *notifyqueue.Queue[*regionReq]

	spaceAvailable chan struct{}
}

func newRequestCache(maxPendingCount int) *requestCache {
	return &requestCache{
		maxPendingCount: maxPendingCount,
		requests:        make(map[*regionReq]struct{}),
		queue:           notifyqueue.New[*regionReq](),
		spaceAvailable:  make(chan struct{}, 1),
	}
}

func (c *requestCache) add(ctx context.Context, region regionInfo, force bool) (bool, error) {
	start := time.Now()
	ticker := time.NewTicker(addReqRetryInterval)
	defer ticker.Stop()
	retries := addReqRetryLimit

	for {
		if c.tryAdd(region, force) {
			metrics.SubscriptionClientAddRegionRequestDuration.Observe(time.Since(start).Seconds())
			return true, nil
		}

		select {
		case <-ticker.C:
			retries--
			if retries <= 0 {
				return false, nil
			}
		case <-c.spaceAvailable:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

func (c *requestCache) tryAdd(region regionInfo, force bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.requests) >= c.maxPendingCount && !force {
		return false
	}

	req := newRegionReq(region)
	c.requests[req] = struct{}{}
	c.queue.Push(req)
	return true
}

func (c *requestCache) pop(ctx context.Context) (*regionReq, error) {
	for {
		if req := c.tryPop(); req != nil {
			return req, nil
		}

		select {
		case <-c.queue.Ready():
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *requestCache) tryPop() *regionReq {
	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		req, ok := c.queue.TryPop()
		if !ok {
			return nil
		}
		if _, ok := c.requests[req]; ok {
			return req
		}
		c.logRemovedRequest(req)
	}
}

func (c *requestCache) isPending(req *regionReq) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.requests[req]
	return ok
}

func (c *requestCache) resolve(req *regionReq) bool {
	if !c.remove(req) {
		return false
	}

	cost := time.Since(req.createTime).Seconds()
	if cost > 0 && cost < abnormalRequestDurationInSec {
		log.Debug("cdc resolve region request",
			zap.Uint64("subID", uint64(req.regionInfo.subscribedSpan.subID)),
			zap.Uint64("regionID", req.regionInfo.verID.GetID()),
			zap.Float64("cost", cost),
			zap.Int("pendingCount", c.pendingCount()))
		metrics.RegionRequestFinishScanDuration.Observe(cost)
		return true
	}
	log.Info("region request duration abnormal, skip metric",
		zap.Float64("cost", cost),
		zap.Uint64("regionID", req.regionInfo.verID.GetID()))
	return true
}

func (c *requestCache) remove(req *regionReq) bool {
	if req == nil {
		return false
	}

	c.mu.Lock()
	removed := c.removeLocked(req)
	c.mu.Unlock()

	if removed {
		c.notifySpace()
	}
	return removed
}

// drain removes all remaining requests. A worker must stop its tracked region
// states first, so only requests that were never sent remain here.
func (c *requestCache) drain() []regionInfo {
	c.mu.Lock()
	regions := make([]regionInfo, 0, len(c.requests))
	for req := range c.requests {
		regions = append(regions, req.regionInfo)
	}
	clear(c.requests)
	c.queue.Drain()
	c.mu.Unlock()

	if len(regions) > 0 {
		c.notifySpace()
	}
	return regions
}

func (c *requestCache) close() {
	c.mu.Lock()
	removed := len(c.requests) > 0
	clear(c.requests)
	c.queue.Drain()
	c.mu.Unlock()

	if removed {
		c.notifySpace()
	}
}

func (c *requestCache) pendingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.requests)
}

func (c *requestCache) removeLocked(req *regionReq) bool {
	if req == nil {
		return false
	}
	if _, ok := c.requests[req]; !ok {
		return false
	}
	delete(c.requests, req)
	return true
}

func (c *requestCache) logRemovedRequest(req *regionReq) {
	log.Warn("request cache pops a removed request",
		zap.Uint64("subID", uint64(req.regionInfo.subscribedSpan.subID)),
		zap.Uint64("regionID", req.regionInfo.verID.GetID()))
}

func (c *requestCache) ready() <-chan struct{} {
	return c.queue.Ready()
}

func (c *requestCache) notifySpace() {
	select {
	case c.spaceAvailable <- struct{}{}:
	default:
	}
}
