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
	"go.uber.org/zap"
)

const (
	addReqRetryInterval          = time.Millisecond * 1
	addReqRetryLimit             = 3
	abnormalRequestDurationInSec = 60 * 60 * 2 // 2 hours
)

type regionReqStage uint8

const (
	// regionReqStageQueued means the request has been admitted but not yet
	// picked up by the worker send loop.
	regionReqStageQueued regionReqStage = iota
	// regionReqStageProcessing means the send loop has popped the request, but
	// it has not been recorded as sent yet.
	regionReqStageProcessing
	// regionReqStageSent means the request has been sent to TiKV and is waiting
	// for initialized/resolved/stopped cleanup.
	regionReqStageSent
	// regionReqStageFinished means the request has left the worker window.
	regionReqStageFinished
)

// regionReq tracks one data request from admission to completion.
type regionReq struct {
	regionInfo regionInfo
	createTime time.Time

	// cache is set by requestCache.add. It lets regionFeedState finish exactly
	// the request that created it instead of looking up by subID/regionID.
	cache *requestCache
	// stage is guarded by requestCache.mu.
	stage regionReqStage
}

func newRegionReq(cache *requestCache, region regionInfo) *regionReq {
	return &regionReq{
		regionInfo: region,
		createTime: time.Now(),
		cache:      cache,
		stage:      regionReqStageQueued,
	}
}

func (r *regionReq) markSent() {
	if r == nil || r.cache == nil {
		return
	}
	r.cache.markSent(r)
}

func (r *regionReq) resolve() bool {
	if r == nil || r.cache == nil {
		return false
	}
	return r.cache.resolve(r)
}

func (r *regionReq) finish() bool {
	if r == nil || r.cache == nil {
		return false
	}
	return r.cache.finish(r)
}

// requestCache manages worker-local data requests with flow control.
//
// requests is the source of truth for live requests. A request is inserted by
// add(), moves through queued/processing/sent, and is removed by resolve(),
// finish(), takeUnsentRegions(), or clear().
type requestCache struct {
	mu sync.Mutex

	// requests owns every live data request in this worker. Its length is the
	// flow-control count used by add(), getPendingCount(), and metrics.
	requests map[*regionReq]struct{}

	// ready is the FIFO list of queued requests. Entries already popped or
	// removed are left as nil/stale and skipped by tryPop; readyIdx is the next
	// candidate index. compactReadyLocked occasionally drops skipped entries.
	ready    []*regionReq
	readyIdx int

	// maxPendingCount limits len(requests) for non-force adds. force adds bypass
	// this limit, matching the old force behavior for high-priority data requests.
	maxPendingCount int

	// readyAvailable wakes a worker blocked in pop() when a queued request is
	// appended. It is a level-trigger hint; callers must re-check ready under mu.
	readyAvailable chan struct{}
	// spaceAvailable wakes add() when a live request leaves requests.
	spaceAvailable chan struct{}
	// onSpaceAvailable lets the store-level deferred scheduler retry a task when
	// this worker frees request capacity.
	onSpaceAvailable func()
}

func newRequestCache(maxPendingCount int, onSpaceAvailable ...func()) *requestCache {
	res := &requestCache{
		requests:        make(map[*regionReq]struct{}),
		ready:           make([]*regionReq, 0, maxPendingCount),
		maxPendingCount: maxPendingCount,
		readyAvailable:  make(chan struct{}, 1),
		spaceAvailable:  make(chan struct{}, 1),
	}
	if len(onSpaceAvailable) > 0 {
		res.onSpaceAvailable = onSpaceAvailable[0]
	}

	return res
}

// add admits a data request into the worker window.
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
	notifyReady := false

	c.mu.Lock()
	defer func() {
		c.mu.Unlock()
		if notifyReady {
			c.notifyReady()
		}
	}()

	if len(c.requests) >= c.maxPendingCount && !force {
		return false
	}

	req := newRegionReq(c, region)
	c.requests[req] = struct{}{}
	c.ready = append(c.ready, req)
	notifyReady = true
	return true
}

func (c *requestCache) canAdd(force bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return force || len(c.requests) < c.maxPendingCount
}

// pop takes the next queued request and moves it into processing state.
func (c *requestCache) pop(ctx context.Context) (*regionReq, error) {
	for {
		if req := c.tryPop(); req != nil {
			return req, nil
		}

		select {
		case <-c.readyAvailable:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *requestCache) tryPop() *regionReq {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.readyIdx < len(c.ready) {
		req := c.ready[c.readyIdx]
		c.ready[c.readyIdx] = nil
		c.readyIdx++

		if req == nil {
			continue
		}
		if _, ok := c.requests[req]; !ok || req.stage != regionReqStageQueued {
			continue
		}

		req.stage = regionReqStageProcessing
		c.compactReadyLocked()
		return req
	}

	c.compactReadyLocked()
	return nil
}

func (c *requestCache) markSent(req *regionReq) {
	c.mu.Lock()
	if _, ok := c.requests[req]; ok && req.stage == regionReqStageProcessing {
		req.stage = regionReqStageSent
	}
	c.mu.Unlock()
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
			zap.Int("pendingCount", c.getPendingCount()))
		metrics.RegionRequestFinishScanDuration.Observe(cost)
		return true
	}
	log.Info("region request duration abnormal, skip metric",
		zap.Float64("cost", cost),
		zap.Uint64("regionID", req.regionInfo.verID.GetID()))
	return true
}

func (c *requestCache) finish(req *regionReq) bool {
	return c.remove(req)
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

func (c *requestCache) takeUnsentRegions() []regionInfo {
	c.mu.Lock()
	regions := make([]regionInfo, 0, len(c.requests))
	removed := 0
	for req := range c.requests {
		if req.stage == regionReqStageSent {
			continue
		}
		regions = append(regions, req.regionInfo)
		if c.removeLocked(req) {
			removed++
		}
	}
	if removed > 0 {
		c.compactReadyLocked()
	}
	c.mu.Unlock()

	if removed > 0 {
		c.notifySpace()
	}
	return regions
}

// clear removes all live requests and returns their regions.
func (c *requestCache) clear() []regionInfo {
	c.mu.Lock()
	regions := make([]regionInfo, 0, len(c.requests))
	for req := range c.requests {
		regions = append(regions, req.regionInfo)
		delete(c.requests, req)
		req.stage = regionReqStageFinished
	}
	removed := len(regions)
	c.ready = c.ready[:0]
	c.readyIdx = 0
	c.mu.Unlock()

	if removed > 0 {
		c.notifySpace()
	}
	return regions
}

// getPendingCount returns the number of queued, processing and sent requests.
func (c *requestCache) getPendingCount() int {
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

	stage := req.stage
	delete(c.requests, req)
	req.stage = regionReqStageFinished
	if stage != regionReqStageSent {
		c.compactReadyLocked()
	}
	return true
}

func (c *requestCache) notifyReady() {
	select {
	case c.readyAvailable <- struct{}{}:
	default:
	}
}

func (c *requestCache) notifySpace() {
	select {
	case c.spaceAvailable <- struct{}{}:
	default:
	}
	if c.onSpaceAvailable != nil {
		c.onSpaceAvailable()
	}
}

func (c *requestCache) compactReadyLocked() {
	if c.readyIdx == 0 {
		return
	}
	if c.readyIdx < len(c.ready) && c.readyIdx < 1024 {
		return
	}

	n := copy(c.ready, c.ready[c.readyIdx:])
	for i := n; i < len(c.ready); i++ {
		c.ready[i] = nil
	}
	c.ready = c.ready[:n]
	c.readyIdx = 0
}
