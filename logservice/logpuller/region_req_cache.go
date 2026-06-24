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
	regionReqStageQueued regionReqStage = iota
	regionReqStageProcessing
	regionReqStageSent
	regionReqStageFinished
)

type regionReqKey struct {
	subID    SubscriptionID
	regionID uint64
}

func newRegionReqKey(region regionInfo) regionReqKey {
	return regionReqKey{
		subID:    region.subscribedSpan.subID,
		regionID: region.verID.GetID(),
	}
}

// regionReq tracks one data request from admission to completion.
type regionReq struct {
	regionInfo regionInfo
	createTime time.Time

	cache *requestCache
	key   regionReqKey
	stage regionReqStage

	replacesActive bool
}

func newRegionReq(cache *requestCache, region regionInfo) *regionReq {
	return &regionReq{
		regionInfo: region,
		createTime: time.Now(),
		cache:      cache,
		key:        newRegionReqKey(region),
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

	requests map[*regionReq]struct{}
	current  map[regionReqKey]*regionReq

	ready    []*regionReq
	readyIdx int

	maxPendingCount int

	readyAvailable   chan struct{}
	spaceAvailable   chan struct{}
	onSpaceAvailable func()
}

func newRequestCache(maxPendingCount int, onSpaceAvailable ...func()) *requestCache {
	res := &requestCache{
		requests:         make(map[*regionReq]struct{}),
		current:          make(map[regionReqKey]*regionReq),
		ready:            make([]*regionReq, 0, maxPendingCount),
		maxPendingCount:  maxPendingCount,
		readyAvailable:   make(chan struct{}, 1),
		spaceAvailable:   make(chan struct{}, 1),
		onSpaceAvailable: nil,
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
	req := newRegionReq(c, region)
	notifyReady := false

	c.mu.Lock()
	defer func() {
		c.mu.Unlock()
		if notifyReady {
			c.notifyReady()
		}
	}()

	if existing, ok := c.current[req.key]; ok {
		if existing.stage == regionReqStageQueued {
			existing.regionInfo = region
			return true
		}
		req.replacesActive = true
		log.Warn("duplicate active region request detected, keep newest request",
			zap.Uint64("subID", uint64(existing.key.subID)),
			zap.Uint64("regionID", existing.key.regionID),
			zap.Uint8("stage", uint8(existing.stage)),
			zap.Int("pendingCount", len(c.requests)))
	}
	if len(c.requests) >= c.maxPendingCount && !force {
		return false
	}

	c.requests[req] = struct{}{}
	c.current[req.key] = req
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
	removed := false
	c.mu.Lock()
	if _, ok := c.requests[req]; ok && req.stage == regionReqStageProcessing {
		if req.replacesActive {
			if old := c.findSentLocked(req.key, req); old != nil {
				removed = c.removeLocked(old) || removed
			}
		}
		req.stage = regionReqStageSent
	}
	c.mu.Unlock()

	if removed {
		c.notifySpace()
	}
}

func (c *requestCache) resolve(req *regionReq) bool {
	if !c.remove(req) {
		return false
	}

	cost := time.Since(req.createTime).Seconds()
	if cost > 0 && cost < abnormalRequestDurationInSec {
		log.Debug("cdc resolve region request",
			zap.Uint64("subID", uint64(req.key.subID)),
			zap.Uint64("regionID", req.key.regionID),
			zap.Float64("cost", cost),
			zap.Int("pendingCount", c.getPendingCount()))
		metrics.RegionRequestFinishScanDuration.Observe(cost)
		return true
	}
	log.Info("region request duration abnormal, skip metric",
		zap.Float64("cost", cost),
		zap.Uint64("regionID", req.key.regionID))
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
	c.current = make(map[regionReqKey]*regionReq)
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
	if c.current[req.key] == req {
		delete(c.current, req.key)
	}
	if stage != regionReqStageSent {
		c.compactReadyLocked()
	}
	return true
}

func (c *requestCache) findSentLocked(key regionReqKey, except *regionReq) *regionReq {
	for req := range c.requests {
		if req == except {
			continue
		}
		if req.key == key && req.stage == regionReqStageSent {
			return req
		}
	}
	return nil
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
