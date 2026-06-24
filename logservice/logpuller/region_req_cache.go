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
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/heap"
	"go.uber.org/zap"
)

const (
	addReqRetryInterval          = time.Millisecond
	addReqRetryLimit             = 3
	abnormalRequestDurationInSec = 60 * 60 * 2 // 2 hours
)

// regionReq is a pending or in-flight Region register request.
type regionReq struct {
	regionInfo regionInfo
	createTime time.Time
	priority   int
	heapIndex  int
}

func newRegionReq(region regionInfo, priority int) *regionReq {
	return &regionReq{
		regionInfo: region,
		createTime: time.Now(),
		priority:   priority,
	}
}

func (r *regionReq) SetHeapIndex(index int) {
	r.heapIndex = index
}

func (r *regionReq) GetHeapIndex() int {
	return r.heapIndex
}

func (r *regionReq) LessThan(other *regionReq) bool {
	if r.priority == other.priority {
		return r.createTime.Before(other.createTime)
	}
	return r.priority < other.priority
}

// regionRegisterQueue is the bounded, store-level queue shared by all workers.
// Queue capacity limits local pending work only; active scans are controlled by
// regionScanLimiter.
type regionRegisterQueue struct {
	mu       sync.Mutex
	requests *heap.Heap[*regionReq]
	capacity int
	changed  chan struct{}
}

func newRegionRegisterQueue(capacity int) *regionRegisterQueue {
	if capacity <= 0 {
		capacity = 1
	}
	return &regionRegisterQueue{
		requests: heap.NewHeap[*regionReq](),
		capacity: capacity,
		changed:  make(chan struct{}),
	}
}

func (q *regionRegisterQueue) add(
	ctx context.Context, region regionInfo, priority int,
) (bool, error) {
	start := time.Now()
	ticker := time.NewTicker(addReqRetryInterval)
	defer ticker.Stop()
	retries := addReqRetryLimit

	for {
		q.mu.Lock()
		if q.requests.Len() < q.capacity {
			q.requests.AddOrUpdate(newRegionReq(region, priority))
			q.notifyChangedLocked()
			q.mu.Unlock()
			metrics.SubscriptionClientAddRegionRequestDuration.Observe(time.Since(start).Seconds())
			return true, nil
		}
		changed := q.changed
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-changed:
		case <-ticker.C:
			retries--
			if retries <= 0 {
				return false, nil
			}
		}
	}
}

func (q *regionRegisterQueue) tryPopOrNotify() (*regionReq, bool, <-chan struct{}) {
	q.mu.Lock()
	defer q.mu.Unlock()
	req, ok := q.requests.PopTop()
	if ok {
		q.notifyChangedLocked()
		return req, true, nil
	}
	return nil, false, q.changed
}

func (q *regionRegisterQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.requests.Len()
}

func (q *regionRegisterQueue) clear() []regionInfo {
	q.mu.Lock()
	defer q.mu.Unlock()
	regions := make([]regionInfo, 0, q.requests.Len())
	for {
		req, ok := q.requests.PopTop()
		if !ok {
			break
		}
		regions = append(regions, req.regionInfo)
	}
	q.notifyChangedLocked()
	return regions
}

func (q *regionRegisterQueue) notifyChangedLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}

// regionScanLimiter controls the exact number of active incremental scans for
// one store.
type regionScanLimiter struct {
	mu        sync.Mutex
	limit     int
	active    int
	available chan struct{}
}

func newRegionScanLimiter(limit int) *regionScanLimiter {
	if limit <= 0 {
		limit = 1
	}
	return &regionScanLimiter{
		limit:     limit,
		available: make(chan struct{}),
	}
}

func (l *regionScanLimiter) tryAcquireOrNotify() (*regionScanSlot, <-chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active >= l.limit {
		return nil, l.available
	}
	l.active++
	return &regionScanSlot{limiter: l}, nil
}

func (l *regionScanLimiter) usage() (active, limit int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active, l.limit
}

func (l *regionScanLimiter) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active <= 0 {
		log.Error("region scan limiter underflow", zap.Int("limit", l.limit))
		return
	}
	l.active--
	close(l.available)
	l.available = make(chan struct{})
}

// regionScanSlot is an idempotent ownership token for one active scan.
type regionScanSlot struct {
	limiter  *regionScanLimiter
	released atomic.Bool
}

func (s *regionScanSlot) release() {
	if s == nil || !s.released.CompareAndSwap(false, true) {
		return
	}
	s.limiter.release()
}

type trackedRegionRequest struct {
	req  *regionReq
	slot *regionScanSlot
}

// regionRequestTracker owns the sent Register requests and scan slots for one
// worker's gRPC stream.
type regionRequestTracker struct {
	mu       sync.Mutex
	requests map[SubscriptionID]map[uint64]trackedRegionRequest
}

func newRegionRequestTracker() *regionRequestTracker {
	return &regionRequestTracker{
		requests: make(map[SubscriptionID]map[uint64]trackedRegionRequest),
	}
}

func (t *regionRequestTracker) track(req *regionReq, slot *regionScanSlot) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	subID := req.regionInfo.subscribedSpan.subID
	regionID := req.regionInfo.verID.GetID()
	regions := t.requests[subID]
	if regions == nil {
		regions = make(map[uint64]trackedRegionRequest)
		t.requests[subID] = regions
	}
	if _, exists := regions[regionID]; exists {
		return false
	}
	regions[regionID] = trackedRegionRequest{req: req, slot: slot}
	return true
}

func (t *regionRequestTracker) stop(subID SubscriptionID, regionID uint64) bool {
	tracked, ok := t.remove(subID, regionID)
	if ok {
		tracked.slot.release()
	}
	return ok
}

func (t *regionRequestTracker) resolve(subID SubscriptionID, regionID uint64) bool {
	tracked, ok := t.remove(subID, regionID)
	if !ok {
		return false
	}
	tracked.slot.release()
	cost := time.Since(tracked.req.createTime).Seconds()
	if cost > 0 && cost < abnormalRequestDurationInSec {
		metrics.RegionRequestFinishScanDuration.Observe(cost)
	} else {
		log.Info("region request duration abnormal, skip metric",
			zap.Float64("cost", cost),
			zap.Uint64("regionID", regionID))
	}
	return true
}

func (t *regionRequestTracker) remove(
	subID SubscriptionID, regionID uint64,
) (trackedRegionRequest, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	regions := t.requests[subID]
	if regions == nil {
		return trackedRegionRequest{}, false
	}
	tracked, ok := regions[regionID]
	if !ok {
		return trackedRegionRequest{}, false
	}
	delete(regions, regionID)
	if len(regions) == 0 {
		delete(t.requests, subID)
	}
	return tracked, true
}

func (t *regionRequestTracker) removeSubscription(subID SubscriptionID) {
	t.mu.Lock()
	regions := t.requests[subID]
	delete(t.requests, subID)
	t.mu.Unlock()
	for _, tracked := range regions {
		tracked.slot.release()
	}
}

func (t *regionRequestTracker) clear() []regionInfo {
	t.mu.Lock()
	requests := t.requests
	t.requests = make(map[SubscriptionID]map[uint64]trackedRegionRequest)
	t.mu.Unlock()

	var regions []regionInfo
	for _, trackedRegions := range requests {
		for _, tracked := range trackedRegions {
			regions = append(regions, tracked.req.regionInfo)
			tracked.slot.release()
		}
	}
	return regions
}

func (t *regionRequestTracker) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	count := 0
	for _, regions := range t.requests {
		count += len(regions)
	}
	return count
}
