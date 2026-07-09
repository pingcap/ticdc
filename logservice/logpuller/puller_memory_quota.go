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
)

const (
	defaultPullerMemoryQuotaBytes = 1 * 1024 * 1024 * 1024
	pullerMemoryQuotaRequestRatio = 0.8
	minPullerEventMemoryBytes     = 256
)

type pullerMemoryQuotaSnapshot struct {
	MaxBytes             uint64
	UsedBytes            uint64
	RegionRequestLimit   uint64
	EventWaiterCount     int
	RegionReqWaiterCount int
	Closed               bool
}

// pullerMemoryQuota provides coarse backpressure for two puller-owned
// boundaries: region events entering dynamic stream and normal region feed
// requests being sent to TiKV. It does not reorder anything; callers wait at
// their current position and continue when memory is released.
type pullerMemoryQuota struct {
	mu sync.Mutex

	maxBytes           uint64
	regionRequestLimit uint64
	usedBytes          uint64

	eventWaiterCount     int
	regionReqWaiterCount int

	closed bool
	notify chan struct{}
}

func newPullerMemoryQuota(maxBytes uint64) *pullerMemoryQuota {
	if maxBytes == 0 {
		maxBytes = defaultPullerMemoryQuotaBytes
	}
	regionRequestLimit := uint64(float64(maxBytes) * pullerMemoryQuotaRequestRatio)
	if regionRequestLimit == 0 {
		regionRequestLimit = 1
	}
	return &pullerMemoryQuota{
		maxBytes:           maxBytes,
		regionRequestLimit: regionRequestLimit,
		notify:             make(chan struct{}),
	}
}

func (q *pullerMemoryQuota) acquireEvent(ctx context.Context, size uint64) (uint64, bool) {
	if size < minPullerEventMemoryBytes {
		size = minPullerEventMemoryBytes
	}

	q.mu.Lock()
	for {
		if q.closed {
			q.mu.Unlock()
			return 0, false
		}
		if q.canAcquireEventLocked(size) {
			q.usedBytes += size
			q.mu.Unlock()
			return size, true
		}

		notify := q.notify
		q.eventWaiterCount++
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			q.mu.Lock()
			q.eventWaiterCount--
			q.mu.Unlock()
			return 0, false
		case <-notify:
			q.mu.Lock()
			q.eventWaiterCount--
		}
	}
}

func (q *pullerMemoryQuota) canAcquireEventLocked(size uint64) bool {
	if q.usedBytes == 0 && size > q.maxBytes {
		return true
	}
	if q.usedBytes > q.maxBytes {
		return false
	}
	return size <= q.maxBytes-q.usedBytes
}

func (q *pullerMemoryQuota) release(size uint64) {
	if size == 0 {
		return
	}

	q.mu.Lock()
	if size >= q.usedBytes {
		q.usedBytes = 0
	} else {
		q.usedBytes -= size
	}
	q.notifyWaitersLocked()
	q.mu.Unlock()
}

func (q *pullerMemoryQuota) waitRegionRequest(ctx context.Context) bool {
	q.mu.Lock()
	for {
		if q.closed {
			q.mu.Unlock()
			return false
		}
		if q.usedBytes < q.regionRequestLimit {
			q.mu.Unlock()
			return true
		}

		notify := q.notify
		q.regionReqWaiterCount++
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			q.mu.Lock()
			q.regionReqWaiterCount--
			q.mu.Unlock()
			return false
		case <-notify:
			q.mu.Lock()
			q.regionReqWaiterCount--
		}
	}
}

func (q *pullerMemoryQuota) close() {
	q.mu.Lock()
	q.closed = true
	q.notifyWaitersLocked()
	q.mu.Unlock()
}

func (q *pullerMemoryQuota) snapshot() pullerMemoryQuotaSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	return pullerMemoryQuotaSnapshot{
		MaxBytes:             q.maxBytes,
		UsedBytes:            q.usedBytes,
		RegionRequestLimit:   q.regionRequestLimit,
		EventWaiterCount:     q.eventWaiterCount,
		RegionReqWaiterCount: q.regionReqWaiterCount,
		Closed:               q.closed,
	}
}

func (q *pullerMemoryQuota) notifyWaitersLocked() {
	close(q.notify)
	q.notify = make(chan struct{})
}

func (s *subscriptionClient) acquireRegionEventQuota(event *regionEvent) bool {
	if s == nil || s.memoryQuota == nil {
		return true
	}
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	chargedBytes, ok := s.memoryQuota.acquireEvent(ctx, uint64(event.getSize()))
	if !ok {
		return false
	}
	event.quotaBytes = chargedBytes
	return true
}

func (s *subscriptionClient) releaseRegionEventQuota(event regionEvent) {
	if s == nil || s.memoryQuota == nil {
		return
	}
	s.memoryQuota.release(event.quotaBytes)
}

func (s *subscriptionClient) waitRegionRequestQuota(ctx context.Context) error {
	if s == nil || s.memoryQuota == nil {
		return nil
	}
	if s.memoryQuota.waitRegionRequest(ctx) {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return context.Canceled
}
