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
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

const (
	regionScanPauseRatio  = 0.5
	regionScanResumeRatio = 0.4
)

// pullerMemoryQuota is the admission controller for events entering the puller
// dynamic stream. It is intentionally independent from dynamic stream memory
// control because it also controls region scan admission.
type pullerMemoryQuota struct {
	mu sync.Mutex

	capacity uint64
	used     uint64
	closed   bool

	memoryReleased chan struct{}

	regionScanPaused bool
	regionScanResume chan struct{}

	subscriptions map[SubscriptionID]*pullerMemorySubscription
}

type pullerMemorySubscription struct {
	usage uint64
}

type pullerMemoryReservation struct {
	quota        *pullerMemoryQuota
	subID        SubscriptionID
	subscription *pullerMemorySubscription
	bytes        uint64
	released     atomic.Bool
}

func newPullerMemoryQuota(capacity uint64) *pullerMemoryQuota {
	metrics.PullerRegionScanGate.Set(1)
	metrics.PullerMemoryQuota.WithLabelValues("quota").Set(float64(capacity))
	metrics.PullerMemoryQuota.WithLabelValues("used").Set(0)
	return &pullerMemoryQuota{
		capacity:         capacity,
		memoryReleased:   make(chan struct{}),
		regionScanResume: make(chan struct{}),
		subscriptions:    make(map[SubscriptionID]*pullerMemorySubscription),
	}
}

func (q *pullerMemoryQuota) acquire(
	ctx context.Context, subID SubscriptionID, bytes uint64, stopped <-chan struct{},
) (*pullerMemoryReservation, error) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return nil, context.Canceled
		}
		// A single oversized event must be allowed to run alone, otherwise it can
		// never make progress and permanently deadlocks the puller.
		fits := q.used <= q.capacity && bytes <= q.capacity-q.used
		if fits || q.used == 0 && bytes > q.capacity {
			subscription := q.subscriptions[subID]
			if subscription == nil {
				subscription = &pullerMemorySubscription{}
				q.subscriptions[subID] = subscription
			}
			q.used += bytes
			subscription.usage += bytes
			q.updateRegionScanStateLocked()
			q.mu.Unlock()
			return &pullerMemoryReservation{
				quota:        q,
				subID:        subID,
				subscription: subscription,
				bytes:        bytes,
			}, nil
		}
		memoryReleased := q.memoryReleased
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-stopped:
			return nil, context.Canceled
		case <-memoryReleased:
		}
	}
}

func (r *pullerMemoryReservation) release() {
	if r == nil || !r.released.CompareAndSwap(false, true) {
		return
	}
	r.quota.release(r)
}

func (q *pullerMemoryQuota) release(reservation *pullerMemoryReservation) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.subscriptions[reservation.subID] != reservation.subscription {
		return
	}

	usage := reservation.subscription.usage
	if usage < reservation.bytes || q.used < reservation.bytes {
		log.Error("puller memory quota accounting underflow",
			zap.Uint64("subscriptionID", uint64(reservation.subID)),
			zap.Uint64("releaseBytes", reservation.bytes),
			zap.Uint64("subscriptionUsage", usage),
			zap.Uint64("memoryUsage", q.used))
		return
	}

	q.used -= reservation.bytes
	usage -= reservation.bytes
	if usage == 0 {
		delete(q.subscriptions, reservation.subID)
	} else {
		reservation.subscription.usage = usage
	}
	q.notifyMemoryReleasedLocked()
	q.updateRegionScanStateLocked()
}

// releaseSubscription releases all reservations owned by a removed
// subscription. Reservation ownership makes later stale releases no-ops.
func (q *pullerMemoryQuota) releaseSubscription(subID SubscriptionID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}

	subscription := q.subscriptions[subID]
	if subscription == nil {
		return
	}
	usage := subscription.usage
	if usage > q.used {
		log.Error("puller subscription memory usage exceeds total usage",
			zap.Uint64("subscriptionID", uint64(subID)),
			zap.Uint64("subscriptionUsage", usage),
			zap.Uint64("memoryUsage", q.used))
		usage = q.used
	}
	q.used -= usage
	delete(q.subscriptions, subID)
	if usage != 0 {
		q.notifyMemoryReleasedLocked()
		q.updateRegionScanStateLocked()
	}
}

func (q *pullerMemoryQuota) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	q.used = 0
	clear(q.subscriptions)
	metrics.PullerMemoryQuota.WithLabelValues("used").Set(0)
	close(q.memoryReleased)
	if q.regionScanPaused {
		q.regionScanPaused = false
		close(q.regionScanResume)
	}
	metrics.PullerRegionScanGate.Set(1)
}

func (q *pullerMemoryQuota) notifyMemoryReleasedLocked() {
	close(q.memoryReleased)
	q.memoryReleased = make(chan struct{})
}

func (q *pullerMemoryQuota) updateRegionScanStateLocked() {
	usageRatio := float64(q.used) / float64(q.capacity)
	switch {
	case !q.regionScanPaused && usageRatio >= regionScanPauseRatio:
		q.regionScanPaused = true
		q.regionScanResume = make(chan struct{})
		metrics.PullerRegionScanGate.Set(0)
		metrics.PullerRegionScanGateTransition.WithLabelValues("pause").Inc()
		log.Info("puller pauses region scans",
			zap.Uint64("memoryUsage", q.used),
			zap.Uint64("memoryQuota", q.capacity),
			zap.Float64("memoryUsageRatio", usageRatio))
	case q.regionScanPaused && usageRatio < regionScanResumeRatio:
		q.regionScanPaused = false
		close(q.regionScanResume)
		metrics.PullerRegionScanGate.Set(1)
		metrics.PullerRegionScanGateTransition.WithLabelValues("resume").Inc()
		log.Info("puller resumes region scans",
			zap.Uint64("memoryUsage", q.used),
			zap.Uint64("memoryQuota", q.capacity),
			zap.Float64("memoryUsageRatio", usageRatio))
	}
}

func (q *pullerMemoryQuota) waitRegionScanAllowed(ctx context.Context) error {
	for {
		resume, paused := q.regionScanResumeNotify()
		if !paused {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resume:
		}
	}
}

func (q *pullerMemoryQuota) regionScanResumeNotify() (<-chan struct{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.regionScanResume, q.regionScanPaused
}

func (q *pullerMemoryQuota) usage() (used, capacity uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used, q.capacity
}
