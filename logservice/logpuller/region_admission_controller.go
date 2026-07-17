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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/heap"
	"go.uber.org/zap"
)

const (
	abnormalRequestDurationInSec = 60 * 60 * 2 // 2 hours
)

// regionReq is an admission lease for one sent-but-not-initialized region.
// finish and abort are idempotent and return the lease to its worker controller.
type regionReq struct {
	regionInfo regionInfo
	createTime time.Time
	controller *regionAdmissionController
	scanQuota  *memoryQuotaLease
	released   atomic.Bool
}

func (r *regionReq) finish() bool {
	if !r.release() {
		return false
	}

	cost := time.Since(r.createTime).Seconds()
	if cost > 0 && cost < abnormalRequestDurationInSec {
		log.Debug("cdc resolve region request",
			zap.Uint64("subID", uint64(r.regionInfo.subscribedSpan.subID)),
			zap.Uint64("regionID", r.regionInfo.verID.GetID()),
			zap.Float64("cost", cost),
			zap.Int("inflightCount", r.controller.stats().inflight))
		metrics.RegionRequestFinishScanDuration.Observe(cost)
		return true
	}
	log.Info("region request duration abnormal, skip metric",
		zap.Float64("cost", cost),
		zap.Uint64("regionID", r.regionInfo.verID.GetID()))
	return true
}

func (r *regionReq) abort() bool {
	return r.release()
}

func (r *regionReq) isActive() bool {
	return !r.released.Load()
}

func (r *regionReq) release() bool {
	if !r.released.CompareAndSwap(false, true) {
		return false
	}
	r.scanQuota.Release()
	r.controller.release()
	return true
}

// regionAdmissionController owns one request worker's pending queue and
// initial-scan window.
type regionAdmissionController struct {
	// mu guards the window state, inflight count, pending queue and closed flag.
	mu sync.Mutex

	// currentWindow limits ordinary region scans.
	currentWindow int
	// maxWindow is the hard limit for previously initialized and low-lag regions.
	maxWindow int
	// inflight is the number of admitted regions that have not finished their
	// initial scan. It is guarded by mu.
	inflight int
	// pending keeps requests that have not entered the initial-scan window.
	// It is guarded by mu.
	pending *heap.Heap[*regionPriorityTask]
	// memoryQuota gates initial scans using the log puller's global memory
	// pressure. currentTs is sampled when a request is admitted.
	memoryQuota *memoryQuotaController
	currentTs   func() uint64
	// notify wakes workers when a request is submitted or an admission slot is
	// released. The one-element buffer prevents a wakeup from being lost between
	// checking the admission condition and waiting on this channel. Notifications
	// are only signals to recheck state; they do not correspond one-to-one with
	// pending requests or available slots.
	notify chan struct{}
	// closed prevents new submissions and makes waiting workers exit.
	closed bool
}

type regionAdmissionStats struct {
	pending  int
	inflight int
}

func newRegionAdmissionController(
	currentWindow int,
	maxWindowMultiplier int,
	memoryQuota *memoryQuotaController,
	currentTs func() uint64,
) *regionAdmissionController {
	if currentWindow <= 0 {
		currentWindow = 1
	}
	if maxWindowMultiplier <= 0 {
		maxWindowMultiplier = 1
	}
	maxWindow := math.MaxInt
	if currentWindow <= math.MaxInt/maxWindowMultiplier {
		maxWindow = currentWindow * maxWindowMultiplier
	}
	return &regionAdmissionController{
		currentWindow: currentWindow,
		maxWindow:     maxWindow,
		pending:       heap.NewHeap[*regionPriorityTask](),
		memoryQuota:   memoryQuota,
		currentTs:     currentTs,
		notify:        make(chan struct{}, 1),
	}
}

func (c *regionAdmissionController) submit(task *regionPriorityTask) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.pending.AddOrUpdate(task)
	c.notifyOneLocked()
	c.mu.Unlock()
	return true
}

// pop waits for an eligible request. If interrupt is signaled first, it returns
// nil without an error so the worker can handle its control queue.
func (c *regionAdmissionController) pop(
	ctx context.Context,
	interrupt <-chan struct{},
) (*regionReq, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, context.Canceled
		}
		request, scanQuota := c.popEligibleLocked()
		if request != nil {
			c.inflight++
			c.mu.Unlock()
			return &regionReq{
				regionInfo: request.regionInfo,
				createTime: time.Now(),
				controller: c,
				scanQuota:  scanQuota,
			}, nil
		}
		c.mu.Unlock()

		select {
		case <-c.notify:
		case <-interrupt:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *regionAdmissionController) popEligibleLocked() (
	*regionPriorityTask,
	*memoryQuotaLease,
) {
	request, ok := c.pending.PeekTop()
	if !ok {
		return nil, nil
	}
	if c.inflight >= c.windowFor(request) {
		return nil, nil
	}

	scanQuota, admitted := c.memoryQuota.acquireScan(
		request.regionInfo, c.currentTs())
	if !admitted {
		return nil, nil
	}
	request, _ = c.pending.PopTop()
	return request, scanQuota
}

func (c *regionAdmissionController) windowFor(request *regionPriorityTask) int {
	if request.canUseMaxWindow() {
		return c.maxWindow
	}
	return c.currentWindow
}

func (c *regionAdmissionController) release() {
	c.mu.Lock()
	if c.inflight > 0 {
		c.inflight--
		c.notifyOneLocked()
	}
	c.mu.Unlock()
}

func (c *regionAdmissionController) notifyAvailable() {
	c.mu.Lock()
	c.notifyOneLocked()
	c.mu.Unlock()
}

func (c *regionAdmissionController) close() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.notify)
	}
	c.mu.Unlock()
}

func (c *regionAdmissionController) stats() regionAdmissionStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return regionAdmissionStats{
		pending:  c.pending.Len(),
		inflight: c.inflight,
	}
}

func (c *regionAdmissionController) drain() []*regionPriorityTask {
	c.mu.Lock()
	defer c.mu.Unlock()

	requests := make([]*regionPriorityTask, 0, c.pending.Len())
	for {
		request, ok := c.pending.PopTop()
		if !ok {
			return requests
		}
		requests = append(requests, request)
	}
}

func (c *regionAdmissionController) notifyOneLocked() {
	if c.closed {
		return
	}
	select {
	case c.notify <- struct{}{}:
	default:
	}
}
