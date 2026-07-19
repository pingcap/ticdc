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
	scanBytes  uint64
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
	r.controller.memoryQuota.releaseScan(r.scanBytes)
	r.controller.release()
	return true
}

// regionAdmissionController owns one request worker's pending queue and
// initial-scan window.
type regionAdmissionController struct {
	// currentWindow limits ordinary region scans.
	currentWindow int
	// maxWindow is the hard limit for high-priority region scans.
	maxWindow int
	// state guards the window state, inflight count, pending queue and closed flag.
	state struct {
		sync.Mutex

		// inflight is the number of admitted regions that have not finished their
		// initial scan. It is guarded by state.
		inflight int
		// pending keeps requests that have not entered the initial-scan window.
		// It is guarded by state.
		pending *heap.Heap[*regionPriorityTask]
		// closed prevents new submissions and makes waiting workers exit.
		closed bool
	}

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
	controller := &regionAdmissionController{
		currentWindow: currentWindow,
		maxWindow:     maxWindow,
		memoryQuota:   memoryQuota,
		currentTs:     currentTs,
		notify:        make(chan struct{}, 1),
	}
	controller.state.pending = heap.NewHeap[*regionPriorityTask]()
	return controller
}

func (c *regionAdmissionController) submit(task *regionPriorityTask) bool {
	c.state.Lock()
	if c.state.closed {
		c.state.Unlock()
		return false
	}
	c.state.pending.AddOrUpdate(task)
	c.notifyOneLocked()
	c.state.Unlock()
	return true
}

// pop waits for an eligible request. If controlReady is signaled first, it
// returns nil without an error so the worker can handle its control queue.
func (c *regionAdmissionController) pop(
	ctx context.Context,
	controlReady <-chan struct{},
) (*regionReq, error) {
	for {
		c.state.Lock()
		if c.state.closed {
			c.state.Unlock()
			return nil, context.Canceled
		}
		request, scanBytes, memoryReady := c.popEligibleLocked()
		if request != nil {
			c.state.inflight++
			c.state.Unlock()
			return &regionReq{
				regionInfo: request.regionInfo,
				createTime: time.Now(),
				controller: c,
				scanBytes:  scanBytes,
			}, nil
		}
		c.state.Unlock()

		select {
		case <-c.notify:
		case <-memoryReady:
		case <-controlReady:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *regionAdmissionController) popEligibleLocked() (
	*regionPriorityTask,
	uint64,
	<-chan struct{},
) {
	request, ok := c.state.pending.PeekTop()
	if !ok {
		return nil, 0, nil
	}
	if c.state.inflight >= c.windowFor(request) {
		return nil, 0, nil
	}

	scanBytes, memoryReady, admitted := c.memoryQuota.acquireScan(
		request.regionInfo, c.currentTs())
	if !admitted {
		return nil, 0, memoryReady
	}
	request, _ = c.state.pending.PopTop()
	return request, scanBytes, nil
}

func (c *regionAdmissionController) windowFor(request *regionPriorityTask) int {
	if request.canUseMaxWindow() {
		return c.maxWindow
	}
	return c.currentWindow
}

func (c *regionAdmissionController) release() {
	c.state.Lock()
	if c.state.inflight > 0 {
		c.state.inflight--
		c.notifyOneLocked()
	}
	c.state.Unlock()
}

func (c *regionAdmissionController) close() {
	c.state.Lock()
	if !c.state.closed {
		c.state.closed = true
		close(c.notify)
	}
	c.state.Unlock()
}

func (c *regionAdmissionController) stats() regionAdmissionStats {
	c.state.Lock()
	defer c.state.Unlock()
	return regionAdmissionStats{
		pending:  c.state.pending.Len(),
		inflight: c.state.inflight,
	}
}

func (c *regionAdmissionController) drain() []*regionPriorityTask {
	c.state.Lock()
	defer c.state.Unlock()

	requests := make([]*regionPriorityTask, 0, c.state.pending.Len())
	for {
		request, ok := c.state.pending.PopTop()
		if !ok {
			return requests
		}
		requests = append(requests, request)
	}
}

func (c *regionAdmissionController) notifyOneLocked() {
	if c.state.closed {
		return
	}
	select {
	case c.notify <- struct{}{}:
	default:
	}
}
