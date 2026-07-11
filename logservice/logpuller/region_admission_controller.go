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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/heap"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	fastRegionScanLag            = 10 * time.Minute
	abnormalRequestDurationInSec = 60 * 60 * 2 // 2 hours
)

// pendingRegionRequest waits in the store-level admission queue. It does not
// own an admission slot until it is returned by pop or tryPop.
type pendingRegionRequest struct {
	task       PriorityTask
	regionInfo regionInfo
	fastScan   bool
	heapIndex  int
}

func (r *pendingRegionRequest) SetHeapIndex(index int) {
	r.heapIndex = index
}

func (r *pendingRegionRequest) GetHeapIndex() int {
	return r.heapIndex
}

func (r *pendingRegionRequest) LessThan(other *pendingRegionRequest) bool {
	if r.task.GetTaskType() != other.task.GetTaskType() {
		return r.task.GetTaskType() == TaskHighPrior
	}
	if r.canUseMaxWindow() != other.canUseMaxWindow() {
		return r.canUseMaxWindow()
	}
	return r.task.LessThan(other.task)
}

func (r *pendingRegionRequest) canUseMaxWindow() bool {
	return r.fastScan || r.task.GetTaskType() == TaskHighPrior
}

// regionReq is an admission lease for one sent-but-not-initialized region.
// finish and abort are idempotent and return the lease to the store controller.
type regionReq struct {
	regionInfo regionInfo
	createTime time.Time
	controller *regionAdmissionController
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
			zap.Int("inflightCount", r.controller.inflightCount()))
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
	return r != nil && !r.released.Load()
}

func (r *regionReq) release() bool {
	if r == nil || !r.released.CompareAndSwap(false, true) {
		return false
	}
	r.controller.release()
	return true
}

// regionAdmissionController owns the pending queue and initial-scan window for
// all request workers connected to one TiKV store.
type regionAdmissionController struct {
	mu sync.Mutex

	currentWindow int
	maxWindow     int
	inflight      int
	pending       *heap.Heap[*pendingRegionRequest]
	notify        chan struct{}
	closed        bool
}

func newRegionAdmissionController(currentWindow, maxWindowMultiplier int) *regionAdmissionController {
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
		pending:       heap.NewHeap[*pendingRegionRequest](),
		notify:        make(chan struct{}, 1),
	}
}

func (c *regionAdmissionController) submit(
	task PriorityTask,
	region regionInfo,
	currentTs uint64,
) bool {
	request := &pendingRegionRequest{
		task:       task,
		regionInfo: region,
		fastScan:   regionScanLag(currentTs, region.resolvedTs()) < fastRegionScanLag,
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.pending.AddOrUpdate(request)
	c.notifyOneLocked()
	c.mu.Unlock()
	return true
}

func regionScanLag(currentTs, checkpointTs uint64) time.Duration {
	currentTime := oracle.GetTimeFromTS(currentTs)
	checkpointTime := oracle.GetTimeFromTS(checkpointTs)
	if !currentTime.After(checkpointTime) {
		return 0
	}
	return currentTime.Sub(checkpointTime)
}

func (c *regionAdmissionController) pop(ctx context.Context) (*regionReq, error) {
	for {
		request, closed := c.tryPop()
		if request != nil {
			return request, nil
		}
		if closed {
			return nil, context.Canceled
		}

		select {
		case <-c.notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// tryPop returns whether the controller has been closed as its second result.
func (c *regionAdmissionController) tryPop() (*regionReq, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, true
	}
	request := c.popEligibleLocked()
	if request == nil {
		return nil, false
	}
	c.inflight++
	if c.hasEligibleRequestLocked() {
		c.notifyOneLocked()
	}
	return &regionReq{
		regionInfo: request.regionInfo,
		createTime: time.Now(),
		controller: c,
	}, false
}

func (c *regionAdmissionController) popEligibleLocked() *pendingRegionRequest {
	request, ok := c.pending.PeekTop()
	if !ok {
		return nil
	}
	if c.inflight >= c.windowFor(request) {
		return nil
	}
	request, _ = c.pending.PopTop()
	return request
}

func (c *regionAdmissionController) hasEligibleRequestLocked() bool {
	request, ok := c.pending.PeekTop()
	if !ok {
		return false
	}
	return c.inflight < c.windowFor(request)
}

func (c *regionAdmissionController) windowFor(request *pendingRegionRequest) int {
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

func (c *regionAdmissionController) close() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.notify)
	}
	c.mu.Unlock()
}

func (c *regionAdmissionController) ready() <-chan struct{} {
	return c.notify
}

func (c *regionAdmissionController) inflightCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inflight
}

func (c *regionAdmissionController) pendingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pending.Len()
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
