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

package eventservice

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/atomic"
)

const (
	scanAdmissionSweepInterval = time.Second
	scanAdmissionWaitThreshold = scanAdmissionSweepInterval
)

type scanAdmissionWaitReason int

const (
	scanAdmissionGranted scanAdmissionWaitReason = iota
	scanAdmissionChangefeedQuota
	scanAdmissionDispatcherQuota
	scanAdmissionProtectedReserve
	scanAdmissionQuotaNotInitialized
)

type nodeScanAdmission struct {
	availableMemoryQuota atomic.Uint64
	protectedReserve     atomic.Uint64
}

type scanAdmissionController struct {
	nodes          sync.Map // node.ID -> *nodeScanAdmission
	nextGrantEpoch atomic.Uint64
}

func newScanAdmissionController() *scanAdmissionController {
	return &scanAdmissionController{}
}

func (c *scanAdmissionController) updateAvailableMemory(nodeID node.ID, available uint64) {
	actual, _ := c.nodes.LoadOrStore(nodeID, &nodeScanAdmission{})
	actual.(*nodeScanAdmission).availableMemoryQuota.Store(available)
}

func (c *scanAdmissionController) availableMemory(nodeID node.ID) (uint64, bool) {
	value, ok := c.nodes.Load(nodeID)
	if !ok {
		return 0, false
	}
	return value.(*nodeScanAdmission).availableMemoryQuota.Load(), true
}

func (c *scanAdmissionController) clearProtectedReserves() {
	c.nodes.Range(func(_, value any) bool {
		value.(*nodeScanAdmission).protectedReserve.Store(0)
		return true
	})
}

func (c *scanAdmissionController) setProtectedReserve(nodeID node.ID, reserve uint64) {
	value, ok := c.nodes.Load(nodeID)
	if !ok {
		return
	}
	value.(*nodeScanAdmission).protectedReserve.Store(reserve)
}

func (c *scanAdmissionController) tryGrant(
	dispatcher *dispatcherStat,
	requestedBytes uint64,
	now time.Time,
) (scanAdmissionGrant, scanAdmissionWaitReason) {
	value, ok := c.nodes.Load(node.ID(dispatcher.info.GetServerID()))
	if !ok {
		return scanAdmissionGrant{}, scanAdmissionQuotaNotInitialized
	}
	nodeAdmission := value.(*nodeScanAdmission)

	if !allocQuota(&dispatcher.availableMemoryQuota, requestedBytes) {
		return scanAdmissionGrant{}, scanAdmissionDispatcherQuota
	}

	protected := dispatcher.isScanAdmissionProtected(now)
	reserve := nodeAdmission.protectedReserve.Load()
	if !allocQuotaWithReserve(&nodeAdmission.availableMemoryQuota, requestedBytes, reserve, protected) {
		releaseQuota(&dispatcher.availableMemoryQuota, requestedBytes)
		if !protected && nodeAdmission.availableMemoryQuota.Load() >= requestedBytes {
			return scanAdmissionGrant{}, scanAdmissionProtectedReserve
		}
		return scanAdmissionGrant{}, scanAdmissionChangefeedQuota
	}

	return scanAdmissionGrant{
		bytes:               requestedBytes,
		changefeedAvailable: &nodeAdmission.availableMemoryQuota,
		dispatcherAvailable: &dispatcher.availableMemoryQuota,
	}, scanAdmissionGranted
}

func (c *scanAdmissionController) recordGrant(dispatcher *dispatcherStat) {
	dispatcher.clearScanAdmissionWait()
	dispatcher.lastScanGrantEpoch.Store(c.nextGrantEpoch.Add(1))
}

func allocQuotaWithReserve(quota *atomic.Uint64, nBytes uint64, reserve uint64, protected bool) bool {
	for {
		available := quota.Load()
		if available < nBytes {
			return false
		}
		if !protected && available-nBytes < reserve {
			return false
		}
		if quota.CompareAndSwap(available, available-nBytes) {
			return true
		}
	}
}

type scanAdmissionGrant struct {
	bytes               uint64
	changefeedAvailable *atomic.Uint64
	dispatcherAvailable *atomic.Uint64
}

func (g *scanAdmissionGrant) releaseAll() {
	g.commit(0)
}

func (g *scanAdmissionGrant) commit(scannedBytes int64) {
	if g.bytes == 0 {
		return
	}
	usedBytes := uint64(0)
	if scannedBytes > 0 {
		usedBytes = min(uint64(scannedBytes), g.bytes)
	}
	unusedBytes := g.bytes - usedBytes
	if unusedBytes > 0 {
		releaseQuota(g.changefeedAvailable, unusedBytes)
		releaseQuota(g.dispatcherAvailable, unusedBytes)
	}
	g.bytes = 0
}

func (c *eventBroker) runScanAdmissionScheduler(ctx context.Context) error {
	ticker := time.NewTicker(scanAdmissionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case now := <-ticker.C:
			c.scheduleWaitingDispatchers(now)
		}
	}
}

func (c *eventBroker) scheduleWaitingDispatchers(now time.Time) {
	c.changefeedMap.Range(func(_, value any) bool {
		c.scheduleChangefeedWaitingDispatchers(value.(*changefeedStatus), now)
		return true
	})
}

func (c *eventBroker) scheduleChangefeedWaitingDispatchers(status *changefeedStatus, now time.Time) {
	protectedWaiters := make(map[node.ID]*dispatcherStat)
	regularWaiters := make(map[node.ID]*dispatcherStat)
	status.scanAdmission.clearProtectedReserves()

	status.dispatchers.Range(func(_, value any) bool {
		dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
		if !eligibleScanAdmissionWaiter(dispatcher) {
			return true
		}
		nodeID := node.ID(dispatcher.info.GetServerID())
		if dispatcher.isScanAdmissionProtected(now) {
			storeEarlierWaiter(protectedWaiters, nodeID, dispatcher)
		} else {
			storeEarlierWaiter(regularWaiters, nodeID, dispatcher)
		}
		return true
	})

	for nodeID, dispatcher := range protectedWaiters {
		status.scanAdmission.setProtectedReserve(nodeID, maxScanLimitInBytes)
		c.tryPushWaitingTask(dispatcher)
	}

	for nodeID, dispatcher := range regularWaiters {
		if _, hasProtected := protectedWaiters[nodeID]; !hasProtected {
			c.tryPushWaitingTask(dispatcher)
		}
	}
}

func eligibleScanAdmissionWaiter(dispatcher *dispatcherStat) bool {
	return dispatcher != nil &&
		!dispatcher.isRemoved.Load() &&
		dispatcher.seq.Load() > 0 &&
		dispatcher.scanAdmissionWaitingSince.Load() > 0
}

func storeEarlierWaiter(
	waiters map[node.ID]*dispatcherStat,
	nodeID node.ID,
	dispatcher *dispatcherStat,
) {
	current, ok := waiters[nodeID]
	if !ok || scanAdmissionWaiterLess(dispatcher, current) {
		waiters[nodeID] = dispatcher
	}
}

func scanAdmissionWaiterLess(left, right *dispatcherStat) bool {
	leftEpoch := left.lastScanGrantEpoch.Load()
	rightEpoch := right.lastScanGrantEpoch.Load()
	if leftEpoch != rightEpoch {
		return leftEpoch < rightEpoch
	}
	leftWaitingSince := left.scanAdmissionWaitingSince.Load()
	rightWaitingSince := right.scanAdmissionWaitingSince.Load()
	if leftWaitingSince != rightWaitingSince {
		return leftWaitingSince < rightWaitingSince
	}
	return left.id.Less(right.id)
}
