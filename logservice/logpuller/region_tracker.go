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
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type trackedRegionStates map[uint64]*regionFeedState

// regionTracker owns the region states tracked by one region request worker.
type regionTracker struct {
	sync.RWMutex
	workerID uint64

	regionsBySubscription map[SubscriptionID]trackedRegionStates
}

func newRegionTracker(workerID uint64) *regionTracker {
	return &regionTracker{
		workerID:              workerID,
		regionsBySubscription: make(map[SubscriptionID]trackedRegionStates),
	}
}

// Track records a region after the worker picks its request and before the
// request is sent to TiKV. An overwritten state no longer has another owner
// that can clean up its request, so Track aborts it explicitly.
func (t *regionTracker) Track(subscriptionID SubscriptionID, regionID uint64, state *regionFeedState) {
	t.Lock()
	regions := t.regionsBySubscription[subscriptionID]
	if regions == nil {
		regions = make(trackedRegionStates)
		t.regionsBySubscription[subscriptionID] = regions
	}
	oldState := regions[regionID]
	regions[regionID] = state
	t.Unlock()

	if oldState == nil {
		return
	}
	log.Warn("region request state overwritten",
		zap.Uint64("workerID", t.workerID),
		zap.Uint64("subscriptionID", uint64(subscriptionID)),
		zap.Uint64("regionID", regionID))
	oldState.abortScanIfNeeded()
}

func (t *regionTracker) Get(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	t.RLock()
	defer t.RUnlock()

	if regions, ok := t.regionsBySubscription[subscriptionID]; ok {
		return regions[regionID]
	}
	return nil
}

func (t *regionTracker) RemoveRegion(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	t.Lock()
	var state *regionFeedState
	if regions, ok := t.regionsBySubscription[subscriptionID]; ok {
		state = regions[regionID]
		delete(regions, regionID)
		if len(regions) == 0 {
			delete(t.regionsBySubscription, subscriptionID)
		}
	}
	t.Unlock()
	return state
}

func (t *regionTracker) RemoveSubscription(subscriptionID SubscriptionID) []*regionFeedState {
	t.Lock()
	regions := t.regionsBySubscription[subscriptionID]
	delete(t.regionsBySubscription, subscriptionID)
	t.Unlock()

	return collectTrackedRegionStates(regions)
}

func (t *regionTracker) Drain() map[SubscriptionID][]*regionFeedState {
	t.Lock()
	regionsBySubscription := t.regionsBySubscription
	t.regionsBySubscription = make(map[SubscriptionID]trackedRegionStates)
	t.Unlock()

	statesBySubscription := make(map[SubscriptionID][]*regionFeedState, len(regionsBySubscription))
	for subID, regions := range regionsBySubscription {
		statesBySubscription[subID] = collectTrackedRegionStates(regions)
	}
	return statesBySubscription
}

func collectTrackedRegionStates(regions trackedRegionStates) []*regionFeedState {
	states := make([]*regionFeedState, 0, len(regions))
	for _, state := range regions {
		if state != nil {
			states = append(states, state)
		}
	}
	return states
}
