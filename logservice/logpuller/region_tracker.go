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

import "sync"

type trackedRegionStates map[uint64]*regionFeedState

// regionTracker owns the region states tracked by one region request worker.
type regionTracker struct {
	mu sync.RWMutex

	regionsBySubscription map[SubscriptionID]trackedRegionStates
}

func newRegionTracker() *regionTracker {
	return &regionTracker{
		regionsBySubscription: make(map[SubscriptionID]trackedRegionStates),
	}
}

// Get returns the state tracked by a subscription and region.
func (t *regionTracker) Get(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if regions, ok := t.regionsBySubscription[subscriptionID]; ok {
		return regions[regionID]
	}
	return nil
}

// Replace records a region state and returns the state previously tracked by
// the same subscription and region, if any.
func (t *regionTracker) Replace(
	subscriptionID SubscriptionID,
	regionID uint64,
	state *regionFeedState,
) *regionFeedState {
	t.mu.Lock()
	defer t.mu.Unlock()

	regions := t.regionsBySubscription[subscriptionID]
	if regions == nil {
		regions = make(trackedRegionStates)
		t.regionsBySubscription[subscriptionID] = regions
	}
	oldState := regions[regionID]
	regions[regionID] = state
	return oldState
}

// RemoveIf removes a region only when it is still tracked by the expected
// state. It prevents delayed events for an old state from removing its
// replacement.
func (t *regionTracker) RemoveIf(
	subscriptionID SubscriptionID,
	regionID uint64,
	expected *regionFeedState,
) bool {
	if expected == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if regions, ok := t.regionsBySubscription[subscriptionID]; ok {
		if regions[regionID] != expected {
			return false
		}
		delete(regions, regionID)
		if len(regions) == 0 {
			delete(t.regionsBySubscription, subscriptionID)
		}
		return true
	}
	return false
}

// TakeSubscription removes and returns all states tracked by a subscription.
func (t *regionTracker) TakeSubscription(subscriptionID SubscriptionID) []*regionFeedState {
	t.mu.Lock()
	regions := t.regionsBySubscription[subscriptionID]
	delete(t.regionsBySubscription, subscriptionID)
	t.mu.Unlock()

	return collectTrackedRegionStates(regions)
}

// Drain removes and returns all tracked states grouped by subscription.
func (t *regionTracker) Drain() map[SubscriptionID][]*regionFeedState {
	t.mu.Lock()
	regionsBySubscription := t.regionsBySubscription
	t.regionsBySubscription = make(map[SubscriptionID]trackedRegionStates)
	t.mu.Unlock()

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
