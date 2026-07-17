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
	"maps"
	"slices"
	"sync"
)

type regionStatesByID map[uint64]*regionFeedState

// regionTracker owns the region states tracked by one region request worker.
type regionTracker struct {
	mu sync.RWMutex

	statesBySubscription map[SubscriptionID]regionStatesByID
}

func newRegionTracker() *regionTracker {
	return &regionTracker{
		statesBySubscription: make(map[SubscriptionID]regionStatesByID),
	}
}

// Get returns the state tracked by a subscription and region.
func (t *regionTracker) Get(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if states, ok := t.statesBySubscription[subscriptionID]; ok {
		return states[regionID]
	}
	return nil
}

// Add records a region state unless the same subscription and region is
// already tracked.
func (t *regionTracker) Add(
	subscriptionID SubscriptionID,
	regionID uint64,
	state *regionFeedState,
) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	states := t.statesBySubscription[subscriptionID]
	if states == nil {
		states = make(regionStatesByID)
		t.statesBySubscription[subscriptionID] = states
	}
	if _, ok := states[regionID]; ok {
		return false
	}
	states[regionID] = state
	return true
}

// RemoveIf removes a region only when it is still tracked by the expected
// state. It prevents delayed events for an old state from removing a newer
// owner.
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

	if states, ok := t.statesBySubscription[subscriptionID]; ok {
		if states[regionID] != expected {
			return false
		}
		delete(states, regionID)
		if len(states) == 0 {
			delete(t.statesBySubscription, subscriptionID)
		}
		return true
	}
	return false
}

// TakeSubscription removes and returns all states tracked by a subscription.
func (t *regionTracker) TakeSubscription(subscriptionID SubscriptionID) []*regionFeedState {
	t.mu.Lock()
	states := t.statesBySubscription[subscriptionID]
	delete(t.statesBySubscription, subscriptionID)
	t.mu.Unlock()

	return slices.Collect(maps.Values(states))
}

// Drain removes and returns all tracked states.
func (t *regionTracker) Drain() []*regionFeedState {
	t.mu.Lock()
	statesBySubscription := t.statesBySubscription
	t.statesBySubscription = make(map[SubscriptionID]regionStatesByID)
	t.mu.Unlock()

	stateCount := 0
	for _, states := range statesBySubscription {
		stateCount += len(states)
	}
	drainedStates := make([]*regionFeedState, 0, stateCount)
	for _, states := range statesBySubscription {
		for _, state := range states {
			drainedStates = append(drainedStates, state)
		}
	}
	return drainedStates
}
