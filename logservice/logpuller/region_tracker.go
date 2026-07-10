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

// Replace records a region state and returns the state previously tracked by
// the same subscription and region, if any.
func (t *regionTracker) Replace(
	subscriptionID SubscriptionID,
	regionID uint64,
	state *regionFeedState,
) *regionFeedState {
	t.mu.Lock()
	defer t.mu.Unlock()

	states := t.statesBySubscription[subscriptionID]
	if states == nil {
		states = make(regionStatesByID)
		t.statesBySubscription[subscriptionID] = states
	}
	oldState := states[regionID]
	states[regionID] = state
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

// Drain removes and returns all tracked states grouped by subscription.
func (t *regionTracker) Drain() map[SubscriptionID][]*regionFeedState {
	t.mu.Lock()
	statesBySubscription := t.statesBySubscription
	t.statesBySubscription = make(map[SubscriptionID]regionStatesByID)
	t.mu.Unlock()

	drainedStates := make(map[SubscriptionID][]*regionFeedState, len(statesBySubscription))
	for subID, states := range statesBySubscription {
		drainedStates[subID] = slices.Collect(maps.Values(states))
	}
	return drainedStates
}
