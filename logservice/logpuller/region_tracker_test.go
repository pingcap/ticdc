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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegionTrackerOperations(t *testing.T) {
	tracker := newRegionTracker()
	state1 := &regionFeedState{}
	state2 := &regionFeedState{}
	state3 := &regionFeedState{}

	require.Nil(t, tracker.Get(1, 1))
	require.Nil(t, tracker.Replace(1, 1, state1))
	require.Same(t, state1, tracker.Get(1, 1))
	require.Nil(t, tracker.Replace(1, 2, state2))
	require.Nil(t, tracker.Replace(2, 3, state3))

	require.ElementsMatch(t, []*regionFeedState{state1, state2}, tracker.TakeSubscription(1))
	require.Nil(t, tracker.Get(1, 1))
	require.Nil(t, tracker.Get(1, 2))
	require.Empty(t, tracker.TakeSubscription(1))

	drained := tracker.Drain()
	require.Equal(t, map[SubscriptionID][]*regionFeedState{
		2: {state3},
	}, drained)
	require.Nil(t, tracker.Get(2, 3))
	require.Empty(t, tracker.Drain())
}

func TestRegionTrackerRemoveIfPreservesReplacement(t *testing.T) {
	tracker := newRegionTracker()
	oldState := &regionFeedState{}
	newState := &regionFeedState{}

	require.Nil(t, tracker.Replace(1, 1, oldState))
	require.Same(t, oldState, tracker.Replace(1, 1, newState))

	require.False(t, tracker.RemoveIf(1, 1, oldState))
	require.Same(t, newState, tracker.Get(1, 1))
	require.True(t, tracker.RemoveIf(1, 1, newState))
	require.Nil(t, tracker.Get(1, 1))
	require.False(t, tracker.RemoveIf(1, 1, newState))
}
