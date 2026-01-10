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

package node

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLivenessStateTransitions(t *testing.T) {
	t.Parallel()

	t.Run("Alive to Draining", func(t *testing.T) {
		var l Liveness
		l.Store(LivenessCaptureAlive)

		// Should succeed: Alive -> Draining
		ok := l.StoreDraining()
		require.True(t, ok)
		require.Equal(t, LivenessCaptureDraining, l.Load())
	})

	t.Run("Draining to Stopping", func(t *testing.T) {
		var l Liveness
		l.Store(LivenessCaptureDraining)

		// Should succeed: Draining -> Stopping
		ok := l.DrainComplete()
		require.True(t, ok)
		require.Equal(t, LivenessCaptureStopping, l.Load())
	})

	t.Run("Cannot transition from Stopping to Draining", func(t *testing.T) {
		var l Liveness
		l.Store(LivenessCaptureStopping)

		// Should fail: Stopping -> Draining is not allowed
		ok := l.StoreDraining()
		require.False(t, ok)
		require.Equal(t, LivenessCaptureStopping, l.Load())
	})

	t.Run("Cannot transition from Draining to Draining", func(t *testing.T) {
		var l Liveness
		l.Store(LivenessCaptureDraining)

		// Should fail: already Draining
		ok := l.StoreDraining()
		require.False(t, ok)
		require.Equal(t, LivenessCaptureDraining, l.Load())
	})

	t.Run("Cannot DrainComplete from Alive", func(t *testing.T) {
		var l Liveness
		l.Store(LivenessCaptureAlive)

		// Should fail: must be Draining first
		ok := l.DrainComplete()
		require.False(t, ok)
		require.Equal(t, LivenessCaptureAlive, l.Load())
	})

	t.Run("Cannot DrainComplete from Stopping", func(t *testing.T) {
		var l Liveness
		l.Store(LivenessCaptureStopping)

		// Should fail: already Stopping
		ok := l.DrainComplete()
		require.False(t, ok)
		require.Equal(t, LivenessCaptureStopping, l.Load())
	})
}

func TestIsSchedulable(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		liveness    Liveness
		schedulable bool
	}{
		{
			name:        "Alive is schedulable",
			liveness:    LivenessCaptureAlive,
			schedulable: true,
		},
		{
			name:        "Draining is not schedulable",
			liveness:    LivenessCaptureDraining,
			schedulable: false,
		},
		{
			name:        "Stopping is not schedulable",
			liveness:    LivenessCaptureStopping,
			schedulable: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var l Liveness
			l.Store(tc.liveness)
			require.Equal(t, tc.schedulable, l.IsSchedulable())
		})
	}
}

func TestLivenessConcurrentAccess(t *testing.T) {
	t.Parallel()

	var l Liveness
	l.Store(LivenessCaptureAlive)

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	// Multiple goroutines try to transition to Draining
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.StoreDraining() {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Only one should succeed
	require.Equal(t, 1, successCount)
	require.Equal(t, LivenessCaptureDraining, l.Load())
}
