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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLivenessConstants(t *testing.T) {
	// Verify the constant values are as expected
	require.Equal(t, Liveness(0), LivenessCaptureAlive)
	require.Equal(t, Liveness(1), LivenessCaptureStopping)
	require.Equal(t, Liveness(2), LivenessCaptureDraining)
}

func TestLivenessLoadStore(t *testing.T) {
	var l Liveness

	// Default value should be Alive (0)
	require.Equal(t, LivenessCaptureAlive, l.Load())

	// Test Store and Load
	l.Store(LivenessCaptureDraining)
	require.Equal(t, LivenessCaptureDraining, l.Load())

	l.Store(LivenessCaptureStopping)
	require.Equal(t, LivenessCaptureStopping, l.Load())

	l.Store(LivenessCaptureAlive)
	require.Equal(t, LivenessCaptureAlive, l.Load())
}

func TestLivenessIsSchedulable(t *testing.T) {
	var l Liveness

	// Alive node is schedulable
	l.Store(LivenessCaptureAlive)
	require.True(t, l.IsSchedulable())

	// Draining node is not schedulable
	l.Store(LivenessCaptureDraining)
	require.False(t, l.IsSchedulable())

	// Stopping node is not schedulable
	l.Store(LivenessCaptureStopping)
	require.False(t, l.IsSchedulable())
}

func TestLivenessStoreDraining(t *testing.T) {
	var l Liveness

	// Can transition from Alive to Draining
	l.Store(LivenessCaptureAlive)
	require.True(t, l.StoreDraining())
	require.Equal(t, LivenessCaptureDraining, l.Load())

	// Cannot transition from Draining to Draining (already draining)
	require.False(t, l.StoreDraining())
	require.Equal(t, LivenessCaptureDraining, l.Load())

	// Cannot transition from Stopping to Draining
	l.Store(LivenessCaptureStopping)
	require.False(t, l.StoreDraining())
	require.Equal(t, LivenessCaptureStopping, l.Load())
}

func TestLivenessDrainComplete(t *testing.T) {
	var l Liveness

	// Cannot transition from Alive to Stopping via DrainComplete
	l.Store(LivenessCaptureAlive)
	require.False(t, l.DrainComplete())
	require.Equal(t, LivenessCaptureAlive, l.Load())

	// Can transition from Draining to Stopping
	l.Store(LivenessCaptureDraining)
	require.True(t, l.DrainComplete())
	require.Equal(t, LivenessCaptureStopping, l.Load())

	// Cannot transition from Stopping to Stopping (already stopping)
	require.False(t, l.DrainComplete())
	require.Equal(t, LivenessCaptureStopping, l.Load())
}

func TestLivenessStateTransitionFlow(t *testing.T) {
	var l Liveness

	// Test the expected state transition flow: Alive -> Draining -> Stopping
	require.Equal(t, LivenessCaptureAlive, l.Load())
	require.True(t, l.IsSchedulable())

	// Transition to Draining
	require.True(t, l.StoreDraining())
	require.Equal(t, LivenessCaptureDraining, l.Load())
	require.False(t, l.IsSchedulable())

	// Transition to Stopping
	require.True(t, l.DrainComplete())
	require.Equal(t, LivenessCaptureStopping, l.Load())
	require.False(t, l.IsSchedulable())
}
