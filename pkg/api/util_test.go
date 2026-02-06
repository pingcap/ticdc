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

package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLivenessStoreMonotonic(t *testing.T) {
	var l Liveness
	require.Equal(t, LivenessCaptureAlive, l.Load())

	// Alive -> Draining.
	require.True(t, l.Store(LivenessCaptureDraining))
	require.Equal(t, LivenessCaptureDraining, l.Load())

	// Idempotent store.
	require.True(t, l.Store(LivenessCaptureDraining))
	require.Equal(t, LivenessCaptureDraining, l.Load())

	// Reject downgrade.
	require.False(t, l.Store(LivenessCaptureAlive))
	require.Equal(t, LivenessCaptureDraining, l.Load())

	// Draining -> Stopping.
	require.True(t, l.Store(LivenessCaptureStopping))
	require.Equal(t, LivenessCaptureStopping, l.Load())

	// Idempotent store.
	require.True(t, l.Store(LivenessCaptureStopping))
	require.Equal(t, LivenessCaptureStopping, l.Load())

	// Reject downgrade.
	require.False(t, l.Store(LivenessCaptureDraining))
	require.Equal(t, LivenessCaptureStopping, l.Load())
}

func TestLivenessStoreRejectSkip(t *testing.T) {
	var l Liveness
	require.Equal(t, LivenessCaptureAlive, l.Load())

	// Reject skipping Draining.
	require.False(t, l.Store(LivenessCaptureStopping))
	require.Equal(t, LivenessCaptureAlive, l.Load())
}
