// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	require.Equal(t, "Alive", l.String())

	require.True(t, l.Store(LivenessCaptureDraining))
	require.Equal(t, LivenessCaptureDraining, l.Load())
	require.Equal(t, "Draining", l.String())

	// Reject downgrade and no-op writes.
	require.False(t, l.Store(LivenessCaptureAlive))
	require.False(t, l.Store(LivenessCaptureDraining))

	require.True(t, l.Store(LivenessCaptureStopping))
	require.Equal(t, LivenessCaptureStopping, l.Load())
	require.Equal(t, "Stopping", l.String())

	require.False(t, l.Store(LivenessCaptureDraining))
	require.False(t, l.Store(LivenessCaptureStopping))
}

func TestLivenessStoreDisallowSkip(t *testing.T) {
	var l Liveness
	require.False(t, l.Store(LivenessCaptureStopping))
	require.Equal(t, LivenessCaptureAlive, l.Load())
}
