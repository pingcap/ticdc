package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLivenessStoreMonotonic(t *testing.T) {
	var l Liveness
	require.Equal(t, LivenessCaptureAlive, l.Load())

	require.True(t, l.Store(LivenessCaptureDraining))
	require.Equal(t, LivenessCaptureDraining, l.Load())

	require.True(t, l.Store(LivenessCaptureStopping))
	require.Equal(t, LivenessCaptureStopping, l.Load())

	// Reject downgrade and further updates.
	require.False(t, l.Store(LivenessCaptureDraining))
	require.False(t, l.Store(LivenessCaptureAlive))
	require.False(t, l.Store(LivenessCaptureStopping))
}

func TestLivenessStoreDirectToStopping(t *testing.T) {
	var l Liveness
	require.Equal(t, LivenessCaptureAlive, l.Load())

	require.True(t, l.Store(LivenessCaptureStopping))
	require.Equal(t, LivenessCaptureStopping, l.Load())

	// Reject any updates after STOPPING.
	require.False(t, l.Store(LivenessCaptureDraining))
	require.False(t, l.Store(LivenessCaptureAlive))
}
