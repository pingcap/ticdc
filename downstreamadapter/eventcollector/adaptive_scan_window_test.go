package eventcollector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdaptiveScanWindowAdjust(t *testing.T) {
	t.Parallel()

	w := newAdaptiveScanWindow()
	maxInterval := 30 * time.Minute

	// Grow after 30 seconds below 50%.
	for range 29 {
		interval, needReset := w.observe(0.49, maxInterval)
		require.False(t, needReset)
		require.Equal(t, 5*time.Second, interval)
	}
	interval, needReset := w.observe(0.49, maxInterval)
	require.False(t, needReset)
	require.Equal(t, 10*time.Second, interval)

	// Shrink after 30 seconds above 110%.
	for range 29 {
		interval, needReset = w.observe(1.11, maxInterval)
		require.False(t, needReset)
		require.Equal(t, 10*time.Second, interval)
	}
	interval, needReset = w.observe(1.11, maxInterval)
	require.False(t, needReset)
	require.Equal(t, 5*time.Second, interval)

	// Reset after 30 seconds above 150%.
	for range 29 {
		interval, needReset = w.observe(1.51, maxInterval)
		require.False(t, needReset)
	}
	interval, needReset = w.observe(1.51, maxInterval)
	require.True(t, needReset)
	require.Equal(t, 1*time.Second, interval)

	// Clamp to max interval.
	for range 120 {
		interval, needReset = w.observe(0.0, 2*time.Second)
		require.False(t, needReset)
	}
	require.LessOrEqual(t, interval, 2*time.Second)
	require.GreaterOrEqual(t, interval, 1*time.Second)
}

