package eventcollector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdaptiveScanWindowAdjust(t *testing.T) {
	t.Parallel()

	w := newAdaptiveScanWindow()
	now := time.Unix(0, 0)
	w.now = func() time.Time { return now }
	w.lastObserveTime = now

	step := func(ratio float64, maxInterval time.Duration) time.Duration {
		now = now.Add(time.Second)
		return w.observe(ratio, maxInterval)
	}

	// Grow after 10 seconds below 50%.
	for range 9 {
		interval := step(0.49, adaptiveScanWindowMax)
		require.Equal(t, 5*time.Second, interval)
	}
	interval := step(0.49, adaptiveScanWindowMax)
	require.Equal(t, 10*time.Second, interval)

	// Shrink after 10 seconds above 80%.
	for range 9 {
		interval = step(0.81, adaptiveScanWindowMax)
		require.Equal(t, 10*time.Second, interval)
	}
	interval = step(0.81, adaptiveScanWindowMax)
	require.Equal(t, 5*time.Second, interval)

	// Above 150% uses the same shrink logic (reset is handled elsewhere).
	for range 9 {
		interval = step(1.51, adaptiveScanWindowMax)
		require.Equal(t, 5*time.Second, interval)
	}
	interval = step(1.51, adaptiveScanWindowMax)
	require.Equal(t, 2500*time.Millisecond, interval)

	// Clamp to provided max interval.
	w.interval = 100 * time.Hour
	interval = step(0.0, 2*time.Second)
	require.Equal(t, 2*time.Second, interval)
}
