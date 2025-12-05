package maintainer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPendingScheduleEventMapDeduplicate(t *testing.T) {
	m := newPendingScheduleEventMap()
	event1 := &BarrierEvent{commitTs: 10, isSyncPoint: false}
	event2 := &BarrierEvent{commitTs: 10, isSyncPoint: false}
	require.NotSame(t, event1, event2)

	m.add(event1)
	m.add(event2)

	require.Equal(t, 1, m.queue.Len())

	ready, candidate := m.popIfHead(event1)
	require.True(t, ready)
	require.Equal(t, event1, candidate)

	ready, candidate = m.popIfHead(event1)
	require.False(t, ready)
	require.Nil(t, candidate)
}

func TestPendingScheduleEventMapPopIfHead(t *testing.T) {
	m := newPendingScheduleEventMap()
	event1 := &BarrierEvent{commitTs: 10, isSyncPoint: false}
	event2 := &BarrierEvent{commitTs: 20, isSyncPoint: false}

	m.add(event1)
	m.add(event2)

	ready, candidate := m.popIfHead(event2)
	require.False(t, ready)
	require.Equal(t, event1, candidate)

	ready, candidate = m.popIfHead(event1)
	require.True(t, ready)
	require.Equal(t, event1, candidate)

	ready, candidate = m.popIfHead(event2)
	require.True(t, ready)
	require.Equal(t, event2, candidate)
}
