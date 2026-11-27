// Copyright 2025 PingCAP, Inc.
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
package maintainer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareEventKey(t *testing.T) {
	testCases := []struct {
		name     string
		a        eventKey
		b        eventKey
		expected int
	}{
		{name: "lower block ts", a: getEventKey(1, false), b: getEventKey(2, false), expected: -1},
		{name: "higher block ts", a: getEventKey(2, false), b: getEventKey(1, false), expected: 1},
		{name: "ddl before sync point", a: getEventKey(1, false), b: getEventKey(1, true), expected: -1},
		{name: "sync point after ddl", a: getEventKey(1, true), b: getEventKey(1, false), expected: 1},
		{name: "equal ddl", a: getEventKey(1, false), b: getEventKey(1, false), expected: 0},
		{name: "equal sync point", a: getEventKey(1, true), b: getEventKey(1, true), expected: 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, compareEventKey(tc.a, tc.b))
		})
	}
}

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
