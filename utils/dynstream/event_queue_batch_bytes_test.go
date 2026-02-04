package dynstream

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEventQueueBatchBytesStopsBeforeExceedingLimit(t *testing.T) {
	handler := &mockHandler{}
	option := Option{
		BatchCount: 10,
		BatchBytes: 100,
	}

	q := newEventQueue[int, string, *mockEvent, any, *mockHandler](option, handler)
	pi := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "path1", nil)
	q.initPath(pi)

	appendEvent := func(id int, size int) {
		q.appendEvent(eventWrap[int, string, *mockEvent, any, *mockHandler]{
			event:     &mockEvent{id: id, path: "path1"},
			pathInfo:  pi,
			eventSize: size,
			eventType: EventType{DataGroup: 1, Property: BatchableData},
		})
	}

	appendEvent(1, 60)
	appendEvent(2, 60)

	buf := make([]*mockEvent, 0, option.BatchCount)
	buf, gotPath, gotBytes := q.popEvents(buf)
	require.Equal(t, pi, gotPath)
	require.Len(t, buf, 1)
	require.Equal(t, 60, gotBytes)

	buf = buf[:0]
	buf, gotPath, gotBytes = q.popEvents(buf)
	require.Equal(t, pi, gotPath)
	require.Len(t, buf, 1)
	require.Equal(t, 60, gotBytes)
}

func TestEventQueueBatchBytesAllowsFirstEventLargerThanLimit(t *testing.T) {
	handler := &mockHandler{}
	option := Option{
		BatchCount: 10,
		BatchBytes: 50,
	}

	q := newEventQueue[int, string, *mockEvent, any, *mockHandler](option, handler)
	pi := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "path1", nil)
	q.initPath(pi)

	appendEvent := func(id int, size int) {
		q.appendEvent(eventWrap[int, string, *mockEvent, any, *mockHandler]{
			event:     &mockEvent{id: id, path: "path1"},
			pathInfo:  pi,
			eventSize: size,
			eventType: EventType{DataGroup: 1, Property: BatchableData},
		})
	}

	appendEvent(1, 100)
	appendEvent(2, 10)

	buf := make([]*mockEvent, 0, option.BatchCount)
	buf, gotPath, gotBytes := q.popEvents(buf)
	require.Equal(t, pi, gotPath)
	require.Len(t, buf, 1)
	require.Equal(t, 100, gotBytes)

	buf = buf[:0]
	buf, gotPath, gotBytes = q.popEvents(buf)
	require.Equal(t, pi, gotPath)
	require.Len(t, buf, 1)
	require.Equal(t, 10, gotBytes)
}
