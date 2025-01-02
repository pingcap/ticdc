package dynstream

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewEventQueue(t *testing.T) {
	handler := mockHandler{}
	option := Option{BatchCount: 10}

	eq := newEventQueue(option, &handler)

	require.NotNil(t, eq.eventBlockAlloc)
	require.NotNil(t, eq.signalQueue)
	require.NotNil(t, eq.totalPendingLength)
	require.Equal(t, option, eq.option)
	require.Equal(t, &handler, eq.handler)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestAppendAndPopSingleEvent(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(Option{BatchCount: 10}, &handler)

	// create a path
	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", nil)
	eq.initPath(path)

	// append an event
	event := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		pathInfo: path,
		event:    &mockEvent{value: 1},
		eventType: EventType{
			DataGroup: 1,
			Property:  BatchableData,
		},
	}

	eq.appendEvent(event)

	// verify the event is appended
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	// pop the event
	buf := make([]*mockEvent, 0)
	events, popPath := eq.popEvents(buf)

	require.Equal(t, 1, len(events))
	require.Equal(t, mockEvent{value: 1}, *events[0])
	require.Equal(t, path, popPath)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestBlockAndWakePath(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(Option{BatchCount: 10}, &handler)

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", nil)
	eq.initPath(path)

	// append an event
	event := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		pathInfo: path,
		event:    &mockEvent{value: 1},
		eventType: EventType{
			DataGroup: 1,
			Property:  BatchableData,
		},
	}
	eq.appendEvent(event)

	// block the path
	eq.blockPath(path)

	// try to pop the event (should not pop)
	buf := make([]*mockEvent, 0)
	events, _ := eq.popEvents(buf)
	require.Equal(t, 0, len(events))
	require.Equal(t, int64(0), eq.totalPendingLength.Load())

	// wake the path
	eq.wakePath(path)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	// try to pop the event (should pop)
	events, popPath := eq.popEvents(buf)
	require.Equal(t, 1, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, path, popPath)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestBatchEvents(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(Option{BatchCount: 3}, &handler)

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", nil)
	eq.initPath(path)

	// append multiple events with the same DataGroup
	for i := 1; i <= 5; i++ {
		event := eventWrap[int, string, *mockEvent, any, *mockHandler]{
			pathInfo: path,
			event:    &mockEvent{value: i},
			eventType: EventType{
				DataGroup: 1,
				Property:  BatchableData,
			},
		}
		eq.appendEvent(event)
	}

	// verify the batch pop
	buf := make([]*mockEvent, 0)
	events, _ := eq.popEvents(buf)

	// since BatchCount = 3, only the first 3 events should be popped
	require.Equal(t, 3, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, &mockEvent{value: 2}, events[1])
	require.Equal(t, &mockEvent{value: 3}, events[2])

	// verify the remaining event count
	require.Equal(t, int64(2), eq.totalPendingLength.Load())
}

func TestBatchableAndNonBatchableEvents(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(Option{BatchCount: 3}, &handler)

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", nil)
	eq.initPath(path)

	// append a non-batchable event
	event1 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		pathInfo: path,
		event:    &mockEvent{value: 1},
		eventType: EventType{
			DataGroup: 1,
			Property:  NonBatchable,
		},
	}
	eq.appendEvent(event1)

	// append 2 batchable events
	for i := 1; i <= 2; i++ {
		e := eventWrap[int, string, *mockEvent, any, *mockHandler]{
			pathInfo: path,
			event:    &mockEvent{value: i},
			eventType: EventType{
				DataGroup: 1,
				Property:  BatchableData,
			},
		}
		eq.appendEvent(e)
	}

	// add 2 non-batchable events
	for i := 1; i <= 2; i++ {
		e := eventWrap[int, string, *mockEvent, any, *mockHandler]{
			pathInfo: path,
			event:    &mockEvent{value: i},
			eventType: EventType{
				DataGroup: 1,
				Property:  NonBatchable,
			},
		}
		eq.appendEvent(e)
	}

	// append 5 batchable events
	for i := 1; i <= 5; i++ {
		e := eventWrap[int, string, *mockEvent, any, *mockHandler]{
			pathInfo: path,
			event:    &mockEvent{value: i},
			eventType: EventType{
				DataGroup: 1,
				Property:  BatchableData,
			},
		}
		eq.appendEvent(e)
	}

	// case 1: pop the first non-batchable event
	buf := make([]*mockEvent, 0)
	events, _ := eq.popEvents(buf)
	require.Equal(t, 1, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, int64(9), eq.totalPendingLength.Load())

	// case 2: pop the first 2 batchable event
	buf = make([]*mockEvent, 0)
	events, _ = eq.popEvents(buf)
	require.Equal(t, 2, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, &mockEvent{value: 2}, events[1])
	require.Equal(t, int64(7), eq.totalPendingLength.Load())

	// case 3: pop a non-batchable event
	buf = make([]*mockEvent, 0)
	events, _ = eq.popEvents(buf)
	require.Equal(t, 1, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, int64(6), eq.totalPendingLength.Load())

	// case 4: pop the second non-batchable event
	buf = make([]*mockEvent, 0)
	events, _ = eq.popEvents(buf)
	require.Equal(t, 1, len(events))
	require.Equal(t, &mockEvent{value: 2}, events[0])
	require.Equal(t, int64(5), eq.totalPendingLength.Load())

	// case 5: pop the first 3 batchable events
	buf = make([]*mockEvent, 0)
	events, _ = eq.popEvents(buf)
	require.Equal(t, 3, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, &mockEvent{value: 2}, events[1])
	require.Equal(t, &mockEvent{value: 3}, events[2])
	require.Equal(t, int64(2), eq.totalPendingLength.Load())

	// case 6: pop the remaining 2 batchable events
	buf = make([]*mockEvent, 0)
	events, _ = eq.popEvents(buf)
	require.Equal(t, 2, len(events))
	require.Equal(t, &mockEvent{value: 4}, events[0])
	require.Equal(t, &mockEvent{value: 5}, events[1])
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestRemovePath(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(Option{BatchCount: 3}, &handler)

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", nil)
	eq.initPath(path)

	e := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		pathInfo: path,
		event:    &mockEvent{value: 1},
		eventType: EventType{
			DataGroup: 1,
			Property:  BatchableData,
		},
	}
	eq.appendEvent(e)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	path.removed = true
	buf := make([]*mockEvent, 0)
	events, _ := eq.popEvents(buf)
	require.Equal(t, 0, len(events))
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}
