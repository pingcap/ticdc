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

package dynstream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestBatchConfigRegistry() *areaBatchConfigRegistry[int] {
	return newAreaBatchConfigRegistry[int](newDefaultBatchConfig())
}

func newTestBatchConfigRegistryWithDefault(cfg batchConfig) *areaBatchConfigRegistry[int] {
	return newAreaBatchConfigRegistry[int](cfg)
}

func TestNewEventQueue(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler, newTestBatchConfigRegistry())

	require.NotNil(t, eq.eventBlockAlloc)
	require.NotNil(t, eq.signalQueue)
	require.NotNil(t, eq.totalPendingLength)
	require.Equal(t, &handler, eq.handler)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestAppendAndPopSingleEvent(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler, newTestBatchConfigRegistry())
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil)
	eq.initPath(path)

	event := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		pathInfo: path,
		event:    &mockEvent{value: 1},
		eventType: EventType{
			DataGroup: 1,
			Property:  BatchableData,
		},
	}
	eq.appendEvent(event)

	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	events, popPath, _, _ := eq.popEvents(b)
	require.Equal(t, 1, len(events))
	require.Equal(t, mockEvent{value: 1}, *events[0])
	require.Equal(t, path, popPath)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestBlockAndWakePath(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler, newTestBatchConfigRegistry())
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil)
	eq.initPath(path)

	event := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		pathInfo: path,
		event:    &mockEvent{value: 1},
		eventType: EventType{
			DataGroup: 1,
			Property:  BatchableData,
		},
	}
	eq.appendEvent(event)

	eq.blockPath(path)

	events, _, _, _ := eq.popEvents(b)
	require.Equal(t, 0, len(events))
	require.Equal(t, int64(0), eq.totalPendingLength.Load())

	eq.wakePath(path)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	events, popPath, _, _ := eq.popEvents(b)
	require.Equal(t, 1, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, path, popPath)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestBatchEvents(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler, newTestBatchConfigRegistryWithDefault(NewBatchConfig(3, 0)))
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil)
	eq.initPath(path)

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

	events, _, _, _ := eq.popEvents(b)
	require.Equal(t, 3, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, &mockEvent{value: 2}, events[1])
	require.Equal(t, &mockEvent{value: 3}, events[2])
	require.Equal(t, int64(2), eq.totalPendingLength.Load())
}

func TestBatchableAndNonBatchableEvents(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler, newTestBatchConfigRegistryWithDefault(NewBatchConfig(3, 0)))
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil)
	eq.initPath(path)

	event1 := eventWrap[int, string, *mockEvent, any, *mockHandler]{
		pathInfo: path,
		event:    &mockEvent{value: 1},
		eventType: EventType{
			DataGroup: 1,
			Property:  NonBatchable,
		},
	}
	eq.appendEvent(event1)

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

	events, _, _, _ := eq.popEvents(b)
	require.Equal(t, 1, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, int64(9), eq.totalPendingLength.Load())

	events, _, _, _ = eq.popEvents(b)
	require.Equal(t, 2, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, &mockEvent{value: 2}, events[1])
	require.Equal(t, int64(7), eq.totalPendingLength.Load())

	events, _, _, _ = eq.popEvents(b)
	require.Equal(t, 1, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, int64(6), eq.totalPendingLength.Load())

	events, _, _, _ = eq.popEvents(b)
	require.Equal(t, 1, len(events))
	require.Equal(t, &mockEvent{value: 2}, events[0])
	require.Equal(t, int64(5), eq.totalPendingLength.Load())

	events, _, _, _ = eq.popEvents(b)
	require.Equal(t, 3, len(events))
	require.Equal(t, &mockEvent{value: 1}, events[0])
	require.Equal(t, &mockEvent{value: 2}, events[1])
	require.Equal(t, &mockEvent{value: 3}, events[2])
	require.Equal(t, int64(2), eq.totalPendingLength.Load())

	events, _, _, _ = eq.popEvents(b)
	require.Equal(t, 2, len(events))
	require.Equal(t, &mockEvent{value: 4}, events[0])
	require.Equal(t, &mockEvent{value: 5}, events[1])
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestRemovePath(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler, newTestBatchConfigRegistry())
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil)
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

	path.removed.Store(true)
	events, _, _, _ := eq.popEvents(b)
	require.Equal(t, 0, len(events))
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestAreaBatchCount(t *testing.T) {
	handler := mockHandler{}
	registry := newAreaBatchConfigRegistry[int](NewBatchConfig(10, 0))
	eq := newEventQueue(&handler, registry)

	path1 := newPathInfo[int, string, *mockEvent, any, *mockHandler](1, "test", "path1", nil)
	path2 := newPathInfo[int, string, *mockEvent, any, *mockHandler](2, "test", "path2", nil)
	eq.initPath(path1)
	eq.initPath(path2)

	registry.onAddPath(1, batchConfig{softCount: 1})
	registry.onAddPath(2, batchConfig{})

	appendEvent := func(path *pathInfo[int, string, *mockEvent, any, *mockHandler], value int) {
		eq.appendEvent(eventWrap[int, string, *mockEvent, any, *mockHandler]{
			pathInfo: path,
			event:    &mockEvent{value: value},
			eventType: EventType{
				DataGroup: 1,
				Property:  BatchableData,
			},
		})
	}

	appendEvent(path1, 1)
	appendEvent(path1, 2)
	appendEvent(path2, 3)
	appendEvent(path2, 4)

	b := newDefaultBatcher[*mockEvent]()

	events, path, _, _ := eq.popEvents(b)
	require.Equal(t, path1, path)
	require.Len(t, events, 1)

	events, path, _, _ = eq.popEvents(b)
	require.Equal(t, path1, path)
	require.Len(t, events, 1)

	events, path, _, _ = eq.popEvents(b)
	require.Equal(t, path2, path)
	require.Len(t, events, 2)
}

func TestPopEventsReturnsBatchResidenceDuration(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler, newTestBatchConfigRegistry())
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil)
	eq.initPath(path)

	eq.appendEvent(eventWrap[int, string, *mockEvent, any, *mockHandler]{
		pathInfo:  path,
		event:     &mockEvent{value: 1},
		queueTime: time.Now(),
		eventType: EventType{
			DataGroup: 1,
			Property:  BatchableData,
		},
	})

	time.Sleep(20 * time.Millisecond)

	events, popPath, _, batchDuration := eq.popEvents(b)
	require.Len(t, events, 1)
	require.Equal(t, path, popPath)
	require.GreaterOrEqual(t, batchDuration, 15*time.Millisecond)
}
