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

	"github.com/stretchr/testify/require"
)

func TestBatcherSetLimit(t *testing.T) {
	b := newBatcher[*mockEvent](NewBatchConfig(4, 0))
	b.addEvent(&mockEvent{value: 1}, 16)
	require.Equal(t, 1, len(b.buf))
	require.Equal(t, 16, b.nBytes)

	b.setLimit(NewBatchConfig(8, 64))
	require.Equal(t, 0, len(b.buf))
	require.Equal(t, 0, b.nBytes)
	require.Equal(t, 8, b.config.softCount)
	require.Equal(t, 64, b.config.hardBytes)
}

func TestBatchByBytes(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler)
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil, NewBatchConfig(10, 12))
	eq.initPath(path)

	for i := 1; i <= 4; i++ {
		eq.appendEvent(eventWrap[int, string, *mockEvent, any, *mockHandler]{
			pathInfo:  path,
			event:     &mockEvent{value: i},
			eventSize: 4,
			eventType: EventType{
				DataGroup: 1,
				Property:  BatchableData,
			},
		})
	}

	events, _, _ := eq.popEvents(b)
	require.Len(t, events, 3)
	require.Equal(t, 1, events[0].value)
	require.Equal(t, 2, events[1].value)
	require.Equal(t, 3, events[2].value)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	events, _, _ = eq.popEvents(b)
	require.Len(t, events, 1)
	require.Equal(t, 4, events[0].value)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())

	eq = newEventQueue(&handler)
	path = newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil, NewBatchConfig(10, 12))
	eq.initPath(path)
	for i := 1; i <= 4; i++ {
		eq.appendEvent(eventWrap[int, string, *mockEvent, any, *mockHandler]{
			pathInfo:  path,
			event:     &mockEvent{value: i},
			eventSize: 5,
			eventType: EventType{
				DataGroup: 1,
				Property:  BatchableData,
			},
		})
	}
	events, _, _ = eq.popEvents(b)
	require.Len(t, events, 3)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	events, _, _ = eq.popEvents(b)
	require.Len(t, events, 1)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestBatchByHardCount(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler)
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil, NewBatchConfig(3, 100))
	eq.initPath(path)

	for i := 1; i <= 7; i++ {
		eq.appendEvent(eventWrap[int, string, *mockEvent, any, *mockHandler]{
			pathInfo:  path,
			event:     &mockEvent{value: i},
			eventSize: 10,
			eventType: EventType{
				DataGroup: 1,
				Property:  BatchableData,
			},
		})
	}

	events, _, _ := eq.popEvents(b)
	require.Len(t, events, 6)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	events, _, _ = eq.popEvents(b)
	require.Len(t, events, 1)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestBatchByBytesLargeFirstEvent(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler)
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil, NewBatchConfig(10, 50))
	eq.initPath(path)

	eq.appendEvent(eventWrap[int, string, *mockEvent, any, *mockHandler]{
		pathInfo:  path,
		event:     &mockEvent{value: 1},
		eventSize: 100,
		eventType: EventType{
			DataGroup: 1,
			Property:  BatchableData,
		},
	})
	eq.appendEvent(eventWrap[int, string, *mockEvent, any, *mockHandler]{
		pathInfo:  path,
		event:     &mockEvent{value: 2},
		eventSize: 10,
		eventType: EventType{
			DataGroup: 1,
			Property:  BatchableData,
		},
	})

	events, _, _ := eq.popEvents(b)
	require.Len(t, events, 1)
	require.Equal(t, 1, events[0].value)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	events, _, _ = eq.popEvents(b)
	require.Len(t, events, 1)
	require.Equal(t, 2, events[0].value)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}
