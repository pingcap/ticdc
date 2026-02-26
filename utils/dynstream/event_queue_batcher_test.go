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

func TestBatchEventsBySize(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler)

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil, newBatcher[*mockEvent](NewBatchConfig(10, 12)))
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

	events, _, _ := eq.popEvents()
	require.Len(t, events, 3)
	require.Equal(t, 1, events[0].value)
	require.Equal(t, 2, events[1].value)
	require.Equal(t, 3, events[2].value)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	events, _, _ = eq.popEvents()
	require.Len(t, events, 1)
	require.Equal(t, 4, events[0].value)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

func TestBatchEventsBySize_NotExceedHardBytes(t *testing.T) {
	handler := mockHandler{}
	eq := newEventQueue(&handler)

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil, newBatcher[*mockEvent](NewBatchConfig(10, 12)))
	eq.initPath(path)

	for i := 1; i <= 3; i++ {
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

	events, _, _ := eq.popEvents()
	require.Len(t, events, 2)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	events, _, _ = eq.popEvents()
	require.Len(t, events, 1)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}
