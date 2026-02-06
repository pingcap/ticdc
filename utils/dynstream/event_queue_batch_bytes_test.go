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

func TestEventQueueBatchBytesFlushesAfterReachingLimit(t *testing.T) {
	handler := &mockHandler{}
	option := Option{
		BatchCount: 10,
		BatchBytes: 100,
	}

	q := newEventQueue[int, string, *mockEvent, any, *mockHandler](option, handler, nil)
	pi := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "path1", nil)
	q.initPath(pi)

	batchConfig := newBatchConfig(option.BatchCount, option.BatchBytes)
	b := newBatcher[*mockEvent](batchConfig, option.BatchCount)

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

	events, gotPath, gotBytes := q.popEvents(&b)
	require.Equal(t, pi, gotPath)
	require.Len(t, events, 2)
	require.Equal(t, uint64(120), gotBytes)
}

func TestEventQueueBatchBytesAllowsFirstEventLargerThanLimit(t *testing.T) {
	handler := &mockHandler{}
	option := Option{
		BatchCount: 10,
		BatchBytes: 50,
	}

	q := newEventQueue[int, string, *mockEvent, any, *mockHandler](option, handler, nil)
	pi := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "path1", nil)
	q.initPath(pi)

	batchConfig := newBatchConfig(option.BatchCount, option.BatchBytes)
	b := newBatcher[*mockEvent](batchConfig, option.BatchCount)

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

	events, gotPath, gotBytes := q.popEvents(&b)
	require.Equal(t, pi, gotPath)
	require.Len(t, events, 1)
	require.Equal(t, uint64(100), gotBytes)

	b.reset()
	events, gotPath, gotBytes = q.popEvents(&b)
	require.Equal(t, pi, gotPath)
	require.Len(t, events, 1)
	require.Equal(t, uint64(10), gotBytes)
}
