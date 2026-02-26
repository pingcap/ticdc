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

// Test per-area batch config is applied in eventQueue.popEvents.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEventQueuePopEventsUsesPerAreaBatchCount(t *testing.T) {
	handler := &mockHandler{}
	option := Option{
		BatchCount: 10,
		BatchBytes: 0,
	}
	defaultConfig := newBatchConfig(option.BatchCount, option.BatchBytes)
	configStore := newAreaBatchConfigStore[int](defaultConfig)

	q := newEventQueue[int, string, *mockEvent, any, *mockHandler](option, handler, configStore)
	pi1 := newPathInfo[int, string, *mockEvent, any, *mockHandler](1, "test", "path1", nil)
	pi2 := newPathInfo[int, string, *mockEvent, any, *mockHandler](2, "test", "path2", nil)
	q.initPath(pi1)
	q.initPath(pi2)

	// Simulate AddPath effects for store: the area must exist before overrides apply.
	configStore.onAddPath(1)
	configStore.onAddPath(2)
	configStore.setAreaBatchConfig(1, 1, 0)

	appendEvent := func(pi *pathInfo[int, string, *mockEvent, any, *mockHandler], id int) {
		q.appendEvent(eventWrap[int, string, *mockEvent, any, *mockHandler]{
			event:     &mockEvent{id: id, path: pi.path},
			pathInfo:  pi,
			eventSize: 1,
			eventType: EventType{DataGroup: 1, Property: BatchableData},
		})
	}

	appendEvent(pi1, 1)
	appendEvent(pi1, 2)
	appendEvent(pi2, 3)
	appendEvent(pi2, 4)

	b := newBatcher[*mockEvent](defaultConfig, option.BatchCount)

	events, gotPath, _ := q.popEvents(&b)
	require.Equal(t, pi1, gotPath)
	require.Len(t, events, 1)
	b.reset()

	events, gotPath, _ = q.popEvents(&b)
	require.Equal(t, pi1, gotPath)
	require.Len(t, events, 1)
	b.reset()

	events, gotPath, _ = q.popEvents(&b)
	require.Equal(t, pi2, gotPath)
	require.Len(t, events, 2)
}
