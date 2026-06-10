// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestBatcherSetLimit verifies switching area batch config clears buffered
// state before the batcher is reused.
func TestBatcherSetLimit(t *testing.T) {
	b := newBatcher[*mockEvent](newBatchConfig(4, 0))
	b.addEvent(&mockEvent{value: 1}, 16)
	require.Equal(t, 1, len(b.buf))
	require.Equal(t, 16, b.nBytes)

	b.setLimit(newBatchConfig(8, 64))
	require.Equal(t, 0, len(b.buf))
	require.Equal(t, 0, b.nBytes)
	require.Equal(t, 8, b.config.softCount)
	require.Equal(t, 64, b.config.hardBytes)
}

// TestNewAreaSettingsWithMaxPendingSizeAndBatchConfigNormalizesBatchConfig
// verifies the public constructor keeps the same normalization rules as the
// internal batch config helper.
func TestNewAreaSettingsWithMaxPendingSizeAndBatchConfigNormalizesBatchConfig(t *testing.T) {
	settings := NewAreaSettingsWithMaxPendingSizeAndBatchConfig(
		64*1024*1024, 0, "test", 0, -1,
	)
	require.Equal(t, newBatchConfig(0, -1), settings.batchConfig)
}

// TestNewBatchConfigClampsSoftCount keeps user overrides within the supported
// upper bound before dynstream starts batching.
func TestNewBatchConfigClampsSoftCount(t *testing.T) {
	cfg := newBatchConfig(config.MaxEventCollectorBatchCount+1, 0)
	require.Equal(t, config.MaxEventCollectorBatchCount, cfg.softCount)
	require.Equal(t, config.MaxEventCollectorBatchCount*countCapMultiple, cfg.hardCount)
	require.Equal(t, 0, cfg.hardBytes)
}

// TestBatchByBytes verifies byte-based batching flushes at the first event that
// would push the accumulated batch size over the configured threshold.
func TestBatchByBytes(t *testing.T) {
	handler := mockHandler{}
	registry := newAreaBatchConfigRegistry[int](newDefaultBatchConfig())
	registry.onAddPath(0, newBatchConfig(10, 12))
	eq := newEventQueue(&handler, registry)
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil)
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

	events, _, _, _ := eq.popEvents(b)
	require.Len(t, events, 3)
	require.Equal(t, 1, events[0].value)
	require.Equal(t, 2, events[1].value)
	require.Equal(t, 3, events[2].value)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	events, _, _, _ = eq.popEvents(b)
	require.Len(t, events, 1)
	require.Equal(t, 4, events[0].value)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())

	registry = newAreaBatchConfigRegistry[int](newDefaultBatchConfig())
	registry.onAddPath(0, newBatchConfig(10, 12))
	eq = newEventQueue(&handler, registry)
	path = newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil)
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
	events, _, _, _ = eq.popEvents(b)
	require.Len(t, events, 3)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	events, _, _, _ = eq.popEvents(b)
	require.Len(t, events, 1)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

// TestBatchByHardCount verifies byte-based batching still has a hard event cap
// to prevent unbounded growth when individual events are tiny.
func TestBatchByHardCount(t *testing.T) {
	handler := mockHandler{}
	registry := newAreaBatchConfigRegistry[int](newDefaultBatchConfig())
	registry.onAddPath(0, newBatchConfig(1, 1000))
	eq := newEventQueue(&handler, registry)
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil)
	eq.initPath(path)

	for i := 1; i <= 9; i++ {
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

	events, _, _, _ := eq.popEvents(b)
	require.Len(t, events, countCapMultiple)
	require.Equal(t, int64(9-countCapMultiple), eq.totalPendingLength.Load())

	events, _, _, _ = eq.popEvents(b)
	require.Len(t, events, countCapMultiple)
	require.Equal(t, int64(9-2*countCapMultiple), eq.totalPendingLength.Load())

	events, _, _, _ = eq.popEvents(b)
	require.Len(t, events, 1)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}

// TestBatchByBytesLargeFirstEvent verifies an oversized first event still gets
// delivered and does not consume the following event in the same batch.
func TestBatchByBytesLargeFirstEvent(t *testing.T) {
	handler := mockHandler{}
	registry := newAreaBatchConfigRegistry[int](newDefaultBatchConfig())
	registry.onAddPath(0, newBatchConfig(10, 50))
	eq := newEventQueue(&handler, registry)
	b := newDefaultBatcher[*mockEvent]()

	path := newPathInfo[int, string, *mockEvent, any, *mockHandler](0, "test", "test", nil)
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

	events, _, _, _ := eq.popEvents(b)
	require.Len(t, events, 1)
	require.Equal(t, 1, events[0].value)
	require.Equal(t, int64(1), eq.totalPendingLength.Load())

	events, _, _, _ = eq.popEvents(b)
	require.Len(t, events, 1)
	require.Equal(t, 2, events[0].value)
	require.Equal(t, int64(0), eq.totalPendingLength.Load())
}
