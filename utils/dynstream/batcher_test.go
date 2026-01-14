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

type testDest struct{}

type testHandler struct{}

func (h *testHandler) Path(event int) string                    { return "" }
func (h *testHandler) Handle(dest testDest, events ...int) bool { return false }
func (h *testHandler) GetSize(event int) int                    { return 0 }
func (h *testHandler) IsPaused(event int) bool                  { return false }
func (h *testHandler) GetArea(path string, dest testDest) int   { return 0 }
func (h *testHandler) GetTimestamp(event int) Timestamp         { return 0 }
func (h *testHandler) GetType(event int) EventType              { return DefaultEventType }
func (h *testHandler) OnDrop(event int) interface{}             { return nil }

func TestBatcherCount(t *testing.T) {
	b := newBatcher[int](BatchTypeCount, 2)

	b.addEvent(1, 100)
	require.False(t, b.isFull())

	b.addEvent(2, 100)
	require.True(t, b.isFull())

	events := b.reset()
	require.Equal(t, []int{1, 2}, events)
	require.Equal(t, 0, b.current)
	require.Equal(t, 0, len(b.buf))
}

func TestBatcherSize(t *testing.T) {
	b := newBatcher[int](BatchTypeSize, 10)

	b.addEvent(1, 4)
	require.False(t, b.isFull())

	b.addEvent(2, 6)
	require.True(t, b.isFull())

	events := b.reset()
	require.Equal(t, []int{1, 2}, events)
	require.Equal(t, 0, b.current)
	require.Equal(t, 0, len(b.buf))
}

func TestBatcherClone(t *testing.T) {
	b1 := newBatcher[int](BatchTypeCount, 2)
	b2 := b1.clone()

	require.NotSame(t, b1, b2)
	require.Equal(t, b1.batchType, b2.batchType)
	require.Equal(t, b1.capacity, b2.capacity)
	require.Equal(t, 0, b2.current)
	require.Equal(t, 0, len(b2.buf))

	b1.addEvent(1, 0)
	require.Equal(t, 0, len(b2.buf))
}

func TestParallelDynamicStreamAddPathClonesBatcher(t *testing.T) {
	handler := &testHandler{}
	ds := newParallelDynamicStream[int, string, int, testDest](handler, Option{StreamCount: 1})
	ds.Start()
	defer ds.Close()

	settings := NewAreaSettingsWithBatcher[int](BatchTypeCount, 2)

	require.NoError(t, ds.AddPath("p1", testDest{}, settings))
	require.NoError(t, ds.AddPath("p2", testDest{}, settings))

	ds.pathMap.RLock()
	defer ds.pathMap.RUnlock()
	require.NotNil(t, ds.pathMap.m["p1"].batcher)
	require.NotNil(t, ds.pathMap.m["p2"].batcher)
	require.NotSame(t, ds.pathMap.m["p1"].batcher, ds.pathMap.m["p2"].batcher)
	require.Equal(t, ds.pathMap.m["p1"].batcher.batchType, ds.pathMap.m["p2"].batcher.batchType)
	require.Equal(t, ds.pathMap.m["p1"].batcher.capacity, ds.pathMap.m["p2"].batcher.capacity)
}

func TestParallelDynamicStreamAddPathDefaultsBatcherWhenNil(t *testing.T) {
	handler := &testHandler{}
	ds := newParallelDynamicStream[int, string, int, testDest](handler, Option{StreamCount: 1})
	ds.Start()
	defer ds.Close()

	require.NoError(t, ds.AddPath("p1", testDest{}, AreaSettings[int]{}))

	ds.pathMap.RLock()
	defer ds.pathMap.RUnlock()
	require.NotNil(t, ds.pathMap.m["p1"].batcher)
	require.Equal(t, BatchTypeCount, ds.pathMap.m["p1"].batcher.batchType)
	require.Equal(t, 1, ds.pathMap.m["p1"].batcher.capacity)
}
