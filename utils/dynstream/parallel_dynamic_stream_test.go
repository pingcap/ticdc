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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Mock hasher
func mockHasher(p string) uint64 {
	return uint64(len(p))
}

func TestParallelDynamicStreamBasic(t *testing.T) {
	handler := &mockHandler{}
	option := Option{StreamCount: 4}
	stream := NewParallelDynamicStream(mockHasher, handler, option)
	stream.Start()
	defer stream.Close()

	t.Run("add path", func(t *testing.T) {
		err := stream.AddPath("path1", "dest1")
		require.NoError(t, err)
		// Test duplicate path
		err = stream.AddPath("path1", "dest1")
		require.Error(t, err)
	})

	t.Run("remove path", func(t *testing.T) {
		err := stream.RemovePath("path1")
		require.NoError(t, err)
		// Test non-existent path
		err = stream.RemovePath("path1")
		require.Error(t, err)
	})
}

func TestParallelDynamicStreamPush(t *testing.T) {
	handler := &mockHandler{}
	option := Option{StreamCount: 4}
	stream := newParallelDynamicStream(mockHasher, handler, option)
	stream.Start()
	defer stream.Close()

	// case 1: push to non-existent path
	event := mockEvent{id: 1, path: "non-existent", value: 10, sleep: 10 * time.Millisecond}
	stream.Push("non-existent", &event) // Should be dropped silently
	require.Equal(t, 1, len(handler.droppedEvents))
	require.Equal(t, event, *handler.droppedEvents[0])
	handler.droppedEvents = handler.droppedEvents[:0]

	// case 2: push to existing path
	path := "test/path"
	err := stream.AddPath(path, "dest1")
	require.NoError(t, err)
	event = mockEvent{id: 1, path: path, value: 10, sleep: 10 * time.Millisecond}
	stream.Push(path, &event)
	require.Equal(t, 0, len(handler.droppedEvents))
}

func TestParallelDynamicStreamMetrics(t *testing.T) {
	handler := &mockHandler{}
	option := Option{StreamCount: 4}
	stream := newParallelDynamicStream(mockHasher, handler, option)

	stream.Start()
	defer stream.Close()

	// Add some paths
	err := stream.AddPath("path1", "dest1")
	require.NoError(t, err)
	err = stream.AddPath("path2", "dest2")
	require.NoError(t, err)

	// Remove one path
	err = stream.RemovePath("path1")
	require.NoError(t, err)

	metrics := stream.GetMetrics()
	require.Equal(t, 2, metrics.AddPath)
	require.Equal(t, 1, metrics.RemovePath)
}

func TestParallelDynamicStreamMemoryControl(t *testing.T) {
	handler := &mockHandler{}
	option := Option{
		StreamCount:         4,
		EnableMemoryControl: true,
	}
	stream := newParallelDynamicStream(mockHasher, handler, option)

	stream.Start()
	defer stream.Close()

	// case 1: memory control enabled
	require.NotNil(t, stream.memControl)
	require.NotNil(t, stream.feedbackChan)
	settings := AreaSettings{maxPendingSize: 1024, feedbackInterval: 10 * time.Millisecond}
	// The path is belong to area 0
	stream.AddPath("path1", "dest1", settings)
	stream.mutex.Lock()
	require.Equal(t, 1, len(stream.pathMap))
	pi := stream.pathMap["path1"]
	stream.mutex.Unlock()
	require.Equal(t, 0, pi.area)
	require.Equal(t, 1024, pi.areaMemStat.settings.Load().maxPendingSize)
	require.Equal(t, 10*time.Millisecond, pi.areaMemStat.settings.Load().feedbackInterval)

	// case 2: add event to the path
	startNotify := &sync.WaitGroup{}
	doneNotify := &sync.WaitGroup{}
	inc := &atomic.Int64{}
	work := newInc(1, inc)
	stream.Push("path1", newMockEvent(1, "path1", 10*time.Millisecond, work, startNotify, doneNotify))
	startNotify.Wait()
	require.Equal(t, int64(0), inc.Load())
	doneNotify.Wait()
	require.Equal(t, int64(1), inc.Load())
}

func TestFeedBack(t *testing.T) {
	fb1 := Feedback[int, string, any]{
		FeedbackType: PauseArea,
	}
	require.Equal(t, PauseArea, fb1.FeedbackType)

	fb1.FeedbackType = ResumeArea
	require.Equal(t, ResumeArea, fb1.FeedbackType)
}
