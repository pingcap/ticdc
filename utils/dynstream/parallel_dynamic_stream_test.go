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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
	stream.pathMap.RLock()
	require.Equal(t, 1, len(stream.pathMap.m))
	pi := stream.pathMap.m["path1"]
	stream.pathMap.RUnlock()
	require.Equal(t, 0, pi.area)
	require.Equal(t, uint64(1024), pi.areaMemStat.settings.Load().maxPendingSize)
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
	t.Parallel()
	fb1 := Feedback[int, string, any]{
		FeedbackType: PauseArea,
	}
	require.Equal(t, PauseArea, fb1.FeedbackType)

	fb1.FeedbackType = ResumeArea
	require.Equal(t, ResumeArea, fb1.FeedbackType)
}

// TestParallelDynamicStreamStress tests concurrent operations on multiple ParallelDynamicStream instances
// to verify the system handles concurrent operations gracefully, including operations during shutdown.
// This test is designed to detect data races and validate proper concurrent behavior.
func TestParallelDynamicStreamStress(t *testing.T) {
	const (
		streamCount  = 100                    // Number of ParallelDynamicStream instances
		workerCount  = 100                    // Number of concurrent goroutines
		testDuration = 200 * time.Millisecond // How long to run the test
	)

	streams := make([]*parallelDynamicStream[int, string, *mockEvent, any, *mockHandler], streamCount)
	handlers := make([]*mockHandler, streamCount)
	done := make(chan struct{})

	// Create all streams
	for i := 0; i < streamCount; i++ {
		handlers[i] = &mockHandler{}
		option := Option{StreamCount: 4}
		streams[i] = newParallelDynamicStream(mockHasher, handlers[i], option)
		streams[i].Start()
	}

	var wg sync.WaitGroup

	// Launch workers to add paths and push events concurrently
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			streamID := workerID % streamCount
			stream := streams[streamID]
			eventID := 0

			for {
				select {
				case <-done:
					return
				default:
					// Continuously add paths, push events, remove paths
					pathName := fmt.Sprintf("worker_%d_path_%d", workerID, eventID%10)
					destName := fmt.Sprintf("dest_%d_%d", workerID, eventID%10)

					// Add path (ignore errors - path may already exist)
					stream.AddPath(pathName, destName)

					// Push event
					event := &mockEvent{
						id:    eventID,
						path:  pathName,
						value: workerID*1000 + eventID,
						sleep: 0, // No sleep to increase concurrency
					}
					stream.Push(pathName, event)

					// Occasionally wake and remove paths
					if eventID%5 == 0 {
						stream.Wake(pathName)
					}
					if eventID%7 == 0 {
						stream.RemovePath(pathName)
					}

					eventID++
				}
			}
		}(i)
	}

	// Launch a goroutine to close streams during operation
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait a bit for operations to start
		time.Sleep(50 * time.Millisecond)

		// Close streams during operation
		for i := 0; i < streamCount; i++ {
			select {
			case <-done:
				return
			default:
				// Close every 10th stream during operation
				if i%10 == 0 {
					streams[i].Close()
					time.Sleep(time.Millisecond)
				}
			}
		}
	}()

	// Run for a specific duration
	time.Sleep(testDuration)
	close(done)

	// Wait for all workers to complete
	wg.Wait()

	// Close remaining streams
	for i, stream := range streams {
		if i%10 != 0 { // Skip already closed streams
			stream.Close()
		}
	}

	// The test passes if it completes without crashing
}

// TestParallelDynamicStreamConcurrentClose tests the specific scenario that can cause
// data races: concurrent Push/Wake operations while Close is being called.
// This test is designed to detect race conditions and validate that the system
// can handle concurrent shutdown scenarios gracefully.
//
// Expected behavior:
// - Some panics may occur when pushing to closed channels (this is expected)
// - Data races should be detected by the race detector if present
// - The test should complete without crashing
func TestParallelDynamicStreamConcurrentClose(t *testing.T) {
	const (
		iterations = 10 // Number of test iterations
		pushers    = 50 // Number of goroutines pushing events
		closers    = 5  // Number of goroutines closing streams
	)

	for iter := 0; iter < iterations; iter++ {
		t.Run(fmt.Sprintf("iteration_%d", iter), func(t *testing.T) {
			handler := &mockHandler{}
			option := Option{StreamCount: 4}
			stream := newParallelDynamicStream(mockHasher, handler, option)
			stream.Start()

			// Add some initial paths
			for i := 0; i < 10; i++ {
				pathName := fmt.Sprintf("path_%d", i)
				stream.AddPath(pathName, fmt.Sprintf("dest_%d", i))
			}

			var wg sync.WaitGroup
			done := make(chan struct{})

			// Launch pushers
			for i := 0; i < pushers; i++ {
				wg.Add(1)
				go func(pusherID int) {
					defer wg.Done()

					eventID := 0
					for {
						select {
						case <-done:
							return
						default:
							pathName := fmt.Sprintf("path_%d", eventID%10)
							event := &mockEvent{
								id:    eventID,
								path:  pathName,
								value: pusherID*1000 + eventID,
								sleep: 0,
							}
							stream.Push(pathName, event)
							stream.Wake(pathName)
							eventID++
						}
					}
				}(i)
			}

			// Launch closers
			for i := 0; i < closers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					// Wait a tiny bit then close
					time.Sleep(time.Millisecond)
					stream.Close()
				}()
			}

			// Let it run for a short time
			time.Sleep(10 * time.Millisecond)
			close(done)
			wg.Wait()
		})
		log.Info("pass iteration", zap.Int("iter", iter))
	}
}
