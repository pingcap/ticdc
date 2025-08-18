// Copyright 2024 PingCAP, Inc.
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

package logpuller

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestPriorityQueue(t *testing.T) {
	// Create a mock regionInfo for testing
	mockRegionInfo := regionInfo{
		verID: tikv.NewRegionVerID(1, 1, 1),
		span: heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	t.Run("test basic operations", func(t *testing.T) {
		pq := NewPriorityQueue()
		defer pq.Close()

		// Test empty queue
		require.True(t, pq.IsEmpty())
		require.Equal(t, 0, pq.Len())
		require.Nil(t, pq.Peek())
		require.Nil(t, pq.TryPop())

		// Add a task
		task := NewRegionPriorityTask(TaskHighPrior, mockRegionInfo)
		pq.Push(task)

		require.False(t, pq.IsEmpty())
		require.Equal(t, 1, pq.Len())

		// Peek should return the task without removing it
		peeked := pq.Peek()
		require.Equal(t, task, peeked)
		require.Equal(t, 1, pq.Len())

		// TryPop should return and remove the task
		popped := pq.TryPop()
		require.Equal(t, task, popped)
		require.True(t, pq.IsEmpty())
		require.Equal(t, 0, pq.Len())
	})

	t.Run("test priority ordering", func(t *testing.T) {
		pq := NewPriorityQueue()
		defer pq.Close()

		// Create tasks with different priorities
		regionChangeTask := NewRegionPriorityTask(TaskHighPrior, mockRegionInfo)
		newSubTask := NewRegionPriorityTask(TaskLowPrior, mockRegionInfo)

		// Add them in reverse order
		pq.Push(newSubTask)
		pq.Push(regionChangeTask)

		// Region change task should be popped first (higher priority)
		first := pq.TryPop()
		require.Equal(t, regionChangeTask, first)

		// New subscription task should be popped second
		second := pq.TryPop()
		require.Equal(t, newSubTask, second)

		require.True(t, pq.IsEmpty())
	})

	t.Run("test time-based priority", func(t *testing.T) {
		pq := NewPriorityQueue()
		defer pq.Close()

		// Create two tasks of the same type
		task1 := NewRegionPriorityTask(TaskLowPrior, mockRegionInfo)

		// Wait a bit
		time.Sleep(10 * time.Millisecond)

		task2 := NewRegionPriorityTask(TaskLowPrior, mockRegionInfo)

		// Add them in reverse order
		pq.Push(task2)
		pq.Push(task1)

		// task1 should be popped first (waited longer, higher priority)
		first := pq.TryPop()
		require.Equal(t, task1, first)

		second := pq.TryPop()
		require.Equal(t, task2, second)
	})

	t.Run("test remove operation", func(t *testing.T) {
		pq := NewPriorityQueue()
		defer pq.Close()

		task1 := NewRegionPriorityTask(TaskHighPrior, mockRegionInfo)
		task2 := NewRegionPriorityTask(TaskLowPrior, mockRegionInfo)

		pq.Push(task1)
		pq.Push(task2)

		require.Equal(t, 2, pq.Len())

		// Remove task1
		removed := pq.Remove(task1)
		require.True(t, removed)
		require.Equal(t, 1, pq.Len())

		// task2 should still be there
		popped := pq.TryPop()
		require.Equal(t, task2, popped)

		// Try to remove non-existent task
		removed = pq.Remove(task1)
		require.False(t, removed)
	})

	t.Run("test blocking pop", func(t *testing.T) {
		pq := NewPriorityQueue()
		defer pq.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Try to pop from empty queue with timeout
		start := time.Now()
		task := pq.Pop(ctx)
		duration := time.Since(start)

		require.Nil(t, task)
		require.GreaterOrEqual(t, duration, 90*time.Millisecond) // Should wait almost the full timeout
	})

	t.Run("test blocking pop with signal", func(t *testing.T) {
		pq := NewPriorityQueue()
		defer pq.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Start a goroutine to push a task after a short delay
		go func() {
			time.Sleep(10 * time.Millisecond)
			task := NewRegionPriorityTask(TaskHighPrior, mockRegionInfo)
			pq.Push(task)
		}()

		// Pop should return the task after signal
		start := time.Now()
		task := pq.Pop(ctx)
		duration := time.Since(start)

		require.NotNil(t, task)
		require.Less(t, duration, 100*time.Millisecond) // Should return quickly after signal
	})

	t.Run("test concurrent operations", func(t *testing.T) {
		pq := NewPriorityQueue()
		defer pq.Close()

		// Test that the queue is thread-safe
		done := make(chan bool, 2)

		// Goroutine 1: Push tasks
		go func() {
			for i := 0; i < 100; i++ {
				task := NewRegionPriorityTask(TaskHighPrior, mockRegionInfo)
				pq.Push(task)
			}
			done <- true
		}()

		// Goroutine 2: Pop tasks
		go func() {
			for i := 0; i < 100; i++ {
				for pq.IsEmpty() {
					time.Sleep(1 * time.Millisecond)
				}
				task := pq.TryPop()
				require.NotNil(t, task)
			}
			done <- true
		}()

		// Wait for both goroutines to complete
		<-done
		<-done

		require.True(t, pq.IsEmpty())
	})
}
