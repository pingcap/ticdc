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

package logpuller

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

// mockPriorityTask is a simple mock implementation of PriorityTask for testing
type mockPriorityTask struct {
	priority    int
	heapIndex   int
	regionInfo  regionInfo
	description string
}

func newMockPriorityTask(priority int, description string) *mockPriorityTask {
	// Create a minimal regionInfo for testing
	verID := tikv.NewRegionVerID(1, 1, 1)
	span := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}

	// Create a subscribedSpan with atomic resolvedTs
	subscribedSpan := &subscribedSpan{
		resolvedTs: atomic.Uint64{},
	}
	subscribedSpan.resolvedTs.Store(oracle.GoTimeToTS(time.Now()))

	regionInfo := regionInfo{
		verID:          verID,
		span:           span,
		subscribedSpan: subscribedSpan,
	}

	return &mockPriorityTask{
		priority:    priority,
		heapIndex:   0,
		regionInfo:  regionInfo,
		description: description,
	}
}

func (m *mockPriorityTask) Priority() int {
	return m.priority
}

func (m *mockPriorityTask) GetRegionInfo() regionInfo {
	return m.regionInfo
}

func (m *mockPriorityTask) SetHeapIndex(index int) {
	m.heapIndex = index
}

func (m *mockPriorityTask) GetHeapIndex() int {
	return m.heapIndex
}

func (m *mockPriorityTask) LessThan(other PriorityTask) bool {
	return m.Priority() < other.Priority()
}

func TestNewPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue()
	require.NotNil(t, pq)
	require.NotNil(t, pq.heap)
	require.NotNil(t, pq.signal)
	require.Equal(t, 0, pq.Len())
}

func TestPriorityQueue_Push(t *testing.T) {
	pq := NewPriorityQueue()

	task1 := newMockPriorityTask(10, "task1")
	task2 := newMockPriorityTask(5, "task2")

	// Test pushing single task
	pq.Push(task1)
	require.Equal(t, 1, pq.Len())

	// Test pushing multiple tasks
	pq.Push(task2)
	require.Equal(t, 2, pq.Len())

	// Verify signal channel receives notifications
	select {
	case <-pq.signal:
		// Expected - signal received
	case <-time.After(time.Millisecond * 100):
		t.Fatal("Expected signal but none received")
	}
}

func TestPriorityQueue_Peek(t *testing.T) {
	pq := NewPriorityQueue()

	// Test peek on empty queue
	task := pq.Peek()
	require.Nil(t, task)

	// Add tasks with different priorities
	task1 := newMockPriorityTask(10, "task1")
	task2 := newMockPriorityTask(5, "task2") // Higher priority (lower value)
	task3 := newMockPriorityTask(15, "task3")

	pq.Push(task1)
	pq.Push(task2)
	pq.Push(task3)

	// Peek should return highest priority task (lowest value)
	topTask := pq.Peek()
	require.NotNil(t, topTask)
	require.Equal(t, 5, topTask.Priority())
	require.Equal(t, "task2", topTask.(*mockPriorityTask).description)

	// Verify peek doesn't remove the task
	require.Equal(t, 3, pq.Len())

	// Peek again should return the same task
	topTaskAgain := pq.Peek()
	require.Equal(t, topTask, topTaskAgain)
}

func TestPriorityQueue_PopBlocking(t *testing.T) {
	pq := NewPriorityQueue()

	// Test pop on empty queue with context cancellation
	t.Run("PopWithCancellation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()

		start := time.Now()
		task, err := pq.Pop(ctx)
		require.Error(t, err)
		elapsed := time.Since(start)

		require.Nil(t, task)
		require.True(t, elapsed >= time.Millisecond*50)
	})

	// Test pop with signal
	t.Run("PopWithSignal", func(t *testing.T) {
		ctx := context.Background()

		// Add a task in a goroutine after a short delay
		go func() {
			time.Sleep(time.Millisecond * 50)
			task1 := newMockPriorityTask(10, "task1")
			pq.Push(task1)
		}()

		start := time.Now()
		task, err := pq.Pop(ctx)
		require.NoError(t, err)
		elapsed := time.Since(start)

		require.NotNil(t, task)
		require.Equal(t, 10, task.Priority())
		require.True(t, elapsed >= time.Millisecond*50)
		require.True(t, elapsed < time.Millisecond*200) // Should not wait too long
	})
}

func TestPriorityQueue_PopOrder(t *testing.T) {
	pq := NewPriorityQueue()
	ctx := context.Background()

	// Add tasks with different priorities
	tasks := []*mockPriorityTask{
		newMockPriorityTask(10, "task1"),
		newMockPriorityTask(5, "task2"), // Highest priority
		newMockPriorityTask(15, "task3"),
		newMockPriorityTask(7, "task4"),
		newMockPriorityTask(12, "task5"),
	}

	for _, task := range tasks {
		pq.Push(task)
	}

	// Pop tasks and verify they come out in priority order
	expectedOrder := []string{"task2", "task4", "task1", "task5", "task3"}
	expectedPriorities := []int{5, 7, 10, 12, 15}

	for i, expectedDesc := range expectedOrder {
		task, err := pq.Pop(ctx)
		require.NoError(t, err)
		require.NotNil(t, task)
		require.Equal(t, expectedPriorities[i], task.Priority())
		require.Equal(t, expectedDesc, task.(*mockPriorityTask).description)
	}

	// Verify queue is empty
	require.Equal(t, 0, pq.Len())
}

func TestPriorityQueue_Len(t *testing.T) {
	pq := NewPriorityQueue()

	// Test empty queue
	require.Equal(t, 0, pq.Len())

	// Add tasks and verify length
	for i := 0; i < 5; i++ {
		task := newMockPriorityTask(i, "task")
		pq.Push(task)
		require.Equal(t, i+1, pq.Len())
	}

	// Remove tasks and verify length
	ctx := context.Background()
	for i := 4; i >= 0; i-- {
		pq.Pop(ctx)
		require.Equal(t, i, pq.Len())
	}
}

func TestPriorityQueue_ConcurrentOperations(t *testing.T) {
	pq := NewPriorityQueue()

	numProducers := 3
	numConsumers := 2
	tasksPerProducer := 10
	totalTasks := numProducers * tasksPerProducer

	var wg sync.WaitGroup
	var consumedCount int64
	var mu sync.Mutex
	consumedTasks := make([]PriorityTask, 0, totalTasks)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consumers
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()
			for {
				task, err := pq.Pop(ctx)
				if err != nil {
					return
				}

				mu.Lock()
				consumedTasks = append(consumedTasks, task)
				count := atomic.AddInt64(&consumedCount, 1)
				mu.Unlock()

				if count >= int64(totalTasks) {
					cancel() // Signal other consumers to stop
					return
				}
			}
		}(i)
	}

	// Start producers
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for j := 0; j < tasksPerProducer; j++ {
				priority := (producerID * tasksPerProducer) + j
				task := newMockPriorityTask(priority, "concurrent_task")
				pq.Push(task)
				time.Sleep(time.Microsecond * 10) // Small delay to simulate real work
			}
		}(i)
	}

	// Wait for all producers to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait with timeout
	select {
	case <-done:
		// Success
	case <-time.After(time.Second * 5):
		cancel() // Cancel to stop consumers
		t.Fatal("Test timed out")
	}

	// Verify all tasks were consumed
	require.Equal(t, int64(totalTasks), atomic.LoadInt64(&consumedCount))
	require.Equal(t, totalTasks, len(consumedTasks))

	// Verify all tasks were processed
	for i := 0; i < len(consumedTasks); i++ {
		require.NotNil(t, consumedTasks[i])
	}
}

func TestPriorityQueue_SignalChannelFull(t *testing.T) {
	pq := NewPriorityQueue()

	// Fill the signal channel to capacity
	for i := 0; i < cap(pq.signal); i++ {
		select {
		case pq.signal <- struct{}{}:
		default:
			t.Fatalf("Failed to fill signal channel at iteration %d", i)
		}
	}

	// Push a task when signal channel is full - should not block
	task := newMockPriorityTask(10, "task")
	start := time.Now()
	pq.Push(task)
	elapsed := time.Since(start)

	// Should complete quickly even though signal channel is full
	require.True(t, elapsed < time.Millisecond*100)
	require.Equal(t, 1, pq.Len())
}

func TestPriorityQueue_UpdateExistingTask(t *testing.T) {
	pq := NewPriorityQueue()

	// Create a task and add it to queue
	task := newMockPriorityTask(10, "task")
	pq.Push(task)
	require.Equal(t, 1, pq.Len())

	// Update the task's priority and push again
	task.priority = 5
	pq.Push(task)

	// Length should still be 1 (task was updated, not added)
	require.Equal(t, 1, pq.Len())

	// Verify the task has the updated priority
	ctx := context.Background()
	poppedTask, err := pq.Pop(ctx)
	require.NoError(t, err)
	require.NotNil(t, poppedTask)
	require.Equal(t, 5, poppedTask.Priority())
}

func TestPriorityQueue_Close(t *testing.T) {
	pq := NewPriorityQueue()

	// Add 3 task before closing
	task := newMockPriorityTask(10, "task")
	pq.Push(task)
	require.Equal(t, 1, pq.Len())
	task2 := newMockPriorityTask(5, "task2")
	pq.Push(task2)
	require.Equal(t, 2, pq.Len())
	task3 := newMockPriorityTask(15, "task3")
	pq.Push(task3)
	require.Equal(t, 3, pq.Len())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			pq.Push(newMockPriorityTask(i, "task"))
		}
	}()

	go func() {
		defer wg.Done()
		defer cancel()

		for i := 0; i < 1000; i++ {
			// Test that close doesn't panic
			require.NotPanics(t, func() {
				pq.Close()
			})
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			// Make sure it won't block when the queue is closed
			pq.Pop(ctx)
		}
	}()

	wg.Wait()
	require.NotPanics(t, func() {
		pq.Close()
	})
	// Test that the tasks are popped
	require.Equal(t, 0, pq.Len())
}

func TestPriorityQueue_EmptyQueueOperations(t *testing.T) {
	pq := NewPriorityQueue()

	// Test peek on empty queue
	task := pq.Peek()
	require.Nil(t, task)

	// Test len on empty queue
	require.Equal(t, 0, pq.Len())

	// Test pop on empty queue with immediate cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	task2, err := pq.Pop(ctx)
	require.Nil(t, task2)
	require.Error(t, err)
}

func TestPriorityQueue_RealPriorityTaskIntegration(t *testing.T) {
	pq := NewPriorityQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	currentTs := oracle.GoTimeToTS(time.Now())

	// Create real priority tasks with different types
	verID := tikv.NewRegionVerID(1, 1, 1)
	span := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}

	subscribedSpan := &subscribedSpan{
		resolvedTs: atomic.Uint64{},
	}
	subscribedSpan.resolvedTs.Store(oracle.GoTimeToTS(time.Now().Add(-time.Second)))

	regionInfo := regionInfo{
		verID:          verID,
		span:           span,
		subscribedSpan: subscribedSpan,
	}

	// Create tasks with different priorities
	errorTask := NewRegionPriorityTask(TaskHighPrior, regionInfo, currentTs+1)
	highTask := NewRegionPriorityTask(TaskHighPrior, regionInfo, currentTs)
	lowTask := NewRegionPriorityTask(TaskLowPrior, regionInfo, currentTs)

	// Add tasks in non-priority order
	pq.Push(lowTask)
	pq.Push(errorTask)
	pq.Push(highTask)

	require.Equal(t, 3, pq.Len())

	// Pop tasks and verify they come out in priority order
	// TaskRegionError should have highest priority (lowest value)
	first, err := pq.Pop(ctx)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, TaskHighPrior, first.(*regionPriorityTask).taskType)

	second, err := pq.Pop(ctx)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, TaskHighPrior, second.(*regionPriorityTask).taskType)

	third, err := pq.Pop(ctx)
	require.NoError(t, err)
	require.NotNil(t, third)
	require.Equal(t, TaskLowPrior, third.(*regionPriorityTask).taskType)

	require.Equal(t, 0, pq.Len())

	pq.Close()
	cancel()
	task, err := pq.Pop(ctx)
	require.Nil(t, task)
	require.Error(t, err)
}
