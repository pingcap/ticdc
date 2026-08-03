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

package priorityqueue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockItem struct {
	priority    int
	heapIndex   int
	description string
}

func newMockItem(priority int, description string) *mockItem {
	return &mockItem{priority: priority, description: description}
}

func (m *mockItem) SetHeapIndex(index int) {
	m.heapIndex = index
}

func (m *mockItem) GetHeapIndex() int {
	return m.heapIndex
}

func (m *mockItem) LessThan(other *mockItem) bool {
	return m.priority < other.priority
}

func TestQueuePushPeekPopOrder(t *testing.T) {
	q := New[*mockItem]()

	_, ok := q.Peek()
	require.False(t, ok)

	tasks := []*mockItem{
		newMockItem(10, "task1"),
		newMockItem(5, "task2"),
		newMockItem(15, "task3"),
		newMockItem(7, "task4"),
		newMockItem(12, "task5"),
	}
	for _, task := range tasks {
		require.True(t, q.Push(task))
	}
	require.Equal(t, 5, q.Len())

	top, ok := q.Peek()
	require.True(t, ok)
	require.Equal(t, "task2", top.description)
	require.Equal(t, 5, q.Len())

	expectedOrder := []string{"task2", "task4", "task1", "task5", "task3"}
	for _, expected := range expectedOrder {
		task, err := q.Pop(context.Background())
		require.NoError(t, err)
		require.Equal(t, expected, task.description)
	}
	require.Equal(t, 0, q.Len())
}

func TestQueuePopBlocking(t *testing.T) {
	q := New[*mockItem]()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	task, err := q.Pop(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, task)
	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)

	go func() {
		time.Sleep(50 * time.Millisecond)
		q.Push(newMockItem(10, "task1"))
	}()

	start = time.Now()
	task, err = q.Pop(context.Background())
	require.NoError(t, err)
	require.Equal(t, "task1", task.description)
	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
}

func TestQueueTryPopAndUpdateExistingItem(t *testing.T) {
	q := New[*mockItem]()

	_, ok := q.TryPop()
	require.False(t, ok)

	task := newMockItem(10, "task")
	require.True(t, q.Push(task))
	task.priority = 5
	require.True(t, q.AddOrUpdate(task))
	require.Equal(t, 1, q.Len())

	poppedTask, ok := q.TryPop()
	require.True(t, ok)
	require.Equal(t, 5, poppedTask.priority)
	require.Equal(t, 0, poppedTask.heapIndex)
}

func TestQueueConcurrentOperations(t *testing.T) {
	q := New[*mockItem]()

	const (
		numProducers     = 3
		numConsumers     = 2
		tasksPerProducer = 10
	)
	totalTasks := numProducers * tasksPerProducer

	var wg sync.WaitGroup
	var consumedCount int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for range numConsumers {
		wg.Go(func() {
			for {
				task, err := q.Pop(ctx)
				if err != nil {
					return
				}
				require.NotNil(t, task)
				if atomic.AddInt64(&consumedCount, 1) >= int64(totalTasks) {
					cancel()
					return
				}
			}
		})
	}

	for producerID := range numProducers {
		wg.Go(func() {
			for j := range tasksPerProducer {
				priority := producerID*tasksPerProducer + j
				require.True(t, q.Push(newMockItem(priority, "task")))
			}
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("test timed out")
	}
	require.Equal(t, int64(totalTasks), consumedCount)
}

func TestQueueClose(t *testing.T) {
	q := New[*mockItem]()
	require.True(t, q.Push(newMockItem(10, "task1")))
	require.True(t, q.Push(newMockItem(5, "task2")))

	q.Close()
	require.False(t, q.Push(newMockItem(1, "closed")))

	task, err := q.Pop(context.Background())
	require.NoError(t, err)
	require.Equal(t, "task2", task.description)

	task, err = q.Pop(context.Background())
	require.NoError(t, err)
	require.Equal(t, "task1", task.description)

	task, err = q.Pop(context.Background())
	require.ErrorIs(t, err, ErrClosed)
	require.Nil(t, task)

	require.NotPanics(t, q.Close)
}

func TestQueueCloseWakesBlockedPop(t *testing.T) {
	q := New[*mockItem]()

	done := make(chan struct{})
	go func() {
		defer close(done)
		task, err := q.Pop(context.Background())
		require.ErrorIs(t, err, ErrClosed)
		require.Nil(t, task)
	}()

	q.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Pop was not woken by Close")
	}
}

func TestQueuePushMultipleItemsWakesMultipleBlockedPop(t *testing.T) {
	q := New[*mockItem]()

	const waiters = 2
	ready := make(chan struct{}, waiters)
	done := make(chan struct{}, waiters)
	for range waiters {
		go func() {
			ready <- struct{}{}
			task, err := q.Pop(context.Background())
			require.NoError(t, err)
			require.NotNil(t, task)
			done <- struct{}{}
		}()
	}

	for range waiters {
		<-ready
	}
	require.True(t, q.Push(newMockItem(10, "task1")))
	require.True(t, q.Push(newMockItem(20, "task2")))

	for range waiters {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("blocked Pop was not woken")
		}
	}
}
