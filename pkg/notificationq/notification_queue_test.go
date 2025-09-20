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

package notificationq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNotificationQueue_BasicOperations(t *testing.T) {
	queue := NewNotificationQueue[string, int](10)

	task := NotificationTask[string, int]{
		Key:     "test",
		Payload: 42,
	}

	queue.Enqueue(task, func(existing, new int) int { return new })

	if queue.Len() != 1 {
		t.Errorf("Expected queue length 1, got %d", queue.Len())
	}

	dequeued, ok := queue.Dequeue()
	if !ok {
		t.Fatal("Expected to dequeue task")
	}

	if dequeued.Key != "test" || dequeued.Payload != 42 {
		t.Errorf("Expected task {test, 42}, got {%s, %d}", dequeued.Key, dequeued.Payload)
	}

	if !queue.IsEmpty() {
		t.Error("Queue should be empty after dequeue")
	}
}

func TestNotificationQueue_TaskMerging(t *testing.T) {
	queue := NewNotificationQueue[string, int](2)

	mergeFunc := func(existing, new int) int {
		return existing + new
	}

	tasks := []NotificationTask[string, int]{
		{Key: "sum", Payload: 10},
		{Key: "sum", Payload: 20},
		{Key: "sum", Payload: 30},
	}

	for _, task := range tasks {
		queue.Enqueue(task, mergeFunc)
	}

	if queue.Len() != 1 {
		t.Errorf("Expected queue length 1 after merging, got %d", queue.Len())
	}

	dequeued, ok := queue.Dequeue()
	if !ok {
		t.Fatal("Expected to dequeue merged task")
	}

	if dequeued.Payload != 60 {
		t.Errorf("Expected merged payload 60, got %d", dequeued.Payload)
	}
}

func TestNotificationQueue_NoOrderingGuarantee(t *testing.T) {
	queue := NewNotificationQueue[string, int](3)
	var results []int

	// Enqueue tasks with different keys - order not guaranteed
	queue.Enqueue(NotificationTask[string, int]{Key: "A", Payload: 1}, func(e, n int) int { return n })
	queue.Enqueue(NotificationTask[string, int]{Key: "B", Payload: 2}, func(e, n int) int { return n })
	queue.Enqueue(NotificationTask[string, int]{Key: "C", Payload: 3}, func(e, n int) int { return n })

	// Dequeue all tasks
	for queue.Len() > 0 {
		task, _ := queue.Dequeue()
		results = append(results, task.Payload)
	}

	// Verify we got all values but order is not specified
	expectedValues := map[int]bool{1: true, 2: true, 3: true}
	for _, result := range results {
		if !expectedValues[result] {
			t.Errorf("Unexpected result value: %d", result)
		}
		delete(expectedValues, result)
	}

	if len(expectedValues) > 0 {
		t.Errorf("Missing expected values: %v", expectedValues)
	}
}

func TestNotificationQueue_ConcurrentEnqueueDequeue(t *testing.T) {
	queue := NewNotificationQueue[int, string](100)
	var wg sync.WaitGroup
	const numWriters = 10
	const numTasks = 100

	// Concurrent writers
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < numTasks; j++ {
				task := NotificationTask[int, string]{
					Key:     writerID*1000 + j,
					Payload: "data",
				}
				queue.Enqueue(task, func(e, n string) string { return n })
			}
		}(i)
	}

	// Concurrent reader
	var totalDequeued int32
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-done:
				return
			default:
				if _, ok := queue.Dequeue(); ok {
					atomic.AddInt32(&totalDequeued, 1)
				}
			}
		}
	}()

	wg.Wait()
	// Give reader some time to process
	time.Sleep(100 * time.Millisecond)
	close(done)

	stats := queue.Stats()
	if stats.TotalEnqueued != int64(numWriters*numTasks) {
		t.Errorf("Expected %d enqueued, got %d", numWriters*numTasks, stats.TotalEnqueued)
	}
}

func TestNotificationQueue_Clear(t *testing.T) {
	queue := NewNotificationQueue[string, int](5)

	for i := 0; i < 3; i++ {
		task := NotificationTask[string, int]{
			Key:     fmt.Sprintf("key-%d", i),
			Payload: i,
		}
		queue.Enqueue(task, func(e, n int) int { return n })
	}

	if queue.Len() != 3 {
		t.Errorf("Expected queue length 3 before clear, got %d", queue.Len())
	}

	queue.Clear()

	if queue.Len() != 0 {
		t.Errorf("Expected queue length 0 after clear, got %d", queue.Len())
	}
	if !queue.IsEmpty() {
		t.Error("Queue should be empty after clear")
	}
}

func TestNotificationQueue_StatsTracking(t *testing.T) {
	queue := NewNotificationQueue[string, int](10)

	for i := 0; i < 5; i++ {
		task := NotificationTask[string, int]{
			Key:     "stats",
			Payload: i,
		}
		queue.Enqueue(task, func(existing, new int) int { return new })
	}

	stats := queue.Stats()
	if stats.TotalEnqueued != 5 {
		t.Errorf("Expected 5 enqueued, got %d", stats.TotalEnqueued)
	}
	if stats.TotalMerged != 4 {
		t.Errorf("Expected 4 merged, got %d", stats.TotalMerged)
	}

	// Dequeue the merged task
	queue.Dequeue()

	stats = queue.Stats()
	if stats.TotalDequeued != 1 {
		t.Errorf("Expected 1 dequeued, got %d", stats.TotalDequeued)
	}
}
