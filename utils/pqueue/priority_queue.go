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

package pqueue

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/utils/heap"
)

// PriorityTask is the interface for priority-based tasks
// It implements heap.Item interface
type PriorityTask interface {
	// Priority returns the priority value, lower value means higher priority
	Priority() int
	// heap.Item interface methods
	SetHeapIndex(int)
	GetHeapIndex() int
	LessThan(PriorityTask) bool
}

// PriorityQueue is a thread-safe priority queue for region tasks
// It integrates a signal channel to support blocking operations
type PriorityQueue struct {
	mu       sync.Mutex
	heap     *heap.Heap[PriorityTask]
	capacity int

	// signal channel for blocking operations
	signal chan struct{}
}

const defaultSignalSize = 1024

// NewPriorityQueue creates an unbounded priority queue.
func NewPriorityQueue() *PriorityQueue {
	return NewBoundedPriorityQueue(0)
}

// NewBoundedPriorityQueue creates a priority queue with a maximum size limit.
// When limit <= 0, the priority queue behaves as unbounded.
func NewBoundedPriorityQueue(limit int) *PriorityQueue {
	if limit < 0 {
		panic("priority queue capacity must not be negative")
	}
	signalSize := defaultSignalSize
	if limit > 0 {
		if limit < defaultSignalSize {
			signalSize = limit
		}
		if signalSize == 0 {
			signalSize = 1
		}
	}
	return &PriorityQueue{
		heap:     heap.NewHeap[PriorityTask](),
		signal:   make(chan struct{}, signalSize),
		capacity: limit,
	}
}

// Push adds a task to the priority queue and sends a signal.
// This helper keeps backward compatibility and does not enforce queue capacity.
func (pq *PriorityQueue) Push(task PriorityTask) {
	pq.mu.Lock()
	pq.heap.AddOrUpdate(task)
	pq.mu.Unlock()
	pq.notify()
}

// TryPush inserts the task into the queue while respecting the capacity limit.
// It returns whether the task remains in the queue and, if an existing task was
// evicted to make space, the evicted task.
func (pq *PriorityQueue) TryPush(task PriorityTask) (bool, PriorityTask) {
	pq.mu.Lock()
	added, evicted := pq.tryPushLocked(task)
	pq.mu.Unlock()
	if added {
		pq.notify()
	}
	return added, evicted
}

// Pop removes and returns the highest priority task
// This is a blocking operation that waits for a signal
// Returns nil if the context is cancelled
func (pq *PriorityQueue) Pop(ctx context.Context) (PriorityTask, error) {
	for {
		// First try to pop without waiting
		pq.mu.Lock()
		task, ok := pq.heap.PopTop()
		pq.mu.Unlock()

		if ok {
			return task, nil
		}

		// Queue is empty, wait for signal
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case _, ok := <-pq.signal:
			if !ok {
				// Signal channel is closed.
				return nil, errors.New("signal channel is closed")
			}
			// Got signal, try to pop again
			continue
		}
	}
}

// TryPop attempts to pop a task without blocking
// Returns nil if the queue is empty
func (pq *PriorityQueue) TryPop() PriorityTask {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	task, ok := pq.heap.PopTop()
	if !ok {
		return nil
	}
	return task
}

// Peek returns the highest priority task without removing it
// Returns nil if the queue is empty
func (pq *PriorityQueue) Peek() PriorityTask {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	task, ok := pq.heap.PeekTop()
	if !ok {
		return nil
	}
	return task
}

// Len returns the number of tasks in the queue
func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return pq.heap.Len()
}

// Close closes the signal channel
func (pq *PriorityQueue) Close() {
	// pop all tasks
	for pq.Len() > 0 {
		pq.TryPop()
	}
}

// Remove removes a task from the queue if it exists.
func (pq *PriorityQueue) Remove(task PriorityTask) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return pq.heap.Remove(task)
}

func (pq *PriorityQueue) notify() {
	select {
	case pq.signal <- struct{}{}:
	default:
		// Signal channel is full, ignore.
	}
}

func (pq *PriorityQueue) tryPushLocked(task PriorityTask) (bool, PriorityTask) {
	// Updating an existing task never changes the queue size.
	if task.GetHeapIndex() != 0 {
		pq.heap.AddOrUpdate(task)
		return true, nil
	}

	// Unlimited or not full yet.
	if pq.capacity <= 0 || pq.heap.Len() < pq.capacity {
		pq.heap.AddOrUpdate(task)
		return true, nil
	}

	// Queue is full, find the task with the lowest priority (largest value).
	worst := pq.findWorstLocked()
	if worst == nil {
		return false, nil
	}

	// If the new task is not better, drop it.
	if !task.LessThan(worst) {
		return false, nil
	}

	// Replace the worst task with the new, higher priority task.
	pq.heap.Remove(worst)
	pq.heap.AddOrUpdate(task)
	return true, worst
}

func (pq *PriorityQueue) findWorstLocked() PriorityTask {
	var worst PriorityTask
	for _, item := range pq.heap.All() {
		if worst == nil || worst.LessThan(item) {
			worst = item
		}
	}
	return worst
}
