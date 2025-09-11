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

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/utils/heap"
)

// PriorityQueue is a thread-safe priority queue for region tasks
// It integrates a signal channel to support blocking operations
type PriorityQueue struct {
	mu   sync.Mutex
	heap *heap.Heap[PriorityTask]

	// signal channel for blocking operations
	signal chan struct{}
}

// NewPriorityQueue creates a new priority queue
func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		heap:   heap.NewHeap[PriorityTask](),
		signal: make(chan struct{}, 1024),
	}
}

// Push adds a task to the priority queue and sends a signal
// This is a non-blocking operation
func (pq *PriorityQueue) Push(task PriorityTask) {
	pq.mu.Lock()
	pq.heap.AddOrUpdate(task)
	pq.mu.Unlock()

	// Send signal to notify waiting consumers
	select {
	case pq.signal <- struct{}{}:
	default:
		// Signal channel is full, ignore
	}
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
	close(pq.signal)
}
