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

// Package priorityqueue provides a thread-safe blocking priority queue.
package priorityqueue

import (
	"context"
	"errors"
	"sync"

	"github.com/pingcap/ticdc/utils/heap"
)

// ErrClosed is returned by Pop when the queue has been closed and drained.
var ErrClosed = errors.New("priority queue is closed")

// PriorityQueue is a thread-safe priority queue based on utils/heap.
//
// The queue uses heap.Item.LessThan to order items. Push and AddOrUpdate both
// use heap.AddOrUpdate semantics: if an item is already queued, its heap
// position is updated instead of inserting a duplicate. If an item's ordering
// fields change while it is queued, callers must call AddOrUpdate again to
// restore heap order.
//
// Pop blocks until an item is available, the context is canceled, or the queue
// is closed and drained. TryPop never blocks: it returns the current top item if
// one exists, or ok=false immediately when the queue is empty.
type PriorityQueue[T heap.Item[T]] struct {
	mu     sync.Mutex
	heap   *heap.Heap[T]
	notify chan struct{}
	closed bool
}

// New creates an empty priority queue.
func New[T heap.Item[T]]() *PriorityQueue[T] {
	return &PriorityQueue[T]{
		heap:   heap.NewHeap[T](),
		notify: make(chan struct{}, 1),
	}
}

// Push adds or updates an item and wakes one blocked Pop caller.
// It returns false if the queue has been closed.
func (q *PriorityQueue[T]) Push(item T) bool {
	return q.AddOrUpdate(item)
}

// AddOrUpdate adds an item if it is not in the queue, or updates its heap
// position if it is already queued.
func (q *PriorityQueue[T]) AddOrUpdate(item T) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	q.heap.AddOrUpdate(item)
	q.notifyOneLocked()
	q.mu.Unlock()
	return true
}

// Pop blocks until an item is available, the queue is closed and drained, or ctx
// is done.
func (q *PriorityQueue[T]) Pop(ctx context.Context) (item T, err error) {
	for {
		q.mu.Lock()
		var ok bool
		item, ok = q.heap.PopTop()
		if ok {
			if q.heap.Len() > 0 {
				q.notifyOneLocked()
			}
			q.mu.Unlock()
			return item, nil
		}
		if q.closed {
			q.mu.Unlock()
			return item, ErrClosed
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return item, ctx.Err()
		case _, open := <-q.notify:
			if !open {
				continue
			}
		}
	}
}

// TryPop removes and returns the top item without blocking.
func (q *PriorityQueue[T]) TryPop() (item T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	item, ok = q.heap.PopTop()
	if ok && q.heap.Len() > 0 {
		q.notifyOneLocked()
	}
	return item, ok
}

// Peek returns the top item without removing it.
func (q *PriorityQueue[T]) Peek() (item T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.heap.PeekTop()
}

// Len returns the number of queued items.
func (q *PriorityQueue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.heap.Len()
}

// Close prevents future pushes and wakes blocked Pop callers. Items already in
// the queue remain available to Pop.
func (q *PriorityQueue[T]) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	close(q.notify)
	q.mu.Unlock()
}

func (q *PriorityQueue[T]) notifyOneLocked() {
	if q.closed {
		return
	}
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
