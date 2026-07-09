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

// Package notifyqueue provides a FIFO queue with a selectable ready signal.
package notifyqueue

import "github.com/pingcap/ticdc/utils/deque"

// Queue is a FIFO queue with a wake-up channel.
//
// Queue is not safe for concurrent use. Callers must serialize Push, TryPop,
// Drain, and Len by themselves.
//
// Ready returns a wake-up hint. Receiving from it does not guarantee TryPop
// will return an item, and callers must re-check the queue state after wake-up.
type Queue[T any] struct {
	queue *deque.Deque[T]
	ready chan struct{}
}

// New creates an empty Queue.
func New[T any]() *Queue[T] {
	return &Queue[T]{
		queue: deque.NewDequeDefault[T](),
		ready: make(chan struct{}, 1),
	}
}

// Push appends an item and signals Ready.
func (q *Queue[T]) Push(item T) {
	q.queue.PushBack(item)
	q.signal()
}

// TryPop pops one item from the front of the queue.
func (q *Queue[T]) TryPop() (T, bool) {
	return q.queue.PopFront()
}

// Drain removes and returns all queued items in FIFO order.
func (q *Queue[T]) Drain() []T {
	items := make([]T, 0, q.queue.Length())
	for {
		item, ok := q.queue.PopFront()
		if !ok {
			return items
		}
		items = append(items, item)
	}
}

// Len returns the number of queued items.
func (q *Queue[T]) Len() int {
	return q.queue.Length()
}

// Ready returns a wake-up channel for consumers.
func (q *Queue[T]) Ready() <-chan struct{} {
	return q.ready
}

func (q *Queue[T]) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}
