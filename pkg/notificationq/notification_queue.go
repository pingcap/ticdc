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
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// NotificationTask represents a generic notification task with a key and payload.
// Key must be comparable and is used for task deduplication and merging.
type NotificationTask[K comparable, V any] struct {
	Key         K
	Payload     V
	EnqueueTime time.Time
}

type QueueStats struct {
	TotalEnqueued   int64
	TotalDequeued   int64
	TotalMerged     int64
	MaxInternalSize int64
	TotalWaitTime   time.Duration
}

// NotificationQueue is a high-performance queue that blocks on fastChan
// when the internal queue is empty, ensuring no missed notifications.
type NotificationQueue[K comparable, V any] struct {
	fastChan   chan NotificationTask[K, V] // Fast path for low latency
	internalMu sync.Mutex                  // Protects internal structures
	tasks      *list.List                  // Internal queue for merged tasks
	taskMap    map[K]*list.Element         // Quick lookup for merging
	hasTasks   chan struct{}               // Signals when internal tasks are available
	stats      QueueStats                  // Performance statistics
	closed     atomic.Bool                 // Atomic flag for closed state
}

// NewNotificationQueue creates a new notification queue
func NewNotificationQueue[K comparable, V any](bufferSize int) *NotificationQueue[K, V] {
	return &NotificationQueue[K, V]{
		fastChan: make(chan NotificationTask[K, V], bufferSize),
		tasks:    list.New(),
		taskMap:  make(map[K]*list.Element),
		hasTasks: make(chan struct{}, 1), // Buffered to avoid missing signals
		stats:    QueueStats{},
	}
}

func (q *NotificationQueue[K, V]) Close() {
	q.closed.Store(true)
	select {
	case q.hasTasks <- struct{}{}:
	default:
	}
}

// Dequeue retrieves a task from the queue, blocking if no tasks are available
// return false when ctx done
func (q *NotificationQueue[K, V]) Dequeue(ctx context.Context) (NotificationTask[K, V], bool) {
	select {
	case task := <-q.fastChan:
		atomic.AddInt64(&q.stats.TotalDequeued, 1)
		return task, true
	default:
	}

	q.internalMu.Lock()
	if q.tasks.Len() > 0 {
		front := q.tasks.Front()
		task := front.Value.(NotificationTask[K, V])
		q.tasks.Remove(front)
		delete(q.taskMap, task.Key)
		q.internalMu.Unlock()

		atomic.AddInt64(&q.stats.TotalDequeued, 1)
		return task, true
	}
	q.internalMu.Unlock()

	if q.closed.Load() {
		return NotificationTask[K, V]{}, false
	}

	for {
		select {
		case <-ctx.Done():
			return NotificationTask[K, V]{}, false
		case task := <-q.fastChan:
			atomic.AddInt64(&q.stats.TotalDequeued, 1)
			return task, true
		case <-q.hasTasks:
			if q.closed.Load() {
				return NotificationTask[K, V]{}, false
			}
			q.internalMu.Lock()
			if q.tasks.Len() > 0 {
				front := q.tasks.Front()
				task := front.Value.(NotificationTask[K, V])
				q.tasks.Remove(front)
				delete(q.taskMap, task.Key)
				q.internalMu.Unlock()

				atomic.AddInt64(&q.stats.TotalDequeued, 1)
				return task, true
			}
			q.internalMu.Unlock()
		}
	}
}

// Enqueue adds a task to the queue with merging support
func (q *NotificationQueue[K, V]) Enqueue(task NotificationTask[K, V], mergeFunc func(existing, new V) V) {
	if q.closed.Load() {
		return
	}

	task.EnqueueTime = time.Now()

	select {
	case q.fastChan <- task:
		atomic.AddInt64(&q.stats.TotalEnqueued, 1)
		return
	default:
	}

	q.internalMu.Lock()
	defer q.internalMu.Unlock()

	if q.closed.Load() {
		return
	}

	atomic.AddInt64(&q.stats.TotalEnqueued, 1)

	wasEmpty := q.tasks.Len() == 0

	if elem, exists := q.taskMap[task.Key]; exists {
		atomic.AddInt64(&q.stats.TotalMerged, 1)
		existingTask := elem.Value.(NotificationTask[K, V])
		existingTask.Payload = mergeFunc(existingTask.Payload, task.Payload)
		elem.Value = existingTask
	} else {
		elem := q.tasks.PushBack(task)
		q.taskMap[task.Key] = elem
		if size := int64(q.tasks.Len()); size > atomic.LoadInt64(&q.stats.MaxInternalSize) {
			atomic.StoreInt64(&q.stats.MaxInternalSize, size)
		}
	}

	if wasEmpty {
		select {
		case q.hasTasks <- struct{}{}:
		default:
		}
	}
}
