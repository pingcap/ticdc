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

package eventstore

import (
	"container/list"
	"sync"
)

type notificationTask struct {
	notifierID  NotifierID
	notifier    ResolvedTsNotifier
	resolvedTs  uint64
	maxCommitTs uint64
}

type NotificationQueue struct {
	fastChan chan notificationTask
	mu       sync.RWMutex
	tasks    *list.List
	taskMap  map[NotifierID]*list.Element // 快速查找
	cond     *sync.Cond
}

func NewNotificationQueue() *NotificationQueue {
	q := &NotificationQueue{
		fastChan: make(chan notificationTask, 10000),
		tasks:    list.New(),
		taskMap:  make(map[NotifierID]*list.Element),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *NotificationQueue) Enqueue(task notificationTask) {
	select {
	case q.fastChan <- task:
		return
	default:
		q.mu.Lock()
		defer q.mu.Unlock()

		if elem, exists := q.taskMap[task.notifierID]; exists {
			// 合并任务
			existing := elem.Value.(notificationTask)
			if task.resolvedTs > existing.resolvedTs {
				existing.resolvedTs = task.resolvedTs
			}
			if task.maxCommitTs > existing.maxCommitTs {
				existing.maxCommitTs = task.maxCommitTs
			}
			elem.Value = existing
		} else {
			// 添加新任务
			elem := q.tasks.PushBack(task)
			q.taskMap[task.notifierID] = elem
		}

		q.cond.Signal()
	}
}

func (q *NotificationQueue) Dequeue() (notificationTask, bool) {
	select {
	case task := <-q.fastChan:
		return task, true
	default:
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for q.tasks.Len() == 0 {
		q.cond.Wait()
	}

	front := q.tasks.Front()
	task := front.Value.(notificationTask)
	q.tasks.Remove(front)
	delete(q.taskMap, task.notifierID)

	return task, true
}
