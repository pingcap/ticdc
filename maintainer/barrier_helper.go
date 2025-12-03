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

package maintainer

import (
	"container/heap"
	"sync"

	"github.com/pingcap/ticdc/pkg/common"
)

type BlockedEventMap struct {
	mutex sync.Mutex
	m     map[eventKey]*BarrierEvent
}

func NewBlockEventMap() *BlockedEventMap {
	return &BlockedEventMap{
		m: make(map[eventKey]*BarrierEvent),
	}
}

type pendingScheduleEventMap struct {
	mutex  sync.Mutex
	queues map[common.DispatcherID]*dispatcherPendingQueue
}

func newPendingScheduleEventMap() *pendingScheduleEventMap {
	return &pendingScheduleEventMap{
		queues: make(map[common.DispatcherID]*dispatcherPendingQueue),
	}
}

type dispatcherPendingQueue struct {
	heap pendingEventHeap
	set  map[*BarrierEvent]struct{}
}

func newDispatcherPendingQueue() *dispatcherPendingQueue {
	return &dispatcherPendingQueue{
		set: make(map[*BarrierEvent]struct{}),
	}
}

func (q *dispatcherPendingQueue) empty() bool {
	return len(q.heap) == 0
}

func (q *dispatcherPendingQueue) head() *BarrierEvent {
	if len(q.heap) == 0 {
		return nil
	}
	return q.heap[0]
}

func (q *dispatcherPendingQueue) add(event *BarrierEvent) {
	if _, ok := q.set[event]; ok {
		return
	}
	heap.Push(&q.heap, event)
	q.set[event] = struct{}{}
}

func (q *dispatcherPendingQueue) popIfHead(event *BarrierEvent) (bool, *BarrierEvent) {
	head := q.head()
	if head == nil {
		return false, nil
	}
	if head != event {
		return false, head
	}
	heap.Pop(&q.heap)
	delete(q.set, event)
	return true, nil
}

func (m *pendingScheduleEventMap) add(event *BarrierEvent) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	queue := m.queues[event.writerDispatcher]
	if queue == nil {
		queue = newDispatcherPendingQueue()
		m.queues[event.writerDispatcher] = queue
	}
	queue.add(event)
}

func (m *pendingScheduleEventMap) popIfHead(event *BarrierEvent) (bool, *BarrierEvent) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	queue, ok := m.queues[event.writerDispatcher]
	if !ok || queue.empty() {
		return false, nil
	}
	ready, blocking := queue.popIfHead(event)
	if queue.empty() {
		delete(m.queues, event.writerDispatcher)
	}
	return ready, blocking
}

type pendingEventHeap []*BarrierEvent

func (h pendingEventHeap) Len() int { return len(h) }

func (h pendingEventHeap) Less(i, j int) bool {
	return compareBarrierEvent(h[i], h[j]) < 0
}

func (h pendingEventHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *pendingEventHeap) Push(x any) {
	*h = append(*h, x.(*BarrierEvent))
}

func (h *pendingEventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func compareBarrierEvent(a, b *BarrierEvent) int {
	if a.commitTs < b.commitTs {
		return -1
	}
	if a.commitTs > b.commitTs {
		return 1
	}
	if a.isSyncPoint == b.isSyncPoint {
		return 0
	}
	if !a.isSyncPoint && b.isSyncPoint {
		return -1
	}
	return 1
}

func (b *BlockedEventMap) Range(f func(key eventKey, value *BarrierEvent) bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for k, v := range b.m {
		if !f(k, v) {
			break
		}
	}
}

func (b *BlockedEventMap) RangeWoLock(f func(key eventKey, value *BarrierEvent) bool) {
	for k, v := range b.m {
		if !f(k, v) {
			break
		}
	}
}

func (b *BlockedEventMap) Get(key eventKey) (*BarrierEvent, bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	event, ok := b.m[key]
	return event, ok
}

func (b *BlockedEventMap) Set(key eventKey, event *BarrierEvent) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.m[key] = event
}

func (b *BlockedEventMap) Delete(key eventKey) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	delete(b.m, key)
}

// eventKey is the key of the block event,
// the ddl and sync point are identified by the blockTs and isSyncPoint since they can share the same blockTs
type eventKey struct {
	blockTs     uint64
	isSyncPoint bool
}
