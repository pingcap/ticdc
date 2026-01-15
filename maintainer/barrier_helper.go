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

	"github.com/pingcap/log"
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

// pendingScheduleEventMap keeps pending BarrierEvents keyed by eventKey
// since only DDLs related add/drop/truncate/recover etc. tables need scheduling, and those events will always be written
// by the table trigger dispatcher, we can store only the eventKey heap with a map to the actual event.
type pendingScheduleEventMap struct {
	mutex  sync.Mutex
	queue  pendingEventKeyHeap
	events map[eventKey]*BarrierEvent
}

func newPendingScheduleEventMap() *pendingScheduleEventMap {
	return &pendingScheduleEventMap{
		events: make(map[eventKey]*BarrierEvent),
	}
}

func (m *pendingScheduleEventMap) Len() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return len(m.events)
}

func (m *pendingScheduleEventMap) add(event *BarrierEvent) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	key := getEventKey(event.commitTs, event.isSyncPoint)
	if _, ok := m.events[key]; ok {
		return
	}
	heap.Push(&m.queue, key)
	m.events[key] = event
}

func (m *pendingScheduleEventMap) popIfHead(event *BarrierEvent) (bool, *BarrierEvent) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(m.queue) == 0 {
		return false, nil
	}
	headKey := m.queue[0]
	candidate := m.events[headKey]
	if candidate == nil {
		log.Panic("candidate is nil")
	}
	eventKey := getEventKey(event.commitTs, event.isSyncPoint)
	if headKey != eventKey {
		return false, candidate
	}
	heap.Pop(&m.queue)
	delete(m.events, headKey)
	return true, candidate
}

type pendingEventKeyHeap []eventKey

func (h pendingEventKeyHeap) Len() int { return len(h) }

func (h pendingEventKeyHeap) Less(i, j int) bool {
	return compareEventKey(h[i], h[j]) < 0
}

func (h pendingEventKeyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *pendingEventKeyHeap) Push(x any) {
	*h = append(*h, x.(eventKey))
}

func (h *pendingEventKeyHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func compareEventKey(a, b eventKey) int {
	if a.blockTs < b.blockTs {
		return -1
	}
	if a.blockTs > b.blockTs {
		return 1
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

// getEventKey returns the key of the block event
func getEventKey(blockTs uint64, isSyncPoint bool) eventKey {
	return eventKey{
		blockTs:     blockTs,
		isSyncPoint: isSyncPoint,
	}
}
