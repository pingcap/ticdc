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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
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

type pendingUnreplicatingStatusKey struct {
	blockTs     uint64
	isSyncPoint bool
	stage       heartbeatpb.BlockStage
}

type pendingUnreplicatingStatus struct {
	cfID        common.ChangeFeedID
	from        node.ID
	state       *heartbeatpb.State
	firstSeenAt time.Time
	lastSeenAt  time.Time
}

type pendingUnreplicatingStatusEntry struct {
	dispatcherID common.DispatcherID
	key          pendingUnreplicatingStatusKey
	value        *pendingUnreplicatingStatus
}

type pendingUnreplicatingStatusMap struct {
	mutex        sync.Mutex
	byDispatcher map[common.DispatcherID]map[pendingUnreplicatingStatusKey]*pendingUnreplicatingStatus
}

func newPendingScheduleEventMap() *pendingScheduleEventMap {
	return &pendingScheduleEventMap{
		events: make(map[eventKey]*BarrierEvent),
	}
}

func newPendingUnreplicatingStatusMap() *pendingUnreplicatingStatusMap {
	return &pendingUnreplicatingStatusMap{
		byDispatcher: make(map[common.DispatcherID]map[pendingUnreplicatingStatusKey]*pendingUnreplicatingStatus),
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

func (m *pendingUnreplicatingStatusMap) upsert(
	dispatcherID common.DispatcherID,
	key pendingUnreplicatingStatusKey,
	value *pendingUnreplicatingStatus,
) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	statuses, ok := m.byDispatcher[dispatcherID]
	if !ok {
		statuses = make(map[pendingUnreplicatingStatusKey]*pendingUnreplicatingStatus)
		m.byDispatcher[dispatcherID] = statuses
	}
	if existing, ok := statuses[key]; ok {
		value.firstSeenAt = existing.firstSeenAt
	}
	statuses[key] = value
}

func (m *pendingUnreplicatingStatusMap) delete(
	dispatcherID common.DispatcherID,
	key pendingUnreplicatingStatusKey,
) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	statuses, ok := m.byDispatcher[dispatcherID]
	if !ok {
		return
	}
	delete(statuses, key)
	if len(statuses) == 0 {
		delete(m.byDispatcher, dispatcherID)
	}
}

func (m *pendingUnreplicatingStatusMap) snapshot() []pendingUnreplicatingStatusEntry {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	entries := make([]pendingUnreplicatingStatusEntry, 0)
	for dispatcherID, statuses := range m.byDispatcher {
		for key, value := range statuses {
			entries = append(entries, pendingUnreplicatingStatusEntry{
				dispatcherID: dispatcherID,
				key:          key,
				value:        value,
			})
		}
	}
	return entries
}

func (m *pendingUnreplicatingStatusMap) len() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	length := 0
	for _, statuses := range m.byDispatcher {
		length += len(statuses)
	}
	return length
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

func (b *BlockedEventMap) RangeWithoutLock(f func(key eventKey, value *BarrierEvent) bool) {
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

func (b *BlockedEventMap) Len() int {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return len(b.m)
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
