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

package span

import (
	"container/heap"

	"github.com/pingcap/ticdc/pkg/common"
)

type checkpointTsTracker struct {
	// byID contains exactly the non-DDL spans that are absent or scheduling.
	// The owning Controller must hold its mutex when accessing this tracker.
	byID   map[common.DispatcherID]uint64
	counts map[uint64]int
	heap   checkpointTsHeap
}

func newCheckpointTsTracker() *checkpointTsTracker {
	return &checkpointTsTracker{
		byID:   make(map[common.DispatcherID]uint64),
		counts: make(map[uint64]int),
		heap:   newCheckpointTsHeap(),
	}
}

func (t *checkpointTsTracker) addOrUpdate(id common.DispatcherID, checkpointTs uint64) {
	if old, ok := t.byID[id]; ok {
		if old == checkpointTs {
			return
		}
		t.decrement(old)
	}
	t.byID[id] = checkpointTs
	t.increment(checkpointTs)
}

func (t *checkpointTsTracker) update(id common.DispatcherID, checkpointTs uint64) {
	old, ok := t.byID[id]
	if !ok || old == checkpointTs {
		return
	}
	t.decrement(old)
	t.byID[id] = checkpointTs
	t.increment(checkpointTs)
}

func (t *checkpointTsTracker) remove(id common.DispatcherID) {
	old, ok := t.byID[id]
	if !ok {
		return
	}
	delete(t.byID, id)
	t.decrement(old)
	if len(t.byID) == 0 {
		t.reset()
	}
}

func (t *checkpointTsTracker) min() (uint64, bool) {
	if t.heap.Len() == 0 {
		return 0, false
	}
	return t.heap.peek(), true
}

func (t *checkpointTsTracker) increment(checkpointTs uint64) {
	if t.counts[checkpointTs] > 0 {
		t.counts[checkpointTs]++
		return
	}
	t.counts[checkpointTs] = 1
	heap.Push(&t.heap, checkpointTs)
}

func (t *checkpointTsTracker) decrement(checkpointTs uint64) {
	count := t.counts[checkpointTs]
	if count <= 1 {
		delete(t.counts, checkpointTs)
		t.heap.remove(checkpointTs)
		return
	}
	t.counts[checkpointTs] = count - 1
}

func (t *checkpointTsTracker) reset() {
	t.byID = make(map[common.DispatcherID]uint64)
	t.counts = make(map[uint64]int)
	t.heap = newCheckpointTsHeap()
}

type checkpointTsHeap struct {
	values  []uint64
	indexes map[uint64]int
}

func newCheckpointTsHeap() checkpointTsHeap {
	return checkpointTsHeap{
		indexes: make(map[uint64]int),
	}
}

func (h checkpointTsHeap) Len() int {
	return len(h.values)
}

func (h checkpointTsHeap) Less(i, j int) bool {
	return h.values[i] < h.values[j]
}

func (h checkpointTsHeap) Swap(i, j int) {
	h.values[i], h.values[j] = h.values[j], h.values[i]
	h.indexes[h.values[i]] = i
	h.indexes[h.values[j]] = j
}

func (h *checkpointTsHeap) Push(x any) {
	checkpointTs := x.(uint64)
	h.indexes[checkpointTs] = len(h.values)
	h.values = append(h.values, checkpointTs)
}

func (h *checkpointTsHeap) Pop() any {
	n := len(h.values)
	checkpointTs := h.values[n-1]
	delete(h.indexes, checkpointTs)
	h.values[n-1] = 0
	h.values = h.values[:n-1]
	return checkpointTs
}

func (h *checkpointTsHeap) peek() uint64 {
	return h.values[0]
}

func (h *checkpointTsHeap) remove(checkpointTs uint64) {
	index, ok := h.indexes[checkpointTs]
	if !ok {
		return
	}
	heap.Remove(h, index)
}
