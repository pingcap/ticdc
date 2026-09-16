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

// checkpointTsTracker maintains the minimum checkpointTs among non-DDL spans
// that are not replicating yet. The owning SpanController must hold its mutex
// when accessing this tracker.
type checkpointTsTracker struct {
	// checkpointTsBySpanID contains exactly the non-DDL spans that are absent or
	// scheduling. Replicating spans are removed because they no longer block the
	// non-replicating minimum checkpoint.
	checkpointTsBySpanID map[common.DispatcherID]uint64

	// checkpointTsRefCounts tracks how many spans currently hold each checkpointTs.
	// The heap stores each checkpointTs once, so the count keeps duplicate values
	// from being removed too early.
	checkpointTsRefCounts map[uint64]int

	// minCheckpointTsHeap stores unique checkpointTs values and gives O(1) access
	// to the current minimum. Insertions and removals are O(log n).
	minCheckpointTsHeap checkpointTsHeap
}

func newCheckpointTsTracker() *checkpointTsTracker {
	return &checkpointTsTracker{
		checkpointTsBySpanID:  make(map[common.DispatcherID]uint64),
		checkpointTsRefCounts: make(map[uint64]int),
		minCheckpointTsHeap:   newCheckpointTsHeap(),
	}
}

// trackSpan records a span that has entered a non-replicating state. It also
// handles duplicate calls for the same span by replacing the old checkpointTs.
func (t *checkpointTsTracker) trackSpan(id common.DispatcherID, checkpointTs uint64) {
	if old, ok := t.checkpointTsBySpanID[id]; ok {
		if old == checkpointTs {
			return
		}
		t.decrement(old)
	}
	t.checkpointTsBySpanID[id] = checkpointTs
	t.increment(checkpointTs)
}

// updateTrackedSpan updates checkpointTs only for spans that are already
// tracked. Missing spans are ignored because DDL or replicating spans are not
// part of the non-replicating minimum.
func (t *checkpointTsTracker) updateTrackedSpan(id common.DispatcherID, checkpointTs uint64) {
	old, ok := t.checkpointTsBySpanID[id]
	if !ok || old == checkpointTs {
		return
	}
	t.decrement(old)
	t.checkpointTsBySpanID[id] = checkpointTs
	t.increment(checkpointTs)
}

// untrackSpan removes a span after it becomes replicating or leaves the
// controller. Missing spans are ignored for the same reason as updateTrackedSpan.
func (t *checkpointTsTracker) untrackSpan(id common.DispatcherID) {
	old, ok := t.checkpointTsBySpanID[id]
	if !ok {
		return
	}
	delete(t.checkpointTsBySpanID, id)
	t.decrement(old)
	if len(t.checkpointTsBySpanID) == 0 {
		// Release large maps after a bootstrap wave drains. A 1M-table changefeed
		// can otherwise retain the tracker backing storage for its whole lifetime.
		t.reset()
	}
}

// min returns the current minimum checkpointTs among tracked spans.
func (t *checkpointTsTracker) min() (uint64, bool) {
	if t.minCheckpointTsHeap.Len() == 0 {
		return 0, false
	}
	return t.minCheckpointTsHeap.peek(), true
}

func (t *checkpointTsTracker) increment(checkpointTs uint64) {
	if t.checkpointTsRefCounts[checkpointTs] > 0 {
		t.checkpointTsRefCounts[checkpointTs]++
		return
	}
	t.checkpointTsRefCounts[checkpointTs] = 1
	heap.Push(&t.minCheckpointTsHeap, checkpointTs)
}

func (t *checkpointTsTracker) decrement(checkpointTs uint64) {
	count := t.checkpointTsRefCounts[checkpointTs]
	if count <= 1 {
		delete(t.checkpointTsRefCounts, checkpointTs)
		t.minCheckpointTsHeap.remove(checkpointTs)
		return
	}
	t.checkpointTsRefCounts[checkpointTs] = count - 1
}

func (t *checkpointTsTracker) reset() {
	t.checkpointTsBySpanID = make(map[common.DispatcherID]uint64)
	t.checkpointTsRefCounts = make(map[uint64]int)
	t.minCheckpointTsHeap = newCheckpointTsHeap()
}

// checkpointTsHeap is a removable min-heap for unique checkpointTs values.
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
