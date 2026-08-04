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

package logpuller

import (
	"bytes"
	"sync"

	"github.com/google/btree"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
)

type initializedRange struct {
	startKey []byte
	endKey   []byte
}

func initializedRangeLess(lhs, rhs initializedRange) bool {
	return common.StartCompare(lhs.startKey, rhs.startKey) < 0
}

// spanInitializationTracker records the union of ranges that have received an
// INITIALIZED event. The coverage is monotonic: a duplicate or stale event may
// add coverage, but a retry or a region split never removes coverage. This is
// intentional because subscribedSpan.initialized only means that every key in
// the span has completed an initial scan at least once.
//
// ranges contains disjoint and non-adjacent intervals ordered by start key. To
// add an interval, the tracker clamps it to the subscribed span, merges it with
// a connected predecessor and all connected successors, then inserts the
// merged interval. The span is complete exactly when the merged interval
// covers totalSpan. Once complete, the tree is released because the state can
// never become incomplete again.
//
// An insertion costs O((K+1)log N), where K is the number of intervals merged
// by this insertion. Since every merged interval is deleted, the total number
// of merges is amortized across all INITIALIZED events. The worst-case memory
// usage is O(N) for N disjoint initialized ranges.
type spanInitializationTracker struct {
	mu        sync.Mutex
	ranges    *btree.BTreeG[initializedRange]
	completed bool
}

// add records an initialized range and returns true when it completes the
// coverage of totalSpan for the first time.
func (t *spanInitializationTracker) add(
	totalSpan, initializedSpan heartbeatpb.TableSpan,
) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.completed || common.IsEmptySpan(totalSpan) || common.IsEmptySpan(initializedSpan) {
		return false
	}

	merged, ok := intersectInitializedRange(totalSpan, initializedSpan)
	if !ok {
		return false
	}
	if t.ranges == nil {
		t.ranges = btree.NewG(16, initializedRangeLess)
	}

	pivot := initializedRange{startKey: merged.startKey}
	toDelete := make([]initializedRange, 0, 2)

	// Only the closest predecessor can overlap or touch initialized because the
	// intervals already stored in the tree are disjoint and non-adjacent.
	var predecessor initializedRange
	hasPredecessor := false
	t.ranges.DescendLessOrEqual(pivot, func(item initializedRange) bool {
		predecessor = item
		hasPredecessor = true
		return false
	})
	if hasPredecessor && initializedRangesConnected(predecessor, merged) {
		merged.startKey = predecessor.startKey
		if common.EndCompare(predecessor.endKey, merged.endKey) > 0 {
			merged.endKey = predecessor.endKey
		}
		toDelete = append(toDelete, predecessor)
	}

	// Merge consecutive successors until the first gap. Once a gap is found,
	// later intervals cannot be connected because the tree is start-key ordered.
	t.ranges.AscendGreaterOrEqual(pivot, func(item initializedRange) bool {
		if hasPredecessor && common.StartCompare(item.startKey, predecessor.startKey) == 0 {
			return true
		}
		if !initializedRangesConnected(merged, item) {
			return false
		}
		if common.EndCompare(item.endKey, merged.endKey) > 0 {
			merged.endKey = item.endKey
		}
		toDelete = append(toDelete, item)
		return true
	})

	for _, item := range toDelete {
		t.ranges.Delete(item)
	}
	t.ranges.ReplaceOrInsert(merged)

	// All recorded intervals are clamped to totalSpan. Therefore one interval
	// with both boundaries equal to totalSpan proves complete coverage.
	if common.StartCompare(merged.startKey, totalSpan.StartKey) == 0 &&
		common.EndCompare(merged.endKey, totalSpan.EndKey) == 0 {
		t.completed = true
		t.ranges = nil
		return true
	}
	return false
}

func intersectInitializedRange(
	totalSpan, initializedSpan heartbeatpb.TableSpan,
) (initializedRange, bool) {
	startKey := totalSpan.StartKey
	if common.StartCompare(initializedSpan.StartKey, startKey) > 0 {
		startKey = initializedSpan.StartKey
	}
	endKey := totalSpan.EndKey
	if common.EndCompare(initializedSpan.EndKey, endKey) < 0 {
		endKey = initializedSpan.EndKey
	}
	if len(startKey) != 0 && len(endKey) != 0 && bytes.Compare(startKey, endKey) >= 0 {
		return initializedRange{}, false
	}
	return initializedRange{startKey: startKey, endKey: endKey}, true
}

func initializedRangesConnected(lhs, rhs initializedRange) bool {
	if len(lhs.endKey) == 0 || len(rhs.startKey) == 0 {
		return true
	}
	return bytes.Compare(lhs.endKey, rhs.startKey) >= 0
}
