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

// spanInitializationTracker records the ranges that have received an
// INITIALIZED event. The recorded coverage is monotonic and is never removed.
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

	initialized, ok := intersectInitializedRange(totalSpan, initializedSpan)
	if !ok {
		return false
	}
	if t.ranges == nil {
		t.ranges = btree.NewG(16, initializedRangeLess)
	}

	merged := initialized
	pivot := initializedRange{startKey: initialized.startKey}
	toDelete := make([]initializedRange, 0, 2)

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
