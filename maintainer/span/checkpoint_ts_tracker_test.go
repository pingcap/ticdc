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
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
)

func TestCheckpointTsTrackerMin(t *testing.T) {
	t.Parallel()

	tracker := newCheckpointTsTracker()
	id1 := common.NewDispatcherID()
	id2 := common.NewDispatcherID()
	id3 := common.NewDispatcherID()

	tracker.addOrUpdate(id1, 100)
	tracker.addOrUpdate(id2, 80)
	tracker.addOrUpdate(id3, 80)

	got, ok := tracker.min()
	if !ok || got != 80 {
		t.Fatalf("checkpointTsTracker.min() = %d, %v, want 80, true", got, ok)
	}

	tracker.update(id2, 120)
	got, ok = tracker.min()
	if !ok || got != 80 {
		t.Fatalf("checkpointTsTracker.min() after one duplicate update = %d, %v, want 80, true", got, ok)
	}

	tracker.remove(id3)
	got, ok = tracker.min()
	if !ok || got != 100 {
		t.Fatalf("checkpointTsTracker.min() after removing duplicate min = %d, %v, want 100, true", got, ok)
	}

	tracker.remove(id1)
	got, ok = tracker.min()
	if !ok || got != 120 {
		t.Fatalf("checkpointTsTracker.min() after removing current min = %d, %v, want 120, true", got, ok)
	}

	tracker.remove(id2)
	got, ok = tracker.min()
	if ok || got != 0 {
		t.Fatalf("checkpointTsTracker.min() after removing all = %d, %v, want 0, false", got, ok)
	}
}

func TestCheckpointTsTrackerIgnoresMissingUpdate(t *testing.T) {
	t.Parallel()

	tracker := newCheckpointTsTracker()
	id := common.NewDispatcherID()
	tracker.update(id, 100)
	tracker.remove(id)

	got, ok := tracker.min()
	if ok || got != 0 {
		t.Fatalf("checkpointTsTracker.min() after missing update = %d, %v, want 0, false", got, ok)
	}
}

func TestCheckpointTsTrackerRemovesStaleCheckpointTs(t *testing.T) {
	t.Parallel()

	tracker := newCheckpointTsTracker()
	blockingID := common.NewDispatcherID()
	movingID := common.NewDispatcherID()
	tracker.addOrUpdate(blockingID, 1)
	tracker.addOrUpdate(movingID, 2)

	for checkpointTs := uint64(3); checkpointTs < 100; checkpointTs++ {
		tracker.update(movingID, checkpointTs)
	}

	if got := tracker.heap.Len(); got != 2 {
		t.Fatalf("checkpointTsTracker heap size = %d, want 2", got)
	}

	tracker.remove(blockingID)
	got, ok := tracker.min()
	if !ok || got != 99 {
		t.Fatalf("checkpointTsTracker.min() after removing blocker = %d, %v, want 99, true", got, ok)
	}
}
