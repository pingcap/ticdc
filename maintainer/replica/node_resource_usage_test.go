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

package replica

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestNodeResourceUsageTrackerRequiresFreshCompleteSnapshot(t *testing.T) {
	now := time.Unix(100, 0)
	tracker := NewNodeResourceUsageTracker()
	tracker.now = func() time.Time { return now }

	tracker.UpdateEventStoreWriteBytes("node1", 100)
	writeBytes, ok := tracker.EventStoreWriteBytes([]node.ID{"node1"})
	require.True(t, ok)
	require.Equal(t, map[node.ID]uint64{"node1": 100}, writeBytes)

	// A node running an older version does not report this counter. In that case
	// the complete snapshot is unavailable and the caller falls back.
	_, ok = tracker.EventStoreWriteBytes([]node.ID{"node1", "node2"})
	require.False(t, ok)

	tracker.UpdateEventStoreWriteBytes("node2", 200)
	now = now.Add(nodeResourceUsageStaleThreshold + time.Nanosecond)
	_, ok = tracker.EventStoreWriteBytes([]node.ID{"node1", "node2"})
	require.False(t, ok)

	tracker.UpdateEventStoreWriteBytes("node1", 300)
	tracker.UpdateEventStoreWriteBytes("node2", 400)
	writeBytes, ok = tracker.EventStoreWriteBytes([]node.ID{"node1", "node2"})
	require.True(t, ok)
	require.Equal(t, map[node.ID]uint64{"node1": 300, "node2": 400}, writeBytes)
}

func TestSplitSpanCheckerRebuildsBaselineAfterStaleSample(t *testing.T) {
	now := time.Unix(100, 0)
	tracker := NewNodeResourceUsageTracker()
	tracker.now = func() time.Time { return now }
	checker := &SplitSpanChecker{nodeResourceUsage: tracker}
	nodeIDs := []node.ID{"node1", "node2"}

	tracker.UpdateEventStoreWriteBytes("node1", 100)
	tracker.UpdateEventStoreWriteBytes("node2", 200)
	_, ok := checker.sampleEventStoreWriteBytes(nodeIDs)
	require.False(t, ok)

	now = now.Add(time.Second)
	tracker.UpdateEventStoreWriteBytes("node1", 110)
	tracker.UpdateEventStoreWriteBytes("node2", 220)
	writeBytes, ok := checker.sampleEventStoreWriteBytes(nodeIDs)
	require.True(t, ok)
	require.Equal(t, map[node.ID]uint64{"node1": 10, "node2": 20}, writeBytes)

	now = now.Add(nodeResourceUsageStaleThreshold + time.Nanosecond)
	_, ok = checker.sampleEventStoreWriteBytes(nodeIDs)
	require.False(t, ok)
	require.Nil(t, checker.lastEventStoreWriteBytes)

	// Fresh reports after an interruption establish a new baseline. They must
	// not be compared with counters from before the interruption.
	tracker.UpdateEventStoreWriteBytes("node1", 200)
	tracker.UpdateEventStoreWriteBytes("node2", 400)
	_, ok = checker.sampleEventStoreWriteBytes(nodeIDs)
	require.False(t, ok)

	now = now.Add(time.Second)
	tracker.UpdateEventStoreWriteBytes("node1", 230)
	tracker.UpdateEventStoreWriteBytes("node2", 440)
	writeBytes, ok = checker.sampleEventStoreWriteBytes(nodeIDs)
	require.True(t, ok)
	require.Equal(t, map[node.ID]uint64{"node1": 30, "node2": 40}, writeBytes)
}
