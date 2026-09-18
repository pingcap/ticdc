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

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestNodeResourceUsageTrackerSharesRateSnapshot(t *testing.T) {
	now := time.Unix(100, 0)
	tracker := NewNodeResourceUsageTracker()
	tracker.now = func() time.Time { return now }
	nodeIDs := []node.ID{"node1", "node2"}

	tracker.ReplaceEventStoreWriteBytesPerSecond([]*heartbeatpb.NodeResourceUsage{
		{NodeId: "node1", EventStoreWriteBytesPerSecond: 10},
		{NodeId: "node2", EventStoreWriteBytesPerSecond: 20},
	}, heartbeatpb.NodeResourceUsageStatus_AVAILABLE)
	rate, status, generation := tracker.EventStoreWriteBytesPerSecond(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_AVAILABLE, status)
	require.Equal(t, map[node.ID]uint64{"node1": 10, "node2": 20}, rate)
	require.Equal(t, uint64(1), generation)

	rateAgain, status, generationAgain := tracker.EventStoreWriteBytesPerSecond(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_AVAILABLE, status)
	require.Equal(t, rate, rateAgain)
	require.Equal(t, generation, generationAgain)
	require.Zero(t, testing.AllocsPerRun(100, func() {
		tracker.EventStoreWriteBytesPerSecond(nodeIDs)
	}))

	now = now.Add(nodeResourceUsageStaleThreshold + time.Nanosecond)
	_, status, generationAgain = tracker.EventStoreWriteBytesPerSecond(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_INCOMPLETE, status)
	require.Equal(t, generation, generationAgain)

	tracker.ReplaceEventStoreWriteBytesPerSecond([]*heartbeatpb.NodeResourceUsage{
		{NodeId: "node1", EventStoreWriteBytesPerSecond: 10},
		{NodeId: "node2", EventStoreWriteBytesPerSecond: 20},
	}, heartbeatpb.NodeResourceUsageStatus_AVAILABLE)
	_, status, generationAgain = tracker.EventStoreWriteBytesPerSecond(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_AVAILABLE, status)
	require.Equal(t, generation+1, generationAgain)
}

func TestNodeResourceUsageTrackerDistinguishesUnsupportedAndIncomplete(t *testing.T) {
	tracker := NewNodeResourceUsageTracker()
	nodeIDs := []node.ID{"node1", "node2"}

	_, status, _ := tracker.EventStoreWriteBytesPerSecond(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_INCOMPLETE, status)

	tracker.ReplaceEventStoreWriteBytesPerSecond(nil, heartbeatpb.NodeResourceUsageStatus_UNSUPPORTED)
	_, status, _ = tracker.EventStoreWriteBytesPerSecond(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_UNSUPPORTED, status)

	tracker.ReplaceEventStoreWriteBytesPerSecond(nil, heartbeatpb.NodeResourceUsageStatus_INCOMPLETE)
	_, status, _ = tracker.EventStoreWriteBytesPerSecond(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_INCOMPLETE, status)
}
