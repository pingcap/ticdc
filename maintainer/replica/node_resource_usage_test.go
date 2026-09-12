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

func TestNodeResourceUsageTrackerSharesDeltaSnapshot(t *testing.T) {
	now := time.Unix(100, 0)
	tracker := NewNodeResourceUsageTracker()
	tracker.now = func() time.Time { return now }
	nodeIDs := []node.ID{"node1", "node2"}

	tracker.ReplaceEventStoreWriteBytes(map[node.ID]uint64{
		"node1": 100,
		"node2": 200,
	}, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_AVAILABLE)
	_, status := tracker.EventStoreWriteBytesDelta(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_INCOMPLETE, status)

	now = now.Add(time.Second)
	tracker.ReplaceEventStoreWriteBytes(map[node.ID]uint64{
		"node1": 110,
		"node2": 220,
	}, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_AVAILABLE)
	delta, status := tracker.EventStoreWriteBytesDelta(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_AVAILABLE, status)
	require.Equal(t, map[node.ID]uint64{"node1": 10, "node2": 20}, delta)

	deltaAgain, status := tracker.EventStoreWriteBytesDelta(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_AVAILABLE, status)
	require.Equal(t, delta, deltaAgain)
	require.Zero(t, testing.AllocsPerRun(100, func() {
		tracker.EventStoreWriteBytesDelta(nodeIDs)
	}))

	now = now.Add(nodeResourceUsageStaleThreshold + time.Nanosecond)
	_, status = tracker.EventStoreWriteBytesDelta(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_INCOMPLETE, status)

	// Fresh reports after an interruption establish a new baseline. They must
	// not be compared with counters from before the interruption.
	tracker.ReplaceEventStoreWriteBytes(map[node.ID]uint64{
		"node1": 200,
		"node2": 400,
	}, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_AVAILABLE)
	_, status = tracker.EventStoreWriteBytesDelta(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_INCOMPLETE, status)

	now = now.Add(time.Second)
	tracker.ReplaceEventStoreWriteBytes(map[node.ID]uint64{
		"node1": 230,
		"node2": 440,
	}, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_AVAILABLE)
	delta, status = tracker.EventStoreWriteBytesDelta(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_AVAILABLE, status)
	require.Equal(t, map[node.ID]uint64{"node1": 30, "node2": 40}, delta)
}

func TestNodeResourceUsageTrackerDistinguishesUnsupportedAndIncomplete(t *testing.T) {
	tracker := NewNodeResourceUsageTracker()
	nodeIDs := []node.ID{"node1", "node2"}

	_, status := tracker.EventStoreWriteBytesDelta(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_UNSUPPORTED, status)

	tracker.ReplaceEventStoreWriteBytes(nil, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_INCOMPLETE)
	_, status = tracker.EventStoreWriteBytesDelta(nodeIDs)
	require.Equal(t, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_INCOMPLETE, status)
}
