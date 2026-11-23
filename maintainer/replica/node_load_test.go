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
	"math"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/resource"
	"github.com/stretchr/testify/require"
)

func TestNodeLoadTracker(t *testing.T) {
	now := time.Now()
	nodes := map[node.ID]*node.Info{"a": {ID: "a"}, "b": {ID: "b"}}
	tracker := NewNodeLoadTracker()
	_, status := tracker.Snapshot(nodes, now)
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, status)
	loads := []*heartbeatpb.NodeLoad{{NodeId: "a", BytesPerSecond: 100}, {NodeId: "b"}}
	tracker.Replace(loads, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, now)
	rates, status := tracker.Snapshot(nodes, now)
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, status)
	require.Equal(t, float64(100), rates["a"][resource.Sink])
	// Mutating a protobuf or replacing the tracker never mutates old snapshots.
	loads[0].BytesPerSecond = 200
	tracker.Replace(loads, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, now)
	require.Equal(t, float64(100), rates["a"][resource.Sink])
	_, status = tracker.Snapshot(nodes, now.Add(NodeLoadStaleThreshold+1))
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, status)
	nodes["new"] = &node.Info{ID: "new"}
	_, status = tracker.Snapshot(nodes, now)
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, status)
	delete(nodes, "new")
	// Resource vectors must be complete and must not alias incoming protobufs.
	loads = []*heartbeatpb.NodeLoad{
		resource.Load{resource.Sink: 100, resource.EventStoreRead: 200}.ToNodeLoad("a"),
		resource.Load{resource.Sink: 0, resource.EventStoreRead: 300}.ToNodeLoad("b"),
	}
	tracker.Replace(loads, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, now)
	rates, status = tracker.Snapshot(nodes, now)
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, status)
	loads[0].Resources[1].BytesPerSecond = 999
	require.Equal(t, float64(200), rates["a"][resource.EventStoreRead])
	loads[1] = resource.Load{resource.Sink: 0}.ToNodeLoad("b")
	tracker.Replace(loads, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, now)
	_, status = tracker.Snapshot(nodes, now)
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, status)
	for _, invalid := range [][]*heartbeatpb.NodeLoad{
		{nil},
		{{NodeId: ""}},
		{{NodeId: "a", BytesPerSecond: -1}},
		{{NodeId: "a", BytesPerSecond: math.NaN()}},
		{{NodeId: "a", BytesPerSecond: math.Inf(1)}},
		{{NodeId: "a"}, {NodeId: "a"}},
	} {
		tracker.Replace(invalid, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, now)
		_, status = tracker.Snapshot(nodes, now)
		require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, status)
	}
	tracker.Replace(nil, heartbeatpb.NodeLoadStatus_LOAD_UNSUPPORTED, now)
	_, status = tracker.Snapshot(nodes, now.Add(time.Hour))
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_UNSUPPORTED, status)
}

func TestSpanTrafficSamples(t *testing.T) {
	r := NewSpanReplication(common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName), common.NewDispatcherID(), 1, getTableSpanByID(4), 10, 0, false)
	now := time.Now()
	require.Zero(t, r.TrafficRate(now))
	r.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 11, EventSizePerSecond: 100})
	require.Equal(t, float64(100), r.TrafficRate(time.Now()))
	// Status-only heartbeats and rejected old checkpoints retain the sample.
	r.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 12})
	r.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 9, EventSizePerSecond: 200})
	require.Equal(t, float64(100), r.TrafficRate(time.Now()))
	// Complete dispatcher samples arrive every ten seconds, independently of
	// the more frequent node heartbeat carrying the shared resource view.
	require.Equal(t, float64(100), r.TrafficRate(time.Now().Add(heartbeatpb.DispatcherTrafficSampleInterval)))
	require.Zero(t, r.TrafficRate(time.Now().Add(DispatcherTrafficStaleThreshold+time.Second)))
	r.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 12, EventSizePerSecond: 1})
	require.Equal(t, float64(1), r.TrafficRate(time.Now()))
	r.SetNodeID("another")
	require.Zero(t, r.TrafficRate(time.Now()))
}
