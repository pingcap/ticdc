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

package coordinator

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestNodeLoadHeartbeatSnapshot(t *testing.T) {
	now := time.Unix(100, 0)
	c := newCaptureWriteLeaseController(10, "coordinator")
	c.now = func() time.Time { return now }
	enableP2PForNodes(c, "a", "b")
	report := func(id node.ID, seq, bytes uint64) *heartbeatpb.NodeHeartbeatResponse {
		hb := newWriteLeaseHeartbeat(1, seq)
		hb.NodeLoad = &heartbeatpb.NodeLoad{FlushedEventBytes: bytes}
		messages := c.handleHeartbeat(id, hb, nil)
		require.Len(t, messages, 1)
		return requireWriteLeaseResponse(t, messages[0])
	}
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_UNSUPPORTED, report("a", 1, 100).NodeLoadStatus)
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, report("b", 1, 1000).NodeLoadStatus)
	now = now.Add(time.Second)
	report("a", 2, 200)
	response := report("b", 2, 1200)
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, response.NodeLoadStatus)
	require.Equal(t, []*heartbeatpb.NodeLoad{
		{NodeId: "a", BytesPerSecond: 100}, {NodeId: "b", BytesPerSecond: 200},
	}, response.NodeLoads)

	// A new a sample does not turn b's unchanged cumulative counter into zero.
	now = now.Add(2 * time.Second)
	response = report("a", 3, 400)
	require.Equal(t, float64(100), response.NodeLoads[0].BytesPerSecond)
	require.Equal(t, float64(200), response.NodeLoads[1].BytesPerSecond)
	// Replayed reports cannot replace the sample, even with another counter.
	hb := newWriteLeaseHeartbeat(1, 3)
	hb.NodeLoad = &heartbeatpb.NodeLoad{FlushedEventBytes: 9999}
	require.Empty(t, c.handleHeartbeat("a", hb, nil))
	require.Equal(t, uint64(400), c.nodes["a"].load.bytes)

	now = now.Add(nodeLoadStaleThreshold)
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, report("a", 4, 900).NodeLoadStatus)
	// A stale sample needs a new baseline before its rate becomes available.
	report("b", 3, 2000)
	now = now.Add(time.Second)
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, report("b", 4, 2200).NodeLoadStatus)
	// Counter reset also requires a new baseline.
	now = now.Add(time.Second)
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, report("b", 5, 10).NodeLoadStatus)
	now = now.Add(time.Second)
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, report("b", 6, 110).NodeLoadStatus)

	c.updateClusterMode([]node.ID{"a", "b", "new"})
	_, status := c.nodeLoadSnapshot()
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_UNSUPPORTED, status)
	c.removeNode("new")
	_, status = c.nodeLoadSnapshot()
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, status)
	// A new process with the same ID must not inherit the previous baseline.
	c.removeNode("b")
	c.updateClusterMode([]node.ID{"a", "b"})
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, report("b", 1, 5).NodeLoadStatus)
}

func TestNodeLoadKeepsSamplingWindowAcrossForcedHeartbeats(t *testing.T) {
	now := time.Unix(100, 0)
	state := nodeLoadState{}
	state.observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 0}, now)
	state.observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 100}, now.Add(time.Second))
	require.Equal(t, float64(100), state.rate)
	state.observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 100}, now.Add(time.Second+time.Millisecond))
	state.observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 150}, now.Add(1500*time.Millisecond))
	require.Equal(t, float64(100), state.rate)
	require.Equal(t, uint64(100), state.bytes)
	state.observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 300}, now.Add(3*time.Second))
	require.Equal(t, float64(100), state.rate)
	require.Equal(t, uint64(300), state.bytes)
}
