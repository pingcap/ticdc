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
	"math"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/resource"
	"github.com/stretchr/testify/require"
)

func TestNodeLoadReestablishesBaselineAfterStatsRollover(t *testing.T) {
	stats := resource.NewNodeStats()
	recorder := stats.NewCounter(resource.Sink).Recorder()
	now := time.Now()
	var state nodeLoadState
	recorder.Add(math.MaxUint64 - 7)
	state.observe(stats.Snapshot(now), now)
	recorder.Add(3)
	now = now.Add(resource.SampleInterval)
	state.observe(stats.Snapshot(now), now)
	require.True(t, state.available)
	require.Equal(t, float64(3), state.rates[resource.Sink])
	recorder.Add(8)
	now = now.Add(resource.SampleInterval)
	state.observe(stats.Snapshot(now), now)
	require.False(t, state.available, "rollover must not look idle or produce an enormous rate")
	recorder.Add(5)
	now = now.Add(resource.SampleInterval)
	state.observe(stats.Snapshot(now), now)
	require.True(t, state.available)
	require.Equal(t, float64(5), state.rates[resource.Sink])
}

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
		resource.Load{resource.Sink: 100}.ToNodeLoad("a"), resource.Load{resource.Sink: 200}.ToNodeLoad("b"),
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
	require.Equal(t, uint64(400), c.nodes["a"].load.bytes[resource.Sink])

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
	require.Equal(t, float64(100), state.rates[resource.Sink])
	state.observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 100}, now.Add(time.Second+time.Millisecond))
	state.observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 150}, now.Add(1500*time.Millisecond))
	require.Equal(t, float64(100), state.rates[resource.Sink])
	require.Equal(t, uint64(100), state.bytes[resource.Sink])
	state.observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 300}, now.Add(3*time.Second))
	require.Equal(t, float64(100), state.rates[resource.Sink])
	require.Equal(t, uint64(300), state.bytes[resource.Sink])
}

func TestNodeLoadResourceSamplingAndMixedVersions(t *testing.T) {
	now := time.Unix(100, 0)
	c := newCaptureWriteLeaseController(10, "coordinator")
	c.now = func() time.Time { return now }
	c.updateClusterMode([]node.ID{"a", "b"})
	c.nodes["a"] = &captureLeaseNodeState{}
	c.nodes["b"] = &captureLeaseNodeState{}
	vector := func(sink, write, read uint64) *heartbeatpb.NodeLoad {
		return &heartbeatpb.NodeLoad{Resources: []*heartbeatpb.ResourceUsage{
			{Resource: resource.Sink, TotalBytes: sink},
			{Resource: resource.EventStoreWrite, TotalBytes: write},
			{Resource: resource.EventStoreRead, TotalBytes: read},
		}}
	}
	c.nodes["a"].load.observe(vector(0, 0, 0), now)
	c.nodes["b"].load.observe(&heartbeatpb.NodeLoad{}, now) // Old capture reports sink only.
	now = now.Add(time.Second)
	c.nodes["a"].load.observe(vector(100, 200, 300), now)
	c.nodes["b"].load.observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 400}, now)
	loads, status := c.nodeLoadSnapshot()
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, status)
	require.Equal(t, []*heartbeatpb.NodeLoad{
		resource.Load{resource.Sink: 100}.ToNodeLoad("a"),
		resource.Load{resource.Sink: 400}.ToNodeLoad("b"),
	}, loads, "missing EventStore metrics must not be interpreted as idle")

	// A capability change needs a fresh baseline, just like a process restart.
	c.nodes["b"].load.observe(vector(400, 1000, 2000), now.Add(time.Millisecond))
	_, status = c.nodeLoadSnapshot()
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, status)
	now = now.Add(2 * time.Second)
	c.nodes["a"].load.observe(vector(300, 600, 900), now)
	c.nodes["b"].load.observe(vector(1200, 1020, 2040), now)
	loads, status = c.nodeLoadSnapshot()
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, status)
	require.Equal(t, resource.Load{resource.Sink: 100, resource.EventStoreWrite: 200, resource.EventStoreRead: 300}.ToNodeLoad("a"), loads[0])
	rates, valid := resource.Rates(loads[1])
	require.True(t, valid)
	require.InDelta(t, 10, rates[resource.EventStoreWrite], .01)
	require.InDelta(t, 20, rates[resource.EventStoreRead], .02)

	// Resetting only one counter invalidates the whole sample; no negative
	// rate or stale pre-reset rate can escape through another resource.
	now = now.Add(time.Second)
	c.nodes["a"].load.observe(vector(400, 1, 1200), now)
	_, status = c.nodeLoadSnapshot()
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, status)
	now = now.Add(time.Second)
	c.nodes["a"].load.observe(vector(500, 51, 1500), now)
	loads, status = c.nodeLoadSnapshot()
	require.Equal(t, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, status)
	rates, valid = resource.Rates(loads[0])
	require.True(t, valid)
	require.Equal(t, resource.Load{resource.Sink: 100, resource.EventStoreWrite: 50, resource.EventStoreRead: 300}, rates)
}

func TestNodeLoadSmoothsFlushBurstsAndLoadSteps(t *testing.T) {
	now := time.Unix(100, 0)
	states := [3]nodeLoadState{}
	for i := range states {
		states[i].observe(&heartbeatpb.NodeLoad{}, now)
		states[i].observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 1000}, now.Add(time.Second))
	}
	// A resolved-ts flush can deliver several seconds of work in one node's
	// sample before the other nodes flush. Keep its rate comparable to the
	// ten-second dispatcher sample instead of interpreting this as 3x load.
	states[0].observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 4000}, now.Add(2*time.Second))
	require.Greater(t, states[0].rates[resource.Sink], float64(1000))
	require.Less(t, states[0].rates[resource.Sink]-states[1].rates[resource.Sink], float64(1000))

	// Increase all three equally loaded nodes to 3x traffic. Even while one
	// node's heartbeat arrives first, its temporary lead cannot justify moving
	// a whole hot dispatcher based on the previous 1000-byte/s detail sample.
	counters := [3]uint64{4000, 1000, 1000}
	for second := 3; second < 25; second++ {
		for i := range states {
			counters[i] += 3000
			states[i].observe(&heartbeatpb.NodeLoad{FlushedEventBytes: counters[i]}, now.Add(time.Duration(second)*time.Second))
			for j := range states {
				require.Less(t, states[i].rates[resource.Sink]-states[j].rates[resource.Sink], float64(1000))
			}
		}
	}
	for i := range states {
		require.Greater(t, states[i].rates[resource.Sink], float64(2700))
	}
	// A counter reset invalidates the old smoother and starts a fresh baseline.
	states[0].observe(&heartbeatpb.NodeLoad{}, now.Add(25*time.Second))
	require.False(t, states[0].available)
	states[0].observe(&heartbeatpb.NodeLoad{FlushedEventBytes: 500}, now.Add(26*time.Second))
	require.Equal(t, float64(500), states[0].rates[resource.Sink])
}
