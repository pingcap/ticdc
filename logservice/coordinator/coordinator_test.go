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

package logcoordinator

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func newLogCoordinatorForTest() *logCoordinator {
	c := &logCoordinator{}
	c.eventStoreStates.m = make(map[node.ID]*logservicepb.EventStoreState)
	c.nodes.m = make(map[node.ID]*node.Info)
	c.changefeedStates.m = make(map[common.GID]*changefeedState)
	return c
}

func TestGetCandidateNodes(t *testing.T) {
	coordinator := newLogCoordinatorForTest()

	nodeID1 := node.ID("node-1")
	nodeID2 := node.ID("node-2")
	nodeID3 := node.ID("node-3")
	coordinator.nodes.m[nodeID1] = &node.Info{ID: nodeID1}
	coordinator.nodes.m[nodeID2] = &node.Info{ID: nodeID2}
	coordinator.nodes.m[nodeID3] = &node.Info{ID: nodeID3}

	// initialize table spans
	tableID1 := int64(100)
	tableID2 := int64(101)
	span1 := common.TableIDToComparableSpan(common.DefaultKeyspaceID, tableID1)
	span2 := common.TableIDToComparableSpan(common.DefaultKeyspaceID, tableID2)

	// initialize event store states
	coordinator.updateEventStoreState(nodeID1, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			tableID1: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &span1,
						CheckpointTs: 100,
						ResolvedTs:   200,
					},
				},
			},
		},
	})
	coordinator.updateEventStoreState(nodeID2, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			tableID1: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &span1,
						CheckpointTs: 90,
						ResolvedTs:   180,
					},
					{
						SubID:        2,
						Span:         &span1,
						CheckpointTs: 100,
						ResolvedTs:   220,
					},
					{
						SubID:        3,
						Span:         &span1,
						CheckpointTs: 80,
						ResolvedTs:   160,
					},
				},
			},
			tableID2: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        4,
						Span:         &span2,
						CheckpointTs: 90,
						ResolvedTs:   190,
					},
					{
						SubID:        5,
						Span:         &span2,
						CheckpointTs: 90,
						ResolvedTs:   240,
					},
				},
			},
		},
	})
	coordinator.updateEventStoreState(nodeID3, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			tableID2: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &span2,
						CheckpointTs: 100,
						ResolvedTs:   290,
					},
					{
						SubID:        2,
						Span:         &span2,
						CheckpointTs: 100,
						ResolvedTs:   230,
					},
				},
			},
		},
	})

	// check get candidates
	{
		nodes := coordinator.getCandidateNodes(nodeID1, &span1, uint64(100))
		assert.Equal(t, []string{nodeID2.String()}, nodes)
	}
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span1, uint64(100))
		assert.Equal(t, []string{nodeID2.String(), nodeID1.String()}, nodes)
	}
	{
		nodes := coordinator.getCandidateNodes(nodeID1, &span2, uint64(100))
		assert.Equal(t, []string{nodeID3.String(), nodeID2.String()}, nodes)
	}
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span2, uint64(100))
		assert.Equal(t, []string{nodeID2.String()}, nodes)
	}

	// update event store state for node1 and check again
	coordinator.updateEventStoreState(nodeID1, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			tableID1: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &span1,
						CheckpointTs: 100,
						ResolvedTs:   300,
					},
				},
			},
		},
	})
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span1, uint64(100))
		assert.Equal(t, []string{nodeID1.String(), nodeID2.String()}, nodes)
	}

	// update event store state for node2 and check again
	coordinator.updateEventStoreState(nodeID2, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			tableID1: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &span1,
						CheckpointTs: 100,
						ResolvedTs:   230,
					},
					{
						SubID:        2,
						Span:         &span1,
						CheckpointTs: 100,
						ResolvedTs:   310,
					},
				},
			},
		},
	})
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span1, uint64(100))
		assert.Equal(t, []string{nodeID2.String(), nodeID1.String()}, nodes)
	}

	// remove node1 and check again
	delete(coordinator.nodes.m, nodeID1)
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span1, uint64(100))
		assert.Equal(t, []string{nodeID2.String()}, nodes)
	}
}

func TestUpdateChangefeedStates(t *testing.T) {
	c := newLogCoordinatorForTest()

	cfID1 := common.NewChangefeedID4Test("default", "test1")
	cfID2 := common.NewChangefeedID4Test("default", "test2")

	nodeID1 := node.ID("node-1")
	nodeID2 := node.ID("node-2")

	// 1. First update from node-1 for cf1 and cf2
	states1 := &logservicepb.ChangefeedStates{
		States: []*logservicepb.ChangefeedStateEntry{
			{ChangefeedID: cfID1.ToPB(), ResolvedTs: 100},
			{ChangefeedID: cfID2.ToPB(), ResolvedTs: 110},
		},
	}
	c.updateChangefeedStates(nodeID1, states1)

	// Check state for cf1
	cf1State, ok := c.changefeedStates.m[cfID1.ID()]
	require.True(t, ok)
	require.Equal(t, cfID1, cf1State.cfID)
	require.Len(t, cf1State.nodeStates, 1)
	require.Equal(t, uint64(100), cf1State.nodeStates[nodeID1])
	require.NotNil(t, cf1State.resolvedTsGauge)
	require.NotNil(t, cf1State.resolvedTsLagGauge)

	// Check state for cf2
	cf2State, ok := c.changefeedStates.m[cfID2.ID()]
	require.True(t, ok)
	require.Equal(t, cfID2, cf2State.cfID)
	require.Len(t, cf2State.nodeStates, 1)
	require.Equal(t, uint64(110), cf2State.nodeStates[nodeID1])

	// 2. Update from node-2 for cf1
	states2 := &logservicepb.ChangefeedStates{
		States: []*logservicepb.ChangefeedStateEntry{
			{ChangefeedID: cfID1.ToPB(), ResolvedTs: 105},
		},
	}
	c.updateChangefeedStates(nodeID2, states2)

	// Check state for cf1 from node-2
	cf1State, ok = c.changefeedStates.m[cfID1.ID()]
	require.True(t, ok)
	require.Len(t, cf1State.nodeStates, 2)
	require.Equal(t, uint64(100), cf1State.nodeStates[nodeID1])
	require.Equal(t, uint64(105), cf1State.nodeStates[nodeID2])

	// cf2 state should not change
	cf2State, ok = c.changefeedStates.m[cfID2.ID()]
	require.True(t, ok)
	require.Len(t, cf2State.nodeStates, 1)
	require.Equal(t, uint64(110), cf2State.nodeStates[nodeID1])

	// 3. Update from node-1 again, but this time cf2 is removed from node-1
	states3 := &logservicepb.ChangefeedStates{
		States: []*logservicepb.ChangefeedStateEntry{
			{ChangefeedID: cfID1.ToPB(), ResolvedTs: 120}, // cf1 resolved ts updated
		},
	}
	c.updateChangefeedStates(nodeID1, states3)

	// Check cf1 state updated
	cf1State, ok = c.changefeedStates.m[cfID1.ID()]
	require.True(t, ok)
	require.Len(t, cf1State.nodeStates, 2)
	require.Equal(t, uint64(120), cf1State.nodeStates[nodeID1])
	require.Equal(t, uint64(105), cf1State.nodeStates[nodeID2])

	// Check cf2 is removed from node-1, and since it's the only node for cf2, cf2 should be removed entirely.
	_, ok = c.changefeedStates.m[cfID2.ID()]
	require.False(t, ok, "cf2 should be removed as it has no nodes")

	// 4. Update from node-2 again, removing cf1 from node-2
	states4 := &logservicepb.ChangefeedStates{
		States: []*logservicepb.ChangefeedStateEntry{}, // empty states
	}
	c.updateChangefeedStates(nodeID2, states4)

	// Check cf1 state from node-2 is removed
	cf1State, ok = c.changefeedStates.m[cfID1.ID()]
	require.True(t, ok)
	require.Len(t, cf1State.nodeStates, 1)
	require.Equal(t, uint64(120), cf1State.nodeStates[nodeID1])
	_, ok = cf1State.nodeStates[nodeID2]
	require.False(t, ok)
}

func TestReportMetricsForAffectedChangefeeds(t *testing.T) {
	c := newLogCoordinatorForTest()
	mockPDClock := pdutil.NewClock4Test()
	c.pdClock = mockPDClock

	cfID1 := common.NewChangefeedID4Test("default", "test1")
	cfID2 := common.NewChangefeedID4Test("default", "test2")

	nodeID1 := node.ID("node-1")
	nodeID2 := node.ID("node-2")

	// Setup initial state
	c.changefeedStates.m[cfID1.ID()] = &changefeedState{
		cfID: cfID1,
		nodeStates: map[node.ID]uint64{
			nodeID1: 100,
			nodeID2: 120,
		},
		resolvedTsGauge:    prometheus.NewGauge(prometheus.GaugeOpts{}),
		resolvedTsLagGauge: prometheus.NewGauge(prometheus.GaugeOpts{}),
	}
	c.changefeedStates.m[cfID2.ID()] = &changefeedState{
		cfID: cfID2,
		nodeStates: map[node.ID]uint64{
			nodeID1: 150,
		},
		resolvedTsGauge:    prometheus.NewGauge(prometheus.GaugeOpts{}),
		resolvedTsLagGauge: prometheus.NewGauge(prometheus.GaugeOpts{}),
	}

	// Set PD time
	pdTime := time.Now()
	mockPDClock.(*pdutil.Clock4Test).SetTS(oracle.GoTimeToTS(pdTime))
	pdPhyTs := oracle.GetPhysical(c.pdClock.CurrentTime())

	// Call update metrics
	c.updateChangefeedMetrics()

	// Verify metrics for cf1
	cf1State := c.changefeedStates.m[cfID1.ID()]
	minResolvedTs1 := uint64(100)
	phyResolvedTs1 := oracle.ExtractPhysical(minResolvedTs1)
	lag1 := float64(pdPhyTs-phyResolvedTs1) / 1e3
	require.Equal(t, float64(phyResolvedTs1), testutil.ToFloat64(cf1State.resolvedTsGauge))
	require.InDelta(t, lag1, testutil.ToFloat64(cf1State.resolvedTsLagGauge), 1e-9)

	// Verify metrics for cf2
	cf2State := c.changefeedStates.m[cfID2.ID()]
	minResolvedTs2 := uint64(150)
	phyResolvedTs2 := oracle.ExtractPhysical(minResolvedTs2)
	lag2 := float64(pdPhyTs-phyResolvedTs2) / 1e3
	require.Equal(t, float64(phyResolvedTs2), testutil.ToFloat64(cf2State.resolvedTsGauge))
	require.InDelta(t, lag2, testutil.ToFloat64(cf2State.resolvedTsLagGauge), 1e-9)
}

func TestHandleNodeChange(t *testing.T) {
	c := newLogCoordinatorForTest()
	c.nodes.m["node-1"] = &node.Info{ID: "node-1"}
	c.nodes.m["node-2"] = &node.Info{ID: "node-2"}

	// Node-1 is removed, node-3 is added
	allNodes := map[node.ID]*node.Info{
		"node-2": {ID: "node-2"},
		"node-3": {ID: "node-3"},
	}
	c.handleNodeChange(allNodes)

	require.Len(t, c.nodes.m, 2)
	_, ok := c.nodes.m["node-1"]
	require.False(t, ok)
	_, ok = c.nodes.m["node-2"]
	require.True(t, ok)
	_, ok = c.nodes.m["node-3"]
	require.True(t, ok)
}

func TestHandleNodeChange_CleanState(t *testing.T) {
	c := newLogCoordinatorForTest()
	nodeID1 := node.ID("node-1")
	nodeID2 := node.ID("node-2")
	cfID1 := common.NewChangefeedID4Test("default", "test1")

	// 1. Initial state with two nodes
	c.nodes.m[nodeID1] = &node.Info{ID: nodeID1}
	c.nodes.m[nodeID2] = &node.Info{ID: nodeID2}

	// 2. Populate eventStoreStates for both nodes
	c.eventStoreStates.m[nodeID1] = &logservicepb.EventStoreState{}
	c.eventStoreStates.m[nodeID2] = &logservicepb.EventStoreState{}

	// 3. Populate changefeedStates for a changefeed running on both nodes
	c.changefeedStates.m[cfID1.ID()] = &changefeedState{
		cfID: cfID1,
		nodeStates: map[node.ID]uint64{
			nodeID1: 100,
			nodeID2: 110,
		},
	}

	// Verify initial state
	require.Len(t, c.nodes.m, 2)
	require.Len(t, c.eventStoreStates.m, 2)
	require.Len(t, c.changefeedStates.m[cfID1.ID()].nodeStates, 2)

	// 4. Simulate node-1 is removed
	allNodes := map[node.ID]*node.Info{
		nodeID2: {ID: nodeID2},
	}
	c.handleNodeChange(allNodes)

	// 5. Assertions
	// Node map should be updated
	require.Len(t, c.nodes.m, 1, "node-1 should be removed from nodes map")
	require.Nil(t, c.nodes.m[nodeID1])

	// eventStoreStates for node-1 should be cleaned up
	require.Len(t, c.eventStoreStates.m, 1, "eventStoreStates for node-1 should be cleaned up")
	require.Nil(t, c.eventStoreStates.m[nodeID1])

	// changefeedStates for node-1 should be cleaned up
	require.Len(t, c.changefeedStates.m[cfID1.ID()].nodeStates, 1, "changefeedStates for node-1 should be cleaned up")
	_, exists := c.changefeedStates.m[cfID1.ID()].nodeStates[nodeID1]
	require.False(t, exists)
}
