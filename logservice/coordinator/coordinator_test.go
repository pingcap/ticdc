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
	c := &logCoordinator{pdClock: pdutil.NewClock4Test()}
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
	startTs := uint64(100)

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
	require.Len(t, coordinator.eventStoreStates.m, 3)

	// check get candidates
	{
		nodes := coordinator.getCandidateNodes(nodeID1, &span1, startTs)
		assert.Equal(t, []string{nodeID2.String()}, nodes)
	}
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span1, startTs)
		assert.Equal(t, []string{nodeID2.String(), nodeID1.String()}, nodes)
	}
	{
		nodes := coordinator.getCandidateNodes(nodeID1, &span2, startTs)
		assert.Equal(t, []string{nodeID3.String(), nodeID2.String()}, nodes)
	}
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span2, startTs)
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
		nodes := coordinator.getCandidateNodes(nodeID3, &span1, startTs)
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
		nodes := coordinator.getCandidateNodes(nodeID3, &span1, startTs)
		assert.Equal(t, []string{nodeID2.String(), nodeID1.String()}, nodes)
	}

	// remove node1 and check again
	delete(coordinator.nodes.m, nodeID1)
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span1, startTs)
		assert.Equal(t, []string{nodeID2.String()}, nodes)
	}
}

func TestGetCandidateNodesIgnoreDifferentKeyspace(t *testing.T) {
	coordinator := newLogCoordinatorForTest()

	requestNodeID := node.ID("request-node")
	candidateNodeID := node.ID("candidate-node")
	coordinator.nodes.m[requestNodeID] = &node.Info{ID: requestNodeID}
	coordinator.nodes.m[candidateNodeID] = &node.Info{ID: candidateNodeID}

	// Different keyspaces normally produce different encoded keys. Keep the key range identical here
	// to verify that candidate selection explicitly isolates subscriptions by KeyspaceID.
	requestedSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, 100)
	requestedSpan.KeyspaceID = 1
	otherKeyspaceSpan := requestedSpan
	otherKeyspaceSpan.KeyspaceID = 2

	coordinator.updateEventStoreState(candidateNodeID, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			requestedSpan.TableID: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &otherKeyspaceSpan,
						CheckpointTs: 100,
						ResolvedTs:   200,
					},
				},
			},
		},
	})

	nodes := coordinator.getCandidateNodes(requestNodeID, &requestedSpan, 100)
	require.Empty(t, nodes)
}

func TestGetCandidateNodesIgnoreResolvedEqCheckpoint(t *testing.T) {
	coordinator := newLogCoordinatorForTest()

	nodeID1 := node.ID("node-1")
	nodeID2 := node.ID("node-2")
	coordinator.nodes.m[nodeID1] = &node.Info{ID: nodeID1}
	coordinator.nodes.m[nodeID2] = &node.Info{ID: nodeID2}

	tableID := int64(200)
	span := common.TableIDToComparableSpan(common.DefaultKeyspaceID, tableID)
	startTs := uint64(600)

	coordinator.updateEventStoreState(nodeID2, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			tableID: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &span,
						CheckpointTs: startTs - 1,
						ResolvedTs:   startTs - 1,
					},
				},
			},
		},
	})

	nodes := coordinator.getCandidateNodes(nodeID1, &span, startTs)
	require.Empty(t, nodes, "resolvedTs equal to checkpointTs should not be reused")
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
	require.Equal(t, uint64(100), cf1State.minLogServiceResolvedTs)
	require.NotNil(t, cf1State.resolvedTsGauge)
	require.NotNil(t, cf1State.resolvedTsLagGauge)

	// Check state for cf2
	cf2State, ok := c.changefeedStates.m[cfID2.ID()]
	require.True(t, ok)
	require.Equal(t, cfID2, cf2State.cfID)
	require.Len(t, cf2State.nodeStates, 1)
	require.Equal(t, uint64(110), cf2State.nodeStates[nodeID1])
	require.Equal(t, uint64(110), cf2State.minLogServiceResolvedTs)

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
	require.Equal(t, uint64(100), cf1State.minLogServiceResolvedTs)

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
	require.Equal(t, uint64(105), cf1State.minLogServiceResolvedTs)

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
	require.Equal(t, uint64(105), cf1State.minLogServiceResolvedTs)
	_, ok = cf1State.nodeStates[nodeID2]
	require.False(t, ok)
}

func TestUpdateChangefeedStatesRefreshesMetricsImmediately(t *testing.T) {
	c := newLogCoordinatorForTest()
	mockPDClock := c.pdClock.(*pdutil.Clock4Test)
	pdTime := time.Now().Truncate(time.Millisecond)
	mockPDClock.SetTS(oracle.GoTimeToTS(pdTime))

	cfID := common.NewChangefeedID4Test("default", "immediate-metrics")
	resolvedTs := oracle.GoTimeToTS(pdTime.Add(-500 * time.Millisecond))
	c.updateChangefeedStates(node.ID("node-1"), &logservicepb.ChangefeedStates{
		States: []*logservicepb.ChangefeedStateEntry{{
			ChangefeedID: cfID.ToPB(),
			ResolvedTs:   resolvedTs,
		}},
	})

	state := c.changefeedStates.m[cfID.ID()]
	require.Equal(t, resolvedTs, state.minLogServiceResolvedTs)
	require.Equal(t, float64(oracle.ExtractPhysical(resolvedTs)), testutil.ToFloat64(state.resolvedTsGauge))
	require.InDelta(t, 0.5, testutil.ToFloat64(state.resolvedTsLagGauge), 1e-9)

	mockPDClock.SetTS(oracle.GoTimeToTS(pdTime.Add(time.Second)))
	newerResolvedTs := oracle.GoTimeToTS(pdTime.Add(-100 * time.Millisecond))
	c.updateChangefeedStates(node.ID("node-2"), &logservicepb.ChangefeedStates{
		States: []*logservicepb.ChangefeedStateEntry{{
			ChangefeedID: cfID.ToPB(),
			ResolvedTs:   newerResolvedTs,
		}},
	})
	require.Equal(t, resolvedTs, state.minLogServiceResolvedTs)
	require.InDelta(t, 0.5, testutil.ToFloat64(state.resolvedTsLagGauge), 1e-9)
}

func TestUpdateChangefeedStatesWaitsForCompleteReportingRound(t *testing.T) {
	c := newLogCoordinatorForTest()
	mockPDClock := c.pdClock.(*pdutil.Clock4Test)
	pdTime := time.Now().Truncate(time.Millisecond)
	mockPDClock.SetTS(oracle.GoTimeToTS(pdTime))

	cfID := common.NewChangefeedID4Test("default", "complete-reporting-round")
	oldResolvedTs := oracle.GoTimeToTS(pdTime.Add(-900 * time.Millisecond))
	state := &changefeedState{
		cfID: cfID,
		nodeStates: map[node.ID]uint64{
			"node-1": oldResolvedTs,
			"node-2": oldResolvedTs,
			"node-3": oldResolvedTs,
		},
		nodesReportedSinceLastUpdate: make(map[node.ID]struct{}),
		nodeReportPhyTs:              make(map[node.ID]int64),
		minLogServiceResolvedTs:      oldResolvedTs,
		resolvedTsGauge:              prometheus.NewGauge(prometheus.GaugeOpts{}),
		resolvedTsLagGauge:           prometheus.NewGauge(prometheus.GaugeOpts{}),
	}
	state.resolvedTsGauge.Set(float64(oracle.ExtractPhysical(oldResolvedTs)))
	state.resolvedTsLagGauge.Set(0.9)
	c.changefeedStates.m[cfID.ID()] = state

	report := func(nodeID node.ID, reportTime time.Time, lag time.Duration) uint64 {
		mockPDClock.SetTS(oracle.GoTimeToTS(reportTime))
		resolvedTs := oracle.GoTimeToTS(reportTime.Add(-lag))
		c.updateChangefeedStates(nodeID, &logservicepb.ChangefeedStates{
			States: []*logservicepb.ChangefeedStateEntry{{
				ChangefeedID: cfID.ToPB(),
				ResolvedTs:   resolvedTs,
			}},
		})
		return resolvedTs
	}

	newResolvedTs := report("node-1", pdTime, 50*time.Millisecond)
	report("node-1", pdTime, 50*time.Millisecond)
	report("node-2", pdTime.Add(300*time.Millisecond), 180*time.Millisecond)
	require.Equal(t, oldResolvedTs, state.minLogServiceResolvedTs)
	require.InDelta(t, 0.9, testutil.ToFloat64(state.resolvedTsLagGauge), 1e-9)

	report("node-3", pdTime.Add(600*time.Millisecond), 120*time.Millisecond)
	require.Equal(t, newResolvedTs, state.minLogServiceResolvedTs)
	require.InDelta(t, 0.18, testutil.ToFloat64(state.resolvedTsLagGauge), 1e-9)
	require.Empty(t, state.nodesReportedSinceLastUpdate)
}

func TestPartialReportingRoundKeepsLastPublishedMetrics(t *testing.T) {
	c := newLogCoordinatorForTest()
	mockPDClock := c.pdClock.(*pdutil.Clock4Test)
	pdTime := time.Now().Truncate(time.Millisecond)
	mockPDClock.SetTS(oracle.GoTimeToTS(pdTime))

	cfID := common.NewChangefeedID4Test("default", "partial-reporting-round")
	oldResolvedTs := oracle.GoTimeToTS(pdTime.Add(-900 * time.Millisecond))
	otherResolvedTs := oracle.GoTimeToTS(pdTime.Add(-500 * time.Millisecond))
	state := &changefeedState{
		cfID: cfID,
		nodeStates: map[node.ID]uint64{
			"node-1": oldResolvedTs,
			"node-2": otherResolvedTs,
		},
		nodesReportedSinceLastUpdate: make(map[node.ID]struct{}),
		nodeReportPhyTs:              make(map[node.ID]int64),
		minLogServiceResolvedTs:      oldResolvedTs,
		resolvedTsGauge:              prometheus.NewGauge(prometheus.GaugeOpts{}),
		resolvedTsLagGauge:           prometheus.NewGauge(prometheus.GaugeOpts{}),
	}
	state.resolvedTsGauge.Set(float64(oracle.ExtractPhysical(oldResolvedTs)))
	state.resolvedTsLagGauge.Set(0.9)
	c.changefeedStates.m[cfID.ID()] = state

	newResolvedTs := oracle.GoTimeToTS(pdTime.Add(-100 * time.Millisecond))
	c.updateChangefeedStates("node-1", &logservicepb.ChangefeedStates{
		States: []*logservicepb.ChangefeedStateEntry{{
			ChangefeedID: cfID.ToPB(),
			ResolvedTs:   newResolvedTs,
		}},
	})
	require.Equal(t, oldResolvedTs, state.minLogServiceResolvedTs)

	mockPDClock.SetTS(oracle.GoTimeToTS(pdTime.Add(200 * time.Millisecond)))
	require.Equal(t, oldResolvedTs, state.minLogServiceResolvedTs)
	require.Equal(t, float64(oracle.ExtractPhysical(oldResolvedTs)), testutil.ToFloat64(state.resolvedTsGauge))
	require.InDelta(t, 0.9, testutil.ToFloat64(state.resolvedTsLagGauge), 1e-9)

	mockPDClock.SetTS(oracle.GoTimeToTS(pdTime.Add(300 * time.Millisecond)))
	c.updateChangefeedStates("node-2", &logservicepb.ChangefeedStates{
		States: []*logservicepb.ChangefeedStateEntry{{
			ChangefeedID: cfID.ToPB(),
			ResolvedTs:   oracle.GoTimeToTS(pdTime.Add(200 * time.Millisecond)),
		}},
	})
	require.Equal(t, newResolvedTs, state.minLogServiceResolvedTs)
	require.Equal(t, float64(oracle.ExtractPhysical(newResolvedTs)), testutil.ToFloat64(state.resolvedTsGauge))
	require.InDelta(t, 0.1, testutil.ToFloat64(state.resolvedTsLagGauge), 1e-9)
}

func TestUpdateMetricsForAffectedChangefeeds(t *testing.T) {
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
		nodeReportPhyTs:    make(map[node.ID]int64),
	}
	c.changefeedStates.m[cfID2.ID()] = &changefeedState{
		cfID: cfID2,
		nodeStates: map[node.ID]uint64{
			nodeID1: 150,
		},
		resolvedTsGauge:    prometheus.NewGauge(prometheus.GaugeOpts{}),
		resolvedTsLagGauge: prometheus.NewGauge(prometheus.GaugeOpts{}),
		nodeReportPhyTs:    make(map[node.ID]int64),
	}

	// Set PD time
	pdTime := time.Now()
	mockPDClock.(*pdutil.Clock4Test).SetTS(oracle.GoTimeToTS(pdTime))
	pdPhyTs := oracle.GetPhysical(c.pdClock.CurrentTime())
	c.changefeedStates.m[cfID1.ID()].nodeReportPhyTs[nodeID1] = pdPhyTs
	c.changefeedStates.m[cfID1.ID()].nodeReportPhyTs[nodeID2] = pdPhyTs
	c.changefeedStates.m[cfID2.ID()].nodeReportPhyTs[nodeID1] = pdPhyTs

	// Call update metrics
	c.updateChangefeedMetrics(c.changefeedStates.m[cfID1.ID()])
	c.updateChangefeedMetrics(c.changefeedStates.m[cfID2.ID()])

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
