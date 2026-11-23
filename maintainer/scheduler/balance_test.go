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

package scheduler

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

const testDefaultBalanceMoveBatchSize = 1024

func TestBalanceSchedulerUsesCheckBalanceInterval(t *testing.T) {
	cfID, _, oc, sc, drainState, _ := newDrainSchedulerTestHarness(t)
	checkBalanceInterval := time.Minute
	testCases := []struct {
		name string
		mode int64
	}{
		{name: "default", mode: common.DefaultMode},
		{name: "redo", mode: common.RedoMode},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewBalanceScheduler(
				cfID,
				nil,
				oc,
				sc,
				checkBalanceInterval,
				tc.mode,
				drainState,
				testDefaultBalanceMoveBatchSize,
			)

			beforeExecute := time.Now()
			nextCheckTime := s.Execute()
			afterExecute := time.Now()

			require.False(t, nextCheckTime.Before(beforeExecute.Add(checkBalanceInterval)))
			require.False(t, nextCheckTime.After(afterExecute.Add(checkBalanceInterval)))
		})
	}
}

func TestBalanceSchedulerSkipsWhenDrainActive(t *testing.T) {
	cfID, nodeManager, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest := node.ID("dest")

	nodeManager.GetAliveNodes()[self] = &node.Info{ID: self}
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	for i := 1; i <= 20; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}
	drainState.SetSelfNodeID(self)
	drainState.SetDispatcherDrainTarget(target, 1)

	s := NewBalanceScheduler(
		cfID,
		nil,
		oc,
		sc,
		0,
		common.DefaultMode,
		drainState,
		testDefaultBalanceMoveBatchSize,
	)
	_ = s.Execute()

	require.Equal(t, 0, oc.OperatorSize())
}

func TestBalanceSchedulerUsesConfiguredMoveBatchSize(t *testing.T) {
	cfID, nodeManager, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest := node.ID("dest")

	nodeManager.GetAliveNodes()[self] = &node.Info{ID: self}
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	for i := 1; i <= 20; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}
	drainState.SetSelfNodeID(self)

	s := NewBalanceScheduler(
		cfID,
		nil,
		oc,
		sc,
		0,
		common.DefaultMode,
		drainState,
		5,
	)
	_ = s.Execute()

	require.Equal(t, 5, oc.OperatorSize())
}

func TestBalanceSchedulerSkipsDuringDrainCooldown(t *testing.T) {
	cfID, nodeManager, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest := node.ID("dest")

	nodeManager.GetAliveNodes()[self] = &node.Info{ID: self}
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	for i := 1; i <= 20; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}
	drainState.SetSelfNodeID(self)
	drainState.SetDispatcherDrainTarget(target, 1)

	s := NewBalanceScheduler(
		cfID,
		nil,
		oc,
		sc,
		0,
		common.DefaultMode,
		drainState,
		testDefaultBalanceMoveBatchSize,
	)
	s.drainBalanceBlockedUntil = time.Time{}

	// First run sees an active drain and starts cooldown.
	_ = s.Execute()
	require.Equal(t, 0, oc.OperatorSize())

	// Drain is cleared but scheduler should still be blocked by cooldown.
	drainState.SetDispatcherDrainTarget("", 1)
	_ = s.Execute()
	require.Equal(t, 0, oc.OperatorSize())

	// Expire cooldown in test and verify scheduling resumes.
	s.drainBalanceBlockedUntil = time.Now().Add(-time.Millisecond)
	_ = s.Execute()
	require.Greater(t, oc.OperatorSize(), 0)
}

// Reproduce #6251: nine indivisible tables, equal counts, and three hot
// dispatchers initially on one node. Execute real move operators to completion
// and feed their placement back through subsequent node samples.
func TestBalanceSchedulerBalancesWholeTableTraffic(t *testing.T) {
	cfID, nm, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	nodes := []node.ID{self, "a", "b"}
	tracker := replica.NewNodeLoadTracker()
	sc.SetNodeLoadTracker(tracker)
	replicas := make([]*replica.SpanReplication, 0, 9)
	traffic := make(map[common.DispatcherID]float32)
	for n, id := range nodes {
		nm.GetAliveNodes()[id] = &node.Info{ID: id}
		for i := range 3 {
			r := addReplicatingSpan(t, cfID, sc, int64(n*3+i+1), id)
			replicas = append(replicas, r)
			traffic[r.ID] = 1
			if n == 0 {
				traffic[r.ID] = 1000
			}
		}
	}
	s := NewBalanceScheduler(cfID, nil, oc, sc, time.Second, common.DefaultMode, drainState, 5)
	s.random = rand.New(rand.NewSource(1))
	hotMoves := 0
	for round := range 40 {
		loads := map[node.ID]float64{}
		for _, r := range replicas {
			r.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 1, ComponentStatus: heartbeatpb.ComponentState_Working, EventSizePerSecond: traffic[r.ID]})
			if traffic[r.ID] > 1 {
				loads[r.GetNodeID()] += float64(traffic[r.ID])
			}
		}
		reports := make([]*heartbeatpb.NodeLoad, 0, 3)
		for _, id := range nodes {
			reports = append(reports, &heartbeatpb.NodeLoad{NodeId: id.String(), BytesPerSecond: loads[id]})
		}
		tracker.Replace(reports, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, time.Now())
		// Advance the observation window without sleeping in the unit test.
		s.trafficMovePending = false
		s.trafficBalanceNotBefore = time.Time{}
		s.Execute()
		if round == 0 {
			require.Equal(t, 1, oc.OperatorSize(), "traffic balance is locally limited to one move")
			s.Execute()
			require.Equal(t, 1, oc.OperatorSize(), "an unfinished move blocks another round")
			require.True(t, s.trafficMovePending)
		}
		for _, op := range oc.GetAllOperators() {
			if traffic[op.ID()] > 1 {
				hotMoves++
			}
			origin := op.Schedule().To
			op.Check(origin, &heartbeatpb.TableSpanStatus{CheckpointTs: 1, ComponentStatus: heartbeatpb.ComponentState_Stopped})
			target := op.Schedule().To
			op.Check(target, &heartbeatpb.TableSpanStatus{CheckpointTs: 1, ComponentStatus: heartbeatpb.ComponentState_Working})
			require.True(t, op.IsFinished())
			op.PostFinish()
			oc.RemoveOp(op.ID())
		}
		if round == 0 {
			// Even after the first move finishes, give node sampling time to
			// observe it before generating another traffic or count move.
			before := time.Now()
			s.Execute()
			require.Zero(t, oc.OperatorSize())
			require.False(t, s.trafficBalanceNotBefore.Before(before.Add(replica.NodeLoadStaleThreshold)))
		}
	}
	counts, hotCounts := map[node.ID]int{}, map[node.ID]int{}
	for _, r := range replicas {
		counts[r.GetNodeID()]++
		if traffic[r.ID] > 1 {
			hotCounts[r.GetNodeID()]++
		}
		require.False(t, r.IsSplitEnabled())
	}
	require.Equal(t, 2, hotMoves, "hot tables must not be moved back by count balancing")
	for _, id := range nodes {
		require.Equal(t, 1, hotCounts[id])
		require.Equal(t, 3, counts[id])
	}
}

func TestBalanceSchedulerRequiresReliableNodeLoad(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status heartbeatpb.NodeLoadStatus
		age    time.Duration
		moves  bool
	}{
		{"incomplete", heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE, 0, false},
		{"stale", heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, replica.NodeLoadStaleThreshold + time.Second, false},
		{"legacy count balance", heartbeatpb.NodeLoadStatus_LOAD_UNSUPPORTED, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfID, nm, oc, sc, ds, self := newDrainSchedulerTestHarness(t)
			nm.GetAliveNodes()[self] = &node.Info{ID: self}
			nm.GetAliveNodes()["dest"] = &node.Info{ID: "dest"}
			for i := 1; i <= 6; i++ {
				addReplicatingSpan(t, cfID, sc, int64(i), self)
			}
			tracker := replica.NewNodeLoadTracker()
			tracker.Replace([]*heartbeatpb.NodeLoad{{NodeId: self.String(), BytesPerSecond: 300}, {NodeId: "dest"}}, tc.status, time.Now().Add(-tc.age))
			sc.SetNodeLoadTracker(tracker)
			s := NewBalanceScheduler(cfID, nil, oc, sc, time.Second, common.DefaultMode, ds, 2)
			s.Execute()
			require.Equal(t, tc.moves, oc.OperatorSize() > 0)
		})
	}
}

func TestBalanceSchedulerUsesNodeTotalsAcrossChangefeeds(t *testing.T) {
	cfID, nm, oc, sc, ds, self := newDrainSchedulerTestHarness(t)
	for _, id := range []node.ID{self, "busy", "idle"} {
		nm.GetAliveNodes()[id] = &node.Info{ID: id}
	}
	for i, id := range []node.ID{self, "busy", "idle"} {
		r := addReplicatingSpan(t, cfID, sc, int64(i+1), id)
		// This changefeed has equal traffic per node. Other changefeeds make
		// self and busy overloaded; only idle is a valid destination.
		r.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 1, ComponentStatus: heartbeatpb.ComponentState_Working, EventSizePerSecond: 100})
	}
	tracker := replica.NewNodeLoadTracker()
	tracker.Replace([]*heartbeatpb.NodeLoad{{NodeId: self.String(), BytesPerSecond: 1000}, {NodeId: "busy", BytesPerSecond: 2000}, {NodeId: "idle", BytesPerSecond: 100}}, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, time.Now())
	sc.SetNodeLoadTracker(tracker)
	s := NewBalanceScheduler(cfID, nil, oc, sc, time.Second, common.DefaultMode, ds, 5)
	s.Execute()
	require.Equal(t, 1, oc.OperatorSize())
	op := oc.GetAllOperators()[0]
	origin := op.Schedule().To
	require.NotEqual(t, node.ID("idle"), origin)
	op.Check(origin, &heartbeatpb.TableSpanStatus{CheckpointTs: 1, ComponentStatus: heartbeatpb.ComponentState_Stopped})
	require.Equal(t, node.ID("idle"), op.Schedule().To)
}

func TestBalanceSchedulerDoesNotMoveAnIndivisibleHotTable(t *testing.T) {
	cfID, nm, oc, sc, ds, self := newDrainSchedulerTestHarness(t)
	nm.GetAliveNodes()[self] = &node.Info{ID: self}
	nm.GetAliveNodes()["dest"] = &node.Info{ID: "dest"}
	r := addReplicatingSpan(t, cfID, sc, 1, self)
	r.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 1, ComponentStatus: heartbeatpb.ComponentState_Working, EventSizePerSecond: 100})
	r = addReplicatingSpan(t, cfID, sc, 2, "dest")
	r.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 1, ComponentStatus: heartbeatpb.ComponentState_Working, EventSizePerSecond: 1})
	tracker := replica.NewNodeLoadTracker()
	tracker.Replace([]*heartbeatpb.NodeLoad{{NodeId: self.String(), BytesPerSecond: 100}, {NodeId: "dest"}}, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE, time.Now())
	sc.SetNodeLoadTracker(tracker)
	s := NewBalanceScheduler(cfID, nil, oc, sc, time.Second, common.DefaultMode, ds, 5)
	s.Execute()
	require.Zero(t, oc.OperatorSize())
}
