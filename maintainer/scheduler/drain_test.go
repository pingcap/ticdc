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
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestDrainSchedulerMovesOnlyDispatchersOnTarget(t *testing.T) {
	cfID, nodeManager, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest := node.ID("dest")
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	onTarget1 := addReplicatingSpan(t, cfID, sc, 1, target)
	onTarget2 := addReplicatingSpan(t, cfID, sc, 2, target)
	offTarget := addReplicatingSpan(t, cfID, sc, 3, dest)
	drainState.SetSelfNodeID(self)
	drainState.SetDispatcherDrainTarget(target, 1)

	s := NewDrainScheduler(
		cfID,
		10,
		oc,
		sc,
		common.DefaultMode,
		drainState,
	)
	_ = s.Execute()

	require.NotNil(t, oc.GetOperator(onTarget1.ID))
	require.NotNil(t, oc.GetOperator(onTarget2.ID))
	require.Nil(t, oc.GetOperator(offTarget.ID))
	require.Equal(t, 2, oc.OperatorSize())
}

func TestDrainSchedulerCapsInflightDrainMoves(t *testing.T) {
	cfID, nodeManager, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest1 := node.ID("dest1")
	dest2 := node.ID("dest2")
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest1] = &node.Info{ID: dest1}
	nodeManager.GetAliveNodes()[dest2] = &node.Info{ID: dest2}

	for i := 1; i <= 25; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}
	drainState.SetSelfNodeID(self)
	drainState.SetDispatcherDrainTarget(target, 1)

	s := NewDrainScheduler(
		cfID,
		100,
		oc,
		sc,
		common.DefaultMode,
		drainState,
	)
	_ = s.Execute()
	require.Equal(t, maxDrainMovePerRound, oc.OperatorSize())

	// Subsequent scheduling should not exceed the in-flight drain move cap.
	_ = s.Execute()
	require.Equal(t, maxDrainMovePerRound, oc.OperatorSize())

	operators := oc.GetAllOperators()
	require.NotEmpty(t, operators)
	oc.RemoveOp(operators[0].ID())
	require.Equal(t, maxDrainMovePerRound-1, oc.OperatorSize())

	_ = s.Execute()
	require.Equal(t, maxDrainMovePerRound, oc.OperatorSize())
}

func TestDrainSchedulerKeepsFixedLimitWithinDrainEpoch(t *testing.T) {
	cfID, nodeManager, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest1 := node.ID("dest1")
	dest2 := node.ID("dest2")
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest1] = &node.Info{ID: dest1}
	nodeManager.GetAliveNodes()[dest2] = &node.Info{ID: dest2}

	for i := 1; i <= 1100; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}
	drainState.SetSelfNodeID(self)
	drainState.SetDispatcherDrainTarget(target, 1)

	s := NewDrainScheduler(
		cfID,
		100,
		oc,
		sc,
		common.DefaultMode,
		drainState,
	)

	_ = s.Execute()
	require.Equal(t, 11, oc.OperatorSize())

	operators := oc.GetAllOperators()
	require.NotEmpty(t, operators)
	oc.RemoveOp(operators[0].ID())
	require.Equal(t, 10, oc.OperatorSize())

	removed := 0
	for _, replication := range sc.GetTaskByNodeID(target) {
		if oc.GetOperator(replication.ID) != nil {
			continue
		}
		sc.RemoveReplicatingSpan(replication)
		removed++
		if removed == 200 {
			break
		}
	}
	require.Equal(t, 200, removed)

	_ = s.Execute()
	require.Equal(t, 11, oc.OperatorSize())
}

func TestDrainSchedulerRecomputesLimitForNewDrainEpoch(t *testing.T) {
	cfID, nodeManager, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	target := node.ID("target")
	dest1 := node.ID("dest1")
	dest2 := node.ID("dest2")
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest1] = &node.Info{ID: dest1}
	nodeManager.GetAliveNodes()[dest2] = &node.Info{ID: dest2}

	for i := 1; i <= 1100; i++ {
		addReplicatingSpan(t, cfID, sc, int64(i), target)
	}
	drainState.SetSelfNodeID(self)
	drainState.SetDispatcherDrainTarget(target, 1)

	s := NewDrainScheduler(
		cfID,
		100,
		oc,
		sc,
		common.DefaultMode,
		drainState,
	)

	_ = s.Execute()
	require.Equal(t, 11, oc.OperatorSize())

	for _, op := range oc.GetAllOperators() {
		oc.RemoveOp(op.ID())
	}
	require.Equal(t, 0, oc.OperatorSize())

	drainState.SetDispatcherDrainTarget("", 1)
	_ = s.Execute()

	removed := 0
	for _, replication := range sc.GetTaskByNodeID(target) {
		sc.RemoveReplicatingSpan(replication)
		removed++
		if removed == 200 {
			break
		}
	}
	require.Equal(t, 200, removed)

	drainState.SetDispatcherDrainTarget(target, 2)
	_ = s.Execute()
	require.Equal(t, maxDrainMovePerRound, oc.OperatorSize())
}

func TestDrainSchedulerSkipsWhenSelfIsTarget(t *testing.T) {
	cfID, nodeManager, oc, sc, drainState, self := newDrainSchedulerTestHarness(t)
	target := self
	dest := node.ID("dest")
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}
	addReplicatingSpan(t, cfID, sc, 1, target)
	drainState.SetSelfNodeID(self)
	drainState.SetDispatcherDrainTarget(target, 1)

	s := NewDrainScheduler(
		cfID,
		10,
		oc,
		sc,
		common.DefaultMode,
		drainState,
	)
	_ = s.Execute()

	require.Equal(t, 0, oc.OperatorSize())
}

func newDrainSchedulerTestHarness(
	t *testing.T,
) (common.ChangeFeedID, *watcher.NodeManager, *operator.Controller, *span.Controller, *DrainState, node.ID) {
	t.Helper()

	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	cfID := common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName)
	self := node.ID("self")
	ddlID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(
		cfID,
		ddlID,
		0,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID),
		&heartbeatpb.TableSpanStatus{
			ID:              ddlID.ToPB(),
			CheckpointTs:    1,
			ComponentStatus: heartbeatpb.ComponentState_Working,
			Mode:            common.DefaultMode,
		},
		self,
		false,
	)
	sc := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	oc := operator.NewOperatorController(cfID, sc, 100, common.DefaultMode)
	return cfID, nodeManager, oc, sc, NewDrainState(), self
}

func addReplicatingSpan(
	t *testing.T,
	cfID common.ChangeFeedID,
	sc *span.Controller,
	tableID int64,
	nodeID node.ID,
) *replica.SpanReplication {
	t.Helper()
	spanRange := common.TableIDToComparableSpan(common.DefaultKeyspaceID, tableID)
	replication := replica.NewSpanReplication(
		cfID,
		common.NewDispatcherID(),
		1,
		&heartbeatpb.TableSpan{
			TableID:    tableID,
			StartKey:   spanRange.StartKey,
			EndKey:     spanRange.EndKey,
			KeyspaceID: common.DefaultKeyspaceID,
		},
		1,
		common.DefaultMode,
		false,
	)
	replication.SetNodeID(nodeID)
	sc.AddReplicatingSpan(replication)
	return replication
}
