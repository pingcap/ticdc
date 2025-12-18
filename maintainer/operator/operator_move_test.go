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

package operator

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func setupTestEnvironment(t *testing.T) (*span.Controller, common.ChangeFeedID, *replica.SpanReplication, node.ID, node.ID) {
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	regionCache := testutil.NewMockRegionCache()
	appcontext.SetService(appcontext.RegionCache, regionCache)

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceNamme)
	dispatcherID := common.NewDispatcherID()

	tableSpan := testutil.GetTableSpanByID(100)
	status := &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    1000,
	}

	nodeA := node.ID("node-a")
	nodeB := node.ID("node-b")

	ddlDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(
		changefeedID,
		ddlDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID),
		&heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		},
		nodeA,
		false,
	)

	refresher := replica.NewRegionCountRefresher(changefeedID, time.Minute)
	spanController := span.NewController(changefeedID, ddlSpan, nil, nil, refresher, common.DefaultKeyspaceID, common.DefaultMode)

	replicaSet := replica.NewWorkingSpanReplication(
		changefeedID,
		dispatcherID,
		1,
		tableSpan,
		status,
		nodeA,
		false,
	)

	return spanController, changefeedID, replicaSet, nodeA, nodeB
}

// TestMoveOperator_DestNodeRemovedBeforeOriginStopped tests the scenario where:
// 1. Dispatcher 'a' is on node A, maintainer is also on node A
// 2. A move operation is initiated to move 'a' from node A to node B
// 3. Before node A reports non-working status, node B is removed
// 4. Verify that the move is aborted and the span is marked absent after origin is stopped
func TestMoveOperator_DestNodeRemovedBeforeOriginStopped(t *testing.T) {
	spanController, _, replicaSet, nodeA, nodeB := setupTestEnvironment(t)

	op := NewMoveDispatcherOperator(spanController, replicaSet, nodeA, nodeB)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	require.Equal(t, moveStateRemoveOrigin, op.state)

	op.OnNodeRemove(nodeB)

	require.Equal(t, moveStateAbortRemoveOrigin, op.state)
	require.Equal(t, nodeB, op.dest)
	require.Equal(t, nodeA, op.origin)

	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, messaging.TypeScheduleDispatcherRequest, msg.Type)
	scheduleMsg, ok := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.True(t, ok)
	require.Equal(t, heartbeatpb.ScheduleAction_Remove, scheduleMsg.ScheduleAction)
	require.Equal(t, replicaSet.ID.ToPB(), scheduleMsg.Config.DispatcherID)

	absentSizeBefore := spanController.GetAbsentSize()
	nonWorkingStatus := &heartbeatpb.TableSpanStatus{
		ID:              replicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
		CheckpointTs:    1500,
	}
	op.Check(nodeA, nonWorkingStatus)
	require.True(t, op.IsFinished())
	require.Equal(t, absentSizeBefore+1, spanController.GetAbsentSize())
	require.Equal(t, "", replicaSet.GetNodeID().String())
}

// TestMoveOperator_DestNodeRemovedAfterOriginStopped tests the scenario where:
// 1. Dispatcher 'a' is on node A, maintainer is also on node A
// 2. A move operation is initiated to move 'a' from node A to node B
// 3. Node A reports non-working status first
// 4. Then node B is removed
// 5. Verify that the span is marked as absent for rescheduling
func TestMoveOperator_DestNodeRemovedAfterOriginStopped(t *testing.T) {
	spanController, _, replicaSet, nodeA, nodeB := setupTestEnvironment(t)

	op := NewMoveDispatcherOperator(spanController, replicaSet, nodeA, nodeB)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	nonWorkingStatus := &heartbeatpb.TableSpanStatus{
		ID:              replicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
		CheckpointTs:    1500,
	}
	op.Check(nodeA, nonWorkingStatus)
	require.Equal(t, moveStateAddDest, op.state)

	absentSizeBefore := spanController.GetAbsentSize()
	op.OnNodeRemove(nodeB)

	require.True(t, op.IsFinished())
	require.Equal(t, absentSizeBefore+1, spanController.GetAbsentSize())
	require.Equal(t, "", replicaSet.GetNodeID().String())
}

// TestMoveOperator_OriginNodeRemovedBeforeOriginStopped tests the scenario where:
// 1. Dispatcher 'a' is on node A
// 2. A move operation is initiated to move 'a' from node A to node B
// 3. Before node A reports non-working status, node A is removed
// 4. Verify that the operator waits for node B to report working status before finishing
func TestMoveOperator_OriginNodeRemovedBeforeOriginStopped(t *testing.T) {
	spanController, _, replicaSet, nodeA, nodeB := setupTestEnvironment(t)

	op := NewMoveDispatcherOperator(spanController, replicaSet, nodeA, nodeB)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	require.Equal(t, moveStateRemoveOrigin, op.state)

	op.OnNodeRemove(nodeA)

	require.Equal(t, moveStateAddDest, op.state)
	require.Equal(t, nodeA, op.origin)
	require.Equal(t, nodeB, op.dest)

	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, messaging.TypeScheduleDispatcherRequest, msg.Type)
	scheduleMsg, ok := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.True(t, ok)
	require.Equal(t, heartbeatpb.ScheduleAction_Create, scheduleMsg.ScheduleAction)
	require.Equal(t, nodeB.String(), msg.To.String())
	require.Equal(t, replicaSet.ID.ToPB(), scheduleMsg.Config.DispatcherID)

	require.False(t, op.IsFinished())

	workingStatus := &heartbeatpb.TableSpanStatus{
		ID:              replicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    2000,
	}
	op.Check(nodeB, workingStatus)
	require.True(t, op.IsFinished())
}

// TestMoveOperator_OriginNodeRemovedAfterOriginStopped tests the scenario where:
// 1. Dispatcher 'a' is on node A
// 2. A move operation is initiated to move 'a' from node A to node B
// 3. Node A reports non-working status first
// 4. Then node A is removed
// 5. Verify that the operator waits for node B to report working status before finishing
func TestMoveOperator_OriginNodeRemovedAfterOriginStopped(t *testing.T) {
	spanController, _, replicaSet, nodeA, nodeB := setupTestEnvironment(t)

	op := NewMoveDispatcherOperator(spanController, replicaSet, nodeA, nodeB)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	nonWorkingStatus := &heartbeatpb.TableSpanStatus{
		ID:              replicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
		CheckpointTs:    1500,
	}
	op.Check(nodeA, nonWorkingStatus)
	require.Equal(t, moveStateAddDest, op.state)

	op.OnNodeRemove(nodeA)

	require.Equal(t, nodeA, op.origin)
	require.Equal(t, nodeB, op.dest)

	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, messaging.TypeScheduleDispatcherRequest, msg.Type)
	scheduleMsg, ok := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.True(t, ok)
	require.Equal(t, heartbeatpb.ScheduleAction_Create, scheduleMsg.ScheduleAction)
	require.Equal(t, nodeB.String(), msg.To.String())
	require.Equal(t, replicaSet.ID.ToPB(), scheduleMsg.Config.DispatcherID)

	require.False(t, op.IsFinished())

	workingStatus := &heartbeatpb.TableSpanStatus{
		ID:              replicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    2000,
	}
	op.Check(nodeB, workingStatus)
	require.True(t, op.IsFinished())
}

func TestMoveOperator_BothNodesRemovedBeforeStartDoesNotLeaveSchedulingWithoutNodeID(t *testing.T) {
	messageCenter, _, _ := messaging.NewMessageCenterForTest(t)
	appcontext.SetService(appcontext.MessageCenter, messageCenter)

	spanController, changefeedID, replicaSet, nodeA, nodeB := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	setAliveNodes(nodeManager, map[node.ID]*node.Info{})

	oc := NewOperatorController(changefeedID, spanController, 1, common.DefaultMode)
	op := NewMoveDispatcherOperator(spanController, replicaSet, nodeA, nodeB)
	require.True(t, oc.AddOperator(op))

	require.Equal(t, 1, spanController.GetAbsentSize())
	require.Equal(t, 0, spanController.GetSchedulingSize())
	require.Equal(t, "", replicaSet.GetNodeID().String())
}

// TestMoveOperator_DestThenOriginRemovedAbortsToAbsent tests the scenario where:
// 1. A move operation is initiated to move a span from node A to node B
// 2. Node B is removed first
// 3. Node A is removed later
// 4. Verify that the move is aborted and the span becomes absent for rescheduling
func TestMoveOperator_DestThenOriginRemovedAbortsToAbsent(t *testing.T) {
	spanController, _, replicaSet, nodeA, nodeB := setupTestEnvironment(t)

	op := NewMoveDispatcherOperator(spanController, replicaSet, nodeA, nodeB)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	op.OnNodeRemove(nodeB)
	require.Equal(t, moveStateAbortRemoveOrigin, op.state)
	require.False(t, op.IsFinished())

	absentSizeBefore := spanController.GetAbsentSize()
	op.OnNodeRemove(nodeA)
	require.True(t, op.IsFinished())
	require.Equal(t, absentSizeBefore+1, spanController.GetAbsentSize())

	op.PostFinish()
	require.Equal(t, absentSizeBefore+1, spanController.GetAbsentSize())
	require.Equal(t, "", replicaSet.GetNodeID().String())
}

// TestMoveOperator_TaskRemovedByDDL tests the scenario where:
// 1. A move operation is initiated
// 2. The task is removed (for example, due to DDL) while the operator is running
// 3. Verify that the operator finishes and restores the span to replicating state without running PostFinish
func TestMoveOperator_TaskRemovedByDDL(t *testing.T) {
	spanController, _, replicaSet, nodeA, nodeB := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	op := NewMoveDispatcherOperator(spanController, replicaSet, nodeA, nodeB)
	require.NotNil(t, op)

	op.Start()
	require.Equal(t, moveStateRemoveOrigin, op.state)
	require.Equal(t, 1, spanController.GetSchedulingSize())

	op.OnTaskRemoved()
	require.True(t, op.IsFinished())
	require.Equal(t, moveStateDoneNoPostFinish, op.state)
	require.Equal(t, 1, spanController.GetReplicatingSize())
	require.Equal(t, 0, spanController.GetSchedulingSize())
	require.Equal(t, nodeA, replicaSet.GetNodeID())
	require.Nil(t, op.Schedule())

	op.PostFinish()
	require.Equal(t, 1, spanController.GetReplicatingSize())
}
