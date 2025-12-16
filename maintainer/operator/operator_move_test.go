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
// 4. Verify that dispatcher 'a' can be recreated on node A
func TestMoveOperator_DestNodeRemovedBeforeOriginStopped(t *testing.T) {
	spanController, _, replicaSet, nodeA, nodeB := setupTestEnvironment(t)

	op := NewMoveDispatcherOperator(spanController, replicaSet, nodeA, nodeB)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	require.False(t, op.originNodeStopped.Load())

	op.OnNodeRemove(nodeB)

	require.False(t, op.originNodeStopped.Load())

	require.True(t, op.bind)
	require.Equal(t, nodeA, op.dest)
	require.Equal(t, nodeA, op.origin)

	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, messaging.TypeScheduleDispatcherRequest, msg.Type)
	scheduleMsg, ok := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.True(t, ok)
	require.Equal(t, heartbeatpb.ScheduleAction_Remove, scheduleMsg.ScheduleAction)
	require.Equal(t, replicaSet.ID.ToPB(), scheduleMsg.Config.DispatcherID)

	nonWorkingStatus := &heartbeatpb.TableSpanStatus{
		ID:              replicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
		CheckpointTs:    1500,
	}
	op.Check(nodeA, nonWorkingStatus)
	require.True(t, op.originNodeStopped.Load())

	msg = op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, messaging.TypeScheduleDispatcherRequest, msg.Type)
	scheduleMsg, ok = msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.True(t, ok)
	require.Equal(t, heartbeatpb.ScheduleAction_Create, scheduleMsg.ScheduleAction)
	require.Equal(t, nodeA.String(), msg.To.String())
	require.Equal(t, replicaSet.ID.ToPB(), scheduleMsg.Config.DispatcherID)

	workingStatus := &heartbeatpb.TableSpanStatus{
		ID:              replicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    2000,
	}
	op.Check(nodeA, workingStatus)
	require.True(t, op.IsFinished())
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
	require.True(t, op.originNodeStopped.Load())

	op.OnNodeRemove(nodeB)

	require.True(t, op.noPostFinishNeed)
	require.True(t, op.IsFinished())

	require.Equal(t, 1, spanController.GetAbsentSize())
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

	require.False(t, op.originNodeStopped.Load())

	op.OnNodeRemove(nodeA)

	require.True(t, op.originNodeStopped.Load())
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
	require.True(t, op.originNodeStopped.Load())

	op.OnNodeRemove(nodeA)

	require.True(t, op.originNodeStopped.Load())
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
