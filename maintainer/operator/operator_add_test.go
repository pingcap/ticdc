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

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func newAddTestReplicaSet(
	spanController *span.Controller,
	changefeedID common.ChangeFeedID,
) *replica.SpanReplication {
	dispatcherID := common.NewDispatcherID()
	tableSpan := &heartbeatpb.TableSpan{
		TableID:  200,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
	}
	replicaSet := replica.NewSpanReplication(
		changefeedID,
		dispatcherID,
		2,
		tableSpan,
		1000,
		common.DefaultMode,
		false,
	)
	spanController.AddAbsentReplicaSet(replicaSet)
	return replicaSet
}

// TestAddOperatorDestNodeRemoved tests the scenario where:
// 1. An add operation is initiated to create dispatcher on node A
// 2. Before node A reports working status, node A is removed
// 3. Verify that the operator is marked as finished and removed
// 4. Verify that the span is marked as absent for rescheduling
func TestAddOperatorDestNodeRemoved(t *testing.T) {
	spanController, changefeedID, _, _, nodeB := setupTestEnvironment(t)

	absentReplicaSet := newAddTestReplicaSet(spanController, changefeedID)

	op := NewAddDispatcherOperator(spanController, absentReplicaSet, nodeB, heartbeatpb.OperatorType_O_Add, 7)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	// Verify that the span is bound to nodeB
	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, nodeB.String(), msg.To.String())
	req := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.Equal(t, uint64(7), req.MaintainerEpoch)

	// Node B is removed before it reports working status
	op.OnNodeRemove(nodeB)

	require.True(t, op.IsFinished())
	require.True(t, op.removed.Load())

	op.PostFinish()

	// Verify that the span is marked as absent
	require.Equal(t, 1, spanController.GetAbsentSize())
}

// TestAddOperatorDestReportsWorking tests the scenario where:
// 1. An add operation is initiated to create dispatcher on node B
// 2. Node B reports working status
// 3. Verify that the operator finishes and marks the span replicating
func TestAddOperatorDestReportsWorking(t *testing.T) {
	spanController, changefeedID, _, _, nodeB := setupTestEnvironment(t)
	absentReplicaSet := newAddTestReplicaSet(spanController, changefeedID)

	op := NewAddDispatcherOperator(spanController, absentReplicaSet, nodeB, heartbeatpb.OperatorType_O_Add, 7)
	require.NotNil(t, op)

	op.Start()
	require.Equal(t, 0, spanController.GetAbsentSize())
	require.Equal(t, 1, spanController.GetSchedulingSize())

	workingStatus := &heartbeatpb.TableSpanStatus{
		ID:              absentReplicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    1000,
	}
	op.Check(nodeB, workingStatus)
	require.True(t, op.IsFinished())
	require.False(t, op.removed.Load())
	require.Nil(t, op.Schedule())

	op.PostFinish()
	require.Equal(t, 0, spanController.GetAbsentSize())
	require.Equal(t, 0, spanController.GetSchedulingSize())
	require.Equal(t, 1, spanController.GetReplicatingSize())
	require.Equal(t, nodeB, absentReplicaSet.GetNodeID())
}

// TestAddOperatorDestReportsRemoved tests the scenario where:
// 1. An add operation is initiated to create dispatcher on node B
// 2. Node B reports removed status
// 3. Verify that the operator finishes and marks the span absent for rescheduling
func TestAddOperatorDestReportsRemoved(t *testing.T) {
	spanController, changefeedID, _, _, nodeB := setupTestEnvironment(t)
	absentReplicaSet := newAddTestReplicaSet(spanController, changefeedID)

	op := NewAddDispatcherOperator(spanController, absentReplicaSet, nodeB, heartbeatpb.OperatorType_O_Add, 7)
	require.NotNil(t, op)

	op.Start()
	require.Equal(t, 1, spanController.GetSchedulingSize())

	removedStatus := &heartbeatpb.TableSpanStatus{
		ID:              absentReplicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Removed,
		CheckpointTs:    1000,
	}
	op.Check(nodeB, removedStatus)
	require.True(t, op.IsFinished())
	require.True(t, op.removed.Load())

	op.PostFinish()
	require.Equal(t, 1, spanController.GetAbsentSize())
	require.Equal(t, 0, spanController.GetSchedulingSize())
	require.Equal(t, 0, spanController.GetReplicatingSize())
	require.Equal(t, "", absentReplicaSet.GetNodeID().String())
}

// TestAddOperatorStoppedStatusIgnored tests the scenario where:
// 1. An add operation is initiated to create dispatcher on node B
// 2. Node B reports stopped status
// 3. Verify that the operator keeps running and continues scheduling create requests
func TestAddOperatorStoppedStatusIgnored(t *testing.T) {
	spanController, changefeedID, _, _, nodeB := setupTestEnvironment(t)
	absentReplicaSet := newAddTestReplicaSet(spanController, changefeedID)

	op := NewAddDispatcherOperator(spanController, absentReplicaSet, nodeB, heartbeatpb.OperatorType_O_Add, 7)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	stoppedStatus := &heartbeatpb.TableSpanStatus{
		ID:              absentReplicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
		CheckpointTs:    1000,
	}
	op.Check(nodeB, stoppedStatus)
	require.False(t, op.IsFinished())

	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, nodeB.String(), msg.To.String())
}

// TestAddOperatorTaskRemovedDoesNotReintroduceSpan tests the scenario where:
// 1. An add operation is initiated to create dispatcher on node B
// 2. The span is removed from spanController before the operator is finalized
// 3. Verify that PostFinish does not mark the span absent again
func TestAddOperatorTaskRemovedDoesNotReintroduceSpan(t *testing.T) {
	spanController, changefeedID, _, _, nodeB := setupTestEnvironment(t)
	absentReplicaSet := newAddTestReplicaSet(spanController, changefeedID)

	op := NewAddDispatcherOperator(spanController, absentReplicaSet, nodeB, heartbeatpb.OperatorType_O_Add, 7)
	require.NotNil(t, op)

	op.Start()
	require.NotNil(t, spanController.GetTaskByID(absentReplicaSet.ID))

	spanController.RemoveReplicatingSpan(absentReplicaSet)
	require.Nil(t, spanController.GetTaskByID(absentReplicaSet.ID))

	absentSizeBefore := spanController.GetAbsentSize()
	op.OnTaskRemoved()
	op.PostFinish()
	require.Equal(t, absentSizeBefore, spanController.GetAbsentSize())
}
