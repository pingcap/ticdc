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
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/stretchr/testify/require"
)

// setupMergeTestEnvironment creates multiple replica sets for merge testing.
func setupMergeTestEnvironment(t *testing.T) (*span.Controller, []*replica.SpanReplication, []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus], node.ID) {
	return setupMergeTestEnvironmentWithCheckpointTs(t, 1000, 1000)
}

func setupMergeTestEnvironmentWithCheckpointTs(
	t *testing.T,
	checkpointTs1 uint64,
	checkpointTs2 uint64,
) (*span.Controller, []*replica.SpanReplication, []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus], node.ID) {
	spanController, changefeedID, _, nodeA, _ := setupTestEnvironment(t)

	// Create two consecutive spans to merge
	dispatcherID1 := common.NewDispatcherID()
	span1 := &heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
	}
	status1 := &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID1.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    checkpointTs1,
	}
	replicaSet1 := replica.NewWorkingSpanReplication(
		changefeedID,
		dispatcherID1,
		1,
		span1,
		status1,
		nodeA,
		false,
	)

	dispatcherID2 := common.NewDispatcherID()
	span2 := &heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
	}
	status2 := &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID2.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    checkpointTs2,
	}
	replicaSet2 := replica.NewWorkingSpanReplication(
		changefeedID,
		dispatcherID2,
		1,
		span2,
		status2,
		nodeA,
		false,
	)

	toMergedReplicaSets := []*replica.SpanReplication{replicaSet1, replicaSet2}

	// Add replica sets to span controller
	spanController.AddReplicatingSpan(replicaSet1)
	spanController.AddReplicatingSpan(replicaSet2)

	// Create occupy operators for each replica set
	occupyOp1 := NewOccupyDispatcherOperator(spanController, replicaSet1)
	occupyOp2 := NewOccupyDispatcherOperator(spanController, replicaSet2)
	occupyOperators := []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]{
		occupyOp1,
		occupyOp2,
	}

	return spanController, toMergedReplicaSets, occupyOperators, nodeA
}

// TestMergeOperator_NodeRemovedBeforeWorking tests the scenario where:
// 1. A merge operation is initiated to merge multiple spans on node A
// 2. Before the new merged dispatcher reports working status, node A is removed
// 3. Verify that the operator is marked as finished and removed
// 4. Verify that all original spans are marked as absent
// 5. Verify that occupy operators are marked as finished
func TestMergeOperator_NodeRemovedBeforeWorking(t *testing.T) {
	spanController, toMergedReplicaSets, occupyOperators, nodeA := setupMergeTestEnvironment(t)

	op := NewMergeDispatcherOperator(spanController, toMergedReplicaSets, occupyOperators)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	// Verify that merge message is scheduled
	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, messaging.HeartbeatCollectorTopic, msg.Topic)
	require.Equal(t, nodeA.String(), msg.To.String())

	// Node A is removed before the merged dispatcher reports working status
	op.OnNodeRemove(nodeA)

	require.True(t, op.IsFinished())
	require.True(t, op.aborted.Load())

	// Get the absent size before PostFinish
	absentSizeBefore := spanController.GetAbsentSize()

	op.PostFinish()

	// Verify that all original spans are marked as absent
	require.Equal(t, absentSizeBefore+2, spanController.GetAbsentSize())

	// Verify that occupy operators are marked as finished
	for _, occupyOp := range occupyOperators {
		require.True(t, occupyOp.IsFinished())
	}
}

// TestMergeOperator_TaskRemovedByDDLBeforeWorking tests the scenario where:
// 1. A merge operation is initiated to merge multiple spans on node A
// 2. Before the merged dispatcher reports working status, a DDL removes the task
// 3. Verify that the operator is marked as finished and does not mark original spans as absent
// 4. Verify that the merged span is removed from span controller
// 5. Verify that occupy operators are marked as finished
func TestMergeOperator_TaskRemovedByDDLBeforeWorking(t *testing.T) {
	spanController, toMergedReplicaSets, occupyOperators, nodeA := setupMergeTestEnvironment(t)

	op := NewMergeDispatcherOperator(spanController, toMergedReplicaSets, occupyOperators)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	absentSizeBefore := spanController.GetAbsentSize()
	require.NotNil(t, spanController.GetTaskByID(op.ID()))

	op.OnTaskRemoved()
	require.True(t, op.IsFinished())
	require.True(t, op.aborted.Load())
	require.True(t, op.abortedByDDL.Load())

	op.PostFinish()

	require.Equal(t, absentSizeBefore, spanController.GetAbsentSize())
	for _, replicaSet := range toMergedReplicaSets {
		require.Equal(t, nodeA, replicaSet.GetNodeID())
	}
	require.Nil(t, spanController.GetTaskByID(op.ID()))

	for _, occupyOp := range occupyOperators {
		require.True(t, occupyOp.IsFinished())
	}
}

func TestMergeOperator_NewReplicaSetCheckpointTsUsesMinOfMergedReplicas(t *testing.T) {
	spanController, toMergedReplicaSets, occupyOperators, _ := setupMergeTestEnvironmentWithCheckpointTs(t, 1500, 1000)

	op := NewMergeDispatcherOperator(spanController, toMergedReplicaSets, occupyOperators)
	require.NotNil(t, op)
	// The merged replica should inherit a safe checkpointTs to avoid regressing global checkpoint.
	require.Equal(t, uint64(1000), op.newReplicaSet.GetStatus().GetCheckpointTs())
}

// TestMergeOperator_SuccessfulMerge tests the scenario where:
// 1. A merge operation is initiated to merge multiple spans on node A
// 2. The new merged dispatcher reports working status
// 3. Verify that PostFinish removes the old spans and marks the merged span replicating
func TestMergeOperator_SuccessfulMerge(t *testing.T) {
	spanController, toMergedReplicaSets, occupyOperators, nodeA := setupMergeTestEnvironment(t)

	op := NewMergeDispatcherOperator(spanController, toMergedReplicaSets, occupyOperators)
	require.NotNil(t, op)

	op.Start()

	workingStatus := &heartbeatpb.TableSpanStatus{
		ID:              op.ID().ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    2000,
	}
	op.Check(nodeA, workingStatus)
	require.True(t, op.IsFinished())
	require.False(t, op.aborted.Load())

	op.PostFinish()

	require.Nil(t, spanController.GetTaskByID(toMergedReplicaSets[0].ID))
	require.Nil(t, spanController.GetTaskByID(toMergedReplicaSets[1].ID))
	require.NotNil(t, spanController.GetTaskByID(op.ID()))
	require.Equal(t, 0, spanController.GetAbsentSize())
	require.Equal(t, 0, spanController.GetSchedulingSize())
	require.Equal(t, 1, spanController.GetReplicatingSize())

	for _, occupyOp := range occupyOperators {
		require.True(t, occupyOp.IsFinished())
	}
}

// TestMergeOperator_NodeRemovedAfterWorking tests the scenario where:
// 1. A merge operation is initiated and the merged dispatcher reports working status
// 2. Then the origin node is removed before PostFinish runs
// 3. Verify that the merge is aborted and the old spans become absent for rescheduling
func TestMergeOperator_NodeRemovedAfterWorking(t *testing.T) {
	spanController, toMergedReplicaSets, occupyOperators, nodeA := setupMergeTestEnvironment(t)

	op := NewMergeDispatcherOperator(spanController, toMergedReplicaSets, occupyOperators)
	require.NotNil(t, op)

	op.Start()

	workingStatus := &heartbeatpb.TableSpanStatus{
		ID:              op.ID().ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    2000,
	}
	op.Check(nodeA, workingStatus)
	require.True(t, op.IsFinished())

	op.OnNodeRemove(nodeA)
	require.True(t, op.aborted.Load())

	op.PostFinish()

	require.Nil(t, spanController.GetTaskByID(op.ID()))
	require.Equal(t, 2, spanController.GetAbsentSize())
	require.Equal(t, 0, spanController.GetSchedulingSize())
	require.Equal(t, 0, spanController.GetReplicatingSize())
	require.Equal(t, "", toMergedReplicaSets[0].GetNodeID().String())
	require.Equal(t, "", toMergedReplicaSets[1].GetNodeID().String())

	for _, occupyOp := range occupyOperators {
		require.True(t, occupyOp.IsFinished())
	}
}

// TestMergeOperator_TaskRemovedByDDLAfterWorking tests the scenario where:
// 1. A merge operation is initiated and the merged dispatcher reports working status
// 2. Then a DDL removes the task before PostFinish runs
// 3. Verify that the operator does not clear node binding of old spans
func TestMergeOperator_TaskRemovedByDDLAfterWorking(t *testing.T) {
	spanController, toMergedReplicaSets, occupyOperators, nodeA := setupMergeTestEnvironment(t)

	op := NewMergeDispatcherOperator(spanController, toMergedReplicaSets, occupyOperators)
	require.NotNil(t, op)

	op.Start()

	workingStatus := &heartbeatpb.TableSpanStatus{
		ID:              op.ID().ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    2000,
	}
	op.Check(nodeA, workingStatus)
	require.True(t, op.IsFinished())

	op.OnTaskRemoved()
	require.True(t, op.aborted.Load())
	require.True(t, op.abortedByDDL.Load())

	op.PostFinish()

	require.Nil(t, spanController.GetTaskByID(op.ID()))
	require.Equal(t, 0, spanController.GetAbsentSize())
	require.Equal(t, 0, spanController.GetSchedulingSize())
	require.Equal(t, 2, spanController.GetReplicatingSize())
	require.Equal(t, nodeA, toMergedReplicaSets[0].GetNodeID())
	require.Equal(t, nodeA, toMergedReplicaSets[1].GetNodeID())

	for _, occupyOp := range occupyOperators {
		require.True(t, occupyOp.IsFinished())
	}
}
