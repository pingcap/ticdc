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
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

// TestSplitOperator_OriginNodeRemovedBeforeStopped tests the scenario where:
// 1. A split operation is initiated to split dispatcher on node A
// 2. Before node A reports stopped status, node A is removed
// 3. Verify that the operator is marked as finished and removed
// 4. Verify that the span is marked as absent, and split is not executed
func TestSplitOperator_OriginNodeRemovedBeforeStopped(t *testing.T) {
	spanController, _, replicaSet, nodeA, _ := setupTestEnvironment(t)

	// Define split spans
	splitSpans := []*heartbeatpb.TableSpan{
		{
			TableID:  100,
			StartKey: []byte("a"),
			EndKey:   []byte("m"),
		},
		{
			TableID:  100,
			StartKey: []byte("m"),
			EndKey:   []byte("z"),
		},
	}

	// Split targets are empty, meaning let scheduler decide
	splitTargetNodes := []node.ID{"", ""}

	op := NewSplitDispatcherOperator(spanController, replicaSet, splitSpans, splitTargetNodes, nil)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	// Verify that remove message is scheduled
	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, messaging.TypeScheduleDispatcherRequest, msg.Type)

	// Node A is removed before it reports stopped status
	op.OnNodeRemove(nodeA)

	require.True(t, op.IsFinished())
	require.True(t, op.removed.Load())

	// Get the absent size before PostFinish
	absentSizeBefore := spanController.GetAbsentSize()

	op.PostFinish()

	// Verify that the original span is marked as absent (not split executed)
	require.Equal(t, absentSizeBefore+1, spanController.GetAbsentSize())
}

// TestSplitOperator_OriginNodeRemovedAfterStopped tests the scenario where:
// 1. A split operation is initiated to split dispatcher on node A
// 2. Node A reports stopped status first
// 3. Then node A is removed
// 4. Verify that the span is still marked as absent, and split is not executed
func TestSplitOperator_OriginNodeRemovedAfterStopped(t *testing.T) {
	spanController, _, replicaSet, nodeA, _ := setupTestEnvironment(t)

	// Define split spans
	splitSpans := []*heartbeatpb.TableSpan{
		{
			TableID:  100,
			StartKey: []byte("a"),
			EndKey:   []byte("m"),
		},
		{
			TableID:  100,
			StartKey: []byte("m"),
			EndKey:   []byte("z"),
		},
	}

	// Split targets are empty, meaning let scheduler decide
	splitTargetNodes := []node.ID{"", ""}

	op := NewSplitDispatcherOperator(spanController, replicaSet, splitSpans, splitTargetNodes, nil)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	// Node A reports stopped status
	stoppedStatus := &heartbeatpb.TableSpanStatus{
		ID:              replicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
		CheckpointTs:    1500,
	}
	op.Check(nodeA, stoppedStatus)

	require.True(t, op.IsFinished())
	require.False(t, op.removed.Load())

	// Node A is removed after it reported stopped status
	op.OnNodeRemove(nodeA)

	require.True(t, op.IsFinished())
	require.True(t, op.removed.Load())

	// Get the absent size before PostFinish
	absentSizeBefore := spanController.GetAbsentSize()

	op.PostFinish()

	// Verify that the original span is marked as absent (split is not executed even after stopped)
	require.Equal(t, absentSizeBefore+1, spanController.GetAbsentSize())
}

// TestSplitOperator_SuccessfulSplitCreatesAbsentSpans tests the scenario where:
// 1. A split operation is initiated to split dispatcher on node A
// 2. Node A reports stopped status
// 3. Verify that PostFinish replaces the original span with new absent spans
func TestSplitOperator_SuccessfulSplitCreatesAbsentSpans(t *testing.T) {
	spanController, _, replicaSet, nodeA, _ := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	splitSpans := []*heartbeatpb.TableSpan{
		{
			TableID:  100,
			StartKey: []byte("a"),
			EndKey:   []byte("m"),
		},
		{
			TableID:  100,
			StartKey: []byte("m"),
			EndKey:   []byte("z"),
		},
	}

	op := NewSplitDispatcherOperator(spanController, replicaSet, splitSpans, []node.ID{}, nil)
	require.NotNil(t, op)

	op.Start()

	stoppedStatus := &heartbeatpb.TableSpanStatus{
		ID:              replicaSet.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
		CheckpointTs:    1500,
	}
	op.Check(nodeA, stoppedStatus)
	require.True(t, op.IsFinished())
	require.False(t, op.removed.Load())

	op.PostFinish()

	require.Nil(t, spanController.GetTaskByID(replicaSet.ID))
	require.Equal(t, 2, len(spanController.GetTasksByTableID(100)))
	require.Equal(t, 2, spanController.GetAbsentSize())
	for _, s := range spanController.GetTasksByTableID(100) {
		require.Equal(t, "", s.GetNodeID().String())
	}
}

// TestSplitOperator_SuccessfulSplitToSchedulingTargets tests the scenario where:
// 1. A split operation is initiated to split dispatcher on node A
// 2. Node A reports stopped status
// 3. Verify that PostFinish replaces the original span with new scheduling spans with target nodes
func TestSplitOperator_SuccessfulSplitToSchedulingTargets(t *testing.T) {
	spanController, changefeedID, _, nodeA, nodeB := setupTestEnvironment(t)

	dispatcherID1 := common.NewDispatcherID()
	spanToSplit := &heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
	}
	status1 := &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID1.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    1000,
	}
	replicaSetToSplit := replica.NewWorkingSpanReplication(
		changefeedID,
		dispatcherID1,
		1,
		spanToSplit,
		status1,
		nodeA,
		false,
	)
	spanController.AddReplicatingSpan(replicaSetToSplit)

	// Keep one more replica in the same table group so that ReplaceReplicaSet will not remove the group
	// before adding new scheduling replicas.
	dispatcherID2 := common.NewDispatcherID()
	spanToKeep := &heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
	}
	status2 := &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID2.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    1000,
	}
	replicaSetToKeep := replica.NewWorkingSpanReplication(
		changefeedID,
		dispatcherID2,
		1,
		spanToKeep,
		status2,
		nodeA,
		false,
	)
	spanController.AddReplicatingSpan(replicaSetToKeep)

	splitSpans := []*heartbeatpb.TableSpan{
		{
			TableID:  100,
			StartKey: []byte("a"),
			EndKey:   []byte("h"),
		},
		{
			TableID:  100,
			StartKey: []byte("h"),
			EndKey:   []byte("m"),
		},
	}
	splitTargetNodes := []node.ID{nodeA, nodeB}

	op := NewSplitDispatcherOperator(spanController, replicaSetToSplit, splitSpans, splitTargetNodes, nil)
	require.NotNil(t, op)

	op.Start()

	stoppedStatus := &heartbeatpb.TableSpanStatus{
		ID:              replicaSetToSplit.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
		CheckpointTs:    1500,
	}
	op.Check(nodeA, stoppedStatus)
	require.True(t, op.IsFinished())

	op.PostFinish()

	require.Nil(t, spanController.GetTaskByID(replicaSetToSplit.ID))
	require.Equal(t, 0, spanController.GetAbsentSize())
	require.Equal(t, 2, spanController.GetSchedulingSize())

	nodeSeen := map[string]bool{}
	for _, s := range spanController.GetTasksByTableID(100) {
		nodeSeen[s.GetNodeID().String()] = true
	}
	require.True(t, nodeSeen[nodeA.String()])
	require.True(t, nodeSeen[nodeB.String()])
}

// TestSplitOperator_PostFinishCallbackFailureMarksSpanAbsent tests the scenario where:
// 1. A split operation is initiated with splitTargetNodes and a postFinish callback
// 2. Node A reports stopped status
// 3. postFinish fails for one new span
// 4. Verify that the failed span is marked absent for rescheduling
func TestSplitOperator_PostFinishCallbackFailureMarksSpanAbsent(t *testing.T) {
	spanController, changefeedID, _, nodeA, nodeB := setupTestEnvironment(t)

	dispatcherID1 := common.NewDispatcherID()
	spanToSplit := &heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
	}
	status1 := &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID1.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    1000,
	}
	replicaSetToSplit := replica.NewWorkingSpanReplication(
		changefeedID,
		dispatcherID1,
		1,
		spanToSplit,
		status1,
		nodeA,
		false,
	)
	spanController.AddReplicatingSpan(replicaSetToSplit)

	dispatcherID2 := common.NewDispatcherID()
	spanToKeep := &heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
	}
	status2 := &heartbeatpb.TableSpanStatus{
		ID:              dispatcherID2.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    1000,
	}
	replicaSetToKeep := replica.NewWorkingSpanReplication(
		changefeedID,
		dispatcherID2,
		1,
		spanToKeep,
		status2,
		nodeA,
		false,
	)
	spanController.AddReplicatingSpan(replicaSetToKeep)

	splitSpans := []*heartbeatpb.TableSpan{
		{
			TableID:  100,
			StartKey: []byte("a"),
			EndKey:   []byte("h"),
		},
		{
			TableID:  100,
			StartKey: []byte("h"),
			EndKey:   []byte("m"),
		},
	}
	splitTargetNodes := []node.ID{nodeA, nodeB}

	op := NewSplitDispatcherOperator(spanController, replicaSetToSplit, splitSpans, splitTargetNodes,
		func(_ *replica.SpanReplication, target node.ID) bool {
			return target != nodeB
		})
	require.NotNil(t, op)

	op.Start()

	stoppedStatus := &heartbeatpb.TableSpanStatus{
		ID:              replicaSetToSplit.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Stopped,
		CheckpointTs:    1500,
	}
	op.Check(nodeA, stoppedStatus)
	require.True(t, op.IsFinished())

	op.PostFinish()

	require.Equal(t, 1, spanController.GetAbsentSize())
	require.Equal(t, 1, spanController.GetSchedulingSize())

	nodeIDs := make(map[string]struct{})
	for _, s := range spanController.GetTasksByTableID(100) {
		nodeIDs[s.GetNodeID().String()] = struct{}{}
	}
	_, hasNodeA := nodeIDs[nodeA.String()]
	_, hasEmpty := nodeIDs[""]
	require.True(t, hasNodeA)
	require.True(t, hasEmpty)
}

// TestSplitOperator_TaskRemovedByDDLDoesNotSplit tests the scenario where:
// 1. A split operation is initiated to split dispatcher on node A
// 2. The task is removed (for example, due to DDL) before the origin dispatcher stops
// 3. Verify that PostFinish does not execute ReplaceReplicaSet and does not create new spans
func TestSplitOperator_TaskRemovedByDDLDoesNotSplit(t *testing.T) {
	spanController, _, replicaSet, _, _ := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	splitSpans := []*heartbeatpb.TableSpan{
		{
			TableID:  100,
			StartKey: []byte("a"),
			EndKey:   []byte("m"),
		},
		{
			TableID:  100,
			StartKey: []byte("m"),
			EndKey:   []byte("z"),
		},
	}

	op := NewSplitDispatcherOperator(spanController, replicaSet, splitSpans, []node.ID{}, nil)
	require.NotNil(t, op)

	op.Start()
	op.OnTaskRemoved()
	require.True(t, op.IsFinished())
	require.True(t, op.removed.Load())

	op.PostFinish()

	require.Equal(t, 1, len(spanController.GetTasksByTableID(100)))
	require.Equal(t, 1, spanController.GetAbsentSize())
}
