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

// setupMergeTestEnvironment creates multiple replica sets for merge testing
func setupMergeTestEnvironment(t *testing.T) (*span.Controller, []*replica.SpanReplication, []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus], node.ID) {
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
		CheckpointTs:    1000,
	}
	replicaSet1 := replica.NewWorkingSpanReplication(
		changefeedID,
		dispatcherID1,
		1,
		span1,
		status1,
		nodeA,
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
		CheckpointTs:    1000,
	}
	replicaSet2 := replica.NewWorkingSpanReplication(
		changefeedID,
		dispatcherID2,
		1,
		span2,
		status2,
		nodeA,
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
	require.True(t, op.removed.Load())

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
