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

	"github.com/stretchr/testify/require"
)

// TestOccupyOperator_NodeRemoved tests the scenario where:
// 1. An occupy operator is created to hold a replica set
// 2. Removing an unrelated node does not affect the operator
// 3. Removing the replica's node marks the span absent and finishes the operator
func TestOccupyOperator_NodeRemoved(t *testing.T) {
	spanController, _, replicaSet, nodeA, nodeB := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	op := NewOccupyDispatcherOperator(spanController, replicaSet)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	// Call OnNodeRemove with another node
	op.OnNodeRemove(nodeB)

	// Verify that the operator state is not affected
	require.False(t, op.IsFinished())
	require.False(t, op.removed.Load())

	absentSizeBefore := spanController.GetAbsentSize()
	// Call OnNodeRemove with the node holding the replica set
	op.OnNodeRemove(nodeA)
	// Verify that the span is marked absent and operator is finished
	require.True(t, op.IsFinished())
	require.False(t, op.removed.Load())
	require.Equal(t, absentSizeBefore+1, spanController.GetAbsentSize())

	// Verify that Schedule returns nil
	msg := op.Schedule()
	require.Nil(t, msg)

	op.PostFinish()
}

// TestOccupyOperator_TaskRemoved tests the scenario where:
// 1. An occupy operator is created to hold a replica set
// 2. The task is removed (for example, due to DDL) while occupy operator is running
// 3. Verify that the operator finishes without changing span state
func TestOccupyOperator_TaskRemoved(t *testing.T) {
	spanController, _, replicaSet, nodeA, _ := setupTestEnvironment(t)
	spanController.AddReplicatingSpan(replicaSet)

	op := NewOccupyDispatcherOperator(spanController, replicaSet)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	absentSizeBefore := spanController.GetAbsentSize()
	op.OnTaskRemoved()
	require.True(t, op.IsFinished())
	require.True(t, op.removed.Load())
	require.Equal(t, absentSizeBefore, spanController.GetAbsentSize())
	require.Equal(t, nodeA, replicaSet.GetNodeID())

	op.PostFinish()
}
