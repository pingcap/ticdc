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
// 2. A node is removed
// 3. Verify that the operator is not affected by node removal
// 4. The occupy operator is a helper operator that doesn't respond to node removal
func TestOccupyOperator_NodeRemoved(t *testing.T) {
	spanController, _, replicaSet, nodeA, nodeB := setupTestEnvironment(t)

	op := NewOccupyDispatcherOperator(spanController, replicaSet)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	// Call OnNodeRemove with the node holding the replica set
	op.OnNodeRemove(nodeA)

	// Verify that the operator state is not affected (OnNodeRemove is empty implementation)
	require.False(t, op.IsFinished())
	require.False(t, op.removed.Load())

	// Call OnNodeRemove with another node
	op.OnNodeRemove(nodeB)

	// Verify that the operator state is still not affected
	require.False(t, op.IsFinished())
	require.False(t, op.removed.Load())

	// Verify that Schedule returns nil
	msg := op.Schedule()
	require.Nil(t, msg)

	// Manually finish the operator (as would be done by the caller)
	op.SetFinished()
	require.True(t, op.IsFinished())

	op.PostFinish()
}
