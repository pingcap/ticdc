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

	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/stretchr/testify/require"
)

// TestRemoveOperator_NodeRemovedBeforeStopped tests the scenario where:
// 1. A remove operation is initiated to remove dispatcher from node A
// 2. Before node A reports stopped status, node A is removed
// 3. Verify that the operator is marked as finished immediately
func TestRemoveOperator_NodeRemovedBeforeStopped(t *testing.T) {
	spanController, _, replicaSet, nodeA, _ := setupTestEnvironment(t)

	op := newRemoveDispatcherOperator(spanController, replicaSet)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	// Verify that remove message is scheduled
	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, messaging.TypeScheduleDispatcherRequest, msg.Type)
	require.Equal(t, nodeA.String(), msg.To.String())

	// Node A is removed before it reports stopped status
	op.OnNodeRemove(nodeA)

	// Verify that the operator is marked as finished
	require.True(t, op.IsFinished())

	op.PostFinish()
}
