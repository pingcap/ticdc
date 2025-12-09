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
	"github.com/stretchr/testify/require"
)

// TestAddOperator_DestNodeRemoved tests the scenario where:
// 1. An add operation is initiated to create dispatcher on node A
// 2. Before node A reports working status, node A is removed
// 3. Verify that the operator is marked as finished and removed
// 4. Verify that the span is marked as absent for rescheduling
func TestAddOperator_DestNodeRemoved(t *testing.T) {
	spanController, changefeedID, _, _, nodeB := setupTestEnvironment(t)

	// Create a new replica set for adding
	dispatcherID := common.NewDispatcherID()
	tableSpan := &heartbeatpb.TableSpan{
		TableID:  200,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
	}

	absentReplicaSet := replica.NewSpanReplication(
		changefeedID,
		dispatcherID,
		2,
		tableSpan,
		1000,
		common.DefaultMode,
		false,
	)

	spanController.AddAbsentReplicaSet(absentReplicaSet)

	op := NewAddDispatcherOperator(spanController, absentReplicaSet, nodeB)
	require.NotNil(t, op)

	op.Start()
	require.False(t, op.IsFinished())

	// Verify that the span is bound to nodeB
	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, nodeB.String(), msg.To.String())

	// Node B is removed before it reports working status
	op.OnNodeRemove(nodeB)

	require.True(t, op.IsFinished())
	require.True(t, op.removed.Load())

	op.PostFinish()

	// Verify that the span is marked as absent
	require.Equal(t, 1, spanController.GetAbsentSize())
}
