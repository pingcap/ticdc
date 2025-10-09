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
