// Copyright 2025 PingCAP, Inc.
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
	"container/heap"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

// mockReplicationID implements replica.ReplicationID interface for testing
type mockReplicationID struct {
	id string
}

func (m mockReplicationID) String() string {
	return m.id
}

// mockReplicationStatus implements replica.ReplicationStatus interface for testing
type mockReplicationStatus struct {
	status string
}

// mockOperator implements Operator interface for testing
type mockOperator struct {
	id       mockReplicationID
	status   mockReplicationStatus
	finished bool
	typ      string
	nodes    []node.ID
}

func (m mockOperator) GetID() mockReplicationID {
	return m.id
}

func (m mockOperator) GetStatus() mockReplicationStatus {
	return m.status
}

func (m mockOperator) ID() mockReplicationID {
	return m.id
}

func (m mockOperator) Type() string {
	return m.typ
}

func (m mockOperator) Start() {
}

func (m mockOperator) Schedule() *messaging.TargetMessage {
	return nil
}

func (m mockOperator) IsFinished() bool {
	return m.finished
}

func (m mockOperator) PostFinish() {
}

func (m mockOperator) Check(from node.ID, status mockReplicationStatus) {
}

func (m mockOperator) OnNodeRemove(id node.ID) {
}

func (m mockOperator) AffectedNodes() []node.ID {
	return m.nodes
}

func (m mockOperator) OnTaskRemoved() {
	// 测试用空实现
}

func (m mockOperator) String() string {
	return m.id.String()
}

func TestOperatorQueue(t *testing.T) {
	queue := make(OperatorQueue[mockReplicationID, mockReplicationStatus], 0)

	// Create test operators with different execution times
	now := time.Now()
	op1 := NewOperatorWithTime[mockReplicationID, mockReplicationStatus](
		mockOperator{
			id:       mockReplicationID{"op1"},
			status:   mockReplicationStatus{"status1"},
			finished: false,
			typ:      "test",
			nodes:    []node.ID{"node1"},
		},
		now.Add(time.Hour),
	)
	op2 := NewOperatorWithTime(
		mockOperator{id: mockReplicationID{"op2"}, status: mockReplicationStatus{"status2"}},
		now,
	)
	op3 := NewOperatorWithTime(
		mockOperator{id: mockReplicationID{"op3"}, status: mockReplicationStatus{"status3"}},
		now.Add(2*time.Hour),
	)

	// Test Push
	queue.Push(op1)
	require.Equal(t, 1, queue.Len())
	queue.Push(op2)
	queue.Push(op3)
	require.Equal(t, 3, queue.Len())

	// Test Less (sorting)
	require.True(t, queue.Less(1, 0), "op2 should be before op1")
	require.True(t, queue.Less(1, 2), "op2 should be before op3")

	heap.Init(&queue)

	popped := heap.Pop(&queue).(*OperatorWithTime[mockReplicationID, mockReplicationStatus])
	require.Equal(t, "op2", popped.OP.(mockOperator).id.String())

	popped = heap.Pop(&queue).(*OperatorWithTime[mockReplicationID, mockReplicationStatus])
	require.Equal(t, "op1", popped.OP.(mockOperator).id.String())

	popped = heap.Pop(&queue).(*OperatorWithTime[mockReplicationID, mockReplicationStatus])
	require.Equal(t, "op3", popped.OP.(mockOperator).id.String())
}
