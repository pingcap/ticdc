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

package scheduler

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

// mockDrainState implements DrainStateProvider for testing
type mockDrainState struct {
	drainingTarget node.ID
	cleared        bool
}

func (m *mockDrainState) GetDrainingTarget() node.ID {
	return m.drainingTarget
}

func (m *mockDrainState) IsDraining() bool {
	return m.drainingTarget != ""
}

func (m *mockDrainState) ClearDrainingTarget() {
	m.cleared = true
	m.drainingTarget = ""
}

// mockNodeManager implements NodeManagerProvider for testing
type mockNodeManager struct {
	schedulableNodes map[node.ID]*node.Info
}

func (m *mockNodeManager) GetSchedulableNodes() map[node.ID]*node.Info {
	return m.schedulableNodes
}

func TestDrainScheduler_NoDrainingTarget(t *testing.T) {
	t.Parallel()

	drainState := &mockDrainState{drainingTarget: ""}
	nodeManager := &mockNodeManager{
		schedulableNodes: map[node.ID]*node.Info{
			"node-1": {ID: "node-1"},
		},
	}

	scheduler := NewDrainScheduler("test", 1, nil, nil, drainState, nodeManager)

	// Should return quickly when no draining target
	nextTime := scheduler.Execute()
	require.NotZero(t, nextTime)
}

func TestDrainScheduler_NoSchedulableNodes(t *testing.T) {
	t.Parallel()

	drainState := &mockDrainState{drainingTarget: "node-1"}
	nodeManager := &mockNodeManager{
		schedulableNodes: map[node.ID]*node.Info{},
	}

	scheduler := NewDrainScheduler("test", 1, nil, nil, drainState, nodeManager)

	// Should handle no schedulable nodes gracefully
	nextTime := scheduler.Execute()
	require.NotZero(t, nextTime)
}

func TestDrainScheduler_Name(t *testing.T) {
	t.Parallel()

	scheduler := NewDrainScheduler("test", 1, nil, nil, nil, nil)
	require.Equal(t, "drain-scheduler", scheduler.Name())
}

func TestDrainScheduler_DefaultBatchSize(t *testing.T) {
	t.Parallel()

	// Test with invalid batch size
	scheduler := NewDrainScheduler("test", 0, nil, nil, nil, nil)
	require.Equal(t, 1, scheduler.batchSize)

	scheduler = NewDrainScheduler("test", -1, nil, nil, nil, nil)
	require.Equal(t, 1, scheduler.batchSize)

	// Test with valid batch size
	scheduler = NewDrainScheduler("test", 5, nil, nil, nil, nil)
	require.Equal(t, 5, scheduler.batchSize)
}

func TestDrainScheduler_SelectDestination(t *testing.T) {
	t.Parallel()

	// This test requires a mock changefeedDB which is complex to set up
	// For now, we just verify the scheduler can be created
	scheduler := NewDrainScheduler("test", 1, nil, nil, nil, nil)
	require.NotNil(t, scheduler)
}
