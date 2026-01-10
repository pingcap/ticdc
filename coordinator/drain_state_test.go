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

package coordinator

import (
	"sync"
	"testing"

	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func newTestNodeManager() *watcher.NodeManager {
	nm := watcher.NewNodeManagerForTest()
	return nm
}

func TestDrainState_SetDrainingTarget(t *testing.T) {
	t.Parallel()

	nm := newTestNodeManager()
	ds := NewDrainState(nm)

	nodeID := node.ID("node-1")

	// First drain should succeed
	err := ds.SetDrainingTarget(nodeID, 5, 10)
	require.NoError(t, err)
	require.Equal(t, nodeID, ds.GetDrainingTarget())
	require.True(t, ds.IsDraining())
	require.Equal(t, node.LivenessCaptureDraining, nm.GetNodeLiveness(nodeID))

	// Second drain should fail
	err = ds.SetDrainingTarget(node.ID("node-2"), 3, 6)
	require.Error(t, err)
	require.Contains(t, err.Error(), "another drain operation is in progress")
	require.Equal(t, nodeID, ds.GetDrainingTarget())
}

func TestDrainState_ClearDrainingTarget(t *testing.T) {
	t.Parallel()

	nm := newTestNodeManager()
	ds := NewDrainState(nm)

	nodeID := node.ID("node-1")

	// Set draining target
	err := ds.SetDrainingTarget(nodeID, 5, 10)
	require.NoError(t, err)

	// Clear draining target
	ds.ClearDrainingTarget()
	require.Equal(t, node.ID(""), ds.GetDrainingTarget())
	require.False(t, ds.IsDraining())
	require.Equal(t, node.LivenessCaptureStopping, nm.GetNodeLiveness(nodeID))
}

func TestDrainState_ClearDrainingTargetWithoutTransition(t *testing.T) {
	t.Parallel()

	nm := newTestNodeManager()
	ds := NewDrainState(nm)

	nodeID := node.ID("node-1")

	// Set draining target
	err := ds.SetDrainingTarget(nodeID, 5, 10)
	require.NoError(t, err)

	// Clear without transition (reset to Alive)
	ds.ClearDrainingTargetWithoutTransition()
	require.Equal(t, node.ID(""), ds.GetDrainingTarget())
	require.False(t, ds.IsDraining())
	require.Equal(t, node.LivenessCaptureAlive, nm.GetNodeLiveness(nodeID))
}

func TestDrainState_ClearDrainingTargetOnNodeRemove(t *testing.T) {
	t.Parallel()

	nm := newTestNodeManager()
	ds := NewDrainState(nm)

	nodeID := node.ID("node-1")
	otherNodeID := node.ID("node-2")

	// Set draining target
	err := ds.SetDrainingTarget(nodeID, 5, 10)
	require.NoError(t, err)

	// Remove a different node - should not affect drain state
	ds.ClearDrainingTargetOnNodeRemove(otherNodeID)
	require.Equal(t, nodeID, ds.GetDrainingTarget())
	require.True(t, ds.IsDraining())

	// Remove the draining node - should clear drain state
	ds.ClearDrainingTargetOnNodeRemove(nodeID)
	require.Equal(t, node.ID(""), ds.GetDrainingTarget())
	require.False(t, ds.IsDraining())
}

func TestDrainState_GetDrainProgress(t *testing.T) {
	t.Parallel()

	nm := newTestNodeManager()
	ds := NewDrainState(nm)

	nodeID := node.ID("node-1")

	// Set draining target
	err := ds.SetDrainingTarget(nodeID, 5, 10)
	require.NoError(t, err)

	target, startTime, maintainerCount, dispatcherCount := ds.GetDrainProgress()
	require.Equal(t, nodeID, target)
	require.False(t, startTime.IsZero())
	require.Equal(t, 5, maintainerCount)
	require.Equal(t, 10, dispatcherCount)
}

func TestDrainState_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	nm := newTestNodeManager()
	ds := NewDrainState(nm)

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	// Multiple goroutines try to set draining target
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nodeID := node.ID("node-" + string(rune('0'+idx)))
			if err := ds.SetDrainingTarget(nodeID, 1, 1); err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Only one should succeed
	require.Equal(t, 1, successCount)
	require.True(t, ds.IsDraining())
}
