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

package watcher

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestNodeManagerLiveness(t *testing.T) {
	t.Parallel()

	nm := &NodeManager{}
	nm.nodes.Store(&map[node.ID]*node.Info{})

	nodeID1 := node.ID("node-1")
	nodeID2 := node.ID("node-2")
	nodeID3 := node.ID("node-3")

	t.Run("GetNodeLiveness returns Alive for untracked node", func(t *testing.T) {
		liveness := nm.GetNodeLiveness(nodeID1)
		require.Equal(t, node.LivenessCaptureAlive, liveness)
	})

	t.Run("SetNodeLiveness and GetNodeLiveness", func(t *testing.T) {
		nm.SetNodeLiveness(nodeID1, node.LivenessCaptureDraining)
		require.Equal(t, node.LivenessCaptureDraining, nm.GetNodeLiveness(nodeID1))

		nm.SetNodeLiveness(nodeID1, node.LivenessCaptureStopping)
		require.Equal(t, node.LivenessCaptureStopping, nm.GetNodeLiveness(nodeID1))
	})

	t.Run("ClearNodeLiveness", func(t *testing.T) {
		nm.SetNodeLiveness(nodeID2, node.LivenessCaptureDraining)
		require.Equal(t, node.LivenessCaptureDraining, nm.GetNodeLiveness(nodeID2))

		nm.ClearNodeLiveness(nodeID2)
		require.Equal(t, node.LivenessCaptureAlive, nm.GetNodeLiveness(nodeID2))
	})

	t.Run("GetSchedulableNodes excludes draining and stopping nodes", func(t *testing.T) {
		// Setup nodes
		nodes := map[node.ID]*node.Info{
			nodeID1: {ID: nodeID1},
			nodeID2: {ID: nodeID2},
			nodeID3: {ID: nodeID3},
		}
		nm.nodes.Store(&nodes)

		// Set liveness states
		nm.SetNodeLiveness(nodeID1, node.LivenessCaptureAlive)
		nm.SetNodeLiveness(nodeID2, node.LivenessCaptureDraining)
		nm.SetNodeLiveness(nodeID3, node.LivenessCaptureStopping)

		schedulable := nm.GetSchedulableNodes()
		require.Len(t, schedulable, 1)
		require.Contains(t, schedulable, nodeID1)
		require.NotContains(t, schedulable, nodeID2)
		require.NotContains(t, schedulable, nodeID3)
	})

	t.Run("GetCoordinatorCandidates excludes draining and stopping nodes", func(t *testing.T) {
		// Setup nodes
		nodes := map[node.ID]*node.Info{
			nodeID1: {ID: nodeID1},
			nodeID2: {ID: nodeID2},
			nodeID3: {ID: nodeID3},
		}
		nm.nodes.Store(&nodes)

		// Set liveness states
		nm.SetNodeLiveness(nodeID1, node.LivenessCaptureAlive)
		nm.SetNodeLiveness(nodeID2, node.LivenessCaptureDraining)
		nm.SetNodeLiveness(nodeID3, node.LivenessCaptureStopping)

		candidates := nm.GetCoordinatorCandidates()
		require.Len(t, candidates, 1)
		require.Contains(t, candidates, nodeID1)
		require.NotContains(t, candidates, nodeID2)
		require.NotContains(t, candidates, nodeID3)
	})

	t.Run("GetSchedulableNodeIDs", func(t *testing.T) {
		// Setup nodes
		nodes := map[node.ID]*node.Info{
			nodeID1: {ID: nodeID1},
			nodeID2: {ID: nodeID2},
		}
		nm.nodes.Store(&nodes)

		nm.SetNodeLiveness(nodeID1, node.LivenessCaptureAlive)
		nm.SetNodeLiveness(nodeID2, node.LivenessCaptureDraining)

		ids := nm.GetSchedulableNodeIDs()
		require.Len(t, ids, 1)
		require.Contains(t, ids, nodeID1)
	})
}
