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

package scheduler

import (
	"math/rand"
	"testing"

	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/stretchr/testify/require"
)

type testReplicationID string

func (id testReplicationID) String() string { return string(id) }

type testReplication struct {
	id     testReplicationID
	nodeID node.ID
}

func (r *testReplication) GetID() testReplicationID    { return r.id }
func (r *testReplication) GetGroupID() replica.GroupID { return replica.DefaultGroupID }
func (r *testReplication) GetNodeID() node.ID          { return r.nodeID }
func (r *testReplication) SetNodeID(nodeID node.ID)    { r.nodeID = nodeID }
func (r *testReplication) ShouldRun() bool             { return true }

func TestBalanceStopsAfterAllMovesAreRejected(t *testing.T) {
	nodes := map[node.ID]*node.Info{
		"node1": {ID: "node1"},
		"node2": {ID: "node2"},
		"node3": {ID: "node3"},
	}
	var replicating []*testReplication
	for i, nodeID := range []node.ID{"node1", "node1", "node2", "node2"} {
		replicating = append(replicating, &testReplication{
			id:     testReplicationID(rune('a' + i)),
			nodeID: nodeID,
		})
	}

	// Simulates a quiescing operator controller that rejects every move.
	calls := 0
	attempted := make(map[testReplicationID]bool)
	moved := Balance(10, rand.New(rand.NewSource(1)), nodes, replicating,
		func(r *testReplication, target node.ID) bool {
			calls++
			require.Equal(t, node.ID("node3"), target)
			require.False(t, attempted[r.id], "rejected pair must not be retried without progress")
			attempted[r.id] = true
			return false
		})
	require.Equal(t, 0, moved)
	require.Equal(t, len(replicating), calls)
}

func TestBalanceMovesTasksToEmptyNode(t *testing.T) {
	nodes := map[node.ID]*node.Info{
		"node1": {ID: "node1"},
		"node2": {ID: "node2"},
		"node3": {ID: "node3"},
	}
	var replicating []*testReplication
	for i, nodeID := range []node.ID{"node1", "node1", "node1", "node2", "node2", "node2"} {
		replicating = append(replicating, &testReplication{
			id:     testReplicationID(rune('a' + i)),
			nodeID: nodeID,
		})
	}

	moved := Balance(10, rand.New(rand.NewSource(1)), nodes, replicating,
		func(r *testReplication, target node.ID) bool {
			require.Equal(t, node.ID("node3"), target)
			return true
		})
	require.Equal(t, 2, moved)
}

func TestBalanceTriesOtherTargets(t *testing.T) {
	nodes := map[node.ID]*node.Info{
		"source": {ID: "source"}, "busy": {ID: "busy"}, "eligible": {ID: "eligible"},
	}
	var replicating []*testReplication
	for i, id := range []node.ID{"source", "source", "source", "source", "source", "eligible"} {
		replicating = append(replicating, &testReplication{id: testReplicationID(rune('a' + i)), nodeID: id})
	}
	rejected := 0
	moved := Balance(10, rand.New(rand.NewSource(1)), nodes, replicating, func(r *testReplication, target node.ID) bool {
		if target == "busy" {
			rejected++
			return false
		}
		require.Equal(t, node.ID("eligible"), target)
		r.SetNodeID(target)
		return true
	})
	require.Positive(t, rejected)
	require.Equal(t, 1, moved, "a rejected empty node must not hide another underloaded node")
}

func TestBalanceTriesOtherTasksAndSources(t *testing.T) {
	for _, eligible := range []testReplicationID{"c", "e"} {
		t.Run(eligible.String(), func(t *testing.T) {
			nodes := map[node.ID]*node.Info{
				"source1": {ID: "source1"}, "source2": {ID: "source2"}, "target": {ID: "target"},
			}
			var replicating []*testReplication
			for i, id := range []node.ID{"source1", "source1", "source1", "source1", "source2", "source2", "source2"} {
				replicating = append(replicating, &testReplication{id: testReplicationID(rune('a' + i)), nodeID: id})
			}
			moved := Balance(10, rand.New(rand.NewSource(1)), nodes, replicating, func(r *testReplication, target node.ID) bool {
				require.Equal(t, node.ID("target"), target)
				return r.id == eligible
			})
			require.Equal(t, 1, moved)
		})
	}
}

func TestCheckBalanceStatus(t *testing.T) {
	require.Equal(t, 0, CheckBalanceStatus(map[node.ID]int{
		"node1": 2,
	}, map[node.ID]*node.Info{}))

	// 4 tasks assigned 2 servers,and an empty server
	require.Equal(t, 1, CheckBalanceStatus(map[node.ID]int{
		"node1": 2,
		"node2": 2,
	}, map[node.ID]*node.Info{
		"node1": {ID: "node1"},
		"node2": {ID: "node2"},
		"node3": {ID: "node3"},
	}))

	require.Equal(t, 1, CheckBalanceStatus(map[node.ID]int{
		"node1": 2,
		"node2": 0,
	}, map[node.ID]*node.Info{
		"node1": {ID: "node1"},
		"node2": {ID: "node2"},
	}))

	require.Equal(t, 4, CheckBalanceStatus(map[node.ID]int{
		"node1": 16,
		"node2": 16,
		"node3": 16,
		"node4": 16,
		"node5": 16,
		"node6": 11,
	}, map[node.ID]*node.Info{
		"node1": {ID: "node1"},
		"node2": {ID: "node2"},
		"node3": {ID: "node3"},
		"node4": {ID: "node4"},
		"node5": {ID: "node5"},
		"node6": {ID: "node6"},
	}))
}
