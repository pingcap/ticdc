// Copyright 2026 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestNodeLivenessViewDefaultAliveBeforeHeartbeat(t *testing.T) {
	view := NewNodeLivenessView()
	require.True(t, view.IsSchedulableDestination(node.ID("node-1")))
}

func TestNodeLivenessViewTimeoutBecomesUnknown(t *testing.T) {
	now := time.Unix(0, 0)

	view := NewNodeLivenessView()
	view.now = func() time.Time { return now }
	view.ttl = 10 * time.Second

	n := node.ID("node-1")
	view.UpdateFromHeartbeat(n, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	})

	require.True(t, view.IsSchedulableDestination(n))

	now = now.Add(11 * time.Second)
	require.False(t, view.IsSchedulableDestination(n))
}

func TestNodeLivenessViewDrainingNotSchedulable(t *testing.T) {
	view := NewNodeLivenessView()
	view.UpdateFromHeartbeat(node.ID("node-1"), &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	})
	require.False(t, view.IsSchedulableDestination(node.ID("node-1")))
	require.True(t, view.IsDraining(node.ID("node-1")))
}

func TestDestNodeSelectorFiltersNonAliveDestinations(t *testing.T) {
	now := time.Unix(0, 0)

	view := NewNodeLivenessView()
	view.now = func() time.Time { return now }
	view.ttl = 10 * time.Second

	nodeManager := watcher.NewNodeManager(nil, nil)
	node1 := node.NewInfo("127.0.0.1:8301", "")
	node2 := node.NewInfo("127.0.0.1:8302", "")
	node3 := node.NewInfo("127.0.0.1:8303", "")

	nodeManager.GetAliveNodes()[node1.ID] = node1
	nodeManager.GetAliveNodes()[node2.ID] = node2
	nodeManager.GetAliveNodes()[node3.ID] = node3

	view.UpdateFromHeartbeat(node2.ID, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 2,
	})
	view.UpdateFromHeartbeat(node3.ID, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 3,
	})

	selector := &destNodeSelector{
		nodeManager:  nodeManager,
		livenessView: view,
	}

	ids := selector.GetSchedulableDestNodeIDs()
	require.Contains(t, ids, node1.ID)
	require.NotContains(t, ids, node2.ID)
	require.Contains(t, ids, node3.ID)

	now = now.Add(11 * time.Second)
	ids = selector.GetSchedulableDestNodeIDs()
	require.Contains(t, ids, node1.ID)
	require.NotContains(t, ids, node2.ID)
	require.NotContains(t, ids, node3.ID)
}
