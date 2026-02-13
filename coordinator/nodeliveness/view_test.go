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
package nodeliveness

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestViewUnknownAfterTTL(t *testing.T) {
	v := NewView(30 * time.Second)
	id := node.ID("n1")

	// Never observed nodes should never become unknown.
	require.Equal(t, StateAlive, v.GetState(id))

	now := time.Now()
	v.ObserveHeartbeat(id, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	}, now.Add(-5*time.Second))
	require.Equal(t, StateAlive, v.GetState(id))

	v.ObserveHeartbeat(id, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	}, now.Add(-35*time.Second))
	require.Equal(t, StateUnknown, v.GetState(id))
}

func TestViewDestinationEligibility(t *testing.T) {
	v := NewView(30 * time.Second)
	now := time.Now()

	alive := node.ID("alive")
	draining := node.ID("draining")
	stopping := node.ID("stopping")

	v.ObserveHeartbeat(draining, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	}, now)
	v.ObserveHeartbeat(stopping, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: 1,
	}, now)

	require.True(t, v.IsSchedulableDest(alive))
	require.False(t, v.IsSchedulableDest(draining))
	require.False(t, v.IsSchedulableDest(stopping))
}

func TestViewGetNodeEpoch(t *testing.T) {
	v := NewView(30 * time.Second)
	now := time.Unix(0, 0)
	id := node.ID("n1")

	epoch, ok := v.GetNodeEpoch(id)
	require.False(t, ok)
	require.Equal(t, uint64(0), epoch)

	v.ObserveSetNodeLivenessResponse(id, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 123,
	}, now)

	epoch, ok = v.GetNodeEpoch(id)
	require.True(t, ok)
	require.Equal(t, uint64(123), epoch)
}

func TestViewGetDrainingOrStoppingNodes(t *testing.T) {
	v := NewView(30 * time.Second)
	now := time.Unix(0, 0)

	v.ObserveHeartbeat(node.ID("n1"), &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	}, now)
	v.ObserveHeartbeat(node.ID("n2"), &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: 1,
	}, now)
	v.ObserveHeartbeat(node.ID("n3"), &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	}, now)

	nodes := v.GetDrainingOrStoppingNodes(now)
	require.ElementsMatch(t, []node.ID{"n1", "n2"}, nodes)

	nodes = v.GetDrainingOrStoppingNodes(now.Add(31 * time.Second))
	require.Empty(t, nodes)
}
