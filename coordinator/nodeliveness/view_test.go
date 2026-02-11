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
	now := time.Unix(0, 0)

	// Never observed nodes should never become unknown.
	require.Equal(t, StateAlive, v.GetState(id, now.Add(10*time.Minute)))

	v.ObserveHeartbeat(id, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	}, now)
	require.Equal(t, StateAlive, v.GetState(id, now.Add(29*time.Second)))
	require.Equal(t, StateUnknown, v.GetState(id, now.Add(31*time.Second)))
}

func TestViewDestinationEligibility(t *testing.T) {
	v := NewView(30 * time.Second)
	now := time.Unix(0, 0)

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

	require.True(t, v.IsSchedulableDest(alive, now))
	require.False(t, v.IsSchedulableDest(draining, now))
	require.False(t, v.IsSchedulableDest(stopping, now))
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
