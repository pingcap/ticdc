package nodeliveness

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestViewUnknownTTLGuard(t *testing.T) {
	v := NewView(30 * time.Second)
	n1 := node.ID("n1")
	n2 := node.ID("n2")

	now := time.Unix(0, 0)
	require.Equal(t, StateAlive, v.GetState(n1, now))

	v.HandleNodeHeartbeat(n1, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	}, now)

	require.Equal(t, StateAlive, v.GetState(n1, now.Add(29*time.Second)))
	require.Equal(t, StateUnknown, v.GetState(n1, now.Add(31*time.Second)))

	// Never-seen nodes must not become UNKNOWN.
	require.Equal(t, StateAlive, v.GetState(n2, now.Add(31*time.Second)))
}

func TestFilterSchedulableDestNodes(t *testing.T) {
	v := NewView(30 * time.Second)
	now := time.Unix(0, 0)

	draining := node.ID("draining")
	alive := node.ID("alive")
	unseen := node.ID("unseen")

	v.HandleNodeHeartbeat(draining, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	}, now)
	v.HandleNodeHeartbeat(alive, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	}, now)

	nodes := map[node.ID]*node.Info{
		draining: node.NewInfo("127.0.0.1:1", ""),
		alive:    node.NewInfo("127.0.0.1:2", ""),
		unseen:   node.NewInfo("127.0.0.1:3", ""),
	}

	filtered := v.FilterSchedulableDestNodes(nodes, now)
	require.Contains(t, filtered, alive)
	require.Contains(t, filtered, unseen)
	require.NotContains(t, filtered, draining)
}
