package drain

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/coordinator/nodeliveness"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestDrainControllerResendAndPromoteToStopping(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	view := nodeliveness.NewView(30 * time.Second)
	c := NewController(mc, view, time.Second)

	target := node.ID("n1")
	now := time.Now()
	view.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 42,
	}, now)

	c.RequestDrain(target, now)
	msg := <-mc.GetMessageChannel()
	require.Equal(t, messaging.TypeSetNodeLivenessRequest, msg.Type)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
	require.Equal(t, target, msg.To)
	req := msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	require.Equal(t, heartbeatpb.NodeLiveness_DRAINING, req.Target)
	require.Equal(t, uint64(42), req.NodeEpoch)

	// Before draining observed, it should retry after the resend interval.
	c.Tick(now.Add(500*time.Millisecond), nil)
	select {
	case <-mc.GetMessageChannel():
		require.FailNow(t, "unexpected command before resend interval")
	default:
	}

	c.Tick(now.Add(time.Second+10*time.Millisecond), nil)
	msg = <-mc.GetMessageChannel()
	req = msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	require.Equal(t, heartbeatpb.NodeLiveness_DRAINING, req.Target)

	c.ObserveSetNodeLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 42,
	})

	// Once readyToStop, it should send STOPPING.
	c.Tick(now.Add(2*time.Second), func(node.ID) bool { return true })
	msg = <-mc.GetMessageChannel()
	req = msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	require.Equal(t, heartbeatpb.NodeLiveness_STOPPING, req.Target)
}
