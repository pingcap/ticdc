package maintainer

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/api"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestSetNodeLivenessRejectEpochMismatch(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var liveness api.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &liveness)
	m.coordinatorID = node.ID("coordinator")
	m.coordinatorVersion = 1

	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: m.nodeEpoch + 1,
	}
	msg := messaging.NewSingleTargetMessage(m.nodeInfo.ID, messaging.MaintainerManagerTopic, req)
	msg.From = m.coordinatorID

	m.onSetNodeLivenessRequest(msg)

	out := <-mc.GetMessageChannel()
	require.Equal(t, messaging.TypeSetNodeLivenessResponse, out.Type)
	resp := out.Message[0].(*heartbeatpb.SetNodeLivenessResponse)
	require.Equal(t, heartbeatpb.NodeLiveness_ALIVE, resp.Applied)
	require.Equal(t, m.nodeEpoch, resp.NodeEpoch)
	require.Equal(t, api.LivenessCaptureAlive, liveness.Load())
}

func TestSetNodeLivenessApplyTransition(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var liveness api.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &liveness)
	m.coordinatorID = node.ID("coordinator")
	m.coordinatorVersion = 1

	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: m.nodeEpoch,
	}
	msg := messaging.NewSingleTargetMessage(m.nodeInfo.ID, messaging.MaintainerManagerTopic, req)
	msg.From = m.coordinatorID

	m.onSetNodeLivenessRequest(msg)

	// Successful transition sends both a node heartbeat and a response.
	first := <-mc.GetMessageChannel()
	second := <-mc.GetMessageChannel()

	require.ElementsMatch(t, []messaging.IOType{messaging.TypeNodeHeartbeatRequest, messaging.TypeSetNodeLivenessResponse},
		[]messaging.IOType{first.Type, second.Type})
	require.Equal(t, api.LivenessCaptureDraining, liveness.Load())
}
