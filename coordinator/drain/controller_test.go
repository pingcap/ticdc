package drain

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/nodeliveness"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestRemainingSafetyRules(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	view := nodeliveness.NewView(30 * time.Second)
	db := changefeed.NewChangefeedDB(1)

	c := NewController(mc, view, db, nil)
	target := node.ID("n1")

	now := time.Unix(0, 0)
	view.HandleNodeHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 101,
	}, now)

	c.RequestDrain(target)
	require.GreaterOrEqual(t, c.Remaining(target), 1)

	msg := recvMessage(t, mc)
	require.Equal(t, messaging.TypeSetNodeLivenessRequest, msg.Type)
	req := msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	require.Equal(t, heartbeatpb.NodeLiveness_DRAINING, req.Target)
	require.Equal(t, uint64(101), req.NodeEpoch)

	// Draining observed, but STOPPING not observed => remaining must stay non-zero.
	view.HandleSetNodeLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 101,
	}, now)
	require.Equal(t, 1, c.remaining(target, now))

	// STOPPING observed and no work => remaining can be 0.
	view.HandleSetNodeLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: 101,
	}, now)
	require.Equal(t, 0, c.remaining(target, now))
}

func TestTickPromoteToStoppingWhenQuiescent(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	view := nodeliveness.NewView(30 * time.Second)
	db := changefeed.NewChangefeedDB(1)

	c := NewController(mc, view, db, nil)
	target := node.ID("n1")

	now := time.Unix(0, 0)
	view.HandleNodeHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 101,
	}, now)

	cfID := common.NewChangeFeedIDWithName("cf1", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	db.AddReplicatingMaintainer(cf, target)

	// Not quiescent => no STOPPING request.
	c.tick(now)
	select {
	case msg := <-mc.GetMessageChannel():
		t.Fatalf("unexpected message: %v", msg)
	default:
	}

	// Quiescent => should promote to STOPPING.
	db.StopByChangefeedID(cfID, true)
	c.tick(now.Add(2 * time.Second))
	msg := recvMessage(t, mc)
	req := msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	require.Equal(t, heartbeatpb.NodeLiveness_STOPPING, req.Target)
}

func recvMessage(t *testing.T, mc interface {
	GetMessageChannel() chan *messaging.TargetMessage
},
) *messaging.TargetMessage {
	t.Helper()
	select {
	case msg := <-mc.GetMessageChannel():
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting message")
		return nil
	}
}
