package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/drain"
	"github.com/pingcap/ticdc/coordinator/nodeliveness"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/heartbeatpb"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func newDrainTestController(t *testing.T) (*Controller, *nodeliveness.View, *drain.Controller, node.ID) {
	t.Helper()

	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	view := nodeliveness.NewView(30 * time.Second)
	drainController := drain.NewController(mc, view, time.Second)

	db := changefeed.NewChangefeedDB(1)
	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, 10)

	target := node.ID("target")
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}

	c := &Controller{
		nodeManager:        nodeManager,
		changefeedDB:       db,
		operatorController: oc,
		nodeLivenessView:   view,
		drainController:    drainController,
	}
	return c, view, drainController, target
}

func TestDrainNodeRemainingNeverZeroBeforeObserved(t *testing.T) {
	c, _, _, target := newDrainTestController(t)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)
}

func TestDrainNodeRemainingNeverZeroWithoutStoppingObserved(t *testing.T) {
	c, view, drainController, target := newDrainTestController(t)

	now := time.Now()
	hb := &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	}
	view.ObserveHeartbeat(target, hb, now)
	drainController.ObserveHeartbeat(target, hb)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)
}

func TestDrainNodeReturnsZeroOnlyAfterStoppingObserved(t *testing.T) {
	c, view, drainController, target := newDrainTestController(t)

	now := time.Now()
	resp := &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: 1,
	}
	view.ObserveSetNodeLivenessResponse(target, resp, now)
	drainController.ObserveSetNodeLivenessResponse(target, resp)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodeUnknownNeverReturnsZero(t *testing.T) {
	c, view, drainController, target := newDrainTestController(t)

	old := time.Now().Add(-31 * time.Second)
	hb := &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: 1,
	}
	view.ObserveHeartbeat(target, hb, old)
	drainController.ObserveHeartbeat(target, hb)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)
}
