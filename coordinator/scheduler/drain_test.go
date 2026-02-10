package scheduler

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/nodeliveness"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestDrainSchedulerSkipsChangefeedsWithInFlightOperators(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	self := node.NewInfo("127.0.0.1:1", "")
	origin := node.ID("origin")
	dest := node.ID("dest")

	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[origin] = node.NewInfo("127.0.0.1:2", "")
	nodeManager.GetAliveNodes()[dest] = node.NewInfo("127.0.0.1:3", "")

	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	oc := operator.NewOperatorController(self, changefeedDB, nil, 10)

	cf1ID := common.NewChangeFeedIDWithName("cf1", common.DefaultKeyspaceName)
	cf1 := changefeed.NewChangefeed(cf1ID, &config.ChangeFeedInfo{
		ChangefeedID: cf1ID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddReplicatingMaintainer(cf1, origin)

	cf2ID := common.NewChangeFeedIDWithName("cf2", common.DefaultKeyspaceName)
	cf2 := changefeed.NewChangefeed(cf2ID, &config.ChangeFeedInfo{
		ChangefeedID: cf2ID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddReplicatingMaintainer(cf2, origin)

	require.True(t, oc.AddOperator(operator.NewMoveMaintainerOperator(changefeedDB, cf1, origin, dest)))

	view := nodeliveness.NewView(30 * time.Second)
	view.HandleNodeHeartbeat(origin, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	}, time.Now())

	s := NewDrainScheduler("test", 10, oc, changefeedDB, view)
	s.Execute()

	require.Equal(t, 2, oc.OperatorSize())
	op := oc.GetOperator(cf2ID)
	require.NotNil(t, op)
	require.Equal(t, "move", op.Type())
	require.ElementsMatch(t, []node.ID{origin, dest}, op.AffectedNodes())
}
