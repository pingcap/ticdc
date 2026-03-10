// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package scheduler

import (
	"testing"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/drain"
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

func TestDrainSchedulerCreatesMoveOperators(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	origin := node.ID("origin")
	destHot := node.ID("dest-hot")
	destCold := node.ID("dest-cold")
	nodeManager.GetAliveNodes()[origin] = &node.Info{ID: origin}
	nodeManager.GetAliveNodes()[destHot] = &node.Info{ID: destHot}
	nodeManager.GetAliveNodes()[destCold] = &node.Info{ID: destCold}

	drainController := drain.NewController(mc)
	drainController.ObserveHeartbeat(origin, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	})

	db := changefeed.NewChangefeedDB(1)
	cfID := common.NewChangeFeedIDWithName("cf1", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		SinkURI:      "blackhole://",
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
	}
	cf := changefeed.NewChangefeed(cfID, info, 1, false)
	db.AddReplicatingMaintainer(cf, origin)

	// Create load on one destination to ensure the scheduler deterministically chooses the least loaded node.
	loadInfo := &config.ChangeFeedInfo{
		SinkURI: "blackhole://load",
		Config:  config.GetDefaultReplicaConfig(),
		State:   config.StateNormal,
	}
	loadInfo1 := *loadInfo
	loadInfo1.ChangefeedID = common.NewChangeFeedIDWithName("cf-load-1", common.DefaultKeyspaceName)
	db.AddReplicatingMaintainer(changefeed.NewChangefeed(loadInfo1.ChangefeedID, &loadInfo1, 1, false), destHot)
	loadInfo2 := *loadInfo
	loadInfo2.ChangefeedID = common.NewChangeFeedIDWithName("cf-load-2", common.DefaultKeyspaceName)
	db.AddReplicatingMaintainer(changefeed.NewChangefeed(loadInfo2.ChangefeedID, &loadInfo2, 1, false), destHot)

	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, 10)

	s := NewDrainScheduler("test", 10, oc, db, drainController)
	_ = s.Execute()

	require.Equal(t, 1, oc.OperatorSize())
	op := oc.GetOperator(cfID)
	require.NotNil(t, op)
	require.ElementsMatch(t, []node.ID{origin, destCold}, op.AffectedNodes())
}

func TestDrainSchedulerSkipsChangefeedWithInflightOperator(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	origin := node.ID("origin")
	dest := node.ID("dest")
	nodeManager.GetAliveNodes()[origin] = &node.Info{ID: origin}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	drainController := drain.NewController(mc)
	drainController.ObserveHeartbeat(origin, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	})

	db := changefeed.NewChangefeedDB(1)
	cfID1 := common.NewChangeFeedIDWithName("cf1", common.DefaultKeyspaceName)
	cfID2 := common.NewChangeFeedIDWithName("cf2", common.DefaultKeyspaceName)

	info := &config.ChangeFeedInfo{
		SinkURI: "blackhole://",
		Config:  config.GetDefaultReplicaConfig(),
		State:   config.StateNormal,
	}

	info1 := *info
	info1.ChangefeedID = cfID1
	cf1 := changefeed.NewChangefeed(cfID1, &info1, 1, false)
	db.AddReplicatingMaintainer(cf1, origin)

	info2 := *info
	info2.ChangefeedID = cfID2
	cf2 := changefeed.NewChangefeed(cfID2, &info2, 1, false)
	db.AddReplicatingMaintainer(cf2, origin)

	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, 10)

	require.True(t, oc.AddOperator(operator.NewMoveMaintainerOperator(db, cf1, origin, dest)))
	require.Equal(t, 1, oc.OperatorSize())

	s := NewDrainScheduler("test", 2, oc, db, drainController)
	_ = s.Execute()

	require.Equal(t, 2, oc.OperatorSize())
	require.NotNil(t, oc.GetOperator(cfID2))
}
