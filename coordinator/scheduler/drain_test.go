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

func TestChooseLeastLoadedDest(t *testing.T) {
	origin := node.ID("n1")
	candidates := []node.ID{"n1", "n2", "n3"}
	sizes := map[node.ID]int{
		"n1": 0,
		"n2": 5,
		"n3": 1,
	}
	dest, ok := chooseLeastLoadedDest(origin, candidates, sizes)
	require.True(t, ok)
	require.Equal(t, node.ID("n3"), dest)

	dest, ok = chooseLeastLoadedDest(origin, []node.ID{"n1"}, sizes)
	require.False(t, ok)
}

func TestDrainSchedulerCreatesMoveOperators(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	origin := node.ID("origin")
	dest := node.ID("dest")
	nodeManager.GetAliveNodes()[origin] = &node.Info{ID: origin}
	nodeManager.GetAliveNodes()[dest] = &node.Info{ID: dest}

	now := time.Now()
	view := nodeliveness.NewView(30 * time.Second)
	view.ObserveHeartbeat(origin, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	}, now)

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

	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, 10)

	s := NewDrainScheduler("test", 10, oc, db, view)
	_ = s.Execute()

	require.Equal(t, 1, oc.OperatorSize())
	op := oc.GetOperator(cfID)
	require.NotNil(t, op)
	require.ElementsMatch(t, []node.ID{origin, dest}, op.AffectedNodes())
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

	now := time.Now()
	view := nodeliveness.NewView(30 * time.Second)
	view.ObserveHeartbeat(origin, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	}, now)

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

	s := NewDrainScheduler("test", 2, oc, db, view)
	_ = s.Execute()

	require.Equal(t, 2, oc.OperatorSize())
	require.NotNil(t, oc.GetOperator(cfID2))
}
