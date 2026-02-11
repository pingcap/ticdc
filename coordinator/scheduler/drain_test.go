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

package scheduler

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

type mockDrainController struct {
	drainingNodes []node.ID
	destNodes     map[node.ID]*node.Info
}

func (m *mockDrainController) GetDrainingNodes(_ time.Time) []node.ID {
	return m.drainingNodes
}

func (m *mockDrainController) GetSchedulableDestNodes(_ time.Time) map[node.ID]*node.Info {
	return m.destNodes
}

func TestDrainSchedulerSkipsInFlightAndSchedulesMove(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	t.Cleanup(ctrl.Finish)
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	selfNode := node.NewInfo("127.0.0.1:8300", "")
	nodeManager.GetAliveNodes()[selfNode.ID] = selfNode
	destNode := node.NewInfo("127.0.0.1:8301", "")
	nodeManager.GetAliveNodes()[destNode.ID] = destNode

	oc := operator.NewOperatorController(selfNode, changefeedDB, backend, 16)

	cfID1 := common.NewChangeFeedIDWithName("cf-1", common.DefaultKeyspaceName)
	cfID2 := common.NewChangeFeedIDWithName("cf-2", common.DefaultKeyspaceName)
	cf1 := changefeed.NewChangefeed(cfID1, &config.ChangeFeedInfo{
		ChangefeedID: cfID1,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 10, false)
	cf2 := changefeed.NewChangefeed(cfID2, &config.ChangeFeedInfo{
		ChangefeedID: cfID2,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 10, false)

	target := node.ID("drain-node")
	changefeedDB.AddReplicatingMaintainer(cf1, target)
	changefeedDB.AddReplicatingMaintainer(cf2, target)

	// One in-flight operator already exists, drain scheduler should skip it.
	require.True(t, oc.AddOperator(oc.NewMoveMaintainerOperator(cf1, target, destNode.ID)))

	s := NewDrainScheduler(16, oc, changefeedDB, &mockDrainController{
		drainingNodes: []node.ID{target},
		destNodes: map[node.ID]*node.Info{
			destNode.ID: destNode,
		},
	})

	s.Execute()

	// We already had one operator for cf1, now scheduler should add one for cf2.
	require.NotNil(t, oc.GetOperator(cf1.ID))
	require.NotNil(t, oc.GetOperator(cf2.ID))
}
