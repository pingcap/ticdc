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

	"github.com/pingcap/ticdc/coordinator/changefeed"
	cooperator "github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

type mockDestSelector struct {
	nodes map[node.ID]*node.Info
	ids   []node.ID
}

func (m *mockDestSelector) GetSchedulableDestNodes() map[node.ID]*node.Info { return m.nodes }
func (m *mockDestSelector) GetSchedulableDestNodeIDs() []node.ID            { return m.ids }

type mockDrainingChecker struct {
	draining node.ID
}

func (m *mockDrainingChecker) IsDraining(id node.ID) bool { return id == m.draining }

func newTestChangefeed(t *testing.T, name string) *changefeed.Changefeed {
	t.Helper()

	cfID := common.NewChangeFeedIDWithName(name, common.DefaultKeyspaceName)
	return changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
}

func TestDrainSchedulerCreatesMoveOperators(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	draining := node.NewInfo("127.0.0.1:8301", "")
	dest := node.NewInfo("127.0.0.1:8302", "")
	nodeManager.GetAliveNodes()[draining.ID] = draining
	nodeManager.GetAliveNodes()[dest.ID] = dest

	db := changefeed.NewChangefeedDB(1)
	cf1 := newTestChangefeed(t, "cf-1")
	cf2 := newTestChangefeed(t, "cf-2")
	db.AddReplicatingMaintainer(cf1, draining.ID)
	db.AddReplicatingMaintainer(cf2, draining.ID)

	oc := cooperator.NewOperatorController(node.NewInfo("127.0.0.1:0", ""), db, nil, 10)

	selector := &mockDestSelector{
		nodes: map[node.ID]*node.Info{dest.ID: dest},
		ids:   []node.ID{dest.ID},
	}
	liveness := &mockDrainingChecker{draining: draining.ID}

	s := NewDrainScheduler("test", 10, oc, db, nodeManager, selector, liveness)
	s.Execute()

	op1 := oc.GetOperator(cf1.ID)
	require.NotNil(t, op1)
	require.Equal(t, "move", op1.Type())

	op2 := oc.GetOperator(cf2.ID)
	require.NotNil(t, op2)
	require.Equal(t, "move", op2.Type())

	require.Contains(t, op1.AffectedNodes(), draining.ID)
	require.Contains(t, op1.AffectedNodes(), dest.ID)
	require.Contains(t, op2.AffectedNodes(), draining.ID)
	require.Contains(t, op2.AffectedNodes(), dest.ID)
}

func TestDrainSchedulerSkipsChangefeedWithInFlightOperator(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	draining := node.NewInfo("127.0.0.1:8301", "")
	dest := node.NewInfo("127.0.0.1:8302", "")
	nodeManager.GetAliveNodes()[draining.ID] = draining
	nodeManager.GetAliveNodes()[dest.ID] = dest

	db := changefeed.NewChangefeedDB(1)
	cf1 := newTestChangefeed(t, "cf-1")
	cf2 := newTestChangefeed(t, "cf-2")
	db.AddReplicatingMaintainer(cf1, draining.ID)
	db.AddReplicatingMaintainer(cf2, draining.ID)

	oc := cooperator.NewOperatorController(node.NewInfo("127.0.0.1:0", ""), db, nil, 10)
	require.True(t, oc.AddOperator(cooperator.NewMoveMaintainerOperator(db, cf1, draining.ID, dest.ID)))

	selector := &mockDestSelector{
		nodes: map[node.ID]*node.Info{dest.ID: dest},
		ids:   []node.ID{dest.ID},
	}
	liveness := &mockDrainingChecker{draining: draining.ID}

	s := NewDrainScheduler("test", 10, oc, db, nodeManager, selector, liveness)
	s.Execute()

	require.NotNil(t, oc.GetOperator(cf1.ID))
	require.NotNil(t, oc.GetOperator(cf2.ID))
	require.Equal(t, 2, oc.OperatorSize())
}
