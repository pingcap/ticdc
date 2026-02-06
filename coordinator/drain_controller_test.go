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

package coordinator

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/coordinator/changefeed"
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

func TestDrainControllerDrainNodeSendsDrainingRequest(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	view := NewNodeLivenessView()
	db := changefeed.NewChangefeedDB(1)
	oc := operator.NewOperatorController(node.NewInfo("127.0.0.1:0", ""), db, nil, 10)

	d := NewDrainController(mc, view, db, oc)
	now := time.Unix(0, 0)
	d.now = func() time.Time { return now }

	target := node.ID("node-1")
	remaining := d.DrainNode(target)
	require.Equal(t, 1, remaining)

	out := <-mc.GetMessageChannel()
	require.Equal(t, messaging.TypeSetNodeLivenessRequest, out.Type)
	require.Equal(t, messaging.MaintainerManagerTopic, out.Topic)
	require.Equal(t, target, out.To)

	req := out.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	require.Equal(t, heartbeatpb.NodeLiveness_DRAINING, req.Target)
	require.Equal(t, uint64(0), req.NodeEpoch)
}

func TestDrainControllerPromoteToStoppingWhenQuiescent(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	view := NewNodeLivenessView()
	db := changefeed.NewChangefeedDB(1)
	oc := operator.NewOperatorController(node.NewInfo("127.0.0.1:0", ""), db, nil, 10)

	d := NewDrainController(mc, view, db, oc)
	now := time.Unix(0, 0)
	d.now = func() time.Time { return now }

	target := node.ID("node-1")
	_ = d.DrainNode(target)
	<-mc.GetMessageChannel() // Drain request.

	d.ObserveSetNodeLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 10,
	})
	view.UpdateFromSetLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 10,
	})

	d.Tick()
	out := <-mc.GetMessageChannel()
	require.Equal(t, messaging.TypeSetNodeLivenessRequest, out.Type)

	req := out.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	require.Equal(t, heartbeatpb.NodeLiveness_STOPPING, req.Target)
	require.Equal(t, uint64(10), req.NodeEpoch)
}

func TestDrainControllerRemainingZeroAfterStoppingObserved(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	view := NewNodeLivenessView()
	db := changefeed.NewChangefeedDB(1)
	oc := operator.NewOperatorController(node.NewInfo("127.0.0.1:0", ""), db, nil, 10)

	d := NewDrainController(mc, view, db, oc)
	target := node.ID("node-1")

	d.DrainNode(target)
	<-mc.GetMessageChannel() // Drain request.

	d.ObserveSetNodeLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: 10,
	})
	view.UpdateFromSetLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: 10,
	})

	remaining := d.DrainNode(target)
	require.Equal(t, 0, remaining)
}

func TestDrainControllerUnknownNeverReportsZero(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	now := time.Unix(0, 0)
	view := NewNodeLivenessView()
	view.now = func() time.Time { return now }
	view.ttl = time.Second

	db := changefeed.NewChangefeedDB(1)
	oc := operator.NewOperatorController(node.NewInfo("127.0.0.1:0", ""), db, nil, 10)
	d := NewDrainController(mc, view, db, oc)

	target := node.ID("node-1")
	view.UpdateFromHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: 10,
	})
	d.ObserveSetNodeLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: 10,
	})

	now = now.Add(2 * time.Second)
	require.True(t, view.IsUnknown(target))

	remaining := d.DrainNode(target)
	require.Equal(t, 1, remaining)

	// DrainNode sends a request even if it cannot prove completion.
	<-mc.GetMessageChannel()
}

func TestDrainControllerRemainingIncludesInFlightOperators(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	view := NewNodeLivenessView()
	db := changefeed.NewChangefeedDB(1)
	oc := operator.NewOperatorController(node.NewInfo("127.0.0.1:0", ""), db, nil, 10)
	d := NewDrainController(mc, view, db, oc)

	target := node.ID("node-1")
	cfID := common.NewChangeFeedIDWithName("cf-1", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	db.AddReplicatingMaintainer(cf, target)

	view.UpdateFromSetLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 10,
	})
	d.ObserveSetNodeLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 10,
	})

	// Add an operator involving the target to simulate in-flight scheduling.
	dest := node.ID("node-2")
	_ = oc.AddOperator(operator.NewMoveMaintainerOperator(db, cf, target, dest))

	require.Equal(t, 1, d.DrainNode(target))
	<-mc.GetMessageChannel()
}
