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
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func newDrainTestController(t *testing.T) (*Controller, *nodeliveness.View, *drain.Controller, node.ID) {
	t.Helper()

	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	view := nodeliveness.NewView(30 * time.Second)
	drainController := drain.NewController(mc, view)

	db := changefeed.NewChangefeedDB(1)
	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, 10)

	target := node.ID("target")
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}

	c := &Controller{
		nodeManager:         nodeManager,
		changefeedDB:        db,
		operatorController:  oc,
		nodeLivenessView:    view,
		drainController:     drainController,
		drainCheckpointGate: make(map[node.ID]*drainCheckpointGateState),
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

func TestDrainNodeCheckpointGateRequiresRunningChangefeedProgress(t *testing.T) {
	c, view, drainController, target := newDrainTestController(t)
	gatePhysicalTs := time.Now().UnixMilli()
	setDrainGatePhysicalTs(c, gatePhysicalTs)
	baselineTs := oracle.ComposeTS(gatePhysicalTs-int64((40*time.Minute)/time.Millisecond), 0)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), baselineTs)
	setTargetStoppingObserved(view, drainController, target)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setChangefeedCheckpointTs(cf, baselineTs+1)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodeCheckpointGateUsesFrozenBaseline(t *testing.T) {
	c, view, drainController, target := newDrainTestController(t)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), 100)
	setTargetStoppingObserved(view, drainController, target)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	// Newly created running changefeeds after baseline snapshot must not block this drain gate.
	_ = addRunningChangefeed(c, "cf2", node.ID("other"), 100)

	setChangefeedCheckpointTs(cf, 101)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodeCheckpointGateIgnoresNonRunningChangefeed(t *testing.T) {
	c, view, drainController, target := newDrainTestController(t)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), 100)
	setTargetStoppingObserved(view, drainController, target)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	info, cloneErr := cf.GetInfo().Clone()
	require.NoError(t, cloneErr)
	info.State = config.StateStopped
	cf.SetInfo(info)

	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodeCheckpointGateLowLagMustCatchGateCreateTime(t *testing.T) {
	c, view, drainController, target := newDrainTestController(t)
	gatePhysicalTs := time.Now().UnixMilli()
	setDrainGatePhysicalTs(c, gatePhysicalTs)
	baselinePhysicalTs := gatePhysicalTs - int64((30*time.Second)/time.Millisecond)
	baselineTs := oracle.ComposeTS(baselinePhysicalTs, 0)
	cf := addRunningChangefeed(c, "cf-low-lag", node.ID("other"), baselineTs)
	setTargetStoppingObserved(view, drainController, target)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	// Low-lag bucket must satisfy both:
	// 1) catch up to gate create time; and
	// 2) advance one minute from baseline.
	// Reaching gate create time alone is still insufficient here.
	setChangefeedCheckpointTs(cf, oracle.ComposeTS(gatePhysicalTs, 0))
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setChangefeedCheckpointTs(cf, oracle.ComposeTS(baselinePhysicalTs+int64((time.Minute)/time.Millisecond), 0))
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodeCheckpointGateMidLagRequiresOneMinuteAdvance(t *testing.T) {
	c, view, drainController, target := newDrainTestController(t)
	gatePhysicalTs := time.Now().UnixMilli()
	setDrainGatePhysicalTs(c, gatePhysicalTs)
	baselinePhysicalTs := gatePhysicalTs - int64((10*time.Minute)/time.Millisecond)
	baselineTs := oracle.ComposeTS(baselinePhysicalTs, 0)
	cf := addRunningChangefeed(c, "cf-mid-lag", node.ID("other"), baselineTs)
	setTargetStoppingObserved(view, drainController, target)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setChangefeedCheckpointTs(cf, oracle.ComposeTS(baselinePhysicalTs+int64((30*time.Second)/time.Millisecond), 0))
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setChangefeedCheckpointTs(cf, oracle.ComposeTS(baselinePhysicalTs+int64((time.Minute)/time.Millisecond), 0))
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func setTargetStoppingObserved(
	view *nodeliveness.View,
	drainController *drain.Controller,
	target node.ID,
) {
	now := time.Now()
	resp := &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: 1,
	}
	view.ObserveSetNodeLivenessResponse(target, resp, now)
	drainController.ObserveSetNodeLivenessResponse(target, resp)
}

func addRunningChangefeed(c *Controller, name string, nodeID node.ID, checkpointTs uint64) *changefeed.Changefeed {
	cfID := common.NewChangeFeedIDWithName(name, common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		SinkURI:      "blackhole://",
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
	}
	cf := changefeed.NewChangefeed(cfID, info, checkpointTs, false)
	c.changefeedDB.AddReplicatingMaintainer(cf, nodeID)
	return cf
}

func setChangefeedCheckpointTs(cf *changefeed.Changefeed, checkpointTs uint64) {
	_, _, _ = cf.ForceUpdateStatus(&heartbeatpb.MaintainerStatus{
		ChangefeedID: cf.ID.ToPB(),
		CheckpointTs: checkpointTs,
	})
}

func setDrainGatePhysicalTs(c *Controller, physicalTs int64) {
	clock := &pdutil.Clock4Test{}
	clock.SetTS(oracle.ComposeTS(physicalTs, 0))
	c.pdClock = clock
}
