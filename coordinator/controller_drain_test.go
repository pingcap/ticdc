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
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func newDrainTestController(t *testing.T) (*Controller, *drain.Controller, node.ID) {
	t.Helper()

	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	drainController := drain.NewControllerWithTTL(mc, 30*time.Second)
	db := changefeed.NewChangefeedDB(1)
	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, 10)

	target := node.ID("target")
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}

	c := &Controller{
		nodeManager:        nodeManager,
		changefeedDB:       db,
		operatorController: oc,
		drainController:    drainController,
		messageCenter:      mc,
		initialized:        atomic.NewBool(true),
	}
	return c, drainController, target
}

func TestDrainNodeReturnsNonZeroBeforeCoordinatorBootstrap(t *testing.T) {
	c, _, target := newDrainTestController(t)
	c.initialized.Store(false)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	drainTarget, epoch, ok := c.getDispatcherDrainTarget()
	require.False(t, ok)
	require.Equal(t, node.ID(""), drainTarget)
	require.Equal(t, uint64(0), epoch)
}

func TestDrainNodeReturnsNonZeroBeforeStoppingObserved(t *testing.T) {
	c, _, target := newDrainTestController(t)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)
}

func TestDrainNodeRequiresCheckpointAdvanceAfterCompletionObserved(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), 100)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	setChangefeedDrainStatus(cf, target, epoch, 0)
	setTargetStoppingObserved(drainController, target)

	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setChangefeedCheckpointTs(cf, 101)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodeDispatcherCountBlocksCompletion(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), 100)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	setChangefeedDrainStatus(cf, target, epoch, 2)
	setTargetStoppingObserved(drainController, target)

	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 2, remaining)

	setChangefeedDrainStatus(cf, target, epoch, 0)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setChangefeedCheckpointTs(cf, 101)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodePendingStatusConvergenceBlocksCompletion(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), 100)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	setTargetStoppingObserved(drainController, target)

	// Checkpoint progress alone must not complete drain before target epoch convergence.
	setChangefeedCheckpointTs(cf, 101)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	// After convergence, checkpoint gate still requires one forward step from its baseline.
	setChangefeedDrainStatus(cf, target, epoch, 0)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setChangefeedCheckpointTs(cf, 102)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodeCheckpointBaselineKeepsAcrossNonCandidate(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), 100)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	setChangefeedDrainStatus(cf, target, epoch, 0)
	setTargetStoppingObserved(drainController, target)

	// First completion candidate freezes baseline and returns non-zero.
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	// Checkpoint already advanced beyond frozen baseline.
	setChangefeedCheckpointTs(cf, 101)

	// A temporary non-candidate should not wipe the frozen checkpoint baseline.
	setChangefeedDrainStatus(cf, target, epoch, 2)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 2, remaining)

	setChangefeedDrainStatus(cf, target, epoch, 0)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodeRejectConcurrentDifferentDrainTarget(t *testing.T) {
	c, _, target := newDrainTestController(t)
	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, err = c.DrainNode(context.Background(), other)
	require.Error(t, err)
	require.Contains(t, err.Error(), "drain already in progress")
}

func TestRemoveNodeClearsActiveDrainTarget(t *testing.T) {
	c, _, target := newDrainTestController(t)
	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, _, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)

	c.RemoveNode(target)
	_, _, ok = c.getDispatcherDrainTarget()
	require.False(t, ok)
}

func setTargetStoppingObserved(
	drainController *drain.Controller,
	target node.ID,
) {
	resp := &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: 1,
	}
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
	status := cf.GetStatus()
	_, _, _ = cf.ForceUpdateStatus(&heartbeatpb.MaintainerStatus{
		ChangefeedID:  cf.ID.ToPB(),
		CheckpointTs:  checkpointTs,
		DrainProgress: status.GetDrainProgress(),
	})
}

func setChangefeedDrainStatus(
	cf *changefeed.Changefeed,
	target node.ID,
	epoch uint64,
	dispatcherCount uint32,
) {
	status := cf.GetStatus()
	_, _, _ = cf.ForceUpdateStatus(&heartbeatpb.MaintainerStatus{
		ChangefeedID: cf.ID.ToPB(),
		CheckpointTs: status.CheckpointTs,
		DrainProgress: &heartbeatpb.DrainProgress{
			TargetNodeId:          target.String(),
			TargetEpoch:           epoch,
			TargetDispatcherCount: dispatcherCount,
		},
	})
}
