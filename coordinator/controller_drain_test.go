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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/drain"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/bootstrap"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
)

type drainTestPDClient struct {
	pd.Client

	mu       sync.Mutex
	physical int64
	logical  int64
	err      error
	called   chan struct{}
	unblock  chan struct{}
}

func newDrainTestPDClient() *drainTestPDClient {
	return &drainTestPDClient{physical: 100}
}

func (c *drainTestPDClient) GetTS(context.Context) (int64, int64, error) {
	if c.called != nil {
		close(c.called)
	}
	if c.unblock != nil {
		<-c.unblock
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return 0, 0, c.err
	}
	c.logical++
	return c.physical, c.logical, nil
}

func newDrainTestController(t *testing.T) (*Controller, *drain.Controller, node.ID) {
	t.Helper()

	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	drainController := drain.NewController(mc)
	db := changefeed.NewChangefeedDB(1)
	selfNode := &node.Info{ID: node.ID("coordinator")}
	oc := operator.NewOperatorController(selfNode, db, nil, nil, 10)

	target := node.ID("target")
	nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}

	c := &Controller{
		nodeManager:        nodeManager,
		changefeedDB:       db,
		operatorController: oc,
		drainController:    drainController,
		messageCenter:      mc,
		pdClient:           newDrainTestPDClient(),
		initialized:        atomic.NewBool(true),
		bootstrapper: bootstrap.NewBootstrapper[heartbeatpb.CoordinatorBootstrapResponse](
			"test-drain-bootstrapper",
			func(id node.ID, _ string) *messaging.TargetMessage {
				return messaging.NewSingleTargetMessage(
					id,
					messaging.MaintainerManagerTopic,
					&heartbeatpb.CoordinatorBootstrapRequest{},
				)
			},
		),
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
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)
}

func TestDrainNodeCompletesAfterCompletionObserved(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), 100)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	setChangefeedDrainStatus(cf, target, epoch, 0, 0)
	setTargetStoppingObserved(drainController, target)

	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
	require.NotNil(t, c.drainSession)
	require.Nil(t, c.drainClearState)
	require.Equal(t, target, c.drainSession.target)
	require.Equal(t, epoch, c.drainSession.epoch)
}

func TestDrainNodeDispatcherCountBlocksCompletion(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), 100)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	setChangefeedDrainStatus(cf, target, epoch, 2, 0)
	setTargetStoppingObserved(drainController, target)

	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 2, remaining)

	setChangefeedDrainStatus(cf, target, epoch, 0, 0)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodePendingStatusConvergenceBlocksCompletion(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), 100)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	setTargetStoppingObserved(drainController, target)

	// Status convergence must finish before drain can complete.
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setChangefeedDrainStatus(cf, target, epoch, 0, 0)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodeInflightDrainMovesBlockCompletion(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), 100)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	setChangefeedDrainStatus(cf, target, epoch, 0, 1)
	setTargetStoppingObserved(drainController, target)

	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setChangefeedDrainStatus(cf, target, epoch, 0, 0)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodeWaitsForConcurrentDifferentDrainTarget(t *testing.T) {
	c, _, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}
	setDrainProtocolVersion(c, other, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	remaining, err = c.DrainNode(context.Background(), other)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	activeTarget, _, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	require.Equal(t, target, activeTarget)
}

func TestDrainNodeUsesUnixNanoCompatibleDrainEpoch(t *testing.T) {
	c, _, target := newDrainTestController(t)
	pdClient := &drainTestPDClient{physical: 123, logical: 44}
	c.pdClient = pdClient
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	before := uint64(time.Now().UnixNano())
	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	pdEpoch, err := unixNanoCompatibleDrainEpoch(123, 45)
	require.NoError(t, err)
	require.Greater(t, epoch, pdEpoch)
	require.Greater(t, epoch, before)

	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)
	_, reusedEpoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	require.Equal(t, epoch, reusedEpoch)
}

func TestDrainNodeEpochExceedsObservedOldUnixNanoEpoch(t *testing.T) {
	c, _, target := newDrainTestController(t)
	c.pdClient = &drainTestPDClient{physical: 123, logical: 44}
	bootstrapTrackedNodes(c, target)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	oldEpoch := uint64(math.MaxUint64 - 2)
	changed := c.observeDispatcherDrainTargetHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		DispatcherDrainTargetNodeId: "old-target",
		DispatcherDrainTargetEpoch:  oldEpoch,
	})
	require.True(t, changed)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, _, ok := c.getDispatcherDrainTarget()
	require.False(t, ok)

	c.observeDispatcherDrainTargetHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		DispatcherDrainTargetEpoch: oldEpoch,
	})
	require.Nil(t, c.drainClearState)

	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	require.Equal(t, oldEpoch+1, epoch)
}

func TestDrainNodeEpochExceedsObservedEmptyClearEpoch(t *testing.T) {
	c, _, target := newDrainTestController(t)
	c.pdClient = &drainTestPDClient{physical: 123, logical: 44}
	bootstrapTrackedNodes(c, target)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	clearEpoch := uint64(math.MaxUint64 - 2)
	changed := c.observeDispatcherDrainTargetHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		DispatcherDrainTargetEpoch: clearEpoch,
	})
	require.False(t, changed)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	require.Equal(t, clearEpoch+1, epoch)
}

func TestDrainNodeEpochExceedsBootstrapEmptyClearEpoch(t *testing.T) {
	c, _, target := newDrainTestController(t)
	c.pdClient = &drainTestPDClient{physical: 123, logical: 44}
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	clearEpoch := uint64(math.MaxUint64 - 2)
	changed := c.recoverStaleDispatcherDrainTargetFromBootstrap(map[node.ID]*heartbeatpb.CoordinatorBootstrapResponse{
		target: {
			DrainProtocolVersion:        heartbeatpb.CurrentDrainProtocolVersion,
			DispatcherDrainTargetEpoch:  clearEpoch,
			DispatcherDrainTargetNodeId: "",
		},
	})
	require.False(t, changed)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	require.Equal(t, clearEpoch+1, epoch)
}

func TestDrainNodeReturnsErrorWhenDrainEpochOverflows(t *testing.T) {
	c, _, target := newDrainTestController(t)
	c.maxObservedDrainEpoch = math.MaxUint64
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.Error(t, err)
	require.Contains(t, err.Error(), "drain epoch overflow")
	require.Zero(t, remaining)

	_, _, ok := c.getDispatcherDrainTarget()
	require.False(t, ok)
}

func TestUnixNanoCompatibleDrainEpochRejectsInvalidTSO(t *testing.T) {
	_, err := unixNanoCompatibleDrainEpoch(-1, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid drain epoch")

	_, err = unixNanoCompatibleDrainEpoch(0, -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid drain epoch")

	_, err = unixNanoCompatibleDrainEpoch(int64(math.MaxInt64), int64(math.MaxInt64))
	require.Error(t, err)
	require.Contains(t, err.Error(), "drain epoch overflow")
}

func TestDrainNodeReturnsErrorWhenPDAllocEpochFails(t *testing.T) {
	c, _, target := newDrainTestController(t)
	c.pdClient.(*drainTestPDClient).err = context.Canceled
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")
	require.Zero(t, remaining)

	_, _, ok := c.getDispatcherDrainTarget()
	require.False(t, ok)
}

func TestDrainNodeDoesNotCreateSessionWhenTargetLeavesDuringEpochAlloc(t *testing.T) {
	c, _, target := newDrainTestController(t)
	c.pdClient = &drainTestPDClient{
		physical: 123,
		called:   make(chan struct{}),
		unblock:  make(chan struct{}),
	}
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	type drainResult struct {
		remaining int
		err       error
	}
	resultCh := make(chan drainResult, 1)
	go func() {
		remaining, err := c.DrainNode(context.Background(), target)
		resultCh <- drainResult{remaining: remaining, err: err}
	}()

	<-c.pdClient.(*drainTestPDClient).called
	delete(c.nodeManager.GetAliveNodes(), target)
	close(c.pdClient.(*drainTestPDClient).unblock)

	result := <-resultCh
	require.Error(t, result.err)
	require.Zero(t, result.remaining)
	require.True(t, errors.ErrCaptureNotExist.Equal(result.err))

	_, _, ok := c.getDispatcherDrainTarget()
	require.False(t, ok)
}

func TestDrainNodeCompletionKeepsBlockingDifferentTargetUntilRemoval(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	cf := addRunningChangefeed(c, "cf1", node.ID("other"), 100)

	other := node.ID("other-target")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}
	setDrainProtocolVersion(c, other, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	setChangefeedDrainStatus(cf, target, epoch, 0, 0)
	setTargetStoppingObserved(drainController, target)

	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)

	remaining, err = c.DrainNode(context.Background(), other)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	activeTarget, _, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	require.Equal(t, target, activeTarget)
}

func TestDrainNodeReturnsZeroAfterCompletedTargetIsRemoved(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setTargetStoppingObserved(drainController, target)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)

	delete(c.nodeManager.GetAliveNodes(), target)
	c.RemoveNode(target)

	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
}

func TestDrainNodeReturnsZeroWhenCompletedTargetLeavesBeforeRemoveNode(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setTargetStoppingObserved(drainController, target)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)

	messageCh := outboundMessages(c)
	drainMessageChannel(messageCh)

	delete(c.nodeManager.GetAliveNodes(), target)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)
	require.NotNil(t, c.drainSession)
	requireNoMessage(t, messageCh)
}

func TestDrainNodeKeepsNonZeroWhenIncompleteTargetLeavesBeforeRemoveNode(t *testing.T) {
	c, _, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	messageCh := outboundMessages(c)
	drainMessageChannel(messageCh)

	delete(c.nodeManager.GetAliveNodes(), target)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)
	require.NotNil(t, c.drainSession)
	requireNoMessage(t, messageCh)
}

func TestDrainNodeReturnsCaptureNotExistWhenIncompleteTargetIsRemoved(t *testing.T) {
	c, _, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	delete(c.nodeManager.GetAliveNodes(), target)
	c.RemoveNode(target)

	remaining, err = c.DrainNode(context.Background(), target)
	require.Error(t, err)
	require.Zero(t, remaining)
	require.True(t, errors.ErrCaptureNotExist.Equal(err))
}

func TestDrainCompletedTombstoneIsClearedWhenTargetRejoins(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	bootstrapTrackedNodes(c, target)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	setTargetStoppingObserved(drainController, target)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)

	delete(c.nodeManager.GetAliveNodes(), target)
	c.onNodeChanged(context.Background())
	require.True(t, c.isCompletedDrainTarget(target))

	c.nodeManager.GetAliveNodes()[target] = &node.Info{ID: target}
	c.onNodeChanged(context.Background())
	require.False(t, c.isCompletedDrainTarget(target))

	delete(c.nodeManager.GetAliveNodes(), target)
	c.onNodeChanged(context.Background())
	remaining, err = c.DrainNode(context.Background(), target)
	require.Error(t, err)
	require.Zero(t, remaining)
	require.True(t, errors.ErrCaptureNotExist.Equal(err))
}

func TestRemoveNodeClearsActiveDrainTarget(t *testing.T) {
	c, _, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, _, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)

	c.RemoveNode(target)
	_, _, ok = c.getDispatcherDrainTarget()
	require.False(t, ok)
}

func TestDrainNodeLegacyTargetFallsBackToHardRestart(t *testing.T) {
	c, _, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.LegacyDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)

	drainTarget, epoch, ok := c.getDispatcherDrainTarget()
	require.False(t, ok)
	require.Equal(t, node.ID(""), drainTarget)
	require.Equal(t, uint64(0), epoch)
}

func TestDrainNodeWaitsForTargetCapabilityObservation(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	setTargetStoppingObserved(drainController, target)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	drainTarget, epoch, ok := c.getDispatcherDrainTarget()
	require.False(t, ok)
	require.Equal(t, node.ID(""), drainTarget)
	require.Equal(t, uint64(0), epoch)
}

func TestDrainNodeWaitsForPeerCapabilityObservation(t *testing.T) {
	c, _, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	drainTarget, epoch, ok := c.getDispatcherDrainTarget()
	require.False(t, ok)
	require.Equal(t, node.ID(""), drainTarget)
	require.Equal(t, uint64(0), epoch)
}

func TestDrainNodeFallsBackWhenAlivePeerIsLegacy(t *testing.T) {
	c, _, target := newDrainTestController(t)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}
	setDrainProtocolVersion(c, other, heartbeatpb.LegacyDrainProtocolVersion)
	addRunningChangefeed(c, "cf1", other, 100)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)

	drainTarget, epoch, ok := c.getDispatcherDrainTarget()
	require.False(t, ok)
	require.Equal(t, node.ID(""), drainTarget)
	require.Equal(t, uint64(0), epoch)
	require.Nil(t, c.drainSession)
	require.Nil(t, c.drainClearState)
}

func TestDrainNodeIgnoresLateUnknownPeerAfterSessionStart(t *testing.T) {
	c, _, target := newDrainTestController(t)
	bootstrapTrackedNodes(c, target)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}

	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	drainTarget, _, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	require.Equal(t, target, drainTarget)
	require.NotNil(t, c.drainSession)
}

func TestDrainNodeContinuesProcessingBootstrappedPeerHeartbeatDuringLateJoin(t *testing.T) {
	c, _, target := newDrainTestController(t)
	owner := node.ID("owner")
	c.nodeManager.GetAliveNodes()[owner] = &node.Info{ID: owner}
	bootstrapTrackedNodes(c, target, owner)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	setDrainProtocolVersion(c, owner, heartbeatpb.CurrentDrainProtocolVersion)

	cf := addRunningChangefeed(c, "cf1", owner, 100)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	_, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	require.Equal(t, 1, c.collectDrainPendingStatus(target, epoch))

	late := node.ID("late")
	c.nodeManager.GetAliveNodes()[late] = &node.Info{ID: late}
	c.onNodeChanged(context.Background())
	require.False(t, c.bootstrapper.AllNodesReady())

	hb := &heartbeatpb.MaintainerHeartbeat{
		Statuses: []*heartbeatpb.MaintainerStatus{{
			ChangefeedID: cf.ID.ToPB(),
			CheckpointTs: 100,
			State:        heartbeatpb.ComponentState_Working,
			DrainProgress: &heartbeatpb.DrainProgress{
				TargetNodeId:          target.String(),
				TargetEpoch:           epoch,
				TargetDispatcherCount: 0,
			},
		}},
	}
	c.onMessage(context.Background(), &messaging.TargetMessage{
		From:    owner,
		Topic:   messaging.CoordinatorTopic,
		Type:    messaging.TypeMaintainerHeartbeatRequest,
		Message: []messaging.IOTypeT{hb},
	})

	require.Zero(t, c.collectDrainPendingStatus(target, epoch))
	require.NotNil(t, cf.GetStatus().GetDrainProgress())
}

func TestLateCompatiblePeerJoinsTargetSyncAndClearAck(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	bootstrapTrackedNodes(c, target)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}

	mc := c.messageCenter.(interface {
		GetMessageChannel() chan *messaging.TargetMessage
	})
	drainMessageChannel(mc.GetMessageChannel())
	c.onNodeChanged(context.Background())

	require.NotNil(t, c.drainSession)
	_, ok := c.drainSession.targetSyncNodes[other]
	require.False(t, ok)

	for len(mc.GetMessageChannel()) > 0 {
		msg := <-mc.GetMessageChannel()
		require.False(t, msg.Type == messaging.TypeSetDispatcherDrainTargetRequest && msg.To == other)
	}

	c.onMaintainerBootstrapResponse(context.Background(), &messaging.TargetMessage{
		From:    other,
		Topic:   messaging.CoordinatorTopic,
		Type:    messaging.TypeCoordinatorBootstrapResponse,
		Message: []messaging.IOTypeT{&heartbeatpb.CoordinatorBootstrapResponse{DrainProtocolVersion: heartbeatpb.CurrentDrainProtocolVersion}},
	})

	require.NotNil(t, c.drainSession)
	_, ok = c.drainSession.targetSyncNodes[other]
	require.True(t, ok)

	foundActiveBroadcastToOther := false
	for len(mc.GetMessageChannel()) > 0 {
		msg := <-mc.GetMessageChannel()
		if msg.Type != messaging.TypeSetDispatcherDrainTargetRequest || msg.To != other {
			continue
		}
		req := msg.Message[0].(*heartbeatpb.SetDispatcherDrainTargetRequest)
		if req.TargetNodeId == target.String() && req.TargetEpoch > 0 {
			foundActiveBroadcastToOther = true
		}
	}
	require.True(t, foundActiveBroadcastToOther)

	setTargetStoppingObserved(drainController, target)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)

	require.NotNil(t, c.drainSession)
	require.Nil(t, c.drainClearState)

	delete(c.nodeManager.GetAliveNodes(), target)
	c.RemoveNode(target)
	require.Nil(t, c.drainSession)
	require.NotNil(t, c.drainClearState)
	clearEpoch := c.drainClearState.epoch
	_, ok = c.drainClearState.pendingNodes[other]
	require.True(t, ok)
	_, ok = c.drainClearState.pendingNodes[target]
	require.False(t, ok)

	c.observeDispatcherDrainTargetHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		DispatcherDrainTargetEpoch: clearEpoch,
	})
	require.NotNil(t, c.drainClearState)

	c.observeDispatcherDrainTargetHeartbeat(other, &heartbeatpb.NodeHeartbeat{
		DispatcherDrainTargetEpoch: clearEpoch,
	})
	require.Nil(t, c.drainClearState)
}

func TestLateLegacyPeerDoesNotEnterTargetSyncOrClearAck(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	bootstrapTrackedNodes(c, target)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}

	mc := c.messageCenter.(interface {
		GetMessageChannel() chan *messaging.TargetMessage
	})
	drainMessageChannel(mc.GetMessageChannel())
	c.onNodeChanged(context.Background())

	require.NotNil(t, c.drainSession)
	_, ok := c.drainSession.targetSyncNodes[other]
	require.False(t, ok)

	for len(mc.GetMessageChannel()) > 0 {
		msg := <-mc.GetMessageChannel()
		require.False(t, msg.Type == messaging.TypeSetDispatcherDrainTargetRequest && msg.To == other)
	}

	c.onMaintainerBootstrapResponse(context.Background(), &messaging.TargetMessage{
		From:    other,
		Topic:   messaging.CoordinatorTopic,
		Type:    messaging.TypeCoordinatorBootstrapResponse,
		Message: []messaging.IOTypeT{&heartbeatpb.CoordinatorBootstrapResponse{DrainProtocolVersion: heartbeatpb.LegacyDrainProtocolVersion}},
	})

	require.NotNil(t, c.drainSession)
	_, ok = c.drainSession.targetSyncNodes[other]
	require.False(t, ok)

	for len(mc.GetMessageChannel()) > 0 {
		msg := <-mc.GetMessageChannel()
		require.False(t, msg.Type == messaging.TypeSetDispatcherDrainTargetRequest && msg.To == other)
	}

	setTargetStoppingObserved(drainController, target)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)

	require.NotNil(t, c.drainSession)
	require.Nil(t, c.drainClearState)

	delete(c.nodeManager.GetAliveNodes(), target)
	c.RemoveNode(target)
	require.Nil(t, c.drainSession)
	require.Nil(t, c.drainClearState)
}

func TestClearDispatcherDrainTargetSkipsRemovedPeerBeforeClear(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	bootstrapTrackedNodes(c, target)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}
	bootstrapTrackedNodes(c, other)
	setDrainProtocolVersion(c, other, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	delete(c.nodeManager.GetAliveNodes(), other)
	c.RemoveNode(other)

	setTargetStoppingObserved(drainController, target)
	remaining, err = c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 0, remaining)

	require.NotNil(t, c.drainSession)
	require.Nil(t, c.drainClearState)

	delete(c.nodeManager.GetAliveNodes(), target)
	c.RemoveNode(target)
	require.Nil(t, c.drainSession)
	require.Nil(t, c.drainClearState)
}

func TestClearDispatcherDrainTargetTracksNodeHeartbeatAck(t *testing.T) {
	c, _, target := newDrainTestController(t)
	bootstrapTrackedNodes(c, target)
	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}
	bootstrapTrackedNodes(c, other)

	epoch, err := c.ensureDispatcherDrainTarget(context.Background(), target)
	require.NoError(t, err)

	mc := c.messageCenter.(interface {
		GetMessageChannel() chan *messaging.TargetMessage
	})
	drainMessageChannel(mc.GetMessageChannel())

	c.clearDispatcherDrainTarget(target, epoch)
	require.Nil(t, c.drainSession)
	require.NotNil(t, c.drainClearState)
	require.Len(t, c.drainClearState.pendingNodes, 2)

	c.observeDispatcherDrainTargetHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		DispatcherDrainTargetEpoch: epoch,
	})
	require.NotNil(t, c.drainClearState)
	require.Len(t, c.drainClearState.pendingNodes, 1)

	c.observeDispatcherDrainTargetHeartbeat(other, &heartbeatpb.NodeHeartbeat{
		DispatcherDrainTargetEpoch: epoch,
	})
	require.Nil(t, c.drainClearState)
}

func TestClearDispatcherDrainTargetBlocksPendingDestinationsUntilAck(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	bootstrapTrackedNodes(c, target)
	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}
	bootstrapTrackedNodes(c, other)

	drainController.ObserveHeartbeat(other, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	})

	epoch, err := c.ensureDispatcherDrainTarget(context.Background(), target)
	require.NoError(t, err)

	c.clearDispatcherDrainTarget(target, epoch)
	require.NotNil(t, c.drainClearState)
	require.False(t, drainController.IsSchedulableDest(other))

	ack := &heartbeatpb.NodeHeartbeat{
		Liveness:                    heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch:                   1,
		DispatcherDrainTargetEpoch:  epoch,
		DispatcherDrainTargetNodeId: "",
	}
	drainController.ObserveHeartbeat(other, ack)
	c.observeDispatcherDrainTargetHeartbeat(other, ack)

	require.True(t, drainController.IsSchedulableDest(other))
	require.NotNil(t, c.drainClearState)
	require.Len(t, c.drainClearState.pendingNodes, 1)
}

func TestStaleDispatcherDrainTargetRecoveredFromBootstrap(t *testing.T) {
	c, drainController, target := newDrainTestController(t)
	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}
	bootstrapTrackedNodes(c, target, other)

	responses := map[node.ID]*heartbeatpb.CoordinatorBootstrapResponse{
		target: {
			DrainProtocolVersion:        heartbeatpb.CurrentDrainProtocolVersion,
			DispatcherDrainTargetNodeId: target.String(),
			DispatcherDrainTargetEpoch:  10,
		},
		other: {
			DrainProtocolVersion: heartbeatpb.CurrentDrainProtocolVersion,
		},
	}
	for id, resp := range responses {
		c.drainController.ObserveBootstrapResponse(id, resp)
	}
	drainController.ObserveHeartbeat(other, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	})

	require.True(t, c.recoverStaleDispatcherDrainTargetFromBootstrap(responses))
	require.Nil(t, c.drainSession)
	require.NotNil(t, c.drainClearState)
	require.Equal(t, target, c.drainClearState.target)
	require.Equal(t, uint64(10), c.drainClearState.epoch)
	require.Equal(t, uint64(10), c.maxObservedDrainEpoch)
	require.Contains(t, c.drainClearState.pendingNodes, target)
	require.Contains(t, c.drainClearState.pendingNodes, other)
	require.False(t, drainController.IsSchedulableDest(other))
}

func TestStaleDispatcherDrainTargetRecoveredFromHeartbeat(t *testing.T) {
	c, _, target := newDrainTestController(t)
	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}
	bootstrapTrackedNodes(c, target, other)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	setDrainProtocolVersion(c, other, heartbeatpb.CurrentDrainProtocolVersion)

	changed := c.observeDispatcherDrainTargetHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		DispatcherDrainTargetNodeId: target.String(),
		DispatcherDrainTargetEpoch:  10,
	})

	require.True(t, changed)
	require.Nil(t, c.drainSession)
	require.NotNil(t, c.drainClearState)
	require.Equal(t, target, c.drainClearState.target)
	require.Equal(t, uint64(10), c.drainClearState.epoch)
	require.Contains(t, c.drainClearState.pendingNodes, target)
	require.Contains(t, c.drainClearState.pendingNodes, other)
}

func TestStaleDispatcherDrainTargetIgnoredDuringActiveSession(t *testing.T) {
	c, _, target := newDrainTestController(t)
	bootstrapTrackedNodes(c, target)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	epoch, err := c.ensureDispatcherDrainTarget(context.Background(), target)
	require.NoError(t, err)

	changed := c.observeDispatcherDrainTargetHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		DispatcherDrainTargetNodeId: "stale-target",
		DispatcherDrainTargetEpoch:  epoch - 1,
	})

	require.False(t, changed)
	require.NotNil(t, c.drainSession)
	require.Nil(t, c.drainClearState)
	require.Equal(t, target, c.drainSession.target)
	require.Equal(t, epoch, c.drainSession.epoch)
}

func TestDrainNodeWaitsForPendingClearBeforeNewTarget(t *testing.T) {
	c, _, target := newDrainTestController(t)
	other := node.ID("other")
	nextTarget := node.ID("next-target")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}
	c.nodeManager.GetAliveNodes()[nextTarget] = &node.Info{ID: nextTarget}
	bootstrapTrackedNodes(c, target, other, nextTarget)

	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	setDrainProtocolVersion(c, other, heartbeatpb.CurrentDrainProtocolVersion)
	setDrainProtocolVersion(c, nextTarget, heartbeatpb.CurrentDrainProtocolVersion)

	remaining, err := c.DrainNode(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	activeTarget, epoch, ok := c.getDispatcherDrainTarget()
	require.True(t, ok)
	require.Equal(t, target, activeTarget)

	c.clearDispatcherDrainTarget(target, epoch)
	require.NotNil(t, c.drainClearState)

	remaining, err = c.DrainNode(context.Background(), nextTarget)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	activeTarget, _, ok = c.getDispatcherDrainTarget()
	require.False(t, ok)
	require.Equal(t, node.ID(""), activeTarget)
	require.NotNil(t, c.drainClearState)
	require.Equal(t, target, c.drainClearState.target)

	pendingNodes := make([]node.ID, 0, len(c.drainClearState.pendingNodes))
	for id := range c.drainClearState.pendingNodes {
		pendingNodes = append(pendingNodes, id)
	}
	for _, id := range pendingNodes {
		c.observeDispatcherDrainTargetHeartbeat(id, &heartbeatpb.NodeHeartbeat{
			DispatcherDrainTargetEpoch: epoch,
		})
	}
	require.Nil(t, c.drainClearState)

	remaining, err = c.DrainNode(context.Background(), nextTarget)
	require.NoError(t, err)
	require.Equal(t, 1, remaining)

	activeTarget, _, ok = c.getDispatcherDrainTarget()
	require.True(t, ok)
	require.Equal(t, nextTarget, activeTarget)
}

func TestClearDispatcherDrainTargetResendsUntilAck(t *testing.T) {
	c, _, target := newDrainTestController(t)
	epoch, err := c.ensureDispatcherDrainTarget(context.Background(), target)
	require.NoError(t, err)

	mc := c.messageCenter.(interface {
		GetMessageChannel() chan *messaging.TargetMessage
	})
	drainMessageChannel(mc.GetMessageChannel())

	c.clearDispatcherDrainTarget(target, epoch)
	drainMessageChannel(mc.GetMessageChannel())

	require.NotNil(t, c.drainClearState)
	c.drainClearState.lastSent = time.Now().Add(-dispatcherDrainTargetResendIntvl - time.Second)
	c.maybeBroadcastDispatcherDrainTarget(false)

	msg := <-mc.GetMessageChannel()
	require.Equal(t, messaging.TypeSetDispatcherDrainTargetRequest, msg.Type)
	req := msg.Message[0].(*heartbeatpb.SetDispatcherDrainTargetRequest)
	require.Equal(t, "", req.TargetNodeId)
	require.Equal(t, epoch, req.TargetEpoch)
}

func TestHigherEpochNonEmptyHeartbeatStartsNewStaleClear(t *testing.T) {
	c, _, target := newDrainTestController(t)
	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}
	bootstrapTrackedNodes(c, target, other)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)
	setDrainProtocolVersion(c, other, heartbeatpb.CurrentDrainProtocolVersion)

	epoch, err := c.ensureDispatcherDrainTarget(context.Background(), target)
	require.NoError(t, err)

	c.clearDispatcherDrainTarget(target, epoch)
	require.NotNil(t, c.drainClearState)

	changed := c.observeDispatcherDrainTargetHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		DispatcherDrainTargetNodeId: other.String(),
		DispatcherDrainTargetEpoch:  epoch + 1,
	})

	require.True(t, changed)
	require.NotNil(t, c.drainClearState)
	require.Equal(t, other, c.drainClearState.target)
	require.Equal(t, epoch+1, c.drainClearState.epoch)
	require.Contains(t, c.drainClearState.pendingNodes, target)
	require.Contains(t, c.drainClearState.pendingNodes, other)
}

func TestHigherEpochEmptyHeartbeatAcknowledgesPendingClear(t *testing.T) {
	c, _, target := newDrainTestController(t)
	bootstrapTrackedNodes(c, target)
	setDrainProtocolVersion(c, target, heartbeatpb.CurrentDrainProtocolVersion)

	epoch, err := c.ensureDispatcherDrainTarget(context.Background(), target)
	require.NoError(t, err)

	c.clearDispatcherDrainTarget(target, epoch)
	require.NotNil(t, c.drainClearState)

	c.observeDispatcherDrainTargetHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		DispatcherDrainTargetEpoch: epoch + 1,
	})
	require.Nil(t, c.drainClearState)
}

func TestRemoveNodeAcknowledgesPendingClear(t *testing.T) {
	c, _, target := newDrainTestController(t)
	bootstrapTrackedNodes(c, target)
	other := node.ID("other")
	c.nodeManager.GetAliveNodes()[other] = &node.Info{ID: other}
	bootstrapTrackedNodes(c, other)

	epoch, err := c.ensureDispatcherDrainTarget(context.Background(), target)
	require.NoError(t, err)

	c.clearDispatcherDrainTarget(target, epoch)
	require.NotNil(t, c.drainClearState)
	require.Len(t, c.drainClearState.pendingNodes, 2)

	c.RemoveNode(other)
	require.NotNil(t, c.drainClearState)
	require.Len(t, c.drainClearState.pendingNodes, 1)

	c.RemoveNode(target)
	require.Nil(t, c.drainClearState)
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

func drainMessageChannel(ch chan *messaging.TargetMessage) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func outboundMessages(c *Controller) chan *messaging.TargetMessage {
	return c.messageCenter.(interface {
		GetMessageChannel() chan *messaging.TargetMessage
	}).GetMessageChannel()
}

func requireNoMessage(t *testing.T, ch chan *messaging.TargetMessage) {
	t.Helper()

	select {
	case msg := <-ch:
		require.Failf(t, "unexpected message", "type=%s to=%s", msg.Type.String(), msg.To.String())
	default:
	}
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

func setChangefeedDrainStatus(
	cf *changefeed.Changefeed,
	target node.ID,
	epoch uint64,
	dispatcherCount uint32,
	inflightDrainMoveCount uint32,
) {
	status := cf.GetStatus()
	_, _, _ = cf.ForceUpdateStatus(&heartbeatpb.MaintainerStatus{
		ChangefeedID: cf.ID.ToPB(),
		CheckpointTs: status.CheckpointTs,
		DrainProgress: &heartbeatpb.DrainProgress{
			TargetNodeId:                 target.String(),
			TargetEpoch:                  epoch,
			TargetDispatcherCount:        dispatcherCount,
			TargetInflightDrainMoveCount: inflightDrainMoveCount,
		},
	})
}

func setDrainProtocolVersion(c *Controller, target node.ID, version uint32) {
	c.drainController.ObserveBootstrapResponse(target, &heartbeatpb.CoordinatorBootstrapResponse{
		DrainProtocolVersion: version,
	})
}

func bootstrapTrackedNodes(c *Controller, ids ...node.ID) {
	if c.bootstrapper == nil {
		return
	}
	c.bootstrapper.HandleNodesChange(c.nodeManager.GetAliveNodes())
	for _, id := range ids {
		c.bootstrapper.HandleBootstrapResponse(id, &heartbeatpb.CoordinatorBootstrapResponse{})
	}
}
