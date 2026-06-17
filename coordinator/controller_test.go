// Copyright 2024 PingCAP, Inc.
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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/coordinator/drain"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/bootstrap"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/eventservice"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	pkgscheduler "github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type noopScheduler struct{}

func (noopScheduler) Execute() time.Time {
	return time.Now().Add(time.Hour)
}

func (noopScheduler) Name() string {
	return pkgscheduler.BasicScheduler
}

func TestOnPeriodTaskAdvanceLiveness(t *testing.T) {
	newController := func(t *testing.T) (*Controller, chan *messaging.TargetMessage, *changefeed.ChangefeedDB, node.ID) {
		t.Helper()

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		backend := mock_changefeed.NewMockBackend(ctrl)
		changefeedDB := changefeed.NewChangefeedDB(1216)
		self := node.NewInfo("localhost:8300", "")
		target := node.NewInfo("localhost:8301", "")
		nodeManager := watcher.NewNodeManager(nil, nil)
		nodeManager.GetAliveNodes()[self.ID] = self
		nodeManager.GetAliveNodes()[target.ID] = target

		mc := messaging.NewMockMessageCenter()
		appcontext.SetService(appcontext.MessageCenter, mc)
		appcontext.SetService(watcher.NodeManagerName, nodeManager)

		return &Controller{
			changefeedDB: changefeedDB,
			operatorController: operator.NewOperatorController(
				self, changefeedDB, backend, nil, 10,
			),
			nodeManager:     nodeManager,
			initialized:     atomic.NewBool(true),
			drainController: drain.NewController(mc),
			pdClient:        newDrainTestPDClient(),
			bootstrapper: bootstrap.NewBootstrapper[heartbeatpb.CoordinatorBootstrapResponse](
				"test",
				func(node.ID, string) *messaging.TargetMessage { return nil },
			),
			messageCenter: mc,
		}, mc.GetMessageChannel(), changefeedDB, target.ID
	}

	t.Run("skip stopping before bootstrap completion", func(t *testing.T) {
		controller, messageCh, _, targetNodeID := newController(t)
		controller.initialized.Store(false)
		controller.drainController.ObserveHeartbeat(targetNodeID, &heartbeatpb.NodeHeartbeat{
			NodeEpoch: 1,
			Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		})

		controller.onPeriodTask()

		select {
		case msg := <-messageCh:
			t.Fatalf("unexpected liveness command sent before bootstrap completion: %v", msg.Type)
		default:
		}
	})

	t.Run("skip stopping without active drain session", func(t *testing.T) {
		controller, messageCh, _, targetNodeID := newController(t)
		controller.drainController.ObserveHeartbeat(targetNodeID, &heartbeatpb.NodeHeartbeat{
			NodeEpoch: 1,
			Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		})

		controller.onPeriodTask()

		select {
		case msg := <-messageCh:
			t.Fatalf("unexpected liveness command sent without active session: %v", msg.Type)
		default:
		}
	})

	t.Run("skip stopping when active drain session is not ready", func(t *testing.T) {
		controller, messageCh, changefeedDB, targetNodeID := newController(t)
		cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
		cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
			ChangefeedID: cfID,
			Config:       config.GetDefaultReplicaConfig(),
			SinkURI:      "mysql://127.0.0.1:3306",
			State:        config.StateNormal,
		}, 1, true)
		changefeedDB.AddReplicatingMaintainer(cf, targetNodeID)

		_, err := controller.ensureDispatcherDrainTarget(context.Background(), targetNodeID)
		require.NoError(t, err)

		controller.drainController.ObserveHeartbeat(targetNodeID, &heartbeatpb.NodeHeartbeat{
			NodeEpoch: 1,
			Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		})
		controller.onPeriodTask()

		for {
			select {
			case msg := <-messageCh:
				if msg.Type == messaging.TypeSetNodeLivenessRequest {
					t.Fatalf("unexpected liveness command sent before readiness: %v", msg.Type)
				}
			default:
				return
			}
		}
	})

	t.Run("send stopping only for active drain session target", func(t *testing.T) {
		controller, messageCh, _, targetNodeID := newController(t)
		_, err := controller.ensureDispatcherDrainTarget(context.Background(), targetNodeID)
		require.NoError(t, err)

		controller.drainSessionMu.Lock()
		controller.drainSession.dirty = false
		controller.drainSession.lastSent = time.Now()
		controller.drainSessionMu.Unlock()

		controller.drainController.ObserveHeartbeat(targetNodeID, &heartbeatpb.NodeHeartbeat{
			NodeEpoch: 1,
			Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		})

		controller.onPeriodTask()

		foundStop := false
		for {
			select {
			case msg := <-messageCh:
				if msg.Type != messaging.TypeSetNodeLivenessRequest {
					continue
				}
				require.Equal(t, targetNodeID, msg.To)
				req := msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
				if req.Target == heartbeatpb.NodeLiveness_STOPPING {
					require.Equal(t, uint64(1), req.NodeEpoch)
					foundStop = true
				}
			default:
				require.True(t, foundStop)
				return
			}
		}
	})
}

func TestMaintainerHeartbeatAdmissionRequiresInitializedSender(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	owner := node.ID("owner")
	late := node.ID("late")
	nodeManager.GetAliveNodes()[owner] = &node.Info{ID: owner}
	nodeManager.GetAliveNodes()[late] = &node.Info{ID: late}

	db := changefeed.NewChangefeedDB(1)
	cfID := common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "blackhole://",
		State:        config.StateNormal,
	}, 100, false)
	db.AddReplicatingMaintainer(cf, late)

	controller := &Controller{
		initialized:  atomic.NewBool(true),
		changefeedDB: db,
		operatorController: operator.NewOperatorController(
			&node.Info{ID: node.ID("coordinator")},
			db,
			nil,
			nil,
			10,
		),
		bootstrapper: bootstrap.NewBootstrapper[heartbeatpb.CoordinatorBootstrapResponse](
			"test",
			func(id node.ID, _ string) *messaging.TargetMessage {
				return messaging.NewSingleTargetMessage(
					id,
					messaging.MaintainerManagerTopic,
					&heartbeatpb.CoordinatorBootstrapRequest{},
				)
			},
		),
	}

	controller.bootstrapper.HandleNodesChange(nodeManager.GetAliveNodes())
	controller.bootstrapper.HandleBootstrapResponse(owner, &heartbeatpb.CoordinatorBootstrapResponse{})
	require.False(t, controller.bootstrapper.AllNodesReady())

	ignored := &heartbeatpb.MaintainerHeartbeat{
		Statuses: []*heartbeatpb.MaintainerStatus{{
			ChangefeedID:  cfID.ToPB(),
			CheckpointTs:  200,
			State:         heartbeatpb.ComponentState_Working,
			BootstrapDone: true,
		}},
	}
	controller.onMessage(context.Background(), &messaging.TargetMessage{
		From:    late,
		Topic:   messaging.CoordinatorTopic,
		Type:    messaging.TypeMaintainerHeartbeatRequest,
		Message: []messaging.IOTypeT{ignored},
	})
	require.Equal(t, uint64(100), cf.GetStatus().CheckpointTs)

	controller.bootstrapper.HandleBootstrapResponse(late, &heartbeatpb.CoordinatorBootstrapResponse{})
	accepted := &heartbeatpb.MaintainerHeartbeat{
		Statuses: []*heartbeatpb.MaintainerStatus{{
			ChangefeedID:  cfID.ToPB(),
			CheckpointTs:  200,
			State:         heartbeatpb.ComponentState_Working,
			BootstrapDone: true,
		}},
	}
	controller.onMessage(context.Background(), &messaging.TargetMessage{
		From:    late,
		Topic:   messaging.CoordinatorTopic,
		Type:    messaging.TypeMaintainerHeartbeatRequest,
		Message: []messaging.IOTypeT{accepted},
	})
	require.Equal(t, uint64(200), cf.GetStatus().CheckpointTs)
}

func TestMaintainerHeartbeatAdmissionDropsStaleMaintainerEpoch(t *testing.T) {
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMockMessageCenter())
	appcontext.SetService(watcher.NodeManagerName, watcher.NewNodeManager(nil, nil))

	db := changefeed.NewChangefeedDB(1)
	cfID := common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName)
	owner := node.ID("owner")
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "blackhole://",
		State:        config.StateNormal,
		Epoch:        2,
	}, 100, false)
	db.AddReplicatingMaintainer(cf, owner)

	controller := &Controller{
		changefeedDB: db,
		operatorController: operator.NewOperatorController(
			&node.Info{ID: node.ID("coordinator")},
			db,
			nil,
			nil,
			10,
		),
	}

	stale := &heartbeatpb.MaintainerStatus{
		ChangefeedID:    cfID.ToPB(),
		CheckpointTs:    200,
		State:           heartbeatpb.ComponentState_Working,
		BootstrapDone:   true,
		MaintainerEpoch: 1,
	}
	require.Nil(t, controller.handleSingleMaintainerStatus(owner, stale, cfID))
	require.Equal(t, uint64(100), cf.GetStatus().CheckpointTs)

	current := &heartbeatpb.MaintainerStatus{
		ChangefeedID:    cfID.ToPB(),
		CheckpointTs:    200,
		State:           heartbeatpb.ComponentState_Working,
		BootstrapDone:   true,
		MaintainerEpoch: 2,
	}
	require.NotNil(t, controller.handleSingleMaintainerStatus(owner, current, cfID))
	require.Equal(t, uint64(200), cf.GetStatus().CheckpointTs)
}

func TestHandleNonExistentChangefeedRemovesWithReportedEpoch(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	db := changefeed.NewChangefeedDB(1)
	controller := &Controller{
		changefeedDB: db,
		operatorController: operator.NewOperatorController(
			&node.Info{ID: node.ID("coordinator")},
			db,
			nil,
			nil,
			10,
		),
		messageCenter: mc,
	}
	cfID := common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName)

	controller.handleNonExistentChangefeed(cfID, node.ID("owner"), &heartbeatpb.MaintainerStatus{
		ChangefeedID:    cfID.ToPB(),
		State:           heartbeatpb.ComponentState_Working,
		MaintainerEpoch: 7,
	})

	msg := <-mc.GetMessageChannel()
	req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	require.Equal(t, uint64(7), req.MaintainerEpoch)
	require.True(t, req.Cascade)
	require.True(t, req.Removed)
}

func TestFinishBootstrapStopsStaleEpochMaintainerWithReportedEpoch(t *testing.T) {
	testCases := []struct {
		name          string
		progress      config.Progress
		expectRemoved bool
		expectInDB    bool
		expectAbsent  bool
		expectStopped bool
	}{
		{
			name:         "running",
			progress:     config.ProgressNone,
			expectInDB:   true,
			expectAbsent: true,
		},
		{
			name:          "removing",
			progress:      config.ProgressRemoving,
			expectRemoved: true,
		},
		{
			name:          "stopping",
			progress:      config.ProgressStopping,
			expectInDB:    true,
			expectStopped: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			backend := mock_changefeed.NewMockBackend(ctrl)
			mc := messaging.NewMockMessageCenter()
			appcontext.SetService(appcontext.MessageCenter, mc)
			appcontext.SetService(appcontext.SchemaStore, eventservice.NewMockSchemaStore())

			nodeManager := watcher.NewNodeManager(nil, nil)
			appcontext.SetService(watcher.NodeManagerName, nodeManager)
			oldNode := node.ID("old-owner")
			nodeManager.GetAliveNodes()[oldNode] = &node.Info{ID: oldNode}

			db := changefeed.NewChangefeedDB(1)
			cfID := common.NewChangeFeedIDWithName(tc.name, common.DefaultKeyspaceName)
			info := &config.ChangeFeedInfo{
				ChangefeedID: cfID,
				Config:       config.GetDefaultReplicaConfig(),
				SinkURI:      "blackhole://",
				State:        config.StateNormal,
				Epoch:        2,
			}
			db.Init(map[common.ChangeFeedID]*changefeed.Changefeed{
				cfID: changefeed.NewChangefeed(cfID, info, 100, false),
			})
			backend.EXPECT().GetAllChangefeeds(gomock.Any()).Return(map[common.ChangeFeedID]*changefeed.ChangefeedMetaWrapper{
				cfID: {
					Info:   info,
					Status: &config.ChangeFeedStatus{CheckpointTs: 100, Progress: tc.progress},
				},
			}, nil).Times(1)

			self := &node.Info{ID: node.ID("coordinator")}
			controller := &Controller{
				selfNode:     self,
				initialized:  atomic.NewBool(false),
				backend:      backend,
				changefeedDB: db,
				operatorController: operator.NewOperatorController(
					self,
					db,
					backend,
					nil,
					10,
				),
				nodeManager:   nodeManager,
				taskScheduler: threadpool.NewThreadPool(1),
				scheduler: pkgscheduler.NewController(map[string]pkgscheduler.Scheduler{
					pkgscheduler.BasicScheduler: noopScheduler{},
				}),
				messageCenter: mc,
			}
			t.Cleanup(controller.taskScheduler.Stop)

			controller.finishBootstrap(context.Background(), map[common.ChangeFeedID][]remoteMaintainer{
				cfID: {{
					nodeID: oldNode,
					status: &heartbeatpb.MaintainerStatus{
						ChangefeedID:    cfID.ToPB(),
						State:           heartbeatpb.ComponentState_Working,
						CheckpointTs:    200,
						BootstrapDone:   true,
						MaintainerEpoch: 1,
					},
				}},
			})

			if tc.expectInDB {
				require.NotNil(t, db.GetByID(cfID))
			} else {
				require.Nil(t, db.GetByID(cfID))
			}
			if tc.expectAbsent {
				require.Equal(t, 1, db.GetAbsentSize())
			}
			if tc.expectStopped {
				require.Equal(t, 1, db.GetStoppedSize())
			}

			op := controller.operatorController.GetOperator(cfID)
			require.NotNil(t, op)
			require.False(t, op.IsFinished())
			reqMsg := op.Schedule()
			require.Equal(t, oldNode, reqMsg.To)
			req := reqMsg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
			require.Equal(t, uint64(1), req.MaintainerEpoch)
			require.Equal(t, tc.expectRemoved, req.Removed)
		})
	}
}

func TestHandleBootstrapResponsesKeepsCurrentEpochAndStopsStaleDuplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	mc := messaging.NewMockMessageCenter()
	oldNode := node.ID("old-owner")
	currentNode := node.ID("current-owner")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[oldNode] = &node.Info{ID: oldNode}
	nodeManager.GetAliveNodes()[currentNode] = &node.Info{ID: currentNode}
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(appcontext.SchemaStore, eventservice.NewMockSchemaStore())
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	cfID := common.NewChangeFeedIDWithName("duplicate-epoch", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "blackhole://",
		State:        config.StateNormal,
		Epoch:        2,
	}
	backend.EXPECT().GetAllChangefeeds(gomock.Any()).Return(map[common.ChangeFeedID]*changefeed.ChangefeedMetaWrapper{
		cfID: {
			Info:   info,
			Status: &config.ChangeFeedStatus{CheckpointTs: 100},
		},
	}, nil).Times(1)

	db := changefeed.NewChangefeedDB(1)
	self := &node.Info{ID: node.ID("coordinator")}
	controller := &Controller{
		selfNode:     self,
		initialized:  atomic.NewBool(false),
		backend:      backend,
		changefeedDB: db,
		operatorController: operator.NewOperatorController(
			self,
			db,
			backend,
			nil,
			10,
		),
		nodeManager:   nodeManager,
		taskScheduler: threadpool.NewThreadPool(1),
		scheduler: pkgscheduler.NewController(map[string]pkgscheduler.Scheduler{
			pkgscheduler.BasicScheduler: noopScheduler{},
		}),
		messageCenter: mc,
		bootstrapper: bootstrap.NewBootstrapper[heartbeatpb.CoordinatorBootstrapResponse](
			"test",
			func(node.ID, string) *messaging.TargetMessage { return nil },
		),
	}
	t.Cleanup(controller.taskScheduler.Stop)

	require.NotPanics(t, func() {
		controller.handleBootstrapResponses(context.Background(), map[node.ID]*heartbeatpb.CoordinatorBootstrapResponse{
			oldNode: {
				Statuses: []*heartbeatpb.MaintainerStatus{{
					ChangefeedID:    cfID.ToPB(),
					State:           heartbeatpb.ComponentState_Working,
					CheckpointTs:    150,
					BootstrapDone:   true,
					MaintainerEpoch: 1,
				}},
			},
			currentNode: {
				Statuses: []*heartbeatpb.MaintainerStatus{{
					ChangefeedID:    cfID.ToPB(),
					State:           heartbeatpb.ComponentState_Working,
					CheckpointTs:    200,
					BootstrapDone:   true,
					MaintainerEpoch: 2,
				}},
			},
		})
	})

	cf := db.GetByID(cfID)
	require.NotNil(t, cf)
	require.Equal(t, currentNode, cf.GetNodeID())
	require.Equal(t, uint64(200), cf.GetStatus().CheckpointTs)

	op := controller.operatorController.GetOperator(cfID)
	require.NotNil(t, op)
	reqMsg := op.Schedule()
	require.Equal(t, oldNode, reqMsg.To)
	req := reqMsg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	require.Equal(t, uint64(1), req.MaintainerEpoch)
	require.False(t, req.Removed)
}

func TestResumeChangefeed(t *testing.T) {
	// Scenario: resume should propagate backend failures and update in-memory state after success.
	// Steps: try a missing changefeed, simulate a backend resume failure, then return a persisted
	// changefeed info from the backend and verify the stopped changefeed becomes normal.
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
	}
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateFailed,
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddStoppedChangefeed(cf)

	// no changefeed
	require.NotNil(t, controller.ResumeChangefeed(context.Background(), common.NewChangeFeedIDWithName("test2", common.DefaultKeyspaceName), 12, true))

	backend.EXPECT().BumpChangefeedEpoch(gomock.Any(), cfID, gomock.Any(), gomock.Any()).Return(nil, errors.New("failed")).Times(1)
	require.NotNil(t, controller.ResumeChangefeed(context.Background(), cfID, 12, true))
	require.Equal(t, config.StateFailed, changefeedDB.GetByID(cfID).GetInfo().State)

	expectResumeEpochBump(t, backend, cfID, cf, 12)
	require.Nil(t, controller.ResumeChangefeed(context.Background(), cfID, 12, false))
	require.Equal(t, config.StateNormal, changefeedDB.GetByID(cfID).GetInfo().State)
}

func TestResumeChangefeedNormalState(t *testing.T) {
	// Scenario: a running changefeed receives a resume request. The controller
	// should reject it before touching backend state, so the epoch remains unchanged.
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
	}
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
		SinkURI:      "mysql://127.0.0.1:3306",
		Epoch:        233,
	}, 1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "node1")

	err := controller.ResumeChangefeed(context.Background(), cfID, 12, true)
	require.True(t, errors.ErrChangefeedUpdateRefused.Equal(err))

	changefeed := controller.changefeedDB.GetByID(cfID)
	require.Equal(t, changefeed.GetInfo().Epoch, uint64(233))
}

func TestResumeChangefeedOverwriteUpdatesLastSavedCheckpointTs(t *testing.T) {
	// Scenario: overwrite resume should reset the persisted checkpoint baseline.
	// Steps: resume a stopped changefeed with overwriteCheckpointTs and verify the in-memory
	// last saved checkpoint is updated to the requested checkpoint.
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
	}
	cfID := common.NewChangeFeedIDWithName("test-overwrite", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateStopped,
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 100, true)
	cf.SetLastSavedCheckPointTs(200)
	changefeedDB.AddStoppedChangefeed(cf)

	newCheckpointTs := uint64(120)
	expectResumeEpochBump(t, backend, cfID, cf, newCheckpointTs)
	require.Nil(t, controller.ResumeChangefeed(context.Background(), cfID, newCheckpointTs, true))
	require.Equal(t, newCheckpointTs, changefeedDB.GetByID(cfID).GetLastSavedCheckPointTs())
}

func TestResumeChangefeedIgnoresStaleMaintainerErrorAndSchedules(t *testing.T) {
	// Scenario: a stale maintainer error kept in memory must not block manual resume.
	// Steps: install an errored in-memory status, resume from the backend, and verify
	// the changefeed is scheduled with a clean status.
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
	}
	cfID := common.NewChangeFeedIDWithName("test-stale-error", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateFailed,
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 100, true)
	changefeedDB.AddStoppedChangefeed(cf)

	// Simulate a stale maintainer error retained in the in-memory status.
	stale := cf.GetStatusForResume()
	stale.State = heartbeatpb.ComponentState_Working
	stale.BootstrapDone = true
	stale.Err = []*heartbeatpb.RunningError{{
		Node:    "node1",
		Code:    "CDC:ErrChangefeedRetryable",
		Message: "stale error",
	}}
	_, _, err := cf.ForceUpdateStatus(stale)
	require.NotNil(t, err)

	expectResumeEpochBump(t, backend, cfID, cf, 100)
	require.NoError(t, controller.ResumeChangefeed(context.Background(), cfID, 100, false))

	// The changefeed should be enqueued for scheduling and should not be blocked by the stale error.
	waiting, _ := changefeedDB.GetWaitingSchedulingChangefeeds(10)
	require.Len(t, waiting, 1)
	require.Equal(t, cf, waiting[0])

	status := cf.GetStatus()
	require.False(t, status.BootstrapDone)
	require.Len(t, status.Err, 0)
	require.True(t, cf.ShouldRun())
}

func TestResumeChangefeedUsesBackendReturnedInfo(t *testing.T) {
	// Scenario: stopped changefeed metadata can be edited directly in the backend while
	// the coordinator still has an older in-memory copy. Steps: resume the changefeed
	// with epoch-bumped backend info whose sink URI differs from memory, then verify
	// the in-memory changefeed uses the backend value instead of overwriting it.
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
	}
	cfID := common.NewChangeFeedIDWithName("test-backend-info", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateStopped,
		SinkURI:      "mysql://downstream:3306",
	}, 100, true)
	changefeedDB.AddStoppedChangefeed(cf)

	backendInfo, err := cf.GetInfo().Clone()
	require.NoError(t, err)
	backendInfo.SinkURI = "mysql://upstream:4000"
	backendInfo.State = config.StateNormal
	backend.EXPECT().BumpChangefeedEpoch(gomock.Any(), cfID, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ common.ChangeFeedID, candidateEpoch uint64, options changefeed.EpochBumpOptions) (*config.ChangeFeedInfo, error) {
			require.NotZero(t, candidateEpoch)
			require.True(t, options.UpdateStatus)
			require.True(t, options.UpdateError)
			require.NotNil(t, options.State)
			require.Equal(t, config.StateNormal, *options.State)
			backendInfo.Epoch = candidateEpoch
			return backendInfo, nil
		}).Times(1)

	require.NoError(t, controller.ResumeChangefeed(context.Background(), cfID, 100, false))
	require.Equal(t, "mysql://upstream:4000", changefeedDB.GetByID(cfID).GetInfo().SinkURI)
	require.Equal(t, config.StateNormal, changefeedDB.GetByID(cfID).GetInfo().State)
}

func expectResumeEpochBump(
	t *testing.T,
	backend *mock_changefeed.MockBackend,
	cfID common.ChangeFeedID,
	cf *changefeed.Changefeed,
	checkpointTs uint64,
) {
	t.Helper()

	backend.EXPECT().BumpChangefeedEpoch(gomock.Any(), cfID, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ common.ChangeFeedID, candidateEpoch uint64, options changefeed.EpochBumpOptions) (*config.ChangeFeedInfo, error) {
			require.NotZero(t, candidateEpoch)
			require.NotNil(t, options.State)
			require.Equal(t, config.StateNormal, *options.State)
			require.True(t, options.UpdateStatus)
			require.Equal(t, checkpointTs, options.CheckpointTs)
			require.Equal(t, config.ProgressNone, options.Progress)
			info, err := cf.GetInfo().Clone()
			require.NoError(t, err)
			info.State = *options.State
			info.Epoch = candidateEpoch
			return info, nil
		}).Times(1)
}

func TestPauseChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)

	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self

	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(node.NewInfo("node1", ""),
			changefeedDB, backend, nil, 10),
	}
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "node1")

	// no changefeed
	require.NotNil(t, controller.PauseChangefeed(context.Background(), common.NewChangeFeedIDWithName("test2", common.DefaultKeyspaceName)))

	go func() {
		for {
			op := controller.operatorController.GetOperator(cfID)
			if op != nil {
				op.OnTaskRemoved()
			}
			time.Sleep(time.Second)
		}
	}()
	backend.EXPECT().PauseChangefeed(gomock.Any(), gomock.Any()).Return(errors.New("failed")).Times(1)
	require.NotNil(t, controller.PauseChangefeed(context.Background(), cfID))
	require.Equal(t, config.StateNormal, changefeedDB.GetByID(cfID).GetInfo().State)

	backend.EXPECT().PauseChangefeed(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	require.Nil(t, controller.PauseChangefeed(context.Background(), cfID))
	require.Equal(t, config.StateStopped, changefeedDB.GetByID(cfID).GetInfo().State)
	require.Equal(t, 1, changefeedDB.GetStoppedSize())
}

func TestUpdateChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
	}
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateStopped,
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddStoppedChangefeed(cf)

	newConfig := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "kafka://127.0.0.1:9092",
	}
	// no changefeed
	require.NotNil(t, controller.UpdateChangefeed(context.Background(), &config.ChangeFeedInfo{
		ChangefeedID: common.NewChangeFeedIDWithName("test1", common.DefaultKeyspaceName),
	}))

	backend.EXPECT().UpdateChangefeed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("failed")).Times(1)
	require.NotNil(t, controller.UpdateChangefeed(context.Background(), newConfig))
	require.Equal(t, false, changefeedDB.GetByID(cfID).NeedCheckpointTsMessage())

	backend.EXPECT().UpdateChangefeed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	require.Nil(t, controller.UpdateChangefeed(context.Background(), newConfig))
	require.Equal(t, true, changefeedDB.GetByID(cfID).NeedCheckpointTsMessage())
	require.Equal(t, 1, changefeedDB.GetStoppedSize())
}

func TestGetChangefeed(t *testing.T) {
	// Scenario: API-facing GetChangefeed must not expose coordinator-owned mutable info.
	// Steps: fetch a changefeed, mutate the returned copy, and verify the in-memory
	// changefeed state is unchanged while status fields are still reported.
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	nodeManager := watcher.NewNodeManager(nil, nil)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		nodeManager:  nodeManager,
	}
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateStopped,
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddStoppedChangefeed(cf)

	ret, status, err := controller.GetChangefeed(context.Background(), cfID.DisplayName)
	require.Nil(t, err)
	require.Equal(t, ret.State, config.StateStopped)
	require.Equal(t, uint64(1), status.CheckpointTs)

	ret.SinkURI = "kafka://127.0.0.1:9092"
	ret.Config = nil
	storedInfo := changefeedDB.GetByID(cfID).GetInfo()
	require.Equal(t, "mysql://127.0.0.1:3306", storedInfo.SinkURI)
	require.NotNil(t, storedInfo.Config)

	_, _, err = controller.GetChangefeed(context.Background(), common.NewChangeFeedDisplayName("test1", "default"))
	require.True(t, errors.ErrChangeFeedNotExists.Equal(err))
}

func TestGetChangefeedReturnedInfoMutationDoesNotRaceWithStoredInfo(t *testing.T) {
	// Scenario: API handlers may mutate the info returned from GetChangefeed while coordinator
	// goroutines read the stored info. Steps: mutate the returned copy and read the stored
	// info concurrently, then verify the stored fields remain unchanged.
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	nodeManager := watcher.NewNodeManager(nil, nil)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		nodeManager:  nodeManager,
	}
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateStopped,
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddStoppedChangefeed(cf)

	ret, _, err := controller.GetChangefeed(context.Background(), cfID.DisplayName)
	require.NoError(t, err)

	var (
		wg            sync.WaitGroup
		storedChanged bool
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			ret.SinkURI = "kafka://127.0.0.1:9092"
			ret.TargetTs = uint64(i + 1)
		}
	}()
	go func() {
		defer wg.Done()
		storedInfo := changefeedDB.GetByID(cfID).GetInfo()
		for i := 0; i < 1000; i++ {
			if storedInfo.SinkURI != "mysql://127.0.0.1:3306" || storedInfo.TargetTs != 0 {
				storedChanged = true
				return
			}
		}
	}()
	wg.Wait()
	require.False(t, storedChanged)
}

func TestRemoveChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self

	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(node.NewInfo("node1", ""),
			changefeedDB, backend, nil, 10),
	}
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	go func() {
		for {
			op := controller.operatorController.GetOperator(cfID)
			if op != nil {
				op.OnTaskRemoved()
			}
			time.Sleep(time.Second)
		}
	}()
	changefeedDB.AddReplicatingMaintainer(cf, "node1")
	// no changefeed
	_, err := controller.RemoveChangefeed(context.Background(), common.NewChangeFeedIDWithName("test2", common.DefaultKeyspaceName))
	require.NotNil(t, err)

	backend.EXPECT().SetChangefeedProgress(gomock.Any(), cfID, config.ProgressRemoving).Return(errors.New("failed")).Times(1)
	_, err = controller.RemoveChangefeed(context.Background(), cfID)
	require.NotNil(t, err)

	backend.EXPECT().SetChangefeedProgress(gomock.Any(), cfID, config.ProgressRemoving).Return(nil).Times(1)
	cp, err := controller.RemoveChangefeed(context.Background(), cfID)
	require.Nil(t, err)
	require.Equal(t, uint64(1), cp)
}

func TestListChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(node.NewInfo("node1", ""),
			changefeedDB, backend, nil, 10),
	}
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "node1")
	cf2ID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf2 := changefeed.NewChangefeed(cf2ID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		2, true)
	changefeedDB.AddAbsentChangefeed(cf2)

	cf3ID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf3 := changefeed.NewChangefeed(cf3ID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		2, true)
	changefeedDB.AddStoppedChangefeed(cf3)
	cfs, status, err := controller.ListChangefeeds(context.Background(), common.DefaultKeyspaceName)
	require.Nil(t, err)
	require.Len(t, cfs, 3)
	require.Len(t, status, 3)
}

func TestCreateChangefeed(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(node.NewInfo("node1", ""),
			changefeedDB, backend, nil, 10),
		initialized: atomic.NewBool(false),
	}
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cfConfig := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		State:        config.StateNormal,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "kafka://127.0.0.1:9092",
	}
	require.NotNil(t, controller.CreateChangefeed(context.Background(), cfConfig))
	require.Equal(t, 0, changefeedDB.GetSize())

	controller.initialized.Store(true)
	backend.EXPECT().CreateChangefeed(gomock.Any(), gomock.Any()).Return(errors.New("failed")).Times(1)
	require.NotNil(t, controller.CreateChangefeed(context.Background(), cfConfig))
	require.Equal(t, 0, changefeedDB.GetSize())

	backend.EXPECT().CreateChangefeed(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	require.Nil(t, controller.CreateChangefeed(context.Background(), cfConfig))

	// add it again
	require.Equal(t, 1, changefeedDB.GetAbsentSize())
	require.NotNil(t, controller.CreateChangefeed(context.Background(), cfConfig))

	// changefeed is in stopping
	require.Equal(t, 1, changefeedDB.GetAbsentSize())
	controller.operatorController.AddOperator(operator.NewAddMaintainerOperator(changefeedDB, changefeedDB.GetByID(cfID), "node1"))

	cf2ID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf2Config := &config.ChangeFeedInfo{
		ChangefeedID: cf2ID,
		State:        config.StateNormal,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "kafka://127.0.0.1:9092",
	}
	require.NotNil(t, controller.CreateChangefeed(context.Background(), cf2Config))
}
