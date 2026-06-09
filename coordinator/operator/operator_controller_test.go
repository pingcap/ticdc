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

package operator

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	scheduleroperator "github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

type operatorEpochPDClient struct {
	pd.Client
	physical int64
	logical  int64
}

func (m *operatorEpochPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	return m.physical, m.logical, nil
}

func newOperatorControllerForTest(
	t *testing.T,
	changefeedDB *changefeed.ChangefeedDB,
	backend changefeed.Backend,
	pdClient pd.Client,
) (*Controller, *node.Info, *watcher.NodeManager) {
	t.Helper()

	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self

	appcontext.SetService(appcontext.MessageCenter, messaging.NewMockMessageCenter())
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	return NewOperatorController(self, changefeedDB, backend, pdClient, 10), self, nodeManager
}

func TestController_StopChangefeed(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, self, _ := newOperatorControllerForTest(t, changefeedDB, backend, nil)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, self.ID)

	oc.StopChangefeed(context.Background(), cfID, false)
	require.Len(t, oc.operators, 1)
	// the old  PostFinish will be called
	backend.EXPECT().SetChangefeedProgress(gomock.Any(), gomock.Any(), config.ProgressNone).Return(nil).Times(1)
	oc.StopChangefeed(context.Background(), cfID, true)
	require.Len(t, oc.operators, 1)
	oc.StopChangefeed(context.Background(), cfID, true)
	require.Len(t, oc.operators, 1)
}

func TestController_StopChangefeedWithMaintainerEpoch(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, self, _ := newOperatorControllerForTest(t, changefeedDB, backend, nil)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
		Epoch:        20,
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, self.ID)

	op := oc.StopChangefeedWithMaintainerEpoch(context.Background(), cfID, false, 10)
	req := op.Schedule().Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	require.Equal(t, uint64(10), req.MaintainerEpoch)
}

func TestController_StopRemoteMaintainerWithMaintainerEpoch(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, _, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend, nil)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
		Epoch:        20,
	}, 1, true)
	changefeedDB.AddAbsentChangefeed(cf)

	oldNode := node.ID("old-node")
	nodeManager.GetAliveNodes()[oldNode] = &node.Info{ID: oldNode}
	op := oc.StopRemoteMaintainerWithMaintainerEpoch(cfID, oldNode, false, 10)
	reqMsg := op.Schedule()
	require.Equal(t, oldNode, reqMsg.To)
	req := reqMsg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	require.Equal(t, uint64(10), req.MaintainerEpoch)

	op.Check(oldNode, &heartbeatpb.MaintainerStatus{
		State:           heartbeatpb.ComponentState_Stopped,
		MaintainerEpoch: 20,
	})
	require.False(t, op.IsFinished())

	op.Check(oldNode, &heartbeatpb.MaintainerStatus{
		State:           heartbeatpb.ComponentState_Stopped,
		MaintainerEpoch: 10,
	})
	require.True(t, op.IsFinished())
}

func TestController_AddOperator(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, self, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend, nil)
	target := node.NewInfo("localhost:8301", "")
	nodeManager.GetAliveNodes()[target.ID] = target
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, self.ID)

	require.True(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, target.ID)))
	require.False(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, target.ID)))
	cf2ID := common.NewChangeFeedIDWithName("test2", common.DefaultKeyspaceName)
	cf2 := changefeed.NewChangefeed(cf2ID, &config.ChangeFeedInfo{
		ChangefeedID: cf2ID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	require.False(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf2, target.ID)))

	require.NotNil(t, oc.GetOperator(cfID))
	require.Nil(t, oc.GetOperator(cf2ID))

	require.True(t, oc.HasOperator(cfID.DisplayName))
	require.False(t, oc.HasOperator(cf2ID.DisplayName))
}

func TestController_HasOperatorInvolvingNode(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, self, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend, nil)
	target := node.NewInfo("localhost:8301", "")
	nodeManager.GetAliveNodes()[target.ID] = target

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddReplicatingMaintainer(cf, self.ID)

	require.True(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, target.ID)))

	require.Equal(t, 1, oc.CountOperatorsInvolvingNode(target.ID))
	require.Equal(t, 0, oc.CountOperatorsInvolvingNode("n3"))
	require.True(t, oc.HasOperatorInvolvingNode(target.ID))
	require.False(t, oc.HasOperatorInvolvingNode("n3"))
}

func TestController_CountMoveMaintainerOperatorsFromNodes(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, self, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend, nil)
	dest := node.NewInfo("localhost:8301", "")
	nodeManager.GetAliveNodes()[dest.ID] = dest

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddReplicatingMaintainer(cf, self.ID)

	require.True(t, oc.AddOperator(NewMoveMaintainerOperator(changefeedDB, cf, self.ID, dest.ID)))

	require.Equal(t, 1, oc.CountMoveMaintainerOperatorsFromNodes([]node.ID{self.ID}))
	require.Equal(t, 0, oc.CountMoveMaintainerOperatorsFromNodes([]node.ID{"n3"}))
}

func TestController_AddOperatorBumpsAndPersistsOwnershipEpoch(t *testing.T) {
	testCases := []struct {
		name    string
		addToDB func(*changefeed.ChangefeedDB, *changefeed.Changefeed, node.ID)
		newOp   func(*changefeed.ChangefeedDB, *changefeed.Changefeed, node.ID, node.ID) scheduleroperator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]
	}{
		{
			name: "add-maintainer",
			addToDB: func(db *changefeed.ChangefeedDB, cf *changefeed.Changefeed, self node.ID) {
				db.AddAbsentChangefeed(cf)
			},
			newOp: func(db *changefeed.ChangefeedDB, cf *changefeed.Changefeed, self, dest node.ID) scheduleroperator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus] {
				return NewAddMaintainerOperator(db, cf, dest)
			},
		},
		{
			name: "move-maintainer",
			addToDB: func(db *changefeed.ChangefeedDB, cf *changefeed.Changefeed, self node.ID) {
				db.AddReplicatingMaintainer(cf, self)
			},
			newOp: func(db *changefeed.ChangefeedDB, cf *changefeed.Changefeed, self, dest node.ID) scheduleroperator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus] {
				return NewMoveMaintainerOperator(db, cf, self, dest)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			changefeedDB := changefeed.NewChangefeedDB(1216)
			ctrl := gomock.NewController(t)
			backend := mock_changefeed.NewMockBackend(ctrl)
			oc, self, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend, &operatorEpochPDClient{physical: 100, logical: 1})
			target := node.NewInfo("localhost:8301", "")
			nodeManager.GetAliveNodes()[target.ID] = target

			candidateEpoch := oracle.ComposeTS(100, 1)
			oldEpoch := candidateEpoch + 10
			expectedEpoch := oldEpoch + 1
			cfID := common.NewChangeFeedIDWithName(tc.name, common.DefaultKeyspaceName)
			cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
				ChangefeedID: cfID,
				Config:       config.GetDefaultReplicaConfig(),
				SinkURI:      "mysql://127.0.0.1:3306",
				Epoch:        oldEpoch,
			}, 123, true)
			tc.addToDB(changefeedDB, cf, self.ID)

			backend.EXPECT().
				BumpChangefeedEpoch(gomock.Any(), cfID, gomock.Any(), changefeed.EpochBumpOptions{}).
				DoAndReturn(func(ctx context.Context, id common.ChangeFeedID, candidateEpoch uint64, options changefeed.EpochBumpOptions) (*config.ChangeFeedInfo, error) {
					require.NotZero(t, candidateEpoch)
					require.Equal(t, cfID, id)
					require.False(t, options.UpdateStatus)
					info, err := cf.GetInfo().Clone()
					require.NoError(t, err)
					require.Equal(t, oldEpoch, info.Epoch)
					info.Epoch = expectedEpoch
					return info, nil
				}).
				Times(1)

			op := tc.newOp(changefeedDB, cf, self.ID, target.ID)
			require.True(t, oc.AddOperator(op))
			require.Equal(t, expectedEpoch, cf.GetInfo().Epoch)
			if move, ok := op.(*MoveMaintainerOperator); ok {
				removeReq := move.Schedule().Message[0].(*heartbeatpb.RemoveMaintainerRequest)
				require.Equal(t, oldEpoch, removeReq.MaintainerEpoch)

				move.Check(self.ID, &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped})
				addReq := move.Schedule().Message[0].(*heartbeatpb.AddMaintainerRequest)
				info := &config.ChangeFeedInfo{}
				require.NoError(t, json.Unmarshal(addReq.Config, info))
				require.Equal(t, expectedEpoch, info.Epoch)
			}

			req := cf.NewAddMaintainerMessage(target.ID).Message[0].(*heartbeatpb.AddMaintainerRequest)
			info := &config.ChangeFeedInfo{}
			require.NoError(t, json.Unmarshal(req.Config, info))
			require.Equal(t, expectedEpoch, info.Epoch)
		})
	}
}

func TestController_AddOperatorRejectsConcurrentEpochBump(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, _, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend, &operatorEpochPDClient{physical: 100, logical: 1})
	target1 := node.NewInfo("localhost:8301", "")
	target2 := node.NewInfo("localhost:8302", "")
	nodeManager.GetAliveNodes()[target1.ID] = target1
	nodeManager.GetAliveNodes()[target2.ID] = target2

	cfID := common.NewChangeFeedIDWithName("concurrent-epoch-bump", common.DefaultKeyspaceName)
	oldEpoch := uint64(10)
	newEpoch := uint64(20)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
		Epoch:        oldEpoch,
	}, 123, true)
	changefeedDB.AddAbsentChangefeed(cf)

	bumpStarted := make(chan struct{})
	releaseBump := make(chan struct{})
	backend.EXPECT().
		BumpChangefeedEpoch(gomock.Any(), cfID, gomock.Any(), changefeed.EpochBumpOptions{}).
		DoAndReturn(func(ctx context.Context, id common.ChangeFeedID, candidateEpoch uint64, options changefeed.EpochBumpOptions) (*config.ChangeFeedInfo, error) {
			close(bumpStarted)
			<-releaseBump
			info, err := cf.GetInfo().Clone()
			require.NoError(t, err)
			info.Epoch = newEpoch
			return info, nil
		}).
		Times(1)

	firstResult := make(chan bool, 1)
	go func() {
		firstResult <- oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, target1.ID))
	}()
	select {
	case <-bumpStarted:
	case <-time.After(time.Second):
		close(releaseBump)
		require.FailNow(t, "timed out waiting for epoch bump to start")
	}

	require.False(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, target2.ID)))
	close(releaseBump)
	select {
	case ok := <-firstResult:
		require.True(t, ok)
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for epoch bump to finish")
	}
	require.Equal(t, newEpoch, cf.GetInfo().Epoch)
	require.NotNil(t, oc.GetOperator(cfID))
}

func TestController_StopChangefeedDuringMoveUsesOriginEpoch(t *testing.T) {
	testCases := []struct {
		name                    string
		originStoppedBeforeStop bool
	}{
		{
			name: "before-origin-stopped",
		},
		{
			name:                    "after-origin-stopped-before-dest-bind",
			originStoppedBeforeStop: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			changefeedDB := changefeed.NewChangefeedDB(1216)
			ctrl := gomock.NewController(t)
			backend := mock_changefeed.NewMockBackend(ctrl)
			oc, self, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend, &operatorEpochPDClient{physical: 100, logical: 1})
			target := node.NewInfo("localhost:8301", "")
			nodeManager.GetAliveNodes()[target.ID] = target

			oldEpoch := uint64(10)
			newEpoch := uint64(20)
			cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
			cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
				ChangefeedID: cfID,
				Config:       config.GetDefaultReplicaConfig(),
				SinkURI:      "mysql://127.0.0.1:3306",
				Epoch:        oldEpoch,
			}, 1, true)
			changefeedDB.AddReplicatingMaintainer(cf, self.ID)

			backend.EXPECT().
				BumpChangefeedEpoch(gomock.Any(), cfID, gomock.Any(), changefeed.EpochBumpOptions{}).
				DoAndReturn(func(ctx context.Context, id common.ChangeFeedID, candidateEpoch uint64, options changefeed.EpochBumpOptions) (*config.ChangeFeedInfo, error) {
					info, err := cf.GetInfo().Clone()
					require.NoError(t, err)
					info.Epoch = newEpoch
					return info, nil
				}).
				Times(1)

			moveOp := NewMoveMaintainerOperator(changefeedDB, cf, self.ID, target.ID)
			require.True(t, oc.AddOperator(moveOp))
			require.Equal(t, newEpoch, cf.GetInfo().Epoch)

			if tc.originStoppedBeforeStop {
				moveOp.Check(self.ID, &heartbeatpb.MaintainerStatus{
					State:           heartbeatpb.ComponentState_Stopped,
					MaintainerEpoch: oldEpoch,
				})
				require.True(t, moveOp.originNodeStopped)
				require.False(t, moveOp.bind)
			}

			stopOp := oc.StopChangefeed(context.Background(), cfID, false)
			require.Equal(t, "stop", stopOp.Type())
			reqMsg := stopOp.Schedule()
			require.Equal(t, self.ID, reqMsg.To)
			req := reqMsg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
			require.Equal(t, oldEpoch, req.MaintainerEpoch)

			stopOp.Check(self.ID, &heartbeatpb.MaintainerStatus{
				State:           heartbeatpb.ComponentState_Stopped,
				MaintainerEpoch: newEpoch,
			})
			require.False(t, stopOp.IsFinished())

			stopOp.Check(self.ID, &heartbeatpb.MaintainerStatus{
				State:           heartbeatpb.ComponentState_Stopped,
				MaintainerEpoch: oldEpoch,
			})
			require.True(t, stopOp.IsFinished())
		})
	}
}

func TestController_AddOperatorEpochBumpDoesNotBlockStatusAndStop(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, _, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend, &operatorEpochPDClient{physical: 100, logical: 1})
	target := node.NewInfo("localhost:8301", "")
	nodeManager.GetAliveNodes()[target.ID] = target

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
		Epoch:        10,
	}, 1, true)
	changefeedDB.AddAbsentChangefeed(cf)

	bumpStarted := make(chan struct{})
	bumpHasDeadline := make(chan bool, 1)
	unblockBump := make(chan struct{})
	var unblockOnce sync.Once
	unblock := func() {
		unblockOnce.Do(func() {
			close(unblockBump)
		})
	}
	t.Cleanup(unblock)

	backend.EXPECT().
		BumpChangefeedEpoch(gomock.Any(), cfID, gomock.Any(), changefeed.EpochBumpOptions{}).
		DoAndReturn(func(ctx context.Context, id common.ChangeFeedID, candidateEpoch uint64, options changefeed.EpochBumpOptions) (*config.ChangeFeedInfo, error) {
			_, ok := ctx.Deadline()
			bumpHasDeadline <- ok
			close(bumpStarted)
			select {
			case <-unblockBump:
				info, err := cf.GetInfo().Clone()
				if err != nil {
					return nil, err
				}
				info.Epoch = candidateEpoch
				return info, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).
		Times(1)

	addDone := make(chan bool, 1)
	go func() {
		addDone <- oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, target.ID))
	}()

	select {
	case <-bumpStarted:
	case <-time.After(time.Second):
		t.Fatal("AddOperator did not reach epoch bump")
	}
	require.True(t, <-bumpHasDeadline)

	statusDone := make(chan struct{}, 1)
	go func() {
		oc.UpdateOperatorStatus(cfID, target.ID, &heartbeatpb.MaintainerStatus{
			State: heartbeatpb.ComponentState_Working,
		})
		statusDone <- struct{}{}
	}()
	select {
	case <-statusDone:
	case <-time.After(time.Second):
		t.Fatal("UpdateOperatorStatus blocked behind AddOperator epoch bump")
	}

	stopDone := make(chan scheduleroperator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus], 1)
	go func() {
		stopDone <- oc.StopChangefeed(context.Background(), cfID, true)
	}()
	var stopOp scheduleroperator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]
	select {
	case stopOp = <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("StopChangefeed blocked behind AddOperator epoch bump")
	}
	require.NotNil(t, stopOp)
	require.Equal(t, "stop", stopOp.Type())
	require.Equal(t, 1, oc.OperatorSize())

	unblock()
	select {
	case added := <-addDone:
		require.False(t, added)
	case <-time.After(time.Second):
		t.Fatal("AddOperator did not finish after epoch bump was unblocked")
	}
	require.Equal(t, 1, oc.OperatorSize())
	require.Equal(t, "stop", oc.GetOperator(cfID).Type())
}

func TestController_StopChangefeedDuringAddOperator(t *testing.T) {
	// Setup test environment
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, _, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend, nil)
	target := node.NewInfo("localhost:8301", "")
	nodeManager.GetAliveNodes()[target.ID] = target

	// Create changefeed and add it to absent state (simulating a newly created changefeed)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddAbsentChangefeed(cf)

	// Verify changefeed is in absent state
	require.Equal(t, 1, changefeedDB.GetAbsentSize())
	require.Equal(t, 1, changefeedDB.GetSize())

	// Add AddMaintainerOperator (simulating starting to schedule the changefeed)
	addOp := NewAddMaintainerOperator(changefeedDB, cf, target.ID)
	require.True(t, oc.AddOperator(addOp))

	// Verify operator has been added
	require.Equal(t, 1, oc.OperatorSize())
	require.NotNil(t, oc.GetOperator(cfID))

	// Verify changefeed is now in scheduling state
	require.Equal(t, 0, changefeedDB.GetAbsentSize())
	require.Equal(t, 1, changefeedDB.GetSchedulingSize())

	// Execute StopChangefeed (remove=true) while AddMaintainerOperator is not yet finished
	// Set up backend expectation
	backend.EXPECT().DeleteChangefeed(gomock.Any(), cfID).Return(nil).Times(1)

	oc.StopChangefeed(context.Background(), cfID, true)

	// Verify there is now a StopChangefeedOperator
	require.Equal(t, 1, oc.OperatorSize())
	stopOp := oc.GetOperator(cfID)
	require.NotNil(t, stopOp)
	require.Equal(t, "stop", stopOp.Type())

	// Simulate StopChangefeedOperator completion (by calling Check method)
	// First simulate maintainer reporting non-working status
	stopOp.Check(target.ID, &heartbeatpb.MaintainerStatus{
		State: heartbeatpb.ComponentState_Stopped,
	})

	// Execute operator controller to trigger operator completion
	oc.Execute()

	// Verify StopChangefeedOperator has completed and been cleaned up
	require.Equal(t, 0, oc.OperatorSize())
	require.Nil(t, oc.GetOperator(cfID))

	// Verify changefeed has been removed from ChangefeedDB
	require.Equal(t, 0, changefeedDB.GetSize())
	require.Nil(t, changefeedDB.GetByID(cfID))
	require.Equal(t, 0, changefeedDB.GetAbsentSize())
	require.Equal(t, 0, changefeedDB.GetSchedulingSize())
	require.Equal(t, 0, changefeedDB.GetReplicatingSize())
}
