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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type deadlineCheckingPDClient struct {
	pd.Client
}

func (m *deadlineCheckingPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	if _, ok := ctx.Deadline(); !ok {
		return 0, 0, errors.New("missing deadline")
	}
	return 100, 1, nil
}

func newOperatorControllerForTest(
	t *testing.T,
	changefeedDB *changefeed.ChangefeedDB,
	backend changefeed.Backend,
) (*Controller, *node.Info, *watcher.NodeManager) {
	t.Helper()

	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self

	appcontext.SetService(appcontext.MessageCenter, messaging.NewMockMessageCenter())
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	return NewOperatorController(self, changefeedDB, backend, 10, nil), self, nodeManager
}

func TestController_StopChangefeed(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, self, _ := newOperatorControllerForTest(t, changefeedDB, backend)
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

func TestController_StopChangefeedCarriesCurrentMaintainerSessionEpoch(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, self, _ := newOperatorControllerForTest(t, changefeedDB, backend)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	cf.SetCurrentMaintainerSessionEpoch(66)
	changefeedDB.AddReplicatingMaintainer(cf, self.ID)

	op := oc.StopChangefeed(context.Background(), cfID, false)
	stopOp, ok := op.(*StopChangefeedOperator)
	require.True(t, ok)
	require.Equal(t, uint64(66), stopOp.sessionEpoch)
	require.Equal(t, uint64(66), stopOp.Schedule().Message[0].(*heartbeatpb.RemoveMaintainerRequest).SessionEpoch)
}

func TestController_StopChangefeedCarriesPublishedSessionEpochDuringAdd(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, _, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend)
	target := node.NewInfo("localhost:8301", "")
	nodeManager.GetAliveNodes()[target.ID] = target

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddAbsentChangefeed(cf)

	require.True(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, target.ID, 42)))

	op := oc.StopChangefeed(context.Background(), cfID, true)
	stopOp, ok := op.(*StopChangefeedOperator)
	require.True(t, ok)
	require.Equal(t, target.ID, stopOp.nodeID)
	require.Equal(t, uint64(42), stopOp.sessionEpoch)
	require.Equal(t, uint64(42), stopOp.Schedule().Message[0].(*heartbeatpb.RemoveMaintainerRequest).SessionEpoch)
}

func TestController_StopChangefeedDuringAddAcceptsLegacyZeroSessionAck(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, _, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend)
	target := node.NewInfo("localhost:8301", "")
	nodeManager.GetAliveNodes()[target.ID] = target

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddAbsentChangefeed(cf)

	require.True(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, target.ID, 42)))

	op := oc.StopChangefeed(context.Background(), cfID, false)
	stopOp, ok := op.(*StopChangefeedOperator)
	require.True(t, ok)

	stopOp.Check(target.ID, &heartbeatpb.MaintainerStatus{
		State:        heartbeatpb.ComponentState_Stopped,
		SessionEpoch: 0,
	})
	require.True(t, stopOp.finished.Load())
}

func TestController_StopChangefeedCarriesPublishedSessionEpochAfterMoveCutover(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, self, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend)
	target := node.NewInfo("localhost:8301", "")
	nodeManager.GetAliveNodes()[target.ID] = target

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	cf.SetCurrentMaintainerSessionEpoch(10)
	changefeedDB.AddReplicatingMaintainer(cf, self.ID)

	moveOp := NewMoveMaintainerOperator(changefeedDB, cf, self.ID, target.ID, 10, 20)
	require.True(t, oc.AddOperator(moveOp))

	moveOp.Check(self.ID, &heartbeatpb.MaintainerStatus{
		State:        heartbeatpb.ComponentState_Stopped,
		SessionEpoch: 10,
	})
	msg := moveOp.Schedule()
	require.NotNil(t, msg)

	op := oc.StopChangefeed(context.Background(), cfID, false)
	stopOp, ok := op.(*StopChangefeedOperator)
	require.True(t, ok)
	require.Equal(t, target.ID, stopOp.nodeID)
	require.Equal(t, uint64(20), stopOp.sessionEpoch)
	require.Equal(t, uint64(20), stopOp.Schedule().Message[0].(*heartbeatpb.RemoveMaintainerRequest).SessionEpoch)
}

func TestController_AddOperator(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, self, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend)
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

	require.True(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, target.ID, 1)))
	require.False(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, target.ID, 1)))
	cf2ID := common.NewChangeFeedIDWithName("test2", common.DefaultKeyspaceName)
	cf2 := changefeed.NewChangefeed(cf2ID, &config.ChangeFeedInfo{
		ChangefeedID: cf2ID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	require.False(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf2, target.ID, 1)))

	require.NotNil(t, oc.GetOperator(cfID))
	require.Nil(t, oc.GetOperator(cf2ID))

	require.True(t, oc.HasOperator(cfID.DisplayName))
	require.False(t, oc.HasOperator(cf2ID.DisplayName))
}

func TestController_HasOperatorInvolvingNode(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, self, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend)
	target := node.NewInfo("localhost:8301", "")
	nodeManager.GetAliveNodes()[target.ID] = target

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddReplicatingMaintainer(cf, self.ID)

	require.True(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, target.ID, 1)))

	require.True(t, oc.HasOperatorInvolvingNode(target.ID))
	require.False(t, oc.HasOperatorInvolvingNode("n3"))
}

func TestController_NewAddMaintainerOperatorUsesTimeoutContextForSessionEpoch(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	backend := mock_changefeed.NewMockBackend(ctrl)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self

	appcontext.SetService(appcontext.MessageCenter, messaging.NewMockMessageCenter())
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	oc := NewOperatorController(self, changefeedDB, backend, 10, &deadlineCheckingPDClient{})
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddAbsentChangefeed(cf)

	op, err := oc.NewAddMaintainerOperator(cf, node.ID("node-1"))
	require.NoError(t, err)
	require.NotNil(t, op)
}

func TestController_NewMoveMaintainerOperatorUsesTimeoutContextForSessionEpoch(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	backend := mock_changefeed.NewMockBackend(ctrl)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self

	appcontext.SetService(appcontext.MessageCenter, messaging.NewMockMessageCenter())
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	oc := NewOperatorController(self, changefeedDB, backend, 10, &deadlineCheckingPDClient{})
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	cf.SetCurrentMaintainerSessionEpoch(10)
	changefeedDB.AddReplicatingMaintainer(cf, self.ID)

	op, err := oc.NewMoveMaintainerOperator(cf, self.ID, node.ID("node-1"))
	require.NoError(t, err)
	require.NotNil(t, op)
}

func TestController_StopChangefeedDuringAddOperator(t *testing.T) {
	// Setup test environment
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	oc, _, nodeManager := newOperatorControllerForTest(t, changefeedDB, backend)
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
	addOp := NewAddMaintainerOperator(changefeedDB, cf, target.ID, 1)
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
		State:        heartbeatpb.ComponentState_Stopped,
		SessionEpoch: 1,
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
