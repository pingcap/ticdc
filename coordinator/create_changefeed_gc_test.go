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
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/coordinator/gccleaner"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func newTestCoordinatorWithGCManager(
	t *testing.T,
	backend *mock_changefeed.MockBackend,
	gcManager gc.Manager,
) (*coordinator, *changefeed.ChangefeedDB) {
	t.Helper()

	mc := messaging.NewMockMessageCenter()
	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	self := node.NewInfo("node1", "")
	nodeManager.GetAliveNodes()[self.ID] = self

	changefeedDB := changefeed.NewChangefeedDB(1)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(
			self,
			changefeedDB,
			backend,
			10,
		),
		initialized: atomic.NewBool(true),
		nodeManager: nodeManager,
	}

	co := &coordinator{
		controller: controller,
		backend:    backend,
		gcManager:  gcManager,
		pdClock:    pdutil.NewClock4Test(),

		gcCleaner: gccleaner.New(nil, "test-gc-service"),
	}
	return co, changefeedDB
}

func TestCreateChangefeedDoesNotUpdateGCSafepoint(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	gcManager := gc.NewMockManager(ctrl)

	co, changefeedDB := newTestCoordinatorWithGCManager(t, backend, gcManager)

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		StartTs:      100,
		State:        config.StateNormal,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "kafka://127.0.0.1:9092",
		KeyspaceID:   1,
	}

	backend.EXPECT().CreateChangefeed(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	if kerneltype.IsClassic() {
		gcManager.EXPECT().
			TryUpdateServiceGCSafepoint(gomock.Any(), gomock.Any()).
			Times(0)
	} else {
		gcManager.EXPECT().
			TryUpdateKeyspaceGCBarrier(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Times(0)
	}

	require.NoError(t, co.CreateChangefeed(context.Background(), info))
	require.Equal(t, 1, changefeedDB.GetAbsentSize())
	require.Equal(t, 0, changefeedDB.GetStoppedSize())
	require.Equal(t, 1, co.gcCleaner.PendingLen())
}

func TestUpdateGCSafepointCallsGCManagerUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	gcManager := gc.NewMockManager(ctrl)

	co, changefeedDB := newTestCoordinatorWithGCManager(t, backend, gcManager)

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		StartTs:      100,
		State:        config.StateNormal,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "kafka://127.0.0.1:9092",
		KeyspaceID:   1,
	}

	if kerneltype.IsClassic() {
		gcManager.EXPECT().
			TryUpdateServiceGCSafepoint(gomock.Any(), common.Ts(info.StartTs-1)).
			Return(nil).Times(1)
	} else {
		gcManager.EXPECT().
			TryUpdateKeyspaceGCBarrier(gomock.Any(), gomock.Any(), gomock.Any(), common.Ts(info.StartTs-1)).
			Return(nil).Times(1)
	}
	gcManager.EXPECT().
		CheckStaleCheckpointTs(info.KeyspaceID, cfID, info.StartTs).
		Return(nil).Times(1)

	changefeedDB.AddAbsentChangefeed(changefeed.NewChangefeed(cfID, info, info.StartTs, true))

	require.NoError(t, co.updateGCSafepoint(context.Background()))

	require.Equal(t, 1, changefeedDB.GetAbsentSize())
	require.Equal(t, 0, changefeedDB.GetStoppedSize())
	cf := changefeedDB.GetByID(cfID)
	require.NotNil(t, cf)
	require.Equal(t, config.StateNormal, cf.GetInfo().State)
	require.Nil(t, cf.GetInfo().Error)
}

func TestUpdateGCSafepointChecksFailedChangefeedGCTTL(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	gcManager := gc.NewMockManager(ctrl)

	co, changefeedDB := newTestCoordinatorWithGCManager(t, backend, gcManager)
	co.changefeedChangeCh = make(chan []*changefeedChange, 1)

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	checkpointTs := common.Ts(100)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		State:        config.StateFailed,
		Error: &config.RunningError{
			Code: string(errors.ErrToTLSConfigFailed.ID()),
		},
		Config:     config.GetDefaultReplicaConfig(),
		SinkURI:    "kafka://127.0.0.1:9092",
		KeyspaceID: 1,
	}
	changefeedDB.AddStoppedChangefeed(changefeed.NewChangefeed(cfID, info, checkpointTs, false))

	if kerneltype.IsClassic() {
		gcManager.EXPECT().
			TryUpdateServiceGCSafepoint(gomock.Any(), checkpointTs-1).
			Return(nil).Times(1)
	} else {
		gcManager.EXPECT().
			TryUpdateKeyspaceGCBarrier(gomock.Any(), info.KeyspaceID, cfID.Keyspace(), checkpointTs-1).
			Return(nil).Times(1)
	}
	ttlErr := errors.ErrGCTTLExceeded.GenWithStackByArgs(checkpointTs, cfID)
	gcManager.EXPECT().
		CheckStaleCheckpointTs(info.KeyspaceID, cfID, checkpointTs).
		Return(ttlErr).Times(1)

	require.NoError(t, co.updateGCSafepoint(context.Background()))

	select {
	case changes := <-co.changefeedChangeCh:
		require.Len(t, changes, 1)
		require.Equal(t, config.StateFailed, changes[0].state)
		require.Equal(t, ChangeState, changes[0].changeType)
		require.Equal(t, string(errors.ErrGCTTLExceeded.ID()), changes[0].err.Code)
	default:
		require.FailNow(t, "expected gc ttl state change")
	}
}
