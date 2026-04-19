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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/coordinator/gccleaner"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
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

	changefeedDB.AddAbsentChangefeed(changefeed.NewChangefeed(cfID, info, info.StartTs, true))

	require.NoError(t, co.updateGCSafepoint(context.Background()))

	require.Equal(t, 1, changefeedDB.GetAbsentSize())
	require.Equal(t, 0, changefeedDB.GetStoppedSize())
	cf := changefeedDB.GetByID(cfID)
	require.NotNil(t, cf)
	require.Equal(t, config.StateNormal, cf.GetInfo().State)
	require.Nil(t, cf.GetInfo().Error)
}

func TestUpdateGCSafepointDeletesServiceSafepointWhenNoChangefeed(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("classic mode only")
	}

	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	gcManager := gc.NewMockManager(ctrl)

	co, _ := newTestCoordinatorWithGCManager(t, backend, gcManager)

	gcManager.EXPECT().
		TryDeleteServiceGCSafepoint(gomock.Any()).
		Return(nil).
		Times(1)
	gcManager.EXPECT().
		TryUpdateServiceGCSafepoint(gomock.Any(), gomock.Any()).
		Times(0)

	require.NoError(t, co.updateGCSafepoint(context.Background()))
}

func TestRemoveLastChangefeedDeletesServiceSafepointImmediately(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("classic mode only")
	}

	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	gcManager := gc.NewMockManager(ctrl)

	co, changefeedDB := newTestCoordinatorWithGCManager(t, backend, gcManager)

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 101, true)
	changefeedDB.AddReplicatingMaintainer(cf, "node1")

	backend.EXPECT().
		SetChangefeedProgress(gomock.Any(), cfID, config.ProgressRemoving).
		Return(nil).
		Times(1)
	gcManager.EXPECT().
		TryDeleteServiceGCSafepoint(gomock.Any()).
		Return(nil).
		Times(1)

	cpCh := make(chan uint64, 1)
	errCh := make(chan error, 1)
	go func() {
		cp, err := co.RemoveChangefeed(context.Background(), cfID)
		cpCh <- cp
		errCh <- err
	}()

	var op interface{ OnTaskRemoved() }
	require.Eventually(t, func() bool {
		op = co.controller.operatorController.GetOperator(cfID)
		return op != nil
	}, 5*time.Second, 10*time.Millisecond)
	op.OnTaskRemoved()

	require.NoError(t, <-errCh)
	require.Equal(t, uint64(101), <-cpCh)
}

func TestConcurrentDeleteLastChangefeedAndCreateNewOneKeepsExpectedGCSafepoint(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("classic mode only")
	}

	for i := 0; i < 5; i++ {
		ctrl := gomock.NewController(t)
		backend := mock_changefeed.NewMockBackend(ctrl)
		gcManager := gc.NewMockManager(ctrl)

		co, changefeedDB := newTestCoordinatorWithGCManager(t, backend, gcManager)

		oldID := common.NewChangeFeedIDWithName("old", common.DefaultKeyspaceName)
		oldCF := changefeed.NewChangefeed(oldID, &config.ChangeFeedInfo{
			ChangefeedID: oldID,
			Config:       config.GetDefaultReplicaConfig(),
			State:        config.StateNormal,
			SinkURI:      "mysql://127.0.0.1:3306",
		}, 101, true)
		changefeedDB.AddReplicatingMaintainer(oldCF, "node1")

		newID := common.NewChangeFeedIDWithName("new", common.DefaultKeyspaceName)
		newInfo := &config.ChangeFeedInfo{
			ChangefeedID: newID,
			StartTs:      205,
			State:        config.StateNormal,
			Config:       config.GetDefaultReplicaConfig(),
			SinkURI:      "kafka://127.0.0.1:9092",
			KeyspaceID:   1,
		}

		backend.EXPECT().
			SetChangefeedProgress(gomock.Any(), oldID, config.ProgressRemoving).
			Return(nil).
			Times(1)
		backend.EXPECT().
			CreateChangefeed(gomock.Any(), gomock.Any()).
			Return(nil).
			Times(1)
		gcManager.EXPECT().
			TryDeleteServiceGCSafepoint(gomock.Any()).
			Times(0)
		gcManager.EXPECT().
			TryUpdateServiceGCSafepoint(gomock.Any(), common.Ts(newInfo.StartTs-1)).
			Return(nil).
			Times(1)

		cpCh := make(chan uint64, 1)
		errCh := make(chan error, 1)
		go func() {
			cp, err := co.RemoveChangefeed(context.Background(), oldID)
			cpCh <- cp
			errCh <- err
		}()

		var op interface{ OnTaskRemoved() }
		require.Eventually(t, func() bool {
			op = co.controller.operatorController.GetOperator(oldID)
			return op != nil
		}, 5*time.Second, 10*time.Millisecond)

		require.NoError(t, co.CreateChangefeed(context.Background(), newInfo))
		op.OnTaskRemoved()

		require.NoError(t, <-errCh)
		require.Equalf(t, uint64(101), <-cpCh, "iteration %d", i)

		require.NoError(t, co.updateGCSafepoint(context.Background()))
	}
}
