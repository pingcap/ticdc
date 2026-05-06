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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestShouldPersistRuntimeStateIgnoresRunningErrorTime(t *testing.T) {
	errTime := time.Unix(1, 0)
	info := &config.ChangeFeedInfo{
		State: config.StateWarning,
		Error: &config.RunningError{
			Time:    errTime,
			Addr:    "127.0.0.1:8300",
			Code:    "CDC:ErrSinkURIInvalid",
			Message: "sink uri invalid",
		},
	}

	require.False(t, shouldPersistRuntimeState(nil, config.StateWarning, info.Error))
	require.False(t, shouldPersistRuntimeState(info, config.StateWarning, &config.RunningError{
		Time:    errTime.Add(time.Hour),
		Addr:    "127.0.0.1:8300",
		Code:    "CDC:ErrSinkURIInvalid",
		Message: "sink uri invalid",
	}))
	require.True(t, shouldPersistRuntimeState(info, config.StateWarning, &config.RunningError{
		Time:    errTime.Add(time.Hour),
		Addr:    "127.0.0.1:8300",
		Code:    "CDC:ErrSinkURIInvalid",
		Message: "another sink error",
	}))
	require.True(t, shouldPersistRuntimeState(info, config.StateFailed, info.Error))
	require.True(t, shouldPersistRuntimeState(info, config.StateWarning, nil))
}

func TestHandleStateChangeSkipsDuplicateRuntimeStatePersistence(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMockMessageCenter())
	appcontext.SetService(watcher.NodeManagerName, nodeManager)

	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
		operatorController: operator.NewOperatorController(
			self,
			changefeedDB,
			backend,
			10,
		),
	}
	co := &coordinator{
		backend:    backend,
		controller: controller,
	}

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	storedError := &config.RunningError{
		Time:    time.Unix(1, 0),
		Addr:    "127.0.0.1:8300",
		Code:    "CDC:ErrSinkURIInvalid",
		Message: "sink uri invalid",
	}
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateWarning,
		Error:        storedError,
		SinkURI:      "mysql://127.0.0.1:3306",
		Epoch:        233,
	}, 1, false)
	changefeedDB.AddReplicatingMaintainer(cf, self.ID)

	newError := &config.RunningError{
		Time:    storedError.Time.Add(time.Hour),
		Addr:    storedError.Addr,
		Code:    storedError.Code,
		Message: storedError.Message,
	}
	event := newChangefeedChange(cf, config.StateWarning, ChangeState, newError)
	require.NoError(t, co.handleStateChange(context.Background(), event))

	require.True(t, cf.GetInfo().Error.Time.Equal(storedError.Time))
	require.Equal(t, uint64(233), cf.GetInfo().Epoch)
	require.Equal(t, 0, controller.operatorController.OperatorSize())
	require.Equal(t, 1, changefeedDB.GetReplicatingSize())
}

func TestHandleStateChangeSkipsNilChangefeedInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
	}
	co := &coordinator{
		backend:    backend,
		controller: controller,
	}

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, false)
	changefeedDB.AddAbsentChangefeed(cf)
	cf.SetInfo(nil)

	event := newChangefeedChange(cf, config.StateWarning, ChangeState, &config.RunningError{
		Time:    time.Unix(1, 0),
		Addr:    "127.0.0.1:8300",
		Code:    "CDC:ErrSinkURIInvalid",
		Message: "sink uri invalid",
	})
	require.NoError(t, co.handleStateChange(context.Background(), event))
}

func TestHandleStateChangePersistsRuntimeStateWhenStateChanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	backend := mock_changefeed.NewMockBackend(ctrl)
	changefeedDB := changefeed.NewChangefeedDB(1216)
	controller := &Controller{
		backend:      backend,
		changefeedDB: changefeedDB,
	}
	co := &coordinator{
		backend:    backend,
		controller: controller,
	}

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	storedError := &config.RunningError{
		Time:    time.Unix(1, 0),
		Addr:    "127.0.0.1:8300",
		Code:    "CDC:ErrSinkURIInvalid",
		Message: "sink uri invalid",
	}
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateWarning,
		Error:        storedError,
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, false)
	changefeedDB.AddAbsentChangefeed(cf)

	backend.EXPECT().
		UpdateChangefeed(gomock.Any(), gomock.Any(), uint64(1), config.ProgressNone).
		DoAndReturn(func(_ context.Context, info *config.ChangeFeedInfo, _ uint64, _ config.Progress) error {
			require.Equal(t, config.StateNormal, info.State)
			require.Nil(t, info.Error)
			return nil
		}).
		Times(1)

	event := newChangefeedChange(cf, config.StateNormal, ChangeState, nil)
	require.NoError(t, co.handleStateChange(context.Background(), event))

	require.Equal(t, config.StateNormal, cf.GetInfo().State)
	require.Nil(t, cf.GetInfo().Error)
}
