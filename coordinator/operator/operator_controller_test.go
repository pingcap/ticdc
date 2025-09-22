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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestController_StopChangefeed(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	oc := NewOperatorController(nil, node.NewInfo("localhost:8300", ""), changefeedDB,
		backend, nodeManager, 10)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	oc.StopChangefeed(context.Background(), cfID, false)
	require.Len(t, oc.operators, 1)
	// the old  PostFinish will be called
	backend.EXPECT().SetChangefeedProgress(gomock.Any(), gomock.Any(), config.ProgressNone).Return(nil).Times(1)
	oc.StopChangefeed(context.Background(), cfID, true)
	require.Len(t, oc.operators, 1)
	oc.StopChangefeed(context.Background(), cfID, true)
	require.Len(t, oc.operators, 1)
}

func TestController_AddOperator(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	oc := NewOperatorController(nil, node.NewInfo("localhost:8300", ""), changefeedDB,
		backend, nodeManager, 10)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	require.True(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, "n2")))
	require.False(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf, "n2")))
	cf2ID := common.NewChangeFeedIDWithName("test2", common.DefaultKeyspace)
	cf2 := changefeed.NewChangefeed(cf2ID, &config.ChangeFeedInfo{
		ChangefeedID: cf2ID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	require.False(t, oc.AddOperator(NewAddMaintainerOperator(changefeedDB, cf2, "n2")))

	require.NotNil(t, oc.GetOperator(cfID))
	require.Nil(t, oc.GetOperator(cf2ID))

	require.True(t, oc.HasOperator(cfID.DisplayName))
	require.False(t, oc.HasOperator(cf2ID.DisplayName))
}

func TestController_StopChangefeedDuringAddOperator(t *testing.T) {
	// Setup test environment
	changefeedDB := changefeed.NewChangefeedDB(1216)
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	self := node.NewInfo("localhost:8300", "")
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()[self.ID] = self
	oc := NewOperatorController(nil, self, changefeedDB, backend, nodeManager, 10)

	// Create changefeed and add it to absent state (simulating a newly created changefeed)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
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
	addOp := NewAddMaintainerOperator(changefeedDB, cf, "n1")
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
	stopOp.Check("n1", &heartbeatpb.MaintainerStatus{
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
