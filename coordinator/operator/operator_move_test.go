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
	"testing"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestMoveMaintainerOperator_OnNodeRemove(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2", 10, 20)
	op.OnNodeRemove("n2")

	require.True(t, op.bind)
	require.True(t, op.originNodeStopped)
	require.Equal(t, "n1", op.dest.String())
	require.Len(t, changefeedDB.GetByNodeID("n1"), 1)
	req := op.Schedule().Message[0].(*heartbeatpb.AddMaintainerRequest)
	require.NotNil(t, req)
	require.Equal(t, uint64(10), req.SessionEpoch)
	require.Len(t, changefeedDB.GetByNodeID("n1"), 1)

	op.OnNodeRemove("n1")
	require.Len(t, changefeedDB.GetByNodeID("n1"), 0)
	require.Equal(t, 1, changefeedDB.GetAbsentSize())
	require.True(t, op.canceled)
	require.Nil(t, op.Schedule())

	cf2ID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf2 := changefeed.NewChangefeed(cf2ID, &config.ChangeFeedInfo{
		ChangefeedID: cf2ID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf2, "n1")
	op2 := NewMoveMaintainerOperator(changefeedDB, cf2, "n1", "n2", 10, 20)
	op2.OnNodeRemove("n1")
	require.True(t, op2.originNodeStopped)
	op2.Schedule()
	require.True(t, op2.bind)
	require.Len(t, changefeedDB.GetByNodeID("n2"), 1)
}

func TestMoveMaintainerOperator_OnTaskRemoved(t *testing.T) {
	op := NewMoveMaintainerOperator(nil, &changefeed.Changefeed{}, "n1", "n2", 10, 20)
	op.OnTaskRemoved()
	require.True(t, op.canceled)
	require.Nil(t, op.Schedule())
	// backend is nil, but op is canceled , no nil pointer error
	op.PostFinish()
}

func TestMoveMaintainerOperator_CheckRequiresDestBootstrapDone(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2", 10, 20)

	op.Check("n1", &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped})
	require.True(t, op.originNodeStopped)
	require.False(t, op.finished)

	op.Check("n2", &heartbeatpb.MaintainerStatus{
		State:         heartbeatpb.ComponentState_Working,
		BootstrapDone: false,
	})
	require.False(t, op.finished)

	op.Check("n2", &heartbeatpb.MaintainerStatus{
		State:         heartbeatpb.ComponentState_Working,
		BootstrapDone: true,
	})
	require.True(t, op.finished)
	require.Nil(t, op.Schedule())
}

func TestMoveMaintainerOperator_ScheduleUsesOldAndNewSessions(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2", 10, 20)

	removeReq := op.Schedule().Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	require.Equal(t, uint64(10), removeReq.SessionEpoch)

	op.Check("n1", &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped})
	addReq := op.Schedule().Message[0].(*heartbeatpb.AddMaintainerRequest)
	require.Equal(t, uint64(20), addReq.SessionEpoch)

	op.finished = true
	op.PostFinish()
	require.Equal(t, uint64(20), cf.GetCurrentMaintainerSessionEpoch())
}

func TestMoveMaintainerOperator_CutoverPublishesDestOwnerSessionEpoch(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	cf.SetCurrentMaintainerSessionEpoch(10)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2", 10, 20)
	op.Check("n1", &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped})
	msg := op.Schedule()

	require.NotNil(t, msg)
	require.Equal(t, node.ID("n2"), cf.GetNodeID())
	require.Equal(t, uint64(20), cf.GetCurrentMaintainerSessionEpoch())
}
