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
	t.Run("dest removed before origin stopped keeps removing origin", func(t *testing.T) {
		changefeedDB, cf := newMoveMaintainerTestChangefeed(t, "dest-before-origin-stopped")
		op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2")

		op.OnNodeRemove("n2")
		require.Equal(t, moveMaintainerStateRemoveOrigin, op.state)
		require.Equal(t, node.ID("n1"), op.target)

		msg := op.Schedule()
		require.Equal(t, node.ID("n1"), msg.To)
		removeReq := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
		require.Equal(t, cf.GetInfo().Epoch, removeReq.MaintainerEpoch)
		require.Len(t, changefeedDB.GetByNodeID("n1"), 1)
	})

	t.Run("dest removed after origin stopped adds back to origin", func(t *testing.T) {
		changefeedDB, cf := newMoveMaintainerTestChangefeed(t, "dest-after-origin-stopped")
		op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2")

		op.Check("n1", &heartbeatpb.MaintainerStatus{
			State:           heartbeatpb.ComponentState_Stopped,
			MaintainerEpoch: cf.GetInfo().Epoch,
		})
		require.Equal(t, moveMaintainerStateOriginStopped, op.state)

		op.OnNodeRemove("n2")
		require.Equal(t, moveMaintainerStateOriginStopped, op.state)
		require.Equal(t, node.ID("n1"), op.target)

		msg := op.Schedule()
		require.Equal(t, node.ID("n1"), msg.To)
		require.NotNil(t, msg.Message[0].(*heartbeatpb.AddMaintainerRequest))
		require.Equal(t, moveMaintainerStateAddTarget, op.state)
		require.Len(t, changefeedDB.GetByNodeID("n1"), 1)
	})

	t.Run("dest removed after add starts marks absent", func(t *testing.T) {
		changefeedDB, cf := newMoveMaintainerTestChangefeed(t, "dest-after-add-starts")
		op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2")

		op.Check("n1", &heartbeatpb.MaintainerStatus{
			State:           heartbeatpb.ComponentState_Stopped,
			MaintainerEpoch: cf.GetInfo().Epoch,
		})
		require.NotNil(t, op.Schedule())
		require.Equal(t, moveMaintainerStateAddTarget, op.state)
		require.Equal(t, node.ID("n2"), cf.GetNodeID())

		op.OnNodeRemove("n2")
		require.True(t, op.IsFinished())
		require.Equal(t, 1, changefeedDB.GetAbsentSize())
		require.Len(t, changefeedDB.GetByNodeID("n2"), 0)
		require.Nil(t, op.Schedule())

		op.PostFinish()
		require.Equal(t, 1, changefeedDB.GetAbsentSize())
		require.Len(t, changefeedDB.GetByNodeID("n2"), 0)
	})

	t.Run("origin removed falls through to destination add", func(t *testing.T) {
		changefeedDB, cf := newMoveMaintainerTestChangefeed(t, "origin-removed")
		op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2")

		op.OnNodeRemove("n1")
		require.Equal(t, moveMaintainerStateOriginStopped, op.state)

		msg := op.Schedule()
		require.Equal(t, node.ID("n2"), msg.To)
		require.NotNil(t, msg.Message[0].(*heartbeatpb.AddMaintainerRequest))
		require.Equal(t, moveMaintainerStateAddTarget, op.state)
		require.Len(t, changefeedDB.GetByNodeID("n2"), 1)
	})

	t.Run("fallback origin removed marks absent", func(t *testing.T) {
		changefeedDB, cf := newMoveMaintainerTestChangefeed(t, "fallback-origin-removed")
		op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2")

		op.OnNodeRemove("n2")
		op.OnNodeRemove("n1")

		require.True(t, op.IsFinished())
		require.Equal(t, 1, changefeedDB.GetAbsentSize())
		require.Len(t, changefeedDB.GetByNodeID("n1"), 0)
		require.Nil(t, op.Schedule())
	})

	t.Run("origin removed before fallback target marks absent", func(t *testing.T) {
		changefeedDB, cf := newMoveMaintainerTestChangefeed(t, "origin-before-fallback-target")
		op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2")

		op.OnNodeRemove("n1")
		require.Equal(t, moveMaintainerStateOriginStopped, op.state)

		op.OnNodeRemove("n2")
		require.True(t, op.IsFinished())
		require.Equal(t, 1, changefeedDB.GetAbsentSize())
		require.Len(t, changefeedDB.GetByNodeID("n1"), 0)
		require.Len(t, changefeedDB.GetByNodeID("n2"), 0)
		require.Nil(t, op.Schedule())
	})
}

func newMoveMaintainerTestChangefeed(t *testing.T, name string) (*changefeed.ChangefeedDB, *changefeed.Changefeed) {
	t.Helper()

	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName(name, common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
		Epoch:        1,
	}, 1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")
	return changefeedDB, cf
}

func TestMoveMaintainerOperator_OnTaskRemoved(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	op := NewMoveMaintainerOperator(nil, cf, "n1", "n2")
	op.OnTaskRemoved()
	require.True(t, op.IsFinished())
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

	op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2")

	op.Check("n1", &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped})
	require.Equal(t, moveMaintainerStateOriginStopped, op.state)
	require.False(t, op.IsFinished())
	require.NotNil(t, op.Schedule())

	op.Check("n2", &heartbeatpb.MaintainerStatus{
		State:         heartbeatpb.ComponentState_Working,
		BootstrapDone: false,
	})
	require.False(t, op.IsFinished())

	op.Check("n2", &heartbeatpb.MaintainerStatus{
		State:         heartbeatpb.ComponentState_Working,
		BootstrapDone: true,
	})
	require.True(t, op.IsFinished())
	require.Nil(t, op.Schedule())
}

func TestMoveMaintainerOperator_CheckAcceptsCompatDestEpochDuringRollingUpgrade(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
		Epoch:        2,
	}, 1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2")
	op.Check("n1", &heartbeatpb.MaintainerStatus{
		State:           heartbeatpb.ComponentState_Stopped,
		MaintainerEpoch: 2,
	})
	require.NotNil(t, op.Schedule())
	status := &heartbeatpb.MaintainerStatus{
		State:           heartbeatpb.ComponentState_Working,
		BootstrapDone:   true,
		MaintainerEpoch: 0,
	}

	op.Check("n2", status)
	require.True(t, op.IsFinished())
}

func TestMoveMaintainerOperator_OnNodeRemoveAfterFinishMarksAbsent(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	op := NewMoveMaintainerOperator(changefeedDB, cf, "n1", "n2")
	op.Check("n1", &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped})
	require.NotNil(t, op.Schedule())
	require.Equal(t, node.ID("n2"), cf.GetNodeID())

	op.Check("n2", &heartbeatpb.MaintainerStatus{
		State:         heartbeatpb.ComponentState_Working,
		BootstrapDone: true,
	})
	require.True(t, op.IsFinished())

	op.OnNodeRemove("n2")
	require.True(t, op.IsFinished())
	require.Equal(t, 1, changefeedDB.GetAbsentSize())
	require.Len(t, changefeedDB.GetByNodeID("n2"), 0)

	op.PostFinish()
	require.Equal(t, 1, changefeedDB.GetAbsentSize())
	require.Len(t, changefeedDB.GetByNodeID("n2"), 0)
}
