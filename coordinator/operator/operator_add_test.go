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

// TestAddMaintainerOperator_OnNodeRemove verifies that removing a non-destination node
// does not affect scheduling, while removing the destination node cancels the operator
// and stops it from scheduling further commands.
func TestAddMaintainerOperator_OnNodeRemove(t *testing.T) {
	op := NewAddMaintainerOperator(nil, &changefeed.Changefeed{}, "n1", 1)
	op.OnNodeRemove("n2")
	require.Equal(t, None, op.canceled.Load())
	require.False(t, op.finished.Load())

	op.OnNodeRemove("n1")
	require.Equal(t, NodeRemoved, op.canceled.Load())
	require.True(t, op.finished.Load())

	require.Nil(t, op.Schedule())
}

// TestAddMaintainerOperator_OnTaskRemoved verifies that removing the changefeed task
// cancels the operator and prevents any further scheduling.
func TestAddMaintainerOperator_OnTaskRemoved(t *testing.T) {
	op := NewAddMaintainerOperator(nil, &changefeed.Changefeed{}, "n1", 1)

	op.OnTaskRemoved()
	require.Equal(t, TaskRemoved, op.canceled.Load())
	require.True(t, op.finished.Load())

	require.Nil(t, op.Schedule())
}

// TestAddMaintainerOperator_CheckRequiresBootstrapDone verifies that the operator only
// completes after it observes a Working status with BootstrapDone from the destination node.
func TestAddMaintainerOperator_CheckRequiresBootstrapDone(t *testing.T) {
	op := NewAddMaintainerOperator(nil, &changefeed.Changefeed{}, "n1", 1)

	op.Check("n1", &heartbeatpb.MaintainerStatus{
		State:         heartbeatpb.ComponentState_Working,
		BootstrapDone: false,
		SessionEpoch:  1,
	})
	require.False(t, op.finished.Load())

	op.Check("n1", &heartbeatpb.MaintainerStatus{
		State:         heartbeatpb.ComponentState_Working,
		BootstrapDone: true,
		SessionEpoch:  1,
	})
	require.True(t, op.finished.Load())
}

func TestAddMaintainerOperator_CheckIgnoresStaleSession(t *testing.T) {
	op := NewAddMaintainerOperator(nil, &changefeed.Changefeed{}, "n1", 42)

	op.Check("n1", &heartbeatpb.MaintainerStatus{
		State:         heartbeatpb.ComponentState_Working,
		BootstrapDone: true,
		SessionEpoch:  41,
	})
	require.False(t, op.finished.Load())

	op.Check("n1", &heartbeatpb.MaintainerStatus{
		State:         heartbeatpb.ComponentState_Working,
		BootstrapDone: true,
		SessionEpoch:  42,
	})
	require.True(t, op.finished.Load())
}

func TestAddMaintainerOperator_ScheduleCarriesSessionEpochAndPostFinishPublishesIt(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddAbsentChangefeed(cf)

	op := NewAddMaintainerOperator(changefeedDB, cf, "n1", 42)
	msg := op.Schedule()
	require.NotNil(t, msg)
	require.Equal(t, uint64(42), msg.Message[0].(*heartbeatpb.AddMaintainerRequest).SessionEpoch)

	op.Start()
	require.Equal(t, uint64(42), cf.GetCurrentMaintainerSessionEpoch())

	op.finished.Store(true)
	op.PostFinish()
	require.Equal(t, uint64(42), cf.GetCurrentMaintainerSessionEpoch())
}

func TestAddMaintainerOperator_StartPublishesOwnerSessionEpoch(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	}, 1, true)
	changefeedDB.AddAbsentChangefeed(cf)

	op := NewAddMaintainerOperator(changefeedDB, cf, "n1", 42)
	op.Start()

	require.Equal(t, node.ID("n1"), cf.GetNodeID())
	require.Equal(t, uint64(42), cf.GetCurrentMaintainerSessionEpoch())
}
