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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestStopChangefeedOperator_OnNodeRemove(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	op := NewStopChangefeedOperator(common.DefaultKeyspaceID, cfID, "n1", "n2", backend, true, 0)
	op.OnNodeRemove("n1")
	require.Equal(t, "n2", op.nodeID.String())
	require.False(t, op.finished.Load())
}

func TestStopChangefeedOperator_OnTaskRemoved(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")
	op := NewStopChangefeedOperator(common.DefaultKeyspaceID, cfID, "n1", "n2", nil, true, 0)
	op.OnTaskRemoved()
	require.True(t, op.finished.Load())
}

func TestStopChangefeedOperator_PostFinish(t *testing.T) {
	changefeedDB := changefeed.NewChangefeedDB(1216)
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cf := changefeed.NewChangefeed(cfID, &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "mysql://127.0.0.1:3306",
	},
		1, true)
	changefeedDB.AddReplicatingMaintainer(cf, "n1")

	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	op := NewStopChangefeedOperator(common.DefaultKeyspaceID, cfID, "n1", "n2", backend, true, 0)
	backend.EXPECT().DeleteChangefeed(gomock.Any(), cfID).Return(errors.New("err"))
	op.PostFinish()

	op2 := NewStopChangefeedOperator(common.DefaultKeyspaceID, cfID, "n1", "n2", backend, false, 0)
	backend.EXPECT().SetChangefeedProgress(gomock.Any(), cfID, config.ProgressNone).Return(errors.New("err"))
	op2.PostFinish()
}

func TestStopChangefeedOperator_ScheduleMaintainerEpoch(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	op := NewStopChangefeedOperator(common.DefaultKeyspaceID, cfID, "n1", "n2", nil, true, 9)

	req := op.Schedule().Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	require.Equal(t, uint64(9), req.MaintainerEpoch)
}

func TestStopChangefeedOperator_CheckRequiresTargetNodeAndEpoch(t *testing.T) {
	t.Parallel()

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	tests := []struct {
		name          string
		from          node.ID
		operatorEpoch uint64
		status        *heartbeatpb.MaintainerStatus
		finished      bool
	}{
		{
			name:          "ignore status from other node",
			from:          "n2",
			operatorEpoch: 7,
			status:        &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped, MaintainerEpoch: 7},
		},
		{
			name:          "ignore working status",
			from:          "n1",
			operatorEpoch: 7,
			status:        &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Working, MaintainerEpoch: 7},
		},
		{
			name:          "ignore newer maintainer status",
			from:          "n1",
			operatorEpoch: 7,
			status:        &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped, MaintainerEpoch: 8},
		},
		{
			name:          "accept same epoch stopped status",
			from:          "n1",
			operatorEpoch: 7,
			status:        &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped, MaintainerEpoch: 7},
			finished:      true,
		},
		{
			name:          "accept older maintainer stopped status",
			from:          "n1",
			operatorEpoch: 7,
			status:        &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped, MaintainerEpoch: 6},
			finished:      true,
		},
		{
			name:          "accept unfenced maintainer stopped status",
			from:          "n1",
			operatorEpoch: 7,
			status:        &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped},
			finished:      true,
		},
		{
			name:          "unfenced operator keeps old compatibility",
			from:          "n1",
			operatorEpoch: 0,
			status:        &heartbeatpb.MaintainerStatus{State: heartbeatpb.ComponentState_Stopped, MaintainerEpoch: 8},
			finished:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := NewStopChangefeedOperator(common.DefaultKeyspaceID, cfID, "n1", "n2", nil, true, tt.operatorEpoch)
			op.Check(tt.from, tt.status)
			require.Equal(t, tt.finished, op.IsFinished())
		})
	}
}
