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

package changefeed

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestNewChangefeed(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	checkpointTs := uint64(100)
	cf := NewChangefeed(cfID, info, checkpointTs, true)

	require.Equal(t, cfID, cf.ID)
	require.Equal(t, info, cf.GetInfo())
	require.Equal(t, checkpointTs, cf.GetLastSavedCheckPointTs())
	require.True(t, cf.NeedCheckpointTsMessage())
}

func TestNewChangefeedRejectsInvalidInfo(t *testing.T) {
	t.Parallel()

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	require.Panics(t, func() {
		NewChangefeed(cfID, nil, 100, true)
	})
	require.Panics(t, func() {
		NewChangefeed(cfID, &config.ChangeFeedInfo{
			SinkURI: "kafka://127.0.0.1:9092",
			State:   config.StateNormal,
		}, 100, true)
	})
}

func TestChangefeed_GetSetInfo(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100, true)

	newInfo := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9097",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf.SetInfo(newInfo)
	require.Equal(t, newInfo, cf.GetInfo())
	require.Panics(t, func() {
		cf.SetInfo(nil)
	})
}

func TestChangefeed_GetSetNodeID(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100, true)

	nodeID := node.ID("node-1")
	cf.SetNodeID(nodeID)
	require.Equal(t, nodeID, cf.GetNodeID())
}

func TestChangefeed_UpdateStatus(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100, true)

	newStatus := &heartbeatpb.MaintainerStatus{CheckpointTs: 200}
	updated, state, err := cf.UpdateStatus(newStatus)
	require.False(t, updated)
	require.Equal(t, config.StateNormal, state)
	require.Nil(t, err)
	require.Equal(t, newStatus, cf.GetStatus())
}

func TestChangefeed_UpdateStatusFastFailWhenBootstrapDoneChanges(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100, true)

	fastFailErr := &heartbeatpb.RunningError{
		Node:    "node-1",
		Code:    string(errors.ErrTableRouteConflict.RFCCode()),
		Message: "table route conflict",
	}
	newStatus := &heartbeatpb.MaintainerStatus{
		CheckpointTs:  200,
		BootstrapDone: true,
		Err:           []*heartbeatpb.RunningError{fastFailErr},
	}
	updated, state, err := cf.UpdateStatus(newStatus)

	require.True(t, updated)
	require.Equal(t, config.StateFailed, state)
	require.Same(t, fastFailErr, err)
	require.Equal(t, newStatus, cf.GetStatus())
	require.False(t, cf.ShouldRun())
}

func TestChangefeed_UpdateStatusRetryableErrorWhenBootstrapDoneChanges(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100, true)

	retryableErr := &heartbeatpb.RunningError{
		Node:    "node-1",
		Code:    "CDC:ErrChangefeedRetryable",
		Message: "retryable error",
	}
	newStatus := &heartbeatpb.MaintainerStatus{
		CheckpointTs:  100,
		BootstrapDone: true,
		Err:           []*heartbeatpb.RunningError{retryableErr},
	}
	updated, state, err := cf.UpdateStatus(newStatus)

	require.True(t, updated)
	require.Equal(t, config.StateNormal, state)
	require.Nil(t, err)
	require.Equal(t, newStatus, cf.GetStatus())
	require.True(t, cf.ShouldRun())
	require.False(t, cf.backoff.retrying.Load())
	require.False(t, cf.backoff.isRestarting.Load())
	require.True(t, cf.backoff.nextRetryTime.Load().IsZero())

	nextStatus := &heartbeatpb.MaintainerStatus{
		CheckpointTs:  100,
		BootstrapDone: true,
		Err:           []*heartbeatpb.RunningError{retryableErr},
	}
	updated, state, err = cf.UpdateStatus(nextStatus)

	require.True(t, updated)
	require.Equal(t, config.StateWarning, state)
	require.Same(t, retryableErr, err)
	require.Equal(t, nextStatus, cf.GetStatus())
	require.False(t, cf.ShouldRun())
	require.True(t, cf.backoff.retrying.Load())
	require.True(t, cf.backoff.isRestarting.Load())
}

func TestChangefeed_IsMQSink(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100, true)

	require.True(t, cf.NeedCheckpointTsMessage())
}

func TestChangefeed_NeedCheckpointMysqlActiveActive(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cfg := config.GetDefaultReplicaConfig()
	enable := true
	cfg.EnableActiveActive = &enable
	info := &config.ChangeFeedInfo{
		SinkURI: "mysql://127.0.0.1:4000/",
		State:   config.StateNormal,
		Config:  cfg,
	}
	cf := NewChangefeed(cfID, info, 100, true)

	require.True(t, cf.NeedCheckpointTsMessage())
}

func TestChangefeed_NeedCheckpointMysqlDisabled(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	cfg := config.GetDefaultReplicaConfig()
	disable := false
	cfg.EnableActiveActive = &disable
	info := &config.ChangeFeedInfo{
		SinkURI: "mysql://127.0.0.1:4000/",
		State:   config.StateNormal,
		Config:  cfg,
	}
	cf := NewChangefeed(cfID, info, 100, true)

	require.False(t, cf.NeedCheckpointTsMessage())
}

func TestChangefeed_GetSetLastSavedCheckPointTs(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100, true)

	newTs := uint64(200)
	cf.SetLastSavedCheckPointTs(newTs)
	require.Equal(t, newTs, cf.GetLastSavedCheckPointTs())
}

func TestChangefeed_NewAddMaintainerMessage(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
		Epoch:   7,
	}
	info.KeyspaceID = 123
	cf := NewChangefeed(cfID, info, 100, true)

	server := node.ID("server-1")
	msg := cf.NewAddMaintainerMessage(server)
	require.Equal(t, server, msg.To)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
	req := msg.Message[0].(*heartbeatpb.AddMaintainerRequest)
	require.Equal(t, info.KeyspaceID, req.KeyspaceId)
	require.Equal(t, info.Epoch, req.MaintainerEpoch)
	configInfo := &config.ChangeFeedInfo{}
	require.NoError(t, json.Unmarshal(req.Config, configInfo))
	require.Equal(t, info.Epoch, configInfo.Epoch)
	require.Equal(t, info.SinkURI, configInfo.SinkURI)
}

func TestChangefeed_NewCheckpointTsMessage(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100, true)

	ts := uint64(200)
	msg := cf.NewCheckpointTsMessage(ts)
	require.Equal(t, cf.nodeID, msg.To)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
}

func TestRemoveMaintainerMessage(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	server := node.ID("server-1")
	msg := RemoveMaintainerMessage(common.DefaultKeyspaceID, cfID, server, true, true, 10)
	require.Equal(t, server, msg.To)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
	req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
	require.Equal(t, uint64(10), req.MaintainerEpoch)
}

func TestChangefeedGetStatusForResume(t *testing.T) {
	// Prepare test data
	originalStatus := &heartbeatpb.MaintainerStatus{
		ChangefeedID: &heartbeatpb.ChangefeedID{
			High:     123,
			Low:      456,
			Name:     "test-changefeed",
			Keyspace: "test-keyspace",
		},
		CheckpointTs:    789,
		FeedState:       "normal",
		State:           heartbeatpb.ComponentState_Working,
		MaintainerEpoch: 42,
		Err: []*heartbeatpb.RunningError{
			{
				Time:    "2024-01-01 00:00:00",
				Node:    "test-node",
				Code:    "test-error",
				Message: "test error message",
			},
		},
	}

	// Create a Changefeed instance
	cf := &Changefeed{
		status: atomic.NewPointer(originalStatus),
	}

	// Get the cloned status
	clonedStatus := cf.GetStatusForResume()

	// Check if the cloned status is equal to the original status
	require.Equal(t, originalStatus.ChangefeedID.High, clonedStatus.ChangefeedID.High)
	require.Equal(t, originalStatus.ChangefeedID.Low, clonedStatus.ChangefeedID.Low)
	require.Equal(t, originalStatus.ChangefeedID.Name, clonedStatus.ChangefeedID.Name)
	require.Equal(t, originalStatus.ChangefeedID.Keyspace, clonedStatus.ChangefeedID.Keyspace)
	require.Equal(t, originalStatus.CheckpointTs, clonedStatus.CheckpointTs)
	require.Equal(t, originalStatus.FeedState, clonedStatus.FeedState)
	require.Equal(t, originalStatus.State, clonedStatus.State)
	require.Equal(t, originalStatus.MaintainerEpoch, clonedStatus.MaintainerEpoch)

	require.Equal(t, 0, len(clonedStatus.Err))
}

func TestChangefeed_GetKeyspaceID(t *testing.T) {
	var c1 *Changefeed
	require.Equal(t, uint32(0), c1.GetKeyspaceID())

	cfID := common.ChangeFeedID{
		Id: common.GID{
			Low:  1,
			High: 2,
		},
		DisplayName: common.ChangeFeedDisplayName{
			Name:     "hello",
			Keyspace: "ks1",
		},
	}

	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		KeyspaceID:   1,
	}
	c2 := &Changefeed{
		ID:   cfID,
		info: atomic.NewPointer(info),
	}
	require.Equal(t, uint32(1), c2.GetKeyspaceID())
}
