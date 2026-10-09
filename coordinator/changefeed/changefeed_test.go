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
	"fmt"
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

func TestChangefeedUpdateStatusProcessesErrorsWhenCheckpointRegresses(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 200, true)

	regressedStatus := &heartbeatpb.MaintainerStatus{CheckpointTs: 150}
	updated, state, err := cf.UpdateStatus(regressedStatus)
	require.False(t, updated)
	require.Equal(t, config.StateNormal, state)
	require.Nil(t, err)
	require.Equal(t, uint64(200), cf.GetStatus().CheckpointTs)

	retryableErr := &heartbeatpb.RunningError{
		Node:    "node-1",
		Code:    "CDC:ErrChangefeedRetryable",
		Message: "retryable error",
	}
	regressedStatus = &heartbeatpb.MaintainerStatus{
		CheckpointTs: 150,
		Err:          []*heartbeatpb.RunningError{retryableErr},
	}
	updated, state, err = cf.UpdateStatus(regressedStatus)
	require.True(t, updated)
	require.Equal(t, config.StateWarning, state)
	require.Same(t, retryableErr, err)
	require.Equal(t, uint64(150), regressedStatus.CheckpointTs)
	status := cf.GetStatus()
	require.Equal(t, uint64(200), status.CheckpointTs)
	require.Len(t, status.Err, 1)
	require.Same(t, retryableErr, status.Err[0])
	require.False(t, cf.ShouldRun())
	require.True(t, cf.backoff.retrying.Load())
	require.True(t, cf.backoff.isRestarting.Load())
}

func TestChangefeedUpdateStatusFastFailWithLowerCheckpoint(t *testing.T) {
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
		CheckpointTs:  99,
		BootstrapDone: true,
		Err:           []*heartbeatpb.RunningError{fastFailErr},
	}
	updated, state, err := cf.UpdateStatus(newStatus)

	require.True(t, updated)
	require.Equal(t, config.StateFailed, state)
	require.Same(t, fastFailErr, err)
	require.Equal(t, uint64(100), cf.GetStatus().CheckpointTs)
	require.Equal(t, uint64(100), cf.backoff.checkpointTs)
	require.Equal(t, uint64(99), newStatus.CheckpointTs)
	require.Same(t, fastFailErr, cf.GetStatus().Err[0])
	require.False(t, cf.ShouldRun())
}

func TestBootstrapDoneFastFail(t *testing.T) {
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

func TestBootstrapDoneRetryableError(t *testing.T) {
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

func TestBootstrapDoneProgressDoesNotSkipRetry(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100, true)

	// A lower checkpoint heartbeat must not replace the accepted status.
	updated, state, err := cf.UpdateStatus(&heartbeatpb.MaintainerStatus{CheckpointTs: 90})
	require.False(t, updated)
	require.Equal(t, config.StateNormal, state)
	require.Nil(t, err)
	require.Equal(t, uint64(100), cf.GetStatus().CheckpointTs)

	bootstrapStatus := &heartbeatpb.MaintainerStatus{CheckpointTs: 200, BootstrapDone: true}
	updated, state, err = cf.UpdateStatus(bootstrapStatus)
	require.True(t, updated)
	require.Equal(t, config.StateNormal, state)
	require.Nil(t, err)
	require.Same(t, bootstrapStatus, cf.GetStatus())
	require.False(t, cf.backoff.retrying.Load())
	require.True(t, cf.backoff.nextRetryTime.Load().IsZero())

	// Bootstrap progress has already been accepted, so this error must trigger retry.
	retryableErr := &heartbeatpb.RunningError{
		Node: "node-1", Code: "CDC:ErrChangefeedRetryable", Message: "retryable error",
	}
	updated, state, err = cf.UpdateStatus(&heartbeatpb.MaintainerStatus{
		CheckpointTs: 200, BootstrapDone: true, Err: []*heartbeatpb.RunningError{retryableErr},
	})
	require.True(t, updated)
	require.Equal(t, config.StateWarning, state)
	require.Same(t, retryableErr, err)
	require.Equal(t, uint64(200), cf.backoff.checkpointTs)
	require.True(t, cf.backoff.retrying.Load())
	require.True(t, cf.backoff.isRestarting.Load())
	require.False(t, cf.ShouldRun())
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
	}
	cf := NewChangefeed(cfID, info, 100, true)

	server := node.ID("server-1")
	msg := cf.NewAddMaintainerMessage(server)
	require.Equal(t, server, msg.To)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
}

func TestChangefeed_NewRemoveMaintainerMessage(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100, true)

	server := node.ID("server-1")
	msg := cf.NewRemoveMaintainerMessage(server, true, true)
	require.Equal(t, server, msg.To)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
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
	msg := RemoveMaintainerMessage(common.DefaultKeyspaceID, cfID, server, true, true)
	require.Equal(t, server, msg.To)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
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
		CheckpointTs: 789,
		FeedState:    "normal",
		State:        heartbeatpb.ComponentState_Working,
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

	require.Equal(t, 0, len(clonedStatus.Err))
}

func TestChangefeed_GetKeyspaceID(t *testing.T) {
	var c1 *Changefeed
	require.Equal(t, common.DefaultKeyspaceID, c1.GetKeyspaceID())

	c2 := &Changefeed{}
	require.Equal(t, common.DefaultKeyspaceID, c2.GetKeyspaceID())

	var nilInfo *config.ChangeFeedInfo
	c3 := &Changefeed{info: atomic.NewPointer(nilInfo)}
	require.Equal(t, common.DefaultKeyspaceID, c3.GetKeyspaceID())

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
	c4 := &Changefeed{
		ID:   cfID,
		info: atomic.NewPointer(info),
	}
	require.Equal(t, uint32(1), c4.GetKeyspaceID())
}

func TestBootstrapDoneDoesNotRegress(t *testing.T) {
	for _, targetTs := range []uint64{0, 200} {
		t.Run(fmt.Sprintf("target %d", targetTs), func(t *testing.T) {
			info := &config.ChangeFeedInfo{
				SinkURI: "mysql://localhost:3306", State: config.StateNormal,
				Config: config.GetDefaultReplicaConfig(), TargetTs: targetTs,
			}
			cf := NewChangefeed(common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName), info, 200, true)
			cf.UpdateStatus(&heartbeatpb.MaintainerStatus{CheckpointTs: 200, BootstrapDone: true})
			incoming := &heartbeatpb.MaintainerStatus{CheckpointTs: 200}
			if targetTs == 0 {
				incoming.Err = []*heartbeatpb.RunningError{{Code: "CDC:ErrChangefeedRetryable", Message: "retry"}}
			}
			changed, state, err := cf.UpdateStatus(incoming)
			require.True(t, changed)
			require.True(t, cf.GetStatus().BootstrapDone)
			require.False(t, incoming.BootstrapDone)
			if targetTs == 0 {
				require.Equal(t, config.StateWarning, state)
				require.Same(t, incoming.Err[0], err)
				require.False(t, cf.ShouldRun())
			} else {
				require.Equal(t, config.StateFinished, state)
				require.Nil(t, err)
			}
		})
	}
}
