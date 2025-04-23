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
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestNewChangefeed(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
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
	cfID := common.NewChangeFeedIDWithName("test")
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
	cfID := common.NewChangeFeedIDWithName("test")
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
	cfID := common.NewChangeFeedIDWithName("test")
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

func TestChangefeed_IsMQSink(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	info := &config.ChangeFeedInfo{
		SinkURI: "kafka://127.0.0.1:9092",
		State:   config.StateNormal,
		Config:  config.GetDefaultReplicaConfig(),
	}
	cf := NewChangefeed(cfID, info, 100, true)

	require.True(t, cf.NeedCheckpointTsMessage())
}

func TestChangefeed_GetSetLastSavedCheckPointTs(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
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
	cfID := common.NewChangeFeedIDWithName("test")
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
	cfID := common.NewChangeFeedIDWithName("test")
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
	cfID := common.NewChangeFeedIDWithName("test")
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
	cfID := common.NewChangeFeedIDWithName("test")
	server := node.ID("server-1")
	msg := RemoveMaintainerMessage(cfID, server, true, true)
	require.Equal(t, server, msg.To)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
}

func TestChangefeedGetCloneStatus(t *testing.T) {
	// Prepare test data
	originalStatus := &heartbeatpb.MaintainerStatus{
		ChangefeedID: &heartbeatpb.ChangefeedID{
			High:      123,
			Low:       456,
			Name:      "test-changefeed",
			Namespace: "test-namespace",
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
	clonedStatus := cf.GetClonedStatus()

	// Check if the cloned status is equal to the original status
	require.Equal(t, originalStatus.ChangefeedID.High, clonedStatus.ChangefeedID.High)
	require.Equal(t, originalStatus.ChangefeedID.Low, clonedStatus.ChangefeedID.Low)
	require.Equal(t, originalStatus.ChangefeedID.Name, clonedStatus.ChangefeedID.Name)
	require.Equal(t, originalStatus.ChangefeedID.Namespace, clonedStatus.ChangefeedID.Namespace)
	require.Equal(t, originalStatus.CheckpointTs, clonedStatus.CheckpointTs)
	require.Equal(t, originalStatus.FeedState, clonedStatus.FeedState)
	require.Equal(t, originalStatus.State, clonedStatus.State)
	require.Equal(t, len(originalStatus.Err), len(clonedStatus.Err))

	// Check if the error array elements are the same
	if len(originalStatus.Err) > 0 {
		require.Equal(t, originalStatus.Err[0].Time, clonedStatus.Err[0].Time)
		require.Equal(t, originalStatus.Err[0].Node, clonedStatus.Err[0].Node)
		require.Equal(t, originalStatus.Err[0].Code, clonedStatus.Err[0].Code)
		require.Equal(t, originalStatus.Err[0].Message, clonedStatus.Err[0].Message)
	}

	// Check if the cloned status is a new object
	require.NotSame(t, originalStatus, clonedStatus)
	require.NotSame(t, originalStatus.ChangefeedID, clonedStatus.ChangefeedID)
	if len(originalStatus.Err) > 0 {
		require.NotSame(t, originalStatus.Err[0], clonedStatus.Err[0])
	}
}
