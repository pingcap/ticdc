// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package maintainer

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/writelease"
	"github.com/stretchr/testify/require"
)

func TestSetNodeLivenessRejectEpochMismatch(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var nodeLiveness liveness.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &nodeLiveness)
	m.coordinatorID = node.ID("coordinator")
	m.coordinatorVersion = 1

	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: m.node.nodeEpoch + 1,
	}
	msg := messaging.NewSingleTargetMessage(m.nodeInfo.ID, messaging.MaintainerManagerTopic, req)
	msg.From = m.coordinatorID

	m.onSetNodeLivenessRequest(msg)

	out := <-mc.GetMessageChannel()
	require.Equal(t, messaging.TypeSetNodeLivenessResponse, out.Type)
	resp := out.Message[0].(*heartbeatpb.SetNodeLivenessResponse)
	require.Equal(t, heartbeatpb.NodeLiveness_ALIVE, resp.Applied)
	require.Equal(t, m.node.nodeEpoch, resp.NodeEpoch)
	require.Equal(t, liveness.CaptureAlive, nodeLiveness.Load())
}

func TestSetNodeLivenessApplyTransition(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var nodeLiveness liveness.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &nodeLiveness)
	m.coordinatorID = node.ID("coordinator")
	m.coordinatorVersion = 1

	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: m.node.nodeEpoch,
	}
	msg := messaging.NewSingleTargetMessage(m.nodeInfo.ID, messaging.MaintainerManagerTopic, req)
	msg.From = m.coordinatorID

	m.onSetNodeLivenessRequest(msg)

	// Successful transition sends both a node heartbeat and a response.
	first := <-mc.GetMessageChannel()
	second := <-mc.GetMessageChannel()
	require.ElementsMatch(t,
		[]messaging.IOType{messaging.TypeNodeHeartbeatRequest, messaging.TypeSetNodeLivenessResponse},
		[]messaging.IOType{first.Type, second.Type},
	)
	require.Equal(t, liveness.CaptureDraining, nodeLiveness.Load())
}

func TestSetDispatcherDrainTargetApplyAndClear(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var nodeLiveness liveness.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &nodeLiveness)
	m.coordinatorID = node.ID("coordinator")

	msg := messaging.NewSingleTargetMessage(
		m.nodeInfo.ID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.SetDispatcherDrainTargetRequest{
			TargetNodeId: "n2",
			TargetEpoch:  1,
		},
	)
	msg.From = m.coordinatorID
	m.onSetDispatcherDrainTargetRequest(msg)
	target, epoch := m.getDispatcherDrainTarget()
	require.Equal(t, node.ID("n2"), target)
	require.Equal(t, uint64(1), epoch)

	msg = messaging.NewSingleTargetMessage(
		m.nodeInfo.ID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.SetDispatcherDrainTargetRequest{
			TargetNodeId: "",
			TargetEpoch:  1,
		},
	)
	msg.From = m.coordinatorID
	m.onSetDispatcherDrainTargetRequest(msg)
	target, epoch = m.getDispatcherDrainTarget()
	require.Equal(t, node.ID(""), target)
	require.Equal(t, uint64(1), epoch)
}

func TestSetDispatcherDrainTargetRejectStaleUpdate(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var nodeLiveness liveness.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &nodeLiveness)
	m.coordinatorID = node.ID("coordinator")

	apply := func(target string, epoch uint64) {
		msg := messaging.NewSingleTargetMessage(
			m.nodeInfo.ID,
			messaging.MaintainerManagerTopic,
			&heartbeatpb.SetDispatcherDrainTargetRequest{
				TargetNodeId: target,
				TargetEpoch:  epoch,
			},
		)
		msg.From = m.coordinatorID
		m.onSetDispatcherDrainTargetRequest(msg)
	}

	apply("n2", 1)
	apply("", 1)
	apply("n2", 1) // stale reactivation at the same epoch should be ignored.
	target, epoch := m.getDispatcherDrainTarget()
	require.Equal(t, node.ID(""), target)
	require.Equal(t, uint64(1), epoch)

	apply("n3", 0) // stale lower epoch.
	target, epoch = m.getDispatcherDrainTarget()
	require.Equal(t, node.ID(""), target)
	require.Equal(t, uint64(1), epoch)

	apply("n4", 2) // new epoch should be accepted.
	target, epoch = m.getDispatcherDrainTarget()
	require.Equal(t, node.ID("n4"), target)
	require.Equal(t, uint64(2), epoch)
}

func TestSetDispatcherDrainTargetSendsNodeHeartbeatAck(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var nodeLiveness liveness.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &nodeLiveness)
	m.coordinatorID = node.ID("coordinator")
	m.coordinatorVersion = 1

	apply := func(target string, epoch uint64) *heartbeatpb.NodeHeartbeat {
		msg := messaging.NewSingleTargetMessage(
			m.nodeInfo.ID,
			messaging.MaintainerManagerTopic,
			&heartbeatpb.SetDispatcherDrainTargetRequest{
				TargetNodeId: target,
				TargetEpoch:  epoch,
			},
		)
		msg.From = m.coordinatorID
		m.onSetDispatcherDrainTargetRequest(msg)

		out := <-mc.GetMessageChannel()
		require.Equal(t, messaging.TypeNodeHeartbeatRequest, out.Type)
		return out.Message[0].(*heartbeatpb.NodeHeartbeat)
	}

	hb := apply("n2", 1)
	require.Equal(t, "n2", hb.DispatcherDrainTargetNodeId)
	require.Equal(t, uint64(1), hb.DispatcherDrainTargetEpoch)

	hb = apply("", 1)
	require.Equal(t, "", hb.DispatcherDrainTargetNodeId)
	require.Equal(t, uint64(1), hb.DispatcherDrainTargetEpoch)

	// Same-epoch reactivation is rejected locally, but retries should still get
	// an immediate heartbeat that reflects the latest applied snapshot.
	hb = apply("n2", 1)
	require.Equal(t, "", hb.DispatcherDrainTargetNodeId)
	require.Equal(t, uint64(1), hb.DispatcherDrainTargetEpoch)
}

func TestCoordinatorBootstrapResponseIncludesDispatcherDrainTarget(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var nodeLiveness liveness.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &nodeLiveness)
	require.True(t, m.node.tryUpdateDispatcherDrainTarget(node.ID("n2"), 7))

	req := messaging.NewSingleTargetMessage(
		m.nodeInfo.ID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: 1},
	)
	req.From = node.ID("coordinator")
	m.onCoordinatorBootstrapRequest(req)

	out := <-mc.GetMessageChannel()
	require.Equal(t, messaging.TypeCoordinatorBootstrapResponse, out.Type)
	resp := out.Message[0].(*heartbeatpb.CoordinatorBootstrapResponse)
	require.Equal(t, "n2", resp.DispatcherDrainTargetNodeId)
	require.Equal(t, uint64(7), resp.DispatcherDrainTargetEpoch)
	require.Equal(t, heartbeatpb.CurrentWriteLeaseProtocolVersion, resp.WriteLeaseProtocolVersion)
}

func TestNodeHeartbeatResponseRenewsP2PWriteLease(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	gate := writelease.NewGate()
	appcontext.SetService(appcontext.CaptureWriteGate, gate)

	var nodeLiveness liveness.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &nodeLiveness)
	m.coordinatorID = node.ID("coordinator")
	m.coordinatorVersion = 10
	require.True(t, gate.RenewEtcd(time.Now(), writelease.EtcdProofDuration))

	m.sendNodeHeartbeat(true)
	heartbeatMessage := <-mc.GetMessageChannel()
	heartbeat := heartbeatMessage.Message[0].(*heartbeatpb.NodeHeartbeat)
	require.Equal(t, heartbeatpb.CurrentWriteLeaseProtocolVersion, heartbeat.WriteLeaseProtocolVersion)
	require.NotZero(t, heartbeat.WriteLeaseRequestSeq)

	responseMessage := messaging.NewSingleTargetMessage(
		m.nodeInfo.ID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.NodeHeartbeatResponse{
			CoordinatorVersion: 10,
			TargetNodeEpoch:    m.node.nodeEpoch,
			RequestSeq:         heartbeat.WriteLeaseRequestSeq,
			LeaseDurationMs:    uint64(writelease.P2PLeaseDuration.Milliseconds()),
		},
	)
	responseMessage.From = m.coordinatorID
	m.onNodeHeartbeatResponse(responseMessage)

	require.True(t, gate.IsWritable())

	// An old coordinator response cannot renew a new request.
	m.writeGate.InvalidateP2P()
	responseMessage.Message[0].(*heartbeatpb.NodeHeartbeatResponse).CoordinatorVersion = 9
	m.onNodeHeartbeatResponse(responseMessage)
	require.False(t, gate.IsWritable())

	// Replaying an already applied sequence from the current coordinator is also rejected.
	responseMessage.Message[0].(*heartbeatpb.NodeHeartbeatResponse).CoordinatorVersion = 10
	m.onNodeHeartbeatResponse(responseMessage)
	require.False(t, gate.IsWritable())
}

func TestNodeHeartbeatResponseEchoesWitnessChallenge(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(appcontext.CaptureWriteGate, writelease.NewGate())

	var nodeLiveness liveness.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &nodeLiveness)
	m.coordinatorID = node.ID("coordinator")
	m.coordinatorVersion = 10

	challenge := &heartbeatpb.WriteLeaseWitnessChallenge{
		CoordinatorVersion:   10,
		CoordinatorNodeEpoch: 11,
		SelfRequestSeq:       7,
		WitnessNodeEpoch:     m.node.nodeEpoch,
		Nonce:                []byte("nonce"),
	}
	message := messaging.NewSingleTargetMessage(
		m.nodeInfo.ID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.NodeHeartbeatResponse{
			CoordinatorVersion: 10,
			TargetNodeEpoch:    m.node.nodeEpoch,
			WitnessChallenge:   challenge,
		},
	)
	message.From = m.coordinatorID
	m.onNodeHeartbeatResponse(message)

	heartbeatMessage := <-mc.GetMessageChannel()
	heartbeat := heartbeatMessage.Message[0].(*heartbeatpb.NodeHeartbeat)
	ack := heartbeat.GetWriteLeaseWitnessAck()
	require.NotNil(t, ack)
	require.Equal(t, challenge.CoordinatorVersion, ack.CoordinatorVersion)
	require.Equal(t, challenge.CoordinatorNodeEpoch, ack.CoordinatorNodeEpoch)
	require.Equal(t, challenge.SelfRequestSeq, ack.SelfRequestSeq)
	require.Equal(t, challenge.WitnessNodeEpoch, ack.WitnessNodeEpoch)
	require.Equal(t, challenge.Nonce, ack.Nonce)
}

func TestAddMaintainerIgnoreInvalidConfig(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var nodeLiveness liveness.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &nodeLiveness)

	changefeedID := common.NewChangeFeedIDWithName("cf-invalid-config", common.DefaultKeyspaceName)
	status := m.onAddMaintainerRequest(&heartbeatpb.AddMaintainerRequest{
		Id:           changefeedID.ToPB(),
		Config:       []byte("not-json"),
		CheckpointTs: 10,
	})
	require.Nil(t, status)

	_, ok := m.GetMaintainerForChangefeed(changefeedID)
	require.False(t, ok)
}

func TestAddMaintainerIgnoreInvalidCheckpointTs(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var nodeLiveness liveness.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &nodeLiveness)

	changefeedID := common.NewChangeFeedIDWithName("cf-invalid-checkpoint", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: changefeedID,
		Config:       config.GetDefaultReplicaConfig(),
	}
	data, err := json.Marshal(info)
	require.NoError(t, err)

	status := m.onAddMaintainerRequest(&heartbeatpb.AddMaintainerRequest{
		Id:           changefeedID.ToPB(),
		Config:       data,
		CheckpointTs: 0,
	})
	require.Nil(t, status)

	_, ok := m.GetMaintainerForChangefeed(changefeedID)
	require.False(t, ok)
}
