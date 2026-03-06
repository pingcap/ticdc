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
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/api"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestSetNodeLivenessRejectEpochMismatch(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var liveness api.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &liveness)
	m.coordinatorID = node.ID("coordinator")
	m.coordinatorVersion = 1

	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: m.nodeEpoch + 1,
	}
	msg := messaging.NewSingleTargetMessage(m.nodeInfo.ID, messaging.MaintainerManagerTopic, req)
	msg.From = m.coordinatorID

	m.onSetNodeLivenessRequest(msg)

	out := <-mc.GetMessageChannel()
	require.Equal(t, messaging.TypeSetNodeLivenessResponse, out.Type)
	resp := out.Message[0].(*heartbeatpb.SetNodeLivenessResponse)
	require.Equal(t, heartbeatpb.NodeLiveness_ALIVE, resp.Applied)
	require.Equal(t, m.nodeEpoch, resp.NodeEpoch)
	require.Equal(t, api.LivenessCaptureAlive, liveness.Load())
}

func TestSetNodeLivenessApplyTransition(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var liveness api.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &liveness)
	m.coordinatorID = node.ID("coordinator")
	m.coordinatorVersion = 1

	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: m.nodeEpoch,
	}
	msg := messaging.NewSingleTargetMessage(m.nodeInfo.ID, messaging.MaintainerManagerTopic, req)
	msg.From = m.coordinatorID

	m.onSetNodeLivenessRequest(msg)

	// Successful transition sends both a node heartbeat and a response.
	first := <-mc.GetMessageChannel()
	second := <-mc.GetMessageChannel()

	require.ElementsMatch(t, []messaging.IOType{messaging.TypeNodeHeartbeatRequest, messaging.TypeSetNodeLivenessResponse},
		[]messaging.IOType{first.Type, second.Type})
	require.Equal(t, api.LivenessCaptureDraining, liveness.Load())
}

func TestSetDispatcherDrainTargetApplyAndClear(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var liveness api.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &liveness)
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

	var liveness api.Liveness
	m := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &liveness)
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
