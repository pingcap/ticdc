// Copyright 2026 PingCAP, Inc.
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

func TestNodeAgentSetNodeLivenessRejectEpochMismatch(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	self := node.NewInfo("127.0.0.1:0", "")
	var liveness api.Liveness
	m := NewMaintainerManager(self, &config.SchedulerConfig{}, &liveness)

	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: m.nodeEpoch + 1,
	}
	msg := messaging.NewSingleTargetMessage(self.ID, messaging.MaintainerManagerTopic, req)
	msg.From = node.ID("coordinator")

	m.onSetNodeLivenessRequest(msg)

	out := <-mc.GetMessageChannel()
	require.Equal(t, messaging.TypeSetNodeLivenessResponse, out.Type)

	resp := out.Message[0].(*heartbeatpb.SetNodeLivenessResponse)
	require.Equal(t, heartbeatpb.NodeLiveness_ALIVE, resp.Applied)
	require.Equal(t, m.nodeEpoch, resp.NodeEpoch)
	require.Equal(t, api.LivenessCaptureAlive, liveness.Load())
}

func TestNodeAgentSetNodeLivenessTransitionSendsHeartbeat(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	self := node.NewInfo("127.0.0.1:0", "")
	var liveness api.Liveness
	m := NewMaintainerManager(self, &config.SchedulerConfig{}, &liveness)

	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: m.nodeEpoch,
	}
	msg := messaging.NewSingleTargetMessage(self.ID, messaging.MaintainerManagerTopic, req)
	msg.From = node.ID("coordinator")

	m.onSetNodeLivenessRequest(msg)

	first := <-mc.GetMessageChannel()
	second := <-mc.GetMessageChannel()

	require.Equal(t, messaging.TypeNodeHeartbeatRequest, first.Type)
	hb := first.Message[0].(*heartbeatpb.NodeHeartbeat)
	require.Equal(t, heartbeatpb.NodeLiveness_DRAINING, hb.Liveness)
	require.Equal(t, m.nodeEpoch, hb.NodeEpoch)

	require.Equal(t, messaging.TypeSetNodeLivenessResponse, second.Type)
	resp := second.Message[0].(*heartbeatpb.SetNodeLivenessResponse)
	require.Equal(t, heartbeatpb.NodeLiveness_DRAINING, resp.Applied)
	require.Equal(t, m.nodeEpoch, resp.NodeEpoch)

	require.Equal(t, api.LivenessCaptureDraining, liveness.Load())
}
