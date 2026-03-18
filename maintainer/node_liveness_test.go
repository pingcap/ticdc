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

func TestSetNodeLivenessRejectEpochMismatch(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var liveness api.Liveness
	manager := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &liveness)
	manager.coordinatorID = node.ID("coordinator")
	manager.coordinatorVersion = 1

	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: manager.node.nodeEpoch + 1,
	}
	msg := messaging.NewSingleTargetMessage(manager.nodeInfo.ID, messaging.MaintainerManagerTopic, req)
	msg.From = manager.coordinatorID

	manager.onSetNodeLivenessRequest(msg)

	out := <-mc.GetMessageChannel()
	require.Equal(t, messaging.TypeSetNodeLivenessResponse, out.Type)
	resp := out.Message[0].(*heartbeatpb.SetNodeLivenessResponse)
	require.Equal(t, heartbeatpb.NodeLiveness_ALIVE, resp.Applied)
	require.Equal(t, manager.node.nodeEpoch, resp.NodeEpoch)
	require.Equal(t, api.LivenessCaptureAlive, liveness.Load())
}

func TestSetNodeLivenessApplyTransition(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var liveness api.Liveness
	manager := NewMaintainerManager(&node.Info{ID: node.ID("n1")}, &config.SchedulerConfig{}, &liveness)
	manager.coordinatorID = node.ID("coordinator")
	manager.coordinatorVersion = 1

	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: manager.node.nodeEpoch,
	}
	msg := messaging.NewSingleTargetMessage(manager.nodeInfo.ID, messaging.MaintainerManagerTopic, req)
	msg.From = manager.coordinatorID

	manager.onSetNodeLivenessRequest(msg)

	first := <-mc.GetMessageChannel()
	second := <-mc.GetMessageChannel()
	require.ElementsMatch(t,
		[]messaging.IOType{messaging.TypeNodeHeartbeatRequest, messaging.TypeSetNodeLivenessResponse},
		[]messaging.IOType{first.Type, second.Type},
	)
	require.Equal(t, api.LivenessCaptureDraining, liveness.Load())
}
