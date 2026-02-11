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
	"context"
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/api"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestSetNodeLivenessRequestMonotonic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selfNode := node.NewInfo("127.0.0.1:0", "")
	coordinator := node.NewInfo("127.0.0.1:1", "")
	mc := messaging.NewMessageCenter(ctx, selfNode.ID, config.NewDefaultMessageCenterConfig(selfNode.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	var liveness api.Liveness
	manager := NewMaintainerManagerWithLiveness(selfNode, config.GetDefaultServerConfig().Debug.Scheduler, &liveness)
	manager.coordinatorID = coordinator.ID
	manager.coordinatorVersion = 1

	msg := messaging.NewSingleTargetMessage(selfNode.ID, messaging.MaintainerManagerTopic, &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: manager.nodeEpoch,
	})
	msg.From = coordinator.ID
	manager.onSetNodeLivenessRequest(msg)
	require.Equal(t, api.LivenessCaptureDraining, liveness.Load())

	msg = messaging.NewSingleTargetMessage(selfNode.ID, messaging.MaintainerManagerTopic, &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: manager.nodeEpoch,
	})
	msg.From = coordinator.ID
	manager.onSetNodeLivenessRequest(msg)
	require.Equal(t, api.LivenessCaptureDraining, liveness.Load())

	msg = messaging.NewSingleTargetMessage(selfNode.ID, messaging.MaintainerManagerTopic, &heartbeatpb.SetNodeLivenessRequest{
		Target:    heartbeatpb.NodeLiveness_STOPPING,
		NodeEpoch: manager.nodeEpoch,
	})
	msg.From = coordinator.ID
	manager.onSetNodeLivenessRequest(msg)
	require.Equal(t, api.LivenessCaptureStopping, liveness.Load())
}
