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
package drain

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestDrainControllerResendAndPromoteToStopping(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	c := NewController(mc)

	target := node.ID("n1")
	c.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 42,
	})

	c.RequestDrain(target)
	msg := <-mc.GetMessageChannel()
	require.Equal(t, messaging.TypeSetNodeLivenessRequest, msg.Type)
	require.Equal(t, messaging.MaintainerManagerTopic, msg.Topic)
	require.Equal(t, target, msg.To)
	req := msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	require.Equal(t, heartbeatpb.NodeLiveness_DRAINING, req.Target)
	require.Equal(t, uint64(42), req.NodeEpoch)

	// Before draining observed, it should retry after the resend interval.
	c.AdvanceLiveness(nil)
	select {
	case <-mc.GetMessageChannel():
		require.FailNow(t, "unexpected command before resend interval")
	default:
	}

	// Rewind the last send time to cross resendInterval without sleep.
	c.mu.Lock()
	c.ensureNodeStateLocked(target).lastDrainCmdSentAt = time.Now().Add(-resendInterval - 10*time.Millisecond)
	c.mu.Unlock()
	c.AdvanceLiveness(nil)
	msg = <-mc.GetMessageChannel()
	req = msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	require.Equal(t, heartbeatpb.NodeLiveness_DRAINING, req.Target)

	c.ObserveSetNodeLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 42,
	})

	// Once readyToStop, it should send STOPPING.
	c.AdvanceLiveness(func(node.ID) bool { return true })
	msg = <-mc.GetMessageChannel()
	req = msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	require.Equal(t, heartbeatpb.NodeLiveness_STOPPING, req.Target)
}

func TestDrainControllerRemoveNodeClearsState(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	c := NewController(mc)

	target := node.ID("n1")
	c.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 42,
	})
	c.RequestDrain(target)

	c.RemoveNode(target)

	c.mu.Lock()
	_, ok := c.nodes[target]
	c.mu.Unlock()
	require.False(t, ok)
}

func TestDrainControllerResetObservedStateForNewEpoch(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	c := NewController(mc)

	target := node.ID("n1")
	c.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 42,
	})

	c.mu.Lock()
	st := c.ensureNodeStateLocked(target)
	st.lastDrainCmdSentAt = time.Now()
	st.lastStopCmdSentAt = time.Now()
	c.mu.Unlock()

	c.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 43,
	})

	drainRequested, drainingObserved, stoppingObserved := c.GetStatus(target)
	require.True(t, drainRequested)
	require.False(t, drainingObserved)
	require.False(t, stoppingObserved)

	epoch, ok := c.GetNodeEpoch(target)
	require.True(t, ok)
	require.Equal(t, uint64(43), epoch)

	c.mu.Lock()
	st = c.ensureNodeStateLocked(target)
	require.True(t, st.lastDrainCmdSentAt.IsZero())
	require.True(t, st.lastStopCmdSentAt.IsZero())
	c.mu.Unlock()
}

func TestDrainControllerSkipStoppingForNewEpochWithoutDraining(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	c := NewController(mc)

	target := node.ID("n1")
	c.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 42,
	})

	// Simulate a node restart after AdvanceLiveness snapshots the old draining
	// observation but before it tries to send STOPPING.
	c.AdvanceLiveness(func(node.ID) bool {
		c.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{
			Liveness:  heartbeatpb.NodeLiveness_ALIVE,
			NodeEpoch: 43,
		})
		return true
	})

	select {
	case msg := <-mc.GetMessageChannel():
		req := msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
		t.Fatalf("unexpected liveness command sent for new epoch: target=%s epoch=%d", req.Target.String(), req.NodeEpoch)
	default:
	}
}
