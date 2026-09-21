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
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/common"
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
	c.AdvanceLiveness(nil, nil)
	select {
	case <-mc.GetMessageChannel():
		require.FailNow(t, "unexpected command before resend interval")
	default:
	}

	// Rewind the last send time to cross resendInterval without sleep.
	c.mu.Lock()
	c.ensureNodeStateLocked(target).lastDrainCmdSentAt = time.Now().Add(-resendInterval - 10*time.Millisecond)
	c.mu.Unlock()
	c.AdvanceLiveness(nil, nil)
	msg = <-mc.GetMessageChannel()
	req = msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	require.Equal(t, heartbeatpb.NodeLiveness_DRAINING, req.Target)

	c.ObserveSetNodeLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{
		Applied:   heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 42,
	})

	// Once readyToStop, it should send STOPPING.
	c.AdvanceLiveness(func(node.ID) bool { return true }, func(node.ID) bool { return true })
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

func TestDrainControllerEventBrokerDispatcherCount(t *testing.T) {
	c := NewController(messaging.NewMockMessageCenter())
	target := node.ID("n1")
	c.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{Liveness: heartbeatpb.NodeLiveness_DRAINING, NodeEpoch: 42})
	// Broker counts must not prevent progressing to STOPPING and closing admission.
	require.Nil(t, c.NewEventBrokerDispatcherCountRequest(target))
	c.ObserveSetNodeLivenessResponse(target, &heartbeatpb.SetNodeLivenessResponse{Applied: heartbeatpb.NodeLiveness_STOPPING, NodeEpoch: 42})
	_, observed := c.GetEventBrokerDispatcherCount(target)
	require.False(t, observed)

	req := c.NewEventBrokerDispatcherCountRequest(target)
	require.NotNil(t, req)
	require.Nil(t, c.NewEventBrokerDispatcherCountRequest(target))
	response := &logservicepb.EventBrokerDispatcherCountResponse{
		TargetNodeId: req.TargetNodeId, RequestId: req.RequestId,
		Report: &logservicepb.EventBrokerDispatcherCount{DispatcherCount: 2, RegistrationsStopped: true},
	}
	c.ObserveEventBrokerDispatcherCountResponse(response)
	count, observed := c.GetEventBrokerDispatcherCount(target)
	require.True(t, observed)
	require.Equal(t, 2, count)

	// Restarting creates a new capture ID. A delayed reply for the old capture
	// must not satisfy the new capture's query, even with the same request ID.
	c.RemoveNode(target)
	target = node.ID("n2")
	c.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{Liveness: heartbeatpb.NodeLiveness_STOPPING, NodeEpoch: 43})
	req = c.NewEventBrokerDispatcherCountRequest(target)
	require.Equal(t, response.RequestId, req.RequestId)
	c.ObserveEventBrokerDispatcherCountResponse(response)
	_, observed = c.GetEventBrokerDispatcherCount(target)
	require.False(t, observed)
	response.TargetNodeId, response.RequestId = req.TargetNodeId, req.RequestId
	response.Report.DispatcherCount = 0
	c.ObserveEventBrokerDispatcherCountResponse(response)
	count, observed = c.GetEventBrokerDispatcherCount(target)
	require.True(t, observed)
	require.Zero(t, count)

	c.nodes[target].eventBrokerReportExpiresAt = time.Now().Add(-time.Second)
	_, observed = c.GetEventBrokerDispatcherCount(target)
	require.False(t, observed)
}

func TestDrainControllerRejectsUnusableBrokerReports(t *testing.T) {
	for _, name := range []string{"missing", "admission open", "stale report", "delayed response", "different capture", "different request"} {
		t.Run(name, func(t *testing.T) {
			c := NewController(messaging.NewMockMessageCenter())
			target := node.ID("n1")
			c.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{Liveness: heartbeatpb.NodeLiveness_STOPPING, NodeEpoch: 42})
			req := c.NewEventBrokerDispatcherCountRequest(target)
			response := &logservicepb.EventBrokerDispatcherCountResponse{
				TargetNodeId: req.TargetNodeId, RequestId: req.RequestId,
				Report: &logservicepb.EventBrokerDispatcherCount{RegistrationsStopped: true},
			}
			switch name {
			case "missing":
				response.Report = nil
			case "admission open":
				response.Report.RegistrationsStopped = false
			case "stale report":
				response.ReportAgeMs = uint64(common.EventBrokerReportTTL / time.Millisecond)
			case "delayed response":
				response.ReportAgeMs = 1000
				c.nodes[target].eventBrokerRequestSentAt = time.Now().Add(-common.EventBrokerReportTTL + 500*time.Millisecond)
			case "different capture":
				response.TargetNodeId = "another-capture"
			case "different request":
				response.RequestId++
			}
			c.ObserveEventBrokerDispatcherCountResponse(response)
			_, observed := c.GetEventBrokerDispatcherCount(target)
			require.False(t, observed)

			// Lost replies can be retried, but a reply to the old query cannot
			// complete the new query, even if it reports an admission-closed zero.
			c.nodes[target].eventBrokerRequestSentAt = time.Now().Add(-common.EventBrokerReportTTL)
			next := c.NewEventBrokerDispatcherCountRequest(target)
			require.Greater(t, next.RequestId, req.RequestId)
			response.TargetNodeId = req.TargetNodeId
			response.RequestId = req.RequestId
			response.Report = &logservicepb.EventBrokerDispatcherCount{RegistrationsStopped: true}
			response.ReportAgeMs = 0
			c.ObserveEventBrokerDispatcherCountResponse(response)
			_, observed = c.GetEventBrokerDispatcherCount(target)
			require.False(t, observed)
		})
	}
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
	c.AdvanceLiveness(func(node.ID) bool { return true }, func(node.ID) bool {
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

func TestDrainControllerSchedulerGateRequiresTargetAck(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	c := NewController(mc)

	target := node.ID("target")
	acked := node.ID("acked")
	pending := node.ID("pending")
	epoch := uint64(99)

	c.StartDrainTargetSchedulerGate(target, epoch)
	c.ObserveHeartbeat(acked, &heartbeatpb.NodeHeartbeat{
		Liveness:                    heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch:                   1,
		DispatcherDrainTargetNodeId: target.String(),
		DispatcherDrainTargetEpoch:  epoch,
	})
	c.ObserveHeartbeat(pending, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	})

	require.False(t, c.IsSchedulableDest(target))
	require.True(t, c.IsSchedulableDest(acked))
	require.False(t, c.IsSchedulableDest(pending))

	c.SwitchDrainTargetSchedulerGateToClear(target, epoch, nil)
	require.True(t, c.IsSchedulableDest(pending))
}

func TestDrainControllerSchedulerGateRequiresReackAfterNodeRestart(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	c := NewController(mc)

	target := node.ID("target")
	dest := node.ID("dest")
	epoch := uint64(99)

	c.StartDrainTargetSchedulerGate(target, epoch)
	c.ObserveHeartbeat(dest, &heartbeatpb.NodeHeartbeat{
		Liveness:                    heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch:                   1,
		DispatcherDrainTargetNodeId: target.String(),
		DispatcherDrainTargetEpoch:  epoch,
	})
	require.True(t, c.IsSchedulableDest(dest))

	// A replacement process with a newer node epoch must re-ack the active
	// drain target before the destination becomes schedulable again.
	c.ObserveHeartbeat(dest, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 2,
	})
	require.False(t, c.IsSchedulableDest(dest))

	// A stale heartbeat from the old process must not resurrect the old ack.
	c.ObserveHeartbeat(dest, &heartbeatpb.NodeHeartbeat{
		Liveness:                    heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch:                   1,
		DispatcherDrainTargetNodeId: target.String(),
		DispatcherDrainTargetEpoch:  epoch,
	})
	require.False(t, c.IsSchedulableDest(dest))

	c.ObserveHeartbeat(dest, &heartbeatpb.NodeHeartbeat{
		Liveness:                    heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch:                   2,
		DispatcherDrainTargetNodeId: target.String(),
		DispatcherDrainTargetEpoch:  epoch,
	})
	require.True(t, c.IsSchedulableDest(dest))
}

func TestDrainControllerSchedulingFreezeBlocksDestinations(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	c := NewController(mc)

	dest := node.ID("dest")
	c.ObserveHeartbeat(dest, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	})

	require.True(t, c.IsSchedulableDest(dest))

	c.SetSchedulingFrozen(true)
	require.False(t, c.IsSchedulableDest(dest))

	c.SetSchedulingFrozen(false)
	require.True(t, c.IsSchedulableDest(dest))
}

func TestDrainControllerClearGateBlocksPendingNodesUntilAck(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	c := NewController(mc)

	ready := node.ID("ready")
	pending := node.ID("pending")
	target := node.ID("target")
	epoch := uint64(77)

	c.ObserveHeartbeat(ready, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	})
	c.ObserveHeartbeat(pending, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	})

	c.StartDrainTargetClearGate(target, epoch, map[node.ID]struct{}{
		pending: {},
	})

	require.True(t, c.IsSchedulableDest(ready))
	require.False(t, c.IsSchedulableDest(pending))

	c.RemoveDrainTargetClearPendingNode(pending, target, epoch)
	require.True(t, c.IsSchedulableDest(pending))
}

func TestDrainControllerNewActiveGateSupersedesOldClearGate(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	c := NewController(mc)

	dest := node.ID("dest")
	oldTarget := node.ID("old-target")
	newTarget := node.ID("new-target")

	c.ObserveHeartbeat(dest, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	})
	c.StartDrainTargetClearGate(oldTarget, 10, map[node.ID]struct{}{
		dest: {},
	})
	require.False(t, c.IsSchedulableDest(dest))

	c.StartDrainTargetSchedulerGate(newTarget, 11)
	// The new active gate should replace the old clear-pending restriction, but
	// the destination still needs to acknowledge the new target before reuse.
	require.False(t, c.IsSchedulableDest(dest))

	c.ObserveHeartbeat(dest, &heartbeatpb.NodeHeartbeat{
		Liveness:                    heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch:                   1,
		DispatcherDrainTargetNodeId: newTarget.String(),
		DispatcherDrainTargetEpoch:  11,
	})
	require.True(t, c.IsSchedulableDest(dest))
}

func TestDrainControllerStaleClearDoesNotOverrideNewActiveGate(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	c := NewController(mc)

	dest := node.ID("dest")
	oldTarget := node.ID("old-target")
	newTarget := node.ID("new-target")

	c.ObserveHeartbeat(dest, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch: 1,
	})
	c.StartDrainTargetSchedulerGate(oldTarget, 10)
	c.StartDrainTargetSchedulerGate(newTarget, 11)

	c.SwitchDrainTargetSchedulerGateToClear(oldTarget, 10, map[node.ID]struct{}{
		dest: {},
	})

	c.ObserveHeartbeat(dest, &heartbeatpb.NodeHeartbeat{
		Liveness:                    heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch:                   1,
		DispatcherDrainTargetNodeId: newTarget.String(),
		DispatcherDrainTargetEpoch:  11,
	})
	require.True(t, c.IsSchedulableDest(dest))
}
