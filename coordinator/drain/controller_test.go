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
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestEventBrokerDispatcherCountFallsBackWhenOldLogCoordinatorDoesNotRespond(t *testing.T) {
	c := NewController(messaging.NewMockMessageCenter())
	target := node.ID("old-log-coordinator")
	c.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	})

	count, observed := c.GetEventBrokerDispatcherCount(target)
	require.Zero(t, count)
	require.False(t, observed)

	// Avoid sleeping in the test while exercising the no-response path. In
	// this path ObserveEventBrokerDispatcherCountResponse is never called.
	c.mu.Lock()
	c.ensureNodeStateLocked(target).eventBrokerDispatcherCountUnavailableSince =
		time.Now().Add(-eventBrokerDispatcherCountNoReportTimeout - time.Second)
	c.mu.Unlock()

	count, observed = c.GetEventBrokerDispatcherCount(target)
	require.Zero(t, count)
	require.True(t, observed)
}

func TestEventBrokerDispatcherCountFreshResponseResetsCoordinatorFallback(t *testing.T) {
	c := NewController(messaging.NewMockMessageCenter())
	target := node.ID("target")
	c.ObserveHeartbeat(target, &heartbeatpb.NodeHeartbeat{
		Liveness:  heartbeatpb.NodeLiveness_DRAINING,
		NodeEpoch: 1,
	})

	c.mu.Lock()
	st := c.ensureNodeStateLocked(target)
	st.eventBrokerDispatcherCountUnavailableSince =
		time.Now().Add(-eventBrokerDispatcherCountNoReportTimeout - time.Second)
	c.mu.Unlock()

	count, observed := c.GetEventBrokerDispatcherCount(target)
	require.Zero(t, count)
	require.True(t, observed)

	c.ObserveEventBrokerDispatcherCountResponse(&logservicepb.EventBrokerDispatcherCountResponse{
		TargetNodeId:    target.String(),
		DispatcherCount: 2,
		Observed:        true,
	})
	count, observed = c.GetEventBrokerDispatcherCount(target)
	require.Equal(t, uint32(2), count)
	require.True(t, observed)

	c.mu.Lock()
	st = c.ensureNodeStateLocked(target)
	require.True(t, st.eventBrokerDispatcherCountUnavailableSince.IsZero())
	require.False(t, st.eventBrokerDispatcherCountFallbackLogged)
	c.mu.Unlock()
}

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

func TestShouldPauseRegularBalanceForWholeDrainWorkflow(t *testing.T) {
	c := NewController(messaging.NewMockMessageCenter())
	target := node.ID("target")

	require.False(t, c.ShouldPauseRegularBalance())

	// A requested drain must block regular balance before DRAINING is observed
	// and after heartbeat expiry changes the derived state to Unknown.
	c.RequestDrain(target)
	require.True(t, c.ShouldPauseRegularBalance())
	c.mu.Lock()
	st := c.ensureNodeStateLocked(target)
	st.observedSet = true
	st.lastSeen = time.Now().Add(-c.ttl - time.Second)
	st.liveness = heartbeatpb.NodeLiveness_DRAINING
	c.mu.Unlock()
	require.Equal(t, StateUnknown, c.GetState(target))
	require.True(t, c.ShouldPauseRegularBalance())

	// Membership removal completes the per-node drain state.
	c.RemoveNode(target)
	require.False(t, c.ShouldPauseRegularBalance())

	// The clear handshake remains part of the drain workflow.
	pending := map[node.ID]struct{}{node.ID("peer"): {}}
	c.StartDrainTargetClearGate(target, 1, pending)
	require.True(t, c.ShouldPauseRegularBalance())
	c.ClearDrainTargetClearGate(target, 1)
	require.False(t, c.ShouldPauseRegularBalance())
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
