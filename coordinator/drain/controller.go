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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils"
	"go.uber.org/zap"
)

const (
	resendInterval     = time.Second
	defaultLivenessTTL = 30 * time.Second
)

// State is the coordinator-derived node liveness state.
// It extends node-reported liveness with Unknown based on heartbeat TTL.
type State int32

const (
	StateAlive State = iota
	StateDraining
	StateStopping
	StateUnknown
)

type nodeState struct {
	// drainProtocolObserved marks whether coordinator has received a bootstrap
	// capability response for this node. Unknown capability must not silently
	// degrade to the legacy fallback because that would hide bootstrap failures.
	drainProtocolObserved bool
	// drainProtocolVersion is the node-scoped drain capability reported during
	// bootstrap. Zero means the node only supports the legacy hard-restart path.
	drainProtocolVersion uint32
	// drainRequested indicates this node has entered the drain workflow.
	drainRequested bool
	// drainingObserved indicates the node has reported DRAINING.
	drainingObserved bool
	// stoppingObserved indicates the node has reported STOPPING.
	stoppingObserved bool

	// lastDrainCmdSentAt is the last send time of a DRAINING command for resend throttling.
	lastDrainCmdSentAt time.Time
	// lastStopCmdSentAt is the last send time of a STOPPING command for resend throttling.
	lastStopCmdSentAt time.Time

	observedSet bool
	lastSeen    time.Time
	nodeEpoch   uint64
	liveness    heartbeatpb.NodeLiveness
}

type drainTargetSchedulerGate struct {
	// target/epoch identify the active dispatcher drain target that a node must
	// acknowledge before it can receive newly placed maintainers.
	target node.ID
	epoch  uint64
	// ackedNodes records the node epoch whose heartbeat already reported the
	// active drain target snapshot. The target node itself is never schedulable.
	ackedNodes map[node.ID]uint64
}

type drainTargetClearGate struct {
	// target/epoch identify the cleared drain target that some nodes may still
	// cache locally. Those nodes must not receive new maintainers until they
	// have acknowledged the clear or disappeared from membership.
	target node.ID
	epoch  uint64
	// pendingNodes tracks the destinations that are still blocked by the clear
	// handshake for this target epoch.
	pendingNodes map[node.ID]struct{}
}

// Controller manages node drain progression by sending SetNodeLiveness commands and tracking observations.
//
// It is in-memory only. Observations come from either:
// - NodeHeartbeat, or
// - SetNodeLivenessResponse.
type Controller struct {
	mu sync.Mutex

	mc  messaging.MessageCenter
	ttl time.Duration

	nodes map[node.ID]*nodeState

	// targetSchedulerGate blocks maintainer placement onto nodes that have not
	// yet reported the active dispatcher drain target in node heartbeat.
	targetSchedulerGate *drainTargetSchedulerGate
	// clearSchedulerGate blocks placement onto nodes that may still cache a
	// stale drain target after coordinator has already cleared the active
	// drain session.
	clearSchedulerGate *drainTargetClearGate
	// schedulingFrozen pauses all coordinator-side scheduling destinations when
	// coordinator has observed an in-flight drain but cannot prove which active
	// dispatcher drain target every maintainer should follow.
	schedulingFrozen bool
}

// NewController creates a drain controller with in-memory state only.
func NewController(mc messaging.MessageCenter) *Controller {
	return &Controller{
		mc:    mc,
		ttl:   defaultLivenessTTL,
		nodes: make(map[node.ID]*nodeState),
	}
}

// RemoveNode drops all in-memory drain state for a node that has been removed
// from cluster membership. This cleanup must be tied to membership removal
// rather than heartbeat TTL so stale nodes can still remain Unknown.
func (c *Controller) RemoveNode(nodeID node.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.nodes, nodeID)
	if c.targetSchedulerGate != nil {
		delete(c.targetSchedulerGate.ackedNodes, nodeID)
	}
	if c.clearSchedulerGate != nil {
		delete(c.clearSchedulerGate.pendingNodes, nodeID)
		if len(c.clearSchedulerGate.pendingNodes) == 0 {
			c.clearSchedulerGate = nil
		}
	}
}

// ensureNodeStateLocked returns existing node state or creates one.
// Caller must hold c.mu.
func (c *Controller) ensureNodeStateLocked(nodeID node.ID) *nodeState {
	st, ok := c.nodes[nodeID]
	if !ok {
		st = &nodeState{}
		c.nodes[nodeID] = st
	}
	return st
}

// RequestDrain marks a node as drain requested and tries to send DRAINING immediately.
func (c *Controller) RequestDrain(nodeID node.ID) {
	c.mu.Lock()
	st := c.ensureNodeStateLocked(nodeID)
	st.drainRequested = true
	c.mu.Unlock()

	c.trySendDrainCommand(nodeID)
}

// ObserveBootstrapResponse records the node-scoped drain capability reported
// by a maintainer manager during bootstrap.
func (c *Controller) ObserveBootstrapResponse(nodeID node.ID, resp *heartbeatpb.CoordinatorBootstrapResponse) {
	if resp == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	st := c.ensureNodeStateLocked(nodeID)
	st.drainProtocolObserved = true
	st.drainProtocolVersion = resp.GetDrainProtocolVersion()
}

// ObserveHeartbeat updates drain progression from node heartbeat liveness.
func (c *Controller) ObserveHeartbeat(nodeID node.ID, hb *heartbeatpb.NodeHeartbeat) {
	if hb == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.observeLivenessLocked(nodeID, hb.NodeEpoch, hb.Liveness)
	c.observeTargetSchedulerAckLocked(nodeID, hb)
}

// ObserveSetNodeLivenessResponse updates drain progression from explicit liveness responses.
func (c *Controller) ObserveSetNodeLivenessResponse(nodeID node.ID, resp *heartbeatpb.SetNodeLivenessResponse) {
	if resp == nil {
		return
	}

	c.observeLiveness(nodeID, resp.NodeEpoch, resp.Applied)
}

// observeLiveness applies an observed node-reported liveness to the drain state.
func (c *Controller) observeLiveness(nodeID node.ID, nodeEpoch uint64, liveness heartbeatpb.NodeLiveness) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observeLivenessLocked(nodeID, nodeEpoch, liveness)
}

func (c *Controller) observeLivenessLocked(nodeID node.ID, nodeEpoch uint64, liveness heartbeatpb.NodeLiveness) {
	st := c.ensureNodeStateLocked(nodeID)

	if st.observedSet {
		// Drop stale observations to avoid transient rollback under reordering.
		if nodeEpoch < st.nodeEpoch {
			return
		}
		if nodeEpoch > st.nodeEpoch {
			resetObservedStateForNewEpoch(c.targetSchedulerGate, nodeID, st)
		}
		if nodeEpoch == st.nodeEpoch && liveness < st.liveness {
			return
		}
	}

	st.observedSet = true
	st.lastSeen = time.Now()
	st.nodeEpoch = nodeEpoch
	st.liveness = liveness

	applyObservedLiveness(st, liveness)
}

func (c *Controller) observeTargetSchedulerAckLocked(nodeID node.ID, hb *heartbeatpb.NodeHeartbeat) {
	gate := c.targetSchedulerGate
	if gate == nil || nodeID == gate.target {
		return
	}
	st, ok := c.nodes[nodeID]
	if !ok || !st.observedSet || hb.NodeEpoch != st.nodeEpoch {
		return
	}
	if node.ID(hb.GetDispatcherDrainTargetNodeId()) != gate.target ||
		hb.GetDispatcherDrainTargetEpoch() != gate.epoch {
		return
	}
	gate.ackedNodes[nodeID] = hb.NodeEpoch
}

// StartDrainTargetSchedulerGate blocks new maintainer placement onto nodes
// until their node heartbeat has reported the active dispatcher drain target.
func (c *Controller) StartDrainTargetSchedulerGate(target node.ID, epoch uint64) {
	if target.IsEmpty() || epoch == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.targetSchedulerGate = &drainTargetSchedulerGate{
		target:     target,
		epoch:      epoch,
		ackedNodes: make(map[node.ID]uint64),
	}
	// A newer active drain target supersedes any older clear handshake.
	c.clearSchedulerGate = nil
}

// SwitchDrainTargetSchedulerGateToClear atomically replaces the matching
// active drain-target gate with a clear gate. A stale clear from an old session
// must not overwrite a newer active drain gate.
func (c *Controller) SwitchDrainTargetSchedulerGateToClear(target node.ID, epoch uint64, pendingNodes map[node.ID]struct{}) {
	if target.IsEmpty() || epoch == 0 {
		return
	}

	clonedPendingNodes := cloneNodeSet(pendingNodes)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.targetSchedulerGate == nil ||
		c.targetSchedulerGate.target != target ||
		c.targetSchedulerGate.epoch != epoch {
		return
	}
	c.targetSchedulerGate = nil
	if len(clonedPendingNodes) == 0 {
		return
	}
	c.startDrainTargetClearGateLocked(target, epoch, clonedPendingNodes)
}

// StartDrainTargetClearGate blocks scheduling onto nodes that may still retain
// the cleared drain target snapshot locally.
func (c *Controller) StartDrainTargetClearGate(target node.ID, epoch uint64, pendingNodes map[node.ID]struct{}) {
	if target.IsEmpty() || epoch == 0 || len(pendingNodes) == 0 {
		return
	}

	clonedPendingNodes := cloneNodeSet(pendingNodes)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.targetSchedulerGate != nil {
		return
	}
	c.startDrainTargetClearGateLocked(target, epoch, clonedPendingNodes)
}

// startDrainTargetClearGateLocked installs or extends a clear gate without
// allowing older epochs to override a newer clear handshake.
func (c *Controller) startDrainTargetClearGateLocked(target node.ID, epoch uint64, pendingNodes map[node.ID]struct{}) {
	if c.clearSchedulerGate != nil {
		if c.clearSchedulerGate.epoch > epoch {
			return
		}
		if c.clearSchedulerGate.epoch == epoch {
			if c.clearSchedulerGate.target != target {
				return
			}
			utils.CopySetToSet(pendingNodes, c.clearSchedulerGate.pendingNodes)
			return
		}
	}
	c.clearSchedulerGate = &drainTargetClearGate{
		target:       target,
		epoch:        epoch,
		pendingNodes: pendingNodes,
	}
}

func cloneNodeSet(nodes map[node.ID]struct{}) map[node.ID]struct{} {
	cloned := make(map[node.ID]struct{}, len(nodes))
	utils.CopySetToSet(nodes, cloned)
	return cloned
}

// RemoveDrainTargetClearPendingNode unblocks one destination after it either
// acknowledged the cleared drain target or left the cluster.
func (c *Controller) RemoveDrainTargetClearPendingNode(nodeID node.ID, target node.ID, epoch uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.clearSchedulerGate == nil ||
		c.clearSchedulerGate.target != target ||
		c.clearSchedulerGate.epoch != epoch {
		return
	}
	delete(c.clearSchedulerGate.pendingNodes, nodeID)
	if len(c.clearSchedulerGate.pendingNodes) == 0 {
		c.clearSchedulerGate = nil
	}
}

// ClearDrainTargetClearGate removes the clear-pending placement gate for the
// matching cleared drain target.
func (c *Controller) ClearDrainTargetClearGate(target node.ID, epoch uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.clearSchedulerGate == nil ||
		c.clearSchedulerGate.target != target ||
		c.clearSchedulerGate.epoch != epoch {
		return
	}
	c.clearSchedulerGate = nil
}

// SetSchedulingFrozen updates the coordinator-side scheduling freeze flag.
func (c *Controller) SetSchedulingFrozen(frozen bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schedulingFrozen = frozen
}

// resetObservedStateForNewEpoch clears per-epoch observations when the node
// restarts with a newer epoch, while preserving the drain request so the
// coordinator can continue driving the drain workflow for the replacement
// process.
//
// It also drops any scheduler-gate acknowledgement cached for the old process
// lifetime. The replacement process must report the active drain target again
// before coordinator can safely place new maintainers on it.
func resetObservedStateForNewEpoch(gate *drainTargetSchedulerGate, nodeID node.ID, st *nodeState) {
	drainRequested := st.drainRequested
	if gate != nil {
		delete(gate.ackedNodes, nodeID)
	}
	*st = nodeState{
		drainRequested: drainRequested,
	}
}

// applyObservedLiveness applies monotonic progression from observed node liveness.
func applyObservedLiveness(st *nodeState, liveness heartbeatpb.NodeLiveness) {
	switch liveness {
	case heartbeatpb.NodeLiveness_DRAINING:
		st.drainRequested = true
		st.drainingObserved = true
	case heartbeatpb.NodeLiveness_STOPPING:
		st.drainRequested = true
		st.drainingObserved = true
		st.stoppingObserved = true
	}
}

// GetStatus returns the current drain workflow state for a node.
func (c *Controller) GetStatus(nodeID node.ID) (drainRequested, drainingObserved, stoppingObserved bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.nodes[nodeID]
	if !ok {
		return false, false, false
	}
	return st.drainRequested, st.drainingObserved, st.stoppingObserved
}

// GetDrainProtocolVersion returns the bootstrap-observed drain capability for a node.
func (c *Controller) GetDrainProtocolVersion(nodeID node.ID) (uint32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.nodes[nodeID]
	if !ok || !st.drainProtocolObserved {
		return 0, false
	}
	return st.drainProtocolVersion, true
}

// GetState returns coordinator-derived liveness state.
func (c *Controller) GetState(nodeID node.ID) State {
	c.mu.Lock()
	state := c.getStateLocked(nodeID, time.Now())
	c.mu.Unlock()
	return state
}

func (c *Controller) getStateLocked(nodeID node.ID, now time.Time) State {
	st, ok := c.nodes[nodeID]
	if !ok || !st.observedSet {
		// Never observed: keep compatibility during rollout.
		return StateAlive
	}
	if now.Sub(st.lastSeen) > c.ttl {
		return StateUnknown
	}
	switch st.liveness {
	case heartbeatpb.NodeLiveness_ALIVE:
		return StateAlive
	case heartbeatpb.NodeLiveness_DRAINING:
		return StateDraining
	case heartbeatpb.NodeLiveness_STOPPING:
		return StateStopping
	default:
		return StateAlive
	}
}

// GetNodeEpoch returns the last observed epoch for node liveness commands.
func (c *Controller) GetNodeEpoch(nodeID node.ID) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.nodes[nodeID]
	if !ok || !st.observedSet {
		return 0, false
	}
	return st.nodeEpoch, true
}

// IsSchedulableDest returns true only when the node is eligible as a scheduling destination.
func (c *Controller) IsSchedulableDest(nodeID node.ID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.schedulingFrozen {
		return false
	}
	if c.getStateLocked(nodeID, time.Now()) != StateAlive {
		return false
	}
	if c.clearSchedulerGate != nil {
		if _, ok := c.clearSchedulerGate.pendingNodes[nodeID]; ok {
			return false
		}
	}
	if c.targetSchedulerGate == nil {
		return true
	}
	if nodeID == c.targetSchedulerGate.target {
		return false
	}
	st, ok := c.nodes[nodeID]
	if !ok || !st.observedSet {
		return false
	}
	ackedEpoch, ok := c.targetSchedulerGate.ackedNodes[nodeID]
	return ok && ackedEpoch == st.nodeEpoch
}

// GetDrainingOrStoppingNodes returns non-stale nodes observed as draining or stopping.
func (c *Controller) GetDrainingOrStoppingNodes() []node.ID {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.nodes) == 0 {
		return nil
	}

	res := make([]node.ID, 0, len(c.nodes))
	for id := range c.nodes {
		switch c.getStateLocked(id, now) {
		case StateDraining, StateStopping:
			res = append(res, id)
		}
	}
	return res
}

// AdvanceLiveness advances liveness commands for the node set selected by
// shouldManage:
// - Request DRAINING until observed
// - Once readyToStop and DRAINING observed, request STOPPING until observed
func (c *Controller) AdvanceLiveness(
	shouldManage func(node.ID) bool,
	readyToStop func(node.ID) bool,
) {
	nodeIDs := c.listDrainRequestedNodeIDs()

	for _, nodeID := range nodeIDs {
		if shouldManage != nil && !shouldManage(nodeID) {
			continue
		}
		drainRequested, drainingObserved, stoppingObserved := c.GetStatus(nodeID)
		if !drainRequested {
			continue
		}

		if !drainingObserved {
			c.trySendDrainCommand(nodeID)
			continue
		}

		if !stoppingObserved && readyToStop != nil && readyToStop(nodeID) {
			c.trySendStopCommand(nodeID)
		}
	}
}

// listDrainRequestedNodeIDs snapshots nodes that are already in drain workflow.
func (c *Controller) listDrainRequestedNodeIDs() []node.ID {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeIDs := make([]node.ID, 0, len(c.nodes))
	for id, st := range c.nodes {
		if st.drainRequested {
			nodeIDs = append(nodeIDs, id)
		}
	}
	return nodeIDs
}

// trySendDrainCommand sends DRAINING when it is not yet observed and resend is not throttled.
func (c *Controller) trySendDrainCommand(nodeID node.ID) {
	if !c.checkAndMarkDrainCommandSend(nodeID) {
		return
	}
	c.sendSetNodeLiveness(nodeID, heartbeatpb.NodeLiveness_DRAINING)
}

// trySendStopCommand sends STOPPING when it is not yet observed and resend is not throttled.
func (c *Controller) trySendStopCommand(nodeID node.ID) {
	epoch, ok := c.checkAndMarkStopCommandSend(nodeID)
	if !ok {
		return
	}
	c.sendSetNodeLivenessWithEpoch(nodeID, heartbeatpb.NodeLiveness_STOPPING, epoch)
}

// checkAndMarkDrainCommandSend checks whether a DRAINING command should be sent
// and records the send timestamp for resend throttling.
func (c *Controller) checkAndMarkDrainCommandSend(nodeID node.ID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	st := c.ensureNodeStateLocked(nodeID)
	if st.drainingObserved || isResendThrottled(st.lastDrainCmdSentAt) {
		return false
	}
	st.lastDrainCmdSentAt = time.Now()
	return true
}

// checkAndMarkStopCommandSend captures the epoch that observed DRAINING while it
// still holds c.mu. This keeps the STOPPING eligibility check and the command
// epoch consistent, so a fresh ALIVE heartbeat from a restarted node cannot
// reuse an old draining observation to advance a newer epoch directly to
// STOPPING.
func (c *Controller) checkAndMarkStopCommandSend(nodeID node.ID) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st := c.ensureNodeStateLocked(nodeID)
	if !st.observedSet ||
		!st.drainRequested ||
		!st.drainingObserved ||
		st.stoppingObserved ||
		isResendThrottled(st.lastStopCmdSentAt) {
		return 0, false
	}

	st.lastStopCmdSentAt = time.Now()
	return st.nodeEpoch, true
}

// isResendThrottled returns whether a resend should be skipped in the current interval.
func isResendThrottled(lastSentAt time.Time) bool {
	return !lastSentAt.IsZero() && time.Since(lastSentAt) < resendInterval
}

// sendSetNodeLiveness sends a liveness command to the target maintainer manager.
func (c *Controller) sendSetNodeLiveness(nodeID node.ID, target heartbeatpb.NodeLiveness) {
	var epoch uint64
	if e, ok := c.GetNodeEpoch(nodeID); ok {
		epoch = e
	}
	c.sendSetNodeLivenessWithEpoch(nodeID, target, epoch)
}

// sendSetNodeLivenessWithEpoch sends a liveness command using a caller-provided
// epoch that was already validated against the controller state.
func (c *Controller) sendSetNodeLivenessWithEpoch(nodeID node.ID, target heartbeatpb.NodeLiveness, epoch uint64) {
	msg := messaging.NewSingleTargetMessage(nodeID, messaging.MaintainerManagerTopic, &heartbeatpb.SetNodeLivenessRequest{
		Target:    target,
		NodeEpoch: epoch,
	})
	if err := c.mc.SendCommand(msg); err != nil {
		log.Warn("send set node liveness command failed",
			zap.Stringer("nodeID", nodeID),
			zap.String("target", target.String()),
			zap.Error(err))
		return
	}
	log.Info("send set node liveness command",
		zap.Stringer("nodeID", nodeID),
		zap.String("target", target.String()),
		zap.Uint64("epoch", epoch))
}
