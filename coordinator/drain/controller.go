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
}

// NewController creates a drain controller with in-memory state only.
func NewController(mc messaging.MessageCenter) *Controller {
	return NewControllerWithTTL(mc, defaultLivenessTTL)
}

// NewControllerWithTTL creates a drain controller with customized liveness TTL.
func NewControllerWithTTL(mc messaging.MessageCenter, ttl time.Duration) *Controller {
	return &Controller{
		mc:    mc,
		ttl:   ttl,
		nodes: make(map[node.ID]*nodeState),
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

// ObserveHeartbeat updates drain progression from node heartbeat liveness.
func (c *Controller) ObserveHeartbeat(nodeID node.ID, hb *heartbeatpb.NodeHeartbeat) {
	if hb == nil {
		return
	}

	c.observeLiveness(nodeID, hb.NodeEpoch, hb.Liveness)
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
	st := c.ensureNodeStateLocked(nodeID)

	if st.observedSet {
		// Drop stale observations to avoid transient rollback under reordering.
		if nodeEpoch < st.nodeEpoch {
			return
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

// GetState returns coordinator-derived liveness state.
func (c *Controller) GetState(nodeID node.ID) State {
	c.mu.Lock()
	st, ok := c.nodes[nodeID]
	if !ok || !st.observedSet {
		c.mu.Unlock()
		// Never observed: keep compatibility during rollout.
		return StateAlive
	}
	lastSeen := st.lastSeen
	liveness := st.liveness
	c.mu.Unlock()

	if time.Since(lastSeen) > c.ttl {
		return StateUnknown
	}
	switch liveness {
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
	return c.GetState(nodeID) == StateAlive
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
	for id, st := range c.nodes {
		if !st.observedSet || now.Sub(st.lastSeen) > c.ttl {
			continue
		}
		if st.liveness == heartbeatpb.NodeLiveness_DRAINING ||
			st.liveness == heartbeatpb.NodeLiveness_STOPPING {
			res = append(res, id)
		}
	}
	return res
}

// AdvanceLiveness advances liveness commands:
// - Request DRAINING until observed
// - Once readyToStop and DRAINING observed, request STOPPING until observed
func (c *Controller) AdvanceLiveness(readyToStop func(node.ID) bool) {
	nodeIDs := c.listDrainRequestedNodeIDs()

	for _, nodeID := range nodeIDs {
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
	if !c.checkAndMarkCommandSend(nodeID, heartbeatpb.NodeLiveness_DRAINING) {
		return
	}
	c.sendSetNodeLiveness(nodeID, heartbeatpb.NodeLiveness_DRAINING)
}

// trySendStopCommand sends STOPPING when it is not yet observed and resend is not throttled.
func (c *Controller) trySendStopCommand(nodeID node.ID) {
	if !c.checkAndMarkCommandSend(nodeID, heartbeatpb.NodeLiveness_STOPPING) {
		return
	}
	c.sendSetNodeLiveness(nodeID, heartbeatpb.NodeLiveness_STOPPING)
}

// checkAndMarkCommandSend checks observed/throttle conditions and records command send time.
func (c *Controller) checkAndMarkCommandSend(nodeID node.ID, target heartbeatpb.NodeLiveness) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	st := c.ensureNodeStateLocked(nodeID)
	switch target {
	case heartbeatpb.NodeLiveness_DRAINING:
		if st.drainingObserved || isResendThrottled(st.lastDrainCmdSentAt) {
			return false
		}
		st.lastDrainCmdSentAt = time.Now()
	case heartbeatpb.NodeLiveness_STOPPING:
		if st.stoppingObserved || isResendThrottled(st.lastStopCmdSentAt) {
			return false
		}
		st.lastStopCmdSentAt = time.Now()
	default:
		return false
	}
	return true
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
