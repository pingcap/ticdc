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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// nodeHeartbeatInterval bounds background node heartbeat frequency.
// Forced heartbeats bypass this throttle to acknowledge state changes immediately.
const nodeHeartbeatInterval = 5 * time.Second

// managerNodePart owns node-scoped state shared by all local maintainers.
type managerNodePart struct {
	// liveness points to the server-wide node liveness state shared with other
	// modules such as the elector and coordinator command handlers.
	liveness *api.Liveness
	// nodeEpoch identifies the current process lifetime. Coordinator commands
	// must match it so stale requests from a previous process instance are ignored.
	nodeEpoch uint64

	// dispatcherDrainTarget caches the latest coordinator-issued dispatcher drain
	// target at manager scope so this node can acknowledge activation and clear
	// even when it temporarily hosts no maintainers.
	dispatcherDrainTarget struct {
		sync.RWMutex
		target node.ID
		epoch  uint64
	}

	// lastNodeHeartbeatSentAt records the last successful periodic node heartbeat
	// send so background heartbeats can be throttled.
	lastNodeHeartbeatSentAt time.Time
}

// newManagerNodePart initializes the node-scoped state owned by a manager.
func newManagerNodePart(liveness *api.Liveness) *managerNodePart {
	return &managerNodePart{
		liveness:  liveness,
		nodeEpoch: newNodeEpoch(),
	}
}

// newNodeEpoch creates a non-zero epoch for this process lifetime.
// Zero is reserved as "unknown epoch" in coordinator requests before any observation.
func newNodeEpoch() uint64 {
	nodeEpoch := uint64(time.Now().UnixNano())
	if nodeEpoch == 0 {
		return 1
	}
	return nodeEpoch
}

// sendNodeHeartbeat reports node-scoped liveness and dispatcher drain target to
// coordinator. It is the authoritative acknowledgement channel for node-level
// drain state, including cases where no changefeed maintainer exists locally.
func (m *Manager) sendNodeHeartbeat(force bool) {
	if m.coordinatorVersion <= 0 || m.coordinatorID.IsEmpty() {
		return
	}

	now := time.Now()
	if !force && now.Sub(m.node.lastNodeHeartbeatSentAt) < nodeHeartbeatInterval {
		return
	}

	drainTarget, drainEpoch := m.getDispatcherDrainTarget()
	hb := &heartbeatpb.NodeHeartbeat{
		Liveness:  m.toNodeLivenessPB(m.node.liveness.Load()),
		NodeEpoch: m.node.nodeEpoch,
		// Report the manager-level dispatcher drain target so coordinator can
		// confirm both activation and clearing even when no maintainers exist.
		DispatcherDrainTargetNodeId: drainTarget.String(),
		DispatcherDrainTargetEpoch:  drainEpoch,
	}
	target := m.newCoordinatorTopicMessage(hb)
	if err := m.mc.SendCommand(target); err != nil {
		log.Warn("send node heartbeat failed",
			zap.Stringer("from", m.nodeInfo.ID),
			zap.Stringer("target", target.To),
			zap.Error(err))
		return
	}
	m.node.lastNodeHeartbeatSentAt = now
}

// onSetNodeLivenessRequest applies a coordinator-driven liveness transition if
// the request targets the current process epoch. The transition is monotonic:
// the node may move forward to a stricter state but never roll back locally.
func (m *Manager) onSetNodeLivenessRequest(msg *messaging.TargetMessage) {
	if m.coordinatorID != msg.From {
		log.Warn("ignore set node liveness request from non coordinator",
			zap.Stringer("from", msg.From),
			zap.Stringer("coordinatorID", m.coordinatorID))
		return
	}

	req := msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	if req.NodeEpoch != m.node.nodeEpoch {
		log.Info("reject set node liveness request due to epoch mismatch",
			zap.Stringer("nodeID", m.nodeInfo.ID),
			zap.Uint64("localEpoch", m.node.nodeEpoch),
			zap.Uint64("requestEpoch", req.NodeEpoch))
		m.sendSetNodeLivenessResponse(m.node.liveness.Load())
		return
	}

	target := m.fromNodeLivenessPB(req.Target)
	current := m.node.liveness.Load()
	if target > current {
		if m.node.liveness.Store(target) {
			log.Info("node liveness transition applied",
				zap.Stringer("nodeID", m.nodeInfo.ID),
				zap.String("from", current.String()),
				zap.String("to", target.String()),
				zap.Uint64("epoch", m.node.nodeEpoch))
			current = target
			m.sendNodeHeartbeat(true)
		}
	}

	m.sendSetNodeLivenessResponse(current)
}

// onSetDispatcherDrainTargetRequest updates the latest dispatcher drain target
// and forwards it to all existing maintainers. A manager-level node heartbeat
// is sent after each accepted update so coordinator can observe the ack even
// when this node currently hosts no maintainers.
func (m *Manager) onSetDispatcherDrainTargetRequest(msg *messaging.TargetMessage) {
	if m.coordinatorID != msg.From {
		log.Warn("ignore set dispatcher drain target request from non coordinator",
			zap.Stringer("from", msg.From),
			zap.Stringer("coordinatorID", m.coordinatorID))
		return
	}

	req := msg.Message[0].(*heartbeatpb.SetDispatcherDrainTargetRequest)
	target := node.ID(req.TargetNodeId)
	if !m.node.tryUpdateDispatcherDrainTarget(target, req.TargetEpoch) {
		return
	}

	log.Info("dispatcher drain target updated",
		zap.Stringer("targetNodeID", target),
		zap.Uint64("targetEpoch", req.TargetEpoch))
	m.maintainers.applyDispatcherDrainTarget(target, req.TargetEpoch)
	// A manager-level heartbeat is the authoritative acknowledgement that this
	// node has applied the latest drain target, even when no maintainers exist.
	m.sendNodeHeartbeat(true)
}

// getDispatcherDrainTarget returns a consistent snapshot of the manager-level
// dispatcher drain target and its epoch.
func (m *Manager) getDispatcherDrainTarget() (node.ID, uint64) {
	m.node.dispatcherDrainTarget.RLock()
	defer m.node.dispatcherDrainTarget.RUnlock()
	return m.node.dispatcherDrainTarget.target, m.node.dispatcherDrainTarget.epoch
}

// tryUpdateDispatcherDrainTarget applies only monotonic target updates.
// A higher epoch always wins, while the same epoch may only perform the
// one-way transition from a non-empty target to an empty target.
func (n *managerNodePart) tryUpdateDispatcherDrainTarget(target node.ID, epoch uint64) bool {
	n.dispatcherDrainTarget.Lock()
	defer n.dispatcherDrainTarget.Unlock()

	if epoch < n.dispatcherDrainTarget.epoch {
		return false
	}
	if epoch == n.dispatcherDrainTarget.epoch {
		// When epoch is unchanged, only allow clear-once transition:
		// non-empty target -> empty target.
		// Reject all other transitions to avoid stale message reactivation.
		if target == n.dispatcherDrainTarget.target {
			return false
		}
		if target.IsEmpty() && !n.dispatcherDrainTarget.target.IsEmpty() {
			n.dispatcherDrainTarget.target = target
			return true
		}
		return false
	}
	n.dispatcherDrainTarget.target = target
	n.dispatcherDrainTarget.epoch = epoch
	return true
}

// sendSetNodeLivenessResponse returns the liveness currently applied by this
// process together with the local process epoch.
func (m *Manager) sendSetNodeLivenessResponse(applied api.Liveness) {
	resp := &heartbeatpb.SetNodeLivenessResponse{
		Applied:   m.toNodeLivenessPB(applied),
		NodeEpoch: m.node.nodeEpoch,
	}
	target := m.newCoordinatorTopicMessage(resp)
	if err := m.mc.SendCommand(target); err != nil {
		log.Warn("send set node liveness response failed",
			zap.Stringer("from", m.nodeInfo.ID),
			zap.Stringer("target", target.To),
			zap.Error(err))
	}
}

// fromNodeLivenessPB converts the protocol enum into the server-local liveness enum.
func (m *Manager) fromNodeLivenessPB(l heartbeatpb.NodeLiveness) api.Liveness {
	switch l {
	case heartbeatpb.NodeLiveness_ALIVE:
		return api.LivenessCaptureAlive
	case heartbeatpb.NodeLiveness_DRAINING:
		return api.LivenessCaptureDraining
	case heartbeatpb.NodeLiveness_STOPPING:
		return api.LivenessCaptureStopping
	default:
		return api.LivenessCaptureAlive
	}
}

// toNodeLivenessPB converts the server-local liveness enum into the protocol enum.
func (m *Manager) toNodeLivenessPB(l api.Liveness) heartbeatpb.NodeLiveness {
	switch l {
	case api.LivenessCaptureAlive:
		return heartbeatpb.NodeLiveness_ALIVE
	case api.LivenessCaptureDraining:
		return heartbeatpb.NodeLiveness_DRAINING
	case api.LivenessCaptureStopping:
		return heartbeatpb.NodeLiveness_STOPPING
	default:
		return heartbeatpb.NodeLiveness_ALIVE
	}
}
