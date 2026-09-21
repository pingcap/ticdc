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
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/writelease"
	"go.uber.org/zap"
)

// managerNodeState owns node-scoped state shared by all local maintainers.
type managerNodeState struct {
	// liveness points to the server-wide node liveness state shared with other
	// modules such as the elector and coordinator command handlers.
	liveness *liveness.Liveness
	// nodeEpoch identifies the current process lifetime. Coordinator commands
	// must match it so stale requests from a previous process instance are ignored.
	nodeEpoch uint64

	// dispatcherDrainTarget stores the latest drain target for dispatcher
	// evacuation already applied on this capture. Here "dispatcher" explains
	// what is being drained: the dispatcher tasks currently hosted on the target
	// node, not the maintainer itself and not the node-liveness state. It is
	// capture-wide state, not per-changefeed state. Keeping it here lets the
	// manager acknowledge "target set" and "target cleared" in node heartbeat
	// even when this capture currently hosts no maintainers.
	dispatcherDrainTarget struct {
		sync.RWMutex
		target node.ID
		epoch  uint64
	}

	// lastNodeHeartbeatSentAt records the last successful periodic node heartbeat
	// send so background heartbeats can be throttled.
	lastNodeHeartbeatSentAt time.Time

	writeLeaseRequestSeq    uint64
	writeLeaseRequestSentAt map[uint64]time.Time
	lastAppliedLeaseSeq     uint64
	pendingWitnessAck       *heartbeatpb.WriteLeaseWitnessAck
}

// newManagerNodeState initializes the node-scoped state owned by a manager.
func newManagerNodeState(nodeLiveness *liveness.Liveness) *managerNodeState {
	return &managerNodeState{
		liveness:                nodeLiveness,
		nodeEpoch:               newNodeEpoch(),
		writeLeaseRequestSentAt: make(map[uint64]time.Time),
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
	if !m.isBootstrap() {
		return
	}

	now := time.Now()
	if !force && now.Sub(m.node.lastNodeHeartbeatSentAt) < writelease.NodeHeartbeatInterval {
		return
	}
	// Update before sending so a transient send failure will not cause
	// frequent retries and log spam on the 200ms tick.
	m.node.lastNodeHeartbeatSentAt = now

	currentLiveness := liveness.CaptureAlive
	if m.node.liveness != nil {
		currentLiveness = m.node.liveness.Load()
	}
	drainTarget, drainEpoch := m.getDispatcherDrainTarget()
	m.node.writeLeaseRequestSeq++
	requestSeq := m.node.writeLeaseRequestSeq
	m.node.writeLeaseRequestSentAt[requestSeq] = now
	m.node.pruneWriteLeaseRequests(now)
	hb := &heartbeatpb.NodeHeartbeat{
		Liveness:  m.toNodeLivenessPB(currentLiveness),
		NodeEpoch: m.node.nodeEpoch,
		// Report the manager-level dispatcher drain target so coordinator can
		// confirm both activation and clearing even when no maintainers exist.
		DispatcherDrainTargetNodeId: drainTarget.String(),
		DispatcherDrainTargetEpoch:  drainEpoch,
		WriteLeaseRequestSeq:        requestSeq,
		WriteLeaseProtocolVersion:   heartbeatpb.CurrentWriteLeaseProtocolVersion,
		WriteLeaseWitnessAck:        m.node.pendingWitnessAck,
	}
	target := m.newCoordinatorTopicMessage(hb)
	if err := m.mc.SendCommand(target); err != nil {
		delete(m.node.writeLeaseRequestSentAt, requestSeq)
		log.Warn("send node heartbeat failed",
			zap.Stringer("from", m.nodeInfo.ID),
			zap.Stringer("target", target.To),
			zap.Error(err))
		return
	}
	m.node.pendingWitnessAck = nil
}

func (m *Manager) onNodeHeartbeatResponse(msg *messaging.TargetMessage) {
	metrics.CaptureLeaseResponseCounter.WithLabelValues("received").Inc()
	if msg.From != m.coordinatorID {
		metrics.CaptureLeaseResponseRejectedCounter.WithLabelValues("sender").Inc()
		return
	}
	response := msg.Message[0].(*heartbeatpb.NodeHeartbeatResponse)
	if response.GetCoordinatorVersion() != m.coordinatorVersion {
		metrics.CaptureLeaseResponseRejectedCounter.WithLabelValues("coordinator_version").Inc()
		return
	}
	if response.GetTargetNodeEpoch() != m.node.nodeEpoch {
		metrics.CaptureLeaseResponseRejectedCounter.WithLabelValues("node_epoch").Inc()
		return
	}

	challenge := response.GetWitnessChallenge()
	if challenge != nil &&
		challenge.GetCoordinatorVersion() == m.coordinatorVersion &&
		challenge.GetWitnessNodeEpoch() == m.node.nodeEpoch &&
		len(challenge.GetNonce()) > 0 {
		m.node.pendingWitnessAck = &heartbeatpb.WriteLeaseWitnessAck{
			CoordinatorVersion:   challenge.GetCoordinatorVersion(),
			CoordinatorNodeEpoch: challenge.GetCoordinatorNodeEpoch(),
			SelfRequestSeq:       challenge.GetSelfRequestSeq(),
			WitnessNodeEpoch:     challenge.GetWitnessNodeEpoch(),
			Nonce:                append([]byte(nil), challenge.GetNonce()...),
		}
		m.sendNodeHeartbeat(true)
	}

	requestSeq := response.GetRequestSeq()
	if requestSeq == 0 {
		if challenge == nil {
			metrics.CaptureLeaseResponseRejectedCounter.WithLabelValues("request_sequence").Inc()
		}
		return
	}
	if requestSeq <= m.node.lastAppliedLeaseSeq {
		metrics.CaptureLeaseResponseRejectedCounter.WithLabelValues("replayed_sequence").Inc()
		return
	}
	requestSentAt, ok := m.node.writeLeaseRequestSentAt[requestSeq]
	if !ok {
		metrics.CaptureLeaseResponseRejectedCounter.WithLabelValues("unknown_sequence").Inc()
		return
	}
	leaseDurationMs := response.GetLeaseDurationMs()
	if leaseDurationMs == 0 {
		// The coordinator observed an unknown or legacy capture, so the whole
		// cluster temporarily falls back to etcd-only write admission.
		m.writeGate.SetP2PRequired(false)
	} else {
		duration := time.Duration(leaseDurationMs) * time.Millisecond
		if duration <= 0 || duration > writelease.P2PLeaseDuration {
			metrics.CaptureLeaseResponseRejectedCounter.WithLabelValues("lease_duration").Inc()
			return
		}
		if !m.writeGate.RenewP2P(requestSentAt, duration) {
			metrics.CaptureLeaseResponseRejectedCounter.WithLabelValues("expired_or_fenced").Inc()
			return
		}
		m.writeGate.SetP2PRequired(true)
	}
	metrics.CaptureLeaseResponseCounter.WithLabelValues("accepted").Inc()
	m.node.lastAppliedLeaseSeq = requestSeq
	for seq := range m.node.writeLeaseRequestSentAt {
		if seq <= requestSeq {
			delete(m.node.writeLeaseRequestSentAt, seq)
		}
	}
}

func (n *managerNodeState) pruneWriteLeaseRequests(now time.Time) {
	for seq, sentAt := range n.writeLeaseRequestSentAt {
		if !sentAt.Add(writelease.P2PLeaseDuration).After(now) {
			delete(n.writeLeaseRequestSentAt, seq)
		}
	}
}

func (n *managerNodeState) resetWriteLeaseRequests() {
	n.writeLeaseRequestSentAt = make(map[uint64]time.Time)
	n.lastAppliedLeaseSeq = 0
	n.pendingWitnessAck = nil
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
	current := liveness.CaptureAlive
	if m.node.liveness != nil {
		current = m.node.liveness.Load()
	}

	if req.NodeEpoch != m.node.nodeEpoch {
		log.Info("reject set node liveness request due to epoch mismatch",
			zap.Stringer("nodeID", m.nodeInfo.ID),
			zap.Uint64("localEpoch", m.node.nodeEpoch),
			zap.Uint64("requestEpoch", req.NodeEpoch))
		m.sendSetNodeLivenessResponse(current)
		return
	}

	target := m.fromNodeLivenessPB(req.Target)
	if m.node.liveness != nil && target > current && m.node.liveness.Store(target) {
		log.Info("node liveness transition applied",
			zap.Stringer("nodeID", m.nodeInfo.ID),
			zap.String("from", current.String()),
			zap.String("to", target.String()),
			zap.Uint64("epoch", m.node.nodeEpoch))
		current = target
		m.sendNodeHeartbeat(true)
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
	if m.node.tryUpdateDispatcherDrainTarget(target, req.TargetEpoch) {
		log.Info("dispatcher drain target updated",
			zap.Stringer("targetNodeID", target),
			zap.Uint64("targetEpoch", req.TargetEpoch))
		m.maintainers.applyDispatcherDrainTarget(target, req.TargetEpoch)
	}
	// A manager-level heartbeat is the authoritative acknowledgement of the
	// latest local drain snapshot, even when this request is a retry or stale
	// duplicate and no maintainer update is needed.
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
func (n *managerNodeState) tryUpdateDispatcherDrainTarget(target node.ID, epoch uint64) bool {
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
func (m *Manager) sendSetNodeLivenessResponse(applied liveness.Liveness) {
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
func (m *Manager) fromNodeLivenessPB(pbLiveness heartbeatpb.NodeLiveness) liveness.Liveness {
	switch pbLiveness {
	case heartbeatpb.NodeLiveness_DRAINING:
		return liveness.CaptureDraining
	case heartbeatpb.NodeLiveness_STOPPING:
		return liveness.CaptureStopping
	default:
		return liveness.CaptureAlive
	}
}

// toNodeLivenessPB converts the server-local liveness enum into the protocol enum.
func (m *Manager) toNodeLivenessPB(nodeLiveness liveness.Liveness) heartbeatpb.NodeLiveness {
	switch nodeLiveness {
	case liveness.CaptureDraining:
		return heartbeatpb.NodeLiveness_DRAINING
	case liveness.CaptureStopping:
		return heartbeatpb.NodeLiveness_STOPPING
	default:
		return heartbeatpb.NodeLiveness_ALIVE
	}
}
