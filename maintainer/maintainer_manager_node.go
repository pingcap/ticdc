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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/messaging"
	"go.uber.org/zap"
)

const nodeHeartbeatInterval = 5 * time.Second

// managerNodeState owns node-scoped state shared by all local maintainers.
type managerNodeState struct {
	liveness  *liveness.Liveness
	nodeEpoch uint64

	// lastNodeHeartbeatSentAt throttles periodic node heartbeats while still
	// allowing forced heartbeats for liveness transitions.
	lastNodeHeartbeatSentAt time.Time
}

func newManagerNodeState(nodeLiveness *liveness.Liveness) *managerNodeState {
	return &managerNodeState{
		liveness:  nodeLiveness,
		nodeEpoch: newNodeEpoch(),
	}
}

func newNodeEpoch() uint64 {
	nodeEpoch := uint64(time.Now().UnixNano())
	if nodeEpoch == 0 {
		return 1
	}
	return nodeEpoch
}

func (m *Manager) sendNodeHeartbeat(force bool) {
	if !m.isBootstrap() {
		return
	}

	now := time.Now()
	if !force && now.Sub(m.node.lastNodeHeartbeatSentAt) < nodeHeartbeatInterval {
		return
	}
	// Update before sending so a transient send failure will not cause
	// frequent retries and log spam on the 200ms tick.
	m.node.lastNodeHeartbeatSentAt = now

	currentLiveness := liveness.CaptureAlive
	if m.node.liveness != nil {
		currentLiveness = m.node.liveness.Load()
	}
	target := m.newCoordinatorTopicMessage(&heartbeatpb.NodeHeartbeat{
		Liveness:  m.toNodeLivenessPB(currentLiveness),
		NodeEpoch: m.node.nodeEpoch,
	})
	if err := m.mc.SendCommand(target); err != nil {
		log.Warn("send node heartbeat failed",
			zap.Stringer("from", m.nodeInfo.ID),
			zap.Stringer("target", target.To),
			zap.Error(err))
		return
	}
}

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

func (m *Manager) sendSetNodeLivenessResponse(applied liveness.Liveness) {
	target := m.newCoordinatorTopicMessage(&heartbeatpb.SetNodeLivenessResponse{
		Applied:   m.toNodeLivenessPB(applied),
		NodeEpoch: m.node.nodeEpoch,
	})
	if err := m.mc.SendCommand(target); err != nil {
		log.Warn("send set node liveness response failed",
			zap.Stringer("from", m.nodeInfo.ID),
			zap.Stringer("target", target.To),
			zap.Error(err))
	}
}

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
