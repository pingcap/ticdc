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
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/messaging"
	"go.uber.org/zap"
)

const nodeHeartbeatInterval = 5 * time.Second

// managerNodeState owns node-scoped state shared by all local maintainers.
type managerNodeState struct {
	liveness  *api.Liveness
	nodeEpoch uint64

	// lastNodeHeartbeatSentAt throttles periodic node heartbeats while still
	// allowing forced heartbeats for liveness transitions.
	lastNodeHeartbeatSentAt time.Time
}

func newManagerNodeState(liveness *api.Liveness) *managerNodeState {
	return &managerNodeState{
		liveness:  liveness,
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

	liveness := api.LivenessCaptureAlive
	if m.node.liveness != nil {
		liveness = m.node.liveness.Load()
	}
	target := m.newCoordinatorTopicMessage(&heartbeatpb.NodeHeartbeat{
		Liveness:  m.toNodeLivenessPB(liveness),
		NodeEpoch: m.node.nodeEpoch,
	})
	if err := m.mc.SendCommand(target); err != nil {
		log.Warn("send node heartbeat failed",
			zap.Stringer("from", m.nodeInfo.ID),
			zap.Stringer("target", target.To),
			zap.Error(err))
		return
	}
	m.node.lastNodeHeartbeatSentAt = now
}

func (m *Manager) onSetNodeLivenessRequest(msg *messaging.TargetMessage) {
	if m.coordinatorID != msg.From {
		log.Warn("ignore set node liveness request from non coordinator",
			zap.Stringer("from", msg.From),
			zap.Stringer("coordinatorID", m.coordinatorID))
		return
	}

	req := msg.Message[0].(*heartbeatpb.SetNodeLivenessRequest)
	current := api.LivenessCaptureAlive
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

func (m *Manager) sendSetNodeLivenessResponse(applied api.Liveness) {
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

func (m *Manager) fromNodeLivenessPB(liveness heartbeatpb.NodeLiveness) api.Liveness {
	switch liveness {
	case heartbeatpb.NodeLiveness_DRAINING:
		return api.LivenessCaptureDraining
	case heartbeatpb.NodeLiveness_STOPPING:
		return api.LivenessCaptureStopping
	default:
		return api.LivenessCaptureAlive
	}
}

func (m *Manager) toNodeLivenessPB(liveness api.Liveness) heartbeatpb.NodeLiveness {
	switch liveness {
	case api.LivenessCaptureDraining:
		return heartbeatpb.NodeLiveness_DRAINING
	case api.LivenessCaptureStopping:
		return heartbeatpb.NodeLiveness_STOPPING
	default:
		return heartbeatpb.NodeLiveness_ALIVE
	}
}
