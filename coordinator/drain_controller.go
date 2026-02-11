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

package coordinator

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

const (
	nodeHeartbeatTTL           = 30 * time.Second
	nodeLivenessResendInterval = time.Second
)

type nodeLiveness int32

const (
	nodeLivenessAlive nodeLiveness = iota
	nodeLivenessDraining
	nodeLivenessStopping
	nodeLivenessUnknown
)

func (n nodeLiveness) String() string {
	switch n {
	case nodeLivenessAlive:
		return "alive"
	case nodeLivenessDraining:
		return "draining"
	case nodeLivenessStopping:
		return "stopping"
	case nodeLivenessUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

type nodeLivenessStatus struct {
	nodeID            node.ID
	nodeEpoch         uint64
	liveness          nodeLiveness
	lastSeen          time.Time
	everSeenHeartbeat bool
}

type nodeLivenessView struct {
	mu    sync.RWMutex
	nodes map[node.ID]*nodeLivenessStatus
	ttl   time.Duration
}

func newNodeLivenessView(ttl time.Duration) *nodeLivenessView {
	if ttl <= 0 {
		ttl = nodeHeartbeatTTL
	}
	return &nodeLivenessView{
		nodes: make(map[node.ID]*nodeLivenessStatus),
		ttl:   ttl,
	}
}

func (v *nodeLivenessView) getStatus(nodeID node.ID, now time.Time) nodeLivenessStatus {
	v.mu.RLock()
	defer v.mu.RUnlock()

	status := nodeLivenessStatus{nodeID: nodeID, liveness: nodeLivenessAlive}
	if s, ok := v.nodes[nodeID]; ok {
		status = *s
		status.liveness = deriveLivenessWithTTL(s, now, v.ttl)
	}
	return status
}

func (v *nodeLivenessView) getSchedulableDestNodes(aliveNodes map[node.ID]*node.Info, now time.Time) map[node.ID]*node.Info {
	ret := make(map[node.ID]*node.Info, len(aliveNodes))
	for id, info := range aliveNodes {
		if v.getStatus(id, now).liveness == nodeLivenessAlive {
			ret[id] = info
		}
	}
	return ret
}

func (v *nodeLivenessView) getSchedulableDestNodeIDs(aliveNodes map[node.ID]*node.Info, now time.Time) []node.ID {
	ids := make([]node.ID, 0, len(aliveNodes))
	for id := range aliveNodes {
		if v.getStatus(id, now).liveness == nodeLivenessAlive {
			ids = append(ids, id)
		}
	}
	return ids
}

func (v *nodeLivenessView) getNodesByLiveness(target nodeLiveness, now time.Time) []node.ID {
	v.mu.RLock()
	defer v.mu.RUnlock()

	ids := make([]node.ID, 0)
	for id, status := range v.nodes {
		if deriveLivenessWithTTL(status, now, v.ttl) == target {
			ids = append(ids, id)
		}
	}
	return ids
}

func (v *nodeLivenessView) applyHeartbeat(nodeID node.ID, heartbeat *heartbeatpb.NodeHeartbeat, now time.Time) nodeLivenessStatus {
	liveness, ok := heartbeatNodeLivenessToInternal(heartbeat.Liveness)
	if !ok {
		log.Warn("ignore node heartbeat with invalid liveness",
			zap.Stringer("nodeID", nodeID),
			zap.Int32("liveness", int32(heartbeat.Liveness)))
		return v.getStatus(nodeID, now)
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	s := v.nodes[nodeID]
	if s == nil || heartbeat.NodeEpoch != s.nodeEpoch {
		s = &nodeLivenessStatus{nodeID: nodeID, nodeEpoch: heartbeat.NodeEpoch}
		v.nodes[nodeID] = s
	}
	s.lastSeen = now
	s.everSeenHeartbeat = true
	if liveness < s.liveness {
		log.Warn("ignore stale node liveness downgrade from heartbeat",
			zap.Stringer("nodeID", nodeID),
			zap.String("current", s.liveness.String()),
			zap.String("reported", liveness.String()),
			zap.Uint64("nodeEpoch", heartbeat.NodeEpoch))
	} else if liveness > s.liveness {
		log.Info("observed node liveness upgrade from heartbeat",
			zap.Stringer("nodeID", nodeID),
			zap.String("from", s.liveness.String()),
			zap.String("to", liveness.String()),
			zap.Uint64("nodeEpoch", heartbeat.NodeEpoch))
		s.liveness = liveness
	}

	ret := *s
	ret.liveness = deriveLivenessWithTTL(s, now, v.ttl)
	return ret
}

func deriveLivenessWithTTL(s *nodeLivenessStatus, now time.Time, ttl time.Duration) nodeLiveness {
	if s == nil {
		return nodeLivenessAlive
	}
	if s.everSeenHeartbeat && now.Sub(s.lastSeen) > ttl {
		return nodeLivenessUnknown
	}
	return s.liveness
}

type drainNodeStatus struct {
	drainRequested     bool
	drainingObserved   bool
	stoppingObserved   bool
	lastDrainRequestAt time.Time
	lastStopRequestAt  time.Time
}

type drainRemaining struct {
	targetNode                 node.ID
	maintainersOnTarget        int
	inflightOpsInvolvingTarget int
	drainingObserved           bool
	stoppingObserved           bool
	nodeLiveness               nodeLiveness
}

func (r drainRemaining) remaining() int {
	remaining := max(r.maintainersOnTarget, r.inflightOpsInvolvingTarget)
	if !r.drainingObserved {
		if remaining == 0 {
			return 1
		}
		return remaining
	}
	if r.nodeLiveness == nodeLivenessUnknown {
		if remaining == 0 {
			return 1
		}
		return remaining
	}
	if r.stoppingObserved && remaining == 0 {
		return 0
	}
	if remaining == 0 {
		return 1
	}
	return remaining
}

type drainSummary struct {
	remaining             int
	maintainersOnTarget   int
	inflightOperatorCount int
	drainingObserved      bool
	stoppingObserved      bool
	nodeLiveness          nodeLiveness
}

func (c *drainController) summarizeDrain(targetNode node.ID, now time.Time) drainSummary {
	r := c.remaining(targetNode, now)
	return drainSummary{
		remaining:             r.remaining(),
		maintainersOnTarget:   r.maintainersOnTarget,
		inflightOperatorCount: r.inflightOpsInvolvingTarget,
		drainingObserved:      r.drainingObserved,
		stoppingObserved:      r.stoppingObserved,
		nodeLiveness:          r.nodeLiveness,
	}
}

type drainController struct {
	mu sync.Mutex

	nodeView           *nodeLivenessView
	nodeManager        *watcher.NodeManager
	messageCenter      messaging.MessageCenter
	operatorController *operator.Controller
	changefeedDB       *changefeed.ChangefeedDB
	selfNodeID         node.ID

	drainNodes map[node.ID]*drainNodeStatus
}

// controllerDrainView exposes only drain-related scheduler view methods.
type controllerDrainView struct {
	controller *drainController
}

func (v *controllerDrainView) GetDrainingNodes(now time.Time) []node.ID {
	return v.controller.getDrainingNodes(now)
}

func (v *controllerDrainView) GetSchedulableDestNodes(now time.Time) map[node.ID]*node.Info {
	if v == nil || v.controller == nil {
		return nil
	}
	return v.controller.getSchedulableDestNodes(v.controller.nodeManager.GetAliveNodes(), now)
}

func newControllerDrainView(controller *drainController) *controllerDrainView {
	return &controllerDrainView{controller: controller}
}

func newDrainController(
	selfNodeID node.ID,
	nodeView *nodeLivenessView,
	nodeManager *watcher.NodeManager,
	messageCenter messaging.MessageCenter,
	operatorController *operator.Controller,
	changefeedDB *changefeed.ChangefeedDB,
) *drainController {
	return &drainController{
		nodeView:           nodeView,
		nodeManager:        nodeManager,
		messageCenter:      messageCenter,
		operatorController: operatorController,
		changefeedDB:       changefeedDB,
		selfNodeID:         selfNodeID,
		drainNodes:         make(map[node.ID]*drainNodeStatus),
	}
}

func (c *drainController) requestDrain(nodeID node.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	status := c.ensureDrainNodeLocked(nodeID)
	status.drainRequested = true
	observed := c.nodeView.getStatus(nodeID, time.Now())
	if observed.liveness == nodeLivenessDraining {
		status.drainingObserved = true
	}
	if observed.liveness == nodeLivenessStopping {
		status.drainingObserved = true
		status.stoppingObserved = true
	}
}

func (c *drainController) markDrainingObserved(nodeID node.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	status := c.ensureDrainNodeLocked(nodeID)
	status.drainingObserved = true
}

func (c *drainController) markStoppingObserved(nodeID node.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	status := c.ensureDrainNodeLocked(nodeID)
	status.drainingObserved = true
	status.stoppingObserved = true
}

func (c *drainController) ensureDrainNodeLocked(nodeID node.ID) *drainNodeStatus {
	status := c.drainNodes[nodeID]
	if status == nil {
		status = &drainNodeStatus{}
		c.drainNodes[nodeID] = status
	}
	return status
}

func (c *drainController) handleNodeHeartbeat(nodeID node.ID, heartbeat *heartbeatpb.NodeHeartbeat, now time.Time) {
	status := c.nodeView.applyHeartbeat(nodeID, heartbeat, now)
	if status.liveness == nodeLivenessDraining {
		c.markDrainingObserved(nodeID)
	}
	if status.liveness == nodeLivenessStopping {
		c.markStoppingObserved(nodeID)
	}
}

func (c *drainController) handleSetNodeLivenessResponse(nodeID node.ID, resp *heartbeatpb.SetNodeLivenessResponse) {
	liveness, ok := heartbeatNodeLivenessToInternal(resp.Applied)
	if !ok {
		return
	}
	if liveness == nodeLivenessDraining {
		c.markDrainingObserved(nodeID)
	}
	if liveness == nodeLivenessStopping {
		c.markStoppingObserved(nodeID)
	}
}

func (c *drainController) getDrainingNodes(now time.Time) []node.ID {
	c.mu.Lock()
	defer c.mu.Unlock()

	viewDraining := c.nodeView.getNodesByLiveness(nodeLivenessDraining, now)
	viewStopping := c.nodeView.getNodesByLiveness(nodeLivenessStopping, now)
	for _, nodeID := range viewDraining {
		status := c.ensureDrainNodeLocked(nodeID)
		status.drainRequested = true
		status.drainingObserved = true
	}
	for _, nodeID := range viewStopping {
		status := c.ensureDrainNodeLocked(nodeID)
		status.drainRequested = true
		status.drainingObserved = true
		status.stoppingObserved = true
	}

	ret := make([]node.ID, 0)
	for nodeID, status := range c.drainNodes {
		if !status.drainRequested {
			continue
		}
		nodeStatus := c.nodeView.getStatus(nodeID, now)
		if nodeStatus.liveness == nodeLivenessDraining || nodeStatus.liveness == nodeLivenessStopping {
			ret = append(ret, nodeID)
		}
	}
	return ret
}

func (c *drainController) listDrainNodes() []node.ID {
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := make([]node.ID, 0, len(c.drainNodes))
	for nodeID, status := range c.drainNodes {
		if status.drainRequested {
			ret = append(ret, nodeID)
		}
	}
	return ret
}

func (c *drainController) shouldSendDrainRequest(nodeID node.ID, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	status := c.ensureDrainNodeLocked(nodeID)
	if !status.drainRequested || status.drainingObserved {
		return false
	}
	if status.lastDrainRequestAt.IsZero() || now.Sub(status.lastDrainRequestAt) >= nodeLivenessResendInterval {
		status.lastDrainRequestAt = now
		return true
	}
	return false
}

func (c *drainController) sendSetNodeLivenessRequest(targetNode node.ID, liveness heartbeatpb.NodeLiveness) {
	nodeStatus := c.nodeView.getStatus(targetNode, time.Now())
	err := c.messageCenter.SendCommand(messaging.NewSingleTargetMessage(
		targetNode,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.SetNodeLivenessRequest{Target: liveness, NodeEpoch: nodeStatus.nodeEpoch},
	))
	if err != nil {
		log.Warn("failed to send set node liveness request",
			zap.Stringer("targetNode", targetNode),
			zap.Uint64("nodeEpoch", nodeStatus.nodeEpoch),
			zap.Int32("targetLiveness", int32(liveness)),
			zap.Error(err))
	}
}

func (c *drainController) shouldSendStopRequest(nodeID node.ID, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	status := c.ensureDrainNodeLocked(nodeID)
	if !status.drainRequested || !status.drainingObserved || status.stoppingObserved {
		return false
	}
	if status.lastStopRequestAt.IsZero() || now.Sub(status.lastStopRequestAt) >= nodeLivenessResendInterval {
		status.lastStopRequestAt = now
		return true
	}
	return false
}

func (c *drainController) remaining(nodeID node.ID, now time.Time) drainRemaining {
	c.mu.Lock()
	status := c.ensureDrainNodeLocked(nodeID)
	drainingObserved := status.drainingObserved
	stoppingObserved := status.stoppingObserved
	c.mu.Unlock()

	maintainersOnTarget := len(c.changefeedDB.GetByNodeID(nodeID))
	inflightOpsInvolvingTarget := c.operatorController.CountOperatorsByNode(nodeID)
	nodeStatus := c.nodeView.getStatus(nodeID, now)
	return drainRemaining{
		targetNode:                 nodeID,
		maintainersOnTarget:        maintainersOnTarget,
		inflightOpsInvolvingTarget: inflightOpsInvolvingTarget,
		drainingObserved:           drainingObserved,
		stoppingObserved:           stoppingObserved,
		nodeLiveness:               nodeStatus.liveness,
	}
}

func (c *drainController) canPromoteToStopping(nodeID node.ID, now time.Time) bool {
	r := c.remaining(nodeID, now)
	return r.drainingObserved && r.maintainersOnTarget == 0 && r.inflightOpsInvolvingTarget == 0
}

func (c *drainController) getSchedulableDestNodes(aliveNodes map[node.ID]*node.Info, now time.Time) map[node.ID]*node.Info {
	return c.nodeView.getSchedulableDestNodes(aliveNodes, now)
}

func (c *drainController) getSchedulableDestNodeIDs(aliveNodes map[node.ID]*node.Info, now time.Time) []node.ID {
	return c.nodeView.getSchedulableDestNodeIDs(aliveNodes, now)
}

func heartbeatNodeLivenessToInternal(liveness heartbeatpb.NodeLiveness) (nodeLiveness, bool) {
	switch liveness {
	case heartbeatpb.NodeLiveness_ALIVE:
		return nodeLivenessAlive, true
	case heartbeatpb.NodeLiveness_DRAINING:
		return nodeLivenessDraining, true
	case heartbeatpb.NodeLiveness_STOPPING:
		return nodeLivenessStopping, true
	default:
		return nodeLivenessAlive, false
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
