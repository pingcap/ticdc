package drain

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/nodeliveness"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type nodeState struct {
	drainRequested   bool
	drainingObserved bool
	stoppingObserved bool

	lastDrainCmdSentAt time.Time
	lastStopCmdSentAt  time.Time
}

// Controller manages node drain progression by sending SetNodeLiveness commands and tracking observations.
//
// It is in-memory only. Observations come from either:
// - NodeHeartbeat, or
// - SetNodeLivenessResponse.
type Controller struct {
	mu sync.Mutex

	mc             messaging.MessageCenter
	livenessView   *nodeliveness.View
	resendInterval time.Duration

	nodes map[node.ID]*nodeState
}

func NewController(
	mc messaging.MessageCenter,
	livenessView *nodeliveness.View,
	resendInterval time.Duration,
) *Controller {
	return &Controller{
		mc:             mc,
		livenessView:   livenessView,
		resendInterval: resendInterval,
		nodes:          make(map[node.ID]*nodeState),
	}
}

func (c *Controller) ensureNodeStateLocked(nodeID node.ID) *nodeState {
	st, ok := c.nodes[nodeID]
	if !ok {
		st = &nodeState{}
		c.nodes[nodeID] = st
	}
	return st
}

func (c *Controller) RequestDrain(nodeID node.ID, now time.Time) {
	c.mu.Lock()
	st := c.ensureNodeStateLocked(nodeID)
	st.drainRequested = true
	c.mu.Unlock()

	c.maybeSendDrainCommand(nodeID, now)
}

func (c *Controller) ObserveHeartbeat(nodeID node.ID, hb *heartbeatpb.NodeHeartbeat) {
	if hb == nil {
		return
	}
	c.mu.Lock()
	st := c.ensureNodeStateLocked(nodeID)
	switch hb.Liveness {
	case heartbeatpb.NodeLiveness_DRAINING:
		st.drainRequested = true
		st.drainingObserved = true
	case heartbeatpb.NodeLiveness_STOPPING:
		st.drainRequested = true
		st.drainingObserved = true
		st.stoppingObserved = true
	}
	c.mu.Unlock()
}

func (c *Controller) ObserveSetNodeLivenessResponse(nodeID node.ID, resp *heartbeatpb.SetNodeLivenessResponse) {
	if resp == nil {
		return
	}
	c.mu.Lock()
	st := c.ensureNodeStateLocked(nodeID)
	switch resp.Applied {
	case heartbeatpb.NodeLiveness_DRAINING:
		st.drainRequested = true
		st.drainingObserved = true
	case heartbeatpb.NodeLiveness_STOPPING:
		st.drainRequested = true
		st.drainingObserved = true
		st.stoppingObserved = true
	}
	c.mu.Unlock()
}

func (c *Controller) GetStatus(nodeID node.ID) (drainRequested, drainingObserved, stoppingObserved bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st, ok := c.nodes[nodeID]
	if !ok {
		return false, false, false
	}
	return st.drainRequested, st.drainingObserved, st.stoppingObserved
}

// Tick drives liveness commands:
// - Request DRAINING until observed
// - Once readyToStop and DRAINING observed, request STOPPING until observed
func (c *Controller) Tick(now time.Time, readyToStop func(node.ID) bool) {
	c.mu.Lock()
	nodeIDs := make([]node.ID, 0, len(c.nodes))
	for id, st := range c.nodes {
		if st.drainRequested {
			nodeIDs = append(nodeIDs, id)
		}
	}
	c.mu.Unlock()

	for _, nodeID := range nodeIDs {
		drainRequested, drainingObserved, stoppingObserved := c.GetStatus(nodeID)
		if !drainRequested {
			continue
		}

		if !drainingObserved {
			c.maybeSendDrainCommand(nodeID, now)
			continue
		}

		if !stoppingObserved && readyToStop != nil && readyToStop(nodeID) {
			c.maybeSendStopCommand(nodeID, now)
		}
	}
}

func (c *Controller) maybeSendDrainCommand(nodeID node.ID, now time.Time) {
	c.mu.Lock()
	st := c.ensureNodeStateLocked(nodeID)
	if st.drainingObserved {
		c.mu.Unlock()
		return
	}
	if !st.lastDrainCmdSentAt.IsZero() && now.Sub(st.lastDrainCmdSentAt) < c.resendInterval {
		c.mu.Unlock()
		return
	}
	st.lastDrainCmdSentAt = now
	c.mu.Unlock()

	c.sendSetNodeLiveness(nodeID, heartbeatpb.NodeLiveness_DRAINING)
}

func (c *Controller) maybeSendStopCommand(nodeID node.ID, now time.Time) {
	c.mu.Lock()
	st := c.ensureNodeStateLocked(nodeID)
	if st.stoppingObserved {
		c.mu.Unlock()
		return
	}
	if !st.lastStopCmdSentAt.IsZero() && now.Sub(st.lastStopCmdSentAt) < c.resendInterval {
		c.mu.Unlock()
		return
	}
	st.lastStopCmdSentAt = now
	c.mu.Unlock()

	c.sendSetNodeLiveness(nodeID, heartbeatpb.NodeLiveness_STOPPING)
}

func (c *Controller) sendSetNodeLiveness(nodeID node.ID, target heartbeatpb.NodeLiveness) {
	var epoch uint64
	if c.livenessView != nil {
		if e, ok := c.livenessView.GetNodeEpoch(nodeID); ok {
			epoch = e
		}
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
