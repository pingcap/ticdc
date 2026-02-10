package drain

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/nodeliveness"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

const (
	defaultResendInterval = time.Second
)

type nodeState struct {
	drainRequested bool

	lastSendDrain    time.Time
	lastSendStopping time.Time
}

// Controller drives node drain by sending SetNodeLiveness requests and computing remaining work.
//
// It is purely in-memory and relies on node-reported liveness to survive coordinator failover.
type Controller struct {
	mc messaging.MessageCenter

	livenessView *nodeliveness.View
	changefeedDB *changefeed.ChangefeedDB
	oc           *operator.Controller

	resendInterval time.Duration

	mu    sync.Mutex
	nodes map[node.ID]*nodeState
}

func NewController(
	mc messaging.MessageCenter,
	livenessView *nodeliveness.View,
	changefeedDB *changefeed.ChangefeedDB,
	oc *operator.Controller,
) *Controller {
	return &Controller{
		mc:             mc,
		livenessView:   livenessView,
		changefeedDB:   changefeedDB,
		oc:             oc,
		resendInterval: defaultResendInterval,
		nodes:          make(map[node.ID]*nodeState),
	}
}

// RequestDrain marks nodeID as requested to drain and sends SetNodeLiveness(DRAINING) eagerly.
func (c *Controller) RequestDrain(nodeID node.ID) {
	now := time.Now()

	c.mu.Lock()
	st := c.mustGetStateLocked(nodeID)
	st.drainRequested = true
	c.mu.Unlock()

	c.sendSetNodeLiveness(nodeID, heartbeatpb.NodeLiveness_DRAINING, now)
	c.mu.Lock()
	st.lastSendDrain = now
	c.mu.Unlock()
}

// Remaining returns the current drain remaining for nodeID.
func (c *Controller) Remaining(nodeID node.ID) int {
	return c.remaining(nodeID, time.Now())
}

func (c *Controller) remaining(nodeID node.ID, now time.Time) int {
	state := nodeliveness.StateAlive
	if c.livenessView != nil {
		state = c.livenessView.GetState(nodeID, now)
	}

	maintainersOnTarget := 0
	if c.changefeedDB != nil {
		maintainersOnTarget = len(c.changefeedDB.GetByNodeID(nodeID))
	}
	inflightOps := 0
	if c.oc != nil {
		inflightOps = c.oc.CountOperatorsInvolvingNode(nodeID)
	}
	remaining := maxInt(maintainersOnTarget, inflightOps)

	drainingObserved := state == nodeliveness.StateDraining || state == nodeliveness.StateStopping
	stoppingObserved := state == nodeliveness.StateStopping

	// Safety rules: never return 0 before drain is truly proven complete.
	if state == nodeliveness.StateUnknown {
		remaining = maxInt(remaining, 1)
	}
	if !drainingObserved {
		remaining = maxInt(remaining, 1)
	}

	if stoppingObserved && maintainersOnTarget == 0 && inflightOps == 0 {
		return 0
	}
	if remaining == 0 {
		return 1
	}
	return remaining
}

// Execute implements threadpool.Task.
func (c *Controller) Execute() time.Time {
	now := time.Now()
	c.tick(now)
	return now.Add(c.resendInterval)
}

func (c *Controller) tick(now time.Time) {
	if c.livenessView == nil {
		return
	}

	// Ensure draining/stopping nodes are tracked so drain can continue after coordinator failover.
	for _, id := range c.livenessView.GetNodesByState(nodeliveness.StateDraining, now) {
		c.mu.Lock()
		c.mustGetStateLocked(id)
		c.mu.Unlock()
	}
	for _, id := range c.livenessView.GetNodesByState(nodeliveness.StateStopping, now) {
		c.mu.Lock()
		c.mustGetStateLocked(id)
		c.mu.Unlock()
	}

	c.mu.Lock()
	ids := make([]node.ID, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	c.mu.Unlock()

	for _, id := range ids {
		c.tickNode(id, now)
	}
}

func (c *Controller) tickNode(nodeID node.ID, now time.Time) {
	state := c.livenessView.GetState(nodeID, now)

	c.mu.Lock()
	st := c.nodes[nodeID]
	requested := st != nil && st.drainRequested
	lastSendDrain := time.Time{}
	lastSendStopping := time.Time{}
	if st != nil {
		lastSendDrain = st.lastSendDrain
		lastSendStopping = st.lastSendStopping
	}
	c.mu.Unlock()

	drainingObserved := state == nodeliveness.StateDraining || state == nodeliveness.StateStopping
	stoppingObserved := state == nodeliveness.StateStopping

	if requested && !drainingObserved && now.Sub(lastSendDrain) >= c.resendInterval {
		c.sendSetNodeLiveness(nodeID, heartbeatpb.NodeLiveness_DRAINING, now)
		c.mu.Lock()
		if st := c.nodes[nodeID]; st != nil {
			st.lastSendDrain = now
		}
		c.mu.Unlock()
	}

	if stoppingObserved || !drainingObserved {
		return
	}

	maintainersOnTarget := 0
	if c.changefeedDB != nil {
		maintainersOnTarget = len(c.changefeedDB.GetByNodeID(nodeID))
	}
	inflightOps := 0
	if c.oc != nil {
		inflightOps = c.oc.CountOperatorsInvolvingNode(nodeID)
	}
	if maintainersOnTarget != 0 || inflightOps != 0 {
		return
	}
	if now.Sub(lastSendStopping) < c.resendInterval {
		return
	}

	c.sendSetNodeLiveness(nodeID, heartbeatpb.NodeLiveness_STOPPING, now)
	c.mu.Lock()
	if st := c.nodes[nodeID]; st != nil {
		st.lastSendStopping = now
	}
	c.mu.Unlock()
}

func (c *Controller) mustGetStateLocked(id node.ID) *nodeState {
	st, ok := c.nodes[id]
	if !ok {
		st = &nodeState{}
		c.nodes[id] = st
	}
	return st
}

func (c *Controller) sendSetNodeLiveness(nodeID node.ID, target heartbeatpb.NodeLiveness, now time.Time) {
	epoch := uint64(0)
	if c.livenessView != nil {
		if e, ok := c.livenessView.GetNodeEpoch(nodeID); ok {
			epoch = e
		}
	}

	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    target,
		NodeEpoch: epoch,
	}
	msg := messaging.NewSingleTargetMessage(nodeID, messaging.MaintainerManagerTopic, req)
	if err := c.mc.SendCommand(msg); err != nil {
		log.Warn("send set node liveness request failed",
			zap.Stringer("target", nodeID),
			zap.String("liveness", target.String()),
			zap.Error(err))
		return
	}
	log.Info("send set node liveness request",
		zap.Stringer("target", nodeID),
		zap.String("liveness", target.String()),
		zap.Uint64("nodeEpoch", epoch))
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
