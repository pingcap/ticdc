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
	"go.uber.org/zap"
)

const (
	setLivenessRetryInterval = time.Second
)

type drainNodeState struct {
	drainRequested bool

	drainingObserved bool
	stoppingObserved bool

	lastDrainSendAt time.Time
	lastStopSendAt  time.Time
}

// DrainController drives node drain via SetNodeLiveness requests.
//
// It is coordinator-local state and is intentionally not persisted.
type DrainController struct {
	mu sync.Mutex

	now func() time.Time

	mc           messaging.MessageCenter
	livenessView *NodeLivenessView
	changefeedDB *changefeed.ChangefeedDB
	operatorCtl  *operator.Controller

	nodes map[node.ID]*drainNodeState
}

func NewDrainController(
	mc messaging.MessageCenter,
	livenessView *NodeLivenessView,
	changefeedDB *changefeed.ChangefeedDB,
	operatorCtl *operator.Controller,
) *DrainController {
	return &DrainController{
		now:          time.Now,
		mc:           mc,
		livenessView: livenessView,
		changefeedDB: changefeedDB,
		operatorCtl:  operatorCtl,
		nodes:        make(map[node.ID]*drainNodeState),
	}
}

func (d *DrainController) ObserveNodeHeartbeat(nodeID node.ID, hb *heartbeatpb.NodeHeartbeat) {
	if hb == nil {
		return
	}
	d.observeLiveness(nodeID, hb.Liveness)
}

func (d *DrainController) ObserveSetNodeLivenessResponse(nodeID node.ID, resp *heartbeatpb.SetNodeLivenessResponse) {
	if resp == nil {
		return
	}
	d.observeLiveness(nodeID, resp.Applied)
}

func (d *DrainController) observeLiveness(nodeID node.ID, liveness heartbeatpb.NodeLiveness) {
	d.mu.Lock()
	defer d.mu.Unlock()

	state, ok := d.nodes[nodeID]
	if !ok {
		return
	}

	switch liveness {
	case heartbeatpb.NodeLiveness_DRAINING:
		state.drainingObserved = true
	case heartbeatpb.NodeLiveness_STOPPING:
		state.drainingObserved = true
		state.stoppingObserved = true
	}
}

// DrainNode requests the given node to be drained and returns the current remaining work.
//
// The returned value is designed to be used by the v1 drain API:
// - It must not return 0 until the drain is truly complete.
func (d *DrainController) DrainNode(target node.ID) int {
	d.mu.Lock()
	state := d.nodes[target]
	if state == nil {
		state = &drainNodeState{}
		d.nodes[target] = state
	}
	state.drainRequested = true
	d.mu.Unlock()

	d.trySendSetLiveness(target)
	return d.remaining(target)
}

func (d *DrainController) Tick() {
	d.mu.Lock()
	targets := make([]node.ID, 0, len(d.nodes))
	for id, st := range d.nodes {
		if st.drainRequested {
			targets = append(targets, id)
		}
	}
	d.mu.Unlock()

	for _, target := range targets {
		d.trySendSetLiveness(target)
	}
}

func (d *DrainController) trySendSetLiveness(target node.ID) {
	now := d.now()

	d.mu.Lock()
	state := d.nodes[target]
	if state == nil || !state.drainRequested {
		d.mu.Unlock()
		return
	}

	if !state.drainingObserved {
		if now.Sub(state.lastDrainSendAt) < setLivenessRetryInterval {
			d.mu.Unlock()
			return
		}
		state.lastDrainSendAt = now
		d.mu.Unlock()
		d.sendSetNodeLiveness(target, heartbeatpb.NodeLiveness_DRAINING)
		return
	}

	if state.stoppingObserved {
		d.mu.Unlock()
		return
	}

	if !d.canPromoteToStopping(target) {
		d.mu.Unlock()
		return
	}

	if now.Sub(state.lastStopSendAt) < setLivenessRetryInterval {
		d.mu.Unlock()
		return
	}
	state.lastStopSendAt = now
	d.mu.Unlock()

	d.sendSetNodeLiveness(target, heartbeatpb.NodeLiveness_STOPPING)
}

func (d *DrainController) canPromoteToStopping(target node.ID) bool {
	maintainersOnTarget := len(d.changefeedDB.GetByNodeID(target))
	inflightOps := d.operatorCtl.CountOperatorsInvolvingNode(target)
	return maintainersOnTarget == 0 && inflightOps == 0
}

func (d *DrainController) sendSetNodeLiveness(target node.ID, liveness heartbeatpb.NodeLiveness) {
	epoch, _ := d.livenessView.GetNodeEpoch(target)
	req := &heartbeatpb.SetNodeLivenessRequest{
		Target:    liveness,
		NodeEpoch: epoch,
	}
	msg := messaging.NewSingleTargetMessage(target, messaging.MaintainerManagerTopic, req)
	if err := d.mc.SendCommand(msg); err != nil {
		log.Warn("send set node liveness request failed",
			zap.Stringer("target", target),
			zap.Int32("liveness", int32(liveness)),
			zap.Error(err))
	}
}

func (d *DrainController) remaining(target node.ID) int {
	maintainersOnTarget := len(d.changefeedDB.GetByNodeID(target))
	inflightOps := d.operatorCtl.CountOperatorsInvolvingNode(target)

	remaining := maintainersOnTarget
	if inflightOps > remaining {
		remaining = inflightOps
	}

	d.mu.Lock()
	state := d.nodes[target]
	drainingObserved := state != nil && state.drainingObserved
	stoppingObserved := state != nil && state.stoppingObserved
	d.mu.Unlock()

	// If the node is UNKNOWN, graceful drain completion cannot be proven.
	if d.livenessView.IsUnknown(target) {
		if remaining == 0 {
			return 1
		}
		return remaining
	}

	// Must not return 0 until DRAINING is observed.
	if !drainingObserved && remaining == 0 {
		return 1
	}

	// Return 0 only after STOPPING is observed and there is no remaining work.
	if stoppingObserved && remaining == 0 {
		return 0
	}
	if remaining == 0 {
		return 1
	}
	return remaining
}
