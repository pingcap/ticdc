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
	"bytes"
	"crypto/rand"
	"sort"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/writelease"
)

const witnessNonceSize = 16

type captureLeaseNodeState struct {
	nodeEpoch      uint64
	lastRequestSeq uint64
}

type pendingWitnessChallenge struct {
	selfNodeEpoch    uint64
	selfRequestSeq   uint64
	witnessNodeID    node.ID
	witnessNodeEpoch uint64
	nonce            []byte
	expiresAt        time.Time
}

type captureWriteLeaseController struct {
	coordinatorVersion int64
	selfNodeID         node.ID
	now                func() time.Time
	nonce              func([]byte) (int, error)

	nodes            map[node.ID]*captureLeaseNodeState
	pendingWitness   *pendingWitnessChallenge
	nextWitnessIndex int
}

func newCaptureWriteLeaseController(version int64, selfNodeID node.ID) *captureWriteLeaseController {
	return &captureWriteLeaseController{
		coordinatorVersion: version,
		selfNodeID:         selfNodeID,
		now:                time.Now,
		nonce:              rand.Read,
		nodes:              make(map[node.ID]*captureLeaseNodeState),
	}
}

func (c *captureWriteLeaseController) handleHeartbeat(
	from node.ID,
	heartbeat *heartbeatpb.NodeHeartbeat,
	initializedNodes []node.ID,
) []*messaging.TargetMessage {
	if heartbeat.GetWriteLeaseProtocolVersion() != heartbeatpb.CurrentWriteLeaseProtocolVersion ||
		heartbeat.GetWriteLeaseRequestSeq() == 0 || heartbeat.GetNodeEpoch() == 0 ||
		heartbeat.GetLiveness() == heartbeatpb.NodeLiveness_STOPPING {
		return nil
	}

	state := c.nodes[from]
	if state == nil || state.nodeEpoch != heartbeat.GetNodeEpoch() {
		state = &captureLeaseNodeState{nodeEpoch: heartbeat.GetNodeEpoch()}
		c.nodes[from] = state
		if c.pendingWitness != nil &&
			(c.pendingWitness.witnessNodeID == from || from == c.selfNodeID) {
			c.pendingWitness = nil
		}
	}
	if heartbeat.GetWriteLeaseRequestSeq() <= state.lastRequestSeq {
		return nil
	}
	state.lastRequestSeq = heartbeat.GetWriteLeaseRequestSeq()

	messages := c.handleWitnessAck(from, heartbeat)
	if from != c.selfNodeID {
		return append(messages, c.newGrant(from, heartbeat.GetNodeEpoch(), heartbeat.GetWriteLeaseRequestSeq()))
	}

	return append(messages, c.handleSelfHeartbeat(heartbeat, initializedNodes)...)
}

func (c *captureWriteLeaseController) handleWitnessAck(
	from node.ID,
	heartbeat *heartbeatpb.NodeHeartbeat,
) []*messaging.TargetMessage {
	ack := heartbeat.GetWriteLeaseWitnessAck()
	pending := c.pendingWitness
	if ack == nil || pending == nil {
		return nil
	}
	if !c.now().Before(pending.expiresAt) {
		c.pendingWitness = nil
		return nil
	}
	if from != pending.witnessNodeID ||
		heartbeat.GetNodeEpoch() != pending.witnessNodeEpoch ||
		ack.GetCoordinatorVersion() != c.coordinatorVersion ||
		ack.GetCoordinatorNodeEpoch() != pending.selfNodeEpoch ||
		ack.GetSelfRequestSeq() != pending.selfRequestSeq ||
		ack.GetWitnessNodeEpoch() != pending.witnessNodeEpoch ||
		!bytes.Equal(ack.GetNonce(), pending.nonce) {
		return nil
	}

	selfState := c.nodes[c.selfNodeID]
	if selfState == nil || selfState.nodeEpoch != pending.selfNodeEpoch {
		c.pendingWitness = nil
		return nil
	}
	c.pendingWitness = nil
	return []*messaging.TargetMessage{
		c.newGrant(c.selfNodeID, pending.selfNodeEpoch, pending.selfRequestSeq),
	}
}

func (c *captureWriteLeaseController) handleSelfHeartbeat(
	heartbeat *heartbeatpb.NodeHeartbeat,
	initializedNodes []node.ID,
) []*messaging.TargetMessage {
	remoteExists := false
	witnesses := make([]node.ID, 0, len(initializedNodes))
	for _, id := range initializedNodes {
		if id == c.selfNodeID {
			continue
		}
		remoteExists = true
		if state := c.nodes[id]; state != nil && state.nodeEpoch != 0 {
			witnesses = append(witnesses, id)
		}
	}
	if !remoteExists {
		return []*messaging.TargetMessage{
			c.newGrant(c.selfNodeID, heartbeat.GetNodeEpoch(), heartbeat.GetWriteLeaseRequestSeq()),
		}
	}
	if len(witnesses) == 0 {
		return nil
	}

	if c.pendingWitness != nil {
		if c.now().Before(c.pendingWitness.expiresAt) {
			return nil
		}
		c.pendingWitness = nil
	}

	sort.Slice(witnesses, func(i, j int) bool { return witnesses[i] < witnesses[j] })
	witness := witnesses[c.nextWitnessIndex%len(witnesses)]
	c.nextWitnessIndex++
	witnessEpoch := c.nodes[witness].nodeEpoch
	nonce := make([]byte, witnessNonceSize)
	if _, err := c.nonce(nonce); err != nil {
		return nil
	}

	c.pendingWitness = &pendingWitnessChallenge{
		selfNodeEpoch:    heartbeat.GetNodeEpoch(),
		selfRequestSeq:   heartbeat.GetWriteLeaseRequestSeq(),
		witnessNodeID:    witness,
		witnessNodeEpoch: witnessEpoch,
		nonce:            nonce,
		expiresAt:        c.now().Add(writelease.P2PLeaseDuration),
	}
	response := &heartbeatpb.NodeHeartbeatResponse{
		CoordinatorVersion: c.coordinatorVersion,
		TargetNodeEpoch:    witnessEpoch,
		WitnessChallenge: &heartbeatpb.WriteLeaseWitnessChallenge{
			CoordinatorVersion:   c.coordinatorVersion,
			CoordinatorNodeEpoch: heartbeat.GetNodeEpoch(),
			SelfRequestSeq:       heartbeat.GetWriteLeaseRequestSeq(),
			WitnessNodeEpoch:     witnessEpoch,
			Nonce:                append([]byte(nil), nonce...),
		},
	}
	return []*messaging.TargetMessage{
		messaging.NewSingleTargetMessage(witness, messaging.MaintainerManagerTopic, response),
	}
}

func (c *captureWriteLeaseController) newGrant(
	target node.ID,
	targetNodeEpoch uint64,
	requestSeq uint64,
) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(
		target,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.NodeHeartbeatResponse{
			CoordinatorVersion: c.coordinatorVersion,
			TargetNodeEpoch:    targetNodeEpoch,
			RequestSeq:         requestSeq,
			LeaseDurationMs:    uint64(writelease.P2PLeaseDuration.Milliseconds()),
		},
	)
}

func (c *captureWriteLeaseController) removeNode(id node.ID) {
	delete(c.nodes, id)
	if c.pendingWitness != nil &&
		(c.pendingWitness.witnessNodeID == id || id == c.selfNodeID) {
		c.pendingWitness = nil
	}
}
