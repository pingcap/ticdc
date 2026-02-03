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

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
)

type derivedNodeLiveness int8

const (
	derivedNodeLivenessAlive derivedNodeLiveness = iota
	derivedNodeLivenessDraining
	derivedNodeLivenessStopping
	derivedNodeLivenessUnknown
)

type nodeLivenessEntry struct {
	lastSeen time.Time

	nodeEpoch uint64
	liveness  heartbeatpb.NodeLiveness

	everSeenHeartbeat bool
}

// NodeLivenessView is an in-memory view of node liveness on the coordinator.
//
// The liveness is node-reported via NodeHeartbeat and can be derived as UNKNOWN
// when the heartbeat has timed out.
//
// Compatibility note:
// A node that has never sent a NodeHeartbeat is treated as ALIVE (not UNKNOWN).
type NodeLivenessView struct {
	mu sync.RWMutex

	now func() time.Time
	ttl time.Duration

	nodes map[node.ID]*nodeLivenessEntry
}

func NewNodeLivenessView() *NodeLivenessView {
	return &NodeLivenessView{
		now:   time.Now,
		ttl:   30 * time.Second,
		nodes: make(map[node.ID]*nodeLivenessEntry),
	}
}

func (v *NodeLivenessView) UpdateFromHeartbeat(nodeID node.ID, hb *heartbeatpb.NodeHeartbeat) {
	if hb == nil {
		return
	}
	now := v.now()

	v.mu.Lock()
	defer v.mu.Unlock()

	entry, ok := v.nodes[nodeID]
	if !ok {
		entry = &nodeLivenessEntry{}
		v.nodes[nodeID] = entry
	}

	entry.lastSeen = now
	entry.nodeEpoch = hb.NodeEpoch
	entry.liveness = hb.Liveness
	entry.everSeenHeartbeat = true
}

func (v *NodeLivenessView) UpdateFromSetLivenessResponse(nodeID node.ID, resp *heartbeatpb.SetNodeLivenessResponse) {
	if resp == nil {
		return
	}
	now := v.now()

	v.mu.Lock()
	defer v.mu.Unlock()

	entry, ok := v.nodes[nodeID]
	if !ok {
		entry = &nodeLivenessEntry{}
		v.nodes[nodeID] = entry
	}

	entry.lastSeen = now
	entry.nodeEpoch = resp.NodeEpoch
	entry.liveness = resp.Applied
	// Treat the response as evidence that the node speaks the new liveness protocol.
	entry.everSeenHeartbeat = true
}

func (v *NodeLivenessView) derivedLiveness(nodeID node.ID) derivedNodeLiveness {
	v.mu.RLock()
	entry, ok := v.nodes[nodeID]
	if !ok || !entry.everSeenHeartbeat {
		v.mu.RUnlock()
		return derivedNodeLivenessAlive
	}
	lastSeen := entry.lastSeen
	reported := entry.liveness
	ttl := v.ttl
	now := v.now()
	v.mu.RUnlock()

	if now.Sub(lastSeen) > ttl {
		return derivedNodeLivenessUnknown
	}

	switch reported {
	case heartbeatpb.NodeLiveness_DRAINING:
		return derivedNodeLivenessDraining
	case heartbeatpb.NodeLiveness_STOPPING:
		return derivedNodeLivenessStopping
	default:
		return derivedNodeLivenessAlive
	}
}

func (v *NodeLivenessView) IsSchedulableDestination(nodeID node.ID) bool {
	return v.derivedLiveness(nodeID) == derivedNodeLivenessAlive
}

func (v *NodeLivenessView) IsDraining(nodeID node.ID) bool {
	return v.derivedLiveness(nodeID) == derivedNodeLivenessDraining
}

func (v *NodeLivenessView) IsStopping(nodeID node.ID) bool {
	return v.derivedLiveness(nodeID) == derivedNodeLivenessStopping
}

func (v *NodeLivenessView) IsUnknown(nodeID node.ID) bool {
	return v.derivedLiveness(nodeID) == derivedNodeLivenessUnknown
}

func (v *NodeLivenessView) GetNodeEpoch(nodeID node.ID) (uint64, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	entry, ok := v.nodes[nodeID]
	if !ok || !entry.everSeenHeartbeat {
		return 0, false
	}
	return entry.nodeEpoch, true
}
