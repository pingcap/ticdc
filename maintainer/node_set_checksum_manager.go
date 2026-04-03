// Copyright 2025 PingCAP, Inc.
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
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/set_checksum"
	"go.uber.org/zap"
)

const (
	// If the heartbeat has been in a non-match state for more than the `warnAfter` time period,
	// and more than the `warnInterval` time period has elapsed since the last `heartbeat unmatch` warning log was printed,
	// then print the `warn` log.
	warnAfter    = 2 * time.Minute
	warnInterval = 2 * time.Minute
	// resendInterval is the minimum interval between resending unacknowledged updates to a node.
	// It can be overridden in tests to disable throttling.
	resendInterval time.Duration = 1 * time.Second
)

type nodeSetChecksumState struct {
	// nodes tracks per-node checksum and resend/ack progress.
	// dirtyNodes is a set of nodes that need a checksum update message.
	// dispatcherToNode records the expected owner node for each dispatcher ID.
	nodes            map[node.ID]*nodeChecksumState
	dirtyNodes       map[node.ID]struct{}
	dispatcherToNode map[common.DispatcherID]node.ID
}

func newNodeSetChecksumState(nodeCap int) nodeSetChecksumState {
	return nodeSetChecksumState{
		nodes:            make(map[node.ID]*nodeChecksumState, nodeCap),
		dirtyNodes:       make(map[node.ID]struct{}, nodeCap),
		dispatcherToNode: make(map[common.DispatcherID]node.ID),
	}
}

// nodeChecksumState tracks expected checksum progress and heartbeat observation for a node.
//
// Invariant: for any dispatcher ID that exists in nodeSetChecksumState.dispatcherToNode,
// the corresponding nodeChecksumState.checksum must include that dispatcher ID, and it must
// not be included in any other node's checksum.
type nodeChecksumState struct {
	seq        uint64                // Latest update sequence sent to this node.
	ackedSeq   uint64                // Highest acknowledged sequence from this node.
	checksum   set_checksum.Checksum // Expected set checksum on this node.
	lastSendAt time.Time             // Last send time for resend throttling.

	lastObservedState heartbeatpb.ChecksumState // Latest checksum state observed from heartbeat.
	nonMatchSince     time.Time                 // Start time of continuous non-MATCH duration.
	lastWarnAt        time.Time                 // Last warning time for log throttling.
}

// observe checksum state in heartbeat for log print if necessary
func (s *nodeChecksumState) observeHeartbeat(
	now time.Time,
	state heartbeatpb.ChecksumState,
	warnAfter time.Duration,
	warnInterval time.Duration,
) (shouldWarn bool, duration time.Duration) {
	if state == heartbeatpb.ChecksumState_MATCH {
		s.lastObservedState = state
		s.nonMatchSince = time.Time{}
		s.lastWarnAt = time.Time{}
		return false, 0
	}

	if s.lastObservedState == heartbeatpb.ChecksumState_MATCH ||
		s.lastObservedState != state ||
		s.nonMatchSince.IsZero() {
		s.nonMatchSince = now
		s.lastWarnAt = time.Time{}
	}

	s.lastObservedState = state

	duration = now.Sub(s.nonMatchSince)
	shouldWarn = duration >= warnAfter
	if shouldWarn && !s.lastWarnAt.IsZero() && now.Sub(s.lastWarnAt) < warnInterval {
		shouldWarn = false
	}
	if shouldWarn {
		s.lastWarnAt = now
	}
	return shouldWarn, duration
}

// nodeSetChecksumManager maintains maintainer-side expected dispatcher IDs for a single mode.
//
// It computes an incremental checksum for each node and sends it to DispatcherManager via
// MessageCenter. DispatcherManager compares the expected checksum with the runtime dispatcher set
// and reports the checksum state in heartbeat.
//
// The manager also tracks per-node update sequence and acknowledgements to support best-effort
// resending when updates are missed.
type nodeSetChecksumManager struct {
	mu sync.Mutex

	changefeedID common.ChangeFeedID
	epoch        uint64 // maintainer's epoch
	mode         int64  // default or redo mode.

	state nodeSetChecksumState
}

func newNodeSetChecksumManager(
	changefeedID common.ChangeFeedID,
	epoch uint64,
	mode int64,
) *nodeSetChecksumManager {
	return &nodeSetChecksumManager{
		changefeedID: changefeedID,
		epoch:        epoch,
		mode:         mode,
		state:        newNodeSetChecksumState(0),
	}
}

// Note: it's not thread safe. caller must hold m.mu
func (m *nodeSetChecksumManager) getOrCreateNodeState(node node.ID) *nodeChecksumState {
	state, ok := m.state.nodes[node]
	if ok {
		return state
	}
	state = &nodeChecksumState{}
	m.state.nodes[node] = state
	return state
}

// ResetAndSendFull resets all internal states and builds full checksum updates for all nodes.
//
// It is typically used after an epoch change, so old acknowledgements become invalid.
func (m *nodeSetChecksumManager) ResetAndSendFull(
	epoch uint64,
	nodes []node.ID,
	expected map[node.ID][]common.DispatcherID,
) []*messaging.TargetMessage {
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch = epoch
	m.state = newNodeSetChecksumState(len(nodes))

	msgs := make([]*messaging.TargetMessage, 0, len(nodes))
	for _, node := range nodes {
		m.state.nodes[node] = &nodeChecksumState{
			seq:        1,
			ackedSeq:   0,
			lastSendAt: now,
		}
	}

	m.applyExpectedSet(expected)

	for _, node := range nodes {
		state := m.state.nodes[node]
		msgs = append(msgs, m.buildUpdateMessage(node, state.seq, state.checksum))
	}
	m.mu.Unlock()

	// Return built messages to the caller (Maintainer), which owns message sending.
	return msgs
}

// Note: it's not thread safe. caller must hold m.mu
func (m *nodeSetChecksumManager) applyExpectedSet(expected map[node.ID][]common.DispatcherID) {
	for node, ids := range expected {
		state, ok := m.state.nodes[node]
		if !ok {
			continue
		}
		for _, id := range ids {
			oldNode, exists := m.state.dispatcherToNode[id]
			if exists {
				if oldNode == node {
					log.Warn("dispatcher already exists in expected set, ignore it",
						zap.Stringer("changefeedID", m.changefeedID),
						zap.String("dispatcherID", id.String()),
						zap.String("node", node.String()),
						zap.String("mode", common.StringMode(m.mode)),
					)
					continue
				}
				log.Warn("dispatcher exists in another node, override expected node",
					zap.Stringer("changefeedID", m.changefeedID),
					zap.String("dispatcherID", id.String()),
					zap.String("oldNode", oldNode.String()),
					zap.String("newNode", node.String()),
					zap.String("mode", common.StringMode(m.mode)),
				)
				if oldState, ok := m.state.nodes[oldNode]; ok {
					oldState.checksum.Remove(id)
				}
			}
			m.state.dispatcherToNode[id] = node
			state.checksum.Add(id)
		}
	}
}

// ApplyDelta applies incremental dispatcher set changes for a node.
//
// It updates checksums of affected nodes (including the previous owner when a dispatcher moves)
// and marks them dirty for periodic flush.
func (m *nodeSetChecksumManager) ApplyDelta(nodeID node.ID, add []common.DispatcherID, remove []common.DispatcherID) {
	if len(add) == 0 && len(remove) == 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	changed := make(map[node.ID]*nodeChecksumState, 2)

	for _, id := range remove {
		oldNode, ok := m.state.dispatcherToNode[id]
		if !ok || oldNode != nodeID {
			continue
		}
		state := m.getOrCreateNodeState(nodeID)
		state.checksum.Remove(id)
		delete(m.state.dispatcherToNode, id)
		changed[nodeID] = state
	}

	for _, id := range add {
		oldNode, exists := m.state.dispatcherToNode[id]
		if exists && oldNode == nodeID {
			continue
		}
		if exists && !oldNode.IsEmpty() && oldNode != nodeID {
			oldState, ok := m.state.nodes[oldNode]
			if ok {
				oldState.checksum.Remove(id)
				changed[oldNode] = oldState
			}
		}

		state := m.getOrCreateNodeState(nodeID)
		state.checksum.Add(id)
		m.state.dispatcherToNode[id] = nodeID
		changed[nodeID] = state
	}

	for node, state := range changed {
		state.seq++
		m.state.dirtyNodes[node] = struct{}{}
	}
}

// FlushDirty builds the latest checksum update for each dirty node.
//
// This method is designed to be called periodically (for example, every 100ms) to coalesce
// frequent ApplyDelta calls into fewer update messages.
func (m *nodeSetChecksumManager) FlushDirty() []*messaging.TargetMessage {
	now := time.Now()

	m.mu.Lock()
	msgs := make([]*messaging.TargetMessage, 0, len(m.state.dirtyNodes))
	for node := range m.state.dirtyNodes {
		state, ok := m.state.nodes[node]
		if !ok {
			delete(m.state.dirtyNodes, node)
			continue
		}
		state.lastSendAt = now
		msgs = append(msgs, m.buildUpdateMessage(node, state.seq, state.checksum))
		delete(m.state.dirtyNodes, node)
	}
	m.mu.Unlock()

	// Return built messages to the caller (Maintainer), which owns message sending.
	return msgs
}

// HandleAck updates the acknowledged sequence for the given node.
//
// Acknowledgements from previous epochs are ignored.
func (m *nodeSetChecksumManager) HandleAck(from node.ID, ack *heartbeatpb.DispatcherSetChecksumAckResponse) {
	if ack == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if ack.Epoch != m.epoch {
		return
	}
	if ack.Mode != m.mode {
		return
	}

	state, ok := m.state.nodes[from]
	if !ok {
		return
	}

	if ack.Seq > state.seq {
		log.Info("The ack seq value did not meet expectations.", zap.Uint64("ackSeq", ack.Seq), zap.Uint64("state.seq", state.seq))
		return
	}

	if ack.Seq > state.ackedSeq {
		state.ackedSeq = ack.Seq
	}
}

// FlushAndResendIfNeeded flushes dirty updates and resends unacknowledged updates when needed.
func (m *nodeSetChecksumManager) FlushAndResendIfNeeded() []*messaging.TargetMessage {
	msgs := m.FlushDirty()
	msgs = append(msgs, m.ResendPending()...)
	return msgs
}

// ResendPending builds the last unacknowledged update if it has been pending for long enough.
func (m *nodeSetChecksumManager) ResendPending() []*messaging.TargetMessage {
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()
	msgs := make([]*messaging.TargetMessage, 0)

	for node, state := range m.state.nodes {
		if state.seq == 0 || state.ackedSeq >= state.seq {
			continue
		}
		if !state.lastSendAt.IsZero() && now.Sub(state.lastSendAt) < resendInterval {
			continue
		}
		state.lastSendAt = now
		msgs = append(msgs, m.buildUpdateMessage(node, state.seq, state.checksum))
	}

	// Return built messages to the caller (Maintainer), which owns message sending.
	return msgs
}

// RemoveNodes removes all states of the specified nodes and their dispatcher mappings.
func (m *nodeSetChecksumManager) RemoveNodes(nodes []node.ID) {
	if len(nodes) == 0 {
		return
	}

	removedSet := make(map[node.ID]struct{}, len(nodes))
	for _, node := range nodes {
		removedSet[node] = struct{}{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for node := range removedSet {
		delete(m.state.nodes, node)
		delete(m.state.dirtyNodes, node)
	}
	for id, node := range m.state.dispatcherToNode {
		if _, ok := removedSet[node]; ok {
			delete(m.state.dispatcherToNode, id)
		}
	}
}

// ObserveHeartbeat consumes checksum states from heartbeat and emits warning logs.
//
// A warning is triggered when a node stays in a non-OK state for warnAfter, and the log
// emission is throttled by warnInterval.
func (m *nodeSetChecksumManager) ObserveHeartbeat(from node.ID, state heartbeatpb.ChecksumState) {
	now := time.Now()

	type warnSnapshot struct {
		state      heartbeatpb.ChecksumState
		duration   time.Duration
		epoch      uint64
		seq        uint64
		ackedSeq   uint64
		lastSend   time.Time
		changefeed common.ChangeFeedID
		node       node.ID
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	nodeState, ok := m.state.nodes[from]
	if !ok {
		return
	}
	shouldWarn, duration := nodeState.observeHeartbeat(now, state, warnAfter, warnInterval)
	snapshot := warnSnapshot{
		state:      state,
		duration:   duration,
		epoch:      m.epoch,
		seq:        nodeState.seq,
		ackedSeq:   nodeState.ackedSeq,
		lastSend:   nodeState.lastSendAt,
		changefeed: m.changefeedID,
		node:       from,
	}
	if !shouldWarn {
		return
	}

	log.Warn("node set checksum state not ok for a long time",
		zap.Stringer("changefeedID", snapshot.changefeed),
		zap.String("node", snapshot.node.String()),
		zap.String("mode", common.StringMode(m.mode)),
		zap.String("state", snapshot.state.String()),
		zap.Duration("duration", snapshot.duration),
		zap.Uint64("epoch", snapshot.epoch),
		zap.Uint64("expectedSeq", snapshot.seq),
		zap.Uint64("ackedSeq", snapshot.ackedSeq),
		zap.Time("lastSendAt", snapshot.lastSend),
	)
}

// buildUpdateMessage builds a checksum update request for one node.
func (m *nodeSetChecksumManager) buildUpdateMessage(
	to node.ID,
	seq uint64,
	checksum set_checksum.Checksum,
) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(to, messaging.DispatcherManagerManagerTopic, &heartbeatpb.DispatcherSetChecksumUpdateRequest{
		ChangefeedID: m.changefeedID.ToPB(),
		Epoch:        m.epoch,
		Mode:         m.mode,
		Seq:          seq,
		Checksum:     checksum.ToPB(),
	})
}
