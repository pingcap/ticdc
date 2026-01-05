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
	warnAfter    = 2 * time.Minute
	warnInterval = 2 * time.Minute
	// resendInterval is the minimum interval between resending unacknowledged updates to a capture.
	// It can be overridden in tests to disable throttling.
	resendInterval time.Duration = 1 * time.Second
)

type nodeSetChecksumState struct {
	// captures tracks per-capture checksum and resend/ack progress.
	// dirtyCaptures is a set of captures that need a checksum update message.
	// dispatcherToNode records the expected owner capture for each dispatcher ID.
	captures         map[node.ID]*nodeChecksumState
	dirtyCaptures    map[node.ID]struct{}
	dispatcherToNode map[common.DispatcherID]node.ID
}

func newNodeSetChecksumState(captureCap int) nodeSetChecksumState {
	return nodeSetChecksumState{
		captures:         make(map[node.ID]*nodeChecksumState, captureCap),
		dirtyCaptures:    make(map[node.ID]struct{}, captureCap),
		dispatcherToNode: make(map[common.DispatcherID]node.ID),
	}
}

// nodeChecksumState tracks expected checksum progress and heartbeat observation for a capture.
//
// Invariant: for any dispatcher ID that exists in nodeSetChecksumState.dispatcherToNode,
// the corresponding nodeChecksumState.checksum must include that dispatcher ID, and it must
// not be included in any other capture's checksum.
type nodeChecksumState struct {
	seq        uint64                // Latest update sequence sent to this capture.
	ackedSeq   uint64                // Highest acknowledged sequence from this capture.
	checksum   set_checksum.Checksum // Expected set checksum on this capture.
	lastSendAt time.Time             // Last send time for resend throttling.

	lastObservedState heartbeatpb.ChecksumState // Latest checksum state observed from heartbeat.
	nonOKSince        time.Time                 // Start time of continuous non-OK duration.
	lastWarnAt        time.Time                 // Last warning time for log throttling.
}

func (s *nodeChecksumState) observeHeartbeat(
	now time.Time,
	state heartbeatpb.ChecksumState,
	warnAfter time.Duration,
	warnInterval time.Duration,
) (shouldWarn bool, duration time.Duration) {
	if state == heartbeatpb.ChecksumState_MATCH {
		s.lastObservedState = state
		s.nonOKSince = time.Time{}
		s.lastWarnAt = time.Time{}
		return false, 0
	}

	needResetTimer := s.lastObservedState == heartbeatpb.ChecksumState_MATCH || s.lastObservedState != state
	if needResetTimer || s.nonOKSince.IsZero() {
		s.nonOKSince = now
		s.lastWarnAt = time.Time{}
	}

	s.lastObservedState = state

	duration = now.Sub(s.nonOKSince)
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
// It computes an incremental checksum for each capture and sends it to DispatcherManager via
// MessageCenter. DispatcherManager compares the expected checksum with the runtime dispatcher set
// and reports the checksum state in heartbeat.
//
// The manager also tracks per-capture update sequence and acknowledgements to support best-effort
// resending when updates are missed.
type nodeSetChecksumManager struct {
	mu sync.Mutex

	changefeedID common.ChangeFeedID
	epoch        uint64
	mode         int64

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

func (m *nodeSetChecksumManager) getOrCreateNodeStateLocked(capture node.ID) *nodeChecksumState {
	state, ok := m.state.captures[capture]
	if ok {
		return state
	}
	state = &nodeChecksumState{}
	m.state.captures[capture] = state
	return state
}

// ResetAndSendFull resets all internal states and builds full checksum updates for all captures.
//
// It is typically used after an epoch change, so old acknowledgements become invalid.
func (m *nodeSetChecksumManager) ResetAndSendFull(
	epoch uint64,
	nodes []node.ID,
	expected map[node.ID][]common.DispatcherID,
) []*messaging.TargetMessage {
	now := time.Now()

	m.mu.Lock()
	m.epoch = epoch
	m.state = newNodeSetChecksumState(len(nodes))

	msgs := make([]*messaging.TargetMessage, 0, len(nodes))
	for _, capture := range nodes {
		m.state.captures[capture] = &nodeChecksumState{
			seq:        1,
			ackedSeq:   0,
			lastSendAt: now,
		}
	}

	m.applyExpectedSet(expected)

	for _, capture := range nodes {
		state := m.state.captures[capture]
		msgs = append(msgs, m.buildUpdateMessage(capture, state.seq, state.checksum))
	}
	m.mu.Unlock()

	// Return built messages to the caller (Maintainer), which owns message sending.
	return msgs
}

func (m *nodeSetChecksumManager) applyExpectedSet(expected map[node.ID][]common.DispatcherID) {
	for capture, ids := range expected {
		state, ok := m.state.captures[capture]
		if !ok {
			continue
		}
		for _, id := range ids {
			oldCapture, exists := m.state.dispatcherToNode[id]
			if exists {
				if oldCapture == capture {
					log.Warn("dispatcher already exists in expected set, ignore it",
						zap.Stringer("changefeedID", m.changefeedID),
						zap.String("dispatcherID", id.String()),
						zap.String("capture", capture.String()),
						zap.String("mode", common.StringMode(m.mode)),
					)
					continue
				}
				log.Warn("dispatcher exists in another capture, override expected node",
					zap.Stringer("changefeedID", m.changefeedID),
					zap.String("dispatcherID", id.String()),
					zap.String("oldCapture", oldCapture.String()),
					zap.String("newCapture", capture.String()),
					zap.String("mode", common.StringMode(m.mode)),
				)
				if oldState, ok := m.state.captures[oldCapture]; ok {
					oldState.checksum.Remove(id)
				}
			}
			m.state.dispatcherToNode[id] = capture
			state.checksum.Add(id)
		}
	}
}

// ApplyDelta applies incremental dispatcher set changes for a capture.
//
// It updates checksums of affected captures (including the previous owner when a dispatcher moves)
// and marks them dirty for periodic flush.
func (m *nodeSetChecksumManager) ApplyDelta(capture node.ID, add []common.DispatcherID, remove []common.DispatcherID) {
	if len(add) == 0 && len(remove) == 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	changed := make(map[node.ID]*nodeChecksumState, 2)

	for _, id := range remove {
		oldCapture, ok := m.state.dispatcherToNode[id]
		if !ok || oldCapture != capture {
			continue
		}
		state := m.getOrCreateNodeStateLocked(capture)
		state.checksum.Remove(id)
		delete(m.state.dispatcherToNode, id)
		changed[capture] = state
	}

	for _, id := range add {
		oldCapture, exists := m.state.dispatcherToNode[id]
		if exists && oldCapture == capture {
			continue
		}
		if exists && oldCapture != "" && oldCapture != capture {
			oldState, ok := m.state.captures[oldCapture]
			if ok {
				oldState.checksum.Remove(id)
				changed[oldCapture] = oldState
			}
		}

		state := m.getOrCreateNodeStateLocked(capture)
		state.checksum.Add(id)
		m.state.dispatcherToNode[id] = capture
		changed[capture] = state
	}

	for capture, state := range changed {
		state.seq++
		m.state.dirtyCaptures[capture] = struct{}{}
	}
}

// FlushDirty builds the latest checksum update for each dirty capture.
//
// This method is designed to be called periodically (for example, every 100ms) to coalesce
// frequent ApplyDelta calls into fewer update messages.
func (m *nodeSetChecksumManager) FlushDirty() []*messaging.TargetMessage {
	now := time.Now()

	m.mu.Lock()
	msgs := make([]*messaging.TargetMessage, 0, len(m.state.dirtyCaptures))
	for capture := range m.state.dirtyCaptures {
		state, ok := m.state.captures[capture]
		if !ok {
			delete(m.state.dirtyCaptures, capture)
			continue
		}
		state.lastSendAt = now
		msgs = append(msgs, m.buildUpdateMessage(capture, state.seq, state.checksum))
		delete(m.state.dirtyCaptures, capture)
	}
	m.mu.Unlock()

	// Return built messages to the caller (Maintainer), which owns message sending.
	return msgs
}

// HandleAck updates the acknowledged sequence for the given capture.
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

	state, ok := m.state.captures[from]
	if !ok {
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
	msgs := make([]*messaging.TargetMessage, 0)

	for capture, state := range m.state.captures {
		if state.seq == 0 || state.ackedSeq >= state.seq {
			continue
		}
		if !state.lastSendAt.IsZero() && now.Sub(state.lastSendAt) < resendInterval {
			continue
		}
		state.lastSendAt = now
		msgs = append(msgs, m.buildUpdateMessage(capture, state.seq, state.checksum))
	}
	m.mu.Unlock()

	// Return built messages to the caller (Maintainer), which owns message sending.
	return msgs
}

// RemoveNodes removes all states of the specified captures and their dispatcher mappings.
func (m *nodeSetChecksumManager) RemoveNodes(nodes []node.ID) {
	if len(nodes) == 0 {
		return
	}

	removedSet := make(map[node.ID]struct{}, len(nodes))
	for _, capture := range nodes {
		removedSet[capture] = struct{}{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for capture := range removedSet {
		delete(m.state.captures, capture)
		delete(m.state.dirtyCaptures, capture)
	}
	for id, capture := range m.state.dispatcherToNode {
		if _, ok := removedSet[capture]; ok {
			delete(m.state.dispatcherToNode, id)
		}
	}
}

// ObserveHeartbeat consumes checksum states from heartbeat and emits warning logs.
//
// A warning is triggered when a capture stays in a non-OK state for warnAfter, and the log
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
		capture    node.ID
	}

	m.mu.Lock()
	captureState, ok := m.state.captures[from]
	if !ok {
		m.mu.Unlock()
		return
	}
	shouldWarn, duration := captureState.observeHeartbeat(now, state, warnAfter, warnInterval)
	snapshot := warnSnapshot{
		state:      state,
		duration:   duration,
		epoch:      m.epoch,
		seq:        captureState.seq,
		ackedSeq:   captureState.ackedSeq,
		lastSend:   captureState.lastSendAt,
		changefeed: m.changefeedID,
		capture:    from,
	}
	m.mu.Unlock()
	if !shouldWarn {
		return
	}

	stateStr := "mismatch"
	if snapshot.state == heartbeatpb.ChecksumState_UNINITIALIZED {
		stateStr = "uninitialized"
	}
	log.Warn("capture set checksum state not ok for a long time",
		zap.Stringer("changefeedID", snapshot.changefeed),
		zap.String("capture", snapshot.capture.String()),
		zap.String("mode", common.StringMode(m.mode)),
		zap.String("state", stateStr),
		zap.Duration("duration", snapshot.duration),
		zap.Uint64("epoch", snapshot.epoch),
		zap.Uint64("expectedSeq", snapshot.seq),
		zap.Uint64("ackedSeq", snapshot.ackedSeq),
		zap.Time("lastSendAt", snapshot.lastSend),
	)
}

// buildUpdateMessage builds a checksum update command for one capture.
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
