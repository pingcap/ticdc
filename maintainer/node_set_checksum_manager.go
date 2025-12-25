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

type nodeSetChecksumCaptureState struct {
	seq        uint64                // Latest update sequence sent to this capture.
	ackedSeq   uint64                // Highest acknowledged sequence from this capture.
	checksum   set_checksum.Checksum // Expected set checksum on this capture.
	lastSendAt time.Time             // Last send time for resend throttling.

	lastObservedState heartbeatpb.ChecksumState // Latest checksum state observed from heartbeat.
	nonOKSince        time.Time                 // Start time of continuous non-OK duration.
	lastWarnAt        time.Time                 // Last warning time for log throttling.
}

type nodeSetChecksumState struct {
	captures         map[node.ID]*nodeSetChecksumCaptureState
	dirtyCaptures    map[node.ID]struct{}
	dispatcherToNode map[common.DispatcherID]node.ID
}

func newNodeSetChecksumState(captureCap int) nodeSetChecksumState {
	return nodeSetChecksumState{
		captures:         make(map[node.ID]*nodeSetChecksumCaptureState, captureCap),
		dirtyCaptures:    make(map[node.ID]struct{}, captureCap),
		dispatcherToNode: make(map[common.DispatcherID]node.ID),
	}
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
	mc           messaging.MessageCenter

	state nodeSetChecksumState
}

func newNodeSetChecksumManager(
	changefeedID common.ChangeFeedID,
	epoch uint64,
	mode int64,
	mc messaging.MessageCenter,
) *nodeSetChecksumManager {
	return &nodeSetChecksumManager{
		changefeedID: changefeedID,
		epoch:        epoch,
		mode:         mode,
		mc:           mc,
		state:        newNodeSetChecksumState(0),
	}
}

// ResetAndSendFull resets all internal states and sends full checksums to all captures.
//
// It is typically used after an epoch change, so old acknowledgements become invalid.
func (m *nodeSetChecksumManager) ResetAndSendFull(
	epoch uint64,
	nodes []node.ID,
	expected map[node.ID][]common.DispatcherID,
) {
	now := time.Now()

	m.mu.Lock()
	m.epoch = epoch
	m.state = newNodeSetChecksumState(len(nodes))

	msgs := make([]*messaging.TargetMessage, 0, len(nodes))
	for _, capture := range nodes {
		m.state.captures[capture] = &nodeSetChecksumCaptureState{
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

	m.sendMessages(msgs)
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

	ensureState := func(capture node.ID) *nodeSetChecksumCaptureState {
		state, ok := m.state.captures[capture]
		if !ok {
			state = &nodeSetChecksumCaptureState{}
			m.state.captures[capture] = state
		}
		return state
	}

	changedCaptures := make([]node.ID, 0, 2)
	markChanged := func(id node.ID) {
		for _, existing := range changedCaptures {
			if existing == id {
				return
			}
		}
		changedCaptures = append(changedCaptures, id)
	}

	for _, id := range remove {
		oldCapture, ok := m.state.dispatcherToNode[id]
		if !ok || oldCapture != capture {
			continue
		}
		state := ensureState(capture)
		state.checksum.Remove(id)
		delete(m.state.dispatcherToNode, id)
		markChanged(capture)
	}

	for _, id := range add {
		oldCapture, exists := m.state.dispatcherToNode[id]
		if exists && oldCapture == capture {
			continue
		}
		if exists && oldCapture != "" && oldCapture != capture {
			state := ensureState(oldCapture)
			state.checksum.Remove(id)
			markChanged(oldCapture)
		}

		state := ensureState(capture)
		state.checksum.Add(id)
		m.state.dispatcherToNode[id] = capture
		markChanged(capture)
	}

	for _, capture := range changedCaptures {
		state := ensureState(capture)
		state.seq++
		m.state.dirtyCaptures[capture] = struct{}{}
	}
	m.mu.Unlock()
}

// FlushDirty sends the latest checksum update for each dirty capture.
//
// This method is designed to be called periodically (for example, every 100ms) to coalesce
// frequent ApplyDelta calls into fewer update messages.
func (m *nodeSetChecksumManager) FlushDirty() {
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

	m.sendMessages(msgs)
}

// HandleAck updates the acknowledged sequence for the given capture.
//
// Acknowledgements from previous epochs are ignored.
func (m *nodeSetChecksumManager) HandleAck(from node.ID, ack *heartbeatpb.DispatcherSetChecksumAck) {
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

// ResendPending resends the last unacknowledged update if it has been pending for long enough.
func (m *nodeSetChecksumManager) ResendPending(resendInterval time.Duration) {
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

	m.sendMessages(msgs)
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
	const (
		warnAfter    = 2 * time.Minute
		warnInterval = 2 * time.Minute
	)

	type warnInfo struct {
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
	if state == heartbeatpb.ChecksumState_OK {
		captureState.lastObservedState = state
		captureState.nonOKSince = time.Time{}
		captureState.lastWarnAt = time.Time{}
		m.mu.Unlock()
		return
	}

	needResetTimer := captureState.lastObservedState == heartbeatpb.ChecksumState_OK || captureState.lastObservedState != state
	if needResetTimer || captureState.nonOKSince.IsZero() {
		captureState.nonOKSince = now
		captureState.lastWarnAt = time.Time{}
	}

	captureState.lastObservedState = state
	duration := now.Sub(captureState.nonOKSince)
	shouldWarn := duration >= warnAfter
	if shouldWarn && !captureState.lastWarnAt.IsZero() && now.Sub(captureState.lastWarnAt) < warnInterval {
		shouldWarn = false
	}
	if shouldWarn {
		captureState.lastWarnAt = now
	}
	warn := warnInfo{
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
	if warn.state == heartbeatpb.ChecksumState_UNINITIALIZED {
		stateStr = "uninitialized"
	}
	log.Warn("node set checksum state not ok for a long time",
		zap.Stringer("changefeedID", warn.changefeed),
		zap.String("capture", warn.capture.String()),
		zap.String("mode", common.StringMode(m.mode)),
		zap.String("state", stateStr),
		zap.Duration("duration", warn.duration),
		zap.Uint64("epoch", warn.epoch),
		zap.Uint64("expectedSeq", warn.seq),
		zap.Uint64("ackedSeq", warn.ackedSeq),
		zap.Time("lastSendAt", warn.lastSend),
	)
}

// buildUpdateMessage builds a checksum update command for one capture.
func (m *nodeSetChecksumManager) buildUpdateMessage(
	to node.ID,
	seq uint64,
	checksum set_checksum.Checksum,
) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(to, messaging.DispatcherManagerManagerTopic, &heartbeatpb.DispatcherSetChecksumUpdate{
		ChangefeedID: m.changefeedID.ToPB(),
		Epoch:        m.epoch,
		Mode:         m.mode,
		Seq:          seq,
		Checksum:     checksum.ToPB(),
	})
}

// sendMessages sends checksum updates to DispatcherManager by best-effort.
func (m *nodeSetChecksumManager) sendMessages(msgs []*messaging.TargetMessage) {
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		if err := m.mc.SendCommand(msg); err != nil {
			log.Debug("failed to send node set checksum message",
				zap.Stringer("changefeedID", m.changefeedID),
				zap.Any("msg", msg),
				zap.Error(err),
			)
		}
	}
}
