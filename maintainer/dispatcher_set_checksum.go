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
	"go.uber.org/zap"
)

type dispatcherSetChecksumFingerprint struct {
	count   uint64
	xorHigh uint64
	xorLow  uint64
	sumHigh uint64
	sumLow  uint64
}

func (f *dispatcherSetChecksumFingerprint) add(id common.DispatcherID) {
	f.count++
	f.xorHigh ^= id.High
	f.xorLow ^= id.Low
	f.sumHigh += id.High
	f.sumLow += id.Low
}

func (f *dispatcherSetChecksumFingerprint) remove(id common.DispatcherID) {
	f.count--
	f.xorHigh ^= id.High
	f.xorLow ^= id.Low
	f.sumHigh -= id.High
	f.sumLow -= id.Low
}

func (f dispatcherSetChecksumFingerprint) toPB() *heartbeatpb.DispatcherSetChecksumFingerprint {
	return &heartbeatpb.DispatcherSetChecksumFingerprint{
		Count:   f.count,
		XorHigh: f.xorHigh,
		XorLow:  f.xorLow,
		SumHigh: f.sumHigh,
		SumLow:  f.sumLow,
	}
}

type dispatcherSetChecksumNodeState struct {
	seq         uint64
	ackedSeq    uint64
	fingerprint dispatcherSetChecksumFingerprint
	lastSendAt  time.Time

	lastObservedState heartbeatpb.ChecksumState
	nonOKSince        time.Time
	lastWarnAt        time.Time
}

type dispatcherSetChecksumManager struct {
	mu sync.Mutex

	changefeedID common.ChangeFeedID
	epoch        uint64
	redoEnabled  bool
	mc           messaging.MessageCenter

	defaultNodes map[node.ID]*dispatcherSetChecksumNodeState
	redoNodes    map[node.ID]*dispatcherSetChecksumNodeState

	defaultDispatcherToNode map[common.DispatcherID]node.ID
	redoDispatcherToNode    map[common.DispatcherID]node.ID
}

func newDispatcherSetChecksumManager(
	changefeedID common.ChangeFeedID,
	epoch uint64,
	redoEnabled bool,
	mc messaging.MessageCenter,
) *dispatcherSetChecksumManager {
	return &dispatcherSetChecksumManager{
		changefeedID:            changefeedID,
		epoch:                   epoch,
		redoEnabled:             redoEnabled,
		mc:                      mc,
		defaultNodes:            make(map[node.ID]*dispatcherSetChecksumNodeState),
		redoNodes:               make(map[node.ID]*dispatcherSetChecksumNodeState),
		defaultDispatcherToNode: make(map[common.DispatcherID]node.ID),
		redoDispatcherToNode:    make(map[common.DispatcherID]node.ID),
	}
}

func (m *dispatcherSetChecksumManager) ResetAndSendFull(
	epoch uint64,
	nodes []node.ID,
	defaultExpected map[node.ID][]common.DispatcherID,
	redoExpected map[node.ID][]common.DispatcherID,
) {
	now := time.Now()

	m.mu.Lock()
	m.epoch = epoch
	m.defaultNodes = make(map[node.ID]*dispatcherSetChecksumNodeState, len(nodes))
	m.redoNodes = make(map[node.ID]*dispatcherSetChecksumNodeState, len(nodes))
	m.defaultDispatcherToNode = make(map[common.DispatcherID]node.ID)
	m.redoDispatcherToNode = make(map[common.DispatcherID]node.ID)

	msgs := make([]*messaging.TargetMessage, 0, len(nodes)*2)
	for _, capture := range nodes {
		state := &dispatcherSetChecksumNodeState{
			seq:        1,
			ackedSeq:   0,
			lastSendAt: now,
		}
		m.defaultNodes[capture] = state

		if !m.redoEnabled {
			continue
		}

		redoState := &dispatcherSetChecksumNodeState{
			seq:        1,
			ackedSeq:   0,
			lastSendAt: now,
		}
		m.redoNodes[capture] = redoState
	}

	for capture, ids := range defaultExpected {
		state, ok := m.defaultNodes[capture]
		if !ok {
			continue
		}
		for _, id := range ids {
			oldCapture, exists := m.defaultDispatcherToNode[id]
			if exists {
				if oldCapture == capture {
					log.Warn("dispatcher already exists in expected set, ignore it",
						zap.Stringer("changefeedID", m.changefeedID),
						zap.String("dispatcherID", id.String()),
						zap.String("capture", capture.String()),
						zap.String("mode", common.StringMode(common.DefaultMode)),
					)
					continue
				}
				log.Warn("dispatcher exists in another capture, override expected node",
					zap.Stringer("changefeedID", m.changefeedID),
					zap.String("dispatcherID", id.String()),
					zap.String("oldCapture", oldCapture.String()),
					zap.String("newCapture", capture.String()),
					zap.String("mode", common.StringMode(common.DefaultMode)),
				)
				if oldState, ok := m.defaultNodes[oldCapture]; ok {
					oldState.fingerprint.remove(id)
				}
			}
			m.defaultDispatcherToNode[id] = capture
			state.fingerprint.add(id)
		}
	}

	if m.redoEnabled {
		for capture, ids := range redoExpected {
			state, ok := m.redoNodes[capture]
			if !ok {
				continue
			}
			for _, id := range ids {
				oldCapture, exists := m.redoDispatcherToNode[id]
				if exists {
					if oldCapture == capture {
						log.Warn("dispatcher already exists in expected set, ignore it",
							zap.Stringer("changefeedID", m.changefeedID),
							zap.String("dispatcherID", id.String()),
							zap.String("capture", capture.String()),
							zap.String("mode", common.StringMode(common.RedoMode)),
						)
						continue
					}
					log.Warn("dispatcher exists in another capture, override expected node",
						zap.Stringer("changefeedID", m.changefeedID),
						zap.String("dispatcherID", id.String()),
						zap.String("oldCapture", oldCapture.String()),
						zap.String("newCapture", capture.String()),
						zap.String("mode", common.StringMode(common.RedoMode)),
					)
					if oldState, ok := m.redoNodes[oldCapture]; ok {
						oldState.fingerprint.remove(id)
					}
				}
				m.redoDispatcherToNode[id] = capture
				state.fingerprint.add(id)
			}
		}
	}

	for _, capture := range nodes {
		state := m.defaultNodes[capture]
		msgs = append(msgs, m.buildUpdateMessage(capture, common.DefaultMode, state.seq, state.fingerprint))
		if m.redoEnabled {
			redoState := m.redoNodes[capture]
			msgs = append(msgs, m.buildUpdateMessage(capture, common.RedoMode, redoState.seq, redoState.fingerprint))
		}
	}
	m.mu.Unlock()

	m.sendMessages(msgs)
}

func (m *dispatcherSetChecksumManager) ApplyDelta(
	mode int64,
	capture node.ID,
	add []common.DispatcherID,
	remove []common.DispatcherID,
) {
	if len(add) == 0 && len(remove) == 0 {
		return
	}

	now := time.Now()

	m.mu.Lock()
	if common.IsRedoMode(mode) && !m.redoEnabled {
		m.mu.Unlock()
		return
	}
	nodes := m.defaultNodes
	dispatcherToNode := m.defaultDispatcherToNode
	if common.IsRedoMode(mode) {
		nodes = m.redoNodes
		dispatcherToNode = m.redoDispatcherToNode
	}

	ensureState := func(capture node.ID) *dispatcherSetChecksumNodeState {
		state, ok := nodes[capture]
		if !ok {
			state = &dispatcherSetChecksumNodeState{}
			nodes[capture] = state
		}
		return state
	}

	changedCaptures := make(map[node.ID]struct{}, 2)

	for _, id := range remove {
		oldCapture, ok := dispatcherToNode[id]
		if !ok {
			continue
		}
		if oldCapture != capture {
			continue
		}
		state := ensureState(capture)
		state.fingerprint.remove(id)
		delete(dispatcherToNode, id)
		changedCaptures[capture] = struct{}{}
	}

	for _, id := range add {
		oldCapture, exists := dispatcherToNode[id]
		if exists && oldCapture == capture {
			continue
		}
		if exists && oldCapture != "" && oldCapture != capture {
			state := ensureState(oldCapture)
			state.fingerprint.remove(id)
			changedCaptures[oldCapture] = struct{}{}
		}

		state := ensureState(capture)
		state.fingerprint.add(id)
		dispatcherToNode[id] = capture
		changedCaptures[capture] = struct{}{}
	}

	msgs := make([]*messaging.TargetMessage, 0, len(changedCaptures))
	for capture := range changedCaptures {
		state := ensureState(capture)
		state.seq++
		state.lastSendAt = now
		msgs = append(msgs, m.buildUpdateMessage(capture, mode, state.seq, state.fingerprint))
	}
	m.mu.Unlock()

	m.sendMessages(msgs)
}

func (m *dispatcherSetChecksumManager) HandleAck(from node.ID, ack *heartbeatpb.DispatcherSetChecksumAck) {
	if ack == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if ack.Epoch != m.epoch {
		return
	}

	nodes := m.defaultNodes
	if common.IsRedoMode(ack.Mode) {
		nodes = m.redoNodes
	}
	state, ok := nodes[from]
	if !ok {
		return
	}
	if ack.Seq > state.ackedSeq {
		state.ackedSeq = ack.Seq
	}
}

func (m *dispatcherSetChecksumManager) ResendPending(resendInterval time.Duration) {
	now := time.Now()

	m.mu.Lock()
	msgs := make([]*messaging.TargetMessage, 0)

	buildResend := func(mode int64, nodes map[node.ID]*dispatcherSetChecksumNodeState) {
		for capture, state := range nodes {
			if state.seq == 0 || state.ackedSeq >= state.seq {
				continue
			}
			if !state.lastSendAt.IsZero() && now.Sub(state.lastSendAt) < resendInterval {
				continue
			}
			state.lastSendAt = now
			msgs = append(msgs, m.buildUpdateMessage(capture, mode, state.seq, state.fingerprint))
		}
	}

	buildResend(common.DefaultMode, m.defaultNodes)
	if m.redoEnabled {
		buildResend(common.RedoMode, m.redoNodes)
	}
	m.mu.Unlock()

	m.sendMessages(msgs)
}

func (m *dispatcherSetChecksumManager) RemoveNodes(nodes []node.ID) {
	if len(nodes) == 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	remove := func(nodeStates map[node.ID]*dispatcherSetChecksumNodeState, dispatcherToNode map[common.DispatcherID]node.ID) {
		for _, capture := range nodes {
			delete(nodeStates, capture)
		}
		for id, capture := range dispatcherToNode {
			for _, removed := range nodes {
				if capture == removed {
					delete(dispatcherToNode, id)
					break
				}
			}
		}
	}

	remove(m.defaultNodes, m.defaultDispatcherToNode)
	if m.redoEnabled {
		remove(m.redoNodes, m.redoDispatcherToNode)
	}
}

func (m *dispatcherSetChecksumManager) ObserveHeartbeat(from node.ID, req *heartbeatpb.HeartBeatRequest) {
	if req == nil {
		return
	}

	now := time.Now()
	const (
		warnAfter    = 2 * time.Minute
		warnInterval = 2 * time.Minute
	)

	type warnInfo struct {
		mode       int64
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
	epoch := m.epoch
	cfID := m.changefeedID

	warns := make([]warnInfo, 0, 2)

	observe := func(mode int64, stateVal heartbeatpb.ChecksumState, nodes map[node.ID]*dispatcherSetChecksumNodeState) {
		st, ok := nodes[from]
		if !ok {
			return
		}
		if stateVal == heartbeatpb.ChecksumState_OK {
			st.lastObservedState = stateVal
			st.nonOKSince = time.Time{}
			st.lastWarnAt = time.Time{}
			return
		}

		needResetTimer := st.lastObservedState == heartbeatpb.ChecksumState_OK || st.lastObservedState != stateVal
		if needResetTimer || st.nonOKSince.IsZero() {
			st.nonOKSince = now
			st.lastWarnAt = time.Time{}
		}

		st.lastObservedState = stateVal
		duration := now.Sub(st.nonOKSince)
		if duration < warnAfter {
			return
		}
		if !st.lastWarnAt.IsZero() && now.Sub(st.lastWarnAt) < warnInterval {
			return
		}
		st.lastWarnAt = now
		warns = append(warns, warnInfo{
			mode:       mode,
			state:      stateVal,
			duration:   duration,
			epoch:      epoch,
			seq:        st.seq,
			ackedSeq:   st.ackedSeq,
			lastSend:   st.lastSendAt,
			changefeed: cfID,
			capture:    from,
		})
	}

	observe(common.DefaultMode, req.ChecksumState, m.defaultNodes)
	if m.redoEnabled {
		observe(common.RedoMode, req.RedoChecksumState, m.redoNodes)
	}
	m.mu.Unlock()

	for _, w := range warns {
		stateStr := "mismatch"
		if w.state == heartbeatpb.ChecksumState_UNINITIALIZED {
			stateStr = "uninitialized"
		}
		log.Warn("dispatcher set checksum state not ok for a long time",
			zap.Stringer("changefeedID", w.changefeed),
			zap.String("capture", w.capture.String()),
			zap.String("mode", common.StringMode(w.mode)),
			zap.String("state", stateStr),
			zap.Duration("duration", w.duration),
			zap.Uint64("epoch", w.epoch),
			zap.Uint64("expectedSeq", w.seq),
			zap.Uint64("ackedSeq", w.ackedSeq),
			zap.Time("lastSendAt", w.lastSend),
		)
	}
}

func (m *dispatcherSetChecksumManager) buildUpdateMessage(
	to node.ID,
	mode int64,
	seq uint64,
	fingerprint dispatcherSetChecksumFingerprint,
) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(to, messaging.DispatcherManagerManagerTopic, &heartbeatpb.DispatcherSetChecksumUpdate{
		ChangefeedID: m.changefeedID.ToPB(),
		Epoch:        m.epoch,
		Mode:         mode,
		Seq:          seq,
		Fingerprint:  fingerprint.toPB(),
	})
}

func (m *dispatcherSetChecksumManager) sendMessages(msgs []*messaging.TargetMessage) {
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		if err := m.mc.SendCommand(msg); err != nil {
			log.Debug("failed to send dispatcher set checksum message",
				zap.Stringer("changefeedID", m.changefeedID),
				zap.Any("msg", msg),
				zap.Error(err),
			)
		}
	}
}
