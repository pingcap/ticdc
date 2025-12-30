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

package dispatchermanager

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/set_checksum"
	"go.uber.org/zap"
)

// dispatcherSetChecksumExpected stores the latest expected dispatcher set checksum pushed by Maintainer.
//
// Maintainer sends (epoch, seq, checksum) updates to DispatcherManager. Updates can be resent and
// reordered, so DispatcherManager keeps the newest seq for each (epoch, mode) and ignores older
// ones to ensure eventual convergence.
type dispatcherSetChecksumExpected struct {
	epoch       uint64
	seq         uint64
	checksum    set_checksum.Checksum
	initialized bool
}

// dispatcherSetChecksumRuntime tracks the runtime verification state used for metrics and log throttling.
//
// It records the last observed checksum state (OK / MISMATCH / UNINITIALIZED) as well as timestamps
// to avoid spamming logs when the state stays non-OK.
type dispatcherSetChecksumRuntime struct {
	state heartbeatpb.ChecksumState
	// seq is a monotonic version for state. It is used to help Maintainer discard out-of-order
	// heartbeats that would otherwise overwrite a newer checksum state.
	seq         uint64
	nonOKSince  time.Time
	lastLogTime time.Time
}

// dispatcherSetChecksumState maintains expected and runtime checksum states for both default and redo modes.
//
// It is owned by DispatcherManager and guarded by mu.
type dispatcherSetChecksumState struct {
	mu sync.RWMutex

	defaultExpected dispatcherSetChecksumExpected
	redoExpected    dispatcherSetChecksumExpected

	defaultRuntime dispatcherSetChecksumRuntime
	redoRuntime    dispatcherSetChecksumRuntime
}

// ApplyDispatcherSetChecksumUpdate applies a checksum update from Maintainer.
//
// The update is best-effort: it may be resent or reordered, and DispatcherManager will keep the
// newest seq for each (epoch, mode).
func (e *DispatcherManager) ApplyDispatcherSetChecksumUpdate(req *heartbeatpb.DispatcherSetChecksumUpdate) {
	// DispatcherSetChecksumUpdate can be resent and reordered. For each (epoch, mode), keep the
	// newest seq to ensure DispatcherManager eventually converges to the latest expected set.
	if req == nil {
		return
	}
	checksum := set_checksum.FromPB(req.Checksum)

	e.dispatcherSetChecksum.mu.Lock()
	defer e.dispatcherSetChecksum.mu.Unlock()

	expected := &e.dispatcherSetChecksum.defaultExpected
	if common.IsRedoMode(req.Mode) {
		expected = &e.dispatcherSetChecksum.redoExpected
	}

	if expected.initialized && expected.epoch == req.Epoch && req.Seq < expected.seq {
		return
	}

	expected.epoch = req.Epoch
	expected.seq = req.Seq
	expected.checksum = checksum
	expected.initialized = true
}

// ResetDispatcherSetChecksum clears both expected and runtime checksum states.
//
// It is typically used when DispatcherManager is reset (for example, on bootstrap or epoch changes).
func (e *DispatcherManager) ResetDispatcherSetChecksum() {
	e.dispatcherSetChecksum.mu.Lock()
	defaultSeq := e.dispatcherSetChecksum.defaultRuntime.seq + 1
	redoSeq := e.dispatcherSetChecksum.redoRuntime.seq + 1

	e.dispatcherSetChecksum.defaultExpected = dispatcherSetChecksumExpected{}
	e.dispatcherSetChecksum.redoExpected = dispatcherSetChecksumExpected{}
	e.dispatcherSetChecksum.defaultRuntime = dispatcherSetChecksumRuntime{
		state: heartbeatpb.ChecksumState_UNINITIALIZED,
		seq:   defaultSeq,
	}
	e.dispatcherSetChecksum.redoRuntime = dispatcherSetChecksumRuntime{
		state: heartbeatpb.ChecksumState_UNINITIALIZED,
		seq:   redoSeq,
	}
	e.dispatcherSetChecksum.mu.Unlock()

	// Force refresh gauges on reset to avoid stale values lingering after runtime state is cleared.
	keyspace := e.changefeedID.Keyspace()
	changefeed := e.changefeedID.Name()
	e.updateDispatcherSetChecksumGauge(keyspace, changefeed, common.StringMode(common.DefaultMode), heartbeatpb.ChecksumState_UNINITIALIZED)
	if e.RedoEnable {
		e.updateDispatcherSetChecksumGauge(keyspace, changefeed, common.StringMode(common.RedoMode), heartbeatpb.ChecksumState_UNINITIALIZED)
	} else {
		// Clear redo gauge to avoid stale non-zero values when redo is not enabled.
		e.updateDispatcherSetChecksumGauge(keyspace, changefeed, common.StringMode(common.RedoMode), heartbeatpb.ChecksumState_OK)
	}
}

// shouldIncludeDispatcherInChecksum decides whether a dispatcher should be included in runtime checksum calculation.
//
// Some intermediate states are excluded so that transient lifecycle stages do not incorrectly
// flip the checksum state and block watermark reporting.
func shouldIncludeDispatcherInChecksum(state heartbeatpb.ComponentState) bool {
	switch state {
	case heartbeatpb.ComponentState_Initializing,
		heartbeatpb.ComponentState_Preparing,
		heartbeatpb.ComponentState_MergeReady:
		return false
	default:
		return true
	}
}

// computeDispatcherSetChecksum computes the checksum of dispatchers that are considered "active" for the mode.
//
// It only includes dispatchers whose component states pass shouldIncludeDispatcherInChecksum.
func (e *DispatcherManager) computeDispatcherSetChecksum(mode int64) set_checksum.Checksum {
	var checksum set_checksum.Checksum
	if common.IsRedoMode(mode) {
		e.redoDispatcherMap.ForEach(func(id common.DispatcherID, d *dispatcher.RedoDispatcher) {
			if !shouldIncludeDispatcherInChecksum(d.GetComponentStatus()) {
				return
			}
			checksum.Add(id)
		})
		return checksum
	}
	e.dispatcherMap.ForEach(func(id common.DispatcherID, d *dispatcher.EventDispatcher) {
		if !shouldIncludeDispatcherInChecksum(d.GetComponentStatus()) {
			return
		}
		checksum.Add(id)
	})
	return checksum
}

// applyChecksumStateToHeartbeat verifies dispatcher set checksums and updates heartbeat fields.
//
// It returns whether watermark reporting should be suppressed for each mode. Callers should only
// nil out outgoing watermark fields when sending the heartbeat, while keeping local watermarks
// intact for internal bookkeeping and metrics.
func (e *DispatcherManager) applyChecksumStateToHeartbeat(
	msg *heartbeatpb.HeartBeatRequest,
	defaultChecksum set_checksum.Checksum,
	redoChecksum set_checksum.Checksum,
) (discardDefaultWatermark bool, discardRedoWatermark bool) {
	if msg == nil {
		return false, false
	}

	defaultState, defaultSeq := e.verifyDispatcherSetChecksum(common.DefaultMode, defaultChecksum)
	msg.ChecksumState = defaultState
	msg.ChecksumStateSeq = defaultSeq
	discardDefaultWatermark = defaultState != heartbeatpb.ChecksumState_OK

	if e.RedoEnable {
		redoState, redoSeq := e.verifyDispatcherSetChecksum(common.RedoMode, redoChecksum)
		msg.RedoChecksumState = redoState
		msg.RedoChecksumStateSeq = redoSeq
		discardRedoWatermark = redoState != heartbeatpb.ChecksumState_OK
	}

	return discardDefaultWatermark, discardRedoWatermark
}

// incDispatcherSetChecksumNotOKTotal increments the total counter when watermark reporting is suppressed.
func (e *DispatcherManager) incDispatcherSetChecksumNotOKTotal(mode int64, state heartbeatpb.ChecksumState) {
	switch state {
	case heartbeatpb.ChecksumState_OK:
		return
	case heartbeatpb.ChecksumState_MISMATCH:
		metrics.DispatcherManagerDispatcherSetChecksumNotOKTotal.WithLabelValues(
			e.changefeedID.Keyspace(), e.changefeedID.Name(), common.StringMode(mode), "mismatch",
		).Inc()
	case heartbeatpb.ChecksumState_UNINITIALIZED:
		metrics.DispatcherManagerDispatcherSetChecksumNotOKTotal.WithLabelValues(
			e.changefeedID.Keyspace(), e.changefeedID.Name(), common.StringMode(mode), "uninitialized",
		).Inc()
	}
}

// verifyDispatcherSetChecksum compares runtime checksum with expected checksum for the mode.
//
// It updates runtime state used for per-capture metrics and emits throttled logs when the state is non-OK.
func (e *DispatcherManager) verifyDispatcherSetChecksum(
	mode int64,
	actual set_checksum.Checksum,
) (state heartbeatpb.ChecksumState, stateSeq uint64) {
	now := time.Now()
	modeLabel := common.StringMode(mode)
	keyspace := e.changefeedID.Keyspace()
	changefeed := e.changefeedID.Name()

	e.dispatcherSetChecksum.mu.Lock()
	expected, runtime := e.getDispatcherSetChecksumStates(mode)

	expectedSeq := expected.seq
	expectedInit := expected.initialized
	expectedChecksum := expected.checksum
	state = computeChecksumState(*expected, actual)

	oldState := runtime.state
	if oldState != state {
		runtime.seq++
	}

	nonOKSince, logNotOK := updateChecksumRuntime(runtime, oldState, state, now)
	stateSeq = runtime.seq
	e.dispatcherSetChecksum.mu.Unlock()

	if oldState != state {
		e.updateDispatcherSetChecksumGauge(keyspace, changefeed, modeLabel, state)
	}

	if logNotOK {
		notOKFor := now.Sub(nonOKSince)
		fields := []zap.Field{
			zap.Stringer("changefeedID", e.changefeedID),
			zap.String("mode", modeLabel),
			zap.String("state", state.String()),
			zap.Duration("duration", notOKFor),
			zap.Uint64("expectedSeq", expectedSeq),
			zap.Bool("expectedInitialized", expectedInit),
			zap.Any("actualChecksum", actual),
			zap.Any("expectedChecksum", expectedChecksum),
			zap.String("prevState", oldState.String()),
		}
		log.Warn("dispatcher set checksum not ok, skip watermark reporting", fields...)
	}

	return state, stateSeq
}

// getDispatcherSetChecksumStates returns pointers to expected/runtime states for the given mode.
// Caller must hold e.dispatcherSetChecksum.mu.
func (e *DispatcherManager) getDispatcherSetChecksumStates(
	mode int64,
) (*dispatcherSetChecksumExpected, *dispatcherSetChecksumRuntime) {
	if common.IsRedoMode(mode) {
		return &e.dispatcherSetChecksum.redoExpected, &e.dispatcherSetChecksum.redoRuntime
	}
	return &e.dispatcherSetChecksum.defaultExpected, &e.dispatcherSetChecksum.defaultRuntime
}

// computeChecksumState compares actual checksum with expected checksum and returns the derived state.
func computeChecksumState(expected dispatcherSetChecksumExpected, actual set_checksum.Checksum) heartbeatpb.ChecksumState {
	if !expected.initialized {
		return heartbeatpb.ChecksumState_UNINITIALIZED
	}
	if !actual.Equal(expected.checksum) {
		return heartbeatpb.ChecksumState_MISMATCH
	}
	return heartbeatpb.ChecksumState_OK
}

// updateDispatcherSetChecksumGauge updates the per-capture gauge reflecting the current checksum state.
func (e *DispatcherManager) updateDispatcherSetChecksumGauge(
	keyspace string,
	changefeed string,
	modeLabel string,
	state heartbeatpb.ChecksumState,
) {
	setGauge := func(stateLabel string, value float64) {
		metrics.DispatcherManagerDispatcherSetChecksumNotOKGauge.WithLabelValues(
			keyspace, changefeed, modeLabel, stateLabel,
		).Set(value)
	}

	switch state {
	case heartbeatpb.ChecksumState_OK:
		setGauge("mismatch", 0)
		setGauge("uninitialized", 0)
	case heartbeatpb.ChecksumState_MISMATCH:
		setGauge("mismatch", 1)
		setGauge("uninitialized", 0)
	case heartbeatpb.ChecksumState_UNINITIALIZED:
		setGauge("mismatch", 0)
		setGauge("uninitialized", 1)
	}
}

// updateChecksumRuntime updates runtime bookkeeping and decides whether to emit a non-OK log.
// Caller must hold e.dispatcherSetChecksum.mu for the corresponding runtime state.
func updateChecksumRuntime(
	runtime *dispatcherSetChecksumRuntime,
	oldState heartbeatpb.ChecksumState,
	newState heartbeatpb.ChecksumState,
	now time.Time,
) (
	nonOKSince time.Time,
	logNotOK bool,
) {
	const (
		logAfter    = 30 * time.Second
		logInterval = 30 * time.Second
	)

	if newState == heartbeatpb.ChecksumState_OK {
		runtime.state = newState
		runtime.nonOKSince = time.Time{}
		runtime.lastLogTime = time.Time{}
		return time.Time{}, false
	}

	needResetTimer := oldState == heartbeatpb.ChecksumState_OK || oldState != newState
	if needResetTimer || runtime.nonOKSince.IsZero() {
		runtime.nonOKSince = now
		runtime.lastLogTime = time.Time{}
	}

	runtime.state = newState
	nonOKSince = runtime.nonOKSince

	notOKFor := now.Sub(nonOKSince)
	if notOKFor < logAfter {
		return nonOKSince, false
	}
	if !runtime.lastLogTime.IsZero() && now.Sub(runtime.lastLogTime) < logInterval {
		return nonOKSince, false
	}
	runtime.lastLogTime = now
	return nonOKSince, true
}
