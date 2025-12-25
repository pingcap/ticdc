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
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/set_checksum"
	"go.uber.org/zap"
)

type dispatcherSetChecksumExpected struct {
	epoch       uint64
	seq         uint64
	checksum    set_checksum.Checksum
	initialized bool
}

type dispatcherSetChecksumRuntime struct {
	state            heartbeatpb.ChecksumState
	nonOKSince       time.Time
	lastErrorLogTime time.Time
	gaugeInitialized bool
}

type dispatcherSetChecksumState struct {
	mu sync.RWMutex

	defaultExpected dispatcherSetChecksumExpected
	redoExpected    dispatcherSetChecksumExpected

	defaultRuntime dispatcherSetChecksumRuntime
	redoRuntime    dispatcherSetChecksumRuntime
}

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

func (e *DispatcherManager) ResetDispatcherSetChecksum() {
	e.dispatcherSetChecksum.mu.Lock()
	defer e.dispatcherSetChecksum.mu.Unlock()

	e.dispatcherSetChecksum.defaultExpected = dispatcherSetChecksumExpected{}
	e.dispatcherSetChecksum.redoExpected = dispatcherSetChecksumExpected{}
	e.dispatcherSetChecksum.defaultRuntime = dispatcherSetChecksumRuntime{}
	e.dispatcherSetChecksum.redoRuntime = dispatcherSetChecksumRuntime{}
}

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

func (e *DispatcherManager) applyChecksumStateToHeartbeat(
	msg *heartbeatpb.HeartBeatRequest,
	actualDefault set_checksum.Checksum,
	actualRedo set_checksum.Checksum,
) {
	if msg == nil {
		return
	}

	defaultState := e.verifyDispatcherSetChecksum(common.DefaultMode, actualDefault)
	msg.ChecksumState = defaultState
	if defaultState != heartbeatpb.ChecksumState_OK {
		if msg.Watermark != nil {
			e.incDispatcherSetChecksumNotOKTotal(common.DefaultMode, defaultState)
		}
		msg.Watermark = nil
	}

	if e.RedoEnable {
		redoState := e.verifyDispatcherSetChecksum(common.RedoMode, actualRedo)
		msg.RedoChecksumState = redoState
		if redoState != heartbeatpb.ChecksumState_OK {
			if msg.RedoWatermark != nil {
				e.incDispatcherSetChecksumNotOKTotal(common.RedoMode, redoState)
			}
			msg.RedoWatermark = nil
		}
	}
}

func (e *DispatcherManager) incDispatcherSetChecksumNotOKTotal(mode int64, state heartbeatpb.ChecksumState) {
	if state == heartbeatpb.ChecksumState_OK {
		return
	}

	stateLabel := "mismatch"
	if state == heartbeatpb.ChecksumState_UNINITIALIZED {
		stateLabel = "uninitialized"
	}

	capture := appcontext.GetID()
	modeLabel := common.StringMode(mode)
	keyspace := e.changefeedID.Keyspace()
	changefeed := e.changefeedID.Name()

	metrics.DispatcherManagerDispatcherSetChecksumNotOKTotal.WithLabelValues(
		keyspace, changefeed, capture, modeLabel, stateLabel,
	).Inc()
}

func (e *DispatcherManager) verifyDispatcherSetChecksum(mode int64, actual set_checksum.Checksum) heartbeatpb.ChecksumState {
	now := time.Now()
	capture := appcontext.GetID()
	modeLabel := common.StringMode(mode)
	keyspace := e.changefeedID.Keyspace()
	changefeed := e.changefeedID.Name()

	var (
		state            heartbeatpb.ChecksumState
		expectedSeq      uint64
		expectedInit     bool
		expectedChecksum set_checksum.Checksum
		oldState         heartbeatpb.ChecksumState
		nonOKSince       time.Time
		needGaugeUpdate  bool
		logRecovered     bool
		logNotOKWarn     bool
		logNotOKError    bool
		recoveredFor     time.Duration
		notOKFor         time.Duration
	)

	e.dispatcherSetChecksum.mu.Lock()
	expected := &e.dispatcherSetChecksum.defaultExpected
	runtime := &e.dispatcherSetChecksum.defaultRuntime
	if common.IsRedoMode(mode) {
		expected = &e.dispatcherSetChecksum.redoExpected
		runtime = &e.dispatcherSetChecksum.redoRuntime
	}

	expectedSeq = expected.seq
	expectedInit = expected.initialized
	expectedChecksum = expected.checksum

	if !expected.initialized {
		state = heartbeatpb.ChecksumState_UNINITIALIZED
	} else if !actual.Equal(expected.checksum) {
		state = heartbeatpb.ChecksumState_MISMATCH
	} else {
		state = heartbeatpb.ChecksumState_OK
	}

	oldState = runtime.state
	nonOKSince = runtime.nonOKSince
	needGaugeUpdate = !runtime.gaugeInitialized || oldState != state

	const (
		errorAfter    = 30 * time.Second
		errorInterval = 30 * time.Second
	)

	if state == heartbeatpb.ChecksumState_OK {
		if oldState != heartbeatpb.ChecksumState_OK && !runtime.nonOKSince.IsZero() {
			logRecovered = true
			recoveredFor = now.Sub(runtime.nonOKSince)
		}
		runtime.state = state
		runtime.nonOKSince = time.Time{}
		runtime.lastErrorLogTime = time.Time{}
	} else {
		needResetTimer := oldState == heartbeatpb.ChecksumState_OK || oldState != state
		if needResetTimer || runtime.nonOKSince.IsZero() {
			runtime.nonOKSince = now
			runtime.lastErrorLogTime = time.Time{}
			logNotOKWarn = true
		} else {
			notOKFor = now.Sub(runtime.nonOKSince)
			if notOKFor >= errorAfter && now.Sub(runtime.lastErrorLogTime) >= errorInterval {
				runtime.lastErrorLogTime = now
				logNotOKError = true
			}
		}
		runtime.state = state
		nonOKSince = runtime.nonOKSince
	}
	runtime.gaugeInitialized = true
	e.dispatcherSetChecksum.mu.Unlock()

	if needGaugeUpdate {
		setGauge := func(stateLabel string, value float64) {
			metrics.DispatcherManagerDispatcherSetChecksumNotOKGauge.WithLabelValues(
				keyspace, changefeed, capture, modeLabel, stateLabel,
			).Set(value)
		}

		setGauge("mismatch", 0)
		setGauge("uninitialized", 0)

		if state != heartbeatpb.ChecksumState_OK {
			stateLabel := "mismatch"
			if state == heartbeatpb.ChecksumState_UNINITIALIZED {
				stateLabel = "uninitialized"
			}
			setGauge(stateLabel, 1)
		}
	}

	if logRecovered {
		log.Info("dispatcher set checksum recovered",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.String("capture", capture),
			zap.String("mode", modeLabel),
			zap.Duration("duration", recoveredFor),
			zap.Uint64("expectedSeq", expectedSeq),
		)
	}

	if logNotOKWarn || logNotOKError {
		level := "warn"
		if logNotOKError {
			level = "error"
		}
		stateStr := "mismatch"
		if state == heartbeatpb.ChecksumState_UNINITIALIZED {
			stateStr = "uninitialized"
		}
		notOKFor = now.Sub(nonOKSince)
		fields := []zap.Field{
			zap.Stringer("changefeedID", e.changefeedID),
			zap.String("capture", capture),
			zap.String("mode", modeLabel),
			zap.String("state", stateStr),
			zap.Duration("duration", notOKFor),
			zap.Uint64("expectedSeq", expectedSeq),
			zap.Bool("expectedInitialized", expectedInit),
			zap.Uint64("actualCount", actual.Count),
			zap.Uint64("actualXorHigh", actual.XorHigh),
			zap.Uint64("actualXorLow", actual.XorLow),
			zap.Uint64("actualSumHigh", actual.SumHigh),
			zap.Uint64("actualSumLow", actual.SumLow),
			zap.Uint64("expectedCount", expectedChecksum.Count),
			zap.Uint64("expectedXorHigh", expectedChecksum.XorHigh),
			zap.Uint64("expectedXorLow", expectedChecksum.XorLow),
			zap.Uint64("expectedSumHigh", expectedChecksum.SumHigh),
			zap.Uint64("expectedSumLow", expectedChecksum.SumLow),
			zap.String("prevState", oldState.String()),
		}
		if level == "error" {
			log.Error("dispatcher set checksum not ok, skip watermark reporting", fields...)
		} else {
			log.Warn("dispatcher set checksum not ok, skip watermark reporting", fields...)
		}
	}

	return state
}
