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

package recoverable

import (
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
)

// RecoverEvent is a structured recover notification emitted by a sink.
type RecoverEvent struct {
	Time          time.Time
	DispatcherIDs []common.DispatcherID
}

// Recoverable is implemented by sinks that can report recoverable errors.
type Recoverable interface {
	SetRecoverReporter(reporter *Reporter)
}

type DispatcherEpoch struct {
	// DispatcherID identifies the dispatcher that produced the recoverable error.
	DispatcherID common.DispatcherID
	// Epoch identifies the dispatcher instance/version observed by the sink.
	// The same dispatcher should be reported at most once per epoch.
	Epoch uint64
}

// Reporter deduplicates reported errors by dispatcher and epoch,
// and sends recoverable error events through a non-blocking channel.
type Reporter struct {
	// outputCh receives recoverable events.
	outputCh chan *RecoverEvent

	mu sync.Mutex
	// reported keeps the max epoch that has been successfully enqueued per dispatcher.
	reported map[common.DispatcherID]uint64
}

func NewReporter(size int) *Reporter {
	if size < 0 {
		size = 0
	}
	return &Reporter{
		outputCh: make(chan *RecoverEvent, size),
		reported: make(map[common.DispatcherID]uint64),
	}
}

func (r *Reporter) OutputCh() <-chan *RecoverEvent {
	return r.outputCh
}

// Report reports recoverable errors once per dispatcher epoch.
// It accepts non-normalized input and deduplicates dispatcher IDs per call by
// keeping the max epoch for each dispatcher.
// It returns:
// - reported dispatcher IDs in this call.
// - handled=true when the error is consumed by dedupe or successfully enqueued.
// - handled=false when this reporter cannot enqueue in non-blocking mode (e.g. channel full).
func (r *Reporter) Report(
	dispatchers []DispatcherEpoch,
) ([]common.DispatcherID, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(dispatchers) == 0 {
		return nil, false
	}

	normalized := make([]DispatcherEpoch, 0, len(dispatchers))
	indexByDispatcher := make(map[common.DispatcherID]int, len(dispatchers))
	for _, dispatcher := range dispatchers {
		dispatcherID := dispatcher.DispatcherID
		if idx, ok := indexByDispatcher[dispatcherID]; ok {
			if dispatcher.Epoch > normalized[idx].Epoch {
				normalized[idx].Epoch = dispatcher.Epoch
			}
			continue
		}
		indexByDispatcher[dispatcherID] = len(normalized)
		normalized = append(normalized, dispatcher)
	}

	// candidates collects dispatchers that still need a recover event after dedupe.
	candidates := make([]DispatcherEpoch, 0, len(normalized))
	for _, dispatcher := range normalized {
		dispatcherID := dispatcher.DispatcherID
		if reportedEpoch, ok := r.reported[dispatcherID]; ok && dispatcher.Epoch <= reportedEpoch {
			continue
		}
		candidates = append(candidates, dispatcher)
	}

	if len(candidates) == 0 {
		return nil, true
	}

	dispatcherIDs := make([]common.DispatcherID, 0, len(candidates))
	for _, dispatcher := range candidates {
		dispatcherIDs = append(dispatcherIDs, dispatcher.DispatcherID)
	}

	event := &RecoverEvent{
		Time:          time.Now(),
		DispatcherIDs: dispatcherIDs,
	}

	select {
	case r.outputCh <- event:
		for _, dispatcher := range candidates {
			r.reported[dispatcher.DispatcherID] = dispatcher.Epoch
		}
		return dispatcherIDs, true
	default:
	}
	return nil, false
}

// Clear removes dedupe state for one dispatcher.
// The next report for this dispatcher is treated as a new lifecycle.
func (r *Reporter) Clear(dispatcherID common.DispatcherID) {
	r.mu.Lock()
	delete(r.reported, dispatcherID)
	r.mu.Unlock()
}
