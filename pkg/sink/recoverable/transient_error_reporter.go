package recoverable

import (
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
)

type reportedDispatcherEpoch struct {
	dispatcherID common.DispatcherID
	epoch        uint64
}

// TransientErrorReporter deduplicates transient error reports by dispatcher and epoch,
// and sends recoverable error events through a non-blocking channel.
type TransientErrorReporter struct {
	mu       sync.Mutex
	outputCh chan<- *ErrorEvent
	reported map[reportedDispatcherEpoch]struct{}
}

func NewTransientErrorReporter(outputCh chan<- *ErrorEvent) *TransientErrorReporter {
	return &TransientErrorReporter{
		outputCh: outputCh,
		reported: make(map[reportedDispatcherEpoch]struct{}),
	}
}

func (r *TransientErrorReporter) SetOutput(outputCh chan<- *ErrorEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outputCh = outputCh
}

// Report reports recoverable error once per dispatcher epoch.
// It returns:
// - reported dispatcher IDs in this call.
// - handled=true when the error is consumed by dedupe or successfully enqueued.
// - handled=false when this reporter cannot handle it (e.g. output channel unavailable/full).
func (r *TransientErrorReporter) Report(
	now time.Time,
	dispatcherIDs []common.DispatcherID,
	dispatcherEpochs map[common.DispatcherID]uint64,
) ([]common.DispatcherID, bool) {
	if len(dispatcherIDs) == 0 {
		return nil, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.outputCh == nil {
		return nil, false
	}

	reportedNow := make([]common.DispatcherID, 0, len(dispatcherIDs))
	seenIDs := make(map[common.DispatcherID]struct{}, len(dispatcherIDs))
	for _, dispatcherID := range dispatcherIDs {
		if _, ok := seenIDs[dispatcherID]; ok {
			continue
		}
		seenIDs[dispatcherID] = struct{}{}
		key := reportedDispatcherEpoch{
			dispatcherID: dispatcherID,
			epoch:        getDispatcherEpoch(dispatcherEpochs, dispatcherID),
		}
		if _, ok := r.reported[key]; ok {
			continue
		}
		reportedNow = append(reportedNow, dispatcherID)
	}

	if len(reportedNow) == 0 {
		return nil, true
	}

	event := &ErrorEvent{
		Time:          now,
		DispatcherIDs: reportedNow,
	}

	select {
	case r.outputCh <- event:
		for _, dispatcherID := range reportedNow {
			key := reportedDispatcherEpoch{
				dispatcherID: dispatcherID,
				epoch:        getDispatcherEpoch(dispatcherEpochs, dispatcherID),
			}
			r.reported[key] = struct{}{}
		}
		return reportedNow, true
	default:
		return nil, false
	}
}

// Ack clears reported state for the successful dispatcher epoch,
// allowing future transient errors in the same epoch to be reported again.
func (r *TransientErrorReporter) Ack(
	dispatcherIDs []common.DispatcherID,
	dispatcherEpochs map[common.DispatcherID]uint64,
) {
	if len(dispatcherIDs) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.reported) == 0 {
		return
	}
	for _, dispatcherID := range dispatcherIDs {
		key := reportedDispatcherEpoch{
			dispatcherID: dispatcherID,
			epoch:        getDispatcherEpoch(dispatcherEpochs, dispatcherID),
		}
		delete(r.reported, key)
	}
}

func getDispatcherEpoch(dispatcherEpochs map[common.DispatcherID]uint64, dispatcherID common.DispatcherID) uint64 {
	if len(dispatcherEpochs) == 0 {
		return 0
	}
	return dispatcherEpochs[dispatcherID]
}
