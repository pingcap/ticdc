package recoverable

import (
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
)

type DispatcherEpoch struct {
	DispatcherID common.DispatcherID
	Epoch        uint64
}

// TransientErrorReporter deduplicates transient error reports by dispatcher and epoch,
// and sends recoverable error events through a non-blocking channel.
type TransientErrorReporter struct {
	mu       sync.Mutex
	outputCh chan<- *ErrorEvent
	// reported records the max reported epoch for each dispatcher.
	// It provides both dedupe and bounded-state cleanup (old epochs are overwritten).
	reported map[common.DispatcherID]uint64
}

func NewTransientErrorReporter(outputCh chan<- *ErrorEvent) *TransientErrorReporter {
	return &TransientErrorReporter{
		outputCh: outputCh,
		reported: make(map[common.DispatcherID]uint64),
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
	dispatchers []DispatcherEpoch,
) ([]common.DispatcherID, bool) {
	if len(dispatchers) == 0 {
		return nil, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.outputCh == nil {
		return nil, false
	}

	reportedNow := make([]common.DispatcherID, 0, len(dispatchers))
	reportedEpochs := make(map[common.DispatcherID]uint64, len(dispatchers))
	seenEpochs := make(map[common.DispatcherID]uint64, len(dispatchers))
	seenOrder := make([]common.DispatcherID, 0, len(dispatchers))

	for _, dispatcher := range dispatchers {
		if dispatcher.DispatcherID == (common.DispatcherID{}) {
			continue
		}
		if epoch, ok := seenEpochs[dispatcher.DispatcherID]; ok {
			if dispatcher.Epoch > epoch {
				seenEpochs[dispatcher.DispatcherID] = dispatcher.Epoch
			}
		} else {
			seenEpochs[dispatcher.DispatcherID] = dispatcher.Epoch
			seenOrder = append(seenOrder, dispatcher.DispatcherID)
		}
	}

	for _, dispatcherID := range seenOrder {
		epoch := seenEpochs[dispatcherID]
		if reportedEpoch, ok := r.reported[dispatcherID]; ok && epoch <= reportedEpoch {
			continue
		}
		reportedNow = append(reportedNow, dispatcherID)
		reportedEpochs[dispatcherID] = epoch
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
		for dispatcherID, epoch := range reportedEpochs {
			r.reported[dispatcherID] = epoch
		}
		return reportedNow, true
	default:
		return nil, false
	}
}
