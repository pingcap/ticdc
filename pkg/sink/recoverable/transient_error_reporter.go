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
	reported map[DispatcherEpoch]struct{}
}

func NewTransientErrorReporter(outputCh chan<- *ErrorEvent) *TransientErrorReporter {
	return &TransientErrorReporter{
		outputCh: outputCh,
		reported: make(map[DispatcherEpoch]struct{}),
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
	reportedKeys := make([]DispatcherEpoch, 0, len(dispatchers))
	seenDispatchers := make(map[DispatcherEpoch]struct{}, len(dispatchers))
	for _, dispatcher := range dispatchers {
		if dispatcher.DispatcherID == (common.DispatcherID{}) {
			continue
		}
		if _, ok := seenDispatchers[dispatcher]; ok {
			continue
		}
		seenDispatchers[dispatcher] = struct{}{}
		if _, ok := r.reported[dispatcher]; ok {
			continue
		}
		reportedNow = append(reportedNow, dispatcher.DispatcherID)
		reportedKeys = append(reportedKeys, dispatcher)
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
		for _, key := range reportedKeys {
			r.reported[key] = struct{}{}
		}
		return reportedNow, true
	default:
		return nil, false
	}
}
