package recoverable

import (
	"time"

	"github.com/pingcap/ticdc/pkg/common"
)

type DispatcherEpoch struct {
	// DispatcherID identifies the dispatcher that produced the recoverable error.
	DispatcherID common.DispatcherID
	// Epoch is bumped when maintainer restarts the dispatcher.
	// Same dispatcher should be reported at most once per epoch.
	Epoch uint64
}

// Reporter deduplicates reported errors by dispatcher and epoch,
// and sends recoverable error events through a non-blocking channel.
type Reporter struct {
	// outputCh receives recoverable error events for maintainer handling.
	outputCh chan<- *ErrorEvent
	// reported keeps the max epoch that has been successfully enqueued per dispatcher.
	reported map[common.DispatcherID]uint64
}

func NewReporter(outputCh chan<- *ErrorEvent) *Reporter {
	return &Reporter{
		outputCh: outputCh,
		reported: make(map[common.DispatcherID]uint64),
	}
}

// Report reports recoverable error once per dispatcher epoch.
// Input assumption:
// - dispatchers are pre-normalized: one item per DispatcherID with the max epoch.
//
// This assumption is guaranteed on the current path:
// BuildRecoverInfo deduplicates dispatcher epochs, and
// callers pass that normalized dispatcher list into Report.
// Report is expected to be called serially on the current path.
// It returns:
// - reported dispatcher IDs in this call.
// - handled=true when the error is consumed by dedupe or successfully enqueued.
// - handled=false when this reporter cannot handle it (e.g. output channel unavailable/full).
func (r *Reporter) Report(
	dispatchers []DispatcherEpoch,
) ([]common.DispatcherID, bool) {
	if r.outputCh == nil {
		return nil, false
	}

	if len(dispatchers) == 0 {
		return nil, false
	}

	// candidates collects dispatchers that still need a recover event after dedupe.
	candidates := make([]DispatcherEpoch, 0, len(dispatchers))
	for _, dispatcher := range dispatchers {
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

	event := &ErrorEvent{
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
