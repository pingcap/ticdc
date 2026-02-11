package recoverable

import (
	common "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
)

// RecoverInfo carries dispatcher context for recoverable transient errors.
type RecoverInfo struct {
	Dispatchers []DispatcherEpoch
}

// BuildRecoverInfo extracts dispatcher recovery context from row events.
// For duplicated dispatcher IDs in one input slice, the max epoch is kept.
func BuildRecoverInfo(events []*commonEvent.RowEvent) *RecoverInfo {
	if len(events) == 0 {
		return nil
	}

	dispatchers := make([]DispatcherEpoch, 0, 1)
	indexByDispatcher := make(map[common.DispatcherID]int, 1)

	for _, event := range events {
		if event == nil {
			continue
		}

		dispatcherID := event.DispatcherID
		idx, ok := indexByDispatcher[dispatcherID]
		if !ok {
			indexByDispatcher[dispatcherID] = len(dispatchers)
			dispatchers = append(dispatchers, DispatcherEpoch{
				DispatcherID: dispatcherID,
				Epoch:        event.Epoch,
			})
			continue
		}
		if event.Epoch > dispatchers[idx].Epoch {
			dispatchers[idx].Epoch = event.Epoch
		}
	}

	if len(dispatchers) == 0 {
		return nil
	}
	return &RecoverInfo{
		Dispatchers: dispatchers,
	}
}
