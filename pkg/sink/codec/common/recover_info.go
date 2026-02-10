package common

import (
	"errors"

	perrors "github.com/pingcap/errors"
	commonPkg "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/recoverable"
)

// AttachMessageRecoverInfo binds dispatcher recover context onto sink messages.
func AttachMessageRecoverInfo(messages []*Message, events []*commonEvent.RowEvent) error {
	if len(messages) == 0 || len(events) == 0 {
		return nil
	}

	eventIdx := 0
	var mismatchErr error
	for idx, message := range messages {
		rowsNeeded := message.GetRowsCount()
		remaining := len(events) - eventIdx
		if rowsNeeded > remaining {
			mismatchErr = annotateRecoverMismatchError(mismatchErr, "messageIndex=%d rowsNeeded=%d remaining=%d", idx, rowsNeeded, remaining)
			rowsNeeded = remaining
		}
		if rowsNeeded <= 0 {
			message.RecoverInfo = nil
			continue
		}
		message.RecoverInfo = buildMessageRecoverInfo(events[eventIdx : eventIdx+rowsNeeded])
		eventIdx += rowsNeeded
		if eventIdx >= len(events) {
			break
		}
	}
	if eventIdx != len(events) {
		mismatchErr = annotateRecoverMismatchError(mismatchErr, "eventsRemaining=%d totalEvents=%d", len(events)-eventIdx, len(events))
	}
	return mismatchErr
}

func annotateRecoverMismatchError(existing error, format string, args ...interface{}) error {
	if existing == nil {
		return perrors.Annotatef(errors.New("message rows count mismatches row events for recover info"), format, args...)
	}
	return perrors.Annotatef(existing, format, args...)
}

func buildMessageRecoverInfo(events []*commonEvent.RowEvent) *MessageRecoverInfo {
	dispatchers := make([]recoverable.DispatcherEpoch, 0, 1)
	dispatcherIndex := make(map[commonPkg.DispatcherID]int, 1)

	for _, event := range events {
		if event == nil || event.DispatcherID == (commonPkg.DispatcherID{}) {
			continue
		}
		if idx, ok := dispatcherIndex[event.DispatcherID]; ok {
			if dispatchers[idx].Epoch == 0 && event.Epoch > 0 {
				dispatchers[idx].Epoch = event.Epoch
			}
			continue
		}
		dispatcherIndex[event.DispatcherID] = len(dispatchers)
		dispatchers = append(dispatchers, recoverable.DispatcherEpoch{
			DispatcherID: event.DispatcherID,
			Epoch:        event.Epoch,
		})
	}

	if len(dispatchers) == 0 {
		return nil
	}
	return &MessageRecoverInfo{
		Dispatchers: dispatchers,
	}
}
