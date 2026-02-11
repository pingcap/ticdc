package common

import (
	"errors"

	perrors "github.com/pingcap/errors"
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
		message.RecoverInfo = recoverable.BuildRecoverInfo(events[eventIdx : eventIdx+rowsNeeded])
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
