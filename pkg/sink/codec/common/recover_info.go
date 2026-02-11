package common

import (
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/recoverable"
)

// AttachMessageRecoverInfo binds dispatcher recover context onto sink messages.
func AttachMessageRecoverInfo(messages []*Message, events []*commonEvent.RowEvent) {
	if len(messages) == 0 || len(events) == 0 {
		return
	}

	eventIdx := 0
	for _, message := range messages {
		rowsNeeded := message.GetRowsCount()
		remaining := len(events) - eventIdx
		if rowsNeeded > remaining {
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
}
