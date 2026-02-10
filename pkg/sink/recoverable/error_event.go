package recoverable

import (
	"time"

	"github.com/pingcap/ticdc/pkg/common"
)

// ErrorEvent is a structured recoverable error notification emitted by a sink.
// DispatcherManager forwards it to maintainer to trigger localized recovery.
type ErrorEvent struct {
	Time          time.Time
	DispatcherIDs []common.DispatcherID
}

// ErrorEventChanSetter is an optional interface implemented by sinks that can report recoverable errors
type ErrorEventChanSetter interface {
	SetRecoverableErrorChan(ch chan<- *ErrorEvent)
}
