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
