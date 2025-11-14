// Copyright 2025 PingCAP, Inc.
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

package applier

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/redo/reader"
	"go.uber.org/zap"
)

// updateEventSplitter splits an update event to a delete event and a deferred insert event
// when the update event is an update to the handle key or the non empty unique key.
// deferred insert event means all delete events and update events in the same transaction are emitted before this insert event
type updateEventSplitter struct {
	rd             reader.RedoLogReader
	rdFinished     bool
	tempStorage    *tempTxnInsertEventStorage
	prevTxnStartTs common.Ts
	// pendingEvent is the event that trigger the process to emit events from tempStorage, it can be
	// 1) an insert event in the same transaction(because there will be no more update and delete events in the same transaction)
	// 2) a new event in the next transaction
	pendingEvent *event.RedoDMLEvent
	// meetInsertInCurTxn is used to indicate whether we meet an insert event in the current transaction
	// this is to add some check to ensure that insert events are emitted after other kinds of events in the same transaction
	meetInsertInCurTxn bool
}

func newUpdateEventSplitter(rd reader.RedoLogReader, dir string) *updateEventSplitter {
	return &updateEventSplitter{
		rd:             rd,
		rdFinished:     false,
		tempStorage:    newTempTxnInsertEventStorage(defaultFlushThreshold, dir),
		prevTxnStartTs: 0,
	}
}

func (u *updateEventSplitter) checkEventOrder(event *event.RedoDMLEvent) {
	if event == nil {
		return
	}
	// meeet a new transaction
	if event.Row.StartTs != u.prevTxnStartTs {
		u.meetInsertInCurTxn = false
		return
	}
	if event.IsInsert() {
		u.meetInsertInCurTxn = true
	} else {
		// delete or update events
		if u.meetInsertInCurTxn {
			log.Panic("insert events should be emitted after other kinds of events in the same transaction")
		}
	}
}

func (u *updateEventSplitter) readNextRow(ctx context.Context) (*event.RedoDMLEvent, error) {
	for {
		// case 1: pendingEvent is not nil, emit all events from tempStorage and then process pendingEvent
		if u.pendingEvent != nil {
			if u.tempStorage.hasEvent() {
				return u.tempStorage.readNextEvent()
			}
			var event *event.RedoDMLEvent
			var err error
			event, u.pendingEvent, err = processEvent(u.pendingEvent, u.prevTxnStartTs, u.tempStorage)
			if err != nil {
				return nil, err
			}
			if event == nil || u.pendingEvent != nil {
				log.Panic("processEvent return wrong result for pending event",
					zap.Any("event", event),
					zap.Any("pendingEvent", u.pendingEvent))
			}
			return event, nil
		}
		// case 2: no more events from RedoLogReader, emit all events from tempStorage and return nil
		if u.rdFinished {
			if u.tempStorage.hasEvent() {
				return u.tempStorage.readNextEvent()
			}
			return nil, nil
		}
		// case 3: read and process events from RedoLogReader
		event, err := u.rd.ReadNextRow(ctx)
		if err != nil {
			return nil, err
		}
		if event == nil {
			u.rdFinished = true
		} else {
			u.checkEventOrder(event)
			prevTxnStartTs := u.prevTxnStartTs
			u.prevTxnStartTs = event.Row.StartTs
			var err error
			event, u.pendingEvent, err = processEvent(event, prevTxnStartTs, u.tempStorage)
			if err != nil {
				return nil, err
			}
			if event != nil {
				return event, nil
			}
			if u.pendingEvent == nil {
				log.Panic("event to emit and pending event cannot all be nil")
			}
		}
	}
}

// processEvent return (event to emit, pending event)
func processEvent(
	event *commonEvent.RedoDMLEvent,
	prevTxnStartTs commonType.Ts,
	tempStorage *tempTxnInsertEventStorage,
) (*commonEvent.RedoDMLEvent, *commonEvent.RedoDMLEvent, error) {
	if event == nil {
		log.Panic("event should not be nil")
	}

	// meet a new transaction
	if prevTxnStartTs != 0 && prevTxnStartTs != event.Row.StartTs {
		if tempStorage.hasEvent() {
			// emit the insert events in the previous transaction
			return nil, event, nil
		}
	}
	if event.IsDelete() {
		return event, nil, nil
	} else if event.IsInsert() {
		if tempStorage.hasEvent() {
			// pend current event and emit the insert events in temp storage first to release memory
			return nil, event, nil
		}
		return event, nil, nil
	} else if shouldSplitUpdateEvent(event) {
		deleteEvent, insertEvent, err := splitUpdateEvent(event)
		if err != nil {
			return nil, nil, err
		}
		err = tempStorage.addEvent(insertEvent)
		if err != nil {
			return nil, nil, err
		}
		return deleteEvent, nil, nil
	} else {
		return event, nil, nil
	}
}
