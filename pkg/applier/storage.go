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
	"fmt"
	"os"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo/codec"
	"go.uber.org/zap"
)

const (
	tempStorageFileName   = "_insert_storage.tmp"
	defaultFlushThreshold = 50
)

// tempTxnInsertEventStorage is used to store insert events in the same transaction
// once you begin to read events from storage, you should read all events before you write new events
type tempTxnInsertEventStorage struct {
	events []*commonEvent.RedoDMLEvent
	// when events num exceed flushThreshold, write all events to file
	flushThreshold int
	dir            string
	txnCommitTs    common.Ts

	useFileStorage bool
	// eventSizes is used to store the size of each event in file storage
	eventSizes  []int
	writingFile *os.File
	readingFile *os.File
	// reading is used to indicate whether we are reading events from storage
	// this is to ensure that we read all events before write new events
	reading bool
}

func newTempTxnInsertEventStorage(flushThreshold int, dir string) *tempTxnInsertEventStorage {
	return &tempTxnInsertEventStorage{
		events:         make([]*commonEvent.RedoDMLEvent, 0),
		flushThreshold: flushThreshold,
		dir:            dir,
		txnCommitTs:    0,

		useFileStorage: false,
		eventSizes:     make([]int, 0),

		reading: false,
	}
}

func (t *tempTxnInsertEventStorage) initializeAddEvent(ts common.Ts) {
	t.reading = false
	t.useFileStorage = false
	t.txnCommitTs = ts
	t.writingFile = nil
	t.readingFile = nil
}

func (t *tempTxnInsertEventStorage) addEvent(event *commonEvent.RedoDMLEvent) error {
	// do some pre check
	if !event.IsInsert() {
		log.Panic("event is not insert event", zap.Any("event", event))
	}
	if t.reading && t.hasEvent() {
		log.Panic("should read all events before write new event")
	}
	if !t.hasEvent() {
		t.initializeAddEvent(event.Row.CommitTs)
	} else {
		if t.txnCommitTs != event.Row.CommitTs {
			log.Panic("commit ts of events should be the same",
				zap.Uint64("commitTs", event.Row.CommitTs),
				zap.Uint64("txnCommitTs", t.txnCommitTs))
		}
	}

	if t.useFileStorage {
		return t.writeEventsToFile(event)
	}

	t.events = append(t.events, event)
	if len(t.events) >= t.flushThreshold {
		err := t.writeEventsToFile(t.events...)
		if err != nil {
			return err
		}
		t.events = t.events[:0]
	}
	return nil
}

func (t *tempTxnInsertEventStorage) writeEventsToFile(events ...*commonEvent.RedoDMLEvent) error {
	if !t.useFileStorage {
		t.useFileStorage = true
		var err error
		t.writingFile, err = os.Create(fmt.Sprintf("%s/%s", t.dir, tempStorageFileName))
		if err != nil {
			return err
		}
	}
	for _, e := range events {
		event := e.ToDMLEvent()
		for {
			row, ok := event.GetNextRow()
			if !ok {
				event.Rewind()
				break
			}
			redoRowEvent := &commonEvent.RedoRowEvent{
				StartTs:         event.StartTs,
				CommitTs:        event.CommitTs,
				Event:           row,
				PhysicalTableID: event.PhysicalTableID,
				TableInfo:       event.TableInfo,
				Callback:        event.PostFlush,
			}
			redoLog := redoRowEvent.ToRedoLog()
			data, err := codec.MarshalRedoLog(redoLog, nil)
			if err != nil {
				return errors.WrapError(errors.ErrMarshalFailed, err)
			}
			t.eventSizes = append(t.eventSizes, len(data))
			_, err = t.writingFile.Write(data)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *tempTxnInsertEventStorage) hasEvent() bool {
	return len(t.events) > 0 || len(t.eventSizes) > 0
}

func (t *tempTxnInsertEventStorage) readFromFile() (*commonEvent.RedoDMLEvent, error) {
	if len(t.eventSizes) == 0 {
		return nil, nil
	}
	if t.readingFile == nil {
		var err error
		t.readingFile, err = os.Open(fmt.Sprintf("%s/%s", t.dir, tempStorageFileName))
		if err != nil {
			return nil, err
		}
	}
	size := t.eventSizes[0]
	data := make([]byte, size)
	n, err := t.readingFile.Read(data)
	if err != nil {
		return nil, err
	}
	if n != size {
		return nil, errors.New("read size not equal to expected size")
	}
	t.eventSizes = t.eventSizes[1:]
	redoLog, _, err := codec.UnmarshalRedoLog(data)
	if err != nil {
		return nil, errors.WrapError(errors.ErrUnmarshalFailed, err)
	}
	return redoLog.RedoRow, nil
}

func (t *tempTxnInsertEventStorage) readNextEvent() (*commonEvent.RedoDMLEvent, error) {
	if !t.hasEvent() {
		return nil, nil
	}
	t.reading = true
	if t.useFileStorage {
		return t.readFromFile()
	}

	event := t.events[0]
	t.events = t.events[1:]
	return event, nil
}
