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

package common

import (
<<<<<<< HEAD
=======
	"sync/atomic"

	"github.com/pingcap/log"
>>>>>>> 90a0a9aa4 (event: serialize row checksum in dml event (#6343))
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

type DMLMessage struct {
	TableID int64
	Schema  string
	Table   string
	RowType commonType.RowType

	commitTs uint64
	// toDMLEvent may be called after the decoder has consumed later messages.
	// It must only use data captured by this DMLMessage and must not depend on decoder cursor state.
	toDMLEvent func() *commonEvent.DMLEvent
}

func NewDMLMessage(
	tableID int64,
	schema string,
	table string,
	commitTs uint64,
	rowType commonType.RowType,
	toDMLEvent func() *commonEvent.DMLEvent,
) *DMLMessage {
	return &DMLMessage{
		TableID:    tableID,
		Schema:     schema,
		Table:      table,
		RowType:    rowType,
		commitTs:   commitTs,
		toDMLEvent: toDMLEvent,
	}
}

func NewDMLMessageFromEvent(event *commonEvent.DMLEvent) *DMLMessage {
	var (
		schema  string
		table   string
		rowType commonType.RowType
	)
	if event.TableInfo != nil {
		schema = event.TableInfo.GetSchemaName()
		table = event.TableInfo.GetTableName()
	}
	if len(event.RowTypes) > 0 {
		rowType = event.RowTypes[0]
	}
	return NewDMLMessage(event.GetTableID(), schema, table, event.GetCommitTs(), rowType, func() *commonEvent.DMLEvent {
		return event
	})
}

func (m *DMLMessage) GetCommitTs() uint64 {
	return m.commitTs
}

// ToDMLEvent materializes a decoded DML message. Once a message exists, decoding
// must produce an event or stop with an error; nil does not mean a skipped row.
func (m *DMLMessage) ToDMLEvent() *commonEvent.DMLEvent {
	event := m.toDMLEvent()
	if event == nil {
		log.Panic("DML message decoded to nil event",
			zap.String("schema", m.Schema), zap.String("table", m.Table),
			zap.Int64("tableID", m.TableID), zap.Uint64("commitTs", m.commitTs),
			zap.Error(errors.ErrDecodeFailed.GenWithStackByArgs("decoder returned a nil DML event")))
	}
	return event
}

// Decoder is an abstraction for events decoder
// this interface is only for testing now
type Decoder interface {
	// AddKeyValue add the received key and values to the decoder,
	// should be called before `HasNext`
	// decoder decode the key and value into the event format.
	AddKeyValue(key, value []byte)

	// HasNext returns
	//     1. the type of the next event
	//     2. a bool if the next event is exist
	//     3. error
	HasNext() (MessageType, bool)

	// NextResolvedEvent returns the next resolved event if exists
	NextResolvedEvent() uint64

	// NextDMLMessage returns the next DML message if exists
	NextDMLMessage() *DMLMessage

	// NextDDLEvent returns the next DDL event if exists
	NextDDLEvent() *commonEvent.DDLEvent
}
