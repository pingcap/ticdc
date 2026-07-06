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
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
)

type DMLMessage struct {
	TableID int64
	Schema  string
	Table   string
	RowType commonType.RowType

	commitTs   uint64
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

func (m *DMLMessage) ToDMLEvent() *commonEvent.DMLEvent {
	return m.toDMLEvent()
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
