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
	"sync/atomic"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
)

// DMLMessageData keeps the original encoded input needed to restore a DML
// message after it has been spilled. One input can be attached to multiple
// DMLMessages; Attach assigns each message its ordinal in that input.
type DMLMessageData struct {
	ID    uint64
	Key   []byte
	Value []byte
	// Decode restores every DML message from one encoded input. The spill
	// store owns the decoded result and maps messages by their attached ordinal.
	Decode func([]byte) ([]*DMLMessage, error)

	nextDMLIndex uint64
}

var nextDMLMessageDataID atomic.Uint64

// NewDMLMessageData creates data shared by DMLMessages decoded from one input.
func NewDMLMessageData(
	key, value []byte,
	decode func([]byte) ([]*DMLMessage, error),
) *DMLMessageData {
	return &DMLMessageData{
		ID:     nextDMLMessageDataID.Add(1),
		Key:    key,
		Value:  value,
		Decode: decode,
	}
}

type DMLMessage struct {
	TableID int64
	Schema  string
	Table   string
	RowType commonType.RowType

	commitTs uint64
	// toDMLEvent may be called after the decoder has consumed later messages.
	// It must only use data captured by this DMLMessage and must not depend on decoder cursor state.
	toDMLEvent func() *commonEvent.DMLEvent
	spillData  *DMLMessageData
	dmlIndex   uint64
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

// AttachDMLMessageData attaches the original input required to restore this
// message after spill. It must be called once for every decoded DML, including
// DMLs the consumer later discards.
func (d *DMLMessageData) AttachDMLMessage(message *DMLMessage) {
	message.spillData = d
	message.dmlIndex = d.nextDMLIndex
	d.nextDMLIndex++
}

// SpillData returns the data and ordinal attached while this message was
// decoded. They are used by the consumer's in-memory events group only.
func (m *DMLMessage) SpillData() (*DMLMessageData, uint64) {
	return m.spillData, m.dmlIndex
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
