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

package event

import (
	"encoding/binary"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

const (
	// defaultRowCount is the start row count of a transaction.
	defaultRowCount = 1
	// DMLEventVersion is the version of the DMLEvent struct.
	DMLEventVersion = 0
)

// DMLEvent represent a batch of DMLs of a whole or partial of a transaction.
type DMLEvent struct {
	// Version is the version of the DMLEvent struct.
	Version          byte                `json:"version"`
	DispatcherID     common.DispatcherID `json:"dispatcher_id"`
	PhysicalTableID  int64               `json:"physical_table_id"`
	StartTs          uint64              `json:"start_ts"`
	CommitTs         uint64              `json:"commit_ts"`
	TableInfoVersion uint64              `json:"table_info_version"`
	// The seq of the event. It is set by event service.
	Seq uint64 `json:"seq"`
	// State is the state of sender when sending this event.
	State EventSenderState `json:"state"`
	// Length is the number of rows in the transaction.
	Length int32 `json:"length"`
	// RowTypes is the types of every row in the transaction.
	// len(RowTypes) == Length
	// ApproximateSize is the approximate size of all rows in the transaction.
	ApproximateSize int64     `json:"approximate_size"`
	RowTypes        []RowType `json:"row_types"`
	// Rows is the rows of the transaction.
	Rows *chunk.Chunk `json:"rows"`
	// RawRows is the raw bytes of the rows.
	// When the DMLEvent is received from a remote eventService, the Rows is nil.
	// All the data is stored in RawRows.
	// The receiver needs to call DecodeRawRows function to decode the RawRows into Rows.
	RawRows []byte `json:"raw_rows"`

	// TableInfo is the table info of the transaction.
	// If the DMLEvent is send from a remote eventService, the TableInfo is nil.
	TableInfo *common.TableInfo `json:"table_info"`
	// The following fields are set and used by dispatcher.
	ReplicatingTs uint64 `json:"replicating_ts"`
	// PostTxnFlushed is the functions to be executed after the transaction is flushed.
	// It is set and used by dispatcher.
	PostTxnFlushed []func() `json:"-"`

	// eventSize is the size of the event in bytes. It is set when it's unmarshaled.
	eventSize int64 `json:"-"`
	// offset is the offset of the current row in the transaction.
	// It is internal field, not exported. So it doesn't need to be marshalled.
	offset int `json:"-"`

	// Checksum for the event, only not nil if the upstream TiDB enable the row level checksum
	// and TiCDC set the integrity check level to the correctness.
	Checksum       []*integrity.Checksum `json:"-"`
	checksumOffset int                   `json:"-"`
}

func NewDMLEvent(
	dispatcherID common.DispatcherID,
	tableID int64,
	startTs,
	commitTs uint64,
	tableInfo *common.TableInfo,
) *DMLEvent {
	// FIXME: check if chk isFull in the future
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), defaultRowCount)
	return &DMLEvent{
		Version:          DMLEventVersion,
		DispatcherID:     dispatcherID,
		PhysicalTableID:  tableID,
		StartTs:          startTs,
		CommitTs:         commitTs,
		TableInfoVersion: tableInfo.UpdateTS(),
		TableInfo:        tableInfo,
		Rows:             chk,
		RowTypes:         make([]RowType, 0, 1),
	}
}

func (t *DMLEvent) AppendRow(raw *common.RawKVEntry,
	decode func(
		rawKv *common.RawKVEntry,
		tableInfo *common.TableInfo, chk *chunk.Chunk) (int, *integrity.Checksum, error),
) error {
	rowType := RowTypeInsert
	if raw.OpType == common.OpTypeDelete {
		rowType = RowTypeDelete
	}
	if len(raw.Value) != 0 && len(raw.OldValue) != 0 {
		rowType = RowTypeUpdate
	}
	count, checksum, err := decode(raw, t.TableInfo, t.Rows)
	if err != nil {
		return err
	}
	for range count {
		t.RowTypes = append(t.RowTypes, rowType)
	}
	t.Length += 1
	t.ApproximateSize += int64(len(raw.Key) + len(raw.Value) + len(raw.OldValue))
	if checksum != nil {
		t.Checksum = append(t.Checksum, checksum)
	}
	return nil
}

func (t *DMLEvent) GetTableID() int64 {
	return t.PhysicalTableID
}

func (t *DMLEvent) GetType() int {
	return TypeDMLEvent
}

func (t *DMLEvent) GetDispatcherID() common.DispatcherID {
	return t.DispatcherID
}

func (t *DMLEvent) GetCommitTs() common.Ts {
	return t.CommitTs
}

func (t *DMLEvent) GetStartTs() common.Ts {
	return t.StartTs
}

func (t *DMLEvent) PostFlush() {
	for _, f := range t.PostTxnFlushed {
		f()
	}
}

func (t *DMLEvent) GetSeq() uint64 {
	return t.Seq
}

func (t *DMLEvent) PushFrontFlushFunc(f func()) {
	t.PostTxnFlushed = append([]func(){f}, t.PostTxnFlushed...)
}

func (t *DMLEvent) ClearPostFlushFunc() {
	t.PostTxnFlushed = t.PostTxnFlushed[:0]
}

func (t *DMLEvent) AddPostFlushFunc(f func()) {
	t.PostTxnFlushed = append(t.PostTxnFlushed, f)
}

// Rewind reset the offset to 0, So that the next GetNextRow will return the first row
func (t *DMLEvent) Rewind() {
	t.offset = 0
	t.checksumOffset = 0
}

func (t *DMLEvent) GetNextRow() (RowChange, bool) {
	if t.offset >= len(t.RowTypes) {
		return RowChange{}, false
	}
	var checksum *integrity.Checksum
	if len(t.Checksum) != 0 {
		if t.checksumOffset >= len(t.Checksum) {
			return RowChange{}, false
		}
		checksum = t.Checksum[t.checksumOffset]
		t.checksumOffset++
	}
	rowType := t.RowTypes[t.offset]
	switch rowType {
	case RowTypeInsert:
		row := RowChange{
			Row:      t.Rows.GetRow(t.offset),
			RowType:  rowType,
			Checksum: checksum,
		}
		t.offset++
		return row, true
	case RowTypeDelete:
		row := RowChange{
			PreRow:   t.Rows.GetRow(t.offset),
			RowType:  rowType,
			Checksum: checksum,
		}
		t.offset++
		return row, true
	case RowTypeUpdate:
		row := RowChange{
			PreRow:   t.Rows.GetRow(t.offset),
			Row:      t.Rows.GetRow(t.offset + 1),
			RowType:  rowType,
			Checksum: checksum,
		}
		t.offset += 2
		return row, true
	default:
		log.Panic("TEvent.GetNextRow: invalid row type")
	}
	return RowChange{}, false
}

// Len returns the number of row change events in the transaction.
// Note: An update event is counted as 1 row.
func (t *DMLEvent) Len() int32 {
	return t.Length
}

func (t *DMLEvent) Marshal() ([]byte, error) {
	return t.encode()
}

// Unmarshal the DMLEvent from the given data.
// Please make sure the TableInfo of the DMLEvent is set before unmarshal.
func (t *DMLEvent) Unmarshal(data []byte) error {
	t.eventSize = int64(len(data))
	return t.decode(data)
}

// GetSize returns the size of the event in bytes, including all fields.
func (t *DMLEvent) GetSize() int64 {
	// Notice: events send from local channel will not have the size field.
	// return t.eventSize
	return t.GetRowsSize()
}

// GetRowsSize returns the approximate size of the rows in the transaction.
func (t *DMLEvent) GetRowsSize() int64 {
	return t.ApproximateSize
}

func (t *DMLEvent) IsPaused() bool {
	return t.State.IsPaused()
}

func (t *DMLEvent) encode() ([]byte, error) {
	if t.Version != 0 {
		log.Panic("DMLEvent: Only version 0 is supported right now", zap.Uint8("version", t.Version))
		return nil, nil
	}
	return t.encodeV0()
}

func (t *DMLEvent) encodeV0() ([]byte, error) {
	if t.Version != 0 {
		log.Panic("DMLEvent: invalid version, expect 0, got ", zap.Uint8("version", t.Version))
		return nil, nil
	}
	// Calculate the total size needed for the encoded data
	size := 1 + t.DispatcherID.GetSize() + 6*8 + 4 + t.State.GetSize() + int(t.Length)

	// Allocate a buffer with the calculated size
	buf := make([]byte, size)
	offset := 0

	// Encode all fields
	// Version
	buf[offset] = t.Version
	offset += 1

	// DispatcherID
	dispatcherIDBytes := t.DispatcherID.Marshal()
	copy(buf[offset:], dispatcherIDBytes)
	offset += len(dispatcherIDBytes)

	// PhysicalTableID
	binary.LittleEndian.PutUint64(buf[offset:], uint64(t.PhysicalTableID))
	offset += 8
	// StartTs
	binary.LittleEndian.PutUint64(buf[offset:], t.StartTs)
	offset += 8
	// CommitTs
	binary.LittleEndian.PutUint64(buf[offset:], t.CommitTs)
	offset += 8
	// TableInfoVersion
	binary.LittleEndian.PutUint64(buf[offset:], t.TableInfoVersion)
	offset += 8
	// Seq
	binary.LittleEndian.PutUint64(buf[offset:], t.Seq)
	offset += 8
	// State
	copy(buf[offset:], t.State.encode())
	offset += t.State.GetSize()
	// Length
	binary.LittleEndian.PutUint32(buf[offset:], uint32(t.Length))
	offset += 4
	// ApproximateSize
	binary.LittleEndian.PutUint64(buf[offset:], uint64(t.ApproximateSize))
	offset += 8
	// RowTypes
	for _, rowType := range t.RowTypes {
		buf[offset] = byte(rowType)
		offset++
	}

	encoder := chunk.NewCodec(t.TableInfo.GetFieldSlice())
	data := encoder.Encode(t.Rows)

	// Append the encoded data to the buffer
	result := append(buf, data...)

	return result, nil
}

func (t *DMLEvent) decode(data []byte) error {
	t.Version = data[0]
	if t.Version != 0 {
		log.Panic("DMLEvent: Only version 0 is supported right now", zap.Uint8("version", t.Version))
		return nil
	}
	return t.decodeV0(data)
}

func (t *DMLEvent) decodeV0(data []byte) error {
	if t.Version != 0 {
		log.Panic("DMLEvent: invalid version, expect 0, got ", zap.Uint8("version", t.Version))
		return nil
	}
	offset := 1
	t.DispatcherID.Unmarshal(data[offset:])
	offset += t.DispatcherID.GetSize()
	t.PhysicalTableID = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	t.StartTs = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	t.CommitTs = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	t.TableInfoVersion = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	t.Seq = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	t.State.decode(data[offset:])
	offset += t.State.GetSize()
	t.Length = int32(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	t.ApproximateSize = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	t.RowTypes = make([]RowType, t.Length)
	for i := 0; i < int(t.Length); i++ {
		t.RowTypes[i] = RowType(data[offset])
		offset++
	}
	t.RawRows = data[offset:]
	return nil
}

// AssembleRows assembles the Rows from the RawRows.
// It also sets the TableInfo and clears the RawRows.
func (t *DMLEvent) AssembleRows(tableInfo *common.TableInfo) {
	defer t.TableInfo.InitPrivateFields()
	// t.Rows is already set, no need to assemble again
	// When the event is passed from the same node, the Rows is already set.
	if t.Rows != nil {
		return
	}
	if tableInfo == nil {
		log.Panic("DMLEvent: TableInfo is nil")
		return
	}
	if len(t.RawRows) == 0 {
		log.Panic("DMLEvent: RawRows is empty")
		return
	}
	if t.TableInfoVersion != tableInfo.UpdateTS() {
		log.Panic("DMLEvent: TableInfoVersion mismatch", zap.Uint64("dmlEventTableInfoVersion", t.TableInfoVersion), zap.Uint64("tableInfoVersion", tableInfo.UpdateTS()))
		return
	}
	decoder := chunk.NewCodec(tableInfo.GetFieldSlice())
	t.Rows, _ = decoder.Decode(t.RawRows)
	t.TableInfo = tableInfo
	t.RawRows = nil
}

type RowChange struct {
	PreRow   chunk.Row
	Row      chunk.Row
	RowType  RowType
	Checksum *integrity.Checksum
}

type RowType byte

const (
	// RowTypeDelete represents a delete row.
	RowTypeDelete RowType = iota
	// RowTypeInsert represents a insert row.
	RowTypeInsert
	// RowTypeUpdate represents a update row.
	RowTypeUpdate
)
