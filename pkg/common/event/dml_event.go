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
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

const (
	// defaultRowCount is the start row count of a transaction.
	defaultRowCount = 1
	// DMLEventVersion is the version of the DMLEvent struct.
	DMLEventVersion = 1
)

var _ Event = &BatchDMLEvent{}

type BatchDMLEvent struct {
	// Version is the version of the BatchDMLEvent struct.
	Version   byte        `json:"version"`
	DMLEvents []*DMLEvent `json:"dml_events"`
	// Rows is the rows of the transactions.
	Rows *chunk.Chunk `json:"rows"`
	// RawRows is the raw bytes of the rows.
	// When the DMLEvent is received from a remote eventService, the Rows is nil.
	// All the data is stored in RawRows.
	// The receiver needs to call DecodeRawRows function to decode the RawRows into Rows.
	RawRows   []byte            `json:"raw_rows"`
	TableInfo *common.TableInfo `json:"table_info"`
}

func (b *BatchDMLEvent) String() string {
	return fmt.Sprintf("BatchDMLEvent{Version: %d, DMLEvents: %v, Rows: %v, RawRows: %v, Table: %v, Len: %d}",
		b.Version, b.DMLEvents, b.Rows, b.RawRows, b.TableInfo.TableName, b.Len())
}

// NewBatchDMLEvent creates a new BatchDMLEvent with proper initialization
func NewBatchDMLEvent() *BatchDMLEvent {
	return &BatchDMLEvent{
		Version:   0,
		DMLEvents: make([]*DMLEvent, 0),
	}
}

// PopHeadDMLEvents pops the first `count` DMLEvents from the BatchDMLEvent and returns a new BatchDMLEvent.
func (b *BatchDMLEvent) PopHeadDMLEvents(count int) *BatchDMLEvent {
	if count <= 0 || len(b.DMLEvents) == 0 {
		return nil
	}
	if count > len(b.DMLEvents) {
		count = len(b.DMLEvents)
	}
	newBatch := &BatchDMLEvent{
		Version:   b.Version,
		DMLEvents: make([]*DMLEvent, 0, count),
		Rows:      b.Rows,
		TableInfo: b.TableInfo,
	}
	for i := 0; i < count; i++ {
		newBatch.DMLEvents = append(newBatch.DMLEvents, b.DMLEvents[i])
	}
	b.DMLEvents = b.DMLEvents[count:]
	return newBatch
}

// AddDMLEvent adds a completed DMLEvent to the BatchDMLEvent
// The DMLEvent should already have all its rows populated
func (b *BatchDMLEvent) AppendDMLEvent(dmlEvent *DMLEvent) {
	if dmlEvent == nil {
		return
	}

	if b.TableInfo == nil {
		b.TableInfo = dmlEvent.TableInfo
		b.Rows = chunk.NewChunkWithCapacity(dmlEvent.TableInfo.GetFieldSlice(), defaultRowCount)
	}
	dmlEvent.SetRows(b.Rows)

	if len(b.DMLEvents) > 0 {
		pre := b.DMLEvents[len(b.DMLEvents)-1]
		dmlEvent.PreviousTotalOffset = pre.PreviousTotalOffset + len(pre.RowTypes)
	}
	// Set the shared Rows chunk
	dmlEvent.Rows = b.Rows
	b.DMLEvents = append(b.DMLEvents, dmlEvent)
}

func (b *BatchDMLEvent) Unmarshal(data []byte) error {
	return b.decodeV0(data)
}

func (b *BatchDMLEvent) decodeV0(data []byte) error {
	if len(data) < 1+8*3 {
		return errors.ErrDecodeFailed.FastGenByArgs("data length is less than the minimum value")
	}
	b.Version = data[0]
	if b.Version != 0 {
		log.Panic("BatchDMLEvent: Only version 0 is supported right now", zap.Uint8("version", b.Version))
		return nil
	}
	offset := 1
	length := int(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	b.DMLEvents = make([]*DMLEvent, 0, length)
	for i := 0; i < length; i++ {
		event := &DMLEvent{}
		eventDataSize := int(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		err := event.Unmarshal(data[offset:])
		if err != nil {
			return err
		}
		b.DMLEvents = append(b.DMLEvents, event)
		offset += eventDataSize
	}
	b.RawRows = data[offset:]
	return nil
}

func (b *BatchDMLEvent) Marshal() ([]byte, error) {
	return b.encodeV0()
}

func (b *BatchDMLEvent) encodeV0() ([]byte, error) {
	if b.Version != 0 {
		log.Panic("BatchDMLEvent: Only version 0 is supported right now", zap.Uint8("version", b.Version))
		return nil, nil
	}
	size := 1 + 8 + (1+16+6*8+4*2+1)*len(b.DMLEvents) + int(b.Len())
	data := make([]byte, 0, size)
	// Encode all fields
	// Version
	data = append(data, b.Version)
	// DMLEvents
	dmlEventsDataSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(dmlEventsDataSize, uint64(len(b.DMLEvents)))
	data = append(data, dmlEventsDataSize...)
	for _, event := range b.DMLEvents {
		buff, err := event.Marshal()
		if err != nil {
			return nil, err
		}
		eventDataSize := make([]byte, 8)
		binary.BigEndian.PutUint64(eventDataSize, uint64(len(buff)))
		data = append(data, eventDataSize...)
		data = append(data, buff...)
	}
	encoder := chunk.NewCodec(b.TableInfo.GetFieldSlice())
	value := encoder.Encode(b.Rows)
	// Append the encoded value to the buffer
	data = append(data, value...)
	return data, nil
}

// AssembleRows assembles the Rows from the RawRows.
// It also sets the TableInfo and clears the RawRows.
func (b *BatchDMLEvent) AssembleRows(tableInfo *common.TableInfo) {
	defer func() {
		b.TableInfo.InitPrivateFields()
	}()
	// rows is already set, no need to assemble again
	// When the event is passed from the same node, the Rows is already set.
	if b.Rows != nil {
		return
	}
	if tableInfo == nil {
		log.Panic("DMLEvent: TableInfo is nil")
		return
	}

	if len(b.RawRows) == 0 {
		log.Panic("DMLEvent: RawRows is empty")
		return
	}

	if b.TableInfo != nil && b.TableInfo.UpdateTS() != tableInfo.UpdateTS() {
		log.Panic("DMLEvent: TableInfoVersion mismatch", zap.Uint64("dmlEventTableInfoVersion", b.TableInfo.UpdateTS()), zap.Uint64("tableInfoVersion", tableInfo.UpdateTS()))
		return
	}
	decoder := chunk.NewCodec(tableInfo.GetFieldSlice())
	b.Rows, _ = decoder.Decode(b.RawRows)
	b.TableInfo = tableInfo
	b.RawRows = nil
	for _, dml := range b.DMLEvents {
		dml.Rows = b.Rows
		dml.TableInfo = b.TableInfo
	}
}

func (b *BatchDMLEvent) GetType() int {
	return TypeBatchDMLEvent
}

func (b *BatchDMLEvent) GetSeq() uint64 {
	return b.DMLEvents[len(b.DMLEvents)-1].Seq
}

func (b *BatchDMLEvent) GetEpoch() uint64 {
	return b.DMLEvents[len(b.DMLEvents)-1].Epoch
}

func (b *BatchDMLEvent) GetDispatcherID() common.DispatcherID {
	return b.DMLEvents[len(b.DMLEvents)-1].DispatcherID
}

func (b *BatchDMLEvent) GetCommitTs() common.Ts {
	return b.DMLEvents[len(b.DMLEvents)-1].GetCommitTs()
}

func (b *BatchDMLEvent) GetStartTs() common.Ts {
	return b.DMLEvents[0].GetStartTs()
}

func (b *BatchDMLEvent) GetSize() uint64 {
	var size uint64
	for _, item := range b.DMLEvents {
		size += item.GetSize()
	}
	return size
}

func (b *BatchDMLEvent) IsPaused() bool {
	return b.DMLEvents[len(b.DMLEvents)-1].IsPaused()
}

// Len returns the number of DML events in the batch.
func (b *BatchDMLEvent) Len() int32 {
	var length int32
	for _, dml := range b.DMLEvents {
		length += dml.Len()
	}
	return length
}

// DMLEvent represent a batch of DMLs of a whole or partial of a transaction.
type DMLEvent struct {
	// Version is the version of the DMLEvent struct.
	Version         byte                `json:"version"`
	DispatcherID    common.DispatcherID `json:"dispatcher_id"`
	PhysicalTableID int64               `json:"physical_table_id"`
	StartTs         uint64              `json:"start_ts"`
	CommitTs        uint64              `json:"commit_ts"`
	// The seq of the event. It is set by event service.
	Seq uint64 `json:"seq"`
	// Epoch is the epoch of the event. It is set by event service.
	Epoch uint64 `json:"epoch"`
	// State is the state of sender when sending this event.
	State EventSenderState `json:"state"`
	// Length is the number of rows in the transaction.
	Length int32 `json:"length"`
	// ApproximateSize is the approximate size of all rows in the transaction.
	ApproximateSize uint64 `json:"approximate_size"`
	// RowTypes is the types of every row in the transaction.
	RowTypes []RowType `json:"row_types"`
	// Rows shares BatchDMLEvent rows
	Rows *chunk.Chunk `json:"-"`

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
	// PreviousTotalOffset accumulates the offsets of all previous DML events to facilitate sharing the same chunk when using batch DML events.
	// It is used to determine the correct offset for the chunk in batch DML operations.
	PreviousTotalOffset int `json:"previous_total_offset"`

	// Checksum for the event, only not nil if the upstream TiDB enable the row level checksum
	// and TiCDC set the integrity check level to the correctness.
	Checksum       []*integrity.Checksum `json:"-"`
	checksumOffset int                   `json:"-"`
}

func (t *DMLEvent) String() string {
	return fmt.Sprintf("DMLEvent{Version: %d, DispatcherID: %s, Seq: %d, PhysicalTableID: %d, StartTs: %d, CommitTs: %d, Table: %v, Checksum: %v, Length: %d, ApproximateSize: %d}",
		t.Version, t.DispatcherID.String(), t.Seq, t.PhysicalTableID, t.StartTs, t.CommitTs, t.TableInfo.TableName, t.Checksum, t.Length, t.ApproximateSize)
}

// NewDMLEvent creates a new DMLEvent with the given parameters
func NewDMLEvent(
	dispatcherID common.DispatcherID,
	tableID int64,
	startTs,
	commitTs uint64,
	tableInfo *common.TableInfo,
) *DMLEvent {
	return &DMLEvent{
		Version:         DMLEventVersion,
		DispatcherID:    dispatcherID,
		PhysicalTableID: tableID,
		StartTs:         startTs,
		CommitTs:        commitTs,
		TableInfo:       tableInfo,
		RowTypes:        make([]RowType, 0),
	}
}

// SetRows sets the Rows chunk for this DMLEvent
func (t *DMLEvent) SetRows(rows *chunk.Chunk) {
	t.Rows = rows
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
	t.ApproximateSize += uint64(len(raw.Key) + len(raw.Value) + len(raw.OldValue))
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

// GetCommitTs returns current transaction commitTs
func (t *DMLEvent) GetCommitTs() common.Ts {
	return t.CommitTs
}

// GetStartTs returns the first transaction startTs
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

func (t *DMLEvent) GetEpoch() uint64 {
	return t.Epoch
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
			Row:      t.Rows.GetRow(t.PreviousTotalOffset + t.offset),
			RowType:  rowType,
			Checksum: checksum,
		}
		t.offset++
		return row, true
	case RowTypeDelete:
		row := RowChange{
			PreRow:   t.Rows.GetRow(t.PreviousTotalOffset + t.offset),
			RowType:  rowType,
			Checksum: checksum,
		}
		t.offset++
		return row, true
	case RowTypeUpdate:
		row := RowChange{
			PreRow:   t.Rows.GetRow(t.PreviousTotalOffset + t.offset),
			Row:      t.Rows.GetRow(t.PreviousTotalOffset + t.offset + 1),
			RowType:  rowType,
			Checksum: checksum,
		}
		t.offset += 2
		return row, true
	default:
		log.Panic("DMLEvent.GetNextRow: invalid row type")
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
func (t *DMLEvent) GetSize() uint64 {
	// Notice: events send from local channel will not have the size field.
	// return t.eventSize
	return t.GetRowsSize()
}

// GetRowsSize returns the approximate size of the rows in the transaction.
func (t *DMLEvent) GetRowsSize() uint64 {
	return t.ApproximateSize
}

func (t *DMLEvent) IsPaused() bool {
	return t.State.IsPaused()
}

func (t *DMLEvent) encode() ([]byte, error) {
	if t.Version != DMLEventVersion {
		log.Panic("DMLEvent: unexpected version", zap.Uint8("expected", DMLEventVersion), zap.Uint8("version", t.Version))
		return nil, nil
	}
	return t.encodeV0()
}

func (t *DMLEvent) encodeV0() ([]byte, error) {
	if t.Version != DMLEventVersion {
		log.Panic("DMLEvent: unexpected version", zap.Uint8("expected", DMLEventVersion), zap.Uint8("version", t.Version))
		return nil, nil
	}
	// Calculate the total size needed for the encoded data
	size := 1 + t.DispatcherID.GetSize() + 5*8 + 4*3 + t.State.GetSize() + uint64(t.Length)

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
	// Seq
	binary.LittleEndian.PutUint64(buf[offset:], t.Seq)
	offset += 8
	// State
	copy(buf[offset:], t.State.encode())
	offset += int(t.State.GetSize())
	// Length
	binary.LittleEndian.PutUint32(buf[offset:], uint32(t.Length))
	offset += 4
	// ApproximateSize
	binary.LittleEndian.PutUint64(buf[offset:], t.ApproximateSize)
	offset += 8
	// PreviousTotalOffset
	binary.LittleEndian.PutUint32(buf[offset:], uint32(t.PreviousTotalOffset))
	offset += 4
	// RowTypes
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(t.RowTypes)))
	offset += 4
	for _, rowType := range t.RowTypes {
		buf[offset] = byte(rowType)
		offset++
	}
	return buf, nil
}

func (t *DMLEvent) decode(data []byte) error {
	t.Version = data[0]
	if t.Version != DMLEventVersion {
		log.Panic("DMLEvent: unexpected version", zap.Uint8("expected", DMLEventVersion), zap.Uint8("version", t.Version))
		return nil
	}
	return t.decodeV0(data)
}

func (t *DMLEvent) decodeV0(data []byte) error {
	if len(data) < 1+16+8*5+4*3 {
		return errors.ErrDecodeFailed.FastGenByArgs("data length is less than the minimum value")
	}
	if t.Version != DMLEventVersion {
		log.Panic("DMLEvent: unexpected version", zap.Uint8("expected", DMLEventVersion), zap.Uint8("version", t.Version))
		return nil
	}
	offset := 1
	t.DispatcherID.Unmarshal(data[offset:])
	offset += int(t.DispatcherID.GetSize())
	t.PhysicalTableID = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	t.StartTs = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	t.CommitTs = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	t.Seq = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	t.State.decode(data[offset:])
	offset += int(t.State.GetSize())
	t.Length = int32(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	t.ApproximateSize = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	t.PreviousTotalOffset = int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	length := int32(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	t.RowTypes = make([]RowType, length)
	for i := 0; i < int(length); i++ {
		t.RowTypes[i] = RowType(data[offset])
		offset++
	}
	return nil
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

func (r RowType) String() string {
	switch r {
	case RowTypeDelete:
		return "delete"
	case RowTypeInsert:
		return "insert"
	case RowTypeUpdate:
		return "update"
	default:
	}
	log.Panic("RowType: invalid row type", zap.Uint8("rowType", uint8(r)))
	return ""
}
