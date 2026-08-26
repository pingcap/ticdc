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

package util

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"sort"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/spill"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

const eventsGroupSpillPattern = "ticdc-events-group-*.spill"

type spilledMessage struct {
	commitTs    uint64
	handle      spill.Handle
	postRestore func(*codeccommon.DMLMessage) *codeccommon.DMLMessage
}

// EventsGroup stores change event messages.
type EventsGroup struct {
	Partition int32
	tableID   int64

	messages      []spilledMessage
	spillFile     *spill.RecordFile
	outOfOrder    bool
	HighWatermark uint64
}

// NewEventsGroup will create new event group.
func NewEventsGroup(partition int32, tableID int64) *EventsGroup {
	return &EventsGroup{
		Partition: partition,
		tableID:   tableID,
		messages:  make([]spilledMessage, 0, 1024),
	}
}

// AppendMessage materializes a message and appends it to a local spill file. DMLMessage carries a
// decoder closure, so persisting its reconstructed event is necessary to release the decoder input
// retained by that closure.
func (g *EventsGroup) AppendMessage(message *codeccommon.DMLMessage) error {
	return g.appendMessage(message, nil)
}

// AppendMessageWithPostRestore appends a message and applies postRestore after it is read back from
// disk. It keeps consumer checks that intentionally run immediately before flushing out of the
// on-disk representation.
func (g *EventsGroup) AppendMessageWithPostRestore(
	message *codeccommon.DMLMessage,
	postRestore func(*codeccommon.DMLMessage) *codeccommon.DMLMessage,
) error {
	return g.appendMessage(message, postRestore)
}

func (g *EventsGroup) appendMessage(
	message *codeccommon.DMLMessage,
	postRestore func(*codeccommon.DMLMessage) *codeccommon.DMLMessage,
) error {
	if message == nil {
		return errors.ErrSpillFileOp.FastGenByArgs("cannot spill nil DML message")
	}
	commitTs := message.GetCommitTs()

	data, _, err := marshalDMLMessage(message)
	if err != nil {
		return err
	}
	if g.spillFile == nil {
		g.spillFile, err = spill.NewRecordFile(os.TempDir(), eventsGroupSpillPattern)
		if err != nil {
			return err
		}
	}
	handle, err := g.spillFile.Append(data)
	if err != nil {
		return err
	}
	if len(g.messages) > 0 && commitTs < g.messages[len(g.messages)-1].commitTs {
		g.outOfOrder = true
	}
	if commitTs > g.HighWatermark {
		g.HighWatermark = commitTs
	}
	g.messages = append(g.messages, spilledMessage{
		commitTs:    commitTs,
		handle:      handle,
		postRestore: postRestore,
	})
	return nil
}

// ResolveInto appends all messages with CommitTs <= resolve into dst in commit-ts order and removes
// them from the group. Resolved messages are restored from the spill file only when downstream needs
// them, keeping the buffered group out of heap memory.
func (g *EventsGroup) ResolveInto(resolve uint64, dst []*codeccommon.DMLMessage) ([]*codeccommon.DMLMessage, error) {
	if len(g.messages) == 0 {
		return dst, nil
	}

	if g.outOfOrder {
		sort.SliceStable(g.messages, func(i, j int) bool {
			return g.messages[i].commitTs < g.messages[j].commitTs
		})
	}

	resolvedCount := sort.Search(len(g.messages), func(i int) bool {
		return g.messages[i].commitTs > resolve
	})
	if g.outOfOrder {
		log.Warn("DML events are out of order before flush, sort them",
			zap.Int32("partition", g.Partition),
			zap.Int64("tableID", g.tableID),
			zap.Uint64("resolveTs", resolve),
			zap.Int("resolved", resolvedCount))
		g.outOfOrder = false
	}
	if resolvedCount == 0 {
		return dst, nil
	}
	if g.spillFile == nil {
		return dst, errors.ErrSpillFileOp.FastGenByArgs("events group spill file is missing")
	}

	for _, message := range g.messages[:resolvedCount] {
		data, err := g.spillFile.Read(message.handle)
		if err != nil {
			return dst, err
		}
		restored, err := unmarshalDMLMessage(data)
		if err != nil {
			return dst, err
		}
		if message.postRestore != nil {
			restored = message.postRestore(restored)
		}
		dst = append(dst, restored)
	}
	remainingCount := len(g.messages) - resolvedCount
	copy(g.messages, g.messages[resolvedCount:])
	clear(g.messages[remainingCount:])
	g.messages = g.messages[:remainingCount]
	if len(g.messages) == 0 {
		if err := g.spillFile.Cleanup(); err != nil {
			return dst, err
		}
		g.spillFile = nil
	}
	if len(g.messages) != 0 {
		firstCommitTs := g.messages[0].commitTs
		log.Debug("not all events resolved",
			zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID),
			zap.Int("resolved", resolvedCount), zap.Int("remained", len(g.messages)),
			zap.Uint64("resolveTs", resolve), zap.Uint64("firstCommitTs", firstCommitTs))
	}
	return dst, nil
}

// GetAllMessages gets all messages.
func (g *EventsGroup) GetAllMessages() ([]*codeccommon.DMLMessage, error) {
	return g.ResolveInto(math.MaxUint64, nil)
}

// Cleanup removes pending spill records when the consumer is stopping.
func (g *EventsGroup) Cleanup() error {
	if g.spillFile == nil {
		return nil
	}
	err := g.spillFile.Cleanup()
	if err != nil {
		return err
	}
	g.spillFile = nil
	clear(g.messages)
	g.messages = g.messages[:0]
	return nil
}

// AppendOrMergeDMLEvent appends row to events, or merges it into the preceding
// event when both are compatible parts of the same transaction. Events with the
// same commit-ts from different sources can use different table schemas, so a
// commit-ts alone is not enough to merge their chunks safely.
func AppendOrMergeDMLEvent(events []*commonEvent.DMLEvent, row *commonEvent.DMLEvent) []*commonEvent.DMLEvent {
	if len(events) == 0 || !canMergeDMLEvents(events[len(events)-1], row) {
		return append(events, row)
	}

	last := events[len(events)-1]
	lastRowTypeCount := len(last.RowTypes)
	rowRowTypeCount := len(row.RowTypes)
	last.Rows.Append(row.Rows, 0, row.Rows.NumRows())
	last.RowTypes = append(last.RowTypes, row.RowTypes...)
	last.RowKeys = appendOptionalDMLValues(last.RowKeys, row.RowKeys, lastRowTypeCount, rowRowTypeCount)
	last.Checksum = appendOptionalDMLValues(last.Checksum, row.Checksum, lastRowTypeCount, rowRowTypeCount)
	last.Length += row.Length
	last.ApproximateSize += row.ApproximateSize
	last.PostTxnEnqueued = append(last.PostTxnEnqueued, row.PostTxnEnqueued...)
	last.PostTxnFlushed = append(last.PostTxnFlushed, row.PostTxnFlushed...)
	return events
}

func canMergeDMLEvents(last, row *commonEvent.DMLEvent) bool {
	if last == nil || row == nil ||
		last.CommitTs != row.CommitTs ||
		last.StartTs != row.StartTs ||
		last.DispatcherID != row.DispatcherID ||
		last.PhysicalTableID != row.PhysicalTableID ||
		last.TableInfoVersion != row.TableInfoVersion ||
		last.TableInfo == nil || row.TableInfo == nil ||
		last.TableInfo.GetSchemaName() != row.TableInfo.GetSchemaName() ||
		last.TableInfo.GetTableName() != row.TableInfo.GetTableName() ||
		last.TableInfo.GetUpdateTS() != row.TableInfo.GetUpdateTS() ||
		last.Rows == nil || row.Rows == nil ||
		last.Rows.NumCols() != row.Rows.NumCols() ||
		last.PreviousTotalOffset != 0 || row.PreviousTotalOffset != 0 ||
		!reflect.DeepEqual(last.TableInfo.GetFieldSlice(), row.TableInfo.GetFieldSlice()) {
		return false
	}

	return hasOptionalDMLValues(last.RowKeys, len(last.RowTypes)) &&
		hasOptionalDMLValues(row.RowKeys, len(row.RowTypes)) &&
		hasOptionalDMLValues(last.Checksum, len(last.RowTypes)) &&
		hasOptionalDMLValues(row.Checksum, len(row.RowTypes))
}

func hasOptionalDMLValues[T any](values []T, rowTypeCount int) bool {
	return len(values) == 0 || len(values) == rowTypeCount
}

func appendOptionalDMLValues[T any](last, row []T, lastRowTypeCount, rowRowTypeCount int) []T {
	if len(last) == 0 && len(row) != 0 {
		last = make([]T, lastRowTypeCount)
	} else if len(last) != 0 && len(row) == 0 {
		row = make([]T, rowRowTypeCount)
	}
	return append(last, row...)
}

func marshalDMLMessage(message *codeccommon.DMLMessage) (data []byte, row *commonEvent.DMLEvent, err error) {
	if message == nil {
		return nil, nil, errors.ErrSpillFileOp.FastGenByArgs("cannot spill nil DML message")
	}

	row = message.ToDMLEvent()
	if row == nil {
		return nil, nil, errors.ErrSpillFileOp.FastGenByArgs("cannot spill DML message without event")
	}
	if row.Version == 0 {
		row.Version = commonEvent.DMLEventVersion1
	}
	// Rows can be shared by several DML events. Persist only this event's rows
	// below, so its offset must be reset in the serialized event as well.
	event := *row
	event.PreviousTotalOffset = 0
	eventData, err := event.Marshal()
	if err != nil {
		return nil, nil, errors.WrapError(errors.ErrSpillFileOp, err, "marshal DML event")
	}

	var tableInfoData []byte
	tableInfoStored := false
	if row.TableInfo != nil {
		tableInfoData, err = marshalDMLTableInfo(row.TableInfo)
		if err != nil {
			if row.Rows != nil && row.Rows.NumRows() > 0 {
				return nil, nil, err
			}
			log.Warn("spill DML event without table info",
				zap.Int64("tableID", row.PhysicalTableID), zap.Uint64("commitTs", row.CommitTs), zap.Error(err))
			tableInfoData = nil
		} else {
			tableInfoStored = true
		}
	}

	var rowsData []byte
	if row.Rows != nil && (row.Rows.NumRows() > 0 || tableInfoStored) {
		rowsData, err = marshalDMLRows(row, tableInfoStored)
		if err != nil {
			if row.Rows.NumRows() > 0 {
				return nil, nil, err
			}
			log.Warn("spill DML event without row data",
				zap.Int64("tableID", row.PhysicalTableID), zap.Uint64("commitTs", row.CommitTs), zap.Error(err))
			rowsData = nil
		}
	}

	checksumData, err := json.Marshal(row.Checksum)
	if err != nil {
		return nil, nil, errors.WrapError(errors.ErrSpillFileOp, err, "marshal DML checksums")
	}

	data = make([]byte, 0, 10*8+len(eventData)+len(tableInfoData)+len(rowsData)+len(checksumData)+len(message.Schema)+len(message.Table))
	data = appendUint64(data, uint64(len(eventData)))
	data = append(data, eventData...)
	data = appendUint64(data, uint64(len(tableInfoData)))
	data = append(data, tableInfoData...)
	data = appendUint64(data, uint64(len(rowsData)))
	data = append(data, rowsData...)
	data = appendUint64(data, uint64(len(checksumData)))
	data = append(data, checksumData...)
	data = appendUint64(data, uint64(len(message.Schema)))
	data = append(data, message.Schema...)
	data = appendUint64(data, uint64(len(message.Table)))
	data = append(data, message.Table...)
	data = appendUint64(data, uint64(message.RowType))
	if row.Rows != nil {
		data = appendUint64(data, 1)
	} else {
		data = appendUint64(data, 0)
	}
	data = appendUint64(data, row.TableInfoVersion)
	data = appendUint64(data, row.ReplicatingTs)
	return data, row, nil
}

func marshalDMLTableInfo(tableInfo *commonType.TableInfo) (data []byte, err error) {
	defer func() {
		if recover() != nil {
			err = errors.ErrSpillFileOp.FastGenByArgs("marshal incomplete DML table info")
		}
	}()

	data, err = tableInfo.Marshal()
	if err != nil {
		return nil, errors.WrapError(errors.ErrSpillFileOp, err, "marshal DML table info")
	}
	return data, nil
}

func marshalDMLRows(row *commonEvent.DMLEvent, tableInfoStored bool) (data []byte, err error) {
	defer func() {
		if recover() != nil {
			err = errors.ErrSpillFileOp.FastGenByArgs("marshal DML rows with incomplete table info")
		}
	}()

	fieldTypes := []*types.FieldType(nil)
	if tableInfoStored {
		fieldTypes = row.TableInfo.GetFieldSlice()
	}
	begin := row.PreviousTotalOffset
	end := row.Rows.NumRows()
	if len(row.RowTypes) != 0 {
		end = begin + len(row.RowTypes)
		// Most decoders, including batched DML events, use one RowType entry per
		// physical chunk row. An update consequently appears twice. The Avro
		// decoder instead represents its single logical update with one entry,
		// while retaining both rows in the chunk. Length distinguishes the two
		// encodings: it is the number of logical row changes.
		compactRowTypes := row.Length > 0 && len(row.RowTypes) == int(row.Length)
		if compactRowTypes {
			end = begin
		}
		for _, rowType := range row.RowTypes {
			switch rowType {
			case commonType.RowTypeInsert, commonType.RowTypeDelete:
				if compactRowTypes {
					end++
				}
			case commonType.RowTypeUpdate:
				if compactRowTypes {
					end += 2
				}
			default:
				return nil, errors.ErrSpillFileOp.FastGenByArgs("DML event has invalid row type")
			}
		}
	}
	if begin < 0 || end < begin || end > row.Rows.NumRows() {
		return nil, errors.ErrSpillFileOp.FastGenByArgs("DML event rows are outside the shared chunk")
	}
	if !tableInfoStored && begin != 0 {
		return nil, errors.ErrSpillFileOp.FastGenByArgs("DML event rows require table info")
	}

	rows := chunk.NewChunkWithCapacity(fieldTypes, end-begin)
	rows.Append(row.Rows, begin, end)
	return chunk.NewCodec(fieldTypes).Encode(rows), nil
}

func unmarshalDMLMessage(data []byte) (*codeccommon.DMLMessage, error) {
	eventData, data, err := readSpilledField(data)
	if err != nil {
		return nil, err
	}
	tableInfoData, data, err := readSpilledField(data)
	if err != nil {
		return nil, err
	}
	rowsData, data, err := readSpilledField(data)
	if err != nil {
		return nil, err
	}
	checksumData, data, err := readSpilledField(data)
	if err != nil {
		return nil, err
	}
	schemaData, data, err := readSpilledField(data)
	if err != nil {
		return nil, err
	}
	tableData, data, err := readSpilledField(data)
	if err != nil {
		return nil, err
	}
	rowType, data, err := readSpilledUint64(data)
	if err != nil {
		return nil, err
	}
	if rowType > uint64(^commonType.RowType(0)) {
		return nil, errors.ErrSpillFileOp.FastGenByArgs("invalid DML spill row type")
	}
	rowsPresent, data, err := readSpilledUint64(data)
	if err != nil {
		return nil, err
	}
	if rowsPresent > 1 {
		return nil, errors.ErrSpillFileOp.FastGenByArgs("invalid DML spill rows flag")
	}
	tableInfoVersion, data, err := readSpilledUint64(data)
	if err != nil {
		return nil, err
	}
	replicatingTs, data, err := readSpilledUint64(data)
	if err != nil {
		return nil, err
	}
	if len(data) != 0 {
		return nil, errors.ErrSpillFileOp.FastGenByArgs("unexpected trailing DML spill data")
	}

	row := &commonEvent.DMLEvent{}
	if err := row.Unmarshal(eventData); err != nil {
		return nil, errors.WrapError(errors.ErrSpillFileOp, err, "unmarshal DML event")
	}
	if len(tableInfoData) != 0 {
		tableInfo, err := commonType.UnmarshalJSONToTableInfo(tableInfoData)
		if err != nil {
			return nil, errors.WrapError(errors.ErrSpillFileOp, err, "unmarshal DML table info")
		}
		row.TableInfo = tableInfo
	}
	if rowsPresent == 1 && len(rowsData) == 0 {
		row.Rows = chunk.NewChunkWithCapacity(nil, 0)
	} else if len(rowsData) != 0 {
		fieldTypes := []*types.FieldType(nil)
		if row.TableInfo != nil {
			fieldTypes = row.TableInfo.GetFieldSlice()
		}
		rows, err := unmarshalDMLRows(rowsData, fieldTypes)
		if err != nil {
			return nil, err
		}
		row.Rows = rows
	}
	row.TableInfoVersion = tableInfoVersion
	row.ReplicatingTs = replicatingTs
	if err := json.Unmarshal(checksumData, &row.Checksum); err != nil {
		return nil, errors.WrapError(errors.ErrSpillFileOp, err, "unmarshal DML checksums")
	}
	return codeccommon.NewDMLMessage(row.PhysicalTableID, string(schemaData), string(tableData), row.CommitTs,
		commonType.RowType(rowType), func() *commonEvent.DMLEvent {
			return row
		}), nil
}

func unmarshalDMLRows(data []byte, fieldTypes []*types.FieldType) (rows *chunk.Chunk, err error) {
	defer func() {
		if recover() != nil {
			err = errors.ErrSpillFileOp.FastGenByArgs("decode DML spill rows")
		}
	}()
	rows, _ = chunk.NewCodec(fieldTypes).Decode(data)
	return rows, nil
}

func appendUint64(data []byte, value uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return append(data, buf[:]...)
}

func readSpilledField(data []byte) ([]byte, []byte, error) {
	length, data, err := readSpilledUint64(data)
	if err != nil {
		return nil, nil, err
	}
	if length > uint64(len(data)) {
		return nil, nil, errors.ErrSpillFileOp.FastGenByArgs("invalid DML spill field length")
	}
	return data[:length], data[length:], nil
}

func readSpilledUint64(data []byte) (uint64, []byte, error) {
	if len(data) < 8 {
		return 0, nil, errors.ErrSpillFileOp.FastGenByArgs("truncated DML spill data")
	}
	return binary.BigEndian.Uint64(data[:8]), data[8:], nil
}
