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
func (g *EventsGroup) AppendMessage(message *codeccommon.DMLMessage) {
	g.appendMessage(message, nil)
}

// AppendMessageWithPostRestore appends a message and applies postRestore after it is read back from
// disk. It keeps consumer checks that intentionally run immediately before flushing out of the
// on-disk representation.
func (g *EventsGroup) AppendMessageWithPostRestore(
	message *codeccommon.DMLMessage,
	postRestore func(*codeccommon.DMLMessage) *codeccommon.DMLMessage,
) {
	g.appendMessage(message, postRestore)
}

func (g *EventsGroup) appendMessage(
	message *codeccommon.DMLMessage,
	postRestore func(*codeccommon.DMLMessage) *codeccommon.DMLMessage,
) {
	commitTs := message.GetCommitTs()
	if len(g.messages) > 0 && commitTs < g.messages[len(g.messages)-1].commitTs {
		g.outOfOrder = true
	}
	if commitTs > g.HighWatermark {
		g.HighWatermark = commitTs
	}

	data, row, err := marshalDMLMessage(message)
	if err != nil {
		log.Panic("marshal DML message for spill failed",
			zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID), zap.Error(err))
	}
	if g.spillFile == nil {
		g.spillFile, err = spill.NewRecordFile(os.TempDir(), eventsGroupSpillPattern)
		if err != nil {
			log.Panic("create events group spill file failed",
				zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID), zap.Error(err))
		}
	}
	handle, err := g.spillFile.Append(data)
	if err != nil {
		log.Panic("write DML message to spill file failed",
			zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID), zap.Error(err))
	}
	g.messages = append(g.messages, spilledMessage{
		commitTs:    commitTs,
		handle:      handle,
		postRestore: postRestore,
	})
	// Codec decoders use this callback to release their pooled chunks. The event is durable in the
	// spill file now, so the original in-memory event is no longer needed.
	row.PostFlush()
}

// ResolveInto appends all messages with CommitTs <= resolve into dst in commit-ts order and removes
// them from the group. Resolved messages are restored from the spill file only when downstream needs
// them, keeping the buffered group out of heap memory.
func (g *EventsGroup) ResolveInto(resolve uint64, dst []*codeccommon.DMLMessage) []*codeccommon.DMLMessage {
	if len(g.messages) == 0 {
		return dst
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
		return dst
	}

	for _, message := range g.messages[:resolvedCount] {
		data, err := g.spillFile.Read(message.handle)
		if err != nil {
			log.Panic("read DML message from spill file failed",
				zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID), zap.Error(err))
		}
		restored, err := unmarshalDMLMessage(data)
		if err != nil {
			log.Panic("unmarshal DML message from spill file failed",
				zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID), zap.Error(err))
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
			log.Panic("cleanup events group spill file failed",
				zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID), zap.Error(err))
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
	return dst
}

// GetAllMessages gets all messages.
func (g *EventsGroup) GetAllMessages() []*codeccommon.DMLMessage {
	return g.ResolveInto(math.MaxUint64, nil)
}

// Cleanup removes pending spill records when the consumer is stopping.
func (g *EventsGroup) Cleanup() error {
	if g.spillFile == nil {
		return nil
	}
	err := g.spillFile.Cleanup()
	g.spillFile = nil
	clear(g.messages)
	g.messages = g.messages[:0]
	return err
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
	eventData, err := row.Marshal()
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
			rowsData = nil
		}
	}

	checksumData, err := json.Marshal(row.Checksum)
	if err != nil {
		return nil, nil, errors.WrapError(errors.ErrSpillFileOp, err, "marshal DML checksums")
	}

	data = make([]byte, 0, 7*8+len(eventData)+len(tableInfoData)+len(rowsData)+len(checksumData))
	data = appendUint64(data, uint64(len(eventData)))
	data = append(data, eventData...)
	data = appendUint64(data, uint64(len(tableInfoData)))
	data = append(data, tableInfoData...)
	data = appendUint64(data, uint64(len(rowsData)))
	data = append(data, rowsData...)
	data = appendUint64(data, uint64(len(checksumData)))
	data = append(data, checksumData...)
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
	return chunk.NewCodec(fieldTypes).Encode(row.Rows), nil
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
		row.Rows, _ = chunk.NewCodec(fieldTypes).Decode(rowsData)
	}
	row.TableInfoVersion = tableInfoVersion
	row.ReplicatingTs = replicatingTs
	if err := json.Unmarshal(checksumData, &row.Checksum); err != nil {
		return nil, errors.WrapError(errors.ErrSpillFileOp, err, "unmarshal DML checksums")
	}
	if len(row.RowTypes) == 0 {
		return nil, errors.ErrSpillFileOp.FastGenByArgs("spilled DML event has no row type")
	}

	var schema, table string
	if row.TableInfo != nil {
		schema = row.TableInfo.GetSchemaName()
		table = row.TableInfo.GetTableName()
	}

	return codeccommon.NewDMLMessage(row.PhysicalTableID, schema, table, row.CommitTs, row.RowTypes[0], func() *commonEvent.DMLEvent {
		return row
	}), nil
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

// AppendOrMergeDMLEvent appends a DML event, or merges it into the previous event
// when both events belong to the same table group and have the same commit-ts.
func AppendOrMergeDMLEvent(events []*commonEvent.DMLEvent, row *commonEvent.DMLEvent) []*commonEvent.DMLEvent {
	var lastDMLEvent *commonEvent.DMLEvent
	if len(events) > 0 {
		lastDMLEvent = events[len(events)-1]
	}

	if lastDMLEvent == nil || lastDMLEvent.GetCommitTs() < row.GetCommitTs() {
		return append(events, row)
	}

	if lastDMLEvent.GetCommitTs() == row.GetCommitTs() {
		lastDMLEvent.Rows.Append(row.Rows, 0, row.Rows.NumRows())
		lastDMLEvent.RowTypes = append(lastDMLEvent.RowTypes, row.RowTypes...)
		lastDMLEvent.Length += row.Length
		lastDMLEvent.PostTxnFlushed = append(lastDMLEvent.PostTxnFlushed, row.PostTxnFlushed...)
		return events
	}

	log.Panic("append event with smaller commit ts",
		zap.Int64("tableID", row.GetTableID()),
		zap.Uint64("lastCommitTs", lastDMLEvent.GetCommitTs()), zap.Uint64("commitTs", row.GetCommitTs()))
	return events
}
