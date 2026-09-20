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

package util

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/integrity"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// seenDMLRow records where the first copy of one row mutation of the commit-ts
// boundary being merged is stored. Both row images point into the merged event
// that owns them, so they stay readable as long as that event is alive.
type seenDMLRow struct {
	rowType common.RowType
	key     []byte
	preRow  chunk.Row
	row     chunk.Row
}

// MessagesToEvents materializes the resolved messages of this group, drops
// replayed row mutations, and merges compatible adjacent messages before they
// are handed to the downstream sink.
//
// MQ delivery can replay a row mutation at an unclosed commit-ts boundary: when
// a dispatcher sends data that never reached a reliable checkpoint, a new owner
// must rescan from an earlier checkpoint and sends the same upstream mutation
// again. The replayed copy carries a different Kafka offset and dispatcher
// sequence, so the only stable identity is (row type, handle key). Keeping both
// copies makes the sink batch merger reject its input, so the duplicate is
// dropped here while the callbacks of every fragment are retained.
//
// The dedup state belongs to the commit-ts boundary being merged, so it is
// released when another boundary starts and when this call returns. Releasing it
// at the end of the call also releases the row images it refers to, and it is
// safe because the fragments of one commit-ts boundary are always merged
// together: the spill index orders messages by commit-ts inside one group, and
// PrepareResolve never splits a commit-ts across batches. A mutation replayed
// after the boundary was written downstream is not covered here; it is recovered
// by the downstream unique constraints.
func (g *EventsGroup) MessagesToEvents(messages []*codeccommon.DMLMessage) ([]*commonEvent.DMLEvent, error) {
	defer clear(g.seenRows)

	events := make([]*commonEvent.DMLEvent, 0, len(messages))
	for _, message := range messages {
		var err error
		events, err = g.mergeMessage(events, message.ToDMLEvent())
		if err != nil {
			return nil, err
		}
	}
	return events, nil
}

// mergeMessage appends fragment as a new event, or merges it into the trailing
// event of the same transaction after dropping replayed row mutations.
func (g *EventsGroup) mergeMessage(
	events []*commonEvent.DMLEvent, fragment *commonEvent.DMLEvent,
) ([]*commonEvent.DMLEvent, error) {
	var last *commonEvent.DMLEvent
	if len(events) != 0 {
		last = events[len(events)-1]
	}
	if !sameDMLTransaction(last, fragment) {
		// Another commit-ts boundary starts: release the state of the previous
		// one instead of keeping a global seen set.
		clear(g.seenRows)
		// The event that opens the new boundary already holds its rows, so only
		// remember them: a later replay of any of them must still be dropped.
		g.rememberRows(fragment)
		return append(events, fragment), nil
	}

	if err := g.mergeRows(last, fragment); err != nil {
		return nil, err
	}
	// Replayed rows are removed, but the callbacks of every fragment are kept:
	// advancing the Kafka offset, the spill index, and the resource release of a
	// fragment must still happen after the retained event is written downstream.
	last.PostTxnEnqueued = append(last.PostTxnEnqueued, fragment.PostTxnEnqueued...)
	last.PostTxnFlushed = append(last.PostTxnFlushed, fragment.PostTxnFlushed...)
	return events, nil
}

// mergeRows merges the row mutations of fragment into last, keeping the first
// copy of each (row type, handle key) inside the commit-ts boundary.
func (g *EventsGroup) mergeRows(last, fragment *commonEvent.DMLEvent) error {
	tableInfo := fragment.TableInfo
	if tableInfo == nil || len(tableInfo.GetOrderedHandleKeyColumnIDs()) == 0 {
		// Without a handle key a legitimately repeated mutation cannot be told
		// apart from a replay, so every row is kept.
		appendRows(last, fragment)
		return nil
	}

	// Keep optional per-row metadata aligned with the rows that are appended.
	if len(last.RowKeys) == 0 && len(fragment.RowKeys) != 0 {
		last.RowKeys = make([][]byte, len(last.RowTypes))
	}
	if len(last.Checksum) == 0 && len(fragment.Checksum) != 0 {
		last.Checksum = make([]*integrity.Checksum, len(last.RowTypes))
	}

	kept := 0
	checksumIndex := 0
	for slot := 0; slot < len(fragment.RowTypes); {
		rowType := fragment.RowTypes[slot]
		preRow, row := fragment.Rows.GetRow(slot), fragment.Rows.GetRow(slot)
		physicalRows := 1
		if rowType == common.RowTypeUpdate {
			physicalRows = 2
			row = fragment.Rows.GetRow(slot + 1)
		}

		// Kept rows are appended at the end of last, so the offsets are known
		// before the rows exist.
		offset := last.Rows.NumRows()
		replayed, err := g.isReplayedRowMutation(last, tableInfo, rowType, preRow, row, offset)
		if err != nil {
			return err
		}
		if !replayed {
			for i := range physicalRows {
				last.Rows.AppendRow(fragment.Rows.GetRow(slot + i))
				last.RowTypes = append(last.RowTypes, fragment.RowTypes[slot+i])
				if len(last.RowKeys) != 0 || len(fragment.RowKeys) != 0 {
					last.RowKeys = append(last.RowKeys, optionalDMLValue(fragment.RowKeys, slot+i))
				}
			}
			// A checksum is recorded once per row mutation.
			if len(last.Checksum) != 0 || len(fragment.Checksum) != 0 {
				last.Checksum = append(last.Checksum, optionalDMLValue(fragment.Checksum, checksumIndex))
			}
			last.Length++
			kept++
		}
		slot += physicalRows
		checksumIndex++
	}

	if kept != 0 {
		// The fragment size is not attributable to single rows, so a partially
		// replayed fragment still accounts for its whole size.
		last.ApproximateSize += fragment.ApproximateSize
	}
	return nil
}

// rememberRows records where the row mutations of an event are stored, so a
// replayed copy of any of them can be recognized. It is used for the event that
// opens a commit-ts boundary, whose rows are already in place.
func (g *EventsGroup) rememberRows(event *commonEvent.DMLEvent) {
	tableInfo := event.TableInfo
	if tableInfo == nil || len(tableInfo.GetOrderedHandleKeyColumnIDs()) == 0 {
		return
	}

	for slot := 0; slot < len(event.RowTypes); {
		rowType := event.RowTypes[slot]
		preRow, row := event.Rows.GetRow(slot), event.Rows.GetRow(slot)
		physicalRows := 1
		if rowType == common.RowTypeUpdate {
			physicalRows = 2
			row = event.Rows.GetRow(slot + 1)
		}
		if handleKey := rowMutationHandleKey(tableInfo, rowType, preRow, row); len(handleKey) != 0 {
			g.remember(rowType, handleKey, event, slot)
		}
		slot += physicalRows
	}
}

// isReplayedRowMutation reports whether the mutation was already seen inside the
// commit-ts boundary being merged, and remembers the first copy at the offset
// where it is appended.
func (g *EventsGroup) isReplayedRowMutation(
	last *commonEvent.DMLEvent, tableInfo *common.TableInfo,
	rowType common.RowType, preRow, row chunk.Row, offset int,
) (bool, error) {
	handleKey := rowMutationHandleKey(tableInfo, rowType, preRow, row)
	if len(handleKey) == 0 {
		// Row identity is unknown, so this mutation cannot be de-duplicated.
		return false, nil
	}

	hash := dmlRowHash(rowType, handleKey)
	for _, seen := range g.seenRows[hash] {
		if seen.rowType != rowType || !bytes.Equal(seen.key, handleKey) {
			continue
		}
		if !sameDMLRowImage(tableInfo, seen, preRow, row) {
			// The event broker guarantees that one transaction mutates a handle
			// key at most once per row type, so a second copy must describe the
			// very same row image. Failing is safer than silently keeping one of
			// two conflicting images.
			return false, errors.ErrUnexpected.FastGenByArgs(fmt.Sprintf(
				"replayed %s row mutation carries a different row image: "+
					"schema %s, table %s, tableID %d, commitTs %d",
				rowType, tableInfo.GetSchemaName(), tableInfo.GetTableName(),
				last.GetTableID(), last.GetCommitTs()))
		}
		return true, nil
	}

	g.remember(rowType, handleKey, last, offset)
	return false, nil
}

// remember records the identity of one kept row mutation and where its row
// images are stored in the merged event.
func (g *EventsGroup) remember(
	rowType common.RowType, handleKey []byte, event *commonEvent.DMLEvent, offset int,
) {
	seen := seenDMLRow{rowType: rowType, key: handleKey}
	switch rowType {
	case common.RowTypeDelete:
		seen.preRow = event.Rows.GetRow(offset)
	case common.RowTypeUpdate:
		seen.preRow = event.Rows.GetRow(offset)
		seen.row = event.Rows.GetRow(offset + 1)
	default:
		seen.row = event.Rows.GetRow(offset)
	}
	if g.seenRows == nil {
		g.seenRows = make(map[uint64][]seenDMLRow)
	}
	hash := dmlRowHash(rowType, handleKey)
	g.seenRows[hash] = append(g.seenRows[hash], seen)
}

// rowMutationHandleKey returns the handle key that identifies a mutation. A
// delete is identified by the row it removes, an insert and an update by the row
// they write.
func rowMutationHandleKey(
	tableInfo *common.TableInfo, rowType common.RowType, preRow, row chunk.Row,
) []byte {
	if rowType == common.RowTypeDelete {
		return encodeHandleKey(&preRow, tableInfo)
	}
	return encodeHandleKey(&row, tableInfo)
}

// sameDMLRowImage reports whether a replayed mutation carries the same row image
// as the kept copy.
func sameDMLRowImage(tableInfo *common.TableInfo, seen seenDMLRow, preRow, row chunk.Row) bool {
	switch seen.rowType {
	case common.RowTypeDelete:
		return sameRowImage(tableInfo, seen.preRow, preRow)
	case common.RowTypeUpdate:
		return sameRowImage(tableInfo, seen.preRow, preRow) && sameRowImage(tableInfo, seen.row, row)
	default:
		return sameRowImage(tableInfo, seen.row, row)
	}
}

// sameRowImage compares every column value of two row images of the same table.
func sameRowImage(tableInfo *common.TableInfo, first, second chunk.Row) bool {
	if first.Len() != second.Len() {
		return false
	}
	columns := tableInfo.GetColumns()
	for idx := range min(first.Len(), len(columns)) {
		column := columns[idx]
		if column == nil {
			continue
		}
		if !sameColumnValue(
			common.ExtractColVal(&first, column, idx),
			common.ExtractColVal(&second, column, idx),
		) {
			return false
		}
	}
	return true
}

func sameColumnValue(first, second any) bool {
	if first == nil || second == nil {
		return first == nil && second == nil
	}
	return common.ColumnValueString(first) == common.ColumnValueString(second)
}

// dmlRowHash indexes one row mutation inside the commit-ts boundary being
// merged.
func dmlRowHash(rowType common.RowType, handleKey []byte) uint64 {
	const (
		fnvOffset32 uint32 = 2166136261
		fnvPrime32  uint32 = 16777619
	)
	hash := fnvOffset32 ^ uint32(rowType)
	for _, b := range handleKey {
		hash = (hash ^ uint32(b)) * fnvPrime32
	}
	return uint64(hash)
}

// appendRows moves every row of fragment to the end of last, keeping the
// per-row metadata aligned.
func appendRows(last, fragment *commonEvent.DMLEvent) {
	lastRowTypeCount := len(last.RowTypes)
	fragmentRowTypeCount := len(fragment.RowTypes)
	last.Rows.Append(fragment.Rows, 0, fragment.Rows.NumRows())
	last.RowTypes = append(last.RowTypes, fragment.RowTypes...)
	last.RowKeys = appendOptionalDMLValues(last.RowKeys, fragment.RowKeys, lastRowTypeCount, fragmentRowTypeCount)
	last.Checksum = appendOptionalDMLValues(last.Checksum, fragment.Checksum, lastRowTypeCount, fragmentRowTypeCount)
	last.Length += fragment.Length
	last.ApproximateSize += fragment.ApproximateSize
}

// optionalDMLValue returns the entry at index of an optional per-row slice, or
// its zero value when the slice does not reach that row.
func optionalDMLValue[T any](values []T, index int) T {
	if index < len(values) {
		return values[index]
	}
	var zero T
	return zero
}

// encodeHandleKey encodes the handle key columns of one row image into the byte
// string that identifies the row.
//
// It mirrors the row identity used by the MySQL sink DML batch merger
// (pkg/sink/mysql genKeyAndHash), so the consumer drops exactly the row
// mutations that the merger treats as the same row. Keep both definitions in
// sync, including the handle key column choice, the value encoding, and the
// collation normalization.
//
// It returns nil when the identity cannot be decided: the table has no handle
// key, a handle key column is missing from the schema, or a handle key column
// value is not available in the row image. Callers must keep the row then.
func encodeHandleKey(row *chunk.Row, tableInfo *common.TableInfo) []byte {
	keyColumns := tableInfo.GetOrderedHandleKeyColumnIDs()
	if len(keyColumns) == 0 {
		return nil
	}

	var key []byte
	for _, colID := range keyColumns {
		if _, exist := tableInfo.GetColumnInfo(colID); !exist {
			return nil
		}
		// chunk.Row is laid out in schema column order (TableInfo.GetColumns()).
		// RowColumnsOffset is based on CDC-visible columns and may skip virtual generated columns,
		// which would cause extracting a different column value and break row identity.
		// Thus, we should not use RowColumnsOffset here.
		colOffset := tableInfo.MustGetColumnOffsetByID(colID)
		if colOffset >= row.Len() {
			return nil
		}
		info := tableInfo.GetColumns()[colOffset]
		if info == nil || info.ID != colID {
			return nil
		}

		value := common.ExtractColVal(row, info, colOffset)
		// if a column value is null, we can ignore this index
		if value == nil {
			return nil
		}

		val := common.ColumnValueString(value)
		if columnNeeds2LowerCase(info.GetType(), info.GetCollate()) {
			val = strings.ToLower(val)
		}

		key = append(key, val...)
		key = append(key, 0)
	}
	return key
}

func columnNeeds2LowerCase(mysqlType byte, collation string) bool {
	switch mysqlType {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return collationNeeds2LowerCase(collation)
	}
	return false
}

func collationNeeds2LowerCase(collation string) bool {
	return strings.HasSuffix(collation, "_ci")
}
