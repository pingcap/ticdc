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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

const (
	// SoftDeleteTimeColumn stores TiDB soft delete timestamp.
	SoftDeleteTimeColumn = "_tidb_softdelete_time"
	// OriginTsColumn stores the origin commit ts for active-active tables.
	OriginTsColumn = "_tidb_origin_ts"
	// CommitTsColumn stores the downstream commit ts for active-active tables.
	CommitTsColumn = "_tidb_commit_ts"
)

// RowPolicyDecision represents how a sink should treat a specific row.
type RowPolicyDecision int

const (
	// RowPolicyKeep means the row should be emitted unchanged.
	RowPolicyKeep RowPolicyDecision = iota
	// RowPolicySkip means the row should be ignored.
	RowPolicySkip
	// RowPolicyConvertToDelete means the row should be converted into a delete event.
	RowPolicyConvertToDelete
)

// EvaluateRowPolicy decides how special tables (active-active/soft-delete)
// should behave under the given changefeed mode.
// The decision order is:
//  1. Tables with neither feature are returned untouched.
//  2. Delete rows are skipped no matter which mode we run in.
//  3. When enableActiveActive is true we keep inserts/updates as-is to let
//     downstream LWW SQL handle the conflict resolution.
//  4. Otherwise, for update rows we determine whether this row represents a
//     soft-delete transition (Origin -> Soft delete), and convert it into a
//     delete event to keep downstream consistent.
func EvaluateRowPolicy(
	tableInfo *common.TableInfo,
	row *RowChange,
	enableActiveActive bool,
) (RowPolicyDecision, error) {
	if tableInfo == nil || row == nil {
		return RowPolicyKeep, nil
	}

	isActiveActive := tableInfo.IsActiveActiveTable()
	isSoftDelete := tableInfo.IsSoftDeleteTable()
	if !isActiveActive && !isSoftDelete {
		return RowPolicyKeep, nil
	}

	if row.RowType == common.RowTypeDelete {
		return RowPolicySkip, nil
	}

	// When enable-active-active is true, we only drop delete events and keep the rest.
	if enableActiveActive {
		return RowPolicyKeep, nil
	}

	if row.RowType != common.RowTypeUpdate {
		return RowPolicyKeep, nil
	}

	convert, err := needConvertUpdateToDelete(tableInfo, row)
	if err != nil {
		return RowPolicyKeep, err
	}
	if convert {
		return RowPolicyConvertToDelete, nil
	}
	return RowPolicyKeep, nil
}

func needConvertUpdateToDelete(tableInfo *common.TableInfo, row *RowChange) (bool, error) {
	if tableInfo == nil || row == nil {
		return false, nil
	}
	// Soft-delete is modeled as `_tidb_softdelete_time` changing from NULL/zero to
	// a non-zero timestamp. Only when the table exposes this column do we attempt
	// the conversion.
	colInfo, ok := tableInfo.GetColumnInfoByName(SoftDeleteTimeColumn)
	if !ok {
		return false, nil
	}
	offset, ok := tableInfo.GetColumnOffsetByName(SoftDeleteTimeColumn)
	if !ok {
		return false, nil
	}
	return isSoftDeleteTransition(row.PreRow, row.Row, offset, colInfo), nil
}

func isSoftDeleteTransition(preRow, row chunk.Row, offset int, colInfo *model.ColumnInfo) bool {
	if preRow.IsEmpty() || row.IsEmpty() {
		return false
	}
	if offset >= preRow.Len() || offset >= row.Len() {
		return false
	}
	oldVal := common.ExtractColVal(&preRow, colInfo, offset)
	newVal := common.ExtractColVal(&row, colInfo, offset)
	return isZeroSoftDeleteValue(oldVal) && !isZeroSoftDeleteValue(newVal)
}

// isZeroSoftDeleteValue assumes `_tidb_softdelete_time` is defined as `TIMESTAMP NULL`
// and ExtractColVal always returns either nil or its string representation.
func isZeroSoftDeleteValue(val interface{}) bool {
	if val == nil {
		return true
	}
	str, ok := val.(string)
	if !ok {
		return false
	}
	return str == "" || str == "0"
}

// ApplyRowPolicyDecision mutates the row based on decision.
func ApplyRowPolicyDecision(row *RowChange, decision RowPolicyDecision) {
	switch decision {
	case RowPolicyConvertToDelete:
		row.RowType = common.RowTypeDelete
		row.Row = chunk.Row{}
	}
}

// FilterDMLEvent applies row policy decisions to each row in the DMLEvent.
//   - Normal tables simply fall through: the original event is returned as-is.
//   - Active-active tables:
//   - enable-active-active = true: only delete rows are removed, other rows pass through.
//   - enable-active-active = false: delete rows are removed and updates that flip
//     `_tidb_softdelete_time` from zero to non-zero will be converted to delete events.
//   - Soft-delete tables behave the same as the second bullet: delete rows are removed and
//     soft-delete transitions are converted into deletes.
//
// It returns the possibly modified event, whether the event should be skipped entirely,
// and an error if evaluation fails.
func FilterDMLEvent(event *DMLEvent, enableActiveActive bool) (*DMLEvent, bool, error) {
	if event == nil {
		return nil, true, nil
	}

	tableInfo := event.TableInfo
	if tableInfo == nil || event.Rows == nil {
		return event, false, nil
	}

	needProcess := enableActiveActive || tableInfo.IsActiveActiveTable() || tableInfo.IsSoftDeleteTable()
	if !needProcess {
		return event, false, nil
	}

	fieldTypes := tableInfo.GetFieldSlice()
	if fieldTypes == nil {
		return event, false, nil
	}

	var (
		softDeleteTimeCol      *model.ColumnInfo
		softDeleteTimeColIndex int
		hasSoftDeleteTimeCol   bool
	)
	if enableActiveActive {
		colInfo, ok := tableInfo.GetColumnInfoByName(SoftDeleteTimeColumn)
		if ok {
			offset, ok := tableInfo.GetColumnOffsetByName(SoftDeleteTimeColumn)
			if ok {
				softDeleteTimeCol = colInfo
				softDeleteTimeColIndex = offset
				hasSoftDeleteTimeCol = true
			}
		}
	}

	newChunk := chunk.NewChunkWithCapacity(fieldTypes, event.Rows.NumRows())
	rowTypes := make([]common.RowType, 0, len(event.RowTypes))
	rowKeys := make([][]byte, 0, len(event.RowTypes))
	checksums := make([]*integrity.Checksum, 0, len(event.RowTypes))

	filtered := false
	kept := 0
	for {
		row, ok := event.GetNextRow()
		if !ok {
			break
		}

		decision, err := EvaluateRowPolicy(tableInfo, &row, enableActiveActive)
		if err != nil {
			event.Rewind()
			return nil, false, err
		}

		switch decision {
		case RowPolicySkip:
			if enableActiveActive && row.RowType == common.RowTypeDelete && hasSoftDeleteTimeCol && !row.PreRow.IsEmpty() && softDeleteTimeColIndex < row.PreRow.Len() {
				// For active-active tables, TiCDC expects user-managed hard deletes to keep
				// `_tidb_softdelete_time` as NULL, so we log it for observability.
				softDeleteTime := common.ExtractColVal(&row.PreRow, softDeleteTimeCol, softDeleteTimeColIndex)
				if softDeleteTime == nil {
					log.Info("received hard delete row",
						zap.Uint64("commitTs", uint64(event.CommitTs)))
				}
			}
			filtered = true
			continue
		case RowPolicyConvertToDelete:
			ApplyRowPolicyDecision(&row, decision)
			filtered = true
		default:
		}

		appendRowChangeToChunk(newChunk, &row)
		// RowTypes/RowKeys are indexed by physical row slots in `DMLEvent.Rows`.
		// An update occupies two slots (pre and post image), so we need to append
		// its type/key twice to keep `GetNextRow()` semantics intact.
		physicalSlots := 1
		if row.RowType == common.RowTypeUpdate {
			physicalSlots = 2
		}
		for i := 0; i < physicalSlots; i++ {
			rowTypes = append(rowTypes, row.RowType)
			rowKeys = append(rowKeys, cloneRowKey(row.RowKey))
		}
		checksums = append(checksums, row.Checksum)
		kept++
	}
	event.Rewind()

	if !filtered {
		return event, false, nil
	}

	if kept == 0 {
		return nil, true, nil
	}

	newEvent := NewDMLEvent(event.DispatcherID, event.PhysicalTableID, event.StartTs, event.CommitTs, event.TableInfo)
	newEvent.TableInfoVersion = event.TableInfoVersion
	newEvent.Seq = event.Seq
	newEvent.Epoch = event.Epoch
	newEvent.ReplicatingTs = event.ReplicatingTs
	newEvent.PostTxnFlushed = event.PostTxnFlushed
	event.PostTxnFlushed = nil

	newEvent.SetRows(newChunk)
	newEvent.RowTypes = rowTypes
	newEvent.RowKeys = rowKeys
	newEvent.Checksum = checksums
	newEvent.Length = int32(kept)
	newEvent.PreviousTotalOffset = 0

	if event.Len() == 0 {
		newEvent.ApproximateSize = 0
	} else {
		newEvent.ApproximateSize = event.ApproximateSize * int64(kept) / int64(event.Len())
	}

	return newEvent, false, nil
}

func appendRowChangeToChunk(chk *chunk.Chunk, row *RowChange) {
	if row == nil || chk == nil {
		return
	}
	switch row.RowType {
	case common.RowTypeInsert:
		if !row.Row.IsEmpty() {
			chk.AppendRow(row.Row)
		}
	case common.RowTypeDelete:
		if !row.PreRow.IsEmpty() {
			chk.AppendRow(row.PreRow)
		}
	case common.RowTypeUpdate:
		if !row.PreRow.IsEmpty() {
			chk.AppendRow(row.PreRow)
		}
		if !row.Row.IsEmpty() {
			chk.AppendRow(row.Row)
		}
	}
}

func cloneRowKey(rowKey []byte) []byte {
	if len(rowKey) == 0 {
		return nil
	}
	cp := make([]byte, len(rowKey))
	copy(cp, rowKey)
	return cp
}
