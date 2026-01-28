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
	"github.com/pingcap/ticdc/pkg/errors"
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
// softDeleteTimeCol and softDeleteTimeColIndex must refer to `_tidb_softdelete_time`
// when enableActiveActive is false and the table is active-active or soft-delete.
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
	softDeleteTimeCol *model.ColumnInfo,
	softDeleteTimeColIndex int,
) RowPolicyDecision {
	if tableInfo == nil || row == nil {
		return RowPolicyKeep
	}

	isActiveActive := tableInfo.IsActiveActiveTable()
	isSoftDelete := tableInfo.IsSoftDeleteTable()
	if !isActiveActive && !isSoftDelete {
		return RowPolicyKeep
	}

	if row.RowType == common.RowTypeDelete {
		return RowPolicySkip
	}

	// When enable-active-active is true, we only drop delete events and keep the rest.
	if enableActiveActive {
		return RowPolicyKeep
	}

	if row.RowType != common.RowTypeUpdate {
		return RowPolicyKeep
	}

	if needConvertUpdateToDelete(row, softDeleteTimeColIndex, softDeleteTimeCol) {
		return RowPolicyConvertToDelete
	}
	return RowPolicyKeep
}

// needConvertUpdateToDelete returns true when this update represents a soft-delete transition.
// The caller must ensure `softDeleteTimeCol` and `softDeleteTimeColIndex` are valid for the row.
func needConvertUpdateToDelete(row *RowChange, softDeleteTimeColIndex int, softDeleteTimeCol *model.ColumnInfo) bool {
	if row == nil || softDeleteTimeCol == nil {
		return false
	}
	return isSoftDeleteTransition(row.PreRow, row.Row, softDeleteTimeColIndex, softDeleteTimeCol)
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
//   - Normal tables: the original event is returned as-is.
//   - Active-active tables: delete rows are removed in both modes. When enable-active-active is
//     false, updates that flip `_tidb_softdelete_time` from zero to non-zero are converted into
//     delete events. When enable-active-active is true, inserts/updates pass through so downstream
//     LWW SQL can resolve conflicts.
//   - Soft-delete tables: delete rows are removed. When enable-active-active is false, updates that
//     flip `_tidb_softdelete_time` from zero to non-zero are converted into deletes.
//
// `handleError` is used to report unexpected schema issues and is expected to stop the
// owning dispatcher. When it is invoked, the returned event must be treated as dropped.
func FilterDMLEvent(event *DMLEvent, enableActiveActive bool, handleError func(error)) (*DMLEvent, bool) {
	if event == nil {
		return nil, true
	}

	tableInfo := event.TableInfo
	if tableInfo == nil || event.Rows == nil {
		return event, false
	}

	isActiveActive := tableInfo.IsActiveActiveTable()
	isSoftDelete := tableInfo.IsSoftDeleteTable()
	if !isActiveActive && !isSoftDelete {
		return event, false
	}

	var (
		softDeleteTimeCol      *model.ColumnInfo
		softDeleteTimeColIndex int
		hasSoftDeleteTimeCol   bool
	)
	// `_tidb_softdelete_time` metadata is required for:
	//   - active-active tables: for logging hard deletes (enable-active-active) and converting
	//     soft-delete transitions (disabled).
	//   - soft-delete tables: for converting soft-delete transitions when enable-active-active is disabled.
	needSoftDeleteTimeCol := isActiveActive || (!enableActiveActive && isSoftDelete)
	if needSoftDeleteTimeCol {
		colInfo, ok := tableInfo.GetColumnInfoByName(SoftDeleteTimeColumn)
		if !ok {
			handleError(errors.Errorf(
				"dispatcher %s table %s.%s missing required column %s",
				event.DispatcherID.String(),
				tableInfo.GetSchemaName(),
				tableInfo.GetTableName(),
				SoftDeleteTimeColumn,
			))
			return nil, true
		}
		offset, ok := tableInfo.GetColumnOffsetByName(SoftDeleteTimeColumn)
		if !ok {
			handleError(errors.Errorf(
				"dispatcher %s table %s.%s missing required column offset %s",
				event.DispatcherID.String(),
				tableInfo.GetSchemaName(),
				tableInfo.GetTableName(),
				SoftDeleteTimeColumn,
			))
			return nil, true
		}
		softDeleteTimeCol = colInfo
		softDeleteTimeColIndex = offset
		hasSoftDeleteTimeCol = true
	}

	// When enable-active-active is true, the only transformation is dropping delete rows.
	// Fast path: after validating schema requirements, avoid iterating/copying rows when
	// the event contains no delete rows.
	if enableActiveActive && !hasRowType(event.RowTypes, common.RowTypeDelete) {
		event.Rewind()
		return event, false
	}

	fieldTypes := tableInfo.GetFieldSlice()
	if fieldTypes == nil {
		return event, false
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

		decision := EvaluateRowPolicy(tableInfo, &row, enableActiveActive, softDeleteTimeCol, softDeleteTimeColIndex)

		switch decision {
		case RowPolicySkip:
			if enableActiveActive && row.RowType == common.RowTypeDelete && hasSoftDeleteTimeCol && !row.PreRow.IsEmpty() && softDeleteTimeColIndex < row.PreRow.Len() {
				// For active-active tables, TiCDC expects user-managed hard deletes to keep
				// `_tidb_softdelete_time` as NULL, so we log it for observability.
				softDeleteTime := common.ExtractColVal(&row.PreRow, softDeleteTimeCol, softDeleteTimeColIndex)
				if softDeleteTime == nil {
					log.Info("received hard delete row",
						zap.Stringer("dispatcherID", event.DispatcherID),
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
		// An update occupies two slots (pre and post), so we need to append
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
		return event, false
	}

	if kept == 0 {
		return nil, true
	}

	return newFilteredDMLEvent(event, newChunk, rowTypes, rowKeys, checksums, kept), false
}

// newFilteredDMLEvent builds a new DMLEvent from the filtered rows while preserving
// dispatcher-managed metadata from the source event.
func newFilteredDMLEvent(
	source *DMLEvent,
	rows *chunk.Chunk,
	rowTypes []common.RowType,
	rowKeys [][]byte,
	checksums []*integrity.Checksum,
	kept int,
) *DMLEvent {
	newEvent := NewDMLEvent(source.DispatcherID, source.PhysicalTableID, source.StartTs, source.CommitTs, source.TableInfo)
	newEvent.TableInfoVersion = source.TableInfoVersion
	newEvent.Seq = source.Seq
	newEvent.Epoch = source.Epoch
	newEvent.ReplicatingTs = source.ReplicatingTs
	newEvent.PostTxnFlushed = source.PostTxnFlushed
	source.PostTxnFlushed = nil

	newEvent.SetRows(rows)
	newEvent.RowTypes = rowTypes
	newEvent.RowKeys = rowKeys
	newEvent.Checksum = checksums
	newEvent.Length = int32(kept)
	newEvent.PreviousTotalOffset = 0

	if source.Len() == 0 {
		newEvent.ApproximateSize = 0
	} else {
		newEvent.ApproximateSize = source.ApproximateSize * int64(kept) / int64(source.Len())
	}
	return newEvent
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

func hasRowType(rowTypes []common.RowType, target common.RowType) bool {
	for _, rowType := range rowTypes {
		if rowType == target {
			return true
		}
	}
	return false
}
