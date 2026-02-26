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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tidbTypes "github.com/pingcap/tidb/pkg/types"
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
// softDeleteTimeColIndex must refer to `_tidb_softdelete_time`
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

	if needConvertUpdateToDelete(row, softDeleteTimeColIndex) {
		return RowPolicyConvertToDelete
	}
	return RowPolicyKeep
}

// needConvertUpdateToDelete returns true when this update represents a soft-delete transition.
// The caller must ensure `softDeleteTimeColIndex` is valid for the row.
func needConvertUpdateToDelete(row *RowChange, softDeleteTimeColIndex int) bool {
	if row == nil {
		return false
	}
	return isSoftDeleteTransition(row.PreRow, row.Row, softDeleteTimeColIndex)
}

// isSoftDeleteTransition determines whether an update represents a soft-delete transition.
//
// It relies on the invariant that `_tidb_softdelete_time` is defined as `TIMESTAMP(6) NULL`,
// and NULL is the only representation of "not deleted". Under this invariant, a transition
// from not-deleted to deleted is strictly a NULL -> non-NULL change.
func isSoftDeleteTransition(preRow, row chunk.Row, offset int) bool {
	if preRow.IsEmpty() || row.IsEmpty() {
		return false
	}
	if offset >= preRow.Len() || offset >= row.Len() {
		return false
	}
	return preRow.IsNull(offset) && !row.IsNull(offset)
}

// getSoftDeleteTimeColumnIndex returns the column offset for `_tidb_softdelete_time` after validating
// its semantics. It reports schema issues via `handleError` and returns ok=false in that case.
//
// The soft-delete transition detection relies on the invariant that `_tidb_softdelete_time` is
// defined as `TIMESTAMP(6) NULL`, and NULL is the only representation of "not deleted".
func getSoftDeleteTimeColumnIndex(event *DMLEvent, handleError func(error)) (idx int, ok bool) {
	tableInfo := event.TableInfo
	colInfo, ok := tableInfo.GetColumnInfoByName(SoftDeleteTimeColumn)
	if !ok {
		handleError(errors.Errorf(
			"dispatcher %s table %s.%s missing required column %s",
			event.DispatcherID.String(),
			tableInfo.GetSchemaName(),
			tableInfo.GetTableName(),
			SoftDeleteTimeColumn,
		))
		return 0, false
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
		return 0, false
	}
	notNull := mysql.HasNotNullFlag(colInfo.GetFlag())
	if colInfo.GetType() != mysql.TypeTimestamp || colInfo.FieldType.GetDecimal() != tidbTypes.MaxFsp || notNull {
		handleError(errors.Errorf(
			"dispatcher %s table %s.%s invalid column %s, expect TIMESTAMP(6) NULL, got type %d fsp %d notNull %t",
			event.DispatcherID.String(),
			tableInfo.GetSchemaName(),
			tableInfo.GetTableName(),
			SoftDeleteTimeColumn,
			colInfo.GetType(),
			colInfo.FieldType.GetDecimal(),
			notNull,
		))
		return 0, false
	}
	return offset, true
}

// getSoftDeleteTimeColumnOffset returns the column offset for `_tidb_softdelete_time` without validating
// its type semantics. It reports schema issues via `handleError` and returns ok=false in that case.
func getSoftDeleteTimeColumnOffset(event *DMLEvent, handleError func(error)) (idx int, ok bool) {
	tableInfo := event.TableInfo
	if _, ok := tableInfo.GetColumnInfoByName(SoftDeleteTimeColumn); !ok {
		handleError(errors.Errorf(
			"dispatcher %s table %s.%s missing required column %s",
			event.DispatcherID.String(),
			tableInfo.GetSchemaName(),
			tableInfo.GetTableName(),
			SoftDeleteTimeColumn,
		))
		return 0, false
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
		return 0, false
	}
	return offset, true
}

// ApplyRowPolicyDecision mutates the row based on decision.
func ApplyRowPolicyDecision(row *RowChange, decision RowPolicyDecision) {
	switch decision {
	case RowPolicyConvertToDelete:
		row.RowType = common.RowTypeDelete
		row.Row = chunk.Row{}
		// When converting an update to a delete, only the pre-image is retained. Clear the
		// current checksum so downstream verification does not attempt to checksum an empty row.
		if row.Checksum != nil {
			row.Checksum.Current = 0
		}
	}
}

// FilterDMLEvent applies row policy decisions to each row in the DMLEvent.
//   - Normal tables: the original event is returned as-is.
//   - Active-active tables: delete rows are removed in both modes. When enable-active-active is
//     false, updates that flip `_tidb_softdelete_time` from NULL to non-NULL are converted into delete
//     events. When enable-active-active is true, inserts/updates pass through so downstream LWW SQL can
//     resolve conflicts.
//   - Soft-delete tables: delete rows are removed. When enable-active-active is false, updates that
//     flip `_tidb_softdelete_time` from NULL to non-NULL are converted into deletes.
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

	// FilterDMLEvent iterates rows via GetNextRow. Always start from the beginning to
	// keep the filtering result independent of the caller's current offset.
	event.Rewind()

	var (
		softDeleteTimeColIndex int
		hasSoftDeleteTimeCol   bool
	)
	// `_tidb_softdelete_time` metadata is required for:
	//   - active-active tables: for logging hard deletes (enable-active-active) and converting
	//     soft-delete transitions (disabled).
	//   - soft-delete tables: for converting soft-delete transitions when enable-active-active is disabled.
	needSoftDeleteTimeCol := isActiveActive || (!enableActiveActive && isSoftDelete)
	if needSoftDeleteTimeCol {
		var (
			offset int
			ok     bool
		)
		offset, ok = getSoftDeleteTimeColumnOffset(event, handleError)
		if !ok {
			return nil, true
		}
		softDeleteTimeColIndex = offset
		hasSoftDeleteTimeCol = true
	}

	rowTypeSummary := summarizeRowTypes(event.RowTypes)
	if filtered, skip, ok := filterDMLEventFastPath(
		event,
		tableInfo,
		enableActiveActive,
		softDeleteTimeColIndex,
		rowTypeSummary,
	); ok {
		return filtered, skip
	}

	return filterDMLEventSlowPath(
		event,
		tableInfo,
		enableActiveActive,
		softDeleteTimeColIndex,
		hasSoftDeleteTimeCol,
	)
}

// filterDMLEventSlowPath rebuilds the event by applying the row policy decisions to each row.
// The caller must ensure the input event has been rewound.
func filterDMLEventSlowPath(
	event *DMLEvent,
	tableInfo *common.TableInfo,
	enableActiveActive bool,
	softDeleteTimeColIndex int,
	hasSoftDeleteTimeCol bool,
) (*DMLEvent, bool) {
	fieldTypes := tableInfo.GetFieldSlice()
	if fieldTypes == nil {
		return event, false
	}

	newChunk := chunk.NewChunkWithCapacity(fieldTypes, len(event.RowTypes))
	rowTypes := make([]common.RowType, 0, len(event.RowTypes))
	hasRowKeys := len(event.RowKeys) != 0
	var rowKeys [][]byte
	if hasRowKeys {
		rowKeys = make([][]byte, 0, len(event.RowTypes))
	}
	hasChecksum := len(event.Checksum) != 0
	var checksums []*integrity.Checksum
	if hasChecksum {
		checksums = make([]*integrity.Checksum, 0, len(event.Checksum))
	}

	filtered := false
	kept := 0
	for {
		row, ok := event.GetNextRow()
		if !ok {
			break
		}

		decision := EvaluateRowPolicy(tableInfo, &row, enableActiveActive, softDeleteTimeColIndex)

		switch decision {
		case RowPolicySkip:
			if enableActiveActive && row.RowType == common.RowTypeDelete && hasSoftDeleteTimeCol && !row.PreRow.IsEmpty() && softDeleteTimeColIndex < row.PreRow.Len() {
				// For active-active tables, TiCDC expects user-managed hard deletes to keep
				// `_tidb_softdelete_time` as NULL, so we log it for observability.
				if row.PreRow.IsNull(softDeleteTimeColIndex) {
					log.Info("received hard delete row",
						zap.Stringer("dispatcherID", event.DispatcherID),
						zap.Int64("tableID", tableInfo.TableName.TableID),
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
		}
		if hasRowKeys {
			rowKey := row.RowKey
			if len(rowKey) == 0 {
				rowKey = nil
			}
			for i := 0; i < physicalSlots; i++ {
				rowKeys = append(rowKeys, rowKey)
			}
		}
		if hasChecksum {
			checksums = append(checksums, row.Checksum)
		}
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

// filterDMLEventFastPath returns early when the row policy would not change the event,
// or when every row is dropped. It keeps the logic aligned with EvaluateRowPolicy to
// avoid diverging behavior between fast and slow paths.
//
// The input event must have been rewound. On return, the event is guaranteed to be
// rewound as well.
func filterDMLEventFastPath(
	event *DMLEvent,
	tableInfo *common.TableInfo,
	enableActiveActive bool,
	softDeleteTimeColIndex int,
	rowTypeSummary rowTypeSummary,
) (*DMLEvent, bool, bool) {
	if event == nil {
		return nil, true, true
	}

	// enable-active-active only removes delete rows. When there is no delete row, return
	// the original event directly.
	if enableActiveActive && !rowTypeSummary.hasDelete {
		return event, false, true
	}

	// When enable-active-active is false, delete rows are always dropped and soft-delete
	// transitions are rewritten into deletes.
	//
	// If the event contains only delete rows, drop it without iterating the underlying
	// chunk. When there is no delete row, scan for the first row that would be mutated
	// by EvaluateRowPolicy and return the original event if none exists.
	if !enableActiveActive {
		if rowTypeSummary.allDelete {
			return nil, true, true
		}
		if !rowTypeSummary.hasDelete {
			if !rowTypeSummary.hasUpdate {
				return event, false, true
			}

			for {
				row, ok := event.GetNextRow()
				if !ok {
					break
				}
				if EvaluateRowPolicy(tableInfo, &row, enableActiveActive, softDeleteTimeColIndex) != RowPolicyKeep {
					event.Rewind()
					return nil, false, false
				}
			}
			event.Rewind()
			return event, false, true
		}
	}

	return nil, false, false
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
	newEvent.PostTxnEnqueued = source.PostTxnEnqueued
	newEvent.PostTxnFlushed = source.PostTxnFlushed
	newEvent.postEnqueueCalled.Store(source.postEnqueueCalled.Load())
	source.PostTxnEnqueued = nil
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

type rowTypeSummary struct {
	hasDelete bool
	hasUpdate bool
	allDelete bool
}

func summarizeRowTypes(rowTypes []common.RowType) rowTypeSummary {
	summary := rowTypeSummary{
		allDelete: len(rowTypes) != 0,
	}
	for _, rowType := range rowTypes {
		switch rowType {
		case common.RowTypeDelete:
			summary.hasDelete = true
		case common.RowTypeUpdate:
			summary.hasUpdate = true
			summary.allDelete = false
		default:
			summary.allDelete = false
		}
	}
	return summary
}
