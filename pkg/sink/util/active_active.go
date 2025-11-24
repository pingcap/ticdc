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
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
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
func EvaluateRowPolicy(
	tableInfo *common.TableInfo,
	row *commonEvent.RowChange,
	enableActiveActive bool,
) (RowPolicyDecision, error) {
	if tableInfo == nil || row == nil {
		return RowPolicyKeep, nil
	}

	if !tableInfo.IsActiveActiveTable() && !tableInfo.IsSoftDeleteTable() {
		return RowPolicyKeep, nil
	}

	if row.RowType == common.RowTypeDelete {
		return RowPolicySkip, nil
	}

	// When enable-active-active is true, other sink types should not be used,
	// but to keep semantics consistent we only skip delete rows and keep the rest.
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

func needConvertUpdateToDelete(tableInfo *common.TableInfo, row *commonEvent.RowChange) (bool, error) {
	if tableInfo == nil || row == nil {
		return false, nil
	}
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
	return isZeroValue(oldVal) && !isZeroValue(newVal)
}

func isZeroValue(val interface{}) bool {
	if val == nil {
		return true
	}
	switch v := val.(type) {
	case int:
		return v == 0
	case int32:
		return v == 0
	case int64:
		return v == 0
	case uint:
		return v == 0
	case uint32:
		return v == 0
	case uint64:
		return v == 0
	case float32:
		return v == 0
	case float64:
		return v == 0
	case string:
		return v == "" || v == "0"
	case []byte:
		return len(v) == 0
	default:
		return false
	}
}

// ApplyRowPolicyDecision mutates the row based on decision.
func ApplyRowPolicyDecision(row *commonEvent.RowChange, decision RowPolicyDecision) {
	switch decision {
	case RowPolicyConvertToDelete:
		row.RowType = common.RowTypeDelete
		row.Row = chunk.Row{}
	}
}

// FilterDMLEvent applies row policy decisions to each row in the DMLEvent.
// Returns the possibly modified event, whether it should be skipped entirely, and error if any.
func FilterDMLEvent(event *commonEvent.DMLEvent, enableActiveActive bool) (*commonEvent.DMLEvent, bool, error) {
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
			filtered = true
			continue
		case RowPolicyConvertToDelete:
			ApplyRowPolicyDecision(&row, decision)
			filtered = true
		default:
		}

		appendRowChangeToChunk(newChunk, &row)
		rowTypes = append(rowTypes, row.RowType)
		rowKeys = append(rowKeys, cloneRowKey(row.RowKey))
		checksums = append(checksums, row.Checksum)
		kept++
	}
	event.Rewind()

	if !filtered {
		return event, false, nil
	}

	if kept == 0 {
		event.PostFlush()
		return nil, true, nil
	}

	newEvent := commonEvent.NewDMLEvent(event.DispatcherID, event.PhysicalTableID, event.StartTs, event.CommitTs, event.TableInfo)
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

func appendRowChangeToChunk(chk *chunk.Chunk, row *commonEvent.RowChange) {
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
