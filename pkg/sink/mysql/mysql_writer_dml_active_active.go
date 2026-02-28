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

package mysql

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

// Active-active DMLs mirror the same three batching tiers used by normal DMLs:
//  1. Normal SQL (no batching) – per-row UPSERTs via generateActiveActiveNormalSQLs.
//  2. Per-event batch – rows inside a DMLEvent merged by generateActiveActiveSQLForSingleEvent.
//  3. Cross-event batch – multiple events merged first, then emitted via generateActiveActiveBatchSQL.
// Sections below reuse ===== markers to highlight each tier.

// ===== Normal SQL layer =====

// generateActiveActiveNormalSQLs emits one UPSERT per row without any cross-event batching.
func (w *Writer) generateActiveActiveNormalSQLs(events []*commonEvent.DMLEvent) ([]string, [][]interface{}, []common.RowType) {
	queries := make([]string, 0)
	argsList := make([][]interface{}, 0)
	rowTypes := make([]common.RowType, 0)
	for _, event := range events {
		if event.Len() == 0 {
			continue
		}
		originTsChecker := w.newOriginTsChecker(event.TableInfo, event.GetTableID())
		for {
			row, ok := event.GetNextRow()
			if !ok {
				event.Rewind()
				break
			}
			// In active-active replication with TiDB downstream, TiCDC assumes user-managed
			// DMLs keep `_tidb_origin_ts` as NULL. A non-NULL value indicates the row was
			// written by TiCDC and should be dropped to avoid replication loops.
			if originTsChecker.shouldDropRow(&row, event.CommitTs) {
				continue
			}
			sql, args, rowType := buildActiveActiveUpsertSQL(
				event.TableInfo,
				[]*commonEvent.RowChange{&row},
				[]uint64{event.CommitTs},
			)
			queries = append(queries, sql)
			argsList = append(argsList, args)
			rowTypes = append(rowTypes, rowType)
		}
	}
	return queries, argsList, rowTypes
}

// ===== Per-event batch layer =====

// generateActiveActiveBatchSQLForPerEvent falls back to per-event batching when merging fails.
func (w *Writer) generateActiveActiveBatchSQLForPerEvent(events []*commonEvent.DMLEvent) ([]string, [][]interface{}, []common.RowType) {
	var (
		queriesList  []string
		argsList     [][]interface{}
		rowTypesList []common.RowType
	)
	for _, event := range events {
		if event.Len() == 0 {
			continue
		}
		sqls, vals, rowTypes := w.generateActiveActiveSQLForSingleEvent(event)
		queriesList = append(queriesList, sqls...)
		argsList = append(argsList, vals...)
		rowTypesList = append(rowTypesList, rowTypes...)
	}
	return queriesList, argsList, rowTypesList
}

// generateActiveActiveSQLForSingleEvent merges rows from a single event into one active-active UPSERT.
func (w *Writer) generateActiveActiveSQLForSingleEvent(event *commonEvent.DMLEvent) ([]string, [][]interface{}, []common.RowType) {
	rows, commitTs := w.collectActiveActiveRows(event)
	if len(rows) == 0 {
		return nil, nil, []common.RowType{common.RowTypeUnknown}
	}
	sql, args, rowType := buildActiveActiveUpsertSQL(event.TableInfo, rows, commitTs)
	return []string{sql}, [][]interface{}{args}, []common.RowType{rowType}
}

// ===== Cross-event batch layer =====

// generateActiveActiveBatchSQL reuses the unsafe batching logic to build a single LWW UPSERT.
func (w *Writer) generateActiveActiveBatchSQL(events []*commonEvent.DMLEvent) ([]string, [][]interface{}, []common.RowType) {
	if len(events) == 0 {
		return []string{}, [][]interface{}{}, []common.RowType{}
	}

	if len(events) == 1 {
		return w.generateActiveActiveSQLForSingleEvent(events[0])
	}

	tableInfo := events[0].TableInfo
	rowChanges, commitTs, err := w.buildRowChangesForUnSafeBatch(events, tableInfo)
	if err != nil {
		// In active active mode, when the upstream tidb_translate_softdelete_sql = off,  and insert A + delete A + insert A
		// tidb_translate_softdelete_sql = off will lead to delete be a hard delete, which event will be discarded
		// Then here we will only get insert A + insert A, which may meet error in batch optimization in buildRowChangesForUnSafeBatch
		// So here we fallback to per-event batching
		log.Warn("meet error when building row changes for active-active unsafe batch, "+
			"falling back to per-event batching",
			zap.Error(err),
			zap.Int("eventCount", len(events)),
			zap.Int64("tableID", events[0].GetTableID()))
		return w.generateActiveActiveBatchSQLForPerEvent(events)
	}
	return w.batchSingleTxnActiveRows(rowChanges, commitTs, tableInfo, events[0].GetTableID())
}

// ===== Helpers =====
// collectActiveActiveRows collects rows for active-active SQL generation and applies
// downstream-specific filtering rules.
func (w *Writer) collectActiveActiveRows(event *commonEvent.DMLEvent) ([]*commonEvent.RowChange, []uint64) {
	rows := make([]*commonEvent.RowChange, 0, event.Len())
	commitTs := make([]uint64, 0, event.Len())

	originTsChecker := w.newOriginTsChecker(event.TableInfo, event.GetTableID())

	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		// See the comment in generateActiveActiveNormalSQLs for the invariant.
		if originTsChecker.shouldDropRow(&row, event.CommitTs) {
			continue
		}
		rowCopy := row
		rows = append(rows, &rowCopy)
		commitTs = append(commitTs, event.CommitTs)
	}
	return rows, commitTs
}

// batchSingleTxnActiveRows wraps multiple row changes into one active-active UPSERT statement.
func (w *Writer) batchSingleTxnActiveRows(
	rows []*commonEvent.RowChange,
	commitTs []uint64,
	tableInfo *common.TableInfo,
	tableID int64,
) ([]string, [][]interface{}, []common.RowType) {
	if len(rows) != len(commitTs) {
		log.Panic("mismatched rows and commitTs for active active batch",
			zap.Int("rows", len(rows)), zap.Int("commitTs", len(commitTs)))
	}
	filteredRows := make([]*commonEvent.RowChange, 0, len(rows))
	filteredCommitTs := make([]uint64, 0, len(rows))
	originTsChecker := w.newOriginTsChecker(tableInfo, tableID)
	for i, row := range rows {
		if row == nil || row.Row.IsEmpty() {
			continue
		}
		if originTsChecker.shouldDropRow(row, commitTs[i]) {
			continue
		}
		filteredRows = append(filteredRows, row)
		filteredCommitTs = append(filteredCommitTs, commitTs[i])
	}
	if len(filteredRows) == 0 {
		return nil, nil, nil
	}
	sql, args, rowType := buildActiveActiveUpsertSQL(tableInfo, filteredRows, filteredCommitTs)
	return []string{sql}, [][]interface{}{args}, []common.RowType{rowType}
}

// originTsChecker filters out rows whose upstream payload already contains a non-NULL _tidb_origin_ts.
//
// Business invariant: application DMLs are expected to keep _tidb_origin_ts as NULL. The only expected
// non-NULL values come from manual operational changes (for example, directly updating the column).
// To make such incidents visible and to prevent replication loops, TiCDC logs the row metadata and
// skips these rows during replication.
type originTsChecker struct {
	enabled bool
	tableID int64
	col     *model.ColumnInfo
	offset  int
}

func (w *Writer) newOriginTsChecker(tableInfo *common.TableInfo, tableID int64) originTsChecker {
	if !w.cfg.EnableActiveActive || !w.cfg.IsTiDB {
		return originTsChecker{}
	}
	// Active-active mode relies on _tidb_origin_ts being present in the table schema.
	// Missing metadata indicates a schema mismatch and must fail fast.
	if tableInfo == nil {
		log.Panic("table info is nil when origin ts checker is enabled",
			zap.Int64("tableID", tableID))
		return originTsChecker{}
	}
	colInfo, ok := tableInfo.GetColumnInfoByName(commonEvent.OriginTsColumn)
	if !ok {
		log.Panic("origin ts column not found when origin ts checker is enabled",
			zap.Int64("tableID", tableID),
			zap.Int64("logicalTableID", tableInfo.TableName.TableID),
			zap.String("column", commonEvent.OriginTsColumn))
		return originTsChecker{}
	}
	offset, ok := tableInfo.GetColumnOffsetByName(commonEvent.OriginTsColumn)
	if !ok {
		log.Panic("origin ts column offset not found when origin ts checker is enabled",
			zap.Int64("tableID", tableID),
			zap.Int64("logicalTableID", tableInfo.TableName.TableID),
			zap.String("column", commonEvent.OriginTsColumn))
		return originTsChecker{}
	}
	return originTsChecker{
		enabled: true,
		tableID: tableID,
		col:     colInfo,
		offset:  offset,
	}
}

func (c originTsChecker) shouldDropRow(row *commonEvent.RowChange, commitTs uint64) bool {
	if !c.enabled || row == nil {
		return false
	}
	if row.RowType == common.RowTypeDelete || row.Row.IsEmpty() || c.offset >= row.Row.Len() {
		return false
	}
	originTs := common.ExtractColVal(&row.Row, c.col, c.offset)
	if originTs == nil {
		return false
	}
	log.Info("drop row with non null origin ts",
		zap.Uint64("commitTs", commitTs),
		zap.Int64("tableID", c.tableID),
		zap.Uint8("rowType", uint8(row.RowType)),
		zap.Binary("rowKey", row.RowKey),
		zap.Int("rowLen", row.Row.Len()))
	return true
}
