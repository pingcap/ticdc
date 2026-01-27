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
func (w *Writer) generateActiveActiveNormalSQLs(events []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	queries := make([]string, 0)
	argsList := make([][]interface{}, 0)
	for _, event := range events {
		if event.Len() == 0 {
			continue
		}
		var (
			originTsCol    *model.ColumnInfo
			originTsOffset int
			checkOriginTs  bool
		)
		if w.cfg.IsTiDB {
			colInfo, ok := event.TableInfo.GetColumnInfoByName(commonEvent.OriginTsColumn)
			if ok {
				offset, ok := event.TableInfo.GetColumnOffsetByName(commonEvent.OriginTsColumn)
				if ok {
					originTsCol = colInfo
					originTsOffset = offset
					checkOriginTs = true
				}
			}
		}
		for {
			row, ok := event.GetNextRow()
			if !ok {
				event.Rewind()
				break
			}
			if checkOriginTs && row.RowType != common.RowTypeDelete && !row.Row.IsEmpty() && originTsOffset < row.Row.Len() {
				// In active-active replication with TiDB downstream, TiCDC assumes user-managed
				// DMLs keep `_tidb_origin_ts` as NULL. A non-NULL value indicates the row was
				// written by TiCDC and should be dropped to avoid replication loops.
				originTs := common.ExtractColVal(&row.Row, originTsCol, originTsOffset)
				if originTs != nil {
					log.Info("drop row with non null origin ts",
						zap.Uint64("commitTs", event.CommitTs))
					continue
				}
			}
			sql, args := buildActiveActiveUpsertSQL(
				event.TableInfo,
				[]*commonEvent.RowChange{&row},
				[]uint64{event.CommitTs},
			)
			queries = append(queries, sql)
			argsList = append(argsList, args)
		}
	}
	return queries, argsList
}

// ===== Per-event batch layer =====

// generateActiveActiveBatchSQLForPerEvent falls back to per-event batching when merging fails.
func (w *Writer) generateActiveActiveBatchSQLForPerEvent(events []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	var (
		queries []string
		args    [][]interface{}
	)
	for _, event := range events {
		if event.Len() == 0 {
			continue
		}
		sqls, vals := w.generateActiveActiveSQLForSingleEvent(event)
		queries = append(queries, sqls...)
		args = append(args, vals...)
	}
	return queries, args
}

// generateActiveActiveSQLForSingleEvent merges rows from a single event into one active-active UPSERT.
func (w *Writer) generateActiveActiveSQLForSingleEvent(event *commonEvent.DMLEvent) ([]string, [][]interface{}) {
	rows, commitTs := w.collectActiveActiveRowsForWrite(event)
	if len(rows) == 0 {
		return nil, nil
	}
	sql, args := buildActiveActiveUpsertSQL(event.TableInfo, rows, commitTs)
	return []string{sql}, [][]interface{}{args}
}

// ===== Cross-event batch layer =====

// generateActiveActiveBatchSQL reuses the unsafe batching logic to build a single LWW UPSERT.
func (w *Writer) generateActiveActiveBatchSQL(events []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	if len(events) == 0 {
		return []string{}, [][]interface{}{}
	}

	if len(events) == 1 {
		return w.generateActiveActiveSQLForSingleEvent(events[0])
	}

	tableInfo := events[0].TableInfo
	rowChanges, commitTs, err := w.buildRowChangesForUnSafeBatch(events, tableInfo)
	if err != nil {
		sql, values := w.generateActiveActiveBatchSQLForPerEvent(events)
		log.Info("normal sql should be", zap.Any("sql", sql), zap.Any("values", values), zap.Int("writerID", w.id))
		log.Panic("invalid rows when generating batch active active SQL",
			zap.Error(err), zap.Any("events", events), zap.Int("writerID", w.id))
		return []string{}, [][]interface{}{}
	}
	return w.batchSingleTxnActiveRows(rowChanges, commitTs, tableInfo)
}

// ===== Helpers =====

// collectActiveActiveRows copies all row changes inside the event, keeping GetNextRow semantics intact.
func collectActiveActiveRows(event *commonEvent.DMLEvent) ([]*commonEvent.RowChange, []uint64) {
	rows := make([]*commonEvent.RowChange, 0, event.Len())
	commitTs := make([]uint64, 0, event.Len())
	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		rowCopy := row
		rows = append(rows, &rowCopy)
		commitTs = append(commitTs, event.CommitTs)
	}
	return rows, commitTs
}

// collectActiveActiveRowsForWrite collects rows for active-active SQL generation and applies
// downstream-specific filtering rules.
func (w *Writer) collectActiveActiveRowsForWrite(event *commonEvent.DMLEvent) ([]*commonEvent.RowChange, []uint64) {
	rows := make([]*commonEvent.RowChange, 0, event.Len())
	commitTs := make([]uint64, 0, event.Len())

	var (
		originTsCol    *model.ColumnInfo
		originTsOffset int
		checkOriginTs  bool
	)
	if w.cfg.IsTiDB {
		colInfo, ok := event.TableInfo.GetColumnInfoByName(commonEvent.OriginTsColumn)
		if ok {
			offset, ok := event.TableInfo.GetColumnOffsetByName(commonEvent.OriginTsColumn)
			if ok {
				originTsCol = colInfo
				originTsOffset = offset
				checkOriginTs = true
			}
		}
	}

	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		if checkOriginTs && row.RowType != common.RowTypeDelete && !row.Row.IsEmpty() && originTsOffset < row.Row.Len() {
			// See the comment in generateActiveActiveNormalSQLs for the invariant.
			originTs := common.ExtractColVal(&row.Row, originTsCol, originTsOffset)
			if originTs != nil {
				log.Info("drop row with non null origin ts",
					zap.Uint64("commitTs", event.CommitTs))
				continue
			}
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
) ([]string, [][]interface{}) {
	if len(rows) != len(commitTs) {
		log.Panic("mismatched rows and commitTs for active-active batch",
			zap.Int("rows", len(rows)), zap.Int("commitTs", len(commitTs)))
	}
	filteredRows := make([]*commonEvent.RowChange, 0, len(rows))
	filteredCommitTs := make([]uint64, 0, len(rows))
	var (
		originTsCol    *model.ColumnInfo
		originTsOffset int
		checkOriginTs  bool
	)
	if w.cfg.IsTiDB {
		colInfo, ok := tableInfo.GetColumnInfoByName(commonEvent.OriginTsColumn)
		if ok {
			offset, ok := tableInfo.GetColumnOffsetByName(commonEvent.OriginTsColumn)
			if ok {
				originTsCol = colInfo
				originTsOffset = offset
				checkOriginTs = true
			}
		}
	}
	for i, row := range rows {
		if row == nil || row.Row.IsEmpty() {
			continue
		}
		if checkOriginTs && row.RowType != common.RowTypeDelete && originTsOffset < row.Row.Len() {
			originTs := common.ExtractColVal(&row.Row, originTsCol, originTsOffset)
			if originTs != nil {
				log.Info("drop row with non null origin ts",
					zap.Uint64("commitTs", commitTs[i]))
				continue
			}
		}
		filteredRows = append(filteredRows, row)
		filteredCommitTs = append(filteredCommitTs, commitTs[i])
	}
	if len(filteredRows) == 0 {
		return nil, nil
	}
	sql, args := buildActiveActiveUpsertSQL(tableInfo, filteredRows, filteredCommitTs)
	return []string{sql}, [][]interface{}{args}
}
