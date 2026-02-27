// Copyright 2024 PingCAP, Inc.
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
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/sqlmodel"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

// mysql_writer DML flow exposes three batching levels:
//  1. Normal SQL: one row → one statement, no batching (`generateNormalSQL`/`generateNormalSQLs`).
//  2. Per-event batch: rows inside a single DMLEvent are grouped before execution
//     (`generateSQLForSingleEvent`/`generateBatchSQLsPerEvent`).
//  3. Cross-event batch: multiple events are merged first, then emitted as a limited set of SQLs
//     (`generateBatchSQL` plus the `buildRowChanges*` helpers).
// Sections below are separated with ===== comments to highlight these layers.

// rowChangeWithKeys keeps a row change with its row/previous-row keys to assist de-duplication.
type rowChangeWithKeys struct {
	rowChange  *commonEvent.RowChange
	rowKeys    []byte
	preRowKeys []byte
	commitTs   uint64
}

// ===== Normal SQL (one row -> one statement) =====

// generateNormalSQLs simply iterates each event and produces SQLs without batching.
func (w *Writer) generateNormalSQLs(events []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	var (
		queries []string
		args    [][]interface{}
	)

	for _, event := range events {
		if event.Len() == 0 {
			continue
		}

		queryList, argsList := w.generateNormalSQL(event)
		queries = append(queries, queryList...)
		args = append(args, argsList...)
	}
	return queries, args
}

// generateNormalSQL converts a single DMLEvent into SQL statements, respecting safe mode.
func (w *Writer) generateNormalSQL(event *commonEvent.DMLEvent) ([]string, [][]interface{}) {
	inSafeMode := w.cfg.SafeMode || w.isInErrorCausedSafeMode || event.CommitTs < event.ReplicatingTs

	log.Debug("inSafeMode",
		zap.Bool("inSafeMode", inSafeMode),
		zap.Uint64("firstRowCommitTs", event.CommitTs),
		zap.Uint64("firstRowReplicatingTs", event.ReplicatingTs),
		zap.Bool("cfgSafeMode", w.cfg.SafeMode),
		zap.Bool("isInErrorCausedSafeMode", w.isInErrorCausedSafeMode),
		zap.Int("writerID", w.id),
	)

	var (
		queries  []string
		argsList [][]interface{}
	)
	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		var (
			query string
			args  []interface{}
		)
		switch row.RowType {
		case common.RowTypeUpdate:
			if inSafeMode {
				query, args = buildDelete(event.TableInfo, row)
				if query != "" {
					queries = append(queries, query)
					argsList = append(argsList, args)
				}
				query, args = buildInsert(event.TableInfo, row, inSafeMode)
			} else {
				query, args = buildUpdate(event.TableInfo, row)
			}
		case common.RowTypeDelete:
			query, args = buildDelete(event.TableInfo, row)
		case common.RowTypeInsert:
			query, args = buildInsert(event.TableInfo, row, inSafeMode)
		}

		if query != "" {
			queries = append(queries, query)
			argsList = append(argsList, args)
		}
	}
	return queries, argsList
}

// ===== Per-event batch (single DMLEvent) =====

// generateSQLForSingleEvent batches all row changes of one DMLEvent using the given safe-mode flag.
func (w *Writer) generateSQLForSingleEvent(event *commonEvent.DMLEvent, inDataSafeMode bool) ([]string, [][]interface{}) {
	tableInfo := event.TableInfo
	rowLists := make([]*commonEvent.RowChange, 0, event.Len())
	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		rowLists = append(rowLists, &row)
	}
	return w.batchSingleTxnDmls(rowLists, tableInfo, inDataSafeMode)
}

// generateBatchSQLsPerEvent falls back to per-event batching when cross-event merging is not possible.
func (w *Writer) generateBatchSQLsPerEvent(events []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	var (
		queries []string
		args    [][]interface{}
	)
	for _, event := range events {
		if event.Len() == 0 {
			continue
		}
		inSafeMode := w.cfg.SafeMode || w.isInErrorCausedSafeMode || event.CommitTs < event.ReplicatingTs
		sqls, vals := w.generateSQLForSingleEvent(event, inSafeMode)
		queries = append(queries, sqls...)
		args = append(args, vals...)
	}
	return queries, args
}

// ========= Cross-event batch: multiple DMLEvents ========

// buildRowChangesForUnSafeBatch merges row changes within the same batch following the
// unsafe-mode algorithm that splits delete/update/insert combinations.
// It returns the merged rows which can be used to generate final SQLs.
// for generate batch sql for multi events, we first need to compare the rows with the same pk, to generate the final rows.
// because for the batch sqls, we will first execute delete sqls, then update sqls, and finally insert sqls.
// Here we mainly divide it into 2 cases:
//  1. if all the events are in unsafe mode, we need to split update into delete and insert. So we first split each update row into a delete and a insert one.
//     Then compare all delete and insert rows, to delete useless rows.
//     if the previous row is Insert A, and the next row is Delete A -- Romove the `Insert A` one.
//
// 2. if all the events are in safe mode:
// Consider we will split the event if PK is changed, so the Update will not change the PK
// for the rows comparation, there are six situations:
// 1. the previous row is Delete A, the next row is Insert A. --- we don't need to combine the rows.
// 2. the previous row is Delete A, the next row is Update xx where A . --- we don't need to combine the rows.
// 3. the previous row is Insert A, the next row is Delete A. --- remove the row of `Insert A`
// 4. the previous row is Insert A, the next row is Update xx where A --  remove the row of `Insert A`, change the row `Update A` to `Insert A`
// 5. the previous row is Update xx where A, the next row is Delete A. --- remove the row `Update xx where A`
// 6. the previous row is Update xx where A, the next row is Update xx where A. --- we need to remove the row, and change the second Update's preRows = first Update's preRows
//
// For these all changes to row, we will continue to compare from the beginnning to the end, until there is no change.
// Then we can generate the final sql of delete/update/insert.
//
// generateBatchSQL decides which batch algorithm (safe vs unsafe) should be used and
// returns the SQLs/args pairs accordingly.
// Considering the batch algorithm in safe mode is O(n^3), which n is the number of rows.
// So we need to limit the number of rows in one batch to avoid performance issues.
func (w *Writer) generateBatchSQL(events []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	if len(events) == 0 {
		return []string{}, [][]interface{}{}
	}

	inSafeMode := w.cfg.SafeMode || w.isInErrorCausedSafeMode || events[0].CommitTs < events[0].ReplicatingTs

	if len(events) == 1 {
		// only one event, we don't need to do batch
		return w.generateSQLForSingleEvent(events[0], inSafeMode)
	}

	if inSafeMode {
		// Insert will translate to Replace
		return w.generateBatchSQLInSafeMode(events)
	}

	return w.generateBatchSQLInUnSafeMode(events)
}

func (w *Writer) buildRowChangesForUnSafeBatch(
	events []*commonEvent.DMLEvent,
	tableInfo *common.TableInfo,
) ([]*commonEvent.RowChange, []uint64, error) {
	// Step 1 extract all rows in these events to rowLists, and calcuate row key for each row(based on pk value)
	rowLists := make([]rowChangeWithKeys, 0)
	for _, event := range events {
		for {
			row, ok := event.GetNextRow()
			if !ok {
				event.Rewind()
				break
			}
			rowCopy := row
			rowChangeWithKeys := rowChangeWithKeys{
				rowChange: &rowCopy,
				commitTs:  event.CommitTs,
			}
			if !row.Row.IsEmpty() {
				_, keys := genKeyAndHash(&row.Row, tableInfo)
				rowChangeWithKeys.rowKeys = keys
			}
			if !row.PreRow.IsEmpty() {
				_, keys := genKeyAndHash(&row.PreRow, tableInfo)
				rowChangeWithKeys.preRowKeys = keys
			}
			rowLists = append(rowLists, rowChangeWithKeys)
		}
	}

	// Step 2 combine the rows until there is no change
	// Consider we will split the event if PK is changed, so the Update will not change the PK
	// for the rows comparation, there are six situations:
	// 1. the previous row is Delete A, the next row is Insert A. --- we don't need to combine the rows.
	// 2. the previous row is Delete A, the next row is Update xx where A . --- we don't need to combine the rows.
	// 3. the previous row is Insert A, the next row is Delete A. --- remove the row of `Insert A`
	// 4. the previous row is Insert A, the next row is Update xx where A --  remove the row of `Insert A`, change the row `Update A` to `Insert A`
	// 5. the previous row is Update xx where A, the next row is Delete A. --- remove the row `Update xx where A`
	// 6. the previous row is Update xx where A, the next row is Update xx where A. --- we need to remove the row, and change the second Update's preRows = first Update's preRows
	for {
		// hasUpdate to determine whether we can break the combine logic
		hasUpdate := false
		// flagList used to store the exists or not for this row. True means exists.
		flagList := make([]bool, len(rowLists))
		for i := range flagList {
			flagList[i] = true
		}
		for i := 0; i < len(rowLists); i++ {
			if !flagList[i] {
				continue
			}
		innerLoop:
			for j := i + 1; j < len(rowLists); j++ {
				if !flagList[j] {
					continue
				}
				rowType := rowLists[i].rowChange.RowType
				nextRowType := rowLists[j].rowChange.RowType
				switch rowType {
				case common.RowTypeInsert:
					rowKey := rowLists[i].rowKeys
					if nextRowType == common.RowTypeInsert {
						if compareKeys(rowKey, rowLists[j].rowKeys) {
							return nil, nil, cerror.ErrUnexpected.FastGenByArgs("duplicate insert rows with same key")
						}
					} else if nextRowType == common.RowTypeDelete {
						if compareKeys(rowKey, rowLists[j].preRowKeys) {
							flagList[i] = false
							hasUpdate = true
							break innerLoop
						}
					} else if nextRowType == common.RowTypeUpdate {
						if !compareKeys(rowLists[j].preRowKeys, rowLists[j].rowKeys) {
							return nil, nil, cerror.ErrUnexpected.FastGenByArgs("update row key mismatch")
						}
						if compareKeys(rowKey, rowLists[j].preRowKeys) {
							flagList[i] = false
							preRowChange := rowLists[j].rowChange
							newRowChange := commonEvent.RowChange{
								Row:     preRowChange.Row,
								RowType: common.RowTypeInsert,
							}
							rowLists[j] = rowChangeWithKeys{
								rowChange: &newRowChange,
								rowKeys:   rowLists[j].rowKeys,
								commitTs:  rowLists[j].commitTs,
							}
							hasUpdate = true
							break innerLoop
						}
					}
				case common.RowTypeUpdate:
					rowKey := rowLists[i].rowKeys
					if !compareKeys(rowKey, rowLists[i].preRowKeys) {
						return nil, nil, cerror.ErrUnexpected.FastGenByArgs("update row key mismatch")
					}
					if nextRowType == common.RowTypeInsert {
						if compareKeys(rowKey, rowLists[j].rowKeys) {
							return nil, nil, cerror.ErrUnexpected.FastGenByArgs("duplicate rows for update and insert")
						}
					} else if nextRowType == common.RowTypeDelete {
						if compareKeys(rowKey, rowLists[j].preRowKeys) {
							flagList[j] = false
							preRowChange := rowLists[i].rowChange
							newRowChange := commonEvent.RowChange{
								PreRow:  preRowChange.PreRow,
								RowType: common.RowTypeDelete,
							}
							rowLists[i] = rowChangeWithKeys{
								rowChange:  &newRowChange,
								preRowKeys: rowKey,
								commitTs:   rowLists[j].commitTs, // use the delete's commitTs to make LWW correctly
							}
							hasUpdate = true
							break innerLoop
						}
					} else if nextRowType == common.RowTypeUpdate {
						if compareKeys(rowKey, rowLists[j].preRowKeys) {
							if !compareKeys(rowLists[j].preRowKeys, rowLists[j].rowKeys) {
								return nil, nil, cerror.ErrUnexpected.FastGenByArgs("update row key mismatch")
							}
							newRowChange := commonEvent.RowChange{
								PreRow:  rowLists[j].rowChange.PreRow,
								Row:     rowLists[j].rowChange.Row,
								RowType: common.RowTypeUpdate,
							}
							rowLists[j] = rowChangeWithKeys{
								rowChange:  &newRowChange,
								preRowKeys: rowKey,
								rowKeys:    rowKey,
								commitTs:   rowLists[j].commitTs,
							}
							flagList[i] = false
							hasUpdate = true
							break innerLoop
						}
					}
				}
			}
		}

		if !hasUpdate {
			// means no more changes for the rows, break and generate sqls.
			break
		}
		newRowLists := make([]rowChangeWithKeys, 0, len(rowLists))
		for i := 0; i < len(rowLists); i++ {
			if flagList[i] {
				newRowLists = append(newRowLists, rowLists[i])
			}
		}
		rowLists = newRowLists
	}

	finalRowLists := make([]*commonEvent.RowChange, 0, len(rowLists))
	finalCommitTs := make([]uint64, 0, len(rowLists))
	for i := 0; i < len(rowLists); i++ {
		finalRowLists = append(finalRowLists, rowLists[i].rowChange)
		finalCommitTs = append(finalCommitTs, rowLists[i].commitTs)
	}
	return finalRowLists, finalCommitTs, nil
}

// generateBatchSQLInUnSafeMode merges rows with the same keys and produces batch SQLs
// following the non-safe path (INSERT/UPDATE/DELETE without REPLACE semantics).
func (w *Writer) generateBatchSQLInUnSafeMode(events []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	tableInfo := events[0].TableInfo
	finalRowLists, _, err := w.buildRowChangesForUnSafeBatch(events, tableInfo)
	if err != nil {
		sql, values := w.generateBatchSQLsPerEvent(events)
		log.Info("normal sql should be", zap.Any("sql", sql), zap.String("values", util.RedactAny(values)), zap.Int("writerID", w.id))
		log.Panic("invalid rows when generating batch SQL in unsafe mode",
			zap.Error(err), zap.Any("events", events), zap.Int("writerID", w.id))
	}
	return w.batchSingleTxnDmls(finalRowLists, tableInfo, false)
}

// generateBatchSQLInSafeMode rewrites rows into delete/insert pairs to ensure REPLACE semantics.
func (w *Writer) generateBatchSQLInSafeMode(events []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	tableInfo := events[0].TableInfo

	// step 1. divide update row to delete row and insert row, and set into map based on the key hash
	rowsMap := make(map[uint64][]*commonEvent.RowChange)
	hashToKeyMap := make(map[uint64][]byte)

	addRowToMap := func(row *commonEvent.RowChange, rowData *chunk.Row, event *commonEvent.DMLEvent) ([]string, [][]interface{}, bool) {
		hashValue, keyValue := genKeyAndHash(rowData, tableInfo)
		if _, ok := hashToKeyMap[hashValue]; !ok {
			hashToKeyMap[hashValue] = keyValue
		} else {
			if !compareKeys(hashToKeyMap[hashValue], keyValue) {
				log.Warn("the key hash is equal, but the keys is not the same; so we don't use batch generate sql, but use the normal generated sql instead")
				event.Rewind() // reset event
				// fallback to per-event batch sql
				sql, args := w.generateBatchSQLsPerEvent(events)
				return sql, args, false
			}
		}
		rowsMap[hashValue] = append(rowsMap[hashValue], row)
		return nil, nil, true
	}

	for _, event := range events {
		for {
			row, ok := event.GetNextRow()
			if !ok {
				event.Rewind()
				break
			}
			switch row.RowType {
			case common.RowTypeUpdate:
				{
					deleteRow := commonEvent.RowChange{RowType: common.RowTypeDelete, PreRow: row.PreRow}
					sql, args, ok := addRowToMap(&deleteRow, &row.PreRow, event)
					if !ok {
						return sql, args
					}
				}

				{
					insertRow := commonEvent.RowChange{RowType: common.RowTypeInsert, Row: row.Row}
					sql, args, ok := addRowToMap(&insertRow, &row.Row, event)
					if !ok {
						return sql, args
					}
				}
			case common.RowTypeDelete:
				sql, args, ok := addRowToMap(&row, &row.PreRow, event)
				if !ok {
					return sql, args
				}
			case common.RowTypeInsert:
				sql, args, ok := addRowToMap(&row, &row.Row, event)
				if !ok {
					return sql, args
				}
			}
		}
	}

	// step 2. compare the rows in the same key hash, to generate the final rows
	rowsList := make([]*commonEvent.RowChange, 0, len(rowsMap))
	for _, rowChanges := range rowsMap {
		if len(rowChanges) == 0 {
			continue
		}
		if len(rowChanges) == 1 {
			rowsList = append(rowsList, rowChanges[0])
			continue
		}
		// should only happen the rows like 'insert / delete / insert / delete ...' or 'delete / insert /delete ...' ,
		// should not happen 'insert / insert' or 'delete / delete'
		// so only the last one can be the final row changes
		prevType := rowChanges[0].RowType
		for i := 1; i < len(rowChanges); i++ {
			rowType := rowChanges[i].RowType
			if rowType == prevType {
				sql, values := w.generateBatchSQLsPerEvent(events)
				log.Info("normal sql should be", zap.Any("sql", sql), zap.String("values", util.RedactAny(values)), zap.Int("writerID", w.id))
				log.Panic("invalid row changes", zap.String("schemaName", tableInfo.GetSchemaName()),
					zap.String("tableName", tableInfo.GetTableName()), zap.Any("rowChanges", rowChanges),
					zap.Any("prevType", prevType), zap.Any("currentType", rowType), zap.Int("writerID", w.id))
			}
			prevType = rowType
		}
		rowsList = append(rowsList, rowChanges[len(rowChanges)-1])
	}
	// step 3. generate sqls based on rowsList
	return w.batchSingleTxnDmls(rowsList, tableInfo, true)
}

// batchSingleTxnDmls groups row changes into delete/update/insert SQLs for a single transaction.
// inSafeMode means we should use replace sql instead of insert sql to make sure there will not
// be duplicate entry error.
func (w *Writer) batchSingleTxnDmls(
	rows []*commonEvent.RowChange,
	tableInfo *common.TableInfo,
	inSafeMode bool,
) (sqls []string, values [][]interface{}) {
	insertRows, updateRows, deleteRows := w.groupRowsByType(rows, tableInfo)

	// handle delete
	if len(deleteRows) > 0 {
		for _, rows := range deleteRows {
			sql, value := sqlmodel.GenDeleteSQL(w.cfg.whereClause, rows...)
			sqls = append(sqls, sql)
			values = append(values, value)
		}
	}

	// handle update
	if len(updateRows) > 0 {
		if w.cfg.IsTiDB {
			for _, rows := range updateRows {
				s, v := w.genUpdateSQL(rows...)
				sqls = append(sqls, s...)
				values = append(values, v...)
			}
			// The behavior of update statement differs between TiDB and MySQL.
			// So we don't use batch update statement when downstream is MySQL.
			// Ref:https://docs.pingcap.com/tidb/stable/sql-statement-update#mysql-compatibility
		} else {
			for _, rows := range updateRows {
				for _, row := range rows {
					sql, value := row.GenSQL(sqlmodel.DMLUpdate)
					sqls = append(sqls, sql)
					values = append(values, value)
				}
			}
		}
	}

	// handle insert
	if len(insertRows) > 0 {
		for _, rows := range insertRows {
			if inSafeMode {
				sql, value := sqlmodel.GenInsertSQL(sqlmodel.DMLReplace, rows...)
				sqls = append(sqls, sql)
				values = append(values, value)
			} else {
				sql, value := sqlmodel.GenInsertSQL(sqlmodel.DMLInsert, rows...)
				sqls = append(sqls, sql)
				values = append(values, value)

			}
		}
	}

	return
}

// groupRowsByType categorizes row changes by operation type and splits them by configured limits.
func (w *Writer) groupRowsByType(
	rows []*commonEvent.RowChange,
	tableInfo *common.TableInfo,
) (insertRows, updateRows, deleteRows [][]*sqlmodel.RowChange) {
	rowSize := len(rows)
	if rowSize > w.cfg.MaxTxnRow {
		rowSize = w.cfg.MaxTxnRow
	}

	insertRow := make([]*sqlmodel.RowChange, 0, rowSize)
	updateRow := make([]*sqlmodel.RowChange, 0, rowSize)
	deleteRow := make([]*sqlmodel.RowChange, 0, rowSize)

	eventTableInfo := tableInfo
	for _, row := range rows {
		switch row.RowType {
		case common.RowTypeInsert:
			args := getArgsWithGeneratedColumn(&row.Row, tableInfo)
			newInsertRow := sqlmodel.NewRowChange(
				&tableInfo.TableName,
				nil,
				nil,
				args,
				eventTableInfo,
				nil, nil)

			insertRow = append(insertRow, newInsertRow)
			if len(insertRow) >= w.cfg.MaxTxnRow {
				insertRows = append(insertRows, insertRow)
				insertRow = make([]*sqlmodel.RowChange, 0, rowSize)
			}
		case common.RowTypeUpdate:
			args := getArgsWithGeneratedColumn(&row.Row, tableInfo)
			preArgs := getArgsWithGeneratedColumn(&row.PreRow, tableInfo)
			newUpdateRow := sqlmodel.NewRowChange(
				&tableInfo.TableName,
				nil,
				preArgs,
				args,
				eventTableInfo,
				nil, nil)
			updateRow = append(updateRow, newUpdateRow)
			if len(updateRow) >= w.cfg.MaxMultiUpdateRowCount {
				updateRows = append(updateRows, updateRow)
				updateRow = make([]*sqlmodel.RowChange, 0, rowSize)
			}
		case common.RowTypeDelete:
			preArgs := getArgsWithGeneratedColumn(&row.PreRow, tableInfo)
			newDeleteRow := sqlmodel.NewRowChange(
				&tableInfo.TableName,
				nil,
				preArgs,
				nil,
				eventTableInfo,
				nil, nil)
			deleteRow = append(deleteRow, newDeleteRow)
			if len(deleteRow) >= w.cfg.MaxTxnRow {
				deleteRows = append(deleteRows, deleteRow)
				deleteRow = make([]*sqlmodel.RowChange, 0, rowSize)
			}
		}
	}
	if len(insertRow) > 0 {
		insertRows = append(insertRows, insertRow)
	}
	if len(updateRow) > 0 {
		updateRows = append(updateRows, updateRow)
	}
	if len(deleteRow) > 0 {
		deleteRows = append(deleteRows, deleteRow)
	}

	return
}

// genUpdateSQL creates batched UPDATE statements when the payload size permits.
func (w *Writer) genUpdateSQL(rows ...*sqlmodel.RowChange) ([]string, [][]interface{}) {
	size := 0
	for _, r := range rows {
		size += int(r.GetApproximateDataSize())
	}
	if size < w.cfg.MaxMultiUpdateRowSize*len(rows) {
		// use multi update in one SQL
		sql, value := sqlmodel.GenUpdateSQL(w.cfg.whereClause, rows...)
		return []string{sql}, [][]interface{}{value}
	}
	// each row has one independent update SQL.
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		sql, value := row.GenSQL(sqlmodel.DMLUpdate)
		sqls = append(sqls, sql)
		values = append(values, value)
	}
	return sqls, values
}
