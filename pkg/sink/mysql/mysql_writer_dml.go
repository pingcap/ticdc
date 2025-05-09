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
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/sink/sqlmodel"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// for the events, we try to batch the events of the same table into single update / insert / delete query,
// to enhance the performance of the sink.
// While we only support to batch the events with pks, and all the events inSafeMode or all not in inSafeMode.
// the process is as follows:
//  1. we group the events by dispatcherID, and hold the order for the events of the same dispatcher
//  2. For each group,
//     if the table does't have a handle key or have virtual column, we just generate the sqls for each event row.(TODO: support the case without pk but have uk)
//     Otherwise,
//     if there is only one rows of the whole group, we generate the sqls for the row.
//     Otherwise, we batch all the event rows for the same dispatcherID to a single delete / update/ insert query(in order)
func (w *Writer) prepareDMLs(events []*commonEvent.DMLEvent) *preparedDMLs {
	dmls := dmlsPool.Get().(*preparedDMLs)
	dmls.reset()
	// Step 1: group the events by table ID
	eventsGroup := make(map[int64][]*commonEvent.DMLEvent) // tableID --> events
	for _, event := range events {
		// calculate for metrics
		dmls.rowCount += int(event.Len())
		if len(dmls.startTs) == 0 || dmls.startTs[len(dmls.startTs)-1] != event.StartTs {
			dmls.startTs = append(dmls.startTs, event.StartTs)
		}
		dmls.approximateSize += event.GetRowsSize()
		tableID := event.GetTableID()
		if _, ok := eventsGroup[tableID]; !ok {
			eventsGroup[tableID] = make([]*commonEvent.DMLEvent, 0)
		}
		eventsGroup[tableID] = append(eventsGroup[tableID], event)
	}

	// Step 2: prepare the dmls for each group
	var (
		queryList []string
		argsList  [][]interface{}
	)
	for _, eventsInGroup := range eventsGroup {
		tableInfo := eventsInGroup[0].TableInfo
		if !tableInfo.HasPrimaryKey() || tableInfo.HasVirtualColumns() {
			// check if the table has a handle key or has a virtual column
			queryList, argsList = w.generateNormalSQLs(eventsInGroup)
		} else if len(eventsInGroup) == 1 && eventsInGroup[0].Len() == 1 {
			// if there only one row in the group, we can use the normal sql generate
			queryList, argsList = w.generateNormalSQLs(eventsInGroup)
		} else if len(eventsInGroup) > 0 {
			// if the events are in different safe mode, we can't use the batch dml generate
			firstEventSafeMode := !w.cfg.SafeMode && eventsInGroup[0].CommitTs > eventsInGroup[0].ReplicatingTs
			finalEventSafeMode := !w.cfg.SafeMode && eventsInGroup[len(eventsInGroup)-1].CommitTs > eventsInGroup[len(eventsInGroup)-1].ReplicatingTs
			if firstEventSafeMode != finalEventSafeMode {
				queryList, argsList = w.generateNormalSQLs(eventsInGroup)
			} else {
				// use the batch dml generate
				queryList, argsList = w.generateBatchSQL(eventsInGroup)
			}
		}
		dmls.sqls = append(dmls.sqls, queryList...)
		dmls.values = append(dmls.values, argsList...)
	}
	// Pre-check log level to avoid dmls.String() being called unnecessarily
	// This method is expensive, so we only log it when the log level is debug.
	if log.GetLevel() == zapcore.DebugLevel {
		log.Debug("prepareDMLs", zap.Any("dmls", dmls.String()), zap.Any("events", events))
	}

	return dmls
}

// for generate batch sql for multi events, we first need to compare the rows with the same pk, to generate the final rows.
// because for the batch sqls, we will first execute delete sqls, then update sqls, and finally insert sqls.
// Here we mainly divide it into 2 cases:
//  1. if all the events are in unsafe mode, we need to split update into delete and insert. So we first split each update row into a delete and a insert one.
//     Then compare all delete and insert rows, to delete useless rows.
//     if the previous row is Insert A, and the next row is Delete A -- Romove the `Insert A` one.
//
// 2. if all the events are in safe mode:
// for the rows comparation, there are six situations:
// 1. the previous row is Delete A, the next row is Insert A. --- we don't need to combine the rows.
// 2. the previous row is Delete A, the next row is Update B to A. --- we don't need to combine the rows.
// 3. the previous row is Insert A, the next row is Delete A. --- remove the row of `Insert A`
// 4. the previous row is Insert A, the next row is Update A to C --  remove the row of `Insert A`, change the row `Update A to C` to `Insert C`
// 5. the previous row is Update A to B, the next row is Delete B. --- remove the row `Delete B`, change the row `Update A to B` to `Delete A`
// 6. the previous row is Update A to B, the next row is Update B to C. --- we don't need to combine the rows.
// 7. the previous row is Update A to B, the next row is Update A to C. --- remove the row `Update A to B`
//
// For these all changes to row, we will continue to compare from the beginnning to the end, until there is no change.
// Then we can generate the final sql of delete/update/insert.
func (w *Writer) generateBatchSQL(events []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	inSafeMode := !w.cfg.SafeMode && events[0].CommitTs > events[0].ReplicatingTs
	if inSafeMode {
		return w.generateBatchSQLInSafeMode(events)
	}
	return w.generateBatchSQLInUnsafeMode(events)
}

func (w *Writer) generateBatchSQLInSafeMode(events []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	tableInfo := events[0].TableInfo
	type RowChangeWithKeys struct {
		RowChange  *commonEvent.RowChange
		RowKeys    []byte
		PreRowKeys []byte
	}
	// step 1. loop to combine the rows until there is no change
	rowLists := make([]RowChangeWithKeys, 0)
	for _, event := range events {
		for {
			row, ok := event.GetNextRow()
			if !ok {
				event.Rewind()
				break
			}
			rowChangeWithKeys := RowChangeWithKeys{RowChange: &row}
			if !row.Row.IsEmpty() {
				_, keys := genKeyAndHash(&row.Row, tableInfo)
				rowChangeWithKeys.RowKeys = keys
			}
			if !row.PreRow.IsEmpty() {
				_, keys := genKeyAndHash(&row.PreRow, tableInfo)
				rowChangeWithKeys.PreRowKeys = keys
			}
			rowLists = append(rowLists, rowChangeWithKeys)
		}
	}

	for {
		// hasUpdate to determine whether we can break the combine logic
		hasUpdate := false
		// flagList used to store the exists or not for this row. True means exists.
		flagList := make([]bool, len(rowLists))
		for i := 0; i < len(rowLists); i++ {
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
				rowType := rowLists[i].RowChange.RowType
				nextRowType := rowLists[j].RowChange.RowType
				switch rowType {
				case commonEvent.RowTypeDelete:
					rowKey := rowLists[i].PreRowKeys
					if nextRowType == commonEvent.RowTypeUpdate {
						if compareKeys(rowKey, rowLists[j].PreRowKeys) {
							log.Panic("Here are two invalid rows, one is Delete A, the other is Update A to B", zap.Any("Events", events))
						}
					}
				case commonEvent.RowTypeInsert:
					rowKey := rowLists[i].RowKeys
					if nextRowType == commonEvent.RowTypeInsert {
						if compareKeys(rowKey, rowLists[j].RowKeys) {
							log.Panic("Here are two invalid rows with the same row type and keys", zap.Any("Events", events))
						}
					} else if nextRowType == commonEvent.RowTypeDelete {
						if compareKeys(rowKey, rowLists[j].PreRowKeys) {
							// remove the insert one, and break the inner loop for row i
							flagList[i] = false
							hasUpdate = true
							break innerLoop
						}
					} else if nextRowType == commonEvent.RowTypeUpdate {
						if compareKeys(rowKey, rowLists[j].PreRowKeys) {
							// remove insert one, and break the inner loop for row i
							flagList[i] = false
							// change update one to insert
							preRowChange := rowLists[j].RowChange
							newRowChange := commonEvent.RowChange{
								Row:     preRowChange.Row,
								RowType: commonEvent.RowTypeInsert,
							}
							rowLists[j] = RowChangeWithKeys{
								RowChange: &newRowChange,
								RowKeys:   rowLists[j].RowKeys,
							}
							hasUpdate = true
							break innerLoop
						}
					}
				case commonEvent.RowTypeUpdate:
					rowKey := rowLists[i].RowKeys
					preRowKey := rowLists[i].PreRowKeys
					if nextRowType == commonEvent.RowTypeInsert {
						if compareKeys(rowKey, rowLists[j].RowKeys) {
							log.Panic("Here are two invalid rows with the same row type and keys", zap.Any("Events", events))
						}
					} else if nextRowType == commonEvent.RowTypeDelete {
						if compareKeys(rowKey, rowLists[j].PreRowKeys) {
							// remove the update one, and break the inner loop
							flagList[j] = false
							// change the update to delete
							preRowChange := rowLists[i].RowChange
							newRowChange := commonEvent.RowChange{
								PreRow:  preRowChange.PreRow,
								RowType: commonEvent.RowTypeDelete,
							}
							rowLists[i] = RowChangeWithKeys{
								RowChange:  &newRowChange,
								PreRowKeys: rowLists[i].PreRowKeys,
							}
							hasUpdate = true
							break innerLoop
						}
					} else if nextRowType == commonEvent.RowTypeUpdate {
						if compareKeys(preRowKey, rowLists[j].PreRowKeys) {
							// remove the first one, and break the loop
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
		} else {
			newRowLists := make([]RowChangeWithKeys, 0, len(rowLists))
			for i := 0; i < len(rowLists); i++ {
				if flagList[i] {
					newRowLists = append(newRowLists, rowLists[i])
				}
			}
			rowLists = newRowLists
		}

	}

	finalRowLists := make([]*commonEvent.RowChange, 0, len(rowLists))

	for i := 0; i < len(rowLists); i++ {
		finalRowLists = append(finalRowLists, rowLists[i].RowChange)
	}

	// step 2. generate sqls
	return w.batchSingleTxnDmls(finalRowLists, tableInfo, true)
}

func (w *Writer) generateBatchSQLInUnsafeMode(events []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	tableInfo := events[0].TableInfo
	// step 1. divide update row to delete row and insert row, and set into map based on the key hash
	rowsMap := make(map[uint64][]*commonEvent.RowChange)
	hashToKeyMap := make(map[uint64][]byte)

	// TODO: extract a function here to clean code
	for _, event := range events {
		for {
			row, ok := event.GetNextRow()
			if !ok {
				event.Rewind()
				break
			}
			switch row.RowType {
			case commonEvent.RowTypeUpdate:
				{
					deleteRow := commonEvent.RowChange{RowType: commonEvent.RowTypeDelete, PreRow: row.PreRow}
					hashValue, keyValue := genKeyAndHash(&row.PreRow, tableInfo)
					if _, ok = hashToKeyMap[hashValue]; !ok {
						hashToKeyMap[hashValue] = keyValue
					} else {
						if !compareKeys(hashToKeyMap[hashValue], keyValue) {
							log.Warn("the key hash is equal, but the keys is not the same; so we don't use batch generate sql, but use the normal generated sql instead")
							event.Rewind() // reset event
							// use normal sql instead
							return w.generateNormalSQLs(events)
						}
					}
					rowsMap[hashValue] = append(rowsMap[hashValue], &deleteRow)
				}

				{
					insertRow := commonEvent.RowChange{RowType: commonEvent.RowTypeInsert, Row: row.Row}
					hashValue, keyValue := genKeyAndHash(&row.Row, tableInfo)
					if _, ok = hashToKeyMap[hashValue]; !ok {
						hashToKeyMap[hashValue] = keyValue
					} else {
						if !compareKeys(hashToKeyMap[hashValue], keyValue) {
							log.Warn("the key hash is equal, but the keys is not the same; so we don't use batch generate sql, but use the normal generated sql instead")
							event.Rewind() // reset event
							// use normal sql instead
							return w.generateNormalSQLs(events)
						}
					}

					rowsMap[hashValue] = append(rowsMap[hashValue], &insertRow)
				}
			case commonEvent.RowTypeDelete:
				hashValue, keyValue := genKeyAndHash(&row.PreRow, tableInfo)
				if _, ok = hashToKeyMap[hashValue]; !ok {
					hashToKeyMap[hashValue] = keyValue
				} else {
					if !compareKeys(hashToKeyMap[hashValue], keyValue) {
						log.Warn("the key hash is equal, but the keys is not the same; so we don't use batch generate sql, but use the normal generated sql instead")
						event.Rewind() // reset event
						// use normal sql instead
						return w.generateNormalSQLs(events)
					}
				}
				rowsMap[hashValue] = append(rowsMap[hashValue], &row)
			case commonEvent.RowTypeInsert:
				hashValue, keyValue := genKeyAndHash(&row.Row, tableInfo)
				if _, ok = hashToKeyMap[hashValue]; !ok {
					hashToKeyMap[hashValue] = keyValue
				} else {
					if !compareKeys(hashToKeyMap[hashValue], keyValue) {
						log.Warn("the key hash is equal, but the keys is not the same; so we don't use batch generate sql, but use the normal generated sql instead")
						event.Rewind() // reset event
						// use normal sql instead
						return w.generateNormalSQLs(events)
					}
				}
				rowsMap[hashValue] = append(rowsMap[hashValue], &row)
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
				log.Panic("invalid row changes", zap.Any("rowChanges", rowChanges), zap.Any("prevType", prevType), zap.Any("currentType", rowType))
			}
			prevType = rowType
		}
		rowsList = append(rowsList, rowChanges[len(rowChanges)-1])
	}

	// step 3. generate sqls
	return w.batchSingleTxnDmls(rowsList, tableInfo, false)
}

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

func (w *Writer) generateNormalSQL(event *commonEvent.DMLEvent) ([]string, [][]interface{}) {
	inSafeMode := !w.cfg.SafeMode && event.CommitTs > event.ReplicatingTs
	log.Debug("inSafeMode",
		zap.Bool("inSafeMode", inSafeMode),
		zap.Uint64("firstRowCommitTs", event.CommitTs),
		zap.Uint64("firstRowReplicatingTs", event.ReplicatingTs),
		zap.Bool("safeMode", w.cfg.SafeMode))

	var (
		queries  []string
		argsList [][]interface{}
	)
	for {
		row, ok := event.GetNextRow()
		if !ok {
			break
		}
		var (
			query string
			args  []interface{}
		)
		switch row.RowType {
		case commonEvent.RowTypeUpdate:
			if inSafeMode {
				query, args = buildUpdate(event.TableInfo, row, w.cfg.ForceReplicate)
			} else {
				query, args = buildDelete(event.TableInfo, row, w.cfg.ForceReplicate)
				if query != "" {
					queries = append(queries, query)
					argsList = append(argsList, args)
				}
				query, args = buildInsert(event.TableInfo, row, inSafeMode)
			}
		case commonEvent.RowTypeDelete:
			query, args = buildDelete(event.TableInfo, row, w.cfg.ForceReplicate)
		case commonEvent.RowTypeInsert:
			query, args = buildInsert(event.TableInfo, row, inSafeMode)
		}

		if query != "" {
			queries = append(queries, query)
			argsList = append(argsList, args)
		}
	}
	return queries, argsList
}

func (w *Writer) execDMLWithMaxRetries(dmls *preparedDMLs) error {
	if len(dmls.sqls) != len(dmls.values) {
		return cerror.ErrUnexpected.FastGenByArgs(fmt.Sprintf("unexpected number of sqls and values, sqls is %s, values is %s", dmls.sqls, dmls.values))
	}

	// approximateSize is multiplied by 2 because in extreme circustumas, every
	// byte in dmls can be escaped and adds one byte.
	fallbackToSeqWay := dmls.approximateSize*2 > w.maxAllowedPacket

	writeTimeout, _ := time.ParseDuration(w.cfg.WriteTimeout)
	writeTimeout += networkDriftDuration

	tryExec := func() (int, int64, error) {
		tx, err := w.db.BeginTx(w.ctx, nil)
		if err != nil {
			return 0, 0, errors.Trace(err)
		}

		// Set session variables first and then execute the transaction.
		// we try to set write source for each txn,
		// so we can use it to trace the data source
		if err = SetWriteSource(w.ctx, w.cfg, tx); err != nil {
			log.Error("Failed to set write source", zap.Error(err))
			if rbErr := tx.Rollback(); rbErr != nil {
				if errors.Cause(rbErr) != context.Canceled {
					log.Warn("failed to rollback txn", zap.Error(rbErr))
				}
			}
			return 0, 0, err
		}

		if !fallbackToSeqWay {
			err = w.multiStmtExecute(dmls, tx, writeTimeout)
			if err != nil {
				fallbackToSeqWay = true
				return 0, 0, err
			}
		} else {
			err = w.sequenceExecute(dmls, tx, writeTimeout)
			if err != nil {
				return 0, 0, err
			}
		}

		if err = tx.Commit(); err != nil {
			return 0, 0, err
		}
		log.Debug("Exec Rows succeeded", zap.Any("rowCount", dmls.rowCount))
		return dmls.rowCount, dmls.approximateSize, nil
	}
	return retry.Do(w.ctx, func() error {
		failpoint.Inject("MySQLSinkTxnRandomError", func() {
			log.Warn("inject MySQLSinkTxnRandomError")
			err := errors.Trace(driver.ErrBadConn)
			logDMLTxnErr(err, time.Now(), w.ChangefeedID.String(), dmls.sqls[0], dmls.rowCount, dmls.startTs)
			failpoint.Return(err)
		})

		failpoint.Inject("MySQLSinkHangLongTime", func() { _ = util.Hang(w.ctx, time.Hour) })

		failpoint.Inject("MySQLDuplicateEntryError", func() {
			log.Warn("inject MySQLDuplicateEntryError")
			err := cerror.WrapError(cerror.ErrMySQLDuplicateEntry, &dmysql.MySQLError{
				Number: uint16(mysql.ErrDupEntry),
			})
			logDMLTxnErr(err, time.Now(), w.ChangefeedID.String(), dmls.sqls[0], dmls.rowCount, dmls.startTs)
			failpoint.Return(err)
		})

		err := w.statistics.RecordBatchExecution(tryExec)
		if err != nil {
			logDMLTxnErr(err, time.Now(), w.ChangefeedID.String(), dmls.sqls[0], dmls.rowCount, dmls.startTs)
			return errors.Trace(err)
		}
		return nil
	}, retry.WithBackoffBaseDelay(BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(w.cfg.DMLMaxRetry),
		retry.WithIsRetryableErr(isRetryableDMLError))
}

func (w *Writer) sequenceExecute(
	dmls *preparedDMLs, tx *sql.Tx, writeTimeout time.Duration,
) error {
	for i, query := range dmls.sqls {
		args := dmls.values[i]
		log.Debug("exec row", zap.String("sql", query), zap.Any("args", args))
		ctx, cancelFunc := context.WithTimeout(w.ctx, writeTimeout)

		var prepStmt *sql.Stmt
		if w.cachePrepStmts {
			if stmt, ok := w.stmtCache.Get(query); ok {
				prepStmt = stmt.(*sql.Stmt)
			} else if stmt, err := w.db.Prepare(query); err == nil {
				prepStmt = stmt
				w.stmtCache.Add(query, stmt)
			} else {
				// Generally it means the downstream database doesn't allow
				// too many preapred statements. So clean some of them.
				w.stmtCache.RemoveOldest()
			}
		}

		var execError error
		if prepStmt == nil {
			_, execError = tx.ExecContext(ctx, query, args...)
		} else {
			//nolint:sqlclosecheck
			_, execError = tx.Stmt(prepStmt).ExecContext(ctx, args...)
		}

		if execError != nil {
			log.Error("ExecContext", zap.Error(execError), zap.Any("dmls", dmls))
			if rbErr := tx.Rollback(); rbErr != nil {
				if errors.Cause(rbErr) != context.Canceled {
					log.Warn("failed to rollback txn", zap.Error(rbErr))
				}
			}
			cancelFunc()
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(execError, fmt.Sprintf("Failed to execute DMLs, query info:%s, args:%v; ", query, args)))
		}
		cancelFunc()
	}
	return nil
}

// execute SQLs in the multi statements way.
func (w *Writer) multiStmtExecute(
	dmls *preparedDMLs, tx *sql.Tx, writeTimeout time.Duration,
) error {
	var multiStmtArgs []any
	for _, value := range dmls.values {
		multiStmtArgs = append(multiStmtArgs, value...)
	}
	multiStmtSQL := strings.Join(dmls.sqls, ";")

	ctx, cancel := context.WithTimeout(w.ctx, writeTimeout)
	defer cancel()

	_, err := tx.ExecContext(ctx, multiStmtSQL, multiStmtArgs...)
	if err != nil {
		log.Error("ExecContext", zap.Error(err), zap.Any("multiStmtSQL", multiStmtSQL), zap.Any("multiStmtArgs", multiStmtArgs))
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Warn("failed to rollback txn", zap.Error(rbErr))
			}
		}
		cancel()
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Failed to execute DMLs, query info:%s, args:%v; ", multiStmtSQL, multiStmtArgs)))
	}
	return nil
}

func logDMLTxnErr(
	err error, start time.Time, changefeed string,
	query string, count int, startTs []common.Ts,
) error {
	if len(query) > 1024 {
		query = query[:1024]
	}
	if isRetryableDMLError(err) {
		log.Warn("execute DMLs with error, retry later",
			zap.Error(err), zap.Duration("duration", time.Since(start)),
			zap.String("query", query), zap.Int("count", count),
			zap.Uint64s("startTs", startTs),
			zap.String("changefeed", changefeed))
	} else {
		log.Error("execute DMLs with error, can not retry",
			zap.Error(err), zap.Duration("duration", time.Since(start)),
			zap.String("query", query), zap.Int("count", count),
			zap.String("changefeed", changefeed))
	}
	return errors.WithMessage(err, fmt.Sprintf("Failed query info: %s; ", query))
}

func (w *Writer) batchSingleTxnDmls(
	rows []*commonEvent.RowChange,
	tableInfo *common.TableInfo,
	translateToInsert bool,
) (sqls []string, values [][]interface{}) {
	insertRows, updateRows, deleteRows := w.groupRowsByType(rows, tableInfo)

	// handle delete
	if len(deleteRows) > 0 {
		for _, rows := range deleteRows {
			sql, value := sqlmodel.GenDeleteSQL(rows...)
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
			if translateToInsert {
				sql, value := sqlmodel.GenInsertSQL(sqlmodel.DMLInsert, rows...)
				sqls = append(sqls, sql)
				values = append(values, value)
			} else {
				sql, value := sqlmodel.GenInsertSQL(sqlmodel.DMLReplace, rows...)
				sqls = append(sqls, sql)
				values = append(values, value)
			}
		}
	}

	return
}

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
	// RowChangedEvent doesn't contain data for virtual columns,
	// so we need to create a new table info without virtual columns before pass it to NewRowChange.
	if eventTableInfo.HasVirtualColumns() {
		eventTableInfo = common.BuildTiDBTableInfoWithoutVirtualColumns(eventTableInfo)
	}
	for _, row := range rows {
		switch row.RowType {
		case commonEvent.RowTypeInsert:
			args := getArgs(&row.Row, tableInfo, true)
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
		case commonEvent.RowTypeUpdate:
			args := getArgs(&row.Row, tableInfo, true)
			preArgs := getArgs(&row.PreRow, tableInfo, true)
			newUpdateRow := sqlmodel.NewRowChange(
				&tableInfo.TableName,
				nil,
				preArgs,
				args,
				eventTableInfo,
				nil, nil)
			updateRow = append(updateRow, newUpdateRow)
			if len(updateRow) >= w.cfg.MaxTxnRow {
				updateRows = append(updateRows, updateRow)
				updateRow = make([]*sqlmodel.RowChange, 0, rowSize)
			}
		case commonEvent.RowTypeDelete:
			preArgs := getArgs(&row.PreRow, tableInfo, true)
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

func (w *Writer) genUpdateSQL(rows ...*sqlmodel.RowChange) ([]string, [][]interface{}) {
	size := 0
	for _, r := range rows {
		size += int(r.GetApproximateDataSize())
	}
	if size < w.cfg.MaxMultiUpdateRowSize*len(rows) {
		// use multi update in one SQL
		sql, value := sqlmodel.GenUpdateSQL(rows...)
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
