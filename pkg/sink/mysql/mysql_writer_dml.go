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
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (w *MysqlWriter) prepareDMLs(events []*commonEvent.DMLEvent) (*preparedDMLs, error) {
	dmls := dmlsPool.Get().(*preparedDMLs)
	dmls.reset()

	log.Info("prepareDMLs", zap.Any("events len", len(events)))

	for _, event := range events {
		if event.Len() == 0 {
			log.Info("event len is 0")
			continue
		}

		dmls.rowCount += int(event.Len())
		dmls.approximateSize += event.GetRowsSize()

		if len(dmls.startTs) == 0 || dmls.startTs[len(dmls.startTs)-1] != event.StartTs {
			dmls.startTs = append(dmls.startTs, event.StartTs)
		}

		inSafeMode := !w.cfg.SafeMode && event.CommitTs > event.ReplicatingTs

		log.Debug("inSafeMode",
			zap.Bool("inSafeMode", inSafeMode),
			zap.Uint64("firstRowCommitTs", event.CommitTs),
			zap.Uint64("firstRowReplicatingTs", event.ReplicatingTs),
			zap.Bool("safeMode", w.cfg.SafeMode))

		enableBatchModeThreshold := 1
		// Determine whether to use batch dml feature here.
		if w.cfg.BatchDMLEnable && int(event.Len()) > enableBatchModeThreshold {
			log.Info("into batch dml")
			// only use batch dml when the table has a handle key
			if event.TableInfo.HasHandleKey() {
				sql, value, err := w.batchSingleTxnDmls(event, event.TableInfo, inSafeMode)
				if err != nil {
					dmlsPool.Put(dmls) // Return to pool on error
					return nil, errors.Trace(err)
				}
				dmls.sqls = append(dmls.sqls, sql...)
				dmls.values = append(dmls.values, value...)
				continue
			}
		}

		for {
			row, ok := event.GetNextRow()
			if !ok {
				break
			}

			var query string
			var args []interface{}
			var err error

			switch row.RowType {
			case commonEvent.RowTypeUpdate:
				if inSafeMode {
					query, args, err = buildUpdate(event.TableInfo, row, w.cfg.ForceReplicate)
				} else {
					query, args, err = buildDelete(event.TableInfo, row, w.cfg.ForceReplicate)
					if err != nil {
						dmlsPool.Put(dmls) // Return to pool on error
						return nil, errors.Trace(err)
					}
					if query != "" {
						dmls.sqls = append(dmls.sqls, query)
						dmls.values = append(dmls.values, args)
					}
					query, args, err = buildInsert(event.TableInfo, row, inSafeMode)
				}
			case commonEvent.RowTypeDelete:
				query, args, err = buildDelete(event.TableInfo, row, w.cfg.ForceReplicate)
			case commonEvent.RowTypeInsert:
				query, args, err = buildInsert(event.TableInfo, row, inSafeMode)
			}

			if err != nil {
				dmlsPool.Put(dmls) // Return to pool on error
				return nil, errors.Trace(err)
			}

			if query != "" {
				dmls.sqls = append(dmls.sqls, query)
				dmls.values = append(dmls.values, args)
			}
		}
	}

	// Pre-check log level to avoid dmls.String() being called unnecessarily
	// This method is expensive, so we only log it when the log level is debug.
	if log.GetLevel() == zapcore.DebugLevel {
		log.Debug("prepareDMLs", zap.Any("dmls", dmls.String()), zap.Any("events", events))
	}

	return dmls, nil
}

func (w *MysqlWriter) execDMLWithMaxRetries(dmls *preparedDMLs) error {
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
		if err = SetWriteSource(w.cfg, tx); err != nil {
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
		log.Debug("Exec Rows succeeded", zap.Any("row count", dmls.rowCount), zap.Any("approximate size", dmls.approximateSize))
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
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(w.cfg.DMLMaxRetry),
		retry.WithIsRetryableErr(isRetryableDMLError))
}

func (w *MysqlWriter) sequenceExecute(
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
func (w *MysqlWriter) multiStmtExecute(
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

func (w *MysqlWriter) batchSingleTxnDmls(
	event *commonEvent.DMLEvent,
	tableInfo *common.TableInfo,
	translateToInsert bool,
) (sqls []string, values [][]interface{}, err error) {
	insertRows, updateRows, deleteRows, err := w.groupRowsByType(event, tableInfo)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	log.Info("batchSingleTxnDmls", zap.Any("len(insertRows)", len(insertRows)), zap.Any("len(updateRows)", len(updateRows)), zap.Any("len(deleteRows)", len(deleteRows)), zap.Any("event len", event.Len()))

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

func (w *MysqlWriter) groupRowsByType(
	event *commonEvent.DMLEvent,
	tableInfo *common.TableInfo,
) (insertRows, updateRows, deleteRows [][]*sqlmodel.RowChange, err error) {
	rowSize := int(event.Len())
	if rowSize > w.cfg.MaxTxnRow {
		rowSize = w.cfg.MaxTxnRow
	}

	insertRow := make([]*sqlmodel.RowChange, 0, rowSize)
	updateRow := make([]*sqlmodel.RowChange, 0, rowSize)
	deleteRow := make([]*sqlmodel.RowChange, 0, rowSize)

	eventTableInfo := tableInfo
	log.Info("eventTableInfo", zap.Any("eventTableInfo columnschema", eventTableInfo.GetColumnSchema()))
	// RowChangedEvent doesn't contain data for virtual columns,
	// so we need to create a new table info without virtual columns before pass it to NewRowChange.
	if eventTableInfo.HasVirtualColumns() {
		eventTableInfo = common.BuildTiDBTableInfoWithoutVirtualColumns(eventTableInfo)
	}
	for {
		row, ok := event.GetNextRow()
		if !ok {
			break
		}

		log.Info("next row", zap.Any("type", row.RowType))
		switch row.RowType {
		case commonEvent.RowTypeInsert:
			args, err := getArgs(&row.Row, tableInfo, true)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}

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
			args, err := getArgs(&row.Row, tableInfo, true)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
			preArgs, err := getArgs(&row.PreRow, tableInfo, true)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
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
			preArgs, err := getArgs(&row.PreRow, tableInfo, true)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
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

func (w *MysqlWriter) genUpdateSQL(rows ...*sqlmodel.RowChange) ([]string, [][]interface{}) {
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
