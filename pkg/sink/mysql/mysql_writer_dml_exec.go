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
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"go.uber.org/zap"
)

// execDMLWithMaxRetries executes prepared DMLs with retry/backoff handling.
func (w *Writer) execDMLWithMaxRetries(dmls *preparedDMLs) error {
	if len(dmls.sqls) != len(dmls.values) {
		return cerror.ErrUnexpected.FastGenByArgs(fmt.Sprintf("unexpected number of sqls and values, sqls is %s, values is %s", dmls.sqls, util.RedactAny(dmls.values)))
	}

	// approximateSize is multiplied by 2 because in extreme circustumas, every
	// byte in dmls can be escaped and adds one byte.
	fallbackToSeqWay := dmls.approximateSize*2 > w.cfg.MaxAllowedPacket

	writeTimeout, _ := time.ParseDuration(w.cfg.WriteTimeout)
	writeTimeout += networkDriftDuration

	tryExec := func() (int, int64, error) {
		start := time.Now()
		defer func() {
			if time.Since(start) > w.cfg.SlowQuery {
				log.Info("Slow Query", zap.Any("sql", dmls.LogWithoutValues()), zap.Any("writerID", w.id))
			}
		}()
		err := w.dmlSession.withConn(w, writeTimeout, func(conn *sql.Conn) error {
			if fallbackToSeqWay || !w.cfg.MultiStmtEnable {
				// use sequence way to execute the dmls
				tx, err := conn.BeginTx(w.ctx, nil)
				if err != nil {
					return errors.Trace(err)
				}

				err = w.sequenceExecute(dmls, tx, writeTimeout)
				if err != nil {
					return err
				}

				if err = tx.Commit(); err != nil {
					return err
				}

				log.Debug("Exec Rows succeeded", zap.Any("rowCount", dmls.rowCount), zap.Int("writerID", w.id))
				return nil
			}

			// use multi stmt way to execute the dmls
			if err := w.multiStmtExecute(conn, dmls, writeTimeout); err != nil {
				log.Warn("multiStmtExecute failed, fallback to sequence way",
					zap.Error(err),
					zap.Any("sql", dmls.LogWithoutValues()),
					zap.Int("writerID", w.id))
				fallbackToSeqWay = true
				return err
			}
			return nil
		})
		if err != nil {
			return 0, 0, err
		}
		return dmls.rowCount, dmls.approximateSize, nil
	}
	return retry.Do(w.ctx, func() error {
		failpoint.Inject("MySQLSinkTxnRandomError", func() {
			log.Warn("inject MySQLSinkTxnRandomError")
			err := errors.Trace(driver.ErrBadConn)
			w.logDMLTxnErr(err, time.Now(), w.ChangefeedID.String(), dmls)
			failpoint.Return(err)
		})

		failpoint.Inject("MySQLSinkHangLongTime", func() { _ = util.Hang(w.ctx, time.Hour) })

		failpoint.Inject("MySQLDuplicateEntryError", func() {
			log.Warn("inject MySQLDuplicateEntryError")
			err := cerror.WrapError(cerror.ErrMySQLDuplicateEntry, &dmysql.MySQLError{
				Number:  uint16(mysql.ErrDupEntry),
				Message: "Duplicate entry",
			})
			w.logDMLTxnErr(err, time.Now(), w.ChangefeedID.String(), dmls)
			failpoint.Return(err)
		})

		err := w.statistics.RecordBatchExecution(tryExec)
		if err != nil {
			w.logDMLTxnErr(err, time.Now(), w.ChangefeedID.String(), dmls)
			return errors.Trace(err)
		}
		return nil
	}, retry.WithBackoffBaseDelay(BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(w.cfg.DMLMaxRetry),
		retry.WithIsRetryableErr(isRetryableDMLError))
}

// sequenceExecute runs each SQL sequentially inside a transaction.
func (w *Writer) sequenceExecute(
	dmls *preparedDMLs, tx *sql.Tx, writeTimeout time.Duration,
) error {
	for i, query := range dmls.sqls {
		args := dmls.values[i]
		log.Debug("exec row", zap.String("sql", query), zap.String("args", util.RedactArgs(args)), zap.Int("writerID", w.id))
		ctx, cancelFunc := context.WithTimeout(w.ctx, writeTimeout)

		var prepStmt *sql.Stmt
		if w.cfg.CachePrepStmts {
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

		var (
			res       sql.Result
			execError error
		)
		if prepStmt == nil {
			res, execError = tx.ExecContext(ctx, query, args...)
		} else {
			//nolint:sqlclosecheck
			res, execError = tx.Stmt(prepStmt).ExecContext(ctx, args...)
		}

		if execError != nil {
			log.Error("ExecContext", zap.Error(execError), zap.Any("dmls", dmls), zap.Int("writerID", w.id))
			if rbErr := tx.Rollback(); rbErr != nil {
				if errors.Cause(rbErr) != context.Canceled {
					log.Warn("failed to rollback txn", zap.Error(rbErr), zap.Int("writerID", w.id))
				}
			}
			cancelFunc()
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(execError, fmt.Sprintf("Failed to execute DMLs, query info:%s, args:%v; ", query, util.RedactArgs(args))))
		}
		if rowsAffected, err := res.RowsAffected(); err != nil {
			log.Warn("get rows affected rows failed", zap.Error(err))
		} else {
			w.statistics.RecordRowsAffected(rowsAffected, dmls.rowTypes[i])
		}
		cancelFunc()
	}
	return nil
}

// multiStmtExecute runs SQLs using the multi-statements protocol with an implicit transaction.
func (w *Writer) multiStmtExecute(
	conn *sql.Conn,
	dmls *preparedDMLs, writeTimeout time.Duration,
) error {
	var multiStmtArgs []any
	for _, value := range dmls.values {
		multiStmtArgs = append(multiStmtArgs, value...)
	}
	multiStmtSQL := strings.Join(dmls.sqls, ";")
	// we use BEGIN and COMMIT to ensure the transaction is atomic.
	multiStmtSQLWithTxn := "BEGIN;" + multiStmtSQL + ";COMMIT;"

	ctx, cancel := context.WithTimeout(w.ctx, writeTimeout)
	defer cancel()

	// we use conn.ExecContext to reduce the overhead of network latency.
	// conn.ExecContext only use one RTT, while db.Begin + tx.ExecContext + db.Commit need three RTTs.
	// When an error happens before COMMIT, the server session can be left with an open transaction.
	// Best-effort rollback is required to ensure the connection can be safely reused by the pool.
	res, err := conn.ExecContext(ctx, multiStmtSQLWithTxn, multiStmtArgs...)
	if err != nil {
		rbCtx, rbCancel := context.WithTimeout(w.ctx, writeTimeout)
		_, rbErr := conn.ExecContext(rbCtx, "ROLLBACK")
		rbCancel()
		if rbErr != nil {
			log.Info("failed to rollback after multi statement exec error",
				zap.Int("writerID", w.id),
				zap.Error(rbErr))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Failed to execute DMLs, query info:%s, args:%v; ", multiStmtSQLWithTxn, util.RedactArgs(multiStmtArgs))))
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		log.Warn("get rows affected rows failed", zap.Error(err))
	} else {
		w.statistics.RecordTotalRowsAffected(rowsAffected, int64(len(multiStmtArgs)))
	}
	return nil
}

// logDMLTxnErr prints retryable/irretryable errors with contextual information.
func (w *Writer) logDMLTxnErr(
	err error, start time.Time, changefeed string,
	dmls *preparedDMLs,
) error {
	if isRetryableDMLError(err) {
		log.Warn("execute DMLs with error, retry later",
			zap.String("changefeed", changefeed),
			zap.Duration("duration", time.Since(start)),
			zap.Any("tsPairs", dmls.tsPairs),
			zap.Int("count", dmls.rowCount),
			zap.String("dmls", dmls.String()),
			zap.Int("writerID", w.id),
			zap.Error(err))
	} else {
		if !w.checkIsDuplicateEntryError(err) {
			log.Error("execute DMLs with error, can not retry",
				zap.String("changefeed", changefeed),
				zap.Duration("duration", time.Since(start)),
				zap.Any("tsPairs", dmls.tsPairs),
				zap.Int("count", dmls.rowCount),
				zap.String("dmls", dmls.String()),
				zap.Int("writerID", w.id),
				zap.Error(err))
		}
	}
	return errors.WithMessage(err, fmt.Sprintf("Failed query info: %s; ", dmls.String()))
}
