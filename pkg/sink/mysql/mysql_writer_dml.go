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
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (w *MysqlWriter) prepareDMLs(events []*commonEvent.DMLEvent) (*preparedDMLs, error) {
	dmls := dmlsPool.Get().(*preparedDMLs)
	dmls.reset()

	for _, event := range events {
		if event.Len() == 0 {
			continue
		}

		dmls.rowCount += int(event.Len())
		dmls.approximateSize += event.GetRowsSize()

		if len(dmls.startTs) == 0 || dmls.startTs[len(dmls.startTs)-1] != event.StartTs {
			dmls.startTs = append(dmls.startTs, event.StartTs)
		}

		translateToInsert := !w.cfg.SafeMode && event.CommitTs > event.ReplicatingTs
		log.Debug("translate to insert",
			zap.Bool("translateToInsert", translateToInsert),
			zap.Uint64("firstRowCommitTs", event.CommitTs),
			zap.Uint64("firstRowReplicatingTs", event.ReplicatingTs),
			zap.Bool("safeMode", w.cfg.SafeMode))

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
				if translateToInsert {
					query, args, err = buildUpdate(event.TableInfo, row)
				} else {
					query, args, err = buildDelete(event.TableInfo, row)
					if err != nil {
						dmlsPool.Put(dmls) // Return to pool on error
						return nil, errors.Trace(err)
					}
					if query != "" {
						dmls.sqls = append(dmls.sqls, query)
						dmls.values = append(dmls.values, args)
					}
					query, args, err = buildInsert(event.TableInfo, row, translateToInsert)
				}
			case commonEvent.RowTypeDelete:
				query, args, err = buildDelete(event.TableInfo, row)
			case commonEvent.RowTypeInsert:
				query, args, err = buildInsert(event.TableInfo, row, translateToInsert)
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
		log.Debug("Exec Rows succeeded")
		return dmls.rowCount, dmls.approximateSize, nil
	}
	return retry.Do(w.ctx, func() error {
		err := w.statistics.RecordBatchExecution(tryExec)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(w.cfg.DMLMaxRetry))
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
