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
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"go.uber.org/zap"
)

func (w *MysqlWriter) asyncExecAddIndexDDLIfTimeout(event *commonEvent.DDLEvent) error {
	var tableIDs []int64
	switch event.GetBlockedTables().InfluenceType {
	// only normal type may have ddl need to async exec
	case commonEvent.InfluenceTypeNormal:
		tableIDs = event.GetBlockedTables().TableIDs
	}

	for _, tableID := range tableIDs {
		// change the async ddl state to 1, means the tableID have async table ddl executing
		w.asyncDDLState.Store(tableID, 1)
	}

	done := make(chan error, 1)
	// wait for 2 seconds at most
	tick := time.NewTimer(2 * time.Second)
	defer tick.Stop()
	log.Info("async exec add index ddl start",
		zap.Uint64("commitTs", event.FinishedTs),
		zap.String("ddl", event.GetDDLQuery()))
	go func() {
		if err := w.execDDLWithMaxRetries(event); err != nil {
			log.Error("async exec add index ddl failed",
				zap.Uint64("commitTs", event.FinishedTs),
				zap.String("ddl", event.GetDDLQuery()))
			done <- err
			return
		}
		log.Info("async exec add index ddl done",
			zap.Uint64("commitTs", event.FinishedTs),
			zap.String("ddl", event.GetDDLQuery()))
		done <- nil

		for _, tableID := range tableIDs {
			// change the async ddl state to 2, means the tableID don't have async table ddl executing
			w.asyncDDLState.Store(tableID, 2)
		}
	}()

	select {
	case err := <-done:
		// if the ddl is executed within 2 seconds, we just return the result to the caller.
		return err
	case <-tick.C:
		// if the ddl is still running, we just return nil,
		// then if the ddl is failed, the downstream ddl is lost.
		// because the checkpoint ts is forwarded.
		log.Info("async add index ddl is still running",
			zap.Uint64("commitTs", event.FinishedTs),
			zap.String("ddl", event.GetDDLQuery()))
		return nil
	}
}

func needAsyncExecDDL(ddlType timodel.ActionType) bool {
	switch ddlType {
	case timodel.ActionAddIndex:
		return true
	default:
		return false
	}
}

func needTimeoutCheck(ddlType timodel.ActionType) bool {
	if needAsyncExecDDL(ddlType) {
		return false
	}
	switch ddlType {
	// partition related
	case timodel.ActionAddTablePartition, timodel.ActionExchangeTablePartition, timodel.ActionReorganizePartition:
		return false
	// reorg related
	case timodel.ActionAddPrimaryKey, timodel.ActionAddIndex, timodel.ActionModifyColumn:
		return false
	// following ddls can be fast when the downstream is TiDB, we must
	// still take them into consideration to ensure compatibility with all
	// MySQL-compatible databases.
	case timodel.ActionAddColumn, timodel.ActionAddColumns, timodel.ActionDropColumn, timodel.ActionDropColumns:
		return false
	default:
		return true
	}
}

func (w *MysqlWriter) execDDL(event *commonEvent.DDLEvent) error {
	if w.cfg.DryRun {
		log.Info("Dry run DDL", zap.String("sql", event.GetDDLQuery()))
		return nil
	}

	// exchange partition is not Idempotent, so we need to check ddl_ts_table whether the ddl is executed before.
	if timodel.ActionType(event.Type) == timodel.ActionExchangeTablePartition {
		tableID := event.BlockedTables.TableIDs[0]
		ddlTs := event.GetCommitTs()
		flag, err := w.isDDLExecuted(tableID, ddlTs)
		if err != nil {
			return nil
		}
		if flag {
			log.Info("Skip Already Executed DDL", zap.String("sql", event.GetDDLQuery()))
			return nil
		}
	}

	ctx := w.ctx
	// We check the most of the ddl event executed with timeout.
	if !needTimeoutCheck(event.GetDDLType()) {
		writeTimeout, _ := time.ParseDuration(w.cfg.WriteTimeout)
		writeTimeout += networkDriftDuration
		var cancelFunc func()
		ctx, cancelFunc = context.WithTimeout(w.ctx, writeTimeout)
		defer cancelFunc()
	}

	shouldSwitchDB := needSwitchDB(event)
	// Convert vector type to string type for unsupport database
	if w.needFormat {
		if newQuery := formatQuery(event.Query); newQuery != event.Query {
			log.Warn("format ddl query", zap.String("newQuery", newQuery), zap.String("query", event.Query))
			event.Query = newQuery
		}
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+common.QuoteName(event.GetDDLSchemaName())+";")
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.Error(err))
			}
			return err
		}
	}

	// we try to set cdc write source for the ddl
	if err = SetWriteSource(w.cfg, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Error("Failed to rollback", zap.Error(err))
			}
		}
		return err
	}

	query := event.GetDDLQuery()
	_, err = tx.ExecContext(ctx, query)
	if err != nil {
		log.Error("Fail to ExecContext", zap.Any("err", err))
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("sql", event.GetDDLQuery()), zap.Error(err))
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Query info: %s; ", event.GetDDLQuery())))
	}

	return nil
}

func (w *MysqlWriter) execDDLWithMaxRetries(event *commonEvent.DDLEvent) error {
	return retry.Do(w.ctx, func() error {
		err := w.statistics.RecordDDLExecution(func() error { return w.execDDL(event) })
		if err != nil {
			if apperror.IsIgnorableMySQLDDLError(err) {
				// NOTE: don't change the log, some tests depend on it.
				log.Info("Execute DDL failed, but error can be ignored",
					zap.String("ddl", event.Query),
					zap.Error(err))
				// If the error is ignorable, we will ignore the error directly.
				return nil
			}
			log.Warn("Execute DDL with error, retry later",
				zap.String("ddl", event.Query),
				zap.Error(err))
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Execute DDL failed, Query info: %s; ", event.GetDDLQuery())))
		}
		log.Info("Execute DDL succeeded",
			zap.String("changefeed", w.ChangefeedID.String()),
			zap.Any("ddl", event))
		return nil
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(defaultDDLMaxRetry),
		retry.WithIsRetryableErr(apperror.IsRetryableDDLError))
}

func (w *MysqlWriter) waitAsyncDDLDone(event *commonEvent.DDLEvent) {
	if !needWaitAsyncExecDone(event.GetDDLType()) {
		return
	}

	var relatedTableIDs []int64
	switch event.GetBlockedTables().InfluenceType {
	case commonEvent.InfluenceTypeNormal:
		relatedTableIDs = event.GetBlockedTables().TableIDs
	// db-class, all-class ddl with not affect by async ddl, just return
	case commonEvent.InfluenceTypeDB, commonEvent.InfluenceTypeAll:
		return
	}

	for _, tableID := range relatedTableIDs {
		state, ok := w.asyncDDLState.Load(tableID)
		if !ok {
			// query the downstream,
			// if the ddl is still running, we should wait for it.
			w.checkAndWaitAsyncDDLDoneDownstream(tableID)
			// update async ddl state
			w.asyncDDLState.Store(tableID, 2)
			// TODO
		} else if state.(int) == 1 {
			w.waitTableAsyncDDLDone(tableID)
		}
	}
}

func (w *MysqlWriter) waitTableAsyncDDLDone(tableID int64) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			state, ok := w.asyncDDLState.Load(tableID)
			if ok && state.(int) == 2 {
				return
			}
		}
	}
}

var checkRunningAddIndexSQL = `
SELECT JOB_ID, JOB_TYPE, SCHEMA_STATE, SCHEMA_ID, TABLE_ID, STATE, QUERY
FROM information_schema.ddl_jobs
WHERE TABLE_ID = "%s"
    AND JOB_TYPE LIKE "add index%%"
    AND (STATE = "running" OR STATE = "queueing");
`

// query the ddl jobs to find the state of the async ddl
// if the ddl is still running, we should wait for it.
func (w *MysqlWriter) checkAndWaitAsyncDDLDoneDownstream(tableID int64) error {
	query := fmt.Sprintf(checkRunningAddIndexSQL, tableID)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return nil
		case <-ticker.C:
			start := time.Now()
			rows, err := w.db.QueryContext(w.ctx, query)
			if err != nil {
				return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to query ddl jobs table; Query is %s", query)))
			}

			defer rows.Close()
			var jobID int64
			var jobType string
			var schemaState string
			var state string

			noRows := true
			for rows.Next() {
				noRows = false
				err := rows.Scan(&jobID, &jobType, &schemaState, &state)
				if err != nil {
					return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to query ddl jobs table; Query is %s", query)))
				}

				log.Info("async ddl is still running",
					zap.String("changefeed", w.ChangefeedID.String()),
					zap.Duration("checkDuration", time.Since(start)),
					zap.Any("tableID", tableID),
					zap.Any("jobID", jobID),
					zap.String("jobType", jobType),
					zap.String("schemaState", schemaState),
					zap.String("state", state))
				break
			}

			if noRows {
				return nil
			}
		}
	}
}

func needWaitAsyncExecDone(t timodel.ActionType) bool {
	switch t {
	case timodel.ActionCreateTable, timodel.ActionCreateTables:
		return false
	case timodel.ActionCreateSchema:
		return false
	default:
		return true
	}
}
