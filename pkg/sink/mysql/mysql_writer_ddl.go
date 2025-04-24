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

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

func (w *Writer) execDDL(event *commonEvent.DDLEvent) error {
	if w.cfg.DryRun {
		log.Info("Dry run DDL", zap.String("sql", event.GetDDLQuery()))
		time.Sleep(w.cfg.DryRunDelay)
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
	if err = SetWriteSource(ctx, w.cfg, tx); err != nil {
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
		log.Error("Fail to ExecContext", zap.Any("err", err), zap.Any("query", query))
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("sql", event.GetDDLQuery()), zap.Error(err))
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Query info: %s; ", event.GetDDLQuery())))
	}

	return nil
}

// execDDLWithMaxRetries will retry executing DDL statements.
// When a DDL execution takes a long time and an invalid connection error occurs.
// If the downstream is TiDB, it will query the DDL and wait until it finishes.
// For 'add index' ddl, it will return immediately without waiting and will query it during the next DDL execution.
func (w *Writer) execDDLWithMaxRetries(event *commonEvent.DDLEvent) error {
	ddlCreateTime := getDDLCreateTime(w.ctx, w.db)
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
			if w.cfg.IsTiDB && ddlCreateTime != "" && errors.Cause(err) == mysql.ErrInvalidConn {
				log.Warn("Wait the asynchronous ddl to synchronize", zap.String("ddl", event.Query), zap.String("ddlCreateTime", ddlCreateTime),
					zap.String("readTimeout", w.cfg.ReadTimeout), zap.Error(err))
				return w.waitDDLDone(w.ctx, event, ddlCreateTime)
			}
			log.Warn("Execute DDL with error, retry later",
				zap.String("ddl", event.Query),
				zap.Error(err))
			return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Execute DDL failed, Query info: %s; ", event.GetDDLQuery())))
		}
		log.Info("Execute DDL succeeded",
			zap.String("changefeed", w.ChangefeedID.String()),
			zap.Any("ddl", event))
		return nil
	}, retry.WithBackoffBaseDelay(BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(defaultDDLMaxRetry),
		retry.WithIsRetryableErr(apperror.IsRetryableDDLError))
}

// waitDDLDone wait current ddl
func (w *Writer) waitDDLDone(ctx context.Context, ddl *commonEvent.DDLEvent, ddlCreateTime string) error {
	ticker := time.NewTicker(5 * time.Second)
	ticker1 := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	defer ticker1.Stop()
	for {
		state, err := getDDLStateFromTiDB(ctx, w.db, ddl.Query, ddlCreateTime)
		if err != nil {
			log.Error("Error when getting DDL state from TiDB", zap.Error(err))
		}
		switch state {
		case timodel.JobStateDone, timodel.JobStateSynced:
			log.Info("DDL replicate success", zap.String("ddl", ddl.Query), zap.String("ddlCreateTime", ddlCreateTime))
			return nil
		case timodel.JobStateCancelled, timodel.JobStateRollingback, timodel.JobStateRollbackDone, timodel.JobStateCancelling:
			return errors.ErrExecDDLFailed.GenWithStackByArgs(ddl.Query)
		case timodel.JobStateRunning, timodel.JobStateQueueing:
			switch ddl.GetDDLType() {
			// returned immediately if not block dml
			case timodel.ActionAddIndex:
				log.Info("DDL is running downstream", zap.String("ddl", ddl.Query), zap.String("ddlCreateTime", ddlCreateTime), zap.Any("ddlState", state))
				return nil
			}
		default:
			log.Warn("Unexpected DDL state, may not be found downstream", zap.String("ddl", ddl.Query), zap.String("ddlCreateTime", ddlCreateTime), zap.Any("ddlState", state))
			return errors.ErrDDLStateNotFound.GenWithStackByArgs(state)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-ticker1.C:
			log.Info("DDL is still running downstream, it blocks other DDL or DML events", zap.String("ddl", ddl.Query), zap.String("ddlCreateTime", ddlCreateTime))
		}
	}
}

// waitAsyncDDLDone wait previous ddl
func (w *Writer) waitAsyncDDLDone(event *commonEvent.DDLEvent) {
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
		// tableID 0 means table trigger, which can't do async ddl
		if tableID == 0 {
			continue
		}
		// query the downstream,
		// if the ddl is still running, we should wait for it.
		err := w.checkAndWaitAsyncDDLDoneDownstream(tableID)
		if err != nil {
			log.Error("check previous asynchronous ddl failed",
				zap.String("namespace", w.ChangefeedID.Namespace()),
				zap.Stringer("changefeed", w.ChangefeedID),
				zap.Error(err))
		}
	}
}

// true means the async ddl is still running, false means the async ddl is done.
func (w *Writer) doQueryAsyncDDL(tableID int64, query string) (bool, error) {
	start := time.Now()
	rows, err := w.db.QueryContext(w.ctx, query)
	log.Debug("query duration", zap.Any("duration", time.Since(start)), zap.Any("query", query))
	if err != nil {
		return false, errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to query ddl jobs table; Query is %s", query)))
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
			return false, errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to query ddl jobs table; Query is %s", query)))
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
		return false, nil
	}

	return true, nil
}

// query the ddl jobs to find the state of the async ddl
// if the ddl is still running, we should wait for it.
func (w *Writer) checkAndWaitAsyncDDLDoneDownstream(tableID int64) error {
	query := fmt.Sprintf(checkRunningAddIndexSQL, tableID)
	running, err := w.doQueryAsyncDDL(tableID, query)
	if err != nil {
		return err
	}
	if !running {
		return nil
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return nil
		case <-ticker.C:
			running, err = w.doQueryAsyncDDL(tableID, query)
			if err != nil {
				return err
			}
			if !running {
				return nil
			}
		}
	}
}
