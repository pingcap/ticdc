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
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/tidb/dumpling/export"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	tidbmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

func (w *Writer) execDDL(event *commonEvent.DDLEvent) error {
	if w.cfg.DryRun {
		log.Info("Dry run DDL", zap.String("sql", event.GetDDLQuery()))
		time.Sleep(w.cfg.DryRunDelay)
		return nil
	}

	// exchange partition is not Idempotent, so we need to check ddl_ts_table whether the ddl is executed before.
	if timodel.ActionType(event.Type) == timodel.ActionExchangeTablePartition && w.cfg.EnableDDLTs {
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
	if w.cfg.HasVectorType {
		if newQuery := formatQuery(event.Query); newQuery != event.Query {
			log.Info("format ddl query", zap.String("newQuery", newQuery), zap.String("query", event.Query))
			event.Query = newQuery
		}
	}

	failpoint.Inject("MySQLSinkExecDDLDelay", func(val failpoint.Value) {
		delay := time.Hour
		if seconds, ok := val.(string); ok && seconds != "" {
			if v, err := strconv.Atoi(strings.TrimSpace(seconds)); err == nil && v > 0 {
				delay = time.Duration(v) * time.Second
			}
		}
		select {
		case <-ctx.Done():
			failpoint.Return(ctx.Err())
		case <-time.After(delay):
		}
	})

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+common.QuoteName(event.GetDDLSchemaName())+";")
		if err != nil {
			if tsErr := resetSessionTimestamp(ctx, tx); tsErr != nil {
				log.Warn("Failed to reset session timestamp after USE failure", zap.Error(tsErr))
			}
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.Error(err))
			}
			return err
		}
	}

	// Each DDL statement should use its own StartTs timestamp from upstream
	// This ensures that timestamp functions like NOW(), CURRENT_TIMESTAMP(), LOCALTIME()
	// are evaluated with the exact same temporal context as upstream
	tsToUse := event.GetStartTs()
	if tsToUse == 0 {
		// If StartTs is not available, fall back to CommitTs
		// This preserves timestamp consistency for DDL statements executed in the same transaction
		tsToUse = event.GetCommitTs()
	}
	ddlTime := oracle.GetTimeFromTS(tsToUse)
	// Use second-level precision to match upstream default value evaluation.
	ddlTimestamp := float64(ddlTime.Unix())

	if ts, ok := ddlSessionTimestampFromOriginDefault(event, w.cfg.Timezone); ok {
		ddlTimestamp = ts
	}

	// Set the session timestamp to match upstream DDL execution time
	// This is critical for preserving timestamp function behavior
	if err := setSessionTimestamp(ctx, tx, ddlTimestamp); err != nil {
		log.Error("Fail to set session timestamp for DDL",
			zap.Float64("timestamp", ddlTimestamp),
			zap.Uint64("startTs", tsToUse),
			zap.Uint64("commitTs", event.GetCommitTs()),
			zap.String("query", event.GetDDLQuery()),
			zap.Error(err))
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.Error(err))
		}
		return err
	}

	query := event.GetDDLQuery()
	_, err = tx.ExecContext(ctx, query)
	if err != nil {
		log.Error("Fail to ExecContext", zap.Any("err", err), zap.Any("query", query))
		if tsErr := resetSessionTimestamp(ctx, tx); tsErr != nil {
			log.Warn("Failed to reset session timestamp after DDL execution failure", zap.Error(tsErr))
		}
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("sql", event.GetDDLQuery()), zap.Error(err))
		}
		return err
	}

	// Reset session timestamp after DDL execution to avoid affecting subsequent operations
	if err := resetSessionTimestamp(ctx, tx); err != nil {
		log.Error("Failed to reset session timestamp after DDL execution", zap.Error(err))
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("sql", event.GetDDLQuery()), zap.Error(err))
		}
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Query info: %s; ", event.GetDDLQuery())))
	}

	if err = tx.Commit(); err != nil {
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Query info: %s; ", event.GetDDLQuery())))
	}

	// Log successful DDL execution with timestamp information for debugging
	log.Debug("DDL executed with timestamp",
		zap.String("query", event.GetDDLQuery()),
		zap.Uint64("startTs", tsToUse),
		zap.Float64("sessionTimestamp", ddlTimestamp))

	return nil
}

func setSessionTimestamp(ctx context.Context, tx *sql.Tx, unixTimestamp float64) error {
	_, err := tx.ExecContext(ctx, fmt.Sprintf("SET TIMESTAMP = %s", formatUnixTimestamp(unixTimestamp)))
	return err
}

func resetSessionTimestamp(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "SET TIMESTAMP = DEFAULT")
	return err
}

func formatUnixTimestamp(unixTimestamp float64) string {
	return strconv.FormatFloat(unixTimestamp, 'f', 6, 64)
}

func ddlSessionTimestampFromOriginDefault(event *commonEvent.DDLEvent, timezone string) (float64, bool) {
	if event == nil || event.TableInfo == nil {
		return 0, false
	}
	targetColumns, err := extractCurrentTimestampDefaultColumns(event.GetDDLQuery())
	if err != nil || len(targetColumns) == 0 {
		return 0, false
	}

	for _, col := range event.TableInfo.GetColumns() {
		if _, ok := targetColumns[col.Name.L]; !ok {
			continue
		}
		val := col.GetOriginDefaultValue()
		valStr, ok := val.(string)
		if !ok || valStr == "" {
			continue
		}
		ts, err := parseOriginDefaultTimestamp(valStr, col, timezone)
		if err != nil {
			log.Warn("Failed to parse OriginDefaultValue for DDL timestamp",
				zap.String("column", col.Name.O),
				zap.String("originDefault", valStr),
				zap.Error(err))
			continue
		}
		log.Info("Using OriginDefaultValue for DDL timestamp",
			zap.String("column", col.Name.O),
			zap.String("originDefault", valStr),
			zap.Float64("timestamp", ts),
			zap.String("timezone", timezone))
		return ts, true
	}

	return 0, false
}

func extractCurrentTimestampDefaultColumns(query string) (map[string]struct{}, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return nil, err
	}

	cols := make(map[string]struct{})
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		for _, col := range s.Cols {
			if hasCurrentTimestampDefault(col) {
				cols[col.Name.Name.L] = struct{}{}
			}
		}
	case *ast.AlterTableStmt:
		for _, spec := range s.Specs {
			switch spec.Tp {
			case ast.AlterTableAddColumns, ast.AlterTableModifyColumn, ast.AlterTableChangeColumn, ast.AlterTableAlterColumn:
				for _, col := range spec.NewColumns {
					if hasCurrentTimestampDefault(col) {
						cols[col.Name.Name.L] = struct{}{}
					}
				}
			}
		}
	}

	return cols, nil
}

func hasCurrentTimestampDefault(col *ast.ColumnDef) bool {
	if col == nil {
		return false
	}
	for _, opt := range col.Options {
		if opt.Tp != ast.ColumnOptionDefaultValue {
			continue
		}
		if isCurrentTimestampExpr(opt.Expr) {
			return true
		}
	}
	return false
}

func isCurrentTimestampExpr(expr ast.ExprNode) bool {
	if expr == nil {
		return false
	}
	switch v := expr.(type) {
	case *ast.FuncCallExpr:
		return isCurrentTimestampFuncName(v.FnName.L)
	case ast.ValueExpr:
		return isCurrentTimestampFuncName(strings.ToLower(v.GetString()))
	default:
		return false
	}
}

func isCurrentTimestampFuncName(name string) bool {
	switch name {
	case ast.CurrentTimestamp, ast.Now, ast.LocalTime, ast.LocalTimestamp:
		return true
	default:
		return false
	}
}

func parseOriginDefaultTimestamp(val string, col *timodel.ColumnInfo, timezone string) (float64, error) {
	loc, err := resolveOriginDefaultLocation(col, timezone)
	if err != nil {
		return 0, err
	}
	return parseTimestampInLocation(val, loc)
}

func resolveOriginDefaultLocation(col *timodel.ColumnInfo, timezone string) (*time.Location, error) {
	if col != nil && col.GetType() == tidbmysql.TypeTimestamp && col.Version >= timodel.ColumnInfoVersion1 {
		return time.UTC, nil
	}
	if timezone == "" {
		return time.UTC, nil
	}
	tz := strings.Trim(timezone, "\"")
	return time.LoadLocation(tz)
}

func parseTimestampInLocation(val string, loc *time.Location) (float64, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999",
	}
	for _, f := range formats {
		t, err := time.ParseInLocation(f, val, loc)
		if err == nil {
			return float64(t.UnixNano()) / float64(time.Second), nil
		}
	}
	return 0, fmt.Errorf("failed to parse timestamp: %s", val)
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
			if errors.IsIgnorableMySQLDDLError(err) {
				// NOTE: don't change the log, some tests depend on it.
				log.Info("Execute DDL failed, but error can be ignored",
					zap.String("ddl", event.Query),
					zap.Uint64("startTs", event.GetStartTs()), zap.Uint64("commitTs", event.GetCommitTs()),
					zap.Error(err))
				// If the error is ignorable, we will ignore the error directly.
				return nil
			}
			if w.cfg.IsTiDB && ddlCreateTime != "" && errors.Cause(err) == mysql.ErrInvalidConn {
				log.Warn("Wait the asynchronous ddl to synchronize", zap.String("ddl", event.Query), zap.String("ddlCreateTime", ddlCreateTime),
					zap.Uint64("startTs", event.GetStartTs()), zap.Uint64("commitTs", event.GetCommitTs()),
					zap.String("readTimeout", w.cfg.ReadTimeout), zap.Error(err))
				return w.waitDDLDone(w.ctx, event, ddlCreateTime)
			}
			log.Warn("Execute DDL with error, retry later",
				zap.String("ddl", event.Query),
				zap.Uint64("startTs", event.GetStartTs()), zap.Uint64("commitTs", event.GetCommitTs()),
				zap.Error(err))
			return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Execute DDL failed, Query info: %s; ", event.GetDDLQuery())))
		}
		log.Info("Execute DDL succeeded",
			zap.String("changefeed", w.ChangefeedID.String()), zap.String("query", event.GetDDLQuery()),
			zap.Uint64("startTs", event.GetStartTs()), zap.Uint64("commitTs", event.GetCommitTs()),
			zap.Any("ddl", event))
		return nil
	}, retry.WithBackoffBaseDelay(BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(defaultDDLMaxRetry),
		retry.WithIsRetryableErr(errors.IsRetryableDDLError))
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

	switch event.GetBlockedTables().InfluenceType {
	// db-class, all-class ddl with not affect by async ddl, just return
	case commonEvent.InfluenceTypeDB, commonEvent.InfluenceTypeAll:
		return
	}

	for _, blockedTable := range event.GetBlockedTableNames() {
		// query the downstream,
		// if the ddl is still running, we should wait for it.
		err := w.checkAndWaitAsyncDDLDoneDownstream(blockedTable.SchemaName, blockedTable.TableName)
		if err != nil {
			log.Error("check previous asynchronous ddl failed",
				zap.String("keyspace", w.ChangefeedID.Keyspace()),
				zap.Stringer("changefeed", w.ChangefeedID),
				zap.Error(err))
		}
	}
}

// true means the async ddl is still running, false means the async ddl is done.
func (w *Writer) doQueryAsyncDDL(query string) (bool, error) {
	start := time.Now()
	rows, err := w.db.QueryContext(w.ctx, query)
	log.Debug("query duration", zap.Any("duration", time.Since(start)), zap.Any("query", query))
	if err != nil {
		return false, errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to query ddl jobs table; Query is %s", query)))
	}
	rets, err := export.GetSpecifiedColumnValuesAndClose(rows, "JOB_ID", "JOB_TYPE", "SCHEMA_STATE", "STATE", "QUERY")
	if err != nil {
		log.Error("check previous asynchronous ddl failed",
			zap.String("changefeed", w.ChangefeedID.String()),
			zap.Error(err))
		return false, errors.Trace(err)
	}

	if len(rets) == 0 {
		return false, nil
	}
	ret := rets[0]
	jobID, jobType, schemaState, state, runningDDL := ret[0], ret[1], ret[2], ret[3], ret[4]
	log.Info("async ddl is still running",
		zap.String("changefeed", w.ChangefeedID.String()),
		zap.Duration("checkDuration", time.Since(start)),
		zap.String("runningDDL", runningDDL),
		zap.String("query", query),
		zap.Any("jobID", jobID),
		zap.String("jobType", jobType),
		zap.String("schemaState", schemaState),
		zap.String("state", state))

	return true, nil
}

// query the ddl jobs to find the state of the async ddl
// if the ddl is still running, we should wait for it.
func (w *Writer) checkAndWaitAsyncDDLDoneDownstream(schemaName, tableName string) error {
	checkSQL := getCheckRunningAddIndexSQL(w.cfg)
	query := fmt.Sprintf(checkSQL, schemaName, tableName)
	running, err := w.doQueryAsyncDDL(query)
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
			running, err = w.doQueryAsyncDDL(query)
			if err != nil {
				return err
			}
			if !running {
				return nil
			}
		}
	}
}
