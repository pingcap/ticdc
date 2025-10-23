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
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"go.uber.org/zap"
)

// FlushDDLTsPre is used to flush ddl ts before the ddl event is sent to downstream.
// It's used to fix the potential data loss problem leading by the ddl ts event and ddl event can't be atomicly send to downstream.
//
// For example,
// If we don't flush ddl ts pre before the ddl event,
// it may happens that the ddl event is sent to downstream, and the server is down
// so the ddl ts event is not sent to downstream.
// Then when the server is up, we search the ddl ts table and find the ddl ts is not exist,
// and we may think the ddl event is not sent to downstream, so we will use the startTs with the last ddl ts.
// It will cause we use the wrong startTs and cause the data loss.
//
// Thus, we try to flush ddl ts pre first before the ddl event is sent to downstream,
// and after send the ddl ts, we update the ddl ts item to finished.
// It can maximum guarantee we use the correct startTs.
func (w *Writer) FlushDDLTsPre(event commonEvent.BlockEvent) error {
	if w.cfg.DryRun || !w.cfg.EnableDDLTs {
		return nil
	}
	err := w.createDDLTsTableIfNotExist()
	if err != nil {
		return err
	}
	return w.SendDDLTsPre(event)
}

func (w *Writer) FlushDDLTs(event commonEvent.BlockEvent) error {
	if w.cfg.DryRun || !w.cfg.EnableDDLTs {
		return nil
	}
	err := w.createDDLTsTableIfNotExist()
	if err != nil {
		return err
	}
	return w.SendDDLTs(event)
}

func (w *Writer) SendDDLTsPre(event commonEvent.BlockEvent) error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, "ddl ts table: begin Tx fail;"))
	}

	changefeedID := w.ChangefeedID.String()
	ticdcClusterID := config.GetGlobalServerConfig().ClusterID
	ddlTs := strconv.FormatUint(event.GetCommitTs(), 10)
	var tableIds []int64

	relatedTables := event.GetBlockedTables()

	switch relatedTables.InfluenceType {
	case commonEvent.InfluenceTypeNormal:
		tableIds = append(tableIds, relatedTables.TableIDs...)
	case commonEvent.InfluenceTypeDB:
		ids := w.tableSchemaStore.GetTableIdsByDB(relatedTables.SchemaID)
		tableIds = append(tableIds, ids...)
	case commonEvent.InfluenceTypeAll:
		ids := w.tableSchemaStore.GetAllTableIds()
		tableIds = append(tableIds, ids...)
	}

	addTables := event.GetNeedAddedTables()
	for _, table := range addTables {
		tableIds = append(tableIds, table.TableID)
	}

	if len(tableIds) > 0 {
		isSyncpoint := "1"
		if event.GetType() == commonEvent.TypeDDLEvent {
			isSyncpoint = "0"
		}
		query := insertItemQuery(tableIds, ticdcClusterID, changefeedID, ddlTs, "0", isSyncpoint)
		log.Info("send ddl ts table query", zap.String("query", query))

		_, err = tx.Exec(query)
		if err != nil {
			log.Error("failed to write ddl ts table", zap.Error(err))
			err2 := tx.Rollback()
			if err2 != nil {
				log.Error("failed to write ddl ts table", zap.Error(err2))
			}
			return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to write ddl ts table; Exec Failed; Query is %s", query)))
		}
	} else {
		log.Error("table ids is empty when write ddl ts table, FIX IT", zap.Any("event", event))
	}

	err = tx.Commit()
	return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, "failed to write ddl ts table; Commit Fail;"))
}

func (w *Writer) SendDDLTs(event commonEvent.BlockEvent) error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, "ddl ts table: begin Tx fail;"))
	}

	changefeedID := w.ChangefeedID.String()
	ticdcClusterID := config.GetGlobalServerConfig().ClusterID
	ddlTs := strconv.FormatUint(event.GetCommitTs(), 10)
	var tableIds []int64
	var dropTableIds []int64

	relatedTables := event.GetBlockedTables()

	switch relatedTables.InfluenceType {
	case commonEvent.InfluenceTypeNormal:
		tableIds = append(tableIds, relatedTables.TableIDs...)
	case commonEvent.InfluenceTypeDB:
		ids := w.tableSchemaStore.GetTableIdsByDB(relatedTables.SchemaID)
		tableIds = append(tableIds, ids...)
	case commonEvent.InfluenceTypeAll:
		ids := w.tableSchemaStore.GetAllTableIds()
		tableIds = append(tableIds, ids...)
	}

	dropTables := event.GetNeedDroppedTables()
	if dropTables != nil {
		switch dropTables.InfluenceType {
		case commonEvent.InfluenceTypeNormal:
			dropTableIds = append(dropTableIds, dropTables.TableIDs...)
		case commonEvent.InfluenceTypeDB:
			// for drop table, we will never delete the item of table trigger, so we get normal table ids for the schemaID.
			ids := w.tableSchemaStore.GetNormalTableIdsByDB(dropTables.SchemaID)
			dropTableIds = append(dropTableIds, ids...)
		case commonEvent.InfluenceTypeAll:
			// for drop table, we will never delete the item of table trigger, so we get normal table ids for the schemaID.
			ids := w.tableSchemaStore.GetAllNormalTableIds()
			dropTableIds = append(dropTableIds, ids...)
		}
	}

	addTables := event.GetNeedAddedTables()
	for _, table := range addTables {
		tableIds = append(tableIds, table.TableID)
	}

	if len(tableIds) > 0 {
		isSyncpoint := "1"
		if event.GetType() == commonEvent.TypeDDLEvent {
			isSyncpoint = "0"
		}
		query := insertItemQuery(tableIds, ticdcClusterID, changefeedID, ddlTs, "1", isSyncpoint)
		log.Info("send ddl ts table query", zap.String("query", query))

		_, err = tx.Exec(query)
		if err != nil {
			log.Error("failed to write ddl ts table", zap.Error(err))
			err2 := tx.Rollback()
			if err2 != nil {
				log.Error("failed to write ddl ts table", zap.Error(err2))
			}
			return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to write ddl ts table; Exec Failed; Query is %s", query)))
		}
	} else {
		log.Error("table ids is empty when write ddl ts table, FIX IT", zap.Any("event", event))
	}

	if len(dropTableIds) > 0 {
		// drop item for this tableid
		query := dropItemQuery(dropTableIds, ticdcClusterID, changefeedID)
		log.Debug("send ddl ts table query", zap.String("query", query))

		_, err = tx.Exec(query)
		if err != nil {
			log.Error("failed to delete ddl ts item ", zap.Error(err))
			err2 := tx.Rollback()
			if err2 != nil {
				log.Error("failed to delete ddl ts item", zap.Error(err2))
			}
			return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to delete ddl ts item; Query is %s", query)))
		}
	}

	err = tx.Commit()
	return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, "failed to write ddl ts table; Commit Fail;"))
}

func insertItemQuery(tableIds []int64, ticdcClusterID, changefeedID, ddlTs, finished, isSyncpoint string) string {
	var builder strings.Builder

	builder.WriteString("INSERT INTO ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.DDLTsTable)
	builder.WriteString(" (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ")

	for idx, tableId := range tableIds {
		builder.WriteString("('")
		builder.WriteString(ticdcClusterID)
		builder.WriteString("', '")
		builder.WriteString(changefeedID)
		builder.WriteString("', '")
		builder.WriteString(ddlTs)
		builder.WriteString("', ")
		builder.WriteString(strconv.FormatInt(tableId, 10))
		builder.WriteString(", ")
		builder.WriteString(finished)
		builder.WriteString(", ")
		builder.WriteString(isSyncpoint)
		builder.WriteString(")")
		if idx < len(tableIds)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString(" ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);")

	return builder.String()
}

func dropItemQuery(dropTableIds []int64, ticdcClusterID string, changefeedID string) string {
	var builder strings.Builder
	builder.WriteString("DELETE FROM ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.DDLTsTable)
	builder.WriteString(" WHERE (ticdc_cluster_id, changefeed, table_id) IN (")

	for idx, tableId := range dropTableIds {
		builder.WriteString("('")
		builder.WriteString(ticdcClusterID)
		builder.WriteString("', '")
		builder.WriteString(changefeedID)
		builder.WriteString("', ")
		builder.WriteString(strconv.FormatInt(tableId, 10))
		builder.WriteString(")")
		if idx < len(dropTableIds)-1 {
			builder.WriteString(", ")
		}
	}

	builder.WriteString(")")
	return builder.String()
}

// GetTableRecoveryInfo queries the ddl-ts table to determine recovery information for the given tables.
//
// Returns:
//   - startTsList: The startTs to use for each table
//   - skipSyncpointAtStartTsList: Whether to skip syncpoint events at startTs for each table
//   - skipDMLAsStartTsList: Whether to skip DML events at startTs+1 for each table
//
// Recovery logic based on ddl-ts table state:
//  1. No record found (new table): Returns 0 for all values
//  2. finished=1: DDL and optional syncpoint completed normally
//     - Returns ddlTs
//     - skipSyncpointAtStartTs = is_syncpoint (skip if it was a syncpoint)
//     - skipDMLAsStartTs = false
//  3. finished=0, is_syncpoint=false: DDL not finished (crash during DDL)
//     - Returns ddlTs-1 to replay DDL at ddlTs
//     - skipDMLAsStartTs = true (skip already-written DML at ddlTs)
//  4. finished=0, is_syncpoint=true: Syncpoint not finished
//     - If there was a DDL at the same ts, it has already been executed
//     (because we only write syncpoint pre after DDL is fully completed)
//     - Returns ddlTs (no need to replay DDL even if it existed)
//     - skipSyncpointAtStartTs = false (replay syncpoint)
//     - skipDMLAsStartTs = false (process DML normally)
func (w *Writer) GetTableRecoveryInfo(tableIDs []int64) ([]int64, []bool, []bool, error) {
	retStartTsList := make([]int64, len(tableIDs))
	// when split table enabled, there may have some same tableID in tableIDs
	tableIdIdxMap := make(map[int64][]int, len(tableIDs))
	skipSyncpointAtStartTs := make([]bool, len(tableIDs))
	skipDMLAsStartTsList := make([]bool, len(tableIDs))
	for i, tableID := range tableIDs {
		tableIdIdxMap[tableID] = append(tableIdIdxMap[tableID], i)
		skipSyncpointAtStartTs[i] = false
		skipDMLAsStartTsList[i] = false
	}

	changefeedID := w.ChangefeedID.String()
	ticdcClusterID := config.GetGlobalServerConfig().ClusterID

	query := selectDDLTsQuery(tableIDs, ticdcClusterID, changefeedID)
	log.Info("query ddl ts table", zap.String("query", query))
	rows, err := w.db.Query(query)
	if err != nil {
		if errors.IsTableNotExistsErr(err) {
			// If this table is not existed, this means the table is first being synced
			log.Info("ddl ts table is not found",
				zap.String("keyspace", w.ChangefeedID.Keyspace()),
				zap.String("changefeedID", w.ChangefeedID.Name()),
				zap.Error(err))
			return retStartTsList, skipSyncpointAtStartTs, skipDMLAsStartTsList, nil
		}
		return retStartTsList, skipSyncpointAtStartTs, skipDMLAsStartTsList, errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
	}

	defer rows.Close()
	var ddlTs, tableId int64
	var finished, isSyncpoint bool
	for rows.Next() {
		err = rows.Scan(&tableId, &ddlTs, &finished, &isSyncpoint)
		if err != nil {
			return retStartTsList, skipSyncpointAtStartTs, skipDMLAsStartTsList, errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
		}
		if finished {
			for _, idx := range tableIdIdxMap[tableId] {
				retStartTsList[idx] = ddlTs
				skipSyncpointAtStartTs[idx] = isSyncpoint
			}
		} else {
			// finished = 0, need to distinguish between DDL and Syncpoint
			if isSyncpoint {
				// Case: Syncpoint not finished (crashed after writing syncpoint pre).
				// The execution order is: FlushDDLTsPre(DDL) -> execDDL() -> FlushDDLTs(DDL) -> FlushDDLTsPre(Syncpoint)
				// If we see isSyncpoint=1 && finished=0, we crashed after FlushDDLTsPre(Syncpoint).
				//
				// Key insight: Since we've written syncpoint pre, any DDL at the same ts (if exists)
				// must have already been executed and completed. We only write syncpoint pre after
				// the DDL is fully done.
				//
				// Therefore:
				// - Start from ddlTs (not ddlTs-1) - no need to replay DDL even if it existed
				// - Set skipSyncpoint=false to receive and replay the syncpoint event
				// - Set skipDMLAsStartTs=false because DML should be processed normally
				for _, idx := range tableIdIdxMap[tableId] {
					retStartTsList[idx] = ddlTs
					skipSyncpointAtStartTs[idx] = false // Need to receive syncpoint event
					skipDMLAsStartTsList[idx] = false   // DML should not be skipped
				}
			} else {
				// Case: DDL not finished (or finished but not marked).
				// Even in some corner case, the DDL actually executed, but ddl ts is not finished.
				// We can tolerate to rewrite the DDL again.
				// Because the ddl ts is not finish, so no dmls after this ddl will be flushed downstream.
				// Besides, the granularity of DDL execution is guaranteed, so executing DDL once more will not affect its correctness.
				//
				// In this case, we set startTs = ddlTs - 1 to replay the DDL at ddlTs.
				// However, the DML at ddlTs might have already been written to downstream before the crash.
				// To avoid duplicate DML writes, we set skipDMLAsStartTs = true to filter out DML events at startTs+1 (which is ddlTs).
				// DDL events at ddlTs will still be processed to ensure the DDL is replayed.
				for _, idx := range tableIdIdxMap[tableId] {
					retStartTsList[idx] = ddlTs - 1
					skipDMLAsStartTsList[idx] = true
				}
			}
		}
	}

	return retStartTsList, skipSyncpointAtStartTs, skipDMLAsStartTsList, nil
}

func selectDDLTsQuery(tableIDs []int64, ticdcClusterID string, changefeedID string) string {
	var builder strings.Builder
	builder.WriteString("SELECT table_id, ddl_ts, finished, is_syncpoint FROM ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.DDLTsTable)
	builder.WriteString(" WHERE (ticdc_cluster_id, changefeed, table_id) IN (")

	for idx, tableID := range tableIDs {
		builder.WriteString("('")
		builder.WriteString(ticdcClusterID)
		builder.WriteString("', '")
		builder.WriteString(changefeedID)
		builder.WriteString("', ")
		builder.WriteString(strconv.FormatInt(tableID, 10))
		builder.WriteString(")")
		if idx < len(tableIDs)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString(")")
	return builder.String()
}

func (w *Writer) RemoveDDLTsItem() error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, "select ddl ts table: begin Tx fail;"))
	}

	changefeedID := w.ChangefeedID.String()
	ticdcClusterID := config.GetGlobalServerConfig().ClusterID

	var builder strings.Builder
	builder.WriteString("DELETE FROM ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.DDLTsTable)
	builder.WriteString(" WHERE (ticdc_cluster_id, changefeed) IN (")

	builder.WriteString("('")
	builder.WriteString(ticdcClusterID)
	builder.WriteString("', '")
	builder.WriteString(changefeedID)
	builder.WriteString("')")
	builder.WriteString(")")
	query := builder.String()

	_, err = tx.Exec(query)
	if err != nil {
		if errors.IsTableNotExistsErr(err) {
			// If this table is not existed, this means the changefeed has not table, so we just return nil.
			log.Info("ddl ts table is not found when RemoveDDLTsItem",
				zap.String("keyspace", w.ChangefeedID.Keyspace()),
				zap.String("changefeedID", w.ChangefeedID.Name()),
				zap.Error(err))
			return nil
		}
		log.Error("failed to delete ddl ts item ", zap.Error(err))
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to delete ddl ts item", zap.Error(err2))
		}
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to delete ddl ts item; Query is %s", query)))
	}

	err = tx.Commit()
	return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to delete ddl ts item; Query is %s", query)))
}

func (w *Writer) isDDLExecuted(tableID int64, ddlTs uint64) (bool, error) {
	changefeedID := w.ChangefeedID.String()
	ticdcClusterID := config.GetGlobalServerConfig().ClusterID

	// select * from xx where (ticdc_cluster_id, changefeed, table_id, ddl_ts) in (("xx","xx",x,x));
	var builder strings.Builder
	builder.WriteString("SELECT * FROM ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.DDLTsTable)
	builder.WriteString(" WHERE (ticdc_cluster_id, changefeed, table_id, ddl_ts, finished) IN (")

	builder.WriteString("('")
	builder.WriteString(ticdcClusterID)
	builder.WriteString("', '")
	builder.WriteString(changefeedID)
	builder.WriteString("', ")
	builder.WriteString(strconv.FormatInt(tableID, 10))
	builder.WriteString(", ")
	builder.WriteString(strconv.FormatUint(ddlTs, 10))
	builder.WriteString(", ")
	builder.WriteString("1")
	builder.WriteString(")")
	builder.WriteString(")")
	query := builder.String()

	rows, err := w.db.Query(query)
	if err != nil {
		return false, errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
	}

	defer rows.Close()
	if rows.Next() {
		return true, nil
	}
	return false, nil
}

func (w *Writer) createTable(dbName string, tableName string, createTableQuery string) error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: begin Tx fail;", tableName)))
	}

	_, err = tx.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to rollback", zap.Any("tableName", tableName), zap.Error(errRollback))
		}
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to create %s table;", tableName)))
	}
	_, err = tx.Exec("USE " + dbName)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to rollback", zap.Any("tableName", tableName), zap.Error(errRollback))
		}
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: use %s db fail;", tableName, dbName)))
	}

	_, err = tx.Exec(createTableQuery)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to rollback", zap.Any("tableName", tableName), zap.Error(errRollback))
		}
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: Exec fail; Query is %s", tableName, createTableQuery)))
	}
	err = tx.Commit()
	if err != nil {
		return errors.WrapError(errors.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: Commit Failed; Query is %s", tableName, createTableQuery)))
	}
	return nil
}

func (w *Writer) createDDLTsTable() error {
	database := filter.TiCDCSystemSchema
	query := `CREATE TABLE IF NOT EXISTS %s
	(
		ticdc_cluster_id varchar (255),
		changefeed varchar(255),
		ddl_ts varchar(18),
		table_id bigint(21),
		finished bool,
		is_syncpoint bool,
		INDEX (ticdc_cluster_id, changefeed, table_id),
		PRIMARY KEY (ticdc_cluster_id, changefeed, table_id)
	);`
	query = fmt.Sprintf(query, filter.DDLTsTable)

	return w.createTable(database, filter.DDLTsTable, query)
}

func (w *Writer) createDDLTsTableIfNotExist() error {
	w.ddlTsTableInitMutex.Lock()
	defer w.ddlTsTableInitMutex.Unlock()
	if w.ddlTsTableInit {
		return nil
	}
	// create checkpoint ts table if not exist
	err := w.createDDLTsTable()
	if err != nil {
		return err
	}
	w.ddlTsTableInit = true
	return nil
}
