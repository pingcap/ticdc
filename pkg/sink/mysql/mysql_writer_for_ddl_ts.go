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
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/apperror"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

// FlushDDLTsPre is used to flush ddl ts before the ddl event is sent to downstream.
// It only be called when downstream is tidb.
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
func (w *MysqlWriter) FlushDDLTsPre(event commonEvent.BlockEvent) error {
	err := w.createDDLTsTableIfNotExist()
	if err != nil {
		return err
	}
	err = w.SendDDLTsPre(event)
	return errors.Trace(err)
}

func (w *MysqlWriter) FlushDDLTs(event commonEvent.BlockEvent) error {
	err := w.createDDLTsTableIfNotExist()
	if err != nil {
		return err
	}
	err = w.SendDDLTs(event)
	return errors.Trace(err)
}

func (w *MysqlWriter) SendDDLTsPre(event commonEvent.BlockEvent) error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "ddl ts table: begin Tx fail;"))
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
		DDLQuery := ""
		if event.GetType() == commonEvent.TypeDDLEvent {
			isSyncpoint = "0"
			DDLQuery = event.(*commonEvent.DDLEvent).Query
		}
		query := insertItemQuery(tableIds, ticdcClusterID, changefeedID, ddlTs, "0", isSyncpoint, DDLQuery)
		log.Info("send ddl ts table query", zap.String("query", query))

		_, err = tx.Exec(query)
		if err != nil {
			log.Error("failed to write ddl ts table", zap.Error(err))
			err2 := tx.Rollback()
			if err2 != nil {
				log.Error("failed to write ddl ts table", zap.Error(err2))
			}
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to write ddl ts table; Exec Failed; Query is %s", query)))
		}
	} else {
		log.Error("table ids is empty when write ddl ts table, FIX IT", zap.Any("event", event))
	}

	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write ddl ts table; Commit Fail;"))
}

func (w *MysqlWriter) SendDDLTs(event commonEvent.BlockEvent) error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "ddl ts table: begin Tx fail;"))
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
		ddlQuery := ""
		if event.GetType() == commonEvent.TypeDDLEvent {
			isSyncpoint = "0"
			ddlQuery = event.(*commonEvent.DDLEvent).Query
		}
		query := insertItemQuery(tableIds, ticdcClusterID, changefeedID, ddlTs, "1", isSyncpoint, ddlQuery)
		log.Info("send ddl ts table query", zap.String("query", query))

		_, err = tx.Exec(query)
		if err != nil {
			log.Error("failed to write ddl ts table", zap.Error(err))
			err2 := tx.Rollback()
			if err2 != nil {
				log.Error("failed to write ddl ts table", zap.Error(err2))
			}
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to write ddl ts table; Exec Failed; Query is %s", query)))
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
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to delete ddl ts item; Query is %s", query)))
		}
	}

	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write ddl ts table; Commit Fail;"))
}

func insertItemQuery(tableIds []int64, ticdcClusterID string, changefeedID string, ddlTs string, finished string, isSyncpoint string, query string) string {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.DDLTsTable)
	builder.WriteString(" (ticdc_cluster_id, changefeed, ddl_ts, table_id, query, finished, is_syncpoint) VALUES ")

	for idx, tableId := range tableIds {
		builder.WriteString("('")
		builder.WriteString(ticdcClusterID)
		builder.WriteString("', '")
		builder.WriteString(changefeedID)
		builder.WriteString("', '")
		builder.WriteString(ddlTs)
		builder.WriteString("', ")
		builder.WriteString(strconv.FormatInt(tableId, 10))
		builder.WriteString(", '")
		builder.WriteString(query)
		builder.WriteString("', ")
		builder.WriteString(finished)
		builder.WriteString(", ")
		builder.WriteString(isSyncpoint)
		builder.WriteString(")")
		if idx < len(tableIds)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteString(" ON DUPLICATE KEY UPDATE finished=VALUES(finished), query=VALUES(query), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);")

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

// GetStartTsList return the startTs list for each table in the tableIDs list.
// For each table,
//  1. If no ddl-ts-v1 table or no the row for the table , startTs = 0; -- means the table is new.
//  2. Else,
//     2.1 If the downstream is non-tidb, startTs = ddlTs
//     2.2 Else
//     2.2.1 if the ddlTs is finished, startTs = ddlTs
//     2.2.2 else query the ddl_jobs table to find the latest ddl job time for the table
//     (we use related table to find the ddl job -- take `truncate table` as an example, the ddl job used the table truncated, but not the new table)
//     2.2.2.1 if the latest ddl job time is larger than the createdAt, startTs = ddlTs
//     2.2.2.2 else startTs = ddlTs - 1
func (w *MysqlWriter) GetStartTsList(tableIDs []int64) ([]int64, []bool, error) {
	retStartTsList := make([]int64, len(tableIDs))
	tableIdIdxMap := make(map[int64]int, 0)
	isSyncpoints := make([]bool, len(tableIDs))
	for i, tableID := range tableIDs {
		tableIdIdxMap[tableID] = i
		isSyncpoints[i] = false
	}

	changefeedID := w.ChangefeedID.String()
	ticdcClusterID := config.GetGlobalServerConfig().ClusterID

	query := selectDDLTsQuery(tableIDs, ticdcClusterID, changefeedID)
	rows, err := w.db.Query(query)
	if err != nil {
		if apperror.IsTableNotExistsErr(err) {
			// If this table is not existed, this means the table is first being synced
			log.Info("ddl ts table is not found",
				zap.String("namespace", w.ChangefeedID.Namespace()),
				zap.String("changefeedID", w.ChangefeedID.Name()),
				zap.Error(err))
			return retStartTsList, isSyncpoints, nil
		}
		return retStartTsList, isSyncpoints, cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
	}

	defer rows.Close()
	var ddlTs, tableId int64
	var ddlQuery string
	var finished, isSyncpoint bool
	var createdAtBytes []byte
	for rows.Next() {
		err := rows.Scan(&tableId, &ddlQuery, &ddlTs, &finished, &createdAtBytes, &isSyncpoint)
		if err != nil {
			return retStartTsList, isSyncpoints, cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
		}
		if finished {
			retStartTsList[tableIdIdxMap[tableId]] = ddlTs
			isSyncpoints[tableIdIdxMap[tableId]] = isSyncpoint
		} else {
			if !w.cfg.IsTiDB {
				log.Panic("ddl ts table is not finished, but downstream is not tidb, FIX IT")
			}

			createdAt, err := time.Parse("2006-01-02 15:04:05", string(createdAtBytes))
			if err != nil {
				log.Error("Failed to parse created_at", zap.Any("createdAtBytes", createdAtBytes), zap.Any("error", err))
				retStartTsList[tableIdIdxMap[tableId]] = ddlTs - 1
				continue
			}

			// query the ddl_jobs table to find whether the ddl is executed and the ddl created time
			createdTime, ok := w.queryDDLJobs(ddlQuery)
			if !ok {
				retStartTsList[tableIdIdxMap[tableId]] = ddlTs - 1
			} else {
				if createdAt.Before(createdTime) {
					// show the ddl is executed
					retStartTsList[tableIdIdxMap[tableId]] = ddlTs
					isSyncpoints[tableIdIdxMap[tableId]] = isSyncpoint
					log.Debug("createdTime is larger than createdAt", zap.Int64("tableId", tableId), zap.Any("ddlQuery", ddlQuery), zap.Int64("ddlTs", ddlTs), zap.Int64("startTs", ddlTs))
					continue
				} else {
					// show the ddl is not executed
					retStartTsList[tableIdIdxMap[tableId]] = ddlTs - 1
					log.Debug("createdTime is less than  createdAt", zap.Int64("tableId", tableId), zap.Any("ddlQuery", ddlQuery), zap.Int64("ddlTs", ddlTs), zap.Int64("startTs", ddlTs-1))
					continue
				}
			}
		}
	}

	return retStartTsList, isSyncpoints, nil
}

func selectDDLTsQuery(tableIDs []int64, ticdcClusterID string, changefeedID string) string {
	var builder strings.Builder
	builder.WriteString("SELECT table_id, query, ddl_ts, finished, created_at, is_syncpoint FROM ")
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

var queryDDLJobs = `SELECT CREATE_TIME FROM information_schema.ddl_jobs WHERE QUERY = "%s" order by CREATE_TIME desc limit 1;`

func (w *MysqlWriter) queryDDLJobs(ddlQuery string) (time.Time, bool) {
	if ddlQuery == "" {
		log.Info("ddlQuery is empty")
		return time.Time{}, false
	}
	// query the ddl_jobs table to find whether the ddl is executed
	query := fmt.Sprintf(queryDDLJobs, ddlQuery)
	log.Info("query the info from ddl jobs", zap.String("query", query))

	start := time.Now()
	ddlJobRows, err := w.db.Query(query)
	if err != nil {
		log.Error("failed to query ddl jobs", zap.Error(err))
		return time.Time{}, false
	}
	log.Info("query ddl jobs cost time", zap.Duration("cost", time.Since(start)))

	defer ddlJobRows.Close()
	var createdTimeBytes []byte
	var createdTime time.Time
	for ddlJobRows.Next() {
		err := ddlJobRows.Scan(&createdTimeBytes)
		if err != nil {
			log.Error("failed to query ddl jobs", zap.Error(err))
			return time.Time{}, false
		}
		createdTime, err = time.Parse("2006-01-02 15:04:05", string(createdTimeBytes))
		if err != nil {
			log.Error("Failed to parse createdTimeBytes", zap.Any("createdTimeBytes", createdTimeBytes), zap.Any("error", err))
			return time.Time{}, false
		}
		return createdTime, true
	}
	log.Debug("no ddl job item", zap.Any("query", ddlQuery))
	return time.Time{}, false
}

func (w *MysqlWriter) RemoveDDLTsItem() error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "select ddl ts table: begin Tx fail;"))
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
		if apperror.IsTableNotExistsErr(err) {
			// If this table is not existed, this means the changefeed has not table, so we just return nil.
			log.Info("ddl ts table is not found when RemoveDDLTsItem",
				zap.String("namespace", w.ChangefeedID.Namespace()),
				zap.String("changefeedID", w.ChangefeedID.Name()),
				zap.Error(err))
			return nil
		}
		log.Error("failed to delete ddl ts item ", zap.Error(err))
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to delete ddl ts item", zap.Error(err2))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to delete ddl ts item; Query is %s", query)))
	}

	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to delete ddl ts item; Query is %s", query)))
}

func (w *MysqlWriter) isDDLExecuted(tableID int64, ddlTs uint64) (bool, error) {
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
		return false, cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
	}

	defer rows.Close()
	if rows.Next() {
		return true, nil
	}
	return false, nil
}

func (w *MysqlWriter) createTable(dbName string, tableName string, createTableQuery string) error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: begin Tx fail;", tableName)))
	}

	// we try to set cdc write source for the ddl
	if err = SetWriteSource(w.cfg, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			if errors.Cause(rbErr) != context.Canceled {
				log.Error("Failed to rollback", zap.Error(err))
			}
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: set write source fail;", tableName)))
	}

	_, err = tx.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to rollback", zap.Any("tableName", tableName), zap.Error(errRollback))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to create %s table;", tableName)))
	}
	_, err = tx.Exec("USE " + dbName)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to rollback", zap.Any("tableName", tableName), zap.Error(errRollback))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: use %s db fail;", tableName, dbName)))
	}

	_, err = tx.Exec(createTableQuery)
	if err != nil {
		errRollback := tx.Rollback()
		if errRollback != nil {
			log.Error("failed to rollback", zap.Any("tableName", tableName), zap.Error(errRollback))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: Exec fail; Query is %s", tableName, createTableQuery)))
	}
	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("create %s table: Commit Failed; Query is %s", tableName, createTableQuery)))
}

func (w *MysqlWriter) createDDLTsTable() error {
	database := filter.TiCDCSystemSchema
	query := `CREATE TABLE IF NOT EXISTS %s
	(
		ticdc_cluster_id varchar (255),
		changefeed varchar(255),
		ddl_ts varchar(18),
		table_id bigint(21),
		finished bool,
		query varchar(1024),
		is_syncpoint bool,
		created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
		INDEX (ticdc_cluster_id, changefeed, table_id),
		PRIMARY KEY (ticdc_cluster_id, changefeed, table_id)
	);`
	query = fmt.Sprintf(query, filter.DDLTsTable)

	return w.createTable(database, filter.DDLTsTable, query)
}

func (w *MysqlWriter) createDDLTsTableIfNotExist() error {
	if !w.ddlTsTableInit {
		// create checkpoint ts table if not exist
		err := w.createDDLTsTable()
		if err != nil {
			return err
		}
		w.ddlTsTableInit = true
	}
	return nil
}
