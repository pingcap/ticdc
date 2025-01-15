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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/apperror"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

func (w *MysqlWriter) FlushDDLTs(event *commonEvent.DDLEvent) error {
	if !w.ddlTsTableInit {
		// create checkpoint ts table if not exist
		err := w.CreateDDLTsTable()
		if err != nil {
			return err
		}
		w.ddlTsTableInit = true
	}

	err := w.SendDDLTs(event)
	return errors.Trace(err)
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
	builder.WriteString(" WHERE (ticdc_cluster_id, changefeed, table_id, ddl_ts) IN (")

	builder.WriteString("('")
	builder.WriteString(ticdcClusterID)
	builder.WriteString("', '")
	builder.WriteString(changefeedID)
	builder.WriteString("', ")
	builder.WriteString(strconv.FormatInt(tableID, 10))
	builder.WriteString(", ")
	builder.WriteString(strconv.FormatUint(ddlTs, 10))
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

func (w *MysqlWriter) CreateDDLTsTable() error {
	database := filter.TiCDCSystemSchema
	query := `CREATE TABLE IF NOT EXISTS %s
	(
		ticdc_cluster_id varchar (255),
		changefeed varchar(255),
		ddl_ts varchar(18),
		table_id bigint(21),
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		INDEX (ticdc_cluster_id, changefeed, table_id),
		PRIMARY KEY (ticdc_cluster_id, changefeed, table_id)
	);`
	query = fmt.Sprintf(query, filter.DDLTsTable)

	return w.CreateTable(database, filter.DDLTsTable, query)
}

func (w *MysqlWriter) SendDDLTs(event *commonEvent.DDLEvent) error {
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
		// generate query
		// INSERT INTO `tidb_cdc`.`ddl_ts` (ticdc_cluster_id, changefeed, ddl_ts, table_id) values(...) ON DUPLICATE KEY UPDATE ddl_ts=VALUES(ddl_ts), created_at=CURRENT_TIMESTAMP;
		var builder strings.Builder
		builder.WriteString("INSERT INTO ")
		builder.WriteString(filter.TiCDCSystemSchema)
		builder.WriteString(".")
		builder.WriteString(filter.DDLTsTable)
		builder.WriteString(" (ticdc_cluster_id, changefeed, ddl_ts, table_id) VALUES ")

		for idx, tableId := range tableIds {
			builder.WriteString("('")
			builder.WriteString(ticdcClusterID)
			builder.WriteString("', '")
			builder.WriteString(changefeedID)
			builder.WriteString("', '")
			builder.WriteString(ddlTs)
			builder.WriteString("', ")
			builder.WriteString(strconv.FormatInt(tableId, 10))
			builder.WriteString(")")
			if idx < len(tableIds)-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString(" ON DUPLICATE KEY UPDATE ddl_ts=VALUES(ddl_ts), created_at=CURRENT_TIMESTAMP;")

		query := builder.String()
		log.Debug("send ddl ts table query", zap.String("query", query))

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
		query := builder.String()
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

// GetStartTsList return the startTs list for each table in the tableIDs list.
// For each table,
// If no ddl-ts-v1 table or no the row for the table , startTs = 0; -- means the table is new.
// Otherwise, startTs = ddl-ts value.
func (w *MysqlWriter) GetStartTsList(tableIDs []int64) ([]int64, error) {
	retStartTsList := make([]int64, len(tableIDs))
	tableIdIdxMap := make(map[int64]int, 0)
	for i, tableID := range tableIDs {
		tableIdIdxMap[tableID] = i
	}

	changefeedID := w.ChangefeedID.String()
	ticdcClusterID := config.GetGlobalServerConfig().ClusterID

	var builder strings.Builder
	builder.WriteString("SELECT table_id, ddl_ts FROM ")
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
	query := builder.String()

	rows, err := w.db.Query(query)
	if err != nil {
		if apperror.IsTableNotExistsErr(err) {
			// If this table is not existed, this means the table is first being synced
			log.Info("ddl ts table is not found",
				zap.String("namespace", w.ChangefeedID.Namespace()),
				zap.String("changefeedID", w.ChangefeedID.Name()),
				zap.Error(err))
			return retStartTsList, nil
		}
		return retStartTsList, cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
	}

	defer rows.Close()
	var ddlTs int64
	var tableId int64
	count := 0
	for rows.Next() {
		err := rows.Scan(&tableId, &ddlTs)
		if err != nil {
			return retStartTsList, cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to check ddl ts table; Query is %s", query)))
		}
		count += 1
		retStartTsList[tableIdIdxMap[tableId]] = ddlTs
	}

	return retStartTsList, nil
}

func (w *MysqlWriter) CreateTable(dbName string, tableName string, createTableQuery string) error {
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
