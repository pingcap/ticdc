// Copyright 2025 PingCAP, Inc.
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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	progressTableName = "ticdc_progress_table"
	maxBatchSize      = 256
)

// ProgressTableWriter is responsible for writing active-active progress rows.
type ProgressTableWriter struct {
	ctx          context.Context
	db           *sql.DB
	changefeedID common.ChangeFeedID

	tableSchemaStore *event.TableSchemaStore

	progressUpdateInterval time.Duration

	// lastProgressUpdate is the last time Flush successfully wrote progress rows.
	// Zero value means Flush has never succeeded and should not be throttled.
	lastProgressUpdate time.Time
	lastDDLCommitTs    atomic.Uint64

	// initMu serializes initProgressTable and protects progressTableInit.
	// It allows retries when initialization fails.
	initMu            sync.Mutex
	progressTableInit bool

	maxTableNameCount int
}

// NewProgressTableWriter creates a new writer for active-active progress updates.
func NewProgressTableWriter(ctx context.Context, db *sql.DB, changefeedID common.ChangeFeedID, maxTableNameCount int, progressUpdateInterval time.Duration) *ProgressTableWriter {
	if maxTableNameCount <= 0 {
		maxTableNameCount = maxBatchSize
	}
	return &ProgressTableWriter{
		ctx:                    ctx,
		db:                     db,
		changefeedID:           changefeedID,
		maxTableNameCount:      maxTableNameCount,
		progressUpdateInterval: progressUpdateInterval,
	}
}

// SetTableSchemaStore injects the schema store.
func (w *ProgressTableWriter) SetTableSchemaStore(store *event.TableSchemaStore) {
	w.tableSchemaStore = store
}

// Flush writes checkpoint info for all tracked tables.
func (w *ProgressTableWriter) Flush(checkpoint uint64) error {
	if w.tableSchemaStore == nil || checkpoint == 0 {
		return nil
	}

	if !w.lastProgressUpdate.IsZero() && time.Since(w.lastProgressUpdate) < w.progressUpdateInterval {
		return nil
	}
	// skip checkpointTs less than or equal to last DDL commit ts
	// which will makes tableNames incorrect after DDL like drop table/database
	if checkpoint <= w.lastDDLCommitTs.Load() {
		return nil
	}

	tableNames := w.tableSchemaStore.GetAllTableNames(checkpoint, false)
	if len(tableNames) == 0 {
		return nil
	}
	sort.Slice(tableNames, func(i, j int) bool {
		if tableNames[i].SchemaName != tableNames[j].SchemaName {
			return tableNames[i].SchemaName < tableNames[j].SchemaName
		}
		return tableNames[i].TableName < tableNames[j].TableName
	})

	if err := w.initProgressTable(w.ctx); err != nil {
		return err
	}

	changefeed := w.changefeedID.DisplayName.String()
	clusterID := config.GetGlobalServerConfig().ClusterID
	for start := 0; start < len(tableNames); start += w.maxTableNameCount {
		end := start + w.maxTableNameCount
		if end > len(tableNames) {
			end = len(tableNames)
		}
		if err := w.flushBatch(tableNames[start:end], checkpoint, changefeed, clusterID); err != nil {
			return errors.Trace(err)
		}
	}
	w.lastProgressUpdate = time.Now()
	return nil
}

// flushBatch writes a batch of progress rows in a single INSERT statement.
// The progress table is keyed by (changefeed_id, cluster_id, database_name, table_name),
// so repeated writes for the same table are idempotent.
func (w *ProgressTableWriter) flushBatch(
	tableNames []*event.SchemaTableName,
	checkpoint uint64,
	changefeed string,
	clusterID string,
) error {
	var builder strings.Builder
	builder.WriteString("INSERT INTO `")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString("`.`")
	builder.WriteString(progressTableName)
	builder.WriteString("` (changefeed_id, cluster_id, database_name, table_name, checkpoint_ts) VALUES ")

	args := make([]interface{}, 0, len(tableNames)*5)
	for idx, tbl := range tableNames {
		if idx > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("(?,?,?,?,?)")
		args = append(args, changefeed, clusterID, tbl.SchemaName, tbl.TableName, checkpoint)
	}

	builder.WriteString(" ON DUPLICATE KEY UPDATE checkpoint_ts = VALUES(checkpoint_ts)")
	query := builder.String()
	_, err := w.db.ExecContext(w.ctx, query, args...)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Failed to execute query, query info:%s, args:%v; ", query, util.RedactArgs(args))))
	}
	return nil
}

// initProgressTable lazily creates the system database and progress table.
// It is safe to call multiple times and caches initialization after success.
func (w *ProgressTableWriter) initProgressTable(ctx context.Context) error {
	w.initMu.Lock()
	defer w.initMu.Unlock()

	if w.progressTableInit {
		return nil
	}

	createDB := "CREATE DATABASE IF NOT EXISTS `" + filter.TiCDCSystemSchema + "`"
	if _, err := w.db.ExecContext(ctx, createDB); err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Failed to execute sql, sql info:%s", createDB)))
	}
	createTable := "CREATE TABLE IF NOT EXISTS `" + filter.TiCDCSystemSchema + "`.`" + progressTableName + "` (" +
		"changefeed_id VARCHAR(128) NOT NULL COMMENT 'Unique identifier for the changefeed synchronization task'," +
		"cluster_id VARCHAR(128) NOT NULL COMMENT 'TiCDC cluster ID'," +
		"database_name VARCHAR(128) NOT NULL COMMENT 'Name of the upstream database'," +
		"table_name VARCHAR(128) NOT NULL COMMENT 'Name of the upstream table'," +
		"checkpoint_ts BIGINT UNSIGNED NOT NULL COMMENT 'Safe watermark CommitTS indicating the data has been synchronized'," +
		"PRIMARY KEY (changefeed_id, cluster_id, database_name, table_name)" +
		") COMMENT='TiCDC synchronization progress table for HardDelete safety check'"
	if _, err := w.db.ExecContext(ctx, createTable); err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Failed to execute sql, sql info:%s", createTable)))
	}

	w.progressTableInit = true
	return nil
}

// RemoveTables removes progress rows for dropped tables or databases described in the DDL event.
// This is needed to keep downstream hard delete safety checks accurate after schema changes.
func (w *ProgressTableWriter) RemoveTables(ddl *event.DDLEvent) error {
	tableNameChange := ddl.TableNameChange
	if tableNameChange == nil {
		return nil
	}

	if err := w.initProgressTable(w.ctx); err != nil {
		return err
	}

	w.lastDDLCommitTs.Store(ddl.GetCommitTs())

	changefeed := w.changefeedID.DisplayName.String()
	clusterID := config.GetGlobalServerConfig().ClusterID

	if len(tableNameChange.DropName) != 0 {
		for start := 0; start < len(tableNameChange.DropName); start += w.maxTableNameCount {
			end := start + w.maxTableNameCount
			if end > len(tableNameChange.DropName) {
				end = len(tableNameChange.DropName)
			}
			if err := w.removeTableBatch(changefeed, clusterID, tableNameChange.DropName[start:end]); err != nil {
				log.Error("failed to remove dropped tables from progress table",
					zap.String("keyspace", w.changefeedID.Keyspace()),
					zap.Stringer("changefeed", w.changefeedID),
					zap.Error(err))
				return err
			}
		}
	}

	if tableNameChange.DropDatabaseName != "" {
		if err := w.removeDatabase(changefeed, clusterID, tableNameChange.DropDatabaseName); err != nil {
			log.Error("failed to remove dropped database from progress table",
				zap.String("keyspace", w.changefeedID.Keyspace()),
				zap.Stringer("changefeed", w.changefeedID),
				zap.Error(err))
			return err
		}
	}
	return nil
}

// removeTableBatch deletes progress rows for a batch of (database_name, table_name) pairs.
func (w *ProgressTableWriter) removeTableBatch(changefeed, clusterID string, tables []event.SchemaTableName) error {
	var builder strings.Builder
	builder.WriteString("DELETE FROM `")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString("`.`")
	builder.WriteString(progressTableName)
	builder.WriteString("` WHERE changefeed_id = ? AND cluster_id = ? AND (")

	args := make([]interface{}, 0, len(tables)*2+2)
	args = append(args, changefeed, clusterID)

	for idx, tbl := range tables {
		if idx > 0 {
			builder.WriteString(" OR ")
		}
		builder.WriteString("(database_name = ? AND table_name = ?)")
		args = append(args, tbl.SchemaName, tbl.TableName)
	}
	builder.WriteString(")")

	query := builder.String()
	_, err := w.db.ExecContext(w.ctx, query, args...)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Failed to execute query, query info:%s, args:%v; ", query, util.RedactArgs(args))))
	}
	return nil
}

// removeDatabase deletes all progress rows for a database name.
func (w *ProgressTableWriter) removeDatabase(changefeed, clusterID, dbName string) error {
	var builder strings.Builder
	builder.WriteString("DELETE FROM `")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString("`.`")
	builder.WriteString(progressTableName)
	builder.WriteString("` WHERE changefeed_id = ? AND cluster_id = ? AND database_name = ?")

	query := builder.String()
	_, err := w.db.ExecContext(w.ctx, query, changefeed, clusterID, dbName)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("Failed to execute query, query info:%s, args:%v; ", query, util.RedactArgs([]interface{}{changefeed, clusterID, dbName}))))
	}
	return nil
}
