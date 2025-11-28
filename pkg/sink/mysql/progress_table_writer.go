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
	"strings"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
)

const (
	progressTableName = "ticdcProgressTable"
)

// ProgressTableWriter is responsible for writing active-active progress rows.
type tableNameProvider interface {
	GetAllTableNames(ts uint64) []*event.SchemaTableName
}

// ProgressTableWriter is responsible for writing active-active progress rows.
type ProgressTableWriter struct {
	ctx          context.Context
	db           *sql.DB
	changefeedID common.ChangeFeedID

	tableSchemaStore tableNameProvider

	progressTableInit bool

	maxTableNameCount int
}

// NewProgressTableWriter creates a new writer for active-active progress updates.
func NewProgressTableWriter(ctx context.Context, db *sql.DB, changefeedID common.ChangeFeedID, maxTableNameCount int) *ProgressTableWriter {
	return &ProgressTableWriter{
		ctx:               ctx,
		db:                db,
		changefeedID:      changefeedID,
		maxTableNameCount: maxTableNameCount,
	}
}

// SetTableSchemaStore injects the schema store.
func (w *ProgressTableWriter) SetTableSchemaStore(store tableNameProvider) {
	w.tableSchemaStore = store
}

// Flush writes checkpoint info for all tracked tables.
func (w *ProgressTableWriter) Flush(checkpoint uint64) error {
	if w.tableSchemaStore == nil || checkpoint == 0 {
		return nil
	}

	tableNames := w.tableSchemaStore.GetAllTableNames(checkpoint)
	if len(tableNames) == 0 {
		return nil
	}

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
	return nil
}

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
	_, err := w.db.ExecContext(w.ctx, builder.String(), args...)
	return err
}

func (w *ProgressTableWriter) initProgressTable(ctx context.Context) error {
	if w.progressTableInit {
		return nil
	}

	createDB := "CREATE DATABASE IF NOT EXISTS `" + filter.TiCDCSystemSchema + "`"
	if _, err := w.db.ExecContext(ctx, createDB); err != nil {
		return errors.Trace(err)
	}
	createTable := "CREATE TABLE IF NOT EXISTS `" + filter.TiCDCSystemSchema + "`.`" + progressTableName + "` (" +
		"changefeed_id VARCHAR(255) NOT NULL COMMENT 'Unique identifier for the changefeed synchronization task'," +
		"cluster_id VARCHAR(255) NOT NULL COMMENT 'TiCDC cluster ID'," +
		"database_name VARCHAR(255) NOT NULL COMMENT 'Name of the upstream database'," +
		"table_name VARCHAR(255) NOT NULL COMMENT 'Name of the upstream table'," +
		"checkpoint_ts BIGINT UNSIGNED NOT NULL COMMENT 'Safe watermark CommitTS indicating the data has been synchronized'," +
		"PRIMARY KEY (changefeed_id, cluster_id, database_name, table_name)" +
		") COMMENT='TiCDC synchronization progress table for HardDelete safety check'"
	if _, err := w.db.ExecContext(ctx, createTable); err != nil {
		return errors.Trace(err)
	}

	w.progressTableInit = true
	return nil
}
