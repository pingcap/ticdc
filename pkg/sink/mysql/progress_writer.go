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
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/util"
)

const (
	progressDatabase  = "tidb_cdc"
	progressTableName = "ticdcProgressTable"
)

// ProgressWriter is responsible for writing active-active progress rows.
type ProgressWriter struct {
	ctx          context.Context
	db           *sql.DB
	changefeedID common.ChangeFeedID
	upstreamID   uint64

	tableSchemaStore *util.TableSchemaStore

	progressTableInit bool
	progressTableMu   sync.Mutex
}

// NewProgressWriter creates a new writer for active-active progress updates.
func NewProgressWriter(ctx context.Context, db *sql.DB, changefeedID common.ChangeFeedID, upstreamID uint64) *ProgressWriter {
	return &ProgressWriter{
		ctx:          ctx,
		db:           db,
		changefeedID: changefeedID,
		upstreamID:   upstreamID,
	}
}

// SetTableSchemaStore injects the schema store.
func (w *ProgressWriter) SetTableSchemaStore(store *util.TableSchemaStore) {
	w.tableSchemaStore = store
}

// Flush writes checkpoint info for all tracked tables.
func (w *ProgressWriter) Flush(checkpoint uint64) error {
	if w == nil || w.tableSchemaStore == nil || checkpoint == 0 {
		return nil
	}

	tableNames := w.tableSchemaStore.GetAllTableNames(checkpoint)
	if len(tableNames) == 0 {
		return nil
	}

	if err := w.ensureProgressTable(w.ctx); err != nil {
		return err
	}

	changefeed := w.changefeedID.DisplayName.String()
	upstream := strconv.FormatUint(w.upstreamID, 10)
	var builder strings.Builder
	builder.WriteString("INSERT INTO `")
	builder.WriteString(progressDatabase)
	builder.WriteString("`.`")
	builder.WriteString(progressTableName)
	builder.WriteString("` (changefeed_id, upstreamID, database_name, table_name, checkpoint_ts) VALUES ")

	args := make([]interface{}, 0, len(tableNames)*5)
	written := 0
	for _, tbl := range tableNames {
		if written > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("(?,?,?,?,?)")
		args = append(args, changefeed, upstream, tbl.SchemaName, tbl.TableName, checkpoint)
		written++
	}

	builder.WriteString(" ON DUPLICATE KEY UPDATE checkpoint_ts = VALUES(checkpoint_ts)")
	_, err := w.db.ExecContext(w.ctx, builder.String(), args...)
	return errors.Trace(err)
}

func (w *ProgressWriter) ensureProgressTable(ctx context.Context) error {
	if w.progressTableInit {
		return nil
	}
	w.progressTableMu.Lock()
	defer w.progressTableMu.Unlock()
	if w.progressTableInit {
		return nil
	}
	createDB := "CREATE DATABASE IF NOT EXISTS `" + progressDatabase + "`"
	if _, err := w.db.ExecContext(ctx, createDB); err != nil {
		return errors.Trace(err)
	}
	createTable := "CREATE TABLE IF NOT EXISTS `" + progressDatabase + "`.`" + progressTableName + "` (" +
		"changefeed_id VARCHAR(255) NOT NULL COMMENT 'Unique identifier for the changefeed synchronization task'," +
		"upstreamID VARCHAR(255) NOT NULL COMMENT 'Unique identifier for the upstream cluster'," +
		"database_name VARCHAR(255) NOT NULL COMMENT 'Name of the upstream database'," +
		"table_name VARCHAR(255) NOT NULL COMMENT 'Name of the upstream table'," +
		"checkpoint_ts BIGINT UNSIGNED NOT NULL COMMENT 'Safe watermark CommitTS indicating the data has been synchronized'," +
		"PRIMARY KEY (changefeed_id, upstreamID, database_name, table_name)" +
		") COMMENT='TiCDC synchronization progress table for HardDelete safety check'"
	if _, err := w.db.ExecContext(ctx, createTable); err != nil {
		return errors.Trace(err)
	}
	w.progressTableInit = true
	return nil
}
