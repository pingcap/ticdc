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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

func (s *Sink) runProgressUpdater(ctx context.Context) error {
	if s.progressUpdateInterval <= 0 {
		return nil
	}
	ticker := time.NewTicker(s.progressUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if err := s.flushProgressTable(ctx); err != nil {
				log.Warn("failed to update active-active progress table",
					zap.String("changefeed", s.changefeedID.DisplayName.String()),
					zap.Error(err))
			}
		}
	}
}

func (s *Sink) flushProgressTable(ctx context.Context) error {
	checkpoint := s.latestCheckpoint.Load()
	if checkpoint == 0 {
		return nil
	}
	tables := s.getTrackedTables()
	if len(tables) == 0 {
		return nil
	}
	if err := s.ensureProgressTable(ctx); err != nil {
		return err
	}
	changefeedID := s.changefeedID.DisplayName.String()
	upstream := strconv.FormatUint(s.upstreamID, 10)

	var builder strings.Builder
	builder.WriteString("INSERT INTO `")
	builder.WriteString(progressDatabase)
	builder.WriteString("`.`")
	builder.WriteString(progressTableName)
	builder.WriteString("` (changefeed_id, upstreamID, database_name, table_name, checkpoint_ts) VALUES ")
	args := make([]interface{}, 0, len(tables)*5)
	placeholderIndex := 0
	for _, tbl := range tables {
		if tbl == nil {
			continue
		}
		if tbl.SchemaName == "" || tbl.TableName == "" {
			continue
		}
		if placeholderIndex > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("(?,?,?,?,?)")
		args = append(args, changefeedID, upstream, tbl.SchemaName, tbl.TableName, checkpoint)
		placeholderIndex++
	}
	if placeholderIndex == 0 {
		return nil
	}
	builder.WriteString(" ON DUPLICATE KEY UPDATE checkpoint_ts = VALUES(checkpoint_ts)")
	_, err := s.db.ExecContext(ctx, builder.String(), args...)
	return errors.Trace(err)
}

func (s *Sink) ensureProgressTable(ctx context.Context) error {
	if s.progressTableInit.Load() {
		return nil
	}
	s.progressTableInitMu.Lock()
	defer s.progressTableInitMu.Unlock()
	if s.progressTableInit.Load() {
		return nil
	}
	createDB := "CREATE DATABASE IF NOT EXISTS `" + progressDatabase + "`"
	if _, err := s.db.ExecContext(ctx, createDB); err != nil {
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
	if _, err := s.db.ExecContext(ctx, createTable); err != nil {
		return errors.Trace(err)
	}
	s.progressTableInit.Store(true)
	return nil
}

func (s *Sink) getTrackedTables() []*commonEvent.SchemaTableName {
	if s.tableSchemaStore == nil {
		return nil
	}
	return s.tableSchemaStore.GetAllTableNames(math.MaxUint64)
}
