// Copyright 2026 PingCAP, Inc.
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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	plog "github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	ddlColumnPrefix = "ddl_col_"
	ddlIndexPrefix  = "ddl_idx_"
)

var ddlNameSeq atomic.Uint64

func (r *DDLRunner) startWorkers(wg *sync.WaitGroup) {
	workerCount := r.app.Config.DDLWorker
	if workerCount <= 0 {
		workerCount = 1
	}

	wg.Add(workerCount)
	for workerID := range workerCount {
		db := r.app.DBManager.GetDB()
		go func(workerID int, db *DBWrapper) {
			defer func() {
				plog.Info("ddl worker exited", zap.Int("worker", workerID))
				wg.Done()
			}()

			conn, err := getConnWithTimeout(db.DB, 10*time.Second)
			if err != nil {
				plog.Info("get connection failed for ddl worker", zap.Error(err))
				time.Sleep(5 * time.Second)
				return
			}
			defer func() {
				if conn != nil {
					conn.Close()
				}
			}()

			plog.Info("start ddl worker", zap.Int("worker", workerID), zap.String("db", db.Name))

			for {
				task := <-r.taskCh
				if err := r.executeTask(conn, task); err != nil {
					if r.app.isConnectionError(err) {
						conn.Close()
						time.Sleep(2 * time.Second)
						newConn, err := getConnWithTimeout(db.DB, 10*time.Second)
						if err != nil {
							plog.Info("reconnect failed for ddl worker", zap.Error(err))
							time.Sleep(5 * time.Second)
							continue
						}
						conn = newConn
					}
				}
			}
		}(workerID, db)
	}
}

func getConnWithTimeout(db *sql.DB, timeout time.Duration) (*sql.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return conn, nil
}

func (r *DDLRunner) executeTask(conn *sql.Conn, task DDLTask) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.app.Config.DDLTimeout)
	defer cancel()

	sqlStr, skipped, reason, err := r.buildDDL(ctx, conn, task)
	if err != nil {
		r.app.Stats.DDLFailed.Add(1)
		r.app.Stats.ErrorCount.Add(1)
		plog.Info("build ddl failed",
			zap.String("ddlType", task.Type.String()),
			zap.String("table", task.Table.String()),
			zap.Error(err))
		return err
	}
	if skipped {
		r.app.Stats.DDLSkipped.Add(1)
		if reason != "" {
			plog.Debug("ddl task skipped",
				zap.String("ddlType", task.Type.String()),
				zap.String("table", task.Table.String()),
				zap.String("reason", reason))
		}
		return nil
	}

	r.app.Stats.DDLExecuted.Add(1)
	r.app.Stats.QueryCount.Add(1)
	start := time.Now()
	if _, err := conn.ExecContext(ctx, sqlStr); err != nil {
		r.app.Stats.DDLFailed.Add(1)
		r.app.Stats.ErrorCount.Add(1)
		plog.Info("execute ddl failed",
			zap.String("ddlType", task.Type.String()),
			zap.String("table", task.Table.String()),
			zap.Duration("cost", time.Since(start)),
			zap.String("sql", getSQLPreview(sqlStr)),
			zap.Error(err))
		return err
	}

	r.app.Stats.DDLSucceeded.Add(1)
	plog.Debug("ddl executed",
		zap.String("ddlType", task.Type.String()),
		zap.String("table", task.Table.String()),
		zap.Duration("cost", time.Since(start)))
	return nil
}

func (r *DDLRunner) buildDDL(ctx context.Context, conn *sql.Conn, task DDLTask) (sqlStr string, skipped bool, reason string, err error) {
	switch task.Type {
	case ddlAddColumn:
		return r.buildAddColumnDDL(task.Table), false, "", nil
	case ddlDropColumn:
		return r.buildDropColumnDDL(ctx, conn, task.Table)
	case ddlAddIndex:
		return r.buildAddIndexDDL(ctx, conn, task.Table)
	case ddlDropIndex:
		return r.buildDropIndexDDL(ctx, conn, task.Table)
	case ddlTruncateTable:
		return r.buildTruncateTableDDL(task.Table), false, "", nil
	default:
		return "", false, "", errors.Errorf("unknown ddl type: %d", task.Type)
	}
}

func (r *DDLRunner) buildAddColumnDDL(table TableName) string {
	colName := ddlColumnPrefix + ddlNameSuffix()
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s BIGINT NOT NULL DEFAULT 0",
		quoteTable(table),
		quoteIdent(colName))
}

func (r *DDLRunner) buildDropColumnDDL(ctx context.Context, conn *sql.Conn, table TableName) (string, bool, string, error) {
	col, ok, err := selectOne(ctx, conn, `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = ?
  AND table_name = ?
  AND column_name LIKE ?
`, table.Schema, table.Name, ddlColumnPrefix+"%")
	if err != nil {
		return "", false, "", err
	}
	if !ok {
		return "", true, "no ddl columns", nil
	}
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
		quoteTable(table),
		quoteIdent(col)), false, "", nil
}

func (r *DDLRunner) buildAddIndexDDL(ctx context.Context, conn *sql.Conn, table TableName) (string, bool, string, error) {
	col, ok, err := selectOne(ctx, conn, `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = ?
  AND table_name = ?
  AND column_name LIKE ?
`, table.Schema, table.Name, ddlColumnPrefix+"%")
	if err != nil {
		return "", false, "", err
	}

	if !ok {
		col, ok, err = selectOne(ctx, conn, `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = ?
  AND table_name = ?
  AND data_type NOT IN ('json','tinyblob','blob','mediumblob','longblob','tinytext','text','mediumtext','longtext')
`, table.Schema, table.Name)
		if err != nil {
			return "", false, "", err
		}
		if !ok {
			return "", true, "no suitable columns", nil
		}
	}

	idxName := ddlIndexPrefix + ddlNameSuffix()
	return fmt.Sprintf("ALTER TABLE %s ADD INDEX %s (%s)",
		quoteTable(table),
		quoteIdent(idxName),
		quoteIdent(col)), false, "", nil
}

func (r *DDLRunner) buildDropIndexDDL(ctx context.Context, conn *sql.Conn, table TableName) (string, bool, string, error) {
	idx, ok, err := selectOne(ctx, conn, `
SELECT DISTINCT index_name
FROM information_schema.statistics
WHERE table_schema = ?
  AND table_name = ?
  AND index_name LIKE ?
`, table.Schema, table.Name, ddlIndexPrefix+"%")
	if err != nil {
		return "", false, "", err
	}
	if !ok {
		return "", true, "no ddl indexes", nil
	}
	return fmt.Sprintf("ALTER TABLE %s DROP INDEX %s",
		quoteTable(table),
		quoteIdent(idx)), false, "", nil
}

func (r *DDLRunner) buildTruncateTableDDL(table TableName) string {
	return fmt.Sprintf("TRUNCATE TABLE %s", quoteTable(table))
}

func ddlNameSuffix() string {
	seq := ddlNameSeq.Add(1)
	return fmt.Sprintf("%d_%d", time.Now().UnixNano(), seq)
}

func quoteIdent(name string) string {
	escaped := strings.ReplaceAll(name, "`", "``")
	return "`" + escaped + "`"
}

func quoteTable(table TableName) string {
	return quoteIdent(table.Schema) + "." + quoteIdent(table.Name)
}

func selectOne(ctx context.Context, conn *sql.Conn, query string, args ...interface{}) (value string, ok bool, err error) {
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return "", false, errors.Trace(err)
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return "", false, errors.Trace(err)
		}
		values = append(values, v)
	}
	if err := rows.Err(); err != nil {
		return "", false, errors.Trace(err)
	}
	if len(values) == 0 {
		return "", false, nil
	}
	return values[rand.Intn(len(values))], true, nil
}
