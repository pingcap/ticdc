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
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"workload/schema"
)

func (app *WorkloadApp) executeDDLWorkers(ddlConcurrency int, wg *sync.WaitGroup) {
	if ddlConcurrency == 0 {
		log.Info("skip ddl workload",
			zap.String("action", app.Config.Action),
			zap.Int("ddlConcurrency", ddlConcurrency))
		return
	}

	ddlWorkload, ok := app.Workload.(schema.DDLWorkload)
	if !ok {
		log.Info("skip ddl workload",
			zap.String("action", app.Config.Action),
			zap.String("workloadType", app.Config.WorkloadType),
			zap.String("reason", "ddl not supported"))
		return
	}

	wg.Add(ddlConcurrency)
	for i := 0; i < ddlConcurrency; i++ {
		db := app.DBManager.GetDB()

		go func(workerID int) {
			defer func() {
				log.Info("ddl worker exited", zap.Int("worker", workerID))
				wg.Done()
			}()

			// Get connection once and reuse it with context timeout
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			conn, err := db.DB.Conn(ctx)
			cancel()
			if err != nil {
				log.Info("get connection failed, wait 5 seconds and retry", zap.Error(err))
				time.Sleep(time.Second * 5)
				return
			}
			defer conn.Close()

			log.Info("start ddl worker",
				zap.Int("worker", workerID),
				zap.String("db", db.Name),
				zap.Duration("ddlInterval", app.Config.DDLInterval))

			for {
				err = app.doDDL(conn, ddlWorkload)
				if err != nil {
					// Check if it's a connection-level error that requires reconnection
					if app.isConnectionError(err) {
						log.Warn("connection error detected, reconnecting", zap.Error(err))
						conn.Close()
						time.Sleep(time.Second * 2)

						// Get new connection with timeout
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						conn, err = db.DB.Conn(ctx)
						cancel()
						if err != nil {
							fmt.Println("reconnection failed, wait 5 seconds and retry", zap.Error(err))
							time.Sleep(time.Second * 5)
							continue
						}
						continue
					}

					log.Info("ddl worker failed, retrying", zap.Int("worker", workerID), zap.Error(err))
					time.Sleep(time.Second * 2)
					continue
				}
			}
		}(i)
	}
}

func (app *WorkloadApp) doDDL(conn *sql.Conn, ddlWorkload schema.DDLWorkload) error {
	tableIndex := rand.Intn(app.Config.TableCount) + app.Config.TableStartIndex
	ddlSQL := ddlWorkload.BuildDDLSql(schema.DDLOption{TableIndex: tableIndex})
	if ddlSQL == "" {
		app.sleepAfterDDL()
		return nil
	}

	_, err := app.executeDDL(conn, ddlSQL, tableIndex)
	if err != nil {
		app.Stats.ErrorCount.Add(1)
		log.Info("ddl error",
			zap.Error(err),
			zap.String("sql", getSQLPreview(ddlSQL)))
	}

	app.sleepAfterDDL()
	return err
}

func (app *WorkloadApp) sleepAfterDDL() {
	if app.Config.DDLInterval > 0 {
		time.Sleep(app.Config.DDLInterval)
	}
}

func (app *WorkloadApp) executeDDL(conn *sql.Conn, ddlSQL string, tableIndex int) (sql.Result, error) {
	app.Stats.QueryCount.Add(1)

	res, err := conn.ExecContext(context.Background(), ddlSQL)
	if err == nil {
		log.Info("execute ddl success",
			zap.Error(err),
			zap.String("sql", getSQLPreview(ddlSQL)))
		return res, nil
	}

	if isIgnorableDDLError(err) {
		return res, nil
	}

	// If table does not exist, create it and retry.
	if strings.Contains(err.Error(), "Error 1146") {
		_, createErr := conn.ExecContext(context.Background(), app.Workload.BuildCreateTableStatement(tableIndex))
		if createErr != nil {
			log.Info("create table error", zap.Error(createErr))
			return res, createErr
		}
		res, err = conn.ExecContext(context.Background(), ddlSQL)
		if err != nil && isIgnorableDDLError(err) {
			return res, nil
		}
		return res, err
	}

	log.Info("execute ddl error",
		zap.Error(err),
		zap.String("sql", getSQLPreview(ddlSQL)))
	return res, err
}

func isIgnorableDDLError(err error) bool {
	if err == nil {
		return false
	}

	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		switch myErr.Number {
		case 1060: // ER_DUP_FIELDNAME
			return true
		case 1061: // ER_DUP_KEYNAME
			return true
		case 1091: // ER_CANT_DROP_FIELD_OR_KEY
			return true
		}
	}

	// Fallback matching for error strings not wrapped as MySQLError.
	errStr := err.Error()
	return strings.Contains(errStr, "Duplicate column name") ||
		strings.Contains(errStr, "Duplicate key name") ||
		strings.Contains(errStr, "check that column/key exists")
}
