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

package main

import (
	"context"
	"database/sql"
	"math/rand"
	"strings"
	"sync"
	"time"

	plog "github.com/pingcap/log"
	"go.uber.org/zap"
	"workload/schema"
	pbank2 "workload/schema/bank2"
	psysbench "workload/schema/sysbench"
)

// updateTask defines a task for updating data
type updateTask struct {
	schema.UpdateOption
	generatedSQL string
	// reserved for future use
	callback func()
}

// executeUpdateWorkers executes update workers
func (app *WorkloadApp) executeUpdateWorkers(updateConcurrency int, wg *sync.WaitGroup) {
	if updateConcurrency == 0 {
		plog.Info("skip update workload",
			zap.String("action", app.Config.Action),
			zap.Int("totalThread", app.Config.Thread),
			zap.Float64("percentageForUpdate", app.Config.PercentageForUpdate))
		return
	}

	updateTaskCh := make(chan updateTask, updateConcurrency)

	// generate update tasks
	wg.Add(1)
	go func() {
		defer wg.Done()
		app.genUpdateTask(updateTaskCh)
	}()

	// start update workers
	wg.Add(updateConcurrency)
	for i := range updateConcurrency {
		db := app.DBManager.GetDB()

		go func(workerID int) {
			defer func() {
				plog.Info("update worker exited", zap.Int("worker", workerID))
				wg.Done()
			}()
			for {
				conn, err := db.DB.Conn(context.Background())
				if err != nil {
					plog.Info("get connection failed, wait 5 seconds and retry", zap.Error(err))
					time.Sleep(time.Second * 5)
					continue
				}
				plog.Info("start update worker", zap.Int("worker", workerID))
				err = app.doUpdate(conn, updateTaskCh)
				if err != nil {
					plog.Info("update worker failed, reset the connection and retry", zap.Error(err))
					conn.Close()
					time.Sleep(time.Second * 2)
				}
			}
		}(i)
	}
}

// genUpdateTask generates update tasks
func (app *WorkloadApp) genUpdateTask(output chan updateTask) {
	for {
		tableIndex := rand.Intn(app.Config.TableCount) + app.Config.TableStartIndex
		task := updateTask{
			UpdateOption: schema.UpdateOption{
				TableIndex: tableIndex,
				Batch:      app.Config.BatchSize,
				RangeNum:   app.Config.RangeNum,
			},
		}
		output <- task
	}
}

// doUpdate performs update operations on the database
func (app *WorkloadApp) doUpdate(conn *sql.Conn, input chan updateTask) error {
	for task := range input {
		if err := app.processUpdateTask(conn, task); err != nil {
			// Return only if connection is closed, other errors are handled within processUpdateTask
			if strings.Contains(err.Error(), "connection is already closed") {
				return err
			}
		}
	}
	return nil
}

// processUpdateTask handles a single update task
func (app *WorkloadApp) processUpdateTask(conn *sql.Conn, task updateTask) error {
	// Execute update and get result
	res, err := app.executeUpdate(conn, task)
	if err != nil {
		return app.handleUpdateError(err, task)
	}

	// Process update result
	if err := app.processUpdateResult(res, task); err != nil {
		return err
	}

	// Execute callback if exists
	if task.callback != nil {
		task.callback()
	}

	return nil
}

// executeUpdate performs the actual update operation based on workload type
func (app *WorkloadApp) executeUpdate(conn *sql.Conn, task updateTask) (sql.Result, error) {
	switch app.Config.WorkloadType {
	case bank2:
		return app.executeBank2Update(conn, task)
	case sysbench:
		return app.executeSysbenchUpdate(conn, task)
	default:
		return app.executeRegularUpdate(conn, task)
	}
}

// executeBank2Update handles updates specific to bank2 workload
func (app *WorkloadApp) executeBank2Update(conn *sql.Conn, task updateTask) (sql.Result, error) {
	task.UpdateOption.Batch = 1
	updateSQL, values := app.Workload.(*pbank2.Bank2Workload).BuildUpdateSqlWithValues(task.UpdateOption)
	return app.executeWithValues(conn, updateSQL, task.UpdateOption.TableIndex, values)
}

// executeSysbenchUpdate handles updates specific to sysbench workload
func (app *WorkloadApp) executeSysbenchUpdate(conn *sql.Conn, task updateTask) (sql.Result, error) {
	updateSQL := app.Workload.(*psysbench.SysbenchWorkload).BuildUpdateSqlWithConn(conn, task.UpdateOption)
	if updateSQL == "" {
		return nil, nil
	}
	return app.execute(conn, updateSQL, task.TableIndex)
}

// executeRegularUpdate handles updates for non-bank2 workloads
func (app *WorkloadApp) executeRegularUpdate(conn *sql.Conn, task updateTask) (sql.Result, error) {
	updateSQL := app.Workload.BuildUpdateSql(task.UpdateOption)
	if updateSQL == "" {
		return nil, nil
	}
	task.generatedSQL = updateSQL
	return app.execute(conn, updateSQL, task.TableIndex)
}

// handleUpdateError processes update operation errors
func (app *WorkloadApp) handleUpdateError(err error, task updateTask) error {
	app.Stats.ErrorCount.Add(1)
	// Truncate long SQL for logging
	plog.Info("update error",
		zap.Error(err),
		zap.String("sql", getSQLPreview(task.generatedSQL)))
	return err
}

// processUpdateResult handles the result of update operation
func (app *WorkloadApp) processUpdateResult(res sql.Result, task updateTask) error {
	if res == nil {
		return nil
	}

	cnt, err := res.RowsAffected()
	if err != nil {
		plog.Info("get rows affected error",
			zap.Error(err),
			zap.Int64("affectedRows", cnt),
			zap.Int("rowCount", task.Batch),
			zap.String("sql", getSQLPreview(task.generatedSQL)))
		app.Stats.ErrorCount.Add(1)
	}

	app.Stats.FlushedRowCount.Add(uint64(cnt))

	if task.IsSpecialUpdate {
		plog.Info("update full table succeed",
			zap.Int("table", task.TableIndex),
			zap.Int64("affectedRows", cnt))
	}

	return nil
}
