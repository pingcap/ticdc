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
	"fmt"
	"math/rand"
	"sync"
	"time"

	plog "github.com/pingcap/log"
	"go.uber.org/zap"
	"workload/schema"
	pbank2 "workload/schema/bank2"
	pjsonzstd "workload/schema/jsonzstd"
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

			// Get connection once and reuse it with context timeout
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			conn, err := db.DB.Conn(ctx)
			cancel()
			if err != nil {
				plog.Info("get connection failed, wait 5 seconds and retry", zap.Error(err))
				time.Sleep(time.Second * 5)
				return
			}
			defer conn.Close()

			plog.Info("start update worker", zap.Int("worker", workerID))

			for {
				flushedRows, err := app.runTransaction(conn, func() (uint64, error) {
					return app.doUpdateOnce(conn, updateTaskCh)
				})
				if err != nil {
					// Check if it's a connection-level error that requires reconnection
					if app.isConnectionError(err) {
						fmt.Println("connection error detected, reconnecting", zap.Error(err))
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
					}

					app.Stats.ErrorCount.Add(1)
					plog.Info("update worker failed, retrying", zap.Int("worker", workerID), zap.Error(err))
					time.Sleep(time.Second * 2)
					continue
				}

				if flushedRows != 0 {
					app.Stats.FlushedRowCount.Add(flushedRows)
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

func (app *WorkloadApp) doUpdateOnce(conn *sql.Conn, input chan updateTask) (uint64, error) {
	task := <-input
	return app.processUpdateTask(conn, &task)
}

// processUpdateTask handles a single update task
func (app *WorkloadApp) processUpdateTask(conn *sql.Conn, task *updateTask) (uint64, error) {
	// Execute update and get result
	res, err := app.executeUpdate(conn, task)
	if err != nil {
		app.handleUpdateError(err, task)
		return 0, err
	}

	// Process update result
	affectedRows := app.processUpdateResult(res, task)

	// Execute callback if exists
	if task.callback != nil {
		task.callback()
	}

	return affectedRows, nil
}

// executeUpdate performs the actual update operation based on workload type
func (app *WorkloadApp) executeUpdate(conn *sql.Conn, task *updateTask) (sql.Result, error) {
	switch app.Config.WorkloadType {
	case bank2:
		return app.executeBank2Update(conn, task)
	case sysbench:
		return app.executeSysbenchUpdate(conn, task)
	case jsonZstd:
		return app.executeJSONZstdUpdate(conn, task)
	default:
		return app.executeRegularUpdate(conn, task)
	}
}

// executeBank2Update handles updates specific to bank2 workload
func (app *WorkloadApp) executeBank2Update(conn *sql.Conn, task *updateTask) (sql.Result, error) {
	task.UpdateOption.Batch = 1
	updateSQL, values := app.Workload.(*pbank2.Bank2Workload).BuildUpdateSqlWithValues(task.UpdateOption)
	task.generatedSQL = updateSQL
	return app.executeWithValues(conn, updateSQL, task.UpdateOption.TableIndex, values)
}

func (app *WorkloadApp) executeJSONZstdUpdate(conn *sql.Conn, task *updateTask) (sql.Result, error) {
	updateSQL, values := app.Workload.(*pjsonzstd.JSONZstdWorkload).BuildUpdateSqlWithValues(task.UpdateOption)
	task.generatedSQL = updateSQL
	return app.executeWithValues(conn, updateSQL, task.UpdateOption.TableIndex, values)
}

// executeSysbenchUpdate handles updates specific to sysbench workload
func (app *WorkloadApp) executeSysbenchUpdate(conn *sql.Conn, task *updateTask) (sql.Result, error) {
	updateSQL := app.Workload.(*psysbench.SysbenchWorkload).BuildUpdateSqlWithConn(conn, task.UpdateOption)
	if updateSQL == "" {
		return nil, nil
	}
	task.generatedSQL = updateSQL
	return app.execute(conn, updateSQL, task.TableIndex)
}

// executeRegularUpdate handles updates for non-bank2 workloads
func (app *WorkloadApp) executeRegularUpdate(conn *sql.Conn, task *updateTask) (sql.Result, error) {
	updateSQL := app.Workload.BuildUpdateSql(task.UpdateOption)
	if updateSQL == "" {
		return nil, nil
	}
	task.generatedSQL = updateSQL
	return app.execute(conn, updateSQL, task.TableIndex)
}

// handleUpdateError processes update operation errors
func (app *WorkloadApp) handleUpdateError(err error, task *updateTask) {
	// Truncate long SQL for logging
	plog.Info("update error",
		zap.Error(err),
		zap.String("sql", getSQLPreview(task.generatedSQL)))
}

// processUpdateResult handles the result of update operation
func (app *WorkloadApp) processUpdateResult(res sql.Result, task *updateTask) uint64 {
	if res == nil {
		return 0
	}

	cnt, err := res.RowsAffected()
	if err != nil {
		plog.Info("get rows affected error",
			zap.Error(err),
			zap.Int64("affectedRows", cnt),
			zap.Int("rowCount", task.Batch),
			zap.String("sql", getSQLPreview(task.generatedSQL)))
		app.Stats.ErrorCount.Add(1)
		return 0
	}

	if task.IsSpecialUpdate {
		plog.Info("update full table succeed",
			zap.Int("table", task.TableIndex),
			zap.Int64("affectedRows", cnt))
	}

	return uint64(cnt)
}
