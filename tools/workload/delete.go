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
)

// deleteTask defines a task for deleting data
type deleteTask struct {
	schema.DeleteOption
	generatedSQL string
	// reserved for future use
	callback func()
}

// executeDeleteWorkers executes delete workers
func (app *WorkloadApp) executeDeleteWorkers(deleteConcurrency int, wg *sync.WaitGroup) {
	if deleteConcurrency == 0 {
		plog.Info("skip delete workload",
			zap.String("action", app.Config.Action),
			zap.Int("totalThread", app.Config.Thread),
			zap.Float64("percentageForDelete", app.Config.PercentageForDelete))
		return
	}

	deleteTaskCh := make(chan deleteTask, deleteConcurrency)

	// generate delete tasks
	wg.Add(1)
	go func() {
		defer wg.Done()
		app.genDeleteTask(deleteTaskCh)
	}()

	// start delete workers
	wg.Add(deleteConcurrency)
	for i := range deleteConcurrency {
		db := app.DBManager.GetDB()

		go func(workerID int) {
			defer func() {
				plog.Info("delete worker exited", zap.Int("worker", workerID))
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

			plog.Info("start delete worker", zap.Int("worker", workerID))

			for {
				flushedRows, err := app.runTransaction(conn, func() (uint64, error) {
					return app.doDeleteOnce(conn, deleteTaskCh)
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
					plog.Info("delete worker failed, retrying", zap.Int("worker", workerID), zap.Error(err))
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

// genDeleteTask generates delete tasks
func (app *WorkloadApp) genDeleteTask(output chan deleteTask) {
	for {
		tableIndex := rand.Intn(app.Config.TableCount) + app.Config.TableStartIndex
		task := deleteTask{
			DeleteOption: schema.DeleteOption{
				TableIndex: tableIndex,
				Batch:      app.Config.BatchSize,
				RangeNum:   app.Config.RangeNum,
			},
		}
		output <- task
	}
}

func (app *WorkloadApp) doDeleteOnce(conn *sql.Conn, input chan deleteTask) (uint64, error) {
	task := <-input
	return app.processDeleteTask(conn, &task)
}

// processDeleteTask handles a single delete task
func (app *WorkloadApp) processDeleteTask(conn *sql.Conn, task *deleteTask) (uint64, error) {
	// Execute delete and get result
	res, err := app.executeDelete(conn, task)
	if err != nil {
		app.handleDeleteError(err, task)
		return 0, err
	}

	// Process delete result
	affectedRows := app.processDeleteResult(res, task)

	// Execute callback if exists
	if task.callback != nil {
		task.callback()
	}

	return affectedRows, nil
}

// executeDelete performs the actual delete operation
func (app *WorkloadApp) executeDelete(conn *sql.Conn, task *deleteTask) (sql.Result, error) {
	deleteSQL := app.Workload.BuildDeleteSql(task.DeleteOption)
	if deleteSQL == "" {
		return nil, nil
	}
	task.generatedSQL = deleteSQL
	return app.execute(conn, deleteSQL, task.TableIndex)
}

// handleDeleteError processes delete operation errors
func (app *WorkloadApp) handleDeleteError(err error, task *deleteTask) {
	// Truncate long SQL for logging
	plog.Info("delete error",
		zap.Error(err),
		zap.String("sql", getSQLPreview(task.generatedSQL)))
}

// processDeleteResult handles the result of delete operation
func (app *WorkloadApp) processDeleteResult(res sql.Result, task *deleteTask) uint64 {
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
	return uint64(cnt)
}
