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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	plog "github.com/pingcap/log"
	"go.uber.org/zap"
	"workload/schema"
	pbank "workload/schema/bank"
	pbank2 "workload/schema/bank2"
	"workload/schema/bankupdate"
	pcrawler "workload/schema/crawler"
	pdc "workload/schema/dc"
	"workload/schema/largerow"
	"workload/schema/shop"
	psysbench "workload/schema/sysbench"
	puuu "workload/schema/uuu"
)

// WorkloadExecutor executes the workload and collects statistics
type WorkloadExecutor struct {
	Config    *WorkloadConfig
	DBManager *DBManager
	Workload  schema.Workload

	// statistics
	FlushedRowCount atomic.Uint64
	QueryCount      atomic.Uint64
	ErrorCount      atomic.Uint64
}

// WorkloadStats saves the statistics of the workload
type WorkloadStats struct {
	FlushedRowCount atomic.Uint64
	QueryCount      atomic.Uint64
	ErrorCount      atomic.Uint64
	CreatedTableNum atomic.Int32
}

// WorkloadApp is the main structure of the application
type WorkloadApp struct {
	Config    *WorkloadConfig
	DBManager *DBManager
	Workload  schema.Workload
	Stats     *WorkloadStats
}

const (
	bank       = "bank"
	sysbench   = "sysbench"
	largeRow   = "large_row"
	shopItem   = "shop_item"
	uuu        = "uuu"
	crawler    = "crawler"
	bank2      = "bank2"
	bankUpdate = "bank_update"
	dc         = "dc"
)

// stmtCacheKey is used as the key for statement cache
type stmtCacheKey struct {
	conn *sql.Conn
	sql  string
}

// NewWorkloadApp creates a new workload application
func NewWorkloadApp(config *WorkloadConfig) *WorkloadApp {
	return &WorkloadApp{
		Config: config,
		Stats:  &WorkloadStats{},
	}
}

// Initialize initializes the workload application
func (app *WorkloadApp) Initialize() error {
	// set database connection
	dbManager, err := NewDBManager(app.Config)
	if err != nil {
		return err
	}
	app.DBManager = dbManager

	// create workload
	app.Workload = app.createWorkload()

	return nil
}

// createWorkload creates a workload based on configuration
func (app *WorkloadApp) createWorkload() schema.Workload {
	plog.Info("start to create workload")
	defer func() {
		plog.Info("create workload finished")
	}()

	var workload schema.Workload
	switch app.Config.WorkloadType {
	case bank:
		workload = pbank.NewBankWorkload()
	case sysbench:
		workload = psysbench.NewSysbenchWorkload()
	case largeRow:
		workload = largerow.NewLargeRowWorkload(app.Config.RowSize, app.Config.LargeRowSize, app.Config.LargeRowRatio)
	case shopItem:
		workload = shop.NewShopItemWorkload(app.Config.TotalRowCount, app.Config.RowSize)
	case uuu:
		workload = puuu.NewUUUWorkload()
	case crawler:
		workload = pcrawler.NewCrawlerWorkload()
	case bank2:
		workload = pbank2.NewBank2Workload()
	case bankUpdate:
		workload = bankupdate.NewBankUpdateWorkload(app.Config.TotalRowCount, app.Config.UpdateLargeColumnSize)
	case dc:
		workload = pdc.NewDCWorkload()
	default:
		plog.Panic("unsupported workload type", zap.String("workload", app.Config.WorkloadType))
	}
	return workload
}

// Execute executes the workload
func (app *WorkloadApp) Execute() error {
	wg := &sync.WaitGroup{}
	err := app.executeWorkload(wg)
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

// executeWorkload executes the workload
func (app *WorkloadApp) executeWorkload(wg *sync.WaitGroup) error {
	deleteConcurrency := int(float64(app.Config.Thread) * app.Config.PercentageForDelete)
	updateConcurrency := int(float64(app.Config.Thread) * app.Config.PercentageForUpdate)
	insertConcurrency := app.Config.Thread - deleteConcurrency - updateConcurrency

	plog.Info("database info",
		zap.Int("dbCount", len(app.DBManager.GetDBs())),
		zap.Int("tableCount", app.Config.TableCount))

	if !app.Config.SkipCreateTable && app.Config.Action == "prepare" {
		app.handlePrepareAction(insertConcurrency, wg)
		return nil
	}

	if app.Config.OnlyDDL {
		return nil
	}

	app.handleWorkloadExecution(insertConcurrency, updateConcurrency, deleteConcurrency, wg)
	return nil
}

// handlePrepareAction handles the prepare action
func (app *WorkloadApp) handlePrepareAction(insertConcurrency int, mainWg *sync.WaitGroup) {
	plog.Info("start to create tables", zap.Int("tableCount", app.Config.TableCount))
	wg := &sync.WaitGroup{}
	for _, db := range app.DBManager.GetDBs() {
		wg.Add(1)
		go func(db *DBWrapper) {
			defer wg.Done()
			app.initTables(db.DB)
		}(db)
	}
	wg.Wait()
	plog.Info("All dbs create tables finished")
	if app.Config.TotalRowCount != 0 {
		app.executeInsertWorkers(insertConcurrency, wg)
	}
}

// handleWorkloadExecution handles the workload execution
func (app *WorkloadApp) handleWorkloadExecution(insertConcurrency, updateConcurrency, deleteConcurrency int, wg *sync.WaitGroup) {
	plog.Info("start running workload",
		zap.String("workloadType", app.Config.WorkloadType),
		zap.Float64("largeRatio", app.Config.LargeRowRatio),
		zap.Int("totalThread", app.Config.Thread),
		zap.Int("insertConcurrency", insertConcurrency),
		zap.Int("updateConcurrency", updateConcurrency),
		zap.Int("deleteConcurrency", deleteConcurrency),
		zap.Int("batchSize", app.Config.BatchSize),
		zap.String("action", app.Config.Action),
	)

	if app.Config.Action == "write" || app.Config.Action == "insert" {
		app.executeInsertWorkers(insertConcurrency, wg)
	}

	if app.Config.Action == "write" || app.Config.Action == "update" {
		app.executeUpdateWorkers(updateConcurrency, wg)
	}

	if app.Config.Action == "write" || app.Config.Action == "delete" {
		app.executeDeleteWorkers(deleteConcurrency, wg)
	}
}

// initTables initializes tables
func (app *WorkloadApp) initTables(db *sql.DB) {
	for tableIndex := 0; tableIndex < app.Config.TableCount; tableIndex++ {
		sql := app.Workload.BuildCreateTableStatement(tableIndex + app.Config.TableStartIndex)
		if _, err := db.Exec(sql); err != nil {
			err := errors.Annotate(err, "create table failed")
			plog.Error("create table failed", zap.Error(err))
		}
		app.Stats.CreatedTableNum.Add(1)
	}
	plog.Info("create tables finished")
}

// executeInsertWorkers executes insert workers
func (app *WorkloadApp) executeInsertWorkers(insertConcurrency int, wg *sync.WaitGroup) {
	wg.Add(insertConcurrency)
	var retryCount atomic.Uint64
	for i := range insertConcurrency {
		db := app.DBManager.GetDB()
		go func(workerID int) {
			defer func() {
				plog.Info("insert worker exited", zap.Int("worker", workerID))
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

			plog.Info("start insert worker to write data to db", zap.Int("worker", workerID), zap.String("db", db.Name))

			for {
				err = app.doInsert(conn)
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
					retryCount.Add(1)
					plog.Info("do insert error, retrying", zap.Int("worker", workerID), zap.String("db", db.Name), zap.Uint64("retryCount", retryCount.Load()), zap.Error(err))
					time.Sleep(time.Second * 2)
					continue
				}
			}
		}(i)
	}
}

// isConnectionError checks if the error is a connection-level error that requires reconnection
func (app *WorkloadApp) isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// Check for common connection-level errors
	return strings.Contains(errStr, "connection is already closed") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "invalid connection")
}

// doInsert performs insert operations
func (app *WorkloadApp) doInsert(conn *sql.Conn) error {
	for {
		j := rand.Intn(app.Config.TableCount) + app.Config.TableStartIndex
		var err error

		switch app.Config.WorkloadType {
		case uuu:
			insertSql, values := app.Workload.(*puuu.UUUWorkload).BuildInsertSqlWithValues(j, app.Config.BatchSize)
			_, err = app.executeWithValues(conn, insertSql, j, values)
		case bank2:
			insertSql, values := app.Workload.(*pbank2.Bank2Workload).BuildInsertSqlWithValues(j, app.Config.BatchSize)
			_, err = app.executeWithValues(conn, insertSql, j, values)
		default:
			insertSql := app.Workload.BuildInsertSql(j, app.Config.BatchSize)
			_, err = app.execute(conn, insertSql, j)
		}
		if err != nil {
			if strings.Contains(err.Error(), "connection is already closed") {
				plog.Info("connection is already closed", zap.Error(err))
				app.Stats.ErrorCount.Add(1)
				return err
			}

			plog.Info("do insert error", zap.Error(err))
			app.Stats.ErrorCount.Add(1)
			continue
		}
		app.Stats.FlushedRowCount.Add(uint64(app.Config.BatchSize))
	}
}

// execute executes a SQL statement
func (app *WorkloadApp) execute(conn *sql.Conn, sql string, tableIndex int) (sql.Result, error) {
	app.Stats.QueryCount.Add(1)
	res, err := conn.ExecContext(context.Background(), sql)
	if err != nil {
		if !strings.Contains(err.Error(), "Error 1146") {
			plog.Info("execute error", zap.Error(err), zap.String("sql", sql))
			return res, err
		}
		// if table not exists, we create it
		_, err := conn.ExecContext(context.Background(), app.Workload.BuildCreateTableStatement(tableIndex))
		if err != nil {
			plog.Info("create table error: ", zap.Error(err))
			return res, err
		}
		_, err = conn.ExecContext(context.Background(), sql)
		return res, err
	}
	return res, nil
}

// executeWithValues executes a SQL statement with values
func (app *WorkloadApp) executeWithValues(conn *sql.Conn, sqlStr string, n int, values []interface{}) (sql.Result, error) {
	app.Stats.QueryCount.Add(1)

	// Try to get prepared statement from cache
	key := stmtCacheKey{conn: conn, sql: sqlStr}
	if stmt, ok := app.DBManager.StmtCache.Load(key); ok {
		return stmt.(*sql.Stmt).Exec(values...)
	}

	// Prepare the statement
	stmt, err := conn.PrepareContext(context.Background(), sqlStr)
	if err != nil {
		if !strings.Contains(err.Error(), "Error 1146") {
			plog.Info("prepare error", zap.Error(err))
			return nil, err
		}
		// Create table if not exists
		_, err := conn.ExecContext(context.Background(), app.Workload.BuildCreateTableStatement(n))
		if err != nil {
			plog.Info("create table error: ", zap.Error(err))
			return nil, err
		}
		// Try prepare again
		stmt, err = conn.PrepareContext(context.Background(), sqlStr)
		if err != nil {
			return nil, err
		}
	}

	// Cache the prepared statement
	app.DBManager.StmtCache.Store(key, stmt)

	// Execute the prepared statement
	return stmt.Exec(values...)
}

// StartMetricsReporting starts reporting metrics
func (app *WorkloadApp) StartMetricsReporting() {
	go app.reportMetrics()
}

func getSQLPreview(sql string) string {
	if len(sql) > 512 {
		return sql[:512] + "..."
	}
	return sql
}
