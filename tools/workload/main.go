// Copyright 2024 PingCAP, Inc.
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
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	plog "github.com/pingcap/log"
	"go.uber.org/zap"
	"workload/schema"
	pbank "workload/schema/bank"
	pbank2 "workload/schema/bank2"
	"workload/schema/bankupdate"
	pcrawler "workload/schema/crawler"
	"workload/schema/largerow"
	"workload/schema/shop"
	psysbench "workload/schema/sysbench"
	puuu "workload/schema/uuu"
)

var (
	logFile  string
	logLevel string

	tableCount      int
	tableStartIndex int
	thread          int
	batchSize       int

	dbHost     string
	dbPort     int
	dbUser     string
	dbPassword string
	dbName     string

	// totalRowCount is the total number of rows that will be inserted.
	totalRowCount uint64
	// flushedRowCount is the number of rows that have been flushed.
	flushedRowCount atomic.Uint64
	// queryCount is the number of queries that have been executed.
	queryCount atomic.Uint64
	// errCount is the number of errors that have occurred.
	errCount atomic.Uint64

	workloadType string

	skipCreateTable bool
	onlyDDL         bool

	rowSize       int
	largeRowSize  int
	largeRowRatio float64

	action              string
	percentageForUpdate float64

	dbNum    int
	dbPrefix string

	updateLargeColumnSize int
)

const (
	bank     = "bank"
	sysbench = "sysbench"
	largeRow = "large_row"
	shopItem = "shop_item"
	uuu      = "uuu"
	crawler  = "crawler"
	// for gf case, at most support table count = 2. Here only 2 tables in this cases.
	// And each insert sql contains 200 batch, each update sql only contains 1 batch.
	bank2      = "bank2"
	bankUpdate = "bank_update"
)

// Add a prepared statement cache
var stmtCache sync.Map // map[string]*sql.Stmt

func init() {
	flag.StringVar(&dbPrefix, "db-prefix", "", "the prefix of the database name")
	flag.IntVar((&dbNum), "db-num", 1, "the number of databases")
	flag.IntVar(&tableCount, "table-count", 1, "table count of the workload")
	flag.IntVar(&tableStartIndex, "table-start-index", 0, "table start index, sbtest<index>")
	flag.IntVar(&thread, "thread", 16, "total thread of the workload")
	flag.IntVar(&batchSize, "batch-size", 10, "batch size of each insert/update/delete")
	flag.Uint64Var(&totalRowCount, "total-row-count", 1000000000, "the total row count of the workload, default is 1 billion")
	flag.Float64Var(&percentageForUpdate, "percentage-for-update", 0, "percentage for update: [0, 1.0]")
	flag.BoolVar(&skipCreateTable, "skip-create-table", false, "do not create tables")
	flag.StringVar(&action, "action", "prepare", "action of the workload: [prepare, insert, update, delete, write, cleanup]")
	flag.StringVar(&workloadType, "workload-type", "sysbench", "workload type: [bank, sysbench, large_row, shop_item, uuu, bank2, bank_update, crawler]")
	flag.StringVar(&dbHost, "database-host", "127.0.0.1", "database host")
	flag.StringVar(&dbUser, "database-user", "root", "database user")
	flag.StringVar(&dbPassword, "database-password", "", "database password")
	flag.StringVar(&dbName, "database-db-name", "test", "database db name")
	flag.IntVar(&dbPort, "database-port", 4000, "database port")
	flag.BoolVar(&onlyDDL, "only-ddl", false, "only generate ddl")
	flag.StringVar(&logFile, "log-file", "workload.log", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log file path")
	// For large row workload
	flag.IntVar(&rowSize, "row-size", 10240, "the size of each row")
	flag.IntVar(&largeRowSize, "large-row-size", 1024*1024, "the size of the large row")
	flag.Float64Var(&largeRowRatio, "large-ratio", 0.0, "large row ratio in the each transaction")
	flag.IntVar(&updateLargeColumnSize, "update-large-column-size", 1024, "the size of the large column to update")
	flag.Parse()
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	plog.Info("start to run workload")
	validateFlags()

	dbs := setupDatabases()
	workload := createWorkload()

	// Start monitoring
	go reportMetrics()

	// Execute workload
	wg := &sync.WaitGroup{}
	executeWorkload(dbs, workload, wg)

	// Cleanup
	wg.Wait()
	closeDatabases(dbs)
}

func validateFlags() {
	if flags := flag.Args(); len(flags) > 0 {
		plog.Panic(fmt.Sprintf("unparsed flags: %v", flags))
	}
}

func setupDatabases() []*sql.DB {
	plog.Info("start to setup databases")
	defer func() {
		plog.Info("setup databases finished")
	}()

	dbs := make([]*sql.DB, dbNum)

	if dbPrefix != "" {
		dbs = setupMultipleDatabases()
	} else {
		dbs = setupSingleDatabase()
	}

	if len(dbs) == 0 {
		plog.Panic("no mysql client was created successfully")
	}

	return dbs
}

func setupMultipleDatabases() []*sql.DB {
	dbs := make([]*sql.DB, dbNum)
	for i := 0; i < dbNum; i++ {
		dbName := fmt.Sprintf("%s%d", dbPrefix, i+1)
		db, err := createDBConnection(dbName)
		if err != nil {
			plog.Info("create the sql client failed", zap.Error(err))
			continue
		}
		configureDBConnection(db)
		dbs[i] = db
	}
	return dbs
}

func setupSingleDatabase() []*sql.DB {
	dbs := make([]*sql.DB, 1)
	db, err := createDBConnection(dbName)
	if err != nil {
		plog.Panic("create the sql client failed", zap.Error(err))
	}
	configureDBConnection(db)
	dbs[0] = db
	dbNum = 1
	return dbs
}

func createDBConnection(dbName string) (*sql.DB, error) {
	plog.Info("create db connection", zap.String("dbName", dbName))
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&maxAllowedPacket=1073741824&multiStatements=true",
		dbUser, dbPassword, dbHost, dbPort, dbName)
	return sql.Open("mysql", dsn)
}

func configureDBConnection(db *sql.DB) {
	db.SetMaxIdleConns(512)
	db.SetMaxOpenConns(512)
	db.SetConnMaxLifetime(time.Minute)
}

func createWorkload() schema.Workload {
	plog.Info("start to create workload")
	defer func() {
		plog.Info("create workload finished")
	}()

	var workload schema.Workload
	switch workloadType {
	case bank:
		workload = pbank.NewBankWorkload()
	case sysbench:
		workload = psysbench.NewSysbenchWorkload()
	case largeRow:
		workload = largerow.NewLargeRowWorkload(rowSize, largeRowSize, largeRowRatio)
	case shopItem:
		workload = shop.NewShopItemWorkload(totalRowCount, rowSize)
	case uuu:
		workload = puuu.NewUUUWorkload()
	case crawler:
		workload = pcrawler.NewCrawlerWorkload()
	case bank2:
		workload = pbank2.NewBank2Workload()
	case bankUpdate:
		workload = bankupdate.NewBankUpdateWorkload(totalRowCount, updateLargeColumnSize)
	default:
		plog.Panic("unsupported workload type", zap.String("workload", workloadType))
	}
	return workload
}

func executeWorkload(dbs []*sql.DB, workload schema.Workload, wg *sync.WaitGroup) {
	updateConcurrency := int(float64(thread) * percentageForUpdate)
	insertConcurrency := thread - updateConcurrency

	plog.Info("database info", zap.Int("dbCount", dbNum), zap.Int("tableCount", tableCount))

	if !skipCreateTable && action == "prepare" {
		handlePrepareAction(dbs, insertConcurrency, workload, wg)
		return
	}

	if onlyDDL {
		return
	}

	handleWorkloadExecution(dbs, insertConcurrency, updateConcurrency, workload, wg)
}

func handlePrepareAction(dbs []*sql.DB, insertConcurrency int, workload schema.Workload, _ *sync.WaitGroup) {
	plog.Info("start to create tables", zap.Int("tableCount", tableCount))
	wg := &sync.WaitGroup{}
	for _, db := range dbs {
		wg.Add(1)
		go initTables(wg, db, workload)
	}
	wg.Wait()
	plog.Info("All dbs create tables finished")
	if totalRowCount != 0 {
		executeInsertWorkers(dbs, insertConcurrency, workload, wg)
	}
}

func handleWorkloadExecution(dbs []*sql.DB, insertConcurrency, updateConcurrency int, workload schema.Workload, wg *sync.WaitGroup) {
	plog.Info("start running workload",
		zap.String("workloadType", workloadType),
		zap.Float64("largeRatio", largeRowRatio),
		zap.Int("totalThread", thread),
		zap.Int("batchSize", batchSize),
		zap.String("action", action),
	)

	if action == "write" || action == "insert" {
		executeInsertWorkers(dbs, insertConcurrency, workload, wg)
	}

	if action == "write" || action == "update" {
		executeUpdateWorkers(dbs, updateConcurrency, workload, wg)
	}
}

func executeInsertWorkers(dbs []*sql.DB, insertConcurrency int, workload schema.Workload, wg *sync.WaitGroup) {
	wg.Add(insertConcurrency)
	for i := 0; i < insertConcurrency; i++ {
		db := dbs[i%len(dbs)]

		conn, err := db.Conn(context.Background())
		// TODO: support recreate connection when the worker failed.
		if err != nil {
			plog.Info("get connection failed", zap.Error(err))
			return
		}
		go func(workerID int) {
			defer func() {
				plog.Info("insert worker exited", zap.Int("worker", workerID))
				wg.Done()
			}()
			plog.Info("start insert worker", zap.Int("worker", workerID))
			doInsert(conn, workload)
		}(i)
	}
}

func executeUpdateWorkers(dbs []*sql.DB, updateConcurrency int, workload schema.Workload, wg *sync.WaitGroup) {
	if updateConcurrency == 0 {
		plog.Info("skip update workload",
			zap.String("action", action),
			zap.Int("totalThread", thread),
			zap.Float64("percentageForUpdate", percentageForUpdate))
		return
	}

	updateTaskCh := make(chan updateTask, updateConcurrency)
	wg.Add(updateConcurrency + 1) // +1 for task generator

	for i := 0; i < updateConcurrency; i++ {
		db := dbs[i%len(dbs)]

		conn, err := db.Conn(context.Background())
		// TODO: support recreate connection when the worker failed.
		if err != nil {
			plog.Info("get connection failed", zap.Error(err))
			return
		}
		go func(workerID int) {
			defer func() {
				plog.Info("update worker exited", zap.Int("worker", workerID))
				wg.Done()
			}()
			plog.Info("start update worker", zap.Int("worker", workerID))
			doUpdate(conn, workload, updateTaskCh)
		}(i)
	}

	go func() {
		defer wg.Done()
		genUpdateTask(updateTaskCh)
	}()
}

func closeDatabases(dbs []*sql.DB) {
	for _, db := range dbs {
		if err := db.Close(); err != nil {
			plog.Error("failed to close database connection", zap.Error(err))
		}
	}
}

var createdTableNum atomic.Int32

// initTables create tables if not exists
func initTables(wg *sync.WaitGroup, db *sql.DB, workload schema.Workload) error {
	defer wg.Done()
	for tableIndex := range tableCount {
		sql := workload.BuildCreateTableStatement(tableIndex + tableStartIndex)
		if _, err := db.Exec(sql); err != nil {
			err := errors.Annotate(err, "create table failed")
			plog.Error("create table failed", zap.Error(err))
		}
		createdTableNum.Add(1)
	}
	plog.Info("create tables finished")
	return nil
}

type updateTask struct {
	schema.UpdateOption
	// reserved for future use
	cb func()
}

func genUpdateTask(output chan updateTask) {
	for {
		j := rand.Intn(tableCount) + tableStartIndex
		// TODO: add more randomness.
		task := updateTask{
			UpdateOption: schema.UpdateOption{
				Table: j,
				Batch: batchSize,
			},
		}
		output <- task
	}
}

func doUpdate(conn *sql.Conn, workload schema.Workload, input chan updateTask) {
	var err error
	var res sql.Result
	var updateSql string
	for task := range input {
		if workloadType == bank2 {
			task.UpdateOption.Batch = 1
			updateSql, values := workload.(*pbank2.Bank2Workload).BuildUpdateSqlWithValues(task.UpdateOption)
			res, err = executeWithValues(conn, updateSql, workload, task.UpdateOption.Table, values)
		} else {
			updateSql := workload.BuildUpdateSql(task.UpdateOption)
			if updateSql == "" {
				continue
			}
			res, err = execute(conn, updateSql, workload, task.Table)
		}
		if err != nil {
			if len(updateSql) > 20 {
				updateSql = updateSql[:20] + "..."
			}
			plog.Info("update error", zap.Error(err), zap.String("sql", updateSql))
			errCount.Add(1)
		}
		if res != nil {
			cnt, err := res.RowsAffected()
			if err != nil || cnt < int64(task.Batch) {
				plog.Info("get rows affected error", zap.Error(err), zap.Int64("affectedRows", cnt), zap.Int("rowCount", task.Batch), zap.String("sql", updateSql))
				errCount.Add(1)
			}
			flushedRowCount.Add(uint64(cnt))
			if task.IsSpecialUpdate {
				plog.Info("update full table succeed, row count %d\n", zap.Int("table", task.Table), zap.Int64("affectedRows", cnt))
			}
		} else {
			plog.Info("update result is nil")
		}
		if task.cb != nil {
			task.cb()
		}
	}
}

func doInsert(conn *sql.Conn, workload schema.Workload) {
	for {
		j := rand.Intn(tableCount) + tableStartIndex
		var err error

		switch workloadType {
		case uuu:
			insertSql, values := workload.(*puuu.UUUWorkload).BuildInsertSqlWithValues(j, batchSize)
			_, err = executeWithValues(conn, insertSql, workload, j, values)
		case bank2:
			insertSql, values := workload.(*pbank2.Bank2Workload).BuildInsertSqlWithValues(j, batchSize)
			_, err = executeWithValues(conn, insertSql, workload, j, values)
		default:
			insertSql := workload.BuildInsertSql(j, batchSize)
			_, err = execute(conn, insertSql, workload, j)
		}

		if err != nil {
			plog.Info("insert error", zap.Error(err))
			errCount.Add(1)
			continue
		}
		flushedRowCount.Add(uint64(batchSize))
	}
}

func execute(conn *sql.Conn, sql string, workload schema.Workload, n int) (sql.Result, error) {
	queryCount.Add(1)
	res, err := conn.ExecContext(context.Background(), sql)
	if err != nil {
		if !strings.Contains(err.Error(), "Error 1146") {
			plog.Info("insert error", zap.Error(err))
			return res, err
		}
		// if table not exists, we create it
		_, err := conn.ExecContext(context.Background(), workload.BuildCreateTableStatement(n))
		if err != nil {
			plog.Info("create table error: ", zap.Error(err))
			return res, err
		}
		_, err = conn.ExecContext(context.Background(), sql)
		return res, err
	}
	return res, nil
}

// TODO: redesign a better stmtCacheKey
type stmtCacheKey struct {
	conn *sql.Conn
	sql  string
}

func executeWithValues(conn *sql.Conn, sqlStr string, workload schema.Workload, n int, values []interface{}) (sql.Result, error) {
	queryCount.Add(1)

	// Try to get prepared statement from cache
	if stmt, ok := stmtCache.Load(stmtCacheKey{
		conn: conn,
		sql:  sqlStr,
	}); ok {
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
		_, err := conn.ExecContext(context.Background(), workload.BuildCreateTableStatement(n))
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
	stmtCache.Store(stmtCacheKey{
		conn: conn,
		sql:  sqlStr,
	}, stmt)

	// Execute the prepared statement
	return stmt.Exec(values...)
}

// reportMetrics prints throughput statistics every 5 seconds
func reportMetrics() {
	plog.Info("start to report metrics")
	const (
		reportInterval = 5 * time.Second
	)

	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	var (
		lastQueryCount uint64
		lastFlushed    uint64
		lastErrorCount uint64
	)

	for range ticker.C {
		stats := calculateStats(lastQueryCount, lastFlushed, lastErrorCount, reportInterval)
		// Update last values for next iteration
		lastQueryCount = stats.queryCount
		lastFlushed = stats.flushedRowCount
		lastErrorCount = stats.errCount
		// Print statistics
		printStats(stats)

		if stats.flushedRowCount > totalRowCount {
			plog.Info("total row count reached", zap.Uint64("flushedRowCount", stats.flushedRowCount), zap.Uint64("totalRowCount", totalRowCount))
			os.Exit(0)
		}
	}
}

type statistics struct {
	queryCount      uint64
	flushedRowCount uint64
	errCount        uint64
	// QPS
	qps int
	// row/s
	rps int
	// error/s
	eps int
}

func calculateStats(
	lastQueryCount,
	lastFlushed,
	lastErrors uint64,
	reportInterval time.Duration,
) statistics {
	currentFlushed := flushedRowCount.Load()
	currentErrors := errCount.Load()
	currentQueryCount := queryCount.Load()

	return statistics{
		queryCount:      currentQueryCount,
		flushedRowCount: currentFlushed,
		errCount:        currentErrors,
		qps:             int(currentQueryCount-lastQueryCount) / int(reportInterval.Seconds()),
		rps:             int(currentFlushed-lastFlushed) / int(reportInterval.Seconds()),
		eps:             int(currentErrors-lastErrors) / int(reportInterval.Seconds()),
	}
}

func printStats(stats statistics) {
	status := fmt.Sprintf(
		"Total Write Rows: %d, Total Queries: %d, Total Created Tables: %d, Total Errors: %d, QPS: %d, Row/s: %d, Error/s: %d",
		stats.flushedRowCount,
		stats.queryCount,
		createdTableNum.Load(),
		stats.errCount,
		stats.qps,
		stats.rps,
		stats.eps,
	)
	plog.Info(status)
}
