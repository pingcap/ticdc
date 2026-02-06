package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	plog "github.com/pingcap/log"
	"go.uber.org/zap"
	pfastslow "workload/schema/fastslow"
)

func (app *WorkloadApp) startFastSlowDDLWorkers(wg *sync.WaitGroup) {
	wl, ok := app.Workload.(*pfastslow.FastSlowWorkload)
	if !ok {
		plog.Warn("fast slow ddl workload is not initialized")
		return
	}

	if app.Config.SlowTableCount <= 0 {
		return
	}

	for _, db := range app.DBManager.GetDBs() {
		wg.Add(1)
		go func(db *DBWrapper) {
			defer wg.Done()
			app.runFastSlowDDLWorker(db, wl)
		}(db)
	}
}

func (app *WorkloadApp) runFastSlowDDLWorker(db *DBWrapper, wl *pfastslow.FastSlowWorkload) {
	if delay := app.Config.SlowDDLStartDelay; delay > 0 {
		time.Sleep(delay)
	}

	ticker := time.NewTicker(app.Config.SlowDDLInterval)
	defer ticker.Stop()

	conn := app.getConnWithRetry(db)
	defer func() {
		_ = conn.Close()
	}()

	perTableAdded := make([]int, app.Config.SlowTableCount)
	nextSlowTable := 0
	var lastNoTableLog time.Time

	for range ticker.C {
		slowTableIndex, ok := pickSlowTable(perTableAdded, app.Config.SlowDDLMaxColumns, nextSlowTable)
		if !ok {
			if time.Since(lastNoTableLog) > time.Minute {
				lastNoTableLog = time.Now()
				plog.Info("all slow tables reached ddl max columns, skip ddl",
					zap.Int("slowTableCount", app.Config.SlowTableCount),
					zap.Int("slowDDLMaxColumns", app.Config.SlowDDLMaxColumns),
				)
			}
			continue
		}
		nextSlowTable = (slowTableIndex + 1) % app.Config.SlowTableCount

		tableIndex := app.Config.TableStartIndex + app.Config.FastTableCount + slowTableIndex
		tableName := wl.TableName(tableIndex)
		columnName := fmt.Sprintf("ddl_c_%d_%04d", time.Now().UnixNano(), rand.Intn(10000))
		ddlSQL := app.buildAddColumnDDL(tableName, columnName)

		_, err := app.execute(conn, ddlSQL, tableIndex)
		if err != nil {
			app.Stats.ErrorCount.Add(1)
			if app.isConnectionError(err) {
				_ = conn.Close()
				conn = app.getConnWithRetry(db)
				continue
			}
			plog.Info("execute slow table ddl failed", zap.Error(err), zap.String("table", tableName))
			continue
		}
		perTableAdded[slowTableIndex]++
	}
}

func (app *WorkloadApp) getConnWithRetry(db *DBWrapper) *sql.Conn {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		conn, err := db.DB.Conn(ctx)
		cancel()
		if err == nil {
			return conn
		}
		plog.Info("get connection failed, wait 5 seconds and retry", zap.Error(err), zap.String("db", db.Name))
		time.Sleep(5 * time.Second)
	}
}

func (app *WorkloadApp) buildAddColumnDDL(tableName, columnName string) string {
	ddlSQL := fmt.Sprintf("alter table `%s` add column `%s` int not null default 0", tableName, columnName)
	if opts := strings.TrimSpace(app.Config.SlowDDLOptions); opts != "" {
		ddlSQL += " " + opts
	}
	return ddlSQL
}

func pickSlowTable(perTableAdded []int, maxColumns int, start int) (int, bool) {
	if len(perTableAdded) == 0 {
		return 0, false
	}
	for i := 0; i < len(perTableAdded); i++ {
		idx := (start + i) % len(perTableAdded)
		if maxColumns <= 0 || perTableAdded[idx] < maxColumns {
			return idx, true
		}
	}
	return 0, false
}
