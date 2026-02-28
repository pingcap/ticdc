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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	plog "github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	randomModeTableSampleSize = 10
	ddlTaskChanMinBufSize     = 1024
)

type DDLTask struct {
	Type  DDLType
	Table TableName
}

type DDLRunner struct {
	app *WorkloadApp
	cfg *DDLConfig

	taskCh   chan DDLTask
	selector ddlTableSelector

	randomSchema string
}

func NewDDLRunner(app *WorkloadApp, cfg *DDLConfig) (*DDLRunner, error) {
	if app == nil || app.Config == nil || app.DBManager == nil {
		return nil, errors.New("ddl runner requires initialized app")
	}

	r := &DDLRunner{
		app: app,
		cfg: cfg,
	}

	bufSize := cfg.totalRate() * 2
	if bufSize < ddlTaskChanMinBufSize {
		bufSize = ddlTaskChanMinBufSize
	}
	r.taskCh = make(chan DDLTask, bufSize)

	switch cfg.Mode {
	case ddlModeFixed:
		defaultSchema := app.Config.DBName
		if app.Config.DBPrefix != "" && app.Config.DBNum > 0 {
			defaultSchema = ""
		}
		tables, err := parseTableList(cfg.Tables, defaultSchema)
		if err != nil {
			return nil, err
		}
		r.selector = newFixedTableSelector(tables)
	case ddlModeRandom:
		if app.Config.DBPrefix != "" || app.Config.DBNum != 1 {
			return nil, errors.New("ddl random mode only supports single database connection")
		}
		r.randomSchema = app.Config.DBName
		randomSelector := newRandomTableSelector()
		if err := r.refreshRandomTables(randomSelector); err != nil {
			return nil, err
		}
		r.selector = randomSelector
	default:
		return nil, errors.Errorf("unsupported ddl mode: %s", cfg.Mode)
	}

	return r, nil
}

func (r *DDLRunner) Start(wg *sync.WaitGroup) {
	if r.cfg.Mode == ddlModeRandom {
		r.startRandomTableRefresh()
	}

	r.startTaskSchedulers()
	r.startWorkers(wg)
}

func (r *DDLRunner) startTaskSchedulers() {
	r.startTypeScheduler(ddlAddColumn, r.cfg.RatePerMinute.AddColumn)
	r.startTypeScheduler(ddlDropColumn, r.cfg.RatePerMinute.DropColumn)
	r.startTypeScheduler(ddlAddIndex, r.cfg.RatePerMinute.AddIndex)
	r.startTypeScheduler(ddlDropIndex, r.cfg.RatePerMinute.DropIndex)
	r.startTypeScheduler(ddlTruncateTable, r.cfg.RatePerMinute.TruncateTable)
}

func (r *DDLRunner) startTypeScheduler(ddlType DDLType, perMinute int) {
	if perMinute <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			for i := 0; i < perMinute; i++ {
				table, ok := r.selector.Next()
				if !ok {
					r.app.Stats.DDLSkipped.Add(1)
					continue
				}
				r.taskCh <- DDLTask{Type: ddlType, Table: table}
			}
			<-ticker.C
		}
	}()
}

func (r *DDLRunner) startRandomTableRefresh() {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		randomSelector := r.selector.(*randomTableSelector)
		for range ticker.C {
			if err := r.refreshRandomTables(randomSelector); err != nil {
				plog.Info("refresh random tables failed", zap.Error(err))
			}
		}
	}()
}

func (r *DDLRunner) refreshRandomTables(selector *randomTableSelector) error {
	dbs := r.app.DBManager.GetDBs()
	if len(dbs) == 0 {
		return errors.New("no database connections available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableNames, err := fetchBaseTables(ctx, dbs[0].DB, r.randomSchema)
	if err != nil {
		return err
	}

	if len(tableNames) == 0 {
		selector.Update(nil)
		plog.Info("no tables found for ddl random mode", zap.String("schema", r.randomSchema))
		return nil
	}

	sampled := sampleStrings(tableNames, randomModeTableSampleSize)
	tables := make([]TableName, 0, len(sampled))
	for _, name := range sampled {
		tables = append(tables, TableName{Schema: r.randomSchema, Name: name})
	}
	selector.Update(tables)

	plog.Info("random tables refreshed",
		zap.String("schema", r.randomSchema),
		zap.Int("tableCount", len(tables)))
	return nil
}

func fetchBaseTables(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	const query = `
SELECT table_name
FROM information_schema.tables
WHERE table_schema = ?
  AND table_type = 'BASE TABLE'`
	rows, err := db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, errors.Annotate(err, "query tables from information_schema failed")
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, errors.Annotate(err, "scan table name failed")
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Annotate(err, "iterate table names failed")
	}
	return tables, nil
}

func sampleStrings(in []string, n int) []string {
	if n <= 0 || len(in) == 0 {
		return nil
	}
	if len(in) <= n {
		out := make([]string, len(in))
		copy(out, in)
		return out
	}

	indices := rand.Perm(len(in))[:n]
	out := make([]string, 0, n)
	for _, idx := range indices {
		out = append(out, in[idx])
	}
	return out
}

type ddlTableSelector interface {
	Next() (TableName, bool)
}

type fixedTableSelector struct {
	tables []TableName
	next   atomic.Uint64
}

func newFixedTableSelector(tables []TableName) *fixedTableSelector {
	return &fixedTableSelector{tables: tables}
}

func (s *fixedTableSelector) Next() (TableName, bool) {
	if len(s.tables) == 0 {
		return TableName{}, false
	}
	i := int(s.next.Add(1)-1) % len(s.tables)
	return s.tables[i], true
}

type randomTableSelector struct {
	mu     sync.RWMutex
	tables []TableName
}

func newRandomTableSelector() *randomTableSelector {
	return &randomTableSelector{}
}

func (s *randomTableSelector) Update(tables []TableName) {
	s.mu.Lock()
	s.tables = tables
	s.mu.Unlock()
}

func (s *randomTableSelector) Next() (TableName, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.tables) == 0 {
		return TableName{}, false
	}
	return s.tables[rand.Intn(len(s.tables))], true
}

func parseTableList(rawTables []string, defaultSchema string) ([]TableName, error) {
	seen := make(map[string]struct{}, len(rawTables))
	out := make([]TableName, 0, len(rawTables))
	for _, raw := range rawTables {
		table, err := ParseTableName(raw, defaultSchema)
		if err != nil {
			return nil, err
		}
		key := table.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, table)
	}
	return out, nil
}
