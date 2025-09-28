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
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	plog "github.com/pingcap/log"
	"go.uber.org/zap"
)

// DBWrapper represents a database connection with usage tracking
type DBWrapper struct {
	DB         *sql.DB // The underlying database connection
	Name       string  // Database name
	UsageCount int32   // Number of threads currently using this connection
}

// DBManager manage database connections and statement cache
type DBManager struct {
	Config    *WorkloadConfig
	DBs       []*DBWrapper // Array of managed database connections
	currentDB int32        // Counter for Round Robin selection
	StmtCache sync.Map     // map[string]*sql.Stmt
}

// SetupConnections establishes database connections
func (m *DBManager) SetupConnections() error {
	plog.Info("start to setup databases")
	defer func() {
		plog.Info("setup databases finished")
	}()

	if m.Config.DBPrefix != "" {
		return m.setupMultipleDatabases()
	} else {
		return m.setupSingleDatabase()
	}
}

// setupMultipleDatabases sets up connections to multiple databases
func (m *DBManager) setupMultipleDatabases() error {
	m.DBs = make([]*DBWrapper, m.Config.DBNum)
	for i := 0; i < m.Config.DBNum; i++ {
		dbName := fmt.Sprintf("%s%d", m.Config.DBPrefix, i+1)
		db, err := m.createDBConnection(dbName)
		if err != nil {
			plog.Info("create the sql client failed", zap.Error(err))
			continue
		}
		m.configureDBConnection(db)
		m.DBs[i] = &DBWrapper{
			DB:         db,
			Name:       dbName,
			UsageCount: 0,
		}
	}

	if len(m.DBs) == 0 {
		return fmt.Errorf("no mysql client was created successfully")
	}

	return nil
}

// setupSingleDatabase sets up connection to a single database
func (m *DBManager) setupSingleDatabase() error {
	m.DBs = make([]*DBWrapper, 1)
	db, err := m.createDBConnection(m.Config.DBName)
	if err != nil {
		return fmt.Errorf("create the sql client failed: %w", err)
	}
	m.configureDBConnection(db)
	m.DBs[0] = &DBWrapper{
		DB:         db,
		Name:       m.Config.DBName,
		UsageCount: 0,
	}
	return nil
}

// GetDB returns a database connection using Round Robin algorithm
func (m *DBManager) GetDB() *DBWrapper {
	if len(m.DBs) == 0 {
		return nil
	}

	// Atomically get the next DB index using Round Robin
	next := atomic.AddInt32(&m.currentDB, 1) % int32(len(m.DBs))
	db := m.DBs[next]

	// Increment the usage counter
	atomic.AddInt32(&db.UsageCount, 1)

	return db
}

// createDBConnection creates a database connection
func (m *DBManager) createDBConnection(dbName string) (*sql.DB, error) {
	plog.Info("create db connection", zap.String("dbName", dbName))
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local&maxAllowedPacket=1073741824&multiStatements=true",
		m.Config.DBUser, m.Config.DBPassword, m.Config.DBHost, m.Config.DBPort, dbName)
	return sql.Open("mysql", dsn)
}

// configureDBConnection configures a database connection
func (m *DBManager) configureDBConnection(db *sql.DB) {
	db.SetMaxIdleConns(512)
	db.SetMaxOpenConns(512)
	db.SetConnMaxLifetime(time.Minute)
}

// CloseAll closes all database connections
func (m *DBManager) CloseAll() {
	for _, db := range m.DBs {
		if err := db.DB.Close(); err != nil {
			plog.Error("failed to close database connection", zap.Error(err))
		}
	}
}

func (m *DBManager) GetDBs() []*DBWrapper {
	return m.DBs
}
