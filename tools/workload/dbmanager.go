package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	plog "github.com/pingcap/log"
	"go.uber.org/zap"
)

// DBManager manage database connections and statement cache
type DBManager struct {
	Config      *WorkloadConfig
	Connections []*sql.DB
	StmtCache   sync.Map // map[string]*sql.Stmt
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
	m.Connections = make([]*sql.DB, m.Config.DBNum)
	for i := 0; i < m.Config.DBNum; i++ {
		dbName := fmt.Sprintf("%s%d", m.Config.DBPrefix, i+1)
		db, err := m.createDBConnection(dbName)
		if err != nil {
			plog.Info("create the sql client failed", zap.Error(err))
			continue
		}
		m.configureDBConnection(db)
		m.Connections[i] = db
	}

	if len(m.Connections) == 0 {
		return fmt.Errorf("no mysql client was created successfully")
	}

	return nil
}

// setupSingleDatabase sets up connection to a single database
func (m *DBManager) setupSingleDatabase() error {
	m.Connections = make([]*sql.DB, 1)
	db, err := m.createDBConnection(m.Config.DBName)
	if err != nil {
		return fmt.Errorf("create the sql client failed: %w", err)
	}
	m.configureDBConnection(db)
	m.Connections[0] = db
	return nil
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
	for _, db := range m.Connections {
		if err := db.Close(); err != nil {
			plog.Error("failed to close database connection", zap.Error(err))
		}
	}
}
