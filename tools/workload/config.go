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
	"flag"
	"fmt"
	"sync"
)

// WorkloadConfig saves all the configurations for the workload
type WorkloadConfig struct {
	// Database related
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	DBPrefix   string
	DBNum      int

	// Workload related
	WorkloadType        string
	TableCount          int
	TableStartIndex     int
	Thread              int
	BatchSize           int
	TotalRowCount       uint64
	PercentageForUpdate float64
	PercentageForDelete float64

	// Action control
	Action          string
	SkipCreateTable bool
	OnlyDDL         bool

	// Special workload config
	RowSize       int
	LargeRowSize  int
	LargeRowRatio float64
	// Only for bankupdate workload
	UpdateLargeColumnSize int
	// For sysbench workload
	RangeNum int

	// Log related
	LogFile  string
	LogLevel string
}

// NewWorkloadConfig creates a new config with default values
func NewWorkloadConfig() *WorkloadConfig {
	return &WorkloadConfig{
		// Default database config
		DBHost:     "127.0.0.1",
		DBPort:     4000,
		DBUser:     "root",
		DBPassword: "",
		DBName:     "test",
		DBPrefix:   "",
		DBNum:      1,

		// Default workload config
		WorkloadType:        "sysbench",
		TableCount:          1,
		TableStartIndex:     0,
		Thread:              16,
		BatchSize:           10,
		TotalRowCount:       1000000000,
		PercentageForUpdate: 0,
		PercentageForDelete: 0,

		// Action control
		Action:          "prepare",
		SkipCreateTable: false,
		OnlyDDL:         false,

		// For large row workload
		RowSize:       10240,
		LargeRowSize:  1024 * 1024,
		LargeRowRatio: 0.0,

		// For bankupdate workload
		UpdateLargeColumnSize: 1024,

		// For sysbench workload
		RangeNum: 5,

		// Log related
		LogFile:  "workload.log",
		LogLevel: "info",
	}
}

// ParseFlags parses command line flags and updates config
func (c *WorkloadConfig) ParseFlags() error {
	flag.StringVar(&c.DBPrefix, "db-prefix", c.DBPrefix, "the prefix of the database name")
	flag.IntVar(&c.DBNum, "db-num", c.DBNum, "the number of databases")
	flag.IntVar(&c.TableCount, "table-count", c.TableCount, "table count of the workload")
	flag.IntVar(&c.TableStartIndex, "table-start-index", c.TableStartIndex, "table start index, sbtest<index>")
	flag.IntVar(&c.Thread, "thread", c.Thread, "total thread of the workload")
	flag.IntVar(&c.BatchSize, "batch-size", c.BatchSize, "batch size of each insert/update/delete")
	flag.Uint64Var(&c.TotalRowCount, "total-row-count", c.TotalRowCount, "the total row count of the workload, default is 1 billion")
	flag.Float64Var(&c.PercentageForUpdate, "percentage-for-update", c.PercentageForUpdate, "percentage for update: [0, 1.0]")
	flag.Float64Var(&c.PercentageForDelete, "percentage-for-delete", c.PercentageForDelete, "percentage for delete: [0, 1.0]")
	flag.BoolVar(&c.SkipCreateTable, "skip-create-table", c.SkipCreateTable, "do not create tables")
	flag.StringVar(&c.Action, "action", c.Action, "action of the workload: [prepare, insert, update, delete, write, cleanup]")
	flag.StringVar(&c.WorkloadType, "workload-type", c.WorkloadType, "workload type: [bank, sysbench, large_row, shop_item, uuu, bank2, bank_update, crawler]")
	flag.StringVar(&c.DBHost, "database-host", c.DBHost, "database host")
	flag.StringVar(&c.DBUser, "database-user", c.DBUser, "database user")
	flag.StringVar(&c.DBPassword, "database-password", c.DBPassword, "database password")
	flag.StringVar(&c.DBName, "database-db-name", c.DBName, "database db name")
	flag.IntVar(&c.DBPort, "database-port", c.DBPort, "database port")
	flag.BoolVar(&c.OnlyDDL, "only-ddl", c.OnlyDDL, "only generate ddl")
	flag.StringVar(&c.LogFile, "log-file", c.LogFile, "log file path")
	flag.StringVar(&c.LogLevel, "log-level", c.LogLevel, "log file path")
	// For large row workload
	flag.IntVar(&c.RowSize, "row-size", c.RowSize, "the size of each row")
	flag.IntVar(&c.LargeRowSize, "large-row-size", c.LargeRowSize, "the size of the large row")
	flag.Float64Var(&c.LargeRowRatio, "large-ratio", c.LargeRowRatio, "large row ratio in the each transaction")
	flag.IntVar(&c.UpdateLargeColumnSize, "update-large-column-size", c.UpdateLargeColumnSize, "the size of the large column to update")
	// For sysbench workload
	flag.IntVar(&c.RangeNum, "range-num", c.RangeNum, "the number of ranges for sysbench workload")

	flag.Parse()

	// Validate command line arguments
	if flags := flag.Args(); len(flags) > 0 {
		return fmt.Errorf("unparsed flags: %v", flags)
	}

	// Validate percentage constraints
	if c.PercentageForUpdate+c.PercentageForDelete > 1.0 {
		return fmt.Errorf("PercentageForUpdate (%.2f) + PercentageForDelete (%.2f) must be <= 1.0",
			c.PercentageForUpdate, c.PercentageForDelete)
	}

	return nil
}

// NewDBManager creates a new database manager
func NewDBManager(config *WorkloadConfig) (*DBManager, error) {
	manager := &DBManager{
		Config:    config,
		StmtCache: sync.Map{},
	}

	if err := manager.SetupConnections(); err != nil {
		return nil, err
	}

	return manager, nil
}
