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
	"fmt"
	"os"
	"time"

	plog "github.com/pingcap/log"
	"go.uber.org/zap"
)

// statistics represents the statistics of the workload
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

// calculateStats calculates the statistics
func (app *WorkloadApp) calculateStats(
	lastQueryCount,
	lastFlushed,
	lastErrors uint64,
	reportInterval time.Duration,
) statistics {
	currentFlushed := app.Stats.FlushedRowCount.Load()
	currentErrors := app.Stats.ErrorCount.Load()
	currentQueryCount := app.Stats.QueryCount.Load()

	return statistics{
		queryCount:      currentQueryCount,
		flushedRowCount: currentFlushed,
		errCount:        currentErrors,
		qps:             int(currentQueryCount-lastQueryCount) / int(reportInterval.Seconds()),
		rps:             int(currentFlushed-lastFlushed) / int(reportInterval.Seconds()),
		eps:             int(currentErrors-lastErrors) / int(reportInterval.Seconds()),
	}
}

// printStats prints the statistics
func (app *WorkloadApp) printStats(stats statistics) {
	status := fmt.Sprintf(
		"Total Write Rows: %d, Total Queries: %d, Total Created Tables: %d, Total Errors: %d, QPS: %d, Row/s: %d, Error/s: %d",
		stats.flushedRowCount,
		stats.queryCount,
		app.Stats.CreatedTableNum.Load(),
		stats.errCount,
		stats.qps,
		stats.rps,
		stats.eps,
	)
	plog.Info(status)
}

// reportMetrics prints the statistics every 5 seconds
func (app *WorkloadApp) reportMetrics() {
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
		stats := app.calculateStats(lastQueryCount, lastFlushed, lastErrorCount, reportInterval)
		// Update last values for next iteration
		lastQueryCount = stats.queryCount
		lastFlushed = stats.flushedRowCount
		lastErrorCount = stats.errCount
		// Print statistics
		app.printStats(stats)

		if stats.flushedRowCount > app.Config.TotalRowCount && app.Config.Action != "update" {
			plog.Info("total row count reached",
				zap.Uint64("flushedRowCount", stats.flushedRowCount),
				zap.Uint64("totalRowCount", app.Config.TotalRowCount))
			os.Exit(0)
		}
	}
}
