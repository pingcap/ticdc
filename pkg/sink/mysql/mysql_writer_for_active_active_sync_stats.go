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

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	tidbmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const selectActiveActiveSyncStatsSQL = "SELECT CONNECTION_ID(), @@tidb_cdc_active_active_sync_stats;"

type activeActiveSyncStats struct {
	ConflictSkipRows uint64 `json:"conflict_skip_rows"`
}

// CheckActiveActiveSyncStatsSupported checks whether downstream TiDB supports
// @@tidb_cdc_active_active_sync_stats session variable.
func CheckActiveActiveSyncStatsSupported(ctx context.Context, db *sql.DB) (bool, error) {
	row := db.QueryRowContext(ctx, "SELECT @@tidb_cdc_active_active_sync_stats;")
	var v sql.NullString
	if err := row.Scan(&v); err != nil {
		if mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError); ok &&
			mysqlErr.Number == tidbmysql.ErrUnknownSystemVariable {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// ActiveActiveSyncStatsCollector accumulates TiDB active-active conflict statistics
// for a changefeed.
//
// Note: @@tidb_cdc_active_active_sync_stats is a session-scoped variable. Since sql.DB
// can reuse sessions across different writers, we must key the last observed value
// by CONNECTION_ID() to avoid double counting.
type ActiveActiveSyncStatsCollector struct {
	keyspace   string
	changefeed string

	conflictSkipRows prometheus.Counter

	mu                       sync.Mutex
	lastConflictSkipRowsByID map[uint64]uint64
}

// NewActiveActiveSyncStatsCollector creates a metric collector for active-active
// conflict statistics of a changefeed.
func NewActiveActiveSyncStatsCollector(changefeedID common.ChangeFeedID) *ActiveActiveSyncStatsCollector {
	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	return &ActiveActiveSyncStatsCollector{
		keyspace:                 keyspace,
		changefeed:               changefeed,
		conflictSkipRows:         metrics.ActiveActiveConflictSkipRowsCounter.WithLabelValues(keyspace, changefeed),
		lastConflictSkipRowsByID: make(map[uint64]uint64),
	}
}

// ObserveConflictSkipRows records an observation from a downstream connection.
//
// The session variable is monotonically increasing within a connection. Since sql.DB
// may reuse connections across different writers, this method computes a delta per
// CONNECTION_ID() to avoid double counting.
func (c *ActiveActiveSyncStatsCollector) ObserveConflictSkipRows(connID uint64, current uint64) {
	var delta uint64
	c.mu.Lock()
	last, ok := c.lastConflictSkipRowsByID[connID]
	if ok {
		if current >= last {
			delta = current - last
		} else {
			log.Warn("unexpected tidb_cdc_active_active_sync_stats decrease",
				zap.String("keyspace", c.keyspace),
				zap.String("changefeed", c.changefeed),
				zap.Uint64("connID", connID),
				zap.Uint64("lastConflictSkipRows", last),
				zap.Uint64("currentConflictSkipRows", current))
		}
	}
	c.lastConflictSkipRowsByID[connID] = current
	c.mu.Unlock()

	if delta == 0 {
		return
	}
	c.conflictSkipRows.Add(float64(delta))
}

// ForgetConn removes the last observed statistics for a connection. This prevents
// the internal map from growing unbounded when sessions are closed and recreated.
func (c *ActiveActiveSyncStatsCollector) ForgetConn(connID uint64) {
	c.mu.Lock()
	delete(c.lastConflictSkipRowsByID, connID)
	c.mu.Unlock()
}

// Close releases metric series held by this collector.
func (c *ActiveActiveSyncStatsCollector) Close() {
	// Reset the series on sink rebuild.
	metrics.ActiveActiveConflictSkipRowsCounter.DeleteLabelValues(c.keyspace, c.changefeed)
}

// queryActiveActiveSyncStats queries CONNECTION_ID() and the session variable
// @@tidb_cdc_active_active_sync_stats from the given connection.
func queryActiveActiveSyncStats(
	ctx context.Context,
	conn *sql.Conn,
) (uint64, uint64, error) {
	row := conn.QueryRowContext(ctx, selectActiveActiveSyncStatsSQL)
	var (
		connID int64
		raw    sql.NullString
	)
	if err := row.Scan(&connID, &raw); err != nil {
		return 0, 0, errors.Trace(err)
	}
	if !raw.Valid || raw.String == "" {
		return uint64(connID), 0, errors.New("empty tidb_cdc_active_active_sync_stats value")
	}

	var parsed activeActiveSyncStats
	if err := json.Unmarshal([]byte(raw.String), &parsed); err != nil {
		return uint64(connID), 0, errors.Annotate(err, "unmarshal tidb_cdc_active_active_sync_stats")
	}
	log.Debug("queried tidb_cdc_active_active_sync_stats",
		zap.Int64("connID", connID),
		zap.Uint64("conflictSkipRows", parsed.ConflictSkipRows))
	return uint64(connID), parsed.ConflictSkipRows, nil
}
