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
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	tidbmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const selectActiveActiveSyncStatsSQL = "SELECT CONNECTION_ID(), @@tidb_active_active_sync_stats;"

type activeActiveSyncStats struct {
	ConflictSkipRows uint64 `json:"conflict_skip_rows"`
}

// CheckActiveActiveSyncStatsSupported checks whether downstream TiDB supports
// @@tidb_active_active_sync_stats session variable.
//
// This is a best-effort check used by metrics collection and should not affect
// replication correctness.
func CheckActiveActiveSyncStatsSupported(ctx context.Context, db *sql.DB) (bool, error) {
	row := db.QueryRowContext(ctx, "SELECT @@tidb_active_active_sync_stats;")
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
// Note: @@tidb_active_active_sync_stats is a session-scoped variable. Since sql.DB
// can reuse sessions across different writers, we must key the last observed value
// by CONNECTION_ID() to avoid double counting.
type ActiveActiveSyncStatsCollector struct {
	keyspace   string
	changefeed string

	conflictSkipRows prometheus.Counter

	mu                       sync.Mutex
	lastConflictSkipRowsByID map[uint64]uint64
}

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

func (c *ActiveActiveSyncStatsCollector) ObserveConflictSkipRows(connID uint64, current uint64) {
	var delta uint64
	c.mu.Lock()
	last, ok := c.lastConflictSkipRowsByID[connID]
	if ok {
		if current >= last {
			delta = current - last
		} else {
			log.Warn("unexcepted tidb_active_active_sync_stats decrease",
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

func (c *ActiveActiveSyncStatsCollector) Close() {
	// Reset the series on sink rebuild.
	metrics.ActiveActiveConflictSkipRowsCounter.DeleteLabelValues(c.keyspace, c.changefeed)
}

type activeActiveSyncStatsQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func queryActiveActiveSyncStats(
	ctx context.Context,
	q activeActiveSyncStatsQuerier,
) (uint64, uint64, error) {
	row := q.QueryRowContext(ctx, selectActiveActiveSyncStatsSQL)
	var (
		connID int64
		raw    sql.NullString
	)
	if err := row.Scan(&connID, &raw); err != nil {
		return 0, 0, errors.Trace(err)
	}
	if !raw.Valid || raw.String == "" {
		return uint64(connID), 0, errors.New("empty tidb_active_active_sync_stats value")
	}

	var parsed activeActiveSyncStats
	if err := json.Unmarshal([]byte(raw.String), &parsed); err != nil {
		return uint64(connID), 0, errors.Annotate(err, "unmarshal tidb_active_active_sync_stats")
	}
	return uint64(connID), parsed.ConflictSkipRows, nil
}

func (w *Writer) shouldCollectActiveActiveSyncStats(now time.Time) bool {
	if w.activeActiveSyncStatsCollector == nil || w.activeActiveSyncStatsInterval <= 0 {
		return false
	}
	if w.lastActiveActiveSyncStatsCollectTime.IsZero() {
		return true
	}
	return now.Sub(w.lastActiveActiveSyncStatsCollectTime) >= w.activeActiveSyncStatsInterval
}

func (w *Writer) maybeQueryActiveActiveSyncStats(
	writeTimeout time.Duration,
	q activeActiveSyncStatsQuerier,
) {
	now := time.Now()
	if !w.shouldCollectActiveActiveSyncStats(now) {
		return
	}

	// Throttle by time to avoid querying on every flush.
	w.lastActiveActiveSyncStatsCollectTime = now

	ctx, cancel := context.WithTimeout(w.ctx, writeTimeout)
	defer cancel()

	connID, conflictSkipRows, err := queryActiveActiveSyncStats(ctx, q)
	if err != nil {
		log.Warn("failed to query tidb_active_active_sync_stats",
			zap.String("keyspace", w.ChangefeedID.Keyspace()),
			zap.String("changefeed", w.ChangefeedID.Name()),
			zap.Int("writerID", w.id),
			zap.Error(err))
		return
	}

	w.activeActiveSyncStatsCollector.ObserveConflictSkipRows(connID, conflictSkipRows)
}
