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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type dmlSessionStats struct {
	// connID is the downstream connection ID used as a stable key in the
	// active-active sync stats collector.
	connID uint64

	// lastCollect is the last time we attempted the stats query. It is updated on
	// attempt (even if the query fails) to avoid tight retry loops.
	lastCollect time.Time
}

// dmlSession holds a writer-owned downstream session for DML execution.
//
// Note: sql.Conn is not safe for concurrent use. This struct serializes DML execution
// and background maintenance (stats query and idle close) on the same session.
type dmlSession struct {
	mu   sync.Mutex
	conn *sql.Conn

	// lastActive is updated after a successful DML execution via withConn.
	// It is used by the background loop to close idle sessions.
	lastActive time.Time

	// idleTimeout is the threshold used by the background loop to decide whether
	// the session can be closed. A non-positive value disables idle closing.
	idleTimeout time.Duration

	stats dmlSessionStats
}

// NewDMLSession creates a dmlSession that will close the underlying connection
// after it has been idle for at least idleTimeout.
func NewDMLSession(idleTimeout time.Duration) *dmlSession {
	return &dmlSession{
		idleTimeout: idleTimeout,
	}
}

// withConn executes fn with the session-owned connection while holding the session lock.
//
// The lock is held during fn to guarantee that the underlying sql.Conn is never
// used concurrently by DML execution and background maintenance.
func (s *dmlSession) withConn(
	w *Writer,
	writeTimeout time.Duration,
	fn func(conn *sql.Conn) error,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.getOrCreateLocked(w, writeTimeout)
	if err != nil {
		return err
	}
	if err := fn(conn); err != nil {
		// fn must best-effort clean up any explicit transaction state it started on conn
		// (e.g. rollback) before returning an error. This method discards the session
		// handle on error to avoid reusing a connection with uncertain state.
		// Discard the session on error to avoid reusing a session with unknown txn state.
		s.closeLocked(w)
		return err
	}
	s.lastActive = time.Now()
	return nil
}

// CheckStats performs best-effort maintenance for the current session:
//   - query active-active sync stats at a fixed interval
//   - close the session when it has been idle for too long
//
// It is called periodically from runDMLConnLoop and shares the same lock as DML
// execution to ensure the sql.Conn is never used concurrently.
func (s *dmlSession) CheckStats(w *Writer, now time.Time, writeTimeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
		return
	}

	s.tryQueryActiveActiveSyncStatsLocked(w, now, writeTimeout, s.conn)
	if s.shouldCloseLocked(now) {
		// Give up pending stats collection after idle timeout.
		s.closeLocked(w)
	}
}

func (s *dmlSession) close(w *Writer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked(w)
}

func (s *dmlSession) shouldCloseLocked(now time.Time) bool {
	if s.conn == nil {
		return false
	}
	if s.idleTimeout <= 0 || s.lastActive.IsZero() {
		return false
	}
	return now.Sub(s.lastActive) >= s.idleTimeout
}

// closeLocked closes the session connection and clears per-connection state in
// the active-active stats collector, if any.
func (s *dmlSession) closeLocked(w *Writer) {
	if s.conn == nil {
		return
	}
	_ = s.conn.Close()
	s.conn = nil
	s.lastActive = time.Time{}

	if w.activeActiveSyncStatsCollector != nil && s.stats.connID != 0 {
		w.activeActiveSyncStatsCollector.ForgetConn(s.stats.connID)
	}
	s.stats = dmlSessionStats{}
}

// getOrCreateLocked returns the existing session connection, or creates a new
// one on demand.
//
// Any failure during active-active stats baseline query is treated as best-effort:
// stats should not block DML execution, so the connection is kept alive and the
// error is only logged.
func (s *dmlSession) getOrCreateLocked(w *Writer, writeTimeout time.Duration) (*sql.Conn, error) {
	if s.conn != nil {
		return s.conn, nil
	}

	conn, err := w.db.Conn(w.ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.conn = conn
	s.lastActive = time.Time{}
	s.stats = dmlSessionStats{}

	// Baseline @@tidb_cdc_active_active_sync_stats for this session. The first successful
	// baseline query seeds the collector without increasing the counter.
	if w.activeActiveSyncStatsCollector != nil && w.activeActiveSyncStatsInterval > 0 {
		ctx, cancel := context.WithTimeout(w.ctx, writeTimeout)
		defer cancel()

		connID, conflictSkipRows, err := queryActiveActiveSyncStats(ctx, conn)
		if err != nil {
			log.Info("failed to query tidb_cdc_active_active_sync_stats baseline",
				zap.String("keyspace", w.ChangefeedID.Keyspace()),
				zap.String("changefeed", w.ChangefeedID.Name()),
				zap.Int("writerID", w.id),
				zap.Error(err))
			return conn, nil
		}

		w.activeActiveSyncStatsCollector.ObserveConflictSkipRows(connID, conflictSkipRows)
		s.stats.connID = connID
		s.stats.lastCollect = time.Now()
	}
	return conn, nil
}

// shouldCollectActiveActiveSyncStatsLocked decides whether it's time to query
// @@tidb_cdc_active_active_sync_stats again for this session.
func (s *dmlSession) shouldCollectActiveActiveSyncStatsLocked(w *Writer, now time.Time) bool {
	if w.activeActiveSyncStatsCollector == nil ||
		w.activeActiveSyncStatsInterval <= 0 ||
		s.conn == nil {
		return false
	}
	if s.stats.lastCollect.IsZero() {
		return true
	}
	return now.Sub(s.stats.lastCollect) >= w.activeActiveSyncStatsInterval
}

// tryQueryActiveActiveSyncStatsLocked runs the stats query and updates the collector.
func (s *dmlSession) tryQueryActiveActiveSyncStatsLocked(
	w *Writer,
	now time.Time,
	writeTimeout time.Duration,
	conn *sql.Conn,
) {
	if !s.shouldCollectActiveActiveSyncStatsLocked(w, now) {
		return
	}

	s.stats.lastCollect = now

	ctx, cancel := context.WithTimeout(w.ctx, writeTimeout)
	defer cancel()

	connID, conflictSkipRows, err := queryActiveActiveSyncStats(ctx, conn)
	if err != nil {
		log.Info("failed to query tidb_cdc_active_active_sync_stats",
			zap.String("keyspace", w.ChangefeedID.Keyspace()),
			zap.String("changefeed", w.ChangefeedID.Name()),
			zap.Int("writerID", w.id),
			zap.Error(err))
		return
	}

	w.activeActiveSyncStatsCollector.ObserveConflictSkipRows(connID, conflictSkipRows)
	s.stats.connID = connID
}

func (w *Writer) runDMLConnLoop() {
	// The loop frequency is determined by the smallest positive interval among:
	//   - the idle timeout for closing the session
	//   - the stats collection interval
	//
	// The ticker runs at half of that interval to bound overshoot when lastActive
	// is updated just after a tick.
	tickInterval := w.dmlSession.idleTimeout
	if w.activeActiveSyncStatsCollector != nil &&
		w.activeActiveSyncStatsInterval > 0 &&
		w.activeActiveSyncStatsInterval < tickInterval {
		tickInterval = w.activeActiveSyncStatsInterval
	}
	if tickInterval <= 0 {
		return
	}

	ticker := time.NewTicker(tickInterval / 2)
	defer ticker.Stop()

	writeTimeout, _ := time.ParseDuration(w.cfg.WriteTimeout)
	writeTimeout += networkDriftDuration

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.dmlSession.CheckStats(w, time.Now(), writeTimeout)
		}
	}
}
