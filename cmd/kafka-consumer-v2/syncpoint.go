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
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	mysqlsink "github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const consumerSyncpointTableName = "consumer_syncpoint_v1"

type syncpointManager struct {
	enabled  bool
	consumer string
	topic    string

	interval  time.Duration
	retention time.Duration

	db            *sql.DB
	lastSyncedTs  uint64
	nextTs        uint64
	lastCleanTime time.Time
}

func newSyncpointManager(ctx context.Context, o *option) (*syncpointManager, error) {
	m := &syncpointManager{
		enabled:   o.enableSyncpoint,
		consumer:  o.groupID,
		topic:     o.topic,
		interval:  o.syncpointInterval,
		retention: o.syncpointRetention,
	}
	if !m.enabled {
		return m, nil
	}

	changefeedID := commonType.NewChangeFeedIDWithName("kafka-consumer-v2-syncpoint", commonType.DefaultKeyspaceName)
	cfg := &config.ChangefeedConfig{
		ChangefeedID: changefeedID,
		SinkURI:      o.downstreamURI,
		SinkConfig:   o.sinkConfig,
	}
	_, db, err := mysqlsink.NewMysqlConfigAndDB(ctx, changefeedID, o.downstreamURIParsed, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m.db = db
	if err = m.init(ctx); err != nil {
		_ = db.Close()
		return nil, errors.Trace(err)
	}
	return m, nil
}

func (m *syncpointManager) Close() {
	if m.db != nil {
		if err := m.db.Close(); err != nil {
			log.Warn("close syncpoint db failed", zap.Error(err))
		}
	}
}

func (m *syncpointManager) init(ctx context.Context) error {
	if err := m.createTable(ctx); err != nil {
		return errors.Trace(err)
	}
	lastSynced, err := m.loadLastSyncedTs(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	m.lastSyncedTs = lastSynced
	if lastSynced != 0 {
		m.nextTs = syncpoint.CalculateStartSyncPointTs(lastSynced, m.interval, true)
	}
	m.lastCleanTime = time.Now()
	log.Info("consumer syncpoint initialized",
		zap.String("consumerID", m.consumer),
		zap.String("topic", m.topic),
		zap.Uint64("lastSyncedTs", m.lastSyncedTs),
		zap.Uint64("nextSyncpointTs", m.nextTs))
	return nil
}

func (m *syncpointManager) createTable(ctx context.Context) error {
	if _, err := m.db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+filter.TiCDCSystemSchema); err != nil {
		return errors.Trace(err)
	}
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s
	(
		consumer_id varchar(255) NOT NULL,
		topic varchar(255) NOT NULL,
		primary_ts varchar(18) NOT NULL,
		secondary_ts varchar(18) NOT NULL,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		INDEX idx_created_at (created_at),
		PRIMARY KEY (consumer_id, topic, primary_ts)
	);`, filter.TiCDCSystemSchema, consumerSyncpointTableName)
	_, err := m.db.ExecContext(ctx, query)
	return errors.Trace(err)
}

func (m *syncpointManager) loadLastSyncedTs(ctx context.Context) (uint64, error) {
	query := fmt.Sprintf(`SELECT primary_ts FROM %s.%s
WHERE consumer_id = ? AND topic = ?
ORDER BY CAST(primary_ts AS UNSIGNED) DESC
LIMIT 1`, filter.TiCDCSystemSchema, consumerSyncpointTableName)
	var primaryTsStr string
	err := m.db.QueryRowContext(ctx, query, m.consumer, m.topic).Scan(&primaryTsStr)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	primaryTs, err := strconv.ParseUint(primaryTsStr, 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return primaryTs, nil
}

func (m *syncpointManager) EnsureNextTs(globalWatermark uint64) {
	if !m.enabled || m.nextTs != 0 || globalWatermark == 0 {
		return
	}
	m.nextTs = syncpoint.CalculateStartSyncPointTs(globalWatermark, m.interval, false)
	log.Info("consumer syncpoint schedule initialized from watermark",
		zap.Uint64("globalWatermark", globalWatermark),
		zap.Uint64("nextSyncpointTs", m.nextTs),
		zap.Duration("interval", m.interval))
}

func (m *syncpointManager) AdvanceNextTs() {
	m.nextTs = oracle.GoTimeToTS(oracle.GetTimeFromTS(m.nextTs).Add(m.interval))
}

func (m *syncpointManager) Write(ctx context.Context, primaryTs uint64) (uint64, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, errors.Trace(err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	var secondaryTs string
	if err = tx.QueryRowContext(ctx, "select @@tidb_current_ts").Scan(&secondaryTs); err != nil {
		return 0, errors.Trace(err)
	}
	if secondaryTs == "" || secondaryTs == "0" {
		return 0, errors.New("invalid downstream secondary ts")
	}
	secondaryTsUint, err := strconv.ParseUint(secondaryTs, 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}

	query := fmt.Sprintf(`INSERT INTO %s.%s
(consumer_id, topic, primary_ts, secondary_ts)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE secondary_ts = secondary_ts`,
		filter.TiCDCSystemSchema, consumerSyncpointTableName)
	if _, err = tx.ExecContext(ctx, query,
		m.consumer, m.topic,
		strconv.FormatUint(primaryTs, 10),
		secondaryTs,
	); err != nil {
		return 0, errors.Trace(err)
	}

	if err = m.cleanExpiredIfNeeded(ctx, tx); err != nil {
		return 0, errors.Trace(err)
	}

	if err = tx.Commit(); err != nil {
		return 0, errors.Trace(err)
	}
	rollback = false
	m.lastSyncedTs = primaryTs
	return secondaryTsUint, nil
}

func (m *syncpointManager) cleanExpiredIfNeeded(ctx context.Context, tx *sql.Tx) error {
	if m.retention <= 0 || time.Since(m.lastCleanTime) < m.retention {
		return nil
	}
	query := fmt.Sprintf(`DELETE IGNORE FROM %s.%s
WHERE consumer_id = ? AND topic = ?
AND created_at < (NOW() - INTERVAL %.2f SECOND)`,
		filter.TiCDCSystemSchema,
		consumerSyncpointTableName,
		m.retention.Seconds())
	if _, err := tx.ExecContext(ctx, query, m.consumer, m.topic); err != nil {
		return errors.Trace(err)
	}
	m.lastCleanTime = time.Now()
	return nil
}
