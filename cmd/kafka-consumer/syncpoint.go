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
	"net/url"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	mysqlcfg "github.com/pingcap/ticdc/pkg/sink/mysql"
	"go.uber.org/zap"
)

const consumerSyncpointTable = "consumer_syncpoint_v1"

type consumerSyncpointStore interface {
	Init(ctx context.Context) (uint64, error)
	Write(ctx context.Context, primaryTs uint64) error
	Close() error
}

type consumerSyncpointStoreConfig struct {
	downstreamURI string
	consumerID    string
	topic         string
	retention     time.Duration
}

type mysqlConsumerSyncpointStore struct {
	db         *sql.DB
	consumerID string
	topic      string
	retention  time.Duration
}

func newMySQLConsumerSyncpointStore(
	ctx context.Context,
	cfg consumerSyncpointStoreConfig,
) (consumerSyncpointStore, error) {
	sinkURI, err := url.Parse(cfg.downstreamURI)
	if err != nil {
		return nil, cerrors.WrapError(cerrors.ErrSinkURIInvalid, err)
	}
	scheme := config.GetScheme(sinkURI)
	if !config.IsMySQLCompatibleScheme(scheme) {
		return nil, cerrors.ErrInvalidReplicaConfig.FastGenByArgs(
			"consumer syncpoint requires a tidb or mysql downstream")
	}
	changefeedID := commonType.NewChangeFeedIDWithName(cfg.consumerID, commonType.DefaultKeyspaceName)
	changefeedConfig := &config.ChangefeedConfig{
		ChangefeedID: changefeedID,
		SinkURI:      cfg.downstreamURI,
		SinkConfig:   config.GetDefaultReplicaConfig().Sink,
	}
	changefeedConfig.SinkConfig.TiDBSourceID = 1
	_, db, err := mysqlcfg.NewMysqlConfigAndDB(ctx, changefeedID, sinkURI, changefeedConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &mysqlConsumerSyncpointStore{
		db:         db,
		consumerID: cfg.consumerID,
		topic:      cfg.topic,
		retention:  cfg.retention,
	}, nil
}

func (s *mysqlConsumerSyncpointStore) Init(ctx context.Context) (uint64, error) {
	if err := s.createTable(ctx); err != nil {
		return 0, err
	}
	query := fmt.Sprintf(
		"SELECT primary_ts FROM %s.%s WHERE consumer_id = ? AND topic = ? ORDER BY CAST(primary_ts AS UNSIGNED) DESC LIMIT 1",
		filter.TiCDCSystemSchema,
		consumerSyncpointTable,
	)
	var primaryTs string
	err := s.db.QueryRowContext(ctx, query, s.consumerID, s.topic).Scan(&primaryTs)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, cerrors.WrapError(cerrors.ErrMySQLTxnError, errors.Trace(err))
	}
	ts, err := strconv.ParseUint(primaryTs, 10, 64)
	if err != nil {
		return 0, cerrors.WrapError(cerrors.ErrMySQLTxnError, errors.Trace(err))
	}
	return ts, nil
}

func (s *mysqlConsumerSyncpointStore) createTable(ctx context.Context) error {
	createDatabaseQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", filter.TiCDCSystemSchema)
	if _, err := s.db.ExecContext(ctx, createDatabaseQuery); err != nil {
		return cerrors.WrapError(cerrors.ErrMySQLTxnError, errors.Trace(err))
	}
	createTableQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s
	(
		ticdc_cluster_id varchar(255),
		consumer_id varchar(255),
		topic varchar(255),
		primary_ts varchar(18),
		secondary_ts varchar(18),
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		INDEX (created_at),
		PRIMARY KEY (consumer_id, topic, primary_ts)
	);`, filter.TiCDCSystemSchema, consumerSyncpointTable)
	if _, err := s.db.ExecContext(ctx, createTableQuery); err != nil {
		return cerrors.WrapError(cerrors.ErrMySQLTxnError, errors.Trace(err))
	}
	return nil
}

func (s *mysqlConsumerSyncpointStore) Write(ctx context.Context, primaryTs uint64) error {
	secondaryTs := "0"
	gotSecondaryTs := true
	if err := s.db.QueryRowContext(ctx, "select @@tidb_current_ts").Scan(&secondaryTs); err != nil {
		gotSecondaryTs = false
		log.Warn("get downstream tidb current ts failed, use zero secondary ts",
			zap.Uint64("primaryTs", primaryTs), zap.Error(err))
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return cerrors.WrapError(cerrors.ErrMySQLTxnError, errors.Trace(err))
	}
	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Warn("rollback consumer syncpoint transaction failed", zap.Error(rollbackErr))
			}
		}
	}()

	insertQuery := fmt.Sprintf(
		"INSERT IGNORE INTO %s.%s (ticdc_cluster_id, consumer_id, topic, primary_ts, secondary_ts) VALUES (?, ?, ?, ?, ?)",
		filter.TiCDCSystemSchema,
		consumerSyncpointTable,
	)
	if _, err = tx.ExecContext(ctx, insertQuery,
		config.GetGlobalServerConfig().ClusterID,
		s.consumerID,
		s.topic,
		strconv.FormatUint(primaryTs, 10),
		secondaryTs,
	); err != nil {
		return cerrors.WrapError(cerrors.ErrMySQLTxnError, errors.Trace(err))
	}

	if gotSecondaryTs {
		setExternalTsQuery := fmt.Sprintf("SET GLOBAL tidb_external_ts = %s", secondaryTs)
		if _, err = tx.ExecContext(ctx, setExternalTsQuery); err != nil {
			if cerrors.IsSyncPointIgnoreError(err) {
				log.Warn("set global external ts failed, ignore this error", zap.Error(err))
			} else {
				return cerrors.WrapError(cerrors.ErrMySQLTxnError, errors.Trace(err))
			}
		}
	}

	if s.retention > 0 {
		cleanupQuery := fmt.Sprintf(
			"DELETE IGNORE FROM %s.%s WHERE consumer_id = ? AND topic = ? AND created_at < (NOW() - INTERVAL %d SECOND)",
			filter.TiCDCSystemSchema,
			consumerSyncpointTable,
			int64(s.retention.Seconds()),
		)
		if _, err = tx.ExecContext(ctx, cleanupQuery, s.consumerID, s.topic); err != nil {
			log.Warn("cleanup stale consumer syncpoint records failed", zap.Error(err))
		}
	}

	if err = tx.Commit(); err != nil {
		return cerrors.WrapError(cerrors.ErrMySQLTxnError, errors.Trace(err))
	}
	committed = true
	return nil
}

func (s *mysqlConsumerSyncpointStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
