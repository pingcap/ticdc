// Copyright 2024 PingCAP, Inc.
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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/apperror"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

func (w *MysqlWriter) CreateSyncTable() error {
	database := filter.TiCDCSystemSchema
	query := `CREATE TABLE IF NOT EXISTS %s
	(
		ticdc_cluster_id varchar (255),
		changefeed varchar(255),
		primary_ts varchar(18),
		secondary_ts varchar(18),
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		INDEX (created_at),
		PRIMARY KEY (changefeed, primary_ts)
	);`
	query = fmt.Sprintf(query, filter.SyncPointTable)
	return w.CreateTable(database, filter.SyncPointTable, query)
}

func (w *MysqlWriter) FlushSyncPointEvent(event *commonEvent.SyncPointEvent) error {
	if !w.syncPointTableInit {
		// create sync point table if not exist
		err := w.CreateSyncTable()
		if err != nil {
			return errors.Trace(err)
		}
		w.syncPointTableInit = true
	}
	err := w.SendSyncPointEvent(event)
	if err != nil {
		return errors.Trace(err)
	}
	for _, callback := range event.PostTxnFlushed {
		callback()
	}
	return nil
}

func (w *MysqlWriter) SendSyncPointEvent(event *commonEvent.SyncPointEvent) error {
	tx, err := w.db.BeginTx(w.ctx, nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "sync table: begin Tx fail;"))
	}
	row := tx.QueryRow("select @@tidb_current_ts")
	var secondaryTs string
	err = row.Scan(&secondaryTs)
	if err != nil {
		log.Info("sync table: get tidb_current_ts err", zap.String("changefeed", w.ChangefeedID.String()))
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to write syncpoint table", zap.Error(err))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table; Failed to get tidb_current_ts;"))
	}
	// insert ts map
	var builder strings.Builder
	builder.WriteString("insert ignore into ")
	builder.WriteString(filter.TiCDCSystemSchema)
	builder.WriteString(".")
	builder.WriteString(filter.SyncPointTable)
	builder.WriteString(" (ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES ('")
	builder.WriteString(config.GetGlobalServerConfig().ClusterID)
	builder.WriteString("', '")
	builder.WriteString(w.ChangefeedID.String())
	builder.WriteString("', ")
	builder.WriteString(strconv.FormatUint(event.GetCommitTs(), 10))
	builder.WriteString(", ")
	builder.WriteString(secondaryTs)
	builder.WriteString(")")
	query := builder.String()

	_, err = tx.Exec(query)
	if err != nil {
		log.Error("failed to write syncpoint table", zap.Error(err))
		err2 := tx.Rollback()
		if err2 != nil {
			log.Error("failed to write syncpoint table", zap.Error(err2))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to write syncpoint table; Exec Failed; Query is %s", query)))
	}

	// set global tidb_external_ts to secondary ts
	// TiDB supports tidb_external_ts system variable since v6.4.0.
	query = fmt.Sprintf("set global tidb_external_ts = %s", secondaryTs)
	_, err = tx.Exec(query)
	if err != nil {
		if apperror.IsSyncPointIgnoreError(err) {
			// TODO(dongmen): to confirm if we need to log this error.
			log.Warn("set global external ts failed, ignore this error", zap.Error(err))
		} else {
			err2 := tx.Rollback()
			if err2 != nil {
				log.Error("failed to write syncpoint table", zap.Error(err2))
			}
			return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, fmt.Sprintf("failed to write syncpoint table; Exec Failed; Query is %s", query)))
		}
	}

	// clean stale ts map in downstream
	if time.Since(w.lastCleanSyncPointTime) >= w.cfg.SyncPointRetention {
		var builder strings.Builder
		builder.WriteString("DELETE IGNORE FROM ")
		builder.WriteString(filter.TiCDCSystemSchema)
		builder.WriteString(".")
		builder.WriteString(filter.SyncPointTable)
		builder.WriteString(" WHERE ticdc_cluster_id = '")
		builder.WriteString(config.GetGlobalServerConfig().ClusterID)
		builder.WriteString("' and changefeed = '")
		builder.WriteString(w.ChangefeedID.String())
		builder.WriteString("' and created_at < (NOW() - INTERVAL ")
		builder.WriteString(fmt.Sprintf("%.2f", w.cfg.SyncPointRetention.Seconds()))
		builder.WriteString(" SECOND)")
		query := builder.String()

		_, err = tx.Exec(query)
		if err != nil {
			// It is ok to ignore the error, since it will not affect the correctness of the system,
			// and no any business logic depends on this behavior, so we just log the error.
			log.Error("failed to clean syncpoint table", zap.Error(cerror.WrapError(cerror.ErrMySQLTxnError, err)), zap.Any("query", query))
		} else {
			w.lastCleanSyncPointTime = time.Now()
		}
	}

	err = tx.Commit()
	return cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(err, "failed to write syncpoint table; Commit Fail;"))
}
