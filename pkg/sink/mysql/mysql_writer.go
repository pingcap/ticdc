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
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
)

const (
	defaultDDLMaxRetry uint64 = 20

	// networkDriftDuration is used to construct a context timeout for database operations.
	networkDriftDuration = 5 * time.Second

	defaultSupportVectorVersion = "8.4.0"

	defaultErrorCausedSafeModeDuration = 5 * time.Second
)

// Writer is responsible for writing various dml events, ddl events, syncpoint events to mysql downstream.
type Writer struct {
	id           int
	ctx          context.Context
	db           *sql.DB
	cfg          *Config
	ChangefeedID common.ChangeFeedID

	syncPointTableInit     bool
	lastCleanSyncPointTime time.Time

	ddlTsTableInit      bool
	ddlTsTableInitMutex sync.Mutex
	tableSchemaStore    *util.TableSchemaStore

	// implement stmtCache to improve performance, especially when the downstream is TiDB
	stmtCache *lru.Cache

	statistics *metrics.Statistics

	// When encountered an `Duplicate entry` error, we will set the `isInErrorCausedSafeMode` to true,
	// and set the `lastErrorCausedSafeModeTime` to the current time.
	// After the `errorCausedSafeModeDuration`, we will set the `isInErrorCausedSafeMode` to false.
	isInErrorCausedSafeMode     bool
	lastErrorCausedSafeModeTime time.Time
	errorCausedSafeModeDuration time.Duration

	// for dry-run mode
	blockerTicker *time.Ticker
}

func NewWriter(
	ctx context.Context,
	id int,
	db *sql.DB,
	cfg *Config,
	changefeedID common.ChangeFeedID,
	statistics *metrics.Statistics,
) *Writer {
	res := &Writer{
		ctx:                    ctx,
		id:                     id,
		db:                     db,
		cfg:                    cfg,
		syncPointTableInit:     false,
		ChangefeedID:           changefeedID,
		lastCleanSyncPointTime: time.Now(),
		ddlTsTableInit:         false,
		stmtCache:              cfg.stmtCache,
		statistics:             statistics,

		isInErrorCausedSafeMode:     false,
		errorCausedSafeModeDuration: defaultErrorCausedSafeModeDuration,
	}

	if cfg.DryRun && cfg.DryRunBlockInterval > 0 {
		res.blockerTicker = time.NewTicker(cfg.DryRunBlockInterval)
	}

	return res
}

func (w *Writer) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	w.tableSchemaStore = tableSchemaStore
}

func (w *Writer) FlushDDLEvent(event *commonEvent.DDLEvent) error {
	if w.cfg.IsTiDB {
		// first we check whether there is some async ddl executed now.
		w.waitAsyncDDLDone(event)
	}
	if w.cfg.IsTiDB || !event.TiDBOnly {
		// we write ddl ts before ddl first, and update the ddl ts item after ddl executed,
		// to ensure the atomic with ddl writing when server is restarted.
		err := w.FlushDDLTsPre(event)
		if err != nil {
			return err
		}

		err = w.execDDLWithMaxRetries(event)
		if err != nil {
			return err
		}
		// We need to record ddl' ts after each ddl for each table in the downstream when sink is mysql-compatible.
		// Only in this way, when the node restart, we can continue sync data from the last ddl ts at least.
		// Otherwise, after restarting, we may sync old data in new schema, which will leading to data loss.

		// We make Flush ddl ts before callback(), in order to make sure the ddl ts is flushed
		// before new checkpointTs will report to maintainer. Therefore, when the table checkpointTs is forward,
		// we can ensure the ddl and ddl ts are both flushed downstream successfully.
		err = w.FlushDDLTs(event)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) FlushSyncPointEvent(event *commonEvent.SyncPointEvent) error {
	if w.cfg.DryRun {
		return nil
	}

	if !w.syncPointTableInit {
		// create sync point table if not exist
		err := w.createSyncTable()
		if err != nil {
			return errors.Trace(err)
		}
		w.syncPointTableInit = true
	}

	// we write ddl ts before ddl first, and update the ddl ts item after ddl executed,
	// to ensure the atomic with ddl writing when server is restarted.
	err := w.FlushDDLTsPre(event)
	if err != nil {
		return errors.Trace(err)
	}

	err = w.SendSyncPointEvent(event)
	if err != nil {
		return errors.Trace(err)
	}

	// We need to record ddl' ts after each ddl for each table in the downstream when sink is mysql-compatible.
	// Only in this way, when the node restart, we can continue sync data from the last ddl ts at least.
	// Otherwise, after restarting, we may sync old data in new schema, which will leading to data loss.

	// We make Flush ddl ts before callback(), in order to make sure the ddl ts is flushed
	// before new checkpointTs will report to maintainer. Therefore, when the table checkpointTs is forward,
	// we can ensure the ddl and ddl ts are both flushed downstream successfully.
	err = w.FlushDDLTs(event)

	return err
}

func (w *Writer) Flush(events []*commonEvent.DMLEvent) error {
	w.updateIsInErrorCausedSafeMode()

	dmls, err := w.prepareDMLs(events)
	defer dmlsPool.Put(dmls) // Return dmls to pool after use
	if err != nil {
		return errors.Trace(err)
	}
	if dmls.rowCount == 0 {
		return nil
	}

	if !w.cfg.DryRun {
		err = w.execDMLWithMaxRetries(dmls)
		// If the error is a duplicate entry error, we will retry the dmls.
		if w.checkIsDuplicateEntryError(err) {
			log.Info("Meet Duplicate Entry Error, retry the dmls in safemode", zap.Error(err))
			for _, event := range events {
				event.Rewind()
			}
			dmls, err = w.prepareDMLs(events)
			if err != nil {
				return errors.Trace(err)
			}
			err = w.execDMLWithMaxRetries(dmls)
		}

	} else {
		w.tryDryRunBlock()
		err = w.statistics.RecordBatchExecution(func() (int, int64, error) {
			return dmls.rowCount, dmls.approximateSize, nil
		})
	}

	if err != nil {
		return errors.Trace(err)
	}

	for _, event := range events {
		event.PostFlush()
	}

	return nil
}

func (w *Writer) checkIsDuplicateEntryError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Cause(err) == cerror.ErrMySQLDuplicateEntry ||
		strings.Contains(err.Error(), "Duplicate entry") {
		if !w.isInErrorCausedSafeMode {
			w.isInErrorCausedSafeMode = true
			w.lastErrorCausedSafeModeTime = time.Now()
		}
		return true
	}
	return false
}

func (w *Writer) updateIsInErrorCausedSafeMode() {
	if !w.isInErrorCausedSafeMode {
		return
	}
	if time.Since(w.lastErrorCausedSafeModeTime) > w.errorCausedSafeModeDuration {
		w.isInErrorCausedSafeMode = false
	}
}

func (w *Writer) tryDryRunBlock() {
	time.Sleep(w.cfg.DryRunDelay)
	if w.blockerTicker != nil {
		select {
		case <-w.blockerTicker.C:
			log.Info("dry-run mode, blocker ticker triggered, block for a while",
				zap.Duration("duration", w.cfg.DryRunBlockInterval))
			time.Sleep(w.cfg.DryRunBlockInterval)
		default:
		}
	}
}

func (w *Writer) Close() {
	if w.stmtCache != nil {
		w.stmtCache.Purge()
	}
	if w.blockerTicker != nil {
		w.blockerTicker.Stop()
	}
}
