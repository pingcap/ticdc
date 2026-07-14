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
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mysql/causality"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultConflictDetectorSlots uint64 = 16 * 1024
)

// Sink is responsible for writing data to mysql downstream.
// Including DDL and DML.
type Sink struct {
	changefeedID common.ChangeFeedID

	dmlWriter []*mysql.Writer
	ddlWriter *mysql.Writer
	// progressTableWriter writes progress rows to the downstream progress table for
	// active-active replication (hard delete safety checks). It is nil when
	// enableActiveActive is false.
	progressTableWriter *mysql.ProgressTableWriter

	// dmlDB and controlDB are the DB pools this sink is responsible for closing.
	// Compatibility callers built through NewMySQLSink use one shared pool.
	dmlDB      *sql.DB
	controlDB  *sql.DB
	statistics *metrics.Statistics

	conflictDetector *causality.ConflictDetector

	// isNormal indicate whether the sink is in the normal state.
	isNormal   *atomic.Bool
	cfg        *mysql.Config
	maxTxnRows int
	bdrMode    bool
	// enableActiveActive enables active-active replication behaviors in the MySQL-class sink.
	enableActiveActive bool

	// activeActiveSyncStatsCollector collects conflict statistics from TiDB session
	// variable @@tidb_cdc_active_active_sync_stats and is shared by all DML writers.
	// It is nil when disabled or unsupported by downstream.
	activeActiveSyncStatsCollector *mysql.ActiveActiveSyncStatsCollector

	barrierMu           sync.Mutex
	outstandingBarriers map[*dmlBarrier]struct{}
}

type dmlBarrier struct {
	mu        sync.Mutex
	done      chan struct{}
	err       error
	remaining int
	acked     []bool
	doneFuncs []func()
}

func newDMLBarrier(workerCount int) *dmlBarrier {
	barrier := &dmlBarrier{
		done:      make(chan struct{}),
		remaining: workerCount,
		acked:     make([]bool, workerCount),
	}
	if workerCount == 0 {
		barrier.Fail(errors.ErrMySQLTxnError.GenWithStackByArgs("mysql DML barrier has no writers"))
	}
	return barrier
}

func (b *dmlBarrier) Ack(writerID int) {
	b.mu.Lock()
	if b.err != nil || b.remaining == 0 || writerID < 0 || writerID >= len(b.acked) || b.acked[writerID] {
		b.mu.Unlock()
		return
	}
	b.acked[writerID] = true
	b.remaining--
	if b.remaining > 0 {
		b.mu.Unlock()
		return
	}
	doneFuncs := b.doneFuncs
	b.doneFuncs = nil
	close(b.done)
	b.mu.Unlock()
	runBarrierDoneFuncs(doneFuncs)
}

func (b *dmlBarrier) Fail(err error) {
	if err == nil {
		err = errors.ErrMySQLTxnError.GenWithStackByArgs("mysql DML barrier failed")
	}
	b.mu.Lock()
	if b.err != nil || b.remaining == 0 {
		b.mu.Unlock()
		return
	}
	b.err = err
	b.remaining = 0
	doneFuncs := b.doneFuncs
	b.doneFuncs = nil
	close(b.done)
	b.mu.Unlock()
	runBarrierDoneFuncs(doneFuncs)
}

func (b *dmlBarrier) Wait() error {
	<-b.done
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

func (b *dmlBarrier) OnDone(f func()) {
	b.mu.Lock()
	if b.remaining == 0 {
		b.mu.Unlock()
		f()
		return
	}
	b.doneFuncs = append(b.doneFuncs, f)
	b.mu.Unlock()
}

func runBarrierDoneFuncs(funcs []func()) {
	for _, f := range funcs {
		f()
	}
}

// Verify is used to verify the sink URI and config are valid.
// It creates the same DML and control DB pools as New so verification covers
// connection availability for both data and control-plane paths.
func Verify(
	ctx context.Context,
	uri *url.URL,
	config *config.ChangefeedConfig,
) error {
	testID := common.NewChangefeedID4Test("test", "mysql_create_sink_test")
	_, dmlDB, controlDB, err := mysql.NewMysqlConfigAndDBs(ctx, testID, uri, config)
	if err != nil {
		return err
	}
	_ = dmlDB.Close()
	_ = controlDB.Close()
	return nil
}

func New(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	config *config.ChangefeedConfig,
	sinkURI *url.URL,
) (*Sink, error) {
	cfg, dmlDB, controlDB, err := mysql.NewMysqlConfigAndDBs(ctx, changefeedID, sinkURI, config)
	if err != nil {
		return nil, err
	}

	// Expose whether the MySQL-compatible downstream is confirmed to be TiDB, so
	// dashboards can display "tidb" when we can prove it. Otherwise, the
	// scheme-based label remains "mysql/tidb".
	keyspace := changefeedID.Keyspace()
	name := changefeedID.Name()
	if cfg.IsTiDB {
		metrics.ChangefeedDownstreamIsTiDBGauge.WithLabelValues(keyspace, name).Set(1)
	} else {
		metrics.ChangefeedDownstreamIsTiDBGauge.DeleteLabelValues(keyspace, name)
	}

	return newMySQLSinkWithControlDB(ctx, changefeedID, cfg, dmlDB, controlDB, config.BDRMode, config.EnableActiveActive, config.ActiveActiveProgressInterval), nil
}

func NewMySQLSink(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	cfg *mysql.Config,
	db *sql.DB,
	bdrMode bool,
	enableActiveActive bool,
	progressInterval time.Duration,
) *Sink {
	return newMySQLSinkWithDBs(ctx, changefeedID, cfg, db, db, bdrMode, enableActiveActive, progressInterval)
}

// newMySQLSinkWithControlDB creates a MySQL sink with separate pools for DML and
// control-plane operations. The control pool is used by DDL, DDL-ts, syncpoint,
// and active-active progress metadata paths so they do not wait behind long-lived
// DML sessions.
func newMySQLSinkWithControlDB(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	cfg *mysql.Config,
	dmlDB *sql.DB,
	controlDB *sql.DB,
	bdrMode bool,
	enableActiveActive bool,
	progressInterval time.Duration,
) *Sink {
	return newMySQLSinkWithDBs(ctx, changefeedID, cfg, dmlDB, controlDB, bdrMode, enableActiveActive, progressInterval)
}

func newMySQLSinkWithDBs(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	cfg *mysql.Config,
	dmlDB *sql.DB,
	controlDB *sql.DB,
	bdrMode bool,
	enableActiveActive bool,
	progressInterval time.Duration,
) *Sink {
	stat := metrics.NewStatistics(changefeedID, "TxnSink")

	var activeActiveSyncStatsCollector *mysql.ActiveActiveSyncStatsCollector
	if enableActiveActive && cfg.IsTiDB && cfg.ActiveActiveSyncStatsInterval > 0 {
		supported, err := mysql.CheckActiveActiveSyncStatsSupported(ctx, dmlDB)
		if err != nil {
			log.Info("failed to check tidb_cdc_active_active_sync_stats support, disable metric collection",
				zap.String("keyspace", changefeedID.Keyspace()),
				zap.Stringer("changefeed", changefeedID),
				zap.Error(err))
		} else if supported {
			activeActiveSyncStatsCollector = mysql.NewActiveActiveSyncStatsCollector(changefeedID)
		} else {
			log.Info("downstream does not support tidb_cdc_active_active_sync_stats, disable metric collection",
				zap.String("keyspace", changefeedID.Keyspace()),
				zap.Stringer("changefeed", changefeedID))
		}
	}

	result := &Sink{
		changefeedID: changefeedID,
		dmlDB:        dmlDB,
		controlDB:    controlDB,
		dmlWriter:    make([]*mysql.Writer, cfg.WorkerCount),
		statistics:   stat,
		conflictDetector: causality.New(defaultConflictDetectorSlots,
			causality.TxnCacheOption{
				Count:         cfg.WorkerCount,
				Size:          1024,
				BlockStrategy: causality.BlockStrategyWaitEmpty,
			},
			changefeedID),
		isNormal:                       atomic.NewBool(true),
		cfg:                            cfg,
		maxTxnRows:                     cfg.MaxTxnRow,
		bdrMode:                        bdrMode,
		enableActiveActive:             enableActiveActive,
		activeActiveSyncStatsCollector: activeActiveSyncStatsCollector,
		outstandingBarriers:            make(map[*dmlBarrier]struct{}),
	}
	for i := 0; i < len(result.dmlWriter); i++ {
		result.dmlWriter[i] = mysql.NewWriter(ctx, i, dmlDB, cfg, changefeedID, stat, activeActiveSyncStatsCollector)
	}
	result.ddlWriter = mysql.NewWriter(ctx, len(result.dmlWriter), controlDB, cfg, changefeedID, stat, nil)
	if enableActiveActive {
		result.progressTableWriter = mysql.NewProgressTableWriter(ctx, controlDB, changefeedID, cfg.MaxTxnRow, progressInterval)
	}
	return result
}

func (s *Sink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.conflictDetector.Run(ctx)
	})
	for idx := range s.dmlWriter {
		g.Go(func() error {
			defer s.conflictDetector.CloseNotifiedNodes()
			return s.runDMLWriter(ctx, idx)
		})
	}
	err := g.Wait()
	s.isNormal.Store(false)
	return err
}

func (s *Sink) runDMLWriter(ctx context.Context, idx int) error {
	keyspace := s.changefeedID.Keyspace()
	changefeed := s.changefeedID.Name()

	workerBatchFlushDuration := metrics.WorkerBatchFlushDuration.WithLabelValues(keyspace, changefeed, strconv.Itoa(idx))
	workerFlushDuration := metrics.WorkerFlushDuration.WithLabelValues(keyspace, changefeed, strconv.Itoa(idx))
	workerTotalDuration := metrics.WorkerTotalDuration.WithLabelValues(keyspace, changefeed, strconv.Itoa(idx))
	workerHandledRows := metrics.WorkerHandledRows.WithLabelValues(keyspace, changefeed, strconv.Itoa(idx))
	workerEventRowCount := metrics.WorkerEventRowCount.WithLabelValues(keyspace, changefeed, strconv.Itoa(idx))

	defer func() {
		metrics.WorkerFlushDuration.DeleteLabelValues(keyspace, changefeed, strconv.Itoa(idx))
		metrics.WorkerTotalDuration.DeleteLabelValues(keyspace, changefeed, strconv.Itoa(idx))
		metrics.WorkerHandledRows.DeleteLabelValues(keyspace, changefeed, strconv.Itoa(idx))
		metrics.WorkerBatchFlushDuration.DeleteLabelValues(keyspace, changefeed, strconv.Itoa(idx))
		metrics.WorkerEventRowCount.DeleteLabelValues(keyspace, changefeed, strconv.Itoa(idx))
	}()

	inputCh := s.conflictDetector.GetOutChByCacheID(idx)
	writer := s.dmlWriter[idx]

	totalStart := time.Now()
	itemBuffer := make([]causality.WriterItem, 0, s.maxTxnRows)
	dmlBuffer := make([]*commonEvent.DMLEvent, 0, s.maxTxnRows)
	for {
		select {
		case <-ctx.Done():
			err := errors.Trace(ctx.Err())
			s.failOutstandingBarriers(err)
			return err
		default:
			items, ok := inputCh.GetMultipleNoGroup(itemBuffer)
			if !ok {
				err := errors.Trace(ctx.Err())
				s.failOutstandingBarriers(err)
				return err
			}

			if len(items) == 0 {
				itemBuffer = itemBuffer[:0]
				continue
			}
			start := time.Now()
			singleFlushStart := time.Now()

			flushDMLs := func(events []*commonEvent.DMLEvent, rowCount int32) error {
				if len(events) == 0 {
					return nil
				}
				workerHandledRows.Add(float64(rowCount))
				err := writer.Flush(events)
				if err != nil {
					return errors.Trace(err)
				}
				workerFlushDuration.Observe(time.Since(singleFlushStart).Seconds())
				singleFlushStart = time.Now()
				return nil
			}

			rowCount := int32(0)
			for _, item := range items {
				if item.DML != nil {
					workerEventRowCount.Observe(float64(item.DML.Len()))
					if rowCount+item.DML.Len() > int32(s.maxTxnRows) {
						if err := flushDMLs(dmlBuffer, rowCount); err != nil {
							s.failOutstandingBarriers(err)
							return err
						}
						dmlBuffer = dmlBuffer[:0]
						rowCount = 0
					}
					dmlBuffer = append(dmlBuffer, item.DML)
					rowCount += item.DML.Len()
					continue
				}

				if item.Barrier != nil {
					if err := flushDMLs(dmlBuffer, rowCount); err != nil {
						item.Barrier.Fail(err)
						s.failOutstandingBarriers(err)
						return err
					}
					dmlBuffer = dmlBuffer[:0]
					rowCount = 0
					item.Barrier.Ack(idx)
				}
			}
			// flush last batch
			if err := flushDMLs(dmlBuffer, rowCount); err != nil {
				s.failOutstandingBarriers(err)
				return err
			}
			dmlBuffer = dmlBuffer[:0]
			workerBatchFlushDuration.Observe(time.Since(start).Seconds())

			// we record total time to calculate the worker busy ratio.
			// so we record the total time after flushing, to unified statistics on
			// flush time and total time
			workerTotalDuration.Observe(time.Since(totalStart).Seconds())
			totalStart = time.Now()
			itemBuffer = itemBuffer[:0]
		}
	}
}

func (s *Sink) IsNormal() bool {
	return s.isNormal.Load()
}

func (s *Sink) SinkType() common.SinkType {
	return common.MysqlSinkType
}

func (s *Sink) SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore) {
	s.ddlWriter.SetTableSchemaStore(tableSchemaStore)
	if s.progressTableWriter != nil {
		// ProgressTableWriter needs table name snapshots to build progress rows.
		s.progressTableWriter.SetTableSchemaStore(tableSchemaStore)
	}
}

func (s *Sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.conflictDetector.Add(event)
}

func (s *Sink) FlushDMLBeforeBlock(_ commonEvent.BlockEvent) error {
	barrier := newDMLBarrier(len(s.dmlWriter))
	s.registerBarrier(barrier)
	defer s.unregisterBarrier(barrier)

	if err := s.conflictDetector.BroadcastBarrier(barrier); err != nil {
		barrier.Fail(err)
		return err
	}
	return barrier.Wait()
}

func (s *Sink) registerBarrier(barrier *dmlBarrier) {
	s.barrierMu.Lock()
	s.outstandingBarriers[barrier] = struct{}{}
	s.barrierMu.Unlock()
}

func (s *Sink) unregisterBarrier(barrier *dmlBarrier) {
	s.barrierMu.Lock()
	delete(s.outstandingBarriers, barrier)
	s.barrierMu.Unlock()
}

func (s *Sink) failOutstandingBarriers(err error) {
	s.barrierMu.Lock()
	barriers := make([]*dmlBarrier, 0, len(s.outstandingBarriers))
	for barrier := range s.outstandingBarriers {
		barriers = append(barriers, barrier)
	}
	s.barrierMu.Unlock()

	for _, barrier := range barriers {
		barrier.Fail(err)
	}
}

func (s *Sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	var err error
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		ddl := event.(*commonEvent.DDLEvent)
		// In enable-active-active mode, TiCDC maintains a downstream progress table for
		// hard delete safety checks. DDLs that drop/rename tables must also clean up the
		// corresponding progress rows by TiCDC. The cleanup is idempotent and must run before
		// flushing the DDL event (and regardless of the BDR role).
		if s.enableActiveActive {
			err = s.progressTableWriter.RemoveTables(ddl)
			if err != nil {
				break
			}
		}
		// a BDR mode cluster, TiCDC can receive DDLs from all roles of TiDB.
		// However, CDC only executes the DDLs from the TiDB that has BDRRolePrimary role.
		if s.bdrMode && ddl.BDRMode != string(ast.BDRRolePrimary) {
			break
		}
		err = s.ddlWriter.FlushDDLEvent(ddl)
	case commonEvent.TypeSyncPointEvent:
		err = s.ddlWriter.FlushSyncPointEvent(event.(*commonEvent.SyncPointEvent))
	default:
		log.Panic("mysql sink meet unknown event type",
			zap.String("keyspace", s.changefeedID.Keyspace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("event", event))
	}
	if err != nil {
		s.isNormal.Store(false)
		return errors.Trace(err)
	}
	event.PostFlush()
	return nil
}

// AddCheckpointTs is invoked by dispatcher manager whenever Maintainer broadcasts a
// new changefeed-level checkpoint. It updates the active-active progress table on a
// best-effort basis. ProgressTableWriter throttles updates internally to avoid
// hammering downstream on every tick.
func (s *Sink) AddCheckpointTs(ts uint64) {
	if !s.enableActiveActive || s.progressTableWriter == nil {
		return
	}

	if err := s.progressTableWriter.Flush(ts); err != nil {
		log.Warn("failed to update active active progress table",
			zap.String("changefeed", s.changefeedID.DisplayName.String()),
			zap.Error(err))
		return
	}
}

// GetTableRecoveryInfo queries DDL crash recovery information for the given tables.
//
// Returns:
//   - startTsList: The actual startTs to use for each table
//   - skipSyncpointAtStartTsList: Whether to skip syncpoint events at startTs
//   - skipDMLAsStartTsList: Whether to skip DML events at startTs+1
//
// Parameters:
//   - tableIds: List of table IDs to query
//   - startTsList: Input startTs list (used as fallback if larger than ddl_ts)
//   - removeDDLTs: If true, remove ddl_ts records and use input startTsList directly
//
// Logic:
//  1. If removeDDLTs is true: Remove ddl_ts records for this changefeed and return
//     input startTsList with all skip flags set to false (normal operation, no recovery needed).
//  2. If removeDDLTs is false: Query ddl_ts table to get recovery information:
//     - For each table, compare ddl_ts with input startTs
//     - If input startTs > ddl_ts: Use input startTs and reset all skip flags to false
//     (the table has advanced beyond the ddl_ts crash point, no recovery needed)
//     - Otherwise: Use ddl_ts and its associated skip flags from the ddl_ts table
func (s *Sink) GetTableRecoveryInfo(
	tableIds []int64,
	startTsList []int64,
	removeDDLTs bool,
) ([]int64, []bool, []bool, error) {
	if removeDDLTs {
		// Removing changefeed: clean up ddl_ts records and use input startTs directly.
		// All skip flags are false because we're starting fresh, no crash recovery needed.
		err := s.ddlWriter.RemoveDDLTsItem()
		if err != nil {
			s.isNormal.Store(false)
			return nil, nil, nil, err
		}
		skipSyncpointAtStartTsList := make([]bool, len(startTsList))
		skipDMLAsStartTsList := make([]bool, len(startTsList))
		return startTsList, skipSyncpointAtStartTsList, skipDMLAsStartTsList, nil
	}

	// Query ddl_ts table for crash recovery information
	ddlTsList, skipSyncpointAtStartTsList, skipDMLAsStartTsList, err := s.ddlWriter.GetTableRecoveryInfo(tableIds)
	if err != nil {
		s.isNormal.Store(false)
		return nil, nil, nil, err
	}

	// For each table, determine the actual startTs and skip flags
	newStartTsList := make([]int64, len(startTsList))
	for idx, ddlTs := range ddlTsList {
		if startTsList[idx] > ddlTs {
			// Input startTs is ahead of ddl_ts crash point.
			// This means the table has already progressed beyond the crash point,
			// so we use input startTs and disable all skip flags (no recovery needed).
			skipSyncpointAtStartTsList[idx] = false
			skipDMLAsStartTsList[idx] = false
		}
		// Use the maximum of ddl_ts and input startTs
		newStartTsList[idx] = max(ddlTs, startTsList[idx])
	}
	return newStartTsList, skipSyncpointAtStartTsList, skipDMLAsStartTsList, nil
}

func (s *Sink) Close() {
	s.failOutstandingBarriers(context.Canceled)
	s.conflictDetector.CloseNotifiedNodes()
	s.ddlWriter.Close()
	for _, w := range s.dmlWriter {
		w.Close()
	}

	s.closeDBPool("dml", s.dmlDB)
	if s.controlDB != s.dmlDB {
		s.closeDBPool("control", s.controlDB)
	}
	if s.activeActiveSyncStatsCollector != nil {
		s.activeActiveSyncStatsCollector.Close()
	}
	s.statistics.Close()

	metrics.ChangefeedDownstreamIsTiDBGauge.DeleteLabelValues(s.changefeedID.Keyspace(), s.changefeedID.Name())
}

func (s *Sink) closeDBPool(role string, db *sql.DB) {
	if err := db.Close(); err != nil {
		log.Warn("failed to close mysql sink db pool",
			zap.String("changefeed", s.changefeedID.String()),
			zap.String("dbRole", role),
			zap.Error(err))
	}
}

// CleanupRemovedChangefeed removes ddl_ts state for a deleted changefeed.
// It uses a short-lived DB connection so the cleanup can still run after the
// normal sink close path has already closed the long-lived connection.
func (s *Sink) CleanupRemovedChangefeed() error {
	if !s.cfg.EnableDDLTs {
		return nil
	}

	// Remove-changefeed cleanup must stay available even if the sink has already
	// closed its long-lived DB connection in the normal close path.
	dsnStr, err := mysql.GenerateDSN(context.Background(), s.cfg)
	if err != nil {
		return err
	}
	db, err := mysql.CreateMysqlDBConn(dsnStr)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Warn("close mysql cleanup db meet error",
				zap.Any("changefeed", s.changefeedID.String()), zap.Error(closeErr))
		}
	}()

	cleanupWriter := mysql.NewWriter(context.Background(), -1, db, s.cfg, s.changefeedID, nil, nil)
	defer cleanupWriter.Close()
	return cleanupWriter.RemoveDDLTsItem()
}

func (s *Sink) BatchCount() int {
	return s.maxTxnRows * len(s.dmlWriter)
}

func (s *Sink) BatchBytes() int {
	return int(s.cfg.MaxAllowedPacket)
}
