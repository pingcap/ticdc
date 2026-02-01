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

// Package iceberg wires the Iceberg sink into the downstream adapter.
package iceberg

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/br/pkg/storage"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type pendingTxn struct {
	commitTs       uint64
	tableInfo      *common.TableInfo
	tableID        int64
	rows           []sinkiceberg.ChangeRow
	estimatedBytes int64
	callback       func()
}

type columnPlanColumn struct {
	idx  int
	name string
	ft   *types.FieldType
}

type tableColumnPlan struct {
	updateTs       uint64
	dataColumns    []columnPlanColumn
	keyColumns     []columnPlanColumn
	keyColumnNames []string
}

type tableBuffer struct {
	pending []pendingTxn
}

type sink struct {
	changefeedID common.ChangeFeedID
	sinkURI      *url.URL
	cfg          *sinkiceberg.Config

	warehouseStorage storage.ExternalStorage
	tableWriter      *sinkiceberg.TableWriter

	dmlCh *chann.UnlimitedChannel[*commonEvent.DMLEvent, any]
	// checkpointTs is the changefeed-level checkpointTs provided by the coordinator.
	// For iceberg sink, we only use it for optional checkpoint publishing (for example, global checkpoint table).
	checkpointTs atomic.Uint64
	// maxCommitTs is the maximum commitTs observed from incoming DML events.
	// It is used as the upper bound for periodic commits to avoid a deadlock with checkpointTs,
	// which itself depends on sink flush progress.
	maxCommitTs atomic.Uint64
	// lastGlobalCheckpointTs is the last checkpointTs successfully written to the global checkpoint table.
	lastGlobalCheckpointTs atomic.Uint64

	commitMu      sync.Mutex
	mu            sync.Mutex
	planMu        sync.Mutex
	buffers       map[int64]*tableBuffer
	columnPlans   map[int64]*tableColumnPlan
	committed     map[int64]uint64
	bufferedRows  map[int64]int64
	bufferedBytes map[int64]int64
	totalRows     int64
	totalBytes    int64

	tableSchemaStore *commonEvent.TableSchemaStore
	statistics       *metrics.Statistics
	isNormal         *atomic.Bool
	ctx              context.Context
}

type upsertKeyState struct {
	existedBefore bool
	deleteRow     *sinkiceberg.ChangeRow
	dataRow       *sinkiceberg.ChangeRow
}

// Verify validates the Iceberg sink URI and configuration.
func Verify(ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig) error {
	cfg := sinkiceberg.NewConfig()
	if err := cfg.Apply(ctx, sinkURI, sinkConfig); err != nil {
		return err
	}
	switch cfg.Catalog {
	case sinkiceberg.CatalogHadoop, sinkiceberg.CatalogGlue:
	default:
		return errors.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("iceberg catalog is not supported yet: %s", cfg.Catalog))
	}
	if cfg.Catalog == sinkiceberg.CatalogGlue {
		warehouseURL, err := url.Parse(cfg.WarehouseURI)
		if err != nil {
			return errors.WrapError(errors.ErrSinkURIInvalid, err)
		}
		if warehouseURL.Scheme != "s3" {
			return errors.ErrSinkURIInvalid.GenWithStackByArgs("iceberg glue catalog requires a s3 warehouse")
		}
	}
	switch cfg.Mode {
	case sinkiceberg.ModeAppend, sinkiceberg.ModeUpsert:
	default:
		return errors.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("iceberg mode is not supported yet: %s", cfg.Mode))
	}

	warehouseStorage, err := util.GetExternalStorageWithDefaultTimeout(ctx, cfg.WarehouseURI)
	if err != nil {
		return err
	}
	tableWriter := sinkiceberg.NewTableWriter(cfg, warehouseStorage)
	if err := tableWriter.VerifyCatalog(ctx); err != nil {
		warehouseStorage.Close()
		return err
	}
	warehouseStorage.Close()
	_ = changefeedID // reserved for future validation hooks
	return nil
}

// New constructs an Iceberg sink for the given changefeed.
func New(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
	_ []func(), /* cleanupJobs, reserved */
) (*sink, error) {
	cfg := sinkiceberg.NewConfig()
	if err := cfg.Apply(ctx, sinkURI, sinkConfig); err != nil {
		return nil, err
	}
	switch cfg.Catalog {
	case sinkiceberg.CatalogHadoop, sinkiceberg.CatalogGlue:
	default:
		return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("iceberg catalog is not supported yet: %s", cfg.Catalog))
	}
	if cfg.Catalog == sinkiceberg.CatalogGlue {
		warehouseURL, err := url.Parse(cfg.WarehouseURI)
		if err != nil {
			return nil, errors.WrapError(errors.ErrSinkURIInvalid, err)
		}
		if warehouseURL.Scheme != "s3" {
			return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs("iceberg glue catalog requires a s3 warehouse")
		}
	}
	switch cfg.Mode {
	case sinkiceberg.ModeAppend, sinkiceberg.ModeUpsert:
	default:
		return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("iceberg mode is not supported yet: %s", cfg.Mode))
	}

	warehouseStorage, err := util.GetExternalStorageWithDefaultTimeout(ctx, cfg.WarehouseURI)
	if err != nil {
		return nil, err
	}

	statistics := metrics.NewStatistics(changefeedID, "iceberg")
	s := &sink{
		changefeedID:     changefeedID,
		sinkURI:          sinkURI,
		cfg:              cfg,
		warehouseStorage: warehouseStorage,
		tableWriter:      sinkiceberg.NewTableWriter(cfg, warehouseStorage),
		dmlCh:            chann.NewUnlimitedChannelDefault[*commonEvent.DMLEvent](),
		buffers:          make(map[int64]*tableBuffer),
		columnPlans:      make(map[int64]*tableColumnPlan),
		committed:        make(map[int64]uint64),
		bufferedRows:     make(map[int64]int64),
		bufferedBytes:    make(map[int64]int64),
		statistics:       statistics,
		isNormal:         atomic.NewBool(true),
		ctx:              ctx,
	}
	return s, nil
}

func (s *sink) SinkType() common.SinkType {
	return common.IcebergSinkType
}

func (s *sink) IsNormal() bool {
	return s.isNormal.Load()
}

func (s *sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.dmlCh.Push(event)
}

func (s *sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch e := event.(type) {
	case *commonEvent.DDLEvent:
		s.commitMu.Lock()
		defer s.commitMu.Unlock()

		ddlTs := e.FinishedTs
		if ddlTs != 0 {
			if err := s.commitOnce(s.ctx, ddlTs); err != nil {
				s.isNormal.Store(false)
				return err
			}
		}
		if err := s.handleDDLEvent(s.ctx, e); err != nil {
			s.isNormal.Store(false)
			return err
		}
	default:
		log.Warn("iceberg sink ignores block event",
			zap.String("namespace", s.changefeedID.Keyspace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.String("eventType", commonEvent.TypeToString(event.GetType())))
	}
	event.PostFlush()
	return nil
}

func (s *sink) AddCheckpointTs(ts uint64) {
	for {
		current := s.checkpointTs.Load()
		if ts <= current {
			return
		}
		if s.checkpointTs.CAS(current, ts) {
			metrics.IcebergGlobalResolvedTsGauge.WithLabelValues(
				s.changefeedID.Keyspace(),
				s.changefeedID.Name(),
			).Set(float64(ts))
			return
		}
	}
}

func (s *sink) SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore) {
	s.tableSchemaStore = tableSchemaStore
}

func (s *sink) Close(_ bool) {
	s.dmlCh.Close()
	s.statistics.Close()
	s.warehouseStorage.Close()
}

func (s *sink) handleDDLEvent(ctx context.Context, event *commonEvent.DDLEvent) error {
	if event == nil {
		return nil
	}
	return s.statistics.RecordDDLExecution(func() (string, error) {
		events := event.GetEvents()
		for _, e := range events {
			if e == nil || e.NotSync {
				continue
			}
			if e.TableInfo == nil {
				continue
			}
			switch e.GetDDLType() {
			case timodel.ActionDropTable, timodel.ActionDropSchema:
				continue
			case timodel.ActionRenameTable, timodel.ActionRenameTables:
				if s.cfg.Catalog != sinkiceberg.CatalogGlue {
					return "", errors.ErrSinkURIInvalid.GenWithStackByArgs("iceberg rename table requires glue catalog")
				}
				if err := s.tableWriter.RenameGlueTable(ctx, s.changefeedID, e.GetExtraSchemaName(), e.GetExtraTableName(), e.GetSchemaName(), e.GetTableName()); err != nil {
					return "", err
				}
			case timodel.ActionTruncateTable:
				start := time.Now()
				commitResult, err := s.tableWriter.TruncateTable(ctx, s.changefeedID, e.TableInfo, e.GetTableID(), e.FinishedTs)
				if err != nil {
					return "", err
				}
				if commitResult != nil {
					keyspace := s.changefeedID.Keyspace()
					changefeed := s.changefeedID.Name()
					schema := e.TableInfo.GetSchemaName()
					table := e.TableInfo.GetTableName()
					metrics.IcebergCommitDurationHistogram.WithLabelValues(keyspace, changefeed, schema, table).Observe(time.Since(start).Seconds())
					metrics.IcebergLastCommittedResolvedTsGauge.WithLabelValues(keyspace, changefeed, schema, table).Set(float64(e.FinishedTs))
					metrics.IcebergLastCommittedSnapshotIDGauge.WithLabelValues(keyspace, changefeed, schema, table).Set(float64(commitResult.SnapshotID))
					lagSeconds := time.Since(oracle.GetTimeFromTS(e.FinishedTs)).Seconds()
					if lagSeconds < 0 {
						lagSeconds = 0
					}
					metrics.IcebergResolvedTsLagSecondsGauge.WithLabelValues(keyspace, changefeed, schema, table).Set(lagSeconds)
					if commitResult.DataFilesWritten > 0 {
						metrics.IcebergFilesWrittenCounter.WithLabelValues(keyspace, changefeed, schema, table, "data").Add(float64(commitResult.DataFilesWritten))
					}
					if commitResult.DeleteFilesWritten > 0 {
						metrics.IcebergFilesWrittenCounter.WithLabelValues(keyspace, changefeed, schema, table, "delete").Add(float64(commitResult.DeleteFilesWritten))
					}

					log.Info("iceberg table truncated",
						zap.String("namespace", keyspace),
						zap.String("changefeed", changefeed),
						zap.String("schema", schema),
						zap.String("table", table),
						zap.Int64("tableID", e.GetTableID()),
						zap.Int64("snapshotID", commitResult.SnapshotID),
						zap.Uint64("resolvedTs", e.FinishedTs),
						zap.Int64("bytes", commitResult.BytesWritten),
						zap.Int("dataFiles", commitResult.DataFilesWritten),
						zap.Int("deleteFiles", commitResult.DeleteFilesWritten),
						zap.Duration("duration", time.Since(start)))
					if s.cfg.EnableCheckpointTable {
						if err := s.tableWriter.RecordCheckpoint(ctx, s.changefeedID, e.TableInfo, e.GetTableID(), e.FinishedTs, commitResult); err != nil {
							log.Warn("record iceberg checkpoint failed",
								zap.String("namespace", s.changefeedID.Keyspace()),
								zap.String("changefeed", s.changefeedID.Name()),
								zap.String("schema", e.TableInfo.GetSchemaName()),
								zap.String("table", e.TableInfo.GetTableName()),
								zap.Int64("tableID", e.GetTableID()),
								zap.Uint64("resolvedTs", e.FinishedTs),
								zap.Error(err))
						}
					}
				}
			case timodel.ActionTruncateTablePartition,
				timodel.ActionAddTablePartition, timodel.ActionDropTablePartition,
				timodel.ActionExchangeTablePartition, timodel.ActionReorganizePartition,
				timodel.ActionAlterTablePartitioning, timodel.ActionRemovePartitioning:
				return "", errors.ErrSinkURIInvalid.GenWithStackByArgs(
					fmt.Sprintf("iceberg sink does not support ddl action: %s", e.GetDDLType()),
				)
			}
			if err := s.tableWriter.EnsureTable(ctx, s.changefeedID, e.TableInfo); err != nil {
				return "", err
			}
		}
		return event.GetDDLType().String(), nil
	})
}

func (s *sink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		// UnlimitedChannel will block when there is no event, it cannot directly find ctx.Done().
		// Close it to unblock the buffer loop on shutdown.
		<-ctx.Done()
		s.dmlCh.Close()
		return nil
	})

	g.Go(func() error {
		return s.bufferLoop(ctx)
	})

	g.Go(func() error {
		return s.commitLoop(ctx)
	})

	return g.Wait()
}

func (s *sink) bufferLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
			ev, ok := s.dmlCh.Get()
			if !ok {
				return nil
			}
			if ev == nil || ev.TableInfo == nil || ev.Rows == nil {
				continue
			}
			var (
				rows []sinkiceberg.ChangeRow
				err  error
			)
			plan, err := s.getColumnPlan(ev.PhysicalTableID, ev.TableInfo)
			if err != nil {
				s.isNormal.Store(false)
				return err
			}
			if s.cfg.Mode == sinkiceberg.ModeUpsert {
				rows, err = convertToUpsertOps(ev, plan)
			} else {
				rows, err = convertToChangeRows(ev, plan)
			}
			if err != nil {
				s.isNormal.Store(false)
				return err
			}
			txn := pendingTxn{
				commitTs:       ev.CommitTs,
				tableInfo:      ev.TableInfo,
				tableID:        ev.PhysicalTableID,
				rows:           rows,
				estimatedBytes: estimateChangeRowsBytes(rows, s.cfg.EmitMetadataColumns),
				callback:       ev.PostFlush,
			}
			for {
				current := s.maxCommitTs.Load()
				if txn.commitTs <= current {
					break
				}
				if s.maxCommitTs.CAS(current, txn.commitTs) {
					break
				}
			}

			s.mu.Lock()
			buf := s.buffers[ev.PhysicalTableID]
			if buf == nil {
				buf = &tableBuffer{}
				s.buffers[ev.PhysicalTableID] = buf
			}
			buf.pending = append(buf.pending, txn)
			delta := int64(len(txn.rows))
			s.bufferedRows[ev.PhysicalTableID] += delta
			s.bufferedBytes[ev.PhysicalTableID] += txn.estimatedBytes
			s.totalRows += delta
			s.totalBytes += txn.estimatedBytes
			bufferedRows := s.bufferedRows[ev.PhysicalTableID]
			bufferedBytes := s.bufferedBytes[ev.PhysicalTableID]
			totalRows := s.totalRows
			totalBytes := s.totalBytes
			s.mu.Unlock()

			metrics.IcebergBufferedRowsGauge.WithLabelValues(
				s.changefeedID.Keyspace(),
				s.changefeedID.Name(),
				ev.TableInfo.GetSchemaName(),
				ev.TableInfo.GetTableName(),
			).Set(float64(bufferedRows))
			metrics.IcebergBufferedBytesGauge.WithLabelValues(
				s.changefeedID.Keyspace(),
				s.changefeedID.Name(),
				ev.TableInfo.GetSchemaName(),
				ev.TableInfo.GetTableName(),
			).Set(float64(bufferedBytes))

			if s.cfg.MaxBufferedRows > 0 && totalRows > s.cfg.MaxBufferedRows {
				s.isNormal.Store(false)
				return errors.ErrSinkURIInvalid.GenWithStackByArgs("iceberg sink buffered rows exceeded max")
			}
			if s.cfg.MaxBufferedBytes > 0 && totalBytes > s.cfg.MaxBufferedBytes {
				s.isNormal.Store(false)
				return errors.ErrSinkURIInvalid.GenWithStackByArgs("iceberg sink buffered bytes exceeded max")
			}
			if s.cfg.MaxBufferedRowsPerTable > 0 && bufferedRows > s.cfg.MaxBufferedRowsPerTable {
				s.isNormal.Store(false)
				return errors.ErrSinkURIInvalid.GenWithStackByArgs("iceberg sink buffered rows per table exceeded max")
			}
			if s.cfg.MaxBufferedBytesPerTable > 0 && bufferedBytes > s.cfg.MaxBufferedBytesPerTable {
				s.isNormal.Store(false)
				return errors.ErrSinkURIInvalid.GenWithStackByArgs("iceberg sink buffered bytes per table exceeded max")
			}
		}
	}
}

func (s *sink) commitLoop(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.CommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			commitUpperBound := s.maxCommitTs.Load()
			if commitUpperBound == 0 {
				continue
			}
			roundStart := time.Now()
			s.commitMu.Lock()
			err := s.commitOnce(ctx, commitUpperBound)
			if err == nil && s.cfg.EnableGlobalCheckpointTable {
				checkpointTs := s.checkpointTs.Load()
				lastPublished := s.lastGlobalCheckpointTs.Load()
				if checkpointTs != 0 && checkpointTs > lastPublished {
					if globalErr := s.tableWriter.RecordGlobalCheckpoint(ctx, s.changefeedID, checkpointTs); globalErr != nil {
						log.Warn("record iceberg global checkpoint failed",
							zap.String("namespace", s.changefeedID.Keyspace()),
							zap.String("changefeed", s.changefeedID.Name()),
							zap.Uint64("resolvedTs", checkpointTs),
							zap.Error(globalErr))
					} else {
						for {
							current := s.lastGlobalCheckpointTs.Load()
							if checkpointTs <= current {
								break
							}
							if s.lastGlobalCheckpointTs.CAS(current, checkpointTs) {
								break
							}
						}
					}
				}
			}
			s.commitMu.Unlock()
			metrics.IcebergCommitRoundDurationHistogram.WithLabelValues(
				s.changefeedID.Keyspace(),
				s.changefeedID.Name(),
			).Observe(time.Since(roundStart).Seconds())
			if err != nil {
				s.isNormal.Store(false)
				return err
			}
		}
	}
}

func collapseUpsertTxns(tableInfo *common.TableInfo, txns []pendingTxn, keyColumnNames []string) ([]sinkiceberg.ChangeRow, []sinkiceberg.ChangeRow, error) {
	if tableInfo == nil {
		return nil, nil, errors.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}
	if len(txns) == 0 {
		return nil, nil, nil
	}
	if len(keyColumnNames) == 0 {
		return nil, nil, errors.ErrSinkURIInvalid.GenWithStackByArgs("handle key columns are empty")
	}

	buildKey := func(row sinkiceberg.ChangeRow) (string, error) {
		var b strings.Builder
		var lenBuf [binary.MaxVarintLen64]byte
		for _, name := range keyColumnNames {
			v, ok := row.Columns[name]
			if !ok || v == nil {
				return "", errors.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("handle key column is null: %s", name))
			}
			n := binary.PutUvarint(lenBuf[:], uint64(len(*v)))
			_, _ = b.Write(lenBuf[:n])
			b.WriteString(*v)
		}
		return b.String(), nil
	}

	states := make(map[string]*upsertKeyState, 1024)
	order := make([]string, 0, 256)

	for _, txn := range txns {
		for _, opRow := range txn.rows {
			key, err := buildKey(opRow)
			if err != nil {
				return nil, nil, err
			}

			state, ok := states[key]
			if !ok {
				state = &upsertKeyState{existedBefore: opRow.Op == "D"}
				if state.existedBefore {
					delCopy := opRow
					state.deleteRow = &delCopy
				}
				states[key] = state
				order = append(order, key)
			} else if state.existedBefore && state.deleteRow == nil && opRow.Op == "D" {
				delCopy := opRow
				state.deleteRow = &delCopy
			}

			switch opRow.Op {
			case "D":
				state.dataRow = nil
			case "I", "U":
				rowCopy := opRow
				state.dataRow = &rowCopy
			default:
				continue
			}
		}
	}

	dataRows := make([]sinkiceberg.ChangeRow, 0, len(order))
	deleteRows := make([]sinkiceberg.ChangeRow, 0, len(order))
	for _, key := range order {
		state := states[key]
		if state == nil {
			continue
		}
		if state.dataRow != nil {
			dataRows = append(dataRows, *state.dataRow)
		}
		if !state.existedBefore {
			continue
		}
		if state.deleteRow != nil {
			deleteRows = append(deleteRows, *state.deleteRow)
			continue
		}
		if state.dataRow == nil {
			return nil, nil, errors.ErrSinkURIInvalid.GenWithStackByArgs("missing delete row for existing key")
		}
		cols := make(map[string]*string, len(keyColumnNames))
		for _, name := range keyColumnNames {
			cols[name] = state.dataRow.Columns[name]
		}
		deleteRows = append(deleteRows, sinkiceberg.ChangeRow{
			Op:         "D",
			CommitTs:   state.dataRow.CommitTs,
			CommitTime: state.dataRow.CommitTime,
			Columns:    cols,
		})
	}

	return dataRows, deleteRows, nil
}

func (s *sink) commitOnce(ctx context.Context, resolvedTs uint64) error {
	type task struct {
		tableID   int64
		tableInfo *common.TableInfo
		txns      []pendingTxn
	}

	pruneCallbacks, err := s.pruneCommittedTxns(ctx)
	if err != nil {
		return err
	}

	var tasks []task
	s.mu.Lock()
	for tableID, buf := range s.buffers {
		if buf == nil || len(buf.pending) == 0 {
			continue
		}
		n := 0
		for n < len(buf.pending) && buf.pending[n].commitTs <= resolvedTs {
			n++
		}
		if n == 0 {
			continue
		}
		txns := append([]pendingTxn(nil), buf.pending[:n]...)
		tasks = append(tasks, task{
			tableID:   tableID,
			tableInfo: txns[len(txns)-1].tableInfo,
			txns:      txns,
		})
	}
	s.mu.Unlock()

	for _, cb := range pruneCallbacks {
		cb()
	}

	for _, t := range tasks {
		taskResolvedTs := t.txns[len(t.txns)-1].commitTs
		var (
			rows         []sinkiceberg.ChangeRow
			callbacks    []func()
			removed      int64
			removedBytes int64
		)
		for _, txn := range t.txns {
			rows = append(rows, txn.rows...)
			removed += int64(len(txn.rows))
			removedBytes += txn.estimatedBytes
			if txn.callback != nil {
				callbacks = append(callbacks, txn.callback)
			}
		}

		deleteRowsCount := 0
		if s.cfg.Mode == sinkiceberg.ModeUpsert {
			for _, row := range rows {
				if row.Op == "D" {
					deleteRowsCount++
				}
			}
		}
		dataRowsCount := len(rows)
		if s.cfg.Mode == sinkiceberg.ModeUpsert {
			dataRowsCount = len(rows) - deleteRowsCount
		}

		start := time.Now()
		var commitResult *sinkiceberg.CommitResult
		if err := s.statistics.RecordBatchExecution(func() (int, int64, error) {
			switch s.cfg.Mode {
			case sinkiceberg.ModeUpsert:
				if len(rows) == 0 {
					return 0, 0, nil
				}
				equalityFieldIDs, err := sinkiceberg.GetEqualityFieldIDs(t.tableInfo)
				if err != nil {
					return 0, 0, err
				}
				plan, err := s.getColumnPlan(t.tableID, t.tableInfo)
				if err != nil {
					return 0, 0, err
				}
				collapsedRows, collapsedDeleteRows, err := collapseUpsertTxns(t.tableInfo, t.txns, plan.keyColumnNames)
				if err != nil {
					return 0, 0, err
				}
				var upsertErr error
				commitResult, upsertErr = s.tableWriter.Upsert(ctx, s.changefeedID, t.tableInfo, t.tableID, collapsedRows, collapsedDeleteRows, equalityFieldIDs, taskResolvedTs)
				if upsertErr != nil {
					return 0, 0, upsertErr
				}
				if commitResult == nil {
					return 0, 0, nil
				}
				return len(rows), commitResult.BytesWritten, nil
			default:
				if len(rows) == 0 {
					return 0, 0, nil
				}
				var appendErr error
				commitResult, appendErr = s.tableWriter.AppendChangelog(ctx, s.changefeedID, t.tableInfo, t.tableID, rows, taskResolvedTs)
				if appendErr != nil {
					return 0, 0, appendErr
				}
				if commitResult == nil {
					return 0, 0, nil
				}
				return len(rows), commitResult.BytesWritten, nil
			}
		}); err != nil {
			return err
		}

		if s.cfg.Mode == sinkiceberg.ModeUpsert && len(rows) == 0 {
			continue
		}
		if s.cfg.Mode != sinkiceberg.ModeUpsert && len(rows) == 0 {
			continue
		}
		if commitResult == nil {
			continue
		}

		keyspace := s.changefeedID.Keyspace()
		changefeed := s.changefeedID.Name()
		schema := t.tableInfo.GetSchemaName()
		table := t.tableInfo.GetTableName()
		metrics.IcebergCommitDurationHistogram.WithLabelValues(keyspace, changefeed, schema, table).Observe(time.Since(start).Seconds())
		metrics.IcebergLastCommittedResolvedTsGauge.WithLabelValues(keyspace, changefeed, schema, table).Set(float64(taskResolvedTs))
		metrics.IcebergLastCommittedSnapshotIDGauge.WithLabelValues(keyspace, changefeed, schema, table).Set(float64(commitResult.SnapshotID))
		lagSeconds := time.Since(oracle.GetTimeFromTS(taskResolvedTs)).Seconds()
		if lagSeconds < 0 {
			lagSeconds = 0
		}
		metrics.IcebergResolvedTsLagSecondsGauge.WithLabelValues(keyspace, changefeed, schema, table).Set(lagSeconds)
		if commitResult.DataFilesWritten > 0 {
			metrics.IcebergFilesWrittenCounter.WithLabelValues(keyspace, changefeed, schema, table, "data").Add(float64(commitResult.DataFilesWritten))
		}
		if commitResult.DeleteFilesWritten > 0 {
			metrics.IcebergFilesWrittenCounter.WithLabelValues(keyspace, changefeed, schema, table, "delete").Add(float64(commitResult.DeleteFilesWritten))
		}
		if commitResult.DataBytesWritten > 0 {
			metrics.IcebergBytesWrittenCounter.WithLabelValues(keyspace, changefeed, schema, table, "data").Add(float64(commitResult.DataBytesWritten))
		}
		if commitResult.DeleteBytesWritten > 0 {
			metrics.IcebergBytesWrittenCounter.WithLabelValues(keyspace, changefeed, schema, table, "delete").Add(float64(commitResult.DeleteBytesWritten))
		}

		log.Info("iceberg table committed",
			zap.String("namespace", keyspace),
			zap.String("changefeed", changefeed),
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Int64("tableID", t.tableID),
			zap.Int64("snapshotID", commitResult.SnapshotID),
			zap.Uint64("resolvedTs", taskResolvedTs),
			zap.Int("rows", dataRowsCount),
			zap.Int("deleteRows", deleteRowsCount),
			zap.Int64("bytes", commitResult.BytesWritten),
			zap.Int("dataFiles", commitResult.DataFilesWritten),
			zap.Int("deleteFiles", commitResult.DeleteFilesWritten),
			zap.Duration("duration", time.Since(start)))

		if s.cfg.EnableCheckpointTable {
			if err := s.tableWriter.RecordCheckpoint(ctx, s.changefeedID, t.tableInfo, t.tableID, taskResolvedTs, commitResult); err != nil {
				log.Warn("record iceberg checkpoint failed",
					zap.String("namespace", s.changefeedID.Keyspace()),
					zap.String("changefeed", s.changefeedID.Name()),
					zap.String("schema", t.tableInfo.GetSchemaName()),
					zap.String("table", t.tableInfo.GetTableName()),
					zap.Int64("tableID", t.tableID),
					zap.Uint64("resolvedTs", taskResolvedTs),
					zap.Error(err))
			}
		}

		for _, cb := range callbacks {
			cb()
		}

		s.mu.Lock()
		buf := s.buffers[t.tableID]
		if buf != nil && len(buf.pending) >= len(t.txns) {
			buf.pending = buf.pending[len(t.txns):]
		}
		s.committed[t.tableID] = taskResolvedTs
		s.bufferedRows[t.tableID] -= removed
		if s.bufferedRows[t.tableID] < 0 {
			s.bufferedRows[t.tableID] = 0
		}
		s.bufferedBytes[t.tableID] -= removedBytes
		if s.bufferedBytes[t.tableID] < 0 {
			s.bufferedBytes[t.tableID] = 0
		}
		s.totalRows -= removed
		if s.totalRows < 0 {
			s.totalRows = 0
		}
		s.totalBytes -= removedBytes
		if s.totalBytes < 0 {
			s.totalBytes = 0
		}
		bufferedRows := s.bufferedRows[t.tableID]
		bufferedBytes := s.bufferedBytes[t.tableID]
		s.mu.Unlock()

		metrics.IcebergBufferedRowsGauge.WithLabelValues(keyspace, changefeed, schema, table).Set(float64(bufferedRows))
		metrics.IcebergBufferedBytesGauge.WithLabelValues(keyspace, changefeed, schema, table).Set(float64(bufferedBytes))
	}
	return nil
}

func (s *sink) pruneCommittedTxns(ctx context.Context) ([]func(), error) {
	type tableMeta struct {
		tableID   int64
		tableInfo *common.TableInfo
	}

	var toLoad []tableMeta
	s.mu.Lock()
	for tableID, buf := range s.buffers {
		if buf == nil || len(buf.pending) == 0 {
			continue
		}
		if _, ok := s.committed[tableID]; ok {
			continue
		}
		toLoad = append(toLoad, tableMeta{
			tableID:   tableID,
			tableInfo: buf.pending[len(buf.pending)-1].tableInfo,
		})
	}
	s.mu.Unlock()

	loaded := make(map[int64]uint64, len(toLoad))
	for _, t := range toLoad {
		committedTs, err := s.tableWriter.GetLastCommittedResolvedTs(ctx, t.tableInfo)
		if err != nil {
			return nil, err
		}
		loaded[t.tableID] = committedTs
	}

	var callbacks []func()
	type gaugeUpdate struct {
		schema   string
		table    string
		buffered int64
		bytes    int64
	}
	var updates []gaugeUpdate
	s.mu.Lock()
	for tableID, committedTs := range loaded {
		s.committed[tableID] = committedTs
	}
	for tableID, buf := range s.buffers {
		if buf == nil || len(buf.pending) == 0 {
			continue
		}
		tableInfo := buf.pending[len(buf.pending)-1].tableInfo
		committedTs := s.committed[tableID]
		n := 0
		var removed int64
		var removedBytes int64
		for n < len(buf.pending) && buf.pending[n].commitTs <= committedTs {
			if cb := buf.pending[n].callback; cb != nil {
				callbacks = append(callbacks, cb)
			}
			removed += int64(len(buf.pending[n].rows))
			removedBytes += buf.pending[n].estimatedBytes
			n++
		}
		if n > 0 {
			buf.pending = buf.pending[n:]
			s.bufferedRows[tableID] -= removed
			if s.bufferedRows[tableID] < 0 {
				s.bufferedRows[tableID] = 0
			}
			s.bufferedBytes[tableID] -= removedBytes
			if s.bufferedBytes[tableID] < 0 {
				s.bufferedBytes[tableID] = 0
			}
			s.totalRows -= removed
			if s.totalRows < 0 {
				s.totalRows = 0
			}
			s.totalBytes -= removedBytes
			if s.totalBytes < 0 {
				s.totalBytes = 0
			}
		}
		if tableInfo != nil {
			updates = append(updates, gaugeUpdate{
				schema:   tableInfo.GetSchemaName(),
				table:    tableInfo.GetTableName(),
				buffered: s.bufferedRows[tableID],
				bytes:    s.bufferedBytes[tableID],
			})
		}
	}
	s.mu.Unlock()

	for _, u := range updates {
		metrics.IcebergBufferedRowsGauge.WithLabelValues(
			s.changefeedID.Keyspace(),
			s.changefeedID.Name(),
			u.schema,
			u.table,
		).Set(float64(u.buffered))
		metrics.IcebergBufferedBytesGauge.WithLabelValues(
			s.changefeedID.Keyspace(),
			s.changefeedID.Name(),
			u.schema,
			u.table,
		).Set(float64(u.bytes))
	}

	return callbacks, nil
}

func estimateChangeRowsBytes(rows []sinkiceberg.ChangeRow, emitMetadata bool) int64 {
	var size int64
	for _, row := range rows {
		size += estimateChangeRowBytes(row, emitMetadata)
	}
	return size
}

func (s *sink) getColumnPlan(tableID int64, tableInfo *common.TableInfo) (*tableColumnPlan, error) {
	if tableInfo == nil {
		return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}
	updateTs := tableInfo.UpdateTS
	s.planMu.Lock()
	plan := s.columnPlans[tableID]
	if plan != nil && plan.updateTs == updateTs {
		s.planMu.Unlock()
		return plan, nil
	}
	newPlan, err := buildColumnPlan(tableInfo)
	if err != nil {
		s.planMu.Unlock()
		return nil, err
	}
	newPlan.updateTs = updateTs
	s.columnPlans[tableID] = newPlan
	s.planMu.Unlock()
	return newPlan, nil
}

func buildColumnPlan(tableInfo *common.TableInfo) (*tableColumnPlan, error) {
	if tableInfo == nil {
		return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}

	colInfos := tableInfo.GetColumns()
	if len(colInfos) == 0 {
		return &tableColumnPlan{}, nil
	}

	dataColumns := make([]columnPlanColumn, 0, len(colInfos))
	idToColumn := make(map[int64]columnPlanColumn, len(colInfos))
	for idx, colInfo := range colInfos {
		if colInfo == nil || colInfo.IsVirtualGenerated() {
			continue
		}
		col := columnPlanColumn{
			idx:  idx,
			name: colInfo.Name.O,
			ft:   &colInfo.FieldType,
		}
		dataColumns = append(dataColumns, col)
		idToColumn[colInfo.ID] = col
	}

	keyIDs := tableInfo.GetOrderedHandleKeyColumnIDs()
	keyColumns := make([]columnPlanColumn, 0, len(keyIDs))
	keyColumnNames := make([]string, 0, len(keyIDs))
	for _, id := range keyIDs {
		col, ok := idToColumn[id]
		if !ok {
			return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("handle key column not found in table columns: %d", id))
		}
		keyColumns = append(keyColumns, col)
		keyColumnNames = append(keyColumnNames, col.name)
	}

	return &tableColumnPlan{
		dataColumns:    dataColumns,
		keyColumns:     keyColumns,
		keyColumnNames: keyColumnNames,
	}, nil
}

func estimateChangeRowBytes(row sinkiceberg.ChangeRow, emitMetadata bool) int64 {
	var size int64
	if emitMetadata {
		size += int64(len(row.Op) + len(row.CommitTs) + len(row.CommitTime) + 32)
	}
	for _, v := range row.Columns {
		if v == nil {
			size++
			continue
		}
		size += int64(len(*v))
	}
	if size <= 0 {
		return 1
	}
	return size
}

func convertToChangeRows(event *commonEvent.DMLEvent, plan *tableColumnPlan) ([]sinkiceberg.ChangeRow, error) {
	commitTime := oracle.GetTimeFromTS(event.CommitTs).UTC()
	commitTimeStr := commitTime.Format(time.RFC3339Nano)
	commitTsStr := strconv.FormatUint(event.CommitTs, 10)

	if plan == nil || len(plan.dataColumns) == 0 {
		return nil, nil
	}

	event.Rewind()
	defer event.Rewind()

	rows := make([]sinkiceberg.ChangeRow, 0, event.Len())
	for {
		change, ok := event.GetNextRow()
		if !ok {
			break
		}

		var (
			op  string
			row chunk.Row
		)
		switch change.RowType {
		case common.RowTypeInsert:
			op = "I"
			row = change.Row
		case common.RowTypeUpdate:
			op = "U"
			row = change.Row
		case common.RowTypeDelete:
			op = "D"
			row = change.PreRow
		default:
			continue
		}

		columns := make(map[string]*string, len(plan.dataColumns))
		for _, col := range plan.dataColumns {
			v, err := formatColumnAsString(&row, col.idx, col.ft)
			if err != nil {
				return nil, err
			}
			columns[col.name] = v
		}

		rows = append(rows, sinkiceberg.ChangeRow{
			Op:         op,
			CommitTs:   commitTsStr,
			CommitTime: commitTimeStr,
			Columns:    columns,
		})
	}
	return rows, nil
}

func convertToUpsertOps(event *commonEvent.DMLEvent, plan *tableColumnPlan) ([]sinkiceberg.ChangeRow, error) {
	commitTime := oracle.GetTimeFromTS(event.CommitTs).UTC()
	commitTimeStr := commitTime.Format(time.RFC3339Nano)
	commitTsStr := strconv.FormatUint(event.CommitTs, 10)

	if plan == nil || len(plan.dataColumns) == 0 {
		return nil, nil
	}

	if len(plan.keyColumns) == 0 {
		return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs("upsert requires a primary key or not null unique key")
	}

	event.Rewind()
	defer event.Rewind()

	rows := make([]sinkiceberg.ChangeRow, 0, event.Len())
	for {
		change, ok := event.GetNextRow()
		if !ok {
			break
		}

		switch change.RowType {
		case common.RowTypeInsert:
			row := change.Row
			columns := make(map[string]*string, len(plan.dataColumns))
			for _, col := range plan.dataColumns {
				v, err := formatColumnAsString(&row, col.idx, col.ft)
				if err != nil {
					return nil, err
				}
				columns[col.name] = v
			}
			rows = append(rows, sinkiceberg.ChangeRow{
				Op:         "I",
				CommitTs:   commitTsStr,
				CommitTime: commitTimeStr,
				Columns:    columns,
			})
		case common.RowTypeDelete:
			row := change.PreRow
			columns := make(map[string]*string, len(plan.keyColumns))
			for _, key := range plan.keyColumns {
				v, err := formatColumnAsString(&row, key.idx, key.ft)
				if err != nil {
					return nil, err
				}
				if v == nil {
					return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("handle key column is null: %s", key.name))
				}
				columns[key.name] = v
			}
			rows = append(rows, sinkiceberg.ChangeRow{
				Op:         "D",
				CommitTs:   commitTsStr,
				CommitTime: commitTimeStr,
				Columns:    columns,
			})
		case common.RowTypeUpdate:
			before := change.PreRow
			after := change.Row

			delColumns := make(map[string]*string, len(plan.keyColumns))
			for _, key := range plan.keyColumns {
				v, err := formatColumnAsString(&before, key.idx, key.ft)
				if err != nil {
					return nil, err
				}
				if v == nil {
					return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("handle key column is null: %s", key.name))
				}
				delColumns[key.name] = v
			}
			rows = append(rows, sinkiceberg.ChangeRow{
				Op:         "D",
				CommitTs:   commitTsStr,
				CommitTime: commitTimeStr,
				Columns:    delColumns,
			})

			columns := make(map[string]*string, len(plan.dataColumns))
			for _, col := range plan.dataColumns {
				v, err := formatColumnAsString(&after, col.idx, col.ft)
				if err != nil {
					return nil, err
				}
				columns[col.name] = v
			}
			rows = append(rows, sinkiceberg.ChangeRow{
				Op:         "U",
				CommitTs:   commitTsStr,
				CommitTime: commitTimeStr,
				Columns:    columns,
			})
		default:
			continue
		}
	}
	return rows, nil
}

func formatColumnAsString(row *chunk.Row, idx int, ft *types.FieldType) (*string, error) {
	if row.IsNull(idx) {
		return nil, nil
	}

	d := row.GetDatum(idx, ft)
	var value string

	switch ft.GetType() {
	case mysql.TypeBit:
		v, err := d.GetMysqlBit().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = strconv.FormatUint(v, 10)
	case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDatetime:
		value = d.GetMysqlTime().String()
	case mysql.TypeDuration:
		value = d.GetMysqlDuration().String()
	case mysql.TypeNewDecimal:
		value = d.GetMysqlDecimal().String()
	case mysql.TypeJSON:
		value = d.GetMysqlJSON().String()
	case mysql.TypeEnum:
		v := d.GetMysqlEnum().Value
		enumVal, err := types.ParseEnumValue(ft.GetElems(), v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = enumVal.Name
	case mysql.TypeSet:
		v := d.GetMysqlSet().Value
		setVal, err := types.ParseSetValue(ft.GetElems(), v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = setVal.Name
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		value = base64.StdEncoding.EncodeToString(d.GetBytes())
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			value = base64.StdEncoding.EncodeToString(d.GetBytes())
		} else {
			value = d.GetString()
		}
	default:
		switch v := d.GetValue().(type) {
		case int64:
			value = strconv.FormatInt(v, 10)
		case uint64:
			value = strconv.FormatUint(v, 10)
		case float32:
			value = strconv.FormatFloat(float64(v), 'f', -1, 32)
		case float64:
			value = strconv.FormatFloat(v, 'f', -1, 64)
		case string:
			value = v
		case []byte:
			if mysql.HasBinaryFlag(ft.GetFlag()) {
				value = base64.StdEncoding.EncodeToString(v)
			} else {
				value = string(v)
			}
		default:
			value = fmt.Sprintf("%v", v)
		}
	}
	return &value, nil
}
