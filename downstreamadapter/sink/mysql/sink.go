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
	// defaultConflictDetectorSlots indicates the default slot count of conflict detector. TODO:check this
	defaultConflictDetectorSlots uint64 = 16 * 1024
)

// Sink is responsible for writing data to mysql downstream.
// Including DDL and DML.
type Sink struct {
	changefeedID common.ChangeFeedID

	dmlWriter []*mysql.Writer
	ddlWriter *mysql.Writer

	db         *sql.DB
	statistics *metrics.Statistics

	conflictDetector *causality.ConflictDetector

	// isNormal indicate whether the sink is in the normal state.
	isNormal   *atomic.Bool
	maxTxnRows int
	bdrMode    bool
}

// Verify is used to verify the sink uri and config is valid
// Currently, we verify by create a real mysql connection.
func Verify(
	ctx context.Context,
	uri *url.URL,
	config *config.ChangefeedConfig,
) error {
	testID := common.NewChangefeedID4Test("test", "mysql_create_sink_test")
	_, db, err := mysql.NewMysqlConfigAndDB(ctx, testID, uri, config)
	if err != nil {
		return err
	}
	_ = db.Close()
	return nil
}

func New(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	config *config.ChangefeedConfig,
	sinkURI *url.URL,
) (*Sink, error) {
	cfg, db, err := mysql.NewMysqlConfigAndDB(ctx, changefeedID, sinkURI, config)
	if err != nil {
		return nil, err
	}
	return NewMySQLSink(ctx, changefeedID, cfg, db, config.BDRMode), nil
}

func NewMySQLSink(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	cfg *mysql.Config,
	db *sql.DB,
	bdrMode bool,
) *Sink {
	stat := metrics.NewStatistics(changefeedID, "TxnSink")
	result := &Sink{
		changefeedID: changefeedID,
		db:           db,
		dmlWriter:    make([]*mysql.Writer, cfg.WorkerCount),
		statistics:   stat,
		conflictDetector: causality.New(defaultConflictDetectorSlots,
			causality.TxnCacheOption{
				Count:         cfg.WorkerCount,
				Size:          1024,
				BlockStrategy: causality.BlockStrategyWaitEmpty,
			},
			changefeedID),
		isNormal:   atomic.NewBool(true),
		maxTxnRows: cfg.MaxTxnRow,
		bdrMode:    bdrMode,
	}
	for i := 0; i < len(result.dmlWriter); i++ {
		result.dmlWriter[i] = mysql.NewWriter(ctx, i, db, cfg, changefeedID, stat)
	}
	result.ddlWriter = mysql.NewWriter(ctx, len(result.dmlWriter), db, cfg, changefeedID, stat)
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
	buffer := make([]*commonEvent.DMLEvent, 0, s.maxTxnRows)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
			txnEvents, ok := inputCh.GetMultipleNoGroup(buffer)
			if !ok {
				return errors.Trace(ctx.Err())
			}

			if len(txnEvents) == 0 {
				buffer = buffer[:0]
				continue
			}
			start := time.Now()
			singleFlushStart := time.Now()

			flushEvent := func(beginIndex, endIndex int, rowCount int32) error {
				workerHandledRows.Add(float64(rowCount))
				err := writer.Flush(txnEvents[beginIndex:endIndex])
				if err != nil {
					return errors.Trace(err)
				}
				workerFlushDuration.Observe(time.Since(singleFlushStart).Seconds())
				singleFlushStart = time.Now()
				return nil
			}

			beginIndex, rowCount := 0, txnEvents[0].Len()
			workerEventRowCount.Observe(float64(rowCount))
			for i := 1; i < len(txnEvents); i++ {
				workerEventRowCount.Observe(float64(txnEvents[i].Len()))
				if rowCount+txnEvents[i].Len() > int32(s.maxTxnRows) {
					if err := flushEvent(beginIndex, i, rowCount); err != nil {
						return errors.Trace(err)
					}
					beginIndex, rowCount = i, txnEvents[i].Len()
				} else {
					rowCount += txnEvents[i].Len()
				}
			}
			// flush last batch
			if err := flushEvent(beginIndex, len(txnEvents), rowCount); err != nil {
				return errors.Trace(err)
			}
			workerBatchFlushDuration.Observe(time.Since(start).Seconds())

			// we record total time to calculate the worker busy ratio.
			// so we record the total time after flushing, to unified statistics on
			// flush time and total time
			workerTotalDuration.Observe(time.Since(totalStart).Seconds())
			totalStart = time.Now()
			buffer = buffer[:0]
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
}

func (s *Sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.conflictDetector.Add(event)
}

func (s *Sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	var err error
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		ddl := event.(*commonEvent.DDLEvent)
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

func (s *Sink) AddCheckpointTs(_ uint64) {}

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

func (s *Sink) Close(removeChangefeed bool) {
	// when remove the changefeed, we need to remove the ddl ts item in the ddl worker
	if removeChangefeed {
		if err := s.ddlWriter.RemoveDDLTsItem(); err != nil {
			log.Warn("close mysql sink, remove changefeed meet error",
				zap.Any("changefeed", s.changefeedID.String()), zap.Error(err))
		}
	}

	s.conflictDetector.CloseNotifiedNodes()
	s.ddlWriter.Close()
	for _, w := range s.dmlWriter {
		w.Close()
	}

	if err := s.db.Close(); err != nil {
		log.Warn("close mysql sink db meet error",
			zap.Any("changefeed", s.changefeedID.String()),
			zap.Error(err))
	}
	s.statistics.Close()
}
