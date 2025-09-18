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
	"github.com/pingcap/ticdc/pkg/sink/util"
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
	return newMySQLSink(ctx, changefeedID, cfg, db, config.BDRMode), nil
}

func newMySQLSink(
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
	formatVectorType := mysql.ShouldFormatVectorType(db, cfg)
	for i := 0; i < len(result.dmlWriter); i++ {
		result.dmlWriter[i] = mysql.NewWriter(ctx, db, cfg, changefeedID, stat, formatVectorType)
	}
	result.ddlWriter = mysql.NewWriter(ctx, db, cfg, changefeedID, stat, formatVectorType)
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

	workerFlushDuration := metrics.WorkerFlushDuration.WithLabelValues(keyspace, changefeed, strconv.Itoa(idx))
	workerTotalDuration := metrics.WorkerTotalDuration.WithLabelValues(keyspace, changefeed, strconv.Itoa(idx))
	workerHandledRows := metrics.WorkerHandledRows.WithLabelValues(keyspace, changefeed, strconv.Itoa(idx))

	defer func() {
		metrics.WorkerFlushDuration.DeleteLabelValues(keyspace, changefeed, strconv.Itoa(idx))
		metrics.WorkerTotalDuration.DeleteLabelValues(keyspace, changefeed, strconv.Itoa(idx))
		metrics.WorkerHandledRows.DeleteLabelValues(keyspace, changefeed, strconv.Itoa(idx))
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

			flushEvent := func(beginIndex, endIndex int, rowCount int32) error {
				workerHandledRows.Add(float64(rowCount))
				err := writer.Flush(txnEvents[beginIndex:endIndex])
				if err != nil {
					return errors.Trace(err)
				}
				return nil
			}

			start := time.Now()
			beginIndex, rowCount := 0, txnEvents[0].Len()

			for i := 1; i < len(txnEvents); i++ {
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
			workerFlushDuration.Observe(time.Since(start).Seconds())

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

func (s *Sink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
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

// GetStartTsList return the startTs list and startTsIsSyncpoint list for each table in the tableIDs list.
// If removeDDLTs is true, we just need to remove the ddl ts item for this changefeed, and return startTsList directly.
// If removeDDLTs is false, we need to query the ddl ts from the ddl_ts table, and return the startTs list and startTsIsSyncpoint list.
// The startTsIsSyncpoint list is used to determine whether the startTs is a syncpoint event.
// when the startTs in input list is larger than the the startTs from ddlTs,
// we need to set the related startTsIsSyncpoint to false, and return the input startTs value.
func (s *Sink) GetStartTsList(
	tableIds []int64,
	startTsList []int64,
	removeDDLTs bool,
) ([]int64, []bool, error) {
	if removeDDLTs {
		// means we just need to remove the ddl ts item for this changefeed, and return startTsList directly.
		err := s.ddlWriter.RemoveDDLTsItem()
		if err != nil {
			s.isNormal.Store(false)
			return nil, nil, err
		}
		isSyncpointList := make([]bool, len(startTsList))
		return startTsList, isSyncpointList, nil
	}
	ddlTsList, isSyncpointList, err := s.ddlWriter.GetStartTsList(tableIds)
	if err != nil {
		s.isNormal.Store(false)
		return nil, nil, err
	}
	newStartTsList := make([]int64, len(startTsList))
	for idx, ddlTs := range ddlTsList {
		if startTsList[idx] > ddlTs {
			isSyncpointList[idx] = false
		}
		newStartTsList[idx] = max(ddlTs, startTsList[idx])
	}
	return newStartTsList, isSyncpointList, nil
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
