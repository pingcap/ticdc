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
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// defaultConflictDetectorSlots indicates the default slot count of conflict detector. TODO:check this
	defaultConflictDetectorSlots uint64 = 16 * 1024
)

// sink is responsible for writing data to mysql downstream.
// Including DDL and DML.
type sink struct {
	changefeedID common.ChangeFeedID

	dmlWriter []*mysql.Writer
	ddlWriter *mysql.Writer

	db         *sql.DB
	statistics *metrics.Statistics

	conflictDetector *causality.ConflictDetector

	// isNormal indicate whether the sink is in the normal state.
	isNormal   *atomic.Bool
	maxTxnRows int
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
) (*sink, error) {
	cfg, db, err := mysql.NewMysqlConfigAndDB(ctx, changefeedID, sinkURI, config)
	if err != nil {
		return nil, err
	}
	return newMysqlSinkWithDBAndConfig(ctx, changefeedID, cfg, db), nil
}

func newMysqlSinkWithDBAndConfig(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	cfg *mysql.Config,
	db *sql.DB,
) *sink {
	stat := metrics.NewStatistics(changefeedID, "TxnSink")
	result := &sink{
		changefeedID: changefeedID,
		db:           db,
		dmlWriter:    make([]*mysql.Writer, cfg.WorkerCount),
		statistics:   stat,
		conflictDetector: causality.New(defaultConflictDetectorSlots, causality.TxnCacheOption{
			Count:         cfg.WorkerCount,
			Size:          1024,
			BlockStrategy: causality.BlockStrategyWaitEmpty,
		}),
		isNormal:   atomic.NewBool(true),
		maxTxnRows: cfg.MaxTxnRow,
	}
	formatVectorType := mysql.ShouldFormatVectorType(db, cfg)
	for i := 0; i < len(result.dmlWriter); i++ {
		result.dmlWriter[i] = mysql.NewWriter(ctx, db, cfg, changefeedID, stat, formatVectorType)
	}
	result.ddlWriter = mysql.NewWriter(ctx, db, cfg, changefeedID, stat, formatVectorType)
	return result
}

func (s *sink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.conflictDetector.Run(ctx)
	})
	for idx := range s.dmlWriter {
		g.Go(func() error {
			return s.runDMLWriter(ctx, idx)
		})
	}
	err := g.Wait()
	s.isNormal.Store(false)
	return err
}

func (s *sink) runDMLWriter(ctx context.Context, idx int) error {
	namespace := s.changefeedID.Namespace()
	changefeed := s.changefeedID.Name()

	workerFlushDuration := metrics.WorkerFlushDuration.WithLabelValues(namespace, changefeed, strconv.Itoa(idx))
	workerTotalDuration := metrics.WorkerTotalDuration.WithLabelValues(namespace, changefeed, strconv.Itoa(idx))
	workerHandledRows := metrics.WorkerHandledRows.WithLabelValues(namespace, changefeed, strconv.Itoa(idx))

	defer func() {
		metrics.WorkerFlushDuration.DeleteLabelValues(namespace, changefeed, strconv.Itoa(idx))
		metrics.WorkerTotalDuration.DeleteLabelValues(namespace, changefeed, strconv.Itoa(idx))
		metrics.WorkerHandledRows.DeleteLabelValues(namespace, changefeed, strconv.Itoa(idx))
	}()

	inputCh := s.conflictDetector.GetOutChByCacheID(idx)
	writer := s.dmlWriter[idx]

	totalStart := time.Now()
	events := make([]*commonEvent.DMLEvent, 0)
	rows := 0
	for {
		needFlush := false
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case txnEvent := <-inputCh:
			events = append(events, txnEvent)
			rows += int(txnEvent.Len())
			if rows > s.maxTxnRows {
				needFlush = true
			}
			if !needFlush {
				delay := time.NewTimer(10 * time.Millisecond)
				for !needFlush {
					select {
					case txnEvent = <-inputCh:
						workerHandledRows.Add(float64(txnEvent.Len()))
						events = append(events, txnEvent)
						rows += int(txnEvent.Len())
						if rows > s.maxTxnRows {
							needFlush = true
						}
					case <-delay.C:
						needFlush = true
					}
				}
				// Release resources promptly
				if !delay.Stop() {
					select {
					case <-delay.C:
					default:
					}
				}
			}
			start := time.Now()
			err := writer.Flush(events)
			if err != nil {
				return errors.Trace(err)
			}
			workerFlushDuration.Observe(time.Since(start).Seconds())
			// we record total time to calculate the worker busy ratio.
			// so we record the total time after flushing, to unified statistics on
			// flush time and total time
			workerTotalDuration.Observe(time.Since(totalStart).Seconds())
			totalStart = time.Now()
			events = events[:0]
			rows = 0
		}
	}
}

func (s *sink) IsNormal() bool {
	return s.isNormal.Load()
}

func (s *sink) SinkType() common.SinkType {
	return common.MysqlSinkType
}

func (s *sink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlWriter.SetTableSchemaStore(tableSchemaStore)
}

func (s *sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.conflictDetector.Add(event)
}

func (s *sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	var err error
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		err = s.ddlWriter.FlushDDLEvent(event.(*commonEvent.DDLEvent))
	case commonEvent.TypeSyncPointEvent:
		err = s.ddlWriter.FlushSyncPointEvent(event.(*commonEvent.SyncPointEvent))
	default:
		log.Panic("mysql sink meet unknown event type",
			zap.String("namespace", s.changefeedID.Namespace()),
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

func (s *sink) AddCheckpointTs(_ uint64) {}

func (s *sink) GetStartTsList(
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
	for idx, ddlTs := range ddlTsList {
		if startTsList[idx] > ddlTs {
			isSyncpointList[idx] = false
		}
		startTsList[idx] = max(ddlTs, startTsList[idx])
	}
	return startTsList, isSyncpointList, nil
}

func (s *sink) Close(removeChangefeed bool) {
	// when remove the changefeed, we need to remove the ddl ts item in the ddl worker
	if removeChangefeed {
		if err := s.ddlWriter.RemoveDDLTsItem(); err != nil {
			log.Warn("close mysql sink, remove changefeed meet error",
				zap.Any("changefeed", s.changefeedID.String()), zap.Error(err))
		}
	}

	s.conflictDetector.Close()
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
