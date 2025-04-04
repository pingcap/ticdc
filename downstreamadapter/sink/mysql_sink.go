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

package sink

import (
	"context"
	"database/sql"
	"net/url"
	"sync/atomic"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/conflictdetector"
	"github.com/pingcap/ticdc/downstreamadapter/worker"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tiflow/pkg/causality"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultConflictDetectorSlots indicates the default slot count of conflict detector. TODO:check this
	DefaultConflictDetectorSlots uint64 = 16 * 1024
)

// MysqlSink is responsible for writing data to mysql downstream.
// Including DDL and DML.
type MysqlSink struct {
	changefeedID common.ChangeFeedID

	ddlWorker   *worker.MysqlDDLWorker
	dmlWorker   []*worker.MysqlDMLWorker
	workerCount int

	db         *sql.DB
	statistics *metrics.Statistics

	conflictDetector *conflictdetector.ConflictDetector

	isNormal uint32 // if sink is normal, isNormal is 1, otherwise is 0
}

// verifyMySQLSink is used to verify the sink uri and config is valid
// Currently, we verify by create a real mysql connection.
func verifyMySQLSink(
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

func newMySQLSink(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	config *config.ChangefeedConfig,
	sinkURI *url.URL,
) (*MysqlSink, error) {
	cfg, db, err := mysql.NewMysqlConfigAndDB(ctx, changefeedID, sinkURI, config)
	if err != nil {
		return nil, err
	}
	return newMysqlSinkWithDBAndConfig(ctx, changefeedID, cfg.WorkerCount, cfg, db), nil
}

func newMysqlSinkWithDBAndConfig(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	workerCount int,
	cfg *mysql.MysqlConfig,
	db *sql.DB,
) *MysqlSink {
	stat := metrics.NewStatistics(changefeedID, "TxnSink")
	mysqlSink := &MysqlSink{
		changefeedID: changefeedID,
		db:           db,
		dmlWorker:    make([]*worker.MysqlDMLWorker, workerCount),
		workerCount:  workerCount,
		statistics:   stat,
		conflictDetector: conflictdetector.NewConflictDetector(DefaultConflictDetectorSlots, conflictdetector.TxnCacheOption{
			Count:         workerCount,
			Size:          1024,
			BlockStrategy: causality.BlockStrategyWaitEmpty,
		}),
		isNormal: 1,
	}
	formatVectorType := mysql.ShouldFormatVectorType(db, cfg)
	for i := 0; i < workerCount; i++ {
		mysqlSink.dmlWorker[i] = worker.NewMysqlDMLWorker(ctx, db, cfg, i, changefeedID, stat, formatVectorType, mysqlSink.conflictDetector.GetOutChByCacheID(int64(i)))
	}
	mysqlSink.ddlWorker = worker.NewMysqlDDLWorker(ctx, db, cfg, changefeedID, stat, formatVectorType)
	return mysqlSink
}

func (s *MysqlSink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < s.workerCount; i++ {
		i := i // capture loop variable
		g.Go(func() error {
			return s.dmlWorker[i].Run(ctx)
		})
	}
	err := g.Wait()
	atomic.StoreUint32(&s.isNormal, 0)
	return errors.Trace(err)
}

func (s *MysqlSink) IsNormal() bool {
	value := atomic.LoadUint32(&s.isNormal) == 1
	return value
}

func (s *MysqlSink) SinkType() common.SinkType {
	return common.MysqlSinkType
}

func (s *MysqlSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlWorker.SetTableSchemaStore(tableSchemaStore)
}

func (s *MysqlSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.conflictDetector.Add(event)
	// // We use low value of dispatcherID to divide different tables into different workers.
	// // And ensure the same table always goes to the same worker.
	// index := event.GetDispatcherID().GetLow() % uint64(s.workerCount)
	// s.dmlWorker[index].AddDMLEvent(event)
}

func (s *MysqlSink) PassBlockEvent(event commonEvent.BlockEvent) {
	event.PostFlush()
}

func (s *MysqlSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	err := s.ddlWorker.WriteBlockEvent(event)
	if err != nil {
		atomic.StoreUint32(&s.isNormal, 0)
		return err
	}
	return nil
}

func (s *MysqlSink) AddCheckpointTs(_ uint64) {}

func (s *MysqlSink) GetStartTsList(
	tableIds []int64,
	startTsList []int64,
	removeDDLTs bool,
) ([]int64, []bool, error) {
	if removeDDLTs {
		// means we just need to remove the ddl ts item for this changefeed, and return startTsList directly.
		err := s.ddlWorker.RemoveDDLTsItem()
		if err != nil {
			atomic.StoreUint32(&s.isNormal, 0)
			return nil, nil, err
		}
		isSyncpointList := make([]bool, len(startTsList))
		return startTsList, isSyncpointList, nil
	}

	startTsList, isSyncpointList, err := s.ddlWorker.GetStartTsList(tableIds, startTsList)
	if err != nil {
		atomic.StoreUint32(&s.isNormal, 0)
		return nil, nil, err
	}
	return startTsList, isSyncpointList, nil
}

func (s *MysqlSink) Close(removeChangefeed bool) {
	// when remove the changefeed, we need to remove the ddl ts item in the ddl worker
	if removeChangefeed {
		if err := s.ddlWorker.RemoveDDLTsItem(); err != nil {
			log.Warn("close mysql sink, remove changefeed meet error",
				zap.Any("changefeed", s.changefeedID.String()), zap.Error(err))
		}
	}
	for i := 0; i < s.workerCount; i++ {
		s.dmlWorker[i].Close()
	}

	s.ddlWorker.Close()

	if err := s.db.Close(); err != nil {
		log.Warn("close mysql sink db meet error",
			zap.Any("changefeed", s.changefeedID.String()),
			zap.Error(err))
	}
	s.statistics.Close()
}

func MysqlSinkForTest() (*MysqlSink, sqlmock.Sqlmock) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	cfg := mysql.NewMysqlConfig()
	cfg.DMLMaxRetry = 1
	cfg.MaxAllowedPacket = int64(variable.DefMaxAllowedPacket)
	cfg.CachePrepStmts = false

	sink := newMysqlSinkWithDBAndConfig(ctx, changefeedID, 1, cfg, db)
	go sink.Run(ctx)

	return sink, mock
}
