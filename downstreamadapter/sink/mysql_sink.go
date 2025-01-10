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

	"github.com/pingcap/errors"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/worker"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	prime = 31
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

	isNormal uint32 // if sink is normal, isNormal is 1, otherwise is 0
}

func NewMysqlSink(ctx context.Context, changefeedID common.ChangeFeedID, workerCount int, config *config.ChangefeedConfig, sinkURI *url.URL) (*MysqlSink, error) {
	cfg, db, err := mysql.NewMysqlConfigAndDB(ctx, changefeedID, sinkURI, config)
	if err != nil {
		return nil, err
	}
	cfg.SyncPointRetention = config.SyncPointRetention
	mysqlSink := MysqlSink{
		changefeedID: changefeedID,
		db:           db,
		dmlWorker:    make([]*worker.MysqlDMLWorker, workerCount),
		workerCount:  workerCount,
		statistics:   metrics.NewStatistics(changefeedID, "TxnSink"),
		isNormal:     1,
	}
	for i := 0; i < workerCount; i++ {
		mysqlSink.dmlWorker[i] = worker.NewMysqlDMLWorker(ctx, db, cfg, i, mysqlSink.changefeedID, mysqlSink.statistics)
	}
	mysqlSink.ddlWorker = worker.NewMysqlDDLWorker(ctx, db, cfg, mysqlSink.changefeedID, mysqlSink.statistics)
	return &mysqlSink, nil
}

func (s *MysqlSink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < s.workerCount; i++ {
		g.Go(func() error {
			return s.dmlWorker[i].Run()
		})
	}
	err := g.Wait()
	atomic.StoreUint32(&s.isNormal, 0)
	return errors.Trace(err)
}

// for test
func NewMysqlSinkWithDBAndConfig(ctx context.Context, changefeedID common.ChangeFeedID, workerCount int, cfg *mysql.MysqlConfig, db *sql.DB) (*MysqlSink, error) {
	mysqlSink := MysqlSink{
		changefeedID: changefeedID,
		dmlWorker:    make([]*worker.MysqlDMLWorker, workerCount),
		workerCount:  workerCount,
		statistics:   metrics.NewStatistics(changefeedID, "TxnSink"),
		isNormal:     1,
	}

	for i := 0; i < workerCount; i++ {
		mysqlSink.dmlWorker[i] = worker.NewMysqlDMLWorker(ctx, db, cfg, i, mysqlSink.changefeedID, mysqlSink.statistics)
	}
	mysqlSink.ddlWorker = worker.NewMysqlDDLWorker(ctx, db, cfg, mysqlSink.changefeedID, mysqlSink.statistics)
	mysqlSink.db = db

	go mysqlSink.Run(ctx)

	return &mysqlSink, nil
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
	// Considering that the parity of tableID is not necessarily even,
	// directly dividing by the number of buckets may cause unevenness between buckets.
	// Therefore, we first take the modulus of the prime number and then take the modulus of the bucket.
	index := int64(event.PhysicalTableID) % prime % int64(s.workerCount)
	s.dmlWorker[index].GetEventChan() <- event
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

func (s *MysqlSink) AddCheckpointTs(ts uint64) {}

func (s *MysqlSink) GetStartTsList(tableIds []int64, startTsList []int64) ([]int64, error) {
	startTsList, err := s.ddlWorker.GetStartTsList(tableIds, startTsList)
	if err != nil {
		atomic.StoreUint32(&s.isNormal, 0)
		return nil, err
	}
	return startTsList, nil
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
		log.Warn("close mysql sink db meet error", zap.Any("changefeed", s.changefeedID.String()), zap.Error(err))
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

	sink, _ := NewMysqlSinkWithDBAndConfig(ctx, changefeedID, 8, cfg, db)
	return sink, mock
}
