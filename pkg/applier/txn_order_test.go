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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package applier

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/pingcap/ticdc/downstreamadapter/sink/mysql"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/redo/reader"
	mysqlsink "github.com/pingcap/ticdc/pkg/sink/mysql"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestTxnOrderRedoReplay(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	cfg := mysqlsink.New()
	cfg.WorkerCount = 4
	cfg.MaxTxnRow = 2
	cfg.MaxAllowedPacket = int64(vardef.DefMaxAllowedPacket)
	cfg.CachePrepStmts = false
	cfg.MultiStmtEnable = false
	cfg.BatchDMLEnable = false
	cfg.DMLMaxRetry = 1
	cfg.TxnOrderFilter, err = config.ParseTxnOrderTables([]string{"test.ordered"}, false)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	sink := dmysql.NewMySQLSink(ctx, common.NewChangefeedID4Test("test", "ordered-redo"), cfg, db, false, false, time.Minute, 0)
	done := make(chan error, 1)
	go func() { done <- sink.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		mock.ExpectClose()
		sink.Close()
		<-done
		require.NoError(t, mock.ExpectationsWereMet())
	})
	ap := NewRedoApplier(&RedoApplierConfig{})
	ap.mysqlSink = sink
	ap.needRecoveryInfo = false
	ap.txnOrderFilter = cfg.TxnOrderFilter
	defer ap.spillStore.Cleanup()

	// Spill rows are restored as complete table transactions, even above max-txn-row.
	mock.ExpectBegin()
	for id := int64(1); id <= 4; id++ {
		commitTs := uint64(110)
		if id == 4 {
			commitTs = 120
		}
		row := &commonEvent.RedoDMLEvent{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs: commitTs - 1, CommitTs: commitTs,
				Table:        &common.TableName{Schema: "test", Table: "ordered", TableID: 42},
				Columns:      []*commonEvent.RedoColumn{{Name: "id", Type: mysql.TypeLonglong}},
				IndexColumns: [][]int{{0}},
			},
			Columns: []commonEvent.RedoColumnValue{{Value: id, Flag: newFlag(mysql.PriKeyFlag)}},
		}
		require.NoError(t, ap.applyRow(row, 100))
		if id == 4 {
			mock.ExpectBegin()
		}
		mock.ExpectExec("INSERT INTO " + "`test`.`ordered` (`id`) VALUES (?)").WithArgs(id).WillReturnResult(sqlmock.NewResult(0, 1))
		if id == 3 || id == 4 {
			mock.ExpectCommit()
		}
	}
	require.NoError(t, ap.waitTableFlush(ctx, 42, 120))
	partition := &common.TableName{Schema: "test", Table: "ordered", TableID: 43, IsPartition: true}
	require.ErrorContains(t, ap.applyRow(&commonEvent.RedoDMLEvent{Row: &commonEvent.DMLEventInRedoLog{Table: partition}}, 100), "does not support partitioned table")
	require.ErrorContains(t, ap.applyDDL(ctx, &commonEvent.RedoDDLEvent{
		Type: byte(timodel.ActionCreateTable), TableName: *partition, DDL: &commonEvent.DDLEventInRedoLog{CommitTs: 130},
	}, 100), "does not support partitioned table")
}

func TestTxnOrderRedoInvalidRules(t *testing.T) {
	original := createRedoReader
	createRedoReader = func(context.Context, *RedoApplierConfig) (reader.RedoLogReader, error) {
		return NewMockReader(0, 1, nil, nil), nil
	}
	defer func() { createRedoReader = original }()
	ap := NewRedoApplier(&RedoApplierConfig{
		SinkURI:    "mysql://127.0.0.1:1/",
		SinkConfig: &config.SinkConfig{MySQLConfig: &config.MySQLConfig{TxnOrderTables: []string{"invalid"}}},
	})
	defer ap.spillStore.Cleanup()
	require.ErrorContains(t, ap.Apply(context.Background()), "txn-order-tables")
}
