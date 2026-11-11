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
	"github.com/pingcap/ticdc/downstreamadapter/sink/mysql/causality"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	mysqlsink "github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestRoutedRedoPreservesOrdering(t *testing.T) {
	f, err := config.ParseTxnOrderTables([]string{"app.orders"}, false)
	require.NoError(t, err)
	ap := NewRedoApplier(&RedoApplierConfig{})
	ap.needRecoveryInfo = false
	ap.txnOrderFilter = f
	ap.hasTableRoute = true
	defer ap.spillStore.Cleanup()
	source := &common.TableName{Schema: "app", Table: "orders", TableID: 42}
	target := &common.TableName{Schema: "archive", Table: "orders_copy", TableID: 42}
	for id := int64(1); id <= 2; id++ {
		row := &commonEvent.RedoDMLEvent{
			Row: &commonEvent.DMLEventInRedoLog{
				StartTs: uint64(100 + id), CommitTs: uint64(200 + id), Table: target, SourceTable: source,
				Columns: []*commonEvent.RedoColumn{{Name: "id", Type: mysql.TypeLonglong}}, IndexColumns: [][]int{{0}},
			},
			Columns: []commonEvent.RedoColumnValue{{Value: id, Flag: newFlag(mysql.PriKeyFlag)}},
		}
		require.NoError(t, ap.applyRow(row, 0))
	}
	batch, _, err := ap.eventsGroup[42].PrepareResolve(300, ap.spillStore.ResolveLimit())
	require.NoError(t, err)
	require.Len(t, batch.Messages, 2)
	var keys map[uint64]struct{}
	for _, message := range batch.Messages {
		event := message.ToDMLEvent()
		require.Equal(t, "app", event.TableInfo.GetSchemaName())
		require.Equal(t, "orders", event.TableInfo.GetTableName())
		require.Equal(t, "archive", event.TableInfo.GetTargetSchemaName())
		require.Equal(t, "orders_copy", event.TableInfo.GetTargetTableName())
		require.Contains(t, event.TableInfo.GetPreInsertSQL(), "`archive`.`orders_copy`")
		current := causality.ConflictKeys(event, f)
		require.Equal(t, map[uint64]struct{}{uint64(42) | (1 << 63): {}}, current)
		if keys != nil {
			require.Equal(t, keys, current)
		}
		keys = current
		event.PostFlush()
	}
	require.NoError(t, batch.Ack())

	update := &commonEvent.RedoDMLEvent{Row: &commonEvent.DMLEventInRedoLog{Table: target, SourceTable: source}}
	deleted, inserted, err := splitUpdateEvent(update)
	require.NoError(t, err)
	require.Equal(t, source, deleted.Row.SourceTable)
	require.Equal(t, source, inserted.Row.SourceTable)
	require.ErrorContains(t, ap.verifyRedoTable(nil, target), "lacks source table identity")
	require.NoError(t, ap.verifyRedoTable(source, target))
}

func TestRedoMultiTablePartitionMetadata(t *testing.T) {
	regular := common.NewTableInfo4Decoder("app", &model.TableInfo{ID: 1, Name: ast.NewCIStr("regular")})
	partitioned := common.NewTableInfo4Decoder("app", &model.TableInfo{ID: 2, Name: ast.NewCIStr("ordered_p"), Partition: &model.PartitionInfo{Enable: true}})
	regular = regular.CloneWithRouting("archive", "regular_copy")
	partitioned = partitioned.CloneWithRouting("archive", "partition_copy")
	for _, action := range []model.ActionType{model.ActionCreateTables, model.ActionRenameTables} {
		ddl := (&commonEvent.DDLEvent{
			Type: byte(action), FinishedTs: 200, TableInfo: regular,
			MultipleTableInfos: []*common.TableInfo{regular, partitioned},
		}).ToRedoLog().RedoDDL
		data, err := ddl.MarshalMsg(nil)
		require.NoError(t, err)
		restored := &commonEvent.RedoDDLEvent{}
		_, err = restored.UnmarshalMsg(data)
		require.NoError(t, err)
		require.Len(t, restored.Tables, 3)
		require.Equal(t, "app", restored.Tables[2].Source.Schema)
		require.Equal(t, "ordered_p", restored.Tables[2].Source.Table)
		require.Equal(t, "archive", restored.Tables[2].Target.Schema)
		require.True(t, restored.Tables[2].Source.IsPartition)
		ap := NewRedoApplier(&RedoApplierConfig{})
		ap.needRecoveryInfo = false
		ap.hasTableRoute = true
		ap.txnOrderFilter, err = config.ParseTxnOrderTables([]string{"app.ordered_*"}, false)
		require.NoError(t, err)
		// No sink is installed: rejection must happen before any downstream work.
		require.ErrorContains(t, ap.applyDDL(context.Background(), restored, 100), "does not support partitioned table")
		require.Zero(t, ap.appliedDDLCount)
		require.NoError(t, ap.spillStore.Cleanup())
	}
}

func TestLegacyRedoRenameChecksEveryTable(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		partitioned, queryError, chain bool
	}{
		{"partitioned second table", true, false, false},
		{"regular tables", false, false, false},
		{"query failure", false, true, false},
		{"partitioned table through intermediate rename", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			cfg := mysqlsink.New()
			cfg.WorkerCount = 1
			sink := dmysql.NewMySQLSink(context.Background(), common.NewChangefeedID4Test("test", "legacy-redo"), cfg, db, false, false, time.Minute, 0)
			defer func() { mock.ExpectClose(); sink.Close(); require.NoError(t, mock.ExpectationsWereMet()) }()
			ap := NewRedoApplier(&RedoApplierConfig{})
			ap.mysqlSink = sink
			ap.needRecoveryInfo = false
			ap.txnOrderFilter, err = config.ParseTxnOrderTables([]string{"test.ordered_*"}, false)
			require.NoError(t, err)
			defer ap.spillStore.Cleanup()
			query := "SELECT EXISTS(SELECT 1 FROM information_schema.partitions WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL)"
			mock.ExpectQuery(query).WithArgs("test", "first").WillReturnRows(sqlmock.NewRows([]string{"partitioned"}).AddRow(tc.chain))
			if !tc.chain {
				second := mock.ExpectQuery(query).WithArgs("test", "second")
				if tc.queryError {
					second.WillReturnError(errors.ErrMySQLQueryError.GenWithStack("credential-sentinel"))
				} else {
					second.WillReturnRows(sqlmock.NewRows([]string{"partitioned"}).AddRow(tc.partitioned))
				}
			}
			ddl := &commonEvent.RedoDDLEvent{Type: byte(model.ActionRenameTables), DDL: &commonEvent.DDLEventInRedoLog{
				CommitTs: 200, Query: "RENAME TABLE test.first TO test.ordered_first, test.second TO test.ordered_second",
			}}
			if tc.chain {
				ddl.DDL.Query = "RENAME TABLE test.first TO test.tmp, test.second TO test.first, test.tmp TO test.ordered_second"
			}
			// Marshal without the optional metadata, as older writers did.
			data, err := ddl.MarshalMsg(nil)
			require.NoError(t, err)
			legacy := &commonEvent.RedoDDLEvent{}
			_, err = legacy.UnmarshalMsg(data)
			require.NoError(t, err)
			require.Nil(t, legacy.Tables)
			if tc.partitioned || tc.queryError {
				err = ap.applyDDL(context.Background(), legacy, 100)
				require.Error(t, err)
				require.Zero(t, ap.appliedDDLCount)
				if tc.partitioned {
					require.ErrorContains(t, err, "does not support partitioned table")
				}
				if tc.queryError {
					require.True(t, errors.ErrMySQLQueryError.Equal(err))
					require.NotContains(t, err.Error(), "credential-sentinel")
				}
			} else {
				require.NoError(t, ap.verifyTxnOrderDDL(context.Background(), legacy))
			}
		})
	}
}

func TestLegacyRedoCreateTablesChecksEveryPartitionClause(t *testing.T) {
	ap := NewRedoApplier(&RedoApplierConfig{})
	defer ap.spillStore.Cleanup()
	var err error
	ap.txnOrderFilter, err = config.ParseTxnOrderTables([]string{"test.ordered_*"}, false)
	require.NoError(t, err)
	ddl := &commonEvent.RedoDDLEvent{Type: byte(model.ActionCreateTables), DDL: &commonEvent.DDLEventInRedoLog{
		Query: "CREATE TABLE test.regular(id INT PRIMARY KEY); CREATE TABLE test.ordered_p(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2",
	}}
	require.ErrorContains(t, ap.verifyTxnOrderDDL(context.Background(), ddl), "does not support partitioned table")
	ddl.DDL.Query = "CREATE TABLE test.regular(id INT PRIMARY KEY); CREATE TABLE test.ordered_p(id INT PRIMARY KEY)"
	require.NoError(t, ap.verifyTxnOrderDDL(context.Background(), ddl))
	ddl.DDL.Query = "CREATE TABLE test.regular(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2; CREATE TABLE test.ordered_p LIKE test.regular"
	require.ErrorContains(t, ap.verifyTxnOrderDDL(context.Background(), ddl), "does not support partitioned table")
	ddl.Type = byte(model.ActionRenameTables)
	ddl.DDL.Query = "RENAME TABLE test.first TO test.ordered_tmp, test.ordered_tmp TO test.unordered"
	require.NoError(t, ap.verifyTxnOrderDDL(context.Background(), ddl))
}
