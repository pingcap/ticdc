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

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func newTestMysqlWriterForDDLTs(t *testing.T) (*Writer, *sql.DB, sqlmock.Sqlmock) {
	db, mock := newTestMockDBForDDLTs(t)

	ctx := context.Background()
	cfg := New()
	cfg.MaxAllowedPacket = int64(67108864) // 64MB
	cfg.SyncPointRetention = 100 * time.Second
	cfg.MaxTxnRow = 256
	cfg.BatchDMLEnable = true
	cfg.EnableDDLTs = true
	cfg.IsTiDB = false // Default to non-TiDB
	changefeedID := common.NewChangefeedID4Test("test", "test")
	statistics := metrics.NewStatistics(changefeedID, "mysqlSink")
	writer := NewWriter(ctx, 0, db, cfg, changefeedID, statistics)

	// Initialize table schema store
	writer.tableSchemaStore = commonEvent.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{}, common.MysqlSinkType)

	return writer, db, mock
}

func newTestMysqlWriterForDDLTsTiDB(t *testing.T) (*Writer, *sql.DB, sqlmock.Sqlmock) {
	db, mock := newTestMockDBForDDLTs(t)

	ctx := context.Background()
	cfg := New()
	cfg.MaxAllowedPacket = int64(67108864) // 64MB
	cfg.SyncPointRetention = 100 * time.Second
	cfg.MaxTxnRow = 256
	cfg.BatchDMLEnable = true
	cfg.EnableDDLTs = true
	cfg.IsTiDB = true // TiDB downstream
	changefeedID := common.NewChangefeedID4Test("test", "test")
	statistics := metrics.NewStatistics(changefeedID, "mysqlSink")
	writer := NewWriter(ctx, 0, db, cfg, changefeedID, statistics)

	// Initialize table schema store
	writer.tableSchemaStore = commonEvent.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{}, common.MysqlSinkType)

	return writer, db, mock
}

func newTestMockDBForDDLTs(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return
}

// TestGetTableRecoveryInfo_Comprehensive - Comprehensive end-to-end test for GetTableRecoveryInfo
func TestGetTableRecoveryInfo_Comprehensive(t *testing.T) {
	// Test scenarios:
	// 1. Table not exists (should return all 0s)
	// 2. Finished DDL for non-TiDB downstream
	// 3. Finished DDL for TiDB downstream
	// 4. Unfinished DDL (should return ddlTs - 1)
	// 5. Mixed scenarios with duplicate table IDs

	t.Run("TableNotExists", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTs(t)
		defer db.Close()

		tableIDs := []int64{1, 2, 3}
		expectedQuery := "SELECT table_id, ddl_ts, finished, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1), ('default', 'test/test', 2), ('default', 'test/test', 3))"

		tableNotExistsErr := &mysql.MySQLError{
			Number:  1146, // ER_NO_SUCH_TABLE
			Message: "Table 'tidb_cdc.ddl_ts_v1' doesn't exist",
		}
		mock.ExpectQuery(expectedQuery).WillReturnError(tableNotExistsErr)

		startTsList, skipSyncpointAtStartTs, skipDMLAsStartTsList, err := writer.GetTableRecoveryInfo(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 3)
		require.Len(t, skipSyncpointAtStartTs, 3)
		require.Len(t, skipDMLAsStartTsList, 3)

		for i := 0; i < 3; i++ {
			require.Equal(t, int64(0), startTsList[i])
			require.False(t, skipSyncpointAtStartTs[i])
			require.False(t, skipDMLAsStartTsList[i])
		}

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("FinishedDDL_NonTiDB", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTs(t)
		defer db.Close()

		tableIDs := []int64{1, 2}
		expectedQuery := "SELECT table_id, ddl_ts, finished, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1), ('default', 'test/test', 2))"

		// Mock query results: finished DDL with different syncpoint settings
		rows := sqlmock.NewRows([]string{"table_id", "ddl_ts", "finished", "is_syncpoint"}).
			AddRow(1, 100, true, true).
			AddRow(2, 200, true, false)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		startTsList, skipSyncpointAtStartTs, skipDMLAsStartTsList, err := writer.GetTableRecoveryInfo(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 2)
		require.Len(t, skipSyncpointAtStartTs, 2)
		require.Len(t, skipDMLAsStartTsList, 2)

		require.Equal(t, int64(100), startTsList[0])
		require.True(t, skipSyncpointAtStartTs[0])
		require.False(t, skipDMLAsStartTsList[0])
		require.Equal(t, int64(200), startTsList[1])
		require.False(t, skipSyncpointAtStartTs[1])
		require.False(t, skipDMLAsStartTsList[1])

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("FinishedDDL_TiDB", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTsTiDB(t)
		defer db.Close()

		tableIDs := []int64{1}
		expectedQuery := "SELECT table_id, ddl_ts, finished, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1))"

		rows := sqlmock.NewRows([]string{"table_id", "ddl_ts", "finished", "is_syncpoint"}).
			AddRow(1, 100, true, true)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		startTsList, skipSyncpointAtStartTs, skipDMLAsStartTsList, err := writer.GetTableRecoveryInfo(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 1)
		require.Len(t, skipSyncpointAtStartTs, 1)
		require.Len(t, skipDMLAsStartTsList, 1)

		require.Equal(t, int64(100), startTsList[0])
		require.True(t, skipSyncpointAtStartTs[0])
		require.False(t, skipDMLAsStartTsList[0])

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("UnfinishedDDL", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTsTiDB(t)
		defer db.Close()

		tableIDs := []int64{1}
		expectedQuery := "SELECT table_id, ddl_ts, finished, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1))"

		// Mock DDL TS query result: unfinished DDL (is_syncpoint=false)
		rows := sqlmock.NewRows([]string{"table_id", "ddl_ts", "finished", "is_syncpoint"}).
			AddRow(1, 100, false, false)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		startTsList, skipSyncpointAtStartTs, skipDMLAsStartTsList, err := writer.GetTableRecoveryInfo(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 1)
		require.Len(t, skipSyncpointAtStartTs, 1)
		require.Len(t, skipDMLAsStartTsList, 1)

		require.Equal(t, int64(99), startTsList[0]) // Should use ddlTs - 1
		require.False(t, skipSyncpointAtStartTs[0])
		require.True(t, skipDMLAsStartTsList[0]) // Should skip DML at startTs+1

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("UnfinishedSyncpoint_DDLFinished", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTsTiDB(t)
		defer db.Close()

		tableIDs := []int64{1}
		expectedQuery := "SELECT table_id, ddl_ts, finished, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1))"

		// Mock query result: DDL finished, but Syncpoint not finished (is_syncpoint=true, finished=false)
		// This happens when crash occurs after FlushDDLTsPre(Syncpoint) but before FlushDDLTs(Syncpoint)
		rows := sqlmock.NewRows([]string{"table_id", "ddl_ts", "finished", "is_syncpoint"}).
			AddRow(1, 100, false, true)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		startTsList, skipSyncpointAtStartTs, skipDMLAsStartTsList, err := writer.GetTableRecoveryInfo(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 1)
		require.Len(t, skipSyncpointAtStartTs, 1)
		require.Len(t, skipDMLAsStartTsList, 1)

		// Should start from ddlTs (not ddlTs-1) because DDL is already finished
		require.Equal(t, int64(100), startTsList[0])
		// Should receive syncpoint event
		require.False(t, skipSyncpointAtStartTs[0])
		// DML should not be skipped
		require.False(t, skipDMLAsStartTsList[0])

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("DuplicateTableIDs", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTs(t)
		defer db.Close()

		// Test with duplicate table IDs
		tableIDs := []int64{1, 1, 2, 1}
		// The actual query will include duplicate entries for table ID 1
		expectedQuery := "SELECT table_id, ddl_ts, finished, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1), ('default', 'test/test', 1), ('default', 'test/test', 2), ('default', 'test/test', 1))"

		rows := sqlmock.NewRows([]string{"table_id", "ddl_ts", "finished", "is_syncpoint"}).
			AddRow(1, 100, true, true).
			AddRow(2, 200, true, false)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		startTsList, skipSyncpointAtStartTs, skipDMLAsStartTsList, err := writer.GetTableRecoveryInfo(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 4)
		require.Len(t, skipSyncpointAtStartTs, 4)
		require.Len(t, skipDMLAsStartTsList, 4)

		// All positions with table ID 1 should have the same values
		require.Equal(t, int64(100), startTsList[0])
		require.True(t, skipSyncpointAtStartTs[0])
		require.False(t, skipDMLAsStartTsList[0])
		require.Equal(t, int64(100), startTsList[1])
		require.True(t, skipSyncpointAtStartTs[1])
		require.Equal(t, int64(100), startTsList[3])
		require.True(t, skipSyncpointAtStartTs[3])

		// Position with table ID 2 should have different values
		require.Equal(t, int64(200), startTsList[2])
		require.False(t, skipSyncpointAtStartTs[2])

		require.NoError(t, mock.ExpectationsWereMet())
	})
}
