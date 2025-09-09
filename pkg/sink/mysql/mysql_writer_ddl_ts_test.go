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
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/stretchr/testify/require"
)

func newTestMysqlWriterForDDLTs(t *testing.T) (*Writer, *sql.DB, sqlmock.Sqlmock) {
	db, mock := newTestMockDBForDDLTs(t)

	ctx := context.Background()
	cfg := &Config{
		MaxAllowedPacket:   int64(67108864), // 64MB
		SyncPointRetention: 100 * time.Second,
		MaxTxnRow:          256,
		BatchDMLEnable:     true,
		EnableDDLTs:        true,
		IsTiDB:             false, // Default to non-TiDB
	}
	changefeedID := common.NewChangefeedID4Test("test", "test")
	statistics := metrics.NewStatistics(changefeedID, "mysqlSink")
	writer := NewWriter(ctx, db, cfg, changefeedID, statistics, false)

	// Initialize table schema store
	writer.tableSchemaStore = util.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{}, common.MysqlSinkType)

	return writer, db, mock
}

func newTestMysqlWriterForDDLTsTiDB(t *testing.T) (*Writer, *sql.DB, sqlmock.Sqlmock) {
	db, mock := newTestMockDBForDDLTs(t)

	ctx := context.Background()
	cfg := &Config{
		MaxAllowedPacket:   int64(67108864), // 64MB
		SyncPointRetention: 100 * time.Second,
		MaxTxnRow:          256,
		BatchDMLEnable:     true,
		EnableDDLTs:        true,
		IsTiDB:             true, // TiDB downstream
	}
	changefeedID := common.NewChangefeedID4Test("test", "test")
	statistics := metrics.NewStatistics(changefeedID, "mysqlSink")
	writer := NewWriter(ctx, db, cfg, changefeedID, statistics, false)

	// Initialize table schema store
	writer.tableSchemaStore = util.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{}, common.MysqlSinkType)

	return writer, db, mock
}

func newTestMockDBForDDLTs(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return
}

// TestGetStartTsList_Comprehensive - Comprehensive end-to-end test for GetStartTsList
func TestGetStartTsList_Comprehensive(t *testing.T) {
	// Test scenarios:
	// 1. Table not exists (should return all 0s)
	// 2. Finished DDL for non-TiDB downstream
	// 3. Finished DDL for TiDB downstream
	// 4. Unfinished DDL with DDL job executed
	// 5. Unfinished DDL with DDL job not executed
	// 6. Unfinished DDL with DDL job query failure
	// 7. Mixed scenarios with duplicate table IDs

	t.Run("TableNotExists", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTs(t)
		defer db.Close()

		tableIDs := []int64{1, 2, 3}
		expectedQuery := "SELECT table_id, table_name_in_ddl_job, db_name_in_ddl_job, ddl_ts, finished, created_at, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1), ('default', 'test/test', 2), ('default', 'test/test', 3))"

		tableNotExistsErr := &mysql.MySQLError{
			Number:  1146, // ER_NO_SUCH_TABLE
			Message: "Table 'tidb_cdc.ddl_ts_v1' doesn't exist",
		}
		mock.ExpectQuery(expectedQuery).WillReturnError(tableNotExistsErr)

		startTsList, isSyncpoints, err := writer.GetStartTsList(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 3)
		require.Len(t, isSyncpoints, 3)

		for i := 0; i < 3; i++ {
			require.Equal(t, int64(0), startTsList[i])
			require.False(t, isSyncpoints[i])
		}

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("FinishedDDL_NonTiDB", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTs(t)
		defer db.Close()

		tableIDs := []int64{1, 2}
		expectedQuery := "SELECT table_id, table_name_in_ddl_job, db_name_in_ddl_job, ddl_ts, finished, created_at, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1), ('default', 'test/test', 2))"

		// Mock query results: finished DDL with different syncpoint settings
		rows := sqlmock.NewRows([]string{"table_id", "table_name_in_ddl_job", "db_name_in_ddl_job", "ddl_ts", "finished", "created_at", "is_syncpoint"}).
			AddRow(1, "table1", "test", 100, true, "2023-01-01 10:00:00.000000", true).
			AddRow(2, "table2", "test", 200, true, "2023-01-01 11:00:00.000000", false)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		startTsList, isSyncpoints, err := writer.GetStartTsList(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 2)
		require.Len(t, isSyncpoints, 2)

		require.Equal(t, int64(100), startTsList[0])
		require.True(t, isSyncpoints[0])
		require.Equal(t, int64(200), startTsList[1])
		require.False(t, isSyncpoints[1])

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("FinishedDDL_TiDB", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTsTiDB(t)
		defer db.Close()

		tableIDs := []int64{1}
		expectedQuery := "SELECT table_id, table_name_in_ddl_job, db_name_in_ddl_job, ddl_ts, finished, created_at, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1))"

		rows := sqlmock.NewRows([]string{"table_id", "table_name_in_ddl_job", "db_name_in_ddl_job", "ddl_ts", "finished", "created_at", "is_syncpoint"}).
			AddRow(1, "table1", "test", 100, true, "2023-01-01 10:00:00.000000", true)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		startTsList, isSyncpoints, err := writer.GetStartTsList(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 1)
		require.Len(t, isSyncpoints, 1)

		require.Equal(t, int64(100), startTsList[0])
		require.True(t, isSyncpoints[0])

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("UnfinishedDDL_DDLJobExecuted", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTsTiDB(t)
		defer db.Close()

		tableIDs := []int64{1}
		expectedQuery := "SELECT table_id, table_name_in_ddl_job, db_name_in_ddl_job, ddl_ts, finished, created_at, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1))"

		// Mock DDL TS query result: unfinished DDL
		rows := sqlmock.NewRows([]string{"table_id", "table_name_in_ddl_job", "db_name_in_ddl_job", "ddl_ts", "finished", "created_at", "is_syncpoint"}).
			AddRow(1, "table1", "test", 100, false, "2023-01-01 10:00:00.000000", true)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		// Mock DDL jobs query: DDL job executed after the DDL TS record was created
		ddlJobsQuery := "SELECT CREATE_TIME FROM information_schema.ddl_jobs WHERE DB_NAME = 'test' AND TABLE_NAME = 'table1' order by CREATE_TIME desc limit 1;"
		ddlJobsRows := sqlmock.NewRows([]string{"CREATE_TIME"}).
			AddRow("2023-01-01 10:30:00.000000") // Later than created_at

		mock.ExpectQuery(ddlJobsQuery).WillReturnRows(ddlJobsRows)

		startTsList, isSyncpoints, err := writer.GetStartTsList(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 1)
		require.Len(t, isSyncpoints, 1)

		require.Equal(t, int64(100), startTsList[0]) // Should use ddlTs
		require.True(t, isSyncpoints[0])

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("UnfinishedDDL_DDLJobNotExecuted", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTsTiDB(t)
		defer db.Close()

		tableIDs := []int64{1}
		expectedQuery := "SELECT table_id, table_name_in_ddl_job, db_name_in_ddl_job, ddl_ts, finished, created_at, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1))"

		// Mock DDL TS query result: unfinished DDL
		rows := sqlmock.NewRows([]string{"table_id", "table_name_in_ddl_job", "db_name_in_ddl_job", "ddl_ts", "finished", "created_at", "is_syncpoint"}).
			AddRow(1, "table1", "test", 100, false, "2023-01-01 10:00:00.000000", true)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		// Mock DDL jobs query: DDL job executed before the DDL TS record was created
		ddlJobsQuery := "SELECT CREATE_TIME FROM information_schema.ddl_jobs WHERE DB_NAME = 'test' AND TABLE_NAME = 'table1' order by CREATE_TIME desc limit 1;"
		ddlJobsRows := sqlmock.NewRows([]string{"CREATE_TIME"}).
			AddRow("2023-01-01 09:30:00.000000") // Earlier than created_at

		mock.ExpectQuery(ddlJobsQuery).WillReturnRows(ddlJobsRows)

		startTsList, isSyncpoints, err := writer.GetStartTsList(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 1)
		require.Len(t, isSyncpoints, 1)

		require.Equal(t, int64(99), startTsList[0]) // Should use ddlTs - 1
		require.False(t, isSyncpoints[0])           // Should be false when DDL not executed

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("UnfinishedDDL_DDLJobQueryFailure", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTsTiDB(t)
		defer db.Close()

		tableIDs := []int64{1}
		expectedQuery := "SELECT table_id, table_name_in_ddl_job, db_name_in_ddl_job, ddl_ts, finished, created_at, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1))"

		// Mock DDL TS query result: unfinished DDL
		rows := sqlmock.NewRows([]string{"table_id", "table_name_in_ddl_job", "db_name_in_ddl_job", "ddl_ts", "finished", "created_at", "is_syncpoint"}).
			AddRow(1, "table1", "test", 100, false, "2023-01-01 10:00:00.000000", true)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		// Mock DDL jobs query failure
		ddlJobsQuery := "SELECT CREATE_TIME FROM information_schema.ddl_jobs WHERE DB_NAME = 'test' AND TABLE_NAME = 'table1' order by CREATE_TIME desc limit 1;"
		mock.ExpectQuery(ddlJobsQuery).WillReturnError(sql.ErrNoRows)

		startTsList, isSyncpoints, err := writer.GetStartTsList(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 1)
		require.Len(t, isSyncpoints, 1)

		require.Equal(t, int64(99), startTsList[0]) // Should use ddlTs - 1
		require.False(t, isSyncpoints[0])           // Should be false when query fails

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("DuplicateTableIDs", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTs(t)
		defer db.Close()

		// Test with duplicate table IDs
		tableIDs := []int64{1, 1, 2, 1}
		// The actual query will include duplicate entries for table ID 1
		expectedQuery := "SELECT table_id, table_name_in_ddl_job, db_name_in_ddl_job, ddl_ts, finished, created_at, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1), ('default', 'test/test', 1), ('default', 'test/test', 2), ('default', 'test/test', 1))"

		rows := sqlmock.NewRows([]string{"table_id", "table_name_in_ddl_job", "db_name_in_ddl_job", "ddl_ts", "finished", "created_at", "is_syncpoint"}).
			AddRow(1, "table1", "test", 100, true, "2023-01-01 10:00:00.000000", true).
			AddRow(2, "table2", "test", 200, true, "2023-01-01 11:00:00.000000", false)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		startTsList, isSyncpoints, err := writer.GetStartTsList(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 4)
		require.Len(t, isSyncpoints, 4)

		// All positions with table ID 1 should have the same values
		require.Equal(t, int64(100), startTsList[0])
		require.True(t, isSyncpoints[0])
		require.Equal(t, int64(100), startTsList[1])
		require.True(t, isSyncpoints[1])
		require.Equal(t, int64(100), startTsList[3])
		require.True(t, isSyncpoints[3])

		// Position with table ID 2 should have different values
		require.Equal(t, int64(200), startTsList[2])
		require.False(t, isSyncpoints[2])

		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestGetStartTsList_QueryDDLJobs - Test queryDDLJobs logic specifically
func TestGetStartTsList_QueryDDLJobs(t *testing.T) {
	// Test scenarios for queryDDLJobs:
	// 1. Empty dbNameInDDLJob and tableNameInDDLJob
	// 2. DDL jobs query success with valid time
	// 3. DDL jobs query success but time parsing fails
	// 4. DDL jobs query returns no rows
	// 5. DDL jobs query fails with database error

	t.Run("EmptyDBAndTableName", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTsTiDB(t)
		defer db.Close()

		tableIDs := []int64{1}
		expectedQuery := "SELECT table_id, table_name_in_ddl_job, db_name_in_ddl_job, ddl_ts, finished, created_at, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1))"

		// Mock DDL TS query result: unfinished DDL with empty db and table names
		rows := sqlmock.NewRows([]string{"table_id", "table_name_in_ddl_job", "db_name_in_ddl_job", "ddl_ts", "finished", "created_at", "is_syncpoint"}).
			AddRow(1, "", "", 100, false, "2023-01-01 10:00:00.000000", true)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		startTsList, isSyncpoints, err := writer.GetStartTsList(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 1)
		require.Len(t, isSyncpoints, 1)

		// Should use ddlTs - 1 when db and table names are empty
		require.Equal(t, int64(99), startTsList[0])
		require.False(t, isSyncpoints[0])

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("DDLJobsQuerySuccess", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTsTiDB(t)
		defer db.Close()

		tableIDs := []int64{1}
		expectedQuery := "SELECT table_id, table_name_in_ddl_job, db_name_in_ddl_job, ddl_ts, finished, created_at, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1))"

		// Mock DDL TS query result: unfinished DDL
		rows := sqlmock.NewRows([]string{"table_id", "table_name_in_ddl_job", "db_name_in_ddl_job", "ddl_ts", "finished", "created_at", "is_syncpoint"}).
			AddRow(1, "table1", "test", 100, false, "2023-01-01 10:00:00.000000", true)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		// Mock DDL jobs query: successful query with valid time
		ddlJobsQuery := "SELECT CREATE_TIME FROM information_schema.ddl_jobs WHERE DB_NAME = 'test' AND TABLE_NAME = 'table1' order by CREATE_TIME desc limit 1;"
		ddlJobsRows := sqlmock.NewRows([]string{"CREATE_TIME"}).
			AddRow("2023-01-01 10:30:00.000000") // Later than created_at

		mock.ExpectQuery(ddlJobsQuery).WillReturnRows(ddlJobsRows)

		startTsList, isSyncpoints, err := writer.GetStartTsList(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 1)
		require.Len(t, isSyncpoints, 1)

		require.Equal(t, int64(100), startTsList[0]) // Should use ddlTs
		require.True(t, isSyncpoints[0])

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("DDLJobsQueryNoRows", func(t *testing.T) {
		writer, db, mock := newTestMysqlWriterForDDLTsTiDB(t)
		defer db.Close()

		tableIDs := []int64{1}
		expectedQuery := "SELECT table_id, table_name_in_ddl_job, db_name_in_ddl_job, ddl_ts, finished, created_at, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1))"

		// Mock DDL TS query result: unfinished DDL
		rows := sqlmock.NewRows([]string{"table_id", "table_name_in_ddl_job", "db_name_in_ddl_job", "ddl_ts", "finished", "created_at", "is_syncpoint"}).
			AddRow(1, "table1", "test", 100, false, "2023-01-01 10:00:00.000000", true)

		mock.ExpectQuery(expectedQuery).WillReturnRows(rows)

		// Mock DDL jobs query: no rows returned
		ddlJobsQuery := "SELECT CREATE_TIME FROM information_schema.ddl_jobs WHERE DB_NAME = 'test' AND TABLE_NAME = 'table1' order by CREATE_TIME desc limit 1;"
		ddlJobsRows := sqlmock.NewRows([]string{"CREATE_TIME"}) // Empty result set

		mock.ExpectQuery(ddlJobsQuery).WillReturnRows(ddlJobsRows)

		startTsList, isSyncpoints, err := writer.GetStartTsList(tableIDs)

		require.NoError(t, err)
		require.Len(t, startTsList, 1)
		require.Len(t, isSyncpoints, 1)

		require.Equal(t, int64(99), startTsList[0]) // Should use ddlTs - 1
		require.False(t, isSyncpoints[0])

		require.NoError(t, mock.ExpectationsWereMet())
	})
}
