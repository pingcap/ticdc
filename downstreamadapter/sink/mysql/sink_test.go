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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func getMysqlSink() (context.Context, *Sink, sqlmock.Sqlmock) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	cfg := mysql.New()
	cfg.WorkerCount = 1
	cfg.DMLMaxRetry = 1
	cfg.MaxAllowedPacket = int64(vardef.DefMaxAllowedPacket)
	cfg.CachePrepStmts = false

	sink := newMySQLSink(ctx, changefeedID, cfg, db, false)
	return ctx, sink, mock
}

// getMysqlSinkWithDDLTs creates a sink with DDL-ts feature enabled for testing GetTableRecoveryInfo
func getMysqlSinkWithDDLTs() (context.Context, *Sink, sqlmock.Sqlmock) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	cfg := mysql.New()
	cfg.WorkerCount = 1
	cfg.DMLMaxRetry = 1
	cfg.MaxAllowedPacket = int64(vardef.DefMaxAllowedPacket)
	cfg.CachePrepStmts = false
	cfg.EnableDDLTs = true // Enable DDL-ts feature for testing

	sink := newMySQLSink(ctx, changefeedID, cfg, db, false)
	return ctx, sink, mock
}

func MysqlSinkForTest() (*Sink, sqlmock.Sqlmock) {
	ctx, sink, mock := getMysqlSink()
	go sink.Run(ctx)

	return sink, mock
}

func MysqlSinkForTestWithMaxTxnRows(maxTxnRows int) (*Sink, sqlmock.Sqlmock) {
	ctx, sink, mock := getMysqlSink()
	sink.maxTxnRows = maxTxnRows
	go sink.Run(ctx)
	return sink, mock
}

// Test callback and tableProgress works as expected after AddDMLEvent
func TestMysqlSinkBasicFunctionality(t *testing.T) {
	sink, mock := MysqlSinkForTest()

	var count atomic.Int64

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 1,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count.Add(1) },
		},
	}

	ddlEvent2 := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count.Add(1) },
		},
	}

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')", "insert into t values (2, 'test2');")
	dmlEvent.PostTxnFlushed = []func(){
		func() { count.Add(1) },
	}
	dmlEvent.CommitTs = 2

	// Step 1: FlushDDLTsPre - Create ddl_ts table and insert pre-record (finished=0)
	mock.ExpectBegin()
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS tidb_cdc").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("USE tidb_cdc").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS ddl_ts_v1
		(
			ticdc_cluster_id varchar (255),
			changefeed varchar(255),
			ddl_ts varchar(18),
			table_id bigint(21),
			finished bool,
			is_syncpoint bool,
			INDEX (ticdc_cluster_id, changefeed, table_id),
			PRIMARY KEY (ticdc_cluster_id, changefeed, table_id)
		);`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 0, 0, 0), ('default', 'test/test', '1', 1, 0, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Step 2: execDDLWithMaxRetries - Execute the actual DDL
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table t (id int primary key, name varchar(32));").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Step 3: FlushDDLTs - Update ddl_ts record (finished=1)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 0, 1, 0), ('default', 'test/test', '1', 1, 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectExec("BEGIN;INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?);COMMIT;").
		WithArgs(1, "test", 2, "test2").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := sink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	sink.AddDMLEvent(dmlEvent)
	time.Sleep(1 * time.Second)

	ddlEvent2.PostFlush()

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)

	require.Equal(t, count.Load(), int64(3))
}

// test the situation meets error when executing DML
// whether the sink state is correct
func TestMysqlSinkMeetsDMLError(t *testing.T) {
	sink, mock := MysqlSinkForTest()
	var count atomic.Int64

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')", "insert into t values (2, 'test2');")
	dmlEvent.PostTxnFlushed = []func(){
		func() { count.Add(1) },
	}

	mock.ExpectExec("BEGIN;INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?);COMMIT;").
		WithArgs(1, "test", 2, "test2").
		WillReturnError(errors.New("connect: connection refused"))

	sink.AddDMLEvent(dmlEvent)

	time.Sleep(1 * time.Second)

	err := mock.ExpectationsWereMet()
	require.NoError(t, err)

	require.Equal(t, count.Load(), int64(0))
	require.False(t, sink.IsNormal())
}

// test the situation meets error when executing DDL
// whether the sink state is correct
func TestMysqlSinkMeetsDDLError(t *testing.T) {
	sink, mock := MysqlSinkForTest()

	var count atomic.Int64

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count.Add(1) },
		},
	}

	// Step 1: FlushDDLTsPre - Create ddl_ts table and insert pre-record (finished=0)
	mock.ExpectBegin()
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS tidb_cdc").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("USE tidb_cdc").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS ddl_ts_v1
		(
			ticdc_cluster_id varchar (255),
			changefeed varchar(255),
			ddl_ts varchar(18),
			table_id bigint(21),
			finished bool,
			is_syncpoint bool,
			INDEX (ticdc_cluster_id, changefeed, table_id),
			PRIMARY KEY (ticdc_cluster_id, changefeed, table_id)
		);`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '2', 0, 0, 0), ('default', 'test/test', '2', 1, 0, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Step 2: execDDLWithMaxRetries - Execute the actual DDL (will fail)
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table t (id int primary key, name varchar(32));").WillReturnError(errors.New("connect: connection refused"))
	mock.ExpectRollback()

	err := sink.WriteBlockEvent(ddlEvent)
	require.Error(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)

	require.Equal(t, count.Load(), int64(0))

	require.Equal(t, sink.IsNormal(), false)
}

func TestMysqlSinkFlushLargeBatchEvent(t *testing.T) {
	sink, mock := MysqlSinkForTestWithMaxTxnRows(4)

	var count atomic.Int64

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	// Generate 10 insert statements for the first event
	var insertStatements1 []string
	for i := 1; i <= 10; i++ {
		insertStatements1 = append(insertStatements1, fmt.Sprintf("insert into t values (%d, 'test%d')", i, i))
	}

	// Generate 10 insert statements for the second event
	var insertStatements2 []string
	for i := 11; i <= 20; i++ {
		insertStatements2 = append(insertStatements2, fmt.Sprintf("insert into t values (%d, 'test%d')", i, i))
	}

	// Create two DML events, each with 10 rows (exceeding MaxTxnRow=4)
	dmlEvent1 := helper.DML2Event("test", "t", insertStatements1...)
	dmlEvent1.PostTxnFlushed = []func(){
		func() { count.Add(1) },
	}
	dmlEvent1.CommitTs = 2

	dmlEvent2 := helper.DML2Event("test", "t", insertStatements2...)
	dmlEvent2.PostTxnFlushed = []func(){
		func() { count.Add(1) },
	}
	dmlEvent2.CommitTs = 3

	// Verify that each event has 10 rows
	require.Equal(t, int32(10), dmlEvent1.Length, "First event should contain 10 rows")
	require.Equal(t, int32(10), dmlEvent2.Length, "Second event should contain 10 rows")

	// Set up mock expectations for DDL
	mock.ExpectExec("BEGIN;INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?),(?,?),(?,?),(?,?),(?,?),(?,?),(?,?),(?,?),(?,?);COMMIT;").
		WithArgs(1, "test1", 2, "test2", 3, "test3", 4, "test4", 5, "test5", 6, "test6", 7, "test7", 8, "test8", 9, "test9", 10, "test10").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec("BEGIN;INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?),(?,?),(?,?),(?,?),(?,?),(?,?),(?,?),(?,?),(?,?);COMMIT;").
		WithArgs(11, "test11", 12, "test12", 13, "test13", 14, "test14", 15, "test15", 16, "test16", 17, "test17", 18, "test18", 19, "test19", 20, "test20").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Add DML events
	sink.AddDMLEvent(dmlEvent1)
	sink.AddDMLEvent(dmlEvent2)

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify all expectations were met
	err := mock.ExpectationsWereMet()
	require.NoError(t, err)

	require.Equal(t, int64(2), count.Load(), "Should have executed 2 callbacks (2 DML events)")
}

// TestGetTableRecoveryInfo_StartTsGreaterThanDDLTs tests the critical scenario where
// input startTs is greater than ddlTs from the ddl_ts table.
// This happens when the table has already progressed beyond the crash point.
// In this case, all skip flags should be reset to false.
func TestGetTableRecoveryInfo_StartTsGreaterThanDDLTs(t *testing.T) {
	_, sink, mock := getMysqlSinkWithDDLTs()

	// Test scenario:
	// - Table 1: ddlTs=100, skipSyncpoint=true, skipDML=true, but input startTs=150
	//   Expected: Return startTs=150, reset both skip flags to false
	// - Table 2: ddlTs=200, skipSyncpoint=false, skipDML=false, input startTs=150
	//   Expected: Return startTs=200, keep skip flags as is
	// - Table 3: ddlTs=300, skipSyncpoint=true, skipDML=true, input startTs=300
	//   Expected: Return startTs=300, keep skip flags as is (equal case)

	tableIDs := []int64{1, 2, 3}
	inputStartTsList := []int64{150, 150, 300}

	// Mock the ddl_ts query
	expectedQuery := "SELECT table_id, ddl_ts, finished, is_syncpoint FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed, table_id) IN (('default', 'test/test', 1), ('default', 'test/test', 2), ('default', 'test/test', 3))"

	rows := sqlmock.NewRows([]string{"table_id", "ddl_ts", "finished", "is_syncpoint"}).
		AddRow(1, 100, false, false). // Unfinished DDL, skipDML should be true initially
		AddRow(2, 200, true, false).  // Finished DDL, skipSyncpoint=false
		AddRow(3, 250, false, false)  // Unfinished DDL, input startTs=300 > ddlTs=250

	mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
	mock.ExpectClose() // Expect database close when sink.Close() is called

	// Call GetTableRecoveryInfo
	resultStartTsList, skipSyncpointList, skipDMLList, err := sink.GetTableRecoveryInfo(tableIDs, inputStartTsList, false)

	require.NoError(t, err)
	require.Len(t, resultStartTsList, 3)
	require.Len(t, skipSyncpointList, 3)
	require.Len(t, skipDMLList, 3)

	// Table 1: startTs > ddlTs, should reset all skip flags
	require.Equal(t, int64(150), resultStartTsList[0], "Table 1: Should use input startTs (150) instead of ddlTs (100)")
	require.False(t, skipSyncpointList[0], "Table 1: skipSyncpoint should be reset to false when startTs > ddlTs")
	require.False(t, skipDMLList[0], "Table 1: skipDML should be reset to false when startTs > ddlTs")

	// Table 2: startTs < ddlTs, should use ddlTs and keep original skip flags
	require.Equal(t, int64(200), resultStartTsList[1], "Table 2: Should use ddlTs (200) instead of input startTs (150)")
	require.False(t, skipSyncpointList[1], "Table 2: skipSyncpoint should be false (finished DDL)")
	require.False(t, skipDMLList[1], "Table 2: skipDML should be false (finished DDL)")

	// Table 3: startTs > ddlTs, should reset skip flags
	require.Equal(t, int64(300), resultStartTsList[2], "Table 3: Should use input startTs (300) instead of ddlTs (250)")
	require.False(t, skipSyncpointList[2], "Table 3: skipSyncpoint should be reset to false when startTs > ddlTs")
	require.False(t, skipDMLList[2], "Table 3: skipDML should be reset to false when startTs > ddlTs")

	// Clean up
	sink.Close(false)

	// Check all mock expectations were met (after closing)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestGetTableRecoveryInfo_RemoveDDLTs tests the scenario where removeDDLTs=true
// This happens when removing a changefeed, and we need to clean up ddl_ts records.
func TestGetTableRecoveryInfo_RemoveDDLTs(t *testing.T) {
	_, sink, mock := getMysqlSinkWithDDLTs()

	tableIDs := []int64{1, 2, 3}
	inputStartTsList := []int64{100, 200, 300}

	// Mock the DELETE query for removing ddl_ts records
	// Note: The actual SQL uses IN syntax
	expectedDeleteQuery := "DELETE FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed) IN (('default', 'test/test'))"
	mock.ExpectBegin()
	mock.ExpectExec(expectedDeleteQuery).WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectCommit()
	mock.ExpectClose() // Expect database close when sink.Close() is called

	// Call GetTableRecoveryInfo with removeDDLTs=true
	resultStartTsList, skipSyncpointList, skipDMLList, err := sink.GetTableRecoveryInfo(tableIDs, inputStartTsList, true)

	require.NoError(t, err)
	require.Len(t, resultStartTsList, 3)
	require.Len(t, skipSyncpointList, 3)
	require.Len(t, skipDMLList, 3)

	// When removeDDLTs=true, should return input startTs with all skip flags = false
	for i := 0; i < 3; i++ {
		require.Equal(t, inputStartTsList[i], resultStartTsList[i], "Should return input startTs unchanged")
		require.False(t, skipSyncpointList[i], "All skip flags should be false when removeDDLTs=true")
		require.False(t, skipDMLList[i], "All skip flags should be false when removeDDLTs=true")
	}

	// Clean up
	sink.Close(false)

	// Check all mock expectations were met (after closing)
	require.NoError(t, mock.ExpectationsWereMet())
}
