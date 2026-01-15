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
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/tidb/br/pkg/version"
	ticonfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func newTestMysqlWriter(t *testing.T) (*Writer, *sql.DB, sqlmock.Sqlmock) {
	db, mock := newTestMockDB(t)

	ctx := context.Background()
	cfg := New()
	cfg.MaxAllowedPacket = int64(vardef.DefMaxAllowedPacket)
	cfg.SyncPointRetention = 100 * time.Second
	cfg.MaxTxnRow = 256
	cfg.BatchDMLEnable = true
	cfg.EnableDDLTs = defaultEnableDDLTs
	changefeedID := common.NewChangefeedID4Test("test", "test")
	statistics := metrics.NewStatistics(changefeedID, "mysqlSink")
	writer := NewWriter(ctx, 0, db, cfg, changefeedID, statistics)
	// assign a no-op stmt cache to bypass actual DB operations in unit tests
	cache, err := lru.New(prepStmtCacheSize)
	require.NoError(t, err)
	writer.stmtCache = cache

	return writer, db, mock
}

func newTestMysqlWriterForTiDB(t *testing.T) (*Writer, *sql.DB, sqlmock.Sqlmock) {
	db, mock := newTestMockDB(t)

	ctx := context.Background()
	cfg := New()
	cfg.MaxAllowedPacket = int64(vardef.DefMaxAllowedPacket)
	cfg.SyncPointRetention = 100 * time.Second
	cfg.IsTiDB = true
	cfg.EnableDDLTs = defaultEnableDDLTs
	cfg.ServerInfo = version.ParseServerInfo(defaultRunningAddIndexNewSQLVersion)

	changefeedID := common.NewChangefeedID4Test("test", "test")
	statistics := metrics.NewStatistics(changefeedID, "mysqlSink")
	writer := NewWriter(ctx, 0, db, cfg, changefeedID, statistics)

	if kerneltype.IsNextGen() {
		ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
			conf.Instance.TiDBServiceScope = handle.NextGenTargetScope
		})
	}

	return writer, db, mock
}

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return db, mock
}

func TestMysqlWriter_FlushDML(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')", "insert into t values (2, 'test2');")
	dmlEvent.CommitTs = 2
	dmlEvent.ReplicatingTs = 1
	dmlEvent.DispatcherID = common.NewDispatcherID()

	dmlEvent2 := helper.DML2Event("test", "t", "insert into t values (3, 'test3');")
	dmlEvent2.CommitTs = 3
	dmlEvent2.ReplicatingTs = 1
	dmlEvent2.DispatcherID = dmlEvent.DispatcherID

	mock.ExpectExec("BEGIN;INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?),(?,?);COMMIT;").
		WithArgs(1, "test", 2, "test2", 3, "test3").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := writer.Flush([]*commonEvent.DMLEvent{dmlEvent, dmlEvent2})
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestMysqlWriter_FlushDML_DuplicateEntryRetry(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	writer.cfg.CachePrepStmts = false
	writer.cfg.DMLMaxRetry = 0

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')")
	dmlEvent.CommitTs = 2
	dmlEvent.ReplicatingTs = 1
	dmlEvent.DispatcherID = common.NewDispatcherID()

	// First execution should fail with duplicate entry error
	mock.ExpectExec("BEGIN;INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?);COMMIT;").
		WithArgs(1, "test").
		WillReturnError(fmt.Errorf("Error 1062: Duplicate entry '1' for key 'PRIMARY'"))

	// Second execution should use REPLACE (safe mode) and succeed
	mock.ExpectExec("BEGIN;REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?);COMMIT;").
		WithArgs(1, "test").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := writer.Flush([]*commonEvent.DMLEvent{dmlEvent})
	require.NoError(t, err)

	// Verify that writer is now in error-caused safe mode
	require.True(t, writer.isInErrorCausedSafeMode)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestMysqlWriter_FlushMultiDML(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	// case	1: insert + insert
	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')")
	dmlEvent.CommitTs = 2
	dmlEvent.DispatcherID = common.NewDispatcherID()
	dmlEvent2 := helper.DML2Event("test", "t", "insert into t values (2, 'test2');")
	dmlEvent2.CommitTs = 3
	dmlEvent2.DispatcherID = dmlEvent.DispatcherID

	mock.ExpectExec("BEGIN;INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?);COMMIT;").
		WithArgs(1, "test", 2, "test2").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := writer.Flush([]*commonEvent.DMLEvent{dmlEvent, dmlEvent2})
	require.NoError(t, err)

	/* TODO(hyy): complete all types, including
	insert + update
	insert + delete
	update + insert
	update + delete
	update + update
	delete + insert
	delete + delete
	delete + update
	*/
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// Test flush ddl event
// Ensure the ddl query will be write to the databases
// and the ddl_ts_v1 table will be updated with the ddl_ts and table_id
func TestMysqlWriter_FlushDDLEvent(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

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

	err := writer.FlushDDLEvent(ddlEvent)
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)

	// another flush ddl event
	job = helper.DDL2Job("alter table t add column age int;")
	require.NotNil(t, job)

	ddlEvent = &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{1},
		},
	}

	// Second DDL: Step 1: FlushDDLTsPre - Insert pre-record (finished=0)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '2', 1, 0, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Step 2: execDDLWithMaxRetries - Execute the actual DDL
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("alter table t add column age int;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Step 3: FlushDDLTs - Update ddl_ts record (finished=1)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '2', 1, 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = writer.FlushDDLEvent(ddlEvent)
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestMysqlWriter_Flush_EmptyEvents(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	events := []*commonEvent.DMLEvent{}

	err := writer.Flush(events)
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestMysqlWriter_FlushSyncPointEvent(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	syncPointEvent := &commonEvent.SyncPointEvent{
		CommitTs: 1,
	}
	tableSchemaStore := commonEvent.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{}, common.MysqlSinkType)
	writer.SetTableSchemaStore(tableSchemaStore)

	// First sync point: Step 0: Create syncpoint table (only for first sync point)
	mock.ExpectBegin()
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS tidb_cdc").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("USE tidb_cdc").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS syncpoint_v1
		(
			ticdc_cluster_id varchar (255),
			changefeed varchar(255),
			primary_ts varchar(18),
			secondary_ts varchar(18),
			created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX (created_at),
			PRIMARY KEY (changefeed, primary_ts)
		);`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

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
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '1', -1, 0, 1) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Step 2: SendSyncPointEvent - Send syncpoint
	mock.ExpectBegin()
	mock.ExpectQuery("select @@tidb_current_ts").WillReturnRows(sqlmock.NewRows([]string{"@@tidb_current_ts"}).AddRow(0))
	mock.ExpectExec("insert ignore into tidb_cdc.syncpoint_v1 (ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES ('default', 'test/test', 1, 0)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("set global tidb_external_ts = 0").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Step 3: FlushDDLTs - Update ddl_ts record (finished=1)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '1', -1, 1, 1) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := writer.FlushSyncPointEvent(syncPointEvent)
	require.NoError(t, err)

	syncPointEvent = &commonEvent.SyncPointEvent{
		CommitTs: 2,
	}

	// Second sync point: Step 1: FlushDDLTsPre - Insert pre-record (finished=0)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '2', -1, 0, 1) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Step 2: SendSyncPointEvent
	mock.ExpectBegin()
	mock.ExpectQuery("select @@tidb_current_ts").WillReturnRows(sqlmock.NewRows([]string{"@@tidb_current_ts"}).AddRow(2))
	mock.ExpectExec("insert ignore into tidb_cdc.syncpoint_v1 (ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES ('default', 'test/test', 2, 2)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("set global tidb_external_ts = 2").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Step 3: FlushDDLTs - Update ddl_ts record (finished=1)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '2', -1, 1, 1) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = writer.FlushSyncPointEvent(syncPointEvent)
	require.NoError(t, err)

	// flush multiple sync point events

	syncPointEvent = &commonEvent.SyncPointEvent{
		CommitTs: 3,
	}

	// Third sync point: Step 1: FlushDDLTsPre - Insert pre-record (finished=0)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '3', -1, 0, 1) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Step 2: SendSyncPointEvent
	mock.ExpectBegin()
	mock.ExpectQuery("select @@tidb_current_ts").WillReturnRows(sqlmock.NewRows([]string{"@@tidb_current_ts"}).AddRow(3))
	mock.ExpectExec("insert ignore into tidb_cdc.syncpoint_v1 (ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES ('default', 'test/test', 3, 3)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("set global tidb_external_ts = 3").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Step 3: FlushDDLTs - Update ddl_ts record (finished=1)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '3', -1, 1, 1) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = writer.FlushSyncPointEvent(syncPointEvent)
	require.NoError(t, err)
}

func TestMysqlWriter_RemoveDDLTsTable(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM tidb_cdc.ddl_ts_v1 WHERE (ticdc_cluster_id, changefeed) IN (('default', 'test/test'))").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := writer.RemoveDDLTsItem()
	require.NoError(t, err)
}

func TestWaitAsyncDDLDone_CreateTableLikeShouldQueryDownstreamAddIndexJob(t *testing.T) {
	writer, db, mock := newTestMysqlWriterForTiDB(t)
	defer db.Close()

	event := &commonEvent.DDLEvent{
		Type: byte(timodel.ActionCreateTable),
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		BlockedTableNames: []commonEvent.SchemaTableName{
			{
				SchemaName: "test",
				TableName:  "a",
			},
		},
	}

	expected := fmt.Sprintf(checkRunningAddIndexSQL, "test", "a")
	mock.ExpectQuery(expected).
		WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "JOB_TYPE", "SCHEMA_STATE", "STATE", "QUERY"}))

	writer.waitAsyncDDLDone(event)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Test the async ddl can be write successfully
func TestMysqlWriter_AsyncDDL(t *testing.T) {
	writer, db, mock := newTestMysqlWriterForTiDB(t)
	defer db.Close()

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
		Type:       byte(job.Type),
		FinishedTs: 1,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
	}

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

	mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").WillReturnRows(sqlmock.NewRows([]string{"@ticdc_ts"}).AddRow("2021-05-26 11:33:37.776000"))
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table t (id int primary key, name varchar(32));").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 0, 1, 0), ('default', 'test/test', '1', 1, 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := writer.FlushDDLEvent(ddlEvent)
	require.NoError(t, err)

	// test the case with add index ddl, and the later ddl/dmls
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)

	mock.ExpectQuery(fmt.Sprintf(checkRunningAddIndexSQL, "test", "t")).WillReturnError(sqlmock.ErrCancelled)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 1, 0, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").WillReturnRows(sqlmock.NewRows([]string{"@ticdc_ts"}).AddRow("2021-05-26 11:33:37.776000"))
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	log.Info("before add index")
	mock.ExpectExec("alter table t add index nameIndex(name);").WillDelayFor(10 * time.Second).WillReturnError(mysql.ErrInvalidConn)
	log.Info("after add index")
	mock.ExpectQuery(fmt.Sprintf(checkRunningSQL, "2021-05-26 11:33:37.776000", "alter table t add index nameIndex(name);")).
		WillReturnRows(sqlmock.NewRows([]string{"JOB_ID", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID", "STATE", "QUERY"}).
			AddRow("", "", "", "", "", "running", "alter table t add index nameIndex(name);"))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 1, 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// for dml event, it is a replace since we set it's commitTs less than replicatingTs
	mock.ExpectExec("BEGIN;REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?);COMMIT;").
		WithArgs(3, "test3").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// for ddl job for table t1
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '2', 0, 0, 0), ('default', 'test/test', '2', 2, 0, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").WillReturnRows(sqlmock.NewRows([]string{"@ticdc_ts"}).AddRow("2021-05-26 11:33:37.776000"))
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table t1 (id int primary key, name varchar(32));").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '2', 0, 1, 0), ('default', 'test/test', '2', 2, 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// for add column ddl for table t
	mock.ExpectQuery(fmt.Sprintf(checkRunningAddIndexSQL, "test", "t")).WillReturnError(sqlmock.ErrCancelled)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '3', 1, 0, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").WillReturnRows(sqlmock.NewRows([]string{"@ticdc_ts"}).AddRow("2021-05-26 11:33:37.776000"))
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("alter table t add column age int;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '3', 1, 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), ddl_ts=VALUES(ddl_ts), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	{
		// add index ddl
		addIndexSQL := "alter table t add index nameIndex(name);"
		addIndexjob := helper.DDL2Job(addIndexSQL)
		require.NotNil(t, addIndexjob)

		addIndexddlEvent := &commonEvent.DDLEvent{
			Type:       byte(timodel.ActionAddIndex),
			Query:      addIndexjob.Query,
			SchemaName: addIndexjob.SchemaName,
			TableName:  addIndexjob.TableName,
			FinishedTs: 1,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{1},
			},
			BlockedTableNames: []commonEvent.SchemaTableName{
				{SchemaName: addIndexjob.SchemaName, TableName: addIndexjob.TableName},
			},
		}

		err = writer.FlushDDLEvent(addIndexddlEvent)
		require.NoError(t, err)
	}

	{
		// ensure the dml can be writen succesfully before add index finished
		dmlEvent := helper.DML2Event("test", "t", "insert into t values (3, 'test3');")
		dmlEvent.CommitTs = 3
		dmlEvent.ReplicatingTs = 4

		err = writer.Flush([]*commonEvent.DMLEvent{dmlEvent})
		require.NoError(t, err)
	}

	{
		// ensure the ddl for other tables can write successfully before add index finished
		createTableSQL2 := "create table t1 (id int primary key, name varchar(32));"
		job2 := helper.DDL2Job(createTableSQL2)
		require.NotNil(t, job2)

		ddlEvent2 := &commonEvent.DDLEvent{
			Query:      job2.Query,
			SchemaName: job2.SchemaName,
			TableName:  job2.TableName,
			FinishedTs: 2,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{0},
			},
			BlockedTableNames: []commonEvent.SchemaTableName{
				{SchemaName: job2.SchemaName, TableName: job2.TableName},
			},
			NeedAddedTables: []commonEvent.Table{{TableID: 2, SchemaID: 1}},
		}

		err = writer.FlushDDLEvent(ddlEvent2)
		require.NoError(t, err)
	}

	{
		// ensure the ddl for the table t can only executed after the add index finished
		job = helper.DDL2Job("alter table t add column age int;")
		require.NotNil(t, job)

		ddlEvent = &commonEvent.DDLEvent{
			Query:      job.Query,
			SchemaName: job.SchemaName,
			TableName:  job.TableName,
			FinishedTs: 3,
			BlockedTables: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{1},
			},
			BlockedTableNames: []commonEvent.SchemaTableName{
				{SchemaName: job.SchemaName, TableName: job.TableName},
			},
		}

		err = writer.FlushDDLEvent(ddlEvent)
		require.NoError(t, err)
	}

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestCheckIsDuplicateEntryError(t *testing.T) {
	writer, db, _ := newTestMysqlWriter(t)
	defer db.Close()

	// Test case 1: nil error should return false
	result := writer.checkIsDuplicateEntryError(nil)
	require.False(t, result)
	require.False(t, writer.isInErrorCausedSafeMode)

	// Test case 2: should return true and set safe mode
	// The issue is that GenWithStackByArgs creates a new error instance that doesn't equal the original
	// So let's test the string matching path instead
	beforeTime := time.Now()
	duplicateErr := fmt.Errorf("Error 1062: Duplicate entry 'test' for key 'PRIMARY'")
	result = writer.checkIsDuplicateEntryError(duplicateErr)

	require.True(t, result)
	require.True(t, writer.isInErrorCausedSafeMode)
	require.True(t, writer.lastErrorCausedSafeModeTime.After(beforeTime) || writer.lastErrorCausedSafeModeTime.Equal(beforeTime))

	// Reset for next test
	writer.isInErrorCausedSafeMode = false

	// Reset for next test
	writer.isInErrorCausedSafeMode = false

	// Test case 3: Wrap Error with "Duplicate entry" in message should return true
	beforeTime = time.Now()
	stringErr2 := fmt.Errorf("Error 1062: Duplicate entry 'test' for key 'PRIMARY'")
	testErr := cerror.WrapError(cerror.ErrMySQLTxnError, errors.WithMessage(stringErr2, fmt.Sprintf("Failed to execute DMLs, query info:%s, args:%v; ", "test sql", "test args")))
	result = writer.checkIsDuplicateEntryError(testErr)

	require.True(t, result)
	require.True(t, writer.isInErrorCausedSafeMode)
	require.True(t, writer.lastErrorCausedSafeModeTime.After(beforeTime) || writer.lastErrorCausedSafeModeTime.Equal(beforeTime))

	// Reset for next test
	writer.isInErrorCausedSafeMode = false

	// Test case 4: Other errors should return false and not set safe mode
	otherErr := fmt.Errorf("some other MySQL error")
	result = writer.checkIsDuplicateEntryError(otherErr)

	require.False(t, result)
	require.False(t, writer.isInErrorCausedSafeMode)
}

func TestUpdateIsInErrorCausedSafeMode(t *testing.T) {
	writer, db, _ := newTestMysqlWriter(t)
	defer db.Close()

	// Test case 1: When not in safe mode, should do nothing
	writer.isInErrorCausedSafeMode = false
	writer.updateIsInErrorCausedSafeMode()
	require.False(t, writer.isInErrorCausedSafeMode)

	// Test case 2: When in safe mode but time hasn't elapsed, should remain in safe mode
	writer.isInErrorCausedSafeMode = true
	writer.lastErrorCausedSafeModeTime = time.Now()
	writer.errorCausedSafeModeDuration = 10 * time.Second

	writer.updateIsInErrorCausedSafeMode()
	require.True(t, writer.isInErrorCausedSafeMode)

	// Test case 3: When in safe mode and time has elapsed, should exit safe mode
	writer.isInErrorCausedSafeMode = true
	writer.lastErrorCausedSafeModeTime = time.Now().Add(-15 * time.Second) // 15 seconds ago
	writer.errorCausedSafeModeDuration = 10 * time.Second                  // 10 second duration

	writer.updateIsInErrorCausedSafeMode()
	require.False(t, writer.isInErrorCausedSafeMode)

	// Test case 4: Edge case - exactly at the duration boundary should exit safe mode
	writer.isInErrorCausedSafeMode = true
	writer.lastErrorCausedSafeModeTime = time.Now().Add(-10*time.Second - time.Millisecond) // Just over 10 seconds ago
	writer.errorCausedSafeModeDuration = 10 * time.Second

	writer.updateIsInErrorCausedSafeMode()
	require.False(t, writer.isInErrorCausedSafeMode)
}
