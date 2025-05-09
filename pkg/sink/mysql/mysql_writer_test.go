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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func newTestMysqlWriter(t *testing.T) (*Writer, *sql.DB, sqlmock.Sqlmock) {
	db, mock := newTestMockDB(t)

	ctx := context.Background()
	cfg := &Config{
		MaxAllowedPacket:   int64(variable.DefMaxAllowedPacket),
		SyncPointRetention: 100 * time.Second,
		MaxTxnRow:          256,
		EnableDDLTs:        defaultEnableDDLTs,
	}
	changefeedID := common.NewChangefeedID4Test("test", "test")
	statistics := metrics.NewStatistics(changefeedID, "mysqlSink")
	writer := NewWriter(ctx, db, cfg, changefeedID, statistics, false)

	return writer, db, mock
}

func newTestMysqlWriterForTiDB(t *testing.T) (*Writer, *sql.DB, sqlmock.Sqlmock) {
	db, mock := newTestMockDB(t)

	ctx := context.Background()
	cfg := &Config{
		MaxAllowedPacket:   int64(variable.DefMaxAllowedPacket),
		SyncPointRetention: 100 * time.Second,
		IsTiDB:             true,
		EnableDDLTs:        defaultEnableDDLTs,
	}
	changefeedID := common.NewChangefeedID4Test("test", "test")
	statistics := metrics.NewStatistics(changefeedID, "mysqlSink")
	writer := NewWriter(ctx, db, cfg, changefeedID, statistics, false)

	return writer, db, mock
}

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)
	return
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

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?),(?,?)").
		WithArgs(1, "test", 2, "test2", 3, "test3").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := writer.Flush([]*commonEvent.DMLEvent{dmlEvent, dmlEvent2})
	require.NoError(t, err)

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

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?)").
		WithArgs(1, "test", 2, "test2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

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

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table t (id int primary key, name varchar(32));").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

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
			table_name_in_ddl_job varchar(1024),
			db_name_in_ddl_job varchar(1024),
			is_syncpoint bool,
			created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX (ticdc_cluster_id, changefeed, table_id),
			PRIMARY KEY (ticdc_cluster_id, changefeed, table_id)
		);`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 0, '', '', 1, 0), ('default', 'test/test', '1', 1, '', '', 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
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

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("alter table t add column age int;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '2', 1, '', '', 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
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
		CommitTsList: []uint64{1},
	}
	tableSchemaStore := util.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{}, common.MysqlSinkType)
	writer.SetTableSchemaStore(tableSchemaStore)

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

	mock.ExpectBegin()
	mock.ExpectQuery("select @@tidb_current_ts").WillReturnRows(sqlmock.NewRows([]string{"@@tidb_current_ts"}).AddRow(0))
	mock.ExpectExec("insert ignore into tidb_cdc.syncpoint_v1 (ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES ('default', 'test/test', 1, 0)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("set global tidb_external_ts = 0").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

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
			table_name_in_ddl_job varchar(1024),
			db_name_in_ddl_job varchar(1024),
			is_syncpoint bool,
			created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX (ticdc_cluster_id, changefeed, table_id),
			PRIMARY KEY (ticdc_cluster_id, changefeed, table_id)
		);`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 0, '', '', 1, 1) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := writer.FlushSyncPointEvent(syncPointEvent)
	require.NoError(t, err)

	syncPointEvent = &commonEvent.SyncPointEvent{
		CommitTsList: []uint64{2},
	}

	mock.ExpectBegin()
	mock.ExpectQuery("select @@tidb_current_ts").WillReturnRows(sqlmock.NewRows([]string{"@@tidb_current_ts"}).AddRow(2))
	mock.ExpectExec("insert ignore into tidb_cdc.syncpoint_v1 (ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES ('default', 'test/test', 2, 2)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("set global tidb_external_ts = 2").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '2', 0, '', '', 1, 1) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = writer.FlushSyncPointEvent(syncPointEvent)
	require.NoError(t, err)

	// flush multiple sync point events

	syncPointEvent = &commonEvent.SyncPointEvent{
		CommitTsList: []uint64{3, 4, 5},
	}

	mock.ExpectBegin()
	mock.ExpectQuery("select @@tidb_current_ts").WillReturnRows(sqlmock.NewRows([]string{"@@tidb_current_ts"}).AddRow(3))
	mock.ExpectExec("insert ignore into tidb_cdc.syncpoint_v1 (ticdc_cluster_id, changefeed, primary_ts, secondary_ts) VALUES ('default', 'test/test', 3, 3), ('default', 'test/test', 4, 3), ('default', 'test/test', 5, 3)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("set global tidb_external_ts = 3").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '3', 0, '', '', 1, 1) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
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
			table_name_in_ddl_job varchar(1024),
			db_name_in_ddl_job varchar(1024),
			is_syncpoint bool,
			created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX (ticdc_cluster_id, changefeed, table_id),
			PRIMARY KEY (ticdc_cluster_id, changefeed, table_id)
		);`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 0, '', '', 0, 0), ('default', 'test/test', '1', 1, '', '', 0, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").WillReturnRows(sqlmock.NewRows([]string{"@ticdc_ts"}).AddRow("2021-05-26 11:33:37.776000"))
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table t (id int primary key, name varchar(32));").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 0, '', '', 1, 0), ('default', 'test/test', '1', 1, '', '', 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := writer.FlushDDLEvent(ddlEvent)
	require.NoError(t, err)

	// test the case with add index ddl, and the later ddl/dmls
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)

	mock.ExpectQuery(fmt.Sprintf(checkRunningAddIndexSQL, 1)).WillReturnError(sqlmock.ErrCancelled)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 1, '', '', 0, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
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
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 1, '', '', 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// for dml event
	mock.ExpectBegin()
	mock.ExpectExec("REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?)").
		WithArgs(3, "test3").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// for ddl job for table t1
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '2', 0, '', '', 0, 0), ('default', 'test/test', '2', 2, '', '', 0, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").WillReturnRows(sqlmock.NewRows([]string{"@ticdc_ts"}).AddRow("2021-05-26 11:33:37.776000"))
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table t1 (id int primary key, name varchar(32));").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '2', 0, '', '', 1, 0), ('default', 'test/test', '2', 2, '', '', 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// for add column ddl for table t
	mock.ExpectQuery(fmt.Sprintf(checkRunningAddIndexSQL, 1)).WillReturnError(sqlmock.ErrCancelled)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '3', 1, '', '', 0, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("BEGIN; SET @ticdc_ts := TIDB_PARSE_TSO(@@tidb_current_ts); ROLLBACK; SELECT @ticdc_ts; SET @ticdc_ts=NULL;").WillReturnRows(sqlmock.NewRows([]string{"@ticdc_ts"}).AddRow("2021-05-26 11:33:37.776000"))
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("alter table t add column age int;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, table_name_in_ddl_job, db_name_in_ddl_job, finished, is_syncpoint) VALUES ('default', 'test/test', '3', 1, '', '', 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), table_name_in_ddl_job=VALUES(table_name_in_ddl_job), db_name_in_ddl_job=VALUES(db_name_in_ddl_job), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
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
		}

		err = writer.FlushDDLEvent(ddlEvent)
		require.NoError(t, err)
	}

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}
