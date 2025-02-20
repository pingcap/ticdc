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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

var count atomic.Int64

// Test callback and tableProgress works as expected after AddDMLEvent
func TestMysqlSinkBasicFunctionality(t *testing.T) {
	sink, mock := MysqlSinkForTest()

	count.Store(0)

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
			related_table_id bigint(21),
			is_syncpoint bool,
			created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX (ticdc_cluster_id, changefeed, table_id),
			PRIMARY KEY (ticdc_cluster_id, changefeed, table_id)
		);`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO tidb_cdc.ddl_ts_v1 (ticdc_cluster_id, changefeed, ddl_ts, table_id, related_table_id, finished, is_syncpoint) VALUES ('default', 'test/test', '1', 0, 1, 1, 0), ('default', 'test/test', '1', 1, 1, 1, 0) ON DUPLICATE KEY UPDATE finished=VALUES(finished), related_table_id=VALUES(related_table_id), ddl_ts=VALUES(ddl_ts), created_at=NOW(), is_syncpoint=VALUES(is_syncpoint);").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?)").
		WithArgs(1, "test", 2, "test2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := sink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	sink.AddDMLEvent(dmlEvent)
	time.Sleep(1 * time.Second)

	sink.PassBlockEvent(ddlEvent2)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)

	require.Equal(t, count.Load(), int64(3))
}

// test the situation meets error when executing DML
// whether the sink state is correct
func TestMysqlSinkMeetsDMLError(t *testing.T) {
	sink, mock := MysqlSinkForTest()
	count.Store(0)

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
	dmlEvent.CommitTs = 2

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?)").
		WithArgs(1, "test", 2, "test2").
		WillReturnError(errors.New("connect: connection refused"))
	mock.ExpectRollback()

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

	count.Store(0)

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
