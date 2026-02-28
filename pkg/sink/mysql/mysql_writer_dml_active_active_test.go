// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package mysql

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestBuildActiveActiveUpsertSQLMultiRows(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32), _tidb_origin_ts bigint unsigned null, _tidb_softdelete_time timestamp null);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	event := helper.DML2Event("test", "t",
		"insert into t values (1, 'alice', 10, NULL)",
		"insert into t values (2, 'bob', 11, NULL)",
	)
	rows, commitTs := writer.collectActiveActiveRows(event)
	sql, args, rowTypes := buildActiveActiveUpsertSQL(event.TableInfo, rows, commitTs)
	require.Equal(t,
		"INSERT INTO `test`.`t` (`id`,`name`,`_tidb_origin_ts`,`_tidb_softdelete_time`) VALUES (?,?,?,?),(?,?,?,?) ON DUPLICATE KEY UPDATE `id` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`id`), `id`),`name` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`name`), `name`),`_tidb_origin_ts` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`_tidb_origin_ts`), `_tidb_origin_ts`),`_tidb_softdelete_time` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`_tidb_softdelete_time`), `_tidb_softdelete_time`)",
		sql)
	expectedArgs := []interface{}{
		int64(1), "alice", event.CommitTs, nil,
		int64(2), "bob", event.CommitTs, nil,
	}
	require.Equal(t, expectedArgs, args)
	require.Equal(t, []common.RowType{common.RowTypeInsert}, rowTypes)
}

func TestActiveActiveNormalSQLs(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	writer.cfg.EnableActiveActive = true

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32), _tidb_origin_ts bigint unsigned null, _tidb_softdelete_time timestamp null);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	event := helper.DML2Event("test", "t",
		"insert into t values (1, 'a', 10, NULL)",
		"insert into t values (2, 'b', 11, NULL)",
		"insert into t values (3, 'c', 12, NULL)",
	)

	sqls, args, rowTypes := writer.generateActiveActiveNormalSQLs([]*commonEvent.DMLEvent{event})
	require.Len(t, sqls, 3)
	require.Len(t, args, 3)
	require.Len(t, rowTypes, 3)
	expectedSQL := "INSERT INTO `test`.`t` (`id`,`name`,`_tidb_origin_ts`,`_tidb_softdelete_time`) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE `id` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`id`), `id`),`name` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`name`), `name`),`_tidb_origin_ts` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`_tidb_origin_ts`), `_tidb_origin_ts`),`_tidb_softdelete_time` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`_tidb_softdelete_time`), `_tidb_softdelete_time`)"
	require.Equal(t, expectedSQL, sqls[0])
	require.Equal(t, expectedSQL, sqls[1])
	require.Equal(t, expectedSQL, sqls[2])
	require.Equal(t, []interface{}{int64(1), "a", event.CommitTs, nil}, args[0])
	require.Equal(t, []interface{}{int64(2), "b", event.CommitTs, nil}, args[1])
	require.Equal(t, []interface{}{int64(3), "c", event.CommitTs, nil}, args[2])
}

func TestActiveActivePerEventBatch(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	writer.cfg.EnableActiveActive = true

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32), _tidb_origin_ts bigint unsigned null, _tidb_softdelete_time timestamp null);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	event := helper.DML2Event("test", "t",
		"insert into t values (1, 'a', 10, NULL)",
		"insert into t values (2, 'b', 11, NULL)",
	)

	sqls, args, rowTypes := writer.generateActiveActiveBatchSQLForPerEvent([]*commonEvent.DMLEvent{event})
	require.Len(t, sqls, 1)
	require.Len(t, args, 1)
	require.Len(t, rowTypes, 1)
	expectedSQL := "INSERT INTO `test`.`t` (`id`,`name`,`_tidb_origin_ts`,`_tidb_softdelete_time`) VALUES (?,?,?,?),(?,?,?,?) ON DUPLICATE KEY UPDATE `id` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`id`), `id`),`name` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`name`), `name`),`_tidb_origin_ts` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`_tidb_origin_ts`), `_tidb_origin_ts`),`_tidb_softdelete_time` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`_tidb_softdelete_time`), `_tidb_softdelete_time`)"
	require.Equal(t, expectedSQL, sqls[0])
	require.Equal(t, []interface{}{
		int64(1), "a", event.CommitTs, nil,
		int64(2), "b", event.CommitTs, nil,
	}, args[0])
}

func TestActiveActiveCrossEventBatch(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	writer.cfg.EnableActiveActive = true

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32), _tidb_origin_ts bigint unsigned null, _tidb_softdelete_time timestamp null);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	eventA := helper.DML2Event("test", "t",
		"insert into t values (1, 'a', 10, NULL)",
	)
	eventB := helper.DML2Event("test", "t",
		"insert into t values (2, 'b', 11, NULL)",
	)

	sqls, args, rowTypes := writer.generateActiveActiveBatchSQL([]*commonEvent.DMLEvent{eventA, eventB})
	require.Len(t, sqls, 1)
	require.Len(t, args, 1)
	require.Len(t, rowTypes, 1)
	expectedSQL := "INSERT INTO `test`.`t` (`id`,`name`,`_tidb_origin_ts`,`_tidb_softdelete_time`) VALUES (?,?,?,?),(?,?,?,?) ON DUPLICATE KEY UPDATE `id` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`id`), `id`),`name` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`name`), `name`),`_tidb_origin_ts` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`_tidb_origin_ts`), `_tidb_origin_ts`),`_tidb_softdelete_time` = IF((IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`)), VALUES(`_tidb_softdelete_time`), `_tidb_softdelete_time`)"
	require.Equal(t, expectedSQL, sqls[0])
	require.Equal(t, []interface{}{
		int64(1), "a", eventA.CommitTs, nil,
		int64(2), "b", eventB.CommitTs, nil,
	}, args[0])
}

func TestActiveActiveDropRowsWithNonNullOriginTsForTiDBDownstream(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	writer.cfg.EnableActiveActive = true
	writer.cfg.IsTiDB = true

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32), _tidb_origin_ts bigint unsigned null, _tidb_softdelete_time timestamp null);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	event := helper.DML2Event("test", "t",
		"insert into t values (1, 'a', 10, NULL)",
	)
	sqls, args, rowTypes := writer.generateActiveActiveNormalSQLs([]*commonEvent.DMLEvent{event})
	require.Len(t, sqls, 0)
	require.Len(t, args, 0)

	event = helper.DML2Event("test", "t",
		"insert into t values (2, 'b', 11, NULL)",
		"insert into t values (3, 'c', 12, NULL)",
	)
	sqls, args, rowTypes = writer.generateActiveActiveBatchSQLForPerEvent([]*commonEvent.DMLEvent{event})
	require.Len(t, sqls, 0)
	require.Len(t, args, 0)
	require.Len(t, rowTypes, 0)

	eventA := helper.DML2Event("test", "t",
		"insert into t values (4, 'd', 13, NULL)",
	)
	eventB := helper.DML2Event("test", "t",
		"insert into t values (5, 'e', 14, NULL)",
	)
	sqls, args, rowTypes = writer.generateActiveActiveBatchSQL([]*commonEvent.DMLEvent{eventA, eventB})
	require.Len(t, sqls, 0)
	require.Len(t, args, 0)
	require.Len(t, rowTypes, 0)
}

func TestActiveActiveKeepRowsWithNullOriginTsForTiDBDownstream(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	writer.cfg.EnableActiveActive = true
	writer.cfg.IsTiDB = true

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32), _tidb_origin_ts bigint unsigned null, _tidb_softdelete_time timestamp null);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	event := helper.DML2Event("test", "t",
		"insert into t values (1, 'a', NULL, NULL)",
	)

	sqls, args, rowTypes := writer.generateActiveActiveNormalSQLs([]*commonEvent.DMLEvent{event})
	require.Len(t, sqls, 1)
	require.Len(t, args, 1)
	require.Len(t, rowTypes, 1)
	require.Equal(t, []interface{}{int64(1), "a", event.CommitTs, nil}, args[0])
}
