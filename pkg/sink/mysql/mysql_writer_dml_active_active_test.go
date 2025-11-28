package mysql

import (
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestBuildActiveActiveUpsertSQLMultiRows(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32), _tidb_origin_ts bigint unsigned null, _tidb_commit_ts bigint unsigned null, _tidb_softdelete_time timestamp null);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	event := helper.DML2Event("test", "t",
		"insert into t values (1, 'alice', 10, 20, NULL)",
		"insert into t values (2, 'bob', 11, 21, NULL)",
	)
	rows := collectActiveActiveRows(event)
	sql, args := buildActiveActiveUpsertSQL(event.TableInfo, rows)
	require.Equal(t,
		"INSERT INTO `test`.`t` (`id`,`name`,`_tidb_origin_ts`,`_tidb_softdelete_time`) VALUES (?,?,?,?),(?,?,?,?) ON DUPLICATE KEY UPDATE `id` = IF((@ticdc_lww_cond := (IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`))), VALUES(`id`), `id`),`name` = IF(@ticdc_lww_cond, VALUES(`name`), `name`),`_tidb_origin_ts` = IF(@ticdc_lww_cond, VALUES(`_tidb_origin_ts`), `_tidb_origin_ts`),`_tidb_softdelete_time` = IF(@ticdc_lww_cond, VALUES(`_tidb_softdelete_time`), `_tidb_softdelete_time`)",
		sql)
	expectedArgs := []interface{}{
		int64(1), "alice", uint64(20), nil,
		int64(2), "bob", uint64(21), nil,
	}
	require.Equal(t, expectedArgs, args)
}

func TestActiveActiveNormalSQLs(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	writer.cfg.EnableActiveActive = true

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32), _tidb_origin_ts bigint unsigned null, _tidb_commit_ts bigint unsigned null, _tidb_softdelete_time timestamp null);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	event := helper.DML2Event("test", "t",
		"insert into t values (1, 'a', 10, 20, NULL)",
		"insert into t values (2, 'b', 11, 21, NULL)",
		"insert into t values (3, 'c', 12, 22, NULL)",
	)

	sqls, args := writer.generateActiveActiveNormalSQLs([]*commonEvent.DMLEvent{event})
	require.Len(t, sqls, 3)
	require.Len(t, args, 3)
	expectedSQL := "INSERT INTO `test`.`t` (`id`,`name`,`_tidb_origin_ts`,`_tidb_softdelete_time`) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE `id` = IF((@ticdc_lww_cond := (IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`))), VALUES(`id`), `id`),`name` = IF(@ticdc_lww_cond, VALUES(`name`), `name`),`_tidb_origin_ts` = IF(@ticdc_lww_cond, VALUES(`_tidb_origin_ts`), `_tidb_origin_ts`),`_tidb_softdelete_time` = IF(@ticdc_lww_cond, VALUES(`_tidb_softdelete_time`), `_tidb_softdelete_time`)"
	require.Equal(t, expectedSQL, sqls[0])
	require.Equal(t, expectedSQL, sqls[1])
	require.Equal(t, expectedSQL, sqls[2])
	require.Equal(t, []interface{}{int64(1), "a", uint64(20), nil}, args[0])
	require.Equal(t, []interface{}{int64(2), "b", uint64(21), nil}, args[1])
	require.Equal(t, []interface{}{int64(3), "c", uint64(22), nil}, args[2])
}

func TestActiveActivePerEventBatch(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	writer.cfg.EnableActiveActive = true

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32), _tidb_origin_ts bigint unsigned null, _tidb_commit_ts bigint unsigned null, _tidb_softdelete_time timestamp null);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	event := helper.DML2Event("test", "t",
		"insert into t values (1, 'a', 10, 20, NULL)",
		"insert into t values (2, 'b', 11, 21, NULL)",
	)

	sqls, args := writer.generateActiveActiveBatchSQLForPerEvent([]*commonEvent.DMLEvent{event})
	require.Len(t, sqls, 1)
	require.Len(t, args, 1)
	expectedSQL := "INSERT INTO `test`.`t` (`id`,`name`,`_tidb_origin_ts`,`_tidb_softdelete_time`) VALUES (?,?,?,?),(?,?,?,?) ON DUPLICATE KEY UPDATE `id` = IF((@ticdc_lww_cond := (IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`))), VALUES(`id`), `id`),`name` = IF(@ticdc_lww_cond, VALUES(`name`), `name`),`_tidb_origin_ts` = IF(@ticdc_lww_cond, VALUES(`_tidb_origin_ts`), `_tidb_origin_ts`),`_tidb_softdelete_time` = IF(@ticdc_lww_cond, VALUES(`_tidb_softdelete_time`), `_tidb_softdelete_time`)"
	require.Equal(t, expectedSQL, sqls[0])
	require.Equal(t, []interface{}{
		int64(1), "a", uint64(20), nil,
		int64(2), "b", uint64(21), nil,
	}, args[0])
}

func TestActiveActiveCrossEventBatch(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	writer.cfg.EnableActiveActive = true

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32), _tidb_origin_ts bigint unsigned null, _tidb_commit_ts bigint unsigned null, _tidb_softdelete_time timestamp null);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	eventA := helper.DML2Event("test", "t",
		"insert into t values (1, 'a', 10, 20, NULL)",
	)
	eventB := helper.DML2Event("test", "t",
		"insert into t values (2, 'b', 11, 21, NULL)",
	)

	sqls, args := writer.generateActiveActiveBatchSQL([]*commonEvent.DMLEvent{eventA, eventB})
	require.Len(t, sqls, 1)
	require.Len(t, args, 1)
	expectedSQL := "INSERT INTO `test`.`t` (`id`,`name`,`_tidb_origin_ts`,`_tidb_softdelete_time`) VALUES (?,?,?,?),(?,?,?,?) ON DUPLICATE KEY UPDATE `id` = IF((@ticdc_lww_cond := (IFNULL(`_tidb_origin_ts`, `_tidb_commit_ts`) <= VALUES(`_tidb_origin_ts`))), VALUES(`id`), `id`),`name` = IF(@ticdc_lww_cond, VALUES(`name`), `name`),`_tidb_origin_ts` = IF(@ticdc_lww_cond, VALUES(`_tidb_origin_ts`), `_tidb_origin_ts`),`_tidb_softdelete_time` = IF(@ticdc_lww_cond, VALUES(`_tidb_softdelete_time`), `_tidb_softdelete_time`)"
	require.Equal(t, expectedSQL, sqls[0])
	require.Equal(t, []interface{}{
		int64(1), "a", uint64(20), nil,
		int64(2), "b", uint64(21), nil,
	}, args[0])
}
