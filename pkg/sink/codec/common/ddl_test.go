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

package common

import (
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	ticonfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func init() {
	if kerneltype.IsNextGen() {
		ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
			conf.Instance.TiDBServiceScope = handle.NextGenTargetScope
		})
	}
}

func TestGetDDLActionType(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	// disable the clustered index to allow drop and add primary key
	helper.Tk().MustExec("set @@tidb_enable_clustered_index=0;")

	createDBSQL := `create database abc`
	ddl := helper.DDL2Event(createDBSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(createDBSQL))

	createSchemaSQL := `create schema aaa`
	ddl = helper.DDL2Event(createSchemaSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(createSchemaSQL))

	dropDBSQL := `drop database abc`
	ddl = helper.DDL2Event(dropDBSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropDBSQL))

	dropSchemaSQL := `drop schema aaa`
	ddl = helper.DDL2Event(dropSchemaSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropSchemaSQL))

	helper.Tk().MustExec("use test")

	createTableSQL := `create table t (a int primary key)`
	ddl = helper.DDL2Event(createTableSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(createTableSQL))

	addColumnSQL := `alter table t add column b int`
	ddl = helper.DDL2Event(addColumnSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(addColumnSQL))

	modifyColumnSQL := `alter table t modify column b bigint`
	ddl = helper.DDL2Event(modifyColumnSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(modifyColumnSQL))

	renameColumnSQL := `alter table t change column b c int`
	ddl = helper.DDL2Event(renameColumnSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(renameColumnSQL))

	dropPrimaryKeySQL := `alter table t drop primary key`
	ddl = helper.DDL2Event(dropPrimaryKeySQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropPrimaryKeySQL))

	addPrimaryKeySQL := `alter table t add primary key(a)`
	ddl = helper.DDL2Event(addPrimaryKeySQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(addPrimaryKeySQL))

	addIndexSQL := `alter table t add index idx(c)`
	ddl = helper.DDL2Event(addIndexSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(addIndexSQL))

	renameIndexSQL := `alter table t rename index idx to idx2`
	ddl = helper.DDL2Event(renameIndexSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(renameIndexSQL))

	dropIndexSQL := `alter table t drop index idx2`
	ddl = helper.DDL2Event(dropIndexSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropIndexSQL))

	addColumnSQL = `alter table t add column b int`
	ddl = helper.DDL2Event(addColumnSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(addColumnSQL))

	addKeySQL := `alter table t add key idx(b)`
	ddl = helper.DDL2Event(addKeySQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(addKeySQL))

	dropKeySQL := `alter table t drop index idx`
	ddl = helper.DDL2Event(dropKeySQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropKeySQL))

	addUniqueIndexSQL := `alter table t add unique index idx(b)`
	ddl = helper.DDL2Event(addUniqueIndexSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(addUniqueIndexSQL))

	dropUniqueIndexSQL := `alter table t drop index idx`
	ddl = helper.DDL2Event(dropUniqueIndexSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropUniqueIndexSQL))

	addUniqueKeySQL := `alter table t add unique key idx(b)`
	ddl = helper.DDL2Event(addUniqueKeySQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(addUniqueKeySQL))

	dropUniqueKeySQL := `alter table t drop index idx`
	ddl = helper.DDL2Event(dropUniqueKeySQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropUniqueKeySQL))

	//addFulltextIndexSQL := `alter table t add fulltext index idx(b)`
	//ddl = helper.DDL2Event(addFulltextIndexSQL)
	//require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(addFulltextIndexSQL))
	//
	//dropFulltextIndexSQL := `alter table t drop index idx`
	//ddl = helper.DDL2Event(dropFulltextIndexSQL)
	//require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropFulltextIndexSQL))

	//addFulltextKeySQL := `alter table t add fulltext key idx(b)`
	//ddl = helper.DDL2Event(addFulltextKeySQL)
	//require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(addFulltextKeySQL))
	//
	//dropFulltextKeySQL := `alter table t drop index idx`
	//ddl = helper.DDL2Event(dropFulltextKeySQL)
	//require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropFulltextKeySQL))

	//createTable2SQL := `create table t2 (a int primary key)`
	//ddl = helper.DDL2Event(createTable2SQL)
	//require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(createTable2SQL))
	//
	//addForeignKeySQL := `alter table t2 add FOREIGN KEY fk (a) references t2(a)`
	//ddl = helper.DDL2Event(addForeignKeySQL)
	//require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(addForeignKeySQL))
	//
	//dropForeignKeySQL := `alter table t2 drop FOREIGN KEY fk`
	//ddl = helper.DDL2Event(dropForeignKeySQL)
	//require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropForeignKeySQL))

	setDefaultValueSQL := `alter table t alter column c set default 5`
	ddl = helper.DDL2Event(setDefaultValueSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(setDefaultValueSQL))

	dropColumnSQL := `alter table t drop column c`
	ddl = helper.DDL2Event(dropColumnSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropColumnSQL))

	truncateTableSQL := `truncate table t`
	ddl = helper.DDL2Event(truncateTableSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(truncateTableSQL))

	truncateTableSQL = `truncate t`
	ddl = helper.DDL2Event(truncateTableSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(truncateTableSQL))

	renameTableSQL := `rename table t to t2`
	ddl = helper.DDL2Event(renameTableSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(renameTableSQL))

	dropTableSQL := `drop table t2`
	ddl = helper.DDL2Event(dropTableSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropTableSQL))

	// partition table related test
	createPartitionTableSQL := `create table t (a int primary key)
    	partition by range(a) (
    	partition p0 values less than (10),
    	partition p1 values less than (20))`
	ddl = helper.DDL2Event(createPartitionTableSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(createPartitionTableSQL))

	addTablePartitionSQL := `alter table t add partition (partition p2 values less than (30))`
	ddl = helper.DDL2Event(addTablePartitionSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(addTablePartitionSQL))

	reorganizePartitionSQL := `alter table t reorganize partition p0, p1 into (partition p0 values less than (20))`
	ddl = helper.DDL2Event(reorganizePartitionSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(reorganizePartitionSQL))

	truncatePartitionTableSQL := `alter table t truncate partition p2`
	ddl = helper.DDL2Event(truncatePartitionTableSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(truncatePartitionTableSQL))

	createTableSQL = `create table t1 (a int primary key)`
	ddl = helper.DDL2Event(createTableSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(createTableSQL))

	exchangePartitionSQL := `alter table t exchange partition p0 with table t1`
	ddl = helper.DDL2Event(exchangePartitionSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(exchangePartitionSQL))

	dropTablePartitionSQL := `alter table t drop partition p0`
	ddl = helper.DDL2Event(dropTablePartitionSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(dropTablePartitionSQL))

	partitionBySQL := `alter table t1 partition by hash(a) partitions 4`
	ddl = helper.DDL2Event(partitionBySQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(partitionBySQL))

	removePartitioningSQL := `alter table t1 remove partitioning`
	ddl = helper.DDL2Event(removePartitioningSQL)
	require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(removePartitioningSQL))

	//// modify table charset
	//modifyTableCharsetSQL := `alter table t1 character set utf8mb4`
	//ddl = helper.DDL2Event(modifyTableCharsetSQL)
	//require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(modifyTableCharsetSQL))
	//
	//// modify schema charset
	//modifySchemaCharsetSQL := `alter database test character set utf8mb4`
	//ddl = helper.DDL2Event(modifySchemaCharsetSQL)
	//require.Equal(t, timodel.ActionType(ddl.Type), GetDDLActionType(modifySchemaCharsetSQL))
}
