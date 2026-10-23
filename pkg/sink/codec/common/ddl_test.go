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
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
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

func TestModifyTableComment(t *testing.T) {
	allocator := NewTableIDAllocator()
	allocator.AddBlockTableID("test", "t", 1)
	ddl := &commonEvent.DDLEvent{
		Type:       byte(timodel.ActionModifyTableComment),
		SchemaName: "test",
		TableName:  "t",
		Query:      "alter table t comment 'test'",
	}

	blockedTables := GetBlockedTables(allocator, ddl)
	require.Equal(t, commonEvent.InfluenceTypeNormal, blockedTables.InfluenceType)
	require.Equal(t, []int64{1}, blockedTables.TableIDs)
}

func TestGetBlockedTablesForRecoverSchema(t *testing.T) {
	allocator := NewTableIDAllocator()
	ddl := &commonEvent.DDLEvent{
		Type:       byte(timodel.ActionRecoverSchema),
		SchemaName: "test",
		Query:      "flashback database test",
	}

	blockedTables := GetBlockedTables(allocator, ddl)
	require.Equal(t, commonEvent.InfluenceTypeNormal, blockedTables.InfluenceType)
	require.Empty(t, blockedTables.TableIDs)
}

func TestGetDDLActionType(t *testing.T) {
	for query, expected := range map[string]timodel.ActionType{
		"create database abc":                        timodel.ActionCreateSchema,
		"create schema aaa":                          timodel.ActionCreateSchema,
		"drop database abc":                          timodel.ActionDropSchema,
		"drop schema aaa":                            timodel.ActionDropSchema,
		"flashback database abc":                     timodel.ActionRecoverSchema,
		"create table t (a int primary key)":         timodel.ActionCreateTable,
		"alter table t add column b int":             timodel.ActionAddColumn,
		"alter table t modify column b bigint":       timodel.ActionModifyColumn,
		"alter table t change column b c int":        timodel.ActionModifyColumn,
		"alter table t drop primary key":             timodel.ActionDropPrimaryKey,
		"alter table t add primary key(a)":           timodel.ActionAddPrimaryKey,
		"alter table t add index idx(c)":             timodel.ActionAddIndex,
		"alter table t rename index idx to idx2":     timodel.ActionRenameIndex,
		"alter table t drop index idx2":              timodel.ActionDropIndex,
		"alter table t add key idx(b)":               timodel.ActionAddIndex,
		"alter table t drop index idx":               timodel.ActionDropIndex,
		"alter table t add unique index idx(b)":      timodel.ActionAddIndex,
		"alter table t add unique key idx(b)":        timodel.ActionAddIndex,
		"alter table t alter column c set default 5": timodel.ActionSetDefaultValue,
		"alter table t drop column c":                timodel.ActionDropColumn,
		"truncate table t":                           timodel.ActionTruncateTable,
		"truncate t":                                 timodel.ActionTruncateTable,
		"rename table t to t2":                       timodel.ActionRenameTable,
		"drop table t2":                              timodel.ActionDropTable,
		"create table t (a int primary key) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))": timodel.ActionCreateTable,
		"alter table t add partition (partition p2 values less than (30))":                                                                  timodel.ActionAddTablePartition,
		"alter table t reorganize partition p0, p1 into (partition p0 values less than (20))":                                               timodel.ActionReorganizePartition,
		"alter table t truncate partition p2":                                                                                               timodel.ActionTruncateTablePartition,
		"create table t1 (a int primary key)":                                                                                               timodel.ActionCreateTable,
		"alter table t exchange partition p0 with table t1":                                                                                 timodel.ActionExchangeTablePartition,
		"alter table t drop partition p0":                                                                                                   timodel.ActionDropTablePartition,
		"alter table t1 partition by hash(a) partitions 4":                                                                                  timodel.ActionAlterTablePartitioning,
		"alter table t1 remove partitioning":                                                                                                timodel.ActionRemovePartitioning,
	} {
		t.Run(query, func(t *testing.T) {
			require.Equal(t, expected, GetDDLActionType(query))
		})
	}
}
