// Copyright 2026 PingCAP, Inc.
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

package routing

import (
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	cdcfilter "github.com/pingcap/ticdc/pkg/filter"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

type supportedDDLRewriteCase struct {
	name               string
	ddl                *event.DDLEvent
	requiredFragments  []string
	forbiddenFragments []string
}

func TestRewriteDDLQueryWithRoutingSupportsParserBackedDDLTypes(t *testing.T) {
	t.Parallel()

	router := mustNewRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "{table}_r",
	}})

	cases := map[timodel.ActionType]supportedDDLRewriteCase{
		timodel.ActionCreateSchema: schemaRewriteCase(
			"create schema",
			timodel.ActionCreateSchema,
			"CREATE DATABASE `source_db`",
		),
		timodel.ActionDropSchema: schemaRewriteCase(
			"drop schema",
			timodel.ActionDropSchema,
			"DROP DATABASE `source_db`",
		),
		timodel.ActionModifySchemaCharsetAndCollate: schemaRewriteCase(
			"modify schema charset",
			timodel.ActionModifySchemaCharsetAndCollate,
			"ALTER DATABASE `source_db` CHARACTER SET utf8mb4",
		),
		timodel.ActionCreateTable: singleTableRewriteCase(
			"create table",
			timodel.ActionCreateTable,
			"CREATE TABLE `source_db`.`t1` (`id` INT PRIMARY KEY)",
			"t1",
		),
		timodel.ActionCreateTables: {
			name: "create tables",
			ddl: &event.DDLEvent{
				Type:       byte(timodel.ActionCreateTables),
				Query:      "CREATE TABLE `t1` (`id` INT PRIMARY KEY);CREATE TABLE `t2` (`id` INT PRIMARY KEY);",
				SchemaName: "source_db",
				MultipleTableInfos: []*common.TableInfo{
					newRoutingTestTableInfo("source_db", "t1"),
					newRoutingTestTableInfo("source_db", "t2"),
				},
			},
			requiredFragments: []string{
				"CREATE TABLE `target_db`.`t1_r`",
				"CREATE TABLE `target_db`.`t2_r`",
			},
			forbiddenFragments: []string{
				"CREATE TABLE `t1`",
				"CREATE TABLE `t2`",
			},
		},
		timodel.ActionDropTable: singleTableRewriteCase(
			"drop table",
			timodel.ActionDropTable,
			"DROP TABLE `source_db`.`t1`",
			"t1",
		),
		timodel.ActionTruncateTable: singleTableRewriteCase(
			"truncate table",
			timodel.ActionTruncateTable,
			"TRUNCATE TABLE `source_db`.`t1`",
			"t1",
		),
		timodel.ActionRenameTable: {
			name: "rename table",
			ddl: &event.DDLEvent{
				Type:            byte(timodel.ActionRenameTable),
				Query:           "RENAME TABLE `source_db`.`t1` TO `source_db`.`t1_new`",
				SchemaName:      "source_db",
				TableName:       "t1_new",
				ExtraSchemaName: "source_db",
				ExtraTableName:  "t1",
			},
			requiredFragments: []string{
				"`target_db`.`t1_r`",
				"`target_db`.`t1_new_r`",
			},
			forbiddenFragments: []string{
				"`source_db`.`t1`",
				"`source_db`.`t1_new`",
			},
		},
		timodel.ActionRenameTables: {
			name: "rename tables",
			ddl: &event.DDLEvent{
				Type:  byte(timodel.ActionRenameTables),
				Query: "RENAME TABLE `source_db`.`t1` TO `source_db`.`t1_new`, `source_db`.`t2` TO `source_db`.`t2_new`",
				MultipleTableInfos: []*common.TableInfo{
					newRoutingTestTableInfo("source_db", "t1_new"),
					newRoutingTestTableInfo("source_db", "t2_new"),
				},
			},
			requiredFragments: []string{
				"`target_db`.`t1_r`",
				"`target_db`.`t1_new_r`",
				"`target_db`.`t2_r`",
				"`target_db`.`t2_new_r`",
			},
			forbiddenFragments: []string{
				"`source_db`.`t1`",
				"`source_db`.`t1_new`",
				"`source_db`.`t2`",
				"`source_db`.`t2_new`",
			},
		},
		timodel.ActionRecoverTable: singleTableRewriteCase(
			"recover table",
			timodel.ActionRecoverTable,
			"FLASHBACK TABLE `source_db`.`t1`",
			"t1",
		),
		timodel.ActionModifyTableComment: singleTableRewriteCase(
			"modify table comment",
			timodel.ActionModifyTableComment,
			"ALTER TABLE `source_db`.`t1` COMMENT = 'hello'",
			"t1",
		),
		timodel.ActionModifyTableCharsetAndCollate: singleTableRewriteCase(
			"modify table charset",
			timodel.ActionModifyTableCharsetAndCollate,
			"ALTER TABLE `source_db`.`t1` CHARACTER SET utf8mb4",
			"t1",
		),
		timodel.ActionCreateView: {
			name: "create view",
			ddl: &event.DDLEvent{
				Type:       byte(timodel.ActionCreateView),
				Query:      "CREATE VIEW `source_db`.`v1` AS SELECT * FROM `source_db`.`t1`",
				SchemaName: "source_db",
				TableName:  "v1",
				TableInfo:  newRoutingTestTableInfo("source_db", "v1"),
			},
			requiredFragments: []string{
				"`target_db`.`v1_r`",
				"`target_db`.`t1_r`",
			},
			forbiddenFragments: []string{
				"`source_db`.`v1`",
				"`source_db`.`t1`",
			},
		},
		timodel.ActionDropView: singleTableRewriteCase(
			"drop view",
			timodel.ActionDropView,
			"DROP VIEW `source_db`.`v1`",
			"v1",
		),
		timodel.ActionAddTablePartition: singleTableRewriteCase(
			"add table partition",
			timodel.ActionAddTablePartition,
			"ALTER TABLE `source_db`.`pt` ADD PARTITION (PARTITION `p2` VALUES LESS THAN (30))",
			"pt",
		),
		timodel.ActionDropTablePartition: singleTableRewriteCase(
			"drop table partition",
			timodel.ActionDropTablePartition,
			"ALTER TABLE `source_db`.`pt` DROP PARTITION `p0`",
			"pt",
		),
		timodel.ActionTruncateTablePartition: singleTableRewriteCase(
			"truncate table partition",
			timodel.ActionTruncateTablePartition,
			"ALTER TABLE `source_db`.`pt` TRUNCATE PARTITION `p0`",
			"pt",
		),
		timodel.ActionExchangeTablePartition: {
			name: "exchange table partition",
			ddl: &event.DDLEvent{
				Type:       byte(timodel.ActionExchangeTablePartition),
				Query:      "ALTER TABLE `source_db`.`pt` EXCHANGE PARTITION `p0` WITH TABLE `source_db`.`t1`",
				SchemaName: "source_db",
				TableName:  "pt",
				TableInfo:  newRoutingTestTableInfo("source_db", "pt"),
			},
			requiredFragments: []string{
				"`target_db`.`pt_r`",
				"`target_db`.`t1_r`",
			},
			forbiddenFragments: []string{
				"`source_db`.`pt`",
				"`source_db`.`t1`",
			},
		},
		timodel.ActionReorganizePartition: singleTableRewriteCase(
			"reorganize partition",
			timodel.ActionReorganizePartition,
			"ALTER TABLE `source_db`.`pt` REORGANIZE PARTITION `p0`, `p1` INTO (PARTITION `p0` VALUES LESS THAN (20))",
			"pt",
		),
		timodel.ActionAlterTablePartitioning: singleTableRewriteCase(
			"alter table partitioning",
			timodel.ActionAlterTablePartitioning,
			"ALTER TABLE `source_db`.`t1` PARTITION BY HASH(`id`) PARTITIONS 4",
			"t1",
		),
		timodel.ActionRemovePartitioning: singleTableRewriteCase(
			"remove partitioning",
			timodel.ActionRemovePartitioning,
			"ALTER TABLE `source_db`.`pt` REMOVE PARTITIONING",
			"pt",
		),
		timodel.ActionAddColumn: singleTableRewriteCase(
			"add column",
			timodel.ActionAddColumn,
			"ALTER TABLE `source_db`.`t1` ADD COLUMN `c1` INT",
			"t1",
		),
		timodel.ActionDropColumn: singleTableRewriteCase(
			"drop column",
			timodel.ActionDropColumn,
			"ALTER TABLE `source_db`.`t1` DROP COLUMN `c1`",
			"t1",
		),
		timodel.ActionModifyColumn: singleTableRewriteCase(
			"modify column",
			timodel.ActionModifyColumn,
			"ALTER TABLE `source_db`.`t1` MODIFY COLUMN `c1` BIGINT",
			"t1",
		),
		timodel.ActionSetDefaultValue: singleTableRewriteCase(
			"set default value",
			timodel.ActionSetDefaultValue,
			"ALTER TABLE `source_db`.`t1` ALTER COLUMN `c1` SET DEFAULT 5",
			"t1",
		),
		timodel.ActionRebaseAutoID: singleTableRewriteCase(
			"rebase auto id",
			timodel.ActionRebaseAutoID,
			"ALTER TABLE `source_db`.`t1` AUTO_INCREMENT = 100",
			"t1",
		),
		timodel.ActionAddPrimaryKey: singleTableRewriteCase(
			"add primary key",
			timodel.ActionAddPrimaryKey,
			"ALTER TABLE `source_db`.`t1` ADD PRIMARY KEY(`id`)",
			"t1",
		),
		timodel.ActionDropPrimaryKey: singleTableRewriteCase(
			"drop primary key",
			timodel.ActionDropPrimaryKey,
			"ALTER TABLE `source_db`.`t1` DROP PRIMARY KEY",
			"t1",
		),
		timodel.ActionAddIndex: singleTableRewriteCase(
			"add index",
			timodel.ActionAddIndex,
			"CREATE INDEX `idx_c1` ON `source_db`.`t1`(`c1`)",
			"t1",
		),
		timodel.ActionDropIndex: singleTableRewriteCase(
			"drop index",
			timodel.ActionDropIndex,
			"DROP INDEX `idx_c1` ON `source_db`.`t1`",
			"t1",
		),
		timodel.ActionRenameIndex: singleTableRewriteCase(
			"rename index",
			timodel.ActionRenameIndex,
			"ALTER TABLE `source_db`.`t1` RENAME INDEX `idx_c1` TO `idx_c2`",
			"t1",
		),
		timodel.ActionAlterIndexVisibility: singleTableRewriteCase(
			"alter index visibility",
			timodel.ActionAlterIndexVisibility,
			"ALTER TABLE `source_db`.`t1` ALTER INDEX `idx_c1` INVISIBLE",
			"t1",
		),
		timodel.ActionAlterTTLInfo: singleTableRewriteCase(
			"alter ttl info",
			timodel.ActionAlterTTLInfo,
			"ALTER TABLE `source_db`.`t1` TTL_ENABLE = 'OFF'",
			"t1",
		),
		timodel.ActionAlterTTLRemove: singleTableRewriteCase(
			"alter ttl remove",
			timodel.ActionAlterTTLRemove,
			"ALTER TABLE `source_db`.`t1` REMOVE TTL",
			"t1",
		),
		timodel.ActionMultiSchemaChange: singleTableRewriteCase(
			"multi schema change",
			timodel.ActionMultiSchemaChange,
			"ALTER TABLE `source_db`.`t1` ADD COLUMN `c1` INT, DROP COLUMN `c2`",
			"t1",
		),
		timodel.ActionAddForeignKey: {
			name: "add foreign key",
			ddl: &event.DDLEvent{
				Type:       byte(timodel.ActionAddForeignKey),
				Query:      "ALTER TABLE `source_db`.`t1` ADD CONSTRAINT `fk_t2` FOREIGN KEY (`t2_id`) REFERENCES `source_db`.`t2`(`id`)",
				SchemaName: "source_db",
				TableName:  "t1",
				TableInfo:  newRoutingTestTableInfo("source_db", "t1"),
			},
			requiredFragments: []string{
				"`target_db`.`t1_r`",
				"`target_db`.`t2_r`",
			},
			forbiddenFragments: []string{
				"`source_db`.`t1`",
				"`source_db`.`t2`",
			},
		},
		timodel.ActionDropForeignKey: singleTableRewriteCase(
			"drop foreign key",
			timodel.ActionDropForeignKey,
			"ALTER TABLE `source_db`.`t1` DROP FOREIGN KEY `fk_t2`",
			"t1",
		),
		timodel.ActionAddColumns: singleTableRewriteCase(
			"legacy add columns",
			timodel.ActionAddColumns,
			"ALTER TABLE `source_db`.`t1` ADD COLUMN (`c1` INT, `c2` INT)",
			"t1",
		),
		timodel.ActionDropColumns: singleTableRewriteCase(
			"legacy drop columns",
			timodel.ActionDropColumns,
			"ALTER TABLE `source_db`.`t1` DROP COLUMN `c1`, DROP COLUMN `c2`",
			"t1",
		),
	}

	require.Len(t, cases, 39)

	for action, tc := range cases {
		action := action
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, byte(action), tc.ddl.Type)

			newQuery := router.mustRewriteParserBackedDDLQuery(tc.ddl)
			for _, fragment := range tc.requiredFragments {
				require.Contains(t, newQuery, fragment)
			}
			for _, fragment := range tc.forbiddenFragments {
				require.NotContains(t, newQuery, fragment)
			}
		})
	}
}

func TestApplyToDDLEventSupportsParserUnsupportedIndexDDL(t *testing.T) {
	t.Parallel()

	router := mustNewRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "{table}_r",
	}})

	cases := []struct {
		name          string
		ddl           *event.DDLEvent
		expectedQuery string
	}{
		{
			name: "add fulltext index without index name",
			ddl: &event.DDLEvent{
				Type:       byte(cdcfilter.ActionAddFullTextIndex),
				Query:      "ALTER TABLE t1 ADD FULLTEXT INDEX (b) WITH PARSER standard;",
				SchemaName: "source_db",
				TableName:  "t1",
				TableInfo:  newRoutingTestTableInfo("source_db", "t1"),
			},
			expectedQuery: "ALTER TABLE `target_db`.`t1_r` ADD FULLTEXT INDEX (b) WITH PARSER standard;",
		},
		{
			name: "add fulltext index with qualified table",
			ddl: &event.DDLEvent{
				Type:       byte(cdcfilter.ActionAddFullTextIndex),
				Query:      "ALTER TABLE `source_db`.`t1` ADD FULLTEXT INDEX `ft_idx`(`c1`)",
				SchemaName: "source_db",
				TableName:  "t1",
				TableInfo:  newRoutingTestTableInfo("source_db", "t1"),
			},
			expectedQuery: "ALTER TABLE `target_db`.`t1_r` ADD FULLTEXT INDEX `ft_idx`(`c1`)",
		},
		{
			name: "create hybrid index",
			ddl: &event.DDLEvent{
				Type:       byte(cdcfilter.ActionCreateHybridIndex),
				Query:      "CREATE HYBRID INDEX i_idx ON t1(b, c, d, e, g) PARAMETER 'hybrid_index_param';",
				SchemaName: "source_db",
				TableName:  "t1",
				TableInfo:  newRoutingTestTableInfo("source_db", "t1"),
			},
			expectedQuery: "CREATE HYBRID INDEX i_idx ON `target_db`.`t1_r`(b, c, d, e, g) PARAMETER 'hybrid_index_param';",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			routed, err := router.ApplyToDDLEvent(tc.ddl)
			require.NoError(t, err)
			require.NotSame(t, tc.ddl, routed)
			require.Equal(t, tc.expectedQuery, routed.Query)
		})
	}
}

func TestApplyToDDLEventKeepsParserUnsupportedIndexDDLWhenNoRoutingApplies(t *testing.T) {
	t.Parallel()

	router := mustNewRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"other_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "{table}_r",
	}})

	cases := []struct {
		name string
		ddl  *event.DDLEvent
	}{
		{
			name: "add fulltext index",
			ddl: &event.DDLEvent{
				Type:       byte(cdcfilter.ActionAddFullTextIndex),
				Query:      "ALTER TABLE `source_db`.`t1` ADD FULLTEXT INDEX `ft_idx`(`c1`)",
				SchemaName: "source_db",
				TableName:  "t1",
				TableInfo:  newRoutingTestTableInfo("source_db", "t1"),
			},
		},
		{
			name: "create hybrid index",
			ddl: &event.DDLEvent{
				Type:       byte(cdcfilter.ActionCreateHybridIndex),
				Query:      "CREATE HYBRID INDEX i_idx ON t1(b, c, d, e, g) PARAMETER 'hybrid_index_param';",
				SchemaName: "source_db",
				TableName:  "t1",
				TableInfo:  newRoutingTestTableInfo("source_db", "t1"),
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			routed, err := router.ApplyToDDLEvent(tc.ddl)
			require.NoError(t, err)
			require.Same(t, tc.ddl, routed)
		})
	}
}

func TestApplyToDDLEventSupportsCreateTables(t *testing.T) {
	t.Parallel()

	router := mustNewRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "{table}_r",
	}})

	ddl := &event.DDLEvent{
		Type:       byte(timodel.ActionCreateTables),
		Query:      "CREATE TABLE `t1` (`id` INT PRIMARY KEY);CREATE TABLE `t2` (`id` INT PRIMARY KEY);",
		SchemaName: "source_db",
		MultipleTableInfos: []*common.TableInfo{
			newRoutingTestTableInfo("source_db", "t1"),
			newRoutingTestTableInfo("source_db", "t2"),
		},
	}

	routed, err := router.ApplyToDDLEvent(ddl)
	require.NoError(t, err)
	require.Contains(t, routed.Query, "CREATE TABLE `target_db`.`t1_r`")
	require.Contains(t, routed.Query, "CREATE TABLE `target_db`.`t2_r`")
	require.Len(t, routed.MultipleTableInfos, 2)
	require.Equal(t, "target_db", routed.MultipleTableInfos[0].GetTargetSchemaName())
	require.Equal(t, "t1_r", routed.MultipleTableInfos[0].GetTargetTableName())
	require.Equal(t, "target_db", routed.MultipleTableInfos[1].GetTargetSchemaName())
	require.Equal(t, "t2_r", routed.MultipleTableInfos[1].GetTargetTableName())
}

func schemaRewriteCase(name string, action timodel.ActionType, query string) supportedDDLRewriteCase {
	return supportedDDLRewriteCase{
		name: name,
		ddl: &event.DDLEvent{
			Type:       byte(action),
			Query:      query,
			SchemaName: "source_db",
		},
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_db`"},
	}
}

func singleTableRewriteCase(
	name string, action timodel.ActionType, query string, table string,
) supportedDDLRewriteCase {
	return supportedDDLRewriteCase{
		name: name,
		ddl: &event.DDLEvent{
			Type:       byte(action),
			Query:      query,
			SchemaName: "source_db",
			TableName:  table,
			TableInfo:  newRoutingTestTableInfo("source_db", table),
		},
		requiredFragments: []string{
			fmt.Sprintf("`target_db`.`%s_r`", table),
		},
		forbiddenFragments: []string{
			fmt.Sprintf("`source_db`.`%s`", table),
		},
	}
}

func newRoutingTestTableInfo(schema, table string) *common.TableInfo {
	return &common.TableInfo{
		TableName: common.TableName{
			Schema: schema,
			Table:  table,
		},
	}
}
