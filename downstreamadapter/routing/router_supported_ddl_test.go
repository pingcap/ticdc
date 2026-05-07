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
	"testing"

	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

type supportedDDLRewriteCase struct {
	name               string
	action             timodel.ActionType
	ddl                *event.DDLEvent
	requiredFragments  []string
	forbiddenFragments []string
}

func TestRewriteDDLQueryWithRoutingSupportsParserBackedDDLTypes(t *testing.T) {
	originGC := ddlutil.IsEmulatorGCEnable()
	ddlutil.EmulatorGCDisable()
	defer func() {
		if originGC {
			ddlutil.EmulatorGCEnable()
		}
	}()

	router := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_*.*"},
		TargetSchema: "target_db",
		TargetTable:  "{table}_r",
	}})

	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	cases := make([]supportedDDLRewriteCase, 0, 117)

	cases = append(cases, supportedDDLRewriteCase{
		name:               "create schema",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_create_schema`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_create_schema`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_drop_schema",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_drop_schema`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_drop_schema`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "drop schema",
		action:             timodel.ActionDropSchema,
		ddl:                helper.DDL2Event("DROP DATABASE `source_drop_schema`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_drop_schema`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_modify_schema",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_modify_schema`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_modify_schema`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "modify schema charset",
		action:             timodel.ActionModifySchemaCharsetAndCollate,
		ddl:                helper.DDL2Event("ALTER DATABASE `source_modify_schema` COLLATE utf8mb4_general_ci"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_modify_schema`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_create_table",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_create_table`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_create_table`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "create table",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_create_table`.`t_create` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_create_r`"},
		forbiddenFragments: []string{"`source_create_table`.`t_create`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_create_tables",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_create_tables`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_create_tables`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "create tables",
		action:             timodel.ActionCreateTables,
		ddl:                helper.BatchCreateTableDDLs2Event("source_create_tables", "CREATE TABLE `source_create_tables`.`t_create_1` (`id` INT PRIMARY KEY)", "CREATE TABLE `source_create_tables`.`t_create_2` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"CREATE TABLE `target_db`.`t_create_1_r`", "CREATE TABLE `target_db`.`t_create_2_r`"},
		forbiddenFragments: []string{"`source_create_tables`.`t_create_1`", "`source_create_tables`.`t_create_2`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_drop_table",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_drop_table`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_drop_table`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_drop_table.t_drop",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_drop_table`.`t_drop` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_drop_r`"},
		forbiddenFragments: []string{"`source_drop_table`.`t_drop`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "drop table",
		action:             timodel.ActionDropTable,
		ddl:                helper.DDL2Event("DROP TABLE `source_drop_table`.`t_drop`"),
		requiredFragments:  []string{"`target_db`.`t_drop_r`"},
		forbiddenFragments: []string{"`source_drop_table`.`t_drop`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_truncate_table",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_truncate_table`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_truncate_table`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_truncate_table.t_truncate",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_truncate_table`.`t_truncate` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_truncate_r`"},
		forbiddenFragments: []string{"`source_truncate_table`.`t_truncate`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "truncate table",
		action:             timodel.ActionTruncateTable,
		ddl:                helper.DDL2Event("TRUNCATE TABLE `source_truncate_table`.`t_truncate`"),
		requiredFragments:  []string{"`target_db`.`t_truncate_r`"},
		forbiddenFragments: []string{"`source_truncate_table`.`t_truncate`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_rename_table",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_rename_table`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_rename_table`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_rename_table.t_old",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_rename_table`.`t_old` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_old_r`"},
		forbiddenFragments: []string{"`source_rename_table`.`t_old`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "rename table",
		action:             timodel.ActionRenameTable,
		ddl:                helper.DDL2Event("RENAME TABLE `source_rename_table`.`t_old` TO `source_rename_table`.`t_new`"),
		requiredFragments:  []string{"`target_db`.`t_old_r`", "`target_db`.`t_new_r`"},
		forbiddenFragments: []string{"`source_rename_table`.`t_old`", "`source_rename_table`.`t_new`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_rename_tables",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_rename_tables`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_rename_tables`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_rename_tables.t1_old",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_rename_tables`.`t1_old` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t1_old_r`"},
		forbiddenFragments: []string{"`source_rename_tables`.`t1_old`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_rename_tables.t2_old",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_rename_tables`.`t2_old` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t2_old_r`"},
		forbiddenFragments: []string{"`source_rename_tables`.`t2_old`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "rename tables",
		action:             timodel.ActionRenameTables,
		ddl:                helper.DDL2Event("RENAME TABLE `source_rename_tables`.`t1_old` TO `source_rename_tables`.`t1_new`, `source_rename_tables`.`t2_old` TO `source_rename_tables`.`t2_new`"),
		requiredFragments:  []string{"`target_db`.`t1_old_r`", "`target_db`.`t1_new_r`", "`target_db`.`t2_old_r`", "`target_db`.`t2_new_r`"},
		forbiddenFragments: []string{"`source_rename_tables`.`t1_old`", "`source_rename_tables`.`t1_new`", "`source_rename_tables`.`t2_old`", "`source_rename_tables`.`t2_new`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_recover_table",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_recover_table`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_recover_table`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_recover_table.t_recover",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_recover_table`.`t_recover` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_recover_r`"},
		forbiddenFragments: []string{"`source_recover_table`.`t_recover`"},
	})

	helper.Tk().MockGCSavePoint()

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup drop table source_recover_table.t_recover",
		action:             timodel.ActionDropTable,
		ddl:                helper.DDL2Event("DROP TABLE `source_recover_table`.`t_recover`"),
		requiredFragments:  []string{"`target_db`.`t_recover_r`"},
		forbiddenFragments: []string{"`source_recover_table`.`t_recover`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "recover table",
		action:             timodel.ActionRecoverTable,
		ddl:                helper.DDL2Event("FLASHBACK TABLE `source_recover_table`.`t_recover`"),
		requiredFragments:  []string{"`target_db`.`t_recover_r`"},
		forbiddenFragments: []string{"`source_recover_table`.`t_recover`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_modify_table_comment",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_modify_table_comment`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_modify_table_comment`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_modify_table_comment.t_comment",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_modify_table_comment`.`t_comment` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_comment_r`"},
		forbiddenFragments: []string{"`source_modify_table_comment`.`t_comment`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "modify table comment",
		action:             timodel.ActionModifyTableComment,
		ddl:                helper.DDL2Event("ALTER TABLE `source_modify_table_comment`.`t_comment` COMMENT = 'hello'"),
		requiredFragments:  []string{"`target_db`.`t_comment_r`"},
		forbiddenFragments: []string{"`source_modify_table_comment`.`t_comment`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_modify_table_charset",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_modify_table_charset`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_modify_table_charset`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_modify_table_charset.t_charset",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_modify_table_charset`.`t_charset` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_charset_r`"},
		forbiddenFragments: []string{"`source_modify_table_charset`.`t_charset`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "modify table charset",
		action:             timodel.ActionModifyTableCharsetAndCollate,
		ddl:                helper.DDL2Event("ALTER TABLE `source_modify_table_charset`.`t_charset` COLLATE utf8mb4_general_ci"),
		requiredFragments:  []string{"`target_db`.`t_charset_r`"},
		forbiddenFragments: []string{"`source_modify_table_charset`.`t_charset`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_create_view",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_create_view`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_create_view`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_create_view.t_view_src",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_create_view`.`t_view_src` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_view_src_r`"},
		forbiddenFragments: []string{"`source_create_view`.`t_view_src`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "create view",
		action:             timodel.ActionCreateView,
		ddl:                helper.DDL2Event("CREATE VIEW `source_create_view`.`v1` AS SELECT * FROM `source_create_view`.`t_view_src`"),
		requiredFragments:  []string{"`target_db`.`v1_r`", "`target_db`.`t_view_src_r`"},
		forbiddenFragments: []string{"`source_create_view`.`v1`", "`source_create_view`.`t_view_src`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_drop_view",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_drop_view`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_drop_view`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_drop_view.t_view_src",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_drop_view`.`t_view_src` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_view_src_r`"},
		forbiddenFragments: []string{"`source_drop_view`.`t_view_src`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create view source_drop_view.v1",
		action:             timodel.ActionCreateView,
		ddl:                helper.DDL2Event("CREATE VIEW `source_drop_view`.`v1` AS SELECT * FROM `source_drop_view`.`t_view_src`"),
		requiredFragments:  []string{"`target_db`.`v1_r`", "`target_db`.`t_view_src_r`"},
		forbiddenFragments: []string{"`source_drop_view`.`v1`", "`source_drop_view`.`t_view_src`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "drop view",
		action:             timodel.ActionDropView,
		ddl:                helper.DDL2Event("DROP VIEW `source_drop_view`.`v1`"),
		requiredFragments:  []string{"`target_db`.`v1_r`"},
		forbiddenFragments: []string{"`source_drop_view`.`v1`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_add_partition",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_add_partition`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_add_partition`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create partition table source_add_partition.pt_add",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_add_partition`.`pt_add` (`id` INT PRIMARY KEY) PARTITION BY RANGE (`id`) (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20))"),
		requiredFragments:  []string{"`target_db`.`pt_add_r`"},
		forbiddenFragments: []string{"`source_add_partition`.`pt_add`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "add table partition",
		action:             timodel.ActionAddTablePartition,
		ddl:                helper.DDL2Event("ALTER TABLE `source_add_partition`.`pt_add` ADD PARTITION (PARTITION `p2` VALUES LESS THAN (30))"),
		requiredFragments:  []string{"`target_db`.`pt_add_r`"},
		forbiddenFragments: []string{"`source_add_partition`.`pt_add`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_drop_partition",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_drop_partition`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_drop_partition`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create partition table source_drop_partition.pt_drop",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_drop_partition`.`pt_drop` (`id` INT PRIMARY KEY) PARTITION BY RANGE (`id`) (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20))"),
		requiredFragments:  []string{"`target_db`.`pt_drop_r`"},
		forbiddenFragments: []string{"`source_drop_partition`.`pt_drop`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "drop table partition",
		action:             timodel.ActionDropTablePartition,
		ddl:                helper.DDL2Event("ALTER TABLE `source_drop_partition`.`pt_drop` DROP PARTITION `p0`"),
		requiredFragments:  []string{"`target_db`.`pt_drop_r`"},
		forbiddenFragments: []string{"`source_drop_partition`.`pt_drop`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_truncate_partition",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_truncate_partition`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_truncate_partition`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create partition table source_truncate_partition.pt_truncate",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_truncate_partition`.`pt_truncate` (`id` INT PRIMARY KEY) PARTITION BY RANGE (`id`) (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20))"),
		requiredFragments:  []string{"`target_db`.`pt_truncate_r`"},
		forbiddenFragments: []string{"`source_truncate_partition`.`pt_truncate`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "truncate table partition",
		action:             timodel.ActionTruncateTablePartition,
		ddl:                helper.DDL2Event("ALTER TABLE `source_truncate_partition`.`pt_truncate` TRUNCATE PARTITION `p0`"),
		requiredFragments:  []string{"`target_db`.`pt_truncate_r`"},
		forbiddenFragments: []string{"`source_truncate_partition`.`pt_truncate`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_exchange_partition",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_exchange_partition`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_exchange_partition`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create partition table source_exchange_partition.pt_exchange",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_exchange_partition`.`pt_exchange` (`id` INT PRIMARY KEY) PARTITION BY RANGE (`id`) (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20))"),
		requiredFragments:  []string{"`target_db`.`pt_exchange_r`"},
		forbiddenFragments: []string{"`source_exchange_partition`.`pt_exchange`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_exchange_partition.t_exchange",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_exchange_partition`.`t_exchange` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_exchange_r`"},
		forbiddenFragments: []string{"`source_exchange_partition`.`t_exchange`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "exchange table partition",
		action:             timodel.ActionExchangeTablePartition,
		ddl:                helper.DDL2Event("ALTER TABLE `source_exchange_partition`.`pt_exchange` EXCHANGE PARTITION `p0` WITH TABLE `source_exchange_partition`.`t_exchange`"),
		requiredFragments:  []string{"`target_db`.`pt_exchange_r`", "`target_db`.`t_exchange_r`"},
		forbiddenFragments: []string{"`source_exchange_partition`.`pt_exchange`", "`source_exchange_partition`.`t_exchange`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_reorganize_partition",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_reorganize_partition`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_reorganize_partition`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create partition table source_reorganize_partition.pt_reorganize",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_reorganize_partition`.`pt_reorganize` (`id` INT PRIMARY KEY) PARTITION BY RANGE (`id`) (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20))"),
		requiredFragments:  []string{"`target_db`.`pt_reorganize_r`"},
		forbiddenFragments: []string{"`source_reorganize_partition`.`pt_reorganize`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "reorganize partition",
		action:             timodel.ActionReorganizePartition,
		ddl:                helper.DDL2Event("ALTER TABLE `source_reorganize_partition`.`pt_reorganize` REORGANIZE PARTITION `p0`, `p1` INTO (PARTITION `p01` VALUES LESS THAN (20))"),
		requiredFragments:  []string{"`target_db`.`pt_reorganize_r`"},
		forbiddenFragments: []string{"`source_reorganize_partition`.`pt_reorganize`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_alter_partitioning",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_alter_partitioning`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_alter_partitioning`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_alter_partitioning.t_partitioning",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_alter_partitioning`.`t_partitioning` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_partitioning_r`"},
		forbiddenFragments: []string{"`source_alter_partitioning`.`t_partitioning`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "alter table partitioning",
		action:             timodel.ActionAlterTablePartitioning,
		ddl:                helper.DDL2Event("ALTER TABLE `source_alter_partitioning`.`t_partitioning` PARTITION BY HASH(`id`) PARTITIONS 4"),
		requiredFragments:  []string{"`target_db`.`t_partitioning_r`"},
		forbiddenFragments: []string{"`source_alter_partitioning`.`t_partitioning`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_remove_partitioning",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_remove_partitioning`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_remove_partitioning`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create partition table source_remove_partitioning.pt_remove",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_remove_partitioning`.`pt_remove` (`id` INT PRIMARY KEY) PARTITION BY RANGE (`id`) (PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20))"),
		requiredFragments:  []string{"`target_db`.`pt_remove_r`"},
		forbiddenFragments: []string{"`source_remove_partitioning`.`pt_remove`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "remove partitioning",
		action:             timodel.ActionRemovePartitioning,
		ddl:                helper.DDL2Event("ALTER TABLE `source_remove_partitioning`.`pt_remove` REMOVE PARTITIONING"),
		requiredFragments:  []string{"`target_db`.`pt_remove_r`"},
		forbiddenFragments: []string{"`source_remove_partitioning`.`pt_remove`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_add_column",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_add_column`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_add_column`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_add_column.t_add_column",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_add_column`.`t_add_column` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_add_column_r`"},
		forbiddenFragments: []string{"`source_add_column`.`t_add_column`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "add column",
		action:             timodel.ActionAddColumn,
		ddl:                helper.DDL2Event("ALTER TABLE `source_add_column`.`t_add_column` ADD COLUMN `c1` INT"),
		requiredFragments:  []string{"`target_db`.`t_add_column_r`"},
		forbiddenFragments: []string{"`source_add_column`.`t_add_column`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_drop_column",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_drop_column`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_drop_column`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_drop_column.t_drop_column",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_drop_column`.`t_drop_column` (`id` INT PRIMARY KEY, `c1` INT)"),
		requiredFragments:  []string{"`target_db`.`t_drop_column_r`"},
		forbiddenFragments: []string{"`source_drop_column`.`t_drop_column`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "drop column",
		action:             timodel.ActionDropColumn,
		ddl:                helper.DDL2Event("ALTER TABLE `source_drop_column`.`t_drop_column` DROP COLUMN `c1`"),
		requiredFragments:  []string{"`target_db`.`t_drop_column_r`"},
		forbiddenFragments: []string{"`source_drop_column`.`t_drop_column`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_modify_column",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_modify_column`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_modify_column`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_modify_column.t_modify_column",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_modify_column`.`t_modify_column` (`id` INT PRIMARY KEY, `c1` INT)"),
		requiredFragments:  []string{"`target_db`.`t_modify_column_r`"},
		forbiddenFragments: []string{"`source_modify_column`.`t_modify_column`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "modify column",
		action:             timodel.ActionModifyColumn,
		ddl:                helper.DDL2Event("ALTER TABLE `source_modify_column`.`t_modify_column` MODIFY COLUMN `c1` BIGINT"),
		requiredFragments:  []string{"`target_db`.`t_modify_column_r`"},
		forbiddenFragments: []string{"`source_modify_column`.`t_modify_column`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_set_default",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_set_default`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_set_default`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_set_default.t_set_default",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_set_default`.`t_set_default` (`id` INT PRIMARY KEY, `c1` INT)"),
		requiredFragments:  []string{"`target_db`.`t_set_default_r`"},
		forbiddenFragments: []string{"`source_set_default`.`t_set_default`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "set default value",
		action:             timodel.ActionSetDefaultValue,
		ddl:                helper.DDL2Event("ALTER TABLE `source_set_default`.`t_set_default` ALTER COLUMN `c1` SET DEFAULT 5"),
		requiredFragments:  []string{"`target_db`.`t_set_default_r`"},
		forbiddenFragments: []string{"`source_set_default`.`t_set_default`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_rebase_auto_id",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_rebase_auto_id`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_rebase_auto_id`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_rebase_auto_id.t_rebase",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_rebase_auto_id`.`t_rebase` (`id` INT PRIMARY KEY AUTO_INCREMENT)"),
		requiredFragments:  []string{"`target_db`.`t_rebase_r`"},
		forbiddenFragments: []string{"`source_rebase_auto_id`.`t_rebase`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "rebase auto id",
		action:             timodel.ActionRebaseAutoID,
		ddl:                helper.DDL2Event("ALTER TABLE `source_rebase_auto_id`.`t_rebase` AUTO_INCREMENT = 100"),
		requiredFragments:  []string{"`target_db`.`t_rebase_r`"},
		forbiddenFragments: []string{"`source_rebase_auto_id`.`t_rebase`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_add_primary_key",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_add_primary_key`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_add_primary_key`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_add_primary_key.t_add_pk",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_add_primary_key`.`t_add_pk` (`id` INT)"),
		requiredFragments:  []string{"`target_db`.`t_add_pk_r`"},
		forbiddenFragments: []string{"`source_add_primary_key`.`t_add_pk`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "add primary key",
		action:             timodel.ActionAddPrimaryKey,
		ddl:                helper.DDL2Event("ALTER TABLE `source_add_primary_key`.`t_add_pk` ADD PRIMARY KEY(`id`)"),
		requiredFragments:  []string{"`target_db`.`t_add_pk_r`"},
		forbiddenFragments: []string{"`source_add_primary_key`.`t_add_pk`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_drop_primary_key",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_drop_primary_key`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_drop_primary_key`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_drop_primary_key.t_drop_pk",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_drop_primary_key`.`t_drop_pk` (`id` INT, PRIMARY KEY(`id`) NONCLUSTERED)"),
		requiredFragments:  []string{"`target_db`.`t_drop_pk_r`"},
		forbiddenFragments: []string{"`source_drop_primary_key`.`t_drop_pk`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "drop primary key",
		action:             timodel.ActionDropPrimaryKey,
		ddl:                helper.DDL2Event("ALTER TABLE `source_drop_primary_key`.`t_drop_pk` DROP PRIMARY KEY"),
		requiredFragments:  []string{"`target_db`.`t_drop_pk_r`"},
		forbiddenFragments: []string{"`source_drop_primary_key`.`t_drop_pk`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_add_index",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_add_index`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_add_index`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_add_index.t_add_index",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_add_index`.`t_add_index` (`id` INT PRIMARY KEY, `c1` INT)"),
		requiredFragments:  []string{"`target_db`.`t_add_index_r`"},
		forbiddenFragments: []string{"`source_add_index`.`t_add_index`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "add index",
		action:             timodel.ActionAddIndex,
		ddl:                helper.DDL2Event("CREATE INDEX `idx_c1` ON `source_add_index`.`t_add_index`(`c1`)"),
		requiredFragments:  []string{"`target_db`.`t_add_index_r`"},
		forbiddenFragments: []string{"`source_add_index`.`t_add_index`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_drop_index",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_drop_index`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_drop_index`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_drop_index.t_drop_index",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_drop_index`.`t_drop_index` (`id` INT PRIMARY KEY, `c1` INT, KEY `idx_c1` (`c1`))"),
		requiredFragments:  []string{"`target_db`.`t_drop_index_r`"},
		forbiddenFragments: []string{"`source_drop_index`.`t_drop_index`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "drop index",
		action:             timodel.ActionDropIndex,
		ddl:                helper.DDL2Event("DROP INDEX `idx_c1` ON `source_drop_index`.`t_drop_index`"),
		requiredFragments:  []string{"`target_db`.`t_drop_index_r`"},
		forbiddenFragments: []string{"`source_drop_index`.`t_drop_index`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_rename_index",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_rename_index`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_rename_index`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_rename_index.t_rename_index",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_rename_index`.`t_rename_index` (`id` INT PRIMARY KEY, `c1` INT, KEY `idx_c1` (`c1`))"),
		requiredFragments:  []string{"`target_db`.`t_rename_index_r`"},
		forbiddenFragments: []string{"`source_rename_index`.`t_rename_index`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "rename index",
		action:             timodel.ActionRenameIndex,
		ddl:                helper.DDL2Event("ALTER TABLE `source_rename_index`.`t_rename_index` RENAME INDEX `idx_c1` TO `idx_c2`"),
		requiredFragments:  []string{"`target_db`.`t_rename_index_r`"},
		forbiddenFragments: []string{"`source_rename_index`.`t_rename_index`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_alter_index_visibility",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_alter_index_visibility`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_alter_index_visibility`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_alter_index_visibility.t_alter_index",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_alter_index_visibility`.`t_alter_index` (`id` INT PRIMARY KEY, `c1` INT, KEY `idx_c1` (`c1`))"),
		requiredFragments:  []string{"`target_db`.`t_alter_index_r`"},
		forbiddenFragments: []string{"`source_alter_index_visibility`.`t_alter_index`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "alter index visibility",
		action:             timodel.ActionAlterIndexVisibility,
		ddl:                helper.DDL2Event("ALTER TABLE `source_alter_index_visibility`.`t_alter_index` ALTER INDEX `idx_c1` INVISIBLE"),
		requiredFragments:  []string{"`target_db`.`t_alter_index_r`"},
		forbiddenFragments: []string{"`source_alter_index_visibility`.`t_alter_index`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_alter_ttl_info",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_alter_ttl_info`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_alter_ttl_info`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create ttl table source_alter_ttl_info.t_ttl_info",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_alter_ttl_info`.`t_ttl_info` (`id` INT PRIMARY KEY, `created_at` DATETIME) TTL = `created_at` + INTERVAL 1 DAY TTL_ENABLE = 'OFF'"),
		requiredFragments:  []string{"`target_db`.`t_ttl_info_r`"},
		forbiddenFragments: []string{"`source_alter_ttl_info`.`t_ttl_info`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "alter ttl info",
		action:             timodel.ActionAlterTTLInfo,
		ddl:                helper.DDL2Event("ALTER TABLE `source_alter_ttl_info`.`t_ttl_info` TTL_ENABLE = 'ON'"),
		requiredFragments:  []string{"`target_db`.`t_ttl_info_r`"},
		forbiddenFragments: []string{"`source_alter_ttl_info`.`t_ttl_info`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_alter_ttl_remove",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_alter_ttl_remove`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_alter_ttl_remove`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create ttl table source_alter_ttl_remove.t_ttl_remove",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_alter_ttl_remove`.`t_ttl_remove` (`id` INT PRIMARY KEY, `created_at` DATETIME) TTL = `created_at` + INTERVAL 1 DAY TTL_ENABLE = 'OFF'"),
		requiredFragments:  []string{"`target_db`.`t_ttl_remove_r`"},
		forbiddenFragments: []string{"`source_alter_ttl_remove`.`t_ttl_remove`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "alter ttl remove",
		action:             timodel.ActionAlterTTLRemove,
		ddl:                helper.DDL2Event("ALTER TABLE `source_alter_ttl_remove`.`t_ttl_remove` REMOVE TTL"),
		requiredFragments:  []string{"`target_db`.`t_ttl_remove_r`"},
		forbiddenFragments: []string{"`source_alter_ttl_remove`.`t_ttl_remove`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_multi_schema_change",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_multi_schema_change`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_multi_schema_change`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_multi_schema_change.t_multi",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_multi_schema_change`.`t_multi` (`id` INT PRIMARY KEY, `c2` INT)"),
		requiredFragments:  []string{"`target_db`.`t_multi_r`"},
		forbiddenFragments: []string{"`source_multi_schema_change`.`t_multi`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "multi schema change",
		action:             timodel.ActionMultiSchemaChange,
		ddl:                helper.DDL2Event("ALTER TABLE `source_multi_schema_change`.`t_multi` ADD COLUMN `c1` INT, DROP COLUMN `c2`"),
		requiredFragments:  []string{"`target_db`.`t_multi_r`"},
		forbiddenFragments: []string{"`source_multi_schema_change`.`t_multi`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_add_foreign_key",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_add_foreign_key`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_add_foreign_key`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_add_foreign_key.t2",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_add_foreign_key`.`t2` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t2_r`"},
		forbiddenFragments: []string{"`source_add_foreign_key`.`t2`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_add_foreign_key.t1",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_add_foreign_key`.`t1` (`id` INT PRIMARY KEY, `t2_id` INT)"),
		requiredFragments:  []string{"`target_db`.`t1_r`"},
		forbiddenFragments: []string{"`source_add_foreign_key`.`t1`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "add foreign key",
		action:             timodel.ActionMultiSchemaChange,
		ddl:                helper.DDL2Event("ALTER TABLE `source_add_foreign_key`.`t1` ADD CONSTRAINT `fk_t2` FOREIGN KEY (`t2_id`) REFERENCES `source_add_foreign_key`.`t2`(`id`)"),
		requiredFragments:  []string{"`target_db`.`t1_r`", "`target_db`.`t2_r`"},
		forbiddenFragments: []string{"`source_add_foreign_key`.`t1`", "`source_add_foreign_key`.`t2`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_drop_foreign_key",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_drop_foreign_key`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_drop_foreign_key`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_drop_foreign_key.t2",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_drop_foreign_key`.`t2` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t2_r`"},
		forbiddenFragments: []string{"`source_drop_foreign_key`.`t2`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_drop_foreign_key.t1",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_drop_foreign_key`.`t1` (`id` INT PRIMARY KEY, `t2_id` INT, CONSTRAINT `fk_t2` FOREIGN KEY (`t2_id`) REFERENCES `t2`(`id`))"),
		requiredFragments:  []string{"`target_db`.`t1_r`"},
		forbiddenFragments: []string{"`source_drop_foreign_key`.`t1`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "drop foreign key",
		action:             timodel.ActionDropForeignKey,
		ddl:                helper.DDL2Event("ALTER TABLE `source_drop_foreign_key`.`t1` DROP FOREIGN KEY `fk_t2`"),
		requiredFragments:  []string{"`target_db`.`t1_r`"},
		forbiddenFragments: []string{"`source_drop_foreign_key`.`t1`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_add_columns",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_add_columns`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_add_columns`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_add_columns.t_add_columns",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_add_columns`.`t_add_columns` (`id` INT PRIMARY KEY)"),
		requiredFragments:  []string{"`target_db`.`t_add_columns_r`"},
		forbiddenFragments: []string{"`source_add_columns`.`t_add_columns`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "add columns as multi schema change",
		action:             timodel.ActionMultiSchemaChange,
		ddl:                helper.DDL2Event("ALTER TABLE `source_add_columns`.`t_add_columns` ADD COLUMN (`c1` INT, `c2` INT)"),
		requiredFragments:  []string{"`target_db`.`t_add_columns_r`"},
		forbiddenFragments: []string{"`source_add_columns`.`t_add_columns`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create schema source_drop_columns",
		action:             timodel.ActionCreateSchema,
		ddl:                helper.DDL2Event("CREATE DATABASE `source_drop_columns`"),
		requiredFragments:  []string{"`target_db`"},
		forbiddenFragments: []string{"`source_drop_columns`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "setup create table source_drop_columns.t_drop_columns",
		action:             timodel.ActionCreateTable,
		ddl:                helper.DDL2Event("CREATE TABLE `source_drop_columns`.`t_drop_columns` (`id` INT PRIMARY KEY, `c1` INT, `c2` INT)"),
		requiredFragments:  []string{"`target_db`.`t_drop_columns_r`"},
		forbiddenFragments: []string{"`source_drop_columns`.`t_drop_columns`"},
	})

	cases = append(cases, supportedDDLRewriteCase{
		name:               "drop columns as multi schema change",
		action:             timodel.ActionMultiSchemaChange,
		ddl:                helper.DDL2Event("ALTER TABLE `source_drop_columns`.`t_drop_columns` DROP COLUMN `c1`, DROP COLUMN `c2`"),
		requiredFragments:  []string{"`target_db`.`t_drop_columns_r`"},
		forbiddenFragments: []string{"`source_drop_columns`.`t_drop_columns`"},
	})

	require.GreaterOrEqual(t, len(cases), 39)

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, byte(tc.action), tc.ddl.Type)

			routed, err := router.ApplyToDDLEvent(tc.ddl)
			require.NoError(t, err)
			for _, fragment := range tc.requiredFragments {
				require.Contains(t, routed.Query, fragment)
			}
			for _, fragment := range tc.forbiddenFragments {
				require.NotContains(t, routed.Query, fragment)
			}
		})
	}
}

func TestApplyToDDLEventSupportsCreateTables(t *testing.T) {
	router := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "{table}_r",
	}})

	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	schemaDDL := helper.DDL2Event("CREATE DATABASE `source_db`")
	// TiDB ActionCreateTables is same-schema only. Cross-schema CREATE TABLE
	// statements are emitted as separate DDL jobs upstream, not one ActionCreateTables event.
	ddl := helper.BatchCreateTableDDLs2Event("source_db",
		"CREATE TABLE `source_db`.`t1` (`id` INT PRIMARY KEY)",
		"CREATE TABLE `source_db`.`t2` (`id` INT PRIMARY KEY)",
	)

	routedSchema, err := router.ApplyToDDLEvent(schemaDDL)
	require.NoError(t, err)
	require.Contains(t, routedSchema.Query, "`target_db`")

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
