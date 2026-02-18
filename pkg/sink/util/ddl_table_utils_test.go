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

package util

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	sql                string
	expectedSQLs       []string
	expectedTableNames [][]*filter.Table
	targetTableNames   [][]*filter.Table
	targetSQLs         []string
}

var testCases = []testCase{
	// Test case with foreign key - foreign key references should NOT be renamed
	{
		"create table `t1` (`id` int, `student_id` int, primary key (`id`), foreign key (`student_id`) references `t2`(`id`))",
		[]string{"CREATE TABLE `t1` (`id` INT,`student_id` INT,PRIMARY KEY(`id`),CONSTRAINT FOREIGN KEY (`student_id`) REFERENCES `t2`(`id`))"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "t1")}},
		[]string{"CREATE TABLE `xtest`.`t1` (`id` INT,`student_id` INT,PRIMARY KEY(`id`),CONSTRAINT FOREIGN KEY (`student_id`) REFERENCES `t2`(`id`))"},
	},
	// CREATE SCHEMA/DATABASE
	{
		"create schema `s1`",
		[]string{"CREATE DATABASE `s1`"},
		[][]*filter.Table{{genTableName("s1", "")}},
		[][]*filter.Table{{genTableName("xs1", "")}},
		[]string{"CREATE DATABASE `xs1`"},
	},
	{
		"create schema if not exists `s1`",
		[]string{"CREATE DATABASE IF NOT EXISTS `s1`"},
		[][]*filter.Table{{genTableName("s1", "")}},
		[][]*filter.Table{{genTableName("xs1", "")}},
		[]string{"CREATE DATABASE IF NOT EXISTS `xs1`"},
	},
	// DROP SCHEMA/DATABASE
	{
		"drop schema `s1`",
		[]string{"DROP DATABASE `s1`"},
		[][]*filter.Table{{genTableName("s1", "")}},
		[][]*filter.Table{{genTableName("xs1", "")}},
		[]string{"DROP DATABASE `xs1`"},
	},
	{
		"drop schema if exists `s1`",
		[]string{"DROP DATABASE IF EXISTS `s1`"},
		[][]*filter.Table{{genTableName("s1", "")}},
		[][]*filter.Table{{genTableName("xs1", "")}},
		[]string{"DROP DATABASE IF EXISTS `xs1`"},
	},
	// ALTER DATABASE without explicit name - cannot rename since AST doesn't store database name
	{
		"alter database collate utf8mb4_general_ci",
		[]string{"ALTER DATABASE COLLATE = utf8mb4_general_ci"},
		[][]*filter.Table{{genTableName("test", "")}},
		[][]*filter.Table{{genTableName("xtest", "")}},
		// Note: When no database name is in original SQL, RenameDDLTable cannot add it
		// because the AST parser tracks whether name was present
		[]string{"ALTER DATABASE COLLATE = utf8mb4_general_ci"},
	},
	// DROP TABLE - single table
	{
		"drop table `Ss1`.`tT1`",
		[]string{"DROP TABLE `Ss1`.`tT1`"},
		[][]*filter.Table{{genTableName("Ss1", "tT1")}},
		[][]*filter.Table{{genTableName("xSs1", "xtT1")}},
		[]string{"DROP TABLE `xSs1`.`xtT1`"},
	},
	// DROP TABLE - multiple tables (requires SplitDDL to split, so we test without splitting)
	{
		"drop table `s1`.`t1`, `s2`.`t2`",
		[]string{"DROP TABLE `s1`.`t1`, `s2`.`t2`"},
		[][]*filter.Table{{genTableName("s1", "t1"), genTableName("s2", "t2")}},
		[][]*filter.Table{{genTableName("xs1", "xt1"), genTableName("xs2", "xt2")}},
		[]string{"DROP TABLE `xs1`.`xt1`, `xs2`.`xt2`"},
	},
	{
		"drop table `s1`.`t1`, `s2`.`t2`, `xx`",
		[]string{"DROP TABLE `s1`.`t1`, `s2`.`t2`, `xx`"},
		[][]*filter.Table{{genTableName("s1", "t1"), genTableName("s2", "t2"), genTableName("test", "xx")}},
		[][]*filter.Table{{genTableName("xs1", "xt1"), genTableName("xs2", "xt2"), genTableName("xtest", "xxx")}},
		[]string{"DROP TABLE `xs1`.`xt1`, `xs2`.`xt2`, `xtest`.`xxx`"},
	},
	// CREATE TABLE
	{
		"create table `s1`.`t1` (id int)",
		[]string{"CREATE TABLE `s1`.`t1` (`id` INT)"},
		[][]*filter.Table{{genTableName("s1", "t1")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}},
		[]string{"CREATE TABLE `xs1`.`xt1` (`id` INT)"},
	},
	{
		"create table `t1` (id int)",
		[]string{"CREATE TABLE `t1` (`id` INT)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"CREATE TABLE `xtest`.`xt1` (`id` INT)"},
	},
	{
		"create table `s1` (c int default '0')",
		[]string{"CREATE TABLE `s1` (`c` INT DEFAULT '0')"},
		[][]*filter.Table{{genTableName("test", "s1")}},
		[][]*filter.Table{{genTableName("xtest", "xs1")}},
		[]string{"CREATE TABLE `xtest`.`xs1` (`c` INT DEFAULT '0')"},
	},
	// CREATE TABLE LIKE
	{
		"create table `t1` like `t2`",
		[]string{"CREATE TABLE `t1` LIKE `t2`"},
		[][]*filter.Table{{genTableName("test", "t1"), genTableName("test", "t2")}},
		[][]*filter.Table{{genTableName("xtest", "xt1"), genTableName("xtest", "xt2")}},
		[]string{"CREATE TABLE `xtest`.`xt1` LIKE `xtest`.`xt2`"},
	},
	{
		"create table `s1`.`t1` like `t2`",
		[]string{"CREATE TABLE `s1`.`t1` LIKE `t2`"},
		[][]*filter.Table{{genTableName("s1", "t1"), genTableName("test", "t2")}},
		[][]*filter.Table{{genTableName("xs1", "xt1"), genTableName("xtest", "xt2")}},
		[]string{"CREATE TABLE `xs1`.`xt1` LIKE `xtest`.`xt2`"},
	},
	{
		"create table `t1` like `xx`.`t2`",
		[]string{"CREATE TABLE `t1` LIKE `xx`.`t2`"},
		[][]*filter.Table{{genTableName("test", "t1"), genTableName("xx", "t2")}},
		[][]*filter.Table{{genTableName("xtest", "xt1"), genTableName("xxx", "xt2")}},
		[]string{"CREATE TABLE `xtest`.`xt1` LIKE `xxx`.`xt2`"},
	},
	// TRUNCATE TABLE
	{
		"truncate table `t1`",
		[]string{"TRUNCATE TABLE `t1`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"TRUNCATE TABLE `xtest`.`xt1`"},
	},
	{
		"truncate table `s1`.`t1`",
		[]string{"TRUNCATE TABLE `s1`.`t1`"},
		[][]*filter.Table{{genTableName("s1", "t1")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}},
		[]string{"TRUNCATE TABLE `xs1`.`xt1`"},
	},
	// RENAME TABLE - single
	{
		"rename table `s1`.`t1` to `s2`.`t2`",
		[]string{"RENAME TABLE `s1`.`t1` TO `s2`.`t2`"},
		[][]*filter.Table{{genTableName("s1", "t1"), genTableName("s2", "t2")}},
		[][]*filter.Table{{genTableName("xs1", "xt1"), genTableName("xs2", "xt2")}},
		[]string{"RENAME TABLE `xs1`.`xt1` TO `xs2`.`xt2`"},
	},
	// RENAME TABLE - multiple (no splitting)
	{
		"rename table `t1` to `t2`, `s1`.`t1` to `t2`",
		[]string{"RENAME TABLE `t1` TO `t2`, `s1`.`t1` TO `t2`"},
		[][]*filter.Table{{genTableName("test", "t1"), genTableName("test", "t2"), genTableName("s1", "t1"), genTableName("test", "t2")}},
		[][]*filter.Table{{genTableName("xtest", "xt1"), genTableName("xtest", "xt2"), genTableName("xs1", "xt1"), genTableName("xtest", "xt2")}},
		[]string{"RENAME TABLE `xtest`.`xt1` TO `xtest`.`xt2`, `xs1`.`xt1` TO `xtest`.`xt2`"},
	},
	// DROP INDEX
	{
		"drop index i1 on `s1`.`t1`",
		[]string{"DROP INDEX `i1` ON `s1`.`t1`"},
		[][]*filter.Table{{genTableName("s1", "t1")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}},
		[]string{"DROP INDEX `i1` ON `xs1`.`xt1`"},
	},
	{
		"drop index i1 on `t1`",
		[]string{"DROP INDEX `i1` ON `t1`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"DROP INDEX `i1` ON `xtest`.`xt1`"},
	},
	// CREATE INDEX
	{
		"create index i1 on `t1`(`c1`)",
		[]string{"CREATE INDEX `i1` ON `t1` (`c1`)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"CREATE INDEX `i1` ON `xtest`.`xt1` (`c1`)"},
	},
	{
		"create index i1 on `s1`.`t1`(`c1`)",
		[]string{"CREATE INDEX `i1` ON `s1`.`t1` (`c1`)"},
		[][]*filter.Table{{genTableName("s1", "t1")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}},
		[]string{"CREATE INDEX `i1` ON `xs1`.`xt1` (`c1`)"},
	},
	// ALTER TABLE - multiple specs (no splitting, test as single statement)
	{
		"alter table `t1` add column c1 int, drop column c2",
		[]string{"ALTER TABLE `t1` ADD COLUMN `c1` INT, DROP COLUMN `c2`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` ADD COLUMN `c1` INT, DROP COLUMN `c2`"},
	},
	{
		"alter table `s1`.`t1` add column c1 int, rename to `t2`, drop column c2",
		[]string{"ALTER TABLE `s1`.`t1` ADD COLUMN `c1` INT, RENAME AS `t2`, DROP COLUMN `c2`"},
		[][]*filter.Table{{genTableName("s1", "t1"), genTableName("test", "t2")}},
		[][]*filter.Table{{genTableName("xs1", "xt1"), genTableName("xtest", "xt2")}},
		[]string{"ALTER TABLE `xs1`.`xt1` ADD COLUMN `c1` INT, RENAME AS `xtest`.`xt2`, DROP COLUMN `c2`"},
	},
	{
		"alter table `s1`.`t1` add column c1 int, rename to `xx`.`t2`, drop column c2",
		[]string{"ALTER TABLE `s1`.`t1` ADD COLUMN `c1` INT, RENAME AS `xx`.`t2`, DROP COLUMN `c2`"},
		[][]*filter.Table{{genTableName("s1", "t1"), genTableName("xx", "t2")}},
		[][]*filter.Table{{genTableName("xs1", "xt1"), genTableName("xxx", "xt2")}},
		[]string{"ALTER TABLE `xs1`.`xt1` ADD COLUMN `c1` INT, RENAME AS `xxx`.`xt2`, DROP COLUMN `c2`"},
	},
	// ALTER TABLE with IF NOT EXISTS / IF EXISTS
	// Note: TiDB parser converts these to TiDB-specific comment syntax (/*T! ... */)
	{
		"alter table `t1` add column if not exists c1 int",
		[]string{"ALTER TABLE `t1` ADD COLUMN IF NOT EXISTS `c1` INT"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` ADD COLUMN /*T! IF NOT EXISTS  */`c1` INT"},
	},
	{
		"alter table `t1` add index if not exists (a) using btree comment 'a'",
		[]string{"ALTER TABLE `t1` ADD INDEX IF NOT EXISTS(`a`) USING BTREE COMMENT 'a'"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` ADD INDEX/*T!  IF NOT EXISTS */(`a`) USING BTREE COMMENT 'a'"},
	},
	{
		"alter table `t1` add constraint fk_t2_id foreign key if not exists (t2_id) references t2(id)",
		[]string{"ALTER TABLE `t1` ADD CONSTRAINT `fk_t2_id` FOREIGN KEY IF NOT EXISTS (`t2_id`) REFERENCES `t2`(`id`)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` ADD CONSTRAINT `fk_t2_id` FOREIGN KEY /*T! IF NOT EXISTS  */(`t2_id`) REFERENCES `t2`(`id`)"},
	},
	{
		"create index if not exists i1 on `t1`(`c1`)",
		[]string{"CREATE INDEX IF NOT EXISTS `i1` ON `t1` (`c1`)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"CREATE INDEX /*T! IF NOT EXISTS  */`i1` ON `xtest`.`xt1` (`c1`)"},
	},
	{
		"alter table `t1` add partition if not exists ( partition p2 values less than maxvalue)",
		[]string{"ALTER TABLE `t1` ADD PARTITION IF NOT EXISTS (PARTITION `p2` VALUES LESS THAN (MAXVALUE))"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` ADD PARTITION/*T!  IF NOT EXISTS */ (PARTITION `p2` VALUES LESS THAN (MAXVALUE))"},
	},
	{
		"alter table `t1` drop column if exists c2",
		[]string{"ALTER TABLE `t1` DROP COLUMN IF EXISTS `c2`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` DROP COLUMN /*T! IF EXISTS  */`c2`"},
	},
	{
		"alter table `t1` change column if exists a b varchar(255)",
		[]string{"ALTER TABLE `t1` CHANGE COLUMN IF EXISTS `a` `b` VARCHAR(255)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` CHANGE COLUMN /*T! IF EXISTS  */`a` `b` VARCHAR(255)"},
	},
	{
		"alter table `t1` modify column if exists a varchar(255)",
		[]string{"ALTER TABLE `t1` MODIFY COLUMN IF EXISTS `a` VARCHAR(255)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` MODIFY COLUMN /*T! IF EXISTS  */`a` VARCHAR(255)"},
	},
	{
		"alter table `t1` drop index if exists i1",
		[]string{"ALTER TABLE `t1` DROP INDEX IF EXISTS `i1`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` DROP INDEX /*T! IF EXISTS  */`i1`"},
	},
	{
		"alter table `t1` drop foreign key fk_t2_id",
		[]string{"ALTER TABLE `t1` DROP FOREIGN KEY `fk_t2_id`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` DROP FOREIGN KEY `fk_t2_id`"},
	},
	{
		"alter table `t1` drop partition if exists p2",
		[]string{"ALTER TABLE `t1` DROP PARTITION IF EXISTS `p2`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` DROP PARTITION /*T! IF EXISTS  */`p2`"},
	},
	// ALTER TABLE PARTITION BY
	{
		"alter table `t1` partition by hash(a)",
		[]string{"ALTER TABLE `t1` PARTITION BY HASH (`a`) PARTITIONS 1"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY HASH (`a`) PARTITIONS 1"},
	},
	{
		"alter table `t1` partition by key(a)",
		[]string{"ALTER TABLE `t1` PARTITION BY KEY (`a`) PARTITIONS 1"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY KEY (`a`) PARTITIONS 1"},
	},
	{
		"alter table `t1` partition by range(a) (partition x values less than (75))",
		[]string{"ALTER TABLE `t1` PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (75))"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (75))"},
	},
	{
		"alter table `t1` partition by list columns (a, b) (partition x values in ((10, 20)))",
		[]string{"ALTER TABLE `t1` PARTITION BY LIST COLUMNS (`a`,`b`) (PARTITION `x` VALUES IN ((10, 20)))"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY LIST COLUMNS (`a`,`b`) (PARTITION `x` VALUES IN ((10, 20)))"},
	},
	{
		"alter table `t1` partition by list (a) (partition x default)",
		[]string{"ALTER TABLE `t1` PARTITION BY LIST (`a`) (PARTITION `x` DEFAULT)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY LIST (`a`) (PARTITION `x` DEFAULT)"},
	},
	{
		"alter table `t1` partition by system_time (partition x history, partition y current)",
		[]string{"ALTER TABLE `t1` PARTITION BY SYSTEM_TIME (PARTITION `x` HISTORY,PARTITION `y` CURRENT)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY SYSTEM_TIME (PARTITION `x` HISTORY,PARTITION `y` CURRENT)"},
	},
	// ALTER DATABASE with explicit name
	{
		"alter database `test` charset utf8mb4",
		[]string{"ALTER DATABASE `test` CHARACTER SET = utf8mb4"},
		[][]*filter.Table{{genTableName("test", "")}},
		[][]*filter.Table{{genTableName("xtest", "")}},
		[]string{"ALTER DATABASE `xtest` CHARACTER SET = utf8mb4"},
	},
	// ALTER TABLE ADD COLUMN with multiple columns (no splitting)
	{
		"alter table `t1` add column (c1 int, c2 int)",
		[]string{"ALTER TABLE `t1` ADD COLUMN (`c1` INT, `c2` INT)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "t1")}},
		[]string{"ALTER TABLE `xtest`.`t1` ADD COLUMN (`c1` INT, `c2` INT)"},
	},
}

var nonDDLs = []string{
	"GRANT CREATE TABLESPACE ON *.* TO `root`@`%` WITH GRANT OPTION",
}

func TestParser(t *testing.T) {
	t.Parallel()
	p := parser.New()

	for _, ca := range testCases {
		sql := ca.sql
		stmts, _, err := p.Parse(sql, "", "")
		require.NoError(t, err)
		require.Len(t, stmts, 1)
	}

	for _, sql := range nonDDLs {
		stmts, _, err := p.Parse(sql, "", "")
		require.NoError(t, err)
		require.Len(t, stmts, 1)
	}

	unsupportedSQLs := []string{
		"alter table bar ADD SPATIAL INDEX (`g`)",
	}

	for _, sql := range unsupportedSQLs {
		_, _, err := p.Parse(sql, "", "")
		require.NotNil(t, err)
	}
}

func TestError(t *testing.T) {
	t.Parallel()
	p := parser.New()

	// DML will report ErrUnknownTypeDDL
	dml := "INSERT INTO `t1` VALUES (1)"

	stmts, _, err := p.Parse(dml, "", "")
	require.NoError(t, err)
	_, err = FetchDDLTables("test", stmts[0])
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown DDL type")

	_, err = RenameDDLTable(stmts[0], nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown DDL type")

	// tableRenameVisitor with less `targetNames` won't panic
	ddl := "create table `s1`.`t1` (id int)"
	stmts, _, err = p.Parse(ddl, "", "")
	require.NoError(t, err)
	_, err = RenameDDLTable(stmts[0], nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not enough target tables")
}

// TestResolveDDL tests FetchDDLTables and RenameDDLTable
func TestResolveDDL(t *testing.T) {
	t.Parallel()
	p := parser.New()

	for _, ca := range testCases {
		stmts, _, err := p.Parse(ca.sql, "", "")
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		// Test FetchDDLTables
		tableNames, err := FetchDDLTables("test", stmts[0])
		require.NoError(t, err)
		require.Equal(t, ca.expectedTableNames[0], tableNames, "FetchDDLTables failed for: %s", ca.sql)

		// Re-parse for RenameDDLTable since it modifies AST in place
		stmts, _, err = p.Parse(ca.sql, "", "")
		require.NoError(t, err)

		// Test RenameDDLTable
		targetSQL, err := RenameDDLTable(stmts[0], ca.targetTableNames[0])
		require.NoError(t, err)
		require.Equal(t, ca.targetSQLs[0], targetSQL, "RenameDDLTable failed for: %s", ca.sql)
	}
}
