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

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	sql                string
	expectedTableNames []commonEvent.SchemaTableName
	targetTableNames   []commonEvent.SchemaTableName
	targetSQL          string
}

// TestResolveDDL tests FetchDDLTables and RenameDDLTable
func TestResolveDDL(t *testing.T) {
	t.Parallel()
	testCases := []testCase{
		// Test case with foreign key - foreign key references should be renamed together
		{
			"create table `t1` (`id` int, `student_id` int, primary key (`id`), foreign key (`student_id`) references `t2`(`id`))",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}, {SchemaName: "", TableName: "t2"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "t1"}, {SchemaName: "xtest", TableName: "t2"}},
			"CREATE TABLE `xtest`.`t1` (`id` INT,`student_id` INT,PRIMARY KEY(`id`),CONSTRAINT FOREIGN KEY (`student_id`) REFERENCES `xtest`.`t2`(`id`))",
		},
		// CREATE SCHEMA/DATABASE
		{
			"create schema `s1`",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: ""}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: ""}},
			"CREATE DATABASE `xs1`",
		},
		{
			"create schema if not exists `s1`",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: ""}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: ""}},
			"CREATE DATABASE IF NOT EXISTS `xs1`",
		},
		// DROP SCHEMA/DATABASE
		{
			"drop schema `s1`",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: ""}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: ""}},
			"DROP DATABASE `xs1`",
		},
		{
			"drop schema if exists `s1`",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: ""}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: ""}},
			"DROP DATABASE IF EXISTS `xs1`",
		},
		// ALTER DATABASE without explicit name: nothing to route, text is kept as is
		{
			"alter database collate utf8mb4_general_ci",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: ""}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: ""}},
			"alter database collate utf8mb4_general_ci",
		},
		// DROP TABLE - single table
		{
			"drop table `Ss1`.`tT1`",
			[]commonEvent.SchemaTableName{{SchemaName: "Ss1", TableName: "tT1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xSs1", TableName: "xtT1"}},
			"DROP TABLE `xSs1`.`xtT1`",
		},
		// DROP TABLE - multiple tables (requires SplitDDL to split, so we test without splitting)
		{
			"drop table `s1`.`t1`, `s2`.`t2`",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: "t1"}, {SchemaName: "s2", TableName: "t2"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: "xt1"}, {SchemaName: "xs2", TableName: "xt2"}},
			"DROP TABLE `xs1`.`xt1`, `xs2`.`xt2`",
		},
		{
			"drop table `s1`.`t1`, `s2`.`t2`, `xx`",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: "t1"}, {SchemaName: "s2", TableName: "t2"}, {SchemaName: "", TableName: "xx"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: "xt1"}, {SchemaName: "xs2", TableName: "xt2"}, {SchemaName: "xtest", TableName: "xxx"}},
			"DROP TABLE `xs1`.`xt1`, `xs2`.`xt2`, `xtest`.`xxx`",
		},
		// CREATE TABLE
		{
			"create table `s1`.`t1` (id int)",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: "xt1"}},
			"CREATE TABLE `xs1`.`xt1` (`id` INT)",
		},
		{
			"create table `t1` (id int)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"CREATE TABLE `xtest`.`xt1` (`id` INT)",
		},
		{
			"create table `s1` (c int default '0')",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "s1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xs1"}},
			"CREATE TABLE `xtest`.`xs1` (`c` INT DEFAULT '0')",
		},
		// CREATE TABLE LIKE
		{
			"create table `t1` like `t2`",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}, {SchemaName: "", TableName: "t2"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}, {SchemaName: "xtest", TableName: "xt2"}},
			"CREATE TABLE `xtest`.`xt1` LIKE `xtest`.`xt2`",
		},
		{
			"create table `s1`.`t1` like `t2`",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: "t1"}, {SchemaName: "", TableName: "t2"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: "xt1"}, {SchemaName: "xtest", TableName: "xt2"}},
			"CREATE TABLE `xs1`.`xt1` LIKE `xtest`.`xt2`",
		},
		{
			"create table `t1` like `xx`.`t2`",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}, {SchemaName: "xx", TableName: "t2"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}, {SchemaName: "xxx", TableName: "xt2"}},
			"CREATE TABLE `xtest`.`xt1` LIKE `xxx`.`xt2`",
		},
		// TRUNCATE TABLE
		{
			"truncate table `t1`",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"TRUNCATE TABLE `xtest`.`xt1`",
		},
		{
			"truncate table `s1`.`t1`",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: "xt1"}},
			"TRUNCATE TABLE `xs1`.`xt1`",
		},
		// RENAME TABLE - single
		{
			"rename table `s1`.`t1` to `s2`.`t2`",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: "t1"}, {SchemaName: "s2", TableName: "t2"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: "xt1"}, {SchemaName: "xs2", TableName: "xt2"}},
			"RENAME TABLE `xs1`.`xt1` TO `xs2`.`xt2`",
		},
		// RENAME TABLE - multiple (no splitting)
		{
			"rename table `t1` to `t2`, `s1`.`t1` to `t2`",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}, {SchemaName: "", TableName: "t2"}, {SchemaName: "s1", TableName: "t1"}, {SchemaName: "", TableName: "t2"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}, {SchemaName: "xtest", TableName: "xt2"}, {SchemaName: "xs1", TableName: "xt1"}, {SchemaName: "xtest", TableName: "xt2"}},
			"RENAME TABLE `xtest`.`xt1` TO `xtest`.`xt2`, `xs1`.`xt1` TO `xtest`.`xt2`",
		},
		// DROP INDEX
		{
			"drop index i1 on `s1`.`t1`",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: "xt1"}},
			"DROP INDEX `i1` ON `xs1`.`xt1`",
		},
		{
			"drop index i1 on `t1`",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"DROP INDEX `i1` ON `xtest`.`xt1`",
		},
		// CREATE INDEX
		{
			"create index i1 on `t1`(`c1`)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"CREATE INDEX `i1` ON `xtest`.`xt1` (`c1`)",
		},
		{
			"create index i1 on `s1`.`t1`(`c1`)",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: "xt1"}},
			"CREATE INDEX `i1` ON `xs1`.`xt1` (`c1`)",
		},
		// ALTER TABLE - multiple specs (no splitting, test as single statement)
		{
			"alter table `t1` add column c1 int, drop column c2",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` ADD COLUMN `c1` INT, DROP COLUMN `c2`",
		},
		{
			"alter table `s1`.`t1` add column c1 int, rename to `t2`, drop column c2",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: "t1"}, {SchemaName: "", TableName: "t2"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: "xt1"}, {SchemaName: "xtest", TableName: "xt2"}},
			"ALTER TABLE `xs1`.`xt1` ADD COLUMN `c1` INT, RENAME AS `xtest`.`xt2`, DROP COLUMN `c2`",
		},
		{
			"alter table `s1`.`t1` add column c1 int, rename to `xx`.`t2`, drop column c2",
			[]commonEvent.SchemaTableName{{SchemaName: "s1", TableName: "t1"}, {SchemaName: "xx", TableName: "t2"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xs1", TableName: "xt1"}, {SchemaName: "xxx", TableName: "xt2"}},
			"ALTER TABLE `xs1`.`xt1` ADD COLUMN `c1` INT, RENAME AS `xxx`.`xt2`, DROP COLUMN `c2`",
		},
		// ALTER TABLE with IF NOT EXISTS / IF EXISTS
		// Note: TiDB parser converts these to TiDB-specific comment syntax (/*T! ... */)
		{
			"alter table `t1` add column if not exists c1 int",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` ADD COLUMN /*T! IF NOT EXISTS  */`c1` INT",
		},
		{
			"alter table `t1` add index if not exists (a) using btree comment 'a'",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` ADD INDEX/*T!  IF NOT EXISTS */(`a`) USING BTREE COMMENT 'a'",
		},
		{
			"alter table `t1` add constraint fk_t2_id foreign key if not exists (t2_id) references t2(id)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}, {SchemaName: "", TableName: "t2"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}, {SchemaName: "xtest", TableName: "xt2"}},
			"ALTER TABLE `xtest`.`xt1` ADD CONSTRAINT `fk_t2_id` FOREIGN KEY /*T! IF NOT EXISTS  */(`t2_id`) REFERENCES `xtest`.`xt2`(`id`)",
		},
		{
			"create index if not exists i1 on `t1`(`c1`)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"CREATE INDEX /*T! IF NOT EXISTS  */`i1` ON `xtest`.`xt1` (`c1`)",
		},
		{
			"alter table `t1` add partition if not exists ( partition p2 values less than maxvalue)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` ADD PARTITION/*T!  IF NOT EXISTS */ (PARTITION `p2` VALUES LESS THAN (MAXVALUE))",
		},
		{
			"alter table `t1` drop column if exists c2",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` DROP COLUMN /*T! IF EXISTS  */`c2`",
		},
		{
			"alter table `t1` change column if exists a b varchar(255)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` CHANGE COLUMN /*T! IF EXISTS  */`a` `b` VARCHAR(255)",
		},
		{
			"alter table `t1` modify column if exists a varchar(255)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` MODIFY COLUMN /*T! IF EXISTS  */`a` VARCHAR(255)",
		},
		{
			"alter table `t1` drop index if exists i1",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` DROP INDEX /*T! IF EXISTS  */`i1`",
		},
		{
			"alter table `t1` drop foreign key fk_t2_id",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` DROP FOREIGN KEY `fk_t2_id`",
		},
		{
			"alter table `t1` drop partition if exists p2",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` DROP PARTITION /*T! IF EXISTS  */`p2`",
		},
		// ALTER TABLE PARTITION BY
		{
			"alter table `t1` partition by hash(a)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` PARTITION BY HASH (`a`) PARTITIONS 1",
		},
		{
			"alter table `t1` partition by key(a)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` PARTITION BY KEY (`a`) PARTITIONS 1",
		},
		{
			"alter table `t1` partition by range(a) (partition x values less than (75))",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (75))",
		},
		{
			"alter table `t1` partition by list columns (a, b) (partition x values in ((10, 20)))",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` PARTITION BY LIST COLUMNS (`a`,`b`) (PARTITION `x` VALUES IN ((10, 20)))",
		},
		{
			"alter table `t1` partition by list (a) (partition x default)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` PARTITION BY LIST (`a`) (PARTITION `x` DEFAULT)",
		},
		{
			"alter table `t1` partition by system_time (partition x history, partition y current)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "xt1"}},
			"ALTER TABLE `xtest`.`xt1` PARTITION BY SYSTEM_TIME (PARTITION `x` HISTORY,PARTITION `y` CURRENT)",
		},
		// ALTER DATABASE with explicit name
		{
			"alter database `test` charset utf8mb4",
			[]commonEvent.SchemaTableName{{SchemaName: "test", TableName: ""}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: ""}},
			"ALTER DATABASE `xtest` CHARACTER SET = utf8mb4",
		},
		// ALTER TABLE ADD COLUMN with multiple columns, identity mapping: text is kept as is
		{
			"alter table `t1` add column (c1 int, c2 int)",
			[]commonEvent.SchemaTableName{{SchemaName: "", TableName: "t1"}},
			[]commonEvent.SchemaTableName{{SchemaName: "xtest", TableName: "t1"}},
			"alter table `t1` add column (c1 int, c2 int)",
		},
	}
	for _, ca := range testCases {
		router := newTestRouter(t, false, routeRulesForTest(t, ca.expectedTableNames, ca.targetTableNames))
		newQuery, changed, err := router.rewriteSingleDDLQuery(ca.sql, "xtest")
		require.NoError(t, err, "rewriteSingleDDLQuery failed for: %s", ca.sql)
		if !changed {
			require.Equal(t, ca.sql, newQuery, "unrouted statement changed: %s", ca.sql)
			continue
		}
		require.Equal(t, ca.targetSQL, newQuery, "rewriteSingleDDLQuery failed for: %s", ca.sql)
	}
}

// routeRulesForTest builds one rule per routed source/target pair, so each test
// case keeps expressing the mapping it asserts.
func routeRulesForTest(t *testing.T, sources, targets []commonEvent.SchemaTableName) []*config.DispatchRule {
	t.Helper()
	require.Len(t, targets, len(sources))

	var rules []*config.DispatchRule
	for i, source := range sources {
		target := targets[i]
		schema := source.SchemaName
		if schema == "" {
			schema = "xtest"
		}
		if target.SchemaName == schema && target.TableName == source.TableName {
			continue
		}
		rule := &config.DispatchRule{TargetSchema: target.SchemaName}
		if source.TableName == "" {
			rule.Matcher = []string{schema + ".*"}
		} else {
			rule.Matcher = []string{schema + "." + source.TableName}
			rule.TargetTable = target.TableName
		}
		rules = append(rules, rule)
	}
	return rules
}

func TestRewriteParserBackedDDLQueryWithSemicolonsInLiteralsAndComments(t *testing.T) {
	t.Parallel()

	router := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "{table}_routed",
	}})

	tests := []struct {
		name              string
		query             string
		requiredFragments []string
		forbiddenFragment string
	}{
		{
			name:  "semicolon inside default string",
			query: "CREATE TABLE `source_db`.`semi_table` (`c` VARCHAR(100) DEFAULT 'a;b;c');",
			requiredFragments: []string{
				"`target_db`.`semi_table_routed`",
				"DEFAULT 'a;b;c'",
			},
			forbiddenFragment: "`source_db`.`semi_table`",
		},
		{
			name:  "semicolon inside block comment",
			query: "/* comment; not a statement */ CREATE TABLE `source_db`.`comment_table` (`c` INT);",
			requiredFragments: []string{
				"`target_db`.`comment_table_routed`",
			},
			forbiddenFragment: "`source_db`.`comment_table`",
		},
		{
			name: "multi statement with semicolons inside default string and comment",
			query: "CREATE TABLE `source_db`.`t1` (`c` VARCHAR(100) DEFAULT 'a;b'); " +
				"/* comment; not a statement */ CREATE TABLE `source_db`.`t2` (`c` INT);",
			requiredFragments: []string{
				"`target_db`.`t1_routed`",
				"DEFAULT 'a;b'",
				"`target_db`.`t2_routed`",
			},
			forbiddenFragment: "`source_db`",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			newQuery, err := router.rewriteParserBackedDDLQuery(&commonEvent.DDLEvent{Query: tc.query})
			require.NoError(t, err)
			for _, fragment := range tc.requiredFragments {
				require.Contains(t, newQuery, fragment)
			}
			require.NotContains(t, newQuery, tc.forbiddenFragment)
		})
	}
}

func TestRewriteParserBackedDDLQueryUsesEventSchemaForUnqualifiedReferences(t *testing.T) {
	t.Parallel()

	router := newTestRouter(t, false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.*"},
			TargetSchema: "target_db",
			TargetTable:  TablePlaceholder,
		},
		{
			Matcher:      []string{"source_extra_db.*"},
			TargetSchema: "target_extra_db",
			TargetTable:  TablePlaceholder,
		},
	})

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "create table like",
			query:    "CREATE TABLE `source_db`.`external_users` LIKE `users`",
			expected: "CREATE TABLE `target_db`.`external_users` LIKE `target_db`.`users`",
		},
		{
			name:     "create table as select",
			query:    "CREATE TABLE `source_db`.`external_users` AS SELECT * FROM `users`",
			expected: "CREATE TABLE `target_db`.`external_users` AS SELECT * FROM `target_db`.`users`",
		},
		{
			name:     "create view as select",
			query:    "CREATE VIEW `source_db`.`external_users` AS SELECT * FROM `users`",
			expected: "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `target_db`.`external_users` AS SELECT * FROM `target_db`.`users`",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			newQuery, err := router.rewriteParserBackedDDLQuery(&commonEvent.DDLEvent{
				SchemaName: "source_db",
				TableName:  "external_users",
				Query:      tc.query,
			})
			require.NoError(t, err)
			require.Equal(t, tc.expected, newQuery)
		})
	}
}

// TestRewriteParserBackedDDLQueryCorrelatedReference covers references that are
// bound through the SELECT scope chain: a table-qualified `orders`.`id` that
// resolves to a table of an enclosing SELECT follows that table's routed name,
// and an alias reference stays untouched.
//
// The statement carrier is `CREATE TABLE ... AS SELECT`, which TiDB rejects
// before it reaches CDC (pkg/planner/core/preprocess.go, issue 4754). The same
// scope rules apply to every DDL that carries a SELECT, so the test pins the
// resolution behavior independently of that reachability.
func TestRewriteParserBackedDDLQueryCorrelatedReference(t *testing.T) {
	t.Parallel()

	router := newTestRouter(t, false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.*"},
			TargetSchema: "target_db",
			TargetTable:  "{table}_r",
		},
	})

	rewrite := func(t *testing.T, table, query string) string {
		t.Helper()
		newQuery, err := router.rewriteParserBackedDDLQuery(&commonEvent.DDLEvent{
			SchemaName: "source_db",
			TableName:  table,
			Query:      query,
		})
		require.NoError(t, err)
		return newQuery
	}

	t.Run("where clause", func(t *testing.T) {
		t.Parallel()
		require.Equal(t,
			"CREATE TABLE `target_db`.`t3_r` AS SELECT `id` FROM `target_db`.`orders_r` "+
				"WHERE EXISTS (SELECT 1 FROM `target_db`.`line_items_r` "+
				"WHERE `target_db`.`line_items_r`.`order_id`=`target_db`.`orders_r`.`id`)",
			rewrite(t, "t3", "CREATE TABLE t3 AS SELECT id FROM source_db.orders WHERE EXISTS "+
				"(SELECT 1 FROM source_db.line_items WHERE line_items.order_id = orders.id)"))
	})

	// The reference is visited before the FROM of its own SELECT, so resolution
	// must not depend on the order in which the AST is walked.
	t.Run("field list", func(t *testing.T) {
		t.Parallel()
		newQuery := rewrite(t, "t4", "CREATE TABLE t4 AS SELECT (SELECT orders.id "+
			"FROM source_db.line_items WHERE line_items.order_id = orders.id) AS x "+
			"FROM source_db.orders")
		require.Contains(t, newQuery, "`target_db`.`orders_r`.`id`")
		require.NotContains(t, newQuery, "`orders`.`id`")
	})

	t.Run("alias reference", func(t *testing.T) {
		t.Parallel()
		newQuery := rewrite(t, "t5", "CREATE TABLE t5 AS SELECT x.id FROM source_db.orders AS x "+
			"WHERE EXISTS (SELECT 1 FROM source_db.line_items WHERE line_items.order_id = x.id)")
		require.Contains(t, newQuery, "`target_db`.`orders_r` AS `x`")
		require.Contains(t, newQuery, "`x`.`id`")
		require.NotContains(t, newQuery, "`target_db`.`orders_r`.`id`")
	})
}

// TestRewriteParserBackedDDLQueryRangeVariableResolution pins the resolution
// rules that pkg/common/event's view normalizer also implements (see
// TestCorrelatedColumns there): a table-qualified reference resolves from its own
// SELECT outward, and an alias, a CTE name, or an ambiguous declaration stops the
// search and keeps the reference unchanged.
func TestRewriteParserBackedDDLQueryRangeVariableResolution(t *testing.T) {
	t.Parallel()

	router := newTestRouter(t, false, []*config.DispatchRule{
		{Matcher: []string{"source_db.*"}, TargetSchema: "target_db", TargetTable: "{table}_r"},
		{Matcher: []string{"other_db.*"}, TargetSchema: "other_target", TargetTable: "{table}_r"},
	})

	tests := []struct {
		name        string
		eventSchema string
		query       string
		expected    []string
		absent      []string
	}{
		{
			name:     "case insensitive reference matches",
			query:    "CREATE TABLE routed AS SELECT Orders.id FROM source_db.orders",
			expected: []string{"`target_db`.`orders_r`.`id`"},
		},
		{
			name:     "local table shadows outer",
			query:    "CREATE TABLE routed AS SELECT id FROM source_db.orders WHERE EXISTS (SELECT 1 FROM other_db.orders WHERE orders.id = 1)",
			expected: []string{"`other_target`.`orders_r`.`id`"},
			absent:   []string{"`target_db`.`orders_r`.`id`"},
		},
		{
			name:        "unqualified local table shadows outer",
			eventSchema: "other_db",
			query:       "CREATE TABLE routed AS SELECT id FROM source_db.line_items WHERE EXISTS (SELECT 1 FROM orders WHERE orders.id = 1)",
			expected:    []string{"`other_target`.`orders_r`.`id`"},
		},
		{
			name:     "alias shadows outer",
			query:    "CREATE TABLE routed AS SELECT id FROM source_db.line_items WHERE EXISTS (SELECT 1 FROM other_db.orders AS orders WHERE orders.id = 1)",
			expected: []string{"`orders`.`id`", "FROM `other_target`.`orders_r` AS `orders`"},
			absent:   []string{"`other_target`.`orders_r`.`id`"},
		},
		{
			name:     "CTE shadows outer",
			query:    "CREATE TABLE routed AS SELECT id FROM source_db.orders WHERE EXISTS (WITH orders AS (SELECT 1 AS id) SELECT 1 FROM orders WHERE orders.id = 1)",
			expected: []string{"`orders`.`id`"},
			absent:   []string{"`target_db`.`orders_r`.`id`"},
		},
		{
			name:     "ambiguous local table",
			query:    "CREATE TABLE routed AS SELECT id FROM source_db.line_items WHERE EXISTS (SELECT 1 FROM source_db.orders, other_db.orders WHERE orders.id = 1)",
			expected: []string{"`orders`.`id`"},
			absent:   []string{"`orders_r`.`id`"},
		},
		{
			name:     "multiple levels",
			query:    "CREATE TABLE routed AS SELECT id FROM source_db.orders WHERE EXISTS (SELECT 1 FROM source_db.line_items WHERE EXISTS (SELECT 1 WHERE orders.id = 1))",
			expected: []string{"`target_db`.`orders_r`.`id`"},
			absent:   []string{"`orders`.`id`"},
		},
		{
			name:     "cross schema correlated",
			query:    "CREATE TABLE routed AS SELECT id FROM other_db.orders WHERE EXISTS (SELECT 1 FROM source_db.line_items WHERE line_items.order_id = orders.id)",
			expected: []string{"`other_target`.`orders_r`.`id`"},
			absent:   []string{"`target_db`.`orders_r`.`id`"},
		},
		{
			name:     "union branch",
			query:    "CREATE TABLE routed AS SELECT id FROM source_db.orders WHERE EXISTS (SELECT orders.id FROM source_db.line_items UNION SELECT orders.id FROM source_db.line_items)",
			expected: []string{"`target_db`.`orders_r`.`id`"},
			absent:   []string{"`orders`.`id`"},
		},
		{
			name:     "derived table alias",
			query:    "CREATE TABLE routed AS SELECT x.id FROM (SELECT id FROM source_db.orders) AS x WHERE EXISTS (SELECT 1 FROM source_db.line_items WHERE line_items.order_id = x.id)",
			expected: []string{"FROM (SELECT `id` FROM `target_db`.`orders_r`) AS `x`", "`x`.`id`"},
			absent:   []string{"`target_db`.`orders_r`.`id`"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eventSchema := tc.eventSchema
			if eventSchema == "" {
				eventSchema = "source_db"
			}
			newQuery, err := router.rewriteParserBackedDDLQuery(&commonEvent.DDLEvent{
				SchemaName: eventSchema,
				TableName:  "routed",
				Query:      tc.query,
			})
			require.NoError(t, err)
			for _, fragment := range tc.expected {
				require.Contains(t, newQuery, fragment)
			}
			for _, fragment := range tc.absent {
				require.NotContains(t, newQuery, fragment)
			}
		})
	}
}

// TestRewriteParserBackedDDLQueryCaseSensitiveBinding covers the router's
// case-sensitive mode: physical table names stay distinct, while aliases are
// still matched case-insensitively.
func TestRewriteParserBackedDDLQueryCaseSensitiveBinding(t *testing.T) {
	t.Parallel()

	router := newTestRouter(t, true, []*config.DispatchRule{
		{Matcher: []string{"source_db.Orders"}, TargetSchema: "target_db", TargetTable: "{table}_r"},
		{Matcher: []string{"other_db.orders"}, TargetSchema: "other_target", TargetTable: "{table}_r"},
	})

	rewrite := func(t *testing.T, query string) string {
		t.Helper()
		newQuery, err := router.rewriteParserBackedDDLQuery(&commonEvent.DDLEvent{
			SchemaName: "source_db",
			TableName:  "routed",
			Query:      query,
		})
		require.NoError(t, err)
		return newQuery
	}

	t.Run("tables differing only by case stay distinct", func(t *testing.T) {
		t.Parallel()
		newQuery := rewrite(t, "CREATE TABLE routed AS SELECT Orders.id, orders.id FROM source_db.Orders, other_db.orders")
		require.Contains(t, newQuery, "`target_db`.`Orders_r`.`id`")
		require.Contains(t, newQuery, "`other_target`.`orders_r`.`id`")
	})

	t.Run("reference case must match", func(t *testing.T) {
		t.Parallel()
		newQuery := rewrite(t, "CREATE TABLE routed AS SELECT orders.id FROM source_db.Orders")
		require.Contains(t, newQuery, "`orders`.`id`")
		require.NotContains(t, newQuery, "`target_db`.`Orders_r`.`id`")
	})
}

func TestRewriteParserBackedDDLQueryUsesQuerySchemaForCreateTableLike(t *testing.T) {
	t.Parallel()

	router := newTestRouter(t, false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.*"},
			TargetSchema: "target_db",
			TargetTable:  TablePlaceholder,
		},
		{
			Matcher:      []string{"source_extra_db.*"},
			TargetSchema: "target_extra_db",
			TargetTable:  TablePlaceholder,
		},
	})

	newQuery, err := router.rewriteParserBackedDDLQuery(&commonEvent.DDLEvent{
		SchemaName: "source_extra_db",
		TableName:  "external_users",
		Query:      "CREATE TABLE `source_extra_db`.`external_users` LIKE `source_db`.`users`",
	})
	require.NoError(t, err)
	require.Equal(t, "CREATE TABLE `target_extra_db`.`external_users` LIKE `target_db`.`users`", newQuery)
}
