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

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

// TestRewriteDDLQueryWithNilRouter tests that DDL queries pass through unchanged when no router is configured.
func TestRewriteDDLQueryWithNilRouter(t *testing.T) {
	t.Parallel()

	ddlEvent := &commonEvent.DDLEvent{
		Query: "CREATE TABLE `source_db`.`test_table` (id INT PRIMARY KEY)",
		TableInfo: &common.TableInfo{
			TableName: common.TableName{Schema: "source_db", Table: "test_table"},
		},
	}

	originalQuery := ddlEvent.Query
	result, err := RewriteDDLQueryWithRouting(nil, ddlEvent, "test-changefeed")
	require.NoError(t, err)
	require.False(t, result.WasRewritten, "Query should not be rewritten when router is nil")
	require.Equal(t, originalQuery, result.NewQuery, "Query should not be modified when router is nil")
}

// TestRewriteDDLQueryWithEmptyQuery tests that empty queries are handled correctly.
func TestRewriteDDLQueryWithEmptyQuery(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []RoutingRuleConfig{
		{
			Matcher:    []string{"source_db.*"},
			SchemaRule: "target_db",
			TableRule:  TablePlaceholder,
		},
	})
	require.NoError(t, err)

	ddlEvent := &commonEvent.DDLEvent{
		Query: "",
	}

	result, err := RewriteDDLQueryWithRouting(router, ddlEvent, "test-changefeed")
	require.NoError(t, err)
	require.False(t, result.WasRewritten)
	require.Equal(t, "", result.NewQuery)
}

// TestRewriteDDLQueryWithBasicRouting tests that DDL queries are rewritten when routing is configured.
func TestRewriteDDLQueryWithBasicRouting(t *testing.T) {
	t.Parallel()

	// Create a router with a routing rule: source_db.* -> target_db.*
	router, err := NewRouter(false, []RoutingRuleConfig{
		{
			Matcher:    []string{"source_db.*"},
			SchemaRule: "target_db",
			TableRule:  TablePlaceholder,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// Test case 1: DDL with routing applied
	ddlEvent := &commonEvent.DDLEvent{
		Query: "CREATE TABLE `source_db`.`test_table` (id INT PRIMARY KEY)",
		TableInfo: &common.TableInfo{
			TableName: common.TableName{Schema: "source_db", Table: "test_table"},
		},
	}

	result, err := RewriteDDLQueryWithRouting(router, ddlEvent, "test-changefeed")
	require.NoError(t, err)
	require.True(t, result.WasRewritten)
	require.Contains(t, result.NewQuery, "target_db")
	require.Contains(t, result.NewQuery, "test_table")
	require.NotContains(t, result.NewQuery, "source_db")
	require.Equal(t, "target_db", result.TargetSchemaName)

	// Test case 2: DDL without routing (no matching rule)
	ddlEvent2 := &commonEvent.DDLEvent{
		Query: "CREATE TABLE `other_db`.`other_table` (id INT PRIMARY KEY)",
		TableInfo: &common.TableInfo{
			TableName: common.TableName{Schema: "other_db", Table: "other_table"},
		},
	}

	originalQuery := ddlEvent2.Query
	result2, err := RewriteDDLQueryWithRouting(router, ddlEvent2, "test-changefeed")
	require.NoError(t, err)
	require.False(t, result2.WasRewritten, "Query should not be rewritten when no routing rule matches")
	require.Equal(t, originalQuery, result2.NewQuery, "Query should not be modified when no routing rule matches")
}

// TestRewriteDDLQueryWithSchemaAndTableRouting tests routing where BOTH schema AND table change.
// Example: source_db.test_table -> target_db.test_table_routed
func TestRewriteDDLQueryWithSchemaAndTableRouting(t *testing.T) {
	t.Parallel()

	// Create a router with full routing: source_db.test_table -> target_db.test_table_routed
	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"source_db.test_table"},
			SchemaRule: "target_db",
			TableRule:  "test_table_routed",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	tests := []struct {
		name          string
		query         string
		schema        string
		table         string
		expectedQuery string
	}{
		{
			name:          "CREATE TABLE",
			query:         "CREATE TABLE `source_db`.`test_table` (id INT PRIMARY KEY)",
			schema:        "source_db",
			table:         "test_table",
			expectedQuery: "`target_db`.`test_table_routed`",
		},
		{
			name:          "DROP TABLE",
			query:         "DROP TABLE `source_db`.`test_table`",
			schema:        "source_db",
			table:         "test_table",
			expectedQuery: "`target_db`.`test_table_routed`",
		},
		{
			name:          "TRUNCATE TABLE",
			query:         "TRUNCATE TABLE `source_db`.`test_table`",
			schema:        "source_db",
			table:         "test_table",
			expectedQuery: "`target_db`.`test_table_routed`",
		},
		{
			name:          "ALTER TABLE ADD COLUMN",
			query:         "ALTER TABLE `source_db`.`test_table` ADD COLUMN name VARCHAR(255)",
			schema:        "source_db",
			table:         "test_table",
			expectedQuery: "`target_db`.`test_table_routed`",
		},
		{
			name:          "CREATE INDEX",
			query:         "CREATE INDEX idx_name ON `source_db`.`test_table`(name)",
			schema:        "source_db",
			table:         "test_table",
			expectedQuery: "`target_db`.`test_table_routed`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddlEvent := &commonEvent.DDLEvent{
				Query: tt.query,
				TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: tt.schema, Table: tt.table},
				},
			}

			result, err := RewriteDDLQueryWithRouting(router, ddlEvent, "test-changefeed")
			require.NoError(t, err)
			require.True(t, result.WasRewritten)
			require.Contains(t, result.NewQuery, tt.expectedQuery, "Query should contain routed schema and table")
			require.NotContains(t, result.NewQuery, "source_db", "Query should not contain source schema")
			require.NotContains(t, result.NewQuery, "test_table`", "Query should not contain source table (checking with backtick to avoid matching test_table_routed)")
			require.Equal(t, "target_db", result.TargetSchemaName)
		})
	}
}

// TestRewriteDDLQueryWithSchemaOnlyRouting tests routing where ONLY the schema changes.
// The table name stays the same via TableRule="{table}".
// Example: source_db.test_table -> target_db.test_table
func TestRewriteDDLQueryWithSchemaOnlyRouting(t *testing.T) {
	t.Parallel()

	// Create a router that only routes the schema: source_db.* -> target_db.*
	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"source_db.*"},
			SchemaRule: "target_db",
			TableRule:  TablePlaceholder,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	tests := []struct {
		name          string
		query         string
		schema        string
		table         string
		expectedQuery string
	}{
		{
			name:          "CREATE TABLE users",
			query:         "CREATE TABLE `source_db`.`users` (id INT PRIMARY KEY)",
			schema:        "source_db",
			table:         "users",
			expectedQuery: "`target_db`.`users`",
		},
		{
			name:          "CREATE TABLE IF NOT EXISTS",
			query:         "CREATE TABLE IF NOT EXISTS `source_db`.`test_table` (id INT PRIMARY KEY)",
			schema:        "source_db",
			table:         "test_table",
			expectedQuery: "`target_db`.`test_table`",
		},
		{
			name:          "DROP TABLE",
			query:         "DROP TABLE `source_db`.`test_table`",
			schema:        "source_db",
			table:         "test_table",
			expectedQuery: "`target_db`.`test_table`",
		},
		{
			name:          "TRUNCATE TABLE",
			query:         "TRUNCATE TABLE `source_db`.`test_table`",
			schema:        "source_db",
			table:         "test_table",
			expectedQuery: "`target_db`.`test_table`",
		},
		{
			name:          "ALTER TABLE products",
			query:         "ALTER TABLE `source_db`.`products` ADD COLUMN price DECIMAL(10,2)",
			schema:        "source_db",
			table:         "products",
			expectedQuery: "`target_db`.`products`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddlEvent := &commonEvent.DDLEvent{
				Query: tt.query,
				TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: tt.schema, Table: tt.table},
				},
			}

			result, err := RewriteDDLQueryWithRouting(router, ddlEvent, "test-changefeed")
			require.NoError(t, err)
			require.True(t, result.WasRewritten)
			require.Contains(t, result.NewQuery, tt.expectedQuery, "Query should contain routed schema with original table")
			require.NotContains(t, result.NewQuery, "source_db", "Query should not contain source schema")
			require.Equal(t, "target_db", result.TargetSchemaName)
		})
	}
}

// TestRewriteDDLQueryWithTableOnlyRouting tests routing where ONLY the table name changes.
// The schema stays the same via SchemaRule="{schema}".
// Example: source_db.test_table -> source_db.test_table_routed
func TestRewriteDDLQueryWithTableOnlyRouting(t *testing.T) {
	t.Parallel()

	// Create a router that only routes the table name: mydb.old_table -> mydb.new_table
	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"mydb.old_table"},
			SchemaRule: SchemaPlaceholder, // Keep schema unchanged
			TableRule:  "new_table",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	tests := []struct {
		name          string
		query         string
		schema        string
		table         string
		expectedQuery string
	}{
		{
			name:          "CREATE TABLE",
			query:         "CREATE TABLE `mydb`.`old_table` (id INT PRIMARY KEY)",
			schema:        "mydb",
			table:         "old_table",
			expectedQuery: "`mydb`.`new_table`",
		},
		{
			name:          "DROP TABLE",
			query:         "DROP TABLE `mydb`.`old_table`",
			schema:        "mydb",
			table:         "old_table",
			expectedQuery: "`mydb`.`new_table`",
		},
		{
			name:          "TRUNCATE TABLE",
			query:         "TRUNCATE TABLE `mydb`.`old_table`",
			schema:        "mydb",
			table:         "old_table",
			expectedQuery: "`mydb`.`new_table`",
		},
		{
			name:          "ALTER TABLE",
			query:         "ALTER TABLE `mydb`.`old_table` ADD COLUMN status VARCHAR(50)",
			schema:        "mydb",
			table:         "old_table",
			expectedQuery: "`mydb`.`new_table`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddlEvent := &commonEvent.DDLEvent{
				Query: tt.query,
				TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: tt.schema, Table: tt.table},
				},
			}

			result, err := RewriteDDLQueryWithRouting(router, ddlEvent, "test-changefeed")
			require.NoError(t, err)
			require.True(t, result.WasRewritten)
			require.Contains(t, result.NewQuery, tt.expectedQuery, "Query should contain original schema with routed table")
			require.NotContains(t, result.NewQuery, "old_table", "Query should not contain source table")
			require.Equal(t, "mydb", result.TargetSchemaName)
		})
	}
}

// TestRewriteDDLQueryWithMultiTableOrdering tests that FetchDDLTables and RenameDDLTable maintain correct ordering
// when DDLs reference multiple tables (e.g., RENAME TABLE, CREATE TABLE LIKE).
// This ensures that tables are rewritten in the correct order when some are mapped and some are not.
func TestRewriteDDLQueryWithMultiTableOrdering(t *testing.T) {
	t.Parallel()

	// Create a router where one schema is mapped and one is not
	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"mapped_source_db.*"},
			SchemaRule: "mapped_target_db",
			TableRule:  TablePlaceholder,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	tests := []struct {
		name          string
		query         string
		schema        string
		table         string
		expectedQuery string
		description   string
	}{
		{
			name:          "DROP TABLE - mapped schema",
			query:         "DROP TABLE `mapped_source_db`.`t1`",
			schema:        "mapped_source_db",
			table:         "t1",
			expectedQuery: "DROP TABLE `mapped_target_db`.`t1`",
			description:   "Tests DROP TABLE with mapped schema",
		},
		{
			name:          "DROP TABLE - unmapped schema",
			query:         "DROP TABLE `unmapped_db`.`t1`",
			schema:        "unmapped_db",
			table:         "t1",
			expectedQuery: "DROP TABLE `unmapped_db`.`t1`",
			description:   "Tests DROP TABLE with unmapped schema (identity mapping)",
		},
		{
			name:          "RENAME TABLE - mapped old, mapped new",
			query:         "RENAME TABLE `mapped_source_db`.`old_table` TO `mapped_source_db`.`new_table`",
			schema:        "mapped_source_db",
			table:         "old_table",
			expectedQuery: "RENAME TABLE `mapped_target_db`.`old_table` TO `mapped_target_db`.`new_table`",
			description:   "Tests RENAME TABLE where both old and new schemas are mapped (same schema)",
		},
		{
			name:          "RENAME TABLE - mapped old, unmapped new",
			query:         "RENAME TABLE `mapped_source_db`.`old_table` TO `unmapped_db`.`new_table`",
			schema:        "mapped_source_db",
			table:         "old_table",
			expectedQuery: "RENAME TABLE `mapped_target_db`.`old_table` TO `unmapped_db`.`new_table`",
			description:   "Tests RENAME TABLE ordering: old table is mapped, new table is unmapped",
		},
		{
			name:          "RENAME TABLE - unmapped old, mapped new",
			query:         "RENAME TABLE `unmapped_db`.`old_table` TO `mapped_source_db`.`new_table`",
			schema:        "unmapped_db",
			table:         "old_table",
			expectedQuery: "RENAME TABLE `unmapped_db`.`old_table` TO `mapped_target_db`.`new_table`",
			description:   "Tests RENAME TABLE ordering: old table is unmapped, new table is mapped",
		},
		{
			name:          "CREATE TABLE LIKE - mapped source, unmapped template",
			query:         "CREATE TABLE `mapped_source_db`.`new_table` LIKE `unmapped_db`.`template_table`",
			schema:        "mapped_source_db",
			table:         "new_table",
			expectedQuery: "CREATE TABLE `mapped_target_db`.`new_table` LIKE `unmapped_db`.`template_table`",
			description:   "Tests CREATE LIKE ordering: new table is mapped, template is unmapped",
		},
		{
			name:          "CREATE TABLE LIKE - unmapped source, mapped template",
			query:         "CREATE TABLE `unmapped_db`.`new_table` LIKE `mapped_source_db`.`template_table`",
			schema:        "unmapped_db",
			table:         "new_table",
			expectedQuery: "CREATE TABLE `unmapped_db`.`new_table` LIKE `mapped_target_db`.`template_table`",
			description:   "Tests CREATE LIKE ordering: new table is unmapped, template is mapped",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddlEvent := &commonEvent.DDLEvent{
				Query: tt.query,
				TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: tt.schema, Table: tt.table},
				},
			}

			result, err := RewriteDDLQueryWithRouting(router, ddlEvent, "test-changefeed")
			require.NoError(t, err)
			require.Equal(t, tt.expectedQuery, result.NewQuery,
				"ORDERING TEST FAILED for %s\n%s\nExpected: %s\nActual:   %s",
				tt.name, tt.description, tt.expectedQuery, result.NewQuery)
		})
	}
}

// TestRewriteDDLQueryWithComplexMultiTableRouting tests complex multi-table routing scenarios
func TestRewriteDDLQueryWithComplexMultiTableRouting(t *testing.T) {
	t.Parallel()

	// Test with full routing (schema + table name changes)
	tableRouter, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"source_db.source_table"},
			SchemaRule: "target_db",
			TableRule:  "target_table_routed",
		},
		{
			Matcher:    []string{"source_db.old_table"},
			SchemaRule: "target_db",
			TableRule:  "old_table_routed",
		},
		{
			Matcher:    []string{"source_db.new_table"},
			SchemaRule: "target_db",
			TableRule:  "new_table_routed",
		},
		{
			Matcher:    []string{"source_db.template_table"},
			SchemaRule: "target_db",
			TableRule:  "template_table_routed",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, tableRouter)

	tableRoutingTests := []struct {
		name          string
		query         string
		schema        string
		table         string
		expectedQuery string
		description   string
	}{
		{
			name:          "DROP TABLE - with table routing",
			query:         "DROP TABLE `source_db`.`source_table`",
			schema:        "source_db",
			table:         "source_table",
			expectedQuery: "DROP TABLE `target_db`.`target_table_routed`",
			description:   "Tests DROP TABLE with both schema and table routing",
		},
		{
			name:          "TRUNCATE TABLE - with table routing",
			query:         "TRUNCATE TABLE `source_db`.`source_table`",
			schema:        "source_db",
			table:         "source_table",
			expectedQuery: "TRUNCATE TABLE `target_db`.`target_table_routed`",
			description:   "Tests TRUNCATE TABLE with both schema and table routing",
		},
		{
			name:          "CREATE TABLE - with table routing",
			query:         "CREATE TABLE `source_db`.`source_table` (id INT PRIMARY KEY)",
			schema:        "source_db",
			table:         "source_table",
			expectedQuery: "CREATE TABLE `target_db`.`target_table_routed` (`id` INT PRIMARY KEY)",
			description:   "Tests CREATE TABLE with both schema and table routing",
		},
		{
			name:          "RENAME TABLE - with table routing",
			query:         "RENAME TABLE `source_db`.`old_table` TO `source_db`.`new_table`",
			schema:        "source_db",
			table:         "old_table",
			expectedQuery: "RENAME TABLE `target_db`.`old_table_routed` TO `target_db`.`new_table_routed`",
			description:   "Tests RENAME TABLE with both schema and table routing on both old and new tables",
		},
		{
			name:          "CREATE TABLE LIKE - with table routing",
			query:         "CREATE TABLE `source_db`.`new_table` LIKE `source_db`.`template_table`",
			schema:        "source_db",
			table:         "new_table",
			expectedQuery: "CREATE TABLE `target_db`.`new_table_routed` LIKE `target_db`.`template_table_routed`",
			description:   "Tests CREATE TABLE LIKE with both schema and table routing on both new and template tables",
		},
	}

	for _, tt := range tableRoutingTests {
		t.Run(tt.name, func(t *testing.T) {
			ddlEvent := &commonEvent.DDLEvent{
				Query: tt.query,
				TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: tt.schema, Table: tt.table},
				},
			}

			result, err := RewriteDDLQueryWithRouting(tableRouter, ddlEvent, "test-changefeed")
			require.NoError(t, err)
			require.Equal(t, tt.expectedQuery, result.NewQuery,
				"TABLE ROUTING TEST FAILED for %s\n%s\nExpected: %s\nActual:   %s",
				tt.name, tt.description, tt.expectedQuery, result.NewQuery)
		})
	}
}

// TestRewriteDDLQueryWithConsolidatedRouting tests scenarios where multiple schemas are consolidated
func TestRewriteDDLQueryWithConsolidatedRouting(t *testing.T) {
	t.Parallel()

	// Test scenario with multiple schemas consolidated to a single target
	// Scenario: db1.* -> consolidated, db2.* -> consolidated
	consolidatedRouter, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"db1.*"},
			SchemaRule: "consolidated",
			TableRule:  TablePlaceholder,
		},
		{
			Matcher:    []string{"db2.*"},
			SchemaRule: "consolidated",
			TableRule:  TablePlaceholder,
		},
	})
	require.NoError(t, err)

	t.Run("CREATE TABLE LIKE - multiple schemas consolidated to single target", func(t *testing.T) {
		// CREATE TABLE db1.t1 LIKE db2.t2
		// Both schemas map to the same target
		ddlEvent := &commonEvent.DDLEvent{
			Query: "CREATE TABLE `db1`.`t1` LIKE `db2`.`t2`",
			TableInfo: &common.TableInfo{
				TableName: common.TableName{Schema: "db1", Table: "t1"},
			},
		}

		result, err := RewriteDDLQueryWithRouting(consolidatedRouter, ddlEvent, "test-changefeed")
		require.NoError(t, err)

		expectedQuery := "CREATE TABLE `consolidated`.`t1` LIKE `consolidated`.`t2`"
		require.Equal(t, expectedQuery, result.NewQuery,
			"Multiple schemas consolidated to single target should route both tables to same target")
	})

	// Test scenarios with multiple schemas mapped to different targets
	// Scenario: db1.* -> target1, db2.* -> target2
	multiSchemaRouter, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"db1.*"},
			SchemaRule: "target1",
			TableRule:  TablePlaceholder,
		},
		{
			Matcher:    []string{"db2.*"},
			SchemaRule: "target2",
			TableRule:  TablePlaceholder,
		},
	})
	require.NoError(t, err)

	t.Run("CREATE TABLE LIKE - same table name different schemas routed differently", func(t *testing.T) {
		// CREATE TABLE db1.users LIKE db2.users
		// Both schemas are mapped but to different targets
		// This is a critical test case: same table name but different routing per schema
		ddlEvent := &commonEvent.DDLEvent{
			Query: "CREATE TABLE `db1`.`users` LIKE `db2`.`users`",
			TableInfo: &common.TableInfo{
				TableName: common.TableName{Schema: "db1", Table: "users"},
			},
		}

		result, err := RewriteDDLQueryWithRouting(multiSchemaRouter, ddlEvent, "test-changefeed")
		require.NoError(t, err)

		expectedQuery := "CREATE TABLE `target1`.`users` LIKE `target2`.`users`"
		require.Equal(t, expectedQuery, result.NewQuery,
			"Same table name in different schemas should route to their respective targets")
		require.True(t, result.WasRewritten, "Query should be marked as rewritten")
		require.Equal(t, "target1", result.TargetSchemaName, "TargetSchemaName should be set to first target schema")
	})

	t.Run("RENAME TABLE - different schemas with same table name routed differently", func(t *testing.T) {
		// RENAME TABLE db1.users TO db2.users
		// Moving a table from one schema to another, both with routing
		ddlEvent := &commonEvent.DDLEvent{
			Query: "RENAME TABLE `db1`.`users` TO `db2`.`users`",
			TableInfo: &common.TableInfo{
				TableName: common.TableName{Schema: "db1", Table: "users"},
			},
		}

		result, err := RewriteDDLQueryWithRouting(multiSchemaRouter, ddlEvent, "test-changefeed")
		require.NoError(t, err)

		expectedQuery := "RENAME TABLE `target1`.`users` TO `target2`.`users`"
		require.Equal(t, expectedQuery, result.NewQuery,
			"RENAME TABLE should apply different routing to each schema")
		require.True(t, result.WasRewritten)
		require.Equal(t, "target1", result.TargetSchemaName)
	})
}

// TestRewriteDDLQueryWithErrors tests error handling for malformed DDLs
func TestRewriteDDLQueryWithErrors(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []RoutingRuleConfig{
		{
			Matcher:    []string{"source_db.*"},
			SchemaRule: "target_db",
			TableRule:  TablePlaceholder,
		},
	})
	require.NoError(t, err)

	t.Run("Invalid SQL syntax", func(t *testing.T) {
		ddlEvent := &commonEvent.DDLEvent{
			Query: "CREATE TABLE this is not valid SQL",
			TableInfo: &common.TableInfo{
				TableName: common.TableName{Schema: "source_db", Table: "test_table"},
			},
		}

		result, err := RewriteDDLQueryWithRouting(router, ddlEvent, "test-changefeed")
		require.Error(t, err, "Should return error for invalid SQL syntax")
		require.Nil(t, result, "Result should be nil on error")
		require.Contains(t, err.Error(), "failed to parse DDL query", "Error should mention parsing failure")
	})

	t.Run("SQL with syntax error in table name", func(t *testing.T) {
		ddlEvent := &commonEvent.DDLEvent{
			Query: "CREATE TABLE `source_db`.`unclosed (id INT)",
			TableInfo: &common.TableInfo{
				TableName: common.TableName{Schema: "source_db", Table: "unclosed"},
			},
		}

		result, err := RewriteDDLQueryWithRouting(router, ddlEvent, "test-changefeed")
		require.Error(t, err, "Should return error for syntax error")
		require.Nil(t, result)
		require.Contains(t, err.Error(), "failed to parse DDL query", "Error should mention parsing failure")
	})

	t.Run("Empty TableInfo with database-level DDL that cannot be parsed", func(t *testing.T) {
		// This simulates a case where we can't extract tables from the DDL
		// but since it's database-level, it should still work (no tables to route)
		ddlEvent := &commonEvent.DDLEvent{
			Query:     "CREATE DATABASE `new_db`",
			TableInfo: nil, // Database-level DDL has no TableInfo
		}

		result, err := RewriteDDLQueryWithRouting(router, ddlEvent, "test-changefeed")
		require.NoError(t, err, "Database-level DDL with no tables should not error")
		require.False(t, result.WasRewritten, "Database DDL should not be rewritten")
		require.Equal(t, ddlEvent.Query, result.NewQuery, "Query should remain unchanged")
	})
}
