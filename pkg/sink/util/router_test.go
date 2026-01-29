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

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestValidateRoutingExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		expr    string
		wantErr bool
	}{
		// Valid expressions
		{"", false},
		{SchemaPlaceholder, false},
		{TablePlaceholder, false},
		{"prefix_" + SchemaPlaceholder, false},
		{SchemaPlaceholder + "_suffix", false},
		{"prefix_" + SchemaPlaceholder + "_suffix", false},
		{SchemaPlaceholder + "_" + TablePlaceholder, false},
		{"static_name", false},
		{"db_" + SchemaPlaceholder + "_" + TablePlaceholder + "_v2", false},

		// Invalid expressions
		{"{invalid}", true},
		{"{Schema}", true}, // case sensitive
		{"{TABLE}", true},  // case sensitive
		{"{db}", true},
		{"{", true},
		{"}", true},
		{"{schema", true},
		{"schema}", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			err := config.ValidateRoutingExpression(tt.expr)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSubstituteExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		expr         string
		sourceSchema string
		sourceTable  string
		defaultValue string
		expected     string
	}{
		{SchemaPlaceholder, "mydb", "mytable", "mydb", "mydb"},
		{TablePlaceholder, "mydb", "mytable", "mytable", "mytable"},
		{SchemaPlaceholder + "_" + TablePlaceholder, "mydb", "mytable", "mydb", "mydb_mytable"},
		{"prefix_" + SchemaPlaceholder, "mydb", "mytable", "mydb", "prefix_mydb"},
		{TablePlaceholder + "_suffix", "mydb", "mytable", "mytable", "mytable_suffix"},
		{"static_name", "mydb", "mytable", "mydb", "static_name"},
		{"", "mydb", "mytable", "mydb", "mydb"},       // empty schema expr defaults to sourceSchema
		{"", "mydb", "mytable", "mytable", "mytable"}, // empty table expr defaults to sourceTable
		{"db_" + SchemaPlaceholder + "_" + TablePlaceholder + "_v2", "prod", "users", "prod", "db_prod_users_v2"},
	}

	for _, tt := range tests {
		t.Run(tt.expr+"_default_"+tt.defaultValue, func(t *testing.T) {
			result := substituteExpression(tt.expr, tt.sourceSchema, tt.sourceTable, tt.defaultValue)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNewRouter(t *testing.T) {
	t.Parallel()

	// Empty rules returns nil router
	router, err := NewRouter(true, nil)
	require.NoError(t, err)
	require.Nil(t, router)

	router, err = NewRouter(true, []RoutingRuleConfig{})
	require.NoError(t, err)
	require.Nil(t, router)

	// Rules with empty schema and table rules are skipped
	router, err = NewRouter(true, []RoutingRuleConfig{
		{Matcher: []string{"db1.*"}, SchemaRule: "", TableRule: ""},
	})
	require.NoError(t, err)
	require.Nil(t, router)

	// Valid rules
	router, err = NewRouter(true, []RoutingRuleConfig{
		{Matcher: []string{"db1.*"}, SchemaRule: "target_db", TableRule: TablePlaceholder},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// Invalid schema rule
	_, err = NewRouter(true, []RoutingRuleConfig{
		{Matcher: []string{"db1.*"}, SchemaRule: "{invalid}", TableRule: TablePlaceholder},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid schema rule")

	// Invalid table rule
	_, err = NewRouter(true, []RoutingRuleConfig{
		{Matcher: []string{"db1.*"}, SchemaRule: SchemaPlaceholder, TableRule: "{bad}"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid table rule")
}

func TestRouterRoute(t *testing.T) {
	t.Parallel()

	// Nil router returns source unchanged
	var nilRouter *Router
	schema, table := nilRouter.Route("mydb", "mytable")
	require.Equal(t, "mydb", schema)
	require.Equal(t, "mytable", table)

	// Router with rules
	router, err := NewRouter(true, []RoutingRuleConfig{
		// Route all tables in db1 to target_db
		{Matcher: []string{"db1.*"}, SchemaRule: "target_db", TableRule: TablePlaceholder},
		// Route specific table to a different name
		{Matcher: []string{"db2.users"}, SchemaRule: SchemaPlaceholder, TableRule: "customers"},
		// Route with combined expression
		{Matcher: []string{"staging.*"}, SchemaRule: "prod", TableRule: SchemaPlaceholder + "_" + TablePlaceholder},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	tests := []struct {
		sourceSchema   string
		sourceTable    string
		expectedSchema string
		expectedTable  string
	}{
		// Matches first rule
		{"db1", "orders", "target_db", "orders"},
		{"db1", "products", "target_db", "products"},
		// Matches second rule
		{"db2", "users", "db2", "customers"},
		// Matches third rule
		{"staging", "events", "prod", "staging_events"},
		// No match - returns source unchanged
		{"db2", "orders", "db2", "orders"},
		{"other", "table", "other", "table"},
	}

	for _, tt := range tests {
		t.Run(tt.sourceSchema+"."+tt.sourceTable, func(t *testing.T) {
			schema, table := router.Route(tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.expectedSchema, schema)
			require.Equal(t, tt.expectedTable, table)
		})
	}
}

func TestRouterCaseInsensitive(t *testing.T) {
	t.Parallel()

	// Case-insensitive router
	router, err := NewRouter(false, []RoutingRuleConfig{
		{Matcher: []string{"db1.*"}, SchemaRule: "target", TableRule: TablePlaceholder},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should match regardless of case
	schema, table := router.Route("DB1", "MyTable")
	require.Equal(t, "target", schema)
	require.Equal(t, "MyTable", table) // {table} substitutes to the original table name
}

func TestRouterFirstMatchWins(t *testing.T) {
	t.Parallel()

	t.Run("specific_before_wildcard", func(t *testing.T) {
		t.Parallel()

		// First matching rule wins
		router, err := NewRouter(true, []RoutingRuleConfig{
			{Matcher: []string{"db1.specific"}, SchemaRule: "first", TableRule: TablePlaceholder},
			{Matcher: []string{"db1.*"}, SchemaRule: "second", TableRule: TablePlaceholder},
		})
		require.NoError(t, err)

		// Specific match uses first rule
		schema, _ := router.Route("db1", "specific")
		require.Equal(t, "first", schema)

		// General match uses second rule
		schema, _ = router.Route("db1", "other")
		require.Equal(t, "second", schema)
	})

	// Test overlapping rules where multiple rules could match the same table.
	// The first matching rule in configuration order wins.
	t.Run("overlapping_rules", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(true, []RoutingRuleConfig{
			// Rule 1: Specific table match
			{Matcher: []string{"db1.users"}, SchemaRule: "target_specific", TableRule: "users_v1"},
			// Rule 2: All tables in db1 (overlaps with Rule 1 for db1.users)
			{Matcher: []string{"db1.*"}, SchemaRule: "target_db1", TableRule: TablePlaceholder},
			// Rule 3: Wildcard match (overlaps with Rules 1 and 2)
			{Matcher: []string{"*.*"}, SchemaRule: "target_all", TableRule: TablePlaceholder},
		})
		require.NoError(t, err)
		require.NotNil(t, router)

		tests := []struct {
			name         string
			sourceSchema string
			sourceTable  string
			wantSchema   string
			wantTable    string
		}{
			// db1.users matches Rule 1 (most specific), not Rule 2 or 3
			{"specific_table", "db1", "users", "target_specific", "users_v1"},
			// db1.orders matches Rule 2 (db1.*), not Rule 3
			{"schema_wildcard", "db1", "orders", "target_db1", "orders"},
			// db2.products only matches Rule 3 (*.*)"
			{"global_wildcard", "db2", "products", "target_all", "products"},
		}

		for _, tt := range tests {
			gotSchema, gotTable := router.Route(tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.wantSchema, gotSchema, "schema mismatch for %s", tt.name)
			require.Equal(t, tt.wantTable, gotTable, "table mismatch for %s", tt.name)
		}
	})

	// Verify that rule order in configuration determines which rule wins,
	// not specificity. If a wildcard rule comes first, it wins.
	t.Run("order_matters_not_specificity", func(t *testing.T) {
		t.Parallel()

		// Intentionally put the wildcard rule FIRST - it should win for all matches
		router, err := NewRouter(true, []RoutingRuleConfig{
			// Rule 1: Wildcard - catches everything
			{Matcher: []string{"*.*"}, SchemaRule: "catch_all", TableRule: TablePlaceholder},
			// Rule 2: More specific, but comes after wildcard
			{Matcher: []string{"db1.*"}, SchemaRule: "should_not_match", TableRule: TablePlaceholder},
			// Rule 3: Even more specific, but comes after wildcard
			{Matcher: []string{"db1.users"}, SchemaRule: "also_should_not_match", TableRule: "renamed"},
		})
		require.NoError(t, err)
		require.NotNil(t, router)

		// All should match the first rule (wildcard) because it comes first
		tests := []struct {
			sourceSchema string
			sourceTable  string
		}{
			{"db1", "users"},    // Would match all 3 rules, but first wins
			{"db1", "orders"},   // Would match rules 1 and 2, but first wins
			{"db2", "products"}, // Only matches rule 1
		}

		for _, tt := range tests {
			gotSchema, gotTable := router.Route(tt.sourceSchema, tt.sourceTable)
			require.Equal(t, "catch_all", gotSchema,
				"first matching rule should win for %s.%s", tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.sourceTable, gotTable,
				"first matching rule should win for %s.%s", tt.sourceSchema, tt.sourceTable)
		}
	})
}

func TestRouterSchemaOnly(t *testing.T) {
	t.Parallel()

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
		name         string
		sourceSchema string
		sourceTable  string
		wantSchema   string
		wantTable    string
	}{
		{
			name:         "matching table",
			sourceSchema: "source_db",
			sourceTable:  "users",
			wantSchema:   "target_db",
			wantTable:    "users",
		},
		{
			name:         "another matching table",
			sourceSchema: "source_db",
			sourceTable:  "orders",
			wantSchema:   "target_db",
			wantTable:    "orders",
		},
		{
			name:         "non-matching schema",
			sourceSchema: "other_db",
			sourceTable:  "users",
			wantSchema:   "other_db",
			wantTable:    "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotSchema, gotTable := router.Route(tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.wantSchema, gotSchema)
			require.Equal(t, tt.wantTable, gotTable)
		})
	}
}

func TestRouterTableOnly(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"mydb.source_table"},
			SchemaRule: SchemaPlaceholder,
			TableRule:  "target_table",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	tests := []struct {
		name         string
		sourceSchema string
		sourceTable  string
		wantSchema   string
		wantTable    string
	}{
		{
			name:         "matching table",
			sourceSchema: "mydb",
			sourceTable:  "source_table",
			wantSchema:   "mydb",
			wantTable:    "target_table",
		},
		{
			name:         "non-matching table",
			sourceSchema: "mydb",
			sourceTable:  "other_table",
			wantSchema:   "mydb",
			wantTable:    "other_table",
		},
		{
			name:         "non-matching schema",
			sourceSchema: "otherdb",
			sourceTable:  "source_table",
			wantSchema:   "otherdb",
			wantTable:    "source_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotSchema, gotTable := router.Route(tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.wantSchema, gotSchema)
			require.Equal(t, tt.wantTable, gotTable)
		})
	}
}

func TestRouterSchemaAndTable(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"src_db.src_table"},
			SchemaRule: "dst_db",
			TableRule:  "dst_table",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// Matching
	schema, table := router.Route("src_db", "src_table")
	require.Equal(t, "dst_db", schema)
	require.Equal(t, "dst_table", table)

	// Non-matching
	schema, table = router.Route("src_db", "other_table")
	require.Equal(t, "src_db", schema)
	require.Equal(t, "other_table", table)
}

func TestRouterWithTransformation(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"prod.*"},
			SchemaRule: "backup_" + SchemaPlaceholder,
			TableRule:  TablePlaceholder + "_archive",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	schema, table := router.Route("prod", "users")
	require.Equal(t, "backup_prod", schema)
	require.Equal(t, "users_archive", table)
}

func TestRouterMultipleRules(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []RoutingRuleConfig{
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
		{
			Matcher:    []string{"db3.specific_table"},
			SchemaRule: "target3",
			TableRule:  "renamed_table",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	tests := []struct {
		sourceSchema string
		sourceTable  string
		wantSchema   string
		wantTable    string
	}{
		{"db1", "table1", "target1", "table1"},
		{"db1", "table2", "target1", "table2"},
		{"db2", "table1", "target2", "table1"},
		{"db3", "specific_table", "target3", "renamed_table"},
		{"db3", "other_table", "db3", "other_table"}, // No match
		{"db4", "table1", "db4", "table1"},           // No match
	}

	for _, tt := range tests {
		gotSchema, gotTable := router.Route(tt.sourceSchema, tt.sourceTable)
		require.Equal(t, tt.wantSchema, gotSchema, "schema mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
		require.Equal(t, tt.wantTable, gotTable, "table mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
	}
}

func TestRouterCaseSensitiveSchema(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"MyDB.*"},
			SchemaRule: "target_db",
			TableRule:  TablePlaceholder,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should only match exact case
	schema, table := router.Route("MyDB", "table1")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "table1", table)

	// Should not match different case
	schema, table = router.Route("mydb", "table2")
	require.Equal(t, "mydb", schema)
	require.Equal(t, "table2", table)

	schema, table = router.Route("MYDB", "table3")
	require.Equal(t, "MYDB", schema)
	require.Equal(t, "table3", table)
}

func TestRouterCaseSensitiveTable(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"mydb.MyTable"},
			SchemaRule: "target_db",
			TableRule:  "target_table",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should only match exact table case
	schema, table := router.Route("mydb", "MyTable")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "target_table", table)

	// Should not match different table case
	schema, table = router.Route("mydb", "mytable")
	require.Equal(t, "mydb", schema)
	require.Equal(t, "mytable", table)

	schema, table = router.Route("mydb", "MYTABLE")
	require.Equal(t, "mydb", schema)
	require.Equal(t, "MYTABLE", table)
}

func TestRouterCaseSensitiveBoth(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"MyDB.MyTable"},
			SchemaRule: "target_db",
			TableRule:  "target_table",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should only match exact case for both schema and table
	tests := []struct {
		sourceSchema string
		sourceTable  string
		shouldMatch  bool
	}{
		{"MyDB", "MyTable", true},  // Exact match
		{"mydb", "mytable", false}, // Both wrong case
		{"MYDB", "MYTABLE", false}, // Both wrong case
		{"MyDB", "mytable", false}, // Table wrong case
		{"mydb", "MyTable", false}, // Schema wrong case
	}

	for _, tt := range tests {
		schema, table := router.Route(tt.sourceSchema, tt.sourceTable)
		if tt.shouldMatch {
			require.Equal(t, "target_db", schema, "schema mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
			require.Equal(t, "target_table", table, "table mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
		} else {
			require.Equal(t, tt.sourceSchema, schema, "should pass through for %s.%s", tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.sourceTable, table, "should pass through for %s.%s", tt.sourceSchema, tt.sourceTable)
		}
	}
}

func TestRouterCaseInsensitiveSchema(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []RoutingRuleConfig{
		{
			Matcher:    []string{"MyDB.*"},
			SchemaRule: "target_db",
			TableRule:  TablePlaceholder,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should match regardless of schema case
	schema, table := router.Route("mydb", "table1")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "table1", table)

	schema, table = router.Route("MYDB", "table2")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "table2", table)

	schema, table = router.Route("MyDb", "table3")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "table3", table)
}

func TestRouterCaseInsensitiveTable(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []RoutingRuleConfig{
		{
			Matcher:    []string{"mydb.MyTable"},
			SchemaRule: "target_db",
			TableRule:  "target_table",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should match regardless of table case
	schema, table := router.Route("mydb", "mytable")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "target_table", table)

	schema, table = router.Route("mydb", "MYTABLE")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "target_table", table)

	schema, table = router.Route("mydb", "MyTable")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "target_table", table)

	// Non-matching table should pass through
	schema, table = router.Route("mydb", "other_table")
	require.Equal(t, "mydb", schema)
	require.Equal(t, "other_table", table)
}

func TestRouterCaseInsensitiveBoth(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []RoutingRuleConfig{
		{
			Matcher:    []string{"MyDB.MyTable"},
			SchemaRule: "target_db",
			TableRule:  "target_table",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should match regardless of case for both schema and table
	tests := []struct {
		sourceSchema string
		sourceTable  string
		shouldMatch  bool
	}{
		{"MyDB", "MyTable", true},
		{"mydb", "mytable", true},
		{"MYDB", "MYTABLE", true},
		{"Mydb", "Mytable", true},
		{"MyDB", "other", false},
		{"other", "MyTable", false},
	}

	for _, tt := range tests {
		schema, table := router.Route(tt.sourceSchema, tt.sourceTable)
		if tt.shouldMatch {
			require.Equal(t, "target_db", schema, "schema mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
			require.Equal(t, "target_table", table, "table mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
		} else {
			require.Equal(t, tt.sourceSchema, schema, "should pass through for %s.%s", tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.sourceTable, table, "should pass through for %s.%s", tt.sourceSchema, tt.sourceTable)
		}
	}
}

func TestRouterCaseInsensitiveWithSchemaPlaceholder(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []RoutingRuleConfig{
		{
			Matcher:    []string{"MyDB.*"},
			SchemaRule: "backup_" + SchemaPlaceholder,
			TableRule:  TablePlaceholder,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// "mydb" matches "MyDB.*" case-insensitively, but output preserves actual source case
	schema, table := router.Route("mydb", "t1")
	require.Equal(t, "backup_mydb", schema) // NOT "backup_MyDB"
	require.Equal(t, "t1", table)
}

func TestRouterWildcardMatchers(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"*.*"},
			SchemaRule: "all_to_one",
			TableRule:  TablePlaceholder,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	// All tables should be routed
	schema, table := router.Route("any_db", "any_table")
	require.Equal(t, "all_to_one", schema)
	require.Equal(t, "any_table", table)

	schema, table = router.Route("other_db", "other_table")
	require.Equal(t, "all_to_one", schema)
	require.Equal(t, "other_table", table)
}

func TestNewRouterInvalidSchemaExpression(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"db.*"},
			SchemaRule: "{invalid}",
			TableRule:  TablePlaceholder,
		},
	})
	require.Error(t, err)
	require.Nil(t, router)
	require.Contains(t, err.Error(), "invalid schema rule")
}

func TestNewRouterInvalidTableExpression(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"db.*"},
			SchemaRule: SchemaPlaceholder,
			TableRule:  "{bad}",
		},
	})
	require.Error(t, err)
	require.Nil(t, router)
	require.Contains(t, err.Error(), "invalid table rule")
}

func TestNewRouterInvalidMatcher(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []RoutingRuleConfig{
		{
			Matcher:    []string{"[invalid"},
			SchemaRule: "target",
			TableRule:  TablePlaceholder,
		},
	})
	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewRouterFromDispatchRulesNilConfig(t *testing.T) {
	t.Parallel()

	router, err := NewRouterFromDispatchRules(true, nil)
	require.NoError(t, err)
	require.Nil(t, router)
}

func TestNewRouterFromDispatchRulesNoRoutingRules(t *testing.T) {
	t.Parallel()

	// Config with dispatch rules but no schema/table routing
	rules := []*config.DispatchRule{
		{
			Matcher:        []string{"test.*"},
			DispatcherRule: "ts",
			// No SchemaRule or TableRule
		},
	}
	router, err := NewRouterFromDispatchRules(true, rules)
	require.NoError(t, err)
	require.Nil(t, router, "router should be nil when no routing rules configured")
}

func TestNewRouterFromDispatchRulesWithSchemaRouting(t *testing.T) {
	t.Parallel()

	rules := []*config.DispatchRule{
		{
			Matcher:    []string{"source_db.*"},
			SchemaRule: "target_db",
			TableRule:  TablePlaceholder,
		},
	}
	router, err := NewRouterFromDispatchRules(true, rules)
	require.NoError(t, err)
	require.NotNil(t, router)
}

func TestNewRouterFromDispatchRulesWithTableRouting(t *testing.T) {
	t.Parallel()

	rules := []*config.DispatchRule{
		{
			Matcher:    []string{"db.source_table"},
			SchemaRule: SchemaPlaceholder,
			TableRule:  "target_table",
		},
	}
	router, err := NewRouterFromDispatchRules(true, rules)
	require.NoError(t, err)
	require.NotNil(t, router)
}

func TestNewRouterFromDispatchRulesMixedRulesWithAndWithoutRouting(t *testing.T) {
	t.Parallel()

	// Some dispatch rules have routing, some don't
	rules := []*config.DispatchRule{
		{
			Matcher:        []string{"db1.*"},
			DispatcherRule: "ts",
			// No routing - just partition dispatch
		},
		{
			Matcher:    []string{"db2.*"},
			SchemaRule: "routed_db",
			TableRule:  TablePlaceholder,
		},
		{
			Matcher:        []string{"db3.*"},
			DispatcherRule: "rowid",
			// No routing
		},
	}
	router, err := NewRouterFromDispatchRules(true, rules)
	require.NoError(t, err)
	require.NotNil(t, router, "router should exist because db2 has routing")

	// db1 has no routing rule - should pass through
	schema, table := router.Route("db1", "table1")
	require.Equal(t, "db1", schema)
	require.Equal(t, "table1", table)

	// db2 has routing - should be routed
	schema, table = router.Route("db2", "table1")
	require.Equal(t, "routed_db", schema)
	require.Equal(t, "table1", table)

	// db3 has no routing rule - should pass through
	schema, table = router.Route("db3", "table1")
	require.Equal(t, "db3", schema)
	require.Equal(t, "table1", table)
}
