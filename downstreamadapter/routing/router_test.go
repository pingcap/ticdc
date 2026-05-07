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

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestSubstituteExpression(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		expr         string
		sourceSchema string
		sourceTable  string
		defaultValue string
		expected     string
	}{
		{
			name:         "empty expression falls back to default",
			expr:         "",
			sourceSchema: "mydb",
			sourceTable:  "mytable",
			defaultValue: "mydb",
			expected:     "mydb",
		},
		{
			name:         "schema placeholder",
			expr:         SchemaPlaceholder,
			sourceSchema: "mydb",
			sourceTable:  "mytable",
			defaultValue: "unused",
			expected:     "mydb",
		},
		{
			name:         "table placeholder",
			expr:         TablePlaceholder + "_bak",
			sourceSchema: "mydb",
			sourceTable:  "mytable",
			defaultValue: "unused",
			expected:     "mytable_bak",
		},
		{
			name:         "literal with both placeholders",
			expr:         "db_" + SchemaPlaceholder + "_" + TablePlaceholder + "_v2",
			sourceSchema: "prod",
			sourceTable:  "users",
			defaultValue: "unused",
			expected:     "db_prod_users_v2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := substituteExpression(tc.expr, tc.sourceSchema, tc.sourceTable, tc.defaultValue)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestNewRouter(t *testing.T) {
	t.Parallel()

	t.Run("nil or empty rules return zero value router", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(newTestChangefeedID(), true, nil)
		require.NoError(t, err)
		require.Empty(t, router.rules)
		schema, table, changed, err := router.route("db1", "t1")
		require.NoError(t, err)
		require.Equal(t, "db1", schema)
		require.Equal(t, "t1", table)
		require.False(t, changed)

		router, err = NewRouter(newTestChangefeedID(), true, []*config.DispatchRule{})
		require.NoError(t, err)
		require.Empty(t, router.rules)
		schema, table, changed, err = router.route("db1", "t1")
		require.NoError(t, err)
		require.Equal(t, "db1", schema)
		require.Equal(t, "t1", table)
		require.False(t, changed)
	})

	t.Run("rules without routing targets are skipped", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(newTestChangefeedID(), true, []*config.DispatchRule{
			{Matcher: []string{"db1.*"}},
		})
		require.NoError(t, err)
		require.Empty(t, router.rules)
	})

	t.Run("invalid matcher returns error", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(newTestChangefeedID(), true, []*config.DispatchRule{
			{
				Matcher:      []string{"[invalid"},
				TargetSchema: "target",
				TargetTable:  TablePlaceholder,
			},
		})
		require.Error(t, err)
		code, ok := errors.RFCCode(err)
		require.True(t, ok)
		require.Equal(t, errors.ErrInvalidTableRoutingRule.RFCCode(), code)
		require.Empty(t, router.rules)
	})

	t.Run("builds router from rules with target fields and ignores pure dispatch rules", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(newTestChangefeedID(), true, []*config.DispatchRule{
			{
				Matcher:        []string{"db1.*"},
				DispatcherRule: "ts",
			},
			{
				Matcher:      []string{"db2.*"},
				TargetSchema: "archive",
				TargetTable:  TablePlaceholder,
			},
			{
				Matcher:      []string{"db3.users"},
				TargetSchema: SchemaPlaceholder,
				TargetTable:  "users_bak",
			},
		})
		require.NoError(t, err)
		require.Len(t, router.rules, 2)

		schema, table, changed, err := router.route("db1", "orders")
		require.NoError(t, err)
		require.Equal(t, "db1", schema)
		require.Equal(t, "orders", table)
		require.False(t, changed)

		schema, table, changed, err = router.route("db2", "orders")
		require.NoError(t, err)
		require.Equal(t, "archive", schema)
		require.Equal(t, "orders", table)
		require.True(t, changed)

		schema, table, changed, err = router.route("db3", "users")
		require.NoError(t, err)
		require.Equal(t, "db3", schema)
		require.Equal(t, "users_bak", table)
		require.True(t, changed)
	})
}

func TestRouterRoute(t *testing.T) {
	t.Parallel()

	var zeroRouter Router
	schema, table, changed, err := zeroRouter.route("source_db", "source_table")
	require.NoError(t, err)
	require.Equal(t, "source_db", schema)
	require.Equal(t, "source_table", table)
	require.False(t, changed)

	router, err := NewRouter(newTestChangefeedID(), true, []*config.DispatchRule{
		{Matcher: []string{"db1.specific"}, TargetSchema: "specific_db", TargetTable: "specific_table"},
		{Matcher: []string{"db1.*"}, TargetSchema: "db1_archive", TargetTable: TablePlaceholder},
		{Matcher: []string{"staging.*"}, TargetSchema: "prod", TargetTable: SchemaPlaceholder + "_" + TablePlaceholder},
		{Matcher: []string{"*.*"}, TargetSchema: "fallback", TargetTable: TablePlaceholder},
	})
	require.NoError(t, err)
	require.Len(t, router.rules, 4)

	testCases := []struct {
		name            string
		sourceSchema    string
		sourceTable     string
		expectedSchema  string
		expectedTable   string
		expectedChanged bool
	}{
		{
			name:            "specific matcher rewrites both names",
			sourceSchema:    "db1",
			sourceTable:     "specific",
			expectedSchema:  "specific_db",
			expectedTable:   "specific_table",
			expectedChanged: true,
		},
		{
			name:            "schema wildcard rewrites schema only",
			sourceSchema:    "db1",
			sourceTable:     "orders",
			expectedSchema:  "db1_archive",
			expectedTable:   "orders",
			expectedChanged: true,
		},
		{
			name:            "placeholders use source names",
			sourceSchema:    "staging",
			sourceTable:     "events",
			expectedSchema:  "prod",
			expectedTable:   "staging_events",
			expectedChanged: true,
		},
		{
			name:            "fallback matcher applies when no prior rule matches",
			sourceSchema:    "db2",
			sourceTable:     "products",
			expectedSchema:  "fallback",
			expectedTable:   "products",
			expectedChanged: true,
		},
		{
			name:            "empty schema and table do not match any rule",
			sourceSchema:    "",
			sourceTable:     "",
			expectedSchema:  "",
			expectedTable:   "",
			expectedChanged: false,
		},
		{
			name:            "empty schema with table does not match any rule",
			sourceSchema:    "",
			sourceTable:     "products",
			expectedSchema:  "",
			expectedTable:   "products",
			expectedChanged: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotSchema, gotTable, gotChanged, err := router.route(tc.sourceSchema, tc.sourceTable)
			require.NoError(t, err)
			require.Equal(t, tc.expectedSchema, gotSchema)
			require.Equal(t, tc.expectedTable, gotTable)
			require.Equal(t, tc.expectedChanged, gotChanged)
		})
	}

	t.Run("schema only routing does not produce target table", func(t *testing.T) {
		router, err := NewRouter(newTestChangefeedID(), true, []*config.DispatchRule{
			{Matcher: []string{"db1.*"}, TargetSchema: "db1_archive", TargetTable: "should_not_apply"},
		})
		require.NoError(t, err)

		schema, table, changed, err := router.route("db1", "")
		require.NoError(t, err)
		require.Equal(t, "db1_archive", schema)
		require.Empty(t, table)
		require.True(t, changed)
	})

	t.Run("schema only routing allows table rules with same target schema", func(t *testing.T) {
		router, err := NewRouter(newTestChangefeedID(), true, []*config.DispatchRule{
			{Matcher: []string{"db1.orders"}, TargetSchema: "db1_archive", TargetTable: "orders_archive"},
			{Matcher: []string{"db1.users"}, TargetSchema: "db1_archive", TargetTable: "users_archive"},
		})
		require.NoError(t, err)

		schema, table, changed, err := router.route("db1", "")
		require.NoError(t, err)
		require.Equal(t, "db1_archive", schema)
		require.Empty(t, table)
		require.True(t, changed)
	})

	t.Run("schema only routing rejects table rules with different target schemas", func(t *testing.T) {
		router, err := NewRouter(newTestChangefeedID(), true, []*config.DispatchRule{
			{Matcher: []string{"db1.orders"}, TargetSchema: "orders_db", TargetTable: "orders"},
			{Matcher: []string{"db1.users"}, TargetSchema: "users_db", TargetTable: "users"},
		})
		require.NoError(t, err)

		_, _, _, err = router.route("db1", "")
		require.Error(t, err)
		require.True(t, errors.ErrTableRoutingFailed.Equal(err))
		require.Contains(t, err.Error(), "ambiguous schema routing")
	})

	t.Run("empty schema and table do not trigger routing", func(t *testing.T) {
		router, err := NewRouter(newTestChangefeedID(), true, []*config.DispatchRule{
			{Matcher: []string{"*.*"}, TargetSchema: "fallback", TargetTable: "should_not_apply"},
		})
		require.NoError(t, err)

		schema, table, changed, err := router.route("", "")
		require.NoError(t, err)
		require.Empty(t, schema)
		require.Empty(t, table)
		require.False(t, changed)
	})
}

func TestRouterFirstMatchWins(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(newTestChangefeedID(), true, []*config.DispatchRule{
		{Matcher: []string{"*.*"}, TargetSchema: "catch_all", TargetTable: TablePlaceholder},
		{Matcher: []string{"db1.*"}, TargetSchema: "db1_only", TargetTable: TablePlaceholder},
		{Matcher: []string{"db1.users"}, TargetSchema: "users_only", TargetTable: "users_bak"},
	})
	require.NoError(t, err)
	require.Len(t, router.rules, 3)

	schema, table, changed, err := router.route("db1", "users")
	require.NoError(t, err)
	require.Equal(t, "catch_all", schema)
	require.Equal(t, "users", table)
	require.True(t, changed)
}

func TestRouterCaseSensitivity(t *testing.T) {
	t.Parallel()

	t.Run("case sensitive router requires exact match", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(newTestChangefeedID(), true, []*config.DispatchRule{
			{Matcher: []string{"MyDB.MyTable"}, TargetSchema: "target_db", TargetTable: "target_table"},
		})
		require.NoError(t, err)

		schema, table, changed, err := router.route("MyDB", "MyTable")
		require.NoError(t, err)
		require.Equal(t, "target_db", schema)
		require.Equal(t, "target_table", table)
		require.True(t, changed)

		schema, table, changed, err = router.route("mydb", "mytable")
		require.NoError(t, err)
		require.Equal(t, "mydb", schema)
		require.Equal(t, "mytable", table)
		require.False(t, changed)
	})

	t.Run("case insensitive router preserves source case in placeholders", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(newTestChangefeedID(), false, []*config.DispatchRule{
			{Matcher: []string{"MyDB.*"}, TargetSchema: "backup_" + SchemaPlaceholder, TargetTable: TablePlaceholder},
		})
		require.NoError(t, err)

		schema, table, changed, err := router.route("mydb", "MyTable")
		require.NoError(t, err)
		require.Equal(t, "backup_mydb", schema)
		require.Equal(t, "MyTable", table)
		require.True(t, changed)
	})
}
