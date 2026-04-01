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

	t.Run("nil or empty rules return nil router", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(true, nil)
		require.NoError(t, err)
		require.Nil(t, router)

		router, err = NewRouter(true, []*config.DispatchRule{})
		require.NoError(t, err)
		require.Nil(t, router)
	})

	t.Run("rules without routing targets are skipped", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(true, []*config.DispatchRule{
			{Matcher: []string{"db1.*"}},
		})
		require.NoError(t, err)
		require.Nil(t, router)
	})

	t.Run("invalid matcher returns error", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(true, []*config.DispatchRule{
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
		require.Nil(t, router)
	})

	t.Run("builds router from rules with target fields and ignores pure dispatch rules", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(true, []*config.DispatchRule{
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
		require.NotNil(t, router)

		schema, table := router.Route("db1", "orders")
		require.Equal(t, "db1", schema)
		require.Equal(t, "orders", table)

		schema, table = router.Route("db2", "orders")
		require.Equal(t, "archive", schema)
		require.Equal(t, "orders", table)

		schema, table = router.Route("db3", "users")
		require.Equal(t, "db3", schema)
		require.Equal(t, "users_bak", table)
	})
}

func TestRouterRoute(t *testing.T) {
	t.Parallel()

	var nilRouter *Router
	schema, table := nilRouter.Route("source_db", "source_table")
	require.Equal(t, "source_db", schema)
	require.Equal(t, "source_table", table)

	router, err := NewRouter(true, []*config.DispatchRule{
		{Matcher: []string{"db1.specific"}, TargetSchema: "specific_db", TargetTable: "specific_table"},
		{Matcher: []string{"db1.*"}, TargetSchema: "db1_archive", TargetTable: TablePlaceholder},
		{Matcher: []string{"staging.*"}, TargetSchema: "prod", TargetTable: SchemaPlaceholder + "_" + TablePlaceholder},
		{Matcher: []string{"*.*"}, TargetSchema: "fallback", TargetTable: TablePlaceholder},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	testCases := []struct {
		name           string
		sourceSchema   string
		sourceTable    string
		expectedSchema string
		expectedTable  string
	}{
		{
			name:           "specific matcher rewrites both names",
			sourceSchema:   "db1",
			sourceTable:    "specific",
			expectedSchema: "specific_db",
			expectedTable:  "specific_table",
		},
		{
			name:           "schema wildcard rewrites schema only",
			sourceSchema:   "db1",
			sourceTable:    "orders",
			expectedSchema: "db1_archive",
			expectedTable:  "orders",
		},
		{
			name:           "placeholders use source names",
			sourceSchema:   "staging",
			sourceTable:    "events",
			expectedSchema: "prod",
			expectedTable:  "staging_events",
		},
		{
			name:           "fallback matcher applies when no prior rule matches",
			sourceSchema:   "db2",
			sourceTable:    "products",
			expectedSchema: "fallback",
			expectedTable:  "products",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotSchema, gotTable := router.Route(tc.sourceSchema, tc.sourceTable)
			require.Equal(t, tc.expectedSchema, gotSchema)
			require.Equal(t, tc.expectedTable, gotTable)
		})
	}
}

func TestRouterFirstMatchWins(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(true, []*config.DispatchRule{
		{Matcher: []string{"*.*"}, TargetSchema: "catch_all", TargetTable: TablePlaceholder},
		{Matcher: []string{"db1.*"}, TargetSchema: "db1_only", TargetTable: TablePlaceholder},
		{Matcher: []string{"db1.users"}, TargetSchema: "users_only", TargetTable: "users_bak"},
	})
	require.NoError(t, err)
	require.NotNil(t, router)

	schema, table := router.Route("db1", "users")
	require.Equal(t, "catch_all", schema)
	require.Equal(t, "users", table)
}

func TestRouterCaseSensitivity(t *testing.T) {
	t.Parallel()

	t.Run("case sensitive router requires exact match", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(true, []*config.DispatchRule{
			{Matcher: []string{"MyDB.MyTable"}, TargetSchema: "target_db", TargetTable: "target_table"},
		})
		require.NoError(t, err)

		schema, table := router.Route("MyDB", "MyTable")
		require.Equal(t, "target_db", schema)
		require.Equal(t, "target_table", table)

		schema, table = router.Route("mydb", "mytable")
		require.Equal(t, "mydb", schema)
		require.Equal(t, "mytable", table)
	})

	t.Run("case insensitive router preserves source case in placeholders", func(t *testing.T) {
		t.Parallel()

		router, err := NewRouter(false, []*config.DispatchRule{
			{Matcher: []string{"MyDB.*"}, TargetSchema: "backup_" + SchemaPlaceholder, TargetTable: TablePlaceholder},
		})
		require.NoError(t, err)

		schema, table := router.Route("mydb", "MyTable")
		require.Equal(t, "backup_mydb", schema)
		require.Equal(t, "MyTable", table)
	})
}
