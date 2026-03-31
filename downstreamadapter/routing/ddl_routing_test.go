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

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestRewriteDDLQueryWithRouting(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test-changefeed")
	tests := []struct {
		name              string
		router            *Router
		ddl               *event.DDLEvent
		expectedChanged   bool
		expectedQuery     string
		requiredFragments []string
		forbiddenFragment string
	}{
		{
			name:            "no router keeps original query",
			ddl:             &event.DDLEvent{Query: "CREATE TABLE `source_db`.`test_table` (id INT PRIMARY KEY)", TableInfo: &common.TableInfo{TableName: common.TableName{Schema: "source_db", Table: "test_table"}}},
			expectedChanged: false,
			expectedQuery:   "CREATE TABLE `source_db`.`test_table` (id INT PRIMARY KEY)",
		},
		{
			name:            "empty query stays empty",
			ddl:             &event.DDLEvent{},
			expectedChanged: false,
			expectedQuery:   "",
		},
		{
			name: "no matched rule keeps original query",
			router: mustNewRouter(t, false, []*config.DispatchRule{{
				Matcher:      []string{"source_db.*"},
				TargetSchema: "target_db",
				TargetTable:  TablePlaceholder,
			}}),
			ddl: &event.DDLEvent{
				Query: "CREATE TABLE `other_db`.`test_table` (id INT PRIMARY KEY)",
				TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: "other_db", Table: "test_table"},
				},
			},
			expectedChanged: false,
			expectedQuery:   "CREATE TABLE `other_db`.`test_table` (id INT PRIMARY KEY)",
		},
		{
			name: "matched table ddl rewrites target table",
			router: mustNewRouter(t, false, []*config.DispatchRule{{
				Matcher:      []string{"source_db.*"},
				TargetSchema: "target_db",
				TargetTable:  "{table}_routed",
			}}),
			ddl: &event.DDLEvent{
				Query: "ALTER TABLE `source_db`.`test_table` ADD COLUMN c INT",
				TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: "source_db", Table: "test_table"},
				},
			},
			expectedChanged:   true,
			requiredFragments: []string{"`target_db`.`test_table_routed`"},
			forbiddenFragment: "`source_db`.`test_table`",
		},
		{
			name: "rename ddl rewrites both tables",
			router: mustNewRouter(t, false, []*config.DispatchRule{
				{
					Matcher:      []string{"db1.*"},
					TargetSchema: "target1",
					TargetTable:  TablePlaceholder,
				},
				{
					Matcher:      []string{"db2.*"},
					TargetSchema: "target2",
					TargetTable:  TablePlaceholder,
				},
			}),
			ddl: &event.DDLEvent{
				Query: "RENAME TABLE `db1`.`t1` TO `db2`.`t2`",
				TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: "db2", Table: "t2"},
				},
				MultipleTableInfos: []*common.TableInfo{
					{TableName: common.TableName{Schema: "db2", Table: "t2"}},
					{TableName: common.TableName{Schema: "db1", Table: "t1"}},
				},
			},
			expectedChanged:   true,
			requiredFragments: []string{"`target1`.`t1`", "`target2`.`t2`"},
		},
		{
			name: "database ddl rewrites schema",
			router: mustNewRouter(t, false, []*config.DispatchRule{{
				Matcher:      []string{"source_db.*"},
				TargetSchema: "target_db",
			}}),
			ddl: &event.DDLEvent{
				Query: "CREATE DATABASE `source_db`",
			},
			expectedChanged:   true,
			requiredFragments: []string{"`target_db`"},
			forbiddenFragment: "`source_db`",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			newQuery, changed, err := rewriteDDLQueryWithRouting(tc.router, tc.ddl, changefeedID)
			require.NoError(t, err)
			require.Equal(t, tc.expectedChanged, changed)
			if tc.expectedQuery != "" || tc.ddl.Query == "" {
				require.Equal(t, tc.expectedQuery, newQuery)
			}
			for _, fragment := range tc.requiredFragments {
				require.Contains(t, newQuery, fragment)
			}
			if tc.forbiddenFragment != "" {
				require.NotContains(t, newQuery, tc.forbiddenFragment)
			}
		})
	}
}

func TestRewriteDDLQueryWithRoutingReturnsTypedParseError(t *testing.T) {
	t.Parallel()

	router := mustNewRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  TablePlaceholder,
	}})

	ddl := &event.DDLEvent{Query: "INVALID DDL"}

	_, _, err := rewriteDDLQueryWithRouting(
		router, ddl, common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test-changefeed"),
	)
	require.Error(t, err)
	code, ok := errors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, errors.ErrTableRoutingFailed.RFCCode(), code)
}

func mustNewRouter(t *testing.T, caseSensitive bool, rules []*config.DispatchRule) *Router {
	t.Helper()

	router, err := NewRouter(caseSensitive, rules)
	require.NoError(t, err)
	return router
}
