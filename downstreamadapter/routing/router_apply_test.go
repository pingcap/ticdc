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
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func newTestChangefeedID() common.ChangeFeedID {
	return common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test-changefeed")
}

func TestApplyToTableInfo(t *testing.T) {
	t.Parallel()

	tableInfo := &common.TableInfo{
		TableName: common.TableName{
			Schema:  "source_db",
			Table:   "source_table",
			TableID: 1,
		},
	}

	var zeroRouter Router
	require.Same(t, tableInfo, zeroRouter.ApplyToTableInfo(tableInfo))

	noOpRouter, err := NewRouter(newTestChangefeedID(), false, []*config.DispatchRule{
		{
			Matcher:      []string{"other_db.*"},
			TargetSchema: "target_db",
			TargetTable:  TablePlaceholder,
		},
	})
	require.NoError(t, err)
	require.Same(t, tableInfo, noOpRouter.ApplyToTableInfo(tableInfo))

	router, err := NewRouter(newTestChangefeedID(), false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.source_table"},
			TargetSchema: "target_db",
			TargetTable:  "target_table",
		},
	})
	require.NoError(t, err)

	routed := router.ApplyToTableInfo(tableInfo)
	require.NotSame(t, tableInfo, routed)
	require.Equal(t, "source_db", routed.GetSchemaName())
	require.Equal(t, "source_table", routed.GetTableName())
	require.Equal(t, "target_db", routed.GetTargetSchemaName())
	require.Equal(t, "target_table", routed.GetTargetTableName())
	require.Equal(t, "source_db", routed.TableName.Schema)
	require.Equal(t, "source_table", routed.TableName.Table)
	require.Equal(t, "target_db", routed.TableName.TargetSchema)
	require.Equal(t, "target_table", routed.TableName.TargetTable)

	require.Empty(t, tableInfo.TableName.TargetSchema)
	require.Empty(t, tableInfo.TableName.TargetTable)
}

func TestApplyToDDLEventReturnsOriginalWhenRoutingDoesNotChangeAnything(t *testing.T) {
	t.Parallel()

	ddl := &event.DDLEvent{
		Query:      "ALTER TABLE `source_db`.`source_table` ADD INDEX idx_id(id)",
		SchemaName: "source_db",
		TableName:  "source_table",
		TableInfo: &common.TableInfo{
			TableName: common.TableName{
				Schema:  "source_db",
				Table:   "source_table",
				TableID: 1,
			},
		},
	}

	router, err := NewRouter(newTestChangefeedID(), false, []*config.DispatchRule{{
		Matcher:      []string{"other_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "target_table",
	}})
	require.NoError(t, err)

	routed, err := router.ApplyToDDLEvent(ddl)
	require.NoError(t, err)
	require.Same(t, ddl, routed)
}

func TestApplyToDDLEventWithZeroRouterReturnsOriginal(t *testing.T) {
	t.Parallel()

	ddl := &event.DDLEvent{
		Query:      "ALTER TABLE `source_db`.`source_table` ADD INDEX idx_id(id)",
		SchemaName: "source_db",
		TableName:  "source_table",
		TableInfo: &common.TableInfo{
			TableName: common.TableName{
				Schema:  "source_db",
				Table:   "source_table",
				TableID: 1,
			},
		},
	}

	var zeroRouter Router
	routed, err := zeroRouter.ApplyToDDLEvent(ddl)
	require.NoError(t, err)
	require.Same(t, ddl, routed)
}

func TestApplyToDDLEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		router Router
		ddl    *event.DDLEvent
		check  func(t *testing.T, original, routed *event.DDLEvent)
	}{
		{
			name: "single table ddl",
			router: func() Router {
				router, err := NewRouter(newTestChangefeedID(), false, []*config.DispatchRule{{
					Matcher:      []string{"source_db.source_table"},
					TargetSchema: "target_db",
					TargetTable:  "target_table",
				}})
				require.NoError(t, err)
				return router
			}(),
			ddl: func() *event.DDLEvent {
				originalTableInfo := &common.TableInfo{
					TableName: common.TableName{
						Schema:  "source_db",
						Table:   "source_table",
						TableID: 1,
					},
				}
				return &event.DDLEvent{
					Query:      "ALTER TABLE `source_db`.`source_table` ADD INDEX idx_id(id)",
					SchemaName: "source_db",
					TableName:  "source_table",
					TableInfo:  originalTableInfo,
					BlockedTableNames: []event.SchemaTableName{
						{SchemaName: "source_db", TableName: "source_table"},
					},
				}
			}(),
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Contains(t, routed.Query, "`target_db`.`target_table`")
				require.Equal(t, "source_db", routed.GetSchemaName())
				require.Equal(t, "source_db", routed.SchemaName)
				require.Equal(t, "source_table", routed.TableName)
				require.Equal(t, "target_db", routed.GetTargetSchemaName())
				require.Equal(t, "target_table", routed.GetTargetTableName())
				require.NotSame(t, original.TableInfo, routed.TableInfo)
				require.Equal(t, "source_db", routed.TableInfo.GetSchemaName())
				require.Equal(t, "source_table", routed.TableInfo.GetTableName())
				require.Equal(t, "target_db", routed.TableInfo.GetTargetSchemaName())
				require.Equal(t, "target_table", routed.TableInfo.GetTargetTableName())
				require.Equal(t, "target_db", routed.TableInfo.TableName.TargetSchema)
				require.Equal(t, "target_table", routed.TableInfo.TableName.TargetTable)
				require.Equal(t, event.SchemaTableName{
					SchemaName: "target_db",
					TableName:  "target_table",
				}, routed.BlockedTableNames[0])

				require.Equal(t, "ALTER TABLE `source_db`.`source_table` ADD INDEX idx_id(id)", original.Query)
				require.Equal(t, "source_db", original.GetSchemaName())
				require.Empty(t, original.TableInfo.TableName.TargetSchema)
				require.Empty(t, original.TableInfo.TableName.TargetTable)
			},
		},
		{
			name: "rename ddl",
			router: func() Router {
				router, err := NewRouter(newTestChangefeedID(), false, []*config.DispatchRule{
					{
						Matcher:      []string{"old_db.*"},
						TargetSchema: "old_target_db",
						TargetTable:  "{table}_old",
					},
					{
						Matcher:      []string{"new_db.*"},
						TargetSchema: "new_target_db",
						TargetTable:  "{table}_new",
					},
				})
				require.NoError(t, err)
				return router
			}(),
			ddl: &event.DDLEvent{
				Query:           "RENAME TABLE `old_db`.`orders` TO `new_db`.`orders_archive`",
				SchemaName:      "new_db",
				TableName:       "orders_archive",
				ExtraSchemaName: "old_db",
				ExtraTableName:  "orders",
				TableNameChange: &event.TableNameChange{
					AddName: []event.SchemaTableName{{
						SchemaName: "new_db",
						TableName:  "orders_archive",
					}},
					DropName: []event.SchemaTableName{{
						SchemaName: "old_db",
						TableName:  "orders",
					}},
				},
			},
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Equal(t, "new_db", routed.SchemaName)
				require.Equal(t, "orders_archive", routed.TableName)
				require.Equal(t, "old_db", routed.ExtraSchemaName)
				require.Equal(t, "orders", routed.ExtraTableName)
				require.Equal(t, "new_target_db", routed.GetTargetSchemaName())
				require.Equal(t, "orders_archive_new", routed.GetTargetTableName())
				require.Equal(t, "old_target_db", routed.GetTargetExtraSchemaName())
				require.Equal(t, "orders_old", routed.GetTargetExtraTableName())
				require.Equal(t, event.SchemaTableName{
					SchemaName: "new_db",
					TableName:  "orders_archive",
				}, routed.TableNameChange.AddName[0])
				require.Equal(t, event.SchemaTableName{
					SchemaName: "old_db",
					TableName:  "orders",
				}, routed.TableNameChange.DropName[0])
				require.Contains(t, routed.Query, "`old_target_db`.`orders_old`")
				require.Contains(t, routed.Query, "`new_target_db`.`orders_archive_new`")
				require.Equal(t, "new_db", original.SchemaName)
				require.Equal(t, "orders_archive", original.TableName)
				require.Equal(t, "old_db", original.ExtraSchemaName)
				require.Equal(t, "orders", original.ExtraTableName)
			},
		},
		{
			name: "database ddl",
			router: func() Router {
				router, err := NewRouter(newTestChangefeedID(), false, []*config.DispatchRule{{
					Matcher:      []string{"source_db.*"},
					TargetSchema: "target_db",
				}})
				require.NoError(t, err)
				return router
			}(),
			ddl: &event.DDLEvent{
				Query:      "CREATE DATABASE `source_db`",
				SchemaName: "source_db",
			},
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Equal(t, "source_db", routed.SchemaName)
				require.Equal(t, "target_db", routed.GetTargetSchemaName())
				require.Equal(t, "source_db", routed.GetSchemaName())
				require.Equal(t, "source_db", original.SchemaName)
			},
		},
		{
			name: "multiple table infos only",
			router: func() Router {
				router, err := NewRouter(newTestChangefeedID(), false, []*config.DispatchRule{{
					Matcher:      []string{"source_db.source_table"},
					TargetSchema: "target_db",
					TargetTable:  "target_table",
				}})
				require.NoError(t, err)
				return router
			}(),
			ddl: &event.DDLEvent{
				MultipleTableInfos: []*common.TableInfo{{
					TableName: common.TableName{
						Schema:  "source_db",
						Table:   "source_table",
						TableID: 1,
					},
				}},
			},
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.True(t, &original.MultipleTableInfos[0] != &routed.MultipleTableInfos[0])
				require.NotSame(t, original.MultipleTableInfos[0], routed.MultipleTableInfos[0])
				require.Equal(t, "source_db", routed.MultipleTableInfos[0].GetSchemaName())
				require.Equal(t, "source_table", routed.MultipleTableInfos[0].GetTableName())
				require.Equal(t, "target_db", routed.MultipleTableInfos[0].GetTargetSchemaName())
				require.Equal(t, "target_table", routed.MultipleTableInfos[0].GetTargetTableName())
				require.Empty(t, original.MultipleTableInfos[0].TableName.TargetSchema)
				require.Empty(t, original.MultipleTableInfos[0].TableName.TargetTable)
			},
		},
		{
			name: "blocked table names only",
			router: func() Router {
				router, err := NewRouter(newTestChangefeedID(), false, []*config.DispatchRule{{
					Matcher:      []string{"source_db.source_table"},
					TargetSchema: "target_db",
					TargetTable:  "target_table",
				}})
				require.NoError(t, err)
				return router
			}(),
			ddl: &event.DDLEvent{
				BlockedTableNames: []event.SchemaTableName{{
					SchemaName: "source_db",
					TableName:  "source_table",
				}},
			},
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.True(t, &original.BlockedTableNames[0] != &routed.BlockedTableNames[0])
				require.Equal(t, event.SchemaTableName{
					SchemaName: "target_db",
					TableName:  "target_table",
				}, routed.BlockedTableNames[0])
				require.Equal(t, event.SchemaTableName{
					SchemaName: "source_db",
					TableName:  "source_table",
				}, original.BlockedTableNames[0])
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			routed, err := tc.router.ApplyToDDLEvent(tc.ddl)
			require.NoError(t, err)
			require.NotSame(t, tc.ddl, routed)
			tc.check(t, tc.ddl, routed)
		})
	}
}

func TestRewriteDDLQueryWithRouting(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		router            Router
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
		{
			name: "multi ddl keeps separator when only later query routes",
			router: mustNewRouter(t, false, []*config.DispatchRule{{
				Matcher:      []string{"source_db.*"},
				TargetSchema: "target_db",
				TargetTable:  "{table}_routed",
			}}),
			ddl: &event.DDLEvent{
				Query: "CREATE TABLE `t1` (`id` INT PRIMARY KEY);CREATE TABLE `t2` (`id` INT PRIMARY KEY);",
				MultipleTableInfos: []*common.TableInfo{
					{TableName: common.TableName{Schema: "other_db", Table: "t1"}},
					{TableName: common.TableName{Schema: "source_db", Table: "t2"}},
				},
			},
			expectedChanged: true,
			requiredFragments: []string{
				"CREATE TABLE `t1`",
				";CREATE TABLE `target_db`.`t2_routed`",
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			newQuery, err := tc.router.rewriteDDLQuery(tc.ddl)
			require.NoError(t, err)
			require.Equal(t, tc.expectedChanged, newQuery != tc.ddl.Query)
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

	_, err := router.rewriteDDLQuery(ddl)
	require.Error(t, err)
	code, ok := errors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, errors.ErrTableRoutingFailed.RFCCode(), code)
}

func TestApplyToDDLEventSkipsQueryRewriteWhenRoutingNotNeeded(t *testing.T) {
	t.Parallel()

	router := mustNewRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  TablePlaceholder,
	}})

	ddl := &event.DDLEvent{
		Type:       byte(timodel.ActionAddColumn),
		Query:      "INVALID DDL",
		SchemaName: "other_db",
		TableName:  "t1",
		TableInfo:  &common.TableInfo{TableName: common.TableName{Schema: "other_db", Table: "t1"}},
	}

	routed, err := router.ApplyToDDLEvent(ddl)
	require.NoError(t, err)
	require.Same(t, ddl, routed)
}

func TestApplyToDDLEventRewritesQueryOnlyTableReferences(t *testing.T) {
	t.Parallel()

	router := mustNewRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "{table}_routed",
	}})

	ddl := &event.DDLEvent{
		Type:       byte(timodel.ActionCreateView),
		Query:      "CREATE VIEW `other_db`.`v1` AS SELECT * FROM `source_db`.`orders`",
		SchemaName: "other_db",
		TableName:  "v1",
		TableInfo:  &common.TableInfo{TableName: common.TableName{Schema: "other_db", Table: "v1"}},
	}

	routed, err := router.ApplyToDDLEvent(ddl)
	require.NoError(t, err)
	require.NotSame(t, ddl, routed)
	require.Contains(t, routed.Query, "`other_db`.`v1`")
	require.Contains(t, routed.Query, "`target_db`.`orders_routed`")
	require.NotContains(t, routed.Query, "`source_db`.`orders`")
	require.Equal(t, "other_db", routed.GetTargetSchemaName())
	require.Equal(t, "v1", routed.GetTargetTableName())
}

func mustNewRouter(t *testing.T, caseSensitive bool, rules []*config.DispatchRule) Router {
	t.Helper()

	router, err := NewRouter(newTestChangefeedID(), caseSensitive, rules)
	require.NoError(t, err)
	return router
}
