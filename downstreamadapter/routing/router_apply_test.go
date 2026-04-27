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
	routed, err := zeroRouter.ApplyToTableInfo(tableInfo)
	require.NoError(t, err)
	require.Same(t, tableInfo, routed)

	noOpRouter, err := NewRouter(newTestChangefeedID(), false, []*config.DispatchRule{
		{
			Matcher:      []string{"other_db.*"},
			TargetSchema: "target_db",
			TargetTable:  TablePlaceholder,
		},
	})
	require.NoError(t, err)
	routed, err = noOpRouter.ApplyToTableInfo(tableInfo)
	require.NoError(t, err)
	require.Same(t, tableInfo, routed)

	router, err := NewRouter(newTestChangefeedID(), false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.source_table"},
			TargetSchema: "target_db",
			TargetTable:  "target_table",
		},
	})
	require.NoError(t, err)

	routed, err = router.ApplyToTableInfo(tableInfo)
	require.NoError(t, err)
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

func TestApplyToTableInfoReturnsAmbiguousSchemaRoutingError(t *testing.T) {
	t.Parallel()

	router := newTestRouter(t, false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.orders"},
			TargetSchema: "orders_db",
			TargetTable:  TablePlaceholder,
		},
		{
			Matcher:      []string{"source_db.users"},
			TargetSchema: "users_db",
			TargetTable:  TablePlaceholder,
		},
	})
	tableInfo := &common.TableInfo{
		TableName: common.TableName{
			Schema: "source_db",
		},
	}

	_, err := router.ApplyToTableInfo(tableInfo)
	require.Error(t, err)
	require.True(t, errors.ErrTableRoutingFailed.Equal(err))
	require.Contains(t, err.Error(), "ambiguous schema routing")
}

func TestApplyToDDLEventReturnsOriginalWhenRoutingDoesNotChangeAnything(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `source_db`")
	helper.DDL2Event("CREATE TABLE `source_db`.`source_table` (`id` INT PRIMARY KEY)")
	ddl := helper.DDL2Event("ALTER TABLE `source_db`.`source_table` ADD INDEX `idx_id`(`id`)")

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
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `source_db`")
	helper.DDL2Event("CREATE TABLE `source_db`.`source_table` (`id` INT PRIMARY KEY)")
	ddl := helper.DDL2Event("ALTER TABLE `source_db`.`source_table` ADD INDEX `idx_id`(`id`)")

	var zeroRouter Router
	routed, err := zeroRouter.ApplyToDDLEvent(ddl)
	require.NoError(t, err)
	require.Same(t, ddl, routed)
}

func TestApplyToDDLEvent(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `source_db`")
	helper.Tk().MustExec("CREATE DATABASE `old_db`")
	helper.Tk().MustExec("CREATE DATABASE `new_db`")
	helper.DDL2Event("CREATE TABLE `source_db`.`source_table` (`id` INT PRIMARY KEY)")
	singleTableDDL := helper.DDL2Event("ALTER TABLE `source_db`.`source_table` ADD INDEX `idx_id`(`id`)")
	helper.DDL2Event("CREATE TABLE `old_db`.`orders` (`id` INT PRIMARY KEY)")
	renameDDL := helper.DDL2Event("RENAME TABLE `old_db`.`orders` TO `new_db`.`orders_archive`")

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
			ddl: singleTableDDL,
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

				require.Contains(t, original.Query, "`source_db`.`source_table`")
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
			ddl: renameDDL,
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
				Query: "ALTER TABLE `source_db`.`source_table` ADD COLUMN `c1` INT",
				// This intentionally tests metadata-only routing; real DDL2Event
				// should also carry the matching DDL identity and query.
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
				Query: "ALTER TABLE `source_db`.`source_table` ADD INDEX `idx_id`(`id`)",
				// This intentionally tests metadata-only routing; real DDL2Event
				// should also carry the matching DDL identity and query.
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

func TestApplyToDDLEventRejectsAmbiguousSchemaRouting(t *testing.T) {
	router := newTestRouter(t, false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.orders"},
			TargetSchema: "orders_db",
			TargetTable:  TablePlaceholder,
		},
		{
			Matcher:      []string{"source_db.users"},
			TargetSchema: "users_db",
			TargetTable:  TablePlaceholder,
		},
	})

	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	ddl := helper.DDL2Event("CREATE DATABASE `source_db`")

	_, err := router.ApplyToDDLEvent(ddl)
	require.Error(t, err)
	require.True(t, errors.ErrTableRoutingFailed.Equal(err))
	require.Contains(t, err.Error(), "ambiguous schema routing")
}

func TestRewriteDDLQueryWithRouting(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `source_db`")
	helper.Tk().MustExec("CREATE DATABASE `other_db`")
	helper.Tk().MustExec("CREATE DATABASE `db1`")
	helper.Tk().MustExec("CREATE DATABASE `db2`")
	noRouterDDL := helper.DDL2Event("CREATE TABLE `source_db`.`test_table` (`id` INT PRIMARY KEY)")
	noMatchedDDL := helper.DDL2Event("CREATE TABLE `other_db`.`test_table` (`id` INT PRIMARY KEY)")
	matchedTableDDL := helper.DDL2Event("ALTER TABLE `source_db`.`test_table` ADD COLUMN `c` INT")
	databaseDDL := helper.DDL2Event("ALTER DATABASE `source_db` CHARACTER SET utf8mb4")
	helper.DDL2Event("CREATE TABLE `db1`.`t1` (`id` INT PRIMARY KEY)")
	renameDDL := helper.DDL2Event("RENAME TABLE `db1`.`t1` TO `db2`.`t2`")

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
			ddl:             noRouterDDL,
			expectedChanged: false,
			expectedQuery:   noRouterDDL.Query,
		},
		{
			name: "no matched rule keeps original query",
			router: newTestRouter(t, false, []*config.DispatchRule{{
				Matcher:      []string{"source_db.*"},
				TargetSchema: "target_db",
				TargetTable:  TablePlaceholder,
			}}),
			ddl:             noMatchedDDL,
			expectedChanged: false,
			expectedQuery:   noMatchedDDL.Query,
		},
		{
			name: "matched table ddl rewrites target table",
			router: newTestRouter(t, false, []*config.DispatchRule{{
				Matcher:      []string{"source_db.*"},
				TargetSchema: "target_db",
				TargetTable:  "{table}_routed",
			}}),
			ddl:               matchedTableDDL,
			expectedChanged:   true,
			requiredFragments: []string{"`target_db`.`test_table_routed`"},
			forbiddenFragment: "`source_db`.`test_table`",
		},
		{
			name: "rename ddl rewrites both tables",
			router: newTestRouter(t, false, []*config.DispatchRule{
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
			ddl:               renameDDL,
			expectedChanged:   true,
			requiredFragments: []string{"`target1`.`t1`", "`target2`.`t2`"},
		},
		{
			name: "database ddl rewrites schema",
			router: newTestRouter(t, false, []*config.DispatchRule{{
				Matcher:      []string{"source_db.*"},
				TargetSchema: "target_db",
			}}),
			ddl:               databaseDDL,
			expectedChanged:   true,
			requiredFragments: []string{"`target_db`"},
			forbiddenFragment: "`source_db`",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			newQuery, err := tc.router.rewriteParserBackedDDLQuery(tc.ddl)
			require.NoError(t, err)
			require.Equal(t, tc.expectedChanged, newQuery != tc.ddl.Query)
			if tc.expectedQuery != "" {
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

func TestApplyToDDLEventReturnsOriginalWhenQueryDoesNotRoute(t *testing.T) {
	router := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  TablePlaceholder,
	}})

	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `other_db`")
	helper.DDL2Event("CREATE TABLE `other_db`.`t1` (`id` INT PRIMARY KEY)")
	ddl := helper.DDL2Event("ALTER TABLE `other_db`.`t1` ADD COLUMN `c1` INT")

	routed, err := router.ApplyToDDLEvent(ddl)
	require.NoError(t, err)
	require.Same(t, ddl, routed)
}

func TestApplyToDDLEventRewritesQueryOnlyTableReferences(t *testing.T) {
	router := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "{table}_routed",
	}})

	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `source_db`")
	helper.Tk().MustExec("CREATE DATABASE `other_db`")
	helper.DDL2Event("CREATE TABLE `source_db`.`orders` (`id` INT PRIMARY KEY)")
	ddl := helper.DDL2Event("CREATE VIEW `other_db`.`v1` AS SELECT * FROM `source_db`.`orders`")

	routed, err := router.ApplyToDDLEvent(ddl)
	require.NoError(t, err)
	require.NotSame(t, ddl, routed)
	require.Contains(t, routed.Query, "`other_db`.`v1`")
	require.Contains(t, routed.Query, "`target_db`.`orders_routed`")
	require.NotContains(t, routed.Query, "`source_db`.`orders`")
	require.Equal(t, "other_db", routed.GetTargetSchemaName())
	require.Equal(t, "v1", routed.GetTargetTableName())
}

func newTestRouter(t *testing.T, caseSensitive bool, rules []*config.DispatchRule) Router {
	t.Helper()

	router, err := NewRouter(newTestChangefeedID(), caseSensitive, rules)
	require.NoError(t, err)
	return router
}

func TestRewriteParserBackedDDLQueryError(t *testing.T) {
	t.Parallel()

	router := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  TablePlaceholder,
	}})

	ddl := &event.DDLEvent{
		Query:      "INVALID SQL !!!",
		SchemaName: "source_db",
	}

	_, err := router.rewriteParserBackedDDLQuery(ddl)
	code, ok := errors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, errors.ErrTableRoutingFailed.RFCCode(), code)
}

func TestRewriteAddFullTextIndexQueryError(t *testing.T) {
	t.Parallel()

	router := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "{table}_r",
	}})

	_, err := router.rewriteAddFullTextIndexQuery("NOT AN ALTER TABLE", "target_db", "t1_r")
	require.True(t, errors.ErrTableRoutingFailed.Equal(err))
}

func TestRewriteCreateHybridIndexQueryError(t *testing.T) {
	t.Parallel()

	router := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "{table}_r",
	}})

	_, err := router.rewriteCreateHybridIndexQuery("NOT A CREATE HYBRID INDEX", "target_db", "t1_r")
	require.True(t, errors.ErrTableRoutingFailed.Equal(err))
}
