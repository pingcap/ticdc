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

func TestApplyToDDLEvent(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	sourceDBDDL := helper.DDL2Event("CREATE DATABASE `source_db`")
	oldDBDDL := helper.DDL2Event("CREATE DATABASE `old_db`")
	newDBDDL := helper.DDL2Event("CREATE DATABASE `new_db`")
	multiDBDDL := helper.DDL2Event("CREATE DATABASE `multi_db`")
	sourceTableDDL := helper.DDL2Event("CREATE TABLE `source_db`.`source_table` (`id` INT PRIMARY KEY)")
	singleTableDDL := helper.DDL2Event("ALTER TABLE `source_db`.`source_table` ADD INDEX `idx_id`(`id`)")
	multiT1DDL := helper.DDL2Event("CREATE TABLE `multi_db`.`t1` (`id` INT PRIMARY KEY)")
	multiT2DDL := helper.DDL2Event("CREATE TABLE `multi_db`.`t2` (`id` INT PRIMARY KEY)")
	renameTablesDDL := helper.DDL2Event("RENAME TABLE `multi_db`.`t1` TO `multi_db`.`t1_new`, `multi_db`.`t2` TO `multi_db`.`t2_new`")
	oldOrdersDDL := helper.DDL2Event("CREATE TABLE `old_db`.`orders` (`id` INT PRIMARY KEY)")
	renameDDL := helper.DDL2Event("RENAME TABLE `old_db`.`orders` TO `new_db`.`orders_archive`")

	var zeroRouter Router
	noMatchedRouter := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"other_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "target_table",
	}})
	sourceRouter := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.source_table"},
		TargetSchema: "target_db",
		TargetTable:  "target_table",
	}})
	sourceSchemaRouter := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"source_db.*"},
		TargetSchema: "target_db",
		TargetTable:  "target_table",
	}})
	renameRouter := newTestRouter(t, false, []*config.DispatchRule{
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
	multiRouter := newTestRouter(t, false, []*config.DispatchRule{{
		Matcher:      []string{"multi_db.*"},
		TargetSchema: "target_multi_db",
		TargetTable:  "{table}_routed",
	}})

	tests := []struct {
		name       string
		router     Router
		ddl        *event.DDLEvent
		expectSame bool
		check      func(t *testing.T, original, routed *event.DDLEvent)
	}{
		{
			name:       "zero router keeps original",
			router:     zeroRouter,
			ddl:        singleTableDDL,
			expectSame: true,
		},
		{
			name:       "no matched rule keeps original",
			router:     noMatchedRouter,
			ddl:        singleTableDDL,
			expectSame: true,
		},
		{
			name:   "source database ddl",
			router: sourceSchemaRouter,
			ddl:    sourceDBDDL,
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Contains(t, routed.Query, "`target_db`")
				require.NotContains(t, routed.Query, "`source_db`")
				require.Equal(t, "source_db", routed.SchemaName)
				require.Equal(t, "target_db", routed.GetTargetSchemaName())
				require.Equal(t, "source_db", original.SchemaName)
			},
		},
		{
			name:   "old database ddl",
			router: renameRouter,
			ddl:    oldDBDDL,
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Contains(t, routed.Query, "`old_target_db`")
				require.Equal(t, "old_db", routed.SchemaName)
				require.Equal(t, "old_target_db", routed.GetTargetSchemaName())
			},
		},
		{
			name:   "new database ddl",
			router: renameRouter,
			ddl:    newDBDDL,
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Contains(t, routed.Query, "`new_target_db`")
				require.Equal(t, "new_db", routed.SchemaName)
				require.Equal(t, "new_target_db", routed.GetTargetSchemaName())
			},
		},
		{
			name:   "multi database ddl",
			router: multiRouter,
			ddl:    multiDBDDL,
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Contains(t, routed.Query, "`target_multi_db`")
				require.Equal(t, "multi_db", routed.SchemaName)
				require.Equal(t, "target_multi_db", routed.GetTargetSchemaName())
			},
		},
		{
			name:   "create source table ddl",
			router: sourceRouter,
			ddl:    sourceTableDDL,
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Contains(t, routed.Query, "`target_db`.`target_table`")
				require.NotSame(t, original.TableInfo, routed.TableInfo)
				require.Equal(t, "target_db", routed.TableInfo.GetTargetSchemaName())
				require.Equal(t, "target_table", routed.TableInfo.GetTargetTableName())
			},
		},
		{
			name:   "single table ddl",
			router: sourceRouter,
			ddl:    singleTableDDL,
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
			name:   "old table create ddl",
			router: renameRouter,
			ddl:    oldOrdersDDL,
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Contains(t, routed.Query, "`old_target_db`.`orders_old`")
				require.NotSame(t, original.TableInfo, routed.TableInfo)
				require.Equal(t, "old_target_db", routed.TableInfo.GetTargetSchemaName())
				require.Equal(t, "orders_old", routed.TableInfo.GetTargetTableName())
			},
		},
		{
			name:   "rename ddl",
			router: renameRouter,
			ddl:    renameDDL,
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
			name:   "multi table t1 create ddl",
			router: multiRouter,
			ddl:    multiT1DDL,
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Contains(t, routed.Query, "`target_multi_db`.`t1_routed`")
				require.NotSame(t, original.TableInfo, routed.TableInfo)
				require.Equal(t, "target_multi_db", routed.TableInfo.GetTargetSchemaName())
				require.Equal(t, "t1_routed", routed.TableInfo.GetTargetTableName())
			},
		},
		{
			name:   "multi table t2 create ddl",
			router: multiRouter,
			ddl:    multiT2DDL,
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Contains(t, routed.Query, "`target_multi_db`.`t2_routed`")
				require.NotSame(t, original.TableInfo, routed.TableInfo)
				require.Equal(t, "target_multi_db", routed.TableInfo.GetTargetSchemaName())
				require.Equal(t, "t2_routed", routed.TableInfo.GetTargetTableName())
			},
		},
		{
			name:   "rename tables routes multiple table infos",
			router: multiRouter,
			ddl:    renameTablesDDL,
			check: func(t *testing.T, original, routed *event.DDLEvent) {
				require.Len(t, routed.MultipleTableInfos, 2)
				require.Contains(t, routed.Query, "`target_multi_db`.`t1_routed`")
				require.Contains(t, routed.Query, "`target_multi_db`.`t1_new_routed`")
				require.Contains(t, routed.Query, "`target_multi_db`.`t2_routed`")
				require.Contains(t, routed.Query, "`target_multi_db`.`t2_new_routed`")
				require.NotSame(t, original.MultipleTableInfos[0], routed.MultipleTableInfos[0])
				require.NotSame(t, original.MultipleTableInfos[1], routed.MultipleTableInfos[1])
				require.Equal(t, "multi_db", routed.MultipleTableInfos[0].GetSchemaName())
				require.Equal(t, "t1_new", routed.MultipleTableInfos[0].GetTableName())
				require.Equal(t, "target_multi_db", routed.MultipleTableInfos[0].GetTargetSchemaName())
				require.Equal(t, "t1_new_routed", routed.MultipleTableInfos[0].GetTargetTableName())
				require.Equal(t, "multi_db", routed.MultipleTableInfos[1].GetSchemaName())
				require.Equal(t, "t2_new", routed.MultipleTableInfos[1].GetTableName())
				require.Equal(t, "target_multi_db", routed.MultipleTableInfos[1].GetTargetSchemaName())
				require.Equal(t, "t2_new_routed", routed.MultipleTableInfos[1].GetTargetTableName())
				require.Empty(t, original.MultipleTableInfos[0].TableName.TargetSchema)
				require.Empty(t, original.MultipleTableInfos[0].TableName.TargetTable)
			},
		},
		{
			name:   "single table ddl routes blocked table names",
			router: sourceRouter,
			ddl:    singleTableDDL,
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
			if tc.expectSame {
				require.Same(t, tc.ddl, routed)
				return
			}
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

	sourceDBDDL := helper.DDL2Event("CREATE DATABASE `source_db`")
	otherDBDDL := helper.DDL2Event("CREATE DATABASE `other_db`")
	db1DDL := helper.DDL2Event("CREATE DATABASE `db1`")
	db2DDL := helper.DDL2Event("CREATE DATABASE `db2`")
	noRouterDDL := helper.DDL2Event("CREATE TABLE `source_db`.`test_table` (`id` INT PRIMARY KEY)")
	noMatchedDDL := helper.DDL2Event("CREATE TABLE `other_db`.`test_table` (`id` INT PRIMARY KEY)")
	matchedTableDDL := helper.DDL2Event("ALTER TABLE `source_db`.`test_table` ADD COLUMN `c` INT")
	databaseDDL := helper.DDL2Event("ALTER DATABASE `source_db` CHARACTER SET utf8mb4")
	db1TableDDL := helper.DDL2Event("CREATE TABLE `db1`.`t1` (`id` INT PRIMARY KEY)")
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
			name: "source database ddl rewrites schema",
			router: newTestRouter(t, false, []*config.DispatchRule{{
				Matcher:      []string{"source_db.*"},
				TargetSchema: "target_db",
			}}),
			ddl:               sourceDBDDL,
			expectedChanged:   true,
			requiredFragments: []string{"`target_db`"},
			forbiddenFragment: "`source_db`",
		},
		{
			name:            "other database ddl keeps original query",
			router:          newTestRouter(t, false, []*config.DispatchRule{{Matcher: []string{"source_db.*"}, TargetSchema: "target_db"}}),
			ddl:             otherDBDDL,
			expectedChanged: false,
			expectedQuery:   otherDBDDL.Query,
		},
		{
			name: "db1 database ddl rewrites schema",
			router: newTestRouter(t, false, []*config.DispatchRule{{
				Matcher:      []string{"db1.*"},
				TargetSchema: "target1",
			}}),
			ddl:               db1DDL,
			expectedChanged:   true,
			requiredFragments: []string{"`target1`"},
			forbiddenFragment: "`db1`",
		},
		{
			name: "db2 database ddl rewrites schema",
			router: newTestRouter(t, false, []*config.DispatchRule{{
				Matcher:      []string{"db2.*"},
				TargetSchema: "target2",
			}}),
			ddl:               db2DDL,
			expectedChanged:   true,
			requiredFragments: []string{"`target2`"},
			forbiddenFragment: "`db2`",
		},
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
			name: "db1 table create ddl rewrites table",
			router: newTestRouter(t, false, []*config.DispatchRule{{
				Matcher:      []string{"db1.*"},
				TargetSchema: "target1",
				TargetTable:  TablePlaceholder,
			}}),
			ddl:               db1TableDDL,
			expectedChanged:   true,
			requiredFragments: []string{"`target1`.`t1`"},
			forbiddenFragment: "`db1`.`t1`",
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

	otherDBDDL := helper.DDL2Event("CREATE DATABASE `other_db`")
	otherTableDDL := helper.DDL2Event("CREATE TABLE `other_db`.`t1` (`id` INT PRIMARY KEY)")
	ddl := helper.DDL2Event("ALTER TABLE `other_db`.`t1` ADD COLUMN `c1` INT")

	routed, err := router.ApplyToDDLEvent(otherDBDDL)
	require.NoError(t, err)
	require.Same(t, otherDBDDL, routed)

	routed, err = router.ApplyToDDLEvent(otherTableDDL)
	require.NoError(t, err)
	require.Same(t, otherTableDDL, routed)

	routed, err = router.ApplyToDDLEvent(ddl)
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

	sourceDBDDL := helper.DDL2Event("CREATE DATABASE `source_db`")
	otherDBDDL := helper.DDL2Event("CREATE DATABASE `other_db`")
	sourceOrdersDDL := helper.DDL2Event("CREATE TABLE `source_db`.`orders` (`id` INT PRIMARY KEY)")
	ddl := helper.DDL2Event("CREATE VIEW `other_db`.`v1` AS SELECT * FROM `source_db`.`orders`")

	routed, err := router.ApplyToDDLEvent(sourceDBDDL)
	require.NoError(t, err)
	require.Contains(t, routed.Query, "`target_db`")

	routed, err = router.ApplyToDDLEvent(otherDBDDL)
	require.NoError(t, err)
	require.Same(t, otherDBDDL, routed)

	routed, err = router.ApplyToDDLEvent(sourceOrdersDDL)
	require.NoError(t, err)
	require.Contains(t, routed.Query, "`target_db`.`orders_routed`")

	routed, err = router.ApplyToDDLEvent(ddl)
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

	_, _, err := router.rewriteSingleDDLQuery("INVALID SQL !!!")
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
