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
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestApplyToTableInfo(t *testing.T) {
	t.Parallel()

	tableInfo := &common.TableInfo{
		TableName: common.TableName{
			Schema:  "source_db",
			Table:   "source_table",
			TableID: 1,
		},
	}

	var nilRouter *Router
	require.Same(t, tableInfo, nilRouter.ApplyToTableInfo(tableInfo))

	noOpRouter, err := NewRouter(false, []*config.DispatchRule{
		{
			Matcher:      []string{"other_db.*"},
			TargetSchema: "target_db",
			TargetTable:  TablePlaceholder,
		},
	})
	require.NoError(t, err)
	require.Same(t, tableInfo, noOpRouter.ApplyToTableInfo(tableInfo))

	router, err := NewRouter(false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.source_table"},
			TargetSchema: "target_db",
			TargetTable:  "target_table",
		},
	})
	require.NoError(t, err)

	routed := router.ApplyToTableInfo(tableInfo)
	require.NotSame(t, tableInfo, routed)
	require.Equal(t, "source_db", routed.TableName.Schema)
	require.Equal(t, "source_table", routed.TableName.Table)
	require.Equal(t, "target_db", routed.TableName.TargetSchema)
	require.Equal(t, "target_table", routed.TableName.TargetTable)

	require.Empty(t, tableInfo.TableName.TargetSchema)
	require.Empty(t, tableInfo.TableName.TargetTable)
}

func TestApplyToTableInfoExposesTargetNames(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.source_table"},
			TargetSchema: "target_db",
			TargetTable:  "target_table",
		},
	})
	require.NoError(t, err)

	tableInfo := &common.TableInfo{
		TableName: common.TableName{
			Schema:  "source_db",
			Table:   "source_table",
			TableID: 1,
		},
	}

	routed := router.ApplyToTableInfo(tableInfo)
	require.NotSame(t, tableInfo, routed)
	require.Equal(t, "target_db", routed.GetSchemaName())
	require.Equal(t, "target_table", routed.GetTableName())
}

func TestApplyToDDLEvent(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.source_table"},
			TargetSchema: "target_db",
			TargetTable:  "target_table",
		},
	})
	require.NoError(t, err)

	originalTableInfo := &common.TableInfo{
		TableName: common.TableName{
			Schema:  "source_db",
			Table:   "source_table",
			TableID: 1,
		},
	}
	ddl := &commonEvent.DDLEvent{
		Query:      "ALTER TABLE `source_db`.`source_table` ADD INDEX idx_id(id)",
		SchemaName: "source_db",
		TableName:  "source_table",
		TableInfo:  originalTableInfo,
		MultipleTableInfos: []*common.TableInfo{
			originalTableInfo,
		},
		BlockedTableNames: []commonEvent.SchemaTableName{
			{SchemaName: "source_db", TableName: "source_table"},
		},
	}

	routed, err := router.ApplyToDDLEvent(ddl, common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test-changefeed"))
	require.NoError(t, err)
	require.NotSame(t, ddl, routed)
	require.Contains(t, routed.Query, "`target_db`.`target_table`")
	require.Equal(t, "target_db", routed.GetDDLSchemaName())
	require.NotSame(t, originalTableInfo, routed.TableInfo)
	require.Equal(t, "target_db", routed.TableInfo.TableName.TargetSchema)
	require.Equal(t, "target_table", routed.TableInfo.TableName.TargetTable)
	require.NotSame(t, originalTableInfo, routed.MultipleTableInfos[0])
	require.Equal(t, "target_db", routed.MultipleTableInfos[0].TableName.TargetSchema)
	require.Equal(t, "target_table", routed.MultipleTableInfos[0].TableName.TargetTable)
	require.Equal(t, commonEvent.SchemaTableName{
		SchemaName: "target_db",
		TableName:  "target_table",
	}, routed.BlockedTableNames[0])

	require.Equal(t, "ALTER TABLE `source_db`.`source_table` ADD INDEX idx_id(id)", ddl.Query)
	require.Equal(t, "source_db", ddl.GetDDLSchemaName())
	require.Empty(t, ddl.TableInfo.TableName.TargetSchema)
	require.Empty(t, ddl.TableInfo.TableName.TargetTable)
	require.Equal(t, commonEvent.SchemaTableName{
		SchemaName: "source_db",
		TableName:  "source_table",
	}, ddl.BlockedTableNames[0])
}

func TestApplyToDDLEventRewritesStructuredFieldsToTargetNames(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []*config.DispatchRule{
		{
			Matcher:      []string{"source_db.source_table"},
			TargetSchema: "target_db",
			TargetTable:  "target_table",
		},
	})
	require.NoError(t, err)

	ddl := &commonEvent.DDLEvent{
		Query:      "ALTER TABLE `source_db`.`source_table` ADD COLUMN c INT",
		SchemaName: "source_db",
		TableName:  "source_table",
		TableInfo: &common.TableInfo{
			TableName: common.TableName{
				Schema:  "source_db",
				Table:   "source_table",
				TableID: 1,
			},
		},
		MultipleTableInfos: []*common.TableInfo{
			{
				TableName: common.TableName{
					Schema:  "source_db",
					Table:   "source_table",
					TableID: 1,
				},
			},
		},
		BlockedTableNames: []commonEvent.SchemaTableName{
			{SchemaName: "source_db", TableName: "source_table"},
		},
	}

	routed, err := router.ApplyToDDLEvent(ddl, common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test-changefeed"))
	require.NoError(t, err)
	require.NotSame(t, ddl, routed)
	require.Equal(t, "target_db", routed.SchemaName)
	require.Equal(t, "target_table", routed.TableName)
	require.Equal(t, "target_db", routed.TableInfo.GetSchemaName())
	require.Equal(t, "target_table", routed.TableInfo.GetTableName())
	require.Equal(t, "target_db", routed.MultipleTableInfos[0].GetSchemaName())
	require.Equal(t, "target_table", routed.MultipleTableInfos[0].GetTableName())
	require.Equal(t, commonEvent.SchemaTableName{
		SchemaName: "target_db",
		TableName:  "target_table",
	}, routed.BlockedTableNames[0])
	require.Contains(t, routed.Query, "`target_db`.`target_table`")
}

func TestApplyToDDLEventPreservesRenameRolesWithTargetNames(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []*config.DispatchRule{
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

	ddl := &commonEvent.DDLEvent{
		Query:           "RENAME TABLE `old_db`.`orders` TO `new_db`.`orders_archive`",
		SchemaName:      "new_db",
		TableName:       "orders_archive",
		ExtraSchemaName: "old_db",
		ExtraTableName:  "orders",
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{{
				SchemaName: "new_db",
				TableName:  "orders_archive",
			}},
			DropName: []commonEvent.SchemaTableName{{
				SchemaName: "old_db",
				TableName:  "orders",
			}},
		},
	}

	routed, err := router.ApplyToDDLEvent(ddl, common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test-changefeed"))
	require.NoError(t, err)
	require.NotSame(t, ddl, routed)
	require.Equal(t, "new_target_db", routed.SchemaName)
	require.Equal(t, "orders_archive_new", routed.TableName)
	require.Equal(t, "old_target_db", routed.ExtraSchemaName)
	require.Equal(t, "orders_old", routed.ExtraTableName)
	require.Equal(t, commonEvent.SchemaTableName{
		SchemaName: "new_target_db",
		TableName:  "orders_archive_new",
	}, routed.TableNameChange.AddName[0])
	require.Equal(t, commonEvent.SchemaTableName{
		SchemaName: "old_target_db",
		TableName:  "orders_old",
	}, routed.TableNameChange.DropName[0])
	require.Contains(t, routed.Query, "`old_target_db`.`orders_old`")
	require.Contains(t, routed.Query, "`new_target_db`.`orders_archive_new`")
}
