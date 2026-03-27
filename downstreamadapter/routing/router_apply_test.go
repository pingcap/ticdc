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

	noOpRouter, err := NewRouter(false, []RoutingRuleConfig{
		{
			Matcher:      []string{"other_db.*"},
			TargetSchema: "target_db",
			TargetTable:  TablePlaceholder,
		},
	})
	require.NoError(t, err)
	require.Same(t, tableInfo, noOpRouter.ApplyToTableInfo(tableInfo))

	router, err := NewRouter(false, []RoutingRuleConfig{
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

func TestApplyToDDLEvent(t *testing.T) {
	t.Parallel()

	router, err := NewRouter(false, []RoutingRuleConfig{
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

	routed, err := router.ApplyToDDLEvent(ddl, "test-changefeed")
	require.NoError(t, err)
	require.NotSame(t, ddl, routed)
	require.Contains(t, routed.Query, "`target_db`.`target_table`")
	require.Equal(t, "target_db", routed.TargetSchemaName)
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
	require.Empty(t, ddl.TargetSchemaName)
	require.Empty(t, ddl.TableInfo.TableName.TargetSchema)
	require.Empty(t, ddl.TableInfo.TableName.TargetTable)
	require.Equal(t, commonEvent.SchemaTableName{
		SchemaName: "source_db",
		TableName:  "source_table",
	}, ddl.BlockedTableNames[0])
}
