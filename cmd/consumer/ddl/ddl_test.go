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

package ddl

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func testTable(schema, table string) *common.TableInfo {
	return common.WrapTableInfo(schema, &model.TableInfo{
		ID:   20,
		Name: ast.NewCIStr(table),
	})
}

func TestBlocksIdentity(t *testing.T) {
	other := testTable("test", "other")
	cases := []struct {
		name    string
		ddl     *event.DDLEvent
		tableID int64
		schema  string
		table   string
		want    bool
	}{
		{
			name:   "global barrier without schema",
			ddl:    &event.DDLEvent{},
			schema: "test", table: "t", want: true,
		},
		{
			name: "global influence",
			ddl: &event.DDLEvent{
				SchemaName:    "test",
				BlockedTables: &event.InfluencedTables{InfluenceType: event.InfluenceTypeAll},
			},
			schema: "test", table: "t", want: true,
		},
		{
			name: "database scope",
			ddl: &event.DDLEvent{
				SchemaName:    "test",
				BlockedTables: &event.InfluencedTables{InfluenceType: event.InfluenceTypeDB},
			},
			schema: "test", table: "t", want: true,
		},
		{
			name: "database scope other schema",
			ddl: &event.DDLEvent{
				SchemaName:    "test",
				BlockedTables: &event.InfluencedTables{InfluenceType: event.InfluenceTypeDB},
			},
			schema: "other", table: "t", want: false,
		},
		{
			name:   "exact table",
			ddl:    &event.DDLEvent{SchemaName: "test", TableName: "t"},
			schema: "test", table: "t", want: true,
		},
		{
			name: "extra table",
			ddl: &event.DDLEvent{
				SchemaName:      "test",
				TableName:       "t",
				ExtraSchemaName: "test",
				ExtraTableName:  "extra",
			},
			schema: "test", table: "extra", want: true,
		},
		{
			name: "blocked table name",
			ddl: &event.DDLEvent{
				SchemaName:        "test",
				TableName:         "t",
				BlockedTableNames: []event.SchemaTableName{{SchemaName: "test", TableName: "other"}},
			},
			schema: "test", table: "other", want: true,
		},
		{
			name: "multiple table infos",
			ddl: &event.DDLEvent{
				SchemaName:         "test",
				TableName:          "t",
				MultipleTableInfos: []*common.TableInfo{other},
			},
			schema: "test", table: "other", want: true,
		},
		{
			name: "blocked table id",
			ddl: &event.DDLEvent{
				SchemaName: "test",
				TableName:  "t",
				BlockedTables: &event.InfluencedTables{
					InfluenceType: event.InfluenceTypeNormal,
					TableIDs:      []int64{20},
				},
			},
			tableID: 20, schema: "test", table: "t2", want: true,
		},
		{
			name:   "unrelated table",
			ddl:    &event.DDLEvent{SchemaName: "test", TableName: "t"},
			schema: "test", table: "other", want: false,
		},
		{
			name: "rename without table metadata",
			ddl: &event.DDLEvent{
				Type:       byte(model.ActionRenameTables),
				SchemaName: "test",
				TableName:  "t",
			},
			schema: "test", table: "other", want: true,
		},
		{
			name: "rename with known table names",
			ddl: &event.DDLEvent{
				Type:              byte(model.ActionRenameTables),
				SchemaName:        "test",
				TableName:         "t",
				BlockedTableNames: []event.SchemaTableName{{SchemaName: "test", TableName: "other"}},
			},
			schema: "test", table: "other", want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, BlocksIdentity(tc.ddl, tc.tableID, tc.schema, tc.table))
		})
	}
}

func TestBlocksTableKeepsGlobalBarrierWithoutTableInfo(t *testing.T) {
	dml := &event.DMLEvent{PhysicalTableID: 20}
	require.True(t, BlocksTable(&event.DDLEvent{}, dml))
}
