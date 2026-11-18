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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemastore

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/ordering"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestTxnOrderSnapshotAndDDL(t *testing.T) {
	for _, partitioned := range []bool{false, true} {
		t.Run(map[bool]string{false: "regular", true: "partitioned"}[partitioned], func(t *testing.T) {
			db, err := pebble.Open(t.TempDir(), &pebble.Options{})
			require.NoError(t, err)
			defer db.Close()
			dbInfo := &model.DBInfo{ID: 100, Name: ast.NewCIStr("test")}
			tableInfo := newEligibleTableInfoForTest(200, "ordered")
			if partitioned {
				tableInfo.Partition = &model.PartitionInfo{Enable: true, Definitions: []model.PartitionDefinition{{ID: 201, Name: ast.NewCIStr("p0")}}}
			}
			batch := db.NewBatch()
			defer batch.Close()
			require.NoError(t, addSchemaInfoToBatchWithEncryption(context.Background(), batch, 100, dbInfo, nil, 0))
			_, _, _, _, err = addTableInfoToBatchWithEncryption(context.Background(), batch, 100, dbInfo, tableInfo, nil, 0, nil)
			require.NoError(t, err)
			require.NoError(t, batch.Commit(pebble.NoSync))
			snap := db.NewSnapshot()
			defer snap.Close()
			for _, rule := range []string{"test.ordered", "test.unmatched"} {
				f, err := filter.NewFilter(config.NewDefaultFilterConfig(), "", false, false)
				require.NoError(t, err)
				matcher, err := ordering.NewTxnOrderMatcher([]string{rule}, false)
				require.NoError(t, err)
				tables, err := loadAllPhysicalTablesAtTs(snap, 100, 100, f, matcher, nil, 0)
				if partitioned && rule == "test.ordered" {
					require.ErrorContains(t, err, "txn-order-tables does not support partitioned table")
				} else {
					require.NoError(t, err)
					require.Len(t, tables, 1)
					if rule == "test.ordered" {
						require.False(t, tables[0].Splitable)
					}
				}

				raw := &PersistedDDLEvent{
					Type: byte(model.ActionCreateTable), SchemaID: 100, SchemaName: "test",
					TableID: 200, TableName: "ordered", TableInfo: tableInfo, FinishedTs: 101,
					Query: "CREATE TABLE test.ordered (id INT PRIMARY KEY)",
				}
				ddl, ok, err := buildDDLEvent(raw, f, matcher, common.DDLSpanTableID)
				require.NoError(t, err)
				require.True(t, ok)
				if partitioned && rule == "test.ordered" {
					require.ErrorContains(t, ddl.GetError(), "txn-order-tables does not support partitioned table")
					data, err := ddl.Marshal()
					require.NoError(t, err)
					var decoded commonEvent.DDLEvent
					require.NoError(t, decoded.Unmarshal(data))
					require.ErrorContains(t, decoded.GetError(), "txn-order-tables does not support partitioned table")
					require.True(t, errors.ShouldFailChangefeed(decoded.GetError()))
				} else {
					require.NoError(t, ddl.GetError())
				}
				// Existing table dispatchers must receive the same validation error.
				raw.Type = byte(model.ActionAddIndex)
				physicalID := int64(200)
				if partitioned {
					physicalID = 201
				}
				tableDDL, ok, err := buildTableDDLEvent(raw, f, matcher, physicalID)
				require.NoError(t, err)
				require.True(t, ok)
				if partitioned && rule == "test.ordered" {
					require.ErrorContains(t, tableDDL.GetError(), "does not support partitioned table")
					require.True(t, errors.ShouldFailChangefeed(tableDDL.GetError()))
				} else {
					require.NoError(t, tableDDL.GetError())
				}
			}
			if !partitioned {
				return
			}
			// Renaming into the ordered set and acquiring a unique key must also validate.
			f, err := filter.NewFilter(config.NewDefaultFilterConfig(), "", false, false)
			require.NoError(t, err)
			matcher, err := ordering.NewTxnOrderMatcher([]string{"test.ordered"}, false)
			require.NoError(t, err)
			for _, ddlType := range []model.ActionType{model.ActionRenameTable, model.ActionAlterTablePartitioning, model.ActionAddIndex, model.ActionCreateTables, model.ActionRenameTables} {
				raw := &PersistedDDLEvent{Type: byte(ddlType), SchemaName: "test", TableInfo: tableInfo}
				if ddlType == model.ActionCreateTables || ddlType == model.ActionRenameTables {
					raw.TableInfo = nil
					raw.MultipleTableInfos = []*model.TableInfo{tableInfo}
					raw.SchemaNames = []string{"test"}
				}
				require.ErrorContains(t, verifyTxnOrderDDL(raw, f, matcher), "does not support partitioned table")
			}
			noKey := tableInfo.Clone()
			noKey.PKIsHandle = false
			noKey.Indices = nil
			raw := &PersistedDDLEvent{SchemaName: "test", TableInfo: noKey}
			require.NoError(t, verifyTxnOrderDDL(raw, f, matcher))
			forced, err := filter.NewFilter(config.NewDefaultFilterConfig(), "", false, true)
			require.NoError(t, err)
			require.ErrorContains(t, verifyTxnOrderDDL(raw, forced, matcher), "does not support partitioned table")
			excludedConfig := config.NewDefaultFilterConfig()
			excludedConfig.Rules = []string{"*.*", "!test.ordered"}
			excluded, err := filter.NewFilter(excludedConfig, "", false, false)
			require.NoError(t, err)
			require.NoError(t, verifyTxnOrderDDL(raw, excluded, matcher))
			tables, err := loadAllPhysicalTablesAtTs(snap, 100, 100, excluded, matcher, nil, 0)
			require.NoError(t, err)
			require.Empty(t, tables)
		})
	}
}
