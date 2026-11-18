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

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	parser_model "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestVerifyTablesSkipsInternalMaterializedViewTables(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	dbInfo := &model.DBInfo{ID: 100, Name: parser_model.NewCIStr("test")}
	eligibleTable := newEligibleTableInfoForTest(200, "eligible")
	ineligibleTable := newEligibleTableInfoForTest(201, "ineligible")
	ineligibleTable.PKIsHandle = false
	ineligibleTable.Indices = nil
	ineligibleTable.Columns[0].DelFlag(mysql.PriKeyFlag)
	mlogTable := ineligibleTable.Clone()
	mlogTable.ID = 202
	mlogTable.Name = parser_model.NewCIStr("$mlog$eligible")
	mlogTable.MaterializedViewLog = &model.MaterializedViewLogInfo{BaseTableID: eligibleTable.ID}
	shadowTable := eligibleTable.Clone()
	shadowTable.ID = 203
	shadowTable.Name = parser_model.NewCIStr("__mv_shadow_200_1")
	shadowTable.MaterializedViewShadow = &model.MaterializedViewShadowInfo{SourceMViewID: eligibleTable.ID}

	txn, err := store.Begin()
	require.NoError(t, err)
	metaMutator := meta.NewMutator(txn)
	require.NoError(t, metaMutator.CreateDatabase(dbInfo))
	for _, tableInfo := range []*model.TableInfo{eligibleTable, ineligibleTable, mlogTable, shadowTable} {
		require.NoError(t, metaMutator.CreateTableOrView(dbInfo.ID, tableInfo))
	}
	require.NoError(t, txn.Commit(context.Background()))

	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	tableFilter, err := filter.NewFilter(
		&config.FilterConfig{Rules: []string{"test.*"}}, "", false, false,
	)
	require.NoError(t, err)

	tableInfos, ineligibleTables, eligibleTables, allTables, err := VerifyTables(tableFilter, store, version.Ver)
	require.NoError(t, err)
	require.Len(t, tableInfos, 2)
	require.ElementsMatch(t, []common.TableName{
		{Schema: "test", Table: "ineligible", TableID: 201},
	}, ineligibleTables)
	require.ElementsMatch(t, []common.TableName{
		{Schema: "test", Table: "eligible", TableID: 200},
	}, eligibleTables)
	require.ElementsMatch(t, []common.TableName{
		{Schema: "test", Table: "eligible", TableID: 200},
		{Schema: "test", Table: "ineligible", TableID: 201},
		{Schema: "test", Table: "$mlog$eligible", TableID: 202},
		{Schema: "test", Table: "__mv_shadow_200_1", TableID: 203},
	}, allTables)
}
