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

package cloudstorage

import (
	"context"
	"testing"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/utils/chann"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestAddDMLEventUsesTargetNames(t *testing.T) {
	t.Parallel()

	routedTableInfo := commonType.WrapTableInfo("test", &timodel.TableInfo{
		ID:   100,
		Name: ast.NewCIStr("t"),
		Columns: []*timodel.ColumnInfo{
			{
				Name:      ast.NewCIStr("id"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
				State:     timodel.StatePublic,
			},
		},
		UpdateTS: 99,
	}).CloneWithRouting("target_db", "target_table")
	dmlEvent := commonEvent.NewDMLEvent(commonType.NewDispatcherID(), routedTableInfo.TableName.TableID, 1, 2, routedTableInfo)
	dmlEvent.TableInfoVersion = routedTableInfo.GetUpdateTS()

	writers := &dmlWriters{
		msgCh: chann.NewUnlimitedChannelDefault[*task](),
	}

	writers.addDMLEvent(dmlEvent)

	task, ok, err := writers.msgCh.GetWithContext(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, task)
	require.Equal(t, commonType.TableName{
		Schema:      "target_db",
		Table:       "target_table",
		TableID:     dmlEvent.PhysicalTableID,
		IsPartition: routedTableInfo.IsPartitionTable(),
	}, task.versionedTable.TableNameWithPhysicTableID)
}
