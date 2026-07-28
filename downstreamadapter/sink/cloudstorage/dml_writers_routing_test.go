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

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/pkg/meta/model"
	parser_model "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestAddDMLEventUsesTargetNames(t *testing.T) {
	t.Parallel()

	idFieldType := types.NewFieldType(mysql.TypeLong)
	idFieldType.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	routedTableInfo := common.WrapTableInfo("source_db", &model.TableInfo{
		ID:       20,
		Name:     parser_model.NewCIStr("source_table"),
		UpdateTS: 100,
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      parser_model.NewCIStr("id"),
				FieldType: *idFieldType,
				State:     model.StatePublic,
			},
		},
	}).CloneWithRouting("target_db", "target_table")
	dmlEvent := commonEvent.NewDMLEvent(
		common.NewDispatcherID(),
		routedTableInfo.TableName.TableID,
		1,
		2,
		routedTableInfo,
	)
	dmlEvent.TableInfoVersion = routedTableInfo.GetUpdateTS()

	writers := &dmlWriters{
		msgCh: chann.NewUnlimitedChannelDefault[*task](),
	}

	writers.addDMLEvent(dmlEvent)

	task, ok, err := writers.msgCh.GetWithContext(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, task)
	require.Equal(t, "target_db", task.versionedTable.TableNameWithPhysicTableID.Schema)
	require.Equal(t, "target_table", task.versionedTable.TableNameWithPhysicTableID.Table)
	require.Equal(t, dmlEvent.PhysicalTableID, task.versionedTable.TableNameWithPhysicTableID.TableID)
	require.Equal(t, routedTableInfo.IsPartitionTable(), task.versionedTable.TableNameWithPhysicTableID.IsPartition)
}
