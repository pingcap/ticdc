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

package debezium

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// TestDecodedTableInfoLocatesRowByPrimaryKey checks the table info this decoder
// builds from a payload: a composite primary key must stay the handle key, with
// the real column offsets, because that is what the MySQL sink locates rows by.
func TestDecodedTableInfoLocatesRowByPrimaryKey(t *testing.T) {
	cfg := common.NewConfig(config.ProtocolDebezium)
	cfg.EnableTiDBExtension = true
	cfg.TimeZone = time.UTC

	encoder := NewBatchEncoder(cfg, "dbserver1")
	require.NoError(t, encoder.AppendRowChangedEvent(context.Background(), "", compositeKeyRowEvent(t)))
	messages := encoder.Build()
	require.Len(t, messages, 1)

	decoder := NewDecoder(cfg, 0, nil)
	decoder.AddKeyValue(messages[0].Key, messages[0].Value)
	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event := decoder.NextDMLMessage().ToDMLEvent()
	common.RequireRowLocatorByPrimaryKey(t, event.TableInfo, "a", "b")
}

// compositeKeyRowEvent builds an insert into test.t(a, b, c) with primary key
// (a, b), the shape a composite key arrives in from an upstream table.
func compositeKeyRowEvent(t *testing.T) *commonEvent.RowEvent {
	t.Helper()

	newColumn := func(id int64, offset int, name string, tp byte) *model.ColumnInfo {
		return &model.ColumnInfo{
			ID:        id,
			Name:      ast.NewCIStr(name),
			FieldType: *types.NewFieldType(tp),
			State:     model.StatePublic,
			Offset:    offset,
		}
	}
	tidbTableInfo := &model.TableInfo{
		Name: ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			newColumn(1, 0, "a", mysql.TypeLong),
			newColumn(2, 1, "b", mysql.TypeLong),
			newColumn(3, 2, "c", mysql.TypeVarchar),
		},
		Indices: []*model.IndexInfo{{
			Name:    ast.NewCIStr("primary"),
			Primary: true,
			Unique:  true,
			State:   model.StatePublic,
			Columns: []*model.IndexColumn{
				{Name: ast.NewCIStr("a"), Offset: 0},
				{Name: ast.NewCIStr("b"), Offset: 1},
			},
		}},
	}
	tableInfo := commonType.NewTableInfo4Decoder("test", tidbTableInfo)

	event := commonEvent.NewDMLEvent(
		commonType.NewDispatcherID(), tableInfo.TableName.TableID, 1, 2, tableInfo)
	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	rows.AppendRow(chunk.MutRowFromValues(int64(1), int64(2), "x").ToRow())
	event.SetRows(rows)
	event.RowTypes = append(event.RowTypes, commonType.RowTypeInsert)
	event.RowKeys = append(event.RowKeys, []byte("row-key"))
	event.Length = 1
	event.TableInfoVersion = tableInfo.GetUpdateTS()

	row, ok := event.GetNextRow()
	require.True(t, ok)
	event.Rewind()
	return &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       event.CommitTs,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}
}
