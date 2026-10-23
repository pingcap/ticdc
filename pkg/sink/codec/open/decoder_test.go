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

package open

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestDecoderReusesTableInfo(t *testing.T) {
	d := &decoder{config: common.NewConfig(config.ProtocolOpen), idx: 1}
	key := &messageKey{Schema: "test", Table: "t", Ts: 100}
	cols := map[string]column{"id": {Type: mysql.TypeLong, Flag: primaryKeyFlag, Value: json.Number("1")}}
	first := d.queryTableInfo(key, &messageRow{Update: cols})
	require.Nil(t, d.tables[d.tableCacheKey(key)].columns["id"].Value)
	cols["id"] = column{Type: mysql.TypeLong, Flag: primaryKeyFlag, Value: json.Number("2")}
	require.Same(t, first, d.queryTableInfo(key, &messageRow{Update: cols}))
	// DDL messages can be repeated or arrive out of timestamp order.
	for _, ts := range []uint64{300, 200, 200} {
		k, v, err := encodeDDLEvent(&commonEvent.DDLEvent{
			SchemaName: "test", TableName: "t", FinishedTs: ts,
			Type: byte(timodel.ActionModifyColumn), Query: "alter table test.t modify id bigint primary key",
		}, d.config)
		require.NoError(t, err)
		d.AddKeyValue(k, v)
		typ, ok := d.HasNext()
		require.True(t, ok)
		require.Equal(t, common.MessageTypeDDL, typ)
		d.NextDDLEvent()
	}
	require.Equal(t, []uint64{200, 300}, d.ddlCommitTs[[2]string{"test", "t"}])
	key.Ts = 200
	require.Same(t, first, d.queryTableInfo(key, &messageRow{Update: cols}))
	key.Ts = 201
	cols["id"] = column{Type: mysql.TypeLonglong, Flag: primaryKeyFlag}
	second := d.queryTableInfo(key, &messageRow{Update: cols})
	require.NotSame(t, first, second)
	require.Equal(t, mysql.TypeLonglong, second.GetColumns()[0].GetType())
	key.Ts = 250
	require.Same(t, second, d.queryTableInfo(key, &messageRow{Update: cols}))
	key.Ts = 100
	cols["id"] = column{Type: mysql.TypeLong, Flag: primaryKeyFlag, Value: json.Number("1")}
	require.Same(t, first, d.queryTableInfo(key, &messageRow{Update: cols}))
	require.Equal(t, mysql.TypeLong, first.GetColumns()[0].GetType())
}

func TestDeleteReusesCompleteTableInfo(t *testing.T) {
	d := &decoder{}
	key := &messageKey{Schema: "test", Table: "t", Ts: 100}
	handle := map[string]column{"id": {Type: mysql.TypeLong, Flag: primaryKeyFlag, Value: json.Number("1")}}
	// A cold key-only delete does not seed the full-table cache.
	cold := d.queryTableInfo(key, &messageRow{Delete: handle})
	require.Len(t, cold.GetColumns(), 1)
	require.Empty(t, d.tables)
	cols := map[string]column{
		"id": handle["id"],
		"v":  {Type: mysql.TypeVarchar, Value: "value"},
	}
	full := d.queryTableInfo(key, &messageRow{Update: cols})
	deleted := d.assembleDMLEvent(key, &messageRow{Delete: handle})
	require.Same(t, full, deleted.TableInfo)
	row := deleted.Rows.GetRow(0)
	require.Equal(t, int64(1), commonType.ExtractColVal(&row, full.GetColumns()[0], 0))
	require.True(t, row.IsNull(1))
}

func TestHandleKeyOnlyPreservesEnumSetTypes(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	d := &decoder{upstreamTiDB: db}
	key := &messageKey{Schema: "test", Table: "t", Ts: 100}
	cols := map[string]column{
		"id": {Type: mysql.TypeLong, Flag: primaryKeyFlag, Value: json.Number("1")},
		"e":  {Type: mysql.TypeEnum, Value: json.Number("1")},
		"s":  {Type: mysql.TypeSet, Value: json.Number("3")},
	}
	insert := d.assembleDMLEvent(key, &messageRow{Update: cols})
	for _, ts := range []string{"99", "100"} {
		mock.ExpectExec("set @@tidb_snapshot=" + ts).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("select CAST\\(`e` AS UNSIGNED\\) AS `e`,`id`,CAST\\(`s` AS UNSIGNED\\) AS `s` from test.t where id = 1").WillReturnRows(sqlmock.NewRows([]string{"e", "id", "s"}).AddRow(int64(2), int64(1), []byte("9223372036854775808")))
	}
	update := d.assembleHandleKeyOnlyDMLEvent(t.Context(), key, &messageRow{
		Update: map[string]column{"id": cols["id"]}, PreColumns: map[string]column{"id": cols["id"]},
	})
	require.NoError(t, mock.ExpectationsWereMet())
	require.Same(t, insert.TableInfo, update.TableInfo)
	// A batch can use the first event's table info to read every later row.
	for i := range update.Rows.NumRows() {
		row := update.Rows.GetRow(i)
		require.Equal(t, uint64(2), commonType.ExtractColVal(&row, insert.TableInfo.GetColumns()[0], 0))
		require.Equal(t, uint64(1)<<63, commonType.ExtractColVal(&row, insert.TableInfo.GetColumns()[2], 2))
	}
}

func TestHandleKeyOnlyColdCache(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		t.Run(fmt.Sprint(nullable), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			d := &decoder{upstreamTiDB: db}
			// The new interval must discover its own schema even with an old
			// interval already cached for this table.
			oldKey := &messageKey{Schema: "test", Table: "t", Ts: 50}
			oldInfo := d.queryTableInfo(oldKey, &messageRow{Update: map[string]column{
				"id": {Type: mysql.TypeLong, Flag: primaryKeyFlag},
				"e":  {Type: mysql.TypeVarchar},
			}})
			d.addDDLCommitTs("test", "t", 90)
			mock.ExpectExec("set @@tidb_snapshot=100").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(`select \* from test.t where id = 1`).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("e").OfType("ENUM", "").Nullable(true),
				sqlmock.NewColumn("id").OfType("INT", int64(0)),
			).AddRow("second", int64(1)))
			var enumValue driver.Value = int64(2)
			if nullable {
				enumValue = nil
			}
			mock.ExpectExec("set @@tidb_snapshot=100").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery("select CAST\\(`e` AS UNSIGNED\\) AS `e`,`id` from test.t where id = 1").WillReturnRows(sqlmock.NewRows([]string{"e", "id"}).AddRow(enumValue, int64(1)))
			event := d.assembleHandleKeyOnlyDMLEvent(t.Context(), &messageKey{Schema: "test", Table: "t", Ts: 100}, &messageRow{Update: map[string]column{
				"id": {Type: mysql.TypeLong, Flag: primaryKeyFlag, Value: json.Number("1")},
			}})
			require.NoError(t, mock.ExpectationsWereMet())
			require.Equal(t, mysql.TypeEnum, event.TableInfo.GetColumns()[0].GetType())
			require.NotSame(t, oldInfo, event.TableInfo)
			require.Equal(t, mysql.TypeVarchar, oldInfo.GetColumns()[0].GetType())
			row := event.Rows.GetRow(0)
			got := commonType.ExtractColVal(&row, event.TableInfo.GetColumns()[0], 0)
			if nullable {
				require.Nil(t, got)
			} else {
				require.Equal(t, uint64(2), got)
			}
		})
	}
}
