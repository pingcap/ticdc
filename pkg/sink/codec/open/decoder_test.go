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
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestDecoderMessageSchemaIsIndependent(t *testing.T) {
	d := &decoder{}
	key := &messageKey{Schema: "test", Table: "t", Ts: 100}
	var first *commonType.TableInfo
	for i, tp := range []byte{mysql.TypeLong, mysql.TypeLonglong, mysql.TypeLong} {
		cols := map[string]column{"id": {Type: tp, Flag: primaryKeyFlag, Value: json.Number("1")}}
		info := d.queryTableInfo(key, &messageRow{Update: cols})
		require.Equal(t, tp, info.GetColumns()[0].GetType())
		common.RequireRowLocatorByPrimaryKey(t, info, "id")
		switch i {
		case 0:
			first = info
		case 2:
			require.Same(t, first, info, "old schemas remain reusable without receiving DDL")
		}
	}
	handle := map[string]column{"id": {Type: mysql.TypeLong, Flag: primaryKeyFlag, Value: json.Number("1")}}
	full := d.queryTableInfo(key, &messageRow{Update: map[string]column{
		"id": handle["id"], "v": {Type: mysql.TypeVarchar, Value: "row value"},
	}})
	deleted := d.assembleDMLEvent(key, &messageRow{Delete: handle})
	t.Cleanup(deleted.PostFlush)
	require.Same(t, full, deleted.TableInfo)
	require.True(t, deleted.Rows.GetRow(0).IsNull(1))
	require.Equal(t, int64(1), deleted.Rows.GetRow(0).GetInt64(0))
	common.RequireRowLocatorByPrimaryKey(t, deleted.TableInfo, "id")
	// Neither a changed key type nor a new composite locator may use the old layout.
	changed := map[string]column{"id": {Type: mysql.TypeLonglong, Flag: primaryKeyFlag}}
	require.NotSame(t, full, d.queryTableInfo(key, &messageRow{Delete: changed}))
	d.queryTableInfo(key, &messageRow{Update: map[string]column{
		"id": handle["id"], "other": {Type: mysql.TypeLong, Flag: primaryKeyFlag},
	}})
	partial := d.queryTableInfo(key, &messageRow{Delete: handle})
	require.Len(t, partial.GetColumns(), 1, "missing locator columns must not be filled with NULL")
	for _, key := range d.tables.Keys() {
		value, _ := d.tables.Peek(key)
		for _, col := range value.(*cachedTable).columns {
			require.Nil(t, col.Value)
		}
	}
	for i := range decoderTableCacheSize + 1 {
		key.Table = fmt.Sprint(i)
		d.queryTableInfo(key, &messageRow{Update: handle})
	}
	require.Equal(t, decoderTableCacheSize, d.tables.Len())
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
		mock.ExpectQuery(`select \* from test.t where id = 1`).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
			sqlmock.NewColumn("e").OfType("ENUM", ""),
			sqlmock.NewColumn("id").OfType("INT", int64(0)),
			sqlmock.NewColumn("s").OfType("SET", ""),
		).AddRow("second", int64(1), "last"))
		mock.ExpectExec("set @@tidb_snapshot=" + ts).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery("select CAST\\(`e` AS UNSIGNED\\) AS `e`,`id`,CAST\\(`s` AS UNSIGNED\\) AS `s` from test.t where id = 1").WillReturnRows(sqlmock.NewRows([]string{"e", "id", "s"}).AddRow(int64(2), int64(1), []byte("9223372036854775808")))
	}
	update := d.assembleHandleKeyOnlyDMLEvent(t.Context(), key, &messageRow{
		Update: map[string]column{"id": cols["id"]}, PreColumns: map[string]column{"id": cols["id"]},
	})
	require.NoError(t, mock.ExpectationsWereMet())
	require.Equal(t, len(insert.TableInfo.GetColumns()), len(update.TableInfo.GetColumns()))
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
			// Snapshot metadata must be independent of earlier messages.
			oldKey := &messageKey{Schema: "test", Table: "t", Ts: 50}
			oldInfo := d.newTableInfo(oldKey, &messageRow{Update: map[string]column{
				"id": {Type: mysql.TypeLong, Flag: primaryKeyFlag},
				"e":  {Type: mysql.TypeVarchar},
			}})
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

func TestSnapshotSchemaCacheUsesTimestamp(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	d := &decoder{upstreamTiDB: db}
	key := &messageKey{Schema: "test", Table: "t", Ts: 100}
	conditions := map[string]any{"id": int64(1)}
	handle := map[string]column{"id": {Type: mysql.TypeLong, Flag: primaryKeyFlag, Value: int64(1)}}
	mock.ExpectExec("set @@tidb_snapshot=100").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`select \* from test.t where id = 1`).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("id").OfType("INT", int64(0)), sqlmock.NewColumn("v").OfType("VARCHAR", ""),
	).AddRow(int64(1), "first"))
	first := d.snapshotColumns(t.Context(), key, 100, conditions, handle)
	mock.ExpectExec("set @@tidb_snapshot=100").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("select `id`,`v` from test.t where id = 1").WillReturnRows(sqlmock.NewRows([]string{"id", "v"}).AddRow(int64(1), "second"))
	second := d.snapshotColumns(t.Context(), key, 100, conditions, handle)
	require.Equal(t, "first", first["v"].Value)
	require.Equal(t, "second", second["v"].Value)
	// A different timestamp discovers the new schema even if this decoder saw no DDL.
	mock.ExpectExec("set @@tidb_snapshot=200").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`select \* from test.t where id = 1`).WillReturnRows(sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("id").OfType("INT", int64(0)), sqlmock.NewColumn("new").OfType("BIGINT", int64(0)),
	).AddRow(int64(1), int64(2)))
	third := d.snapshotColumns(t.Context(), key, 200, conditions, map[string]column{"id": handle["id"]})
	require.Contains(t, third, "new")
	require.NotContains(t, third, "v")
	require.NoError(t, mock.ExpectationsWereMet())
}
