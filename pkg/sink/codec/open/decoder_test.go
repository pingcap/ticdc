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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestDecoderReusesTableInfo(t *testing.T) {
	d := &decoder{}
	key := &messageKey{Schema: "test", Table: "t"}
	cols := map[string]column{"id": {Type: mysql.TypeLong, Flag: primaryKeyFlag, Value: json.Number("1")}}
	first := d.queryTableInfo(key, &messageRow{Update: cols})
	require.Nil(t, d.tables[[2]string{"test", "t"}].columns["id"].Value)
	cols["id"] = column{Type: mysql.TypeLong, Flag: primaryKeyFlag, Value: json.Number("2")}
	require.Same(t, first, d.queryTableInfo(key, &messageRow{Update: cols}))
	cols["id"] = column{Type: mysql.TypeLonglong, Flag: primaryKeyFlag}
	require.NotSame(t, first, d.queryTableInfo(key, &messageRow{Update: cols}))
	require.Equal(t, mysql.TypeLong, first.GetColumns()[0].GetType())
	for _, changed := range []map[string]column{
		{"id": {Type: mysql.TypeLong, Flag: nullableFlag}},
		{"renamed": {Type: mysql.TypeLong, Flag: primaryKeyFlag}},
		{"id": {Type: mysql.TypeLong, Flag: primaryKeyFlag}, "added": {Type: mysql.TypeLong}},
	} {
		baseline := d.queryTableInfo(key, &messageRow{Update: map[string]column{"id": {Type: mysql.TypeLong, Flag: primaryKeyFlag}}})
		require.NotSame(t, baseline, d.queryTableInfo(key, &messageRow{Update: changed}))
	}
	for _, cached := range d.tables {
		for _, col := range cached.columns {
			require.Nil(t, col.Value)
		}
	}
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
