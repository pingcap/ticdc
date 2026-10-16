// Copyright 2025 PingCAP, Inc.
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

package common

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mysqlDriver "github.com/go-sql-driver/mysql"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/meta/model"
	parser_model "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestSanitizeName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "normalColumnName123", SanitizeName("normalColumnName123"))
	require.Equal(
		t,
		"_1ColumnNameStartWithNumber",
		SanitizeName("1ColumnNameStartWithNumber"),
	)
	require.Equal(t, "A_B", SanitizeName("A.B"))
	require.Equal(t, "columnNameWith__", SanitizeName("columnNameWith中文"))
}

func TestQueryRowChecksumQueryErrors(t *testing.T) {
	columns := []*model.ColumnInfo{{ID: 1, Name: parser_model.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)}}
	for _, name := range []string{"permission denied", "other unsupported function", "missing row", "invalid value"} {
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(context.Background())
			require.NoError(t, err)
			defer conn.Close()
			mock.ExpectExec("set @@tidb_snapshot=100").WillReturnResult(sqlmock.NewResult(0, 0))
			query := mock.ExpectQuery("select tidb_row_checksum() from test.t where id = 1")
			switch name {
			case "permission denied":
				query.WillReturnError(&mysqlDriver.MySQLError{Number: mysql.ErrTableaccessDenied, Message: "SELECT command denied"})
			case "other unsupported function":
				query.WillReturnError(&mysqlDriver.MySQLError{Number: mysql.ErrNotSupportedYet, Message: "another unsupported feature"})
			default:
				query.WillReturnError(&mysqlDriver.MySQLError{
					Number:  mysql.ErrNotSupportedYet,
					Message: "FUNCTION tidb_row_checksum can only be used as a select field in a fast point plan",
				})
				mock.ExpectQuery("select @@session.time_zone").
					WillReturnRows(sqlmock.NewRows([]string{"time_zone"}).AddRow("SYSTEM"))
				mock.ExpectExec("set @@session.time_zone = '+00:00'").WillReturnResult(sqlmock.NewResult(0, 0))
				rows := sqlmock.NewRows([]string{"id"})
				if name == "invalid value" {
					rows.AddRow("not an integer")
				}
				mock.ExpectQuery("select `id` from test.t where id = 1").WillReturnRows(rows)
				mock.ExpectExec("set @@session.time_zone = ?").WithArgs("SYSTEM").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			require.PanicsWithValue(t, "scan row failed", func() {
				queryRowChecksumAux(context.Background(), conn, 100, "test", "t", map[string]any{"id": 1}, columns)
			})
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestQueryRowChecksumDuringReorganization(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.DDL2Event("create table test.tp_int2 (id int primary key, v int)")
	event, _ := helper.DML2UpdateEvent("test", "tp_int2",
		"insert into test.tp_int2 values (1, 10)", "update test.tp_int2 set v = 20 where id = 1")
	checksum := func(value uint64) uint32 {
		buf := binary.LittleEndian.AppendUint64(nil, 1)
		return crc32.ChecksumIEEE(binary.LittleEndian.AppendUint64(buf, value))
	}
	event.CommitTs = 100
	event.Checksum = []*integrity.Checksum{{Current: checksum(20), Previous: checksum(10)}}
	for _, test := range []struct {
		name            string
		corruptSnapshot string
	}{
		{name: "matching"},
		{name: "current corrupted", corruptSnapshot: "set @@tidb_snapshot=100"},
		{name: "previous corrupted", corruptSnapshot: "set @@tidb_snapshot=99"},
	} {
		t.Run(test.name, func(t *testing.T) {
			event.Rewind()
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()
			for _, snapshot := range []struct {
				statement string
				value     int64
			}{{"set @@tidb_snapshot=100", 20}, {"set @@tidb_snapshot=99", 10}} {
				mock.ExpectExec(snapshot.statement).WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("select tidb_row_checksum() from test.tp_int2 where id = 1").
					WillReturnError(&mysqlDriver.MySQLError{
						Number:  mysql.ErrNotSupportedYet,
						Message: "FUNCTION tidb_row_checksum can only be used as a select field in a fast point plan",
					})
				mock.ExpectQuery("select @@session.time_zone").
					WillReturnRows(sqlmock.NewRows([]string{"time_zone"}).AddRow("+08:00"))
				mock.ExpectExec("set @@session.time_zone = '+00:00'").WillReturnResult(sqlmock.NewResult(0, 0))
				value := snapshot.value
				corrupted := snapshot.statement == test.corruptSnapshot
				if corrupted {
					value++
				}
				mock.ExpectQuery("select `id`,`v` from test.tp_int2 where id = 1").
					WillReturnRows(sqlmock.NewRows([]string{"id", "v"}).AddRow(int64(1), value))
				mock.ExpectExec("set @@session.time_zone = ?").WithArgs("+08:00").WillReturnResult(sqlmock.NewResult(0, 0))
				if corrupted {
					break
				}
			}
			err = VerifyChecksum(event, db, nil, nil)
			if test.corruptSnapshot != "" {
				require.ErrorContains(t, err, "checksum mismatch")
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestQueryRowChecksumPreservesNumericPrecision(t *testing.T) {
	// Reproduce the tp_real row from kafka_simple_basic. Protocol metadata can
	// use default numeric precision even though the SQL result has fractions.
	names := []string{"id", "c_float", "c_double", "c_decimal", "c_decimal_2"}
	typesList := []byte{mysql.TypeLong, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal, mysql.TypeNewDecimal}
	columns := make([]*model.ColumnInfo, len(names))
	for i, name := range names {
		ft := types.NewFieldType(typesList[i])
		ft.SetFlen(10)
		ft.SetDecimal(0)
		columns[i] = &model.ColumnInfo{ID: int64(i + 1), Name: parser_model.NewCIStr(name), FieldType: *ft}
	}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	mock.ExpectQuery("select @@session.time_zone").
		WillReturnRows(sqlmock.NewRows([]string{"time_zone"}).AddRow("SYSTEM"))
	mock.ExpectExec("set @@session.time_zone = '+00:00'").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("select `id`,`c_float`,`c_double`,`c_decimal`,`c_decimal_2` from test.tp_real where id = 2").
		WillReturnRows(sqlmock.NewRows(names).AddRow(int64(2), float64(2020.0201), float64(2020.0303), []byte("2020"), []byte("2021.1208")))
	mock.ExpectExec("set @@session.time_zone = ?").WithArgs("SYSTEM").WillReturnResult(sqlmock.NewResult(0, 0))
	actual, err := queryRowChecksumFromColumns(context.Background(), conn,
		"select tidb_row_checksum() from test.tp_real where id = 2", columns)
	require.NoError(t, err)
	// Independently obtained from TiDB's tidb_row_checksum() for this row.
	require.Equal(t, uint32(866891115), actual)
	require.Zero(t, columns[1].GetDecimal())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestQueryRowChecksumEnumAndSet(t *testing.T) {
	columns := []*model.ColumnInfo{
		{ID: 3, Name: parser_model.NewCIStr("s"), FieldType: *types.NewFieldType(mysql.TypeSet)},
		{ID: 1, Name: parser_model.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		{ID: 2, Name: parser_model.NewCIStr("e"), FieldType: *types.NewFieldType(mysql.TypeEnum)},
	}
	for _, null := range []bool{false, true} {
		t.Run(map[bool]string{false: "numeric values", true: "null values"}[null], func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(context.Background())
			require.NoError(t, err)
			defer conn.Close()
			mock.ExpectQuery("select @@session.time_zone").
				WillReturnRows(sqlmock.NewRows([]string{"time_zone"}).AddRow("SYSTEM"))
			mock.ExpectExec("set @@session.time_zone = '+00:00'").WillReturnResult(sqlmock.NewResult(0, 0))
			rows := sqlmock.NewRows([]string{"s", "id", "e"})
			// Checksum order follows column IDs, independently of the projection.
			encoded := binary.LittleEndian.AppendUint64(nil, 1)
			if null {
				rows.AddRow(nil, int64(1), nil)
			} else {
				// Preserve ENUM's empty value and SET's high bit without parsing names.
				rows.AddRow([]byte("18446744073709551615"), int64(1), int64(0))
				encoded = binary.LittleEndian.AppendUint64(encoded, 0)
				encoded = binary.LittleEndian.AppendUint64(encoded, ^uint64(0))
			}
			mock.ExpectQuery("select CAST(`s` AS UNSIGNED),`id`,CAST(`e` AS UNSIGNED) from test.t where id = 1").
				WillReturnRows(rows)
			mock.ExpectExec("set @@session.time_zone = ?").WithArgs("SYSTEM").WillReturnResult(sqlmock.NewResult(0, 0))
			actual, err := queryRowChecksumFromColumns(context.Background(), conn,
				"select tidb_row_checksum() from test.t where id = 1", columns)
			require.NoError(t, err)
			require.Equal(t, crc32.ChecksumIEEE(encoded), actual)
			require.Equal(t, mysql.TypeSet, columns[0].GetType())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestQueryRowChecksumGBK(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeVarchar)
	ft.SetCharset("gbk")
	ft.SetCollate("gbk_bin")
	columns := []*model.ColumnInfo{
		{ID: 1, Name: parser_model.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		{ID: 2, Name: parser_model.NewCIStr("name"), FieldType: *ft},
		{ID: 3, Name: parser_model.NewCIStr("image"), FieldType: *types.NewFieldType(mysql.TypeTinyBlob)},
	}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	mock.ExpectQuery("select @@session.time_zone").
		WillReturnRows(sqlmock.NewRows([]string{"time_zone"}).AddRow("SYSTEM"))
	mock.ExpectExec("set @@session.time_zone = '+00:00'").WillReturnResult(sqlmock.NewResult(0, 0))
	// SQL character results use UTF-8; binary columns retain their original bytes.
	name, blob := []byte("你好,世界"), []byte{0xC4, 0xE3, 0xBA, 0xC3}
	mock.ExpectQuery("select `id`,`name`,`image` from test.cs_gbk where id = 1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "image"}).AddRow(int64(1), name, blob))
	mock.ExpectExec("set @@session.time_zone = ?").WithArgs("SYSTEM").WillReturnResult(sqlmock.NewResult(0, 0))
	actual, err := queryRowChecksumFromColumns(context.Background(), conn,
		"select tidb_row_checksum() from test.cs_gbk where id = 1", columns)
	require.NoError(t, err)
	encoded := binary.LittleEndian.AppendUint64(nil, 1)
	encoded = append(binary.LittleEndian.AppendUint32(encoded, uint32(len(name))), name...)
	encoded = append(binary.LittleEndian.AppendUint32(encoded, uint32(len(blob))), blob...)
	require.Equal(t, crc32.ChecksumIEEE(encoded), actual)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVerifySparseChecksums(t *testing.T) {
	column := &model.ColumnInfo{ID: 1, Name: parser_model.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	tableInfo := commonType.NewTableInfo4Decoder("test", &model.TableInfo{ID: 1, Name: parser_model.NewCIStr("t"), Columns: []*model.ColumnInfo{column}})
	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 3)
	for i := range 3 {
		rows.AppendInt64(0, int64(i+1))
	}
	checksum, err := calculateChecksum(rows.GetRow(1), tableInfo.GetColumns(), nil)
	require.NoError(t, err)
	event := &commonEvent.DMLEvent{
		TableInfo: tableInfo, Rows: rows, Length: 3,
		RowTypes: []commonType.RowType{commonType.RowTypeInsert, commonType.RowTypeInsert, commonType.RowTypeInsert},
		Checksum: []*integrity.Checksum{nil, {Current: checksum}, nil},
	}
	require.NoError(t, VerifyChecksum(event, nil, nil, nil))
	event.Checksum[1].Current++
	require.ErrorContains(t, VerifyChecksum(event, nil, nil, nil), "current checksum mismatch")
}
