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

package lake

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func nativeTestInfo() *common.TableInfo {
	id := types.NewFieldType(mysql.TypeLonglong)
	id.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	value := types.NewFieldType(mysql.TypeVarchar)
	value.SetCharset("utf8mb4")
	return common.WrapTableInfo("lake", &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true, Columns: []*model.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), FieldType: *id, State: model.StatePublic}, {ID: 2, Name: ast.NewCIStr("value"), FieldType: *value, State: model.StatePublic}}})
}

func nativeTestEvent(info *common.TableInfo, ts uint64, n int) *commonEvent.DMLEvent {
	e := commonEvent.NewDMLEvent(common.NewDispatcherID(), 1, ts-1, ts, info)
	e.Rows = chunk.NewChunkWithCapacity(info.GetFieldSlice(), n)
	for i := range n {
		e.Rows.AppendInt64(0, int64(i))
		e.Rows.AppendString(1, "payload")
		e.RowTypes = append(e.RowTypes, common.RowTypeInsert)
	}
	e.Length = int32(n)
	return e
}

func TestNativeDatumAdapterAndRawChangeContract(t *testing.T) {
	info := nativeTestInfo()
	enumType := types.NewFieldType(mysql.TypeEnum)
	enumType.SetElems([]string{"2", "x"})
	info = common.WrapTableInfo("lake", &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true, Columns: []*model.ColumnInfo{info.GetColumns()[0], {ID: 2, Name: ast.NewCIStr("value"), FieldType: *enumType, State: model.StatePublic}}})
	s, err := schemaFromNative(info, "", "")
	require.NoError(t, err)
	chk := chunk.NewChunkWithCapacity(info.GetFieldSlice(), 2)
	chk.AppendInt64(0, 1)
	chk.AppendEnum(1, types.Enum{Name: "2", Value: 1})
	chk.AppendInt64(0, 2)
	chk.AppendEnum(1, types.Enum{Name: "x", Value: 2})
	row, err := normalizeChange(info, s, commonEvent.RowChange{Row: chk.GetRow(0), RowType: common.RowTypeInsert}, 100, time.UTC)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.payload), &payload))
	require.Equal(t, "2", payload["value"])
	_, err = normalizeChange(info, s, commonEvent.RowChange{Row: chk.GetRow(1), PreRow: chk.GetRow(0), RowType: common.RowTypeUpdate}, 100, time.UTC)
	require.ErrorContains(t, err, "split change mode")
	// The normal mode keeps split DELETE and INSERT as distinct, ordered actions.
	del, err := normalizeChange(info, s, commonEvent.RowChange{PreRow: chk.GetRow(0), RowType: common.RowTypeDelete}, 100, time.UTC)
	require.NoError(t, err)
	ins, err := normalizeChange(info, s, commonEvent.RowChange{Row: chk.GetRow(1), RowType: common.RowTypeInsert}, 100, time.UTC)
	require.NoError(t, err)
	require.Equal(t, "1", del.id)
	require.Equal(t, "2", ins.id)
}

func TestNativeTypeMapping(t *testing.T) {
	for _, tc := range []struct {
		typ      byte
		unsigned bool
		want     string
	}{
		{mysql.TypeTiny, false, "INT8"}, {mysql.TypeTiny, true, "UINT8"}, {mysql.TypeShort, false, "INT16"}, {mysql.TypeLong, true, "UINT32"}, {mysql.TypeLonglong, true, "UINT64"}, {mysql.TypeYear, false, "INT16"}, {mysql.TypeDuration, false, "VARCHAR"}, {mysql.TypeDate, false, "DATE"}, {mysql.TypeTimestamp, false, "TIMESTAMP"}, {mysql.TypeJSON, false, "VARIANT"},
	} {
		ft := types.NewFieldType(tc.typ)
		if tc.unsigned {
			ft.SetFlag(mysql.UnsignedFlag)
		}
		typ, _, err := nativeColumnType(&model.ColumnInfo{FieldType: *ft})
		require.NoError(t, err)
		require.Equal(t, tc.want, typ)
	}
	ft := types.NewFieldType(mysql.TypeNewDecimal)
	ft.SetFlen(65)
	ft.SetDecimal(30)
	typ, _, err := nativeColumnType(&model.ColumnInfo{FieldType: *ft})
	require.NoError(t, err)
	require.Equal(t, "DECIMAL(65,30)", typ)
	ft = types.NewFieldType(mysql.TypeBlob)
	ft.SetCharset("binary")
	typ, enc, err := nativeColumnType(&model.ColumnInfo{FieldType: *ft})
	require.NoError(t, err)
	require.Equal(t, "BINARY", typ)
	require.Equal(t, binaryColumn, enc)
}

func TestNativeDatumValuesPreserveExactRepresentations(t *testing.T) {
	base := nativeTestInfo().GetColumns()[0]
	makeColumn := func(id int64, name string, tp byte) *model.ColumnInfo {
		ft := types.NewFieldType(tp)
		return &model.ColumnInfo{ID: id, Name: ast.NewCIStr(name), FieldType: *ft, State: model.StatePublic}
	}
	binary := makeColumn(2, "binary", mysql.TypeBlob)
	binary.SetCharset("binary")
	decimal := makeColumn(3, "decimal", mysql.TypeNewDecimal)
	decimal.SetFlen(65)
	decimal.SetDecimal(30)
	document := makeColumn(4, "document", mysql.TypeJSON)
	set := makeColumn(5, "set_label", mysql.TypeSet)
	elems := make([]string, 64)
	for i := range elems {
		elems[i] = strconv.Itoa(i)
	}
	set.SetElems(elems)
	stamp := makeColumn(6, "stamp", mysql.TypeTimestamp)
	stamp.SetDecimal(6)
	null := makeColumn(7, "sql_null", mysql.TypeJSON)
	jsonNull := makeColumn(8, "json_null", mysql.TypeJSON)
	info := common.WrapTableInfo("lake", &model.TableInfo{ID: 1, Name: ast.NewCIStr("typed"), PKIsHandle: true, Columns: []*model.ColumnInfo{base, binary, decimal, document, set, stamp, null, jsonNull}})
	s, err := schemaFromNative(info, "", "")
	require.NoError(t, err)
	chk := chunk.NewChunkWithCapacity(info.GetFieldSlice(), 1)
	chk.AppendInt64(0, 1)
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}
	chk.AppendBytes(1, data)
	dec := new(types.MyDecimal)
	require.NoError(t, dec.FromString([]byte("12345678901234567890.123456789")))
	chk.AppendMyDecimal(2, dec)
	doc, err := types.ParseBinaryJSONFromString(`{"uint":18446744073709551615}`)
	require.NoError(t, err)
	chk.AppendJSON(3, doc)
	chk.AppendSet(4, types.Set{Name: "63", Value: uint64(1) << 63})
	chk.AppendTime(5, types.NewTime(types.FromDate(2026, 10, 25, 1, 2, 3, 123456), mysql.TypeTimestamp, 6))
	chk.AppendNull(6)
	nilDoc, err := types.ParseBinaryJSONFromString("null")
	require.NoError(t, err)
	chk.AppendJSON(7, nilDoc)
	row, err := normalizeChange(info, s, commonEvent.RowChange{Row: chk.GetRow(0), RowType: common.RowTypeInsert}, 100, time.UTC)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.payload), &payload))
	require.Equal(t, hex.EncodeToString(data), payload["binary"])
	require.Equal(t, "12345678901234567890.123456789", payload["decimal"])
	require.Equal(t, doc.String(), payload["document"])
	require.Equal(t, "63", payload["set_label"])
	require.Equal(t, "2026-10-25 01:02:03.123456", payload["stamp"])
	require.Nil(t, payload["sql_null"])
	require.Equal(t, "null", payload["json_null"])
}

func TestNativeDefaultEncoding(t *testing.T) {
	col := nativeTestInfo().GetColumns()[1].Clone()
	col.DefaultValue = []byte("a'\\b")
	def, expr, err := nativeDefault(col, scalarColumn)
	require.NoError(t, err)
	require.False(t, expr)
	require.Equal(t, `'a''\\b'`, def)
	def, expr, err = nativeDefault(col, binaryColumn)
	require.NoError(t, err)
	require.False(t, expr)
	require.Equal(t, `unhex('61275c62')`, def)
	ft := types.NewFieldType(mysql.TypeBit)
	col.FieldType = *ft
	require.NoError(t, col.SetDefaultValue(string([]byte{0xff, 0xff})))
	def, expr, err = nativeDefault(col, scalarColumn)
	require.NoError(t, err)
	require.False(t, expr)
	require.Equal(t, "65535", def)
	ft = types.NewFieldType(mysql.TypeTimestamp)
	col.FieldType = *ft
	col.DefaultValue = "CURRENT_TIMESTAMP"
	def, expr, err = nativeDefault(col, scalarColumn)
	require.NoError(t, err)
	require.True(t, expr)
	require.Empty(t, def)
}

func TestNativeTimestampUsesSourceTimezoneButDatetimeKeepsWallClock(t *testing.T) {
	id := nativeTestInfo().GetColumns()[0]
	timestampType := types.NewFieldType(mysql.TypeTimestamp)
	timestampType.SetDecimal(6)
	datetimeType := types.NewFieldType(mysql.TypeDatetime)
	datetimeType.SetDecimal(6)
	info := common.WrapTableInfo("lake", &model.TableInfo{ID: 1, Name: ast.NewCIStr("time_values"), PKIsHandle: true, Columns: []*model.ColumnInfo{id, {ID: 2, Name: ast.NewCIStr("stamp"), FieldType: *timestampType, State: model.StatePublic}, {ID: 3, Name: ast.NewCIStr("wall"), FieldType: *datetimeType, State: model.StatePublic}}})
	schema, err := schemaFromNative(info, "", "")
	require.NoError(t, err)
	chk := chunk.NewChunkWithCapacity(info.GetFieldSlice(), 1)
	chk.AppendInt64(0, 1)
	chk.AppendTime(1, types.NewTime(types.FromDate(2026, 10, 25, 8, 2, 3, 456789), mysql.TypeTimestamp, 6))
	chk.AppendTime(2, types.NewTime(types.FromDate(2026, 10, 25, 8, 2, 3, 456789), mysql.TypeDatetime, 6))
	row, err := normalizeChange(info, schema, commonEvent.RowChange{Row: chk.GetRow(0), RowType: common.RowTypeInsert}, 100, time.FixedZone("UTC+8", 8*60*60))
	require.NoError(t, err)
	var values map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.payload), &values))
	require.Equal(t, "2026-10-25 00:02:03.456789", values["stamp"])
	require.Equal(t, "2026-10-25 08:02:03.456789", values["wall"])
}

// TiCDC's GetPrimaryKeyColumnNames intentionally falls back to a NOT NULL
// UNIQUE index. Do not describe or accidentally narrow this to declared PKs.
func TestNativeImplicitUniqueConflictKey(t *testing.T) {
	base := nativeTestInfo()
	value := *base.GetColumns()[1]
	value.Offset = 1
	id := *base.GetColumns()[0]
	id.FieldType = *types.NewFieldType(mysql.TypeLonglong)
	id.SetFlag(mysql.NotNullFlag)
	info := common.WrapTableInfo("lake", &model.TableInfo{
		ID: 2, Name: ast.NewCIStr("implicit_key"),
		Columns: []*model.ColumnInfo{&id, &value},
		Indices: []*model.IndexInfo{{
			ID: 1, Name: ast.NewCIStr("uk_id"), Unique: true, State: model.StatePublic,
			Columns: []*model.IndexColumn{{Name: id.Name, Offset: 0, Length: types.UnspecifiedLength}},
		}},
	})
	schema, err := schemaFromNative(info, "", "")
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, schema.primaryKey)
	nullableID := id
	nullableID.SetFlag(0)
	info = common.WrapTableInfo("lake", &model.TableInfo{
		ID: 3, Name: ast.NewCIStr("nullable_unique"), Columns: []*model.ColumnInfo{&nullableID, &value},
		Indices: []*model.IndexInfo{{
			ID: 1, Name: ast.NewCIStr("uk_id"), Unique: true, State: model.StatePublic,
			Columns: []*model.IndexColumn{{Name: id.Name, Offset: 0, Length: types.UnspecifiedLength}},
		}},
	})
	_, err = schemaFromNative(info, "", "")
	require.Error(t, err, "nullable UNIQUE is not an implicit conflict key")
}

func TestNativeConfiguredConflictKey(t *testing.T) {
	base := nativeTestInfo()
	id, value := *base.GetColumns()[0], *base.GetColumns()[1]
	id.SetFlag(0)
	value.Offset = 1
	info := common.WrapTableInfo("lake", &model.TableInfo{ID: 7, Name: ast.NewCIStr("keyless"), Columns: []*model.ColumnInfo{&id, &value}})
	_, err := schemaFromNative(info, "", "")
	require.Error(t, err)
	schema, err := schemaFromNative(info, "", "ID")
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, schema.primaryKey)
	row, err := normalizeNativeRow(schema, insertAction, 100, map[string]any{"id": 1, "value": "v"})
	require.NoError(t, err)
	require.NotEmpty(t, row.id)
	_, err = normalizeNativeRow(schema, insertAction, 100, map[string]any{"id": nil, "value": "v"})
	require.Error(t, err, "the configured column may be nullable in source metadata, but identity values must not be NULL")
	_, err = schemaFromNative(info, "", "missing")
	require.Error(t, err)
	// The explicit single-column override also matches bend-cdc on keyed tables.
	schema, err = schemaFromNative(base, "", "VALUE")
	require.NoError(t, err)
	require.Equal(t, []string{"value"}, schema.primaryKey)
	composite := common.WrapTableInfo("lake", &model.TableInfo{ID: 8, Name: ast.NewCIStr("composite"), Columns: []*model.ColumnInfo{&id, &value}, Indices: []*model.IndexInfo{{ID: 1, Primary: true, Unique: true, State: model.StatePublic, Columns: []*model.IndexColumn{{Name: id.Name, Offset: 0}, {Name: value.Name, Offset: 1}}}}})
	_, err = schemaFromNative(composite, "", "id")
	require.ErrorContains(t, err, "composite")
	// Arbitrary logical keys are not necessarily split by TiCDC's PK/UK scanner.
	// A mutated key in an unsplit UPDATE must stop instead of orphaning old rows.
	schema, err = schemaFromNative(info, "", "id")
	require.NoError(t, err)
	event := nativeTestEvent(info, 100, 2)
	_, err = normalizeChange(info, schema, commonEvent.RowChange{PreRow: event.Rows.GetRow(0), Row: event.Rows.GetRow(1), RowType: common.RowTypeUpdate}, 100, time.UTC)
	require.ErrorContains(t, err, "stable conflict-key")
}
