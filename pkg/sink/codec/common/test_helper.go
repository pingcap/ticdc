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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/sqlmodel"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func AppendRow2Chunk(data map[string]any, columns []*model.ColumnInfo, chk *chunk.Chunk) {
	for idx, col := range columns {
		value := data[col.Name.O]
		appendCol2Chunk(idx, value, col.FieldType, chk)
	}
}

func appendCol2Chunk(idx int, raw interface{}, ft types.FieldType, chk *chunk.Chunk) {
	if raw == nil {
		chk.AppendNull(idx)
		return
	}
	switch ft.GetType() {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			chk.AppendUint64(idx, raw.(uint64))
			return
		}
		chk.AppendInt64(idx, raw.(int64))
	case mysql.TypeYear:
		chk.AppendInt64(idx, raw.(int64))
	case mysql.TypeFloat:
		chk.AppendFloat32(idx, raw.(float32))
	case mysql.TypeDouble:
		chk.AppendFloat64(idx, raw.(float64))
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		chk.AppendBytes(idx, raw.([]byte))
	case mysql.TypeNewDecimal:
		chk.AppendMyDecimal(idx, raw.(*tiTypes.MyDecimal))
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		chk.AppendTime(idx, raw.(tiTypes.Time))
	case mysql.TypeDuration:
		chk.AppendDuration(idx, raw.(tiTypes.Duration))
	case mysql.TypeEnum:
		chk.AppendEnum(idx, raw.(tiTypes.Enum))
	case mysql.TypeSet:
		chk.AppendSet(idx, raw.(tiTypes.Set))
	case mysql.TypeBit:
		chk.AppendBytes(idx, raw.(tiTypes.BinaryLiteral))
	case mysql.TypeJSON:
		chk.AppendJSON(idx, raw.(tiTypes.BinaryJSON))
	case mysql.TypeTiDBVectorFloat32:
		chk.AppendVectorFloat32(idx, raw.(tiTypes.VectorFloat32))
	default:
		log.Panic("unknown column type", zap.Any("type", ft.GetType()), zap.Any("raw", raw))
	}
}

func compareDatum(t *testing.T, a, b *tiTypes.Datum, col *model.ColumnInfo) {
	if a.IsNull() && b.IsNull() {
		return
	}
	if a.IsNull() || b.IsNull() {
		require.Fail(t, "one of the datum is null", zap.Any("a", a), zap.Any("b", b))
		return
	}

	switch col.GetType() {
	case mysql.TypeNewDecimal:
		require.Equal(t, a.GetMysqlDecimal().String(), b.GetMysqlDecimal().String())
	case mysql.TypeEnum:
		require.Equal(t, a.GetMysqlEnum().Value, b.GetMysqlEnum().Value)
	case mysql.TypeSet:
		require.Equal(t, a.GetMysqlSet().Value, b.GetMysqlSet().Value)
	case mysql.TypeBit:
		require.Equal(t, a.GetMysqlBit().Compare(b.GetMysqlBit()), 0)
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDate:
		require.Equal(t, a.GetMysqlTime().Compare(b.GetMysqlTime()), 0)
	case mysql.TypeDuration:
		require.Equal(t, a.GetMysqlDuration().Compare(b.GetMysqlDuration()), 0)
	case mysql.TypeString:
		if mysql.HasBinaryFlag(col.GetFlag()) {
			actual := a.GetString()
			expected := b.GetString()
			length := min(len(actual), len(expected))
			require.Equal(t, actual[:length], expected[:length])
		} else {
			require.Equal(t, a.GetString(), b.GetString())
		}
	default:
		require.Equal(t, a.GetValue(), b.GetValue(), "type: %v", col.GetType())
	}
}

func CompareRow(
	t *testing.T,
	origin commonEvent.RowChange,
	originTableInfo *commonType.TableInfo,
	obtained commonEvent.RowChange,
	obtainedTableInfo *commonType.TableInfo,
) {
	if !origin.Row.IsEmpty() {
		a := origin.Row.GetDatumRow(originTableInfo.GetFieldSlice())
		b := obtained.Row.GetDatumRow(obtainedTableInfo.GetFieldSlice())
		require.Equal(t, len(a), len(b))
		for idx, col := range originTableInfo.GetColumns() {
			colID := obtainedTableInfo.ForceGetColumnIDByName(col.Name.O)
			offset := obtainedTableInfo.MustGetColumnOffsetByID(colID)
			compareDatum(t, &a[idx], &b[offset], col)
		}
	}

	if !origin.PreRow.IsEmpty() {
		a := origin.PreRow.GetDatumRow(originTableInfo.GetFieldSlice())
		b := obtained.PreRow.GetDatumRow(obtainedTableInfo.GetFieldSlice())
		require.Equal(t, len(a), len(b))
		for idx, col := range originTableInfo.GetColumns() {
			colID := obtainedTableInfo.ForceGetColumnIDByName(col.Name.O)
			offset := obtainedTableInfo.MustGetColumnOffsetByID(colID)
			compareDatum(t, &a[idx], &b[offset], col)
		}
	}
}

// RequireRowLocatorByPrimaryKey asserts that an update of the given table info is
// located by its primary key columns. The MySQL sink builds the WHERE clause of
// an UPDATE from the handle key of the table info a decoder produced, so this is
// the contract that table info has to satisfy: when the handle key is not the
// primary key the sink scans the table by an unindexed column instead.
func RequireRowLocatorByPrimaryKey(t require.TestingT, tableInfo *commonType.TableInfo, primaryKeys ...string) {
	columns := tableInfo.GetColumns()
	preValues := make([]interface{}, 0, len(columns))
	postValues := make([]interface{}, 0, len(columns))
	for _, column := range columns {
		preValues = append(preValues, "pre_"+column.Name.O)
		postValues = append(postValues, "post_"+column.Name.O)
	}

	change := sqlmodel.NewRowChange(
		&tableInfo.TableName, nil, preValues, postValues, tableInfo, tableInfo, nil)
	sql, args := change.GenSQL(sqlmodel.DMLUpdate)

	predicates := make([]string, 0, len(primaryKeys))
	values := make([]interface{}, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		predicates = append(predicates, fmt.Sprintf("`%s` = ?", key))
		values = append(values, "pre_"+key)
	}
	require.Contains(t, sql, "WHERE "+strings.Join(predicates, " AND "))
	require.Equal(t, values, args[len(args)-len(primaryKeys):])
}
