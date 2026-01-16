// Copyright 2023 PingCAP, Inc.
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

package canal

import (
	"testing"

	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func newTestColumnInfo(name string, tp byte, flags uint) *timodel.ColumnInfo {
	ft := types.NewFieldType(tp)
	ft.AddFlag(flags)
	return &timodel.ColumnInfo{
		ID:        1,
		Name:      ast.NewCIStr(name),
		FieldType: *ft,
		State:     timodel.StatePublic,
		Version:   timodel.CurrLatestColumnInfoVersion,
	}
}

func makeOneColumnRow(columnInfo *timodel.ColumnInfo, appendValue func(chk *chunk.Chunk)) chunk.Row {
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{&columnInfo.FieldType}, 1)
	appendValue(chk)
	return chk.GetRow(0)
}

func TestGetMySQLTypeUnsignedAndLength(t *testing.T) {
	t.Parallel()

	signed := newTestColumnInfo("a", mysql.TypeLong, 0)
	require.Equal(t, "int", codecCommon.GetMySQLType(signed, false))
	require.Equal(t, "int(11)", codecCommon.GetMySQLType(signed, true))

	unsigned := newTestColumnInfo("a", mysql.TypeLong, mysql.UnsignedFlag)
	require.Equal(t, "int unsigned", codecCommon.GetMySQLType(unsigned, false))
	require.Equal(t, "int(11) unsigned", codecCommon.GetMySQLType(unsigned, true))
}

func TestFormatColumnValueIntegerJavaTypeAdjust(t *testing.T) {
	t.Parallel()

	// tinyint unsigned can be widened to SMALLINT when overflow int8.
	tinyUnsigned := newTestColumnInfo("a", mysql.TypeTiny, mysql.UnsignedFlag)
	row := makeOneColumnRow(tinyUnsigned, func(chk *chunk.Chunk) {
		chk.AppendUint64(0, 128)
	})
	s, javaType := formatColumnValue(&row, 0, tinyUnsigned)
	require.False(t, s.isNull)
	require.Equal(t, "128", s.value)
	require.Equal(t, codecCommon.JavaSQLTypeSMALLINT, javaType)

	// int unsigned can be widened to BIGINT when overflow int32.
	intUnsigned := newTestColumnInfo("a", mysql.TypeLong, mysql.UnsignedFlag)
	row = makeOneColumnRow(intUnsigned, func(chk *chunk.Chunk) {
		chk.AppendUint64(0, uint64(2147483648))
	})
	s, javaType = formatColumnValue(&row, 0, intUnsigned)
	require.False(t, s.isNull)
	require.Equal(t, "2147483648", s.value)
	require.Equal(t, codecCommon.JavaSQLTypeBIGINT, javaType)

	// bigint unsigned can be widened to DECIMAL when overflow int64.
	bigintUnsigned := newTestColumnInfo("a", mysql.TypeLonglong, mysql.UnsignedFlag)
	row = makeOneColumnRow(bigintUnsigned, func(chk *chunk.Chunk) {
		chk.AppendUint64(0, uint64(9223372036854775808))
	})
	s, javaType = formatColumnValue(&row, 0, bigintUnsigned)
	require.False(t, s.isNull)
	require.Equal(t, "9223372036854775808", s.value)
	require.Equal(t, codecCommon.JavaSQLTypeDECIMAL, javaType)
}

func TestFormatColumnValueBinaryString(t *testing.T) {
	t.Parallel()

	binaryString := newTestColumnInfo("a", mysql.TypeString, mysql.BinaryFlag)
	row := makeOneColumnRow(binaryString, func(chk *chunk.Chunk) {
		chk.AppendBytes(0, []byte("abc"))
	})
	s, javaType := formatColumnValue(&row, 0, binaryString)
	require.False(t, s.isNull)
	require.Equal(t, "abc", s.value)
	require.Equal(t, codecCommon.JavaSQLTypeBLOB, javaType)

	plainString := newTestColumnInfo("a", mysql.TypeString, 0)
	row = makeOneColumnRow(plainString, func(chk *chunk.Chunk) {
		chk.AppendBytes(0, []byte("abc"))
	})
	s, javaType = formatColumnValue(&row, 0, plainString)
	require.False(t, s.isNull)
	require.Equal(t, "abc", s.value)
	require.Equal(t, codecCommon.JavaSQLTypeCHAR, javaType)
}
