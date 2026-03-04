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

package common

import (
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func newColumnInfoWithFieldType(tp byte, configure func(ft *types.FieldType)) *model.ColumnInfo {
	ft := types.NewFieldType(tp)
	if configure != nil {
		configure(ft)
	}
	return &model.ColumnInfo{
		FieldType: *ft,
		State:     model.StatePublic,
		Version:   model.CurrLatestColumnInfoVersion,
	}
}

func TestExtractColValNull(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeLonglong)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
	chk.AppendNull(0)

	row := chk.GetRow(0)
	col := newColumnInfoWithFieldType(mysql.TypeLonglong, nil)
	val := ExtractColVal(&row, col, 0)
	require.Nil(t, val)
}

func TestExtractColValTimeTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		tp        byte
		inputTime string
	}{
		{
			name:      "date",
			tp:        mysql.TypeDate,
			inputTime: "2020-01-02",
		},
		{
			name:      "datetime",
			tp:        mysql.TypeDatetime,
			inputTime: "2020-01-02 03:04:05",
		},
		{
			name:      "timestamp",
			tp:        mysql.TypeTimestamp,
			inputTime: "2020-01-02 03:04:05",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ft := types.NewFieldType(tc.tp)
			chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)

			parsed, err := types.ParseTime(types.DefaultStmtNoWarningContext, tc.inputTime, tc.tp, types.DefaultFsp)
			require.NoError(t, err)
			chk.AppendTime(0, parsed)

			row := chk.GetRow(0)
			col := newColumnInfoWithFieldType(tc.tp, nil)
			val := ExtractColVal(&row, col, 0)
			require.Equal(t, parsed.String(), val)
		})
	}
}

func TestExtractColValDuration(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeDuration)
	ft.SetDecimal(2)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)

	dur := 12*time.Hour + 34*time.Minute + 56*time.Second + 780*time.Millisecond + 90*time.Microsecond
	chk.AppendDuration(0, types.Duration{Duration: dur})

	row := chk.GetRow(0)
	col := newColumnInfoWithFieldType(mysql.TypeDuration, func(ft *types.FieldType) {
		ft.SetDecimal(2)
	})
	val := ExtractColVal(&row, col, 0)
	require.Equal(t, "12:34:56.78", val)
}

func TestExtractColValJSON(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeJSON)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)

	j, err := types.ParseBinaryJSONFromString(`{"key":"value"}`)
	require.NoError(t, err)
	chk.AppendJSON(0, j)

	row := chk.GetRow(0)
	col := newColumnInfoWithFieldType(mysql.TypeJSON, nil)
	val := ExtractColVal(&row, col, 0)
	require.Equal(t, j.String(), val)
}

func TestExtractColValDecimal(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeNewDecimal)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)

	dec := types.NewDecFromStringForTest("123.45")
	chk.AppendMyDecimal(0, dec)

	row := chk.GetRow(0)
	col := newColumnInfoWithFieldType(mysql.TypeNewDecimal, nil)
	val := ExtractColVal(&row, col, 0)
	require.Equal(t, "123.45", val)
}

func TestExtractColValEnumAndSet(t *testing.T) {
	t.Parallel()

	t.Run("enum", func(t *testing.T) {
		t.Parallel()

		ft := types.NewFieldType(mysql.TypeEnum)
		chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
		chk.AppendEnum(0, types.Enum{Name: "a", Value: 2})

		row := chk.GetRow(0)
		col := newColumnInfoWithFieldType(mysql.TypeEnum, nil)
		val := ExtractColVal(&row, col, 0)
		require.Equal(t, uint64(2), val)
	})

	t.Run("set", func(t *testing.T) {
		t.Parallel()

		ft := types.NewFieldType(mysql.TypeSet)
		chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
		chk.AppendSet(0, types.Set{Name: "a,b", Value: 3})

		row := chk.GetRow(0)
		col := newColumnInfoWithFieldType(mysql.TypeSet, nil)
		val := ExtractColVal(&row, col, 0)
		require.Equal(t, uint64(3), val)
	})
}

func TestExtractColValBit(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeBit)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
	chk.AppendBytes(0, []byte{0x41})

	row := chk.GetRow(0)
	col := newColumnInfoWithFieldType(mysql.TypeBit, nil)
	val := ExtractColVal(&row, col, 0)
	require.Equal(t, uint64(65), val)
}

func TestExtractColValBytesAndCharset(t *testing.T) {
	t.Parallel()

	t.Run("nonbinary charset returns string", func(t *testing.T) {
		t.Parallel()

		ft := types.NewFieldType(mysql.TypeVarchar)
		chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
		chk.AppendString(0, "abc")

		row := chk.GetRow(0)
		col := newColumnInfoWithFieldType(mysql.TypeVarchar, nil)
		val := ExtractColVal(&row, col, 0)
		require.Equal(t, "abc", val)
	})

	t.Run("binary charset keeps bytes", func(t *testing.T) {
		t.Parallel()

		ft := types.NewFieldType(mysql.TypeVarchar)
		ft.SetCharset(charset.CharsetBin)
		ft.SetCollate(charset.CollationBin)
		chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
		chk.AppendString(0, "abc")

		row := chk.GetRow(0)
		col := newColumnInfoWithFieldType(mysql.TypeVarchar, func(ft *types.FieldType) {
			ft.SetCharset(charset.CharsetBin)
			ft.SetCollate(charset.CollationBin)
		})
		val := ExtractColVal(&row, col, 0)
		require.Equal(t, []byte("abc"), val)
	})

	t.Run("empty bytes and nonbinary charset returns empty string", func(t *testing.T) {
		t.Parallel()

		ft := types.NewFieldType(mysql.TypeVarchar)
		chk := chunk.NewEmptyChunk([]*types.FieldType{ft})
		chk.AppendBytes(0, nil)

		row := chk.GetRow(0)
		col := newColumnInfoWithFieldType(mysql.TypeVarchar, nil)
		val := ExtractColVal(&row, col, 0)
		require.Equal(t, "", val)
	})

	t.Run("nil bytes and binary charset returns empty slice", func(t *testing.T) {
		t.Parallel()

		ft := types.NewFieldType(mysql.TypeBlob)
		chk := chunk.NewEmptyChunk([]*types.FieldType{ft})
		chk.AppendBytes(0, nil)

		row := chk.GetRow(0)
		col := newColumnInfoWithFieldType(mysql.TypeBlob, nil)
		val := ExtractColVal(&row, col, 0)
		b, ok := val.([]byte)
		require.True(t, ok)
		require.NotNil(t, b)
		require.Len(t, b, 0)
	})
}

func TestExtractColValFloatInvalid(t *testing.T) {
	t.Parallel()

	t.Run("float32 NaN and Inf become 0", func(t *testing.T) {
		t.Parallel()

		ft := types.NewFieldType(mysql.TypeFloat)
		chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 2)
		chk.AppendFloat32(0, float32(math.NaN()))
		chk.AppendFloat32(0, float32(math.Inf(1)))

		col := newColumnInfoWithFieldType(mysql.TypeFloat, nil)
		firstRow := chk.GetRow(0)
		secondRow := chk.GetRow(1)
		require.Equal(t, float32(0), ExtractColVal(&firstRow, col, 0))
		require.Equal(t, float32(0), ExtractColVal(&secondRow, col, 0))
	})

	t.Run("float64 NaN and Inf become 0", func(t *testing.T) {
		t.Parallel()

		ft := types.NewFieldType(mysql.TypeDouble)
		chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 2)
		chk.AppendFloat64(0, math.NaN())
		chk.AppendFloat64(0, math.Inf(-1))

		col := newColumnInfoWithFieldType(mysql.TypeDouble, nil)
		firstRow := chk.GetRow(0)
		secondRow := chk.GetRow(1)
		require.Equal(t, float64(0), ExtractColVal(&firstRow, col, 0))
		require.Equal(t, float64(0), ExtractColVal(&secondRow, col, 0))
	})
}

func TestExtractColValVectorFloat32(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeTiDBVectorFloat32)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)

	v := types.InitVectorFloat32(3)
	v.Elements()[0] = 1.5
	v.Elements()[1] = -2
	v.Elements()[2] = 3
	chk.AppendVectorFloat32(0, v)

	row := chk.GetRow(0)
	col := newColumnInfoWithFieldType(mysql.TypeTiDBVectorFloat32, nil)
	val := ExtractColVal(&row, col, 0)
	require.Equal(t, v.String(), val)
}

func TestExtractColValDefaultDatumValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		configure func(ft *types.FieldType)
		append    func(chk *chunk.Chunk)
		expected  interface{}
	}{
		{
			name:      "signed int",
			configure: nil,
			append: func(chk *chunk.Chunk) {
				chk.AppendInt64(0, -123)
			},
			expected: int64(-123),
		},
		{
			name: "unsigned int",
			configure: func(ft *types.FieldType) {
				ft.AddFlag(mysql.UnsignedFlag)
			},
			append: func(chk *chunk.Chunk) {
				chk.AppendUint64(0, 123)
			},
			expected: uint64(123),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ft := types.NewFieldType(mysql.TypeLonglong)
			if tc.configure != nil {
				tc.configure(ft)
			}
			chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1)
			tc.append(chk)

			row := chk.GetRow(0)
			col := newColumnInfoWithFieldType(mysql.TypeLonglong, tc.configure)
			val := ExtractColVal(&row, col, 0)
			require.Equal(t, tc.expected, val)
		})
	}
}
