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

package traffic

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestMeasureValueV1(t *testing.T) {
	jsonValue, err := types.ParseBinaryJSONFromString(`{"a":1}`)
	require.NoError(t, err)
	vector, err := types.ParseVectorFloat32("[1,2,3]")
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		tp    byte
		value any
		bytes uint64
	}{
		{"tiny", mysql.TypeTiny, int64(127), 1},
		{"short", mysql.TypeShort, int64(-32768), 2},
		{"medium", mysql.TypeInt24, int64(0), 3},
		{"int", mysql.TypeLong, int64(0), 4},
		{"bigint", mysql.TypeLonglong, ^uint64(0), 8},
		{"float", mysql.TypeFloat, float32(1), 4},
		{"double", mysql.TypeDouble, float64(1), 8},
		{"date", mysql.TypeDate, types.ZeroDate, 4},
		{"new date", mysql.TypeNewDate, types.ZeroDate, 4},
		{"datetime", mysql.TypeDatetime, types.ZeroDatetime, 8},
		{"timestamp", mysql.TypeTimestamp, types.ZeroTimestamp, 8},
		{"time", mysql.TypeDuration, types.Duration{}, 8},
		{"year", mysql.TypeYear, int64(2026), 2},
		{"char spaces", mysql.TypeString, "a  ", 3},
		{"varchar utf8", mysql.TypeVarchar, "你好", 6},
		{"varstring", mysql.TypeVarString, "abc", 3},
		{"tinyblob empty", mysql.TypeTinyBlob, []byte{}, 0},
		{"blob trailing zero", mysql.TypeBlob, []byte{65, 0, 0, 0}, 4},
		{"mediumblob", mysql.TypeMediumBlob, []byte{1}, 1},
		{"longblob", mysql.TypeLongBlob, []byte{1, 2}, 2},
		{"bit zero", mysql.TypeBit, []byte{0, 0}, 1},
		{"bit leading zero", mysql.TypeBit, []byte{0, 1}, 1},
		{"bit two bytes", mysql.TypeBit, []byte{0, 1, 0}, 2},
		{"enum", mysql.TypeEnum, types.Enum{Name: "large", Value: 2}, 5},
		{"set", mysql.TypeSet, types.Set{Name: "a,ccc", Value: 5}, 5},
		{"empty enum", mysql.TypeEnum, types.Enum{}, 0},
		{"empty set", mysql.TypeSet, types.Set{}, 0},
		// The expected JSON size is fixed by rule v1, not computed by the
		// dependency API in the assertion. A dependency format change must fail.
		// Object header 8 + key entry 6 + value entry 5 + key 1 + int64 8.
		{"json object", mysql.TypeJSON, jsonValue, 28},
		{"vector", mysql.TypeTiDBVectorFloat32, vector, 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := chunk.MutRowFromValues(tc.value).ToRow()
			n, err := MeasureValueV1(row, 0, tc.tp)
			require.NoError(t, err)
			require.Equal(t, tc.bytes, n)
			row = chunk.MutRowFromValues(nil).ToRow()
			n, err = MeasureValueV1(row, 0, tc.tp)
			require.NoError(t, err)
			require.Equal(t, uint64(1), n)
		})
	}
	_, err = MeasureValueV1(chunk.MutRowFromValues(1).ToRow(), 0, mysql.TypeGeometry)
	require.Error(t, err)
}

func TestMeasureDecimalV1(t *testing.T) {
	for _, tc := range []struct {
		value string
		bytes uint64
	}{
		{"0", 2},
		{"0.0000", 2},
		{"-0.0", 2},
		{"1.1", 3},
		{"1.1000", 3},
		{"01.100", 3},
		{"-1.1000", 3},
		{"12", 2},
		{"1.2", 3},
		{"0.001", 3},
		{"123456789", 5},
		{"1234567890", 6},
		{"99999999999999999999999999999999999999999999999999999999999999999", 30},
	} {
		t.Run(tc.value, func(t *testing.T) {
			decimal := new(types.MyDecimal)
			require.NoError(t, decimal.FromString([]byte(tc.value)))
			row := chunk.MutRowFromValues(decimal).ToRow()
			n, err := MeasureValueV1(row, 0, mysql.TypeNewDecimal)
			require.NoError(t, err)
			require.Equal(t, tc.bytes, n)
			require.Zero(t, testing.AllocsPerRun(10, func() {
				_, measureErr := MeasureValueV1(row, 0, mysql.TypeNewDecimal)
				if measureErr != nil {
					panic(measureErr)
				}
			}))
		})
	}
}
