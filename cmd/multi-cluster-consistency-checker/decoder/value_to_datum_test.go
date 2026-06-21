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

package decoder

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestValueToDatum_NilValue(t *testing.T) {
	t.Parallel()
	ft := ptypes.NewFieldType(mysql.TypeLong)
	d := valueToDatum(nil, ft)
	require.True(t, d.IsNull())
}

func TestValueToDatum_NonStringPanics(t *testing.T) {
	t.Parallel()
	ft := ptypes.NewFieldType(mysql.TypeLong)
	require.Panics(t, func() {
		valueToDatum(123, ft)
	})
}

func TestValueToDatum_SignedIntegers(t *testing.T) {
	t.Parallel()

	intTypes := []struct {
		name string
		tp   byte
	}{
		{"TypeTiny", mysql.TypeTiny},
		{"TypeShort", mysql.TypeShort},
		{"TypeInt24", mysql.TypeInt24},
		{"TypeLong", mysql.TypeLong},
		{"TypeLonglong", mysql.TypeLonglong},
	}

	for _, it := range intTypes {
		t.Run(it.name, func(t *testing.T) {
			t.Parallel()
			ft := ptypes.NewFieldType(it.tp)

			t.Run("positive", func(t *testing.T) {
				t.Parallel()
				d := valueToDatum("42", ft)
				require.Equal(t, tiTypes.KindInt64, d.Kind())
				require.Equal(t, int64(42), d.GetInt64())
			})

			t.Run("zero", func(t *testing.T) {
				t.Parallel()
				d := valueToDatum("0", ft)
				require.Equal(t, tiTypes.KindInt64, d.Kind())
				require.Equal(t, int64(0), d.GetInt64())
			})

			t.Run("negative", func(t *testing.T) {
				t.Parallel()
				d := valueToDatum("-100", ft)
				require.Equal(t, tiTypes.KindInt64, d.Kind())
				require.Equal(t, int64(-100), d.GetInt64())
			})

			t.Run("max int64", func(t *testing.T) {
				t.Parallel()
				d := valueToDatum("9223372036854775807", ft)
				require.Equal(t, tiTypes.KindInt64, d.Kind())
				require.Equal(t, int64(math.MaxInt64), d.GetInt64())
			})

			t.Run("min int64", func(t *testing.T) {
				t.Parallel()
				d := valueToDatum("-9223372036854775808", ft)
				require.Equal(t, tiTypes.KindInt64, d.Kind())
				require.Equal(t, int64(math.MinInt64), d.GetInt64())
			})
		})
	}
}

func TestValueToDatum_UnsignedIntegers(t *testing.T) {
	t.Parallel()

	intTypes := []struct {
		name string
		tp   byte
	}{
		{"TypeTiny", mysql.TypeTiny},
		{"TypeShort", mysql.TypeShort},
		{"TypeInt24", mysql.TypeInt24},
		{"TypeLong", mysql.TypeLong},
		{"TypeLonglong", mysql.TypeLonglong},
	}

	for _, it := range intTypes {
		t.Run(it.name, func(t *testing.T) {
			t.Parallel()
			ft := ptypes.NewFieldType(it.tp)
			ft.AddFlag(mysql.UnsignedFlag)

			t.Run("positive", func(t *testing.T) {
				t.Parallel()
				d := valueToDatum("42", ft)
				require.Equal(t, tiTypes.KindUint64, d.Kind())
				require.Equal(t, uint64(42), d.GetUint64())
			})

			t.Run("zero", func(t *testing.T) {
				t.Parallel()
				d := valueToDatum("0", ft)
				require.Equal(t, tiTypes.KindUint64, d.Kind())
				require.Equal(t, uint64(0), d.GetUint64())
			})

			t.Run("max uint64", func(t *testing.T) {
				t.Parallel()
				d := valueToDatum("18446744073709551615", ft)
				require.Equal(t, tiTypes.KindUint64, d.Kind())
				require.Equal(t, uint64(math.MaxUint64), d.GetUint64())
			})
		})
	}
}

func TestValueToDatum_InvalidIntegerPanics(t *testing.T) {
	t.Parallel()

	t.Run("signed invalid", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeLong)
		require.Panics(t, func() {
			valueToDatum("not_a_number", ft)
		})
	})

	t.Run("unsigned invalid", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeLong)
		ft.AddFlag(mysql.UnsignedFlag)
		require.Panics(t, func() {
			valueToDatum("not_a_number", ft)
		})
	})
}

func TestValueToDatum_Year(t *testing.T) {
	t.Parallel()

	ft := ptypes.NewFieldType(mysql.TypeYear)

	t.Run("normal year", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("2026", ft)
		require.Equal(t, tiTypes.KindInt64, d.Kind())
		require.Equal(t, int64(2026), d.GetInt64())
	})

	t.Run("zero year", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("0", ft)
		require.Equal(t, tiTypes.KindInt64, d.Kind())
		require.Equal(t, int64(0), d.GetInt64())
	})

	t.Run("invalid year panics", func(t *testing.T) {
		t.Parallel()
		require.Panics(t, func() {
			valueToDatum("abc", ft)
		})
	})
}

func TestValueToDatum_Float(t *testing.T) {
	t.Parallel()

	ft := ptypes.NewFieldType(mysql.TypeFloat)

	t.Run("positive float", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("3.14", ft)
		require.Equal(t, tiTypes.KindFloat32, d.Kind())
		require.InDelta(t, float32(3.14), d.GetFloat32(), 0.001)
	})

	t.Run("negative float", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("-2.5", ft)
		require.Equal(t, tiTypes.KindFloat32, d.Kind())
		require.InDelta(t, float32(-2.5), d.GetFloat32(), 0.001)
	})

	t.Run("zero float", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("0", ft)
		require.Equal(t, tiTypes.KindFloat32, d.Kind())
		require.Equal(t, float32(0), d.GetFloat32())
	})

	t.Run("invalid float panics", func(t *testing.T) {
		t.Parallel()
		require.Panics(t, func() {
			valueToDatum("not_a_float", ft)
		})
	})
}

func TestValueToDatum_Double(t *testing.T) {
	t.Parallel()

	ft := ptypes.NewFieldType(mysql.TypeDouble)

	t.Run("positive double", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("3.141592653589793", ft)
		require.Equal(t, tiTypes.KindFloat64, d.Kind())
		require.InDelta(t, 3.141592653589793, d.GetFloat64(), 1e-15)
	})

	t.Run("negative double", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("-1.23456789", ft)
		require.Equal(t, tiTypes.KindFloat64, d.Kind())
		require.InDelta(t, -1.23456789, d.GetFloat64(), 1e-9)
	})

	t.Run("zero double", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("0", ft)
		require.Equal(t, tiTypes.KindFloat64, d.Kind())
		require.Equal(t, float64(0), d.GetFloat64())
	})

	t.Run("very large double", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("1.7976931348623157e+308", ft)
		require.Equal(t, tiTypes.KindFloat64, d.Kind())
		require.InDelta(t, math.MaxFloat64, d.GetFloat64(), 1e+293)
	})

	t.Run("invalid double panics", func(t *testing.T) {
		t.Parallel()
		require.Panics(t, func() {
			valueToDatum("not_a_double", ft)
		})
	})
}

func TestValueToDatum_StringTypes(t *testing.T) {
	t.Parallel()

	stringTypes := []struct {
		name string
		tp   byte
	}{
		{"TypeVarString", mysql.TypeVarString},
		{"TypeVarchar", mysql.TypeVarchar},
		{"TypeString", mysql.TypeString},
		{"TypeBlob", mysql.TypeBlob},
		{"TypeTinyBlob", mysql.TypeTinyBlob},
		{"TypeMediumBlob", mysql.TypeMediumBlob},
		{"TypeLongBlob", mysql.TypeLongBlob},
	}

	for _, st := range stringTypes {
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			ft := ptypes.NewFieldType(st.tp)
			ft.SetCollate("utf8mb4_bin")

			t.Run("normal string", func(t *testing.T) {
				t.Parallel()
				d := valueToDatum("hello world", ft)
				require.Equal(t, tiTypes.KindString, d.Kind())
				require.Equal(t, "hello world", d.GetString())
			})

			t.Run("empty string", func(t *testing.T) {
				t.Parallel()
				d := valueToDatum("", ft)
				require.Equal(t, tiTypes.KindString, d.Kind())
				require.Equal(t, "", d.GetString())
			})

			t.Run("unicode string", func(t *testing.T) {
				t.Parallel()
				d := valueToDatum("你好世界🌍", ft)
				require.Equal(t, tiTypes.KindString, d.Kind())
				require.Equal(t, "你好世界🌍", d.GetString())
			})
		})
	}
}

func TestValueToDatum_BinaryFlag(t *testing.T) {
	t.Parallel()

	ft := ptypes.NewFieldType(mysql.TypeString)
	ft.AddFlag(mysql.BinaryFlag)
	ft.SetCharset("binary")
	ft.SetCollate("binary")

	t.Run("ascii content", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("abc", ft)
		require.Equal(t, tiTypes.KindString, d.Kind())
		require.Equal(t, "abc", d.GetString())
	})

	t.Run("empty binary", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("", ft)
		require.Equal(t, tiTypes.KindString, d.Kind())
		require.Equal(t, "", d.GetString())
	})
}

func TestValueToDatum_Decimal(t *testing.T) {
	t.Parallel()

	t.Run("simple decimal", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeNewDecimal)
		ft.SetFlen(10)
		ft.SetDecimal(2)

		d := valueToDatum("123.45", ft)
		require.Equal(t, tiTypes.KindMysqlDecimal, d.Kind())
		require.Equal(t, "123.45", d.GetMysqlDecimal().String())
		require.Equal(t, 10, d.Length())
		require.Equal(t, 2, d.Frac())
	})

	t.Run("negative decimal", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeNewDecimal)
		ft.SetFlen(10)
		ft.SetDecimal(3)

		d := valueToDatum("-99.999", ft)
		require.Equal(t, tiTypes.KindMysqlDecimal, d.Kind())
		require.Equal(t, "-99.999", d.GetMysqlDecimal().String())
		require.Equal(t, 3, d.Frac())
	})

	t.Run("zero decimal", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeNewDecimal)
		ft.SetFlen(10)
		ft.SetDecimal(0)

		d := valueToDatum("0", ft)
		require.Equal(t, tiTypes.KindMysqlDecimal, d.Kind())
		require.Equal(t, "0", d.GetMysqlDecimal().String())
	})

	t.Run("large decimal", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeNewDecimal)
		ft.SetFlen(65)
		ft.SetDecimal(30)

		d := valueToDatum("12345678901234567890.123456789012345678", ft)
		require.Equal(t, tiTypes.KindMysqlDecimal, d.Kind())
		require.Equal(t, 65, d.Length())
		require.Equal(t, 30, d.Frac())
	})

	t.Run("unspecified decimal uses actual frac", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeNewDecimal)
		ft.SetFlen(10)
		ft.SetDecimal(tiTypes.UnspecifiedLength)

		d := valueToDatum("12.345", ft)
		require.Equal(t, tiTypes.KindMysqlDecimal, d.Kind())
		require.Equal(t, 3, d.Frac()) // actual digits frac from the value
	})

	t.Run("invalid decimal panics", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeNewDecimal)
		ft.SetFlen(10)
		ft.SetDecimal(2)
		require.Panics(t, func() {
			valueToDatum("not_decimal", ft)
		})
	})
}

func TestValueToDatum_Date(t *testing.T) {
	t.Parallel()

	ft := ptypes.NewFieldType(mysql.TypeDate)
	ft.SetDecimal(0)

	t.Run("normal date", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("2026-02-11", ft)
		require.Equal(t, tiTypes.KindMysqlTime, d.Kind())
		require.Equal(t, "2026-02-11", d.GetMysqlTime().String())
	})

	t.Run("zero date", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("0000-00-00", ft)
		require.Equal(t, tiTypes.KindMysqlTime, d.Kind())
		require.Equal(t, "0000-00-00", d.GetMysqlTime().String())
	})
}

func TestValueToDatum_Datetime(t *testing.T) {
	t.Parallel()

	t.Run("datetime without fractional seconds", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeDatetime)
		ft.SetDecimal(0)

		d := valueToDatum("2026-02-11 10:30:00", ft)
		require.Equal(t, tiTypes.KindMysqlTime, d.Kind())
		require.Equal(t, "2026-02-11 10:30:00", d.GetMysqlTime().String())
	})

	t.Run("datetime with fractional seconds", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeDatetime)
		ft.SetDecimal(6)

		d := valueToDatum("2026-02-11 10:30:00.123456", ft)
		require.Equal(t, tiTypes.KindMysqlTime, d.Kind())
		require.Equal(t, "2026-02-11 10:30:00.123456", d.GetMysqlTime().String())
	})
}

func TestValueToDatum_Timestamp(t *testing.T) {
	t.Parallel()

	ft := ptypes.NewFieldType(mysql.TypeTimestamp)
	ft.SetDecimal(0)

	d := valueToDatum("2026-02-11 10:30:00", ft)
	require.Equal(t, tiTypes.KindMysqlTime, d.Kind())
	require.Equal(t, "2026-02-11 10:30:00", d.GetMysqlTime().String())
}

func TestValueToDatum_Duration(t *testing.T) {
	t.Parallel()

	t.Run("positive duration", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeDuration)
		ft.SetDecimal(0)

		d := valueToDatum("12:30:45", ft)
		require.Equal(t, tiTypes.KindMysqlDuration, d.Kind())
		require.Equal(t, "12:30:45", d.GetMysqlDuration().String())
	})

	t.Run("negative duration", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeDuration)
		ft.SetDecimal(0)

		d := valueToDatum("-01:00:00", ft)
		require.Equal(t, tiTypes.KindMysqlDuration, d.Kind())
		require.Equal(t, "-01:00:00", d.GetMysqlDuration().String())
	})

	t.Run("duration with fractional seconds", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeDuration)
		ft.SetDecimal(3)

		d := valueToDatum("10:20:30.123", ft)
		require.Equal(t, tiTypes.KindMysqlDuration, d.Kind())
		require.Equal(t, "10:20:30.123", d.GetMysqlDuration().String())
	})

	t.Run("zero duration", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeDuration)
		ft.SetDecimal(0)

		d := valueToDatum("00:00:00", ft)
		require.Equal(t, tiTypes.KindMysqlDuration, d.Kind())
		require.Equal(t, "00:00:00", d.GetMysqlDuration().String())
	})
}

func TestValueToDatum_Enum(t *testing.T) {
	t.Parallel()

	ft := ptypes.NewFieldType(mysql.TypeEnum)
	ft.SetCharset("utf8mb4")
	ft.SetCollate("utf8mb4_bin")
	ft.SetElems([]string{"a", "b", "c"})

	t.Run("valid enum value", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("1", ft)
		require.Equal(t, tiTypes.KindMysqlEnum, d.Kind())
		require.Equal(t, uint64(1), d.GetMysqlEnum().Value)
	})

	t.Run("enum value 2", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("2", ft)
		require.Equal(t, tiTypes.KindMysqlEnum, d.Kind())
		require.Equal(t, uint64(2), d.GetMysqlEnum().Value)
	})

	t.Run("enum value 0", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("0", ft)
		require.Equal(t, tiTypes.KindMysqlEnum, d.Kind())
		require.Equal(t, uint64(0), d.GetMysqlEnum().Value)
	})

	t.Run("invalid enum panics", func(t *testing.T) {
		t.Parallel()
		require.Panics(t, func() {
			valueToDatum("abc", ft)
		})
	})
}

func TestValueToDatum_Set(t *testing.T) {
	t.Parallel()

	ft := ptypes.NewFieldType(mysql.TypeSet)
	ft.SetCharset("utf8mb4")
	ft.SetCollate("utf8mb4_bin")
	ft.SetElems([]string{"a", "b", "c"})

	t.Run("single set value", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("1", ft)
		require.Equal(t, tiTypes.KindMysqlSet, d.Kind())
		require.Equal(t, uint64(1), d.GetMysqlSet().Value)
	})

	t.Run("combined set value", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("3", ft) // a,b
		require.Equal(t, tiTypes.KindMysqlSet, d.Kind())
		require.Equal(t, uint64(3), d.GetMysqlSet().Value)
	})

	t.Run("zero set value", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("0", ft)
		require.Equal(t, tiTypes.KindMysqlSet, d.Kind())
		require.Equal(t, uint64(0), d.GetMysqlSet().Value)
	})

	t.Run("invalid set panics", func(t *testing.T) {
		t.Parallel()
		require.Panics(t, func() {
			valueToDatum("xyz", ft)
		})
	})
}

func TestValueToDatum_Bit(t *testing.T) {
	t.Parallel()

	t.Run("bit(1) value 1", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeBit)
		ft.SetFlen(1)

		d := valueToDatum("1", ft)
		require.Equal(t, tiTypes.KindMysqlBit, d.Kind())
	})

	t.Run("bit(8) value 255", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeBit)
		ft.SetFlen(8)

		d := valueToDatum("255", ft)
		require.Equal(t, tiTypes.KindMysqlBit, d.Kind())
	})

	t.Run("bit(64) large value", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeBit)
		ft.SetFlen(64)

		d := valueToDatum("18446744073709551615", ft)
		require.Equal(t, tiTypes.KindMysqlBit, d.Kind())
	})

	t.Run("bit zero", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeBit)
		ft.SetFlen(8)

		d := valueToDatum("0", ft)
		require.Equal(t, tiTypes.KindMysqlBit, d.Kind())
	})

	t.Run("invalid bit panics", func(t *testing.T) {
		t.Parallel()
		ft := ptypes.NewFieldType(mysql.TypeBit)
		ft.SetFlen(8)
		require.Panics(t, func() {
			valueToDatum("not_a_bit", ft)
		})
	})
}

func TestValueToDatum_JSON(t *testing.T) {
	t.Parallel()

	ft := ptypes.NewFieldType(mysql.TypeJSON)

	t.Run("json object", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum(`{"key": "value"}`, ft)
		require.Equal(t, tiTypes.KindMysqlJSON, d.Kind())
		require.Contains(t, d.GetMysqlJSON().String(), "key")
		require.Contains(t, d.GetMysqlJSON().String(), "value")
	})

	t.Run("json array", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum(`[1, 2, 3]`, ft)
		require.Equal(t, tiTypes.KindMysqlJSON, d.Kind())
	})

	t.Run("json string", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum(`"hello"`, ft)
		require.Equal(t, tiTypes.KindMysqlJSON, d.Kind())
	})

	t.Run("json number", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum(`42`, ft)
		require.Equal(t, tiTypes.KindMysqlJSON, d.Kind())
	})

	t.Run("json null", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum(`null`, ft)
		require.Equal(t, tiTypes.KindMysqlJSON, d.Kind())
	})

	t.Run("json boolean", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum(`true`, ft)
		require.Equal(t, tiTypes.KindMysqlJSON, d.Kind())
	})

	t.Run("nested json", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum(`{"a": [1, {"b": "c"}], "d": null}`, ft)
		require.Equal(t, tiTypes.KindMysqlJSON, d.Kind())
	})

	t.Run("invalid json panics", func(t *testing.T) {
		t.Parallel()
		require.Panics(t, func() {
			valueToDatum(`{invalid`, ft)
		})
	})
}

func TestValueToDatum_VectorFloat32(t *testing.T) {
	t.Parallel()

	ft := ptypes.NewFieldType(mysql.TypeTiDBVectorFloat32)

	t.Run("simple vector", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("[1,2,3]", ft)
		require.False(t, d.IsNull())
	})

	t.Run("single element vector", func(t *testing.T) {
		t.Parallel()
		d := valueToDatum("[0.5]", ft)
		require.False(t, d.IsNull())
	})

	t.Run("invalid vector panics", func(t *testing.T) {
		t.Parallel()
		require.Panics(t, func() {
			valueToDatum("not_a_vector", ft)
		})
	})
}

func TestValueToDatum_UnknownType(t *testing.T) {
	t.Parallel()
	// Use a type that doesn't match any case in the switch (TypeGeometry).
	// The default datum returned is a zero-value datum, which is null.
	ft := ptypes.NewFieldType(mysql.TypeGeometry)
	d := valueToDatum("some_value", ft)
	require.True(t, d.IsNull())
}

func TestValueToDatum_ViaNewPKColumnFieldType(t *testing.T) {
	t.Parallel()
	// Test valueToDatum using FieldTypes produced by newPKColumnFieldTypeFromMysqlType,
	// which is the real caller in production code.

	t.Run("int", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("int")
		d := valueToDatum("42", ft)
		require.Equal(t, tiTypes.KindInt64, d.Kind())
		require.Equal(t, int64(42), d.GetInt64())
	})

	t.Run("int unsigned", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("int unsigned")
		d := valueToDatum("42", ft)
		require.Equal(t, tiTypes.KindUint64, d.Kind())
		require.Equal(t, uint64(42), d.GetUint64())
	})

	t.Run("bigint", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("bigint")
		d := valueToDatum("9223372036854775807", ft)
		require.Equal(t, tiTypes.KindInt64, d.Kind())
		require.Equal(t, int64(math.MaxInt64), d.GetInt64())
	})

	t.Run("bigint unsigned", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("bigint unsigned")
		d := valueToDatum("18446744073709551615", ft)
		require.Equal(t, tiTypes.KindUint64, d.Kind())
		require.Equal(t, uint64(math.MaxUint64), d.GetUint64())
	})

	t.Run("varchar", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("varchar")
		d := valueToDatum("hello", ft)
		require.Equal(t, tiTypes.KindString, d.Kind())
		require.Equal(t, "hello", d.GetString())
	})

	t.Run("char", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("char")
		d := valueToDatum("x", ft)
		require.Equal(t, tiTypes.KindString, d.Kind())
		require.Equal(t, "x", d.GetString())
	})

	t.Run("decimal(10,2)", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("decimal(10,2)")
		d := valueToDatum("123.45", ft)
		require.Equal(t, tiTypes.KindMysqlDecimal, d.Kind())
		require.Equal(t, "123.45", d.GetMysqlDecimal().String())
		require.Equal(t, 10, d.Length())
		require.Equal(t, 2, d.Frac())
	})

	t.Run("float", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("float")
		d := valueToDatum("3.14", ft)
		require.Equal(t, tiTypes.KindFloat32, d.Kind())
		require.InDelta(t, float32(3.14), d.GetFloat32(), 0.001)
	})

	t.Run("double", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("double")
		d := valueToDatum("3.141592653589793", ft)
		require.Equal(t, tiTypes.KindFloat64, d.Kind())
		require.InDelta(t, 3.141592653589793, d.GetFloat64(), 1e-15)
	})

	t.Run("binary", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("binary")
		require.True(t, mysql.HasBinaryFlag(ft.GetFlag()))
		d := valueToDatum("abc", ft)
		require.Equal(t, tiTypes.KindString, d.Kind())
	})

	t.Run("varbinary", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("varbinary")
		require.True(t, mysql.HasBinaryFlag(ft.GetFlag()))
		d := valueToDatum("abc", ft)
		require.Equal(t, tiTypes.KindString, d.Kind())
	})

	t.Run("tinyint", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("tinyint")
		d := valueToDatum("127", ft)
		require.Equal(t, tiTypes.KindInt64, d.Kind())
		require.Equal(t, int64(127), d.GetInt64())
	})

	t.Run("smallint unsigned", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("smallint unsigned")
		d := valueToDatum("65535", ft)
		require.Equal(t, tiTypes.KindUint64, d.Kind())
		require.Equal(t, uint64(65535), d.GetUint64())
	})

	t.Run("date", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("date")
		d := valueToDatum("2026-02-11", ft)
		require.Equal(t, tiTypes.KindMysqlTime, d.Kind())
		require.Equal(t, "2026-02-11", d.GetMysqlTime().String())
	})

	t.Run("datetime", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("datetime")
		d := valueToDatum("2026-02-11 10:30:00", ft)
		require.Equal(t, tiTypes.KindMysqlTime, d.Kind())
	})

	t.Run("timestamp", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("timestamp")
		d := valueToDatum("2026-02-11 10:30:00", ft)
		require.Equal(t, tiTypes.KindMysqlTime, d.Kind())
	})

	t.Run("time", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("time")
		d := valueToDatum("12:30:45", ft)
		require.Equal(t, tiTypes.KindMysqlDuration, d.Kind())
	})

	t.Run("year", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("year")
		d := valueToDatum("2026", ft)
		require.Equal(t, tiTypes.KindInt64, d.Kind())
		require.Equal(t, int64(2026), d.GetInt64())
	})

	t.Run("enum('a','b','c')", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("enum('a','b','c')")
		d := valueToDatum("2", ft)
		require.Equal(t, tiTypes.KindMysqlEnum, d.Kind())
		require.Equal(t, uint64(2), d.GetMysqlEnum().Value)
	})

	t.Run("set('x','y','z')", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("set('x','y','z')")
		d := valueToDatum("5", ft)
		require.Equal(t, tiTypes.KindMysqlSet, d.Kind())
		require.Equal(t, uint64(5), d.GetMysqlSet().Value)
	})

	t.Run("bit(8)", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("bit(8)")
		d := valueToDatum("255", ft)
		require.Equal(t, tiTypes.KindMysqlBit, d.Kind())
	})

	t.Run("json", func(t *testing.T) {
		t.Parallel()
		ft := newPKColumnFieldTypeFromMysqlType("json")
		d := valueToDatum(`{"key":"value"}`, ft)
		require.Equal(t, tiTypes.KindMysqlJSON, d.Kind())
	})
}
