// Copyright 2024 PingCAP, Inc.
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

package debezium

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type dbzCodec struct {
	config    *common.Config
	clusterID string
	nowFunc   func() time.Time
}

func (c *dbzCodec) writeDebeziumFieldValues(
	writer *util.JSONWriter,
	fieldName string,
	row *chunk.Row,
	tableInfo *commonType.TableInfo,
	columnSelector commonEvent.Selector,
) error {
	var err error
	writer.WriteObjectField(fieldName, func() {
		for i, colInfo := range tableInfo.GetColumns() {
			if !columnSelector.Select(colInfo) {
				continue
			}
			err = c.writeDebeziumFieldValue(writer, row, i, colInfo)
			if err != nil {
				log.Error("write Debezium field value meet error", zap.Error(err))
				break
			}
		}
	})
	return err
}

func (c *dbzCodec) writeDebeziumFieldSchema(
	writer *util.JSONWriter,
	col *timodel.ColumnInfo,
) {
	colName := col.Name.O
	ft := &col.FieldType
	writer.WriteObjectElement(func() {
		if c.config.EnableTiDBExtension {
			// The followings are TiDB extended fields
			writer.WriteStringField("tidb_type", getTiDBType(ft))
		}
		switch col.GetType() {
		case mysql.TypeBit:
			n := ft.GetFlen()
			var v uint64
			var err error
			if col.GetDefaultValue() != nil {
				val, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				v, err = strconv.ParseUint(parseBit(val, n), 2, 64)
				if err != nil {
					return
				}
			}
			if n == 1 {
				writer.WriteStringField("type", "boolean")
				writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
				writer.WriteStringField("field", colName)
				if col.GetDefaultValue() != nil {
					writer.WriteBoolField("default", v != 0) // bool
				}
			} else {
				writer.WriteStringField("type", "bytes")
				writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
				writer.WriteStringField("name", "io.debezium.data.Bits")
				writer.WriteIntField("version", 1)
				writer.WriteObjectField("parameters", func() {
					writer.WriteStringField("length", fmt.Sprintf("%d", n))
				})
				writer.WriteStringField("field", colName)
				if col.GetDefaultValue() != nil {
					c.writeBinaryField(writer, "default", getBitFromUint64(n, v)) // binary
				}
			}
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
			mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				writer.WriteAnyField("default", col.GetDefaultValue())
			}
		case mysql.TypeEnum:
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.data.Enum")
			writer.WriteIntField("version", 1)
			writer.WriteObjectField("parameters", func() {
				elems := ft.GetElems()
				parameters := make([]string, 0, len(elems))
				for _, ele := range elems {
					parameters = append(parameters, common.EscapeEnumAndSetOptions(ele))
				}
				writer.WriteStringField("allowed", strings.Join(parameters, ","))
			})
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				writer.WriteAnyField("default", col.GetDefaultValue())
			}
		case mysql.TypeSet:
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.data.EnumSet")
			writer.WriteIntField("version", 1)
			writer.WriteObjectField("parameters", func() {
				writer.WriteStringField("allowed", strings.Join(ft.GetElems(), ","))
			})
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				writer.WriteAnyField("default", col.GetDefaultValue())
			}
		case mysql.TypeDate, mysql.TypeNewDate:
			writer.WriteStringField("type", "int32")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.Date")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				t, err := time.Parse("2006-01-02", v)
				if err != nil {
					// For example, time may be invalid like 1000-00-00
					// return nil, nil
					if mysql.HasNotNullFlag(ft.GetFlag()) {
						writer.WriteInt64Field("default", 0)
					}
					return
				}
				year := t.Year()
				if year < 70 {
					// treats "0018" as 2018
					t = t.AddDate(2000, 0, 0)
				} else if year < 100 {
					//  treats "0099" as 1999
					t = t.AddDate(1900, 0, 0)
				}
				writer.WriteInt64Field("default", t.UTC().Unix()/60/60/24)
			}
		case mysql.TypeDatetime:
			writer.WriteStringField("type", "int64")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			if ft.GetDecimal() <= 3 {
				writer.WriteStringField("name", "io.debezium.time.Timestamp")
			} else {
				writer.WriteStringField("name", "io.debezium.time.MicroTimestamp")
			}
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				if v == "CURRENT_TIMESTAMP" {
					writer.WriteInt64Field("default", 0)
					return
				}
				t, err := types.StrToDateTime(types.DefaultStmtNoWarningContext, v, ft.GetDecimal())
				if err != nil {
					writer.WriteInt64Field("default", 0)
					return
				}
				gt, err := t.GoTime(time.UTC)
				if err != nil {
					if mysql.HasNotNullFlag(ft.GetFlag()) {
						writer.WriteInt64Field("default", 0)
					}
					return
				}
				year := gt.Year()
				if year < 70 {
					// treats "0018" as 2018
					gt = gt.AddDate(2000, 0, 0)
				} else if year < 100 {
					//  treats "0099" as 1999
					gt = gt.AddDate(1900, 0, 0)
				}
				if ft.GetDecimal() <= 3 {
					writer.WriteInt64Field("default", gt.UnixMilli())
				} else {
					writer.WriteInt64Field("default", gt.UnixMicro())
				}
			}
		case mysql.TypeTimestamp:
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.ZonedTimestamp")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				if v == "CURRENT_TIMESTAMP" {
					if mysql.HasNotNullFlag(ft.GetFlag()) {
						writer.WriteStringField("default", "1970-01-01T00:00:00Z")
					}
					return
				}
				t, err := types.StrToDateTime(types.DefaultStmtNoWarningContext, v, ft.GetDecimal())
				if err != nil {
					writer.WriteInt64Field("default", 0)
					return
				}
				if t.Compare(types.MinTimestamp) < 0 {
					if mysql.HasNotNullFlag(ft.GetFlag()) {
						writer.WriteStringField("default", "1970-01-01T00:00:00Z")
					}
					return
				}
				gt, err := t.GoTime(time.UTC)
				if err != nil {
					writer.WriteInt64Field("default", 0)
					return
				}
				str := gt.Format("2006-01-02T15:04:05")
				fsp := ft.GetDecimal()
				if fsp > 0 {
					tmp := fmt.Sprintf(".%06d", gt.Nanosecond()/1000)
					str = str + tmp[:1+fsp]
				}
				str += "Z"
				writer.WriteStringField("default", str)
			}
		case mysql.TypeDuration:
			writer.WriteStringField("type", "int64")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.MicroTime")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				d, _, _, err := types.StrToDuration(types.DefaultStmtNoWarningContext.WithLocation(c.config.TimeZone), v, ft.GetDecimal())
				if err != nil {
					return
				}
				writer.WriteInt64Field("default", d.Microseconds())
			}
		case mysql.TypeJSON:
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.data.Json")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				writer.WriteAnyField("default", col.GetDefaultValue())
			}
		case mysql.TypeTiny: // TINYINT
			writer.WriteStringField("type", "int16")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		case mysql.TypeShort: // SMALLINT
			if mysql.HasUnsignedFlag(ft.GetFlag()) {
				writer.WriteStringField("type", "int32")
			} else {
				writer.WriteStringField("type", "int16")
			}
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		case mysql.TypeInt24: // MEDIUMINT
			writer.WriteStringField("type", "int32")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		case mysql.TypeLong: // INT
			if mysql.HasUnsignedFlag(col.GetFlag()) {
				writer.WriteStringField("type", "int64")
			} else {
				writer.WriteStringField("type", "int32")
			}
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		case mysql.TypeLonglong: // BIGINT
			writer.WriteStringField("type", "int64")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		case mysql.TypeFloat:
			if ft.GetDecimal() != -1 {
				writer.WriteStringField("type", "double")
			} else {
				writer.WriteStringField("type", "float")
			}
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		case mysql.TypeDouble, mysql.TypeNewDecimal:
			// https://dev.mysql.com/doc/refman/8.4/en/numeric-types.html
			// MySQL also treats REAL as a synonym for DOUBLE PRECISION (a nonstandard variation), unless the REAL_AS_FLOAT SQL mode is enabled.
			writer.WriteStringField("type", "double")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				writer.WriteFloat64Field("default", floatV)
			}
		case mysql.TypeYear:
			writer.WriteStringField("type", "int32")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.time.Year")
			writer.WriteIntField("version", 1)
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				v, ok := col.GetDefaultValue().(string)
				if !ok {
					return
				}
				floatV, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return
				}
				if floatV < 70 {
					// treats "DEFAULT 1" as 2001
					floatV += 2000
				} else if floatV < 100 {
					//  treats "DEFAULT 99" as 1999
					floatV += 1900
				}
				writer.WriteFloat64Field("default", floatV)
			}
		case mysql.TypeTiDBVectorFloat32:
			writer.WriteStringField("type", "string")
			writer.WriteBoolField("optional", !mysql.HasNotNullFlag(ft.GetFlag()))
			writer.WriteStringField("name", "io.debezium.data.TiDBVectorFloat32")
			writer.WriteStringField("field", colName)
			if col.GetDefaultValue() != nil {
				writer.WriteAnyField("default", col.GetDefaultValue())
			}
		default:
			log.Warn(
				"meet unsupported field type",
				zap.Any("fieldType", col.GetType()),
				zap.Any("column", colName),
			)
		}
	})
}

// See https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-data-types
//
//revive:disable indent-error-flow
func (c *dbzCodec) writeDebeziumFieldValue(
	writer *util.JSONWriter,
	row *chunk.Row,
	idx int,
	colInfo *timodel.ColumnInfo,
) error {
	ft := &colInfo.FieldType
	colName := colInfo.Name.O
	datum := row.GetDatum(idx, ft)
	if datum.IsNull() {
		defaultVal := colInfo.GetDefaultValue()
		if defaultVal == nil {
			writer.WriteNullField(colName)
			return nil
		}
		val := getValueFromDefault(defaultVal, ft)
		datum.SetValue(val, ft)
	}
	switch colInfo.GetType() {
	case mysql.TypeBit:
		n := ft.GetFlen()
		v, err := datum.GetMysqlBit().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			return errors.WrapError(
				errors.ErrDebeziumEncodeFailed,
				err)
		}
		// Debezium behavior:
		// BIT(1) → BOOLEAN
		// BIT(>1) → BYTES		The byte[] contains the bits in little-endian form and is sized to
		//						contain the specified number of bits.
		if n == 1 {
			writer.WriteBoolField(colName, v != 0)
			return nil
		} else {
			c.writeBinaryField(writer, colName, getBitFromUint64(n, v))
			return nil
		}

	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		v := datum.GetBytes()
		isBinary := mysql.HasBinaryFlag(colInfo.GetFlag())
		if !isBinary {
			writer.WriteStringField(colName, common.UnsafeBytesToString(v))
		} else {
			c.writeBinaryField(writer, colName, v)
		}
		return nil

	case mysql.TypeEnum:
		v := datum.GetMysqlEnum().Value
		enumVar, err := types.ParseEnumValue(ft.GetElems(), v)
		if err != nil {
			// Invalid enum value inserted in non-strict mode.
			writer.WriteStringField(colName, "")
			return nil
		}
		writer.WriteStringField(colName, enumVar.Name)
		return nil

	case mysql.TypeSet:
		v := datum.GetMysqlSet().Value
		setVar, err := types.ParseSetValue(ft.GetElems(), v)
		if err != nil {
			// Invalid enum value inserted in non-strict mode.
			writer.WriteStringField(colName, "")
			return nil
		}
		writer.WriteStringField(colName, setVar.Name)
		return nil

	case mysql.TypeNewDecimal:
		v, err := datum.GetMysqlDecimal().ToFloat64()
		if err != nil {
			return errors.WrapError(
				errors.ErrDebeziumEncodeFailed,
				err)
		}
		writer.WriteFloat64Field(colName, v)
		return nil

	case mysql.TypeDate, mysql.TypeNewDate:
		v := datum.GetMysqlTime().String()
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			// For example, time may be invalid like 1000-00-00
			// return nil, nil
			if mysql.HasNotNullFlag(ft.GetFlag()) {
				writer.WriteInt64Field(colName, 0)
			} else {
				writer.WriteNullField(colName)
			}
			return nil
		}
		year := t.Year()
		if year < 70 {
			// treats "0018" as 2018
			t = t.AddDate(2000, 0, 0)
		} else if year < 100 {
			//  treats "0099" as 1999
			t = t.AddDate(1900, 0, 0)
		}

		writer.WriteInt64Field(colName, t.UTC().Unix()/60/60/24)
		return nil

	case mysql.TypeDatetime:
		// Debezium behavior from doc:
		// > Such columns are converted into epoch milliseconds or microseconds based on the
		// > column's precision by using UTC.
		value := datum.GetValue()
		var v types.Time
		switch val := value.(type) {
		case string:
			if val == "CURRENT_TIMESTAMP" {
				writer.WriteInt64Field(colName, 0)
				return nil
			} else {
				t, err := types.StrToDateTime(types.DefaultStmtNoWarningContext.WithLocation(c.config.TimeZone), val, ft.GetDecimal())
				if err != nil {
					return errors.WrapError(
						errors.ErrDebeziumEncodeFailed,
						err)
				}
				v = t
			}
		case types.Time:
			v = val
		default:
			return errors.Trace(
				errors.ErrDebeziumEncodeFailed)
		}
		gt, err := v.GoTime(time.UTC)
		if err != nil {
			if mysql.HasNotNullFlag(ft.GetFlag()) {
				writer.WriteInt64Field(colName, 0)
			} else {
				writer.WriteNullField(colName)
			}
			return nil
		}
		year := gt.Year()
		if year < 70 {
			// treats "0018" as 2018
			gt = gt.AddDate(2000, 0, 0)
		} else if year < 100 {
			//  treats "0099" as 1999
			gt = gt.AddDate(1900, 0, 0)
		}
		if ft.GetDecimal() <= 3 {
			writer.WriteInt64Field(colName, gt.UnixMilli())
		} else {
			writer.WriteInt64Field(colName, gt.UnixMicro())
		}
		return nil

	case mysql.TypeTimestamp:
		// Debezium behavior from doc:
		// > The TIMESTAMP type represents a timestamp without time zone information.
		// > It is converted by MySQL from the server (or session's) current time zone into UTC
		// > when writing and from UTC into the server (or session's) current time zone when reading
		// > back the value.
		// > Such columns are converted into an equivalent io.debezium.time.ZonedTimestamp in UTC
		// > based on the server (or session's) current time zone. The time zone will be queried from
		// > the server by default. If this fails, it must be specified explicitly by the database
		// > connectionTimeZone MySQL configuration option.
		value := datum.GetValue()
		var v types.Time
		switch val := value.(type) {
		case string:
			if val == "CURRENT_TIMESTAMP" {
				if mysql.HasNotNullFlag(ft.GetFlag()) {
					writer.WriteStringField(colName, "1970-01-01T00:00:00Z")
				} else {
					writer.WriteNullField(colName)
				}
				return nil
			} else {
				t, err := types.StrToDateTime(types.DefaultStmtNoWarningContext.WithLocation(c.config.TimeZone), val, ft.GetDecimal())
				if err != nil {
					return errors.WrapError(
						errors.ErrDebeziumEncodeFailed,
						err)
				}
				v = t
			}
		case types.Time:
			v = val
		}
		if v.Compare(types.MinTimestamp) < 0 {
			if row.IsNull(idx) {
				writer.WriteNullField(colName)
			} else {
				writer.WriteStringField(colName, "1970-01-01T00:00:00Z")
			}
			return nil
		}
		gt, err := v.GoTime(c.config.TimeZone)
		if err != nil {
			return errors.WrapError(
				errors.ErrDebeziumEncodeFailed,
				err)
		}
		str := gt.UTC().Format("2006-01-02T15:04:05")
		fsp := ft.GetDecimal()
		if fsp > 0 {
			tmp := fmt.Sprintf(".%06d", gt.Nanosecond()/1000)
			str = str + tmp[:1+fsp]
		}
		str += "Z"
		writer.WriteStringField(colName, str)
		return nil

	case mysql.TypeDuration:
		// Debezium behavior from doc:
		// > Represents the time value in microseconds and does not include
		// > time zone information. MySQL allows M to be in the range of 0-6.
		d := datum.GetMysqlDuration()
		writer.WriteInt64Field(colName, d.Microseconds())
		return nil

	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		// Note: Although Debezium's doc claims to use INT32 for INT, but it
		// actually uses INT64. Debezium also uses INT32 for SMALLINT.
		maxValue := types.GetMaxValue(ft)
		minValue := types.GetMinValue(ft)
		isUnsigned := mysql.HasUnsignedFlag(colInfo.GetFlag())
		if isUnsigned {
			v := datum.GetUint64()
			if ft.GetType() == mysql.TypeLonglong && v == maxValue.GetUint64() || v > maxValue.GetUint64() {
				writer.WriteAnyField(colName, -1)
			} else {
				writer.WriteInt64Field(colName, int64(v))
			}
		} else {
			v := datum.GetInt64()
			if v < minValue.GetInt64() || v > maxValue.GetInt64() {
				writer.WriteAnyField(colName, -1)
			} else {
				writer.WriteInt64Field(colName, v)
			}
		}
		return nil

	case mysql.TypeDouble:
		v := datum.GetFloat64()
		writer.WriteFloat64Field(colName, v)
		return nil

	case mysql.TypeFloat:
		v := datum.GetFloat32()
		writer.WriteFloat32Field(colName, v)
		return nil

	case mysql.TypeYear:
		v := datum.GetInt64()
		writer.WriteInt64Field(colName, v)
		return nil

	case mysql.TypeTiDBVectorFloat32:
		v := datum.GetVectorFloat32()
		writer.WriteStringField(colName, v.String())
		return nil
	}
	// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
	// Make specified convert upper if you need
	// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
	writer.WriteAnyField(colName, fmt.Sprintf("%v", datum.GetValue()))
	return nil
}

func (c *dbzCodec) writeBinaryField(writer *util.JSONWriter, fieldName string, value []byte) {
	// TODO: Deal with different binary output later.
	writer.WriteBase64StringField(fieldName, value)
}

func (c *dbzCodec) writeSourceSchema(writer *util.JSONWriter) {
	writer.WriteObjectElement(func() {
		writer.WriteStringField("type", "struct")
		writer.WriteArrayField("fields", func() {
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "version")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "connector")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "name")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "int64")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "ts_ms")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("name", "io.debezium.data.Enum")
				writer.WriteIntField("version", 1)
				writer.WriteObjectField("parameters", func() {
					writer.WriteStringField("allowed", "true,last,false,incremental")
				})
				writer.WriteStringField("default", "false")
				writer.WriteStringField("field", "snapshot")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "db")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("field", "sequence")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("field", "table")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "int64")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "server_id")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("field", "gtid")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "file")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "int64")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "pos")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "int32")
				writer.WriteBoolField("optional", false)
				writer.WriteStringField("field", "row")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "int64")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("field", "thread")
			})
			writer.WriteObjectElement(func() {
				writer.WriteStringField("type", "string")
				writer.WriteBoolField("optional", true)
				writer.WriteStringField("field", "query")
			})
		})
		writer.WriteBoolField("optional", false)
		writer.WriteStringField("name", "io.debezium.connector.mysql.Source")
		writer.WriteStringField("field", "source")
	})
}

// EncodeKey encode RowChangedEvent into key message
func (c *dbzCodec) EncodeKey(
	e *commonEvent.RowEvent,
	dest io.Writer,
) error {
	// schema field describes the structure of the primary key, or the unique key if the table does not have a primary key, for the table that was changed.
	// see https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-events
	jWriter := util.BorrowJSONWriter(dest)
	defer util.ReturnJSONWriter(jWriter)

	var err error
	jWriter.WriteObject(func() {
		jWriter.WriteObjectField("payload", func() {
			columns := e.TableInfo.GetColumns()
			row := e.GetRows()
			if e.IsDelete() {
				row = e.GetPreRows()
			}
			for idx, colInfo := range columns {
				if colInfo != nil && e.TableInfo.IsHandleKey(colInfo.ID) {
					err = c.writeDebeziumFieldValue(jWriter, row, idx, colInfo)
				}
			}
		})
		if !c.config.DebeziumDisableSchema {
			jWriter.WriteObjectField("schema", func() {
				jWriter.WriteStringField("type", "struct")
				jWriter.WriteStringField("name",
					fmt.Sprintf("%s.Key", getSchemaTopicName(c.clusterID, e.TableInfo.GetSchemaName(), e.TableInfo.GetTableName())))
				jWriter.WriteBoolField("optional", false)
				jWriter.WriteArrayField("fields", func() {
					columns := e.TableInfo.GetColumns()
					for _, colInfo := range columns {
						if colInfo != nil && e.TableInfo.IsHandleKey(colInfo.ID) {
							c.writeDebeziumFieldSchema(jWriter, colInfo)
						}
					}
				})
			})
		}
	})
	return err
}

// EncodeValue encode RowChangedEvent into value message
func (c *dbzCodec) EncodeValue(
	e *commonEvent.RowEvent,
	dest io.Writer,
) error {
	jWriter := util.BorrowJSONWriter(dest)
	defer util.ReturnJSONWriter(jWriter)

	commitTime := oracle.GetTimeFromTS(e.CommitTs)

	var err error

	jWriter.WriteObject(func() {
		jWriter.WriteObjectField("payload", func() {
			jWriter.WriteObjectField("source", func() {
				jWriter.WriteStringField("version", "2.4.0.Final")
				jWriter.WriteStringField("connector", "TiCDC")
				jWriter.WriteStringField("name", c.clusterID)
				// ts_ms: In the source object, ts_ms indicates the time that the change was made in the database.
				// https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
				jWriter.WriteInt64Field("ts_ms", commitTime.UnixMilli())
				// snapshot field is a string of true,last,false,incremental
				jWriter.WriteStringField("snapshot", "false")
				jWriter.WriteStringField("db", e.TableInfo.GetSchemaName())
				jWriter.WriteStringField("table", e.TableInfo.GetTableName())
				jWriter.WriteInt64Field("server_id", 0)
				jWriter.WriteNullField("gtid")
				jWriter.WriteStringField("file", "")
				jWriter.WriteInt64Field("pos", 0)
				jWriter.WriteInt64Field("row", 0)
				jWriter.WriteInt64Field("thread", 0)
				jWriter.WriteNullField("query")

				// The followings are TiDB extended fields
				jWriter.WriteUint64Field("commit_ts", e.CommitTs)
				jWriter.WriteStringField("cluster_id", c.clusterID)
			})

			// ts_ms: displays the time at which the connector processed the event
			// https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
			jWriter.WriteInt64Field("ts_ms", c.nowFunc().UnixMilli())
			jWriter.WriteNullField("transaction")
			if e.IsInsert() {
				// op: Mandatory string that describes the type of operation that caused the connector to generate the event.
				// Valid values are:
				// c = create
				// u = update
				// d = delete
				// r = read (applies to only snapshots)
				// https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
				jWriter.WriteStringField("op", "c")

				// before: An optional field that specifies the state of the row before the event occurred.
				// When the op field is c for create, the before field is null since this change event is for new content.
				// In a delete event value, the before field contains the values that were in the row before
				// it was deleted with the database commit.
				jWriter.WriteNullField("before")

				// after: An optional field that specifies the state of the row after the event occurred.
				// Optional field that specifies the state of the row after the event occurred.
				// In a delete event value, the after field is null, signifying that the row no longer exists.
				err = c.writeDebeziumFieldValues(jWriter, "after", e.GetRows(), e.TableInfo, e.ColumnSelector)
			} else if e.IsDelete() {
				jWriter.WriteStringField("op", "d")
				jWriter.WriteNullField("after")
				err = c.writeDebeziumFieldValues(jWriter, "before", e.GetPreRows(), e.TableInfo, e.ColumnSelector)
			} else if e.IsUpdate() {
				jWriter.WriteStringField("op", "u")
				if c.config.DebeziumOutputOldValue {
					err = c.writeDebeziumFieldValues(jWriter, "before", e.GetPreRows(), e.TableInfo, e.ColumnSelector)
				}
				if err == nil {
					err = c.writeDebeziumFieldValues(jWriter, "after", e.GetRows(), e.TableInfo, e.ColumnSelector)
				}
			}
		})

		if !c.config.DebeziumDisableSchema {
			jWriter.WriteObjectField("schema", func() {
				jWriter.WriteStringField("type", "struct")
				jWriter.WriteBoolField("optional", false)
				jWriter.WriteStringField("name",
					fmt.Sprintf("%s.Envelope", getSchemaTopicName(c.clusterID, e.TableInfo.GetSchemaName(), e.TableInfo.GetTableName())))
				jWriter.WriteIntField("version", 1)
				jWriter.WriteArrayField("fields", func() {
					// schema is the same for `before` and `after`. So we build a new buffer to
					// build the JSON, so that content can be reused.
					var fieldsJSON string
					{
						fieldsBuf := &bytes.Buffer{}
						fieldsWriter := util.BorrowJSONWriter(fieldsBuf)
						for _, col := range e.TableInfo.GetColumns() {
							if !e.ColumnSelector.Select(col) {
								continue
							}
							c.writeDebeziumFieldSchema(fieldsWriter, col)
						}
						if e.TableInfo.HasVirtualColumns() {
							for _, colInfo := range e.TableInfo.GetColumns() {
								if commonType.IsColCDCVisible(colInfo) {
									continue
								}
								c.writeDebeziumFieldSchema(fieldsWriter, colInfo)
							}
						}
						util.ReturnJSONWriter(fieldsWriter)
						fieldsJSON = fieldsBuf.String()
					}
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "struct")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("name",
							fmt.Sprintf("%s.Value", getSchemaTopicName(c.clusterID, e.TableInfo.GetSchemaName(), e.TableInfo.GetTableName())))
						jWriter.WriteStringField("field", "before")
						jWriter.WriteArrayField("fields", func() {
							jWriter.WriteRaw(fieldsJSON)
						})
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "struct")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("name",
							fmt.Sprintf("%s.Value", getSchemaTopicName(c.clusterID, e.TableInfo.GetSchemaName(), e.TableInfo.GetTableName())))
						jWriter.WriteStringField("field", "after")
						jWriter.WriteArrayField("fields", func() {
							jWriter.WriteRaw(fieldsJSON)
						})
					})
					c.writeSourceSchema(jWriter)
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "string")
						jWriter.WriteBoolField("optional", false)
						jWriter.WriteStringField("field", "op")
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "int64")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("field", "ts_ms")
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "struct")
						jWriter.WriteArrayField("fields", func() {
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "id")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "int64")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "total_order")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "int64")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "data_collection_order")
							})
						})
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("name", "event.block")
						jWriter.WriteIntField("version", 1)
						jWriter.WriteStringField("field", "transaction")
					})
				})
			})
		}
	})
	return err
}

// EncodeDDLEvent encode DDLEvent into debezium change event
func (c *dbzCodec) EncodeDDLEvent(
	e *commonEvent.DDLEvent,
	keyDest io.Writer,
	dest io.Writer,
) error {
	keyJWriter := util.BorrowJSONWriter(keyDest)
	jWriter := util.BorrowJSONWriter(dest)
	defer util.ReturnJSONWriter(keyJWriter)
	defer util.ReturnJSONWriter(jWriter)

	commitTime := oracle.GetTimeFromTS(e.FinishedTs)
	var changeType string
	// refer to: https://docs.pingcap.com/tidb/dev/mysql-compatibility#ddl-operations
	switch e.GetDDLType() {
	case timodel.ActionCreateSchema,
		timodel.ActionCreateTable,
		timodel.ActionCreateTables,
		timodel.ActionCreateView:
		changeType = "CREATE"
	case timodel.ActionAddColumn,
		timodel.ActionModifyColumn,
		timodel.ActionDropColumn,
		timodel.ActionMultiSchemaChange,
		timodel.ActionAddTablePartition,
		timodel.ActionRemovePartitioning,
		timodel.ActionReorganizePartition,
		timodel.ActionExchangeTablePartition,
		timodel.ActionAlterTablePartitioning,
		timodel.ActionTruncateTablePartition,
		timodel.ActionDropTablePartition,
		timodel.ActionRebaseAutoID,
		timodel.ActionSetDefaultValue,
		timodel.ActionModifyTableComment,
		timodel.ActionModifyTableCharsetAndCollate,
		timodel.ActionModifySchemaCharsetAndCollate,
		timodel.ActionAddIndex,
		timodel.ActionAlterIndexVisibility,
		timodel.ActionRenameIndex,
		timodel.ActionRenameTable,
		timodel.ActionRenameTables,
		timodel.ActionAddPrimaryKey,
		timodel.ActionDropPrimaryKey:
		changeType = "ALTER"
	case timodel.ActionDropSchema,
		timodel.ActionDropTable,
		timodel.ActionDropIndex,
		timodel.ActionDropView,
		timodel.ActionTruncateTable:
		changeType = "DROP"
	default:
		return errors.ErrDDLUnsupportType.GenWithStackByArgs(e.Type, e.Query)
	}

	var err error
	dbName, tableName := getDBTableName(e)
	// message key
	keyJWriter.WriteObject(func() {
		keyJWriter.WriteObjectField("payload", func() {
			keyJWriter.WriteStringField("databaseName", dbName)
		})
		if !c.config.DebeziumDisableSchema {
			keyJWriter.WriteObjectField("schema", func() {
				keyJWriter.WriteStringField("type", "struct")
				keyJWriter.WriteStringField("name", "io.debezium.connector.mysql.SchemaChangeKey")
				keyJWriter.WriteBoolField("optional", false)
				keyJWriter.WriteIntField("version", 1)
				keyJWriter.WriteArrayField("fields", func() {
					keyJWriter.WriteObjectElement(func() {
						keyJWriter.WriteStringField("field", "databaseName")
						keyJWriter.WriteBoolField("optional", false)
						keyJWriter.WriteStringField("type", "string")
					})
				})
			})
		}
	})

	// message value
	jWriter.WriteObject(func() {
		jWriter.WriteObjectField("payload", func() {
			jWriter.WriteObjectField("source", func() {
				jWriter.WriteStringField("version", "2.4.0.Final")
				jWriter.WriteStringField("connector", "TiCDC")
				jWriter.WriteStringField("name", c.clusterID)
				jWriter.WriteInt64Field("ts_ms", commitTime.UnixMilli())
				jWriter.WriteStringField("snapshot", "false")
				jWriter.WriteStringField("db", dbName)
				jWriter.WriteStringField("table", tableName)
				jWriter.WriteInt64Field("server_id", 0)
				jWriter.WriteNullField("gtid")
				jWriter.WriteStringField("file", "")
				jWriter.WriteInt64Field("pos", 0)
				jWriter.WriteInt64Field("row", 0)
				jWriter.WriteInt64Field("thread", 0)
				jWriter.WriteNullField("query")

				// The followings are TiDB extended fields
				jWriter.WriteUint64Field("commit_ts", e.GetCommitTs())
				jWriter.WriteStringField("cluster_id", c.clusterID)
			})
			jWriter.WriteInt64Field("ts_ms", c.nowFunc().UnixMilli())

			jWriter.WriteStringField("databaseName", dbName)
			jWriter.WriteNullField("schemaName")
			jWriter.WriteStringField("ddl", e.Query)
			jWriter.WriteArrayField("tableChanges", func() {
				// return early if there is no table changes
				if tableName == "" || e.GetDDLType() == timodel.ActionTruncateTable {
					return
				}
				jWriter.WriteObjectElement(func() {
					// Describes the kind of change. The value is one of the following:
					// CREATE: Table created.
					// ALTER: Table modified.
					// DROP: Table deleted.
					jWriter.WriteStringField("type", changeType)
					// In the case of a table rename, this identifier is a concatenation of <old>,<new> table names.
					switch e.GetDDLType() {
					case timodel.ActionRenameTable:
						jWriter.WriteStringField("id", fmt.Sprintf("\"%s\".\"%s\",\"%s\".\"%s\"",
							e.ExtraSchemaName,
							e.ExtraTableName,
							dbName,
							tableName))
					case timodel.ActionExchangeTablePartition:
						jWriter.WriteStringField("id", fmt.Sprintf("\"%s\".\"%s\"",
							e.ExtraSchemaName,
							e.ExtraTableName))
					case timodel.ActionDropTable:
						jWriter.WriteStringField("id", fmt.Sprintf("\"%s\".\"%s\"",
							dbName,
							tableName))
						// return early if there is no table info
						jWriter.WriteNullField("table")
						return
					default:
						jWriter.WriteStringField("id", fmt.Sprintf("\"%s\".\"%s\"",
							dbName,
							tableName))
					}
					jWriter.WriteObjectField("table", func() {
						jWriter.WriteStringField("defaultCharsetName", e.TableInfo.Charset)
						jWriter.WriteArrayField("primaryKeyColumnNames", func() {
							for _, pk := range e.TableInfo.GetPrimaryKeyColumnNames() {
								jWriter.WriteStringElement(pk)
							}
						})
						jWriter.WriteArrayField("columns", func() {
							parseColumns(e.Query, e.TableInfo.GetColumns())
							for pos, col := range e.TableInfo.GetColumns() {
								if col.Hidden {
									continue
								}
								jWriter.WriteObjectElement(func() {
									flag := col.GetFlag()
									jdbcType := common.MySQLType2JdbcType(col.GetType(), mysql.HasBinaryFlag(flag))
									expression, name := getExpressionAndName(col.FieldType)
									jWriter.WriteStringField("name", col.Name.O)
									jWriter.WriteIntField("jdbcType", int(jdbcType))
									jWriter.WriteNullField("nativeType")
									if col.Comment != "" {
										jWriter.WriteStringField("comment", col.Comment)
									} else {
										jWriter.WriteNullField("comment")
									}
									if col.DefaultValue == nil {
										jWriter.WriteNullField("defaultValueExpression")
									} else {
										v, ok := col.DefaultValue.(string)
										if ok {
											if strings.ToUpper(v) == "CURRENT_TIMESTAMP" {
												// https://debezium.io/documentation/reference/3.0/connectors/mysql.html#mysql-temporal-types
												jWriter.WriteAnyField("defaultValueExpression", "1970-01-01 00:00:00")
											} else if v == "<nil>" {
												jWriter.WriteNullField("defaultValueExpression")
											} else if col.DefaultValueBit != nil {
												jWriter.WriteStringField("defaultValueExpression", parseBit(v, col.GetFlen()))
											} else {
												jWriter.WriteStringField("defaultValueExpression", v)
											}
										} else {
											jWriter.WriteAnyField("defaultValueExpression", col.DefaultValue)
										}
									}
									elems := col.GetElems()
									if len(elems) != 0 {
										// Format is ENUM ('e1', 'e2') or SET ('e1', 'e2')
										jWriter.WriteArrayField("enumValues", func() {
											for _, ele := range elems {
												jWriter.WriteStringElement(fmt.Sprintf("'%s'", ele))
											}
										})
									} else {
										jWriter.WriteNullField("enumValues")
									}

									jWriter.WriteStringField("typeName", name)
									jWriter.WriteStringField("typeExpression", expression)

									charsetName := getCharset(col.FieldType)
									if charsetName != "" {
										jWriter.WriteStringField("charsetName", charsetName)
									} else {
										jWriter.WriteNullField("charsetName")
									}

									length := getLen(col.FieldType)
									if length != -1 {
										jWriter.WriteIntField("length", length)
									} else {
										jWriter.WriteNullField("length")
									}

									scale := getScale(col.FieldType)
									if scale != -1 {
										jWriter.WriteFloat64Field("scale", scale)
									} else {
										jWriter.WriteNullField("scale")
									}
									jWriter.WriteIntField("position", pos+1)
									jWriter.WriteBoolField("optional", !mysql.HasNotNullFlag(flag))

									updateNowWithTimestamp := mysql.HasOnUpdateNowFlag(flag) && jdbcType == common.JavaSQLTypeTIMESTAMP_WITH_TIMEZONE
									autoIncrementFlag := mysql.HasAutoIncrementFlag(flag) || updateNowWithTimestamp

									jWriter.WriteBoolField("autoIncremented", autoIncrementFlag)
									jWriter.WriteBoolField("generated", autoIncrementFlag)
								})
							}
						})
						jWriter.WriteNullField("comment")
					})
				})
			})
		})

		if !c.config.DebeziumDisableSchema {
			jWriter.WriteObjectField("schema", func() {
				jWriter.WriteBoolField("optional", false)
				jWriter.WriteStringField("type", "struct")
				jWriter.WriteIntField("version", 1)
				jWriter.WriteStringField("name", "io.debezium.connector.mysql.SchemaChangeValue")
				jWriter.WriteArrayField("fields", func() {
					c.writeSourceSchema(jWriter)
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("field", "ts_ms")
						jWriter.WriteBoolField("optional", false)
						jWriter.WriteStringField("type", "int64")
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("field", "databaseName")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("type", "string")
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("field", "schemaName")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("type", "string")
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("field", "ddl")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("type", "string")
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("field", "tableChanges")
						jWriter.WriteBoolField("optional", false)
						jWriter.WriteStringField("type", "array")
						jWriter.WriteObjectField("items", func() {
							jWriter.WriteStringField("name", "io.debezium.connector.schema.Change")
							jWriter.WriteBoolField("optional", false)
							jWriter.WriteStringField("type", "struct")
							jWriter.WriteIntField("version", 1)
							jWriter.WriteArrayField("fields", func() {
								jWriter.WriteObjectElement(func() {
									jWriter.WriteStringField("field", "type")
									jWriter.WriteBoolField("optional", false)
									jWriter.WriteStringField("type", "string")
								})
								jWriter.WriteObjectElement(func() {
									jWriter.WriteStringField("field", "id")
									jWriter.WriteBoolField("optional", false)
									jWriter.WriteStringField("type", "string")
								})
								jWriter.WriteObjectElement(func() {
									jWriter.WriteStringField("field", "table")
									jWriter.WriteBoolField("optional", true)
									jWriter.WriteStringField("type", "struct")
									jWriter.WriteStringField("name", "io.debezium.connector.schema.Table")
									jWriter.WriteIntField("version", 1)
									jWriter.WriteArrayField("fields", func() {
										jWriter.WriteObjectElement(func() {
											jWriter.WriteStringField("field", "defaultCharsetName")
											jWriter.WriteBoolField("optional", true)
											jWriter.WriteStringField("type", "string")
										})
										jWriter.WriteObjectElement(func() {
											jWriter.WriteStringField("field", "primaryKeyColumnNames")
											jWriter.WriteBoolField("optional", true)
											jWriter.WriteStringField("type", "array")
											jWriter.WriteObjectField("items", func() {
												jWriter.WriteStringField("type", "string")
												jWriter.WriteBoolField("optional", false)
											})
										})
										jWriter.WriteObjectElement(func() {
											jWriter.WriteStringField("field", "columns")
											jWriter.WriteBoolField("optional", false)
											jWriter.WriteStringField("type", "array")
											jWriter.WriteObjectField("items", func() {
												jWriter.WriteStringField("name", "io.debezium.connector.schema.Column")
												jWriter.WriteBoolField("optional", false)
												jWriter.WriteStringField("type", "struct")
												jWriter.WriteIntField("version", 1)
												jWriter.WriteArrayField("fields", func() {
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "name")
														jWriter.WriteBoolField("optional", false)
														jWriter.WriteStringField("type", "string")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "jdbcType")
														jWriter.WriteBoolField("optional", false)
														jWriter.WriteStringField("type", "int32")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "nativeType")
														jWriter.WriteBoolField("optional", true)
														jWriter.WriteStringField("type", "int32")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "typeName")
														jWriter.WriteBoolField("optional", false)
														jWriter.WriteStringField("type", "string")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "typeExpression")
														jWriter.WriteBoolField("optional", true)
														jWriter.WriteStringField("type", "string")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "charsetName")
														jWriter.WriteBoolField("optional", true)
														jWriter.WriteStringField("type", "string")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "length")
														jWriter.WriteBoolField("optional", true)
														jWriter.WriteStringField("type", "int32")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "scale")
														jWriter.WriteBoolField("optional", true)
														jWriter.WriteStringField("type", "int32")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "position")
														jWriter.WriteBoolField("optional", false)
														jWriter.WriteStringField("type", "int32")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "optional")
														jWriter.WriteBoolField("optional", true)
														jWriter.WriteStringField("type", "boolean")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "autoIncremented")
														jWriter.WriteBoolField("optional", true)
														jWriter.WriteStringField("type", "boolean")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "generated")
														jWriter.WriteBoolField("optional", true)
														jWriter.WriteStringField("type", "boolean")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "comment")
														jWriter.WriteBoolField("optional", true)
														jWriter.WriteStringField("type", "string")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "defaultValueExpression")
														jWriter.WriteBoolField("optional", true)
														jWriter.WriteStringField("type", "string")
													})
													jWriter.WriteObjectElement(func() {
														jWriter.WriteStringField("field", "enumValues")
														jWriter.WriteBoolField("optional", true)
														jWriter.WriteStringField("type", "array")
														jWriter.WriteObjectField("items", func() {
															jWriter.WriteStringField("type", "string")
															jWriter.WriteBoolField("optional", false)
														})
													})
												})
											})
										})
										jWriter.WriteObjectElement(func() {
											jWriter.WriteStringField("field", "comment")
											jWriter.WriteBoolField("optional", true)
											jWriter.WriteStringField("type", "string")
										})
									})
								})
							})
						})
					})
				})
			})
		}
	})
	return err
}

// EncodeCheckpointEvent encode checkpointTs into debezium change event
func (c *dbzCodec) EncodeCheckpointEvent(
	ts uint64,
	keyDest io.Writer,
	dest io.Writer,
) error {
	keyJWriter := util.BorrowJSONWriter(keyDest)
	jWriter := util.BorrowJSONWriter(dest)
	defer util.ReturnJSONWriter(keyJWriter)
	defer util.ReturnJSONWriter(jWriter)
	commitTime := oracle.GetTimeFromTS(ts)
	var err error
	// message key
	keyJWriter.WriteObject(func() {
		keyJWriter.WriteObjectField("payload", func() {})
		if !c.config.DebeziumDisableSchema {
			keyJWriter.WriteObjectField("schema", func() {
				keyJWriter.WriteStringField("type", "struct")
				keyJWriter.WriteStringField("name",
					fmt.Sprintf("%s.%s.Key", common.SanitizeName(c.clusterID), "watermark"))
				keyJWriter.WriteBoolField("optional", false)
				keyJWriter.WriteArrayField("fields", func() {
				})
			})
		}
	})
	// message value
	jWriter.WriteObject(func() {
		jWriter.WriteObjectField("payload", func() {
			jWriter.WriteObjectField("source", func() {
				jWriter.WriteStringField("version", "2.4.0.Final")
				jWriter.WriteStringField("connector", "TiCDC")
				jWriter.WriteStringField("name", c.clusterID)
				// ts_ms: In the source object, ts_ms indicates the time that the change was made in the database.
				// https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
				jWriter.WriteInt64Field("ts_ms", commitTime.UnixMilli())
				// snapshot field is a string of true,last,false,incremental
				jWriter.WriteStringField("snapshot", "false")
				jWriter.WriteStringField("db", "")
				jWriter.WriteStringField("table", "")
				jWriter.WriteInt64Field("server_id", 0)
				jWriter.WriteNullField("gtid")
				jWriter.WriteStringField("file", "")
				jWriter.WriteInt64Field("pos", 0)
				jWriter.WriteInt64Field("row", 0)
				jWriter.WriteInt64Field("thread", 0)
				jWriter.WriteNullField("query")

				// The followings are TiDB extended fields
				jWriter.WriteUint64Field("commit_ts", ts)
				jWriter.WriteStringField("cluster_id", c.clusterID)
			})

			// ts_ms: displays the time at which the connector processed the event
			// https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-create-events
			jWriter.WriteInt64Field("ts_ms", c.nowFunc().UnixMilli())
			jWriter.WriteNullField("transaction")
			jWriter.WriteStringField("op", "m")
		})

		if !c.config.DebeziumDisableSchema {
			jWriter.WriteObjectField("schema", func() {
				jWriter.WriteStringField("type", "struct")
				jWriter.WriteBoolField("optional", false)
				jWriter.WriteStringField("name",
					fmt.Sprintf("%s.%s.Envelope", common.SanitizeName(c.clusterID), "watermark"))
				jWriter.WriteIntField("version", 1)
				jWriter.WriteArrayField("fields", func() {
					c.writeSourceSchema(jWriter)
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "string")
						jWriter.WriteBoolField("optional", false)
						jWriter.WriteStringField("field", "op")
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "int64")
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("field", "ts_ms")
					})
					jWriter.WriteObjectElement(func() {
						jWriter.WriteStringField("type", "struct")
						jWriter.WriteArrayField("fields", func() {
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "string")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "id")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "int64")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "total_order")
							})
							jWriter.WriteObjectElement(func() {
								jWriter.WriteStringField("type", "int64")
								jWriter.WriteBoolField("optional", false)
								jWriter.WriteStringField("field", "data_collection_order")
							})
						})
						jWriter.WriteBoolField("optional", true)
						jWriter.WriteStringField("name", "event.block")
						jWriter.WriteIntField("version", 1)
						jWriter.WriteStringField("field", "transaction")
					})
				})
			})
		}
	})
	return err
}
