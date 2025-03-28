// Copyright 2022 PingCAP, Inc.
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
	"strconv"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/charmap"
)

// RowEventDecoder is an abstraction for events decoder
// this interface is only for testing now
type RowEventDecoder interface {
	// AddKeyValue add the received key and values to the decoder,
	// should be called before `HasNext`
	// decoder decode the key and value into the event format.
	AddKeyValue(key, value []byte) error

	// HasNext returns
	//     1. the type of the next event
	//     2. a bool if the next event is exist
	//     3. error
	HasNext() (MessageType, bool, error)

	// NextResolvedEvent returns the next resolved event if exists
	NextResolvedEvent() (uint64, error)

	// NextDMLEvent returns the next DML event if exists
	NextDMLEvent() (*commonEvent.DMLEvent, error)

	// NextDDLEvent returns the next DDL event if exists
	NextDDLEvent() (*commonEvent.DDLEvent, error)
}

func appendCol2Chunk(idx int, raw interface{}, ft types.FieldType, chk *chunk.Chunk) {
	mysqlType := ft.GetType()
	if raw == nil {
		chk.AppendNull(idx)
		return
	}

	rawValue, ok := raw.(string)
	if !ok {
		log.Panic("canal-json encoded message should have type in `string`")
	}
	if mysql.HasBinaryFlag(ft.GetFlag()) {
		// when encoding the `JavaSQLTypeBLOB`, use `ISO8859_1` decoder, now reverse it back.
		encoded, err := charmap.ISO8859_1.NewEncoder().String(rawValue)
		if err != nil {
			log.Panic("invalid column value, please report a bug", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		rawValue = encoded
	}
	switch mysqlType {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			value, err := strconv.ParseUint(rawValue, 10, 64)
			if err != nil {
				log.Panic("invalid column value for unsigned integer", zap.Any("rawValue", rawValue), zap.Error(err))
			}
			chk.AppendUint64(idx, value)
			return
		}
		value, err := strconv.ParseInt(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for integer", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendInt64(idx, value)
	case mysql.TypeYear:
		value, err := strconv.ParseInt(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for year", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendInt64(idx, value)
	case mysql.TypeFloat:
		value, err := strconv.ParseFloat(rawValue, 32)
		if err != nil {
			log.Panic("invalid column value for float", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendFloat32(idx, float32(value))
	case mysql.TypeDouble:
		value, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			log.Panic("invalid column value for double", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendFloat64(idx, value)
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		chk.AppendBytes(idx, []byte(rawValue))
	case mysql.TypeNewDecimal:
		value := new(types.MyDecimal)
		err := value.FromString([]byte(rawValue))
		if err != nil {
			log.Panic("invalid column value for decimal", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		// workaround the decimal `digitInt` field incorrect problem.
		bin, err := value.ToBin(ft.GetFlen(), ft.GetDecimal())
		if err != nil {
			log.Panic("convert decimal to binary failed", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		_, err = value.FromBin(bin, ft.GetFlen(), ft.GetDecimal())
		if err != nil {
			log.Panic("convert binary to decimal failed", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendMyDecimal(idx, value)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		t, err := types.ParseTime(types.DefaultStmtNoWarningContext, rawValue, ft.GetType(), ft.GetDecimal())
		if err != nil {
			log.Panic("invalid column value for time", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		// todo: shall we also convert timezone for the mysql.TypeTimestamp ?
		//if mysqlType == mysql.TypeTimestamp && decoder.loc != nil && !t.IsZero() {
		//	err = t.ConvertTimeZone(time.UTC, decoder.loc)
		//	if err != nil {
		//		log.Panic("convert timestamp to local timezone failed", zap.Any("rawValue", rawValue), zap.Error(err))
		//	}
		//}
		chk.AppendTime(idx, t)
	case mysql.TypeDuration:
		dur, _, err := types.ParseDuration(types.DefaultStmtNoWarningContext, rawValue, ft.GetDecimal())
		if err != nil {
			log.Panic("invalid column value for duration", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendDuration(idx, dur)
	case mysql.TypeEnum:
		enumValue, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for enum", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		enum, err := types.ParseEnumValue(ft.GetElems(), enumValue)
		if err != nil {
			log.Panic("parse enum value failed", zap.Any("rawValue", rawValue),
				zap.Any("enumValue", enumValue), zap.Error(err))
		}
		chk.AppendEnum(idx, enum)
	case mysql.TypeSet:
		value, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for set", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		setValue, err := types.ParseSetValue(ft.GetElems(), value)
		if err != nil {
			log.Panic("parse set value failed", zap.Any("rawValue", rawValue),
				zap.Any("setValue", setValue), zap.Error(err))
		}
		chk.AppendSet(idx, setValue)
	case mysql.TypeBit:
		value, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for bit", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		byteSize := (ft.GetFlen() + 7) >> 3
		chk.AppendBytes(idx, types.NewBinaryLiteralFromUint(value, byteSize))
	case mysql.TypeJSON:
		bj, err := types.ParseBinaryJSONFromString(rawValue)
		if err != nil {
			log.Panic("invalid column value for json", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendJSON(idx, bj)
	case mysql.TypeTiDBVectorFloat32:
		value, err := types.ParseVectorFloat32(rawValue)
		if err != nil {
			log.Panic("cannot parse vector32 value from string", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendVectorFloat32(idx, value)
	default:
		log.Panic("unknown column type", zap.Any("mysqlType", mysqlType), zap.Any("rawValue", rawValue))
	}
}

func AppendRow2Chunk(data map[string]interface{}, columns []*timodel.ColumnInfo, chk *chunk.Chunk) {
	for idx, col := range columns {
		raw := data[col.Name.O]
		appendCol2Chunk(idx, raw, col.FieldType, chk)
	}
}
