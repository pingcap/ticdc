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

package open

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

type messageKey struct {
	Ts        uint64             `json:"ts"`
	Schema    string             `json:"scm,omitempty"`
	Table     string             `json:"tbl,omitempty"`
	RowID     int64              `json:"rid,omitempty"`
	Partition *int64             `json:"ptn,omitempty"`
	Type      common.MessageType `json:"t"`
	// Only Handle Key Columns encoded in the message's value part.
	OnlyHandleKey bool `json:"ohk,omitempty"`

	// Claim check location for the message
	ClaimCheckLocation string `json:"ccl,omitempty"`
}

// Decode codes a message key from a byte slice.
func (m *messageKey) Decode(data []byte) {
	err := json.Unmarshal(data, m)
	if err != nil {
		log.Panic("decode message key failed", zap.Any("data", data), zap.Error(err))
	}
}

// column is a type only used in codec internally.
type column struct {
	Type byte `json:"t"`
	// Deprecated: please use Flag instead.
	WhereHandle *bool `json:"h,omitempty"`
	Flag        uint  `json:"f"`
	Value       any   `json:"v"`
}

// formatColumn formats a codec column.
func formatColumn(c column, ft types.FieldType) column {
	var err error
	switch c.Type {
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		str := c.Value.(string)
		if mysql.HasBinaryFlag(c.Flag) {
			str, err = strconv.Unquote("\"" + str + "\"")
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("value", str), zap.Error(err))
			}
		}
		c.Value = []byte(str)
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob,
		mysql.TypeLongBlob, mysql.TypeBlob:
		if s, ok := c.Value.(string); ok {
			c.Value, err = base64.StdEncoding.DecodeString(s)
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		s, ok := c.Value.(json.Number)
		if !ok {
			log.Panic("float / double not json.Number, please report a bug", zap.Any("value", c.Value))
		}
		c.Value, err = s.Float64()
		if err != nil {
			log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
		}
		if c.Type == mysql.TypeFloat {
			c.Value = float32(c.Value.(float64))
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		if s, ok := c.Value.(json.Number); ok {
			if mysql.HasUnsignedFlag(c.Flag) {
				c.Value, err = strconv.ParseUint(s.String(), 10, 64)
			} else {
				c.Value, err = strconv.ParseInt(s.String(), 10, 64)
			}
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
			// is it possible be the float64?
		} else if f, ok := c.Value.(float64); ok {
			if mysql.HasUnsignedFlag(c.Flag) {
				c.Value = uint64(f)
			} else {
				c.Value = int64(f)
			}
		}
	case mysql.TypeYear:
		c.Value, err = c.Value.(json.Number).Int64()
		if err != nil {
			log.Panic("invalid column value for year", zap.Any("value", c.Value), zap.Error(err))
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		c.Value, err = tiTypes.ParseTime(tiTypes.DefaultStmtNoWarningContext, c.Value.(string), ft.GetType(), ft.GetDecimal())
		if err != nil {
			log.Panic("invalid column value for date / datetime / timestamp", zap.Any("value", c.Value), zap.Error(err))
		}
	// todo: shall we also convert timezone for the mysql.TypeTimestamp ?
	//if mysqlType == mysql.TypeTimestamp && decoder.loc != nil && !t.IsZero() {
	//	err = t.ConvertTimeZone(time.UTC, decoder.loc)
	//	if err != nil {
	//		log.Panic("convert timestamp to local timezone failed", zap.Any("rawValue", rawValue), zap.Error(err))
	//	}
	//}
	case mysql.TypeDuration:
		c.Value, _, err = tiTypes.ParseDuration(tiTypes.DefaultStmtNoWarningContext, c.Value.(string), ft.GetDecimal())
		if err != nil {
			log.Panic("invalid column value for duration", zap.Any("value", c.Value), zap.Error(err))
		}
	case mysql.TypeBit:
		intVal, err := c.Value.(json.Number).Int64()
		if err != nil {
			log.Panic("invalid column value for the bit type", zap.Any("value", c.Value), zap.Error(err))
		}
		// todo: shall we get the flen to make it compatible to the MySQL Sink?
		// byteSize := (ft.GetFlen() + 7) >> 3
		c.Value = tiTypes.NewBinaryLiteralFromUint(uint64(intVal), -1)
	case mysql.TypeEnum:
		var enumValue int64
		enumValue, err = c.Value.(json.Number).Int64()
		if err != nil {
			log.Panic("invalid column value for enum", zap.Any("value", c.Value), zap.Error(err))
		}
		// only enum's value accessed by the MySQL Sink, and lack the elements, so let's make a compromise.
		c.Value = tiTypes.Enum{
			Value: uint64(enumValue),
		}
	case mysql.TypeSet:
		var setValue int64
		setValue, err = c.Value.(json.Number).Int64()
		if err != nil {
			log.Panic("invalid column value for set", zap.Any("value", c.Value), zap.Error(err))
		}
		// only set's value accessed by the MySQL Sink, and lack the elements, so let's make a compromise.
		c.Value = tiTypes.Set{
			Value: uint64(setValue),
		}
	case mysql.TypeJSON:
		c.Value, err = tiTypes.ParseBinaryJSONFromString(c.Value.(string))
		if err != nil {
			log.Panic("invalid column value for json", zap.Any("value", c.Value), zap.Error(err))
		}
	case mysql.TypeNewDecimal:
		dec := new(tiTypes.MyDecimal)
		err = dec.FromString([]byte(c.Value.(string)))
		if err != nil {
			log.Panic("invalid column value for decimal", zap.Any("value", c.Value), zap.Error(err))
		}
		//dec.GetDigitsFrac()
		//// workaround the decimal `digitInt` field incorrect problem.
		//bin, err := dec.ToBin(ft.GetFlen(), ft.GetDecimal())
		//if err != nil {
		//	log.Panic("convert decimal to binary failed", zap.Any("value", c.Value), zap.Error(err))
		//}
		//_, err = dec.FromBin(bin, ft.GetFlen(), ft.GetDecimal())
		//if err != nil {
		//	log.Panic("convert binary to decimal failed", zap.Any("value", c.Value), zap.Error(err))
		//}
		c.Value = dec
	case mysql.TypeTiDBVectorFloat32:
		c.Value, err = tiTypes.ParseVectorFloat32(c.Value.(string))
		if err != nil {
			log.Panic("invalid column value for vector float32", zap.Any("value", c.Value), zap.Error(err))
		}
	default:
		log.Panic("unknown data type found", zap.Any("type", c.Type), zap.Any("value", c.Value))
	}
	return c
}

type messageRow struct {
	Update     map[string]column `json:"u,omitempty"`
	PreColumns map[string]column `json:"p,omitempty"`
	Delete     map[string]column `json:"d,omitempty"`
}

func (m *messageRow) decode(data []byte) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(m)
	if err != nil {
		log.Panic("decode message row failed", zap.Any("data", data), zap.Error(err))
	}
}
