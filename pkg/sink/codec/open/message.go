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
	commonType "github.com/pingcap/ticdc/pkg/common"
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
	WhereHandle *bool  `json:"h,omitempty"`
	Flag        uint64 `json:"f"`
	Value       any    `json:"v"`
}

// formatColumn formats a codec column.
func formatColumn(c column, ft types.FieldType) column {
	var err error
	switch c.Type {
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		str := c.Value.(string)
		if isBinary(c.Flag) {
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
			if isUnsigned(c.Flag) {
				c.Value, err = strconv.ParseUint(s.String(), 10, 64)
			} else {
				c.Value, err = strconv.ParseInt(s.String(), 10, 64)
			}
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
			// is it possible be the float64?
		} else if f, ok := c.Value.(float64); ok {
			if isUnsigned(c.Flag) {
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

const (
	// binaryFlag means the column charset is binary
	binaryFlag uint64 = 1 << iota

	// handleKeyFlag means the column is selected as the handle key
	// The handleKey is chosen by the following rules in the order:
	// 1. if the table has primary key, it's the handle key.
	// 2. If the table has not null unique key, it's the handle key.
	// 3. If the table has no primary key and no not null unique key, it has no handleKey.
	handleKeyFlag

	// generatedColumnFlag means the column is a generated column
	generatedColumnFlag

	// primaryKeyFlag means the column is primary key
	primaryKeyFlag

	// uniqueKeyFlag means the column is unique key
	uniqueKeyFlag

	// multipleKeyFlag means the column is multiple key
	multipleKeyFlag

	// nullableFlag means the column is nullable
	nullableFlag

	// unsignedFlag means the column stores an unsigned integer
	unsignedFlag
)

func isBinary(flag uint64) bool {
	return flag&binaryFlag != 0
}

func isPrimary(flag uint64) bool {
	return flag&primaryKeyFlag != 0
}

func isUnique(flag uint64) bool {
	return flag&uniqueKeyFlag != 0
}

func isMultiKey(flag uint64) bool {
	return flag&multipleKeyFlag != 0
}

func isNullable(flag uint64) bool {
	return flag&nullableFlag != 0
}

func isUnsigned(flag uint64) bool {
	return flag&unsignedFlag != 0
}

func initColumnFlags(tableInfo *commonType.TableInfo) map[string]uint64 {
	result := make(map[string]uint64, len(tableInfo.GetColumns()))
	for _, col := range tableInfo.GetColumns() {
		var flag uint64
		if col.GetCharset() == "binary" {
			flag |= binaryFlag
		}
		origin := col.GetFlag()
		if col.IsGenerated() {
			flag |= generatedColumnFlag
		}
		if mysql.HasUniKeyFlag(origin) {
			flag |= uniqueKeyFlag
		}
		if mysql.HasPriKeyFlag(origin) {
			flag |= primaryKeyFlag
			if tableInfo.PKIsHandle() {
				flag |= handleKeyFlag
			}
		}
		if !mysql.HasNotNullFlag(origin) {
			flag |= nullableFlag
		}
		if mysql.HasMultipleKeyFlag(origin) {
			flag |= multipleKeyFlag
		}
		if mysql.HasUnsignedFlag(origin) {
			flag |= unsignedFlag
		}
		result[col.Name.O] = flag
	}

	// In TiDB, just as in MySQL, only the first column of an index can be marked as "multiple key" or "unique key",
	// and only the first column of a unique index may be marked as "unique key".
	// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html.
	// Yet if an index has multiple columns, we would like to easily determine that all those columns are indexed,
	// which is crucial for the completeness of the information we pass to the downstream.
	// Therefore, instead of using the MySQL standard,
	// we made our own decision to mark all columns in an index with the appropriate flag(s).
	for _, idxInfo := range tableInfo.GetIndices() {
		for _, idxCol := range idxInfo.Columns {
			flag := result[idxCol.Name.O]
			if idxInfo.Primary {
				flag |= primaryKeyFlag
			} else if idxInfo.Unique {
				flag |= uniqueKeyFlag
			}
			if len(idxInfo.Columns) > 1 {
				flag |= multipleKeyFlag
			}
			colID := tableInfo.ForceGetColumnIDByName(idxCol.Name.O)
			if tableInfo.IsHandleKey(colID) {
				flag |= handleKeyFlag
			}
			result[idxCol.Name.O] = flag
		}
	}
	return result
}
