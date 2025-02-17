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
	pCommon "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
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

// Encode encodes the message key to a byte slice.
func (m *messageKey) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, errors.WrapError(errors.ErrMarshalFailed, err)
}

// Decode codes a message key from a byte slice.
func (m *messageKey) Decode(data []byte) error {
	return errors.WrapError(errors.ErrUnmarshalFailed, json.Unmarshal(data, m))
}

// column is a type only used in codec internally.
type column struct {
	Type byte `json:"t"`
	// Deprecated: please use Flag instead.
	WhereHandle *bool                  `json:"h,omitempty"`
	Flag        pCommon.ColumnFlagType `json:"f"`
	Value       any                    `json:"v"`
}

// FromRowChangeColumn converts from a row changed column to a codec column.
func (c *column) FromRowChangeColumn(col *pCommon.Column) {
	c.Type = col.Type
	c.Flag = col.Flag
	if c.Flag.IsHandleKey() {
		whereHandle := true
		c.WhereHandle = &whereHandle
	}
	if col.Value == nil {
		c.Value = nil
		return
	}
	switch col.Type {
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		var str string
		switch col.Value.(type) {
		case []byte:
			str = string(col.Value.([]byte))
		case string:
			str = col.Value.(string)
		default:
			log.Panic("invalid column value, please report a bug", zap.Any("col", col))
		}
		if c.Flag.IsBinary() {
			str = strconv.Quote(str)
			str = str[1 : len(str)-1]
		}
		c.Value = str
	case mysql.TypeTiDBVectorFloat32:
		c.Value = col.Value.(types.VectorFloat32).String()
	default:
		c.Value = col.Value
	}
}

// ToRowChangeColumn converts from a codec column to a row changed column.
func (c *column) ToRowChangeColumn(name string) *pCommon.Column {
	col := new(pCommon.Column)
	col.Type = c.Type
	col.Flag = c.Flag
	col.Name = name
	col.Value = c.Value
	if c.Value == nil {
		return col
	}
	switch col.Type {
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		str := col.Value.(string)
		var err error
		if c.Flag.IsBinary() {
			str, err = strconv.Unquote("\"" + str + "\"")
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
		}
		col.Value = []byte(str)
	case mysql.TypeFloat:
		col.Value = float32(col.Value.(float64))
	case mysql.TypeYear:
		col.Value = int64(col.Value.(uint64))
	case mysql.TypeEnum, mysql.TypeSet:
		val, err := col.Value.(json.Number).Int64()
		if err != nil {
			log.Panic("invalid column value for enum, please report a bug",
				zap.Any("col", c), zap.Error(err))
		}
		col.Value = uint64(val)
	case mysql.TypeTiDBVectorFloat32:
	default:
	}
	return col
}

// FormatColumn formats a codec column.
func FormatColumn(c column) column {
	switch c.Type {
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob,
		mysql.TypeLongBlob, mysql.TypeBlob:
		if s, ok := c.Value.(string); ok {
			var err error
			c.Value, err = base64.StdEncoding.DecodeString(s)
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		if s, ok := c.Value.(json.Number); ok {
			f64, err := s.Float64()
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
			c.Value = f64
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
		if s, ok := c.Value.(json.Number); ok {
			var err error
			if c.Flag.IsUnsigned() {
				c.Value, err = strconv.ParseUint(s.String(), 10, 64)
			} else {
				c.Value, err = strconv.ParseInt(s.String(), 10, 64)
			}
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
		} else if f, ok := c.Value.(float64); ok {
			if c.Flag.IsUnsigned() {
				c.Value = uint64(f)
			} else {
				c.Value = int64(f)
			}
		}
	case mysql.TypeBit:
		if s, ok := c.Value.(json.Number); ok {
			intNum, err := s.Int64()
			if err != nil {
				log.Panic("invalid column value, please report a bug", zap.Any("col", c), zap.Error(err))
			}
			c.Value = uint64(intNum)
		}
	}
	return c
}

type messageRow struct {
	Update     map[string]column `json:"u,omitempty"`
	PreColumns map[string]column `json:"p,omitempty"`
	Delete     map[string]column `json:"d,omitempty"`
}

func (m *messageRow) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, errors.WrapError(errors.ErrMarshalFailed, err)
}

func (m *messageRow) decode(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(m)
	if err != nil {
		return errors.WrapError(errors.ErrUnmarshalFailed, err)
	}
	for colName, column := range m.Update {
		m.Update[colName] = FormatColumn(column)
	}
	for colName, column := range m.Delete {
		m.Delete[colName] = FormatColumn(column)
	}
	for colName, column := range m.PreColumns {
		m.PreColumns[colName] = FormatColumn(column)
	}
	return nil
}

type messageDDL struct {
	Query string             `json:"q"`
	Type  timodel.ActionType `json:"t"`
}

func (m *messageDDL) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, errors.WrapError(errors.ErrMarshalFailed, err)
}

func (m *messageDDL) decode(data []byte) error {
	return errors.WrapError(errors.ErrUnmarshalFailed, json.Unmarshal(data, m))
}
