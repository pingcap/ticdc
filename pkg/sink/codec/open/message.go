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
	"encoding/json"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/parser/mysql"
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

type messageRow struct {
	Update     map[string]column `json:"u,omitempty"`
	PreColumns map[string]column `json:"p,omitempty"`
	Delete     map[string]column `json:"d,omitempty"`
}

// only for test
func (m *messageRow) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, errors.WrapError(errors.ErrMarshalFailed, err)
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

func isHandle(flag uint64) bool {
	return flag&handleKeyFlag != 0
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

func isGenerated(flag uint64) bool {
	return flag&generatedColumnFlag != 0
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
			colInfo := tableInfo.GetColumns()[idxCol.Offset]
			if colInfo.IsGenerated() {
				continue
			}
			if tableInfo.IsHandleKey(colInfo.ID) {
				flag |= handleKeyFlag
			}
			result[idxCol.Name.O] = flag
		}
	}
	return result
}
