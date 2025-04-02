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

package common

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

var EmptyBytes = make([]byte, 0)

// ExtractColVal returns the value in the row
func ExtractColVal(row *chunk.Row, col *model.ColumnInfo, idx int) interface{} {
	if row.IsNull(idx) {
		return nil
	}
	switch col.GetType() {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		return row.GetTime(idx).String()
	case mysql.TypeDuration:
		return row.GetDuration(idx, 0).String()
	case mysql.TypeJSON:
		return row.GetJSON(idx).String()
	case mysql.TypeNewDecimal:
		d := row.GetMyDecimal(idx)
		if d == nil {
			// nil takes 0 byte.
			return nil
		}
		return d.String()
	case mysql.TypeEnum, mysql.TypeSet:
		return row.GetEnum(idx).Value
	case mysql.TypeBit:
		d := row.GetDatum(idx, &col.FieldType)
		dp := &d
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		result, err := dp.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			log.Panic("extract bit value failed", zap.Any("bit", dp.GetBinaryLiteral()), zap.Error(err))
		}
		return result
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		b := row.GetBytes(idx)
		if b == nil {
			b = EmptyBytes
		}
		// If the column value type is []byte and charset is not binary, we get its string
		// representation. Because if we use the byte array respresentation, the go-sql-driver
		// will automatically set `_binary` charset for that column, which is not expected.
		// See https://github.com/go-sql-driver/mysql/blob/ce134bfc/connection.go#L267
		if col.GetCharset() != "" && col.GetCharset() != charset.CharsetBin {
			if len(b) == 0 {
				return ""
			}
			return unsafe.String(&b[0], len(b))
		}
		return b
	case mysql.TypeFloat:
		b := row.GetFloat32(idx)
		if math.IsNaN(float64(b)) || math.IsInf(float64(b), 1) || math.IsInf(float64(b), -1) {
			warn := fmt.Sprintf("the value is invalid in column: %f", b)
			log.Warn(warn)
			b = 0
		}
		return b
	case mysql.TypeDouble:
		b := row.GetFloat64(idx)
		if math.IsNaN(b) || math.IsInf(b, 1) || math.IsInf(b, -1) {
			warn := fmt.Sprintf("the value is invalid in column: %f", b)
			log.Warn(warn)
			b = 0
		}
		return b
	case mysql.TypeTiDBVectorFloat32:
		b := row.GetVectorFloat32(idx).String()
		return b
	default:
		d := row.GetDatum(idx, &col.FieldType)
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		return d.GetValue()
	}
}
