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

// Package metering measures canonical SQL values selected by a sink's encoder.
// It does not select columns, count delivery attempts, or confirm delivery.
package metering

import (
	"fmt"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// RuleVersion1 identifies the canonical rules in the traffic metering specification.
const RuleVersion1 uint64 = 1

// MeasureValueV1 returns the canonical bytes for one actual value encoding.
// row and offset refer to an already decoded TiCDC row; typeCode is its SQL base
// type. Callers own selection and accumulation, including repeated encodings.
// Existing rules must retain their results across TiDB dependency upgrades.
func MeasureValueV1(row chunk.Row, offset int, typeCode byte) (uint64, error) {
	if row.IsNull(offset) {
		return 1, nil
	}
	switch typeCode {
	case mysql.TypeTiny:
		return 1, nil
	case mysql.TypeShort, mysql.TypeYear:
		return 2, nil
	case mysql.TypeInt24:
		return 3, nil
	case mysql.TypeLong, mysql.TypeFloat, mysql.TypeDate, mysql.TypeNewDate:
		return 4, nil
	case mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeTimestamp:
		return 8, nil
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		return uint64(len(row.GetBytes(offset))), nil
	case mysql.TypeJSON:
		return uint64(len(row.GetJSON(offset).Value)), nil
	case mysql.TypeNewDecimal:
		size, err := row.GetMyDecimal(offset).HashKeySize()
		if err != nil {
			return 0, errors.WrapError(errors.ErrEncodeFailed, err)
		}
		return uint64(size), nil
	case mysql.TypeBit:
		value := row.GetBytes(offset)
		for len(value) > 1 && value[0] == 0 {
			value = value[1:]
		}
		return uint64(max(1, len(value))), nil
	case mysql.TypeEnum:
		return uint64(len(row.GetEnum(offset).Name)), nil
	case mysql.TypeSet:
		return uint64(len(row.GetSet(offset).Name)), nil
	case mysql.TypeTiDBVectorFloat32:
		return 4 * uint64(row.GetVectorFloat32(offset).Len()), nil
	default:
		return 0, errors.ErrEncodeFailed.GenWithStackByArgs(fmt.Sprintf("unsupported traffic metering SQL type %d", typeCode))
	}
}
