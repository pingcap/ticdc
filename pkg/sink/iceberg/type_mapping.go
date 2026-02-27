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

package iceberg

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

type mappedColumnType struct {
	icebergType string
	arrowType   arrow.DataType
}

func mapTiDBFieldType(ft *types.FieldType) mappedColumnType {
	if ft == nil {
		return mappedColumnType{icebergType: "string", arrowType: arrow.BinaryTypes.String}
	}

	tp := ft.GetType()
	unsigned := mysql.HasUnsignedFlag(ft.GetFlag())
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeYear:
		if unsigned && tp == mysql.TypeLong {
			return mappedColumnType{icebergType: "long", arrowType: arrow.PrimitiveTypes.Int64}
		}
		return mappedColumnType{icebergType: "int", arrowType: arrow.PrimitiveTypes.Int32}
	case mysql.TypeLonglong:
		if unsigned {
			return mappedColumnType{
				icebergType: "decimal(20,0)",
				arrowType:   &arrow.Decimal128Type{Precision: 20, Scale: 0},
			}
		}
		return mappedColumnType{icebergType: "long", arrowType: arrow.PrimitiveTypes.Int64}
	case mysql.TypeFloat:
		return mappedColumnType{icebergType: "float", arrowType: arrow.PrimitiveTypes.Float32}
	case mysql.TypeDouble:
		return mappedColumnType{icebergType: "double", arrowType: arrow.PrimitiveTypes.Float64}
	case mysql.TypeNewDecimal:
		prec := int32(ft.GetFlen())
		scale := int32(ft.GetDecimal())
		if prec <= 0 || prec > 38 || scale < 0 || scale > prec {
			return mappedColumnType{icebergType: "string", arrowType: arrow.BinaryTypes.String}
		}
		return mappedColumnType{
			icebergType: fmt.Sprintf("decimal(%d,%d)", prec, scale),
			arrowType:   &arrow.Decimal128Type{Precision: prec, Scale: scale},
		}
	case mysql.TypeDate:
		return mappedColumnType{icebergType: "date", arrowType: arrow.PrimitiveTypes.Date32}
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		return mappedColumnType{icebergType: "timestamp", arrowType: &arrow.TimestampType{Unit: arrow.Microsecond}}
	case mysql.TypeJSON:
		return mappedColumnType{icebergType: "string", arrowType: arrow.BinaryTypes.String}
	case mysql.TypeBit:
		return mappedColumnType{icebergType: "long", arrowType: arrow.PrimitiveTypes.Int64}
	case mysql.TypeEnum, mysql.TypeSet:
		return mappedColumnType{icebergType: "string", arrowType: arrow.BinaryTypes.String}
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return mappedColumnType{icebergType: "binary", arrowType: arrow.BinaryTypes.Binary}
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			return mappedColumnType{icebergType: "binary", arrowType: arrow.BinaryTypes.Binary}
		}
		return mappedColumnType{icebergType: "string", arrowType: arrow.BinaryTypes.String}
	default:
		return mappedColumnType{icebergType: "string", arrowType: arrow.BinaryTypes.String}
	}
}

func icebergTypeToAvroType(icebergType string) any {
	switch icebergType {
	case "int", "long", "float", "double", "string", "boolean":
		return icebergType
	case "binary":
		return "bytes"
	case "date":
		return map[string]any{
			"type":        "int",
			"logicalType": "date",
		}
	case "timestamp":
		return map[string]any{
			"type":        "long",
			"logicalType": "timestamp-micros",
		}
	default:
		if strings.HasPrefix(icebergType, "decimal(") && strings.HasSuffix(icebergType, ")") {
			inner := strings.TrimSuffix(strings.TrimPrefix(icebergType, "decimal("), ")")
			parts := strings.Split(inner, ",")
			if len(parts) == 2 {
				prec, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
				scale, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
				if err1 == nil && err2 == nil && prec > 0 && scale >= 0 {
					return map[string]any{
						"type":        "bytes",
						"logicalType": "decimal",
						"precision":   prec,
						"scale":       scale,
					}
				}
			}
		}
		return "string"
	}
}
