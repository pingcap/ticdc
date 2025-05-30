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

package canal

import (
	"fmt"
	"math"
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	canal "github.com/pingcap/tiflow/proto/canal"
	"go.uber.org/zap"
)

func formatColumnValue(row *chunk.Row, idx int, columnInfo *model.ColumnInfo, flag uint) (string, common.JavaSQLType) {
	colType := columnInfo.GetType()

	var (
		value    string
		javaType common.JavaSQLType
	)
	switch colType {
	case mysql.TypeBit:
		javaType = common.JavaSQLTypeBIT
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			uintValue, err := d.GetMysqlBit().ToInt(types.DefaultStmtNoWarningContext)
			if err != nil {
				log.Panic("failed to convert bit to int", zap.Any("data", d), zap.Error(err))
			}
			value = strconv.FormatUint(uintValue, 10)
		}
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		bytesValue := row.GetBytes(idx)
		if mysql.HasBinaryFlag(flag) {
			javaType = common.JavaSQLTypeBLOB
		} else {
			javaType = common.JavaSQLTypeCLOB
		}
		if string(bytesValue) == "" {
			value = "null"
			break
		}

		if mysql.HasBinaryFlag(flag) {
			decoded, err := bytesDecoder.Bytes(bytesValue)
			if err != nil {
				log.Panic("failed to decode bytes", zap.Any("bytes", bytesValue), zap.Error(err))
			}
			value = string(decoded)
		} else {
			value = string(bytesValue)
		}
	case mysql.TypeVarchar, mysql.TypeVarString:
		bytesValue := row.GetBytes(idx)
		if mysql.HasBinaryFlag(flag) {
			javaType = common.JavaSQLTypeBLOB
		} else {
			javaType = common.JavaSQLTypeVARCHAR
		}

		if string(bytesValue) == "" {
			value = "null"
			break
		}
		if mysql.HasBinaryFlag(flag) {
			decoded, err := bytesDecoder.Bytes(bytesValue)
			if err != nil {
				log.Panic("failed to decode bytes", zap.Any("bytes", bytesValue), zap.Error(err))
			}
			value = string(decoded)
		} else {
			value = string(bytesValue)
		}
	case mysql.TypeString:
		bytesValue := row.GetBytes(idx)
		if mysql.HasBinaryFlag(flag) {
			javaType = common.JavaSQLTypeBLOB
		} else {
			javaType = common.JavaSQLTypeCHAR
		}
		if string(bytesValue) == "" {
			value = "null"
			break
		}
		if mysql.HasBinaryFlag(flag) {
			decoded, err := bytesDecoder.Bytes(bytesValue)
			if err != nil {
				log.Panic("failed to decode bytes", zap.Any("bytes", bytesValue), zap.Error(err))
			}
			value = string(decoded)
		} else {
			value = string(bytesValue)
		}
	case mysql.TypeEnum:
		javaType = common.JavaSQLTypeINTEGER
		enumValue := row.GetEnum(idx).Value
		if enumValue == 0 {
			value = "null"
		} else {
			value = fmt.Sprintf("%d", enumValue)
		}
	case mysql.TypeSet:
		javaType = common.JavaSQLTypeBIT
		bitValue := row.GetEnum(idx).Value
		if bitValue == 0 {
			value = "null"
		} else {
			value = fmt.Sprintf("%d", bitValue)
		}
	case mysql.TypeDate, mysql.TypeNewDate:
		javaType = common.JavaSQLTypeDATE
		timeValue := row.GetTime(idx)
		if timeValue.IsZero() {
			value = "null"
		} else {
			value = timeValue.String()
		}
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		javaType = common.JavaSQLTypeTIMESTAMP
		timeValue := row.GetTime(idx)
		if timeValue.IsZero() {
			value = "null"
		} else {
			value = timeValue.String()
		}
	case mysql.TypeDuration:
		javaType = common.JavaSQLTypeTIME
		durationValue := row.GetDuration(idx, 0)
		if durationValue.ToNumber().IsZero() {
			value = "null"
		} else {
			value = durationValue.String()
		}
	case mysql.TypeJSON:
		javaType = common.JavaSQLTypeVARCHAR
		// json needs null check before, otherwise it will panic.
		if row.IsNull(idx) {
			value = "null"
		} else {
			jsonValue := row.GetJSON(idx)
			if jsonValue.IsZero() {
				value = "null"
			} else {
				value = jsonValue.String()
			}
		}
	case mysql.TypeNewDecimal:
		javaType = common.JavaSQLTypeDECIMAL
		decimalValue := row.GetMyDecimal(idx)
		if decimalValue.IsZero() {
			value = "null"
		} else {
			value = decimalValue.String()
		}
	case mysql.TypeInt24:
		javaType = common.JavaSQLTypeINTEGER
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			if mysql.HasUnsignedFlag(flag) {
				uintValue := d.GetUint64()
				value = strconv.FormatUint(uintValue, 10)
			} else {
				intValue := d.GetInt64()
				value = strconv.FormatInt(intValue, 10)
			}
		}
	case mysql.TypeTiny:
		javaType = common.JavaSQLTypeTINYINT
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			if mysql.HasUnsignedFlag(flag) {
				uintValue := d.GetUint64()
				if uintValue > math.MaxInt8 {
					javaType = common.JavaSQLTypeSMALLINT
				}
				value = strconv.FormatUint(uintValue, 10)
			} else {
				intValue := d.GetInt64()
				value = strconv.FormatInt(intValue, 10)
			}
		}
	case mysql.TypeShort:
		javaType = common.JavaSQLTypeSMALLINT
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			if mysql.HasUnsignedFlag(flag) {
				uintValue := d.GetUint64()
				if uintValue > math.MaxInt16 {
					javaType = common.JavaSQLTypeINTEGER
				}
				value = strconv.FormatUint(uintValue, 10)
			} else {
				intValue := d.GetInt64()
				value = strconv.FormatInt(intValue, 10)
			}
		}
	case mysql.TypeLong:
		javaType = common.JavaSQLTypeINTEGER
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			if mysql.HasUnsignedFlag(flag) {
				uintValue := d.GetUint64()
				if uintValue > math.MaxInt32 {
					javaType = common.JavaSQLTypeBIGINT
				}
				value = strconv.FormatUint(uintValue, 10)
			} else {
				intValue := d.GetInt64()
				value = strconv.FormatInt(intValue, 10)
			}
		}
	case mysql.TypeLonglong:
		javaType = common.JavaSQLTypeBIGINT
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			if mysql.HasUnsignedFlag(flag) {
				uintValue := d.GetUint64()
				if uintValue > math.MaxInt64 {
					javaType = common.JavaSQLTypeDECIMAL
				}
				value = strconv.FormatUint(uintValue, 10)
			} else {
				intValue := d.GetInt64()
				value = strconv.FormatInt(intValue, 10)
			}
		}
	case mysql.TypeFloat:
		javaType = common.JavaSQLTypeREAL
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			floatValue := d.GetFloat32()
			value = strconv.FormatFloat(float64(floatValue), 'f', -1, 32)
		}
	case mysql.TypeDouble:
		javaType = common.JavaSQLTypeDOUBLE
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			floatValue := d.GetFloat64()
			value = strconv.FormatFloat(floatValue, 'f', -1, 64)
		}
	case mysql.TypeYear:
		javaType = common.JavaSQLTypeVARCHAR
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			yearValue := d.GetInt64()
			value = strconv.FormatInt(yearValue, 10)
		}
	case mysql.TypeTiDBVectorFloat32:
		javaType = common.JavaSQLTypeVARCHAR
		d := row.GetDatum(idx, &columnInfo.FieldType)
		value = d.GetVectorFloat32().String()
	default:
		javaType = common.JavaSQLTypeVARCHAR
		d := row.GetDatum(idx, &columnInfo.FieldType)
		if d.IsNull() {
			value = "null"
		} else {
			// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
			// Make specified convert upper if you need
			// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
			value = fmt.Sprintf("%v", d.GetValue())
		}
	}
	return value, javaType
}

// convert ts in tidb to timestamp(in ms) in canal
func convertToCanalTs(commitTs uint64) int64 {
	return int64(commitTs >> 18)
}

// get the canal EventType according to the DDLEvent
func convertDdlEventType(t byte) canal.EventType {
	// see https://github.com/alibaba/canal/blob/d53bfd7ee76f8fe6eb581049d64b07d4fcdd692d/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/ddl/DruidDdlParser.java#L59-L178
	switch model.ActionType(t) {
	case model.ActionCreateSchema, model.ActionDropSchema, model.ActionShardRowID, model.ActionCreateView,
		model.ActionDropView, model.ActionRecoverTable, model.ActionModifySchemaCharsetAndCollate,
		model.ActionLockTable, model.ActionUnlockTable, model.ActionRepairTable, model.ActionSetTiFlashReplica,
		model.ActionUpdateTiFlashReplicaStatus, model.ActionCreateSequence, model.ActionAlterSequence,
		model.ActionDropSequence, model.ActionModifyTableAutoIDCache, model.ActionRebaseAutoRandomBase:
		return canal.EventType_QUERY
	case model.ActionCreateTable:
		return canal.EventType_CREATE
	case model.ActionRenameTable, model.ActionRenameTables:
		return canal.EventType_RENAME
	case model.ActionAddIndex, model.ActionAddForeignKey, model.ActionAddPrimaryKey:
		return canal.EventType_CINDEX
	case model.ActionDropIndex, model.ActionDropForeignKey, model.ActionDropPrimaryKey:
		return canal.EventType_DINDEX
	case model.ActionAddColumn, model.ActionDropColumn, model.ActionModifyColumn, model.ActionRebaseAutoID,
		model.ActionSetDefaultValue, model.ActionModifyTableComment, model.ActionRenameIndex, model.ActionAddTablePartition,
		model.ActionDropTablePartition, model.ActionModifyTableCharsetAndCollate, model.ActionTruncateTablePartition,
		model.ActionAlterIndexVisibility, model.ActionMultiSchemaChange, model.ActionReorganizePartition,
		model.ActionAlterTablePartitioning, model.ActionRemovePartitioning,
		// AddColumns and DropColumns are removed in TiDB v6.2.0, see https://github.com/pingcap/tidb/pull/35862.
		model.ActionAddColumns, model.ActionDropColumns:
		return canal.EventType_ALTER
	case model.ActionDropTable:
		return canal.EventType_ERASE
	case model.ActionTruncateTable:
		return canal.EventType_TRUNCATE
	default:
		return canal.EventType_QUERY
	}
}
