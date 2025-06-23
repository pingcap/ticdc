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

func mysqlType2JavaType(t byte, isBinary bool) common.JavaSQLType {
	switch t {
	case mysql.TypeBit:
		return common.JavaSQLTypeBIT
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if isBinary {
			return common.JavaSQLTypeBLOB
		}
		return common.JavaSQLTypeCLOB
	case mysql.TypeVarchar, mysql.TypeVarString:
		if isBinary {
			return common.JavaSQLTypeBLOB
		}
		return common.JavaSQLTypeVARCHAR
	case mysql.TypeString:
		if isBinary {
			return common.JavaSQLTypeBLOB
		}
		return common.JavaSQLTypeCHAR
	case mysql.TypeEnum:
		return common.JavaSQLTypeINTEGER
	case mysql.TypeSet:
		return common.JavaSQLTypeBIT
	case mysql.TypeDate, mysql.TypeNewDate:
		return common.JavaSQLTypeDATE
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		return common.JavaSQLTypeTIMESTAMP
	case mysql.TypeDuration:
		return common.JavaSQLTypeTIME
	case mysql.TypeJSON:
		return common.JavaSQLTypeVARCHAR
	case mysql.TypeNewDecimal:
		return common.JavaSQLTypeDECIMAL
	case mysql.TypeInt24:
		return common.JavaSQLTypeINTEGER
	case mysql.TypeTiny:
		return common.JavaSQLTypeTINYINT
	case mysql.TypeShort:
		return common.JavaSQLTypeSMALLINT
	case mysql.TypeLong:
		return common.JavaSQLTypeINTEGER
	case mysql.TypeLonglong:
		return common.JavaSQLTypeBIGINT
	case mysql.TypeFloat:
		return common.JavaSQLTypeREAL
	case mysql.TypeDouble:
		return common.JavaSQLTypeDOUBLE
	case mysql.TypeYear:
		return common.JavaSQLTypeVARCHAR
	case mysql.TypeTiDBVectorFloat32:
		return common.JavaSQLTypeVARCHAR
	default:
	}
	return common.JavaSQLTypeVARCHAR
}

func formatColumnValue(row *chunk.Row, idx int, columnInfo *model.ColumnInfo) (string, common.JavaSQLType) {
	isBinary := mysql.HasBinaryFlag(columnInfo.GetFlag())
	javaType := mysqlType2JavaType(columnInfo.GetType(), isBinary)
	d := row.GetDatum(idx, &columnInfo.FieldType)
	if d.IsNull() {
		return "null", javaType
	}

	var value string
	switch columnInfo.GetType() {
	case mysql.TypeBit:
		uintValue, err := d.GetMysqlBit().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			log.Panic("failed to convert bit to int", zap.Any("data", d), zap.Error(err))
		}
		value = strconv.FormatUint(uintValue, 10)
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
		mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		bytes := d.GetBytes()
		if isBinary {
			decoded, err := bytesDecoder.Bytes(bytes)
			if err != nil {
				log.Panic("failed to decode bytes", zap.Any("bytes", bytes), zap.Error(err))
			}
			value = string(decoded)
		} else {
			value = string(bytes)
		}
	case mysql.TypeEnum:
		enumValue := d.GetMysqlEnum().Value
		value = fmt.Sprintf("%d", enumValue)
	case mysql.TypeSet:
		bitValue := d.GetMysqlSet().Value
		value = fmt.Sprintf("%d", bitValue)
	case mysql.TypeDate, mysql.TypeNewDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		value = d.GetMysqlTime().String()
	case mysql.TypeDuration:
		value = d.GetMysqlDuration().String()
	case mysql.TypeJSON:
		value = d.GetMysqlJSON().String()
	case mysql.TypeNewDecimal:
		value = d.GetMysqlDecimal().String()
	case mysql.TypeInt24:
		if mysql.HasUnsignedFlag(columnInfo.GetFlag()) {
			uintValue := d.GetUint64()
			value = strconv.FormatUint(uintValue, 10)
		} else {
			intValue := d.GetInt64()
			value = strconv.FormatInt(intValue, 10)
		}
	case mysql.TypeTiny:
		if mysql.HasUnsignedFlag(columnInfo.GetFlag()) {
			uintValue := d.GetUint64()
			if uintValue > math.MaxInt8 {
				javaType = common.JavaSQLTypeSMALLINT
			}
			value = strconv.FormatUint(uintValue, 10)
		} else {
			intValue := d.GetInt64()
			value = strconv.FormatInt(intValue, 10)
		}
	case mysql.TypeShort:
		if mysql.HasUnsignedFlag(columnInfo.GetFlag()) {
			uintValue := d.GetUint64()
			if uintValue > math.MaxInt16 {
				javaType = common.JavaSQLTypeINTEGER
			}
			value = strconv.FormatUint(uintValue, 10)
		} else {
			intValue := d.GetInt64()
			value = strconv.FormatInt(intValue, 10)
		}
	case mysql.TypeLong:
		if mysql.HasUnsignedFlag(columnInfo.GetFlag()) {
			uintValue := d.GetUint64()
			if uintValue > math.MaxInt32 {
				javaType = common.JavaSQLTypeBIGINT
			}
			value = strconv.FormatUint(uintValue, 10)
		} else {
			intValue := d.GetInt64()
			value = strconv.FormatInt(intValue, 10)
		}
	case mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(columnInfo.GetFlag()) {
			uintValue := d.GetUint64()
			if uintValue > math.MaxInt64 {
				javaType = common.JavaSQLTypeDECIMAL
			}
			value = strconv.FormatUint(uintValue, 10)
		} else {
			intValue := d.GetInt64()
			value = strconv.FormatInt(intValue, 10)
		}
	case mysql.TypeFloat:
		value = strconv.FormatFloat(float64(d.GetFloat32()), 'f', -1, 32)
	case mysql.TypeDouble:
		value = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64)
	case mysql.TypeYear:
		value = strconv.FormatInt(d.GetInt64(), 10)
	case mysql.TypeTiDBVectorFloat32:
		javaType = common.JavaSQLTypeVARCHAR
		if d.IsNull() {
			value = "null"
			break
		}
		value = d.GetVectorFloat32().String()
	default:
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		value = fmt.Sprintf("%v", d.GetValue())
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
