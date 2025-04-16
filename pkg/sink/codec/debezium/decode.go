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

package debezium

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

// Decoder implement the RowEventDecoder interface
type Decoder struct {
	config *common.Config

	upstreamTiDB     *sql.DB
	tableIDAllocator *common.FakeTableIDAllocator

	keyPayload   map[string]interface{}
	keySchema    map[string]interface{}
	valuePayload map[string]interface{}
	valueSchema  map[string]interface{}
}

// NewDecoder return an debezium decoder
func NewDecoder(
	config *common.Config,
	db *sql.DB,
) common.RowEventDecoder {
	return &Decoder{
		config:           config,
		upstreamTiDB:     db,
		tableIDAllocator: common.NewFakeTableIDAllocator(),
	}
}

// AddKeyValue add the received key and values to the Decoder
func (d *Decoder) AddKeyValue(key, value []byte) error {
	if d.valuePayload != nil || d.valueSchema != nil {
		return errors.New("key or value is not nil")
	}
	keyPayload, keySchema, err := decodeRawBytes(key)
	if err != nil {
		return errors.ErrDebeziumEncodeFailed.FastGenByArgs(err)
	}
	valuePayload, valueSchema, err := decodeRawBytes(value)
	if err != nil {
		return errors.ErrDebeziumEncodeFailed.FastGenByArgs(err)
	}
	d.keyPayload = keyPayload
	d.keySchema = keySchema
	d.valuePayload = valuePayload
	d.valueSchema = valueSchema
	return nil
}

// HasNext returns whether there is any event need to be consumed
func (d *Decoder) HasNext() (common.MessageType, bool, error) {
	if d.valuePayload == nil && d.valueSchema == nil {
		return common.MessageTypeUnknown, false, nil
	}

	if len(d.valuePayload) < 1 {
		return common.MessageTypeUnknown, false, errors.ErrDebeziumInvalidMessage.FastGenByArgs(d.valuePayload)
	}
	op, ok := d.valuePayload["op"]
	if !ok {
		return common.MessageTypeDDL, true, nil
	}
	switch op {
	case "c", "u", "d":
		return common.MessageTypeRow, true, nil
	case "m":
		return common.MessageTypeResolved, true, nil
	}
	return common.MessageTypeUnknown, false, errors.ErrDebeziumInvalidMessage.FastGenByArgs(d.valuePayload)
}

// NextResolvedEvent returns the next resolved event if exists
func (d *Decoder) NextResolvedEvent() (uint64, error) {
	if len(d.valuePayload) == 0 {
		return 0, errors.ErrDebeziumEmptyValueMessage
	}
	commitTs := d.getCommitTs()
	d.clear()
	return commitTs, nil
}

// NextDDLEvent returns the next DDL event if exists
func (d *Decoder) NextDDLEvent() (*commonEvent.DDLEvent, error) {
	if len(d.valuePayload) == 0 {
		return nil, errors.ErrDebeziumEmptyValueMessage
	}
	defer d.clear()
	event := new(commonEvent.DDLEvent)
	event.TableInfo = new(commonType.TableInfo)
	tableName := d.getTableName()
	if tableName != "" {
		event.TableInfo.TableName = commonType.TableName{
			Schema: d.getSchemaName(),
			Table:  tableName,
		}
	}
	event.Query = d.valuePayload["ddl"].(string)
	event.FinishedTs = d.getCommitTs()
	return event, nil
}

// NextDMLEvent returns the next dml event if exists
func (d *Decoder) NextDMLEvent() (*commonEvent.DMLEvent, error) {
	if len(d.valuePayload) == 0 {
		return nil, errors.ErrDebeziumEmptyValueMessage
	}
	if d.config.DebeziumDisableSchema {
		return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("DebeziumDisableSchema is true")
	}
	if !d.config.EnableTiDBExtension {
		return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("EnableTiDBExtension is false")
	}
	defer d.clear()
	tableInfo := d.getTableInfo()
	commitTs := d.getCommitTs()
	event := &commonEvent.DMLEvent{
		CommitTs:  commitTs,
		TableInfo: tableInfo,
	}
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	columns := tableInfo.GetColumns()
	if before, ok := d.valuePayload["before"].(map[string]interface{}); ok {
		data := assembleColumnData(before, columns)
		common.AppendRow2Chunk(data, columns, chk)
	}
	if after, ok := d.valuePayload["after"].(map[string]interface{}); ok {
		data := assembleColumnData(after, columns)
		common.AppendRow2Chunk(data, columns, chk)
	}
	event.PhysicalTableID = d.tableIDAllocator.AllocateTableID(tableInfo.GetSchemaName(), tableInfo.GetTableName())
	return event, nil
}

func (d *Decoder) getCommitTs() uint64 {
	source := d.valuePayload["source"].(map[string]interface{})
	commitTs, err := source["commit_ts"].(json.Number).Int64()
	if err != nil {
		log.Error("decode value failed", zap.Error(err), zap.Any("value", source))
	}
	return uint64(commitTs)
}

func (d *Decoder) getSchemaName() string {
	source := d.valuePayload["source"].(map[string]interface{})
	schemaName := source["db"].(string)
	return schemaName
}

func (d *Decoder) getTableName() string {
	source := d.valuePayload["source"].(map[string]interface{})
	tableName := source["table"].(string)
	return tableName
}

func (d *Decoder) clear() {
	d.keyPayload = nil
	d.keySchema = nil
	d.valuePayload = nil
	d.valueSchema = nil
}

func (d *Decoder) getTableInfo() *commonType.TableInfo {
	tidbTableInfo := new(timodel.TableInfo)
	tidbTableInfo.Name = pmodel.NewCIStr(d.getTableName())
	fields := d.valueSchema["fields"].([]interface{})
	after := fields[1].(map[string]interface{})
	columnsField := after["fields"].([]interface{})
	indexColumns := make([]*timodel.IndexColumn, 0, len(d.keyPayload))
	for idx, column := range columnsField {
		col := column.(map[string]interface{})
		colName := col["field"].(string)
		tidbType := col["tidb_type"].(string)
		optional := col["optional"].(bool)
		fieldType := parseTiDBType(tidbType, optional)
		if fieldType.GetType() == mysql.TypeDatetime {
			name := col["name"].(string)
			if name == "io.debezium.time.MicroTimestamp" {
				fieldType.SetDecimal(6)
			}
		}
		if _, ok := d.keyPayload[colName]; ok {
			indexColumns = append(indexColumns, &timodel.IndexColumn{
				Name:   pmodel.NewCIStr(colName),
				Offset: idx,
			})
			fieldType.AddFlag(mysql.PriKeyFlag)
		}
		tidbTableInfo.Columns = append(tidbTableInfo.Columns, &timodel.ColumnInfo{
			ID:        int64(idx),
			State:     timodel.StatePublic,
			Name:      pmodel.NewCIStr(colName),
			FieldType: *fieldType,
		})
	}
	tidbTableInfo.Indices = append(tidbTableInfo.Indices, &timodel.IndexInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("primary"),
		Columns: indexColumns,
		Unique:  true,
		Primary: true,
	})
	return commonType.NewTableInfo4Decoder(d.getSchemaName(), tidbTableInfo)
}

func assembleColumnData(data map[string]interface{}, columns []*timodel.ColumnInfo) map[string]interface{} {
	result := make(map[string]interface{}, 0)
	for _, col := range columns {
		val, ok := data[col.Name.O]
		if !ok {
			continue
		}
		result[col.Name.O] = decodeColumn(val, col)
	}
	return result
}

func decodeColumn(value interface{}, colInfo *timodel.ColumnInfo) interface{} {
	if value == nil {
		return value
	}
	var err error
	// Notice: value may be the default value of the column
	switch colInfo.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if mysql.HasBinaryFlag(colInfo.GetFlag()) {
			s := value.(string)
			value, err = base64.StdEncoding.DecodeString(s)
			if err != nil {
				log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
				return nil
			}
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate:
		val := value.(json.Number).String()
		value, err = types.ParseTime(types.DefaultStmtNoWarningContext, val, colInfo.GetType(), colInfo.GetDecimal())
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
	case mysql.TypeDuration:
		val := value.(json.Number).String()
		value, _, err := types.ParseDuration(types.DefaultStmtNoWarningContext, val, colInfo.GetDecimal())
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		v, err := value.(json.Number).Int64()
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
		if mysql.HasUnsignedFlag(colInfo.GetFlag()) {
			value = uint64(v)
		} else {
			if v > math.MaxInt64 {
				value = math.MaxInt64
			} else if v < math.MinInt64 {
				value = math.MinInt64
			} else {
				value = v
			}
		}
	case mysql.TypeBit:
		switch val := value.(type) {
		case string:
			value, err = types.NewBitLiteral(val)
			if err != nil {
				log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
				return nil
			}
		case bool:
			if val {
				value = types.NewBinaryLiteralFromUint(uint64(1), -1)
			} else {
				value = types.NewBinaryLiteralFromUint(uint64(0), -1)
			}
		}
	case mysql.TypeNewDecimal:
		v, err := value.(json.Number).Float64()
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
		value = types.NewDecFromFloatForTest(v)
	case mysql.TypeDouble:
		value, err := value.(json.Number).Float64()
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
	case mysql.TypeFloat:
		v, err := value.(json.Number).Float64()
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
		value = float32(v)
	case mysql.TypeYear:
		value, err = value.(json.Number).Int64()
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil

		}
	case mysql.TypeEnum:
		value, err = types.ParseEnumName(colInfo.GetElems(), value.(string), colInfo.GetCollate())
	case mysql.TypeSet:
		value, err = types.ParseSetName(colInfo.GetElems(), value.(string), colInfo.GetCollate())
	case mysql.TypeJSON:
		value, err = types.ParseBinaryJSONFromString(value.(string))
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
	case mysql.TypeTiDBVectorFloat32:
		value, err = types.ParseVectorFloat32(value.(string))
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
	default:
	}
	return value
}

func parseTiDBType(tidbType string, optional bool) *ptypes.FieldType {
	ft := new(ptypes.FieldType)
	if optional {
		ft.AddFlag(mysql.NotNullFlag)
	}
	if strings.Contains(tidbType, " unsigned") {
		ft.AddFlag(mysql.UnsignedFlag)
		tidbType = strings.Replace(tidbType, " unsigned", "", 1)
	}
	if strings.Contains(tidbType, "blob") || strings.Contains(tidbType, "binary") {
		ft.AddFlag(mysql.BinaryFlag)
	}
	tp := ptypes.StrToType(tidbType)
	ft.SetType(tp)
	return ft
}

func decodeRawBytes(data []byte) (map[string]interface{}, map[string]interface{}, error) {
	var v map[string]interface{}
	d := json.NewDecoder(bytes.NewBuffer(data))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return nil, nil, errors.Trace(err)
	}
	payload, ok := v["payload"].(map[string]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("decode payload failed, data: %+v", v)
	}
	schema, ok := v["schema"].(map[string]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("decode payload failed, data: %+v", v)
	}
	return payload, schema, nil
}
