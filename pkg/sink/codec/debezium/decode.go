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

var (
	tableIDAllocator  = common.NewFakeTableIDAllocator()
	tableInfoAccessor = common.NewTableInfoAccessor()
)

// decoder implement the Decoder interface
type decoder struct {
	config *common.Config

	upstreamTiDB *sql.DB

	keyPayload   map[string]interface{}
	keySchema    map[string]interface{}
	valuePayload map[string]interface{}
	valueSchema  map[string]interface{}
}

// NewDecoder return an debezium decoder
func NewDecoder(
	config *common.Config,
	db *sql.DB,
) common.Decoder {
	tableIDAllocator.Clean()
	tableInfoAccessor.Clean()
	return &decoder{
		config:       config,
		upstreamTiDB: db,
	}
}

// AddKeyValue add the received key and values to the decoder
func (d *decoder) AddKeyValue(key, value []byte) {
	if d.valuePayload != nil || d.valueSchema != nil {
		log.Panic("add key / value to the decoder failed, since it's already set")
	}
	keyPayload, keySchema, err := decodeRawBytes(key)
	if err != nil {
		log.Panic("decode key failed", zap.Error(err), zap.ByteString("key", key))
	}
	valuePayload, valueSchema, err := decodeRawBytes(value)
	if err != nil {
		log.Panic("decode value failed", zap.Error(err), zap.ByteString("value", value))
	}
	d.keyPayload = keyPayload
	d.keySchema = keySchema
	d.valuePayload = valuePayload
	d.valueSchema = valueSchema
	return
}

// HasNext returns whether there is any event need to be consumed
func (d *decoder) HasNext() (common.MessageType, bool) {
	if d.valuePayload == nil && d.valueSchema == nil {
		log.Panic("has next failed, since key / value is not set")
	}

	if len(d.valuePayload) < 1 {
		log.Panic("has next failed, since value payload is empty")
	}
	op, ok := d.valuePayload["op"]
	if !ok {
		return common.MessageTypeDDL, true
	}
	switch op {
	case "c", "u", "d":
		return common.MessageTypeRow, true
	case "m":
		return common.MessageTypeResolved, true
	}
	log.Panic("has next failed, since op is not set", zap.Any("op", op))
	return common.MessageTypeUnknown, false
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() uint64 {
	if len(d.valuePayload) == 0 {
		log.Panic("next resolved event failed, since value payload is empty")
	}
	commitTs := d.getCommitTs()
	d.clear()
	return commitTs
}

// NextDDLEvent returns the next DDL event if exists
func (d *decoder) NextDDLEvent() *commonEvent.DDLEvent {
	if len(d.valuePayload) == 0 {
		log.Panic("next DDL event failed, since value payload is empty")
	}
	defer d.clear()

	schemaName := d.getSchemaName()
	tableName := d.getTableName()

	event := new(commonEvent.DDLEvent)
	event.FinishedTs = d.getCommitTs()
	event.SchemaName = schemaName
	event.TableName = tableName
	event.Query = d.valuePayload["ddl"].(string)
	actionType := common.GetDDLActionType(event.Query)
	event.Type = byte(actionType)

	var tableID int64
	tableInfo, ok := tableInfoAccessor.Get(schemaName, tableName)
	if ok {
		tableID = tableInfo.TableName.TableID
	}
	event.BlockedTables = common.GetInfluenceTables(actionType, tableID)
	log.Debug("set blocked tables for the DDL event",
		zap.String("schema", schemaName), zap.String("table", tableName),
		zap.String("query", event.Query), zap.Any("blocked", event.BlockedTables))

	tableInfoAccessor.Remove(schemaName, tableName)
	return event
}

// NextDMLEvent returns the next dml event if exists
func (d *decoder) NextDMLEvent() *commonEvent.DMLEvent {
	if len(d.valuePayload) == 0 {
		log.Panic("next DML event failed, since value payload is empty")
	}
	if d.config.DebeziumDisableSchema {
		log.Panic("next DML event failed, since DebeziumDisableSchema is true")
	}
	if !d.config.EnableTiDBExtension {
		log.Panic("next DML event failed, since EnableTiDBExtension is false")
	}
	defer d.clear()
	tableInfo := d.queryTableInfo()
	commitTs := d.getCommitTs()
	event := &commonEvent.DMLEvent{
		Rows:            chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1),
		StartTs:         commitTs,
		CommitTs:        commitTs,
		TableInfo:       tableInfo,
		PhysicalTableID: tableInfo.TableName.TableID,
		Length:          1,
	}
	columns := tableInfo.GetColumns()
	before, ok1 := d.valuePayload["before"].(map[string]interface{})
	if ok1 {
		data := assembleColumnData(before, columns)
		common.AppendRow2Chunk(data, columns, event.Rows)
	}
	after, ok2 := d.valuePayload["after"].(map[string]interface{})
	if ok2 {
		data := assembleColumnData(after, columns)
		common.AppendRow2Chunk(data, columns, event.Rows)
	}
	if ok1 && ok2 {
		event.RowTypes = append(event.RowTypes, commonEvent.RowTypeUpdate)
		event.RowTypes = append(event.RowTypes, commonEvent.RowTypeUpdate)
	} else if ok1 {
		event.RowTypes = append(event.RowTypes, commonEvent.RowTypeDelete)
	} else if ok2 {
		event.RowTypes = append(event.RowTypes, commonEvent.RowTypeInsert)
	} else {
		log.Panic("unknown event type for the DML event")
	}
	return event
}

func (d *decoder) getCommitTs() uint64 {
	source := d.valuePayload["source"].(map[string]interface{})
	commitTs, err := source["commit_ts"].(json.Number).Int64()
	if err != nil {
		log.Error("decode value failed", zap.Error(err), zap.Any("value", source))
	}
	return uint64(commitTs)
}

func (d *decoder) getSchemaName() string {
	source := d.valuePayload["source"].(map[string]interface{})
	schemaName := source["db"].(string)
	return schemaName
}

func (d *decoder) getTableName() string {
	source := d.valuePayload["source"].(map[string]interface{})
	tableName := source["table"].(string)
	return tableName
}

func (d *decoder) clear() {
	d.keyPayload = nil
	d.keySchema = nil
	d.valuePayload = nil
	d.valueSchema = nil
}

func (d *decoder) queryTableInfo() *commonType.TableInfo {
	schemaName := d.getSchemaName()
	tableName := d.getTableName()

	tableInfo, ok := tableInfoAccessor.Get(schemaName, tableName)
	if ok {
		return tableInfo
	}

	tidbTableInfo := new(timodel.TableInfo)
	tidbTableInfo.ID = tableIDAllocator.AllocateTableID(schemaName, tableName)
	tidbTableInfo.Name = pmodel.NewCIStr(tableName)

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
		if _, ok = d.keyPayload[colName]; ok {
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
	result := commonType.NewTableInfo4Decoder(schemaName, tidbTableInfo)
	tableInfoAccessor.Add(schemaName, tableName, result)
	return result
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
