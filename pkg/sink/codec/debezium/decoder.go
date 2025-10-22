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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

var tableIDAllocator = common.NewTableIDAllocator()

// decoder implement the Decoder interface
type decoder struct {
	idx    int
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
	idx int,
	db *sql.DB,
) common.Decoder {
	tableIDAllocator.Clean()
	return &decoder{
		idx:          idx,
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
		return common.MessageTypeUnknown, false
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
	event.TableID = tableIDAllocator.Allocate(event.SchemaName, event.TableName)

	if d.idx == 0 {
		tableIDAllocator.AddBlockTableID(event.SchemaName, event.TableName, event.TableID)
		event.BlockedTables = common.GetBlockedTables(tableIDAllocator, event)
		if event.Type == byte(timodel.ActionRenameTable) {
			schemaName = event.ExtraSchemaName
			tableName = event.ExtraTableName
		}
	}
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
		Rows:            chunk.NewChunkFromPoolWithCapacity(tableInfo.GetFieldSlice(), chunk.InitialCapacity),
		StartTs:         commitTs,
		CommitTs:        commitTs,
		TableInfo:       tableInfo,
		PhysicalTableID: tableInfo.TableName.TableID,
		Length:          1,
	}
	event.AddPostFlushFunc(func() {
		event.Rows.Destroy(chunk.InitialCapacity, tableInfo.GetFieldSlice())
	})
	columns := tableInfo.GetColumns()
	before, ok1 := d.valuePayload["before"].(map[string]interface{})
	if ok1 {
		data := assembleColumnData(before, columns, d.config.TimeZone)
		common.AppendRow2Chunk(data, columns, event.Rows)
	}
	after, ok2 := d.valuePayload["after"].(map[string]interface{})
	if ok2 {
		data := assembleColumnData(after, columns, d.config.TimeZone)
		common.AppendRow2Chunk(data, columns, event.Rows)
	}
	if ok1 && ok2 {
		event.RowTypes = append(event.RowTypes, commonType.RowTypeUpdate)
		event.RowTypes = append(event.RowTypes, commonType.RowTypeUpdate)
	} else if ok1 {
		event.RowTypes = append(event.RowTypes, commonType.RowTypeDelete)
	} else if ok2 {
		event.RowTypes = append(event.RowTypes, commonType.RowTypeInsert)
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

	tidbTableInfo := new(timodel.TableInfo)
	tidbTableInfo.ID = tableIDAllocator.Allocate(schemaName, tableName)
	tableIDAllocator.AddBlockTableID(schemaName, tableName, tidbTableInfo.ID)
	tidbTableInfo.Name = ast.NewCIStr(tableName)

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
		switch fieldType.GetType() {
		case mysql.TypeEnum, mysql.TypeSet:
			parameters := col["parameters"].(map[string]interface{})
			allowed := parameters["allowed"].(string)
			fieldType.SetElems(strings.Split(allowed, ","))
		case mysql.TypeDatetime:
			name := col["name"].(string)
			if name == "io.debezium.time.MicroTimestamp" {
				fieldType.SetDecimal(6)
			}
		}
		if _, ok := d.keyPayload[colName]; ok {
			indexColumns = append(indexColumns, &timodel.IndexColumn{
				Name:   ast.NewCIStr(colName),
				Offset: idx,
			})
			fieldType.AddFlag(mysql.PriKeyFlag)
		}
		tidbTableInfo.Columns = append(tidbTableInfo.Columns, &timodel.ColumnInfo{
			ID:        int64(idx),
			State:     timodel.StatePublic,
			Name:      ast.NewCIStr(colName),
			FieldType: *fieldType,
		})
	}
	tidbTableInfo.Indices = append(tidbTableInfo.Indices, &timodel.IndexInfo{
		ID:      1,
		Name:    ast.NewCIStr("primary"),
		Columns: indexColumns,
		Unique:  true,
		Primary: true,
	})
	result := commonType.NewTableInfo4Decoder(schemaName, tidbTableInfo)
	return result
}

func assembleColumnData(data map[string]interface{}, columns []*timodel.ColumnInfo, timeZone *time.Location) map[string]interface{} {
	result := make(map[string]interface{}, 0)
	for _, col := range columns {
		val, ok := data[col.Name.O]
		if !ok {
			continue
		}
		result[col.Name.O] = decodeColumn(val, col, timeZone)
	}
	return result
}

func decodeColumn(value interface{}, colInfo *timodel.ColumnInfo, timeZone *time.Location) interface{} {
	if value == nil {
		return value
	}
	var err error
	// Notice: value may be the default value of the column
	switch colInfo.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if mysql.HasBinaryFlag(colInfo.GetFlag()) {
			value, err = base64.StdEncoding.DecodeString(value.(string))
			if err != nil {
				log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
			}
			return value
		}
		return common.UnsafeStringToBytes(value.(string))
	case mysql.TypeDate, mysql.TypeNewDate:
		val, err := value.(json.Number).Int64()
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
		t := time.Unix(val*60*60*24, 0)
		value = types.NewTime(types.FromGoTime(t.UTC()), colInfo.GetType(), colInfo.GetDecimal())
	case mysql.TypeTimestamp:
		t, err := types.ParseTimestamp(types.DefaultStmtNoWarningContext, value.(string))
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
		err = t.ConvertTimeZone(time.UTC, timeZone)
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
		value = t
	case mysql.TypeDatetime:
		val, err := value.(json.Number).Int64()
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
		var t time.Time
		if colInfo.GetDecimal() <= 3 {
			t = time.UnixMilli(val)
		} else {
			t = time.UnixMicro(val)
		}
		value = types.NewTime(types.FromGoTime(t.UTC()), colInfo.GetType(), colInfo.GetDecimal())
	case mysql.TypeDuration:
		val, err := value.(json.Number).Int64()
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
		value = types.NewDuration(0, 0, 0, int(val), types.MaxFsp)
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		var intVal int64
		intVal, err = value.(json.Number).Int64()
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
		if mysql.HasUnsignedFlag(colInfo.GetFlag()) {
			return uint64(intVal)
		}
		return intVal
	case mysql.TypeBit:
		switch val := value.(type) {
		case string:
			b, err := base64.StdEncoding.DecodeString(val)
			if err != nil {
				log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
			}
			v := binary.LittleEndian.Uint64(b)
			value = types.NewBinaryLiteralFromUint(v, len(b))
		case bool:
			if val {
				return types.NewBinaryLiteralFromUint(uint64(1), -1)
			}
			return types.NewBinaryLiteralFromUint(uint64(0), -1)
		}
	case mysql.TypeNewDecimal:
		var f64 float64
		f64, err = value.(json.Number).Float64()
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
		value = types.NewDecFromFloatForTest(f64)
	case mysql.TypeDouble:
		value, err = value.(json.Number).Float64()
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
	case mysql.TypeFloat:
		var f64 float64
		f64, err = value.(json.Number).Float64()
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
		value = float32(f64)
	case mysql.TypeYear:
		value, err = value.(json.Number).Int64()
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
	case mysql.TypeEnum:
		value, err = types.ParseEnumName(colInfo.GetElems(), value.(string), colInfo.GetCollate())
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
	case mysql.TypeSet:
		value, err = types.ParseSetName(colInfo.GetElems(), value.(string), colInfo.GetCollate())
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
	case mysql.TypeJSON:
		value, err = types.ParseBinaryJSONFromString(value.(string))
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
		}
	case mysql.TypeTiDBVectorFloat32:
		value, err = types.ParseVectorFloat32(value.(string))
		if err != nil {
			log.Panic("decode value failed", zap.Error(err), zap.Any("value", value))
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
		ft.SetCharset("binary")
		ft.SetCollate("binary")
	}
	if strings.HasPrefix(tidbType, "char") ||
		strings.HasPrefix(tidbType, "varchar") ||
		strings.Contains(tidbType, "text") ||
		strings.Contains(tidbType, "enum") ||
		strings.Contains(tidbType, "set") {
		ft.SetCharset("utf8mb4")
		ft.SetCollate("utf8mb4_bin")
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
