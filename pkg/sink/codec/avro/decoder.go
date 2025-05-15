// Copyright 2020 PingCAP, Inc.
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

package avro

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

var (
	tableIDAllocator  = common.NewFakeTableIDAllocator()
	tableInfoAccessor = common.NewTableInfoAccessor()
)

type decoder struct {
	idx    int
	config *common.Config
	topic  string

	upstreamTiDB *sql.DB

	schemaM SchemaManager

	key   []byte
	value []byte
}

// NewDecoder return an avro decoder
func NewDecoder(
	config *common.Config,
	idx int,
	schemaM SchemaManager,
	topic string,
	db *sql.DB,
) common.Decoder {
	tableIDAllocator.Clean()
	tableInfoAccessor.Clean()
	return &decoder{
		idx:          idx,
		config:       config,
		topic:        topic,
		schemaM:      schemaM,
		upstreamTiDB: db,
	}
}

func (d *decoder) AddKeyValue(key, value []byte) {
	if d.key != nil || d.value != nil {
		log.Panic("add key/value to the decoder failed, since it's already set")
	}
	d.key = key
	d.value = value
}

func (d *decoder) HasNext() (common.MessageType, bool) {
	if d.key == nil && d.value == nil {
		return common.MessageTypeUnknown, false
	}

	// it must a row event.
	if d.key != nil {
		return common.MessageTypeRow, true
	}
	if len(d.value) < 1 {
		log.Panic("avro invalid data, the length of value is less than 1", zap.Any("data", d.value))
	}
	switch d.value[0] {
	case magicByte:
		return common.MessageTypeRow, true
	case ddlByte:
		return common.MessageTypeDDL, true
	case checkpointByte:
		return common.MessageTypeResolved, true
	default:
	}
	log.Panic("avro invalid data, the first byte is not magic byte or ddl byte")
	return common.MessageTypeUnknown, false
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() uint64 {
	if len(d.value) == 0 {
		log.Panic("value is empty, cannot found the resolved-ts")
	}
	ts := binary.BigEndian.Uint64(d.value[1:])
	d.value = nil
	return ts
}

// NextDMLEvent returns the next row changed event if exists
func (d *decoder) NextDMLEvent() *commonEvent.DMLEvent {
	var (
		valueMap    map[string]interface{}
		valueSchema map[string]interface{}
		err         error
	)

	ctx := context.Background()
	keyMap, keySchema, err := d.decodeKey(ctx)
	if err != nil {
		log.Panic("decode key failed", zap.Error(err))
	}

	// for the delete event, only have key part, it holds primary key or the unique key columns.
	// for the insert / update, extract the value part, it holds all columns.
	isDelete := len(d.value) == 0
	if isDelete {
		// delete event only have key part, treat it as the value part also.
		valueMap = keyMap
		valueSchema = keySchema
	} else {
		valueMap, valueSchema, err = d.decodeValue(ctx)
		if err != nil {
			log.Panic("decode value failed", zap.Error(err))
		}
	}

	event, err := assembleEvent(keyMap, valueMap, valueSchema, isDelete)
	if err != nil {
		log.Panic("assemble event failed", zap.Error(err))
	}

	// Delete event only has Primary Key Columns, but the checksum is calculated based on the whole row columns,
	// checksum verification cannot be done here, so skip it.
	if isDelete {
		return event
	}

	expectedChecksum, found := extractExpectedChecksum(valueMap)
	corrupted := isCorrupted(valueMap)
	if found {
		event.Checksum = []*integrity.Checksum{{
			Current:   uint32(expectedChecksum),
			Corrupted: corrupted,
		}}
	}

	if isCorrupted(valueMap) {
		log.Warn("row data is corrupted",
			zap.String("topic", d.topic), zap.Uint64("checksum", expectedChecksum))
		for _, col := range event.TableInfo.GetColumns() {
			log.Info("data corrupted, print each column for debugging",
				zap.String("name", col.Name.O),
				zap.Any("type", col.GetType()),
				zap.Any("charset", col.GetCharset()),
				zap.Any("flag", col.GetFlag()),
				zap.Any("value", valueMap[col.Name.O]),
				zap.Any("default", col.GetDefaultValue()))
		}
	}
	if found {
		if err = common.VerifyChecksum(event, d.upstreamTiDB); err != nil {
			return nil
		}
	}

	return event
}

// assembleEvent return a row changed event
// keyMap hold primary key or unique key columns
// valueMap hold all columns information
// schema is corresponding to the valueMap, it can be used to decode the valueMap to construct columns.
func assembleEvent(
	keyMap, valueMap, schema map[string]interface{}, isDelete bool,
) (*commonEvent.DMLEvent, error) {
	fields, ok := schema["fields"].([]interface{})
	if !ok {
		return nil, errors.New("schema fields should be a map")
	}
	columns := make([]*timodel.ColumnInfo, 0, len(valueMap))
	data := make(map[string]interface{}, 0)
	// fields is ordered by the column id, so iterate over it to build columns
	// it's also the order to calculate the checksum.
	for idx, item := range fields {
		field, ok := item.(map[string]interface{})
		if !ok {
			return nil, errors.New("schema field should be a map")
		}
		// `tidbOp` is the first extension field in the schema,
		// it's not real columns, so break here.
		colName := field["name"].(string)
		if colName == tidbOp {
			break
		}
		// query the field to get `tidbType`, and get the mysql type from it.
		var holder map[string]interface{}
		switch ty := field["type"].(type) {
		case []interface{}:
			if m, ok := ty[0].(map[string]interface{}); ok {
				holder = m["connect.parameters"].(map[string]interface{})
			} else if m, ok := ty[1].(map[string]interface{}); ok {
				holder = m["connect.parameters"].(map[string]interface{})
			} else {
				log.Panic("type info is anything else", zap.Any("typeInfo", field["type"]))
			}
		case map[string]interface{}:
			holder = ty["connect.parameters"].(map[string]interface{})
		default:
			log.Panic("type info is anything else", zap.Any("typeInfo", field["type"]))
		}
		tidbType := holder["tidb_type"].(string)
		mysqlType := mysqlTypeFromTiDBType(tidbType)
		flag := flagFromTiDBType(tidbType)
		value, ok := valueMap[colName]
		if !ok {
			return nil, errors.New("value not found")
		}
		value, err := getColumnValue(value, holder, mysqlType, flag)
		if err != nil {
			return nil, errors.Trace(err)
		}
		data[colName] = value

		tiCol := &timodel.ColumnInfo{
			ID:    int64(idx),
			Name:  pmodel.NewCIStr(colName),
			State: timodel.StatePublic,
		}
		tiCol.SetType(mysqlType)
		tiCol.SetFlag(flag)
		columns = append(columns, tiCol)
	}

	// "namespace.schema"
	namespace := schema["namespace"].(string)
	schemaName := strings.Split(namespace, ".")[1]
	tableName := schema["name"].(string)

	var commitTs int64
	if !isDelete {
		o, ok := valueMap[tidbCommitTs]
		if !ok {
			return nil, errors.New("commit ts not found")
		}
		commitTs = o.(int64)
	}

	event := new(commonEvent.DMLEvent)
	event.TableInfo = queryTableInfo(schemaName, tableName, columns, keyMap)
	event.StartTs = uint64(commitTs)
	event.CommitTs = uint64(commitTs)
	event.PhysicalTableID = event.TableInfo.TableName.TableID
	event.Rows = chunk.NewChunkWithCapacity(event.TableInfo.GetFieldSlice(), 1)
	event.Length++
	common.AppendRow2Chunk(data, event.TableInfo.GetColumns(), event.Rows)

	rowType := commonEvent.RowTypeInsert
	if isDelete {
		rowType = commonEvent.RowTypeDelete
	}
	event.RowTypes = append(event.RowTypes, rowType)
	return event, nil
}

func queryTableInfo(schemaName, tableName string, columns []*timodel.ColumnInfo, keyMap map[string]interface{}) *commonType.TableInfo {
	tableInfo, ok := tableInfoAccessor.Get(schemaName, tableName)
	if ok {
		return tableInfo
	}
	tableInfo = newTableInfo(schemaName, tableName, columns, keyMap)
	tableInfoAccessor.Add(schemaName, tableName, tableInfo)
	return tableInfo
}

func newTableInfo(schemaName, tableName string, columns []*timodel.ColumnInfo, keyMap map[string]interface{}) *commonType.TableInfo {
	tidbTableInfo := new(timodel.TableInfo)
	tidbTableInfo.ID = tableIDAllocator.AllocateTableID(schemaName, tableName)
	tidbTableInfo.Name = pmodel.NewCIStr(tableName)
	tidbTableInfo.Columns = columns
	indexColumns := make([]*timodel.IndexColumn, 0)
	for _, col := range columns {
		if _, ok := keyMap[col.Name.O]; ok {
			indexColumns = append(indexColumns, &timodel.IndexColumn{
				Name: col.Name,
			})
		}
	}
	tidbTableInfo.Indices = []*timodel.IndexInfo{{
		Primary: true,
		Name:    pmodel.NewCIStr("primary"),
		Columns: indexColumns,
		State:   timodel.StatePublic,
	}}
	return commonType.NewTableInfo4Decoder(schemaName, tidbTableInfo)
}

func isCorrupted(valueMap map[string]interface{}) bool {
	o, ok := valueMap[tidbCorrupted]
	if !ok {
		return false
	}

	corrupted := o.(bool)
	return corrupted
}

// extract the checksum from the received value map
// return true if the checksum found, and return error if the checksum is not valid
func extractExpectedChecksum(valueMap map[string]interface{}) (uint64, bool) {
	o, ok := valueMap[tidbRowLevelChecksum]
	if !ok {
		return 0, false
	}
	checksum := o.(string)
	if checksum == "" {
		return 0, false
	}
	result, err := strconv.ParseUint(checksum, 10, 64)
	if err != nil {
		log.Panic("parse checksum into uint64 failed", zap.String("checksum", checksum), zap.Error(err))
	}
	return result, true
}

// value is an interface, need to convert it to the real value with the help of type info.
// holder has the value's column info.
func getColumnValue(
	value interface{}, holder map[string]interface{}, mysqlType byte, flag uint,
) (interface{}, error) {
	switch t := value.(type) {
	// for nullable columns, the value is encoded as a map with one pair.
	// key is the encoded type, value is the encoded value, only care about the value here.
	case map[string]interface{}:
		for _, v := range t {
			value = v
		}
	}
	if value == nil {
		return nil, nil
	}

	switch mysqlType {
	case mysql.TypeBit:
		value = types.BinaryLiteral(value.([]byte))
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		if val, ok := value.(string); ok {
			value = []byte(val)
		}
	case mysql.TypeEnum:
		// enum type is encoded as string,
		// we need to convert it to int by the order of the enum values definition.
		allowed := strings.Split(holder["allowed"].(string), ",")
		enum, err := types.ParseEnum(allowed, value.(string), "")
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = enum
	case mysql.TypeSet:
		// set type is encoded as string,
		// we need to convert it to int by the order of the set values definition.
		elems := strings.Split(holder["allowed"].(string), ",")
		set, err := types.ParseSet(elems, value.(string), "")
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = set
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		var v int64
		switch val := value.(type) {
		case int32:
			v = int64(val)
		case int64:
			v = val
		case string:
			tmp, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return nil, errors.Trace(err)
			}
			v = int64(tmp)
		}
		if mysql.HasUnsignedFlag(flag) {
			value = uint64(v)
		} else {
			value = v
		}
	case mysql.TypeNewDecimal:
		switch val := value.(type) {
		case string:
			value = types.NewDecFromStringForTest(val)
		case *big.Rat:
			v, _ := val.Float64()
			value = types.NewDecFromFloatForTest(v)
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		val, err := types.ParseTime(types.DefaultStmtNoWarningContext, value.(string), mysqlType, types.MaxFsp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = val
	case mysql.TypeDuration:
		val, _, err := types.ParseDuration(types.DefaultStmtNoWarningContext, value.(string), types.MaxFsp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = val
	case mysql.TypeYear:
		value = int64(value.(int32))
	case mysql.TypeJSON:
		val, err := types.ParseBinaryJSONFromString(value.(string))
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = val
	case mysql.TypeTiDBVectorFloat32:
		val, err := types.ParseVectorFloat32(value.(string))
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = val
	}
	return value, nil
}

// NextDDLEvent returns the next DDL event if exists
func (d *decoder) NextDDLEvent() *commonEvent.DDLEvent {
	if len(d.value) == 0 {
		log.Panic("value is empty, cannot found the ddl event")
	}
	if d.value[0] != ddlByte {
		log.Panic("avro invalid data, the first byte is not ddl byte", zap.Any("value", d.value))
	}

	data := d.value[1:]
	var baseDDLEvent ddlEvent
	err := json.Unmarshal(data, &baseDDLEvent)
	if err != nil {
		log.Panic("unmarshal ddl event failed", zap.Any("value", d.value), zap.Error(err))
	}
	d.value = nil

	result := new(commonEvent.DDLEvent)
	result.FinishedTs = baseDDLEvent.CommitTs
	result.SchemaName = baseDDLEvent.Schema
	result.TableName = baseDDLEvent.Table
	result.Query = baseDDLEvent.Query
	actionType := common.GetDDLActionType(result.Query)
	result.Type = byte(actionType)

	if d.idx == 0 {
		var tableID int64
		tableInfo, ok := tableInfoAccessor.Get(result.SchemaName, result.TableName)
		if ok {
			tableID = tableInfo.TableName.TableID
			log.Info("found tableID for the blocked table in the table accessor",
				zap.String("schema", result.SchemaName), zap.String("table", result.TableName),
				zap.Any("actionType", actionType.String()), zap.Int64("tableID", tableID))
		}
		result.BlockedTables = common.GetInfluenceTables(actionType, tableID)
		log.Info("set blocked table", zap.String("schema", result.SchemaName), zap.String("table", result.TableName),
			zap.Any("actionType", actionType.String()), zap.Any("tableID", tableID))
		tableInfoAccessor.Remove(result.SchemaName, result.TableName)
	}
	return result
}

// return the schema ID and the encoded binary data
// schemaID can be used to fetch the corresponding schema from schema registry,
// which should be used to decode the binary data.
func extractConfluentSchemaIDAndBinaryData(data []byte) (int, []byte, error) {
	if len(data) < 5 {
		return 0, nil, errors.ErrAvroInvalidMessage.
			FastGenByArgs("an avro message using confluent schema registry should have at least 5 bytes")
	}
	if data[0] != magicByte {
		return 0, nil, errors.ErrAvroInvalidMessage.
			FastGenByArgs("magic byte is not match, it should be 0")
	}
	id, err := getConfluentSchemaIDFromHeader(data[0:5])
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	return int(id), data[5:], nil
}

func extractGlueSchemaIDAndBinaryData(data []byte) (string, []byte, error) {
	if len(data) < 18 {
		return "", nil, errors.ErrAvroInvalidMessage.
			FastGenByArgs("an avro message using glue schema registry should have at least 18 bytes")
	}
	if data[0] != headerVersionByte {
		return "", nil, errors.ErrAvroInvalidMessage.
			FastGenByArgs("header version byte is not match, it should be %d", headerVersionByte)
	}
	if data[1] != compressionDefaultByte {
		return "", nil, errors.ErrAvroInvalidMessage.
			FastGenByArgs("compression byte is not match, it should be %d", compressionDefaultByte)
	}
	id, err := getGlueSchemaIDFromHeader(data[0:18])
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	return id, data[18:], nil
}

func decodeRawBytes(
	ctx context.Context, schemaM SchemaManager, data []byte, topic string,
) (map[string]interface{}, map[string]interface{}, error) {
	var schemaID schemaID
	var binary []byte
	var err error
	var cid int
	var gid string

	switch schemaM.RegistryType() {
	case common.SchemaRegistryTypeConfluent:
		cid, binary, err = extractConfluentSchemaIDAndBinaryData(data)
		if err != nil {
			return nil, nil, err
		}
		schemaID.confluentSchemaID = cid
	case common.SchemaRegistryTypeGlue:
		gid, binary, err = extractGlueSchemaIDAndBinaryData(data)
		if err != nil {
			return nil, nil, err
		}
		schemaID.glueSchemaID = gid
	default:
		return nil, nil, errors.ErrCodecDecode.GenWithStack("unknown schema registry type")
	}

	codec, err := schemaM.Lookup(ctx, topic, schemaID)
	if err != nil {
		return nil, nil, err
	}

	native, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		return nil, nil, err
	}

	result, ok := native.(map[string]interface{})
	if !ok {
		return nil, nil, errors.ErrCodecDecode.GenWithStack("raw avro message is not a map")
	}

	schema := make(map[string]interface{})
	if err := json.Unmarshal([]byte(codec.Schema()), &schema); err != nil {
		return nil, nil, errors.Trace(err)
	}

	return result, schema, nil
}

func (d *decoder) decodeKey(ctx context.Context) (map[string]interface{}, map[string]interface{}, error) {
	data := d.key
	d.key = nil
	return decodeRawBytes(ctx, d.schemaM, data, d.topic)
}

func (d *decoder) decodeValue(ctx context.Context) (map[string]interface{}, map[string]interface{}, error) {
	data := d.value
	d.value = nil
	return decodeRawBytes(ctx, d.schemaM, data, d.topic)
}

func mysqlTypeFromTiDBType(tidbType string) byte {
	var result byte
	switch tidbType {
	case "INT", "INT UNSIGNED":
		result = mysql.TypeLong
	case "BIGINT", "BIGINT UNSIGNED":
		result = mysql.TypeLonglong
	case "FLOAT":
		result = mysql.TypeFloat
	case "DOUBLE":
		result = mysql.TypeDouble
	case "BIT":
		result = mysql.TypeBit
	case "DECIMAL":
		result = mysql.TypeNewDecimal
	case "TEXT":
		result = mysql.TypeVarchar
	case "BLOB":
		result = mysql.TypeLongBlob
	case "ENUM":
		result = mysql.TypeEnum
	case "SET":
		result = mysql.TypeSet
	case "JSON":
		result = mysql.TypeJSON
	case "DATE":
		result = mysql.TypeDate
	case "DATETIME":
		result = mysql.TypeDatetime
	case "TIMESTAMP":
		result = mysql.TypeTimestamp
	case "TIME":
		result = mysql.TypeDuration
	case "YEAR":
		result = mysql.TypeYear
	case "TiDBVECTORFloat32":
		result = mysql.TypeTiDBVectorFloat32
	default:
		log.Panic("this should not happen, unknown TiDB type", zap.String("type", tidbType))
	}
	return result
}

func flagFromTiDBType(tp string) uint {
	var flag uint
	if strings.Contains(tp, "UNSIGNED") {
		return mysql.UnsignedFlag
	}
	return flag
}
