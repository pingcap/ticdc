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
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

type decoder struct {
	config *common.Config
	topic  string

	upstreamTiDB     *sql.DB
	tableIDAllocator *common.FakeTableIDAllocator

	schemaM SchemaManager

	key   []byte
	value []byte
}

// NewDecoder return an avro decoder
func NewDecoder(
	config *common.Config,
	schemaM SchemaManager,
	topic string,
	db *sql.DB,
) *decoder {
	return &decoder{
		config:           config,
		topic:            topic,
		schemaM:          schemaM,
		upstreamTiDB:     db,
		tableIDAllocator: common.NewFakeTableIDAllocator(),
	}
}

func (d *decoder) AddKeyValue(key, value []byte) error {
	if d.key != nil || d.value != nil {
		return errors.New("key or value is not nil")
	}
	d.key = key
	d.value = value
	return nil
}

func (d *decoder) HasNext() (common.MessageType, bool, error) {
	if d.key == nil && d.value == nil {
		return common.MessageTypeUnknown, false, nil
	}

	// it must a row event.
	if d.key != nil {
		return common.MessageTypeRow, true, nil
	}
	if len(d.value) < 1 {
		return common.MessageTypeUnknown, false, errors.ErrAvroInvalidMessage.FastGenByArgs(d.value)
	}
	switch d.value[0] {
	case magicByte:
		return common.MessageTypeRow, true, nil
	case ddlByte:
		return common.MessageTypeDDL, true, nil
	case checkpointByte:
		return common.MessageTypeResolved, true, nil
	}
	return common.MessageTypeUnknown, false, errors.ErrAvroInvalidMessage.FastGenByArgs(d.value)
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *decoder) NextRowChangedEvent() (*commonEvent.DMLEvent, error) {
	event := &commonEvent.DMLEvent{}
	event.PhysicalTableID = d.tableIDAllocator.AllocateTableID(event.TableInfo.GetSchemaName(), event.TableInfo.GetTableName())

	return event, nil
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
func extractExpectedChecksum(valueMap map[string]interface{}) (uint64, bool, error) {
	o, ok := valueMap[tidbRowLevelChecksum]
	if !ok {
		return 0, false, nil
	}
	checksum := o.(string)
	if checksum == "" {
		return 0, false, nil
	}
	result, err := strconv.ParseUint(checksum, 10, 64)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	return result, true, nil
}

// value is an interface, need to convert it to the real value with the help of type info.
// holder has the value's column info.
func getColumnValue(
	value interface{}, holder map[string]interface{}, mysqlType byte,
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
	case mysql.TypeEnum:
		// enum type is encoded as string,
		// we need to convert it to int by the order of the enum values definition.
		allowed := strings.Split(holder["allowed"].(string), ",")
		enum, err := types.ParseEnum(allowed, value.(string), "")
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = enum.Value
	case mysql.TypeSet:
		// set type is encoded as string,
		// we need to convert it to int by the order of the set values definition.
		elems := strings.Split(holder["allowed"].(string), ",")
		s, err := types.ParseSet(elems, value.(string), "")
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = s.Value
	}
	return value, nil
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() (uint64, error) {
	if len(d.value) == 0 {
		return 0, errors.New("value should not be empty")
	}
	ts := binary.BigEndian.Uint64(d.value[1:])
	d.value = nil
	return ts, nil
}

// NextDDLEvent returns the next DDL event if exists
func (d *decoder) NextDDLEvent() (*commonEvent.DDLEvent, error) {
	if len(d.value) == 0 {
		return nil, errors.New("value should not be empty")
	}
	if d.value[0] != ddlByte {
		return nil, fmt.Errorf("first byte is not the ddl byte, but got: %+v", d.value[0])
	}

	data := d.value[1:]
	var baseDDLEvent ddlEvent
	err := json.Unmarshal(data, &baseDDLEvent)
	if err != nil {
		return nil, errors.WrapError(errors.ErrDecodeFailed, err)
	}
	d.value = nil

	result := new(commonEvent.DDLEvent)
	result.TableInfo = new(commonType.TableInfo)
	result.FinishedTs = baseDDLEvent.CommitTs
	result.TableInfo.TableName = commonType.TableName{
		Schema: baseDDLEvent.Schema,
		Table:  baseDDLEvent.Table,
	}
	result.Type = byte(baseDDLEvent.Type)
	result.Query = baseDDLEvent.Query

	return result, nil
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
		return nil, nil, errors.New("unknown schema registry type")
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
		return nil, nil, errors.New("raw avro message is not a map")
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
