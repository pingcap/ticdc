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

package debezium

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/linkedin/goavro/v2"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	debeziumAvroKeySchemaSuffix   = "-key"
	debeziumAvroValueSchemaSuffix = "-value"

	debeziumAvroConnectFieldKey = "connect.field"
	debeziumAvroTiDBTypeKey     = "tidb_type"
	debeziumAvroDecimalName     = "org.apache.kafka.connect.data.Decimal"
)

type debeziumAvroMessage struct {
	Schema  *debeziumConnectSchema `json:"schema"`
	Payload any                    `json:"payload"`
}

type debeziumConnectSchema struct {
	Type       string                   `json:"type"`
	Optional   bool                     `json:"optional"`
	Name       string                   `json:"name"`
	Version    int                      `json:"version"`
	Field      string                   `json:"field"`
	Fields     []*debeziumConnectSchema `json:"fields"`
	Items      *debeziumConnectSchema   `json:"items"`
	Parameters map[string]string        `json:"parameters"`
	TiDBType   string                   `json:"tidb_type"`
}

type debeziumAvroSchemaConverter struct {
	definedNames map[string]struct{}
}

func (d *BatchEncoder) appendAvroRowChangedEvent(
	ctx context.Context,
	topic string,
	e *commonEvent.RowEvent,
) error {
	keyMessage, err := d.codec.buildDebeziumAvroKeyMessage(e)
	if err != nil {
		return err
	}
	valueMessage, err := d.codec.buildDebeziumAvroValueMessage(e)
	if err != nil {
		return err
	}
	message, err := d.encodeAvroConnectMessage(
		ctx,
		topic,
		keyMessage,
		valueMessage,
		e.TableInfo.GetUpdateTS(),
	)
	if err != nil {
		return err
	}
	message.Callback = e.Callback
	message.IncRowsCount()

	d.messages = append(d.messages, message)
	return nil
}

func (d *BatchEncoder) encodeAvroMessage(
	ctx context.Context,
	topic string,
	keyJSON []byte,
	valueJSON []byte,
	schemaVersion uint64,
) (*common.Message, error) {
	keyMessage, err := unmarshalDebeziumAvroMessage(keyJSON)
	if err != nil {
		return nil, err
	}
	valueMessage, err := unmarshalDebeziumAvroMessage(valueJSON)
	if err != nil {
		return nil, err
	}
	return d.encodeAvroConnectMessage(
		ctx,
		topic,
		keyMessage,
		valueMessage,
		schemaVersion,
	)
}

func (d *BatchEncoder) encodeAvroConnectMessage(
	ctx context.Context,
	topic string,
	keyMessage *debeziumAvroMessage,
	valueMessage *debeziumAvroMessage,
	schemaVersion uint64,
) (*common.Message, error) {
	key, err := d.encodeAvroPayload(
		ctx,
		topic,
		debeziumAvroKeySchemaSuffix,
		keyMessage,
		schemaVersion,
	)
	if err != nil {
		return nil, err
	}

	value, err := d.encodeAvroPayload(
		ctx,
		topic,
		debeziumAvroValueSchemaSuffix,
		valueMessage,
		schemaVersion,
	)
	if err != nil {
		return nil, err
	}

	return common.NewMsg(key, value), nil
}

func (d *BatchEncoder) encodeAvroPayload(
	ctx context.Context,
	topic string,
	subjectSuffix string,
	message *debeziumAvroMessage,
	schemaVersion uint64,
) ([]byte, error) {
	if message.Schema == nil {
		return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("schema is missing")
	}

	subject := debeziumAvroSubject(topic, subjectSuffix, message.Schema.Name)
	avroCodec, header, err := d.schemaM.GetCachedOrRegister(
		ctx,
		subject,
		schemaVersion,
		func() (string, error) {
			converter := newDebeziumAvroSchemaConverter()
			avroSchema, err := converter.toAvroSchema(message.Schema, "")
			if err != nil {
				return "", err
			}
			schemaBytes, err := json.Marshal(avroSchema)
			if err != nil {
				return "", errors.WrapError(errors.ErrAvroMarshalFailed, err)
			}
			return string(schemaBytes), nil
		},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	native, err := newDebeziumAvroSchemaConverter().toNative(message.Schema, message.Payload, "")
	if err != nil {
		return nil, err
	}
	binaryData, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, errors.WrapError(errors.ErrAvroEncodeToBinary, err)
	}

	result := make([]byte, 0, len(header)+len(binaryData))
	result = append(result, header...)
	result = append(result, binaryData...)
	return result, nil
}

func unmarshalDebeziumAvroMessage(data []byte) (*debeziumAvroMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var message debeziumAvroMessage
	if err := decoder.Decode(&message); err != nil {
		return nil, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
	}
	return &message, nil
}

func newDebeziumAvroSchemaConverter() *debeziumAvroSchemaConverter {
	return &debeziumAvroSchemaConverter{
		definedNames: make(map[string]struct{}),
	}
}

func debeziumAvroSubject(topic string, subjectSuffix string, schemaName string) string {
	if topic != "" {
		return topic + subjectSuffix
	}
	if schemaName != "" {
		return schemaName
	}
	return "debezium" + subjectSuffix
}

func (c *dbzCodec) buildDebeziumAvroKeyMessage(
	e *commonEvent.RowEvent,
) (*debeziumAvroMessage, error) {
	schemaName := e.TableInfo.GetSchemaName()
	tableName := e.TableInfo.GetTableName()
	columns := e.TableInfo.GetColumns()
	row := e.GetRows()
	if e.IsDelete() {
		row = e.GetPreRows()
	}

	fields := make([]*debeziumConnectSchema, 0)
	payload := make(map[string]any)
	for idx, colInfo := range columns {
		if colInfo == nil || !e.TableInfo.IsHandleKey(colInfo.ID) {
			continue
		}
		fieldSchema, err := c.buildDebeziumConnectFieldSchema(colInfo)
		if err != nil {
			return nil, err
		}
		fields = append(fields, fieldSchema)

		value, err := c.debeziumAvroFieldValue(row, idx, colInfo)
		if err != nil {
			return nil, err
		}
		payload[colInfo.Name.O] = value
	}

	return &debeziumAvroMessage{
		Schema: &debeziumConnectSchema{
			Type:     "struct",
			Name:     c.keySchemaName(schemaName, tableName),
			Optional: false,
			Fields:   fields,
		},
		Payload: payload,
	}, nil
}

func (c *dbzCodec) buildDebeziumAvroValueMessage(
	e *commonEvent.RowEvent,
) (*debeziumAvroMessage, error) {
	schemaName := e.TableInfo.GetSchemaName()
	tableName := e.TableInfo.GetTableName()
	rowSchema, err := c.buildDebeziumAvroRowSchema(e.TableInfo, e.ColumnSelector)
	if err != nil {
		return nil, err
	}
	sourceSchema, err := c.buildDebeziumConnectSourceSchema(schemaName)
	if err != nil {
		return nil, err
	}

	payload := make(map[string]any)
	payload["source"] = c.buildDebeziumAvroSourcePayload(e, schemaName, tableName)
	payload["ts_ms"] = c.nowFunc().UnixMilli()

	switch {
	case e.IsInsert():
		payload["op"] = "c"
		payload["before"] = nil
		after, err := c.buildDebeziumAvroRowPayload(
			e.GetRows(), e.TableInfo, e.ColumnSelector)
		if err != nil {
			return nil, err
		}
		payload["after"] = after
	case e.IsDelete():
		payload["op"] = "d"
		payload["after"] = nil
		before, err := c.buildDebeziumAvroRowPayload(
			e.GetPreRows(), e.TableInfo, e.ColumnSelector)
		if err != nil {
			return nil, err
		}
		payload["before"] = before
	case e.IsUpdate():
		payload["op"] = "u"
		if c.config.DebeziumOutputOldValue {
			before, err := c.buildDebeziumAvroRowPayload(
				e.GetPreRows(), e.TableInfo, e.ColumnSelector)
			if err != nil {
				return nil, err
			}
			payload["before"] = before
		}
		after, err := c.buildDebeziumAvroRowPayload(
			e.GetRows(), e.TableInfo, e.ColumnSelector)
		if err != nil {
			return nil, err
		}
		payload["after"] = after
	}

	return &debeziumAvroMessage{
		Schema: &debeziumConnectSchema{
			Type:     "struct",
			Optional: false,
			Name:     c.envelopeSchemaName(schemaName, tableName),
			Version:  1,
			Fields: []*debeziumConnectSchema{
				{
					Type:     "struct",
					Optional: true,
					Name:     c.valueSchemaName(schemaName, tableName),
					Field:    "before",
					Fields:   rowSchema,
				},
				{
					Type:     "struct",
					Optional: true,
					Name:     c.valueSchemaName(schemaName, tableName),
					Field:    "after",
					Fields:   rowSchema,
				},
				sourceSchema,
				{
					Type:     "string",
					Optional: false,
					Field:    "op",
				},
				{
					Type:     "int64",
					Optional: false,
					Field:    "ts_ms",
				},
			},
		},
		Payload: payload,
	}, nil
}

func (c *dbzCodec) buildDebeziumAvroRowSchema(
	tableInfo *commonType.TableInfo,
	columnSelector commonEvent.Selector,
) ([]*debeziumConnectSchema, error) {
	fields := make([]*debeziumConnectSchema, 0, len(tableInfo.GetColumns()))
	for _, colInfo := range tableInfo.GetColumns() {
		if !columnSelector.Select(colInfo) {
			continue
		}
		fieldSchema, err := c.buildDebeziumConnectFieldSchema(colInfo)
		if err != nil {
			return nil, err
		}
		fields = append(fields, fieldSchema)
	}
	if tableInfo.HasVirtualColumns() {
		for _, colInfo := range tableInfo.GetColumns() {
			if commonType.IsColCDCVisible(colInfo) {
				continue
			}
			fieldSchema, err := c.buildDebeziumConnectFieldSchema(colInfo)
			if err != nil {
				return nil, err
			}
			fields = append(fields, fieldSchema)
		}
	}
	return fields, nil
}

func (c *dbzCodec) buildDebeziumAvroRowPayload(
	row *chunk.Row,
	tableInfo *commonType.TableInfo,
	columnSelector commonEvent.Selector,
) (map[string]any, error) {
	payload := make(map[string]any)
	for idx, colInfo := range tableInfo.GetColumns() {
		if colInfo.IsVirtualGenerated() || !columnSelector.Select(colInfo) {
			continue
		}
		value, err := c.debeziumAvroFieldValue(row, idx, colInfo)
		if err != nil {
			return nil, err
		}
		payload[colInfo.Name.O] = value
	}
	return payload, nil
}

func (c *dbzCodec) buildDebeziumAvroSourcePayload(
	e *commonEvent.RowEvent,
	schemaName string,
	tableName string,
) map[string]any {
	commitTime := oracle.GetTimeFromTS(e.CommitTs)
	return map[string]any{
		"version":    "2.4.0.Final",
		"connector":  "TiCDC",
		"name":       c.clusterID,
		"ts_ms":      commitTime.UnixMilli(),
		"snapshot":   nil,
		"db":         schemaName,
		"table":      tableName,
		"server_id":  int64(0),
		"gtid":       nil,
		"file":       "",
		"pos":        int64(0),
		"row":        int32(0),
		"thread":     nil,
		"query":      nil,
		"commit_ts":  e.CommitTs,
		"cluster_id": c.clusterID,
	}
}

func (c *dbzCodec) buildDebeziumConnectFieldSchema(
	col *timodel.ColumnInfo,
) (*debeziumConnectSchema, error) {
	buf := &bytes.Buffer{}
	writer := util.BorrowJSONWriter(buf)
	c.writeDebeziumFieldSchema(writer, col)
	util.ReturnJSONWriter(writer)

	return decodeDebeziumConnectSchema(buf.Bytes())
}

func (c *dbzCodec) buildDebeziumConnectSourceSchema(
	schemaName string,
) (*debeziumConnectSchema, error) {
	buf := &bytes.Buffer{}
	writer := util.BorrowJSONWriter(buf)
	c.writeSourceSchema(writer, schemaName)
	util.ReturnJSONWriter(writer)

	return decodeDebeziumConnectSchema(buf.Bytes())
}

func decodeDebeziumConnectSchema(data []byte) (*debeziumConnectSchema, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var schema debeziumConnectSchema
	if err := decoder.Decode(&schema); err != nil {
		return nil, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
	}
	return &schema, nil
}

func (c *dbzCodec) debeziumAvroFieldValue(
	row *chunk.Row,
	idx int,
	colInfo *timodel.ColumnInfo,
) (any, error) {
	ft := &colInfo.FieldType
	datum := row.GetDatum(idx, ft)
	if datum.IsNull() {
		defaultVal := colInfo.GetDefaultValue()
		if defaultVal == nil {
			return nil, nil
		}
		val := getValueFromDefault(defaultVal, ft)
		datum.SetValue(val, ft)
	}

	switch colInfo.GetType() {
	case mysql.TypeBit:
		n := ft.GetFlen()
		v, err := datum.GetMysqlBit().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			return nil, errors.WrapError(errors.ErrDebeziumEncodeFailed, err)
		}
		if n == 1 {
			return v != 0, nil
		}
		return base64.StdEncoding.EncodeToString(getBitFromUint64(n, v)), nil

	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		v := datum.GetBytes()
		if !mysql.HasBinaryFlag(colInfo.GetFlag()) {
			return common.UnsafeBytesToString(v), nil
		}
		return base64.StdEncoding.EncodeToString(v), nil

	case mysql.TypeEnum:
		v := datum.GetMysqlEnum().Value
		if enumVar, err := types.ParseEnumValue(ft.GetElems(), v); err == nil {
			return enumVar.Name, nil
		}
		return "", nil

	case mysql.TypeSet:
		v := datum.GetMysqlSet().Value
		if setVar, err := types.ParseSetValue(ft.GetElems(), v); err == nil {
			return setVar.Name, nil
		}
		return "", nil

	case mysql.TypeNewDecimal:
		return datum.GetMysqlDecimal().String(), nil

	case mysql.TypeDate, mysql.TypeNewDate:
		v := datum.GetMysqlTime().String()
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			if mysql.HasNotNullFlag(ft.GetFlag()) {
				return int64(0), nil
			}
			return nil, nil
		}
		year := t.Year()
		if year < 70 {
			t = t.AddDate(2000, 0, 0)
		} else if year < 100 {
			t = t.AddDate(1900, 0, 0)
		}
		return t.UTC().Unix() / 60 / 60 / 24, nil

	case mysql.TypeDatetime:
		value := datum.GetValue()
		var v types.Time
		switch val := value.(type) {
		case string:
			if val == "CURRENT_TIMESTAMP" {
				return int64(0), nil
			}
			t, err := types.StrToDateTime(
				types.DefaultStmtNoWarningContext.WithLocation(c.config.TimeZone),
				val,
				ft.GetDecimal(),
			)
			if err != nil {
				return nil, errors.WrapError(errors.ErrDebeziumEncodeFailed, err)
			}
			v = t
		case types.Time:
			v = val
		default:
			return nil, errors.Trace(errors.ErrDebeziumEncodeFailed)
		}
		gt, err := v.GoTime(time.UTC)
		if err != nil {
			if mysql.HasNotNullFlag(ft.GetFlag()) {
				return int64(0), nil
			}
			return nil, nil
		}
		year := gt.Year()
		if year < 70 {
			gt = gt.AddDate(2000, 0, 0)
		} else if year < 100 {
			gt = gt.AddDate(1900, 0, 0)
		}
		if ft.GetDecimal() <= 3 {
			return gt.UnixMilli(), nil
		}
		return gt.UnixMicro(), nil

	case mysql.TypeTimestamp:
		value := datum.GetValue()
		var v types.Time
		switch val := value.(type) {
		case string:
			if val == "CURRENT_TIMESTAMP" {
				if mysql.HasNotNullFlag(ft.GetFlag()) {
					return "1970-01-01T00:00:00Z", nil
				}
				return nil, nil
			}
			t, err := types.StrToDateTime(
				types.DefaultStmtNoWarningContext.WithLocation(c.config.TimeZone),
				val,
				ft.GetDecimal(),
			)
			if err != nil {
				return nil, errors.WrapError(errors.ErrDebeziumEncodeFailed, err)
			}
			v = t
		case types.Time:
			v = val
		}
		if v.Compare(types.MinTimestamp) < 0 {
			if row.IsNull(idx) {
				return nil, nil
			}
			return "1970-01-01T00:00:00Z", nil
		}
		gt, err := v.GoTime(c.config.TimeZone)
		if err != nil {
			return nil, errors.WrapError(errors.ErrDebeziumEncodeFailed, err)
		}
		str := gt.UTC().Format("2006-01-02T15:04:05")
		fsp := ft.GetDecimal()
		if fsp > 0 {
			tmp := fmt.Sprintf(".%06d", gt.Nanosecond()/1000)
			str += tmp[:1+fsp]
		}
		return str + "Z", nil

	case mysql.TypeDuration:
		return datum.GetMysqlDuration().Microseconds(), nil

	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		maxValue := types.GetMaxValue(ft)
		minValue := types.GetMinValue(ft)
		if mysql.HasUnsignedFlag(colInfo.GetFlag()) {
			v := datum.GetUint64()
			if ft.GetType() == mysql.TypeLonglong {
				if c.config.AvroBigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeString {
					return strconv.FormatUint(v, 10), nil
				}
				if v > math.MaxInt64 {
					return nil, errors.ErrDebeziumEncodeFailed.GenWithStackByArgs(
						fmt.Sprintf("unsigned bigint value %d overflows avro long", v))
				}
				return int64(v), nil
			}
			if ft.GetType() == mysql.TypeLonglong && v == maxValue.GetUint64() ||
				v > maxValue.GetUint64() {
				return -1, nil
			}
			return int64(v), nil
		}
		v := datum.GetInt64()
		if v < minValue.GetInt64() || v > maxValue.GetInt64() {
			return -1, nil
		}
		return v, nil

	case mysql.TypeDouble:
		return datum.GetFloat64(), nil

	case mysql.TypeFloat:
		return datum.GetFloat32(), nil

	case mysql.TypeYear:
		return datum.GetInt64(), nil

	case mysql.TypeTiDBVectorFloat32:
		return datum.GetVectorFloat32().String(), nil
	}

	return fmt.Sprintf("%v", datum.GetValue()), nil
}

func (c *debeziumAvroSchemaConverter) toAvroSchema(
	schema *debeziumConnectSchema,
	fallbackName string,
) (any, error) {
	if schema == nil {
		return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("schema is nil")
	}

	switch schema.Type {
	case "struct":
		fullName := avroFullName(schema.Name, fallbackName)
		if _, exists := c.definedNames[fullName]; exists {
			return fullName, nil
		}
		c.definedNames[fullName] = struct{}{}

		name, namespace := splitAvroFullName(fullName)
		record := map[string]any{
			"type":   "record",
			"name":   name,
			"fields": make([]any, 0, len(schema.Fields)),
		}
		if namespace != "" {
			record["namespace"] = namespace
		}
		addConnectMetadata(record, schema)

		fields := record["fields"].([]any)
		for _, fieldSchema := range schema.Fields {
			fieldName := avroFieldName(fieldSchema.Field)
			fieldType, err := c.toAvroSchema(fieldSchema, fieldName)
			if err != nil {
				return nil, err
			}

			field := map[string]any{
				"name": fieldName,
				"type": fieldType,
			}
			if fieldSchema.Field != "" {
				field[debeziumAvroConnectFieldKey] = fieldSchema.Field
			}
			if fieldSchema.TiDBType != "" {
				field[debeziumAvroTiDBTypeKey] = fieldSchema.TiDBType
			}
			if fieldSchema.Optional {
				field["type"] = []any{"null", fieldType}
				field["default"] = nil
			}
			fields = append(fields, field)
		}
		record["fields"] = fields
		return record, nil
	case "array":
		if schema.Items == nil {
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("array schema is missing items")
		}
		items, err := c.toAvroSchema(schema.Items, fallbackName+"Item")
		if err != nil {
			return nil, err
		}
		arraySchema := map[string]any{
			"type":  "array",
			"items": items,
		}
		addConnectMetadata(arraySchema, schema)
		return arraySchema, nil
	default:
		if isDebeziumAvroDecimalSchema(schema) {
			precision, scale, err := decimalSchemaPrecisionAndScale(schema)
			if err != nil {
				return nil, err
			}
			decimalSchema := map[string]any{
				"type":        "bytes",
				"logicalType": "decimal",
				"precision":   precision,
				"scale":       scale,
			}
			addConnectMetadata(decimalSchema, schema)
			return decimalSchema, nil
		}

		avroType, err := connectPrimitiveToAvro(schema.Type)
		if err != nil {
			return nil, err
		}
		if !hasConnectMetadata(schema) && schema.Type != "int8" && schema.Type != "int16" {
			return avroType, nil
		}
		primitive := map[string]any{
			"type": avroType,
		}
		if schema.Type == "int8" || schema.Type == "int16" {
			primitive["connect.type"] = schema.Type
		}
		addConnectMetadata(primitive, schema)
		return primitive, nil
	}
}

func (c *debeziumAvroSchemaConverter) toNative(
	schema *debeziumConnectSchema,
	value any,
	fallbackName string,
) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch schema.Type {
	case "struct":
		valueMap, ok := value.(map[string]any)
		if !ok {
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("struct payload is not an object")
		}

		native := make(map[string]any, len(schema.Fields))
		for _, fieldSchema := range schema.Fields {
			fieldName := avroFieldName(fieldSchema.Field)
			rawValue := valueMap[fieldSchema.Field]
			if rawValue == nil && fieldSchema.Field != fieldName {
				rawValue = valueMap[fieldName]
			}

			fieldValue, err := c.toNative(fieldSchema, rawValue, fieldName)
			if err != nil {
				return nil, err
			}
			if fieldSchema.Optional {
				if fieldValue == nil {
					native[fieldName] = nil
				} else {
					native[fieldName] = goavro.Union(
						avroUnionBranchName(fieldSchema, fieldName),
						fieldValue,
					)
				}
			} else {
				native[fieldName] = fieldValue
			}
		}
		return native, nil
	case "array":
		values, ok := value.([]any)
		if !ok {
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("array payload is not an array")
		}
		native := make([]any, 0, len(values))
		for _, item := range values {
			itemValue, err := c.toNative(schema.Items, item, fallbackName+"Item")
			if err != nil {
				return nil, err
			}
			if schema.Items.Optional && itemValue != nil {
				itemValue = goavro.Union(
					avroUnionBranchName(schema.Items, fallbackName+"Item"),
					itemValue,
				)
			}
			native = append(native, itemValue)
		}
		return native, nil
	case "boolean":
		v, ok := value.(bool)
		if !ok {
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("boolean payload is invalid")
		}
		return v, nil
	case "string":
		v, ok := value.(string)
		if !ok {
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("string payload is invalid")
		}
		return v, nil
	case "bytes":
		if isDebeziumAvroDecimalSchema(schema) {
			v, ok := value.(string)
			if !ok {
				return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("decimal payload is invalid")
			}
			rat, ok := new(big.Rat).SetString(v)
			if !ok {
				return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("decimal payload is invalid")
			}
			return rat, nil
		}

		v, ok := value.(string)
		if !ok {
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("bytes payload is invalid")
		}
		data, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
		}
		return data, nil
	case "int8", "int16", "int32":
		switch v := value.(type) {
		case json.Number:
			i, err := v.Int64()
			if err == nil {
				return int32(i), nil
			}
			f, err := v.Float64()
			if err != nil {
				return nil, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
			}
			return int32(f), nil
		case int:
			return int32(v), nil
		case int32:
			return v, nil
		case int64:
			return int32(v), nil
		case uint64:
			return int32(v), nil
		case float64:
			return int32(v), nil
		default:
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("number payload is invalid")
		}
	case "int64":
		switch v := value.(type) {
		case json.Number:
			i, err := v.Int64()
			if err == nil {
				return i, nil
			}
			f, err := v.Float64()
			if err != nil {
				return nil, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
			}
			return int64(f), nil
		case int:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		case uint64:
			return int64(v), nil
		case float64:
			return int64(v), nil
		default:
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("number payload is invalid")
		}
	case "float":
		switch v := value.(type) {
		case json.Number:
			f, err := v.Float64()
			if err != nil {
				return nil, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
			}
			return float32(f), nil
		case int:
			return float32(v), nil
		case int32:
			return float32(v), nil
		case int64:
			return float32(v), nil
		case uint64:
			return float32(v), nil
		case float32:
			return v, nil
		case float64:
			return float32(v), nil
		default:
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("number payload is invalid")
		}
	case "double":
		switch v := value.(type) {
		case json.Number:
			f, err := v.Float64()
			if err != nil {
				return nil, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
			}
			return f, nil
		case int:
			return float64(v), nil
		case int32:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case uint64:
			return float64(v), nil
		case float32:
			return float64(v), nil
		case float64:
			return v, nil
		default:
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("number payload is invalid")
		}
	default:
		return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("unsupported schema type " + schema.Type)
	}
}

func connectPrimitiveToAvro(connectType string) (string, error) {
	switch connectType {
	case "boolean":
		return "boolean", nil
	case "string":
		return "string", nil
	case "bytes":
		return "bytes", nil
	case "int8", "int16", "int32":
		return "int", nil
	case "int64":
		return "long", nil
	case "float":
		return "float", nil
	case "double":
		return "double", nil
	default:
		return "", errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("unsupported schema type " + connectType)
	}
}

func isDebeziumAvroDecimalSchema(schema *debeziumConnectSchema) bool {
	return schema.Type == "bytes" && schema.Name == debeziumAvroDecimalName
}

func decimalSchemaPrecisionAndScale(schema *debeziumConnectSchema) (int, int, error) {
	if schema.Parameters == nil {
		return 0, 0, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("decimal schema is missing parameters")
	}
	precision, err := strconv.Atoi(schema.Parameters["precision"])
	if err != nil {
		return 0, 0, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
	}
	scale, err := strconv.Atoi(schema.Parameters["scale"])
	if err != nil {
		return 0, 0, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
	}
	return precision, scale, nil
}

func addConnectMetadata(avroSchema map[string]any, schema *debeziumConnectSchema) {
	if schema.Name != "" {
		avroSchema["connect.name"] = schema.Name
	}
	if schema.Version != 0 {
		avroSchema["connect.version"] = schema.Version
	}
	if len(schema.Parameters) != 0 {
		avroSchema["connect.parameters"] = schema.Parameters
	}
}

func hasConnectMetadata(schema *debeziumConnectSchema) bool {
	return schema.Name != "" || schema.Version != 0 || len(schema.Parameters) != 0
}

func avroFullName(connectName string, fallbackName string) string {
	if connectName != "" {
		return sanitizeAvroFullName(connectName)
	}
	if fallbackName != "" {
		return sanitizeAvroFullName(fallbackName)
	}
	return "ConnectDefault"
}

func splitAvroFullName(fullName string) (name string, namespace string) {
	fullName = sanitizeAvroFullName(fullName)
	idx := strings.LastIndex(fullName, ".")
	if idx < 0 {
		return fullName, ""
	}
	return fullName[idx+1:], fullName[:idx]
}

func sanitizeAvroFullName(fullName string) string {
	parts := strings.Split(fullName, ".")
	for i := range parts {
		parts[i] = avroFieldName(parts[i])
	}
	return strings.Join(parts, ".")
}

func avroFieldName(field string) string {
	return common.SanitizeName(field)
}

func avroUnionBranchName(schema *debeziumConnectSchema, fallbackName string) string {
	if isDebeziumAvroDecimalSchema(schema) {
		return "bytes.decimal"
	}

	switch schema.Type {
	case "struct":
		return avroFullName(schema.Name, fallbackName)
	case "array":
		return "array"
	case "int8", "int16", "int32":
		return "int"
	case "int64":
		return "long"
	case "float":
		return "float"
	case "double":
		return "double"
	default:
		return schema.Type
	}
}
