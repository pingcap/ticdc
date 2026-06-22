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
	"strings"

	"github.com/linkedin/goavro/v2"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

const (
	debeziumAvroKeySchemaSuffix   = "-key"
	debeziumAvroValueSchemaSuffix = "-value"
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
}

type debeziumAvroSchemaConverter struct {
	definedNames map[string]struct{}
}

func (d *BatchEncoder) appendAvroRowChangedEvent(
	ctx context.Context,
	topic string,
	e *commonEvent.RowEvent,
) error {
	keyBuf := bytes.Buffer{}
	if err := d.codec.EncodeKey(e, &keyBuf); err != nil {
		return errors.Trace(err)
	}

	valueBuf := bytes.Buffer{}
	if err := d.codec.EncodeValue(e, &valueBuf); err != nil {
		return errors.Trace(err)
	}

	message, err := d.encodeAvroMessage(
		ctx,
		topic,
		keyBuf.Bytes(),
		valueBuf.Bytes(),
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
	key, err := d.encodeAvroPayload(
		ctx,
		topic,
		debeziumAvroKeySchemaSuffix,
		keyJSON,
		schemaVersion,
	)
	if err != nil {
		return nil, err
	}

	value, err := d.encodeAvroPayload(
		ctx,
		topic,
		debeziumAvroValueSchemaSuffix,
		valueJSON,
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
	data []byte,
	schemaVersion uint64,
) ([]byte, error) {
	message, err := unmarshalDebeziumAvroMessage(data)
	if err != nil {
		return nil, err
	}
	if message.Schema == nil {
		return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("schema is missing")
	}

	converter := newDebeziumAvroSchemaConverter()
	avroSchema, err := converter.toAvroSchema(message.Schema, "")
	if err != nil {
		return nil, err
	}
	schemaBytes, err := json.Marshal(avroSchema)
	if err != nil {
		return nil, errors.WrapError(errors.ErrAvroMarshalFailed, err)
	}

	subject := debeziumAvroSubject(topic, subjectSuffix, message.Schema.Name)
	avroCodec, header, err := d.schemaM.GetCachedOrRegister(
		ctx,
		subject,
		schemaVersion,
		func() (string, error) {
			return string(schemaBytes), nil
		},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	native, err := converter.toNative(message.Schema, message.Payload, "")
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
		v, err := numberToInt64(value)
		if err != nil {
			return nil, err
		}
		return int32(v), nil
	case "int64":
		return numberToInt64(value)
	case "float":
		v, err := numberToFloat64(value)
		if err != nil {
			return nil, err
		}
		return float32(v), nil
	case "double":
		return numberToFloat64(value)
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
		return connectName
	}
	if fallbackName != "" {
		return avroFieldName(fallbackName)
	}
	return "ConnectDefault"
}

func splitAvroFullName(fullName string) (name string, namespace string) {
	idx := strings.LastIndex(fullName, ".")
	if idx < 0 {
		return avroFieldName(fullName), ""
	}
	return avroFieldName(fullName[idx+1:]), fullName[:idx]
}

func avroFieldName(field string) string {
	return common.SanitizeName(field)
}

func avroUnionBranchName(schema *debeziumConnectSchema, fallbackName string) string {
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

func numberToInt64(value any) (int64, error) {
	switch v := value.(type) {
	case json.Number:
		i, err := v.Int64()
		if err == nil {
			return i, nil
		}
		f, err := v.Float64()
		if err != nil {
			return 0, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
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
		return 0, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("number payload is invalid")
	}
}

func numberToFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
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
		return 0, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("number payload is invalid")
	}
}
