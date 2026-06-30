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
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	codecavro "github.com/pingcap/ticdc/pkg/sink/codec/avro"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

const confluentAvroHeaderLen = 5

type avroDecoder struct {
	ctx         context.Context
	registryURL string
	httpClient  *http.Client
	inner       *decoder

	mu      sync.RWMutex
	schemas map[int]*registeredDebeziumAvroSchema
}

type registeredDebeziumAvroSchema struct {
	schema       any
	namedSchemas map[string]any
	codec        *goavro.Codec
}

// NewAvroDecoder returns a Debezium decoder for Confluent Avro wire-format
// messages. It decodes the Avro payload and then delegates Debezium event
// semantics to the JSON decoder.
func NewAvroDecoder(
	ctx context.Context,
	config *common.Config,
	idx int,
	db *sql.DB,
) (common.Decoder, error) {
	registryURL := strings.TrimRight(config.AvroConfluentSchemaRegistry, "/")
	if registryURL == "" {
		return nil, errors.ErrAvroSchemaAPIError.GenWithStackByArgs("schema registry URI is empty")
	}

	return &avroDecoder{
		ctx:         ctx,
		registryURL: registryURL,
		httpClient:  http.DefaultClient,
		inner:       NewDecoder(config, idx, db).(*decoder),
		schemas:     make(map[int]*registeredDebeziumAvroSchema),
	}, nil
}

func (d *avroDecoder) AddKeyValue(key, value []byte) {
	keyJSON, err := d.toDebeziumJSON(key)
	if err != nil {
		log.Panic("decode Debezium Avro key failed", zap.Error(err), zap.Int("keySize", len(key)))
	}
	valueJSON, err := d.toDebeziumJSON(value)
	if err != nil {
		log.Panic("decode Debezium Avro value failed", zap.Error(err), zap.Int("valueSize", len(value)))
	}
	d.inner.AddKeyValue(keyJSON, valueJSON)
}

func (d *avroDecoder) HasNext() (common.MessageType, bool) {
	return d.inner.HasNext()
}

func (d *avroDecoder) NextResolvedEvent() uint64 {
	return d.inner.NextResolvedEvent()
}

func (d *avroDecoder) NextDMLEvent() *commonEvent.DMLEvent {
	return d.inner.NextDMLEvent()
}

func (d *avroDecoder) NextDDLEvent() *commonEvent.DDLEvent {
	return d.inner.NextDDLEvent()
}

func (d *avroDecoder) toDebeziumJSON(data []byte) ([]byte, error) {
	payload, schema, err := d.decodeConfluentAvroMessage(data)
	if err != nil {
		return nil, err
	}
	message := map[string]any{
		"schema":  schema,
		"payload": payload,
	}
	result, err := json.Marshal(message)
	if err != nil {
		return nil, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
	}
	return result, nil
}

func (d *avroDecoder) decodeConfluentAvroMessage(data []byte) (any, map[string]any, error) {
	if len(data) == 0 {
		return nil, nil, errors.ErrDebeziumEmptyValueMessage.GenWithStackByArgs()
	}
	if len(data) < confluentAvroHeaderLen {
		return nil, nil, errors.ErrAvroInvalidMessage.GenWithStackByArgs("confluent header is too short")
	}
	if data[0] != 0 {
		return nil, nil, errors.ErrAvroInvalidMessage.GenWithStackByArgs("invalid confluent magic byte")
	}

	schemaID := int(binary.BigEndian.Uint32(data[1:confluentAvroHeaderLen]))
	registeredSchema, err := d.getSchema(schemaID)
	if err != nil {
		return nil, nil, err
	}

	native, _, err := registeredSchema.codec.NativeFromBinary(data[confluentAvroHeaderLen:])
	if err != nil {
		return nil, nil, errors.WrapError(errors.ErrAvroInvalidMessage, err)
	}

	payload, err := avroNativeToConnectPayload(
		registeredSchema.schema,
		native,
		registeredSchema.namedSchemas,
	)
	if err != nil {
		return nil, nil, err
	}
	schema, err := avroSchemaToConnectSchema(
		registeredSchema.schema,
		"",
		nil,
		registeredSchema.namedSchemas,
	)
	if err != nil {
		return nil, nil, err
	}
	return payload, schema, nil
}

func (d *avroDecoder) getSchema(schemaID int) (*registeredDebeziumAvroSchema, error) {
	d.mu.RLock()
	schema, ok := d.schemas[schemaID]
	d.mu.RUnlock()
	if ok {
		return schema, nil
	}

	uri := d.registryURL + "/schemas/ids/" + strconv.Itoa(schemaID)
	req, err := http.NewRequestWithContext(d.ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}
	req.Header.Add(
		"Accept",
		"application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, "+
			"application/json",
	)

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.ErrAvroSchemaAPIError.GenWithStackByArgs(
			"failed to query schema id " + strconv.Itoa(schemaID))
	}

	var lookupResp struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &lookupResp); err != nil {
		return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}

	codec, err := codecavro.GenCodec(lookupResp.Schema)
	if err != nil {
		return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}

	decoder := json.NewDecoder(strings.NewReader(lookupResp.Schema))
	decoder.UseNumber()
	var schemaDef any
	if err := decoder.Decode(&schemaDef); err != nil {
		return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}

	namedSchemas := make(map[string]any)
	collectAvroNamedSchemas(schemaDef, namedSchemas)

	schema = &registeredDebeziumAvroSchema{
		schema:       schemaDef,
		namedSchemas: namedSchemas,
		codec:        codec,
	}
	d.mu.Lock()
	d.schemas[schemaID] = schema
	d.mu.Unlock()
	return schema, nil
}

func avroNativeToConnectPayload(schema any, value any, namedSchemas map[string]any) (any, error) {
	switch typedSchema := schema.(type) {
	case []any:
		if value == nil {
			return nil, nil
		}
		branchSchema, branchValue, err := avroUnionBranch(typedSchema, value)
		if err != nil {
			return nil, err
		}
		return avroNativeToConnectPayload(branchSchema, branchValue, namedSchemas)
	case map[string]any:
		rawType, ok := typedSchema["type"]
		if !ok {
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro schema is missing type")
		}
		if unionType, ok := rawType.([]any); ok {
			return avroNativeToConnectPayload(unionType, value, namedSchemas)
		}
		typeName, ok := rawType.(string)
		if !ok {
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro schema type is invalid")
		}
		switch typeName {
		case "record":
			valueMap, ok := value.(map[string]any)
			if !ok {
				return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro record payload is invalid")
			}
			fields, ok := typedSchema["fields"].([]any)
			if !ok {
				return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro record schema is missing fields")
			}
			result := make(map[string]any, len(fields))
			for _, rawField := range fields {
				field, ok := rawField.(map[string]any)
				if !ok {
					return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro field schema is invalid")
				}
				avroFieldName, ok := field["name"].(string)
				if !ok {
					return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro field is missing name")
				}
				connectFieldName := avroConnectFieldName(field, avroFieldName)
				rawValue, exists := valueMap[avroFieldName]
				if !exists && connectFieldName != avroFieldName {
					rawValue, exists = valueMap[connectFieldName]
				}
				if !exists {
					rawValue, exists = avroMissingFieldValue(field)
				}
				if !exists {
					return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs(
						"avro record payload is missing field " + avroFieldName)
				}
				fieldValue, err := avroNativeToConnectPayload(
					field["type"],
					rawValue,
					namedSchemas,
				)
				if err != nil {
					return nil, err
				}
				result[connectFieldName] = fieldValue
			}
			return result, nil
		case "array":
			items, ok := typedSchema["items"]
			if !ok {
				return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro array schema is missing items")
			}
			if value == nil {
				return []any{}, nil
			}
			values, ok := value.([]any)
			if !ok {
				return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro array payload is invalid")
			}
			result := make([]any, 0, len(values))
			for _, item := range values {
				itemValue, err := avroNativeToConnectPayload(items, item, namedSchemas)
				if err != nil {
					return nil, err
				}
				result = append(result, itemValue)
			}
			return result, nil
		case "bytes":
			if avroSchemaIsDecimal(typedSchema) {
				return avroDecimalNativeToString(typedSchema, value)
			}
			return value, nil
		default:
			return value, nil
		}
	case string:
		if namedSchema, ok := namedSchemas[typedSchema]; ok {
			return avroNativeToConnectPayload(namedSchema, value, namedSchemas)
		}
		return value, nil
	default:
		return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro schema is invalid")
	}
}

func avroSchemaToConnectSchema(
	schema any,
	fieldName string,
	fieldMeta map[string]any,
	namedSchemas map[string]any,
) (map[string]any, error) {
	switch typedSchema := schema.(type) {
	case []any:
		branchSchema, _, err := avroNonNullUnionBranch(typedSchema)
		if err != nil {
			return nil, err
		}
		connectSchema, err := avroSchemaToConnectSchema(
			branchSchema,
			fieldName,
			fieldMeta,
			namedSchemas,
		)
		if err != nil {
			return nil, err
		}
		connectSchema["optional"] = true
		return connectSchema, nil
	case map[string]any:
		rawType, ok := typedSchema["type"]
		if !ok {
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro schema is missing type")
		}
		if unionType, ok := rawType.([]any); ok {
			return avroSchemaToConnectSchema(unionType, fieldName, fieldMeta, namedSchemas)
		}
		typeName, ok := rawType.(string)
		if !ok {
			return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro schema type is invalid")
		}
		switch typeName {
		case "record":
			connectSchema := newConnectSchema("struct", false, fieldName, typedSchema, fieldMeta)
			fields, ok := typedSchema["fields"].([]any)
			if !ok {
				return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro record schema is missing fields")
			}
			connectFields := make([]any, 0, len(fields))
			for _, rawField := range fields {
				field, ok := rawField.(map[string]any)
				if !ok {
					return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro field schema is invalid")
				}
				avroFieldName, ok := field["name"].(string)
				if !ok {
					return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro field is missing name")
				}
				fieldSchema, err := avroSchemaToConnectSchema(
					field["type"],
					avroConnectFieldName(field, avroFieldName),
					field,
					namedSchemas,
				)
				if err != nil {
					return nil, err
				}
				connectFields = append(connectFields, fieldSchema)
			}
			connectSchema["fields"] = connectFields
			return connectSchema, nil
		case "array":
			connectSchema := newConnectSchema("array", false, fieldName, typedSchema, fieldMeta)
			items, ok := typedSchema["items"]
			if !ok {
				return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro array schema is missing items")
			}
			connectItems, err := avroSchemaToConnectSchema(items, "", nil, namedSchemas)
			if err != nil {
				return nil, err
			}
			connectSchema["items"] = connectItems
			return connectSchema, nil
		default:
			connectType, err := avroPrimitiveToConnectType(typeName, typedSchema)
			if err != nil {
				return nil, err
			}
			return newConnectSchema(connectType, false, fieldName, typedSchema, fieldMeta), nil
		}
	case string:
		if namedSchema, ok := namedSchemas[typedSchema]; ok {
			return avroSchemaToConnectSchema(namedSchema, fieldName, fieldMeta, namedSchemas)
		}
		connectType, err := avroPrimitiveToConnectType(typedSchema, nil)
		if err != nil {
			return nil, err
		}
		return newConnectSchema(connectType, false, fieldName, nil, fieldMeta), nil
	default:
		return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro schema is invalid")
	}
}

func collectAvroNamedSchemas(schema any, namedSchemas map[string]any) {
	switch typedSchema := schema.(type) {
	case []any:
		for _, branch := range typedSchema {
			collectAvroNamedSchemas(branch, namedSchemas)
		}
	case map[string]any:
		rawType := typedSchema["type"]
		if unionType, ok := rawType.([]any); ok {
			collectAvroNamedSchemas(unionType, namedSchemas)
			return
		}
		typeName, _ := rawType.(string)
		switch typeName {
		case "record":
			name := avroBranchName(typedSchema)
			if name != "" {
				namedSchemas[name] = typedSchema
				shortName := avroShortBranchName(name)
				if _, exists := namedSchemas[shortName]; shortName != "" && !exists {
					namedSchemas[shortName] = typedSchema
				}
			}
			fields, _ := typedSchema["fields"].([]any)
			for _, rawField := range fields {
				field, ok := rawField.(map[string]any)
				if !ok {
					continue
				}
				collectAvroNamedSchemas(field["type"], namedSchemas)
			}
		case "array":
			collectAvroNamedSchemas(typedSchema["items"], namedSchemas)
		}
	}
}

func newConnectSchema(
	connectType string,
	optional bool,
	fieldName string,
	schemaMeta map[string]any,
	fieldMeta map[string]any,
) map[string]any {
	connectSchema := map[string]any{
		"type":     connectType,
		"optional": optional,
	}
	if fieldName != "" {
		connectSchema["field"] = fieldName
	}
	addConnectSchemaMetadata(connectSchema, schemaMeta)
	addConnectFieldMetadata(connectSchema, fieldMeta)
	return connectSchema
}

func addConnectSchemaMetadata(connectSchema map[string]any, schemaMeta map[string]any) {
	if schemaMeta == nil {
		return
	}
	if name, ok := schemaMeta["connect.name"].(string); ok && name != "" {
		connectSchema["name"] = name
	}
	if version, ok := schemaMeta["connect.version"]; ok {
		connectSchema["version"] = version
	}
	if parameters, ok := schemaMeta["connect.parameters"].(map[string]any); ok {
		connectSchema["parameters"] = parameters
	}
	if tidbType, ok := schemaMeta[debeziumAvroTiDBTypeKey].(string); ok && tidbType != "" {
		connectSchema[debeziumAvroTiDBTypeKey] = tidbType
	}
}

func addConnectFieldMetadata(connectSchema map[string]any, fieldMeta map[string]any) {
	if fieldMeta == nil {
		return
	}
	if tidbType, ok := fieldMeta[debeziumAvroTiDBTypeKey].(string); ok && tidbType != "" {
		connectSchema[debeziumAvroTiDBTypeKey] = tidbType
	}
}

func avroPrimitiveToConnectType(avroType string, schemaMeta map[string]any) (string, error) {
	if schemaMeta != nil {
		if connectType, ok := schemaMeta["connect.type"].(string); ok && connectType != "" {
			return connectType, nil
		}
	}
	switch avroType {
	case "boolean":
		return "boolean", nil
	case "string":
		return "string", nil
	case "bytes":
		return "bytes", nil
	case "int":
		return "int32", nil
	case "long":
		return "int64", nil
	case "float":
		return "float", nil
	case "double":
		return "double", nil
	default:
		return "", errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("unsupported avro type " + avroType)
	}
}

func avroSchemaIsDecimal(schema map[string]any) bool {
	typeName, _ := schema["type"].(string)
	logicalType, _ := schema["logicalType"].(string)
	return typeName == "bytes" && logicalType == "decimal"
}

func avroDecimalNativeToString(schema map[string]any, value any) (string, error) {
	scale, err := avroDecimalScale(schema)
	if err != nil {
		return "", err
	}
	switch v := value.(type) {
	case *big.Rat:
		return v.FloatString(scale), nil
	case big.Rat:
		return v.FloatString(scale), nil
	case string:
		return v, nil
	default:
		return "", errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("decimal payload is invalid")
	}
}

func avroDecimalScale(schema map[string]any) (int, error) {
	switch scale := schema["scale"].(type) {
	case float64:
		return int(scale), nil
	case int:
		return scale, nil
	case int32:
		return int(scale), nil
	case int64:
		return int(scale), nil
	case json.Number:
		value, err := scale.Int64()
		if err != nil {
			return 0, errors.WrapError(errors.ErrDebeziumInvalidMessage, err)
		}
		return int(value), nil
	default:
		return 0, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("decimal schema is missing scale")
	}
}

func avroUnionBranch(union []any, value any) (any, any, error) {
	if value == nil {
		return nil, nil, nil
	}
	var wrappedBranchName string
	var wrappedBranchValue any
	hasWrappedBranch := false
	if branchValueMap, ok := value.(map[string]any); ok && len(branchValueMap) == 1 {
		for branchName, branchValue := range branchValueMap {
			wrappedBranchName = branchName
			wrappedBranchValue = branchValue
			hasWrappedBranch = true
			for _, branchSchema := range union {
				if avroBranchName(branchSchema) == branchName {
					return branchSchema, branchValue, nil
				}
			}
		}
	}

	branchSchema, isSingleNonNullBranch, err := avroNonNullUnionBranch(union)
	if err != nil {
		return nil, nil, err
	}
	if hasWrappedBranch &&
		isSingleNonNullBranch &&
		avroShortBranchName(branchSchema) == avroShortBranchName(wrappedBranchName) {
		return branchSchema, wrappedBranchValue, nil
	}
	return branchSchema, value, nil
}

func avroNonNullUnionBranch(union []any) (any, bool, error) {
	var result any
	count := 0
	for _, branch := range union {
		if avroBranchName(branch) != "null" {
			if count == 0 {
				result = branch
			}
			count++
		}
	}
	if count > 0 {
		return result, count == 1, nil
	}
	return nil, false, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("avro union has no non-null branch")
}

func avroBranchName(schema any) string {
	switch typedSchema := schema.(type) {
	case string:
		return typedSchema
	case map[string]any:
		typeName, _ := typedSchema["type"].(string)
		switch typeName {
		case "record":
			name, _ := typedSchema["name"].(string)
			namespace, _ := typedSchema["namespace"].(string)
			if namespace != "" && name != "" {
				return namespace + "." + name
			}
			return name
		case "array":
			return "array"
		default:
			if avroSchemaIsDecimal(typedSchema) {
				return "bytes.decimal"
			}
			return typeName
		}
	default:
		return ""
	}
}

func avroShortBranchName(schema any) string {
	switch typedSchema := schema.(type) {
	case string:
		if idx := strings.LastIndex(typedSchema, "."); idx >= 0 {
			return typedSchema[idx+1:]
		}
		return typedSchema
	case map[string]any:
		typeName, _ := typedSchema["type"].(string)
		if typeName == "record" {
			name, _ := typedSchema["name"].(string)
			return name
		}
		return avroBranchName(schema)
	default:
		return ""
	}
}

func avroFieldAllowsMissing(field map[string]any) bool {
	if _, hasDefault := field["default"]; hasDefault {
		return true
	}
	return avroSchemaAllowsNull(field["type"])
}

func avroMissingFieldValue(field map[string]any) (any, bool) {
	if avroFieldAllowsMissing(field) {
		return nil, true
	}
	if avroSchemaIsArray(field["type"]) {
		return []any{}, true
	}
	return nil, false
}

func avroSchemaIsArray(schema any) bool {
	switch typedSchema := schema.(type) {
	case map[string]any:
		typeName, _ := typedSchema["type"].(string)
		return typeName == "array"
	case string:
		return typedSchema == "array"
	default:
		return false
	}
}

func avroSchemaAllowsNull(schema any) bool {
	union, ok := schema.([]any)
	if !ok {
		return false
	}
	for _, branch := range union {
		if avroBranchName(branch) == "null" {
			return true
		}
	}
	return false
}

func avroConnectFieldName(field map[string]any, fallback string) string {
	if fieldName, ok := field[debeziumAvroConnectFieldKey].(string); ok && fieldName != "" {
		return fieldName
	}
	return fallback
}
