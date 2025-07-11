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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

func (a *BatchEncoder) getValueSchemaCodec(
	ctx context.Context, topic string, tableName *commonType.TableName, tableVersion uint64, input *avroEncodeInput,
) (*goavro.Codec, []byte, error) {
	schemaGen := func() (string, error) {
		schema, err := a.value2AvroSchema(tableName, input)
		if err != nil {
			log.Error("avro: generating value schema failed", zap.Error(err))
			return "", errors.Trace(err)
		}
		return schema, nil
	}

	subject := topicName2SchemaSubjects(topic, valueSchemaSuffix)
	avroCodec, header, err := a.schemaM.GetCachedOrRegister(ctx, subject, tableVersion, schemaGen)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return avroCodec, header, nil
}

func (a *BatchEncoder) getKeySchemaCodec(
	ctx context.Context, topic string, tableName *commonType.TableName, tableVersion uint64, keyColumns *avroEncodeInput,
) (*goavro.Codec, []byte, error) {
	schemaGen := func() (string, error) {
		schema, err := a.key2AvroSchema(tableName, keyColumns)
		if err != nil {
			log.Error("AvroEventBatchEncoder: generating key schema failed", zap.Error(err))
			return "", errors.Trace(err)
		}
		return schema, nil
	}

	subject := topicName2SchemaSubjects(topic, keySchemaSuffix)
	avroCodec, header, err := a.schemaM.GetCachedOrRegister(ctx, subject, tableVersion, schemaGen)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return avroCodec, header, nil
}

func (a *BatchEncoder) encodeKey(ctx context.Context, topic string, e *commonEvent.RowEvent) ([]byte, error) {
	index, colInfos := e.PrimaryKeyColumn()
	// result may be nil if the event has no handle key columns, this may happen in the force replicate mode.
	// todo: disallow force replicate mode if using the avro.
	if len(index) == 0 {
		return nil, nil
	}
	keyColumns := &avroEncodeInput{
		row:            e.GetRows(),
		index:          index,
		colInfos:       colInfos,
		columnselector: e.ColumnSelector,
	}
	avroCodec, header, err := a.getKeySchemaCodec(ctx, topic, &e.TableInfo.TableName, e.TableInfo.UpdateTS(), keyColumns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	native, err := a.columns2AvroData(keyColumns)
	if err != nil {
		log.Error("avro: key converting to native failed", zap.Error(err))
		return nil, errors.Trace(err)
	}

	bin, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		log.Error("avro: key converting to Avro binary failed", zap.Error(err))
		return nil, errors.WrapError(errors.ErrAvroEncodeToBinary, err)
	}

	result := &avroEncodeResult{
		data:   bin,
		header: header,
	}
	data, err := result.toEnvelope()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

func (a *BatchEncoder) encodeValue(ctx context.Context, topic string, e *commonEvent.RowEvent) ([]byte, error) {
	if e.IsDelete() {
		return nil, nil
	}
	length := e.GetRows().Len()
	if length == 0 {
		return nil, nil
	}
	index := make([]int, length)
	for i := 0; i < length; i++ {
		index[i] = i
	}
	input := &avroEncodeInput{
		row:            e.GetRows(),
		colInfos:       e.TableInfo.GetColumns(),
		index:          index,
		columnselector: e.ColumnSelector,
	}
	avroCodec, header, err := a.getValueSchemaCodec(ctx, topic, &e.TableInfo.TableName, e.TableInfo.UpdateTS(), input)
	if err != nil {
		return nil, errors.Trace(err)
	}
	native, err := a.columns2AvroData(input)
	if err != nil {
		log.Error("avro: converting input to native failed", zap.Error(err))
		return nil, errors.Trace(err)
	}
	if a.config.EnableTiDBExtension {
		native = a.nativeValueWithExtension(native, e)
	}

	bin, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		log.Error("avro: converting native to Avro binary failed", zap.Error(err))
		return nil, errors.WrapError(errors.ErrAvroEncodeToBinary, err)
	}

	result := &avroEncodeResult{
		data:   bin,
		header: header,
	}
	data, err := result.toEnvelope()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

func (a *BatchEncoder) nativeValueWithExtension(
	native map[string]interface{},
	e *commonEvent.RowEvent,
) map[string]interface{} {
	native[tidbOp] = getOperation(e)
	native[tidbCommitTs] = int64(e.CommitTs)
	native[tidbPhysicalTime] = oracle.ExtractPhysical(e.CommitTs)

	if a.config.EnableRowChecksum && e.Checksum != nil {
		native[tidbRowLevelChecksum] = strconv.FormatUint(uint64(e.Checksum.Current), 10)
		native[tidbCorrupted] = e.Checksum.Corrupted
		native[tidbChecksumVersion] = e.Checksum.Version
	}
	return native
}

func (a *BatchEncoder) schemaWithExtension(
	top *avroSchemaTop,
) *avroSchemaTop {
	top.Fields = append(top.Fields,
		map[string]interface{}{
			"name":    tidbOp,
			"type":    "string",
			"default": "",
		},
		map[string]interface{}{
			"name":    tidbCommitTs,
			"type":    "long",
			"default": 0,
		},
		map[string]interface{}{
			"name":    tidbPhysicalTime,
			"type":    "long",
			"default": 0,
		},
	)

	if a.config.EnableRowChecksum {
		top.Fields = append(top.Fields,
			map[string]interface{}{
				"name":    tidbRowLevelChecksum,
				"type":    "string",
				"default": "",
			},
			map[string]interface{}{
				"name":    tidbCorrupted,
				"type":    "boolean",
				"default": false,
			},
			map[string]interface{}{
				"name":    tidbChecksumVersion,
				"type":    "int",
				"default": 0,
			})
	}

	return top
}

func (a *BatchEncoder) getDefaultValue(col *timodel.ColumnInfo) (interface{}, error) {
	defaultVal := col.GetDefaultValue()
	if defaultVal == nil {
		return nil, nil
	}
	// defaultValue shoul be string
	// see https://github.com/pingcap/tidb/blob/72b1b7c564c301de33a4bd335a05770c78528db4/pkg/ddl/add_column.go#L791
	v, ok := defaultVal.(string)
	if !ok {
		log.Debug("default value is not string", zap.Any("defaultValue", defaultVal))
		return defaultVal, nil
	}

	switch col.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return nil, errors.WrapError(errors.ErrAvroEncodeFailed, err)
		}
		return int32(n), nil
	case mysql.TypeLong:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, errors.WrapError(errors.ErrAvroEncodeFailed, err)
		}
		if mysql.HasUnsignedFlag(col.GetFlag()) {
			return n, nil
		}
		return int32(n), nil
	case mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(col.GetFlag()) {
			if a.config.AvroBigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeString {
				return v, nil
			}
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, errors.WrapError(errors.ErrAvroEncodeFailed, err)
			}
			return int64(n), nil
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, errors.WrapError(errors.ErrAvroEncodeFailed, err)
		}
		return n, nil

	case mysql.TypeFloat:
		n, err := strconv.ParseFloat(v, 32)
		if err != nil {
			return nil, errors.WrapError(errors.ErrAvroEncodeFailed, err)
		}
		return n, nil
	case mysql.TypeDouble:
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, errors.WrapError(errors.ErrAvroEncodeFailed, err)
		}
		return n, nil
	case mysql.TypeBit:
		return []byte(v), nil
	case mysql.TypeNewDecimal:
		if a.config.AvroDecimalHandlingMode == common.DecimalHandlingModePrecise {
			v, succ := new(big.Rat).SetString(v)
			if !succ {
				return nil, errors.ErrAvroEncodeFailed.GenWithStack(
					"fail to encode Decimal value",
				)
			}
			return v, nil
		}
		// decimalHandlingMode == "string"
		return v, nil
	case mysql.TypeVarchar,
		mysql.TypeString,
		mysql.TypeVarString,
		mysql.TypeTinyBlob,
		mysql.TypeBlob,
		mysql.TypeMediumBlob,
		mysql.TypeLongBlob:
		if mysql.HasBinaryFlag(col.GetFlag()) {
			return []byte(v), nil
		}
		return v, nil
	case mysql.TypeEnum, mysql.TypeSet, mysql.TypeJSON, mysql.TypeTiDBVectorFloat32,
		mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDuration:
		return v, nil
	case mysql.TypeYear:
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			log.Info("avro encoder parse year value failed", zap.String("value", v), zap.Error(err))
			return nil, errors.WrapError(errors.ErrAvroEncodeFailed, err)
		}
		return int32(n), nil
	default:
		log.Error("unknown mysql type", zap.Any("value", defaultVal), zap.Any("mysqlType", col.GetType()))
		return nil, errors.ErrAvroEncodeFailed.GenWithStack("unknown mysql type")
	}
}

func (a *BatchEncoder) columns2AvroSchema(
	tableName *commonType.TableName,
	input *avroEncodeInput,
) (*avroSchemaTop, error) {
	top := &avroSchemaTop{
		Tp:        "record",
		Name:      common.SanitizeName(tableName.Table),
		Namespace: getAvroNamespace(a.namespace, tableName.Schema),
		Fields:    nil,
	}
	for _, info := range input.colInfos {
		if !input.columnselector.Select(info) {
			continue
		}
		avroType, err := a.columnToAvroSchema(info)
		if err != nil {
			return nil, err
		}
		field := make(map[string]interface{})
		field["name"] = common.SanitizeName(info.Name.O)

		defaultValue, err := a.getDefaultValue(info)
		if err != nil {
			log.Error("fail to get default value for avro schema")
			return nil, errors.Trace(err)
		}
		// goavro doesn't support set default value for logical type
		// https://github.com/linkedin/goavro/issues/202
		if _, ok := avroType.(avroLogicalTypeSchema); ok {
			if !mysql.HasNotNullFlag(info.GetFlag()) {
				field["type"] = []interface{}{"null", avroType}
				field["default"] = nil
			} else {
				field["type"] = avroType
			}
		} else {
			if !mysql.HasNotNullFlag(info.GetFlag()) {
				// https://stackoverflow.com/questions/22938124/avro-field-default-values
				if defaultValue == nil {
					field["type"] = []interface{}{"null", avroType}
				} else {
					field["type"] = []interface{}{avroType, "null"}
				}
				field["default"] = defaultValue
			} else {
				field["type"] = avroType
				if defaultValue != nil {
					field["default"] = defaultValue
				}
			}
		}
		top.Fields = append(top.Fields, field)
	}
	return top, nil
}

func (a *BatchEncoder) value2AvroSchema(
	tableName *commonType.TableName,
	input *avroEncodeInput,
) (string, error) {
	if a.config.EnableRowChecksum {
		sort.Sort(input)
	}

	top, err := a.columns2AvroSchema(tableName, input)
	if err != nil {
		return "", err
	}

	if a.config.EnableTiDBExtension {
		top = a.schemaWithExtension(top)
	}

	str, err := json.Marshal(top)
	if err != nil {
		return "", errors.WrapError(errors.ErrAvroMarshalFailed, err)
	}
	log.Info("avro: row to schema",
		zap.ByteString("schema", str),
		zap.Bool("enableTiDBExtension", a.config.EnableRowChecksum),
		zap.Bool("enableRowLevelChecksum", a.config.EnableRowChecksum))
	return string(str), nil
}

func (a *BatchEncoder) key2AvroSchema(
	tableName *commonType.TableName,
	keyColumns *avroEncodeInput,
) (string, error) {
	top, err := a.columns2AvroSchema(tableName, keyColumns)
	if err != nil {
		return "", err
	}

	str, err := json.Marshal(top)
	if err != nil {
		return "", errors.WrapError(errors.ErrAvroMarshalFailed, err)
	}
	log.Info("avro: key to schema", zap.ByteString("schema", str))
	return string(str), nil
}

func (a *BatchEncoder) columns2AvroData(
	input *avroEncodeInput,
) (map[string]interface{}, error) {
	ret := make(map[string]interface{}, len(input.colInfos))
	for i, col := range input.colInfos {
		if col == nil || !input.columnselector.Select(col) {
			continue
		}
		data, str, err := a.columnToAvroData(input.row, input.index[i], col)
		if err != nil {
			return nil, err
		}

		// https: //pkg.go.dev/github.com/linkedin/goavro/v2#Union
		if !mysql.HasNotNullFlag(col.GetFlag()) {
			ret[common.SanitizeName(col.Name.O)] = goavro.Union(str, data)
		} else {
			ret[common.SanitizeName(col.Name.O)] = data
		}
	}

	log.Debug("rowToAvroData", zap.Any("data", ret))
	return ret, nil
}

func (a *BatchEncoder) columnToAvroSchema(col *timodel.ColumnInfo) (interface{}, error) {
	tt := getTiDBTypeFromColumn(col)
	switch col.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		// BOOL/TINYINT/SMALLINT/MEDIUMINT
		return avroSchema{
			Type:       "int",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeLong: // INT
		if mysql.HasUnsignedFlag(col.GetFlag()) {
			return avroSchema{
				Type:       "long",
				Parameters: map[string]string{tidbType: tt},
			}, nil
		}
		return avroSchema{
			Type:       "int",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeLonglong: // BIGINT
		t := "long"
		if mysql.HasUnsignedFlag(col.GetFlag()) &&
			a.config.AvroBigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeString {
			t = "string"
		}
		return avroSchema{
			Type:       t,
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeFloat:
		return avroSchema{
			Type:       "float",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeDouble:
		return avroSchema{
			Type:       "double",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeBit:
		displayFlen := col.GetFlen()
		if displayFlen == -1 {
			displayFlen, _ = mysql.GetDefaultFieldLengthAndDecimal(col.GetType())
		}
		return avroSchema{
			Type: "bytes",
			Parameters: map[string]string{
				tidbType: tt,
				"length": strconv.Itoa(displayFlen),
			},
		}, nil
	case mysql.TypeNewDecimal:
		if a.config.AvroDecimalHandlingMode == common.DecimalHandlingModePrecise {
			defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(col.GetType())
			displayFlen, displayDecimal := col.GetFlen(), col.GetDecimal()
			// length not specified, set it to system type default
			if displayFlen == -1 {
				displayFlen = defaultFlen
			}
			if displayDecimal == -1 {
				displayDecimal = defaultDecimal
			}
			return avroLogicalTypeSchema{
				avroSchema: avroSchema{
					Type:       "bytes",
					Parameters: map[string]string{tidbType: tt},
				},
				LogicalType: "decimal",
				Precision:   displayFlen,
				Scale:       displayDecimal,
			}, nil
		}
		// decimalHandlingMode == string
		return avroSchema{
			Type:       "string",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	// TINYTEXT/MEDIUMTEXT/TEXT/LONGTEXT/CHAR/VARCHAR
	// TINYBLOB/MEDIUMBLOB/BLOB/LONGBLOB/BINARY/VARBINARY
	case mysql.TypeVarchar,
		mysql.TypeString,
		mysql.TypeVarString,
		mysql.TypeTinyBlob,
		mysql.TypeMediumBlob,
		mysql.TypeLongBlob,
		mysql.TypeBlob:
		t := "string"
		if mysql.HasBinaryFlag(col.GetFlag()) {
			t = "bytes"
		}
		return avroSchema{
			Type:       t,
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeEnum, mysql.TypeSet:
		es := make([]string, 0, len(col.GetElems()))
		for _, e := range col.GetElems() {
			e = common.EscapeEnumAndSetOptions(e)
			es = append(es, e)
		}
		return avroSchema{
			Type: "string",
			Parameters: map[string]string{
				tidbType:  tt,
				"allowed": strings.Join(es, ","),
			},
		}, nil
	case mysql.TypeJSON:
		return avroSchema{
			Type:       "string",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDuration:
		return avroSchema{
			Type:       "string",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeYear:
		return avroSchema{
			Type:       "int",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	case mysql.TypeTiDBVectorFloat32:
		return avroSchema{
			Type:       "string",
			Parameters: map[string]string{tidbType: tt},
		}, nil
	default:
		log.Error("unknown mysql type", zap.Any("mysqlType", col.GetType()))
		return nil, errors.ErrAvroEncodeFailed.GenWithStack("unknown mysql type")
	}
}

func (a *BatchEncoder) columnToAvroData(
	row *chunk.Row,
	idx int,
	col *timodel.ColumnInfo,
) (interface{}, string, error) {
	if row.IsNull(idx) {
		return nil, "null", nil
	}
	d := row.GetDatum(idx, &col.FieldType)
	switch col.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		if mysql.HasUnsignedFlag(col.GetFlag()) {
			return int32(d.GetUint64()), "int", nil
		}
		return int32(d.GetInt64()), "int", nil
	case mysql.TypeLong:
		if mysql.HasUnsignedFlag(col.GetFlag()) {
			return int64(d.GetUint64()), "long", nil
		}
		return int32(d.GetInt64()), "int", nil
	case mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(col.GetFlag()) {
			if a.config.AvroBigintUnsignedHandlingMode == common.BigintUnsignedHandlingModeLong {
				return int64(d.GetUint64()), "long", nil
			}
			// bigintUnsignedHandlingMode == "string"
			return strconv.FormatUint(d.GetUint64(), 10), "string", nil
		}
		return d.GetInt64(), "long", nil
	case mysql.TypeFloat:
		return d.GetFloat32(), "float", nil
	case mysql.TypeDouble:
		return d.GetFloat64(), "double", nil
	case mysql.TypeBit:
		return []byte(d.GetMysqlBit()), "bytes", nil
	case mysql.TypeNewDecimal:
		if a.config.AvroDecimalHandlingMode == common.DecimalHandlingModePrecise {
			v, succ := new(big.Rat).SetString(d.GetMysqlDecimal().String())
			if !succ {
				return nil, "", errors.ErrAvroEncodeFailed.GenWithStack(
					"fail to encode Decimal value",
				)
			}
			return v, "bytes.decimal", nil
		}
		// decimalHandlingMode == "string"
		return d.GetMysqlDecimal().String(), "string", nil
	case mysql.TypeVarchar,
		mysql.TypeString,
		mysql.TypeVarString,
		mysql.TypeTinyBlob,
		mysql.TypeBlob,
		mysql.TypeMediumBlob,
		mysql.TypeLongBlob:
		if mysql.HasBinaryFlag(col.GetFlag()) {
			return d.GetBytes(), "bytes", nil
		}
		return d.GetString(), "string", nil
	case mysql.TypeEnum:
		elements := col.GetElems()
		number := d.GetMysqlEnum().Value
		enumVar, err := types.ParseEnumValue(elements, number)
		if err != nil {
			log.Info("avro encoder parse enum value failed", zap.Strings("elements", elements), zap.Uint64("number", number))
			return nil, "", errors.WrapError(errors.ErrAvroEncodeFailed, err)
		}
		return enumVar.Name, "string", nil
	case mysql.TypeSet:
		elements := col.GetElems()
		number := d.GetMysqlSet().Value
		setVar, err := types.ParseSetValue(elements, number)
		if err != nil {
			log.Info("avro encoder parse set value failed",
				zap.Strings("elements", elements), zap.Uint64("number", number), zap.Error(err))
			return nil, "", errors.WrapError(errors.ErrAvroEncodeFailed, err)
		}
		return setVar.Name, "string", nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDuration,
		mysql.TypeJSON,
		mysql.TypeTiDBVectorFloat32:
		return fmt.Sprintf("%v", d.GetValue()), "string", nil
	case mysql.TypeYear:
		return int32(d.GetInt64()), "int", nil
	default:
		log.Error("unknown mysql type", zap.Any("value", d.GetValue()), zap.Any("mysqlType", col.GetType()))
		return nil, "", errors.ErrAvroEncodeFailed.GenWithStack("unknown mysql type")
	}
}

func (a *BatchEncoder) Clean() {}

type avroEncodeResult struct {
	data []byte
	// header is the message header, it will be encoder into the head
	// of every single avro message. Note: Confluent schema registry and
	// Aws Glue schema registry have different header format.
	header []byte
}

func (r *avroEncodeResult) toEnvelope() ([]byte, error) {
	buf := new(bytes.Buffer)
	data := []interface{}{r.header, r.data}
	for _, v := range data {
		err := binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			return nil, errors.WrapError(errors.ErrAvroToEnvelopeError, err)
		}
	}
	return buf.Bytes(), nil
}

// SetupEncoderAndSchemaRegistry4Testing start a local schema registry for testing.
func SetupEncoderAndSchemaRegistry4Testing(
	ctx context.Context,
	config *common.Config,
) (*BatchEncoder, error) {
	startHTTPInterceptForTestingRegistry()
	schemaM, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &BatchEncoder{
		namespace: commonType.DefaultNamespace,
		schemaM:   schemaM,
		result:    make([]*common.Message, 0, 1),
		config:    config,
	}, nil
}

// TeardownEncoderAndSchemaRegistry4Testing stop the local schema registry for testing.
func TeardownEncoderAndSchemaRegistry4Testing() {
	stopHTTPInterceptForTestingRegistry()
}
