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

package decoder

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/charmap"
)

const tidbWaterMarkType = "TIDB_WATERMARK"

type canalValueDecoderJSONMessage struct {
	PkNames   []string          `json:"pkNames"`
	IsDDL     bool              `json:"isDdl"`
	EventType string            `json:"type"`
	MySQLType map[string]string `json:"mysqlType"`
	Data      []map[string]any  `json:"data"`
}

func (c *canalValueDecoderJSONMessage) messageType() common.MessageType {
	if c.IsDDL {
		return common.MessageTypeDDL
	}

	if c.EventType == tidbWaterMarkType {
		return common.MessageTypeResolved
	}

	return common.MessageTypeRow
}

type TiDBCommitTsExtension struct {
	CommitTs uint64 `json:"commitTs"`
}

type canalValueDecoderJSONMessageWithTiDBExtension struct {
	canalValueDecoderJSONMessage

	TiDBCommitTsExtension *TiDBCommitTsExtension `json:"_tidb"`
}

func defaultCanalJSONCodecConfig() *common.Config {
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	// Always enable tidb extension for canal-json protocol
	// because we need to get the commit ts from the extension field.
	codecConfig.EnableTiDBExtension = true
	codecConfig.Terminator = config.CRLF
	return codecConfig
}

type Record struct {
	types.CdcVersion
	Pk           types.PkType
	PkStr        string
	PkMap        map[string]any
	ColumnValues map[string]any
}

func (r *Record) EqualReplicatedRecord(replicatedRecord *Record) bool {
	if replicatedRecord == nil {
		return false
	}
	if r.CommitTs != replicatedRecord.OriginTs {
		return false
	}
	if r.Pk != replicatedRecord.Pk {
		return false
	}
	if len(r.ColumnValues) != len(replicatedRecord.ColumnValues) {
		return false
	}
	for columnName, columnValue := range r.ColumnValues {
		replicatedColumnValue, ok := replicatedRecord.ColumnValues[columnName]
		if !ok {
			return false
		}
		// NOTE: This comparison is safe because ColumnValues only holds comparable
		// types (nil, string, int64, float64, etc.) as produced by the canal-json
		// decoder. If a non-comparable type (e.g. []byte or map) were ever stored,
		// the != operator would panic at runtime.
		if columnValue != replicatedColumnValue {
			return false
		}
	}
	return true
}

type columnValueDecoder struct {
	data   []byte
	config *common.Config

	msg              *canalValueDecoderJSONMessageWithTiDBExtension
	columnFieldTypes map[string]*ptypes.FieldType
}

func newColumnValueDecoder(data []byte) (*columnValueDecoder, error) {
	config := defaultCanalJSONCodecConfig()
	data, err := common.Decompress(config.LargeMessageHandle.LargeMessageHandleCompression, data)
	if err != nil {
		log.Error("decompress data failed",
			zap.String("compression", config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Any("data", data),
			zap.Error(err))
		return nil, errors.Annotatef(err, "decompress data failed")
	}
	return &columnValueDecoder{
		config: config,
		data:   data,
	}, nil
}

func Decode(data []byte, columnFieldTypes map[string]*ptypes.FieldType) ([]*Record, error) {
	decoder, err := newColumnValueDecoder(data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	decoder.columnFieldTypes = columnFieldTypes

	records := make([]*Record, 0)
	for {
		msgType, hasNext := decoder.tryNext()
		if !hasNext {
			break
		}
		if msgType == common.MessageTypeRow {
			record, err := decoder.decodeNext()
			if err != nil {
				return nil, errors.Trace(err)
			}
			records = append(records, record)
		}
	}

	return records, nil
}

func (d *columnValueDecoder) tryNext() (common.MessageType, bool) {
	if d.data == nil {
		return common.MessageTypeUnknown, false
	}
	var (
		msg         = &canalValueDecoderJSONMessageWithTiDBExtension{}
		encodedData []byte
	)

	idx := bytes.IndexAny(d.data, d.config.Terminator)
	if idx >= 0 {
		encodedData = d.data[:idx]
		d.data = d.data[idx+len(d.config.Terminator):]
	} else {
		encodedData = d.data
		d.data = nil
	}

	if len(encodedData) == 0 {
		return common.MessageTypeUnknown, false
	}

	if err := json.Unmarshal(encodedData, msg); err != nil {
		log.Error("canal-json decoder unmarshal data failed",
			zap.Error(err), zap.ByteString("data", encodedData))
		d.msg = nil
		return common.MessageTypeUnknown, true
	}
	d.msg = msg
	return d.msg.messageType(), true
}

func (d *columnValueDecoder) decodeNext() (*Record, error) {
	if d.msg == nil || len(d.msg.Data) == 0 || d.msg.messageType() != common.MessageTypeRow {
		log.Error("invalid message", zap.Any("msg", d.msg))
		return nil, errors.New("invalid message")
	}

	var pkStrBuilder strings.Builder
	pkStrBuilder.WriteString("[")
	pkValues := make([]tiTypes.Datum, 0, len(d.msg.PkNames))
	pkMap := make(map[string]any, len(d.msg.PkNames))
	slices.Sort(d.msg.PkNames)
	for i, pkName := range d.msg.PkNames {
		columnValue, ok := d.msg.Data[0][pkName]
		if !ok {
			log.Error("column value not found", zap.String("pkName", pkName), zap.Any("msg", d.msg))
			return nil, errors.Errorf("column value of column %s not found", pkName)
		}
		if i > 0 {
			pkStrBuilder.WriteString(", ")
		}
		fmt.Fprintf(&pkStrBuilder, "%s: %v", pkName, columnValue)
		pkMap[pkName] = columnValue
		ft := d.getColumnFieldType(pkName)
		if ft == nil {
			log.Error("field type not found", zap.String("pkName", pkName), zap.Any("msg", d.msg))
			return nil, errors.Errorf("field type of column %s not found", pkName)
		}
		datum := valueToDatum(columnValue, ft)
		if datum.IsNull() {
			log.Error("column value is null", zap.String("pkName", pkName), zap.Any("msg", d.msg))
			return nil, errors.Errorf("column value of column %s is null", pkName)
		}
		pkValues = append(pkValues, *datum)
		delete(d.msg.Data[0], pkName)
	}
	pkStrBuilder.WriteString("]")
	pkEncoded, err := codec.EncodeKey(time.UTC, nil, pkValues...)
	if err != nil {
		return nil, errors.Annotate(err, "failed to encode primary key")
	}
	pk := hex.EncodeToString(pkEncoded)
	originTs := uint64(0)
	columnValues := make(map[string]any)
	for columnName, columnValue := range d.msg.Data[0] {
		if columnName == event.OriginTsColumn {
			if columnValue != nil {
				originTs, err = strconv.ParseUint(columnValue.(string), 10, 64)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		} else {
			columnValues[columnName] = columnValue
		}
	}
	commitTs := d.msg.TiDBCommitTsExtension.CommitTs
	d.msg = nil
	return &Record{
		Pk:           types.PkType(pk),
		PkStr:        pkStrBuilder.String(),
		PkMap:        pkMap,
		ColumnValues: columnValues,
		CdcVersion: types.CdcVersion{
			CommitTs: commitTs,
			OriginTs: originTs,
		},
	}, nil
}

// getColumnFieldType returns the FieldType for a column.
// It first looks up from the tableDefinition-based columnFieldTypes map,
// then falls back to parsing the MySQLType string from the canal-json message.
func (d *columnValueDecoder) getColumnFieldType(columnName string) *ptypes.FieldType {
	if d.columnFieldTypes != nil {
		if ft, ok := d.columnFieldTypes[columnName]; ok {
			return ft
		}
	}
	// Fallback: parse from MySQLType in the canal-json message
	mysqlType, ok := d.msg.MySQLType[columnName]
	if !ok {
		return nil
	}
	return newPKColumnFieldTypeFromMysqlType(mysqlType)
}

func newPKColumnFieldTypeFromMysqlType(mysqlType string) *ptypes.FieldType {
	tp := ptypes.NewFieldType(common.ExtractBasicMySQLType(mysqlType))
	if common.IsBinaryMySQLType(mysqlType) {
		tp.AddFlag(mysql.BinaryFlag)
		tp.SetCharset("binary")
		tp.SetCollate("binary")
	}
	if strings.HasPrefix(mysqlType, "char") ||
		strings.HasPrefix(mysqlType, "varchar") ||
		strings.Contains(mysqlType, "text") ||
		strings.Contains(mysqlType, "enum") ||
		strings.Contains(mysqlType, "set") {
		tp.SetCharset("utf8mb4")
		tp.SetCollate("utf8mb4_bin")
	}

	if common.IsUnsignedMySQLType(mysqlType) {
		tp.AddFlag(mysql.UnsignedFlag)
	}

	flen, decimal := common.ExtractFlenDecimal(mysqlType, tp.GetType())
	tp.SetFlen(flen)
	tp.SetDecimal(decimal)
	switch tp.GetType() {
	case mysql.TypeEnum, mysql.TypeSet:
		tp.SetElems(common.ExtractElements(mysqlType))
	case mysql.TypeDuration:
		decimal = common.ExtractDecimal(mysqlType)
		tp.SetDecimal(decimal)
	default:
	}
	return tp
}

func valueToDatum(value any, ft *ptypes.FieldType) *tiTypes.Datum {
	d := &tiTypes.Datum{}
	if value == nil {
		d.SetNull()
		return d
	}
	rawValue, ok := value.(string)
	if !ok {
		log.Panic("canal-json encoded message should have type in `string`")
	}
	if mysql.HasBinaryFlag(ft.GetFlag()) {
		// when encoding the `JavaSQLTypeBLOB`, use `IS08859_1` decoder, now reverse it back.
		result, err := charmap.ISO8859_1.NewEncoder().String(rawValue)
		if err != nil {
			log.Panic("invalid column value, please report a bug", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		rawValue = result
	}

	switch ft.GetType() {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			data, err := strconv.ParseUint(rawValue, 10, 64)
			if err != nil {
				log.Panic("invalid column value for unsigned integer", zap.Any("rawValue", rawValue), zap.Error(err))
			}
			d.SetUint64(data)
			return d
		}
		data, err := strconv.ParseInt(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for integer", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		d.SetInt64(data)
		return d
	case mysql.TypeYear:
		data, err := strconv.ParseInt(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for year", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		d.SetInt64(data)
		return d
	case mysql.TypeFloat:
		data, err := strconv.ParseFloat(rawValue, 32)
		if err != nil {
			log.Panic("invalid column value for float", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		d.SetFloat32(float32(data))
		return d
	case mysql.TypeDouble:
		data, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			log.Panic("invalid column value for double", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		d.SetFloat64(data)
		return d
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetString(rawValue, ft.GetCollate())
		return d
	case mysql.TypeNewDecimal:
		data := new(tiTypes.MyDecimal)
		err := data.FromString([]byte(rawValue))
		if err != nil {
			log.Panic("invalid column value for decimal", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		d.SetMysqlDecimal(data)
		d.SetLength(ft.GetFlen())
		if ft.GetDecimal() == tiTypes.UnspecifiedLength {
			d.SetFrac(int(data.GetDigitsFrac()))
		} else {
			d.SetFrac(ft.GetDecimal())
		}
		return d
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		data, err := tiTypes.ParseTime(tiTypes.DefaultStmtNoWarningContext, rawValue, ft.GetType(), ft.GetDecimal())
		if err != nil {
			log.Panic("invalid column value for time", zap.Any("rawValue", rawValue),
				zap.Int("flen", ft.GetFlen()), zap.Int("decimal", ft.GetDecimal()),
				zap.Error(err))
		}
		d.SetMysqlTime(data)
		return d
	case mysql.TypeDuration:
		data, _, err := tiTypes.ParseDuration(tiTypes.DefaultStmtNoWarningContext, rawValue, ft.GetDecimal())
		if err != nil {
			log.Panic("invalid column value for duration", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		d.SetMysqlDuration(data)
		return d
	case mysql.TypeEnum:
		enumValue, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for enum", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		d.SetMysqlEnum(tiTypes.Enum{
			Name:  "",
			Value: enumValue,
		}, ft.GetCollate())
		return d
	case mysql.TypeSet:
		setValue, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for set", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		d.SetMysqlSet(tiTypes.Set{
			Name:  "",
			Value: setValue,
		}, ft.GetCollate())
		return d
	case mysql.TypeBit:
		data, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for bit", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		byteSize := (ft.GetFlen() + 7) >> 3
		d.SetMysqlBit(tiTypes.NewBinaryLiteralFromUint(data, byteSize))
		return d
	case mysql.TypeJSON:
		data, err := tiTypes.ParseBinaryJSONFromString(rawValue)
		if err != nil {
			log.Panic("invalid column value for json", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		d.SetMysqlJSON(data)
		return d
	case mysql.TypeTiDBVectorFloat32:
		data, err := tiTypes.ParseVectorFloat32(rawValue)
		if err != nil {
			log.Panic("cannot parse vector32 value from string", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		d.SetVectorFloat32(data)
		return d
	}
	return d
}
