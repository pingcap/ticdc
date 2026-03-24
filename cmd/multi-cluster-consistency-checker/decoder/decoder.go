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
	"context"
	"encoding/hex"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
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
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/encoding/charmap"
)

const tidbWaterMarkType = "TIDB_WATERMARK"

var fastJSON = jsoniter.ConfigCompatibleWithStandardLibrary

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
	// OriginTs mirrors _tidb.originTs
	OriginTs uint64 `json:"originTs,omitempty"`
	Checksum []byte `json:"checksum,omitempty"`
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
	// Checksum is from _tidb.checksum when present; nil means unset (legacy messages).
	Checksum []byte
}

// EqualReplicatedRecord compares a local row to a replicated row.
// Checksum: both nil or both empty slice match; if exactly one side has a non-empty checksum, they differ;
// if both are non-empty, bytes must match. Two non-nil empty slices also match via bytes.Equal.
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
	if !bytes.Equal(r.Checksum, replicatedRecord.Checksum) {
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

func tryDecompressData(data []byte, config *common.Config) ([]byte, error) {
	data, err := common.Decompress(config.LargeMessageHandle.LargeMessageHandleCompression, data)
	if err != nil {
		log.Error("decompress data failed",
			zap.String("compression", config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Any("data", data),
			zap.Error(err))
		return nil, errors.Annotatef(err, "decompress data failed")
	}
	return data, nil
}

func newWorkers(encodedDataCh chan []byte, columnFieldTypes map[string]*ptypes.FieldType, collector func([]*Record)) *errgroup.Group {
	eg, _ := errgroup.WithContext(context.Background())
	workers := max(runtime.GOMAXPROCS(0)+1, 2)
	for range workers {
		eg.Go(func() error {
			records := make([]*Record, 0)
			for encodedData := range encodedDataCh {
				record, err := tryDecodeRecord(encodedData, columnFieldTypes)
				if err != nil {
					return errors.Trace(err)
				}
				if record != nil {
					records = append(records, record)
				}
			}
			collector(records)
			return nil
		})
	}
	return eg
}

func Decode(data []byte, columnFieldTypes map[string]*ptypes.FieldType) ([]*Record, error) {
	config := defaultCanalJSONCodecConfig()
	data, err := tryDecompressData(data, config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var lk sync.Mutex
	totalRecords := make([]*Record, 0)
	encodedDataCh := make(chan []byte, 128)
	eg := newWorkers(encodedDataCh, columnFieldTypes, func(records []*Record) {
		lk.Lock()
		totalRecords = append(totalRecords, records...)
		lk.Unlock()
	})
	for len(data) != 0 {
		encodedData, restData := CRLFNext(data)
		data = restData
		if len(encodedData) == 0 {
			continue
		}

		encodedDataCh <- encodedData
	}

	close(encodedDataCh)
	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}

	return totalRecords, nil
}

func CRLFNext(data []byte) ([]byte, []byte) {
	idx := bytes.IndexByte(data, '\n')
	if idx >= 0 {
		encodedData := data[:idx]
		restData := data[idx+1:]
		if n := len(encodedData); n > 0 && encodedData[n-1] == '\r' {
			encodedData = encodedData[:n-1]
		}
		return encodedData, restData
	}
	return data, nil
}

func tryDecodeRecord(encodedData []byte, columnFieldTypes map[string]*ptypes.FieldType) (*Record, error) {
	msg := &canalValueDecoderJSONMessageWithTiDBExtension{}
	if err := fastJSON.Unmarshal(encodedData, msg); err != nil {
		log.Error("canal json decoder unmarshal data failed",
			zap.Error(err), zap.ByteString("data", encodedData))
		return nil, nil
	}
	if msg.messageType() != common.MessageTypeRow || len(msg.Data) == 0 {
		log.Error("message type is not row", zap.Any("msg", msg))
		return nil, nil
	}

	return decodeRecord(msg, columnFieldTypes)
}

func decodeRecord(msg *canalValueDecoderJSONMessageWithTiDBExtension, columnFieldTypes map[string]*ptypes.FieldType) (*Record, error) {
	var pkStrBuilder strings.Builder
	pkStrBuilder.WriteString("[")
	pkValues := make([]tiTypes.Datum, 0, len(msg.PkNames))
	pkMap := make(map[string]any, len(msg.PkNames))
	slices.Sort(msg.PkNames)
	for i, pkName := range msg.PkNames {
		columnValue, ok := msg.Data[0][pkName]
		if !ok {
			log.Error("column value not found", zap.String("pkName", pkName), zap.Any("msg", msg))
			return nil, errors.Errorf("column value of column %s not found", pkName)
		}
		if i > 0 {
			pkStrBuilder.WriteString(", ")
		}
		fmt.Fprintf(&pkStrBuilder, "%s: %v", pkName, columnValue)
		pkMap[pkName] = columnValue
		ft := getColumnFieldType(msg, pkName, columnFieldTypes)
		if ft == nil {
			log.Error("field type not found", zap.String("pkName", pkName), zap.Any("msg", msg))
			return nil, errors.Errorf("field type of column %s not found", pkName)
		}
		datum, err := safeValueToDatum(columnValue, ft)
		if err != nil {
			log.Error("failed to convert primary key column value",
				zap.String("pkName", pkName),
				zap.Any("columnValue", columnValue),
				zap.Error(err))
			return nil, errors.Annotatef(err, "failed to convert primary key column %s", pkName)
		}
		if datum.IsNull() {
			log.Error("column value is null", zap.String("pkName", pkName), zap.Any("msg", msg))
			return nil, errors.Errorf("column value of column %s is null", pkName)
		}
		pkValues = append(pkValues, *datum)
		delete(msg.Data[0], pkName)
	}
	pkStrBuilder.WriteString("]")
	pkEncoded, err := codec.EncodeKey(time.UTC, nil, pkValues...)
	if err != nil {
		return nil, errors.Annotate(err, "failed to encode primary key")
	}
	pk := hex.EncodeToString(pkEncoded)
	originTs := msg.TiDBCommitTsExtension.OriginTs
	columnValues := make(map[string]any)
	for columnName, columnValue := range msg.Data[0] {
		if columnName == event.OriginTsColumn {
			if columnValue != nil && originTs == 0 {
				originTs, err = strconv.ParseUint(columnValue.(string), 10, 64)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		} else {
			columnValues[columnName] = columnValue
		}
	}
	commitTs := msg.TiDBCommitTsExtension.CommitTs
	return &Record{
		Pk:           types.PkType(pk),
		PkStr:        pkStrBuilder.String(),
		PkMap:        pkMap,
		ColumnValues: columnValues,
		CdcVersion: types.CdcVersion{
			CommitTs: commitTs,
			OriginTs: originTs,
		},
		Checksum: msg.TiDBCommitTsExtension.Checksum,
	}, nil
}

func safeValueToDatum(value any, ft *ptypes.FieldType) (datum *tiTypes.Datum, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("value to datum conversion panic: %v", r)
			datum = nil
		}
	}()
	return valueToDatum(value, ft), nil
}

// getColumnFieldType returns the FieldType for a column.
// It first looks up from the tableDefinition-based columnFieldTypes map,
// then falls back to parsing the MySQLType string from the canal-json message.
func getColumnFieldType(
	msg *canalValueDecoderJSONMessageWithTiDBExtension,
	columnName string,
	columnFieldTypes map[string]*ptypes.FieldType,
) *ptypes.FieldType {
	if columnFieldTypes != nil {
		if ft, ok := columnFieldTypes[columnName]; ok {
			return ft
		}
	}
	// Fallback: parse from MySQLType in the canal-json message
	mysqlType, ok := msg.MySQLType[columnName]
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
