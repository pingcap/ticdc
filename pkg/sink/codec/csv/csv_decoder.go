// Copyright 2022 PingCAP, Inc.
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

package csv

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	lconfig "github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/lightning/worker"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
)

const defaultIOConcurrency = 1

type batchDecoder struct {
	codecConfig *common.Config
	parser      *mydump.CSVParser
	data        []byte
	msg         *csvMessage
	tableInfo   *commonType.TableInfo
	closed      bool
}

// NewBatchDecoder creates a new BatchDecoder
func NewBatchDecoder(ctx context.Context,
	codecConfig *common.Config,
	tableInfo *commonType.TableInfo,
	value []byte,
) (common.RowEventDecoder, error) {
	var backslashEscape bool

	// if quote is not set in config, we should unespace backslash
	// when parsing csv columns.
	if len(codecConfig.Quote) == 0 {
		backslashEscape = true
	}
	cfg := &lconfig.CSVConfig{
		Separator:       codecConfig.Delimiter,
		Delimiter:       codecConfig.Quote,
		Terminator:      codecConfig.Terminator,
		Null:            []string{codecConfig.NullString},
		BackslashEscape: backslashEscape,
	}
	csvParser, err := mydump.NewCSVParser(ctx, cfg,
		mydump.NewStringReader(string(value)),
		int64(lconfig.ReadBlockSize),
		worker.NewPool(ctx, defaultIOConcurrency, "io"), false, nil)
	if err != nil {
		return nil, err
	}
	return &batchDecoder{
		codecConfig: codecConfig,
		tableInfo:   tableInfo,
		data:        value,
		msg:         newCSVMessage(codecConfig),
		parser:      csvParser,
	}, nil
}

// AddKeyValue implements the RowEventDecoder interface.
func (b *batchDecoder) AddKeyValue(_, _ []byte) error {
	return nil
}

// HasNext implements the RowEventDecoder interface.
func (b *batchDecoder) HasNext() (common.MessageType, bool, error) {
	err := b.parser.ReadRow()
	if err != nil {
		b.closed = true
		if errors.Cause(err) == io.EOF {
			return common.MessageTypeUnknown, false, nil
		}
		return common.MessageTypeUnknown, false, err
	}

	row := b.parser.LastRow()
	if err = b.msg.decode(row.Row); err != nil {
		return common.MessageTypeUnknown, false, errors.Trace(err)
	}

	return common.MessageTypeRow, true, nil
}

// NextResolvedEvent implements the RowEventDecoder interface.
func (b *batchDecoder) NextResolvedEvent() (uint64, error) {
	return 0, nil
}

// NextDMLEvent implements the RowEventDecoder interface.
func (b *batchDecoder) NextDMLEvent() (*commonEvent.DMLEvent, error) {
	if b.closed {
		return nil, errors.WrapError(errors.ErrCSVDecodeFailed, errors.New("no csv row can be found"))
	}

	e, err := csvMsg2RowChangedEvent(b.codecConfig, b.msg, b.tableInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return e, nil
}

// NextDDLEvent implements the RowEventDecoder interface.
func (b *batchDecoder) NextDDLEvent() (*commonEvent.DDLEvent, error) {
	return nil, nil
}

func fromCsvValToColValue(csvConfig *common.Config, csvVal any, ft types.FieldType) (any, error) {
	str, ok := csvVal.(string)
	if !ok {
		return csvVal, nil
	}
	var (
		val any
		err error
	)
	switch ft.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if ft.GetCharset() == charset.CharsetBin {
			switch csvConfig.BinaryEncodingMethod {
			case config.BinaryEncodingBase64:
				return base64.StdEncoding.DecodeString(str)
			case config.BinaryEncodingHex:
				return hex.DecodeString(str)
			default:
				return nil, errors.WrapError(errors.ErrCSVEncodeFailed,
					errors.Errorf("unsupported binary encoding method %s",
						csvConfig.BinaryEncodingMethod))
			}
		}
		val = []byte(str)
	case mysql.TypeNewDecimal:
		val = types.NewDecFromStringForTest(str)
	case mysql.TypeFloat:
		val, err = strconv.ParseFloat(str, 32)
	case mysql.TypeDouble:
		val, err = strconv.ParseFloat(str, 64)
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			val, err = strconv.ParseUint(str, 10, 64)
		} else {
			val, err = strconv.ParseInt(str, 10, 64)
		}
	case mysql.TypeYear:
		val, err = strconv.ParseInt(str, 10, 64)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		val, err = types.ParseTime(types.DefaultStmtNoWarningContext, str, ft.GetType(), ft.GetDecimal())
	case mysql.TypeDuration:
		val, _, err = types.ParseDuration(types.DefaultStmtNoWarningContext, str, ft.GetDecimal())
	case mysql.TypeBit:
		val, err = types.NewBitLiteral(str)
	case mysql.TypeSet:
		val, err = types.ParseSet(ft.GetElems(), str, ft.GetCollate())
	case mysql.TypeEnum:
		val, err = types.ParseEnum(ft.GetElems(), str, ft.GetCollate())
	case mysql.TypeJSON:
		val, err = types.ParseBinaryJSONFromString(str)
	case mysql.TypeTiDBVectorFloat32:
		val, err = types.ParseVectorFloat32(str)
	default:
		return str, nil
	}
	return val, err
}

func csvMsg2RowChangedEvent(csvConfig *common.Config, csvMsg *csvMessage, tableInfo *commonType.TableInfo) (*commonEvent.DMLEvent, error) {
	var err error
	if len(csvMsg.columns) != len(tableInfo.GetColumns()) {
		return nil, errors.WrapError(errors.ErrCSVDecodeFailed,
			fmt.Errorf("the column length of csv message %d doesn't equal to that of tableInfo %d",
				len(csvMsg.columns), len(tableInfo.GetColumns())))
	}

	e := new(commonEvent.DMLEvent)
	e.CommitTs = csvMsg.commitTs
	e.TableInfo = tableInfo

	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	columns := tableInfo.GetColumns()
	data, err := formatAllColumnsValue(csvConfig, csvMsg.columns, columns)
	if err != nil {
		return nil, err
	}
	common.AppendRow2Chunk(data, columns, chk)
	return e, nil
}

func formatAllColumnsValue(csvConfig *common.Config, csvCols []any, ticols []*model.ColumnInfo) (map[string]any, error) {
	data := make(map[string]interface{}, 0)
	for idx, csvCol := range csvCols {
		ticol := ticols[idx]
		val, err := fromCsvValToColValue(csvConfig, csvCol, ticol.FieldType)
		if err != nil {
			return nil, err
		}
		data[ticol.Name.O] = val
	}

	return data, nil
}
