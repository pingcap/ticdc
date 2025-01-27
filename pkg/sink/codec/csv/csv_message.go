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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/kv"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"

	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// a csv row should at least contain operation-type, table-name, schema-name and one table column
const minimumColsCnt = 4

// operation specifies the operation type
type operation int

// enum types of operation
const (
	operationInsert operation = iota
	operationDelete
	operationUpdate
)

func (o operation) String() string {
	switch o {
	case operationInsert:
		return "I"
	case operationDelete:
		return "D"
	case operationUpdate:
		return "U"
	default:
		return "unknown"
	}
}

func (o *operation) FromString(op string) error {
	switch op {
	case "I":
		*o = operationInsert
	case "D":
		*o = operationDelete
	case "U":
		*o = operationUpdate
	default:
		return fmt.Errorf("invalid operation type %s", op)
	}

	return nil
}

type csvMessage struct {
	// config hold the codec configuration items.
	config *common.Config
	// opType denotes the specific operation type.
	opType     operation
	tableName  string
	schemaName string
	commitTs   uint64
	columns    []any
	preColumns []any
	// newRecord indicates whether we encounter a new record.
	newRecord bool
	HandleKey kv.Handle
}

func newCSVMessage(config *common.Config) *csvMessage {
	return &csvMessage{
		config:    config,
		newRecord: true,
	}
}

// encode returns a byte slice composed of the columns as follows:
// Col1: The operation-type indicator: I, D, U.
// Col2: Table name, the name of the source table.
// Col3: Schema name, the name of the source schema.
// Col4: Commit TS, the commit-ts of the source txn (optional).
// Col5-n: one or more columns that represent the data to be changed.
func (c *csvMessage) encode() []byte {
	strBuilder := new(strings.Builder)
	if c.opType == operationUpdate && c.config.OutputOldValue && len(c.preColumns) != 0 {
		// Encode the old value first as a dedicated row.
		c.encodeMeta("D", strBuilder)
		c.encodeColumns(c.preColumns, strBuilder)

		// Encode the after value as a dedicated row.
		c.newRecord = true // reset newRecord to true, so that the first column will not start with delimiter.
		c.encodeMeta("I", strBuilder)
		c.encodeColumns(c.columns, strBuilder)
	} else {
		c.encodeMeta(c.opType.String(), strBuilder)
		c.encodeColumns(c.columns, strBuilder)
	}
	return []byte(strBuilder.String())
}

func (c *csvMessage) encodeMeta(opType string, b *strings.Builder) {
	c.formatValue(opType, b)
	c.formatValue(c.tableName, b)
	c.formatValue(c.schemaName, b)
	if c.config.IncludeCommitTs {
		c.formatValue(c.commitTs, b)
	}
	if c.config.OutputOldValue {
		// When c.config.OutputOldValue, we need an extra column "is-updated"
		// to indicate whether the row is updated or just original insert/delete
		if c.opType == operationUpdate {
			c.formatValue(true, b)
		} else {
			c.formatValue(false, b)
		}
	}
	if c.config.OutputHandleKey {
		c.formatValue(c.HandleKey.String(), b)
	}
}

func (c *csvMessage) encodeColumns(columns []any, b *strings.Builder) {
	for _, col := range columns {
		c.formatValue(col, b)
	}
	b.WriteString(c.config.Terminator)
}

func (c *csvMessage) decode(datums []types.Datum) error {
	var dataColIdx int
	if len(datums) < minimumColsCnt {
		return cerror.WrapError(cerror.ErrCSVDecodeFailed,
			errors.New("the csv row should have at least four columns"+
				"(operation-type, table-name, schema-name, commit-ts)"))
	}

	if err := c.opType.FromString(datums[0].GetString()); err != nil {
		return cerror.WrapError(cerror.ErrCSVDecodeFailed, err)
	}
	dataColIdx++
	c.tableName = datums[1].GetString()
	dataColIdx++
	c.schemaName = datums[2].GetString()
	dataColIdx++
	if c.config.IncludeCommitTs {
		commitTs, err := strconv.ParseUint(datums[3].GetString(), 10, 64)
		if err != nil {
			return cerror.WrapError(cerror.ErrCSVDecodeFailed,
				fmt.Errorf("the 4th column(%s) of csv row should be a valid commit-ts", datums[3].GetString()))
		}
		c.commitTs = commitTs
		dataColIdx++
	} else {
		c.commitTs = 0
	}
	if c.config.OutputOldValue {
		// When c.config.OutputOldValue, we need an extra column "is-updated".
		// TODO: use this flag to guarantee data consistency in update uk/pk scenario.
		dataColIdx++
	}
	c.columns = c.columns[:0]

	for i := dataColIdx; i < len(datums); i++ {
		if datums[i].IsNull() {
			c.columns = append(c.columns, nil)
		} else {
			c.columns = append(c.columns, datums[i].GetString())
		}
	}

	return nil
}

// as stated in https://datatracker.ietf.org/doc/html/rfc4180,
// if double-quotes are used to enclose fields, then a double-quote
// appearing inside a field must be escaped by preceding it with
// another double quote.
func (c *csvMessage) formatWithQuotes(value string, strBuilder *strings.Builder) {
	quote := c.config.Quote

	strBuilder.WriteString(quote)
	// replace any quote in csv column with two quotes.
	strBuilder.WriteString(strings.ReplaceAll(value, quote, quote+quote))
	strBuilder.WriteString(quote)
}

// formatWithEscapes escapes the csv column if necessary.
func (c *csvMessage) formatWithEscapes(value string, strBuilder *strings.Builder) {
	lastPos := 0
	delimiter := c.config.Delimiter

	for i := 0; i < len(value); i++ {
		ch := value[i]
		isDelimiterStart := strings.HasPrefix(value[i:], delimiter)
		// if '\r', '\n', '\' or the delimiter (may have multiple characters) are contained in
		// csv column, we should escape these characters.
		if ch == config.CR || ch == config.LF || ch == config.Backslash || isDelimiterStart {
			// write out characters up until this position.
			strBuilder.WriteString(value[lastPos:i])
			switch ch {
			case config.LF:
				ch = 'n'
			case config.CR:
				ch = 'r'
			}
			strBuilder.WriteRune(config.Backslash)
			strBuilder.WriteRune(rune(ch))

			// escape each characters in delimiter.
			if isDelimiterStart {
				for k := 1; k < len(c.config.Delimiter); k++ {
					strBuilder.WriteRune(config.Backslash)
					strBuilder.WriteRune(rune(delimiter[k]))
				}
				lastPos = i + len(delimiter)
			} else {
				lastPos = i + 1
			}
		}
	}
	strBuilder.WriteString(value[lastPos:])
}

// formatValue formats the csv column and appends it to a string builder.
func (c *csvMessage) formatValue(value any, strBuilder *strings.Builder) {
	defer func() {
		// reset newRecord to false after handing the first csv column
		c.newRecord = false
	}()

	if !c.newRecord {
		strBuilder.WriteString(c.config.Delimiter)
	}

	if value == nil {
		strBuilder.WriteString(c.config.NullString)
		return
	}

	switch v := value.(type) {
	case string:
		// if quote is configured, format the csv column with quotes,
		// otherwise escape this csv column.
		if len(c.config.Quote) != 0 {
			c.formatWithQuotes(v, strBuilder)
		} else {
			c.formatWithEscapes(v, strBuilder)
		}
	default:
		strBuilder.WriteString(fmt.Sprintf("%v", v))
	}
}

// fromColValToCsvVal converts column from TiDB type to csv type.
func fromColValToCsvVal(csvConfig *common.Config, row *chunk.Row, idx int, colInfo *timodel.ColumnInfo, flag *commonType.ColumnFlagType) (any, error) {
	if row.IsNull(idx) {
		return nil, nil
	}

	switch colInfo.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if flag.IsBinary() {
			v := row.GetBytes(idx)
			switch csvConfig.BinaryEncodingMethod {
			case config.BinaryEncodingBase64:
				return base64.StdEncoding.EncodeToString(v), nil
			case config.BinaryEncodingHex:
				return hex.EncodeToString(v), nil
			default:
				return nil, cerror.WrapError(cerror.ErrCSVEncodeFailed,
					errors.Errorf("unsupported binary encoding method %s",
						csvConfig.BinaryEncodingMethod))
			}
		}
		return row.GetString(idx), nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		return row.GetTime(idx).String(), nil
	case mysql.TypeDuration:
		return row.GetDuration(idx, colInfo.GetDecimal()).String(), nil
	case mysql.TypeEnum:
		enumValue := row.GetEnum(idx).Value
		enumVar, err := types.ParseEnumValue(colInfo.GetElems(), enumValue)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrCSVEncodeFailed, err)
		}
		return enumVar.Name, nil
	case mysql.TypeSet:
		bitValue := row.GetEnum(idx).Value
		setVar, err := types.ParseSetValue(colInfo.GetElems(), bitValue)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrCSVEncodeFailed, err)
		}
		return setVar.Name, nil
	case mysql.TypeBit:
		d := row.GetDatum(idx, &colInfo.FieldType)
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		return d.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
	case mysql.TypeNewDecimal:
		return row.GetMyDecimal(idx).String(), nil
	case mysql.TypeJSON:
		return row.GetJSON(idx).String(), nil
	case mysql.TypeTiDBVectorFloat32:
		vec := row.GetVectorFloat32(idx)
		return vec.String(), nil
	default:
		d := row.GetDatum(idx, &colInfo.FieldType)
		return d.GetValue(), nil
	}
}

// rowChangedEvent2CSVMsg converts a RowChangedEvent to a csv record.
func rowChangedEvent2CSVMsg(csvConfig *common.Config, e *event.RowEvent) (*csvMessage, error) {
	var err error

	csvMsg := &csvMessage{
		config:     csvConfig,
		tableName:  e.TableInfo.GetTableName(),
		schemaName: e.TableInfo.GetSchemaName(),
		commitTs:   e.CommitTs,
		newRecord:  true,
	}

	if csvConfig.OutputHandleKey {
		log.Warn("not support output handle key")
		// csvMsg.HandleKey = e.HandleKey
	}

	if e.IsDelete() {
		csvMsg.opType = operationDelete
		csvMsg.columns, err = rowChangeColumns2CSVColumns(csvConfig, e.GetPreRows(), e.TableInfo)
		if err != nil {
			return nil, err
		}
	} else {
		if e.GetPreRows() == nil {
			// This is a insert operation.
			csvMsg.opType = operationInsert
			csvMsg.columns, err = rowChangeColumns2CSVColumns(csvConfig, e.GetRows(), e.TableInfo)
			if err != nil {
				return nil, err
			}
		} else {
			// This is a update operation.
			csvMsg.opType = operationUpdate
			if csvConfig.OutputOldValue {
				if e.GetPreRows().Len() != e.GetRows().Len() {
					return nil, cerror.WrapError(cerror.ErrCSVDecodeFailed,
						fmt.Errorf("the column length of preColumns %d doesn't equal to that of columns %d",
							e.GetPreRows().Len(), e.GetRows().Len()))
				}
				csvMsg.preColumns, err = rowChangeColumns2CSVColumns(csvConfig, e.GetPreRows(), e.TableInfo)
				if err != nil {
					return nil, err
				}
			}
			csvMsg.columns, err = rowChangeColumns2CSVColumns(csvConfig, e.GetRows(), e.TableInfo)
			if err != nil {
				return nil, err
			}
		}
	}
	return csvMsg, nil
}

func rowChangeColumns2CSVColumns(csvConfig *common.Config, row *chunk.Row, tableInfo *commonType.TableInfo) ([]any, error) {
	var csvColumns []any

	for i, col := range tableInfo.GetColumns() {
		// column could be nil in a condition described in
		// https://github.com/pingcap/tiflow/issues/6198#issuecomment-1191132951
		if col == nil {
			continue
		}

		flag := tableInfo.GetColumnFlags()[col.ID]
		converted, err := fromColValToCsvVal(csvConfig, row, i, col, flag)
		if err != nil {
			return nil, errors.Trace(err)
		}
		csvColumns = append(csvColumns, converted)
	}

	return csvColumns, nil
}
