// Copyright 2023 PingCAP, Inc.
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

package simple

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

const (
	defaultVersion = 1
)

// MessageType is the type of the message.
type MessageType string

const (
	// MessageTypeWatermark is the type of the watermark event.
	MessageTypeWatermark MessageType = "WATERMARK"
	// MessageTypeBootstrap is the type of the bootstrap event.
	MessageTypeBootstrap MessageType = "BOOTSTRAP"
	// MessageTypeDDL is the type of the ddl event.
	MessageTypeDDL MessageType = "DDL"
	// MessageTypeDML is the type of the row event.
	MessageTypeDML MessageType = "DML"
)

// DML Message types
const (
	// DMLTypeInsert is the type of the insert event.
	DMLTypeInsert MessageType = "INSERT"
	// DMLTypeUpdate is the type of the update event.
	DMLTypeUpdate MessageType = "UPDATE"
	// DMLTypeDelete is the type of the delete event.
	DMLTypeDelete MessageType = "DELETE"
)

// DDL message types
const (
	DDLTypeCreate   MessageType = "CREATE"
	DDLTypeRename   MessageType = "RENAME"
	DDLTypeCIndex   MessageType = "CINDEX"
	DDLTypeDIndex   MessageType = "DINDEX"
	DDLTypeErase    MessageType = "ERASE"
	DDLTypeTruncate MessageType = "TRUNCATE"
	DDLTypeAlter    MessageType = "ALTER"
	DDLTypeQuery    MessageType = "QUERY"
)

func getDDLType(t timodel.ActionType) MessageType {
	switch t {
	case timodel.ActionCreateTable:
		return DDLTypeCreate
	case timodel.ActionRenameTable, timodel.ActionRenameTables:
		return DDLTypeRename
	case timodel.ActionAddIndex, timodel.ActionAddForeignKey, timodel.ActionAddPrimaryKey:
		return DDLTypeCIndex
	case timodel.ActionDropIndex, timodel.ActionDropForeignKey, timodel.ActionDropPrimaryKey:
		return DDLTypeDIndex
	case timodel.ActionDropTable:
		return DDLTypeErase
	case timodel.ActionTruncateTable:
		return DDLTypeTruncate
	case timodel.ActionAddColumn, timodel.ActionDropColumn, timodel.ActionModifyColumn, timodel.ActionRebaseAutoID,
		timodel.ActionSetDefaultValue, timodel.ActionModifyTableComment, timodel.ActionRenameIndex, timodel.ActionAddTablePartition,
		timodel.ActionDropTablePartition, timodel.ActionModifyTableCharsetAndCollate, timodel.ActionTruncateTablePartition,
		timodel.ActionAlterIndexVisibility, timodel.ActionMultiSchemaChange, timodel.ActionReorganizePartition,
		timodel.ActionAlterTablePartitioning, timodel.ActionRemovePartitioning, timodel.ActionExchangeTablePartition:
		return DDLTypeAlter
	default:
		return DDLTypeQuery
	}
}

// columnSchema is the schema of the column.
type columnSchema struct {
	Name     string      `json:"name"`
	DataType dataType    `json:"dataType"`
	Nullable bool        `json:"nullable"`
	Default  interface{} `json:"default"`
}

type dataType struct {
	// MySQLType represent the basic mysql type
	MySQLType string `json:"mysqlType"`

	Charset string `json:"charset"`
	Collate string `json:"collate"`

	// length represent size of bytes of the field
	Length int `json:"length,omitempty"`
	// Decimal represent decimal length of the field
	Decimal int `json:"decimal,omitempty"`
	// Elements represent the element list for enum and set type.
	Elements []string `json:"elements,omitempty"`

	Unsigned bool `json:"unsigned,omitempty"`
	Zerofill bool `json:"zerofill,omitempty"`
}

// newColumnSchema converts from TiDB ColumnInfo to columnSchema.
func newColumnSchema(col *timodel.ColumnInfo) *columnSchema {
	tp := dataType{
		MySQLType: types.TypeToStr(col.GetType(), col.GetCharset()),
		Charset:   col.GetCharset(),
		Collate:   col.GetCollate(),
		Length:    col.GetFlen(),
		Elements:  col.GetElems(),
		Unsigned:  mysql.HasUnsignedFlag(col.GetFlag()),
		Zerofill:  mysql.HasZerofillFlag(col.GetFlag()),
	}

	switch col.GetType() {
	// Float and Double decimal is always -1, do not encode it into the schema.
	case mysql.TypeFloat, mysql.TypeDouble:
	default:
		tp.Decimal = col.GetDecimal()
	}

	defaultValue := col.GetDefaultValue()
	if defaultValue != nil && col.GetType() == mysql.TypeBit {
		defaultValue = common.MustBinaryLiteralToInt([]byte(defaultValue.(string)))
	}
	return &columnSchema{
		Name:     col.Name.O,
		DataType: tp,
		Nullable: !mysql.HasNotNullFlag(col.GetFlag()),
		Default:  defaultValue,
	}
}

// IndexSchema is the schema of the index.
type IndexSchema struct {
	Name     string   `json:"name"`
	Unique   bool     `json:"unique"`
	Primary  bool     `json:"primary"`
	Nullable bool     `json:"nullable"`
	Columns  []string `json:"columns"`
}

// newIndexSchema converts from TiDB IndexInfo to IndexSchema.
func newIndexSchema(index *timodel.IndexInfo, columns []*timodel.ColumnInfo) *IndexSchema {
	indexSchema := &IndexSchema{
		Name:    index.Name.O,
		Unique:  index.Unique,
		Primary: index.Primary,
	}
	for _, col := range index.Columns {
		indexSchema.Columns = append(indexSchema.Columns, col.Name.O)
		colInfo := columns[col.Offset]
		// An index is not null when all columns of are not null
		if !mysql.HasNotNullFlag(colInfo.GetFlag()) {
			indexSchema.Nullable = true
		}
	}
	return indexSchema
}

// TableSchema is the schema of the table.
type TableSchema struct {
	Schema  string          `json:"schema"`
	Table   string          `json:"table"`
	TableID int64           `json:"tableID"`
	Version uint64          `json:"version"`
	Columns []*columnSchema `json:"columns"`
	Indexes []*IndexSchema  `json:"indexes"`
}

func newTableSchema(tableInfo *commonType.TableInfo) *TableSchema {
	pkInIndexes := false
	indexes := make([]*IndexSchema, 0, len(tableInfo.GetIndices()))
	colInfos := tableInfo.GetColumns()
	for _, colInfo := range tableInfo.GetIndices() {
		index := newIndexSchema(colInfo, colInfos)
		if index.Primary {
			pkInIndexes = true
		}
		indexes = append(indexes, index)
	}

	// sometimes the primary key is not in the index, we need to find it manually.
	if !pkInIndexes {
		pkColumns := tableInfo.GetPrimaryKeyColumnNames()
		if len(pkColumns) != 0 {
			index := &IndexSchema{
				Name:     "primary",
				Nullable: false,
				Primary:  true,
				Unique:   true,
				Columns:  pkColumns,
			}
			indexes = append(indexes, index)
		}
	}

	columns := make([]*columnSchema, 0, len(colInfos))
	for _, col := range colInfos {
		colSchema := newColumnSchema(col)
		columns = append(columns, colSchema)
	}

	return &TableSchema{
		Schema:  tableInfo.TableName.Schema,
		Table:   tableInfo.TableName.Table,
		TableID: tableInfo.TableName.TableID,
		Version: tableInfo.UpdateTS(),
		Columns: columns,
		Indexes: indexes,
	}
}

type checksum struct {
	Version   int    `json:"version"`
	Corrupted bool   `json:"corrupted"`
	Current   uint32 `json:"current"`
	Previous  uint32 `json:"previous"`
}

type message struct {
	Version int `json:"version"`
	// Schema and Table is empty for the resolved ts event.
	Schema  string      `json:"database,omitempty"`
	Table   string      `json:"table,omitempty"`
	TableID int64       `json:"tableID,omitempty"`
	Type    MessageType `json:"type"`
	// SQL is only for the DDL event.
	SQL      string `json:"sql,omitempty"`
	CommitTs uint64 `json:"commitTs"`
	BuildTs  int64  `json:"buildTs"`
	// SchemaVersion is for the DML event.
	SchemaVersion uint64 `json:"schemaVersion,omitempty"`

	// ClaimCheckLocation is only for the DML event.
	ClaimCheckLocation string `json:"claimCheckLocation,omitempty"`
	// HandleKeyOnly is only for the DML event.
	HandleKeyOnly bool `json:"handleKeyOnly,omitempty"`

	// E2E checksum related fields, only set when enable checksum functionality.
	Checksum *checksum `json:"checksum,omitempty"`

	// Data is available for the Insert and Update event.
	Data map[string]interface{} `json:"data,omitempty"`
	// Old is available for the Update and Delete event.
	Old map[string]interface{} `json:"old,omitempty"`
	// TableSchema is for the DDL and Bootstrap event.
	TableSchema *TableSchema `json:"tableSchema,omitempty"`
	// PreTableSchema holds schema information before the DDL executed.
	PreTableSchema *TableSchema `json:"preTableSchema,omitempty"`
}

func newResolvedMessage(ts uint64) *message {
	return &message{
		Version:  defaultVersion,
		Type:     MessageTypeWatermark,
		CommitTs: ts,
		BuildTs:  time.Now().UnixMilli(),
	}
}

func newBootstrapMessage(tableInfo *commonType.TableInfo) *message {
	schema := newTableSchema(tableInfo)
	msg := &message{
		Version:     defaultVersion,
		Type:        MessageTypeBootstrap,
		BuildTs:     time.Now().UnixMilli(),
		TableSchema: schema,
	}
	return msg
}

func newDDLMessage(ddl *commonEvent.DDLEvent) *message {
	var (
		schema    *TableSchema
		preSchema *TableSchema
	)
	// the tableInfo maybe nil if the DDL is `drop database`
	if ddl.TableInfo != nil {
		schema = newTableSchema(ddl.TableInfo)
	}
	// `PreTableInfo` may not exist for some DDL, such as `create table`
	if len(ddl.MultipleTableInfos) > 1 {
		preSchema = newTableSchema(ddl.MultipleTableInfos[1]) // TODO: need check it
	}
	msg := &message{
		Version:        defaultVersion,
		Type:           getDDLType(ddl.GetDDLType()),
		CommitTs:       ddl.GetCommitTs(),
		BuildTs:        time.Now().UnixMilli(),
		SQL:            ddl.Query,
		TableSchema:    schema,
		PreTableSchema: preSchema,
	}
	return msg
}

func (a *jsonMarshaller) newDMLMessage(
	event *commonEvent.RowEvent,
	onlyHandleKey bool, claimCheckFileName string,
) *message {
	m := &message{
		Version:            defaultVersion,
		Schema:             event.TableInfo.GetSchemaName(),
		Table:              event.TableInfo.GetTableName(),
		TableID:            event.GetTableID(),
		CommitTs:           event.CommitTs,
		BuildTs:            time.Now().UnixMilli(),
		SchemaVersion:      event.TableInfo.UpdateTS(),
		HandleKeyOnly:      onlyHandleKey,
		ClaimCheckLocation: claimCheckFileName,
	}
	if event.IsInsert() {
		m.Type = DMLTypeInsert
		m.Data = a.formatColumns(event.GetRows(), event.TableInfo, onlyHandleKey, event.ColumnSelector)
	} else if event.IsDelete() {
		m.Type = DMLTypeDelete
		m.Old = a.formatColumns(event.GetPreRows(), event.TableInfo, onlyHandleKey, event.ColumnSelector)
	} else if event.IsUpdate() {
		m.Type = DMLTypeUpdate
		m.Data = a.formatColumns(event.GetRows(), event.TableInfo, onlyHandleKey, event.ColumnSelector)
		m.Old = a.formatColumns(event.GetPreRows(), event.TableInfo, onlyHandleKey, event.ColumnSelector)
	}
	if a.config.EnableRowChecksum && event.Checksum != nil {
		m.Checksum = &checksum{
			Version:   event.Checksum.Version,
			Corrupted: event.Checksum.Corrupted,
			Current:   event.Checksum.Current,
			Previous:  event.Checksum.Previous,
		}
	}
	return m
}

func (a *jsonMarshaller) formatColumns(
	row *chunk.Row, tableInfo *commonType.TableInfo, onlyHandleKey bool, columnSelector columnselector.Selector,
) map[string]interface{} {
	colInfos := tableInfo.GetColumns()
	result := make(map[string]interface{}, len(colInfos))
	for i, colInfo := range colInfos {
		if !columnSelector.Select(colInfo) {
			continue
		}
		if colInfo != nil {
			if onlyHandleKey && !tableInfo.IsHandleKey(colInfo.ID) {
				continue
			}
			value := encodeValue(row, i, &colInfo.FieldType, a.config.TimeZone.String())
			result[colInfo.Name.O] = value
		}
	}
	return result
}

func (a *avroMarshaller) encodeValue4Avro(row *chunk.Row, i int, ft *types.FieldType) (interface{}, string) {
	if row.IsNull(i) {
		return nil, "null"
	}
	d := row.GetDatum(i, ft)
	switch ft.GetType() {
	case mysql.TypeTimestamp:
		return map[string]interface{}{
			"location": a.config.TimeZone.String(),
			"value":    d.GetMysqlTime().String(),
		}, "com.pingcap.simple.avro.Timestamp"
	case mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			return map[string]interface{}{
				"value": d.GetInt64(),
			}, "com.pingcap.simple.avro.UnsignedBigint"
		}
		return d.GetInt64(), "long"
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			return d.GetBytes(), "bytes"
		}
		return d.GetString(), "string"
	case mysql.TypeTiDBVectorFloat32:
		return d.GetVectorFloat32().String(), "string"
	case mysql.TypeFloat:
		return d.GetFloat32(), "float"
	case mysql.TypeDouble:
		return d.GetFloat64(), "double"
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong,
		mysql.TypeYear, mysql.TypeEnum, mysql.TypeSet:
		return d.GetInt64(), "long"
	case mysql.TypeNewDecimal:
		return string(d.GetMysqlDecimal().ToString()), "string"
	case mysql.TypeDate, mysql.TypeDatetime:
		return d.GetMysqlTime().String(), "string"
	case mysql.TypeDuration:
		return d.GetMysqlDuration().String(), "string"
	case mysql.TypeBit:
		v, err := d.GetMysqlBit().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			log.Panic("invalid column value for bit", zap.Any("value", d.GetValue()), zap.Error(err))
		}
		return strconv.FormatUint(v, 10), "string"
	case mysql.TypeJSON:
		return d.GetMysqlJSON().String(), "string"
	default:
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		return d.GetValue(), ""
	}
}

func encodeValue(
	row *chunk.Row, i int, ft *types.FieldType, location string,
) interface{} {
	if row.IsNull(i) {
		return nil
	}
	var value any
	d := row.GetDatum(i, ft)
	switch ft.GetType() {
	case mysql.TypeBit:
		v, err := d.GetMysqlBit().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			log.Panic("invalid column value for bit", zap.Any("value", value), zap.Error(err))
		}
		value = strconv.FormatUint(v, 10)
	case mysql.TypeTimestamp:
		value = map[string]interface{}{
			"location": location,
			"value":    d.GetMysqlTime().String(),
		}
	case mysql.TypeEnum:
		value = strconv.FormatUint(d.GetMysqlEnum().Value, 10)
	case mysql.TypeSet:
		value = strconv.FormatUint(d.GetMysqlSet().Value, 10)
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			value = base64.StdEncoding.EncodeToString(d.GetBytes())
		} else {
			value = d.GetString()
		}
	default:
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		value = fmt.Sprintf("%v", d.GetValue())
	}
	return value
}
