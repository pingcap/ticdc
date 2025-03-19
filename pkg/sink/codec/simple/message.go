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
	"fmt"
	"sort"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
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
		timodel.ActionAlterTablePartitioning, timodel.ActionRemovePartitioning:
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

// newTiColumnInfo uses columnSchema and IndexSchema to construct a tidb column info.
func newTiColumnInfo(
	column *columnSchema, colID int64, indexes []*IndexSchema,
) *timodel.ColumnInfo {
	col := new(timodel.ColumnInfo)
	col.ID = colID
	col.Name = pmodel.NewCIStr(column.Name)

	col.FieldType = *types.NewFieldType(types.StrToType(column.DataType.MySQLType))
	col.SetCharset(column.DataType.Charset)
	col.SetCollate(column.DataType.Collate)
	if column.DataType.Unsigned {
		col.AddFlag(mysql.UnsignedFlag)
	}
	if column.DataType.Zerofill {
		col.AddFlag(mysql.ZerofillFlag)
	}
	col.SetFlen(column.DataType.Length)
	col.SetDecimal(column.DataType.Decimal)
	col.SetElems(column.DataType.Elements)

	if utils.IsBinaryMySQLType(column.DataType.MySQLType) {
		col.AddFlag(mysql.BinaryFlag)
	}

	if !column.Nullable {
		col.AddFlag(mysql.NotNullFlag)
	}

	defaultValue := column.Default
	if defaultValue != nil && col.GetType() == mysql.TypeBit {
		switch v := defaultValue.(type) {
		case float64:
			byteSize := (col.GetFlen() + 7) >> 3
			defaultValue = tiTypes.NewBinaryLiteralFromUint(uint64(v), byteSize)
			defaultValue = defaultValue.(tiTypes.BinaryLiteral).ToString()
		default:
		}
	}

	for _, index := range indexes {
		for _, name := range index.Columns {
			if name == column.Name {
				if index.Primary {
					col.AddFlag(mysql.PriKeyFlag)
				} else if index.Unique {
					col.AddFlag(mysql.UniqueKeyFlag)
				} else {
					col.AddFlag(mysql.MultipleKeyFlag)
				}
			}
		}
	}

	err := col.SetDefaultValue(defaultValue)
	if err != nil {
		log.Panic("set default value failed", zap.Any("column", col), zap.Any("default", defaultValue))
	}
	return col
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

// newTiIndexInfo convert IndexSchema to a tidb index info.
func newTiIndexInfo(indexSchema *IndexSchema, columns []*timodel.ColumnInfo, indexID int64) *timodel.IndexInfo {
	indexColumns := make([]*timodel.IndexColumn, len(indexSchema.Columns))
	for i, col := range indexSchema.Columns {
		var offset int
		for idx, column := range columns {
			if column.Name.O == col {
				offset = idx
				break
			}
		}
		indexColumns[i] = &timodel.IndexColumn{
			Name:   pmodel.NewCIStr(col),
			Offset: offset,
		}
	}

	return &timodel.IndexInfo{
		ID:      indexID,
		Name:    pmodel.NewCIStr(indexSchema.Name),
		Columns: indexColumns,
		Unique:  indexSchema.Unique,
		Primary: indexSchema.Primary,
	}
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

	sort.SliceStable(colInfos, func(i, j int) bool {
		return colInfos[i].ID < colInfos[j].ID
	})

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

// newTableInfo converts from TableSchema to TableInfo.
func newTableInfo(m *TableSchema) *commonType.TableInfo {
	var database string

	tidbTableInfo := &timodel.TableInfo{}
	if m != nil {
		database = m.Schema

		tidbTableInfo.ID = m.TableID
		tidbTableInfo.Name = pmodel.NewCIStr(m.Table)
		tidbTableInfo.UpdateTS = m.Version

		nextMockID := int64(100)
		for _, col := range m.Columns {
			tiCol := newTiColumnInfo(col, nextMockID, m.Indexes)
			nextMockID += 100
			tidbTableInfo.Columns = append(tidbTableInfo.Columns, tiCol)
		}

		mockIndexID := int64(1)
		for _, idx := range m.Indexes {
			index := newTiIndexInfo(idx, tidbTableInfo.Columns, mockIndexID)
			tidbTableInfo.Indices = append(tidbTableInfo.Indices, index)
			mockIndexID += 1
		}
	}
	return commonType.WrapTableInfo(database, tidbTableInfo)
}

// newDDLEvent converts from message to DDLEvent.
func newDDLEvent(msg *message) *commonEvent.DDLEvent {
	var (
		tableInfo    *commonType.TableInfo
		preTableInfo *commonType.TableInfo
	)

	tableInfo = newTableInfo(msg.TableSchema)

	ddl := &commonEvent.DDLEvent{
		FinishedTs: msg.CommitTs,
		TableInfo:  tableInfo,
		Query:      msg.SQL,
	}
	if msg.PreTableSchema != nil {
		preTableInfo = newTableInfo(msg.PreTableSchema)
		ddl.MultipleTableInfos = append(ddl.MultipleTableInfos, tableInfo, preTableInfo)
	}
	return ddl
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

func (a *JSONMarshaller) newDMLMessage(
	event *commonEvent.RowEvent,
	onlyHandleKey bool, claimCheckFileName string,
) *message {
	m := &message{
		Version:            defaultVersion,
		Schema:             event.TableInfo.GetSchemaName(),
		Table:              event.TableInfo.GetTableName(),
		TableID:            event.TableInfo.TableName.TableID,
		CommitTs:           event.CommitTs,
		BuildTs:            time.Now().UnixMilli(),
		SchemaVersion:      event.TableInfo.UpdateTS(),
		HandleKeyOnly:      onlyHandleKey,
		ClaimCheckLocation: claimCheckFileName,
	}
	if event.IsInsert() {
		m.Type = DMLTypeInsert
		m.Data = a.formatColumns(event.GetRows(), event.TableInfo, onlyHandleKey)
	} else if event.IsDelete() {
		m.Type = DMLTypeDelete
		m.Old = a.formatColumns(event.GetPreRows(), event.TableInfo, onlyHandleKey)
	} else if event.IsUpdate() {
		m.Type = DMLTypeUpdate
		m.Data = a.formatColumns(event.GetRows(), event.TableInfo, onlyHandleKey)
		m.Old = a.formatColumns(event.GetPreRows(), event.TableInfo, onlyHandleKey)
	}
	// TODO: EnableRowChecksum
	// if a.config.EnableRowChecksum && event.Checksum != nil {
	// 	m.Checksum = &checksum{
	// 		Version:   event.Checksum.Version,
	// 		Corrupted: event.Checksum.Corrupted,
	// 		Current:   event.Checksum.Current,
	// 		Previous:  event.Checksum.Previous,
	// 	}
	// }
	return m
}

func (a *JSONMarshaller) formatColumns(
	row *chunk.Row, tableInfo *commonType.TableInfo, onlyHandleKey bool,
) map[string]interface{} {
	colInfos := tableInfo.GetColumns()
	result := make(map[string]interface{}, len(colInfos))
	for i, colInfo := range colInfos {
		if colInfo != nil {
			if onlyHandleKey && !tableInfo.ForceGetColumnFlagType(colInfo.ID).IsPrimaryKey() {
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
	switch ft.GetType() {
	case mysql.TypeTimestamp:
		return map[string]interface{}{
			"location": a.config.TimeZone.String(),
			"value":    row.GetString(i),
		}, "com.pingcap.simple.avro.Timestamp"
	case mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			return map[string]interface{}{
				"value": row.GetInt64(i),
			}, "com.pingcap.simple.avro.UnsignedBigint"
		}
	}
	d := row.GetDatum(i, ft)
	value := d.GetValue()
	switch v := value.(type) {
	case uint64:
		return int64(v), "long"
	case int64:
		return v, "long"
	case []byte:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			return v, "bytes"
		}
		return string(v), "string"
	case float32:
		return v, "float"
	case float64:
		return v, "double"
	case string:
		return v, "string"
	case tiTypes.VectorFloat32:
		return v.String(), "string"
	default:
		log.Panic("unexpected type for avro value", zap.Any("value", value))
	}
	return value, ""
}

func encodeValue(
	row *chunk.Row, i int, ft *types.FieldType, location string,
) interface{} {
	if row.IsNull(i) {
		return nil
	}
	var value any
	d := row.GetDatum(i, ft)
	var err error
	switch ft.GetType() {
	case mysql.TypeBit:
		value, err = d.GetMysqlBit().ToInt(tiTypes.DefaultStmtNoWarningContext)
	case mysql.TypeTimestamp:
		return map[string]string{
			"location": location,
			"value":    d.GetMysqlTime().String(),
		}
	case mysql.TypeEnum:
		value = d.GetMysqlEnum().Value
	case mysql.TypeSet:
		value = d.GetMysqlSet().Value
	case mysql.TypeTiDBVectorFloat32:
		value = d.GetVectorFloat32().String()
	default:
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		value = fmt.Sprintf("%v", d.GetValue())
	}
	if err != nil {
		log.Panic("parse bit to int failed", zap.Any("name", value), zap.Error(err))
	}
	return value
}
