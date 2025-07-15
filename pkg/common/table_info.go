// Copyright 2025 PingCAP, Inc.
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

package common

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
)

// QuoteSchema quotes a full table name
func QuoteSchema(schema string, table string) string {
	var builder strings.Builder
	builder.WriteString("`")
	builder.WriteString(EscapeName(schema))
	builder.WriteString("`.`")
	builder.WriteString(EscapeName(table))
	builder.WriteString("`")
	return builder.String()
}

// QuoteName wraps a name with "`"
func QuoteName(name string) string {
	return "`" + EscapeName(name) + "`"
}

// EscapeName replaces all "`" in name with double "`"
func EscapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

const (
	// HandleIndexPKIsHandle represents that the handle index is the pk and the pk is the handle
	HandleIndexPKIsHandle = -1
	// HandleIndexTableIneligible represents that the table is ineligible
	HandleIndexTableIneligible = -2
)

const (
	preSQLInsert = iota
	preSQLReplace
	preSQLUpdate
	preSQLDelete
)

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	// NOTICE: We probably store the logical ID inside TableName,
	// not the physical ID.
	// For normal table, there is only one ID, which is the physical ID.
	// AKA TIDB_TABLE_ID.
	// For partitioned table, there are two kinds of ID:
	// 1. TIDB_PARTITION_ID is the physical ID of the partition.
	// 2. TIDB_TABLE_ID is the logical ID of the table.
	// In general, we always use the physical ID to represent a table, but we
	// record the logical ID from the DDL event(job.BinlogInfo.TableInfo).
	// So be careful when using the TableInfo.
	TableName TableName `json:"table-name"`
	Charset   string    `json:"charset"`
	Collate   string    `json:"collate"`
	Comment   string    `json:"comment"`

	columnSchema *columnSchema `json:"-"`

	View *model.ViewInfo `json:"view"`

	Sequence *model.SequenceInfo `json:"sequence"`

	preSQLs struct {
		isInitialized atomic.Bool
		mutex         sync.Mutex
		m             [4]string
	} `json:"-"`
}

func (ti *TableInfo) InitPrivateFields() {
	if ti == nil {
		return
	}

	if ti.preSQLs.isInitialized.Load() {
		return
	}

	ti.preSQLs.mutex.Lock()
	defer ti.preSQLs.mutex.Unlock()

	// Double-checked locking
	if ti.preSQLs.isInitialized.Load() {
		return
	}

	ti.TableName.quotedName = QuoteSchema(ti.TableName.Schema, ti.TableName.Table)
	ti.preSQLs.m[preSQLInsert] = fmt.Sprintf(ti.columnSchema.PreSQLs[preSQLInsert], ti.TableName.QuoteString())
	ti.preSQLs.m[preSQLReplace] = fmt.Sprintf(ti.columnSchema.PreSQLs[preSQLReplace], ti.TableName.QuoteString())
	ti.preSQLs.m[preSQLUpdate] = fmt.Sprintf(ti.columnSchema.PreSQLs[preSQLUpdate], ti.TableName.QuoteString())

	ti.preSQLs.isInitialized.Store(true)
}

func (ti *TableInfo) Marshal() ([]byte, error) {
	// otherField | columnSchemaData | columnSchemaDataSize
	data, err := json.Marshal(ti)
	if err != nil {
		return nil, err
	}
	columnSchemaData, err := ti.columnSchema.Marshal()
	if err != nil {
		return nil, err
	}
	columnSchemaDataSize := len(columnSchemaData)
	sizeByte := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeByte, uint64(columnSchemaDataSize))
	data = append(data, columnSchemaData...)
	data = append(data, sizeByte...)
	return data, nil
}

func UnmarshalJSONToTableInfo(data []byte) (*TableInfo, error) {
	// otherField | columnSchemaData | columnSchemaDataSize
	ti := &TableInfo{}
	var err error
	var columnSchemaDataSize uint64
	columnSchemaDataSizeValue := data[len(data)-8:]
	columnSchemaDataSize = binary.BigEndian.Uint64(columnSchemaDataSizeValue)

	columnSchemaData := data[len(data)-8-int(columnSchemaDataSize) : len(data)-8]
	restData := data[:len(data)-8-int(columnSchemaDataSize)]

	err = json.Unmarshal(restData, ti)
	if err != nil {
		return nil, err
	}

	ti.columnSchema, err = unmarshalJsonToColumnSchema(columnSchemaData)
	if err != nil {
		return nil, err
	}

	// when this tableInfo is released, we need to cut down the reference count of the columnSchema
	// This function should be appear when tableInfo is created as a pair.
	runtime.SetFinalizer(ti, func(ti *TableInfo) {
		GetSharedColumnSchemaStorage().tryReleaseColumnSchema(ti.columnSchema)
	})
	return ti, nil
}

func (ti *TableInfo) ShadowCopyColumnSchema() *columnSchema {
	return ti.columnSchema.Clone()
}

func (ti *TableInfo) GetColumns() []*model.ColumnInfo {
	return ti.columnSchema.Columns
}

func (ti *TableInfo) GetIndices() []*model.IndexInfo {
	return ti.columnSchema.Indices
}

// GetRowColumnsOffset return offset with visible column
func (ti *TableInfo) GetRowColumnsOffset() map[int64]int {
	return ti.columnSchema.RowColumnsOffset
}

func (ti *TableInfo) GetIndexColumns() [][]int64 {
	return ti.columnSchema.IndexColumns
}

func (ti *TableInfo) PKIsHandle() bool {
	return ti.columnSchema.PKIsHandle
}

func (ti *TableInfo) GetPKIndex() []int64 {
	return ti.columnSchema.PKIndex
}

// UpdateTS returns the UpdateTS of columnSchema
// These changing schema operations don't include 'truncate table', 'rename table',
// 'rename tables', 'truncate partition' and 'exchange partition'.
func (ti *TableInfo) UpdateTS() uint64 {
	return ti.columnSchema.UpdateTS
}

func (ti *TableInfo) GetPreInsertSQL() string {
	if ti.preSQLs.m[preSQLInsert] == "" {
		log.Panic("preSQLs[preSQLInsert] is not initialized")
	}
	return ti.preSQLs.m[preSQLInsert]
}

func (ti *TableInfo) GetPreReplaceSQL() string {
	if ti.preSQLs.m[preSQLReplace] == "" {
		log.Panic("preSQLs[preSQLReplace] is not initialized")
	}
	return ti.preSQLs.m[preSQLReplace]
}

func (ti *TableInfo) GetPreUpdateSQL() string {
	if ti.preSQLs.m[preSQLUpdate] == "" {
		log.Panic("preSQLs[preSQLUpdate] is not initialized")
	}
	return ti.preSQLs.m[preSQLUpdate]
}

// GetColumnInfo returns the column info by ID
func (ti *TableInfo) GetColumnInfo(colID int64) (info *model.ColumnInfo, exist bool) {
	colOffset, exist := ti.columnSchema.ColumnsOffset[colID]
	if !exist {
		return nil, false
	}
	return ti.columnSchema.Columns[colOffset], true
}

// ForceGetColumnInfo return the column info by ID
// Caller must ensure `colID` exists
func (ti *TableInfo) ForceGetColumnInfo(colID int64) *model.ColumnInfo {
	colInfo, ok := ti.GetColumnInfo(colID)
	if !ok {
		log.Panic("invalid column id", zap.Int64("columnID", colID))
	}
	return colInfo
}

// ForceGetColumnFlagType return the column flag type by ID
// Caller must ensure `colID` exists
func (ti *TableInfo) ForceGetColumnFlagType(colID int64) uint {
	info, exist := ti.GetColumnInfo(colID)
	if !exist {
		log.Panic("invalid column id", zap.Int64("columnID", colID))
	}
	return info.GetFlag()
}

// ForceGetColumnName return the column name by ID
// Caller must ensure `colID` exists
func (ti *TableInfo) ForceGetColumnName(colID int64) string {
	return ti.ForceGetColumnInfo(colID).Name.O
}

// ForceGetColumnIDByName return column ID by column name
// Caller must ensure `colID` exists
func (ti *TableInfo) ForceGetColumnIDByName(name string) int64 {
	colID, ok := ti.columnSchema.NameToColID[name]
	if !ok {
		log.Panic("invalid column name", zap.String("column", name))
	}
	return colID
}

func (ti *TableInfo) MustGetColumnOffsetByID(id int64) int {
	offset, ok := ti.columnSchema.ColumnsOffset[id]
	if !ok {
		log.Panic("invalid column id", zap.Int64("columnID", id))
	}
	return offset
}

// GetSchemaName returns the schema name of the table
func (ti *TableInfo) GetSchemaName() string {
	return ti.TableName.Schema
}

// GetTableName returns the table name of the table
func (ti *TableInfo) GetTableName() string {
	return ti.TableName.Table
}

// GetSchemaNamePtr returns the pointer to the schema name of the table
func (ti *TableInfo) GetSchemaNamePtr() *string {
	return &ti.TableName.Schema
}

// GetTableNamePtr returns the pointer to the table name of the table
func (ti *TableInfo) GetTableNamePtr() *string {
	return &ti.TableName.Table
}

// IsPartitionTable returns whether the table is partition table
func (ti *TableInfo) IsPartitionTable() bool {
	return ti.TableName.IsPartition
}

// IsView checks if TableInfo is a view.
func (t *TableInfo) IsView() bool {
	return t.View != nil
}

// IsSequence checks if TableInfo is a sequence.
func (t *TableInfo) IsSequence() bool {
	return t.Sequence != nil
}

// GetRowColInfos returns all column infos for rowcodec
func (ti *TableInfo) GetRowColInfos() ([]int64, map[int64]*types.FieldType, []rowcodec.ColInfo) {
	return ti.columnSchema.HandleColID, ti.columnSchema.RowColFieldTps, ti.columnSchema.RowColInfos
}

// GetFieldSlice returns the field types of all columns
func (ti *TableInfo) GetFieldSlice() []*types.FieldType {
	return ti.columnSchema.RowColFieldTpsSlice
}

// GetColInfosForRowChangedEvent return column infos for non-virtual columns
// The column order in the result is the same as the order in its corresponding RowChangedEvent
func (ti *TableInfo) GetColInfosForRowChangedEvent() []rowcodec.ColInfo {
	return *ti.columnSchema.RowColInfosWithoutVirtualCols
}

// IsColCDCVisible returns whether the col is visible for CDC
func IsColCDCVisible(col *model.ColumnInfo) bool {
	return !col.IsGenerated() || col.GeneratedStored
}

// HasVirtualColumns returns whether the table has virtual columns
func (ti *TableInfo) HasVirtualColumns() bool {
	return ti.columnSchema.VirtualColumnCount > 0
}

// GetIndex return the corresponding index by the given name.
func (ti *TableInfo) GetIndex(name string) *model.IndexInfo {
	for _, index := range ti.columnSchema.Indices {
		if index != nil && index.Name.L == strings.ToLower(name) {
			return index
		}
	}
	return nil
}

// IndexByName returns the index columns and offsets of the corresponding index by name
// Column is not case-sensitive on any platform, nor are column aliases.
// So we always match in lowercase.
// See also: https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html
func (ti *TableInfo) IndexByName(name string) ([]string, []int, bool) {
	index := ti.GetIndex(name)
	if index == nil {
		return nil, nil, false
	}
	names := make([]string, 0, len(index.Columns))
	offset := make([]int, 0, len(index.Columns))
	for _, col := range index.Columns {
		names = append(names, col.Name.O)
		offset = append(offset, col.Offset)
	}
	return names, offset, true
}

// OffsetsByNames returns the column offsets of the corresponding columns by names
// If any column does not exist, return false
// Column is not case-sensitive on any platform, nor are column aliases.
// So we always match in lowercase.
// See also: https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html
func (ti *TableInfo) OffsetsByNames(names []string) ([]int, bool) {
	// todo: optimize it
	columnOffsets := make(map[string]int, len(ti.columnSchema.Columns))
	for idx, col := range ti.columnSchema.Columns {
		if col != nil {
			columnOffsets[col.Name.L] = idx
		}
	}

	result := make([]int, 0, len(names))
	for _, col := range names {
		offset, ok := columnOffsets[strings.ToLower(col)]
		if !ok {
			return nil, false
		}
		result = append(result, offset)
	}

	return result, true
}

func (ti *TableInfo) HasPrimaryKey() bool {
	return ti.columnSchema.GetPkColInfo() != nil
}

func (ti *TableInfo) GetPkColInfo() *model.ColumnInfo {
	return ti.columnSchema.GetPkColInfo()
}

// GetPrimaryKeyColumnNames returns the primary key column names
func (ti *TableInfo) GetPrimaryKeyColumnNames() []string {
	var result []string
	if ti.columnSchema.PKIsHandle {
		result = append(result, ti.columnSchema.GetPkColInfo().Name.O)
		return result
	}

	indexInfo := ti.columnSchema.GetPrimaryKey()
	if indexInfo != nil {
		for _, col := range indexInfo.Columns {
			result = append(result, col.Name.O)
		}
	}
	return result
}

// IsHandleKey shows whether the column is selected as the handle key
func (ti *TableInfo) IsHandleKey(colID int64) bool {
	_, ok := ti.columnSchema.HandleKeyIDs[colID]
	return ok
}

func newTableInfo(schema string, table string, tableID int64, isPartition bool, columnSchema *columnSchema, tableInfo *model.TableInfo) *TableInfo {
	return &TableInfo{
		TableName: TableName{
			Schema:      schema,
			Table:       table,
			TableID:     tableID,
			IsPartition: isPartition,
			quotedName:  QuoteSchema(schema, table),
		},
		columnSchema: columnSchema,
		View:         tableInfo.View,
		Sequence:     tableInfo.Sequence,
		Charset:      tableInfo.Charset,
		Collate:      tableInfo.Collate,
		Comment:      tableInfo.Comment,
	}
}

func NewTableInfo(schemaName string, tableName string, tableID int64, isPartition bool, columnSchema *columnSchema, tableInfo *model.TableInfo) *TableInfo {
	ti := newTableInfo(schemaName, tableName, tableID, isPartition, columnSchema, tableInfo)

	// when this tableInfo is released, we need to cut down the reference count of the columnSchema
	// This function should be appeared when tableInfo is created as a pair.
	runtime.SetFinalizer(ti, func(ti *TableInfo) {
		GetSharedColumnSchemaStorage().tryReleaseColumnSchema(ti.columnSchema)
	})

	return ti
}

// WrapTableInfo creates a TableInfo from a model.TableInfo
func WrapTableInfo(schemaName string, info *model.TableInfo) *TableInfo {
	// search column schema object
	sharedColumnSchemaStorage := GetSharedColumnSchemaStorage()
	columnSchema := sharedColumnSchemaStorage.GetOrSetColumnSchema(info)
	return NewTableInfo(schemaName, info.Name.O, info.ID, info.GetPartitionInfo() != nil, columnSchema, info)
}

// NewTableInfo4Decoder is only used by the codec decoder for the test purpose,
// do not call this method on the production code.
func NewTableInfo4Decoder(schema string, tableInfo *model.TableInfo) *TableInfo {
	cs := newColumnSchema4Decoder(tableInfo)
	result := newTableInfo(schema, tableInfo.Name.O, tableInfo.ID, tableInfo.GetPartitionInfo() != nil, cs, tableInfo)
	result.InitPrivateFields()
	return result
}
