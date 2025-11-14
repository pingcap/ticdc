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

package event

import (
	"fmt"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

// RedoLogType is the type of log
//
//go:generate msgp
type RedoLogType int

// RedoLog defines the persistent structure of redo log
// since MsgPack do not support types that are defined in another package,
// more info https://github.com/tinylib/msgp/issues/158, https://github.com/tinylib/msgp/issues/149
// so define a RedoColumnValue, RedoDDLEvent instead of using the Column, DDLEvent
type RedoLog struct {
	RedoRow *RedoDMLEvent `msg:"row"`
	RedoDDL *RedoDDLEvent `msg:"ddl"`
	Type    RedoLogType   `msg:"type"`
}

// RedoDMLEvent represents the DML event used in RedoLog
type RedoDMLEvent struct {
	Row        *DMLEventInRedoLog `msg:"row"`
	Columns    []RedoColumnValue  `msg:"columns"`
	PreColumns []RedoColumnValue  `msg:"pre-columns"`
}

// RedoDDLEvent represents DDL event used in redo log persistent
type RedoDDLEvent struct {
	DDL              *DDLEventInRedoLog   `msg:"ddl"`
	Type             byte                 `msg:"type"`
	TableName        commonType.TableName `msg:"table-name"`
	TableSchemaStore *TableSchemaStore    `msg:"table-schema-store"`
}

// DMLEventInRedoLog is used to store DMLEvent in redo log v2 format
type DMLEventInRedoLog struct {
	StartTs  uint64 `msg:"start-ts"`
	CommitTs uint64 `msg:"commit-ts"`

	// Table contains the table name and table ID.
	// NOTICE: We store the physical table ID here, not the logical table ID.
	Table *commonType.TableName `msg:"table"`

	Columns    []*RedoColumn `msg:"columns"`
	PreColumns []*RedoColumn `msg:"pre-columns"`

	IndexColumns [][]int `msg:"index-columns"`
}

// DDLEventInRedoLog is used to store DDLEvent in redo log v2 format
type DDLEventInRedoLog struct {
	StartTs           uint64            `msg:"start-ts"`
	CommitTs          uint64            `msg:"commit-ts"`
	Query             string            `msg:"query"`
	BlockTables       *InfluencedTables `msg:"block-tables"`
	NeedDroppedTables *InfluencedTables `msg:"need-dropped-tables"`
	NeedAddedTables   []Table           `msg:"need_added_tables"`
}

// RedoColumn is for column meta
type RedoColumn struct {
	Name      string `msg:"name"`
	Type      byte   `msg:"type"`
	Charset   string `msg:"charset"`
	Collation string `msg:"collation"`
}

// RedoColumnValue stores Column change
type RedoColumnValue struct {
	// Fields from Column and can't be marshaled directly in Column.
	Value interface{} `msg:"column"`
	// msgp transforms empty byte slice into nil, PTAL msgp#247.
	ValueIsEmptyBytes bool   `msg:"value-is-empty-bytes"`
	Flag              uint64 `msg:"flag"`
}

//msgp:ignore RedoRowEvent
type RedoRowEvent struct {
	StartTs         uint64
	CommitTs        uint64
	PhysicalTableID int64
	TableInfo       *commonType.TableInfo
	Event           RowChange
	Callback        func()
}

const (
	// RedoLogTypeRow is row type of log
	RedoLogTypeRow RedoLogType = 1
	// RedoLogTypeDDL is ddl type of log
	RedoLogTypeDDL RedoLogType = 2
)

func (r *RedoRowEvent) PostFlush() {
	if r.Callback != nil {
		r.Callback()
	}
}

func (r *RedoRowEvent) ToRedoLog() *RedoLog {
	startTs := r.StartTs
	commitTs := r.CommitTs
	redoLog := &RedoLog{
		RedoRow: &RedoDMLEvent{
			Row: &DMLEventInRedoLog{
				StartTs:      startTs,
				CommitTs:     commitTs,
				Table:        nil,
				Columns:      nil,
				PreColumns:   nil,
				IndexColumns: nil,
			},
			PreColumns: nil,
			Columns:    nil,
		},
		Type: RedoLogTypeRow,
	}
	if r.TableInfo != nil {
		redoLog.RedoRow.Row.Table = &commonType.TableName{
			Schema:      r.TableInfo.TableName.Schema,
			Table:       r.TableInfo.TableName.Table,
			TableID:     r.PhysicalTableID,
			IsPartition: r.TableInfo.TableName.IsPartition,
		}
		redoLog.RedoRow.Row.IndexColumns = getIndexColumns(r.TableInfo)

		columnCount := len(r.TableInfo.GetColumns())
		columns := make([]*RedoColumn, 0, columnCount)
		switch r.Event.RowType {
		case commonType.RowTypeInsert:
			redoLog.RedoRow.Columns = make([]RedoColumnValue, 0, columnCount)
		case commonType.RowTypeDelete:
			redoLog.RedoRow.PreColumns = make([]RedoColumnValue, 0, columnCount)
		case commonType.RowTypeUpdate:
			redoLog.RedoRow.Columns = make([]RedoColumnValue, 0, columnCount)
			redoLog.RedoRow.PreColumns = make([]RedoColumnValue, 0, columnCount)
		default:
		}

		for i, column := range r.TableInfo.GetColumns() {
			if commonType.IsColCDCVisible(column) {
				columns = append(columns, &RedoColumn{
					Name:      column.Name.String(),
					Type:      column.GetType(),
					Charset:   column.GetCharset(),
					Collation: column.GetCollate(),
				})
				isHandleKey := r.TableInfo.IsHandleKey(column.ID)
				switch r.Event.RowType {
				case commonType.RowTypeInsert:
					v := parseColumnValue(&r.Event.Row, column, i, isHandleKey)
					redoLog.RedoRow.Columns = append(redoLog.RedoRow.Columns, v)
				case commonType.RowTypeDelete:
					v := parseColumnValue(&r.Event.PreRow, column, i, isHandleKey)
					redoLog.RedoRow.PreColumns = append(redoLog.RedoRow.PreColumns, v)
				case commonType.RowTypeUpdate:
					v := parseColumnValue(&r.Event.Row, column, i, isHandleKey)
					redoLog.RedoRow.Columns = append(redoLog.RedoRow.Columns, v)
					v = parseColumnValue(&r.Event.PreRow, column, i, isHandleKey)
					redoLog.RedoRow.PreColumns = append(redoLog.RedoRow.PreColumns, v)
				default:
				}
			}
		}
		switch r.Event.RowType {
		case commonType.RowTypeInsert:
			redoLog.RedoRow.Row.Columns = columns
		case commonType.RowTypeDelete:
			redoLog.RedoRow.Row.PreColumns = columns
		case commonType.RowTypeUpdate:
			redoLog.RedoRow.Row.Columns = columns
			redoLog.RedoRow.Row.PreColumns = columns
		}
	}
	return redoLog
}

// ToRedoLog converts ddl event to redo log
func (d *DDLEvent) ToRedoLog() *RedoLog {
	redoLog := &RedoLog{
		RedoDDL: &RedoDDLEvent{
			DDL: &DDLEventInRedoLog{
				StartTs:           d.GetStartTs(),
				CommitTs:          d.GetCommitTs(),
				Query:             d.Query,
				BlockTables:       d.BlockedTables,
				NeedDroppedTables: d.NeedDroppedTables,
				NeedAddedTables:   d.NeedAddedTables,
			},
			Type: d.Type,
		},
		Type: RedoLogTypeDDL,
	}
	if d.TableInfo != nil {
		redoLog.RedoDDL.TableName = d.TableInfo.TableName
	}

	return redoLog
}

// GetCommitTs returns commit timestamp of the log event.
func (r *RedoLog) GetCommitTs() commonType.Ts {
	switch r.Type {
	case RedoLogTypeRow:
		return r.RedoRow.Row.CommitTs
	case RedoLogTypeDDL:
		return r.RedoDDL.DDL.CommitTs
	default:
		log.Panic("Unexpected redo log type")
		return 0
	}
}

// IsInsert checks whether it's a insert or not.
func (r RedoDMLEvent) IsInsert() bool {
	return len(r.Row.Columns) > 0 && len(r.Row.PreColumns) == 0
}

// IsDelete checks whether it's a deletion or not.
func (r RedoDMLEvent) IsDelete() bool {
	return len(r.Row.PreColumns) > 0 && len(r.Row.Columns) == 0
}

// IsUpdate checks whether it's a update or not.
func (r RedoDMLEvent) IsUpdate() bool {
	return len(r.Row.PreColumns) > 0 && len(r.Row.Columns) > 0
}

func (r *RedoDMLEvent) ToDMLEvent() *DMLEvent {
	if len(r.Row.PreColumns) != len(r.PreColumns) || len(r.Row.Columns) != len(r.Columns) {
		log.Panic("decode redo dmlevent failed",
			zap.Any("preColumns", r.Row.PreColumns), zap.Any("preColumnsValue", r.PreColumns),
			zap.Any("columns", r.Row.Columns), zap.Any("columnsValue", r.Columns),
		)
	}
	tidbTableInfo := &timodel.TableInfo{
		ID:   r.Row.Table.TableID,
		Name: ast.NewCIStr(r.Row.Table.Table),
	}
	rawCols := r.Row.Columns
	rawColsValue := r.Columns
	if r.IsDelete() {
		rawCols = r.Row.PreColumns
		rawColsValue = r.PreColumns
	}
	for idx, col := range rawCols {
		colInfo := &timodel.ColumnInfo{
			ID:    int64(idx),
			Name:  ast.NewCIStr(col.Name),
			State: timodel.StatePublic,
		}
		colInfo.SetType(col.Type)
		colInfo.SetCharset(col.Charset)
		colInfo.SetCollate(col.Collation)
		flag := commonType.ColumnFlagType(rawColsValue[idx].Flag)
		// if flag.IsHandleKey() {
		// }
		// if flag.IsBinary(){
		// }
		// if flag.IsGeneratedColumn() {
		// }
		if flag.IsPrimaryKey() {
			colInfo.AddFlag(mysql.PriKeyFlag)
		}
		if flag.IsUniqueKey() {
			colInfo.AddFlag(mysql.UniqueKeyFlag)
		}
		if !flag.IsNullable() {
			colInfo.AddFlag(mysql.NotNullFlag)
		}
		if flag.IsMultipleKey() {
			colInfo.AddFlag(mysql.MultipleKeyFlag)
		}
		if flag.IsUnsigned() {
			colInfo.AddFlag(mysql.UnsignedFlag)
		}
		tidbTableInfo.Columns = append(tidbTableInfo.Columns, colInfo)
	}
	for i, index := range r.Row.IndexColumns {
		indexInfo := &timodel.IndexInfo{
			Name:  ast.NewCIStr(fmt.Sprintf("index_%d", i)),
			State: timodel.StatePublic,
		}
		firstCol := tidbTableInfo.Columns[index[0]]
		if mysql.HasPriKeyFlag(firstCol.GetFlag()) || mysql.HasUniKeyFlag(firstCol.GetFlag()) {
			indexInfo.Unique = true
		}
		isPrimary := true
		for _, id := range index {
			col := tidbTableInfo.Columns[id]
			// When only all columns in the index are primary key, then the index is primary key.
			if col == nil || !mysql.HasPriKeyFlag(firstCol.GetFlag()) {
				isPrimary = false
			}
			indexInfo.Columns = append(indexInfo.Columns, &timodel.IndexColumn{
				Name:   ast.NewCIStr(rawCols[id].Name),
				Offset: id,
			})
		}
		indexInfo.Primary = isPrimary
		tidbTableInfo.Indices = append(tidbTableInfo.Indices, indexInfo)
	}
	event := &DMLEvent{
		TableInfo:       commonType.NewTableInfo4Decoder(r.Row.Table.Schema, tidbTableInfo),
		CommitTs:        r.Row.CommitTs,
		StartTs:         r.Row.StartTs,
		Length:          1,
		PhysicalTableID: r.Row.Table.TableID,
	}

	chk := chunk.NewChunkFromPoolWithCapacity(event.TableInfo.GetFieldSlice(), chunk.InitialCapacity)
	event.AddPostFlushFunc(func() {
		chk.Destroy(chunk.InitialCapacity, event.TableInfo.GetFieldSlice())
	})
	columns := event.TableInfo.GetColumns()
	if r.IsDelete() {
		collectAllColumnsValue(r.PreColumns, columns, chk)
		event.RowTypes = append(event.RowTypes, commonType.RowTypeDelete)
	} else if r.IsUpdate() {
		collectAllColumnsValue(r.PreColumns, columns, chk)
		collectAllColumnsValue(r.Columns, columns, chk)
		// FIXME: exclude columns with same value
		event.RowTypes = append(event.RowTypes, commonType.RowTypeUpdate, commonType.RowTypeUpdate)
	} else if r.IsInsert() {
		collectAllColumnsValue(r.Columns, columns, chk)
		event.RowTypes = append(event.RowTypes, commonType.RowTypeInsert)
	} else {
		log.Panic("unknown event type for the DML event")
	}
	event.Rows = chk
	return event
}

func (r *RedoDDLEvent) ToDDLEvent() *DDLEvent {
	return &DDLEvent{
		TableInfo: &commonType.TableInfo{
			TableName: r.TableName,
		},
		Query:             r.DDL.Query,
		Type:              r.Type,
		SchemaName:        r.TableName.Schema,
		TableName:         r.TableName.Table,
		FinishedTs:        r.DDL.CommitTs,
		BlockedTables:     r.DDL.BlockTables,
		NeedDroppedTables: r.DDL.NeedDroppedTables,
	}
}

func (r *RedoDDLEvent) SetTableSchemaStore(tableSchemaStore *TableSchemaStore) {
	if r.DDL.BlockTables != nil && r.DDL.BlockTables.InfluenceType != InfluenceTypeNormal {
		r.TableSchemaStore = tableSchemaStore
	}
}

func parseColumnValue(row *chunk.Row, colInfo *timodel.ColumnInfo, i int, isHandleKey bool) RedoColumnValue {
	v := commonType.ExtractColVal(row, colInfo, i)
	rrv := RedoColumnValue{Value: v}
	switch t := rrv.Value.(type) {
	case []byte:
		rrv.ValueIsEmptyBytes = len(t) == 0
	}
	// FIXME: Use tidb column flag
	rrv.Flag = convertFlag(colInfo, isHandleKey)
	return rrv
}

// For compatibility
func convertFlag(colInfo *timodel.ColumnInfo, isHandleKey bool) uint64 {
	var flag commonType.ColumnFlagType
	if isHandleKey {
		flag.SetIsHandleKey()
	}
	if colInfo.GetCharset() == "binary" {
		flag.SetIsBinary()
	}
	if colInfo.IsGenerated() {
		flag.SetIsGeneratedColumn()
	}
	if mysql.HasPriKeyFlag(colInfo.GetFlag()) {
		flag.SetIsPrimaryKey()
	}
	if mysql.HasUniKeyFlag(colInfo.GetFlag()) {
		flag.SetIsUniqueKey()
	}
	if !mysql.HasNotNullFlag(colInfo.GetFlag()) {
		flag.SetIsNullable()
	}
	if mysql.HasMultipleKeyFlag(colInfo.GetFlag()) {
		flag.SetIsMultipleKey()
	}
	if mysql.HasUnsignedFlag(colInfo.GetFlag()) {
		flag.SetIsUnsigned()
	}
	return uint64(flag)
}

// For compatibility
func getIndexColumns(tableInfo *commonType.TableInfo) [][]int {
	indexColumns := make([][]int, 0, len(tableInfo.GetIndexColumns()))
	rowColumnsOffset := tableInfo.GetRowColumnsOffset()
	for _, index := range tableInfo.GetIndexColumns() {
		offsets := make([]int, 0, len(index))
		for _, id := range index {
			offsets = append(offsets, rowColumnsOffset[id])
		}
		indexColumns = append(indexColumns, offsets)
	}
	return indexColumns
}

func collectAllColumnsValue(data []RedoColumnValue, columns []*timodel.ColumnInfo, chk *chunk.Chunk) {
	for idx := range data {
		appendCol2Chunk(idx, data[idx].Value, columns[idx].FieldType, chk)
	}
}

func appendCol2Chunk(idx int, raw interface{}, ft tiTypes.FieldType, chk *chunk.Chunk) {
	if raw == nil {
		chk.AppendNull(idx)
		return
	}
	switch ft.GetType() {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			chk.AppendUint64(idx, raw.(uint64))
			return
		}
		chk.AppendInt64(idx, raw.(int64))
	case mysql.TypeYear:
		chk.AppendInt64(idx, raw.(int64))
	case mysql.TypeFloat:
		chk.AppendFloat32(idx, raw.(float32))
	case mysql.TypeDouble:
		chk.AppendFloat64(idx, raw.(float64))
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		switch val := raw.(type) {
		case string:
			chk.AppendBytes(idx, []byte(val))
		case []byte:
			chk.AppendBytes(idx, val)
		}
	case mysql.TypeNewDecimal:
		chk.AppendMyDecimal(idx, raw.(*tiTypes.MyDecimal))
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		chk.AppendTime(idx, raw.(tiTypes.Time))
	case mysql.TypeDuration:
		chk.AppendDuration(idx, raw.(tiTypes.Duration))
	case mysql.TypeEnum:
		chk.AppendEnum(idx, raw.(tiTypes.Enum))
	case mysql.TypeSet:
		chk.AppendSet(idx, raw.(tiTypes.Set))
	case mysql.TypeBit:
		chk.AppendBytes(idx, raw.(tiTypes.BinaryLiteral))
	case mysql.TypeJSON:
		chk.AppendJSON(idx, raw.(tiTypes.BinaryJSON))
	case mysql.TypeTiDBVectorFloat32:
		chk.AppendVectorFloat32(idx, raw.(tiTypes.VectorFloat32))
	default:
		log.Panic("unknown column type", zap.Any("type", ft.GetType()), zap.Any("raw", raw))
	}
}
