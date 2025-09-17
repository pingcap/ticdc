// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE_2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package event

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// RedoLogType is the type of log
type RedoLogType int

type RedoLog struct {
	RedoRow *RedoDMLEvent `json:"redo_row"`
	RedoDDL *RedoDDLEvent `json:"redo_ddl"`
	Type    RedoLogType   `json:"log_type"`
}

// RedoDMLEvent represents the DML event used in RedoLog
type RedoDMLEvent struct {
	Row        *DMLEventInRedoLog `json:"row"`
	Columns    []RedoColumnValue  `json:"columns"`
	PreColumns []RedoColumnValue  `json:"pre_columns"`
}

// ToDMLEvent converts RowChangedEventInRedoLog to RowChangedEvent
func (r *RedoDMLEvent) ToDMLEvent() *DMLEvent {
	tidbTableInfo := &timodel.TableInfo{
		ID:   r.Row.Table.TableID,
		Name: ast.NewCIStr(r.Row.Table.Table),
	}
	// setIndexColumns(tidbTableInfo, r.Row.IndexColumns),
	tableInfo := common.WrapTableInfo(r.Row.Table.Schema, tidbTableInfo)

	tableInfo.TableName.IsPartition = r.Row.Table.IsPartition
	row := NewDMLEvent(common.NewDispatcherID(),
		r.Row.Table.TableID,
		r.Row.StartTs,
		r.Row.CommitTs,
		tableInfo,
	)
	return row
}

// RedoDDLEvent represents DDL event used in redo log persistent
type RedoDDLEvent struct {
	DDL       *DDLEvent        `json:"ddl"`
	TableName common.TableName `json:"table"`
}

// DMLEventInRedoLog is used to store DMLEvent in redo log v2 format
type DMLEventInRedoLog struct {
	StartTs  uint64 `json:"start_ts"`
	CommitTs uint64 `json:"commit_ts"`

	// Table contains the table name and table ID.
	// NOTICE: We store the physical table ID here, not the logical table ID.
	Table *common.TableName `json:"table"`

	Columns    []*RedoColumn `json:"columns"`
	PreColumns []*RedoColumn `json:"pre_columns"`

	IndexColumns [][]int `json:"index_columns"`
}

// RedoColumn is for column meta
type RedoColumn struct {
	Name      string `json:"name"`
	Type      byte   `json:"type"`
	Charset   string `json:"charset"`
	Collation string `json:"collation"`
	Flag      uint64 `json:"flag"`
}

// RedoColumnValue stores Column change
type RedoColumnValue struct {
	Value             interface{} `json:"value"`
	ValueIsEmptyBytes bool        `json:"value_is_empty_bytes"`
	Flag              uint64      `json:"flag"`
}

type RedoRowEvent struct {
	StartTs   uint64
	CommitTs  uint64
	TableInfo *common.TableInfo
	Event     RowChange
	Callback  func()
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
		redoLog.RedoRow.Row.Table = new(common.TableName)
		*redoLog.RedoRow.Row.Table = r.TableInfo.TableName
		redoLog.RedoRow.Row.IndexColumns = getIndexColumns(r.TableInfo)

		columnCount := len(r.TableInfo.GetColumns())
		columns := make([]*RedoColumn, 0, columnCount)
		switch r.Event.RowType {
		case common.RowTypeInsert:
			redoLog.RedoRow.Columns = make([]RedoColumnValue, 0, columnCount)
		case common.RowTypeDelete:
			redoLog.RedoRow.PreColumns = make([]RedoColumnValue, 0, columnCount)
		case common.RowTypeUpdate:
			redoLog.RedoRow.Columns = make([]RedoColumnValue, 0, columnCount)
			redoLog.RedoRow.PreColumns = make([]RedoColumnValue, 0, columnCount)
		default:
		}

		for i, column := range r.TableInfo.GetColumns() {
			if common.IsColCDCVisible(column) {
				columns = append(columns, &RedoColumn{
					Name:      column.Name.String(),
					Type:      column.GetType(),
					Charset:   column.GetCharset(),
					Collation: column.GetCollate(),
				})
				isHandleKey := r.TableInfo.IsHandleKey(column.ID)
				switch r.Event.RowType {
				case common.RowTypeInsert:
					v := parseColumnValue(&r.Event.Row, column, i, isHandleKey)
					redoLog.RedoRow.Columns = append(redoLog.RedoRow.Columns, v)
				case common.RowTypeDelete:
					v := parseColumnValue(&r.Event.PreRow, column, i, isHandleKey)
					redoLog.RedoRow.PreColumns = append(redoLog.RedoRow.PreColumns, v)
				case common.RowTypeUpdate:
					v := parseColumnValue(&r.Event.Row, column, i, isHandleKey)
					redoLog.RedoRow.Columns = append(redoLog.RedoRow.Columns, v)
					v = parseColumnValue(&r.Event.PreRow, column, i, isHandleKey)
					redoLog.RedoRow.PreColumns = append(redoLog.RedoRow.PreColumns, v)
				default:
				}
			}
		}
		switch r.Event.RowType {
		case common.RowTypeInsert:
			redoLog.RedoRow.Row.Columns = columns
		case common.RowTypeDelete:
			redoLog.RedoRow.Row.PreColumns = columns
		case common.RowTypeUpdate:
			redoLog.RedoRow.Row.Columns = columns
			redoLog.RedoRow.Row.PreColumns = columns
		}
	}
	return redoLog
}

// ToRedoLog converts ddl event to redo log
func (d *DDLEvent) ToRedoLog() *RedoLog {
	return &RedoLog{
		RedoDDL: &RedoDDLEvent{DDL: d},
		Type:    RedoLogTypeDDL,
	}
}

// GetCommitTs returns commit timestamp of the log event.
func (r *RedoLog) GetCommitTs() common.Ts {
	switch r.Type {
	case RedoLogTypeRow:
		return r.RedoRow.Row.CommitTs
	case RedoLogTypeDDL:
		return r.RedoDDL.DDL.GetCommitTs()
	default:
		log.Panic("Unexpected redo log type")
		return 0
	}
}

// IsDelete checks whether it's a deletion or not.
func (r RedoDMLEvent) IsDelete() bool {
	return len(r.Row.PreColumns) > 0 && len(r.Row.Columns) == 0
}

// IsUpdate checks whether it's a update or not.
func (r RedoDMLEvent) IsUpdate() bool {
	return len(r.Row.PreColumns) > 0 && len(r.Row.Columns) > 0
}

func parseColumnValue(row *chunk.Row, colInfo *timodel.ColumnInfo, i int, isHandleKey bool) RedoColumnValue {
	v := common.ExtractColVal(row, colInfo, i)
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
	var flag common.ColumnFlagType
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
func getIndexColumns(tableInfo *common.TableInfo) [][]int {
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

func getIndexInfo(indexColumns [][]int) []*timodel.IndexInfo {
	indexInfos := make([]*timodel.IndexInfo, len(indexColumns))
	// for _, index := range indexColumns {
	// 	indexInfo := &timodel.IndexInfo{}
	// 	for _, id := range index {

	// 	}
	// }
	// rowColumnsOffset := tableInfo.GetRowColumnsOffset()
	// for _, index := range tableInfo.GetIndexColumns() {
	// 	offsets := make([]int, 0, len(index))
	// 	for _, id := range index {
	// 		offsets = append(offsets, rowColumnsOffset[id])
	// 	}
	// 	indexColumns = append(indexColumns, offsets)
	// }
	return indexInfos
}
