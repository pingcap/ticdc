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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

//go:generate msgp

// RedoLogType is the type of log
type RedoLogType int

// RedoLog defines the persistent structure of redo log
// since MsgPack do not support types that are defined in another package,
// more info https://github.com/tinylib/msgp/issues/158, https://github.com/tinylib/msgp/issues/149
// so define a RedoColumnValue, RedoDDLEvent instead of using the Column, DDLEvent
type RedoLog struct {
	RedoRow RedoDMLEvent `msg:"row"`
	RedoDDL RedoDDLEvent `msg:"ddl"`
	Type    RedoLogType  `msg:"type"`
}

// RedoDMLEvent represents the DML event used in RedoLog
type RedoDMLEvent struct {
	Row        *DMLEventInRedoLog `msg:"row"`
	Columns    []RedoColumnValue  `msg:"columns"`
	PreColumns []RedoColumnValue  `msg:"pre-columns"`
}

// RedoDDLEvent represents DDL event used in redo log persistent
type RedoDDLEvent struct {
	DDL       *DDLEventInRedoLog `msg:"ddl"`
	Type      byte               `msg:"type"`
	TableName common.TableName   `msg:"table-name"`
}

// DMLEventInRedoLog is used to store DMLEvent in redo log v2 format
type DMLEventInRedoLog struct {
	StartTs  uint64 `msg:"start-ts"`
	CommitTs uint64 `msg:"commit-ts"`

	// Table contains the table name and table ID.
	// NOTICE: We store the physical table ID here, not the logical table ID.
	Table *common.TableName `msg:"table"`

	Columns    []*RedoColumn `msg:"columns"`
	PreColumns []*RedoColumn `msg:"pre-columns"`

	// TODO: seems it's unused. Maybe we can remove it?
	IndexColumns [][]int `msg:"index-columns"`
}

// DDLEventInRedoLog is used to store DDLEvent in redo log v2 format
type DDLEventInRedoLog struct {
	StartTs  uint64 `msg:"start-ts"`
	CommitTs uint64 `msg:"commit-ts"`
	Query    string `msg:"query"`
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

const (
	// RedoLogTypeRow is row type of log
	RedoLogTypeRow RedoLogType = 1
	// RedoLogTypeDDL is ddl type of log
	RedoLogTypeDDL RedoLogType = 2
)

// ToRedoLog converts row changed event to redo log
func (r *DMLEvent) ToRedoLog() *RedoLog {
	r.FinishGetRow()
	row, valid := r.GetNextRow()
	r.FinishGetRow()

	redoLog := &RedoLog{
		RedoRow: RedoDMLEvent{
			Row: &DMLEventInRedoLog{
				StartTs:  r.StartTs,
				CommitTs: r.CommitTs,
				Table: &common.TableName{
					Schema:      r.TableInfo.GetSchemaName(),
					Table:       r.TableInfo.GetTableName(),
					TableID:     r.PhysicalTableID,
					IsPartition: r.TableInfo.IsPartitionTable(),
				},
				Columns:      nil,
				PreColumns:   nil,
				IndexColumns: nil,
			},
			PreColumns: nil,
			Columns:    nil,
		},
		Type: RedoLogTypeRow,
	}

	if valid {
		columnCount := len(r.TableInfo.GetColumns())
		columns := make([]*RedoColumn, 0, columnCount)
		switch row.RowType {
		case RowTypeInsert:
			redoLog.RedoRow.Columns = make([]RedoColumnValue, 0, columnCount)
		case RowTypeDelete:
			redoLog.RedoRow.PreColumns = make([]RedoColumnValue, 0, columnCount)
		case RowTypeUpdate:
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
				switch row.RowType {
				case RowTypeInsert:
					v := parseColumnValue(&row.Row, column, i)
					redoLog.RedoRow.Columns = append(redoLog.RedoRow.Columns, v)
				case RowTypeDelete:
					v := parseColumnValue(&row.PreRow, column, i)
					redoLog.RedoRow.PreColumns = append(redoLog.RedoRow.PreColumns, v)
				case RowTypeUpdate:
					v := parseColumnValue(&row.Row, column, i)
					redoLog.RedoRow.Columns = append(redoLog.RedoRow.Columns, v)
					v = parseColumnValue(&row.PreRow, column, i)
					redoLog.RedoRow.PreColumns = append(redoLog.RedoRow.PreColumns, v)
				default:
				}
			}
		}
		redoLog.RedoRow.Row.Columns = columns
		redoLog.RedoRow.Row.PreColumns = columns
	}

	return redoLog
}

// ToRedoLog converts ddl event to redo log
func (d *DDLEvent) ToRedoLog() *RedoLog {
	return &RedoLog{
		RedoDDL: RedoDDLEvent{
			DDL: &DDLEventInRedoLog{
				StartTs:  d.GetStartTs(),
				CommitTs: d.GetCommitTs(),
				Query:    d.Query,
			},
			Type:      d.Type,
			TableName: d.TableInfo.TableName,
		},
		Type: RedoLogTypeDDL,
	}
}

// GetCommitTs returns commit timestamp of the log event.
func (r *RedoLog) GetCommitTs() common.Ts {
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

func parseColumnValue(row *chunk.Row, column *model.ColumnInfo, i int) RedoColumnValue {
	v, err := common.FormatColVal(row, column, i)
	if err != nil {
		log.Panic("FormatColVal fail", zap.Error(err))
	}
	rrv := RedoColumnValue{Value: v}
	switch t := rrv.Value.(type) {
	case []byte:
		rrv.ValueIsEmptyBytes = len(t) == 0
	}
	rrv.Flag = uint64(column.GetFlag())
	return rrv
}
