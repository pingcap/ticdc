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
	"github.com/pingcap/ticdc/pkg/common"
)

// RedoLogType is the type of log
type RedoLogType int

// RedoLog defines the persistent structure of redo log
// since MsgPack do not support types that are defined in another package,
// more info https://github.com/tinylib/msgp/issues/158, https://github.com/tinylib/msgp/issues/149
// so define a RedoColumnValue, RedoDDLEvent instead of using the Column, DDLEvent
type RedoLog struct {
	RedoRow RedoRowChangedEvent `msg:"row"`
	RedoDDL RedoDDLEvent        `msg:"ddl"`
	Type    RedoLogType         `msg:"type"`
}

// RedoRowChangedEvent represents the DML event used in RedoLog
type RedoRowChangedEvent struct {
	Row        *RowChangedEventInRedoLog `msg:"row"`
	Columns    []RedoColumnValue              `msg:"columns"`
	PreColumns []RedoColumnValue              `msg:"pre-columns"`
}

// RedoDDLEvent represents DDL event used in redo log persistent
type RedoDDLEvent struct {
	DDL       *DDLEventInRedoLog `msg:"ddl"`
	Type      byte      `msg:"type"`
	TableName *common.TableName `msg:"table-name"`
}

// RowChangedEventInRedoLog is used to store RowChangedEvent in redo log v2 format
type RowChangedEventInRedoLog struct {
	StartTs  uint64 `msg:"start-ts"`
	CommitTs uint64 `msg:"commit-ts"`

	// Table contains the table name and table ID.
	// NOTICE: We store the physical table ID here, not the logical table ID.
	Table *common.TableName `msg:"table"`

	Columns      []*common.Column `msg:"columns"`
	PreColumns   []*common.Column `msg:"pre-columns"`

	// TODO: seems it's unused. Maybe we can remove it?
	IndexColumns [][]int   `msg:"index-columns"`
}

// DDLEventInRedoLog is used to store DDLEvent in redo log v2 format
type DDLEventInRedoLog struct {
	StartTs      uint64           `msg:"start-ts"`
	CommitTs     uint64           `msg:"commit-ts"`
	Query        string           `msg:"query"`
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
	RedoLogTypeRow RedoLogType = iota
	// RedoLogTypeDDL is ddl type of log
	RedoLogTypeDDL
)

// ToRedoLog converts row changed event to redo log
func (r *RowChangedEvent) ToRedoLog() *RedoLog {
	rowInRedoLog := &RowChangedEventInRedoLog{
		StartTs:  r.StartTs,
		CommitTs: r.CommitTs,
		Table: &common.TableName{
			Schema:      r.TableInfo.GetSchemaName(),
			Table:       r.TableInfo.GetTableName(),
			TableID:     r.GetTableID(),
			IsPartition: r.TableInfo.IsPartitionTable(),
		},
		Columns:      r.GetColumns(),
		PreColumns:   r.GetPreColumns(),
		IndexColumns: nil,
	}
	return &RedoLog{
		RedoRow: RedoRowChangedEvent{Row: rowInRedoLog},
		Type:    RedoLogTypeRow,
	}
}

// ToRedoLog converts ddl event to redo log
func (d *DDLEvent) ToRedoLog() *RedoLog {
	ddlInRedoLog := &DDLEventInRedoLog{
		StartTs: d.GetStartTs(),
		CommitTs: d.GetCommitTs(),
		Query: d.Query,
	}
	return &RedoLog{
		RedoDDL: RedoDDLEvent{DDL: ddlInRedoLog},
		Type:    RedoLogTypeDDL,
	}
}
