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
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	"github.com/pingcap/ticdc/pkg/integrity"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

//go:generate msgp
//
//msgp:ignore DDLEvent

type RowChangedEvent struct {
	PhysicalTableID int64

	StartTs  uint64
	CommitTs uint64

	// NOTICE: We probably store the logical ID inside TableInfo's TableName,
	// not the physical ID.
	// For normal table, there is only one ID, which is the physical ID.
	// AKA TIDB_TABLE_ID.
	// For partitioned table, there are two kinds of ID:
	// 1. TIDB_PARTITION_ID is the physical ID of the partition.
	// 2. TIDB_TABLE_ID is the logical ID of the table.
	// In general, we always use the physical ID to represent a table, but we
	// record the logical ID from the DDL event(job.BinlogInfo.TableInfo).
	// So be careful when using the TableInfo.
	TableInfo *common.TableInfo `msg:"-"`

	Columns    []*common.Column `msg:"columns"`
	PreColumns []*common.Column `msg:"pre-columns"`

	// ReplicatingTs is ts when a table starts replicating events to downstream.
	ReplicatingTs uint64 `msg:"replicating-ts"`

	// Checksum for the event, only not nil if the upstream TiDB enable the row level checksum
	// and TiCDC set the integrity check level to the correctness.
	Checksum *integrity.Checksum
}

// GetTableID returns the table ID of the event.
func (r *RowChangedEvent) GetTableID() int64 {
	return r.PhysicalTableID
}

// GetColumns returns the columns of the event
func (r *RowChangedEvent) GetColumns() []*common.Column {
	return r.Columns
}

// IsDelete returns true if the row is a delete event
func (r *RowChangedEvent) IsDelete() bool {
	return len(r.PreColumns) != 0 && len(r.Columns) == 0
}

// IsInsert returns true if the row is an insert event
func (r *RowChangedEvent) IsInsert() bool {
	return len(r.PreColumns) == 0 && len(r.Columns) != 0
}

// IsUpdate returns true if the row is an update event
func (r *RowChangedEvent) IsUpdate() bool {
	return len(r.PreColumns) != 0 && len(r.Columns) != 0
}

type MQRowEvent struct {
	Key      TopicPartitionKey
	RowEvent RowEvent
}

// TopicPartitionKey contains the topic and partition key of the message.
type TopicPartitionKey struct {
	Topic          string
	Partition      int32
	PartitionKey   string
	TotalPartition int32
}

type RowEvent struct {
	PhysicalTableID int64
	TableInfo       *common.TableInfo
	CommitTs        uint64
	Event           RowChange
	ColumnSelector  columnselector.Selector
	Callback        func()

	Checksum *integrity.Checksum
}

func (e *RowEvent) IsDelete() bool {
	return !e.Event.PreRow.IsEmpty() && e.Event.Row.IsEmpty()
}

func (e *RowEvent) IsUpdate() bool {
	return !e.Event.PreRow.IsEmpty() && !e.Event.Row.IsEmpty()
}

func (e *RowEvent) IsInsert() bool {
	return e.Event.PreRow.IsEmpty() && !e.Event.Row.IsEmpty()
}

func (e *RowEvent) GetRows() *chunk.Row {
	return &e.Event.Row
}

func (e *RowEvent) GetPreRows() *chunk.Row {
	return &e.Event.PreRow
}

func (t *RowEvent) GetTableID() int64 {
	return t.PhysicalTableID
}

// PrimaryKeyColumnNames return all primary key's name
// TODO: need a test for delete / insert / update event
func (e *RowEvent) PrimaryKeyColumnNames() []string {
	var result []string

	result = make([]string, 0)
	columns := e.TableInfo.GetColumns()
	for _, col := range columns {
		if col != nil && mysql.HasPriKeyFlag(col.GetFlag()) {
			result = append(result, col.Name.O)
		}
	}
	return result
}

// PrimaryKeyColumn return all primary key's indexes and column infos
func (e *RowEvent) PrimaryKeyColumn() ([]int, []*timodel.ColumnInfo) {
	infos := make([]*timodel.ColumnInfo, 0)
	index := make([]int, 0)
	columns := e.TableInfo.GetColumns()
	for i, col := range columns {
		if col != nil && mysql.HasPriKeyFlag(col.GetFlag()) {
			infos = append(infos, col)
			index = append(index, i)
		}
	}
	return index, infos
}
