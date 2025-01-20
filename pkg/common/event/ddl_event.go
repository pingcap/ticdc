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
	"encoding/binary"
	"encoding/json"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

const (
	DDLEventVersion = 0
)

type DDLEvent struct {
	// Version is the version of the DDLEvent struct.
	Version      byte                `json:"version"`
	DispatcherID common.DispatcherID `json:"-"`
	Type         byte                `json:"type"`
	// SchemaID means different for different job types:
	// - ExchangeTablePartition: db id of non-partitioned table
	SchemaID int64 `json:"schema_id"`
	// TableID means different for different job types:
	// - ExchangeTablePartition: non-partitioned table id
	TableID        int64             `json:"table_id"`
	SchemaName     string            `json:"schema_name"`
	TableName      string            `json:"table_name"`
	PrevSchemaName string            `json:"prev_schema_name"`
	PrevTableName  string            `json:"prev_table_name"`
	Query          string            `json:"query"`
	TableInfo      *common.TableInfo `json:"-"`
	FinishedTs     uint64            `json:"finished_ts"`
	// The seq of the event. It is set by event service.
	Seq uint64 `json:"seq"`
	// State is the state of sender when sending this event.
	State EventSenderState `json:"state"`
	// TODO: just here for compile, may be changed later
	MultipleTableInfos []*common.TableInfo `json:"-"`

	BlockedTables     *InfluencedTables `json:"blocked_tables"`
	NeedDroppedTables *InfluencedTables `json:"need_dropped_tables"`
	NeedAddedTables   []Table           `json:"need_added_tables"`

	// Only set when tables moves between databases
	UpdatedSchemas []SchemaIDChange `json:"updated_schemas"`

	// DDLs which may change table name:
	//   Create Table
	//   Create Tables
	//   Drop Table
	//   Rename Table
	//   Rename Tables
	//   Drop Schema
	//   Recover Table
	TableNameChange *TableNameChange `json:"table_name_change"`

	TiDBOnly bool `json:"tidb_only"`
	// Call when event flush is completed
	PostTxnFlushed []func() `json:"-"`
	// eventSize is the size of the event in bytes. It is set when it's unmarshaled.
	eventSize int64 `json:"-"`
}

func (d *DDLEvent) GetType() int {
	return TypeDDLEvent
}

func (d *DDLEvent) GetDispatcherID() common.DispatcherID {
	return d.DispatcherID
}

func (d *DDLEvent) GetStartTs() common.Ts {
	return 0
}

func (d *DDLEvent) GetCommitTs() common.Ts {
	return d.FinishedTs
}

func (d *DDLEvent) PostFlush() {
	for _, f := range d.PostTxnFlushed {
		f()
	}
}

func (d *DDLEvent) GetCurrentSchemaName() string {
	return d.SchemaName
}

func (d *DDLEvent) GetCurrentTableName() string {
	return d.TableName
}

func (d *DDLEvent) GetPrevSchemaName() string {
	return d.PrevSchemaName
}

func (d *DDLEvent) GetPrevTableName() string {
	return d.PrevTableName
}

func (d *DDLEvent) GetEvents() []*DDLEvent {
	// Some ddl event may be multi-events, we need to split it into multiple messages.
	// Such as rename table test.table1 to test.table10, test.table2 to test.table20
	switch model.ActionType(d.Type) {
	case model.ActionExchangeTablePartition:
		return []*DDLEvent{
			// partition table before exchange
			{
				Version: d.Version,
				Type:    d.Type,
				// SchemaID:   d.SchemaID,
				// TableID:    d.TableID,
				SchemaName: d.SchemaName,
				TableName:  d.TableName,
				Query:      d.Query,
				FinishedTs: d.FinishedTs,
			},
			// normal table before exchange(TODO: this may be wrong)
			{
				Version: d.Version,
				Type:    d.Type,
				// SchemaID:   d.TableInfo.SchemaID,
				// TableID:    d.TableInfo.TableName.TableID,
				SchemaName: d.PrevSchemaName,
				TableName:  d.PrevTableName,
				Query:      d.Query,
				FinishedTs: d.FinishedTs,
			},
		}
	case model.ActionCreateTables:
		events := make([]*DDLEvent, 0, len(d.TableNameChange.AddName))
		// TODO: don't use ; to split query, please use parser
		queries := strings.Split(d.Query, ";")
		if len(queries) != len(d.TableNameChange.AddName) {
			log.Panic("queries length should be equal to addName length", zap.String("query", d.Query), zap.Any("addName", d.TableNameChange.AddName))
		}
		for i, schemaAndTable := range d.TableNameChange.AddName {
			events = append(events, &DDLEvent{
				Version:    d.Version,
				Type:       d.Type,
				SchemaName: schemaAndTable.SchemaName,
				TableName:  schemaAndTable.TableName,
				Query:      queries[i],
				FinishedTs: d.FinishedTs,
			})
		}
		return events
	default:
	}
	return []*DDLEvent{d}
}

func (d *DDLEvent) GetSeq() uint64 {
	return d.Seq
}

func (d *DDLEvent) ClearPostFlushFunc() {
	d.PostTxnFlushed = d.PostTxnFlushed[:0]
}

func (d *DDLEvent) AddPostFlushFunc(f func()) {
	d.PostTxnFlushed = append(d.PostTxnFlushed, f)
}

func (d *DDLEvent) PushFrontFlushFunc(f func()) {
	d.PostTxnFlushed = append([]func(){f}, d.PostTxnFlushed...)
}

func (e *DDLEvent) GetBlockedTables() *InfluencedTables {
	return e.BlockedTables
}

func (e *DDLEvent) GetNeedDroppedTables() *InfluencedTables {
	return e.NeedDroppedTables
}

func (e *DDLEvent) GetNeedAddedTables() []Table {
	return e.NeedAddedTables
}

func (e *DDLEvent) GetUpdatedSchemas() []SchemaIDChange {
	return e.UpdatedSchemas
}

func (e *DDLEvent) GetDDLQuery() string {
	if e == nil {
		log.Error("DDLEvent is nil, should not happened in production env", zap.Any("event", e))
		return ""
	}
	return e.Query
}

func (e *DDLEvent) GetDDLSchemaName() string {
	if e == nil {
		return "" // 要报错的
	}
	return e.SchemaName
}

func (e *DDLEvent) GetDDLType() model.ActionType {
	return model.ActionType(e.Type)
}

func (t DDLEvent) Marshal() ([]byte, error) {
	// restData | dispatcherIDData | dispatcherIDDataSize | tableInfoData | tableInfoDataSize
	data, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	dispatcherIDData := t.DispatcherID.Marshal()
	dispatcherIDDataSize := make([]byte, 8)
	binary.BigEndian.PutUint64(dispatcherIDDataSize, uint64(len(dispatcherIDData)))
	data = append(data, dispatcherIDData...)
	data = append(data, dispatcherIDDataSize...)

	if t.TableInfo != nil {
		tableInfoData, err := t.TableInfo.Marshal()
		if err != nil {
			return nil, err
		}
		tableInfoDataSize := make([]byte, 8)
		binary.BigEndian.PutUint64(tableInfoDataSize, uint64(len(tableInfoData)))
		data = append(data, tableInfoData...)
		data = append(data, tableInfoDataSize...)
	} else {
		tableInfoDataSize := make([]byte, 8)
		binary.BigEndian.PutUint64(tableInfoDataSize, 0)
		data = append(data, tableInfoDataSize...)
	}
	return data, nil
}

func (t *DDLEvent) Unmarshal(data []byte) error {
	// restData | dispatcherIDData | dispatcherIDDataSize | tableInfoData | tableInfoDataSize
	t.eventSize = int64(len(data))
	tableInfoDataSize := binary.BigEndian.Uint64(data[len(data)-8:])
	var err error
	end := len(data) - 8 - int(tableInfoDataSize)
	if tableInfoDataSize > 0 {
		tableInfoData := data[len(data)-8-int(tableInfoDataSize) : len(data)-8]
		t.TableInfo, err = common.UnmarshalJSONToTableInfo(tableInfoData)
		if err != nil {
			return err
		}
	}
	dispatcherIDDatSize := binary.BigEndian.Uint64(data[end-8 : end])
	dispatcherIDData := data[end-8-int(dispatcherIDDatSize) : end-8]
	err = t.DispatcherID.Unmarshal(dispatcherIDData)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data[:end-8-int(dispatcherIDDatSize)], t)
	if err != nil {
		return err
	}
	return nil
}

func (t *DDLEvent) GetSize() int64 {
	return t.eventSize
}

func (t *DDLEvent) IsPaused() bool {
	return t.State.IsPaused()
}

type SchemaTableName struct {
	SchemaName string
	TableName  string
}

type DB struct {
	SchemaID   int64
	SchemaName string
}

// TableNameChange will record each ddl change of the table name.
// Each TableNameChange is related to a ddl event
type TableNameChange struct {
	AddName          []SchemaTableName
	DropName         []SchemaTableName
	DropDatabaseName string
}
