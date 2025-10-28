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
	"errors"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

const (
	DDLEventVersion = 0
)

var _ Event = &DDLEvent{}

type DDLEvent struct {
	// Version is the version of the DDLEvent struct.
	Version      byte                `json:"version"`
	DispatcherID common.DispatcherID `json:"-"`
	Type         byte                `json:"type"`
	// SchemaID is from upstream job.SchemaID
	SchemaID int64 `json:"schema_id"`
	// TableID is from upstream job.TableID
	// TableID means different for different job types:
	// - for most ddl types which just involve a single table id, it is the table id of the table
	// - for ExchangeTablePartition, it is the table id of the normal table before exchange
	//   and it is one of of the partition ids after exchange
	// - for TruncateTable, it the table ID of the old table
	TableID    int64  `json:"table_id"`
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
	// the following two fields are just used for RenameTable,
	// they are the old schema/table name of the table
	ExtraSchemaName string            `json:"extra_schema_name"`
	ExtraTableName  string            `json:"extra_table_name"`
	Query           string            `json:"query"`
	TableInfo       *common.TableInfo `json:"-"`
	FinishedTs      uint64            `json:"finished_ts"`
	// The seq of the event. It is set by event service.
	Seq uint64 `json:"seq"`
	// The epoch of the event. It is set by event service.
	Epoch uint64 `json:"epoch"`
	// MultipleTableInfos holds information for multiple versions of a table.
	// The first entry always represents the current table information.
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

	TiDBOnly bool   `json:"tidb_only"`
	BDRMode  string `json:"bdr_mode"`
	Err      string `json:"err"`

	// Call when event flush is completed
	PostTxnFlushed []func() `json:"-"`
	// eventSize is the size of the event in bytes. It is set when it's unmarshaled.
	eventSize int64 `json:"-"`

	// for simple protocol
	IsBootstrap bool `msg:"-"`
	// NotSync is used to indicate whether the event should be synced to downstream.
	// If it is true, sink should not sync this event to downstream.
	// It is used for some special DDL events that do not need to be synced,
	// but only need to be sent to dispatcher to update some metadata.
	// For example, if a `TRUNCATE TABLE` DDL is filtered by event filter,
	// we don't need to sync it to downstream, but the DML events of the new truncated table
	// should be sent to downstream.
	// So we should send the `TRUNCATE TABLE` DDL event to dispatcher,
	// to ensure the new truncated table can be handled correctly.
	// If the DDL involves multiple tables, this field is not effective.
	// The multiple table DDL event will be handled by filtering querys and table infos.
	NotSync bool `msg:"not_sync"`
}

func (d *DDLEvent) String() string {
	return fmt.Sprintf("DDLEvent{Version: %d, DispatcherID: %s, Type: %d, SchemaID: %d, TableID: %d, SchemaName: %s, TableName: %s, ExtraSchemaName: %s, ExtraTableName: %s, Query: %s, TableInfo: %v, FinishedTs: %d, Seq: %d, BlockedTables: %v, NeedDroppedTables: %v, NeedAddedTables: %v, UpdatedSchemas: %v, TableNameChange: %v, TiDBOnly: %t, BDRMode: %s, Err: %s, eventSize: %d}",
		d.Version, d.DispatcherID.String(), d.Type, d.SchemaID, d.TableID, d.SchemaName, d.TableName, d.ExtraSchemaName, d.ExtraTableName, d.Query, d.TableInfo, d.FinishedTs, d.Seq, d.BlockedTables, d.NeedDroppedTables, d.NeedAddedTables, d.UpdatedSchemas, d.TableNameChange, d.TiDBOnly, d.BDRMode, d.Err, d.eventSize)
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

func (d *DDLEvent) GetError() error {
	if len(d.Err) == 0 {
		return nil
	}
	return errors.New(d.Err)
}

func (d *DDLEvent) GetCommitTs() common.Ts {
	return d.FinishedTs
}

func (d *DDLEvent) PostFlush() {
	for _, f := range d.PostTxnFlushed {
		f()
	}
}

func (d *DDLEvent) GetSchemaName() string {
	return d.SchemaName
}

func (d *DDLEvent) GetTableName() string {
	return d.TableName
}

func (d *DDLEvent) GetExtraSchemaName() string {
	return d.ExtraSchemaName
}

func (d *DDLEvent) GetExtraTableName() string {
	return d.ExtraTableName
}

func (d *DDLEvent) GetEvents() []*DDLEvent {
	// Some ddl event may be multi-events, we need to split it into multiple messages.
	// Such as rename table test.table1 to test.table10, test.table2 to test.table20
	switch model.ActionType(d.Type) {
	case model.ActionCreateTables, model.ActionRenameTables:
		events := make([]*DDLEvent, 0, len(d.MultipleTableInfos))
		queries, err := SplitQueries(d.Query)
		if err != nil {
			log.Panic("split queries failed", zap.Error(err))
		}
		if len(queries) != len(d.MultipleTableInfos) {
			log.Panic("queries length should be equal to multipleTableInfos length", zap.String("query", d.Query), zap.Any("multipleTableInfos", d.MultipleTableInfos))
		}

		t := model.ActionCreateTable
		if model.ActionType(d.Type) == model.ActionRenameTables {
			t = model.ActionRenameTable
		}
		for i, info := range d.MultipleTableInfos {
			event := &DDLEvent{
				Version:    d.Version,
				Type:       byte(t),
				SchemaName: info.GetSchemaName(),
				TableName:  info.GetTableName(),
				TableInfo:  info,
				Query:      queries[i],
				FinishedTs: d.FinishedTs,
			}
			if model.ActionType(d.Type) == model.ActionRenameTables {
				event.ExtraSchemaName = d.TableNameChange.DropName[i].SchemaName
				event.ExtraTableName = d.TableNameChange.DropName[i].TableName
			}
			events = append(events, event)
		}
		return events
	default:
	}
	return []*DDLEvent{d}
}

func (d *DDLEvent) GetSeq() uint64 {
	return d.Seq
}

func (d *DDLEvent) GetEpoch() uint64 {
	return d.Epoch
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
		return ""
	}
	return e.SchemaName
}

func (e *DDLEvent) GetDDLType() model.ActionType {
	return model.ActionType(e.Type)
}

func (t DDLEvent) Marshal() ([]byte, error) {
	// restData | dispatcherIDData | dispatcherIDDataSize | tableInfoData | tableInfoDataSize | multipleTableInfos | multipletableInfosDataSize |errorData | errorDataSize
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

	for _, info := range t.MultipleTableInfos {
		tableInfoData, err := info.Marshal()
		if err != nil {
			return nil, err
		}
		tableInfoDataSize := make([]byte, 8)
		binary.BigEndian.PutUint64(tableInfoDataSize, uint64(len(tableInfoData)))
		data = append(data, tableInfoData...)
		data = append(data, tableInfoDataSize...)
	}
	multipletableInfosDataSize := make([]byte, 8)
	binary.BigEndian.PutUint64(multipletableInfosDataSize, uint64(len(t.MultipleTableInfos)))
	data = append(data, multipletableInfosDataSize...)

	return data, nil
}

func (t *DDLEvent) Unmarshal(data []byte) error {
	// restData | dispatcherIDData | dispatcherIDDataSize | tableInfoData | tableInfoDataSize | multipleTableInfos | multipletableInfosDataSize
	t.eventSize = int64(len(data))
	end := len(data)
	multipletableInfosDataSize := binary.BigEndian.Uint64(data[end-8 : end])
	for i := 0; i < int(multipletableInfosDataSize); i++ {
		tableInfoDataSize := binary.BigEndian.Uint64(data[end-8 : end])
		tableInfoData := data[end-8-int(tableInfoDataSize) : end-8]
		info, err := common.UnmarshalJSONToTableInfo(tableInfoData)
		if err != nil {
			return err
		}
		t.MultipleTableInfos = append(t.MultipleTableInfos, info)
		end -= 8 + int(tableInfoDataSize)
	}
	end -= 8 + int(multipletableInfosDataSize)
	tableInfoDataSize := binary.BigEndian.Uint64(data[end-8 : end])
	var err error
	if tableInfoDataSize > 0 {
		tableInfoData := data[end-8-int(tableInfoDataSize) : end-8]
		t.TableInfo, err = common.UnmarshalJSONToTableInfo(tableInfoData)
		if err != nil {
			return err
		}
	}
	end -= 8 + int(tableInfoDataSize)
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

	for _, info := range t.MultipleTableInfos {
		info.InitPrivateFields()
	}
	if t.TableInfo != nil {
		t.TableInfo.InitPrivateFields()
	}

	return nil
}

func (t *DDLEvent) GetSize() int64 {
	return t.eventSize
}

func (t *DDLEvent) IsPaused() bool {
	return false
}

func (t *DDLEvent) Len() int32 {
	return 1
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
