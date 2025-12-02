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
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
)

//go:generate msgp

type Event interface {
	GetType() int
	GetSeq() uint64
	GetEpoch() uint64
	GetDispatcherID() common.DispatcherID
	GetCommitTs() common.Ts
	GetStartTs() common.Ts
	// GetSize returns the approximate size of the event in bytes.
	// It's used for memory control and monitoring.
	GetSize() int64
	IsPaused() bool
	// GetLen returns the number of rows in the event.
	Len() int32
}

// FlushableEvent is an event that can be flushed to downstream by a dispatcher.
type FlushableEvent interface {
	Event
	PostFlush()
	AddPostFlushFunc(func())
	PushFrontFlushFunc(f func())
	ClearPostFlushFunc()
}

// BlockEvent is an event that may block the dispatcher.
// It could be a ddl event or a sync point event.
type BlockEvent interface {
	FlushableEvent
	GetBlockedTables() *InfluencedTables
	GetNeedDroppedTables() *InfluencedTables
	GetNeedAddedTables() []Table
	GetUpdatedSchemas() []SchemaIDChange
}

const (
	// DMLEvent is the event type of a transaction.
	TypeDMLEvent = 0
	// BatchDMLEvent is the event type of a batch transactions.
	TypeBatchDMLEvent = 1
	// DDLEvent is the event type of a DDL.
	TypeDDLEvent = 2
	// ResolvedEvent is the event type of a resolvedTs.
	TypeResolvedEvent = 3
	// BatchResolvedTs is the event type of a batch resolvedTs.
	TypeBatchResolvedEvent = 4
	// SyncPointEvent is the event type of a sync point.
	TypeSyncPointEvent = 5
	// HandshakeEvent is the event type to indicate the start of a new event stream.
	TypeHandshakeEvent = 7
	// TypeReadyEvent is the event type to indicate the event service is ready to send events.
	TypeReadyEvent = 6
	// TypeNotReusableEvent is the event type to indicate the event service has no data for reuse.
	TypeNotReusableEvent = 8
	// TypeDropEvent is the event type to indicate an event has been dropped.
	TypeDropEvent = 9
	// TypeCongestionControl is the event type for congestion control messages.
	TypeCongestionControl = 10
	// TypeDispatcherHeartbeat is the event type for dispatcher heartbeat messages.
	TypeDispatcherHeartbeat = 11
	// TypeDispatcherHeartbeatResponse is the event type for dispatcher heartbeat response messages.
	TypeDispatcherHeartbeatResponse = 12
)

func TypeToString(t int) string {
	switch t {
	case TypeDMLEvent:
		return "DMLEvent"
	case TypeBatchDMLEvent:
		return "BatchDMLEvent"
	case TypeDDLEvent:
		return "DDLEvent"
	case TypeResolvedEvent:
		return "ResolvedEvent"
	case TypeBatchResolvedEvent:
		return "BatchResolvedEvent"
	case TypeSyncPointEvent:
		return "SyncPointEvent"
	case TypeHandshakeEvent:
		return "HandshakeEvent"
	case TypeReadyEvent:
		return "ReadyEvent"
	case TypeNotReusableEvent:
		return "NotReusableEvent"
	case TypeDropEvent:
		return "DropEvent"
	case TypeCongestionControl:
		return "CongestionControl"
	case TypeDispatcherHeartbeat:
		return "DispatcherHeartbeat"
	case TypeDispatcherHeartbeatResponse:
		return "DispatcherHeartbeatResponse"
	default:
		return "unknown"
	}
}

// fakeDispatcherID is a fake dispatcherID for batch resolvedTs.
var fakeDispatcherID = common.DispatcherID(common.NewGIDWithValue(0, 0))

type InfluenceType int

const (
	InfluenceTypeAll InfluenceType = iota // influence all tables
	InfluenceTypeDB                       // influence all tables in the same database
	InfluenceTypeNormal
)

func (t InfluenceType) toPB() heartbeatpb.InfluenceType {
	switch t {
	case InfluenceTypeAll:
		return heartbeatpb.InfluenceType_All
	case InfluenceTypeDB:
		return heartbeatpb.InfluenceType_DB
	case InfluenceTypeNormal:
		return heartbeatpb.InfluenceType_Normal
	default:
		log.Error("unknown influence type")
	}
	return heartbeatpb.InfluenceType_Normal
}

type InfluencedTables struct {
	InfluenceType InfluenceType `msg:"influence-type"`
	// only exists when InfluenceType is InfluenceTypeNormal
	// NOTE: All the table IDs in TableIDs are physical table IDs.
	TableIDs []int64 `msg:"tables"`
	// only exists when InfluenceType is InfluenceTypeDB
	SchemaID int64 `msg:"schema"`
}

func (i *InfluencedTables) ToPB() *heartbeatpb.InfluencedTables {
	if i == nil {
		return nil
	}
	return &heartbeatpb.InfluencedTables{
		InfluenceType: i.InfluenceType.toPB(),
		TableIDs:      i.TableIDs,
		SchemaID:      i.SchemaID,
	}
}

func ToTablesPB(tables []Table) []*heartbeatpb.Table {
	res := make([]*heartbeatpb.Table, len(tables))
	for i, t := range tables {
		res[i] = &heartbeatpb.Table{
			TableID:   t.TableID,
			SchemaID:  t.SchemaID,
			Splitable: t.Splitable,
		}
	}
	return res
}

type Table struct {
	SchemaID         int64 `msg:"-"`
	TableID          int64 `msg:"table"`
	Splitable        bool  `msg:"-"` // whether the table is eligible for split
	*SchemaTableName `msg:"-"`
}

//msgp:ignore SchemaIDChange
type SchemaIDChange struct {
	TableID     int64
	OldSchemaID int64
	NewSchemaID int64
}

func ToSchemaIDChangePB(SchemaIDChange []SchemaIDChange) []*heartbeatpb.SchemaIDChange {
	if SchemaIDChange == nil {
		return nil
	}
	res := make([]*heartbeatpb.SchemaIDChange, len(SchemaIDChange))
	for i, c := range SchemaIDChange {
		res[i] = &heartbeatpb.SchemaIDChange{
			TableID:     c.TableID,
			OldSchemaID: c.OldSchemaID,
			NewSchemaID: c.NewSchemaID,
		}
	}
	return res
}

//msgp:ignore Selector
type Selector interface {
	Select(colInfo *model.ColumnInfo) bool
}
