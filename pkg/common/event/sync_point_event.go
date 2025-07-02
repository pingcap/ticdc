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
	"encoding/json"

	"github.com/pingcap/ticdc/pkg/common"
)

var _ Event = &SyncPointEvent{}

// Implement Event / FlushEvent / BlockEvent interface
// CommitTsList contains the commit ts of sync point.
// If a period of time has no other dml and ddl, commitTsList may contains multiple commit ts in order.
// Otherwise, the commitTsList only contains one commit ts.
type SyncPointEvent struct {
	// State is the state of sender when sending this event.
	State        EventSenderState    `json:"state"`
	DispatcherID common.DispatcherID `json:"dispatcher_id"`
	CommitTsList []uint64            `json:"commit_ts_list"`
	// The seq of the event. It is set by event service.
	Seq uint64 `json:"seq"`
	// The epoch of the event. It is set by event service.
	Epoch          uint64   `json:"epoch"`
	PostTxnFlushed []func() `msg:"-"`
}

func (e *SyncPointEvent) GetType() int {
	return TypeSyncPointEvent
}

func (e *SyncPointEvent) GetDispatcherID() common.DispatcherID {
	return e.DispatcherID
}

func (e *SyncPointEvent) GetCommitTsList() []common.Ts {
	return e.CommitTsList
}

func (e *SyncPointEvent) GetCommitTs() common.Ts {
	return e.CommitTsList[0]
}

func (e *SyncPointEvent) GetStartTs() common.Ts {
	return e.CommitTsList[0]
}

func (e *SyncPointEvent) GetSize() int64 {
	return int64(e.State.GetSize() + e.DispatcherID.GetSize() + 8*len(e.CommitTsList))
}

func (e *SyncPointEvent) IsPaused() bool {
	return e.State.IsPaused()
}

func (e SyncPointEvent) Marshal() ([]byte, error) {
	// TODO: optimize it
	return json.Marshal(e)
}

func (e SyncPointEvent) GetSeq() uint64 {
	return e.Seq
}

func (e SyncPointEvent) GetEpoch() uint64 {
	return e.Epoch
}

func (e *SyncPointEvent) Unmarshal(data []byte) error {
	// TODO: optimize it
	return json.Unmarshal(data, e)
}

func (e *SyncPointEvent) GetBlockedTables() *InfluencedTables {
	return &InfluencedTables{
		InfluenceType: InfluenceTypeAll,
	}
}

func (e *SyncPointEvent) GetNeedDroppedTables() *InfluencedTables {
	return nil
}

func (e *SyncPointEvent) GetNeedAddedTables() []Table {
	return nil
}

func (e *SyncPointEvent) GetUpdatedSchemas() []SchemaIDChange {
	return nil
}

func (e *SyncPointEvent) PostFlush() {
	for _, f := range e.PostTxnFlushed {
		f()
	}
}

func (e *SyncPointEvent) AddPostFlushFunc(f func()) {
	e.PostTxnFlushed = append(e.PostTxnFlushed, f)
}

func (e *SyncPointEvent) PushFrontFlushFunc(f func()) {
	e.PostTxnFlushed = append([]func(){f}, e.PostTxnFlushed...)
}

func (e *SyncPointEvent) ClearPostFlushFunc() {
	e.PostTxnFlushed = e.PostTxnFlushed[:0]
}

func (e *SyncPointEvent) Len() int32 {
	return 0
}
