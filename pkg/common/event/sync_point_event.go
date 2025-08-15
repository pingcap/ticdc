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
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

var _ Event = &SyncPointEvent{}

const (
	SyncPointEventVersion = 1
)

// Implement Event / FlushEvent / BlockEvent interface
// CommitTsList contains the commit ts of sync point.
// If a period of time has no other dml and ddl, commitTsList may contains multiple commit ts in order.
// Otherwise, the commitTsList only contains one commit ts.
type SyncPointEvent struct {
	// State is the state of sender when sending this event.
	State        EventSenderState
	DispatcherID common.DispatcherID
	CommitTsList []uint64
	// The seq of the event. It is set by event service.
	Seq uint64
	// The epoch of the event. It is set by event service.
	Epoch          uint64
	Version        byte
	PostTxnFlushed []func()
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
	// Version(1) + Seq(8) + Epoch(8) + State(1) + DispatcherID(16) + len(CommitTsList) + 8 * len(CommitTsList)
	return 1 + 8*2 + int64(e.State.GetSize()+e.DispatcherID.GetSize()+4+8*len(e.CommitTsList))
}

func (e *SyncPointEvent) IsPaused() bool {
	return e.State.IsPaused()
}

func (e SyncPointEvent) GetSeq() uint64 {
	return e.Seq
}

func (e SyncPointEvent) GetEpoch() uint64 {
	return e.Epoch
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
	return 1
}

func (e SyncPointEvent) Marshal() ([]byte, error) {
	return e.encode()
}

func (e *SyncPointEvent) Unmarshal(data []byte) error {
	return e.decode(data)
}

func (e SyncPointEvent) encode() ([]byte, error) {
	if e.Version != SyncPointEventVersion {
		log.Panic("SyncPointEvent: invalid version",
			zap.Uint64("expected", SyncPointEventVersion), zap.Uint8("received", e.Version))
	}
	return e.encodeV0()
}

func (e *SyncPointEvent) decode(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("SyncPointEvent.decode: empty data")
	}
	e.Version = data[0]
	if e.Version != SyncPointEventVersion {
		return fmt.Errorf("SyncPointEvent: invalid version, expect %d, got %d", SyncPointEventVersion, e.Version)
	}
	return e.decodeV0(data)
}

func (e SyncPointEvent) encodeV0() ([]byte, error) {
	data := make([]byte, e.GetSize())
	offset := 0
	data[offset] = e.Version
	offset += 1
	binary.BigEndian.PutUint64(data[offset:], uint64(e.Seq))
	offset += 8
	binary.BigEndian.PutUint64(data[offset:], e.Epoch)
	offset += 8
	binary.BigEndian.PutUint32(data[offset:], uint32(len(e.CommitTsList)))
	offset += 4
	for _, ts := range e.CommitTsList {
		binary.BigEndian.PutUint64(data[offset:], ts)
		offset += 8
	}
	copy(data[offset:], e.State.encode())
	offset += e.State.GetSize()
	copy(data[offset:], e.DispatcherID.Marshal())
	offset += e.DispatcherID.GetSize()
	return data, nil
}

func (e *SyncPointEvent) decodeV0(data []byte) error {
	offset := 1 // Skip version byte
	e.Seq = common.Ts(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	e.Epoch = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	count := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	e.CommitTsList = make([]uint64, count)
	for i := uint32(0); i < count; i++ {
		e.CommitTsList[i] = binary.BigEndian.Uint64(data[offset:])
		offset += 8
	}
	e.State.decode(data[offset:])
	offset += e.State.GetSize()
	return e.DispatcherID.Unmarshal(data[offset:])
}
