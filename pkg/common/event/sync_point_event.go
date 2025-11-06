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

	"github.com/pingcap/ticdc/pkg/common"
)

var _ Event = &SyncPointEvent{}

const (
	SyncPointEventVersion1 = 1
)

// Implement Event / FlushEvent / BlockEvent interface
// CommitTsList contains the commit ts of sync point.
// If a period of time has no other dml and ddl, commitTsList may contains multiple commit ts in order.
// Otherwise, the commitTsList only contains one commit ts.
type SyncPointEvent struct {
	DispatcherID common.DispatcherID
	CommitTsList []uint64
	// The seq of the event. It is set by event service.
	Seq uint64
	// The epoch of the event. It is set by event service.
	Epoch          uint64
	Version        byte
	PostTxnFlushed []func()
}

func NewSyncPointEvent(id common.DispatcherID, commitTsList []uint64, seq uint64, epoch uint64) *SyncPointEvent {
	return &SyncPointEvent{
		DispatcherID: id,
		CommitTsList: commitTsList,
		Seq:          seq,
		Epoch:        epoch,
		Version:      SyncPointEventVersion1,
	}
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
	// Size does not include header or version (those are only for serialization)
	// Only business data: Seq(8) + Epoch(8) + DispatcherID + len(CommitTsList)(4) + CommitTsList
	return int64(8 + 8 + e.DispatcherID.GetSize() + 4 + 8*len(e.CommitTsList))
}

func (e *SyncPointEvent) IsPaused() bool {
	return false
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
	// 1. Encode payload based on version
	var payload []byte
	var err error
	switch e.Version {
	case SyncPointEventVersion1:
		payload, err = e.encodeV1()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported SyncPointEvent version: %d", e.Version)
	}

	// 2. Use unified header format
	return MarshalEventWithHeader(TypeSyncPointEvent, int(e.Version), payload)
}

func (e *SyncPointEvent) Unmarshal(data []byte) error {
	// 1. Validate header and extract payload
	payload, version, err := ValidateAndExtractPayload(data, TypeSyncPointEvent)
	if err != nil {
		return err
	}

	// 2. Store version
	e.Version = byte(version)

	// 3. Decode based on version
	switch version {
	case SyncPointEventVersion1:
		return e.decodeV1(payload)
	default:
		return fmt.Errorf("unsupported SyncPointEvent version: %d", version)
	}
}

func (e SyncPointEvent) encodeV1() ([]byte, error) {
	// Note: version is now handled in the header by Marshal(), not here
	// payload: Seq + Epoch + len(CommitTsList) + CommitTsList + DispatcherID
	payloadSize := 8 + 8 + 4 + 8*len(e.CommitTsList) + e.DispatcherID.GetSize()
	data := make([]byte, payloadSize)
	offset := 0

	// Seq
	binary.BigEndian.PutUint64(data[offset:], uint64(e.Seq))
	offset += 8

	// Epoch
	binary.BigEndian.PutUint64(data[offset:], e.Epoch)
	offset += 8

	// CommitTsList length
	binary.BigEndian.PutUint32(data[offset:], uint32(len(e.CommitTsList)))
	offset += 4

	// CommitTsList
	for _, ts := range e.CommitTsList {
		binary.BigEndian.PutUint64(data[offset:], ts)
		offset += 8
	}

	// DispatcherID
	copy(data[offset:], e.DispatcherID.Marshal())

	return data, nil
}

func (e *SyncPointEvent) decodeV1(data []byte) error {
	// Note: header (magic + event type + version + length) has already been read and removed from data
	offset := 0

	// Seq
	e.Seq = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Epoch
	e.Epoch = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// CommitTsList length
	count := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// CommitTsList
	e.CommitTsList = make([]uint64, count)
	for i := uint32(0); i < count; i++ {
		e.CommitTsList[i] = binary.BigEndian.Uint64(data[offset:])
		offset += 8
	}

	// DispatcherID
	err := e.DispatcherID.Unmarshal(data[offset:])
	if err != nil {
		return err
	}

	return nil
}
