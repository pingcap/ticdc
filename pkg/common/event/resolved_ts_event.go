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

var _ Event = &BatchResolvedEvent{}

const (
	BatchResolvedEventVersion1 = 1
)

type BatchResolvedEvent struct {
	// Version is the version of the BatchResolvedEvent struct.
	Version byte
	Events  []ResolvedEvent
}

func NewBatchResolvedEvent(events []ResolvedEvent) *BatchResolvedEvent {
	return &BatchResolvedEvent{
		Version: BatchResolvedEventVersion1,
		Events:  events,
	}
}

func (b BatchResolvedEvent) GetType() int {
	return TypeBatchResolvedEvent
}

func (b BatchResolvedEvent) GetDispatcherID() common.DispatcherID {
	// It's a fake dispatcherID.
	return fakeDispatcherID
}

func (b BatchResolvedEvent) GetCommitTs() common.Ts {
	// It's a fake commitTs.
	return 0
}

func (b BatchResolvedEvent) GetStartTs() common.Ts {
	// It's a fake startTs.
	return 0
}

func (b *BatchResolvedEvent) GetSeq() uint64 {
	// It's a fake seq.
	return 0
}

func (b *BatchResolvedEvent) GetEpoch() uint64 {
	// It's a fake epoch.
	return 0
}

func (b *BatchResolvedEvent) Len() int32 {
	// Return the length of events.
	return int32(len(b.Events))
}

func (b *BatchResolvedEvent) Marshal() ([]byte, error) {
	if len(b.Events) == 0 {
		return nil, nil
	}
	firstEvent := b.Events[0]
	buf := make([]byte, 0, len(b.Events)*int(firstEvent.GetSize()))
	for _, e := range b.Events {
		data, err := e.Marshal()
		if err != nil {
			return nil, err
		}
		buf = append(buf, data...)
	}
	return buf, nil
}

func (b *BatchResolvedEvent) Unmarshal(data []byte) error {
	// Now each ResolvedEvent has a header, so we need to parse them one by one
	b.Events = make([]ResolvedEvent, 0)
	offset := 0

	for offset < len(data) {
		// Parse the header to get the payload length
		if offset+GetEventHeaderSize() > len(data) {
			return fmt.Errorf("BatchResolvedEvent.Unmarshal: incomplete header at offset %d", offset)
		}

		_, _, payloadLen, err := UnmarshalEventHeader(data[offset:])
		if err != nil {
			return fmt.Errorf("BatchResolvedEvent.Unmarshal: failed to parse header at offset %d: %w", offset, err)
		}

		// Calculate total event size (header + payload)
		eventSize := uint64(GetEventHeaderSize()) + payloadLen
		if uint64(offset)+eventSize > uint64(len(data)) {
			return fmt.Errorf("BatchResolvedEvent.Unmarshal: incomplete event at offset %d", offset)
		}

		// Unmarshal the event
		var e ResolvedEvent
		if err := e.Unmarshal(data[offset : uint64(offset)+eventSize]); err != nil {
			return fmt.Errorf("BatchResolvedEvent.Unmarshal: failed to unmarshal event at offset %d: %w", offset, err)
		}

		b.Events = append(b.Events, e)
		offset += int(eventSize)
	}

	return nil
}

// No one will use this method, just for implementing Event interface.
func (b *BatchResolvedEvent) GetSize() int64 {
	return 0
}

// No one will use this method, just for implementing Event interface.
func (b *BatchResolvedEvent) IsPaused() bool {
	return false
}

const (
	ResolvedEventVersion1 = 1
)

var _ Event = &ResolvedEvent{}

// ResolvedEvent represents a resolvedTs event of a dispatcher.
type ResolvedEvent struct {
	Version      byte
	DispatcherID common.DispatcherID
	ResolvedTs   common.Ts
	Epoch        uint64
	// It's the last concrete data event's (eg. dml/ddl/handshake) seq.
	// Use it to check if there is a missing
	Seq uint64
}

func NewResolvedEvent(
	resolvedTs common.Ts,
	dispatcherID common.DispatcherID,
	epoch uint64,
) ResolvedEvent {
	return ResolvedEvent{
		DispatcherID: dispatcherID,
		ResolvedTs:   resolvedTs,
		Version:      ResolvedEventVersion1,
		Epoch:        epoch,
	}
}

func (e ResolvedEvent) GetType() int {
	return TypeResolvedEvent
}

func (e ResolvedEvent) GetDispatcherID() common.DispatcherID {
	return e.DispatcherID
}

func (e ResolvedEvent) GetCommitTs() common.Ts {
	return e.ResolvedTs
}

func (e ResolvedEvent) GetStartTs() common.Ts {
	return e.ResolvedTs
}

func (e ResolvedEvent) GetSeq() uint64 {
	return e.Seq
}

func (e ResolvedEvent) GetEpoch() uint64 {
	return e.Epoch
}

func (e ResolvedEvent) Len() int32 {
	return 1
}

func (e ResolvedEvent) Marshal() ([]byte, error) {
	// 1. Encode payload based on version
	var payload []byte
	var err error
	switch e.Version {
	case ResolvedEventVersion1:
		payload, err = e.encodeV1()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported ResolvedEvent version: %d", e.Version)
	}

	// 2. Use unified header format
	return MarshalEventWithHeader(TypeResolvedEvent, int(e.Version), payload)
}

func (e *ResolvedEvent) Unmarshal(data []byte) error {
	// 1. Validate header and extract payload
	payload, version, err := ValidateAndExtractPayload(data, TypeResolvedEvent)
	if err != nil {
		return err
	}

	// 2. Store version
	e.Version = byte(version)

	// 3. Decode based on version
	switch version {
	case ResolvedEventVersion1:
		return e.decodeV1(payload)
	default:
		return fmt.Errorf("unsupported ResolvedEvent version: %d", version)
	}
}

func (e ResolvedEvent) encodeV1() ([]byte, error) {
	// Note: version is now handled in the header by Marshal(), not here
	// payload: ResolvedTs + Epoch + Seq + DispatcherID
	payloadSize := 8 + 8 + 8 + e.DispatcherID.GetSize()
	data := make([]byte, payloadSize)
	offset := 0

	// ResolvedTs
	binary.BigEndian.PutUint64(data[offset:], uint64(e.ResolvedTs))
	offset += 8

	// Epoch
	binary.BigEndian.PutUint64(data[offset:], e.Epoch)
	offset += 8

	// Seq
	binary.BigEndian.PutUint64(data[offset:], e.Seq)
	offset += 8

	// DispatcherID
	copy(data[offset:], e.DispatcherID.Marshal())

	return data, nil
}

func (e *ResolvedEvent) decodeV1(data []byte) error {
	// Note: header (magic + event type + version + length) has already been read and removed from data
	offset := 0

	// ResolvedTs
	e.ResolvedTs = common.Ts(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Epoch
	e.Epoch = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Seq
	e.Seq = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// DispatcherID
	err := e.DispatcherID.Unmarshal(data[offset:])
	if err != nil {
		return err
	}

	return nil
}

func (e ResolvedEvent) String() string {
	return fmt.Sprintf("ResolvedEvent{DispatcherID: %s, ResolvedTs: %d, Epoch: %d, Seq: %d}", e.DispatcherID.String(), e.ResolvedTs, e.Epoch, e.Seq)
}

// GetSize returns the approximate size of the event in bytes
func (e ResolvedEvent) GetSize() int64 {
	// Size does not include header or version (those are only for serialization)
	// Only business data: ResolvedTs(8) + Epoch(8) + Seq(8) + DispatcherID
	return int64(8 + 8 + 8 + e.DispatcherID.GetSize())
}

func (e ResolvedEvent) IsPaused() bool {
	return false
}
