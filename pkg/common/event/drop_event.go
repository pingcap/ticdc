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

const (
	DropEventVersion1 = 1
)

var _ Event = &DropEvent{}

// DropEvent represents an event that has been dropped due to memory pressure
type DropEvent struct {
	Version         int
	DispatcherID    common.DispatcherID
	DroppedSeq      uint64
	DroppedCommitTs common.Ts
	DroppedEpoch    uint64
}

// NewDropEvent creates a new DropEvent
func NewDropEvent(
	dispatcherID common.DispatcherID,
	seq uint64,
	epoch uint64,
	commitTs common.Ts,
) *DropEvent {
	return &DropEvent{
		Version:         DropEventVersion1,
		DispatcherID:    dispatcherID,
		DroppedSeq:      seq,
		DroppedCommitTs: commitTs,
		DroppedEpoch:    epoch,
	}
}

// GetType returns the event type
func (e *DropEvent) GetType() int {
	return TypeDropEvent
}

// GetSeq returns the sequence number of the dropped event
func (e *DropEvent) GetSeq() uint64 {
	return e.DroppedSeq
}

func (e *DropEvent) GetEpoch() uint64 {
	return e.DroppedEpoch
}

// GetDispatcherID returns the dispatcher ID
func (e *DropEvent) GetDispatcherID() common.DispatcherID {
	return e.DispatcherID
}

// GetCommitTs returns the commit timestamp of the dropped event
func (e *DropEvent) GetCommitTs() common.Ts {
	return e.DroppedCommitTs
}

// GetStartTs returns the start timestamp (not used for DropEvent)
func (e *DropEvent) GetStartTs() common.Ts {
	return 0
}

// GetSize returns the approximate size of the event in bytes
func (e *DropEvent) GetSize() int64 {
	// payload: dispatcherID + seq + commitTs + epoch
	payloadSize := int64(e.DispatcherID.GetSize() + 8 + 8 + 8)
	return payloadSize
}

// IsPaused returns false as drop events are not pausable
func (e *DropEvent) IsPaused() bool {
	return false
}

// Len returns 0 as drop events don't contain actual data rows
func (e *DropEvent) Len() int32 {
	return 0
}

// Marshal encodes the DropEvent to bytes
func (e *DropEvent) Marshal() ([]byte, error) {
	// 1. Encode payload based on version
	var payload []byte
	var err error
	switch e.Version {
	case DropEventVersion1:
		payload, err = e.encodeV1()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported DropEvent version: %d", e.Version)
	}

	// 2. Use unified header format
	return MarshalEventWithHeader(TypeDropEvent, e.Version, payload)
}

// Unmarshal decodes the DropEvent from bytes
func (e *DropEvent) Unmarshal(data []byte) error {
	// 1. Validate header and extract payload
	payload, version, err := ValidateAndExtractPayload(data, TypeDropEvent)
	if err != nil {
		return err
	}

	// 2. Decode based on version
	switch version {
	case DropEventVersion1:
		if err := e.decodeV1(payload); err != nil {
			return err
		}
		e.Version = version
		return nil
	default:
		return fmt.Errorf("unsupported DropEvent version: %d", version)
	}
}

func (e *DropEvent) encodeV1() ([]byte, error) {
	// Note: version is now handled in the header by Marshal(), not here
	// payload: dispatcherID + seq + commitTs + epoch
	payloadSize := e.DispatcherID.GetSize() + 8 + 8 + 8
	data := make([]byte, payloadSize)
	offset := 0

	// DispatcherID
	copy(data[offset:], e.DispatcherID.Marshal())
	offset += e.DispatcherID.GetSize()

	// DroppedSeq
	binary.BigEndian.PutUint64(data[offset:], e.DroppedSeq)
	offset += 8

	// DroppedCommitTs
	binary.BigEndian.PutUint64(data[offset:], uint64(e.DroppedCommitTs))
	offset += 8

	// DroppedEpoch
	binary.BigEndian.PutUint64(data[offset:], e.DroppedEpoch)
	offset += 8

	return data, nil
}

func (e *DropEvent) decodeV1(data []byte) error {
	// Note: header (magic + event type + version + length) has already been read and removed from data
	offset := 0

	// DispatcherID
	dispatcherIDSize := e.DispatcherID.GetSize()
	err := e.DispatcherID.Unmarshal(data[offset : offset+dispatcherIDSize])
	if err != nil {
		return err
	}
	offset += dispatcherIDSize

	// DroppedSeq
	e.DroppedSeq = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// DroppedCommitTs
	e.DroppedCommitTs = common.Ts(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// DroppedEpoch
	e.DroppedEpoch = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	return nil
}
