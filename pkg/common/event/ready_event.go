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
	"fmt"

	"github.com/pingcap/ticdc/pkg/common"
)

const (
	ReadyEventVersion0 = 0
)

var _ Event = &ReadyEvent{}

type ReadyEvent struct {
	Version      byte
	DispatcherID common.DispatcherID
}

func NewReadyEvent(dispatcherID common.DispatcherID) ReadyEvent {
	return ReadyEvent{
		Version:      ReadyEventVersion0,
		DispatcherID: dispatcherID,
	}
}

func (e *ReadyEvent) String() string {
	return fmt.Sprintf("ReadyEvent{Version: %d, DispatcherID: %s}", e.Version, e.DispatcherID)
}

// GetType returns the event type
func (e *ReadyEvent) GetType() int {
	return TypeReadyEvent
}

// GeSeq return the sequence number of handshake event.
func (e *ReadyEvent) GetSeq() uint64 {
	// not used
	return 0
}

func (e *ReadyEvent) GetEpoch() uint64 {
	// not used
	return 0
}

// GetDispatcherID returns the dispatcher ID
func (e *ReadyEvent) GetDispatcherID() common.DispatcherID {
	return e.DispatcherID
}

// GetCommitTs returns the commit timestamp
func (e *ReadyEvent) GetCommitTs() common.Ts {
	// not used
	return 0
}

// GetStartTs returns the start timestamp
func (e *ReadyEvent) GetStartTs() common.Ts {
	// not used
	return 0
}

// GetSize returns the approximate size of the event in bytes
func (e *ReadyEvent) GetSize() int64 {
	// Size does not include header or version (those are only for serialization)
	// Only business data: dispatcherID
	return int64(e.DispatcherID.GetSize())
}

func (e *ReadyEvent) IsPaused() bool {
	// TODO: is this ok?
	return false
}

func (e *ReadyEvent) Len() int32 {
	return 0
}

func (e ReadyEvent) Marshal() ([]byte, error) {
	// 1. Encode payload based on version
	var payload []byte
	var err error
	switch e.Version {
	case ReadyEventVersion0:
		payload, err = e.encodeV0()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported ReadyEvent version: %d", e.Version)
	}

	// 2. Use unified header format
	return MarshalEventWithHeader(TypeReadyEvent, e.Version, payload)
}

func (e *ReadyEvent) Unmarshal(data []byte) error {
	// 1. Parse unified header
	eventType, version, payloadLen, err := UnmarshalEventHeader(data)
	if err != nil {
		return err
	}

	// 2. Validate event type
	if eventType != TypeReadyEvent {
		return fmt.Errorf("expected ReadyEvent (type %d), got type %d (%s)",
			TypeReadyEvent, eventType, TypeToString(eventType))
	}

	// 3. Validate total data length
	headerSize := GetEventHeaderSize()
	expectedLen := headerSize + payloadLen
	if len(data) < expectedLen {
		return fmt.Errorf("incomplete data: expected %d bytes (header %d + payload %d), got %d",
			expectedLen, headerSize, payloadLen, len(data))
	}

	// 4. Extract payload
	payload := data[headerSize : headerSize+payloadLen]

	// 5. Store version
	e.Version = version

	// 6. Decode based on version
	switch version {
	case ReadyEventVersion0:
		return e.decodeV0(payload)
	default:
		return fmt.Errorf("unsupported ReadyEvent version: %d", version)
	}
}

func (e ReadyEvent) encodeV0() ([]byte, error) {
	// Note: version is now handled in the header by Marshal(), not here
	// payload: dispatcherID
	payloadSize := e.DispatcherID.GetSize()
	data := make([]byte, payloadSize)
	offset := 0

	// DispatcherID
	copy(data[offset:], e.DispatcherID.Marshal())

	return data, nil
}

func (e *ReadyEvent) decodeV0(data []byte) error {
	// Note: header (magic + event type + version + length) has already been read and removed from data
	offset := 0

	// DispatcherID
	err := e.DispatcherID.Unmarshal(data[offset:])
	if err != nil {
		return err
	}

	return nil
}
