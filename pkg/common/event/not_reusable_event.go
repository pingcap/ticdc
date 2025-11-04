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
	NotReusableEventVersion = 0
)

var _ Event = &NotReusableEvent{}

type NotReusableEvent struct {
	Version      int
	DispatcherID common.DispatcherID
}

func NewNotReusableEvent(dispatcherID common.DispatcherID) NotReusableEvent {
	return NotReusableEvent{
		Version:      NotReusableEventVersion,
		DispatcherID: dispatcherID,
	}
}

func (e *NotReusableEvent) String() string {
	return fmt.Sprintf("NotReusableEvent{Version: %d, DispatcherID: %s}", e.Version, e.DispatcherID)
}

// GetType returns the event type
func (e *NotReusableEvent) GetType() int {
	return TypeNotReusableEvent
}

// GeSeq return the sequence number of handshake event.
func (e *NotReusableEvent) GetSeq() uint64 {
	// not used
	return 0
}

func (e *NotReusableEvent) GetEpoch() uint64 {
	// not used
	return 0
}

// GetDispatcherID returns the dispatcher ID
func (e *NotReusableEvent) GetDispatcherID() common.DispatcherID {
	return e.DispatcherID
}

// GetCommitTs returns the commit timestamp
func (e *NotReusableEvent) GetCommitTs() common.Ts {
	// not used
	return 0
}

// GetStartTs returns the start timestamp
func (e *NotReusableEvent) GetStartTs() common.Ts {
	// not used
	return 0
}

// GetSize returns the approximate size of the event in bytes
func (e *NotReusableEvent) GetSize() int64 {
	// Size does not include header or version (those are only for serialization)
	// Only business data: dispatcherID
	return int64(e.DispatcherID.GetSize())
}

func (e *NotReusableEvent) IsPaused() bool {
	// TODO: is this ok?
	return false
}

func (e *NotReusableEvent) Len() int32 {
	return 0
}

func (e NotReusableEvent) Marshal() ([]byte, error) {
	// 1. Encode payload based on version
	var payload []byte
	var err error
	switch e.Version {
	case NotReusableEventVersion:
		payload, err = e.encodeV1()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported NotReusableEvent version: %d", e.Version)
	}

	// 2. Use unified header format
	return MarshalEventWithHeader(TypeNotReusableEvent, e.Version, payload)
}

func (e *NotReusableEvent) Unmarshal(data []byte) error {
	// 1. Validate header and extract payload
	payload, version, err := ValidateAndExtractPayload(data, TypeNotReusableEvent)
	if err != nil {
		return err
	}

	// 2. Store version
	e.Version = version

	// 3. Decode based on version
	switch version {
	case NotReusableEventVersion:
		return e.decodeV1(payload)
	default:
		return fmt.Errorf("unsupported NotReusableEvent version: %d", version)
	}
}

func (e NotReusableEvent) encodeV1() ([]byte, error) {
	// Note: version is now handled in the header by Marshal(), not here
	// payload: dispatcherID
	payloadSize := e.DispatcherID.GetSize()
	data := make([]byte, payloadSize)
	offset := 0

	// DispatcherID
	copy(data[offset:], e.DispatcherID.Marshal())

	return data, nil
}

func (e *NotReusableEvent) decodeV1(data []byte) error {
	// Note: header (magic + event type + version + length) has already been read and removed from data
	offset := 0

	// DispatcherID
	err := e.DispatcherID.Unmarshal(data[offset:])
	if err != nil {
		return err
	}

	return nil
}
