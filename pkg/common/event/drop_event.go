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
	DropEventVersion = 0
)

var _ Event = &DropEvent{}

// DropEvent represents an event that has been dropped due to memory pressure
type DropEvent struct {
	Version         byte
	DispatcherID    common.DispatcherID
	DroppedSeq      uint64
	DroppedCommitTs common.Ts
	DroppedEpoch    uint64
	// only for redo
	IsRedo bool
}

// NewDropEvent creates a new DropEvent
func NewDropEvent(
	dispatcherID common.DispatcherID,
	seq uint64,
	epoch uint64,
	commitTs common.Ts,
) *DropEvent {
	return &DropEvent{
		Version:         DropEventVersion,
		DispatcherID:    dispatcherID,
		DroppedSeq:      seq,
		DroppedCommitTs: commitTs,
		DroppedEpoch:    epoch,
	}
}

func (e *DropEvent) GetIsRedo() bool {
	return e.IsRedo
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
	return int64(2 + e.DispatcherID.GetSize() + 8 + 8) // version + isRedo + dispatcherID + seq + commitTs
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
	return e.encode()
}

// Unmarshal decodes the DropEvent from bytes
func (e *DropEvent) Unmarshal(data []byte) error {
	return e.decode(data)
}

func (e *DropEvent) encode() ([]byte, error) {
	if e.Version != 0 {
		return nil, fmt.Errorf("DropEvent.encode: unsupported version %d", e.Version)
	}
	return e.encodeV0()
}

func (e *DropEvent) decode(data []byte) error {
	if len(data) != int(e.GetSize()) {
		return fmt.Errorf("DropEvent.decode: invalid data length, expected %d, got %d", e.GetSize(), len(data))
	}

	version := data[0]
	if version != 0 {
		return fmt.Errorf("DropEvent.decode: unsupported version %d", version)
	}

	return e.decodeV0(data)
}

func (e *DropEvent) encodeV0() ([]byte, error) {
	data := make([]byte, e.GetSize())
	offset := 0

	// Version
	data[offset] = e.Version
	offset += 1

	// Redo
	data[offset] = bool2byte(e.IsRedo)
	offset += 1

	// DispatcherID
	copy(data[offset:], e.DispatcherID.Marshal())
	offset += e.DispatcherID.GetSize()

	// DroppedSeq
	binary.LittleEndian.PutUint64(data[offset:], e.DroppedSeq)
	offset += 8

	// DroppedCommitTs
	binary.LittleEndian.PutUint64(data[offset:], uint64(e.DroppedCommitTs))
	offset += 8

	return data, nil
}

func (e *DropEvent) decodeV0(data []byte) error {
	offset := 0

	// Version
	e.Version = data[offset]
	offset += 1

	// Redo
	e.IsRedo = byte2bool(data[offset])
	offset += 1

	// DispatcherID
	dispatcherIDSize := e.DispatcherID.GetSize()
	err := e.DispatcherID.Unmarshal(data[offset : offset+dispatcherIDSize])
	if err != nil {
		return err
	}
	offset += dispatcherIDSize

	// DroppedSeq
	e.DroppedSeq = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// DroppedCommitTs
	e.DroppedCommitTs = common.Ts(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	return nil
}
