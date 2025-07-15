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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

const (
	ReadyEventVersion = 1
)

var _ Event = &ReadyEvent{}

type ReadyEvent struct {
	Version      byte
	DispatcherID common.DispatcherID
	// only for redo
	IsRedo bool
}

func NewReadyEvent(dispatcherID common.DispatcherID, isRedo bool) ReadyEvent {
	return ReadyEvent{
		Version:      ReadyEventVersion,
		DispatcherID: dispatcherID,
		IsRedo:       isRedo,
	}
}

func (e *ReadyEvent) String() string {
	return fmt.Sprintf("ReadyEvent{Version: %d, DispatcherID: %s}", e.Version, e.DispatcherID)
}

func (e *ReadyEvent) GetIsRedo() bool {
	return e.IsRedo
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
	return int64(2 + e.DispatcherID.GetSize())
}

func (e *ReadyEvent) IsPaused() bool {
	// TODO: is this ok?
	return false
}

func (e *ReadyEvent) Len() int32 {
	return 0
}

func (e ReadyEvent) Marshal() ([]byte, error) {
	return e.encode()
}

func (e *ReadyEvent) Unmarshal(data []byte) error {
	return e.decode(data)
}

func (e ReadyEvent) encode() ([]byte, error) {
	if e.Version != ReadyEventVersion {
		log.Panic("ReadyEvent: invalid version", zap.Uint8("expected", ReadyEventVersion), zap.Uint8("version", e.Version))
	}
	return e.encodeV0()
}

func (e *ReadyEvent) decode(data []byte) error {
	version := data[0]
	if version != ReadyEventVersion {
		log.Panic("ReadyEvent: invalid version", zap.Uint8("expected", ReadyEventVersion), zap.Uint8("version", e.Version))
	}
	return e.decodeV0(data)
}

func (e ReadyEvent) encodeV0() ([]byte, error) {
	data := make([]byte, e.GetSize())
	offset := 0
	data[offset] = e.Version
	offset += 1
	// Redo
	data[offset] = bool2byte(e.IsRedo)
	offset += 1
	copy(data[offset:], e.DispatcherID.Marshal())
	offset += e.DispatcherID.GetSize()
	return data, nil
}

func (e *ReadyEvent) decodeV0(data []byte) error {
	offset := 0
	e.Version = data[offset]
	offset += 1
	// Redo
	e.IsRedo = byte2bool(data[offset])
	offset += 1
	dispatcherIDData := data[offset:]
	return e.DispatcherID.Unmarshal(dispatcherIDData)
}
