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
	HandshakeEventVersion = 0
)

var _ Event = &HandshakeEvent{}

type HandshakeEvent struct {
	// Version is the version of the HandshakeEvent struct.
	Version      int                 `json:"version"`
	ResolvedTs   uint64              `json:"resolved_ts"`
	Seq          uint64              `json:"seq"`
	Epoch        uint64              `json:"epoch"`
	DispatcherID common.DispatcherID `json:"-"`
	TableInfo    *common.TableInfo   `json:"table_info"`
}

func NewHandshakeEvent(
	dispatcherID common.DispatcherID,
	resolvedTs common.Ts,
	epoch uint64,
	tableInfo *common.TableInfo,
) HandshakeEvent {
	return HandshakeEvent{
		Version:    HandshakeEventVersion,
		ResolvedTs: resolvedTs,
		// handshake event always have seq 1
		Seq:          1,
		Epoch:        epoch,
		DispatcherID: dispatcherID,
		TableInfo:    tableInfo,
	}
}

func (e *HandshakeEvent) String() string {
	return fmt.Sprintf("HandshakeEvent{Version: %d, ResolvedTs: %d, Seq: %d, DispatcherID: %s, TableInfo: %v}",
		e.Version, e.ResolvedTs, e.Seq, e.DispatcherID, e.TableInfo)
}

// GetType returns the event type
func (e *HandshakeEvent) GetType() int {
	return TypeHandshakeEvent
}

// GeSeq return the sequence number of handshake event.
func (e *HandshakeEvent) GetSeq() uint64 {
	return e.Seq
}

func (e *HandshakeEvent) GetEpoch() uint64 {
	return e.Epoch
}

// GetDispatcherID returns the dispatcher ID
func (e *HandshakeEvent) GetDispatcherID() common.DispatcherID {
	return e.DispatcherID
}

// GetCommitTs returns the commit timestamp
func (e *HandshakeEvent) GetCommitTs() common.Ts {
	return e.ResolvedTs
}

// GetStartTs returns the start timestamp
func (e *HandshakeEvent) GetStartTs() common.Ts {
	return e.ResolvedTs
}

// GetSize returns the approximate size of the event in bytes
func (e *HandshakeEvent) GetSize() int64 {
	// header size + payload size (version is now in header, not payload)
	// payload: resolvedTs + seq + epoch + dispatcherID + tableInfo (variable)
	// Note: TableInfo size is not included in this calculation as it's variable
	payloadSize := int64(8 + 8 + 8 + e.DispatcherID.GetSize())
	return payloadSize
}

func (e *HandshakeEvent) IsPaused() bool {
	return false
}

func (e *HandshakeEvent) Len() int32 {
	return 0
}

func (e HandshakeEvent) Marshal() ([]byte, error) {
	// 1. Encode payload based on version
	var payload []byte
	var err error
	switch e.Version {
	case HandshakeEventVersion:
		payload, err = e.encodeV1()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported HandshakeEvent version: %d", e.Version)
	}

	// 2. Use unified header format
	return MarshalEventWithHeader(TypeHandshakeEvent, e.Version, payload)
}

func (e *HandshakeEvent) Unmarshal(data []byte) error {
	// 1. Validate header and extract payload
	payload, version, err := ValidateAndExtractPayload(data, TypeHandshakeEvent)
	if err != nil {
		return err
	}

	// 2. Store version
	e.Version = version

	// 3. Decode based on version
	switch version {
	case HandshakeEventVersion:
		return e.decodeV1(payload)
	default:
		return fmt.Errorf("unsupported HandshakeEvent version: %d", version)
	}
}

func (e HandshakeEvent) encodeV1() ([]byte, error) {
	// Note: version is now handled in the header by Marshal(), not here
	// payload: resolvedTs + seq + epoch + dispatcherID + tableInfo
	tableInfoData, err := e.TableInfo.Marshal()
	if err != nil {
		return nil, err
	}

	// payload size: resolvedTs + seq + epoch + dispatcherID + tableInfo
	payloadSize := 8 + 8 + 8 + e.DispatcherID.GetSize() + len(tableInfoData)
	data := make([]byte, payloadSize)
	offset := 0

	// ResolvedTs
	binary.BigEndian.PutUint64(data[offset:], e.ResolvedTs)
	offset += 8

	// Seq
	binary.BigEndian.PutUint64(data[offset:], e.Seq)
	offset += 8

	// Epoch
	binary.BigEndian.PutUint64(data[offset:], e.Epoch)
	offset += 8

	// DispatcherID
	copy(data[offset:], e.DispatcherID.Marshal())
	offset += e.DispatcherID.GetSize()

	// TableInfo
	copy(data[offset:], tableInfoData)

	return data, nil
}

func (e *HandshakeEvent) decodeV1(data []byte) error {
	// Note: header (magic + event type + version + length) has already been read and removed from data
	offset := 0

	// ResolvedTs
	e.ResolvedTs = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Seq
	e.Seq = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Epoch
	e.Epoch = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// DispatcherID
	err := e.DispatcherID.Unmarshal(data[offset : offset+e.DispatcherID.GetSize()])
	if err != nil {
		return err
	}
	offset += e.DispatcherID.GetSize()

	// TableInfo
	e.TableInfo, err = common.UnmarshalJSONToTableInfo(data[offset:])
	if err != nil {
		return err
	}

	// Initialize private fields after unmarshaling
	e.TableInfo.InitPrivateFields()

	return nil
}
