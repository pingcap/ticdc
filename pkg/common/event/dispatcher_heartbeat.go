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
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/pingcap/ticdc/pkg/common"
)

const (
	DispatcherHeartbeatVersion1         = 1
	DispatcherHeartbeatVersion2         = 2
	DispatcherHeartbeatResponseVersion1 = 1
	DispatcherProgressVersion1          = 1
)

// DispatcherProgressLegacy is the legacy wire format used by heartbeat v1.
type DispatcherProgressLegacy struct {
	DispatcherID common.DispatcherID
	CheckpointTs uint64 // 8 bytes
}

func (dp DispatcherProgressLegacy) GetSize() int {
	return dp.DispatcherID.GetSize() + 8 // dispatcherID size + checkpointTs size
}

func (dp DispatcherProgressLegacy) Marshal() ([]byte, error) {
	return dp.encodeV1()
}

func (dp *DispatcherProgressLegacy) Unmarshal(data []byte) error {
	return dp.decodeV1(data)
}

func (dp DispatcherProgressLegacy) encodeV1() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.Write(dp.DispatcherID.Marshal())
	binary.Write(buf, binary.BigEndian, dp.CheckpointTs)
	return buf.Bytes(), nil
}

func (dp *DispatcherProgressLegacy) decodeV1(data []byte) error {
	buf := bytes.NewBuffer(data)
	dp.DispatcherID.Unmarshal(buf.Next(dp.DispatcherID.GetSize()))
	dp.CheckpointTs = binary.BigEndian.Uint64(buf.Next(8))
	return nil
}

// DispatcherProgress is the current wire format used by heartbeat v2.
// The embedded version allows this structure to evolve without introducing
// DispatcherProgressV3, DispatcherProgressV4, etc.
type DispatcherProgress struct {
	Version      byte
	DispatcherID common.DispatcherID
	CheckpointTs uint64 // 8 bytes
	Epoch        uint64 // 8 bytes
}

func (dp DispatcherProgress) GetSize() int {
	return 1 + dp.DispatcherID.GetSize() + 8 + 8
}

func (dp DispatcherProgress) Marshal() ([]byte, error) {
	return dp.encode()
}

func (dp *DispatcherProgress) Unmarshal(data []byte) error {
	return dp.decode(data)
}

func (dp DispatcherProgress) encode() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(dp.Version)
	buf.Write(dp.DispatcherID.Marshal())
	binary.Write(buf, binary.BigEndian, dp.CheckpointTs)
	binary.Write(buf, binary.BigEndian, dp.Epoch)
	return buf.Bytes(), nil
}

func (dp *DispatcherProgress) decode(data []byte) error {
	buf := bytes.NewBuffer(data)
	version, err := buf.ReadByte()
	if err != nil {
		return err
	}
	dp.Version = version
	if dp.Version != DispatcherProgressVersion1 {
		return fmt.Errorf("unsupported DispatcherProgress version: %d", dp.Version)
	}
	dp.DispatcherID.Unmarshal(buf.Next(dp.DispatcherID.GetSize()))
	dp.CheckpointTs = binary.BigEndian.Uint64(buf.Next(8))
	dp.Epoch = binary.BigEndian.Uint64(buf.Next(8))
	return nil
}

// DispatcherHeartbeat is used to report the progress of a dispatcher to the EventService
type DispatcherHeartbeat struct {
	Version                    int
	ClusterID                  uint64
	DispatcherCount            uint32
	DispatcherProgressesLegacy []DispatcherProgressLegacy
	DispatcherProgresses       []DispatcherProgress
}

func NewDispatcherHeartbeat() *DispatcherHeartbeat {
	return &DispatcherHeartbeat{
		Version: DispatcherHeartbeatVersion2,
		// TODO: Pass a real clusterID when we support 1 TiCDC cluster subscribe multiple TiDB clusters
		ClusterID:                  0,
		DispatcherProgressesLegacy: make([]DispatcherProgressLegacy, 0),
		DispatcherProgresses:       make([]DispatcherProgress, 0),
	}
}

func (d *DispatcherHeartbeat) AppendLegacy(dp DispatcherProgressLegacy) {
	d.DispatcherCount++
	d.DispatcherProgressesLegacy = append(d.DispatcherProgressesLegacy, dp)
}

func (d *DispatcherHeartbeat) AddDispatcherProgress(dispatcherID common.DispatcherID, checkpointTs uint64, epoch uint64) {
	d.DispatcherCount++
	if d.Version >= DispatcherHeartbeatVersion2 {
		d.DispatcherProgresses = append(d.DispatcherProgresses, DispatcherProgress{
			Version:      DispatcherProgressVersion1,
			DispatcherID: dispatcherID,
			CheckpointTs: checkpointTs,
			Epoch:        epoch,
		})
		return
	}
	d.DispatcherProgressesLegacy = append(d.DispatcherProgressesLegacy, DispatcherProgressLegacy{
		DispatcherID: dispatcherID,
		CheckpointTs: checkpointTs,
	})
}

func (d *DispatcherHeartbeat) GetSize() int {
	size := 8 // clusterID
	size += 4 // dispatcher count
	if d.Version >= DispatcherHeartbeatVersion2 {
		for _, dp := range d.DispatcherProgresses {
			size += dp.GetSize()
		}
		return size
	}
	for _, dp := range d.DispatcherProgressesLegacy {
		size += dp.GetSize()
	}
	return size
}

func (d *DispatcherHeartbeat) Marshal() ([]byte, error) {
	// 1. Encode payload based on version
	var payload []byte
	var err error
	switch d.Version {
	case DispatcherHeartbeatVersion1:
		payload, err = d.encodeV1()
		if err != nil {
			return nil, err
		}
	case DispatcherHeartbeatVersion2:
		payload, err = d.encodeV2()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported DispatcherHeartbeat version: %d", d.Version)
	}

	// 2. Use unified header format
	return MarshalEventWithHeader(TypeDispatcherHeartbeat, d.Version, payload)
}

func (d *DispatcherHeartbeat) Unmarshal(data []byte) error {
	// 1. Validate header and extract payload
	payload, version, err := ValidateAndExtractPayload(data, TypeDispatcherHeartbeat)
	if err != nil {
		return err
	}

	// 2. Store version
	d.Version = version

	// 3. Decode based on version
	switch version {
	case DispatcherHeartbeatVersion1:
		return d.decodeV1(payload)
	case DispatcherHeartbeatVersion2:
		return d.decodeV2(payload)
	default:
		return fmt.Errorf("unsupported DispatcherHeartbeat version: %d", version)
	}
}

func (d *DispatcherHeartbeat) encodeV1() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	binary.Write(buf, binary.BigEndian, d.ClusterID)
	binary.Write(buf, binary.BigEndian, d.DispatcherCount)
	for _, dp := range d.DispatcherProgressesLegacy {
		dpData, err := dp.encodeV1()
		if err != nil {
			return nil, err
		}
		buf.Write(dpData)
	}
	return buf.Bytes(), nil
}

func (d *DispatcherHeartbeat) decodeV1(data []byte) error {
	buf := bytes.NewBuffer(data)
	d.ClusterID = binary.BigEndian.Uint64(buf.Next(8))
	d.DispatcherCount = binary.BigEndian.Uint32(buf.Next(4))
	d.DispatcherProgressesLegacy = make([]DispatcherProgressLegacy, 0, d.DispatcherCount)
	d.DispatcherProgresses = nil
	for range d.DispatcherCount {
		var dp DispatcherProgressLegacy
		dpData := buf.Next(dp.GetSize())
		if err := dp.decodeV1(dpData); err != nil {
			return err
		}
		d.DispatcherProgressesLegacy = append(d.DispatcherProgressesLegacy, dp)
	}
	return nil
}

func (d *DispatcherHeartbeat) encodeV2() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	binary.Write(buf, binary.BigEndian, d.ClusterID)
	binary.Write(buf, binary.BigEndian, d.DispatcherCount)
	for _, dp := range d.DispatcherProgresses {
		dpData, err := dp.encode()
		if err != nil {
			return nil, err
		}
		buf.Write(dpData)
	}
	return buf.Bytes(), nil
}

func (d *DispatcherHeartbeat) decodeV2(data []byte) error {
	buf := bytes.NewBuffer(data)
	d.ClusterID = binary.BigEndian.Uint64(buf.Next(8))
	d.DispatcherCount = binary.BigEndian.Uint32(buf.Next(4))
	d.DispatcherProgressesLegacy = nil
	d.DispatcherProgresses = make([]DispatcherProgress, 0, d.DispatcherCount)
	for range d.DispatcherCount {
		var dp DispatcherProgress
		dpData := buf.Next(dp.GetSize())
		if err := dp.decode(dpData); err != nil {
			return err
		}
		d.DispatcherProgresses = append(d.DispatcherProgresses, dp)
	}
	return nil
}

type DSState byte

const (
	DSStateNormal DSState = iota
	DSStateRemoved
)

// It is a part of DispatcherHeartbeatResponse, so it has no version field.
type DispatcherState struct {
	State        DSState
	DispatcherID common.DispatcherID
}

func NewDispatcherState(dispatcherID common.DispatcherID, state DSState) DispatcherState {
	return DispatcherState{
		State:        state,
		DispatcherID: dispatcherID,
	}
}

func (d *DispatcherState) GetSize() int {
	return d.DispatcherID.GetSize() + 1 // + state
}

func (d DispatcherState) Marshal() ([]byte, error) {
	return d.encodeV1()
}

func (d *DispatcherState) Unmarshal(data []byte) error {
	return d.decodeV1(data)
}

func (d *DispatcherState) decodeV1(data []byte) error {
	buf := bytes.NewBuffer(data)
	var err error
	if err != nil {
		return err
	}
	d.DispatcherID.Unmarshal(buf.Next(d.DispatcherID.GetSize()))
	state, err := buf.ReadByte()
	if err != nil {
		return err
	}
	d.State = DSState(state)
	return nil
}

func (d *DispatcherState) encodeV1() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.Write(d.DispatcherID.Marshal())
	buf.WriteByte(byte(d.State))
	return buf.Bytes(), nil
}

type DispatcherHeartbeatResponse struct {
	Version   int
	ClusterID uint64
	// DispatcherCount is use for decoding of the response.
	DispatcherCount  uint32
	DispatcherStates []DispatcherState
}

func NewDispatcherHeartbeatResponse() *DispatcherHeartbeatResponse {
	return &DispatcherHeartbeatResponse{
		Version: DispatcherHeartbeatResponseVersion1,
		// TODO: Pass a real clusterID when we support 1 TiCDC cluster subscribe multiple TiDB clusters
		ClusterID:        0,
		DispatcherCount:  0,
		DispatcherStates: make([]DispatcherState, 0, 32),
	}
}

func (d *DispatcherHeartbeatResponse) Append(ds DispatcherState) {
	d.DispatcherCount++
	d.DispatcherStates = append(d.DispatcherStates, ds)
}

func (d *DispatcherHeartbeatResponse) GetSize() int {
	size := 8 // clusterID
	size += 4 // dispatcher count
	for _, ds := range d.DispatcherStates {
		size += ds.GetSize()
	}
	return size
}

func (d *DispatcherHeartbeatResponse) Marshal() ([]byte, error) {
	// 1. Encode payload based on version
	var payload []byte
	var err error
	switch d.Version {
	case DispatcherHeartbeatResponseVersion1:
		payload, err = d.encodeV1()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported DispatcherHeartbeatResponse version: %d", d.Version)
	}
	// 2. Use unified header format
	return MarshalEventWithHeader(TypeDispatcherHeartbeatResponse, d.Version, payload)
}

func (d *DispatcherHeartbeatResponse) Unmarshal(data []byte) error {
	// 1. Parse unified header
	eventType, version, payloadLen, err := UnmarshalEventHeader(data)
	if err != nil {
		return err
	}

	// 2. Validate event type
	if eventType != TypeDispatcherHeartbeatResponse {
		return fmt.Errorf("expected DispatcherHeartbeatResponse (type %d), got type %d (%s)",
			TypeDispatcherHeartbeatResponse, eventType, TypeToString(eventType))
	}

	// 3. Validate total data length
	headerSize := GetEventHeaderSize()
	expectedLen := uint64(headerSize) + payloadLen
	if uint64(len(data)) < expectedLen {
		return fmt.Errorf("incomplete data: expected %d bytes (header %d + payload %d), got %d",
			expectedLen, headerSize, payloadLen, len(data))
	}

	// 4. Extract payload
	payload := data[headerSize:expectedLen]

	// 5. Store version
	d.Version = version

	// 6. Decode based on version
	switch version {
	case DispatcherHeartbeatResponseVersion1:
		return d.decodeV1(payload)
	default:
		return fmt.Errorf("unsupported DispatcherHeartbeatResponse version: %d", version)
	}
}

func (d *DispatcherHeartbeatResponse) encodeV1() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	binary.Write(buf, binary.BigEndian, d.ClusterID)
	binary.Write(buf, binary.BigEndian, d.DispatcherCount)
	for _, ds := range d.DispatcherStates {
		dsData, err := ds.Marshal()
		if err != nil {
			return nil, err
		}
		buf.Write(dsData)
	}
	return buf.Bytes(), nil
}

func (d *DispatcherHeartbeatResponse) decodeV1(data []byte) error {
	buf := bytes.NewBuffer(data)
	d.ClusterID = binary.BigEndian.Uint64(buf.Next(8))
	d.DispatcherCount = binary.BigEndian.Uint32(buf.Next(4))
	d.DispatcherStates = make([]DispatcherState, 0, d.DispatcherCount)
	for range d.DispatcherCount {
		var ds DispatcherState
		dsData := buf.Next(ds.GetSize())
		if err := ds.Unmarshal(dsData); err != nil {
			return err
		}
		d.DispatcherStates = append(d.DispatcherStates, ds)
	}
	return nil
}
