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
	"errors"

	"github.com/pingcap/ticdc/pkg/common"
)

const (
	DispatcherHeartbeatVersion         = 0
	DispatcherHeartbeatResponseVersion = 0
)

// DispatcherProgress is used to report the progress of a dispatcher to the EventService
type DispatcherProgress struct {
	Version      byte // 1 byte, it should be the same as DispatcherHeartbeatVersion
	DispatcherID common.DispatcherID
	CheckpointTs uint64 // 8 bytes
}

func NewDispatcherProgress(dispatcherID common.DispatcherID, checkpointTs uint64) DispatcherProgress {
	return DispatcherProgress{
		Version:      DispatcherHeartbeatVersion,
		DispatcherID: dispatcherID,
		CheckpointTs: checkpointTs,
	}
}

func (dp DispatcherProgress) GetSize() uint64 {
	return dp.DispatcherID.GetSize() + 8 + 1 // version
}

func (dp DispatcherProgress) Marshal() ([]byte, error) {
	return dp.encodeV0()
}

func (dp *DispatcherProgress) Unmarshal(data []byte) error {
	return dp.decodeV0(data)
}

func (dp DispatcherProgress) encodeV0() ([]byte, error) {
	if dp.Version != 0 {
		return nil, errors.New("invalid version")
	}
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(dp.Version)
	buf.Write(dp.DispatcherID.Marshal())
	binary.Write(buf, binary.BigEndian, dp.CheckpointTs)
	return buf.Bytes(), nil
}

func (dp *DispatcherProgress) decodeV0(data []byte) error {
	buf := bytes.NewBuffer(data)
	var err error
	dp.Version, err = buf.ReadByte()
	if err != nil {
		return err
	}
	dp.DispatcherID.Unmarshal(buf.Next(int(dp.DispatcherID.GetSize())))
	dp.CheckpointTs = binary.BigEndian.Uint64(buf.Next(8))
	return nil
}

// DispatcherHeartbeat is used to report the progress of a dispatcher to the EventService
type DispatcherHeartbeat struct {
	Version              byte
	ClusterID            uint64
	DispatcherCount      uint32
	DispatcherProgresses []DispatcherProgress
}

func NewDispatcherHeartbeat(dispatcherCount int) *DispatcherHeartbeat {
	return &DispatcherHeartbeat{
		Version: DispatcherHeartbeatVersion,
		// TODO: Pass a real clusterID when we support 1 TiCDC cluster subscribe multiple TiDB clusters
		ClusterID:            0,
		DispatcherProgresses: make([]DispatcherProgress, 0, dispatcherCount),
	}
}

func (d *DispatcherHeartbeat) Append(dp DispatcherProgress) {
	d.DispatcherCount++
	d.DispatcherProgresses = append(d.DispatcherProgresses, dp)
}

func (d *DispatcherHeartbeat) GetSize() uint64 {
	var size uint64
	size += 1 // version
	size += 8 // clusterID
	size += 4 // dispatcher count
	for _, dp := range d.DispatcherProgresses {
		size += dp.GetSize()
	}
	return size
}

func (d *DispatcherHeartbeat) Marshal() ([]byte, error) {
	return d.encodeV0()
}

func (d *DispatcherHeartbeat) Unmarshal(data []byte) error {
	return d.decodeV0(data)
}

func (d *DispatcherHeartbeat) encodeV0() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(d.Version)
	binary.Write(buf, binary.BigEndian, d.ClusterID)
	binary.Write(buf, binary.BigEndian, d.DispatcherCount)
	for _, dp := range d.DispatcherProgresses {
		dpData, err := dp.Marshal()
		if err != nil {
			return nil, err
		}
		buf.Write(dpData)
	}
	return buf.Bytes(), nil
}

func (d *DispatcherHeartbeat) decodeV0(data []byte) error {
	buf := bytes.NewBuffer(data)
	var err error
	d.Version, err = buf.ReadByte()
	if err != nil {
		return err
	}
	d.ClusterID = binary.BigEndian.Uint64(buf.Next(8))
	d.DispatcherCount = binary.BigEndian.Uint32(buf.Next(4))
	d.DispatcherProgresses = make([]DispatcherProgress, 0, d.DispatcherCount)
	for range d.DispatcherCount {
		var dp DispatcherProgress
		dpData := buf.Next(int(dp.GetSize()))
		if err = dp.Unmarshal(dpData); err != nil {
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

type DispatcherState struct {
	Version      byte // 1 byte, it should be the same as DispatcherHeartbeatResponseVersion
	State        DSState
	DispatcherID common.DispatcherID
}

func NewDispatcherState(dispatcherID common.DispatcherID, state DSState) DispatcherState {
	return DispatcherState{
		Version:      DispatcherHeartbeatResponseVersion,
		State:        state,
		DispatcherID: dispatcherID,
	}
}

func (d *DispatcherState) GetSize() uint64 {
	return d.DispatcherID.GetSize() + 2 // version + state
}

func (d DispatcherState) Marshal() ([]byte, error) {
	return d.encodeV0()
}

func (d *DispatcherState) Unmarshal(data []byte) error {
	return d.decodeV0(data)
}

func (d *DispatcherState) decodeV0(data []byte) error {
	buf := bytes.NewBuffer(data)
	var err error
	d.Version, err = buf.ReadByte()
	if err != nil {
		return err
	}
	d.DispatcherID.Unmarshal(buf.Next(int(d.DispatcherID.GetSize())))
	state, err := buf.ReadByte()
	if err != nil {
		return err
	}
	d.State = DSState(state)
	return nil
}

func (d *DispatcherState) encodeV0() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(d.Version)
	buf.Write(d.DispatcherID.Marshal())
	buf.WriteByte(byte(d.State))
	return buf.Bytes(), nil
}

type DispatcherHeartbeatResponse struct {
	Version   byte
	ClusterID uint64
	// DispatcherCount is use for decoding of the response.
	DispatcherCount  uint32
	DispatcherStates []DispatcherState
}

func NewDispatcherHeartbeatResponse() *DispatcherHeartbeatResponse {
	return &DispatcherHeartbeatResponse{
		Version: DispatcherHeartbeatVersion,
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

func (d *DispatcherHeartbeatResponse) GetSize() uint64 {
	var size uint64
	size += 1 // version
	size += 8 // clusterID
	size += 4 // dispatcher count
	for _, ds := range d.DispatcherStates {
		size += ds.GetSize()
	}
	return size
}

func (d DispatcherHeartbeatResponse) Marshal() ([]byte, error) {
	return d.encodeV0()
}

func (d *DispatcherHeartbeatResponse) Unmarshal(data []byte) error {
	return d.decodeV0(data)
}

func (d *DispatcherHeartbeatResponse) decodeV0(data []byte) error {
	buf := bytes.NewBuffer(data)
	var err error
	d.Version, err = buf.ReadByte()
	if err != nil {
		return err
	}
	d.ClusterID = binary.BigEndian.Uint64(buf.Next(8))
	d.DispatcherCount = binary.BigEndian.Uint32(buf.Next(4))
	d.DispatcherStates = make([]DispatcherState, 0, d.DispatcherCount)
	for range d.DispatcherCount {
		var ds DispatcherState
		dsData := buf.Next(int(ds.GetSize()))
		if err = ds.Unmarshal(dsData); err != nil {
			return err
		}
		d.DispatcherStates = append(d.DispatcherStates, ds)
	}
	return nil
}

func (d *DispatcherHeartbeatResponse) encodeV0() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(d.Version)
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
