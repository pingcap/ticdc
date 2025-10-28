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

	CongestionControlVersion = 1
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

func (dp DispatcherProgress) GetSize() int {
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
	dp.DispatcherID.Unmarshal(buf.Next(dp.DispatcherID.GetSize()))
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

func (d *DispatcherHeartbeat) GetSize() int {
	size := 1 // version
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
		dpData := buf.Next(dp.GetSize())
		if err := dp.Unmarshal(dpData); err != nil {
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

func (d *DispatcherState) GetSize() int {
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
	d.DispatcherID.Unmarshal(buf.Next(d.DispatcherID.GetSize()))
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

func (d *DispatcherHeartbeatResponse) GetSize() int {
	size := 1 // version
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
		dsData := buf.Next(ds.GetSize())
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

type AvailableMemory struct {
	Gid                 common.GID                     // GID is the internal representation of ChangeFeedID
	Available           uint64                         // in bytes, used to report the Available memory
	DispatcherCount     uint32                         // used to report the number of dispatchers
	DispatcherAvailable map[common.DispatcherID]uint64 // in bytes, used to report the memory usage of each dispatcher
}

func NewAvailableMemory(gid common.GID, available uint64) AvailableMemory {
	return AvailableMemory{
		Gid:                 gid,
		Available:           available,
		DispatcherAvailable: make(map[common.DispatcherID]uint64),
	}
}

func (m AvailableMemory) Marshal() []byte {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.Write(m.Gid.Marshal())
	binary.Write(buf, binary.BigEndian, m.Available)
	binary.Write(buf, binary.BigEndian, m.DispatcherCount)
	for dispatcherID, available := range m.DispatcherAvailable {
		buf.Write(dispatcherID.Marshal())
		binary.Write(buf, binary.BigEndian, available)
	}
	return buf.Bytes()
}

func (m *AvailableMemory) Unmarshal(buf *bytes.Buffer) {
	m.Gid.Unmarshal(buf.Next(m.Gid.GetSize()))
	m.Available = binary.BigEndian.Uint64(buf.Next(8))
	m.DispatcherCount = binary.BigEndian.Uint32(buf.Next(4))
	m.DispatcherAvailable = make(map[common.DispatcherID]uint64)
	for range m.DispatcherCount {
		dispatcherID := common.DispatcherID{}
		dispatcherID.Unmarshal(buf.Next(dispatcherID.GetSize()))
		m.DispatcherAvailable[dispatcherID] = binary.BigEndian.Uint64(buf.Next(8))
	}
}

func (m AvailableMemory) GetSize() int {
	// changefeedID size + changefeed available size
	size := m.Gid.GetSize() + 8
	size += 4 // dispatcher count
	for range m.DispatcherCount {
		dispatcherID := &common.DispatcherID{}
		// dispatcherID size + dispatcher available size
		size += dispatcherID.GetSize() + 8
	}
	return size
}

type CongestionControl struct {
	version   byte
	clusterID uint64

	changefeedCount uint32
	availables      []AvailableMemory
}

func NewCongestionControl() *CongestionControl {
	return &CongestionControl{
		version: CongestionControlVersion,
	}
}

func (c *CongestionControl) GetSize() int {
	size := 1 // version
	size += 8 // clusterID

	size += 4 // changefeed count
	for _, mem := range c.availables {
		size += mem.GetSize()
	}
	return size
}

func (c *CongestionControl) Marshal() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(c.version)
	_ = binary.Write(buf, binary.BigEndian, c.clusterID)
	_ = binary.Write(buf, binary.BigEndian, c.changefeedCount)

	for _, item := range c.availables {
		data := item.Marshal()
		buf.Write(data)
	}
	return buf.Bytes(), nil
}

func (c *CongestionControl) Unmarshal(data []byte) error {
	buf := bytes.NewBuffer(data)
	var err error
	c.version, err = buf.ReadByte()
	if err != nil {
		return err
	}
	c.clusterID = binary.BigEndian.Uint64(buf.Next(8))
	c.changefeedCount = binary.BigEndian.Uint32(buf.Next(4))
	c.availables = make([]AvailableMemory, 0, c.changefeedCount)
	for i := uint32(0); i < c.changefeedCount; i++ {
		var item AvailableMemory
		item.Unmarshal(buf)
		c.availables = append(c.availables, item)
	}
	return nil
}

func (c *CongestionControl) AddAvailableMemory(gid common.GID, available uint64) {
	c.changefeedCount++
	c.availables = append(c.availables, NewAvailableMemory(gid, available))
}

func (c *CongestionControl) AddAvailableMemoryWithDispatchers(gid common.GID, available uint64, dispatcherAvailable map[common.DispatcherID]uint64) {
	c.changefeedCount++
	availMem := NewAvailableMemory(gid, available)
	availMem.DispatcherAvailable = dispatcherAvailable
	availMem.DispatcherCount = uint32(len(dispatcherAvailable))
	c.availables = append(c.availables, availMem)
}

func (c *CongestionControl) GetAvailables() []AvailableMemory {
	return c.availables
}

func (c *CongestionControl) GetClusterID() uint64 {
	return c.clusterID
}
