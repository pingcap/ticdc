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

const CongestionControlVersion1 = 1

type AvailableMemory struct {
	Version             byte                           // 1 byte, it should be the same as CongestionControlVersion
	Gid                 common.GID                     // GID is the internal representation of ChangeFeedID
	Available           uint64                         // in bytes, used to report the Available memory
	DispatcherCount     uint32                         // used to report the number of dispatchers
	DispatcherAvailable map[common.DispatcherID]uint64 // in bytes, used to report the memory usage of each dispatcher
}

func NewAvailableMemory(gid common.GID, available uint64) AvailableMemory {
	return AvailableMemory{
		Version:             CongestionControlVersion1,
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
	version   int
	clusterID uint64

	changefeedCount uint32
	availables      []AvailableMemory
}

func NewCongestionControl() *CongestionControl {
	return &CongestionControl{
		version: CongestionControlVersion1,
	}
}

func (c *CongestionControl) GetSize() int {
	size := 8 // clusterID
	size += 4 // changefeed count
	for _, mem := range c.availables {
		size += mem.GetSize()
	}
	return size
}

func (c *CongestionControl) Marshal() ([]byte, error) {
	// 1. Encode payload based on version
	var payload []byte
	var err error
	switch c.version {
	case CongestionControlVersion1:
		payload, err = c.encodeV1()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported CongestionControl version: %d", c.version)
	}

	// 2. Use unified header format
	return MarshalEventWithHeader(TypeCongestionControl, c.version, payload)
}

func (c *CongestionControl) encodeV1() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	_ = binary.Write(buf, binary.BigEndian, c.clusterID)
	_ = binary.Write(buf, binary.BigEndian, c.changefeedCount)

	for _, item := range c.availables {
		data := item.Marshal()
		buf.Write(data)
	}
	return buf.Bytes(), nil
}

func (c *CongestionControl) Unmarshal(data []byte) error {
	// 1. Validate header and extract payload
	payload, version, err := ValidateAndExtractPayload(data, TypeCongestionControl)
	if err != nil {
		return err
	}

	// 2. Store version
	c.version = version

	// 3. Decode based on version
	switch version {
	case CongestionControlVersion1:
		return c.decodeV1(payload)
	default:
		return fmt.Errorf("unsupported CongestionControl version: %d", version)
	}
}

func (c *CongestionControl) decodeV1(data []byte) error {
	buf := bytes.NewBuffer(data)
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
