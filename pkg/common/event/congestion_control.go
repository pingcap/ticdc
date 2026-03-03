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
	"math"

	"github.com/pingcap/ticdc/pkg/common"
)

const (
	CongestionControlVersion1 = 1
	CongestionControlVersion2 = 2
)

type AvailableMemory struct {
	Version             byte                           // 1 byte, it should be the same as CongestionControlVersion
	Gid                 common.GID                     // GID is the internal representation of ChangeFeedID
	Available           uint64                         // in bytes, used to report the available memory
	UsageRatio          float64                        // [0,1], used to report memory usage ratio of the changefeed
	DispatcherCount     uint32                         // used to report the number of dispatchers
	DispatcherAvailable map[common.DispatcherID]uint64 // in bytes, used to report the memory usage of each dispatcher
	MemoryReleaseCount  uint32                         // used to report the number of memory release events
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
	return m.marshalV1()
}

func (m *AvailableMemory) Unmarshal(buf *bytes.Buffer) {
	m.unmarshalV1(buf)
}

func (m AvailableMemory) GetSize() int {
	return m.sizeV1()
}

func (m AvailableMemory) marshalV1() []byte {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.Write(m.Gid.Marshal())
	_ = binary.Write(buf, binary.BigEndian, m.Available)
	_ = binary.Write(buf, binary.BigEndian, m.DispatcherCount)
	for dispatcherID, available := range m.DispatcherAvailable {
		buf.Write(dispatcherID.Marshal())
		_ = binary.Write(buf, binary.BigEndian, available)
	}
	return buf.Bytes()
}

func (m AvailableMemory) marshalV2() []byte {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.Write(m.Gid.Marshal())
	_ = binary.Write(buf, binary.BigEndian, m.Available)
	_ = binary.Write(buf, binary.BigEndian, math.Float64bits(m.UsageRatio))
	_ = binary.Write(buf, binary.BigEndian, m.DispatcherCount)
	for dispatcherID, available := range m.DispatcherAvailable {
		buf.Write(dispatcherID.Marshal())
		_ = binary.Write(buf, binary.BigEndian, available)
	}
	return buf.Bytes()
}

func (m *AvailableMemory) unmarshalV1(buf *bytes.Buffer) error {
	gidSize := m.Gid.GetSize()
	if buf.Len() < gidSize {
		return fmt.Errorf("invalid AvailableMemory payload: insufficient bytes for gid, need %d, got %d", gidSize, buf.Len())
	}
	m.Gid.Unmarshal(buf.Next(gidSize))

	if buf.Len() < 8 {
		return fmt.Errorf("invalid AvailableMemory payload: insufficient bytes for available, need %d, got %d", 8, buf.Len())
	}
	m.Available = binary.BigEndian.Uint64(buf.Next(8))

	if buf.Len() < 4 {
		return fmt.Errorf("invalid AvailableMemory payload: insufficient bytes for dispatcher count, need %d, got %d", 4, buf.Len())
	}
	m.DispatcherCount = binary.BigEndian.Uint32(buf.Next(4))
	m.DispatcherAvailable = make(map[common.DispatcherID]uint64)
	for range m.DispatcherCount {
		dispatcherID := common.DispatcherID{}
		dispatcherIDSize := dispatcherID.GetSize()
		if buf.Len() < dispatcherIDSize {
			return fmt.Errorf("invalid AvailableMemory payload: insufficient bytes for dispatcher id, need %d, got %d", dispatcherIDSize, buf.Len())
		}
		dispatcherID.Unmarshal(buf.Next(dispatcherIDSize))

		if buf.Len() < 8 {
			return fmt.Errorf("invalid AvailableMemory payload: insufficient bytes for dispatcher available, need %d, got %d", 8, buf.Len())
		}
		m.DispatcherAvailable[dispatcherID] = binary.BigEndian.Uint64(buf.Next(8))
	}
	return nil
}

func (m *AvailableMemory) unmarshalV2(buf *bytes.Buffer) error {
	gidSize := m.Gid.GetSize()
	if buf.Len() < gidSize {
		return fmt.Errorf("invalid AvailableMemory payload: insufficient bytes for gid, need %d, got %d", gidSize, buf.Len())
	}
	m.Gid.Unmarshal(buf.Next(gidSize))

	if buf.Len() < 8 {
		return fmt.Errorf("invalid AvailableMemory payload: insufficient bytes for available, need %d, got %d", 8, buf.Len())
	}
	m.Available = binary.BigEndian.Uint64(buf.Next(8))

	if buf.Len() < 8 {
		return fmt.Errorf("invalid AvailableMemory payload: insufficient bytes for usage ratio, need %d, got %d", 8, buf.Len())
	}
	m.UsageRatio = math.Float64frombits(binary.BigEndian.Uint64(buf.Next(8)))

	if buf.Len() < 4 {
		return fmt.Errorf("invalid AvailableMemory payload: insufficient bytes for dispatcher count, need %d, got %d", 4, buf.Len())
	}
	m.DispatcherCount = binary.BigEndian.Uint32(buf.Next(4))
	m.DispatcherAvailable = make(map[common.DispatcherID]uint64)
	for range m.DispatcherCount {
		dispatcherID := common.DispatcherID{}
		dispatcherIDSize := dispatcherID.GetSize()
		if buf.Len() < dispatcherIDSize {
			return fmt.Errorf("invalid AvailableMemory payload: insufficient bytes for dispatcher id, need %d, got %d", dispatcherIDSize, buf.Len())
		}
		dispatcherID.Unmarshal(buf.Next(dispatcherIDSize))

		if buf.Len() < 8 {
			return fmt.Errorf("invalid AvailableMemory payload: insufficient bytes for dispatcher available, need %d, got %d", 8, buf.Len())
		}
		m.DispatcherAvailable[dispatcherID] = binary.BigEndian.Uint64(buf.Next(8))
	}
	return nil
}

func (m AvailableMemory) sizeV1() int {
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

func (m AvailableMemory) sizeV2() int {
	// changefeedID size + changefeed available size + usage ratio size
	size := m.Gid.GetSize() + 8 + 8
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

func NewCongestionControlWithVersion(version int) *CongestionControl {
	return &CongestionControl{
		version: version,
	}
}

func (c *CongestionControl) GetSize() int {
	size := 8 // clusterID
	size += 4 // changefeed count
	for _, mem := range c.availables {
		switch c.version {
		case CongestionControlVersion2:
			size += mem.sizeV2()
		default:
			size += mem.sizeV1()
		}
	}
	if c.version == CongestionControlVersion2 {
		gidSize := (&common.GID{}).GetSize()
		releaseEntryCount := 0
		for _, mem := range c.availables {
			if mem.MemoryReleaseCount > 0 {
				releaseEntryCount++
			}
		}
		if releaseEntryCount > 0 {
			size += 4 + releaseEntryCount*(gidSize+4)
		}
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
	case CongestionControlVersion2:
		payload, err = c.encodeV2()
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
		data := item.marshalV1()
		buf.Write(data)
	}
	return buf.Bytes(), nil
}

func (c *CongestionControl) encodeV2() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	_ = binary.Write(buf, binary.BigEndian, c.clusterID)
	_ = binary.Write(buf, binary.BigEndian, c.changefeedCount)

	for _, item := range c.availables {
		data := item.marshalV2()
		buf.Write(data)
	}

	releaseEntryCount := uint32(0)
	for _, item := range c.availables {
		if item.MemoryReleaseCount > 0 {
			releaseEntryCount++
		}
	}
	if releaseEntryCount > 0 {
		_ = binary.Write(buf, binary.BigEndian, releaseEntryCount)
		for _, item := range c.availables {
			if item.MemoryReleaseCount == 0 {
				continue
			}
			buf.Write(item.Gid.Marshal())
			_ = binary.Write(buf, binary.BigEndian, item.MemoryReleaseCount)
		}
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
	case CongestionControlVersion2:
		return c.decodeV2(payload)
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
		if err := item.unmarshalV1(buf); err != nil {
			return err
		}
		c.availables = append(c.availables, item)
	}
	return nil
}

func (c *CongestionControl) decodeV2(data []byte) error {
	buf := bytes.NewBuffer(data)
	c.clusterID = binary.BigEndian.Uint64(buf.Next(8))
	c.changefeedCount = binary.BigEndian.Uint32(buf.Next(4))
	c.availables = make([]AvailableMemory, 0, c.changefeedCount)
	for i := uint32(0); i < c.changefeedCount; i++ {
		var item AvailableMemory
		if err := item.unmarshalV2(buf); err != nil {
			return err
		}
		c.availables = append(c.availables, item)
	}

	if buf.Len() == 0 {
		return nil
	}

	if buf.Len() < 4 {
		return fmt.Errorf("invalid CongestionControl payload: insufficient bytes for memory release section count, need %d, got %d", 4, buf.Len())
	}

	releaseEntryCount := binary.BigEndian.Uint32(buf.Next(4))
	if releaseEntryCount == 0 {
		if buf.Len() != 0 {
			return fmt.Errorf("invalid CongestionControl payload: unexpected bytes after empty memory release section, got %d", buf.Len())
		}
		return nil
	}

	gidSize := (&common.GID{}).GetSize()
	expectedBytes := int(releaseEntryCount) * (gidSize + 4)
	if buf.Len() != expectedBytes {
		return fmt.Errorf("invalid CongestionControl payload: inconsistent memory release section length, expected %d, got %d", expectedBytes, buf.Len())
	}

	releaseByGid := make(map[common.GID]uint32, releaseEntryCount)
	for i := uint32(0); i < releaseEntryCount; i++ {
		gid := common.GID{}
		gid.Unmarshal(buf.Next(gidSize))
		releaseByGid[gid] = binary.BigEndian.Uint32(buf.Next(4))
	}

	for i := range c.availables {
		c.availables[i].MemoryReleaseCount = releaseByGid[c.availables[i].Gid]
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

func (c *CongestionControl) AddAvailableMemoryWithDispatchersAndUsage(
	gid common.GID,
	available uint64,
	usageRatio float64,
	dispatcherAvailable map[common.DispatcherID]uint64,
) {
	c.changefeedCount++
	availMem := NewAvailableMemory(gid, available)
	availMem.Version = CongestionControlVersion2
	availMem.UsageRatio = usageRatio
	availMem.DispatcherAvailable = dispatcherAvailable
	availMem.DispatcherCount = uint32(len(dispatcherAvailable))
	c.availables = append(c.availables, availMem)
}

func (c *CongestionControl) AddAvailableMemoryWithDispatchersAndUsageAndReleaseCount(
	gid common.GID,
	available uint64,
	usageRatio float64,
	dispatcherAvailable map[common.DispatcherID]uint64,
	memoryReleaseCount uint32,
) {
	c.changefeedCount++
	availMem := NewAvailableMemory(gid, available)
	availMem.Version = CongestionControlVersion2
	availMem.UsageRatio = usageRatio
	availMem.DispatcherAvailable = dispatcherAvailable
	availMem.DispatcherCount = uint32(len(dispatcherAvailable))
	availMem.MemoryReleaseCount = memoryReleaseCount
	c.availables = append(c.availables, availMem)
}

func (c *CongestionControl) GetAvailables() []AvailableMemory {
	return c.availables
}

func (c *CongestionControl) GetClusterID() uint64 {
	return c.clusterID
}

func (c *CongestionControl) GetVersion() int {
	return c.version
}

func (c *CongestionControl) HasUsageRatio() bool {
	return c.version >= CongestionControlVersion2
}
