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
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestCongestionControl(t *testing.T) {
	t.Parallel()

	control := NewCongestionControl()
	bytes, err := control.Marshal()
	require.NoError(t, err)
	require.Equal(t, len(bytes), control.GetSize())

	// Verify header format: [MAGIC(2B)][EVENT_TYPE(1B)][VERSION(1B)][PAYLOAD_LENGTH(4B)]
	require.Greater(t, len(bytes), 8, "data should include header")
	require.Equal(t, byte(0xDA), bytes[0], "magic high byte")
	require.Equal(t, byte(0x7A), bytes[1], "magic low byte")
	require.Equal(t, byte(TypeCongestionControl), bytes[2], "event type")
	require.Equal(t, byte(CongestionControlVersion0), bytes[3], "version byte")

	var decoded CongestionControl
	err = decoded.Unmarshal(bytes)
	require.NoError(t, err)
	require.Equal(t, control.GetClusterID(), decoded.GetClusterID())
	require.Equal(t, len(decoded.availables), len(control.availables))

	control.AddAvailableMemory(common.NewGID(), 1024)
	bytes, err = control.Marshal()
	require.NoError(t, err)
	require.Equal(t, len(bytes), control.GetSize())

	err = decoded.Unmarshal(bytes)
	require.NoError(t, err)

	for idx, item := range control.availables {
		require.Equal(t, item.Gid, decoded.availables[idx].Gid)
		require.Equal(t, item.Available, decoded.availables[idx].Available)
	}
}

func TestCongestionControlAddAvailableMemoryWithDispatchers(t *testing.T) {
	t.Parallel()

	// Test case 1: Add available memory with dispatcher details
	control := NewCongestionControl()
	gid := common.NewGID()
	available := uint64(1000)
	dispatcherAvailable := map[common.DispatcherID]uint64{
		common.NewDispatcherID(): 500,
		common.NewDispatcherID(): 500,
	}

	control.AddAvailableMemoryWithDispatchers(gid, available, dispatcherAvailable)

	availables := control.GetAvailables()
	require.Len(t, availables, 1)

	availableMem := availables[0]
	require.Equal(t, gid, availableMem.Gid)
	require.Equal(t, available, availableMem.Available)
	require.Len(t, availableMem.DispatcherAvailable, 2)
}

func TestCongestionControlMarshalUnmarshal(t *testing.T) {
	t.Parallel()

	// Test case 1: Empty CongestionControl
	control1 := NewCongestionControl()
	control1.clusterID = 12345

	data1, err := control1.Marshal()
	require.NoError(t, err)
	require.Equal(t, control1.GetSize(), len(data1))

	var unmarshaled1 CongestionControl
	err = unmarshaled1.Unmarshal(data1)
	require.NoError(t, err)
	require.Equal(t, control1.GetClusterID(), unmarshaled1.GetClusterID())
	require.Len(t, unmarshaled1.GetAvailables(), 0)

	// Test case 2: CongestionControl with single AvailableMemory
	control2 := NewCongestionControl()
	control2.clusterID = 67890
	gid1 := common.NewGID()
	control2.AddAvailableMemory(gid1, 1024)

	data2, err := control2.Marshal()
	require.NoError(t, err)
	require.Equal(t, control2.GetSize(), len(data2))

	var unmarshaled2 CongestionControl
	err = unmarshaled2.Unmarshal(data2)
	require.NoError(t, err)
	require.Equal(t, control2.GetClusterID(), unmarshaled2.GetClusterID())
	require.Len(t, unmarshaled2.GetAvailables(), 1)
	require.Equal(t, gid1, unmarshaled2.GetAvailables()[0].Gid)
	require.Equal(t, uint64(1024), unmarshaled2.GetAvailables()[0].Available)

	// Test case 3: CongestionControl with multiple AvailableMemory entries
	control3 := NewCongestionControl()
	control3.clusterID = 11111
	gid2 := common.NewGID()
	gid3 := common.NewGID()
	control3.AddAvailableMemory(gid2, 2048)
	control3.AddAvailableMemory(gid3, 4096)

	data3, err := control3.Marshal()
	require.NoError(t, err)
	require.Equal(t, control3.GetSize(), len(data3))

	var unmarshaled3 CongestionControl
	err = unmarshaled3.Unmarshal(data3)
	require.NoError(t, err)
	require.Equal(t, control3.GetClusterID(), unmarshaled3.GetClusterID())
	require.Len(t, unmarshaled3.GetAvailables(), 2)

	// Verify the order and values
	availables := unmarshaled3.GetAvailables()
	require.Equal(t, gid2, availables[0].Gid)
	require.Equal(t, uint64(2048), availables[0].Available)
	require.Equal(t, gid3, availables[1].Gid)
	require.Equal(t, uint64(4096), availables[1].Available)

	// Test case 4: CongestionControl with AvailableMemoryWithDispatchers
	// Note: DispatcherAvailable field is not properly serialized/deserialized in current implementation
	// So we only test the basic GID and Available fields
	control4 := NewCongestionControl()
	control4.clusterID = 22222
	gid4 := common.NewGID()
	dispatcherAvailable := map[common.DispatcherID]uint64{
		common.NewDispatcherID(): 1000,
		common.NewDispatcherID(): 2000,
	}
	control4.AddAvailableMemoryWithDispatchers(gid4, 3000, dispatcherAvailable)

	data4, err := control4.Marshal()
	require.NoError(t, err)
	require.Equal(t, control4.GetSize(), len(data4))

	var unmarshaled4 CongestionControl
	err = unmarshaled4.Unmarshal(data4)
	require.NoError(t, err)
	require.Equal(t, control4.GetClusterID(), unmarshaled4.GetClusterID())
	require.Len(t, unmarshaled4.GetAvailables(), 1)

	availableMem := unmarshaled4.GetAvailables()[0]
	require.Equal(t, gid4, availableMem.Gid)
	require.Equal(t, uint64(3000), availableMem.Available)
	require.Equal(t, uint32(2), availableMem.DispatcherCount)
	require.Len(t, availableMem.DispatcherAvailable, 2)
}

func TestCongestionControlMarshalUnmarshalEdgeCases(t *testing.T) {
	t.Parallel()

	// Test case 1: Very large available memory values
	control1 := NewCongestionControl()
	control1.clusterID = 99999
	gid1 := common.NewGID()
	control1.AddAvailableMemory(gid1, ^uint64(0)) // Maximum uint64 value

	data1, err := control1.Marshal()
	require.NoError(t, err)

	var unmarshaled1 CongestionControl
	err = unmarshaled1.Unmarshal(data1)
	require.NoError(t, err)
	require.Equal(t, ^uint64(0), unmarshaled1.GetAvailables()[0].Available)

	// Test case 2: Zero available memory
	control2 := NewCongestionControl()
	control2.clusterID = 88888
	gid2 := common.NewGID()
	control2.AddAvailableMemory(gid2, 0)

	data2, err := control2.Marshal()
	require.NoError(t, err)

	var unmarshaled2 CongestionControl
	err = unmarshaled2.Unmarshal(data2)
	require.NoError(t, err)
	require.Equal(t, uint64(0), unmarshaled2.GetAvailables()[0].Available)

	// Test case 3: Multiple changefeeds with different memory values
	control3 := NewCongestionControl()
	control3.clusterID = 77777

	// Add multiple changefeeds
	for i := 0; i < 5; i++ {
		gid := common.NewGID()
		control3.AddAvailableMemory(gid, uint64(i*1000))
	}

	data3, err := control3.Marshal()
	require.NoError(t, err)

	var unmarshaled3 CongestionControl
	err = unmarshaled3.Unmarshal(data3)
	require.NoError(t, err)
	require.Len(t, unmarshaled3.GetAvailables(), 5)

	// Verify all values are preserved
	availables := unmarshaled3.GetAvailables()
	for i, available := range availables {
		require.Equal(t, uint64(i*1000), available.Available)
	}
}

func TestCongestionControlHeaderValidation(t *testing.T) {
	t.Parallel()

	control := NewCongestionControl()
	control.clusterID = 12345
	control.AddAvailableMemory(common.NewGID(), 1024)

	data, err := control.Marshal()
	require.NoError(t, err)

	// Make a copy for manipulation
	data2 := make([]byte, len(data))
	copy(data2, data)

	// Test 1: Invalid magic bytes
	data2[0] = 0xFF
	var decoded CongestionControl
	err = decoded.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid magic bytes")

	// Restore for next test
	copy(data2, data)

	// Test 2: Wrong event type
	data2[2] = byte(TypeDDLEvent)
	err = decoded.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected CongestionControl")

	// Restore for next test
	copy(data2, data)

	// Test 3: Unsupported version
	mockVersion := byte(99)
	data2[3] = mockVersion
	err = decoded.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported CongestionControl version")

	// Test 4: Data too short
	shortData := []byte{0xDA, 0x7A, 0x00}
	err = decoded.Unmarshal(shortData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data too short")

	// Test 5: Incomplete payload
	incompleteData := make([]byte, 8)
	incompleteData[0] = 0xDA
	incompleteData[1] = 0x7A
	incompleteData[2] = TypeCongestionControl
	incompleteData[3] = CongestionControlVersion0
	// Set payload length to 100 but don't provide that much data
	incompleteData[4] = 0
	incompleteData[5] = 0
	incompleteData[6] = 0
	incompleteData[7] = 100
	err = decoded.Unmarshal(incompleteData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incomplete data")
}

func TestCongestionControlVersionCompatibility(t *testing.T) {
	t.Parallel()

	// Test that we can successfully marshal and unmarshal with Version0
	control := NewCongestionControl()
	control.clusterID = 54321
	gid := common.NewGID()
	control.AddAvailableMemory(gid, 2048)

	data, err := control.Marshal()
	require.NoError(t, err)

	var decoded CongestionControl
	err = decoded.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, byte(CongestionControlVersion0), decoded.version)
	require.Equal(t, control.clusterID, decoded.clusterID)
	require.Len(t, decoded.availables, 1)
	require.Equal(t, gid, decoded.availables[0].Gid)
	require.Equal(t, uint64(2048), decoded.availables[0].Available)
}
