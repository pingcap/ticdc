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

func TestDispatcherProgress(t *testing.T) {
	t.Parallel()
	// Test GetSize function
	dispatcherID := common.NewDispatcherID()
	progress := DispatcherProgress{
		Version:      0,
		DispatcherID: dispatcherID,
		CheckpointTs: 123456789,
	}
	expectedSize := dispatcherID.GetSize() + 8 + 1 // dispatcherID size + checkpointTs size + version size
	require.Equal(t, expectedSize, progress.GetSize())

	// Test Marshal and Unmarshal
	data, err := progress.Marshal()
	require.NoError(t, err)
	require.Len(t, data, progress.GetSize())

	var unmarshalledProgress DispatcherProgress
	err = unmarshalledProgress.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, progress.Version, unmarshalledProgress.Version)
	require.Equal(t, progress.CheckpointTs, unmarshalledProgress.CheckpointTs)
	require.Equal(t, progress.DispatcherID, unmarshalledProgress.DispatcherID)

	// Test invalid version
	invalidProgress := DispatcherProgress{
		Version:      1, // Invalid version
		DispatcherID: dispatcherID,
		CheckpointTs: 123456789,
	}
	_, err = invalidProgress.Marshal()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid version")
}

func TestDispatcherHeartbeat(t *testing.T) {
	t.Parallel()
	// Test NewDispatcherHeartbeat
	dispatcherCount := 3
	heartbeat := NewDispatcherHeartbeat(dispatcherCount)
	require.Equal(t, byte(DispatcherHeartbeatVersion), heartbeat.Version)
	require.Empty(t, heartbeat.DispatcherProgresses)
	require.Equal(t, dispatcherCount, cap(heartbeat.DispatcherProgresses))

	// Test Append
	dispatcherID1 := common.NewDispatcherID()
	progress1 := DispatcherProgress{
		Version:      0,
		DispatcherID: dispatcherID1,
		CheckpointTs: 100,
	}
	heartbeat.Append(progress1)
	require.Len(t, heartbeat.DispatcherProgresses, 1)
	require.Equal(t, progress1, heartbeat.DispatcherProgresses[0])

	dispatcherID2 := common.NewDispatcherID()
	progress2 := DispatcherProgress{
		Version:      0,
		DispatcherID: dispatcherID2,
		CheckpointTs: 200,
	}
	heartbeat.Append(progress2)
	require.Len(t, heartbeat.DispatcherProgresses, 2)
	require.Equal(t, progress2, heartbeat.DispatcherProgresses[1])

	// Test GetSize
	expectedSize := 1 + 4 + 8 + progress1.GetSize() + progress2.GetSize() // version(byte) + clusterID(uint64) + dispatcher count(uint32) + progress1 size + progress2 size
	require.Equal(t, expectedSize, heartbeat.GetSize())

	// Test Marshal and Unmarshal
	heartbeat.DispatcherCount = uint32(len(heartbeat.DispatcherProgresses))
	data, err := heartbeat.Marshal()
	require.NoError(t, err)
	require.Len(t, data, heartbeat.GetSize())

	var unmarshalledResponse DispatcherHeartbeat
	err = unmarshalledResponse.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, heartbeat.Version, unmarshalledResponse.Version)
	require.Equal(t, heartbeat.DispatcherCount, unmarshalledResponse.DispatcherCount)
	require.Equal(t, len(heartbeat.DispatcherProgresses), len(unmarshalledResponse.DispatcherProgresses))

	for i, progress := range heartbeat.DispatcherProgresses {
		require.Equal(t, progress.Version, unmarshalledResponse.DispatcherProgresses[i].Version)
		require.Equal(t, progress.CheckpointTs, unmarshalledResponse.DispatcherProgresses[i].CheckpointTs)
		require.Equal(t, progress.DispatcherID, unmarshalledResponse.DispatcherProgresses[i].DispatcherID)
	}

	// Test with invalid progress version
	heartbeat.DispatcherProgresses[0].Version = 1 // Invalid version
	_, err = heartbeat.Marshal()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid version")
}

func TestDispatcherHeartbeatWithMultipleDispatchers(t *testing.T) {
	t.Parallel()
	// Create multiple dispatchers
	dispatcherCount := 5
	heartbeat := NewDispatcherHeartbeat(dispatcherCount)

	// Add progress for each dispatcher
	for i := 0; i < dispatcherCount; i++ {
		progress := DispatcherProgress{
			Version:      0,
			DispatcherID: common.NewDispatcherID(),
			CheckpointTs: uint64(i * 100),
		}
		heartbeat.Append(progress)
	}

	require.Len(t, heartbeat.DispatcherProgresses, dispatcherCount)
	heartbeat.DispatcherCount = uint32(len(heartbeat.DispatcherProgresses))

	// Test Marshal and Unmarshal
	data, err := heartbeat.Marshal()
	require.NoError(t, err)

	var unmarshalledResponse DispatcherHeartbeat
	err = unmarshalledResponse.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, heartbeat.DispatcherCount, unmarshalledResponse.DispatcherCount)
	require.Equal(t, len(heartbeat.DispatcherProgresses), len(unmarshalledResponse.DispatcherProgresses))

	for i, progress := range heartbeat.DispatcherProgresses {
		require.Equal(t, progress.Version, unmarshalledResponse.DispatcherProgresses[i].Version)
		require.Equal(t, progress.CheckpointTs, unmarshalledResponse.DispatcherProgresses[i].CheckpointTs)
		require.Equal(t, progress.DispatcherID, unmarshalledResponse.DispatcherProgresses[i].DispatcherID)
	}
}

func TestDispatcherState(t *testing.T) {
	t.Parallel()
	// Test constructor function
	dispatcherID := common.NewDispatcherID()
	state := DSStateNormal
	ds := NewDispatcherState(dispatcherID, state)

	require.Equal(t, byte(DispatcherHeartbeatResponseVersion), ds.Version)
	require.Equal(t, state, ds.State)
	require.Equal(t, dispatcherID, ds.DispatcherID)

	// Test GetSize
	expectedSize := dispatcherID.GetSize() + 2 // dispatcherID size + version + state
	require.Equal(t, expectedSize, ds.GetSize())

	// Test Marshal and Unmarshal
	data, err := ds.Marshal()
	require.NoError(t, err)
	require.Len(t, data, ds.GetSize())

	var unmarshaledState DispatcherState
	err = unmarshaledState.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, ds.Version, unmarshaledState.Version)
	require.Equal(t, ds.State, unmarshaledState.State)
	require.Equal(t, ds.DispatcherID, unmarshaledState.DispatcherID)
}

func TestDispatcherHeartbeatResponse(t *testing.T) {
	t.Parallel()
	// Test constructor function
	response := NewDispatcherHeartbeatResponse()

	require.Equal(t, byte(DispatcherHeartbeatVersion), response.Version)
	require.Equal(t, response.DispatcherCount, uint32(0))
	require.Empty(t, response.DispatcherStates)

	// Test Append
	dispatcherID1 := common.NewDispatcherID()
	state1 := NewDispatcherState(dispatcherID1, DSStateNormal)
	response.Append(state1)
	require.Len(t, response.DispatcherStates, 1)
	require.Equal(t, response.DispatcherCount, uint32(len(response.DispatcherStates)))
	require.Equal(t, state1, response.DispatcherStates[0])

	dispatcherID2 := common.NewDispatcherID()
	state2 := NewDispatcherState(dispatcherID2, DSStateRemoved)
	response.Append(state2)
	require.Equal(t, response.DispatcherCount, uint32(len(response.DispatcherStates)))
	require.Len(t, response.DispatcherStates, 2)
	require.Equal(t, state2, response.DispatcherStates[1])

	// Test GetSize
	expectedSize := 1 + 4 + 8 + state1.GetSize() + state2.GetSize() // version(byte) + clusterID(uint64) + dispatcher count(uint32) + state1 size + state2 size
	require.Equal(t, expectedSize, response.GetSize())

	// Test Marshal and Unmarshal
	response.DispatcherCount = uint32(len(response.DispatcherStates))
	data, err := response.Marshal()
	require.NoError(t, err)
	require.Len(t, data, response.GetSize())

	var unmarshalledResponse DispatcherHeartbeatResponse
	err = unmarshalledResponse.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, response.Version, unmarshalledResponse.Version)
	require.Equal(t, response.DispatcherCount, unmarshalledResponse.DispatcherCount)
	require.Equal(t, len(response.DispatcherStates), len(unmarshalledResponse.DispatcherStates))

	for i, state := range response.DispatcherStates {
		require.Equal(t, state.Version, unmarshalledResponse.DispatcherStates[i].Version)
		require.Equal(t, state.State, unmarshalledResponse.DispatcherStates[i].State)
		require.Equal(t, state.DispatcherID, unmarshalledResponse.DispatcherStates[i].DispatcherID)
	}
}

func TestDispatcherHeartbeatResponseWithMultipleStates(t *testing.T) {
	t.Parallel()
	// Create response with multiple dispatcher states
	response := NewDispatcherHeartbeatResponse()

	// Add state for each dispatcher - alternating between Normal and Removed
	dispatcherCount := 5
	for i := 0; i < dispatcherCount; i++ {
		var state DSState
		if i%2 == 0 {
			state = DSStateNormal
		} else {
			state = DSStateRemoved
		}

		response.Append(NewDispatcherState(common.NewDispatcherID(), state))
	}

	require.Equal(t, response.DispatcherCount, uint32(dispatcherCount))
	require.Len(t, response.DispatcherStates, dispatcherCount)

	// Test Marshal and Unmarshal
	data, err := response.Marshal()
	require.NoError(t, err)

	var unmarshalledResponse DispatcherHeartbeatResponse
	err = unmarshalledResponse.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, response.DispatcherCount, unmarshalledResponse.DispatcherCount)
	require.Equal(t, len(response.DispatcherStates), len(unmarshalledResponse.DispatcherStates))

	for i, state := range response.DispatcherStates {
		require.Equal(t, state.Version, unmarshalledResponse.DispatcherStates[i].Version)
		require.Equal(t, state.State, unmarshalledResponse.DispatcherStates[i].State)
		require.Equal(t, state.DispatcherID, unmarshalledResponse.DispatcherStates[i].DispatcherID)
	}
}

func TestCongestionControl(t *testing.T) {
	t.Parallel()

	control := NewCongestionControl()
	bytes, err := control.Marshal()
	require.NoError(t, err)
	require.Equal(t, len(bytes), control.GetSize())

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
