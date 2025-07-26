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
	progress := NewDispatcherProgress(dispatcherID, 123456789)
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
	progress.Version = 0
	_, err = progress.Marshal()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid version")
}

func TestDispatcherHeartbeat(t *testing.T) {
	t.Parallel()
	// Test NewDispatcherHeartbeat
	heartbeat := NewDispatcherHeartbeat()
	require.Equal(t, byte(DispatcherHeartbeatVersion), heartbeat.Version)
	require.Empty(t, heartbeat.DispatcherProgresses)
	require.Empty(t, heartbeat.AvailableMemory)

	// Test Append
	progress1 := NewDispatcherProgress(common.NewDispatcherID(), 100)
	heartbeat.Append(progress1)
	require.Equal(t, heartbeat.DispatcherCount, uint32(1))
	require.Len(t, heartbeat.DispatcherProgresses, 1)
	require.Equal(t, progress1, heartbeat.DispatcherProgresses[0])

	progress2 := NewDispatcherProgress(common.NewDispatcherID(), 200)
	heartbeat.Append(progress2)
	require.Equal(t, heartbeat.DispatcherCount, uint32(2))
	require.Len(t, heartbeat.DispatcherProgresses, 2)
	require.Equal(t, progress2, heartbeat.DispatcherProgresses[1])

	available := make([]AvailableMemory, 0, 2)
	available = append(available, AvailableMemory{Gid: common.NewGID(), Available: 1024})
	available = append(available, AvailableMemory{Gid: common.NewGID(), Available: 2048})

	heartbeat.AppendAvailableMemory(available)
	require.Equal(t, heartbeat.changefeedCount, uint32(2))
	require.Len(t, heartbeat.AvailableMemory, 2)

	// Test GetSize
	// version(byte) + clusterID(uint64) + dispatcher count(uint32) + progress1 size + progress2 size + changefeed count(uint32) + Add size for Available memory entries
	expectedSize := 1 + 4 + 8 + progress1.GetSize() + progress2.GetSize() + 4 + 48
	require.Equal(t, expectedSize, heartbeat.GetSize())

	// Test Marshal and Unmarshal
	data, err := heartbeat.Marshal()
	require.NoError(t, err)
	require.Len(t, data, heartbeat.GetSize())

	var unmarshalledResponse DispatcherHeartbeat
	err = unmarshalledResponse.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, heartbeat.Version, unmarshalledResponse.Version)
	require.Equal(t, heartbeat.DispatcherCount, unmarshalledResponse.DispatcherCount)
	require.Equal(t, len(heartbeat.DispatcherProgresses), len(unmarshalledResponse.DispatcherProgresses))
	require.Equal(t, heartbeat.changefeedCount, unmarshalledResponse.changefeedCount)
	require.Equal(t, len(heartbeat.AvailableMemory), len(unmarshalledResponse.AvailableMemory))

	for i, progress := range heartbeat.DispatcherProgresses {
		require.Equal(t, progress.Version, unmarshalledResponse.DispatcherProgresses[i].Version)
		require.Equal(t, progress.CheckpointTs, unmarshalledResponse.DispatcherProgresses[i].CheckpointTs)
		require.Equal(t, progress.DispatcherID, unmarshalledResponse.DispatcherProgresses[i].DispatcherID)
	}

	for i, entry := range heartbeat.AvailableMemory {
		require.Equal(t, entry.Gid, unmarshalledResponse.AvailableMemory[i].Gid)
		require.Equal(t, entry.Available, unmarshalledResponse.AvailableMemory[i].Available)
	}

	// Test with invalid progress version
	heartbeat.DispatcherProgresses[0].Version = 0 // Invalid version
	_, err = heartbeat.Marshal()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid version")
}

func TestDispatcherHeartbeatWithMultipleDispatchers(t *testing.T) {
	t.Parallel()
	// Create multiple dispatchers
	heartbeat := NewDispatcherHeartbeat()

	// Add progress for each dispatcher
	dispatcherCount := 5
	for i := 0; i < dispatcherCount; i++ {
		heartbeat.Append(NewDispatcherProgress(common.NewDispatcherID(), uint64(i*100)))
	}

	require.Len(t, heartbeat.DispatcherProgresses, dispatcherCount)
	require.Equal(t, heartbeat.DispatcherCount, uint32(dispatcherCount))

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

	var unmarshalledState DispatcherState
	err = unmarshalledState.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, ds.Version, unmarshalledState.Version)
	require.Equal(t, ds.State, unmarshalledState.State)
	require.Equal(t, ds.DispatcherID, unmarshalledState.DispatcherID)
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
