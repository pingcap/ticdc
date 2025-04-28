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

	var unmarshaledProgress DispatcherProgress
	err = unmarshaledProgress.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, progress.Version, unmarshaledProgress.Version)
	require.Equal(t, progress.CheckpointTs, unmarshaledProgress.CheckpointTs)
	require.Equal(t, progress.DispatcherID, unmarshaledProgress.DispatcherID)

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
	expectedSize := 1 + 4 + progress1.GetSize() + progress2.GetSize() // version + dispatcher count + progress1 size + progress2 size
	require.Equal(t, expectedSize, heartbeat.GetSize())

	// Test Marshal and Unmarshal
	heartbeat.DispatcherCount = uint32(len(heartbeat.DispatcherProgresses))
	data, err := heartbeat.Marshal()
	require.NoError(t, err)
	require.Len(t, data, heartbeat.GetSize())

	var unmarshaledHeartbeat DispatcherHeartbeat
	err = unmarshaledHeartbeat.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, heartbeat.Version, unmarshaledHeartbeat.Version)
	require.Equal(t, heartbeat.DispatcherCount, unmarshaledHeartbeat.DispatcherCount)
	require.Equal(t, len(heartbeat.DispatcherProgresses), len(unmarshaledHeartbeat.DispatcherProgresses))

	for i, progress := range heartbeat.DispatcherProgresses {
		require.Equal(t, progress.Version, unmarshaledHeartbeat.DispatcherProgresses[i].Version)
		require.Equal(t, progress.CheckpointTs, unmarshaledHeartbeat.DispatcherProgresses[i].CheckpointTs)
		require.Equal(t, progress.DispatcherID, unmarshaledHeartbeat.DispatcherProgresses[i].DispatcherID)
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

	var unmarshaledHeartbeat DispatcherHeartbeat
	err = unmarshaledHeartbeat.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, heartbeat.DispatcherCount, unmarshaledHeartbeat.DispatcherCount)
	require.Equal(t, len(heartbeat.DispatcherProgresses), len(unmarshaledHeartbeat.DispatcherProgresses))

	for i, progress := range heartbeat.DispatcherProgresses {
		require.Equal(t, progress.Version, unmarshaledHeartbeat.DispatcherProgresses[i].Version)
		require.Equal(t, progress.CheckpointTs, unmarshaledHeartbeat.DispatcherProgresses[i].CheckpointTs)
		require.Equal(t, progress.DispatcherID, unmarshaledHeartbeat.DispatcherProgresses[i].DispatcherID)
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
	dispatcherCount := 3
	response := NewDispatcherHeartbeatResponse(dispatcherCount)

	require.Equal(t, byte(DispatcherHeartbeatVersion), response.Version)
	require.Equal(t, uint32(dispatcherCount), response.DispatcherCount)
	require.Empty(t, response.DispatcherStates)
	require.Equal(t, dispatcherCount, cap(response.DispatcherStates))

	// Test Append
	dispatcherID1 := common.NewDispatcherID()
	state1 := NewDispatcherState(dispatcherID1, DSStateNormal)
	response.Append(state1)
	require.Len(t, response.DispatcherStates, 1)
	require.Equal(t, state1, response.DispatcherStates[0])

	dispatcherID2 := common.NewDispatcherID()
	state2 := NewDispatcherState(dispatcherID2, DSStateRemoved)
	response.Append(state2)
	require.Len(t, response.DispatcherStates, 2)
	require.Equal(t, state2, response.DispatcherStates[1])

	// Test GetSize
	expectedSize := 1 + 4 + state1.GetSize() + state2.GetSize() // version + dispatcher count + state1 size + state2 size
	require.Equal(t, expectedSize, response.GetSize())

	// Test Marshal and Unmarshal
	response.DispatcherCount = uint32(len(response.DispatcherStates))
	data, err := response.Marshal()
	require.NoError(t, err)
	require.Len(t, data, response.GetSize())

	var unmarshaledResponse DispatcherHeartbeatResponse
	err = unmarshaledResponse.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, response.Version, unmarshaledResponse.Version)
	require.Equal(t, response.DispatcherCount, unmarshaledResponse.DispatcherCount)
	require.Equal(t, len(response.DispatcherStates), len(unmarshaledResponse.DispatcherStates))

	for i, state := range response.DispatcherStates {
		require.Equal(t, state.Version, unmarshaledResponse.DispatcherStates[i].Version)
		require.Equal(t, state.State, unmarshaledResponse.DispatcherStates[i].State)
		require.Equal(t, state.DispatcherID, unmarshaledResponse.DispatcherStates[i].DispatcherID)
	}
}

func TestDispatcherHeartbeatResponseWithMultipleStates(t *testing.T) {
	t.Parallel()
	// Create response with multiple dispatcher states
	dispatcherCount := 5
	response := NewDispatcherHeartbeatResponse(dispatcherCount)

	// Add state for each dispatcher - alternating between Normal and Removed
	for i := 0; i < dispatcherCount; i++ {
		var state DSState
		if i%2 == 0 {
			state = DSStateNormal
		} else {
			state = DSStateRemoved
		}

		response.Append(NewDispatcherState(common.NewDispatcherID(), state))
	}

	require.Len(t, response.DispatcherStates, dispatcherCount)
	response.DispatcherCount = uint32(len(response.DispatcherStates))

	// Test Marshal and Unmarshal
	data, err := response.Marshal()
	require.NoError(t, err)

	var unmarshaledResponse DispatcherHeartbeatResponse
	err = unmarshaledResponse.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, response.DispatcherCount, unmarshaledResponse.DispatcherCount)
	require.Equal(t, len(response.DispatcherStates), len(unmarshaledResponse.DispatcherStates))

	for i, state := range response.DispatcherStates {
		require.Equal(t, state.Version, unmarshaledResponse.DispatcherStates[i].Version)
		require.Equal(t, state.State, unmarshaledResponse.DispatcherStates[i].State)
		require.Equal(t, state.DispatcherID, unmarshaledResponse.DispatcherStates[i].DispatcherID)
	}
}
