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
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestDispatcherProgress(t *testing.T) {
	t.Parallel()
	// Test GetSize function
	dispatcherID := common.NewDispatcherID()
	progress := DispatcherProgress{
		DispatcherID: dispatcherID,
		CheckpointTs: 123456789,
	}
	expectedSize := dispatcherID.GetSize() + 8 // dispatcherID size + checkpointTs size
	require.Equal(t, expectedSize, progress.GetSize())

	// Test Marshal and Unmarshal
	data, err := progress.Marshal()
	require.NoError(t, err)
	require.Len(t, data, progress.GetSize())

	var unmarshalledProgress DispatcherProgress
	err = unmarshalledProgress.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, progress.CheckpointTs, unmarshalledProgress.CheckpointTs)
	require.Equal(t, progress.DispatcherID, unmarshalledProgress.DispatcherID)
}

func TestDispatcherHeartbeat(t *testing.T) {
	t.Parallel()
	// Test NewDispatcherHeartbeat
	dispatcherCount := 3
	heartbeat := NewDispatcherHeartbeat(dispatcherCount)
	require.Equal(t, DispatcherHeartbeatVersion1, heartbeat.Version)
	require.Empty(t, heartbeat.DispatcherProgresses)
	require.Equal(t, dispatcherCount, cap(heartbeat.DispatcherProgresses))

	// Test Append
	dispatcherID1 := common.NewDispatcherID()
	progress1 := DispatcherProgress{
		DispatcherID: dispatcherID1,
		CheckpointTs: 100,
	}
	heartbeat.Append(progress1)
	require.Len(t, heartbeat.DispatcherProgresses, 1)
	require.Equal(t, progress1, heartbeat.DispatcherProgresses[0])

	dispatcherID2 := common.NewDispatcherID()
	progress2 := DispatcherProgress{
		DispatcherID: dispatcherID2,
		CheckpointTs: 200,
	}
	heartbeat.Append(progress2)
	require.Len(t, heartbeat.DispatcherProgresses, 2)
	require.Equal(t, progress2, heartbeat.DispatcherProgresses[1])

	// Test GetSize
	expectedSize := 4 + 8 + progress1.GetSize() + progress2.GetSize() // dispatcher count(uint32) + clusterID(uint64) + progress sizes
	require.Equal(t, expectedSize, heartbeat.GetSize())

	// Test Marshal and Unmarshal
	heartbeat.DispatcherCount = uint32(len(heartbeat.DispatcherProgresses))
	data, err := heartbeat.Marshal()
	require.NoError(t, err)
	require.Len(t, data, heartbeat.GetSize()+GetEventHeaderSize())

	// Verify header format: [MAGIC(4B)][EVENT_TYPE(2B)][VERSION(2B)][PAYLOAD_LENGTH(8B)]
	require.Greater(t, len(data), 16, "data should include header")
	// Magic (4 bytes)
	require.Equal(t, uint32(0xDA7A6A6A), binary.BigEndian.Uint32(data[0:4]), "magic bytes")
	// Event type (2 bytes)
	require.Equal(t, uint16(TypeDispatcherHeartbeat), binary.BigEndian.Uint16(data[4:6]), "event type")
	// Version (2 bytes)
	require.Equal(t, uint16(DispatcherHeartbeatVersion1), binary.BigEndian.Uint16(data[6:8]), "version")

	var unmarshalledResponse DispatcherHeartbeat
	err = unmarshalledResponse.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, heartbeat.Version, unmarshalledResponse.Version)
	require.Equal(t, heartbeat.DispatcherCount, unmarshalledResponse.DispatcherCount)
	require.Equal(t, len(heartbeat.DispatcherProgresses), len(unmarshalledResponse.DispatcherProgresses))

	for i, progress := range heartbeat.DispatcherProgresses {
		require.Equal(t, progress.CheckpointTs, unmarshalledResponse.DispatcherProgresses[i].CheckpointTs)
		require.Equal(t, progress.DispatcherID, unmarshalledResponse.DispatcherProgresses[i].DispatcherID)
	}
}

func TestDispatcherHeartbeatWithMultipleDispatchers(t *testing.T) {
	t.Parallel()
	// Create multiple dispatchers
	dispatcherCount := 5
	heartbeat := NewDispatcherHeartbeat(dispatcherCount)

	// Add progress for each dispatcher
	for i := 0; i < dispatcherCount; i++ {
		progress := DispatcherProgress{
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

	require.Equal(t, state, ds.State)
	require.Equal(t, dispatcherID, ds.DispatcherID)

	// Test GetSize
	expectedSize := dispatcherID.GetSize() + 1 // dispatcherID size + state
	require.Equal(t, expectedSize, ds.GetSize())

	// Test Marshal and Unmarshal
	data, err := ds.Marshal()
	require.NoError(t, err)
	require.Len(t, data, ds.GetSize())

	var unmarshaledState DispatcherState
	err = unmarshaledState.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, ds.State, unmarshaledState.State)
	require.Equal(t, ds.DispatcherID, unmarshaledState.DispatcherID)
}

func TestDispatcherHeartbeatResponse(t *testing.T) {
	t.Parallel()
	// Test constructor function
	response := NewDispatcherHeartbeatResponse()

	require.Equal(t, DispatcherHeartbeatVersion1, response.Version)
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
	expectedSize := 4 + 8 + state1.GetSize() + state2.GetSize() // dispatcher count(uint32) + clusterID(uint64) + state sizes
	require.Equal(t, expectedSize, response.GetSize())

	// Test Marshal and Unmarshal
	response.DispatcherCount = uint32(len(response.DispatcherStates))
	data, err := response.Marshal()
	require.NoError(t, err)
	require.Len(t, data, response.GetSize()+GetEventHeaderSize())

	// Verify header format: [MAGIC(4B)][EVENT_TYPE(2B)][VERSION(2B)][PAYLOAD_LENGTH(8B)]
	require.Greater(t, len(data), 16, "data should include header")
	// Magic (4 bytes)
	require.Equal(t, uint32(0xDA7A6A6A), binary.BigEndian.Uint32(data[0:4]), "magic bytes")
	// Event type (2 bytes)
	require.Equal(t, uint16(TypeDispatcherHeartbeatResponse), binary.BigEndian.Uint16(data[4:6]), "event type")
	// Version (2 bytes)
	require.Equal(t, uint16(DispatcherHeartbeatResponseVersion1), binary.BigEndian.Uint16(data[6:8]), "version")

	var unmarshalledResponse DispatcherHeartbeatResponse
	err = unmarshalledResponse.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, response.Version, unmarshalledResponse.Version)
	require.Equal(t, response.DispatcherCount, unmarshalledResponse.DispatcherCount)
	require.Equal(t, len(response.DispatcherStates), len(unmarshalledResponse.DispatcherStates))

	for i, state := range response.DispatcherStates {
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
		require.Equal(t, state.State, unmarshalledResponse.DispatcherStates[i].State)
		require.Equal(t, state.DispatcherID, unmarshalledResponse.DispatcherStates[i].DispatcherID)
	}
}

func TestDispatcherHeartbeatHeaderValidation(t *testing.T) {
	t.Parallel()

	heartbeat := NewDispatcherHeartbeat(1)
	heartbeat.Append(NewDispatcherProgress(common.NewDispatcherID(), 100))

	data, err := heartbeat.Marshal()
	require.NoError(t, err)

	// Make a copy for manipulation
	data2 := make([]byte, len(data))
	copy(data2, data)

	// Test 1: Invalid magic bytes
	data2[0] = 0xFF
	var decoded DispatcherHeartbeat
	err = decoded.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid magic bytes")

	// Restore for next test
	copy(data2, data)

	// Test 2: Wrong event type (bytes 4-5 in new format)
	binary.BigEndian.PutUint16(data2[4:6], uint16(TypeCongestionControl))
	err = decoded.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "DispatcherHeartbeat")

	// Restore for next test
	copy(data2, data)

	// Test 3: Unsupported version (bytes 6-7 in new format)
	binary.BigEndian.PutUint16(data2[6:8], 99)
	err = decoded.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported DispatcherHeartbeat version")

	// Test 4: Data too short
	shortData := []byte{0xDA, 0x7A, 0x00}
	err = decoded.Unmarshal(shortData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data too short")

	// Test 5: Incomplete payload
	incompleteData := make([]byte, 8)
	incompleteData[0] = 0xDA
	incompleteData[1] = 0x7A
	incompleteData[2] = TypeDispatcherHeartbeat
	incompleteData[3] = DispatcherHeartbeatVersion1
	incompleteData[4] = 0
	incompleteData[5] = 0
	incompleteData[6] = 0
	incompleteData[7] = 100 // Claim 100 bytes but don't provide them
	err = decoded.Unmarshal(incompleteData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data too short")
}

func TestDispatcherHeartbeatResponseHeaderValidation(t *testing.T) {
	t.Parallel()

	response := NewDispatcherHeartbeatResponse()
	response.Append(NewDispatcherState(common.NewDispatcherID(), DSStateNormal))

	data, err := response.Marshal()
	require.NoError(t, err)

	// Make a copy for manipulation
	data2 := make([]byte, len(data))
	copy(data2, data)

	// Test 1: Invalid magic bytes
	data2[0] = 0xFF
	var decoded DispatcherHeartbeatResponse
	err = decoded.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid magic bytes")

	// Restore for next test
	copy(data2, data)

	// Test 2: Wrong event type (bytes 4-5 in new format)
	binary.BigEndian.PutUint16(data2[4:6], uint16(TypeDispatcherHeartbeat))
	err = decoded.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "DispatcherHeartbeatResponse")

	// Restore for next test
	copy(data2, data)

	// Test 3: Unsupported version (bytes 6-7 in new format)
	binary.BigEndian.PutUint16(data2[6:8], 99)
	err = decoded.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported DispatcherHeartbeatResponse version")

	// Test 4: Data too short
	shortData := []byte{0xDA, 0x7A, 0x00}
	err = decoded.Unmarshal(shortData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data too short")

	// Test 5: Incomplete payload
	incompleteData := make([]byte, 8)
	incompleteData[0] = 0xDA
	incompleteData[1] = 0x7A
	incompleteData[2] = TypeDispatcherHeartbeatResponse
	incompleteData[3] = DispatcherHeartbeatResponseVersion1
	incompleteData[4] = 0
	incompleteData[5] = 0
	incompleteData[6] = 0
	incompleteData[7] = 100 // Claim 100 bytes but don't provide them
	err = decoded.Unmarshal(incompleteData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data too short")
}
