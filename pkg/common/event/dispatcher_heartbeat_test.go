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
