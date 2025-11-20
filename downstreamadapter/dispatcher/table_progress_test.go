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

package dispatcher

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/assert"
)

func TestTableProgress(t *testing.T) {
	tp := NewTableProgress()
	// Test Empty
	assert.True(t, tp.Empty())

	// Create a mock DML event
	mockDMLEvent := &commonEvent.DMLEvent{
		StartTs:  1,
		CommitTs: 2,
		Seq:      1,
		Epoch:    1,
	}

	// Add an event
	tp.Add(mockDMLEvent)
	assert.False(t, tp.Empty())

	// Verify GetCheckpointTs
	checkpointTs, isEmpty := tp.GetCheckpointTs()
	assert.Equal(t, uint64(1), checkpointTs)
	assert.False(t, isEmpty)

	// Verify maxCommitTs
	assert.Equal(t, uint64(2), tp.maxCommitTs)

	// verify after event is flushed
	mockDMLEvent.PostFlush()
	checkpointTs, isEmpty = tp.GetCheckpointTs()
	assert.Equal(t, uint64(1), checkpointTs)
	assert.True(t, isEmpty)

	// Create a mock DDL event
	mockDDLEvent := &commonEvent.DDLEvent{
		FinishedTs: 4,
		Seq:        2,
		Epoch:      1,
	}

	tp.Pass(mockDDLEvent)
	assert.Equal(t, uint64(4), tp.maxCommitTs, "Expected maxCommitTs to be 4 after Pass")
	checkpointTs, isEmpty = tp.GetCheckpointTs()
	assert.Equal(t, uint64(3), checkpointTs)
	assert.True(t, isEmpty)
}

// TestSyncPointEventCommitTs tests the behavior for SyncPointEvent commitTs
func TestSyncPointEventCommitTs(t *testing.T) {
	tp := NewTableProgress()
	assert.True(t, tp.Empty())

	dispatcherID := common.NewDispatcherID()

	// Create a SyncPointEvent
	syncPointEvent := &commonEvent.SyncPointEvent{
		DispatcherID: dispatcherID,
		CommitTs:     40,
	}

	finalCommitTs := syncPointEvent.CommitTs
	assert.Equal(t, uint64(40), finalCommitTs, "getFinalCommitTs should return the largest commitTs")

	// Test Add method with SyncPointEvent
	tp.Add(syncPointEvent)
	assert.False(t, tp.Empty())

	assert.Equal(t, uint64(40), tp.maxCommitTs, "maxCommitTs should be set to the largest commitTs")

	// Verify GetCheckpointTs behavior
	checkpointTs, isEmpty := tp.GetCheckpointTs()
	assert.Equal(t, uint64(39), checkpointTs, "checkpointTs should be largest commitTs - 1")
	assert.False(t, isEmpty)

	// Test Remove method
	tp.Remove(syncPointEvent)
	assert.True(t, tp.Empty(), "TableProgress should be empty after removing the event")

	// Verify checkpointTs after removal
	checkpointTs, isEmpty = tp.GetCheckpointTs()
	assert.Equal(t, uint64(39), checkpointTs, "checkpointTs should remain as maxCommitTs - 1 after removal")
	assert.True(t, isEmpty)

	// Create another SyncPointEvent
	syncPointEvent = &commonEvent.SyncPointEvent{
		DispatcherID: dispatcherID,
		CommitTs:     60,
	}

	tp.Add(syncPointEvent)
	assert.Equal(t, uint64(60), tp.maxCommitTs, "maxCommitTs should be set to the largest commitTs")

	checkpointTs, isEmpty = tp.GetCheckpointTs()
	assert.Equal(t, uint64(59), checkpointTs, "checkpointTs should be largest commitTs - 1")
	assert.False(t, isEmpty)

	tp.Remove(syncPointEvent)

	checkpointTs, isEmpty = tp.GetCheckpointTs()
	assert.Equal(t, uint64(59), checkpointTs, "checkpointTs should be largest commitTs - 1")
	assert.True(t, isEmpty)

	syncPointEvent = &commonEvent.SyncPointEvent{
		DispatcherID: dispatcherID,
		CommitTs:     80,
	}

	tp.Pass(syncPointEvent)
	assert.Equal(t, uint64(80), tp.maxCommitTs, "maxCommitTs should be set to the largest commitTs")

	checkpointTs, isEmpty = tp.GetCheckpointTs()
	assert.Equal(t, uint64(79), checkpointTs, "checkpointTs should be largest commitTs - 1")
	assert.True(t, isEmpty)
}
