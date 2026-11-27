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
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/scheduler/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	syncPointEvent.PostFlush()
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

	syncPointEvent.PostFlush()

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

// TestMultiSameCommitTsStartTsPair tests the behavior when multiple events have the same (startTs, commitTs) pair.
func TestMultiSameCommitTsStartTsPair(t *testing.T) {
	tp := NewTableProgress()

	// Create a mock DML event
	mockDMLEvent1 := &commonEvent.DMLEvent{
		StartTs:  1,
		CommitTs: 2,
		Seq:      1,
		Epoch:    1,
	}

	mockDMLEvent2 := &commonEvent.DMLEvent{
		StartTs:  1,
		CommitTs: 2,
		Seq:      2,
		Epoch:    1,
	}

	mockDMLEvent3 := &commonEvent.DMLEvent{
		StartTs:  1,
		CommitTs: 4,
		Seq:      3,
		Epoch:    1,
	}

	// Add an event
	tp.Add(mockDMLEvent1)
	tp.Add(mockDMLEvent2)
	assert.False(t, tp.Empty())

	// Verify GetCheckpointTs
	checkpointTs, isEmpty := tp.GetCheckpointTs()
	assert.Equal(t, uint64(1), checkpointTs)
	assert.False(t, isEmpty)
	// Verify maxCommitTs
	assert.Equal(t, uint64(2), tp.maxCommitTs)
	assert.Equal(t, 2, tp.Len())

	// verify after event is flushed
	mockDMLEvent1.PostFlush()
	checkpointTs, isEmpty = tp.GetCheckpointTs()
	assert.Equal(t, uint64(1), checkpointTs)
	assert.False(t, isEmpty)
	assert.Equal(t, 1, tp.Len())

	tp.Add(mockDMLEvent3)
	assert.Equal(t, 2, tp.Len())

	mockDMLEvent2.PostFlush()
	checkpointTs, isEmpty = tp.GetCheckpointTs()
	assert.Equal(t, uint64(3), checkpointTs)
	assert.False(t, isEmpty)
	assert.Equal(t, 1, tp.Len())

	mockDMLEvent3.PostFlush()
	checkpointTs, isEmpty = tp.GetCheckpointTs()
	assert.Equal(t, uint64(3), checkpointTs)
	assert.True(t, isEmpty)
	assert.Equal(t, 0, tp.Len())
}

func TestResourceCountersPreserveDispatcherSamples(t *testing.T) {
	stats := resource.NewNodeStats()
	counter := stats.NewCounter(resource.Sink)
	now := time.Now()
	a, b := NewTableProgress(), NewTableProgress()
	a.sinkLoad, b.sinkLoad = counter.Recorder(), counter.Recorder()
	first := &commonEvent.DMLEvent{StartTs: 1, CommitTs: 2, ApproximateSize: 100}
	second := &commonEvent.DMLEvent{StartTs: 1, CommitTs: 3, ApproximateSize: 200}
	a.Add(first)
	b.Add(second)
	require.Zero(t, stats.Snapshot(now).FlushedEventBytes)
	first.PostFlush()
	second.PostFlush()
	require.Equal(t, uint64(300), stats.Snapshot(now.Add(resource.SampleInterval)).FlushedEventBytes)
	// Node-level reads never consume the per-dispatcher traffic samples.
	a.lastQueryTime = time.Now().Add(-time.Second)
	b.lastQueryTime = a.lastQueryTime
	require.InDelta(t, float64(100), a.GetEventSizePerSecond(), 1)
	require.InDelta(t, float64(200), b.GetEventSizePerSecond(), 1)
	require.Equal(t, uint64(300), stats.Snapshot(now.Add(resource.SampleInterval)).FlushedEventBytes)
}

func TestDispatcherModesShareComponentResourceCounter(t *testing.T) {
	stats := resource.NewNodeStats()
	shared := newTestSharedInfo(false, false, nil)
	defer shared.Close()
	shared.SinkLoad = stats.NewCounter(resource.Sink)
	testSink := newDispatcherTestSink(t, common.KafkaSinkType)
	for _, mode := range []int64{common.DefaultMode, common.RedoMode} {
		d := NewBasicDispatcher(common.NewDispatcherID(), &heartbeatpb.TableSpan{TableID: 1},
			0, 0, NewSchemaIDToDispatchers(), false, false, 0, 0, 0, mode, testSink.Sink(), shared)
		event := &commonEvent.DMLEvent{StartTs: 1, CommitTs: 2, ApproximateSize: 100}
		d.tableProgress.Add(event)
		event.PostFlush()
	}
	shared.SinkLoad.Close()
	require.Equal(t, uint64(200), stats.Snapshot(time.Now()).FlushedEventBytes)
}
