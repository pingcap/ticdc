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

package eventservice

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestCollectSlowestDispatchersByCheckpointTs(t *testing.T) {
	t.Parallel()

	// Create a mock metrics collector
	broker := &eventBroker{
		dispatchers:             sync.Map{},
		tableTriggerDispatchers: sync.Map{},
		pdClock:                 pdutil.NewClock4Test(),
	}

	// Create 20 dispatchers with different checkpointTs values
	// checkpointTs values: 100, 110, 120, ..., 290 (20 dispatchers)
	dispatchers := make([]*dispatcherStat, 20)
	for i := 0; i < 20; i++ {
		checkpointTs := uint64(100 + i*10)
		info := newMockDispatcherInfo(t, checkpointTs, common.NewDispatcherID(), int64(i+1), eventpb.ActionType_ACTION_TYPE_REGISTER)
		status := newChangefeedStatus(info.GetChangefeedID())
		stat := newDispatcherStat(info, 1, 1, nil, status)
		stat.checkpointTs.Store(checkpointTs)
		stat.receivedResolvedTs.Store(checkpointTs + 10)
		stat.sentResolvedTs.Store(checkpointTs + 5)
		dispatchers[i] = stat

		// Add to dispatchers map
		dispPtr := atomic.NewPointer(stat)
		broker.dispatchers.Store(stat.id, dispPtr)
	}

	// Create metrics collector
	mc := &metricsCollector{
		broker: broker,
	}

	// Collect metrics
	snapshot := &metricsSnapshot{
		receivedMinResolvedTs: uint64(math.MaxUint64),
		sentMinResolvedTs:     uint64(math.MaxUint64),
		pdTime:                time.Now(),
	}

	mc.collectDispatcherMetrics(snapshot)

	// Verify that we collected the 10 slowest dispatchers (checkpointTs: 100-190)
	require.Equal(t, 10, len(snapshot.slowestDispatchers), "should collect exactly 10 slowest dispatchers")
	require.Equal(t, 20, snapshot.dispatcherCount, "should count all 20 dispatchers")

	// Verify that the dispatchers are sorted by checkpointTs (ascending, slowest first)
	for i := 0; i < len(snapshot.slowestDispatchers); i++ {
		expectedCheckpointTs := uint64(100 + i*10)
		actualCheckpointTs := snapshot.slowestDispatchers[i].checkpointTs.Load()
		require.Equal(t, expectedCheckpointTs, actualCheckpointTs,
			"dispatcher at index %d should have checkpointTs %d, got %d", i, expectedCheckpointTs, actualCheckpointTs)
	}

	// Verify that the slowest dispatcher has checkpointTs = 100
	require.Equal(t, uint64(100), snapshot.slowestDispatchers[0].checkpointTs.Load(),
		"slowest dispatcher should have checkpointTs = 100")

	// Verify that the fastest among the slowest has checkpointTs = 190
	require.Equal(t, uint64(190), snapshot.slowestDispatchers[9].checkpointTs.Load(),
		"fastest among slowest should have checkpointTs = 190")
}

func TestCollectSlowestDispatchersLessThan10(t *testing.T) {
	t.Parallel()

	// Create a mock metrics collector
	broker := &eventBroker{
		dispatchers:             sync.Map{},
		tableTriggerDispatchers: sync.Map{},
		pdClock:                 pdutil.NewClock4Test(),
	}

	// Create only 5 dispatchers
	dispatchers := make([]*dispatcherStat, 5)
	for i := 0; i < 5; i++ {
		checkpointTs := uint64(100 + i*10)
		info := newMockDispatcherInfo(t, checkpointTs, common.NewDispatcherID(), int64(i+1), eventpb.ActionType_ACTION_TYPE_REGISTER)
		status := newChangefeedStatus(info.GetChangefeedID())
		stat := newDispatcherStat(info, 1, 1, nil, status)
		stat.checkpointTs.Store(checkpointTs)
		stat.receivedResolvedTs.Store(checkpointTs + 10)
		stat.sentResolvedTs.Store(checkpointTs + 5)
		dispatchers[i] = stat

		dispPtr := atomic.NewPointer(stat)
		broker.dispatchers.Store(stat.id, dispPtr)
	}

	// Create metrics collector
	mc := &metricsCollector{
		broker: broker,
	}

	// Collect metrics
	snapshot := &metricsSnapshot{
		receivedMinResolvedTs: uint64(math.MaxUint64),
		sentMinResolvedTs:     uint64(math.MaxUint64),
		pdTime:                time.Now(),
	}

	mc.collectDispatcherMetrics(snapshot)

	// Verify that we collected all 5 dispatchers
	require.Equal(t, 5, len(snapshot.slowestDispatchers), "should collect all 5 dispatchers")
	require.Equal(t, 5, snapshot.dispatcherCount, "should count all 5 dispatchers")

	// Verify that the dispatchers are sorted by checkpointTs (ascending)
	for i := 0; i < len(snapshot.slowestDispatchers); i++ {
		expectedCheckpointTs := uint64(100 + i*10)
		actualCheckpointTs := snapshot.slowestDispatchers[i].checkpointTs.Load()
		require.Equal(t, expectedCheckpointTs, actualCheckpointTs,
			"dispatcher at index %d should have checkpointTs %d, got %d", i, expectedCheckpointTs, actualCheckpointTs)
	}
}

func TestDispatcherHeapItem(t *testing.T) {
	t.Parallel()

	// Create two dispatchers with different checkpointTs
	info1 := newMockDispatcherInfo(t, 100, common.NewDispatcherID(), 1, eventpb.ActionType_ACTION_TYPE_REGISTER)
	status1 := newChangefeedStatus(info1.GetChangefeedID())
	stat1 := newDispatcherStat(info1, 1, 1, nil, status1)
	stat1.checkpointTs.Store(100)

	info2 := newMockDispatcherInfo(t, 200, common.NewDispatcherID(), 2, eventpb.ActionType_ACTION_TYPE_REGISTER)
	status2 := newChangefeedStatus(info2.GetChangefeedID())
	stat2 := newDispatcherStat(info2, 1, 1, nil, status2)
	stat2.checkpointTs.Store(200)

	// Create heap items
	item1 := &dispatcherHeapItem{dispatcher: stat1, heapIndex: 0}
	item2 := &dispatcherHeapItem{dispatcher: stat2, heapIndex: 0}

	// Test LessThan: item1 has smaller checkpointTs (100), so it should be "less" in our heap
	// But our LessThan returns true when checkpointTs is larger, so item2 should be "less"
	require.True(t, item2.LessThan(item1), "item2 (checkpointTs=200) should be less than item1 (checkpointTs=100) in heap")
	require.False(t, item1.LessThan(item2), "item1 (checkpointTs=100) should not be less than item2 (checkpointTs=200) in heap")

	// Test heap index
	item1.SetHeapIndex(5)
	require.Equal(t, 5, item1.GetHeapIndex())
	item2.SetHeapIndex(10)
	require.Equal(t, 10, item2.GetHeapIndex())
}
