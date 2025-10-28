// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package dispatchermanager

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

var mockSink = sink.NewMockSink(common.BlackHoleSinkType)

// createTestDispatcher creates a test dispatcher with given parameters
func createTestDispatcher(t *testing.T, manager *DispatcherManager, id common.DispatcherID, tableID int64, startKey, endKey []byte) *dispatcher.EventDispatcher {
	span := &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: startKey,
		EndKey:   endKey,
	}
	var redoTs atomic.Uint64
	redoTs.Store(math.MaxUint64)
	sharedInfo := dispatcher.NewSharedInfo(
		manager.changefeedID,
		"system",
		false,
		false,
		nil,
		nil,
		nil,
		false,
		make(chan dispatcher.TableSpanStatusWithSeq, 1),
		make(chan *heartbeatpb.TableSpanBlockStatus, 1),
		make(chan error, 1),
	)
	d := dispatcher.NewEventDispatcher(
		id,
		span,
		0,
		0,
		dispatcher.NewSchemaIDToDispatchers(),
		false, // skipSyncpointAtStartTs
		false, // skipDMLAsStartTs
		0,     // currentPDTs
		mockSink,
		sharedInfo,
		false,
		&redoTs,
	)
	d.SetComponentStatus(heartbeatpb.ComponentState_Working)
	return d
}

// createTestManager creates a test DispatcherManager
func createTestManager(t *testing.T) *DispatcherManager {
	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	manager := &DispatcherManager{
		changefeedID:            changefeedID,
		dispatcherMap:           newDispatcherMap[*dispatcher.EventDispatcher](),
		heartbeatRequestQueue:   NewHeartbeatRequestQueue(),
		blockStatusRequestQueue: NewBlockStatusRequestQueue(),
		sink:                    mockSink,
		schemaIDToDispatchers:   dispatcher.NewSchemaIDToDispatchers(),
		sinkQuota:               config.GetDefaultReplicaConfig().MemoryQuota,
		latestWatermark:         NewWatermark(0),
		closing:                 atomic.Bool{},
		pdClock:                 pdutil.NewClock4Test(),
		config: &config.ChangefeedConfig{
			BDRMode: true,
		},
		metricEventDispatcherCount: metrics.EventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "eventDispatcher"),
		metricCheckpointTs:         metrics.DispatcherManagerCheckpointTsGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricResolvedTs:           metrics.DispatcherManagerResolvedTsGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricCheckpointTsLag:      metrics.DispatcherManagerCheckpointTsLagGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricResolvedTsLag:        metrics.DispatcherManagerResolvedTsLagGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
	}

	// Create shared info for the test manager
	manager.sharedInfo = dispatcher.NewSharedInfo(
		manager.changefeedID,
		"system",
		manager.config.BDRMode,
		false, // outputRawChangeEvent
		nil,   // integrityConfig
		nil,   // filterConfig
		nil,   // syncPointConfig
		false,
		make(chan dispatcher.TableSpanStatusWithSeq, 8192),
		make(chan *heartbeatpb.TableSpanBlockStatus, 1024*1024),
		make(chan error, 1),
	)
	nodeID := node.NewID()
	messageCenter, _, _ := messaging.NewMessageCenterForTest(t)
	appcontext.SetService(appcontext.MessageCenter, messageCenter)
	ec := eventcollector.New(nodeID)
	appcontext.SetService(appcontext.EventCollector, ec)
	return manager
}

func TestMergeDispatcherNormal(t *testing.T) {
	manager := createTestManager(t)

	// Create two adjacent dispatchers
	dispatcher1 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("a"),
		[]byte("m"),
	)
	dispatcher2 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("m"),
		[]byte("z"),
	)

	// Add dispatchers to manager
	manager.dispatcherMap.Set(dispatcher1.GetId(), dispatcher1)
	manager.dispatcherMap.Set(dispatcher2.GetId(), dispatcher2)

	// Execute merge
	mergedID := common.NewDispatcherID()
	manager.mergeEventDispatcher([]common.DispatcherID{dispatcher1.GetId(), dispatcher2.GetId()}, mergedID)

	// Verify merged state
	mergedDispatcher, exists := manager.dispatcherMap.Get(mergedID)
	require.True(t, exists)
	require.Equal(t, heartbeatpb.ComponentState_Preparing, mergedDispatcher.GetComponentStatus())
	require.Equal(t, []byte("a"), mergedDispatcher.GetTableSpan().StartKey)
	require.Equal(t, []byte("z"), mergedDispatcher.GetTableSpan().EndKey)
}

func TestMergeDispatcherInvalidIDs(t *testing.T) {
	manager := createTestManager(t)

	// Test case with only one dispatcherID
	dispatcher1 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("a"),
		[]byte("z"),
	)
	manager.dispatcherMap.Set(dispatcher1.GetId(), dispatcher1)

	mergedID := common.NewDispatcherID()
	manager.mergeEventDispatcher([]common.DispatcherID{dispatcher1.GetId()}, mergedID)

	// Verify no new dispatcher is created
	_, exists := manager.dispatcherMap.Get(mergedID)
	require.False(t, exists)
}

func TestMergeDispatcherExistingID(t *testing.T) {
	manager := createTestManager(t)

	// Create an existing dispatcher
	existingDispatcher := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("a"),
		[]byte("z"),
	)
	manager.dispatcherMap.Set(existingDispatcher.GetId(), existingDispatcher)

	// Try to merge using existing ID
	manager.mergeEventDispatcher([]common.DispatcherID{existingDispatcher.GetId()}, existingDispatcher.GetId())

	// Verify state remains unchanged
	dispatcher, exists := manager.dispatcherMap.Get(existingDispatcher.GetId())
	require.True(t, exists)
	require.Equal(t, heartbeatpb.ComponentState_Working, dispatcher.GetComponentStatus())
}

func TestMergeDispatcherNonExistent(t *testing.T) {
	manager := createTestManager(t)

	// Use non-existent dispatcherID
	nonExistentID := common.NewDispatcherID()
	mergedID := common.NewDispatcherID()
	manager.mergeEventDispatcher([]common.DispatcherID{nonExistentID}, mergedID)

	// Verify no new dispatcher is created
	_, exists := manager.dispatcherMap.Get(mergedID)
	require.False(t, exists)
}

func TestMergeDispatcherNotWorking(t *testing.T) {
	manager := createTestManager(t)

	// Create a dispatcher not in working state
	dispatcher1 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("a"),
		[]byte("z"),
	)
	dispatcher1.SetComponentStatus(heartbeatpb.ComponentState_Stopped)
	manager.dispatcherMap.Set(dispatcher1.GetId(), dispatcher1)

	mergedID := common.NewDispatcherID()
	manager.mergeEventDispatcher([]common.DispatcherID{dispatcher1.GetId()}, mergedID)

	// Verify no new dispatcher is created
	_, exists := manager.dispatcherMap.Get(mergedID)
	require.False(t, exists)
}

func TestMergeDispatcherNonAdjacent(t *testing.T) {
	manager := createTestManager(t)

	// Create two non-adjacent dispatchers
	dispatcher1 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("a"),
		[]byte("m"),
	)
	dispatcher2 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("n"), // Note: this is not adjacent to dispatcher1's EndKey
		[]byte("z"),
	)

	manager.dispatcherMap.Set(dispatcher1.GetId(), dispatcher1)
	manager.dispatcherMap.Set(dispatcher2.GetId(), dispatcher2)

	mergedID := common.NewDispatcherID()
	manager.mergeEventDispatcher([]common.DispatcherID{dispatcher1.GetId(), dispatcher2.GetId()}, mergedID)

	// Verify no new dispatcher is created
	_, exists := manager.dispatcherMap.Get(mergedID)
	require.False(t, exists)
}

func TestMergeDispatcherThreeDispatchers(t *testing.T) {
	manager := createTestManager(t)

	// Create three adjacent dispatchers
	dispatcher1 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("a"),
		[]byte("m"),
	)
	dispatcher2 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("m"),
		[]byte("t"),
	)
	dispatcher3 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("t"),
		[]byte("z"),
	)

	// Add dispatchers to manager
	manager.dispatcherMap.Set(dispatcher1.GetId(), dispatcher1)
	manager.dispatcherMap.Set(dispatcher2.GetId(), dispatcher2)
	manager.dispatcherMap.Set(dispatcher3.GetId(), dispatcher3)

	// Execute merge
	mergedID := common.NewDispatcherID()
	manager.mergeEventDispatcher([]common.DispatcherID{
		dispatcher1.GetId(),
		dispatcher2.GetId(),
		dispatcher3.GetId(),
	}, mergedID)

	// Verify merged state
	mergedDispatcher, exists := manager.dispatcherMap.Get(mergedID)
	require.True(t, exists)
	require.Equal(t, heartbeatpb.ComponentState_Preparing, mergedDispatcher.GetComponentStatus())
	require.Equal(t, []byte("a"), mergedDispatcher.GetTableSpan().StartKey)
	require.Equal(t, []byte("z"), mergedDispatcher.GetTableSpan().EndKey)

	// Verify original dispatchers are in waiting merge state
	dispatcher1After, exists := manager.dispatcherMap.Get(dispatcher1.GetId())
	require.True(t, exists)
	require.Equal(t, heartbeatpb.ComponentState_WaitingMerge, dispatcher1After.GetComponentStatus())

	dispatcher2After, exists := manager.dispatcherMap.Get(dispatcher2.GetId())
	require.True(t, exists)
	require.Equal(t, heartbeatpb.ComponentState_WaitingMerge, dispatcher2After.GetComponentStatus())

	dispatcher3After, exists := manager.dispatcherMap.Get(dispatcher3.GetId())
	require.True(t, exists)
	require.Equal(t, heartbeatpb.ComponentState_WaitingMerge, dispatcher3After.GetComponentStatus())
}

func TestDoMerge(t *testing.T) {
	manager := createTestManager(t)

	// Create two adjacent dispatchers
	dispatcher1 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("a"),
		[]byte("m"),
	)
	dispatcher2 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("m"),
		[]byte("z"),
	)

	// Add resolved event to dispatcher1 to update the checkpointTs
	resolvedEvent1 := event.NewResolvedEvent(300, dispatcher1.GetId(), 0)
	dispatcher1.HandleEvents([]dispatcher.DispatcherEvent{dispatcher.NewDispatcherEvent(nil, resolvedEvent1)}, func() {})

	// Add resolved event to dispatcher2 to update the checkpointTs
	resolvedEvent2 := event.NewResolvedEvent(200, dispatcher2.GetId(), 0)
	dispatcher2.HandleEvents([]dispatcher.DispatcherEvent{dispatcher.NewDispatcherEvent(nil, resolvedEvent2)}, func() {})

	// Add dispatchers to manager
	manager.dispatcherMap.Set(dispatcher1.GetId(), dispatcher1)
	manager.dispatcherMap.Set(dispatcher2.GetId(), dispatcher2)

	mergedID := common.NewDispatcherID()
	task := manager.mergeEventDispatcher([]common.DispatcherID{
		dispatcher1.GetId(),
		dispatcher2.GetId(),
	}, mergedID)

	// Execute DoMerge
	doMerge(task, task.manager.dispatcherMap)

	// Verify merged dispatcher state
	mergedDispatcherAfter, exists := manager.dispatcherMap.Get(mergedID)
	require.True(t, exists)
	require.Equal(t, heartbeatpb.ComponentState_Initializing, mergedDispatcherAfter.GetComponentStatus())
	// Verify startTs is set to the minimum checkpointTs
	require.Equal(t, uint64(200), mergedDispatcherAfter.GetStartTs())

	// Verify original dispatchers are removed
	manager.aggregateDispatcherHeartbeats(false) // use heartbeat collector to remove merged dispatchers
	_, exists = manager.dispatcherMap.Get(dispatcher1.GetId())
	require.False(t, exists)
	_, exists = manager.dispatcherMap.Get(dispatcher2.GetId())
	require.False(t, exists)
}

func TestDoMergeWithThreeDispatchers(t *testing.T) {
	manager := createTestManager(t)

	// Create three adjacent dispatchers
	dispatcher1 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("a"),
		[]byte("m"),
	)
	dispatcher2 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("m"),
		[]byte("t"),
	)
	dispatcher3 := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("t"),
		[]byte("z"),
	)

	// Add resolved event to dispatcher1 to update the checkpointTs
	resolvedEvent1 := event.NewResolvedEvent(300, dispatcher1.GetId(), 0)
	dispatcher1.HandleEvents([]dispatcher.DispatcherEvent{dispatcher.NewDispatcherEvent(nil, resolvedEvent1)}, func() {})

	// Add resolved event to dispatcher2 to update the checkpointTs
	resolvedEvent2 := event.NewResolvedEvent(100, dispatcher2.GetId(), 0)
	dispatcher2.HandleEvents([]dispatcher.DispatcherEvent{dispatcher.NewDispatcherEvent(nil, resolvedEvent2)}, func() {})

	// Add resolved event to dispatcher3 to update the checkpointTs
	resolvedEvent3 := event.NewResolvedEvent(200, dispatcher3.GetId(), 0)
	dispatcher3.HandleEvents([]dispatcher.DispatcherEvent{dispatcher.NewDispatcherEvent(nil, resolvedEvent3)}, func() {})

	// Add dispatchers to manager
	manager.dispatcherMap.Set(dispatcher1.GetId(), dispatcher1)
	manager.dispatcherMap.Set(dispatcher2.GetId(), dispatcher2)
	manager.dispatcherMap.Set(dispatcher3.GetId(), dispatcher3)

	// merge dispatcher
	mergedID := common.NewDispatcherID()
	task := manager.mergeEventDispatcher([]common.DispatcherID{
		dispatcher1.GetId(),
		dispatcher2.GetId(),
		dispatcher3.GetId(),
	}, mergedID)

	// Execute DoMerge
	doMerge(task, task.manager.dispatcherMap)

	// Verify merged dispatcher state
	mergedDispatcherAfter, exists := manager.dispatcherMap.Get(mergedID)
	require.True(t, exists)
	require.Equal(t, heartbeatpb.ComponentState_Initializing, mergedDispatcherAfter.GetComponentStatus())
	// Verify startTs is set to the minimum checkpointTs
	require.Equal(t, uint64(100), mergedDispatcherAfter.GetStartTs())

	// Verify original dispatchers are removed
	manager.aggregateDispatcherHeartbeats(false) // use heartbeat collector to remove merged dispatchers
	_, exists = manager.dispatcherMap.Get(dispatcher1.GetId())
	require.False(t, exists)
	_, exists = manager.dispatcherMap.Get(dispatcher2.GetId())
	require.False(t, exists)
	_, exists = manager.dispatcherMap.Get(dispatcher3.GetId())
	require.False(t, exists)
}
