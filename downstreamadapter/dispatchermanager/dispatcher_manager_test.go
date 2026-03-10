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
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	pkgRedo "github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/stretchr/testify/require"
)

func newDispatcherManagerTestSink(t *testing.T, sinkType common.SinkType) sink.Sink {
	t.Helper()

	ctrl := gomock.NewController(t)
	mockSink := mock.NewMockSink(ctrl)
	mockSink.EXPECT().SinkType().Return(sinkType).AnyTimes()
	mockSink.EXPECT().IsNormal().Return(true).AnyTimes()
	mockSink.EXPECT().AddDMLEvent(gomock.Any()).AnyTimes()
	mockSink.EXPECT().FlushDMLBeforeBlock(gomock.Any()).Return(nil).AnyTimes()
	mockSink.EXPECT().WriteBlockEvent(gomock.Any()).DoAndReturn(func(blockEvent event.BlockEvent) error {
		blockEvent.PostFlush()
		return nil
	}).AnyTimes()
	mockSink.EXPECT().AddCheckpointTs(gomock.Any()).AnyTimes()
	mockSink.EXPECT().SetTableSchemaStore(gomock.Any()).AnyTimes()
	mockSink.EXPECT().Close(gomock.Any()).AnyTimes()
	mockSink.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()
	return mockSink
}

type testDynamicStream[A dynstream.Area, P dynstream.Path, T dynstream.Event, D dynstream.Dest, H dynstream.Handler[A, P, T, D]] struct {
	onAddPath func() error
}

func (s *testDynamicStream[A, P, T, D, H]) Start() {}

func (s *testDynamicStream[A, P, T, D, H]) Close() {}

func (s *testDynamicStream[A, P, T, D, H]) Push(path P, event T) {}

func (s *testDynamicStream[A, P, T, D, H]) Wake(path P) {}

func (s *testDynamicStream[A, P, T, D, H]) Feedback() <-chan dynstream.Feedback[A, P, D] {
	return nil
}

func (s *testDynamicStream[A, P, T, D, H]) AddPath(path P, dest D, area ...dynstream.AreaSettings) error {
	if s.onAddPath != nil {
		return s.onAddPath()
	}
	return nil
}

func (s *testDynamicStream[A, P, T, D, H]) RemovePath(path P) error {
	return nil
}

func (s *testDynamicStream[A, P, T, D, H]) Release(path P) {}

func (s *testDynamicStream[A, P, T, D, H]) SetAreaSettings(area A, settings dynstream.AreaSettings) {}

func (s *testDynamicStream[A, P, T, D, H]) GetMetrics() dynstream.Metrics[A, P] {
	return dynstream.Metrics[A, P]{}
}

// createTestDispatcher creates a test dispatcher with given parameters
func createTestDispatcher(t *testing.T, manager *DispatcherManager, id common.DispatcherID, tableID int64, startKey, endKey []byte) *dispatcher.EventDispatcher {
	span := &heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: startKey,
		EndKey:   endKey,
	}
	var redoTs atomic.Uint64
	redoTs.Store(math.MaxUint64)
	defaultAtomicity := config.DefaultAtomicityLevel()
	sharedInfo := dispatcher.NewSharedInfo(
		manager.changefeedID,
		"system",
		false,
		false,
		false,
		nil,
		nil,
		nil,
		&defaultAtomicity,
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
		manager.sink,
		sharedInfo,
		false,
		&redoTs,
	)
	d.SetComponentStatus(heartbeatpb.ComponentState_Working)
	return d
}

// createTestManager creates a test DispatcherManager
func createTestManager(t *testing.T) *DispatcherManager {
	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	testSink := newDispatcherManagerTestSink(t, common.BlackHoleSinkType)
	manager := &DispatcherManager{
		changefeedID:            changefeedID,
		dispatcherMap:           newDispatcherMap[*dispatcher.EventDispatcher](),
		heartbeatRequestQueue:   NewHeartbeatRequestQueue(),
		blockStatusRequestQueue: NewBlockStatusRequestQueue(),
		sink:                    testSink,
		schemaIDToDispatchers:   dispatcher.NewSchemaIDToDispatchers(),
		sinkQuota:               util.GetOrZero(config.GetDefaultReplicaConfig().MemoryQuota),
		latestWatermark:         NewWatermark(0),
		latestRedoWatermark:     NewWatermark(0),
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
	defaultAtomicity := config.DefaultAtomicityLevel()
	manager.sharedInfo = dispatcher.NewSharedInfo(
		manager.changefeedID,
		"system",
		manager.config.BDRMode,
		manager.config.EnableActiveActive,
		false, // outputRawChangeEvent
		nil,   // integrityConfig
		nil,   // filterConfig
		nil,   // syncPointConfig
		&defaultAtomicity,
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

func TestCollectComponentStatusWhenChangedWatermarkSeqNoFallback(t *testing.T) {
	manager := createTestManager(t)

	manager.latestWatermark.Set(&heartbeatpb.Watermark{
		CheckpointTs: 1000,
		ResolvedTs:   1000,
		Seq:          100,
	})
	manager.latestRedoWatermark.Set(&heartbeatpb.Watermark{
		CheckpointTs: 1000,
		ResolvedTs:   1000,
		Seq:          200,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		manager.collectComponentStatusWhenChanged(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	statusesChan := manager.sharedInfo.GetStatusesChan()
	statusesChan <- dispatcher.TableSpanStatusWithSeq{
		TableSpanStatus: &heartbeatpb.TableSpanStatus{
			ID:              common.NewDispatcherID().ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    900,
			Mode:            common.DefaultMode,
		},
		Seq: 10,
	}

	dequeueCtx, cancelDequeue := context.WithTimeout(context.Background(), time.Second)
	req := manager.heartbeatRequestQueue.Dequeue(dequeueCtx)
	cancelDequeue()

	require.NotNil(t, req)
	require.NotNil(t, req.Request)
	require.NotNil(t, req.Request.Watermark)
	require.Equal(t, uint64(100), req.Request.Watermark.Seq)

	statusesChan <- dispatcher.TableSpanStatusWithSeq{
		TableSpanStatus: &heartbeatpb.TableSpanStatus{
			ID:              common.NewDispatcherID().ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    800,
			Mode:            common.RedoMode,
		},
		Seq: 20,
	}

	dequeueCtx, cancelDequeue = context.WithTimeout(context.Background(), time.Second)
	req = manager.heartbeatRequestQueue.Dequeue(dequeueCtx)
	cancelDequeue()

	require.NotNil(t, req)
	require.NotNil(t, req.Request)
	require.NotNil(t, req.Request.RedoWatermark)
	require.Equal(t, uint64(200), req.Request.RedoWatermark.Seq)
}

func TestInitRedoComponentPublishesReadyAfterRedoRegistration(t *testing.T) {
	manager := &DispatcherManager{
		changefeedID:        common.NewChangeFeedIDWithName("redo-test", common.DefaultKeyspaceName),
		latestRedoWatermark: NewWatermark(0),
		sinkQuota:           200,
		redoEnabled:         true,
		config: &config.ChangefeedConfig{
			Consistent: &config.ConsistentConfig{
				Level:                 util.AddressOf(string(pkgRedo.ConsistentLevelEventual)),
				MaxLogSize:            util.AddressOf(pkgRedo.DefaultMaxLogSize),
				Storage:               util.AddressOf("blackhole://"),
				FlushIntervalInMs:     util.AddressOf(int64(pkgRedo.MinFlushIntervalInMs)),
				MetaFlushIntervalInMs: util.AddressOf(int64(pkgRedo.MinFlushIntervalInMs)),
				EncodingWorkerNum:     util.AddressOf(1),
				FlushWorkerNum:        util.AddressOf(1),
				UseFileBackend:        util.AddressOf(false),
				MemoryUsage: &config.ConsistentMemoryUsage{
					MemoryQuotaPercentage: 25,
				},
			},
		},
	}
	registrationStarted := make(chan struct{})
	releaseRegistration := make(chan struct{})
	hb := &HeartBeatCollector{
		redoResolvedTsForwardMessageDynamicStream: &testDynamicStream[int, common.GID, RedoResolvedTsForwardMessage, *DispatcherManager, *RedoResolvedTsForwardMessageHandler]{
			onAddPath: func() error {
				select {
				case <-registrationStarted:
				default:
					close(registrationStarted)
				}
				<-releaseRegistration
				return nil
			},
		},
		redoMetaMessageDynamicStream: &testDynamicStream[int, common.GID, RedoMetaMessage, *DispatcherManager, *RedoMetaMessageHandler]{},
	}
	appcontext.SetService(appcontext.HeartbeatCollector, hb)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- initRedoComponet(ctx, manager, manager.changefeedID, nil, 0, false)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-registrationStarted:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(50), manager.redoQuota)
	require.Equal(t, uint64(150), manager.sinkQuota)
	require.False(t, manager.IsRedoReady())

	close(releaseRegistration)
	require.NoError(t, <-errCh)
	require.True(t, manager.IsRedoReady())

	cancel()
	manager.wg.Wait()
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

func TestDoMergeAbortWhenSourceDispatcherMissing(t *testing.T) {
	manager := createTestManager(t)

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

	manager.dispatcherMap.Set(dispatcher1.GetId(), dispatcher1)
	manager.dispatcherMap.Set(dispatcher2.GetId(), dispatcher2)

	mergedID := common.NewDispatcherID()
	task := manager.mergeEventDispatcher([]common.DispatcherID{
		dispatcher1.GetId(),
		dispatcher2.GetId(),
	}, mergedID)
	require.NotNil(t, task)

	manager.dispatcherMap.Delete(dispatcher1.GetId())

	require.NotPanics(t, func() {
		doMerge(task, task.manager.dispatcherMap)
	})

	mergedDispatcher, exists := manager.dispatcherMap.Get(mergedID)
	require.True(t, exists)
	require.True(t, mergedDispatcher.GetTryRemoving())

	dispatcher2After, exists := manager.dispatcherMap.Get(dispatcher2.GetId())
	require.True(t, exists)
	require.Equal(t, heartbeatpb.ComponentState_Working, dispatcher2After.GetComponentStatus())
}

func TestDoMergeAbortWhenSourceDispatcherRemoving(t *testing.T) {
	manager := createTestManager(t)

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

	manager.dispatcherMap.Set(dispatcher1.GetId(), dispatcher1)
	manager.dispatcherMap.Set(dispatcher2.GetId(), dispatcher2)

	mergedID := common.NewDispatcherID()
	task := manager.mergeEventDispatcher([]common.DispatcherID{
		dispatcher1.GetId(),
		dispatcher2.GetId(),
	}, mergedID)
	require.NotNil(t, task)

	dispatcher1.SetTryRemoving()

	require.NotPanics(t, func() {
		doMerge(task, task.manager.dispatcherMap)
	})

	mergedDispatcher, exists := manager.dispatcherMap.Get(mergedID)
	require.True(t, exists)
	require.True(t, mergedDispatcher.GetTryRemoving())

	dispatcher2After, exists := manager.dispatcherMap.Get(dispatcher2.GetId())
	require.True(t, exists)
	require.Equal(t, heartbeatpb.ComponentState_Working, dispatcher2After.GetComponentStatus())
}

func TestAbortMergeRestoresSourceDispatchersRegistration(t *testing.T) {
	manager := createTestManager(t)
	ec := appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector)

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

	manager.dispatcherMap.Set(dispatcher1.GetId(), dispatcher1)
	manager.dispatcherMap.Set(dispatcher2.GetId(), dispatcher2)

	ec.AddDispatcher(dispatcher1, manager.sinkQuota)
	ec.AddDispatcher(dispatcher2, manager.sinkQuota)
	require.True(t, ec.HasDispatcher(dispatcher1.GetId()))
	require.True(t, ec.HasDispatcher(dispatcher2.GetId()))

	dispatcher1.SetComponentStatus(heartbeatpb.ComponentState_WaitingMerge)
	dispatcher2.SetComponentStatus(heartbeatpb.ComponentState_WaitingMerge)
	ec.RemoveDispatcher(dispatcher1)
	ec.RemoveDispatcher(dispatcher2)
	require.False(t, ec.HasDispatcher(dispatcher1.GetId()))
	require.False(t, ec.HasDispatcher(dispatcher2.GetId()))

	mergedDispatcher := createTestDispatcher(t, manager,
		common.NewDispatcherID(),
		1,
		[]byte("a"),
		[]byte("z"),
	)
	manager.dispatcherMap.Set(mergedDispatcher.GetId(), mergedDispatcher)

	taskScheduler := threadpool.NewThreadPoolDefault()
	defer taskScheduler.Stop()
	taskHandle := taskScheduler.SubmitFunc(func() time.Time { return time.Time{} }, time.Now())

	task := &MergeCheckTask{
		taskHandle:       taskHandle,
		manager:          manager,
		mergedDispatcher: mergedDispatcher,
		dispatcherIDs: []common.DispatcherID{
			dispatcher1.GetId(),
			dispatcher2.GetId(),
		},
	}

	abortMerge(task, manager.dispatcherMap, manager.sink.SinkType(), "test_abort")

	require.Equal(t, heartbeatpb.ComponentState_Working, dispatcher1.GetComponentStatus())
	require.Equal(t, heartbeatpb.ComponentState_Working, dispatcher2.GetComponentStatus())
	require.True(t, ec.HasDispatcher(dispatcher1.GetId()))
	require.True(t, ec.HasDispatcher(dispatcher2.GetId()))
}
