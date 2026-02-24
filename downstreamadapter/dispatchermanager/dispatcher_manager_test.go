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

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/sink/recoverable"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/threadpool"
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
	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	manager := &DispatcherManager{
		changefeedID:            changefeedID,
		dispatcherMap:           newDispatcherMap[*dispatcher.EventDispatcher](),
		heartbeatRequestQueue:   NewHeartbeatRequestQueue(),
		blockStatusRequestQueue: NewBlockStatusRequestQueue(),
		sink:                    mockSink,
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

func TestCollectRecoverableErrorsEnqueueRecoverDispatcherRequest(t *testing.T) {
	manager := &DispatcherManager{
		changefeedID:                  common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		recoverDispatcherRequestQueue: make(chan *RecoverDispatcherRequestWithTargetID, 1),
		dispatcherMap:                 newDispatcherMap[*dispatcher.EventDispatcher](),
		reporter:                      recoverable.NewReporter(recoverEventChSize),
		recoverTracker:                newRecoverTracker(common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)),
	}
	manager.SetMaintainerID(node.ID("maintainer"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		manager.collectRecoverableEvents(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	dispatcherID := common.NewDispatcherID()
	manager.dispatcherMap.Set(dispatcherID, nil)
	reported, handled := manager.reporter.Report([]recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 1},
	})
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)

	var req *RecoverDispatcherRequestWithTargetID
	select {
	case req = <-manager.recoverDispatcherRequestQueue:
	case <-time.After(time.Second):
		t.Fatal("recover dispatcher request not received")
	}

	require.NotNil(t, req)
	require.Equal(t, node.ID("maintainer"), req.TargetID)
	require.NotNil(t, req.Request)
	require.Equal(t, manager.changefeedID.ToPB(), req.Request.ChangefeedID)
	require.Equal(t, []*heartbeatpb.DispatcherID{dispatcherID.ToPB()}, req.Request.DispatcherIDs)
}

func TestCollectRecoverableEventsSkipPendingDispatcher(t *testing.T) {
	manager := &DispatcherManager{
		changefeedID:                  common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		recoverDispatcherRequestQueue: make(chan *RecoverDispatcherRequestWithTargetID, 4),
		dispatcherMap:                 newDispatcherMap[*dispatcher.EventDispatcher](),
		reporter:                      recoverable.NewReporter(recoverEventChSize),
		recoverTracker:                newRecoverTracker(common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)),
	}
	manager.SetMaintainerID(node.ID("maintainer"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		manager.collectRecoverableEvents(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	dispatcherID := common.NewDispatcherID()
	manager.dispatcherMap.Set(dispatcherID, nil)
	reported, handled := manager.reporter.Report([]recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 1},
	})
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)

	select {
	case <-manager.recoverDispatcherRequestQueue:
	case <-time.After(time.Second):
		t.Fatal("first recover dispatcher request not received")
	}

	reported, handled = manager.reporter.Report([]recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 2},
	})
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)

	select {
	case <-manager.recoverDispatcherRequestQueue:
		t.Fatal("unexpected duplicate recover dispatcher request for pending dispatcher")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestResendPendingRecoverDispatcherRequest(t *testing.T) {
	manager := &DispatcherManager{
		changefeedID:                  common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		recoverDispatcherRequestQueue: make(chan *RecoverDispatcherRequestWithTargetID, 1),
		dispatcherMap:                 newDispatcherMap[*dispatcher.EventDispatcher](),
		reporter:                      recoverable.NewReporter(recoverEventChSize),
		recoverTracker:                newRecoverTracker(common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)),
	}

	dispatcherID := common.NewDispatcherID()
	manager.dispatcherMap.Set(dispatcherID, nil)
	manager.recoverTracker.add([]common.DispatcherID{dispatcherID})

	manager.SetMaintainerID(node.ID("maintainer-new"))
	manager.resendPendingRecoverRequests(context.Background())

	select {
	case req := <-manager.recoverDispatcherRequestQueue:
		require.Equal(t, node.ID("maintainer-new"), req.TargetID)
		require.NotNil(t, req.Request)
		require.Equal(t, []*heartbeatpb.DispatcherID{dispatcherID.ToPB()}, req.Request.DispatcherIDs)
	case <-time.After(time.Second):
		t.Fatal("resend recover dispatcher request not received")
	}
}

func TestResendPendingRecoverDispatcherRequestSkipNonLocal(t *testing.T) {
	manager := &DispatcherManager{
		changefeedID:                  common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		recoverDispatcherRequestQueue: make(chan *RecoverDispatcherRequestWithTargetID, 1),
		dispatcherMap:                 newDispatcherMap[*dispatcher.EventDispatcher](),
		reporter:                      recoverable.NewReporter(recoverEventChSize),
		recoverTracker:                newRecoverTracker(common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)),
	}
	manager.SetMaintainerID(node.ID("maintainer"))

	dispatcherID := common.NewDispatcherID()
	manager.recoverTracker.add([]common.DispatcherID{dispatcherID})

	manager.resendPendingRecoverRequests(context.Background())

	select {
	case <-manager.recoverDispatcherRequestQueue:
		t.Fatal("unexpected recover dispatcher request for non-local dispatcher")
	default:
	}
	require.Equal(t, []common.DispatcherID{dispatcherID}, manager.recoverTracker.pendingDispatcherIDs())
}

func TestResendPendingRecoverDispatcherRequestClearSupersededNonLocal(t *testing.T) {
	manager := &DispatcherManager{
		changefeedID:                  common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		recoverDispatcherRequestQueue: make(chan *RecoverDispatcherRequestWithTargetID, 1),
		dispatcherMap:                 newDispatcherMap[*dispatcher.EventDispatcher](),
		reporter:                      recoverable.NewReporter(recoverEventChSize),
		recoverTracker:                newRecoverTracker(common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)),
	}
	manager.SetMaintainerID(node.ID("maintainer"))

	dispatcherID := common.NewDispatcherID()
	manager.recoverTracker.add([]common.DispatcherID{dispatcherID})
	manager.onDispatcherRemovedForRecover(dispatcherID)

	reported, handled := manager.reporter.Report([]recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 5},
	})
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
	<-manager.reporter.OutputCh()

	manager.resendPendingRecoverRequests(context.Background())

	select {
	case <-manager.recoverDispatcherRequestQueue:
		t.Fatal("unexpected recover dispatcher request for superseded non-local dispatcher")
	default:
	}
	require.Empty(t, manager.recoverTracker.pendingDispatcherIDs())

	reported, handled = manager.reporter.Report([]recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 1},
	})
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
}

func TestRecoverPendingClearsAfterDispatcherRecreated(t *testing.T) {
	manager := &DispatcherManager{
		changefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
	}
	manager.reporter = recoverable.NewReporter(2)
	manager.recoverTracker = newRecoverTracker(manager.changefeedID)

	dispatcherID := common.NewDispatcherID()
	reported, handled := manager.reporter.Report([]recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 5},
	})
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
	<-manager.reporter.OutputCh()

	manager.recoverTracker.add([]common.DispatcherID{dispatcherID})
	manager.onDispatcherRemovedForRecover(dispatcherID)
	require.Equal(t, []common.DispatcherID{dispatcherID}, manager.recoverTracker.pendingDispatcherIDs())

	manager.onDispatcherRecreatedForRecover(dispatcherID)
	require.Empty(t, manager.recoverTracker.pendingDispatcherIDs())

	reported, handled = manager.reporter.Report([]recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 1},
	})
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
}

func TestWorkingStatusClearsRecoverReporterState(t *testing.T) {
	manager := &DispatcherManager{
		changefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
	}
	manager.reporter = recoverable.NewReporter(2)
	manager.recoverTracker = newRecoverTracker(manager.changefeedID)

	dispatcherID := common.NewDispatcherID()
	reported, handled := manager.reporter.Report([]recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 5},
	})
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
	<-manager.reporter.OutputCh()

	manager.recoverTracker.add([]common.DispatcherID{dispatcherID})
	manager.onDispatcherStatusForRecover(&heartbeatpb.TableSpanStatus{
		ID:              dispatcherID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
	})

	reported, handled = manager.reporter.Report([]recoverable.DispatcherEpoch{
		{DispatcherID: dispatcherID, Epoch: 1},
	})
	require.True(t, handled)
	require.Equal(t, []common.DispatcherID{dispatcherID}, reported)
}

func TestRecoverPendingTimeoutFallback(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	defaultAtomicity := config.DefaultAtomicityLevel()
	manager := &DispatcherManager{
		changefeedID: changefeedID,
		sharedInfo: dispatcher.NewSharedInfo(
			changefeedID,
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
		),
	}
	manager.reporter = recoverable.NewReporter(1)
	manager.recoverTracker = newRecoverTracker(manager.changefeedID)
	dispatcherID := common.NewDispatcherID()
	manager.recoverTracker.addAt(
		[]common.DispatcherID{dispatcherID},
		time.Now().Add(-recoverLifecyclePendingTimeout-time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager.reportRecoverPendingTimeout(ctx)

	select {
	case err := <-manager.sharedInfo.GetErrCh():
		require.Equal(t, errors.ErrChangefeedRetryable.RFCCode(), errors.ErrorCode(err))
	case <-time.After(time.Second):
		t.Fatal("expected recover pending timeout fallback error")
	}

	timedOut := manager.recoverTracker.takeExpired()
	require.Empty(t, timedOut)
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

func TestDoMergeForceCloseWhenRecoverPending(t *testing.T) {
	manager := createTestManager(t)
	manager.recoverTracker = newRecoverTracker(manager.changefeedID)

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

	dmlEvent := event.NewDMLEvent(dispatcher1.GetId(), 1, 1, 2, nil)
	dmlEvent.Length = 1
	require.True(t, dispatcher1.AddDMLEventsToSink([]*event.DMLEvent{dmlEvent}))
	_, ok := dispatcher1.TryClose(false)
	require.False(t, ok)

	manager.dispatcherMap.Set(dispatcher1.GetId(), dispatcher1)
	manager.dispatcherMap.Set(dispatcher2.GetId(), dispatcher2)
	manager.recoverTracker.add([]common.DispatcherID{dispatcher1.GetId()})

	mergedID := common.NewDispatcherID()
	task := manager.mergeEventDispatcher([]common.DispatcherID{
		dispatcher1.GetId(),
		dispatcher2.GetId(),
	}, mergedID)

	done := make(chan struct{})
	go func() {
		doMerge(task, task.manager.dispatcherMap)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("doMerge should finish with force close when dispatcher is recover pending")
	}

	mergedDispatcherAfter, exists := manager.dispatcherMap.Get(mergedID)
	require.True(t, exists)
	require.Equal(t, heartbeatpb.ComponentState_Initializing, mergedDispatcherAfter.GetComponentStatus())
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
