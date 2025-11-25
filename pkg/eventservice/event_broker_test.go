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
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func newEventBrokerForTest() (*eventBroker, *mockEventStore, *mockSchemaStore, chan *messaging.TargetMessage) {
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	es := newMockEventStore(100)
	ss := NewMockSchemaStore()
	mc := messaging.NewMockMessageCenter()
	outputCh := mc.GetMessageChannel()
	return newEventBroker(context.Background(), 1, es, ss, mc, time.UTC, &integrity.Config{
		IntegrityCheckLevel:   integrity.CheckLevelNone,
		CorruptionHandleLevel: integrity.CorruptionHandleLevelWarn,
	}), es, ss, outputCh
}

func newMockDispatcherInfoForTest(t *testing.T) *mockDispatcherInfo {
	did := common.NewDispatcherID()
	return newMockDispatcherInfo(t, 300, did, 100, eventpb.ActionType_ACTION_TYPE_REGISTER)
}

type notifyMsg struct {
	resolvedTs     uint64
	latestCommitTs uint64
}

func TestCheckNeedScan(t *testing.T) {
	broker, _, _, _ := newEventBrokerForTest()
	// Close the broker, so we can catch all message in the test.
	broker.close()

	disInfo := newMockDispatcherInfoForTest(t)
	changefeedStatus := broker.getOrSetChangefeedStatus(disInfo.GetChangefeedID())

	info := newMockDispatcherInfoForTest(t)
	info.startTs = 100
	disp := newDispatcherStat(info, 1, 1, nil, changefeedStatus)
	// Set the receivedResolvedTs and eventStoreCommitTs to 102 and 101.
	// To simulate the eventStore has just notified the broker.
	disp.receivedResolvedTs.Store(102)
	disp.eventStoreCommitTs.Store(101)

	// Case 1: Is scanning, and mustCheck is false, it should return false.
	disp.isTaskScanning.Store(true)
	needScan := broker.scanReady(disp)
	require.False(t, needScan)
	disp.isTaskScanning.Store(false)
	log.Info("Pass case 1")

	// Case 2: epoch is 0, it should return false.
	// And the broker will send a ready event.
	needScan = broker.scanReady(disp)
	require.False(t, needScan)
	e := <-broker.messageCh[0]
	require.Equal(t, event.TypeReadyEvent, e.msgType)
	log.Info("Pass case 2")

	// Case 3: epoch is not 0, it should return true.
	// And we can get a scan task.
	// And the task.scanning should be true.
	// And the broker will send a handshake event.
	disp.epoch = 1
	needScan = broker.scanReady(disp)
	require.True(t, needScan)
	e = <-broker.messageCh[0]
	require.Equal(t, event.TypeHandshakeEvent, e.msgType)
	log.Info("Pass case 3")
}

func TestOnNotify(t *testing.T) {
	broker, _, ss, _ := newEventBrokerForTest()
	// Close the broker, so we can catch all message in the test.
	broker.close()

	disInfo := newMockDispatcherInfoForTest(t)
	disInfo.epoch = 1
	disInfo.startTs = 100

	err := broker.addDispatcher(disInfo)
	require.NoError(t, err)

	disp := broker.getDispatcher(disInfo.GetID()).Load()
	require.NotNil(t, disp)
	require.Equal(t, disInfo.GetID(), disp.id)

	err = broker.resetDispatcher(disInfo)
	require.Nil(t, err)
	require.Equal(t, disp.lastScannedCommitTs.Load(), uint64(100))
	require.Equal(t, disp.lastScannedStartTs.Load(), uint64(0))

	disp.setHandshaked()

	// Case 1: The resolvedTs is greater than the startTs, it should be updated.
	notifyMsgs := notifyMsg{101, 1}
	broker.onNotify(disp, notifyMsgs.resolvedTs, notifyMsgs.latestCommitTs)
	require.Equal(t, uint64(101), disp.receivedResolvedTs.Load())
	log.Info("Pass case 1")

	// Case 2: The eventStoreCommitTs is greater than the startTs, it triggers a scan task.
	notifyMsgs = notifyMsg{102, 101}
	broker.onNotify(disp, notifyMsgs.resolvedTs, notifyMsgs.latestCommitTs)
	require.Equal(t, uint64(102), disp.receivedResolvedTs.Load())
	require.True(t, disp.isTaskScanning.Load())
	task := <-broker.taskChan[disp.scanWorkerIndex]
	require.Equal(t, task.id, disp.id)
	log.Info("Pass case 2")

	// Case 3: When the scan task is running, even there is a larger resolvedTs,
	// should not trigger a new scan task.
	notifyMsgs = notifyMsg{103, 101}
	broker.onNotify(disp, notifyMsgs.resolvedTs, notifyMsgs.latestCommitTs)
	require.Equal(t, uint64(103), disp.receivedResolvedTs.Load())
	after := time.After(50 * time.Millisecond)
	select {
	case <-after:
		log.Info("Pass case 3")
	case task := <-broker.taskChan[disp.scanWorkerIndex]:
		log.Info("trigger a new scan task", zap.Any("task", task.id.String()), zap.Any("resolvedTs", task.receivedResolvedTs.Load()), zap.Any("eventStoreCommitTs", task.eventStoreCommitTs.Load()), zap.Any("isTaskScanning", task.isTaskScanning.Load()))
		require.Fail(t, "should not trigger a new scan task")
	}

	// Case 4: Do scan, it will update the sentResolvedTs.
	status := broker.getOrSetChangefeedStatus(disInfo.GetChangefeedID())
	status.availableMemoryQuota.Store(node.ID(task.info.GetServerID()), atomic.NewUint64(broker.scanLimitInBytes))

	broker.doScan(context.TODO(), task)
	require.False(t, disp.isTaskScanning.Load())
	require.Equal(t, notifyMsgs.resolvedTs, disp.sentResolvedTs.Load())
	log.Info("pass case 4")

	notifyMsgs5 := notifyMsg{104, 101}
	// Set the schemaStore's maxDDLCommitTs to the sentResolvedTs, so the broker will not scan the schemaStore.
	ss.maxDDLCommitTs = disp.sentResolvedTs.Load()
	broker.onNotify(disp, notifyMsgs5.resolvedTs, notifyMsgs5.latestCommitTs)
	broker.doScan(context.TODO(), task)
	require.Equal(t, notifyMsgs5.resolvedTs, disp.sentResolvedTs.Load())
	log.Info("Pass case 6")
}

func TestCURDDispatcher(t *testing.T) {
	broker, _, _, _ := newEventBrokerForTest()
	defer broker.close()

	dispInfo := newMockDispatcherInfoForTest(t)
	// Case 1: Add and get a dispatcher.
	err := broker.addDispatcher(dispInfo)
	require.Nil(t, err)
	disp := broker.getDispatcher(dispInfo.GetID()).Load()
	require.NotNil(t, disp)
	// Check changefeedStatus after adding a dispatcher
	cfStatus, ok := broker.changefeedMap.Load(dispInfo.GetChangefeedID())
	require.True(t, ok, "changefeedStatus should exist after adding a dispatcher")
	require.False(t, cfStatus.(*changefeedStatus).isEmpty(), "changefeedStatus should not be empty")

	require.Equal(t, disp.id, dispInfo.GetID())

	// Case 2: Reset a dispatcher.
	dispInfo.startTs = 1002
	dispInfo.epoch = 2
	err = broker.resetDispatcher(dispInfo)
	require.Nil(t, err)
	disp = broker.getDispatcher(dispInfo.GetID()).Load()
	require.NotNil(t, disp)
	require.Equal(t, disp.id, dispInfo.GetID())
	// Check the resetTs is updated.
	// Check changefeedStatus after resetting a dispatcher
	cfStatus, ok = broker.changefeedMap.Load(dispInfo.GetChangefeedID())
	require.True(t, ok, "changefeedStatus should still exist after resetting")
	require.False(t, cfStatus.(*changefeedStatus).isEmpty(), "changefeedStatus should not be empty after resetting")
	require.Equal(t, disp.startTs, dispInfo.GetStartTs())

	// Case 3: Remove a dispatcher.
	broker.removeDispatcher(dispInfo)
	dispPtr := broker.getDispatcher(dispInfo.GetID())
	require.Nil(t, dispPtr)
	// Check changefeedStatus after removing the only dispatcher
	_, ok = broker.changefeedMap.Load(dispInfo.GetChangefeedID())
	require.False(t, ok, "changefeedStatus should be removed after the last dispatcher is removed")
}

func TestResetDispatcher(t *testing.T) {
	broker, _, _, _ := newEventBrokerForTest()
	defer broker.close()

	// 1. Reset a non-existent dispatcher.
	dispInfo := newMockDispatcherInfoForTest(t)
	err := broker.resetDispatcher(dispInfo)
	require.Nil(t, err, "resetting a non-existent dispatcher should not return an error")
	dispPtr := broker.getDispatcher(dispInfo.GetID())
	require.Nil(t, dispPtr, "dispatcher should not be created after a failed reset")

	// 2. Add a dispatcher first.
	err = broker.addDispatcher(dispInfo)
	require.Nil(t, err)
	dispPtr = broker.getDispatcher(dispInfo.GetID())
	require.NotNil(t, dispPtr)
	oldStat := dispPtr.Load()
	require.Equal(t, uint64(0), oldStat.epoch)
	require.Equal(t, dispInfo.startTs, oldStat.startTs)

	// 3. Reset with a stale epoch.
	staleDispInfo := newMockDispatcherInfo(t, 400, dispInfo.GetID(), 100, eventpb.ActionType_ACTION_TYPE_RESET)
	staleDispInfo.epoch = 0 // same as oldStat.epoch
	err = broker.resetDispatcher(staleDispInfo)
	require.Nil(t, err)
	currentStat := dispPtr.Load()
	require.Same(t, oldStat, currentStat, "dispatcherStat should not be replaced with a stale epoch")

	// 4. Successful reset.
	resetDispInfo := newMockDispatcherInfo(t, 500, dispInfo.GetID(), 100, eventpb.ActionType_ACTION_TYPE_RESET)
	resetDispInfo.epoch = 1 // new epoch

	// Set some statistics to check if they are copied.
	oldStat.checkpointTs.Store(120)
	oldStat.hasReceivedFirstResolvedTs.Store(true)
	oldStat.currentScanLimitInBytes.Store(2048)

	err = broker.resetDispatcher(resetDispInfo)
	require.Nil(t, err)

	newStat := dispPtr.Load()
	require.NotSame(t, oldStat, newStat, "dispatcherStat should be replaced")
	require.True(t, oldStat.isRemoved.Load(), "old dispatcherStat should be marked as removed")

	require.Equal(t, uint64(1), newStat.epoch)
	require.Equal(t, uint64(500), newStat.startTs)
	require.Equal(t, dispInfo.GetID(), newStat.id)
}

func TestResetDispatcherConcurrently(t *testing.T) {
	broker, _, _, _ := newEventBrokerForTest()
	defer broker.close()

	// 1. Add a dispatcher first.
	dispInfo := newMockDispatcherInfoForTest(t)
	err := broker.addDispatcher(dispInfo)
	require.NoError(t, err)

	dispPtr := broker.getDispatcher(dispInfo.GetID())
	require.NotNil(t, dispPtr)
	initialStat := dispPtr.Load()
	require.Equal(t, uint64(0), initialStat.epoch)

	// 2. Prepare for concurrent resets.
	concurrency := 10
	var wg sync.WaitGroup
	wg.Add(concurrency)

	maxEpoch := uint64(concurrency)

	// 3. Spawn goroutines to reset concurrently.
	for i := 1; i <= concurrency; i++ {
		go func(epoch uint64) {
			defer wg.Done()
			resetInfo := newMockDispatcherInfo(t, 500+epoch, dispInfo.GetID(), 100, eventpb.ActionType_ACTION_TYPE_RESET)
			resetInfo.epoch = epoch
			err := broker.resetDispatcher(resetInfo)
			require.NoError(t, err)
		}(uint64(i))
	}

	// 4. Wait for all goroutines to finish.
	wg.Wait()

	// 5. Verify the final state has the max epoch.
	finalStat := dispPtr.Load()
	require.Equal(t, maxEpoch, finalStat.epoch, "the final epoch should be the maximum one")
	require.Equal(t, 500+maxEpoch, finalStat.startTs, "the final startTs should correspond to the max epoch")
}

func TestHandleResolvedTs(t *testing.T) {
	broker, _, _, outputCh := newEventBrokerForTest()
	defer broker.close()

	dispInfo := newMockDispatcherInfoForTest(t)
	err := broker.addDispatcher(dispInfo)
	require.Nil(t, err)
	disp := broker.getDispatcher(dispInfo.GetID()).Load()
	require.NotNil(t, disp)
	require.Equal(t, disp.id, dispInfo.GetID())

	ctx := context.Background()
	cacheMap := make(map[node.ID]*resolvedTsCache)
	wrapEvent := &wrapEvent{
		serverID:        "test",
		resolvedTsEvent: event.NewResolvedEvent(100, dispInfo.GetID(), 0),
	}
	// handle resolvedTsCacheSize resolvedTs events, so the cache is full.
	for i := 0; i < resolvedTsCacheSize+1; i++ {
		broker.handleResolvedTs(ctx, cacheMap, wrapEvent, disp.messageWorkerIndex, messaging.EventCollectorTopic)
	}

	msg := <-outputCh
	require.Equal(t, msg.Type, messaging.TypeBatchResolvedTs)
}

func TestHandleDispatcherHeartbeat_InactiveDispatcherCleanup(t *testing.T) {
	broker, _, _, outputCh := newEventBrokerForTest()
	defer broker.close()

	// Create a dispatcher and add it to the broker
	dispInfo := newMockDispatcherInfoForTest(t)
	err := broker.addDispatcher(dispInfo)
	require.NoError(t, err)

	// Verify dispatcher exists
	dispatcher := broker.getDispatcher(dispInfo.GetID()).Load()
	require.NotNil(t, dispatcher)
	require.Equal(t, dispatcher.id, dispInfo.GetID())
	dispatcher.setHandshaked()

	// Create a heartbeat with progress for the existing dispatcher
	heartbeat := &DispatcherHeartBeatWithServerID{
		serverID: "test-server-1",
		heartbeat: &event.DispatcherHeartbeat{
			Version:         event.DispatcherHeartbeatVersion1,
			ClusterID:       0,
			DispatcherCount: 1,
			DispatcherProgresses: []event.DispatcherProgress{
				{
					DispatcherID: dispInfo.GetID(),
					CheckpointTs: 100,
				},
			},
		},
	}

	// Handle heartbeat - should update the dispatcher's heartbeat time and checkpoint
	broker.handleDispatcherHeartbeat(heartbeat)

	// Verify the dispatcher's checkpoint and heartbeat time were updated
	// The checkpoint should be updated to the higher value (from heartbeat)
	require.GreaterOrEqual(t, dispatcher.checkpointTs.Load(), uint64(100))
	require.Greater(t, dispatcher.lastReceivedHeartbeatTime.Load(), int64(0))

	// Now Set this dispatcher lastReceivedHeartbeatTime to a time in the past
	// it should be considered as inactive and removed
	dispatcher.lastReceivedHeartbeatTime.Store(time.Now().Add(-heartbeatTimeout * 2).Unix())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go broker.reportDispatcherStatToStore(ctx, time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	// Create a heartbeat for the now-removed (inactive) dispatcher
	heartbeatForInactiveDispatcher := &DispatcherHeartBeatWithServerID{
		serverID: "test-server-1",
		heartbeat: &event.DispatcherHeartbeat{
			Version:         event.DispatcherHeartbeatVersion1,
			ClusterID:       0,
			DispatcherCount: 1,
			DispatcherProgresses: []event.DispatcherProgress{
				{
					DispatcherID: dispInfo.GetID(), // Same dispatcher ID but it's removed
					CheckpointTs: 200,
				},
			},
		},
	}

	// Mock the message center to capture the response
	// Handle heartbeat for the removed dispatcher
	// This should generate a response indicating the dispatcher should be removed
	broker.handleDispatcherHeartbeat(heartbeatForInactiveDispatcher)

	// Verify dispatcher is removed
	removedDispatcher := broker.getDispatcher(dispInfo.GetID())
	require.Nil(t, removedDispatcher)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Verify that a response was sent indicating the dispatcher is removed
	select {
	case msg := <-outputCh:
		require.Equal(t, messaging.TypeDispatcherHeartbeatResponse, msg.Type)
		// The response should contain a dispatcher state indicating removal
		require.Len(t, msg.Message, 1)
		response := msg.Message[0].(*event.DispatcherHeartbeatResponse)
		require.NotNil(t, response)
		states := response.DispatcherStates
		require.Len(t, states, 1)
		require.Equal(t, dispInfo.GetID(), states[0].DispatcherID)
		require.Equal(t, event.DSStateRemoved, states[0].State)
	case <-ctx.Done():
		require.Fail(t, "Expected to receive a dispatcher heartbeat response")
	}
}

// TestSendHandshakeIfNeedConcurrency tests the concurrent safety of sendHandshakeIfNeed method
func TestSendHandshakeIfNeedConcurrency(t *testing.T) {
	broker, _, _, outputCh := newEventBrokerForTest()
	defer broker.close()

	// Create a mock dispatcher info
	dispInfo := newMockDispatcherInfoForTest(t)
	changefeedStatus := broker.getOrSetChangefeedStatus(dispInfo.GetChangefeedID())

	// Test 1: Sequential calls should only send one handshake
	t.Run("Sequential calls", func(t *testing.T) {
		info := newMockDispatcherInfoForTest(t)
		info.startTs = 100
		disp := newDispatcherStat(info, 1, 1, nil, changefeedStatus)
		disp.epoch = 1

		// Clear all message channels
		for i := range broker.messageCh {
			for len(broker.messageCh[i]) > 0 {
				<-broker.messageCh[i]
			}
		}

		// Call sendHandshakeIfNeed multiple times sequentially
		broker.sendHandshakeIfNeed(disp)
		broker.sendHandshakeIfNeed(disp)
		broker.sendHandshakeIfNeed(disp)

		// Give a small delay for messages to be processed
		time.Sleep(10 * time.Millisecond)

		// Should only receive one handshake event
		handshakeCount := 0
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
	LOOP:
		for {
			select {
			case e := <-outputCh:
				if e.Type == messaging.TypeHandshakeEvent {
					handshakeCount++
				}
			case <-ctx.Done():
				break LOOP
			}
		}

		require.Equal(t, 1, handshakeCount, "Should only send one handshake event")
		require.True(t, disp.isHandshaked(), "Dispatcher should be marked as handshaked")
	})

	// Test 2: Concurrent calls - this is the critical test
	t.Run("Concurrent calls", func(t *testing.T) {
		// Create a new dispatcher
		info := newMockDispatcherInfoForTest(t)
		info.startTs = 100
		disp := newDispatcherStat(info, 1, 1, nil, changefeedStatus)
		disp.epoch = 1

		// Clear all message channels
		for i := range broker.messageCh {
			for len(broker.messageCh[i]) > 0 {
				<-broker.messageCh[i]
			}
		}

		const numGoroutines = 100
		var wg sync.WaitGroup
		var startBarrier sync.WaitGroup
		startBarrier.Add(1)

		// Launch multiple goroutines to call sendHandshakeIfNeed concurrently
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Wait for all goroutines to be ready
				startBarrier.Wait()
				// Call the method
				broker.sendHandshakeIfNeed(disp)
			}()
		}

		// Start all goroutines at the same time
		startBarrier.Done()

		// Wait for all goroutines to complete
		wg.Wait()

		// Give a small delay for messages to be processed
		time.Sleep(10 * time.Millisecond)

		// Count handshake events
		handshakeCount := 0
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
	LOOP:
		for {
			select {
			case e := <-outputCh:
				if e.Type == messaging.TypeHandshakeEvent {
					handshakeCount++
				}
			case <-ctx.Done():
				break LOOP
			}
		}
		// The handshake should only be sent once, even with concurrent calls
		require.Equal(t, 1, handshakeCount, "Expected exactly 1 handshake event")
		require.True(t, disp.isHandshaked(), "Dispatcher should be marked as handshaked")
	})
}

func TestAddDispatcherFailure(t *testing.T) {
	broker, _, ss, _ := newEventBrokerForTest()
	defer broker.close()

	// Simulate schema store failure
	ss.registerTableError = errors.New("mock error")

	dispInfo := newMockDispatcherInfoForTest(t)
	err := broker.addDispatcher(dispInfo)
	require.Error(t, err)

	_, ok := broker.changefeedMap.Load(dispInfo.GetChangefeedID())
	require.False(t, ok, "changefeedStatus should be removed after failed registration")
}
