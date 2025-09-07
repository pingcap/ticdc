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

func newEventBrokerForTest() (*eventBroker, *mockEventStore, *mockSchemaStore) {
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	es := newMockEventStore(100)
	ss := newMockSchemaStore()
	mc := newMockMessageCenter()
	return newEventBroker(context.Background(), 1, es, ss, mc, time.UTC, &integrity.Config{
		IntegrityCheckLevel:   integrity.CheckLevelNone,
		CorruptionHandleLevel: integrity.CorruptionHandleLevelWarn,
	}), es, ss
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
	broker, _, _ := newEventBrokerForTest()
	// Close the broker, so we can catch all message in the test.
	broker.close()

	disInfo := newMockDispatcherInfoForTest(t)
	changefeedStatus := broker.getOrSetChangefeedStatus(disInfo.GetChangefeedID())

	info := newMockDispatcherInfoForTest(t)
	info.startTs = 100
	disp := newDispatcherStat(info, nil, 0, 0, changefeedStatus)
	// Set the eventStoreResolvedTs and eventStoreCommitTs to 102 and 101.
	// To simulate the eventStore has just notified the broker.
	disp.eventStoreResolvedTs.Store(102)
	disp.eventStoreCommitTs.Store(101)

	// Case 1: Is scanning, and mustCheck is false, it should return false.
	disp.isTaskScanning.Store(true)
	needScan := broker.scanReady(disp)
	require.False(t, needScan)
	disp.isTaskScanning.Store(false)
	log.Info("Pass case 1")

	// Case 2: ResetTs is 0, it should return false.
	// And the broker will send a ready event.
	needScan = broker.scanReady(disp)
	require.False(t, needScan)
	e := <-broker.messageCh[0]
	require.Equal(t, event.TypeReadyEvent, e.msgType)
	log.Info("Pass case 2")

	// Case 3: ResetTs is not 0, it should return true.
	// And we can get a scan task.
	// And the task.scanning should be true.
	// And the broker will send a handshake event.
	disp.resetTs.Store(100)
	needScan = broker.scanReady(disp)
	require.True(t, needScan)
	e = <-broker.messageCh[0]
	require.Equal(t, event.TypeHandshakeEvent, e.msgType)
	log.Info("Pass case 3")

	// Case 4: The task.isRunning is false, it should return false.
	disp.isReadyReceivingData.Store(false)
	needScan = broker.scanReady(disp)
	require.False(t, needScan)
	log.Info("Pass case 4")
}

func TestOnNotify(t *testing.T) {
	broker, _, ss := newEventBrokerForTest()
	// Close the broker, so we can catch all message in the test.
	broker.close()

	disInfo := newMockDispatcherInfoForTest(t)
	disInfo.startTs = 100

	err := broker.addDispatcher(disInfo)
	require.NoError(t, err)

	disp := broker.getDispatcher(disInfo.GetID())
	require.NotNil(t, disp)
	require.Equal(t, disp.id, disInfo.GetID())

	broker.resetDispatcher(disInfo)

	disp.resetState(100)
	require.Equal(t, disp.isHandshaked.Load(), false)
	require.Equal(t, disp.isReadyReceivingData.Load(), true)
	require.Equal(t, disp.lastScannedCommitTs.Load(), uint64(100))
	require.Equal(t, disp.lastScannedStartTs.Load(), uint64(0))

	disp.isHandshaked.Store(true)

	// Case 1: The resolvedTs is greater than the startTs, it should be updated.
	notifyMsgs := notifyMsg{101, 1}
	broker.onNotify(disp, notifyMsgs.resolvedTs, notifyMsgs.latestCommitTs)
	require.Equal(t, uint64(101), disp.eventStoreResolvedTs.Load())
	log.Info("Pass case 1")

	// Case 2: The eventStoreCommitTs is greater than the startTs, it triggers a scan task.
	notifyMsgs = notifyMsg{102, 101}
	broker.onNotify(disp, notifyMsgs.resolvedTs, notifyMsgs.latestCommitTs)
	require.Equal(t, uint64(102), disp.eventStoreResolvedTs.Load())
	require.True(t, disp.isTaskScanning.Load())
	task := <-broker.taskChan[disp.scanWorkerIndex]
	require.Equal(t, task.id, disp.id)
	log.Info("Pass case 2")

	// Case 3: When the scan task is running, even there is a larger resolvedTs,
	// should not trigger a new scan task.
	notifyMsgs = notifyMsg{103, 101}
	broker.onNotify(disp, notifyMsgs.resolvedTs, notifyMsgs.latestCommitTs)
	require.Equal(t, uint64(103), disp.eventStoreResolvedTs.Load())
	after := time.After(50 * time.Millisecond)
	select {
	case <-after:
		log.Info("Pass case 3")
	case task := <-broker.taskChan[disp.scanWorkerIndex]:
		log.Info("trigger a new scan task", zap.Any("task", task.id.String()), zap.Any("resolvedTs", task.eventStoreResolvedTs.Load()), zap.Any("eventStoreCommitTs", task.eventStoreCommitTs.Load()), zap.Any("isTaskScanning", task.isTaskScanning.Load()))
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
	broker, _, _ := newEventBrokerForTest()
	defer broker.close()

	dispInfo := newMockDispatcherInfoForTest(t)
	// Case 1: Add and get a dispatcher.
	broker.addDispatcher(dispInfo)
	disp := broker.getDispatcher(dispInfo.GetID())
	require.NotNil(t, disp)
	require.Equal(t, disp.id, dispInfo.GetID())

	// Case 2: Reset a dispatcher.
	dispInfo.startTs = 1002
	broker.resetDispatcher(dispInfo)
	disp = broker.getDispatcher(dispInfo.GetID())
	require.NotNil(t, disp)
	require.Equal(t, disp.id, dispInfo.GetID())
	// Check the resetTs is updated.
	require.Equal(t, disp.resetTs.Load(), dispInfo.GetStartTs())

	// Case 3: Pause a dispatcher.
	broker.pauseDispatcher(dispInfo)
	disp = broker.getDispatcher(dispInfo.GetID())
	require.NotNil(t, disp)
	require.False(t, disp.isReadyReceivingData.Load())

	// Case 4: Resume a dispatcher.
	broker.resumeDispatcher(dispInfo)
	disp = broker.getDispatcher(dispInfo.GetID())
	require.NotNil(t, disp)
	require.True(t, disp.isReadyReceivingData.Load())

	// Case 5: Remove a dispatcher.
	broker.removeDispatcher(dispInfo)
	disp = broker.getDispatcher(dispInfo.GetID())
	require.Nil(t, disp)
}

func TestHandleResolvedTs(t *testing.T) {
	broker, _, _ := newEventBrokerForTest()
	defer broker.close()

	dispInfo := newMockDispatcherInfoForTest(t)
	broker.addDispatcher(dispInfo)
	disp := broker.getDispatcher(dispInfo.GetID())
	require.NotNil(t, disp)
	require.Equal(t, disp.id, dispInfo.GetID())

	mc := broker.msgSender.(*mockMessageCenter)

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

	msg := <-mc.messageCh
	require.Equal(t, msg.Type, messaging.TypeBatchResolvedTs)
}

// func TestRateLimiter(t *testing.T) {
// 	broker, _, _ := newEventBrokerForTest()
// 	defer broker.close()

// 	dispInfo := newMockDispatcherInfoForTest(t)
// 	changefeedStatus := broker.getOrSetChangefeedStatus(dispInfo.GetChangefeedID())

// 	// Create a dispatcher with known scan limit
// 	disp := newDispatcherStat(100, dispInfo, nil, 0, 0, changefeedStatus)
// 	disp.resetState(100)
// 	disp.isHandshaked.Store(true)
// 	disp.isReadyReceivingData.Store(true)
// 	disp.eventStoreResolvedTs.Store(200)
// 	disp.eventStoreCommitTs.Store(200)

// 	// Test Case 1: Normal operation - should allow within burst capacity
// 	// Reset the scan limit to minimum value (1MB)
// 	disp.resetScanLimit()
// 	scanLimit := disp.getCurrentScanLimitInBytes()
// 	require.Equal(t, int64(minScanLimitInBytes), scanLimit)

// 	// This should succeed - within burst capacity
// 	allowed := broker.scanRateLimiter.AllowN(time.Now(), int(scanLimit))
// 	require.True(t, allowed, "Rate limiter should allow request within burst capacity")

// 	// Test Case 2: Burst capacity limit - should reject request larger than burst
// 	// Try to consume more than the burst capacity (maxScanLimitInBytes)
// 	oversizedRequest := int(maxScanLimitInBytes + 1)
// 	allowed = broker.scanRateLimiter.AllowN(time.Now(), oversizedRequest)
// 	require.False(t, allowed, "Rate limiter should reject request larger than burst capacity")

// 	// Test Case 3: Multiple requests within burst capacity
// 	// After some time, we should be able to make another request
// 	time.Sleep(100 * time.Millisecond) // Allow some tokens to be replenished
// 	allowed = broker.scanRateLimiter.AllowN(time.Now(), int(scanLimit))
// 	require.True(t, allowed, "Rate limiter should allow another request after token replenishment")

// 	// Test Case 4: Test doScan with rate limiter integration
// 	// Mock the getScanTaskDataRange to return our test data range
// 	disp.sentResolvedTs.Store(99) // Set to less than eventStoreResolvedTs to trigger scan

// 	// Test that doScan respects rate limiter
// 	// First, exhaust the rate limiter
// 	broker.scanRateLimiter.AllowN(time.Now(), int(maxScanLimitInBytes))

// 	// Now doScan should return early due to rate limiter
// 	ctx := context.Background()
// 	// This should return quickly without doing actual scan due to rate limiter
// 	broker.doScan(ctx, disp)
// 	// Since we can't directly check internal state, we verify the scan wasn't blocked
// 	require.False(t, disp.isTaskScanning.Load(), "Task should not be scanning due to rate limiter")

// 	// Test Case 5: Test rate limiter configuration
// 	// Verify that the rate limiter is configured correctly
// 	require.NotNil(t, broker.scanRateLimiter, "Rate limiter should be initialized")

// 	// Test rate limiter limits
// 	rateLimiter := broker.scanRateLimiter
// 	require.Equal(t, float64(maxScanLimitInBytesPerSecond), float64(rateLimiter.Limit()), "Rate limiter should have correct rate limit")

// 	// Test burst capacity by trying to consume exactly the burst amount
// 	// Wait for tokens to be replenished
// 	time.Sleep(200 * time.Millisecond)
// 	burstAmount := int(maxScanLimitInBytes)
// 	allowed = rateLimiter.AllowN(time.Now(), burstAmount)
// 	require.True(t, allowed, "Rate limiter should allow request exactly equal to burst capacity")

// 	// Test Case 6: Test with dispatcher's getCurrentScanLimitInBytes
// 	// Reset scan limit and test integration
// 	disp.resetScanLimit()
// 	currentLimit := disp.getCurrentScanLimitInBytes()
// 	require.Equal(t, int64(minScanLimitInBytes), currentLimit)

// 	// This should work with rate limiter
// 	time.Sleep(100 * time.Millisecond) // Allow token replenishment
// 	allowed = broker.scanRateLimiter.AllowN(time.Now(), int(currentLimit))
// 	require.True(t, allowed, "Rate limiter should work with dispatcher's current scan limit")

// 	// Test Case 7: Test scan limit growth over time
// 	// Wait for scan limit to potentially grow
// 	time.Sleep(updateScanLimitInterval + 100*time.Millisecond)
// 	newLimit := disp.getCurrentScanLimitInBytes()
// 	// Should either stay the same or grow (depending on timing)
// 	require.True(t, newLimit >= currentLimit, "Scan limit should not decrease")
// 	require.True(t, newLimit <= maxScanLimitInBytes, "Scan limit should not exceed maximum")

// 	log.Info("All rate limiter tests passed")
// }
