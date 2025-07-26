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
	return newMockDispatcherInfo(t, did, 100, eventpb.ActionType_ACTION_TYPE_REGISTER)
}

func TestCheckNeedScan(t *testing.T) {
	broker, _, _ := newEventBrokerForTest()
	// Close the broker, so we can catch all message in the test.
	broker.close()

	disInfo := newMockDispatcherInfoForTest(t)
	disInfo.startTs = 100
	err := broker.addDispatcher(disInfo)
	require.NoError(t, err)

	dispStatus, ok := broker.getDispatcher(disInfo.GetID())
	require.True(t, ok)
	require.Equal(t, dispStatus.id, disInfo.GetID())

	// Set the eventStoreResolvedTs and latestCommitTs to 102 and 101.
	// To simulate the eventStore has just notified the broker.
	dispStatus.eventStoreResolvedTs.Store(102)
	dispStatus.latestCommitTs.Store(101)

	// Case 1: Is scanning, and mustCheck is false, it should return false.
	dispStatus.isTaskScanning.Store(true)
	needScan := broker.scanReady(dispStatus)
	require.False(t, needScan)
	log.Info("Pass case 1")

	// Case 2: ResetTs is 0, it should return false.
	// And the broker will send a ready event.
	dispStatus.isTaskScanning.Store(false)
	needScan = broker.scanReady(dispStatus)
	require.False(t, needScan)
	e := <-broker.messageCh[dispStatus.messageWorkerIndex]

	require.Equal(t, event.TypeReadyEvent, e.msgType)
	log.Info("Pass case 2")

	// Case 3: ResetTs is not 0, it should return true.
	// And we can get a scan task.
	// And the task.scanning should be true.
	// And the broker will send a handshake event.
	dispStatus.resetTs.Store(100)
	needScan = broker.scanReady(dispStatus)
	require.True(t, needScan)
	e = <-broker.messageCh[dispStatus.messageWorkerIndex]
	require.Equal(t, event.TypeHandshakeEvent, e.msgType)
	log.Info("Pass case 3")

	// Case 4: The task.isRunning is false, it should return false.
	dispStatus.isReadyRecevingData.Store(false)
	needScan = broker.scanReady(dispStatus)
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

	dispStatus, ok := broker.getDispatcher(disInfo.GetID())
	require.True(t, ok)
	require.Equal(t, dispStatus.id, disInfo.GetID())

	broker.resetDispatcher(disInfo)

	dispStatus.resetState(100)
	dispStatus.isHandshaked.Store(true)

	// Case 1: The resolvedTs is less than the startTs, it should not happen.
	require.Panics(t, func() { broker.onNotify(dispStatus, 1, 1) })
	log.Info("Pass case 1")

	// Case 2: The resolvedTs is greater than the startTs, it should be updated.
	broker.onNotify(dispStatus, 101, 1)
	require.Equal(t, uint64(101), dispStatus.eventStoreResolvedTs.Load())
	log.Info("Pass case 2")

	// Case 3: The latestCommitTs is greater than the startTs, it triggers a scan task.
	broker.onNotify(dispStatus, 102, 101)
	require.Equal(t, uint64(102), dispStatus.eventStoreResolvedTs.Load())
	require.True(t, dispStatus.isTaskScanning.Load())
	task := <-broker.taskChan[dispStatus.scanWorkerIndex]
	require.Equal(t, task.id, dispStatus.id)
	log.Info("Pass case 3")

	// Case 3: When the scan task is running, even there is a larger resolvedTs,
	// should not trigger a new scan task.
	broker.onNotify(dispStatus, 103, 101)
	require.Equal(t, uint64(103), dispStatus.eventStoreResolvedTs.Load())
	after := time.After(50 * time.Millisecond)
	select {
	case <-after:
		log.Info("Pass case 4")
	case task := <-broker.taskChan[dispStatus.scanWorkerIndex]:
		log.Info("trigger a new scan task", zap.Any("task", task.id.String()), zap.Any("resolvedTs", task.eventStoreResolvedTs.Load()), zap.Any("latestCommitTs", task.latestCommitTs.Load()), zap.Any("isTaskScanning", task.isTaskScanning.Load()))
		require.Fail(t, "should not trigger a new scan task")
	}

	ctx := context.Background()
	// Case 5: Do scan, it will update the sentResolvedTs.
	broker.doScan(ctx, task)
	require.False(t, dispStatus.isTaskScanning.Load())
	require.Equal(t, uint64(103), dispStatus.sentResolvedTs.Load())
	log.Info("pass case 5")

	// Set the schemaStore's maxDDLCommitTs to the sentResolvedTs, so the broker will not scan the schemaStore.
	ss.maxDDLCommitTs = dispStatus.sentResolvedTs.Load()
	broker.onNotify(dispStatus, 104, 101)
	broker.doScan(ctx, task)
	require.Equal(t, uint64(104), dispStatus.sentResolvedTs.Load())
	log.Info("Pass case 6")
}

func TestCURDDispatcher(t *testing.T) {
	broker, _, _ := newEventBrokerForTest()
	defer broker.close()

	dispInfo := newMockDispatcherInfoForTest(t)
	dispInfo.startTs = 1002
	// Case 1: Add and get a dispatcher.
	err := broker.addDispatcher(dispInfo)
	require.NoError(t, err)

	disp, ok := broker.getDispatcher(dispInfo.GetID())
	require.True(t, ok)
	require.Equal(t, disp.id, dispInfo.GetID())

	// Case 2: Reset a dispatcher.
	broker.resetDispatcher(dispInfo)
	disp, ok = broker.getDispatcher(dispInfo.GetID())
	require.True(t, ok)
	require.Equal(t, disp.id, dispInfo.GetID())
	// Check the resetTs is updated.
	require.Equal(t, disp.resetTs.Load(), dispInfo.GetStartTs())

	// Case 3: Pause a dispatcher.
	broker.pauseDispatcher(dispInfo)
	disp, ok = broker.getDispatcher(dispInfo.GetID())
	require.True(t, ok)
	require.False(t, disp.isReadyRecevingData.Load())

	// Case 4: Resume a dispatcher.
	broker.resumeDispatcher(dispInfo)
	disp, ok = broker.getDispatcher(dispInfo.GetID())
	require.True(t, ok)
	require.True(t, disp.isReadyRecevingData.Load())

	// Case 5: Remove a dispatcher.
	broker.removeDispatcher(dispInfo)
	disp, ok = broker.getDispatcher(dispInfo.GetID())
	require.False(t, ok)
	require.Nil(t, disp)
}

func TestHandleResolvedTs(t *testing.T) {
	broker, _, _ := newEventBrokerForTest()
	defer broker.close()

	dispInfo := newMockDispatcherInfoForTest(t)
	err := broker.addDispatcher(dispInfo)
	require.NoError(t, err)

	dispStatus, ok := broker.getDispatcher(dispInfo.GetID())
	require.True(t, ok)
	require.Equal(t, dispStatus.id, dispInfo.GetID())

	mc := broker.msgSender.(*mockMessageCenter)

	ctx := context.Background()
	cacheMap := make(map[node.ID]*resolvedTsCache)
	resolvedTsEvent := &wrapEvent{
		serverID:        "test",
		resolvedTsEvent: event.NewResolvedEvent(100, dispInfo.GetID(), 0),
	}
	// handle resolvedTsCacheSize resolvedTs events, so the cache is full.
	for i := 0; i < resolvedTsCacheSize+1; i++ {
		broker.handleResolvedTs(ctx, cacheMap, resolvedTsEvent, dispStatus.messageWorkerIndex)
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
// 	disp.isReadyRecevingData.Store(true)
// 	disp.eventStoreResolvedTs.Store(200)
// 	disp.latestCommitTs.Store(200)

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
