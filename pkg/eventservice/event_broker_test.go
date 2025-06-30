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

	// Case 4: When the scan task is running, even there is a larger resolvedTs,
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
		resolvedTsEvent: event.NewResolvedEvent(100, dispInfo.GetID()),
	}
	// handle resolvedTsCacheSize resolvedTs events, so the cache is full.
	for i := 0; i < resolvedTsCacheSize+1; i++ {
		broker.handleResolvedTs(ctx, cacheMap, resolvedTsEvent, dispStatus.messageWorkerIndex)
	}

	msg := <-mc.messageCh
	require.Equal(t, msg.Type, messaging.TypeBatchResolvedTs)
}
