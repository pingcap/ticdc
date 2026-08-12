// Copyright 2026 PingCAP, Inc.
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
	"sync"
	stdatomic "sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
	uberatomic "go.uber.org/atomic"
)

func newScanAdmissionDispatcherForTest(t *testing.T, controller *scanAdmissionController) *dispatcherStat {
	t.Helper()
	info := newMockDispatcherInfoForTest(t)
	info.epoch = 1
	status := newChangefeedStatus(info.GetChangefeedID(), info.GetSyncPointInterval())
	status.scanAdmission = controller
	dispatcher := newDispatcherStat(info, 1, 1, nil, status)
	dispatcher.availableMemoryQuota.Store(2 * minScanLimitInBytes)
	return dispatcher
}

func TestScanAdmissionGrantReservesAndRefundsBothQuotas(t *testing.T) {
	controller := newScanAdmissionController()
	dispatcher := newScanAdmissionDispatcherForTest(t, controller)
	serverID := node.ID(dispatcher.info.GetServerID())
	controller.updateAvailableMemory(serverID, 2*minScanLimitInBytes)

	grant, reason := controller.tryGrant(dispatcher, minScanLimitInBytes, time.Now())
	require.Equal(t, scanAdmissionGranted, reason)
	available, ok := controller.availableMemory(serverID)
	require.True(t, ok)
	require.Equal(t, uint64(minScanLimitInBytes), available)
	require.Equal(t, uint64(minScanLimitInBytes), dispatcher.availableMemoryQuota.Load())

	grant.releaseAll()
	available, ok = controller.availableMemory(serverID)
	require.True(t, ok)
	require.Equal(t, uint64(2*minScanLimitInBytes), available)
	require.Equal(t, uint64(2*minScanLimitInBytes), dispatcher.availableMemoryQuota.Load())
}

func TestScanAdmissionFailureLeavesBothQuotasUnchanged(t *testing.T) {
	controller := newScanAdmissionController()
	dispatcher := newScanAdmissionDispatcherForTest(t, controller)
	serverID := node.ID(dispatcher.info.GetServerID())
	controller.updateAvailableMemory(serverID, minScanLimitInBytes-1)

	_, reason := controller.tryGrant(dispatcher, minScanLimitInBytes, time.Now())
	require.Equal(t, scanAdmissionChangefeedQuota, reason)
	available, ok := controller.availableMemory(serverID)
	require.True(t, ok)
	require.Equal(t, uint64(minScanLimitInBytes-1), available)
	require.Equal(t, uint64(2*minScanLimitInBytes), dispatcher.availableMemoryQuota.Load())

	controller.updateAvailableMemory(serverID, 2*minScanLimitInBytes)
	dispatcher.availableMemoryQuota.Store(minScanLimitInBytes - 1)
	_, reason = controller.tryGrant(dispatcher, minScanLimitInBytes, time.Now())
	require.Equal(t, scanAdmissionDispatcherQuota, reason)
	available, ok = controller.availableMemory(serverID)
	require.True(t, ok)
	require.Equal(t, uint64(2*minScanLimitInBytes), available)
	require.Equal(t, uint64(minScanLimitInBytes-1), dispatcher.availableMemoryQuota.Load())
}

func TestScanAdmissionProtectedWaiterCanUseReserve(t *testing.T) {
	controller := newScanAdmissionController()
	dispatcher := newScanAdmissionDispatcherForTest(t, controller)
	serverID := node.ID(dispatcher.info.GetServerID())
	controller.updateAvailableMemory(serverID, minScanLimitInBytes)
	controller.setProtectedReserve(serverID, minScanLimitInBytes)

	_, reason := controller.tryGrant(dispatcher, minScanLimitInBytes, time.Now())
	require.Equal(t, scanAdmissionProtectedReserve, reason)

	now := time.Now()
	dispatcher.scanAdmissionWaitingSince.Store(now.Add(-scanAdmissionWaitThreshold).UnixNano())
	grant, reason := controller.tryGrant(dispatcher, minScanLimitInBytes, now)
	require.Equal(t, scanAdmissionGranted, reason)
	grant.releaseAll()
}

func TestScanAdmissionPendingSyncpointIsProtected(t *testing.T) {
	controller := newScanAdmissionController()
	dispatcher := newScanAdmissionDispatcherForTest(t, controller)
	dispatcher.enableSyncPoint = true
	dispatcher.nextSyncPoint.Store(100)
	dispatcher.receivedResolvedTs.Store(101)
	serverID := node.ID(dispatcher.info.GetServerID())
	controller.updateAvailableMemory(serverID, minScanLimitInBytes)
	controller.setProtectedReserve(serverID, minScanLimitInBytes)

	grant, reason := controller.tryGrant(dispatcher, minScanLimitInBytes, time.Now())
	require.Equal(t, scanAdmissionGranted, reason)
	grant.releaseAll()
}

func TestScanAdmissionConcurrentGrantsDoNotExceedQuota(t *testing.T) {
	controller := newScanAdmissionController()
	dispatcher := newScanAdmissionDispatcherForTest(t, controller)
	serverID := node.ID(dispatcher.info.GetServerID())
	const grantCount = uint64(8)
	controller.updateAvailableMemory(serverID, grantCount*minScanLimitInBytes)
	dispatcher.availableMemoryQuota.Store(grantCount * minScanLimitInBytes)

	var successful stdatomic.Uint64
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			_, reason := controller.tryGrant(dispatcher, minScanLimitInBytes, time.Now())
			if reason == scanAdmissionGranted {
				successful.Add(1)
			}
		})
	}
	wg.Wait()

	require.Equal(t, grantCount, successful.Load())
	available, ok := controller.availableMemory(serverID)
	require.True(t, ok)
	require.Zero(t, available)
	require.Zero(t, dispatcher.availableMemoryQuota.Load())
}

func TestScanAdmissionSchedulerSelectsOneOldestProtectedWaiterPerNode(t *testing.T) {
	broker, _, _, _ := newEventBrokerForTest()
	broker.close()

	firstInfo := newMockDispatcherInfoForTest(t)
	firstInfo.epoch = 1
	secondInfo := newMockDispatcherInfoForTest(t)
	secondInfo.epoch = 1
	status := newChangefeedStatus(firstInfo.GetChangefeedID(), 0)

	newWaitingDispatcher := func(info *mockDispatcherInfo, waitingSince time.Time) *dispatcherStat {
		dispatcher := newDispatcherStat(
			info, uint64(len(broker.taskChan)), uint64(len(broker.messageCh)), nil, status)
		dispatcher.seq.Store(1)
		dispatcher.availableMemoryQuota.Store(2 * maxScanLimitInBytes)
		dispatcher.scanAdmissionWaitingSince.Store(waitingSince.UnixNano())
		pointer := &uberatomic.Pointer[dispatcherStat]{}
		pointer.Store(dispatcher)
		status.addDispatcher(dispatcher.id, pointer)
		return dispatcher
	}

	now := time.Now()
	first := newWaitingDispatcher(firstInfo, now.Add(-2*scanAdmissionWaitThreshold))
	second := newWaitingDispatcher(secondInfo, now.Add(-scanAdmissionWaitThreshold))
	serverID := node.ID(first.info.GetServerID())
	status.scanAdmission.updateAvailableMemory(serverID, 2*maxScanLimitInBytes)

	broker.scheduleChangefeedWaitingDispatchers(status, now)

	require.True(t, first.isTaskScanning.Load())
	require.False(t, second.isTaskScanning.Load())
	task := <-broker.taskChan[first.scanWorkerIndex]
	require.Same(t, first, task.dispatcherStat)
	require.Equal(t, uint64(maxScanLimitInBytes), task.grant.bytes)
	broker.releasePreparedScanTask(task)

	broker.scheduleChangefeedWaitingDispatchers(status, now.Add(scanAdmissionSweepInterval))
	require.False(t, first.isTaskScanning.Load())
	require.True(t, second.isTaskScanning.Load())
	task = <-broker.taskChan[second.scanWorkerIndex]
	require.Same(t, second, task.dispatcherStat)
	broker.releasePreparedScanTask(task)
}

func BenchmarkScanAdmissionSweepOneMillionDispatchers(b *testing.B) {
	const dispatcherCount = 1_000_000
	now := time.Now()
	info := newMockDispatcherInfo(nil, 1, common.NewDispatcherID(), 1,
		eventpb.ActionType_ACTION_TYPE_REGISTER)
	info.epoch = 1
	status := newChangefeedStatus(info.GetChangefeedID(), 0)
	serverID := node.ID(info.GetServerID())
	status.scanAdmission.updateAvailableMemory(serverID, 2*maxScanLimitInBytes)

	for i := range dispatcherCount {
		dispatcher := &dispatcherStat{
			id:             common.DispatcherID{Low: uint64(i + 1)},
			changefeedStat: status,
			info:           info,
		}
		dispatcher.seq.Store(1)
		dispatcher.availableMemoryQuota.Store(2 * maxScanLimitInBytes)
		dispatcher.scanAdmissionWaitingSince.Store(
			now.Add(-scanAdmissionWaitThreshold).UnixNano())
		pointer := &uberatomic.Pointer[dispatcherStat]{}
		pointer.Store(dispatcher)
		status.addDispatcher(dispatcher.id, pointer)
	}

	broker := &eventBroker{
		taskChan:         []chan scanTask{make(chan scanTask, 1)},
		scanLimitInBytes: 256 * 1024 * 1024,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		broker.scheduleChangefeedWaitingDispatchers(status, time.Now())
		task := <-broker.taskChan[0]
		broker.releasePreparedScanTask(task)
	}
}
