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

package eventcollector

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/routing"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

var mockChangefeedID = common.NewChangeFeedIDWithName("dispatcher_stat_test", common.DefaultKeyspaceName)

// mockDispatcher implements the dispatcher.EventDispatcher interface for testing
type mockDispatcher struct {
	dispatcher.EventDispatcher
	startTs      uint64
	id           common.DispatcherID
	changefeedID common.ChangeFeedID
	handleEvents func(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool)
	events       []dispatcher.DispatcherEvent
	checkPointTs uint64

	skipSyncpointAtStartTs bool
}

func newMockDispatcher(id common.DispatcherID, startTs uint64) *mockDispatcher {
	return &mockDispatcher{
		id:           id,
		startTs:      startTs,
		changefeedID: mockChangefeedID,
		checkPointTs: startTs,
	}
}

func newDispatcherStatForTest(target dispatcher.DispatcherService, readyCallback func()) *dispatcherStat {
	return newDispatcherStatInternal(
		target,
		nil,
		"",
		func(*messaging.TargetMessage) {},
		readyCallback,
	)
}

func (m *mockDispatcher) GetStartTs() uint64 {
	return m.startTs
}

func (m *mockDispatcher) GetMode() int64 {
	return common.DefaultMode
}

func (m *mockDispatcher) GetId() common.DispatcherID {
	return m.id
}

func (m *mockDispatcher) GetChangefeedID() common.ChangeFeedID {
	return m.changefeedID
}

func (m *mockDispatcher) GetEventCollectorBatchConfig() (batchCount int, batchBytes int) {
	return 0, 0
}

func (m *mockDispatcher) GetTableSpan() *heartbeatpb.TableSpan {
	return &heartbeatpb.TableSpan{
		TableID: 1,
	}
}

func (m *mockDispatcher) GetRouter() routing.Router {
	return routing.Router{}
}

func (m *mockDispatcher) GetBDRMode() bool {
	return false
}

func (m *mockDispatcher) GetFilterConfig() *eventpb.FilterConfig {
	return &eventpb.FilterConfig{}
}

func (m *mockDispatcher) EnableSyncPoint() bool {
	return false
}

func (m *mockDispatcher) GetSyncPointInterval() time.Duration {
	return time.Second * 10
}

func (m *mockDispatcher) GetSkipSyncpointAtStartTs() bool {
	return m.skipSyncpointAtStartTs
}

func (m *mockDispatcher) GetResolvedTs() uint64 {
	return m.startTs
}

func (m *mockDispatcher) GetCheckpointTs() uint64 {
	return m.checkPointTs
}

func (m *mockDispatcher) GetTxnAtomicity() config.AtomicityLevel {
	return config.DefaultAtomicityLevel()
}

func (m *mockDispatcher) HandleEvents(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool) {
	if m.handleEvents == nil {
		return false
	}
	m.events = append(m.events, events...)
	m.checkPointTs = m.events[len(m.events)-1].GetCommitTs()
	return m.handleEvents(m.events, wakeCallback)
}

func (m *mockDispatcher) GetTimezone() string {
	return "UTC"
}

func (m *mockDispatcher) GetIntegrityConfig() *eventpb.IntegrityConfig {
	return &eventpb.IntegrityConfig{}
}

func (m *mockDispatcher) IsOutputRawChangeEvent() bool {
	return false
}

// mockEvent implements the Event interface for testing
type mockEvent struct {
	eventType    int
	seq          uint64
	dispatcherID common.DispatcherID
	commitTs     common.Ts
	startTs      common.Ts
	size         int64
	isPaused     bool
	len          int32
	epoch        uint64
}

func (m *mockEvent) GetType() int {
	return m.eventType
}

func (m *mockEvent) GetSeq() uint64 {
	return m.seq
}

func (m *mockEvent) GetEpoch() uint64 {
	return m.epoch
}

func (m *mockEvent) GetDispatcherID() common.DispatcherID {
	return m.dispatcherID
}

func (m *mockEvent) GetCommitTs() common.Ts {
	return m.commitTs
}

func (m *mockEvent) GetStartTs() common.Ts {
	return m.startTs
}

func (m *mockEvent) GetSize() int64 {
	return m.size
}

func (m *mockEvent) IsPaused() bool {
	return m.isPaused
}

func (m *mockEvent) Len() int32 {
	return m.len
}

// newTestEventCollector creates an EventCollector instance for testing
func newTestEventCollector(localServerID node.ID) *EventCollector {
	mc := messaging.NewMessageCenter(context.TODO(), localServerID, config.NewDefaultMessageCenterConfig("127.0.0.1:18300"), nil)
	appcontext.SetService(appcontext.MessageCenter, mc)
	return New(localServerID)
}

func TestVerifyEventSequence(t *testing.T) {
	tests := []struct {
		name           string
		lastEventSeq   uint64
		event          dispatcher.DispatcherEvent
		expectedResult bool
	}{
		{
			name:         "first event is handshake",
			lastEventSeq: 0,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeHandshakeEvent,
					seq:       1,
				},
			},
			expectedResult: true,
		},
		{
			name:         "first event is not handshake",
			lastEventSeq: 0,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeDMLEvent,
					seq:       1,
				},
			},
			expectedResult: false,
		},
		{
			name:         "continuous DML event sequence",
			lastEventSeq: 1,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeDMLEvent,
					seq:       2,
				},
			},
			expectedResult: true,
		},
		{
			name:         "discontinuous DML event sequence",
			lastEventSeq: 1,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeDMLEvent,
					seq:       3,
				},
			},
			expectedResult: false,
		},
		{
			name:         "continuous DDL event sequence",
			lastEventSeq: 2,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeDDLEvent,
					seq:       3,
				},
			},
			expectedResult: true,
		},
		{
			name:         "discontinuous DDL event sequence",
			lastEventSeq: 2,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeDDLEvent,
					seq:       4,
				},
			},
			expectedResult: false,
		},
		{
			name:         "continuous batch DML event sequence",
			lastEventSeq: 3,
			event: dispatcher.DispatcherEvent{
				Event: &commonEvent.BatchDMLEvent{
					DMLEvents: []*commonEvent.DMLEvent{
						{Seq: 4},
						{Seq: 5},
					},
				},
			},
			expectedResult: true,
		},
		{
			name:         "discontinuous batch DML event sequence",
			lastEventSeq: 3,
			event: dispatcher.DispatcherEvent{
				Event: &commonEvent.BatchDMLEvent{
					DMLEvents: []*commonEvent.DMLEvent{
						{Seq: 5},
						{Seq: 6},
					},
				},
			},
			expectedResult: false,
		},
		{
			name:         "discontinuous sync point event sequence",
			lastEventSeq: 3,
			event: dispatcher.DispatcherEvent{
				Event: &commonEvent.SyncPointEvent{
					CommitTs: 100,
					Seq:      5,
				},
			},
			expectedResult: false,
		},
		{
			name:         "continuous resolved ts event sequence",
			lastEventSeq: 4,
			event: dispatcher.DispatcherEvent{
				Event: &commonEvent.ResolvedEvent{
					DispatcherID: common.NewDispatcherID(),
					ResolvedTs:   100,
					Version:      1,
					Epoch:        1,
					Seq:          4, // ResolvedEvent seq should equal lastEventSeq
				},
			},
			expectedResult: true,
		},
		{
			name:         "discontinuous resolved ts event sequence",
			lastEventSeq: 4,
			event: dispatcher.DispatcherEvent{
				Event: &commonEvent.ResolvedEvent{
					DispatcherID: common.NewDispatcherID(),
					ResolvedTs:   100,
					Version:      1,
					Epoch:        1,
					Seq:          3, // ResolvedEvent seq should equal lastEventSeq, but it's 3 instead of 4
				},
			},
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stat := newDispatcherStatForTest(newMockDispatcher(common.NewDispatcherID(), 0), nil)
			state := stat.loadCurrentEpochState()
			state.lastEventSeq.Store(tt.lastEventSeq)
			result := stat.verifyEventSequence(tt.event, state)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestFilterAndUpdateEventByCommitTs(t *testing.T) {
	tests := []struct {
		name              string
		lastEventCommitTs uint64
		gotDDLOnTs        bool
		gotSyncpointOnTS  bool
		event             dispatcher.DispatcherEvent
		expectedResult    bool
		expectedDDLOnTs   bool
		expectedSyncOnTs  bool
		expectedCommitTs  uint64
	}{
		{
			name:              "event with commit ts less than last commit ts",
			lastEventCommitTs: 100,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeDMLEvent,
					commitTs:  90,
				},
			},
			expectedResult:   false,
			expectedDDLOnTs:  false,
			expectedSyncOnTs: false,
			expectedCommitTs: 100,
		},
		{
			name:              "DDL event with same commit ts and already got DDL",
			lastEventCommitTs: 100,
			gotDDLOnTs:        true,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeDDLEvent,
					commitTs:  100,
				},
			},
			expectedResult:   false,
			expectedDDLOnTs:  true,
			expectedSyncOnTs: false,
			expectedCommitTs: 100,
		},
		{
			name:              "DDL event with same commit ts and not got DDL",
			lastEventCommitTs: 100,
			gotDDLOnTs:        false,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeDDLEvent,
					commitTs:  100,
				},
			},
			expectedResult:   true,
			expectedDDLOnTs:  true,
			expectedSyncOnTs: false,
			expectedCommitTs: 100,
		},
		{
			name:              "SyncPoint event with same commit ts and already got SyncPoint",
			lastEventCommitTs: 101,
			gotSyncpointOnTS:  true,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeSyncPointEvent,
					commitTs:  101,
				},
			},
			expectedResult:   false,
			expectedDDLOnTs:  false,
			expectedSyncOnTs: true,
			expectedCommitTs: 101,
		},
		{
			name:              "SyncPoint event with same commit ts and not got SyncPoint",
			lastEventCommitTs: 101,
			gotSyncpointOnTS:  false,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeSyncPointEvent,
					commitTs:  101,
				},
			},
			expectedResult:   true,
			expectedDDLOnTs:  false,
			expectedSyncOnTs: true,
			expectedCommitTs: 101,
		},

		{
			name:              "DML event with larger commit ts",
			lastEventCommitTs: 100,
			gotDDLOnTs:        true,
			gotSyncpointOnTS:  true,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeDMLEvent,
					commitTs:  110,
				},
			},
			expectedResult:   true,
			expectedDDLOnTs:  false,
			expectedSyncOnTs: false,
			expectedCommitTs: 110,
		},
		{
			name:              "BatchDML event with larger commit ts",
			lastEventCommitTs: 100,
			gotDDLOnTs:        true,
			gotSyncpointOnTS:  true,
			event: dispatcher.DispatcherEvent{
				Event: &commonEvent.BatchDMLEvent{
					DMLEvents: []*commonEvent.DMLEvent{
						{CommitTs: 110},
						{CommitTs: 110},
					},
				},
			},
			expectedResult:   true,
			expectedDDLOnTs:  false,
			expectedSyncOnTs: false,
			expectedCommitTs: 110,
		},
		{
			name:              "Resolved event with larger commit ts",
			lastEventCommitTs: 100,
			gotDDLOnTs:        true,
			gotSyncpointOnTS:  true,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeResolvedEvent,
					commitTs:  110,
				},
			},
			expectedResult:   true,
			expectedDDLOnTs:  false,
			expectedSyncOnTs: false,
			expectedCommitTs: 100, // Resolved event should not update lastEventCommitTs
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stat := newDispatcherStatForTest(newMockDispatcher(common.NewDispatcherID(), 0), nil)
			stat.lastEventCommitTs.Store(tt.lastEventCommitTs)
			stat.gotDDLOnTs.Store(tt.gotDDLOnTs)
			stat.gotSyncpointOnTS.Store(tt.gotSyncpointOnTS)

			state := stat.loadCurrentEpochState()
			result := stat.shouldForwardEventByCommitTs(tt.event)
			if result {
				stat.updateCommitTsStateByEvents(state, []dispatcher.DispatcherEvent{tt.event})
			}
			require.Equal(t, tt.expectedResult, result)
			require.Equal(t, tt.expectedDDLOnTs, stat.gotDDLOnTs.Load())
			require.Equal(t, tt.expectedSyncOnTs, stat.gotSyncpointOnTS.Load())
			require.Equal(t, tt.expectedCommitTs, stat.lastEventCommitTs.Load())
		})
	}
}

func TestUpdateCommitTsStateByEvents(t *testing.T) {
	t.Parallel()

	stat := newDispatcherStatForTest(newMockDispatcher(common.NewDispatcherID(), 0), nil)
	stat.lastEventCommitTs.Store(100)
	stat.gotDDLOnTs.Store(true)
	stat.gotSyncpointOnTS.Store(true)
	state := stat.loadCurrentEpochState()
	state.maxEventTs.Store(100)

	stat.updateCommitTsStateByEvents(state, []dispatcher.DispatcherEvent{
		{
			Event: &mockEvent{
				eventType: commonEvent.TypeResolvedEvent,
				commitTs:  105,
			},
		},
		{
			Event: &mockEvent{
				eventType: commonEvent.TypeDMLEvent,
				commitTs:  110,
			},
		},
	})

	require.Equal(t, uint64(110), stat.lastEventCommitTs.Load())
	require.False(t, stat.gotDDLOnTs.Load())
	require.False(t, stat.gotSyncpointOnTS.Load())
	require.Equal(t, uint64(110), state.maxEventTs.Load())
}

func TestHandleSignalEvent(t *testing.T) {
	localServerID := node.ID("local-server")
	remoteServerID := node.ID("remote-server")
	anotherRemoteServerID := node.ID("another-remote-server")

	tests := []struct {
		name                        string
		event                       dispatcher.DispatcherEvent
		initialState                func(*dispatcherStat)
		expectedEventServiceID      node.ID
		expectedReceivingData       bool
		expectedAwaitingLocalReady  bool
		expectedPendingRemoteTarget node.ID
		expectedPanic               bool
	}{
		{
			name: "ignore signal event when already connected to local server",
			event: dispatcher.DispatcherEvent{
				From: &localServerID,
				Event: &mockEvent{
					eventType: commonEvent.TypeReadyEvent,
				},
			},
			initialState: func(stat *dispatcherStat) {
				setSessionState(stat.session, localServerID, false, "")
			},
			expectedEventServiceID:      localServerID,
			expectedReceivingData:       true,
			expectedAwaitingLocalReady:  false,
			expectedPendingRemoteTarget: "",
		},
		{
			name: "ignore signal event from unknown server",
			event: dispatcher.DispatcherEvent{
				From: &anotherRemoteServerID,
				Event: &mockEvent{
					eventType: commonEvent.TypeReadyEvent,
				},
			},
			initialState: func(stat *dispatcherStat) {
				setSessionState(stat.session, "", true, remoteServerID)
			},
			expectedEventServiceID:      "",
			expectedReceivingData:       false,
			expectedAwaitingLocalReady:  true,
			expectedPendingRemoteTarget: remoteServerID,
		},
		{
			name: "handle ready event from local server with callback",
			event: dispatcher.DispatcherEvent{
				From: &localServerID,
				Event: &mockEvent{
					eventType: commonEvent.TypeReadyEvent,
				},
			},
			initialState: func(stat *dispatcherStat) {
				markSessionRegistering(stat.session, localServerID)
				setSessionReadyCallback(stat.session, func() {})
			},
			expectedEventServiceID:      localServerID,
			expectedReceivingData:       true,
			expectedAwaitingLocalReady:  false,
			expectedPendingRemoteTarget: "",
		},
		{
			name: "handle ready event from local server without callback",
			event: dispatcher.DispatcherEvent{
				From: &localServerID,
				Event: &mockEvent{
					eventType: commonEvent.TypeReadyEvent,
				},
			},
			initialState: func(stat *dispatcherStat) {
				setSessionState(stat.session, "", true, remoteServerID)
			},
			expectedEventServiceID:      localServerID,
			expectedReceivingData:       true,
			expectedAwaitingLocalReady:  false,
			expectedPendingRemoteTarget: "",
		},
		{
			name: "handle ready event from remote server",
			event: dispatcher.DispatcherEvent{
				From: &remoteServerID,
				Event: &mockEvent{
					eventType: commonEvent.TypeReadyEvent,
				},
			},
			initialState: func(stat *dispatcherStat) {
				setSessionState(stat.session, "", true, remoteServerID)
			},
			expectedEventServiceID:      remoteServerID,
			expectedReceivingData:       true,
			expectedAwaitingLocalReady:  true,
			expectedPendingRemoteTarget: "",
		},
		{
			name: "ignore duplicate ready event from remote server",
			event: dispatcher.DispatcherEvent{
				From: &remoteServerID,
				Event: &mockEvent{
					eventType: commonEvent.TypeReadyEvent,
				},
			},
			initialState: func(stat *dispatcherStat) {
				setSessionState(stat.session, remoteServerID, true, "")
			},
			expectedEventServiceID:      remoteServerID,
			expectedReceivingData:       true,
			expectedAwaitingLocalReady:  true,
			expectedPendingRemoteTarget: "",
		},
		{
			name: "handle not reusable event from remote server",
			event: dispatcher.DispatcherEvent{
				From: &remoteServerID,
				Event: &mockEvent{
					eventType: commonEvent.TypeNotReusableEvent,
				},
			},
			initialState: func(stat *dispatcherStat) {
				setSessionState(stat.session, "", true, remoteServerID)
				setSessionRemoteCandidates(stat.session, []string{anotherRemoteServerID.String()})
			},
			expectedEventServiceID:      "",
			expectedReceivingData:       false,
			expectedAwaitingLocalReady:  true,
			expectedPendingRemoteTarget: anotherRemoteServerID,
		},
		{
			name: "panic on not reusable event from local server",
			event: dispatcher.DispatcherEvent{
				From: &localServerID,
				Event: &mockEvent{
					eventType: commonEvent.TypeNotReusableEvent,
				},
			},
			initialState: func(stat *dispatcherStat) {
				markSessionRegistering(stat.session, remoteServerID)
			},
			expectedPanic: true,
		},
		{
			name: "panic on unknown signal event type",
			event: dispatcher.DispatcherEvent{
				From: &localServerID,
				Event: &mockEvent{
					eventType: -1, // Unknown event type
				},
			},
			initialState: func(stat *dispatcherStat) {
				markSessionRegistering(stat.session, remoteServerID)
			},
			expectedPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stat := newDispatcherStat(newMockDispatcher(common.NewDispatcherID(), 0), newTestEventCollector(localServerID), nil)
			if tt.initialState != nil {
				tt.initialState(stat)
			}

			if tt.expectedPanic {
				require.Panics(t, func() {
					stat.handleSignalEvent(tt.event)
				})
				return
			}

			stat.handleSignalEvent(tt.event)
			currentEventServiceID, localReadyPending, pendingRemoteTarget := sessionState(stat.session)
			require.Equal(t, tt.expectedEventServiceID, currentEventServiceID)
			require.Equal(t, tt.expectedReceivingData, stat.session.isReceivingDataEvent())
			require.Equal(t, tt.expectedAwaitingLocalReady, localReadyPending)
			require.Equal(t, tt.expectedPendingRemoteTarget, pendingRemoteTarget)
		})
	}
}

func TestHandleLocalReadyEventCleansUpRemoteRegistrations(t *testing.T) {
	localServerID := node.ID("local-server")
	remoteServerID := node.ID("remote-server")
	anotherRemoteServerID := node.ID("another-remote-server")
	dispatcherID := common.NewDispatcherID()

	newReadyEvent := func(from node.ID) dispatcher.DispatcherEvent {
		return dispatcher.DispatcherEvent{
			From: &from,
			Event: &mockEvent{
				eventType: commonEvent.TypeReadyEvent,
			},
		}
	}

	t.Run("local ready removes pending remote register and resets local", func(t *testing.T) {
		mockDisp := newMockDispatcher(dispatcherID, 0)
		mockEventCollector := newTestEventCollector(localServerID)
		stat := newDispatcherStat(mockDisp, mockEventCollector, nil)
		setSessionState(stat.session, "", true, remoteServerID)

		stat.handleSignalEvent(newReadyEvent(localServerID))

		requireDispatcherRequests(
			t,
			readDispatcherRequests(t, mockEventCollector, 2),
			dispatcherRequestRecord{to: remoteServerID, action: eventpb.ActionType_ACTION_TYPE_REMOVE},
			dispatcherRequestRecord{to: localServerID, action: eventpb.ActionType_ACTION_TYPE_RESET},
		)
		requireNoDispatcherRequest(t, mockEventCollector)
	})

	t.Run("local ready removes current remote and pending remote without duplicates", func(t *testing.T) {
		mockDisp := newMockDispatcher(dispatcherID, 0)
		mockEventCollector := newTestEventCollector(localServerID)
		stat := newDispatcherStat(mockDisp, mockEventCollector, nil)
		setSessionState(stat.session, remoteServerID, true, anotherRemoteServerID)

		stat.handleSignalEvent(newReadyEvent(localServerID))

		requireDispatcherRequests(
			t,
			readDispatcherRequests(t, mockEventCollector, 3),
			dispatcherRequestRecord{to: remoteServerID, action: eventpb.ActionType_ACTION_TYPE_REMOVE},
			dispatcherRequestRecord{to: anotherRemoteServerID, action: eventpb.ActionType_ACTION_TYPE_REMOVE},
			dispatcherRequestRecord{to: localServerID, action: eventpb.ActionType_ACTION_TYPE_RESET},
		)
		requireNoDispatcherRequest(t, mockEventCollector)
	})

	t.Run("local ready with callback still removes speculative remote register", func(t *testing.T) {
		mockDisp := newMockDispatcher(dispatcherID, 0)
		mockEventCollector := newTestEventCollector(localServerID)
		stat := newDispatcherStat(mockDisp, mockEventCollector, nil)
		setSessionState(stat.session, "", true, remoteServerID)
		setSessionReadyCallback(stat.session, func() {})

		stat.handleSignalEvent(newReadyEvent(localServerID))

		requireDispatcherRequests(
			t,
			readDispatcherRequests(t, mockEventCollector, 1),
			dispatcherRequestRecord{to: remoteServerID, action: eventpb.ActionType_ACTION_TYPE_REMOVE},
		)
		requireNoDispatcherRequest(t, mockEventCollector)
	})
}

func TestIsFromCurrentEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		event          dispatcher.DispatcherEvent
		epoch          uint64
		lastEventSeq   uint64
		expectedResult bool
	}{
		{
			name: "first event is not handshake but epoch matches",
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeResolvedEvent,
					epoch:     1,
				},
			},
			epoch:          1,
			lastEventSeq:   0,
			expectedResult: true,
		},
		{
			name: "first event is handshake",
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeHandshakeEvent,
					epoch:     1,
				},
			},
			epoch:          1,
			lastEventSeq:   0,
			expectedResult: true,
		},
		{
			name: "subsequent event with correct epoch",
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeDMLEvent,
					epoch:     1,
				},
			},
			epoch:          1,
			lastEventSeq:   1,
			expectedResult: true,
		},
		{
			name: "stale epoch event",
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeResolvedEvent,
					epoch:     1,
				},
			},
			epoch:          2, // dispatcher epoch is 2, event epoch is 1
			lastEventSeq:   1,
			expectedResult: false,
		},
		{
			name: "batch dml with correct epoch",
			event: dispatcher.DispatcherEvent{
				Event: &commonEvent.BatchDMLEvent{
					DMLEvents: []*commonEvent.DMLEvent{
						{Epoch: 2},
						{Epoch: 2},
					},
				},
			},
			epoch:          2,
			lastEventSeq:   5,
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stat := newDispatcherStatForTest(newMockDispatcher(common.NewDispatcherID(), 0), nil)
			state := newDispatcherEpochState(tt.epoch, tt.lastEventSeq, stat.target.GetStartTs())
			stat.currentEpoch.Store(state)
			result := stat.isFromCurrentEpoch(tt.event, state)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestHandleDataEvents(t *testing.T) {
	localServerID := node.ID("local-server")
	remoteServerID := node.ID("remote-server")

	normalHandleEvents := func(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool) {
		return len(events) > 0
	}

	tests := []struct {
		name           string
		events         []dispatcher.DispatcherEvent
		initialState   func(*dispatcherStat)
		handleEvents   func(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool)
		expectedResult bool
	}{
		{
			name: "return false when event epoch is stale",
			events: []dispatcher.DispatcherEvent{
				{
					From: &remoteServerID,
					Event: &mockEvent{
						eventType: commonEvent.TypeDMLEvent,
						seq:       2,
						epoch:     1,
						commitTs:  100,
					},
				},
			},
			initialState: func(stat *dispatcherStat) {
				markSessionReceiving(stat.session, remoteServerID)
				stat.currentEpoch.Store(newDispatcherEpochState(2, 1, stat.target.GetStartTs()))
			},
			handleEvents:   normalHandleEvents,
			expectedResult: false,
		},
		{
			name: "handle DML events normally",
			events: []dispatcher.DispatcherEvent{
				{
					From: &remoteServerID,
					Event: &mockEvent{
						eventType: commonEvent.TypeDMLEvent,
						seq:       2,
						epoch:     2,
						commitTs:  100,
					},
				},
			},
			initialState: func(stat *dispatcherStat) {
				markSessionReceiving(stat.session, remoteServerID)
				stat.currentEpoch.Store(newDispatcherEpochState(2, 1, stat.target.GetStartTs()))
				stat.lastEventCommitTs.Store(50)
			},
			handleEvents:   normalHandleEvents,
			expectedResult: true,
		},
		{
			name: "return false when event sequence is discontinuous",
			events: []dispatcher.DispatcherEvent{
				{
					From: &remoteServerID,
					Event: &mockEvent{
						eventType: commonEvent.TypeDMLEvent,
						seq:       3,
						epoch:     10,
						commitTs:  100,
					},
				},
			},
			initialState: func(stat *dispatcherStat) {
				markSessionReceiving(stat.session, remoteServerID)
				stat.currentEpoch.Store(newDispatcherEpochState(10, 1, stat.target.GetStartTs()))
				stat.lastEventCommitTs.Store(50)
			},
			handleEvents:   normalHandleEvents,
			expectedResult: false,
		},
		{
			name: "handle DDL event normally",
			events: []dispatcher.DispatcherEvent{
				{
					From: &remoteServerID,
					Event: &commonEvent.DDLEvent{
						Version:    commonEvent.DDLEventVersion1,
						FinishedTs: 100,
						Epoch:      10,
						Seq:        2,
						TableInfo:  &common.TableInfo{},
					},
				},
			},
			initialState: func(stat *dispatcherStat) {
				markSessionReceiving(stat.session, remoteServerID)
				stat.currentEpoch.Store(newDispatcherEpochState(10, 1, stat.target.GetStartTs()))
				stat.lastEventCommitTs.Store(50)
			},
			handleEvents:   normalHandleEvents,
			expectedResult: true,
		},
		{
			name: "handle BatchDML event normally",
			events: []dispatcher.DispatcherEvent{
				{
					From: &remoteServerID,
					Event: &commonEvent.BatchDMLEvent{
						Rows:    chunk.NewEmptyChunk(nil),
						RawRows: []byte("test batchDML event"),
						DMLEvents: []*commonEvent.DMLEvent{
							{
								Seq:      2,
								Epoch:    10,
								CommitTs: 100,
							},
							{
								Seq:      3,
								Epoch:    10,
								CommitTs: 100,
							},
						},
					},
				},
			},
			initialState: func(stat *dispatcherStat) {
				markSessionReceiving(stat.session, remoteServerID)
				stat.currentEpoch.Store(newDispatcherEpochState(10, 1, stat.target.GetStartTs()))
				stat.lastEventCommitTs.Store(50)
				stat.tableInfo.Store(&common.TableInfo{})
			},
			handleEvents:   normalHandleEvents,
			expectedResult: true,
		},
		{
			name: "handle Resolved event normally",
			events: []dispatcher.DispatcherEvent{
				{
					From: &remoteServerID,
					Event: &mockEvent{
						eventType: commonEvent.TypeResolvedEvent,
						seq:       1,
						epoch:     10,
						commitTs:  100,
					},
				},
			},
			initialState: func(stat *dispatcherStat) {
				markSessionReceiving(stat.session, remoteServerID)
				stat.currentEpoch.Store(newDispatcherEpochState(10, 1, stat.target.GetStartTs()))
				stat.lastEventCommitTs.Store(50)
			},
			handleEvents:   normalHandleEvents,
			expectedResult: true,
		},
		{
			name: "ignore events with commit ts less than last commit ts",
			events: []dispatcher.DispatcherEvent{
				{
					From: &remoteServerID,
					Event: &mockEvent{
						eventType: commonEvent.TypeDMLEvent,
						seq:       2,
						epoch:     20,
						commitTs:  40,
					},
				},
			},
			initialState: func(stat *dispatcherStat) {
				markSessionReceiving(stat.session, remoteServerID)
				stat.currentEpoch.Store(newDispatcherEpochState(20, 1, stat.target.GetStartTs()))
				stat.lastEventCommitTs.Store(50)
			},
			handleEvents:   normalHandleEvents,
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stat := newDispatcherStat(newMockDispatcher(common.NewDispatcherID(), 0), newTestEventCollector(localServerID), nil)
			stat.target.(*mockDispatcher).handleEvents = tt.handleEvents

			if tt.initialState != nil {
				tt.initialState(stat)
			}

			result := stat.handleDataEvents(tt.events...)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func createNodeID(id string) *node.ID {
	nid := node.ID(id)
	return &nid
}

func TestHandleBatchDataEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		events         []dispatcher.DispatcherEvent
		currentService node.ID
		lastSeq        uint64
		lastCommitTs   uint64
		epoch          uint64
		want           bool
	}{
		{
			name: "valid events from current service",
			events: []dispatcher.DispatcherEvent{
				{
					From:  createNodeID("service1"),
					Event: &commonEvent.DMLEvent{Seq: 4, Epoch: 3, CommitTs: 100},
				},
				{
					From:  createNodeID("service1"),
					Event: &commonEvent.DMLEvent{Seq: 5, Epoch: 3, CommitTs: 101},
				},
			},
			currentService: node.ID("service1"),
			lastSeq:        3,
			lastCommitTs:   99,
			epoch:          3,
			want:           true,
		},
		{
			name: "invalid sequence",
			events: []dispatcher.DispatcherEvent{
				{
					From:  createNodeID("service1"),
					Event: &commonEvent.DMLEvent{Seq: 5, Epoch: 3, CommitTs: 100},
				},
			},
			currentService: node.ID("service1"),
			lastSeq:        3,
			lastCommitTs:   99,
			epoch:          3,
			want:           false,
		},
		{
			name: "stale events mixed with valid events",
			events: []dispatcher.DispatcherEvent{
				{
					From:  createNodeID("service2"),
					Event: &commonEvent.DMLEvent{Seq: 1, Epoch: 2, CommitTs: 100},
				},
				{
					From:  createNodeID("service1"),
					Event: &commonEvent.DMLEvent{Seq: 2, Epoch: 3, CommitTs: 101},
				},
			},
			currentService: node.ID("service1"),
			lastSeq:        1,
			lastCommitTs:   99,
			epoch:          3,
			want:           true,
		},
	}

	normalHandleEvents := func(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool) {
		return len(events) > 0
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
			mockDisp.handleEvents = normalHandleEvents
			mockEventCollector := newTestEventCollector(tt.currentService)
			stat := newDispatcherStat(mockDisp, mockEventCollector, nil)
			stat.loadCurrentEpochState().lastEventSeq.Store(tt.lastSeq)
			stat.lastEventCommitTs.Store(tt.lastCommitTs)
			state := stat.loadCurrentEpochState()
			stat.currentEpoch.Store(newDispatcherEpochState(tt.epoch, state.lastEventSeq.Load(), state.maxEventTs.Load()))
			markSessionReceiving(stat.session, tt.currentService)

			got := stat.handleBatchDataEvents(tt.events)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestHandleSingleDataEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		events         []dispatcher.DispatcherEvent
		currentService node.ID
		lastSeq        uint64
		lastCommitTs   uint64
		epoch          uint64
		want           bool
	}{
		{
			name: "multiple events",
			events: []dispatcher.DispatcherEvent{
				{Event: &commonEvent.DDLEvent{}},
				{Event: &commonEvent.DDLEvent{}},
			},
			currentService: node.ID("service1"),
			lastSeq:        1,
			want:           false,
		},
		{
			name: "stale service",
			events: []dispatcher.DispatcherEvent{
				{
					From:  createNodeID("service2"),
					Event: &commonEvent.DDLEvent{Seq: 2, Epoch: 9},
				},
			},
			currentService: node.ID("service1"),
			lastSeq:        1,
			epoch:          10,
			want:           false,
		},
		{
			name: "invalid sequence",
			events: []dispatcher.DispatcherEvent{
				{
					From:  createNodeID("service1"),
					Event: &commonEvent.DDLEvent{Seq: 3, Epoch: 10},
				},
			},
			currentService: node.ID("service1"),
			lastSeq:        1,
			epoch:          10,
			want:           false,
		},
		{
			name: "valid DDL event",
			events: []dispatcher.DispatcherEvent{
				{
					From:  createNodeID("service1"),
					Event: &commonEvent.DDLEvent{Seq: 2, Epoch: 10, FinishedTs: 100},
				},
			},
			currentService: node.ID("service1"),
			lastSeq:        1,
			lastCommitTs:   99,
			epoch:          10,
			want:           true,
		},
	}

	normalHandleEvents := func(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool) {
		return len(events) > 0
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
			mockDisp.handleEvents = normalHandleEvents
			mockEventCollector := newTestEventCollector(tt.currentService)
			stat := newDispatcherStat(mockDisp, mockEventCollector, nil)
			stat.loadCurrentEpochState().lastEventSeq.Store(tt.lastSeq)
			stat.lastEventCommitTs.Store(tt.lastCommitTs)
			state := stat.loadCurrentEpochState()
			stat.currentEpoch.Store(newDispatcherEpochState(tt.epoch, state.lastEventSeq.Load(), state.maxEventTs.Load()))
			markSessionReceiving(stat.session, tt.currentService)

			// Special handling for multiple events test case - it should panic
			if tt.name == "multiple events" {
				require.Panics(t, func() {
					stat.handleSingleDataEvents(tt.events)
				})
			} else {
				got := stat.handleSingleDataEvents(tt.events)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestHandleSingleDataEventsUpdatesDDLStateAndDedupsSameTsDDL(t *testing.T) {
	t.Parallel()

	mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
	mockDisp.handleEvents = func(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool) {
		return len(events) > 0
	}

	currentService := node.ID("service1")
	stat := newDispatcherStatForTest(mockDisp, nil)
	stat.lastEventCommitTs.Store(99)
	stat.currentEpoch.Store(newDispatcherEpochState(10, 1, stat.target.GetStartTs()))
	markSessionReceiving(stat.session, currentService)

	firstDDL := dispatcher.DispatcherEvent{
		From: createNodeID("service1"),
		Event: &commonEvent.DDLEvent{
			Seq:        2,
			Epoch:      10,
			FinishedTs: 100,
		},
	}
	secondDDL := dispatcher.DispatcherEvent{
		From: createNodeID("service1"),
		Event: &commonEvent.DDLEvent{
			Seq:        3,
			Epoch:      10,
			FinishedTs: 100,
		},
	}

	require.True(t, stat.handleSingleDataEvents([]dispatcher.DispatcherEvent{firstDDL}))
	require.Equal(t, uint64(100), stat.lastEventCommitTs.Load())
	require.True(t, stat.gotDDLOnTs.Load())
	require.False(t, stat.gotSyncpointOnTS.Load())
	require.Len(t, mockDisp.events, 1)

	require.False(t, stat.handleSingleDataEvents([]dispatcher.DispatcherEvent{secondDDL}))
	require.Equal(t, uint64(100), stat.lastEventCommitTs.Load())
	require.True(t, stat.gotDDLOnTs.Load())
	require.False(t, stat.gotSyncpointOnTS.Load())
	require.Len(t, mockDisp.events, 1)
}

func TestHandleSingleDataEventsUpdatesSyncPointStateAndDedupsSameTsSyncPoint(t *testing.T) {
	t.Parallel()

	mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
	mockDisp.handleEvents = func(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool) {
		return len(events) > 0
	}

	currentService := node.ID("service1")
	stat := newDispatcherStatForTest(mockDisp, nil)
	stat.lastEventCommitTs.Store(199)
	stat.currentEpoch.Store(newDispatcherEpochState(10, 1, stat.target.GetStartTs()))
	markSessionReceiving(stat.session, currentService)

	firstSyncPoint := dispatcher.DispatcherEvent{
		From: createNodeID("service1"),
		Event: &commonEvent.SyncPointEvent{
			Seq:      2,
			Epoch:    10,
			CommitTs: 200,
		},
	}
	secondSyncPoint := dispatcher.DispatcherEvent{
		From: createNodeID("service1"),
		Event: &commonEvent.SyncPointEvent{
			Seq:      3,
			Epoch:    10,
			CommitTs: 200,
		},
	}

	require.True(t, stat.handleSingleDataEvents([]dispatcher.DispatcherEvent{firstSyncPoint}))
	require.Equal(t, uint64(200), stat.lastEventCommitTs.Load())
	require.False(t, stat.gotDDLOnTs.Load())
	require.True(t, stat.gotSyncpointOnTS.Load())
	require.Len(t, mockDisp.events, 1)

	require.False(t, stat.handleSingleDataEvents([]dispatcher.DispatcherEvent{secondSyncPoint}))
	require.Equal(t, uint64(200), stat.lastEventCommitTs.Load())
	require.False(t, stat.gotDDLOnTs.Load())
	require.True(t, stat.gotSyncpointOnTS.Load())
	require.Len(t, mockDisp.events, 1)
}

func TestHandleBatchDMLEvent(t *testing.T) {
	normalHandleEvents := func(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool) {
		return len(events) > 0
	}

	tests := []struct {
		name         string
		events       []dispatcher.DispatcherEvent
		tableInfo    *common.TableInfo
		lastCommitTs uint64
		epoch        uint64
		lastSeq      uint64
		want         bool
	}{
		{
			name: "valid batch DML",
			events: []dispatcher.DispatcherEvent{
				{
					Event: &commonEvent.BatchDMLEvent{
						Rows:    chunk.NewEmptyChunk(nil),
						RawRows: []byte("test batch DML event"),
						DMLEvents: []*commonEvent.DMLEvent{
							{Seq: 2, Epoch: 10, CommitTs: 100},
							{Seq: 3, Epoch: 10, CommitTs: 100},
						},
					},
					From: createNodeID("service1"),
				},
				{
					Event: &commonEvent.BatchDMLEvent{
						Rows:    chunk.NewEmptyChunk(nil),
						RawRows: []byte("test batch DML event"),
						DMLEvents: []*commonEvent.DMLEvent{
							{Seq: 4, Epoch: 10, CommitTs: 200},
							{Seq: 5, Epoch: 10, CommitTs: 200},
						},
					},
					From: createNodeID("service1"),
				},
			},
			tableInfo:    &common.TableInfo{},
			lastCommitTs: 96,
			epoch:        10,
			lastSeq:      1,
			want:         true,
		},
		{
			name: "nil table info",
			events: []dispatcher.DispatcherEvent{
				{
					Event: &commonEvent.BatchDMLEvent{
						Rows:    chunk.NewEmptyChunk(nil),
						RawRows: []byte("test batch DML event"),
						DMLEvents: []*commonEvent.DMLEvent{
							{Seq: 3, Epoch: 10, CommitTs: 100},
							{Seq: 4, Epoch: 10, CommitTs: 100},
						},
					},
					From: createNodeID("service1"),
				},
			},
			epoch:   10,
			lastSeq: 2,
			want:    false,
		},
		{
			name: "stale commit ts",
			events: []dispatcher.DispatcherEvent{
				{
					Event: &commonEvent.BatchDMLEvent{
						Rows:    chunk.NewEmptyChunk(nil),
						RawRows: []byte("test batch DML event"),
						DMLEvents: []*commonEvent.DMLEvent{
							{Seq: 3, Epoch: 10, CommitTs: 98},
						},
					},
					From: createNodeID("service1"),
				},
			},
			tableInfo:    &common.TableInfo{},
			lastCommitTs: 99,
			epoch:        10,
			lastSeq:      2,
			want:         false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
			mockDisp.handleEvents = normalHandleEvents
			stat := newDispatcherStatForTest(mockDisp, nil)
			stat.lastEventCommitTs.Store(tt.lastCommitTs)
			stat.currentEpoch.Store(newDispatcherEpochState(tt.epoch, tt.lastSeq, stat.target.GetStartTs()))
			if tt.tableInfo != nil {
				stat.tableInfo.Store(tt.tableInfo)
			}
			if stat.tableInfo.Load() == nil {
				require.Panics(t, func() {
					stat.handleBatchDataEvents(tt.events)
				})
			} else {
				got := stat.handleBatchDataEvents(tt.events)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestHandleBatchDataEventsDoesNotAdvanceCommitTsWhenNoValidEvents(t *testing.T) {
	t.Parallel()

	mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
	mockDisp.handleEvents = func(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool) {
		return false
	}

	stat := newDispatcherStatForTest(mockDisp, nil)
	stat.lastEventCommitTs.Store(50)
	stat.currentEpoch.Store(newDispatcherEpochState(10, 1, stat.target.GetStartTs()))

	events := []dispatcher.DispatcherEvent{
		{
			Event: &mockEvent{
				eventType: commonEvent.TypeDMLEvent,
				seq:       2,
				epoch:     10,
				commitTs:  40,
			},
			From: createNodeID("service1"),
		},
	}

	require.False(t, stat.handleBatchDataEvents(events))
	require.Equal(t, uint64(50), stat.lastEventCommitTs.Load())
	require.Empty(t, mockDisp.events)
}

func TestNewDispatcherResetRequest(t *testing.T) {
	syncPointInterval := 10 * time.Second
	startTs := oracle.GoTimeToTS(time.Unix(0, 0).Add(1000 * syncPointInterval))
	nextSyncpointTs := oracle.GoTimeToTS(time.Unix(0, 0).Add(1001 * syncPointInterval))

	cases := []struct {
		name                   string
		resetTs                uint64
		skipSyncpointAtStartTs bool
		expectedSyncPointTs    uint64
	}{
		{
			name:                   "reset at startTs, skipSyncpointAtStartTs is true",
			resetTs:                startTs,
			skipSyncpointAtStartTs: true,
			expectedSyncPointTs:    nextSyncpointTs,
		},
		{
			name:                   "reset at startTs, skipSyncpointAtStartTs is false",
			resetTs:                startTs,
			skipSyncpointAtStartTs: false,
			expectedSyncPointTs:    startTs,
		},
		{
			name:                   "reset at nextSyncpointTs, skipSyncpointAtStartTs is true",
			resetTs:                nextSyncpointTs,
			skipSyncpointAtStartTs: true,
			expectedSyncPointTs:    nextSyncpointTs,
		},
		{
			name:                   "reset at nextSyncpointTs, skipSyncpointAtStartTs is false",
			resetTs:                nextSyncpointTs,
			skipSyncpointAtStartTs: false,
			expectedSyncPointTs:    nextSyncpointTs,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockDisp := newMockDispatcher(common.NewDispatcherID(), startTs)
			mockDisp.skipSyncpointAtStartTs = tc.skipSyncpointAtStartTs
			stat := newDispatcherStatForTest(mockDisp, nil)
			resetReq := stat.newDispatcherResetRequest("local", tc.resetTs, 1)
			require.Equal(t, tc.expectedSyncPointTs, resetReq.SyncPointTs)
		})
	}
}

func TestCheckpointTsForEventServiceUsesCollectorObservedMaxTs(t *testing.T) {
	t.Parallel()

	dispatcherID := common.NewDispatcherID()
	mockDisp := newMockDispatcher(dispatcherID, 100)
	mockDisp.checkPointTs = 220
	stat := newDispatcherStat(mockDisp, newTestEventCollector(node.ID("local")), nil)
	getHeartbeatCheckpoint := func() uint64 {
		checkpointTs, _ := stat.getHeartbeatProgressForEventService()
		return checkpointTs
	}

	require.Equal(t, uint64(100), stat.loadCurrentEpochState().maxEventTs.Load())
	require.Equal(t, uint64(100), getHeartbeatCheckpoint())

	stat.doReset(node.ID("event-service-1"), 150)
	require.Equal(t, uint64(150), stat.loadCurrentEpochState().maxEventTs.Load())
	require.Equal(t, uint64(150), getHeartbeatCheckpoint())

	handshake := commonEvent.NewHandshakeEvent(dispatcherID, 180, 1, &common.TableInfo{})
	stat.handleHandshakeEvent(dispatcher.DispatcherEvent{
		Event: &handshake,
	})
	require.Equal(t, uint64(180), stat.loadCurrentEpochState().maxEventTs.Load())
	require.Equal(t, uint64(180), getHeartbeatCheckpoint())

	mockDisp.checkPointTs = 170
	require.Equal(t, uint64(170), getHeartbeatCheckpoint())

	mockDisp.checkPointTs = 220
	resolved := commonEvent.NewResolvedEvent(200, dispatcherID, 1)
	resolved.Seq = 1
	stat.handleDataEvents(dispatcher.DispatcherEvent{Event: resolved})
	require.Equal(t, uint64(200), stat.loadCurrentEpochState().maxEventTs.Load())
	require.Equal(t, uint64(200), getHeartbeatCheckpoint())

	dml := &mockEvent{
		eventType: commonEvent.TypeDMLEvent,
		seq:       2,
		epoch:     1,
		commitTs:  210,
	}
	stat.handleDataEvents(dispatcher.DispatcherEvent{Event: dml})
	require.Equal(t, uint64(210), stat.loadCurrentEpochState().maxEventTs.Load())
	require.Equal(t, uint64(210), getHeartbeatCheckpoint())
}

func TestRegisterTo(t *testing.T) {
	localServerID := node.ID("local-server")
	remoteServerID := node.ID("remote-server")
	dispatcherID := common.NewDispatcherID()

	// Create a mock dispatcher and event collector
	mockDisp := newMockDispatcher(dispatcherID, 0)
	mockEventCollector := newTestEventCollector(localServerID)
	stat := newDispatcherStat(mockDisp, mockEventCollector, nil)

	// Test case 1: Register to local server
	t.Run("register to local server", func(t *testing.T) {
		stat.registerTo(localServerID)

		select {
		case msg := <-mockEventCollector.dispatcherMessageChan.Out():
			require.Equal(t, localServerID, msg.Message.To)
			req, ok := msg.Message.Message[0].(*messaging.DispatcherRequest)
			require.True(t, ok)
			require.Equal(t, eventpb.ActionType_ACTION_TYPE_REGISTER, req.ActionType)
			require.False(t, req.OnlyReuse, "OnlyReuse should be false for local registration")
			require.Equal(t, dispatcherID.ToPB(), req.DispatcherId)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timed out waiting for message")
		}
	})

	// Test case 2: Register to remote server
	t.Run("register to remote server", func(t *testing.T) {
		stat.registerTo(remoteServerID)

		select {
		case msg := <-mockEventCollector.dispatcherMessageChan.Out():
			require.Equal(t, remoteServerID, msg.Message.To)
			req, ok := msg.Message.Message[0].(*messaging.DispatcherRequest)
			require.True(t, ok)
			require.Equal(t, eventpb.ActionType_ACTION_TYPE_REGISTER, req.ActionType)
			require.True(t, req.OnlyReuse, "OnlyReuse should be true for remote registration")
			require.Equal(t, dispatcherID.ToPB(), req.DispatcherId)
		case <-time.After(1 * time.Second):
			require.Fail(t, "timed out waiting for message")
		}
	})
}

func TestRegisterAndRemoveRequestOrder(t *testing.T) {
	localServerID := node.ID("local-server")
	remoteServerID := node.ID("remote-server")
	dispatcherID := common.NewDispatcherID()

	registerStarted := make(chan struct{})
	allowRegister := make(chan struct{})
	var closeRegisterStarted sync.Once
	var mu sync.Mutex
	var requests []dispatcherRequestRecord
	sendMessage := func(msg *messaging.TargetMessage) {
		req, ok := msg.Message[0].(*messaging.DispatcherRequest)
		require.True(t, ok)
		if req.ActionType == eventpb.ActionType_ACTION_TYPE_REGISTER {
			closeRegisterStarted.Do(func() {
				close(registerStarted)
			})
			<-allowRegister
		}
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, dispatcherRequestRecord{
			to:     msg.To,
			action: req.ActionType,
		})
	}
	stat := newDispatcherStatInternal(
		newMockDispatcher(dispatcherID, 0),
		nil,
		localServerID,
		sendMessage,
		nil,
	)

	registerDone := make(chan struct{})
	go func() {
		stat.registerTo(remoteServerID)
		close(registerDone)
	}()

	select {
	case <-registerStarted:
	case <-time.After(1 * time.Second):
		require.FailNow(t, "timed out waiting for register request")
	}

	removeDone := make(chan struct{})
	go func() {
		stat.remove()
		close(removeDone)
	}()

	select {
	case <-removeDone:
		require.FailNow(t, "remove should wait for in-flight register request")
	case <-time.After(100 * time.Millisecond):
	}

	close(allowRegister)
	select {
	case <-registerDone:
	case <-time.After(1 * time.Second):
		require.FailNow(t, "timed out waiting for register to finish")
	}
	select {
	case <-removeDone:
	case <-time.After(1 * time.Second):
		require.FailNow(t, "timed out waiting for remove to finish")
	}

	require.Equal(t, []dispatcherRequestRecord{
		{to: remoteServerID, action: eventpb.ActionType_ACTION_TYPE_REGISTER},
		{to: localServerID, action: eventpb.ActionType_ACTION_TYPE_REMOVE},
		{to: remoteServerID, action: eventpb.ActionType_ACTION_TYPE_REMOVE},
	}, requests)
}

func TestLocalHeartbeatRemovedReregisterReadySendsReset(t *testing.T) {
	localServerID := node.ID("local-server")
	dispatcherID := common.NewDispatcherID()
	mockDisp := newMockDispatcher(dispatcherID, 0)
	mockEventCollector := newTestEventCollector(localServerID)
	stat := newDispatcherStat(mockDisp, mockEventCollector, nil)
	mockEventCollector.dispatcherMap.Store(dispatcherID, stat)
	markSessionReceiving(stat.session, localServerID)

	response := commonEvent.NewDispatcherHeartbeatResponse()
	response.Append(commonEvent.NewDispatcherState(dispatcherID, commonEvent.DSStateRemoved))
	msg := messaging.NewSingleTargetMessage(localServerID, messaging.EventCollectorTopic, response)
	msg.From = localServerID

	mockEventCollector.handleDispatcherHeartbeatResponse(msg)
	requireDispatcherRequests(
		t,
		readDispatcherRequests(t, mockEventCollector, 1),
		dispatcherRequestRecord{to: localServerID, action: eventpb.ActionType_ACTION_TYPE_REGISTER},
	)

	stat.handleSignalEvent(dispatcher.DispatcherEvent{
		From: &localServerID,
		Event: &mockEvent{
			eventType: commonEvent.TypeReadyEvent,
		},
	})

	requireDispatcherRequests(
		t,
		readDispatcherRequests(t, mockEventCollector, 1),
		dispatcherRequestRecord{to: localServerID, action: eventpb.ActionType_ACTION_TYPE_RESET},
	)
	requireNoDispatcherRequest(t, mockEventCollector)
	currentEventServiceID, localReadyPending, pendingRemoteTarget := sessionState(stat.session)
	require.Equal(t, localServerID, currentEventServiceID)
	require.False(t, localReadyPending)
	require.Equal(t, node.ID(""), pendingRemoteTarget)
}

func TestHandleDDLEventTableInfoUpdate(t *testing.T) {
	t.Parallel()

	remoteServerID := node.ID("remote")

	t.Run("stores ddl table info", func(t *testing.T) {
		var capturedEvent *commonEvent.DDLEvent
		mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
		mockDisp.handleEvents = func(events []dispatcher.DispatcherEvent, wakeCallback func()) bool {
			if len(events) > 0 {
				capturedEvent = events[0].Event.(*commonEvent.DDLEvent)
			}
			return false
		}

		stat := newDispatcherStatForTest(mockDisp, nil)
		markSessionReceiving(stat.session, remoteServerID)
		stat.currentEpoch.Store(newDispatcherEpochState(10, 1, stat.target.GetStartTs()))
		stat.lastEventCommitTs.Store(50)

		tableInfo := &common.TableInfo{
			TableName: common.TableName{
				Schema:  "source_db",
				Table:   "users",
				TableID: 1,
			},
		}

		ddlEvent := &commonEvent.DDLEvent{
			Version:    commonEvent.DDLEventVersion1,
			Query:      "ALTER TABLE `source_db`.`users` ADD COLUMN `c1` INT",
			FinishedTs: 100,
			Epoch:      10,
			Seq:        2,
			TableInfo:  tableInfo,
		}

		events := []dispatcher.DispatcherEvent{
			{From: &remoteServerID, Event: ddlEvent},
		}

		stat.handleDataEvents(events...)

		stored := stat.tableInfo.Load()
		require.NotNil(t, stored)
		storedTableInfo := stored.(*common.TableInfo)
		require.Same(t, tableInfo, storedTableInfo)
		require.Equal(t, "source_db", storedTableInfo.TableName.Schema)
		require.Equal(t, "users", storedTableInfo.TableName.Table)
		require.Equal(t, int64(1), storedTableInfo.TableName.TableID)
		require.Equal(t, uint64(100), stat.tableInfoVersion.Load())
		require.NotNil(t, capturedEvent)
		require.Same(t, ddlEvent, capturedEvent)
	})
}

func TestRemove(t *testing.T) {
	localServerID := node.ID("local-server")
	remoteServerID := node.ID("remote-server")
	anotherRemoteServerID := node.ID("another-remote-server")
	dispatcherID := common.NewDispatcherID()

	t.Run("remove local and current remote", func(t *testing.T) {
		mockDisp := newMockDispatcher(dispatcherID, 0)
		mockEventCollector := newTestEventCollector(localServerID)
		stat := newDispatcherStat(mockDisp, mockEventCollector, nil)
		setSessionState(stat.session, remoteServerID, true, "")

		stat.remove()

		requireRemoveTargets(
			t,
			readRemoveTargets(t, mockEventCollector, 2),
			localServerID,
			remoteServerID,
		)
	})

	t.Run("remove local and pending remote even if remote not ready", func(t *testing.T) {
		mockDisp := newMockDispatcher(dispatcherID, 0)
		mockEventCollector := newTestEventCollector(localServerID)
		stat := newDispatcherStat(mockDisp, mockEventCollector, nil)
		setSessionState(stat.session, "", true, remoteServerID)

		stat.remove()

		requireRemoveTargets(
			t,
			readRemoveTargets(t, mockEventCollector, 2),
			localServerID,
			remoteServerID,
		)
	})

	t.Run("remove local current remote and another pending remote without duplicates", func(t *testing.T) {
		mockDisp := newMockDispatcher(dispatcherID, 0)
		mockEventCollector := newTestEventCollector(localServerID)
		stat := newDispatcherStat(mockDisp, mockEventCollector, nil)
		setSessionState(stat.session, remoteServerID, true, anotherRemoteServerID)

		stat.remove()

		requireRemoveTargets(
			t,
			readRemoveTargets(t, mockEventCollector, 3),
			localServerID,
			remoteServerID,
			anotherRemoteServerID,
		)
	})

	t.Run("late signal is ignored after remove", func(t *testing.T) {
		mockDisp := newMockDispatcher(dispatcherID, 0)
		mockEventCollector := newTestEventCollector(localServerID)
		stat := newDispatcherStat(mockDisp, mockEventCollector, nil)
		setSessionState(stat.session, "", true, remoteServerID)

		stat.remove()
		stat.handleSignalEvent(dispatcher.DispatcherEvent{
			From: &localServerID,
			Event: &mockEvent{
				eventType: commonEvent.TypeReadyEvent,
			},
		})

		currentEventServiceID, localReadyPending, pendingRemoteTarget := sessionState(stat.session)
		require.Equal(t, node.ID(""), currentEventServiceID)
		require.False(t, stat.session.isReceivingDataEvent())
		require.False(t, localReadyPending)
		require.Equal(t, node.ID(""), pendingRemoteTarget)
	})
}

func requireRemoveTargets(t *testing.T, got []node.ID, expected ...node.ID) {
	t.Helper()
	require.Len(t, got, len(expected))
	gotSet := make(map[node.ID]struct{}, len(got))
	for _, id := range got {
		gotSet[id] = struct{}{}
	}
	for _, id := range expected {
		_, ok := gotSet[id]
		require.True(t, ok, "missing remove target %s", id)
	}
}

type dispatcherRequestRecord struct {
	to     node.ID
	action eventpb.ActionType
}

func requireDispatcherRequests(t *testing.T, got []dispatcherRequestRecord, expected ...dispatcherRequestRecord) {
	t.Helper()
	require.Len(t, got, len(expected))
	gotSet := make(map[dispatcherRequestRecord]struct{}, len(got))
	for _, record := range got {
		gotSet[record] = struct{}{}
	}
	for _, record := range expected {
		_, ok := gotSet[record]
		require.True(
			t,
			ok,
			"missing request action=%s target=%s",
			record.action.String(),
			record.to,
		)
	}
}

func readDispatcherRequests(t *testing.T, collector *EventCollector, count int) []dispatcherRequestRecord {
	t.Helper()
	requests := make([]dispatcherRequestRecord, 0, count)
	for range count {
		select {
		case msg := <-collector.dispatcherMessageChan.Out():
			req, ok := msg.Message.Message[0].(*messaging.DispatcherRequest)
			require.True(t, ok)
			requests = append(requests, dispatcherRequestRecord{
				to:     msg.Message.To,
				action: req.ActionType,
			})
		case <-time.After(1 * time.Second):
			require.FailNow(t, "timed out waiting for dispatcher request")
		}
	}
	return requests
}

func requireNoDispatcherRequest(t *testing.T, collector *EventCollector) {
	t.Helper()
	select {
	case msg := <-collector.dispatcherMessageChan.Out():
		req, ok := msg.Message.Message[0].(*messaging.DispatcherRequest)
		require.True(t, ok)
		require.FailNowf(
			t,
			"unexpected dispatcher request",
			"action=%s target=%s",
			req.ActionType.String(),
			msg.Message.To,
		)
	case <-time.After(100 * time.Millisecond):
	}
}

func readRemoveTargets(t *testing.T, collector *EventCollector, count int) []node.ID {
	t.Helper()
	targets := make([]node.ID, 0, count)
	for range count {
		select {
		case msg := <-collector.dispatcherMessageChan.Out():
			req, ok := msg.Message.Message[0].(*messaging.DispatcherRequest)
			require.True(t, ok)
			require.Equal(t, eventpb.ActionType_ACTION_TYPE_REMOVE, req.ActionType)
			targets = append(targets, msg.Message.To)
		case <-time.After(1 * time.Second):
			require.FailNow(t, "timed out waiting for remove message")
		}
	}
	return targets
}
