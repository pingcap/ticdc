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
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	sinkutil "github.com/pingcap/ticdc/pkg/sink/util"
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
	router                 *sinkutil.Router
}

func newMockDispatcher(id common.DispatcherID, startTs uint64) *mockDispatcher {
	return &mockDispatcher{
		id:           id,
		startTs:      startTs,
		changefeedID: mockChangefeedID,
		checkPointTs: startTs,
	}
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

func (m *mockDispatcher) GetTableSpan() *heartbeatpb.TableSpan {
	return &heartbeatpb.TableSpan{
		TableID: 1,
	}
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

func (m *mockDispatcher) GetRouter() *sinkutil.Router {
	return m.router
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
			stat := &dispatcherStat{
				target: newMockDispatcher(common.NewDispatcherID(), 0),
			}
			stat.lastEventSeq.Store(tt.lastEventSeq)
			result := stat.verifyEventSequence(tt.event)
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
			stat := &dispatcherStat{
				target: newMockDispatcher(common.NewDispatcherID(), 0),
			}
			stat.lastEventCommitTs.Store(tt.lastEventCommitTs)
			stat.gotDDLOnTs.Store(tt.gotDDLOnTs)
			stat.gotSyncpointOnTS.Store(tt.gotSyncpointOnTS)

			result := stat.filterAndUpdateEventByCommitTs(tt.event)
			require.Equal(t, tt.expectedResult, result)
			require.Equal(t, tt.expectedDDLOnTs, stat.gotDDLOnTs.Load())
			require.Equal(t, tt.expectedSyncOnTs, stat.gotSyncpointOnTS.Load())
			require.Equal(t, tt.expectedCommitTs, stat.lastEventCommitTs.Load())
		})
	}
}

func TestHandleSignalEvent(t *testing.T) {
	localServerID := node.ID("local-server")
	remoteServerID := node.ID("remote-server")
	anotherRemoteServerID := node.ID("another-remote-server")

	tests := []struct {
		name                   string
		event                  dispatcher.DispatcherEvent
		initialState           func(*dispatcherStat)
		expectedEventServiceID node.ID
		expectedReadyReceived  bool
		expectedPanic          bool
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
				stat.connState.setEventServiceID(localServerID)
			},
			expectedEventServiceID: localServerID,
			expectedReadyReceived:  false,
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
				stat.connState.setEventServiceID(remoteServerID)
			},
			expectedEventServiceID: remoteServerID,
			expectedReadyReceived:  false,
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
				stat.readyCallback = func() {}
			},
			expectedEventServiceID: localServerID,
			expectedReadyReceived:  true,
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
				stat.connState.setEventServiceID(remoteServerID)
			},
			expectedEventServiceID: localServerID,
			expectedReadyReceived:  true,
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
				stat.connState.setEventServiceID(remoteServerID)
			},
			expectedEventServiceID: remoteServerID,
			expectedReadyReceived:  true,
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
				stat.connState.setEventServiceID(remoteServerID)
				stat.connState.readyEventReceived.Store(true)
			},
			expectedEventServiceID: remoteServerID,
			expectedReadyReceived:  true,
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
				stat.connState.setEventServiceID(remoteServerID)
				stat.connState.remoteCandidates = []string{anotherRemoteServerID.String()}
			},
			expectedEventServiceID: anotherRemoteServerID,
			expectedReadyReceived:  false,
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
				stat.connState.setEventServiceID(remoteServerID)
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
				stat.connState.setEventServiceID(remoteServerID)
			},
			expectedPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stat := &dispatcherStat{
				target:         newMockDispatcher(common.NewDispatcherID(), 0),
				eventCollector: newTestEventCollector(localServerID),
			}
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
			require.Equal(t, tt.expectedEventServiceID, stat.connState.getEventServiceID())
			require.Equal(t, tt.expectedReadyReceived, stat.connState.readyEventReceived.Load())
		})
	}
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
			stat := &dispatcherStat{
				target: newMockDispatcher(common.NewDispatcherID(), 0),
			}
			stat.epoch.Store(tt.epoch)
			stat.lastEventSeq.Store(tt.lastEventSeq)
			result := stat.isFromCurrentEpoch(tt.event)
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
				stat.connState.setEventServiceID(remoteServerID)
				stat.lastEventSeq.Store(1)
				stat.epoch.Store(2)
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
				stat.connState.setEventServiceID(remoteServerID)
				stat.epoch.Store(2)
				stat.lastEventSeq.Store(1)
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
				stat.connState.setEventServiceID(remoteServerID)
				stat.lastEventSeq.Store(1)
				stat.epoch.Store(10)
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
				stat.connState.setEventServiceID(remoteServerID)
				stat.epoch.Store(10)
				stat.lastEventSeq.Store(1)
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
				stat.connState.setEventServiceID(remoteServerID)
				stat.epoch.Store(10)
				stat.lastEventSeq.Store(1)
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
				stat.connState.setEventServiceID(remoteServerID)
				stat.epoch.Store(10)
				stat.lastEventSeq.Store(1)
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
				stat.connState.setEventServiceID(remoteServerID)
				stat.epoch.Store(20)
				stat.lastEventSeq.Store(1)
				stat.lastEventCommitTs.Store(50)
			},
			handleEvents:   normalHandleEvents,
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stat := &dispatcherStat{
				target:         newMockDispatcher(common.NewDispatcherID(), 0),
				eventCollector: newTestEventCollector(localServerID),
			}
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
			stat.lastEventSeq.Store(tt.lastSeq)
			stat.lastEventCommitTs.Store(tt.lastCommitTs)
			stat.epoch.Store(tt.epoch)
			stat.connState.setEventServiceID(tt.currentService)
			stat.connState.readyEventReceived.Store(true)

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
			stat.lastEventSeq.Store(tt.lastSeq)
			stat.lastEventCommitTs.Store(tt.lastCommitTs)
			stat.epoch.Store(tt.epoch)
			stat.connState.setEventServiceID(tt.currentService)
			stat.connState.readyEventReceived.Store(true)

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
			stat := newDispatcherStat(mockDisp, nil, nil)
			stat.lastEventCommitTs.Store(tt.lastCommitTs)
			stat.epoch.Store(tt.epoch)
			stat.lastEventSeq.Store(tt.lastSeq)
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
			stat := newDispatcherStat(mockDisp, nil, nil)
			resetReq := stat.newDispatcherResetRequest("local", tc.resetTs, 1)
			require.Equal(t, tc.expectedSyncPointTs, resetReq.SyncPointTs)
		})
	}
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

func TestApplyRoutingToTableInfo(t *testing.T) {
	t.Parallel()

	localServerID := node.ID("local")
	remoteServerID := node.ID("remote")

	// Create a router that routes source_db.* -> target_db.*
	router, err := sinkutil.NewRouter(true, []sinkutil.RoutingRuleConfig{
		{Matcher: []string{"source_db.*"}, SchemaRule: "target_db", TableRule: sinkutil.TablePlaceholder},
	})
	require.NoError(t, err)

	t.Run("DDL with TableInfo gets routing applied and stored", func(t *testing.T) {
		// Capture the event passed to HandleEvents to verify routing was applied
		var capturedEvent *commonEvent.DDLEvent
		mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
		mockDisp.router = router
		mockDisp.handleEvents = func(events []dispatcher.DispatcherEvent, wakeCallback func()) bool {
			if len(events) > 0 {
				capturedEvent = events[0].Event.(*commonEvent.DDLEvent)
			}
			return false
		}

		stat := &dispatcherStat{
			target:         mockDisp,
			eventCollector: newTestEventCollector(localServerID),
		}
		stat.connState.setEventServiceID(remoteServerID)
		stat.epoch.Store(10)
		stat.lastEventSeq.Store(1)
		stat.lastEventCommitTs.Store(50)

		// Create original TableInfo - should NOT be mutated
		originalTableInfo := &common.TableInfo{
			TableName: common.TableName{
				Schema:  "source_db",
				Table:   "users",
				TableID: 123,
			},
		}

		ddlEvent := &commonEvent.DDLEvent{
			Version:    commonEvent.DDLEventVersion1,
			FinishedTs: 100,
			Epoch:      10,
			Seq:        2,
			TableInfo:  originalTableInfo,
		}

		events := []dispatcher.DispatcherEvent{
			{From: &remoteServerID, Event: ddlEvent},
		}

		stat.handleDataEvents(events...)

		// Verify the stored tableInfo has routing applied
		storedTableInfo := stat.tableInfo.Load().(*common.TableInfo)
		require.NotNil(t, storedTableInfo)
		require.Equal(t, "target_db", storedTableInfo.TableName.TargetSchema)
		require.Equal(t, "users", storedTableInfo.TableName.TargetTable)

		// Verify original TableInfo was NOT mutated
		require.Equal(t, "", originalTableInfo.TableName.TargetSchema)
		require.Equal(t, "", originalTableInfo.TableName.TargetTable)

		// Verify original ddlEvent was NOT mutated (due to CloneForRouting)
		require.Equal(t, "", ddlEvent.TableInfo.TableName.TargetSchema)

		// Verify the cloned event passed to HandleEvents has routing applied
		require.NotNil(t, capturedEvent)
		require.Equal(t, "target_db", capturedEvent.TableInfo.TableName.TargetSchema)
	})

	t.Run("DDL with MultipleTableInfos gets routing applied but only TableInfo stored", func(t *testing.T) {
		// Capture the event passed to HandleEvents to verify routing was applied
		var capturedEvent *commonEvent.DDLEvent
		mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
		mockDisp.router = router
		mockDisp.handleEvents = func(events []dispatcher.DispatcherEvent, wakeCallback func()) bool {
			if len(events) > 0 {
				capturedEvent = events[0].Event.(*commonEvent.DDLEvent)
			}
			return false
		}

		stat := &dispatcherStat{
			target:         mockDisp,
			eventCollector: newTestEventCollector(localServerID),
		}
		stat.connState.setEventServiceID(remoteServerID)
		stat.epoch.Store(10)
		stat.lastEventSeq.Store(1)
		stat.lastEventCommitTs.Store(50)

		// This dispatcher's table
		primaryTableInfo := &common.TableInfo{
			TableName: common.TableName{
				Schema:  "source_db",
				Table:   "orders",
				TableID: 100,
			},
		}

		// Other tables in a multi-table DDL (e.g., RENAME TABLE)
		otherTableInfo1 := &common.TableInfo{
			TableName: common.TableName{
				Schema:  "source_db",
				Table:   "old_name",
				TableID: 200,
			},
		}
		otherTableInfo2 := &common.TableInfo{
			TableName: common.TableName{
				Schema:  "source_db",
				Table:   "new_name",
				TableID: 201,
			},
		}

		ddlEvent := &commonEvent.DDLEvent{
			Version:            commonEvent.DDLEventVersion1,
			FinishedTs:         100,
			Epoch:              10,
			Seq:                2,
			TableInfo:          primaryTableInfo,
			MultipleTableInfos: []*common.TableInfo{otherTableInfo1, otherTableInfo2},
		}

		events := []dispatcher.DispatcherEvent{
			{From: &remoteServerID, Event: ddlEvent},
		}

		stat.handleDataEvents(events...)

		// Verify only the primary TableInfo is stored
		storedTableInfo := stat.tableInfo.Load().(*common.TableInfo)
		require.NotNil(t, storedTableInfo)
		require.Equal(t, "orders", storedTableInfo.TableName.Table)
		require.Equal(t, "target_db", storedTableInfo.TableName.TargetSchema)

		// Verify original ddlEvent was NOT mutated (due to CloneForRouting)
		require.Equal(t, "", ddlEvent.MultipleTableInfos[0].TableName.TargetSchema)
		require.Equal(t, "", ddlEvent.MultipleTableInfos[1].TableName.TargetSchema)

		// Verify the cloned event passed to HandleEvents has routing applied
		require.NotNil(t, capturedEvent)
		require.Equal(t, "target_db", capturedEvent.MultipleTableInfos[0].TableName.TargetSchema)
		require.Equal(t, "target_db", capturedEvent.MultipleTableInfos[1].TableName.TargetSchema)

		// Verify originals were NOT mutated
		require.Equal(t, "", otherTableInfo1.TableName.TargetSchema)
		require.Equal(t, "", otherTableInfo2.TableName.TargetSchema)
	})

	t.Run("DDL without routing configured passes through unchanged", func(t *testing.T) {
		mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
		// No router configured
		mockDisp.router = nil
		mockDisp.handleEvents = func(events []dispatcher.DispatcherEvent, wakeCallback func()) bool {
			return false
		}

		stat := &dispatcherStat{
			target:         mockDisp,
			eventCollector: newTestEventCollector(localServerID),
		}
		stat.connState.setEventServiceID(remoteServerID)
		stat.epoch.Store(10)
		stat.lastEventSeq.Store(1)
		stat.lastEventCommitTs.Store(50)

		tableInfo := &common.TableInfo{
			TableName: common.TableName{
				Schema:  "source_db",
				Table:   "users",
				TableID: 123,
			},
		}

		ddlEvent := &commonEvent.DDLEvent{
			Version:    commonEvent.DDLEventVersion1,
			FinishedTs: 100,
			Epoch:      10,
			Seq:        2,
			TableInfo:  tableInfo,
		}

		events := []dispatcher.DispatcherEvent{
			{From: &remoteServerID, Event: ddlEvent},
		}

		stat.handleDataEvents(events...)

		// Verify tableInfo is stored (same object since no routing)
		storedTableInfo := stat.tableInfo.Load().(*common.TableInfo)
		require.NotNil(t, storedTableInfo)
		// No routing applied, so TargetSchema/TargetTable should be empty
		require.Equal(t, "", storedTableInfo.TableName.TargetSchema)
		require.Equal(t, "", storedTableInfo.TableName.TargetTable)
	})

	t.Run("DDL with table-only routing (schema unchanged)", func(t *testing.T) {
		// Create a router that only renames the table, keeping schema the same
		tableOnlyRouter, err := sinkutil.NewRouter(true, []sinkutil.RoutingRuleConfig{
			{Matcher: []string{"mydb.old_users"}, SchemaRule: "{schema}", TableRule: "new_users"},
		})
		require.NoError(t, err)

		mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
		mockDisp.router = tableOnlyRouter
		mockDisp.handleEvents = func(events []dispatcher.DispatcherEvent, wakeCallback func()) bool {
			return false
		}

		stat := &dispatcherStat{
			target:         mockDisp,
			eventCollector: newTestEventCollector(localServerID),
		}
		stat.connState.setEventServiceID(remoteServerID)
		stat.epoch.Store(10)
		stat.lastEventSeq.Store(1)
		stat.lastEventCommitTs.Store(50)

		originalTableInfo := &common.TableInfo{
			TableName: common.TableName{
				Schema:  "mydb",
				Table:   "old_users",
				TableID: 123,
			},
		}

		ddlEvent := &commonEvent.DDLEvent{
			Version:    commonEvent.DDLEventVersion1,
			FinishedTs: 100,
			Epoch:      10,
			Seq:        2,
			TableInfo:  originalTableInfo,
		}

		events := []dispatcher.DispatcherEvent{
			{From: &remoteServerID, Event: ddlEvent},
		}

		stat.handleDataEvents(events...)

		// Verify schema is unchanged but table is renamed
		storedTableInfo := stat.tableInfo.Load().(*common.TableInfo)
		require.NotNil(t, storedTableInfo)
		require.Equal(t, "mydb", storedTableInfo.TableName.TargetSchema)
		require.Equal(t, "new_users", storedTableInfo.TableName.TargetTable)

		// Verify original was NOT mutated
		require.Equal(t, "", originalTableInfo.TableName.TargetSchema)
		require.Equal(t, "", originalTableInfo.TableName.TargetTable)
	})

	t.Run("DDL with both schema and table routing", func(t *testing.T) {
		// Create a router that renames both schema and table
		bothRouter, err := sinkutil.NewRouter(true, []sinkutil.RoutingRuleConfig{
			{Matcher: []string{"staging.*"}, SchemaRule: "prod", TableRule: "{schema}_{table}"},
		})
		require.NoError(t, err)

		mockDisp := newMockDispatcher(common.NewDispatcherID(), 0)
		mockDisp.router = bothRouter
		mockDisp.handleEvents = func(events []dispatcher.DispatcherEvent, wakeCallback func()) bool {
			return false
		}

		stat := &dispatcherStat{
			target:         mockDisp,
			eventCollector: newTestEventCollector(localServerID),
		}
		stat.connState.setEventServiceID(remoteServerID)
		stat.epoch.Store(10)
		stat.lastEventSeq.Store(1)
		stat.lastEventCommitTs.Store(50)

		originalTableInfo := &common.TableInfo{
			TableName: common.TableName{
				Schema:  "staging",
				Table:   "events",
				TableID: 456,
			},
		}

		ddlEvent := &commonEvent.DDLEvent{
			Version:    commonEvent.DDLEventVersion1,
			FinishedTs: 100,
			Epoch:      10,
			Seq:        2,
			TableInfo:  originalTableInfo,
		}

		events := []dispatcher.DispatcherEvent{
			{From: &remoteServerID, Event: ddlEvent},
		}

		stat.handleDataEvents(events...)

		// Verify both schema and table are renamed
		storedTableInfo := stat.tableInfo.Load().(*common.TableInfo)
		require.NotNil(t, storedTableInfo)
		require.Equal(t, "prod", storedTableInfo.TableName.TargetSchema)
		require.Equal(t, "staging_events", storedTableInfo.TableName.TargetTable)

		// Verify original was NOT mutated
		require.Equal(t, "", originalTableInfo.TableName.TargetSchema)
		require.Equal(t, "", originalTableInfo.TableName.TargetTable)
	})
}
