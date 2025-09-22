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
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

var mockChangefeedID = common.NewChangeFeedIDWithName("dispatcher_stat_test", common.DefaultKeyspace)

// mockDispatcher implements the dispatcher.EventDispatcher interface for testing
type mockDispatcher struct {
	dispatcher.EventDispatcher
	startTs      uint64
	id           common.DispatcherID
	changefeedID common.ChangeFeedID
	handleEvents func(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool)
	events       []dispatcher.DispatcherEvent
	checkPointTs uint64

	skipSyncpointSameAsStartTs bool
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

func (m *mockDispatcher) GetSkipSyncpointSameAsStartTs() bool {
	return m.skipSyncpointSameAsStartTs
}

func (m *mockDispatcher) GetResolvedTs() uint64 {
	return m.startTs
}

func (m *mockDispatcher) GetCheckpointTs() uint64 {
	return m.checkPointTs
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
					CommitTsList: []uint64{100},
					Seq:          5,
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
			name: "first event is not handshake",
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeResolvedEvent,
					epoch:     1,
				},
			},
			epoch:          1,
			lastEventSeq:   0,
			expectedResult: false,
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
						Version:    commonEvent.DDLEventVersion,
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
						seq:       2,
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
		name                       string
		resetTs                    uint64
		skipSyncpointSameAsStartTs bool
		expectedSyncPointTs        uint64
	}{
		{
			name:                       "reset at startTs, skipSyncpointSameAsStartTs is true",
			resetTs:                    startTs,
			skipSyncpointSameAsStartTs: true,
			expectedSyncPointTs:        nextSyncpointTs,
		},
		{
			name:                       "reset at startTs, skipSyncpointSameAsStartTs is false",
			resetTs:                    startTs,
			skipSyncpointSameAsStartTs: false,
			expectedSyncPointTs:        startTs,
		},
		{
			name:                       "reset at nextSyncpointTs, skipSyncpointSameAsStartTs is true",
			resetTs:                    nextSyncpointTs,
			skipSyncpointSameAsStartTs: true,
			expectedSyncPointTs:        nextSyncpointTs,
		},
		{
			name:                       "reset at nextSyncpointTs, skipSyncpointSameAsStartTs is false",
			resetTs:                    nextSyncpointTs,
			skipSyncpointSameAsStartTs: false,
			expectedSyncPointTs:        nextSyncpointTs,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockDisp := newMockDispatcher(common.NewDispatcherID(), startTs)
			mockDisp.skipSyncpointSameAsStartTs = tc.skipSyncpointSameAsStartTs
			stat := newDispatcherStat(mockDisp, nil, nil)
			resetReq := stat.newDispatcherResetRequest("local", tc.resetTs, 1)
			require.Equal(t, tc.expectedSyncPointTs, resetReq.SyncPointTs)
		})
	}
}
