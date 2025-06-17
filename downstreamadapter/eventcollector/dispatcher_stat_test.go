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
	"github.com/stretchr/testify/require"
)

var mockChangefeedID = common.NewChangeFeedIDWithName("dispatcher_stat_test")

// mockDispatcher implements the dispatcher.EventDispatcher interface for testing
type mockDispatcher struct {
	dispatcher.EventDispatcher
	startTs      uint64
	id           common.DispatcherID
	changefeedID common.ChangeFeedID
}

func newMockDispatcher(id common.DispatcherID, startTs uint64) *mockDispatcher {
	return &mockDispatcher{
		id:           id,
		startTs:      startTs,
		changefeedID: mockChangefeedID,
	}
}

func (m *mockDispatcher) GetStartTs() uint64 {
	return m.startTs
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

func (m *mockDispatcher) GetStartTsIsSyncpoint() bool {
	return false
}

func (m *mockDispatcher) GetResolvedTs() uint64 {
	return m.startTs
}

func (m *mockDispatcher) HandleEvents(events []dispatcher.DispatcherEvent, wakeCallback func()) (block bool) {
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
}

func (m *mockEvent) GetType() int {
	return m.eventType
}

func (m *mockEvent) GetSeq() uint64 {
	return m.seq
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

func TestGetResetTs(t *testing.T) {
	// Test case 1: lastEventCommitTs is greater than startTs
	// Expected: resetTs should be lastEventCommitTs - 1
	stat := &dispatcherStat{
		target: &mockDispatcher{},
	}
	stat.lastEventCommitTs.Store(100)
	stat.target.(*mockDispatcher).startTs = 50
	resetTs := stat.getResetTs()
	if resetTs != 99 {
		t.Errorf("Expected resetTs to be 99, got %d", resetTs)
	}

	// Test case 2: lastEventCommitTs is equal to startTs
	// Expected: resetTs should be startTs
	stat.lastEventCommitTs.Store(50)
	stat.target.(*mockDispatcher).startTs = 50
	resetTs = stat.getResetTs()
	if resetTs != 50 {
		t.Errorf("Expected resetTs to be 50, got %d", resetTs)
	}

	// Test case 3: lastEventCommitTs is less than startTs
	// Expected: resetTs should be startTs
	stat.lastEventCommitTs.Store(30)
	stat.target.(*mockDispatcher).startTs = 50
	resetTs = stat.getResetTs()
	if resetTs != 50 {
		t.Errorf("Expected resetTs to be 50, got %d", resetTs)
	}
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
			lastEventCommitTs: 100,
			gotSyncpointOnTS:  true,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeSyncPointEvent,
					commitTs:  100,
				},
			},
			expectedResult:   false,
			expectedDDLOnTs:  false,
			expectedSyncOnTs: true,
			expectedCommitTs: 100,
		},
		{
			name:              "SyncPoint event with same commit ts and not got SyncPoint",
			lastEventCommitTs: 100,
			gotSyncpointOnTS:  false,
			event: dispatcher.DispatcherEvent{
				Event: &mockEvent{
					eventType: commonEvent.TypeSyncPointEvent,
					commitTs:  100,
				},
			},
			expectedResult:   true,
			expectedDDLOnTs:  false,
			expectedSyncOnTs: true,
			expectedCommitTs: 100,
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

func TestHandleDropEvent(t *testing.T) {
	localServerID := node.ID("local-server")
	remoteServerID := node.ID("remote-server")
	dispatcherID := common.NewDispatcherID()
	changefeedID := common.NewChangeFeedIDWithName("test-changefeed")

	tests := []struct {
		name             string
		event            dispatcher.DispatcherEvent
		initialState     func(*dispatcherStat)
		expectedCommitTs uint64
		expectedPanic    bool
	}{
		{
			name: "handle valid drop event",
			event: dispatcher.DispatcherEvent{
				From: &remoteServerID,
				Event: &commonEvent.DropEvent{
					Version:         commonEvent.DropEventVersion,
					DispatcherID:    dispatcherID,
					DroppedSeq:      1,
					DroppedCommitTs: 100,
				},
			},
			initialState: func(stat *dispatcherStat) {
				stat.connState.setEventServiceID(remoteServerID)
				stat.lastEventCommitTs.Store(50)
			},
			expectedCommitTs: 100,
			expectedPanic:    false,
		},
		{
			name: "panic on non-drop event",
			event: dispatcher.DispatcherEvent{
				From: &remoteServerID,
				Event: &mockEvent{
					eventType: commonEvent.TypeDMLEvent,
					commitTs:  100,
				},
			},
			initialState: func(stat *dispatcherStat) {
				stat.connState.setEventServiceID(remoteServerID)
				stat.lastEventCommitTs.Store(50)
			},
			expectedPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock dispatcher
			mockDisp := newMockDispatcher(dispatcherID, 0)
			mockDisp.changefeedID = changefeedID

			// Create dispatcherStat
			stat := &dispatcherStat{
				target:         mockDisp,
				eventCollector: newTestEventCollector(localServerID),
			}

			// Set initial state
			if tt.initialState != nil {
				tt.initialState(stat)
			}

			if tt.expectedPanic {
				require.Panics(t, func() {
					stat.handleDropEvent(tt.event)
				})
				return
			}

			stat.handleDropEvent(tt.event)

			// Verify results
			require.Equal(t, tt.expectedCommitTs, stat.lastEventCommitTs.Load())
		})
	}
}
