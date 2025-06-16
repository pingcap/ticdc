package eventcollector

import (
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestDispatcherStat_PreCheckEvent(t *testing.T) {
	tests := []struct {
		name           string
		createEvent    func() dispatcher.DispatcherEvent
		handshaked     bool
		expectedResult bool
	}{
		{
			name: "handshake event should always pass",
			createEvent: func() dispatcher.DispatcherEvent {
				handshakeEvent := event.NewHandshakeEvent(common.NewDispatcherID(), 456, 789, nil)
				nodeID := node.NewID()
				return dispatcher.DispatcherEvent{
					From:  &nodeID,
					Event: handshakeEvent,
				}
			},
			handshaked:     false,
			expectedResult: true,
		},
		{
			name: "ready event should always pass",
			createEvent: func() dispatcher.DispatcherEvent {
				readyEvent := event.NewReadyEvent(common.NewDispatcherID())
				nodeID := node.NewID()
				return dispatcher.DispatcherEvent{
					From:  &nodeID,
					Event: &readyEvent,
				}
			},
			handshaked:     false,
			expectedResult: true,
		},
		{
			name: "not reusable event should always pass",
			createEvent: func() dispatcher.DispatcherEvent {
				notReusableEvent := event.NewNotReusableEvent(common.NewDispatcherID())
				nodeID := node.NewID()
				return dispatcher.DispatcherEvent{
					From:  &nodeID,
					Event: &notReusableEvent,
				}
			},
			handshaked:     false,
			expectedResult: true,
		},
		{
			name: "dml event should pass when handshaked",
			createEvent: func() dispatcher.DispatcherEvent {
				dmlEvent := &event.DMLEvent{
					Version:      event.DMLEventVersion,
					DispatcherID: common.NewDispatcherID(),
					CommitTs:     123,
					Seq:          1,
					State:        event.EventSenderStateNormal,
				}
				nodeID := node.NewID()
				return dispatcher.DispatcherEvent{
					From:  &nodeID,
					Event: dmlEvent,
				}
			},
			handshaked:     true,
			expectedResult: true,
		},
		{
			name: "dml event should fail when not handshaked",
			createEvent: func() dispatcher.DispatcherEvent {
				dmlEvent := &event.DMLEvent{
					Version:      event.DMLEventVersion,
					DispatcherID: common.NewDispatcherID(),
					CommitTs:     123,
					Seq:          1,
					State:        event.EventSenderStateNormal,
				}
				nodeID := node.NewID()
				return dispatcher.DispatcherEvent{
					From:  &nodeID,
					Event: dmlEvent,
				}
			},
			handshaked:     false,
			expectedResult: false,
		},
		{
			name: "ddl event should pass when handshaked",
			createEvent: func() dispatcher.DispatcherEvent {
				ddlEvent := &event.DDLEvent{
					DispatcherID: common.NewDispatcherID(),
					FinishedTs:   123,
					Seq:          1,
					State:        event.EventSenderStateNormal,
				}
				nodeID := node.NewID()
				return dispatcher.DispatcherEvent{
					From:  &nodeID,
					Event: ddlEvent,
				}
			},
			handshaked:     true,
			expectedResult: true,
		},
		{
			name: "ddl event should fail when not handshaked",
			createEvent: func() dispatcher.DispatcherEvent {
				ddlEvent := &event.DDLEvent{
					DispatcherID: common.NewDispatcherID(),
					FinishedTs:   123,
					Seq:          1,
					State:        event.EventSenderStateNormal,
				}
				nodeID := node.NewID()
				return dispatcher.DispatcherEvent{
					From:  &nodeID,
					Event: ddlEvent,
				}
			},
			handshaked:     false,
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock dispatcher
			mockDispatcher := &mockDispatcher{}

			// Create a dispatcher stat with the mock dispatcher
			stat := newDispatcherStat(
				mockDispatcher,
				nil, // eventCollector is not needed for this test
				nil, // readyCallback is not needed for this test
				0,   // memoryQuota is not needed for this test
			)

			// Set the handshaked state
			stat.handshaked.Store(tt.handshaked)

			// Create the event using the factory function
			dispatcherEvent := tt.createEvent()

			// Test the preCheckEvent function
			result := stat.preCheckEvent(dispatcherEvent)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

// mockDispatcher implements the dispatcher.EventDispatcher interface for testing
type mockDispatcher struct {
	dispatcher.EventDispatcher
}

func (m *mockDispatcher) GetStartTs() uint64 {
	return 0
}
