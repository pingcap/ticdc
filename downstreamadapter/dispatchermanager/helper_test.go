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

package dispatchermanager

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/redo"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestCheckpointTsMessageHandlerDeadlock(t *testing.T) {
	t.Parallel()

	changefeedID := &heartbeatpb.ChangefeedID{
		Keyspace: "test-namespace",
		Name:     "test-changefeed",
	}

	checkpointTsMessage := NewCheckpointTsMessage(&heartbeatpb.CheckpointTsMessage{
		ChangefeedID: changefeedID,
		CheckpointTs: 12345,
	})

	handler := &CheckpointTsMessageHandler{}

	t.Run("normal_operation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockSink := mock.NewMockSink(ctrl)
		mockSink.EXPECT().AddCheckpointTs(checkpointTsMessage.CheckpointTs).Times(1)

		dispatcherManager := &DispatcherManager{
			sink:                        mockSink,
			tableTriggerEventDispatcher: &dispatcher.EventDispatcher{},
		}

		done := make(chan bool, 1)
		go func() {
			blocking := handler.Handle(dispatcherManager, checkpointTsMessage)
			require.False(t, blocking, "Handler should not return blocking=true")
			done <- true
		}()

		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Fatal("Normal operation took too long, unexpected")
		}
	})

	t.Run("deadlock_scenario", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		blockCh := make(chan struct{})
		deadlockMockSink := mock.NewMockSink(ctrl)
		deadlockMockSink.EXPECT().AddCheckpointTs(checkpointTsMessage.CheckpointTs).Do(
			func(uint64) {
				<-blockCh
			},
		).Times(1)

		deadlockDispatcherManager := &DispatcherManager{
			sink:                        deadlockMockSink,
			tableTriggerEventDispatcher: &dispatcher.EventDispatcher{},
		}

		done := make(chan bool, 1)
		go func() {
			handler.Handle(deadlockDispatcherManager, checkpointTsMessage)
			done <- true
		}()

		select {
		case <-done:
			t.Fatal("Handler completed unexpectedly - deadlock was not reproduced")
		case <-time.After(1 * time.Second):
			t.Log("Successfully reproduced the deadlock: handler is blocked in AddCheckpointTs")
		}

		close(blockCh)
		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Fatal("handler should resume once AddCheckpointTs unblocks")
		}
	})

	t.Run("deadlock_resolve_scenario", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		deadlockMockSink := mock.NewMockSink(ctrl)
		deadlockMockSink.EXPECT().AddCheckpointTs(checkpointTsMessage.CheckpointTs).Do(
			func(uint64) {
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
					t.Fatal("context cancellation should unblock AddCheckpointTs path")
				}
			},
		).Times(1)

		deadlockDispatcherManager := &DispatcherManager{
			sink:                        deadlockMockSink,
			tableTriggerEventDispatcher: &dispatcher.EventDispatcher{},
		}

		done := make(chan bool, 1)
		go func() {
			handler.Handle(deadlockDispatcherManager, checkpointTsMessage)
			done <- true
		}()

		select {
		case <-done:
			t.Log("Handler completed normally")
		case <-time.After(1 * time.Second):
			t.Fatal("deadlock: handler is blocked in AddCheckpointTs")
		}
	})
}

func TestPreCheckForSchedulerHandler_RemoveAllowedWhenDispatcherMissing(t *testing.T) {
	t.Parallel()

	// Scenario:
	// 1) Dispatcher manager receives a Remove request for a dispatcherID that does not exist locally yet.
	//
	// Expectation:
	// preCheckForSchedulerHandler should allow the request to proceed so dispatcher manager can emit
	// a terminal (Stopped) status back to the maintainer and help it converge.
	dispatcherID := common.NewDispatcherID()
	dm := &DispatcherManager{
		changefeedID:  common.NewChangeFeedIDWithName("test-changefeed", "test-namespace"),
		dispatcherMap: newDispatcherMap[*dispatcher.EventDispatcher](),
	}

	removeReq := NewSchedulerDispatcherRequest(&heartbeatpb.ScheduleDispatcherRequest{
		ChangefeedID: &heartbeatpb.ChangefeedID{Keyspace: "test-namespace", Name: "test-changefeed"},
		Config: &heartbeatpb.DispatcherConfig{
			DispatcherID: dispatcherID.ToPB(),
			Mode:         0,
		},
		ScheduleAction: heartbeatpb.ScheduleAction_Remove,
		OperatorType:   heartbeatpb.OperatorType_O_Remove,
	})

	operatorKey, ok := preCheckForSchedulerHandler(removeReq, dm)
	require.True(t, ok)
	require.Equal(t, dispatcherID, operatorKey)
}

func TestPreCheckForSchedulerHandler_CreateSkippedWhenDispatcherExists(t *testing.T) {
	t.Parallel()

	// Scenario:
	// 1) Dispatcher manager already has a dispatcher in its local dispatcherMap (e.g. duplicate Create after retry).
	//
	// Expectation:
	// preCheckForSchedulerHandler should drop the Create request as an idempotent no-op.
	dispatcherID := common.NewDispatcherID()
	dm := &DispatcherManager{
		changefeedID:  common.NewChangeFeedIDWithName("test-changefeed", "test-namespace"),
		dispatcherMap: newDispatcherMap[*dispatcher.EventDispatcher](),
	}
	dm.dispatcherMap.Set(dispatcherID, &dispatcher.EventDispatcher{})

	createReq := NewSchedulerDispatcherRequest(&heartbeatpb.ScheduleDispatcherRequest{
		ChangefeedID: &heartbeatpb.ChangefeedID{Keyspace: "test-namespace", Name: "test-changefeed"},
		Config: &heartbeatpb.DispatcherConfig{
			DispatcherID: dispatcherID.ToPB(),
			Mode:         0,
		},
		ScheduleAction: heartbeatpb.ScheduleAction_Create,
		OperatorType:   heartbeatpb.OperatorType_O_Add,
	})

	_, ok := preCheckForSchedulerHandler(createReq, dm)
	require.False(t, ok)
}

func TestDispatcherManagerIsRedoReadyRequiresPublication(t *testing.T) {
	t.Parallel()

	dm := &DispatcherManager{
		redoEnabled:               true,
		redoDispatcherMap:         newDispatcherMap[*dispatcher.RedoDispatcher](),
		redoSink:                  &redo.Sink{},
		redoSchemaIDToDispatchers: dispatcher.NewSchemaIDToDispatchers(),
	}

	require.True(t, dm.IsRedoEnabled())
	require.False(t, dm.IsRedoReady())

	dm.redoReady.Store(true)

	require.True(t, dm.IsRedoReady())
}

func TestDispatcherManagerAcceptBootstrapSession(t *testing.T) {
	t.Parallel()

	dm := &DispatcherManager{}
	dm.meta.maintainerID = node.ID("old")
	dm.meta.maintainerSessionEpoch = 10

	accepted, reason := dm.AcceptBootstrapSession(node.ID("new"), 9)
	require.False(t, accepted)
	require.Contains(t, reason, "stale")
	require.Equal(t, uint64(10), dm.GetMaintainerSessionEpoch())

	accepted, reason = dm.AcceptBootstrapSession(node.ID("new"), 10)
	require.True(t, accepted)
	require.Equal(t, "current", reason)
	require.Equal(t, node.ID("new"), dm.GetMaintainerID())

	accepted, reason = dm.AcceptBootstrapSession(node.ID("next"), 11)
	require.True(t, accepted)
	require.Equal(t, "advance", reason)
	require.Equal(t, uint64(11), dm.GetMaintainerSessionEpoch())
	require.Equal(t, node.ID("next"), dm.GetMaintainerID())
}

func TestDispatcherManagerAcceptBootstrapSessionLegacyCompatibility(t *testing.T) {
	t.Parallel()

	dm := &DispatcherManager{}
	dm.meta.maintainerID = node.ID("current")
	dm.meta.maintainerSessionEpoch = 10

	accepted, reason := dm.AcceptBootstrapSession(node.ID("legacy"), 0)
	require.True(t, accepted)
	require.Equal(t, "legacy", reason)
	require.Equal(t, node.ID("current"), dm.GetMaintainerID())
	require.Equal(t, uint64(10), dm.GetMaintainerSessionEpoch())
}

func TestDispatcherManagerAcceptMaintainerSession(t *testing.T) {
	t.Parallel()

	dm := &DispatcherManager{}
	dm.meta.maintainerSessionEpoch = 10

	accepted, _ := dm.AcceptMaintainerSession(0)
	require.True(t, accepted)

	accepted, reason := dm.AcceptMaintainerSession(9)
	require.False(t, accepted)
	require.Contains(t, reason, "stale")

	accepted, reason = dm.AcceptMaintainerSession(10)
	require.True(t, accepted)
	require.Equal(t, "current", reason)

	accepted, reason = dm.AcceptMaintainerSession(11)
	require.False(t, accepted)
	require.Contains(t, reason, "future")
}

func TestRedoResolvedTsForwardMessageHandlerFiltersSessionEpoch(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", "test-namespace")
	handler := &RedoResolvedTsForwardMessageHandler{}
	dm := &DispatcherManager{
		changefeedID:  changefeedID,
		dispatcherMap: newDispatcherMap[*dispatcher.EventDispatcher](),
	}
	dm.meta.maintainerSessionEpoch = 10
	dm.redoGlobalTs.Store(100)

	blocking := handler.Handle(dm, NewRedoResolvedTsForwardMessage(&heartbeatpb.RedoResolvedTsForwardMessage{
		ChangefeedID: changefeedID.ToPB(),
		ResolvedTs:   150,
		SessionEpoch: 9,
	}))
	require.False(t, blocking)
	require.Equal(t, uint64(100), dm.redoGlobalTs.Load())

	blocking = handler.Handle(dm, NewRedoResolvedTsForwardMessage(&heartbeatpb.RedoResolvedTsForwardMessage{
		ChangefeedID: changefeedID.ToPB(),
		ResolvedTs:   150,
		SessionEpoch: 10,
	}))
	require.False(t, blocking)
	require.Equal(t, uint64(150), dm.redoGlobalTs.Load())
}
