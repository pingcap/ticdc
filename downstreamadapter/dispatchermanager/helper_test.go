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

	removeReq := NewSchedulerDispatcherRequest("node1", &heartbeatpb.ScheduleDispatcherRequest{
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

	createReq := NewSchedulerDispatcherRequest("node1", &heartbeatpb.ScheduleDispatcherRequest{
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

func TestPreCheckForSchedulerHandler_MaintainerEpochFence(t *testing.T) {
	t.Parallel()

	dispatcherID := common.NewDispatcherID()
	currentDM := &DispatcherManager{
		changefeedID:  common.NewChangeFeedIDWithName("test-changefeed", "test-namespace"),
		dispatcherMap: newDispatcherMap[*dispatcher.EventDispatcher](),
	}
	currentDM.meta.maintainerID = "current-maintainer"
	currentDM.meta.maintainerEpoch = 2

	newReq := func(epoch uint64) *heartbeatpb.ScheduleDispatcherRequest {
		return &heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: &heartbeatpb.ChangefeedID{Keyspace: "test-namespace", Name: "test-changefeed"},
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: dispatcherID.ToPB(),
				Mode:         0,
			},
			ScheduleAction:  heartbeatpb.ScheduleAction_Create,
			OperatorType:    heartbeatpb.OperatorType_O_Add,
			MaintainerEpoch: epoch,
		}
	}

	_, ok := preCheckForSchedulerHandler(NewSchedulerDispatcherRequest("old-maintainer", newReq(1)), currentDM)
	require.False(t, ok)

	operatorKey, ok := preCheckForSchedulerHandler(NewSchedulerDispatcherRequest("current-maintainer", newReq(2)), currentDM)
	require.True(t, ok)
	require.Equal(t, dispatcherID, operatorKey)

	_, ok = preCheckForSchedulerHandler(NewSchedulerDispatcherRequest("current-maintainer", newReq(0)), currentDM)
	require.False(t, ok)

	compatDM := &DispatcherManager{
		changefeedID:  currentDM.changefeedID,
		dispatcherMap: newDispatcherMap[*dispatcher.EventDispatcher](),
	}
	compatDM.meta.maintainerID = "current-maintainer"
	operatorKey, ok = preCheckForSchedulerHandler(NewSchedulerDispatcherRequest("current-maintainer", newReq(0)), compatDM)
	require.True(t, ok)
	require.Equal(t, dispatcherID, operatorKey)

	currentDM.currentOperatorMap.Store(dispatcherID, NewSchedulerDispatcherRequest("old-maintainer", newReq(1)))
	operatorKey, ok = preCheckForSchedulerHandler(NewSchedulerDispatcherRequest("current-maintainer", newReq(2)), currentDM)
	require.True(t, ok)
	require.Equal(t, dispatcherID, operatorKey)
	_, exists := currentDM.currentOperatorMap.Load(dispatcherID)
	require.False(t, exists)
}

func TestDispatcherManagerTryUpdateMaintainerEpoch(t *testing.T) {
	t.Parallel()

	strictDM := &DispatcherManager{}
	strictDM.meta.maintainerID = "current-maintainer"
	strictDM.meta.maintainerEpoch = 2

	require.False(t, strictDM.CanUpdateMaintainer("current-maintainer", 0))
	require.Equal(t, node.ID("current-maintainer"), strictDM.GetMaintainerID())
	require.Equal(t, uint64(2), strictDM.GetMaintainerEpoch())
	require.True(t, strictDM.CanUpdateMaintainer("new-maintainer", 3))
	require.Equal(t, node.ID("current-maintainer"), strictDM.GetMaintainerID())
	require.Equal(t, uint64(2), strictDM.GetMaintainerEpoch())
	require.False(t, strictDM.TryUpdateMaintainer("current-maintainer", 0))
	require.Equal(t, node.ID("current-maintainer"), strictDM.GetMaintainerID())
	require.Equal(t, uint64(2), strictDM.GetMaintainerEpoch())

	compatDM := &DispatcherManager{}
	compatDM.meta.maintainerID = "old-maintainer"

	require.True(t, compatDM.TryUpdateMaintainer("new-maintainer", 0))
	require.Equal(t, node.ID("new-maintainer"), compatDM.GetMaintainerID())
	require.Zero(t, compatDM.GetMaintainerEpoch())
}

func TestHeartBeatResponseAllowedByMaintainerEpoch(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", "test-namespace")
	strictDM := &DispatcherManager{
		changefeedID: changefeedID,
	}
	strictDM.meta.maintainerID = "current-maintainer"
	strictDM.meta.maintainerEpoch = 2

	require.False(t, isHeartBeatResponseAllowed(strictDM, NewHeartBeatResponse(
		node.ID("old-maintainer"),
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID:    changefeedID.ToPB(),
			MaintainerEpoch: 1,
			DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
				{
					InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
						InfluenceType: heartbeatpb.InfluenceType_All,
					},
					Action: &heartbeatpb.DispatcherAction{Action: heartbeatpb.Action_Pass},
				},
			},
		},
	)))
	require.False(t, isHeartBeatResponseAllowed(strictDM, NewHeartBeatResponse(
		node.ID("current-maintainer"),
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID:    changefeedID.ToPB(),
			MaintainerEpoch: 0,
		},
	)))
	require.True(t, isHeartBeatResponseAllowed(strictDM, NewHeartBeatResponse(
		node.ID("current-maintainer"),
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID:    changefeedID.ToPB(),
			MaintainerEpoch: 2,
		},
	)))

	compatDM := &DispatcherManager{
		changefeedID: changefeedID,
	}
	compatDM.meta.maintainerID = "compat-maintainer"
	require.True(t, isHeartBeatResponseAllowed(compatDM, NewHeartBeatResponse(
		node.ID("compat-maintainer"),
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID:    changefeedID.ToPB(),
			MaintainerEpoch: 0,
		},
	)))
}

func TestHeartBeatResponseHandlerDropsStaleMaintainerEpoch(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", "test-namespace")
	dispatcherID := common.NewDispatcherID()
	dispatcherManager := &DispatcherManager{
		changefeedID: changefeedID,
	}
	dispatcherManager.meta.maintainerID = "current-maintainer"
	dispatcherManager.meta.maintainerEpoch = 2

	handler := &HeartBeatResponseHandler{}
	staleResponse := NewHeartBeatResponse(
		node.ID("old-maintainer"),
		&heartbeatpb.HeartBeatResponse{
			ChangefeedID:    changefeedID.ToPB(),
			MaintainerEpoch: 1,
			DispatcherStatuses: []*heartbeatpb.DispatcherStatus{
				{
					InfluencedDispatchers: &heartbeatpb.InfluencedDispatchers{
						InfluenceType: heartbeatpb.InfluenceType_Normal,
						DispatcherIDs: []*heartbeatpb.DispatcherID{dispatcherID.ToPB()},
					},
					Action: &heartbeatpb.DispatcherAction{Action: heartbeatpb.Action_Pass},
				},
			},
		})

	require.NotPanics(t, func() {
		require.False(t, handler.Handle(dispatcherManager, staleResponse))
	})
}

func TestRedoControlMessagesAllowedByMaintainerEpoch(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", "test-namespace")
	strictDM := &DispatcherManager{
		changefeedID: changefeedID,
	}
	strictDM.meta.maintainerID = "current-maintainer"
	strictDM.meta.maintainerEpoch = 2

	newRedoMeta := func(from node.ID, epoch uint64) RedoMetaMessage {
		return NewRedoMetaMessage(from, &heartbeatpb.RedoMetaMessage{
			ChangefeedID:    changefeedID.ToPB(),
			CheckpointTs:    100,
			ResolvedTs:      200,
			MaintainerEpoch: epoch,
		})
	}
	newRedoResolved := func(from node.ID, epoch uint64) RedoResolvedTsForwardMessage {
		return NewRedoResolvedTsForwardMessage(from, &heartbeatpb.RedoResolvedTsForwardMessage{
			ChangefeedID:    changefeedID.ToPB(),
			ResolvedTs:      200,
			MaintainerEpoch: epoch,
		})
	}

	require.False(t, isRedoMetaMessageAllowed(strictDM, newRedoMeta("old-maintainer", 1)))
	require.True(t, isRedoMetaMessageAllowed(strictDM, newRedoMeta("current-maintainer", 2)))
	require.False(t, isRedoResolvedTsForwardMessageAllowed(strictDM, newRedoResolved("old-maintainer", 1)))
	require.True(t, isRedoResolvedTsForwardMessageAllowed(strictDM, newRedoResolved("current-maintainer", 2)))

	compatDM := &DispatcherManager{
		changefeedID: changefeedID,
	}
	compatDM.meta.maintainerID = "compat-maintainer"
	require.True(t, isRedoMetaMessageAllowed(compatDM, newRedoMeta("compat-maintainer", 0)))
}

func TestRedoResolvedTsForwardMessageHandlerDropsStaleMaintainerEpoch(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", "test-namespace")
	dispatcherManager := &DispatcherManager{
		changefeedID:  changefeedID,
		dispatcherMap: newDispatcherMap[*dispatcher.EventDispatcher](),
	}
	dispatcherManager.meta.maintainerID = "current-maintainer"
	dispatcherManager.meta.maintainerEpoch = 2
	dispatcherManager.redoGlobalTs.Store(50)

	handler := &RedoResolvedTsForwardMessageHandler{}
	newResolvedMessage := func(from node.ID, resolvedTs, epoch uint64) RedoResolvedTsForwardMessage {
		return NewRedoResolvedTsForwardMessage(from, &heartbeatpb.RedoResolvedTsForwardMessage{
			ChangefeedID:    changefeedID.ToPB(),
			ResolvedTs:      resolvedTs,
			MaintainerEpoch: epoch,
		})
	}

	require.False(t, handler.Handle(dispatcherManager, newResolvedMessage("old-maintainer", 100, 1)))
	require.Equal(t, uint64(50), dispatcherManager.redoGlobalTs.Load())

	require.False(t, handler.Handle(dispatcherManager, newResolvedMessage("current-maintainer", 120, 2)))
	require.Equal(t, uint64(120), dispatcherManager.redoGlobalTs.Load())
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
