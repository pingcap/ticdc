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
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
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

	t.Run("normalOperation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockSink := sinkmock.NewMockSink(ctrl)
		mockSink.EXPECT().AddCheckpointTs(uint64(12345)).Times(1)
		dispatcherManager := &DispatcherManager{
			sink:                        mockSink,
			tableTriggerEventDispatcher: &dispatcher.EventDispatcher{},
		}

		done := make(chan bool, 1)
		go func() {
			done <- handler.Handle(dispatcherManager, checkpointTsMessage)
		}()

		select {
		case blocking := <-done:
			require.False(t, blocking)
		case <-time.After(time.Second):
			t.Fatal("normal operation timed out")
		}
	})

	t.Run("deadlockScenario", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockSink := sinkmock.NewMockSink(ctrl)
		blockCh := make(chan struct{})
		mockSink.EXPECT().AddCheckpointTs(uint64(12345)).DoAndReturn(func(uint64) {
			<-blockCh
		}).Times(1)

		dispatcherManager := &DispatcherManager{
			sink:                        mockSink,
			tableTriggerEventDispatcher: &dispatcher.EventDispatcher{},
		}

		done := make(chan bool, 1)
		go func() {
			done <- handler.Handle(dispatcherManager, checkpointTsMessage)
		}()

		select {
		case <-done:
			t.Fatal("handler completed unexpectedly, deadlock not reproduced")
		case <-time.After(time.Second):
		}

		close(blockCh)

		select {
		case blocking := <-done:
			require.False(t, blocking)
		case <-time.After(time.Second):
			t.Fatal("handler did not return after unblocking AddCheckpointTs")
		}
	})

	t.Run("deadlockResolveScenario", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockSink := sinkmock.NewMockSink(ctrl)
		sinkCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mockSink.EXPECT().AddCheckpointTs(uint64(12345)).DoAndReturn(func(uint64) {
			<-sinkCtx.Done()
		}).Times(1)

		dispatcherManager := &DispatcherManager{
			sink:                        mockSink,
			tableTriggerEventDispatcher: &dispatcher.EventDispatcher{},
		}
		done := make(chan bool, 1)
		go func() {
			done <- handler.Handle(dispatcherManager, checkpointTsMessage)
		}()

		select {
		case <-done:
			t.Fatal("handler completed unexpectedly before cancel")
		case <-time.After(200 * time.Millisecond):
		}

		cancel()
		select {
		case blocking := <-done:
			require.False(t, blocking)
		case <-time.After(time.Second):
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
