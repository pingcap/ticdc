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

package dispatchermanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func newStateTestConfig() *config.ChangefeedConfig {
	replicaConfig := config.GetDefaultReplicaConfig()
	return &config.ChangefeedConfig{
		SinkURI:     "blackhole://",
		SinkConfig:  replicaConfig.Sink,
		Filter:      replicaConfig.Filter,
		MemoryQuota: util.GetOrZero(replicaConfig.MemoryQuota),
		TimeZone:    "system",
		Consistent:  replicaConfig.Consistent,
	}
}

func TestDispatcherManagerLifecycle(t *testing.T) {
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())
	collector := &HeartBeatCollector{}
	collector.isClosed.Store(true)
	appcontext.SetService(appcontext.HeartbeatCollector, collector)
	var initialState dispatcherManagerState
	manager, err := NewDispatcherManager(
		0, common.NewChangeFeedIDWithName("lifecycle", common.DefaultKeyspaceName),
		newStateTestConfig(), nil, nil, 1, node.ID("maintainer"), 1, false,
		func(manager *DispatcherManager) bool {
			initialState = manager.getState()
			return true
		},
	)
	require.NoError(t, err)
	require.Equal(t, dispatcherManagerInitializing, initialState)
	require.Equal(t, dispatcherManagerRunning, manager.getState())
	require.True(t, manager.IsReusable())
	t.Cleanup(manager.LocalFence)

	// Hold background completion so repeated close requests observe Closing.
	manager.wg.Add(1)
	release := sync.OnceFunc(manager.wg.Done)
	t.Cleanup(release)
	require.False(t, manager.TryClose(false))
	require.Equal(t, dispatcherManagerClosing, manager.getState())
	require.False(t, manager.IsReusable())
	require.False(t, manager.TryClose(false))
	select {
	case <-manager.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel background work")
	}
	release()
	require.Eventually(t, func() bool { return manager.TryClose(false) }, time.Second, time.Millisecond)
	require.Equal(t, dispatcherManagerClosed, manager.getState())
	require.False(t, manager.IsReusable())
}

func TestDispatcherManagerCloseDuringInitialization(t *testing.T) {
	for _, localFence := range []bool{false, true} {
		name := "close request"
		if localFence {
			name = "local fence"
		}
		t.Run(name, func(t *testing.T) {
			appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())
			registered := make(chan *DispatcherManager, 1)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			type result struct {
				manager *DispatcherManager
				err     error
			}
			constructed := make(chan result, 1)
			go func() {
				manager, err := NewDispatcherManager(
					0, common.NewChangeFeedIDWithName("initializing", common.DefaultKeyspaceName),
					newStateTestConfig(), nil, nil, 1, node.ID("maintainer"), 1, false,
					func(manager *DispatcherManager) bool {
						registered <- manager
						<-release
						return true
					},
				)
				constructed <- result{manager, err}
			}()
			var manager *DispatcherManager
			select {
			case manager = <-registered:
			case <-time.After(time.Second):
				t.Fatal("initializing manager was not registered")
			}
			require.Equal(t, dispatcherManagerInitializing, manager.getState())
			require.False(t, manager.IsReusable())
			// Runtime requests must not access the constructor's partial resources.
			handler := &SchedulerDispatcherRequestHandler{}
			require.False(t, handler.Handle(manager, NewSchedulerDispatcherRequest("maintainer", &heartbeatpb.ScheduleDispatcherRequest{
				ChangefeedID: manager.changefeedID.ToPB(), MaintainerEpoch: 1,
				ScheduleAction: heartbeatpb.ScheduleAction_Create,
				Config: &heartbeatpb.DispatcherConfig{
					DispatcherID: common.NewDispatcherID().ToPB(),
					Span:         &heartbeatpb.TableSpan{TableID: 1},
				},
			})))
			require.Zero(t, manager.dispatcherMap.Len())
			if localFence {
				manager.LocalFence()
			} else {
				require.False(t, manager.TryClose(false))
			}
			select {
			case <-manager.ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("shutdown did not cancel initialization")
			}
			require.ErrorIs(t, manager.ctx.Err(), context.Canceled)
			require.Equal(t, dispatcherManagerClosing, manager.getState())
			require.False(t, manager.TryClose(false))
			manager.LocalFence()
			unblock()
			select {
			case result := <-constructed:
				require.Nil(t, result.manager)
				require.True(t, IsWritePathClosedError(result.err))
				// Failed construction returns only after its own cleanup completes.
				require.Equal(t, dispatcherManagerClosed, manager.getState())
			case <-time.After(time.Second):
				t.Fatal("initialization did not return after shutdown")
			}
			require.Eventually(t, func() bool { return manager.TryClose(false) }, time.Second, time.Millisecond)
			require.Equal(t, dispatcherManagerClosed, manager.getState())
		})
	}
}

func TestDispatcherManagerInitializationFailure(t *testing.T) {
	for _, withSink := range []bool{false, true} {
		name := "sink creation"
		if withSink {
			name = "after sink creation"
		}
		t.Run(name, func(t *testing.T) {
			appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())
			collector := &HeartBeatCollector{}
			collector.isClosed.Store(true)
			appcontext.SetService(appcontext.HeartbeatCollector, collector)
			cfg := newStateTestConfig()
			if withSink {
				cfg.SinkConfig.DispatchRules = []*config.DispatchRule{{
					Matcher: []string{"[invalid"}, TargetSchema: "target",
				}}
			} else {
				cfg.SinkURI = "invalid://"
			}
			var initializing *DispatcherManager
			manager, err := NewDispatcherManager(
				0, common.NewChangeFeedIDWithName("failed-init", common.DefaultKeyspaceName),
				cfg, nil, nil, 1, node.ID("maintainer"), 1, false,
				func(manager *DispatcherManager) bool {
					initializing = manager
					return true
				},
			)
			require.Error(t, err)
			require.Nil(t, manager)
			require.Equal(t, withSink, initializing.sink != nil)
			require.Equal(t, dispatcherManagerClosed, initializing.getState())
			require.ErrorIs(t, initializing.ctx.Err(), context.Canceled)
			require.True(t, initializing.TryClose(false))
		})
	}
}

func TestDispatcherManagerCloseRacesWithInitialization(t *testing.T) {
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())
	collector := &HeartBeatCollector{}
	collector.isClosed.Store(true)
	appcontext.SetService(appcontext.HeartbeatCollector, collector)
	for i := range 20 {
		registered := make(chan *DispatcherManager, 1)
		release := make(chan struct{})
		done := make(chan struct{})
		var constructed *DispatcherManager
		var err error
		go func() {
			constructed, err = NewDispatcherManager(
				0, common.NewChangeFeedIDWithName("init-race", common.DefaultKeyspaceName),
				newStateTestConfig(), nil, nil, 1, node.ID("maintainer"), 1, false,
				func(manager *DispatcherManager) bool {
					registered <- manager
					<-release
					return true
				},
			)
			close(done)
		}()
		manager := <-registered
		// Race resource construction and the final Running publication with close.
		close(release)
		if i%2 == 0 {
			manager.TryClose(false)
		} else {
			manager.LocalFence()
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("construction did not return after close")
		}
		if err != nil {
			require.True(t, IsWritePathClosedError(err))
			require.Nil(t, constructed)
		} else {
			require.Same(t, manager, constructed)
		}
		require.Eventually(t, func() bool { return manager.TryClose(false) }, time.Second, time.Millisecond)
		require.False(t, manager.IsReusable())
	}
}

func TestDispatcherManagerTasksStopDuringClosing(t *testing.T) {
	for _, state := range []dispatcherManagerState{dispatcherManagerClosing, dispatcherManagerClosed} {
		manager := &DispatcherManager{}
		manager.state.Store(uint32(state))
		require.Zero(t, (&HeartBeatTask{manager: manager}).Execute())
		require.Zero(t, (&MergeCheckTask{manager: manager}).Execute())
		require.Zero(t, (&RemoveDispatcherTask{manager: manager}).Execute())
	}
}

func TestDispatcherManagerHeartbeatWaitsForRunning(t *testing.T) {
	manager := &DispatcherManager{config: &config.ChangefeedConfig{PerformanceMode: config.PerformanceModeLowLatency}}
	// The task must stay scheduled without reading partially initialized fields.
	require.False(t, (&HeartBeatTask{manager: manager}).Execute().IsZero())
}

func TestDispatcherManagerCloseWaitsForAdmittedWork(t *testing.T) {
	manager := createTestManager(t)
	ctx, cancel := context.WithCancel(t.Context())
	manager.ctx, manager.cancel = ctx, cancel
	collector := &HeartBeatCollector{}
	collector.isClosed.Store(true)
	appcontext.SetService(appcontext.HeartbeatCollector, collector)
	manager.SetTableTriggerEventDispatcher(&dispatcher.EventDispatcher{})
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	sinkClosed := make(chan struct{})
	ctrl := gomock.NewController(t)
	sink := mock.NewMockSink(ctrl)
	sink.EXPECT().SinkType().Return(common.BlackHoleSinkType).AnyTimes()
	sink.EXPECT().Close().Do(func() { close(sinkClosed) }).Times(1)
	sink.EXPECT().AddCheckpointTs(uint64(100)).Do(func(uint64) {
		close(started)
		<-release
	}).Times(1)
	manager.sink = sink

	handler := &CheckpointTsMessageHandler{}
	message := NewCheckpointTsMessage(&heartbeatpb.CheckpointTsMessage{CheckpointTs: 100})
	done := make(chan struct{})
	go func() {
		handler.Handle(manager, message)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("checkpoint work was not admitted")
	}
	require.False(t, manager.TryClose(false))
	select {
	case <-sinkClosed:
	case <-time.After(time.Second):
		t.Fatal("sink close did not start")
	}

	// Repeated requests cannot finish or duplicate cleanup while work is running.
	var closers sync.WaitGroup
	for i := range 8 {
		closers.Go(func() {
			if manager.TryClose(i == 0) {
				t.Error("close succeeded before admitted work returned")
			}
			manager.LocalFence()
		})
	}
	closers.Wait()
	require.Equal(t, dispatcherManagerClosing, manager.getState())
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.False(t, handler.Handle(manager, message))
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("checkpoint work did not return")
	}
	require.Eventually(t, func() bool { return manager.TryClose(false) }, time.Second, time.Millisecond)
	require.True(t, manager.removeChangefeedRequested.Load())
	require.Eventually(t, manager.removeChangefeedCleaned.Load, time.Second, time.Millisecond)
}

func TestDispatcherManagerLocalFenceStopsInitializedWritePath(t *testing.T) {
	manager := createTestManager(t)
	ctx, cancel := context.WithCancel(t.Context())
	manager.ctx, manager.cancel = ctx, cancel
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	ctrl := gomock.NewController(t)
	sink := mock.NewMockSink(ctrl)
	sink.EXPECT().SinkType().Return(common.BlackHoleSinkType)
	sink.EXPECT().Close().DoAndReturn(func() {
		close(started)
		<-release
	})
	manager.sink = sink
	fenced := make(chan struct{})
	go func() {
		manager.LocalFence()
		close(fenced)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("local fence did not stop the sink")
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, dispatcherManagerClosing, manager.getState())
	select {
	case <-fenced:
		t.Fatal("local fence returned before its write path stopped")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case <-fenced:
	case <-time.After(time.Second):
		t.Fatal("local fence did not return after its write path stopped")
	}
	require.Eventually(t, func() bool { return manager.TryClose(false) }, time.Second, time.Millisecond)
}
