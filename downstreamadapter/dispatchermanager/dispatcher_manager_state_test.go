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
	"math"
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
	var wasInitializing bool
	manager, err := NewDispatcherManager(
		0, common.NewChangeFeedIDWithName("lifecycle", common.DefaultKeyspaceName),
		newStateTestConfig(), nil, nil, 1, node.ID("maintainer"), 1, false,
		func(manager *DispatcherManager) bool {
			wasInitializing = manager.isInitializing()
			return true
		},
	)
	require.NoError(t, err)
	require.True(t, wasInitializing)
	require.True(t, manager.IsRunning())
	t.Cleanup(manager.LocalFence)

	// Hold background completion so repeated close requests observe Closing.
	manager.wg.Add(1)
	release := sync.OnceFunc(manager.wg.Done)
	t.Cleanup(release)
	require.False(t, manager.TryClose(false))
	require.True(t, manager.isClosingOrClosed())
	require.False(t, manager.isClosed())
	require.False(t, manager.IsRunning())
	require.False(t, manager.TryClose(false))
	select {
	case <-manager.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel background work")
	}
	release()
	require.Eventually(t, func() bool { return manager.TryClose(false) }, time.Second, time.Millisecond)
	require.True(t, manager.isClosed())
	require.False(t, manager.IsRunning())
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
			require.True(t, manager.isInitializing())
			require.False(t, manager.IsRunning())
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
			require.True(t, manager.isClosingOrClosed())
			require.False(t, manager.isClosed())
			require.False(t, manager.TryClose(false))
			manager.LocalFence()
			unblock()
			select {
			case result := <-constructed:
				require.Nil(t, result.manager)
				require.True(t, IsWritePathClosedError(result.err))
				// Failed construction returns only after its own cleanup completes.
				require.True(t, manager.isClosed())
			case <-time.After(time.Second):
				t.Fatal("initialization did not return after shutdown")
			}
			require.Eventually(t, func() bool { return manager.TryClose(false) }, time.Second, time.Millisecond)
			require.True(t, manager.isClosed())
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
			require.True(t, initializing.isClosed())
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
		require.False(t, manager.IsRunning())
	}
}

func TestDispatcherManagerTasksStopDuringClosing(t *testing.T) {
	for _, state := range []dispatcherManagerState{dispatcherManagerClosing, dispatcherManagerClosed} {
		manager := &DispatcherManager{}
		manager.lifecycleMu.Lock()
		manager.state = state
		manager.lifecycleMu.Unlock()
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

func TestDispatcherManagerHeartbeatSurvivesInitialization(t *testing.T) {
	manager := createTestManager(t)
	manager.config.PerformanceMode = config.PerformanceModeLowLatency
	task := &HeartBeatTask{manager: manager}
	var tasks sync.WaitGroup
	t.Cleanup(tasks.Wait)
	for range 10000 {
		manager.lifecycleMu.Lock()
		manager.state = dispatcherManagerInitializing
		manager.lifecycleMu.Unlock()
		started := make(chan struct{})
		next := make(chan time.Time, 1)
		tasks.Go(func() {
			close(started)
			next <- task.Execute()
		})
		<-started
		manager.lifecycleMu.Lock()
		manager.state = dispatcherManagerRunning
		manager.lifecycleMu.Unlock()
		require.False(t, (<-next).IsZero(), "initialization must not retire the heartbeat task")
	}
}

func TestDispatcherManagerCloseUnblocksFullHeartbeatQueue(t *testing.T) {
	manager := createTestManager(t)
	manager.ctx, manager.cancel = context.WithCancel(t.Context())
	queue := &HeartbeatRequestQueue{queue: make(chan *HeartBeatRequestWithTargetID, 1)}
	queue.queue <- &HeartBeatRequestWithTargetID{}
	manager.heartbeatRequestQueue = queue
	collector := &HeartBeatCollector{}
	collector.isClosed.Store(true)
	appcontext.SetService(appcontext.HeartbeatCollector, collector)
	done := make(chan struct{})
	var next time.Time
	go func() {
		next = (&HeartBeatTask{manager: manager}).Execute()
		close(done)
	}()
	t.Cleanup(func() {
		manager.cancel()
		select {
		case <-queue.queue:
		default:
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("heartbeat task did not return")
		}
	})
	// Aggregation publishes the watermark after the task has joined manager.wg.
	require.Eventually(t, func() bool {
		return manager.latestWatermark.Get().CheckpointTs == math.MaxUint64
	}, time.Second, time.Millisecond)
	require.False(t, manager.TryClose(false))
	require.Eventually(t, manager.isClosed, time.Second, time.Millisecond)
	<-done
	require.Zero(t, next)
	require.True(t, manager.TryClose(false))
}

func TestDispatcherManagerCloseWaitsForRunningTask(t *testing.T) {
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
		t.Fatal("checkpoint handler did not start")
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
				t.Error("close succeeded before the running handler returned")
			}
			manager.LocalFence()
		})
	}
	closers.Wait()
	require.True(t, manager.isClosingOrClosed())
	require.False(t, manager.isClosed())
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
	require.True(t, manager.isClosingOrClosed())
	require.False(t, manager.isClosed())
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
