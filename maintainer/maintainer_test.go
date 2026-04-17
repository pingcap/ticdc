// Copyright 2024 PingCAP, Inc.
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

package maintainer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/eventservice"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type mockDispatcherManager struct {
	mc           messaging.MessageCenter
	self         node.ID
	dispatchers  []*heartbeatpb.TableSpanStatus
	msgCh        chan *messaging.TargetMessage
	maintainerID node.ID
	checkpointTs uint64
	changefeedID *heartbeatpb.ChangefeedID

	bootstrapTables []*heartbeatpb.BootstrapTableSpan
	dispatchersMap  map[heartbeatpb.DispatcherID]*heartbeatpb.TableSpanStatus
}

func MockDispatcherManager(mc messaging.MessageCenter, self node.ID) *mockDispatcherManager {
	// Keep the default allocations small: these mocks are used by multiple tests (including
	// integration-style ones that spin up several nodes). Preallocating for millions of
	// dispatchers makes unit tests unnecessarily memory-hungry and can cause CI flakiness.
	const defaultDispatcherCapacity = 1024

	m := &mockDispatcherManager{
		mc:             mc,
		dispatchers:    make([]*heartbeatpb.TableSpanStatus, 0, defaultDispatcherCapacity),
		msgCh:          make(chan *messaging.TargetMessage, 1024),
		dispatchersMap: make(map[heartbeatpb.DispatcherID]*heartbeatpb.TableSpanStatus, defaultDispatcherCapacity),
		self:           self,
	}
	mc.RegisterHandler(messaging.DispatcherManagerManagerTopic, m.recvMessages)
	mc.RegisterHandler(messaging.HeartbeatCollectorTopic, m.recvMessages)
	return m
}

func (m *mockDispatcherManager) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 1000)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-m.msgCh:
			m.handleMessage(msg)
		case <-tick.C:
			m.sendHeartbeat()
		}
	}
}

func (m *mockDispatcherManager) handleMessage(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeMaintainerBootstrapRequest:
		m.onBootstrapRequest(msg)
	case messaging.TypeMaintainerPostBootstrapRequest:
		m.onPostBootstrapRequest(msg)
	case messaging.TypeScheduleDispatcherRequest:
		m.onDispatchRequest(msg)
	case messaging.TypeMaintainerCloseRequest:
		m.onMaintainerCloseRequest(msg)
	default:
		log.Panic("unknown msg type", zap.Any("msg", msg))
	}
}

func (m *mockDispatcherManager) sendMessages(msg *heartbeatpb.HeartBeatRequest) {
	target := messaging.NewSingleTargetMessage(
		m.maintainerID,
		messaging.MaintainerManagerTopic,
		msg,
	)
	err := m.mc.SendCommand(target)
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
}

func (m *mockDispatcherManager) recvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	// receive message from maintainer
	case messaging.TypeScheduleDispatcherRequest,
		messaging.TypeMaintainerBootstrapRequest,
		messaging.TypeMaintainerPostBootstrapRequest,
		messaging.TypeMaintainerCloseRequest:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.msgCh <- msg:
		}
		return nil
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message), zap.Any("type", msg.Type))
	}
	return nil
}

func (m *mockDispatcherManager) onBootstrapRequest(msg *messaging.TargetMessage) {
	req := msg.Message[0].(*heartbeatpb.MaintainerBootstrapRequest)
	m.maintainerID = msg.From
	response := &heartbeatpb.MaintainerBootstrapResponse{
		ChangefeedID: req.ChangefeedID,
		Spans:        m.bootstrapTables,
		CheckpointTs: req.StartTs,
	}
	m.changefeedID = req.ChangefeedID
	m.checkpointTs = req.StartTs
	if req.TableTriggerEventDispatcherId != nil {
		m.dispatchersMap[*req.TableTriggerEventDispatcherId] = &heartbeatpb.TableSpanStatus{
			ID:              req.TableTriggerEventDispatcherId,
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    req.StartTs,
		}
		m.dispatchers = append(m.dispatchers, m.dispatchersMap[*req.TableTriggerEventDispatcherId])
	}
	err := m.mc.SendCommand(messaging.NewSingleTargetMessage(
		m.maintainerID,
		messaging.MaintainerManagerTopic,
		response,
	))
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
	log.Info("New maintainer online",
		zap.String("server", m.maintainerID.String()))
}

func (m *mockDispatcherManager) onPostBootstrapRequest(msg *messaging.TargetMessage) {
	req := msg.Message[0].(*heartbeatpb.MaintainerPostBootstrapRequest)
	m.maintainerID = msg.From
	response := &heartbeatpb.MaintainerPostBootstrapResponse{
		ChangefeedID:                  req.ChangefeedID,
		TableTriggerEventDispatcherId: req.TableTriggerEventDispatcherId,
		Err:                           nil,
	}
	err := m.mc.SendCommand(messaging.NewSingleTargetMessage(
		m.maintainerID,
		messaging.MaintainerManagerTopic,
		response,
	))
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
	log.Info("Post bootstrap finished",
		zap.String("server", m.maintainerID.String()))
}

func (m *mockDispatcherManager) onDispatchRequest(
	msg *messaging.TargetMessage,
) {
	request := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	if m.maintainerID != msg.From {
		log.Warn("ignore invalid maintainer id",
			zap.Any("request", request),
			zap.Any("maintainer", msg.From))
		return
	}
	if request.ScheduleAction == heartbeatpb.ScheduleAction_Create {
		if m.dispatchersMap[*request.Config.DispatcherID] != nil {
			log.Warn("dispatcher already exists",
				zap.String("from", msg.From.String()),
				zap.String("self", m.self.String()),
				zap.String("dispatcher", common.NewDispatcherIDFromPB(request.Config.DispatcherID).String()))
			return
		}
		status := &heartbeatpb.TableSpanStatus{
			ID:              request.Config.DispatcherID,
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    request.Config.StartTs,
		}
		m.dispatchers = append(m.dispatchers, status)
		m.dispatchersMap[*request.Config.DispatcherID] = status
	} else {
		dispatchers := make([]*heartbeatpb.TableSpanStatus, 0, len(m.dispatchers))
		delete(m.dispatchersMap, *request.Config.DispatcherID)
		for _, status := range m.dispatchers {
			newStatus := &heartbeatpb.TableSpanStatus{
				ID:              status.ID,
				ComponentStatus: status.ComponentStatus,
				CheckpointTs:    status.CheckpointTs,
			}
			if newStatus.ID.High != request.Config.DispatcherID.High || newStatus.ID.Low != request.Config.DispatcherID.Low {
				dispatchers = append(dispatchers, newStatus)
			} else {
				newStatus.ComponentStatus = heartbeatpb.ComponentState_Stopped
				response := &heartbeatpb.HeartBeatRequest{
					ChangefeedID: m.changefeedID,
					Watermark: &heartbeatpb.Watermark{
						CheckpointTs: m.checkpointTs,
						ResolvedTs:   m.checkpointTs,
					},
					Statuses: []*heartbeatpb.TableSpanStatus{newStatus},
				}
				m.sendMessages(response)
			}
		}
		m.dispatchers = dispatchers
	}
}

func (m *mockDispatcherManager) onMaintainerCloseRequest(msg *messaging.TargetMessage) {
	_ = m.mc.SendCommand(messaging.NewSingleTargetMessage(msg.From,
		messaging.MaintainerTopic, &heartbeatpb.MaintainerCloseResponse{
			ChangefeedID: msg.Message[0].(*heartbeatpb.MaintainerCloseRequest).ChangefeedID,
			Success:      true,
		}))
}

func (m *mockDispatcherManager) sendHeartbeat() {
	if m.maintainerID.String() != "" {
		response := &heartbeatpb.HeartBeatRequest{
			ChangefeedID: m.changefeedID,
			Watermark: &heartbeatpb.Watermark{
				CheckpointTs: m.checkpointTs,
				ResolvedTs:   m.checkpointTs,
			},
			Statuses: m.dispatchers,
		}
		m.checkpointTs++
		m.sendMessages(response)
	}
}

func TestMaintainerSchedule(t *testing.T) {
	// This test exercises a single-node maintainer lifecycle:
	// 1) Bootstrap a changefeed via the dispatcher manager mock.
	// 2) Verify all tables are scheduled to the only node.
	// 3) Remove the maintainer and ensure it can close cleanly.
	//
	// The test intentionally avoids binding any fixed TCP ports so it can run
	// reliably in sandboxed CI environments (and in parallel with other packages).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const tableSize = 100
	tables := make([]commonEvent.Table, 0, tableSize)
	for id := 1; id <= tableSize; id++ {
		tables = append(tables, commonEvent.Table{
			SchemaID:        1,
			TableID:         int64(id),
			SchemaTableName: &commonEvent.SchemaTableName{},
		})
	}

	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	// The maintainer scheduler requires a RegionCache service (used by span split
	// logic and region-count based heuristics). In unit tests we use a lightweight
	// mock to avoid talking to a real TiKV/PD.
	appcontext.SetService(appcontext.RegionCache, testutil.NewMockRegionCache())

	schemaStore := eventservice.NewMockSchemaStore()
	schemaStore.SetTables(tables)
	appcontext.SetService(appcontext.SchemaStore, schemaStore)

	n := node.NewInfo("", "")
	mc := messaging.NewMessageCenter(ctx, n.ID, config.NewDefaultMessageCenterConfig(n.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	nodeManager := watcher.NewNodeManager(nil, nil)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodeManager.GetAliveNodes()[n.ID] = n
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	dispatcherManager := MockDispatcherManager(mc, n.ID)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.ErrorIs(t, dispatcherManager.Run(ctx), context.Canceled)
	}()

	taskScheduler := threadpool.NewThreadPoolDefault()
	maintainer := NewMaintainer(cfID,
		&config.SchedulerConfig{
			CheckBalanceInterval: config.TomlDuration(time.Minute),
			AddTableBatchSize:    10000,
		},
		&config.ChangeFeedInfo{
			Config: config.GetDefaultReplicaConfig(),
		}, n, taskScheduler, 10, true, common.DefaultKeyspaceID)
	defer maintainer.Close()

	mc.RegisterHandler(messaging.MaintainerManagerTopic,
		func(ctx context.Context, msg *messaging.TargetMessage) error {
			maintainer.eventCh.In() <- &Event{
				changefeedID: cfID,
				eventType:    EventMessage,
				message:      msg,
			}
			return nil
		})

	// Mimic the maintainer manager's behavior: push an init event to trigger
	// bootstrap and scheduling logic in the main event loop.
	maintainer.pushEvent(&Event{changefeedID: cfID, eventType: EventInit})

	require.Eventually(t, func() bool {
		// Avoid reading non-atomic internal fields from the test goroutine.
		return maintainer.ddlSpan.IsWorking() && maintainer.initialized.Load()
	}, 20*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return maintainer.controller.spanController.GetReplicatingSize() == tableSize
	}, 20*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return maintainer.controller.spanController.GetTaskSizeByNodeID(n.ID) == tableSize
	}, 20*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		err := mc.SendCommand(messaging.NewSingleTargetMessage(
			n.ID,
			messaging.MaintainerManagerTopic,
			&heartbeatpb.RemoveMaintainerRequest{Id: cfID.ToPB()},
		))
		require.NoError(t, err)
		return maintainer.removed.Load() &&
			maintainer.scheduleState.Load() == int32(heartbeatpb.ComponentState_Stopped)
	}, 20*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
}

func TestMaintainer_GetMaintainerStatusUsesCommittedCheckpoint(t *testing.T) {
	testutil.SetUpTestServices(t)

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    10,
			Mode:            common.DefaultMode,
		}, "node1", false)
	spanController := span.NewController(cfID, ddlSpan, nil, nil, nil, common.DefaultKeyspaceID, common.DefaultMode)
	spanController.AdvanceMaintainerCommittedCheckpointTs(20)

	m := &Maintainer{
		changefeedID: cfID,
		controller: &Controller{
			spanController: spanController,
		},
		statusChanged: atomic.NewBool(false),
	}
	m.watermark.Watermark = &heartbeatpb.Watermark{
		CheckpointTs: 30,
		ResolvedTs:   40,
		LastSyncedTs: 50,
	}
	m.runningErrors.m = make(map[node.ID]*heartbeatpb.RunningError)

	status := m.GetMaintainerStatus()
	require.Equal(t, uint64(20), status.CheckpointTs)
	require.Equal(t, uint64(50), status.LastSyncedTs)
}

func TestMaintainerHeartbeatDuringRemovingSkipsFailoverRecovery(t *testing.T) {
	buildMaintainer := func(t *testing.T) (*Maintainer, *replica.SpanReplication, node.ID) {
		t.Helper()
		testutil.SetUpTestServices(t)

		nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
		captureID := node.ID("node1")
		nodeManager.GetAliveNodes()[captureID] = &node.Info{ID: captureID}

		cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
		ddlDispatcherID := common.NewDispatcherID()
		ddlSpan := replica.NewWorkingSpanReplication(cfID, ddlDispatcherID,
			common.DDLSpanSchemaID,
			common.KeyspaceDDLSpan(common.DefaultKeyspaceID), &heartbeatpb.TableSpanStatus{
				ID:              ddlDispatcherID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
				CheckpointTs:    10,
				Mode:            common.DefaultMode,
			}, captureID, false)
		refresher := replica.NewRegionCountRefresher(cfID, time.Minute)
		controller := NewController(cfID, 10, &mockThreadPool{},
			config.GetDefaultReplicaConfig(), ddlSpan, nil, 1000, 0, refresher, common.DefaultKeyspace, false)

		totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, 1)
		dispatcherID := common.NewDispatcherID()
		workingSpan := replica.NewWorkingSpanReplication(cfID, dispatcherID,
			1,
			&heartbeatpb.TableSpan{
				TableID:    totalSpan.TableID,
				StartKey:   totalSpan.StartKey,
				EndKey:     totalSpan.EndKey,
				KeyspaceID: common.DefaultKeyspaceID,
			}, &heartbeatpb.TableSpanStatus{
				ID:              dispatcherID.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Working,
				CheckpointTs:    10,
				Mode:            common.DefaultMode,
			}, captureID, false)
		controller.spanController.AddReplicatingSpan(workingSpan)

		m := &Maintainer{
			changefeedID:          cfID,
			controller:            controller,
			checkpointTsByCapture: newWatermarkCaptureMap(),
			redoTsByCapture:       newWatermarkCaptureMap(),
			statusChanged:         atomic.NewBool(false),
		}
		m.watermark.Watermark = &heartbeatpb.Watermark{}
		m.runningErrors.m = make(map[node.ID]*heartbeatpb.RunningError)
		m.initialized.Store(true)
		return m, workingSpan, captureID
	}

	makeHeartbeat := func(dispatcherID common.DispatcherID, from node.ID) *messaging.TargetMessage {
		req := &heartbeatpb.HeartBeatRequest{
			Watermark: &heartbeatpb.Watermark{
				CheckpointTs: 20,
				ResolvedTs:   20,
			},
			Statuses: []*heartbeatpb.TableSpanStatus{
				{
					ID:              dispatcherID.ToPB(),
					ComponentStatus: heartbeatpb.ComponentState_Stopped,
					CheckpointTs:    20,
					Mode:            common.DefaultMode,
				},
			},
		}
		return &messaging.TargetMessage{
			From:    from,
			Type:    messaging.TypeHeartBeatRequest,
			Message: []messaging.IOTypeT{req},
		}
	}

	// Normal failover recovery should still mark a non-working span absent when the runtime
	// reports Stopped but maintainer has no operator for it.
	t.Run("normal maintainer still self heals", func(t *testing.T) {
		m, workingSpan, captureID := buildMaintainer(t)

		m.onHeartbeatRequest(makeHeartbeat(workingSpan.ID, captureID))

		require.Equal(t, 1, m.controller.spanController.GetAbsentSize())
		require.Equal(t, heartbeatpb.ComponentState_Stopped, workingSpan.GetStatus().ComponentStatus)
		require.Equal(t, node.ID(""), workingSpan.GetNodeID())
	})

	// When RemoveMaintainer has started, the same late Stopped heartbeat must only update runtime
	// status bookkeeping. Re-marking the span absent here would let the scheduler recreate a
	// dispatcher while the changefeed is shutting down.
	t.Run("removing maintainer skips self healing", func(t *testing.T) {
		m, workingSpan, captureID := buildMaintainer(t)
		m.removing.Store(true)

		m.onHeartbeatRequest(makeHeartbeat(workingSpan.ID, captureID))

		require.Equal(t, 0, m.controller.spanController.GetAbsentSize())
		require.Equal(t, heartbeatpb.ComponentState_Stopped, workingSpan.GetStatus().ComponentStatus)
		require.Equal(t, captureID, workingSpan.GetNodeID())
		require.Zero(t, m.controller.operatorController.OperatorSize())
	})
}

func TestMaintainerRemovingSuppressesLegacyControlPlaneActions(t *testing.T) {
	mockMC := messaging.NewMockMessageCenter()
	nodeManager := watcher.NewNodeManager(nil, nil)
	nodeManager.GetAliveNodes()["node1"] = &node.Info{ID: "node1"}
	nodeManager.GetAliveNodes()["node2"] = &node.Info{ID: "node2"}

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	m := &Maintainer{
		changefeedID:  cfID,
		mc:            mockMC,
		nodeManager:   nodeManager,
		closedNodes:   make(map[node.ID]struct{}),
		statusChanged: atomic.NewBool(false),
		postBootstrapMsg: &heartbeatpb.MaintainerPostBootstrapRequest{
			ChangefeedID: cfID.ToPB(),
		},
	}
	m.watermark.Watermark = &heartbeatpb.Watermark{CheckpointTs: 100, ResolvedTs: 100}
	m.runningErrors.m = make(map[node.ID]*heartbeatpb.RunningError)
	m.initialized.Store(true)
	m.removing.Store(true)

	// Removing maintainer must not keep resending bootstrap/post-bootstrap or barrier traffic.
	// The only remaining control-plane action should be cascade close requests.
	m.handleResendMessage()
	require.Len(t, mockMC.GetMessageChannel(), 0)

	// Block status handling must also stop once removal starts, otherwise the old maintainer
	// can still schedule DDL-driven add/remove operations after handoff begins.
	m.onBlockStateRequest(&messaging.TargetMessage{
		From: "node1",
		Type: messaging.TypeBlockStatusRequest,
		Message: []messaging.IOTypeT{&heartbeatpb.BlockStatusRequest{
			ChangefeedID: cfID.ToPB(),
			Mode:         common.DefaultMode,
		}},
	})
	require.Len(t, mockMC.GetMessageChannel(), 0)

	m.cascadeRemoving.Store(true)
	m.handleResendMessage()
	require.Len(t, mockMC.GetMessageChannel(), 2)
	for i := 0; i < 2; i++ {
		msg := <-mockMC.GetMessageChannel()
		require.Equal(t, messaging.TypeMaintainerCloseRequest, msg.Type)
		req := msg.Message[0].(*heartbeatpb.MaintainerCloseRequest)
		require.Equal(t, cfID.ToPB(), req.ChangefeedID)
	}
}
