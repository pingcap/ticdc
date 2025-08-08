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

package coordinator

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	mock_changefeed "github.com/pingcap/ticdc/coordinator/changefeed/mock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type mockPdClient struct {
	pd.Client
}

func (m *mockPdClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return safePoint, nil
}

type mockMaintainerManager struct {
	mc                 messaging.MessageCenter
	msgCh              chan *messaging.TargetMessage
	coordinatorVersion int64
	coordinatorID      node.ID
	maintainers        []*heartbeatpb.MaintainerStatus
	maintainerMap      map[common.ChangeFeedID]*heartbeatpb.MaintainerStatus
	bootstrapResponse  *heartbeatpb.CoordinatorBootstrapResponse
}

func NewMaintainerManager(mc messaging.MessageCenter) *mockMaintainerManager {
	m := &mockMaintainerManager{
		mc:            mc,
		maintainers:   make([]*heartbeatpb.MaintainerStatus, 0, 1000000),
		maintainerMap: make(map[common.ChangeFeedID]*heartbeatpb.MaintainerStatus, 1000000),
		msgCh:         make(chan *messaging.TargetMessage, 1024),
	}
	mc.RegisterHandler(messaging.MaintainerManagerTopic, m.recvMessages)
	return m
}

func (m *mockMaintainerManager) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 50)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-m.msgCh:
			m.handleMessage(msg)
		case <-tick.C:
			// 1.  try to send heartbeat to coordinator
			m.sendHeartbeat()
		}
	}
}

func (m *mockMaintainerManager) handleMessage(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeCoordinatorBootstrapRequest:
		m.onCoordinatorBootstrapRequest(msg)
	case messaging.TypeAddMaintainerRequest, messaging.TypeRemoveMaintainerRequest:
		absent := m.onDispatchMaintainerRequest(msg)
		if m.coordinatorVersion > 0 {
			response := &heartbeatpb.MaintainerHeartbeat{}
			if absent != nil {
				response.Statuses = append(response.Statuses, &heartbeatpb.MaintainerStatus{
					ChangefeedID: absent,
					State:        heartbeatpb.ComponentState_Stopped,
				})
			}
			if len(response.Statuses) != 0 {
				m.sendMessages(response)
			}
		}
	}
}

func (m *mockMaintainerManager) sendMessages(msg *heartbeatpb.MaintainerHeartbeat) {
	target := messaging.NewSingleTargetMessage(
		m.coordinatorID,
		messaging.CoordinatorTopic,
		msg,
	)
	err := m.mc.SendCommand(target)
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
}

func (m *mockMaintainerManager) recvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	// receive message from coordinator
	case messaging.TypeAddMaintainerRequest, messaging.TypeRemoveMaintainerRequest:
		fallthrough
	case messaging.TypeCoordinatorBootstrapRequest:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.msgCh <- msg:
		}
		return nil
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message))
	}
	return nil
}

func (m *mockMaintainerManager) onCoordinatorBootstrapRequest(msg *messaging.TargetMessage) {
	req := msg.Message[0].(*heartbeatpb.CoordinatorBootstrapRequest)
	if m.coordinatorVersion > req.Version {
		log.Warn("ignore invalid coordinator version",
			zap.Int64("version", req.Version))
		return
	}
	m.coordinatorID = msg.From
	m.coordinatorVersion = req.Version

	response := m.bootstrapResponse
	if response == nil {
		response = &heartbeatpb.CoordinatorBootstrapResponse{}
	}
	err := m.mc.SendCommand(messaging.NewSingleTargetMessage(
		m.coordinatorID,
		messaging.CoordinatorTopic,
		response,
	))
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
	log.Info("New coordinator online",
		zap.Int64("version", m.coordinatorVersion))
}

func (m *mockMaintainerManager) onDispatchMaintainerRequest(
	msg *messaging.TargetMessage,
) *heartbeatpb.ChangefeedID {
	if m.coordinatorID != msg.From {
		log.Warn("ignore invalid coordinator id",
			zap.Any("coordinator", msg.From),
			zap.Any("request", msg))
		return nil
	}
	if msg.Type == messaging.TypeAddMaintainerRequest {
		req := msg.Message[0].(*heartbeatpb.AddMaintainerRequest)
		cfID := common.NewChangefeedIDFromPB(req.GetId())
		cf, ok := m.maintainerMap[cfID]
		if !ok {
			cfConfig := &config.ChangeFeedInfo{}
			err := json.Unmarshal(req.Config, cfConfig)
			if err != nil {
				log.Panic("decode changefeed fail", zap.Error(err))
			}
			cf = &heartbeatpb.MaintainerStatus{
				ChangefeedID: req.GetId(),
				FeedState:    "normal",
				State:        heartbeatpb.ComponentState_Working,
				CheckpointTs: req.CheckpointTs,
			}
			m.maintainerMap[cfID] = cf
			m.maintainers = append(m.maintainers, cf)
		}
	} else {
		req := msg.Message[0].(*heartbeatpb.RemoveMaintainerRequest)
		maintainers := make([]*heartbeatpb.MaintainerStatus, 0, len(m.maintainers))
		delete(m.maintainerMap, common.NewChangefeedIDFromPB(req.GetId()))
		for _, status := range m.maintainerMap {
			maintainers = append(maintainers, status)
		}
		m.maintainers = maintainers
		return req.GetId()
	}
	return nil
}

func (m *mockMaintainerManager) sendHeartbeat() {
	if m.coordinatorVersion > 0 {
		response := &heartbeatpb.MaintainerHeartbeat{}
		response.Statuses = m.maintainers
		if len(response.Statuses) != 0 {
			m.sendMessages(response)
		}
	}
}

func TestCoordinatorScheduling(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	go func() {
		t.Fatal(http.ListenAndServe(":38300", mux))
	}()

	ctx := context.Background()
	info := node.NewInfo("127.0.0.1:8700", "")
	etcdClient := newMockEtcdClient(string(info.ID))
	nodeManager := watcher.NewNodeManager(nil, etcdClient)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	nodeManager.GetAliveNodes()[info.ID] = info
	mc := messaging.NewMessageCenter(ctx,
		info.ID, config.NewDefaultMessageCenterConfig(info.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()

	appcontext.SetService(appcontext.MessageCenter, mc)
	m := NewMaintainerManager(mc)
	go m.Run(ctx)

	if !flag.Parsed() {
		flag.Parse()
	}

	argList := flag.Args()
	if len(argList) > 1 {
		t.Fatal("unexpected args", argList)
	}
	cfSize := 100
	waitTime := 10
	if len(argList) == 1 {
		cfSize, _ = strconv.Atoi(argList[0])
	}

	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	cfs := make(map[common.ChangeFeedID]*changefeed.ChangefeedMetaWrapper)
	backend.EXPECT().GetAllChangefeeds(gomock.Any()).Return(cfs, nil).AnyTimes()
	for i := 0; i < cfSize; i++ {
		cfID := common.NewChangeFeedIDWithDisplayName(common.ChangeFeedDisplayName{
			Name:      fmt.Sprintf("%d", i),
			Namespace: common.DefaultNamespace,
		})
		cfs[cfID] = &changefeed.ChangefeedMetaWrapper{
			Info: &config.ChangeFeedInfo{
				ChangefeedID: cfID,
				Config:       config.GetDefaultReplicaConfig(),
				State:        config.StateNormal,
			},
			Status: &config.ChangeFeedStatus{CheckpointTs: 10},
		}
	}

	cr := New(info, &mockPdClient{}, backend, "default", 100, 10000, time.Minute)
	co := cr.(*coordinator)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = cr.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		return co.controller.changefeedDB.GetReplicatingSize() == cfSize
	}, time.Second*time.Duration(waitTime), time.Millisecond*5)
	require.Eventually(t, func() bool {
		return len(co.controller.changefeedDB.GetByNodeID(info.ID)) == cfSize
	}, time.Second*time.Duration(waitTime), time.Millisecond*5)
}

func TestScaleNode(t *testing.T) {
	ctx := context.Background()
	info := node.NewInfo("127.0.0.1:28300", "")
	etcdClient := newMockEtcdClient(string(info.ID))
	nodeManager := watcher.NewNodeManager(nil, etcdClient)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodeManager.GetAliveNodes()[info.ID] = info
	cfg := config.NewDefaultMessageCenterConfig(info.AdvertiseAddr)
	mc1 := messaging.NewMessageCenter(ctx, info.ID, cfg, nil)
	mc1.Run(ctx)
	defer func() {
		mc1.Close()
		log.Info("close message center 1")
	}()

	appcontext.SetService(appcontext.MessageCenter, mc1)
	startMaintainerNode(ctx, info, mc1, nodeManager)

	serviceID := "default"

	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	cfs := make(map[common.ChangeFeedID]*changefeed.ChangefeedMetaWrapper)
	changefeedNumber := 6
	for i := 0; i < changefeedNumber; i++ {
		cfID := common.NewChangeFeedIDWithDisplayName(common.ChangeFeedDisplayName{
			Name:      fmt.Sprintf("%d", i),
			Namespace: common.DefaultNamespace,
		})
		cfs[cfID] = &changefeed.ChangefeedMetaWrapper{
			Info: &config.ChangeFeedInfo{
				ChangefeedID: cfID,
				Config:       config.GetDefaultReplicaConfig(),
				State:        config.StateNormal,
			},
			Status: &config.ChangeFeedStatus{CheckpointTs: 10},
		}
	}
	backend.EXPECT().GetAllChangefeeds(gomock.Any()).Return(cfs, nil).AnyTimes()

	cr := New(info, &mockPdClient{}, backend, serviceID, 100, 10000, time.Millisecond*1)

	// run coordinator
	go func() { cr.Run(ctx) }()

	co := cr.(*coordinator)
	waitTime := time.Second * 10

	require.Eventually(t, func() bool {
		return co.controller.changefeedDB.GetReplicatingSize() == changefeedNumber
	}, waitTime, time.Millisecond*5)

	// add two nodes
	info2 := node.NewInfo("127.0.0.1:28400", "")
	mc2 := messaging.NewMessageCenter(ctx, info2.ID, config.NewDefaultMessageCenterConfig(info2.AdvertiseAddr), nil)
	mc2.Run(ctx)
	defer func() {
		mc2.Close()
		log.Info("close message center 2")
	}()
	startMaintainerNode(ctx, info2, mc2, nodeManager)
	info3 := node.NewInfo("127.0.0.1:28500", "")
	mc3 := messaging.NewMessageCenter(ctx, info3.ID, config.NewDefaultMessageCenterConfig(info3.AdvertiseAddr), nil)
	mc3.Run(ctx)
	defer func() {
		mc3.Close()
		log.Info("close message center 3")
	}()

	startMaintainerNode(ctx, info3, mc3, nodeManager)

	log.Info("Start maintainer node",
		zap.Stringer("id", info3.ID),
		zap.String("addr", info3.AdvertiseAddr))

	// notify node changes
	_, _ = nodeManager.Tick(ctx, &orchestrator.GlobalReactorState{
		Captures: map[config.CaptureID]*config.CaptureInfo{
			config.CaptureID(info.ID):  {ID: config.CaptureID(info.ID), AdvertiseAddr: info.AdvertiseAddr},
			config.CaptureID(info2.ID): {ID: config.CaptureID(info2.ID), AdvertiseAddr: info2.AdvertiseAddr},
			config.CaptureID(info3.ID): {ID: config.CaptureID(info3.ID), AdvertiseAddr: info3.AdvertiseAddr},
		},
	})

	require.Eventually(t, func() bool {
		return co.controller.changefeedDB.GetReplicatingSize() == changefeedNumber
	}, waitTime, time.Millisecond*5)
	require.Eventually(t, func() bool {
		return len(co.controller.changefeedDB.GetByNodeID(info.ID)) == 2 &&
			len(co.controller.changefeedDB.GetByNodeID(info2.ID)) == 2 &&
			len(co.controller.changefeedDB.GetByNodeID(info3.ID)) == 2
	}, waitTime, time.Millisecond*5)

	// notify node changes
	_, _ = nodeManager.Tick(ctx, &orchestrator.GlobalReactorState{
		Captures: map[config.CaptureID]*config.CaptureInfo{
			config.CaptureID(info.ID):  {ID: config.CaptureID(info.ID), AdvertiseAddr: info.AdvertiseAddr},
			config.CaptureID(info2.ID): {ID: config.CaptureID(info2.ID), AdvertiseAddr: info2.AdvertiseAddr},
		},
	})

	require.Eventually(t, func() bool {
		return co.controller.changefeedDB.GetReplicatingSize() == changefeedNumber
	}, waitTime, time.Millisecond*5)
	require.Eventually(t, func() bool {
		return len(co.controller.changefeedDB.GetByNodeID(info.ID)) == 3 &&
			len(co.controller.changefeedDB.GetByNodeID(info2.ID)) == 3
	}, waitTime, time.Millisecond*5)

	log.Info("pass scale node")
}

func TestBootstrapWithUnStoppedChangefeed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	info := node.NewInfo("127.0.0.1:28301", "")
	etcdClient := newMockEtcdClient(string(info.ID))
	nodeManager := watcher.NewNodeManager(nil, etcdClient)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodeManager.GetAliveNodes()[info.ID] = info

	mc1 := messaging.NewMessageCenter(ctx, info.ID, config.NewDefaultMessageCenterConfig(info.AdvertiseAddr), nil)
	mc1.Run(ctx)
	defer mc1.Close()

	appcontext.SetService(appcontext.MessageCenter, mc1)
	mNode := startMaintainerNode(ctx, info, mc1, nodeManager)

	removingCf1 := &changefeed.ChangefeedMetaWrapper{
		Info: &config.ChangeFeedInfo{
			ChangefeedID: common.NewChangeFeedIDWithName("cf1"),
			Config:       config.GetDefaultReplicaConfig(),
			State:        config.StateNormal,
		},
		Status: &config.ChangeFeedStatus{CheckpointTs: 10, Progress: config.ProgressRemoving},
	}
	removingCf2 := &changefeed.ChangefeedMetaWrapper{
		Info: &config.ChangeFeedInfo{
			ChangefeedID: common.NewChangeFeedIDWithName("cf2"),
			Config:       config.GetDefaultReplicaConfig(),
			State:        config.StateNormal,
		},
		Status: &config.ChangeFeedStatus{CheckpointTs: 10, Progress: config.ProgressRemoving},
	}
	stopingCf1 := &changefeed.ChangefeedMetaWrapper{
		Info: &config.ChangeFeedInfo{
			ChangefeedID: common.NewChangeFeedIDWithName("cf1"),
			Config:       config.GetDefaultReplicaConfig(),
			State:        config.StateStopped,
		},
		Status: &config.ChangeFeedStatus{CheckpointTs: 10, Progress: config.ProgressStopping},
	}

	stopingCf2 := &changefeed.ChangefeedMetaWrapper{
		Info: &config.ChangeFeedInfo{
			ChangefeedID: common.NewChangeFeedIDWithName("cf2"),
			Config:       config.GetDefaultReplicaConfig(),
			State:        config.StateStopped,
		},
		Status: &config.ChangeFeedStatus{CheckpointTs: 10, Progress: config.ProgressStopping},
	}

	// two changefeeds are working
	mNode.manager.bootstrapResponse = &heartbeatpb.CoordinatorBootstrapResponse{
		Statuses: []*heartbeatpb.MaintainerStatus{
			{
				ChangefeedID: removingCf1.Info.ChangefeedID.ToPB(),
				State:        heartbeatpb.ComponentState_Working,
			},
			{
				ChangefeedID: stopingCf1.Info.ChangefeedID.ToPB(),
				State:        heartbeatpb.ComponentState_Working,
			},
		},
	}

	serviceID := "default"
	ctrl := gomock.NewController(t)
	backend := mock_changefeed.NewMockBackend(ctrl)
	backend.EXPECT().GetAllChangefeeds(gomock.Any()).Return(map[common.ChangeFeedID]*changefeed.ChangefeedMetaWrapper{
		removingCf1.Info.ChangefeedID: removingCf1,
		removingCf2.Info.ChangefeedID: removingCf2,
		stopingCf1.Info.ChangefeedID:  stopingCf1,
		stopingCf2.Info.ChangefeedID:  stopingCf2,
	}, nil).AnyTimes()
	backend.EXPECT().DeleteChangefeed(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	backend.EXPECT().SetChangefeedProgress(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	cr := New(info, &mockPdClient{}, backend, serviceID, 100, 10000, time.Millisecond*10)

	// run coordinator
	go func() { cr.Run(ctx) }()

	co := cr.(*coordinator)
	waitTime := time.Second * 10
	require.Eventually(t, func() bool {
		return co.controller.changefeedDB.GetReplicatingSize() == 0 &&
			co.controller.changefeedDB.GetStoppedSize() == 2 &&
			co.controller.operatorController.OperatorSize() == 0
	}, waitTime, time.Millisecond*5)
}

func TestConcurrentStopAndSendEvents(t *testing.T) {
	// Setup context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize node info
	info := node.NewInfo("127.0.0.1:28600", "")
	etcdClient := newMockEtcdClient(string(info.ID))
	nodeManager := watcher.NewNodeManager(nil, etcdClient)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	nodeManager.GetAliveNodes()[info.ID] = info

	// Initialize message center
	mc := messaging.NewMessageCenter(ctx, info.ID, config.NewDefaultMessageCenterConfig(info.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	// Initialize backend
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	backend := mock_changefeed.NewMockBackend(ctrl)
	backend.EXPECT().GetAllChangefeeds(gomock.Any()).Return(map[common.ChangeFeedID]*changefeed.ChangefeedMetaWrapper{}, nil).AnyTimes()

	// Create coordinator
	cr := New(info, &mockPdClient{}, backend, "test-gc-service", 100, 10000, time.Millisecond*10)
	co := cr.(*coordinator)

	// Number of goroutines for each operation
	const (
		sendEventGoroutines = 10
		stopGoroutines      = 5
		eventsPerGoroutine  = 100
	)

	var wg sync.WaitGroup
	wg.Add(sendEventGoroutines + stopGoroutines)

	// Start the coordinator
	ctxRun, cancelRun := context.WithCancel(ctx)
	go func() {
		err := cr.Run(ctxRun)
		if err != nil && err != context.Canceled {
			t.Errorf("Coordinator Run returned unexpected error: %v", err)
		}
	}()

	// Give coordinator some time to initialize
	time.Sleep(500 * time.Millisecond)

	// Start goroutines to send events
	for i := 0; i < sendEventGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			defer func() {
				// Recover from potential panics
				if r := recover(); r != nil {
					t.Errorf("Panic in send event goroutine %d: %v", id, r)
				}
			}()

			for j := 0; j < eventsPerGoroutine; j++ {
				// Try to send an event
				if co.closed.Load() {
					// Coordinator is already closed, stop sending
					return
				}

				msg := &messaging.TargetMessage{
					Topic: messaging.CoordinatorTopic,
					Type:  messaging.TypeMaintainerHeartbeatRequest,
				}

				// Use recvMessages to send event to channel
				err := co.recvMessages(ctx, msg)
				if err != nil && err != context.Canceled {
					t.Logf("Failed to send event in goroutine %d: %v", id, err)
				}

				// Small sleep to increase chance of race conditions
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Start goroutines to stop the coordinator
	for i := 0; i < stopGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			// Small delay to ensure some events are sent first
			time.Sleep(time.Duration(10+id*5) * time.Millisecond)
			co.Stop()
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Cancel the context to ensure the coordinator stops
	cancelRun()

	// Give some time for the coordinator to fully stop
	time.Sleep(100 * time.Millisecond)

	// Verify that the coordinator is closed
	require.True(t, co.closed.Load())

	// Verify that event channel is closed
	select {
	case _, ok := <-co.eventCh.Out():
		require.False(t, ok, "Event channel should be closed")
	default:
		// Channel might be already drained, which is fine
	}

	// Try sending another event - should not panic but may return error
	msg := &messaging.TargetMessage{
		Topic: messaging.CoordinatorTopic,
		Type:  messaging.TypeMaintainerHeartbeatRequest,
	}

	err := co.recvMessages(ctx, msg)
	require.NoError(t, err)
	require.True(t, co.closed.Load())
}

type maintainNode struct {
	cancel  context.CancelFunc
	mc      messaging.MessageCenter
	manager *mockMaintainerManager
}

func (d *maintainNode) stop() {
	d.mc.Close()
	d.cancel()
}

func startMaintainerNode(ctx context.Context,
	node *node.Info, mc messaging.MessageCenter,
	nodeManager *watcher.NodeManager,
) *maintainNode {
	nodeManager.RegisterNodeChangeHandler(node.ID, mc.OnNodeChanges)
	ctx, cancel := context.WithCancel(ctx)
	maintainerM := NewMaintainerManager(mc)
	go func() {
		var opts []grpc.ServerOption
		grpcServer := grpc.NewServer(opts...)
		mcs := messaging.NewMessageCenterServer(mc)
		proto.RegisterMessageServiceServer(grpcServer, mcs)
		lis, err := net.Listen("tcp", node.AdvertiseAddr)
		if err != nil {
			panic(err)
		}
		go func() {
			_ = grpcServer.Serve(lis)
		}()
		_ = maintainerM.Run(ctx)
		grpcServer.Stop()
	}()
	return &maintainNode{
		cancel:  cancel,
		mc:      mc,
		manager: maintainerM,
	}
}

type mockEtcdClient struct {
	ownerID string
	etcd.CDCEtcdClient
}

func newMockEtcdClient(ownerID string) *mockEtcdClient {
	return &mockEtcdClient{
		ownerID: ownerID,
	}
}

func (m *mockEtcdClient) GetOwnerID(ctx context.Context) (config.CaptureID, error) {
	return config.CaptureID(m.ownerID), nil
}
