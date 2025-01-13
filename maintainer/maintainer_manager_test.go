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
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/tiflow/cdc/model"
	config2 "github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// This is a integration test for maintainer manager, it may consume a lot of time.
// scale out/in close, add/remove tables
func TestMaintainerSchedulesNodeChanges(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	selfNode := node.NewInfo("127.0.0.1:18300", "")
	etcdClient := newMockEtcdClient(string(selfNode.ID))
	nodeManager := watcher.NewNodeManager(nil, etcdClient)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodeManager.GetAliveNodes()[selfNode.ID] = selfNode
	store := &mockSchemaStore{
		// 4 tables
		tables: []commonEvent.Table{
			{SchemaID: 1, TableID: 1, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t1"}},
			{SchemaID: 1, TableID: 2, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t2"}},
			{SchemaID: 1, TableID: 3, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t3"}},
			{SchemaID: 1, TableID: 4, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t4"}},
		},
	}
	appcontext.SetService(appcontext.SchemaStore, store)
	mc := messaging.NewMessageCenter(ctx, selfNode.ID, 0, config.NewDefaultMessageCenterConfig(), nil)
	appcontext.SetService(appcontext.MessageCenter, mc)
	startDispatcherNode(t, ctx, selfNode, mc, nodeManager)
	nodeManager.RegisterNodeChangeHandler(appcontext.MessageCenter, mc.OnNodeChanges)
	// Discard maintainer manager messages, cuz we don't need to handle them in this test
	mc.RegisterHandler(messaging.CoordinatorTopic, func(ctx context.Context, msg *messaging.TargetMessage) error {
		return nil
	})
	schedulerConf := &config.SchedulerConfig{
		AddTableBatchSize:    1000,
		CheckBalanceInterval: 0,
	}
	tsoClient := &replica.MockTsoClient{}
	manager := NewMaintainerManager(selfNode, schedulerConf, nil, tsoClient, nil)
	msg := messaging.NewSingleTargetMessage(selfNode.ID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: 1})
	msg.From = msg.To
	manager.onCoordinatorBootstrapRequest(msg)
	go func() {
		_ = manager.Run(ctx)
	}()
	dispManager := MockDispatcherManager(mc, selfNode.ID)
	go func() {
		_ = dispManager.Run(ctx)
	}()
	cfConfig := &model.ChangeFeedInfo{
		ID:     "test",
		Config: config2.GetDefaultReplicaConfig(),
	}
	data, err := json.Marshal(cfConfig)
	require.NoError(t, err)

	// Case 1: Add new changefeed
	cfID := common.NewChangeFeedIDWithName("test")
	_ = mc.SendCommand(messaging.NewSingleTargetMessage(selfNode.ID,
		messaging.MaintainerManagerTopic, &heartbeatpb.AddMaintainerRequest{
			Id:           cfID.ToPB(),
			Config:       data,
			CheckpointTs: 10,
		}))

	value, ok := manager.maintainers.Load(cfID)
	if !ok {
		require.Eventually(t, func() bool {
			value, ok = manager.maintainers.Load(cfID)
			return ok
		}, 20*time.Second, 200*time.Millisecond)
	}
	require.True(t, ok)
	maintainer := value.(*Maintainer)

	require.Eventually(t, func() bool {
		return maintainer.controller.replicationDB.GetReplicatingSize() == 4
	}, 20*time.Second, 200*time.Millisecond)
	require.Equal(t, 4,
		maintainer.controller.GetTaskSizeByNodeID(selfNode.ID))

	log.Info("Pass case 1: Add new changefeed")

	// Case 2: Add new nodes
	node2 := node.NewInfo("127.0.0.1:8400", "")
	mc2 := messaging.NewMessageCenter(ctx, node2.ID, 0, config.NewDefaultMessageCenterConfig(), nil)

	node3 := node.NewInfo("127.0.0.1:8500", "")
	mc3 := messaging.NewMessageCenter(ctx, node3.ID, 0, config.NewDefaultMessageCenterConfig(), nil)

	node4 := node.NewInfo("127.0.0.1:8600", "")
	mc4 := messaging.NewMessageCenter(ctx, node4.ID, 0, config.NewDefaultMessageCenterConfig(), nil)

	startDispatcherNode(t, ctx, node2, mc2, nodeManager)
	dn3 := startDispatcherNode(t, ctx, node3, mc3, nodeManager)
	dn4 := startDispatcherNode(t, ctx, node4, mc4, nodeManager)

	// notify node changes
	_, _ = nodeManager.Tick(ctx, &orchestrator.GlobalReactorState{
		Captures: map[model.CaptureID]*model.CaptureInfo{
			model.CaptureID(selfNode.ID): {ID: model.CaptureID(selfNode.ID), AdvertiseAddr: selfNode.AdvertiseAddr},
			model.CaptureID(node2.ID):    {ID: model.CaptureID(node2.ID), AdvertiseAddr: node2.AdvertiseAddr},
			model.CaptureID(node3.ID):    {ID: model.CaptureID(node3.ID), AdvertiseAddr: node3.AdvertiseAddr},
			model.CaptureID(node4.ID):    {ID: model.CaptureID(node4.ID), AdvertiseAddr: node4.AdvertiseAddr},
		},
	})

	time.Sleep(5 * time.Second)
	require.Eventually(t, func() bool {
		return maintainer.controller.replicationDB.GetReplicatingSize() == 4
	}, 20*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return maintainer.controller.GetTaskSizeByNodeID(selfNode.ID) == 1
	}, 20*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return maintainer.controller.GetTaskSizeByNodeID(node2.ID) == 1
	}, 20*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return maintainer.controller.GetTaskSizeByNodeID(node3.ID) == 1
	}, 20*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return maintainer.controller.GetTaskSizeByNodeID(node4.ID) == 1
	}, 20*time.Second, 200*time.Millisecond)

	log.Info("Pass case 2: Add new nodes")

	// Case 3: Remove 2 nodes
	dn3.stop()
	dn4.stop()
	_, _ = nodeManager.Tick(ctx, &orchestrator.GlobalReactorState{
		Captures: map[model.CaptureID]*model.CaptureInfo{
			model.CaptureID(selfNode.ID): {ID: model.CaptureID(selfNode.ID), AdvertiseAddr: selfNode.AdvertiseAddr},
			model.CaptureID(node2.ID):    {ID: model.CaptureID(node2.ID), AdvertiseAddr: node2.AdvertiseAddr},
		},
	})

	require.Eventually(t, func() bool {
		return maintainer.controller.replicationDB.GetReplicatingSize() == 4
	}, 20*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return maintainer.controller.GetTaskSizeByNodeID(selfNode.ID) == 2
	}, 20*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return maintainer.controller.GetTaskSizeByNodeID(node2.ID) == 2
	}, 20*time.Second, 200*time.Millisecond)

	log.Info("Pass case 3: Remove 2 nodes")

	// Case 4: Remove 2 tables
	maintainer.controller.RemoveTasksByTableIDs(2, 3)
	require.Eventually(t, func() bool {
		return maintainer.controller.replicationDB.GetReplicatingSize() == 2
	}, 20*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return maintainer.controller.GetTaskSizeByNodeID(selfNode.ID) == 1
	}, 20*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return maintainer.controller.GetTaskSizeByNodeID(node2.ID) == 1
	}, 20*time.Second, 200*time.Millisecond)
	log.Info("Pass case 4: Remove 2 tables")

	// Case 5: Add 2 tables
	maintainer.controller.AddNewTable(commonEvent.Table{
		SchemaID: 1,
		TableID:  5,
	}, 3)
	maintainer.controller.AddNewTable(commonEvent.Table{
		SchemaID: 1,
		TableID:  6,
	}, 3)
	require.Eventually(t, func() bool {
		return maintainer.controller.replicationDB.GetReplicatingSize() == 4
	}, 20*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return maintainer.controller.GetTaskSizeByNodeID(selfNode.ID) == 2
	}, 20*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return maintainer.controller.GetTaskSizeByNodeID(node2.ID) == 2
	}, 20*time.Second, 200*time.Millisecond)

	log.Info("Pass case 5: Add 2 tables")

	// Case 6: Remove maintainer
	err = mc.SendCommand(messaging.NewSingleTargetMessage(selfNode.ID, messaging.MaintainerManagerTopic,
		&heartbeatpb.RemoveMaintainerRequest{Id: cfID.ToPB(), Cascade: true}))
	require.NoError(t, err)
	time.Sleep(5 * time.Second)

	require.Eventually(t, func() bool {
		return maintainer.state.Load() == int32(heartbeatpb.ComponentState_Stopped)
	}, 20*time.Second, 200*time.Millisecond)

	_, ok = manager.maintainers.Load(cfID)
	if ok {
		require.Eventually(t, func() bool {
			_, ok = manager.maintainers.Load(cfID)
			return ok == false
		}, 20*time.Second, 200*time.Millisecond)
	}
	require.False(t, ok)
	log.Info("Pass case 6: Remove maintainer")
	cancel()
}

func TestMaintainerBootstrapWithTablesReported(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	selfNode := node.NewInfo("127.0.0.1:18301", "")
	etcdClient := newMockEtcdClient(string(selfNode.ID))
	nodeManager := watcher.NewNodeManager(nil, etcdClient)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodeManager.GetAliveNodes()[selfNode.ID] = selfNode
	store := &mockSchemaStore{
		// 4 tables
		tables: []commonEvent.Table{
			{SchemaID: 1, TableID: 1, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t1"}},
			{SchemaID: 1, TableID: 2, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t2"}},
			{SchemaID: 1, TableID: 3, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t3"}},
			{SchemaID: 1, TableID: 4, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t4"}},
		},
	}
	appcontext.SetService(appcontext.SchemaStore, store)
	mc := messaging.NewMessageCenter(ctx, selfNode.ID, 0, config.NewDefaultMessageCenterConfig(), nil)
	appcontext.SetService(appcontext.MessageCenter, mc)
	startDispatcherNode(t, ctx, selfNode, mc, nodeManager)
	nodeManager.RegisterNodeChangeHandler(appcontext.MessageCenter, mc.OnNodeChanges)
	// discard maintainer manager messages
	mc.RegisterHandler(messaging.CoordinatorTopic, func(ctx context.Context, msg *messaging.TargetMessage) error {
		return nil
	})
	tsoClient := &replica.MockTsoClient{}
	manager := NewMaintainerManager(selfNode, config.GetGlobalServerConfig().Debug.Scheduler, nil, tsoClient, nil)
	msg := messaging.NewSingleTargetMessage(selfNode.ID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: 1})
	msg.From = msg.To
	manager.onCoordinatorBootstrapRequest(msg)
	go func() {
		_ = manager.Run(ctx)
	}()
	dispManager := MockDispatcherManager(mc, selfNode.ID)
	// table1 and table 2 will be reported by remote
	var remotedIds []common.DispatcherID
	for i := 1; i < 3; i++ {
		span := spanz.TableIDToComparableSpan(int64(i))
		tableSpan := &heartbeatpb.TableSpan{
			TableID:  int64(i),
			StartKey: span.StartKey,
			EndKey:   span.EndKey,
		}
		dispatcherID := common.NewDispatcherID()
		remotedIds = append(remotedIds, dispatcherID)
		dispManager.bootstrapTables = append(dispManager.bootstrapTables, &heartbeatpb.BootstrapTableSpan{
			ID:       dispatcherID.ToPB(),
			SchemaID: 1,
			Span: &heartbeatpb.TableSpan{
				TableID:  tableSpan.TableID,
				StartKey: tableSpan.StartKey,
				EndKey:   tableSpan.EndKey,
			},
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    10,
		})
	}

	go func() {
		_ = dispManager.Run(ctx)
	}()
	cfID := common.NewChangeFeedIDWithName("test")
	cfConfig := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
	}
	data, err := json.Marshal(cfConfig)
	require.NoError(t, err)
	_ = mc.SendCommand(messaging.NewSingleTargetMessage(selfNode.ID,
		messaging.MaintainerManagerTopic, &heartbeatpb.AddMaintainerRequest{
			Id:           cfID.ToPB(),
			Config:       data,
			CheckpointTs: 10,
		}))

	value, ok := manager.maintainers.Load(cfID)
	if !ok {
		require.Eventually(t, func() bool {
			value, ok = manager.maintainers.Load(cfID)
			return ok
		}, 20*time.Second, 200*time.Millisecond)
	}
	require.True(t, ok)
	maintainer := value.(*Maintainer)

	require.Eventually(t, func() bool {
		return maintainer.controller.replicationDB.GetReplicatingSize() == 4
	}, 20*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return maintainer.controller.GetTaskSizeByNodeID(selfNode.ID) == 4
	}, 20*time.Second, 200*time.Millisecond)

	require.Len(t, remotedIds, 2)
	foundSize := 0
	hasDDLDispatcher := false
	for _, stm := range maintainer.controller.replicationDB.GetReplicating() {
		if stm.Span.Equal(heartbeatpb.DDLSpan) {
			hasDDLDispatcher = true
		}
		for _, remotedId := range remotedIds {
			if stm.ID == remotedId {
				foundSize++
				tblID := stm.Span.TableID
				require.True(t, int64(1) == tblID || int64(2) == tblID)
			}
		}
	}
	require.Equal(t, 2, foundSize)
	require.False(t, hasDDLDispatcher)
	cancel()
}

func TestStopNotExistsMaintainer(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	selfNode := node.NewInfo("127.0.0.1:8800", "")
	etcdClient := newMockEtcdClient(string(selfNode.ID))
	nodeManager := watcher.NewNodeManager(nil, etcdClient)
	appcontext.SetService(watcher.NodeManagerName, nodeManager)
	nodeManager.GetAliveNodes()[selfNode.ID] = selfNode
	store := &mockSchemaStore{
		// 4 tables
		tables: []commonEvent.Table{
			{SchemaID: 1, TableID: 1, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t1"}},
			{SchemaID: 1, TableID: 2, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t2"}},
			{SchemaID: 1, TableID: 3, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t3"}},
			{SchemaID: 1, TableID: 4, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t4"}},
		},
	}
	appcontext.SetService(appcontext.SchemaStore, store)
	mc := messaging.NewMessageCenter(ctx, selfNode.ID, 0, config.NewDefaultMessageCenterConfig(), nil)
	appcontext.SetService(appcontext.MessageCenter, mc)
	startDispatcherNode(t, ctx, selfNode, mc, nodeManager)
	nodeManager.RegisterNodeChangeHandler(appcontext.MessageCenter, mc.OnNodeChanges)
	// discard maintainer manager messages
	mc.RegisterHandler(messaging.CoordinatorTopic, func(ctx context.Context, msg *messaging.TargetMessage) error {
		return nil
	})
	schedulerConf := &config.SchedulerConfig{AddTableBatchSize: 1000}
	tsoClient := &replica.MockTsoClient{}
	manager := NewMaintainerManager(selfNode, schedulerConf, nil, tsoClient, nil)
	msg := messaging.NewSingleTargetMessage(selfNode.ID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: 1})
	msg.From = msg.To
	manager.onCoordinatorBootstrapRequest(msg)
	go func() {
		_ = manager.Run(ctx)
	}()
	dispManager := MockDispatcherManager(mc, selfNode.ID)
	go func() {
		_ = dispManager.Run(ctx)
	}()
	cfID := common.NewChangeFeedIDWithName("test")
	_ = mc.SendCommand(messaging.NewSingleTargetMessage(selfNode.ID, messaging.MaintainerManagerTopic, &heartbeatpb.RemoveMaintainerRequest{
		Id:      cfID.ToPB(),
		Cascade: true,
		Removed: true,
	}))

	_, ok := manager.maintainers.Load(cfID)
	if ok {
		require.Eventually(t, func() bool {
			_, ok = manager.maintainers.Load(cfID)
			return !ok
		}, 20*time.Second, 200*time.Millisecond)
	}
	require.False(t, ok)
	cancel()
}

type mockSchemaStore struct {
	schemastore.SchemaStore
	tables []commonEvent.Table
}

func (m *mockSchemaStore) GetAllPhysicalTables(snapTs common.Ts, filter filter.Filter) ([]commonEvent.Table, error) {
	return m.tables, nil
}

type dispatcherNode struct {
	cancel            context.CancelFunc
	mc                messaging.MessageCenter
	dispatcherManager *mockDispatcherManager
}

func (d *dispatcherNode) stop() {
	d.mc.Close()
	d.cancel()
}

func startDispatcherNode(t *testing.T, ctx context.Context,
	node *node.Info, mc messaging.MessageCenter, nodeManager *watcher.NodeManager,
) *dispatcherNode {
	nodeManager.RegisterNodeChangeHandler(node.ID, mc.OnNodeChanges)
	ctx, cancel := context.WithCancel(ctx)
	dispManager := MockDispatcherManager(mc, node.ID)
	go func() {
		var opts []grpc.ServerOption
		grpcServer := grpc.NewServer(opts...)
		mcs := messaging.NewMessageCenterServer(mc)
		proto.RegisterMessageCenterServer(grpcServer, mcs)
		lis, err := net.Listen("tcp", node.AdvertiseAddr)
		require.NoError(t, err)
		go func() {
			_ = grpcServer.Serve(lis)
		}()
		_ = dispManager.Run(ctx)
		grpcServer.Stop()
	}()
	return &dispatcherNode{
		cancel:            cancel,
		mc:                mc,
		dispatcherManager: dispManager,
	}
}

type mockEtcdClient struct {
	etcd.CDCEtcdClient
	ownerID string
}

func newMockEtcdClient(ownerID string) *mockEtcdClient {
	return &mockEtcdClient{
		ownerID: ownerID,
	}
}

func (m *mockEtcdClient) GetOwnerID(ctx context.Context) (model.CaptureID, error) {
	return model.CaptureID(m.ownerID), nil
}
