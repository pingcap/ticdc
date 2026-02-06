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

package eventcollector

import (
	"context"
	"errors"
	"sync/atomic"
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
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/stretchr/testify/require"
)

var _ dispatcher.DispatcherService = (*mockEventDispatcher)(nil)

type mockEventDispatcher struct {
	id           common.DispatcherID
	tableSpan    *heartbeatpb.TableSpan
	handle       func(commonEvent.Event)
	changefeedID common.ChangeFeedID

	batchCount uint64
	batchBytes uint64
}

func (m *mockEventDispatcher) GetId() common.DispatcherID {
	return m.id
}

func (m *mockEventDispatcher) GetMode() int64 {
	return common.DefaultMode
}

func (m *mockEventDispatcher) GetStartTs() uint64 {
	return 0
}

func (m *mockEventDispatcher) GetBDRMode() bool {
	return false
}

func (m *mockEventDispatcher) GetChangefeedID() common.ChangeFeedID {
	return m.changefeedID
}

func (m *mockEventDispatcher) GetEventCollectorBatchCount() uint64 {
	if m.batchCount == 0 {
		return 1
	}
	return m.batchCount
}

func (m *mockEventDispatcher) GetEventCollectorBatchBytes() uint64 {
	return m.batchBytes
}

func (m *mockEventDispatcher) GetTableSpan() *heartbeatpb.TableSpan {
	return m.tableSpan
}

func (m *mockEventDispatcher) GetTimezone() string {
	return "system"
}

func (m *mockEventDispatcher) GetIntegrityConfig() *eventpb.IntegrityConfig {
	return nil
}

func (m *mockEventDispatcher) GetFilterConfig() *eventpb.FilterConfig {
	return &eventpb.FilterConfig{}
}

func (m *mockEventDispatcher) EnableSyncPoint() bool {
	return false
}

func (m *mockEventDispatcher) GetSyncPointInterval() time.Duration {
	return time.Second
}

func (m *mockEventDispatcher) GetSkipSyncpointAtStartTs() bool {
	return false
}

func (m *mockEventDispatcher) GetTxnAtomicity() config.AtomicityLevel {
	return config.DefaultAtomicityLevel()
}

func (m *mockEventDispatcher) GetResolvedTs() uint64 {
	return 0
}

func (m *mockEventDispatcher) GetCheckpointTs() uint64 {
	return 0
}

func (m *mockEventDispatcher) HandleEvents(dispatcherEvents []dispatcher.DispatcherEvent, wakeCallback func()) (block bool) {
	for _, dispatcherEvent := range dispatcherEvents {
		m.handle(dispatcherEvent.Event)
	}
	return false
}

func (m *mockEventDispatcher) GetBlockEventStatus() *heartbeatpb.State {
	return &heartbeatpb.State{}
}

func (m *mockEventDispatcher) IsOutputRawChangeEvent() bool {
	return false
}

type mockDynamicStream struct {
	addPathCalls []struct {
		path         common.DispatcherID
		dest         *dispatcherStat
		areaSettings []dynstream.AreaSettings
	}
	removePathCalls   []common.DispatcherID
	setAreaBatchCalls []struct {
		area       common.GID
		batchCount uint64
		batchBytes uint64
	}

	addPathErr error
}

func (m *mockDynamicStream) Start() {}

func (m *mockDynamicStream) Close() {}

func (m *mockDynamicStream) Push(path common.DispatcherID, event dispatcher.DispatcherEvent) {}

func (m *mockDynamicStream) Wake(path common.DispatcherID) {}

func (m *mockDynamicStream) Feedback() <-chan dynstream.Feedback[common.GID, common.DispatcherID, *dispatcherStat] {
	return nil
}

func (m *mockDynamicStream) AddPath(path common.DispatcherID, dest *dispatcherStat, area ...dynstream.AreaSettings) error {
	m.addPathCalls = append(m.addPathCalls, struct {
		path         common.DispatcherID
		dest         *dispatcherStat
		areaSettings []dynstream.AreaSettings
	}{
		path:         path,
		dest:         dest,
		areaSettings: area,
	})
	return m.addPathErr
}

func (m *mockDynamicStream) RemovePath(path common.DispatcherID) error {
	m.removePathCalls = append(m.removePathCalls, path)
	return nil
}

func (m *mockDynamicStream) Release(path common.DispatcherID) {}

func (m *mockDynamicStream) SetAreaSettings(area common.GID, settings dynstream.AreaSettings) {}

func (m *mockDynamicStream) SetAreaBatchConfig(area common.GID, batchCount uint64, batchBytes uint64) {
	m.setAreaBatchCalls = append(m.setAreaBatchCalls, struct {
		area       common.GID
		batchCount uint64
		batchBytes uint64
	}{
		area:       area,
		batchCount: batchCount,
		batchBytes: batchBytes,
	})
}

func (m *mockDynamicStream) GetMetrics() dynstream.Metrics[common.GID, common.DispatcherID] {
	return dynstream.Metrics[common.GID, common.DispatcherID]{}
}

func newMessage(id node.ID, msg messaging.IOTypeT) *messaging.TargetMessage {
	targetMessage := messaging.NewSingleTargetMessage(id, messaging.EventCollectorTopic, msg)
	targetMessage.From = id
	return targetMessage
}

func TestPrepareAddDispatcherAppliesAreaBatchConfig(t *testing.T) {
	ctx := context.Background()
	nodeInfo := node.NewInfo("127.0.0.1:18300", "")

	mc := messaging.NewMessageCenter(ctx, nodeInfo.ID, config.NewDefaultMessageCenterConfig(nodeInfo.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	c := New(nodeInfo.ID)
	c.ds.Close()
	c.redoDs.Close()

	ds := &mockDynamicStream{}
	c.ds = ds
	c.redoDs = ds

	changefeedID := common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName)
	dispatcherID := common.NewDispatcherID()
	d := &mockEventDispatcher{
		id:           dispatcherID,
		tableSpan:    &heartbeatpb.TableSpan{TableID: 1},
		changefeedID: changefeedID,
		handle:       func(commonEvent.Event) {},
		batchCount:   123,
		batchBytes:   456,
	}

	c.PrepareAddDispatcher(d, 1024, nil)
	t.Cleanup(func() { c.RemoveDispatcher(d) })

	require.Len(t, ds.addPathCalls, 1)
	require.Len(t, ds.setAreaBatchCalls, 1)
	require.Equal(t, changefeedID.ID(), ds.setAreaBatchCalls[0].area)
	require.Equal(t, uint64(123), ds.setAreaBatchCalls[0].batchCount)
	require.Equal(t, uint64(456), ds.setAreaBatchCalls[0].batchBytes)
}

func TestPrepareAddDispatcherSkipsAreaBatchConfigWhenAddPathFails(t *testing.T) {
	ctx := context.Background()
	nodeInfo := node.NewInfo("127.0.0.1:18300", "")

	mc := messaging.NewMessageCenter(ctx, nodeInfo.ID, config.NewDefaultMessageCenterConfig(nodeInfo.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	c := New(nodeInfo.ID)
	c.ds.Close()
	c.redoDs.Close()

	ds := &mockDynamicStream{addPathErr: errors.New("add path failed")}
	c.ds = ds
	c.redoDs = ds

	changefeedID := common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName)
	dispatcherID := common.NewDispatcherID()
	d := &mockEventDispatcher{
		id:           dispatcherID,
		tableSpan:    &heartbeatpb.TableSpan{TableID: 1},
		changefeedID: changefeedID,
		handle:       func(commonEvent.Event) {},
		batchCount:   123,
		batchBytes:   456,
	}

	c.PrepareAddDispatcher(d, 1024, nil)
	t.Cleanup(func() { c.RemoveDispatcher(d) })

	require.Len(t, ds.addPathCalls, 1)
	require.Empty(t, ds.setAreaBatchCalls)
}

func TestProcessMessage(t *testing.T) {
	ctx := context.Background()
	node := node.NewInfo("127.0.0.1:18300", "")
	mc := messaging.NewMessageCenter(ctx, node.ID, config.NewDefaultMessageCenterConfig(node.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)
	c := New(node.ID)
	did := common.NewDispatcherID()
	ch := make(chan *messaging.TargetMessage, receiveChanSize)
	go func() {
		c.runDispatchMessage(ctx, ch, common.DefaultMode)
	}()

	var seq atomic.Uint64
	seq.Store(1) // handshake event has seq 1
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	ddl := helper.DDL2Event("create table t(id int primary key, v int)")
	require.NotNil(t, ddl)
	dmls := helper.DML2BatchEvent("test", "t",
		"insert into t values(1, 1)",
		"insert into t values(2, 2)",
		"insert into t values(3, 3)",
		"insert into t values(4, 4)",
	)
	require.NotNil(t, dmls)

	readyEvent := commonEvent.NewReadyEvent(did)
	handshakeEvent := commonEvent.NewHandshakeEvent(did, ddl.GetStartTs()-1, 1, ddl.TableInfo)
	events := make(map[uint64]commonEvent.Event)
	ddl.DispatcherID = did
	ddl.Seq = seq.Add(1)
	ddl.Epoch = 1
	events[ddl.Seq] = ddl
	for i, dml := range dmls.DMLEvents {
		dml.DispatcherID = did
		dml.Seq = seq.Add(1)
		dml.Epoch = 1
		dml.CommitTs = ddl.FinishedTs + uint64(i)
		events[dml.Seq] = dml
	}

	seq.Store(1)
	done := make(chan struct{})
	d := &mockEventDispatcher{id: did, tableSpan: &heartbeatpb.TableSpan{TableID: 1}}
	d.handle = func(e commonEvent.Event) {
		require.Equal(t, e.GetSeq(), seq.Add(1))
		require.Equal(t, events[e.GetSeq()], e)
		if e.GetSeq() == uint64(ddl.Len())+uint64(len(dmls.DMLEvents)) {
			done <- struct{}{}
		}
	}
	c.AddDispatcher(d, util.GetOrZero(config.GetDefaultReplicaConfig().MemoryQuota))

	ch <- newMessage(node.ID, &readyEvent)
	ch <- newMessage(node.ID, &handshakeEvent)
	ch <- newMessage(node.ID, ddl)
	ch <- newMessage(node.ID, dmls)

	ctx1, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()
	select {
	case <-done:
	case <-ctx1.Done():
		require.Fail(t, "timeout")
	}
}

func TestRemoveLastDispatcher(t *testing.T) {
	ctx := context.Background()
	nodeInfo := node.NewInfo("127.0.0.1:18300", "")
	mc := messaging.NewMessageCenter(ctx, nodeInfo.ID, config.NewDefaultMessageCenterConfig(nodeInfo.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)
	c := New(nodeInfo.ID)
	c.Run(ctx)
	defer c.Close()

	cfID1 := common.NewChangefeedID(common.DefaultKeyspaceName)
	cfID2 := common.NewChangefeedID(common.DefaultKeyspaceName)

	d1 := &mockEventDispatcher{id: common.NewDispatcherID(), tableSpan: &heartbeatpb.TableSpan{TableID: 1}, changefeedID: cfID1}
	d2 := &mockEventDispatcher{id: common.NewDispatcherID(), tableSpan: &heartbeatpb.TableSpan{TableID: 2}, changefeedID: cfID1}
	d3 := &mockEventDispatcher{id: common.NewDispatcherID(), tableSpan: &heartbeatpb.TableSpan{TableID: 3}, changefeedID: cfID2}

	// Add dispatchers
	c.AddDispatcher(d1, 1024)
	c.AddDispatcher(d2, 1024)
	c.AddDispatcher(d3, 1024)

	// Check that changefeed stats are created
	_, ok := c.changefeedMap.Load(cfID1.ID())
	require.True(t, ok, "changefeedStat for cfID1 should exist")
	_, ok = c.changefeedMap.Load(cfID2.ID())
	require.True(t, ok, "changefeedStat for cfID2 should exist")

	// Remove one dispatcher from cfID1, stat should still exist
	c.RemoveDispatcher(d1)
	_, ok = c.changefeedMap.Load(cfID1.ID())
	require.True(t, ok, "changefeedStat for cfID1 should still exist after removing one dispatcher")

	// Remove the last dispatcher from cfID1, stat should be removed
	c.RemoveDispatcher(d2)
	_, ok = c.changefeedMap.Load(cfID1.ID())
	require.False(t, ok, "changefeedStat for cfID1 should be removed after removing the last dispatcher")
	_, ok = c.changefeedMap.Load(cfID2.ID())
	require.True(t, ok, "changefeedStat for cfID2 should not be affected")
}
