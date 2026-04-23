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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/routing"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

var _ dispatcher.DispatcherService = (*mockEventDispatcher)(nil)

type mockEventDispatcher struct {
	id                       common.DispatcherID
	tableSpan                *heartbeatpb.TableSpan
	handle                   func(commonEvent.Event)
	handleEvents             func([]dispatcher.DispatcherEvent, func()) bool
	changefeedID             common.ChangeFeedID
	checkpointTs             uint64
	eventCollectorBatchCount int
	eventCollectorBatchBytes int
	batchRecords             chan batchRecord
}

type batchRecord struct {
	size      int
	eventType int
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

func (d *mockEventDispatcher) GetEventCollectorBatchConfig() (batchCount int, batchBytes int) {
	return d.eventCollectorBatchCount, d.eventCollectorBatchBytes
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
	return m.checkpointTs
}

func (m *mockEventDispatcher) HandleEvents(dispatcherEvents []dispatcher.DispatcherEvent, wakeCallback func()) (block bool) {
	if m.batchRecords != nil && len(dispatcherEvents) > 0 {
		m.batchRecords <- batchRecord{
			size:      len(dispatcherEvents),
			eventType: dispatcherEvents[0].GetType(),
		}
	}
	if m.handleEvents != nil {
		return m.handleEvents(dispatcherEvents, wakeCallback)
	}
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

func (m *mockEventDispatcher) GetRouter() *routing.Router {
	return nil
}

func newMessage(id node.ID, msg messaging.IOTypeT) *messaging.TargetMessage {
	targetMessage := messaging.NewSingleTargetMessage(id, messaging.EventCollectorTopic, msg)
	targetMessage.From = id
	return targetMessage
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
		// TableInfoVersion is set during event processing to ddl.FinishedTs
		// (from tableInfoVersion = max(d.tableInfoVersion.Load(), d.target.GetStartTs())
		// where d.tableInfoVersion is set when processing the DDL event).
		// Since BatchDMLEvent is cloned during processing to avoid race conditions,
		// the processed events are different objects, so we set this here on the expected events.
		dml.TableInfoVersion = ddl.FinishedTs
		events[dml.Seq] = dml
	}

	seq.Store(1)
	done := make(chan struct{})
	d := &mockEventDispatcher{
		id:        did,
		tableSpan: &heartbeatpb.TableSpan{TableID: 1},
	}
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

	d1 := &mockEventDispatcher{
		id:           common.NewDispatcherID(),
		tableSpan:    &heartbeatpb.TableSpan{TableID: 1},
		changefeedID: cfID1,
	}
	d2 := &mockEventDispatcher{
		id:           common.NewDispatcherID(),
		tableSpan:    &heartbeatpb.TableSpan{TableID: 2},
		changefeedID: cfID1,
	}
	d3 := &mockEventDispatcher{
		id:           common.NewDispatcherID(),
		tableSpan:    &heartbeatpb.TableSpan{TableID: 3},
		changefeedID: cfID2,
	}

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

func TestGroupHeartbeatUsesEpochAndClamp(t *testing.T) {
	ctx := context.Background()
	serverInfo := node.NewInfo("127.0.0.1:18300", "")
	mc := messaging.NewMessageCenter(ctx, serverInfo.ID, config.NewDefaultMessageCenterConfig(serverInfo.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	c := New(serverInfo.ID)

	localDispatcher := &mockEventDispatcher{
		id:           common.NewDispatcherID(),
		tableSpan:    &heartbeatpb.TableSpan{TableID: 1},
		changefeedID: common.NewChangefeedID4Test("default", "cf"),
		checkpointTs: 200,
	}
	c.AddDispatcher(localDispatcher, 1024)
	localStat := c.getDispatcherStatByID(localDispatcher.id)
	require.NotNil(t, localStat)
	localStat.connState.setEventServiceID(serverInfo.ID)
	localStat.connState.readyEventReceived.Store(true)
	localStat.currentEpoch.Store(newDispatcherEpochState(3, 0, 150))

	remoteID := node.ID("remote-server")
	remoteDispatcher := &mockEventDispatcher{
		id:           common.NewDispatcherID(),
		tableSpan:    &heartbeatpb.TableSpan{TableID: 2},
		changefeedID: common.NewChangefeedID4Test("default", "cf"),
		checkpointTs: 220,
	}
	c.AddDispatcher(remoteDispatcher, 1024)
	remoteStat := c.getDispatcherStatByID(remoteDispatcher.id)
	require.NotNil(t, remoteStat)
	remoteStat.connState.setEventServiceID(remoteID)
	remoteStat.connState.readyEventReceived.Store(true)
	remoteStat.currentEpoch.Store(newDispatcherEpochState(5, 1, 210))

	grouped := c.groupHeartbeat()
	require.Len(t, grouped, 2)

	localHeartbeat := grouped[serverInfo.ID]
	require.NotNil(t, localHeartbeat)
	require.Equal(t, commonEvent.DispatcherHeartbeatVersion2, localHeartbeat.Version)
	require.Len(t, localHeartbeat.DispatcherProgresses, 1)
	require.Equal(t, uint8(commonEvent.DispatcherProgressVersion1), localHeartbeat.DispatcherProgresses[0].Version)
	require.Equal(t, localDispatcher.id, localHeartbeat.DispatcherProgresses[0].DispatcherID)
	require.Equal(t, uint64(150), localHeartbeat.DispatcherProgresses[0].CheckpointTs)
	require.Equal(t, uint64(3), localHeartbeat.DispatcherProgresses[0].Epoch)

	remoteHeartbeat := grouped[remoteID]
	require.NotNil(t, remoteHeartbeat)
	require.Equal(t, commonEvent.DispatcherHeartbeatVersion2, remoteHeartbeat.Version)
	require.Len(t, remoteHeartbeat.DispatcherProgresses, 1)
	require.Equal(t, uint8(commonEvent.DispatcherProgressVersion1), remoteHeartbeat.DispatcherProgresses[0].Version)
	require.Equal(t, remoteDispatcher.id, remoteHeartbeat.DispatcherProgresses[0].DispatcherID)
	require.Equal(t, uint64(210), remoteHeartbeat.DispatcherProgresses[0].CheckpointTs)
	require.Equal(t, uint64(5), remoteHeartbeat.DispatcherProgresses[0].Epoch)
}

func TestGroupHeartbeatResetThenHandshake(t *testing.T) {
	ctx := context.Background()
	serverInfo := node.NewInfo("127.0.0.1:18300", "")
	mc := messaging.NewMessageCenter(ctx, serverInfo.ID, config.NewDefaultMessageCenterConfig(serverInfo.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	c := New(serverInfo.ID)

	dispatcherID := common.NewDispatcherID()
	mockDisp := &mockEventDispatcher{
		id:           dispatcherID,
		tableSpan:    &heartbeatpb.TableSpan{TableID: 1},
		changefeedID: common.NewChangefeedID4Test("default", "cf"),
		checkpointTs: 220,
	}
	c.AddDispatcher(mockDisp, 1024)
	stat := c.getDispatcherStatByID(dispatcherID)
	require.NotNil(t, stat)
	stat.connState.setEventServiceID(serverInfo.ID)
	stat.connState.readyEventReceived.Store(true)

	// Simulate a reset to a smaller ts while old in-flight flushes have already
	// advanced sink checkpoint to a larger value.
	stat.doReset(serverInfo.ID, 150)

	grouped := c.groupHeartbeat()
	heartbeat := grouped[serverInfo.ID]
	require.NotNil(t, heartbeat)
	require.Len(t, heartbeat.DispatcherProgresses, 1)
	require.Equal(t, uint8(commonEvent.DispatcherProgressVersion1), heartbeat.DispatcherProgresses[0].Version)
	require.Equal(t, uint64(150), heartbeat.DispatcherProgresses[0].CheckpointTs)
	require.Equal(t, uint64(1), heartbeat.DispatcherProgresses[0].Epoch)

	// Handshake only proves collector has observed the handshake ts. Even if sink
	// checkpoint has already jumped ahead, heartbeat must stay bounded by
	// collector-observed progress.
	handshake := commonEvent.NewHandshakeEvent(dispatcherID, 180, 1, &common.TableInfo{})
	stat.handleHandshakeEvent(dispatcher.DispatcherEvent{
		Event: &handshake,
	})

	grouped = c.groupHeartbeat()
	heartbeat = grouped[serverInfo.ID]
	require.NotNil(t, heartbeat)
	require.Len(t, heartbeat.DispatcherProgresses, 1)
	require.Equal(t, uint8(commonEvent.DispatcherProgressVersion1), heartbeat.DispatcherProgresses[0].Version)
	require.Equal(t, uint64(180), heartbeat.DispatcherProgresses[0].CheckpointTs)
	require.Equal(t, uint64(1), heartbeat.DispatcherProgresses[0].Epoch)

	stat.loadCurrentEpochState().maxEventTs.Store(210)
	grouped = c.groupHeartbeat()
	heartbeat = grouped[serverInfo.ID]
	require.NotNil(t, heartbeat)
	require.Len(t, heartbeat.DispatcherProgresses, 1)
	require.Equal(t, uint8(commonEvent.DispatcherProgressVersion1), heartbeat.DispatcherProgresses[0].Version)
	require.Equal(t, uint64(210), heartbeat.DispatcherProgresses[0].CheckpointTs)
	require.Equal(t, uint64(1), heartbeat.DispatcherProgresses[0].Epoch)
}

// TestEventCollectorBatchByCount blocks the first wake-up so pending DMLs
// accumulate and the dynamic stream must honor the configured batch count.
func TestEventCollectorBatchByCount(t *testing.T) {
	ctx := context.Background()
	localServerID := node.NewID()
	c := newTestEventCollector(localServerID)
	defer c.ds.Close()
	defer c.redoDs.Close()

	const batchCount = 3
	const totalDML = 10

	did := common.NewDispatcherID()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	ddl := helper.DDL2Event("create table t(id int primary key, v int)")
	require.NotNil(t, ddl)

	batchRecords := make(chan batchRecord, totalDML+8)
	done := make(chan struct{})
	release := make(chan struct{})
	var seenDML atomic.Int64
	var blocked atomic.Bool

	d := &mockEventDispatcher{
		id:                       did,
		tableSpan:                &heartbeatpb.TableSpan{TableID: 1},
		changefeedID:             common.NewChangefeedID(common.DefaultKeyspaceName),
		eventCollectorBatchCount: batchCount,
		eventCollectorBatchBytes: 0,
		batchRecords:             batchRecords,
	}
	d.handle = func(e commonEvent.Event) {
		if e.GetType() != commonEvent.TypeDMLEvent {
			return
		}
		if seenDML.Add(1) == totalDML {
			close(done)
		}
	}
	d.handleEvents = func(events []dispatcher.DispatcherEvent, wakeCallback func()) bool {
		for _, event := range events {
			d.handle(event.Event)
		}
		if blocked.CompareAndSwap(false, true) {
			go func() {
				<-release
				wakeCallback()
			}()
			return true
		}
		return false
	}
	c.AddDispatcher(d, util.GetOrZero(config.GetDefaultReplicaConfig().MemoryQuota))

	from := localServerID
	readyEvent := commonEvent.NewReadyEvent(did)
	c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, &readyEvent))

	handshakeEvent := commonEvent.NewHandshakeEvent(did, ddl.GetStartTs()-1, 1, ddl.TableInfo)
	c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, &handshakeEvent))

	var seq atomic.Uint64
	seq.Store(1) // handshake event has seq 1
	for i := 1; i <= totalDML; i++ {
		dml := helper.DML2Event("test", "t", fmt.Sprintf("insert into t values(%d, %d)", i, i))
		require.NotNil(t, dml)
		dml.DispatcherID = did
		dml.Seq = seq.Add(1)
		dml.Epoch = 1
		dml.CommitTs = ddl.FinishedTs + uint64(i)
		c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, dml))
	}
	close(release)

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	select {
	case <-done:
	case <-ctx.Done():
		require.FailNow(t, "timeout waiting for dml events")
	}

	sumDML := 0
	maxBatch := 0
	for sumDML < totalDML {
		select {
		case r := <-batchRecords:
			if r.eventType != commonEvent.TypeDMLEvent {
				continue
			}
			sumDML += r.size
			if r.size > maxBatch {
				maxBatch = r.size
			}
		case <-ctx.Done():
			require.FailNow(t, "timeout collecting dml batch records")
		}
	}

	require.Equal(t, batchCount, maxBatch)
	require.Equal(t, totalDML, sumDML)
}

// TestEventCollectorBatchByBytes uses a tiny byte budget so each DML must be
// delivered alone once the per-dispatcher batch bytes wiring is applied.
func TestEventCollectorBatchByBytes(t *testing.T) {
	ctx := context.Background()
	localServerID := node.NewID()
	c := newTestEventCollector(localServerID)
	defer c.ds.Close()
	defer c.redoDs.Close()

	const totalDML = 12

	did := common.NewDispatcherID()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	ddl := helper.DDL2Event("create table t(id int primary key, v int)")
	require.NotNil(t, ddl)

	batchRecords := make(chan batchRecord, totalDML+8)
	done := make(chan struct{})
	var seenDML atomic.Int64

	d := &mockEventDispatcher{
		id:                       did,
		tableSpan:                &heartbeatpb.TableSpan{TableID: 1},
		changefeedID:             common.NewChangefeedID(common.DefaultKeyspaceName),
		eventCollectorBatchCount: 1024,
		eventCollectorBatchBytes: 1,
		batchRecords:             batchRecords,
	}
	d.handle = func(e commonEvent.Event) {
		if e.GetType() != commonEvent.TypeDMLEvent {
			return
		}
		if seenDML.Add(1) == totalDML {
			close(done)
		}
	}
	c.AddDispatcher(d, util.GetOrZero(config.GetDefaultReplicaConfig().MemoryQuota))

	from := localServerID
	readyEvent := commonEvent.NewReadyEvent(did)
	c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, &readyEvent))

	handshakeEvent := commonEvent.NewHandshakeEvent(did, ddl.GetStartTs()-1, 1, ddl.TableInfo)
	c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, &handshakeEvent))

	var seq atomic.Uint64
	seq.Store(1) // handshake event has seq 1
	for i := 1; i <= totalDML; i++ {
		dml := helper.DML2Event("test", "t", fmt.Sprintf("insert into t values(%d, %d)", i, i))
		require.NotNil(t, dml)
		dml.DispatcherID = did
		dml.Seq = seq.Add(1)
		dml.Epoch = 1
		dml.CommitTs = ddl.FinishedTs + uint64(i)
		c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, dml))
	}

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	select {
	case <-done:
	case <-ctx.Done():
		require.FailNow(t, "timeout waiting for dml events")
	}

	sumDML := 0
	maxBatch := 0
	for sumDML < totalDML {
		select {
		case r := <-batchRecords:
			if r.eventType != commonEvent.TypeDMLEvent {
				continue
			}
			sumDML += r.size
			if r.size > maxBatch {
				maxBatch = r.size
			}
		case <-ctx.Done():
			require.FailNow(t, "timeout collecting dml batch records")
		}
	}

	require.Equal(t, totalDML, sumDML)
	require.Equal(t, 1, maxBatch)
}
