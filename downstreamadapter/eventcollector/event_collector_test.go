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
	changefeedID             common.ChangeFeedID
	eventCollectorBatchCount int
	eventCollectorBatchBytes int
	batchSizes               chan int
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

func (d *mockEventDispatcher) GetEventCollectorBatchCount() int {
	return d.eventCollectorBatchCount
}

func (d *mockEventDispatcher) GetEventCollectorBatchBytes() int {
	return d.eventCollectorBatchBytes
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
	if m.batchSizes != nil {
		m.batchSizes <- len(dispatcherEvents)
	}
	if m.batchRecords != nil && len(dispatcherEvents) > 0 {
		m.batchRecords <- batchRecord{
			size:      len(dispatcherEvents),
			eventType: dispatcherEvents[0].GetType(),
		}
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

func TestEventCollectorBatchingByCount(t *testing.T) {
	ctx := context.Background()
	nodeInfo := node.NewInfo("127.0.0.1:18300", "")
	mc := messaging.NewMessageCenter(ctx, nodeInfo.ID, config.NewDefaultMessageCenterConfig(nodeInfo.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	c := New(nodeInfo.ID)
	defer c.ds.Close()
	defer c.redoDs.Close()

	did := common.NewDispatcherID()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	ddl := helper.DDL2Event("create table t(id int primary key, v int)")
	require.NotNil(t, ddl)

	const totalDML = 10
	const batchCount = 3
	batchSizes := make(chan int, 128)
	done := make(chan struct{})

	var seen atomic.Int64
	d := &mockEventDispatcher{
		id:                       did,
		tableSpan:                &heartbeatpb.TableSpan{TableID: 1},
		changefeedID:             common.NewChangefeedID(common.DefaultKeyspaceName),
		eventCollectorBatchCount: batchCount,
		eventCollectorBatchBytes: 0,
		batchSizes:               batchSizes,
	}
	d.handle = func(e commonEvent.Event) {
		if seen.Add(1) == totalDML {
			close(done)
		}
	}
	c.AddDispatcher(d, util.GetOrZero(config.GetDefaultReplicaConfig().MemoryQuota))

	from := nodeInfo.ID
	readyEvent := commonEvent.NewReadyEvent(did)
	c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, &readyEvent))

	handshakeEvent := commonEvent.NewHandshakeEvent(did, ddl.GetStartTs()-1, 1, ddl.TableInfo)
	c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, &handshakeEvent))

	var seq atomic.Uint64
	seq.Store(1) // handshake event has seq 1
	dmls := make([]*commonEvent.DMLEvent, 0, totalDML)
	for i := 1; i <= totalDML; i++ {
		dml := helper.DML2Event("test", "t", fmt.Sprintf("insert into t values(%d, %d)", i, i))
		require.NotNil(t, dml)
		dml.DispatcherID = did
		dml.Seq = seq.Add(1)
		dml.Epoch = 1
		dml.CommitTs = ddl.FinishedTs + uint64(i)
		dmls = append(dmls, dml)
	}
	for _, dml := range dmls {
		c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, dml))
	}

	ctx1, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	select {
	case <-done:
	case <-ctx1.Done():
		require.Fail(t, "timeout")
	}

	sum := 0
	maxBatch := 0
	hasBatch := false
	for sum < totalDML {
		select {
		case n := <-batchSizes:
			sum += n
			if n > maxBatch {
				maxBatch = n
			}
			if n > 1 {
				hasBatch = true
			}
		case <-ctx1.Done():
			require.Fail(t, "timeout collecting batch sizes")
		}
	}

	require.True(t, hasBatch)
	require.LessOrEqual(t, maxBatch, batchCount)
	require.Equal(t, totalDML, sum)
}

func TestEventCollectorBatchingByBytes(t *testing.T) {
	ctx := context.Background()
	nodeInfo := node.NewInfo("127.0.0.1:18300", "")
	mc := messaging.NewMessageCenter(ctx, nodeInfo.ID, config.NewDefaultMessageCenterConfig(nodeInfo.AdvertiseAddr), nil)
	mc.Run(ctx)
	defer mc.Close()
	appcontext.SetService(appcontext.MessageCenter, mc)

	c := New(nodeInfo.ID)
	defer c.ds.Close()
	defer c.redoDs.Close()

	did := common.NewDispatcherID()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	ddl := helper.DDL2Event("create table t(id int primary key, v int)")
	require.NotNil(t, ddl)

	const totalDML = 12
	batchRecords := make(chan batchRecord, 128)
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

	from := nodeInfo.ID
	readyEvent := commonEvent.NewReadyEvent(did)
	c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, &readyEvent))

	handshakeEvent := commonEvent.NewHandshakeEvent(did, ddl.GetStartTs()-1, 1, ddl.TableInfo)
	c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, &handshakeEvent))

	var seq atomic.Uint64
	seq.Store(1) // handshake event has seq 1
	dmls := make([]*commonEvent.DMLEvent, 0, totalDML)
	for i := 1; i <= totalDML; i++ {
		dml := helper.DML2Event("test", "t", fmt.Sprintf("insert into t values(%d, %d)", i, i))
		require.NotNil(t, dml)
		dml.DispatcherID = did
		dml.Seq = seq.Add(1)
		dml.Epoch = 1
		dml.CommitTs = ddl.FinishedTs + uint64(i)
		dmls = append(dmls, dml)
	}
	for _, dml := range dmls {
		c.ds.Push(did, dispatcher.NewDispatcherEvent(&from, dml))
	}

	ctx1, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	select {
	case <-done:
	case <-ctx1.Done():
		require.Fail(t, "timeout")
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
		case <-ctx1.Done():
			require.Fail(t, "timeout collecting dml batch records")
		}
	}

	require.Equal(t, totalDML, sumDML)
	require.Equal(t, 1, maxBatch)
}
