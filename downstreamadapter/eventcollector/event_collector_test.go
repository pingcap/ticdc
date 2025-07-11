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
	"github.com/stretchr/testify/require"
)

var _ dispatcher.DispatcherService = (*mockEventDispatcher)(nil)

type mockEventDispatcher struct {
	id        common.DispatcherID
	tableSpan *heartbeatpb.TableSpan
	handle    func(commonEvent.Event)
}

func (m *mockEventDispatcher) GetId() common.DispatcherID {
	return m.id
}

func (m *mockEventDispatcher) GetType() int {
	return dispatcher.TypeDispatcherEvent
}

func (m *mockEventDispatcher) GetStartTs() uint64 {
	return 0
}

func (m *mockEventDispatcher) GetBDRMode() bool {
	return false
}

func (m *mockEventDispatcher) GetChangefeedID() common.ChangeFeedID {
	return common.NewChangefeedID()
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

func (m *mockEventDispatcher) GetStartTsIsSyncpoint() bool {
	return false
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
		c.runDispatchMessage(ctx, ch)
	}()

	var seq atomic.Uint64
	seq.Store(0)
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

	readyEvent := commonEvent.NewReadyEvent(did, false)
	handshakeEvent := commonEvent.NewHandshakeEvent(did, 0, ddl.GetStartTs()-1, 1, ddl.TableInfo, false)
	events := make(map[uint64]commonEvent.Event)
	ddl.DispatcherID = did
	handshakeEvent.Seq = seq.Add(1)
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
	c.AddDispatcher(d, config.GetDefaultReplicaConfig().MemoryQuota)

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
