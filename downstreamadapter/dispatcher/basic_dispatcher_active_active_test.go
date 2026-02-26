// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package dispatcher

import (
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestHandleEventsRejectActiveActiveTableWhenDisabled(t *testing.T) {
	dispatcher := newTestBasicDispatcher(t, common.MysqlSinkType, false)

	tableInfo := &common.TableInfo{
		TableName:         common.TableName{Schema: "test", Table: "t", TableID: 42},
		ActiveActiveTable: true,
	}
	dml := commonEvent.NewDMLEvent(dispatcher.id, tableInfo.TableName.TableID, dispatcher.startTs+1, dispatcher.startTs+2, tableInfo)
	resolved := commonEvent.NewResolvedEvent(dispatcher.startTs+1, dispatcher.id, 0)

	events := []DispatcherEvent{
		{Event: resolved},
		{Event: dml},
	}
	block := dispatcher.handleEvents(events, func() {})
	require.False(t, block)

	select {
	case err := <-dispatcher.sharedInfo.errCh:
		require.Contains(t, err.Error(), "active-active")
	default:
		t.Fatalf("expected active-active error")
	}
}

func TestHandleEventsRejectSoftDeleteTableWhenDisabled(t *testing.T) {
	dispatcher := newTestBasicDispatcher(t, common.MysqlSinkType, false)

	tableInfo := &common.TableInfo{
		TableName:       common.TableName{Schema: "test", Table: "soft", TableID: 43},
		SoftDeleteTable: true,
	}
	dml := commonEvent.NewDMLEvent(dispatcher.id, tableInfo.TableName.TableID, dispatcher.startTs+1, dispatcher.startTs+2, tableInfo)
	resolved := commonEvent.NewResolvedEvent(dispatcher.startTs+1, dispatcher.id, 0)

	events := []DispatcherEvent{
		{Event: resolved},
		{Event: dml},
	}
	block := dispatcher.handleEvents(events, func() {})
	require.False(t, block)

	select {
	case err := <-dispatcher.sharedInfo.errCh:
		require.Contains(t, err.Error(), "soft delete")
	default:
		t.Fatalf("expected soft delete error")
	}
}

func TestHandleEventsIgnoreSpecialTableOnNonMySQLSink(t *testing.T) {
	dispatcher := newTestBasicDispatcher(t, common.KafkaSinkType, false)

	tableInfo := &common.TableInfo{
		TableName:         common.TableName{Schema: "test", Table: "t", TableID: 44},
		ActiveActiveTable: true,
		SoftDeleteTable:   true,
	}
	dml := commonEvent.NewDMLEvent(dispatcher.id, tableInfo.TableName.TableID, dispatcher.startTs+1, dispatcher.startTs+2, tableInfo)
	resolved := commonEvent.NewResolvedEvent(dispatcher.startTs+1, dispatcher.id, 0)

	events := []DispatcherEvent{
		{Event: resolved},
		{Event: dml},
	}
	block := dispatcher.handleEvents(events, func() {})
	require.False(t, block)

	select {
	case err := <-dispatcher.sharedInfo.errCh:
		t.Fatalf("unexpected error: %v", err)
	default:
	}
}

func TestDDLEventsAlwaysValidateActiveActive(t *testing.T) {
	dispatcher := newTestBasicDispatcher(t, common.MysqlSinkType, false)
	dispatcher.tableModeCompatibilityChecked = true

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	event := helper.DDL2Event(createTableSQL)
	tableInfo := event.TableInfo
	tableInfo.SoftDeleteTable = true

	ddl := &commonEvent.DDLEvent{
		DispatcherID: dispatcher.id,
		TableInfo:    tableInfo,
		FinishedTs:   dispatcher.startTs + 2,
	}
	dispatcher.handleEvents([]DispatcherEvent{{Event: ddl}}, func() {})

	require.Equal(t, false, dispatcher.tableModeCompatibilityChecked, "DDL events should reset tableModeCompatibilityChecked")
	dml := commonEvent.NewDMLEvent(dispatcher.id, tableInfo.TableName.TableID, dispatcher.startTs+3, dispatcher.startTs+4, tableInfo)
	dispatcher.handleEvents([]DispatcherEvent{{Event: dml}}, func() {})
	select {
	case err := <-dispatcher.sharedInfo.errCh:
		require.Contains(t, err.Error(), "soft delete")
	default:
		t.Fatalf("expected DDL validation error")
	}
}

func newTestBasicDispatcher(t *testing.T, sinkType common.SinkType, enableActiveActive bool) *BasicDispatcher {
	t.Helper()
	statuses := make(chan TableSpanStatusWithSeq, 2)
	blockStatuses := make(chan *heartbeatpb.TableSpanBlockStatus, 1)
	errCh := make(chan error, 1)
	sharedInfo := NewSharedInfo(
		common.NewChangefeedID("test"),
		"",
		false,
		enableActiveActive,
		false,
		nil,
		nil,
		nil,
		nil,
		false,
		1,
		0,
		statuses,
		blockStatuses,
		errCh,
	)
	dispatcherSink := sink.NewMockSink(sinkType)
	tableSpan := &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{0}, EndKey: []byte{1}}
	dispatcher := NewBasicDispatcher(
		common.NewDispatcherID(),
		tableSpan,
		100,
		1,
		NewSchemaIDToDispatchers(),
		false,
		false,
		200,
		common.DefaultMode,
		dispatcherSink,
		sharedInfo,
	)
	return dispatcher
}
