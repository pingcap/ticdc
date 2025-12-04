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
	dispatcher.activeActiveChecked = true

	tableInfo := &common.TableInfo{
		TableName:       common.TableName{Schema: "test", Table: "ddl", TableID: 45},
		SoftDeleteTable: true,
	}
	ddl := &commonEvent.DDLEvent{
		DispatcherID: dispatcher.id,
		TableInfo:    tableInfo,
		FinishedTs:   dispatcher.startTs + 2,
	}
	dispatcher.handleEvents([]DispatcherEvent{{Event: ddl}}, func() {})

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
