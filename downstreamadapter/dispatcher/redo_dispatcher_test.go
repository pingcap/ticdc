// Copyright 2025 PingCAP, Inc.
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

package dispatcher

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

var redoCount atomic.Int32

func redoCallback() {
	redoCount.Add(1)
}

func newRedoDispatcherForTest(sink sink.Sink, tableSpan *heartbeatpb.TableSpan) *RedoDispatcher {
	defaultAtomicity := config.DefaultAtomicityLevel()
	sharedInfo := NewSharedInfo(
		common.NewChangefeedID(common.DefaultKeyspaceNamme),
		"system",
		false,
		false,
		nil,
		nil,
		nil, // redo dispatcher doesn't need syncPointConfig
		&defaultAtomicity,
		false, // enableSplittableCheck
		make(chan TableSpanStatusWithSeq, 128),
		make(chan *heartbeatpb.TableSpanBlockStatus, 128),
		make(chan error, 1),
	)
	return NewRedoDispatcher(
		common.NewDispatcherID(),
		tableSpan,
		common.Ts(0), // startTs
		1,            // schemaID
		NewSchemaIDToDispatchers(),
		false, // skipSyncpointAtStartTs
		false, // skipDMLAsStartTs
		sink,
		sharedInfo,
	)
}

func TestRedoDispatcherHandleEvents(t *testing.T) {
	redoCount.Swap(0)
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent)
	dmlEvent.CommitTs = 2
	dmlEvent.Length = 1

	tableInfo := dmlEvent.TableInfo

	sink := sink.NewMockSink(common.MysqlSinkType)
	tableSpan, err := getCompleteTableSpan(getTestingKeyspaceID())
	require.NoError(t, err)
	dispatcher := newRedoDispatcherForTest(sink, tableSpan)
	require.Equal(t, uint64(0), dispatcher.GetCheckpointTs())
	require.Equal(t, uint64(0), dispatcher.GetResolvedTs())
	tableProgress := dispatcher.tableProgress

	checkpointTs, isEmpty := tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(0), checkpointTs)

	// ===== dml event =====
	nodeID := node.NewID()
	block := dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent)}, redoCallback)
	require.Equal(t, true, block)
	require.Equal(t, 1, len(sink.GetDMLs()))

	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, false, isEmpty)
	require.Equal(t, uint64(1), checkpointTs)
	require.Equal(t, int32(0), redoCount.Load())

	// flush
	sink.FlushDMLs()
	require.Equal(t, 0, len(sink.GetDMLs()))
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(1), checkpointTs)
	require.Equal(t, int32(1), redoCount.Load())

	// ===== ddl event =====
	// 1. non-block ddl event, and don't need to communicate with maintainer
	ddlEvent := &commonEvent.DDLEvent{
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		TableInfo: tableInfo,
	}

	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, redoCallback)
	require.Equal(t, true, block)
	require.Equal(t, 0, len(sink.GetDMLs()))
	time.Sleep(5 * time.Second)
	// no pending event
	blockPendingEvent, blockStage := dispatcher.blockEventStatus.getEventAndStage()
	require.Nil(t, blockPendingEvent)
	require.Equal(t, blockStage, heartbeatpb.BlockStage_NONE)

	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(1), checkpointTs)
	require.Equal(t, int32(2), redoCount.Load())

	// 2.1 non-block ddl event, but need to communicate with maintainer(drop table)
	ddlEvent21 := &commonEvent.DDLEvent{
		FinishedTs: 3,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedDroppedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{1},
		},
		TableInfo: tableInfo,
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent21)}, redoCallback)
	require.Equal(t, true, block)
	require.Equal(t, 0, len(sink.GetDMLs()))
	time.Sleep(5 * time.Second)
	// no pending event
	blockPendingEvent, blockStage = dispatcher.blockEventStatus.getEventAndStage()
	require.Nil(t, blockPendingEvent)
	require.Equal(t, blockStage, heartbeatpb.BlockStage_NONE)

	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(2), checkpointTs)

	require.Equal(t, int32(3), redoCount.Load())

	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	// receive the ack info
	dispatcherStatus := &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    ddlEvent21.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Equal(t, 0, dispatcher.resendTaskMap.Len())

	// 2.2 block ddl event, but need to communicate with maintainer(add table)
	ddlEvent2 := &commonEvent.DDLEvent{
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{
			{
				SchemaID: 1,
				TableID:  1,
			},
		},
		TableInfo: tableInfo,
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent2)}, redoCallback)
	require.Equal(t, true, block)
	require.Equal(t, 0, len(sink.GetDMLs()))
	time.Sleep(5 * time.Second)
	// no pending event
	blockPendingEvent, blockStage = dispatcher.blockEventStatus.getEventAndStage()
	require.Nil(t, blockPendingEvent)
	require.Equal(t, blockStage, heartbeatpb.BlockStage_NONE)
	// but block table progress until ack
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, false, isEmpty)
	require.Equal(t, uint64(3), checkpointTs)
	require.Equal(t, int32(4), redoCount.Load())

	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	// receive the ack info
	// ack for previous ddl event, not cancel this task
	dispatcherStatusPrev := &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    ddlEvent.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatusPrev)
	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    ddlEvent2.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Equal(t, 0, dispatcher.resendTaskMap.Len())

	// clear the event in tableProgress when receive the ack
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(3), checkpointTs)

	// 3. block ddl event
	ddlEvent3 := &commonEvent.DDLEvent{
		FinishedTs: 5,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0, 1},
		},
		TableInfo: tableInfo,
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent3)}, redoCallback)
	require.Equal(t, true, block)
	require.Equal(t, 0, len(sink.GetDMLs()))
	time.Sleep(5 * time.Second)
	// pending event
	blockPendingEvent, blockStage = dispatcher.blockEventStatus.getEventAndStage()
	require.NotNil(t, blockPendingEvent)
	require.Equal(t, blockStage, heartbeatpb.BlockStage_WAITING)

	// the ddl is not available for write to sink
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(3), checkpointTs)
	require.Equal(t, int32(4), redoCount.Load())

	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	// receive the ack info
	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    ddlEvent3.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Equal(t, 0, dispatcher.resendTaskMap.Len())
	// pending event
	blockPendingEvent, blockStage = dispatcher.blockEventStatus.getEventAndStage()
	require.NotNil(t, blockPendingEvent)
	require.Equal(t, blockStage, heartbeatpb.BlockStage_WAITING)

	// the ddl is still not available for write to sink
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(3), checkpointTs)

	// receive the action info
	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Action: &heartbeatpb.DispatcherAction{
			Action:      heartbeatpb.Action_Write,
			CommitTs:    ddlEvent3.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Eventually(t, func() bool {
		checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
		if !isEmpty || checkpointTs != uint64(4) {
			return false
		}
		blockPendingEvent, blockStage = dispatcher.blockEventStatus.getEventAndStage()
		if blockPendingEvent != nil || blockStage != heartbeatpb.BlockStage_NONE {
			return false
		}
		return redoCount.Load() == int32(5)
	}, 5*time.Second, 10*time.Millisecond)

	// ===== resolved event =====
	resolvedEvent := commonEvent.ResolvedEvent{
		ResolvedTs: 7,
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, resolvedEvent)}, redoCallback)
	require.Equal(t, false, block)
	require.Equal(t, 0, len(sink.GetDMLs()))
	require.Equal(t, uint64(7), dispatcher.GetResolvedTs())
	checkpointTs = dispatcher.GetCheckpointTs()
	require.Equal(t, uint64(7), checkpointTs)
}

func TestRedoUncompeleteTableSpanDispatcherHandleEvents(t *testing.T) {
	redoCount.Store(0)
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	sink := sink.NewMockSink(common.MysqlSinkType)
	tableSpan := getUncompleteTableSpan()
	dispatcher := newRedoDispatcherForTest(sink, tableSpan)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent)
	tableInfo := dmlEvent.TableInfo

	// basic ddl event
	ddlEvent := &commonEvent.DDLEvent{
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		TableInfo: tableInfo,
	}

	nodeID := node.NewID()
	block := dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, redoCallback)
	require.Equal(t, true, block)
	time.Sleep(5 * time.Second)
	// pending event
	blockPendingEvent, blockStage := dispatcher.blockEventStatus.getEventAndStage()
	require.NotNil(t, blockPendingEvent)
	require.Equal(t, blockStage, heartbeatpb.BlockStage_WAITING)
	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	checkpointTs := dispatcher.GetCheckpointTs()
	require.Equal(t, uint64(0), checkpointTs)
	require.Equal(t, int32(0), redoCount.Load())

	// receive the ack info
	dispatcherStatus := &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    ddlEvent.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Equal(t, 0, dispatcher.resendTaskMap.Len())
	// pending event
	blockPendingEvent, blockStage = dispatcher.blockEventStatus.getEventAndStage()
	require.NotNil(t, blockPendingEvent)
	require.Equal(t, blockStage, heartbeatpb.BlockStage_WAITING)

	// the ddl is still not available for write to sink
	checkpointTs = dispatcher.GetCheckpointTs()
	require.Equal(t, uint64(0), checkpointTs)
	require.Equal(t, int32(0), redoCount.Load())

	// receive the action info
	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Action: &heartbeatpb.DispatcherAction{
			Action:      heartbeatpb.Action_Write,
			CommitTs:    ddlEvent.FinishedTs,
			IsSyncPoint: false,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Eventually(t, func() bool {
		checkpointTs = dispatcher.GetCheckpointTs()
		return checkpointTs == uint64(1) && redoCount.Load() == int32(1)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestTableTriggerRedoDispatcherInMysql(t *testing.T) {
	redoCount.Store(0)

	ddlTableSpan := common.KeyspaceDDLSpan(common.DefaultKeyspaceID)
	sink := sink.NewMockSink(common.MysqlSinkType)
	tableTriggerEventDispatcher := newRedoDispatcherForTest(sink, ddlTableSpan)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent)
	tableInfo := dmlEvent.TableInfo

	// basic ddl event(non-block)
	ddlEvent := &commonEvent.DDLEvent{
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		TableInfo: tableInfo,
	}

	nodeID := node.NewID()
	block := tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, redoCallback)
	require.Equal(t, true, block)
	time.Sleep(5 * time.Second)
	// no pending event
	blockPendingEvent := tableTriggerEventDispatcher.blockEventStatus.getEvent()
	require.Nil(t, blockPendingEvent)
	require.Equal(t, int32(1), redoCount.Load())

	// ddl influences tableSchemaStore
	ddlEvent = &commonEvent.DDLEvent{
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{
			{
				SchemaID: 1,
				TableID:  1,
			},
		},
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{
				{
					SchemaName: "test",
					TableName:  "t1",
				},
			},
		},
		TableInfo: tableInfo,
	}

	block = tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, redoCallback)
	require.Equal(t, true, block)
	time.Sleep(5 * time.Second)
	// no pending event
	blockPendingEvent = tableTriggerEventDispatcher.blockEventStatus.getEvent()
	require.Nil(t, blockPendingEvent)
	require.Equal(t, int32(2), redoCount.Load())
}

func TestTableTriggerRedoDispatcherInKafka(t *testing.T) {
	redoCount.Store(0)

	ddlTableSpan := common.KeyspaceDDLSpan(common.DefaultKeyspaceID)
	sink := sink.NewMockSink(common.KafkaSinkType)
	tableTriggerEventDispatcher := newRedoDispatcherForTest(sink, ddlTableSpan)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent)
	tableInfo := dmlEvent.TableInfo

	// basic ddl event(non-block)
	ddlEvent := &commonEvent.DDLEvent{
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		TableInfo: tableInfo,
	}

	nodeID := node.NewID()
	block := tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, redoCallback)
	require.Equal(t, true, block)
	time.Sleep(5 * time.Second)
	// no pending event
	blockPendingEvent := tableTriggerEventDispatcher.blockEventStatus.getEvent()
	require.Nil(t, blockPendingEvent)
	require.Equal(t, int32(1), redoCount.Load())

	// ddl influences tableSchemaStore
	ddlEvent = &commonEvent.DDLEvent{
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{
			{
				SchemaID: 1,
				TableID:  1,
			},
		},
		TableNameChange: &commonEvent.TableNameChange{
			AddName: []commonEvent.SchemaTableName{
				{
					SchemaName: "test",
					TableName:  "t1",
				},
			},
		},
		TableInfo: tableInfo,
	}

	block = tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, redoCallback)
	require.Equal(t, true, block)
	time.Sleep(5 * time.Second)
	// no pending event
	blockPendingEvent = tableTriggerEventDispatcher.blockEventStatus.getEvent()
	require.Nil(t, blockPendingEvent)
	require.Equal(t, int32(2), redoCount.Load())
}

func TestRedoDispatcherClose(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent)
	dmlEvent.CommitTs = 2
	dmlEvent.Length = 1

	{
		sink := sink.NewMockSink(common.MysqlSinkType)
		tableSpan, err := getCompleteTableSpan(getTestingKeyspaceID())
		require.NoError(t, err)
		dispatcher := newRedoDispatcherForTest(sink, tableSpan)

		// ===== dml event =====
		nodeID := node.NewID()
		dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent)}, redoCallback)

		_, ok := dispatcher.TryClose()
		require.Equal(t, false, ok)

		// flush
		sink.FlushDMLs()

		watermark, ok := dispatcher.TryClose()
		require.Equal(t, true, ok)
		require.Equal(t, uint64(1), watermark.CheckpointTs)
		require.Equal(t, uint64(0), watermark.ResolvedTs)
	}

	// test sink is not normal
	{
		sink := sink.NewMockSink(common.MysqlSinkType)
		tableSpan, err := getCompleteTableSpan(getTestingKeyspaceID())
		require.NoError(t, err)
		dispatcher := newRedoDispatcherForTest(sink, tableSpan)

		// ===== dml event =====
		nodeID := node.NewID()
		dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent)}, redoCallback)

		_, ok := dispatcher.TryClose()
		require.Equal(t, false, ok)

		sink.SetIsNormal(false)

		watermark, ok := dispatcher.TryClose()
		require.Equal(t, true, ok)
		require.Equal(t, uint64(1), watermark.CheckpointTs)
		require.Equal(t, uint64(0), watermark.ResolvedTs)
	}
}

func TestRedoBatchDMLEventsPartialFlush(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	// Create multiple DML events with different commit timestamps
	dmlEvent1 := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	require.NotNil(t, dmlEvent1)
	dmlEvent1.CommitTs = 10
	dmlEvent1.Length = 1

	dmlEvent2 := helper.DML2Event("test", "t", "insert into t values(2, 2)")
	require.NotNil(t, dmlEvent2)
	dmlEvent2.CommitTs = 11
	dmlEvent2.Length = 1

	dmlEvent3 := helper.DML2Event("test", "t", "insert into t values(3, 3)")
	require.NotNil(t, dmlEvent3)
	dmlEvent3.CommitTs = 12
	dmlEvent3.Length = 1

	mockSink := sink.NewMockSink(common.MysqlSinkType)
	tableSpan, err := getCompleteTableSpan(getTestingKeyspaceID())
	require.NoError(t, err)
	dispatcher := newRedoDispatcherForTest(mockSink, tableSpan)

	// Create a redoCallback that records when it's called
	var redoCallbackCalled bool
	wakeredoCallback := func() {
		redoCallbackCalled = true
	}

	nodeID := node.NewID()

	// Create dispatcher events for all three DML events
	dispatcherEvents := []DispatcherEvent{
		NewDispatcherEvent(&nodeID, dmlEvent1),
		NewDispatcherEvent(&nodeID, dmlEvent2),
		NewDispatcherEvent(&nodeID, dmlEvent3),
	}

	failpoint.Enable("github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockAddDMLEvents", `pause`)

	go func() {
		block := dispatcher.HandleEvents(dispatcherEvents, wakeredoCallback)
		require.Equal(t, true, block)
	}()

	time.Sleep(1 * time.Second)
	require.Equal(t, 1, len(mockSink.GetDMLs()))
	mockSink.FlushDMLs()
	require.False(t, redoCallbackCalled)

	failpoint.Disable("github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockAddDMLEvents")

	time.Sleep(1 * time.Second)
	require.Equal(t, 2, len(mockSink.GetDMLs()))
	mockSink.FlushDMLs()
	// Now the redoCallback should be called after all events are flushed
	require.True(t, redoCallbackCalled)

	// Verify that all events were actually flushed
	require.Equal(t, 0, len(mockSink.GetDMLs()))
}
