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
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

var defaultAtomicity = config.DefaultAtomicityLevel()

func getCompleteTableSpanWithTableID(keyspaceID uint32, tableID int64) (*heartbeatpb.TableSpan, error) {
	tableSpan := &heartbeatpb.TableSpan{
		KeyspaceID: keyspaceID,
		TableID:    tableID,
	}
	startKey, endKey, err := common.GetKeyspaceTableRange(keyspaceID, tableSpan.TableID)
	if err != nil {
		return nil, err
	}
	tableSpan.StartKey = common.ToComparableKey(startKey)
	tableSpan.EndKey = common.ToComparableKey(endKey)
	return tableSpan, nil
}

func getTestingKeyspaceID() uint32 {
	if kerneltype.IsClassic() {
		return 0
	}
	return 1
}

func getCompleteTableSpan(keyspaceID uint32) (*heartbeatpb.TableSpan, error) {
	return getCompleteTableSpanWithTableID(keyspaceID, 1)
}

func getUncompleteTableSpan() *heartbeatpb.TableSpan {
	return &heartbeatpb.TableSpan{
		TableID: 1,
	}
}

func newDispatcherForTest(sink sink.Sink, tableSpan *heartbeatpb.TableSpan) *EventDispatcher {
	var redoTs atomic.Uint64
	redoTs.Store(math.MaxUint64)
	sharedInfo := NewSharedInfo(
		common.NewChangefeedID(common.DefaultKeyspaceNamme),
		"system",
		false,
		false,
		nil,
		nil,
		&syncpoint.SyncPointConfig{
			SyncPointInterval:  time.Duration(5 * time.Second),
			SyncPointRetention: time.Duration(10 * time.Minute),
		}, // syncPointConfig
		&defaultAtomicity,
		false, // enableSplittableCheck
		make(chan TableSpanStatusWithSeq, 128),
		make(chan *heartbeatpb.TableSpanBlockStatus, 128),
		make(chan error, 1),
	)
	return NewEventDispatcher(
		common.NewDispatcherID(),
		tableSpan,
		common.Ts(0), // startTs
		1,            // schemaID
		NewSchemaIDToDispatchers(),
		false,        // skipSyncpointAtStartTs
		false,        // skipDMLAsStartTs
		common.Ts(0), // pdTs
		sink,
		sharedInfo,
		false,
		&redoTs,
	)
}

var count atomic.Int32

func callback() {
	count.Add(1)
}

// test different events can be correctly handled by the dispatcher
func TestDispatcherHandleEvents(t *testing.T) {
	count.Swap(0)
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
	dispatcher := newDispatcherForTest(sink, tableSpan)
	require.Equal(t, uint64(0), dispatcher.GetCheckpointTs())
	require.Equal(t, uint64(0), dispatcher.GetResolvedTs())
	tableProgress := dispatcher.tableProgress

	checkpointTs, isEmpty := tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(0), checkpointTs)

	// ===== dml event =====
	nodeID := node.NewID()
	block := dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent)}, callback)
	require.Equal(t, true, block)
	require.Equal(t, 1, len(sink.GetDMLs()))

	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, false, isEmpty)
	require.Equal(t, uint64(1), checkpointTs)
	require.Equal(t, int32(0), count.Load())

	// flush
	sink.FlushDMLs()
	require.Equal(t, 0, len(sink.GetDMLs()))
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(1), checkpointTs)
	require.Equal(t, int32(1), count.Load())

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

	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
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
	require.Equal(t, int32(2), count.Load())

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
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent21)}, callback)
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
	require.Equal(t, int32(3), count.Load())

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
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent2)}, callback)
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
	require.Equal(t, int32(4), count.Load())

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
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent3)}, callback)
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
	time.Sleep(5 * time.Second)
	require.Equal(t, int32(4), count.Load())

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
		return count.Load() == int32(5)
	}, 5*time.Second, 10*time.Millisecond)

	// ===== sync point event =====

	syncPointEvent := &commonEvent.SyncPointEvent{
		CommitTs: 6,
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, syncPointEvent)}, callback)
	require.Equal(t, true, block)
	time.Sleep(5 * time.Second)
	require.Equal(t, 0, len(sink.GetDMLs()))
	// pending event
	blockPendingEvent, blockStage = dispatcher.blockEventStatus.getEventAndStage()
	require.NotNil(t, blockPendingEvent)
	require.Equal(t, blockStage, heartbeatpb.BlockStage_WAITING)

	// not available for write to sink
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(4), checkpointTs)

	// receive the ack info
	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    syncPointEvent.GetCommitTs(),
			IsSyncPoint: true,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	require.Equal(t, 0, dispatcher.resendTaskMap.Len())
	// pending event
	blockPendingEvent, blockStage = dispatcher.blockEventStatus.getEventAndStage()
	require.NotNil(t, blockPendingEvent)
	require.Equal(t, blockStage, heartbeatpb.BlockStage_WAITING)

	// receive the action info
	dispatcherStatus = &heartbeatpb.DispatcherStatus{
		Action: &heartbeatpb.DispatcherAction{
			Action:      heartbeatpb.Action_Pass,
			CommitTs:    syncPointEvent.GetCommitTs(),
			IsSyncPoint: true,
		},
	}
	dispatcher.HandleDispatcherStatus(dispatcherStatus)
	checkpointTs, isEmpty = tableProgress.GetCheckpointTs()
	require.Equal(t, true, isEmpty)
	require.Equal(t, uint64(5), checkpointTs)
	require.Equal(t, int32(6), count.Load())

	// ===== resolved event =====
	checkpointTs = dispatcher.GetCheckpointTs()
	require.Equal(t, uint64(5), checkpointTs)
	resolvedEvent := commonEvent.ResolvedEvent{
		ResolvedTs: 7,
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, resolvedEvent)}, callback)
	require.Equal(t, false, block)
	require.Equal(t, 0, len(sink.GetDMLs()))
	require.Equal(t, uint64(7), dispatcher.GetResolvedTs())
	checkpointTs = dispatcher.GetCheckpointTs()
	require.Equal(t, uint64(7), checkpointTs)
}

// test uncompelete table span can correctly handle the ddl events
func TestUncompeleteTableSpanDispatcherHandleEvents(t *testing.T) {
	count.Swap(0)
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlJob := helper.DDL2Job("create table t(id int primary key, v int)")
	require.NotNil(t, ddlJob)

	sink := sink.NewMockSink(common.MysqlSinkType)
	tableSpan := getUncompleteTableSpan()
	dispatcher := newDispatcherForTest(sink, tableSpan)

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
	block := dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
	require.Equal(t, true, block)
	time.Sleep(5 * time.Second)
	// pending event
	blockPendingEvent, blockStage := dispatcher.blockEventStatus.getEventAndStage()
	require.NotNil(t, blockPendingEvent)
	require.Equal(t, blockStage, heartbeatpb.BlockStage_WAITING)
	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	checkpointTs := dispatcher.GetCheckpointTs()
	require.Equal(t, uint64(0), checkpointTs)
	require.Equal(t, int32(0), count.Load())

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
	require.Equal(t, int32(0), count.Load())

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
		return checkpointTs == uint64(1) && count.Load() == int32(1)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestTableTriggerEventDispatcherInMysql(t *testing.T) {
	count.Swap(0)

	ddlTableSpan := common.KeyspaceDDLSpan(common.DefaultKeyspaceID)
	sink := sink.NewMockSink(common.MysqlSinkType)
	tableTriggerEventDispatcher := newDispatcherForTest(sink, ddlTableSpan)
	require.Nil(t, tableTriggerEventDispatcher.tableSchemaStore)

	ok, err := tableTriggerEventDispatcher.InitializeTableSchemaStore([]*heartbeatpb.SchemaInfo{})
	require.NoError(t, err)
	require.True(t, ok)

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
	block := tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
	require.Equal(t, true, block)
	time.Sleep(5 * time.Second)
	// no pending event
	blockPendingEvent := tableTriggerEventDispatcher.blockEventStatus.getEvent()
	require.Nil(t, blockPendingEvent)
	require.Equal(t, int32(1), count.Load())

	tableIds := tableTriggerEventDispatcher.tableSchemaStore.GetAllTableIds()
	require.Equal(t, 1, len(tableIds))
	require.Equal(t, int64(0), tableIds[0])

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

	block = tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
	require.Equal(t, true, block)
	time.Sleep(5 * time.Second)
	// no pending event
	blockPendingEvent = tableTriggerEventDispatcher.blockEventStatus.getEvent()
	require.Nil(t, blockPendingEvent)
	require.Equal(t, int32(2), count.Load())

	tableIds = tableTriggerEventDispatcher.tableSchemaStore.GetAllTableIds()
	require.Equal(t, int(2), len(tableIds))
	require.Equal(t, int64(1), tableIds[0])
	require.Equal(t, int64(0), tableIds[1])
}

func TestTableTriggerEventDispatcherInKafka(t *testing.T) {
	count.Swap(0)

	ddlTableSpan := common.KeyspaceDDLSpan(common.DefaultKeyspaceID)
	sink := sink.NewMockSink(common.KafkaSinkType)
	tableTriggerEventDispatcher := newDispatcherForTest(sink, ddlTableSpan)
	require.Nil(t, tableTriggerEventDispatcher.tableSchemaStore)

	ok, err := tableTriggerEventDispatcher.InitializeTableSchemaStore([]*heartbeatpb.SchemaInfo{})
	require.NoError(t, err)
	require.True(t, ok)

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
	block := tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
	require.Equal(t, true, block)
	time.Sleep(5 * time.Second)
	// no pending event
	blockPendingEvent := tableTriggerEventDispatcher.blockEventStatus.getEvent()
	require.Nil(t, blockPendingEvent)
	require.Equal(t, int32(1), count.Load())

	tableNames := tableTriggerEventDispatcher.tableSchemaStore.GetAllTableNames(2)
	require.Equal(t, int(0), len(tableNames))

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

	block = tableTriggerEventDispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, ddlEvent)}, callback)
	require.Equal(t, true, block)
	time.Sleep(5 * time.Second)
	// no pending event
	blockPendingEvent = tableTriggerEventDispatcher.blockEventStatus.getEvent()
	require.Nil(t, blockPendingEvent)
	require.Equal(t, int32(2), count.Load())

	tableNames = tableTriggerEventDispatcher.tableSchemaStore.GetAllTableNames(3)
	require.Equal(t, int(0), len(tableNames))
	tableNames = tableTriggerEventDispatcher.tableSchemaStore.GetAllTableNames(4)
	require.Equal(t, int(1), len(tableNames))
	require.Equal(t, commonEvent.SchemaTableName{SchemaName: "test", TableName: "t1"}, *tableNames[0])
}

// ensure the dispatcher will be closed when no dml events is in sink
func TestDispatcherClose(t *testing.T) {
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
		dispatcher := newDispatcherForTest(sink, tableSpan)

		// ===== dml event =====
		nodeID := node.NewID()
		dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent)}, callback)

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
		dispatcher := newDispatcherForTest(sink, tableSpan)

		// ===== dml event =====
		nodeID := node.NewID()
		dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent)}, callback)

		_, ok := dispatcher.TryClose()
		require.Equal(t, false, ok)

		sink.SetIsNormal(false)

		watermark, ok := dispatcher.TryClose()
		require.Equal(t, true, ok)
		require.Equal(t, uint64(1), watermark.CheckpointTs)
		require.Equal(t, uint64(0), watermark.ResolvedTs)
	}
}

// TestBatchDMLEventsPartialFlush tests that wakeCallback is called correctly
// when DML events are flushed partially in multiple batches.
func TestBatchDMLEventsPartialFlush(t *testing.T) {
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
	dispatcher := newDispatcherForTest(mockSink, tableSpan)

	// Create a callback that records when it's called
	var callbackCalled bool
	wakeCallback := func() {
		callbackCalled = true
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
		block := dispatcher.HandleEvents(dispatcherEvents, wakeCallback)
		require.Equal(t, true, block)
	}()

	time.Sleep(1 * time.Second)
	require.Equal(t, 1, len(mockSink.GetDMLs()))
	mockSink.FlushDMLs()
	require.False(t, callbackCalled)

	failpoint.Disable("github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockAddDMLEvents")

	time.Sleep(1 * time.Second)
	require.Equal(t, 2, len(mockSink.GetDMLs()))
	mockSink.FlushDMLs()
	// Now the callback should be called after all events are flushed
	require.True(t, callbackCalled)

	// Verify that all events were actually flushed
	require.Equal(t, 0, len(mockSink.GetDMLs()))
}

// TestDispatcherSplittableCheck tests that a split table dispatcher with enableSplittableCheck=true
// correctly reports an error when receiving a DDL that breaks splittable
func TestDispatcherSplittableCheck(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	// Create a table with primary key and unique key (not splittable)
	ddlJob := helper.DDL2Job("CREATE TABLE t (id INT PRIMARY KEY, email VARCHAR(100) UNIQUE)")
	require.NotNil(t, ddlJob)

	// Get table info from the DDL job
	tableInfo := helper.GetModelTableInfo(ddlJob)
	require.NotNil(t, tableInfo)

	// Convert to common.TableInfo
	commonTableInfo := common.WrapTableInfo("test", tableInfo)
	require.NotNil(t, commonTableInfo)

	// Verify that this table is not splittable
	require.False(t, commonEvent.IsSplitable(commonTableInfo))

	// Create a mock sink
	sink := sink.NewMockSink(common.MysqlSinkType)

	// Create an incomplete table span (split table)
	tableSpan := getUncompleteTableSpan()

	// Create shared info with enableSplittableCheck=true
	sharedInfo := NewSharedInfo(
		common.NewChangefeedID(common.DefaultKeyspaceNamme),
		"system",
		false,
		false,
		nil,
		nil,
		&syncpoint.SyncPointConfig{
			SyncPointInterval:  time.Duration(5 * time.Second),
			SyncPointRetention: time.Duration(10 * time.Minute),
		},
		&defaultAtomicity,
		true, // enableSplittableCheck = true
		make(chan TableSpanStatusWithSeq, 128),
		make(chan *heartbeatpb.TableSpanBlockStatus, 128),
		make(chan error, 1),
	)

	// Create dispatcher with the split table span
	var redoTs atomic.Uint64
	redoTs.Store(math.MaxUint64)
	dispatcher := NewEventDispatcher(
		common.NewDispatcherID(),
		tableSpan,
		common.Ts(0), // startTs
		1,            // schemaID
		NewSchemaIDToDispatchers(),
		false,        // skipSyncpointAtStartTs
		false,        // skipDMLAsStartTs
		common.Ts(0), // pdTs
		sink,
		sharedInfo,
		false,
		&redoTs,
	)

	// Verify that the dispatcher is not a complete table (it's split)
	require.False(t, dispatcher.isCompleteTable)

	// Create a DDL event that will break splittable
	ddlEvent := &commonEvent.DDLEvent{
		FinishedTs: 2,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{1},
		},
		TableInfo: commonTableInfo,
		Query:     "ALTER TABLE t ADD COLUMN new_col INT",
	}

	// Create dispatcher event
	nodeID := node.NewID()
	dispatcherEvent := NewDispatcherEvent(&nodeID, ddlEvent)

	// Create a channel to capture errors
	errCh := make(chan error, 1)

	// Replace the error channel in shared info to capture errors
	dispatcher.sharedInfo.errCh = errCh

	// Handle the DDL event
	block := dispatcher.HandleEvents([]DispatcherEvent{dispatcherEvent}, func() {})

	// The event should be blocked
	require.True(t, block)

	// Check that an error was reported
	select {
	case err := <-errCh:
		// Verify that the error is the expected splittable error
		require.Contains(t, err.Error(), "unexpected ddl event; This ddl event will break splitable of this table. Only table with pk and no uk can be split.")
	case <-time.After(1 * time.Second):
		require.Fail(t, "Expected error to be reported within 1 second")
	}
}

// TestDispatcher_SkipDMLAsStartTs_FilterCorrectly tests DML filtering during DDL crash recovery.
// When skipDMLAsStartTs=true and startTs=99, DML events at commitTs=100 (startTs+1) should be skipped.
func TestDispatcher_SkipDMLAsStartTs_FilterCorrectly(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Job("create table t(id int primary key, v int)")

	// Create DML events with different commitTs
	dmlEvent99 := helper.DML2Event("test", "t", "insert into t values(1, 1)")
	dmlEvent99.CommitTs = 99
	dmlEvent99.Length = 1

	dmlEvent100 := helper.DML2Event("test", "t", "insert into t values(2, 2)")
	dmlEvent100.CommitTs = 100
	dmlEvent100.Length = 1

	dmlEvent101 := helper.DML2Event("test", "t", "insert into t values(3, 3)")
	dmlEvent101.CommitTs = 101
	dmlEvent101.Length = 1

	mockSink := sink.NewMockSink(common.MysqlSinkType)
	tableSpan, err := getCompleteTableSpan(getTestingKeyspaceID())
	require.NoError(t, err)

	// Create dispatcher with skipDMLAsStartTs=true, startTs=99
	// This simulates DDL crash recovery where:
	// - DDL commitTs = 100
	// - We start from ddlTs-1 = 99
	// - Need to skip DML at commitTs = 100 (already written before crash)
	var redoTs atomic.Uint64
	redoTs.Store(math.MaxUint64)
	sharedInfo := NewSharedInfo(
		common.NewChangefeedID(common.DefaultKeyspaceNamme),
		"system",
		false,
		false,
		nil,
		nil,
		&syncpoint.SyncPointConfig{
			SyncPointInterval:  time.Duration(5 * time.Second),
			SyncPointRetention: time.Duration(10 * time.Minute),
		},
		&defaultAtomicity,
		false,
		make(chan TableSpanStatusWithSeq, 128),
		make(chan *heartbeatpb.TableSpanBlockStatus, 128),
		make(chan error, 1),
	)

	dispatcher := NewEventDispatcher(
		common.NewDispatcherID(),
		tableSpan,
		common.Ts(99), // startTs = 99 (ddlTs - 1)
		1,             // schemaID
		NewSchemaIDToDispatchers(),
		false, // skipSyncpointAtStartTs
		true,  // skipDMLAsStartTs = true (KEY: enable DML filtering)
		common.Ts(99),
		mockSink,
		sharedInfo,
		false,
		&redoTs,
	)

	nodeID := node.NewID()

	// Test 1: DML at commitTs=99 should NOT be skipped (less than startTs+1)
	block := dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent99)}, func() {})
	require.True(t, block)
	require.Equal(t, 1, len(mockSink.GetDMLs()), "DML at commitTs=99 should be processed")
	mockSink.FlushDMLs()

	// Test 2: DML at commitTs=100 SHOULD be skipped (equals startTs+1)
	// This is the critical test - DML at ddlTs should be filtered
	dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent100)}, func() {})
	// Note: block return value may be false when event is skipped
	require.Equal(t, 0, len(mockSink.GetDMLs()), "DML at commitTs=100 should be skipped (already written before crash)")

	// Test 3: DML at commitTs=101 should NOT be skipped (greater than startTs+1)
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent101)}, func() {})
	require.True(t, block)
	require.Equal(t, 1, len(mockSink.GetDMLs()), "DML at commitTs=101 should be processed")
	mockSink.FlushDMLs()

	// Verify checkpoint advances correctly
	checkpointTs, isEmpty := dispatcher.GetCheckpointTs(), false
	require.False(t, isEmpty)
	require.Greater(t, checkpointTs, uint64(99), "Checkpoint should advance beyond startTs")
}

// TestDispatcher_SkipDMLAsStartTs_Disabled tests that DML is not filtered when skipDMLAsStartTs=false
func TestDispatcher_SkipDMLAsStartTs_Disabled(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Job("create table t(id int primary key, v int)")

	// Create DML event at commitTs=100
	dmlEvent100 := helper.DML2Event("test", "t", "insert into t values(2, 2)")
	dmlEvent100.CommitTs = 100
	dmlEvent100.Length = 1

	mockSink := sink.NewMockSink(common.MysqlSinkType)
	tableSpan, err := getCompleteTableSpan(getTestingKeyspaceID())
	require.NoError(t, err)

	// Create dispatcher with skipDMLAsStartTs=false
	var redoTs atomic.Uint64
	redoTs.Store(math.MaxUint64)
	sharedInfo := NewSharedInfo(
		common.NewChangefeedID(common.DefaultKeyspaceNamme),
		"system",
		false,
		false,
		nil,
		nil,
		&syncpoint.SyncPointConfig{
			SyncPointInterval:  time.Duration(5 * time.Second),
			SyncPointRetention: time.Duration(10 * time.Minute),
		},
		&defaultAtomicity,
		false,
		make(chan TableSpanStatusWithSeq, 128),
		make(chan *heartbeatpb.TableSpanBlockStatus, 128),
		make(chan error, 1),
	)

	dispatcher := NewEventDispatcher(
		common.NewDispatcherID(),
		tableSpan,
		common.Ts(99), // startTs = 99
		1,
		NewSchemaIDToDispatchers(),
		false, // skipSyncpointAtStartTs
		false, // skipDMLAsStartTs = false (KEY: DML filtering disabled)
		common.Ts(99),
		mockSink,
		sharedInfo,
		false,
		&redoTs,
	)

	nodeID := node.NewID()

	// DML at commitTs=100 should NOT be skipped when skipDMLAsStartTs=false
	block := dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dmlEvent100)}, func() {})
	require.True(t, block)
	require.Equal(t, 1, len(mockSink.GetDMLs()), "DML at commitTs=100 should be processed when skipDMLAsStartTs=false")
}

func TestHoldBlockEventUntilNoResendTasks(t *testing.T) {
	keyspaceID := getTestingKeyspaceID()
	ddlTableSpan := common.KeyspaceDDLSpan(keyspaceID)
	mockSink := sink.NewMockSink(common.MysqlSinkType)
	dispatcher := newDispatcherForTest(mockSink, ddlTableSpan)

	nodeID := node.NewID()

	// A non-blocking DDL that adds a new table is tracked in resendTaskMap until maintainer ACKs
	// (ACK implies scheduling is completed).
	createTableDDL := &commonEvent.DDLEvent{
		FinishedTs: 10,
		StartTs:    10,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{
			{SchemaID: 1, TableID: 101},
		},
	}
	block := dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, createTableDDL)}, func() {})
	require.True(t, block)

	select {
	case msg := <-dispatcher.GetBlockStatusesChan():
		require.False(t, msg.State.IsBlocked)
		require.False(t, msg.State.IsSyncPoint)
		require.Equal(t, uint64(10), msg.State.BlockTs)
	case <-time.After(time.Second):
		require.FailNow(t, "expected add-table block status")
	}
	require.Equal(t, 1, dispatcher.resendTaskMap.Len())

	// A DB/All block event must be deferred until resendTaskMap becomes empty,
	// otherwise maintainer may build an incomplete range checker.
	dropDBDDL := &commonEvent.DDLEvent{
		FinishedTs: 20,
		StartTs:    20,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeDB,
			SchemaID:      1,
		},
	}
	block = dispatcher.HandleEvents([]DispatcherEvent{NewDispatcherEvent(&nodeID, dropDBDDL)}, func() {})
	require.True(t, block)

	select {
	case msg := <-dispatcher.GetBlockStatusesChan():
		require.FailNow(t, "unexpected block status", "received=%v", msg)
	case <-time.After(200 * time.Millisecond):
	}

	// Simulate maintainer ACK for the create table scheduling message.
	dispatcher.HandleDispatcherStatus(&heartbeatpb.DispatcherStatus{
		Ack: &heartbeatpb.ACK{
			CommitTs:    10,
			IsSyncPoint: false,
		},
	})

	select {
	case msg := <-dispatcher.GetBlockStatusesChan():
		require.True(t, msg.State.IsBlocked)
		require.False(t, msg.State.IsSyncPoint)
		require.Equal(t, uint64(20), msg.State.BlockTs)
		require.Equal(t, heartbeatpb.InfluenceType_DB, msg.State.BlockTables.InfluenceType)
		require.Equal(t, int64(1), msg.State.BlockTables.SchemaID)
		require.Equal(t, heartbeatpb.BlockStage_WAITING, msg.State.Stage)
	case <-time.After(time.Second):
		require.FailNow(t, "expected deferred DB-level block status")
	}
}
