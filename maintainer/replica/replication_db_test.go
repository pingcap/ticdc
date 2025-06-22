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

package replica

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

// TODO: refactor all test in maintainer with getTableSpanByID
func getTableSpanByID(id common.TableID) *heartbeatpb.TableSpan {
	totalSpan := common.TableIDToComparableSpan(id)
	return &heartbeatpb.TableSpan{
		TableID:  totalSpan.TableID,
		StartKey: totalSpan.StartKey,
		EndKey:   totalSpan.EndKey,
	}
}

func TestBasicFunction(t *testing.T) {
	t.Parallel()

	db := newDBWithCheckerForTest(t)
	absent := NewSpanReplication(db.changefeedID, common.NewDispatcherID(), 1, getTableSpanByID(4), 1)
	db.AddAbsentReplicaSet(absent)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := NewWorkingSpanReplication(db.changefeedID, replicaSpanID,
		1,
		getTableSpanByID(3), &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	db.AddReplicatingSpan(replicaSpan)
	require.Equal(t, 3, db.TaskSize())
	require.Len(t, db.GetAllTasks(), 3)
	require.True(t, db.IsTableExists(3))
	require.False(t, db.IsTableExists(5))
	require.True(t, db.IsTableExists(4))
	require.Len(t, db.GetTasksBySchemaID(1), 2)
	require.Len(t, db.GetTasksBySchemaID(2), 0)
	require.Equal(t, 2, db.GetTaskSizeBySchemaID(1))
	require.Equal(t, 0, db.GetTaskSizeBySchemaID(2))
	require.Len(t, db.GetTasksByTableID(3), 1)
	require.Len(t, db.GetTasksByTableID(4), 1)
	require.Len(t, db.GetTaskByNodeID("node1"), 1)
	require.Len(t, db.GetTaskByNodeID("node2"), 0)
	require.Equal(t, 0, db.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 1, db.GetTaskSizeByNodeID("node1"))

	require.Len(t, db.GetReplicating(), 1)
	require.NotNil(t, db.GetTaskByID(replicaSpan.ID))
	require.NotNil(t, db.GetTaskByID(absent.ID))
	require.Nil(t, db.GetTaskByID(common.NewDispatcherID()))
	require.Equal(t, 0, db.GetSchedulingSize())
	require.Equal(t, 1, db.GetTaskSizePerNode()["node1"])

	db.MarkSpanScheduling(absent)
	require.Equal(t, 1, db.GetSchedulingSize())
	db.BindSpanToNode("", "node2", absent)
	require.Len(t, db.GetTaskByNodeID("node2"), 1)
	db.MarkSpanReplicating(absent)
	require.Len(t, db.GetReplicating(), 2)
	require.Equal(t, "node2", absent.GetNodeID().String())

	db.UpdateSchemaID(3, 2)
	require.Len(t, db.GetTasksBySchemaID(1), 1)
	require.Len(t, db.GetTasksBySchemaID(2), 1)

	require.Len(t, db.RemoveByTableIDs(3), 1)
	require.Len(t, db.GetTasksBySchemaID(1), 1)
	require.Len(t, db.GetTasksBySchemaID(2), 0)
	require.Len(t, db.GetReplicating(), 1)
	require.Equal(t, 1, db.GetReplicatingSize())
	require.Equal(t, 2, db.TaskSize())

	db.UpdateSchemaID(4, 5)
	require.Equal(t, 1, db.GetTaskSizeBySchemaID(5))
	require.Len(t, db.RemoveBySchemaID(5), 1)

	require.Len(t, db.GetReplicating(), 0)
	require.Equal(t, 1, db.TaskSize())
	require.Equal(t, db.GetAbsentSize(), 0)
	require.Equal(t, db.GetSchedulingSize(), 0)
	require.Equal(t, db.GetReplicatingSize(), 0)
	// ddl table id
	require.Len(t, db.tableTasks[0], 1)
	require.Len(t, db.schemaTasks[common.DDLSpanSchemaID], 1)
	require.Len(t, db.GetTaskSizePerNode(), 0)
}

func TestReplaceReplicaSet(t *testing.T) {
	t.Parallel()

	db := newDBWithCheckerForTest(t)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := NewWorkingSpanReplication(db.changefeedID, replicaSpanID,
		1,
		getTableSpanByID(3), &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	db.AddReplicatingSpan(replicaSpan)

	notExists := &SpanReplication{ID: common.NewDispatcherID()}
	require.PanicsWithValue(t, "old replica set not found", func() {
		db.ReplaceReplicaSet([]*SpanReplication{notExists}, []*heartbeatpb.TableSpan{{}, {}}, 1)
	})
	require.Len(t, db.GetAllTasks(), 2)

	db.ReplaceReplicaSet([]*SpanReplication{replicaSpan}, []*heartbeatpb.TableSpan{getTableSpanByID(3), getTableSpanByID(4)}, 5)
	require.Len(t, db.GetAllTasks(), 3)
	require.Equal(t, 2, db.GetAbsentSize())
	require.Equal(t, 2, db.GetTaskSizeBySchemaID(1))
}

func TestMarkSpanAbsent(t *testing.T) {
	t.Parallel()

	db := newDBWithCheckerForTest(t)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := NewWorkingSpanReplication(db.changefeedID, replicaSpanID,
		1,
		getTableSpanByID(3), &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	db.AddReplicatingSpan(replicaSpan)
	db.MarkSpanAbsent(replicaSpan)
	require.Equal(t, 1, db.GetAbsentSize())
	require.Equal(t, "", replicaSpan.GetNodeID().String())
}

func TestForceRemove(t *testing.T) {
	t.Parallel()

	db := newDBWithCheckerForTest(t)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := NewWorkingSpanReplication(db.changefeedID, replicaSpanID,
		1,
		getTableSpanByID(3), &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	db.AddReplicatingSpan(replicaSpan)
	db.ForceRemove(common.NewDispatcherID())
	require.Len(t, db.GetAllTasks(), 2)
	db.ForceRemove(replicaSpan.ID)
	require.Len(t, db.GetAllTasks(), 1)
}

func TestGetAbsents(t *testing.T) {
	t.Parallel()

	db := newDBWithCheckerForTest(t)
	for i := 0; i < 10; i++ {
		absent := NewSpanReplication(db.changefeedID, common.NewDispatcherID(), 1, getTableSpanByID(int64(i+1)), 1)
		db.AddAbsentReplicaSet(absent)
	}
	require.Len(t, db.GetAbsentForTest(nil, 5), 5)
	require.Len(t, db.GetAbsentForTest(nil, 15), 10)
}

func TestRemoveAllTables(t *testing.T) {
	t.Parallel()

	db := newDBWithCheckerForTest(t)
	// ddl span will not be removed
	removed := db.RemoveAll()
	require.Len(t, removed, 0)
	require.Len(t, db.GetAllTasks(), 1)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := NewWorkingSpanReplication(db.changefeedID, replicaSpanID,
		1,
		getTableSpanByID(3), &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	db.AddReplicatingSpan(replicaSpan)

	absent := NewSpanReplication(db.changefeedID, common.NewDispatcherID(), 1, getTableSpanByID(4), 1)
	db.AddAbsentReplicaSet(absent)

	scheduling := NewSpanReplication(db.changefeedID, common.NewDispatcherID(), 1, getTableSpanByID(4), 1)
	db.AddAbsentReplicaSet(scheduling)
	db.MarkSpanScheduling(scheduling)

	require.Len(t, db.GetAllTasks(), 4)
	require.Len(t, db.GetReplicating(), 1)
	require.Len(t, db.GetAbsent(), 1)
	require.Len(t, db.GetScheduling(), 1)

	removed = db.RemoveAll()
	require.Len(t, removed, 2)
	require.Len(t, db.GetAllTasks(), 1)
}

func newDBWithCheckerForTest(t *testing.T) *ReplicationDB {
	cfID := common.NewChangeFeedIDWithName("test")
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	ddlSpan := NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	return NewReplicaSetDB(cfID, ddlSpan, true)
}

func TestReplicationDB_BasicOperations(t *testing.T) {
	// Create test data
	changefeedID := common.NewChangefeedID()
	ddlSpan := NewSpanReplication(changefeedID, common.NewDispatcherID(), common.DDLSpanSchemaID, common.DDLSpan, 1000)
	mockNodeManager := &watcher.NodeManager{}

	// Create unified ReplicationDB
	db := NewReplicaSetDBWithNodeManager(changefeedID, ddlSpan, false, mockNodeManager)

	// Test basic operations
	require.NotNil(t, db)
	require.Equal(t, 1, db.TaskSize()) // Contains DDL span
	require.Equal(t, 0, db.GetAbsentSize())
	require.Equal(t, 0, db.GetReplicatingSize()) // DDL span initial state is not replicating
	require.Equal(t, 0, db.GetSchedulingSize())

	// Test DDL dispatcher
	ddlDispatcher := db.GetDDLDispatcher()
	require.NotNil(t, ddlDispatcher)
	require.True(t, ddlDispatcher.Span.Equal(common.DDLSpan))
}

func TestReplicationDB_AddAndRemoveTasks(t *testing.T) {
	// Create test data
	changefeedID := common.NewChangefeedID()
	ddlSpan := NewSpanReplication(changefeedID, common.NewDispatcherID(), common.DDLSpanSchemaID, common.DDLSpan, 1000)
	mockNodeManager := &watcher.NodeManager{}

	// Create unified ReplicationDB
	db := NewReplicaSetDBWithNodeManager(changefeedID, ddlSpan, false, mockNodeManager)

	// Test adding and removing tasks - use valid span range
	tableSpan := &heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{0x01, 0x02, 0x03}, // Valid StartKey
		EndKey:   []byte{0x04, 0x05, 0x06}, // Valid EndKey, ensure StartKey < EndKey
	}

	db.AddNewSpans(1, []*heartbeatpb.TableSpan{tableSpan}, 1000)
	require.Equal(t, 1, db.GetAbsentSize())

	// Test removing tasks
	removed := db.RemoveByTableIDs(1)
	require.Len(t, removed, 0) // Newly added span hasn't been scheduled yet, so removal returns empty
	require.Equal(t, 0, db.GetAbsentSize())
}

func TestReplicationDB_StatusManagement(t *testing.T) {
	// Create test data
	changefeedID := common.NewChangefeedID()
	ddlSpan := NewSpanReplication(changefeedID, common.NewDispatcherID(), common.DDLSpanSchemaID, common.DDLSpan, 1000)
	mockNodeManager := &watcher.NodeManager{}

	// Create unified ReplicationDB
	db := NewReplicaSetDBWithNodeManager(changefeedID, ddlSpan, false, mockNodeManager)

	// Create test span - use valid span range
	tableSpan := &heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{0x01, 0x02, 0x03}, // Valid StartKey
		EndKey:   []byte{0x04, 0x05, 0x06}, // Valid EndKey, ensure StartKey < EndKey
	}
	span := NewSpanReplication(changefeedID, common.NewDispatcherID(), 1, tableSpan, 1000)

	// Test status conversion
	db.AddAbsentReplicaSet(span)
	require.Equal(t, 1, db.GetAbsentSize())
	require.Equal(t, 0, db.GetSchedulingSize())
	require.Equal(t, 0, db.GetReplicatingSize())

	// Test binding to node
	db.BindSpanToNode("", "node1", span)
	require.Equal(t, "node1", span.GetNodeID().String())

	// Test status conversion
	db.MarkSpanScheduling(span)
	require.Equal(t, 0, db.GetAbsentSize())
	require.Equal(t, 1, db.GetSchedulingSize())
	require.Equal(t, 0, db.GetReplicatingSize())

	db.MarkSpanReplicating(span)
	require.Equal(t, 0, db.GetAbsentSize())
	require.Equal(t, 0, db.GetSchedulingSize())
	require.Equal(t, 1, db.GetReplicatingSize())

	// Test status update
	status := &heartbeatpb.TableSpanStatus{
		ID:              span.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    2000,
	}
	db.UpdateStatus(span, status)
	require.Equal(t, uint64(2000), span.GetStatus().GetCheckpointTs())
}

func TestReplicationDB_QueryOperations(t *testing.T) {
	// Create test data
	changefeedID := common.NewChangefeedID()
	ddlSpan := NewSpanReplication(changefeedID, common.NewDispatcherID(), common.DDLSpanSchemaID, common.DDLSpan, 1000)
	mockNodeManager := &watcher.NodeManager{}

	// Create unified ReplicationDB
	db := NewReplicaSetDBWithNodeManager(changefeedID, ddlSpan, false, mockNodeManager)

	// Add test data - use valid span range
	tableSpan1 := &heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{0x01, 0x02, 0x03}, // Valid StartKey
		EndKey:   []byte{0x04, 0x05, 0x06}, // Valid EndKey
	}
	tableSpan2 := &heartbeatpb.TableSpan{
		TableID:  2,
		StartKey: []byte{0x07, 0x08, 0x09}, // Valid StartKey
		EndKey:   []byte{0x0a, 0x0b, 0x0c}, // Valid EndKey
	}

	db.AddNewSpans(1, []*heartbeatpb.TableSpan{tableSpan1, tableSpan2}, 1000)

	// Test query operations
	require.Equal(t, 2, db.GetAbsentSize())
	require.Equal(t, 2, db.GetTaskSizeBySchemaID(1))
	tasksByTable1 := db.GetTasksByTableID(1)
	require.Len(t, tasksByTable1, 1)
	tasksByTable2 := db.GetTasksByTableID(2)
	require.Len(t, tasksByTable2, 1)

	tasksBySchema := db.GetTasksBySchemaID(1)
	require.Len(t, tasksBySchema, 2)

	tasksByTable := db.GetTasksByTableID(1)
	require.Len(t, tasksByTable, 1)

	allTasks := db.GetAllTasks()
	require.Len(t, allTasks, 3) // 2 table spans + 1 DDL span

	// Test table existence check
	require.True(t, db.IsTableExists(1))
	require.True(t, db.IsTableExists(2))
	require.False(t, db.IsTableExists(999))
}

func TestReplicationDB_NodeManagement(t *testing.T) {
	// Create test data
	changefeedID := common.NewChangefeedID()
	ddlSpan := NewSpanReplication(changefeedID, common.NewDispatcherID(), common.DDLSpanSchemaID, common.DDLSpan, 1000)

	// Create unified ReplicationDB, not using NodeManager
	db := NewReplicaSetDB(changefeedID, ddlSpan, false)

	// Test task binding to node
	tableSpan := &heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{0x01, 0x02, 0x03}, // Valid StartKey
		EndKey:   []byte{0x04, 0x05, 0x06}, // Valid EndKey
	}
	span := NewSpanReplication(changefeedID, common.NewDispatcherID(), 1, tableSpan, 1000)

	db.AddAbsentReplicaSet(span)
	db.BindSpanToNode("", "node1", span)
	db.MarkSpanReplicating(span)

	require.Equal(t, 1, db.GetTaskSizeByNodeID("node1"))
	require.Equal(t, 0, db.GetTaskSizeByNodeID("node2"))

	taskSizePerNode := db.GetTaskSizePerNode()
	require.Equal(t, 1, taskSizePerNode["node1"])
	require.Equal(t, 0, taskSizePerNode["node2"])
}

func TestReplicationDB_CallbackFunctions(t *testing.T) {
	// Create test data
	changefeedID := common.NewChangefeedID()
	ddlSpan := NewSpanReplication(changefeedID, common.NewDispatcherID(), common.DDLSpanSchemaID, common.DDLSpan, 1000)
	mockNodeManager := &watcher.NodeManager{}

	// Create unified ReplicationDB
	db := NewReplicaSetDBWithNodeManager(changefeedID, ddlSpan, false, mockNodeManager)

	// Test callback function setting
	db.SetOperatorStatusUpdater(func(dispatcherID common.DispatcherID, from node.ID, status *heartbeatpb.TableSpanStatus) {
		// Callback function processing logic when called
	})

	db.SetMessageSender(func(msg *messaging.TargetMessage) error {
		// Message sending callback
		return nil
	})

	// Test HandleStatus
	status := &heartbeatpb.TableSpanStatus{
		ID:              ddlSpan.ID.ToPB(),
		ComponentStatus: heartbeatpb.ComponentState_Working,
		CheckpointTs:    1000,
	}
	db.HandleStatus("node1", []*heartbeatpb.TableSpanStatus{status})

	// Note: Due to special processing of DDL span, callback may not be triggered
	// Here mainly tests method call does not fail
	require.NotNil(t, db)
}

// Mock component for testing
type mockNodeManager struct {
	aliveNodes map[node.ID]*node.Info
}

func (m *mockNodeManager) GetAliveNodes() map[node.ID]*node.Info {
	return m.aliveNodes
}
