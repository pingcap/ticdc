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

package span

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	testutil "github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestNewController(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(cfID, ddlDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	appcontext.SetService(watcher.NodeManagerName, watcher.NewNodeManager(nil, nil))
	controller := NewController(cfID, ddlSpan, nil, nil, common.DefaultMode)
	require.NotNil(t, controller)
	require.Equal(t, cfID, controller.changefeedID)
	require.False(t, controller.enableTableAcrossNodes)
}

func TestController_AddNewTable(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(changefeedID, ddlDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")

	controller := NewController(
		changefeedID,
		ddlSpan,
		nil, // splitter
		nil,
		common.DefaultMode,
	)

	table := commonEvent.Table{
		SchemaID: 1,
		TableID:  100,
	}

	// Test adding a new table
	controller.AddNewTable(table, 1000)

	// Verify the table was added
	require.True(t, controller.IsTableExists(table.TableID))
	require.Equal(t, 2, controller.TaskSize()) // DDL span + new table

	// Test adding the same table again (should be ignored)
	controller.AddNewTable(table, 2000)
	require.Equal(t, 2, controller.TaskSize()) // Should still be 2
}

func TestController_GetTaskByID(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(changefeedID, ddlDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")

	controller := NewController(
		changefeedID,
		ddlSpan,
		nil, // splitter
		nil,
		common.DefaultMode,
	)

	// Add a table first
	table := commonEvent.Table{
		SchemaID: 1,
		TableID:  100,
	}
	controller.AddNewTable(table, 1000)

	// Get all tasks to find the dispatcher ID
	tasks := controller.GetAllTasks()
	require.Len(t, tasks, 2) // DDL span + new table

	// Find the non-DDL task
	var tableTask *replica.SpanReplication
	for _, task := range tasks {
		if task.Span.TableID != 0 { // DDL span has TableID 0
			tableTask = task
			break
		}
	}
	require.NotNil(t, tableTask)

	dispatcherID := tableTask.ID

	// Test getting task by ID
	task := controller.GetTaskByID(dispatcherID)
	require.NotNil(t, task)
	require.Equal(t, dispatcherID, task.ID)

	// Test getting non-existent task
	nonExistentID := common.NewDispatcherID()
	task = controller.GetTaskByID(nonExistentID)
	require.Nil(t, task)
}

func TestController_GetTasksByTableID(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(changefeedID, ddlDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")

	controller := NewController(
		changefeedID,
		ddlSpan,
		nil, // splitter
		nil,
		common.DefaultMode,
	)

	// Add a table
	table := commonEvent.Table{
		SchemaID: 1,
		TableID:  100,
	}
	controller.AddNewTable(table, 1000)

	// Test getting tasks by table ID
	tasks := controller.GetTasksByTableID(table.TableID)
	require.Len(t, tasks, 1)
	require.Equal(t, table.TableID, tasks[0].Span.TableID)

	// Test getting tasks for non-existent table
	tasks = controller.GetTasksByTableID(999)
	require.Len(t, tasks, 0)
}

func TestController_GetTasksBySchemaID(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(changefeedID, ddlDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")

	controller := NewController(
		changefeedID,
		ddlSpan,
		nil, // splitter
		nil,
		common.DefaultMode,
	)

	// Add tables from the same schema
	table1 := commonEvent.Table{
		SchemaID: 1,
		TableID:  100,
	}
	table2 := commonEvent.Table{
		SchemaID: 1,
		TableID:  101,
	}
	controller.AddNewTable(table1, 1000)
	controller.AddNewTable(table2, 1000)

	// Test getting tasks by schema ID
	tasks := controller.GetTasksBySchemaID(1)
	require.Len(t, tasks, 2)

	// Test getting tasks for non-existent schema
	tasks = controller.GetTasksBySchemaID(999)
	require.Len(t, tasks, 0)
}

func TestController_UpdateSchemaID(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(changefeedID, ddlDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")

	controller := NewController(
		changefeedID,
		ddlSpan,
		nil, // splitter
		nil,
		common.DefaultMode,
	)

	// Add a table
	table := commonEvent.Table{
		SchemaID: 1,
		TableID:  100,
	}
	controller.AddNewTable(table, 1000)

	// Verify initial state
	tasks := controller.GetTasksBySchemaID(1)
	require.Len(t, tasks, 1)

	// Update schema ID
	controller.UpdateSchemaID(table.TableID, 2)

	// Verify the task moved to new schema
	tasks = controller.GetTasksBySchemaID(1)
	require.Len(t, tasks, 0)

	tasks = controller.GetTasksBySchemaID(2)
	require.Len(t, tasks, 1)
}

func TestController_Statistics(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	ddlDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(changefeedID, ddlDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")

	controller := NewController(
		changefeedID,
		ddlSpan,
		nil, // splitter
		nil,
		common.DefaultMode,
	)

	// Add some tables
	table1 := commonEvent.Table{SchemaID: 1, TableID: 100}
	table2 := commonEvent.Table{SchemaID: 1, TableID: 101}
	table3 := commonEvent.Table{SchemaID: 2, TableID: 200}

	controller.AddNewTable(table1, 1000)
	controller.AddNewTable(table2, 1000)
	controller.AddNewTable(table3, 1000)

	// Test statistics
	require.Equal(t, 4, controller.TaskSize()) // DDL span + 3 tables
	require.Equal(t, 2, controller.GetTaskSizeBySchemaID(1))
	require.Equal(t, 1, controller.GetTaskSizeBySchemaID(2))
	require.Equal(t, 0, controller.GetTaskSizeBySchemaID(3))
}

// TestBasicFunction tests the basic functionality of the controller
func TestBasicFunction(t *testing.T) {
	t.Parallel()

	controller := newControllerWithCheckerForTest(t)
	absent := replica.NewSpanReplication(controller.changefeedID, common.NewDispatcherID(), 1, testutil.GetTableSpanByID(4), 1, common.DefaultMode)
	controller.AddAbsentReplicaSet(absent)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := replica.NewWorkingSpanReplication(controller.changefeedID, replicaSpanID,
		1,
		testutil.GetTableSpanByID(3), &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	controller.AddReplicatingSpan(replicaSpan)
	require.Equal(t, 3, controller.TaskSize())
	require.Len(t, controller.GetAllTasks(), 3)
	require.True(t, controller.IsTableExists(3))
	require.False(t, controller.IsTableExists(5))
	require.True(t, controller.IsTableExists(4))
	require.Len(t, controller.GetTasksBySchemaID(1), 2)
	require.Len(t, controller.GetTasksBySchemaID(2), 0)
	require.Equal(t, 2, controller.GetTaskSizeBySchemaID(1))
	require.Equal(t, 0, controller.GetTaskSizeBySchemaID(2))
	require.Len(t, controller.GetTasksByTableID(3), 1)
	require.Len(t, controller.GetTasksByTableID(4), 1)
	require.Len(t, controller.GetTaskByNodeID("node1"), 1)
	require.Len(t, controller.GetTaskByNodeID("node2"), 0)
	require.Equal(t, 0, controller.GetTaskSizeByNodeID("node2"))
	require.Equal(t, 1, controller.GetTaskSizeByNodeID("node1"))

	require.Len(t, controller.GetReplicating(), 1)
	require.NotNil(t, controller.GetTaskByID(replicaSpan.ID))
	require.NotNil(t, controller.GetTaskByID(absent.ID))
	require.Nil(t, controller.GetTaskByID(common.NewDispatcherID()))
	require.Equal(t, 0, controller.GetSchedulingSize())
	require.Equal(t, 1, controller.GetTaskSizePerNode()["node1"])

	controller.MarkSpanScheduling(absent)
	require.Equal(t, 1, controller.GetSchedulingSize())
	controller.BindSpanToNode("", "node2", absent)
	require.Len(t, controller.GetTaskByNodeID("node2"), 1)
	controller.MarkSpanReplicating(absent)
	require.Len(t, controller.GetReplicating(), 2)
	require.Equal(t, "node2", absent.GetNodeID().String())

	controller.UpdateSchemaID(3, 2)
	require.Len(t, controller.GetTasksBySchemaID(1), 1)
	require.Len(t, controller.GetTasksBySchemaID(2), 1)

	require.Len(t, controller.RemoveByTableIDs(3), 1)
	require.Len(t, controller.GetTasksBySchemaID(1), 1)
	require.Len(t, controller.GetTasksBySchemaID(2), 0)
	require.Len(t, controller.GetReplicating(), 1)
	require.Equal(t, 1, controller.GetReplicatingSize())
	require.Equal(t, 2, controller.TaskSize())

	controller.UpdateSchemaID(4, 5)
	require.Equal(t, 1, controller.GetTaskSizeBySchemaID(5))
	require.Len(t, controller.RemoveBySchemaID(5), 1)

	require.Len(t, controller.GetReplicating(), 0)
	require.Equal(t, 1, controller.TaskSize())
	require.Equal(t, controller.GetAbsentSize(), 0)
	require.Equal(t, controller.GetSchedulingSize(), 0)
	require.Equal(t, controller.GetReplicatingSize(), 0)
	// ddl table id
	require.Len(t, controller.tableTasks[0], 1)
	require.Len(t, controller.schemaTasks[common.DDLSpanSchemaID], 1)
	require.Len(t, controller.GetTaskSizePerNode(), 0)
}

// TestReplaceReplicaSet tests the ReplaceReplicaSet functionality
func TestReplaceReplicaSet(t *testing.T) {
	t.Parallel()

	controller := newControllerWithCheckerForTest(t)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := replica.NewWorkingSpanReplication(controller.changefeedID, replicaSpanID,
		1,
		testutil.GetTableSpanByID(3), &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	controller.AddReplicatingSpan(replicaSpan)

	notExists := &replica.SpanReplication{ID: common.NewDispatcherID()}
	require.PanicsWithValue(t, "old replica set not found", func() {
		controller.ReplaceReplicaSet([]*replica.SpanReplication{notExists}, []*heartbeatpb.TableSpan{{}, {}}, 1, []node.ID{})
	})
	require.Len(t, controller.GetAllTasks(), 2)

	controller.ReplaceReplicaSet([]*replica.SpanReplication{replicaSpan}, []*heartbeatpb.TableSpan{testutil.GetTableSpanByID(3), testutil.GetTableSpanByID(4)}, 5, []node.ID{})
	require.Len(t, controller.GetAllTasks(), 3)
	require.Equal(t, 2, controller.GetAbsentSize())
	require.Equal(t, 2, controller.GetTaskSizeBySchemaID(1))
}

// TestMarkSpanAbsent tests the MarkSpanAbsent functionality
func TestMarkSpanAbsent(t *testing.T) {
	t.Parallel()

	controller := newControllerWithCheckerForTest(t)
	// replicating and scheduling will be returned
	replicaSpanID := common.NewDispatcherID()
	replicaSpan := replica.NewWorkingSpanReplication(controller.changefeedID, replicaSpanID,
		1,
		testutil.GetTableSpanByID(3), &heartbeatpb.TableSpanStatus{
			ID:              replicaSpanID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	controller.AddReplicatingSpan(replicaSpan)
	controller.MarkSpanAbsent(replicaSpan)
	require.Equal(t, 1, controller.GetAbsentSize())
	require.Equal(t, "", replicaSpan.GetNodeID().String())
}

func newControllerWithCheckerForTest(t *testing.T) *Controller {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	tableTriggerEventDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(cfID, tableTriggerEventDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              tableTriggerEventDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	return NewController(cfID, ddlSpan, nil, &config.ChangefeedSchedulerConfig{EnableTableAcrossNodes: true}, common.DefaultMode)
}
