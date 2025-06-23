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
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
)

func TestNewController(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test")
	ddlDispatcherID := common.NewDispatcherID()
	ddlSpan := replica.NewWorkingSpanReplication(cfID, ddlDispatcherID,
		common.DDLSpanSchemaID,
		common.DDLSpan, &heartbeatpb.TableSpanStatus{
			ID:              ddlDispatcherID.ToPB(),
			ComponentStatus: heartbeatpb.ComponentState_Working,
			CheckpointTs:    1,
		}, "node1")
	appcontext.SetService(watcher.NodeManagerName, watcher.NewNodeManager(nil, nil))
	controller := NewController(cfID, ddlSpan, nil, false)
	require.NotNil(t, controller)
	require.Equal(t, cfID, controller.changefeedID)
	require.False(t, controller.enableTableAcrossNodes)
}

func TestController_AddNewTable(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName("test")
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
		nil,   // splitter
		false, // enableTableAcrossNodes
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
	changefeedID := common.NewChangeFeedIDWithName("test")
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
		nil,   // splitter
		false, // enableTableAcrossNodes
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
	changefeedID := common.NewChangeFeedIDWithName("test")
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
		nil,   // splitter
		false, // enableTableAcrossNodes
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
	changefeedID := common.NewChangeFeedIDWithName("test")
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
		nil,   // splitter
		false, // enableTableAcrossNodes
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
	changefeedID := common.NewChangeFeedIDWithName("test")
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
		nil,   // splitter
		false, // enableTableAcrossNodes
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
	changefeedID := common.NewChangeFeedIDWithName("test")
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
		nil,   // splitter
		false, // enableTableAcrossNodes
	)

	// Initially should have only DDL span
	require.Equal(t, 1, controller.TaskSize()) // DDL span
	require.Equal(t, 0, controller.GetAbsentSize())
	require.Equal(t, 0, controller.GetSchedulingSize())
	require.Equal(t, 0, controller.GetReplicatingSize()) // DDL span is replicating

	// Add a table
	table := commonEvent.Table{
		SchemaID: 1,
		TableID:  100,
	}
	controller.AddNewTable(table, 1000)

	// Should have one absent task plus DDL span
	require.Equal(t, 2, controller.TaskSize())
	require.Equal(t, 1, controller.GetAbsentSize())
	require.Equal(t, 0, controller.GetSchedulingSize())
	require.Equal(t, 0, controller.GetReplicatingSize()) // Only DDL span is replicating
}
