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
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils"
	"go.uber.org/zap"
)

// Controller manages span lifecycle and replication state
type Controller struct {
	replicationDB          *replica.ReplicationDB
	changefeedID           common.ChangeFeedID
	nodeManager            *watcher.NodeManager
	splitter               *split.Splitter
	enableTableAcrossNodes bool
	ddlDispatcherID        common.DispatcherID
}

// NewController creates a new span controller
func NewController(
	changefeedID common.ChangeFeedID,
	ddlSpan *replica.SpanReplication,
	nodeManager *watcher.NodeManager,
	splitter *split.Splitter,
	enableTableAcrossNodes bool,
	ddlDispatcherID common.DispatcherID,
) *Controller {
	return &Controller{
		replicationDB:          replica.NewReplicaSetDB(changefeedID, ddlSpan, enableTableAcrossNodes),
		changefeedID:           changefeedID,
		nodeManager:            nodeManager,
		splitter:               splitter,
		enableTableAcrossNodes: enableTableAcrossNodes,
		ddlDispatcherID:        ddlDispatcherID,
	}
}

// AddNewTable adds a new table to the span controller
// This is a complex business logic method that handles table splitting and span creation
func (c *Controller) AddNewTable(table commonEvent.Table, startTs uint64) {
	if c.replicationDB.IsTableExists(table.TableID) {
		log.Warn("table already add, ignore",
			zap.String("changefeed", c.changefeedID.Name()),
			zap.Int64("schema", table.SchemaID),
			zap.Int64("table", table.TableID))
		return
	}
	span := common.TableIDToComparableSpan(table.TableID)
	tableSpan := &heartbeatpb.TableSpan{
		TableID:  table.TableID,
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}
	tableSpans := []*heartbeatpb.TableSpan{tableSpan}
	if c.enableTableAcrossNodes && len(c.nodeManager.GetAliveNodes()) > 1 {
		// split the whole table span base on region count if table region count is exceed the limit
		tableSpans = c.splitter.SplitSpansByRegion(context.Background(), tableSpan)
	}
	c.AddNewSpans(table.SchemaID, tableSpans, startTs)
}

// AddWorkingSpans adds working spans
func (c *Controller) AddWorkingSpans(tableMap utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]) {
	tableMap.Ascend(func(span *heartbeatpb.TableSpan, stm *replica.SpanReplication) bool {
		c.AddReplicatingSpan(stm)
		return true
	})
}

// AddNewSpans creates new spans for the given schema and table spans
// This is a complex business logic method that handles span creation
func (c *Controller) AddNewSpans(schemaID int64, tableSpans []*heartbeatpb.TableSpan, startTs uint64) {
	for _, span := range tableSpans {
		dispatcherID := common.NewDispatcherID()
		replicaSet := replica.NewSpanReplication(c.changefeedID, dispatcherID, schemaID, span, startTs)
		c.replicationDB.AddAbsentReplicaSet(replicaSet)
	}
}

// Simple query methods (direct delegation to replicationDB)

// GetTaskByID queries a task by dispatcherID, return nil if not found
func (c *Controller) GetTaskByID(dispatcherID common.DispatcherID) *replica.SpanReplication {
	return c.replicationDB.GetTaskByID(dispatcherID)
}

// GetTasksByTableID get all tasks by table id
func (c *Controller) GetTasksByTableID(tableID int64) []*replica.SpanReplication {
	return c.replicationDB.GetTasksByTableID(tableID)
}

// GetTasksBySchemaID get all tasks by schema id
func (c *Controller) GetTasksBySchemaID(schemaID int64) []*replica.SpanReplication {
	return c.replicationDB.GetTasksBySchemaID(schemaID)
}

// GetAllTasks get all tasks
func (c *Controller) GetAllTasks() []*replica.SpanReplication {
	return c.replicationDB.GetAllTasks()
}

// GetTaskSizeBySchemaID get task size by schema id
func (c *Controller) GetTaskSizeBySchemaID(schemaID int64) int {
	return c.replicationDB.GetTaskSizeBySchemaID(schemaID)
}

// TaskSize get total task size
func (c *Controller) TaskSize() int {
	return c.replicationDB.TaskSize()
}

// GetTaskSizeByNodeID get task size by node id
func (c *Controller) GetTaskSizeByNodeID(id node.ID) int {
	return c.replicationDB.GetTaskSizeByNodeID(id)
}

// IsTableExists check if table exists
func (c *Controller) IsTableExists(tableID int64) bool {
	return c.replicationDB.IsTableExists(tableID)
}

// State management methods (direct delegation to replicationDB)

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map
// it called when rename a table to another schema
func (c *Controller) UpdateSchemaID(tableID, newSchemaID int64) {
	c.replicationDB.UpdateSchemaID(tableID, newSchemaID)
}

// UpdateStatus updates the status of a span
func (c *Controller) UpdateStatus(span *replica.SpanReplication, status *heartbeatpb.TableSpanStatus) {
	c.replicationDB.UpdateStatus(span, status)
}

// AddAbsentReplicaSet adds absent replica sets
func (c *Controller) AddAbsentReplicaSet(spans ...*replica.SpanReplication) {
	c.replicationDB.AddAbsentReplicaSet(spans...)
}

// AddSchedulingReplicaSet adds scheduling replica sets
func (c *Controller) AddSchedulingReplicaSet(span *replica.SpanReplication, targetNodeID node.ID) {
	c.replicationDB.AddSchedulingReplicaSet(span, targetNodeID)
}

// AddReplicatingSpan adds replicating span
func (c *Controller) AddReplicatingSpan(span *replica.SpanReplication) {
	c.replicationDB.AddReplicatingSpan(span)
}

// MarkSpanAbsent marks span as absent
func (c *Controller) MarkSpanAbsent(span *replica.SpanReplication) {
	c.replicationDB.MarkSpanAbsent(span)
}

// MarkSpanScheduling marks span as scheduling
func (c *Controller) MarkSpanScheduling(span *replica.SpanReplication) {
	c.replicationDB.MarkSpanScheduling(span)
}

// MarkSpanReplicating marks span as replicating
func (c *Controller) MarkSpanReplicating(span *replica.SpanReplication) {
	c.replicationDB.MarkSpanReplicating(span)
}

// BindSpanToNode binds span to node
func (c *Controller) BindSpanToNode(old, new node.ID, span *replica.SpanReplication) {
	c.replicationDB.BindSpanToNode(old, new, span)
}

// RemoveReplicatingSpan removes replicating span
func (c *Controller) RemoveReplicatingSpan(span *replica.SpanReplication) {
	c.replicationDB.RemoveReplicatingSpan(span)
}

// ReplaceReplicaSet replaces replica sets
func (c *Controller) ReplaceReplicaSet(oldReplications []*replica.SpanReplication, newSpans []*heartbeatpb.TableSpan, checkpointTs uint64) {
	c.replicationDB.ReplaceReplicaSet(oldReplications, newSpans, checkpointTs)
}

// Statistics methods (direct delegation to replicationDB)

// GetAbsentSize get absent size
func (c *Controller) GetAbsentSize() int {
	return c.replicationDB.GetAbsentSize()
}

// GetSchedulingSize get scheduling size
func (c *Controller) GetSchedulingSize() int {
	return c.replicationDB.GetSchedulingSize()
}

// GetReplicatingSize get replicating size
func (c *Controller) GetReplicatingSize() int {
	return c.replicationDB.GetReplicatingSize()
}

// GetAbsent get absent spans
func (c *Controller) GetAbsent() []*replica.SpanReplication {
	return c.replicationDB.GetAbsent()
}

// GetScheduling get scheduling spans
func (c *Controller) GetScheduling() []*replica.SpanReplication {
	return c.replicationDB.GetScheduling()
}

// GetReplicating get replicating spans
func (c *Controller) GetReplicating() []*replica.SpanReplication {
	return c.replicationDB.GetReplicating()
}

// GetReplicationDB returns the internal replicationDB
// This is a temporary method for backward compatibility during refactoring
func (c *Controller) GetReplicationDB() *replica.ReplicationDB {
	return c.replicationDB
}

// IsDDLDispatcher checks if the dispatcher is a DDL dispatcher
func (c *Controller) IsDDLDispatcher(dispatcherID common.DispatcherID) bool {
	return dispatcherID == c.ddlDispatcherID
}

// GetDDLDispatcherID returns the DDL dispatcher ID
func (c *Controller) GetDDLDispatcherID() common.DispatcherID {
	return c.ddlDispatcherID
}

// GetDDLDispatcher returns the DDL dispatcher
func (c *Controller) GetDDLDispatcher() *replica.SpanReplication {
	return c.replicationDB.GetTaskByID(c.ddlDispatcherID)
}

// GetAllNodes returns all alive nodes
func (c *Controller) GetAllNodes() []node.ID {
	aliveNodes := c.nodeManager.GetAliveNodes()
	nodes := make([]node.ID, 0, len(aliveNodes))
	for id := range aliveNodes {
		nodes = append(nodes, id)
	}
	return nodes
}

// GetSplitter returns the splitter
func (c *Controller) GetSplitter() *split.Splitter {
	return c.splitter
}

// GetAbsentForTest returns absent spans for testing
func (c *Controller) GetAbsentForTest(existing []*replica.SpanReplication, limit int) []*replica.SpanReplication {
	return c.replicationDB.GetAbsentForTest(existing, limit)
}
