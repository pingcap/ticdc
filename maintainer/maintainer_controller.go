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

package maintainer

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils"
	"go.uber.org/zap"
)

// Controller schedules and balance tables
// there are 3 main components in the controller, scheduler, ReplicationDB and operator controller
type Controller struct {
	replicationDB *replica.ReplicationDB
	nodeManager   *watcher.NodeManager

	splitter               *split.Splitter
	enableTableAcrossNodes bool
	ddlDispatcherID        common.DispatcherID

	changefeedID common.ChangeFeedID
}

func NewController(changefeedID common.ChangeFeedID,
	ddlSpan *replica.SpanReplication, splitter *split.Splitter, enableTableAcrossNodes bool,
) *Controller {
	replicaSetDB := replica.NewReplicaSetDB(changefeedID, ddlSpan, enableTableAcrossNodes)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	return &Controller{
		changefeedID:           changefeedID,
		replicationDB:          replicaSetDB,
		ddlDispatcherID:        ddlSpan.ID,
		nodeManager:            nodeManager,
		splitter:               splitter,
		enableTableAcrossNodes: enableTableAcrossNodes,
	}
}

func (c *Controller) GetTasksBySchemaID(schemaID int64) []*replica.SpanReplication {
	return c.replicationDB.GetTasksBySchemaID(schemaID)
}

func (c *Controller) GetTaskSizeBySchemaID(schemaID int64) int {
	return c.replicationDB.GetTaskSizeBySchemaID(schemaID)
}

func (c *Controller) GetAllNodes() []node.ID {
	aliveNodes := c.nodeManager.GetAliveNodes()
	nodes := make([]node.ID, 0, len(aliveNodes))
	for id := range aliveNodes {
		nodes = append(nodes, id)
	}
	return nodes
}

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
	c.addNewSpans(table.SchemaID, tableSpans, startTs)
}

// GetTask queries a task by dispatcherID, return nil if not found
func (c *Controller) GetTask(dispatcherID common.DispatcherID) *replica.SpanReplication {
	return c.replicationDB.GetTaskByID(dispatcherID)
}

// GetTasksByTableID get all tasks by table id
func (c *Controller) GetTasksByTableID(tableID int64) []*replica.SpanReplication {
	return c.replicationDB.GetTasksByTableID(tableID)
}

// GetAllTasks get all tasks
func (c *Controller) GetAllTasks() []*replica.SpanReplication {
	return c.replicationDB.GetAllTasks()
}

// UpdateSchemaID will update the schema id of the table, and move the task to the new schema map
// it called when rename a table to another schema
func (c *Controller) UpdateSchemaID(tableID, newSchemaID int64) {
	c.replicationDB.UpdateSchemaID(tableID, newSchemaID)
}

func (c *Controller) TaskSize() int {
	return c.replicationDB.TaskSize()
}

func (c *Controller) GetTaskSizeByNodeID(id node.ID) int {
	return c.replicationDB.GetTaskSizeByNodeID(id)
}

func (c *Controller) addWorkingSpans(tableMap utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]) {
	tableMap.Ascend(func(span *heartbeatpb.TableSpan, stm *replica.SpanReplication) bool {
		c.replicationDB.AddReplicatingSpan(stm)
		return true
	})
}

func (c *Controller) addNewSpans(schemaID int64, tableSpans []*heartbeatpb.TableSpan, startTs uint64) {
	for _, span := range tableSpans {
		dispatcherID := common.NewDispatcherID()
		replicaSet := replica.NewSpanReplication(c.changefeedID, dispatcherID, schemaID, span, startTs)
		c.replicationDB.AddAbsentReplicaSet(replicaSet)
	}
}

func (c *Controller) checkParams(tableId int64, targetNode node.ID) error {
	if !c.replicationDB.IsTableExists(tableId) {
		// the table is not exist in this node
		return apperror.ErrTableIsNotFounded.GenWithStackByArgs("tableID", tableId)
	}

	if tableId == 0 {
		return apperror.ErrTableNotSupportMove.GenWithStackByArgs("tableID", tableId)
	}

	nodes := c.nodeManager.GetAliveNodes()
	hasNode := false
	for _, node := range nodes {
		if node.ID == targetNode {
			hasNode = true
			break
		}
	}
	if !hasNode {
		return apperror.ErrNodeIsNotFound.GenWithStackByArgs("targetNode", targetNode)
	}

	return nil
}

func (c *Controller) isDDLDispatcher(dispatcherID common.DispatcherID) bool {
	return dispatcherID == c.ddlDispatcherID
}

func getSchemaInfo(table commonEvent.Table, isMysqlCompatibleBackend bool) *heartbeatpb.SchemaInfo {
	schemaInfo := &heartbeatpb.SchemaInfo{}
	schemaInfo.SchemaID = table.SchemaID
	if !isMysqlCompatibleBackend {
		schemaInfo.SchemaName = table.SchemaName
	}
	return schemaInfo
}

func getTableInfo(table commonEvent.Table, isMysqlCompatibleBackend bool) *heartbeatpb.TableInfo {
	tableInfo := &heartbeatpb.TableInfo{}
	tableInfo.TableID = table.TableID
	if !isMysqlCompatibleBackend {
		tableInfo.TableName = table.TableName
	}
	return tableInfo
}
