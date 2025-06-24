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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	pkgscheduler "github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

// Controller schedules and balance tables
// there are 3 main components in the controller, scheduler, ReplicationDB and operator controller
type Controller struct {
	bootstrapped bool

	schedulerController *pkgscheduler.Controller
	operatorController  *operator.Controller
	replicationDB       *replica.ReplicationDB
	messageCenter       messaging.MessageCenter
	nodeManager         *watcher.NodeManager

	splitter               *split.Splitter
	enableTableAcrossNodes bool
	startCheckpointTs      uint64
	ddlDispatcherID        common.DispatcherID

	cfConfig     *config.ReplicaConfig
	changefeedID common.ChangeFeedID

	taskPool threadpool.ThreadPool

	// Store the task handles, it's used to stop the task handlers when the controller is stopped.
	taskHandles []*threadpool.TaskHandle
}

func NewController(changefeedID common.ChangeFeedID,
	checkpointTs uint64,
	pdAPIClient pdutil.PDAPIClient,
	regionCache split.RegionCache,
	taskPool threadpool.ThreadPool,
	cfConfig *config.ReplicaConfig,
	ddlSpan *replica.SpanReplication,
	batchSize int, balanceInterval time.Duration,
) *Controller {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)

	enableTableAcrossNodes := false
	var splitter *split.Splitter
	if cfConfig != nil && cfConfig.Scheduler.EnableTableAcrossNodes {
		enableTableAcrossNodes = true
		splitter = split.NewSplitter(changefeedID, pdAPIClient, regionCache, cfConfig.Scheduler)
	}

	replicaSetDB := replica.NewReplicaSetDB(changefeedID, ddlSpan, enableTableAcrossNodes)
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	oc := operator.NewOperatorController(changefeedID, mc, replicaSetDB, nodeManager, batchSize)

	var schedulerCfg *config.ChangefeedSchedulerConfig
	if cfConfig != nil {
		schedulerCfg = cfConfig.Scheduler
	}
	sc := NewScheduleController(
		changefeedID, batchSize, oc, replicaSetDB, nodeManager, balanceInterval, splitter, schedulerCfg,
	)

	return &Controller{
		startCheckpointTs:      checkpointTs,
		changefeedID:           changefeedID,
		bootstrapped:           false,
		ddlDispatcherID:        ddlSpan.ID,
		schedulerController:    sc,
		operatorController:     oc,
		messageCenter:          mc,
		replicationDB:          replicaSetDB,
		nodeManager:            nodeManager,
		taskPool:               taskPool,
		cfConfig:               cfConfig,
		splitter:               splitter,
		enableTableAcrossNodes: enableTableAcrossNodes,
	}
}

// HandleStatus handle the status report from the node
func (c *Controller) HandleStatus(from node.ID, statusList []*heartbeatpb.TableSpanStatus) {
	for _, status := range statusList {
		dispatcherID := common.NewDispatcherIDFromPB(status.ID)
		c.operatorController.UpdateOperatorStatus(dispatcherID, from, status)
		stm := c.GetTask(dispatcherID)
		if stm == nil {
			if status.ComponentStatus != heartbeatpb.ComponentState_Working {
				continue
			}
			if op := c.operatorController.GetOperator(dispatcherID); op == nil {
				// it's normal case when the span is not found in replication db
				// the span is removed from replication db first, so here we only check if the span status is working or not
				log.Warn("no span found, remove it",
					zap.String("changefeed", c.changefeedID.Name()),
					zap.String("from", from.String()),
					zap.Any("status", status),
					zap.String("dispatcherID", dispatcherID.String()))
				// if the span is not found, and the status is working, we need to remove it from dispatcher
				_ = c.messageCenter.SendCommand(replica.NewRemoveDispatcherMessage(from, c.changefeedID, status.ID))
			}
			continue
		}
		nodeID := stm.GetNodeID()
		if nodeID != from {
			// todo: handle the case that the node id is mismatch
			log.Warn("node id not match",
				zap.String("changefeed", c.changefeedID.Name()),
				zap.Any("from", from),
				zap.Stringer("node", nodeID))
			continue
		}
		c.replicationDB.UpdateStatus(stm, status)
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

// RemoveAllTasks remove all tasks
func (c *Controller) RemoveAllTasks() {
	c.operatorController.RemoveAllTasks()
}

// RemoveTasksBySchemaID remove all tasks by schema id
func (c *Controller) RemoveTasksBySchemaID(schemaID int64) {
	c.operatorController.RemoveTasksBySchemaID(schemaID)
}

// RemoveTasksByTableIDs remove all tasks by table id
func (c *Controller) RemoveTasksByTableIDs(tables ...int64) {
	c.operatorController.RemoveTasksByTableIDs(tables...)
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

// RemoveNode is called when a node is removed
func (c *Controller) RemoveNode(id node.ID) {
	c.operatorController.OnNodeRemoved(id)
}

// ScheduleFinished return false if not all task are running in working state
func (c *Controller) ScheduleFinished() bool {
	return c.operatorController.OperatorSizeWithLock() == 0 && c.replicationDB.GetAbsentSize() == 0
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

func (c *Controller) Stop() {
	for _, handler := range c.taskHandles {
		handler.Cancel()
	}
}
