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
	spanManager         *replica.ReplicationDB
	messageCenter       messaging.MessageCenter

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

	// 创建统一的 SpanManager
	spanManager := replica.NewReplicaSetDBWithNodeManager(changefeedID, ddlSpan, enableTableAcrossNodes, nodeManager)

	oc := operator.NewOperatorController(changefeedID, mc, replicaSetDB, nodeManager, batchSize)

	// 设置 SpanManager 的回调函数
	spanManager.SetOperatorStatusUpdater(func(dispatcherID common.DispatcherID, from node.ID, status *heartbeatpb.TableSpanStatus) {
		oc.UpdateOperatorStatus(dispatcherID, from, status)
	})
	spanManager.SetMessageSender(func(msg *messaging.TargetMessage) error {
		return mc.SendCommand(msg)
	})

	var schedulerCfg *config.ChangefeedSchedulerConfig
	if cfConfig != nil {
		schedulerCfg = cfConfig.Scheduler
	}
	sc := NewScheduleController(
		changefeedID, batchSize, oc, spanManager, nodeManager, balanceInterval, splitter, schedulerCfg,
	)

	return &Controller{
		startCheckpointTs:      checkpointTs,
		changefeedID:           changefeedID,
		bootstrapped:           false,
		ddlDispatcherID:        ddlSpan.ID,
		schedulerController:    sc,
		operatorController:     oc,
		spanManager:            spanManager,
		messageCenter:          mc,
		taskPool:               taskPool,
		cfConfig:               cfConfig,
		splitter:               splitter,
		enableTableAcrossNodes: enableTableAcrossNodes,
	}
}

// HandleStatus handle the status report from the node
func (c *Controller) HandleStatus(from node.ID, statusList []*heartbeatpb.TableSpanStatus) {
	c.spanManager.HandleStatus(from, statusList)
}

// GetTasksBySchemaID 根据 schemaID 获取任务列表
func (c *Controller) GetTasksBySchemaID(schemaID int64) []*replica.SpanReplication {
	return c.spanManager.GetReplicationDB().GetTasksBySchemaID(schemaID)
}

// GetTaskSizeBySchemaID 根据 schemaID 获取任务数量
func (c *Controller) GetTaskSizeBySchemaID(schemaID int64) int {
	return c.spanManager.GetReplicationDB().GetTaskSizeBySchemaID(schemaID)
}

func (c *Controller) GetAllNodes() []node.ID {
	return c.spanManager.GetAllNodes()
}

func (c *Controller) AddNewTable(table commonEvent.Table, startTs uint64) {
	if c.spanManager.GetReplicationDB().IsTableExists(table.TableID) {
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
	if c.enableTableAcrossNodes && len(c.spanManager.GetAllNodes()) > 1 {
		// split the whole table span base on region count if table region count is exceed the limit
		tableSpans = c.splitter.SplitSpansByRegion(context.Background(), tableSpan)
	}
	c.spanManager.AddNewSpans(table.SchemaID, tableSpans, startTs)
}

// GetTask 根据 dispatcherID 查询任务
func (c *Controller) GetTask(dispatcherID common.DispatcherID) *replica.SpanReplication {
	return c.spanManager.GetReplicationDB().GetTaskByID(dispatcherID)
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

// GetTasksByTableID 根据 tableID 获取任务列表
func (c *Controller) GetTasksByTableID(tableID int64) []*replica.SpanReplication {
	return c.spanManager.GetReplicationDB().GetTasksByTableID(tableID)
}

// GetAllTasks 获取所有任务
func (c *Controller) GetAllTasks() []*replica.SpanReplication {
	return c.spanManager.GetReplicationDB().GetAllTasks()
}

// UpdateSchemaID 更新表的 schema ID
func (c *Controller) UpdateSchemaID(tableID, newSchemaID int64) {
	c.spanManager.GetReplicationDB().UpdateSchemaID(tableID, newSchemaID)
}

// RemoveNode is called when a node is removed
func (c *Controller) RemoveNode(id node.ID) {
	c.operatorController.OnNodeRemoved(id)
}

// ScheduleFinished return false if not all task are running in working state
func (c *Controller) ScheduleFinished() bool {
	return c.operatorController.OperatorSizeWithLock() == 0 && c.spanManager.GetReplicationDB().GetAbsentSize() == 0
}

// TaskSize 获取总任务数量
func (c *Controller) TaskSize() int {
	return c.spanManager.GetReplicationDB().TaskSize()
}

// GetTaskSizeByNodeID 根据节点ID获取任务数量
func (c *Controller) GetTaskSizeByNodeID(id node.ID) int {
	return c.spanManager.GetReplicationDB().GetTaskSizeByNodeID(id)
}

// GetSchedulingSize 获取 scheduling 状态的任务数量
func (c *Controller) GetSchedulingSize() int {
	return c.spanManager.GetReplicationDB().GetSchedulingSize()
}

// GetReplicatingSize 获取 replicating 状态的任务数量
func (c *Controller) GetReplicatingSize() int {
	return c.spanManager.GetReplicationDB().GetReplicatingSize()
}

// GetAbsentSize 获取 absent 状态的任务数量
func (c *Controller) GetAbsentSize() int {
	return c.spanManager.GetReplicationDB().GetAbsentSize()
}

// IsTableExists 检查表是否存在
func (c *Controller) IsTableExists(tableID int64) bool {
	return c.spanManager.GetReplicationDB().IsTableExists(tableID)
}

func (c *Controller) addWorkingSpans(tableMap utils.Map[*heartbeatpb.TableSpan, *replica.SpanReplication]) {
	c.spanManager.AddWorkingSpans(tableMap)
}

func (c *Controller) addNewSpans(schemaID int64, tableSpans []*heartbeatpb.TableSpan, startTs uint64) {
	c.spanManager.AddNewSpans(schemaID, tableSpans, startTs)
}

func (c *Controller) Stop() {
	for _, handler := range c.taskHandles {
		handler.Cancel()
	}
}
