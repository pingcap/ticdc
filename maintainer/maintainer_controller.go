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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

// Controller schedules and balance tables
// there are 3 main components in the controller, scheduler, span controller and operator controller
type Controller struct {
	// bootstrapped set to true after initialize all necessary resources,
	// it's not affected by new node join the cluster.
	bootstrapped bool
	startTs      uint64

	schedulerController    *scheduler.Controller
	operatorController     *operator.Controller
	redoOperatorController *operator.Controller
	spanController         *span.Controller
	redoSpanController     *span.Controller
	barrier                *Barrier
	redoBarrier            *Barrier

	messageCenter messaging.MessageCenter
	nodeManager   *watcher.NodeManager

	splitter *split.Splitter

	replicaConfig *config.ReplicaConfig
	changefeedID  common.ChangeFeedID

	taskPool threadpool.ThreadPool

	// Store the task handles, it's used to stop the task handlers when the controller is stopped.
	taskHandles   []*threadpool.TaskHandle
	taskHandlesMu sync.RWMutex

	enableTableAcrossNodes bool
	batchSize              int

	keyspaceMeta common.KeyspaceMeta
	enableRedo   bool
}

func NewController(changefeedID common.ChangeFeedID,
	checkpointTs uint64,
	taskPool threadpool.ThreadPool,
	replicaConfig *config.ReplicaConfig,
	ddlSpan, redoDDLSpan *replica.SpanReplication,
	batchSize int, balanceInterval time.Duration,
	refresher *replica.RegionCountRefresher,
	keyspaceMeta common.KeyspaceMeta,
	enableRedo bool,
) *Controller {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)

	var (
		enableTableAcrossNodes bool
		splitter               *split.Splitter
	)
	if replicaConfig != nil && util.GetOrZero(replicaConfig.Scheduler.EnableTableAcrossNodes) {
		enableTableAcrossNodes = true
		splitter = split.NewSplitter(keyspaceMeta.ID, changefeedID, replicaConfig.Scheduler)
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)

	// Create span controller
	var schedulerCfg *config.ChangefeedSchedulerConfig
	if replicaConfig != nil {
		schedulerCfg = replicaConfig.Scheduler
	}
	spanController := span.NewController(changefeedID, ddlSpan, splitter, schedulerCfg, refresher, keyspaceMeta.ID, common.DefaultMode)

	var (
		redoSpanController *span.Controller
		redoOC             *operator.Controller
	)
	if enableRedo {
		redoSpanController = span.NewController(changefeedID, redoDDLSpan, splitter, schedulerCfg, refresher, keyspaceMeta.ID, common.RedoMode)
		redoOC = operator.NewOperatorController(changefeedID, redoSpanController, batchSize, common.RedoMode)
	}
	// Create operator controller using spanController
	oc := operator.NewOperatorController(changefeedID, spanController, batchSize, common.DefaultMode)

	sc := NewScheduleController(
		changefeedID, batchSize, oc, redoOC, spanController, redoSpanController, balanceInterval, splitter, schedulerCfg,
	)

	return &Controller{
		startTs:                checkpointTs,
		changefeedID:           changefeedID,
		bootstrapped:           false,
		schedulerController:    sc,
		operatorController:     oc,
		redoOperatorController: redoOC,
		spanController:         spanController,
		redoSpanController:     redoSpanController,
		messageCenter:          mc,
		nodeManager:            nodeManager,
		taskPool:               taskPool,
		replicaConfig:          replicaConfig,
		enableTableAcrossNodes: enableTableAcrossNodes,
		batchSize:              batchSize,
		splitter:               splitter,
		keyspaceMeta:           keyspaceMeta,
		enableRedo:             enableRedo,
	}
}

// HandleStatus handle the status report from the node
func (c *Controller) HandleStatus(from node.ID, statusList []*heartbeatpb.TableSpanStatus) {
	for _, status := range statusList {
		dispatcherID := common.NewDispatcherIDFromPB(status.ID)
		operatorController := c.getOperatorController(status.Mode)
		spanController := c.getSpanController(status.Mode)

		operatorController.UpdateOperatorStatus(dispatcherID, from, status)
		stm := spanController.GetTaskByID(dispatcherID)
		if stm == nil {
			if status.ComponentStatus != heartbeatpb.ComponentState_Working {
				continue
			}
			if op := operatorController.GetOperator(dispatcherID); op == nil {
				// it's normal case when the span is not found in replication db
				// the span is removed from replication db first, so here we only check if the span status is working or not
				log.Warn("no span found, remove it",
					zap.String("changefeed", c.changefeedID.Name()),
					zap.String("from", from.String()),
					zap.Any("status", status),
					zap.String("dispatcherID", dispatcherID.String()))
				// if the span is not found, and the status is working, we need to remove it from dispatcher
				_ = c.messageCenter.SendCommand(replica.NewRemoveDispatcherMessage(from, c.changefeedID, status.ID, nil, status.Mode, heartbeatpb.OperatorType_O_Remove))
			}
			continue
		}
		nodeID := stm.GetNodeID()
		if nodeID != from {
			// todo: handle the case that the nodeID is mismatch
			log.Warn("nodeID not match",
				zap.String("changefeed", c.changefeedID.Name()),
				zap.Any("from", from),
				zap.Stringer("node", nodeID))
			continue
		}
		spanController.UpdateStatus(stm, status)
	}
}

func (c *Controller) GetMinCheckpointTs(minCheckpointTs uint64) uint64 {
	minCheckpointTsForOperator := c.operatorController.GetMinCheckpointTs(minCheckpointTs)
	minCheckpointTsForSpan := c.spanController.GetMinCheckpointTsForNonReplicatingSpans(minCheckpointTs)
	return min(minCheckpointTsForOperator, minCheckpointTsForSpan)
}

func (c *Controller) Stop() {
	c.taskHandlesMu.RLock()
	for _, handler := range c.taskHandles {
		handler.Cancel()
	}
	c.taskHandlesMu.RUnlock()

	c.operatorController.Close()
	if c.enableRedo {
		c.redoOperatorController.Close()
	}
}

func (c *Controller) GetKeyspaceID() uint32 {
	return c.keyspaceMeta.ID
}

// RemoveNode is called when a node is removed
func (c *Controller) RemoveNode(id node.ID) {
	if c.enableRedo {
		c.redoOperatorController.OnNodeRemoved(id)
	}
	c.operatorController.OnNodeRemoved(id)
}

func (c *Controller) GetMinRedoCheckpointTs(minCheckpointTs uint64) uint64 {
	minCheckpointTsForOperator := c.redoOperatorController.GetMinCheckpointTs(minCheckpointTs)
	minCheckpointTsForSpan := c.redoSpanController.GetMinCheckpointTsForNonReplicatingSpans(minCheckpointTs)
	return min(minCheckpointTsForOperator, minCheckpointTsForSpan)
}
