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

package scheduler

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	pkgScheduler "github.com/pingcap/ticdc/pkg/scheduler"
	pkgreplica "github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

// basicScheduler generates add operators for spans and pushes them to the operator controller.
// It creates add operators for absent spans to initiate replication.
//
// Absent spans fall into two categories:
// 1. Regular spans with groupID set to DefaultGroupID
// 2. Split table spans where each table's split spans share the same groupID
//
// During the transition from absent to replicating state, spans undergo incremental scanning.
// To maximize node utilization and balance incremental scan traffic across all nodes,
// we use each node's current scheduling size per group to guide span allocation.
//
// When there are many absent spans, we prioritize scheduling split table spans first,
// then allocate remaining capacity to regular spans.
// We ensure the total number of operators never exceeds batchSize.
type basicScheduler struct {
	changefeedID common.ChangeFeedID
	batchSize    int
	// the max scheduling task count for each non-default group in each node.
	schedulingTaskCountPerNode int

	operatorController *operator.Controller
	spanController     *span.Controller
	nodeManager        *watcher.NodeManager
	mode               int64
}

func NewBasicScheduler(
	changefeedID common.ChangeFeedID, batchSize int,
	oc *operator.Controller,
	spanController *span.Controller,
	schedulerCfg *config.ChangefeedSchedulerConfig,
	mode int64,
) *basicScheduler {
	scheduler := &basicScheduler{
		changefeedID:               changefeedID,
		batchSize:                  batchSize,
		operatorController:         oc,
		spanController:             spanController,
		nodeManager:                appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		schedulingTaskCountPerNode: 1,
		mode:                       mode,
	}

	if schedulerCfg != nil && schedulerCfg.SchedulingTaskCountPerNode > 0 {
		scheduler.schedulingTaskCountPerNode = schedulerCfg.SchedulingTaskCountPerNode
	}

	return scheduler
}

// Execute periodically execute the operator
func (s *basicScheduler) Execute() time.Time {
	// for each node, we limit the scheduling dispatcher for each group.
	// and only when the scheduling count is lower than the threshould,
	// we can assign new dispatcher for the nodes.
	// Thus, we can balance the resource of incremental scan.
	availableSize := s.batchSize - s.operatorController.OperatorSize()
	totalAbsentSize := s.spanController.GetAbsentSize()

	if totalAbsentSize <= 0 || availableSize <= 0 {
		// can not schedule more operators, skip
		return time.Now().Add(time.Millisecond * 500)
	}
	if availableSize < s.batchSize/2 {
		// too many running operators, skip
		return time.Now().Add(time.Millisecond * 100)
	}

	// deal with the split table spans first
	for _, id := range s.spanController.GetGroups() {
		if id == pkgreplica.DefaultGroupID {
			continue
		}
		availableSize -= s.schedule(id, availableSize)
		if availableSize <= 0 {
			break
		}
	}

	if availableSize > 0 {
		// still have available size, deal with the normal spans
		s.schedule(pkgreplica.DefaultGroupID, availableSize)
	}
	return time.Now().Add(time.Millisecond * 500)
}

func (s *basicScheduler) schedule(groupID pkgreplica.GroupID, availableSize int) int {
	scheduleNodeSize := s.spanController.GetScheduleTaskSizePerNodeByGroup(groupID)
	// for each group, we try to make each node have the same scheduling task count,
	// to make the usage of each node as balanced as possible.
	//
	// for the split table spans, each time each node can at most have s.schedulingTaskCountPerNode scheduling tasks.
	// for the normal spans, we don't have the upper limit.
	size := 0
	nodeIDs := s.nodeManager.GetAliveNodeIDs()
	nodeSize := make(map[node.ID]int)
	for _, id := range nodeIDs {
		nodeSize[id] = scheduleNodeSize[id]

		if groupID != pkgreplica.DefaultGroupID {
			num := s.schedulingTaskCountPerNode - nodeSize[id]
			if num >= 0 {
				size += num
			} else {
				log.Warn("available size for scheduler on node is negative", zap.String("node", id.String()), zap.Any("nodeSize", nodeSize[id]), zap.Int("schedulingTaskCountPerNode", s.schedulingTaskCountPerNode))
			}
		}
	}

	if groupID != pkgreplica.DefaultGroupID {
		if size == 0 {
			return 0
		}
		availableSize = min(availableSize, size)
	}

	if availableSize <= 0 {
		log.Warn("available size for scheduler is negative", zap.Int("availableSize", availableSize), zap.Any("groupID", groupID))
		return 0
	}

	absentReplications := s.spanController.GetAbsentByGroup(groupID, availableSize)

	pkgScheduler.BasicSchedule(availableSize, absentReplications, nodeSize, func(replication *replica.SpanReplication, id node.ID) bool {
		return s.operatorController.AddOperator(operator.NewAddDispatcherOperator(s.spanController, replication, id))
	})
	return len(absentReplications)
}

func (s *basicScheduler) Name() string {
	if common.IsRedoMode(s.mode) {
		return pkgScheduler.RedoBasicScheduler
	}
	return pkgScheduler.BasicScheduler
}
