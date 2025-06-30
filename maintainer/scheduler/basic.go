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

	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	pkgScheduler "github.com/pingcap/ticdc/pkg/scheduler"
	pkgreplica "github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
)

// basicScheduler generates operators for the spans, and push them to the operator controller
// it generates add operator for the absent spans, and move operator for the unbalanced replicating spans
// currently, it only supports balance the spans by size
type basicScheduler struct {
	id        string
	batchSize int
	// the max scheduling task count for each group in each node.
	// TODO: we need to select a good value
	schedulingTaskCountPerNode int

	operatorController *operator.Controller
	spanController     *span.Controller
	nodeManager        *watcher.NodeManager
}

func NewBasicScheduler(
	id string, batchSize int,
	oc *operator.Controller,
	spanController *span.Controller,
	schedulerCfg *config.ChangefeedSchedulerConfig,
) *basicScheduler {
	scheduler := &basicScheduler{
		id:                         id,
		batchSize:                  batchSize,
		operatorController:         oc,
		spanController:             spanController,
		nodeManager:                appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		schedulingTaskCountPerNode: 1,
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

	for _, id := range s.spanController.GetGroups() {
		availableSize -= s.schedule(id, availableSize)
		if availableSize <= 0 {
			break
		}
	}

	return time.Now().Add(time.Millisecond * 500)
}

func (s *basicScheduler) schedule(groupID pkgreplica.GroupID, availableSize int) (scheduled int) {
	scheduleNodeSize := s.spanController.GetScheduleTaskSizePerNodeByGroup(groupID)

	// calculate the space based on schedule count
	size := 0
	for id := range s.nodeManager.GetAliveNodes() {
		if _, ok := scheduleNodeSize[id]; !ok {
			scheduleNodeSize[id] = 0
		}
		if groupID == pkgreplica.DefaultGroupID {
			// for default group, each node can support more task
			size += s.schedulingTaskCountPerNode*10 - scheduleNodeSize[id]
		} else {
			size += s.schedulingTaskCountPerNode - scheduleNodeSize[id]
		}

	}

	if size == 0 {
		// no available slot for new replication task
		return
	}
	availableSize = min(availableSize, size)

	absentReplications := s.spanController.GetAbsentByGroup(groupID, availableSize)

	pkgScheduler.BasicSchedule(availableSize, absentReplications, scheduleNodeSize, func(replication *replica.SpanReplication, id node.ID) bool {
		return s.operatorController.AddOperator(operator.NewAddDispatcherOperator(s.spanController, replication, id))
	})
	scheduled = len(absentReplications)
	return
}

func (s *basicScheduler) Name() string {
	return "basic-scheduler"
}
