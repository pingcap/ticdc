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
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/heap"
)

// basicScheduler generates operators for the spans, and push them to the operator controller
// it generates add operator for the absent spans, and move operator for the unbalanced replicating spans
// currently, it only supports balance the spans by size
type basicScheduler struct {
	id        string
	batchSize int

	operatorController *operator.Controller
	changefeedDB       *changefeed.ChangefeedDB
	nodeManager        *watcher.NodeManager
}

func NewBasicScheduler(
	id string, batchSize int,
	oc *operator.Controller,
	changefeedDB *changefeed.ChangefeedDB,
	nodeManager *watcher.NodeManager,
) *basicScheduler {
	return &basicScheduler{
		id:                 id,
		batchSize:          batchSize,
		operatorController: oc,
		changefeedDB:       changefeedDB,
		nodeManager:        nodeManager,
	}
}

// Execute periodically execute the operator
func (s *basicScheduler) Execute() time.Time {
	availableSize := s.batchSize - s.operatorController.OperatorSize()
	if s.changefeedDB.GetAbsentSize() <= 0 || availableSize <= 0 {
		// can not schedule more operators, skip
		return time.Now().Add(time.Millisecond * 500)
	}
	if availableSize < s.batchSize/2 {
		// too many running operators, skip
		return time.Now().Add(time.Millisecond * 100)
	}

	s.doBasicSchedule(availableSize)

	return time.Now().Add(time.Millisecond * 500)
}

func (s *basicScheduler) doBasicSchedule(availableSize int) {
	id := replica.DefaultGroupID

	absentChangefeeds := s.changefeedDB.GetAbsentByGroup(id, availableSize)
	nodeSize := s.changefeedDB.GetTaskSizePerNodeByGroup(id)
	// add the absent node to the node size map
	for id := range s.nodeManager.GetAliveNodes() {
		if _, ok := nodeSize[id]; !ok {
			nodeSize[id] = 0
		}
	}

	s.basicSchedule(availableSize, absentChangefeeds, nodeSize)
}

// basicSchedule schedules the absent tasks to the available nodes
func (s *basicScheduler) basicSchedule(
	availableSize int,
	absentChangefeeds []*changefeed.Changefeed,
	nodeTasks map[node.ID]int,
) {
	if len(nodeTasks) == 0 {
		log.Warn("scheduler: no node available, skip")
		return
	}
	// TODO:change priorityQueue
	minPriorityQueue := priorityQueue{
		h:    heap.NewHeap[*item](),
		less: func(a, b int) bool { return a < b },
	}
	for key, size := range nodeTasks {
		minPriorityQueue.InitItem(key, size, nil)
	}

	taskSize := 0
	for _, cf := range absentChangefeeds {
		item, _ := minPriorityQueue.PeekTop()
		// the operator is pushed successfully
		op := s.operatorController.NewAddMaintainerOperator(cf, item.Node)
		ret := s.operatorController.AddOperator(op)
		if ret {
			// update the task size priority queue
			item.Load++
			taskSize++
		}
		if taskSize >= availableSize {
			break
		}
		minPriorityQueue.AddOrUpdate(item)
	}
}

func (s *basicScheduler) Name() string {
	return "basic-scheduler"
}
