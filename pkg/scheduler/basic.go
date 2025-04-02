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
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/heap"
)

const schedulingTaskCountPerNode = 6

// basicScheduler generates operators for the spans, and push them to the operator controller
// it generates add operator for the absent spans, and move operator for the unbalanced replicating spans
// currently, it only supports balance the spans by size
type basicScheduler[T replica.ReplicationID, S replica.ReplicationStatus, R replica.Replication[T]] struct {
	id        string
	batchSize int

	operatorController operator.Controller[T, S]
	db                 replica.ScheduleGroup[T, R]
	nodeManager        *watcher.NodeManager

	absent         []R                                               // buffer for the absent spans
	newAddOperator func(r R, target node.ID) operator.Operator[T, S] // scheduler r to target node
}

func NewBasicScheduler[T replica.ReplicationID, S replica.ReplicationStatus, R replica.Replication[T]](
	id string, batchSize int,
	oc operator.Controller[T, S],
	db replica.ScheduleGroup[T, R],
	nodeManager *watcher.NodeManager,
	newAddOperator func(R, node.ID) operator.Operator[T, S],
) *basicScheduler[T, S, R] {
	return &basicScheduler[T, S, R]{
		id:                 id,
		batchSize:          batchSize,
		operatorController: oc,
		db:                 db,
		nodeManager:        nodeManager,
		absent:             make([]R, 0, batchSize),
		newAddOperator:     newAddOperator,
	}
}

// Execute periodically execute the operator
// 区分 split table 和 非 split table
func (s *basicScheduler[T, S, R]) Execute() time.Time {
	availableSize := s.batchSize - s.operatorController.OperatorSize()

	totalAbsentSize := s.db.GetAbsentSize()
	defaultGroupAbsentSize := s.db.GetAbsentSizeForGroup(replica.DefaultGroupID)

	if totalAbsentSize <= 0 || availableSize <= 0 {
		// can not schedule more operators, skip
		return time.Now().Add(time.Millisecond * 500)
	}
	if availableSize < s.batchSize/2 {
		// too many running operators, skip
		return time.Now().Add(time.Millisecond * 100)
	}

	// 分配一下 availableSize 的使用， 首先 non-default 的总开销不能大于 batchSize / 2，避免把常规表饿死
	// 每个节点对一个 split 表的空间设为 k（比如10个），有空余了才会花掉一点。
	// 可以先做个暴力的分发，一人一半，如果 前面的没这么多需要用的，后面最大额度也就是 batchSize / 2

	if defaultGroupAbsentSize == totalAbsentSize {
		// only have absent replicas in the default group
		s.schedule(replica.DefaultGroupID, availableSize)
	} else if defaultGroupAbsentSize == 0 {
		// only have absent replicas in non-default group
		maxAvailableSize := max(availableSize, s.batchSize/2)
		for _, id := range s.db.GetGroups() {
			if id == replica.DefaultGroupID {
				continue
			}
			maxAvailableSize -= s.schedule(id, maxAvailableSize)
			if maxAvailableSize <= 0 {
				break
			}
		}
	} else {
		availableSizeForNonDefault := min(min(max(availableSize/2, availableSize-defaultGroupAbsentSize), totalAbsentSize-defaultGroupAbsentSize), s.batchSize/2)
		availableSizeForDefault := min(defaultGroupAbsentSize, availableSize-defaultGroupAbsentSize)
		for _, id := range s.db.GetGroups() {
			if id == replica.DefaultGroupID {
				s.schedule(id, availableSizeForDefault)
			}
			availableSizeForNonDefault -= s.schedule(id, availableSize)
			if availableSizeForNonDefault <= 0 {
				break
			}
		}
	}
	return time.Now().Add(time.Millisecond * 500)
}

func (s *basicScheduler[T, S, R]) schedule(id replica.GroupID, availableSize int) (scheduled int) {
	if id == replica.DefaultGroupID {
		// default 的话，就按照目标是每个节点总的 dispatcher 数目量一致来处理
		absent := s.db.GetAbsentByGroup(id, availableSize)
		// 如果是非 default 组，看一下每个节点有几个 absent 任务， k - absent 任务就是能安排的量
		nodeSize := s.db.GetTaskSizePerNodeByGroup(id)
		// add the absent node to the node size map
		for id := range s.nodeManager.GetAliveNodes() {
			if _, ok := nodeSize[id]; !ok {
				nodeSize[id] = 0
			}
		}
		// what happens if the some node removed when scheduling?
		BasicSchedule(availableSize, absent, nodeSize, func(replication R, id node.ID) bool {
			op := s.newAddOperator(replication, id)
			return s.operatorController.AddOperator(op)
		})
		scheduled = len(absent)
		s.absent = absent[:0]
		return
	}
	// 非 default 的话，就按照目标是每个节点总的 scheding 数目维持一致来处理
	scheduled = 0
	absent := s.db.GetAbsentByGroup(id, availableSize)
	nodeSize := s.db.GetScheduleTaskSizePerNodeByGroup(id)
	spaceCount := 0
	for id := range s.nodeManager.GetAliveNodes() {
		if _, ok := nodeSize[id]; !ok {
			nodeSize[id] = 0
		}
		spaceCount += schedulingTaskCountPerNode - nodeSize[id]
	}

	if spaceCount <= len(absent) {
		// 给每个node 安排 schedulingTaskCountPerNode - nodeSize[id]
		for id, size := range nodeSize {
			taskList := absent[:schedulingTaskCountPerNode-size]
			count := 0
			for idx, task := range taskList {
				count = idx
				op := s.newAddOperator(task, id)
				if !s.operatorController.AddOperator(op) {
					count -= 1
					break
				} else {
					scheduled += 1
				}
			}
			absent = absent[count+1:]
		}
	} else {
		// 那就尽可能给每个节点均匀一点
		updated := false
		for len(absent) != 0 {
			for id, size := range nodeSize {
				if len(absent) == 0 {
					return
				}
				if size < schedulingTaskCountPerNode {
					op := s.newAddOperator(absent[0], id)
					if !s.operatorController.AddOperator(op) {
						continue
					} else {
						absent = absent[1:]
						nodeSize[id] += 1
						updated = true
						scheduled += 1
					}
				}
			}
			if !updated {
				break
			}
		}
	}
	return
}

func (s *basicScheduler[T, S, R]) Name() string {
	return BasicScheduler
}

// BasicSchedule schedules the absent tasks to the available nodes
func BasicSchedule[T replica.ReplicationID, R replica.Replication[T]](
	availableSize int,
	absent []R,
	nodeTasks map[node.ID]int,
	schedule func(R, node.ID) bool,
) {
	if len(nodeTasks) == 0 {
		log.Warn("scheduler: no node available, skip")
		return
	}
	minPriorityQueue := priorityQueue[T, R]{
		h:    heap.NewHeap[*item[T, R]](),
		less: func(a, b int) bool { return a < b },
	}
	for key, size := range nodeTasks {
		minPriorityQueue.InitItem(key, size, nil)
	}

	taskSize := 0
	for _, cf := range absent {
		item, _ := minPriorityQueue.PeekTop()
		// the operator is pushed successfully
		if schedule(cf, item.Node) {
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
