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
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/ticdc/utils/heap"
	"go.uber.org/zap"
)

// balanceScheduler is used to check the balance status of all spans among all nodes
type balanceScheduler struct {
	changefeedID common.ChangeFeedID
	batchSize    int

	operatorController *operator.Controller
	replicationDB      *replica.ReplicationDB
	nodeManager        *watcher.NodeManager

	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
	// forceBalance forces the scheduler to produce schedule tasks regardless of
	// `checkBalanceInterval`.
	// It is set to true when the last time `Schedule` produces some tasks,
	// and it is likely there are more tasks will be produced in the next
	// `Schedule`.
	// It speeds up rebalance.
	forceBalance bool
}

func NewBalanceScheduler(
	changefeedID common.ChangeFeedID, batchSize int,
	oc *operator.Controller, replicationDB *replica.ReplicationDB,
	nodeManager *watcher.NodeManager, balanceInterval time.Duration,
) *balanceScheduler {
	return &balanceScheduler{
		changefeedID:         changefeedID,
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		operatorController:   oc,
		replicationDB:        replicationDB,
		nodeManager:          nodeManager,
		checkBalanceInterval: balanceInterval,
		lastRebalanceTime:    time.Now(),
	}
}

func (s *balanceScheduler) Execute() time.Time {
	if !s.forceBalance && time.Since(s.lastRebalanceTime) < s.checkBalanceInterval {
		return s.lastRebalanceTime.Add(s.checkBalanceInterval)
	}
	now := time.Now()

	failpoint.Inject("StopBalanceScheduler", func() time.Time {
		return now.Add(s.checkBalanceInterval)
	})

	if s.operatorController.OperatorSize() > 0 || s.replicationDB.GetAbsentSize() > 0 {
		// not in stable schedule state, skip balance
		return now.Add(s.checkBalanceInterval)
	}

	nodes := s.nodeManager.GetAliveNodes()
	moved := s.schedulerGroup(nodes)
	if moved == 0 {
		// all groups are balanced, safe to do the global balance
		moved = s.schedulerGlobal(nodes)
	}

	s.forceBalance = moved >= s.batchSize
	s.lastRebalanceTime = time.Now()

	return now.Add(s.checkBalanceInterval)
}

func (s *balanceScheduler) Name() string {
	return "balance-scheduler"
}

func (s *balanceScheduler) schedulerGroup(nodes map[node.ID]*node.Info) int {
	batch, moved := s.batchSize, 0
	for _, group := range s.replicationDB.GetGroups() {
		// fast path, check the balance status
		moveSize := CheckBalanceStatus(s.replicationDB.GetTaskSizePerNodeByGroup(group), nodes)
		if moveSize <= 0 {
			// no need to do the balance, skip
			continue
		}
		replicas := s.replicationDB.GetReplicatingByGroup(group)
		moved += Balance(batch, s.random, nodes, replicas, s.doMove)
		if moved >= batch {
			break
		}
	}
	return moved
}

// TODO: refactor and simplify the implementation and limit max group size
func (s *balanceScheduler) schedulerGlobal(nodes map[node.ID]*node.Info) int {
	// fast path, check the balance status
	moveSize := CheckBalanceStatus(s.replicationDB.GetTaskSizePerNode(), nodes)
	if moveSize <= 0 {
		// no need to do the balance, skip
		return 0
	}
	groupNodetasks, valid := s.replicationDB.GetImbalanceGroupNodeTask(nodes)
	if !valid {
		// no need to do the balance, skip
		return 0
	}

	// complexity note: len(nodes) * len(groups)
	totalTasks := 0
	sizePerNode := make(map[node.ID]int, len(nodes))
	for _, nodeTasks := range groupNodetasks {
		for id, task := range nodeTasks {
			if task != nil {
				totalTasks++
				sizePerNode[id]++
			}
		}
	}
	lowerLimitPerNode := int(math.Floor(float64(totalTasks) / float64(len(nodes))))
	limitCnt := 0
	for _, size := range sizePerNode {
		if size == lowerLimitPerNode {
			limitCnt++
		}
	}
	if limitCnt == len(nodes) {
		// all nodes are global balanced
		return 0
	}

	moved := 0
	for _, nodeTasks := range groupNodetasks {
		availableNodes, victims, next := []node.ID{}, []node.ID{}, 0
		for id, task := range nodeTasks {
			if task != nil && sizePerNode[id] > lowerLimitPerNode {
				victims = append(victims, id)
			} else if task == nil && sizePerNode[id] < lowerLimitPerNode {
				availableNodes = append(availableNodes, id)
			}
		}

		for _, new := range availableNodes {
			if next >= len(victims) {
				break
			}
			old := victims[next]
			if s.doMove(nodeTasks[old], new) {
				sizePerNode[old]--
				sizePerNode[new]++
				next++
				moved++
			}
		}
	}
	log.Info("finish global balance", zap.Stringer("changefeed", s.changefeedID), zap.Int("moved", moved))
	return moved
}

func (s *balanceScheduler) doMove(replication *replica.SpanReplication, id node.ID) bool {
	op := operator.NewMoveDispatcherOperator(s.replicationDB, replication, replication.GetNodeID(), id)
	return s.operatorController.AddOperator(op)
}

// CheckBalanceStatus checks the dispatcher scheduling balance status
// returns the table size need to be moved
func CheckBalanceStatus(nodeTaskSize map[node.ID]int, allNodes map[node.ID]*node.Info) int {
	// add the absent node to the node size map
	for nodeID := range allNodes {
		if _, ok := nodeTaskSize[nodeID]; !ok {
			nodeTaskSize[nodeID] = 0
		}
	}
	totalSize := 0
	for _, ts := range nodeTaskSize {
		totalSize += ts
	}
	lowerLimitPerCapture := int(math.Floor(float64(totalSize) / float64(len(nodeTaskSize))))
	// tables need to be moved
	moveSize := 0
	for _, ts := range nodeTaskSize {
		tableNum2Add := lowerLimitPerCapture - ts
		if tableNum2Add > 0 {
			moveSize += tableNum2Add
		}
	}
	return moveSize
}

// Balance balances the running task by task size per node
func Balance(
	batchSize int, random *rand.Rand,
	activeNodes map[node.ID]*node.Info,
	replicating []*replica.SpanReplication, move func(*replica.SpanReplication, node.ID) bool,
) (movedSize int) {
	nodeTasks := make(map[node.ID][]*replica.SpanReplication)
	for _, task := range replicating {
		nodeID := task.GetNodeID()
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make([]*replica.SpanReplication, 0)
		}
		nodeTasks[nodeID] = append(nodeTasks[nodeID], task)
	}

	absentNodeCnt := 0
	// add the absent node to the node size map
	for nodeID := range activeNodes {
		if _, ok := nodeTasks[nodeID]; !ok {
			nodeTasks[nodeID] = make([]*replica.SpanReplication, 0)
			absentNodeCnt++
		}
	}

	totalSize := len(replicating)
	lowerLimitPerCapture := int(math.Floor(float64(totalSize) / float64(len(nodeTasks))))
	minPriorityQueue := priorityQueue{
		h:    heap.NewHeap[*item](),
		less: func(a, b int) bool { return a < b },
		rand: random,
	}
	maxPriorityQueue := priorityQueue{
		h:    heap.NewHeap[*item](),
		less: func(a, b int) bool { return a > b },
		rand: random,
	}
	totalMoveSize := 0
	for nodeID, tasks := range nodeTasks {
		tableNum2Add := lowerLimitPerCapture - len(tasks)
		if tableNum2Add <= 0 {
			// Complexity note: Shuffle has O(n), where `n` is the number of tables.
			// Also, during a single call of `Schedule`, Shuffle can be called at most
			// `c` times, where `c` is the number of captures (RiCDC nodes).
			// Only called when a rebalance is triggered, which happens rarely,
			// we do not expect a performance degradation as a result of adding
			// the randomness.
			random.Shuffle(len(tasks), func(i, j int) {
				tasks[i], tasks[j] = tasks[j], tasks[i]
			})
			maxPriorityQueue.InitItem(nodeID, len(tasks), tasks)
			continue
		} else {
			minPriorityQueue.InitItem(nodeID, len(tasks), nil)
			totalMoveSize += tableNum2Add
		}
	}
	if totalMoveSize == 0 {
		return 0
	}

	movedSize = 0
	for {
		target, _ := minPriorityQueue.PeekTop()
		if target.Load >= lowerLimitPerCapture {
			// the minimum workload has reached the lower limit
			break
		}
		victim, _ := maxPriorityQueue.PeekTop()
		task := victim.Tasks[0]
		if move(task, target.Node) {
			// update the task size priority queue
			target.Load++
			victim.Load--
			victim.Tasks = victim.Tasks[1:]
			movedSize++
			if movedSize >= batchSize || movedSize >= totalMoveSize {
				break
			}
		}

		minPriorityQueue.AddOrUpdate(target)
		maxPriorityQueue.AddOrUpdate(victim)
	}

	log.Info("scheduler: balance done",
		zap.Int("movedSize", movedSize),
		zap.Int("victims", totalMoveSize))
	return movedSize
}
