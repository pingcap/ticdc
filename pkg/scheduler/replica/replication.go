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

package replica

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type ReplicationID interface {
	comparable
	String() string
}

// Replication is the interface for the replication task
type Replication[T ReplicationID] interface {
	comparable
	// GetID returns the id of the replication task
	GetID() T
	// GetGroupID returns the group id of the replication task
	GetGroupID() GroupID
	// GetNodeID returns the node id this task is scheduled to
	GetNodeID() node.ID
	// SetNodeID sets the node id this task is scheduled to
	SetNodeID(node.ID)
	// ShouldRun returns true if the task should run
	ShouldRun() bool
}

// ScheduleGroup define the querying interface for scheduling information.
// Notice: all methods are thread-safe.
type ScheduleGroup[T ReplicationID, R Replication[T]] interface {
	GetAbsentSize() int
	GetAbsent() []R
	GetSchedulingSize() int
	GetScheduling() []R
	GetReplicatingSize() int
	GetReplicating() []R

	// group scheduler interface
	GetGroups() []GroupID
	GetGroupSize() int
	GetAbsentByGroup(groupID GroupID, batch int) []R
	GetSchedulingByGroup(groupID GroupID) []R
	GetReplicatingByGroup(groupID GroupID) []R
	GetTaskSizeByGroup(groupID GroupID) int
	GetGroupStat() string

	IsReplicating(replica R) bool

	// node scheduler interface
	GetTaskByNodeID(id node.ID) []R
	GetTaskSizeByNodeID(id node.ID) int
	GetTaskSizePerNode() map[node.ID]int
	GetTaskSizePerNodeByGroup(groupID GroupID) map[node.ID]int
	GetScheduleTaskSizePerNodeByGroup(groupID GroupID) map[node.ID]int

	GetGroupChecker(groupID GroupID) GroupChecker[T, R]
	GetCheckerStat() string
}

// ReplicationDB is responsible for managing the scheduling state of replication tasks.
//  1. It provides the interface for the scheduler to query the scheduling information.
//  2. It provides the interface for `Add/Remove“ replication tasks and update the scheduling state.
//  3. It maintains the scheduling group information internally.
type ReplicationDB[T ReplicationID, R Replication[T]] interface {
	ScheduleGroup[T, R]

	// The flowing methods are NOT thread-safe
	GetReplicatingWithoutLock() []R
	GetSchedulingWithoutLock() []R
	AddAbsentWithoutLock(task R)
	AddReplicatingWithoutLock(task R)

	MarkAbsentWithoutLock(task R)
	MarkSchedulingWithoutLock(task R)
	MarkReplicatingWithoutLock(task R)

	BindReplicaToNodeWithoutLock(old, new node.ID, task R)
	RemoveReplicaWithoutLock(task R)
	AddSchedulingReplicaWithoutLock(replica R, targetNodeID node.ID)
}

func NewReplicationDB[T ReplicationID, R Replication[T]](
	id string, withRLock func(action func()), newChecker func(GroupID) GroupChecker[T, R],
) ReplicationDB[T, R] {
	r := &replicationDB[T, R]{
		id:         id,
		taskGroups: make(map[GroupID]*replicationGroup[T, R]),
		withRLock:  withRLock,
		newChecker: newChecker,
	}
	r.taskGroups[DefaultGroupID] = newReplicationGroup(id, DefaultGroupID, r.newChecker(DefaultGroupID))
	return r
}

type replicationDB[T ReplicationID, R Replication[T]] struct {
	id         string
	withRLock  func(action func())
	newChecker func(GroupID) GroupChecker[T, R]
	taskGroups map[GroupID]*replicationGroup[T, R]
}

func (db *replicationDB[T, R]) GetGroups() []GroupID {
	groups := make([]GroupID, 0, db.GetGroupSize())
	db.withRLock(func() {
		for id := range db.taskGroups {
			groups = append(groups, id)
		}
	})
	return groups
}

func (db *replicationDB[T, R]) GetGroupSize() int {
	count := 0
	db.withRLock(func() {
		count = len(db.taskGroups)
	})
	return count
}

func (db *replicationDB[T, R]) GetGroupsWithoutLock() []GroupID {
	groups := make([]GroupID, 0, len(db.taskGroups))
	for id := range db.taskGroups {
		groups = append(groups, id)
	}
	return groups
}

func (db *replicationDB[T, R]) GetGroupChecker(groupID GroupID) (ret GroupChecker[T, R]) {
	db.withRLock(func() {
		ret = db.mustGetGroup(groupID).checker
	})
	return
}

func (db *replicationDB[T, R]) GetAbsent() []R {
	absent := make([]R, 0)
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			absent = append(absent, g.GetAbsent()...)
		}
	})
	return absent
}

func (db *replicationDB[T, R]) GetAbsentSize() int {
	size := 0
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			size += g.GetAbsentSize()
		}
	})
	return size
}

func (db *replicationDB[T, R]) GetAbsentByGroup(id GroupID, batch int) []R {
	buffer := make([]R, 0, batch)
	db.withRLock(func() {
		g := db.mustGetGroup(id)
		for _, stm := range g.GetAbsent() {
			buffer = append(buffer, stm)
			if len(buffer) >= batch {
				break
			}
		}
	})
	return buffer
}

func (db *replicationDB[T, R]) GetSchedulingByGroup(id GroupID) (ret []R) {
	db.withRLock(func() {
		g := db.mustGetGroup(id)
		ret = g.GetScheduling()
	})
	return
}

// GetReplicating returns the replicating spans
func (db *replicationDB[T, R]) GetReplicating() (ret []R) {
	db.withRLock(func() {
		ret = db.GetReplicatingWithoutLock()
	})
	return
}

func (db *replicationDB[T, R]) GetTaskSizeByGroup(id GroupID) (size int) {
	db.withRLock(func() {
		g := db.mustGetGroup(id)
		size = g.GetSize()
	})
	return
}

func (db *replicationDB[T, R]) GetReplicatingWithoutLock() (ret []R) {
	for _, g := range db.taskGroups {
		ret = append(ret, g.GetReplicating()...)
	}
	return
}

func (db *replicationDB[T, R]) GetReplicatingSize() int {
	size := 0
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			size += g.GetReplicatingSize()
		}
	})
	return size
}

func (db *replicationDB[T, R]) GetReplicatingByGroup(id GroupID) (ret []R) {
	db.withRLock(func() {
		g := db.mustGetGroup(id)
		ret = g.GetReplicating()
	})
	return
}

func (db *replicationDB[T, R]) GetScheduling() (ret []R) {
	db.withRLock(func() {
		ret = db.GetSchedulingWithoutLock()
	})
	return
}

func (db *replicationDB[T, R]) GetSchedulingWithoutLock() (ret []R) {
	for _, g := range db.taskGroups {
		ret = append(ret, g.GetScheduling()...)
	}
	return
}

func (db *replicationDB[T, R]) GetSchedulingSize() int {
	size := 0
	for _, g := range db.taskGroups {
		size += g.GetSchedulingSize()
	}
	return size
}

// GetTaskSizePerNode returns the size of the task per node
func (db *replicationDB[T, R]) GetTaskSizePerNode() (sizeMap map[node.ID]int) {
	sizeMap = make(map[node.ID]int)
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			for nodeID, tasks := range g.GetNodeTasks() {
				sizeMap[nodeID] += len(tasks)
			}
		}
	})
	return
}

func (db *replicationDB[T, R]) GetTaskByNodeID(id node.ID) (ret []R) {
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			for _, value := range g.GetNodeTasks()[id] {
				ret = append(ret, value)
			}
		}
	})
	return
}

func (db *replicationDB[T, R]) GetTaskSizeByNodeID(id node.ID) (size int) {
	db.withRLock(func() {
		for _, g := range db.taskGroups {
			size += g.GetTaskSizeByNodeID(id)
		}
	})
	return
}

func (db *replicationDB[T, R]) GetScheduleTaskSizePerNodeByGroup(id GroupID) (sizeMap map[node.ID]int) {
	db.withRLock(func() {
		sizeMap = db.getScheduleTaskSizePerNodeByGroup(id)
	})
	return
}

func (db *replicationDB[T, R]) getScheduleTaskSizePerNodeByGroup(id GroupID) (sizeMap map[node.ID]int) {
	sizeMap = make(map[node.ID]int)
	replicationGroup := db.mustGetGroup(id)
	for nodeID, tasks := range replicationGroup.GetNodeTasks() {
		count := 0
		for taskID := range tasks {
			if replicationGroup.scheduling.Find(taskID) {
				count++
			}
		}
		sizeMap[nodeID] = count
	}
	return
}

func (db *replicationDB[T, R]) GetTaskSizePerNodeByGroup(id GroupID) (sizeMap map[node.ID]int) {
	db.withRLock(func() {
		sizeMap = db.getTaskSizePerNodeByGroup(id)
	})
	return
}

func (db *replicationDB[T, R]) getTaskSizePerNodeByGroup(id GroupID) (sizeMap map[node.ID]int) {
	sizeMap = make(map[node.ID]int)
	for nodeID, tasks := range db.mustGetGroup(id).GetNodeTasks() {
		sizeMap[nodeID] = len(tasks)
	}
	return
}

func (db *replicationDB[T, R]) GetGroupStat() string {
	distribute := strings.Builder{}
	db.withRLock(func() {
		total := 0
		for _, group := range db.GetGroupsWithoutLock() {
			if total > 0 {
				distribute.WriteString(" ")
			}
			distribute.WriteString(GetGroupName(group))
			distribute.WriteString(": [")
			for nodeID, size := range db.getTaskSizePerNodeByGroup(group) {
				distribute.WriteString(nodeID.String())
				distribute.WriteString("->")
				distribute.WriteString(strconv.Itoa(size))
				distribute.WriteString("; ")
			}
			distribute.WriteString("]")
			total++
		}
	})
	return distribute.String()
}

func (db *replicationDB[T, R]) GetCheckerStat() string {
	stat := strings.Builder{}
	db.withRLock(func() {
		total := 0
		for groupID, group := range db.taskGroups {
			if total > 0 {
				stat.WriteString(" ")
			}
			stat.WriteString(GetGroupName(groupID))
			stat.WriteString(fmt.Sprintf("(%s)", group.checker.Name()))
			stat.WriteString(": [")
			stat.WriteString(group.checker.Stat())
			stat.WriteString("] ")
			total++
		}
	})
	return stat.String()
}

func (db *replicationDB[T, R]) getOrCreateGroup(task R) *replicationGroup[T, R] {
	groupID := task.GetGroupID()
	g, ok := db.taskGroups[groupID]
	if ok {
		return g
	}

	checker := db.newChecker(groupID)
	g = newReplicationGroup(db.id, groupID, checker)
	db.taskGroups[groupID] = g
	log.Info("scheduler: add new task group", zap.String("schedulerID", db.id),
		zap.String("group", GetGroupName(groupID)),
		zap.Int64("groupID", groupID))
	return g
}

func (db *replicationDB[T, R]) maybeRemoveGroup(g *replicationGroup[T, R]) {
	if g.groupID == DefaultGroupID || !g.IsEmpty() {
		return
	}
	delete(db.taskGroups, g.groupID)
	log.Info("scheduler: remove task group", zap.String("schedulerID", db.id),
		zap.String("group", GetGroupName(g.groupID)),
		zap.Stringer("groupType", GroupType(g.groupID>>56)))
	zap.Int64("groupID", int64(g.groupID))
}

func (db *replicationDB[T, R]) mustGetGroup(groupID GroupID) *replicationGroup[T, R] {
	g, ok := db.taskGroups[groupID]
	if !ok {
		log.Panic("group not found", zap.String("group", GetGroupName(groupID)))
	}
	return g
}

func (db *replicationDB[T, R]) AddReplicatingWithoutLock(task R) {
	g := db.getOrCreateGroup(task)
	g.AddReplicatingReplica(task)
}

func (db *replicationDB[T, R]) AddAbsentWithoutLock(task R) {
	g := db.getOrCreateGroup(task)
	g.AddAbsentReplica(task)
}

func (db *replicationDB[T, R]) MarkAbsentWithoutLock(task R) {
	g := db.mustGetGroup(task.GetGroupID())
	g.MarkReplicaAbsent(task)
}

func (db *replicationDB[T, R]) MarkSchedulingWithoutLock(task R) {
	g := db.mustGetGroup(task.GetGroupID())
	g.MarkReplicaScheduling(task)
}

func (db *replicationDB[T, R]) MarkReplicatingWithoutLock(task R) {
	g := db.mustGetGroup(task.GetGroupID())
	g.MarkReplicaReplicating(task)
}

func (db *replicationDB[T, R]) BindReplicaToNodeWithoutLock(old, new node.ID, replica R) {
	g := db.mustGetGroup(replica.GetGroupID())
	g.BindReplicaToNode(old, new, replica)
}

func (db *replicationDB[T, R]) RemoveReplicaWithoutLock(replica R) {
	g := db.mustGetGroup(replica.GetGroupID())
	g.RemoveReplica(replica)
	db.maybeRemoveGroup(g)
}

func (db *replicationDB[T, R]) AddSchedulingReplicaWithoutLock(replica R, targetNodeID node.ID) {
	g := db.mustGetGroup(replica.GetGroupID())
	g.AddSchedulingReplica(replica, targetNodeID)
}

func (db *replicationDB[T, R]) IsReplicating(replica R) bool {
	var ret bool
	db.withRLock(func() {
		g := db.mustGetGroup(replica.GetGroupID())
		ret = g.IsReplicating(replica)
	})
	return ret
}
