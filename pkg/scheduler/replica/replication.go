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
	"sync"

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
	GetGroupIDs() []GroupID
	GetGroupSize() int
	GetAbsentByGroup(groupID GroupID, batch int) []R
	GetSchedulingByGroup(groupID GroupID) []R
	GetReplicatingByGroup(groupID GroupID) []R
	GetTaskSizeByGroup(groupID GroupID) int
	GetGroupStat() string

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
	GetScheduling() []R
	AddAbsent(task R)
	AddReplicating(task R)

	MarkAbsent(task R)
	MarkScheduling(task R)
	MarkReplicating(task R)

	BindReplicaToNode(old, new node.ID, task R)
	RemoveReplica(task R)
	AddSchedulingReplica(replica R, targetNodeID node.ID)
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

	mutex      sync.RWMutex // protect taskGroups
	taskGroups map[GroupID]*replicationGroup[T, R]
}

func (db *replicationDB[T, R]) GetGroupIDs() []GroupID {
	groups := make([]GroupID, 0, len(db.taskGroups))
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	for id := range db.taskGroups {
		groups = append(groups, id)
	}
	return groups
}

func (db *replicationDB[T, R]) GetGroups() []*replicationGroup[T, R] {
	groups := make([]*replicationGroup[T, R], 0, db.GetGroupSize())
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	for _, g := range db.taskGroups {
		groups = append(groups, g)
	}
	return groups
}

func (db *replicationDB[T, R]) GetGroupSize() int {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	count := len(db.taskGroups)
	return count
}

func (db *replicationDB[T, R]) GetGroupChecker(groupID GroupID) (ret GroupChecker[T, R]) {
	ret = db.mustGetGroup(groupID).checker
	return
}

func (db *replicationDB[T, R]) GetAbsent() []R {
	absent := make([]R, 0)
	groups := db.GetGroups()
	for _, g := range groups {
		absent = append(absent, g.GetAbsent()...)
	}
	return absent
}

func (db *replicationDB[T, R]) GetAbsentSize() int {
	size := 0
	groups := db.GetGroups()
	for _, g := range groups {
		size += g.GetAbsentSize()
	}
	return size
}

func (db *replicationDB[T, R]) GetAbsentByGroup(id GroupID, batch int) []R {
	buffer := make([]R, 0, batch)
	g := db.mustGetGroup(id)
	for _, stm := range g.GetAbsent() {
		buffer = append(buffer, stm)
		if len(buffer) >= batch {
			break
		}
	}
	return buffer
}

func (db *replicationDB[T, R]) GetSchedulingByGroup(id GroupID) (ret []R) {
	g := db.mustGetGroup(id)
	ret = g.GetScheduling()
	return
}

func (db *replicationDB[T, R]) GetTaskSizeByGroup(id GroupID) (size int) {
	g := db.mustGetGroup(id)
	size = g.GetSize()
	return
}

func (db *replicationDB[T, R]) GetReplicating() (ret []R) {
	groups := db.GetGroups()
	for _, g := range groups {
		ret = append(ret, g.GetReplicating()...)
	}
	return
}

func (db *replicationDB[T, R]) GetReplicatingSize() int {
	size := 0
	groups := db.GetGroups()
	for _, g := range groups {
		size += g.GetReplicatingSize()
	}
	return size
}

func (db *replicationDB[T, R]) GetReplicatingByGroup(id GroupID) (ret []R) {
	g := db.mustGetGroup(id)
	ret = g.GetReplicating()
	return
}

func (db *replicationDB[T, R]) GetScheduling() (ret []R) {
	groups := db.GetGroups()
	for _, g := range groups {
		ret = append(ret, g.GetScheduling()...)
	}
	return
}

func (db *replicationDB[T, R]) GetSchedulingSize() int {
	size := 0
	groups := db.GetGroups()
	for _, g := range groups {
		size += g.GetSchedulingSize()
	}
	return size
}

// GetTaskSizePerNode returns the size of the task per node
func (db *replicationDB[T, R]) GetTaskSizePerNode() (sizeMap map[node.ID]int) {
	sizeMap = make(map[node.ID]int)
	groups := db.GetGroups()
	for _, g := range groups {
		nodeIDs := g.GetNodeIDs()
		for _, nodeID := range nodeIDs {
			sizeMap[nodeID] += g.GetTaskSizeByNodeID(nodeID)
		}
	}
	return
}

func (db *replicationDB[T, R]) GetTaskByNodeID(id node.ID) (ret []R) {
	groups := db.GetGroups()
	for _, g := range groups {
		tasks := g.GetTasksByNodeID(id)
		ret = append(ret, tasks...)
	}
	return
}

func (db *replicationDB[T, R]) GetTaskSizeByNodeID(id node.ID) (size int) {
	groups := db.GetGroups()
	for _, g := range groups {
		size += g.GetTaskSizeByNodeID(id)
	}
	return
}

func (db *replicationDB[T, R]) GetScheduleTaskSizePerNodeByGroup(id GroupID) (sizeMap map[node.ID]int) {
	sizeMap = db.getScheduleTaskSizePerNodeByGroup(id)
	return
}

func (db *replicationDB[T, R]) getScheduleTaskSizePerNodeByGroup(id GroupID) (sizeMap map[node.ID]int) {
	sizeMap = make(map[node.ID]int)
	replicationGroup := db.mustGetGroup(id)
	nodeIDs := replicationGroup.GetNodeIDs()

	for _, nodeID := range nodeIDs {
		tasks := replicationGroup.GetTasksByNodeID(nodeID)
		count := 0
		for _, task := range tasks {
			if replicationGroup.scheduling.Find(task.GetID()) {
				count++
			}
		}
		sizeMap[nodeID] = count
	}
	return
}

func (db *replicationDB[T, R]) GetTaskSizePerNodeByGroup(id GroupID) (sizeMap map[node.ID]int) {
	sizeMap = make(map[node.ID]int)
	g := db.mustGetGroup(id)
	nodeIDs := g.GetNodeIDs()
	for _, nodeID := range nodeIDs {
		sizeMap[nodeID] = g.GetTaskSizeByNodeID(nodeID)
	}
	return
}

func (db *replicationDB[T, R]) GetGroupStat() string {
	distribute := strings.Builder{}

	total := 0
	for _, group := range db.GetGroupIDs() {
		if total > 0 {
			distribute.WriteString(" ")
		}
		distribute.WriteString(GetGroupName(group))
		distribute.WriteString(": [")
		for nodeID, size := range db.GetTaskSizePerNodeByGroup(group) {
			distribute.WriteString(nodeID.String())
			distribute.WriteString("->")
			distribute.WriteString(strconv.Itoa(size))
			distribute.WriteString("; ")
		}
		distribute.WriteString("]")
		total++
	}
	return distribute.String()
}

func (db *replicationDB[T, R]) GetCheckerStat() string {
	stat := strings.Builder{}
	groups := db.GetGroups()
	total := 0
	for _, group := range groups {
		if total > 0 {
			stat.WriteString(" ")
		}
		stat.WriteString(GetGroupName(group.groupID))
		stat.WriteString(fmt.Sprintf("(%s)", group.checker.Name()))
		stat.WriteString(": [")
		stat.WriteString(group.checker.Stat())
		stat.WriteString("] ")
		total++
	}
	return stat.String()
}

func (db *replicationDB[T, R]) getOrCreateGroup(task R) *replicationGroup[T, R] {
	groupID := task.GetGroupID()

	db.mutex.RLock()
	g, ok := db.taskGroups[groupID]
	db.mutex.Unlock()

	if !ok {
		checker := db.newChecker(groupID)
		g = newReplicationGroup(db.id, groupID, checker)

		db.mutex.Lock()
		db.taskGroups[groupID] = g
		db.mutex.Unlock()

		log.Info("scheduler: add new task group", zap.String("schedulerID", db.id),
			zap.String("group", GetGroupName(groupID)),
			zap.Stringer("groupType", GroupType(groupID)))
	}
	return g
}

func (db *replicationDB[T, R]) maybeRemoveGroup(g *replicationGroup[T, R]) {
	if g.groupID == DefaultGroupID || !g.IsEmpty() {
		return
	}
	db.mutex.Lock()
	delete(db.taskGroups, g.groupID)
	db.mutex.Unlock()
	log.Info("scheduler: remove task group", zap.String("schedulerID", db.id),
		zap.String("group", GetGroupName(g.groupID)),
		zap.Stringer("groupType", GroupType(g.groupID)))
}

func (db *replicationDB[T, R]) mustGetGroup(groupID GroupID) *replicationGroup[T, R] {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	g, ok := db.taskGroups[groupID]
	if !ok {
		log.Panic("group not found", zap.String("group", GetGroupName(groupID)))
	}
	return g
}

func (db *replicationDB[T, R]) AddReplicating(task R) {
	g := db.getOrCreateGroup(task)
	g.AddReplicatingReplica(task)
}

func (db *replicationDB[T, R]) AddAbsent(task R) {
	g := db.getOrCreateGroup(task)
	g.AddAbsentReplica(task)
}

func (db *replicationDB[T, R]) MarkAbsent(task R) {
	g := db.mustGetGroup(task.GetGroupID())
	g.MarkReplicaAbsent(task)
}

func (db *replicationDB[T, R]) MarkScheduling(task R) {
	g := db.mustGetGroup(task.GetGroupID())
	g.MarkReplicaScheduling(task)
}

func (db *replicationDB[T, R]) MarkReplicating(task R) {
	g := db.mustGetGroup(task.GetGroupID())
	g.MarkReplicaReplicating(task)
}

func (db *replicationDB[T, R]) BindReplicaToNode(old, new node.ID, replica R) {
	g := db.mustGetGroup(replica.GetGroupID())
	g.BindReplicaToNode(old, new, replica)
}

func (db *replicationDB[T, R]) RemoveReplica(replica R) {
	g := db.mustGetGroup(replica.GetGroupID())
	g.RemoveReplica(replica)
	db.maybeRemoveGroup(g)
}

func (db *replicationDB[T, R]) AddSchedulingReplica(replica R, targetNodeID node.ID) {
	g := db.mustGetGroup(replica.GetGroupID())
	g.AddSchedulingReplica(replica, targetNodeID)
}
