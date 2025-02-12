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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// replicationGroup maintains a group of replication tasks.
// All methods are not thread-safe.
type replicationGroup[T ReplicationID, R Replication[T]] struct {
	id        string
	groupID   GroupID
	groupName string

	nodeTasks map[node.ID]map[T]R // group the tasks by the node id

	// maps that maintained base on the replica scheduling status
	replicating *iMap[T, R]
	scheduling  *iMap[T, R]
	absent      *iMap[T, R]

	checker GroupChecker[T, R]
}

func newReplicationGroup[T ReplicationID, R Replication[T]](
	id string, groupID GroupID, checker GroupChecker[T, R],
) *replicationGroup[T, R] {
	return &replicationGroup[T, R]{
		id:          id,
		groupID:     groupID,
		groupName:   GetGroupName(groupID),
		nodeTasks:   make(map[node.ID]map[T]R),
		replicating: newIMap[T, R](),
		scheduling:  newIMap[T, R](),
		absent:      newIMap[T, R](),
		checker:     checker,
	}
}

func (g *replicationGroup[T, R]) mustVerifyGroupID(id GroupID) {
	if g.groupID != id {
		log.Panic("scheduler: group id not match", zap.Int64("group", g.groupID), zap.Int64("id", id))
	}
}

// MarkReplicaAbsent move the replica to the absent status
func (g *replicationGroup[T, R]) MarkReplicaAbsent(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	log.Info("scheduler: marking replica absent",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("replicaID", replica.GetID().String()),
		zap.String("node", replica.GetNodeID().String()))

	id := replica.GetID()
	g.scheduling.Delete(id)
	g.replicating.Delete(id)
	g.absent.Set(id, replica)
	originNodeID := replica.GetNodeID()
	replica.SetNodeID("")
	g.updateNodeMap(originNodeID, "", replica)
}

// MarkReplicaScheduling move the replica to the scheduling map
func (g *replicationGroup[T, R]) MarkReplicaScheduling(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	log.Info("scheduler: marking replica scheduling",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("replica", replica.GetID().String()))

	g.absent.Delete(replica.GetID())
	g.replicating.Delete(replica.GetID())
	g.scheduling.Set(replica.GetID(), replica)
}

// AddReplicatingReplica adds a replicating the replicating map, that means the task is already scheduled to a dispatcher
func (g *replicationGroup[T, R]) AddReplicatingReplica(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	nodeID := replica.GetNodeID()
	log.Info("scheduler: add an replicating replica",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("nodeID", nodeID.String()),
		zap.String("replica", replica.GetID().String()))
	g.replicating.Set(replica.GetID(), replica)
	g.updateNodeMap("", nodeID, replica)
	g.checker.AddReplica(replica)
}

// MarkReplicaReplicating move the replica to the replicating map
func (g *replicationGroup[T, R]) MarkReplicaReplicating(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	log.Info("scheduler: marking replica replicating",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("replica", replica.GetID().String()))

	g.absent.Delete(replica.GetID())
	g.scheduling.Delete(replica.GetID())
	g.replicating.Set(replica.GetID(), replica)
}

func (g *replicationGroup[T, R]) BindReplicaToNode(old, new node.ID, replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	log.Info("scheduler: bind replica to node",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("replica", replica.GetID().String()),
		zap.String("oldNode", old.String()),
		zap.String("node", new.String()))

	replica.SetNodeID(new)
	g.absent.Delete(replica.GetID())
	g.replicating.Delete(replica.GetID())
	g.scheduling.Set(replica.GetID(), replica)
	g.updateNodeMap(old, new, replica)
}

// updateNodeMap updates the node map, it will remove the task from the old node and add it to the new node
func (g *replicationGroup[T, R]) updateNodeMap(old, new node.ID, replica R) {
	// clear from the old node
	if old != "" {
		oldMap, ok := g.nodeTasks[old]
		if ok {
			delete(oldMap, replica.GetID())
			if len(oldMap) == 0 {
				delete(g.nodeTasks, old)
			}
		}
	}
	// add to the new node if the new node is not empty
	if new != "" {
		newMap, ok := g.nodeTasks[new]
		if !ok {
			newMap = make(map[T]R)
			g.nodeTasks[new] = newMap
		}
		newMap[replica.GetID()] = replica
	}
}

func (g *replicationGroup[T, R]) AddAbsentReplica(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	g.absent.Set(replica.GetID(), replica)
	g.checker.AddReplica(replica)
}

func (g *replicationGroup[T, R]) RemoveReplica(replica R) {
	g.mustVerifyGroupID(replica.GetGroupID())
	log.Info("scheduler: remove replica",
		zap.String("schedulerID", g.id),
		zap.String("group", g.groupName),
		zap.String("replica", replica.GetID().String()))
	g.absent.Delete(replica.GetID())
	g.replicating.Delete(replica.GetID())
	g.scheduling.Delete(replica.GetID())
	nodeMap := g.nodeTasks[replica.GetNodeID()]
	delete(nodeMap, replica.GetID())
	if len(nodeMap) == 0 {
		delete(g.nodeTasks, replica.GetNodeID())
	}
	g.checker.RemoveReplica(replica)
}

func (g *replicationGroup[T, R]) IsEmpty() bool {
	return g.IsStable() && g.replicating.Len() == 0
}

func (g *replicationGroup[T, R]) IsStable() bool {
	return g.scheduling.Len() == 0 && g.absent.Len() == 0
}

func (g *replicationGroup[T, R]) GetTaskSizeByNodeID(nodeID node.ID) int {
	return len(g.nodeTasks[nodeID])
}

func (g *replicationGroup[T, R]) GetNodeTasks() map[node.ID]map[T]R {
	return g.nodeTasks
}

func (g *replicationGroup[T, R]) GetAbsentSize() int {
	return g.absent.Len()
}

func (g *replicationGroup[T, R]) GetAbsent() []R {
	res := make([]R, 0, g.absent.Len())
	g.absent.Range(func(_ T, r R) bool {
		if !r.ShouldRun() {
			return true
		}
		res = append(res, r)
		return true
	})
	return res
}

func (g *replicationGroup[T, R]) GetSchedulingSize() int {
	return g.scheduling.Len()
}

func (g *replicationGroup[T, R]) GetScheduling() []R {
	res := make([]R, 0, g.scheduling.Len())
	g.scheduling.Range(func(_ T, r R) bool {
		res = append(res, r)
		return true
	})
	return res
}

func (g *replicationGroup[T, R]) GetReplicatingSize() int {
	return g.replicating.Len()
}

func (g *replicationGroup[T, R]) GetReplicating() []R {
	res := make([]R, 0, g.replicating.Len())
	g.replicating.Range(func(_ T, r R) bool {
		res = append(res, r)
		return true
	})
	return res
}

func (g *replicationGroup[T, R]) GetTaskSizePerNode() map[node.ID]int {
	res := make(map[node.ID]int)
	for nodeID, tasks := range g.nodeTasks {
		res[nodeID] = len(tasks)
	}
	return res
}

type iMap[T ReplicationID, R Replication[T]] struct {
	inner sync.Map
}

func newIMap[T ReplicationID, R Replication[T]]() *iMap[T, R] {
	return &iMap[T, R]{inner: sync.Map{}}
}

func (m *iMap[T, R]) Get(key T) (R, bool) {
	var value R
	v, exists := m.inner.Load(key)
	if v != nil {
		value = v.(R)
	}
	return value, exists
}

func (m *iMap[T, R]) Set(key T, value R) {
	m.inner.Store(key, value)
}

func (m *iMap[T, R]) Delete(key T) {
	m.inner.Delete(key)
}

func (m *iMap[T, R]) Len() int {
	var count int
	m.inner.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

func (m *iMap[T, R]) Range(f func(T, R) bool) {
	m.inner.Range(func(k, v interface{}) bool {
		if rv, ok := v.(R); ok {
			return f(k.(T), rv)
		}
		return true
	})
}
