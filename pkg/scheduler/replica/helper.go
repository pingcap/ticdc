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

	"github.com/pingcap/ticdc/pkg/node"
)

type replicationMap[T ReplicationID, R Replication[T]] struct {
	m     map[T]R
	mutex sync.RWMutex
}

func newReplicationMap[T ReplicationID, R Replication[T]]() *replicationMap[T, R] {
	return &replicationMap[T, R]{m: make(map[T]R)}
}

func (m *replicationMap[T, R]) set(key T, value R) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.m[key] = value
}

func (m *replicationMap[T, R]) tryGet(key T) (R, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	value, ok := m.m[key]
	return value, ok
}

func (m *replicationMap[T, R]) get(key T) R {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.m[key]
}

func (m *replicationMap[T, R]) delete(key T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.m, key)
}

func (m *replicationMap[T, R]) values() []R {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	values := make([]R, 0, len(m.m))
	for _, value := range m.m {
		values = append(values, value)
	}
	return values
}

func (m *replicationMap[T, R]) size() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.m)
}

type nodeMap[T ReplicationID, R Replication[T]] struct {
	m     map[node.ID]*replicationMap[T, R]
	mutex sync.RWMutex
}

func newNodeMap[T ReplicationID, R Replication[T]]() *nodeMap[T, R] {
	return &nodeMap[T, R]{m: make(map[node.ID]*replicationMap[T, R])}
}

func (m *nodeMap[T, R]) set(nodeID node.ID, value *replicationMap[T, R]) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.m[nodeID] = value
}

func (m *nodeMap[T, R]) tryGet(nodeID node.ID) (*replicationMap[T, R], bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	value, ok := m.m[nodeID]
	return value, ok
}

func (m *nodeMap[T, R]) get(nodeID node.ID) *replicationMap[T, R] {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.m[nodeID]
}

func (m *nodeMap[T, R]) delete(nodeID node.ID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.m, nodeID)
}

func (m *nodeMap[T, R]) keys() []node.ID {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	keys := make([]node.ID, 0, len(m.m))
	for key := range m.m {
		keys = append(keys, key)
	}
	return keys
}
