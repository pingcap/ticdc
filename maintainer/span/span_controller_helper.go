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

package span

import (
	"sync"

	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
)

type spanMap struct {
	m     map[common.DispatcherID]*replica.SpanReplication
	mutex sync.RWMutex
}

func newSpanMap() *spanMap {
	return &spanMap{
		m: make(map[common.DispatcherID]*replica.SpanReplication),
	}
}

func (m *spanMap) set(span *replica.SpanReplication) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.m[span.ID] = span
}

func (m *spanMap) get(id common.DispatcherID) *replica.SpanReplication {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.m[id]
}

func (m *spanMap) tryGet(id common.DispatcherID) (*replica.SpanReplication, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	span, ok := m.m[id]
	return span, ok
}

func (m *spanMap) delete(id common.DispatcherID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.m, id)
}

func (m *spanMap) values() []*replica.SpanReplication {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	values := make([]*replica.SpanReplication, 0, len(m.m))
	for _, v := range m.m {
		values = append(values, v)
	}
	return values
}

func (m *spanMap) size() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.m)
}

type idToSpanMap struct {
	m     map[int64]*spanMap
	mutex sync.RWMutex
}

func newIdToSpanMap() *idToSpanMap {
	return &idToSpanMap{
		m: make(map[int64]*spanMap),
	}
}

func (m *idToSpanMap) set(id int64, span *replica.SpanReplication) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	schemaMap, ok := m.m[id]
	if !ok {
		schemaMap = newSpanMap()
	}
	schemaMap.set(span)
}

func (m *idToSpanMap) get(id int64) *spanMap {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.m[id]
}

func (m *idToSpanMap) tryGet(id int64) (*spanMap, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	sm, ok := m.m[id]
	return sm, ok
}

func (m *idToSpanMap) delete(id int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.m, id)
}
