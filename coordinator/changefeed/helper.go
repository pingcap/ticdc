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

package changefeed

import (
	"sync"

	"github.com/pingcap/ticdc/pkg/common"
)

type changefeedMap struct {
	m     map[common.ChangeFeedID]*Changefeed
	mutex sync.RWMutex
}

func newChangefeedMap() *changefeedMap {
	return &changefeedMap{m: make(map[common.ChangeFeedID]*Changefeed)}
}

func (m *changefeedMap) set(id common.ChangeFeedID, value *Changefeed) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.m[id] = value
}

func (m *changefeedMap) tryGet(id common.ChangeFeedID) (*Changefeed, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	value, ok := m.m[id]
	return value, ok
}

func (m *changefeedMap) get(id common.ChangeFeedID) *Changefeed {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.m[id]
}

func (m *changefeedMap) delete(id common.ChangeFeedID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.m, id)
}

func (m *changefeedMap) size() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.m)
}

func (m *changefeedMap) values() []*Changefeed {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	values := make([]*Changefeed, 0, len(m.m))
	for _, value := range m.m {
		values = append(values, value)
	}
	return values
}

type changefeedNameMap struct {
	m     map[common.ChangeFeedDisplayName]common.ChangeFeedID
	mutex sync.RWMutex
}

func newChangefeedNameMap() *changefeedNameMap {
	return &changefeedNameMap{m: make(map[common.ChangeFeedDisplayName]common.ChangeFeedID)}
}

func (m *changefeedNameMap) set(name common.ChangeFeedDisplayName, id common.ChangeFeedID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.m[name] = id
}

func (m *changefeedNameMap) tryGet(name common.ChangeFeedDisplayName) (common.ChangeFeedID, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	value, ok := m.m[name]
	return value, ok
}

func (m *changefeedNameMap) get(name common.ChangeFeedDisplayName) common.ChangeFeedID {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.m[name]
}

func (m *changefeedNameMap) delete(name common.ChangeFeedDisplayName) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.m, name)
}

func (m *changefeedNameMap) size() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.m)
}
