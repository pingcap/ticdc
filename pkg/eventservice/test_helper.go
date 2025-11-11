// Copyright 2025 PingCAP, Inc.
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

package eventservice

import (
	"context"
	"math"
	"sort"

	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/kv"
)

type mockVersionTableInfo struct {
	tableInfos    []*common.TableInfo
	deleteVersion uint64
}

type mockSchemaStore struct {
	DDLEvents map[common.TableID][]commonEvent.DDLEvent
	TableInfo map[common.TableID]*mockVersionTableInfo
	Tables    []commonEvent.Table

	resolvedTs     uint64
	maxDDLCommitTs uint64
}

func NewMockSchemaStore() *mockSchemaStore {
	return &mockSchemaStore{
		DDLEvents:      make(map[common.TableID][]commonEvent.DDLEvent),
		TableInfo:      make(map[common.TableID]*mockVersionTableInfo),
		resolvedTs:     math.MaxUint64,
		maxDDLCommitTs: math.MaxUint64,
	}
}

func (m *mockSchemaStore) Name() string {
	return "mockSchemaStore"
}

func (m *mockSchemaStore) Run(ctx context.Context) error {
	return nil
}

func (m *mockSchemaStore) Close(ctx context.Context) error {
	return nil
}

func (m *mockSchemaStore) DeleteTable(id common.TableID, ts common.Ts) {
	if info, ok := m.TableInfo[id]; ok {
		info.deleteVersion = uint64(ts)
	} else {
		m.TableInfo[id] = &mockVersionTableInfo{
			tableInfos:    make([]*common.TableInfo, 0),
			deleteVersion: uint64(ts),
		}
	}
}

func (m *mockSchemaStore) AppendDDLEvent(id common.TableID, ddls ...commonEvent.DDLEvent) {
	for _, ddl := range ddls {
		m.DDLEvents[id] = append(m.DDLEvents[id], ddl)
		if _, ok := m.TableInfo[id]; !ok {
			m.TableInfo[id] = &mockVersionTableInfo{
				tableInfos:    make([]*common.TableInfo, 0),
				deleteVersion: math.MaxUint64,
			}
		}
		m.TableInfo[id].tableInfos = append(m.TableInfo[id].tableInfos, ddl.TableInfo)
	}
}

func (m *mockSchemaStore) SetTables(tables []commonEvent.Table) {
	m.Tables = tables
}

func (m *mockSchemaStore) GetTableInfo(keyspaceMeta common.KeyspaceMeta, tableID common.TableID, ts common.Ts) (*common.TableInfo, error) {
	if info, ok := m.TableInfo[tableID]; ok {
		if info.deleteVersion <= uint64(ts) {
			return nil, &schemastore.TableDeletedError{}
		}
		infos := m.TableInfo[tableID].tableInfos
		idx := sort.Search(len(infos), func(i int) bool {
			return infos[i].GetUpdateTS() > uint64(ts)
		})
		if idx == 0 {
			return nil, nil
		}
		return infos[idx-1], nil
	}
	return nil, nil
}

func (m *mockSchemaStore) GetAllPhysicalTables(keyspaceMeta common.KeyspaceMeta, snapTs uint64, filter filter.Filter) ([]commonEvent.Table, error) {
	return m.Tables, nil
}

func (m *mockSchemaStore) GetTableDDLEventState(keyspaceMeta common.KeyspaceMeta, tableID int64) (schemastore.DDLEventState, error) {
	return schemastore.DDLEventState{
		ResolvedTs:       m.resolvedTs,
		MaxEventCommitTs: m.maxDDLCommitTs,
	}, nil
}

func (m *mockSchemaStore) RegisterTable(
	keyspaceMeta common.KeyspaceMeta,
	tableID int64,
	startTS common.Ts,
) error {
	return nil
}

func (m *mockSchemaStore) UnregisterTable(_ common.KeyspaceMeta, _ int64) error {
	return nil
}

// GetNextDDLEvents returns the next ddl event which finishedTs is within the range (start, end]
func (m *mockSchemaStore) FetchTableDDLEvents(keyspaceMeta common.KeyspaceMeta, dispatcherID common.DispatcherID, tableID int64, tableFilter filter.Filter, start, end uint64) ([]commonEvent.DDLEvent, error) {
	events := m.DDLEvents[tableID]
	if len(events) == 0 {
		return nil, nil
	}
	l := sort.Search(len(events), func(i int) bool {
		return events[i].FinishedTs > start
	})
	if l == len(events) {
		return nil, nil
	}
	r := sort.Search(len(events), func(i int) bool {
		return events[i].FinishedTs > end
	})
	return events[l:r], nil
}

func (m *mockSchemaStore) FetchTableTriggerDDLEvents(keyspaceMeta common.KeyspaceMeta, dispatcherID common.DispatcherID, tableFilter filter.Filter, start uint64, limit int) ([]commonEvent.DDLEvent, uint64, error) {
	return nil, 0, nil
}

func (m *mockSchemaStore) RegisterKeyspace(ctx context.Context, keyspaceMeta common.KeyspaceMeta) error {
	return nil
}

func (m *mockSchemaStore) GetKVStorage(keyspaceID uint32) (kv.Storage, error) {
	return nil, nil
}
