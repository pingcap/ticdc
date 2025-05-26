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

package common

import (
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

type accessKey struct {
	schema string
	table  string
}

// tableInfoAccessor provide table information, to helper
// the decoder set table info to the event
type tableInfoAccessor struct {
	memo            map[accessKey]*commonType.TableInfo
	blockedTableIDs map[accessKey]map[int64]struct{}
}

func NewTableInfoAccessor() *tableInfoAccessor {
	return &tableInfoAccessor{
		memo:            make(map[accessKey]*commonType.TableInfo),
		blockedTableIDs: make(map[accessKey]map[int64]struct{}),
	}
}

func (a *tableInfoAccessor) Get(schema, table string) (*commonType.TableInfo, bool) {
	key := accessKey{schema, table}
	tableInfo, ok := a.memo[key]
	return tableInfo, ok
}

func (a *tableInfoAccessor) GetBlockedTables(schema, table string) []int64 {
	key := accessKey{schema, table}
	blocked := a.blockedTableIDs[key]
	result := make([]int64, 0, len(blocked))
	for item := range blocked {
		result = append(result, item)
	}
	return result
}

func (a *tableInfoAccessor) AddBlockTableID(schema string, table string, physicalTableID int64) {
	key := accessKey{schema, table}
	if _, ok := a.blockedTableIDs[key]; !ok {
		a.blockedTableIDs[key] = make(map[int64]struct{})
	}

	if _, exists := a.blockedTableIDs[key][physicalTableID]; !exists {
		a.blockedTableIDs[key][physicalTableID] = struct{}{}
		log.Info("add table id to blocked list",
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", physicalTableID))
	}
}

func (a *tableInfoAccessor) Add(schema, table string, tableInfo *commonType.TableInfo) {
	key := accessKey{schema, table}
	if _, ok := a.memo[key]; !ok {
		a.memo[key] = tableInfo
		log.Info("add table info to cache", zap.String("schema", schema), zap.String("table", table))
	}
}

func (a *tableInfoAccessor) Remove(schema, table string) {
	key := accessKey{schema, table}
	delete(a.memo, key)
}

func (a *tableInfoAccessor) Clean() {
	clear(a.memo)
	clear(a.blockedTableIDs)
}

// FakeTableIDAllocator is a fake table id allocator
type FakeTableIDAllocator struct {
	tableIDs       map[accessKey]int64
	currentTableID int64
}

// NewFakeTableIDAllocator creates a new FakeTableIDAllocator
func NewFakeTableIDAllocator() *FakeTableIDAllocator {
	return &FakeTableIDAllocator{
		tableIDs: make(map[accessKey]int64),
	}
}

func (g *FakeTableIDAllocator) allocateByKey(key accessKey) int64 {
	if tableID, ok := g.tableIDs[key]; ok {
		return tableID
	}
	g.currentTableID++
	g.tableIDs[key] = g.currentTableID
	return g.currentTableID
}

// AllocateTableID allocates a table id
func (g *FakeTableIDAllocator) AllocateTableID(schema, table string) int64 {
	key := accessKey{schema, table}
	return g.allocateByKey(key)
}

func (g *FakeTableIDAllocator) Clean() {
	g.currentTableID = 0
	clear(g.tableIDs)
}
