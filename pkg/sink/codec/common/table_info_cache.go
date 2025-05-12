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
	"fmt"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

type accessKey struct {
	schema string
	table  string
}

// TableInfoAccessor provide table information, to helper
// the decoder set table info to the event
type TableInfoAccessor struct {
	memo map[accessKey]*commonType.TableInfo
}

func NewTableInfoAccessor() *TableInfoAccessor {
	return &TableInfoAccessor{
		memo: make(map[accessKey]*commonType.TableInfo),
	}
}

func (a *TableInfoAccessor) Get(schema, table string) (*commonType.TableInfo, bool) {
	key := accessKey{schema, table}
	tableInfo, ok := a.memo[key]
	return tableInfo, ok
}

func (a *TableInfoAccessor) Add(schema, table string, tableInfo *commonType.TableInfo) {
	key := accessKey{
		schema: schema,
		table:  table,
	}
	if _, ok := a.memo[key]; !ok {
		a.memo[key] = tableInfo
	}
	log.Info("add table info to cache", zap.String("schema", schema), zap.String("table", table),
		zap.Int64("tableID", tableInfo.TableName.TableID))
}

func (a *TableInfoAccessor) Remove(schema, table string) {
	key := accessKey{
		schema: schema,
		table:  table,
	}
	delete(a.memo, key)
}

func (a *TableInfoAccessor) Clean() {
	clear(a.memo)
}

// FakeTableIDAllocator is a fake table id allocator
type FakeTableIDAllocator struct {
	tableIDs       map[string]int64
	currentTableID int64
}

// NewFakeTableIDAllocator creates a new FakeTableIDAllocator
func NewFakeTableIDAllocator() *FakeTableIDAllocator {
	return &FakeTableIDAllocator{
		tableIDs: make(map[string]int64),
	}
}

func (g *FakeTableIDAllocator) allocateByKey(key string) int64 {
	if tableID, ok := g.tableIDs[key]; ok {
		return tableID
	}
	g.currentTableID++
	g.tableIDs[key] = g.currentTableID
	return g.currentTableID
}

// AllocateTableID allocates a table id
func (g *FakeTableIDAllocator) AllocateTableID(schema, table string) int64 {
	key := fmt.Sprintf("`%s`.`%s`", commonType.EscapeName(schema), commonType.EscapeName(table))
	return g.allocateByKey(key)
}

// AllocatePartitionID allocates a partition id
func (g *FakeTableIDAllocator) AllocatePartitionID(schema, table, name string) int64 {
	key := fmt.Sprintf("`%s`.`%s`.`%s`", commonType.EscapeName(schema), commonType.EscapeName(table), commonType.EscapeName(name))
	return g.allocateByKey(key)
}

func (g *FakeTableIDAllocator) Clean() {
	g.currentTableID = 0
	clear(g.tableIDs)
}
