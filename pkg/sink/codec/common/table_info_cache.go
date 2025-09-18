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
	"strings"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type blockedTableProvider interface {
	GetBlockedTables(schema, table string) []int64
}

type accessKey struct {
	schema string
	table  string
}

// newAccessKey return accessKey with lower case
func newAccessKey(schema, table string) accessKey {
	return accessKey{
		strings.ToLower(schema),
		strings.ToLower(table),
	}
}

// tableIDAllocator is a fake table id allocator
type tableIDAllocator struct {
	tableIDs        map[accessKey]int64
	currentTableID  int64
	blockedTableIDs map[accessKey]map[int64]struct{}
}

// NewTableIDAllocator creates a new tableIDAllocator
func NewTableIDAllocator() *tableIDAllocator {
	return &tableIDAllocator{
		tableIDs:        make(map[accessKey]int64),
		blockedTableIDs: make(map[accessKey]map[int64]struct{}),
	}
}

func (a *tableIDAllocator) allocateByKey(key accessKey) int64 {
	if tableID, ok := a.tableIDs[key]; ok {
		return tableID
	}
	a.currentTableID++
	a.tableIDs[key] = a.currentTableID
	return a.currentTableID
}

// Allocate allocates a table id
func (a *tableIDAllocator) Allocate(schema, table string) int64 {
	key := newAccessKey(schema, table)
	return a.allocateByKey(key)
}

func (a *tableIDAllocator) GetBlockedTables(schema, table string) []int64 {
	key := newAccessKey(schema, table)
	blocked := a.blockedTableIDs[key]
	result := make([]int64, 0, len(blocked))
	for item := range blocked {
		result = append(result, item)
	}
	return result
}

func (a *tableIDAllocator) AddBlockTableID(schema string, table string, physicalTableID int64) {
	key := newAccessKey(schema, table)
	if _, ok := a.blockedTableIDs[key]; !ok {
		a.blockedTableIDs[key] = make(map[int64]struct{})
	}

	if _, exists := a.blockedTableIDs[key][physicalTableID]; !exists {
		a.blockedTableIDs[key][physicalTableID] = struct{}{}
		log.Info("add table id to blocked list",
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", physicalTableID))
	}
}

func (a *tableIDAllocator) Clean() {
	a.currentTableID = 0
	clear(a.tableIDs)
	clear(a.blockedTableIDs)
}

type PartitionTableAccessor struct {
	memo map[accessKey]struct{}
}

func NewPartitionTableAccessor() *PartitionTableAccessor {
	return &PartitionTableAccessor{
		memo: make(map[accessKey]struct{}),
	}
}

func (m *PartitionTableAccessor) Add(schema, table string) {
	key := newAccessKey(schema, table)
	_, ok := m.memo[key]
	if ok {
		return
	}
	m.memo[key] = struct{}{}
	log.Info("add partition table to the accessor", zap.String("schema", schema), zap.String("table", table))
}

func (m *PartitionTableAccessor) IsPartitionTable(schema, table string) bool {
	key := newAccessKey(schema, table)
	_, ok := m.memo[key]
	return ok
}
