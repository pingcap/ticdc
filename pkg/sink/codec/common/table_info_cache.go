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
	"go.uber.org/zap"
)

type blockedTableProvider interface {
	GetBlockedTables(schema, table string) []int64
}

type accessKey struct {
	schema string
	table  string
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
	key := accessKey{schema, table}
	_, ok := m.memo[key]
	if ok {
		return
	}
	m.memo[key] = struct{}{}
	log.Info("add partition table to the accessor", zap.String("schema", schema), zap.String("table", table))
}

func (m *PartitionTableAccessor) IsPartitionTable(schema, table string) bool {
	key := accessKey{schema, table}
	_, ok := m.memo[key]
	return ok
}
