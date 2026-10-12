// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"sync"

	"github.com/pingcap/ticdc/pkg/common"
)

type schemaUsage struct {
	memory *memoryUsage
	mu     sync.Mutex
	tables map[*common.TableInfo]*schemaMemory
}

type schemaMemory struct {
	refs  int
	bytes int64
}

// Count shared metadata once across pipeline events and reader/decoder caches.
func (s *schemaUsage) retain(table *common.TableInfo) {
	if table == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if schema := s.tables[table]; schema != nil {
		schema.refs++
		return
	}
	// The budget is an estimate. Serializing metadata here allocates large JSON
	// buffers and repeats whenever the last pipeline reference is released.
	bytes := int64(1024 + len(table.GetSchemaName()) + len(table.GetTableName()))
	for _, column := range table.GetColumns() {
		bytes += int64(512 + len(column.Name.O) + len(column.Comment) + len(column.GeneratedExprString))
		for _, value := range column.FieldType.GetElems() {
			bytes += int64(16 + len(value))
		}
		if value, ok := column.DefaultValue.(string); ok {
			bytes += int64(len(value))
		}
	}
	for _, index := range table.GetIndices() {
		bytes += int64(256 + len(index.Name.O) + len(index.Columns)*64)
	}
	s.memory.reserve(bytes)
	if s.tables == nil {
		s.tables = make(map[*common.TableInfo]*schemaMemory)
	}
	s.tables[table] = &schemaMemory{
		refs:  1,
		bytes: bytes,
	}
}

func (s *schemaUsage) release(table *common.TableInfo) {
	if table == nil {
		return
	}
	s.mu.Lock()
	schema := s.tables[table]
	schema.refs--
	var bytes int64
	if schema.refs == 0 {
		bytes = schema.bytes
		delete(s.tables, table)
	}
	s.mu.Unlock()
	s.memory.release(bytes)
}
