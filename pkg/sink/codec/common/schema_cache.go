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
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"sync"

	commonType "github.com/pingcap/ticdc/pkg/common"
)

// TableInfoCache keeps the latest TableInfo of every table. Decoders share one
// cache: events of a table are ordered, so they only need the newest layout.
// A message carrying an older version reads nothing and waits for its DDL.
type TableInfoCache struct {
	mu      sync.RWMutex
	tables  map[accessKey]*commonType.TableInfo
	changed chan struct{}
}

// NewTableInfoCache creates an empty TableInfo cache.
func NewTableInfoCache() *TableInfoCache {
	return &TableInfoCache{
		tables: make(map[accessKey]*commonType.TableInfo),
	}
}

// Write stores the TableInfo. A stale version never replaces a newer one.
func (c *TableInfoCache) Write(info *commonType.TableInfo) {
	if info == nil || info.TableName.Schema == "" || info.TableName.Table == "" {
		return
	}
	key := newAccessKey(info.TableName.Schema, info.TableName.Table)

	c.mu.Lock()
	defer c.mu.Unlock()
	if current := c.tables[key]; current != nil && current.GetUpdateTS() >= info.GetUpdateTS() {
		return
	}
	c.tables[key] = info
	if c.changed != nil {
		close(c.changed)
		c.changed = nil
	}
}

// Read returns the TableInfo only when it matches the requested version.
func (c *TableInfoCache) Read(schema, table string, version uint64) *commonType.TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	info := c.tables[newAccessKey(schema, table)]
	if info == nil || info.GetUpdateTS() != version {
		return nil
	}
	return info
}

// Delete removes the TableInfo of the table.
func (c *TableInfoCache) Delete(schema, table string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.tables, newAccessKey(schema, table))
}

// Changes returns a channel closed on the next write.
func (c *TableInfoCache) Changes() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.changed == nil {
		c.changed = make(chan struct{})
	}
	return c.changed
}
