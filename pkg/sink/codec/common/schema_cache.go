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

// TableInfoCache keeps the TableInfo of every table version. Decoders share
// one cache: a decoder that lags behind can still read the version its message
// asks for, and versions are released once the watermark passes them.
type TableInfoCache struct {
	mu      sync.RWMutex
	tables  map[accessKey]map[uint64]any
	changed chan struct{}
}

// NewTableInfoCache creates an empty versioned TableInfo cache.
func NewTableInfoCache() *TableInfoCache {
	return &TableInfoCache{
		tables: make(map[accessKey]map[uint64]any),
	}
}

// Write stores the TableInfo under its version. A message carrying an older
// version later overwrites nothing, it adds a version of its own.
func (c *TableInfoCache) Write(info *commonType.TableInfo) {
	if info == nil {
		return
	}
	c.WriteValue(info.TableName.Schema, info.TableName.Table, info.GetUpdateTS(), info)
}

// Read returns the TableInfo of exactly the requested version.
func (c *TableInfoCache) Read(schema, table string, version uint64) *commonType.TableInfo {
	info, _ := c.ReadValue(schema, table, version).(*commonType.TableInfo)
	return info
}

// WriteValue stores a decoder's entry under the table version.
func (c *TableInfoCache) WriteValue(schema, table string, version uint64, value any) {
	if schema == "" || table == "" || value == nil {
		return
	}
	key := newAccessKey(schema, table)

	c.mu.Lock()
	defer c.mu.Unlock()
	versions := c.tables[key]
	if versions == nil {
		versions = make(map[uint64]any)
		c.tables[key] = versions
	}
	versions[version] = value
	if c.changed != nil {
		close(c.changed)
		c.changed = nil
	}
}

// ReadValue returns the entry of exactly the requested version.
func (c *TableInfoCache) ReadValue(schema, table string, version uint64) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tables[newAccessKey(schema, table)][version]
}

// ReleaseBefore drops the versions the watermark has passed. Every decoder has
// consumed the messages carrying them, so no decoder can ask for them again.
func (c *TableInfoCache) ReleaseBefore(watermark uint64) {
	if watermark == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, versions := range c.tables {
		for version := range versions {
			if version != 0 && version <= watermark {
				delete(versions, version)
			}
		}
		if len(versions) == 0 {
			delete(c.tables, key)
		}
	}
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
