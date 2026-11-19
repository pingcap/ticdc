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
	"slices"
	"sync"

	commonType "github.com/pingcap/ticdc/pkg/common"
)

// TableInfoProvider keeps the TableInfo of every table version. Decoders share
// one provider: a decoder that lags behind still reads the version its message
// asks for, and versions are released once the watermark passes them.
type TableInfoProvider struct {
	mu      sync.RWMutex
	tables  map[accessKey]*tableVersions
	changed chan struct{}
}

type entry struct {
	version uint64
	info    *commonType.TableInfo
}

// NewTableInfoProvider creates an empty versioned provider.
func NewTableInfoProvider() *TableInfoProvider {
	return &TableInfoProvider{
		tables: make(map[accessKey]*tableVersions),
	}
}

// WriteInfo stores the table info under its own version.
func (c *TableInfoProvider) WriteInfo(info *commonType.TableInfo) {
	if info == nil {
		return
	}
	c.Write(info.TableName.Schema, info.TableName.Table, info.GetUpdateTS(), info)
}

// Write stores the table info under the version the message belongs to.
func (c *TableInfoProvider) Write(schema, table string, version uint64, info *commonType.TableInfo) {
	if schema == "" || table == "" || info == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.table(schema, table).set(version, info)
	c.notify()
}

// Read returns the table info of exactly the requested version.
func (c *TableInfoProvider) Read(schema, table string, version uint64) *commonType.TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tables[newAccessKey(schema, table)].get(version)
}

// ReadAt returns the table info in effect for the commit ts, together with the
// version it belongs to. A row at a DDL commit ts still reads the previous
// version: it is flushed before that DDL.
func (c *TableInfoProvider) ReadAt(schema, table string, commitTs uint64) (uint64, *commonType.TableInfo) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tables[newAccessKey(schema, table)].at(commitTs)
}

// AddDDL records the commit ts of a DDL, so a row can be resolved to the table
// info in effect when it was written.
func (c *TableInfoProvider) AddDDL(schema, table string, ts uint64) {
	if schema == "" || table == "" || ts == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.table(schema, table).set(ts, nil)
}

// ReleaseBefore drops the versions the watermark has passed. Every decoder has
// consumed the messages carrying them; the newest dropped entry stays as the
// floor later versions are resolved against.
func (c *TableInfoProvider) ReleaseBefore(watermark uint64) {
	if watermark == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, table := range c.tables {
		table.releaseBefore(watermark)
		if len(table.entries) == 0 {
			delete(c.tables, key)
		}
	}
}

// Changes returns a channel closed on the next write.
func (c *TableInfoProvider) Changes() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.changed == nil {
		c.changed = make(chan struct{})
	}
	return c.changed
}

func (c *TableInfoProvider) table(schema, table string) *tableVersions {
	key := newAccessKey(schema, table)
	versions := c.tables[key]
	if versions == nil {
		versions = new(tableVersions)
		c.tables[key] = versions
	}
	return versions
}

func (c *TableInfoProvider) notify() {
	if c.changed != nil {
		close(c.changed)
		c.changed = nil
	}
}

// tableVersions holds the versions of one table in ascending order. A table has
// only a few of them, so the lookups walk backwards: the newest version answers
// almost every call.
type tableVersions struct {
	entries []entry
}

func (t *tableVersions) at(commitTs uint64) (uint64, *commonType.TableInfo) {
	if t == nil {
		return 0, nil
	}
	for i := len(t.entries) - 1; i >= 0; i-- {
		if t.entries[i].version < commitTs {
			return t.entries[i].version, t.entries[i].info
		}
	}
	return 0, nil
}

func (t *tableVersions) get(version uint64) *commonType.TableInfo {
	if t == nil {
		return nil
	}
	for i := len(t.entries) - 1; i >= 0; i-- {
		if t.entries[i].version == version {
			return t.entries[i].info
		}
		if t.entries[i].version < version {
			return nil
		}
	}
	return nil
}

func (t *tableVersions) set(version uint64, info *commonType.TableInfo) {
	for i := len(t.entries) - 1; i >= 0; i-- {
		switch {
		case t.entries[i].version == version:
			if info != nil {
				t.entries[i].info = info
			}
			return
		case t.entries[i].version < version:
			t.entries = slices.Insert(t.entries, i+1, entry{version: version, info: info})
			return
		}
	}
	t.entries = slices.Insert(t.entries, 0, entry{version: version, info: info})
}

func (t *tableVersions) releaseBefore(watermark uint64) {
	i := 0
	for i < len(t.entries) && t.entries[i].version <= watermark {
		i++
	}
	if i > 1 {
		clear(t.entries[:i-1])
		t.entries = t.entries[i-1:]
	}
}
