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
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
)

// TableInfoProvider keeps the TableInfo of every table version. Decoders may
// share one provider: a decoder that lags behind still reads the version its
// message asks for, and versions are released once the watermark passes them.
// Tables are keyed by their original name, so T and t are different tables.
type TableInfoProvider struct {
	mu                sync.RWMutex
	tables            map[accessKey]*tableVersions
	invalidations     []uint64
	changed           chan struct{}
	releasedWatermark atomic.Uint64
	allocator         *tableIDAllocator
}

// NewTableInfoProvider creates an empty versioned provider.
func NewTableInfoProvider() *TableInfoProvider {
	return &TableInfoProvider{
		tables:    make(map[accessKey]*tableVersions),
		allocator: newTableIDAllocator(),
	}
}

// ReleasedWatermark returns the highest watermark passed to ReleaseBefore.
func (c *TableInfoProvider) ReleasedWatermark() uint64 {
	return c.releasedWatermark.Load()
}

// WriteTableInfo stores the table info under its own version.
func (c *TableInfoProvider) WriteTableInfo(info *common.TableInfo) {
	if info == nil {
		return
	}
	c.WriteTableInfoAt(info, info.GetUpdateTS())
}

// WriteTableInfoAt stores the table info under its own version while ordering
// version retention by effectiveTs.
func (c *TableInfoProvider) WriteTableInfoAt(info *common.TableInfo, effectiveTs uint64) {
	if info == nil {
		return
	}
	if effectiveTs == 0 {
		effectiveTs = info.GetUpdateTS()
	}
	c.writeVersion(info.TableName.Schema, info.TableName.Table, info.GetUpdateTS(), effectiveTs, info)
}

// Write stores the table info under the version the message belongs to.
func (c *TableInfoProvider) Write(schema, table string, version uint64, info *common.TableInfo) {
	c.writeVersion(schema, table, version, version, info)
}

func (c *TableInfoProvider) writeVersion(schema, table string, version, effectiveTs uint64, info *common.TableInfo) {
	if schema == "" || table == "" || info == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.table(schema, table).set(version, effectiveTs, info)
	c.notify()
}

// Read returns the table info of exactly the requested version.
func (c *TableInfoProvider) Read(schema, table string, version uint64) *common.TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tables[accessKey{schema, table}].get(version)
}

// ReadAt returns the table info in effect for the commit ts, together with the
// version it belongs to. A row at a DDL commit ts still reads the previous
// version: it is flushed before that DDL.
func (c *TableInfoProvider) ReadAt(schema, table string, commitTs uint64) (uint64, *common.TableInfo) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	version, info := c.tables[accessKey{schema, table}].at(commitTs)
	if invalidated := c.lastInvalidation(commitTs); invalidated > version {
		return invalidated, nil
	}
	return version, info
}

// lastInvalidation returns the newest commit ts of a DDL that invalidated the
// cached table info, below commitTs. Rows before it still read the cached
// version: they are flushed before that DDL.
func (c *TableInfoProvider) lastInvalidation(commitTs uint64) uint64 {
	i := sort.Search(len(c.invalidations), func(i int) bool {
		return c.invalidations[i] >= commitTs
	})
	if i == 0 {
		return 0
	}
	return c.invalidations[i-1]
}

// AddDDL records the commit ts of a DDL, so a row can be resolved to the table
// info in effect when it was written.
func (c *TableInfoProvider) AddDDL(schema, table string, ts uint64) {
	if schema == "" || table == "" || ts == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.table(schema, table).set(ts, ts, nil)
}

// InvalidateAll records a DDL after which every cached table info is rebuilt:
// such a DDL moves the layout between names, so a name may point to another
// table than it did before.
func (c *TableInfoProvider) InvalidateAll(ts uint64) {
	if ts == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	i, exists := slices.BinarySearch(c.invalidations, ts)
	if !exists {
		c.invalidations = slices.Insert(c.invalidations, i, ts)
	}
}

// RecordDDL updates cached table versions for a decoded DDL event.
// DDLs that move layouts between names invalidate all cached table info;
// other DDLs invalidate only the affected table names.
func (c *TableInfoProvider) RecordDDL(ddl *commonEvent.DDLEvent) {
	if c == nil || ddl == nil {
		return
	}
	ts := ddl.GetCommitTs()
	switch timodel.ActionType(ddl.Type) {
	case timodel.ActionRenameTable, timodel.ActionRenameTables, timodel.ActionExchangeTablePartition:
		c.InvalidateAll(ts)
	default:
		c.AddDDL(ddl.SchemaName, ddl.TableName, ts)
		c.AddDDL(ddl.ExtraSchemaName, ddl.ExtraTableName, ts)
	}
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
	if watermark > c.releasedWatermark.Load() {
		c.releasedWatermark.Store(watermark)
	}
	for key, table := range c.tables {
		table.releaseBefore(watermark)
		if len(table.entries) == 0 {
			delete(c.tables, key)
		}
	}
	i := sort.Search(len(c.invalidations), func(i int) bool {
		return c.invalidations[i] > watermark
	})
	if i > 1 {
		c.invalidations = c.invalidations[i-1:]
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

func (c *TableInfoProvider) notify() {
	if c.changed != nil {
		close(c.changed)
		c.changed = nil
	}
}

func (c *TableInfoProvider) table(schema, table string) *tableVersions {
	key := accessKey{schema, table}
	versions := c.tables[key]
	if versions == nil {
		versions = new(tableVersions)
		c.tables[key] = versions
	}
	return versions
}

// AllocateTableID allocates a physical table ID scoped to this provider.
func (c *TableInfoProvider) AllocateTableID(schema, table string) int64 {
	return c.allocator.Allocate(schema, table)
}

// AddBlockTableID registers a physical table ID as blocked by DDLs on the table.
func (c *TableInfoProvider) AddBlockTableID(schema, table string, physicalTableID int64) {
	c.allocator.AddBlockTableID(schema, table, physicalTableID)
}

// GetBlockedTables returns the physical table IDs blocked by DDLs on the table.
func (c *TableInfoProvider) GetBlockedTables(schema, table string) []int64 {
	return c.allocator.GetBlockedTables(schema, table)
}

// tableVersions keeps the versions of one table in ascending order. A table has
// a handful of them, so a binary search is enough and stays allocation free.
type tableVersions struct {
	entries []entry
}

type entry struct {
	version     uint64
	effectiveTs uint64
	info        *common.TableInfo
}

// at returns the version in effect at the commit ts: the newest one below it.
// A version without table info is a boundary the decoder has not filled in yet.
func (t *tableVersions) at(commitTs uint64) (uint64, *common.TableInfo) {
	if t == nil {
		return 0, nil
	}
	i := sort.Search(len(t.entries), func(i int) bool {
		return t.entries[i].version >= commitTs
	})
	if i == 0 {
		return 0, nil
	}
	entry := t.entries[i-1]
	return entry.version, entry.info
}

func (t *tableVersions) get(version uint64) *common.TableInfo {
	if t == nil {
		return nil
	}
	i := sort.Search(len(t.entries), func(i int) bool {
		return t.entries[i].version >= version
	})
	if i == len(t.entries) || t.entries[i].version != version {
		return nil
	}
	return t.entries[i].info
}

func (t *tableVersions) set(version, effectiveTs uint64, info *common.TableInfo) {
	i := sort.Search(len(t.entries), func(i int) bool {
		return t.entries[i].version >= version
	})
	if i < len(t.entries) && t.entries[i].version == version {
		t.entries[i].effectiveTs = max(t.entries[i].effectiveTs, effectiveTs)
		if info != nil {
			t.entries[i].info = info
		}
		return
	}
	t.entries = slices.Insert(t.entries, i, entry{version: version, effectiveTs: effectiveTs, info: info})
}

func (t *tableVersions) releaseBefore(watermark uint64) {
	var covered int
	var floorEffectiveTs uint64
	for _, item := range t.entries {
		if item.effectiveTs <= watermark {
			covered++
			floorEffectiveTs = max(floorEffectiveTs, item.effectiveTs)
		}
	}
	if covered <= 1 {
		return
	}
	kept := 0
	for _, item := range t.entries {
		if item.effectiveTs >= floorEffectiveTs {
			t.entries[kept] = item
			kept++
		}
	}
	clear(t.entries[kept:])
	t.entries = t.entries[:kept]
}
