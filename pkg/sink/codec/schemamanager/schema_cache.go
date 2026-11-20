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

package schemamanager

import (
	"sync"
)

type schemaCacheEntry struct {
	schemaVersion    uint64
	schemaID         SchemaID
	schemaDefinition string
	header           []byte
}

type schemaCacheKey struct {
	schemaName     string
	schemaIdentity string
}

type schemaCache struct {
	mu      sync.RWMutex
	entries map[schemaCacheKey]*schemaCacheEntry

	subjectLocks sync.Map
}

func newSchemaCache() *schemaCache {
	return &schemaCache{
		entries: make(map[schemaCacheKey]*schemaCacheEntry),
	}
}

func newEncodeCacheKey(schemaName, schemaIdentity string) schemaCacheKey {
	return schemaCacheKey{
		schemaName:     schemaName,
		schemaIdentity: schemaIdentity,
	}
}

func newDecodeCacheKey(schemaName string) schemaCacheKey {
	return schemaCacheKey{schemaName: schemaName}
}

func (c *schemaCache) load(key schemaCacheKey) (*schemaCacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[key]
	return entry, ok
}

func (c *schemaCache) loadByVersion(
	key schemaCacheKey, schemaVersion uint64,
) (*schemaCacheEntry, bool) {
	entry, ok := c.load(key)
	return entry, ok && entry.schemaVersion == schemaVersion
}

func (c *schemaCache) store(key schemaCacheKey, entry *schemaCacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = entry
}

func (c *schemaCache) getOrCreate(
	schemaName string,
	schemaIdentity string,
	schemaVersion uint64,
	create func() (*schemaCacheEntry, error),
) (*schemaCacheEntry, bool, error) {
	key := newEncodeCacheKey(schemaName, schemaIdentity)
	if entry, ok := c.loadByVersion(key, schemaVersion); ok {
		return entry, true, nil
	}

	unlock := c.lockSubject(schemaName)
	defer unlock()

	if entry, ok := c.loadByVersion(key, schemaVersion); ok {
		return entry, true, nil
	}

	entry, err := create()
	if err != nil {
		return nil, false, err
	}
	c.store(key, entry)
	return entry, false, nil
}

func (c *schemaCache) lockSubject(schemaName string) func() {
	value, _ := c.subjectLocks.LoadOrStore(schemaName, &sync.Mutex{})
	registerLock := value.(*sync.Mutex)
	registerLock.Lock()
	return registerLock.Unlock
}
