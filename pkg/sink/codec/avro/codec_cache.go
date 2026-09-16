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

package avro

import (
	"context"
	"sync"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/schemamanager"
)

type codecCacheKey struct {
	schemaName     string
	schemaIdentity string
}

type codecCacheEntry struct {
	schemaVersion uint64
	codec         *goavro.Codec
	header        []byte
}

// CodecCache caches Avro codecs and their schema registry wire headers.
type CodecCache struct {
	schemaM schemamanager.SchemaManager

	mu      sync.RWMutex
	entries map[codecCacheKey]*codecCacheEntry
}

// NewCodecCache creates an Avro codec cache backed by a schema manager.
func NewCodecCache(schemaM schemamanager.SchemaManager) *CodecCache {
	return &CodecCache{
		schemaM: schemaM,
		entries: make(map[codecCacheKey]*codecCacheEntry),
	}
}

// GetOrRegister returns a cached codec or generates and registers its schema.
func (c *CodecCache) GetOrRegister(
	ctx context.Context,
	schemaName string,
	schemaIdentity string,
	schemaVersion uint64,
	schemaGen func() (string, error),
) (*goavro.Codec, []byte, error) {
	key := codecCacheKey{schemaName: schemaName, schemaIdentity: schemaIdentity}
	if entry, ok := c.load(key, schemaVersion); ok {
		return entry.codec, entry.header, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok && entry.schemaVersion == schemaVersion {
		return entry.codec, entry.header, nil
	}

	schemaDefinition, err := schemaGen()
	if err != nil {
		return nil, nil, err
	}
	codec, err := GenCodec(schemaDefinition)
	if err != nil {
		return nil, nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}
	header, err := c.schemaM.GetCachedOrRegister(
		ctx, schemaName, schemaIdentity, schemaVersion, schemaDefinition)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	entry := &codecCacheEntry{
		schemaVersion: schemaVersion,
		codec:         codec,
		header:        header,
	}
	c.entries[key] = entry
	return entry.codec, entry.header, nil
}

func (c *CodecCache) load(key codecCacheKey, schemaVersion uint64) (*codecCacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[key]
	return entry, ok && entry.schemaVersion == schemaVersion
}
