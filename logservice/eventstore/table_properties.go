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

package eventstore

import (
	"encoding/binary"
	"strconv"

	"github.com/cockroachdb/pebble"
)

const (
	eventStoreMinCRTsTableProperty = "event-store-min-crts"
	eventStoreMaxCRTsTableProperty = "event-store-max-crts"
	eventStoreCRTsCollectorName    = "event-store-crts-collector"

	encodedKeyCRTsOffset = 16
	encodedKeyCRTsEnd    = encodedKeyCRTsOffset + 8
)

type eventStoreCRTsCollector struct {
	minTs uint64
	maxTs uint64
	hasTs bool
}

func newEventStoreCRTsCollector() pebble.TablePropertyCollector {
	return &eventStoreCRTsCollector{}
}

func (c *eventStoreCRTsCollector) Add(key pebble.InternalKey, value []byte) error {
	c.recordEncodedKey(key.UserKey)
	if key.Kind() == pebble.InternalKeyKindRangeDelete {
		c.recordEncodedKey(value)
	}
	return nil
}

func (c *eventStoreCRTsCollector) Finish(userProps map[string]string) error {
	if !c.hasTs {
		return nil
	}
	userProps[eventStoreMinCRTsTableProperty] = strconv.FormatUint(c.minTs, 10)
	userProps[eventStoreMaxCRTsTableProperty] = strconv.FormatUint(c.maxTs, 10)
	return nil
}

func (c *eventStoreCRTsCollector) Name() string {
	return eventStoreCRTsCollectorName
}

func (c *eventStoreCRTsCollector) recordEncodedKey(key []byte) {
	crts, ok := decodeCRTsFromEncodedKey(key)
	if !ok {
		return
	}
	if !c.hasTs || crts < c.minTs {
		c.minTs = crts
	}
	if !c.hasTs || crts > c.maxTs {
		c.maxTs = crts
	}
	c.hasTs = true
}

func decodeCRTsFromEncodedKey(key []byte) (uint64, bool) {
	if len(key) < encodedKeyCRTsEnd {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[encodedKeyCRTsOffset:encodedKeyCRTsEnd]), true
}

func newEventStoreTableFilter(lowerTs uint64, upperTs uint64) func(map[string]string) bool {
	return func(userProps map[string]string) bool {
		minTs, ok := parseEventStoreCRTsTableProperty(userProps, eventStoreMinCRTsTableProperty)
		if !ok {
			return true
		}
		maxTs, ok := parseEventStoreCRTsTableProperty(userProps, eventStoreMaxCRTsTableProperty)
		if !ok {
			return true
		}
		return maxTs >= lowerTs && minTs <= upperTs
	}
}

func parseEventStoreCRTsTableProperty(userProps map[string]string, key string) (uint64, bool) {
	value, ok := userProps[key]
	if !ok {
		return 0, false
	}
	ts, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return ts, true
}
