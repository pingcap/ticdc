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
	"strconv"

	"github.com/cockroachdb/pebble"
)

const (
	eventStoreMinCRTsTableProperty = "event-store-min-crts"
	eventStoreMaxCRTsTableProperty = "event-store-max-crts"
	eventStoreCRTsCollectorName    = "event-store-crts-collector"
)

type eventStoreCRTsCollector struct {
	minTs uint64
	maxTs uint64
	hasTs bool
}

func newEventStoreCRTsCollector() pebble.TablePropertyCollector {
	return &eventStoreCRTsCollector{}
}

func (c *eventStoreCRTsCollector) Add(key pebble.InternalKey, _ []byte) error {
	// Event store DeleteRange is GC-only: it removes data that should already be
	// below the future scan range. Do not widen table properties with the range
	// tombstone end key. For example, a cleanup tombstone [CRTs=100, CRTs=1000)
	// would make this cleanup-only SST overlap scans like [500,600].
	c.recordEncodedKey(key.UserKey)
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
