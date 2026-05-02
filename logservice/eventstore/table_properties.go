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
	"github.com/pingcap/ticdc/pkg/metrics"
)

// Pebble table properties are per-SST metadata generated when Pebble writes an
// SST file. Event store stores the txn commit ts range of each SST here, and
// passes a TableFilter during scans so Pebble can skip SST files that cannot
// contain events in the requested commit-ts range.

const (
	eventStoreMinTxnCommitTsProperty   = "event-store-min-txn-commit-ts"
	eventStoreMaxTxnCommitTsProperty   = "event-store-max-txn-commit-ts"
	eventStoreLogicalBytesProperty     = "event-store-logical-bytes"
	eventStoreTxnCommitTsCollectorName = "event-store-txn-commit-ts-collector"
)

type eventStoreTxnCommitTsCollector struct {
	minTs        uint64
	maxTs        uint64
	logicalBytes uint64
	hasTs        bool
}

func newEventStoreTxnCommitTsCollector() pebble.TablePropertyCollector {
	return &eventStoreTxnCommitTsCollector{}
}

func (c *eventStoreTxnCommitTsCollector) Add(key pebble.InternalKey, value []byte) error {
	// Event store DeleteRange is GC-only: it removes data that should already be
	// below the future scan range. Do not widen table properties with the range
	// tombstone end key. For example, a cleanup tombstone [commit-ts=100, commit-ts=1000)
	// would make this cleanup-only SST overlap scans like [500,600].
	c.recordEncodedKey(key.UserKey)
	c.logicalBytes += uint64(len(key.UserKey) + len(value))
	return nil
}

func (c *eventStoreTxnCommitTsCollector) Finish(userProps map[string]string) error {
	if !c.hasTs {
		return nil
	}
	userProps[eventStoreMinTxnCommitTsProperty] = strconv.FormatUint(c.minTs, 10)
	userProps[eventStoreMaxTxnCommitTsProperty] = strconv.FormatUint(c.maxTs, 10)
	userProps[eventStoreLogicalBytesProperty] = strconv.FormatUint(c.logicalBytes, 10)
	return nil
}

func (c *eventStoreTxnCommitTsCollector) Name() string {
	return eventStoreTxnCommitTsCollectorName
}

func (c *eventStoreTxnCommitTsCollector) recordEncodedKey(key []byte) {
	txnCommitTs, ok := decodeTxnCommitTsFromEncodedKey(key)
	if !ok {
		return
	}
	if !c.hasTs {
		c.minTs = txnCommitTs
		c.maxTs = txnCommitTs
		c.hasTs = true
		return
	}
	if txnCommitTs < c.minTs {
		c.minTs = txnCommitTs
	}
	if txnCommitTs > c.maxTs {
		c.maxTs = txnCommitTs
	}
}

func newEventStoreSSTFileFilter(lowerTs uint64, upperTs uint64) func(map[string]string) bool {
	return func(userProps map[string]string) bool {
		shouldScan := eventStoreSSTFileMayContainTxnCommitTs(userProps, lowerTs, upperTs)
		recordEventStoreSSTFileFilterMetrics(userProps, shouldScan)
		return shouldScan
	}
}

func eventStoreSSTFileMayContainTxnCommitTs(userProps map[string]string, lowerTs uint64, upperTs uint64) bool {
	minTs, ok := parseEventStoreUint64TableProperty(userProps, eventStoreMinTxnCommitTsProperty)
	if !ok {
		return true
	}
	maxTs, ok := parseEventStoreUint64TableProperty(userProps, eventStoreMaxTxnCommitTsProperty)
	if !ok {
		return true
	}
	if minTs > maxTs {
		// Corrupted or incompatible properties should not make Pebble skip data.
		return true
	}
	// Two inclusive ranges [minTs, maxTs] and [lowerTs, upperTs] overlap iff
	// each range starts before or at the other range's end. Equal boundaries are
	// included because commit-ts scan ranges are inclusive here.
	return maxTs >= lowerTs && minTs <= upperTs
}

func recordEventStoreSSTFileFilterMetrics(userProps map[string]string, shouldScan bool) {
	result := "scanned"
	if !shouldScan {
		result = "skipped"
	}
	metrics.EventStoreSSTFileFilterCount.WithLabelValues(result).Inc()
	if logicalBytes, ok := parseEventStoreUint64TableProperty(userProps, eventStoreLogicalBytesProperty); ok {
		metrics.EventStoreSSTFileFilterLogicalBytes.WithLabelValues(result).Add(float64(logicalBytes))
	}
}

func parseEventStoreUint64TableProperty(userProps map[string]string, key string) (uint64, bool) {
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
