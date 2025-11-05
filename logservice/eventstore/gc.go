// Copyright 2025 PingCAP, Inc.
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
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

type (
	deleteDataRangeFunc  func(db *pebble.DB, uniqueKeyID uint64, tableID int64, startTs uint64, endTs uint64) error
	compactDataRangeFunc func(db *pebble.DB, uniqueKeyID uint64, tableID int64, startTs uint64, endTs uint64) error
)

type gcRangeItem struct {
	dbIndex     int
	uniqueKeyID uint64
	tableID     int64
	// TODO: startCommitTS may be not needed now(just use 0 for every delete range maybe ok),
	// but after split table range, it may be essential?
	startTs uint64
	endTs   uint64
}

type compactItemKey struct {
	dbIndex     int
	uniqueKeyID uint64
	tableID     int64
}

type compactState struct {
	endTs     uint64
	compacted bool
}

type gcManager struct {
	mu            sync.Mutex
	dbs           []*pebble.DB
	ranges        []gcRangeItem
	compactRanges map[compactItemKey]*compactState

	deleteDataRange  deleteDataRangeFunc
	compactDataRange compactDataRangeFunc
}

func newGCManager(
	dbs []*pebble.DB,
	deleteDataRange deleteDataRangeFunc,
	compactDataRange compactDataRangeFunc,
) *gcManager {
	return &gcManager{
		dbs:              dbs,
		compactRanges:    make(map[compactItemKey]*compactState),
		deleteDataRange:  deleteDataRange,
		compactDataRange: compactDataRange,
	}
}

// add an item to delete the data in range (startTS, endTS] for `tableID` with `uniqueID`.
func (d *gcManager) addGCItem(dbIndex int, uniqueKeyID uint64, tableID int64, startTS uint64, endTS uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ranges = append(d.ranges, gcRangeItem{
		dbIndex:     dbIndex,
		uniqueKeyID: uniqueKeyID,
		tableID:     tableID,
		startTs:     startTS,
		endTs:       endTS,
	})
}

func (d *gcManager) fetchAllGCItems() []gcRangeItem {
	d.mu.Lock()
	defer d.mu.Unlock()
	ranges := d.ranges
	d.ranges = nil
	return ranges
}

func (d *gcManager) run(ctx context.Context) error {
	deleteTicker := time.NewTicker(50 * time.Millisecond)
	defer deleteTicker.Stop()
	compactTicker := time.NewTicker(10 * time.Minute)
	defer compactTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-deleteTicker.C:
			ranges := d.fetchAllGCItems()
			if len(ranges) == 0 {
				continue
			}
			d.doGCJob(ranges)
			d.updateCompactRanges(ranges)
			metrics.EventStoreDeleteRangeCount.Add(float64(len(ranges)))
		case <-compactTicker.C:
			// it seems pebble doesn't compact cold range(no data write),
			// so we do a manual compaction periodically.
			d.doCompaction()
		}
	}
}

func (d *gcManager) doGCJob(ranges []gcRangeItem) {
	for _, r := range ranges {
		db := d.dbs[r.dbIndex]
		if err := d.deleteDataRange(db, r.uniqueKeyID, r.tableID, r.startTs, r.endTs); err != nil {
			log.Warn("gc manager failed to delete data range", zap.Error(err))
		}
	}
}

func (d *gcManager) updateCompactRanges(ranges []gcRangeItem) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, r := range ranges {
		key := compactItemKey{dbIndex: r.dbIndex, uniqueKeyID: r.uniqueKeyID, tableID: r.tableID}
		state, ok := d.compactRanges[key]
		if !ok {
			state = &compactState{}
			d.compactRanges[key] = state
		}
		if state.endTs < r.endTs {
			state.endTs = r.endTs
			state.compacted = false
		}
	}
}

func (d *gcManager) doCompaction() {
	toCompact := make(map[compactItemKey]uint64)
	d.mu.Lock()
	for key, state := range d.compactRanges {
		if !state.compacted {
			toCompact[key] = state.endTs
			state.compacted = true
		}
	}
	d.mu.Unlock()

	for key, endTs := range toCompact {
		db := d.dbs[key.dbIndex]
		if err := d.compactDataRange(db, key.uniqueKeyID, key.tableID, 0, endTs); err != nil {
			log.Warn("gc manager failed to compact data range",
				zap.Int("dbIndex", key.dbIndex),
				zap.Uint64("uniqueKeyID", key.uniqueKeyID),
				zap.Int64("tableID", key.tableID),
				zap.Uint64("endTs", endTs),
				zap.Error(err))
		}
	}
}
