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
	"sort"
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
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		deleteTicker := time.NewTicker(50 * time.Millisecond)
		defer deleteTicker.Stop()

		const deleteInfoLogInterval = 10 * time.Minute
		lastInfoLog := time.Now()
		windowStart := lastInfoLog
		var windowBatchCount int
		var windowRangeCount int
		var windowTotalDuration time.Duration
		var windowMaxBatchDuration time.Duration

		logDeleteRangeStats := func(now time.Time) {
			avgRangesPerBatch := 0.0
			if windowBatchCount > 0 {
				avgRangesPerBatch = float64(windowRangeCount) / float64(windowBatchCount)
			}
			avgBatchDuration := time.Duration(0)
			if windowBatchCount > 0 {
				avgBatchDuration = windowTotalDuration / time.Duration(windowBatchCount)
			}

			log.Info("gc manager delete range progress",
				zap.Duration("interval", now.Sub(windowStart)),
				zap.Int("batchCount", windowBatchCount),
				zap.Int("deletedRangeCount", windowRangeCount),
				zap.Float64("avgRangesPerBatch", avgRangesPerBatch),
				zap.Duration("avgBatchDuration", avgBatchDuration),
				zap.Duration("maxBatchDuration", windowMaxBatchDuration))

			lastInfoLog = now
			windowStart = now
			windowBatchCount = 0
			windowRangeCount = 0
			windowTotalDuration = 0
			windowMaxBatchDuration = 0
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-deleteTicker.C:
				ranges := d.fetchAllGCItems()
				if len(ranges) == 0 {
					if time.Since(lastInfoLog) >= deleteInfoLogInterval {
						logDeleteRangeStats(time.Now())
					}
					continue
				}

				metrics.EventStoreDeleteRangeFetchedCount.Add(float64(len(ranges)))

				originalRangeCount := len(ranges)
				ranges, mergedCount := mergeDeleteRanges(ranges)
				if mergedCount > 0 {
					log.Debug("gc manager coalesced delete ranges",
						zap.Int("fetchedRangeCount", originalRangeCount),
						zap.Int("deleteOpCount", len(ranges)))
				}

				startTime := time.Now()
				d.doGCJob(ranges)
				d.updateCompactRanges(ranges)
				metrics.EventStoreDeleteRangeCount.Add(float64(len(ranges)))

				duration := time.Since(startTime)
				windowBatchCount++
				windowRangeCount += len(ranges)
				windowTotalDuration += duration
				if duration > windowMaxBatchDuration {
					windowMaxBatchDuration = duration
				}
				log.Debug("gc manager deleted ranges",
					zap.Int("batchRangeCount", len(ranges)),
					zap.Duration("duration", duration))

				if time.Since(lastInfoLog) >= deleteInfoLogInterval {
					logDeleteRangeStats(time.Now())
				}
			}
		}
	}()

	go func() {
		defer wg.Done()

		compactTicker := time.NewTicker(10 * time.Minute)
		defer compactTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-compactTicker.C:
				// it seems pebble doesn't compact cold range(no data write),
				// so we do a manual compaction periodically.
				d.doCompaction()
			}
		}
	}()

	<-ctx.Done()
	wg.Wait()
	return nil
}

func (d *gcManager) doGCJob(ranges []gcRangeItem) {
	for _, r := range ranges {
		db := d.dbs[r.dbIndex]
		if err := d.deleteDataRange(db, r.uniqueKeyID, r.tableID, r.startTs, r.endTs); err != nil {
			log.Warn("gc manager failed to delete data range", zap.Error(err))
		}
	}
}

// mergeDeleteRanges merges delete ranges for the same (dbIndex, uniqueKeyID, tableID) when they are
// contiguous or overlapping. It is used as a best-effort mitigation for rare cases where the delete
// goroutine is blocked for a long time and ranges accumulate.
func mergeDeleteRanges(ranges []gcRangeItem) ([]gcRangeItem, int) {
	if len(ranges) < 2 {
		return ranges, 0
	}

	// Common case: at most one range per (dbIndex, uniqueKeyID, tableID), so no merge.
	seen := make(map[compactItemKey]struct{}, len(ranges))
	hasDuplicateKey := false
	for _, r := range ranges {
		key := compactItemKey{dbIndex: r.dbIndex, uniqueKeyID: r.uniqueKeyID, tableID: r.tableID}
		if _, ok := seen[key]; ok {
			hasDuplicateKey = true
			break
		}
		seen[key] = struct{}{}
	}
	if !hasDuplicateKey {
		return ranges, 0
	}

	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].dbIndex != ranges[j].dbIndex {
			return ranges[i].dbIndex < ranges[j].dbIndex
		}
		if ranges[i].uniqueKeyID != ranges[j].uniqueKeyID {
			return ranges[i].uniqueKeyID < ranges[j].uniqueKeyID
		}
		if ranges[i].tableID != ranges[j].tableID {
			return ranges[i].tableID < ranges[j].tableID
		}
		if ranges[i].startTs != ranges[j].startTs {
			return ranges[i].startTs < ranges[j].startTs
		}
		return ranges[i].endTs < ranges[j].endTs
	})

	originalCount := len(ranges)
	out := ranges[:0]
	cur := ranges[0]

	for _, r := range ranges[1:] {
		sameRangeKey := cur.dbIndex == r.dbIndex && cur.uniqueKeyID == r.uniqueKeyID && cur.tableID == r.tableID
		contiguousOrOverlapping := r.startTs <= cur.endTs
		if sameRangeKey && contiguousOrOverlapping {
			if r.endTs > cur.endTs {
				cur.endTs = r.endTs
			}
			continue
		}
		out = append(out, cur)
		cur = r
	}
	out = append(out, cur)
	return out, originalCount - len(out)
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

	startTime := time.Now()
	log.Info("gc manager compacting ranges", zap.Int("rangeCount", len(toCompact)))
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
	log.Info("gc manager compacting ranges done",
		zap.Int("rangeCount", len(toCompact)),
		zap.Duration("duration", time.Since(startTime)))
}
