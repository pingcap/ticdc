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
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type (
	deleteDataRangeFunc func(
		db *pebble.DB, uniqueKeyID uint64, tableID int64, startTxnCommitTs uint64, endTxnCommitTs uint64,
	) error
	compactDataRangeFunc func(
		db *pebble.DB, uniqueKeyID uint64, tableID int64, startTxnCommitTs uint64, endTxnCommitTs uint64,
	) error
)

type gcRangeItem struct {
	dbIndex          int
	uniqueKeyID      uint64
	tableID          int64
	startTxnCommitTs uint64
	endTxnCommitTs   uint64
}

// pendingDeleteRange tracks a coarse delete range for one (dbIndex, uniqueKeyID, tableID).
// It is intentionally widened to [minStartTxnCommitTs, maxEndTxnCommitTs]
// across multiple checkpoint updates. Event store retention is checkpoint-based,
// so deleting gaps inside the widened range is safe and avoids issuing a large
// number of small DeleteRange calls.
type pendingDeleteRange struct {
	item             gcRangeItem
	firstEnqueueTime time.Time
}

type compactItemKey struct {
	dbIndex     int
	uniqueKeyID uint64
	tableID     int64
}

type compactState struct {
	endTxnCommitTs uint64
	compacted      bool
}

type gcManager struct {
	mu            sync.Mutex
	dbs           []*pebble.DB
	deleteRanges  map[compactItemKey]*pendingDeleteRange
	compactRanges map[compactItemKey]*compactState

	deleteDataRange  deleteDataRangeFunc
	compactDataRange compactDataRangeFunc
}

const (
	// Avoid issuing a Pebble DeleteRange for every tiny checkpoint movement. Small
	// delete ranges accumulate a large number of tombstones and make iterator
	// initialization expensive for scan-heavy workloads.
	minDeleteRangeInterval = 5 * time.Minute
	// Low-traffic subscriptions may not accumulate enough ts span to hit
	// minDeleteRangeInterval quickly. Flush them eventually to avoid retaining old
	// data forever.
	maxPendingDeleteRangeDelay = 30 * time.Minute
)

func newGCManager(
	dbs []*pebble.DB,
	deleteDataRange deleteDataRangeFunc,
	compactDataRange compactDataRangeFunc,
) *gcManager {
	return &gcManager{
		dbs:              dbs,
		deleteRanges:     make(map[compactItemKey]*pendingDeleteRange),
		compactRanges:    make(map[compactItemKey]*compactState),
		deleteDataRange:  deleteDataRange,
		compactDataRange: compactDataRange,
	}
}

// addGCItem records a coarse pending delete range for one subscription/table.
// The range is widened to [minStartTxnCommitTs, maxEndTxnCommitTs] instead of
// tracking exact disjoint intervals. Once checkpoint advances, any data below
// the newest endTxnCommitTs is safe to delete, and deleting empty gaps is
// harmless for event store.
func (d *gcManager) addGCItem(
	dbIndex int, uniqueKeyID uint64, tableID int64, startTxnCommitTs uint64, endTxnCommitTs uint64,
) {
	if endTxnCommitTs <= startTxnCommitTs {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	key := compactItemKey{
		dbIndex:     dbIndex,
		uniqueKeyID: uniqueKeyID,
		tableID:     tableID,
	}
	pending, ok := d.deleteRanges[key]
	if !ok {
		d.deleteRanges[key] = &pendingDeleteRange{
			item: gcRangeItem{
				dbIndex:          dbIndex,
				uniqueKeyID:      uniqueKeyID,
				tableID:          tableID,
				startTxnCommitTs: startTxnCommitTs,
				endTxnCommitTs:   endTxnCommitTs,
			},
			firstEnqueueTime: time.Now(),
		}
		return
	}

	if startTxnCommitTs < pending.item.startTxnCommitTs {
		pending.item.startTxnCommitTs = startTxnCommitTs
	}
	if endTxnCommitTs > pending.item.endTxnCommitTs {
		pending.item.endTxnCommitTs = endTxnCommitTs
	}
}

func (d *gcManager) fetchAllGCItems() []gcRangeItem {
	return d.fetchGCItems(time.Now(), minDeleteRangeInterval, maxPendingDeleteRangeDelay)
}

func (d *gcManager) fetchGCItems(now time.Time, minRangeInterval, maxDelay time.Duration) []gcRangeItem {
	d.mu.Lock()
	defer d.mu.Unlock()

	var ranges []gcRangeItem
	for key, pending := range d.deleteRanges {
		if !shouldFlushDeleteRange(pending, now, minRangeInterval, maxDelay) {
			continue
		}
		ranges = append(ranges, pending.item)
		delete(d.deleteRanges, key)
	}
	return ranges
}

func (d *gcManager) pendingDeleteRangeCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.deleteRanges)
}

func shouldFlushDeleteRange(pending *pendingDeleteRange, now time.Time, minRangeInterval, maxDelay time.Duration) bool {
	// addGCItem only records valid ranges, so this should not happen in normal flow.
	// Keep the guard as a defensive check against unexpected state or future refactors.
	if pending == nil || pending.item.endTxnCommitTs <= pending.item.startTxnCommitTs {
		return false
	}
	if maxDelay > 0 && now.Sub(pending.firstEnqueueTime) >= maxDelay {
		return true
	}
	if minRangeInterval <= 0 {
		return true
	}

	startPhysical := oracle.ExtractPhysical(pending.item.startTxnCommitTs)
	endPhysical := oracle.ExtractPhysical(pending.item.endTxnCommitTs)
	if endPhysical <= startPhysical {
		return false
	}
	return time.Duration(endPhysical-startPhysical)*time.Millisecond >= minRangeInterval
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
				zap.Int("pendingRangeCount", d.pendingDeleteRangeCount()),
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
		if err := d.deleteDataRange(db, r.uniqueKeyID, r.tableID, r.startTxnCommitTs, r.endTxnCommitTs); err != nil {
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
		if state.endTxnCommitTs < r.endTxnCommitTs {
			state.endTxnCommitTs = r.endTxnCommitTs
			state.compacted = false
		}
	}
}

func (d *gcManager) doCompaction() {
	toCompact := make(map[compactItemKey]uint64)
	d.mu.Lock()
	for key, state := range d.compactRanges {
		if !state.compacted {
			toCompact[key] = state.endTxnCommitTs
			state.compacted = true
		}
	}
	d.mu.Unlock()

	startTime := time.Now()
	log.Info("gc manager compacting ranges", zap.Int("rangeCount", len(toCompact)))
	for key, endTxnCommitTs := range toCompact {
		db := d.dbs[key.dbIndex]
		if err := d.compactDataRange(db, key.uniqueKeyID, key.tableID, 0, endTxnCommitTs); err != nil {
			log.Warn("gc manager failed to compact data range",
				zap.Int("dbIndex", key.dbIndex),
				zap.Uint64("uniqueKeyID", key.uniqueKeyID),
				zap.Int64("tableID", key.tableID),
				zap.Uint64("endTxnCommitTs", endTxnCommitTs),
				zap.Error(err))
		}
	}
	log.Info("gc manager compacting ranges done",
		zap.Int("rangeCount", len(toCompact)),
		zap.Duration("duration", time.Since(startTime)))
}
