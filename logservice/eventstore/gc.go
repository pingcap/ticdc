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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
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

type gcManager struct {
	mu     sync.Mutex
	ranges []gcRangeItem
}

func newGCManager() *gcManager {
	return &gcManager{}
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

type deleteFunc func(dbIndex int, uniqueKeyID uint64, tableID int64, startCommitTS uint64, endCommitTS uint64) error

func (d *gcManager) run(ctx context.Context, deleteDataRange deleteFunc) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			ranges := d.fetchAllGCItems()
			if len(ranges) == 0 {
				continue
			}
			for _, r := range ranges {
				// TODO: delete in batch?
				err := deleteDataRange(r.dbIndex, r.uniqueKeyID, r.tableID, r.startTs, r.endTs)
				if err != nil {
					// TODO: add the data range back?
					log.Fatal("delete fail", zap.Error(err))
					return err
				}
			}
			metrics.EventStoreDeleteRangeCount.Add(float64(len(ranges)))
		}
	}
}
