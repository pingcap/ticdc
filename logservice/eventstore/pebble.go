// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package eventstore

import (
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

// TODO: add config for pebble options
const (
	cacheSize         = 1 << 30  // 1GB
	memTableTotalSize = 1 << 30  // 1GB
	memTableSize      = 64 << 20 // 64MB
)

func newPebbleOptions(dbNum int) *pebble.Options {
	opts := &pebble.Options{
		// Disable WAL to decrease io
		DisableWAL: true,

		MaxOpenFiles: 10000,

		MaxConcurrentCompactions: func() int { return 6 },

		// Decrease compaction frequency
		L0CompactionThreshold:     20,
		L0CompactionFileThreshold: 20,

		// It's meaningless to stop writes in L0
		L0StopWritesThreshold: math.MaxInt32,

		// Configure large memtable to keep recent data in memory
		MemTableSize:                memTableSize,
		MemTableStopWritesThreshold: memTableTotalSize / dbNum / memTableSize,

		// Configure options to optimize read/write performance
		Levels: make([]pebble.LevelOptions, 7),
	}

	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32KB block size
		l.IndexBlockSize = 256 << 10 // 256KB index block
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		l.TargetFileSize = 64 << 20 // 64 MB
		l.Compression = pebble.SnappyCompression
		l.EnsureDefaults()
	}
	opts.Levels[6].FilterPolicy = nil
	opts.FlushSplitBytes = opts.Levels[0].TargetFileSize
	opts.EnsureDefaults()
	return opts
}

func createPebbleDBs(rootDir string, dbNum int) []*pebble.DB {
	cache := pebble.NewCache(cacheSize)
	tableCache := pebble.NewTableCache(cache, dbNum, int(cache.MaxSize()))
	dbs := make([]*pebble.DB, dbNum)
	for i := 0; i < dbNum; i++ {
		id := strconv.Itoa(i + 1)
		var writeStallState atomic.Pointer[eventStoreWriteStallState]

		opts := newPebbleOptions(dbNum)
		opts.Cache = cache
		opts.TableCache = tableCache
		opts.EventListener = &pebble.EventListener{
			BackgroundError: func(err error) {
				log.Warn("pebble background error", zap.String("id", id), zap.Error(err))
			},
			CompactionEnd: func(info pebble.CompactionInfo) {
				metrics.EventStorePebbleCompactionDurationHistogram.WithLabelValues(id).Observe(info.TotalDuration.Seconds())
			},
			FlushEnd: func(info pebble.FlushInfo) {
				metrics.EventStorePebbleFlushDurationHistogram.WithLabelValues(id).Observe(info.TotalDuration.Seconds())
			},
			WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
				metrics.EventStorePebbleWriteStallCount.WithLabelValues(id, info.Reason).Inc()
				writeStallState.CompareAndSwap(nil, &eventStoreWriteStallState{startAt: time.Now(), reason: info.Reason})
			},
			WriteStallEnd: func() {
				state := writeStallState.Swap(nil)
				if state == nil {
					return
				}
				metrics.EventStorePebbleWriteStallDuration.WithLabelValues(id, state.reason).Add(time.Since(state.startAt).Seconds())
			},
		}
		db, err := pebble.Open(fmt.Sprintf("%s/%04d", rootDir, i), opts)
		if err != nil {
			log.Fatal("open db failed", zap.Error(err))
		}
		dbs[i] = db
	}
	return dbs
}

type eventStoreWriteStallState struct {
	startAt time.Time
	reason  string
}
