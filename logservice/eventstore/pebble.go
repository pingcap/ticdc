package eventstore

import (
	"fmt"
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// TODO: add config for pebble options
const (
	cacheSize         = 2 << 30  // 2GB
	memTableTotalSize = 4 << 30  // 4GB
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
		l.TargetFileSize = 32 << 20 // 32 MB
		l.Compression = pebble.ZstdCompression
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
	for i := 0; i < dbCount; i++ {
		opts := newPebbleOptions(dbNum)
		opts.Cache = cache
		opts.TableCache = tableCache
		db, err := pebble.Open(fmt.Sprintf("%s/%04d", rootDir, i), opts)
		if err != nil {
			log.Fatal("open db failed", zap.Error(err))
		}
		dbs[i] = db
	}
	return dbs
}
