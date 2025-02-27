package eventstore

import (
	"bytes"
	"fmt"
	"math"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/pierrec/lz4"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

// TODO: add config for pebble options
const (
	cacheSize         = 2 << 30   // 2GB
	memTableTotalSize = 8 << 30   // 8GB
	memTableSize      = 128 << 20 // 128MB
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
		if i == 0 {
			// level 0 with no compression for better write performance
			l.Compression = pebble.NoCompression
		} else {
			l.Compression = pebble.SnappyCompression
		}
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

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func compressData(data []byte, buf *bytes.Buffer) ([]byte, error) {
	if buf == nil {
		buf = new(bytes.Buffer)
	}
	zw := lz4.NewWriter(buf)
	_, err := zw.Write(data)
	if err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decompressData(compressed []byte, buf *bytes.Buffer) ([]byte, error) {
	if buf == nil {
		buf = new(bytes.Buffer)
	}
	zr := lz4.NewReader(bytes.NewReader(compressed))
	_, err := buf.ReadFrom(zr)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func writeRawKVEntryIntoBatch(key []byte, entry *common.RawKVEntry, batch *pebble.Batch) {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	value := entry.Encode()
	compressedValue, err := compressData(value, buf)
	if err != nil {
		log.Panic("failed to compress data", zap.Error(err))
	}
	ratio := float64(len(value)) / float64(len(compressedValue))
	metrics.EventStoreCompressRatio.Set(ratio)
	if err := batch.Set(key, compressedValue, pebble.NoSync); err != nil {
		log.Panic("failed to update pebble batch", zap.Error(err))
	}
}

func readRawKVEntryFromIter(iter *pebble.Iterator) *common.RawKVEntry {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	value := iter.Value()
	decompressedValue, err := decompressData(value, buf)
	if err != nil {
		log.Panic("failed to decompress value", zap.Error(err))
	}
	metrics.EventStoreScanBytes.Add(float64(len(decompressedValue)))
	rawKV := &common.RawKVEntry{}
	rawKV.Decode(decompressedValue)
	return rawKV
}
