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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
)

const benchmarkKiB = 1024

var benchmarkWriteEventSizes = []struct {
	name string
	size int
}{
	{name: "4K", size: 4 * benchmarkKiB},
	{name: "8K", size: 8 * benchmarkKiB},
	{name: "16K", size: 16 * benchmarkKiB},
	{name: "32K", size: 32 * benchmarkKiB},
}

var benchmarkCompressionModes = []struct {
	name    string
	enabled bool
}{
	{name: "CompressionOff", enabled: false},
	{name: "CompressionOn", enabled: true},
}

func makeBenchmarkRawKVEntry(index int, valueSize int) common.RawKVEntry {
	key := []byte(fmt.Sprintf("key-%06d", index))
	value := bytes.Repeat([]byte{'v'}, valueSize)
	return common.RawKVEntry{
		OpType:  common.OpTypePut,
		StartTs: uint64(100 + index),
		CRTs:    uint64(1000 + index),
		Key:     key,
		Value:   value,
	}
}

func makeBenchmarkRawKVEntries(count int, valueSize int) []common.RawKVEntry {
	kvs := make([]common.RawKVEntry, 0, count)
	for i := 0; i < count; i++ {
		kvs = append(kvs, makeBenchmarkRawKVEntry(i, valueSize))
	}
	return kvs
}

func makeBenchmarkWriteRawKVEntries(targetSize int) []common.RawKVEntry {
	entry := makeBenchmarkRawKVEntry(0, 1)
	fixedSize := int(entry.GetSize()) - len(entry.Value)
	valueSize := targetSize - fixedSize
	if valueSize < 1 {
		valueSize = 1
	}
	return []common.RawKVEntry{makeBenchmarkRawKVEntry(0, valueSize)}
}

func BenchmarkEventStoreWriteEvents(b *testing.B) {
	for _, compressionMode := range benchmarkCompressionModes {
		b.Run(compressionMode.name, func(b *testing.B) {
			for _, size := range benchmarkWriteEventSizes {
				b.Run(size.name, func(b *testing.B) {
					_, storeInt := newEventStoreForTest(b.TempDir())
					store := storeInt.(*eventStore)
					store.enableZstdCompression = compressionMode.enabled
					defer store.Close(context.Background())

					events := []eventWithCallback{{
						subID:    1,
						tableID:  1,
						kvs:      makeBenchmarkWriteRawKVEntries(size.size),
						callback: func() {},
					}}
					if compressionMode.enabled &&
						size.name == "32K" &&
						!hasCompressibleBenchmarkKV(store, events) {
						b.Fatalf("32K case should trigger compression, threshold=%d", store.compressionThreshold)
					}

					encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
					if err != nil {
						b.Fatal(err)
					}
					defer encoder.Close()
					var compressionBuf []byte
					var rawValueBuf []byte

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := store.writeEvents(store.dbs[0], events, encoder, &compressionBuf, &rawValueBuf); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func hasCompressibleBenchmarkKV(store *eventStore, events []eventWithCallback) bool {
	if !store.enableZstdCompression {
		return false
	}
	for _, event := range events {
		for i := range event.kvs {
			if event.kvs[i].GetSize() > int64(store.compressionThreshold) {
				return true
			}
		}
	}
	return false
}

func BenchmarkEventWithCallbackSizer(b *testing.B) {
	event := eventWithCallback{
		subID:   1,
		tableID: 1,
		kvs:     makeBenchmarkRawKVEntries(1024, 128),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = eventWithCallbackSizer(event)
	}
}

func BenchmarkEventStoreIteratorNext(b *testing.B) {
	dir := b.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, storeInt := newEventStoreForTest(b.TempDir())
	store := storeInt.(*eventStore)
	defer store.Close(context.Background())

	kvs := makeBenchmarkRawKVEntries(1024, 256)
	events := []eventWithCallback{{
		subID:    1,
		tableID:  1,
		kvs:      kvs,
		callback: func() {},
	}}
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		b.Fatal(err)
	}
	defer encoder.Close()
	var compressionBuf []byte
	var rawValueBuf []byte
	if err := store.writeEvents(db, events, encoder, &compressionBuf, &rawValueBuf); err != nil {
		b.Fatal(err)
	}

	tableSpan := &heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{},
		EndKey:   []byte{0xff},
	}
	lower := EncodeTxnCommitTsBoundaryKey(1, 1, 1)
	upper := EncodeTxnCommitTsBoundaryKey(1, 1, 1<<63)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		innerIter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: lower,
			UpperBound: upper,
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = innerIter.First()
		iter := &eventStoreIter{
			tableSpan:     tableSpan,
			needCheckSpan: false,
			innerIter:     innerIter,
			decoder:       store.decoderPool.Get().(*zstd.Decoder),
			decoderPool:   store.decoderPool,
		}
		count := 0
		for {
			if _, ok := iter.Next(); !ok {
				break
			}
			count++
		}
		if count != len(kvs) {
			b.Fatalf("unexpected row count: %d", count)
		}
		if _, err := iter.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
