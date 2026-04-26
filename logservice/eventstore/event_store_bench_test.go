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

func makeBenchmarkRawKVEntries(count int, valueSize int) []common.RawKVEntry {
	kvs := make([]common.RawKVEntry, 0, count)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		value := bytes.Repeat([]byte{'v'}, valueSize)
		kvs = append(kvs, common.RawKVEntry{
			OpType:  common.OpTypePut,
			StartTs: uint64(100 + i),
			CRTs:    uint64(1000 + i),
			Key:     key,
			Value:   value,
		})
	}
	return kvs
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

func BenchmarkEventStoreWriteEvents(b *testing.B) {
	_, storeInt := newEventStoreForTest(b.TempDir())
	store := storeInt.(*eventStore)
	defer store.Close(context.Background())

	events := []eventWithCallback{{
		subID:    1,
		tableID:  1,
		kvs:      makeBenchmarkRawKVEntries(128, 256),
		callback: func() {},
	}}
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		b.Fatal(err)
	}
	defer encoder.Close()
	var compressionBuf []byte

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.writeEvents(store.dbs[0], events, encoder, &compressionBuf); err != nil {
			b.Fatal(err)
		}
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
	if err := store.writeEvents(db, events, encoder, &compressionBuf); err != nil {
		b.Fatal(err)
	}

	tableSpan := &heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte{},
		EndKey:   []byte{0xff},
	}
	lower := EncodeKeyPrefix(1, 1, 1)
	upper := EncodeKeyPrefix(1, 1, 1<<63)

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
