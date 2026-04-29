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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestEventStoreTableCRTsCollector(t *testing.T) {
	t.Parallel()

	collector := &tableCRTsCollector{minTs: math.MaxUint64}
	require.NoError(t, collector.Add(pebble.InternalKey{
		UserKey: EncodeKey(1, 1, &common.RawKVEntry{
			OpType:  common.OpTypePut,
			StartTs: 10,
			CRTs:    20,
			Key:     []byte("a"),
		}, CompressionNone),
	}, nil))
	require.NoError(t, collector.Add(pebble.InternalKey{
		UserKey: EncodeKey(1, 1, &common.RawKVEntry{
			OpType:  common.OpTypePut,
			StartTs: 30,
			CRTs:    40,
			Key:     []byte("b"),
		}, CompressionNone),
	}, nil))

	userProps := make(map[string]string)
	require.NoError(t, collector.Finish(userProps))
	require.Equal(t, "20", userProps[minTableCRTsLabel])
	require.Equal(t, "40", userProps[maxTableCRTsLabel])
}

func TestEventStoreIteratorWithTableFilter(t *testing.T) {
	t.Parallel()

	opts := newPebbleOptions(1)
	opts.DisableAutomaticCompactions = true
	db, err := pebble.Open(t.TempDir(), opts)
	require.NoError(t, err)
	defer db.Close()

	writeKeys := func(maxTableID int64, crts uint64) {
		for tableID := int64(1); tableID <= maxTableID; tableID++ {
			key := EncodeKey(1, tableID, &common.RawKVEntry{
				OpType:  common.OpTypePut,
				StartTs: 0,
				CRTs:    crts,
				Key:     []byte{byte(tableID)},
			}, CompressionNone)
			require.NoError(t, db.Set(key, []byte{'x'}, pebble.NoSync))
		}
		require.NoError(t, db.Flush())
	}

	writeKeys(7, 1)
	writeKeys(9, 3)

	for _, tc := range []struct {
		lowerCRTs uint64
		upperCRTs uint64
		expected  int
	}{
		{lowerCRTs: 0, upperCRTs: 1, expected: 7},
		{lowerCRTs: 1, upperCRTs: 2, expected: 7},
		{lowerCRTs: 2, upperCRTs: 3, expected: 9},
		{lowerCRTs: 3, upperCRTs: 4, expected: 9},
		{lowerCRTs: 0, upperCRTs: 10, expected: 16},
		{lowerCRTs: 10, upperCRTs: 20, expected: 0},
	} {
		t.Run(fmt.Sprintf("%d-%d", tc.lowerCRTs, tc.upperCRTs), func(t *testing.T) {
			count := 0
			for tableID := int64(0); tableID <= 9; tableID++ {
				start := EncodeKeyPrefix(1, tableID, tc.lowerCRTs)
				end := EncodeKeyPrefix(1, tableID, tc.upperCRTs+1)
				iter, err := db.NewIter(newEventStoreIterOptions(start, end, tc.lowerCRTs, tc.upperCRTs))
				require.NoError(t, err)
				for iter.First(); iter.Valid(); iter.Next() {
					count++
				}
				require.NoError(t, iter.Close())
			}
			require.Equal(t, tc.expected, count)
		})
	}
}

func TestWriteAndReadRawKVEntry(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	os.RemoveAll(dbPath)
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble db: %v", err)
	}
	defer db.Close()

	sourceEntries := []*common.RawKVEntry{
		{
			OpType:   1,
			CRTs:     123456789,
			StartTs:  987654321,
			RegionID: 1,
			KeyLen:   4,
			ValueLen: 6,
			Key:      []byte("key1"),
			Value:    []byte("value1"),
		},
		{
			OpType:   2,
			CRTs:     987654321,
			StartTs:  123456789,
			RegionID: 2,
			KeyLen:   4,
			ValueLen: 6,
			Key:      []byte("key2"),
			Value:    []byte("value2"),
		},
		{
			OpType:   2,
			CRTs:     987654321,
			StartTs:  123456789,
			RegionID: 2,
			KeyLen:   4,
			ValueLen: 6 * 10000,
			Key:      []byte("key3"),
			Value:    bytes.Repeat([]byte("value3"), 10000),
		},
		{
			OpType:   2,
			CRTs:     987654321,
			StartTs:  123456789,
			RegionID: 2,
			KeyLen:   4,
			ValueLen: 6,
			Key:      []byte("key4"),
			Value:    []byte("value4"),
		},
	}

	batch := db.NewBatch()
	defer batch.Close()
	for index, entry := range sourceEntries {
		// mock key
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(index))
		err := batch.Set(buf, entry.Encode(), pebble.NoSync)
		require.Nil(t, err)
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatalf("failed to commit batch: %v", err)
	}

	iter, err := db.NewIter(nil)
	require.Nil(t, err)
	defer iter.Close()

	// check after read all entries
	readEntries := make([]*common.RawKVEntry, 0, len(sourceEntries))
	for iter.First(); iter.Valid(); iter.Next() {
		value := iter.Value()
		copiedValue := make([]byte, len(value))
		copy(copiedValue, value)
		entry := &common.RawKVEntry{}
		entry.Decode(copiedValue)
		readEntries = append(readEntries, entry)
	}
	for i, entry := range sourceEntries {
		require.Equal(t, entry, readEntries[i])
	}
}

func TestCompressionAndKeyOrder(t *testing.T) {
	t.Parallel()

	// 1. Test key encoding and decoding correctness.
	ev := &common.RawKVEntry{
		OpType:  common.OpTypePut,
		StartTs: 1,
		CRTs:    2,
		Key:     []byte("test-key"),
	}
	keyWithZstd := EncodeKey(1, 1, ev, CompressionZSTD)
	dmlOrder, compressionType := DecodeKeyMetas(keyWithZstd)
	require.Equal(t, DMLOrderInsert, dmlOrder)
	require.Equal(t, CompressionZSTD, compressionType)

	keyWithNone := EncodeKey(1, 1, ev, CompressionNone)
	dmlOrder, compressionType = DecodeKeyMetas(keyWithNone)
	require.Equal(t, DMLOrderInsert, dmlOrder)
	require.Equal(t, CompressionNone, compressionType)

	// 2. Test key sorting order.
	// For the same transaction (same StartTs, same CRTs), the order should be Delete < Update < Insert.
	deleteEvent := &common.RawKVEntry{OpType: common.OpTypeDelete, StartTs: 100, CRTs: 110, Key: []byte("key")}
	updateEvent := &common.RawKVEntry{OpType: common.OpTypePut, OldValue: []byte("old"), StartTs: 100, CRTs: 110, Key: []byte("key")}
	insertEvent := &common.RawKVEntry{OpType: common.OpTypePut, StartTs: 100, CRTs: 110, Key: []byte("key")}

	keyDelete := EncodeKey(1, 1, deleteEvent, CompressionNone)
	keyUpdate := EncodeKey(1, 1, updateEvent, CompressionZSTD) // Use different compression to ensure it does not affect sorting.
	keyInsert := EncodeKey(1, 1, insertEvent, CompressionNone)

	require.Less(t, bytes.Compare(keyDelete, keyUpdate), 0, "Delete should come before Update")
	require.Less(t, bytes.Compare(keyUpdate, keyInsert), 0, "Update should come before Insert")
}
