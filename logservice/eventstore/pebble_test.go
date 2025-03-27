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
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

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
			OpType:      1,
			CRTs:        123456789,
			StartTs:     987654321,
			RegionID:    1,
			KeyLen:      4,
			ValueLen:    6,
			OldValueLen: 0,
			Key:         []byte("key1"),
			Value:       []byte("value1"),
			OldValue:    []byte{},
		},
		{
			OpType:      2,
			CRTs:        987654321,
			StartTs:     123456789,
			RegionID:    2,
			KeyLen:      4,
			ValueLen:    6,
			OldValueLen: 0,
			Key:         []byte("key2"),
			Value:       []byte("value2"),
			OldValue:    []byte{},
		},
		{
			OpType:      2,
			CRTs:        987654321,
			StartTs:     123456789,
			RegionID:    2,
			KeyLen:      4,
			ValueLen:    6 * 10000,
			OldValueLen: 0,
			Key:         []byte("key3"),
			Value:       bytes.Repeat([]byte("value3"), 10000),
			OldValue:    []byte{},
		},
		{
			OpType:      2,
			CRTs:        987654321,
			StartTs:     123456789,
			RegionID:    2,
			KeyLen:      4,
			ValueLen:    6,
			OldValueLen: 0,
			Key:         []byte("key4"),
			Value:       []byte("value4"),
			OldValue:    []byte{},
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
