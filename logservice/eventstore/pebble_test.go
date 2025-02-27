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

func TestCompressAndDecompressData(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "normal data",
			input: []byte("this is a normal text"),
		},
		{
			name:  "empty data",
			input: []byte(""),
		},
		{
			name:  "large data",
			input: bytes.Repeat([]byte("this is a repeat text"), 10000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := compressData(tt.input, nil)
			require.Nil(t, err)

			decompressed, err := decompressData(compressed, nil)
			require.Nil(t, err)

			require.Equal(t, tt.input, decompressed)
		})
	}
}

func TestDecompressInvalidData(t *testing.T) {
	invalidData := []byte("this is invalid lz4 compressed data")
	_, err := decompressData(invalidData, nil)
	require.NotNil(t, err)
}

// 单元测试
func TestWriteAndReadRawKVEntry(t *testing.T) {
	dbPath := fmt.Sprintf("/tmp/testdb-%s", t.Name())
	os.RemoveAll(dbPath)
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		t.Fatalf("failed to open pebble db: %v", err)
	}
	defer db.Close()

	batch := db.NewBatch()
	defer batch.Close()

	entries := []*common.RawKVEntry{
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
	}

	for index, entry := range entries {
		// mock key
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(index))
		writeRawKVEntryIntoBatch(buf, entry, batch)
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatalf("failed to commit batch: %v", err)
	}

	iter, err := db.NewIter(nil)
	require.Nil(t, err)
	defer iter.Close()

	index := 0
	for iter.First(); iter.Valid(); iter.Next() {
		entry := readRawKVEntryFromIter(iter)
		require.Equal(t, entries[index], entry)
		index++
	}
}
