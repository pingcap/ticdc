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

package common

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRawKVEntryEncodeDecode_PutOperation(t *testing.T) {
	original := RawKVEntry{
		OpType:   OpTypePut,            // 4 bytes
		CRTs:     1234567890,           // 8 bytes
		StartTs:  9876543210,           // 8 bytes
		RegionID: 42,                   // 8 bytes
		Key:      []byte("12345678"),   // 8 bytes
		Value:    []byte("123456789A"), // 10 bytes
		OldValue: make([]byte, 0),      // 10 bytes
	}

	original.KeyLen = uint32(len(original.Key))           // 4 bytes
	original.ValueLen = uint32(len(original.Value))       // 4 bytes
	original.OldValueLen = uint32(len(original.OldValue)) // 4 bytes

	encoded := original.Encode()

	log.Info("encoded", zap.Any("encoded", encoded), zap.Int("len", len(encoded)))

	var decoded RawKVEntry
	err := decoded.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

func TestRawKVEntryEncodeDecode_DeleteOperation(t *testing.T) {
	original := RawKVEntry{
		OpType:   OpTypeDelete,
		CRTs:     1111111111,
		StartTs:  2222222222,
		RegionID: 24,
		Key:      []byte("delete_key"),
		Value:    make([]byte, 0),
		OldValue: []byte("old_value"),
	}

	encoded := original.Encode()

	var decoded RawKVEntry
	err := decoded.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

func TestRawKVEntryEncodeDecode_ResolvedOperation(t *testing.T) {
	original := RawKVEntry{
		OpType:   OpTypeResolved,
		CRTs:     3333333333,
		StartTs:  4444444444,
		RegionID: 100,
		Key:      make([]byte, 0),
		Value:    make([]byte, 0),
		OldValue: make([]byte, 0),
	}

	encoded := original.Encode()

	var decoded RawKVEntry
	err := decoded.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}

func TestCompareEncodedSize(t *testing.T) {
	entry := getRawKVEntry()
	encoded := entry.Encode()
	jsonEncoded, err := json.Marshal(entry)
	require.NoError(t, err)

	require.Less(t, len(encoded), len(jsonEncoded))
}

func TestRawKVEntry_SplitUpdate_UpdateOperation(t *testing.T) {
	t.Parallel()
	// Test case: Split an update operation
	updateEntry := &RawKVEntry{
		OpType:   OpTypePut,
		CRTs:     1234567890,
		StartTs:  9876543210,
		RegionID: 42,
		Key:      []byte("test-key"),
		Value:    []byte("new-value"),
		OldValue: []byte("old-value"),
	}

	deleteRow, insertRow, err := updateEntry.SplitUpdate()

	require.NoError(t, err)
	require.NotNil(t, deleteRow)
	require.NotNil(t, insertRow)

	// Verify delete row
	require.Equal(t, OpTypeDelete, deleteRow.OpType)
	require.Equal(t, updateEntry.CRTs, deleteRow.CRTs)
	require.Equal(t, updateEntry.StartTs, deleteRow.StartTs)
	require.Equal(t, updateEntry.RegionID, deleteRow.RegionID)
	require.Equal(t, updateEntry.Key, deleteRow.Key)
	require.Equal(t, updateEntry.OldValue, deleteRow.Value)

	// Verify insert row
	require.Equal(t, OpTypePut, insertRow.OpType)
	require.Equal(t, updateEntry.CRTs, insertRow.CRTs)
	require.Equal(t, updateEntry.StartTs, insertRow.StartTs)
	require.Equal(t, updateEntry.RegionID, insertRow.RegionID)
	require.Equal(t, updateEntry.Key, insertRow.Key)
	require.Equal(t, updateEntry.Value, insertRow.Value)
}

func TestRawKVEntry_SplitUpdate_NotUpdateOperation(t *testing.T) {
	t.Parallel()
	// Test case: Put operation without old value (not an update)
	putEntry := &RawKVEntry{
		OpType:   OpTypePut,
		CRTs:     1111111111,
		StartTs:  2222222222,
		RegionID: 24,
		Key:      []byte("insert-key"),
		Value:    []byte("insert-value"),
		OldValue: nil,
	}

	deleteRow, insertRow, err := putEntry.SplitUpdate()

	require.NoError(t, err)
	require.Nil(t, deleteRow)
	require.Nil(t, insertRow)
}

func TestRawKVEntry_IsUpdate(t *testing.T) {
	t.Parallel()

	// Test case 1: Update operation (OpTypePut with both OldValue and Value)
	t.Run("IsUpdate_UpdateOperation", func(t *testing.T) {
		entry := &RawKVEntry{
			OpType:   OpTypePut,
			Key:      []byte("test-key"),
			Value:    []byte("new-value"),
			OldValue: []byte("old-value"),
		}
		require.True(t, entry.IsUpdate())
	})

	// Test case 2: Delete operation (not an update)
	t.Run("IsUpdate_DeleteOperation", func(t *testing.T) {
		entry := &RawKVEntry{
			OpType:   OpTypeDelete,
			Key:      []byte("test-key"),
			Value:    []byte("value"),
			OldValue: []byte("old-value"),
		}
		require.False(t, entry.IsUpdate())
	})

	// Test case 3: Insert operation (OpTypePut but no OldValue)
	t.Run("IsUpdate_InsertOperation", func(t *testing.T) {
		entry := &RawKVEntry{
			OpType:   OpTypePut,
			Key:      []byte("test-key"),
			Value:    []byte("new-value"),
			OldValue: nil, // No old value means it's an insert
		}
		require.False(t, entry.IsUpdate())
	})

	// Test case 4: Update operation with empty old value
	t.Run("IsUpdate_EmptyOldValue", func(t *testing.T) {
		entry := &RawKVEntry{
			OpType:   OpTypePut,
			Key:      []byte("test-key"),
			Value:    []byte("new-value"),
			OldValue: []byte{},
		}
		require.False(t, entry.IsUpdate())
	})
}
