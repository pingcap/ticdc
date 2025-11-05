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

package event

import (
	"encoding/binary"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestSyncpointEvent(t *testing.T) {
	e := NewSyncPointEvent(common.NewDispatcherID(), []uint64{100, 102}, 1000, 10)
	data, err := e.Marshal()
	require.NoError(t, err)
	// Now includes header size
	require.Len(t, data, int(e.GetSize())+GetEventHeaderSize())

	var e2 SyncPointEvent
	err = e2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, *e, e2)
}

func TestSyncpointEventWithEmptyCommitTsList(t *testing.T) {
	e := NewSyncPointEvent(common.NewDispatcherID(), []uint64{}, 1000, 10)
	data, err := e.Marshal()
	require.NoError(t, err)
	// Now includes header size
	require.Len(t, data, int(e.GetSize())+GetEventHeaderSize())

	var e2 SyncPointEvent
	err = e2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, *e, e2)
}

func TestSyncPointEventMethods(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewSyncPointEvent(did, []uint64{100, 102}, 1000, 10)

	// Test GetType
	require.Equal(t, TypeSyncPointEvent, e.GetType())

	// Test GetSeq
	require.Equal(t, uint64(1000), e.GetSeq())

	// Test GetEpoch
	require.Equal(t, uint64(10), e.GetEpoch())

	// Test GetDispatcherID
	require.Equal(t, did, e.GetDispatcherID())

	// Test GetCommitTsList
	require.Equal(t, []common.Ts{100, 102}, e.GetCommitTsList())

	// Test GetCommitTs
	require.Equal(t, common.Ts(100), e.GetCommitTs())

	// Test GetStartTs
	require.Equal(t, common.Ts(100), e.GetStartTs())

	// Test IsPaused
	require.False(t, e.IsPaused())

	// Test Len
	require.Equal(t, int32(1), e.Len())

	// Test BlockEvent interface
	blockedTables := e.GetBlockedTables()
	require.NotNil(t, blockedTables)
	require.Equal(t, InfluenceTypeAll, blockedTables.InfluenceType)

	require.Nil(t, e.GetNeedDroppedTables())
	require.Nil(t, e.GetNeedAddedTables())
	require.Nil(t, e.GetUpdatedSchemas())
}

func TestSyncPointEventMarshalUnmarshal(t *testing.T) {
	testCases := []struct {
		name         string
		dispatcherID common.DispatcherID
		commitTsList []uint64
		seq          uint64
		epoch        uint64
		version      byte
		wantError    bool
	}{
		{
			name:         "normal case with multiple ts",
			dispatcherID: common.NewDispatcherID(),
			commitTsList: []uint64{100, 102, 104},
			seq:          1000,
			epoch:        10,
			wantError:    false,
		},
		{
			name:         "single ts",
			dispatcherID: common.NewDispatcherID(),
			commitTsList: []uint64{100},
			seq:          1000,
			epoch:        10,
			wantError:    false,
		},
		{
			name:         "empty ts list",
			dispatcherID: common.NewDispatcherID(),
			commitTsList: []uint64{},
			seq:          1000,
			epoch:        10,
			wantError:    false,
		},
		// invalid version
		{
			name:         "invalid version",
			dispatcherID: common.NewDispatcherID(),
			commitTsList: []uint64{100},
			seq:          1000,
			epoch:        10,
			version:      byte(128),
			wantError:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			e := NewSyncPointEvent(tc.dispatcherID, tc.commitTsList, tc.seq, tc.epoch)
			data, err := e.Marshal()
			require.NoError(t, err)

			var e2 SyncPointEvent
			err = e2.Unmarshal(data)
			require.NoError(t, err)
			require.Equal(t, *e, e2)
		})
	}
}

// TestSyncPointEventHeader verifies the unified header format
func TestSyncPointEventHeader(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewSyncPointEvent(did, []uint64{100, 102}, 1000, 10)

	data, err := e.Marshal()
	require.NoError(t, err)

	// Verify header
	eventType, version, payloadLen, err := UnmarshalEventHeader(data)
	require.NoError(t, err)
	require.Equal(t, TypeSyncPointEvent, eventType)
	require.Equal(t, SyncPointEventVersion1, version)
	require.Equal(t, uint64(e.GetSize()), payloadLen)

	// Verify total size
	headerSize := GetEventHeaderSize()
	require.Equal(t, uint64(headerSize)+payloadLen, uint64(len(data)))
}

// TestSyncPointEventUnmarshalErrors tests error handling in Unmarshal
func TestSyncPointEventUnmarshalErrors(t *testing.T) {
	testCases := []struct {
		name      string
		data      []byte
		wantError string
	}{
		{
			name:      "empty data",
			data:      []byte{},
			wantError: "data too short",
		},
		{
			name:      "invalid magic bytes",
			data:      []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			wantError: "invalid magic bytes",
		},
		{
			name: "wrong event type",
			data: func() []byte {
				header := make([]byte, 16)
				binary.BigEndian.PutUint32(header[0:4], 0xDA7A6A6A)
				binary.BigEndian.PutUint16(header[4:6], uint16(TypeDMLEvent)) // wrong type
				binary.BigEndian.PutUint16(header[6:8], uint16(SyncPointEventVersion1))
				binary.BigEndian.PutUint64(header[8:16], 0)
				return header
			}(),
			wantError: "expected SyncPointEvent",
		},
		{
			name: "incomplete data",
			data: func() []byte {
				header := make([]byte, 16)
				binary.BigEndian.PutUint32(header[0:4], 0xDA7A6A6A)
				binary.BigEndian.PutUint16(header[4:6], uint16(TypeSyncPointEvent))
				binary.BigEndian.PutUint16(header[6:8], uint16(SyncPointEventVersion1))
				binary.BigEndian.PutUint64(header[8:16], 100) // Set payload length to 100 but don't provide data
				return header
			}(),
			wantError: "incomplete data",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var e SyncPointEvent
			err := e.Unmarshal(tc.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantError)
		})
	}
}

// TestSyncPointEventSize verifies GetSize calculation
func TestSyncPointEventSize(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewSyncPointEvent(did, []uint64{100, 102}, 1000, 10)

	// GetSize should only return business data size, not including header
	// Seq(8) + Epoch(8) + DispatcherID + len(CommitTsList)(4) + 2*CommitTs(8*2)
	expectedSize := int64(8 + 8 + did.GetSize() + 4 + 8*2)
	require.Equal(t, expectedSize, e.GetSize())

	// Marshaled data should include header
	data, err := e.Marshal()
	require.NoError(t, err)
	require.Equal(t, int(e.GetSize())+GetEventHeaderSize(), len(data))
}

// TestSyncPointEventPostFlush tests PostFlush functionality
func TestSyncPointEventPostFlush(t *testing.T) {
	e := NewSyncPointEvent(common.NewDispatcherID(), []uint64{100}, 1000, 10)

	called := 0
	e.AddPostFlushFunc(func() { called++ })
	e.AddPostFlushFunc(func() { called++ })

	e.PostFlush()
	require.Equal(t, 2, called)

	// Test ClearPostFlushFunc
	e.ClearPostFlushFunc()
	called = 0
	e.PostFlush()
	require.Equal(t, 0, called)

	// Test PushFrontFlushFunc
	e.AddPostFlushFunc(func() { called += 1 })
	e.PushFrontFlushFunc(func() { called += 10 })
	e.PostFlush()
	require.Equal(t, 11, called) // Should be 10 + 1
}
