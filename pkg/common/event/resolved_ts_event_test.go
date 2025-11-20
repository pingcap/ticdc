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

func TestResolvedEvent(t *testing.T) {
	did := common.NewDispatcherID()
	e := ResolvedEvent{
		Version:      ResolvedEventVersion1,
		DispatcherID: did,
		ResolvedTs:   123,
		Epoch:        10,
	}
	data, err := e.Marshal()
	require.NoError(t, err)
	// Now includes header size
	require.Len(t, data, int(e.GetSize())+GetEventHeaderSize())

	var e2 ResolvedEvent
	err = e2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, e, e2)
}

func TestResolvedEventMethods(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewResolvedEvent(123, did, 10)

	// Test GetType
	require.Equal(t, TypeResolvedEvent, e.GetType())

	// Test GetSeq
	require.Equal(t, uint64(0), e.GetSeq())

	// Test GetEpoch
	require.Equal(t, uint64(10), e.GetEpoch())

	// Test GetDispatcherID
	require.Equal(t, did, e.GetDispatcherID())

	// Test GetCommitTs
	require.Equal(t, common.Ts(123), e.GetCommitTs())

	// Test GetStartTs
	require.Equal(t, common.Ts(123), e.GetStartTs())

	// Test IsPaused
	require.False(t, e.IsPaused())

	// Test Len
	require.Equal(t, int32(1), e.Len())
}

func TestResolvedEventMarshalUnmarshal(t *testing.T) {
	testCases := []struct {
		name      string
		event     ResolvedEvent
		wantError bool
	}{
		{
			name: "normal case",
			event: ResolvedEvent{
				Version:      ResolvedEventVersion1,
				DispatcherID: common.NewDispatcherID(),
				ResolvedTs:   123,
				Epoch:        10,
				Seq:          5,
			},
			wantError: false,
		},
		{
			name: "zero values",
			event: ResolvedEvent{
				Version:      ResolvedEventVersion1,
				DispatcherID: common.DispatcherID{},
				ResolvedTs:   0,
				Epoch:        0,
				Seq:          0,
			},
			wantError: false,
		},
		{
			name: "invalid version",
			event: ResolvedEvent{
				Version:      99,
				DispatcherID: common.NewDispatcherID(),
				ResolvedTs:   123,
				Epoch:        10,
			},
			wantError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.event.Marshal()
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			var e2 ResolvedEvent
			err = e2.Unmarshal(data)
			require.NoError(t, err)
			require.Equal(t, tc.event, e2)
		})
	}
}

// TestResolvedEventHeader verifies the unified header format
func TestResolvedEventHeader(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewResolvedEvent(123, did, 10)

	data, err := e.Marshal()
	require.NoError(t, err)

	// Verify header
	eventType, version, payloadLen, err := UnmarshalEventHeader(data)
	require.NoError(t, err)
	require.Equal(t, TypeResolvedEvent, eventType)
	require.Equal(t, ResolvedEventVersion1, version)
	require.Equal(t, uint64(e.GetSize()), payloadLen)

	// Verify total size
	headerSize := GetEventHeaderSize()
	require.Equal(t, uint64(headerSize)+payloadLen, uint64(len(data)))
}

// TestResolvedEventUnmarshalErrors tests error handling in Unmarshal
func TestResolvedEventUnmarshalErrors(t *testing.T) {
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
				binary.BigEndian.PutUint16(header[6:8], uint16(ResolvedEventVersion1))
				binary.BigEndian.PutUint64(header[8:16], 0)
				return header
			}(),
			wantError: "expected ResolvedEvent",
		},
		{
			name: "incomplete data",
			data: func() []byte {
				header := make([]byte, 16)
				binary.BigEndian.PutUint32(header[0:4], 0xDA7A6A6A)
				binary.BigEndian.PutUint16(header[4:6], uint16(TypeResolvedEvent))
				binary.BigEndian.PutUint16(header[6:8], uint16(ResolvedEventVersion1))
				binary.BigEndian.PutUint64(header[8:16], 100) // Set payload length to 100 but don't provide data
				return header
			}(),
			wantError: "incomplete data",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var e ResolvedEvent
			err := e.Unmarshal(tc.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantError)
		})
	}
}

// TestResolvedEventSize verifies GetSize calculation
func TestResolvedEventSize(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewResolvedEvent(123, did, 10)

	// GetSize should only return business data size, not including header
	expectedSize := int64(8 + 8 + 8 + did.GetSize())
	require.Equal(t, expectedSize, e.GetSize())

	// Marshaled data should include header
	data, err := e.Marshal()
	require.NoError(t, err)
	require.Equal(t, int(e.GetSize())+GetEventHeaderSize(), len(data))
}

func TestBatchResolvedTs(t *testing.T) {
	did := common.NewDispatcherID()
	did2 := common.NewDispatcherID()
	e := ResolvedEvent{
		Version:      ResolvedEventVersion1,
		DispatcherID: did,
		ResolvedTs:   123,
		Epoch:        10,
	}
	e2 := ResolvedEvent{
		Version:      ResolvedEventVersion1,
		DispatcherID: did2,
		ResolvedTs:   456,
		Epoch:        11,
	}

	b := BatchResolvedEvent{
		Events: []ResolvedEvent{e, e2},
	}
	data, err := b.Marshal()
	require.NoError(t, err)
	// Each event now includes header size
	expectedSize := (int(e.GetSize()) + GetEventHeaderSize()) * 2
	require.Len(t, data, expectedSize)

	var b2 BatchResolvedEvent
	err = b2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, b, b2)
}

func TestBatchResolvedEventMethods(t *testing.T) {
	did := common.NewDispatcherID()
	e1 := NewResolvedEvent(123, did, 10)
	e2 := NewResolvedEvent(456, did, 11)

	b := NewBatchResolvedEvent([]ResolvedEvent{e1, e2})

	// Test GetType
	require.Equal(t, TypeBatchResolvedEvent, b.GetType())

	// Test GetDispatcherID (returns fake)
	require.Equal(t, fakeDispatcherID, b.GetDispatcherID())

	// Test GetCommitTs (returns 0)
	require.Equal(t, common.Ts(0), b.GetCommitTs())

	// Test GetStartTs (returns 0)
	require.Equal(t, common.Ts(0), b.GetStartTs())

	// Test GetSeq (returns 0)
	require.Equal(t, uint64(0), b.GetSeq())

	// Test GetEpoch (returns 0)
	require.Equal(t, uint64(0), b.GetEpoch())

	// Test Len
	require.Equal(t, int32(2), b.Len())

	// Test IsPaused
	require.False(t, b.IsPaused())
}

func TestBatchResolvedEventEmpty(t *testing.T) {
	b := NewBatchResolvedEvent([]ResolvedEvent{})

	data, err := b.Marshal()
	require.NoError(t, err)
	require.Nil(t, data)
}

func TestBatchResolvedEventUnmarshalErrors(t *testing.T) {
	testCases := []struct {
		name      string
		data      []byte
		wantError string
	}{
		{
			name:      "incomplete header",
			data:      []byte{0xDA, 0x7A, 0x6A}, // Only 3 bytes, not enough for header
			wantError: "incomplete header",
		},
		{
			name: "incomplete event",
			data: func() []byte {
				// Create a valid header claiming 100 bytes but only provide header
				header := make([]byte, 16)
				binary.BigEndian.PutUint32(header[0:4], 0xDA7A6A6A)
				binary.BigEndian.PutUint16(header[4:6], uint16(TypeResolvedEvent))
				binary.BigEndian.PutUint16(header[6:8], uint16(ResolvedEventVersion1))
				binary.BigEndian.PutUint64(header[8:16], 100)
				return header
			}(),
			wantError: "incomplete event",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var b BatchResolvedEvent
			err := b.Unmarshal(tc.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantError)
		})
	}
}
