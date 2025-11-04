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

func TestReadyEvent(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewReadyEvent(did)
	data, err := e.Marshal()
	require.NoError(t, err)
	require.Len(t, data, int(e.GetSize())+int(GetEventHeaderSize()))

	var e2 ReadyEvent
	err = e2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, e.Version, e2.Version)
	require.Equal(t, e.DispatcherID, e2.DispatcherID)
}

func TestReadyEventMethods(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewReadyEvent(did)

	// Test GetType
	require.Equal(t, TypeReadyEvent, e.GetType())

	// Test GetSeq
	require.Equal(t, uint64(0), e.GetSeq())

	// Test GetEpoch
	require.Equal(t, uint64(0), e.GetEpoch())

	// Test GetDispatcherID
	require.Equal(t, did, e.GetDispatcherID())

	// Test GetCommitTs
	require.Equal(t, common.Ts(0), e.GetCommitTs())

	// Test GetStartTs
	require.Equal(t, common.Ts(0), e.GetStartTs())

	// Test IsPaused
	require.False(t, e.IsPaused())

	// Test Len
	require.Equal(t, int32(0), e.Len())
}

func TestReadyEventMarshalUnmarshal(t *testing.T) {
	normalEvent := NewReadyEvent(common.NewDispatcherID())
	testCases := []struct {
		name      string
		event     *ReadyEvent
		wantError bool
	}{
		{
			name:      "normal case",
			event:     &normalEvent,
			wantError: false,
		},
		{
			name: "zero values",
			event: &ReadyEvent{
				Version:      ReadyEventVersion1,
				DispatcherID: common.DispatcherID{},
			},
			wantError: false,
		},
		{
			name: "invalid version",
			event: &ReadyEvent{
				Version:      0,
				DispatcherID: common.NewDispatcherID(),
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

			var e2 ReadyEvent
			err = e2.Unmarshal(data)
			require.NoError(t, err)
			require.Equal(t, tc.event.Version, e2.Version)
			require.Equal(t, tc.event.DispatcherID, e2.DispatcherID)
		})
	}
}

// TestReadyEventHeader verifies the unified header format
func TestReadyEventHeader(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewReadyEvent(did)

	data, err := e.Marshal()
	require.NoError(t, err)

	// Verify header
	eventType, version, payloadLen, err := UnmarshalEventHeader(data)
	require.NoError(t, err)
	require.Equal(t, TypeReadyEvent, eventType)
	require.Equal(t, ReadyEventVersion1, version)
	require.Equal(t, uint64(e.GetSize()), payloadLen)

	// Verify total size
	headerSize := GetEventHeaderSize()
	require.Equal(t, uint64(headerSize)+payloadLen, uint64(len(data)))
}

// TestReadyEventUnmarshalErrors tests error handling in Unmarshal
func TestReadyEventUnmarshalErrors(t *testing.T) {
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
			name: "invalid magic bytes",
			data: func() []byte {
				// Create a 16-byte header with invalid magic
				header := make([]byte, 16)
				binary.BigEndian.PutUint32(header[0:4], 0x00000000) // invalid magic
				binary.BigEndian.PutUint16(header[4:6], uint16(TypeReadyEvent))
				binary.BigEndian.PutUint16(header[6:8], uint16(ReadyEventVersion1))
				binary.BigEndian.PutUint64(header[8:16], 0)
				return header
			}(),
			wantError: "invalid magic bytes",
		},
		{
			name: "wrong event type",
			data: func() []byte {
				// Create a valid header but with wrong event type
				header := make([]byte, 16)
				binary.BigEndian.PutUint32(header[0:4], 0xDA7A6A6A)           // valid magic
				binary.BigEndian.PutUint16(header[4:6], uint16(TypeDMLEvent)) // wrong type
				binary.BigEndian.PutUint16(header[6:8], uint16(ReadyEventVersion1))
				binary.BigEndian.PutUint64(header[8:16], 0)
				return header
			}(),
			wantError: "ReadyEvent",
		},
		{
			name: "incomplete data",
			data: func() []byte {
				// Create a header claiming more data than provided
				header := make([]byte, 16)
				binary.BigEndian.PutUint32(header[0:4], 0xDA7A6A6A) // valid magic
				binary.BigEndian.PutUint16(header[4:6], uint16(TypeReadyEvent))
				binary.BigEndian.PutUint16(header[6:8], uint16(ReadyEventVersion1))
				binary.BigEndian.PutUint64(header[8:16], 100) // claim 100 bytes but don't provide
				return header
			}(),
			wantError: "incomplete data",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var e ReadyEvent
			err := e.Unmarshal(tc.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantError)
		})
	}
}

// TestReadyEventSize verifies GetSize calculation
func TestReadyEventSize(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewReadyEvent(did)

	// GetSize should only return business data size, not including header
	expectedSize := int64(did.GetSize())
	require.Equal(t, expectedSize, e.GetSize())

	// Marshaled data should include header
	data, err := e.Marshal()
	require.NoError(t, err)
	require.Equal(t, int(e.GetSize())+GetEventHeaderSize(), len(data))
}
