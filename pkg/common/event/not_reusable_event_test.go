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
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestNotReusableEvent(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewNotReusableEvent(did)
	data, err := e.Marshal()
	require.NoError(t, err)
	require.Len(t, data, int(e.GetSize())+int(GetEventHeaderSize()))

	var e2 NotReusableEvent
	err = e2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, e.Version, e2.Version)
	require.Equal(t, e.DispatcherID, e2.DispatcherID)
}

func TestNotReusableEventMethods(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewNotReusableEvent(did)

	// Test GetType
	require.Equal(t, TypeNotReusableEvent, e.GetType())

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

func TestNotReusableEventMarshalUnmarshal(t *testing.T) {
	normalEvent := NewNotReusableEvent(common.NewDispatcherID())
	testCases := []struct {
		name      string
		event     *NotReusableEvent
		wantError bool
	}{
		{
			name:      "normal case",
			event:     &normalEvent,
			wantError: false,
		},
		{
			name: "zero values",
			event: &NotReusableEvent{
				Version:      0,
				DispatcherID: common.DispatcherID{},
			},
			wantError: false,
		},
		{
			name: "invalid version",
			event: &NotReusableEvent{
				Version:      1,
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

			var e2 NotReusableEvent
			err = e2.Unmarshal(data)
			require.NoError(t, err)
			require.Equal(t, tc.event.Version, e2.Version)
			require.Equal(t, tc.event.DispatcherID, e2.DispatcherID)
		})
	}
}

// TestNotReusableEventHeader verifies the unified header format
func TestNotReusableEventHeader(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewNotReusableEvent(did)

	data, err := e.Marshal()
	require.NoError(t, err)

	// Verify header
	eventType, version, payloadLen, err := UnmarshalEventHeader(data)
	require.NoError(t, err)
	require.Equal(t, TypeNotReusableEvent, eventType)
	require.Equal(t, byte(NotReusableEventVersion), version)
	require.Equal(t, int(e.GetSize()), payloadLen)

	// Verify total size
	headerSize := GetEventHeaderSize()
	require.Equal(t, headerSize+int(payloadLen), len(data))
}

// TestNotReusableEventUnmarshalErrors tests error handling in Unmarshal
func TestNotReusableEventUnmarshalErrors(t *testing.T) {
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
			data:      []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			wantError: "invalid magic bytes",
		},
		{
			name: "wrong event type",
			data: func() []byte {
				// Create a valid header but with wrong event type
				header := make([]byte, 8)
				header[0] = 0xDA
				header[1] = 0x7A
				header[2] = byte(TypeDMLEvent) // wrong type
				header[3] = 0
				return header
			}(),
			wantError: "expected NotReusableEvent",
		},
		{
			name: "incomplete data",
			data: func() []byte {
				// Create a header claiming more data than provided
				header := make([]byte, 8)
				header[0] = 0xDA
				header[1] = 0x7A
				header[2] = byte(TypeNotReusableEvent)
				header[3] = 0
				// Set payload length to 100 but don't provide data
				header[4] = 0
				header[5] = 0
				header[6] = 0
				header[7] = 100
				return header
			}(),
			wantError: "incomplete data",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var e NotReusableEvent
			err := e.Unmarshal(tc.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantError)
		})
	}
}

// TestNotReusableEventSize verifies GetSize calculation
func TestNotReusableEventSize(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewNotReusableEvent(did)

	// GetSize should only return business data size, not including header
	expectedSize := int64(did.GetSize())
	require.Equal(t, expectedSize, e.GetSize())

	// Marshaled data should include header
	data, err := e.Marshal()
	require.NoError(t, err)
	require.Equal(t, int(e.GetSize())+GetEventHeaderSize(), len(data))
}
